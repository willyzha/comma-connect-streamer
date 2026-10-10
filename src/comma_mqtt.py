import time
import json
import logging
import os
import requests
from datetime import datetime, timezone

try:
    import paho.mqtt.client as mqtt
except ImportError:
    mqtt = None

from comma_api import make_api_request, get_device_location, DONGLE_ID, get_config

# Configure logging
LOG_LEVEL_STR = get_config('LOG_LEVEL', 'INFO')
LOG_LEVEL = getattr(logging, LOG_LEVEL_STR.upper(), logging.INFO)
logging.basicConfig(
    format='[%(asctime)s] [%(name)s] %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p',
    level=LOG_LEVEL
)
logger = logging.getLogger('comma_mqtt')

# MQTT Config
MQTT_HOST = get_config('MQTT_HOST', 'localhost')
MQTT_PORT = get_config('MQTT_PORT', 1883, type=int)
MQTT_USER = get_config('MQTT_USER', None)
MQTT_PASS = get_config('MQTT_PASSWORD', None)
MQTT_DISCOVERY_PREFIX = get_config('MQTT_DISCOVERY_PREFIX', 'homeassistant')
MQTT_STATE_PREFIX = get_config('MQTT_STATE_PREFIX', 'comma')
POLL_INTERVAL = get_config('LOCATION_POLL_INTERVAL', 60, type=int)
SPEED_UNIT = get_config('MQTT_SPEED_UNIT', 'km/h').lower()
MQTT_DEVICE_NAME = get_config('MQTT_DEVICE_NAME', None) or get_config('COMMA_DEVICE_NAME', None) or get_config('COMMA_NICKNAME', None)
ENABLE_REVERSE_GEOCODE = get_config('ENABLE_REVERSE_GEOCODE', True, type=bool)

_last_geocoded_coords = (None, None)
_last_geocoded_address = None


def get_address_for_coords(lat, lng):
    """Performs reverse geocoding to human-readable address with caching."""
    global _last_geocoded_coords, _last_geocoded_address
    if not ENABLE_REVERSE_GEOCODE or lat is None or lng is None:
        return None

    try:
        # Check cache: if coords within ~20-40 meters (0.0003 deg), return cached address
        if _last_geocoded_coords[0] is not None and _last_geocoded_coords[1] is not None:
            lat_diff = abs(float(lat) - float(_last_geocoded_coords[0]))
            lng_diff = abs(float(lng) - float(_last_geocoded_coords[1]))
            if lat_diff < 0.0003 and lng_diff < 0.0003 and _last_geocoded_address:
                return _last_geocoded_address

        url = "https://nominatim.openstreetmap.org/reverse"
        headers = {
            "User-Agent": "CommaConnectStreamer/0.2.3 (https://github.com/willyzha/comma-connect-streamer)"
        }
        params = {
            "format": "jsonv2",
            "lat": lat,
            "lon": lng,
            "zoom": 18,
            "addressdetails": 1
        }
        resp = requests.get(url, params=params, headers=headers, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            addr = data.get('address', {})
            parts = []
            street = addr.get('road') or addr.get('pedestrian') or addr.get('street')
            house_num = addr.get('house_number')
            if street:
                if house_num:
                    parts.append(f"{house_num} {street}")
                else:
                    parts.append(street)

            city = addr.get('city') or addr.get('town') or addr.get('village') or addr.get('suburb')
            if city:
                parts.append(city)

            state = addr.get('state')
            postcode = addr.get('postcode')
            if state and postcode:
                parts.append(f"{state} {postcode}")
            elif state:
                parts.append(state)

            country = addr.get('country')
            if country and country not in ('United States', 'USA'):
                parts.append(country)

            formatted = ', '.join(parts) if parts else data.get('display_name', 'Unknown')
            _last_geocoded_coords = (float(lat), float(lng))
            _last_geocoded_address = formatted
            logger.debug(f"Reverse geocoded location ({lat}, {lng}) -> {formatted}")
            return formatted
    except Exception as e:
        logger.debug(f"Reverse geocode lookup failed: {e}")

    return _last_geocoded_address


def publish_location_attributes(client, location):
    """Publishes device tracker GPS coordinates and companion sensor attributes to MQTT."""
    if not location:
        return False
    lat = location.get('lat')
    lng = location.get('lng')
    if lat is None or lng is None:
        return False

    disp_name = MQTT_DEVICE_NAME or f"Comma {DONGLE_ID}"
    attr_topic = f"{MQTT_STATE_PREFIX}/{DONGLE_ID}/attributes"
    addr = get_address_for_coords(lat, lng)

    source = location.get('source', 'unknown')
    is_live = (source in ('athena_live', 'prime_location'))

    loc_time = location.get('time')
    recorded_at = None
    if loc_time:
        try:
            ts = float(loc_time)
            if ts > 1e11:  # Epoch in milliseconds
                ts = ts / 1000.0
            recorded_at = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        except Exception:
            pass

    if not recorded_at:
        recorded_at = datetime.now(timezone.utc).isoformat()

    ha_attributes = {
        "latitude": float(lat),
        "longitude": float(lng),
        "gps_accuracy": location.get('accuracy', 15),
        "altitude": location.get('altitude', 0),
        "speed": location.get('speed', 0),
        "bearing": location.get('bearing', 0),
        "source": source,
        "is_live": is_live,
        "status": "live" if is_live else "parked",
        "recorded_at": recorded_at,
        "address": addr,
        "dongle_id": DONGLE_ID,
        "device_name": disp_name,
        "last_updated": datetime.now(timezone.utc).isoformat()
    }
    client.publish(attr_topic, json.dumps(ha_attributes), retain=True)
    return True


def on_connect(client, userdata, flags, rc, *args):
    if rc == 0:
        logger.info("Connected to MQTT Broker successfully.")
        status_topic = f"{MQTT_STATE_PREFIX}/{DONGLE_ID}/status"
        client.publish(status_topic, "online", retain=True)
        publish_discovery(client)

        # Immediately publish current / last known location so Home Assistant displays tracker even after broker wipe
        loc = get_device_location()
        if loc and publish_location_attributes(client, loc):
            logger.info(f"Published retained location attributes on connect: lat={loc.get('lat')}, lng={loc.get('lng')}, source={loc.get('source')}")

        # Subscribe to Home Assistant birth topic to recover automatically if HA restarts
        ha_status_topic = f"{MQTT_DISCOVERY_PREFIX}/status"
        client.subscribe(ha_status_topic)
        logger.info(f"Subscribed to Home Assistant birth messages: {ha_status_topic}")
    else:
        logger.error(f"Failed to connect to MQTT Broker, return code {rc}")


def on_message(client, userdata, msg):
    try:
        topic = msg.topic
        payload = msg.payload.decode().strip()
        ha_status_topic = f"{MQTT_DISCOVERY_PREFIX}/status"
        if topic == ha_status_topic and payload.lower() in ("online", "birth"):
            logger.info("Home Assistant birth message detected. Republishing discovery and location attributes...")
            status_topic = f"{MQTT_STATE_PREFIX}/{DONGLE_ID}/status"
            client.publish(status_topic, "online", retain=True)
            publish_discovery(client)
            loc = get_device_location()
            if loc:
                publish_location_attributes(client, loc)
    except Exception as e:
        logger.error(f"Error handling MQTT message: {e}")


def publish_discovery(client):
    """Publishes Home Assistant MQTT Auto-Discovery configurations."""
    device_id = f"comma_{DONGLE_ID}"
    status_topic = f"{MQTT_STATE_PREFIX}/{DONGLE_ID}/status"
    attr_topic = f"{MQTT_STATE_PREFIX}/{DONGLE_ID}/attributes"

    device_display_name = MQTT_DEVICE_NAME if MQTT_DEVICE_NAME else f"Comma {DONGLE_ID}"

    device_info = {
        "identifiers": [device_id],
        "name": device_display_name,
        "model": "comma 3 / 3X",
        "manufacturer": "comma.ai"
    }

    # 1. Device Tracker (GPS Location)
    # Note: Omit state_topic so Home Assistant automatically computes zones (home/not_home)
    # based on the latitude/longitude provided in json_attributes_topic.
    tracker_config_topic = f"{MQTT_DISCOVERY_PREFIX}/device_tracker/{device_id}/config"
    tracker_payload = {
        "name": "Tracker",
        "has_entity_name": True,
        "unique_id": f"{device_id}_tracker",
        "device": device_info,
        "json_attributes_topic": attr_topic,
        "source_type": "gps",
        "availability_topic": status_topic,
        "payload_available": "online",
        "payload_not_available": "offline",
        "icon": "mdi:car-connected"
    }
    client.publish(tracker_config_topic, json.dumps(tracker_payload), retain=True)
    logger.info(f"Published Home Assistant device tracker discovery: {tracker_config_topic}")

    # 2. Speed Sensor
    if SPEED_UNIT == 'mph':
        speed_template = "{{ (value_json.speed * 2.23694) | round(1) if value_json.speed is not none else 0 }}"
        unit_str = "mph"
    elif SPEED_UNIT == 'm/s':
        speed_template = "{{ value_json.speed | round(1) if value_json.speed is not none else 0 }}"
        unit_str = "m/s"
    else:
        speed_template = "{{ (value_json.speed * 3.6) | round(1) if value_json.speed is not none else 0 }}"
        unit_str = "km/h"

    speed_config_topic = f"{MQTT_DISCOVERY_PREFIX}/sensor/{device_id}_speed/config"
    speed_payload = {
        "name": "Speed",
        "has_entity_name": True,
        "unique_id": f"{device_id}_speed",
        "device": device_info,
        "state_topic": attr_topic,
        "value_template": speed_template,
        "unit_of_measurement": unit_str,
        "device_class": "speed",
        "state_class": "measurement",
        "icon": "mdi:speedometer",
        "availability_topic": status_topic,
        "payload_available": "online",
        "payload_not_available": "offline"
    }
    client.publish(speed_config_topic, json.dumps(speed_payload), retain=True)

    # 3. Location Source Diagnostic Sensor
    source_config_topic = f"{MQTT_DISCOVERY_PREFIX}/sensor/{device_id}_source/config"
    source_payload = {
        "name": "Location Source",
        "has_entity_name": True,
        "unique_id": f"{device_id}_source",
        "device": device_info,
        "state_topic": attr_topic,
        "value_template": "{{ value_json.source if value_json.source is not none else 'unknown' }}",
        "icon": "mdi:crosshairs-gps",
        "entity_category": "diagnostic",
        "availability_topic": status_topic,
        "payload_available": "online",
        "payload_not_available": "offline"
    }
    client.publish(source_config_topic, json.dumps(source_payload), retain=True)

    # 4. Compass Bearing Diagnostic Sensor
    bearing_config_topic = f"{MQTT_DISCOVERY_PREFIX}/sensor/{device_id}_bearing/config"
    bearing_payload = {
        "name": "Bearing",
        "has_entity_name": True,
        "unique_id": f"{device_id}_bearing",
        "device": device_info,
        "state_topic": attr_topic,
        "value_template": "{{ value_json.bearing | round(0) if value_json.bearing is not none else 0 }}",
        "unit_of_measurement": "°",
        "icon": "mdi:compass",
        "entity_category": "diagnostic",
        "availability_topic": status_topic,
        "payload_available": "online",
        "payload_not_available": "offline"
    }
    client.publish(bearing_config_topic, json.dumps(bearing_payload), retain=True)

    # 5. Address Diagnostic Sensor
    if ENABLE_REVERSE_GEOCODE:
        address_config_topic = f"{MQTT_DISCOVERY_PREFIX}/sensor/{device_id}_address/config"
        address_payload = {
            "name": "Address",
            "has_entity_name": True,
            "unique_id": f"{device_id}_address",
            "device": device_info,
            "state_topic": attr_topic,
            "value_template": "{{ value_json.address if value_json.address is not none else 'Unknown' }}",
            "icon": "mdi:map-marker",
            "entity_category": "diagnostic",
            "availability_topic": status_topic,
            "payload_available": "online",
            "payload_not_available": "offline"
        }
        client.publish(address_config_topic, json.dumps(address_payload), retain=True)

    # 6. Live Tracking Binary Sensor (Connected = Live GPS, Disconnected = Parked / Last Known)
    live_config_topic = f"{MQTT_DISCOVERY_PREFIX}/binary_sensor/{device_id}_live/config"
    live_payload = {
        "name": "Live Tracking",
        "has_entity_name": True,
        "unique_id": f"{device_id}_live",
        "device": device_info,
        "state_topic": attr_topic,
        "value_template": "{{ 'ON' if value_json.is_live else 'OFF' }}",
        "device_class": "connectivity",
        "payload_on": "ON",
        "payload_off": "OFF",
        "icon": "mdi:car-connected",
        "availability_topic": status_topic,
        "payload_available": "online",
        "payload_not_available": "offline"
    }
    client.publish(live_config_topic, json.dumps(live_payload), retain=True)

    # 7. Last Seen Timestamp Sensor
    last_seen_config_topic = f"{MQTT_DISCOVERY_PREFIX}/sensor/{device_id}_last_seen/config"
    last_seen_payload = {
        "name": "Last Seen",
        "has_entity_name": True,
        "unique_id": f"{device_id}_last_seen",
        "device": device_info,
        "state_topic": attr_topic,
        "value_template": "{{ value_json.recorded_at if value_json.recorded_at is not none else value_json.last_updated }}",
        "device_class": "timestamp",
        "icon": "mdi:clock-check-outline",
        "entity_category": "diagnostic",
        "availability_topic": status_topic,
        "payload_available": "online",
        "payload_not_available": "offline"
    }
    client.publish(last_seen_config_topic, json.dumps(last_seen_payload), retain=True)


def get_location():
    return get_device_location()


def main():
    global DONGLE_ID
    if mqtt is None:
        logger.error("paho-mqtt is not installed. Please install paho-mqtt to use MQTT.")
        return

    while not DONGLE_ID or DONGLE_ID == 'your_dongle_id_here':
        logger.error("COMMA_DONGLE_ID not configured. Set COMMA_DONGLE_ID in /config/config.conf or container environment variables. Checking again in 60s...")
        time.sleep(60)
        DONGLE_ID = get_config('COMMA_DONGLE_ID', 'your_dongle_id_here')

    # Initialize MQTT client with compatibility across paho-mqtt v1 and v2
    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
    except AttributeError:
        client = mqtt.Client()

    if MQTT_USER and MQTT_PASS:
        client.username_pw_set(MQTT_USER, MQTT_PASS)

    client.on_connect = on_connect
    client.on_message = on_message

    # Configure Last Will and Testament (LWT) for availability tracking
    status_topic = f"{MQTT_STATE_PREFIX}/{DONGLE_ID}/status"
    client.will_set(status_topic, "offline", retain=True)

    logger.info(f"Connecting to MQTT Broker at {MQTT_HOST}:{MQTT_PORT}...")
    try:
        client.connect(MQTT_HOST, MQTT_PORT, 60)
    except Exception as e:
        logger.error(f"Could not connect to MQTT Broker at {MQTT_HOST}:{MQTT_PORT}: {e}")
        return

    client.loop_start()

    device_was_online = None

    try:
        while True:
            location = get_location()
            disp_name = MQTT_DEVICE_NAME or f"Comma {DONGLE_ID}"

            if location:
                source = location.get('source', 'unknown')
                is_live = (source in ('athena_live', 'prime_location'))

                if publish_location_attributes(client, location):
                    lat = location.get('lat')
                    lng = location.get('lng')
                    if is_live:
                        if device_was_online is False or device_was_online is None:
                            logger.info(f"Comma device '{disp_name}' is online. Published live location to MQTT: lat={lat}, lng={lng}, source={source} (interval: {POLL_INTERVAL}s)")
                        else:
                            logger.info(f"Published location for '{disp_name}' to MQTT: lat={lat}, lng={lng}, source={source} (interval: {POLL_INTERVAL}s)")
                        device_was_online = True
                    else:
                        # Device is parked / cached
                        if device_was_online is True:
                            logger.info(f"Comma device '{disp_name}' went offline. Preserving parked location: lat={lat}, lng={lng}, source={source}")
                            device_was_online = False
                        elif device_was_online is None:
                            logger.info(f"Comma device '{disp_name}' is currently parked/offline. Published saved location: lat={lat}, lng={lng}, source={source}")
                            device_was_online = False
                        else:
                            logger.debug(f"Preserved parked location for '{disp_name}': lat={lat}, lng={lng}, source={source}")
                else:
                    logger.warning("Location data received from Comma API but lat/lng were empty.")
            else:
                if device_was_online is True:
                    logger.info(f"Comma device '{disp_name}' is now offline or has no GPS fix. Polling quietly in background...")
                    device_was_online = False
                elif device_was_online is None:
                    logger.info(f"Comma device '{disp_name}' is currently offline (no location fix available). Polling quietly in background (interval: {POLL_INTERVAL}s)...")
                    device_was_online = False
                else:
                    logger.debug("No location data available in this polling cycle.")

            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        logger.info("Stopping comma_mqtt...")
    finally:
        client.publish(status_topic, "offline", retain=True)
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
