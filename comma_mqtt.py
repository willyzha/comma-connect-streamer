import time
import json
import logging
import os
import paho.mqtt.client as mqtt
from comma_api import make_api_request, DONGLE_ID, get_config

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

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("Connected to MQTT Broker!")
        publish_discovery(client)
    else:
        logger.error(f"Failed to connect, return code {rc}")

def publish_discovery(client):
    device_id = f"comma_{DONGLE_ID}"
    discovery_topic = f"{MQTT_DISCOVERY_PREFIX}/device_tracker/{device_id}/config"
    
    payload = {
        "state_topic": f"{MQTT_STATE_PREFIX}/{DONGLE_ID}/location",
        "name": f"Comma {DONGLE_ID}",
        "unique_id": f"{device_id}_tracker",
        "device": {
            "identifiers": [device_id],
            "name": f"Comma {DONGLE_ID}",
            "model": "comma 3",
            "manufacturer": "comma.ai"
        },
        "json_attributes_topic": f"{MQTT_STATE_PREFIX}/{DONGLE_ID}/attributes",
        "payload_available": "online",
        "payload_not_available": "offline",
        "availability_topic": f"{MQTT_STATE_PREFIX}/{DONGLE_ID}/status",
        "source_type": "gps"
    }
    
    client.publish(discovery_topic, json.dumps(payload), retain=True)
    logger.info(f"Published discovery topic: {discovery_topic}")

def get_location():
    url = f"https://api.commadotai.com/v1/devices/{DONGLE_ID}/location"
    try:
        return make_api_request(url)
    except Exception as e:
        logger.error(f"Error fetching location: {e}")
        return None

def main():
    if not DONGLE_ID or DONGLE_ID == 'your_dongle_id_here':
        logger.error("COMMA_DONGLE_ID not set. Exiting.")
        return

    client = mqtt.Client()
    if MQTT_USER and MQTT_PASS:
        client.username_pw_set(MQTT_USER, MQTT_PASS)
    
    client.on_connect = on_connect
    
    # Set Will for availability
    status_topic = f"{MQTT_STATE_PREFIX}/{DONGLE_ID}/status"
    client.will_set(status_topic, "offline", retain=True)

    try:
        client.connect(MQTT_HOST, MQTT_PORT, 60)
    except Exception as e:
        logger.error(f"Could not connect to MQTT Broker: {e}")
        return

    client.loop_start()

    # Mark as online
    client.publish(status_topic, "online", retain=True)

    try:
        while True:
            location = get_location()
            if location:
                # Device Tracker state topic expects a JSON with latitude, longitude, etc.
                # or it can be configured to parse from attributes. 
                # For Home Assistant MQTT device_tracker, if we use a single topic for state,
                # we can send a JSON payload.
                state_topic = f"{MQTT_STATE_PREFIX}/{DONGLE_ID}/location"
                attr_topic = f"{MQTT_STATE_PREFIX}/{DONGLE_ID}/attributes"
                
                # GPS data
                lat = location.get('lat')
                lng = location.get('lng')
                
                if lat is not None and lng is not None:
                    # Home Assistant expects 'latitude', 'longitude', and 'gps_accuracy' in the attributes
                    # for the device_tracker to update its location.
                    ha_attributes = location.copy()
                    ha_attributes['latitude'] = lat
                    ha_attributes['longitude'] = lng
                    ha_attributes['gps_accuracy'] = location.get('accuracy', 0)
                    
                    # Update attributes (where HA gets the coordinates)
                    client.publish(attr_topic, json.dumps(ha_attributes))
                    
                    # Update state (can be anything, but we'll use a summary or just 'online')
                    state_topic = f"{MQTT_STATE_PREFIX}/{DONGLE_ID}/location"
                    client.publish(state_topic, "online")
                    
                    logger.info(f"Updated location: {lat}, {lng}")
                else:
                    logger.warning("Location data received but missing lat/lng.")
            
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        logger.info("Stopping...")
    finally:
        client.publish(status_topic, "offline", retain=True)
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    main()
