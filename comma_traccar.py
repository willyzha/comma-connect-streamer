import time
import logging
import os
import requests
from comma_api import make_api_request, DONGLE_ID, get_config

# Configure logging
LOG_LEVEL_STR = get_config('LOG_LEVEL', 'INFO')
LOG_LEVEL = getattr(logging, LOG_LEVEL_STR.upper(), logging.INFO)
logging.basicConfig(
    format='[%(asctime)s] [%(name)s] %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p',
    level=LOG_LEVEL
)
logger = logging.getLogger('comma_traccar')

# Traccar Config
TRACCAR_URL = get_config('TRACCAR_URL', 'http://localhost:5055')
TRACCAR_DEVICE_ID = get_config('TRACCAR_DEVICE_ID', DONGLE_ID)
POLL_INTERVAL = get_config('LOCATION_POLL_INTERVAL', 60, type=int)

def get_location():
    url = f"https://api.commadotai.com/v1/devices/{DONGLE_ID}/location"
    try:
        return make_api_request(url)
    except Exception as e:
        logger.error(f"Error fetching location from Comma API: {e}")
        return None

def send_to_traccar(location):
    """Sends location data to Traccar using the OsmAnd protocol (HTTP GET)."""
    lat = location.get('lat')
    lng = location.get('lng')
    
    if lat is None or lng is None:
        logger.warning("Location data missing lat/lng, skipping Traccar update.")
        return False

    # OsmAnd protocol parameters
    params = {
        'id': TRACCAR_DEVICE_ID,
        'lat': lat,
        'lon': lng,
        'timestamp': int(location.get('time', time.time() * 1000) / 1000), # Traccar expects seconds
        'hdop': location.get('accuracy', 0),
        'speed': location.get('speed', 0),
        'bearing': location.get('bearing', 0),
        'altitude': location.get('altitude', 0)
    }

    try:
        # Some Traccar servers might prefer the params in the query string
        response = requests.get(TRACCAR_URL, params=params, timeout=10)
        response.raise_for_status()
        logger.info(f"Successfully sent location to Traccar: {lat}, {lng}")
        return True
    except Exception as e:
        logger.error(f"Failed to send location to Traccar ({TRACCAR_URL}): {e}")
        return False

def main():
    if not DONGLE_ID or DONGLE_ID == 'your_dongle_id_here':
        logger.error("COMMA_DONGLE_ID not set. Exiting.")
        return

    logger.info(f"Starting Traccar location uploader for device {TRACCAR_DEVICE_ID} to {TRACCAR_URL}")

    try:
        while True:
            location = get_location()
            if location:
                send_to_traccar(location)
            
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        logger.info("Stopping Traccar uploader...")

if __name__ == "__main__":
    main()
