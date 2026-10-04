import requests
from requests.adapters import HTTPAdapter, Retry
import logging
from comma_auth import CommaAuth
import os
import time
from datetime import datetime
from dotenv import load_dotenv

# Load configuration
load_dotenv(os.path.join(os.getcwd(), '.env'), override=True)
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'), override=True)

def get_config(key, fallback, type=str):
    val = os.environ.get(key)
    if val is not None:
        if type == bool:
            return val.lower() in ('true', '1', 't', 'y', 'yes')
        if type == int:
            try: return int(val)
            except ValueError: return fallback
        return val
    return fallback

DONGLE_ID = get_config('COMMA_DONGLE_ID', 'your_dongle_id_here')
HTTP_REQUEST_RETRIES = get_config('HTTP_REQUEST_RETRIES', 10, type=int)

# Initialize Auth
auth = CommaAuth(
    jwt_key=get_config('COMMA_JWT_KEY', None),
    github_user=get_config('GITHUB_USER', None),
    github_pass=get_config('GITHUB_PASS', None),
    cache_path=get_config('JWT_CACHE_PATH', '/data/jwt.cache')
)

logger = logging.getLogger('comma_api')

api_session = requests.Session()
retries = Retry(total=HTTP_REQUEST_RETRIES,
                backoff_factor=1,
                status_forcelist=[ 500, 502, 503, 504 ])
api_session.mount('https://', HTTPAdapter(max_retries=retries))

def make_api_request(url, raise_errors=True):
    """Makes an authenticated GET request to the Comma API with robust error handling and auto-refresh."""
    try:
        response = api_session.get(url, headers={'Authorization': auth.token}, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code
        if status_code == 401:
            logger.error("AUTHENTICATION ERROR: Your JWT token is expired or invalid.")
            if auth.handle_401():
                try:
                    # Retry once with the new token
                    response = api_session.get(url, headers={'Authorization': auth.token}, timeout=30)
                    response.raise_for_status()
                    return response.json()
                except Exception as retry_err:
                    logger.error(f"Retry failed after JWT refresh: {retry_err}")
        if raise_errors:
            if status_code == 403:
                logger.error(f"PERMISSION ERROR: Access forbidden (403). Check if Dongle ID {DONGLE_ID} is correct and accessible with your token.")
            elif status_code == 404:
                logger.error(f"NOT FOUND: The requested resource was not found (404). URL: {url}")
            else:
                logger.error(f"HTTP error {status_code} occurred: {e}")
            raise
        return None
    except requests.exceptions.Timeout:
        if raise_errors:
            logger.error(f"TIMEOUT: The request to {url} timed out.")
            raise
        return None
    except requests.exceptions.RequestException as e:
        if raise_errors:
            logger.error(f"NETWORK ERROR: A connection error occurred: {e}")
            raise
        return None

def get_device_location(dongle_id=None):
    """
    Fetches the best available location for the device using a multi-tiered approach:
    1. Direct /v1/devices/{dongle_id}/location (if Comma Prime is active)
    2. Athena RPC getMessage('gpsLocationExternal') (live GPS directly from device if online)
    3. Cached last_gps_* fields from /v1.1/devices/{dongle_id}/
    4. Parked end_lat/end_lng from latest drive in /v1/devices/{dongle_id}/routes
    """
    dev_id = dongle_id or DONGLE_ID
    if not dev_id or dev_id == 'your_dongle_id_here':
        logger.error("COMMA_DONGLE_ID not set.")
        return None

    # Step 1: Check device details for Prime status and cached GPS
    device_info = make_api_request(f"https://api.commadotai.com/v1.1/devices/{dev_id}/", raise_errors=False)

    # If Prime is active on the device, try the official /location endpoint
    if device_info and device_info.get('prime') is True:
        loc = make_api_request(f"https://api.commadotai.com/v1/devices/{dev_id}/location", raise_errors=False)
        if loc and loc.get('lat') is not None and loc.get('lng') is not None:
            return loc

    # Step 2: Try querying Athena RPC for real-time live GPS if device is online
    try:
        athena_url = f"https://athena.comma.ai/{dev_id}"
        athena_payload = {
            "method": "getMessage",
            "params": {"service": "gpsLocationExternal", "timeout": 3000},
            "jsonrpc": "2.0",
            "id": 0
        }
        resp = api_session.post(
            athena_url,
            headers={'Authorization': auth.token, 'Content-Type': 'application/json'},
            json=athena_payload,
            timeout=5
        )
        if resp.status_code == 200:
            res_json = resp.json()
            if 'result' in res_json and isinstance(res_json['result'], dict):
                gps = res_json['result'].get('gpsLocationExternal', {})
                lat = gps.get('latitude')
                lng = gps.get('longitude')
                if lat is not None and lng is not None and (lat != 0 or lng != 0):
                    logger.debug(f"Retrieved live GPS via Athena RPC: {lat}, {lng}")
                    return {
                        'lat': lat,
                        'lng': lng,
                        'speed': gps.get('speed', 0),
                        'bearing': gps.get('bearingDeg', 0),
                        'altitude': gps.get('altitude', 0),
                        'accuracy': gps.get('horizontalAccuracy', 0),
                        'time': gps.get('unixTimestampMillis', int(time.time() * 1000)),
                        'source': 'athena_live'
                    }
    except Exception as e:
        logger.debug(f"Athena RPC attempt failed: {e}")

    # Step 3: Check cached GPS in device_info (/v1.1/devices/{dev_id}/)
    if device_info:
        lat = device_info.get('last_gps_lat')
        lng = device_info.get('last_gps_lng')
        if lat is not None and lng is not None:
            logger.debug(f"Retrieved GPS from device metadata: {lat}, {lng}")
            return {
                'lat': lat,
                'lng': lng,
                'speed': device_info.get('last_gps_speed', 0),
                'bearing': device_info.get('last_gps_bearing', 0),
                'accuracy': device_info.get('last_gps_accuracy', 0),
                'altitude': 0,
                'time': device_info.get('last_gps_time', int(time.time() * 1000)),
                'source': 'device_cached'
            }

    # Step 4: Fallback to latest route's end_lat/end_lng (last parked position)
    try:
        routes_url = f"https://api.commadotai.com/v1/devices/{dev_id}/routes"
        routes = make_api_request(routes_url, raise_errors=False)
        if routes and isinstance(routes, list) and len(routes) > 0:
            latest = routes[0]
            lat = latest.get('end_lat')
            lng = latest.get('end_lng')
            if lat is not None and lng is not None:
                end_time_str = latest.get('end_time')
                t_ms = int(time.time() * 1000)
                if end_time_str:
                    try:
                        dt = datetime.fromisoformat(end_time_str)
                        t_ms = int(dt.timestamp() * 1000)
                    except Exception:
                        pass
                logger.debug(f"Retrieved GPS from latest route (parked): {lat}, {lng}")
                return {
                    'lat': lat,
                    'lng': lng,
                    'speed': 0,
                    'bearing': 0,
                    'altitude': 0,
                    'accuracy': 15,
                    'time': t_ms,
                    'source': 'last_route_parked'
                }
    except Exception as e:
        logger.error(f"Error fetching route fallback from Comma API: {e}")

    logger.warning("No location data could be retrieved from any source.")
    return None
