import requests
from requests.adapters import HTTPAdapter, Retry
import logging
from comma_auth import CommaAuth
import os
from dotenv import load_dotenv

# Load configuration
load_dotenv(os.path.join(os.getcwd(), '.env'))
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))

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

def make_api_request(url):
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
        elif status_code == 403:
            logger.error(f"PERMISSION ERROR: Access forbidden (403). Check if Dongle ID {DONGLE_ID} is correct and accessible with your token.")
        elif status_code == 404:
            logger.error(f"NOT FOUND: The requested resource was not found (404). URL: {url}")
        else:
            logger.error(f"HTTP error {status_code} occurred: {e}")
        raise
    except requests.exceptions.Timeout:
        logger.error(f"TIMEOUT: The request to {url} timed out.")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"NETWORK ERROR: A connection error occurred: {e}")
        raise
