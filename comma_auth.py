import logging
import time
import json
import base64
import os
from datetime import datetime, timedelta, UTC
from automate_login import get_jwt_via_playwright

logger = logging.getLogger('comma_auth')

class CommaAuth:
    """
    Manages Comma.ai JWT authentication with persistent caching and expiry checking.
    Order of precedence:
    1. COMMA_JWT_KEY from environment (if valid/not expired)
    2. Cached token from file (if valid/not expired)
    3. Fresh token via Playwright automation (if credentials available)
    """
    def __init__(self, jwt_key=None, github_user=None, github_pass=None, 
                 cache_path='/data/jwt.cache'):
        self.github_user = github_user
        self.github_pass = github_pass
        self.cache_path = cache_path
        
        # 1. Check Environment Token
        env_token = self._format_token(jwt_key)
        # Check if environment token is valid and has at least 1 week left
        if env_token and not self.is_token_expired(env_token, buffer_seconds=604800):
            logger.info("Using valid COMMA_JWT_KEY from environment (more than 1 week remaining).")
            self._jwt_key = env_token
        else:
            if env_token:
                logger.info("Environment token is missing, expired, or has less than 1 week remaining.")
            
            # 2. Check Cache Token
            cached_token = self._load_cache()
            if cached_token and not self.is_token_expired(cached_token, buffer_seconds=604800):
                logger.info("Using valid cached JWT token (more than 1 week remaining).")
                self._jwt_key = cached_token
            else:
                if cached_token:
                    logger.info("Cached token is expired or has less than 1 week remaining.")
                
                # 3. Initial Refresh via Automation
                self._jwt_key = None
                
                if self.github_user and self.github_pass:
                    logger.info("No valid token with sufficient remaining life found. Triggering startup JWT refresh...")
                    self.refresh()
                else:
                    # Fallback to whatever we have if automation isn't possible, 
                    # even if it's near expiry.
                    self._jwt_key = env_token or cached_token
                    if not self._jwt_key:
                        logger.warning("No token found and no GitHub credentials provided.")

    def _format_token(self, token):
        if token and token != 'your_jwt_key_here' and not token.startswith('JWT '):
            return f"JWT {token}"
        return token if token != 'your_jwt_key_here' else None

    def is_token_expired(self, token, buffer_seconds=300):
        """Checks if a JWT is expired or within the buffer period."""
        if not token or not token.startswith('JWT '):
            return True
        
        try:
            parts = token.split(' ')[1].split('.')
            if len(parts) != 3:
                return True
            
            payload_b64 = parts[1]
            missing_padding = len(payload_b64) % 4
            if missing_padding:
                payload_b64 += '=' * (4 - missing_padding)
            
            payload = json.loads(base64.b64decode(payload_b64).decode('utf-8'))
            exp = payload.get('exp')
            if not exp:
                return True
            
            return time.time() > (exp - buffer_seconds)
        except Exception as e:
            logger.error(f"Error checking token expiry: {e}")
            return True

    def _load_cache(self):
        """Loads the token from the cache file."""
        if os.path.exists(self.cache_path):
            try:
                with open(self.cache_path, 'r') as f:
                    token = f.read().strip()
                    return self._format_token(token)
            except Exception as e:
                logger.error(f"Failed to load token cache: {e}")
        return None

    def _save_cache(self, token):
        """Saves the token to the cache file."""
        try:
            os.makedirs(os.path.dirname(self.cache_path), exist_ok=True)
            with open(self.cache_path, 'w') as f:
                f.write(token)
            logger.info(f"JWT token cached to {self.cache_path}")
        except Exception as e:
            logger.error(f"Failed to save token cache: {e}")

    def refresh(self):
        """Fetches a new JWT token from https://jwt.comma.ai/ via Playwright automation."""
        if self.github_user and self.github_pass:
            logger.info("Refreshing JWT token via Playwright automation...")
            start_time = time.time()
            new_jwt = get_jwt_via_playwright(self.github_user, self.github_pass)
            duration = time.time() - start_time
            
            if new_jwt:
                self._jwt_key = f"JWT {new_jwt}"
                self._save_cache(self._jwt_key)
                
                masked_token = f"{new_jwt[:10]}...{new_jwt[-10:]}"
                logger.info(f"Successfully refreshed and cached JWT token in {duration:.2f}s. Token: {masked_token}")
                return True
            else:
                logger.error(f"Playwright automation failed to retrieve JWT after {duration:.2f}s.")
        else:
            logger.warning("Attempted to refresh JWT without GitHub credentials.")

        return False

    @property
    def token(self):
        """Returns the current JWT token, triggering a refresh if it has less than 1 week remaining."""
        if (self.github_user and self.github_pass):
            # Refresh if the token has less than 1 week (604800 seconds) remaining
            if self.is_token_expired(self._jwt_key, buffer_seconds=604800):
                logger.info("JWT token has less than 1 week remaining. Refreshing...")
                self.refresh()
        return self._jwt_key

    def handle_401(self):
        """Force an immediate refresh in response to an authentication error."""
        if self.github_user and self.github_pass:
            logger.info("401 Unauthorized received. Triggering immediate JWT refresh...")
            return self.refresh()
        return False
