import os
import time
import logging
from playwright.sync_api import sync_playwright

logger = logging.getLogger('comma_auth_automation')

def get_jwt_via_playwright(github_user, github_pass):
    """
    Automates the GitHub login flow on https://jwt.comma.ai/ using Playwright.
    Returns the raw JWT string.
    """
    if not github_user or not github_pass:
        logger.error("GitHub credentials not provided for automation.")
        return None

    logger.info("Starting Playwright automation for GitHub login...")
    
    with sync_playwright() as p:
        # Docker/Alpine compatibility: Use system Chromium and --no-sandbox
        # executable_path defaults to PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH if set
        executable_path = os.environ.get("PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH")
        
        browser = p.chromium.launch(
            headless=True,
            executable_path=executable_path,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]
        )
        context = browser.new_context()
        page = context.new_page()

        try:
            # 1. Go to the Comma JWT portal
            logger.info("Navigating to https://jwt.comma.ai/...")
            page.goto("https://jwt.comma.ai/", wait_until="networkidle")
            
            # 2. Click "Login with GitHub"
            # The button usually has "GitHub" in the text or is an <a> with a specific href
            github_login_button = page.get_by_role("link", name="GitHub")
            if not github_login_button.is_visible():
                 # Fallback: search for any link containing github
                 github_login_button = page.locator("a[href*='github']")
            
            logger.info("Clicking GitHub login button...")
            github_login_button.click()
            page.wait_for_load_state("networkidle")

            # 3. Handle GitHub Login Page
            if "github.com/login" in page.url or page.locator("#login_field").is_visible():
                logger.info(f"GitHub login page detected ({page.url}). Entering credentials for user: {github_user}...")
                page.fill("#login_field", github_user)
                page.fill("#password", github_pass)
                page.click("input[type='submit']")
                page.wait_for_load_state("networkidle")
            else:
                logger.info(f"Skipped login fields, current URL: {page.url}")

            # 4. Handle GitHub Authorization (if prompted)
            # Sometimes GitHub asks to "Authorize commaai"
            if "oauth/authorize" in page.url or page.get_by_role("button", name="Authorize commaai").is_visible():
                logger.info("GitHub Authorization screen detected. Clicking Authorize...")
                page.get_by_role("button", name="Authorize commaai").click()
                page.wait_for_load_state("networkidle")

            # 5. Extract the JWT
            # The JWT is inside an <input name="jwt"> element.
            logger.info("Waiting for redirect back to https://jwt.comma.ai/ and for the JWT input to appear...")
            page.wait_for_url("https://jwt.comma.ai/**", timeout=60000)
            
            try:
                # Wait for the input named "jwt" to be present
                jwt_input = page.wait_for_selector("input[name='jwt']", timeout=20000)
                jwt_token = jwt_input.get_attribute("value")
                
                if jwt_token and jwt_token.count('.') == 2:
                    masked_token = f"{jwt_token[:10]}...{jwt_token[-10:]}"
                    logger.info(f"Successfully extracted JWT from input field. Token: {masked_token}")
                    return jwt_token
                else:
                    logger.error(f"JWT input found but value is invalid. Value: {jwt_token[:30] if jwt_token else 'None'}...")
            except Exception as e:
                logger.warning(f"Could not find input[name='jwt'] via selector: {e}. Falling back to regex on body...")

            # Fallback to regex if the specific input isn't found
            import re
            body_text = page.inner_text("body").strip()
            jwt_pattern = r'([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)'
            jwt_match = re.search(jwt_pattern, body_text)

            if jwt_match:
                jwt_token = jwt_match.group(1)
                masked_token = f"{jwt_token[:10]}...{jwt_token[-10:]}"
                logger.info(f"Successfully extracted JWT via body regex. Token: {masked_token}")
                return jwt_token
            else:
                logger.error(f"Failed to find JWT in page content. Full body text length: {len(body_text)}")
                # Save screenshot for debugging
                try:
                    page.screenshot(path="/data/login_failed_final.png")
                    logger.info("Saved error screenshot to /data/login_failed_final.png")
                except Exception:
                    pass
                return None

        except Exception as e:
            logger.error(f"Playwright automation failed during navigation/interaction: {e}")
            logger.debug(f"Current page URL: {page.url}")
            # Save screenshot for debugging
            try:
                page.screenshot(path="/data/login_failed_exception.png")
                logger.info("Saved error screenshot to /data/login_failed_exception.png")
            except Exception:
                pass
            return None
        finally:
            browser.close()

if __name__ == "__main__":
    # Test script (requires GH_USER and GH_PASS env vars)
    logging.basicConfig(level=logging.INFO)
    user = os.getenv("GH_USER")
    pw = os.getenv("GH_PASS")
    token = get_jwt_via_playwright(user, pw)
    if token:
        print(f"JWT: {token}")
    else:
        print("Failed to get token.")
