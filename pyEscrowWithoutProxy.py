import time
import random
import re
import json
from pathlib import Path
from playwright.sync_api import Page, expect
from camoufox.sync_api import Camoufox
from camoufox.utils import launch_options

# --- ⚙️ CONFIGURATION ---
USERNAME = "shirahama"
PASSWORD = "banshi21"

FILE_TO_UPLOAD = Path(r"C:\Users\Billy\GitHub\cfxtest\build\test.zip")
DOWNLOAD_PATH  = Path(r"C:\Users\Billy\GitHub\cfxtest\artifact\download.zip")
STATE_FILE     = Path(r"C:\Users\Billy\GitHub\cfxtest\pyEscrow\state.json")
CFG_PATH       = Path(r"C:\Users\Billy\GitHub\cfxtest\pyEscrow\camoufox-config.json")
PROFILE_DIR    = Path(r"C:\Users\Billy\GitHub\cfxtest\pyEscrow\profile")

# The base name for the asset. A timestamp will be added to make it unique.
BASE_ASSET_NAME = "TestAsset"

# --- PROXY CONFIGURATION ---
# Define the proxy configuration at the top so it's always available.
proxy_config = {
    "server": "http://5.tcp.eu.ngrok.io:18949",
    "username": "test",
    "password": "test",
}

# Validate file exists
if not FILE_TO_UPLOAD.exists():
    raise FileNotFoundError(f"Upload file not found: {FILE_TO_UPLOAD}")

# Ensure folders exist
CFG_PATH.parent.mkdir(parents=True, exist_ok=True)
PROFILE_DIR.mkdir(parents=True, exist_ok=True)
DOWNLOAD_PATH.parent.mkdir(parents=True, exist_ok=True)
STATE_FILE.parent.mkdir(parents=True, exist_ok=True)

# Stable Camoufox config (create once, then reuse)
if not CFG_PATH.exists():
    # Now proxy_config is defined and can be used here.
    cfg = launch_options(
        headless=False,
        os="windows",
        locale="en-GB",
        window=(1920, 1080),
        geoip=True,
        proxy=proxy_config,
        user_data_dir=str(PROFILE_DIR)
    )
    CFG_PATH.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
else:
    cfg = json.loads(CFG_PATH.read_text(encoding="utf-8"))


def human_delay(min_seconds=1.5, max_seconds=3.5):
    time.sleep(random.uniform(min_seconds, max_seconds))

def type_like_human(el, text: str):
    el.click()
    human_delay(0.5, 1)
    for ch in text:
        el.type(ch, delay=random.uniform(70, 200))
    human_delay(0.5, 1)

def perform_login(page: Page):
    print("-> Performing login...")
    # Increased timeout to 60 seconds for page navigation.
    page.goto("https://portal.cfx.re/login", wait_until='networkidle', timeout=60000)
    human_delay()

    signin_button = page.get_by_role("button", name="Sign in with Cfx.re")
    expect(signin_button).to_be_visible(timeout=15000)
    signin_button.click()
    print("   Initial sign-in button clicked.")
    human_delay()

    username_field = page.locator("#login-account-name")
    password_field = page.locator("#login-account-password")

    expect(username_field).to_be_visible()
    type_like_human(username_field, USERNAME)

    expect(password_field).to_be_visible()
    type_like_human(password_field, PASSWORD)

    login_button = page.locator("#login-button")
    expect(login_button).to_be_enabled()
    box = login_button.bounding_box()
    if box:
        page.mouse.move(box['x'] + box['width'] / 2, box['y'] + box['height'] / 2, steps=5)
    login_button.click()
    print("   Final login submitted.")

def run_asset_flow(page: Page):
    # Generate a unique asset name for this run to avoid locator conflicts.
    asset_name = f"{BASE_ASSET_NAME}_{int(time.time())}"
    print(f"-> Using unique asset name: {asset_name}")

    print("Navigating to asset creation page...")
    # Increased timeout to 60 seconds to allow for proxy latency.
    page.goto("https://portal.cfx.re/assets/created-assets?modal=create", wait_until='networkidle', timeout=60000)
    human_delay(2, 4)

    asset_name_field = page.get_by_placeholder("Enter asset name")
    try:
        expect(asset_name_field).to_be_visible(timeout=7000)
        print("✅ Already logged in. Proceeding to upload.")
    except Exception:
        print("Login required. Starting login flow...")
        perform_login(page)
        expect(asset_name_field).to_be_visible(timeout=25000)

    print("-> Filling out the asset upload modal...")
    type_like_human(asset_name_field, asset_name)

    print(f"   Preparing to upload file: {FILE_TO_UPLOAD}")
    file_input = page.locator("input[type='file']").first
    if file_input.count() > 0:
        file_input.set_input_files(str(FILE_TO_UPLOAD))
    else:
        with page.expect_file_chooser() as fc_info:
            dropzone = page.locator(".cfxui__InputDropzone__dropzone__bde8d")
            expect(dropzone).to_be_visible()
            human_delay()
            dropzone.click()
        file_chooser = fc_info.value
        file_chooser.set_files(str(FILE_TO_UPLOAD))

    print("   File selected.")
    human_delay(1, 2)

    upload_button = page.get_by_role("button", name="Upload File")
    expect(upload_button).to_be_enabled()
    upload_button.click()
    print("-> 'Upload File' button clicked. Waiting for upload to finish...")

    expect(upload_button).to_be_hidden(timeout=90000)
    print("   Upload complete, modal closed.")
    human_delay()

    print(f"-> Locating asset '{asset_name}' and waiting for it to finish processing...")
    # Because the asset name is now unique, this locator will resolve to a single row.
    asset_row = page.locator(f"tr:has-text('{asset_name}')")
    expect(asset_row).to_be_visible(timeout=30000)

    download_button = asset_row.locator('[data-sentry-component="DownloadButton"]')
    expect(download_button).to_be_enabled(timeout=120000)
    print("   Asset processed! Download button is enabled.")
    human_delay()

    print("-> Attempting to download the asset for verification...")
    with page.expect_download() as download_info:
        download_button.click()

    download = download_info.value
    download.save_as(str(DOWNLOAD_PATH))
    print(f"   Download complete! File saved to: {download.path()}")


def main():
    """
    Before first run:
        pip install -U camoufox
        camoufox fetch
    """
    with Camoufox(
        headless=False, # Can now run in headless mode
        os="windows",
        locale="en-GB",
        window=(1920, 1080),
        geoip=True,
        user_data_dir=str(PROFILE_DIR),
        proxy=proxy_config,
        config={},
        persistent_context=True,
    ) as context:
        page = context.new_page()
        try:
            run_asset_flow(page)
            context.storage_state(path=str(STATE_FILE))
            print(f"✅ Automation successful. Session saved to '{STATE_FILE}'.")
        except Exception as e:
            print(f"❌ An error occurred: {e}")
        finally:
            print("-> Closing browser.")

if __name__ == "__main__":
    main()
