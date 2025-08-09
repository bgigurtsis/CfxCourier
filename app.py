# app.py
import os, json, time, random, urllib.parse, uuid
from pathlib import Path

import boto3
from playwright.sync_api import Page, expect
from camoufox.sync_api import Camoufox

# --------- Config via env ---------
CFX_USERNAME = os.getenv("CFX_USERNAME") 
CFX_PASSWORD = os.getenv("CFX_PASSWORD") 

PROXY_SERVER   = os.getenv("PROXY_SERVER") 
PROXY_USERNAME = os.getenv("PROXY_USERNAME")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")

S3_BUCKET  = os.getenv("S3_BUCKET") 
OUTPUT_PREFIX  = os.getenv("OUTPUT_PREFIX", "processed/")
INPUT_PREFIX   = os.getenv("INPUT_PREFIX", "unprocessed/")
OS_FINGERPRINT = "windows" 
LOCALE         = "en-GB"

# Lambda writable area
TMP_DIR = Path("/tmp")
TMP_DIR.mkdir(exist_ok=True)

# --------- Helpers ---------
def human_delay(min_seconds=1.2, max_seconds=2.6):
    time.sleep(random.uniform(min_seconds, max_seconds))

def type_like_human(el, text: str):
    el.click()
    human_delay(0.3, 0.7)
    for ch in text:
        el.type(ch, delay=random.uniform(70, 200))
    human_delay(0.3, 0.7)

def perform_login(page: Page, username: str, password: str):
    print("-> Performing login...")
    page.goto("https://portal.cfx.re/assets/created-assets?modal=create",
              wait_until="domcontentloaded", timeout=60000)
    human_delay()

    signin_button = page.get_by_role("button", name="Sign in with Cfx.re")
    expect(signin_button).to_be_visible(timeout=15000)
    signin_button.click()
    human_delay()

    username_field = page.locator("#login-account-name")
    password_field = page.locator("#login-account-password")

    expect(username_field).to_be_visible()
    type_like_human(username_field, username)

    expect(password_field).to_be_visible()
    type_like_human(password_field, password)

    login_button = page.locator("#login-button")
    expect(login_button).to_be_enabled()
    login_button.click()
    print("   Final login submitted.")

def run_asset_flow(page: Page, file_to_upload: Path) -> Path:
    # Right now it creates a unique asset name assuming test data. Prod would use zip filename.
    base_asset_name = os.getenv("BASE_ASSET_NAME", "TestAsset")
    asset_name = f"{base_asset_name}_{int(time.time())}"
    print(f"-> Using unique asset name: {asset_name}")

    print("Navigating to asset creation page...")
    page.goto("https://portal.cfx.re/assets/created-assets?modal=create",
              wait_until="domcontentloaded", timeout=60000)
    human_delay(1.5, 3)

    asset_name_field = page.get_by_placeholder("Enter asset name")
    try:
        expect(asset_name_field).to_be_visible(timeout=7000)
        print("✅ Already logged in. Proceeding to upload.")
    except Exception:
        print("Login required. Starting login flow...")
        perform_login(page, CFX_USERNAME, CFX_PASSWORD)
        expect(asset_name_field).to_be_visible(timeout=25000)

    print("-> Filling out the asset upload modal...")
    type_like_human(asset_name_field, asset_name)

    print(f"   Preparing to upload file: {file_to_upload}")
    file_input = page.locator("input[type='file']").first
    if file_input.count() > 0:
        file_input.set_input_files(str(file_to_upload))
    else:
        with page.expect_file_chooser() as fc_info:
            dropzone = page.locator(".cfxui__InputDropzone__dropzone__bde8d")
            expect(dropzone).to_be_visible()
            human_delay()
            dropzone.click()
        file_chooser = fc_info.value
        file_chooser.set_files(str(file_to_upload))

    print("   File selected.")
    human_delay(0.8, 1.6)

    upload_button = page.get_by_role("button", name="Upload File")
    expect(upload_button).to_be_enabled()
    upload_button.click()
    print("-> 'Upload File' clicked. Waiting for upload to finish...")

    expect(upload_button).to_be_hidden(timeout=90000)
    print("   Upload complete, modal closed.")
    human_delay()

    print(f"-> Locating asset '{asset_name}' and waiting for processing...")
    asset_row = page.locator(f"tr:has-text('{asset_name}')")
    expect(asset_row).to_be_visible(timeout=30000)

    download_button = asset_row.locator('[data-sentry-component="DownloadButton"]')
    expect(download_button).to_be_enabled(timeout=120000)
    print("   Asset processed! Download button is enabled.")

    # Download into /tmp/output_<uuid>.zip
    output_path = TMP_DIR / f"download_{uuid.uuid4().hex}.zip"
    print("-> Downloading the processed asset...")
    with page.expect_download() as download_info:
        download_button.click()
    download = download_info.value
    download.save_as(str(output_path))
    print(f"   Download complete! File saved to: {output_path}")

    return output_path

def process_with_camoufox(upload_zip: Path) -> Path:
    proxy = None
    if PROXY_SERVER:
        proxy = {"server": PROXY_SERVER}
        if PROXY_USERNAME and PROXY_PASSWORD:
            proxy["username"] = PROXY_USERNAME
            proxy["password"] = PROXY_PASSWORD

    with Camoufox(
        headless=True,
        os=OS_FINGERPRINT,
        locale=LOCALE,
        geoip=True,
        proxy=proxy,
        window=(1920, 1080),
    ) as browser:
        page = browser.new_page()
        return run_asset_flow(page, upload_zip)

# --------- Lambda Handler ---------
def handler(event, context):
    s3 = boto3.client("s3")
    results = []

    for rec in event.get("Records", []):
        bucket = rec["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(rec["s3"]["object"]["key"])

        # Skip our own outputs to avoid re-trigger loops if same bucket is used
        if OUTPUT_PREFIX and key.startswith(OUTPUT_PREFIX):
            print(f"Skipping output object: {key}")
            continue

        # If an input prefix is set, only process matching keys
        if INPUT_PREFIX and not key.startswith(INPUT_PREFIX):
            print(f"Key not under INPUT_PREFIX ({INPUT_PREFIX}): {key} – skipping")
            continue

        # Compute relative path under the input prefix
        rel = key[len(INPUT_PREFIX):] if INPUT_PREFIX else key

        # Download → process → upload
        in_path = TMP_DIR / f"input_{uuid.uuid4().hex}.zip"
        s3.download_file(bucket, key, str(in_path))

        out_path = process_with_camoufox(in_path)

        out_bucket = S3_BUCKET or bucket
        out_key = f"{OUTPUT_PREFIX}{rel}"  # preserves subfolders/filename
        s3.upload_file(str(out_path), out_bucket, out_key)

        results.append({"in": f"s3://{bucket}/{key}", "out": f"s3://{out_bucket}/{out_key}"})

    return {"statusCode": 200, "body": json.dumps({"processed": results})}
