import os, sys, json, time, random, urllib.parse, uuid, asyncio, traceback, pathlib, signal, re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict
from types import SimpleNamespace

import boto3
import requests
from botocore.config import Config
from playwright.async_api import Page, expect, Browser, BrowserContext
from camoufox.async_api import AsyncCamoufox
from camoufox import DefaultAddons

# ================
# Env / Config
# ================

# Cfx Vars
CFX_USERNAME = os.getenv("CFX_USERNAME")
CFX_PASSWORD = os.getenv("CFX_PASSWORD")

# Proxy Vars
PROXY_SERVER   = os.getenv("PROXY_SERVER")
PROXY_USERNAME = os.getenv("PROXY_USERNAME")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")

# S3 and Camouxfox Vars
S3_BUCKET      = os.getenv("S3_BUCKET")
OUTPUT_PREFIX  = os.getenv("OUTPUT_PREFIX", "processed/")
INPUT_PREFIX   = os.getenv("INPUT_PREFIX",  "unprocessed/")
OS_FINGERPRINT = os.getenv("CAMOUFOX_OS", "windows")
LOCALE         = os.getenv("CAMOUFOX_LOCALE", "en-GB")

# Log Vars
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
DEBUG = os.getenv("DEBUG", "1") == "1"
DEBUG_BUCKET = os.getenv("S3_BUCKET")  # defaulted in code below
DEBUG_PREFIX = os.getenv("DEBUG_PREFIX", "debug/")
DEBUG_UPLOAD_ON_SUCCESS = os.getenv("DEBUG_UPLOAD_ON_SUCCESS", "0") == "1"

# Misc. Vars
MAX_PARALLEL = 1  # Always force to 1 for persistent browser
DISABLE_HUMAN_DELAYS = os.getenv("DISABLE_HUMAN_DELAYS", "0") == "1"  # New: option to disable delays
BROWSER_RESTART_AFTER = int(os.getenv("BROWSER_RESTART_AFTER", "50"))  # Restart browser after N requests

# Runtime mode:
#  - "http": FastAPI server (ECS behind ALB; also easy local testing)
#  - "sqs" : FIFO SQS worker (long-poll queue, no HTTP)
MODE = os.getenv("MODE", "http").lower()

# SQS settings (used only in MODE="sqs")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")
SQS_WAIT_TIME_SECONDS = int(os.getenv("SQS_WAIT_TIME_SECONDS", "20"))
SQS_MAX_MESSAGES = int(os.getenv("SQS_MAX_MESSAGES", "10"))              # API cap = 10
SQS_VISIBILITY_TIMEOUT = int(os.getenv("SQS_VISIBILITY_TIMEOUT", "600")) # seconds

TMP_DIR = Path("/tmp"); TMP_DIR.mkdir(exist_ok=True)

# boto3 clients (we'll call sync methods via asyncio.to_thread)

session = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    aws_session_token=os.getenv("AWS_SESSION_TOKEN"),  # fine if None
    region_name=os.getenv("AWS_DEFAULT_REGION"),  # fine if None
)

s3 = session.client(
    "s3",
    config=Config(
        signature_version="s3v4",
        s3={"addressing_style": "virtual"},  # important for presigned URLs
    ),
)

sqs = session.client("sqs")  # inherits region/creds from the session

# S3 sanity checking
print("S3 endpoint:", s3.meta.endpoint_url)
creds = s3._request_signer._credentials
print("Using access key:", creds.access_key[:4] + "…", "session?", bool(getattr(creds, "token", None)))


# ================
# Browser Pool Management
# ================
class BrowserPool:
    def __init__(self):
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self.logged_in = False
        self.request_count = 0
        self.lock = asyncio.Lock()
        
    async def get_page(self) -> Page:
        """Get or create a browser page, handling initialization and health checks."""
        async with self.lock:
            # Check if we need to restart the browser
            if self.request_count >= BROWSER_RESTART_AFTER and self.browser:
                log("INFO", "browser_pool:restart", reason="request_limit", count=self.request_count)
                await self.close()
                self.request_count = 0
                self.logged_in = False
            
            # Initialize browser if needed
            if not self.browser or not self.page:
                await self._initialize_browser()
            
            # Health check - verify page is responsive
            try:
                # Simple health check to ensure page is alive
                await asyncio.wait_for(self.page.evaluate("() => true"), timeout=5.0)
                # Also check if we're not on an error page
                current_url = self.page.url
                if "error" in current_url.lower() or "404" in current_url:
                    raise Exception(f"Browser on error page: {current_url}")
            except Exception as e:
                log("WARNING", "browser_pool:health_check_failed", reason="page_unresponsive", error=str(e))
                await self.close()
                await self._initialize_browser()
            
            self.request_count += 1
            return self.page
    
    async def _initialize_browser(self):
        """Initialize browser, context, and page."""
        log("INFO", "browser_pool:init:start")
        
        proxy = None
        if PROXY_SERVER:
            proxy = {"server": PROXY_SERVER}
            if PROXY_USERNAME and PROXY_PASSWORD:
                proxy["username"] = PROXY_USERNAME
                proxy["password"] = PROXY_PASSWORD
        
        self.browser = await AsyncCamoufox(
            headless=True,
            os=OS_FINGERPRINT,
            locale=LOCALE,
            geoip="103.7.205.5",
            proxy=proxy,
            window=(1920, 1080),
            exclude_addons=[DefaultAddons.UBO],
        ).start()
        
        self.context = await self.browser.new_context()
        self.page = await self.context.new_page()
        
        # Set up event handlers
        self.page.on("console", lambda m: log("DEBUG", "page.console", type=m.type, text=m.text[:200]))
        self.page.on("pageerror", lambda e: log("WARNING", "page.error", error=str(e)))
        
        log("INFO", "browser_pool:init:complete")
    
    async def mark_logged_in(self):
        """Mark that we've successfully logged in."""
        self.logged_in = True
    
    async def is_logged_in(self) -> bool:
        """Check if we're still logged in."""
        return self.logged_in
    
    async def close(self):
        """Close browser resources."""
        if self.page:
            try:
                await self.page.close()
            except Exception:
                pass
        if self.context:
            try:
                await self.context.close()
            except Exception:
                pass
        if self.browser:
            try:
                await self.browser.close()
            except Exception:
                pass
        self.browser = None
        self.context = None
        self.page = None
        self.logged_in = False
        log("INFO", "browser_pool:closed")

# Global browser pool
browser_pool = BrowserPool()

# ================
# Configuration Validation
# ================
def validate_config():
    """Validate required environment variables based on mode."""
    errors = []
    
    # Always required for actual processing
    if not CFX_USERNAME:
        errors.append("CFX_USERNAME is required")
    if not CFX_PASSWORD:
        errors.append("CFX_PASSWORD is required")
    
    # Mode-specific validation
    if MODE == "sqs" and not SQS_QUEUE_URL:
        errors.append("SQS_QUEUE_URL is required in SQS mode")
    
    # Proxy validation
    if PROXY_SERVER and not PROXY_SERVER.startswith(("http://", "https://")):
        errors.append("PROXY_SERVER must start with http:// or https://")
    
    if errors:
        for err in errors:
            print(f"CONFIG ERROR: {err}", file=sys.stderr)
        raise ValueError(f"Configuration validation failed: {'; '.join(errors)}")

# ================
# Structured Logger
# ================
LEVELS = {"DEBUG":10,"INFO":20,"WARNING":30,"ERROR":40,"CRITICAL":50}
MIN_LEVEL = LEVELS.get(LOG_LEVEL, 20)
_current_ctx = {"request_id": None, "s3_bucket": None, "s3_key": None}

def _ts():
    return datetime.now(timezone.utc).isoformat()

def _redact(v: Optional[str], keep_last=4):
    if not v: return None
    return "***" + v[-keep_last:] if len(v) > keep_last else "***"

def log(level: str, msg: str, **kv):
    if LEVELS[level] < MIN_LEVEL: return
    rec = {
        "ts": _ts(),
        "level": level,
        "message": msg,
        "request_id": _current_ctx.get("request_id"),
        "s3_bucket": _current_ctx.get("s3_bucket"),
        "s3_key": _current_ctx.get("s3_key"),
    }
    rec.update(kv)
    print(json.dumps(rec, default=str), flush=True)

class Timer:
    def __init__(self, name, **fields):
        self.name = name
        self.fields = fields
        self.t0 = None
    async def __aenter__(self):
        self.t0 = time.perf_counter()
        log("INFO", f"{self.name}:start", **self.fields)
        return self
    async def __aexit__(self, exc_type, exc, tb):
        dur = time.perf_counter() - self.t0
        status = "ok" if exc is None else "error"
        log("INFO", f"{self.name}:end", duration_ms=int(dur*1000), status=status, **self.fields)

def _norm_prefix(p: Optional[str]) -> str:
    p = (p or "").lstrip("/")
    if p and not p.endswith("/"): p += "/"
    return p

OUTPUT_PREFIX = _norm_prefix(OUTPUT_PREFIX)
INPUT_PREFIX  = _norm_prefix(INPUT_PREFIX)
DEBUG_PREFIX  = _norm_prefix(DEBUG_PREFIX)

# ================
# S3 helpers (async wrappers)
# ================
async def s3_download(bucket: str, key: str, dest_path: Path):
    size = None
    log("INFO", "s3.download:start", bucket=bucket, key=key)
    try:
        await asyncio.to_thread(s3.download_file, bucket, key, str(dest_path))
        if dest_path.exists(): size = dest_path.stat().st_size
        log("INFO", "s3.download:end", bucket=bucket, key=key, size=size)
    except Exception as e:
        log("ERROR", "s3.download:error", bucket=bucket, key=key, error=str(e))
        raise

async def s3_upload(src_path: Path, bucket: str, key: str):
    size = src_path.stat().st_size if src_path.exists() else None
    log("INFO", "s3.upload:start", bucket=bucket, key=key, size=size)
    try:
        await asyncio.to_thread(s3.upload_file, str(src_path), bucket, key)
        log("INFO", "s3.upload:end", bucket=bucket, key=key, size=size)
    except Exception as e:
        log("ERROR", "s3.upload:error", bucket=bucket, key=key, error=str(e))
        raise

async def generate_presigned_url(bucket: str, key: str, expiration=3600):
    """Generates a presigned URL for an S3 object."""
    log("INFO", "s3.generate_presigned_url:start", bucket=bucket, key=key)
    try:
        url = await asyncio.to_thread(
            s3.generate_presigned_url,
            'get_object',
            Params={'Bucket': bucket, 'Key': key},
            ExpiresIn=expiration
        )
        log("INFO", "s3.generate_presigned_url:end", bucket=bucket, key=key)
        return url
    except Exception as e:
        log("ERROR", "s3.generate_presigned_url:error", error=str(e))
        return None

# ================
# Notifications
# ================
async def send_discord_notification(webhook_url: str, message: str):
    """Sends a message to a Discord webhook (sync requests in thread)."""
    log("INFO", "discord.notification:start")
    try:
        resp = await asyncio.to_thread(requests.post, webhook_url, json={"content": message})
        resp.raise_for_status()
        log("INFO", "discord.notification:end", status=resp.status_code)
    except Exception as e:
        log("ERROR", "discord.notification:error", error=str(e))

# ================
# Human-like helpers
# ================
async def human_delay(min_seconds=1.2, max_seconds=2.6):
    if not DISABLE_HUMAN_DELAYS:
        await asyncio.sleep(random.uniform(min_seconds, max_seconds))

async def type_like_human(el, text: str):
    await el.click()
    await human_delay(0.3, 0.7)
    if DISABLE_HUMAN_DELAYS:
        await el.fill(text)
    else:
        for ch in text:
            await el.type(ch, delay=random.uniform(70, 200))
    await human_delay(0.3, 0.7)

# ================
# Site automation (Camoufox)
# ================
async def perform_login(page: Page, username: str, password: str):
    """Perform login and verify success."""
    log("INFO", "login:begin")
    
    # Check if we're already on the login page
    if "sign-in" not in page.url:
        # Navigate to login page via button click
        signin_button = page.get_by_role("button", name="Sign in with Cfx.re")
        await expect(signin_button).to_be_visible(timeout=15000)
        await signin_button.click()
        await human_delay()

    # Clear and fill login fields
    username_field = page.locator("#login-account-name")
    password_field = page.locator("#login-account-password")
    
    await expect(username_field).to_be_visible(timeout=10000)
    await username_field.clear()
    await type_like_human(username_field, username)
    
    await expect(password_field).to_be_visible()
    await password_field.clear()
    await type_like_human(password_field, password)

    login_button = page.locator("#login-button")
    await expect(login_button).to_be_enabled()
    await login_button.click()
    log("INFO", "login:submitted")
    
    # Verify login success
    try:
        # Wait for redirect back to assets page
        # Use a more flexible URL pattern to handle different portal URLs
        await page.wait_for_url("**/portal.cfx.re/**", timeout=30000)
        # Verify we can see expected elements
        asset_elements = page.locator('[data-sentry-component="AssetRow"], .cfxui__InputDropzone__dropzone__bde8d, input[placeholder*="asset"]')
        await expect(asset_elements.first).to_be_visible(timeout=10000)
        log("INFO", "login:success")
        await browser_pool.mark_logged_in()
    except Exception as e:
        # Check for common login failure indicators
        error_element = page.locator(".error-message, .alert-danger, [role='alert']")
        if await error_element.count() > 0:
            error_text = await error_element.first.text_content()
            raise Exception(f"Login failed: {error_text}")
        raise Exception(f"Login verification failed: {str(e)}")

async def navigate_to_upload_modal(page: Page):
    """Navigate to the upload modal, handling login if needed."""
    target_url = "https://portal.cfx.re/assets/created-assets?modal=create"
    
    # Navigate to the target URL
    if not page.url.startswith("https://portal.cfx.re"):
        await page.goto(target_url, wait_until="domcontentloaded", timeout=60000)
        await human_delay(1.5, 3)
    elif "modal=create" not in page.url:
        # We're on the portal but not on the upload modal
        await page.goto(target_url, wait_until="domcontentloaded", timeout=30000)
        await human_delay()
    
    # Check if we need to log in
    asset_name_field = page.get_by_placeholder("Enter asset name")
    signin_button = page.get_by_role("button", name="Sign in with Cfx.re")
    
    # First check if sign in button is visible (indicates we're not logged in)
    signin_visible = False
    try:
        signin_visible = await signin_button.is_visible()
    except:
        pass
    
    if signin_visible:
        log("INFO", "login:required")
        await perform_login(page, CFX_USERNAME, CFX_PASSWORD)
        # After login, we should be redirected to the upload modal
        await expect(asset_name_field).to_be_visible(timeout=25000)
        return
    
    # Check if asset name field is visible (indicates we're logged in)
    try:
        await expect(asset_name_field).to_be_visible(timeout=7000)
        log("INFO", "login:already_authenticated")
    except Exception:
        # Can't see the form, might be a session issue or different page
        if await browser_pool.is_logged_in():
            # We think we're logged in but can't see the form, try reloading
            log("WARNING", "login:session_expired", action="reloading")
            await page.reload()
            await human_delay()
            
            # Check again after reload
            try:
                await expect(asset_name_field).to_be_visible(timeout=7000)
                return
            except:
                # Still can't see it, force re-login
                browser_pool.logged_in = False
        
        # Navigate to trigger login flow
        await page.goto(target_url, wait_until="domcontentloaded", timeout=30000)
        await human_delay()
        
        # Should now see either login button or asset field
        signin_visible = False
        try:
            signin_visible = await signin_button.is_visible()
        except:
            pass
            
        if signin_visible:
            await perform_login(page, CFX_USERNAME, CFX_PASSWORD)
            await expect(asset_name_field).to_be_visible(timeout=25000)

async def run_asset_flow(page: Page, file_to_upload: Path) -> Path:
    # Validate file exists
    if not file_to_upload.exists():
        raise FileNotFoundError(f"Upload file not found: {file_to_upload}")
    
    base_asset_name = os.getenv("BASE_ASSET_NAME", "TestAsset")
    # Use more unique identifier to avoid collisions
    asset_name = f"{base_asset_name}_{int(time.time())}_{uuid.uuid4().hex[:8]}"
    log("INFO", "asset_flow:start", asset_name=asset_name, upload=str(file_to_upload))

    try:
        # Navigate to upload modal (handles login if needed)
        await navigate_to_upload_modal(page)
        
        # Find and fill asset name
        asset_name_field = page.get_by_placeholder("Enter asset name")
        await asset_name_field.clear()  # Clear any existing text
        await type_like_human(asset_name_field, asset_name)

        # Upload file
        file_input = page.locator("input[type='file']").first
        if await file_input.count() > 0:
            await file_input.set_input_files(str(file_to_upload))
        else:
            async with page.expect_file_chooser() as fc_info:
                dropzone = page.locator(".cfxui__InputDropzone__dropzone__bde8d")
                await expect(dropzone).to_be_visible()
                await human_delay()
                await dropzone.click()
            file_chooser = await fc_info.value
            await file_chooser.set_files(str(file_to_upload))
        log("INFO", "upload:file_selected")

        upload_button = page.get_by_role("button", name="Upload File")
        await expect(upload_button).to_be_enabled()
        await upload_button.click()
        log("INFO", "upload:clicked")

        await expect(upload_button).to_be_hidden(timeout=90000)
        log("INFO", "upload:complete")
        await human_delay()

        # Navigate to assets list to see the processing status
        await page.goto("https://portal.cfx.re/assets/created-assets", wait_until="domcontentloaded")
        await human_delay()

        # Try to find the asset row, it might require scrolling or filtering
        asset_row = page.locator(f"tr:has-text('{asset_name}')")
        
        # Wait for the row to appear, with retries in case we need to refresh
        for retry in range(3):
            try:
                await expect(asset_row).to_be_visible(timeout=30000)
                break
            except:
                if retry < 2:
                    log("INFO", "asset:not_visible_yet", retry=retry, asset_name=asset_name)
                    await page.reload()
                    await human_delay(2, 4)
                else:
                    raise

        download_button = asset_row.locator('[data-sentry-component="DownloadButton"]')
        await expect(download_button).to_be_enabled(timeout=120000)
        log("INFO", "asset:processed_ready")

        # More unique output filename
        output_path = TMP_DIR / f"download_{uuid.uuid4().hex}_{int(time.time())}.zip"
        async with page.expect_download() as download_info:
            await download_button.click()
        download = await download_info.value
        await download.save_as(str(output_path))
        log("INFO", "download:complete", saved_to=str(output_path))

        return output_path

    finally:
        # Always try to navigate back to upload modal for next request
        try:
            await page.goto("https://portal.cfx.re/assets/created-assets?modal=create", 
                          wait_until="domcontentloaded", timeout=15000)
        except Exception as e:
            log("WARNING", "navigation:cleanup_failed", error=str(e))

async def process_with_persistent_browser(upload_zip: Path, dbg_tag: str, s3_debug_uploader):
    """
    Process using persistent browser instance.
    s3_debug_uploader(local_path: Path, key_suffix: str) -> awaitable
    """
    screenshot_path = html_path = None
    
    # simple retry wrapper for transient UI flakiness
    async def _with_retries(fn, *args, **kwargs):
        delays = [1, 2, 4]  # seconds
        last_exc = None
        for attempt, d in enumerate([0] + delays, start=1):
            if d: await asyncio.sleep(d)
            try:
                return await fn(*args, **kwargs)
            except Exception as e:
                last_exc = e
                log("WARNING", "retry", attempt=attempt, error=str(e))
                # On retry, get fresh page in case current one is broken
                if attempt < len(delays) + 1:
                    try:
                        await browser_pool.get_page()  # This will trigger health check
                    except:
                        pass
        raise last_exc

    page = None
    try:
        # Get page from browser pool
        page = await browser_pool.get_page()
        
        async with Timer("camoufox_run", dbg_tag=dbg_tag):
            result = await _with_retries(run_asset_flow, page, upload_zip)
        
        return result

    except Exception as e:
        # Artifacts on failure
        err_id = uuid.uuid4().hex
        screenshot_path = TMP_DIR / f"error_{err_id}.png"
        html_path = TMP_DIR / f"error_{err_id}.html"

        # Try to capture debug info if page is available
        if page:
            try:
                await page.screenshot(path=str(screenshot_path), full_page=True)
            except Exception:
                pass
            try:
                html = await page.content()
                html_path.write_text(html, encoding="utf-8", errors="ignore")
            except Exception:
                pass

        # Upload artifacts (best-effort)
        try:
            if DEBUG:
                if screenshot_path and screenshot_path.exists():
                    await s3_debug_uploader(screenshot_path, f"{DEBUG_PREFIX}{dbg_tag}/error.png")
                if html_path and html_path.exists():
                    await s3_debug_uploader(html_path, f"{DEBUG_PREFIX}{dbg_tag}/error.html")
        except Exception as up_e:
            log("ERROR", "debug_artifact_upload_failed", error=str(up_e))

        log("ERROR", "exception", error=str(e), traceback="".join(traceback.format_exc()))
        raise

    finally:
        # cleanup artifacts
        for p in (screenshot_path, html_path):
            try:
                if p and isinstance(p, Path) and p.exists(): p.unlink()
            except Exception:
                pass

# ================
# Per-record processing
# ================
async def _process_record(rec, debug_bucket_fallback: Optional[str]):
    """
    rec must be an S3-style record, e.g.:
      {"s3": {"bucket": {"name": "b"}, "object": {"key": "k"}}}
    """
    bucket = rec["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(rec["s3"]["object"]["key"])
    _current_ctx["s3_bucket"] = bucket
    _current_ctx["s3_key"] = key
    log("INFO", "event:record", bucket=bucket, key=key)

    if OUTPUT_PREFIX and key.startswith(OUTPUT_PREFIX):
        log("INFO", "skip:output_prefix", reason="avoid_recursion")
        return None

    if INPUT_PREFIX and not key.startswith(INPUT_PREFIX):
        log("INFO", "skip:not_under_input_prefix", input_prefix=INPUT_PREFIX)
        return None

    rel = key[len(INPUT_PREFIX):] if INPUT_PREFIX else key

    # More unique filenames to avoid collisions
    unique_id = f"{uuid.uuid4().hex}_{int(time.time())}"
    in_path = TMP_DIR / f"input_{unique_id}.zip"
    out_path: Optional[Path] = None

    # figure debug bucket
    use_debug_bucket = DEBUG_BUCKET or S3_BUCKET or debug_bucket_fallback or bucket
    dbg_tag = pathlib.Path(rel).stem or uuid.uuid4().hex

    async def s3_debug_uploader(local_path: Path, dbg_key_suffix: str):
        await s3_upload(local_path, use_debug_bucket, dbg_key_suffix)

    try:
        async with Timer("s3.download", key=key, bucket=bucket):
            await s3_download(bucket, key, in_path)

        async with Timer("process_with_persistent_browser", rel=rel):
            out_path = await process_with_persistent_browser(in_path, dbg_tag, s3_debug_uploader)

        out_bucket = S3_BUCKET or bucket
        out_key = f"{OUTPUT_PREFIX}{rel}"

        async with Timer("s3.upload_result", key=out_key, bucket=out_bucket):
            await s3_upload(out_path, out_bucket, out_key)

        # Notify with presigned URL if configured
        presigned_url = await generate_presigned_url(out_bucket, out_key)
        webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
        if presigned_url and webhook_url:
            message = f"🔒 @here Asset encryption has complete for `{rel}`!\n\Direct download link: {presigned_url} \n\n Please note that this URL will expire in 60 minutes."
            await send_discord_notification(webhook_url, message)
        else:
            log("INFO", "discord.notification:skip",
                reason="no_webhook_or_presign_failed",
                has_webhook=bool(webhook_url), has_presigned=bool(presigned_url))

        log("INFO", "done:record", output=f"s3://{out_bucket}/{out_key}")
        return {"in": f"s3://{bucket}/{key}", "out": f"s3://{out_bucket}/{out_key}"}

    finally:
        # cleanup tmp files
        for p in (in_path, out_path):
            try:
                if p and isinstance(p, Path) and p.exists():
                    p.unlink()
            except Exception:
                pass

# ================
# Event handler(s)
# ================
def _as_s3_record(bucket: str, key: str):
    # Ensure key is properly encoded
    encoded_key = urllib.parse.quote_plus(key)
    return {"s3": {"bucket": {"name": bucket}, "object": {"key": encoded_key}}}

async def async_handler(event, context):
    # simple health ping support
    if event.get("health_check") or event.get("rawPath") == "/healthz":
        return {"statusCode": 200, "body": "OK"}

    _current_ctx["request_id"] = getattr(context, "aws_request_id", None)

    # Log sanitized config
    original_max_parallel = int(os.getenv("MAX_PARALLEL", "1"))
    if original_max_parallel > 1:
        log("WARNING", "config:max_parallel_override", 
            message="MAX_PARALLEL forced to 1 for persistent browser mode", 
            requested=original_max_parallel, 
            actual=MAX_PARALLEL)
    
    log("INFO", "config",
        log_level=LOG_LEVEL,
        input_prefix=INPUT_PREFIX,
        output_prefix=OUTPUT_PREFIX,
        debug=DEBUG,
        debug_prefix=DEBUG_PREFIX,
        proxy_server=PROXY_SERVER,
        proxy_username=PROXY_USERNAME,
        proxy_password=_redact(PROXY_PASSWORD),
        cfx_username=CFX_USERNAME,
        cfx_password=_redact(CFX_PASSWORD),
        os_fingerprint=OS_FINGERPRINT,
        locale=LOCALE,
        max_parallel=MAX_PARALLEL,
        mode=MODE,
        disable_human_delays=DISABLE_HUMAN_DELAYS,
        browser_restart_after=BROWSER_RESTART_AFTER,
    )

    # Normalize to S3-style records list
    records = []
    if "Records" in event:
        records = event["Records"]
    elif event.get("bucket") and event.get("key"):
        records = [_as_s3_record(event["bucket"], event["key"])]
    elif event.get("records"):
        for r in event["records"]:
            if r.get("bucket") and r.get("key"):
                records.append(_as_s3_record(r["bucket"], r["key"]))

    if not records:
        log("INFO", "no_records")
        return {"statusCode": 200, "body": json.dumps({"processed": []})}

    # bounded concurrency
    sem = asyncio.Semaphore(MAX_PARALLEL)
    results = []

    async def _guarded(rec):
        async with sem:
            try:
                return await _process_record(rec, debug_bucket_fallback=records[0]["s3"]["bucket"]["name"])
            except Exception as e:
                log("ERROR", "record_failed", error=str(e))
                return {"error": str(e)}

    processed = await asyncio.gather(*[_guarded(r) for r in records], return_exceptions=False)
    for item in processed:
        if item:
            results.append(item)

    return {"statusCode": 200, "body": json.dumps({"processed": results}, ensure_ascii=False)}

def handler(event, context):
    """AWS Lambda entrypoint."""
    # Validate config on first real request (not health checks)
    if not event.get("health_check") and event.get("rawPath") != "/healthz":
        validate_config()
    return asyncio.run(async_handler(event, context))

# ================
# HTTP wrapper (FastAPI) for ECS / local
# ================
try:
    from fastapi import FastAPI, Request
    import uvicorn
    app_srv = FastAPI()

    @app_srv.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    @app_srv.post("/s3-event")
    async def s3_event(req: Request):
        event = await req.json()
        ctx = SimpleNamespace(aws_request_id=f"ecs-{uuid.uuid4().hex}")
        # Validate config on first real request
        validate_config()
        return await async_handler(event, ctx)
    
    @app_srv.on_event("shutdown")
    async def shutdown_event():
        """Clean up browser on shutdown."""
        log("INFO", "http:shutdown", action="closing_browser")
        await browser_pool.close()

except Exception as _e:
    app_srv = None  # FastAPI not installed; that's okay in pure Lambda/SQS mode

# ================
# SQS FIFO worker (MODE="sqs")
# ================
_shutdown = asyncio.Event()

def _install_signal_handlers():
    def _sigterm(*_):
        log("INFO", "signal:SIGTERM")
        try:
            # Try multiple methods to set the shutdown event
            loop = asyncio.get_running_loop()
            loop.call_soon_threadsafe(_shutdown.set)
        except RuntimeError:
            # No running loop, try to get event loop
            try:
                loop = asyncio.get_event_loop()
                if loop and loop.is_running():
                    loop.call_soon_threadsafe(_shutdown.set)
                else:
                    # Direct set if no loop
                    _shutdown.set()
            except Exception:
                # Last resort: direct set
                _shutdown.set()
    signal.signal(signal.SIGTERM, _sigterm)
    signal.signal(signal.SIGINT, _sigterm)

async def _receive_batch():
    def _recv():
        return sqs.receive_message(
            QueueUrl=SQS_QUEUE_URL,
            MaxNumberOfMessages=SQS_MAX_MESSAGES,
            WaitTimeSeconds=SQS_WAIT_TIME_SECONDS,
            VisibilityTimeout=SQS_VISIBILITY_TIMEOUT,
            AttributeNames=["SentTimestamp", "MessageGroupId", "SequenceNumber"],
            MessageAttributeNames=["All"],
        )
    return await asyncio.to_thread(_recv)

async def _delete_message(receipt_handle: str):
    def _del():
        sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
    await asyncio.to_thread(_del)

async def _worker_loop():
    if not SQS_QUEUE_URL:
        raise RuntimeError("SQS_QUEUE_URL required in MODE=sqs")
    
    # Validate config at startup
    validate_config()

    log("INFO", "sqs.worker:start", queue=SQS_QUEUE_URL, wait=SQS_WAIT_TIME_SECONDS, max_msgs=SQS_MAX_MESSAGES)
    sem = asyncio.Semaphore(max(1, int(MAX_PARALLEL)))
    _install_signal_handlers()

    try:
        while not _shutdown.is_set():
            try:
                resp = await _receive_batch()
                msgs = resp.get("Messages", [])
                if not msgs:
                    continue  # immediately issue another long poll

                async def _handle(msg):
                    async with sem:
                        rid = f"sqs-{uuid.uuid4().hex}"
                        body_raw = msg.get("Body", "{}")
                        
                        # Try to parse as JSON, but handle various message formats
                        try:
                            body = json.loads(body_raw)
                            # If it's a plain string that's not a JSON object, wrap it
                            if isinstance(body, str):
                                log("WARNING", "sqs.msg:string_body", body_preview=body[:100])
                                body = {"message": body}
                        except json.JSONDecodeError:
                            log("WARNING", "sqs.msg:parse_error", body_preview=body_raw[:100])
                            # Try to parse as S3 event notification
                            if "s3:ObjectCreated" in body_raw:
                                try:
                                    # Attempt to extract S3 info from raw string
                                    import re
                                    bucket_match = re.search(r'"name"\s*:\s*"([^"]+)"', body_raw)
                                    key_match = re.search(r'"key"\s*:\s*"([^"]+)"', body_raw)
                                    if bucket_match and key_match:
                                        body = {"Records": [_as_s3_record(bucket_match.group(1), key_match.group(1))]}
                                    else:
                                        body = {"raw": body_raw, "error": "Could not parse S3 event"}
                                except:
                                    body = {"raw": body_raw, "error": "Failed to extract S3 info"}
                            else:
                                body = {"raw": body_raw}
                        
                        ctx = SimpleNamespace(aws_request_id=rid)

                        try:
                            await async_handler(body, ctx)
                            await _delete_message(msg["ReceiptHandle"])
                            log("INFO", "sqs.msg:ok",
                                group=msg.get("Attributes", {}).get("MessageGroupId"),
                                seq=msg.get("Attributes", {}).get("SequenceNumber"))
                        except Exception as e:
                            # do not delete on failure (at-least-once)
                            log("ERROR", "sqs.msg:failed", error=str(e), traceback="".join(traceback.format_exc()))

                await asyncio.gather(*[_handle(m) for m in msgs])

            except Exception as e:
                log("ERROR", "sqs.poll:error", error=str(e))
                await asyncio.sleep(2.0)
    finally:
        # Cleanup browser on shutdown
        log("INFO", "sqs.worker:cleanup", action="closing_browser")
        await browser_pool.close()

    log("INFO", "sqs.worker:shutdown")

# ================
# Entrypoint
# ================
if __name__ == "__main__":
    if MODE == "sqs":
        asyncio.run(_worker_loop())
    else:
        if app_srv is None:
            raise RuntimeError("FastAPI/uvicorn not installed but MODE=http requested")
        # Validate config at startup for HTTP mode
        validate_config()
        port = int(os.getenv("PORT", "8080"))
        uvicorn.run("app:app_srv", host="0.0.0.0", port=port, log_level="info")