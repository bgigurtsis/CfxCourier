import os, sys, json, time, random, urllib.parse, uuid, asyncio, traceback, pathlib, boto3, requests

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from playwright.async_api import Page, expect
from camoufox.async_api import AsyncCamoufox
from camoufox import DefaultAddons

# =========================
# Env / Config
# =========================
CFX_USERNAME = os.getenv("CFX_USERNAME")
CFX_PASSWORD = os.getenv("CFX_PASSWORD")

PROXY_SERVER   = os.getenv("PROXY_SERVER")
PROXY_USERNAME = os.getenv("PROXY_USERNAME")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")

S3_BUCKET      = os.getenv("S3_BUCKET")
OUTPUT_PREFIX  = os.getenv("OUTPUT_PREFIX", "processed/")
INPUT_PREFIX   = os.getenv("INPUT_PREFIX",  "unprocessed/")
OS_FINGERPRINT = os.getenv("CAMOUFOX_OS", "windows")
LOCALE         = os.getenv("CAMOUFOX_LOCALE", "en-GB")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
DEBUG = os.getenv("DEBUG", "1") == "1"
DEBUG_BUCKET = os.getenv("S3_BUCKET")  # defaulted in code below
DEBUG_PREFIX = os.getenv("DEBUG_PREFIX", "debug/")
DEBUG_UPLOAD_ON_SUCCESS = os.getenv("DEBUG_UPLOAD_ON_SUCCESS", "0") == "1"
MAX_PARALLEL = int(os.getenv("MAX_PARALLEL", "1"))  # keep 1 unless you know portal rate-limits

TMP_DIR = Path("/tmp"); TMP_DIR.mkdir(exist_ok=True)

# Single boto3 client (non-blocking calls via asyncio.to_thread)
s3 = boto3.client("s3")

# =========================
# Structured Logger
# =========================
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

# =========================
# S3 helpers (non-blocking)
# =========================
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

# =========================
# Helpers
# =========================

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

async def send_discord_notification(webhook_url: str, message: str):
    """Sends a message to a Discord webhook."""
    log("INFO", "discord.notification:start")
    try:
        # Running synchronously for simplicity, can be run in a thread
        response = requests.post(webhook_url, json={"content": message})
        response.raise_for_status() # Raise an exception for bad status codes
        log("INFO", "discord.notification:end", status=response.status_code)
    except Exception as e:
        log("ERROR", "discord.notification:error", error=str(e))

async def human_delay(min_seconds=1.2, max_seconds=2.6):
    await asyncio.sleep(random.uniform(min_seconds, max_seconds))

async def type_like_human(el, text: str):
    await el.click()
    await human_delay(0.3, 0.7)
    for ch in text:
        await el.type(ch, delay=random.uniform(70, 200))
    await human_delay(0.3, 0.7)

async def perform_login(page: Page, username: str, password: str):
    log("INFO", "login:begin")
    await page.goto("https://portal.cfx.re/assets/created-assets?modal=create",
                    wait_until="domcontentloaded", timeout=60000)
    await human_delay()
    signin_button = page.get_by_role("button", name="Sign in with Cfx.re")
    await expect(signin_button).to_be_visible(timeout=15000)
    await signin_button.click()
    await human_delay()

    username_field = page.locator("#login-account-name")
    password_field = page.locator("#login-account-password")
    await expect(username_field).to_be_visible()
    await type_like_human(username_field, username)
    await expect(password_field).to_be_visible()
    await type_like_human(password_field, password)

    login_button = page.locator("#login-button")
    await expect(login_button).to_be_enabled()
    await login_button.click()
    log("INFO", "login:submitted")

async def run_asset_flow(page: Page, file_to_upload: Path) -> Path:
    base_asset_name = os.getenv("BASE_ASSET_NAME", "TestAsset")
    asset_name = f"{base_asset_name}_{int(time.time())}"
    log("INFO", "asset_flow:start", asset_name=asset_name, upload=str(file_to_upload))

    await page.goto("https://portal.cfx.re/assets/created-assets?modal=create",
                    wait_until="domcontentloaded", timeout=60000)
    await human_delay(1.5, 3)

    asset_name_field = page.get_by_placeholder("Enter asset name")
    try:
        await expect(asset_name_field).to_be_visible(timeout=7000)
        log("INFO", "login:already_authenticated")
    except Exception:
        log("INFO", "login:required")
        await perform_login(page, CFX_USERNAME, CFX_PASSWORD)
        await expect(asset_name_field).to_be_visible(timeout=25000)

    await type_like_human(asset_name_field, asset_name)

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

    asset_row = page.locator(f"tr:has-text('{asset_name}')")
    await expect(asset_row).to_be_visible(timeout=30000)

    download_button = asset_row.locator('[data-sentry-component="DownloadButton"]')
    await expect(download_button).to_be_enabled(timeout=120000)
    log("INFO", "asset:processed_ready")

    output_path = TMP_DIR / f"download_{uuid.uuid4().hex}.zip"
    async with page.expect_download() as download_info:
        await download_button.click()
    download = await download_info.value
    await download.save_as(str(output_path))
    log("INFO", "download:complete", saved_to=str(output_path))

    return output_path

async def process_with_camoufox(upload_zip: Path, dbg_tag: str, s3_debug_uploader):
    """
    s3_debug_uploader(local_path: Path, key_suffix: str) -> awaitable
    """
    proxy = None
    if PROXY_SERVER:
        proxy = {"server": PROXY_SERVER}
        if PROXY_USERNAME and PROXY_PASSWORD:
            proxy["username"] = PROXY_USERNAME
            proxy["password"] = PROXY_PASSWORD

    console_msgs, req_failures = [], []
    screenshot_path = html_path = trace_path = None

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
        raise last_exc

    async with AsyncCamoufox(
        headless=True,
        os=OS_FINGERPRINT,
        locale=LOCALE,
        geoip=True,
        proxy=proxy,
        window=(1920, 1080),
        exclude_addons=[DefaultAddons.UBO],  # important for Lambda & local parity
    ) as browser:
        context = await browser.new_context()

        tracing_on = DEBUG or DEBUG_UPLOAD_ON_SUCCESS
        if tracing_on:
            await context.tracing.start(screenshots=True, snapshots=True, sources=True)

        page = await context.new_page()

        page.on("console", lambda m: console_msgs.append({"type": m.type, "text": m.text}))
        page.on("requestfailed", lambda r: req_failures.append({
            "url": r.url, "method": r.method, "failure": getattr(r, "failure", None)
        }))

        try:
            async with Timer("camoufox_run", dbg_tag=dbg_tag):
                result = await _with_retries(run_asset_flow, page, upload_zip)

            if DEBUG_UPLOAD_ON_SUCCESS and tracing_on:
                trace_path = TMP_DIR / f"trace_{dbg_tag}.zip"
                await context.tracing.stop(path=str(trace_path))
                await s3_debug_uploader(trace_path, f"{DEBUG_PREFIX}{dbg_tag}/trace.zip")

            if console_msgs:
                log("INFO", "page.console", messages=console_msgs[:50])
            if req_failures:
                log("WARNING", "page.requestfailed", count=len(req_failures), samples=req_failures[:20])

            return result

        except Exception as e:
            # Artifacts on failure
            err_id = uuid.uuid4().hex
            screenshot_path = TMP_DIR / f"error_{err_id}.png"
            html_path = TMP_DIR / f"error_{err_id}.html"
            trace_path = TMP_DIR / f"trace_{dbg_tag}.zip"

            try:
                await page.screenshot(path=str(screenshot_path), full_page=True)
            except Exception:
                pass
            try:
                html = await page.content()
                html_path.write_text(html, encoding="utf-8", errors="ignore")
            except Exception:
                pass
            try:
                if tracing_on:
                    await context.tracing.stop(path=str(trace_path))
            except Exception:
                pass

            # Upload artifacts
            try:
                if DEBUG:
                    if trace_path and trace_path.exists():
                        await s3_debug_uploader(trace_path, f"{DEBUG_PREFIX}{dbg_tag}/trace.zip")
                    if screenshot_path and screenshot_path.exists():
                        await s3_debug_uploader(screenshot_path, f"{DEBUG_PREFIX}{dbg_tag}/error.png")
                    if html_path and html_path.exists():
                        await s3_debug_uploader(html_path, f"{DEBUG_PREFIX}{dbg_tag}/error.html")
            except Exception as up_e:
                log("ERROR", "debug_artifact_upload_failed", error=str(up_e))

            log("ERROR", "exception", error=str(e), traceback="".join(traceback.format_exc()))
            if console_msgs:
                log("ERROR", "page.console_on_error", messages=console_msgs[:100])
            if req_failures:
                log("ERROR", "page.requestfailed_on_error", count=len(req_failures), samples=req_failures[:50])
            raise

        finally:
            # cleanup artifacts
            for p in (screenshot_path, html_path, trace_path):
                try:
                    if p and isinstance(p, Path) and p.exists(): p.unlink()
                except Exception:
                    pass

# =========================
# Per-record processing
# =========================
async def _process_record(rec, debug_bucket_fallback: Optional[str]):
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

    in_path = TMP_DIR / f"input_{uuid.uuid4().hex}.zip"
    out_path: Optional[Path] = None

    # figure debug bucket
    use_debug_bucket = DEBUG_BUCKET or S3_BUCKET or debug_bucket_fallback or bucket
    dbg_tag = pathlib.Path(rel).stem or uuid.uuid4().hex

    async def s3_debug_uploader(local_path: Path, dbg_key_suffix: str):
        await s3_upload(local_path, use_debug_bucket, dbg_key_suffix)

    try:
        async with Timer("s3.download", key=key, bucket=bucket):
            await s3_download(bucket, key, in_path)

        async with Timer("process_with_camoufox", rel=rel):
            out_path = await process_with_camoufox(in_path, dbg_tag, s3_debug_uploader)

        out_bucket = S3_BUCKET or bucket
        out_key = f"{OUTPUT_PREFIX}{rel}"

        async with Timer("s3.upload_result", key=out_key, bucket=out_bucket):
            await s3_upload(out_path, out_bucket, out_key)
        
        log("INFO", "discord_section_start", checkpoint="after_upload")
        
        # 1. Generate the presigned URL for the uploaded file
        presigned_url = await generate_presigned_url(out_bucket, out_key)

        # 2. Get webhook from environment and send notification
        webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
        log("INFO", "discord_check", 
            has_webhook=bool(webhook_url),
            has_presigned_url=bool(presigned_url),
            webhook_exists=webhook_url is not None,
            presigned_exists=presigned_url is not None)
            
        if presigned_url and webhook_url:
            message = f"✅ Asset processing complete for `{rel}`!\n\nDownload here: {presigned_url}"
            await send_discord_notification(webhook_url, message)
        else:
            # This log will make the problem obvious next time!
            log("WARNING", "discord.notification:skip", 
                reason="Webhook URL not set or presigned URL failed",
                webhook_exists=bool(webhook_url),
                presigned_url_exists=bool(presigned_url))

        log("INFO", "discord_section_end", checkpoint="before_done")
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

# =========================
# Lambda Handler
# =========================
async def async_handler(event, context):
    if event.get("health_check") or event.get("rawPath") == "/healthz":
        return {"statusCode": 200, "body": "OK"}

    _current_ctx["request_id"] = getattr(context, "aws_request_id", None)

    # Log sanitized config
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
    )

    records = event.get("Records", [])
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
                # log but continue with other records
                log("ERROR", "record_failed", error=str(e))
                return {"error": str(e)}

    processed = await asyncio.gather(*[_guarded(r) for r in records], return_exceptions=False)
    for item in processed:
        if item:
            results.append(item)

    return {"statusCode": 200, "body": json.dumps({"processed": results}, ensure_ascii=False)}

def handler(event, context):
    """
    Synchronous entrypoint for AWS Lambda which runs the main async handler.
    """
    return asyncio.run(async_handler(event, context))