// Run with: node scripts/keymaster-upload.js --asset-name "My Asset" --zip-path "build/my.zip" --output "artifact/escrowed.zip"
// Env: CFX_USERNAME, CFX_PASSWORD
import { chromium } from "playwright";
import fs from "fs";
import path from "path";
import { hideBin } from "yargs/helpers";
import yargs from "yargs";

const argv = yargs(hideBin(process.argv))
  .option("asset-name", { type: "string", demandOption: true })
  .option("zip-path", { type: "string", demandOption: true })
  .option("output", { type: "string", default: "artifact/escrowed.zip" })
  .option("timeout-mins", { type: "number", default: 15, describe: "Max minutes to wait for ACTIVE" })
  .option("headless", { type: "boolean", default: true })
  .parse();

const USER = process.env.CFX_USERNAME;
const PASS = process.env.CFX_PASSWORD;

if (!USER || !PASS) {
  console.error("Missing CFX_USERNAME / CFX_PASSWORD env vars");
  process.exit(1);
}
if (!fs.existsSync(argv["zip-path"])) {
  console.error(`Zip not found at ${argv["zip-path"]}`);
  process.exit(1);
}
fs.mkdirSync(path.dirname(argv.output), { recursive: true });

const BASE = "https://portal.cfx.re";
const CREATED_ASSETS_URL = `${BASE}/assets/created-assets`;

function now() { return new Date().toISOString(); }

(async () => {
  const browser = await chromium.launch({ headless: argv.headless });
  const context = await browser.newContext({ acceptDownloads: true });
  await context.tracing.start({ screenshots: true, snapshots: true });
  const page = await context.newPage();

  try {
    console.log(`[${now()}] Navigating to created assets…`);
    await page.goto(CREATED_ASSETS_URL, { waitUntil: "domcontentloaded" });

    // If we’re not logged in, we should see a login page or a redirect chain.
    const loggedIn = await page.getByRole("button", { name: /add asset/i }).count().then(c => c > 0);
    if (!loggedIn) {
      console.log(`[${now()}] Logging in…`);
      // Try common username/password fields
      const userInput = page.locator('input[type="email"], input[name="email"], input[name="username"], input[name="login"]');
      const passInput = page.locator('input[type="password"], input[name="password"]');

      await userInput.first().waitFor({ timeout: 60000 });
      await userInput.first().fill(USER, { timeout: 30000 });
      await passInput.first().fill(PASS, { timeout: 30000 });

      // Click the most likely submit control
      const submit = page.getByRole("button", { name: /sign in|log in|continue|authorize/i });
      if (await submit.count() > 0) {
        await Promise.all([
          page.waitForLoadState("domcontentloaded"),
          submit.first().click()
        ]);
      } else {
        // fallback to Enter key
        await Promise.all([
          page.waitForLoadState("domcontentloaded"),
          page.keyboard.press("Enter")
        ]);
      }

      // Ensure we land in the portal and on the created-assets page
      await page.waitForLoadState("domcontentloaded");
      if (!page.url().includes("/assets/created-assets")) {
        await page.goto(CREATED_ASSETS_URL, { waitUntil: "domcontentloaded" });
      }
      await page.getByRole("button", { name: /add asset/i }).waitFor({ timeout: 60000 });
    }

    // Start creating/uploading asset
    console.log(`[${now()}] Opening "Add Asset" dialog…`);
    await page.getByRole("button", { name: /add asset/i }).click();

    const dialog = page.locator('div[role="dialog"]');
    await dialog.waitFor({ timeout: 30000 });

    // Fill asset name
    const assetName = argv["asset-name"];
    const nameField = dialog.getByLabel(/asset name/i).or(dialog.locator('input[placeholder*="name" i]')).or(dialog.locator('input'));
    await nameField.first().fill(assetName);

    // Choose file (hidden input is OK with setInputFiles)
    const fileInput = dialog.locator('input[type="file"]');
    await fileInput.setInputFiles(argv["zip-path"], { timeout: 60000 });

    // Click "Upload File" (or equivalent)
    const uploadBtn = dialog.getByRole("button", { name: /upload file|create|submit/i });
    if (await uploadBtn.count() > 0) {
      await Promise.all([
        page.waitForLoadState("networkidle"),
        uploadBtn.first().click()
      ]);
    } else {
      // Some UIs auto-start after file select; just wait a beat
      await page.waitForLoadState("networkidle");
    }

    // Wait for row to appear and become ACTIVE
    console.log(`[${now()}] Waiting for asset to become ACTIVE (timeout ${argv["timeout-mins"]}m)…`);
    const deadline = Date.now() + argv["timeout-mins"] * 60_000;

    // helper: get row text and status
    const rowForAsset = () => page.locator("tr").filter({ hasText: assetName }).first();

    // Wait for row to appear
    await rowForAsset().waitFor({ timeout: 120000 });

    let active = false;
    while (Date.now() < deadline) {
      const row = rowForAsset();
      const text = (await row.textContent()) || "";
      if (/\bACTIVE\b/i.test(text)) {
        active = true;
        break;
      }
      // The table can stale; reload page to refresh status
      await page.waitForTimeout(5000);
      await page.reload({ waitUntil: "domcontentloaded" });
    }
    if (!active) throw new Error(`Timed out waiting for ACTIVE status for "${assetName}"`);

    console.log(`[${now()}] ACTIVE. Downloading escrowed zip…`);
    const row = rowForAsset();

    const [download] = await Promise.all([
      page.waitForEvent("download", { timeout: 5 * 60_000 }),
      row.getByRole("button", { name: /download/i }).click()
    ]);

    const savePath = path.resolve(argv.output);
    await download.saveAs(savePath);
    console.log(`[${now()}] Saved: ${savePath}`);
  } catch (err) {
    console.error(`❌ ERROR: ${err?.message || err}`);
    throw err;
  } finally {
    await context.tracing.stop({ path: "playwright-trace.zip" });
    await context.close();
    await browser.close();
  }
})();
