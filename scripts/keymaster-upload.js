// Usage:
//   node scripts/keymaster-upload.js \
//     --asset-name "My Asset v1.2.3" \
//     --zip-path "build/my.zip" \
//     --output "artifact/escrowed.zip" \
//     --timeout-mins 20
//
// Requires GitHub Secrets: CFX_USERNAME, CFX_PASSWORD
import { chromium } from "playwright";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { hideBin } from "yargs/helpers";
import yargs from "yargs";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const argv = yargs(hideBin(process.argv))
  .option("asset-name", { type: "string", demandOption: true })
  .option("zip-path", { type: "string", demandOption: true })
  .option("output", { type: "string", default: "artifact/escrowed.zip" })
  .option("timeout-mins", { type: "number", default: 15 })
  .option("headless", { type: "boolean", default: true })
  .parse();

const USER = process.env.CFX_USERNAME;
const PASS = process.env.CFX_PASSWORD;

if (!USER || !PASS) {
  console.error("Missing CFX_USERNAME / CFX_PASSWORD env vars");
  process.exit(1);
}
if (!fs.existsSync(argv["zip-path"])) {
  console.error(`Zip not found: ${argv["zip-path"]}`);
  process.exit(1);
}
fs.mkdirSync(path.dirname(argv.output), { recursive: true });

const BASE = "https://portal.cfx.re";
const LOGIN_URL = `${BASE}/login`;
const CREATE_URL = `${BASE}/assets/created-assets?modal=create`;
const LIST_URL = `${BASE}/assets/created-assets?page=1&sort=asset.id&direction=desc`;

const now = () => new Date().toISOString();

async function signIn(page) {
  // Go to login and click the exact button you pasted
  await page.goto(LOGIN_URL, { waitUntil: "domcontentloaded" });
  const signInBtn = page.getByRole("button", { name: /^sign in with cfx\.re$/i });
  await signInBtn.waitFor({ timeout: 60_000 });
  await Promise.all([
    page.waitForLoadState("domcontentloaded"),
    signInBtn.click()
  ]);

  // Fill username/password on the identity page
  const userField = page.locator('input[type="email"], input[name="email"], input[name="username"], input[autocomplete="username"], input[name="login"]');
  const passField = page.locator('input[type="password"], input[name="password"], input[autocomplete="current-password"]');
  await userField.first().waitFor({ timeout: 60_000 });
  await userField.first().fill(USER);
  await passField.first().fill(PASS);

  const submit = page.getByRole("button", { name: /sign in|log in|continue|authorize|next/i });
  if (await submit.count()) {
    await Promise.all([
      page.waitForLoadState("domcontentloaded"),
      submit.first().click()
    ]);
  } else {
    await page.keyboard.press("Enter");
    await page.waitForLoadState("domcontentloaded");
  }
}

async function openCreateModal(page) {
  // Directly open the create modal URL you provided
  await page.goto(CREATE_URL, { waitUntil: "domcontentloaded" });

  // Wait for the dropzone/input and the asset name input
  const dialog = page.locator('div[role="dialog"]');
  await dialog.waitFor({ timeout: 30_000 }).catch(() => {}); // some builds render inline, not always with role=dialog

  // Asset name input (exact placeholder you sent)
  const nameInput = page.locator('input[placeholder="Enter asset name"]');
  await nameInput.first().waitFor({ timeout: 30_000 });
  return { nameInput, dialog };
}

async function uploadAsset(page, name, zipPath, dialog) {
  // Fill asset name
  const nameInput = page.locator('input[placeholder="Enter asset name"]');
  await nameInput.first().fill(name);

  // Use the hidden <input type="file"> inside the dropzone you pasted
  const scopedRoot = dialog.count() ? dialog : page;
  const fileInput = scopedRoot.locator('input[type="file"]');
  await fileInput.first().setInputFiles(zipPath, { timeout: 60_000 });

  // Click the submit-ish button. It varies; accept any of these labels.
  const createOrUpload = scopedRoot.getByRole("button", { name: /upload file|upload|create|submit|save/i });
  if (await createOrUpload.count()) {
    await Promise.all([
      page.waitForLoadState("networkidle"),
      createOrUpload.first().click()
    ]);
  } else {
    // Some UIs auto start once a file is selected
    await page.waitForLoadState("networkidle");
  }
}

async function waitForActive(page, name, timeoutMins) {
  // Go to the list view you pasted and poll until the row shows ACTIVE
  await page.goto(LIST_URL, { waitUntil: "domcontentloaded" });

  const deadline = Date.now() + timeoutMins * 60_000;
  const rowForAsset = () => page.locator("tr").filter({ hasText: name }).first();

  // Wait for the row to appear
  await rowForAsset().waitFor({ timeout: 120_000 });

  while (Date.now() < deadline) {
    const row = rowForAsset();
    const text = ((await row.textContent()) || "").toUpperCase();

    if (text.includes("ACTIVE")) return row; // done
    if (text.includes("FAILED")) throw new Error(`Processing FAILED for asset "${name}"`);

    // Refresh so status updates
    await page.waitForTimeout(5000);
    await page.reload({ waitUntil: "domcontentloaded" });
  }
  throw new Error(`Timed out waiting for ACTIVE status for "${name}"`);
}

async function downloadEscrow(page, row, outPath) {
  const dlButton = row.getByRole("button", { name: /^download$/i });
  await dlButton.first().waitFor({ timeout: 60_000 });

  const [download] = await Promise.all([
    page.waitForEvent("download", { timeout: 5 * 60_000 }),
    dlButton.first().click()
  ]);
  await download.saveAs(outPath);
}

(async () => {
  const browser = await chromium.launch({ headless: argv.headless });
  const context = await browser.newContext({ acceptDownloads: true });
  await context.tracing.start({ screenshots: true, snapshots: true });
  const page = await context.newPage();

  try {
    console.log(`[${now()}] Signing in…`);
    await signIn(page);

    console.log(`[${now()}] Opening create modal…`);
    const { dialog } = await openCreateModal(page);

    console.log(`[${now()}] Uploading zip…`);
    await uploadAsset(page, argv["asset-name"], path.resolve(argv["zip-path"]), dialog);

    console.log(`[${now()}] Waiting for ACTIVE (timeout ${argv["timeout-mins"]}m)…`);
    const row = await waitForActive(page, argv["asset-name"], argv["timeout-mins"]);

    console.log(`[${now()}] Downloading escrowed zip…`);
    const outPath = path.resolve(argv.output);
    await downloadEscrow(page, row, outPath);

    console.log(`[${now()}] Saved: ${outPath}`);
  } catch (e) {
    console.error(`[${now()}] ❌ ERROR: ${e?.message || e}`);
    // Quick screenshot too, alongside trace
    try {
      await page.screenshot({ path: path.join(__dirname, "failure.png"), fullPage: true });
    } catch {}
    throw e;
  } finally {
    await context.tracing.stop({ path: "playwright-trace.zip" });
    await context.close();
    await browser.close();
  }
})();
