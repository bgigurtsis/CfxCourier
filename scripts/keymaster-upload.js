// Usage:
//   node scripts/keymaster-upload-stealth.js \
//     --asset-name "My Asset v1.2.3" \
//     --zip-path "build/my.zip" \
//     --output "artifact/escrowed.zip" \
//     --timeout-mins 20
//
// Requires: CFX_USERNAME and CFX_PASSWORD environment variables

import { chromium } from "playwright-extra";
import StealthPlugin from "puppeteer-extra-plugin-stealth";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { hideBin } from "yargs/helpers";
import yargs from "yargs";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Add stealth plugin to avoid detection
chromium.use(StealthPlugin());

const argv = yargs(hideBin(process.argv))
  .option("asset-name", { type: "string", demandOption: true })
  .option("zip-path", { type: "string", demandOption: true })
  .option("output", { type: "string", default: "artifact/escrowed.zip" })
  .option("timeout-mins", { type: "number", default: 15 })
  .option("headless", { type: "boolean", default: false }) // Keep false for better success
  .option("save-cookies", { type: "boolean", default: true })
  .option("cookie-file", { type: "string", default: "cfx-cookies.json" })
  .parse();

const USER = "shirahama";
const PASS = "banshi21";

if (!USER || !PASS) {
  console.error("Missing CFX_USERNAME or CFX_PASSWORD environment variables");
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

// Human-like delay function
const humanDelay = async (min = 500, max = 2000) => {
  const delay = Math.floor(Math.random() * (max - min) + min);
  await new Promise(resolve => setTimeout(resolve, delay));
};

// Random mouse movements to simulate human behavior
async function simulateHumanBehavior(page) {
  const width = page.viewportSize().width;
  const height = page.viewportSize().height;
  
  // Random mouse movements
  for (let i = 0; i < 3; i++) {
    const x = Math.floor(Math.random() * width);
    const y = Math.floor(Math.random() * height);
    await page.mouse.move(x, y, { steps: 10 });
    await humanDelay(100, 300);
  }
  
  // Random scroll
  await page.mouse.wheel(0, Math.random() * 200);
  await humanDelay();
}

// Generate realistic viewport size
function getRandomViewport() {
  const viewports = [
    { width: 1920, height: 1080 }, // Full HD
    { width: 1366, height: 768 },  // Common laptop
    { width: 1440, height: 900 },   // MacBook
    { width: 1536, height: 864 },   // Surface
    { width: 1680, height: 1050 },  // Widescreen
  ];
  const viewport = viewports[Math.floor(Math.random() * viewports.length)];
  // Add small random offset
  viewport.width += Math.floor(Math.random() * 20) - 10;
  viewport.height += Math.floor(Math.random() * 20) - 10;
  return viewport;
}

// User agents that match common browsers
const userAgents = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0',
];

async function signIn(page, context) {
  console.log(`[${now()}] Navigating to login page...`);
  
  // Go to login page with realistic loading
  await page.goto(LOGIN_URL, { waitUntil: "networkidle" });
  await humanDelay(2000, 4000); // Wait for any CF challenges
  
  // Check if Cloudflare challenge is present
  const pageContent = await page.content();
  if (pageContent.includes("Checking your browser") || pageContent.includes("cf-browser-verification")) {
    console.log(`[${now()}] Cloudflare challenge detected, waiting...`);
    await humanDelay(5000, 8000);
    await simulateHumanBehavior(page);
  }
  
  // Look for the CFX sign-in button
  const signInBtn = page.getByRole("button", { name: /^sign in with cfx\.re$/i });
  await signInBtn.waitFor({ timeout: 60_000 });
  
  // Simulate human behavior before clicking
  await simulateHumanBehavior(page);
  
  console.log(`[${now()}] Clicking sign in button...`);
  await signInBtn.click();
  
  // Wait for navigation with human-like delay
  await humanDelay(1000, 2000);
  
  // Wait for login form
  const userField = page.locator('input[type="email"], input[name="email"], input[name="username"], input#username').first();
  await userField.waitFor({ state: "visible", timeout: 60_000 });
  
  // Fill credentials with human-like typing
  console.log(`[${now()}] Filling credentials...`);
  await userField.click();
  await humanDelay(300, 600);
  await userField.type(USER, { delay: 50 + Math.random() * 100 }); // Random typing speed
  
  await humanDelay(500, 1000);
  
  const passField = page.locator('input[type="password"]').first();
  await passField.click();
  await humanDelay(300, 600);
  await passField.type(PASS, { delay: 50 + Math.random() * 100 });
  
  await humanDelay(1000, 2000);
  
  // Find and click submit
  const submitBtn = page.locator('button[type="submit"], button:has-text("Sign in"), button:has-text("Log in")').first();
  if (await submitBtn.count() > 0) {
    await submitBtn.click();
  } else {
    await passField.press("Enter");
  }
  
  // Wait for result
  console.log(`[${now()}] Waiting for login result...`);
  
  try {
    await Promise.race([
      page.waitForURL(/portal\.cfx\.re(?!.*login)/i, { timeout: 30_000 }),
      page.locator('text=/check your email|verify.*email|new location/i').waitFor({ timeout: 30_000 }),
    ]);
  } catch (e) {
    console.log(`[${now()}] Login wait timed out, checking state...`);
  }
  
  // Check for email verification
  const content = await page.content();
  if (content.includes("check your email") || content.includes("new location")) {
    console.log(`[${now()}] ⚠️  Email verification required!`);
    
    if (!argv.headless) {
      console.log(`[${now()}] Please complete email verification in the browser.`);
      console.log(`[${now()}] Press Enter here after clicking the email link...`);
      
      const readline = await import('readline');
      const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout
      });
      
      await new Promise(resolve => {
        rl.question('Press Enter after email verification...', () => {
          rl.close();
          resolve();
        });
      });
    } else {
      throw new Error("Email verification required. Run with --headless false");
    }
  }
  
  // Save cookies if successful
  if (argv["save-cookies"] && page.url().includes("portal.cfx.re") && !page.url().includes("login")) {
    const cookies = await context.cookies();
    fs.writeFileSync(argv["cookie-file"], JSON.stringify(cookies, null, 2));
    console.log(`[${now()}] ✓ Cookies saved for future use`);
  }
}

async function openCreateModal(page) {
  await page.goto(CREATE_URL, { waitUntil: "domcontentloaded" });
  await humanDelay(2000, 3000);
  await simulateHumanBehavior(page);
  
  let dialog = null;
  try {
    dialog = page.locator('div[role="dialog"]');
    await dialog.waitFor({ timeout: 5_000 });
  } catch {
    console.log("No dialog found, assuming inline form");
  }

  const nameInput = page.locator('input[placeholder="Enter asset name"]');
  await nameInput.first().waitFor({ timeout: 30_000 });
  return { nameInput, dialog };
}

async function uploadAsset(page, name, zipPath, dialog) {
  const nameInput = page.locator('input[placeholder="Enter asset name"]');
  await nameInput.first().click();
  await humanDelay(300, 600);
  await nameInput.first().type(name, { delay: 50 + Math.random() * 50 });
  
  await humanDelay(1000, 2000);
  
  const scopedRoot = dialog && (await dialog.count()) > 0 ? dialog : page;
  const fileInput = scopedRoot.locator('input[type="file"]').first();
  await fileInput.setInputFiles(zipPath, { timeout: 60_000 });
  
  await humanDelay(2000, 3000);
  
  const createBtn = scopedRoot.getByRole("button", { name: /upload|create|submit/i });
  if (await createBtn.count()) {
    await createBtn.first().click();
    await page.waitForLoadState("networkidle", { timeout: 60_000 });
  }
}

async function waitForActive(page, name, timeoutMins) {
  await page.goto(LIST_URL, { waitUntil: "domcontentloaded" });
  await humanDelay(2000, 3000);
  
  const deadline = Date.now() + timeoutMins * 60_000;
  const escapedName = name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const rowForAsset = () => page.locator("tr").filter({ hasText: new RegExp(escapedName) }).first();

  await rowForAsset().waitFor({ timeout: 120_000 });

  while (Date.now() < deadline) {
    const row = rowForAsset();
    const text = ((await row.textContent()) || "").toUpperCase();

    if (text.includes("ACTIVE")) return row;
    if (text.includes("FAILED")) throw new Error(`Processing FAILED for asset "${name}"`);

    await humanDelay(5000, 7000);
    await page.reload({ waitUntil: "domcontentloaded" });
  }
  throw new Error(`Timed out waiting for ACTIVE status for "${name}"`);
}

async function downloadEscrow(page, row, outPath) {
  const dlButton = row.getByRole("button", { name: /^download$/i });
  await dlButton.first().waitFor({ timeout: 60_000 });
  
  await simulateHumanBehavior(page);
  await humanDelay(1000, 2000);

  const [download] = await Promise.all([
    page.waitForEvent("download", { timeout: 5 * 60_000 }),
    dlButton.first().click()
  ]);
  await download.saveAs(outPath);
}

// Main execution
(async () => {
  const browser = await chromium.launch({ 
    headless: argv.headless,
    args: [
      '--disable-blink-features=AutomationControlled',
      '--disable-features=IsolateOrigins,site-per-process',
      '--disable-dev-shm-usage',
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-accelerated-2d-canvas',
      '--disable-gpu',
      // Mimic real Chrome args
      '--window-size=1920,1080',
      '--start-maximized',
    ]
  });
  
  const viewport = getRandomViewport();
  const userAgent = userAgents[Math.floor(Math.random() * userAgents.length)];
  
  let contextOptions = {
    viewport,
    userAgent,
    acceptDownloads: true,
    // Realistic browser settings
    locale: 'en-US',
    timezoneId: 'America/New_York',
    permissions: ['geolocation'],
    colorScheme: 'light',
    deviceScaleFactor: 1,
    hasTouch: false,
    // Minimal headers to avoid CORS issues
    extraHTTPHeaders: {
      'Accept-Language': 'en-US,en;q=0.9'
    }
  };
  
  // Try to load existing cookies
  if (fs.existsSync(argv["cookie-file"])) {
    try {
      const cookies = JSON.parse(fs.readFileSync(argv["cookie-file"], 'utf8'));
      contextOptions.storageState = { cookies };
      console.log(`[${now()}] Loaded saved cookies`);
    } catch (e) {
      console.log(`[${now()}] Failed to load cookies, starting fresh`);
    }
  }
  
  const context = await browser.newContext(contextOptions);
  
  // Additional stealth measures - must be added to context, not page
  await context.addInitScript(() => {
    // Override the navigator.webdriver property
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    
    // Add Chrome object
    window.chrome = { runtime: {} };
    
    // Fix permissions
    const originalQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = (parameters) => (
      parameters.name === 'notifications' ?
        Promise.resolve({ state: Notification.permission }) :
        originalQuery(parameters)
    );
  });
  
  await context.tracing.start({ screenshots: true, snapshots: true });
  const page = await context.newPage();

  try {
    // Check if already logged in
    console.log(`[${now()}] Checking authentication status...`);
    await page.goto(LIST_URL, { waitUntil: "domcontentloaded" });
    await humanDelay(2000, 3000);
    
    if (page.url().includes("login")) {
      console.log(`[${now()}] Not logged in, starting sign in process...`);
      await signIn(page, context);
    } else {
      console.log(`[${now()}] ✓ Already authenticated!`);
    }

    console.log(`[${now()}] Opening create modal…`);
    const { dialog } = await openCreateModal(page);

    console.log(`[${now()}] Uploading zip…`);
    await uploadAsset(page, argv["asset-name"], path.resolve(argv["zip-path"]), dialog);

    console.log(`[${now()}] Waiting for ACTIVE (timeout ${argv["timeout-mins"]}m)…`);
    const row = await waitForActive(page, argv["asset-name"], argv["timeout-mins"]);

    console.log(`[${now()}] Downloading escrowed zip…`);
    const outPath = path.resolve(argv.output);
    await downloadEscrow(page, row, outPath);

    console.log(`[${now()}] ✅ Success! Saved: ${outPath}`);
  } catch (e) {
    console.error(`[${now()}] ❌ ERROR: ${e?.message || e}`);
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