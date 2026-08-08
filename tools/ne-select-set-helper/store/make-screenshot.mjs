/**
 * ストア掲載用のスクリーンショットを作る。
 *   node tools/ne-select-set-helper/store/make-screenshot.mjs
 *
 * Chromeウェブストアは 1280x800 の JPEG か 24bit PNG (アルファ無し) を要求する。
 * 実画面のキャプチャはお客様の氏名・住所が写るため使えないので、
 * 説明用のHTMLをその場で描画して撮る。JPEGにするのはアルファを持たせないため。
 */
import path from 'path';
import fs from 'fs';
import { fileURLToPath, pathToFileURL } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// playwright-core はポータルの依存に入っていないので、
// 入っている場所 (ロジザード自動化ツール) からも探す
async function loadChromium() {
  const candidates = [
    'playwright-core',
    'C:/tools/logizard-automation/node_modules/playwright-core/index.mjs',
  ];
  for (const c of candidates) {
    try {
      const mod = await import(c.startsWith('C:') ? pathToFileURL(c).toString() : c);
      if (mod.chromium) return mod.chromium;
    } catch { /* 次を試す */ }
  }
  console.error('playwright-core が見つかりません。撮影はスキップします。');
  process.exit(1);
}
const chromium = await loadChromium();

const CHROME_CANDIDATES = [
  'C:/Program Files/Google/Chrome/Application/chrome.exe',
  'C:/Program Files (x86)/Google/Chrome/Application/chrome.exe',
  'C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe',
  'C:/Program Files/Microsoft/Edge/Application/msedge.exe',
];
const exe = CHROME_CANDIDATES.find((p) => fs.existsSync(p));
if (!exe) {
  console.error('Chrome が見つかりません: ' + CHROME_CANDIDATES.join(' / '));
  process.exit(1);
}

const browser = await chromium.launch({ executablePath: exe, headless: true });
const page = await browser.newPage({ viewport: { width: 1280, height: 800 }, deviceScaleFactor: 1 });
await page.goto(pathToFileURL(path.join(__dirname, 'screenshot.html')).toString(), { waitUntil: 'load' });
await page.waitForTimeout(400); // フォントの反映待ち

const out = path.join(__dirname, 'screenshot-1280x800.jpg');
await page.screenshot({ path: out, type: 'jpeg', quality: 92, clip: { x: 0, y: 0, width: 1280, height: 800 } });
await browser.close();

const kb = Math.round(fs.statSync(out).size / 1024);
console.log(`${out} (1280x800 JPEG / ${kb}KB)`);
