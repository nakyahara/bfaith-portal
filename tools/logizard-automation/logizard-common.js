/**
 * ロジザード自動化の共通部品 (2026-07-25)
 *
 * auto-hokyu.js / auto-zaiko.js / auto-nefuda.js で個別に育ってきた
 * 「.env読み込み / セッションロック / ログイン / 印刷」を新規スクリプト用に切り出したもの。
 * ⚠ 既存3本は本番稼働中のため、あえて書き換えていない (このモジュールは新規分のみが使う)。
 */
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { execFileSync } from 'child_process';
import { fileURLToPath } from 'url';
import { chromium } from 'playwright-core';

export const DIR = path.dirname(fileURLToPath(import.meta.url));
export const ORIGIN = 'https://ap003.logizard.net';
export const BASE = `${ORIGIN}/LPSTD405`;
export const LOG_DIR = path.join(DIR, 'logs');
export const SHOT_DIR = path.join(DIR, 'error-shots');
export const CAPTURE_DIR = path.join(DIR, 'captures');

const CHROME_CANDIDATES = [
  'C:/Program Files/Google/Chrome/Application/chrome.exe',
  'C:/Program Files (x86)/Google/Chrome/Application/chrome.exe',
  'C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe',
  'C:/Program Files/Microsoft/Edge/Application/msedge.exe',
];

// ロックは「同じアカウントでログインするスクリプト同士」を直列化するためのもの。
// 同一アカウントの同時ログインはセッションを追い出し合うため、既定は共通ロック
// (auto-zaiko.js / auto-nefuda.js と同じファイル)。
// 別アカウントで動くスクリプトは acquireLock({ name }) で自分専用のロックを使う
// (共通ロックを使うと、衝突しない相手の実行を無意味に待たされるため)
const DEFAULT_LOCK_NAME = 'logizard-session.lock';
let LOCK_PATH = path.join(LOG_DIR, DEFAULT_LOCK_NAME);
const LOCK_STALE_MS = 30 * 60 * 1000;

// ---- 時刻 (JST) ----
export function jstDate(offsetDays = 0) {
  const d = new Date(Date.now() + 9 * 3600 * 1000 - offsetDays * 86400 * 1000);
  return d.toISOString().slice(0, 10).replace(/-/g, '/'); // YYYY/MM/DD
}
export function jstStamp() {
  const d = new Date(Date.now() + 9 * 3600 * 1000);
  return d.toISOString().replace(/[-:T]/g, '').slice(0, 14); // YYYYMMDDHHmmss
}
export const RUN_ID = jstStamp();
export const UNIQ = `${RUN_ID}_${process.pid}_${crypto.randomBytes(4).toString('hex')}`;

// ---- .env (依存追加なしの素朴パース。値の囲みクォートは除去する) ----
export function loadEnv() {
  const envPath = path.join(DIR, '.env');
  if (!fs.existsSync(envPath)) {
    console.error('❌ .env がありません。LOGIZARD_USER_ID / LOGIZARD_PASSWORD を設定してください。');
    process.exit(1);
  }
  for (const line of fs.readFileSync(envPath, 'utf8').split(/\r?\n/)) {
    const m = line.match(/^\s*([A-Z0-9_]+)\s*=\s*(.*?)\s*$/);
    if (!m) continue;
    let v = m[2];
    if (v.length >= 2 && ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'")))) {
      v = v.slice(1, -1);
    }
    process.env[m[1]] = v;
  }
}

export function requireIntEnv(name, defaultValue, { min, max }) {
  const raw = (process.env[name] ?? String(defaultValue)).trim();
  const n = Number(raw);
  if (!Number.isSafeInteger(n) || n < min || n > max) {
    console.error(`❌ .env の ${name} が不正です: "${raw}" (${min}〜${max} の整数で指定してください)`);
    process.exit(1);
  }
  return n;
}

// ---- 二重起動の拒否 (auto-nefuda.js と同一アルゴリズム・同一ロックファイル) ----
const LOCK_TOKEN = crypto.randomUUID();

function readLock() {
  try { return JSON.parse(fs.readFileSync(LOCK_PATH, 'utf8')); } catch { return null; }
}
function pidAlive(pid) {
  if (!Number.isInteger(pid) || pid <= 0) return false;
  try { process.kill(pid, 0); return true; } catch (e) { return e.code === 'EPERM'; }
}

export function acquireLock({ name = DEFAULT_LOCK_NAME } = {}) {
  LOCK_PATH = path.join(LOG_DIR, name);
  fs.mkdirSync(LOG_DIR, { recursive: true });
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      fs.writeFileSync(LOCK_PATH, JSON.stringify({ pid: process.pid, token: LOCK_TOKEN, startedAt: new Date().toISOString() }), { flag: 'wx' });
      const check = readLock();
      if (!check || check.token !== LOCK_TOKEN) {
        console.error('❌ ロックの取得に失敗しました (他プロセスと競合)。少し待って再実行してください。');
        process.exit(1);
      }
      const hb = setInterval(() => {
        try { const now = new Date(); fs.utimesSync(LOCK_PATH, now, now); } catch { /* 消されていても本体には影響させない */ }
      }, 5 * 60 * 1000);
      hb.unref();
      // 🚨真偽値として使えるハンドルを返す (2026-09-01)。
      //   以前は undefined を返しており、`lock = acquireLock(); ... if (lock) releaseLock();`
      //   と書いている呼び出し側 (auto-nyuka-csv.js / auto-shohin-csv.js) では
      //   **解放が一度も走っていなかった**。次の実行が30分の stale 判定でロックを
      //   捨てるまで残るため、同じ bat の中で続けて別のスクリプトを走らせると必ず失敗する。
      return { token: LOCK_TOKEN, path: LOCK_PATH };
    } catch (e) {
      if (e.code !== 'EEXIST') throw e;
      const existing = readLock();
      let age;
      try { age = Date.now() - fs.statSync(LOCK_PATH).mtimeMs; } catch { continue; }
      const ownerAlive = existing && pidAlive(existing.pid);
      if (age > LOCK_STALE_MS && !ownerAlive) {
        const quarantine = `${LOCK_PATH}.stale_${UNIQ}`;
        try { fs.renameSync(LOCK_PATH, quarantine); } catch { continue; }
        let quarantined = null;
        try { quarantined = JSON.parse(fs.readFileSync(quarantine, 'utf8')); } catch { /* 壊れたロックはnullのまま */ }
        if ((existing?.token ?? null) !== (quarantined?.token ?? null)) {
          try {
            fs.renameSync(quarantine, LOCK_PATH);
          } catch {
            console.error(`❌ ロック競合の復旧に失敗しました。${quarantine} が残っています。手動で確認してください。`);
            process.exit(1);
          }
          console.error('❌ 別のロジザード自動化が直前にロックを取得しました。終了を待ってから再実行してください。');
          process.exit(1);
        }
        console.log(`ℹ 古いロック (${Math.round(age / 60000)}分前・PID${existing?.pid ?? '不明'}は終了済み) を破棄して続行します`);
        try { fs.unlinkSync(quarantine); } catch { /* 隔離済みなので放置しても無害 */ }
        continue;
      }
      const why = ownerAlive ? `PID${existing.pid}が実行中` : `${Math.round(age / 1000)}秒前に開始`;
      console.error(`❌ 同じロックを使う別の処理が実行中です (${name} / ${why})。終了を待ってから再実行してください。`);
      console.error(`   ※異常終了で残った場合は ${LOCK_PATH} を削除してください。`);
      process.exit(1);
    }
  }
  console.error('❌ ロックを取得できませんでした (他プロセスと競合)。少し待って再実行してください。');
  process.exit(1);
}

export function releaseLock() {
  const cur = readLock();
  if (!cur || cur.token !== LOCK_TOKEN) return;
  try { fs.unlinkSync(LOCK_PATH); } catch { /* 既に無ければ無視 */ }
}

/**
 * 書き込み先がローカルであることを保証する (Codex R1 指摘)。
 * このツール一式は共有ドライブ (G: / UNC) に一切書かない方針のため、
 * スクリプト自体が共有ドライブから実行された場合は logs/downloads がそこに作られてしまう。
 */
export function assertLocalWriteDirs() {
  const root = path.parse(DIR).root.toUpperCase();
  const refuse = (why) => {
    console.error(`❌ ${why}: ${DIR}`);
    console.error('   このツールは共有ドライブに書き込まない設計です。C:\\tools\\logizard-automation にコピーして実行してください。');
    process.exit(1);
  };
  if (root.startsWith('\\\\')) refuse('UNCパス上から実行されています');
  if (root.startsWith('G:')) refuse('共有ドライブ上から実行されています');
  // 別のドライブ文字に割り当てたネットワークドライブも拒否する (Codex R2)。
  // 判定できない場合もフェイルクローズで中止する (Codex R3 高-3)
  if ((process.env.LOGIZARD_SKIP_DRIVE_CHECK || '0') === '1') return;
  const letter = root.slice(0, 1);
  if (!/^[A-Z]$/.test(letter)) refuse(`ドライブを判定できないパスです (${root})`);
  let out;
  try {
    // 取得成功を明示マーカーで確認する (Codex R4 高-3: 空文字を「ローカル」と誤読しない)
    out = execFileSync('powershell', [
      '-NoProfile', '-Command',
      `$ErrorActionPreference='Stop'; $d = Get-PSDrive -Name ${letter}; `
      + `if ($d.Provider.Name -ne 'FileSystem') { Write-Output 'BADPROVIDER'; exit 0 }; `
      + "Write-Output ('ROOT=' + $d.DisplayRoot); exit 0",
    ], { encoding: 'utf8', timeout: 15000, shell: false }).trim();
  } catch (e) {
    console.error(`❌ 実行場所がローカルドライブかを確認できませんでした: ${(e.message || '').slice(0, 120)}`);
    console.error(`   ${DIR} がローカル (C:等) であることを確認のうえ、`);
    console.error('   .env に LOGIZARD_SKIP_DRIVE_CHECK=1 を設定して再実行してください。');
    process.exit(1);
  }
  if (!out.startsWith('ROOT=')) {
    console.error(`❌ 実行場所のドライブ種別を判定できませんでした (${letter}: → "${out}")`);
    console.error(`   ${DIR} がローカル (C:等) であることを確認のうえ、`);
    console.error('   .env に LOGIZARD_SKIP_DRIVE_CHECK=1 を設定して再実行してください。');
    process.exit(1);
  }
  const displayRoot = out.slice('ROOT='.length).trim();
  if (displayRoot) refuse(`ネットワークドライブ上から実行されています (${letter}: → ${displayRoot})`);
}

/**
 * このPCのブラウザでロジザードを開いたままになっていないかの事前チェック。
 * 同一IDの多重ログインはセッションを追い出し合うため (auto-hokyu.js と同じ趣旨)。
 *
 * ⚠ 限界: 見えるのは各ウィンドウの「アクティブなタブ」のタイトルだけ。
 *   バックグラウンドタブや他PCのログインは検知できない。
 *   → 実行中の SUSPENDED / ログイン画面検知 (sessionLost) が後段の保険になっている。
 */
export function assertNoLogizardBrowserOpen() {
  if ((process.env.LOGIZARD_SKIP_LOGIN_CHECK || '0') === '1') return;
  let titles = '';
  try {
    // ⚠ -ErrorAction SilentlyContinue だけでは「プロセス無し」で終了コード1になる (PowerShell 5.1)。
    //   $ErrorActionPreference + exit 0 で握りつぶし、日本語タイトルのため出力をUTF-8にそろえる
    titles = execFileSync('powershell', [
      '-NoProfile', '-Command',
      "[Console]::OutputEncoding=[System.Text.Encoding]::UTF8; $ErrorActionPreference='SilentlyContinue'; "
      + "@('chrome','msedge') | ForEach-Object { Get-Process -Name $_ } | ForEach-Object { $_.MainWindowTitle }; exit 0",
    ], { encoding: 'utf8', timeout: 15000, shell: false });
  } catch (e) {
    // フェイルオープンにしない (Codex R2 高-3)。チェックできない状態で走らせると
    // 手動ログイン中のセッションを追い出す事故につながる
    console.error(`❌ ログイン済みブラウザの確認に失敗しました: ${(e.message || '').slice(0, 120)}`);
    console.error('   ブラウザでロジザードを開いていないことを目視で確認したうえで、');
    console.error('   .env に LOGIZARD_SKIP_LOGIN_CHECK=1 を設定して再実行してください。');
    process.exit(1);
  }
  if (/Logizard|ロジザード/i.test(titles)) {
    console.error('❌ このPCのブラウザでロジザードが開いたままです。');
    console.error('   セッション競合を防ぐため、そのウィンドウを閉じる (ログアウトする) か、');
    console.error('   確認済みなら .env に LOGIZARD_SKIP_LOGIN_CHECK=1 を設定して再実行してください。');
    process.exit(1);
  }
}

// ---- ブラウザ ----
export async function launchBrowser({ headless = false } = {}) {
  const chromePath = CHROME_CANDIDATES.find((p) => fs.existsSync(p));
  if (!chromePath) {
    console.error(`❌ Chromeが見つかりません: ${CHROME_CANDIDATES.join(' / ')}`);
    process.exit(1);
  }
  const browser = await chromium.launch({ executablePath: chromePath, headless });
  const context = await browser.newContext({ acceptDownloads: true });
  const page = await context.newPage();
  return { browser, context, page };
}

// ---- セッション喪失を示す専用エラー ----
export class SessionLostError extends Error {
  constructor(msg) { super(msg); this.sessionLost = true; }
}

export async function sessionLost(page) {
  if (page.url().includes('/SUSPENDED')) return true;
  return page.locator('#user_id').isVisible().catch(() => false);
}

export async function isLoggedIn(page) {
  if (await sessionLost(page)) return false;
  return page.locator('a[onclick*="openFunctionBar"], a[href*="Logout"], a[onclick*="logout"]')
    .first().isVisible().catch(() => false);
}

export async function errorShot(page, name) {
  try {
    fs.mkdirSync(SHOT_DIR, { recursive: true });
    await page.screenshot({ path: path.join(SHOT_DIR, `${UNIQ}_${name}.png`), timeout: 8000 });
    fs.writeFileSync(path.join(SHOT_DIR, `${UNIQ}_${name}.html`), await page.content(), 'utf8');
  } catch { /* スクショ失敗は無視 */ }
}

// ---- ログイン (リトライ1回。連続失敗でパスワードロックを避けるため2回まで) ----
// credentials を渡さなければ共通アカウント (LOGIZARD_USER_ID/PASSWORD) を使う
// beforeSubmit (任意): ログインのボタンを押す直前に毎回呼ぶ (リトライも)。例外を投げれば押さない。
//   正の数を返せば、それをボタンを押す持ち時間 (click の timeout・ms) にする (押せるようになるまでの待ちも含めて、その時間を過ぎたら押さない)
//   (auto-barcode.js の夜の止め・③c-1b-3a。渡さない呼び手は今までと同じ)
export async function login(page, { userId, password, label = '', beforeSubmit = null } = {}, attempt = 1) {
  userId = userId ?? process.env.LOGIZARD_USER_ID;
  password = password ?? process.env.LOGIZARD_PASSWORD;
  if (!userId || !password) {
    console.error('❌ ログイン用のID/パスワードが設定されていません (.env を確認してください)。');
    process.exit(1);
  }
  await page.goto(`${BASE}/`, { waitUntil: 'domcontentloaded', timeout: 30000 });

  if (await isLoggedIn(page)) {
    console.log('ℹ セッション生存 (ログインスキップ)');
    return;
  }
  if (await page.locator('#PasswordCng_nowPass').isVisible().catch(() => false)) {
    throw new Error('パスワード変更ダイアログが表示されています。手動で対応してください。');
  }
  if (!(await page.locator('#user_id').isVisible().catch(() => false))) {
    await errorShot(page, 'unknown-screen');
    throw new Error(`ログインフォームもメニューも見つかりません (URL: ${page.url()})`);
  }

  await page.fill('#user_id', userId);
  await page.fill('#password', password);
  let clickOpts;
  if (beforeSubmit) {
    const ms = await beforeSubmit();
    if (Number.isFinite(ms) && ms > 0) clickOpts = { timeout: ms };
  }
  await Promise.all([
    page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 30000 }).catch(() => null),
    page.click('#login', clickOpts),
  ]);

  if (await isLoggedIn(page)) {
    console.log(`✅ ログイン成功${label ? ` (${label})` : ''}`);
    return;
  }
  const err = await page.locator('#err_login').inputValue().catch(() => '');
  if (attempt < 2) {
    console.log(`⚠ ログイン未確立${err ? ' (' + err + ')' : ''} → リトライ (${attempt + 1}/2)`);
    await page.waitForTimeout(3000);
    return login(page, { userId, password, label, beforeSubmit }, attempt + 1);
  }
  await errorShot(page, 'login-fail');
  throw new Error(`ログイン失敗${err ? ': ' + err : ''} (ID/パスワード/多重ログイン状態を確認してください)`);
}

// ---- 画面待ち ----
export async function waitBlockUIGone(page, label, timeoutMs = 60000) {
  const ok = await page.waitForFunction(
    () => !document.querySelector('.blockUI.blockOverlay'),
    undefined, { timeout: timeoutMs },
  ).then(() => true).catch(() => false);
  if (!ok) {
    await errorShot(page, `busy-timeout-${label}`);
    throw new Error(`${label}: 処理中表示が${Math.round(timeoutMs / 1000)}秒消えません`);
  }
}

export async function visibleModalText(page) {
  return page.evaluate(() => {
    const vis = (el) => {
      if (!el) return false;
      const s = getComputedStyle(el);
      if (s.display === 'none' || s.visibility === 'hidden' || +s.opacity === 0) return false;
      const r = el.getBoundingClientRect();
      return r.width > 0 && r.height > 0;
    };
    const dlgs = [...document.querySelectorAll('#popup_content, .ui-dialog, [class*="DIALOG"], [class*="dialog"]')].filter(vis);
    return dlgs.map((d) => d.innerText).join(' / ').replace(/\s+/g, ' ').trim();
  }).catch(() => '');
}

// ---- 自動印刷 (Adobe Acrobat のサイレント印刷 /t。auto-hokyu.js と同じ方式) ----
export const ACROBAT_CANDIDATES = [
  'C:/Program Files/Adobe/Acrobat DC/Acrobat/Acrobat.exe',
  'C:/Program Files (x86)/Adobe/Acrobat DC/Acrobat/Acrobat.exe',
  'C:/Program Files/Adobe/Acrobat Reader DC/Reader/AcroRd32.exe',
  'C:/Program Files (x86)/Adobe/Acrobat Reader DC/Reader/AcroRd32.exe',
];

// Acrobatが無い環境 (miniPC) 用のフォールバック: SumatraPDF のサイレント印刷 (-print-to)
// (2026-08-02 追加。Acrobatがある既存PCでは従来どおりAcrobat優先=挙動不変)
export const SUMATRA_CANDIDATES = [
  'C:/tools/SumatraPDF/SumatraPDF.exe',
  'C:/Program Files/SumatraPDF/SumatraPDF.exe',
];

// 2アップPDF生成: A4縦1枚に元PDFの2ページを上下に縮小配置
export async function makeTwoUpPdf(srcBytes) {
  const { PDFDocument } = await import('pdf-lib');
  const src = await PDFDocument.load(srcBytes);
  const out = await PDFDocument.create();
  const embedded = await out.embedPdf(src, src.getPageIndices());
  const A4W = 595.28, A4H = 841.89, M = 10; // pt, 余白
  for (let i = 0; i < embedded.length; i += 2) {
    const page = out.addPage([A4W, A4H]);
    const slotY = [A4H / 2, 0]; // 上段, 下段
    for (let j = 0; j < 2 && i + j < embedded.length; j++) {
      const ep = embedded[i + j];
      const scale = Math.min((A4W - 2 * M) / ep.width, (A4H / 2 - 2 * M) / ep.height);
      const w = ep.width * scale, h = ep.height * scale;
      page.drawPage(ep, { x: (A4W - w) / 2, y: slotY[j] + (A4H / 2 - h) / 2, width: w, height: h });
    }
  }
  return Buffer.from(await out.save());
}

// インストール済みプリンター名の一覧 (取得できなければ null = 判定不能)
let printerNamesCache;
function listPrinters() {
  if (printerNamesCache !== undefined) return printerNamesCache;
  try {
    const out = execFileSync('powershell', [
      '-NoProfile', '-Command',
      "[Console]::OutputEncoding=[System.Text.Encoding]::UTF8; $ErrorActionPreference='Stop'; Get-Printer | ForEach-Object { $_.Name }",
    ], { encoding: 'utf8', timeout: 20000, shell: false });
    printerNamesCache = out.split(/\r?\n/).map((s) => s.trim()).filter(Boolean);
  } catch {
    printerNamesCache = null;
  }
  return printerNamesCache;
}

/**
 * PDFを指定プリンターへサイレント印刷する。
 * @param pdfPath 印刷するPDF (読み取りのみ。このファイルは書き換えない)
 * @param opts.printer プリンター名 / opts.nup 1|2 / opts.enabled false で何もしない
 *        opts.tmpDir 2アップ用の一時PDFを置く場所 (既定=logs。元PDFの隣には書かない)
 * @returns true=印刷プロセスを起動できた / false=印刷できなかった / null=起動はしたが確認できない
 */
export async function printPdf(pdfPath, { printer, nup = 1, enabled = true, tmpDir = LOG_DIR, label = '' } = {}) {
  if (!enabled) { console.log('ℹ 自動印刷: 無効 (LOGIZARD_PRINT=0)'); return false; }
  if (!fs.existsSync(pdfPath)) { console.log(`⚠ 印刷対象がありません: ${pdfPath}`); return false; }
  const acrobat = ACROBAT_CANDIDATES.find((p) => fs.existsSync(p));
  const sumatra = acrobat ? null : SUMATRA_CANDIDATES.find((p) => fs.existsSync(p));
  if (!acrobat && !sumatra) {
    console.log(`⚠ Acrobat/SumatraPDFが見つかりません。印刷スキップ。手動で印刷してください: ${pdfPath}`);
    return false;
  }
  // プリンター名の実在確認 (Codex R4 高-2: 存在しないプリンター名でも成功扱いにしない)
  const printers = listPrinters();
  if (printers === null) {
    // 一覧を取れないときは印刷ジョブ自体は投げる (紙が出る可能性を残す) が、
    // 成功とは言わず PRINT_UNVERIFIED にして目視確認を促す (Codex R5)
    console.log('⚠ プリンター一覧を取得できませんでした。印刷ジョブは送りますが、成功確認はできません。');
  }
  if (printers && !printers.includes(printer)) {
    console.log(`⚠ プリンター "${printer}" が見つかりません。印刷できません。`);
    console.log(`   このPCのプリンター: ${printers.join(' / ') || '(なし)'}`);
    console.log('   .env の LOGIZARD_PRINTER を実際の名前に合わせてください。');
    return false;
  }

  let printTarget = pdfPath;
  if (Number(nup) === 2) {
    try {
      fs.mkdirSync(tmpDir, { recursive: true });
      const twoUp = await makeTwoUpPdf(fs.readFileSync(pdfPath));
      // ⚠ 一時PDFは必ずローカル(logs)に作る。共有ドライブ側には一切書き込まない
      printTarget = path.join(tmpDir, `print2up_${UNIQ}_${path.basename(pdfPath)}`);
      fs.writeFileSync(printTarget, twoUp);
      console.log(`ℹ 2アップ印刷用PDF生成: ${path.basename(printTarget)}`);
    } catch (e) {
      console.log(`⚠ 2アップ生成に失敗 (${e.message})。元PDFをそのまま印刷します。`);
      printTarget = pdfPath;
    }
  }

  const { spawn } = await import('child_process');

  if (sumatra) {
    // Sumatra: -print-to <printer> -silent は印刷スプール後に自動終了する
    // → detachedにせず終了を待つ (投げっぱなしだと呼び出し元プロセス/SSHセッション終了時に
    //    スプール完了前のSumatraが殺されて紙が出ない。2026-08-02 miniPCで実測)
    const child = spawn(sumatra, ['-print-to', printer, '-silent', printTarget], { stdio: 'ignore' });
    const code = await new Promise((resolve) => {
      const timer = setTimeout(() => resolve(null), 90000);
      child.once('exit', (c) => { clearTimeout(timer); resolve(c); });
      child.once('error', (err) => { clearTimeout(timer); console.log(`⚠ 印刷プロセスを起動できません: ${err.message}`); resolve(false); });
    });
    if (code === false) return false;
    if (code === null) {
      console.log(`⚠ SumatraPDFが90秒で終了しません${label ? ` [${label}]` : ''}。紙が出ているか目視で確認してください。`);
      try { child.kill(); } catch { /* 終了失敗は無視 */ }
      return null;
    }
    if (code !== 0) {
      console.log(`⚠ SumatraPDFの印刷が失敗しました (exit ${code})${label ? ` [${label}]` : ''}`);
      return false;
    }
    console.log(`🖨 印刷ジョブ送信${label ? ` [${label}]` : ''}: ${printer} (${Number(nup) === 2 ? '2アップ' : '等倍'} / Sumatra)`);
    if (printers === null) return null;
    return true;
  }

  // Acrobat: /h=最小化 /t=指定プリンタへサイレント印刷 (Acrobatは常駐するため終了は待たない)
  const child = spawn(acrobat, ['/h', '/t', printTarget, printer], { detached: true, stdio: 'ignore' });
  // 起動そのものの成否を確認してから成功と言う (Codex R4 高-2)。
  // spawnイベントが来れば起動成功、errorなら失敗、どちらも来なければ判定不能(null)
  const spawned = await new Promise((resolve) => {
    const timer = setTimeout(() => resolve(null), 5000);
    child.once('spawn', () => { clearTimeout(timer); resolve(true); });
    child.once('error', (err) => { clearTimeout(timer); console.log(`⚠ 印刷プロセスを起動できません: ${err.message}`); resolve(false); });
  });
  child.unref();
  if (spawned === false) return false;
  if (spawned === null) {
    console.log(`⚠ 印刷プロセスの起動を確認できませんでした${label ? ` [${label}]` : ''}。紙が出ているか目視で確認してください。`);
    return null;
  }
  console.log(`🖨 印刷ジョブ送信${label ? ` [${label}]` : ''}: ${printer} (${Number(nup) === 2 ? '2アップ' : '等倍'})`);
  if (printers === null) return null; // プリンター実在を確認できていない = 成功と言い切らない
  return true;
}

export function writeResult(prefix, result) {
  fs.mkdirSync(LOG_DIR, { recursive: true });
  fs.writeFileSync(path.join(LOG_DIR, `${prefix}_result_${UNIQ}.json`), JSON.stringify(result, null, 2), 'utf8');
  fs.appendFileSync(path.join(LOG_DIR, 'run.log'),
    `${new Date().toISOString()} ${prefix.toUpperCase()} ${result.status} ${result.detail || ''}\n`, 'utf8');
}
