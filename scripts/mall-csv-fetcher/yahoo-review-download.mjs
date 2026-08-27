/**
 * Yahoo!ショッピング ストクリ「商品レビューチェックツール」ZIP 自動ダウンロード (P2-Y PR-Y-A)
 *
 * らくらくーぽん Yahoo 版置換 (『らくらくーぽんYahoo版_置換_要件設計_20260827.md』§Y1) の C1。
 * 毎晩 **直近 90 日 (画面の上限) を全量** 取得して incoming/yahoo-review/ に置く (取込は import-yahoo-review.js)。
 * 楽天版のような 30 日ローリングにしないのは、削除検知が「同一母集団の完全スナップショット」を要求するため。
 *
 * ★実測で確定した仕様 (2026-08-27 プローブ):
 *   - URL: https://pro.store.yahoo.co.jp/pro.<account>/review/low_rate/item (商品レビューチェックツール)
 *   - フィルタ: 評価 5 段階 checkbox (name=filterRating, value 5..1、既定 全ON) / 期間 text (#filterTermFrom/#filterTermTo、
 *     'YYYY/MM/DD'、**最大 90 日**) / 動画あり (#video) / 画像あり (#image) ⚠️動画・画像を ON にすると絞り込まれる (触らない)
 *   - 「絞り込み」ボタンで条件反映。画面に「表示件数： 1〜20 件目/ N件」が出る (この N を全量検証に使う)
 *   - 「ダウンロード」ボタン → ZIP (`YYYYMMDD_YYYYMMDD_ItemReview.zip`、中に cp932 CSV 1 本)。
 *     列 = 評価日 / 評価点数 / 商品名 / 商品コード / 注文ID / コメントタイトル / コメント内容 / 動画本数 / 画像枚数 / いいね数
 *   - 90 日で約 1,150 行・135KB (楽天のような 1.2MB キャップは観測されていない)
 *
 * ガード: DL 後に取込レシピ (prepareYahooReviewFile) で実パース → ヘッダ 10 列一致・行数 = 画面の N・評価日が窓内、
 *         を満たしたときだけ incoming 投入 (削除検知の材料は「検証済みの全量」だけ — Codex 設計R1 High)。
 * 実行: HEADLESS=1 node scripts/mall-csv-fetcher/yahoo-review-download.mjs
 *   env: REVIEW_DAYS=90 (1..90) / WAREHOUSE_DATA_DIR or DATA_DIR
 * exit: 0=成功 / 1=失敗 (通知済み) / 2=env / 3=2FA_REQUIRED (fetch-all はリトライしない)
 */
import { mkdir, copyFile, readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { dirname, join, basename } from 'node:path';
import { openYahooContext, gotoStorePage, ensureStoreLogin, STORE_TOP_URL } from './lib-yahoo-login.mjs';
import { initRunLog, sendGChat, buildErrorReport } from './lib-notify.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DL_DIR = join(__dirname, 'downloads');
const OUT_DIR = join(__dirname, 'spike-output');
const WAREHOUSE_DATA_DIR = (process.env.WAREHOUSE_DATA_DIR || process.env.DATA_DIR || '').trim();
const INCOMING_DIR = WAREHOUSE_DATA_DIR ? join(WAREHOUSE_DATA_DIR, 'incoming', 'yahoo-review') : null;
const TOOL_PATH = 'review/low_rate/item';

function jstNow() { return new Date(Date.now() + 9 * 3600 * 1000); }
const isoDate = (dt) => dt.toISOString().slice(0, 10);
const slashDate = (iso) => iso.replace(/-/g, '/');

/** 対象窓: 直近 REVIEW_DAYS 日 (既定 90 = 画面の上限。終端=当日) */
function computeWindow() {
  const days = Math.min(Math.max(parseInt(process.env.REVIEW_DAYS, 10) || 90, 1), 90);
  const now = jstNow();
  return { from: isoDate(new Date(now.getTime() - (days - 1) * 86400000)), to: isoDate(now) };
}

async function snap(page, label) {
  await page.screenshot({ path: join(OUT_DIR, `yreview_${label}.png`), fullPage: true }).catch(() => {});
  console.log(`  [snap] yreview_${label}  url=${page.url()}`);
}

/** report_fetch_log へ記録 (fail-soft) */
async function logFetch(entry) {
  if (!WAREHOUSE_DATA_DIR) return;
  try {
    const { default: Database } = await import('better-sqlite3');
    const { ensureRppTables } = await import('../../apps/warehouse/rakuten-ads-rpp-lib.js');
    const db = new Database(join(WAREHOUSE_DATA_DIR, 'warehouse.db'));
    try {
      db.pragma('busy_timeout = 5000');
      ensureRppTables(db);
      db.prepare(`INSERT INTO report_fetch_log (fetched_at, mall, report_type, period_from, period_to, file_name, file_sha256, file_bytes, status, message)
                  VALUES (?, 'yahoo', ?, ?, ?, ?, ?, ?, ?, ?)`)
        .run(new Date().toISOString(), entry.report_type, entry.period_from, entry.period_to, entry.file_name || null,
             entry.file_sha256 || null, entry.file_bytes ?? null, entry.status, String(entry.message || '').slice(0, 500));
    } finally { db.close(); }
  } catch (e) { console.warn(`  ⚠ [fetch-log] DB記録失敗 (DLは成功扱い): ${e.message}`); }
}

/** フィルタを「評価 5 段階 ON・動画/画像 OFF・期間 = 窓」にして絞り込み、画面の総件数を返す */
async function applyFilter(page, win) {
  const ok = await page.evaluate(({ from, to }) => {
    const ratings = [...document.querySelectorAll('input[name=filterRating]')];
    if (ratings.length !== 5) return { ok: false, reason: `filterRating が ${ratings.length} 個` };
    for (const cb of ratings) if (!cb.checked) cb.click();
    for (const id of ['video', 'image']) { const el = document.getElementById(id); if (el && el.checked) el.click(); }
    const f = document.getElementById('filterTermFrom'), t = document.getElementById('filterTermTo');
    if (!f || !t) return { ok: false, reason: '期間 input が無い' };
    const set = (el, v) => { el.focus(); el.value = v; el.dispatchEvent(new Event('input', { bubbles: true })); el.dispatchEvent(new Event('change', { bubbles: true })); el.blur(); };
    set(f, from); set(t, to);
    return { ok: true };
  }, { from: slashDate(win.from), to: slashDate(win.to) });
  if (!ok.ok) throw new Error(`FILTER: ${ok.reason} (画面仕様変更の疑い)`);
  const btn = page.locator('button, input[type=submit], input[type=button], a').filter({ hasText: /絞り込み/ }).first();
  if (!(await btn.isVisible().catch(() => false))) throw new Error('FILTER: 「絞り込み」ボタンが見つからない');
  await Promise.all([page.waitForLoadState('networkidle', { timeout: 30000 }).catch(() => {}), btn.click()]);
  await page.waitForTimeout(1500);
  const state = await page.evaluate(() => {
    const text = document.body.innerText.replace(/\s+/g, ' ');
    const m = text.match(/件目\/\s*([\d,]+)\s*件/);
    const ratings = [...document.querySelectorAll('input[name=filterRating]')].map((cb) => cb.checked);
    const media = ['video', 'image'].map((id) => document.getElementById(id)?.checked || false);
    const from = document.getElementById('filterTermFrom')?.value, to = document.getElementById('filterTermTo')?.value;
    return { total: m ? Number(m[1].replace(/,/g, '')) : null, ratings, media, from, to, hasDownload: /ダウンロード/.test(text) };
  });
  if (!state.ratings.every(Boolean) || state.media.some(Boolean)) throw new Error(`FILTER: 絞り込み後の条件が想定外 (ratings=${state.ratings} media=${state.media})`);
  if (state.from !== slashDate(win.from) || state.to !== slashDate(win.to)) throw new Error(`FILTER: 期間が反映されていない (${state.from}〜${state.to})`);
  if (state.total == null) throw new Error('FILTER: 「N件」の表示が読めない (0 件 or 画面仕様変更)');
  return state.total;
}

async function fetchWindow(page, win) {
  const label = `${win.from}〜${win.to}`;
  console.log(`\n--- Yahoo レビュー ZIP ${label} ---`);
  await gotoStorePage(page, `${STORE_TOP_URL}/${TOOL_PATH}`, '商品レビューチェックツール');
  const total = await applyFilter(page, win);
  console.log(`  [filter] 画面の総件数 = ${total}`);
  if (total === 0) {
    await logFetch({ report_type: 'yreview_zip', period_from: win.from, period_to: win.to, status: 'empty', message: 'レビュー0件' });
    return { files: 0, rows: 0, empty: true };
  }
  const dlBtn = page.locator('button, input[type=submit], input[type=button], a').filter({ hasText: /ダウンロード/ }).first();
  const dlPromise = page.waitForEvent('download', { timeout: 120000 });
  await dlBtn.click();
  const download = await dlPromise;
  const ts = new Date().toISOString().replace(/[:.]/g, '').replace('Z', '');
  const dest = join(DL_DIR, `yreview_d${win.from}_${win.to}_${ts}.zip`);
  await download.saveAs(dest);
  const buf = await readFile(dest);
  const sha256 = createHash('sha256').update(buf).digest('hex');
  console.log(`  ✅ ダウンロード成功: ${download.suggestedFilename()} → ${basename(dest)} (${buf.length.toLocaleString()} bytes)`);

  const { prepareYahooReviewFile } = await import('../../apps/warehouse/yahoo-review-lib.js');
  const p = prepareYahooReviewFile(basename(dest), buf);
  if (!p.ok) throw new Error(`DL_VERIFY: 取込レシピを通らない ZIP/CSV (${p.error})。画面仕様変更の疑い`);
  const rows = p.records.length + p.conflicts.length;
  if (rows !== total) throw new Error(`DL_VERIFY: 行数 ${rows} が画面の件数 ${total} と一致しない (部分ダウンロード/フィルタ不一致の疑い)。incoming には置かない`);
  if (p.dateFrom && (p.dateFrom < win.from || p.dateTo > win.to)) throw new Error(`DL_VERIFY: 窓 ${label} の外の評価日 (${p.dateFrom}〜${p.dateTo}) が混入`);
  console.log(`  [verify] ${p.label} (${p.dateFrom}〜${p.dateTo}) = 画面件数と一致`);

  if (INCOMING_DIR) {
    await mkdir(INCOMING_DIR, { recursive: true });
    await copyFile(dest, join(INCOMING_DIR, basename(dest)));
    console.log(`  → incoming投入: ${join(INCOMING_DIR, basename(dest))}`);
  } else {
    console.log('  ⚠ WAREHOUSE_DATA_DIR 未設定のため incoming/ 投入スキップ');
  }
  await logFetch({ report_type: 'yreview_zip', period_from: win.from, period_to: win.to, file_name: basename(dest), file_sha256: sha256, file_bytes: buf.length, status: 'ok', message: `${rows}件` });
  return { files: 1, rows, empty: false };
}

async function main() {
  const runLog = initRunLog('yahoo-review');
  await mkdir(DL_DIR, { recursive: true });
  await mkdir(OUT_DIR, { recursive: true });
  const win = computeWindow();
  let context;
  try {
    context = await openYahooContext();
  } catch (e) {
    console.error(`⚠️ ブラウザ起動失敗: ${e.message}`);
    await sendGChat(buildErrorReport({ mall: 'yahoo-review', failures: [{ reportType: 'browser', error: e.message }], logPath: runLog.logPath, repro: 'node scripts/mall-csv-fetcher/yahoo-review-download.mjs' }), 'yahoo-review');
    process.exitCode = 1;
    return;
  }
  const page = context.pages()[0] || (await context.newPage());
  page.on('dialog', (d) => { console.log(`  [dialog] ${d.message()}`); d.accept().catch(() => {}); });
  const outcomes = [];
  try {
    console.log('=== Yahoo レビュー ZIP 自動DL ===');
    console.log(`対象窓: ${win.from} 〜 ${win.to} (全量)`);
    await ensureStoreLogin(page);
    const r = await fetchWindow(page, win);
    outcomes.push({ spec: 'review', ym: `${win.from}〜${win.to}`, status: r.empty ? 'empty' : 'ok' });
    console.log(`\n=== summary: ${outcomes.map((o) => `${o.ym}=${o.status}`).join(' / ')} ===`);
    console.log('  → 取込は次回 daily-sync (import-yahoo-review.js) が実行');
  } catch (err) {
    const is2fa = String(err.message).startsWith('2FA_REQUIRED');
    console.error(`\n⚠️ ${err.message}`);
    await snap(page, 'error');
    await logFetch({ report_type: 'yreview_zip', period_from: win.from, period_to: win.to, status: 'error', message: err.message });
    await sendGChat(buildErrorReport({
      mall: 'yahoo-review', outcomes, logPath: runLog.logPath,
      failures: [{ reportType: is2fa ? '2FA_REQUIRED (Yahoo セッション切れ)' : 'yreview_zip', error: err.message, url: page.url(), screenshot: join(OUT_DIR, 'yreview_error.png') }],
      repro: is2fa ? 'miniPC のデスクトップ「Yahoo-Relogin.bat」で再ログイン (確認コードはメール)' : 'node scripts/mall-csv-fetcher/yahoo-review-download.mjs (手動DL → incoming/yahoo-review/ でも可。ファイル名に _dYYYY-MM-DD_YYYY-MM-DD_ を付けると削除検知に使う)',
    }), 'yahoo-review');
    process.exitCode = is2fa ? 3 : 1;
  } finally {
    if (process.env.HEADLESS !== '1') { console.log('\n目視用にブラウザを60秒開いたままにします。'); await page.waitForTimeout(60000).catch(() => {}); }
    await context.close().catch(() => {});
  }
}

main();
