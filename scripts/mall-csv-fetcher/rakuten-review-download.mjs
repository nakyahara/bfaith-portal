/**
 * 楽天RMS「レビューチェックツール」CSV自動ダウンロード (mall-csv-fetcher P2 PR-A)
 *
 * らくらくーぽん置換 (AI_reference『らくらくーぽん置換_P2_要件設計_20260716.md』) の C1。
 * 毎晩 直近 REVIEW_DAYS (default 30) 日窓のレビューCSVを取得して incoming/rakuten-review/ に置く
 * (取込は import-rakuten-review.js)。レビューは編集・返信で遡及変化するため毎晩窓ごと取り直し UPSERT。
 *
 * ★実測で確定した仕様 (2026-07-16 R0プローブ):
 *   - 入口: RMS>コミュニティ (left_navi=61)>みんなのレビュー>レビューチェックツール
 *     → https://review.rms.rakuten.co.jp/ (※要「みんなのレビュー」権限=RMS権限分類「通常」)
 *   - ⚠️review.rms への直goto は、ログイン直後に mainmenu の規約遵守確認
 *     「RMSを利用します」をクリックして act=login を完了させていないと /error/auth 302ループになる
 *     → ensureRmsLogin 後に必ず passRmsInterstitials() を通す
 *   - 検索は GET パラメータ: /search/index/?sy&sm&sd&sh&si&ey&em&ed&eh&ei&ev&tc&kw&ao&st
 *     (s*=開始 e*=終了 年/月/日/時/分、ev=評価 0=全て、tc=0 商品+ショップ、st=1 新着順)
 *   - CSVは検索結果画面の「CSVダウンロード」リンク (同期DL、CP932)
 *   - ⚠️出力は新着順で ≈1.2MB 上限に切られる (全期間指定→3,000行=2026-03以降しか返らない実測)
 *     → 行数/バイトのガードで窓を再帰分割 (バックフィルは REVIEW_BACKFILL=YYYY-MM:YYYY-MM で月ループ)
 *
 * 実行: node scripts/mall-csv-fetcher/rakuten-review-download.mjs
 *   env: HEADLESS=1 / REVIEW_DAYS=30 / REVIEW_BACKFILL=2019-01:2026-07 (月窓ループ、日次窓は取らない)
 */

import { mkdir, copyFile, readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { dirname, join, basename } from 'node:path';
import { openContext, ensureRmsLogin, safeHost, assertLoginEnv } from './lib-rakuten-login.mjs';
import { initRunLog, sendGChat, buildErrorReport } from './lib-notify.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DL_DIR = join(__dirname, 'downloads');
const OUT_DIR = join(__dirname, 'spike-output');

const COMMUNITY_URL = 'https://mainmenu.rms.rakuten.co.jp/?left_navi=61';
const REVIEW_HOST = 'review.rms.rakuten.co.jp';

const WAREHOUSE_DATA_DIR = (process.env.WAREHOUSE_DATA_DIR || process.env.DATA_DIR || '').trim();
const INCOMING_DIR = WAREHOUSE_DATA_DIR ? join(WAREHOUSE_DATA_DIR, 'incoming', 'rakuten-review') : null;

// 上限ガード: 実測キャップ (3,000行/1.2MB) より手前で「切られた疑い」と判定して窓分割
const SPLIT_ROWS = 2900;
const SPLIT_BYTES = 1_000_000;

function jstNow() { return new Date(Date.now() + 9 * 3600 * 1000); }
const isoDate = (dt) => dt.toISOString().slice(0, 10);

/** 対象窓: 直近 REVIEW_DAYS 日 (終端=当日。投稿は即日CSVに出る) */
function computeDefaultWindow() {
  const days = Math.min(Math.max(parseInt(process.env.REVIEW_DAYS, 10) || 30, 1), 120);
  const now = jstNow();
  const from = new Date(now.getTime() - (days - 1) * 86400000);
  return { from: isoDate(from), to: isoDate(now) };
}

/** REVIEW_BACKFILL=YYYY-MM:YYYY-MM → 月窓の配列 (古い順) */
function computeBackfillWindows() {
  const spec = (process.env.REVIEW_BACKFILL || '').trim();
  if (!spec) return null;
  const m = spec.match(/^(\d{4})-(\d{2}):(\d{4})-(\d{2})$/);
  if (!m) throw new Error(`REVIEW_BACKFILL は YYYY-MM:YYYY-MM 形式 (got: ${spec})`);
  const windows = [];
  let [y, mo] = [Number(m[1]), Number(m[2])];
  const endYm = `${m[3]}-${m[4]}`;
  while (`${y}-${String(mo).padStart(2, '0')}` <= endYm) {
    const first = `${y}-${String(mo).padStart(2, '0')}-01`;
    const last = isoDate(new Date(Date.UTC(y, mo, 0)));
    windows.push({ from: first, to: last });
    mo === 12 ? (y++, mo = 1) : mo++;
  }
  if (windows.length === 0) throw new Error('REVIEW_BACKFILL の範囲が空');
  return windows;
}

function searchUrl(win) {
  const [sy, sm, sd] = win.from.split('-').map(Number);
  const [ey, em, ed] = win.to.split('-').map(Number);
  const p = new URLSearchParams({
    sy, sm, sd, sh: '0', si: '0', ey, em, ed, eh: '23', ei: '59',
    ev: '0', tc: '0', kw: '', ao: 'A', st: '1',
  });
  return `https://${REVIEW_HOST}/search/index/?${p}`;
}

async function snap(page, label) {
  await page.screenshot({ path: join(OUT_DIR, `rreview_${label}.png`), fullPage: true }).catch(() => {});
  console.log(`  [snap] rreview_${label}  url=${page.url()}`);
}

/** report_fetch_log へ記録 (fail-soft) */
async function logFetch(entry) {
  if (!WAREHOUSE_DATA_DIR) {
    console.log(`  [fetch-log] WAREHOUSE_DATA_DIR 未設定のためDB記録スキップ (${entry.report_type}: ${entry.status})`);
    return;
  }
  try {
    const { default: Database } = await import('better-sqlite3');
    const { ensureRppTables } = await import('../../apps/warehouse/rakuten-ads-rpp-lib.js');
    const db = new Database(join(WAREHOUSE_DATA_DIR, 'warehouse.db'));
    try {
      db.pragma('busy_timeout = 5000');
      ensureRppTables(db);
      db.prepare(`INSERT INTO report_fetch_log
        (fetched_at, mall, report_type, period_from, period_to, file_name, file_sha256, file_bytes, status, message)
        VALUES (?, 'rakuten', ?, ?, ?, ?, ?, ?, ?, ?)`)
        .run(new Date().toISOString(), entry.report_type, entry.period_from, entry.period_to,
             entry.file_name || null, entry.file_sha256 || null, entry.file_bytes ?? null,
             entry.status, String(entry.message || '').slice(0, 500));
    } finally {
      db.close();
    }
  } catch (e) {
    console.warn(`  ⚠ [fetch-log] DB記録失敗 (DLは成功扱い): ${e.message}`);
  }
}

/** ログイン直後の mainmenu インタースティシャルを通過する。
 *  「RMSを利用します」(規約遵守確認) のクリックは review.rms 認可の必須ステップ (実測)。
 *  「RMSメインメニューへ進む」(重要お知らせ) は確認チェックONで次回から出ない。 */
async function passRmsInterstitials(page) {
  for (let i = 0; i < 5; i++) {
    const agree = page.locator('a:has-text("RMSを利用します"), button:has-text("RMSを利用します")').first();
    const proceed = page.locator('a:has-text("RMSメインメニューへ進む"), button:has-text("RMSメインメニューへ進む")').first();
    if (await agree.isVisible().catch(() => false)) {
      console.log('  [notice] 規約遵守確認「RMSを利用します」を通過');
      await agree.click().catch(() => {});
    } else if (await proceed.isVisible().catch(() => false)) {
      console.log('  [notice] 重要お知らせ「RMSメインメニューへ進む」を通過 (確認チェックON)');
      const chk = page.locator('input[type=checkbox]').first();
      if (await chk.isVisible().catch(() => false)) await chk.check().catch(() => {});
      await proceed.click().catch(() => {});
    } else {
      return;
    }
    await page.waitForLoadState('domcontentloaded').catch(() => {});
    await page.waitForTimeout(3000);
  }
}

/** コミュニティページ経由でレビューチェックツールへ入る (SSOハンドオフ)。到達済みなら素通り */
async function gotoReviewTool(page) {
  if (safeHost(page.url()) === REVIEW_HOST) return;
  for (let attempt = 1; attempt <= 3; attempt++) {
    await page.goto(COMMUNITY_URL, { waitUntil: 'domcontentloaded', timeout: 30000 }).catch(() => {});
    await page.waitForTimeout(3000);
    let toolHref = '';
    for (const frame of page.frames()) {
      toolHref = await frame.evaluate(() => {
        const a = [...document.querySelectorAll('a')].find((el) => (el.innerText || '').includes('レビューチェックツール'));
        return a ? a.href : '';
      }).catch(() => '');
      if (toolHref) break;
    }
    if (!toolHref) {
      throw new Error('PERMISSION?: コミュニティにレビューチェックツールのリンクが無い (R-Login権限分類が「通常」以外に戻っていないか確認)');
    }
    await page.goto(toolHref, { waitUntil: 'domcontentloaded', timeout: 30000 }).catch(() => {});
    await page.waitForTimeout(4000);
    if (safeHost(page.url()) === REVIEW_HOST) return;
    console.log(`  [retry ${attempt}] レビューツール到達失敗 (現URL=${page.url().slice(0, 70)})`);
  }
  throw new Error('レビューチェックツールに到達できない (/error/auth ループの可能性。規約確認クリック済みか確認)');
}

/** 1窓のCSVを取得。キャップ疑いなら窓を半分に割って再帰。
 *  @returns {files: number, rows: number, empty: boolean} */
async function fetchWindow(page, win, depth = 0) {
  const label = `${win.from}〜${win.to}`;
  console.log(`\n--- レビューCSV ${label}${depth > 0 ? ` (分割 depth=${depth})` : ''} ---`);
  await page.goto(searchUrl(win), { waitUntil: 'domcontentloaded', timeout: 30000 }).catch(() => {});
  await page.waitForTimeout(3000);
  if (safeHost(page.url()) !== REVIEW_HOST) {
    throw new Error(`検索画面に到達できない (現URL=${page.url().slice(0, 70)})`);
  }

  const csvLink = page.locator('a:has-text("CSVダウンロード")').first();
  if (!(await csvLink.isVisible().catch(() => false))) {
    // 検索結果0件はCSVリンク自体が出ない (2015年窓で実測)。ただし「リンクが無い」だけでは
    // エラー画面・仕様変更と区別できない (Codex R1 medium) → 検索フォームが正しく描画されている
    // (= 検索画面には到達できている) ことを肯定証拠として要求し、無ければ error にする
    const formOk = await page.locator('select[name="sy"]').first().isVisible().catch(() => false)
      && await page.locator('select[name="ev"]').first().isVisible().catch(() => false);
    const looksError = /\/error\//.test(page.url());
    if (!formOk || looksError) {
      throw new Error(`空判定の肯定証拠なし (検索フォーム不可視 or エラーURL)。画面仕様変更/権限/認可切れの疑い (URL=${page.url().slice(0, 70)})`);
    }
    console.log('  [empty] 検索フォーム描画済み+CSVリンクなし = 該当期間のレビュー0件');
    await logFetch({ report_type: 'rreview_csv', period_from: win.from, period_to: win.to, status: 'empty', message: 'レビュー0件' });
    return { files: 0, rows: 0, empty: true };
  }

  const dlPromise = page.waitForEvent('download', { timeout: 120000 });
  await csvLink.click();
  const download = await dlPromise;

  const ts = new Date().toISOString().replace(/[:.]/g, '').replace('Z', '');
  const dest = join(DL_DIR, `rreview_csv_d${win.from}_${win.to}_${ts}.csv`);
  await download.saveAs(dest);
  const buf = await readFile(dest);
  const sha256 = createHash('sha256').update(buf).digest('hex');
  console.log(`  ✅ ダウンロード成功: ${basename(dest)} (${buf.length.toLocaleString()} bytes)`);

  // 取込レシピで実パース検証 — 通らないCSVは incoming に置かない
  const { prepareReviewFile } = await import('../../apps/warehouse/rakuten-review-lib.js');
  const p = prepareReviewFile(basename(dest), buf);
  if (!p.ok) throw new Error(`DL_VERIFY: 取込レシピを通らないCSV (${p.error})。画面仕様変更の疑い`);
  // 窓外の日付が混ざっていないか (検索パラメータ無視の検知)
  if (p.dateFrom && (p.dateFrom < win.from || p.dateTo > win.to)) {
    throw new Error(`DL_VERIFY: 窓 ${label} の外の投稿日 (${p.dateFrom}〜${p.dateTo}) が混入。検索条件が効いていない疑い`);
  }
  console.log(`  [verify] ${p.label} (${p.dateFrom}〜${p.dateTo})`);

  // キャップ疑い → 窓を半分に割って両側を取り直す (このファイルは捨てる。部分データを黙って混ぜない)
  if (p.records.length >= SPLIT_ROWS || buf.length >= SPLIT_BYTES) {
    if (win.from === win.to) {
      throw new Error(`DL_VERIFY: 1日窓 ${win.from} でも上限到達 (${p.records.length}行/${buf.length}bytes)。仕様確認が必要`);
    }
    if (depth >= 8) throw new Error(`窓分割が深すぎる (depth=${depth})。異常`);
    const fromMs = Date.parse(`${win.from}T00:00:00Z`);
    const toMs = Date.parse(`${win.to}T00:00:00Z`);
    const midIso = new Date(fromMs + Math.floor((toMs - fromMs) / 2)).toISOString().slice(0, 10);
    const nextIso = new Date(Date.parse(`${midIso}T00:00:00Z`) + 86400000).toISOString().slice(0, 10);
    console.log(`  ⚠ 上限到達の疑い (${p.records.length}行/${buf.length}bytes) → 窓分割: ${win.from}〜${midIso} + ${nextIso}〜${win.to}`);
    const a = await fetchWindow(page, { from: win.from, to: midIso }, depth + 1);
    const b = await fetchWindow(page, { from: nextIso, to: win.to }, depth + 1);
    return { files: a.files + b.files, rows: a.rows + b.rows, empty: false };
  }

  if (INCOMING_DIR) {
    await mkdir(INCOMING_DIR, { recursive: true });
    await copyFile(dest, join(INCOMING_DIR, basename(dest)));
    console.log(`  → incoming投入: ${join(INCOMING_DIR, basename(dest))}`);
  } else {
    console.log('  ⚠ WAREHOUSE_DATA_DIR 未設定のため incoming/ 投入スキップ');
  }
  await logFetch({
    report_type: 'rreview_csv', period_from: win.from, period_to: win.to,
    file_name: basename(dest), file_sha256: sha256, file_bytes: buf.length,
    status: 'ok', message: `${p.records.length}件`,
  });
  return { files: 1, rows: p.records.length, empty: false };
}

async function main() {
  assertLoginEnv();
  const runLog = initRunLog('rakuten-review');
  await mkdir(DL_DIR, { recursive: true });
  await mkdir(OUT_DIR, { recursive: true });

  const backfill = computeBackfillWindows();
  const windows = backfill || [computeDefaultWindow()];

  const context = await openContext();
  const page = context.pages()[0] || (await context.newPage());
  page.on('dialog', (d) => { console.log(`  [dialog] ${d.message()}`); d.accept().catch(() => {}); });

  const outcomes = [];
  const failures = [];
  try {
    console.log('=== 楽天レビューCSV 自動DL ===');
    console.log(`対象窓: ${windows.length}個 (${windows[0].from} 〜 ${windows[windows.length - 1].to})${backfill ? ' [バックフィル]' : ''}`);
    await ensureRmsLogin(page);
    await passRmsInterstitials(page);
    await gotoReviewTool(page);
    console.log(`  レビューチェックツール到達: ${page.url()}`);

    for (const win of windows) {
      const label = `${win.from}〜${win.to}`;
      try {
        const r = await fetchWindow(page, win);
        outcomes.push({ spec: 'review', ym: label, status: r.empty ? 'empty' : 'ok' });
      } catch (e) {
        console.error(`✗ レビューCSV ${label}: ${e.message}`);
        await snap(page, `${win.from}_error`);
        await logFetch({ report_type: 'rreview_csv', period_from: win.from, period_to: win.to, status: 'error', message: e.message });
        outcomes.push({ spec: 'review', ym: label, status: 'error' });
        failures.push({ reportType: 'rreview_csv', ym: label, error: e.message, url: page.url(), screenshot: join(OUT_DIR, `rreview_${win.from}_error.png`) });
        if (String(e.message).startsWith('2FA_REQUIRED')) throw e;
        if (!backfill) break;
        // バックフィルは1窓の失敗で残りを止めない (欠けた月は再実行で埋める)
      }
    }

    console.log(`\n=== summary: ${outcomes.map((o) => `${o.ym}=${o.status}`).join(' / ') || '(対象なし)'} ===`);
    if (failures.length > 0) {
      console.log('  → 失敗分は手動DLで埋められます (レビューチェックツール>CSVダウンロード → incoming/rakuten-review/)');
      await sendGChat(buildErrorReport({
        mall: 'rakuten-review', outcomes, failures, logPath: runLog.logPath,
        repro: 'node scripts/mall-csv-fetcher/rakuten-review-download.mjs',
      }), 'rakuten-review');
      process.exitCode = 1;
    } else {
      console.log('  → 取込は次回 daily-sync (import-rakuten-review.js) が実行');
    }
  } catch (err) {
    const is2fa = String(err.message).startsWith('2FA_REQUIRED');
    console.error(`\n⚠️ ${err.message}`);
    await snap(page, 'error');
    await sendGChat(buildErrorReport({
      mall: 'rakuten-review', outcomes, logPath: runLog.logPath,
      failures: [{
        reportType: is2fa ? '2FA_REQUIRED (全レポート停止)' : 'rreview(共通処理)',
        error: err.message, url: page.url(), screenshot: join(OUT_DIR, 'rreview_error.png'),
      }],
      repro: is2fa
        ? 'miniPCで $env:MANUAL=1; node scripts/mall-csv-fetcher/rakuten-login-spike.mjs → 手動ログイン+「信頼できる端末」登録 (14日ごと)'
        : 'node scripts/mall-csv-fetcher/rakuten-review-download.mjs',
    }), 'rakuten-review');
    process.exitCode = is2fa ? 3 : 1; // 3=手動対応必須 (fetch-all はリトライしない)
  } finally {
    if (process.env.HEADLESS !== '1') {
      console.log('\n目視用にブラウザを120秒開いたままにします。Ctrl+Cで終了可。');
      await page.waitForTimeout(120000).catch(() => {});
    }
    await context.close().catch(() => {});
  }
}

main();
