/**
 * ABA「Amazon検索用語 (Top Search Terms)」週次レポート取込
 *
 * SP-API GET_BRAND_ANALYTICS_SEARCH_TERMS_REPORT (要: ブランド登録 + Brand Analytics ロール)
 * を取得し aba.db に蓄積する。セラースプライト「注文ワード」の置換 (2026-07-27)。
 *
 * 実行: node apps/aba-keywords/fetch-aba-search-terms.js [--week YYYY-MM-DD] [--backfill N] [--dry-run]
 *   - 引数なし: 直近の「完了した週 (日曜〜土曜, JST)」を対象
 *   - 取込済みの週は即スキップ (冪等)。daily-sync から毎朝呼んでも新しい週だけ取り込む
 *   - レポート未公開 (FATAL/CANCELLED) は正常終了扱いでスキップ (公開まで毎朝リトライになる)
 *
 * メモリ注意: レポートは数百MB級。全体を JSON.parse せず streaming で流し込む
 * (miniPC は AES sorter OOM の前科があるため常に省メモリ側に倒す)。
 */
import 'dotenv/config';
import SellingPartner from 'amazon-sp-api';
import { createGunzip } from 'zlib';
import { Readable } from 'stream';
import { initAbaDB, closeAbaDB } from './db.js';
import { parseAbaReportStream, normalizeAbaItem } from './aba-report-parser.js';

const REPORT_TYPE = 'GET_BRAND_ANALYTICS_SEARCH_TERMS_REPORT';
const KEEP_WEEKS = Math.max(4, parseInt(process.env.ABA_KEEP_WEEKS || '26', 10) || 26);
const POLL_INTERVAL_MS = 15000;
const POLL_MAX = 120;           // 15s × 120 = 30分
const INSERT_BATCH = 5000;

// ---- 引数 ----
const args = process.argv.slice(2);
const DRY_RUN = args.includes('--dry-run');
function argValue(name) {
  const i = args.indexOf(name);
  return i !== -1 && args[i + 1] ? args[i + 1] : null;
}
const WEEK_ARG = argValue('--week');
const BACKFILL = Math.min(12, Math.max(1, parseInt(argValue('--backfill') || '1', 10) || 1));

// ---- JST 週計算 (toISOString の UTC ズレ罠を避けるため UTC+9 を明示加算) ----
function jstToday() {
  const t = new Date(Date.now() + 9 * 3600 * 1000);
  return { y: t.getUTCFullYear(), m: t.getUTCMonth(), d: t.getUTCDate(), dow: t.getUTCDay() };
}
function fmt(dateUtcMs) {
  const t = new Date(dateUtcMs);
  const p = (n) => String(n).padStart(2, '0');
  return `${t.getUTCFullYear()}-${p(t.getUTCMonth() + 1)}-${p(t.getUTCDate())}`;
}
/** 直近の完了済み週 (日曜開始) を新しい順に n 件返す */
function recentCompletedWeeks(n) {
  const { y, m, d } = jstToday();
  const todayMs = Date.UTC(y, m, d);
  // 直近の「終わった土曜」: 今日から遡って最初の土曜 (今日が土曜でも当週は未完了なので昨日から遡る)
  let sat = todayMs - 86400000;
  while (new Date(sat).getUTCDay() !== 6) sat -= 86400000;
  const weeks = [];
  for (let i = 0; i < n; i++) {
    const end = sat - i * 7 * 86400000;
    const start = end - 6 * 86400000;
    weeks.push({ weekStart: fmt(start), weekEnd: fmt(end) });
  }
  return weeks;
}
function weekFromStart(weekStart) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(weekStart)) throw new Error(`--week は YYYY-MM-DD 形式: ${weekStart}`);
  const [y, m, d] = weekStart.split('-').map(Number);
  const ms = Date.UTC(y, m - 1, d);
  if (new Date(ms).getUTCDay() !== 0) throw new Error(`--week は日曜日を指定: ${weekStart} は ${'日月火水木金土'[new Date(ms).getUTCDay()]}曜`);
  return { weekStart, weekEnd: fmt(ms + 6 * 86400000) };
}

// ---- SP-API ----
let spClient = null;
function getClient() {
  if (!process.env.SP_API_REFRESH_TOKEN || !process.env.SP_API_CLIENT_ID || !process.env.SP_API_CLIENT_SECRET) {
    throw new Error('SP-API 認証情報 (SP_API_REFRESH_TOKEN / SP_API_CLIENT_ID / SP_API_CLIENT_SECRET) が未設定');
  }
  if (!spClient) {
    spClient = new SellingPartner({
      region: 'fe',
      refresh_token: process.env.SP_API_REFRESH_TOKEN,
      credentials: {
        SELLING_PARTNER_APP_CLIENT_ID: process.env.SP_API_CLIENT_ID,
        SELLING_PARTNER_APP_CLIENT_SECRET: process.env.SP_API_CLIENT_SECRET,
        AWS_ACCESS_KEY_ID: process.env.AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY: process.env.AWS_SECRET_ACCESS_KEY,
      },
    });
  }
  return spClient;
}
const MARKETPLACE_ID = process.env.SP_API_MARKETPLACE_ID || 'A1VC38T7YXB528';

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function withTimeout(promise, ms, label = '') {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error(`タイムアウト (${ms / 1000}秒): ${label}`)), ms)),
  ]);
}

/**
 * 1週分を取得して取込。
 * @returns {'ingested'|'already'|'unavailable'} 結果種別
 */
async function ingestWeek(db, { weekStart, weekEnd }) {
  // 台帳に行がある = 取込完了済み (台帳は取込がすべて成功した後にだけ書くので、中断時は残らない)
  const existing = db.prepare('SELECT row_count FROM aba_weeks WHERE week_start = ?').get(weekStart);
  if (existing) {
    console.log(`[ABA] ${weekStart}〜${weekEnd}: 取込済み (${existing.row_count}行) → skip`);
    return 'already';
  }

  const sp = getClient();
  console.log(`[ABA] ${weekStart}〜${weekEnd}: createReport ...`);
  const created = await withTimeout(sp.callAPI({
    operation: 'createReport', endpoint: 'reports',
    body: {
      reportType: REPORT_TYPE,
      marketplaceIds: [MARKETPLACE_ID],
      reportOptions: { reportPeriod: 'WEEK' },
      dataStartTime: `${weekStart}T00:00:00Z`,
      dataEndTime: `${weekEnd}T23:59:59Z`,
    },
    options: { version: '2021-06-30' },
  }), 60000, 'createReport');

  let report;
  for (let i = 0; i < POLL_MAX; i++) {
    await sleep(POLL_INTERVAL_MS);
    report = await withTimeout(sp.callAPI({
      operation: 'getReport', endpoint: 'reports',
      path: { reportId: created.reportId },
      options: { version: '2021-06-30' },
    }), 15000, 'getReport');
    if (i % 4 === 0 || ['DONE', 'FATAL', 'CANCELLED'].includes(report.processingStatus)) {
      console.log(`[ABA] ポーリング ${i + 1}/${POLL_MAX}: ${report.processingStatus}`);
    }
    if (['DONE', 'FATAL', 'CANCELLED'].includes(report.processingStatus)) break;
  }
  if (report.processingStatus !== 'DONE') {
    // 未公開週 (集計がまだ) やデータなしは FATAL/CANCELLED で返る。
    // ハード失敗にすると公開までの数日間 毎朝 🔴 通知が出続けるため、正常スキップ扱い。
    console.log(`[ABA] ${weekStart}: レポート未生成 (${report.processingStatus}) → skip (公開後に自動取込)`);
    return 'unavailable';
  }

  const doc = await withTimeout(sp.callAPI({
    operation: 'getReportDocument', endpoint: 'reports',
    path: { reportDocumentId: report.reportDocumentId },
    options: { version: '2021-06-30' },
  }), 30000, 'getReportDocument');

  console.log(`[ABA] ダウンロード開始 (compression=${doc.compressionAlgorithm || 'none'})`);
  const res = await fetch(doc.url, { signal: AbortSignal.timeout(15 * 60 * 1000) });
  if (!res.ok || !res.body) throw new Error(`レポートDL失敗: HTTP ${res.status}`);
  let stream = Readable.fromWeb(res.body);
  if (doc.compressionAlgorithm === 'GZIP') stream = stream.pipe(createGunzip());

  if (DRY_RUN) {
    let count = 0;
    await parseAbaReportStream(stream, () => { count++; });
    console.log(`[ABA] dry-run: ${count}行 (書き込みなし)`);
    return 'ingested';
  }

  const insert = db.prepare(`
    INSERT OR REPLACE INTO aba_search_terms
      (week_start, department, search_term, search_frequency_rank, click_position,
       asin, product_title, click_share, conversion_share)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  const flushBatch = db.transaction((rows) => {
    for (const r of rows) {
      insert.run(weekStart, r.department, r.search_term, r.search_frequency_rank,
        r.click_position, r.asin, r.product_title, r.click_share, r.conversion_share);
    }
  });

  let pending = [];
  let rowCount = 0;
  let skipped = 0;
  const terms = new Set();
  // clickShareRank が無い場合に備えた連番 fallback (レポートは同一検索語の行が連続する前提)
  let lastTermKey = '';
  let posCounter = 0;

  await parseAbaReportStream(stream, (item) => {
    const row = normalizeAbaItem(item);
    if (!row) { skipped++; return; }
    const termKey = `${row.department}${row.search_term}`;
    if (termKey === lastTermKey) posCounter++; else { lastTermKey = termKey; posCounter = 1; }
    if (row.click_position == null) row.click_position = posCounter;
    terms.add(termKey);
    pending.push(row);
    if (pending.length >= INSERT_BATCH) { flushBatch(pending); rowCount += pending.length; pending = []; }
  });
  if (pending.length) { flushBatch(pending); rowCount += pending.length; }

  // 全行が normalize で弾かれた = レポートのフィールド名が想定と違う (仕様変更)。
  // 台帳に書いて「取込済み」にしてしまうとサイレント欠落になるためハード失敗させて通知に載せる
  if (rowCount === 0 && skipped > 0) {
    throw new Error(`全${skipped}行が必須フィールド欠落で skip — レポート形式が想定と異なる (normalizeAbaItem 要確認)`);
  }

  db.prepare(`
    INSERT OR REPLACE INTO aba_weeks (week_start, week_end, ingested_at, term_count, row_count)
    VALUES (?, ?, datetime('now'), ?, ?)
  `).run(weekStart, weekEnd, terms.size, rowCount);

  console.log(`[ABA] ${weekStart}: 取込完了 ${rowCount}行 / ${terms.size}検索語 (不正行skip=${skipped})`);
  return 'ingested';
}

/** 保持期間を過ぎた週から、監視ASINを含まない検索語を削除 */
function pruneOldWeeks(db, anchorWeekStart) {
  const [y, m, d] = anchorWeekStart.split('-').map(Number);
  const cutoff = fmt(Date.UTC(y, m - 1, d) - KEEP_WEEKS * 7 * 86400000);
  const result = db.prepare(`
    DELETE FROM aba_search_terms
    WHERE week_start < ?
      AND NOT EXISTS (
        SELECT 1 FROM aba_search_terms t2
        WHERE t2.week_start = aba_search_terms.week_start
          AND t2.department = aba_search_terms.department
          AND t2.search_term = aba_search_terms.search_term
          AND t2.asin IN (SELECT asin FROM aba_watch_asins)
      )
  `).run(cutoff);
  if (result.changes > 0) {
    console.log(`[ABA] prune: ${cutoff} より古い非監視 ${result.changes}行を削除`);
    db.pragma('wal_checkpoint(TRUNCATE)');
  }
}

async function main() {
  const db = initAbaDB();
  const targets = WEEK_ARG ? [weekFromStart(WEEK_ARG)] : recentCompletedWeeks(BACKFILL);

  let ingested = 0, already = 0, unavailable = 0;
  for (const week of targets) {
    const r = await ingestWeek(db, week);
    if (r === 'ingested') ingested++;
    else if (r === 'already') already++;
    else unavailable++;
  }
  if (!DRY_RUN && ingested > 0) pruneOldWeeks(db, targets[0].weekStart);

  const weekRows = db.prepare('SELECT COUNT(*) AS c FROM aba_weeks WHERE row_count > 0').get();
  closeAbaDB();
  // 最終行 = daily-sync が拾うサマリ
  console.log(`ABA検索ワード: 新規${ingested}週 / 取込済${already}週 / 未公開${unavailable}週 (DB保有${weekRows.c}週)`);
}

main().catch((err) => {
  console.error('[ABA] エラー:', err.message || err);
  closeAbaDB();
  process.exit(1);
});
