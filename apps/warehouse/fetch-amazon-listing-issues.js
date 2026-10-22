/**
 * fetch-amazon-listing-issues.js — Amazon の出品の状態 (BUYABLE / DISCOVERABLE) と出品の問題 (issues・出品停止) の日次の写し
 *   (2026-10-22・AI の分析とアラート。読み手 = alert-amazon-listing.js --kind listing・後で AI の分析)
 *
 * SP-API Listings Items 2021-08-01 の searchListingsItems (includedData = summaries, issues) で自社の出品を取り、
 * 日 × SKU の状態 (amazon_listing_status_daily) と issue (amazon_listing_issue_daily) をその日の分ごと置き換える。前の日との差はアラートの側が出す。
 *
 * 取り方 (公式の仕様):
 *   - 🚨 searchListingsItems は「ページ送りで辿れるのは 1 回の検索につき 1,000 件まで」(モデルの numberOfResults の説明)。
 *     → 出品の作成日時 (createdAfter / createdBefore) の窓に分け、numberOfResults が 1,000 を超える窓は半分に割る (窓の境は 1 秒重ねて SKU で重複を外す)。
 *       割れない (10 秒の窓で 1,000 超) 窓は「上限に当たった」と数え、その回は partial
 *   - その後、アラートの対象 (直近 30 日に売れた or FBA の在庫がある・amazon_sku_fees の SKU) で走査に出てこなかった SKU を identifiers (identifiersType = SKU・20 件ずつ) で引き直す。
 *     引いても返らない SKU は status = 'NOT_FOUND' の行として残す (出品が消えた = BUYABLE でない)。呼び出しが失敗した SKU は書かない (分からない = 差を出さない)
 *   - pageSize = 20 (上限)。rate = 公式の usage plan 5 回/秒 (burst 5) → 間隔 250ms (4 回/秒) で呼ぶ。429 はライブラリが待ってやり直す
 *   - marketplaceIds = 日本だけ。issue の文言は既定の言語 (最初の marketplace の言語 = 日本語)
 *
 * 状態の写し: 回の記録 amazon_listing_fetch_runs (日ごと)。全部の窓を辿れて引き直しも全部通った回 = complete・それ以外 = partial。
 *   🚨 その日に complete の写しがあるのに partial の回が来たら置き換えない (取り直しで写しを痩せさせない)。0 件・失敗が多い (呼び出しの 10% 超) 回は exit 1
 *   90 日より古い写しは消す (カート価格の写しと同じ)
 *
 * 使い方:
 *   node apps/warehouse/fetch-amazon-listing-issues.js               (全部)
 *   node apps/warehouse/fetch-amazon-listing-issues.js --limit 40    (走査で 40 件・引き直し 40 件まで = partial で書く。初回の動作確認用)
 *   node apps/warehouse/fetch-amazon-listing-issues.js --dry-run     (対象の数とセラーID の出どころだけ。API を呼ばない・書かない)
 */
import 'dotenv/config';
import { initDB, getDB } from './db.js';
import {
  MARKETPLACE_ID, businessDate, addDays, skuKey, createRateGate, callWithRetry, loadWatchSkus, resolveSellerId, isMainModule, getSpClient,
} from './amazon-listing-common.js';

export const PAGE_SIZE = 20;                 // searchListingsItems の pageSize の上限
export const CALL_INTERVAL_MS = 250;         // 公式 5 回/秒 → 4 回/秒
export const MAX_PAGEABLE = 1000;            // 1 回の検索で辿れる上限 (公式)
export const MIN_WINDOW_MS = 10000;          // これより狭い窓は割らない (上限に当たったと数える)。割った子の窓は必ず親より狭い (境を 1 秒重ねても縮む幅)
export const SCAN_FROM = '2005-01-01T00:00:00.000Z';
export const LOOKUP_BATCH = 20;              // identifiers の上限
export const KEEP_DAYS = 90;
export const ERROR_RATE_LIMIT = 0.1;
// 出品が止まる・カタログから外れる (BUYABLE でなくなる) enforcement。SEARCH_SUPPRESSED (検索に出ない) は別の列
const SUPPRESS_ACTIONS = new Set(['LISTING_SUPPRESSED', 'CATALOG_ITEM_REMOVED']);

export function ensureListingTables(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS amazon_listing_status_daily (
    date_jst          TEXT NOT NULL,
    sku_key           TEXT NOT NULL,     -- 小文字 (amazon_sku_fees と同じ鍵)
    seller_sku        TEXT NOT NULL,     -- Amazon の登録どおり
    marketplace_id    TEXT NOT NULL,
    asin              TEXT,
    product_type      TEXT,
    condition_type    TEXT,
    fn_sku            TEXT,
    item_name         TEXT,
    status            TEXT NOT NULL,     -- summaries.status を並べて 'BUYABLE,DISCOVERABLE'。どちらも無い = ''・出品が見つからない = 'NOT_FOUND'
    is_buyable        INTEGER NOT NULL,
    is_discoverable   INTEGER NOT NULL,
    error_count       INTEGER NOT NULL,
    warning_count     INTEGER NOT NULL,
    suppressed        INTEGER NOT NULL,  -- issue の enforcements に LISTING_SUPPRESSED / CATALOG_ITEM_REMOVED がある
    search_suppressed INTEGER NOT NULL,  -- 同じく SEARCH_SUPPRESSED
    created_date      TEXT,
    last_updated_date TEXT,
    source            TEXT NOT NULL,     -- 'scan' (作成日時の窓の走査) | 'sku_lookup' (SKU で引き直し) | 'not_found'
    fetched_at        TEXT NOT NULL,
    PRIMARY KEY (date_jst, sku_key)
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_alsd_sku ON amazon_listing_status_daily(sku_key, date_jst)`);
  db.exec(`CREATE TABLE IF NOT EXISTS amazon_listing_issue_daily (
    date_jst            TEXT NOT NULL,
    sku_key             TEXT NOT NULL,
    issue_key           TEXT NOT NULL,   -- code|severity|attributeNames (同じ SKU に同じ鍵が 2 つあれば #2 を付ける)
    code                TEXT NOT NULL,
    severity            TEXT NOT NULL,   -- ERROR / WARNING / INFO
    message             TEXT,
    categories          TEXT,            -- JSON の配列
    attribute_names     TEXT,            -- JSON の配列
    enforcement_actions TEXT,            -- JSON の配列 (LISTING_SUPPRESSED など)
    fetched_at          TEXT NOT NULL,
    PRIMARY KEY (date_jst, sku_key, issue_key)
  )`);
  db.exec(`CREATE TABLE IF NOT EXISTS amazon_listing_fetch_runs (
    date_jst        TEXT PRIMARY KEY,    -- その日の写しを作った (最後に置き換えた) 回
    status          TEXT NOT NULL,       -- complete | partial
    listings        INTEGER NOT NULL,
    not_found       INTEGER NOT NULL,
    windows         INTEGER NOT NULL,
    capped_windows  INTEGER NOT NULL,
    calls           INTEGER NOT NULL,
    errors          INTEGER NOT NULL,
    lookup_requested INTEGER NOT NULL,
    note            TEXT,
    started_at      TEXT NOT NULL,
    finished_at     TEXT NOT NULL
  )`);
}

const arr = (x) => (Array.isArray(x) ? x : []);
const str = (x) => (x == null ? null : String(x));

/**
 * searchListingsItems の 1 件 → 写しの 1 行 + issue。marketplace が違う summary・issue は外す
 * (issue の marketplaceIds が無いときは全部の marketplace = 日本にも当たると読む)
 */
export function parseListingItem(item, marketplaceId, { source = 'scan' } = {}) {
  const sellerSku = String(item?.sku ?? '').trim();
  if (!sellerSku) return null;
  const summary = arr(item.summaries).find((s) => s?.marketplaceId === marketplaceId) || null;
  const statuses = [...new Set(arr(summary?.status).map(String))].sort();
  const issues = [];
  const seen = new Map();
  for (const is of arr(item.issues)) {
    const mps = arr(is?.marketplaceIds);
    if (mps.length && !mps.includes(marketplaceId)) continue;
    const code = String(is?.code ?? '').trim();
    const severity = String(is?.severity ?? '').trim().toUpperCase();
    if (!code || !severity) continue;
    const attrs = [...new Set(arr(is.attributeNames).map(String))].sort();
    const actions = [...new Set(arr(is?.enforcements?.actions).map((a) => String(a?.action ?? '')).filter(Boolean))].sort();
    const base = `${code}|${severity}|${attrs.join(',')}`;
    const n = (seen.get(base) || 0) + 1;
    seen.set(base, n);
    issues.push({
      issue_key: n === 1 ? base : `${base}#${n}`, code, severity,
      message: is.message == null ? null : String(is.message).slice(0, 1000),
      categories: JSON.stringify(arr(is.categories).map(String)), attribute_names: JSON.stringify(attrs), enforcement_actions: JSON.stringify(actions),
      _actions: actions,
    });
  }
  return {
    sku_key: skuKey(sellerSku), seller_sku: sellerSku, marketplace_id: marketplaceId,
    asin: str(summary?.asin), product_type: str(summary?.productType), condition_type: str(summary?.conditionType), fn_sku: str(summary?.fnSku),
    item_name: summary?.itemName == null ? null : String(summary.itemName).slice(0, 500),
    status: statuses.join(','), is_buyable: statuses.includes('BUYABLE') ? 1 : 0, is_discoverable: statuses.includes('DISCOVERABLE') ? 1 : 0,
    error_count: issues.filter((i) => i.severity === 'ERROR').length, warning_count: issues.filter((i) => i.severity === 'WARNING').length,
    suppressed: issues.some((i) => i._actions.some((a) => SUPPRESS_ACTIONS.has(a))) ? 1 : 0,
    search_suppressed: issues.some((i) => i._actions.includes('SEARCH_SUPPRESSED')) ? 1 : 0,
    created_date: str(summary?.createdDate), last_updated_date: str(summary?.lastUpdatedDate),
    source, issues,
  };
}

/** 引いても返らなかった SKU の行 (出品が見つからない) */
export function notFoundRow(rawSku, marketplaceId) {
  return {
    sku_key: skuKey(rawSku), seller_sku: String(rawSku).trim(), marketplace_id: marketplaceId, asin: null, product_type: null, condition_type: null, fn_sku: null, item_name: null,
    status: 'NOT_FOUND', is_buyable: 0, is_discoverable: 0, error_count: 0, warning_count: 0, suppressed: 0, search_suppressed: 0, created_date: null, last_updated_date: null,
    source: 'not_found', issues: [],
  };
}

/**
 * 作成日時の窓で全部の出品を辿る。call(query) は searchListingsItems の応答 ({ numberOfResults, pagination: { nextToken }, items }) を返す。
 * 戻り値 { items: Map(sku_key → item), windows, capped, errors, calls, stopped }
 *   limit > 0 = 集めた件数がそれに届いたら止める (stopped = true)
 */
export async function scanAllListings(call, { fromIso = SCAN_FROM, toIso, limit = 0, log = () => {} } = {}) {
  const items = new Map();
  let windows = 0, capped = 0, errors = 0, calls = 0, collisions = 0, stopped = false;
  const add = (list) => {
    for (const it of arr(list)) {
      const k = skuKey(it?.sku);
      if (!k) continue;
      if (items.has(k)) { if (String(items.get(k).sku).trim() !== String(it.sku).trim()) collisions++; continue; }
      items.set(k, it);
    }
  };
  const base = { pageSize: PAGE_SIZE, sortBy: 'sku', sortOrder: 'ASC' };
  const stack = [[Date.parse(fromIso), Date.parse(toIso)]];
  while (stack.length) {
    if (limit > 0 && items.size >= limit) { stopped = true; break; }
    const [a, b] = stack.pop();
    const win = { ...base, createdAfter: new Date(a).toISOString(), createdBefore: new Date(b).toISOString() };
    let res;
    try { calls++; res = await call(win); } catch (e) { errors++; log(`  ✗ 窓 ${win.createdAfter}〜${win.createdBefore}: ${String(e.message || e).slice(0, 150)}`); continue; }
    const n = Number(res?.numberOfResults);
    if (Number.isFinite(n) && n > MAX_PAGEABLE && b - a > MIN_WINDOW_MS) {
      add(res?.items);   // 1 ページ目も捨てない (重複は外す)
      const mid = Math.floor((a + (b - a) / 2) / 1000) * 1000;
      // 境の 1 秒を両方に入れる (createdAfter / createdBefore が境を含むか公式に書いていないため)
      stack.push([Math.max(a, mid - 1000), b], [a, Math.min(b, mid + 1000)]);
      continue;
    }
    windows++;
    if (Number.isFinite(n) && n > MAX_PAGEABLE) capped++;
    add(res?.items);
    let token = res?.pagination?.nextToken || null;
    while (token) {
      if (limit > 0 && items.size >= limit) { stopped = true; break; }
      try { calls++; res = await call({ ...win, pageToken: token }); } catch (e) { errors++; log(`  ✗ 窓 ${win.createdAfter} のページ送り: ${String(e.message || e).slice(0, 150)}`); break; }
      add(res?.items);
      token = res?.pagination?.nextToken || null;
    }
    if (stopped) break;
  }
  return { items, windows, capped, errors, calls, collisions, stopped };
}

/**
 * 走査に出てこなかった SKU を identifiers で引き直す。wanted = [{ key, raw, trusted }] (raw = Amazon に送る SKU・trusted = 登録どおりの綴りと分かっている)
 * 戻り値 { found: Map(sku_key → item), notFound: [wanted の要素], failed: [raw], skipped: [raw (カンマ入りで送れない)], calls, errors }
 */
export async function lookupSkus(call, wanted, { log = () => {} } = {}) {
  const found = new Map();
  const notFound = [], failed = [], skipped = [];
  let calls = 0, errors = 0;
  const sendable = [];
  for (const w of wanted) (String(w.raw).includes(',') ? skipped : sendable).push(w);
  for (let i = 0; i < sendable.length; i += LOOKUP_BATCH) {
    const batch = sendable.slice(i, i + LOOKUP_BATCH);
    try {
      calls++;
      const res = await call({ identifiers: batch.map((w) => w.raw), identifiersType: 'SKU', pageSize: PAGE_SIZE });
      const got = new Map(arr(res?.items).map((it) => [skuKey(it?.sku), it]));
      for (const w of batch) { if (got.has(w.key)) found.set(w.key, got.get(w.key)); else notFound.push(w); }
    } catch (e) {
      errors++;
      failed.push(...batch.map((w) => w.raw));
      log(`  ✗ SKU の引き直し batch ${i / LOOKUP_BATCH}: ${String(e.message || e).slice(0, 150)}`);
    }
  }
  return { found, notFound, failed, skipped: skipped.map((w) => w.raw), calls, errors };
}

/**
 * その日の写しを置き換える (1 取引)。🚨 その日に complete があり今回が partial なら置き換えない (= { skipped: 'complete_exists' })
 * rows = parseListingItem / notFoundRow の行
 */
export function saveListingSnapshot(db, { date, rows, run, fetchedAt = new Date().toISOString(), keepDays = KEEP_DAYS }) {
  ensureListingTables(db);
  const existing = db.prepare(`SELECT status FROM amazon_listing_fetch_runs WHERE date_jst = ?`).get(date);
  if (existing?.status === 'complete' && run.status !== 'complete') return { skipped: 'complete_exists', saved: 0, issues: 0 };
  const insRow = db.prepare(`INSERT INTO amazon_listing_status_daily (date_jst, sku_key, seller_sku, marketplace_id, asin, product_type, condition_type, fn_sku, item_name,
      status, is_buyable, is_discoverable, error_count, warning_count, suppressed, search_suppressed, created_date, last_updated_date, source, fetched_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
  const insIssue = db.prepare(`INSERT INTO amazon_listing_issue_daily (date_jst, sku_key, issue_key, code, severity, message, categories, attribute_names, enforcement_actions, fetched_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
  let saved = 0, issues = 0;
  db.transaction(() => {
    db.prepare(`DELETE FROM amazon_listing_issue_daily WHERE date_jst = ?`).run(date);
    db.prepare(`DELETE FROM amazon_listing_status_daily WHERE date_jst = ?`).run(date);
    const seen = new Set();
    for (const r of rows) {
      if (!r || seen.has(r.sku_key)) continue;
      seen.add(r.sku_key);
      insRow.run(date, r.sku_key, r.seller_sku, r.marketplace_id, r.asin, r.product_type, r.condition_type, r.fn_sku, r.item_name,
        r.status, r.is_buyable, r.is_discoverable, r.error_count, r.warning_count, r.suppressed, r.search_suppressed, r.created_date, r.last_updated_date, r.source, fetchedAt);
      saved++;
      for (const i of r.issues) { insIssue.run(date, r.sku_key, i.issue_key, i.code, i.severity, i.message, i.categories, i.attribute_names, i.enforcement_actions, fetchedAt); issues++; }
    }
    db.prepare(`INSERT INTO amazon_listing_fetch_runs (date_jst, status, listings, not_found, windows, capped_windows, calls, errors, lookup_requested, note, started_at, finished_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(date_jst) DO UPDATE SET status = excluded.status, listings = excluded.listings, not_found = excluded.not_found, windows = excluded.windows,
        capped_windows = excluded.capped_windows, calls = excluded.calls, errors = excluded.errors, lookup_requested = excluded.lookup_requested,
        note = excluded.note, started_at = excluded.started_at, finished_at = excluded.finished_at`)
      .run(date, run.status, saved, rows.filter((r) => r?.source === 'not_found').length, run.windows || 0, run.capped || 0, run.calls || 0, run.errors || 0,
        run.lookupRequested || 0, run.note || null, run.startedAt || fetchedAt, run.finishedAt || fetchedAt);
    const pruneBefore = addDays(date, -keepDays);
    db.prepare(`DELETE FROM amazon_listing_issue_daily WHERE date_jst < ?`).run(pruneBefore);
    db.prepare(`DELETE FROM amazon_listing_status_daily WHERE date_jst < ?`).run(pruneBefore);
    db.prepare(`DELETE FROM amazon_listing_fetch_runs WHERE date_jst < ?`).run(pruneBefore);
  })();
  return { skipped: null, saved, issues };
}

/**
 * 引き直しの対象 = アラートの対象 + amazon_sku_fees の SKU のうち、走査に出てこなかったもの。
 * 🚨 注文 (sp-api-orders.js)・手数料・在庫の表の SKU は小文字にそろえてある = Amazon の登録の綴りではない (Codex R1 P2)。
 *   綴り = 前の日までの出品の写しで Amazon が返した seller_sku (trusted = true) → 無ければ手元の綴り (trusted = false = 引いても返らなくても「無い」とは言わない)
 */
export function lookupTargets(db, watch, scannedKeys) {
  const want = new Map();
  for (const [k, w] of watch) want.set(k, w.raw || k);
  const hasFees = db.prepare(`SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'amazon_sku_fees'`).get();
  if (hasFees) for (const r of db.prepare(`SELECT seller_sku FROM amazon_sku_fees WHERE seller_sku IS NOT NULL AND TRIM(seller_sku) != ''`).all()) {
    const k = skuKey(r.seller_sku);
    if (!want.has(k)) want.set(k, String(r.seller_sku).trim());
  }
  const spelled = new Map();
  ensureListingTables(db);
  for (const r of db.prepare(`SELECT sku_key, seller_sku FROM amazon_listing_status_daily WHERE source != 'not_found' ORDER BY date_jst`).all()) spelled.set(r.sku_key, r.seller_sku);
  return [...want].filter(([k]) => !scannedKeys.has(k))
    .map(([key, raw]) => (spelled.has(key) ? { key, raw: spelled.get(key), trusted: true } : { key, raw, trusted: false }))
    .sort((x, y) => (x.key < y.key ? -1 : 1));
}

/**
 * 引いても返らなかった SKU のうち「出品が無い」と言えるものだけ NOT_FOUND の行にする。
 *   走査が全部終わった (窓の失敗・上限・途中で止めた が無い) = 走査に出なかった時点で無い / 綴りが確か (trusted) = 引いて返らなければ無い。
 *   それ以外 (綴りが分からない) = 分からない = 書かない (差を出さない)
 */
export function missingRows(notFound, { scanComplete, marketplaceId }) {
  const rows = [], unknown = [];
  for (const w of notFound) (scanComplete || w.trusted ? rows.push(notFoundRow(w.raw, marketplaceId)) : unknown.push(w.raw));
  return { rows, unknown };
}

export function parseArgs(argv = process.argv.slice(2)) {
  const i = argv.indexOf('--limit');
  const limit = i >= 0 ? parseInt(argv[i + 1], 10) || 0 : 0;
  return { limit, dryRun: argv.includes('--dry-run') };
}

async function main() {
  const args = parseArgs();
  await initDB();
  const db = getDB();
  ensureListingTables(db);
  const today = businessDate();
  const mp = MARKETPLACE_ID();
  const watch = loadWatchSkus(db, { today });
  const seller = resolveSellerId();
  console.log(`[ListingIssues] ${today} marketplace=${mp} / アラートの対象 (30 日に売れた or FBA 在庫) ${watch.size} SKU / セラーID ${seller.sellerId ? `あり (${seller.from})` : `無し: ${seller.error}`}`);
  if (args.dryRun) { console.log('[ListingIssues] --dry-run のため API 呼び出しなし'); return; }
  if (!seller.sellerId) throw new Error(seller.error);

  const sp = await getSpClient();
  const gate = createRateGate({ intervalMs: CALL_INTERVAL_MS });
  const startedAt = new Date().toISOString();
  const call = (query) => callWithRetry(() => sp.callAPI({
    operation: 'searchListingsItems', endpoint: 'listingsItems', path: { sellerId: seller.sellerId },
    query: { marketplaceIds: [mp], includedData: ['summaries', 'issues'], ...query },
    options: { version: '2021-08-01' },
  }), { gate, label: 'searchListingsItems' });

  // ① 作成日時の窓で全部
  const scan = await scanAllListings(call, { toIso: startedAt, limit: args.limit, log: console.error });
  console.log(`  [scan] 出品 ${scan.items.size} 件 / 窓 ${scan.windows} (上限に当たった ${scan.capped}) / 呼び出し ${scan.calls} / 失敗 ${scan.errors}${scan.collisions ? ` / 大文字小文字だけ違う SKU ${scan.collisions}` : ''}`);
  // ② 走査に出なかった対象を SKU で引き直す
  let wanted = lookupTargets(db, watch, new Set(scan.items.keys()));
  if (args.limit > 0) wanted = wanted.slice(0, args.limit);
  const look = await lookupSkus(call, wanted, { log: console.error });
  console.log(`  [lookup] 引き直し ${wanted.length} SKU → 見つかった ${look.found.size} / 見つからない ${look.notFound.length} / 失敗 ${look.failed.length} / 送れない (カンマ入り) ${look.skipped.length}`);

  const rows = [];
  for (const it of scan.items.values()) rows.push(parseListingItem(it, mp, { source: 'scan' }));
  for (const it of look.found.values()) rows.push(parseListingItem(it, mp, { source: 'sku_lookup' }));
  const scanComplete = scan.errors === 0 && scan.capped === 0 && !scan.stopped;
  const missing = missingRows(look.notFound, { scanComplete, marketplaceId: mp });
  rows.push(...missing.rows);
  const calls = scan.calls + look.calls;
  const errors = scan.errors + look.errors;
  const complete = errors === 0 && scan.capped === 0 && !scan.stopped && args.limit === 0;
  const notes = [];
  if (scan.capped) notes.push(`上限に当たった窓 ${scan.capped}`);
  if (args.limit) notes.push(`--limit ${args.limit}`);
  if (look.skipped.length) notes.push(`カンマ入り SKU ${look.skipped.length}`);
  if (missing.unknown.length) notes.push(`綴りが分からず無いと言えない SKU ${missing.unknown.length}`);
  if (rows.filter(Boolean).length === 0) { console.error('[ListingIssues] FATAL: 0 件 (書かない)'); process.exitCode = 1; return; }

  const run = { status: complete ? 'complete' : 'partial', windows: scan.windows, capped: scan.capped, calls, errors, lookupRequested: wanted.length, note: notes.join(' / ') || null, startedAt, finishedAt: new Date().toISOString() };
  const r = saveListingSnapshot(db, { date: today, rows: rows.filter(Boolean), run });
  const buyable = rows.filter((x) => x?.is_buyable).length;
  const withErr = rows.filter((x) => x?.error_count > 0).length;
  const head = r.skipped ? '⚠️' : (complete ? '✓' : '⚠️');
  console.log(`${head} [出品の状態] ${today} ${run.status}${r.skipped ? ' (その日に complete の写しがあるので置き換えない)' : ''}: 出品 ${r.skipped ? rows.length : r.saved} (BUYABLE ${buyable}・ERROR あり ${withErr}・見つからない ${missing.rows.length}) / 呼び出し ${calls} / 失敗 ${errors}${notes.length ? ` / ${notes.join(' / ')}` : ''}`);
  if (errors > calls * ERROR_RATE_LIMIT) {
    console.error(`[ListingIssues] FATAL: 呼び出しの失敗が多い (${errors}/${calls})`);
    process.exitCode = 1;
  }
}

if (isMainModule(import.meta.url)) main().catch((e) => {
  console.error(`[ListingIssues] FATAL: ${e.message}`);
  process.exitCode = 1;
});
