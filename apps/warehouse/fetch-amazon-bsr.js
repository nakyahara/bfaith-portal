/**
 * fetch-amazon-bsr.js — Amazon の売れ筋順位 (BSR) の毎日の履歴 (2026-10-22・AI の分析)
 *
 * SP-API Catalog Items 2022-04-01 の searchCatalogItems (identifiers = ASIN 最大 20・includedData = salesRanks・pageSize = 20) で
 * 自社の ASIN の順位を毎朝取り、日 × ASIN × 分類 (classificationRanks = 細かい分類・displayGroupRanks = 大きい分類 の両方) で
 * amazon_catalog_rank_daily に保存する。
 *
 *   - fba.db の daily_snapshots.sales_rank (FBA の SKU だけ・1 つの順位) とは別の表。こちらは自社の全部の ASIN・分類ごと
 *   - 列は Company DB の受け皿 snapshots.catalog_asin_rank_daily (migration 0005) にそろえる = 後で送るとき並べ替えるだけ:
 *       date_jst → snapshot_date / marketplace_id / asin / rank_type ('classification' | 'display_group') / category_id (classificationId | websiteDisplayGroup) /
 *       category_title / rank (> 0) / observed_at。鍵も同じ (日, marketplace, ASIN, 種別, 分類)。🚨 違う分類の順位を前の日と比べない (D-18)
 *     送るのは後 (今回は送らない)
 *   - 取れた ASIN ごとの結果を amazon_catalog_rank_fetch に残す ('ranked' = 順位あり / 'no_rank' = 返ったが順位なし / 'not_returned' = 返らなかった)。
 *     呼び出しが失敗した batch の ASIN は触らない (前の回の同じ日の値を残す = 分からないを「順位なし」にしない)
 *   - 同じ日に取り直すと、取れた ASIN の行をその日の分だけ置き換える (冪等)。400 日より古い行は消す
 *   - rate = 公式の usage plan 2 回/秒 (burst 2) → 間隔 550ms。429 はライブラリが待ってやり直す。~2,500 ASIN = 125 回 ≈ 1.5 分
 *
 * 対象の ASIN: amazon_sku_fees の asin (カート価格と同じアクティブな SKU) + 直近 30 日に売れた注文の asin
 *
 * 使い方:
 *   node apps/warehouse/fetch-amazon-bsr.js               (全部)
 *   node apps/warehouse/fetch-amazon-bsr.js --limit 40    (先頭 40 ASIN だけ・初回の動作確認用)
 *   node apps/warehouse/fetch-amazon-bsr.js --dry-run     (対象の数だけ。API を呼ばない・書かない)
 */
import 'dotenv/config';
import { initDB, getDB } from './db.js';
import { MARKETPLACE_ID, businessDate, addDays, createRateGate, callWithRetry, isMainModule, getSpClient, WATCH_DAYS } from './amazon-listing-common.js';

export const BATCH_SIZE = 20;            // identifiers の上限
export const CALL_INTERVAL_MS = 550;     // 公式 2 回/秒 → 約 1.8 回/秒
export const KEEP_DAYS = 400;
export const ERROR_RATE_LIMIT = 0.1;
const ASIN_RE = /^[A-Z0-9]{10}$/;

export function ensureRankTables(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS amazon_catalog_rank_daily (
    date_jst        TEXT NOT NULL,     -- Company DB の snapshot_date
    marketplace_id  TEXT NOT NULL,
    asin            TEXT NOT NULL,
    rank_type       TEXT NOT NULL CHECK (rank_type IN ('classification', 'display_group')),
    category_id     TEXT NOT NULL,     -- classificationId / websiteDisplayGroup
    category_title  TEXT,
    rank            INTEGER NOT NULL CHECK (rank > 0),
    observed_at     TEXT NOT NULL,     -- 取った時刻 (ISO・UTC)
    PRIMARY KEY (date_jst, marketplace_id, asin, rank_type, category_id)
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_acrd_asin ON amazon_catalog_rank_daily(asin, date_jst)`);
  db.exec(`CREATE TABLE IF NOT EXISTS amazon_catalog_rank_fetch (
    date_jst        TEXT NOT NULL,
    marketplace_id  TEXT NOT NULL,
    asin            TEXT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('ranked', 'no_rank', 'not_returned')),
    rank_rows       INTEGER NOT NULL,
    fetched_at      TEXT NOT NULL,
    PRIMARY KEY (date_jst, marketplace_id, asin)
  )`);
}

/**
 * searchCatalogItems の 1 件 → 順位の行。marketplace が違う salesRanks は外す・rank が正の整数でない・分類が無いものは外す。
 * 同じ (種別, 分類) が 2 つあれば良い方 (小さい順位) を残す
 */
export function parseSalesRanks(item, marketplaceId) {
  const best = new Map();
  for (const sr of (Array.isArray(item?.salesRanks) ? item.salesRanks : [])) {
    if (sr?.marketplaceId && sr.marketplaceId !== marketplaceId) continue;
    const put = (rankType, id, title, rank) => {
      const r = Number(rank);
      const cid = String(id ?? '').trim();
      if (!cid || !Number.isInteger(r) || r <= 0) return;
      const k = `${rankType}|${cid}`;
      if (!best.has(k) || best.get(k).rank > r) best.set(k, { rank_type: rankType, category_id: cid, category_title: title == null ? null : String(title), rank: r });
    };
    for (const c of (Array.isArray(sr?.classificationRanks) ? sr.classificationRanks : [])) put('classification', c?.classificationId, c?.title, c?.rank);
    for (const g of (Array.isArray(sr?.displayGroupRanks) ? sr.displayGroupRanks : [])) put('display_group', g?.websiteDisplayGroup, g?.title, g?.rank);
  }
  return [...best.values()];
}

/** 1 batch を取る (nextToken があれば辿る)。戻り値 Map(asin → item) */
export async function fetchBatch(call, asins, marketplaceId) {
  const got = new Map();
  let token = null;
  let guard = 0;
  do {
    const res = await call({ marketplaceIds: [marketplaceId], identifiers: asins, identifiersType: 'ASIN', includedData: ['salesRanks'], pageSize: BATCH_SIZE, ...(token ? { pageToken: token } : {}) });
    for (const it of (Array.isArray(res?.items) ? res.items : [])) { const a = String(it?.asin ?? '').toUpperCase(); if (a && !got.has(a)) got.set(a, it); }
    token = res?.pagination?.nextToken || null;
  } while (token && ++guard < 5);
  return got;
}

/**
 * 取れた batch の結果を保存する (1 取引)。results = [{ asins: [..要求した ASIN], items: Map(asin → item) }] (失敗した batch は渡さない)
 * 要求した ASIN ごとにその日の行を消して入れ直す = 同じ日の取り直しは置き換え (冪等)
 */
export function saveRanks(db, { date, marketplaceId, results, observedAt = new Date().toISOString(), keepDays = KEEP_DAYS }) {
  ensureRankTables(db);
  const del = db.prepare(`DELETE FROM amazon_catalog_rank_daily WHERE date_jst = ? AND marketplace_id = ? AND asin = ?`);
  const ins = db.prepare(`INSERT INTO amazon_catalog_rank_daily (date_jst, marketplace_id, asin, rank_type, category_id, category_title, rank, observed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`);
  const up = db.prepare(`INSERT INTO amazon_catalog_rank_fetch (date_jst, marketplace_id, asin, status, rank_rows, fetched_at) VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(date_jst, marketplace_id, asin) DO UPDATE SET status = excluded.status, rank_rows = excluded.rank_rows, fetched_at = excluded.fetched_at`);
  const count = { ranked: 0, no_rank: 0, not_returned: 0, rows: 0 };
  db.transaction(() => {
    for (const b of results) {
      for (const asin of b.asins) {
        del.run(date, marketplaceId, asin);
        const item = b.items.get(asin);
        const ranks = item ? parseSalesRanks(item, marketplaceId) : [];
        for (const r of ranks) ins.run(date, marketplaceId, asin, r.rank_type, r.category_id, r.category_title, r.rank, observedAt);
        const status = !item ? 'not_returned' : (ranks.length ? 'ranked' : 'no_rank');
        up.run(date, marketplaceId, asin, status, ranks.length, observedAt);
        count[status]++;
        count.rows += ranks.length;
      }
    }
    const pruneBefore = addDays(date, -keepDays);
    db.prepare(`DELETE FROM amazon_catalog_rank_daily WHERE date_jst < ?`).run(pruneBefore);
    db.prepare(`DELETE FROM amazon_catalog_rank_fetch WHERE date_jst < ?`).run(pruneBefore);
  })();
  return count;
}

/** 対象の ASIN (大文字・10 桁の形だけ・並びは固定) */
export function loadTargetAsins(db, { today, days = WATCH_DAYS } = {}) {
  const set = new Set();
  const has = (t) => !!db.prepare(`SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?`).get(t);
  if (has('amazon_sku_fees')) for (const r of db.prepare(`SELECT DISTINCT asin FROM amazon_sku_fees WHERE asin IS NOT NULL AND asin != ''`).all()) set.add(String(r.asin).trim().toUpperCase());
  if (has('raw_sp_orders')) {
    for (const r of db.prepare(`SELECT DISTINCT asin FROM raw_sp_orders WHERE asin IS NOT NULL AND asin != '' AND order_status NOT IN ('Cancelled') AND purchase_date >= ?`).all(addDays(today, -days))) {
      set.add(String(r.asin).trim().toUpperCase());
    }
  }
  return [...set].filter((a) => ASIN_RE.test(a)).sort();
}

/** 全部の batch を回す。戻り値 { results, errors, batches } */
export async function fetchAllRanks(call, asins, marketplaceId, { log = () => {} } = {}) {
  const results = [];
  let errors = 0, batches = 0;
  for (let i = 0; i < asins.length; i += BATCH_SIZE) {
    const batch = asins.slice(i, i + BATCH_SIZE);
    batches++;
    try {
      results.push({ asins: batch, items: await fetchBatch(call, batch, marketplaceId) });
    } catch (e) {
      errors++;
      log(`  ✗ searchCatalogItems batch ${i / BATCH_SIZE}: ${String(e.message || e).slice(0, 150)}`);
    }
    if ((i / BATCH_SIZE) % 25 === 0) console.log(`  [bsr] ${Math.min(i + BATCH_SIZE, asins.length)}/${asins.length}`);
  }
  return { results, errors, batches };
}

async function main() {
  const argv = process.argv.slice(2);
  const li = argv.indexOf('--limit');
  const limit = li >= 0 ? parseInt(argv[li + 1], 10) || 0 : 0;
  await initDB();
  const db = getDB();
  ensureRankTables(db);
  const today = businessDate();
  const mp = MARKETPLACE_ID();
  let asins = loadTargetAsins(db, { today });
  if (limit > 0) asins = asins.slice(0, limit);
  console.log(`[BSR] ${today} marketplace=${mp} / 対象 ${asins.length} ASIN (呼び出し ${Math.ceil(asins.length / BATCH_SIZE)} 回の見込み)`);
  if (asins.length === 0) { console.log('[BSR] 対象なし、終了'); return; }
  if (argv.includes('--dry-run')) { console.log('[BSR] --dry-run のため API 呼び出しなし'); return; }

  const sp = await getSpClient();
  const gate = createRateGate({ intervalMs: CALL_INTERVAL_MS });
  const call = (query) => callWithRetry(() => sp.callAPI({ operation: 'searchCatalogItems', endpoint: 'catalogItems', query, options: { version: '2022-04-01' } }),
    { gate, label: 'searchCatalogItems' });
  const { results, errors, batches } = await fetchAllRanks(call, asins, mp, { log: console.error });
  const c = saveRanks(db, { date: today, marketplaceId: mp, results });
  const head = errors ? '⚠️' : '✓';
  console.log(`${head} [BSR] ${today}: 順位あり ${c.ranked} / 順位なし ${c.no_rank} / 返らない ${c.not_returned} / 順位の行 ${c.rows} / batch 失敗 ${errors}/${batches}`);
  if (errors > batches * ERROR_RATE_LIMIT) {
    console.error(`[BSR] FATAL: batch の失敗が多い (${errors}/${batches})`);
    process.exitCode = 1;
  }
}

if (isMainModule(import.meta.url)) main().catch((e) => {
  console.error(`[BSR] FATAL: ${e.message}`);
  process.exitCode = 1;
});
