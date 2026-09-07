/**
 * 商品別 想定利益 — 出品列挙 + 価格取得 (miniPC で動く)
 *
 * 正本 = AI_reference『商品別想定利益_要件定義_20260907.md』§5 / §15-4
 *
 * 🚨 母集団は「出品列挙の結果」。売れた実績や手数料キャッシュを起点にしない (§5.1)。
 *    起点を間違えると「売れていない商品が出ない」という元の問題が再発する。
 *
 * 🚨 部分列挙で見つからないことを、削除・未出品の根拠にしない (§5.2.1)。
 *    今夜が partial なら前回の完全集合と UNION し、見つからなかった行は前回の期限を引き継ぐ。
 *
 * 使い方:
 *   node apps/expected-profit/fetch-listings.js            (全モール)
 *   node apps/expected-profit/fetch-listings.js --mall amazon
 */
import { getExpectedProfitDB, initExpectedProfitDB } from './db.js';
import { newRunId, nowIso, addDays } from './util.js';

// 失効期限 (§15-8)
const LISTING_ENUM_VALID_DAYS = 7;
const PRICE_VALID_DAYS = 3;

/** 前回の完全集合から消えた率がこれを超えたら partial 扱い (レポート破損の疑い) */
const DISAPPEARED_RATIO_LIMIT = 0.20;

const AMAZON_SHOP_ID = () =>
  `${process.env.SP_API_SELLER_ID || 'unknown'}@${process.env.SP_API_MARKETPLACE_ID || 'A1VC38T7YXB528'}`;
const RAKUTEN_SHOP_ID = () => process.env.RAKUTEN_SHOP_CODE || '1';

/** 整数円として読めた時だけ返す (price-update の toIntPrice と同じ規約) */
export function toIntPrice(v) {
  if (typeof v === 'number') return Number.isInteger(v) ? v : null;
  if (typeof v !== 'string') return null;
  const s = v.trim().replace(/,/g, '');
  if (!/^\d+$/.test(s)) return null;
  const n = Number(s);
  return Number.isSafeInteger(n) ? n : null;
}

/** Amazon 出品レポートの「ステータス」列 → listing_status */
export function amazonListingStatus(raw) {
  const s = String(raw || '').trim().toLowerCase();
  if (s === 'active') return 'active';
  if (s === 'inactive') return 'inactive';
  if (s === 'incomplete') return 'incomplete';
  return 'unknown';
}

/** レポート行 → snapshot 行 (純関数。テストで固定する) */
export function amazonRowToSnapshot(row, { runId, shopId, fetchedAt, validUntil }) {
  const sku = (row['出品者SKU'] || row['seller-sku'] || '').trim();
  const asin = (row['商品ID'] || row['asin1'] || '').trim();
  const price = toIntPrice(row['価格'] ?? row['price']);
  const channel = String(row['フルフィルメント・チャンネル'] || row['fulfillment-channel'] || '').trim();
  const points = toIntPrice(row['ポイント'] ?? row['points']) ?? 0;
  return {
    run_id: runId,
    mall: 'amazon',
    shop_id: shopId,
    mall_item_key: sku,
    fulfillment: channel.toUpperCase().includes('AMAZON') || channel.toUpperCase().startsWith('AFN') ? 'FBA' : 'FBM',
    ne_code: null,                       // 対応付けは build 側で行う
    price_type: 'normal',
    price_incl_tax: price,
    mall_tax_rate: null,                 // Amazon は税率を返さない (商品マスタ側を使う)
    postage_included: null,              // FBM の送料収入は実績から推定する (§15-2)
    postage_revenue_incl_tax: null,
    points,
    listing_status: amazonListingStatus(row['ステータス'] || row['status']),
    fetch_status: price == null ? 'not_found' : 'ok',
    resolve_status: 'unresolved',        // build 側で解決する
    resolve_reason: null,
    valid_until: validUntil,
    source: 'merchant_listings_all_data',
    fetched_at: fetchedAt,
    _asin: asin,
  };
}

/** 楽天 items/search の 1 商品 → snapshot 行 (variant ごとに1行) */
export function rakutenItemToSnapshots(item, { runId, shopId, fetchedAt, validUntil }) {
  const manageNumber = String(item?.manageNumber || '').trim();
  if (!manageNumber) return [];
  const variants = item?.variants && typeof item.variants === 'object' ? item.variants : {};
  const hideItem = item?.hideItem === true;
  const out = [];
  for (const [variantKey, v] of Object.entries(variants)) {
    const price = toIntPrice(v?.standardPrice);            // 🚨 文字列で返る ("1080")
    const taxRate = v?.payment?.taxRate != null ? Number(v.payment.taxRate) : null;
    const postageIncluded = typeof v?.shipping?.postageIncluded === 'boolean' ? v.shipping.postageIncluded : null;
    const singleItemShipping = toIntPrice(v?.shipping?.singleItemShipping);
    const hidden = hideItem || v?.hidden === true;
    out.push({
      run_id: runId,
      mall: 'rakuten',
      shop_id: shopId,
      mall_item_key: `${manageNumber}/${variantKey}`,
      fulfillment: 'self',
      ne_code: null,
      price_type: 'normal',
      price_incl_tax: price,
      mall_tax_rate: Number.isFinite(taxRate) ? taxRate : null,
      postage_included: postageIncluded == null ? null : (postageIncluded ? 1 : 0),
      // 送料込みなら収入 0。別途なら singleItemShipping (取れなければ NULL = unknown)
      postage_revenue_incl_tax: postageIncluded === true ? 0 : singleItemShipping,
      points: 0,
      listing_status: hidden ? 'hidden' : 'active',
      fetch_status: price == null ? 'not_found' : 'ok',
      resolve_status: 'unresolved',
      resolve_reason: null,
      valid_until: validUntil,
      source: 'rms_items_search',
      fetched_at: fetchedAt,
      _merchantDefinedSkuId: v?.merchantDefinedSkuId || null,
    });
  }
  return out;
}

/**
 * 前回の完全集合と突き合わせて、消えた出品の数と partial 判定を返す (§5.2.1)。
 * 🚨 消えた率が大きいときはレポート破損の疑いなので partial にする。
 *    partial のときは呼び出し側が前回集合と UNION する
 */
export function evaluateEnumeration(previousKeys, currentKeys) {
  if (!previousKeys || previousKeys.size === 0) {
    return { status: 'ok', disappeared: 0, ratio: 0 };   // 初回は比較対象が無い
  }
  let disappeared = 0;
  for (const k of previousKeys) if (!currentKeys.has(k)) disappeared++;
  const ratio = disappeared / previousKeys.size;
  return {
    status: ratio > DISAPPEARED_RATIO_LIMIT ? 'partial' : 'ok',
    disappeared,
    ratio,
  };
}

// ────────────────────────────────────────────────────────────
// 実行 (I/O を伴う部分)
// ────────────────────────────────────────────────────────────

function insertSnapshots(db, rows) {
  const stmt = db.prepare(`
    INSERT OR REPLACE INTO mall_price_snapshot
      (run_id, mall, shop_id, mall_item_key, fulfillment, ne_code, price_type, price_incl_tax,
       mall_tax_rate, postage_included, postage_revenue_incl_tax, points, listing_status,
       fetch_status, resolve_status, resolve_reason, valid_until, source, fetched_at)
    VALUES
      (@run_id, @mall, @shop_id, @mall_item_key, @fulfillment, @ne_code, @price_type, @price_incl_tax,
       @mall_tax_rate, @postage_included, @postage_revenue_incl_tax, @points, @listing_status,
       @fetch_status, @resolve_status, @resolve_reason, @valid_until, @source, @fetched_at)
  `);
  const tx = db.transaction((list) => { for (const r of list) stmt.run(r); });
  tx(rows);
}

/** 直近で完全列挙できた run の出品キー集合 (§5.2.1 の「完全集合」) */
export function loadLastCompleteKeys(db, mall) {
  const run = db.prepare(`
    SELECT run_id FROM price_fetch_run
    WHERE mall = ? AND listing_enum_status = 'ok'
    ORDER BY started_at DESC LIMIT 1
  `).get(mall);
  if (!run) return { runId: null, keys: new Set() };
  const rows = db.prepare('SELECT shop_id, mall_item_key FROM mall_price_snapshot WHERE run_id = ?').all(run.run_id);
  return { runId: run.run_id, keys: new Set(rows.map(r => `${r.shop_id}${r.mall_item_key}`)) };
}

export async function fetchAmazonListings(db, deps = {}) {
  const runId = newRunId();
  const startedAt = nowIso();
  const shopId = AMAZON_SHOP_ID();
  db.prepare(`INSERT INTO price_fetch_run (run_id, mall, started_at, status, listing_enum_status)
              VALUES (?, 'amazon', ?, 'running', 'failed')`).run(runId, startedAt);

  try {
    const getReport = deps.getActiveListingsReport
      || (await import('../profit-calculator/sp-api.js')).getActiveListingsReport;
    const report = await getReport();
    const fetchedAt = nowIso();
    const validUntil = addDays(fetchedAt, PRICE_VALID_DAYS);
    const rows = (report.listings || [])
      .map(r => amazonRowToSnapshot(r, { runId, shopId, fetchedAt, validUntil }))
      .filter(r => r.mall_item_key);

    const currentKeys = new Set(rows.map(r => `${r.shop_id}${r.mall_item_key}`));
    const prev = loadLastCompleteKeys(db, 'amazon');
    const evalResult = evaluateEnumeration(prev.keys, currentKeys);

    insertSnapshots(db, rows);
    db.prepare(`UPDATE price_fetch_run SET finished_at = ?, status = ?, listing_enum_status = ?,
                expected_count = ?, fetched_count = ?, failed_count = ?, disappeared_count = ?
                WHERE run_id = ?`)
      .run(nowIso(), evalResult.status === 'ok' ? 'ok' : 'partial', evalResult.status,
        rows.length, rows.filter(r => r.fetch_status === 'ok').length,
        rows.filter(r => r.fetch_status !== 'ok').length, evalResult.disappeared, runId);
    return { runId, count: rows.length, ...evalResult };
  } catch (e) {
    db.prepare(`UPDATE price_fetch_run SET finished_at = ?, status = 'failed', listing_enum_status = 'failed',
                error_summary = ? WHERE run_id = ?`).run(nowIso(), String(e.message).slice(0, 500), runId);
    throw e;
  }
}

export async function fetchRakutenListings(db, deps = {}) {
  const runId = newRunId();
  const startedAt = nowIso();
  const shopId = RAKUTEN_SHOP_ID();
  db.prepare(`INSERT INTO price_fetch_run (run_id, mall, started_at, status, listing_enum_status)
              VALUES (?, 'rakuten', ?, 'running', 'failed')`).run(runId, startedAt);

  try {
    // 🚨 既存の /items/all-codes・/items/all-skus は 100頁 = 10,000件で打ち切る (§15-4)。
    //    ここでは /items/search を自分でページングし、打ち切りを partial として検出する
    const searchPage = deps.searchPage || defaultRakutenSearchPage;
    const fetchedAt = nowIso();
    const validUntil = addDays(fetchedAt, PRICE_VALID_DAYS);
    const rows = [];
    let cursorMark = '*';
    let pages = 0;
    const MAX_PAGES = deps.maxPages || 500;   // 50,000 商品。到達したら partial (打ち切りを隠さない)
    let truncated = false;
    for (;;) {
      if (pages >= MAX_PAGES) { truncated = true; break; }
      const data = await searchPage(cursorMark);
      pages++;
      const items = data?.results || data?.items || [];
      for (const r of items) {
        rows.push(...rakutenItemToSnapshots(r?.item || r, { runId, shopId, fetchedAt, validUntil }));
      }
      const next = data?.nextCursorMark;
      if (!next || next === cursorMark || items.length === 0) break;
      cursorMark = next;
    }

    const currentKeys = new Set(rows.map(r => `${r.shop_id}${r.mall_item_key}`));
    const prev = loadLastCompleteKeys(db, 'rakuten');
    const evalResult = evaluateEnumeration(prev.keys, currentKeys);
    const enumStatus = truncated ? 'partial' : evalResult.status;

    insertSnapshots(db, rows);
    db.prepare(`UPDATE price_fetch_run SET finished_at = ?, status = ?, listing_enum_status = ?,
                expected_count = ?, fetched_count = ?, failed_count = ?, disappeared_count = ?,
                error_summary = ? WHERE run_id = ?`)
      .run(nowIso(), enumStatus === 'ok' ? 'ok' : 'partial', enumStatus,
        rows.length, rows.filter(r => r.fetch_status === 'ok').length,
        rows.filter(r => r.fetch_status !== 'ok').length, evalResult.disappeared,
        truncated ? `ページ上限 ${MAX_PAGES} に到達 (打ち切りの疑い)` : null, runId);
    return { runId, count: rows.length, pages, truncated, ...evalResult, status: enumStatus };
  } catch (e) {
    db.prepare(`UPDATE price_fetch_run SET finished_at = ?, status = 'failed', listing_enum_status = 'failed',
                error_summary = ? WHERE run_id = ?`).run(nowIso(), String(e.message).slice(0, 500), runId);
    throw e;
  }
}

async function defaultRakutenSearchPage(cursorMark) {
  const { callRakutenProxy } = await import('./rms-client.js');
  return callRakutenProxy(`/service-api/rakuten-rms/items/search?cursorMark=${encodeURIComponent(cursorMark)}&hits=100`);
}

// ────────────────────────────────────────────────────────────
if (process.argv[1] && process.argv[1].endsWith('fetch-listings.js')) {
  const mall = process.argv.includes('--mall') ? process.argv[process.argv.indexOf('--mall') + 1] : null;
  const db = initExpectedProfitDB();
  const run = async () => {
    if (!mall || mall === 'amazon') console.log('[amazon]', JSON.stringify(await fetchAmazonListings(db)));
    if (!mall || mall === 'rakuten') console.log('[rakuten]', JSON.stringify(await fetchRakutenListings(db)));
  };
  run().then(() => db.close()).catch(e => { console.error(e); process.exit(1); });
}
