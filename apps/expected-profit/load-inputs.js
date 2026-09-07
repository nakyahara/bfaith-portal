/**
 * 商品別 想定利益 — warehouse.db から計算入力を読む (読み取り専用)
 *
 * 🚨 warehouse.db は **読むだけ**。書き込みは expected-profit.db にしかしない (§15-10)。
 *    warehouse.db は 11GB あり、product-idea-scout が 14:00〜翌09:00 常駐で読み書きしている。
 *    busy_timeout を短くし、待たされたら中断する (既存同期を待たせない)。
 */
import Database from 'better-sqlite3';
import path from 'path';
import { addDays } from './util.js';

// 期限 (§15-8)
const COST_VALID_DAYS = 30;
const SHIPPING_MASTER_VALID_DAYS = 365;

const WAREHOUSE_BUSY_TIMEOUT_MS = 5000;

export function openWarehouseReadOnly(file) {
  const dbFile = file || process.env.WAREHOUSE_DB
    || path.join(process.env.DATA_DIR || path.join(process.cwd(), 'data'), 'warehouse.db');
  const db = new Database(dbFile, { readonly: true, fileMustExist: true });
  db.pragma(`busy_timeout = ${WAREHOUSE_BUSY_TIMEOUT_MS}`);
  return db;
}

/** 商品マスタ (ne_code → product)。原価・税率・送料区分を持つ */
export function loadProducts(wdb) {
  const rows = wdb.prepare(`
    SELECT 商品コード, 商品名, 原価, 原価ソース, 原価状態, 消費税率, 税区分,
           送料コード, 配送方法, 売上分類, 取扱区分
    FROM m_products
  `).all();
  const map = new Map();
  for (const r of rows) map.set(String(r.商品コード).toLowerCase(), r);
  return map;
}

/** 配送区分マスタ (shipping_code → rate) */
export function loadShippingRates(wdb) {
  const rows = wdb.prepare(`
    SELECT shipping_code, 大分類区分, 小分類区分名称, 送料, 出荷作業料,
           想定梱包資材費, 想定人件費, 配送関係費合計
    FROM shipping_rates
  `).all();
  const map = new Map();
  for (const r of rows) map.set(String(r.shipping_code), r);
  return map;
}

/**
 * SKU → NE商品コードの対応表。
 * Amazon = v_sku_resolved (1 SKU = N components)、楽天 = f_rakuten_sku_map。
 * 🚨 1 SKU が複数 NE を指す場合は配列のまま返す (呼び出し側が ambiguous と判定する)
 */
export function loadSkuMap(wdb, mall) {
  const map = new Map();
  if (mall === 'amazon') {
    const rows = wdb.prepare('SELECT seller_sku, ne_code FROM v_sku_resolved').all();
    for (const r of rows) {
      const k = String(r.seller_sku).toLowerCase();
      if (!map.has(k)) map.set(k, []);
      map.get(k).push({ ne_code: String(r.ne_code).toLowerCase() });
    }
  } else if (mall === 'rakuten') {
    let rows = [];
    try {
      rows = wdb.prepare('SELECT rakuten_code, ne_code FROM f_rakuten_sku_map').all();
    } catch { /* まだ作られていない環境では空 */ }
    for (const r of rows) {
      const k = String(r.rakuten_code).toLowerCase();
      if (!map.has(k)) map.set(k, []);
      map.get(k).push({ ne_code: String(r.ne_code).toLowerCase() });
    }
  }
  return map;
}

/**
 * マスタの鮮度 (§15-8)。
 * 🚨 原価は `m_products.updated_at` ではなく **NE の同期時刻**を基準にする。
 *    m_products.updated_at は再構築した時刻なので、NE 側が止まっていても新しく見える。
 */
export function loadMasterFreshness(wdb) {
  const neSynced = pick(wdb, 'SELECT MAX(synced_at) AS v FROM raw_ne_products');
  const shippingSynced = pick(wdb, 'SELECT MAX(synced_at) AS v FROM shipping_rates');
  const productShippingSynced = pick(wdb, 'SELECT MAX(synced_at) AS v FROM product_shipping');
  // 配送は2つのマスタのうち古い方を基準にする (どちらかが止まれば失効させる)
  // 🚨 片方でも同期時刻が取れなければ「鮮度を確認できない」= 期限なし (不適格) にする。
  //    filter(Boolean) で片方を落とすと、確認できないマスタを ok として採用してしまう
  const shippingBase = (shippingSynced && productShippingSynced)
    ? [shippingSynced, productShippingSynced].sort()[0]
    : null;
  return {
    costSyncedAt: neSynced,
    costValidUntil: neSynced ? addDays(toIso(neSynced), COST_VALID_DAYS) : null,
    shippingMasterSyncedAt: shippingBase,
    shippingMasterValidUntil: shippingBase ? addDays(toIso(shippingBase), SHIPPING_MASTER_VALID_DAYS) : null,
  };
}

function pick(wdb, sql) {
  try { return wdb.prepare(sql).get()?.v ?? null; } catch { return null; }
}

/** 'YYYY-MM-DD HH:MM:SS' も ISO も受ける */
function toIso(v) {
  const s = String(v).trim();
  if (/^\d{4}-\d{2}-\d{2} /.test(s)) return s.replace(' ', 'T') + 'Z';
  return s;
}

export { toIso as _toIso };
