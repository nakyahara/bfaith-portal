/**
 * amazon-listing-common.js — 出品の状態 (fetch-amazon-listing-issues.js)・BSR (fetch-amazon-bsr.js)・出品とカートのアラート (alert-amazon-listing.js) の共通の部品
 *   (2026-10-22・AI の分析とアラート用に、まだ取っていない / 活かしていない Amazon のデータを取る)
 *
 *   - createRateGate: 呼び出しの間隔を守る (公式の usage plan の rate より少し遅く)。時計と sleep は差し替えられる (試験)
 *   - callWithRetry: 一過性の失敗を数回やり直す (429 QuotaExceeded はライブラリ amazon-sp-api が自分で待ってやり直す。ここは それ以外 + 念のため)
 *   - loadWatchSkus: 「直近 30 日に売れた or FBA の在庫がある」SKU (アラートの対象) を warehouse.db から集める
 *   - resolveSellerId: セラーID = 想定利益の DB (expected-profit.db) の覚え書き → env SP_API_SELLER_ID (apps/company-db/push/amazon-listings.mjs の mainShopId と同じ順)。
 *     🚨 値をハードコードしない (miniPC の env には SP_API_SELLER_ID が無い = 覚え書きが正。apps/expected-profit/refresh-fees.js の storedSellerId)
 *   - sendGChat: 通知 (env GCHAT_WEBHOOK・無ければ送らない)。🚨 送った後に process.exit() しない (Windows node の libuv assertion・#439)
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';
import { SETTING_AMAZON_SELLER_ID } from '../expected-profit/db.js';

export const JP_MARKETPLACE = 'A1VC38T7YXB528';
export const MARKETPLACE_ID = () => process.env.SP_API_MARKETPLACE_ID || JP_MARKETPLACE;
export const WATCH_DAYS = 30;             // 「売れた」の判定の日数
export const FBA_STOCK_MAX_AGE_DAYS = 3;  // FBA の在庫 (inv_daily_detail) はこの日数より古い日の値を使わない (在庫集計は daily-sync のこの段より後 = ふだんは前の日の値)

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
export const jstToday = (nowMs = Date.now()) => new Date(nowMs + 9 * 3600000).toISOString().slice(0, 10);
/** 業務日付: daily-sync が env で渡す (JST で確定済み) → 無ければ今の JST の日付 */
export function businessDate(env = process.env, nowMs = Date.now()) {
  const d = env.WAREHOUSE_BUSINESS_DATE;
  return (typeof d === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(d)) ? d : jstToday(nowMs);
}
export function addDays(dateStr, n) {
  const d = new Date(`${dateStr}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}
/** SKU の突き合わせの鍵 = 小文字 (amazon_sku_fees・inv_daily_detail は小文字で持つ。Amazon が返すのは登録どおりの大文字小文字) */
export const skuKey = (s) => String(s ?? '').trim().toLowerCase();

/**
 * 呼び出しの間隔を守る門。wait() は「前の呼び出しの開始から intervalMs 経つまで」待ってから戻る。
 * 公式の rate (例: searchListingsItems 5 回/秒・searchCatalogItems 2 回/秒) を超えないよう、少し長めの間隔を渡す。
 */
export function createRateGate({ intervalMs, now = () => Date.now(), sleepFn = sleep } = {}) {
  if (!(intervalMs > 0)) throw new Error(`intervalMs が正でない: ${intervalMs}`);
  let last = null;
  let waits = 0;
  return {
    async wait() {
      if (last !== null) {
        const left = last + intervalMs - now();
        if (left > 0) { waits++; await sleepFn(left); }
      }
      last = now();
    },
    get waits() { return waits; },
  };
}

/** 一過性の失敗を数回やり直す。1 回ごとに門を通る (やり直しも rate に数える) */
export async function callWithRetry(fn, { gate, label = '', maxRetries = 3, sleepFn = sleep, log = console.warn } = {}) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    if (gate) await gate.wait();
    try {
      return await fn();
    } catch (e) {
      if (attempt === maxRetries) throw e;
      const msg = String(e?.message || e);
      const isThrottle = /QuotaExceeded|429|throttl/i.test(msg) || e?.code === 'QuotaExceeded';
      const wait = isThrottle ? 2000 * attempt : 1500 * attempt;
      log(`  ⚠ ${label} attempt ${attempt} failed (${msg.slice(0, 120)}), retry in ${wait}ms`);
      await sleepFn(wait);
    }
  }
  throw new Error('unreachable');
}

const hasTable = (db, name) => !!db.prepare(`SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name = ?`).get(name);

/**
 * アラートの対象 = 直近 WATCH_DAYS 日に売れた SKU (raw_sp_orders・取消を除く) か、FBA の在庫がある SKU (inv_daily_detail の fba_warehouse・qty > 0)。
 * 戻り値 Map(sku_key → { raw: 注文に出てきた登録どおりの SKU | null, units, sales_jpy, fba_qty, fba_date })
 *   表が無い (新しい DB・試験) ときは空として扱う
 */
export function loadWatchSkus(db, { today, days = WATCH_DAYS, stockMaxAgeDays = FBA_STOCK_MAX_AGE_DAYS } = {}) {
  const out = new Map();
  const get = (k) => { if (!out.has(k)) out.set(k, { raw: null, units: 0, sales_jpy: 0, fba_qty: 0, fba_date: null }); return out.get(k); };
  if (hasTable(db, 'raw_sp_orders')) {
    const cutoff = addDays(today, -days);
    const rows = db.prepare(`
      SELECT seller_sku, SUM(quantity) AS units, SUM(COALESCE(item_price, 0)) AS sales_jpy
      FROM raw_sp_orders
      WHERE order_status NOT IN ('Cancelled') AND purchase_date >= ? AND quantity > 0
        AND seller_sku IS NOT NULL AND TRIM(seller_sku) != ''
      GROUP BY seller_sku
    `).all(cutoff);
    for (const r of rows) {
      const k = skuKey(r.seller_sku);
      const w = get(k);
      w.raw = w.raw || String(r.seller_sku).trim();
      w.units += r.units || 0;
      w.sales_jpy += r.sales_jpy || 0;
    }
  }
  if (hasTable(db, 'inv_daily_detail')) {
    const latest = db.prepare(`
      SELECT MAX(business_date) AS d FROM inv_daily_detail
      WHERE market = 'jp' AND source_system = 'fba' AND category = 'fba_warehouse' AND business_date <= ? AND business_date >= ?
    `).get(today, addDays(today, -stockMaxAgeDays))?.d;
    if (latest) {
      // セットは構成品ごとに行が分かれる (qty = 個数 × 構成数) = SKU ごとに 1 行目の qty があれば「在庫あり」で足りる。数は最大の行を目安に出す
      const rows = db.prepare(`
        SELECT source_item_code AS sku, MAX(qty) AS qty FROM inv_daily_detail
        WHERE business_date = ? AND market = 'jp' AND source_system = 'fba' AND category = 'fba_warehouse' AND qty > 0
        GROUP BY source_item_code
      `).all(latest);
      for (const r of rows) { const w = get(skuKey(r.sku)); w.fba_qty = r.qty; w.fba_date = latest; }
    }
  }
  return out;
}

/**
 * セラーID = expected-profit.db の覚え書き (expected_profit_setting の amazon_seller_id) → env SP_API_SELLER_ID。
 * 戻り値 { sellerId, from: 'setting' | 'env' } / { error }
 */
export function resolveSellerId({ dataDir = process.env.DATA_DIR || path.join(process.cwd(), 'data'), env = process.env,
  openDb = (file) => new Database(file, { readonly: true, fileMustExist: true }) } = {}) {
  let setting = null;
  const file = dataDir ? path.join(dataDir, 'expected-profit.db') : null;
  if (file && fs.existsSync(file)) {
    let db = null;
    try {
      db = openDb(file);
      if (hasTable(db, 'expected_profit_setting')) setting = db.prepare('SELECT value FROM expected_profit_setting WHERE key = ?').get(SETTING_AMAZON_SELLER_ID)?.value ?? null;
    } catch (e) {
      return { error: `想定利益の DB (expected-profit.db) のセラーID の覚え書きを読めない: ${String(e.message).slice(0, 120)}` };
    } finally { try { if (db) db.close(); } catch { /* */ } }
  }
  const seller = (setting && String(setting).trim()) || (env.SP_API_SELLER_ID ? String(env.SP_API_SELLER_ID).trim() : '') || null;
  if (!seller) return { error: 'セラーID が無い (expected-profit.db の覚え書き amazon_seller_id も env SP_API_SELLER_ID も無い) = 推測で選ばない' };
  if (!/^[A-Z0-9]{5,20}$/.test(seller)) return { error: `セラーID の形が違う (${seller.slice(0, 30)})` };
  return { sellerId: seller, from: setting ? 'setting' : 'env' };
}

/** GChat 通知 (best effort)。webhook が無ければ送らない = false */
export async function sendGChat(text, { webhook = process.env.GCHAT_WEBHOOK, fetchImpl = fetch } = {}) {
  if (!webhook) return false;
  try {
    const res = await fetchImpl(webhook, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ text }) });
    return !!res.ok;
  } catch (e) {
    console.error(`[GChat] 通知失敗: ${e.message}`);
    return false;
  }
}

/** 直接起動のときだけ main を走らせる (試験から import しても取得が走らない。fetch-amazon-ads.js と同じ realpath の判定) */
export function isMainModule(importMetaUrl, argv1 = process.argv[1]) {
  const realPath = (p) => { try { return fs.realpathSync.native(p); } catch { return path.resolve(p); } };
  const foldCase = (p) => (process.platform === 'win32' ? p.toLowerCase() : p);
  return !!argv1 && foldCase(realPath(argv1)) === foldCase(realPath(fileURLToPath(importMetaUrl)));
}

/** SP-API のクライアント (fetch-amazon-prices.js と同じ作り方)。import するだけでは作らない */
let spClient = null;
export async function getSpClient() {
  if (!spClient) {
    const { default: SellingPartner } = await import('amazon-sp-api');
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
