/**
 * FBA納品 箱詰め記録 — 商品画像 (Amazon カタログの MAIN 画像 URL)
 *
 * 取得元 = miniPC の /service-api/research/product/:asin (SP-API Catalog Items, mainImage)。
 * SP-API の鍵は miniPC にしか無い ([[feedback_render_vs_minipc_api_placement]]) ので Render からは
 * サービス API 経由。結果は fbx_product_images に FNSKU 単位でキャッシュ (ok は恒久、none/error は翌日再試行)。
 * 画像は補助なので best-effort: 取れなくても作業は続く。
 *
 * FNSKU → ASIN は 行の asin (Excel 添付後) → fba-replenishment の fba_sku_attrs (FNSKU / SKU) の順で引く
 */
import { listRowsNeedingImages, upsertProductImage } from './db.js';

const WAREHOUSE_URL = process.env.WAREHOUSE_URL || 'https://wh.bfaith-wh.uk';
const THROTTLE_MS = 5 * 60 * 1000;   // 同じ納品回の再取得は 5 分に 1 回まで (/api/state のたびに走らせない)
const INTERVAL_MS = 600;             // miniPC 側 rate limit (sp-api) に合わせて直列・間隔

const norm = (s) => String(s ?? '').trim().toUpperCase();

export function imagesConfigured() {
  return !!process.env.WAREHOUSE_SERVICE_TOKEN;
}

function serviceHeaders() {
  return {
    'CF-Access-Client-Id': process.env.CF_ACCESS_CLIENT_ID || '',
    'CF-Access-Client-Secret': process.env.CF_ACCESS_CLIENT_SECRET || '',
    Authorization: `Bearer ${process.env.WAREHOUSE_SERVICE_TOKEN || ''}`,
    Accept: 'application/json',
  };
}

/** FNSKU/SKU → ASIN の辞書源 (fba-replenishment の fba_sku_attrs)。テストで差し替え可 */
let attrsSource = async () => {
  const m = await import('../fba-replenishment/db.js');
  return m.getFbaSkuAttrs();
};
export function _setAttrsSource(fn) { attrsSource = fn; }

/**
 * 画像 URL の検証: https かつ Amazon の画像ホストだけを通す (Codex R13 #5: 任意ホストの画像を iPad に読ませない)。
 * 通らなければ null (= 画像なし扱い)
 */
export function sanitizeImageUrl(url) {
  if (!url || typeof url !== 'string') return null;
  let u;
  try { u = new URL(url); } catch { return null; }
  if (u.protocol !== 'https:') return null;
  const host = u.hostname.toLowerCase();
  const ok = host === 'm.media-amazon.com' || host.endsWith('.media-amazon.com') || host.endsWith('.ssl-images-amazon.com') || host.endsWith('.images-amazon.com');
  return ok ? u.toString() : null;
}

/** ASIN → MAIN 画像 URL (null = 画像なし)。テストで差し替え可 */
let fetcher = async (asin) => {
  const res = await fetch(`${WAREHOUSE_URL}/service-api/research/product/${encodeURIComponent(asin)}`, {
    headers: serviceHeaders(), redirect: 'manual', signal: AbortSignal.timeout(20_000),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const j = await res.json();
  if (!j.ok) throw new Error(j.message || j.error || 'ng');
  return j.mainImage || null;
};
export function _setFetcher(fn) { fetcher = fn; }

const inFlight = new Set();
const lastRunAt = new Map();

/**
 * 納品回の行のうち画像が無いものを取りに行く (直列・間隔付き)。fire-and-forget 前提で例外は投げない。
 * @returns { skipped } | { fetched, failed, none, total }
 */
export async function ensureRunImages(runId, { force = false } = {}) {
  const id = Number(runId);
  if (!force && !imagesConfigured()) return { skipped: 'not_configured' };
  if (inFlight.has(id)) return { skipped: 'in_flight' };
  if (!force && Date.now() - (lastRunAt.get(id) || 0) < THROTTLE_MS) return { skipped: 'throttled' };
  inFlight.add(id);
  lastRunAt.set(id, Date.now());
  try {
    const rows = listRowsNeedingImages(id);
    if (rows.length === 0) return { fetched: 0, failed: 0, none: 0, total: 0 };
    const attrs = new Map();
    try {
      for (const a of await attrsSource()) {
        if (a.fnsku) attrs.set('fnsku:' + norm(a.fnsku), a);
        if (a.amazon_sku) attrs.set('sku:' + norm(a.amazon_sku), a);
      }
    } catch (e) {
      console.warn('[fba-box] SKU属性 (ASIN) を読めません — 行の asin だけで画像を取ります:', e.message);
    }
    let fetched = 0, failed = 0, none = 0;
    for (const r of rows) {
      const asin = r.asin || attrs.get('fnsku:' + norm(r.fnsku))?.asin || (r.seller_sku ? attrs.get('sku:' + norm(r.seller_sku))?.asin : null) || null;
      if (!asin) { upsertProductImage({ fnsku: r.fnsku, asin: null, url: null, status: 'none' }); none++; continue; }
      try {
        const url = sanitizeImageUrl(await fetcher(asin));
        upsertProductImage({ fnsku: r.fnsku, asin, url, status: url ? 'ok' : 'none' });
        if (url) fetched++; else none++;
      } catch (e) {
        failed++;
        upsertProductImage({ fnsku: r.fnsku, asin, url: null, status: 'error' });
        console.warn(`[fba-box] 画像取得失敗 ${asin}: ${e.message}`);
      }
      await new Promise((resolve) => setTimeout(resolve, INTERVAL_MS));
    }
    return { fetched, failed, none, total: rows.length };
  } catch (e) {
    console.error('[fba-box] ensureRunImages', e);
    return { skipped: 'error', error: e.message };
  } finally {
    inFlight.delete(id);
  }
}

/** テスト用: スロットルと実行中フラグを消す */
export function _resetImageState() { inFlight.clear(); lastRunAt.clear(); }
