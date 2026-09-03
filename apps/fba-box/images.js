/**
 * FBA納品 箱詰め記録 — 商品画像 (Amazon カタログの MAIN 画像 URL)
 *
 * 取得元 = miniPC の /service-api/research/product/:asin (SP-API Catalog Items。応答のキーは image)。
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

/** 直近の実行結果 (管理画面の診断用。run_id → {at, ...result}) */
const lastRun = new Map();
export function getLastRunResult(runId) { return lastRun.get(Number(runId)) || null; }

/**
 * FNSKU/SKU → ASIN の索引を作る。attrs が読めなければ理由を返す (画像が出ない原因の切り分け用)
 * @returns { map: Map<'fnsku:X'|'sku:X', {asin}>, count, error }
 */
export async function buildAsinIndex() {
  const map = new Map();
  try {
    const rows = await attrsSource();
    for (const a of rows || []) {
      if (a.asin) {
        if (a.fnsku) map.set('fnsku:' + norm(a.fnsku), a);
        if (a.amazon_sku) map.set('sku:' + norm(a.amazon_sku), a);
      }
    }
    return { map, count: (rows || []).length, error: null };
  } catch (e) {
    return { map, count: 0, error: e.message };
  }
}

/** 1 行の ASIN を決める: 行の asin (Excel 添付後) → FNSKU → SKU */
export function resolveAsin(row, index) {
  if (row.asin) return { asin: row.asin, from: 'row' };
  const byFnsku = index.get('fnsku:' + norm(row.fnsku));
  if (byFnsku?.asin) return { asin: byFnsku.asin, from: 'fnsku' };
  const bySku = row.seller_sku ? index.get('sku:' + norm(row.seller_sku)) : null;
  if (bySku?.asin) return { asin: bySku.asin, from: 'sku' };
  return { asin: null, from: null };
}

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

/**
 * miniPC の /service-api/research/product/:asin の応答から MAIN 画像 URL を取り出す。
 * ⚠ 応答のキーは `image` (apps/profit-calculator/sp-api.js の getProduct)。2026-09-03 に `mainImage` を
 * 読んでいて全商品が「画像なし」になった — 別アプリの応答キーは実物で確かめる
 */
export function pickImageUrl(payload) {
  return payload?.image || payload?.mainImage || payload?.imageUrl || null;
}

/** ASIN → MAIN 画像 URL (null = 画像なし)。テストで差し替え可 */
let fetcher = async (asin) => {
  const res = await fetch(`${WAREHOUSE_URL}/service-api/research/product/${encodeURIComponent(asin)}`, {
    headers: serviceHeaders(), redirect: 'manual', signal: AbortSignal.timeout(20_000),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const j = await res.json();
  if (!j.ok) throw new Error(j.message || j.error || 'ng');
  return pickImageUrl(j);
};
export function _setFetcher(fn) { fetcher = fn; }

/** 実行中の取得 (run_id → Promise)。「今すぐ取り直す」は終わるのを待ってから走る */
const inFlight = new Map();
const lastRunAt = new Map();
// 裏の取得を待つ上限。待ち + 自分の取得が HTTP のタイムアウト (Render のプロキシ ~100秒) に収まる長さにする
const WAIT_FOR_RUNNING_MS = 45_000;

/**
 * 納品回の行のうち画像が無いものを取りに行く (直列・間隔付き)。fire-and-forget 前提で例外は投げない。
 * @returns { skipped } | { fetched, failed, none, total }
 */
export async function ensureRunImages(runId, { force = false } = {}) {
  const id = Number(runId);
  const remember = (r) => { lastRun.set(id, { at: new Date().toISOString(), ...r }); return r; };
  // 設定チェックは force でも省かない (空トークンで miniPC を叩かない — Codex R17 #5)。force が無視するのは
  // キャッシュの再試行待ちとスロットルだけ
  if (!imagesConfigured()) return remember({ skipped: 'not_configured', message: 'Render の WAREHOUSE_SERVICE_TOKEN が未設定です (miniPC の SP-API を呼べません)' });
  if (inFlight.has(id)) {
    // 裏の取得が走っている: 自動 (force なし) はそのまま任せる。「今すぐ取り直す」は終わるのを待ってから実行する
    if (!force) return { skipped: 'in_flight' };
    const deadline = Date.now() + WAIT_FOR_RUNNING_MS;
    while (inFlight.has(id) && Date.now() < deadline) {
      try { await inFlight.get(id); } catch { /* 走っていた実行の失敗はここでは無視 */ }
    }
    if (inFlight.has(id)) return { skipped: 'in_flight', message: '画像の取得がまだ続いています。少し待ってからもう一度押してください' };
  }
  if (!force && Date.now() - (lastRunAt.get(id) || 0) < THROTTLE_MS) return { skipped: 'throttled' };
  lastRunAt.set(id, Date.now());
  let done;
  inFlight.set(id, new Promise((resolve) => { done = resolve; }));
  try {
    const opts = force ? { retryAfterMs: 0 } : {};
    const rows = listRowsNeedingImages(id, opts);
    if (rows.length === 0) return remember({ fetched: 0, failed: 0, none: 0, total: 0, remaining: 0 });
    const index = await buildAsinIndex();
    if (index.error) console.warn('[fba-box] SKU属性 (ASIN) を読めません — 行の asin だけで画像を取ります:', index.error);
    let fetched = 0, failed = 0, none = 0, noAsin = 0;
    const errors = [];
    for (const r of rows) {
      const { asin } = resolveAsin(r, index.map);
      if (!asin) {
        upsertProductImage({ fnsku: r.fnsku, asin: null, url: null, status: 'none', error: 'ASIN が分かりません (Excel を添付するか SKU マスタを確認)' });
        none++; noAsin++;
        continue;
      }
      try {
        const raw = await fetcher(asin);
        const url = sanitizeImageUrl(raw);
        upsertProductImage({ fnsku: r.fnsku, asin, url, status: url ? 'ok' : 'none', error: url ? null : (raw ? `画像URLが対象外: ${String(raw).slice(0, 80)}` : 'Amazon に画像がありません') });
        if (url) fetched++; else none++;
      } catch (e) {
        failed++;
        upsertProductImage({ fnsku: r.fnsku, asin, url: null, status: 'error', error: e.message });
        if (errors.length < 5) errors.push(`${asin}: ${e.message}`);
        console.warn(`[fba-box] 画像取得失敗 ${asin}: ${e.message}`);
      }
      await new Promise((resolve) => setTimeout(resolve, INTERVAL_MS));
    }
    // 1 回の上限 (listRowsNeedingImages の limit) を超えて未取得の分だけを残件として数える。
    // 今回 none/error にしたものは通常の再試行待ち (24h) に入るので数えない (Codex R18 #2)
    const remaining = listRowsNeedingImages(id).length;
    return remember({ fetched, failed, none, noAsin, total: rows.length, remaining, attrsCount: index.count, attrsError: index.error, errors });
  } catch (e) {
    console.error('[fba-box] ensureRunImages', e);
    return remember({ skipped: 'error', error: e.message });
  } finally {
    inFlight.delete(id);
    done();   // 待っている「今すぐ取り直す」を解放する
  }
}

/**
 * 画像が出ない原因の切り分け (管理画面から)。取得はせず、いまの状態だけを返す
 */
export async function diagnoseRunImages(runId, rows) {
  const index = await buildAsinIndex();
  const byFnsku = new Map();
  for (const r of rows) {
    const key = norm(r.fnsku);
    if (!key || byFnsku.has(key)) continue;
    const { asin, from } = resolveAsin(r, index.map);
    byFnsku.set(key, { fnsku: r.fnsku, sku: r.seller_sku || null, rowAsin: r.asin || null, asin, asinFrom: from, imageUrl: r.image_url || null });
  }
  const items = [...byFnsku.values()];
  return {
    configured: imagesConfigured(),
    warehouseUrl: WAREHOUSE_URL,
    attrs: { count: index.count, error: index.error },
    counts: {
      total: items.length,
      withImage: items.filter((x) => x.imageUrl).length,
      noAsin: items.filter((x) => !x.asin).length,
    },
    items,
    lastRun: getLastRunResult(runId),
  };
}

/** テスト用: スロットルと実行中フラグを消す */
export function _resetImageState() { inFlight.clear(); lastRunAt.clear(); }
