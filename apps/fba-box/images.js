/**
 * FBA納品 箱詰め記録 — Amazon カタログの取得 (商品画像 + 参考単重)
 *
 * 取得元 = miniPC の /service-api/research/product/:asin (SP-API Catalog Items。応答のキーは image)。
 * SP-API の鍵は miniPC にしか無い ([[feedback_render_vs_minipc_api_placement]]) ので Render からは
 * サービス API 経由。結果は fbx_product_images / fbx_weight_refs に FNSKU 単位でキャッシュ
 * (ok は恒久、none/error は翌日再試行)。どちらも補助なので best-effort: 取れなくても作業は続く。
 *
 * ⭐PR3: 画像と参考単重は同じ応答に入っている (dimensions.weight) ので、呼び出しは 1 商品 1 回のまま。
 * SP-API のレート制限を増やさずに重量補助を足せる = miniPC 側の改修も不要
 *
 * FNSKU → ASIN は 行の asin (Excel 添付後) → fba-replenishment の fba_sku_attrs (FNSKU / SKU) の順で引く
 */
import { listRowsNeedingCatalog, upsertProductImage, upsertWeightRef, listRunWeights } from './db.js';

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
 * ⚠ 実物の応答は `{ ok: true, result: { asin, itemName, image, ... } }` — 画像は **result.image**。
 * 2026-09-03 に `j.mainImage` → `j.image` と直しても出ず、実機の応答を miniPC で直接叩いて確定した。
 * 別アプリのサービス API は「包み方 (result) とキー名」の両方を実物で確かめる
 */
export function pickImageUrl(payload) {
  const p = payload?.result ?? payload;
  return p?.image || p?.mainImage || p?.imageUrl || null;
}

/**
 * 応答から 1個あたりの梱包重量 (g) を取り出す。
 * miniPC は SP-API カタログの dimensions[0].package.weight (ポンド) を kg 小数2桁の文字列にして返す。
 * ⭐実物で確認済み (2026-09-06, miniPC で直接 getCatalogItem):
 *   {"unit":"pounds","value":0.0440924524} → "0.02" (元は attributes.item_package_weight の 0.02kg)。
 *   単位はポンドで、既存の kg 換算は正しい。ただし Amazon 側のデータ自体が 0.01kg 刻み = 10g 単位なので
 *   軽い商品では粗い → あくまで参考値で、実測 (何個で何g) が入ればそちらが勝つ
 * 重量が無い商品は "-" が来る
 */
export function pickPackageWeightG(payload) {
  const p = payload?.result ?? payload;
  const raw = p?.dimensions?.weight;
  if (raw == null || raw === '-' || raw === '') return { g: null, raw: null };
  const kg = Number(raw);
  if (!Number.isFinite(kg) || kg <= 0) return { g: null, raw: String(raw) };
  // 単位を取り違えた応答を推定に流し込まないための保険 (1個 100kg は箱詰めの対象外)
  if (kg > 100) return { g: null, raw: String(raw), error: `1個あたり ${kg}kg は大きすぎます (単位が想定と違う可能性)` };
  return { g: Math.round(kg * 1000 * 10) / 10, raw: String(raw) };
}

/** ASIN → miniPC の応答 (そのまま)。画像と単重を 1 回の呼び出しで取る。テストで差し替え可 */
let fetcher = async (asin) => {
  const res = await fetch(`${WAREHOUSE_URL}/service-api/research/product/${encodeURIComponent(asin)}`, {
    headers: serviceHeaders(), redirect: 'manual', signal: AbortSignal.timeout(20_000),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const j = await res.json();
  if (!j.ok) throw new Error(j.message || j.error || 'ng');
  return j;
};
export function _setFetcher(fn) { fetcher = fn; }

/** 実行中の取得 (run_id → Promise)。「今すぐ取り直す」は終わるのを待ってから走る */
const inFlight = new Map();
const lastRunAt = new Map();
// 裏の取得を待つ上限。待ち + 自分の取得が HTTP のタイムアウト (Render のプロキシ ~100秒) に収まる長さにする
const WAIT_FOR_RUNNING_MS = 45_000;

/**
 * 納品回の行のうち画像または参考単重が無いものを取りに行く (直列・間隔付き)。
 * fire-and-forget 前提で例外は投げない。
 * @returns { skipped } | { fetched, failed, none, weighed, noWeight, total }
 */
export async function ensureRunCatalog(runId, { force = false } = {}) {
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
    const rows = listRowsNeedingCatalog(id, opts);
    if (rows.length === 0) return remember({ fetched: 0, failed: 0, none: 0, weighed: 0, noWeight: 0, total: 0, remaining: 0 });
    const index = await buildAsinIndex();
    if (index.error) console.warn('[fba-box] SKU属性 (ASIN) を読めません — 行の asin だけでカタログを取ります:', index.error);
    let fetched = 0, failed = 0, none = 0, noAsin = 0, weighed = 0, noWeight = 0;
    const errors = [];
    for (const r of rows) {
      const { asin } = resolveAsin(r, index.map);
      // 取れているキャッシュは壊さない (Codex PR3 R2 #1): 画像と単重は片方だけ欠けていることがあり、
      // その再取得の巻き添えで、もう片方の ok を none/error に落としてはいけない
      const keepImage = r.image_status === 'ok';
      const keepWeight = r.weight_status === 'ok';
      if (!asin) {
        const why = 'ASIN が分かりません (Excel を添付するか SKU マスタを確認)';
        if (!keepImage) { upsertProductImage({ fnsku: r.fnsku, asin: null, url: null, status: 'none', error: why }); none++; }
        if (!keepWeight) { upsertWeightRef({ fnsku: r.fnsku, asin: null, weightG: null, status: 'none', error: why }); noWeight++; }
        noAsin++;
        continue;
      }
      try {
        const payload = await fetcher(asin);
        const url = sanitizeImageUrl(pickImageUrl(payload));
        upsertProductImage({ fnsku: r.fnsku, asin, url, status: url ? 'ok' : 'none', error: url ? null : 'Amazon に画像がありません' });
        if (url) fetched++; else none++;
        // 参考単重 (同じ応答から。画像が無くても重量はあることがある = 別々に記録する)
        const w = pickPackageWeightG(payload);
        upsertWeightRef({ fnsku: r.fnsku, asin, weightG: w.g, raw: w.raw, status: w.g ? 'ok' : 'none',
          error: w.g ? null : (w.error || 'Amazon に梱包重量の登録がありません (現場で「何個で何g」を量ってください)') });
        if (w.g) weighed++; else noWeight++;
      } catch (e) {
        failed++;
        // 通信失敗で、既に取れている画像・単重まで消さない (Codex PR3 R2 #1)
        if (!keepImage) upsertProductImage({ fnsku: r.fnsku, asin, url: null, status: 'error', error: e.message });
        if (!keepWeight) { upsertWeightRef({ fnsku: r.fnsku, asin, weightG: null, status: 'error', error: e.message }); noWeight++; }
        if (errors.length < 5) errors.push(`${asin}: ${e.message}`);
        console.warn(`[fba-box] カタログ取得失敗 ${asin}: ${e.message}`);
      }
      await new Promise((resolve) => setTimeout(resolve, INTERVAL_MS));
    }
    // 1 回の上限 (listRowsNeedingCatalog の limit) を超えて未取得の分だけを残件として数える。
    // 今回 none/error にしたものは通常の再試行待ち (24h) に入るので数えない (Codex R18 #2)
    const remaining = listRowsNeedingCatalog(id).length;
    return remember({ fetched, failed, none, noAsin, weighed, noWeight, total: rows.length, remaining, attrsCount: index.count, attrsError: index.error, errors });
  } catch (e) {
    console.error('[fba-box] ensureRunCatalog', e);
    return remember({ skipped: 'error', error: e.message });
  } finally {
    inFlight.delete(id);
    done();   // 待っている「今すぐ取り直す」を解放する
  }
}

/**
 * 画像・単重が出ない原因の切り分け (管理画面から)。取得はせず、いまの状態だけを返す
 */
export async function diagnoseRunCatalog(runId, rows) {
  const index = await buildAsinIndex();
  const weights = new Map(listRunWeights(runId).map((w) => [norm(w.fnsku), w]));
  const byFnsku = new Map();
  for (const r of rows) {
    const key = norm(r.fnsku);
    if (!key || byFnsku.has(key)) continue;
    const { asin, from } = resolveAsin(r, index.map);
    const w = weights.get(key);
    byFnsku.set(key, { fnsku: r.fnsku, sku: r.seller_sku || null, rowAsin: r.asin || null, asin, asinFrom: from,
      imageUrl: r.image_url || null,
      unitG: w?.unit_g ?? null, weightSource: w?.source ?? null, measCount: w?.meas_count ?? 0,
      refG: w?.ref_g ?? null, refStatus: w?.ref_status ?? null, refError: w?.ref_error ?? null });
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
      withWeight: items.filter((x) => x.unitG > 0).length,
      measured: items.filter((x) => x.weightSource === 'measured').length,
    },
    items,
    lastRun: getLastRunResult(runId),
  };
}

/** テスト用: スロットルと実行中フラグを消す */
export function _resetImageState() { inFlight.clear(); lastRunAt.clear(); }
