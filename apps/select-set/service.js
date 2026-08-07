/**
 * service.js — 選べるセットの展開サービス。
 *
 * 情報源の組み合わせ:
 *   1. RMS Item API 2.0 の customizationOptions … 選択肢定義の第一情報源。オンデマンド取得+TTLキャッシュ
 *      (1日10〜40件しか使わないので定期同期ジョブは作らない = 入口を増やさない)
 *   2. ss_mappings (手動マッピング) … RMSでは解けない分。RMSより優先
 *   3. warehouse.db の raw_ne_products … 商品コードの実在確認と、おまけの在庫判定
 *
 * ⚠ 在庫は日次同期のため最大24時間古い。おまけは在庫が薄い商品なので、
 *   自動で決め切らず候補と在庫を全部返して人が変えられるようにしている。
 */
import { rakutenRequest } from '../warehouse/rakuten-client.js';
import { getDB as getWarehouseDB } from '../warehouse/db.js';
import { buildCatalog, expandOp, toPasteBlocks } from './expand.js';
import {
  getRmsCache, listMappings, listOmake, listSets, putRmsCache,
} from './db.js';

export class SsError extends Error {
  constructor(message, { status = 400, code = 'BAD_REQUEST' } = {}) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

const RMS_TTL_MS = Math.max(1, Number(process.env.SELECT_SET_RMS_TTL_MIN) || 360) * 60 * 1000;
const PRODUCT_CACHE_MS = 5 * 60 * 1000;

let productCache = { at: 0, codes: null, stock: null };

function loadProducts() {
  const now = Date.now();
  if (productCache.codes && now - productCache.at < PRODUCT_CACHE_MS) return productCache;
  let db;
  try {
    db = getWarehouseDB();
  } catch {
    throw new SsError('warehouse.db が使えません (商品マスタ・在庫が引けない環境です)', {
      status: 503, code: 'WAREHOUSE_UNAVAILABLE',
    });
  }
  const codes = new Set();
  const stock = new Map();
  for (const r of db.prepare('SELECT 商品コード AS c, 商品名 AS n, 在庫数 AS z, 引当数 AS h, 取扱区分 AS k FROM raw_ne_products').all()) {
    const code = String(r.c || '').trim();
    if (!code) continue;
    codes.add(code.toLowerCase());
    stock.set(code.toLowerCase(), {
      name: String(r.n || ''),
      available: (Number(r.z) || 0) - (Number(r.h) || 0),
      status: String(r.k || ''),
    });
  }
  productCache = { at: now, codes, stock };
  return productCache;
}

export function stockOf(code) {
  const { stock } = loadProducts();
  return stock.get(String(code || '').toLowerCase()) || null;
}

/** RMSの選択肢定義を取る。キャッシュが新しければAPIを叩かない */
async function fetchRmsOptions(setCode, { force = false } = {}) {
  const cached = getRmsCache(setCode);
  if (!force && cached?.fetched_at) {
    const age = Date.now() - Date.parse(cached.fetched_at);
    if (Number.isFinite(age) && age < RMS_TTL_MS && cached.payload) {
      try { return { options: JSON.parse(cached.payload), fetchedAt: cached.fetched_at, cached: true }; } catch {}
    }
  }
  try {
    const r = await rakutenRequest({ path: `/es/2.0/items/manage-numbers/${encodeURIComponent(setCode)}` });
    const options = (r?.body ?? r?.data ?? r)?.customizationOptions || [];
    putRmsCache(setCode, options, null);
    return { options, fetchedAt: new Date().toISOString(), cached: false };
  } catch (e) {
    const msg = String(e?.message || e).slice(0, 300);
    putRmsCache(setCode, cached?.payload ? JSON.parse(cached.payload) : null, msg);
    // RMSが落ちていても手動マッピングだけで動かす (止めない)
    let options = [];
    if (cached?.payload) { try { options = JSON.parse(cached.payload); } catch {} }
    return { options, fetchedAt: cached?.fetched_at || null, cached: true, error: msg };
  }
}

/** セットの選択肢定義 (RMS + 手動) を組み立てる */
export async function getCatalog(setCode, { force = false } = {}) {
  const code = String(setCode || '').trim();
  if (!code) throw new SsError('セット商品コードが必要です');
  const { codes } = loadProducts();
  const rms = await fetchRmsOptions(code, { force });
  const manualRows = listMappings(code).map((m) => ({ option: m.option_text, code: m.product_code }));
  const catalog = buildCatalog({
    setCode: code,
    rmsOptions: rms.options,
    manualRows,
    knownProductCodes: codes,
  });
  return { catalog, rms, manualCount: manualRows.length };
}

export function omakePriority() {
  return listOmake().map((r) => r.product_code);
}

/** 選べるセットとして登録されているか (拡張が「展開」ボタンを出すかの判定に使う) */
export function isKnownSet(setCode) {
  const c = String(setCode || '').trim().toLowerCase();
  return listSets().some((s) => String(s.set_code).toLowerCase() === c);
}

/**
 * 展開の本体。拡張から呼ばれる。
 * @returns 明細行・おまけ候補・警告・NEに貼る形
 */
export async function expandForOrder({ setCode, op, quantity = 1, force = false }) {
  if (!op || !String(op).trim()) throw new SsError('商品OPが空です');
  if (!isKnownSet(setCode)) {
    throw new SsError(`「${setCode}」は選べるセットとして登録されていません`, { status: 404, code: 'UNKNOWN_SET' });
  }
  const { catalog, rms, manualCount } = await getCatalog(setCode, { force });
  const result = expandOp({
    catalog,
    op,
    quantity,
    omakePriority: omakePriority(),
    stockOf,
  });

  // おまけ候補は在庫と商品名を添えて返す (人が選び直せるように)
  const omake = result.omake
    ? {
        ...result.omake,
        candidates: result.omake.candidates.map((c) => ({ ...c, ...(stockOf(c.code) || {}) })),
      }
    : null;

  const lines = result.lines.map((l) => ({ ...l, name: stockOf(l.code)?.name || '' }));

  return {
    setCode,
    ok: result.ok,
    lines,
    omake,
    warnings: result.warnings,
    notices: result.notices,
    paste: result.ok ? toPasteBlocks({ lines: result.lines, omake: result.omake }) : null,
    source: {
      rmsOptionCount: rms.options.length,
      rmsFetchedAt: rms.fetchedAt,
      rmsCached: rms.cached,
      rmsError: rms.error || null,
      manualCount,
      stockAsOf: '日次同期 (最大24時間前)',
    },
  };
}

/** 管理画面用: セットの選択肢定義を人が読める形で返す */
export async function inspectSet(setCode, { force = false } = {}) {
  const { catalog, rms, manualCount } = await getCatalog(setCode, { force });
  const { codes } = loadProducts();
  return {
    setCode,
    labels: catalog.labels,
    skuCount: catalog.skus.length,
    optionCount: catalog.options.size,
    manualCount,
    rmsOptionCount: rms.options.length,
    rmsFetchedAt: rms.fetchedAt,
    rmsError: rms.error || null,
    conflicts: catalog.conflicts,
    // RMSに載っているのに商品コードを特定できない選択肢 = 手動マッピングを足すべきもの
    unresolved: [...new Set(catalog.unresolved)],
    skus: catalog.skus.map((c) => ({ code: c, known: codes.has(c.toLowerCase()), ...(stockOf(c) || {}) })),
  };
}
