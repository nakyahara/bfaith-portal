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
import { ensureMasterFresh, masterFreshnessProblem, masterStatus } from './master-sync.js';
import {
  getDB, getRmsCache, listMappings, listOmake, listSets, putRmsCache,
} from './db.js';

export class SsError extends Error {
  constructor(message, { status = 400, code = 'BAD_REQUEST' } = {}) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

/**
 * 🚨 同じコードが Render と miniPC の両方で動くが、持っているものが違う。
 *   Render : マスタ (セット・マッピング・おまけ) の正。warehouse.db と楽天認証は無い
 *   miniPC : warehouse.db (商品マスタ・在庫) と楽天認証がある。マスタはRenderから取得
 * Render 側で「動作確認」ができてしまうと、商品名が出ない・おまけが全部在庫0扱い・
 * マスタ照合が効かない、が黙って起きて正しく展開できたように見える (2026-08-08 に実際に踏んだ)。
 * そのため商品マスタが引けない環境では、それを要する機能だけを止めて案内する。
 */
const WRONG_HOST_MESSAGE =
  '商品マスタ (warehouse.db) が無い環境です。動作確認・在庫表示・RMSの選択肢確認は'
  + ' miniPC側の https://wh.bfaith-wh.uk/apps/select-set/ で行ってください'
  + ' (セット・マッピング・おまけの登録はこの画面のままで大丈夫です)。';

const RMS_TTL_MS = Math.max(1, Number(process.env.SELECT_SET_RMS_TTL_MIN) || 360) * 60 * 1000;
/**
 * RMSキャッシュをフォールバックとして使える上限。
 * 🚨 取得失敗時に無期限で古い定義を使うと、楽天側で選択肢の割当先を変えた後に
 *   古い対応で展開し続ける (Codexレビュー4巡目 / 2026-08-08)。
 */
const RMS_MAX_AGE_MS = Math.max(1, Number(process.env.SELECT_SET_RMS_MAX_AGE_H) || 168) * 3600 * 1000;
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
  let rows = [];
  try {
    rows = db.prepare('SELECT 商品コード AS c, 商品名 AS n, 在庫数 AS z, 引当数 AS h, 取扱区分 AS k FROM raw_ne_products').all();
  } catch {
    throw new SsError(WRONG_HOST_MESSAGE, { status: 503, code: 'WAREHOUSE_UNAVAILABLE' });
  }
  for (const r of rows) {
    const code = String(r.c || '').trim();
    if (!code) continue;
    codes.add(code.toLowerCase());
    stock.set(code.toLowerCase(), {
      name: String(r.n || ''),
      available: (Number(r.z) || 0) - (Number(r.h) || 0),
      status: String(r.k || ''),
    });
  }
  // 🚨 商品マスタが空 = warehouse.db を持たない環境 (Render) で開いている。
  //   ここで止めないと「商品名が出ない」「おまけが全部在庫0扱い」「マスタ照合が効かない」が
  //   黙って起きて、正しく展開できたように見えてしまう (2026-08-08 実際に起きた)。
  if (codes.size === 0) {
    throw new SsError(WRONG_HOST_MESSAGE, { status: 503, code: 'WAREHOUSE_EMPTY' });
  }
  productCache = { at: now, codes, stock };
  return productCache;
}

/**
 * この環境で何ができるかを返す。画面はこれを見て出し分ける。
 *   master.mode = 'source'   … Render。マスタの登録・編集はここ (中原さんが普段開く画面)
 *              = 'replica'   … miniPC。マスタはRenderから取得。編集は読み取り専用
 *   ready       … 商品マスタ (warehouse.db) が引けるか。
 *                 動作確認・在庫表示・RMS確認・商品コードの実在チェックはこれが必要
 */
export function environment() {
  const master = masterStatus();
  const primaryUrl = process.env.SELECT_SET_PRIMARY_URL || 'https://wh.bfaith-wh.uk/apps/select-set/';
  const base = {
    master,
    primaryUrl,
    rakutenCreds: !!process.env.RAKUTEN_SERVICE_SECRET && !!process.env.RAKUTEN_LICENSE_KEY,
    extTokenSet: !!process.env.SELECT_SET_EXT_TOKEN,
  };
  try {
    const { codes } = loadProducts();
    return { ...base, ready: true, productCount: codes.size };
  } catch (e) {
    return { ...base, ready: false, reason: e instanceof SsError ? e.code : 'UNKNOWN', message: String(e.message || e) };
  }
}

/**
 * 商品マスタが引ける環境なら在庫情報を返し、引けない環境なら null を返す (投げない)。
 * Render 側でマスタ登録するときに「在庫が見られないから登録できない」とならないようにするため。
 */
export function stockOfSoft(code) {
  try {
    return stockOf(code);
  } catch {
    return null;
  }
}

/** 商品マスタが引ける環境かどうか (実在チェックをするかの判定に使う) */
export function canValidateProducts() {
  try {
    loadProducts();
    return true;
  } catch {
    return false;
  }
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
    // RMSが落ちていても、キャッシュが古すぎなければ前回の定義で動かす
    let options = [];
    if (cached?.payload) { try { options = JSON.parse(cached.payload); } catch {} }
    const age = cached?.fetched_at ? Date.now() - Date.parse(cached.fetched_at) : Infinity;
    const tooStale = !options.length || !Number.isFinite(age) || age > RMS_MAX_AGE_MS;
    return {
      options: tooStale ? [] : options,
      fetchedAt: cached?.fetched_at || null,
      cached: true,
      error: msg,
      tooStale,
    };
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
  // マスタは Render 側が正。必要なら取りに行く (失敗しても前回の内容で続ける)
  await ensureMasterFresh();
  // ただし古すぎるマスタでは展開しない (誤った対応のまま入れ続けるのを防ぐ)
  const stale = masterFreshnessProblem();
  if (stale) throw new SsError(stale, { status: 503, code: 'MASTER_STALE' });
  if (!isKnownSet(setCode)) {
    throw new SsError(`「${setCode}」は選べるセットとして登録されていません`, { status: 404, code: 'UNKNOWN_SET' });
  }
  const { catalog, rms, manualCount } = await getCatalog(setCode, { force });
  if (rms.tooStale) {
    throw new SsError(
      '楽天の選択肢定義を取得できず、手元のキャッシュも古すぎるため展開できません'
      + (rms.error ? ` (${rms.error})` : ''),
      { status: 503, code: 'RMS_STALE' },
    );
  }
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

/**
 * 診断: サーバープロセスが実際に何を見ているかを返す。
 * 「手元のスクリプトでは解決できるのに画面では解決できない」ときの切り分け用。
 * 秘密の値は返さず、有無と件数だけにする。
 */
export function diagnose() {
  const out = {
    cwd: process.cwd(),
    dataDir: process.env.DATA_DIR || '(未設定 → cwd/data)',
    rakutenCreds: !!process.env.RAKUTEN_SERVICE_SECRET && !!process.env.RAKUTEN_LICENSE_KEY,
    extTokenSet: !!process.env.SELECT_SET_EXT_TOKEN,
    rmsTtlMinutes: RMS_TTL_MS / 60000,
  };
  try {
    const db = getWarehouseDB();
    out.warehouse = {
      file: db.name,
      products: db.prepare('SELECT COUNT(*) AS n FROM raw_ne_products').get().n,
      sampleStock: (() => {
        const r = db.prepare('SELECT 商品名 AS n, 在庫数 AS z, 引当数 AS h FROM raw_ne_products WHERE 商品コード = ?').get('ae-plumeria10');
        return r ? { name: r.n, available: (Number(r.z) || 0) - (Number(r.h) || 0) } : null;
      })(),
    };
  } catch (e) {
    out.warehouse = { error: String(e.message || e) };
  }
  try {
    const p = loadProducts();
    out.productCache = { codes: p.codes.size, stock: p.stock.size, loadedAtMsAgo: Date.now() - p.at };
  } catch (e) {
    out.productCache = { error: String(e.message || e) };
  }
  try {
    const d = getDB();
    out.selectSetDb = {
      file: d.name,
      sets: d.prepare('SELECT COUNT(*) AS n FROM ss_sets').get().n,
      mappings: d.prepare('SELECT COUNT(*) AS n FROM ss_mappings').get().n,
      omake: d.prepare('SELECT COUNT(*) AS n FROM ss_omake').get().n,
      rmsCache: d.prepare('SELECT set_code, fetched_at, error, LENGTH(payload) AS len FROM ss_rms_cache').all(),
    };
  } catch (e) {
    out.selectSetDb = { error: String(e.message || e) };
  }
  return out;
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
