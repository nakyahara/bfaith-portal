/**
 * 伝票の商品構成を packing-dispatch から引く。
 *
 * なぜ packing-dispatch か:
 *   印字 (送り状シール) は朝と午後の出荷ごとに行われるが、miniPC の warehouse.db (NE 受注) は
 *   07:00 と 22:00 の同期なので、印字時点では当日受注の約 2 割 (6/1〜9/4 実測 21.7%) がまだ無い。
 *   packing-dispatch は NE → ロジザードの CSV を出した時点 (= 送り状発行より前) で
 *   伝票ごとの商品構成を `pd_shipment_tracking.product_items_json` に残しているので、
 *   印字時点で必ず揃っている。Render 上の同じ DATA_DIR にあるので直接読める。
 *
 * ここは **読むだけ**。warehouse-mirror.db には絶対に書かない (packing-dispatch の持ち物)。
 */
import Database from 'better-sqlite3';
import path from 'path';
import fs from 'fs';

const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), 'data');
const MIRROR_DB = process.env.POSTAGE_MIRROR_DB || path.join(DATA_DIR, 'warehouse-mirror.db');

export const COMPOSITION_SOURCE = 'packing-dispatch';
/** packing-dispatch の配送方法コード。定形 (110円) も NE 上は「定形外郵便」なので teikeigai に含まれる */
export const TEIKEIGAI_METHOD_CODE = 'teikeigai';

function open() {
  return new Database(MIRROR_DB, { readonly: true, fileMustExist: true });
}

/** packing-dispatch の表が読める状態か (ファイルがあっても表が無い環境 = 未稼働では false)。 */
export function mirrorAvailable() {
  if (!fs.existsSync(MIRROR_DB)) return false;
  try {
    const db = open();
    try {
      return !!db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='pd_shipment_tracking'").get();
    } finally { db.close(); }
  } catch {
    return false;
  }
}

export function mirrorPath() { return MIRROR_DB; }

export function normalizeSlipNo(v) {
  return String(v ?? '').normalize('NFKC').trim();
}
export function normalizeSku(v) {
  return String(v ?? '').normalize('NFKC').trim().toLowerCase();
}

/**
 * product_items_json → 判定エンジンの明細。
 * 壊れた JSON は空配列 (判定側で no_lines → 不明に落ちる。黙って確定しない)。
 * 数量は変換しない (小数・0 の扱いはエンジンが決める)。
 */
export function parseItems(json) {
  let items;
  try { items = JSON.parse(json || '[]'); } catch { return []; }
  if (!Array.isArray(items)) return [];
  return items
    .filter((it) => it && typeof it === 'object')
    .map((it) => ({
      sku_code: normalizeSku(it.product_code),
      qty: it.qty,
      product_name: typeof it.product_name === 'string' ? it.product_name : null,
    }))
    .filter((l) => l.sku_code);
}

function chunk(arr, n) {
  const out = [];
  for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
  return out;
}

/**
 * 伝票番号 → 構成。見つからない伝票は Map に入らない (呼び出し側で「構成なし」にする)。
 * @param {string[]} slipNos 正規化済みの伝票番号
 * @returns {Map<string, {slip_no, method_code, exported_at, shop_name, lines}>}
 */
export function readCompositions(slipNos) {
  const out = new Map();
  const list = [...new Set(slipNos.map(normalizeSlipNo).filter(Boolean))];
  if (!list.length || !mirrorAvailable()) return out;
  const db = open();
  try {
    for (const part of chunk(list, 200)) {
      const rows = db.prepare(`
        SELECT ne_uketsuke_no, shipping_method_code, exported_at, shop_name, product_items_json
          FROM pd_shipment_tracking
         WHERE ne_uketsuke_no IN (${part.map(() => '?').join(',')})
      `).all(...part);
      for (const r of rows) {
        out.set(normalizeSlipNo(r.ne_uketsuke_no), {
          slip_no: normalizeSlipNo(r.ne_uketsuke_no),
          method_code: r.shipping_method_code,
          exported_at: r.exported_at,
          shop_name: r.shop_name,
          lines: parseItems(r.product_items_json),
        });
      }
    }
  } finally { db.close(); }
  return out;
}

/** JST の日付 'YYYY-MM-DD' → その日 0:00 JST を UTC ISO で。exported_at (UTC ISO) と比べるため */
function jstDayStartUtc(ymd) {
  return new Date(`${ymd}T00:00:00+09:00`).toISOString();
}
function nextDay(ymd) {
  const d = new Date(`${ymd}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + 1);
  return d.toISOString().slice(0, 10);
}
function jstDateOf(utcIso) {
  const t = Date.parse(utcIso);
  if (!Number.isFinite(t)) return null;
  return new Date(t + 9 * 3600 * 1000).toISOString().slice(0, 10);
}

/**
 * カバー率用: 期間中に packing-dispatch から出力された定形外の伝票と構成。
 * 出荷日は「CSV 出力日 (JST)」で近似する (NE の出荷確定日とは数時間ずれるが、料金表の適用日としては十分)。
 * 再出力された伝票は最後の出力だけが残る (pd_shipment_tracking は伝票番号が主キー)。
 */
export function loadMirrorShipments({ since, until } = {}) {
  if (!mirrorAvailable()) return null;
  const db = open();
  try {
    const params = [TEIKEIGAI_METHOD_CODE];
    let where = 'shipping_method_code = ? AND exported_at IS NOT NULL';
    if (since) { where += ' AND exported_at >= ?'; params.push(jstDayStartUtc(since)); }
    if (until) { where += ' AND exported_at < ?'; params.push(jstDayStartUtc(nextDay(until))); }
    const rows = db.prepare(`
      SELECT ne_uketsuke_no, exported_at, product_items_json
        FROM pd_shipment_tracking WHERE ${where}
    `).all(...params);
    return rows.map((r) => ({
      slip_no: normalizeSlipNo(r.ne_uketsuke_no),
      ship_date: jstDateOf(r.exported_at),
      lines: parseItems(r.product_items_json),
    })).filter((s) => s.ship_date);
  } finally { db.close(); }
}

/** 商品名の補完 (packing-dispatch の出力履歴から。直近の出力を優先)。 */
export function lookupProductNameFromMirror(skuCode) {
  const code = normalizeSku(skuCode);
  if (!code || !mirrorAvailable()) return null;
  const db = open();
  try {
    // 壊れた JSON の行が 1 つでもあると json_each がクエリごと失敗するので、有効な行だけ展開する
    const r = db.prepare(`
      SELECT json_extract(j.value, '$.product_name') AS name
        FROM pd_shipment_tracking t,
             json_each(CASE WHEN json_valid(t.product_items_json) THEN t.product_items_json ELSE '[]' END) j
       WHERE lower(trim(json_extract(j.value, '$.product_code'))) = ?
         AND json_extract(j.value, '$.product_name') <> ''
       ORDER BY t.exported_at DESC LIMIT 1
    `).get(code);
    return r?.name || null;
  } catch {
    return null;   // JSON が壊れている行があっても補完は「あれば便利」なだけ
  } finally { db.close(); }
}
