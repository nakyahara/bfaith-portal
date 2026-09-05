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
/**
 * packing-dispatch に登録されている「定形外ではない」配送方法コード (apps/packing-dispatch/db.js SHIPPING_METHODS)。
 * これらだけを「対象外」にする。ここに無いコード・空・NULL は「不明」に落とす
 * (表記ゆれやデータ破損で定形外の伝票が黙って空印字にならないように)
 */
export const KNOWN_OTHER_METHOD_CODES = new Set([
  'nekopos', 'yupacketpuff', 'takkyu50', 'hatsubarai', 'letterpack', 'aes',
  'yupack', 'clickpost', 'fukuyama', 'seino',
]);

function open() {
  return new Database(MIRROR_DB, { readonly: true, fileMustExist: true });
}

// 読むのに要る列 (readCompositions / loadMirrorShipments が SELECT する列と同じ)。
// 列が足りない旧スキーマ・別物のファイルは「読めない」扱いにする
const PROBE_SQL = 'SELECT ne_uketsuke_no, shipping_method_code, shop_name, product_items_json, exported_at FROM pd_shipment_tracking LIMIT 0';

/** packing-dispatch の表が読める状態か (ファイルがあっても表・列が無い環境 = 未稼働では false)。 */
export function mirrorAvailable() {
  if (!fs.existsSync(MIRROR_DB)) return false;
  try {
    const db = open();
    try { db.prepare(PROBE_SQL).all(); return true; } finally { db.close(); }
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
export function normalizeMethodCode(v) {
  return String(v ?? '').normalize('NFKC').trim().toLowerCase();
}

/**
 * product_items_json → 判定エンジンの明細。
 *
 * 1 明細でも読めない (配列でない / 要素がオブジェクトでない / 商品コードが空 / 数量が JSON の数値でない) なら
 * **構成全体を壊れている扱い** にする (`broken: true`・lines は空)。
 * 読めた明細だけで判定すると、2 商品のうち 1 つが欠けたまま安い区分で確定してしまう (Codex R1 P1)。
 * 数量は `Number()` で寄せない: `true` が 1 個として通る。packing-dispatch は数値で保存している。
 * @returns {{lines: Array<{sku_code, qty, product_name}>, broken: boolean}}
 */
export function parseItems(json) {
  let items;
  try { items = JSON.parse(json ?? '[]'); } catch { return { lines: [], broken: true }; }
  if (!Array.isArray(items)) return { lines: [], broken: true };
  const lines = [];
  for (const it of items) {
    if (!it || typeof it !== 'object' || Array.isArray(it)) return { lines: [], broken: true };
    const sku = normalizeSku(it.product_code);
    if (!sku) return { lines: [], broken: true };
    if (typeof it.qty !== 'number' || !Number.isFinite(it.qty)) return { lines: [], broken: true };
    lines.push({ sku_code: sku, qty: it.qty, product_name: typeof it.product_name === 'string' ? it.product_name : null });
  }
  return { lines, broken: false };
}

function chunk(arr, n) {
  const out = [];
  for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
  return out;
}

/**
 * 伝票番号 → 構成。見つからない伝票は Map に入らない (呼び出し側で「構成なし」にする)。
 * DB が読めなければ例外 (呼び出し側が「全件不明」にするか決める)。
 * @param {string[]} slipNos 正規化済みの伝票番号
 * @returns {Map<string, {slip_no, method_code, exported_at, shop_name, lines, broken}>}
 */
export function readCompositions(slipNos) {
  const out = new Map();
  const list = [...new Set(slipNos.map(normalizeSlipNo).filter(Boolean))];
  if (!list.length) return out;
  const db = open();
  try {
    for (const part of chunk(list, 200)) {
      const rows = db.prepare(`
        SELECT ne_uketsuke_no, shipping_method_code, exported_at, shop_name, product_items_json
          FROM pd_shipment_tracking
         WHERE ne_uketsuke_no IN (${part.map(() => '?').join(',')})
      `).all(...part);
      for (const r of rows) {
        const slip = normalizeSlipNo(r.ne_uketsuke_no);
        out.set(slip, {
          slip_no: slip,
          method_code: normalizeMethodCode(r.shipping_method_code),
          exported_at: r.exported_at,
          shop_name: r.shop_name,
          ...parseItems(r.product_items_json),
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
    let where = "lower(trim(COALESCE(shipping_method_code, ''))) = ? AND exported_at IS NOT NULL";
    if (since) { where += ' AND exported_at >= ?'; params.push(jstDayStartUtc(since)); }
    if (until) { where += ' AND exported_at < ?'; params.push(jstDayStartUtc(nextDay(until))); }
    const rows = db.prepare(`
      SELECT ne_uketsuke_no, exported_at, product_items_json
        FROM pd_shipment_tracking WHERE ${where}
    `).all(...params);
    return rows.map((r) => ({
      slip_no: normalizeSlipNo(r.ne_uketsuke_no),
      ship_date: jstDateOf(r.exported_at),
      ...parseItems(r.product_items_json),
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
