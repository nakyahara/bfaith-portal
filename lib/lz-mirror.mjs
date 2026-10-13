/**
 * lz-mirror.mjs — ロジザードの写し (Render の warehouse-mirror.db・読むだけ) = 毎晩のロジザードの商品の一覧 (f_inbound_check_product_flags) と
 *   バーコードマスタ (f_inbound_check_barcode_master)。CompanyDB構想 23 v4。
 *   新商品の登録・商品の画面で「ロジザードでほかの商品が使っているバーコード」を断るのに使う (Company DB には写しが無い = アプリが見る)
 * 🆕 #1719 Codex R1 High 2 (中原さん「推定で現場を止めない」): 写しが読めない・空・古い = 保存は通す。そのかわり値に「写しと照らせていない (写しの時刻)」の印を残す
 *   (lzCopyCheck の state = ops.sku_logizard_barcode_rules.lz_copy_state)。照らせなかった値は、ロジザード登録の CSV (2 本目) を出すときに新しい写しで照らし直す
 */
import { normLzValue, LZ_COPY_STATE_WORDS } from './logizard-barcode.mjs';

export { LZ_COPY_STATE_WORDS };

/** 写しが古い = この時間より前 (毎晩取り込む = 36 時間・CompanyDB構想 23 v4 §5') */
export const LZ_COPY_FRESH_HOURS = 36;

const tableExists = (sq, name) => !!sq.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?").get(name);
/**
 * ロジザードの写し (Render の SQLite・読むだけ)。getDb = () => SQLite (better-sqlite3)
 * @returns {{ ok, reason, keys: Set<小文字の商品ID>, ids: Map<小文字, Set<商品ID (書いたとおり)>>, barcodes: Map<バーコード, 商品ID>, list_as_of, bc_as_of, rows,
 *   bc: { ok, reason, rows } }}
 *   ok = 商品の一覧の写しが読めた・空でない (2 本目の「まだ無い」の判定に使う)・bc = バーコードマスタの写し (値の重なりに使う・商品の一覧とは別に)
 */
export function readLzMirror(getDb) {
  const empty = (reason, bcReason = reason) => ({ ok: false, reason, keys: new Set(), ids: new Map(), barcodes: new Map(), list_as_of: null, bc_as_of: null, rows: 0,
    bc: { ok: false, reason: bcReason, rows: 0 } });
  let sq;
  try { sq = getDb(); } catch (e) { return empty(`写しを開けない (${e && e.message})`); }
  try {
    const keys = new Set(), ids = new Map();
    const add = (id) => { const s = String(id); const l = s.toLowerCase(); keys.add(l); if (!ids.has(l)) ids.set(l, new Set()); ids.get(l).add(s); };
    // バーコードマスタ (値の重なり)
    const barcodes = new Map();
    let bcAt = null, bcRows = 0, bcReason = null;
    if (tableExists(sq, 'f_inbound_check_barcode_master')) {
      for (const r of sq.prepare('SELECT barcode, code_key, product_id, updated_at FROM f_inbound_check_barcode_master').all()) {
        add(r.product_id || r.code_key);
        barcodes.set(normLzValue(r.barcode), String(r.product_id || r.code_key));
        if (!bcAt || r.updated_at > bcAt) bcAt = r.updated_at;
        bcRows++;
      }
      if (!bcRows) bcReason = 'ロジザードのバーコードマスタの写しが空です';
    } else bcReason = 'ロジザードのバーコードマスタの写しがありません';
    const bc = { ok: !bcReason, reason: bcReason, rows: bcRows, empty: !bcRows && tableExists(sq, 'f_inbound_check_barcode_master') };
    // 商品の一覧 (まだ無いの判定)
    if (!tableExists(sq, 'f_inbound_check_product_flags')) return { ...empty('ロジザードの商品の一覧の写しがありません'), keys, ids, barcodes, bc_as_of: bcAt, bc };
    const pf = sq.prepare("SELECT code_key, updated_at FROM f_inbound_check_product_flags WHERE source = 'logizard'").all();
    let listAt = null;
    for (const r of pf) { keys.add(String(r.code_key).toLowerCase()); if (!listAt || r.updated_at > listAt) listAt = r.updated_at; }
    if (!pf.length) return { ...empty('ロジザードの商品の一覧の写しが空です'), keys, ids, barcodes, bc_as_of: bcAt, bc };
    return { ok: true, reason: null, keys, ids, barcodes, list_as_of: listAt, bc_as_of: bcAt, rows: pf.length, bc };
  } catch (e) {
    return empty(`写しを読めない (${e && e.message})`);
  }
}

/**
 * 値をバーコードマスタの写しと照らせるか = { state: checked | unavailable | empty | stale, as_of (写しの時刻・読めない = null), words }。
 *   読めない (表が無い・開けない) = unavailable・行が 0 = empty・一番新しい行が 36 時間より前 (時刻が無いも) = stale。
 *   🚨 照らせない = 保存は通す (印を残す)・写しにほかの商品の同じ値がある = 古い写しでも断る (呼び手)
 */
export function lzCopyCheck(mirror, nowMs) {
  const bc = mirror && mirror.bc ? mirror.bc : { ok: false, rows: 0 };
  const asOf = mirror && mirror.bc_as_of ? mirror.bc_as_of : null;
  const state = !bc.ok ? (bc.empty ? 'empty' : 'unavailable')
    : !asOf || !(Date.parse(asOf) > nowMs - LZ_COPY_FRESH_HOURS * 3600e3) ? 'stale' : 'checked';
  const iso = asOf && !Number.isNaN(Date.parse(asOf)) ? new Date(Date.parse(asOf)).toISOString() : null;
  return { state, as_of: iso, words: LZ_COPY_STATE_WORDS[state] };
}
