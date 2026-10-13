/**
 * lz-mirror.mjs — ロジザードの写し (Render の warehouse-mirror.db・読むだけ) = 毎晩のロジザードの商品の一覧 (f_inbound_check_product_flags) と
 *   バーコードマスタ (f_inbound_check_barcode_master)。CompanyDB構想 23 v4。
 *   新商品の登録・商品の画面で「ロジザードでほかの商品が使っているバーコード」を断るのに使う (Company DB には写しが無い = アプリが見る)
 */
import { normLzValue } from './logizard-barcode.mjs';

const tableExists = (sq, name) => !!sq.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?").get(name);
/**
 * ロジザードの写し (Render の SQLite・読むだけ)。getDb = () => SQLite (better-sqlite3)
 * @returns {{ ok, reason, keys: Set<小文字の商品ID>, ids: Map<小文字, Set<商品ID (書いたとおり)>>, barcodes: Map<バーコード, 商品ID>, list_as_of, bc_as_of, rows }}
 *   keys = 商品の一覧 (f_inbound_check_product_flags の code_key = 小文字にした商品ID・大文字小文字は分からない) と バーコードマスタの商品ID (小文字)
 *   ids = バーコードマスタの商品ID (書いたとおり = 大文字小文字の違いが分かる)
 */
export function readLzMirror(getDb) {
  const empty = (reason) => ({ ok: false, reason, keys: new Set(), ids: new Map(), barcodes: new Map(), list_as_of: null, bc_as_of: null, rows: 0 });
  let sq;
  try { sq = getDb(); } catch (e) { return empty(`写しを開けない (${e && e.message})`); }
  try {
    if (!tableExists(sq, 'f_inbound_check_product_flags')) return empty('ロジザードの商品の一覧の写しがありません');
    const keys = new Set(), ids = new Map();
    const add = (id) => { const s = String(id); const l = s.toLowerCase(); keys.add(l); if (!ids.has(l)) ids.set(l, new Set()); ids.get(l).add(s); };
    const pf = sq.prepare("SELECT code_key, updated_at FROM f_inbound_check_product_flags WHERE source = 'logizard'").all();
    let listAt = null;
    for (const r of pf) { keys.add(String(r.code_key).toLowerCase()); if (!listAt || r.updated_at > listAt) listAt = r.updated_at; }
    const barcodes = new Map();
    let bcAt = null;
    if (tableExists(sq, 'f_inbound_check_barcode_master')) {
      for (const r of sq.prepare('SELECT barcode, code_key, product_id, updated_at FROM f_inbound_check_barcode_master').all()) {
        add(r.product_id || r.code_key);
        barcodes.set(normLzValue(r.barcode), String(r.product_id || r.code_key));
        if (!bcAt || r.updated_at > bcAt) bcAt = r.updated_at;
      }
    }
    if (!pf.length) return { ...empty('ロジザードの商品の一覧の写しが空です'), barcodes, bc_as_of: bcAt };
    return { ok: true, reason: null, keys, ids, barcodes, list_as_of: listAt, bc_as_of: bcAt, rows: pf.length };
  } catch (e) {
    return empty(`写しを読めない (${e && e.message})`);
  }
}

