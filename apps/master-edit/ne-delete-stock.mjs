/**
 * ne-delete-stock.mjs — NE から消す CSV (0072・決定 Q3 a = 在庫が残っている商品は入れない) の「アプリが読む在庫」(lib/master-ne-delete.mjs の stockReader)
 *
 * Postgres の取引の前に、Render の写し (warehouse-mirror.db) から読む (DB は確かめられない = DB が記録に残す):
 *   ne_own / ne_allocated / ne_on_order / fba = 商品管理リストの公開の回 (sales-qty.mjs の readPmlStock = 毎朝の NE の取得の 在庫数・引当数・発注残数 (NE の古い値)・FBA の在庫)
 *   logizard = ロジザードの今の在庫 (extras.mjs の readWarehouseStock = 毎時の写し・商品ID ごとの合計・古い = 読めない)
 *   po_backorder = 発注アプリの注文残 (extras.mjs の readBackorders = 発注アプリと同じ数)
 * どれかを読めない (写しが無い・古い・商品管理リストにその商品が無い・値が整数でない) = その商品は { unreadable: 理由 } (= DB が断る)。
 * 🚨 Company DB の在庫の日次 (NE・ロジザード) は DB の関数 (ops.ne_del_check) が自分で読む = ここでは読まない
 */
import { readPmlStock } from './sales-qty.mjs';
import { readWarehouseStock, readBackorders, stockOf, backorderOf } from './extras.mjs';
import { normProductCode } from '../purchase-orders/db.js';

const isCount = (v) => Number.isInteger(v) && v >= 0 && v <= 2147483647;

/**
 * skus = [{ code, code_norm }] (単品だけ)。戻り値 = Map(code_norm → { ne_own, ne_allocated, ne_on_order, fba, logizard, po_backorder, sources } | { unreadable })
 * readers = 試験の差し替え ({ pml, warehouse, backorders })
 */
export async function readNeDeleteStock(skus, { now = Date.now(), readers = {} } = {}) {
  const pml = await (readers.pml || readPmlStock)(skus.map((s) => s.code), { now });
  const lz = await (readers.warehouse || readWarehouseStock)({ now });
  const bo = (readers.backorders || readBackorders)();
  const out = new Map();
  for (const s of skus) {
    const why = [];
    if (!pml || !pml.ok) why.push(`NE の在庫 (商品管理リスト): ${pml?.error || '読めない'}`);
    if (!lz || !lz.ok) why.push(`ロジザードの在庫: ${lz?.error || '読めない'}`);
    else if (lz.stale) why.push(`ロジザードの在庫の写しが古い (${lz.asOf})`);
    if (!bo || !bo.ok) why.push(`発注アプリの注文残: ${bo?.error || '読めない'}`);
    const row = pml && pml.ok ? pml.map.get(normProductCode(s.code)) : null;
    if (pml && pml.ok && !row) why.push('商品管理リストにこの商品が無い (NE の在庫を確かめられない)');
    if (row && ![row.total, row.fba, row.allocated, row.onOrder].every(isCount)) why.push('商品管理リストの在庫の値が読めない');
    if (why.length) { out.set(s.code_norm, { unreadable: why.join(' / ').slice(0, 200) }); continue; }
    const neOwn = row.total - row.fba;
    if (!isCount(neOwn)) { out.set(s.code_norm, { unreadable: '商品管理リストの NE の在庫 (総在庫数 − FBA在庫数) が読めない' }); continue; }
    out.set(s.code_norm, {
      ne_own: neOwn, ne_allocated: row.allocated, ne_on_order: row.onOrder, fba: row.fba,
      logizard: stockOf(lz, s.code_norm), po_backorder: backorderOf(bo, s.code),
      sources: { pml_run: pml.runId, ne_synced_at: pml.neSyncedAt, logizard_as_of: lz.asOf, read_at: new Date(now).toISOString() },
    });
  }
  return out;
}
