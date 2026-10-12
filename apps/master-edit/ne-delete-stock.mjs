/**
 * ne-delete-stock.mjs — NE から消す CSV (0072・決定 Q3 a = 在庫が残っている商品は入れない) の「アプリが読む在庫」(lib/master-ne-delete.mjs の stockReader)
 *
 * Postgres の取引の前に、Render の写し (warehouse-mirror.db) から読む (DB は確かめられない = DB が記録に残す):
 *   ne_own / ne_allocated / ne_on_order / fba = 商品管理リストの公開の回 (sales-qty.mjs の readPmlStock = 毎朝の NE の取得の 在庫数・引当数・発注残数 (NE の古い値)・FBA の在庫)
 *   logizard / logizard_allocated = ロジザードの今の在庫と引当を別々に (毎時の写し mirror_logizard_stock を、この削除のためだけに読む = readLogizardForDelete)
 *     🆕 #1690 Codex R1 High 1: 一覧・商品の画面の在庫 (extras.mjs の readWarehouseStock / stockOf = 在庫数だけ・読めない値は 0) は変えない。
 *     ここは 商品ID ごとに 在庫数 と 引当数 を足し、1 行でも 0 以上の整数でない値があればその商品は「読めない」(0 にしない)・写しが古い = 全部「読めない」
 *   po_backorder = 発注アプリの注文残 (extras.mjs の readBackorders = 発注アプリと同じ数)
 * どれかを読めない (写しが無い・古い・商品管理リストにその商品が無い・値が整数でない) = その商品は { unreadable: 理由 } (= DB が断る)。
 * 🚨 Company DB の在庫の日次 (NE・ロジザード) は DB の関数 (ops.ne_del_check) が自分で読む = ここでは読まない
 */
import path from 'node:path';
import { readPmlStock } from './sales-qty.mjs';
import { readBackorders, backorderOf, stockStale } from './extras.mjs';
import { readMirrorLogizardStock } from '../company-db/inventory/logizard.mjs';
import { normProductCode } from '../purchase-orders/db.js';
import { normSku } from '../../lib/sku-norm.js';

const isCount = (v) => Number.isInteger(v) && v >= 0 && v <= 2147483647;
/** 写しの値 (数か数字だけの文字) → 0 以上の整数 / 読めない = null (空・小数・負・文字は 0 にしない) */
function countOf(v) {
  if (typeof v === 'number') return isCount(v) ? v : null;
  if (typeof v === 'string' && /^\d{1,10}$/.test(v.trim())) { const n = Number(v.trim()); return isCount(n) ? n : null; }
  return null;
}

/**
 * ロジザードの今の在庫と引当 (削除の確かめだけ)。reader = readMirrorLogizardStock (試験は差し替え)。
 * 戻り値 = { ok: true, asOf, stale, map: Map(code_norm → { qty, allocated } | { bad: 理由 }) } / { ok: false, error }。Map に無い = 写しに行が無い = 0
 */
export async function readLogizardForDelete({ now = Date.now(), reader = readMirrorLogizardStock } = {}) {
  let got;
  try { got = await reader(process.env.DATA_DIR || path.join(process.cwd(), 'data')); } catch (e) { return { ok: false, error: `ロジザードの在庫を読めない (${String((e && e.message) || e).slice(0, 100)})` }; }
  if (!got || !Array.isArray(got.rows)) return { ok: false, error: 'ロジザードの在庫の写しがまだありません' };
  const map = new Map();
  for (const r of got.rows) {
    const k = normSku(r['商品ID']);
    if (!k) continue;
    const prev = map.get(k) || { qty: 0, allocated: 0 };
    if (prev.bad) continue;
    const q = countOf(r['在庫数']); const a = countOf(r['引当数']);
    if (q == null || a == null) { map.set(k, { bad: `ロジザードの在庫数・引当数が読めない行がある (在庫数 ${String(r['在庫数']).slice(0, 12)}・引当数 ${String(r['引当数']).slice(0, 12)})` }); continue; }
    map.set(k, { qty: prev.qty + q, allocated: prev.allocated + a });
  }
  return { ok: true, asOf: got.capturedAt, stale: stockStale(got.capturedAt, now), map };
}

/**
 * skus = [{ code, code_norm }] (単品だけ)。戻り値 = Map(code_norm → { ne_own, ne_allocated, ne_on_order, fba, logizard, logizard_allocated, po_backorder, sources } | { unreadable })
 * readers = 試験の差し替え ({ pml, logizard, backorders })
 */
export async function readNeDeleteStock(skus, { now = Date.now(), readers = {} } = {}) {
  const pml = await (readers.pml || readPmlStock)(skus.map((s) => s.code), { now });
  const lz = await (readers.logizard || readLogizardForDelete)({ now });
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
    const lzRow = lz && lz.ok ? (lz.map.get(s.code_norm) || { qty: 0, allocated: 0 }) : null;
    if (lzRow && lzRow.bad) why.push(lzRow.bad);
    else if (lzRow && !(isCount(lzRow.qty) && isCount(lzRow.allocated))) why.push('ロジザードの在庫の値が読めない');
    const po = bo && bo.ok ? backorderOf(bo, s.code) : null;
    if (bo && bo.ok && !isCount(po)) why.push('発注アプリの注文残の値が読めない');
    if (why.length) { out.set(s.code_norm, { unreadable: why.join(' / ').slice(0, 200) }); continue; }
    const neOwn = row.total - row.fba;
    if (!isCount(neOwn)) { out.set(s.code_norm, { unreadable: '商品管理リストの NE の在庫 (総在庫数 − FBA在庫数) が読めない' }); continue; }
    out.set(s.code_norm, {
      ne_own: neOwn, ne_allocated: row.allocated, ne_on_order: row.onOrder, fba: row.fba,
      logizard: lzRow.qty, logizard_allocated: lzRow.allocated, po_backorder: po,
      sources: { pml_run: pml.runId, ne_synced_at: pml.neSyncedAt, logizard_as_of: lz.asOf, read_at: new Date(now).toISOString() },
    });
  }
  return out;
}
