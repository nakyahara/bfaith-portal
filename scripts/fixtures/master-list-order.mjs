/**
 * master-list-order.mjs — 商品・セットの一覧の並び (10/8 PR1) の「正しい並び」を、一覧に出している値から手で作る (試験だけ)。
 * 並べる関数 (apps/master-edit/read.mjs の listSkus) を使わずに、listSkus(mode 'all') の行の値 (= 画面・CSV に出る値) を JS で並べる。
 * 決まり: 空 (null) は向きに関係なく最後・同じ値はコード (code_norm) の昇順。文字は文字の番号の順 (DB は collate "C")
 */
export const DISPLAY_VALUE = Object.freeze({
  code: (r) => r.code_norm,
  name: (r) => r.name,
  kind: (r) => ({ single: 0, set: 1, exception: 2 })[r.kind],
  state: (r) => (r.state === 'discontinued' ? 1 : 0),
  reg: (r) => r.registered_on ?? null,
  price: (r) => r.standard_price,
  cost: (r) => r.cost,
  tax: (r) => r.tax_rate,
  profit: (r) => r.profit,
  rate: (r) => r.profit_rate,
  sales_class: (r) => r.sales_class,
  stock: (r) => (r.kind === 'set' ? r.buildable : r.stock),
  sup: (r) => r.primary_supplier ?? null,
});
const cmpRaw = (x, y) => (typeof x === 'number' && typeof y === 'number' ? x - y : x < y ? -1 : x > y ? 1 : 0);
/** rows = listSkus の行 (code_norm が要る)・col = 列の id・dir = 'asc' | 'desc' → 商品コードの並び */
export function expectedOrder(rows, col, dir = 'asc') {
  const val = DISPLAY_VALUE[col];
  const sign = dir === 'desc' ? -1 : 1;
  return [...rows].sort((a, b) => {
    const x = val(a); const y = val(b);
    const xn = x == null; const yn = y == null;
    if (xn !== yn) return xn ? 1 : -1;
    const c = xn ? 0 : cmpRaw(x, y) * sign;
    return c || cmpRaw(a.code_norm, b.code_norm);
  }).map((r) => r.code);
}
/** 並びの中で「空でない値が何種類あるか」(試験の材料が並びを確かめられる形か = 全部同じ値・全部空では並びを確かめたことにならない) */
export function distinctValues(rows, col) {
  return new Set(rows.map(DISPLAY_VALUE[col]).filter((v) => v != null)).size;
}
export function nullCount(rows, col) { return rows.filter((r) => DISPLAY_VALUE[col](r) == null).length; }
