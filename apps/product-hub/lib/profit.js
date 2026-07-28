/**
 * 利益シミュレーション — Notion 商品マスターの数式の移植 (2026-07-28 中原さん提供)。
 *
 * Notion 原式:
 *   利益額 = (prop("売価") * 0.9) - (prop("原価") * (1 + toNumber(prop("税率")) / 100))
 *            - toNumber(first(prop("配送関係費合計")))
 *   利益率 = prop("利益額") / prop("売価")
 *
 * 対応:
 *   売価×0.9        … モール手数料等 10% を控除した手取り
 *   原価×(1+税率)   … NE の原価は税抜き → 税込換算
 *   配送関係費合計   … Notion では配送区分マスタの rollup。アプリでは NE の送料列を使う
 *
 * ⚠️ 表示用の目安 (登録 payload には含めない)。値が欠けたら計算せず null を返す。
 */

/** 手取り率 (= 1 − モール手数料等 10%)。Notion 式の 0.9 */
export const TAKE_RATE = 0.9;

/**
 * @param {object} p
 * @param {number|null} p.price       売価 (税込・円)
 * @param {number|null} p.costExTax   原価 (税抜・円) — NE mirror_products.原価
 * @param {number} p.taxPercent  原価にかかる税率 (8 or 10)。フォールバック (Yahoo欄→NE→10%) は呼び出し側の責務
 * @param {number|null} p.shippingCost 配送関係費 (円) — NE mirror_products.送料。null は 0 扱いにせず計算不可
 * @returns {{profit:number, marginPct:number, costIncTax:number}|null}
 */
export function computeProfit({ price, costExTax, taxPercent, shippingCost }) {
  // ⚠️ Number(null)=0 なので null を先に弾く (原価nullを0円扱いすると利益が過大に見える)
  const req = (v) => (v == null || v === '' ? NaN : Number(v));
  const priceN = req(price);
  const costN = req(costExTax);
  const shipN = req(shippingCost);
  const taxN = req(taxPercent);
  if (!Number.isFinite(priceN) || priceN <= 0) return null;
  if (!Number.isFinite(costN) || costN < 0) return null;
  if (!Number.isFinite(shipN) || shipN < 0) return null;
  if (!Number.isFinite(taxN) || taxN < 0 || taxN > 100) return null;
  const costIncTax = costN * (1 + taxN / 100);
  const profit = priceN * TAKE_RATE - costIncTax - shipN;
  return {
    profit: Math.round(profit),
    marginPct: Math.round((profit / priceN) * 1000) / 10, // 小数1桁
    costIncTax: Math.round(costIncTax),
  };
}
