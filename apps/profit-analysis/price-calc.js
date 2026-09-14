/**
 * 「売価を変えて試算」の入力を NE 商品マスタ (mirror_products) から作る (2026-09-14 中原さん指示)。
 * 修正後の売価を決めるための計算機。
 *
 * 式は商品ハブの基本情報タブと**全く同じ** (中原さん指定):
 *   利益額 = 売価(税込) × TAKE_RATE(0.9) − 原価(税抜) × (1 + 税率/100) − 送料
 *
 * 🚨 数値の読み方・税率・配送方法の選択肢も商品ハブの関数をそのまま使う
 *    (写すと、片方だけ直した日に 2 つの画面の数字がずれる)
 *    - 原価・送料 = mirror_products の値。null は null のまま (0 円にしない)
 *    - 税率 = taxToPercent(消費税率) (8 / 10 だけ)。読めなければ 10 (商品ハブの最後の既定値と同じ)
 *    - 配送方法の選択肢 = listNeShippingOptions → profitShipChoices
 * 🚨 商品ハブはバリエーション (代表商品コード) なら子 SKU を集計するが、ここは
 *    **出品が指している品番そのもの**の行を見る (商品ハブの SKU 表と同じ = 1 SKU ずつの原価・送料)。
 *    商品ハブの税率は下書きの Yahoo 税率欄を先に見るが、ここには下書きが無いので NE の税率から始める
 */
import { TAKE_RATE } from '../product-hub/lib/profit.js';
import { taxToPercent, listNeShippingOptions, profitShipChoices } from '../product-hub/lib/variation.js';

export const DEFAULT_TAX_PERCENT = 10;

// 商品ハブ (variation.js getNeCost) と同じ読み方
const num = (v) => (v == null || !Number.isFinite(Number(v)) ? null : Number(v));

/**
 * @returns {{found:false, reason:string, take_rate:number}
 *   | {found:true, ne_code:string, product_name:string|null, cost_ex_tax:number|null,
 *      shipping_cost:number|null, shipping_method:string|null, tax_percent:number,
 *      tax_source:'ne'|'default', take_rate:number, ship_choices:Array}}
 */
export function priceCalcInputs(db, neCode) {
  const code = String(neCode ?? '').trim().toLowerCase();
  if (!code) return { found: false, reason: 'no_code', take_rate: TAKE_RATE };
  let rows = [];
  try {
    rows = db.prepare(`SELECT 商品コード, 商品名, 原価, 送料, 配送方法, 消費税率
      FROM mirror_products WHERE LOWER(TRIM(商品コード)) = ?`).all(code);
  } catch {
    return { found: false, reason: 'mirror_missing', take_rate: TAKE_RATE };   // mirror 未作成
  }
  if (rows.length === 0) return { found: false, reason: 'not_found', take_rate: TAKE_RATE };
  // 大文字小文字だけ違う別コードが重なったら、どちらの原価か決められない。黙って片方を使わない
  if (rows.length > 1) return { found: false, reason: 'ambiguous', take_rate: TAKE_RATE };
  const row = rows[0];
  const shippingMethod = row.配送方法 && String(row.配送方法).trim() !== '' ? String(row.配送方法).trim() : null;
  const shippingCost = num(row.送料);
  const neTax = taxToPercent(row.消費税率);
  return {
    found: true,
    ne_code: String(row.商品コード).trim(),
    product_name: row.商品名 ?? null,
    cost_ex_tax: num(row.原価),
    shipping_cost: shippingCost,
    shipping_method: shippingMethod,
    tax_percent: neTax ?? DEFAULT_TAX_PERCENT,
    tax_source: neTax != null ? 'ne' : 'default',
    take_rate: TAKE_RATE,
    // 送料が無ければ商品ハブと同じく計算しない (選択肢も出さない)
    ship_choices: shippingCost == null ? [] : profitShipChoices(listNeShippingOptions(db), shippingMethod, shippingCost),
  };
}
