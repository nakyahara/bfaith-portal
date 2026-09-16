/**
 * mall-orders-transform.mjs — モールの注文 (warehouse.db の raw_*_orders) を Company DB の受け皿 `core.apply_order_batch()` の header / lines (0013、08 §4.1 / §4.7) に整える。D5b
 * ここは純粋関数だけ (DB も HTTP も触らない)。まず楽天 (D5b-1)。
 *
 * 楽天 (raw_rakuten_orders。1 行 = 注文 × 明細 (item_detail_id)。注文の列は明細行に重複して入っている):
 *   - 注文の鍵 = order_number → mall 'rakuten' / scope 'main' / shop_code '1' (core.ne_shops 1 = 楽天市場店)
 *   - ordered_at = order_date (ISO8601 '+0900' = JST) → '+09:00' に直して送る。order_date_jst は Render が JST で切る
 *   - 状態 = orderProgress (100〜900) → status_source (Render が core.order_status_map 'rakuten' で正規化。0016)。800 / 900 = キャンセル系 → is_cancelled
 *   - 金額 (税込・円): 顧客が払う額 = request_price (請求金額) / 商品代 = goods_price / 送料 = postage_price / 店負担クーポン = coupon_shop_price /
 *     モール負担クーポン = coupon_all_total_price − coupon_shop_price (全店クーポン総額 = 楽天負担 + 店負担) / ポイント = 取っていない (null)
 *     🚨 取込の番兵 -9999 (値が無かった) は null にする (Render の CHECK >= 0 に当てない)。負の値も null (注文に負の金額は無いはず = 数える)
 *   - 明細: line_key = item_detail_id / listing_code = item_number (商品番号 W。Render が listing_code か別名 (external_ids) で解決。色違いは同じ W を共有するので解決できない
 *     = 宿題: raw に SKU 単位のコードを足す) / sku_code は送らない / qty = units / cancelled_qty = delete_item_flag なら units / unit_price_jpy = price_tax_incl (税込) /
 *     line_amount_jpy = price_tax_incl × units / tax_rate = 0.08 か 0.10 (それ以外は null)
 *   - source_updated_at = synced_at (取込時刻。楽天の raw にはモール側の更新時刻が無い) → content_hash が変化の判定を担う
 */
import { canonicalJson, contentHash, utcToIso } from './ne-shipments-transform.mjs';

export const RAKUTEN_TRANSFORM_VERSION = 'rakuten-orders-1';
export const RAKUTEN_SENTINEL = -9999;
const CANCELLED_STATUSES = new Set([800, 900]);

const nz = (v) => { if (v == null) return null; const t = String(v).trim(); return t === '' ? null : t; };
function intOrNull(v, label) {
  if (v == null || v === '') return null;
  const n = typeof v === 'number' ? v : Number(String(v).trim());
  if (!Number.isInteger(n)) throw new Error(`${label} が整数でない: "${v}"`);
  return n;
}
/** 円の金額: null / 番兵 / 負 → null (注文に負の金額は無いはず。あれば stats に数える)。小数は四捨五入 */
export function yen(v, stats = null, label = '') {
  if (v == null || v === '') return null;
  const n = typeof v === 'number' ? v : Number(v);
  if (!Number.isFinite(n)) throw new Error(`${label} が数でない: "${v}"`);
  if (n === RAKUTEN_SENTINEL) { if (stats) stats.sentinel = (stats.sentinel || 0) + 1; return null; }
  if (n < 0) { if (stats) stats.negative = (stats.negative || 0) + 1; return null; }
  return Math.round(n);
}
/** 楽天の注文日時 'YYYY-MM-DDTHH:MM:SS+0900' → 'YYYY-MM-DDTHH:MM:SS+09:00' (Z や +09:00 のままの形も通す) */
export function rakutenDatetimeToIso(s, label = '楽天の注文日時') {
  const t = nz(s);
  if (!t) return null;
  const m = /^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})(?:\.\d+)?(Z|[+-]\d{2}:?\d{2})?$/.exec(t);
  if (!m) throw new Error(`${label}の形が違う: "${t}"`);
  let tz = m[3] || '+09:00';
  if (/^[+-]\d{4}$/.test(tz)) tz = `${tz.slice(0, 3)}:${tz.slice(3)}`;
  const iso = `${m[1]}T${m[2]}${tz}`;
  if (Number.isNaN(Date.parse(iso))) throw new Error(`${label}が日時として不正: "${t}"`);
  return iso;
}
export const taxRateOf = (v) => { const n = Number(v); return n === 0.08 || n === 0.1 ? n : null; };

/**
 * 1 注文 (raw_rakuten_orders の同じ order_number の行) を送る形に整える。
 * @param {object[]} rows 同じ order_number の行 (順不同でよい)
 * @param {{ fallbackSourceUpdatedAt?: string, stats?: object }} [opts]
 * @returns {{ key, payload: { mall, scope_key, mall_order_no, header, lines }, n_lines, source_updated_at, no_synced_at }}
 */
export function buildRakutenOrder(rows, opts = {}) {
  if (!rows || !rows.length) throw new Error('行が無い');
  const no = nz(rows[0].order_number);
  if (!no) throw new Error('order_number が無い');
  for (const r of rows) if (nz(r.order_number) !== no) throw new Error(`注文 ${no} に別の注文 ${r.order_number} の行が混ざっている`);
  const stats = opts.stats || null;
  const h0 = rows[0];
  const status = intOrNull(h0.order_status, `注文 ${no} の order_status`);
  const shopCoupon = yen(h0.coupon_shop_price, stats, `注文 ${no} の coupon_shop_price`);
  const allCoupon = yen(h0.coupon_all_total_price, stats, `注文 ${no} の coupon_all_total_price`);
  const mallCoupon = allCoupon == null || shopCoupon == null ? null : Math.max(0, allCoupon - shopCoupon);
  const header = {
    source_system: 'mall_api',
    shop_code: '1',
    ordered_at: rakutenDatetimeToIso(h0.order_date, `注文 ${no} の order_date`),
    status_source: status == null ? null : String(status),
    is_cancelled: status != null && CANCELLED_STATUSES.has(status),
    cancelled_at: null,
    shipped_at_source: null,
    total_amount_jpy: yen(h0.request_price, stats, `注文 ${no} の request_price`),
    items_amount_jpy: yen(h0.goods_price, stats, `注文 ${no} の goods_price`),
    shipping_fee_jpy: yen(h0.postage_price, stats, `注文 ${no} の postage_price`),
    shop_coupon_jpy: shopCoupon,
    mall_coupon_jpy: mallCoupon,
    points_used_jpy: null,
    amount_source: 'mall_api',
    currency: 'JPY',
  };
  if (!header.ordered_at) throw new Error(`注文 ${no} の order_date が無い`);
  const seen = new Set();
  const lines = [];
  for (const r of rows) {
    const detail = intOrNull(r.item_detail_id, `注文 ${no} の item_detail_id`);
    if (detail == null) throw new Error(`注文 ${no} に item_detail_id の無い行がある`);
    const key = String(detail);
    if (seen.has(key)) throw new Error(`注文 ${no} の item_detail_id ${key} が重複している`);
    seen.add(key);
    const units = intOrNull(r.units, `注文 ${no} 明細 ${key} の units`);
    if (units == null) throw new Error(`注文 ${no} 明細 ${key} の units が無い (欠落を 0 にしない)`);
    const unit = yen(r.price_tax_incl, stats, `注文 ${no} 明細 ${key} の price_tax_incl`);
    const deleted = intOrNull(r.delete_item_flag, `注文 ${no} 明細 ${key} の delete_item_flag`) === 1;
    lines.push({
      line_key: key,
      listing_code: nz(r.item_number),
      sku_code: null,
      qty: units,
      cancelled_qty: deleted ? units : 0,
      unit_price_jpy: unit,
      line_amount_jpy: unit == null ? null : unit * units,
      tax_rate: taxRateOf(r.tax_rate),
      amount_source: 'mall_api',
      source_line_ref: `item_detail_id:${key}`,
    });
  }
  lines.sort((a, b) => Number(a.line_key) - Number(b.line_key));
  const syncedAts = rows.map((r) => nz(r.synced_at)).filter(Boolean).sort();
  const sourceUpdatedAt = syncedAts.length ? syncedAts[syncedAts.length - 1] : null;
  let sourceUpdatedIso = sourceUpdatedAt ? utcToIso(sourceUpdatedAt, `注文 ${no} の synced_at`) : null;
  if (!sourceUpdatedIso) {
    if (!opts.fallbackSourceUpdatedAt) throw new Error(`注文 ${no} に synced_at が 1 つも無い`);
    sourceUpdatedIso = opts.fallbackSourceUpdatedAt;
  }
  const payload = { mall: 'rakuten', scope_key: 'main', mall_order_no: no, header: { ...header, source_updated_at: sourceUpdatedIso, transform_version: RAKUTEN_TRANSFORM_VERSION, content_hash: contentHash(header) }, lines };
  return { key: `rakuten|main|${no}`, payload, n_lines: lines.length, source_updated_at: sourceUpdatedAt, no_synced_at: !sourceUpdatedAt };
}

export { canonicalJson };
