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

/*
 * Amazon (raw_sp_orders = 注文 ID 単位で最新の状態に置き換わる current 表。1 行 = 注文 × 明細。apps/warehouse/sp-api-orders.js が
 * GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL から作る。追記ログ raw_sp_orders_log は 60 日で回転するので使わない)。D5b-2。2026-09-18 の実測 (2025-01-01 以降 128.6 万注文):
 *   - 注文の鍵 = amazon_order_id → mall 'amazon' / scope 'jp' (core.ne_shops 4 と同じ scope)。
 *     shop_code = 自社発送 (fulfillment_channel 'Merchant') は '4' (NE の店舗 4 = 雑貨イズムAmazon店)、**FBA ('Amazon') は null** (FBA は NE を通らない = NE の店舗が無い)
 *   - 🚨 sales_channel が 'Amazon.co.jp' でない注文 (Non-Amazon / Non-Amazon JP = マルチチャネル発送。他モールの注文を FBA から出しただけ = Amazon の売上ではない。635 注文) は
 *     送らない (送り手の iterate が飛ばして数える。ここに渡ってきたら例外)
 *   - ordered_at = purchase_date (取込側が '+09:00' の ISO8601 にそろえている)。source_updated_at = last_updated_date (**モール側の更新時刻**。無ければ synced_at = 取込時刻)
 *   - 状態 = order_status の原文 ('Shipped' / 'Shipped - Delivered to Buyer' / 'Cancelled' / 'Pending' …) → status_source (Render が core.order_status_map 'amazon' で正規化。0018)。'Cancelled' → is_cancelled
 *   - 明細 ID が無い (同じ注文・SKU・ASIN で 2 行ある組が 838、全列が同じ組が 362) → line_key = `${seller_sku}|${asin}#${同じ組の中の番号}`。組の中は内容 (状態・数量・金額) で並べる
 *     = 取込のたびに raw の id が変わっても同じ内容なら同じ鍵。片方だけ変わったときは鍵と行の対応が入れ替わり得るが、明細集合は注文ごとに丸ごと置き換える契約なので集合としては正しい
 *   - listing_code = seller_sku (core.listings の Amazon は listing_code = seller SKU)。sku_code は送らない
 *   - 金額 (税込・円): item_price は **行の合計 (単価 × 数量) で税込** (item_tax は内数。10/110 に合う行 96%・8/108 に合う行 3%)。line_amount_jpy = item_price / unit_price_jpy = 割り切れるときだけ / tax_rate = item_tax から逆算 (どちらか一方にだけ合うとき)
 *     🚨 取込側が `parseFloat(x) || 0` で入れている = 「値が無い」と「0 円」が raw で区別できない。Amazon は取消の行の数量と金額を空にする (取消 78,477 行のうち 数量 0 = 78,473) →
 *     **item_price = 0 は null にして数える** (stats.zeroPrice。0 円の売上として確定させない)。数量 0 はそのまま 0 (qty は必須)。取消でない行の数量 0 は数える (stats.zeroQtyLive)
 *   - ヘッダの金額: **金額の分からない (item_price = 0) 取消でない明細が 1 つでも残る注文は 商品代・送料・店負担の値引 とも null** (分かる行だけの部分和を注文の合計として確定させない。Codex D5b-2 R1 #3)。
 *     それ以外 = 商品代は金額のある行の合計 (1 行も無ければ null) / 送料 = shipping_price の合計・店負担の値引 = promotion_discount の合計。
 *     送料・値引の 0 を信じる根拠 = レポートは金額の列を行ごとにまとめて埋めるか・まとめて空にする (取消・保留) → **item_price が入っている行は、同じ行の送料・値引の 0 も「0 円」**。
 *     item_price が空の行の送料・値引は分からない (取消の行は合計に入れない)。顧客が払った額・モール負担の値引・ポイント = レポートに無い (null)
 */
export const AMAZON_TRANSFORM_VERSION = 'amazon-orders-1';
export const AMAZON_SALES_CHANNEL = 'Amazon.co.jp';
const numOr0 = (v, label) => { if (v == null || v === '') return 0; const n = typeof v === 'number' ? v : Number(v); if (!Number.isFinite(n)) throw new Error(`${label} が数でない: "${v}"`); return n; };
/** item_tax から税率を逆算する。10% と 8% のどちらか一方にだけ合う (端数の丸め方 3 通りのどれか) ときだけ返す */
export function amazonTaxRateOf(price, tax) {
  if (!(price > 0) || !(tax > 0)) return null;
  const fits = (rate) => { const x = price * rate / (1 + rate); return [Math.floor(x), Math.round(x), Math.ceil(x)].includes(Math.round(tax)); };
  const f10 = fits(0.1), f8 = fits(0.08);
  return f10 && !f8 ? 0.1 : f8 && !f10 ? 0.08 : null;
}
/**
 * 1 注文 (raw_sp_orders の同じ amazon_order_id の行) を送る形に整える。
 * @param {object[]} rows 同じ amazon_order_id の行 (順不同でよい)
 * @param {{ fallbackSourceUpdatedAt?: string, stats?: object }} [opts]
 */
export function buildAmazonOrder(rows, opts = {}) {
  if (!rows || !rows.length) throw new Error('行が無い');
  const no = nz(rows[0].amazon_order_id);
  if (!no) throw new Error('amazon_order_id が無い');
  const stats = opts.stats || null;
  const bump = (k, n = 1) => { if (stats) stats[k] = (stats[k] || 0) + n; };
  const h0 = rows[0];
  for (const r of rows) {
    if (nz(r.amazon_order_id) !== no) throw new Error(`注文 ${no} に別の注文 ${r.amazon_order_id} の行が混ざっている`);
    // 注文の列は明細行に重複して入っている。食い違ったらどれが正か決められない (実測 0 件)
    for (const c of ['purchase_date', 'order_status', 'fulfillment_channel', 'sales_channel']) if (nz(r[c]) !== nz(h0[c])) throw new Error(`注文 ${no} の ${c} が行によって違う`);
    const cur = nz(r.currency); if (cur && cur !== 'JPY') throw new Error(`注文 ${no} の通貨が JPY でない: ${cur}`);
  }
  if (nz(h0.sales_channel) !== AMAZON_SALES_CHANNEL) throw new Error(`注文 ${no} は Amazon.co.jp の注文でない (sales_channel = ${h0.sales_channel})`);
  const channel = nz(h0.fulfillment_channel);
  if (channel !== 'Amazon' && channel !== 'Merchant') throw new Error(`注文 ${no} の fulfillment_channel が知らない値: ${h0.fulfillment_channel}`);
  const status = nz(h0.order_status);
  const cancelled = status === 'Cancelled';

  const items = rows.map((r) => {
    const sku = nz(r.seller_sku), asin = nz(r.asin) || '';
    if (!sku) throw new Error(`注文 ${no} に seller_sku の無い行がある`);
    const qty = numOr0(r.quantity, `注文 ${no} の quantity`);
    if (!Number.isInteger(qty) || qty < 0) throw new Error(`注文 ${no} (${sku}) の quantity が 0 以上の整数でない: "${r.quantity}"`);
    const price = numOr0(r.item_price, `注文 ${no} (${sku}) の item_price`), tax = numOr0(r.item_tax, `注文 ${no} (${sku}) の item_tax`);
    const ship = numOr0(r.shipping_price, `注文 ${no} (${sku}) の shipping_price`), promo = numOr0(r.promotion_discount, `注文 ${no} (${sku}) の promotion_discount`);
    if (price < 0 || ship < 0 || promo < 0) throw new Error(`注文 ${no} (${sku}) に負の金額がある`);
    return { sku, asin, itemStatus: nz(r.item_status) || '', qty, price: Math.round(price), tax, ship: Math.round(ship), promo: Math.round(promo) };
  });
  // 同じ (SKU, ASIN) の組の中は内容で並べる (raw の id は取込のたびに変わる)
  items.sort((a, b) => (a.sku < b.sku ? -1 : a.sku > b.sku ? 1 : a.asin < b.asin ? -1 : a.asin > b.asin ? 1
    : a.itemStatus < b.itemStatus ? -1 : a.itemStatus > b.itemStatus ? 1 : a.qty - b.qty || a.price - b.price || a.tax - b.tax || a.ship - b.ship || a.promo - b.promo));
  const seq = new Map();
  let priced = 0, unknownLive = 0, itemsAmount = 0, shipping = 0, promoSum = 0;
  const lines = items.map((it) => {
    const group = `${it.sku}|${it.asin}`;
    const n = (seq.get(group) || 0) + 1; seq.set(group, n);
    const amount = it.price > 0 ? it.price : null;
    const live = !cancelled && it.itemStatus !== 'Cancelled';
    if (amount == null) { bump('zeroPrice'); if (live) unknownLive++; } else { priced++; itemsAmount += amount; shipping += it.ship; promoSum += it.promo; }   // 送料・値引は金額の入っている行のものだけ信じる
    if (it.qty === 0 && live) bump('zeroQtyLive');
    return {
      line_key: `${group}#${n}`,
      listing_code: it.sku,
      sku_code: null,
      qty: it.qty,
      cancelled_qty: 0,   // Amazon は取消の行の数量を空 (0) にする = 取消した数は分からない。行の状態は source_line_ref に
      unit_price_jpy: amount != null && it.qty > 0 && amount % it.qty === 0 ? amount / it.qty : null,
      line_amount_jpy: amount,
      tax_rate: amount == null ? null : amazonTaxRateOf(amount, it.tax),
      amount_source: 'mall_api',
      source_line_ref: `asin:${it.asin}|item_status:${it.itemStatus}`,
    };
  });
  if (unknownLive) bump('partialAmountOrders');
  const known = priced > 0 && unknownLive === 0;   // 取消でない明細の金額が全部分かっている (取消の行は合計の外)
  const header = {
    source_system: 'mall_api',
    shop_code: channel === 'Merchant' ? '4' : null,
    ordered_at: rakutenDatetimeToIso(h0.purchase_date, `注文 ${no} の purchase_date`),
    status_source: status,
    is_cancelled: cancelled,
    cancelled_at: null,
    shipped_at_source: null,
    total_amount_jpy: null,
    items_amount_jpy: known ? itemsAmount : null,
    shipping_fee_jpy: known ? shipping : null,
    shop_coupon_jpy: known ? promoSum : null,
    mall_coupon_jpy: null,
    points_used_jpy: null,
    amount_source: 'mall_api',
    currency: 'JPY',
  };
  if (!header.ordered_at) throw new Error(`注文 ${no} の purchase_date が無い`);
  const updated = rows.map((r) => nz(r.last_updated_date)).filter(Boolean).sort();
  let sourceUpdatedIso = updated.length ? rakutenDatetimeToIso(updated[updated.length - 1], `注文 ${no} の last_updated_date`) : null;
  let noSyncedAt = false;
  if (!sourceUpdatedIso) {
    const synced = rows.map((r) => nz(r.synced_at)).filter(Boolean).sort();
    sourceUpdatedIso = synced.length ? utcToIso(synced[synced.length - 1], `注文 ${no} の synced_at`) : null;
    if (!sourceUpdatedIso) { if (!opts.fallbackSourceUpdatedAt) throw new Error(`注文 ${no} に last_updated_date も synced_at も無い`); sourceUpdatedIso = opts.fallbackSourceUpdatedAt; noSyncedAt = true; }
  }
  const payload = { mall: 'amazon', scope_key: 'jp', mall_order_no: no, header: { ...header, source_updated_at: sourceUpdatedIso, transform_version: AMAZON_TRANSFORM_VERSION, content_hash: contentHash(header) }, lines };
  return { key: `amazon|jp|${no}`, payload, n_lines: lines.length, source_updated_at: sourceUpdatedIso, no_synced_at: noSyncedAt };
}

export { canonicalJson };
