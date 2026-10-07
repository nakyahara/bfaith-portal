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

/*
 * au PAY マーケット (raw_aupay_orders。1 行 = 注文 × 明細 (order_detail_id)。PK = (order_id, order_detail_id)。apps/warehouse/aupay-orders.js)。D5b-3。2026-09-18 の実測 (2025-01-01 以降 12,238 注文 / 13,238 明細):
 *   - 🚨 この表には注文者・送付先の氏名・住所・電話・メールの列がある → **送り手は要る列だけを select する** (MALL_SPECS.aupay.iterate。ここに渡る行にも載せない)
 *   - 注文の鍵 = order_id → mall 'aupay' / scope 'main' / shop_code '5' (core.ne_shops 5。NE の受注番号 = order_id が 11,931 / 11,950 伝票で一致)
 *   - ordered_at = order_date ('YYYY/MM/DD HH:MM' = JST・秒なし) → 'YYYY-MM-DDTHH:MM:00+09:00'
 *   - 状態 = order_status の原文 (完了 / キャンセル / 発送前入金待ち / 発送待ち) → status_source (0019 の対応表 'aupay')。cancel_status = 'C' か order_status = 'キャンセル' → is_cancelled
 *   - 金額 (税込・円。実測で 3 つの式が全注文で成り立つ): 明細の合計 = total_sale_price / total_price = total_sale + 送料 + 手数料 + オプション + ラッピング /
 *     request_price = total_price − クーポン − ポイント − au ポイント。→ 顧客が払った額 = request_price / 商品代 = total_sale_price / 送料 = postage_price /
 *     店負担の値引 = coupon_total_price (ストアクーポン。既存の f_aupay_finance と同じ扱い) / モール負担 = レポートに無い (null) / ポイント = use_point + use_au_point_price
 *   - 明細: line_key = order_detail_id / **sku_code = item_code** (Company DB に au PAY の出品は無い。item_code は NE の商品コードで 90% が m_products に当たる。当たらなければ Render が unresolved_code に原文を残す) /
 *     qty = unit / cancelled_qty = item_cancel_status が 'N' なら 0・'C' なら unit (それ以外は知らない値 = 例外) / unit_price = item_price / line_amount = total_item_price / tax_rate = 0.08 か 0.10
 *   - source_updated_at = synced_at (取込時刻。モール側の更新時刻は取っていない) → 変化は指紋
 */
export const AUPAY_TRANSFORM_VERSION = 'aupay-orders-1';
/** 送り手が raw_aupay_orders から読む列 (これ以外 = 個人情報・自由記述は読まない) */
export const AUPAY_COLUMNS = ['order_id', 'order_detail_id', 'order_date', 'order_status', 'cancel_status', 'total_sale_price', 'postage_price', 'coupon_total_price', 'use_point', 'use_au_point_price', 'request_price',
  'item_code', 'item_cancel_status', 'item_price', 'unit', 'total_item_price', 'tax_rate', 'synced_at'];
/**
 * 実在する年月日で、時 00〜23・分秒 00〜59 か。🚨 Date.parse は '13 月' を NaN にするが '24:00:00' や '2 月 30 日' は翌日・3 月に繰り上げて受ける →
 * 範囲・突合 (先頭 10 文字の日付) と Render の order_date_jst が 1 日ずれる。繰り上がる値は受けない (Codex D5b-3 R2)
 */
export function isRealDateTime(y, mo, d, h, mi, s) {
  const n = [y, mo, d, h, mi, s].map(Number);
  if (n.some((x) => !Number.isInteger(x))) return false;
  if (n[3] > 23 || n[4] > 59 || n[5] > 59) return false;
  const dt = new Date(Date.UTC(n[0], n[1] - 1, n[2]));
  return dt.getUTCFullYear() === n[0] && dt.getUTCMonth() === n[1] - 1 && dt.getUTCDate() === n[2];
}
/** 'YYYY/MM/DD HH:MM' (JST。秒は無いことが多い) → ISO8601 +09:00 */
export function aupayDatetimeToIso(s, label = 'au PAY の注文日時') {
  const t = nz(s);
  if (!t) return null;
  const m = /^(\d{4})[/-](\d{2})[/-](\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?$/.exec(t);
  if (!m) throw new Error(`${label}の形が違う: "${t}"`);
  if (!isRealDateTime(m[1], m[2], m[3], m[4], m[5], m[6] || '00')) throw new Error(`${label}が日時として不正: "${t}"`);
  return `${m[1]}-${m[2]}-${m[3]}T${m[4]}:${m[5]}:${m[6] || '00'}+09:00`;
}
/** 0 以上の円 (null は null のまま。負・数でないは例外 = au PAY の raw に番兵は無い) */
function yenStrict(v, label) {
  if (v == null || v === '') return null;
  const n = typeof v === 'number' ? v : Number(v);
  if (!Number.isFinite(n) || n < 0) throw new Error(`${label} が 0 以上の数でない: "${v}"`);
  return Math.round(n);
}
const sumOrNull = (...xs) => (xs.every((x) => x == null) ? null : xs.reduce((a, x) => a + (x || 0), 0));
function latestSyncedIso(rows, no, opts) {
  const synced = rows.map((r) => nz(r.synced_at)).filter(Boolean).sort();
  const last = synced.length ? synced[synced.length - 1] : null;
  // 取込時刻の形はモールごとに違う: 'YYYY-MM-DD HH:MM:SS' (UTC。au PAY) / ISO8601 +09:00 (LINE ギフト)
  let iso = null;
  if (last) iso = /[T].*(Z|[+-]\d{2}:?\d{2})$/.test(last) ? rakutenDatetimeToIso(last, `注文 ${no} の synced_at`) : utcToIso(last, `注文 ${no} の synced_at`);
  if (iso) return { iso, missing: false };
  if (!opts.fallbackSourceUpdatedAt) throw new Error(`注文 ${no} に synced_at が 1 つも無い`);
  return { iso: opts.fallbackSourceUpdatedAt, missing: true };
}
export function buildAupayOrder(rows, opts = {}) {
  if (!rows || !rows.length) throw new Error('行が無い');
  const no = nz(rows[0].order_id);
  if (!no) throw new Error('order_id が無い');
  const h0 = rows[0];
  for (const r of rows) {
    if (nz(r.order_id) !== no) throw new Error(`注文 ${no} に別の注文 ${r.order_id} の行が混ざっている`);
    for (const c of ['order_date', 'order_status', 'cancel_status', 'total_sale_price', 'postage_price', 'coupon_total_price', 'use_point', 'use_au_point_price', 'request_price']) {
      if ((r[c] ?? null) !== (h0[c] ?? null)) throw new Error(`注文 ${no} の ${c} が行によって違う`);   // 注文の列は明細行に重複して入っている (実測で食い違い 0)
    }
  }
  const status = nz(h0.order_status);
  const cancelled = nz(h0.cancel_status) === 'C' || status === 'キャンセル';
  const header = {
    source_system: 'mall_api',
    shop_code: '5',
    ordered_at: aupayDatetimeToIso(h0.order_date, `注文 ${no} の order_date`),
    status_source: status,
    is_cancelled: cancelled,
    cancelled_at: null,
    shipped_at_source: null,
    total_amount_jpy: yenStrict(h0.request_price, `注文 ${no} の request_price`),
    items_amount_jpy: yenStrict(h0.total_sale_price, `注文 ${no} の total_sale_price`),
    shipping_fee_jpy: yenStrict(h0.postage_price, `注文 ${no} の postage_price`),
    shop_coupon_jpy: yenStrict(h0.coupon_total_price, `注文 ${no} の coupon_total_price`),
    mall_coupon_jpy: null,
    points_used_jpy: sumOrNull(yenStrict(h0.use_point, `注文 ${no} の use_point`), yenStrict(h0.use_au_point_price, `注文 ${no} の use_au_point_price`)),
    amount_source: 'mall_api',
    currency: 'JPY',
  };
  if (!header.ordered_at) throw new Error(`注文 ${no} の order_date が無い`);
  const seen = new Set();
  const lines = rows.map((r) => {
    const key = nz(r.order_detail_id);
    if (!key) throw new Error(`注文 ${no} に order_detail_id の無い行がある`);
    if (seen.has(key)) throw new Error(`注文 ${no} の order_detail_id ${key} が重複している`);
    seen.add(key);
    const unit = intOrNull(r.unit, `注文 ${no} 明細 ${key} の unit`);
    if (unit == null || unit < 0) throw new Error(`注文 ${no} 明細 ${key} の unit が無い (欠落を 0 にしない)`);
    const ics = nz(r.item_cancel_status);
    if (ics != null && ics !== 'N' && ics !== 'C') throw new Error(`注文 ${no} 明細 ${key} の item_cancel_status が知らない値: ${ics}`);
    return {
      line_key: key,
      listing_code: null,
      sku_code: nz(r.item_code),
      qty: unit,
      cancelled_qty: ics === 'C' ? unit : 0,
      unit_price_jpy: yenStrict(r.item_price, `注文 ${no} 明細 ${key} の item_price`),
      line_amount_jpy: yenStrict(r.total_item_price, `注文 ${no} 明細 ${key} の total_item_price`),
      tax_rate: taxRateOf(r.tax_rate),
      amount_source: 'mall_api',
      source_line_ref: `order_detail_id:${key}`,
    };
  });
  for (const l of lines) if (!l.sku_code) throw new Error(`注文 ${no} 明細 ${l.line_key} の item_code が無い`);
  lines.sort((a, b) => (a.line_key < b.line_key ? -1 : a.line_key > b.line_key ? 1 : 0));
  const su = latestSyncedIso(rows, no, opts);
  const payload = { mall: 'aupay', scope_key: 'main', mall_order_no: no, header: { ...header, source_updated_at: su.iso, transform_version: AUPAY_TRANSFORM_VERSION, content_hash: contentHash(header) }, lines };
  return { key: `aupay|main|${no}`, payload, n_lines: lines.length, source_updated_at: su.iso, no_synced_at: su.missing };
}

/*
 * LINE ギフト (raw_linegift_orders。**1 行 = 1 注文 = 1 商品** (PK = order_id。数量 stock_count は実測で全件 1)。apps/warehouse/linegift-orders.js)。D5b-3。
 * 2026-09-18 の実測: 5,809 注文 (raw は 2026-02-07 以降だけ。それより前の注文は raw に無い = NE 店舗 14 の伝票 10,399 のうち結べるのは 5,388)。
 *   - 🚨 この表には LINE の ID・送付先の氏名・住所・電話の列がある → **送り手は要る列だけを select する** (MALL_SPECS.linegift.iterate)
 *   - 注文の鍵 = order_id (9 桁) → mall 'linegift' / scope 'main' / shop_code '14' (core.ne_shops 14 = いまの LINE ギフト店。11 は古い店)
 *   - ordered_at = bought_at_jst (ISO8601 '.000+09:00')
 *   - 状態 = status の原文 (received / cancel / payment / gift_message_send / gift_message_wait / cvs) → 0019 の対応表 'linegift'。
 *     🚨 'received' は「届いた」ではない: received の 5,389 件は全部に発送時刻 (delivered_on) と送り状番号があり、delivered_on = NE の出荷確定日 (5,221 / 5,387)、
 *     received_on は delivered_on とほぼ同時刻 = **店が発送した後の終端の状態** → shipped (配達完了の根拠は無い)。shipped_at_source = delivered_at_jst
 *   - 金額: selling_price = 売価 (税込。送料込みの価格設定) → 商品代。送料は API に無い (全件 null) / 顧客が払った額・値引・ポイント = 無い (null)。fee (モール手数料) は注文の金額ではないので送らない
 *   - 明細は 1 行: line_key = '1' / sku_code = sku_code (variation.code。m_products に 100% 当たる) / qty = stock_count / 取消なら cancelled_qty = qty / 税率は raw に無い (null)
 */
export const LINEGIFT_TRANSFORM_VERSION = 'linegift-orders-1';
/** LINE ギフトの日時は取込側が必ずこの形 (JST) にする。範囲・突合が先頭 10 文字を JST の日付として使うので、ほかの形 (Z や別の時差) は受けない (Codex D5b-3 R1 #2) */
export const LINEGIFT_JST_RE = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?\+09:00$/;
/** LINE ギフトの日時として受けられるか (形 + 実在する日時)。送り手の iterate (範囲の判定の前) と整形が同じ関数を使う = 片方だけ通る値を作らない */
// 🚨 原文のまま検証する (trim しない): 範囲・突合は原文の先頭 10 文字を使うので、前後に空白がある値を「空白を除けば正しい」と受けると範囲の外に黙って落ちる (Codex D5b-3 R3)
export function isLinegiftJst(s) { const m = typeof s === 'string' ? LINEGIFT_JST_RE.exec(s) : null; return !!m && isRealDateTime(m[1], m[2], m[3], m[4], m[5], m[6]); }
const linegiftJst = (s, label) => { if (s == null || s === '') return null; if (!isLinegiftJst(s)) throw new Error(`${label}が JST (+09:00) の ISO8601 でない (形か日時が不正・前後の空白も不可): "${s}"`); return rakutenDatetimeToIso(s, label); };
export const LINEGIFT_COLUMNS = ['order_id', 'status', 'selling_price', 'sku_code', 'stock_count', 'bought_at_jst', 'delivered_at_jst', 'synced_at'];
export function buildLinegiftOrder(rows, opts = {}) {
  if (!rows || rows.length !== 1) throw new Error(`LINE ギフトの注文は 1 行のはず (${rows ? rows.length : 0} 行)`);
  const r = rows[0];
  const no = nz(r.order_id);
  if (!no) throw new Error('order_id が無い');
  const status = nz(r.status);
  const cancelled = status === 'cancel';
  const qty = intOrNull(r.stock_count, `注文 ${no} の stock_count`);
  if (qty == null || qty < 0) throw new Error(`注文 ${no} の stock_count (数量) が無い (欠落を 0 にしない)`);
  const sku = nz(r.sku_code);
  if (!sku) throw new Error(`注文 ${no} の sku_code が無い`);
  const price = yenStrict(r.selling_price, `注文 ${no} の selling_price`);
  const header = {
    source_system: 'mall_api',
    shop_code: '14',
    ordered_at: linegiftJst(r.bought_at_jst, `注文 ${no} の bought_at_jst`),
    status_source: status,
    is_cancelled: cancelled,
    cancelled_at: null,
    shipped_at_source: linegiftJst(r.delivered_at_jst, `注文 ${no} の delivered_at_jst`),
    total_amount_jpy: null,
    items_amount_jpy: price,
    shipping_fee_jpy: null,
    shop_coupon_jpy: null,
    mall_coupon_jpy: null,
    points_used_jpy: null,
    amount_source: 'mall_api',
    currency: 'JPY',
  };
  if (!header.ordered_at) throw new Error(`注文 ${no} の bought_at_jst が無い`);
  const lines = [{
    line_key: '1', listing_code: null, sku_code: sku, qty, cancelled_qty: cancelled ? qty : 0,
    unit_price_jpy: price != null && qty > 0 && price % qty === 0 ? price / qty : null, line_amount_jpy: price, tax_rate: null, amount_source: 'mall_api', source_line_ref: 'order',
  }];
  const su = latestSyncedIso(rows, no, opts);
  const payload = { mall: 'linegift', scope_key: 'main', mall_order_no: no, header: { ...header, source_updated_at: su.iso, transform_version: LINEGIFT_TRANSFORM_VERSION, content_hash: contentHash(header) }, lines };
  return { key: `linegift|main|${no}`, payload, n_lines: 1, source_updated_at: su.iso, no_synced_at: su.missing };
}

/*
 * Qoo10 (raw_qoo10_orders。PK = order_id。apps/warehouse/qoo10-orders.js)。D5b-4。2026-09-19 の実測:
 *   - 表には 2 種類の行が混ざっている。**送るのは API の行 (source_type 'api_%'。order_id = 'api:<注文番号>'、1 行 = 1 注文 = 1 商品。2026-02-19 以降の 1,994 行) だけ**。
 *     🚨 旧データの行 (source_type 'legacy_migration'。17,252 行・〜2026-05-17) は送らない: 鍵がカート番号 (pack_no) に潰れていて注文番号が無い (NE の受注番号 = 10 桁の注文番号に 1 件も当たらない)・
 *       入金日 / 出荷日が無い・2026-02〜05 は API の行と同じ注文が二重にある。既存の f_qoo10_finance も legacy_fields_missing = 0 の行だけを使っている。
 *       = **Qoo10 は D-28 (2025-01-01 以降) を満たせない** (API は 90 日より前を取り直せない)。2026-02-19 より前の Qoo10 の注文は Company DB に入らない
 *   - 注文の鍵 = source_order_key (Qoo10 の注文番号・10 桁) → mall 'qoo10' / scope 'main' / shop_code '6'。NE 店舗 6 の伝票は API の期間で 1,950 のうち 1,911 が注文番号で一致。
 *     27 伝票は NE がカート番号 (9 桁) で起票している → 注文番号では結べない (宿題。カート番号は明細の source_line_ref に残す)
 *   - ordered_at = order_date ('YYYY-MM-DD HH:MM:SS' = JST・時差の表記なし)
 *   - 状態 = shipping_status の原文 → 0020 の対応表 'qoo10' (Awaiting shipping(1) = 入金待ち / Seller confirm(3) = 発送できる / On delivery(4) = 配送中 / Delivered(5) = 配送完了。意味は apps/qoo10-unshipped/service.js)。
 *     🚨 取消は API に出てこない (状態 1〜5 だけを取っている) = 取り消された注文は最後に見えた状態のまま残る。is_cancelled は常に false (raw 側の限界)
 *   - 金額 (税込・円。実測で total = order_price × order_qty − discount が全 1,994 行で成立): 商品代 = order_price × order_qty (値引前) / 送料 = shipping_rate (実測は全件 0) /
 *     店負担の値引 = seller_discount + cart_discount_seller / **モール負担の値引 = discount (メガ割など。settle_price が値引前の 90% = 店の入金は減らない) + cart_discount_qoo10** (既存の f_qoo10_finance と同じ区分) /
 *     顧客が払った額 = カート単位の値引の按分が分からないので null / ポイント = 無い (null)
 *     🚨 金額の列は NOT NULL DEFAULT 0 = 「値が無い」と「0 円」を区別できない → order_price = 0 は商品代を null にして数える (実測 0 件)
 *   - 明細は 1 行: line_key = '1' / listing_code = item_code (Company DB の Qoo10 の出品は listing_code = Qoo10 の商品番号) / sku_code = seller_item_code (販売者商品コード。87% が m_products に当たる) / 税率は raw に無い (null)
 *   - source_updated_at = last_api_snapshot_at か synced_at (取込時刻) → 変化は指紋
 */
export const QOO10_TRANSFORM_VERSION = 'qoo10-orders-1';
export const QOO10_COLUMNS = ['order_id', 'source_type', 'source_order_key', 'pack_no', 'shipping_status', 'item_code', 'seller_item_code', 'order_price', 'order_qty', 'discount', 'total',
  'seller_discount', 'cart_discount_seller', 'cart_discount_qoo10', 'shipping_rate', 'order_date', 'shipping_date', 'last_api_snapshot_at', 'synced_at'];
export const QOO10_DT_RE = /^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})$/;
/** Qoo10 の日時として受けられるか (原値のまま: 文字列・形・実在する日時)。送り手の iterate (範囲の判定の前) と整形が同じ関数を使う */
export function isQoo10Jst(s) { const m = typeof s === 'string' ? QOO10_DT_RE.exec(s) : null; return !!m && isRealDateTime(m[1], m[2], m[3], m[4], m[5], m[6]); }
/** 'YYYY-MM-DD HH:MM:SS' (JST) → ISO8601 +09:00。形か日時が不正なら例外 */
export function qoo10DatetimeToIso(s, label = 'Qoo10 の日時') {
  if (s == null || s === '') return null;
  if (!isQoo10Jst(s)) throw new Error(`${label}が 'YYYY-MM-DD HH:MM:SS' の実在する日時でない (前後の空白も不可): "${s}"`);
  return `${s.slice(0, 10)}T${s.slice(11)}+09:00`;
}
/**
 * Qoo10 の API の行の鍵として受けられるか: source_order_key が原値のまま注文番号の形 (受け口の ORDER_NO_RE と同じ字種。前後の空白・文字列でない値は不可) で、order_id が 'api:<それ>' と一致する。
 * 🚨 送り手の iterate (範囲・台帳の鍵を作る所) と整形が同じ関数を使う: 片方だけ trim すると、台帳の鍵と違う鍵で範囲を判定して追跡中の注文が黙って落ちる (Codex D5b-4 R1 #1)
 */
export const QOO10_ORDER_NO_RE = /^[0-9A-Za-z][0-9A-Za-z._:-]{0,60}$/;
export function isQoo10ApiKey(row) { return !!row && typeof row.source_order_key === 'string' && QOO10_ORDER_NO_RE.test(row.source_order_key) && row.order_id === `api:${row.source_order_key}`; }
export function buildQoo10Order(rows, opts = {}) {
  if (!rows || rows.length !== 1) throw new Error(`Qoo10 の注文は 1 行のはず (${rows ? rows.length : 0} 行。同じ注文番号の行が複数ある)`);
  const r = rows[0];
  if (!/^api_/.test(String(r.source_type ?? ''))) throw new Error(`注文 ${r.source_order_key} は API の行でない (source_type = ${r.source_type})。旧データの行は送らない`);
  if (!isQoo10ApiKey(r)) throw new Error(`注文番号の形が違う (source_order_key = "${r.source_order_key}"・order_id = "${r.order_id}"。前後の空白も不可・order_id は 'api:<注文番号>')`);
  const no = r.source_order_key;
  const stats = opts.stats || null;
  const qty = intOrNull(r.order_qty, `注文 ${no} の order_qty`);
  if (qty == null || qty < 0) throw new Error(`注文 ${no} の order_qty が無い (欠落を 0 にしない)`);
  const unit = yenStrict(r.order_price, `注文 ${no} の order_price`);
  const priced = unit != null && unit > 0;
  if (!priced && stats) stats.zeroPrice = (stats.zeroPrice || 0) + 1;
  const items = priced ? unit * qty : null;
  const listing = nz(r.item_code), sku = nz(r.seller_item_code);
  if (!listing && !sku) throw new Error(`注文 ${no} に item_code も seller_item_code も無い`);
  const pack = intOrNull(r.pack_no, `注文 ${no} の pack_no`);
  const header = {
    source_system: 'mall_api',
    shop_code: '6',
    ordered_at: qoo10DatetimeToIso(r.order_date, `注文 ${no} の order_date`),
    status_source: nz(r.shipping_status),
    is_cancelled: false,
    cancelled_at: null,
    shipped_at_source: qoo10DatetimeToIso(r.shipping_date, `注文 ${no} の shipping_date`),
    total_amount_jpy: null,
    items_amount_jpy: items,
    shipping_fee_jpy: priced ? yenStrict(r.shipping_rate, `注文 ${no} の shipping_rate`) : null,
    shop_coupon_jpy: priced ? sumOrNull(yenStrict(r.seller_discount, `注文 ${no} の seller_discount`), yenStrict(r.cart_discount_seller, `注文 ${no} の cart_discount_seller`)) : null,
    mall_coupon_jpy: priced ? sumOrNull(yenStrict(r.discount, `注文 ${no} の discount`), yenStrict(r.cart_discount_qoo10, `注文 ${no} の cart_discount_qoo10`)) : null,
    points_used_jpy: null,
    amount_source: 'mall_api',
    currency: 'JPY',
  };
  if (!header.ordered_at) throw new Error(`注文 ${no} の order_date が無い`);
  const lines = [{
    line_key: '1', listing_code: listing, sku_code: sku, qty, cancelled_qty: 0, unit_price_jpy: priced ? unit : null, line_amount_jpy: items, tax_rate: null, amount_source: 'mall_api',
    source_line_ref: `pack_no:${pack == null ? '' : pack}`,
  }];
  const snap = nz(r.last_api_snapshot_at);
  const su = latestSyncedIso(snap ? [{ synced_at: snap }] : rows, no, opts);
  const payload = { mall: 'qoo10', scope_key: 'main', mall_order_no: no, header: { ...header, source_updated_at: su.iso, transform_version: QOO10_TRANSFORM_VERSION, content_hash: contentHash(header) }, lines };
  return { key: `qoo10|main|${no}`, payload, n_lines: 1, source_updated_at: su.iso, no_synced_at: su.missing };
}

// ─── Yahoo!ショッピング (D5b-5。2026-09-26 中原さん「Yahoo の注文を Company DB に入れてよい」= D-32 を a に) ───
/**
 * Yahoo (raw_yahoo_orders。1 行 = 注文 × 明細 (line_id)。注文の列は明細行に重複して入っている = 実測で食い違い 0)。個人情報の列はこの表に無い:
 *   - 注文の鍵 = order_id ('b-faith01-…') → mall 'yahoo' / scope 'main' / shop_code '2' (core.ne_shops 2 = 雑貨イズムYahoo!店)
 *   - ordered_at = order_time (ISO8601 '+09:00')。状態 = OrderStatus / PayStatus / ShipStatus の 3 つ → status_source '5-1-3' の形 (Render が core.order_status_map 'yahoo' で正規化)。
 *     OrderStatus 4 (キャンセル) → is_cancelled
 *   - 金額 (税込・円。API の公式説明 = developer.yahoo.co.jp/webapi/shopping/orderInfo.html を 2026-09-26 に確認):
 *       顧客が払う額 = total_price (TotalPrice = 小計 − 利用ポイント + ギフト包装料 + 手数料 − 値引き + 送料 + 調整額 − モールクーポン値引き額 − …)
 *       商品代 = Σ unit_price × quantity。🚨 UnitPrice は「ストアクーポン利用の注文は、クーポン値引き後の金額」= 店のクーポンはもう引かれている
 *         (実測: coupon_discount のある注文で total_price にクーポンが引かれた形は 0 件) → coupon_discount を値引きにもう一度足さない
 *       送料 = ship_charge / 店負担の値引 = discount (注文後にストアクリエイター Pro で入れた値引き) / ポイント = use_point
 *       モール負担の値引 = mall_coupon_discount (TotalMallCouponDiscount。2026-09-26 に取込に足した)。NULL = この列より前の取込で取っていない → null のまま (作らない。
 *         実測で約 1 割の注文は total_price がこれだけ少ない)
 *       手数料 (pay_charge)・ギフト包装料は列が無い (total_price にだけ入る)
 *   - 明細: line_key = line_id / listing_code = item_id (Yahoo の商品コード) / sku_code = sub_code (サブコード。無ければ item_id) / qty = quantity /
 *     cancelled_qty = 取消の注文なら qty (取消の明細は数量 0 で来ることが多い) / unit_price = unit_price / line_amount = unit_price × quantity / tax_rate = item_tax_ratio (8 / 10 → 0.08 / 0.10)
 *   - source_updated_at = synced_at (取込時刻。UTC 'YYYY-MM-DD HH:MM:SS')
 */
export const YAHOO_TRANSFORM_VERSION = 'yahoo-orders-1';
export const YAHOO_COLUMNS = ['order_id', 'line_id', 'order_time', 'order_status', 'pay_status', 'ship_status', 'total_price', 'ship_charge', 'discount', 'use_point', 'mall_coupon_discount',
  'item_id', 'sub_code', 'unit_price', 'quantity', 'item_tax_ratio', 'synced_at'];
/** 注文の列 (明細行に重複して入っている。行によって違えば例外) */
const YAHOO_HEADER_COLUMNS = ['order_time', 'order_status', 'pay_status', 'ship_status', 'total_price', 'ship_charge', 'discount', 'use_point', 'mall_coupon_discount'];
/** order_time は '+09:00' の ISO8601 (実測 99,843 行すべて)。原値のまま・実在する日時だけ受ける (範囲の判定と整形で同じ関数) */
export function isYahooJst(s) { const m = typeof s === 'string' ? LINEGIFT_JST_RE.exec(s) : null; return !!m && isRealDateTime(m[1], m[2], m[3], m[4], m[5], m[6]); }
/** 注文番号 = 原値のまま・前後の空白なし */
export const YAHOO_ORDER_NO_RE = /^[0-9A-Za-z][0-9A-Za-z_-]{0,60}$/;
export function isYahooOrderNo(v) { return typeof v === 'string' && YAHOO_ORDER_NO_RE.test(v); }
const yahooStatusPart = (v, label) => { const t = v == null ? '' : String(v); if (!/^\d$/.test(t)) throw new Error(`${label} が 1 桁の数字でない: "${v}"`); return t; };
const yahooTaxRate = (v) => { if (v == null || v === '') return null; const n = Number(v); return n === 8 ? 0.08 : n === 10 ? 0.1 : null; };
export function buildYahooOrder(rows, opts = {}) {
  if (!rows || !rows.length) throw new Error('行が無い');
  const h0 = rows[0];
  if (!isYahooOrderNo(h0.order_id)) throw new Error(`注文番号の形が違う: "${h0.order_id}" (前後の空白も不可)`);
  const no = h0.order_id;
  for (const r of rows) {
    if (r.order_id !== no) throw new Error(`注文 ${no} に別の注文 ${r.order_id} の行が混ざっている`);
    for (const c of YAHOO_HEADER_COLUMNS) if ((r[c] ?? null) !== (h0[c] ?? null)) throw new Error(`注文 ${no} の ${c} が行によって違う`);
  }
  if (!isYahooJst(h0.order_time)) throw new Error(`注文 ${no} の order_time が JST (+09:00) の ISO8601 でない (形か日時が不正・前後の空白も不可): "${h0.order_time}"`);
  const os = yahooStatusPart(h0.order_status, `注文 ${no} の order_status`);
  const ps = yahooStatusPart(h0.pay_status, `注文 ${no} の pay_status`);
  const ss = yahooStatusPart(h0.ship_status, `注文 ${no} の ship_status`);
  const cancelled = os === '4';
  const seen = new Set();
  const lines = rows.map((r) => {
    const lineNo = intOrNull(r.line_id, `注文 ${no} の line_id`);
    if (lineNo == null || lineNo < 1) throw new Error(`注文 ${no} に line_id の無い行がある`);
    const key = String(lineNo);
    if (seen.has(key)) throw new Error(`注文 ${no} の line_id ${key} が重複している`);
    seen.add(key);
    const qty = intOrNull(r.quantity, `注文 ${no} 明細 ${key} の quantity`);
    if (qty == null || qty < 0) throw new Error(`注文 ${no} 明細 ${key} の quantity が無い (欠落を 0 にしない)`);
    const unit = yenStrict(r.unit_price, `注文 ${no} 明細 ${key} の unit_price`);
    if (unit == null) throw new Error(`注文 ${no} 明細 ${key} の unit_price が無い (欠落を 0 にしない)`);
    const item = nz(r.item_id);
    if (!item) throw new Error(`注文 ${no} 明細 ${key} の item_id が無い`);
    return {
      line_key: key,
      listing_code: item,
      sku_code: nz(r.sub_code) || item,
      qty,
      cancelled_qty: cancelled ? qty : 0,
      unit_price_jpy: unit,
      line_amount_jpy: unit * qty,
      tax_rate: yahooTaxRate(r.item_tax_ratio),
      amount_source: 'mall_api',
      source_line_ref: `line_id:${key}`,
    };
  });
  lines.sort((a, b) => Number(a.line_key) - Number(b.line_key));
  const header = {
    source_system: 'mall_api',
    shop_code: '2',
    ordered_at: h0.order_time,
    status_source: `${os}-${ps}-${ss}`,
    is_cancelled: cancelled,
    cancelled_at: null,
    shipped_at_source: null,   // 発送日 (ship_date) は日付だけ = 時刻が無いので入れない
    total_amount_jpy: yenStrict(h0.total_price, `注文 ${no} の total_price`),
    items_amount_jpy: lines.reduce((a, l) => a + l.line_amount_jpy, 0),
    shipping_fee_jpy: yenStrict(h0.ship_charge, `注文 ${no} の ship_charge`),
    shop_coupon_jpy: yenStrict(h0.discount, `注文 ${no} の discount`),
    mall_coupon_jpy: yenStrict(h0.mall_coupon_discount, `注文 ${no} の mall_coupon_discount`),   // NULL (取っていない) は null のまま
    points_used_jpy: yenStrict(h0.use_point, `注文 ${no} の use_point`),
    amount_source: 'mall_api',
    currency: 'JPY',
  };
  const su = latestSyncedIso(rows, no, opts);
  const payload = { mall: 'yahoo', scope_key: 'main', mall_order_no: no, header: { ...header, source_updated_at: su.iso, transform_version: YAHOO_TRANSFORM_VERSION, content_hash: contentHash(header) }, lines };
  return { key: `yahoo|main|${no}`, payload, n_lines: lines.length, source_updated_at: su.iso, no_synced_at: su.missing };
}

export { canonicalJson };
