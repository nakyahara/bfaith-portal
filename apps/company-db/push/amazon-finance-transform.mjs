/**
 * amazon-finance-transform.mjs — Amazon の決済の行 (warehouse.db の raw_amazon_settlement_lines) を、1 注文 (疑似注文) の財務の行の集合にする (F2b-2)。
 * DB には触らない (読むのは送り手 amazon-finance.mjs)。設計 = AI_reference『CompanyDB構想/12_Amazon財務のCompanyDB取込_設計_20260929.md』§3.1 / §4.2
 *
 * 🚨 二重の実装: ここは SQLite の日次の財務の build (sql/amazon/build_f_amazon_finance_sku_daily_v1.sql) と
 *   月の手数料の build (apps/warehouse/rebuild-amazon-account-fees.js) の別の実装。式の文字列は比べない =
 *   **同じ決済の行を両方に通して全列一致** の試験 (scripts/test-company-db-amazon-finance.mjs) で守る。build の CASE を変えたらここも変えて試験を流す
 *
 *   重複除去 = build と同じ出現順つき: (決済, business_line_key, 文書) の中の行番号の DENSE_RANK = occ → (決済, business_line_key, occ) ごとに
 *     層 (sp_api_v1 / v2 = 1・manual_csv = 2・ほか 3) → ingested_at の新しい順 → 文書 の 1 行。
 *     business_line_key は注文番号・posted_date・SKU・取引の種類・金額の全部を含む = 注文 (疑似注文 = 注文番号の無いその計上日の行) で絞ってから除いても build と同じ
 *   行 = (計上日, SKU, line_kind, source) ごと。SKU のある行 = 'sku' / SKU の無い行 = 月の手数料の分け方 (amazon-account-fee-rules.js) / 手数料に入れない = not_account_fee / 分けられない = unknown。
 *     SKU のある行でも BuyerRecharge と預かり金 2 種は build が日次の財務から除く → SKU を '-' にして not_account_fee (金額は net に残す = 決済の行の金額の全部)
 *   金額 = 決済の符号のまま整数円 (micro を BigInt で読み、100 万で割り切れなければ整形できない)。日次の view (0043) が費用の列を反転する
 *   どの列にも入らない金額 = unmapped_jpy (net に入る・日次の view には入らない = build が拾わないのと同じ)。「元の行の拾われない列に 0 でない金額があったか」を数える (打ち消して 0 でも見逃さない)
 */
import crypto from 'node:crypto';
import { classifyAccountFee, NOT_ACCOUNT_FEE } from '../../warehouse/amazon-account-fee-rules.js';
import { validateFinanceRows, orderFinanceChecksum, pseudoOrderNo, AMOUNT_COLUMNS, SUB_COLUMNS, CONTENT_COLUMNS } from '../finance/order-finance-checksum.mjs';

export const AMAZON_FINANCE_TRANSFORM_VERSION = 'amazon_finance_v1';
export const FINANCE_SOURCE = 'amazon_settlement_unified';
export const FINANCE_MALL = 'amazon';
export const FINANCE_SCOPE = 'jp';

// 決済の行から読む列 (送り手の SELECT と試験がこの一覧を使う)
export const RAW_COLUMNS = ['id', 'source_settlement_id', 'business_line_key', 'source_document_id', 'source_line_no', 'source_layer', 'ingested_at', 'posted_date_utc',
  'economic_date', 'amazon_order_id', 'seller_sku_normalized', 'transaction_type', 'currency', 'quantity_purchased',
  'price_type', 'price_amount_micro', 'item_related_fee_type', 'item_related_fee_amount_micro', 'promotion_type', 'promotion_amount_micro',
  'shipment_fee_amount_micro', 'order_fee_amount_micro', 'misc_fee_amount_micro', 'other_fee_amount_micro', 'direct_payment_amount_micro', 'other_amount_micro'];

// ── build SQL の決め (sql/amazon/build_f_amazon_finance_sku_daily_v1.sql) ──
export const SKU_EXCLUDED_TX = ['BuyerRecharge', 'Previous Reserve Amount Balance', 'Current Reserve Amount'];   // silver の NOT IN
const TAX_PRICE_TYPES = ['Tax', 'ShippingTax', 'GiftWrapTax'];
const REFUND_CUSTOMER_TX = ['Refund', 'Refund_Retrocharge', 'Order_Retrocharge'];
const REFUND_OTHER_TX = [...REFUND_CUSTOMER_TX, 'Chargeback Refund', 'A-to-z Guarantee Refund'];
const REFUND_OTHER_PRICE = ['Shipping', 'GiftWrap', 'RestockingFee'];
const STORAGE_TX = ['Storage Fee', 'StorageRenewalBilling', 'Storage Fee - Reversal', 'Storage Fee - Correction'];
const REVERSAL_TX = ['REVERSAL_REIMBURSEMENT', 'Goodwill Concession', 'Fee Adjustment', 'Overpaid Fees Adjustment'];
const FEE_COLUMN = {
  Commission: 'commission_jpy', RefundCommission: 'commission_jpy', FBAPerUnitFulfillmentFee: 'fba_fulfillment_jpy',
  ShippingChargeback: 'shipping_chargeback_jpy', GiftwrapChargeback: 'giftwrap_chargeback_jpy',
  PointsGranted: 'points_jpy', PointsReturned: 'points_jpy', MFNPostageFee: 'other_fee_jpy', MFNPostageFeeTax: 'other_fee_jpy',
};

/** price_amount の行き先 { col, sub } (build の CASE の順)。当たらなければ null */
export function priceColumn(tx, pt) {
  if (pt == null) return null;
  if (tx === 'Order' && pt === 'Principal') return { col: 'sales_principal_jpy' };
  if (tx === 'Order' && pt === 'Shipping') return { col: 'sales_shipping_jpy' };
  if (tx === 'Order' && pt === 'GiftWrap') return { col: 'sales_giftwrap_jpy' };
  if (TAX_PRICE_TYPES.includes(pt)) return { col: 'sales_tax_jpy' };
  if (REFUND_CUSTOMER_TX.includes(tx) && pt === 'Principal') return { col: 'refund_principal_jpy', sub: 'refund_principal_customer_jpy' };
  if (tx === 'A-to-z Guarantee Refund' && pt === 'Principal') return { col: 'refund_principal_jpy', sub: 'refund_principal_atoz_jpy' };
  if (REFUND_OTHER_TX.includes(tx) && REFUND_OTHER_PRICE.includes(pt)) return { col: 'refund_principal_jpy' };
  if (tx === 'Chargeback Refund' && pt === 'Principal') return { col: 'refund_principal_jpy' };
  return null;
}
/** other_amount の行き先 (SKU のある行 = build の補てん・保管料の CASE / SKU の無い行 = 保管料だけ分ける) */
export function otherAmountColumn(tx, skuRow) {
  if (STORAGE_TX.includes(tx)) return 'fba_storage_jpy';
  if (!skuRow) return 'other_amount_jpy';
  if (tx === 'WAREHOUSE_DAMAGE' || tx === 'WAREHOUSE_DAMAGE_EXCEPTION') return 'warehouse_damage_jpy';
  if (tx === 'WAREHOUSE_LOST') return 'warehouse_lost_jpy';
  if (tx === 'SAFE-T Reimbursement') return 'safe_t_jpy';
  if (REVERSAL_TX.includes(tx)) return 'reversal_reimbursement_jpy';
  return 'other_amount_jpy';
}

const MICRO = 1000000n;
const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER);
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
export const isRealDate = (s) => typeof s === 'string' && DATE_RE.test(s) && !Number.isNaN(Date.parse(`${s}T00:00:00Z`)) && new Date(`${s}T00:00:00Z`).toISOString().slice(0, 10) === s;
const toBig = (v, what) => {
  if (v == null) return null;
  if (typeof v === 'bigint') return v;
  if (typeof v === 'number' && Number.isSafeInteger(v)) return BigInt(v);
  throw new Error(`${what} が整数でない (${v})`);
};
/** micro → 円 (BigInt)。割り切れなければ整形できない */
const yenOf = (v, what) => {
  const b = toBig(v, what);
  if (b == null) return null;
  if (b % MICRO !== 0n) throw new Error(`${what} が円未満の端数を持つ (${b} micro)`);
  return b / MICRO;
};
const layerRank = (l) => (l === 'sp_api_v1' || l === 'sp_api_v2' ? 1 : l === 'manual_csv' ? 2 : 3);
/** SQLite の既定の並び (NULL が先・TEXT は UTF-8 のバイト順・数は数の順) */
const sqliteCmp = (a, b) => {
  if (a == null || b == null) return a == null ? (b == null ? 0 : -1) : 1;
  if (typeof a === 'string' && typeof b === 'string') return Buffer.compare(Buffer.from(a, 'utf8'), Buffer.from(b, 'utf8'));
  const x = typeof a === 'bigint' ? a : BigInt(a), y = typeof b === 'bigint' ? b : BigInt(b);
  return x < y ? -1 : x > y ? 1 : 0;
};

/** build と同じ出現順つきの重複除去 (注文 (疑似注文) の全部の行を渡す)。戻り値 = 残す行 (元の順) */
export function dedupSettlementRows(rows) {
  const byDoc = new Map();   // (決済, 鍵, 文書) → 行番号の一覧
  const k3 = (r) => `${r.source_settlement_id}\u0000${r.business_line_key}\u0000${r.source_document_id}`;
  for (const r of rows) { const k = k3(r); if (!byDoc.has(k)) byDoc.set(k, []); byDoc.get(k).push(r.source_line_no ?? null); }
  const rankOf = new Map();   // DENSE_RANK ORDER BY source_line_no (同じ行番号は同じ順位)
  for (const [k, nos] of byDoc) {
    const distinct = [];
    for (const n of [...nos].sort(sqliteCmp)) if (!distinct.length || sqliteCmp(distinct[distinct.length - 1], n) !== 0) distinct.push(n);
    rankOf.set(k, distinct);
  }
  const occOf = (r) => { const d = rankOf.get(k3(r)); return d.findIndex((n) => sqliteCmp(n, r.source_line_no ?? null) === 0) + 1; };
  const best = new Map();   // (決済, 鍵, occ) → 選ぶ行
  const better = (a, b) => {   // a が b より先か (層 → ingested_at の新しい順 → 文書。同順位は id の小さい順 = 決まった 1 行)
    const la = layerRank(a.source_layer), lb = layerRank(b.source_layer);
    if (la !== lb) return la < lb;
    const ia = sqliteCmp(a.ingested_at, b.ingested_at);
    if (ia !== 0) return ia > 0;
    const da = sqliteCmp(a.source_document_id, b.source_document_id);
    if (da !== 0) return da < 0;
    return sqliteCmp(a.id, b.id) < 0;
  };
  for (const r of rows) {
    const k = `${r.source_settlement_id}\u0000${r.business_line_key}\u0000${occOf(r)}`;
    const cur = best.get(k);
    if (!cur || better(r, cur)) best.set(k, r);
  }
  const keep = new Set(best.values());
  return rows.filter((r) => keep.has(r));
}

/** 注文番号 (疑似注文) = 決済の行の注文番号 / 無ければ '-:計上日' */
export const orderNoOf = (r) => (r.amazon_order_id == null || r.amazon_order_id === '' ? pseudoOrderNo(r.economic_date) : r.amazon_order_id);
/** SKU のある行か (build の silver = NOT NULL AND TRIM <> '' / 手数料の build = NULL か ''。空白だけの SKU はどちらにも入らない = 整形できない) */
export function skuKindOf(sku) {
  if (sku == null || sku === '') return 'none';
  if (sku.replace(/^ +| +$/g, '') === '') return 'blank';   // SQLite の TRIM は空白 (' ') だけを落とす
  return 'sku';
}
/** SKU の無い行の line_kind */
export function feeKindOf(tx) {
  const k = classifyAccountFee(tx);
  if (k) return k;
  return NOT_ACCOUNT_FEE.includes(tx) ? 'not_account_fee' : 'unknown';
}

const toIso = (s) => {
  if (typeof s !== 'string') return null;
  const t = Date.parse(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(s) ? `${s.replace(' ', 'T')}Z` : s);
  return Number.isNaN(t) ? null : new Date(t).toISOString();
};
export const lineHash = (line) => crypto.createHash('sha256').update(JSON.stringify(CONTENT_COLUMNS.map((c) => line[c]))).digest('hex').slice(0, 32);

/**
 * 1 注文 (疑似注文) の決済の行 (重複除去の前・全部) → { lines, stats }。throw = 整形できない (その注文はまるごと送らない)
 * lines = validateFinanceRows を通した行 + source_updated_at / content_hash。stats = { rawRows, dedupRows, unmapped: { rows, columns: {列: 件数}, exampleIds } }
 */
export function aggregateOrderFinance(orderNo, rawRows) {
  const rows = dedupSettlementRows(rawRows);
  const acc = new Map();   // 行の鍵 → { key 列, BigInt の列, source_lines, updated }
  const unmapped = { rows: 0, columns: {}, exampleIds: [] };
  for (const r of rows) {
    const rowNo = orderNoOf(r);
    if (rowNo !== orderNo) throw new Error(`決済の行 ${r.id} の注文番号 ${rowNo} が集合 ${orderNo} と違う`);
    if (!isRealDate(r.economic_date)) throw new Error(`決済の行 ${r.id} の計上日が読めない (${r.economic_date})`);
    if ((r.currency ?? 'JPY') !== 'JPY') throw new Error(`決済の行 ${r.id} の通貨が JPY でない (${r.currency})`);
    const tx = r.transaction_type;
    const sk = skuKindOf(r.seller_sku_normalized);
    if (sk === 'blank') throw new Error(`決済の行 ${r.id} の SKU が空白だけ (日次の財務にも月の手数料にも入らない)`);
    const skuRow = sk === 'sku' && !SKU_EXCLUDED_TX.includes(tx);
    const seller = skuRow ? r.seller_sku_normalized : '-';
    const kind = skuRow ? 'sku' : sk === 'sku' ? 'not_account_fee' : feeKindOf(tx);
    const k = `${r.economic_date}\u0000${seller}\u0000${kind}`;
    if (!acc.has(k)) {
      const a = { economic_date_jst: r.economic_date, seller_sku: seller, line_kind: kind, source: FINANCE_SOURCE, units_ordered: 0n, unmapped_jpy: 0n, account_fee_amount_jpy: 0n, source_lines: 0, updated: null };
      for (const c of [...AMOUNT_COLUMNS, ...SUB_COLUMNS]) a[c] = 0n;
      acc.set(k, a);
    }
    const a = acc.get(k);
    a.source_lines++;
    const upd = toIso(r.ingested_at) ?? toIso(r.posted_date_utc);
    if (upd && (!a.updated || upd > a.updated)) a.updated = upd;
    const lost = [];   // この行で拾われない列 (0 でない金額)
    const put = (col, v) => { if (v == null) return; a[col] += v; };
    // 数量 (Order の数量だけの行)
    if (tx === 'Order' && r.price_type == null && r.item_related_fee_type == null && r.promotion_type == null) {
      const q = toBig(r.quantity_purchased, `決済の行 ${r.id} の数量`);
      if (q != null) a.units_ordered += q;
    }
    const price = yenOf(r.price_amount_micro, `決済の行 ${r.id} の price`);
    if (price != null) {
      const m = priceColumn(tx, r.price_type);
      if (m) { put(m.col, price); if (m.sub) put(m.sub, price); }
      else if (skuRow) { put('unmapped_jpy', price); if (price !== 0n) lost.push('price'); }
      else put('other_amount_jpy', price);
    }
    const fee = yenOf(r.item_related_fee_amount_micro, `決済の行 ${r.id} の item_related_fee`);
    if (fee != null) {
      const col = r.item_related_fee_type != null && Object.hasOwn(FEE_COLUMN, r.item_related_fee_type) ? FEE_COLUMN[r.item_related_fee_type] : null;
      if (col) put(col, fee);
      else if (skuRow) { put('unmapped_jpy', fee); if (fee !== 0n) lost.push(`item_related_fee:${r.item_related_fee_type}`); }
      else put('other_fee_jpy', fee);
    }
    const promo = yenOf(r.promotion_amount_micro, `決済の行 ${r.id} の promotion`);
    if (promo != null) { put('promotion_jpy', promo); if (r.promotion_type === 'TaxDiscount') put('promotion_tax_jpy', promo); }
    const other = yenOf(r.other_amount_micro, `決済の行 ${r.id} の other_amount`);
    if (other != null) put(otherAmountColumn(tx, skuRow), other);
    put('misc_fee_jpy', yenOf(r.misc_fee_amount_micro, `決済の行 ${r.id} の misc_fee`));
    put('other_fee_jpy', yenOf(r.other_fee_amount_micro, `決済の行 ${r.id} の other_fee`));
    for (const [c, label] of [['shipment_fee_amount_micro', 'shipment_fee'], ['order_fee_amount_micro', 'order_fee'], ['direct_payment_amount_micro', 'direct_payment']]) {
      const v = yenOf(r[c], `決済の行 ${r.id} の ${label}`);
      if (v != null) { put('unmapped_jpy', v); if (v !== 0n) lost.push(label); }
    }
    // 月の手数料の材料 (SKU の無い行だけ・手数料の build と同じ 2 列)
    if (kind !== 'sku') a.account_fee_amount_jpy += (other ?? 0n) + (fee ?? 0n);
    if (lost.length) {
      unmapped.rows++;
      for (const c of lost) unmapped.columns[c] = (unmapped.columns[c] || 0) + 1;
      if (unmapped.exampleIds.length < 5) unmapped.exampleIds.push(r.id);
    }
  }
  const out = [];
  for (const a of acc.values()) {
    const line = { economic_date_jst: a.economic_date_jst, seller_sku: a.seller_sku, line_kind: a.line_kind, source: a.source };
    for (const c of ['units_ordered', ...AMOUNT_COLUMNS, 'unmapped_jpy', ...SUB_COLUMNS, 'account_fee_amount_jpy']) {
      const v = a[c];
      if (v > MAX_SAFE || v < -MAX_SAFE) throw new Error(`${a.economic_date_jst} ${a.seller_sku} ${a.line_kind} の ${c} が安全な整数の範囲を超える (${v})`);
      line[c] = Number(v);
    }
    line.source_lines = a.source_lines;
    line._updated = a.updated;
    out.push(line);
  }
  const content = validateFinanceRows(orderNo, out);   // 受け口と同じ検査 (net の途中の範囲・SKU の NFC・鍵の重複・種類)
  const lines = content.map((c, i) => ({ ...c, source_updated_at: out[i]._updated ?? '1970-01-01T00:00:00.000Z', content_hash: lineHash(c) }));
  lines.sort((x, y) => { for (const c of ['economic_date_jst', 'seller_sku', 'line_kind', 'source']) { const d = Buffer.compare(Buffer.from(x[c], 'utf8'), Buffer.from(y[c], 'utf8')); if (d) return d; } return 0; });
  return { lines, stats: { rawRows: rawRows.length, dedupRows: rows.length, unmapped } };
}

/** 送る形 (受け口 ingest/order-finance.mjs の 1 要素)。lines = aggregateOrderFinance の lines (空 = 注文の行が全部消えた) */
export function financePayload(orderNo, lines, transformVersion = AMAZON_FINANCE_TRANSFORM_VERSION) {
  const setChecksum = orderFinanceChecksum(lines);
  return { mall: FINANCE_MALL, scope_key: FINANCE_SCOPE, mall_order_no: orderNo, header: { transform_version: transformVersion, set_checksum: setChecksum, content_hash: setChecksum }, lines };
}
