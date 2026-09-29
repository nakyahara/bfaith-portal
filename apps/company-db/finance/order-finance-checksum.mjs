/**
 * order-finance-checksum.mjs — 注文 (疑似注文) の財務の行の集合の「形の確かめ」と「指紋 (checksum)」。
 * 送り手 (miniPC の push/amazon-finance.mjs) と受け口 (Render の ingest/order-finance.mjs) の **両方がこの 1 つを使う** (実装を 2 つにしない)。
 * 設計 = AI_reference『CompanyDB構想/12_Amazon財務のCompanyDB取込_設計_20260929.md』§4.6 / §3.1 (0043)
 *
 * 行 = { economic_date_jst, seller_sku, line_kind, source, units_ordered, <20 金額列>, unmapped_jpy, <内訳 3 列>, account_fee_amount_jpy, source_lines }
 *   金額・数量は整数円・JS で安全に扱える整数 (Number.isSafeInteger)。SKU は NFC で変わらない文字列 ('-' = SKU が無い)
 * 指紋 = 行を (計上日, SKU, line_kind, source) の UTF-8 のバイト列で並べ、各行を CONTENT_COLUMNS の順の配列にして JSON.stringify → UTF-8 → sha256 (16 進)。空の集合 = '[]' の sha256
 *   世代・時刻・content_hash・transform_version は入れない (transform_version は受け口が別に比べる)
 */
import crypto from 'node:crypto';

export const AMOUNT_COLUMNS = [
  'sales_principal_jpy', 'sales_shipping_jpy', 'sales_giftwrap_jpy', 'sales_tax_jpy',
  'commission_jpy', 'fba_fulfillment_jpy', 'fba_storage_jpy', 'closing_fee_jpy', 'shipping_chargeback_jpy', 'giftwrap_chargeback_jpy',
  'promotion_jpy', 'points_jpy', 'warehouse_damage_jpy', 'warehouse_lost_jpy', 'safe_t_jpy', 'refund_principal_jpy', 'reversal_reimbursement_jpy',
  'misc_fee_jpy', 'other_fee_jpy', 'other_amount_jpy',
];   // 20 列 = net に入る (0043 の ck_order_finance_daily_net)
export const SUB_COLUMNS = ['promotion_tax_jpy', 'refund_principal_customer_jpy', 'refund_principal_atoz_jpy'];   // 内訳 (net に入らない)
export const INT_COLUMNS = ['units_ordered', ...AMOUNT_COLUMNS, 'unmapped_jpy', ...SUB_COLUMNS, 'account_fee_amount_jpy', 'source_lines'];
export const KEY_COLUMNS = ['economic_date_jst', 'seller_sku', 'line_kind', 'source'];
export const CONTENT_COLUMNS = [...KEY_COLUMNS, ...INT_COLUMNS];
export const LINE_KINDS = ['sku', 'storage', 'long_term_storage', 'removal', 'inbound_defect', 'low_inventory', 'subscription', 'easy_ship', 'other_account_fee', 'not_account_fee', 'unknown'];
export const ACCOUNT_FEE_KINDS = ['storage', 'long_term_storage', 'removal', 'inbound_defect', 'low_inventory', 'subscription', 'easy_ship', 'other_account_fee'];
export const SOURCES = ['amazon_settlement_flat_v1', 'amazon_settlement_flat_v2', 'amazon_finances_api', 'amazon_settlement_unified', 'mall_finance_daily_v1'];
export const PSEUDO_PREFIX = '-:';
export const pseudoOrderNo = (date) => `${PSEUDO_PREFIX}${date}`;
export const isPseudoOrderNo = (no) => typeof no === 'string' && no.startsWith('-');

const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
const isDate = (s) => typeof s === 'string' && DATE_RE.test(s) && !Number.isNaN(Date.parse(`${s}T00:00:00Z`)) && new Date(`${s}T00:00:00Z`).toISOString().slice(0, 10) === s;

/**
 * 1 注文の行の集合を確かめる (throw = 整形できない = その注文はまるごと送らない / 受け口は 400 か failed)。
 * 戻り値 = 正規化した行の配列 (CONTENT_COLUMNS の値だけ・無い整数列は 0)
 */
export function validateFinanceRows(mallOrderNo, rows) {
  if (typeof mallOrderNo !== 'string' || !mallOrderNo) throw new Error('mall_order_no is required');
  if (!Array.isArray(rows)) throw new Error('rows must be an array');
  const pseudo = isPseudoOrderNo(mallOrderNo);
  if (pseudo && !(mallOrderNo.startsWith(PSEUDO_PREFIX) && isDate(mallOrderNo.slice(PSEUDO_PREFIX.length)))) throw new Error(`bad pseudo order no ${mallOrderNo} (-:YYYY-MM-DD)`);
  const seen = new Set();
  return rows.map((r, i) => {
    if (!r || typeof r !== 'object' || Array.isArray(r)) throw new Error(`rows[${i}] must be an object`);
    if (!isDate(r.economic_date_jst)) throw new Error(`rows[${i}].economic_date_jst is not a date`);
    if (pseudo && mallOrderNo !== pseudoOrderNo(r.economic_date_jst)) throw new Error(`rows[${i}]: pseudo order ${mallOrderNo} holds a row of ${r.economic_date_jst}`);
    if (typeof r.seller_sku !== 'string' || r.seller_sku === '' || r.seller_sku.length > 200) throw new Error(`rows[${i}].seller_sku must be a non-empty string`);
    if (r.seller_sku !== r.seller_sku.normalize('NFC')) throw new Error(`rows[${i}].seller_sku is not NFC`);   // NFC で同じになる別の表記を作らない
    if (!LINE_KINDS.includes(r.line_kind)) throw new Error(`rows[${i}].line_kind is not known: ${r.line_kind}`);
    if ((r.seller_sku === '-') !== (r.line_kind !== 'sku')) throw new Error(`rows[${i}]: seller_sku '-' iff line_kind is a fee kind`);
    if (!SOURCES.includes(r.source)) throw new Error(`rows[${i}].source is not known: ${r.source}`);
    const out = { economic_date_jst: r.economic_date_jst, seller_sku: r.seller_sku, line_kind: r.line_kind, source: r.source };
    for (const c of INT_COLUMNS) {
      const v = r[c] ?? 0;
      if (!Number.isSafeInteger(v)) throw new Error(`rows[${i}].${c} must be a safe integer (${v})`);
      out[c] = v;
    }
    if (out.source_lines <= 0) throw new Error(`rows[${i}].source_lines must be > 0`);
    if (out.line_kind === 'sku' && out.account_fee_amount_jpy !== 0) throw new Error(`rows[${i}]: account_fee_amount_jpy must be 0 on a sku row`);
    const net = [...AMOUNT_COLUMNS, 'unmapped_jpy'].reduce((a, c) => a + out[c], 0);
    if (!Number.isSafeInteger(net)) throw new Error(`rows[${i}]: net is not a safe integer`);
    const k = KEY_COLUMNS.map((c) => out[c]).join('\u0000');
    if (seen.has(k)) throw new Error(`rows[${i}]: duplicate row key (${KEY_COLUMNS.map((c) => out[c]).join(' / ')})`);
    seen.add(k);
    return out;
  });
}

const cmpBytes = (a, b) => Buffer.compare(Buffer.from(a, 'utf8'), Buffer.from(b, 'utf8'));

/** 集合の指紋 (validateFinanceRows の後の行を渡す) */
export function orderFinanceChecksum(rows) {
  const sorted = [...rows].sort((x, y) => {
    for (const c of KEY_COLUMNS) { const d = cmpBytes(String(x[c]), String(y[c])); if (d) return d; }
    return 0;
  });
  const json = JSON.stringify(sorted.map((r) => CONTENT_COLUMNS.map((c) => r[c])));
  return crypto.createHash('sha256').update(Buffer.from(json, 'utf8')).digest('hex');
}
