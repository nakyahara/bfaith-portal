/**
 * order-finance-checksum.mjs — 注文 (疑似注文) の財務の行の集合の「形の確かめ」と「指紋 (checksum)」。
 * 送り手 (miniPC の push/amazon-finance.mjs) と受け口 (Render の ingest/order-finance.mjs) の **両方がこの 1 つを使う** (実装を 2 つにしない)。
 * 設計 = AI_reference『CompanyDB構想/12_Amazon財務のCompanyDB取込_設計_20260929.md』§4.6 / §3.1 (0043)
 *
 * 行 = { economic_date_jst, seller_sku, line_kind, source, units_ordered, <20 金額列>, unmapped_jpy, <内訳 3 列>, account_fee_amount_jpy, source_lines, <分けられない部品の 4 列 (0047)> }
 *   金額・数量は整数円・JS で安全に扱える整数 (Number.isSafeInteger)。SKU は NFC で変わらない文字列 ('-' = SKU が無い)
 * 指紋 = 行を 鍵 4 つ (計上日, SKU, line_kind, source = 表の主キーの注文の下の部分) の UTF-8 のバイト列で並べ、各行を CONTENT_COLUMNS の順の配列にして JSON.stringify → UTF-8 → sha256 (16 進)。空の集合 = '[]' の sha256
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
/**
 * 分けられない決済の部品の数と額 (0047・D7b-1a。設計 = AI_reference『CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』§3.6 / §3.7)。
 *   集約の後では復元できない (+100 と −100 が打ち消して 0) = 送り手の変換が生の部品を分類するときに数える。net には入らない (数と診断の額)
 *   unclassified_component_count = 0 でない「分けられない」生の部品の数 / unclassified_mapped_jpy = その符号つきの合計 / unclassified_abs_jpy = 部品ごとの絶対値の合計
 *   unmapped_component_count = unmapped_jpy に入った 0 でない生の部品の数
 * 🚨 古い送り手 (0047 の前の版) の行にはこの 4 つの鍵が無い = 「旧い形」。指紋は旧い形の列だけで計算する (送り手の申告と合わせる = Render と miniPC の deploy の間も 400 にしない)。
 *    旧い形の行は 4 列を 0 として保存する (既存の行と同じ)。1 注文の中で形が混ざるのは拒む
 */
export const CLASS_COLUMNS = ['unclassified_component_count', 'unclassified_mapped_jpy', 'unclassified_abs_jpy', 'unmapped_component_count'];
export const LEGACY_INT_COLUMNS = ['units_ordered', ...AMOUNT_COLUMNS, 'unmapped_jpy', ...SUB_COLUMNS, 'account_fee_amount_jpy', 'source_lines'];
export const INT_COLUMNS = [...LEGACY_INT_COLUMNS, ...CLASS_COLUMNS];
export const KEY_COLUMNS = ['economic_date_jst', 'seller_sku', 'line_kind', 'source'];
export const LEGACY_CONTENT_COLUMNS = [...KEY_COLUMNS, ...LEGACY_INT_COLUMNS];   // 旧い形の指紋の列 (0043〜0046 の取り決め・変えない)
export const CONTENT_COLUMNS = [...KEY_COLUMNS, ...INT_COLUMNS];                 // 今の形の指紋の列 (旧い形の後ろに 4 列)
// SKU の行で「分けられない」金額が入る列 (§3.7 の表・D-63 で分けるまで)。SKU の行ではこの 3 列の和 = unclassified_mapped_jpy
export const UNCLASSIFIED_SKU_COLUMNS = ['misc_fee_jpy', 'other_fee_jpy', 'other_amount_jpy'];
export const LINE_KINDS = ['sku', 'storage', 'long_term_storage', 'removal', 'inbound_defect', 'low_inventory', 'subscription', 'easy_ship', 'other_account_fee', 'not_account_fee', 'unknown'];
export const ACCOUNT_FEE_KINDS = ['storage', 'long_term_storage', 'removal', 'inbound_defect', 'low_inventory', 'subscription', 'easy_ship', 'other_account_fee'];
export const SOURCES = ['amazon_settlement_flat_v1', 'amazon_settlement_flat_v2', 'amazon_finances_api', 'amazon_settlement_unified', 'mall_finance_daily_v1'];
export const PSEUDO_PREFIX = '-:';
export const pseudoOrderNo = (date) => `${PSEUDO_PREFIX}${date}`;
export const isPseudoOrderNo = (no) => typeof no === 'string' && no.startsWith('-');

const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER), MIN_SAFE = -MAX_SAFE;
const isDate = (s) => typeof s === 'string' && DATE_RE.test(s) && !Number.isNaN(Date.parse(`${s}T00:00:00Z`)) && new Date(`${s}T00:00:00Z`).toISOString().slice(0, 10) === s;

/**
 * 行の集合の形 = 'v2' (全部の行に CLASS_COLUMNS の 4 つの鍵がある = 0047 の後の送り手) / 'legacy' (どの行にも無い = 古い送り手) / 'empty' (行が 0)。
 * 1 つの行の中や行の間で混ざっていれば throw (整形できない)
 */
export function financeRowsFormat(rows) {
  if (!Array.isArray(rows)) throw new Error('rows must be an array');
  if (!rows.length) return 'empty';
  let withAll = 0;
  rows.forEach((r, i) => {
    const n = r && typeof r === 'object' ? CLASS_COLUMNS.filter((c) => r[c] !== undefined).length : 0;
    if (n !== 0 && n !== CLASS_COLUMNS.length) throw new Error(`rows[${i}]: ${CLASS_COLUMNS.join(' / ')} must be given all together or not at all`);
    if (n) withAll++;
  });
  if (withAll && withAll !== rows.length) throw new Error(`rows mix the new form (${CLASS_COLUMNS.join(' / ')}) and the old form`);
  return withAll ? 'v2' : 'legacy';
}

/**
 * 1 注文の行の集合を確かめる (throw = 整形できない = その注文はまるごと送らない / 受け口は 400 か failed)。
 * 戻り値 = 正規化した行の配列 (CONTENT_COLUMNS の値だけ・無い整数列は 0 = 旧い形の行の 4 列も 0)
 */
export function validateFinanceRows(mallOrderNo, rows) {
  if (typeof mallOrderNo !== 'string' || !mallOrderNo) throw new Error('mall_order_no is required');
  if (!Array.isArray(rows)) throw new Error('rows must be an array');
  const v2 = financeRowsFormat(rows) === 'v2';
  const pseudo = isPseudoOrderNo(mallOrderNo);
  if (pseudo && !(mallOrderNo.startsWith(PSEUDO_PREFIX) && isDate(mallOrderNo.slice(PSEUDO_PREFIX.length)))) throw new Error(`bad pseudo order no ${mallOrderNo} (-:YYYY-MM-DD)`);
  const seen = new Set();
  return rows.map((r, i) => {
    if (!r || typeof r !== 'object' || Array.isArray(r)) throw new Error(`rows[${i}] must be an object`);
    // 通貨は JPY だけ (正規化で捨てる前に確かめる = 捨てた後の SQL の検査には届かない。#1533 Codex R1 High)
    if (r.currency !== undefined && r.currency !== null && r.currency !== 'JPY') throw new Error(`rows[${i}].currency must be JPY (${r.currency})`);
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
    // net を BigInt で正確に足し、途中も結果も安全な整数の範囲か (途中で精度を落として最後だけ範囲に戻るのを見逃さない。#1533 Codex R1)
    let net = 0n;
    for (const c of [...AMOUNT_COLUMNS, 'unmapped_jpy']) {
      net += BigInt(out[c]);
      if (net > MAX_SAFE || net < MIN_SAFE) throw new Error(`rows[${i}]: net is not a safe integer (at ${c})`);
    }
    // 分けられない部品の数と額の形 (0047。どの形でも = 旧い形は全部 0 で満たす。表の CHECK ck_order_finance_daily_unclassified と同じ)
    if (out.unclassified_component_count < 0 || out.unmapped_component_count < 0 || out.unclassified_abs_jpy < 0) throw new Error(`rows[${i}]: unclassified / unmapped counts and unclassified_abs_jpy must be >= 0`);
    if (Math.abs(out.unclassified_mapped_jpy) > out.unclassified_abs_jpy) throw new Error(`rows[${i}]: |unclassified_mapped_jpy| must be <= unclassified_abs_jpy`);
    if ((out.unclassified_component_count === 0) !== (out.unclassified_abs_jpy === 0)) throw new Error(`rows[${i}]: unclassified_component_count is 0 iff unclassified_abs_jpy is 0`);
    if (v2) {
      // 今の形だけ (旧い形の行は数を持たない = 0 のまま金額がある): 0 でない金額があれば部品は 1 つ以上
      if (out.unmapped_jpy !== 0 && out.unmapped_component_count === 0) throw new Error(`rows[${i}]: unmapped_jpy is not 0 but unmapped_component_count is 0`);
      if (out.line_kind === 'sku') {
        // SKU の行の「分けられない」列 (misc_fee / other_fee / other_amount) は分けられない部品だけから成る = 和が一致し、絶対値の合計はその列の絶対値の和以上
        const sum = UNCLASSIFIED_SKU_COLUMNS.reduce((s, c) => s + out[c], 0), absSum = UNCLASSIFIED_SKU_COLUMNS.reduce((s, c) => s + Math.abs(out[c]), 0);
        if (out.unclassified_mapped_jpy !== sum) throw new Error(`rows[${i}]: on a sku row unclassified_mapped_jpy must equal ${UNCLASSIFIED_SKU_COLUMNS.join(' + ')} (${out.unclassified_mapped_jpy} <> ${sum})`);
        if (out.unclassified_abs_jpy < absSum) throw new Error(`rows[${i}]: on a sku row unclassified_abs_jpy must be >= |${UNCLASSIFIED_SKU_COLUMNS.join('| + |')}| (${out.unclassified_abs_jpy} < ${absSum})`);
      }
    }
    const k = KEY_COLUMNS.map((c) => out[c]).join('\u0000');
    if (seen.has(k)) throw new Error(`rows[${i}]: duplicate row key (${KEY_COLUMNS.map((c) => out[c]).join(' / ')})`);
    seen.add(k);
    return out;
  });
}

const cmpBytes = (a, b) => Buffer.compare(Buffer.from(a, 'utf8'), Buffer.from(b, 'utf8'));

/**
 * 集合の指紋 (validateFinanceRows の後の行を渡す)。legacy = true なら旧い形の列 (LEGACY_CONTENT_COLUMNS) だけで計算する
 *   (受け口が古い送り手の行を受けるとき = financeRowsFormat(元の行) === 'legacy'。空の集合はどちらでも同じ)
 */
export function orderFinanceChecksum(rows, { legacy = false } = {}) {
  const cols = legacy ? LEGACY_CONTENT_COLUMNS : CONTENT_COLUMNS;
  const sorted = [...rows].sort((x, y) => {
    for (const c of KEY_COLUMNS) { const d = cmpBytes(String(x[c]), String(y[c])); if (d) return d; }
    return 0;
  });
  const json = JSON.stringify(sorted.map((r) => cols.map((c) => r[c])));
  return crypto.createHash('sha256').update(Buffer.from(json, 'utf8')).digest('hex');
}
