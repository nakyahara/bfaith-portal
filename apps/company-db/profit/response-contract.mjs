/**
 * response-contract.mjs — Amazon の利益の受け口 (/apps/company-db/sync/amazon-profit/daily・/totals) の **最終の JSON の契約**
 *   (D-60 v3.4 の PR 2b・設計 = AI_reference『CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』§3.10「応答の契約」「長い期間の応答の列ごとの規則」)
 *
 * 🚨 DB の実装より先に固定する (Codex R-D60-v3-4 M5)。今の受け口は 503 のまま (この部品を使う所はまだ無い・開けるのは §3.10 の PR 6 だけ)
 * 文書 = docs/contracts/company_db_amazon_profit_response.contract.md / fixture = scripts/fixtures/amazon-profit-response/ /
 * 試験 = scripts/test-company-db-profit-response-contract.mjs
 *
 * 形 (v3.4 = 旧 #1559 の「日・月・期間の中の月の小計・期間の合計」の行は返さない):
 *   /totals 200 = { ok, contract, kind: 'totals', mall, scope, from, to, master_basis, calculation_version, calculated_at, master_as_of, total: {期間の全体の 1 行}, months: [...] }
 *   /daily  200 = { ok, contract, kind: 'daily',  mall, scope, from, to, master_basis, calculation_version, calculated_at, master_as_of, rows: [日 × 出品], months: [...] }
 *   503         = { ok: false, code: PROFIT_503_CODES のどれか, error: 文, reason?: 'METRICS_…' などの大文字のコード }
 *
 * 値の書き方 (JSON):
 *   bigint (金額・数・ID) = 10 進の文字列 (JS の Number に入れない) / bigint[] = その文字列の配列
 *   numeric の金額 = 小数 2 桁の文字列 (丸めは PostgreSQL の round(numeric, 2) = 0 から遠い方へ・half away from zero。"-0.00" は無い)
 *   numeric の返品数 (units_*_unrounded) = 小数 0 か 6 桁の文字列 / integer = 数 / date = "YYYY-MM-DD" / timestamptz = UTC の ISO (ミリ秒・Z)
 *   text = 文字 / text[] = 文字の配列 / jsonb = object / boolean
 *   calculated_at = 要求の最初の文の statement_timestamp() の 1 つの値 = 上・rows の全部・months の全部で同じ (R5 Low)。master_as_of = calculated_at (D-64)
 *   raw の列 (丸める前の値・名前が _raw で終わる) と旧 totals の行の種類 (row_kind・month_start ほか) は出さない
 */

export const CONTRACT_VERSION = 'amazon_profit_response_v1';

/** 503 の code の一覧 (これ以外は使わない)。文言は人が読む用・機械は code を読む */
export const PROFIT_503_CODES = Object.freeze([
  'PROFIT_ROUTE_DISABLED',        // 封じ込め (今の router・#1570)。PR 6 で外す
  'PROFIT_NOT_CALIBRATED',        // 承認済みの校正の記録が無い・draft・revoked・fingerprint / 材料が今と違う
  'PROFIT_BUSY',                  // 共通の lock (company_db_heavy) が取れない
  'PROFIT_RESOURCE',              // 資源の関門 (メモリ・一時ファイル・負荷の要因・process の数の材料) を満たさない
  'PROFIT_METRICS_UNAVAILABLE',   // Render の metrics が読めない・古い・形が違う (reason に METRICS_… のコード)
  'PROFIT_PARTIAL_FAILED',        // 1 か月でも計算に失敗した (部分の値は返さない)
  'PROFIT_VERSION_MISMATCH',      // calculation_version / master_basis が月で違う
]);
/** 応答に付ける header (200 も 503 も)。AI・画面は正式な値を自分で保存しない */
export const REQUIRED_HEADERS = Object.freeze({ 'cache-control': 'no-store' });

/** 理由の決まった順 (0049 / 0050 の array_remove(array[…]) の順)。totals は ad_unresolved を入れない (合計を止めない) */
export const REASON_ORDER = Object.freeze([
  'finance_incomplete', 'finance_unclassified', 'refund_units_unknown', 'refund_units_partial_month',
  'listing_unresolved', 'composition_missing', 'cost_missing', 'ad_not_collected', 'ad_missing', 'ad_legacy_unverified', 'ad_unresolved',
]);
export const TOTALS_REASONS = Object.freeze(REASON_ORDER.filter((r) => r !== 'ad_unresolved'));
/** 0 と仮定の理由 (日の行) = REASON_ORDER から refund_units_partial_month を除いた順 */
export const ASSUMED_ZERO_REASONS = Object.freeze(REASON_ORDER.filter((r) => r !== 'refund_units_partial_month'));
/** 日の行の master_notes の順・totals の master_note_counts のキー */
export const MASTER_NOTE_KEYS = Object.freeze(['pre_audit_unverifiable', 'current_after_recorded_change', 'listing_changed_since_received']);
/** 状態の順位 (後ろほど弱い)。期間の値 = 月の中で一番弱いもの */
export const AD_STATUS_RANK = Object.freeze(['complete', 'verified_legacy', 'legacy_incomplete', 'missing', 'not_collected']);
export const FINANCE_STATUS_RANK = Object.freeze(['complete', 'provisional', 'missing']);
export const MASTER_BASIS = 'current';

/**
 * /totals の期間の行の **全部の列** の分類 (= 設計の「型の全部の分類の表」・付録)。列 = mart.amazon_profit_day_totals_range の戻り (0049) と同じ並び。
 *   分類:
 *     omit             = 応答に出さない (旧 totals の行の種類の列)
 *     request_bound    = 要求の from / to
 *     int_sum          = integer を足す (JSON の数)
 *     bigint_sum       = bigint を BigInt で足す (文字列)
 *     bigint_sum_null  = 同じ・1 か月でも null なら null (正式な値・原価)
 *     decimal_sum      = 月の raw (丸める前の numeric の文字) を Decimal で足して最後に 1 回 小数 2 桁に丸める
 *     decimal_sum_null = 同じ・1 か月でも null なら null
 *     state_rank       = 月の中で一番弱い状態 (順位の表)
 *     reasons_union    = 月の理由の和集合を決まった順に
 *     dates_concat     = 月の日付の配列を日付の順につなぐ
 *     jsonb_key_sum    = JSON のキーごとに足す
 *     same_all         = 全部の月で同じか確かめる (違えば 503 PROFIT_VERSION_MISMATCH)
 *     null_in_period   = 期間の行では常に null (月ごとの値は months[] に)
 *     calculated_at    = 要求の 1 つの値
 */
export const TOTALS_COLUMNS = Object.freeze([
  ['row_kind', 'text', 'omit'], ['period_from', 'date', 'request_bound'], ['period_to', 'date', 'request_bound'],
  ['economic_date_jst', 'date', 'omit'], ['month_start', 'date', 'omit'], ['day_count', 'integer', 'int_sum'],
  ['day_finance_status', 'text', 'null_in_period'], ['complete_days', 'integer', 'int_sum'], ['resolved_rows', 'integer', 'int_sum'], ['unresolved_rows', 'integer', 'int_sum'],
  ['units_ordered', 'bigint', 'bigint_sum'], ['units_net_sold', 'bigint', 'bigint_sum'], ['sales_principal_jpy', 'bigint', 'bigint_sum'], ['sales_tax_jpy', 'bigint', 'bigint_sum'],
  ['profit_before_cogs_jpy', 'bigint', 'bigint_sum'], ['cogs_jpy', 'bigint', 'bigint_sum_null'],
  ['easy_ship_alloc_jpy', 'bigint', 'bigint_sum'], ['easy_ship_unallocated_jpy', 'bigint', 'bigint_sum'], ['easy_ship_unallocated_count', 'integer', 'int_sum'],
  ['ad_status', 'text', 'state_rank'], ['ad_cost_total', 'numeric', 'decimal_sum_null'], ['ad_cost_allocated', 'numeric', 'decimal_sum'], ['ad_cost_unresolved', 'numeric', 'decimal_sum'],
  ['ad_unresolved_rows', 'integer', 'int_sum'],
  ['account_fee_cost_jpy', 'bigint', 'bigint_sum'], ['account_fee_cost_excl', 'numeric', 'decimal_sum'],
  ['account_fee_storage_cost_jpy', 'bigint', 'bigint_sum'], ['account_fee_long_term_storage_cost_jpy', 'bigint', 'bigint_sum'], ['account_fee_removal_cost_jpy', 'bigint', 'bigint_sum'],
  ['account_fee_inbound_defect_cost_jpy', 'bigint', 'bigint_sum'], ['account_fee_low_inventory_cost_jpy', 'bigint', 'bigint_sum'], ['account_fee_subscription_cost_jpy', 'bigint', 'bigint_sum'],
  ['account_fee_easy_ship_cost_jpy', 'bigint', 'bigint_sum'], ['account_fee_other_cost_jpy', 'bigint', 'bigint_sum'],
  ['net_jpy', 'bigint', 'bigint_sum'], ['not_account_fee_mapped_jpy', 'bigint', 'bigint_sum'], ['unknown_line_mapped_jpy', 'bigint', 'bigint_sum'], ['unclassified_mapped_jpy', 'bigint', 'bigint_sum'],
  ['unmapped_jpy', 'bigint', 'bigint_sum'],
  ['unknown_line_rows', 'integer', 'int_sum'], ['unclassified_component_count', 'integer', 'int_sum'], ['unmapped_component_count', 'integer', 'int_sum'],
  ['sku_unclassified_component_count', 'integer', 'int_sum'], ['sku_unmapped_component_count', 'integer', 'int_sum'], ['finance_legacy_rows', 'integer', 'int_sum'],
  ['contribution_before_ad_incl_jpy', 'bigint', 'bigint_sum_null'], ['contribution_before_ad_excl', 'numeric', 'decimal_sum_null'],
  ['contribution_after_ad_incl', 'numeric', 'decimal_sum_null'], ['contribution_after_ad_excl', 'numeric', 'decimal_sum_null'],
  ['profit_after_account_fees_incl', 'numeric', 'decimal_sum_null'], ['profit_after_account_fees_excl', 'numeric', 'decimal_sum_null'],
  ['contribution_after_ad_assuming_incomplete_zero_incl', 'numeric', 'decimal_sum'], ['contribution_after_ad_assuming_incomplete_zero_excl', 'numeric', 'decimal_sum'],
  ['profit_after_account_fees_assuming_incomplete_zero_incl', 'numeric', 'decimal_sum'], ['profit_after_account_fees_assuming_incomplete_zero_excl', 'numeric', 'decimal_sum'],
  ['before_ad_incomplete_days', 'date[]', 'dates_concat'], ['before_ad_incomplete_day_count', 'integer', 'int_sum'],
  ['after_ad_incomplete_days', 'date[]', 'dates_concat'], ['after_ad_incomplete_day_count', 'integer', 'int_sum'],
  ['after_account_fees_incomplete_days', 'date[]', 'dates_concat'], ['after_account_fees_incomplete_day_count', 'integer', 'int_sum'],
  ['profit_incomplete_reasons', 'text[]', 'reasons_union'], ['master_basis', 'text', 'same_all'], ['master_note_counts', 'jsonb', 'jsonb_key_sum'],
  ['calculation_version', 'text', 'same_all'], ['finance_coverage_generation', 'bigint', 'null_in_period'], ['finance_source_revision', 'bigint', 'null_in_period'],
  ['calculated_at', 'timestamp with time zone', 'calculated_at'],
].map(([name, type, rule]) => Object.freeze({ name, type, rule })));
export const TOTALS_RULES = Object.freeze(['omit', 'request_bound', 'int_sum', 'bigint_sum', 'bigint_sum_null', 'decimal_sum', 'decimal_sum_null', 'state_rank',
  'reasons_union', 'dates_concat', 'jsonb_key_sum', 'same_all', 'null_in_period', 'calculated_at']);
/** 不完全な日の配列と数の組 (数 = 配列の長さ) */
export const INCOMPLETE_DAY_PAIRS = Object.freeze([
  ['before_ad_incomplete_days', 'before_ad_incomplete_day_count'], ['after_ad_incomplete_days', 'after_ad_incomplete_day_count'],
  ['after_account_fees_incomplete_days', 'after_account_fees_incomplete_day_count'],
]);

/**
 * /daily の行の **全部の列** (= mart.amazon_profit_daily_range の戻り・0050 の _amazon_profit_rows と同じ並び)。日の行は月をまたいで足さない (月ごとの計算の行をつなぐだけ)。
 *   notNull = null にならない列 (ほかは null がありうる = 正式な値・原価・広告・出品の解決などで null + 理由)
 *   numeric は小数 2 桁の文字列。ただし units_*_unrounded は小数 0 か 6 桁 (返品なし = "0")
 */
export const DAILY_COLUMNS = Object.freeze([
  ['company_id', 'smallint'], ['mall', 'text'], ['scope_key', 'text'], ['economic_date_jst', 'date'],
  ['listing_id', 'bigint'], ['seller_sku_norm', 'text'], ['listing_resolution', 'text'], ['listing_code', 'text'],
  ['received_listing_ids', 'bigint[]'], ['received_listing_unresolved_count', 'integer'], ['ad_received_listing_ids', 'bigint[]'], ['ad_received_unresolved_rows', 'integer'],
  ['units_ordered', 'integer'], ['units_refunded_customer', 'integer'], ['units_marketplace_guarantee', 'integer'], ['units_a_to_z_refund', 'integer'], ['units_net_sold', 'integer'],
  ['units_refunded_customer_unrounded', 'numeric'], ['units_a_to_z_refund_unrounded', 'numeric'],
  ['sales_principal_jpy', 'bigint'], ['sales_shipping_jpy', 'bigint'], ['sales_giftwrap_jpy', 'bigint'], ['sales_tax_jpy', 'bigint'],
  ['commission_jpy', 'bigint'], ['fba_fulfillment_jpy', 'bigint'], ['fba_storage_jpy', 'bigint'], ['closing_fee_jpy', 'bigint'],
  ['shipping_chargeback_jpy', 'bigint'], ['giftwrap_chargeback_jpy', 'bigint'], ['promotion_jpy', 'bigint'], ['promotion_tax_jpy', 'bigint'], ['points_jpy', 'bigint'],
  ['warehouse_damage_jpy', 'bigint'], ['warehouse_lost_jpy', 'bigint'], ['safe_t_jpy', 'bigint'], ['refund_principal_jpy', 'bigint'], ['reversal_reimbursement_jpy', 'bigint'],
  ['misc_fee_jpy', 'bigint'], ['other_fee_jpy', 'bigint'], ['other_amount_jpy', 'bigint'],
  ['profit_before_cogs_jpy', 'bigint'], ['taxable_sku_fee_cost_jpy', 'bigint'], ['net_jpy', 'bigint'], ['unmapped_jpy', 'bigint'],
  ['unclassified_component_count', 'integer'], ['unclassified_mapped_jpy', 'bigint'], ['unclassified_abs_jpy', 'bigint'], ['unmapped_component_count', 'integer'], ['finance_legacy_rows', 'integer'],
  ['source_lines', 'integer'], ['order_rows', 'integer'],
  ['day_finance_status', 'text'], ['refund_units_status', 'text'], ['refund_incomplete_child_count', 'integer'], ['refund_unestimated_jpy', 'bigint'],
  ['component_unit_cost_jpy', 'bigint'], ['cogs_jpy', 'bigint'], ['cost_basis', 'text'], ['composition_basis', 'text'],
  ['missing_cost_sku_ids', 'bigint[]'], ['cost_sku_cost_ids', 'bigint[]'], ['cost_observed_ids', 'bigint[]'],
  ['ad_status', 'text'], ['ad_cost', 'numeric'], ['ad_rows', 'integer'], ['easy_ship_alloc_jpy', 'bigint'],
  ['contribution_before_ad_incl_jpy', 'bigint'], ['contribution_before_ad_excl', 'numeric'],
  ['contribution_after_ad_incl', 'numeric'], ['contribution_after_ad_excl', 'numeric'],
  ['contribution_before_ad_assuming_incomplete_zero_incl_jpy', 'bigint'], ['contribution_before_ad_assuming_incomplete_zero_excl', 'numeric'],
  ['contribution_after_ad_assuming_incomplete_zero_incl', 'numeric'], ['contribution_after_ad_assuming_incomplete_zero_excl', 'numeric'],
  ['profit_incomplete_reasons', 'text[]'], ['assumed_zero_reasons', 'text[]'],
  ['master_basis', 'text'], ['master_notes', 'text[]'], ['composition_hash', 'text'], ['cost_input_hash', 'text'],
  ['calculation_version', 'text'], ['observed_generation', 'bigint'], ['composition_audit_since', 'timestamp with time zone'], ['composition_audit_through', 'timestamp with time zone'],
  ['finance_coverage_generation', 'bigint'], ['finance_source_revision', 'bigint'], ['calculated_at', 'timestamp with time zone'],
].map(([name, type]) => Object.freeze({ name, type })));
export const DAILY_NOT_NULL = Object.freeze(['company_id', 'mall', 'scope_key', 'economic_date_jst', 'listing_resolution', 'day_finance_status', 'ad_status',
  'profit_incomplete_reasons', 'assumed_zero_reasons', 'master_basis', 'master_notes', 'calculation_version', 'calculated_at']);
const DAILY_SIX_DP = new Set(['units_refunded_customer_unrounded', 'units_a_to_z_refund_unrounded']);

/** months[] の 1 つ (月の metadata の関数 mart.amazon_profit_month_meta から作る・0 行の月にもある) */
export const MONTH_KEYS = Object.freeze(['month_start', 'period_from', 'period_to', 'finance_status', 'has_finance_rows', 'finance_month_settled',
  'finance_coverage_generation', 'finance_source_revision', 'calculation_version', 'calculated_at']);
export const TOP_KEYS = Object.freeze({
  totals: Object.freeze(['ok', 'contract', 'kind', 'mall', 'scope', 'from', 'to', 'master_basis', 'calculation_version', 'calculated_at', 'master_as_of', 'total', 'months']),
  daily: Object.freeze(['ok', 'contract', 'kind', 'mall', 'scope', 'from', 'to', 'master_basis', 'calculation_version', 'calculated_at', 'master_as_of', 'rows', 'months']),
});

// ─── 値の形 ───
const RE = Object.freeze({
  bigint: /^-?(0|[1-9]\d*)$/,
  dec2: /^-?(0|[1-9]\d*)\.\d{2}$/,
  dec6: /^-?(0|[1-9]\d*)(\.\d{6})?$/,
  date: /^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$/,
  ts: /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/,
  code: /^[A-Z][A-Z0-9_]{2,63}$/,
});
/** raw の列・旧 totals の行の種類の列の名前 (どこに出ても違反) */
const FORBIDDEN_KEY = /(^|_)raw$|^raw_|^row_kind$|^range_total/;
const isValidDate = (s) => typeof s === 'string' && RE.date.test(s) && new Date(`${s}T00:00:00Z`).toISOString().slice(0, 10) === s;
const isTs = (s) => typeof s === 'string' && RE.ts.test(s) && new Date(s).toISOString() === s;
const isBigStr = (s) => typeof s === 'string' && RE.bigint.test(s) && s !== '-0';
const isDec2 = (s) => typeof s === 'string' && RE.dec2.test(s) && !/^-0\.00$/.test(s);
const isDec6 = (s) => typeof s === 'string' && RE.dec6.test(s) && !/^-0(\.0+)?$/.test(s);
const isObj = (x) => x != null && typeof x === 'object' && !Array.isArray(x);

/** 型ごとの値の確かめ (null は呼び手が先に見る) */
export function checkValue(type, v, { sixDp = false } = {}) {
  switch (type) {
    case 'smallint': case 'integer': return Number.isSafeInteger(v);
    case 'bigint': return isBigStr(v);
    case 'bigint[]': return Array.isArray(v) && v.every(isBigStr);
    case 'numeric': return sixDp ? isDec6(v) : isDec2(v);
    case 'date': return isValidDate(v);
    case 'date[]': return Array.isArray(v) && v.every(isValidDate);
    case 'timestamp with time zone': return isTs(v);
    case 'text': return typeof v === 'string';
    case 'text[]': return Array.isArray(v) && v.every((x) => typeof x === 'string');
    case 'jsonb': return isObj(v);
    case 'boolean': return typeof v === 'boolean';
    default: return false;
  }
}

/** 理由・印の配列が決まった順の部分列で重複が無いか */
export const isOrderedSubset = (arr, order) => {
  if (!Array.isArray(arr)) return false;
  let last = -1;
  for (const x of arr) { const i = order.indexOf(x); if (i <= last) return false; last = i; }
  return true;
};

const monthStartOf = (d) => `${d.slice(0, 7)}-01`;
const monthEndOf = (ms) => { const [y, m] = ms.split('-').map(Number); return new Date(Date.UTC(y, m, 0)).toISOString().slice(0, 10); };
/** from〜to が触れる暦月 (順に・最大 13 か月の制限は受け口の側) */
export function monthsOf(from, to) {
  const out = [];
  for (let ms = monthStartOf(from); ms <= to; ) {
    const me = monthEndOf(ms);
    out.push({ month_start: ms, period_from: ms < from ? from : ms, period_to: me > to ? to : me });
    const [y, m] = ms.split('-').map(Number);
    ms = new Date(Date.UTC(y, m, 1)).toISOString().slice(0, 10);
  }
  return out;
}

function findForbiddenKeys(x, path, errs) {
  if (Array.isArray(x)) x.forEach((v, i) => findForbiddenKeys(v, `${path}[${i}]`, errs));
  else if (isObj(x)) for (const [k, v] of Object.entries(x)) {
    if (FORBIDDEN_KEY.test(k)) errs.push(`${path}.${k}: raw / 旧 totals の列は出さない`);
    findForbiddenKeys(v, `${path}.${k}`, errs);
  }
}
const exactKeys = (obj, keys, path, errs) => {
  if (!isObj(obj)) { errs.push(`${path}: object でない`); return false; }
  const have = Object.keys(obj);
  for (const k of keys) if (!Object.hasOwn(obj, k)) errs.push(`${path}.${k}: 無い`);
  for (const k of have) if (!keys.includes(k)) errs.push(`${path}.${k}: 契約に無い列`);
  return true;
};

function checkTop(body, kind, errs) {
  if (!exactKeys(body, TOP_KEYS[kind], '$', errs)) return false;
  if (body.ok !== true) errs.push('$.ok: true でない');
  if (body.contract !== CONTRACT_VERSION) errs.push('$.contract: 版が違う');
  if (body.kind !== kind) errs.push('$.kind: 違う');
  if (typeof body.mall !== 'string' || typeof body.scope !== 'string') errs.push('$.mall / scope: 文字でない');
  if (!isValidDate(body.from) || !isValidDate(body.to) || body.from > body.to) errs.push('$.from / to: 日付でない・逆');
  if (body.master_basis !== MASTER_BASIS) errs.push('$.master_basis: current でない');
  if (typeof body.calculation_version !== 'string' || !body.calculation_version) errs.push('$.calculation_version: 無い');
  if (!isTs(body.calculated_at)) errs.push('$.calculated_at: UTC の ISO (ミリ秒) でない');
  if (body.master_as_of !== body.calculated_at) errs.push('$.master_as_of: calculated_at と違う');
  return true;
}

function checkMonths(body, errs) {
  if (!Array.isArray(body.months)) { errs.push('$.months: 配列でない'); return; }
  if (!isValidDate(body.from) || !isValidDate(body.to) || body.from > body.to) return;
  const want = monthsOf(body.from, body.to);
  if (body.months.length !== want.length) errs.push(`$.months: ${want.length} か月のはずが ${body.months.length}`);
  body.months.forEach((m, i) => {
    const p = `$.months[${i}]`;
    if (!exactKeys(m, MONTH_KEYS, p, errs)) return;
    const w = want[i] || {};
    for (const k of ['month_start', 'period_from', 'period_to']) if (m[k] !== w[k]) errs.push(`${p}.${k}: ${m[k]} (期待 ${w[k]})`);
    if (!FINANCE_STATUS_RANK.includes(m.finance_status)) errs.push(`${p}.finance_status: 知らない状態`);
    if (typeof m.has_finance_rows !== 'boolean' || typeof m.finance_month_settled !== 'boolean') errs.push(`${p}: has_finance_rows / finance_month_settled が boolean でない`);
    for (const k of ['finance_coverage_generation', 'finance_source_revision']) if (m[k] !== null && !isBigStr(m[k])) errs.push(`${p}.${k}: 10 進の文字列か null でない`);
    if (m.calculation_version !== body.calculation_version) errs.push(`${p}.calculation_version: 上と違う (違えば 503 PROFIT_VERSION_MISMATCH)`);
    if (m.calculated_at !== body.calculated_at) errs.push(`${p}.calculated_at: 上と違う (1 つの値)`);
  });
}

/** /totals の 200 の応答を確かめる。@returns {{ ok: boolean, errors: string[] }} */
export function validateTotalsResponse(body) {
  const errs = [];
  findForbiddenKeys(body, '$', errs);
  if (checkTop(body, 'totals', errs)) {
    checkMonths(body, errs);
    const t = body.total;
    const cols = TOTALS_COLUMNS.filter((c) => c.rule !== 'omit');
    if (exactKeys(t, cols.map((c) => c.name), '$.total', errs)) {
      for (const c of cols) {
        const v = t[c.name], p = `$.total.${c.name}`;
        if (!Object.hasOwn(t, c.name)) continue;
        switch (c.rule) {
          case 'request_bound': if (v !== (c.name === 'period_from' ? body.from : body.to)) errs.push(`${p}: 要求の from / to と違う`); break;
          case 'null_in_period': if (v !== null) errs.push(`${p}: 期間の行では null`); break;
          case 'calculated_at': if (v !== body.calculated_at) errs.push(`${p}: 上と違う (1 つの値)`); break;
          case 'same_all': if (v !== body[c.name]) errs.push(`${p}: 上と違う`); break;
          case 'state_rank': if (!AD_STATUS_RANK.includes(v)) errs.push(`${p}: 知らない状態`); break;
          case 'reasons_union': if (!isOrderedSubset(v, TOTALS_REASONS)) errs.push(`${p}: 決まった順の理由の並びでない (重複・知らない理由・順の違い)`); break;
          case 'jsonb_key_sum':
            if (!isObj(v) || Object.keys(v).length !== MASTER_NOTE_KEYS.length || !MASTER_NOTE_KEYS.every((k) => Number.isSafeInteger(v[k]) && v[k] >= 0)) errs.push(`${p}: 3 つのキーの数でない`);
            break;
          case 'dates_concat':
            if (!checkValue('date[]', v) || v.some((d, i) => (i > 0 && d <= v[i - 1]) || d < body.from || d > body.to)) errs.push(`${p}: 期間の中の日付の順の配列でない`);
            break;
          case 'bigint_sum_null': case 'decimal_sum_null': if (v === null) break; // fallthrough
          // eslint-disable-next-line no-fallthrough
          default: if (v === null || !checkValue(c.type, v)) errs.push(`${p}: ${c.type} の形でない (${JSON.stringify(v)})`);
        }
      }
      for (const [days, n] of INCOMPLETE_DAY_PAIRS) if (Array.isArray(t[days]) && t[n] !== t[days].length) errs.push(`$.total.${n}: 日付の数と違う`);
    }
  }
  return { ok: errs.length === 0, errors: errs };
}

/** /daily の 200 の応答を確かめる */
export function validateDailyResponse(body) {
  const errs = [];
  findForbiddenKeys(body, '$', errs);
  if (checkTop(body, 'daily', errs)) {
    checkMonths(body, errs);
    if (!Array.isArray(body.rows)) errs.push('$.rows: 配列でない');
    else {
      const names = DAILY_COLUMNS.map((c) => c.name);
      let prev = null;
      body.rows.forEach((r, i) => {
        const p = `$.rows[${i}]`;
        if (!exactKeys(r, names, p, errs)) return;
        for (const c of DAILY_COLUMNS) {
          const v = r[c.name];
          if (v === null) { if (DAILY_NOT_NULL.includes(c.name)) errs.push(`${p}.${c.name}: null にならない列`); continue; }
          if (!checkValue(c.type, v, { sixDp: DAILY_SIX_DP.has(c.name) })) errs.push(`${p}.${c.name}: ${c.type} の形でない (${JSON.stringify(v)})`);
        }
        if (r.mall !== body.mall || r.scope_key !== body.scope) errs.push(`${p}: mall / scope が上と違う`);
        if (typeof r.economic_date_jst === 'string' && (r.economic_date_jst < body.from || r.economic_date_jst > body.to)) errs.push(`${p}.economic_date_jst: 期間の外`);
        if (!FINANCE_STATUS_RANK.includes(r.day_finance_status)) errs.push(`${p}.day_finance_status: 知らない状態`);
        if (!AD_STATUS_RANK.includes(r.ad_status)) errs.push(`${p}.ad_status: 知らない状態`);
        if (!['resolved', 'unresolved'].includes(r.listing_resolution)) errs.push(`${p}.listing_resolution: 知らない値`);
        if (!isOrderedSubset(r.profit_incomplete_reasons, REASON_ORDER)) errs.push(`${p}.profit_incomplete_reasons: 決まった順でない`);
        if (!isOrderedSubset(r.assumed_zero_reasons, ASSUMED_ZERO_REASONS)) errs.push(`${p}.assumed_zero_reasons: 決まった順でない`);
        if (!isOrderedSubset(r.master_notes, MASTER_NOTE_KEYS)) errs.push(`${p}.master_notes: 決まった順でない`);
        if (r.master_basis !== body.master_basis) errs.push(`${p}.master_basis: 上と違う`);
        if (r.calculation_version !== body.calculation_version) errs.push(`${p}.calculation_version: 上と違う`);
        if (r.calculated_at !== body.calculated_at) errs.push(`${p}.calculated_at: 上と違う (1 つの値・R5 Low)`);
        // 並び = 日 → 出品の ID (null は後ろ) → seller_sku_norm (C の照合 = UTF-16 でなく byte の順。試験の fixture は ASCII)
        const key = [r.economic_date_jst, r.listing_id === null ? 1 : 0, r.listing_id === null ? 0n : BigInt(isBigStr(r.listing_id) ? r.listing_id : 0), r.seller_sku_norm ?? ''];
        if (prev && cmpKey(prev, key) >= 0) errs.push(`${p}: 並び (日 → 出品 → SKU) が違う・重複`);
        prev = key;
      });
    }
  }
  return { ok: errs.length === 0, errors: errs };
}
const cmpKey = (a, b) => {
  for (let i = 0; i < a.length; i++) {
    if (a[i] === b[i]) continue;
    if (typeof a[i] === 'bigint') return a[i] < b[i] ? -1 : 1;
    if (typeof a[i] === 'number') return a[i] - b[i];
    return Buffer.compare(Buffer.from(String(a[i]), 'utf8'), Buffer.from(String(b[i]), 'utf8'));
  }
  return 0;
};

/** 503 の本文を確かめる (今の封じ込めの PROFIT_ROUTE_DISABLED もこの形) */
export function validate503Body(body) {
  const errs = [];
  if (!isObj(body)) return { ok: false, errors: ['$: object でない'] };
  for (const k of Object.keys(body)) if (!['ok', 'code', 'error', 'reason'].includes(k)) errs.push(`$.${k}: 503 に出さない列 (値・部分の結果・Render の本文を返さない)`);
  if (body.ok !== false) errs.push('$.ok: false でない');
  if (!PROFIT_503_CODES.includes(body.code)) errs.push(`$.code: 一覧に無い (${body.code})`);
  if (typeof body.error !== 'string' || !body.error) errs.push('$.error: 文が無い');
  if (Object.hasOwn(body, 'reason') && !(typeof body.reason === 'string' && RE.code.test(body.reason))) errs.push('$.reason: 大文字のコードでない');
  return { ok: errs.length === 0, errors: errs };
}
