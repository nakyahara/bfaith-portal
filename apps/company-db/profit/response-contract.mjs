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

/**
 * 503 の code ごとの **固定の安全な文** (#1602 Codex R1 H1)。error はこの文と完全に一致しなければ違反 = 上流の例外の文・Render の応答の本文・
 * 鍵・接続の文字列を error に入れる道が無い。🚨 PROFIT_ROUTE_DISABLED の文は今の router (#1570) の文そのもの (版の「v3.1」は PR 6 で router と一緒に直す)
 */
export const PROFIT_503_ERRORS = Object.freeze({
  PROFIT_ROUTE_DISABLED: 'Amazon の利益の読む口は 2026-10-01 から止めています (封じ込め)。93 日分の計算で本番の Postgres が落ちたため。'
    + '設計『CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』§3.10 (D-60 v3.1 = 1 か月ずつ計算) で作り直すまで使えません。',
  PROFIT_NOT_CALIBRATED: '承認済みの校正の記録が無いか、今の DB と合わないので計算しません。',
  PROFIT_BUSY: 'ほかの重い処理の最中なので計算しません。しばらくしてからもう一度。',
  PROFIT_RESOURCE: 'DB のメモリ・一時ファイル・負荷の条件を満たさないので計算しません。',
  PROFIT_METRICS_UNAVAILABLE: 'Render の metrics が読めないので計算しません。',
  PROFIT_DB_UNAVAILABLE: 'DB に接続できないか、計算の取引を始められない / 終えられないので、どの値も返しません。',
  PROFIT_PARTIAL_FAILED: '1 か月の計算に失敗したので、どの値も返しません。',
  PROFIT_VERSION_MISMATCH: '月で calculation_version / master_basis が違うので計算できません。',
  PROFIT_INTERNAL: '受け口の中の思わぬ失敗なので、どの値も返しません。',
});
/** 503 の code の一覧 (これ以外は使わない) */
export const PROFIT_503_CODES = Object.freeze(Object.keys(PROFIT_503_ERRORS));
/**
 * metrics の client (apps/company-db/profit/render-metrics.mjs の REASONS・PR 2 #1600) の理由のコード。
 * 🚨 2 つの PR がそろったら試験が両方の一覧の一致を確かめる (片方だけにある間は飛ばす)
 */
export const METRICS_REASONS = Object.freeze(['METRICS_CONFIG', 'METRICS_AUTH', 'METRICS_RATE_LIMITED', 'METRICS_UPSTREAM', 'METRICS_HTTP', 'METRICS_TIMEOUT',
  'METRICS_NETWORK', 'METRICS_SHAPE', 'METRICS_NOT_INTEGER', 'METRICS_EMPTY', 'METRICS_DUPLICATE_SERIES', 'METRICS_DUPLICATE_POINT', 'METRICS_LABEL_MISSING',
  'METRICS_WRONG_RESOURCE', 'METRICS_UNIT', 'METRICS_NEGATIVE', 'METRICS_FUTURE', 'METRICS_STALE', 'METRICS_PAIR_SKEW', 'METRICS_ZERO_LIMIT',
  'METRICS_INCONSISTENT', 'METRICS_INTERNAL']);
/** code ごとに許す reason (列挙・無ければ reason を付けない)。これ以外の reason は違反 */
export const PROFIT_503_REASONS = Object.freeze({
  PROFIT_ROUTE_DISABLED: Object.freeze([]),
  PROFIT_NOT_CALIBRATED: Object.freeze(['CALIBRATION_MISSING', 'CALIBRATION_NOT_APPROVED', 'CALIBRATION_REVOKED', 'CALIBRATION_FINGERPRINT_MISMATCH',
    'CALIBRATION_INPUTS_CHANGED', 'CALIBRATION_PLAN_CHANGED']),
  PROFIT_BUSY: Object.freeze(['LOCK_NOT_AVAILABLE']),
  PROFIT_RESOURCE: Object.freeze(['RESOURCE_MEMORY', 'RESOURCE_TEMP_FILES', 'RESOURCE_PROCESS_INPUTS', 'RESOURCE_LOAD_FACTOR', 'RESOURCE_COUNT_PLAN',
    'RESOURCE_TEMP_FILE_LIMIT_NOT_FINITE', 'RESOURCE_PRIVILEGES']),
  PROFIT_METRICS_UNAVAILABLE: METRICS_REASONS,
  PROFIT_DB_UNAVAILABLE: Object.freeze(['DB_CONNECT', 'DB_BEGIN', 'DB_SET_LOCAL', 'DB_SETTING_READBACK', 'DB_LOCK_STATEMENT', 'DB_COMMIT', 'DB_OUTCOME_UNKNOWN',
    'DB_UNEXPECTED']),
  PROFIT_PARTIAL_FAILED: Object.freeze(['MONTH_META_FAILED', 'MONTH_CALC_FAILED', 'MONTH_STATEMENT_TIMEOUT']),
  PROFIT_VERSION_MISMATCH: Object.freeze(['CALCULATION_VERSION', 'MASTER_BASIS']),
  PROFIT_INTERNAL: Object.freeze(['APP_UNEXPECTED']),
});
/**
 * 1 回の要求の **全部の失敗の経路** → 503 の code / reason の対応表 (#1602 Codex R1 M2・設計 §3.10「1 回の要求の取引」0.〜7. の順)。
 * HTTP の切断 (7.) は応答を返さない (pg_cancel_backend・接続を pool に戻さない) = 503 の対象でない
 */
export const FAILURE_PATHS = Object.freeze([
  ['封じ込め (PR 6 より前)', 'PROFIT_ROUTE_DISABLED', null],
  ['0. metrics: Render の API の 200 でない応答 (400・401・403・429・5xx ほか全部)・網・timeout・形の違い', 'PROFIT_METRICS_UNAVAILABLE', 'METRICS_*'],
  ['1. DB の接続の失敗', 'PROFIT_DB_UNAVAILABLE', 'DB_CONNECT'],
  ['2. BEGIN READ ONLY REPEATABLE READ の失敗', 'PROFIT_DB_UNAVAILABLE', 'DB_BEGIN'],
  ['2. SET LOCAL の失敗', 'PROFIT_DB_UNAVAILABLE', 'DB_SET_LOCAL'],
  ['2. 設定の読み返しが違う / 読めない', 'PROFIT_DB_UNAVAILABLE', 'DB_SETTING_READBACK'],
  ['3. 最初の文 (lock + statement_timestamp) の失敗', 'PROFIT_DB_UNAVAILABLE', 'DB_LOCK_STATEMENT'],
  ['3. 共通の lock が取れない', 'PROFIT_BUSY', 'LOCK_NOT_AVAILABLE'],
  ['4. lock の後の metrics の鮮度が 2 分を超えた', 'PROFIT_METRICS_UNAVAILABLE', 'METRICS_STALE'],
  ['4. 校正の記録が無い・未承認・revoked・fingerprint / 材料 / 実行計画が違う', 'PROFIT_NOT_CALIBRATED', 'CALIBRATION_*'],
  ['4. 資源の関門 (メモリ・一時ファイル・process の数の材料・負荷の要因・数え上げの計画・temp_file_limit・権限)', 'PROFIT_RESOURCE', 'RESOURCE_*'],
  ['4. 関門の中の予期しない DB の例外 (月の計算の前)', 'PROFIT_DB_UNAVAILABLE', 'DB_UNEXPECTED'],
  ['5. 月の metadata の関数の失敗', 'PROFIT_PARTIAL_FAILED', 'MONTH_META_FAILED'],
  ['5. 月の包む関数の失敗', 'PROFIT_PARTIAL_FAILED', 'MONTH_CALC_FAILED'],
  ['5. 月の計算の statement_timeout', 'PROFIT_PARTIAL_FAILED', 'MONTH_STATEMENT_TIMEOUT'],
  ['5. calculation_version / master_basis が月で違う', 'PROFIT_VERSION_MISMATCH', 'CALCULATION_VERSION / MASTER_BASIS'],
  ['6. COMMIT の失敗', 'PROFIT_DB_UNAVAILABLE', 'DB_COMMIT'],
  ['6. 結果が分からない (接続が切れた・ROLLBACK も失敗 = 接続を捨てる)', 'PROFIT_DB_UNAVAILABLE', 'DB_OUTCOME_UNKNOWN'],
  ['応答を作る所 (アプリ) の思わぬ例外', 'PROFIT_INTERNAL', 'APP_UNEXPECTED'],
].map(([path, code, reason]) => Object.freeze({ path, code, reason })));
/**
 * 応答に付ける header (200 も 503 も)。AI・画面は正式な値を自分で保存しない。
 * 🚨 今の router の 503 (封じ込め) には付いていない = 契約の定数だけ。**router に触れる PR (遅くとも PR 6) の必須の条件**: 本物の HTTP の応答の header を試験で確かめる
 */
export const REQUIRED_HEADERS = Object.freeze({ 'cache-control': 'no-store' });
/** 1 回の要求で触れる暦月の上限 (受け口の 400 と、この契約の validator の両方で縛る) */
export const MAX_MONTHS = 13;

/** 503 の本文を作る (固定の文・列挙にない reason は付けない = 上流の例外の文を入れる道が無い) */
export function build503Body(code, reason) {
  const c = Object.hasOwn(PROFIT_503_ERRORS, code) ? code : 'PROFIT_INTERNAL';
  const body = { ok: false, code: c, error: PROFIT_503_ERRORS[c] };
  if (typeof reason === 'string' && PROFIT_503_REASONS[c].includes(reason)) body.reason = reason;
  return body;
}

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
 * 🆕 #1602 Codex R1 M1: 0050 の式 (r0〜r3 と最後の SELECT) どおりに、全部の列の null の可否・下限・列挙を分類する。
 *   nul (null になる条件):
 *     never                     = null にならない (数・金額の多くは coalesce(…, 0)・配列は coalesce(…, '{}'))
 *     maybe                     = null がありうる (世代・監査の時刻・coverage の世代と版)
 *     iff_unresolved            = 出品が未解決のときだけ null (listing_id・listing_code)
 *     iff_resolved              = 出品が解決したときだけ null (seller_sku_norm = 未解決の行の鍵)
 *     iff_cost_unknown          = 原価が分からない (理由に listing_unresolved・composition_missing・cost_missing のどれか) ときだけ null
 *     iff_composition_unknown   = 構成が分からない (理由に listing_unresolved・composition_missing) ときだけ null
 *     iff_refund_price_missing  = refund_units_status = unit_price_missing のときだけ null
 *     iff_ad_uncollected        = ad_status が not_collected・missing のときだけ null
 *     iff_not_ok_before         = 理由に「広告の前」の理由 (BEFORE_AD_REASONS) があるときだけ null (正式な値)
 *     iff_not_ok_after          = 理由が 1 つでもあるときだけ null (正式な値)
 *   min = 下限 (件数は 0 以上) / max = 上限 / enum = 値の一覧 / hex64 = 64 桁の小文字の 16 進 / sixDp = 小数 0 か 6 桁
 */
export const LISTING_RESOLUTIONS = Object.freeze(['resolved', 'unresolved']);
export const REFUND_UNITS_STATUSES = Object.freeze(['no_refund', 'estimated_monthly_unit_price', 'estimated_partial_month_unit_price', 'unit_price_missing']);
export const COST_BASES = Object.freeze(['sku_costs', 'observed', 'estimated', 'missing']);
export const COMPOSITION_BASES = Object.freeze(['listing_unresolved', 'missing', 'pre_audit_unverifiable', 'current_after_recorded_change', 'current_no_recorded_change']);
/** 正式な「広告の前」の値を止める理由 (0050 の ok_before) */
export const BEFORE_AD_REASONS = Object.freeze(['finance_incomplete', 'finance_unclassified', 'refund_units_unknown', 'refund_units_partial_month',
  'listing_unresolved', 'composition_missing', 'cost_missing']);
const N0 = Object.freeze({ min: 0 });
export const DAILY_COLUMNS = Object.freeze([
  ['company_id', 'smallint', 'never'], ['mall', 'text', 'never'], ['scope_key', 'text', 'never'], ['economic_date_jst', 'date', 'never'],
  ['listing_id', 'bigint', 'iff_unresolved'], ['seller_sku_norm', 'text', 'iff_resolved'], ['listing_resolution', 'text', 'never', { enum: LISTING_RESOLUTIONS }],
  ['listing_code', 'text', 'iff_unresolved'],
  ['received_listing_ids', 'bigint[]', 'never'], ['received_listing_unresolved_count', 'integer', 'never', N0], ['ad_received_listing_ids', 'bigint[]', 'never'],
  ['ad_received_unresolved_rows', 'integer', 'never', N0],
  ['units_ordered', 'integer', 'never'], ['units_refunded_customer', 'integer', 'never'], ['units_marketplace_guarantee', 'integer', 'never'],
  ['units_a_to_z_refund', 'integer', 'never'], ['units_net_sold', 'integer', 'never'],
  ['units_refunded_customer_unrounded', 'numeric', 'iff_refund_price_missing', { sixDp: true }], ['units_a_to_z_refund_unrounded', 'numeric', 'iff_refund_price_missing', { sixDp: true }],
  ['sales_principal_jpy', 'bigint', 'never'], ['sales_shipping_jpy', 'bigint', 'never'], ['sales_giftwrap_jpy', 'bigint', 'never'], ['sales_tax_jpy', 'bigint', 'never'],
  ['commission_jpy', 'bigint', 'never'], ['fba_fulfillment_jpy', 'bigint', 'never'], ['fba_storage_jpy', 'bigint', 'never'], ['closing_fee_jpy', 'bigint', 'never'],
  ['shipping_chargeback_jpy', 'bigint', 'never'], ['giftwrap_chargeback_jpy', 'bigint', 'never'], ['promotion_jpy', 'bigint', 'never'], ['promotion_tax_jpy', 'bigint', 'never'],
  ['points_jpy', 'bigint', 'never'],
  ['warehouse_damage_jpy', 'bigint', 'never'], ['warehouse_lost_jpy', 'bigint', 'never'], ['safe_t_jpy', 'bigint', 'never'], ['refund_principal_jpy', 'bigint', 'never'],
  ['reversal_reimbursement_jpy', 'bigint', 'never'],
  ['misc_fee_jpy', 'bigint', 'never'], ['other_fee_jpy', 'bigint', 'never'], ['other_amount_jpy', 'bigint', 'never'],
  ['profit_before_cogs_jpy', 'bigint', 'never'], ['taxable_sku_fee_cost_jpy', 'bigint', 'never'], ['net_jpy', 'bigint', 'never'], ['unmapped_jpy', 'bigint', 'never'],
  ['unclassified_component_count', 'integer', 'never', N0], ['unclassified_mapped_jpy', 'bigint', 'never'], ['unclassified_abs_jpy', 'bigint', 'never', N0],
  ['unmapped_component_count', 'integer', 'never', N0], ['finance_legacy_rows', 'integer', 'never', N0],
  ['source_lines', 'integer', 'never', N0], ['order_rows', 'integer', 'never', N0],
  ['day_finance_status', 'text', 'never', { enum: FINANCE_STATUS_RANK }], ['refund_units_status', 'text', 'never', { enum: REFUND_UNITS_STATUSES }],
  ['refund_incomplete_child_count', 'integer', 'never', { min: 0, max: 1 }], ['refund_unestimated_jpy', 'bigint', 'never'],
  ['component_unit_cost_jpy', 'bigint', 'iff_cost_unknown'], ['cogs_jpy', 'bigint', 'iff_cost_unknown'], ['cost_basis', 'text', 'never', { enum: COST_BASES }],
  ['composition_basis', 'text', 'never', { enum: COMPOSITION_BASES }],
  ['missing_cost_sku_ids', 'bigint[]', 'never'], ['cost_sku_cost_ids', 'bigint[]', 'never'], ['cost_observed_ids', 'bigint[]', 'never'],
  ['ad_status', 'text', 'never', { enum: AD_STATUS_RANK }], ['ad_cost', 'numeric', 'iff_ad_uncollected'], ['ad_rows', 'integer', 'never', N0],
  ['easy_ship_alloc_jpy', 'bigint', 'never'],
  ['contribution_before_ad_incl_jpy', 'bigint', 'iff_not_ok_before'], ['contribution_before_ad_excl', 'numeric', 'iff_not_ok_before'],
  ['contribution_after_ad_incl', 'numeric', 'iff_not_ok_after'], ['contribution_after_ad_excl', 'numeric', 'iff_not_ok_after'],
  ['contribution_before_ad_assuming_incomplete_zero_incl_jpy', 'bigint', 'never'], ['contribution_before_ad_assuming_incomplete_zero_excl', 'numeric', 'never'],
  ['contribution_after_ad_assuming_incomplete_zero_incl', 'numeric', 'never'], ['contribution_after_ad_assuming_incomplete_zero_excl', 'numeric', 'never'],
  ['profit_incomplete_reasons', 'text[]', 'never'], ['assumed_zero_reasons', 'text[]', 'never'],
  ['master_basis', 'text', 'never', { enum: Object.freeze([MASTER_BASIS]) }], ['master_notes', 'text[]', 'never'],
  ['composition_hash', 'text', 'iff_composition_unknown', { hex64: true }], ['cost_input_hash', 'text', 'iff_cost_unknown', { hex64: true }],
  ['calculation_version', 'text', 'never'], ['observed_generation', 'bigint', 'maybe'], ['composition_audit_since', 'timestamp with time zone', 'never'],
  ['composition_audit_through', 'timestamp with time zone', 'maybe'],
  ['finance_coverage_generation', 'bigint', 'maybe'], ['finance_source_revision', 'bigint', 'maybe'], ['calculated_at', 'timestamp with time zone', 'never'],
].map(([name, type, nul, extra = {}]) => Object.freeze({ name, type, nul, ...extra })));
export const DAILY_NUL_RULES = Object.freeze(['never', 'maybe', 'iff_unresolved', 'iff_resolved', 'iff_cost_unknown', 'iff_composition_unknown',
  'iff_refund_price_missing', 'iff_ad_uncollected', 'iff_not_ok_before', 'iff_not_ok_after']);
/** 互換の名前 (null にならない列の一覧) */
export const DAILY_NOT_NULL = Object.freeze(DAILY_COLUMNS.filter((c) => c.nul === 'never').map((c) => c.name));

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
// 形の合う文字でも実在しない日時 (2026-99-99T99:99:99.999Z) は toISOString が RangeError を投げる = 先に数で確かめる (#1602 Codex R1 Low)
const isoOf = (s) => { const ms = Date.parse(s); return Number.isFinite(ms) ? new Date(ms).toISOString() : null; };
const isValidDate = (s) => typeof s === 'string' && RE.date.test(s) && (isoOf(`${s}T00:00:00Z`) || '').slice(0, 10) === s;
const isTs = (s) => typeof s === 'string' && RE.ts.test(s) && isoOf(s) === s;
const HEX64 = /^[0-9a-f]{64}$/;
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
  if (want.length > MAX_MONTHS) errs.push(`$.from / to: ${want.length} か月に触れる (上限 ${MAX_MONTHS} か月・受け口が 400 で拒むはず)`);
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
  return guard(() => totalsErrors(body));
}
function totalsErrors(body) {
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
          default:
            if (v === null || !checkValue(c.type, v)) errs.push(`${p}: ${c.type} の形でない (${JSON.stringify(v)})`);
            else if (c.rule === 'int_sum' && v < 0) errs.push(`${p}: 件数が負 (int_sum の列は日数・行数・件数 = 0 以上・#1602 Codex R2 Low)`);
        }
      }
      for (const [days, n] of INCOMPLETE_DAY_PAIRS) if (Array.isArray(t[days]) && t[n] !== t[days].length) errs.push(`$.total.${n}: 日付の数と違う`);
    }
  }
  return errs;
}

/** /daily の 200 の応答を確かめる */
export function validateDailyResponse(body) {
  return guard(() => dailyErrors(body));
}
const has = (arr, x) => Array.isArray(arr) && arr.includes(x);
/** 列の null の条件 (DAILY_COLUMNS の nul) が今の行で「null であるべき」か。maybe は undefined (どちらでもよい) */
function nullExpected(nul, r) {
  const reasons = r.profit_incomplete_reasons;
  switch (nul) {
    case 'never': return false;
    case 'maybe': return undefined;
    case 'iff_unresolved': return r.listing_resolution === 'unresolved';
    case 'iff_resolved': return r.listing_resolution === 'resolved';
    case 'iff_cost_unknown': return ['listing_unresolved', 'composition_missing', 'cost_missing'].some((x) => has(reasons, x));
    case 'iff_composition_unknown': return ['listing_unresolved', 'composition_missing'].some((x) => has(reasons, x));
    case 'iff_refund_price_missing': return r.refund_units_status === 'unit_price_missing';
    case 'iff_ad_uncollected': return r.ad_status === 'not_collected' || r.ad_status === 'missing';
    case 'iff_not_ok_before': return BEFORE_AD_REASONS.some((x) => has(reasons, x));
    case 'iff_not_ok_after': return Array.isArray(reasons) && reasons.length > 0;
    default: throw new Error(`知らない null の規則 ${nul}`);
  }
}
/** 行の中の不変条件 (0050 の式から)。破れていたら文を返す */
function dailyRowRuleErrors(r, p) {
  const e = [];
  const reasons = r.profit_incomplete_reasons, notes = r.master_notes;
  const iff = (a, b, what) => { if (Boolean(a) !== Boolean(b)) e.push(`${p}: ${what}`); };
  if (r.listing_resolution === 'unresolved') {
    if (r.cost_basis !== 'missing') e.push(`${p}.cost_basis: 未解決の行は missing`);
    if (r.composition_basis !== 'listing_unresolved') e.push(`${p}.composition_basis: 未解決の行は listing_unresolved`);
    if (Array.isArray(notes) && notes.some((x) => x !== 'listing_changed_since_received')) e.push(`${p}.master_notes: 未解決の行は listing_changed_since_received だけ`);
  }
  iff(r.listing_resolution === 'unresolved', has(reasons, 'listing_unresolved'), '未解決 ⇔ 理由 listing_unresolved');
  iff(r.listing_resolution === 'resolved' && r.composition_basis === 'missing', has(reasons, 'composition_missing'), '構成が無い (composition_basis = missing) ⇔ 理由 composition_missing');
  if (r.listing_resolution === 'resolved' && r.composition_basis === 'listing_unresolved') e.push(`${p}.composition_basis: 解決した行に listing_unresolved`);
  iff(r.cost_basis === 'missing', ['listing_unresolved', 'composition_missing', 'cost_missing'].some((x) => has(reasons, x)), '原価が分からない ⇔ cost_basis = missing');
  // 0050 の g_unres・g_comp・g_cost は排他 (g_cost = 出品あり かつ 構成あり かつ 原価が分からない・#1602 Codex R3 M1)
  iff(has(reasons, 'cost_missing'), r.listing_resolution === 'resolved' && r.composition_basis !== 'missing' && r.cost_basis === 'missing',
    '理由 cost_missing ⇔ 解決した行 かつ 構成あり (composition_basis ≠ missing) かつ cost_basis = missing (listing_unresolved・composition_missing と排他)');
  if (r.listing_resolution === 'resolved' && !has(reasons, 'composition_missing')) {
    const pre = has(notes, 'pre_audit_unverifiable'), after = has(notes, 'current_after_recorded_change');
    const want = pre ? 'pre_audit_unverifiable' : after ? 'current_after_recorded_change' : 'current_no_recorded_change';
    if (r.composition_basis !== want) e.push(`${p}.composition_basis: master_notes からは ${want}`);
  }
  iff(r.refund_units_status === 'unit_price_missing', has(reasons, 'refund_units_unknown'), '返品の単価が無い ⇔ 理由 refund_units_unknown');
  iff(r.refund_units_status === 'estimated_partial_month_unit_price', has(reasons, 'refund_units_partial_month'), '月の途中の単価 ⇔ 理由 refund_units_partial_month');
  const incomplete = r.refund_units_status === 'unit_price_missing' || r.refund_units_status === 'estimated_partial_month_unit_price';
  if (r.refund_incomplete_child_count !== (incomplete ? 1 : 0)) e.push(`${p}.refund_incomplete_child_count: 返品の状態からは ${incomplete ? 1 : 0}`);
  iff(r.day_finance_status !== 'complete', has(reasons, 'finance_incomplete'), '日の財務が complete でない ⇔ 理由 finance_incomplete');
  // 0050 の g_uncl = (unclassified_component_count + unmapped_component_count + legacy_n) > 0 (#1602 Codex R2 M1)
  const unclassified = [r.unclassified_component_count, r.unmapped_component_count, r.finance_legacy_rows].reduce((a, v) => a + (Number.isSafeInteger(v) ? v : 0), 0);
  iff(unclassified > 0, has(reasons, 'finance_unclassified'), '分けられない部品・対応の無い部品・旧い形の行の件数の合計 > 0 ⇔ 理由 finance_unclassified');
  iff(r.ad_status === 'not_collected', has(reasons, 'ad_not_collected'), '広告 not_collected ⇔ 理由 ad_not_collected');
  iff(r.ad_status === 'missing', has(reasons, 'ad_missing'), '広告 missing ⇔ 理由 ad_missing');
  iff(r.ad_status === 'legacy_incomplete', has(reasons, 'ad_legacy_unverified'), '広告 legacy_incomplete ⇔ 理由 ad_legacy_unverified');
  // 0 と仮定の理由 = 理由から refund_units_partial_month を除いたもの (0050 の 2 つの array_remove)
  if (Array.isArray(reasons) && Array.isArray(r.assumed_zero_reasons)
    && JSON.stringify(r.assumed_zero_reasons) !== JSON.stringify(reasons.filter((x) => x !== 'refund_units_partial_month'))) {
    e.push(`${p}.assumed_zero_reasons: 理由から refund_units_partial_month を除いたものと違う`);
  }
  return e;
}
function dailyErrors(body) {
  const errs = [];
  findForbiddenKeys(body, '$', errs);
  if (!checkTop(body, 'daily', errs)) return errs;
  checkMonths(body, errs);
  if (!Array.isArray(body.rows)) { errs.push('$.rows: 配列でない'); return errs; }
  const names = DAILY_COLUMNS.map((c) => c.name);
  let prev = null;
  body.rows.forEach((r, i) => {
    const p = `$.rows[${i}]`;
    exactKeys(r, names, p, errs);
    if (!isObj(r)) return;
    for (const c of DAILY_COLUMNS) {
      const v = r[c.name];
      const want = nullExpected(c.nul, r);
      if (v === null || v === undefined) {
        if (want === false) errs.push(`${p}.${c.name}: null にならない列 (${c.nul})`);
        continue;
      }
      if (want === true) { errs.push(`${p}.${c.name}: この行では null のはず (${c.nul})`); continue; }
      if (!checkValue(c.type, v, { sixDp: Boolean(c.sixDp) })) { errs.push(`${p}.${c.name}: ${c.type} の形でない (${JSON.stringify(v)})`); continue; }
      if (c.min != null && (c.type === 'bigint' ? BigInt(v) < BigInt(c.min) : v < c.min)) errs.push(`${p}.${c.name}: ${c.min} より小さい`);
      if (c.max != null && v > c.max) errs.push(`${p}.${c.name}: ${c.max} より大きい`);
      if (c.enum && !c.enum.includes(v)) errs.push(`${p}.${c.name}: 知らない値 (${JSON.stringify(v)})`);
      if (c.hex64 && !HEX64.test(v)) errs.push(`${p}.${c.name}: 64 桁の 16 進でない`);
    }
    if (r.mall !== body.mall || r.scope_key !== body.scope) errs.push(`${p}: mall / scope が上と違う`);
    if (typeof r.economic_date_jst === 'string' && (r.economic_date_jst < body.from || r.economic_date_jst > body.to)) errs.push(`${p}.economic_date_jst: 期間の外`);
    if (!isOrderedSubset(r.profit_incomplete_reasons, REASON_ORDER)) errs.push(`${p}.profit_incomplete_reasons: 決まった順でない`);
    if (!isOrderedSubset(r.assumed_zero_reasons, ASSUMED_ZERO_REASONS)) errs.push(`${p}.assumed_zero_reasons: 決まった順でない`);
    if (!isOrderedSubset(r.master_notes, MASTER_NOTE_KEYS)) errs.push(`${p}.master_notes: 決まった順でない`);
    if (r.master_basis !== body.master_basis) errs.push(`${p}.master_basis: 上と違う`);
    if (r.calculation_version !== body.calculation_version) errs.push(`${p}.calculation_version: 上と違う`);
    if (r.calculated_at !== body.calculated_at) errs.push(`${p}.calculated_at: 上と違う (1 つの値・R5 Low)`);
    errs.push(...dailyRowRuleErrors(r, p));
    // 並び = 日 → 出品の ID (null は後ろ) → seller_sku_norm (C の照合 = byte の順)。行の鍵 (日 × 出品 / 日 × 未解決の SKU) は重ならない
    const key = [String(r.economic_date_jst), r.listing_id === null ? 1 : 0, isBigStr(r.listing_id) ? BigInt(r.listing_id) : 0n, String(r.seller_sku_norm ?? '')];
    if (prev && cmpKey(prev, key) >= 0) errs.push(`${p}: 並び (日 → 出品 → SKU) が違う・行の鍵が重なる`);
    prev = key;
  });
  errs.push(...dayLevelErrors(body.rows));
  return errs;
}
/**
 * 日で決まる値は同じ日の行で全部同じ (#1602 Codex R3 M1 の「ほかの理由の漏れ」の突き合わせ)。0050 では
 *   day_finance_status・finance_coverage_generation・finance_source_revision = days (日だけで結ぶ) / ad_status = ad_days (日だけで結ぶ) /
 *   理由 ad_unresolved = ad_u (その日の出品の無い広告の行の数 > 0) = 行ではなく日の値
 */
export const DAY_LEVEL_FIELDS = Object.freeze(['day_finance_status', 'ad_status', 'finance_coverage_generation', 'finance_source_revision']);
function dayLevelErrors(rows) {
  const e = [], first = new Map();
  rows.forEach((r, i) => {
    if (!isObj(r)) return;
    const sig = JSON.stringify([...DAY_LEVEL_FIELDS.map((k) => r[k]), has(r.profit_incomplete_reasons, 'ad_unresolved')]);
    const d = String(r.economic_date_jst);
    if (!first.has(d)) first.set(d, { sig, i });
    else if (first.get(d).sig !== sig) e.push(`$.rows[${i}]: 同じ日 (${d}) の行 ${first.get(d).i} と、日で決まる値 (${DAY_LEVEL_FIELDS.join('・')}・理由 ad_unresolved) が違う`);
  });
  return e;
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

/** 秘密らしい文字 (どの応答のどこにあっても違反・念のための 2 重の守り) */
export const SECRET_PATTERNS = Object.freeze([/Bearer\s/i, /rnd_[A-Za-z0-9]{6,}/, /postgres(ql)?:\/\//i, /RENDER_API_KEY/, /x-sync-key/i, /password\s*=/i, /authorization/i]);
const findSecrets = (body, errs) => {
  let text;
  try { text = JSON.stringify(body); } catch { errs.push('$: JSON にできない'); return; }
  for (const re of SECRET_PATTERNS) if (re.test(text)) errs.push(`$: 秘密らしい文字 (${re}) がある`);
};

/** 503 の本文を確かめる (今の封じ込めの PROFIT_ROUTE_DISABLED もこの形)。error は code の固定の文・reason は code ごとの列挙だけ */
export function validate503Body(body) {
  return guard(() => {
    const errs = [];
    if (!isObj(body)) return ['$: object でない'];
    findSecrets(body, errs);
    for (const k of Object.keys(body)) if (!['ok', 'code', 'error', 'reason'].includes(k)) errs.push(`$.${k}: 503 に出さない列 (値・部分の結果・Render の本文を返さない)`);
    if (body.ok !== false) errs.push('$.ok: false でない');
    if (!PROFIT_503_CODES.includes(body.code)) { errs.push(`$.code: 一覧に無い`); return errs; }
    if (body.error !== PROFIT_503_ERRORS[body.code]) errs.push('$.error: code の固定の文と違う (上流の例外・本文を入れない)');
    if (Object.hasOwn(body, 'reason') && !PROFIT_503_REASONS[body.code].includes(body.reason)) errs.push('$.reason: この code の列挙に無い');
    return errs;
  });
}

/** validator の中の思わぬ例外も「違反」で返す (例外を投げない・#1602 Codex R1 Low) */
function guard(fn) {
  try {
    const errs = fn();
    return { ok: errs.length === 0, errors: errs };
  } catch {
    return { ok: false, errors: ['$: 確かめる途中で例外 (形が違う)'] };
  }
}
