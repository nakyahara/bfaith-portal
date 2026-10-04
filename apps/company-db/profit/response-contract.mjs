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
 *   503         = { ok: false, code: PROFIT_503_CODES のどれか, error: 文, reason?: 'METRICS_…' などの大文字のコード, failed_months?: ['YYYY-MM', …] (PROFIT_PARTIAL_FAILED だけ) }
 *
 * 🆕 v2 (D-60 の PR 2c・設計 §3.10 の PR の表の 2c・§5 の 0b-2 の (d)・0b-3 の (c)・設計 19 v14 §6.6.1 / §6.2.1 ③):
 *   - months[] に finance_coverage_token (64 桁の小文字の 16 進・core.finance_coverage_token の値 = F4 の coverage_token と同じ関数・3a で作る)
 *   - /daily の行に member_seller_skus (その行の粒度にまとまった、受け取った seller SKU を trim + 小文字・UTF-8 の bytes の順・重複なしの配列)
 *   - 503: 57014 / 25P04 の分け方 (CANCEL_MAP・classifyCancellation) の reason を列挙に・PROFIT_PARTIAL_FAILED の本文に failed_months (月だけ・値は出さない)
 *
 * 値の書き方 (JSON):
 *   bigint (金額・数・ID) = 10 進の文字列 (JS の Number に入れない) / bigint[] = その文字列の配列
 *   numeric の金額 = 小数 2 桁の文字列 (丸めは PostgreSQL の round(numeric, 2) = 0 から遠い方へ・half away from zero。"-0.00" は無い)
 *   numeric の返品数 (units_*_unrounded) = 小数 0 か 6 桁の文字列 / integer = 数 / date = "YYYY-MM-DD" / timestamptz = UTC の ISO (ミリ秒・Z)
 *   text = 文字 / text[] = 文字の配列 / jsonb = object / boolean
 *   calculated_at = 要求の最初の文の statement_timestamp() の 1 つの値 = 上・rows の全部・months の全部で同じ (R5 Low)。master_as_of = calculated_at (D-64)
 *   raw の列 (丸める前の値・名前が _raw で終わる) と旧 totals の行の種類 (row_kind・month_start ほか) は出さない
 */

export const CONTRACT_VERSION = 'amazon_profit_response_v2';
/**
 * 版の履歴 (形を変えたら版を上げてここに 1 行足す・試験が CONTRACT_VERSION = 最後の行を確かめる)。
 * 🚨 v2 は PR 5 (校正) と PR 6 (開ける) の前に入れる = 開けた応答に最初から含める (後から足すと校正と契約の版をやり直す・設計 19 §6.6.1)
 */
export const CONTRACT_HISTORY = Object.freeze([
  Object.freeze({ version: 'amazon_profit_response_v1', pr: '#1602 (D-60 PR 2b)', change: '最初の形 (期間の全体の 1 行 + months[]・日 × 出品の行・503 の固定の文と reason の列挙)' }),
  Object.freeze({ version: 'amazon_profit_response_v2', pr: 'D-60 PR 2c', change: 'months[].finance_coverage_token・日の行の member_seller_skus・57014 / 25P04 の分け方の reason (CANCEL_MAP)・PROFIT_PARTIAL_FAILED の failed_months' }),
]);

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
    'RESOURCE_TEMP_FILE_LIMIT_NOT_FINITE', 'RESOURCE_PRIVILEGES',
    // 🆕 v2 (設計 §3.10「57014 の分け方」の表・R-v3-10 M2)
    'RESOURCE_LOAD_COUNT_TIME', 'RESOURCE_TRANSACTION_TIMEOUT']),
  PROFIT_METRICS_UNAVAILABLE: METRICS_REASONS,
  PROFIT_DB_UNAVAILABLE: Object.freeze(['DB_CONNECT', 'DB_BEGIN', 'DB_SET_LOCAL', 'DB_SETTING_READBACK', 'DB_LOCK_STATEMENT', 'DB_COMMIT', 'DB_OUTCOME_UNKNOWN',
    'DB_UNEXPECTED']),
  PROFIT_PARTIAL_FAILED: Object.freeze(['MONTH_META_FAILED', 'MONTH_CALC_FAILED', 'MONTH_STATEMENT_TIMEOUT']),
  PROFIT_VERSION_MISMATCH: Object.freeze(['CALCULATION_VERSION', 'MASTER_BASIS']),
  PROFIT_INTERNAL: Object.freeze(['APP_UNEXPECTED',
    // 🆕 v2 (設計 §3.10「57014 の分け方」の表)
    'INTERNAL_EXTERNAL_CANCEL', 'INTERNAL_UNCLASSIFIED_CANCEL']),
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
  ['5. 月の計算の statement_timeout (57014・アプリの timer の印 app_statement_budget)', 'PROFIT_PARTIAL_FAILED', 'MONTH_STATEMENT_TIMEOUT'],
  ['5. calculation_version / master_basis が月で違う', 'PROFIT_VERSION_MISMATCH', 'CALCULATION_VERSION / MASTER_BASIS'],
  ['6. COMMIT の失敗', 'PROFIT_DB_UNAVAILABLE', 'DB_COMMIT'],
  ['6. 結果が分からない (接続が切れた・ROLLBACK も失敗 = 接続を捨てる)', 'PROFIT_DB_UNAVAILABLE', 'DB_OUTCOME_UNKNOWN'],
  ['応答を作る所 (アプリ) の思わぬ例外', 'PROFIT_INTERNAL', 'APP_UNEXPECTED'],
  // 🆕 v2 = 57014 / 25P04 の分け方 (下の CANCEL_MAP と同じ行・設計 §3.10「門の関数の契約」の「57014 の分け方」の表)
  ['4. 負荷の数え上げの文の取り消し (57014・アプリの timer の印 app_statement_budget)', 'PROFIT_RESOURCE', 'RESOURCE_LOAD_COUNT_TIME'],
  ['4. 負荷の数え上げの wall-clock の deadline (印 app_deadline・57014 か取り消し無し)', 'PROFIT_RESOURCE', 'RESOURCE_LOAD_COUNT_TIME'],
  ['4.・5. 印の無い取り消し (57014 unmarked = 人・見張りの pg_cancel_backend か、server の statement_timeout がアプリの timer より先)', 'PROFIT_INTERNAL', 'INTERNAL_EXTERNAL_CANCEL'],
  ['どの段でも transaction_timeout (25P04・ROLLBACK を送らず接続を捨てる)', 'PROFIT_RESOURCE', 'RESOURCE_TRANSACTION_TIMEOUT'],
  ['取り消しの表 (CANCEL_MAP) に無い組 (例 = 2. の SET LOCAL や 3. の lock の文の 57014)', 'PROFIT_INTERNAL', 'INTERNAL_UNCLASSIFIED_CANCEL'],
].map(([path, code, reason]) => Object.freeze({ path, code, reason })));

/**
 * 🆕 v2 取り消しの分け方 (設計 §3.10「門の関数の契約」の「57014 (query_canceled) の分け方」の表の正本 = R-v3-10 M2・R-v3-11 L-new-2)。
 * 同じ 57014 でも意味が違う → (stage, sqlstate, cancellation_source) で 503 の code / reason を決める。
 *   - cancellation_source はアプリが自分で決める (PostgreSQL の文の文字に頼らない)。アプリの timer・関門の wall-clock の deadline・HTTP の切断のどれかが
 *     発火したら、pg_cancel_backend を送る前に要求の状態に印 cancel_mark = { source, stage } を書く (最初の 1 つだけ・後から変えない)
 *   - **印があれば印の source** (経過時間を見ない) / **印が無ければ unmarked**。印を書いた要求は文が先に終わっても結果を使わずに 503 (接続は捨てる)
 *   - 門 (Gr・_d60_guard) の D6* / 55P03 はこの表でなく HEAVY_GUARD_SQLSTATES の対応 (3a で固定) = classifyCancellation は印が無ければ null を返す
 */
export const CANCELLATION_SOURCES = Object.freeze(['app_statement_budget', 'app_deadline', 'client_disconnect', 'unmarked']);
/** 要求の取引の段 (設計 §3.10「1 回の要求の取引」の 2.〜6.)。CANCEL_MAP の行は 4. と 5. と「どの段でも」だけ = ほかの段の 57014 は表に無い組 */
export const CANCEL_STAGES = Object.freeze(['tx_setup', 'lock_statement', 'load_count', 'month_body', 'commit']);
/**
 * 表の行 (上から順に見る)。'*' = どれでも。sqlstate null = 取り消しの例外が無い (文が先に終わった・文と文の間で deadline が来た)。
 *   respond = false → 応答を作らない (client が居ない・今の契約の 7.) / send_rollback = false → ROLLBACK を送らず接続を捨てる (25P04 = session が終わる)
 *   failed_months = true → 503 の本文に止まった月 (PROFIT_PARTIAL_FAILED) / log = ログの理由 (3 つの組と段だけを出す・文と値は出さない)
 */
export const CANCEL_MAP = Object.freeze([
  ['month_body', '57014', 'app_statement_budget', 'PROFIT_PARTIAL_FAILED', 'MONTH_STATEMENT_TIMEOUT', { failed_months: true, log: 'month_statement_timeout' }],
  ['load_count', '57014', 'app_statement_budget', 'PROFIT_RESOURCE', 'RESOURCE_LOAD_COUNT_TIME', { log: 'load_count_statement_timeout' }],
  ['load_count', '57014', 'app_deadline', 'PROFIT_RESOURCE', 'RESOURCE_LOAD_COUNT_TIME', { log: 'load_count_deadline' }],
  ['load_count', null, 'app_deadline', 'PROFIT_RESOURCE', 'RESOURCE_LOAD_COUNT_TIME', { log: 'load_count_deadline' }],
  ['*', '*', 'client_disconnect', null, null, { respond: false, log: 'client_disconnect' }],
  ['*', '25P04', '*', 'PROFIT_RESOURCE', 'RESOURCE_TRANSACTION_TIMEOUT', { send_rollback: false, log: 'transaction_timeout' }],
  ['load_count', '57014', 'unmarked', 'PROFIT_INTERNAL', 'INTERNAL_EXTERNAL_CANCEL', { log: 'external_cancel' }],
  ['month_body', '57014', 'unmarked', 'PROFIT_INTERNAL', 'INTERNAL_EXTERNAL_CANCEL', { log: 'external_cancel' }],
].map(([stage, sqlstate, source, code, reason, x]) => Object.freeze({
  stage, sqlstate, source, code, reason, respond: x.respond ?? true, send_rollback: x.send_rollback ?? true, failed_months: x.failed_months ?? false, log: x.log,
})));
/** 表に無い組 = 握りつぶして成功・部分の値にしない・rethrow もしない (応答は安全な固定の 503)・ログに 3 つの組だけ */
export const CANCEL_UNCLASSIFIED = Object.freeze({ stage: '*', sqlstate: '*', source: '*', code: 'PROFIT_INTERNAL', reason: 'INTERNAL_UNCLASSIFIED_CANCEL',
  respond: true, send_rollback: true, failed_months: false, log: 'unclassified_cancel' });
/** 取り消しの例外の SQLSTATE (この 2 つと「印あり」だけがこの表の対象) */
export const CANCEL_SQLSTATES = Object.freeze(['57014', '25P04']);

/**
 * 取り消しを (stage, sqlstate, 印) から 1 つの結果に分ける。経過時間は引数に無い (経過時間の近さでは決めない = R-v3-11 L-new-2)。
 *   @param {{ stage: string, sqlstate?: string|null, mark?: { source: string, stage?: string }|null }} x
 *     stage = 例外を受けた (印が無いとき) 段 / mark = 要求の状態の印 (stage があれば印の段で引く = timer を張った段の理由)
 *   @returns 表の行 (CANCEL_MAP の 1 つか CANCEL_UNCLASSIFIED) / null = 取り消しでない (印が無く sqlstate が 57014 / 25P04 でない = 門の対応か FAILURE_PATHS のほかの行)
 * 順: ① 印 client_disconnect = 応答を作らない (client が居ない) ② 25P04 = どの段でも RESOURCE_TRANSACTION_TIMEOUT (ROLLBACK を送らない)
 *     ③ 印あり = 印の source と段で引く (文が先に終わった・ほかの例外で終わった も 57014 と同じに扱う = 結果を使わない) ④ 印なしの 57014 = unmarked で引く
 *     ⑤ どれにも当たらない = CANCEL_UNCLASSIFIED (知らない段・知らない source の印も)
 */
export function classifyCancellation({ stage, sqlstate = null, mark = null } = {}) {
  const hasMark = mark != null;
  const source = hasMark ? mark.source : 'unmarked';
  if (hasMark && (!CANCELLATION_SOURCES.includes(source) || source === 'unmarked')) return CANCEL_UNCLASSIFIED;
  if (source === 'client_disconnect') return CANCEL_MAP.find((e) => e.source === 'client_disconnect');
  if (sqlstate === '25P04') return CANCEL_MAP.find((e) => e.sqlstate === '25P04');
  if (!hasMark && sqlstate !== '57014') return null;
  const st = hasMark && mark.stage != null ? mark.stage : stage;
  if (!CANCEL_STAGES.includes(st)) return CANCEL_UNCLASSIFIED;
  const tries = hasMark && sqlstate === null ? [null, '57014'] : ['57014'];
  for (const s of tries) {
    const hit = CANCEL_MAP.find((e) => (e.stage === '*' || e.stage === st) && (e.sqlstate === '*' || e.sqlstate === s) && (e.source === '*' || e.source === source));
    if (hit) return hit;
  }
  return CANCEL_UNCLASSIFIED;
}
/**
 * 応答に付ける header (200 も 503 も)。AI・画面は正式な値を自分で保存しない。
 * 🚨 今の router の 503 (封じ込め) には付いていない = 契約の定数だけ。**router に触れる PR (遅くとも PR 6) の必須の条件**: 本物の HTTP の応答の header を試験で確かめる
 */
export const REQUIRED_HEADERS = Object.freeze({ 'cache-control': 'no-store' });
/** 1 回の要求で触れる暦月の上限 (受け口の 400 と、この契約の validator の両方で縛る) */
export const MAX_MONTHS = 13;

/**
 * 🆕 v2 failed_months (設計 §5 の 0b-3 の (c) = 2026-10-04 中原さんが推しどおり) = PROFIT_PARTIAL_FAILED の本文 **だけ** に、止まった月を 'YYYY-MM' の配列で。
 * 必須 (1 つ以上)・月の昇順・重複なし・MAX_MONTHS (13) まで・要求の触れる暦月の中。🚨 値 (金額・行・SKU・例外の文) は入れない = 要素は月の形の文字だけ
 */
export const FAILED_MONTHS_CODE = 'PROFIT_PARTIAL_FAILED';
const YM_RE = /^\d{4}-(0[1-9]|1[0-2])$/;
/** 503 の本文に出してよい列 (failed_months は FAILED_MONTHS_CODE のときだけ) */
export const PROFIT_503_BODY_KEYS = Object.freeze(['ok', 'code', 'error', 'reason', 'failed_months']);

/**
 * 503 の本文を作る (固定の文・列挙にない reason は付けない = 上流の例外の文を入れる道が無い)。
 * 🆕 v2: PROFIT_PARTIAL_FAILED は failedMonths ('YYYY-MM' の配列・並べ替えと重複の除きはここでする) が必須。月の形でない要素が 1 つでもある・
 *   空・13 を超える なら、値を出す道を作らないために **PROFIT_INTERNAL / APP_UNEXPECTED** にする (呼び手の誤り)。ほかの code では failedMonths を付けない
 */
export function build503Body(code, reason, failedMonths) {
  const c = Object.hasOwn(PROFIT_503_ERRORS, code) ? code : 'PROFIT_INTERNAL';
  if (c === FAILED_MONTHS_CODE) {
    const ok = Array.isArray(failedMonths) && failedMonths.length > 0 && failedMonths.every((m) => typeof m === 'string' && YM_RE.test(m));
    const months = ok ? [...new Set(failedMonths)].sort() : [];
    if (!ok || months.length > MAX_MONTHS) return build503Body('PROFIT_INTERNAL', 'APP_UNEXPECTED');
    const body = { ok: false, code: c, error: PROFIT_503_ERRORS[c] };
    if (typeof reason === 'string' && PROFIT_503_REASONS[c].includes(reason)) body.reason = reason;
    body.failed_months = months;
    return body;
  }
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
 *   min = 下限 (件数は 0 以上) / max = 上限 / enum = 値の一覧 / hex64 = 64 桁の小文字の 16 進 / sixDp = 小数 0 か 6 桁 / idsAscending = ID の厳密な昇順 (BigInt)
 */
export const LISTING_RESOLUTIONS = Object.freeze(['resolved', 'unresolved']);
export const REFUND_UNITS_STATUSES = Object.freeze(['no_refund', 'estimated_monthly_unit_price', 'estimated_partial_month_unit_price', 'unit_price_missing']);
export const COST_BASES = Object.freeze(['sku_costs', 'observed', 'estimated', 'missing']);
export const COMPOSITION_BASES = Object.freeze(['listing_unresolved', 'missing', 'pre_audit_unverifiable', 'current_after_recorded_change', 'current_no_recorded_change']);
/** 正式な「広告の前」の値を止める理由 (0050 の ok_before) */
export const BEFORE_AD_REASONS = Object.freeze(['finance_incomplete', 'finance_unclassified', 'refund_units_unknown', 'refund_units_partial_month',
  'listing_unresolved', 'composition_missing', 'cost_missing']);
const N0 = Object.freeze({ min: 0 });
/** ID の配列は ID (BigInt) の厳密な昇順 = 重複なし・1 以上 (0050 の array_agg(distinct … order by …) / array_agg(… order by …) と主キー (listing_id, sku_id)・#1602 Codex R5 Low) */
const IDS_ASC = Object.freeze({ idsAscending: true });
export const DAILY_COLUMNS = Object.freeze([
  ['company_id', 'smallint', 'never'], ['mall', 'text', 'never'], ['scope_key', 'text', 'never'], ['economic_date_jst', 'date', 'never'],
  ['listing_id', 'bigint', 'iff_unresolved'], ['seller_sku_norm', 'text', 'iff_resolved'], ['listing_resolution', 'text', 'never', { enum: LISTING_RESOLUTIONS }],
  ['listing_code', 'text', 'iff_unresolved'],
  // 🆕 v2 (PR 2c・設計 19 §6.6.1 の案 (a)) = 0050 の関数の戻りには無い = 3a の包む関数が返す (addedIn: 'v2' = DB の列の突き合わせから外す)
  ['member_seller_skus', 'text[]', 'never', { memberSkus: true, addedIn: 'v2' }],
  ['received_listing_ids', 'bigint[]', 'never', IDS_ASC], ['received_listing_unresolved_count', 'integer', 'never', N0], ['ad_received_listing_ids', 'bigint[]', 'never', IDS_ASC],
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
  ['missing_cost_sku_ids', 'bigint[]', 'never', IDS_ASC], ['cost_sku_cost_ids', 'bigint[]', 'never', IDS_ASC], ['cost_observed_ids', 'bigint[]', 'never', IDS_ASC],
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
/** 今の DB の関数 (0049 / 0050 の mart.amazon_profit_daily_range) の戻りの列 = DAILY_COLUMNS から v2 で足した列を除いたもの (試験が pg_proc と突き合わせる) */
export const DAILY_DB_COLUMNS = Object.freeze(DAILY_COLUMNS.filter((c) => !c.addedIn));

/**
 * 🆕 v2 member_seller_skus の規則 (設計 13 §3.10 の PR の表の 2c・設計 19 §6.6.1 / §6.2.5 の「文字の配列」):
 *   - 中身 = その行の粒度 (解決 = 出品 / 未解決 = 正規化 SKU) にまとまった、その日の財務の行で **受け取った seller SKU** を trim + 小文字にしたもの
 *     (利益の行と同じ計算・同じスナップショットで解決 = F4-5 の「まとめた SKU」の構成の SKU)
 *   - 型 = 文字の配列・null にならない。**空の配列 [] = その日のその粒度に財務の行が無い** (広告だけ・Easy Ship だけの行)。⇔ order_rows = 0
 *   - 並び = UTF-8 の bytes の厳密な昇順 (= 重複なし・locale の比べを使わない・JS は Buffer.compare)
 *   - 要素 = 空でない・前後に空白なし (trim の空白 = MEMBER_SKU_EDGE_SPACE・core.norm_code と 0054 の amazon_map_key_problem と同じ集合)・
 *     ASCII の大文字なし (lower)・MAX_MEMBER_SKU_CHARS 文字まで (0054 の対応の表の seller_sku と同じ 255)
 *   - 上限 = 1 行に MAX_MEMBER_SELLER_SKUS 個まで (直接の一致 = 1 つの粒度の SKU は全部 core.norm_code が同じ = 全角・半角・空白・ダッシュの違いだけ)
 *   - 同じ日の行の間で同じ seller SKU は 1 つの粒度にだけ (trim + 小文字が同じなら core.norm_code も同じ = 同じ粒度・F4-5 の「財務の全部の SKU がちょうど 1 つの粒度」)
 */
export const MAX_MEMBER_SELLER_SKUS = 100;
export const MAX_MEMBER_SKU_CHARS = 255;
// eslint-disable-next-line no-control-regex
export const MEMBER_SKU_EDGE_SPACE = /^[\u0009-\u000d \u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]|[\u0009-\u000d \u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]$/;
/** member_seller_skus の 1 つの配列の形の違反の文 (空 = 違反なし) */
export function memberSkuErrors(v) {
  if (!Array.isArray(v)) return ['配列でない'];
  const e = [];
  if (v.length > MAX_MEMBER_SELLER_SKUS) e.push(`${v.length} 個 (上限 ${MAX_MEMBER_SELLER_SKUS})`);
  v.forEach((s, k) => {
    if (typeof s !== 'string' || s.length === 0) { e.push(`[${k}] 空でない文字でない`); return; }
    if ([...s].length > MAX_MEMBER_SKU_CHARS) e.push(`[${k}] ${MAX_MEMBER_SKU_CHARS} 文字を超える`);
    if (MEMBER_SKU_EDGE_SPACE.test(s)) e.push(`[${k}] 前後に空白 (trim していない)`);
    if (/[A-Z]/.test(s)) e.push(`[${k}] 大文字 (小文字にしていない)`);
    if (k > 0 && typeof v[k - 1] === 'string' && Buffer.compare(Buffer.from(v[k - 1], 'utf8'), Buffer.from(s, 'utf8')) >= 0) e.push(`[${k}] UTF-8 の bytes の厳密な昇順でない (重複・順の違い)`);
  });
  return e;
}

/** months[] の 1 つ (月の metadata の関数 mart.amazon_profit_month_meta から作る・0 行の月にもある) */
export const MONTH_KEYS = Object.freeze(['month_start', 'period_from', 'period_to', 'finance_status', 'has_finance_rows', 'finance_month_settled',
  'finance_coverage_generation', 'finance_source_revision',
  // 🆕 v2 = core.finance_coverage_token(company_id, mall, scope_key, month_start) の値 (64 桁の小文字の 16 進・null にならない・設計 19 §6.2.1 ③ の coverage_token と同じ関数)
  'finance_coverage_token',
  'calculation_version', 'calculated_at']);
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
    // 🆕 v2: coverage の token = 64 桁の小文字の 16 進 (null にならない = 部品の日が月を覆わなければ関数が例外 = 503 MONTH_META_FAILED)
    if (typeof m.finance_coverage_token !== 'string' || !HEX64.test(m.finance_coverage_token)) errs.push(`${p}.finance_coverage_token: 64 桁の小文字の 16 進でない`);
    // 部品の period_from / period_to は月の中の日 = 月が違えば token の入力の文字も違う → 同じ応答の 2 つの月で同じ token は写し間違い
    else if (body.months.slice(0, i).some((q) => isObj(q) && q.finance_coverage_token === m.finance_coverage_token)) errs.push(`${p}.finance_coverage_token: 前の月と同じ (月ごとに違う値のはず)`);
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
  // 0050 の missing_ids = 原価が分からない部品の SKU (uc は解決・構成ありの行だけ) = 空でない ⇔ g_cost (#1602 Codex R5 の突き合わせ)
  iff(Array.isArray(r.missing_cost_sku_ids) && r.missing_cost_sku_ids.length > 0, has(reasons, 'cost_missing'), '原価の無い SKU (missing_cost_sku_ids) がある ⇔ 理由 cost_missing');
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
  // 🆕 v2: member は財務の行から作る = 財務の子がある (order_rows > 0) ⇔ member が空でない (広告だけ・Easy Ship だけの行は [])
  if (Array.isArray(r.member_seller_skus) && Number.isSafeInteger(r.order_rows)) {
    iff(r.order_rows > 0, r.member_seller_skus.length > 0, '財務の行がある (order_rows > 0) ⇔ member_seller_skus が空でない');
  }
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
      if (c.idsAscending && v.some((x, k) => BigInt(x) < 1n || (k > 0 && BigInt(x) <= BigInt(v[k - 1])))) errs.push(`${p}.${c.name}: ID の厳密な昇順 (BigInt・重複なし・1 以上) でない`);
      if (c.memberSkus) for (const m of memberSkuErrors(v)) errs.push(`${p}.${c.name}: ${m}`);
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
  errs.push(...requestLevelErrors(body.rows));
  errs.push(...memberAcrossRowsErrors(body.rows));
  return errs;
}
/** 🆕 v2: 同じ日の行の間で同じ seller SKU (trim + 小文字) は 1 つの粒度にだけ (F4-5 の「財務の全部の SKU がちょうど 1 つの粒度」) */
function memberAcrossRowsErrors(rows) {
  const e = [], seen = new Map();
  rows.forEach((r, i) => {
    if (!isObj(r) || !Array.isArray(r.member_seller_skus)) return;
    for (const s of r.member_seller_skus) {
      if (typeof s !== 'string') continue;
      const k = JSON.stringify([String(r.economic_date_jst), s]);
      if (seen.has(k)) e.push(`$.rows[${i}].member_seller_skus: 同じ日の行 ${seen.get(k)} にもある seller SKU (1 つの SKU は 1 つの粒度にだけ)`);
      else seen.set(k, i);
    }
  });
  return e;
}
/**
 * 要求全体で固定の値は全部の行で同じ (#1602 Codex R4 M1)。0050 の最後の SELECT で要求全体に固定の列 =
 *   p_company_id・p_mall・p_scope_key・'current'・'amazon_profit_v1'・observed_generation (同じ snapshot の max(generation))・
 *   mart.amazon_profit_composition_audit_since() (引数なしの immutable)・statement_timestamp()
 *   → mall・scope_key・master_basis・calculation_version・calculated_at は上 (要求) の値と照合済み。残りの 3 つを行の間でそろえる
 *   (月ごとの計算も同じ REPEATABLE READ の snapshot = observed_generation も月をまたいで同じ)
 */
export const REQUEST_LEVEL_FIELDS = Object.freeze(['company_id', 'observed_generation', 'composition_audit_since']);
function requestLevelErrors(rows) {
  const e = [];
  const okRows = rows.map((r, i) => [r, i]).filter(([r]) => isObj(r));
  if (!okRows.length) return e;
  const [r0, i0] = okRows[0];
  for (const [r, i] of okRows.slice(1)) {
    for (const k of REQUEST_LEVEL_FIELDS) {
      if (JSON.stringify(r[k]) !== JSON.stringify(r0[k])) e.push(`$.rows[${i}].${k}: 行 ${i0} と違う (要求全体で固定の値)`);
    }
  }
  if (!Number.isSafeInteger(r0.company_id) || r0.company_id < 1) e.push(`$.rows[${i0}].company_id: 1 以上の整数でない`);
  return e;
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

/**
 * 503 の本文を確かめる (今の封じ込めの PROFIT_ROUTE_DISABLED もこの形)。error は code の固定の文・reason は code ごとの列挙だけ。
 * 🆕 v2: failed_months は PROFIT_PARTIAL_FAILED のときだけ・必須・'YYYY-MM' の厳密な昇順・13 まで。request ({ from, to }) を渡せば要求の触れる暦月の中かも確かめる
 */
export function validate503Body(body, request) {
  return guard(() => {
    const errs = [];
    if (!isObj(body)) return ['$: object でない'];
    findSecrets(body, errs);
    for (const k of Object.keys(body)) if (!PROFIT_503_BODY_KEYS.includes(k)) errs.push(`$.${k}: 503 に出さない列 (値・部分の結果・Render の本文を返さない)`);
    if (body.ok !== false) errs.push('$.ok: false でない');
    if (!PROFIT_503_CODES.includes(body.code)) { errs.push(`$.code: 一覧に無い`); return errs; }
    if (body.error !== PROFIT_503_ERRORS[body.code]) errs.push('$.error: code の固定の文と違う (上流の例外・本文を入れない)');
    if (Object.hasOwn(body, 'reason') && !PROFIT_503_REASONS[body.code].includes(body.reason)) errs.push('$.reason: この code の列挙に無い');
    if (body.code !== FAILED_MONTHS_CODE) {
      if (Object.hasOwn(body, 'failed_months')) errs.push(`$.failed_months: ${FAILED_MONTHS_CODE} のときだけ`);
      return errs;
    }
    const fm = body.failed_months;
    if (!Array.isArray(fm) || fm.length === 0) { errs.push('$.failed_months: 止まった月の配列 (1 つ以上) が無い'); return errs; }
    if (fm.length > MAX_MONTHS) errs.push(`$.failed_months: ${fm.length} 個 (上限 ${MAX_MONTHS})`);
    fm.forEach((m, k) => {
      if (typeof m !== 'string' || !YM_RE.test(m)) errs.push(`$.failed_months[${k}]: 'YYYY-MM' の形でない (値・行・例外の文を入れない)`);
      else if (k > 0 && !(typeof fm[k - 1] === 'string' && fm[k - 1] < m)) errs.push(`$.failed_months[${k}]: 月の厳密な昇順でない (重複・順の違い)`);
    });
    if (request != null) {
      const touched = isValidDate(request.from) && isValidDate(request.to) && request.from <= request.to ? monthsOf(request.from, request.to).map((x) => x.month_start.slice(0, 7)) : null;
      if (!touched) errs.push('request: from / to が日付でない・逆');
      else for (const m of fm) if (typeof m === 'string' && !touched.includes(m)) errs.push(`$.failed_months: ${m} は要求の触れる暦月の外`);
    }
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
