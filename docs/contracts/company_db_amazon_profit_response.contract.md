# データ契約: Amazon の利益の受け口の最終の JSON (D-60 v3.4・PR 2b / 🆕 v2 = PR 2c)

**状態**: 固定 (2026-10-03・DB の実装より先に) / 🆕 **v2 (2026-10-04・PR 2c)** = `months[].finance_coverage_token`・日の行の `member_seller_skus`・57014 / 25P04 の分け方の reason・`PROFIT_PARTIAL_FAILED` の `failed_months` (版の履歴 = §8) / **受け口は 503 のまま** (開けるのは §3.10 の PR 6 だけ・DB の関数は 3a で作る)
**設計**: AI_reference『CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』§3.10「応答の契約」「長い期間の応答の列ごとの規則」(Codex R-D60-v3-4 M5・R-D60-v3-5 Low・#1602 R1)
**正本 (機械)**: `apps/company-db/profit/response-contract.mjs` (この文書の表は、そこから作った・試験で突き合わせる)
**fixture**: `scripts/fixtures/amazon-profit-response/` / **試験**: `scripts/test-company-db-profit-response-contract.mjs`

## 1. 受け口と形

| 受け口 | 200 の形 |
|---|---|
| `GET /apps/company-db/sync/amazon-profit/totals?mall&scope&from&to` | `{ ok, contract, kind: "totals", mall, scope, from, to, master_basis, calculation_version, calculated_at, master_as_of, total: {期間の全体の 1 行}, months: [...] }` |
| `GET /apps/company-db/sync/amazon-profit/daily?mall&scope&from&to` | `{ ok, contract, kind: "daily", mall, scope, from, to, master_basis, calculation_version, calculated_at, master_as_of, rows: [日 × 出品], months: [...] }` |

- 旧 totals (#1559) の「日・月・期間の中の月の小計・期間の合計」の行 (`row_kind`) は **返さない**。期間の全体の 1 行 + `months[]`
- `contract` = `amazon_profit_response_v2` (🆕 PR 2c で v1 から上げた。形を変えたら版を上げて §8 に 1 行足す)
- 🚨 v2 は **PR 5 (校正) と PR 6 (開ける) の前** に入れる = 開けた応答に最初から含める (後から足すと校正と契約の版をやり直す・設計 19 §6.6.1)
- **header の no-store は PR 6 で付ける**: 開いた後の応答 (200 も 503 も) は `Cache-Control: no-store` (AI・画面は正式な値を自分で保存しない)。**今の封じ込めの 503 には付いていない**。router に触れる PR (遅くとも PR 6) の必須の条件 = 本物の HTTP の応答の header を試験で確かめる
- 長い期間は月ごとに計算してアプリで足す。**1 回の要求で触れる暦月は最大 13** = 受け口が 400 で拒み、この契約の validator も 14 か月以上の応答を違反にする (2 重)
- 1 か月でも失敗したら部分の値は返さず 503

## 2. 値の書き方

| PG の型 | JSON |
|---|---|
| bigint (金額・数・ID) | 10 進の文字列 (JS の Number に入れない・`"-0"` は無い) |
| bigint[] | その文字列の配列 |
| numeric (金額) | 小数 2 桁の文字列。丸めは PostgreSQL の `round(numeric, 2)` = 0 から遠い方へ (half away from zero)。`"-0.00"` は無い |
| numeric (`units_*_unrounded`) | 小数 0 か 6 桁の文字列 (返品なし = `"0"`) |
| integer / smallint | 数 |
| date / date[] | `"YYYY-MM-DD"` (実在する日) / その配列 |
| timestamptz | UTC の ISO (ミリ秒・Z・実在する時刻) |
| text / text[] / jsonb / boolean | 文字 / 文字の配列 / object / true・false |

- **`calculated_at`** = 要求の最初の文 (共通の lock を取る SELECT) の `statement_timestamp()` の **1 つの値**。上・`total`・`rows` の全部・`months` の全部で同じ (月の関数の値は捨てて置き換える・R-D60-v3-5 Low)。`master_as_of = calculated_at` (D-64)
- **raw の列** (丸める前の値・名前が `_raw` で終わる) は **どこにも出さない**
- **理由の決まった順** = `finance_incomplete` → `finance_unclassified` → `refund_units_unknown` → `refund_units_partial_month` → `listing_unresolved` → `composition_missing` → `cost_missing` → `ad_not_collected` → `ad_missing` → `ad_legacy_unverified` → `ad_unresolved` (0049 / 0050 の関数の順・totals は `ad_unresolved` を入れない)
- **状態の順位** (後ろほど弱い): 広告 = `complete` < `verified_legacy` < `legacy_incomplete` < `missing` < `not_collected` / 財務 = `complete` < `provisional` < `missing`
- validator は形の合う実在しない時刻 (`2026-99-99T99:99:99.999Z`) や壊れた入力でも例外を投げず、いつも `{ ok: false }` を返す

## 3. months[] (月の metadata の関数から作る・財務の行が 0 の月にもある)

| 列 | JSON | 意味 |
|---|---|---|
| `month_start` | "YYYY-MM-01" | 暦月 |
| `period_from` / `period_to` | "YYYY-MM-DD" | その月の中で要求が触れる範囲 |
| `finance_status` | `complete` / `provisional` / `missing` | その月の日の財務の状態の一番弱いもの |
| `has_finance_rows` | boolean | その月に財務の行があるか |
| `finance_month_settled` | boolean | `core.finance_month_settled` |
| `finance_coverage_generation` / `finance_source_revision` | 文字列 / null | その月の coverage の世代と版 (期間の行では常に null)。🆕 v2 から **表示の値** (無効化の判定には下の token を使う) |
| 🆕 `finance_coverage_token` | 64 桁の小文字の 16 進 (null にならない) | `core.finance_coverage_token(company_id, mall, scope_key, month_start)` の値 = 設計 19 §6.2.1 ③ の `coverage_token` と **同じ関数** (source ごとの coverage の部品 + 月の 2 つの key の SHA-256・3a で作る・式を 2 つにしない)。月の途中で source が 2 つある月 (世代と版は null) でも、どちらの source の coverage が変わっても値が変わる。財務の行が 0 の月にもある。月が違えば部品の日が違う = 同じ応答の 2 つの月で同じ値なら違反。部品の日が月を覆わなければ関数が例外 = 503 `PROFIT_PARTIAL_FAILED` / `MONTH_META_FAILED` |
| `calculation_version` | 文字 | 全部の月で同じ (違えば 503 `PROFIT_VERSION_MISMATCH`) |
| `calculated_at` | UTC の ISO | 上と同じ 1 つの値 |

## 4. 503 の code の一覧

本文 = `{ ok: false, code, error, reason?, failed_months? }` だけ (🆕 v2 = `failed_months` は `PROFIT_PARTIAL_FAILED` だけ・下)。🚨 **`error` は code ごとの固定の文と完全に一致** し、**`reason` は code ごとの列挙だけ** (#1602 Codex R1 H1)。値・部分の結果・Render の応答の本文・上流の例外の文・鍵・接続の文字列を入れる道を契約の形で無くす。本文は `build503Body(code, reason)` で作る (列挙に無い reason は付けない・知らない code は `PROFIT_INTERNAL`)。念のため秘密らしい文字 (`Bearer `・`rnd_…`・`postgres://`・`RENDER_API_KEY`・`x-sync-key`・`password=`・`authorization`) があれば違反。

| code | error (固定の文) | reason (列挙) |
|---|---|---|
| `PROFIT_ROUTE_DISABLED` | Amazon の利益の読む口は 2026-10-01 から止めています (封じ込め)。93 日分の計算で本番の Postgres が落ちたため。設計『CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』§3.10 (D-60 v3.1 = 1 か月ずつ計算) で作り直すまで使えません。 | (付けない) |
| `PROFIT_NOT_CALIBRATED` | 承認済みの校正の記録が無いか、今の DB と合わないので計算しません。 | `CALIBRATION_MISSING` `CALIBRATION_NOT_APPROVED` `CALIBRATION_REVOKED` `CALIBRATION_FINGERPRINT_MISMATCH` `CALIBRATION_INPUTS_CHANGED` `CALIBRATION_PLAN_CHANGED` |
| `PROFIT_BUSY` | ほかの重い処理の最中なので計算しません。しばらくしてからもう一度。 | `LOCK_NOT_AVAILABLE` |
| `PROFIT_RESOURCE` | DB のメモリ・一時ファイル・負荷の条件を満たさないので計算しません。 | `RESOURCE_MEMORY` `RESOURCE_TEMP_FILES` `RESOURCE_PROCESS_INPUTS` `RESOURCE_LOAD_FACTOR` `RESOURCE_COUNT_PLAN` `RESOURCE_TEMP_FILE_LIMIT_NOT_FINITE` `RESOURCE_PRIVILEGES` 🆕 `RESOURCE_LOAD_COUNT_TIME` `RESOURCE_TRANSACTION_TIMEOUT` |
| `PROFIT_METRICS_UNAVAILABLE` | Render の metrics が読めないので計算しません。 | `METRICS_CONFIG` `METRICS_AUTH` `METRICS_RATE_LIMITED` `METRICS_UPSTREAM` `METRICS_HTTP` `METRICS_TIMEOUT` `METRICS_NETWORK` `METRICS_SHAPE` `METRICS_NOT_INTEGER` `METRICS_EMPTY` `METRICS_DUPLICATE_SERIES` `METRICS_DUPLICATE_POINT` `METRICS_LABEL_MISSING` `METRICS_WRONG_RESOURCE` `METRICS_UNIT` `METRICS_NEGATIVE` `METRICS_FUTURE` `METRICS_STALE` `METRICS_PAIR_SKEW` `METRICS_ZERO_LIMIT` `METRICS_INCONSISTENT` `METRICS_INTERNAL` |
| `PROFIT_DB_UNAVAILABLE` | DB に接続できないか、計算の取引を始められない / 終えられないので、どの値も返しません。 | `DB_CONNECT` `DB_BEGIN` `DB_SET_LOCAL` `DB_SETTING_READBACK` `DB_LOCK_STATEMENT` `DB_COMMIT` `DB_OUTCOME_UNKNOWN` `DB_UNEXPECTED` |
| `PROFIT_PARTIAL_FAILED` | 1 か月の計算に失敗したので、どの値も返しません。 | `MONTH_META_FAILED` `MONTH_CALC_FAILED` `MONTH_STATEMENT_TIMEOUT` (🆕 v2 = 本文に `reason` も `failed_months` も必須) |
| `PROFIT_VERSION_MISMATCH` | 月で calculation_version / master_basis が違うので計算できません。 | `CALCULATION_VERSION` `MASTER_BASIS` |
| `PROFIT_INTERNAL` | 受け口の中の思わぬ失敗なので、どの値も返しません。 | `APP_UNEXPECTED` 🆕 `INTERNAL_EXTERNAL_CANCEL` `INTERNAL_UNCLASSIFIED_CANCEL` |

- `PROFIT_ROUTE_DISABLED` の文は今の router (#1570) の文そのもの。版の「v3.1」は PR 6 で router と一緒に直す (この表も同じ PR で)
- 🆕 **`failed_months`** (v2・設計 §5 の 0b-3 の (c) = 2026-10-04 中原さんが推しどおり) = `PROFIT_PARTIAL_FAILED` の本文 **だけ** に、止まった月を `"YYYY-MM"` の配列で (例 `{ ok: false, code: "PROFIT_PARTIAL_FAILED", error: "…", reason: "MONTH_CALC_FAILED", failed_months: ["2026-07"] }`)。**必須** (1 つ以上)・月の厳密な昇順 (重複なし)・`MAX_MONTHS` (13) まで・要求の触れる暦月の中。🚨 **値は入れない** = 要素は月の形の文字だけ (金額・行・SKU・例外の文・別の形の日付は違反)。ほかの code の本文に付いていれば違反。`build503Body(code, reason, failedMonths)` は並べ替えと重複の除きをし、月の形でない要素が 1 つでもある・空・13 を超えるときは値を出す道を作らないため `PROFIT_INTERNAL` / `APP_UNEXPECTED` にする。`validate503Body(body, { from, to })` は要求を渡せば暦月の中かも確かめる
- 🆕 `PROFIT_PARTIAL_FAILED` は `reason` も **必須** (#1615 Codex R1 M2・`REASON_REQUIRED_CODES`) = 3 つの reason (`MONTH_META_FAILED` / `MONTH_CALC_FAILED` / `MONTH_STATEMENT_TIMEOUT`) のどれかを必ず持つ。`build503Body` は reason が無い・列挙に無いときも `PROFIT_INTERNAL` / `APP_UNEXPECTED` に落とし (reason の無い `PROFIT_PARTIAL_FAILED` を作らない)、`validate503Body` は reason の無い本文を拒む。ほかの code の reason は今までどおり任意
- `PROFIT_METRICS_UNAVAILABLE` の reason は metrics の client (PR 2 #1600 の `REASONS`) と同じ一覧。2 つの PR がそろったら試験が一致を確かめる

## 4b. 全部の失敗の経路 → 503 の code (#1602 Codex R1 M2・設計 §3.10「1 回の要求の取引」の 0.〜7.)

| 経路 | code | reason |
|---|---|---|
| 封じ込め (PR 6 より前) | `PROFIT_ROUTE_DISABLED` | - |
| 0. metrics: Render の API の 200 でない応答 (400・401・403・429・5xx ほか全部)・網・timeout・形の違い | `PROFIT_METRICS_UNAVAILABLE` | `METRICS_*` |
| 1. DB の接続の失敗 | `PROFIT_DB_UNAVAILABLE` | `DB_CONNECT` |
| 2. BEGIN READ ONLY REPEATABLE READ の失敗 | `PROFIT_DB_UNAVAILABLE` | `DB_BEGIN` |
| 2. SET LOCAL の失敗 | `PROFIT_DB_UNAVAILABLE` | `DB_SET_LOCAL` |
| 2. 設定の読み返しが違う / 読めない | `PROFIT_DB_UNAVAILABLE` | `DB_SETTING_READBACK` |
| 3. 最初の文 (lock + statement_timestamp) の失敗 | `PROFIT_DB_UNAVAILABLE` | `DB_LOCK_STATEMENT` |
| 3. 共通の lock が取れない | `PROFIT_BUSY` | `LOCK_NOT_AVAILABLE` |
| 4. lock の後の metrics の鮮度が 2 分を超えた | `PROFIT_METRICS_UNAVAILABLE` | `METRICS_STALE` |
| 4. 校正の記録が無い・未承認・revoked・fingerprint / 材料 / 実行計画が違う | `PROFIT_NOT_CALIBRATED` | `CALIBRATION_*` |
| 4. 資源の関門 (メモリ・一時ファイル・process の数の材料・負荷の要因・数え上げの計画・temp_file_limit・権限) | `PROFIT_RESOURCE` | `RESOURCE_*` |
| 4. 関門の中の予期しない DB の例外 (月の計算の前) | `PROFIT_DB_UNAVAILABLE` | `DB_UNEXPECTED` |
| 5. 月の metadata の関数の失敗 | `PROFIT_PARTIAL_FAILED` | `MONTH_META_FAILED` |
| 5. 月の包む関数の失敗 | `PROFIT_PARTIAL_FAILED` | `MONTH_CALC_FAILED` |
| 5. 月の計算の statement_timeout (57014・アプリの timer の印 app_statement_budget) | `PROFIT_PARTIAL_FAILED` | `MONTH_STATEMENT_TIMEOUT` |
| 5. calculation_version / master_basis が月で違う | `PROFIT_VERSION_MISMATCH` | `CALCULATION_VERSION / MASTER_BASIS` |
| 6. COMMIT の失敗 | `PROFIT_DB_UNAVAILABLE` | `DB_COMMIT` |
| 6. 結果が分からない (接続が切れた・ROLLBACK も失敗 = 接続を捨てる) | `PROFIT_DB_UNAVAILABLE` | `DB_OUTCOME_UNKNOWN` |
| 応答を作る所 (アプリ) の思わぬ例外 | `PROFIT_INTERNAL` | `APP_UNEXPECTED` |
| 🆕 4. 負荷の数え上げの文の取り消し (57014・アプリの timer の印 app_statement_budget) | `PROFIT_RESOURCE` | `RESOURCE_LOAD_COUNT_TIME` |
| 🆕 4. 負荷の数え上げの wall-clock の deadline (印 app_deadline・57014 か取り消し無し) | `PROFIT_RESOURCE` | `RESOURCE_LOAD_COUNT_TIME` |
| 🆕 4.・5. 印の無い取り消し (57014 unmarked = 人・見張りの pg_cancel_backend か、server の statement_timeout がアプリの timer より先) | `PROFIT_INTERNAL` | `INTERNAL_EXTERNAL_CANCEL` |
| 🆕 どの段でも transaction_timeout (25P04・ROLLBACK を送らず接続を捨てる) | `PROFIT_RESOURCE` | `RESOURCE_TRANSACTION_TIMEOUT` |
| 🆕 取り消しの表 (CANCEL_MAP) に無い組 (例 = 2. の SET LOCAL や 3. の lock の文の 57014) | `PROFIT_INTERNAL` | `INTERNAL_UNCLASSIFIED_CANCEL` |

- HTTP の切断 (7.) は応答を返さない (別の接続から `pg_cancel_backend`・接続を pool に戻さない) = 503 の対象でない (🆕 v2 = §4c の `client_disconnect` の行)
- Render の metrics の API は 200 でない応答を **全部** `PROFIT_METRICS_UNAVAILABLE` にする (公式に列挙の 400 を含む)
- `PROFIT_PARTIAL_FAILED` の経路 (5. の 3 つ) は全部、本文に `reason` (その経路の reason・🆕 #1615 R1 M2 で必須) と `failed_months` (止まった月) を付ける

## 4c. 🆕 取り消しの分け方 (v2・設計 §3.10「門の関数の契約」の「57014 (query_canceled) の分け方」の表・R-v3-10 M2・R-v3-11 L-new-2)

正本 (機械) = `CANCEL_MAP`・`classifyCancellation({ stage, sqlstate, mark })`・golden = `scripts/fixtures/amazon-profit-response/cancel-cases.json`。同じ 57014 でも意味が違う (文の `statement_timeout`・アプリの取り消し・人や見張りの `pg_cancel_backend`) → **`(stage, sqlstate, cancellation_source)`** で 503 の code / reason を決める。

- `cancellation_source` = `app_statement_budget` / `app_deadline` / `client_disconnect` / `unmarked`。**アプリが自分で決める** (PostgreSQL の文の文字に頼らない)。アプリの timer (server の `statement_timeout` より 500ms 短い)・関門の wall-clock の deadline・HTTP の切断のどれかが発火したら、`pg_cancel_backend` を送る **前に** 要求の状態に印 `cancel_mark = { source, stage }` を書く (最初の 1 つだけ・後から変えない)
- **印があれば印の source** (経過時間を見ない・印に段があれば印の段で引く) / **印が無ければ `unmarked`**。印を書いた要求は文が先に終わっても (ほかの例外で終わっても) 結果を使わずに 503 (接続は捨てる)
- `stage` = 要求の取引の段 `tx_setup` (2.) / `lock_statement` (3.) / `load_count` (4.) / `month_body` (5.) / `commit` (6.)
- 見る順 (code / reason / 応答 / ログ) = ⓪ 知らない印 (source が一覧に無い・`unmarked` と書いた・中身が無い) = 表に無い組 ① 印 `client_disconnect` (応答を作らない) ② `25P04` (どの段でも) ③ 印あり (印の source と段) ④ 印なしの 57014 (`unmarked`) ⑤ どれにも当たらない = 表に無い組。ROLLBACK はこの順と別に決める (下の表の後の規則)。印が無く sqlstate が 57014 / 25P04 でなければ `null` = 取り消しでない (門の `D6*`・`55P03` は `HEAVY_GUARD_SQLSTATES` の対応 = 3a で固定)

| stage | sqlstate | cancellation_source | code / reason | 応答 | ROLLBACK | 本文の failed_months | ログの理由 |
|---|---|---|---|---|---|---|---|
| `month_body` | `57014` | `app_statement_budget` | `PROFIT_PARTIAL_FAILED` / `MONTH_STATEMENT_TIMEOUT` | 503 | 送る | 付ける | `month_statement_timeout` |
| `load_count` | `57014` | `app_statement_budget` | `PROFIT_RESOURCE` / `RESOURCE_LOAD_COUNT_TIME` | 503 | 送る | - | `load_count_statement_timeout` |
| `load_count` | `57014` | `app_deadline` | `PROFIT_RESOURCE` / `RESOURCE_LOAD_COUNT_TIME` | 503 | 送る | - | `load_count_deadline` |
| `load_count` | (無し) | `app_deadline` | `PROFIT_RESOURCE` / `RESOURCE_LOAD_COUNT_TIME` | 503 | 送る | - | `load_count_deadline` |
| (どれでも) | (どれでも) | `client_disconnect` | - | **作らない** (client が居ない) | 送る (`25P04` なら送らない = 接続を捨てる・下の規則) | - | `client_disconnect` |
| (どれでも) | `25P04` | (どれでも) | `PROFIT_RESOURCE` / `RESOURCE_TRANSACTION_TIMEOUT` | 503 | **送らない** (session が終わる = 接続を捨てる・🆕 印 `client_disconnect`・知らない印で別の行に当たっても = 下の規則) | - | `transaction_timeout` |
| `load_count` | `57014` | `unmarked` | `PROFIT_INTERNAL` / `INTERNAL_EXTERNAL_CANCEL` | 503 | 送る | - | `external_cancel` (🚨 誰かが計算を止めたか、server の timeout が先) |
| `month_body` | `57014` | `unmarked` | `PROFIT_INTERNAL` / `INTERNAL_EXTERNAL_CANCEL` | 503 | 送る | - | `external_cancel` |
| **上の表に無い組** (例 = `tx_setup` や `lock_statement` の 57014・知らない段・知らない source の印) | | | `PROFIT_INTERNAL` / `INTERNAL_UNCLASSIFIED_CANCEL` | 503 (握りつぶして成功・部分の値にしない・rethrow もしない) | 送る (`25P04` なら送らない) | - | `unclassified_cancel` (3 つの組だけ・文と値は出さない) |

- 🆕 **respond と send_rollback は別々に決める** (#1615 Codex R1 M1): 応答 = 印 `client_disconnect` なら sqlstate に関係なく **作らない** / ROLLBACK = sqlstate が `NO_ROLLBACK_SQLSTATES` (`25P04`) なら、上の表のどの行 (表に無い組も) に当たっても・印の正しさに関係なく **送らない** (session が終わっている = 接続を捨てる)。例 = 印 `client_disconnect` × `25P04` → 応答なし・ROLLBACK なし / 知らない印 × `25P04` → `PROFIT_INTERNAL` / `INTERNAL_UNCLASSIFIED_CANCEL` の 503・ROLLBACK なし。`classifyCancellation` は当たった行の「送らない」版 (send_rollback だけ違う凍結した object・同じ入力には同じ参照) を返す。golden = `cancel-cases.json` の「表 4 × 表 6」「表 8 × 表 6」の行

## 5. 付録 A: /totals の期間の行の全部の列の分類 (型の全部の分類の表)

列 = `mart.amazon_profit_day_totals_range` の戻り (0049) と同じ並び。試験が pg_proc と突き合わせる (列が増えたら分類を足すまで落ちる)。

| 列 | PG の型 | 規則 | 期間の値の作り方 |
|---|---|---|---|
| `row_kind` | text | `omit` | 出さない (旧 totals の行の種類の列) |
| `period_from` | date | `request_bound` | 要求の from / to |
| `period_to` | date | `request_bound` | 要求の from / to |
| `economic_date_jst` | date | `omit` | 出さない (旧 totals の行の種類の列) |
| `month_start` | date | `omit` | 出さない (旧 totals の行の種類の列) |
| `day_count` | integer | `int_sum` | integer を足す (JSON の数・件数なので 0 以上) |
| `day_finance_status` | text | `null_in_period` | 期間の行では常に null (月ごとは months[]) |
| `complete_days` | integer | `int_sum` | integer を足す (JSON の数・件数なので 0 以上) |
| `resolved_rows` | integer | `int_sum` | integer を足す (JSON の数・件数なので 0 以上) |
| `unresolved_rows` | integer | `int_sum` | integer を足す (JSON の数・件数なので 0 以上) |
| `units_ordered` | bigint | `bigint_sum` | BigInt で足す (文字列) |
| `units_net_sold` | bigint | `bigint_sum` | BigInt で足す (文字列) |
| `sales_principal_jpy` | bigint | `bigint_sum` | BigInt で足す (文字列) |
| `sales_tax_jpy` | bigint | `bigint_sum` | BigInt で足す (文字列) |
| `profit_before_cogs_jpy` | bigint | `bigint_sum` | BigInt で足す (文字列) |
| `cogs_jpy` | bigint | `bigint_sum_null` | BigInt で足す・1 か月でも null なら null |
| `easy_ship_alloc_jpy` | bigint | `bigint_sum` | BigInt で足す (文字列) |
| `easy_ship_unallocated_jpy` | bigint | `bigint_sum` | BigInt で足す (文字列) |
| `easy_ship_unallocated_count` | integer | `int_sum` | integer を足す (JSON の数・件数なので 0 以上) |
| `ad_status` | text | `state_rank` | 一番弱い状態 (順位の表) |
| `ad_cost_total` | numeric | `decimal_sum_null` | 同じ・1 か月でも null なら null |
| `ad_cost_allocated` | numeric | `decimal_sum` | 月の raw を Decimal で足して最後に 1 回 小数 2 桁に丸める |
| `ad_cost_unresolved` | numeric | `decimal_sum` | 月の raw を Decimal で足して最後に 1 回 小数 2 桁に丸める |
| `ad_unresolved_rows` | integer | `int_sum` | integer を足す (JSON の数・件数なので 0 以上) |
| `account_fee_cost_jpy` | bigint | `bigint_sum` | BigInt で足す (文字列) |
| `account_fee_cost_excl` | numeric | `decimal_sum` | 月の raw を Decimal で足して最後に 1 回 小数 2 桁に丸める |
| `account_fee_storage_cost_jpy` | bigint | `bigint_sum` | BigInt で足す (文字列) |
| `account_fee_long_term_storage_cost_jpy` | bigint | `bigint_sum` | BigInt で足す (文字列) |
| `account_fee_removal_cost_jpy` | bigint | `bigint_sum` | BigInt で足す (文字列) |
| `account_fee_inbound_defect_cost_jpy` | bigint | `bigint_sum` | BigInt で足す (文字列) |
| `account_fee_low_inventory_cost_jpy` | bigint | `bigint_sum` | BigInt で足す (文字列) |
| `account_fee_subscription_cost_jpy` | bigint | `bigint_sum` | BigInt で足す (文字列) |
| `account_fee_easy_ship_cost_jpy` | bigint | `bigint_sum` | BigInt で足す (文字列) |
| `account_fee_other_cost_jpy` | bigint | `bigint_sum` | BigInt で足す (文字列) |
| `net_jpy` | bigint | `bigint_sum` | BigInt で足す (文字列) |
| `not_account_fee_mapped_jpy` | bigint | `bigint_sum` | BigInt で足す (文字列) |
| `unknown_line_mapped_jpy` | bigint | `bigint_sum` | BigInt で足す (文字列) |
| `unclassified_mapped_jpy` | bigint | `bigint_sum` | BigInt で足す (文字列) |
| `unmapped_jpy` | bigint | `bigint_sum` | BigInt で足す (文字列) |
| `unknown_line_rows` | integer | `int_sum` | integer を足す (JSON の数・件数なので 0 以上) |
| `unclassified_component_count` | integer | `int_sum` | integer を足す (JSON の数・件数なので 0 以上) |
| `unmapped_component_count` | integer | `int_sum` | integer を足す (JSON の数・件数なので 0 以上) |
| `sku_unclassified_component_count` | integer | `int_sum` | integer を足す (JSON の数・件数なので 0 以上) |
| `sku_unmapped_component_count` | integer | `int_sum` | integer を足す (JSON の数・件数なので 0 以上) |
| `finance_legacy_rows` | integer | `int_sum` | integer を足す (JSON の数・件数なので 0 以上) |
| `contribution_before_ad_incl_jpy` | bigint | `bigint_sum_null` | BigInt で足す・1 か月でも null なら null |
| `contribution_before_ad_excl` | numeric | `decimal_sum_null` | 同じ・1 か月でも null なら null |
| `contribution_after_ad_incl` | numeric | `decimal_sum_null` | 同じ・1 か月でも null なら null |
| `contribution_after_ad_excl` | numeric | `decimal_sum_null` | 同じ・1 か月でも null なら null |
| `profit_after_account_fees_incl` | numeric | `decimal_sum_null` | 同じ・1 か月でも null なら null |
| `profit_after_account_fees_excl` | numeric | `decimal_sum_null` | 同じ・1 か月でも null なら null |
| `contribution_after_ad_assuming_incomplete_zero_incl` | numeric | `decimal_sum` | 月の raw を Decimal で足して最後に 1 回 小数 2 桁に丸める |
| `contribution_after_ad_assuming_incomplete_zero_excl` | numeric | `decimal_sum` | 月の raw を Decimal で足して最後に 1 回 小数 2 桁に丸める |
| `profit_after_account_fees_assuming_incomplete_zero_incl` | numeric | `decimal_sum` | 月の raw を Decimal で足して最後に 1 回 小数 2 桁に丸める |
| `profit_after_account_fees_assuming_incomplete_zero_excl` | numeric | `decimal_sum` | 月の raw を Decimal で足して最後に 1 回 小数 2 桁に丸める |
| `before_ad_incomplete_days` | date[] | `dates_concat` | 日付の順につなぐ |
| `before_ad_incomplete_day_count` | integer | `int_sum` | integer を足す (JSON の数・件数なので 0 以上) |
| `after_ad_incomplete_days` | date[] | `dates_concat` | 日付の順につなぐ |
| `after_ad_incomplete_day_count` | integer | `int_sum` | integer を足す (JSON の数・件数なので 0 以上) |
| `after_account_fees_incomplete_days` | date[] | `dates_concat` | 日付の順につなぐ |
| `after_account_fees_incomplete_day_count` | integer | `int_sum` | integer を足す (JSON の数・件数なので 0 以上) |
| `profit_incomplete_reasons` | text[] | `reasons_union` | 和集合を決まった順に |
| `master_basis` | text | `same_all` | 全部の月で同じか (違えば 503 PROFIT_VERSION_MISMATCH) |
| `master_note_counts` | jsonb | `jsonb_key_sum` | JSON のキーごとに足す |
| `calculation_version` | text | `same_all` | 全部の月で同じか (違えば 503 PROFIT_VERSION_MISMATCH) |
| `finance_coverage_generation` | bigint | `null_in_period` | 期間の行では常に null (月ごとは months[]) |
| `finance_source_revision` | bigint | `null_in_period` | 期間の行では常に null (月ごとは months[]) |
| `calculated_at` | timestamp with time zone | `calculated_at` | 要求の 1 つの値 |

- 不完全な日の数 (`*_incomplete_day_count`) = 不完全な日の配列の長さ
- `int_sum` の列 (日数・行数・件数) は 0 以上 (#1602 Codex R2 Low)

## 6. 付録 B: /daily の行の全部の列 (#1602 Codex R1 M1 = 0050 の式どおりの null・値域・列挙)

列 = `mart.amazon_profit_daily_range` の戻り (0049・0050 の `_amazon_profit_rows`) と同じ並び (🆕 v2 の `member_seller_skus` だけは今の関数の戻りに無い = 3a の包む関数が返す・試験は v2 の列を除いて pg_proc と突き合わせる)。日の行は月をまたいで足さない (月ごとの行をつなぐだけ)。並び = 日 → `listing_id` (null は後ろ) → `seller_sku_norm` (C の照合)。行の鍵 (日 × 出品 / 日 × 未解決の SKU) は重ならない。

null の規則 (nul):

| nul | 意味 |
|---|---|
| `never` | null にならない |
| `maybe` | null がありうる |
| `iff_unresolved` | 出品が未解決のときだけ null |
| `iff_resolved` | 出品が解決したときだけ null |
| `iff_cost_unknown` | 原価が分からない (理由 listing_unresolved / composition_missing / cost_missing) ときだけ null |
| `iff_composition_unknown` | 構成が分からない (理由 listing_unresolved / composition_missing) ときだけ null |
| `iff_refund_price_missing` | refund_units_status = unit_price_missing のときだけ null |
| `iff_ad_uncollected` | ad_status = not_collected / missing のときだけ null |
| `iff_not_ok_before` | 「広告の前」の理由があるときだけ null |
| `iff_not_ok_after` | 理由が 1 つでもあるときだけ null |

「広告の前」の理由 = `finance_incomplete`・`finance_unclassified`・`refund_units_unknown`・`refund_units_partial_month`・`listing_unresolved`・`composition_missing`・`cost_missing` (0050 の ok_before)

| 列 | PG の型 | nul | JSON | 値域 |
|---|---|---|---|---|
| `company_id` | smallint | `never` | 数 |  |
| `mall` | text | `never` | 文字 |  |
| `scope_key` | text | `never` | 文字 |  |
| `economic_date_jst` | date | `never` | "YYYY-MM-DD" |  |
| `listing_id` | bigint | `iff_unresolved` | 文字列 |  |
| `seller_sku_norm` | text | `iff_resolved` | 文字 |  |
| `listing_resolution` | text | `never` | 文字 | 列挙 (resolved / unresolved) |
| `listing_code` | text | `iff_unresolved` | 文字 |  |
| `member_seller_skus` | text[] | `never` | 配列 (文字) | 🆕 v2 (3a の包む関数が返す・0050 の戻りには無い)。受け取った seller SKU を trim + 小文字 (**ASCII の英字だけ小文字を保証**・非 ASCII の大小は縛らない)・UTF-8 の bytes の厳密な昇順 (重複なし)・1 行 100 個まで・各 255 文字まで・空 `[]` ⇔ `order_rows = 0` |
| `received_listing_ids` | bigint[] | `never` | 配列 (文字列) | ID (BigInt) の厳密な昇順・重複なし・1 以上 |
| `received_listing_unresolved_count` | integer | `never` | 数 | ≥ 0 |
| `ad_received_listing_ids` | bigint[] | `never` | 配列 (文字列) | ID (BigInt) の厳密な昇順・重複なし・1 以上 |
| `ad_received_unresolved_rows` | integer | `never` | 数 | ≥ 0 |
| `units_ordered` | integer | `never` | 数 |  |
| `units_refunded_customer` | integer | `never` | 数 |  |
| `units_marketplace_guarantee` | integer | `never` | 数 |  |
| `units_a_to_z_refund` | integer | `never` | 数 |  |
| `units_net_sold` | integer | `never` | 数 |  |
| `units_refunded_customer_unrounded` | numeric | `iff_refund_price_missing` | 文字列 (小数 0 か 6 桁) |  |
| `units_a_to_z_refund_unrounded` | numeric | `iff_refund_price_missing` | 文字列 (小数 0 か 6 桁) |  |
| `sales_principal_jpy` | bigint | `never` | 文字列 |  |
| `sales_shipping_jpy` | bigint | `never` | 文字列 |  |
| `sales_giftwrap_jpy` | bigint | `never` | 文字列 |  |
| `sales_tax_jpy` | bigint | `never` | 文字列 |  |
| `commission_jpy` | bigint | `never` | 文字列 |  |
| `fba_fulfillment_jpy` | bigint | `never` | 文字列 |  |
| `fba_storage_jpy` | bigint | `never` | 文字列 |  |
| `closing_fee_jpy` | bigint | `never` | 文字列 |  |
| `shipping_chargeback_jpy` | bigint | `never` | 文字列 |  |
| `giftwrap_chargeback_jpy` | bigint | `never` | 文字列 |  |
| `promotion_jpy` | bigint | `never` | 文字列 |  |
| `promotion_tax_jpy` | bigint | `never` | 文字列 |  |
| `points_jpy` | bigint | `never` | 文字列 |  |
| `warehouse_damage_jpy` | bigint | `never` | 文字列 |  |
| `warehouse_lost_jpy` | bigint | `never` | 文字列 |  |
| `safe_t_jpy` | bigint | `never` | 文字列 |  |
| `refund_principal_jpy` | bigint | `never` | 文字列 |  |
| `reversal_reimbursement_jpy` | bigint | `never` | 文字列 |  |
| `misc_fee_jpy` | bigint | `never` | 文字列 |  |
| `other_fee_jpy` | bigint | `never` | 文字列 |  |
| `other_amount_jpy` | bigint | `never` | 文字列 |  |
| `profit_before_cogs_jpy` | bigint | `never` | 文字列 |  |
| `taxable_sku_fee_cost_jpy` | bigint | `never` | 文字列 |  |
| `net_jpy` | bigint | `never` | 文字列 |  |
| `unmapped_jpy` | bigint | `never` | 文字列 |  |
| `unclassified_component_count` | integer | `never` | 数 | ≥ 0 |
| `unclassified_mapped_jpy` | bigint | `never` | 文字列 |  |
| `unclassified_abs_jpy` | bigint | `never` | 文字列 | ≥ 0 |
| `unmapped_component_count` | integer | `never` | 数 | ≥ 0 |
| `finance_legacy_rows` | integer | `never` | 数 | ≥ 0 |
| `source_lines` | integer | `never` | 数 | ≥ 0 |
| `order_rows` | integer | `never` | 数 | ≥ 0 |
| `day_finance_status` | text | `never` | 文字 | 列挙 (complete / provisional / missing) |
| `refund_units_status` | text | `never` | 文字 | 列挙 (no_refund / estimated_monthly_unit_price / estimated_partial_month_unit_price / unit_price_missing) |
| `refund_incomplete_child_count` | integer | `never` | 数 | ≥ 0・≤ 1 |
| `refund_unestimated_jpy` | bigint | `never` | 文字列 |  |
| `component_unit_cost_jpy` | bigint | `iff_cost_unknown` | 文字列 |  |
| `cogs_jpy` | bigint | `iff_cost_unknown` | 文字列 |  |
| `cost_basis` | text | `never` | 文字 | 列挙 (sku_costs / observed / estimated / missing) |
| `composition_basis` | text | `never` | 文字 | 列挙 (listing_unresolved / missing / pre_audit_unverifiable / current_after_recorded_change / current_no_recorded_change) |
| `missing_cost_sku_ids` | bigint[] | `never` | 配列 (文字列) | ID (BigInt) の厳密な昇順・重複なし・1 以上 |
| `cost_sku_cost_ids` | bigint[] | `never` | 配列 (文字列) | ID (BigInt) の厳密な昇順・重複なし・1 以上 |
| `cost_observed_ids` | bigint[] | `never` | 配列 (文字列) | ID (BigInt) の厳密な昇順・重複なし・1 以上 |
| `ad_status` | text | `never` | 文字 | 列挙 (complete / verified_legacy / legacy_incomplete / missing / not_collected) |
| `ad_cost` | numeric | `iff_ad_uncollected` | 文字列 (小数 2 桁) |  |
| `ad_rows` | integer | `never` | 数 | ≥ 0 |
| `easy_ship_alloc_jpy` | bigint | `never` | 文字列 |  |
| `contribution_before_ad_incl_jpy` | bigint | `iff_not_ok_before` | 文字列 |  |
| `contribution_before_ad_excl` | numeric | `iff_not_ok_before` | 文字列 (小数 2 桁) |  |
| `contribution_after_ad_incl` | numeric | `iff_not_ok_after` | 文字列 (小数 2 桁) |  |
| `contribution_after_ad_excl` | numeric | `iff_not_ok_after` | 文字列 (小数 2 桁) |  |
| `contribution_before_ad_assuming_incomplete_zero_incl_jpy` | bigint | `never` | 文字列 |  |
| `contribution_before_ad_assuming_incomplete_zero_excl` | numeric | `never` | 文字列 (小数 2 桁) |  |
| `contribution_after_ad_assuming_incomplete_zero_incl` | numeric | `never` | 文字列 (小数 2 桁) |  |
| `contribution_after_ad_assuming_incomplete_zero_excl` | numeric | `never` | 文字列 (小数 2 桁) |  |
| `profit_incomplete_reasons` | text[] | `never` | 配列 (文字) |  |
| `assumed_zero_reasons` | text[] | `never` | 配列 (文字) |  |
| `master_basis` | text | `never` | 文字 | 列挙 (current) |
| `master_notes` | text[] | `never` | 配列 (文字) |  |
| `composition_hash` | text | `iff_composition_unknown` | 文字 | 64 桁の小文字の 16 進 |
| `cost_input_hash` | text | `iff_cost_unknown` | 文字 | 64 桁の小文字の 16 進 |
| `calculation_version` | text | `never` | 文字 |  |
| `observed_generation` | bigint | `maybe` | 文字列 |  |
| `composition_audit_since` | timestamp with time zone | `never` | UTC の ISO (ミリ秒・Z) |  |
| `composition_audit_through` | timestamp with time zone | `maybe` | UTC の ISO (ミリ秒・Z) |  |
| `finance_coverage_generation` | bigint | `maybe` | 文字列 |  |
| `finance_source_revision` | bigint | `maybe` | 文字列 |  |
| `calculated_at` | timestamp with time zone | `never` | UTC の ISO (ミリ秒・Z) |  |

行の中の不変条件 (0050 の式から):
- 未解決 (`listing_resolution = unresolved`) ⇔ 理由に `listing_unresolved`。未解決の行は `listing_id`・`listing_code`・原価・`composition_hash`・`cost_input_hash` が null、`seller_sku_norm` がある、`cost_basis = missing`・`composition_basis = listing_unresolved`、印は `listing_changed_since_received` だけ
- 解決した行は `listing_id`・`listing_code` があり `seller_sku_norm` が null、`composition_basis` は `listing_unresolved` でない。構成が無い (`composition_basis = missing`) ⇔ 理由に `composition_missing`
- `cost_basis = missing` ⇔ 原価が分からない (理由 `listing_unresolved` / `composition_missing` / `cost_missing`)
- 構成が分かる解決した行の `composition_basis` = 印から決まる (`pre_audit_unverifiable` があればそれ・次に `current_after_recorded_change`・どちらも無ければ `current_no_recorded_change`)
- `refund_units_status` = `unit_price_missing` ⇔ 理由 `refund_units_unknown` / `estimated_partial_month_unit_price` ⇔ 理由 `refund_units_partial_month` / この 2 つのときだけ `refund_incomplete_child_count = 1` (ほかは 0)
- 🆕 `unclassified_component_count + unmapped_component_count + finance_legacy_rows > 0` ⇔ 理由 `finance_unclassified` (0050 の g_uncl・#1602 Codex R2)
- 🆕 理由 `cost_missing` ⇔ 解決した行 かつ 構成あり (`composition_basis` ≠ `missing`) かつ `cost_basis = missing` = `listing_unresolved`・`composition_missing` と排他 (0050 の g_unres・g_comp・g_cost・#1602 Codex R3)
- 🆕 **日で決まる値は同じ日の行で全部同じ**: `day_finance_status`・`finance_coverage_generation`・`finance_source_revision` (0050 の days を日だけで結ぶ)・`ad_status` (ad_days を日だけで結ぶ)・理由 `ad_unresolved` (ad_u = その日の出品の無い広告の行の数) (#1602 Codex R3 の突き合わせ)
- 🆕 **要求全体で固定の値は全部の行で同じ** (#1602 Codex R4): 0050 の最後の SELECT で要求全体に固定の列 = `company_id` (p_company_id・1 以上)・`observed_generation` (同じ snapshot の max(generation)・月をまたいでも同じ)・`composition_audit_since` (引数なしの immutable の関数) は行の間でそろえる。`mall`・`scope_key`・`master_basis`・`calculation_version`・`calculated_at` は上 (要求) の値と照合する。/totals は期間の 1 行なので行の間の照合は無く、`master_basis`・`calculation_version`・`calculated_at` を上と照合する
- 🆕 **ID の配列 5 つ** (`received_listing_ids`・`ad_received_listing_ids`・`missing_cost_sku_ids`・`cost_sku_cost_ids`・`cost_observed_ids`) は **ID (BigInt) の厳密な昇順 = 重複なし・1 以上** (0050 の `array_agg(distinct … order by …)` / `array_agg(… order by …)` と `core.listing_components` の主キー (listing_id, sku_id)・#1602 Codex R5)。文字の順でなく数の順 (`"9"` < `"10"`)
- 🆕 `missing_cost_sku_ids` が空でない ⇔ 理由 `cost_missing` (0050 の missing_ids は原価が分からない部品・未解決 / 構成なしの行は空)
- `day_finance_status` が complete でない ⇔ 理由 `finance_incomplete` / `ad_status` = `not_collected` ⇔ `ad_not_collected`・`missing` ⇔ `ad_missing`・`legacy_incomplete` ⇔ `ad_legacy_unverified`
- `assumed_zero_reasons` = `profit_incomplete_reasons` から `refund_units_partial_month` を除いたもの
- 🆕 **`member_seller_skus` (v2・設計 19 §6.6.1 の案 (a)・設計 13 §5 の 0b-2 の (d) = 2026-10-04 中原さんが推しどおり)** = その行の粒度 (解決 = 出品 `listing_id` / 未解決 = 正規化 SKU `seller_sku_norm`) にまとまった、その日の財務の行で受け取った seller SKU。利益の行と **同じ計算・同じスナップショット** で解決する (F4-5 の「まとめた SKU」の構成の SKU = 財務の和と利益が 1 行でそろう)
  - 型 = 文字の配列・null にならない。**空の配列 `[]` = その日のその粒度に財務の行が無い** (広告だけ・Easy Ship だけの行) ⇔ `order_rows = 0` (財務の子がある ⇔ 空でない)
  - 要素 = trim (前後の空白を除く・空白の集合 = `core.norm_code` / 0054 の `amazon_map_key_problem` と同じ = 全角の空白・NBSP・BOM も) + 小文字 (🆕 #1615 Codex R1 Low: **ASCII の英字だけ小文字を保証** = `A`〜`Z` を含まない。全角の `Ａ`・`É`・`Σ` など非 ASCII の大小は縛らない = PostgreSQL の `lower()` の非 ASCII の扱いは照合順序に依る。全部の Unicode を縛るかは 3a で DB の式と統合試験をそろえるときに決める)・空でない・255 文字まで (0054 の対応の表の `seller_sku` と同じ)
  - 並び = UTF-8 の bytes の厳密な昇順 (重複なし・locale の比べを使わない・設計 19 §6.2.5 の「文字の配列」と同じ)。例 = `["ab-001","ａｂ-００１"]` (全角の SKU も正規化 SKU が同じなら同じ出品の粒度・半角が先)
  - 上限 = 1 行に **100 個** まで (直接の一致 = 1 つの粒度の SKU は全部 `core.norm_code` が同じ = 全角・半角・空白・ダッシュの違いだけ)
  - **同じ日の行の間で同じ seller SKU は 1 つの粒度にだけ** (trim + 小文字が同じなら `core.norm_code` も同じ = 同じ粒度・F4-5 の「財務の全部の SKU がちょうど 1 つの粒度」)
  - 🚨 `core.norm_code` を JS に写さない = 未解決の行の「member の正規化 = `seller_sku_norm`」は応答の契約では確かめない (DB の側で作る)

## 7. 迷った所 (PR 3 / PR 6 で決める)

- 月の包む関数の出力の形 (`<列>_raw` の名前) は fixture の入力 (`*.input.json`) では **仮**。PR 3 で決まったら入力の fixture を合わせる (最終の応答の形 = この文書は変えない)
- `months[].finance_status` の値 (`complete` / `provisional` / `missing` の一番弱いもの) はこの PR の提案 (日の `day_finance_status` と同じ語)
- 503 の reason の列挙 (`CALIBRATION_*`・`RESOURCE_*`・`DB_*`・`MONTH_*`) は PR 3〜6 の実装の前の提案。増やすときはこの表と一緒に直す
- Decimal の足し算は試験の中では BigInt の固定小数 (参照の組み立て)。本番の組み立て (PR 6) は設計どおり Decimal のライブラリを直接の依存にし、`rounding-vectors.json` を通すこと
- 🆕 v2 (PR 2c) で設計に書いていない細部を決めた所 (3a / PR 6 で違えば、この文書と版を一緒に直す):
  - 版の名前 = `amazon_profit_response_v2` (v1 の続き)・版の履歴を `CONTRACT_HISTORY` に
  - `member_seller_skus` の上限 (1 行 100 個・各 255 文字)・trim の空白の集合 (`core.norm_code` と同じ)・小文字は **ASCII の英字だけ小文字を保証** (ASCII の大文字なしで確かめる・`lower()` の非 ASCII の扱いは DB の照合順序に依る = 契約では縛らない・#1615 Codex R1 Low で §6 の主契約の文もこれにそろえた)・日の行の中の位置 (`listing_code` の後ろ)
  - `member_seller_skus` の空 ⇔ `order_rows = 0` と、同じ日の行の間で重ならないこと (設計の「財務の全部の SKU がちょうど 1 つの粒度」から)
  - `finance_coverage_token` は null にならない (関数が部品の日の覆いを確かめて例外 = 503)・同じ応答の 2 つの月で同じ値は違反
  - `failed_months` は `PROFIT_PARTIAL_FAILED` の 3 つの reason 全部で **必須**・`build503Body` は月の形でない要素があれば `PROFIT_INTERNAL` に落とす・🆕 `reason` も必須 (無い・列挙に無いなら `PROFIT_INTERNAL`・#1615 R1 M2)
  - 取り消しの段の名前 (`tx_setup`・`lock_statement`・`load_count`・`month_body`・`commit`)・見る順 (知らない印 → 印 `client_disconnect` → `25P04` → 印 → `unmarked`・ROLLBACK は 25P04 かどうかだけで別に決める = #1615 R1 M1)・印に段があれば印の段で引く・印を書いた後にほかの例外で終わっても印の理由・門の `D6*` / `55P03` の対応は 3a (`HEAVY_GUARD_SQLSTATES`) に残す

## 8. 版の履歴

| 版 | PR | 変えたこと |
|---|---|---|
| `amazon_profit_response_v1` | #1602 (D-60 PR 2b) | 最初の形 (期間の全体の 1 行 + `months[]`・日 × 出品の行・503 の固定の文と reason の列挙) |
| `amazon_profit_response_v2` | D-60 PR 2c | `months[].finance_coverage_token`・日の行の `member_seller_skus`・57014 / 25P04 の分け方の reason (`CANCEL_MAP`)・`PROFIT_PARTIAL_FAILED` の `failed_months` |

- 🆕 #1615 の Codex R1 の直し (`25P04` は表のどの行でも ROLLBACK を送らない・`PROFIT_PARTIAL_FAILED` の `reason` を必須に・「小文字」は ASCII の英字だけの保証) は **同じ PR 2c の中** = v2 のまま (版は上げない)。v2 はまだどこにも出していない (受け口は 503 のまま・マージ前) ので、v2 を読む側はまだ居ない
