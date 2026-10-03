# データ契約: Amazon の利益の受け口の最終の JSON (D-60 v3.4・PR 2b)

**状態**: 固定 (2026-10-03・DB の実装より先に) / **受け口は 503 のまま** (開けるのは §3.10 の PR 6 だけ)
**設計**: AI_reference『CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』§3.10「応答の契約」「長い期間の応答の列ごとの規則」(Codex R-D60-v3-4 M5・R-D60-v3-5 Low)
**正本 (機械)**: `apps/company-db/profit/response-contract.mjs` (この文書の表は、そこから作った・試験で突き合わせる)
**fixture**: `scripts/fixtures/amazon-profit-response/` / **試験**: `scripts/test-company-db-profit-response-contract.mjs`

## 1. 受け口と形

| 受け口 | 200 の形 |
|---|---|
| `GET /apps/company-db/sync/amazon-profit/totals?mall&scope&from&to` | `{ ok, contract, kind: "totals", mall, scope, from, to, master_basis, calculation_version, calculated_at, master_as_of, total: {期間の全体の 1 行}, months: [...] }` |
| `GET /apps/company-db/sync/amazon-profit/daily?mall&scope&from&to` | `{ ok, contract, kind: "daily", mall, scope, from, to, master_basis, calculation_version, calculated_at, master_as_of, rows: [日 × 出品], months: [...] }` |

- 旧 totals (#1559) の「日・月・期間の中の月の小計・期間の合計」の行 (`row_kind`) は **返さない**。期間の全体の 1 行 + `months[]`
- `contract` = `amazon_profit_response_v1`
- 全部の応答 (200 も 503 も) に `Cache-Control: no-store`。AI・画面は正式な値を自分で保存しない
- 長い期間は月ごとに計算してアプリで足す (1 回の要求で触れる暦月は最大 13)。1 か月でも失敗したら部分の値は返さず 503

## 2. 値の書き方

| PG の型 | JSON |
|---|---|
| bigint (金額・数・ID) | 10 進の文字列 (JS の Number に入れない・`"-0"` は無い) |
| bigint[] | その文字列の配列 |
| numeric (金額) | 小数 2 桁の文字列。丸めは PostgreSQL の `round(numeric, 2)` = 0 から遠い方へ (half away from zero)。`"-0.00"` は無い |
| numeric (`units_*_unrounded`) | 小数 0 か 6 桁の文字列 (返品なし = `"0"`) |
| integer / smallint | 数 |
| date / date[] | `"YYYY-MM-DD"` / その配列 |
| timestamptz | UTC の ISO (ミリ秒・Z) |
| text / text[] / jsonb / boolean | 文字 / 文字の配列 / object / true・false |

- **`calculated_at`** = 要求の最初の文 (共通の lock を取る SELECT) の `statement_timestamp()` の **1 つの値**。上・`total`・`rows` の全部・`months` の全部で同じ (月の関数の値は捨てて置き換える・R-D60-v3-5 Low)。`master_as_of = calculated_at` (D-64)
- **raw の列** (丸める前の値・名前が `_raw` で終わる) は **どこにも出さない**
- **理由の決まった順** = `finance_incomplete` → `finance_unclassified` → `refund_units_unknown` → `refund_units_partial_month` → `listing_unresolved` → `composition_missing` → `cost_missing` → `ad_not_collected` → `ad_missing` → `ad_legacy_unverified` → `ad_unresolved` (0049 / 0050 の関数の順・totals は `ad_unresolved` を入れない)
- **状態の順位** (後ろほど弱い): 広告 = `complete` < `verified_legacy` < `legacy_incomplete` < `missing` < `not_collected` / 財務 = `complete` < `provisional` < `missing`

## 3. months[] (月の metadata の関数から作る・財務の行が 0 の月にもある)

| 列 | JSON | 意味 |
|---|---|---|
| `month_start` | "YYYY-MM-01" | 暦月 |
| `period_from` / `period_to` | "YYYY-MM-DD" | その月の中で要求が触れる範囲 |
| `finance_status` | `complete` / `provisional` / `missing` | その月の日の財務の状態の一番弱いもの |
| `has_finance_rows` | boolean | その月に財務の行があるか |
| `finance_month_settled` | boolean | `core.finance_month_settled` |
| `finance_coverage_generation` / `finance_source_revision` | 文字列 / null | その月の coverage の世代と版 (期間の行では常に null) |
| `calculation_version` | 文字 | 全部の月で同じ (違えば 503 `PROFIT_VERSION_MISMATCH`) |
| `calculated_at` | UTC の ISO | 上と同じ 1 つの値 |

## 4. 503 の code の一覧

本文 = `{ ok: false, code, error, reason? }` だけ (値・部分の結果・Render の応答の本文・鍵・接続の文字列は出さない)。`reason` は大文字のコード (例 `METRICS_AUTH`)。

| code | いつ |
|---|---|
| `PROFIT_ROUTE_DISABLED` | 封じ込め (今の router・#1570)。PR 6 で外す |
| `PROFIT_NOT_CALIBRATED` | 承認済みの校正の記録が無い・draft・revoked・fingerprint / 材料が今と違う |
| `PROFIT_BUSY` | 共通の lock (`company_db_heavy`) が取れない |
| `PROFIT_RESOURCE` | 資源の関門 (メモリ・一時ファイル・負荷の要因・process の数の材料) を満たさない |
| `PROFIT_METRICS_UNAVAILABLE` | Render の metrics が読めない・古い・形が違う (`reason` = `METRICS_…`) |
| `PROFIT_PARTIAL_FAILED` | 1 か月でも計算に失敗した |
| `PROFIT_VERSION_MISMATCH` | `calculation_version` / `master_basis` が月で違う |

## 5. 付録 A: /totals の期間の行の全部の列の分類 (型の全部の分類の表)

列 = `mart.amazon_profit_day_totals_range` の戻り (0049) と同じ並び。試験が pg_proc と突き合わせる (列が増えたら分類を足すまで落ちる)。

| 列 | PG の型 | 規則 | 期間の値の作り方 |
|---|---|---|---|
| `row_kind` | text | `omit` | 出さない (旧 totals の行の種類の列) |
| `period_from` | date | `request_bound` | 要求の from / to |
| `period_to` | date | `request_bound` | 要求の from / to |
| `economic_date_jst` | date | `omit` | 出さない (旧 totals の行の種類の列) |
| `month_start` | date | `omit` | 出さない (旧 totals の行の種類の列) |
| `day_count` | integer | `int_sum` | integer を足す (JSON の数) |
| `day_finance_status` | text | `null_in_period` | 期間の行では常に null (月ごとは months[]) |
| `complete_days` | integer | `int_sum` | integer を足す (JSON の数) |
| `resolved_rows` | integer | `int_sum` | integer を足す (JSON の数) |
| `unresolved_rows` | integer | `int_sum` | integer を足す (JSON の数) |
| `units_ordered` | bigint | `bigint_sum` | BigInt で足す (文字列) |
| `units_net_sold` | bigint | `bigint_sum` | BigInt で足す (文字列) |
| `sales_principal_jpy` | bigint | `bigint_sum` | BigInt で足す (文字列) |
| `sales_tax_jpy` | bigint | `bigint_sum` | BigInt で足す (文字列) |
| `profit_before_cogs_jpy` | bigint | `bigint_sum` | BigInt で足す (文字列) |
| `cogs_jpy` | bigint | `bigint_sum_null` | BigInt で足す・1 か月でも null なら null |
| `easy_ship_alloc_jpy` | bigint | `bigint_sum` | BigInt で足す (文字列) |
| `easy_ship_unallocated_jpy` | bigint | `bigint_sum` | BigInt で足す (文字列) |
| `easy_ship_unallocated_count` | integer | `int_sum` | integer を足す (JSON の数) |
| `ad_status` | text | `state_rank` | 一番弱い状態 (順位の表) |
| `ad_cost_total` | numeric | `decimal_sum_null` | 同じ・1 か月でも null なら null |
| `ad_cost_allocated` | numeric | `decimal_sum` | 月の raw を Decimal で足して最後に 1 回 小数 2 桁に丸める |
| `ad_cost_unresolved` | numeric | `decimal_sum` | 月の raw を Decimal で足して最後に 1 回 小数 2 桁に丸める |
| `ad_unresolved_rows` | integer | `int_sum` | integer を足す (JSON の数) |
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
| `unknown_line_rows` | integer | `int_sum` | integer を足す (JSON の数) |
| `unclassified_component_count` | integer | `int_sum` | integer を足す (JSON の数) |
| `unmapped_component_count` | integer | `int_sum` | integer を足す (JSON の数) |
| `sku_unclassified_component_count` | integer | `int_sum` | integer を足す (JSON の数) |
| `sku_unmapped_component_count` | integer | `int_sum` | integer を足す (JSON の数) |
| `finance_legacy_rows` | integer | `int_sum` | integer を足す (JSON の数) |
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
| `before_ad_incomplete_day_count` | integer | `int_sum` | integer を足す (JSON の数) |
| `after_ad_incomplete_days` | date[] | `dates_concat` | 日付の順につなぐ |
| `after_ad_incomplete_day_count` | integer | `int_sum` | integer を足す (JSON の数) |
| `after_account_fees_incomplete_days` | date[] | `dates_concat` | 日付の順につなぐ |
| `after_account_fees_incomplete_day_count` | integer | `int_sum` | integer を足す (JSON の数) |
| `profit_incomplete_reasons` | text[] | `reasons_union` | 和集合を決まった順に |
| `master_basis` | text | `same_all` | 全部の月で同じか (違えば 503 PROFIT_VERSION_MISMATCH) |
| `master_note_counts` | jsonb | `jsonb_key_sum` | JSON のキーごとに足す |
| `calculation_version` | text | `same_all` | 全部の月で同じか (違えば 503 PROFIT_VERSION_MISMATCH) |
| `finance_coverage_generation` | bigint | `null_in_period` | 期間の行では常に null (月ごとは months[]) |
| `finance_source_revision` | bigint | `null_in_period` | 期間の行では常に null (月ごとは months[]) |
| `calculated_at` | timestamp with time zone | `calculated_at` | 要求の 1 つの値 |

- 不完全な日の数 (`*_incomplete_day_count`) = 不完全な日の配列の長さ

## 6. 付録 B: /daily の行の全部の列

列 = `mart.amazon_profit_daily_range` の戻り (0049・0050 の `_amazon_profit_rows`) と同じ並び。日の行は月をまたいで足さない (月ごとの行をつなぐだけ)。並び = 日 → `listing_id` (null は後ろ) → `seller_sku_norm` (C の照合)。

| 列 | PG の型 | JSON | null |
|---|---|---|---|
| `company_id` | smallint | 数 | null にならない |
| `mall` | text | 文字 | null にならない |
| `scope_key` | text | 文字 | null にならない |
| `economic_date_jst` | date | "YYYY-MM-DD" | null にならない |
| `listing_id` | bigint | 文字列 | null がありうる |
| `seller_sku_norm` | text | 文字 | null がありうる |
| `listing_resolution` | text | 文字 | null にならない |
| `listing_code` | text | 文字 | null がありうる |
| `received_listing_ids` | bigint[] | 配列 (文字列) | null がありうる |
| `received_listing_unresolved_count` | integer | 数 | null がありうる |
| `ad_received_listing_ids` | bigint[] | 配列 (文字列) | null がありうる |
| `ad_received_unresolved_rows` | integer | 数 | null がありうる |
| `units_ordered` | integer | 数 | null がありうる |
| `units_refunded_customer` | integer | 数 | null がありうる |
| `units_marketplace_guarantee` | integer | 数 | null がありうる |
| `units_a_to_z_refund` | integer | 数 | null がありうる |
| `units_net_sold` | integer | 数 | null がありうる |
| `units_refunded_customer_unrounded` | numeric | 文字列 (小数 0 か 6 桁) | null がありうる |
| `units_a_to_z_refund_unrounded` | numeric | 文字列 (小数 0 か 6 桁) | null がありうる |
| `sales_principal_jpy` | bigint | 文字列 | null がありうる |
| `sales_shipping_jpy` | bigint | 文字列 | null がありうる |
| `sales_giftwrap_jpy` | bigint | 文字列 | null がありうる |
| `sales_tax_jpy` | bigint | 文字列 | null がありうる |
| `commission_jpy` | bigint | 文字列 | null がありうる |
| `fba_fulfillment_jpy` | bigint | 文字列 | null がありうる |
| `fba_storage_jpy` | bigint | 文字列 | null がありうる |
| `closing_fee_jpy` | bigint | 文字列 | null がありうる |
| `shipping_chargeback_jpy` | bigint | 文字列 | null がありうる |
| `giftwrap_chargeback_jpy` | bigint | 文字列 | null がありうる |
| `promotion_jpy` | bigint | 文字列 | null がありうる |
| `promotion_tax_jpy` | bigint | 文字列 | null がありうる |
| `points_jpy` | bigint | 文字列 | null がありうる |
| `warehouse_damage_jpy` | bigint | 文字列 | null がありうる |
| `warehouse_lost_jpy` | bigint | 文字列 | null がありうる |
| `safe_t_jpy` | bigint | 文字列 | null がありうる |
| `refund_principal_jpy` | bigint | 文字列 | null がありうる |
| `reversal_reimbursement_jpy` | bigint | 文字列 | null がありうる |
| `misc_fee_jpy` | bigint | 文字列 | null がありうる |
| `other_fee_jpy` | bigint | 文字列 | null がありうる |
| `other_amount_jpy` | bigint | 文字列 | null がありうる |
| `profit_before_cogs_jpy` | bigint | 文字列 | null がありうる |
| `taxable_sku_fee_cost_jpy` | bigint | 文字列 | null がありうる |
| `net_jpy` | bigint | 文字列 | null がありうる |
| `unmapped_jpy` | bigint | 文字列 | null がありうる |
| `unclassified_component_count` | integer | 数 | null がありうる |
| `unclassified_mapped_jpy` | bigint | 文字列 | null がありうる |
| `unclassified_abs_jpy` | bigint | 文字列 | null がありうる |
| `unmapped_component_count` | integer | 数 | null がありうる |
| `finance_legacy_rows` | integer | 数 | null がありうる |
| `source_lines` | integer | 数 | null がありうる |
| `order_rows` | integer | 数 | null がありうる |
| `day_finance_status` | text | 文字 | null にならない |
| `refund_units_status` | text | 文字 | null がありうる |
| `refund_incomplete_child_count` | integer | 数 | null がありうる |
| `refund_unestimated_jpy` | bigint | 文字列 | null がありうる |
| `component_unit_cost_jpy` | bigint | 文字列 | null がありうる |
| `cogs_jpy` | bigint | 文字列 | null がありうる |
| `cost_basis` | text | 文字 | null がありうる |
| `composition_basis` | text | 文字 | null がありうる |
| `missing_cost_sku_ids` | bigint[] | 配列 (文字列) | null がありうる |
| `cost_sku_cost_ids` | bigint[] | 配列 (文字列) | null がありうる |
| `cost_observed_ids` | bigint[] | 配列 (文字列) | null がありうる |
| `ad_status` | text | 文字 | null にならない |
| `ad_cost` | numeric | 文字列 (小数 2 桁) | null がありうる |
| `ad_rows` | integer | 数 | null がありうる |
| `easy_ship_alloc_jpy` | bigint | 文字列 | null がありうる |
| `contribution_before_ad_incl_jpy` | bigint | 文字列 | null がありうる |
| `contribution_before_ad_excl` | numeric | 文字列 (小数 2 桁) | null がありうる |
| `contribution_after_ad_incl` | numeric | 文字列 (小数 2 桁) | null がありうる |
| `contribution_after_ad_excl` | numeric | 文字列 (小数 2 桁) | null がありうる |
| `contribution_before_ad_assuming_incomplete_zero_incl_jpy` | bigint | 文字列 | null がありうる |
| `contribution_before_ad_assuming_incomplete_zero_excl` | numeric | 文字列 (小数 2 桁) | null がありうる |
| `contribution_after_ad_assuming_incomplete_zero_incl` | numeric | 文字列 (小数 2 桁) | null がありうる |
| `contribution_after_ad_assuming_incomplete_zero_excl` | numeric | 文字列 (小数 2 桁) | null がありうる |
| `profit_incomplete_reasons` | text[] | 配列 (文字) | null にならない |
| `assumed_zero_reasons` | text[] | 配列 (文字) | null にならない |
| `master_basis` | text | 文字 | null にならない |
| `master_notes` | text[] | 配列 (文字) | null にならない |
| `composition_hash` | text | 文字 | null がありうる |
| `cost_input_hash` | text | 文字 | null がありうる |
| `calculation_version` | text | 文字 | null にならない |
| `observed_generation` | bigint | 文字列 | null がありうる |
| `composition_audit_since` | timestamp with time zone | UTC の ISO (ミリ秒・Z) | null がありうる |
| `composition_audit_through` | timestamp with time zone | UTC の ISO (ミリ秒・Z) | null がありうる |
| `finance_coverage_generation` | bigint | 文字列 | null がありうる |
| `finance_source_revision` | bigint | 文字列 | null がありうる |
| `calculated_at` | timestamp with time zone | UTC の ISO (ミリ秒・Z) | null にならない |

## 7. 迷った所 (PR 3 / PR 6 で決める)

- 月の包む関数の出力の形 (`<列>_raw` の名前) は fixture の入力 (`*.input.json`) では **仮**。PR 3 で決まったら入力の fixture を合わせる (最終の応答の形 = この文書は変えない)
- `months[].finance_status` の値 (`complete` / `provisional` / `missing` の一番弱いもの) はこの PR の提案 (日の `day_finance_status` と同じ語)
- Decimal の足し算は試験の中では BigInt の固定小数 (参照の組み立て)。本番の組み立て (PR 6) は設計どおり Decimal のライブラリを直接の依存にし、`rounding-vectors.json` を通すこと
