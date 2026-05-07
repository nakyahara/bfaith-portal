# Render Amazon Finance Phase 1 — 実装計画

**Status**: 着手中 (2026-05-07〜)
**Owner**: bfaith-portal warehouse / Render team
**Codex review rounds**: 6 ラウンド経て確定

## ゴール

`mirror_amazon_finance_sku_daily` を v4 view から **daily 粒度で生成** し、ミニPC → Render に同期し、Render 内で `mart_amazon_sku_daily/weekly/monthly` を rebuild する pipeline を完成させる。

## 確定事項

### 設計の芯
1. mirror の主役は finance / sales_traffic / ads / inventory / offer の **daily 5 本**
2. repeat purchase だけ source-grain 例外
3. Render の分析基盤は `mart_amazon_sku_daily` を中心に全部派生
4. price history ingest は今すぐ始める、価格弾力性の本実装は後ろ

### 採用利益概念
- **`settlement_margin_excl_tax`** (税抜き settlement 確定利益) — 3 候補から選定済
- daily fact の `profit_amount` は v4 の `settlement_margin_excl_tax` を再現対象として固定

### 利益式 (税抜き)
```
profit_amount =
    sales_principal_jpy + sales_shipping_jpy + sales_giftwrap_jpy
  − commission_jpy − fba_fulfillment_jpy − fba_storage_jpy − closing_fee_jpy
  − shipping_chargeback_jpy − giftwrap_chargeback_jpy − promotion_jpy
  − refund_principal_jpy
  + warehouse_damage_jpy + warehouse_lost_jpy + safe_t_jpy + reversal_reimbursement_jpy
  − cogs_amount
```
`sales_tax_jpy` は別列保持、利益式には含めない。

### 原価方式
- **snapshot 原価で固定** (取引日時点の原価を fact 行に書いて不変)
- `unit_cost_snapshot` + `cost_snapshot_date_jst` を fact に保存
- `latest_unit_cost_reference` は別列で operational simulation 用
- `cost_status` 4 値: `complete` / `missing_cost` / `partial_cost` / `late_bound_after_close`
- `is_cost_complete = CASE WHEN cost_status='complete' THEN 1 ELSE 0 END`
- 既存 `v_sku_costed` (latest 原価) は **触らない**

### Source 構造
- **daily fact の起点**: `raw_amazon_settlement_lines` (3,152,595 行、46 列、micro 単位)
- **daily 主軸**: `economic_date`
- **業務 SKU 主軸**: `seller_sku_normalized`
- **v4 (`v_amazon_sku_profit_actual_v4`) は validation reference として併存** (daily source ではない)
- 詳細: `docs/contracts/raw_amazon_settlement_lines.contract.md`

## 11 ticket 構成 (依存順)

| 番号 | タイトル | 工数 |
|---|---|---|
| #1-0a | Freeze raw settlement source contract | 2-3h |
| #1-0 | Canonical metric mapping and taxonomy | 2-4h |
| **#1-1** | Build `f_amazon_finance_sku_daily_v1` with snapshot cogs | **1.5-2.5d** ★最重 |
| #1-3 | Validate daily fact against v4 monthly reference | 0.5-1d |
| #1-4 | Sync contract registry refactor | 1-1.75d |
| #1-4a | Sync run ledger / backout / replay | 1-1.5d |
| #1-5 | Cutover consumer read path to daily fact | 0.5-1d |
| #1-6 | Backfill / release / runbook | 0.5d |
| #1-7 | daily-sync integration + push rebuild trigger | 0.75-1.25d |
| #1-7a | Concurrency guard via job_locks | 0.5-0.75d |
| #1-8a | Data quality gate / anomaly alert | 0.75-1.25d |

合計: **5-7 日** (source 確認済で楽観側に着地)

## 依存順序グラフ

```
#1-0a (source contract)
   ↓
#1-0 (metric mapping)
   ↓
#1-1 (daily fact + snapshot cogs)  ←── 最重要、最難所
   ↓
#1-3 (monthly validation vs v4)
   ↓
#1-4 (sync contract)
   ↓
#1-4a (sync ledger / backout / replay)
   ↓
#1-5 (consumer read-path cutover)
   ↓
#1-6 (backfill / runbook)
   ↓
#1-7 (push rebuild trigger)
   ↓
#1-7a (concurrency guard)
   ↓
#1-8a (DQ gate / alert)
```

## DoD (12 項目)

1. source 参照名が全て `raw_amazon_settlement_lines` に統一
2. daily fact は `economic_date × seller_sku_normalized` grain
3. source 金額は micro → JPY 正変換 (`/ 1000000.0`)
4. `profit_amount` の比較先が v4 の `settlement_margin_excl_tax` に固定
5. snapshot 関連 4 列 (`unit_cost_snapshot` / `cost_snapshot_date_jst` / `cogs_amount` / immutable 運用) 文書化
6. `latest_unit_cost_reference` が別列で保持、正式利益には未使用
7. `cost_status` 4 値で入る、`is_cost_complete` は `complete` のみ 1
8. `v_sku_costed` は変更なし
9. v4 は daily source ではなく validation reference として位置付けが明記
10. full backfill + incremental の runbook、release gate = monthly validation (3 層 gate)
11. 全 ticket が feature branch 前提で `master` 直書き禁止が明記
12. `unit_cost_snapshot` immutable が job/SQL に明記

## 3 層 validation gate

1. **row_count / date_coverage 厳密一致** (1 件でもズレたら fail)
2. **monthly total**: `abs(total_diff) <= max(1000, abs(legacy_total)*0.001)` (0.1%)
3. **explainability**: `unbucketed_diff <= max(500, legacy_total*0.0005)`, `explainable_sum/total_diff >= 99%`

### explainable diff bucket 9 種
`refund_effective_date` / `adjustment_effective_date` / `promo_rebate_effective_date` / `cost_late_binding` / `sku_null` / `asin_many_to_many` / `legacy_only` / `daily_only` / `unbucketed`

許容:
- `refund/adjustment/promo_rebate_effective_date`: 説明済差分として全量許容
- `cost_late_binding`: 月次総売上 1% まで warn / 2% 超 alert
- `sku_null` / `asin_many_to_many` / `legacy_only` / `daily_only`: 0 許容 (1 件でも alert)
- `unbucketed`: 0.05% or 500 円超で alert

## 全 ticket 共通ルール

- `master` 直書き禁止
- feature branch 必須 (例: `feature/phase1-1-N-<short-name>`)
- PR 経由でのみ merge
- DATA_DIR 未指定は fail-fast (worktree DB 事故防止)
- `process.cwd()/data` fallback への依存を避ける

## 関連ドキュメント

- `docs/contracts/raw_amazon_settlement_lines.contract.md` — source contract v1 (Phase 1 の前提)
- `docs/contracts/transaction_taxonomy_seed.csv` — transaction_type 分類 seed (Ticket #1-0a で生成)
- `sql/contracts/check_raw_amazon_settlement_lines.sql` — schema / baseline 検査 SQL
- `scripts/contracts/assert-raw-amazon-source.js` — contract 差分検知 (CI / 手動チェック用)
- (Ticket #1-0 以降で追加) `docs/amazon-finance/canonical-metric-mapping.md` — line から canonical metric への mapping
- (Ticket #1-1 以降で追加) `docs/amazon-finance/cogs-snapshot-policy.md` — snapshot 原価の運用規律

## 実装着手手順 (#1-0a から)

1. ✅ feature branch を切る (`feature/phase1-1-0a-source-contract-freeze`)
2. ✅ `docs/contracts/raw_amazon_settlement_lines.contract.md` 作成
3. ✅ `sql/contracts/check_raw_amazon_settlement_lines.sql` 作成
4. ✅ `scripts/contracts/assert-raw-amazon-source.js` 作成
5. ⏳ ssh で `sql/contracts/check_raw_amazon_settlement_lines.sql` を実行 → distinct 値抽出
6. ⏳ `docs/contracts/transaction_taxonomy_seed.csv` を生成
7. ⏳ `node scripts/contracts/assert-raw-amazon-source.js` を実行して contract 違反なしを確認
8. ⏳ commit + PR 作成 → master merge → Ticket #1-0 へ

## 改訂履歴

| Version | Date | Changes |
|---|---|---|
| v1 | 2026-05-07 | 初版。Codex 6 ラウンドの確定仕様を反映 |
