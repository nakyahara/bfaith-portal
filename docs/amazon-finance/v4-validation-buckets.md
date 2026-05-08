# v4 Validation — Diff Bucket Rules

**Status**: frozen v1 (2026-05-08)
**Phase 1 ticket**: #1-3
**Owner**: Render Amazon Phase 1
**比較対象**: `v_amazon_sku_profit_actual_v4.gross_margin_with_reimbursement_excl_tax` (PR #61 で追加)

このドキュメントは Phase 1 #1-3 (v4 validation) と #1-8a (DQ gate) で共通使用する **diff bucket の分類ルール** を凍結したもの。

## 背景

`f_amazon_finance_sku_daily_v1` の日次集計を月次に rollup したものと、v4 view (gross_margin_with_reimbursement_excl_tax) を SKU × 月で比較すると、必ず差分が発生する。

差の種類を 7 bucket に分類して **「説明できる差」と「説明できない残差 (unbucketed)」** を区別する。`unbucketed` が 0 円に近いほど「daily fact が v4 と意味的に整合している」と言える。

## 7 bucket 定義

| bucket_code | 意味 | 検出方法 | 期待値 |
|---|---|---|---|
| `sku_null` | daily fact で `seller_sku` が NULL/empty | `WHERE seller_sku IS NULL OR TRIM(seller_sku)=''` | **0** (許容なし) |
| `cost_late_binding` | `cost_status='missing_cost'` の影響額 | `WHERE cost_status='missing_cost'` の `profit_amount` SUM | < 月次総売上の 1% |
| `legacy_only` | v4 にあるが daily fact に無い SKU | v4 ⊃ daily の差集合 | 件数 < 5 (新 SKU の取込ラグ等) |
| `daily_only` | daily fact にあるが v4 に無い SKU | daily ⊃ v4 の差集合 | **0** (理論上ない) |
| `refund_effective_date` | refund/adjustment の月跨ぎ | `transaction_type IN ('Refund','Adjustment',…) AND order_date_month <> settlement_month` の影響額 | Phase 1.x で実装、現状 0 |
| `adjustment_diff` | v4 と daily の単純差 (説明済) | `total_diff - sum(他 bucket)` | < 月次総売上の 0.5% |
| `unbucketed` | 上記いずれでも説明できない残差 | `total_diff - sum(7 bucket)` | **< 500円 / < 0.05%** |

## 利益式 (再掲、Phase 1 確定)

```
profit_amount =
    sales_principal_jpy + sales_shipping_jpy + sales_giftwrap_jpy
  - cogs_amount
  - commission_jpy - fba_fulfillment_jpy - fba_storage_jpy - closing_fee_jpy
  - shipping_chargeback_jpy - giftwrap_chargeback_jpy - promotion_jpy
  - refund_principal_jpy
  + warehouse_damage_jpy + warehouse_lost_jpy + safe_t_jpy + reversal_reimbursement_jpy
```

`sales_tax_jpy` / `misc_fee_jpy` / `other_fee_jpy` / `other_amount_jpy` は **保持のみ、利益式に入れない**。`ad_cost` は Phase 1.x で `ad_cost_jpy` 列追加予定 (利益式から引かない設計)。

## v4 比較先 (gross_margin_with_reimbursement_excl_tax)

```
gross_margin_with_reimbursement_excl_tax =
    sales_principal + sales_shipping + sales_giftwrap
  - cogs (= unit_cogs_excl_tax × qty_ordered)
  + commission + fba_fulfillment + fba_storage + closing_fee
  + shipping_chargeback + giftwrap_chargeback + promotion
  + warehouse_damage + warehouse_lost + safe_t + refund_principal + reversal_reimbursement
```

注: v4 では fees (`commission_micro` 等) が **負数で格納**されているため `+` で加算 = 引く効果。daily fact 側は ABS で正数化してから `-` で引く。等価のはず。

## 差分計算式 (validation report 用)

```
total_diff = profit_daily - profit_v4

unbucketed = total_diff
           - sku_null_amount
           - cost_late_binding_amount
           - (legacy_only_count > 0 の影響、推定)
           - daily_only_amount
           - refund_effective_date_amount
           - adjustment_diff
```

## 既知の差 (Phase 1.x 改善対象)

### 1. silver dedup の微小差 (約 -0.1 ~ -0.2%)

- daily fact の silver dedup ロジック (`(source_settlement_id, business_line_key)` で row_number) と、v4 (= rebuild-amazon-settlement-mart.js の同じ silver dedup 経由) の細かい挙動差
- 月別差: 0.03%〜0.73% (小さい月で大、大きい月で小)
- 5 ヶ月合計: revenue -0.13% / cogs -0.68% / profit +0.18%
- 改善方針: Phase 1.x で daily fact 側の rebuild ロジックを v4 側と完全一致させる (もしくは v4 側を daily fact と整合)

### 2. v_sku_costed の四捨五入

- `single_unit_cost × components_qty` の小数演算で 1 円差が発生
- 影響: profit per SKU で ±1 円程度
- 改善方針: Phase 1.x で `unit_cost_snapshot` を整数 micro 単位に統一

### 3. ad_cost が daily fact に未統合

- v4 の `settlement_margin_excl_tax` (ad_cost 引く方) を比較対象にすると ~ -2 〜 -3% 差
- → 比較対象を `gross_margin_with_reimbursement_excl_tax` (ad_cost 引かない方) に変更済 (中原さん 2026-05-08 確定)
- Phase 1.x で `ad_cost_jpy` 列追加予定

## DQ runner との連動

`#1-8a` の `accounting_diff_buckets` テーブルは本 docs の bucket 分類を実装している:

- 本 docs: 人間が読む bucket ルール定義
- DQ runner: 機械的 bucket 分類 + threshold breach 検知

両者の bucket_code は完全に一致 (sku_null / cost_late_binding / legacy_only / daily_only / refund_effective_date / adjustment_diff / unbucketed)。

## 関連

- 設計: `docs/amazon-finance/canonical-metric-mapping.md`
- DQ runner: `apps/warehouse/run-amazon-finance-dq.js` (#1-8a)
- validation script: `apps/warehouse/validate-v4-reference.js` (#1-3)
- baseline 記録: `reports/amazon-finance/monthly-validation-baseline.md`

## 改訂履歴

| Version | Date | Changes |
|---|---|---|
| v1 | 2026-05-08 | Phase 1 #1-3 として初回作成。7 bucket と #1-8a DQ runner との連動を明文化 |
