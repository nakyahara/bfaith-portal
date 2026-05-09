# raw_rakuten_orders Source Contract v1

**作成**: 2026-05-09
**ticket**: Phase 1a #R-0a (楽天版売上分析、AI_reference: `楽天Phase1a設計書_v0.3_20260509.md`)
**目的**: `f_rakuten_finance_sku_daily_v1` build の source として `raw_rakuten_orders` の schema / 値域 / 業務前提を凍結する。

下流 (build / validation / sync) はこの contract に依存。本表記と実 schema が乖離した場合は、build 失敗ではなく **contract 違反として alert を出す** (`scripts/contracts/assert-rakuten-source.js` 想定)。

---

## 1. 基本情報

| 項目 | 値 |
|------|-----|
| 物理 table 名 | `raw_rakuten_orders` |
| 場所 | miniPC `warehouse.db` |
| 取得元 | 楽天 RMS API (`apps/warehouse/rakuten-orders.js`) |
| 起点日 | 2024-04-05 |
| 行数 (2026-05-09 時点) | 376,254 |
| 行粒度 | 1 注文 × 商品アイテム (item_detail_id 別、selected_choice 違いも別行) |
| ID | `id` (INTEGER PRIMARY KEY、auto increment) |
| 自然キー候補 | `(order_number, item_detail_id)` ※ 楽天の order_id は `order_number` |

---

## 2. 列定義 (21 列)

| # | 列名 | 型 | NULL | 用途 / 値域 |
|---|------|-----|------|-----------|
| 1 | id | INTEGER PK | NO | auto increment |
| 2 | order_number | TEXT | NO | 楽天注文番号 (例: `373343-20260401-0001417474`) |
| 3 | order_date | TEXT | NO | 注文日時 ISO8601 + JST offset (例: `2026-04-01T23:42:50+0900`) |
| 4 | order_status | INTEGER | NO | **業務前提: 700=完了 / 900=キャンセル / 200/300/500/600=途中** ※ §4 参照 |
| 5 | goods_price | REAL | NO | 注文単位の商品合計 (税込) |
| 6 | goods_tax | REAL | YES | 注文単位の税額 |
| 7 | total_price | REAL | NO | 注文単位の総額 (商品+送料、税込) |
| 8 | request_price | REAL | YES | 請求金額 |
| 9 | postage_price | REAL | NO | 注文単位の送料 (税込)、§5 SKU 按分対象 |
| 10 | coupon_shop_price | REAL | NO | 注文単位の **自社負担クーポン** (店舗負担分、税込) |
| 11 | coupon_all_total_price | REAL | NO | 注文単位の **全店クーポン総額** (楽天負担 + 自社負担合算、税込) |
| 12 | item_detail_id | INTEGER | NO | item 識別子 (注文内連番) |
| 13 | item_number | TEXT | NO | **楽天商品コード** (= rakuten_code、本契約では下流で `rakuten_code` に rename) |
| 14 | item_name | TEXT | YES | 商品名 (raw、`product_name` として下流に流す) |
| 15 | price | REAL | NO | item 単価 (**税抜**) |
| 16 | price_tax_incl | REAL | NO | item 単価 (**税込**)、Phase 1a の主指標 |
| 17 | units | INTEGER | NO | item 個数 (整数前提) |
| 18 | tax_rate | REAL | NO | 税率 (0.10 / 0.08) |
| 19 | selected_choice | TEXT | YES | 商品オプション選択値、§6 参照 |
| 20 | delete_item_flag | INTEGER | NO | 削除フラグ (全件 0、削除履歴は `raw_rakuten_orders_log` に集約) |
| 21 | synced_at | TEXT | NO | sync 取込時刻 |

### 列の含意 (補足)

- `coupon_shop_price` と `coupon_all_total_price` は注文単位なので、**SKU 別按分済の値が必要なら `fact_promotion_cost` (mall='rakuten') を参照** (Phase 0 で 30,014 件投入済)
- `price` (税抜) は本 contract では**直接使用しない** (Phase 1a 主指標は税込なので `price_tax_incl` を採用)
- `goods_tax` は「注文単位の税額」、SKU 別の税額は `price_tax_incl - price` で推算可能

---

## 3. 値域 / 統計 (2026-05-09 時点)

### order_status 分布

| status | 件数 | 比率 | 業務意味 |
|--------|------|------|---------|
| 700 | 342,186 | 91.0% | **完了** (お届け完了、売上確定) |
| 500 | 14,238 | 3.8% | 発送前 (準備中) |
| 600 | 12,302 | 3.3% | 発送後 (配達中) |
| 900 | 7,187 | 1.9% | **キャンセル** |
| 300 | 200 | 0.05% | 発送待ち (古い月にも残骸あり、異常状態) |
| 200 | 141 | 0.04% | 楽天処理中 (古い月にも残骸あり、異常状態) |

### selected_choice 分布

| 状態 | 件数 | 比率 |
|------|------|------|
| NULL or 空 | 238,912 | 63.5% |
| 値あり | 137,342 | 36.5% |

値あり行の中で同 `item_number` 内で `price_tax_incl` が複数値あるケース: **0.01% (50/376K)、microscale**。
値の中身は配送方法選択 / 産地確認 / 注意事項同意などが大半 (価格に影響しない確認系)。

### delete_item_flag

全 376,254 行で `0`。削除されたアイテムは `raw_rakuten_orders_log` (491K 行、変更履歴) に保存される。

### 期間

`min(order_date) = 2024-04-05`、`max(order_date) = 2026-05-09` 直近。月 12-15K orders × 1.13 items/order = 14-16K rows。

---

## 4. order_status の業務前提 (重要)

`f_rakuten_finance_sku_daily_v1` build では **`order_status = 700` のみを売上計上対象** とする。

### Why

- **二重計上回避**: 500/600 は出荷前/出荷後だが、status=700 への移行前にキャンセル (status=900 へ遷移) する可能性あり、「売上計上後にキャンセル」の二重控除リスク
- **会計安全側**: Amazon Phase 1 の `Settlement Report` 確定後ベースと同思想 (受注 vs 確定の分離)
- **数日遅れ許容**: 500/600 → 700 への移行は通常数日〜1 週間。月締め時点では大部分が 700 化 (memory 確認: 古い月の 200/300 残骸は微少)

### Out of scope (将来 Phase で扱う可能性)

- 速報性が必要な業務指標 (例: リアルタイム売上ダッシュボード) は別 view (`v_rakuten_orders_in_flight` 等) で 500/600 を含める設計を検討可能、ただし Phase 1a では実装しない
- 200/300 の異常残骸 (1 ヶ月以上経って 700 化していない) は monitoring 対象、本 contract では除外のみ

---

## 5. postage_price SKU 按分の業務前提 (重要)

`postage_price` は注文単位なので、SKU (item) 単位に按分する必要がある。**按分ロジック**:

```
postage_split[item_i] = postage_price × (price_tax_incl[item_i] × units[item_i]) / total_price
```

**前提**:
- `total_price` を分母に使う (商品 + 送料の総額)
- `total_price = 0` のエッジケースは `postage_split = 0` (zero-division ガード)
- 按分は **注文単位 (per `order_number`) で先に行い**、その後 `date_jst × rakuten_code` に再集約 (Codex Round 3 #1 指摘反映)

### 別案 (採用しない理由)
- 単純な「items 数で均等割り」 → 高単価 / 低単価が混在する注文で過小過大評価
- `goods_price` を分母にする → 送料の按分にならない

---

## 6. selected_choice の処理方針

- **集約で吸収**: PK = `(date_jst, rakiten_code)` のまま、`SUM(units)` / `SUM(sales_principal)` 等で吸収
- 同一 `(date_jst, rakuten_code)` で `COUNT(DISTINCT price_tax_incl) > 1` の場合 → `price_variance_warning = 1` フラグ立て、build log に warning 出力
- 0.01% (microscale) なので売上への影響は無視可能、ただし監視は維持

---

## 7. 関連 fact (依存先) の前提固定

### fact_promotion_cost (mall='rakuten')
- 行数: 30,014 (Phase 0 で投入済)
- 列 (13): `id, 日付, モール, 注文番号, raw_line_id, モール商品コード, 商品コード, クーポン金額, 全店クーポン金額, 店舗負担ポイント額, プロモーション金額, 按分方式, ingested_at`
- **既に SKU 別按分済**: `モール商品コード = item_number` (rakuten_code) で JOIN 可能
- 按分方式は `按分方式` 列に記録 (R-0a 別 ticket で値域確認)

### fact_returns (mall='rakuten')
- 行数: 7,138 (Phase 0 で投入済)
- 列 (14): `id, return_id, return_line_id, 返品日, 注文日, モール, 注文番号, モール商品コード, 商品コード, 数量, 返金額, 返品理由, 返品ステータス, ingested_at`
- 楽天分は **返品日 = 注文日** (差 0 日、月またぎ 0%)、JOIN は `注文日 = date_jst AND モール商品コード = rakuten_code` で OK

### f_rakuten_sku_map
- 行数: 6,244
- 列 (4): `rakuten_code, ne_code, source, updated_at`
- カバー率: rakuten_code → ne_code 解決可能率 (R-0a で計測予定、99% 以上想定)
- `source = 'w'` (web), `'al'` (auto-list) など (要 R-0a で値域確認)

---

## 8. contract 違反時の挙動

下流 (build) は本 contract を assert する `scripts/contracts/assert-rakuten-source.js` で fail-fast:
- 列の追加: 警告のみ (forward-compatible)
- 列の削除 / 型変更: error 終了
- `order_status` 値域に新値: 警告 (将来 700 と並ぶ status が追加された場合の検出)
- `delete_item_flag` に 1 が現れた: 警告 (削除フラグ運用変更検出)

---

## 9. 関連

- 設計書: `g:/共有ドライブ/AI_reference/システム設計/楽天Phase1a設計書_v0.3_20260509.md`
- Amazon Phase 1 同型 contract: `docs/contracts/raw_amazon_settlement_lines.contract.md`
- 検査 SQL: `sql/contracts/check_raw_rakuten_orders.sql` (本 PR で新規)
- assert script: `scripts/contracts/assert-rakuten-source.js` (本 PR で新規)
