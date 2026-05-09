# raw_rakuten_orders Source Contract v1.1

**作成**: 2026-05-09 (v1)、改訂 2026-05-09 (v1.1: §4 D 案採用)
**ticket**: Phase 1a #R-0a + #R-1 (楽天版売上分析、AI_reference: `楽天Phase1a設計書_v0.4_20260509.md`)
**目的**: `f_rakuten_finance_sku_daily_v1` build の source として `raw_rakuten_orders` の schema / 値域 / 業務前提を凍結する。

## 改訂履歴
- v1 (2026-05-09): 初版、§4 で `order_status = 700` のみ売上計上
- v1.1 (2026-05-09): 楽天 status=700 が 2 ヶ月遅延と判明 → §4 を D 案に改訂 (status IN (500,600,700)、900 は fact_returns 経由で控除)

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

## 4. order_status の業務前提 (重要、v1.1 で D 案に改訂)

`f_rakuten_finance_sku_daily_v1` build では **`order_status IN (500, 600, 700)` を「受注ベース」で売上計上対象** とする。
`order_status = 900` (キャンセル) と `200/300` (異常残骸) は **silver から除外**。
キャンセル数量は別途 `fact_returns` から `units_cancelled` として控除 (= D 案、Codex セカンドオピニオン 2026-05-09 確定)。

### Why (v1 の 700 only から改訂した理由)

2026-05-09 検証で楽天 `status=700` の月別比率が判明:
- 2026-05 (当月): 0%
- 2026-04 (先月): 0%
- 2026-03: 31%
- 2026-02: 98%
- 2025-12 以前: 98%

→ **status=700 は約 2 ヶ月遅延**。「月初に先月売上を確定」「直近ダッシュボード可視化」の運用要件を満たせない。

700 only の方針は memory `feedback_per_mall_data_design.md` (「楽天は受注時点確定型」) と矛盾。Amazon の Settlement 思想を楽天に持ち込むのは不整合。

### D 案 (採用) の整合性条件 (Codex 確定、検証済 2026-05-09)
1. ✓ 売上 fact 側で `900` を計上しない (silver で IN (500,600,700) に絞り込み)
2. ✓ 同一 order line が売上 fact で一度しか立たない (raw は latest-state-only 型、`(order_number, item_detail_id)` 重複ゼロを 376,254 行で確認)
3. ✓ `fact_returns` がキャンセル/返品を一意キーで重複なく保持 (return_id PK)
4. ✓ 月次定義 = 「受注純額ベース、後日キャンセルは遡及反映」
5. ✓ `fact_returns` と silver(IN 500/600/700) の overlap = 0 (二重計上リスクなし、検証済)

### units の定義
- `units_ordered` = SUM(units) WHERE status IN (500, 600, 700) AND 当月
- `units_cancelled` = SUM(数量) FROM fact_returns LEFT JOIN ON (date_jst, rakuten_code) — 同 (date, code) ペアのみ
- `units_net_sold` = MAX(0, units_ordered - units_cancelled) (負値ガード、4 月実測 6 件)

### Phase 1a 既知の限界 (Codex 第 2 回評価 2026-05-09 確定)
- fact_returns 注文日 と silver date_jst が日付不一致な (date, code) ペアは LEFT JOIN で漏れる
- 4 月実測: 88 units (cancel 全体 234 units の 36%)、refund_amount で ¥153K
- 影響: units_cancelled / refund_amount が若干過小、cogs / variable_margin はほぼ影響なし
- C-lite (UNION で refund-only 行追加) を試したが粗利 KPI 改善せず却下
- **Phase 1b 推奨**: 同月・同 rakuten_code で units_ordered 比例 cancel 配賦
  - 詳細: `g:/共有ドライブ/AI_reference/システム設計/楽天Phase1b案_C-lite検討メモ_20260509.md`
- build script の validation で「日付不一致 SKU 数」を warning 表示済

### 売上 / 原価 / 送料の扱い
- 売上 (sales_principal / sales_postage) = silver 由来、status=900 は含まれない
- 原価 (cogs) / 送料 (shipping) = `unit × MAX(0, units_ordered - units_cancelled)` で再計算
- mall_fee (10%) は `(sales_principal + sales_postage - coupon_all) × 0.10` で units_ordered ベース
  - キャンセル時の手数料返還を厳密に反映するなら × (units_net_sold/units_ordered) だが、影響 1-2% で省略 (将来 Phase 検討)

### Out of scope
- 200/300 の異常残骸 (1 ヶ月以上経って完了化していない、各月数百件) は売上定義に入れない、別途 monitoring 対象
- C 案 (受注 + 完了の 2 列持ち) は経営層が `completed` 指標を実際に使うまで見送り (Phase 1a はシンプルに寄せる)

---

## 5. postage_price SKU 按分の業務前提 (重要)

`postage_price` は注文単位なので、SKU (item) 単位に按分する必要がある。**按分ロジック**:

```
items_total[order_n] = SUM(price_tax_incl[item_i] × units[item_i])  -- 同一 order_number 内の商品金額合計
postage_split[item_i] = postage_price × (price_tax_incl[item_i] × units[item_i]) / items_total[order_n]
```

**前提**:
- 分母は **同一 order_number 内の商品金額合計** (= `SUM(price_tax_incl × units)` per order、`goods_price` 相当)
- これにより SKU 別按分の合計 = `postage_price` 完全保存 (送料漏れなし)
- `items_total = 0` のエッジケース (例: 全部 free item) は `postage_split = 0` (zero-division ガード)
- 按分は **注文単位 (per `order_number`) で先に行い**、その後 `date_jst × rakuten_code` に再集約 (Codex Round 3 #1 指摘反映)

### 別案 (採用しない理由)
- **`total_price` (商品+送料) を分母にすると送料漏れ発生** ← Codex PR #75 #1 指摘で発覚
  - 例: 商品 1,000 + 送料 500 → `total_price=1,500` で按分すると合計 333.33、166.67 消失
  - 過去の v0.3 設計書はこの誤りを含んでいた、本 contract で修正
- 単純な「items 数で均等割り」 → 高単価 / 低単価が混在する注文で過小過大評価

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

- 設計書: `g:/共有ドライブ/AI_reference/システム設計/楽天Phase1a設計書_v0.4_20260509.md` (v1.1 と同期)
- Amazon Phase 1 同型 contract: `docs/contracts/raw_amazon_settlement_lines.contract.md`
- 検査 SQL: `sql/contracts/check_raw_rakuten_orders.sql`
- assert script: `scripts/contracts/assert-rakuten-source.js`
- D 案決定の根拠: Codex セカンドオピニオン (2026-05-09、`C:/tmp/codex-prompt-rakuten-status-strategy.txt` + 結果 `C:/tmp/codex-rakuten-status.txt`)
