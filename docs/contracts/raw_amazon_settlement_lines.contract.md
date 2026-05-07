# `raw_amazon_settlement_lines` Source Contract (v1)

**Status**: frozen (2026-05-07)
**Phase 1 ticket**: #1-0a
**Owner**: Render Amazon Phase 1
**Reviewer**: bfaith-portal warehouse team

このドキュメントは Render Amazon 分析基盤 Phase 1 における唯一の daily source である `raw_amazon_settlement_lines` の **schema contract** を凍結したもの。Phase 1 以降の metric mapping、daily fact build、sync、validation は全てこの contract に依存する。

`v_amazon_sku_profit_actual_v4` は **validation reference として併存** するが、daily source ではない。daily fact (`f_amazon_finance_sku_daily_v1`) は raw から economic_date 軸で再構築する。

## Contract version

- **v1** — 初回凍結 (2026-05-07)

contract version は SQL / 実装コード側でも参照され、列追加・列削除・型変更を検知する schema check が non-zero exit で fail させる。

## 1. Source identity

| 項目 | 値 |
|---|---|
| データベース | `data/warehouse.db` (better-sqlite3 + WAL、miniPC) |
| テーブル名 | `raw_amazon_settlement_lines` |
| オブジェクト型 | `table` |
| 行数 (2026-05-07 時点) | 3,152,595 |
| daily 主軸 | `economic_date` |
| 業務 SKU 主軸 | `seller_sku_normalized` |
| 金額単位 | **micro** (1 / 1,000,000)。JPY 化は `/ 1000000.0` |
| 通貨 | `JPY` (`currency` 列、`NOT NULL DEFAULT 'JPY'`) |

## 2. 日付軸の採用ルール

`raw_amazon_settlement_lines` には 4 種の日付列が存在する。

| 列 | 採用可否 | 用途 |
|---|---|---|
| `posted_date_utc` | ❌ daily 主軸として使わない | Amazon 側 posted 時刻 (UTC)。参考のみ |
| `posted_datetime_jst` | ❌ daily 主軸として使わない | JST 化済 posted 時刻。UI 表示候補 |
| **`economic_date`** | ✅ **daily 主軸として採用** | 会計帰属日。`v_amazon_sku_profit_actual_v4` の `year_month_int` 由来となる column |
| `year_month_int` | ✅ 月次 validation の join key | 月次の整数値表現 (YYYYMM) |

**daily fact の grain は `(economic_date, seller_sku_normalized)`** で固定。

## 3. 列定義 (固定)

`pragma_table_info('raw_amazon_settlement_lines')` の出力を以下に凍結する。

| cid | 列名 | 型 | NOT NULL | 用途 / 備考 |
|---|---|---|---|---|
| 0 | `id` | INTEGER | 0 | PK |
| 1 | `physical_line_hash` | TEXT | 1 | 物理行ハッシュ (重複検出) |
| 2 | `business_line_key` | TEXT | 1 | 業務行キー |
| 3 | `source_document_id` | TEXT | 1 | 元 settlement document ID |
| 4 | `source_file_hash` | TEXT | 0 | 元ファイルハッシュ |
| 5 | `source_path` | TEXT | 0 | 取込元 path |
| 6 | `source_line_no` | INTEGER | 0 | 元ファイルの行番号 |
| 7 | `source_layer` | TEXT | 1 | bronze/silver/gold 等の layer 識別 |
| 8 | `parser_version` | TEXT | 1 | parser version |
| 9 | `source_settlement_id` | TEXT | 1 | settlement 単位 ID |
| 10 | `posted_date_utc` | TEXT | 1 | UTC 時刻 |
| 11 | `posted_datetime_jst` | TEXT | 1 | JST 化済 |
| 12 | **`economic_date`** | TEXT | 1 | **daily 主軸 (採用)** |
| 13 | `year_month_int` | INTEGER | 1 | 月次 join key |
| 14 | `amazon_order_id` | TEXT | 0 | 受注 ID |
| 15 | `merchant_order_id` | TEXT | 0 | 店舗側受注 ID |
| 16 | `shipment_id` | TEXT | 0 | 出荷 ID |
| 17 | `order_item_code` | TEXT | 0 | 受注明細コード |
| 18 | `adjustment_id` | TEXT | 0 | 調整 ID |
| 19 | `seller_sku` | TEXT | 0 | 元 SKU 文字列 |
| 20 | **`seller_sku_normalized`** | TEXT | 0 | **正規化済 SKU (daily fact 主軸)** |
| 21 | `transaction_type` | TEXT | 1 | 取引種別 (Order/Refund/Reimbursement 等、distinct 値は別文書) |
| 22 | `marketplace_name` | TEXT | 0 | marketplace 名 |
| 23 | `fulfillment_id` | TEXT | 0 | フルフィルメント ID |
| 24 | `quantity_purchased` | INTEGER | 0 | 数量 |
| 25 | `price_type` | TEXT | 0 | 価格種別 (Principal/Shipping/Giftwrap/Tax 等) |
| 26 | `price_amount_micro` | INTEGER | 0 | 価格金額 (micro) |
| 27 | `item_related_fee_type` | TEXT | 0 | 商品関連手数料種別 (Commission/FBAPerUnitFulfillmentFee/FBAStorageFee/ClosingFee 等) |
| 28 | `item_related_fee_amount_micro` | INTEGER | 0 | 商品関連手数料金額 (micro) |
| 29 | `promotion_id` | TEXT | 0 | プロモーション ID |
| 30 | `promotion_type` | TEXT | 0 | プロモーション種別 |
| 31 | `promotion_amount_micro` | INTEGER | 0 | プロモーション金額 (micro) |
| 32 | `shipment_fee_type` | TEXT | 0 | 出荷手数料種別 (Shipping 等) |
| 33 | `shipment_fee_amount_micro` | INTEGER | 0 | 出荷手数料金額 (micro) |
| 34 | `order_fee_type` | TEXT | 0 | 受注手数料種別 (ShippingChargeback/GiftwrapChargeback 等) |
| 35 | `order_fee_amount_micro` | INTEGER | 0 | 受注手数料金額 (micro) |
| 36 | `misc_fee_amount_micro` | INTEGER | 0 | その他手数料金額 (micro) |
| 37 | `other_fee_amount_micro` | INTEGER | 0 | その他手数料金額 (micro) |
| 38 | `other_fee_reason_description` | TEXT | 0 | その他手数料理由説明 |
| 39 | `direct_payment_type` | TEXT | 0 | 直接支払種別 |
| 40 | `direct_payment_amount_micro` | INTEGER | 0 | 直接支払金額 (micro) |
| 41 | `other_amount_micro` | INTEGER | 0 | その他金額 (micro) |
| 42 | `currency` | TEXT | 1 | 通貨 (`'JPY'` default) |
| 43 | `ingest_run_id` | TEXT | 0 | ingest run ID |
| 44 | `observed_at` | TEXT | 0 | 観測時刻 |
| 45 | `ingested_at` | TEXT | 0 | 取込時刻 |

**列数: 46**。

## 4. 関連オブジェクト (Phase 1 が依存するもの)

| 名前 | 型 | 役割 |
|---|---|---|
| `raw_amazon_settlement_lines` | table | **daily fact のソース** |
| `raw_amazon_settlement_headers` | table | settlement ヘッダ |
| `fact_amazon_settlement_monthly_wide` | table | v4 が起点に使う月次集約 (validation 用) |
| `fact_amazon_settlement_monthly_long` | table | 月次集約 long 形式 |
| `v_amazon_sku_profit_actual_v4` | view | **validation reference** (daily source ではない) |
| `v_amazon_settlement_unified` | view | 既存の統合 view |
| `v_settlement_v3_v4_validation` | view | v3/v4 差分検証 |
| `v_sku_costed` | view | latest 原価 view (Phase 1 では **触らない**) |
| `v_sku_resolved` | view | `v_sku_costed` の元 |

## 5. 量・単位の取り扱い

- 全 `*_micro` 列は **integer micro 単位** (1 / 1,000,000 倍)。JPY 化は `/ 1000000.0`。
- `quantity_purchased` は **integer 個数**。Order trx で正、Refund trx で負 (or 絶対値で別カウント) になる。daily fact 側では `tx_bucket` で符号を扱う。
- `currency` は `JPY` 固定の前提 (`NOT NULL DEFAULT 'JPY'`)。USD 等が混入したら DQ alert。

## 6. 行の同一性

| 列 | 意味 |
|---|---|
| `physical_line_hash` | 取込済 row の物理ハッシュ。同一なら重複行 |
| `business_line_key` | 業務的に同じ行 (再取得して内容が一致するもの) |
| `source_settlement_id` | settlement document の単位 |
| `ingest_run_id` | ingest 実行ごとの run ID |

## 7. 採用しない / 触らない方針

- **`v_sku_costed` は触らない** (latest 原価 view、既存ロジック維持)。
- **`v_amazon_sku_profit_actual_v4` を daily source として使わない** (月次起点なので daily 派生不可)。
- **`posted_date_utc` / `posted_datetime_jst` を daily 主軸にしない** (`economic_date` を採用)。

## 8. transaction_type の取り扱い

`transaction_type` の distinct 値一覧は **別文書 `docs/contracts/transaction_taxonomy_seed.csv`** で管理する (Ticket #1-0a の deliverable として作成)。

distinct 値は `Order` / `Refund` / `Adjustment` / `SAFE-T` / `A-to-z Guarantee` / `Service Fee` 等の bucket に分類されるが、exact 値は実 DB から抽出して凍結する。SQL 本体への hard-code 散在は禁止、必ず `map_amazon_transaction_type_v1` table か CTE に集約する。

## 9. Schema drift 検知

`scripts/contracts/assert-raw-amazon-source.js` がこの contract と実 DB を突合し、列追加・列欠落・型変更を検知する。CI または手動チェックで mismatch 時に `process.exit(1)` で fail させる。

## 10. baseline metrics (2026-05-07)

| 指標 | 値 |
|---|---|
| total rows | 3,152,595 |
| 列数 | 46 |
| `economic_date` 範囲 | (要計測、`sql/contracts/check_raw_amazon_settlement_lines.sql` で取得) |
| 月別 row 数 | 同上 |

これらは contract の v1 凍結時点での baseline。Phase 1 実装中に row 数が大幅変動 (例: ±10% 超) したら DQ alert。

## 11. 関連 ticket / docs

- Ticket #1-0 (canonical metric mapping) — この contract を前提に metric を確定する
- Ticket #1-1 (`f_amazon_finance_sku_daily_v1`) — daily fact build の source
- Ticket #1-3 (v4 validation) — `v_amazon_sku_profit_actual_v4.settlement_margin_excl_tax` と突合
- `docs/amazon-finance/phase1-plan.md` — Phase 1 全体計画

## 12. 改訂履歴

| Version | Date | Changes |
|---|---|---|
| v1 | 2026-05-07 | 初回凍結。SSH 経由で実 DB を確認し、46 列・economic_date 主軸・micro 単位を確定 |
