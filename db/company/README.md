# Company DB (PostgreSQL) — 会社全体の正規 DB (Phase 1: 商品・SKU・販路商品・人)

正本 = AI_reference『システム設計/CompanyDB構想/』00 開発方針 / 02 正本マップ / 03 内部ID設計 / **06 商品データベース設計調査**。
ここは「その設計を実際に作れる形」= マイグレーション SQL と実行器。

```
db/company/migrations/NNNN_*.sql     … 番号順に 1 回だけ流す (適用済みは書き換えない。直すときは次の番号)
scripts/company-db/migrate.mjs       … 実行器 (ops.schema_migrations に記録、checksum で改変を検知)
scripts/test-company-db-ddl.mjs      … PGlite (WASM の Postgres) で全部流して 03 §10 のセルフチェックを機械で固定
```

## 層 (スキーマ)

| スキーマ | 役割 | 例 |
|---|---|---|
| `raw` | 取ったまま。1 ソース 2 表 (`<src>_contents` = 中身の重複排除 / `<src>_observations` = 毎回の観測) | `raw.rakuten_items_*`, `raw.amazon_listing_report_*` |
| `core` | 解決済みの今。内部 ID (product / sku / listing) と外部 ID (`external_ids`)、属性の観測と解決 | `core.products`, `core.skus`, `core.listings`, `core.catalog_items` |
| `snapshots` | 日次の記録 (月パーティション、append-only) | `snapshots.listing_daily` |
| `events` | 変化 (誰が・いつ・何を。append-only、`idempotency_key` unique) | `events.price_change_events` |
| `ai` | 判断・所見・Action Queue・評価・見張り規則 | `ai.decisions`, `ai.watch_rules` |
| `docs` | 文書台帳 (実体は Drive) | `docs.documents` |
| `ops` | 取込・ジョブの記録、マイグレーション記録 | `ops.ingest_runs`, `ops.schema_migrations` |
| `mart` | AI と画面が読む層 (view) | `mart.v_product_360` (SKU 1 行に全部) |

## 粒度と ID (03 §2 / 06 §5.2)

- `product` = カタログ上の商品 (JAN 単位が目安) / `sku` = 在庫・出荷の単位 (NE 商品コード) / `listing` = モール × 出品コード
- 外部 ID (JAN・NE コード・seller_sku・楽天 manageNumber…) は列を増やさず `core.external_ids` 1 表 (履歴・解決根拠つき)
- **ASIN は product の外部 ID にしない**: `core.catalog_items` (marketplace × ASIN、包装範囲つき) に置き、listing と 1:N、product へは構成数量つきで解決
- 商品コードの比較は `core.norm_code()` (= `lib/sku-norm.js` の `normSku()` と同じ結果。試験で固定)
- 金額は `bigint` 円 (`*_jpy`)、時刻は `timestamptz`、業務日は `core.jst_date()` の JST 日付

## 使い方

```
# 1. 試験 (Postgres 不要。PGlite で全部流す)
node scripts/test-company-db-ddl.mjs

# 2. 本物に流す (Render Postgres 等)。COMPANY_DB_URL は Render の Internal/External Database URL
COMPANY_DB_URL=postgres://user:pass@host:5432/dbname node scripts/company-db/migrate.mjs --list
COMPANY_DB_URL=... node scripts/company-db/migrate.mjs --dry-run
COMPANY_DB_URL=... node scripts/company-db/migrate.mjs
```

- 接続: Render の **External URL** (miniPC から) は TLS 必須で、証明書は検証する。**Internal URL** (Render 内のアプリから) は TLS 無し。ホスト名にドットがあるかで自動判定。検証を切る手段は用意しない (繋がらないときは接続先を疑う)
- 1 ファイル 1 トランザクション。途中で失敗したファイルは巻き戻り、前のファイルまでは適用済みのまま (`--list` で状態が見える)
- 適用済みファイルの内容を変えると `checksum 不一致` で止まる。**直すときは次の番号のファイルを足す**
- 秘密情報 (接続文字列) は `.env` / Render の環境変数に置く。リポジトリに書かない

## 初期ロード (既存の SQLite → Company DB)。PR-B

読み込み元は Render の `DATA_DIR` にある SQLite (`apps/company-db/load/sources.mjs`): mirror_products / mirror_set_components / mirror_sku_master + resolved / mirror_rakuten_sku_map / mirror_qoo10_items / mirror_amazon_sku_fees / product_drafts + draft_page_info + draft_sku_jans / バーコードマスタ / f_inbound_info / po_suppliers + po_vendor_code_map / fba.db (ASIN・JAN・FNSKU) / rakuten-yahoo-sync.db (Yahoo の出品・Notion の JAN) / postage.db (実測重量) / fba-box.db (SP-API 重量・実測) / staff.db。

```
# Render の Shell で (DATA_DIR / COMPANY_DB_URL は env にある)
node apps/company-db/load/run-initial-load.mjs           # dry-run: 全部やって巻き戻す。report だけ残す
node apps/company-db/load/run-initial-load.mjs --apply   # 本適用
# または miniPC から (x-sync-key = MIRROR_SYNC_KEY)
curl -s -X POST -H "x-sync-key: $MIRROR_SYNC_KEY" "$RENDER_MIRROR_URL/apps/company-db/sync/load"            # dry-run
curl -s -X POST -H "x-sync-key: $MIRROR_SYNC_KEY" "$RENDER_MIRROR_URL/apps/company-db/sync/load?apply=1"    # 本適用
curl -s -H "x-sync-key: $MIRROR_SYNC_KEY" "$RENDER_MIRROR_URL/apps/company-db/sync/status"                  # 直近の report と件数
```

約束 (`apps/company-db/load/engine.mjs`):
- 1 回 = 1 トランザクション。dry-run は本番と同じ検査を全部通してから巻き戻す
- 冪等: 何度流しても増えない (upsert / 観測は `observation_key` で on conflict do nothing / 原価は値が変わったときだけ有効期間を付け替え)
- **予定 = 投入 + 理由つき skip** でなければ巻き戻す (skus / products / 構成 / 出品 / 出品の構成)。親不在 (子 SKU が無い、出品の NE コードが無い) は skip の理由として report に残す
- 出どころの食い違いは `report.conflicts` (ASIN: fees vs Sheet / JAN: product_hub vs ロジザード vs Sheet vs Notion / ブランド: product_hub vs Qoo10 / JAN の取り合い) — 両方には付けず、規則 v1 の優先で 1 つ採用。**不一致一覧は人が見る材料** (06 §5.6 の名寄せレポート)
- 入数 (`f_inbound_info.入数`) は観測として残すだけで採用しない (D-20: 意味を確認してから規則を足す)
- report = `DATA_DIR/company-db/load-<run_id>.json / .md` + `latest.json`。`ops.ingest_runs` にも 1 行

## Phase 1 でやること・やらないこと (04 §Phase 1)

- やる: この DDL を Render Postgres に流す → 既存 SQLite (m_products / m_sku_master / f_rakuten_sku_map / fba.db / product_drafts …) から初期ロード (`scripts/company-db/load-*.mjs`、投入予定 vs 実投入の diff レポート必須) → 名寄せレポート (JAN / ASIN / 入数の不一致) → `mart.v_product_360` で 1 商品 1 行
- やらない: warehouse.db・mirror 同期・既存アプリは触らない (並走)。受注・売上・在庫イベントは Phase 3/6
- ホスティング: 推奨 A = Render Postgres (03 §8)。計算 (インスタンス) とストレージ (GB) は別課金。Phase 1〜3 はストレージ 10GB で足りる見込み (06 §5.5)

## 変えるときの約束 (03 §10 セルフチェック = test-company-db-ddl.mjs が機械で見るもの)

- 円の金額列は `bigint` / append-only 表に `updated_at` を置かない / Canonical 表 (core の 15 表) に `company_id` と監査列
- **append-only は trigger で強制** (`core.make_append_only`): raw の 2 表・events・属性の観測・AI の記録 (reviews / results / outcomes)・ops.job_runs は UPDATE / DELETE / TRUNCATE が拒まれる。保持期間の整理は同じトランザクションで `alter table … disable trigger trg_append_only_row` してから
- **正規化列は生成列** (`code_norm` / `listing_norm` / `external_norm`)。手で入れられない。`core.norm_code` と `lib/sku-norm.js normSku` の一致は fixture 28 件で固定 (ECMAScript の空白集合を列挙。NFKC は使わない)
- 外部 ID の付け替えは `valid_to` を埋めてから新行 / 原価の有効行は 1 つ / 単品 product : sku は 1:1 (部分 unique) / **product・sku に直接 ASIN は付けられない** (CHECK。`core.catalog_items` 経由)
- 属性の観測は「観測の回」(`observation_key`) で一意。A→B→A も同じ値の再観測も残る。解決結果 (`attribute_resolutions`) は観測の対象・属性・包装範囲と複合 FK で一致し、**採用した観測の source がその版の規則に載っている**ことを trigger が見る。規則 (`attribute_resolution_rules`) と版 (`rule_versions`) は append-only (直すときは新しい版を足す)
- 文言 (`listing_texts`) は観測時刻で一意 (A→B→A が残る)。current は 1 行だけ。本文の書き換え・削除は trigger が拒む (UPDATE は is_current の付け替えだけ)
- **親子の会社は複合 FK で一致** (`(company_id, product_id)` など)。会社 1 の SKU に会社 2 の構成行・原価・出品対応は入らない
- 価格の比較 (v_product_360 の min/max、v_cross_mall_diff) は **単品出品 (構成 1 行・qty=1)** だけ。組合せ出品の価格を単品の価格にしない
- 月パーティションは `snapshots.ensure_month_partitions(from, to)` で作る。作り忘れても default に入って落ちない。**後から作ると default の行をその月に移してから attach する** (同一トランザクション)
- 実行器: 番号は 0001 からの連番 (欠番は不正)。DB に適用記録があるのにファイルが無い checkout では流さない
