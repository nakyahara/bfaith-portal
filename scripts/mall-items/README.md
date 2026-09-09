# mall-items — モール商品一覧の夜間 raw 保存 (商品の「昨日」を残す)

Company DB構想 06『商品データベース 設計調査』§7 **Step 0** (中原さん決定 2026-09-09 D-17: Postgres を待たずに始める)。

## なぜ

Amazon の出品レポートと楽天の `items/search` は、想定利益の夜間処理 (23:30) が毎晩**全件**取っている。
ところが保存していたのは価格と公開状態だけで、タイトル・説明・画像・JAN・属性・バリエーションは
毎晩取っては捨てていた。商品の「昨日と今日」を比べる材料がどこにも無い (06 §2.4)。
本命は Company DB の raw 層 (`raw.<src>_contents` + `raw.<src>_observations`、Phase 1) だが、
それまで 0 円で履歴を貯め始めるのがこの仕組み。ロジザード在庫の `scripts/logizard-stock/archive-snapshot.mjs` (#1204) と同じ型。

```
想定利益 nightly.js (miniPC 23:30)
  ├ fetch-listings.js fetchAmazonListings  → GET_MERCHANT_LISTINGS_ALL_DATA (TSV 原文)
  │     └→ archiveListings() → data/mall-items-history/amazon/YYYY/MM/items_amazon_YYYYMMDD_HHMMSS_<run_id>.tsv.gz
  ├ fetch-listings.js fetchRakutenListings → /es/2.0/items/search 全頁 (商品 JSON、variants 込み)
  │     └→ archiveListings() → data/mall-items-history/rakuten/YYYY/MM/items_rakuten_YYYYMMDD_HHMMSS_<run_id>.ndjson.gz
  └ 各 <mall>/manifest.jsonl に 1 行/回 (= 毎回の「観測の記録」)。任意で rclone offsite (gdrive:bfaith-backup/mall-items-history)
```

## 約束ごと

- **毎晩保存する (変化が無くても)**。「変化なしの日」と「取れなかった日」を区別するため (06 §11-1)。
  内容が前回と同じかは `content_hash` (行を並べ替えたハッシュ) の一致 = `same_as_previous` で分かる。
- **0 件は保存しない** (`code=empty`)。「取れなかった」を「無くなった」と読み替えない (空の入荷CSV事故 2026-09-08)。
- **部分取得は保存するが `complete=false`** (楽天のページ打ち切り・全体期限)。証拠にはするが、
  出品が消えた・停止したの判定には `complete=true` の回だけを使う (06 §11-2)。「前回比 80%」は完全取得の証明にならない。
- **保存の失敗でジョブの結果を変えない**。`price_fetch_run` / `listing_enum_status` は従来どおり。
  結果は `fetchXxxListings()` の戻り値 `archive` に載り、nightly の ok ping の note に「履歴ok」/「履歴NG: rakuten=部分取得」と写る。
- 同名 gz が既にあり中身も同じなら成功扱い (再実行で冪等)。別内容なら `COLLISION` で止まる (上書きしない)。
- 世代管理はしない (1 日 1 本 × 数 MB、永久)。Company DB へ backfill したあとに整理する。

## ファイルと manifest

- 置き場: `DATA_DIR/mall-items-history/<mall>/YYYY/MM/items_<mall>_YYYYMMDD_HHMMSS_<run_id>.<tsv|ndjson>.gz`
  (時刻 = 取得時刻 JST。run_id は expected-profit.db `price_fetch_run.run_id` と同じ)
- `<mall>/manifest.jsonl` の 1 行:
  `archived_at, snapshot_at (+09:00), mall, shop_id, source, run_id, file, format, encoding, items, rows, bytes, gz_bytes,
   sha256 (原文), content_hash (並べ替え後), same_as_previous, complete, enum_status, pages, truncated, deadline_hit, api_version, note`
- 読み方: `zcat amazon/2026/09/items_amazon_20260909_233105_r_xxx.tsv.gz | head` (UTF-8。Amazon の原文は Shift_JIS だが UTF-8 に直して保存)。
  楽天は 1 行 1 商品の JSON (`items/search` の要素そのまま。`item.manageNumber` 順)。
- サイズ感: Amazon 4,300 行 ≈ 2MB → 0.3MB / 楽天 6,500 SKU ≈ 20MB → 2MB。1 日 3MB 弱、年 1GB 弱。

## offsite (任意)

`MALL_ITEMS_RCLONE_REMOTE` があればそこへ、無ければ `BACKUP_RCLONE_REMOTE` (例 `gdrive:bfaith-backup/warehouse`) の
末尾を置き換えた `gdrive:bfaith-backup/mall-items-history` へ、履歴フォルダ全体を `rclone copy` (既存同一はスキップ、削除しない)。
失敗しても保存は完了 (`offsite=failed` を note に写す。次回に追いつく)。`BACKUP_RCLONE_CONFIG` は既存と共通。

## 手動で保存する (auPAY の商品 CSV など)

```
node scripts/mall-items/archive-items.mjs --mall aupay --file item.csv --format tsv --source wowma_item_csv --shop-id 54318092 [--no-offsite] [--dry-run] [--incomplete]
```

終了コード: 0 保存 / 3 0 件 / 4 offsite 失敗 / 1 保存失敗 / 2 引数不正。

## テスト

```
node scripts/test-mall-items-archive.mjs          # 保存の型 (gz・manifest・冪等・衝突・content_hash)
node apps/expected-profit/test-fetch.mjs          # fetch-listings のフック (fail-soft・complete・0 件)
node apps/expected-profit/test-nightly.mjs        # note への写し方
```

## これから (06 §7)

- PR-2: Yahoo `myItemList` (VPS プロキシ `parseMyItemListXml` が 3 列しか返さないので `<Result>` 全体を返す拡張 + Render 側は offsite 必須)
- PR-3: Amazon Catalog Items 毎晩全件 (D-18) / Qoo10 `GetAllGoodsInfo` / auPAY CSV のローカル分岐
- Phase 1: この gz 群を Company DB `raw.<src>_contents` / `_observations` へ backfill
