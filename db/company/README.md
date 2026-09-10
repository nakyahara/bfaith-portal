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
# または miniPC から (認証はヘッダ x-sync-key = MIRROR_SYNC_KEY だけ。?sync_key= は受けない)
#   🚨 RENDER_MIRROR_URL は末尾に /apps/mirror が付いている。curl で直接叩くなら origin (https://<host>) だけ使う。下のスクリプトはそれをやる
node scripts/company-db/remote-load.mjs load --wait            # dry-run を開始 → 202 {run_id} → 終わるまで待って last / latest を表示
node scripts/company-db/remote-load.mjs load --apply --wait    # 本適用
node scripts/company-db/remote-load.mjs status [--counts]      # current (実行中) / last / latest.json / interrupted / (--counts で Postgres の件数)
node scripts/company-db/remote-load.mjs reports                # report の一覧
node scripts/company-db/remote-load.mjs report <run_id> --out load.json   # その回の明細 (conflicts / unresolved / sections の skip 理由)。--md で Markdown
```

HTTP は結果を待たない (数分かかるので Render の HTTP 制限で切れる)。実測: 7,242 SKU / 14,274 出品 / 8,414 観測の dry-run = **約 10 秒** (2026-09-10、Render Internal 接続)。`POST /load` は 202 で `run_id` を返し、`GET /status` の `current` (実行中) → `last` (終わった直近。`status` = done / failed) と `latest.json` で結果を見る。plan を作る前 (SQLite が無い・Postgres に繋がらない) で落ちても `latest.json` に失敗が残る。
開始したことは `running.json` に永続化する (書けなければ始めない)。終了記録 (report / latest.json) を書けたときだけ `running.json` を消す。プロセスが途中で死ぬ・結果を書けないと `running.json` が残り、`/status` の `interrupted` に出る (`committed` = `ops.ingest_runs` にその run があるか。true なら本適用は済んでいて report だけ無い)。

約束 (`apps/company-db/load/engine.mjs`。試験 `apps/company-db/test-initial-load.mjs` が固定):
- 1 回 = 1 トランザクション。dry-run は本番と同じ検査を全部通してから巻き戻す。途中の SQL エラーも全部巻き戻る
- **全区分で 予定 (除外する前の件数) = 投入 + 既存と同じ + 理由つき skip** でなければ `LOAD_UNBALANCED` で巻き戻す (skus / products / 構成 / 原価 / 仕入先 / 出品 / 出品の構成 / catalog_items / ASIN の紐付け / 外部 ID / FNSKU の解除 / NE コード / 観測 / JAN / 解決 / 物理属性 / 表示義務 / 人)。取り合い・不採用・親不在 (子 SKU が無い、出品の NE コードが無い) は skip の理由として report に残す
- 冪等: 何度流しても増えない (upsert / 原価は値が変わったときだけ有効期間を付け替え)。**観測の再送判定**: 出どころに時刻がある入力は「同じ出どころ・同じ参照・同じ内容・同じ観測時刻」が既にあれば再送 (入れない)。同じ内容でも新しい時刻なら新しい観測 (採用順に効く)。時刻の無い入力 (Sheet / Notion / Qoo10 等) は「その出どころ・参照の最新の観測と同じ内容」なら再送、違えばロード時刻で新しい観測。A→B→A は 3 行残る (キーは run ごと)。物理属性も同じ (全属性 + 出どころ + 参照 + 時刻で判定)
- 採用 (規則 v1) は「出どころ × 参照ごとの最新の観測」だけを候補にし、規則の優先 → 観測時刻の新しい順。不一致は `出どころ:参照` ごとの値で `report.conflicts`
- **正規化衝突で落とした SKU / 出品は、以降の処理 (構成・属性・親・外部 ID・listingRef 経由の観測) でも一切使わない** (隔離 = 原文のコードが一致するときだけ解決する)
- 出どころの食い違いは `report.conflicts` (ASIN: fba_sku_attrs vs Sheet vs fees / FNSKU / JAN: product_hub vs ロジザード vs Sheet vs Notion / ブランド: product_hub vs Qoo10) — 両方には付けず、規則 v1 の優先で 1 つ採用。ASIN は `ASIN_SOURCE_PRIORITY` (出品一覧 → fba_sku_attrs → Sheet → fees。出品一覧は raw 層が入る PR-D から)、FNSKU は `FNSKU_SOURCE_PRIORITY`。**不一致一覧は人が見る材料** (06 §5.6 の名寄せレポート)
- **外部 ID の移動計画**: 先に全部読み、全部の要求を集め、固定点で解いてから「閉じる → 付ける」。既に同じ値を持つ = same (保持。取り合いにも移動にも関わらない) / 同じ値を複数が要求 = `*_contended` (誰にも付けない) / 別のエンティティが持つ値は、持ち主が今回 別の値へ移り (新規に通る要求がある) かつその値を保持しない (same でない) ときだけ手放す。移れなければ `*_taken` (連鎖の途中で止まればその前も止まる。入れ替え (循環) は通る) / 閉じるのは「新規に通る要求があるエンティティの、保持しない (same でも新規でもない) 有効行」だけ / 人が付けた行 (`manual`) は閉じない・その横に別の値を自動で付けない (`*_manual_kept`)。付かなかった product には解決結果も書かない
- **FNSKU の明示的な解除** (fba_sku_attrs が planning / restock で空にした) は既存の自動付与を閉じる (`fnsku_cleared`)。単なる欠落 (候補が無いだけ) では閉じない。解除は外部 ID の移動判定より先にやる (解除した FNSKU を同じ回で別の出品が要求しても 1 回で移る)
- **未来の観測時刻**: 入力はロード時刻 + 5 分まで許容 (時計ずれ)、それより先は理由つきで入れない。時刻の無い再送の比較対象 (「最新」) はロード時刻以前の行だけ (5 分以内の未来行が最新に居座ると毎回増える)。採用の候補・有効行に 5 分超の未来行は数えず、既に採用されている未来由来の解決・有効行はその回で解除する (`future_revocations` 区分、`resolution_future_revoked` / `physical_future_revoked`)。ロード時刻が進めば未来行は普通の行に戻る (永久隔離ではない)
- **JAN・重量は「単品 1 個」(構成 1 行・qty=1・その SKU が単品) の出品からだけ商品に付ける**。複数個パック・セット (セット SKU × 1 も) の出品に付いた JAN / 重量は listing の属性 (`packaging_scope = 'listing'`) として残す。重量の FNSKU 逆引きは採用される FNSKU だけで、さらに engine が「その出品にその FNSKU が実際に付いた (same / 新規 / manual)」ときだけ入れる (`via`。DB 側で manual / 取り合いに負けた FNSKU の重量は理由つき skip)
- 楽天の別名 (AM > AL > W) は 1 listing にまとめるが、同じ商品ページ・同じ NE コードに **AM が 2 つ以上あるグループは束ねない** (行ごとに listing、`plan.sources.rakuten_alias_ambiguous` に記録)
- ロジザードのバーコードは rank 0 だけ `jan`。それ以外は `jan_secondary` (残すが採用しない)
- 店舗キー (`shop_code`) は `SHOP_CODES` の定数 (Amazon = `main@<marketplace>`、他は `main`)。2 店舗目ができたら値を足す
- **今回「完全に読めた」出品 / セット親 (plan に構成が 1 行以上あり、skip が 1 件も無いもの) の構成だけ plan に合わせる** (plan に無い行は消す)。空・読めない・未解決・重複ありは触らない。人が手で確定した行 (`resolution` / `source` = `manual`) は消さず、plan に無ければ `*_manual_kept`、数量が違えば `*_manual_mismatch` + skip (manual の値を保つ)
- **バリエーションのまとまり (D-24 = A)**: NE の 代表商品コード は実在しない「名札」(色違い・サイズ違いのグループ鍵。実データで 2,133 商品のうち 2,128 が指す先が m_products に無い) なので、名札ごとに **SKU を持たない product** を作って子を `parent_product_id` で束ねる。名前は子の商品名から決める (`variationGroupName`: 「【」より前が 2 件以上同じならそれ → 最長共通接頭辞 → 代表コード)。状態は子に取扱中があれば active。代表コードが**実在する単品 SKU** ならその product を親にする (名札は作らない)。**セット・例外 SKU** を指すなら名札にしない (`variation_parent_not_single`)。同じ `display_code` の product が 2 件以上なら決めない (`variation_parent_ambiguous`)。親子が循環するなら付けない (`variation_parent_loop`)。**名札の名前は作ったとき 1 回だけ** (作成者によらず、あとで人が直しても、出どころの子が増えて良い名前になっても、機械は書き戻さない)。**状態 (active / discontinued) は子の取扱区分から毎回決める** (業務データなので上書きが正しい)。同じ子に違う親の候補が来たら決められないので全部 skip (`variation_parent_conflict`)。まとまりは SKU を持たないので `mart.v_product_360` には出ない (買える商品だけ)
- 入数 (`f_inbound_info.入数`) は観測として残すだけで採用しない (D-20: 意味を確認してから規則を足す)
- 観測時刻は出どころの更新時刻 (product_drafts / draft_page_info / pm_skus / fbx_weight_*)。🚨 **「取り込んだ時刻」は観測時刻に使わない** (ロジザードのバーコードマスタ・f_inbound_info は CSV 取込のたびに全行の updated_at が変わるので、内容が同じでも毎回新しい観測になる。2026-09-10 に 1 回のロードで 2,590 行増えて気づいた)。時刻なし (null) で渡し、「その出どころ・参照の最新と同じ内容なら再送」に任せる
- 既存の読み込みは今回の対象 (product / listing) に絞る (観測・物理属性・解決)。7,000 SKU 規模の本番所要時間は初回 dry-run で計測して README に書く
- report = `DATA_DIR/company-db/load-<run_id>.json / .md` + `latest.json` + `running.json` (実行中だけ)。`ops.ingest_runs` にも 1 行
- 🚨 宿題 (0009): `mart.v_product_360.asin` は今 `max(ci.asin)` で全出品から拾うので、複数個パックの ASIN が勝ち得る。単品出品 (構成 1 行・qty=1) に限定する view の差し替えを PR-C の前に入れる

## 毎晩そっくり合わせ直す (夜間の再ロード)

初期ロードは 1 回流しただけ。放っておくと Company DB は「その日の写し」のまま古びる。ロードは**冪等** (同じ材料なら何も変わらない) なので、毎晩そのまま流せばいい。

- **毎晩 02:00 JST**: Render の中の cron (`apps/company-db/nightly.mjs`) が本適用のロードを 1 回流す。夜間の取り込み (Step 0 は 23:30 JST) の後、**Render 外バックアップ (03:30 JST) の前**。こうすると、その晩の控えに新しいロードの結果が入る
- 台帳 = `config/jobs-registry.mjs` の `company-db-nightly-load`。成功も失敗も jobs-monitor に ping する (dead-man 方式なので、**動かなくなったら「締切超過」で催促が出る**)
- 手で叩いたロードが走っていたら、その晩は**見送る** (二重に流さない)。見送りでは ping しないので、続けて見送られ続ければ催促が出る
- 30 分で終わらなければ打ち切って失敗にする (次の晩に持ち越さない)

```
# 有効にする (中原さん): Render → bfaith-portal → Environment
COMPANY_DB_LOAD_CRON_ENABLED=1        # これだけ。COMPANY_DB_URL は初期ロードで既に入っている

# 手で流す (Render Shell)
node apps/company-db/nightly.mjs run

# 結果を見る (miniPC から)
node scripts/company-db/remote-load.mjs status --counts
node scripts/company-db/remote-load.mjs reports
node scripts/company-db/remote-load.mjs report <run_id> --out C:/tmp/r.json
```

**うまくいっている晩は「変化なし」**。ping の note に `run=... / 変化なし` と出る。何か入った晩は `変化 products+2 skus+5` のように、**変わった区分だけ**が並ぶ。不一致 (conflicts) と未解決 (unresolved) の件数も出るので、増えていたら report を見る。

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
