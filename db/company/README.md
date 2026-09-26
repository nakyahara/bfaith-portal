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
🚨 **Amazon の出品は 3 経路**: ① マスタ登録 (mirror_sku_master + resolved = FBA の対応表) ② fba.db の Sheet / attrs ③ **自社発送 (FBM) の seller SKU = NE の商品コードそのもの** (mirror_amazon_sku_fees の `fulfillment_channel = 'FBM'` かつ NE の台帳にあるコードだけ。expected-profit の `fbmNeCode` と同じ規則。FBA なのに NE コードと偶然同じ SKU は結ばない)。③ が無かったので自社ブランドの主力 (hakkap100 など 1,310 種 / 28 日で 12,875 個) が出品に無く、注文明細が `unresolved_code` のままだった (2026-09-23 に見張り W6 で発覚 → #1409)。**出品が増えたら 0024 `core.reresolve_order_lines` が既存の未解決の明細 (注文日が直近 35 日) を解き直し、当たった注文の `updated_at` を進める** (翌朝の売上日次の作り直しに乗る。ロードの段 `order_lines_reresolved` に候補 / 当たった数)。正規化で同じ鍵になる別の原文の seller SKU (FBM と FBA) は自動では結ばず `sources.amazon_fbm.samples_collided` に残す (人が見る)。全履歴を解き直すなら (翌朝の作り直しが数百日ぶんになるので、時間のあるとき) Render の default user で `select * from core.reresolve_order_lines(1, 'amazon', null);` → miniPC で `node apps\company-db\push\mall-orders.mjs --mall amazon --refresh-sales --all`。

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
- ✅ 0009 で `mart.v_product_360.asin` を単品出品 (構成 1 行・qty=1) に限定した。セット出品や複数個パックの ASIN を、中に入っている単品の ASIN にしない

## 毎晩そっくり合わせ直す (夜間の再ロード)

初期ロードは 1 回流しただけ。放っておくと Company DB は「その日の写し」のまま古びる。ロードは**冪等** (同じ材料なら何も変わらない) なので、毎晩そのまま流せばいい。

- **毎晩 02:00 JST**: Render の中の cron (`apps/company-db/nightly.mjs`) が本適用のロードを 1 回流す。夜間の取り込み (Step 0 は 23:30 JST) の後、03:30 JST より前。その晩の控え (下の「バックアップと復元」) に新しいロードの結果が入る
- 台帳 = `config/jobs-registry.mjs` の `company-db-nightly-load`。成功も失敗も jobs-monitor に ping する (dead-man 方式なので、**動かなくなったら「締切超過」で催促が出る**)
- 別のロードが走っていたら、その晩は**見送る** (二重に流さない)。短い見送りは ping しない。**2 時間より前から走ったままなら「前の回が終わっていない」として失敗を ping する**
- 🚨 **Render では `node apps/company-db/load/run-initial-load.mjs --apply` を直接動かさない**。別プロセスなので夜間の見張り (メモリ上) を共有しない。歯止めとして、`running.json` に**生きている pid** の記録があれば CLI は始めずに終わる (`--force` で押し切れる) が、手で流すときは HTTP の口 (下の `remote-load.mjs`) を使う
- 30 分待っても終わらなければ失敗として ping する。🚨 **待つのをやめるだけで、ロード本体は止まらない** (Postgres の 1 トランザクションを外から切る手段がない)。次の回の見送り判定と dead-man に任せる
- **Render の中でだけ動く** (`lib/is-render.js` の `isRender()`)。miniPC も同じ server.js を動かすので、この歯止めが無いと二重に流れる (2026-08-05 に他のジョブで実際に起きた)
- 材料 (`warehouse-mirror.db`) が DATA_DIR に無ければ始めない。🚨 こちらは **どこで動かすかの判定ではなく**、「Render の中なのに材料が消えている」= 異常の検知 (miniPC でも mirror の初期化が同じファイルを作るので、有無だけでは見分けられない)

```
# 有効にする (中原さん): Render → bfaith-portal → Environment
COMPANY_DB_LOAD_CRON_ENABLED=1        # これだけ。COMPANY_DB_URL は初期ロードで既に入っている

# 手で流す (miniPC から。🚨 Render Shell で nightly.mjs を直接動かす口は作っていない =
#            別プロセスだと単一飛行の見張りを迂回して二重に流れるため)
node scripts/company-db/remote-load.mjs load --apply --wait

# 結果を見る (miniPC から)
node scripts/company-db/remote-load.mjs status --counts
node scripts/company-db/remote-load.mjs reports
node scripts/company-db/remote-load.mjs report <run_id> --out C:/tmp/r.json
```

**うまくいっている晩は「変化なし」**。ping の note に `run=... / 変化なし` と出る。何か入った晩は `変化 products+2 skus+5` のように、**変わった区分だけ**が並ぶ。不一致 (conflicts) と未解決 (unresolved) の件数も出るので、増えていたら report を見る。
(2026-09-24 まで skus・suppliers は値が同じでも毎晩全行を UPDATE していたので、`skus+7000` 台が毎晩出ていた。今は値が変わった行だけ UPDATE し、updated_at も変わった行だけ進む)

### 列ごとの持ち主 (`config/master-ownership.mjs`。Company DB構想 10 §5.2)

商品・仕入先マスタの正本を NE から Company DB へ移す (10。2026-09-24 中原さん決定) ために、夜間ロードが**どの列を直してよいか**を 1 か所で決める。

- `'load'` = 夜間ロードが SQLite の値に合わせる (今までの動き)。`'company'` = Company DB が正。**既にある行は上書きしない** (空欄を埋めることもしない)。新しく見つかった行には最初の値だけ入れる
- 対象 = 商品の名前・売上分類・状態 / SKU の名前・区分・税率・税区分・取扱 / 原価 (行ごと) / セット構成 / Amazon SKU ↔ NE コード / 仕入先の名前・発注方法・リードタイム
- 🚨 **切替日 (10 §8) までは全部 `'load'`**。切替日に対象の列をまとめて `'company'` にする。`'load'` に戻せば次のロードで SQLite の値に合わせ直す (切替の取り消し。ただし切替後に人が入れた値は消えるので、戻す前に 10 §8.4 の手順で退避する)
- 知らない列・知らない値・書き漏れがあると `OWNERSHIP_INVALID` でロードを始めない (typo で「守ったつもり」を作らない)
- report の先頭に「Company DB が正の列」が出る。区分ごと見送ったとき (原価・構成) は、その区分のメモに「Company DB が正: N 件は見送り」と出る
- 仕入先ごとの先方品番・入数・ロット・発注条件 (supplier_skus) はここに無い = 発注アプリ (purchase-orders) が正 (10 D-44) なので、夜間ロードは発注アプリの値に合わせ続ける
- 試験 = `node apps/company-db/test-master-ownership.mjs`

### 仕入先コードは 1 つの形 (0025。10 §9 D)

- 数字だけの仕入先コードは **4 桁の 0 埋め (NE の形)** に揃える (`'1'` → `'0001'`。4 桁より長い数字は先頭の 0 を外すだけ・数字以外はそのまま)。JS = `sources.mjs canonicalSupplierCode`、SQL = `core.canonical_supplier_code()`。同じ規則
- 発注アプリ (purchase-orders) は先頭の 0 を外して持つ (`normSupplierCode`)。揃えないと同じ仕入先が 2 行になる (2026-09-24 本番: 83 行 = 実 43 社)。0025 で二重をまとめた
- 🚨 **仕入先をコードで探す処理を新しく書くときは、両側を `core.canonical_supplier_code()` で揃えてから比べる** (例: 発注の取り込み = 0014 の `supplier_id` の解決。まだ作っていない)
- 試験 = `node apps/company-db/test-supplier-canonical.mjs`

### マスタの変更の記録と版番号 (0026。10 §5.2)

- **`events.master_change_events`** (append-only): 商品・SKU・仕入先・仕入先ごとの商品・セット構成・原価・出品・出品の構成の行が変わるたびに、**トリガーが同じ取引の中で**書く (本体が巻き戻れば記録も残らない)
  - UPDATE = 変わった列ごとに 1 行 (`attribute` / `old_value` / `new_value`。同じ行の変更は `change_id` で束ねる)。INSERT / DELETE = 行全体を 1 行
  - 「行が無い」= SQL の null、「値が空」= json の null。主キーは `entity_key` (複合キーも)、1 列の主キーの表だけ `entity_id`
  - 管理用の列 (updated_at・version・created_*・resolved_by_*・*_norm・first/last_seen_at) は比べない。**それ以外は全部** (列を足しても記録し忘れない)
- **誰が**: 取引の最初に `set_config('core.actor_type' | 'core.actor_id' | 'core.source_system' | 'core.run_id' | 'core.request_id' | 'core.reason', 値, true)`。取引を出れば消える (接続の使い回しで漏れない)。入れなければ `system` / `sql`。`db_user` は必ず残る
  - 夜間ロード = `source_system = 'company_db_load'`・`run_id` = ロードの run_id・`actor_id` = host (render-nightly など)
  - ポータル (PR ⑤) = `human`・ログインしたユーザー・`portal`・保存 1 回ごとの `request_id`
- **`version`** (products / skus / suppliers / supplier_skus / listings): 比べる列が実際に変わったときだけ **共通の通し番号 (`core.master_version_seq`) の次の値**になる (入力の version は信じない・INSERT も同じ)。消して同じキーで入れ直しても前の値に戻らない。セット構成・原価が変わると SKU の、出品の構成が変わると出品の version も変わる (子の値が同じ UPDATE では変わらない)。ポータルは `update … where 主キー = $1 and version = $2` で保存し、0 件なら 409 (後勝ちにしない)。**大小や +1 を前提にしない** (「読んだ値と同じか」だけ)
- 夜間ロードは値が同じ行を UPDATE しない (skus・suppliers・supplier_skus・sku_components・listings・listing_components)。ふだんの晩は変わった分だけ記録が増える
- 🚨 保持: 当面は全件を DB に残す。**1,000 万行 または 2 GB を超えたら**退避先・期間・復元方法を決める (消すときは trigger を disable する保守経路)
- `events.sku_attribute_events` (0005) は使わない (書き手なし。非推奨のコメントを付けた)
- 試験 = `node apps/company-db/test-master-audit.mjs`

### 足した列 (0027。10 §3 / ②c-2)

| 表 | 列 | 切替日までの出どころ (夜間ロード) | 持ち主のキー |
|---|---|---|---|
| core.skus | `standard_price_jpy` (標準売価) | mirror_products.標準売価 (円に丸める・負は null) | `skus.standard_price` |
| core.skus | `shipping_code` / `shipping_method` / `shipping_cost_jpy` (自社の計算用の送料) | mirror_products.送料コード / 配送方法 / 送料 | `skus.shipping` |
| core.skus | `reorder_months` (推奨保有月数・0〜60・小数 1 桁) | 商品管理リストの公開 snapshot (`mirror_pml_published` の status が ok/partial で行数が合う日だけ。行が無い商品は触らない・空欄は null) | `skus.reorder_months` |
| core.products | `inbound_date_managed` (ロジザード新商品の入荷日管理の初期値) | 入れない (ポータルの新商品登録で人が選ぶ。以後の正はロジザード) | — |
| core.suppliers | `email_to` / `email_cc` / `contact_name` / `fax_number` / `relay_to` / `order_memo` | 発注アプリの po_suppliers (空にしたら空に = coalesce で戻さない) | `suppliers.contacts` |
| core.supplier_skus | `is_primary` (代表の仕入先・SKU ごとに最大 1 つ) | NE の商品の仕入先コード。変われば「旧い代表を外す → 新しい代表を付ける」。コードが空の商品は触らない (保留) | `supplier_skus.is_primary` |
| core.listing_components | `sort_order` (列は 0002 からある。0027 からロードが入れる) | mirror_sku_resolved.sort_order (Amazon) | `listing_components.amazon` |

- 金額は円の bigint・0 以上・**null = 未取得** (0 円と区別)。ふりがな は持たない (実データで 100% 商品名と同じ)。季節・新商品の印は後で (書き手が無い)
- `core.merge_duplicate_suppliers()` は 0027 で連絡先・代表の印も寄せるように追従した
- 試験 = `node apps/company-db/test-master-columns.mjs`

### 夜間ロードが読んだ材料の世代 (0028。10 §6 / ③a-1)

毎朝の照合 (③a-2) は ①ロードの検証 (Company DB ↔ 実際に読んだ材料) と ②外との照合 (Company DB ↔ 今朝の NE・ロジザード) に分ける。①のために「どの材料を読んだか」を残す。

```
miniPC daily-sync
  NE 取込 (ne-api.js)          最初のページを書く前に前回の印を消し、最後のページまで取れたら sync_meta.ne_api_products_complete_at (= この回の行の synced_at) と件数
                               (セット商品は入れ替えと同じ取引で ne_api_setproducts_complete_at)
  sync-to-render.js            products / set_components を Render の mirror が持つ形にそろえた中身のハッシュ + 世代 ID (apps/warehouse/material-lineage.js)
                               → 控え DATA_DIR/cdb-material/<世代>.json.gz (新しい 14 世代・上書きしない) → 世代を /api/sync に同梱
Render /apps/mirror/api/sync   mirror を入れ替えたのと同じ取引で、入れた中身からハッシュを出し直し、合えば mirror_material_generations に記録。
                               記録できない (古い送り手・形が変・中身が合わない) ときは前の世代の記録を消す。入れ替えはどの場合も続ける
Render 夜間ロード (02:00)       自分が読んだ mirror の中身のハッシュを出し、世代と照らして ops.load_materials に残す
```

- ops.load_materials = 1 回のロード × 材料 (products / set_components)。content_hash・row_count = 実際に読んだ中身。status:
  - `matched` = 世代と同じ中身 → generation_id (= miniPC の控え)・元の NE 取得の完了時刻が付く。**照合 (③a-2) が使ってよいのはこれだけ**
  - `mismatch` = mirror が受信のあと Render 側で書き換えられた (会計アプリ 5 つの税率・売上分類の登録、fba-profitability の原価の例外)。report.notes にも出す
  - `no_generation` = 世代の記録が無い
- どこで失敗しても写しの送信・夜間ロードは止めない (控えや世代が無い日は照合が「判定できない」になるだけ)。0028 が未適用の DB でも夜間ロードは失敗しない
- 列とその空の埋め方は `MATERIAL_COLUMNS` (material-lineage.js) と /api/sync の INSERT で同じにする (試験が mirror の表の列と突き合わせる)
- 試験 = `node scripts/test-material-lineage.mjs`

### 材料の由来と規則の指紋 (0029。10 §6.1.1 / ③a-2 の A)

毎朝の照合 (③a-2) が「写しの遅れ・作り方の違い・本当の差」を取り違えないための前提 (Codex ③a-2 R0・R1)。

- **作り直しの記録** (miniPC の warehouse.db `m_products_builds`・apps/warehouse/master-material.js): rebuild-m-products.js が m_products / m_set_components を入れ替える**同じ取引**で 1 行 = 読んだ NE の完了印 (作り始めと入れ替えの時で違えば null + `changed_during_build`・無ければ `absent`)・送る形 (m_products + raw の代表商品コード) のハッシュ・SKU ごとの採用理由 (例外原価・税率の補い・セット名の空欄・今回の NE の取得に無い古い行)。staging が作った後に変わっていれば入れ替えない (`STAGING_CHANGED`)。60 日残す
- **世代の由来**: sync-to-render は products・set_components・最新の作り直しの記録を 1 つの読み取り取引で読み、中身が同じときだけ世代に build (build_id と作り直しが読んだ NE の印) を付ける。違えば build_id = null (`changed_after_build` = 作り直しの後に /register などで直された)。過去の記録で代用しない
- **Render 到達の証跡** (`DATA_DIR/company-db-evidence/<日付>/render-master.json`): /api/sync の応答の `material_recorded` から entity ごとに recorded / mismatch / not_recorded / not_replaced / unconfirmed (古い受け手)
- **0029** = ops.load_materials に `rule_fingerprint` (夜間ロードの変換コード 5 ファイル = engine.mjs の `LOAD_RULE_FILES` を LF にそろえて sha256。起動時に計算)・`ownership` (その回の持ち主の設定そのもの)・`load_conditions` (適用済み migration の版・0027 の有無)。照合の ① は同じ指紋のコード・その回の持ち主でしか判定しない。0029 が未適用でも夜間ロードは失敗しない
- **NE の元の値と取込の整合 (C1)**: raw_ne_products の 原価_src・売価_src・消費税率_src / raw_ne_set_products の セット販売価格_src・数量_src = 取込の元の値を JSON の文字列で (`db.js neSrc`。'""' = 空文字・'"0"' = 文字列のゼロ・'0' = 数値・'null' = API が null・SQL の NULL = 元の値の記録が無い = 足す前の行・その回に取れなかった行)。NE の API・CSV・自動取込の 3 つとも同じ INSERT で。数値の列は今までどおり (`parseFloat(x) || 0`)。完了の印と一緒に取込の整合 (`ne_api_products_integrity` = 取った行・コードが空・同じコードが 2 度 / `ne_api_setproducts_integrity` = 保存の前の 親の名前・売価の食い違い・親 × 子の重複・キーの欠落) とセットの親の数 (`ne_api_setproducts_complete_parents`)。作り直しの記録は信用した印の番号も組で (`ne_products_complete_rev` / `ne_setproducts_complete_rev`)。試験 = `node scripts/test-ne-src.mjs`
- 控え (DATA_DIR/cdb-material) は**世代の時刻から 35 日**残す (個数ではない。retry で世代が増えても照合に要る控えが消えない)
- 試験 = `node scripts/test-master-build-lineage.mjs` (作り直しの記録・由来・到達の証跡) / `node scripts/test-material-lineage.mjs` (0029・35 日)

### マスタの照合 ①ロードの検証 (0030・W13。10 §6.1.1 B)

毎朝 daily-sync の「マスタ照合」(`apps/company-db/master-compare/run.mjs --daily`・見張りの前) が、最新の夜間ロード (Render・02:00) を検証する。

- **材料**: その回の ops.load_materials (products・set_components とも matched) の世代の控え (miniPC の DATA_DIR/cdb-material) を、mirror と同じ型の一時の SQLite (`apps/warehouse-mirror/material-tables.js`) に戻し、ロードの読んだ中身と同じハッシュになるのを確かめてから `buildPlanFromRender` (now = ロードの時刻)
- **期待値**: ロードと同じ規則 (`engine.mjs` の `skuValuesForLoad`・`SKU_OWNED_COLUMNS`・`costForLoad`) を、**ロードした回の持ち主・条件** (ops.load_materials の ownership・load_conditions.has0027) と **ロードの判断** (0030 の ops.load_decisions) で当てる
  - 0030 = 夜間ロードが取引の中で書く (section = skus / sku_costs / set_components / primary_suppliers・理由コード・60 日)。構成は書こうとした行と manual で数量が同じだった行・削除まで行った親、代表の仕入先は確かめた後の対象全体
- **比べるもの**: SKU が無い / 値 (load の列) / 有効な原価 (金額・source・status) / 代表の仕入先 (1 件だけ・期待の仕入先) / セット構成 (書こうとした行は片方向・削除まで行った親は source が manual 以外の余分も)。差の明細に events.master_change_events の**変更の候補** (ロードの始まり以降) を付ける
- **判定できない (blocked)**: 夜間ロード (host = render-nightly・success・complete) が無い・今日 (JST) でない / 0029・0030 が無い / 材料が matched でない / 規則の指紋がこのコードと違う (LOAD_RULE_FILES に config/master-ownership.mjs も入る = 持ち主の設定を変えた日も) / 判断の記録が無い・形が違う / 控えが無い・壊れている・戻した中身がロードの読んだ中身と違う
- **出すもの**: 全件 JSON = DATA_DIR/cdb-master-compare/<日付>/<compare_run_id>.json (不変・35 日。比べた SKU の一覧と対象外の理由も) / 証跡 master-compare (始めに running で前の結果を無効に → complete で JSON の場所・sha256・件数・判定。書けなければ exit 1)
- **見張りの W13** (config/watch-checks.mjs。評価キー 1 = load・severity info・depends なし・案件 = SKU × 問題の種類 = `<種類>:<code_norm>`): 証跡 (今朝の実行 ID・complete・今日) と全件 JSON (sha256・ID・判定・件数・ロード) を確かめて全案件を渡す。開いている案件が今回の明細に無いとき、その種類 × SKU を比べていれば回復・比べていなければ「監視期間外」(回復にしない)
- **retry**: 「マスタ照合」は RETRYABLE。Render同期 がこの回の retry で成功したら マスタ照合 → 見張り も走らせ直す (`retry-failed-jobs.js` の RERUN_AFTER)
- 手で流す (miniPC): `node -r dotenv/config apps/company-db/master-compare/run.mjs --json` (DAILY_SYNC_RUN_ID が無い = 証跡は master-compare.manual.json = 見張りは読まない)
- 試験 = `node scripts/test-master-compare.mjs` / `node scripts/test-watch-w13.mjs` / `node scripts/test-retry-rerun.mjs`

### マスタの照合 ②外との照合 (C2a。10 §6.1.1 C2 v3〜v6)

同じ「マスタ照合」の実行口が ① の後に、同じ読み取りの取引で ② を流す (`apps/company-db/master-compare/compare-ne.mjs`)。Company DB ↔ NE の「最後まで取れた回」の集合。**次の夜間ロードの結果は予測しない** = 4 つの値 (n = NE・c = Company DB・t_today = 今朝の材料 G_today・t_load = 昨夜のロードの材料 G_load) を比べて差を事実で分ける。

- **前提** (欠ければ ② は blocked。NE の集合が読めていれば値の差の一覧 raw_diffs だけ出す): 鮮度 = NE の印 (UTC の文字列 → JST の日付) と今朝の作り直し (m_products_builds・daily_sync_run_id) が今日 / 材料の信用 = 今の通し番号 = 印の番号・行数・セットの親の数・作り直しが信用した印 = その印・Render 到達の証跡 (render-master) の世代の作り直し = 今朝の作り直し・控えがハッシュどおり / 取込の整合 (ne_api_*_integrity) が読める。到達 (recorded = confirmed / unconfirmed = unknown / それ以外 = not_delivered) は信用とは別
- **値の状態**: 元の値 (*_src) から raw (value / empty / zero / null / unknown) と validity を決め、comparable / no_value (NE に値が無い) / incomparable (不明・不正) に分ける。取扱区分は知っている語 (取扱中・取扱中止・ﾒｰｶｰ取扱中止) 以外は不正。変換は sources.mjs の関数を共用 (`trimOrNull`・`yenOrNull`・mapTaxRate・mapHandling・canonicalSupplierCode)
- **分類** (列・構成の子の行ごと): blocked / incomparable / ne_no_value / match / (A 昨夜の適用 = c と t_load) load_mismatch・held_by_load・rule (manual)・direction_unknown・unexplained / rule (作り直しの理由・ロードの規則) / lag・rule_lag (今朝の値がまだロードに渡っていない・到達 confirmed・期限内) / not_delivered_by_load (期限のロードは済んだのに材料に目標値が無い) / arrival_unknown・not_delivered / spec_undecided (CDB にだけある)
- **0030 の保持状態** (C2 v6-1): 夜間ロードは飛ばした構成の行・原価を飛ばした SKU・代表の仕入先を付けなかった SKU について、ロードが終わった時点の状態を判断の記録に残す。② は今の c がそれと一致したときだけ held_by_load / rule (manual) にする (一致しない・記録が無い = unexplained)
- **反映待ちの期限の台帳** (`pending.mjs`): DATA_DIR/cdb-master-compare/pending/ に版 (pending_<compare_run_id>.json・前の版のハッシュつき) と HEAD。単位 = 案件 × 列 × 目標値のハッシュ。始まり = 目標値が最初に到達 confirmed になった世代。期限 = その後の最初の夜間ロード。再送・作り直しで延ばさない。**HEAD の版が無い・ハッシュ違い・HEAD が無いのに版がある = untrusted** = 反映待ちの判定は blocked・HEAD を進めない。**直し方は下の restore-pending.mjs に一本化** (pending/ を片付けて初めからやり直すと、それまでの反映待ちの期限が全部作り直しになるのでしない)。更新は pending/.lock で排他。**古い .lock も自動では消さない** (回収どうしの競合で排他が破れるため)。要約に「⚠️ ②: 反映待ちの台帳が使えない (locked: … 分前)」が続いたら、daily-sync・retry が走っていないことを確かめてから miniPC で DATA_DIR/cdb-master-compare/pending/.lock を手で消す。保存に失敗した朝は「⚠️ ②: 反映待ちの台帳が使えない (write_failed)」+ pending/WRITE_FAILED.json が残り、次の回からも untrusted (期限を後ろへずらさないため)。書いている途中で落ちた回は WRITE_INTENT.json が残り、同じく untrusted (失敗の印を書けなかった場合もこれで止まる)。**印を手で消さない** (期限が後ろへずれる)。原因 (ディスク・権限) を直してから `node apps/company-db/master-compare/restore-pending.mjs --from <失敗した回の全件 JSON>` = その回が書こうとした台帳の中身 (ne.pending_entries) から作り直して印を消す。失敗した回の全件 JSON も無い (ディスクがいっぱい等) ときは、最後に正常に走った回の JSON を指定する (その後に始まった反映待ちだけ数え直し = 残る限界。要約の ⚠️ で人が気付く)。**もう 1 つの残る限界**: ディスクが丸ごと書けない朝は、書きかけの印も失敗の印も残せない → 次の回は前の HEAD を正常として読み、その朝に初めて出た反映待ちだけ期限を作り直す (最大 1 日ずれる)。その朝の要約は GChat に「⚠️ ②: 反映待ちの台帳が使えない (write_failed)」で届く (ネット経由 = ディスクと無関係) ので、見たら原因を直してから、最後に正常に走った回の全件 JSON で restore-pending.mjs を流す
- **出すもの**: 全件 JSON の形 = **mc-v2** (一番上は今までどおり ①・`ne` の節が ②: items (案件 = `<種類>:<code_norm>`・列ごとの分類)・held・recoverable・out_of_scope (4 つは重ならない)・decisions (判断の一覧・承認の指紋 = 意味の版つき。作り直しの ID・時刻・ファイルの指紋は入れない)・counts)。証跡 master-compare に `ne` (verdict・件数)。朝の要約 = 「① / ②」(② が落ちた・判定できない朝は ② を先頭に ⚠️)。② が落ちても ① の結果・証跡は残る (ne.verdict = error)
- 見張り (W13:ne・案件ごとの保持と明示の回復) は C2b。W13:load は mc-v1 / mc-v2 の両方を読む
- 試験 = `node scripts/test-master-compare-ne.mjs`
## 在庫を毎時写す (ロジザード → raw → 日次。08 §3。D2)

在庫の 3 段 (raw の毎時写し → 日次 2 表 → いまの在庫の view) は **Render の中の毎時 cron** (`apps/company-db/inventory-hourly.mjs`) が作る。本体は `apps/company-db/inventory/logizard.mjs` (Postgres と行の配列だけを見る = PGlite で試験できる)。

- **毎時 :35**: `mirror_logizard_stock` (miniPC が毎時 09〜18 時に送る全置換) を読み、前の世代 (`captured_at`) と比べて**変わった行だけ**を `raw.logizard_inventory_observations` に書く (新規・変化 = `ok`、消えた = `not_found`、ロケ移動 = 旧鍵 not_found + 新鍵 ok)。同じ世代なら `skipped` の run だけ残す (観測は書かない)。世代は `ops.ingest_runs.checksum` に ISO で残す
- **比較元は直前までの完走した run の状態観測だけ** (失敗した run・error / skipped は根拠にしない = view と整理と同じ根拠)。世代の判定は advisory lock を取ってから (待っている間に完走した世代を踏み越えない)
- **日付 (JST) が変わった最初の回** (00:35 JST) で前日までの未締めの日を締める: その日の最後に完走した取得の状態から `snapshots.warehouse_stock_daily` (sku × ロケ) と `sku_stock_daily` (sku。品質区分は分けずに合算) を作り、`stock_capture_days` を building → complete に上げる (1 日 = 1 トランザクション)。取得が 1 回も無い日は `missing`。有効期限・入荷日は実在する日付だけ date にし、読めない値 (13 月・2/30・文字) は null にして件数を数える (1 行の不正で日の締めを止めない)。**締めが追いついている回だけ** raw の整理 (`raw.purge_superseded_observations`、30 日 = D-25) と DB の大きさを `ops.job_runs` に残す (未締めの日が残る間は、その復元材料 = 古い観測を消さない)
- **締めた日どうしの差 → 在庫の「増えた / 減った」** (0022。`apps/company-db/inventory/stock-diff.mjs`。08 §3.2 の 3 段目): 前日 → 当日 (どちらも complete) の差を SKU 単位で `events.inventory_events` に `confidence = 'inferred'` で追記する
  (`source_system = 'logizard_diff'`・`qty_delta`・`qty_after`・`occurred_at` = 当日の世代・理由は分からないので `reason_code` は null・ロケは入れない = 棚移動は SKU 単位では 0)。
  - 作り終えた日は `snapshots.stock_diff_days` に印が残る (イベントの追記と同じ取引)。**倉庫が動かなかった日は 0 件でも `done`** = 「イベントがある = 済んだ」と読まない。毎時の回が「印の無い complete の日」を古い順に拾うので、止まっていた日は次に動いた回が追いつく
  - 間に取れなかった日がある区間は作らない: 前日が missing → `skipped (prev_not_complete)`・最初の日 → `skipped (first_day)`
  - 商品コードで突き合わせてから SKU に寄せる (間に SKU が登録されたコードを「+全量」と読まない・表記だけ変わったコードは同じ SKU の差 1 件)。別の SKU に付け替わったコードは 前の SKU の −全量 と 新しい SKU の +全量。両日とも SKU が分からないコードはイベントにできない → 数だけ印に残す (`unresolved_changed`)
  - 式の版 = `lzdiff:v1` (印の主キーとイベントの `idempotency_key` の頭の両方)。式を変えるときは版を上げる。**0022 が未適用の間は何もしない** (ログに 1 行。毎時ジョブは落とさない)
  - 🚨 inferred のイベントは日次の表から作り直せる **派生データ**。締めをやり直すときは、その区間のイベントも下の手順で消す。消し忘れても黙って done にはならない
    (追記の後に「その区間のイベントの集合 = いまの日次から作った差」を照合 → 合わなければ `STOCK_DIFF_MISMATCH` で毎時の回が fail を ping・印は付かない)。差が失敗した回も、取込・締め・整理は済ませる
- 🚨 **rows が空・鍵が重複・数量が非負の int32 でない・別会社のロケ** は run を `failed` にして何も書かない (黙って合算・全消ししない。締めで落ちる行を success にしない)。別の取込が走っていてロックが取れない回は `skipped` (locked) にして、**その回は締めも整理も見送る** (まだ見ていない世代を待たずに日を確定しない)。`core.locations` は変わった行の ブロック × ロケ を `core.ensure_location` で足す (R* = いろは棟)
- ping: 取り込んだ回・日を締めた回・在庫の差を作った回だけ `ok`。世代が同じで締める日も無い回 (夜間) は打たない。失敗は `fail`。台帳 = `company-db-inventory-hourly` (09:35 JST + 猶予 3 時間)
- **Render の中でだけ動く** (`isRender()`)。材料 (`warehouse-mirror.db` / `mirror_logizard_stock`) が無ければ失敗として ping する

```
# 有効にする (中原さん): Render → bfaith-portal → Environment
COMPANY_DB_INVENTORY_CRON_ENABLED=1   # これだけ。次の :35 に初回 (最初の取込は全行 = 8,000 行前後)

# 結果を見る (Postgres)
select ingest_run_id, status, complete, checksum as generation, rows_seen, rows_inserted, error from ops.ingest_runs
 where source_system = 'logizard' and entity = 'inventory' order by started_at desc limit 20;
select * from snapshots.stock_capture_days where source = 'logizard' order by snapshot_date desc limit 14;
select * from mart.v_sku_stock where warehouse_qty is not null limit 10;
select * from snapshots.stock_diff_days order by to_date desc limit 14;                       -- 在庫の差を作った日の印 (done の events = 変わった SKU の数)
select e.occurred_at, k.code, e.qty_delta, e.qty_after from events.inventory_events e join core.skus k using (sku_id)
 where e.source_system = 'logizard_diff' order by e.occurred_at desc, abs(e.qty_delta) desc limit 20;

# 締めをやり直す (保守経路。その日の capture 行と日次行を消して、次の :35 を待つ)
begin; set local snapshots.maintenance = 'on';
-- 在庫の差 (0022): その日 D に掛かる 2 つの区間 (D-1..D と D..D+1) の印と inferred のイベントを消す。印は取得記録を FK で指すので先に消す。
--   🚨 印は to_date で消す (from_date ではない): 前日が missing だった翌日の印は skipped で from_date が null = from_date では引けず、締め直して complete になっても差が永久に作られない
--   イベントは追記専用 (trigger) → この取引の中だけ外す。消すのは source_system = 'logizard_diff' の 2 区間だけ (exact のイベントには触らない)
delete from snapshots.stock_diff_days where source = 'logizard' and scope_key = 'main' and to_date in (date '2026-09-14', date '2026-09-15');
alter table events.inventory_events disable trigger trg_append_only_row;
delete from events.inventory_events where source_system = 'logizard_diff' and source_ref in ('main:2026-09-13..2026-09-14', 'main:2026-09-14..2026-09-15');
alter table events.inventory_events enable trigger trg_append_only_row;
delete from snapshots.sku_stock_daily where snapshot_date = date '2026-09-14' and source = 'logizard';
delete from snapshots.warehouse_stock_daily where snapshot_date = date '2026-09-14';
delete from snapshots.stock_capture_days where snapshot_date = date '2026-09-14' and source = 'logizard';
commit;
```

試験 = `node scripts/test-company-db-inventory.mjs` (PGlite。鍵と中身 / 取込の差分 / 失敗した run は根拠にしない / 締め / 整理 / mirror の読み取り / ping の出しかた)。🚨 2 接続の並行 (advisory lock・表ロック) は PGlite では書けない。在庫の差 = `node scripts/test-company-db-stock-diff.mjs` (取込 → 締め → 差を本物で通す)。まだ足していない: 13 か月を過ぎた日次 → 週次、90 日 / 13 か月の日次の整理 (08 §3.3 の残り。保持期限はまだ先)

## 在庫の日次を送る (NE → snapshots.sku_stock_daily。08 §3.3 ③。D2b-1)

ロジザードの在庫は Render が mirror から自分で写す (上の章)。**NE の在庫の日次**は miniPC の `warehouse.db` の `ne_stock_daily_snapshot` (朝の NE 取得の直後に 1 日 1 回複製・1 日 約 5,000 行・2026-05-02〜) にしか無いので、miniPC から送る。表は 0011 のまま (新しい migration は無い)。

- **送り手 (miniPC)**: `apps/company-db/push/stock-daily.mjs --source ne --days 14`。daily-sync の「NE在庫スナップショット」の直後に走る (スナップショットが失敗した朝は送らない)。**台帳を持たない** = どの日を送り済みかは Render に聞く (`GET …/sync/stock-daily/status`)。mirror (Render 同期) は経由しない
- **受け口 (Render)**: `POST /apps/company-db/sync/stock-daily` (`apps/company-db/ingest/stock-daily.mjs`)。**1 日 = 1 要求 = 1 取引** で `stock_capture_days` を building → 行 → complete (途中で落ちれば何も残らない)。SKU は `core.skus.code_norm` で解決し、分からない行も入れて数える
- 🚨 **先に確定した日は書き換えない**: 同じ内容の再送 = `same` / 違う内容 = 409。送り手は確定済みの日を送らず、内容の指紋 (`ops.ingest_runs.checksum`) だけ比べて、違えば最後の行に ⚠️ で出す (朝の取得の直後の値を、同じ日に取り直した値で上書きしない)
- 🚨 **取れなかった日は missing** (過去の日だけ・送り手の申告)。0 件を「在庫なし」と読ませない。missing の日に後から元データが入れば complete に上がる。今日の元データが無いのは失敗 (❌ = 自動再試行の対象)
- 読む口 = `mart.v_sku_stock` の `ne_qty` / `ne_as_of` (最新の complete の日)

### FBA の在庫の日次 (fba_jp / fba_us。08 §3.3 ④。D2b-2)

同じ送り手・同じ受け口。daily-sync では「FBA在庫スナップショット」の直後に `--source fba_jp` と `--source fba_us` が走る (JP 1 日 約 4,000 出品 SKU)。

- 🚨 **元は `daily_snapshots` ではなく、朝のスナップショットが作る「送る版」** (`fba.db` の `cdb_stock_export` / `cdb_stock_export_days`。`db.js` の `saveStockExport`)。
  `daily_snapshots` は RESTOCK と PLANNING を混ぜた表で、① RESTOCK に無い SKU の FC 移管中・処理中・出荷待ちが **0 で入る** (「取れなかった」と「0」が区別できない。本番の過去 138 日のうち 95 日は全 SKU で 0)
  ② 同じ日に取り直すと値が変わる ③ 行がいつの取得か残らない。送る版は **その回に取得したレポートの行そのもの** から 1 日 1 market = 1 版を 1 取引で作り、値と取得時刻が必ず同じ回のものになる。
  **最初に作った版は変えない** (例外は「RESTOCK の無い版 → ある版」だけ)。30 日で消す (基準は入力の日付ではなく、いまの JST の日付。未来の日付の版は作らない。長期の履歴は Company DB)
- 🚨 **版を作らない回** (その日は「版の無い日」= partial + 定刻で送られる。朝のスナップショットの最後の行に ⚠️ と理由が出る。JP も US も):
  ① **PLANNING が取れなかった回** (`no_planning`)。出品 SKU の全体は PLANNING にしか無い → RESTOCK だけの版は、載っていない SKU を Company DB で在庫 0 に見せる
  ② 在庫の数が 0 以上の整数でない (レポートの `--` は正規化で NaN になる。**0 にしない**)
  ③ SKU が不正 (空・前後の空白・制御文字)・同じレポートの中で表記違いの同じ SKU がぶつかっている。
  RESTOCK と PLANNING で表記だけ違う同じ SKU (大文字小文字・全角の英数記号・ダッシュの仲間・空白) は **1 行にまとめる** (RESTOCK が正)。
  Company DB では同じ出品・同じ SKU に当たるので、2 行で入ると view が二重に数える。
- 🚨 **「同じ SKU か」を決めるのは DB (`core.norm_code`) で、JS ではない**。JS の鍵 (`normCodeKey` = `lib/sku-norm.js` の `normSku` = `core.norm_code` の JS 版。NFKC はしない) は「鍵が同じなら DB でも同じ」と言える範囲でだけ使う。
  受け口の本体は、2 行の重複 (400。NE も同じ。9/21 の本番に該当 0 件) と「前の版にあった SKU が無い」を **SQL の `core.norm_code` そのもの** で判定する。
  🚨 **JS の鍵が DB と同じ答えになると保証できるのは、正規化の後の鍵が ASCII のときだけ** (ASCII の外は DB の `lower()` が照合環境しだい・JS の `toLowerCase()` は İ を 2 文字にする = 両方向に食い違う)。
  版を作る側 (fba.db には DB が無い) は、鍵が ASCII でない SKU が 1 つでもあれば **その回の版を作らない** (まとめると在庫行を捨てる・別々にすると二重に数えるか、送れない版が固定される)。
  全角の英数記号・ダッシュの仲間・空白は正規化の後に ASCII になるので通る。本番の fba.db の SKU は 4,025 種類とも ASCII (9/21 に確認)。
  もし ASCII でない SKU が出品されたら、毎朝の最後の行に ⚠️ と SKU が出て、その日は partial で送られる (= view の `fba_jp_as_of` が進まなくなる) → そのときに扱いを決める
- **7 区分をそのまま持つ** (`fba_available` / `fba_fc_transfer` / `fba_fc_processing` / `fba_customer_order` / `fba_inbound_working` / `fba_inbound_shipped` / `fba_inbound_received`)。`qty` = FBA の倉庫の中の在庫 = available + FC 移管中 + 処理中 + 出荷待ち (月末の棚卸しと同じ定義。受け口が計算する)
- 🚨 **FC 移管中・処理中・出荷待ちは、行ごとに「3 つとも数字」か「3 つとも null」**。null = その SKU は RESTOCK に載っていなかった = 分からない (PLANNING にしか無い SKU)。その行の `qty` は available だけ
- 🚨 **partial (一部だけ取れた日)** = RESTOCK レポートが丸ごと取れなかった日 = 全部の行が null。日の状態は `partial` = **view は読まない** (`fba_jp_as_of` は最後の complete の日)。
  **partial → complete だけは後から上げられる** (同じ日にもう一度スナップショットを流して RESTOCK が取れたとき = 送る版も入れ替わる)。
  ただし **上げる版に、前の版にあった SKU が無ければ上げない** (受け口が `kept_partial` を返す。消えた SKU は「最新の complete の日に行が無い」= 在庫 0 に見えるため)。
  エラーにはしない (本当に出品が消えた日だと、範囲を抜けるまで毎朝 ❌ になるだけで直す手段が無い) = 送り手の最後の行に「⚠️ complete に上げなかった日」と出て、その日は partial のまま残る
- **版の無い過去の日** (この仕組みの前・30 日より前): **推定しない**。`daily_snapshots` の available と入庫の 3 つだけを読み、3 区分は null・partial・取得時刻はその日の朝の定刻 (07:30 JST) を入れて `captured_at_nominal` で送る (`ops.ingest_runs.format_version = 'v1-nominal-time'` に残る)
- **SKU の解決** = 出品 (`core.resolve_listing_id`) の構成が **1 SKU × 1 個** のときだけ `sku_id` を入れる。まとめ売り (1 SKU × N 個)・セット・Company DB に出品の無い SKU は `sku_id = null` (`source_code` に出品 SKU が残る)。
  本番の実測 (9/21): 4,000 出品 SKU のうち 出品に当たる 2,807 / 1 SKU × 1 個 2,492 / まとめ売り 288 / セット 25。= `mart.v_sku_stock.fba_jp_available` は「1 個売りの出品ぶん」だけの数 (SKU の単位への展開は別の view の仕事)
- 🚨 `fba.db` は sql.js (ファイル全体を書き戻す)。送り手は **読むあいだだけ db.js と同じ lock (`fba.db.lockdb`) を取る** = 常駐サーバが保存している最中のファイルを読まない。書き手は常駐サーバ 1 つのまま (送り手は読むだけ)
- US は今日の行が無くても失敗にしない (US の取得は失敗しても朝のスナップショットは成功扱いなので)。`fba_unfulfillable` は列が無いので送らない (いまは全部 0)

```powershell
cd C:\Users\bfaith\bfaith-portal
node apps\company-db\push\stock-daily.mjs --source fba_jp --all --dry-run --data-dir C:\Users\bfaith\bfaith-portal\data   # 件数だけ (送らない)。partial の日数も出る
node apps\company-db\push\stock-daily.mjs --source fba_jp --all --data-dir C:\Users\bfaith\bfaith-portal\data             # 初回: 2026-04-10 から今日まで (消えた日は「取れていない日」と申告される)
node apps\company-db\push\stock-daily.mjs --source fba_us --all --data-dir C:\Users\bfaith\bfaith-portal\data
```

```powershell
cd C:\Users\bfaith\bfaith-portal
node apps\company-db\push\stock-daily.mjs --source ne --all --dry-run --data-dir C:\Users\bfaith\bfaith-portal\data   # 件数だけ (送らない)
node apps\company-db\push\stock-daily.mjs --source ne --all --data-dir C:\Users\bfaith\bfaith-portal\data             # 初回: 元データの最初の日から今日まで (約 140 日 = 140 要求)
node apps\company-db\push\stock-daily.mjs --source ne --days 14                                          # ふだん (daily-sync と同じ)
node apps\company-db\push\stock-daily.mjs --source ne --from 2026-09-01 --to 2026-09-20                  # 期間を指定
```

確定した日をやり直す (保守経路。その日の行と capture 行を消して送り直す):

```sql
begin; set local snapshots.maintenance = 'on';
delete from snapshots.sku_stock_daily where snapshot_date = date '2026-09-14' and source = 'ne';
delete from snapshots.stock_capture_days where snapshot_date = date '2026-09-14' and source = 'ne';
commit;
```

試験 = `node scripts/test-company-db-stock-daily.mjs` (22 件。FBA = 7 区分と qty・SKU は 1 SKU × 1 個のときだけ・partial は null で受けて view が読まない・partial → complete・記録と推定の根拠・US・fba.db の lock。NE の 10 件: 1 取引・先に確定した日は書き換えない・missing・巻き戻し・検証 / 受け口を HTTP で / 送り手 = 台帳なし・missing の申告・内容が違う日は ⚠️・今日の元データが無ければ失敗・検証に通らない日は送らない・dry-run・--all)。🚨 2 接続の並行と本番の件数での所要時間は試験に無い。

## Amazon 財務の受け皿 (0012。08 §4.4 / §4.7。F2 の DDL 部分)

決済レポートの明細は Company DB に置かない (D-37)。miniPC が明細に「採用する取得元 (policy)」と訂正を当ててから、**注文 × 計上日 × SKU × 取得元** の集約を作って push する (§4.7 の契約。miniPC 側の取込 = F2b は後継レポート V2 のロール待ち)。

- `core.finance_source_policy` = 会社 × モール × scope × 期間 [from, to) → 採用する取得元 (`amazon_settlement_flat_v1` / `_v2` / `amazon_finances_api` / `mall_finance_daily_v1`)。**期間の重複は trigger が拒む** (advisory lock で直列化。版の境目 = 10/31 まで v1・11/1 から V2 のように隣接させる。境目を動かす順は「縮める → 伸ばす」)
- `core.finance_policy_gaps(会社, モール, scope, from, to)` = その窓の中で policy が無い区間。`core.assert_finance_policy_covered(...)` = 無ければ例外。**日次集計 (finance_daily) を作る前に必ず通す** (= 未設定の期間は集計を失敗させる)。`mart.v_order_finance_uncovered` = policy がどの source も指していない計上日の行 (0 件が正常。黙って落ちる行を見える所に出す)
- `core.order_finance_receipts` = 注文単位の受領状態 (最後に受け取った世代 `received_batch_seq`・集合の checksum・行数)。**明細集合が空になっても残る** = 遅れて届いた古い世代を拒む根拠
- `core.order_finance_daily` = 注文 × 計上日 × SKU × 取得元 = 1 行。JPY だけ。**符号は決済レポートのまま** (売上 +、手数料・返金・販促 −、補填は符号そのまま)。旧表と同じ数量 5 列 + 金額 19 列、**net = 19 列の合計 (CHECK)**。注文に紐付かない費用 (保管料・月額) は `mall_order_no = '-'`。🚨 旧表は**明細 1 行ごとに ABS を取ってから合算する**列がある (保管料 −100 + 訂正 +40 → 符号つき −60、旧表 140) ので、**旧互換の `legacy_*` 10 列 + `legacy_complete`** (明細単位の ABS 合計。返金は customer / A-to-z 別。miniPC が集約の前に計算し、入れた行は legacy_complete = true) も持つ。**`mart.finance_daily` の材料が旧互換の view である間は、V2 の行でも legacy_* を計算し続ける** (未提供の行は legacy_complete = false で 0 と区別。`core.assert_legacy_complete()` が例外)
- `core.apply_order_finance_batch(会社, モール, scope, 注文番号, 世代, 集合の checksum, 版, 行の jsonb 配列)` = §4.7 の契約で明細集合を丸ごと置き換える (1 取引の中で呼ぶ): 受領行を for update → 古い世代は `'stale'` (何も変えない) → 集合の checksum が同じなら世代だけ進めて `'same'` → 同じ世代で内容が違えば例外 → 削除 → 挿入 → 受領状態の更新で `'applied'`。listing_id は seller_sku から (会社 × モール で 1 件に当たるときだけ)。sku_id は F2b で
- `mart.v_order_finance_summary` = 注文の累計 (全内訳)。**policy が指す source の行だけ**を足す (旧と V2 の両方が入っていても二重にならない)
- `mart.v_finance_daily_legacy` = **旧表 `f_amazon_finance_sku_daily_v1` と同じ列・同じ式**で作った日次 (legacy_* から。commission = Σ gross − Σ refund で返還だけの日は負、closing_fee は定数 0、**返品数量 = 日 × SKU の返金額 ÷ その月の SKU の Order 単価 を丸める** (旧 build の unit_price_month と同じ。注文ごとに丸めない)、SKU 無しの行は入れない、`legacy_incomplete_rows` = 未提供の行数)。**移行期 (F3) はこれと f_* を列ごとに突き合わせる** (D-35 = 差 0 円)
- `mart.finance_daily` = 日次集計 (run_id publish)。旧表と同じ列名・同じ規約 (publish の step が v_finance_daily_legacy を写す。ABS の列は CHECK で負を拒む、commission は正味なので負も可)。原価・利益の列は持たない (D7)

試験 = `node scripts/test-company-db-finance.mjs` (PGlite 16 件。旧互換 legacy_* と v_finance_daily_legacy の混在例・旧 SQL の式を期待値にした返品数量・SKU 無しの除外・legacy_complete / policy の重複・境目・update / gaps と assert / apply の applied・stale・same・例外・空集合 / JPY・net の検算・複合 FK / 累計 view の policy 絞り込みと全内訳 / uncovered / finance_daily の列・符号・grain_key)。🚨 policy の重複検査と受領行の for update の 2 接続の並行は PGlite では書けない
## 受注・出荷の受け皿 (0013。08 §4.1〜4.3 / §4.7。D4)

受注の raw は Company DB に持ち込まない (年 236 万注文)。miniPC が warehouse.db の追記ログから core の形に整えて §4.7 の契約で push する (取込ジョブ = D5)。**注文 = モールの注文 1 件、出荷 = NE の伝票 1 件**。状態の履歴は持たない (D-36。出荷済み・取消の時刻を列で)。

- `core.order_status_map` = 状態の正規化はデータで (NE 1 / 2 / 20 / 40 / 50。未登録は `unknown`)。`core.map_order_status(source_system, value)`
- `core.ne_shops` = NE の店舗コード → モール・scope・注文番号の接頭辞 (Yahoo は `'b-faith01-'` + NE 受注番号)。warehouse.db の `shops` を 2026-09-14 に写した (1 楽天 / 2 Yahoo / 4 Amazon 自社発送 / 5 auPAY / 6 Qoo10 / 8 メルカリ / 11・14 LINE ギフト / 7・15 は対象外)。伝票と注文を結ぶ根拠
- `core.mall_order_policy` = モール × scope の「注文を入れてよいか」。**Yahoo は約款 第 10 条の確認まで false (D-32)**。`apply_order_batch` が見て、不可なら例外 (黙って入れない)
- `core.orders` / `order_lines` = モールの注文 (unique = 会社 × モール × scope × 注文番号) と明細 (unique = 注文 × line_key)。金額は JPY だけ (顧客が払った額・商品代・送料・店負担の値引・モール負担の値引・ポイント = D-31 の材料)。取得世代 = `received_batch_seq` / `source_updated_at` / `content_hash` (ヘッダ。送り側) / `lines_checksum` (明細集合。**DB が受け取った配列から計算** = `core.lines_checksum()`。送り側は毎回同じ形の明細 JSON を送る = 鍵の増減も「違い」になる)
- `core.shipments` / `shipment_lines` = NE の伝票 (unique = 会社 × 伝票番号) と明細。`order_id` は NE 受注番号から結ぶ (未着なら null)。`ship_date_jst` = 出荷確定日 (JST)
- `core.apply_order_batch(会社, モール, scope, 注文番号, 世代, ヘッダ jsonb, 明細 jsonb 配列)` / `core.apply_shipment_batch(会社, 伝票番号, 世代, ヘッダ, 明細)` = §4.7 の契約 (鍵単位の advisory lock → ヘッダを for update → 古い世代は `'stale'` → 内容が同じなら世代だけ進めて `'same'` (updated_at は動かない) → 同じ世代で内容違いは例外 → ヘッダ更新 + 明細を現行の集合に合わせて `'applied'`)。**明細の行は消さない** (在庫イベント等の参照を壊さない): (注文 × line_key) / (伝票 × line_no) で upsert し、集合から外れた行は `removed_at`、また現れたら null に戻る。現行の集合 = `removed_at is null`。`qty` は必須 (欠落を 0 にしない)。内部 ID は Render が解決 (出品 = 会社 × モール で 1 件に当たるとき、SKU = 会社 × コード。当たらなければ `unresolved_code`)。状態は `status` が来ればそのまま、無ければ対応表 (NE は 'ne'、モール API はモール名で引く)
- `core.link_shipment_order()` / `core.relink_shipments(会社)` = 伝票 → 注文 (ne_shops の接頭辞つき。伝票をロックしてから探し、見つからなければ order_id を null に = 受注番号の訂正で古い結びを残さない)。未着だった注文が届いたら夜間に結び直す (`'same'` の再送では結ばない)。結ばれていない伝票は `mart.v_shipments_unlinked` に理由つき (no_shop / shop_not_linked / no_order_no / order_missing)
- `events.inventory_events.shipment_line_id` = exact の在庫イベント (ピッキング・梱包) を出荷明細に結ぶ (会社一致)
- `mart.v_shipments_daily` = **既存 `f_shipments_daily` と同じ式** (slips = 出荷確定日のある伝票の数 (取消を含む)、cancelled_slips = 内数、delivery_name = 出荷確定日が一番新しい伝票の名称・無ければ (未設定))

試験 = `node scripts/test-company-db-orders.mjs` (PGlite 11 件。seed / apply の applied・same・stale・例外・現行集合への合わせ込み (removed_at・id 不変)・取消・qty 必須・DB 計算の checksum / 可否 (Yahoo) / 状態の対応・内部 ID・会社違い / 伝票の適用と結び (同じ番号・接頭辞・未着 → relink) / 在庫イベント → 出荷明細 / v_shipments_daily)。🚨 ヘッダの for update の 2 接続の並行は PGlite では書けない

## 出荷を毎日送る (NE 伝票 → core.shipments。08 §4.7 / §9 D5a)

miniPC の raw (warehouse.db の `raw_ne_order_base` = 伝票 / `raw_ne_orders` = 明細) を整えて Render の Company DB に送る。**mirror (Render の SQLite) は経由しない**: 受け皿の関数 `core.apply_shipment_batch()` が世代・冪等・明細集合の置換を担うので「公開マーカー」は要らず (1 chunk が commit されるか・されないかの 2 択)、写しを SQLite に残すと年 50 万伝票ぶん Render のディスクを食う。

- **送り手 (miniPC)**: `apps/company-db/push/ne-shipments.mjs` (整形は `ne-shipments-transform.mjs` = 純粋関数、台帳は `ledger.mjs`)。daily-sync の「日次出荷サマリ」の直後に `--incremental` で走る (NE 取得が失敗した朝は送らない = 古い raw を世代として確定させない)。retry-failed-jobs にも登録 (Render が落ちていた朝の自動復旧)
  - **台帳** = `DATA_DIR/company-db-push.db` (warehouse.db とは別ファイル。warehouse.db は**読むだけ**)。伝票ごとの**指紋** (整形の版 + ヘッダの content_hash + 明細) を持ち、毎回 raw を伝票番号順に全部流し読みして (1 つの読み取り取引。所要は初回のバックフィルで実測) 指紋が変わった伝票だけを台帳の **outbox** に書き、raw の読み取りを閉じてから送る (HTTP の間は raw の snapshot を持たない = WAL の回収を止めない)。**伝票単位で完全な明細集合**。🚨 raw の synced_at (秒精度・取込開始時の時刻を数分後まで使う) をカーソルにすると、読んでいる途中の更新や遅れて commit された古い時刻を飛び越えて変更が永久に届かない (Codex R1) → 時刻には頼らない。台帳は**作り直せる写し** (バックアップの対象にしない): 失くしても次の run が Render から投入済みの伝票番号を取り戻して追跡対象にし (範囲の条件から外れた伝票の訂正も届く)、世代を Render の最大に合わせ、全部を送り直す ('same' が返るだけ)。**追跡対象** (伝票番号) と **送付確認済み** (指紋あり) は別に数える。前回送らずに残った outbox の伝票番号も追跡対象に引き継ぐ
  - **範囲** = 受注日か出荷確定日が 2025-01-01 以降 (D-28) **または 投入済み** (投入済みの伝票は出荷確定日を消されても追跡する)。`--from/--to` は受注日の期間だけ (投入済みでも期間外は送らない = 期間で分けたバックフィルが膨らまない)
  - **世代** = 台帳の batch_seq (最初の chunk を送る直前に取引の中で +1。送る物が無い run では進めない)。run の最初に Render の状態 (`GET .../shipments/status`) を取り、**世代を Render の最大以上に補正**する。**前回受領確認した chunk が Render に無ければ (`GET .../shipments/receipt`) Render が過去に復元・作り直されたとみなし、指紋を空にして全部送り直す** (件数が同じ復元でも見つかる)。それでも Render の伝票数が送付確認済みより少なければ説明のつかない食い違いとして止める (確かめてから `--reset-ledger`)。Render 側は伝票ごとに古い世代を 'stale' で拒む → stale が 1 つでもあれば exit 1 (世代がずれている)
  - 'applied' / 'same' が返った伝票の指紋を台帳に書く。**failed / stale は書かない = 次回また送る**。失敗した伝票が 1 つでもあれば exit 1 (朝の通知に ❌)。整形できない伝票 (受注数が無い・日時の形が違う) も同じ (黙って落とさない)
  - **排他** = 台帳の lock (持ち主・pid・心拍。chunk ごとに心拍を打つ)。**pid が生きていて心拍が 15 分以内のときだけ拒む** = daily-sync の 30 分 timeout で殺された送り手の lock (finally を通らない) は次の再試行を塞がない。HTTP を待った後 (状態・受領記録・伝票番号)・走査中 (5,000 伝票ごと)・世代を取るとき・各 POST と再送の前で持ち主を確かめ、台帳を変える取引 (指紋を空にする・追跡に加える・outbox の引き継ぎ・ack) は持ち主の確認と同じ取引にする = 奪われていたら台帳に何も書かず、後続の送り手の outbox も消さずに止まる
  - **chunk** = 伝票 200 (`CDB_PUSH_CHUNK`) / 明細 5,000 / 8MB のどれかで区切る。明細が 500 行を超える伝票は送らずに「整形できない」に数える (他の伝票は続ける)。5xx・通信エラーは 5・10・20・40・80 秒で 6 回まで再送 (master へのマージで Render が再デプロイされる 1〜3 分の 502 をまたぐ)
  - ヘッダ (受注ベース) がまだ無い伝票の明細は送れない → 範囲の中だけ件数を出す (raw_ne_orders は受注ベースより古くから溜まっていて、2025 年より前の 120 万伝票に受注ベースが無いのは正常)。明細がまだ取れていない伝票は明細 0 件で送る (明細が来たら指紋が変わって次の世代で入る)
- **受け口 (Render)**: `POST /apps/company-db/sync/shipments` (x-sync-key。`apps/company-db/ingest/shipments.mjs`)。1 chunk (≤1000 伝票・≤5000 明細) = 1 取引。伝票ごとに savepoint を切り、失敗した伝票だけを `failed`、古い世代を `stale_slips` に返して他は commit する (1 伝票の不良で 1 日分を止めない)。
  - **再送は保存した応答**: (run_id, chunk_index) ごとに受け取った内容の指紋と応答を `ops.ingest_chunks` (0015) に残し、同じ chunk の再送には適用せず同じ応答を返す (集計を二重に数えない)。同じ chunk_index に違う内容 / 違う世代・版 → 409
  - run = `ops.ingest_runs` (source_system=ne, entity=shipments。checksum=世代、format_version=transform_version、pages=chunk の数)。**last=true の chunk が届き 0〜last がそろったときだけ閉じる** (success / partial。error に失敗の総数)。running のまま 6 時間過ぎた run は送り手が途中で死んだもの (status に stalled)
  - **期限**: 文 20 秒 (残り時間が少なければそれ以下)・ロック 10 秒・chunk 全体 80 秒 (Render の HTTP は 100 秒程度で切れる)。伝票の前後と commit の前に見て、受領記録・集計・閉じる文の 1 文ごとに残り時間を計り直して timeout に入れ (statement_timeout は文ごとに計るので)、超えたら (どの文の timeout に当たった場合も) 全部 rollback して 503 CHUNK_DEADLINE → 送り手は半分に割って送り直す (下限 25 伝票)
  - **終端** = last=true の chunk で 1 回だけ決まる。別の終端 / 終端より大きい chunk を受けていた / 終端の後の chunk / 同じ chunk の last だけ違う再送 → 409 (欠けた run を success にしない)
  - 認証は body parser より前 (server.js の `app.use(['/apps/company-db/sync/shipments', '/apps/company-db/sync/orders'], requireSyncKey)` = 未認可の body を読まない。共通 parser はこの path を小文字で比べて素通り)
- **突合** (08 §9 D5 の「f_shipments_daily との突合」): `--reconcile` = miniPC の旧 `f_shipments_daily` と Render の `mart.v_shipments_daily` (`GET /apps/company-db/sync/shipments/daily?from&to`) を 日 × 店舗 × 配送方法 で比べる (slips / cancelled_slips / delivery_name)。366 日ごとの窓に分けて問い合わせる。差があれば exit 1。🚨 集計枠の中で相殺する欠落・過剰や明細の差は見えない (伝票の数だけ)
- **状態**: `GET /apps/company-db/sync/shipments/status` = 伝票・明細の件数、世代、結ばれていない伝票の理由別件数 (注文が入る D5b までは全部 order_missing)、直近の run (chunk の数・失敗の総数・stalled)。`GET .../shipments/receipt?run_id&chunk_index` (受領記録の有無) / `GET .../shipments/slips?after&limit` (投入済みの伝票番号) は送り手が台帳と Render の食い違いを見つける・直すために使う

```
# 初回のバックフィル (miniPC の一時 worktree + 本番 .env から。9/14 実測: 走査 35 万伝票 ≈ 90 秒、送信 ≈ 1 秒/chunk (200 伝票) = 2 か月 (5 万伝票) ≈ 6 分)
# 🚨 ssh 越しに走らせるときは ssh を切らない (PowerShell Start-Process で切り離しても ssh が切れると子も死ぬ = 9/14 実測)。1 回 9 分以内の窓に分ける。落ちても台帳が ack 済みの分を覚え、残りは次の run が引き継ぐ
node apps/company-db/push/ne-shipments.mjs --incremental --dry-run                # 件数と例 (送らない)
node apps/company-db/push/ne-shipments.mjs --from 2025-08-01 --to 2025-09-30      # 受注日の範囲 (2 か月ずつ)。受注ベースは 2025-08 以降しか無い (それより前は backfill-ne-order-base.js で NE から取り直してから)
...  (2 か月ごとに)
node apps/company-db/push/ne-shipments.mjs --incremental                          # 残り (範囲内で台帳に無い・変わった伝票だけ)
node apps/company-db/push/ne-shipments.mjs --reconcile --all                      # 2025-01-01 から今日まで (366 日ごとに分けて)。配送方法名は全期間の最新行から選ぶので全部入れた後に

# ふだん (daily-sync が毎朝)
node apps/company-db/push/ne-shipments.mjs --incremental
node apps/company-db/push/ne-shipments.mjs --incremental --force                  # 指紋が同じでも送る (Render 側を疑うとき。'same' が返るだけ)
node apps/company-db/push/ne-shipments.mjs --reconcile --days 90                  # 月に 1 回

# Company DB を復元・作り直したとき (台帳の指紋を空にして全部送り直す。伝票番号は残る)
node apps/company-db/push/ne-shipments.mjs --reset-ledger
node apps/company-db/push/ne-shipments.mjs --incremental
```

試験 = `node scripts/test-company-db-shipments-push.mjs` (27 件: 整形 / 流し読みと台帳 (SQLite :memory:) / 受け口 (PGlite: applied・same・stale・失敗の切り分け・再送の保存応答・終端の一貫性・期限超過 (最後の伝票でも)) / 通し (fetch を差し替え: 世代の補正・分割と last・指紋の差分・投入済みの追跡・失敗と stale を台帳に書かない・lock (pid と心拍)・Render の復元 (受領記録が無い → 指紋を空にして送り直す) と --reset-ledger・台帳を失くしたときの取り戻しと outbox の引き継ぎ・lock を奪われたら送らない (状態・受領記録・伝票番号を待つ間でも。台帳も新しい送り手の outbox も触らない)・管理 SQL の timeout も CHUNK_DEADLINE (1 文ごとに残り時間)・応答を失った再送・5xx と期限超過とバイト数の分割) / 突合 (旧 rebuild-shipments-daily.js を本物で呼ぶ))。🚨 HTTP と本番の raw は試験に無い → 初回は `--dry-run` → 1 か月だけ送る → `--reconcile` で確かめる

## 注文を毎日送る (モールの注文 → core.orders。08 §4.1 / §4.7 / §9 D5b)

伝票 (上) と同じ流れ。送り手の共通部 = `apps/company-db/push/pipeline.mjs` (lock → Render の状態 → raw を 1 取引で流し読み → outbox → chunk で POST → ack)、受け口の共通部 = `apps/company-db/ingest/chunk.mjs` (検証 / 1 chunk 1 取引 / 再送は保存した応答 / 終端 / 期限)。台帳 (`DATA_DIR/company-db-push.db`) は**種類ごと** ('shipment' / 'order:<mall>') に鍵・世代・lock・outbox・run を分けて持つ (D5a の台帳はそのまま引き継ぐ。表の用意と移行は **1 つの取引 (BEGIN IMMEDIATE)** = 途中で落ちても半端にならず、同時に開いても片方が待つ (busy timeout 30 秒を過ぎれば開けずに終わる = 壊さない)。取引の外で走った古い移行の残り (`outbox_v2`) は、元の outbox が無ければ唯一の写しとして昇格させる)。まず楽天 (D5b-1)。

- **送り手 (miniPC)**: `apps/company-db/push/mall-orders.mjs --mall rakuten` (整形は `mall-orders-transform.mjs` = 純粋関数)。daily-sync の「楽天 RMS API」の直後に `--incremental` で走る (楽天の取込が失敗した朝は送らない)。送った後に **伝票との結び直し** (`POST .../shipments/relink` = `core.relink_shipments_bulk()`。0016 → **0017** = 候補の temp table に照合用の (mall, scope_key, mall_order_no) を列として作ってから注文と等結合 + analyze。0016 の「2 つの表の列を組んだ式での結合」は 2 万件で planner が Hash Join + Join Filter (27 万注文 × 2 万候補) に反転して 60 秒の timeout に当たった = 9/16 の本番投入で発覚。shipment_id の順に **5,000 件ずつ** (`--relink-limit`、受け口の上限 100,000。9/16 本番相当の検証 (楽天の注文に多数一致する候補・rollback) = 5,000 件 0.75 秒 (index の nested loop) / 20,000 件 3.4 秒 (照合鍵が Hash Cond に入った Hash Join)。0016 の形は 20,000 件で 60 秒超)) を **同じ lock の中で** 回す = 注文より先に届いた伝票の order_id が埋まる。「結び直しが要る・先頭も見直す」の印 (台帳の meta `relink_pending` = 1 / `relink_rescan` = 1。走査中の位置 `relink_next` には触らない) は **最初の chunk を送る直前 (世代を取る取引) に書く** = 応答を失って run が落ちても印は残り、次の run が (送る物が無くても) 回す。走査は **続きの位置から走り終え、その間に注文が入っていれば (rescan) 先頭からもう一度** = 予算で打ち切った走査が毎朝の変更注文で先頭へ戻り続けない。**失敗した run は ❌ (exit 1 = retry の対象)**。続きの位置 (`relink_next`) は **HTTP 成功のたびに** 書く (途中で落ちても・30 分で殺されても済んだ所から)。200 回か **時間予算 10 分** (`CDB_RELINK_BUDGET_MS`) で打ち切り = 次の run が続きから。完了で印を消す (持ち主の確認と同じ取引 = 別の送り手が消せない)。`--relink` 単独も打ち切りなら exit 1。受け皿は `for update` で待つ (skip locked にしない = 飛ばした伝票を「完了」にしない。lock_timeout 10 秒で失敗 → 次の run)
  - **範囲** = 注文日 (order_date) が 2025-01-01 以降 (D-28) **または 追跡中 または 2025-01-01 以降に出荷確定した楽天の伝票 (raw_ne_order_base の店舗 1) が参照する注文** (D-28 の「対象期間の出荷から辿れる古い注文」= 年またぎ。古いかどうかは **楽天側の注文日** で見る = NE の受注日と一致する保証が無い。raw_ne_order_base が無ければ警告して入れない)。`--from/--to` は注文日の範囲だけ (初回のバックフィルを 2 か月ずつ)
  - **楽天の列の対応**: 注文の鍵 = order_number (mall 'rakuten' / scope 'main' / shop_code '1') / ordered_at = order_date ('+0900' → '+09:00') / 状態 = orderProgress (100〜900 → `core.order_status_map` 'rakuten' (0016)。100 / 200 = new、300 = confirmed、400 = on_hold、**500 発送済 / 600 支払手続き中 / 700 支払手続き済 = shipped** (600 / 700 は発送後の決済の状態 = apps/rakuten-unshipped の定義と同じ。配達完了の根拠は無いので delivered にしない)、800 / 900 = キャンセル系 → is_cancelled) / 金額 = request_price (顧客が払う額) / goods_price (商品代) / postage_price (送料) / coupon_shop_price (店負担) / coupon_all_total_price − coupon_shop_price (モール負担)。ポイントは raw に無い (null) / 明細 = item_detail_id ごと (listing_code = item_number (商品番号 W)、qty = units、取消明細 (delete_item_flag) は cancelled_qty = units、単価 = price_tax_incl、税率 0.08 / 0.10 以外は null) / source_updated_at = synced_at (楽天の raw にはモール側の更新時刻が無い → 変化の判定は指紋)
  - 🚨 **取込の番兵 -9999 (値が無かった) と負の金額は null** にする (Render の CHECK >= 0 に当てない。件数は run の最後に出す)。欠落 (units 無し・order_date 無し・item_detail_id の重複) は整形できない ❌ (0 にしない)
  - 🚨 **色違い (同じ商品番号 W を共有する SKU) は解決できない**: `core.resolve_listing_id()` (0016) は listing_code か別名 (`core.external_ids` の listing・同じモール・失効していない) で **1 件に決まるときだけ** 解決し、決まらなければ `unresolved_code` に原文を残す。宿題 = raw に SKU 単位のコード (SKU 管理番号 / variantId) を足して取り直す
  - 🚨 楽天の取込 (rakuten-orders.js) は注文日で直近 7 日しか読み直さない → それより古い注文の遅いキャンセルは raw に届かない (raw 側の宿題。Render は raw の写しなので raw が直れば翌朝届く)
  - 🚨 **RMS 仕様と未照合の前提** (Codex が 2 巡とも「問題なしと判定できない」と残した): request_price のポイント・手数料の扱い、goods_price / postage_price の税区分、coupon_all_total_price − coupon_shop_price = 楽天負担、item_detail_id が再取得・注文変更・配送先分割で変わらないこと。**バックフィルの後に実注文 (税・ポイント・クーポンあり) を RMS 画面と突き合わせ、同じ注文の変更前後の応答を比べる** のが残る確認
- **受け口 (Render)**: `POST /apps/company-db/sync/orders` (x-sync-key。`apps/company-db/ingest/orders.mjs`)。1 chunk = **1 モール × 1 scope** (≤1000 注文・≤5000 明細) = 1 取引。伝票と同じ約束 (注文ごとに savepoint / 再送は保存した応答 / 終端は 1 回だけ / 期限 80 秒 → 503 で割る / 文ごとに残り時間)。run = `ops.ingest_runs` (source_system = モール名、entity = orders、scope_key)。**run の種類 (source_system / entity / scope_key)・世代・版は最初の chunk で固まり**、伝票の run_id や別の scope の chunk が混ざれば適用前に 409
  - `GET .../orders/status?mall&scope` (注文・明細の件数、世代、直近の run) / `.../orders/receipt?run_id&chunk_index` / `.../orders/keys?mall&scope&after&limit` (台帳を作り直すとき) / `.../orders/daily?mall&scope&from&to` (突合の材料) / `POST .../shipments/relink {after, limit}` → { linked, examined, last_id }
  - 認証は body parser より前 (server.js の `app.use([...shipments, ...orders], requireSyncKey)`)
- **突合**: `--reconcile` = raw_rakuten_orders と Render の core.orders を **注文日ごとの 注文数 / 明細数 (現行の集合) / 商品代 (goods_price。番兵・負は 0) の合計 / 取消の注文数** で比べる (同じ式を両側に持つ。差があれば exit 1)

```
# 初回のバックフィル (miniPC。伝票と同じ注意 = ssh を切らない・1 回 9 分以内の窓に分ける)
node apps/company-db/push/mall-orders.mjs --mall rakuten --incremental --dry-run      # 件数と例 (送らない)
node apps/company-db/push/mall-orders.mjs --mall rakuten --from 2025-01-01 --to 2025-02-28 --no-relink   # 送るだけ (窓 1 回 9 分に結び直しを含めない)
...  (2 か月ごとに)
node apps/company-db/push/mall-orders.mjs --mall rakuten --incremental --no-relink    # 残り (送るだけ)
node apps/company-db/push/mall-orders.mjs --relink                                    # 伝票との結び直しを 1 回 (初回 9/16 実測: 50.9 万伝票を 2,000 件 × 256 回 = 86 秒で完走・結んだ 265,909 件)。時間予算 (10 分) で打ち切ったら表示される `--relink-after <shipment_id>` で続きから (単独 --relink は Render を叩くだけなので DATA_DIR 不要)
node apps/company-db/push/mall-orders.mjs --mall rakuten --reconcile --all

# ふだん (daily-sync が毎朝)
node apps/company-db/push/mall-orders.mjs --mall rakuten --incremental
node apps/company-db/push/mall-orders.mjs --relink                                    # 伝票との結び直しだけ
node apps/company-db/push/mall-orders.mjs --mall rakuten --reconcile --days 90        # 月に 1 回
node apps/company-db/push/mall-orders.mjs --mall rakuten --reset-ledger               # Company DB を復元・作り直したとき (自動でも見つける)
```

試験 = `node scripts/test-company-db-orders-push.mjs` (32 件: 0016 (対応表 / 別名の解決 / 集合の結び直し) / 整形 / 受け口 (1 モール × 1 scope・伝票の run や別の scope と混ざらない・D5a の保存応答の再送) / **本物の router を PGlite で mount して HTTP で** (401 / 400 / 409 / replay / 各 GET) / 台帳の種類 (D5a の台帳の引き継ぎ・移行は 1 取引 = 残りの昇格・途中失敗の rollback・同時 open) / 通し (本物の受け口を HTTP で: 範囲・変更・--force・範囲指定・台帳を失くした・突合・lock の中の結び直し・失敗と打ち切りの持ち越し (位置は HTTP ごと・時間予算)・応答を失った run の後・D-28 の年またぎ (楽天の注文日で)・予算で打ち切った走査が先頭へ戻り続けない))。伝票の試験は 28 件 (範囲指定は追跡中でも期間外を送らない、を追加)。伝票の試験 27 件も共通部の上で通る。次 = D5b-2 以降 (Amazon / auPAY / Qoo10 / LINE ギフト。Yahoo は D-32 の確認まで入れない)

### Amazon の注文 (D5b-2。0018)

- **元 = `raw_sp_orders`** (注文 ID 単位で最新の状態に置き換わる current 表。`apps/warehouse/sp-api-orders.js` が注文レポート BY_LAST_UPDATE 7 日分から毎朝作る)。追記ログ `raw_sp_orders_log` は 60 日で回転するので使わない。2026-09-18 の実測 = 2025-01-01 以降 **128.6 万注文 / 132.5 万明細** (FBA 116.5 万 / 自社発送 12.1 万。月 6〜7.7 万注文)
- **鍵** = amazon_order_id → mall `amazon` / scope `jp`。**shop_code = 自社発送は `'4'` (NE の店舗 4)、FBA は null** (FBA は NE を通らない)。伝票との結び = NE 店舗 4 の受注番号そのまま (実測 118,212 伝票のうち 117,697 が一致)
- 🚨 **マルチチャネル発送は送らない**: sales_channel が `Amazon.co.jp` でない注文 (`Non-Amazon` / `Non-Amazon JP`。他モールの注文を FBA から出しただけ = Amazon の売上ではない。635 注文) は送り手が飛ばして数える (突合の式も同じ条件)
- **明細 ID が無い** (同じ注文・SKU・ASIN で 2 行ある組が 838) → line_key = `<seller_sku>|<asin>#<同じ組の中の番号>` (組の中は内容で並べる = 取込のたびに raw の id が変わっても同じ鍵)。listing_code = seller_sku (core.listings の Amazon と同じ)
- **金額**: item_price は **行の合計 (単価 × 数量) で税込** (item_tax は内数。10/110 に合う行 96%)。unit_price_jpy は割り切れるときだけ、tax_rate は item_tax から逆算 (10% / 8% の一方にだけ合うとき)。顧客が払った額・モール負担の値引・ポイントはレポートに無い (null)。
  🚨 取込側が `parseFloat(x) || 0` で入れているので raw では「値が無い」と「0 円」を区別できない。Amazon は取消の行の数量・金額を空にする → **item_price = 0 は null にして数える** (0 円の売上として確定させない)。数量 0 はそのまま 0 (qty は必須)、取消でないのに数量 0 の行は数える。**取消でない明細の金額が 1 つでも分からない注文は、ヘッダの 商品代・送料・店負担の値引 を null** (分かる行だけの部分和を注文の合計にしない。突合の式 dailySql も同じ規則で、**整形と同じ前処理 (金額は NULL → 0・四捨五入してから > 0、状態は前後の空白を除く) をしてから判定する**)。送料・値引の 0 は「item_price が入っている行のもの」だけ信じる (レポートは金額の列を行ごとにまとめて埋めるか空にする)
- **状態** = order-status の原文を status_source に → 0018 の対応表 (`Shipped` = shipped / `Shipped - Delivered to Buyer`・`Shipped - Picked Up` だけ delivered / 戻り系 = returned / `Unfulfillable` = on_hold / `Pending` = new / `Cancelled` = cancelled)。表に無い値は unknown (DQ に出る)
- **更新時刻** = last_updated_date (モール側の更新時刻)。ただし送る・送らないは内容の指紋で決める (時刻だけ変わっても送らない)
- **daily-sync** = 「Amazon SP-API」の直後に `--mall amazon --incremental --require-backfilled`。**台帳にバックフィルの完了印 (meta `order:amazon:backfill_done`) が付くまでは「バックフィル前」と出して送らない** (128 万注文を 30 分の枠で送り始めない)。
  完了印は **人が `--mark-backfilled` で付ける** (全期間を流して `--reconcile --all` が一致したのを見てから)。指紋の件数では判定しない = 1 か月だけ流した翌朝に残り全部を送り始めない。1 件も送っていない台帳・知らない mall には付かない (早すぎる印そのものは検出できない = 突合を見てから付ける約束)。`--reset-ledger` は指紋だけ空にするので完了印は残る (翌朝 daily-sync が送り直す)。
  🚨 台帳のファイルごと失くしたときは完了印も消える → 朝の通知に「バックフィル前」が出る → 手で `--mall amazon --incremental --no-relink` を流し (pipeline が Render の投入済みの鍵を取り戻して全部送り直す = 'same' が返るだけ)、終わったら `--mark-backfilled`

```
# 初回のバックフィル (miniPC の PowerShell で直接。ssh 越しに流さない = 1 窓 20〜30 分かかる。DATA_DIR は daily-sync が渡すものなので手で流すときは --data-dir)
cd C:\Users\bfaith\bfaith-portal
node -r dotenv/config scripts\company-db\migrate.mjs                                   # 0018 (applied=1)
node apps\company-db\push\mall-orders.mjs --mall amazon --incremental --dry-run --data-dir C:\Users\bfaith\bfaith-portal\data   # 件数と例 (送らない)。整形できない 0 を確かめる
node apps\company-db\push\mall-orders.mjs --mall amazon --from 2025-01-01 --to 2025-01-31 --no-relink --data-dir C:\Users\bfaith\bfaith-portal\data   # まず 1 か月 (約 5.5 万注文)
node apps\company-db\push\mall-orders.mjs --mall amazon --reconcile --from 2025-01-01 --to 2025-01-31 --data-dir C:\Users\bfaith\bfaith-portal\data  # 1 か月ぶんが一致するか
...  (合っていたら 3 か月ずつ: 2025-02-01〜04-30 / 05-01〜07-31 / … 今日まで。どれも --no-relink)
node apps\company-db\push\mall-orders.mjs --mall amazon --incremental --no-relink --data-dir C:\Users\bfaith\bfaith-portal\data    # 残り (範囲内の出荷が参照する古い注文など)
node apps\company-db\push\mall-orders.mjs --relink                                      # 伝票との結び直しを 1 回 (NE 店舗 4 の約 11.8 万伝票が結ばれる)
node apps\company-db\push\mall-orders.mjs --mall amazon --reconcile --all --data-dir C:\Users\bfaith\bfaith-portal\data
node apps\company-db\push\mall-orders.mjs --mall amazon --mark-backfilled --data-dir C:\Users\bfaith\bfaith-portal\data          # 上の突合が一致したのを見てから。これで翌朝から daily-sync が送る
```

試験 = `node scripts/test-company-db-orders-push-amazon.mjs` (18 件: 整形 (FBA / 自社発送 / 取消 = 金額 null / 金額の分からない明細が残る注文は合計 null / 一部取消 / 数量 0 / 明細 ID なしの鍵と行の順 / 指紋 / 更新時刻 / 例外 / 税率の逆算) / 0018 の対応表 / 通し (範囲・マルチチャネル発送を送らない・出品の解決・差分・突合・NE 店舗 4 の伝票との結び・バックフィルの窓・sales_channel NULL・金額 NULL / 0.1 円・空白つきの状態・注文全体の取消の突合・完了印 (途中まででは付かない / 指紋を空にしても残る)・--require-backfilled / --mark-backfilled の CLI = 素の TCP の待ち受けで「Render へ繋ぎに行ったか」を接続の数で確かめる・知らない mall と未送信の台帳には印が付かない))。楽天と共通の部分 (台帳・chunk・再送・lock・結び直しの持ち越し) は test-company-db-orders-push.mjs

### au PAY・LINE ギフトの注文 (D5b-3。0019)

- 🚨 **どちらの raw にも個人情報の列がある** (au PAY = 注文者・送付先の氏名・住所・電話・メール・自由記述 / LINE ギフト = LINE の ID・送付先の氏名・住所・電話) → 送り手は **固定の一覧の列だけを select する** (`AUPAY_COLUMNS` / `LINEGIFT_COLUMNS`。core の列にも個人情報は無い)
- **Company DB に au PAY・LINE ギフトの出品 (core.listings) は無い** → 明細は `sku_code` で SKU に当てる (au PAY = item_code。2025-01 以降の 90% が m_products に当たる / LINE ギフト = sku_code = variation.code。100%)。当たらなければ Render が `unresolved_code` に原文を残す
- **au PAY** (`raw_aupay_orders`。2025-01-01 以降 12,238 注文 / 13,238 明細): 鍵 = order_id → `aupay` / `main` / shop_code `'5'` (NE 店舗 5 の伝票 11,950 のうち 11,931 が受注番号一致)。order_date は `'YYYY/MM/DD HH:MM'` (JST・秒なし)。
  金額は実測で 3 つの式が全注文で成り立つ (明細の合計 = total_sale_price / total_price = 商品 + 送料 + 手数料 + オプション + ラッピング / request_price = total_price − クーポン − ポイント − au ポイント) →
  顧客が払った額 = request_price / 商品代 = total_sale_price / 送料 = postage_price / **店負担の値引 = coupon_total_price** (ストアクーポン。既存の f_aupay_finance と同じ扱い) / ポイント = use_point + use_au_point_price / モール負担 = null。
  状態 = order_status の原文 (完了 = 発送後 = shipped / 発送待ち = confirmed / 発送前入金待ち = new / キャンセル)。🚨 raw は注文日 7 日の窓でしか更新されない = 古い注文は発送済みでも「発送待ち」のまま残り得る (raw 側の限界)
- **LINE ギフト** (`raw_linegift_orders`。1 行 = 1 注文 = 1 商品。5,809 注文。**raw は 2026-02-07 以降だけ** = NE 店舗 14 の伝票 10,399 のうち結べるのは 5,388): 鍵 = order_id → `linegift` / `main` / shop_code `'14'`。
  商品代 = selling_price (税込・送料込みの価格設定)。送料・値引・ポイント・顧客が払った額は API に無い (null)。fee (モール手数料) は注文の金額ではないので送らない。
  🚨 **状態 `received` は「届いた」ではない**: received の全件に発送時刻 (delivered_on) と送り状番号があり、delivered_on = NE の出荷確定日、received_on は delivered_on とほぼ同時刻 = 店が発送した後の終端の状態 → shipped。shipped_at_source = delivered_at_jst
- **daily-sync** = それぞれの取込の直後に `--mall aupay|linegift --incremental --require-backfilled`。件数は小さい (初回でも数分) が、**0019 より先に送ると状態が unknown のまま入り、内容が変わるまで送り直されない** → Amazon と同じ完了印で止める

```
# 初回 (miniPC の PowerShell。0019 の適用 → 投入 → 突合 → 完了印)
cd C:\Users\bfaith\bfaith-portal
node -r dotenv/config scripts\company-db\migrate.mjs                                   # 0019 (applied=1)
node apps\company-db\push\mall-orders.mjs --mall aupay --incremental --dry-run --data-dir C:\Users\bfaith\bfaith-portal\data      # 整形できない 0 を確かめる
node apps\company-db\push\mall-orders.mjs --mall aupay --incremental --data-dir C:\Users\bfaith\bfaith-portal\data                # 約 1.2 万注文 + 伝票との結び直し
node apps\company-db\push\mall-orders.mjs --mall aupay --reconcile --all --data-dir C:\Users\bfaith\bfaith-portal\data
node apps\company-db\push\mall-orders.mjs --mall aupay --mark-backfilled --data-dir C:\Users\bfaith\bfaith-portal\data            # 突合が一致したのを見てから
(--mall linegift でも同じ 4 行。約 0.6 万注文)
```

- 🚨 **注文日時が読めない注文は黙って範囲の外に落とさない**: iterate が `invalidDate` の印を付け、`--incremental` でも `--from/--to` でも必ず整形に渡して「整形できない」❌ にする (範囲・突合は日時の先頭 10 文字を JST の日付として使うので、LINE ギフトは `+09:00` の ISO8601 以外の形 (Z など) も拒む)。
  **約束 = 範囲の判定に使う値は、整形が検証するのと「同じ値・同じ行の集合」で検証する** (Codex 4 巡の指摘が全部この型): 形 → 実在する日時 (13 月・24 時・2 月 30 日。`Date.parse` は繰り上げて受ける) → 原文のまま (trim しない) → 原値のまま (`String()` に通さない = BLOB) → au PAY は後ろの明細の日時が先頭と違う注文も印を付ける

試験 = `node scripts/test-company-db-orders-push-aupay-linegift.mjs` (13 件: 整形 (au PAY = 金額・取消・欠落を 0 にしない・指紋 / LINE ギフト = 1 行 1 注文・取消) / 0019 の対応表 (delivered を作らない) / **送り手が個人情報の列を読まない・運ばない (payload・ログ・dry-run の例・整形できない注文の記録)** / 通し (範囲・出荷が参照する古い注文・SKU の解決・差分・突合・NE 店舗 5 / 14 の伝票との結び・JST の日付の境目・整形できない注文は ❌ でほかは届く・**読めない日時は 2 モール × 2 mode で ❌**・取込時刻だけ変わっても送り直さない))

### Qoo10 の注文 (D5b-4。0020)

- **元 = `raw_qoo10_orders` の API の行だけ** (`source_type` が `api_` で始まる行。order_id = `api:<注文番号>`、1 行 = 1 注文 = 1 商品。**2026-02-19 以降**。9/19 の実測 1,994 行)
- 🚨 **旧データの行 (`legacy_migration`。17,252 行・〜2026-05-17) は送らない** (送り手が飛ばして数える。突合の式も同じ条件): 鍵がカート番号 (pack_no) に潰れていて注文番号が無い = NE の受注番号 (10 桁の注文番号) に 1 件も当たらない / 入金日・出荷日が無い / 2026-02〜05 は API の行と同じ注文が二重にある。既存の f_qoo10_finance も `legacy_fields_missing = 0` の行だけを使っている。
  = **Qoo10 は D-28 (2025-01-01 以降) を満たせない**: Qoo10 の API は 90 日より前を取り直せないので、2026-02-19 より前の Qoo10 の注文は Company DB に入らない (NE 店舗 6 の伝票は 2025-01〜2026-02 の約 3,000 件が注文と結ばれないまま残る)
- **鍵** = source_order_key (注文番号・10 桁) → `qoo10` / `main` / shop_code `'6'`。NE 店舗 6 の伝票は API の期間で 1,950 のうち 1,911 が注文番号で一致。**27 伝票は NE がカート番号 (9 桁) で起票している → 結べない** (宿題。カート番号は明細の `source_line_ref = pack_no:<番号>` に残してある)
- **出品と SKU の両方を送る**: listing_code = item_code (Company DB の Qoo10 の出品は listing_code = Qoo10 の商品番号) / sku_code = seller_item_code (販売者商品コード。87% が m_products に当たる)。オプション商品のコード (option_code) は m_products にほぼ当たらないので使っていない (宿題)
- **金額** (実測で `total = order_price × order_qty − discount` が全行で成立): 商品代 = order_price × order_qty (値引前) / 送料 = shipping_rate (実測は全件 0) / 店負担の値引 = seller_discount + cart_discount_seller /
  **モール負担の値引 = discount (メガ割など。settle_price が値引前の 90% のまま = 店の入金は減らない) + cart_discount_qoo10** (既存の f_qoo10_finance と同じ区分) / 顧客が払った額 = カート単位の値引の按分が分からないので null。
  🚨 金額の列は `NOT NULL DEFAULT 0` = 「値が無い」と「0 円」を区別できない → 単価 0 は金額を null にして数える (実測 0 件)
- **状態** = shipping_status の原文 → 0020 の対応表 (`Awaiting shipping(1)` = 入金待ち = new / `Seller confirm(3)` = 発送できる = confirmed / `On delivery(4)` = shipped / `Delivered(5)` = delivered)。
  🚨 **取消は API に出てこない** (取込は状態 1〜5 だけ) = 取り消された注文は最後に見えた状態のまま残る。is_cancelled は常に false (raw 側の限界)
- 注文日時は `'YYYY-MM-DD HH:MM:SS'` (JST)。ほかのモールと同じ約束 = 範囲の判定と整形が同じ関数 (`isQoo10Jst`。原値のまま) で検証し、読めなければどの mode でも「整形できない」❌
- **daily-sync** = 「Qoo10」の取込の直後に `--mall qoo10 --incremental --require-backfilled` (0020 の適用 → 初回の投入 → 突合 → `--mark-backfilled` まで「バックフィル前」)。
  **Qoo10 の取込が失敗した朝**は送信を見送り、「⏭️ skipped」の失敗として retry-state に載せる → 8:30 / 10:00 / 11:30 の自動再試行で **取込が成功した回に送信も走る** (取込がまた失敗した回は送らない = `apps/warehouse/retry-failed-jobs.js` の `UPSTREAM_OF`)。
  ほかのモール (楽天・Amazon・au PAY・LINE ギフト) と NE 伝票は、取込そのものが自動再試行の対象ではないので、見送った送信は retry に載せない (翌朝の daily-sync が台帳の指紋で追いつく = 1 日遅れるだけで失われない)

```
# 初回 (miniPC の PowerShell)
cd C:\Users\bfaith\bfaith-portal
node -r dotenv/config scripts\company-db\migrate.mjs                                   # 0020 (applied=1)
node apps\company-db\push\mall-orders.mjs --mall qoo10 --incremental --dry-run --data-dir C:\Users\bfaith\bfaith-portal\data      # 整形できない 0 を確かめる
node apps\company-db\push\mall-orders.mjs --mall qoo10 --incremental --data-dir C:\Users\bfaith\bfaith-portal\data                # 約 2,000 注文 + 伝票との結び直し
node apps\company-db\push\mall-orders.mjs --mall qoo10 --reconcile --all --data-dir C:\Users\bfaith\bfaith-portal\data
node apps\company-db\push\mall-orders.mjs --mall qoo10 --mark-backfilled --data-dir C:\Users\bfaith\bfaith-portal\data            # 突合が一致したのを見てから
```

試験 = `node scripts/test-company-db-orders-push-qoo10.mjs` (10 件: 整形 (金額の区分・旧データの行や形の違う行は例外・単価 0・日時は原値のまま検証・指紋) / 0020 の対応表 / 通し (旧データの行を送らない・出品と SKU の解決・差分・突合・NE 店舗 6 の伝票との結び = カート番号の伝票は結べない・読めない日時は 2 mode で ❌・前後に空白がある鍵や order_id と食い違う行は追跡中でも ❌・同じ注文番号の 2 行は片方だけ範囲の外でも ❌))

### Yahoo の注文 (D5b-5。0031)

- **D-32 (Yahoo の受注データを持ってよいか)** = 0013 では「b) 確認できるまで入れない」(`core.mall_order_policy` の yahoo = false)。**2026-09-26 に中原さんが「Yahoo の注文を Company DB に入れてよい」と決めた = a)** → 0031 で true に
- **元 = `raw_yahoo_orders`** (1 行 = 注文 × 明細 (line_id)。2025-01-01 から。9/26 の実測 = 90,854 注文・99,843 行)。**個人情報の列はこの表に無い** (注文番号・日時・状態・金額・商品コード・数量だけ)
- **鍵** = order_id (`b-faith01-…`) → `yahoo` / `main` / shop_code `'2'`。NE 店舗 2 の伝票は受注番号 (8 桁) を接頭辞 `b-faith01-` つきで結ぶ (0013 の core.ne_shops)
- **明細**: listing_code = item_id (Yahoo の商品コード。Company DB の Yahoo の出品 = yahoo_registered_items の商品コード) / sku_code = sub_code (サブコード。9 割は空 = 無ければ item_id)
- **金額** (API の公式説明 developer.yahoo.co.jp/webapi/shopping/orderInfo.html を 2026-09-26 に確認):
  - 商品代 = Σ unit_price × quantity。🚨 **UnitPrice は「ストアクーポン利用の注文は、クーポン値引き後の金額」** = 店のクーポンはもう引かれている (実測: coupon_discount のある注文で total_price にクーポンが引かれた形は 0 件) → coupon_discount を値引きにもう一度足さない
  - 顧客が払った額 = total_price (TotalPrice = 小計 − 利用ポイント + ギフト包装料 + 手数料 − 値引き + 送料 + 調整額 − モールクーポン値引き額 − …) / 送料 = ship_charge / 店負担の値引 = discount (注文後にストアクリエイター Pro で入れた値引き) / ポイント = use_point
  - **モール負担の値引 = null**: API には TotalMallCouponDiscount があるが取込 (yahoo-orders.js) が取っていない。実測で約 1 割の注文は total_price がこれだけ少ない (半額など) = 作らない (宿題 = 取込でこの列を取る)
  - 手数料 (pay_charge)・ギフト包装料は Company DB に列が無い (total_price にだけ入る)
- **状態** = OrderStatus - PayStatus - ShipStatus を `'5-1-3'` の形の 1 つの原文にして送る → 0031 の対応表 (5-1-3 shipped / 5-1-4 delivered / 2-0-0 new / 2-1-1 confirmed / 2-1-3・2-0-3 shipped / 4-*-* cancelled)。
  実測で出ていない組み合わせと意味の分からない 5-0-0 (1 件) は unknown (DQ に出る)。OrderStatus 4 = キャンセル → is_cancelled・明細の取消の数量 = 数量 (取消の明細は数量 0 で来ることが多い)
- 注文日時は `'+09:00'` の ISO8601。ほかのモールと同じ約束 = 範囲の判定と整形が同じ関数 (`isYahooJst` / `isYahooOrderNo`。原値のまま) で検証し、読めなければどの mode でも「整形できない」❌ (後ろの明細だけ日時が違う注文も)
- **daily-sync** = 「Yahoo!ショッピング」の取込の直後に `--mall yahoo --incremental --require-backfilled` (0031 の適用 → 初回の投入 → 突合 → `--mark-backfilled` まで「バックフィル前」)。
  Yahoo の取込が失敗した朝は送信を見送る (翌朝の daily-sync が台帳の指紋で追いつく)。送信そのものが失敗したら 8:30 / 10:00 / 11:30 の自動再試行に載る
- 🚨 見張り (09) の ORDER_MALLS にはまだ入れていない (W7〜W11 の Yahoo は、完了印のあとに別の変更で足す)
- 🚨 **売上日次 (mart.sales_daily) には公開しない** (中原さん 2026-09-26。MALL_SPECS.yahoo.salesDaily = false): モール負担が null の注文を mart が 0 として「払った額」を出す = 約 1 割の注文で多く出る (#1465 Codex R1)。
  push の後の作り直しを回さない・`--refresh-sales --mall yahoo` は例外。宿題 = VPS の orderInfo の Field に TotalMallCouponDiscount を足して取込で取る + mart がモール負担の分からない注文の払った額を「不明」にする (au PAY も同じ形)
- 🚨 **取消の取込** (2026-09-26 に直した): 以前の取込 (yahoo-orders.js) は数量 0 の明細を一律 skip していた = 取消 (OrderStatus 4) は数量 0 で返るので、後から取り消された注文が raw に届かず取消前の状態のまま残っていた (毎朝 10 件前後)。
  取消の注文だけ数量 0 を受けるようにした。**過去に取り逃した取消は取込の窓 (7 日) の外** = `node apps\warehouse\yahoo-orders.js backfill 20250101 <今日>` で取り直す (VPS 側 1 秒 1 件 = 約 1 日かかる) か、残る分を突合で見つける

```
# 初回 (miniPC の PowerShell)
cd C:\Users\bfaith\bfaith-portal
node -r dotenv/config scripts\company-db\migrate.mjs                                   # 0031 (applied=1)
node apps\company-db\push\mall-orders.mjs --mall yahoo --incremental --dry-run --data-dir C:\Users\bfaith\bfaith-portal\data      # 整形できない 0 を確かめる
node apps\company-db\push\mall-orders.mjs --mall yahoo --incremental --data-dir C:\Users\bfaith\bfaith-portal\data                # 約 9 万注文 + 伝票との結び直し
node apps\company-db\push\mall-orders.mjs --mall yahoo --reconcile --all --data-dir C:\Users\bfaith\bfaith-portal\data
node apps\company-db\push\mall-orders.mjs --mall yahoo --mark-backfilled --data-dir C:\Users\bfaith\bfaith-portal\data            # 突合が一致したのを見てから
```

試験 = `node scripts/test-company-db-orders-push-yahoo.mjs` (10 件: 整形 (金額の区分 = 単価はクーポン後・払った額・ポイント・モール負担は null / 明細の並びとサブコード・税率 / 取消 / 読めない値は例外 / 指紋) / 0031 (有効化・状態の対応表) / 通し (出品と SKU の解決・差分・突合・取消・NE 店舗 2 の伝票との結び・読めない日時と注文番号は 3 mode で ❌))

## 売上の日次 mart.sales_daily (0021。08 §4.5 / §9 D7a)

注文 (core.orders + 現行の明細) を **注文日 (JST) × モール × scope × shop_code × 出品 × SKU** に集計した表。08 §4.5 の最初の 1 本で、注文別の利益 (`v_order_profit`) は Amazon 財務 (F2b) と広告がそろってから。

- **読むのは `mart.v_sales_daily`** (公開中の行だけ)。`mart.sales_daily` をじかに読むと、古い run の行も混ざる
- **売上の定義 (D-31)**: `sales_jpy` = (商品代 − 取消した商品代) + 送料 − 店負担の値引 (税込)。`customer_paid_jpy` = 売上 − モール負担の値引 − ポイント (計算値。モールの言う「払った額」は `core.orders.total_amount_jpy`)
- **按分**: 送料・値引・ポイントは注文のヘッダにしか無い → 注文の中の明細へ配る。重み = 取消を引いた商品代の比 → (合計が 0 なら) 数量の比 → (それも 0 なら) 等分。端数は **最大剰余法** = 明細に配った額の合計がヘッダの額と 1 円も違わない (商は整数の商 `div()`。明細の一部取消の取消額の四捨五入も `div(2ac + q, 2q)` で厳密に。numeric の割り算は有限桁に丸まり floor の前に切り上がることがある。中間の計算は numeric、保存のときに bigint = あふれたら明示の例外)。
  取り消された注文は商品代と取消だけ数え、送料・値引・ポイントは配らない (売上 0)。明細の一部取消は数量の比で取消額を出す
- **金額の分からない明細** (Amazon の取消・保留など `line_amount_jpy` が null) は 0 として足し、`lines_amount_unknown` に数える (0 円の売上と区別できる)。出品にも SKU にも当たらない明細は `lines_unresolved`
- **shop_code を粒度に入れている**: Amazon の 自社発送 (`'4'`) / FBA (null) はここでしか見分けられない。粒度の鍵 `grain_key` は null と実際の値がぶつからない形 (`n` / `s:<shop_code>`)
- 🚨 **P-5 = run_id publish (上書きしない)**: 行は run ごとに追記し、日付ごとの「公開の指し先」(`mart.sales_daily_published`) を同じ取引で差し替える。作りかけ・失敗の run は取引ごと消えるので見えない。指されなくなった古い行は 3 日の猶予のあと `mart.purge_sales_daily()` が消す (全部終わった回のついでに受け口が呼ぶ)
- 🚨 **どの日を作り直すかは DB が自分で見つける** (`mart.refresh_sales_daily()`): 前回そろって終わった回の開始時刻 (watermark) より後に `core.orders.updated_at` が動いた注文の注文日 + まだ公開の無い日。送り手から「変わった日付」は受け取らない = 途中で落ちた run の分が失われない。
  - updated_at は取込の取引の開始時刻 = 集計を始めた後で commit される取込がある → **watermark から 15 分さかのぼって拾う** (受け口の chunk の期限は 80 秒)。直前に動いた日が次の回でもう一度作り直されるのは設計どおり (無害)
  - 1 回の呼び出しで作る日数に上限 (既定 31 日) があり、残りは呼び直す。**回 (session) は DB が発行して `mart.sales_daily_state` に覚え、回を開いた時点の対象日を `mart.sales_daily_session_dates` に固定する** = 送り手が時間切れ・異常終了で途中で止まっても、次の呼び出しは同じ回の続きから (一覧の「まだ作っていない日」を古い順に。毎回先頭に戻って後ろの日に永久に届かない、にならない)。
    呼び出しのたびに対象日を取り直さない = 新しい日が入り続けても回は閉じる。全部終わった回でだけ watermark が進み、回が閉じる。回の途中で動いた注文・途中で増えた日は次の回が拾う (前から開いていた回の続きの呼び出しは `resumed = true` を返す。送り手は **その run の最初の呼び出しが `resumed = true` だった (= 前の run が途中で止めた回を引き継いだ) ときだけ**、終えたあともう 1 回ぶん回して追いつく。自分で開いた回は、呼び出しが複数になっても追いつかない = 直前に入れた注文は 15 分のさかのぼりの内側なので、追いつくと同じ日を丸ごともう 1 周作ってしまう)
  - 🚨 **回の目印や時刻を外から渡す口は無い** (受け口は body.session を 400 にする)。未来の時刻を 1 度渡されるとその日が永久に作り直されなくなる、を作らないため
  - 注文日そのものが変わった注文の「前の日」は、ふだんの回では拾えない (実データでは起きていない)。`--refresh-sales --all` は **注文のある日 + 公開中の日** を全部作り直すので、注文が居なくなった日は 0 行で公開し直される
  - 出品・SKU の解決は注文を適用した時点のもの。後から名寄せが進んでも、その注文が送り直されるまで sales_daily には反映されない
- **いつ動くか**: 送り手 `push/mall-orders.mjs` が注文の push のたびに最後に呼ぶ (送る物が無かった朝も呼ぶ = 前の回の取りこぼしを拾う)。新しい定期実行は無い (daily-sync の既存のステップの中)。
  0021 が未適用のあいだは受け口が 409 `not_migrated` を返し、送り手は最後の行に「⏭️ 売上日次は未適用」と出す (注文の push は失敗にしない)。作り直しが失敗・打ち切りなら ❌ (exit 1 = retry の対象)
- **検算**: `mart.sales_daily_check(会社, モール, scope, from, to)` = 公開中の集計と、材料を今そのまま足した値の食い違い (0 行が正常)。比べるのは 明細数・数量・取消の数量・商品代・**取消額・売上・払った額**・金額の分からない明細数 (送料も値引も無い注文の取消は、明細数と商品代だけでは見つからない)。材料の側は按分を通らない式で計算する = 按分の誤りも見つかる。公開の後に注文が動いた日は食い違う = 作り直しがまだ、の印。期間で先に絞る関数にしてある (view だと日付の条件が集計の後ろに残り全期間を走査する)。日の合計での検算なので、粒度 (SKU / shop) の間の配り間違いまでは見ない

```
# 初回 (miniPC の PowerShell。0021 の適用 → モールごとに全部の日を作る → 検算)
cd C:\Users\bfaith\bfaith-portal
node -r dotenv/config scripts\company-db\migrate.mjs                                   # 0021 (applied=1)
node apps\company-db\push\mall-orders.mjs --mall rakuten --refresh-sales                # 楽天 = 約 630 日ぶん (31 日ずつ・約 21 回)。打ち切られたらもう一度流す
node apps\company-db\push\mall-orders.mjs --mall rakuten --check-sales --from 2025-01-01 --to 2025-12-31
node apps\company-db\push\mall-orders.mjs --mall rakuten --check-sales --days 300
(ほかのモールは、注文のバックフィルが終わったあとで同じ 3 行)

# ふだん = 何もしない (注文の push の最後に自動で回る)。全部作り直したいとき
node apps\company-db\push\mall-orders.mjs --mall rakuten --refresh-sales --all
# バックフィルの窓では --no-sales を付けると作り直しを飛ばせる (最後に --refresh-sales を 1 回)
```

試験 = `node scripts/test-company-db-sales-daily.mjs` (17 件: 最大剰余法 (合計がヘッダと一致) / 重みの 3 段 / 取消 / 粒度 (shop_code・出品・SKU・未解決・null と '-' がぶつからない) / 巨大な額でも合計が一致 / 変わった日だけ作り直す・古い run の行は残る / もう公開してある日の 15 分のさかのぼり (20 分前は拾わない) / 上限つきの呼び直しと DB が覚えている回 / 「1 日作って終了」を繰り返しても前へ進む / 新しい日が入り続けても回は閉じる (対象日の固定) / 境界の値 (取消額の厳密な四捨五入・検算が bigint の足し算であふれない) / --all は注文が居なくなった日も消す・purge / 検算が取消の食い違いを見つける / 受け口の引数 (回の目印は渡せない) と戻り値 / 送り手の呼び直し・reset は最初の 1 回だけ・途中で止まっていた回を終えたらもう 1 回ぶん回す / 未適用・打ち切り・進まない応答)。🚨 advisory lock の 2 接続の並行は PGlite では書けない → 本番に適用したあと scripts/test-company-db-concurrency.mjs の系統で確かめる

## 発注の受け皿 (0014。08 §5。D6)

元 = 発注管理アプリの台帳 (`apps/purchase-orders/db.js`。warehouse-mirror.db の `po_orders` / `po_order_items` / `po_item_events` / `po_settings`)。D-9 = a (NE は正本のまま。2026-07-13 以降の発注はこのアプリで行い、注残の正本 = po_* 台帳)。Company DB は**同じ列・同じ規則・同じ式**で持ち (元の SQLite の trigger をそのまま移植)、夜間の loader が mirror から直接読む (取込は次の PR)。

- `core.purchase_order_settings` = 追跡の境界 (元の `po_settings.tracking_started_at`)。**無ければイベントは入らない** (未設定を黙って通さない)。loader が最初に写す。**確定後は変えられない・消せない** (元の boundary_lock。保守経路だけ)
- `core.purchase_orders` = po_orders 1 行。status は draft / issued、閉鎖は `closed_at` (null = オープン。**イベントから導出**)、`po_number` は発行時に採番 (鍵にしない)、**`tracking_mode` ('tracked' = 発行時に固定される業務属性) と境界は別々に持つ** (元: イベント許可 = issued かつ issued_at >= 境界 / 注残の集計 = さらに tracking_mode = 'tracked')、origin = migration (ne_slip_no + send_blocked 必須。ne_slip_no は移行 PO だけ) / supplement (parent 必須・親は issued)、仕入先ごとに draft は 1 件、仕入先はコードで解決 (当たらなければコードだけ残る)
- **発行のゲート** (元の issue_gate): 境界がある会社では issued の直接 INSERT は不可 (正規経路 = draft で作って明細を入れて issued に上げる)。draft → issued は po_number (形式 `PO-YYYY-NNNN`)・issued_at・tracking_mode = tracked・明細 1 つ以上が必要。**ロックの順は 親 PO → 明細** (発行は明細を for update で取ってから数える / 明細の追加は親を for no key update で取ってから状態を見る (🚨 for share だと「2 つの取引が同じ draft に明細を足してから両方発行」で昇格の deadlock) = 発行と明細の追加・変更が並行しても「明細ゼロの issued」「発行後に足した明細」が残らない)
- **発行済みの PO は 発行属性を変えられない・消せない・明細を足せない、発行済みの明細は 数量・商品・単価を変えられない・消せない・別の PO へ移せない (移動元・移動先の両方を見る)** (元の immutable trigger。数量減 = 取消イベント、数量増 = 新規発注)。Render の解決 (sku_id / unresolved_code) と分納の次回予定は変えてよい
- `core.purchase_order_lines` = po_order_items 1 行。qty > 0、`unit_cost` は元の REAL のまま numeric (小数の単価がある。円の整数列にしない)、同じ PO に同じ `product_key` は 1 行、分納の次回予定は組で決まる (awaiting_delivery ⇔ 日付 + 数量 / awaiting_confirmation ⇔ 期限 / null ⇔ 全部 null)
- `events.purchase_order_events` = po_item_events 1 行 (append-only)。4 種 (receipt / shortage / cancel / reversal) と元の CHECK を移植。**対象 = issued かつ境界以後**、通常イベントは閉鎖済みには入らない、**残数超過は trigger で拒む** (ロックは 親 PO → 明細 の順 = 同じ PO の別明細への並行イベントでも閉鎖の再計算が相手の commit を見る)、逆仕訳は一致・1 回だけ。**登録後にヘッダの closed_at を再計算** (全明細の残数 0 で閉じる・逆仕訳で残数が戻れば開く。元の trg_po_events_closure)。closed_at の直接更新は残数と矛盾しない範囲だけ (元の closed_guard)
- **取込 (loader) の経路**: `set local core.po_maintenance = 'on'` で不変・開閉の guard を外して履歴を写し (🚨 PO ごとにヘッダを先に for update で取る (変更が無くても・複数 PO は id 順) → ヘッダ → 明細 → イベントの順。closed_at は最後に元の値を書く。明細やイベントを先に触ると通常経路のイベント登録 (親 → 明細) と逆順で deadlock)、commit の前に `core.assert_purchase_orders_consistent(会社)` を通す (境界がある / 各明細の有効イベント合計 ≤ 発注数 / イベントを持つ PO は issued かつ境界以後 / closed_at ⇔ 残数 0 を全 PO で検査。矛盾があれば例外)。イベントの CHECK・残数超過・対象範囲は保守経路でも外れない
- `mart.v_purchase_order_open` = 元の `v_po_item_balance` と同じ式 (received / shortage / cancelled / cutoff / remaining) + `in_tracking_window` / `mart.v_purchase_backorder_by_sku` = 元の `v_ledger_backorder_by_product` と同じ条件 (issued・tracking_mode = tracked・open・残 > 0・境界以後。product_key で足す)
- 🚨 **移植していない規則** (元台帳側で検証済みのイベントだけを写す、という契約): logizard 入荷 (`po_inbound_items`) の実在・superseded・ignore・商品/仕入先の一致・割当合計 ≤ 入荷良品数 (入荷の表は Company DB に無い。`inbound_ref` は参照として持つだけ)、`po_item_history`、メール送信の遷移
- 🚨 null になり得る列の CHECK は `is not distinct from` で書く (`=` だと null で CHECK が通る。元の SQLite の教訓と同じ)

試験 = `node scripts/test-company-db-purchase.mjs` (PGlite 12 件。境界と不変 / 発行ゲート / ヘッダの規則 (origin 両方向・親は issued・draft 1 件・一意・supplier_name) / 発行済みの不変と保守経路 / 明細の規則 (小数の単価・組の null の罠) / 会社の分離 (SKU と PO を分けて) / イベントの CHECK・残数超過・逆仕訳・対象範囲・append-only / 閉鎖の導出と guard / 整合性検査 / 商品別の注残)。🚨 明細の for update / 親 PO の for no key update の 2 接続の並行 (発行 ⇄ 明細の追加・削除、同じ draft への 2 つの明細追加 → 両方発行、同じ明細への 2 つのイベント) は PGlite では書けない (本番適用時に使い捨てスクリプトで確かめる)

## AI が見張る (Company DB を毎朝読んで「おかしいところ」に気づく。09)

設計の正本 = AI_reference『CompanyDB構想/09_AIが見張る仕組み_設計_20260922.md』(Codex と 3 巡で確定・中原さん決定済み)。**最初は AI なし** (SQL の判定だけ)。

- **どこで動くか**: miniPC の daily-sync の最後の 1 ステップ「Company DB 見張り」(`apps/company-db/watch/run.mjs`)。新しい定期実行は無い。retry には「見張り自身の失敗 (❌)」だけが載る
- 🚨 **記録する回は daily-sync の中だけ**: daily-sync が実行 ID (`DAILY_SYNC_RUN_ID`) を発行 → 送り手が証跡に `sync_run_id` として書く → 見張りは **同じ ID の証跡だけ** を採用する (同じ日の手動実行・別の回の証跡で pass にしない。retry の回も state から同じ ID を引き継ぐ)。記録する回は as_of = 今日 (JST) だけ・会社単位の advisory lock で 1 本だけ (open の案件は部分 unique で二重に作れない)・snapshot を閉じた後に世代を読み直し、変わっていれば再評価 (最大 3 回。変わり続ければ pass を blocked に)。**人が手で流すのは `--dry-run`** (実行 ID が無ければ今日の証跡を「結びつけずに」読む。過去の日は `--as-of YYYY-MM-DD --dry-run`)。送り手を手で流した回の証跡は `<name>.manual.json` (朝の証跡を上書きしない・見張りは見ない)
- **判定は 4 値** `pass / breach / blocked / execution_error` (重さ info / warn / error とは別の軸)。🚨 **「行がある = そろっている」と読まない**: 前提 (完了の印) が無ければ blocked = pass にしない。上流の障害は 1 件にまとめ、依存する項目は「判定保留」と数える
- **証跡**: 送り手 (mall-orders / ne-shipments / stock-daily) が `DATA_DIR/company-db-evidence/<JST の日付>/<name>.json` に「今朝なにをしたか」(run_id・件数・失敗) を書く (`apps/company-db/push/evidence.mjs`。本文は入れない・14 日で消す)。🚨 変更ゼロの朝は chunk を送らないので Render に run が無い = 「走査は完了した・変わった注文は 0」を後から確かめられるのはこれだけ
- **定義はコード** `config/watch-checks.mjs` (既存の `ai.watch_rules` (0006) は使わない = 廃止候補)。結果は `ops.watch_runs` / `watch_results` / `watch_issues` (案件 = 未解決の異常。検知の状態と人の扱いは別の列) / `watch_result_items` (明細の抜粋・上限つき) = 0023
- **最初の 5 項目**: W1 在庫の取込の完了 (期待する source × scope が complete。fba_us は partial を許す例外 = 理由・責任・見直し期限つき) / W2 欠測の履歴 (7 日。案件は日ごと・窓から外れたら「監視期間外」。`since` = 監視の開始日より前は数えない = 在庫日次を作る前・事故で埋まらない履歴を通知しない) / W3 在庫の差の完了 / W7 注文の取込の完了 (証跡 + 送信があれば ops.ingest_runs の同じ run が success かつ complete。🚨 complete は partial でも立つ = status も見る) / W9 売上日次の公開 (session が閉じている・変わった注文を送った朝は watermark が進んでいる・注文のある日は公開されている。注文ゼロの日は 0 件として扱う)
- **2 本目 (9/22)**: W5 解決できない在庫の差 (昨日の `stock_diff_days.unresolved_changed` ≤ 10 件 かつ 数量の割合 ≤ 2% (日次の元から stock-diff と同じ式で計算)。done の日で 3 日続けば info → warn。前提 W3) / W6 売れ筋 SKU の欠品 (直近 28 日に売れた SKU (`v_sales_daily`・取消を引く。セットの出品は `listing_components`・**NE のセット商品の SKU は `sku_components` で構成品 × 数量に展開 = セット自体は在庫を持たないので判定しない**。構成の無いセットは「展開できない販売」。9/24 追加 = 最初の本番の判定でセット 618 件が「在庫 0」の案件になった) で `v_sku_stock` の倉庫 + FBA JP が 0。案件は SKU ごと = 新 (発生) / 継続 / 回復 (解消)。廃番は対象外。2026-10-06 までは info。前提 = W1 の全部 + W7 の全部 (`depends: ['W1:*', 'W7:*']`)。在庫の view が「不明 (complete な日が無い)」なら 0 と読まずに blocked)。残り = W11 → W4 → W12 (W8・W10 は下)
- **2 本目その 2 (9/23)**: W8 注文の日次の異常 (モール × 昨日 の 件数・売上 (`v_sales_daily`)・取消率・金額不明の明細の割合 を、**同じ曜日の過去 8 週のうち「取込の完了が確かめられた日」** の中央値 ± 3×MAD **かつ** 絶対差 (件数 ≥ 20・売上 ≥ 5 万円) で判定。取消率・金額不明率は上に外れたときだけ (中央値 + max(3×MAD, 5pt))。昨日 0 件は平常の中央値 > 0 なら統計に関係なく異常。**小規模モール (平常の中央値 < 30 件/日) は統計を外し、0 件・取消率 30%・金額不明率 50% の上限だけ**。有効標本 < 4 は blocked (保留として通知に出る)。**祝日・年末年始 (`NON_BUSINESS_DAYS`) は標本から外し、昨日がその日なら判定しない** (9/23 追加 = 9/22 のシルバーウィークを平日の火曜と比べた amazon/jp の偽の異常)。前提 = W7・W9 の同じモール。2026-10-07 までは info)
  - 🚨 **標本の完全性** (Codex #1412): 「行が無い = 0 件」「途中で止まった取込の少ない件数」を黙って平常に混ぜない。標本日 D を採用するのは (a) `ORDER_MALLS[].ordersSince ≤ D ≤ reconciledThrough` (バックフィルの突合 `--reconcile --all` = miniPC の raw と Company DB の日ごとの 件数・明細・金額・取消 が全部一致 → `--mark-backfilled` した範囲。**人が突合の後に config に書く**。9/23 時点 = 5 モールとも 2026-09-21 まで) か、(b) その翌朝 (as_of = D+1) の見張りで同じ scope の W7 が pass (`ops.watch_results`。同じ日に 2 回あれば最後の回) のとき。どちらも無い日は `unverified`、注文があるのに売上日次が未公開の日は `unpublished` で除外 (観測値に残る)。除外して有効標本 < 4 なら blocked = 平常が決まらない (見張りが止まっていた期間の後は、証跡がたまるまで保留が出る = 正直な状態。突合し直して `reconciledThrough` を伸ばせば戻る)
    - 証跡にしないもの: 翌々朝の W7 pass (D+1 に失敗した D が直った証明にならない) / `ops.ingest_runs` の success・complete (届いた chunk の処理が済んだだけ = 整形に失敗した注文を飛ばして残りを送っても success。`--from/--to` の手動 push も同じ形)
    - 限界 (受け入れている): (a) は人が書く範囲で、その後の取込の故障で無効にはならない (検算した過去は過去のまま) / (b) は daily-sync の契約 (上流の取得 → push → 見張り) の証跡で、モール API 側の欠落までは証明しない / 公開済みの標本日が「最新の注文まで反映済み」かは見ていない (公開の世代と注文の世代の照合 = 別 PR)
- **2 本目その 3 (9/23)**: W10 回復していない取込の異常 (`ops.ingest_runs` で `W10_SINCE` 以降 (started_at の JST の日) に始まった run のうち failed / partial / 120 分を超えて running のまま、**かつ回復していない**もの。前提なし。2026-10-07 までは info、その後 error)。🚨 **回復は種類ごとに、証明できたときだけ**。後から閉じた run・行の世代が進んだ・今朝の送信が pass だった だけでは回復にしない (Codex D2 / #1417 R1・R2):
  - 注文 5 モール・NE の出荷 (chunk で送る取込) の **partial**: 失敗した行 (`ops.ingest_chunks.result.failed` = 全件。run の `failed_ranges` は 200 件で切れる) の **1 行ずつ**、今の行 (`core.orders` / `core.shipments`) の `received_batch_seq` が run の世代 (`checksum` = batch_seq) より新しく、かつ **信頼できる世代** (daily-sync の差分送信の証跡が持つ `batch_seq` = 今朝の証跡 + 過去の日の証跡 (miniPC に 14 日。8:30 などの自動 retry が送り直した世代も入る) + 最後の記録から引き継いだ `observed.trusted_batches`) であること = 正規の送り手がその行を送り直して当たった。別の送り手 (別の台帳・手動の再投入) が古い内容で世代だけ進めた・その行を走査しない送信が pass した、では回復にしない。行が無い・同じ世代・読めない鍵が 1 つでもあれば残る
  - 同じく **running のまま止まった run・failed**: 送れなかった行は Render から見えない (outbox は鍵だけを追跡に移す・raw から消えた注文は走査されない) = **自動では回復にしない**。`mall-orders.mjs --reconcile` (出荷は `ne-shipments.mjs --reconcile`) で raw と一致を確かめたら `W10_ACCEPTED_RUNS` に書く
  - **一度証明した回復は記録に残す** (`observed.proven`。最後に記録した W10 の結果 = 記録した順 から引き継ぐ。監視の範囲 `W10_SINCE` の外に出ても捨てない) = 翌朝の送信が確かめられない・行の世代が後から変わった、で未回復に戻さない。信頼できる世代の一覧 (`observed.trusted_batches`) も同じく引き継ぐ (まだ証明できていない run の世代より新しいものだけ) = 見張りの記録の整理 (13 か月) に左右されない
  - 前提 (受け入れている): 「daily-sync の回」は証跡の実行 ID (`sync_run_id`) で見分ける。人が同じ `DAILY_SYNC_RUN_ID` を付けて別のデータから `--incremental` を流すと区別できない (復旧作業で env を付けて流さない。手で流す回は ID なし = `.manual.json`)
  - ロジザードの毎時の在庫: 同じか新しい世代 (`checksum` = 取得時刻の ISO) の run が success・complete なら回復 (状態の写し = 新しい世代が入れば古い世代の失敗は残らない。失敗そのものは jobs-monitor の ping がその時に知らせる)
  - 在庫の日次 (ne / fba_jp / fba_us): その run が指す日が今 complete か、例外 (`allowPartial`) の期限の中の partial なら回復。どの日も指していない = partial → complete に上がって新しい run に差し替わった = 回復。**W1 / W2 の窓の中の日は W1 / W2 が見る** (W10 は数えない)・`STOCK_SCOPES` の `since` より前の日は数えない (申告済みの履歴)。W2 の窓から外れても partial のまま、を W10 が拾う
  - 見ない種類 (`W10_DELEGATED`): 夜間ロード (成功したときだけ行を作る。失敗は jobs-monitor と running.json)。どちらにも無い種類の悪い run は `other/*` で異常 = `W10_KINDS` か `W10_DELEGATED` に足す
  - 今朝の push の run (証跡 `orders-<mall>` の run_id) は **W7 が判定したとき (pass / breach) だけ** W7 に任せる (同じ失敗を 2 回出さない)。W7 が blocked (別の実行の証跡・範囲送信・見送り) なら W10 が数える。案件は取込の種類ごとに 1 つ (同じ行が毎日失敗し続けても「新・回復」をくり返さず「継続 N 日」)・未回復の run は明細 (失敗した行の数・当たっていない行の例 3 件)。通知の理由は新しい run から
  - 確かめて受け入れた run は `W10_ACCEPTED_RUNS` に理由と責任を書いて外す (黙って消さない)。run ごと `{ runId }` か、種類 × 時刻より前 `{ kind: 'rakuten.orders/main', startedBefore: '2026-09-22T12:00:00+09:00' }` (突合で一致を確かめた時刻)
- **2 本目その 4 (9/23)**: W11 注文と出荷の未リンク・発送遅れ (モールごと。自社発送 = `core.orders.shop_code` あり = NE を通る注文の、注文日 30 日前〜5 日前。前提 = W7 の同じモール + 今朝の出荷の push (証跡 `shipments` を W7 と同じ判定) + そのモールの結び直しが済んでいる (証跡の `relink` が途中・失敗なら blocked。結び直しを回さなかった朝 = `relink` なし は見る)。2026-10-07 までは info、その後 warn)。中原さん決定 (9/23) = A と B の両方:
  - **A** = モールでは出荷済み (shipped / delivered / returned) なのに NE の伝票が 1 つも結び付いていない (番号の合う伝票が結べていない も A)。全モール。**1 件でも異常**。9/23 本番で直近 35 日に 0 件 (35 日より前に 楽天 53・Qoo10 6・au PAY 1 = 窓の外 = 別途調べる)
  - **A'** = モールでは出荷済みで、結び付いた伝票が **キャンセルだけ**。多くは同梱 (複数の注文 → 1 伝票。現場で「よくある」。NE は同梱元の伝票をキャンセルで残す) だが、同梱でない取消 (取り消して別の番号で作り直した など) と見分ける材料 (NE のキャンセル区分の原文・同梱先の伝票番号) を取っていない (D-30) = 1 件ずつは判定できない → **毎回数えて明細に残し、件数が `W11_CANCELLED_ONLY_MAX` (暫定。9/23 本番の集計 86 日ぶんを評価の窓 26 日に割り戻した 2 倍を切り上げ・最低 3: 楽天 48・Qoo10 27・au PAY 4・Amazon 3・LINE ギフト 3) を超えたら異常** (Codex #1419 R1・R2)。pass は「同梱だと確かめた」ではない
  - **B** = 未発送アラートの無いモール (`W11_UNSHIPPED_MALLS` = Amazon 自社発送・LINE ギフト) で、モールで未発送 かつ NE でも出荷していない (出荷確定日のある有効な伝票が無い) まま、**内容が最後に変わった取込の日** (`source_updated_at`。注文日より後なら) から 5 日、または注文日から 14 日 (`W11_B_MAX_DAYS` = 安全網) = 要確認 (発送遅れの確定ではない。予約・入金待ち・鮮度の分からない状態を含む)。🚨 `source_updated_at` は状態専用の時刻ではない (金額・数量・SKU などの訂正でも進む・同じ内容の再送では動かない・Amazon の last_updated_date だけの変化は送らない) = 訂正で最大 5 日遅れるが、注文日から 14 日で必ず出る。🚨 Amazon の注文レポートは Pending の次が Shipped (Unshipped が出ない) = 自社発送の未発送は `new` のまま → Amazon は new も未発送に数える。LINE ギフトの new は受取人の住所入力待ちなど = 正当な待ち (数えない)
  - **B2** = NE では出荷して 2 日 (`W11_B2_GRACE_DAYS`) たつのにモールが未発送のまま = 出荷の通知 (送り状番号) がモールに届いていない。**Amazon だけ** (LINE ギフトは API に最後に見えた時刻が Company DB に無い = 状態が固定されたか分からない)
  - 楽天・au PAY・Qoo10・Yahoo の発送遅れは既存の未発送アラート (モール側の状態を見る) に任せる (楽天・au PAY はモールの状態を注文日から 7 日しか読み直さない・Qoo10 は取消が API に出ない = Company DB の状態では判定できない)
  - 案件はモールごとに 1 つ・対象の注文は明細 (注文番号・種類 A / B / B2・注文からの日数)。閾値の材料 = `scripts/company-db/w11-survey.mjs` (読み取り専用・件数だけ。同梱の数・未発送の日数を数え直すとき)
- **3 本目 (9/23)**: W4 在庫の純減の異常・W12 DB の容量
  - **W4** = 昨日のロジザードの在庫の差 (`events.inventory_events` の `logizard_diff`・区間 = 前日 → 昨日の最後の毎時の世代) を SKU で足した 減った数 (out)・増えた数 (in)・差し引き (net) を、同じ曜日の過去 8 週 (差を作った done の日で、印の件数とイベントの数が合い、祝日でない日) の中央値 ± 3×MAD **かつ** 500 個以上の差で判定。減った数は多すぎ (大量の減少)・少なすぎ (出荷が在庫に反映されていない疑い) の両方、差し引きは大きく減ったときだけ。前提 W3。2026-11-02 までは info
    - 🚨 理由は分からない: 同じ日の入荷は SKU ごとに出荷と打ち消し合う・棚卸しの調整もふつうの増減として入る・棚移動は 0・良品と B 品は合算・FBA 納品の出庫も入る・区間は暦日ではなく毎時の最後の世代 (18 時ごろ) の間
    - 🚨 在庫の差は 9/20 から (過去は作れない = 元の在庫の写しが残っていない) → 同じ曜日の標本 4 つがそろう **10/19〜10/25 ごろまで毎朝 blocked** (判定保留。正直な状態)
    - 祝日・年末年始 (`NON_BUSINESS_DAYS`。**毎年足して `NON_BUSINESS_DAYS_UNTIL` を延ばす** = 期限を過ぎたら W4 は blocked で足し忘れに気づく。倉庫の休業日と一致するかは現場の確認待ち) は標本から外し、昨日がその日なら判定しない (W8 も同じ一覧を使う)
  - **W12** = 今の DB の大きさ (`pg_database_size`) と、毎晩の締め (`maintainInventory`) が `ops.job_runs` に残す大きさの記録 (9/20 から) の **日ごとの増え分の中央値** から、容量 (10 GB = 06 の Basic-1GB + ストレージ 10GB) まで 90 日を切る・7 GB (D-34) を超えたら異常。中央値なのは、バックフィルのような一度きりの急増 (9/21 → 9/22 に +1.5 GB) に引きずられないため。日ごとの増え分が 5 個に満たない・**最新の記録が 3 日より古い (毎晩の締めが止まった = 古い増え方で「余裕あり」と言わない)** なら残り日数は推計せず blocked (7 GB の判定は記録に関係なく続ける)。記録は正規表現で数字だけ取る (壊れた summary で落ちない)。2026-10-07 までは info
    - 🚨 `pg_database_size` はストレージ全体 (WAL など) ではない = **Render の容量の監視の代わりではない**。今の大きさは読むたびに変わる (MVCC ではない) ので世代の指紋に入れない。プランやディスクを変えたら `W12_DISK_BYTES` も変える
- **通知**: daily-sync の要約に 1 行 (「⚠️ Company DB 見張り 2026-09-23: 異常 1 (新 1 / 継続 0) / 判定保留 2 / 回復 0 / 評価 19/19 — W1 fba_jp/jp: 対象日 … が partial (新)」)。同じ異常は「継続 N 日」、直れば「回復」を 1 回

**始め方 (中原さん・1 回だけ)**: マージ → Render のデプロイ → migrate (0023) → ロールを作る → .env に 2 行 → 確かめる。次の朝から動く。

```powershell
cd C:\Users\bfaith\bfaith-portal
node -r dotenv/config scripts\company-db\migrate.mjs                          # 0023 (applied=1)
node -r dotenv/config scripts\company-db\create-watch-roles.mjs --dry-run     # 流す SQL を見る (パスワードは出ない)
node -r dotenv/config scripts\company-db\create-watch-roles.mjs               # ロール watcher / watch_writer を作る → 表示された 2 行を .env に足す (パスワードはこの画面にしか出ない)
node -r dotenv/config scripts\company-db\create-watch-roles.mjs --verify      # .env の 2 本で接続し、watcher = 読める・書けない / watch_writer = 記録の経路だけ通る を実際の SQL で確かめる (全部 rollback。期待と違えば ❌ で exit 1)
node apps\company-db\watch\run.mjs --dry-run --data-dir C:\Users\bfaith\bfaith-portal\data   # 今日の証跡で評価だけ (記録しない)
```

- 🚨 ロールは SQL で作る (Render の「新しい credential」は default user を差し替えるので使わない) = Render の管理外。Render の default user は superuser ではなく CREATEROLE だけなので、`alter role … nosuperuser` / `nocreatedb` / `nobypassrls` のように **実行者に無い属性は「書くだけで」拒まれる** (PG16+。9/22 に踏んだ) → 書かない。代わりに作った後・commit の前に `pg_roles` で確かめる (superuser / createrole / createdb / bypassrls / replication が無い・login・noinherit・connection limit・ほかのロールのメンバーでない) = 「書いて直す」ではなく「検査して止める」。止まったら人が見る (superuser が付けた属性は default user には外せない)。**パスワードの更新・DB の復元 / 移設のときは create-watch-roles.mjs をもう一度流して .env を更新する**
- `watcher` = 対象 schema (core / snapshots / events / ops / mart) の select だけ + statement_timeout 10s + default_transaction_read_only (保険であって権限の境界ではない)。security definer の関数は public の execute を外す (owner には残る)。`watch_writer` = ops.watch_* の insert + 限定 update + sequence の usage。保持期限の削除は毎時ジョブの整理 (Render の default user) が行う
- 🚨 将来 AI に渡すのは watcher の接続文字列だけ。同じ .env 全体を読める環境では「writer を渡さない」は成立しない (09 §6・§11.3)

見る:

```sql
select as_of_date, planned_keys, completed_keys, summary, last_line from ops.watch_runs order by started_at desc limit 7;
select check_id, scope_key, subject_key, state, severity, days_seen, transitions, handling, summary from ops.watch_issues where state = 'open' order by first_seen_at;
select check_id, scope_key, verdict, reason, observed from ops.watch_results where watch_run_id = (select watch_run_id from ops.watch_runs order by started_at desc limit 1) order by check_id, scope_key;
```

試験 = `node scripts/test-company-db-watch.mjs` (PGlite。5 項目の 4 値・前提で blocked・案件の 新 / 継続 / 回復 / 監視期間外・証跡・保存・期限・dry-run)。🚨 試験に無いもの: ロールの権限 (PGlite では確かめられない → `--verify` で本番)

## バックアップと復元

Render の時点復元 (PITR) は 3〜7 日しかなく、DB を消すと Render 側のバックアップも消える。だから **Render の外 (Google Drive)** に毎晩置く (06 §12 の Codex 条件)。

- **毎晩 03:30 JST**: Render の `render-backup` (台帳 id = `render-backup`) が SQLite 群と一緒に Company DB の論理ダンプを取り、gzip して Google Drive (`bfaith-backup/render`) へ送る。世代 = 日次 14 日 + 月次 13 か月。`COMPANY_DB_URL` が無ければ 🟡 スキップ (失敗にしない)
- **取り戻しは夜間だけ** (2026-09-25 #1457): 定刻を取りこぼした日・失敗した日の再実行は 22:00〜06:00 JST (env `BACKUP_RECOVERY_WINDOW_JST`) の中だけ、前の試行から 6 時間あけて流す。昼に流すと Company DB (0.5 CPU) が張り付きポータルが重くなる
- **Company DB だけ失敗した晩**: 他の対象 (SQLite 群) は最後まで取れて Drive へ送られる。ジョブ全体は失敗として通知し、成功記録を書かないので監視が催促し続ける。その日の前の run で取れた Company DB のダンプは消さずに残す
- **形式**: `pg_dump` は Render にも miniPC にも無いので、Node だけで完結する自前の論理ダンプ (`apps/company-db/backup/dump.mjs`)。中身は `COPY <table> (...) FROM stdin;` + タブ区切りのテキスト。将来 `psql` が使える環境なら そのまま読める形

```
# 手で取る (Render の Shell、または miniPC から External URL で)
node scripts/company-db/backup-cli.mjs dump                      # DATA_DIR/backup-company-db/company-db_<日時>.dump.gz
node scripts/company-db/backup-cli.mjs dump --out /tmp/x.gz

# 中身を確かめる (DB に触らない。壊れていないか・何行入っているか)
node scripts/company-db/backup-cli.mjs verify /tmp/x.gz

# 戻す (🚨 今の中身を消して入れ替える。--yes が無ければ何もしない)
COMPANY_DB_URL=<戻したい DB> node scripts/company-db/backup-cli.mjs restore /tmp/x.gz --yes
```

**復元の約束**:
- 復元先は先に `migrate.mjs` を流しておく (足りなければ止まる)。migrations を流すと参照データ (会社・倉庫・解決規則) が入るので「完全に空」にはならない。だから復元は **入れ替え** (対象の表を消してから入れる)。全部 1 トランザクションで、失敗したら元に戻る
- ID (product_id など) は元のまま戻る (`overriding system value`)。復元後に採番を進めるので、次に作る行が既存の ID とぶつからない
- 生成列 (`code_norm` など) は入れない (復元時に自動で入る)。自己参照 (`parent_product_id`) は全部入ってから埋める
- append-only の表は trigger を外して消し、終わったら戻す (同じトランザクション内)。わざと止めてあった trigger は止まったまま戻る (パーティションの子も 1 つずつ扱う)
- 取ったあと・戻したあとに行数を照合する。合わなければ失敗して巻き戻す
- 検証 (`verify`) も復元も **1 行ずつ** 読む。ダンプ全体を 1 つの文字列にしない (Node の文字列は約 512 MB が上限。2026-09-26 の Company DB は gzip 前で 1.4 GB)。復元はファイルを 2 回読む = 1 回目は検証だけ (おかしければ何も消さずに止まる)、2 回目で流し込む。1 回目と 2 回目で行数が違えば巻き戻す (`RESTORE_SOURCE_CHANGED`)
- 採番 (identity / serial) の記録がダンプと復元先で食い違っていたら、**何も消さずに** 止まる。抜けたまま戻すと次の登録が主キー重複で落ちるため

**復元訓練** (Codex の条件。年 1 回 + DDL を大きく変えたとき):
1. Render で新しい Postgres を作る (名前は `company-db-drill` など。最小プランでよい)
2. その External URL を控える (中原さん。Claude は値を見ない)
3. Drive から最新のダンプを 1 つ落とし、先頭の `-- migrations:` 行を見る (`gzip -dc <file> | head -5`)
4. `COMPANY_DB_URL=<drill の URL> node scripts/company-db/migrate.mjs --to <ダンプの最後の番号>` で **ダンプと同じ版まで** 表を作る
   (🚨 全部当てると復元先のほうが新しくなり、`RESTORE_MIGRATIONS` で拒否される。新しい migration は復元のあとに当てる)
5. `node scripts/company-db/backup-cli.mjs verify <file>` で行数を見る
6. `COMPANY_DB_URL=<drill の URL> node scripts/company-db/backup-cli.mjs restore <file> --yes`
7. 残りの migration を当てる (`migrate.mjs` を番号なしで)。そのあと `/status?counts=1` 相当で件数を本番と見比べる
8. 確認できたら drill の DB を消す。かかった時間と件数を `07_初期ロード_名寄せレポート` に追記する

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
