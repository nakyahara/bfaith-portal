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

### 実行器の全体の排他と concurrent-index の migration (D-60 PR 3a-i)

設計 = AI_reference『CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』§3.10「migrate の runner の契約 (3a-i)」v3.14。試験 = `scripts/test-company-db-migrate-lock-pg.mjs` (使い捨ての PG 18・`npm run test:company-db` に入っている)。

- **全体の排他** = CLI の入口 (`migrateWithLock` → `withMigrateLock`) が、接続の直後に **session の advisory lock** (`hashtextextended('company_db_migrate', 0)`) を try で取り、記録表の bootstrap → 未適用の判定 → DDL → 検証 → `ops.schema_migrations` の記録 までを同じ接続のまま行う (ふつうの migration も)。取れなければ **待たずに** `FAILED (MIGRATE_LOCKED): 別の migrate が動いている … 持っている接続: pid …` で exit 1
  - 失敗の後の順 = **ROLLBACK が終わってから** `pg_advisory_unlock` → `client.end()`。ROLLBACK も失敗した (接続が死んだ) ときは unlock を呼ばずに接続を捨てる (session が切れて外れる)。concurrent-index の文 (取引の外) の失敗は ROLLBACK をせずに unlock
  - 使い回す `applyMigrations()` (PGlite の試験も通す) は lock を取らない。concurrent-index のファイルを本物の PG で流す道は、この session が lock を持っていなければ `MIGRATE_LOCK_REQUIRED` で止まる
  - `--list` は読むだけ = lock を取らない (流している最中でも見られる)。lock を持っている接続の pid と「接続から何分」を出す (45 分を超えていれば ⚠️)。`--dry-run` は try の lock を取り、未適用の判定と許す文・expect.json の検査までして、**DDL を流さずに** (記録表も作らない) unlock
  - この鍵は `company_db_heavy` (取引の lock だけにする共通の鍵) とは別。アプリ・夜間ロードは取らない = 止めない
  - `applied_by` の末尾に runner の版 `migrate-v2` を書く (古い runner で流した行と見分ける)
- **concurrent-index の migration** = 1 行目が `-- migrate:concurrent-index` のファイル
  - 許す文は `create [unique] index concurrently if not exists <名前> on [only] <schema>.<表> [using btree] (…) [include (…)] [where …]` と `drop index concurrently if exists <schema>.<名前>` だけ。名前・schema・表は引用しない名前。ドルの引用・`begin` / `set`・ほかの DDL・ふつうの `create index`・`with (…)` は **その回に流す全部のファイルを流す前に** 拒む (前の番号のふつうの migration も流さない)。印の無いファイルに `concurrently` があっても止まる
  - 1 ファイル = 1 つの表を推す (失敗の範囲を小さく)。同じ index の名前の操作は 1 つの file に 1 つだけ (create を 2 回・drop と create = 止まる・Codex R1 Low)
  - 期待の属性 = 横の `<番号>_<名前>.expect.json` (必須・**手で書かない**・属性に 🆕 置き場 `tablespace` と storage の設定 `reloptions` (fillfactor など) も入る = 既存の index が別の置き場・fillfactor つきなら違う定義として止まる・Codex R1 M2)。使い捨ての PG 18 でそのファイルを流してから `node scripts/company-db/migrate.mjs --url <使い捨ての DB> --index-expect <番号> > db/company/migrations/<番号>_<名前>.expect.json` で作る。中身は `pg_index` / `pg_class` の正規化した属性 (表・access method・unique・key の数・列ごとの列名か式・演算子のクラス・collation・`indoption`・`pg_get_expr` の式と部分 index の条件。`search_path = pg_catalog` で読む = source の空白・大文字に依らない)
  - 本物の PG (pg の Client の adapter = `supportsConcurrentIndex`) = **取引の外で 1 文ずつ**。各文の前 = ① PostgreSQL の major が 18 (expect.json の `pg_major` とも同じ) ② 同じ表の index の作りが別の接続で動いていない (`pg_stat_progress_create_index`・見えない行も「ある」とみなす) ③ 同じ名前があれば: valid で属性が同じなら飛ばす / valid で違えば止まる (人が見る) / invalid なら `drop index concurrently` してから作る / index でない物が名前を取っていれば止まる ④ 容量 (下)
  - session の設定 = `lock_timeout = 5min`・`statement_timeout = 30min`・`client_connection_check_interval = 1s` (Linux の server だけ。Windows の試験の server では使えないと出して続ける)。ファイルが終われば戻す
  - **記録** = 全部の文が通り、作った index が全部 `indisvalid and indisready and indislive` で属性が期待どおり・消した index が無いときだけ (1 文の取引・🆕 `search_path = pg_catalog, pg_temp` に固定 = Codex R5 M2)。途中で落ちたら記録しない = 次に流すと続きから
  - PGlite (試験) など CIC に対応しない adapter = 同じ文から `concurrently` を外した `create index if not exists` / `drop index if exists` を **ふつうの取引で** 流し、属性の検証は同じに通す (試験の schema は本番と同じ index を持つ)。invalid の回収・lock の待ちは試さない (本物の PG の試験で)
  - 🚨 **見張り** = migrate の lock を持つ時間が **45 分** を超えたら知らせる (CIC 1 文の `statement_timeout = 30min` で先に切れるはず = 鳴るのは止まっている印)。今は `--list` の ⚠️ だけ (GChat の見張りは後の PR)。CIC は古いスナップショットを待つ = **夜間のバックアップ (REPEATABLE READ の長い取引) の間は流さない**
- **空き容量** (concurrent-index の create の各文の前・表示だけにしない) = 予想の index の大きさ = `reltuples × (index の列の pg_stats.avg_width の和 + 式の列は 64 + 16) × 1.3` (列は key と include の両方)。**空き (Render のメトリクスの Disk Capacity − Disk Usage = `apps/company-db/profit/render-metrics.mjs`) が `予想 × 3 + この回に先に作った index の予想 × 3 の和 + 2GB` に満たなければ流さずに exit 1** (メトリクスは最大 2 分古い = 続けて作った分がまだ使用に入っていない・Codex R1 H3)。🆕 **この回に流す全部の未適用の CIC の file の create を最初に集めて 1 回で判定する** (最初の CIC の file の最初の文の前・`予想の合計 × 3 + 2GB`・足りなければ 1 本も作らない・前の回の valid・未記録の index も含める・Codex R5 M1 = 設計 13 v3.13 ⑤ (a)。式は設計の `Σ 予想 + max(予想) × 2 + 2GB` 以上 = 止まる向き。CIC の file が 1 つの回は下の file の判定と同じ = 呼ばない。間の ふつうの file が作る表の index は、その表がまだ無い = 見積もれずに止まる = fail-closed)。file の最初の文の前にも、その file の **全部の create の合計** + この回に先に作った分 (前の回が作って記録の前に落ちた valid の index も含める = メトリクスにまだ出ていないかもしれない・Codex R-D60-v3-14 M4) で 1 回判定する (途中まで作って止まらない・残す)。飛ばす作り済みの index も予約に入れる。止まったら少し待ってもう一度流す。🆕 **容量を読む resource は接続先と同じでなければ流さない** (Codex R1 H2・設計 13 v3.13 ④・`DISK_CHECK_FAILED` の `RESOURCE_MISMATCH/<詳しく>`) = ① `CDB_RENDER_PG_RESOURCE_ID` が接続先 (`COMPANY_DB_URL` / `--url`) の host の最初の名前と同じ (内部 = `dpg-xxxx-a`・外部 = `dpg-xxxx-a.<地域>-postgres.render.com`・pool の host は未確認 = 一致にしない) ② Render の API の名札 (`GET /v1/postgres/{ID}` の `databaseName`・password を含まない) = `current_database()`。🚨 **host の対応はまだ Render に確かめていない** (設計 13 §5 の質問 14) = `RENDER_PG_HOST_MAPPING.confirmed = false` の間は **照合を通さない = CIC の migration は流れない** (`RESOURCE_MISMATCH/HOST_MAPPING_UNCONFIRMED`・Codex R-D60-v3-14 M5)。形の fixture = `scripts/fixtures/render-postgres-hosts.json` (回答で決まったら fixture と定数を同じ PR で直す)。password を返す connection-info は使わない。メトリクスが読めない (`RENDER_API_KEY`・`CDB_RENDER_PG_RESOURCE_ID` が無い・古い・形が違う)・表を一度も ANALYZE していない (reltuples が負・列の pg_stats が無い) ときも流さない (fail-closed・`DISK_CHECK_FAILED`)。人が画面で読んだ空きを渡す道は作らない。dry-run は容量を見ない

- **持ち主の mode** (Codex R-D60-v3-10 H2 = PR 1b の前 / PR 1b 自身 / PR 1b の後) = runner は file ごとに catalog だけで mode を読む (接続の役割が ops の USAGE を失っても読める)
  - **legacy** (表 `ops.migrate_owner` が無い = PR 1b の前・今の本番) = 接続の役割のまま流す (今までどおり)
  - **owner-transition** (1 行目が `-- migrate:owner-transition` の file = PR 1b の migration 自身) = 接続の役割で持ち主を移し (🆕 役割を作る・membership を変えるのは migration の外 = 形 B の operator / Render。この file の取引も役割の図の比較 ROLE_GRAPH_CHANGED の対象)、**同じ取引の終わりに** 表 `ops.migrate_owner` ができて、その持ち主と `ops.schema_migrations` の持ち主が同じ・接続の役割がその役割に SET できることを確かめ、記録は `SET LOCAL ROLE <その役割>` で入れる。印を作らなければ巻き戻す。印を作ってよいのはこの file だけ (ほかの file が作れば巻き戻す)。owner mode になった後の owner-transition は流さない。🚨 **owner-transition の file 自身は `ROLE_ADMIN_REACHABLE` の対象の外** (流す時はまだ印が無い = legacy の接続の役割で流れる = 形 B に守られていない)。次の owner の file・次の回・`--list` からは届けば止まる = **R0b-0 / R0b (接続の役割を形 B の deployer に切り替える) までの間に、別の owner の migration を流さない運用が必須** (Codex R4)
  - **owner** (表 `ops.migrate_owner` がある = PR 1b の後) = 持ち主の役割 = その表の持ち主 (catalog の relowner)。ふつうの file は取引の中の `SET LOCAL ROLE` (commit / rollback で戻る)・concurrent-index は `SET ROLE` → 終わりに `RESET ROLE`・記録表の読み書きと `--list` も同じ役割。接続の役割が SET できない・記録表の持ち主と違う = `OWNER_MODE_INVALID` で止まる
  - PR 1b の file の約束 = 表と記録表の持ち主を移す前に、新しい持ち主へ schema の `usage, create` を付ける (無いと `alter table … owner to` が permission denied)・表の持ち主を移してから schema の持ち主を移す・`ops.migrate_owner` を作ってから `ops` の持ち主を移す (試験 O2 の形)
  - 🚨 **印と owner-transition の適用を起動時に両方向で確かめる** (Codex R-D60-v3-11 M-new-2) = owner-transition の migration が適用済みなのに印が無い (誤って消えた) → legacy に戻らずに止まる / 印があるのに owner-transition の migration が未適用・file が無い → 止まる。owner-transition の file は 1 つだけ
  - 🚨 owner の状態の file の **役割の切り替えの字句の検査** (流す前・何も流さずに止まる・設計 13 v3.11 ③) = 拒む = `RESET ROLE`・`SET ROLE` (LOCAL の無い session)・`SET SESSION ROLE`・`SET [LOCAL] ROLE NONE`・`SET / RESET SESSION AUTHORIZATION`・`set_config('role', …)` (1 つ目の引数がただの文字列でない形も)・`DISCARD`・許す一覧の外の `SET LOCAL ROLE`。🆕 引用の名前も小文字にして同じに見る (`RESET "role"`・`SET LOCAL "role" = 'none'`・`"set_config"('role', …)` も拒む・Codex R1 H1)。🆕 **Unicode の escape の名前・文字列 (`U&"…"`・`U&'…'`・`UESCAPE` 句) は中身を読まずに一律に拒む** (大文字小文字・間の空白とコメントも・ドルの引用の中も。`RESET U&"role"`・`SET LOCAL U&"r\006Fle" = 'none'`・`pg_catalog.U&"set_confi\0067"('role', 'none', true)` で検査を迂回できた・Codex R2 High)。decode はしない (escape の文字・サロゲート・encoding を Postgres と同じに再現できなければ fail-open になる = 拒む方が fail-closed・migration は普通の `"…"` で書ける)。legacy (印が無い) の file は ⚠️ を出すだけで今までどおり流す (0001〜0056 に `U&` は無い)。**許す = `SET LOCAL ROLE <一覧>`** (一覧 = `ALLOWED_SET_LOCAL_ROLES` = `cdb_owner`・`profit_definer`・`heavy_guard_definer`・`heavy_read_definer`・`d60_calib_definer`・`finance_revision_definer` = 「以後の更新」の専用の持ち主の関数と設計 19 の F4-2a が file の中で使う)。owner-transition の file は対象の外。試験 O0 が全部の migration の file を縛る。🆕 **ドルの引用の中 (DO の本文・関数の本文・入れ子) も同じ検査で見る** (`do $$ begin perform set_config('role', 'none', true); end $$` も拒む・中を字句に読めない・入れ子が 4 段を超える = 拒む・Codex R-D60-v3-13 H1)。DO と `EXECUTE` (動的 SQL) は拒まない (0001〜0056 の 18 の file が DO・22 の file が `EXECUTE` を使う) = owner の状態の file にあれば runner は ⚠️ を出して流す。🚨 **字句の検査は補助で、sandbox ではない** = `EXECUTE format(…)` の文字列・関数の呼び出しの先は見ない = owner の状態の migration の中で役割を切り替えられないことは runner では保証できない。**役割の管理 (membership の変更・`cdb_role_admin` の操作) は migration に書かず、別の資格・別の session で流す** (設計 13 v3.13)。🆕 **役割を変えられる GUC (`ROLE_GUCS` = `role`・`session_authorization` = PG 18 の guc_tables.c で assign の hook が役割を変えるのはこの 2 つだけ) と字句の前提の GUC (`LEXER_GUCS` = `standard_conforming_strings`・`backslash_quote`・`escape_string_warning`・`client_encoding`) を変える全部の形を拒む** = `SET [LOCAL | SESSION] <GUC>`・`RESET <GUC>`・`set_config('<GUC>', …)`・関数の `SET` 句・`ALTER ROLE / DATABASE / SYSTEM … SET | RESET`・`SET NAMES`・`pg_settings` の参照 (`UPDATE pg_settings` は rule で `set_config` を呼ぶ = 字句の前提の GUC を変えられる)。`SET session_authorization TO …`・`RESET session_authorization` (1 語の名前) で役割を戻せた・Codex R3 High 1。さらに **runner は owner の状態の file の本文の前に別のクエリで `SET LOCAL standard_conforming_strings = on`・`client_encoding = UTF8` を入れ、reset_val も同じかを確かめる** (違えば `LEXER_PREMISE` で流さない。前の file が session に `standard_conforming_strings = off` を残すと、`'…'` の中の `\` で字句と Postgres の文字列の境目がずれた・Codex R3 High 2。concurrent-index の session と PGlite の道も同じ。🆕 Codex R4 High 1 で **取引の中で流す全部の file (legacy・owner-transition・owner)** に広げた = 下の「字句の前提」)。🚨 **守りの分担 (Codex R4 Low で統一)** = ① 字句の検査は **補助** (sandbox ではない・動的 SQL の中は見ない) ② 記録の INSERT の直前の `SET LOCAL ROLE <印の役割>` が守るのは **記録の行だけ** (試験 O6 = INSERT の時の current_user を trigger で読む・本文の副作用 = 役割を変えた後の DDL は守らない) ③ **本当の役割の境は形 B** (下の `ROLE_ADMIN_REACHABLE`・`ROLE_GRAPH_CHANGED`)。runner は記録の INSERT の直前に必ず `SET LOCAL ROLE <印の役割>` を出す (owner-transition の file でも・Codex R-D60-v3-11 M-new-3)
  - 🚨 **owner の状態の migration は sandbox ではない** (設計 13 v3.13 ①) = 記録の INSERT の直前の `SET LOCAL ROLE <印の役割>` が守るのは **記録の行だけ** (本文の副作用は守らない)。本文は DO・関数の呼び出し・動的 SQL・deferred の制約の trigger で、接続の役割 (session_user) が届く役割の全部の権限を使える
  - 🆕 **役割の図の比較 `ROLE_GRAPH_CHANGED`** (設計 13 v3.13 ③ / v3.14 ①・Codex R-D60-v3-14 M1・L1) = owner の状態のふつうの file と **owner-transition の file** の取引で (役割を作る・membership を変えるのは migration の外 = transition の file も役割の図を変えない)、`SET LOCAL ROLE <印の役割>` の直後に (🆕 Codex R5 M2 = `SET LOCAL search_path = pg_catalog, pg_temp` の中で・hash の関数・aggregate・型は全部 `pg_catalog.` で修飾・本文の前に元の search_path へ戻す) 役割の図 (`pg_auth_members` の全部の行と grantor・options / `pg_roles` の属性・connlimit・validuntil・config / `pg_db_role_setting`) の hash (`jsonb_build_object` と順の決まった `jsonb_agg` の SHA-256) を取り → 本文 → `SET LOCAL ROLE <印の役割>` → **記録の INSERT** → `SET CONSTRAINTS ALL IMMEDIATE` (記録の INSERT が起こす deferred の trigger も先に走らせる) → もう一度 hash → 違えば全部巻き戻す (役割の図を変える migration は無い = 例外なし)。🚨 password の変更は見えない (`pg_authid` は superuser だけ)。本文の後は `search_path = pg_catalog, pg_temp` に戻してから runner の SQL を流す
  - 🆕 **到達の検査 `ROLE_ADMIN_REACHABLE`** (設計 13 v3.13 ②・Codex R-D60-v3-14 M2) = owner の状態の時 (`--list` も)、接続の役割が MEMBER で届く役割 (自身を含む) に superuser・CREATEROLE・CREATEDB・REPLICATION・BYPASSRLS・ADMIN の membership の行・危険な定義済みの役割 (`pg_read_server_files`・`pg_write_server_files`・`pg_execute_server_program`・`pg_create_subscription`・`pg_checkpoint`・`pg_signal_backend`) があるか。**owner の状態 (印がある) では必須 = 届けば流さない** (`OWNER_MODE_INVALID` の `ROLE_ADMIN_REACHABLE`・設計 13 v3.14 ② = 形 A は採らない・警告だけで流す道を残さない)。legacy (PR 1b の前・印が無い = 今の本番) は対象の外。🚨 = PR 1b の後に migration を流す資格は **形 B の deployer** (役割の管理・危険な権限に届かない) でなければならない。PGlite の adapter (試験だけの superuser の 1 つの session) は log を出して飛ばす (`roleAdminReachExempt`・🆕 下の `OWNER_ROLE_CAN_LOGIN` も・本番の道の pg の adapter は持たない)
  - 🚨 **形 B が保証するのは「migration から他の役割の membership・属性を管理できない」まで** (Codex R-D60-v3-14 M3・🆕 R5 M3 で password の範囲を直した)。**password は守らない** = ① **SET で届く owner の役割** (印の役割・許す `SET LOCAL ROLE` の一覧の 6 役割・接続の役割が SET で届く役割) の password は、本文がその役割として **見えずに変えられる** (runner 自身が `SET LOCAL ROLE <印の役割>` で本文を流す・PostgreSQL は普通の役割にも自分の password の変更を許す・`pg_authid` は superuser だけ = 役割の図の比較に出ない・字句で `ALTER ROLE … PASSWORD` を拒んでも DO・関数・動的 SQL で迂回できる = 境にならない)。runner は owner の状態 (流す道・`--dry-run`・`--list`) で、これらの役割が **全部 NOLOGIN** であることを fail-closed で確かめる (1 つでも LOGIN なら `OWNER_MODE_INVALID` の `OWNER_ROLE_CAN_LOGIN` で止まる・接続の役割自身は対象の外) = 変えられた password は **資格として使えない** ② **deployer 自身の password** (`ALTER ROLE deployer PASSWORD …`) も残る (権限の昇格ではなく deployer の資格を壊す穴)。**password の厳密な不変が要るなら、migration の runner ではなく operator / Render の側で管理・再設定する** (deployer と `cdb_role_operator` の資格の回復は Render 経由)。「migration から役割を一切変えられない」とは書かない
  - PR 1b の file は、夜のバックアップ (runtime) が読む `ops.schema_migrations` と `ops.migrate_owner` に `grant select … to runtime` (と `ops` の `usage`) を付ける (持ち主を移す前に付ければ移した後も残る・試験 O2)
  - advisory lock は session (backend) のもの = SET ROLE に左右されない。取るのは接続の直後・外す前に `RESET ROLE`
- 🆕 **ふつうの migration と owner-transition の file の最上位 (コメント・文字列・ドルの引用の外) の取引の制御の文は流す前に拒む** (`TX_CONTROL_REJECTED`・Codex R1 M3) = `begin`・`start transaction`・`commit`・`end`・`rollback` (`rollback to savepoint` も)・`abort`・`savepoint`・`release`・`prepare transaction`。runner が 1 file = 1 取引 (owner-transition は R1 + R2 + 印 + 記録が同じ取引) で流す = file の中で取引を切らない。SQL 標準の関数の本文 `begin atomic … end` は数えない。0001〜0056 には無い (試験 M3)。🆕 行コメントの CR だけの改行の後 (`-- c<CR>COMMIT;`)・字句の頭の NBSP / 和文の空白の後 (`select 1 <NBSP>$a$; COMMIT; …`) の `COMMIT` も拒む (Codex R4 High 2・下の表・試験 O16)。DO の本文の中の `COMMIT` は数えない (取引の中では Postgres が `invalid transaction termination` で止める・試験 O16)
- 🆕 **字句の前提** (Codex R3 High 2 / R4 High 1) = runner は **取引の中で流す全部の file (legacy・owner-transition・owner)** の本文の前に、別のクエリで `SET LOCAL standard_conforming_strings = on`・`SET LOCAL client_encoding = 'UTF8'` を入れ、`setting` と `reset_val` が同じかを確かめる (違えば `reason = LEXER_PREMISE` で流さない。🆕 外向きの code (Codex R5 Low 2) = 流す道は file の失敗として **`code = MIGRATION_FAILED`・`reason = LEXER_PREMISE`**・`--dry-run` も同じ確かめを先にする (取引を開いて巻き戻す = 何も変えない・流す道で前提を入れる file があるときだけ) = **`code = LEXER_PREMISE_INVALID`・`reason = LEXER_PREMISE`**。見分けは `reason` で)。Postgres は simple query の本文の全部を、どの文を流すよりも前に字句に分ける = 本文と同じクエリに入れても効かない・前のクエリで入れる。🚨 取引の制御の文の検査も lexSql の読み方に頼る = 前の legacy の file が session に `off` を残すと、owner-transition の本文の `SELECT 'a\'b'; COMMIT; SELECT 'x\'';` の `COMMIT` を見落とし、R1 + R2 + 印の同じ取引を途中で切られた (R4 High 1・試験 O15 = 古い runner では COMMIT の前の本文が残る)
  - 後方の互換 = 0001〜0056 に字句の前提の GUC は 0 件・最上位の `'…'` と `E'…'` の中の `\` も 0 件 (`\` は全部ドルの引用 = 関数の本文の中 = 今の本番 (`on`) と同じ前提で検証される)。今の本番の再実行・`--list` は本文を流さない = 変わらない。`client_encoding` の reset_val は node-postgres が起動の時に必ず `UTF8` を送る (pg-protocol の startup) = 当たらない
  - legacy・owner-transition の file の中で字句の前提の GUC を変える文 = **⚠️ だけで今までどおり流す** (`lexerPremiseChanges`)。拒まない理由 = 次の file の本文の前に runner が前提を入れ直す・file の中の変更はその file の本文の字句の境目を変えない (変えるのは同じ file の後の DO の本文・動的 SQL = legacy では役割の検査をしない)・ALTER ROLE / DATABASE で既定を変えれば次の回は全部 `LEXER_PREMISE` で止まる。owner の状態の file では拒む (今までどおり)
  - session に残った設定は runner が戻さない (legacy の file が `SET standard_conforming_strings = off` を残せば、runner の後の session は `off` のまま = 今までどおり)
- 🆕 **runner の SQL の名前の解決** (Codex R5 M2) = 前の file が session に `SET search_path = attacker, pg_catalog` と同じ名前の関数・aggregate・演算子・型・表 (`jsonb_agg`・`pg_class` の view など) を残しても、runner の検査・記録の SQL の解決先を変えさせない。PostgreSQL は pg_catalog を search_path に明示で後ろに置くと前の schema の同じ名前が組み込みを隠す (型が exact match の関数は pg_catalog の多相の関数より先に選ばれる)。守りは 2 つ = ① runner の SQL は `SET LOCAL search_path = pg_catalog, pg_temp` の取引の中で流す (取引の中の本文の前の SQL は、今の値を覚えて固定し、本文の前に `set_config('search_path', <元の値>, true)` で戻す = **本文の名前の解決は今までどおり**) ② 関数・表・型は SQL の中でも `pg_catalog.` で修飾 (演算子は ① で守る)。棚卸し (試験 O17 = 本物の PG で旧の hash の SQL が本当に騙されることを先に確かめる):

  | runner の SQL | 前の file の後に流れるか | 守り |
  |---|---|---|
  | 持ち主の mode の読み (`readOwnerMode`・file ごと・本文の後・`--list`) | 流れる | ① + ② (`evil.pg_class` の view で印を隠して legacy と誤らせる形を試験 O17) |
  | 役割の図の hash (`roleGraphHash`・本文の前 / 記録の後) | 流れる | ① + ② (`evil.jsonb_agg` が変えた後の図を先回りする形を試験 O17) |
  | 字句の前提の確かめ (`pg_settings`・本文の前・PGlite の道・`--dry-run`) | 流れる | ① + ② |
  | 到達の検査 `ROLE_ADMIN_REACHABLE`・🆕 `OWNER_ROLE_CAN_LOGIN` | 流れる (同じ回の owner-transition の後) | ① + ② |
  | 記録表の読み (`inModeTx`)・記録の INSERT (ふつうの file = 本文の後・CIC の file) | 流れる | ① (表は `ops.` つき) |
  | 容量の見積もり (`pg_class`・`pg_stats`)・`pg_stat_progress_create_index`・lock を持っているか (`pg_locks`)・lock の持ち主の表示 | 流れる (CIC の file) | ① + ② (`evil.pg_stats` の幅 1 を試験 O17) |
  | index の属性 (`readIndexAttrs`) | 流れる | ① (前から) |
  | PostgreSQL の版・lock を取る / 外す (`pg_try_advisory_lock`・`pg_advisory_unlock`) | 外すのは流れる | ② (演算子を使わない・lock は session のもの = 取引に入れない) |
  | 記録表の bootstrap (`create table ops.schema_migrations`) | 流れない (最初の 1 回) | ① (role / database の既定の search_path に依らない) |
  | session の `SET` / `RESET`・`set local role`・`drop index concurrently if exists <schema>.<名前>` | — | 名前を解決しない・schema つき |
  | CIC の文 (`create index concurrently …`) と migration の本文 | 流れる | **固定しない** (本文の意味を変えない・🆕 PGlite の道 (`concurrently` を外して取引の中で流す) も CIC の文の直前に元の session の値へ戻す = 本物の PG と同じ名前の解決・Codex R6 Low・試験 P2)。CIC の式・演算子のクラスが別の schema に解かれたら、属性の検証 (`pg_catalog` で読む `pg_get_expr` と expect.json) が違いとして止める |
- 🆕 `--list` も印と owner-transition の適用を両方向で確かめる (Codex R1 M1) = 食い違えば一覧と lock の持ち主を出した後に `FAILED (OWNER_MODE_INVALID)` で exit 1
- runner が読むのは `db/company/migrations/` の **番号つきの file (`NNNN_名前.sql`) だけ**。番号の無い置き場 `db/company/migrations-pending/`・下のフォルダ・番号の無い file は読まない (試験 L0b・設計 19 v8 の F4-2a)

#### 字句の検査 (lexSql) と PostgreSQL 18 の scan.l の違い = セキュリティの境に関係する差の棚卸し (Codex R4 High 2・🆕 R5 Low 1 で表題を直した)

`src/backend/parser/scan.l` (REL_18_STABLE) のうち、**役割の切り替え・取引の制御 (`COMMIT` など) を隠せるか** に関係する項目を比べた (scan.l の全部の項目の棚卸しではない = 改行を挟む隣接の文字列の連結・演算子の maximal-munch・`$1` の位置の parameter・数の各状態・quote の継続は表に無い。Codex R5 が scan.l と比べて、これらから COMMIT・役割の切り替えを隠す追加の fail-open は無いと確かめた)。「直した」の 2 つは本物の PG 18.4 と PGlite で迂回が本当に効くことを確かめてから直した (試験 O16 の前提)。

| 項目 | scan.l | lexSql | ずれ・扱い |
|---|---|---|---|
| 行コメントの終わり | `non_newline = [^\n\r]` | 🆕 `\r` と `\n` の早い方 | **直した** (前は `\n` だけ = `-- c<CR>COMMIT;`・`RESET ROLE`・`SET session_authorization` を見落とした) |
| 空白 | `space = [ \t\n\r\f\v]` | 🆕 同じ 6 文字だけ | **直した** (前は JS の `\s` = NBSP・U+3000・U+2028・BOM なども空白 = Postgres では識別子の文字 → 字句の頭の `<NBSP>$a$` をドルの引用と誤って `COMMIT` を見落とした) |
| 1 行目の印 | 改行 = `[\n\r]` | 🆕 `[\r\n]` の早い方・行末の空白は `[ \t\f\v]` | 直した (前は `\n` だけ = 印を見落とす = 止まる向き) |
| VT・FF | 空白で改行ではない | 同じ | なし (`-- c<VT>COMMIT` はコメントの続き・試験 O16) |
| ブロックコメント | 入れ子・`/*` で +1・`\*+\/` で -1 | 入れ子・`/*` +1・`*/` -1 | なし (`/*/`・`**/`・`*/*` を追った) |
| 演算子の中の `--`・`/*` | 演算子をその前で切る | 1 文字ずつ見る | なし |
| 識別子の文字 | 始め `[A-Za-z\200-\377_]`・続き `+[0-9$]` (byte) | 始め `[A-Za-z_]` と U+0080 以上・続き `+[0-9$]` | なし (UTF-8 の非 ASCII の byte は全部 `\200-\377`) |
| 識別子の小文字 | ASCII だけ (UTF8) | JS の `toLowerCase` | 多く拒むだけ (Kelvin 記号など・fail-closed) |
| 識別子の 63 byte の切り詰め | 切る | 切らない | なし (禁止の名前は短い = 切っても一致しない) |
| `"…"` | `""` で `"` | 同じ | なし (GUC の名前は大文字小文字を区別しない = 小文字で比べる) |
| `'…'` | `on` = `''` だけ・`off` = `\` も escape | `on` の読み方 | 前提を runner が全部の file に入れる (R4 High 1) |
| `E'…'` | `\` + 1 byte・8 進・16 進・`\u` | `\` + 1 文字を飛ばす | なし (escape の後の文字は引用を閉じない) |
| `B'…'`・`X'…'`・`N'…'` | 中は `[^']*` (`''` は閉じ + 続き) | 名前 + ふつうの文字列 | なし (引用の中の範囲は同じ) |
| `U&'…'`・`U&"…"`・`UESCAPE` | Unicode の escape | owner の状態では中身を読まずに拒む | fail-closed (R2) |
| ドルの引用 | tag = `\$([A-Za-z\200-\377_][A-Za-z\200-\377_0-9]*)?\$`・違う tag は最後の `$` を戻して探し直す | 同じ tag・最初の同じ tag で閉じる (`indexOf`) | なし (閉じの位置は同じ) |
| 識別子の中の `$` | 識別子の続き (`x$a$` は 1 つ) | 同じ | なし |
| 数の後ろの文字 | `1abc` = trailing junk の誤り (PG 15+) | 数として読む | Postgres が誤りで止める |
| NUL・BOM | NUL = 送れない (`invalid message format`)・BOM = 識別子の文字 (構文の誤り) | 字句にする・印の判定だけ BOM を除く | Postgres が止める (本物の PG 18.4 で確かめた) |
| 文の区切り | 文法 | 引用の外の `;` | `begin atomic … end` は別に扱う・`CREATE RULE … ( …; … )` は多く拒むだけ |

#### concurrent-index の migration が途中で止まったとき (回収の手順)

1. `select indexrelid::regclass, indisvalid, indisready from pg_index where not indisvalid;` で invalid を見る
2. `pg_stat_progress_create_index` と `pg_stat_activity` (`application_name = 'company-db-migrate'`) で前の作りが動いていないかを見る。動いていれば終わるのを待つ (止めるなら人が `pg_cancel_backend`)
3. runner をもう一度流す (同じ名前の invalid を `drop index concurrently` してから作り直す)。🚨 手で `DROP INDEX` (CONCURRENTLY なし) はしない (表に強い lock)

#### 配り方 = runner を先にマージして配る (古い runner と並ばない)

- 古い runner (lock なし) と新しい runner が同時に流れると排他は効かない (古いほうが lock を取らない)。ふつうの migration は `ops.schema_migrations` の主キーで片方が巻き戻り、concurrent-index は古い runner では `CREATE INDEX CONCURRENTLY cannot run inside a transaction block` で失敗する = 黙って二重にはならないが、**concurrent-index の migration を足す PR より先に、この runner をマージして、migration を流す所 (miniPC と中原さんの PC) に pull する** (pull した時刻を確かめる)。古い作業の木から流さない
- miniPC (PowerShell 5.1) で配った後に確かめる (読むだけ):

```
cd C:\Users\bfaith\bfaith-portal
git pull
git log -1 --format="%h %ci"
node -r dotenv/config scripts\company-db\migrate.mjs --list
```

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

### Amazon SKU の対の世代の受け口 (Render・⑦-0。16 §7 H1・§8 契約 v3)

Render の `/apps/mirror/api/sync` が Amazon SKU ↔ NE コードの対 (`sku_master` / `sku_resolved`) を**世代つき**で受ける口 (送り手は ⑦-2)。コード = `apps/warehouse-mirror/sku-map-generation.js`・並べ方とハッシュ = `lib/sku-map-canonical.js`。
**今の送り手 (sync-to-render.js) は世代を付けないので、今の動きは変わらない** (PR #1568 で、前の master (c894ca64) と同じ世代なしの body 16 通りを流し、応答と mirror の全部の表が同じことを確かめた。試験は [G1])。
🚨 SKU の対の部も受け口の上限 (12MB・server.js) に入ること。合成の 2 万 SKU / 4 万構成で 13.7MB = 1 回では送れない (⑦-2 の送り手は `assertPartFits` で見る)。

**決まり**
- 世代つきの body = **SKU の対だけの単独の POST**。置いてよい鍵は `sku_master`・`sku_resolved`・`sku_map_generation`・`meta` (オブジェクト) だけ。ほかの表があれば 422 `sku_map_body_not_standalone` (何も書かない)。2 表・状態・同期の印 (last_sync・meta) は 1 つの取引
- **最初に有効にするのは 2 つがそろったときだけ**: Render の env `SKU_MAP_ACTIVATION_ALLOWED=1` と、世代の印の `activate: true`。どちらか無ければ 409 `sku_map_activation_not_allowed` (`missing` に足りない方)。形・ハッシュを確かめた後に見るので、この 409 は「受けられる形だった・何も書いていない」
- 有効になった後 (**戻せない**・状態は `mirror_sku_map_state` の 1 行): 世代なし・古い世代・同じ世代で違うハッシュ = 409 / 空にする・片方だけ・形・ハッシュ・行数違い = 422 / 同じ世代・同じハッシュ = `replayed`。🚨 **今の送り手のマスタの部 (対が入っている) は部ごと 409** (products なども入らない)
- `SKU_MAP_REQUIRE_GENERATION=1` = 状態の行が無くても (表が無くても) 有効とみなし、世代なしの対を断る。**有効は DB ファイルごと** (Render のディスクを戻した・DATA_DIR を変えた = 行が消えて世代なしを黙って受ける) なので、**切替の後に Render に置く**。行が無いときの世代つきは、上の 2 つがそろったときだけ有効にして記録する (下の「バックアップから戻したとき」)
- 初期化 (表・trigger・列) が途中で落ちた = 確かめる口が 503 (capability を出さない)・世代つきも 503。世代なしは今までどおり (有効になった後なら 409 のまま)。再起動で直らなければ 503 の `init_error` を見る
- 状態が読めない (表が無いのとは別の失敗) = 世代なしの対も 503 (有効かどうか分からないまま入れない)。対の無い部は今までどおり
- trigger は名前に版 (`trg_sku_map_state_*_v1`)。`CREATE TRIGGER IF NOT EXISTS` は今ある定義を直さないので、**定義を変えるときは名前の版を上げ、前の名前を `RETIRED_STATE_TRIGGERS` (`apps/warehouse-mirror/sku-map-state-schema.js`) に足す** (新しいのを作った後に DROP)。起動のたびに sqlite_master の定義と照らし、違えば上の「初期化の失敗」

**確かめる口** (`GET /api/sync/sku-map/state`・x-sync-key)。200 = `capability`・`state` (activated・generation は 10 進の文字・content_hash・行数)・`receiver` (今の env)。503 = capability なし (世代つきで送らない)
```
# miniPC の PowerShell (リポジトリ直下。RENDER_MIRROR_URL は末尾に /apps/mirror が付いている)
node -r dotenv/config -e "fetch(process.env.RENDER_MIRROR_URL + '/api/sync/sku-map/state', { headers: { 'x-sync-key': process.env.MIRROR_SYNC_KEY } }).then(async (r) => console.log(r.status, await r.text()))"
```

**受け手が miniPC (SQLite) より厳しいところ** (⑦-2 の影運転で `validateSkuMap` / `skuMapKeyProblem` をそのまま使って数え、切替の前に 0 にする)
- 鍵 (seller_sku・ne_code): 前後に U+0020 以外の空白 (TAB・改行・VT・FF・NBSP U+00A0・全角の空白 U+3000・BOM U+FEFF・U+1680・U+2000〜U+200A・U+2028・U+2029・U+202F・U+205F) / どこかに制御文字 (U+0000〜U+001F・U+007F) / 256 文字以上。miniPC の CHECK (`lower(x) = x AND trim(x) = x`) は SQLite の trim が U+0020 しか削らないので通してしまう (Company DB の `btrim` も同じ)
- 名前: 空白だけ (全角の空白だけなど) / NUL を含む
- 時刻: `YYYY-MM-DDTHH:MM:SS.sssZ` 以外 (miniPC の既定値はこの形。CSV・API で別の形が入った行)
- 構成: 構成が 0 行の親 / sort_order が SKU ごとに 0..N-1 でない (隙間・重なり。CSV の REPLACE で起きうる) / 数量が 1 以上の整数でない
- 同じところ: ASCII の大文字は両方とも断る。全角の大文字・鍵の中の空白は両方とも通す (鍵 = `core.norm_code(鍵)` までは求めない。正規化で重なる鍵は切替前の片付け = 16 §3 #4)

**⑦-2 で本当に有効にするとき**
1. 前提: ⑦-2 の送り手が **SKU の対を別の部 (単独の POST) で送り、マスタの部から対を外した** 版が miniPC に配られていること (でないと有効にした次の朝にマスタの部が 409 = 下の「壊れるもの」)。ほかに (Codex R1 の申し送り):
   - 影運転で上の「厳しいところ」が **0 件** (legacy_reopen の送り手も同じ決まりで断られるので、有効にする条件にする)
   - 対の部が受け口の上限 (12MB) に入る。超える件数なら、送り手で分けるだけでは足りない (1 回の POST が 2 表をそろえて持つ決まり) = 受け口の側に staging → chunk → finalize の契約を作るか、上限を上げる
   - 同じ世代の再送 (replayed) は `synced_at` を進めない = `recent-missing-candidates` (GAS) は 26 時間で止まる。切替の後も GAS を並べて動かすなら、状態の表に「受け取った時刻」を足す
2. Render → Environment に `SKU_MAP_ACTIVATION_ALLOWED=1` (再起動を待つ) → 送り手が `activate: true` で 1 回送る → 応答が `result: activated`
3. `SKU_MAP_ACTIVATION_ALLOWED` を消し、`SKU_MAP_REQUIRE_GENERATION=1` を置く → 確かめる口の `receiver` が `{ activation_allowed: false, require_generation: true }`

**間違えて有効にしてしまったとき** (影運転が本番に `activate: true` で送った・env を消し忘れた など)
- 気づき方: 毎朝の Render 同期が `マスタ: HTTP 409 {"error":"sku_map_generation_required"...}` で失敗する。確かめる口で `state.activated: true`・`activated_at`・`activation_generation` (いつ・どの世代で有効になったか)
- 壊れるもの: マスタの部 (products・set_components・手数料・楽天 SKU・在庫の集計・材料の世代・SKU の対) が**部ごと** 409 = mirror が前の日のまま。送り手は 409 で止まるので**後の部** (出荷サマリ・在庫の明細・月末在庫・月次・日次・商品管理リスト…) も送られない。材料の世代の証跡は `unconfirmed`・夜間ロード・FBA 補充・分析の画面が古い値
- 戻し方 (記録つき。手で SQL を打たない):
  1. Render → Environment から `SKU_MAP_ACTIVATION_ALLOWED` (と `SKU_MAP_REQUIRE_GENERATION`) を消す → 再起動を待つ。間違えて送った送り手を止める
  2. **送り手を止めた後、処理中の要求が無いことを確かめる** (Render のログで `/apps/mirror/api/sync` の最後の要求が終わっている・確かめる口の `state.generation` が 1 分ほど変わらない)
  3. Render の Shell で見るだけ: `node apps/warehouse-mirror/sku-map-state-reset.mjs` (今の状態・控えに書く中身と、次に打つ `--expect-generation` / `--expect-content-hash` が出る。何も書かない)
  4. 全体の控えも取るなら、空きを確かめてから (`df -h $DATA_DIR`・DB の大きさの 2 倍以上の空きがあるときだけ): `node -e "new (require('better-sqlite3'))(process.env.DATA_DIR + '/warehouse-mirror.db', { readonly: true }).backup(process.env.DATA_DIR + '/warehouse-mirror.before-sku-map-reset.db').then(() => console.log('ok'))"`
  5. 戻す: `node apps/warehouse-mirror/sku-map-state-reset.mjs --apply --by "名前" --reason "いつ・どの送り手が・なぜ" --expect-generation <3 で出た世代> --expect-content-hash <3 で出たハッシュ>`
     - 全部 1 つの取引 (BEGIN IMMEDIATE)。**鍵を取った後に状態を読み直し、期待と違えば何もしないで断る** (`RESET_STATE_CHANGED` = 見た後に送り手が新しい世代を入れた・誰かが先に戻した → 2 からやり直す)。控えと記録は鍵の中で読んだ状態から作る
     - 控え = `DATA_DIR/sku-map-state-resets/sku-map-state-reset-<時刻>.json` (前の状態の行・表と trigger の定義・2 表の行数とハッシュ。上書きしない)。`status` = `committed` (戻した・`audit_id` つき) / `aborted` (DB で落ちた = 何も戻していない・`error` つき) / `pending` (途中で止まった = 記録の表と照らす)
     - 記録 = 表 `mirror_sku_map_state_resets` (消せない・直せない) に 誰が・いつ・なぜ・控えの場所・前の状態
     - `mirror_sku_map_state` を DROP → 空で作り直す (有効でない)。2 表の中身は触らない (次の世代なしの同期が入れ替える)。env が残っていれば断る
  6. 確かめる: 確かめる口が `activated: false`。miniPC の Render 同期を流し直す (か翌朝) → マスタの部が 200
  7. AI_reference のインシデントのメモに 誰が・いつ・なぜ・控えの場所 を残す

**Render のディスクをバックアップから戻したとき** (有効にした後。状態はバックアップの時点に戻る = 行が無い・古い世代のどちらか)
1. 送り手 (⑦-2 の写し・daily-sync の Render 同期) を止め、処理中の要求が無いことを確かめる。`SKU_MAP_REQUIRE_GENERATION=1` は残す (世代なしの対を断り続ける)
2. 確かめる口で今の状態を見る (`state.activated`・`state.generation`)
3. 次に送る世代を決める: **max(Company DB の世代・miniPC の世代・Render の今の世代) より大きい値** (受け口は戻した後の状態しか知らないので、行が無ければ小さい世代でも受けてしまう。付け直しは ⑦-2 の世代の付け直しと同じ道で)
4. 行が無い (`activated: false`) とき: Render → Environment に `SKU_MAP_ACTIVATION_ALLOWED=1` を一時的に置く (再起動を待つ) → 送り手が 3 の世代で `activate: true` の単独の POST を 1 回 → 応答が `result: activated` → **すぐ `SKU_MAP_ACTIVATION_ALLOWED` を消す** (再起動を待つ)
   古い世代の行がある (`activated: true`) とき: 許しは要らない。3 の世代で送れば `result: applied` (有効の時刻と最初の世代はバックアップのまま)
5. 確かめる口で `state.activated: true`・`state.generation` = 3 の世代・`state.content_hash` = 送った中身・`receiver` = `{ activation_allowed: false, require_generation: true }`
6. 送り手を戻す。試験 = guards の [G11]

- 試験 = `node scripts/test-sku-map-canonical.mjs` (並べ方・ハッシュ・空白と時刻の決まり) / `node scripts/test-sku-map-receiver.mjs` (受け口の契約) / `node scripts/test-sku-map-receiver-guards.mjs` (今のマスタの部・古い DB・初期化の失敗・状態が読めない・相乗り・有効にする許し・REQUIRE_GENERATION・戻し (鍵の後の読み直し・aborted)・時間・バックアップから戻したとき)

### Amazon SKU の対応の編集 (0054・⑦-1。16 §2・§3・§7 v2・§8 契約 v3)

Amazon の seller SKU ↔ NE コード (今の正本 = miniPC の `m_sku_master` + `m_sku_components`) を、切替 (⑥) の後に Company DB で直すための土台。
**今の動きは変わらない** (新しい表は空・足した列は null・夜間ロードは対応の無い出品を今までどおり作る。試験 [1] = 0053 (⑤-2b) までの DB と 0054 までの DB で夜間ロードの結果が同じ)。

- 表 `core.amazon_sku_maps` (1 行 = 1 つの出品の対応・`state` = active / deleted (墓標)・`origin` = legacy (切替の日の移行) / portal (画面))。構成の正本は `core.listing_components` のまま (`updated_at` を足した = 写しの構成の更新時刻)
- 書くのは security definer の関数だけ: `ops.save_amazon_sku_map` (登録・直す・墓標から戻す) / `ops.delete_amazon_sku_map` (墓標にする)。画面 = `/apps/master-edit/amazon/` (lib/amazon-map-write.mjs)。門は ⑤ と同じ (段階 new_open・持ち主表のハッシュ・`MASTER_EDIT_OPEN=1`) + 持ち主 `listing_components.amazon` = company
- 🚨 墓標は消さない: `core.amazon_sku_maps` の DELETE / TRUNCATE は trigger がいつも拒む (持ち主も)。復元 (`apps/company-db/backup/dump.mjs`) はユーザーの trigger を止めて入れ直すので通る
- 🚨 **残る危うさ (Codex #1586 R1 High・⑥ の go / no-go「夜間ロードのロールを分ける」)**: 夜間ロード・push・migration・復元・ロールの設定は全部同じログイン (`COMPANY_DB_URL` = DB・schema・表の持ち主・CREATEROLE) で動く。
  持ち主は trigger を止められ・schema の持ち主として表を DROP でき・CREATEROLE で作ったロールの一員に自分でなれる (PostgreSQL 18 で試した) = **この表の持ち主だけを別のロールにしても守りにならない**ので、この PR ではしていない。
  持ち主のパスワードが漏れた・持ち主の権限で動くコードの誤り (trigger を止めて消す) なら、墓標は消せてしまう。本当の直し = 夜間ロードと push を、持ち主でなく CREATEROLE の無い別のログインにする (今の全部の書き手に効く = ⑥ で決める)。
  それまでの手当て: **消えた対応** `ops.amazon_map_lost_listings()` (変更の記録 = 追記だけ に対応の行の記録があるのに今の行が無い出品) を、夜間ロードは「対応がある」と同じに扱う (自動の構成を作り直さない・報告の conflicts に `amazon_map_lost`)・切替の段階を company_owner / new_open に進める前提にする (`0054_amazon_map`)。両方の表の trigger を止めて消すまでは、墓標が消えても自動の対応は戻らない
- 表の CHECK は写しの受け手 (`lib/sku-map-canonical.js`) と同じ空白の決まり: seller SKU の前後の TAB・NBSP・全角の空白・ASCII の大文字・制御文字、名前が空白だけ (TAB・NBSP・全角の空白だけも) は、どの書き手でも断る (Codex #1586 R1 M1)
- 不変条件 (commit のとき・deferred の constraint trigger): Amazon (日本) の出品・`listing_norm = core.norm_code(seller_sku)`・active は構成 1 行以上で並び 0..N-1・墓標は構成 0 行。対応の無い出品は見ない
- 構成の書き手: 段階 company_owner / new_open の間、対応のある出品の構成と対応の行は、取引の設定 `core.source_system` が `portal_amazon_map` (画面の関数) か `amazon_map_migration` (切替の日の移行) のときだけ書ける
- 夜間ロード (`apps/company-db/load/engine.mjs`): 対応 (墓標も) のある出品の構成は持ち主によらず作らない。持ち主 company = SKU マスタ・Sheet の構成は材料にしない・FBM の完全一致は対応の無い出品にだけ

**移行 (影運転・切替の日)** = `scripts/company-db/amazon-map-migrate.mjs` (lib/amazon-map-migrate.mjs)。古い表 (warehouse.db・fba.db) は読むだけで開く
```
# 影運転 (T-7 から毎日)。🚨 試し用の DB だけ。本番の URL (COMPANY_DB_URL) が要る = ホスト・ポート・DB 名が同じ (ユーザーは見ない)・つないだ DB の識別が同じか読めない (同じ DB 名) なら断る。1 つの取引で移して照らし、必ず巻き戻す
node -r dotenv/config scripts/company-db/amazon-map-migrate.mjs --shadow --db-url <試し用の DB> --legacy <warehouse.db> --fba-db <fba.db> --json shadow.json
# 古い表のハッシュ (H0) だけ
node -r dotenv/config scripts/company-db/amazon-map-migrate.mjs --legacy-hash --legacy <warehouse.db>
# 切替の日 ③ (段階 frozen の間だけ・止める項目 0・H0 と同じときだけ commit。⑥ の手順書の順番でだけ)
node -r dotenv/config scripts/company-db/amazon-map-migrate.mjs --apply --expect-hash <H0> --legacy <warehouse.db> --fba-db <fba.db> --actor <人のメール> --yes
```
- `--fba-db` は影運転と apply の両方で要る (無い・読めない・`sku_mapping` の表が無い = すぐ断る。Sheet にだけある SKU を 0 件と読まない・Codex #1586 R1 M3)。識別 (system_identifier) が読めない所では、試し用の DB は本番と違う DB 名にする
- 止める項目 (目標は全部 0・16 §5 の 4): key (受け手の鍵の決まり)・name_blank・timestamp・qty・no_components・sort_gap・orphan_component・not_in_company (NE に無いコード)・component_collision / seller_sku_collision (正規化で重なる)・ne_code_differs (Company DB の SKU のコードから作る NE コードが違う)・sheet_only
- 同じ構成の行は時刻 (created_at / updated_at) だけそろえる = 変更の記録・出品の version を増やさない (0049 の印を増やさない)。FBM の完全一致など古い表に無い行は消す
- ⑦-2 (写し・世代・FBA の Sheet 無し・台帳) と ⑥ (段階の戻す道) はこの PR に無い

### 商品の登録日 (0057・2026-10-05 中原さんの要望)
`core.skus.registered_on` (JST の日付) と `registered_on_source` (`ne` = NE の作成日 / `portal` = ポータルで登録した日 / `first_seen` = 夜間ロードが初めて見た日)。両方空か両方あり・2000 年より前は入らない・**一度入った値は変えない** (trigger `trg_skus_registered_on_fixed`。空 → 値 だけ通す)。画面のロール master_edit には列の UPDATE 権限なし。
- **列の既定値は無い** (= 空)。書き手が明示する: 新商品の登録の関数 `ops.register_new_sku` (0057 で作り直し = SKU の INSERT に `v_today`・`portal` を足しただけ) / 夜間ロード (NE の作成日・新しいセットと例外は初めて見た日・作成日の無い単品は空)。古い夜間ロードのプロセスや戻したコードの INSERT (2 つの列を書かない) は空 = 次の晩に NE の作成日で埋まる (portal で確定しない・#1617 Codex R1 Medium)
- 0057 の前にポータルで登録した SKU (⑤-2a の登録の関数が作った = `ops.master_registrations` の `origin = 'new_entry'`・状態は問わない): 0057 が適用の中で先に `portal`・その行を作った日 (`created_at` の JST の日付 = 登録の関数の今日と同じ決め方) で埋める。埋めないと単品は NE に登録した後の夜間ロードで NE の作成日 (= CSV を取り込んだ日) の `ne` で確定し、セットはずっと空になる (#1617 Codex R2 Medium)。変更の記録 (`source_system = migration_0057`) と version が付く = 開いていた画面は 1 度 409。**2026-10-05 の本番 (watcher で数えた) = 0 件** (登録の状態の行 7,377 件は全部 `backfill` = 単品 5,059・セット 2,229・例外 89) = 当てる日までにポータルの登録が無ければ何も変わらない (`backfill` の行は対象の外 = 夜間ロードが NE の作成日で埋める)
- 既にある商品: 適用の後の最初の夜間ロードが、商品管理リストの公開 snapshot の `登録日` (= miniPC の `raw_ne_products.作成日` = NE の `goods_creation_date`) で、空の行だけを埋める (使える snapshot の判定は推奨保有月数と同じ)。report の skus の notes に「登録日: 空だった N 件を NE の作成日で埋めた」。0057 の前からあるセット・例外は空のまま (NE のセットの作成日は取っていない)
- 🆕 (#1624・10/5) **セット**: miniPC の NE の取得が `set_goods_creation_date` を `raw_ne_set_products.作成日` に入れ、商品管理リストの snapshot の `登録日` は「単品の作成日が無いセットだけ、最新の API の完全な取得の世代 (完了の印の通し番号 = 今の raw の通し番号・1 つの読み取りの取引) で、構成の行の作成日が 1 つに決まるとき」に使う (食い違い・空の混在・CSV の取込の後は空)。夜間ロードはそれで空のセットを `ne` で埋める (sources.mjs・lib/sku-registered-on.mjs の説明は「goods_creation_date」のままだが、読んでいるのは snapshot の `登録日` = 同じ道。ロードの指紋を変えないためにコメントは直していない)。既に `first_seen`・`portal` が入ったセットは変わらない (一度入った値は変えない)。
- 当てる順: コードを先に出す (0057 の前でも動く = 画面は登録日を出さずコード順・ロードは見送り) → `migrate.mjs --dry-run` で 0057 だけ → 本適用 → その晩の夜間ロードで単品 (本番 約 5,059 件) が埋まる
- 🚨 最初の埋めで、埋めた単品の version が 1 回進む (変更の記録も 1 回 約 2 列 × 5,000 行)。その晩に開いていた **単品の画面** と、**その単品を構成品に含むセットの画面 (構成の依頼も同じ画面の印)** の保存は 1 度 409 (開き直し)。セットの 409 の「その間の変更」はセット自身の記録だけ = 構成品の登録日は並ばない (中身は変わっていない・開き直せば保存できる)。翌晩からは空の行が無いので増えない (試験 = test-master-edit.mjs [6]・test-sku-registered-on.mjs)

### マスタの照合 ①ロードの検証 (0030・W13。10 §6.1.1 B)

毎朝 daily-sync の「マスタ照合」(`apps/company-db/master-compare/run.mjs --daily`・見張りの前) が、最新の夜間ロード (Render・02:00) を検証する。

- **材料**: その回の ops.load_materials (products・set_components とも matched) の世代の控え (miniPC の DATA_DIR/cdb-material) を、mirror と同じ型の一時の SQLite (`apps/warehouse-mirror/material-tables.js`) に戻し、ロードの読んだ中身と同じハッシュになるのを確かめてから `buildPlanFromRender` (now = ロードの時刻)
- **期待値**: ロードと同じ規則 (`engine.mjs` の `skuValuesForLoad`・`SKU_OWNED_COLUMNS`・`costForLoad`) を、**ロードした回の持ち主・条件** (ops.load_materials の ownership・load_conditions.has0027) と **ロードの判断** (0030 の ops.load_decisions) で当てる
  - 0030 = 夜間ロードが取引の中で書く (section = skus / sku_costs / set_components / primary_suppliers・理由コード・60 日)。構成は書こうとした行と manual で数量が同じだった行・削除まで行った親、代表の仕入先は確かめた後の対象全体
- **比べるもの**: SKU が無い / 値 (load の列) / 有効な原価 (金額・source・status) / 代表の仕入先 (1 件だけ・期待の仕入先) / セット構成 (書こうとした行は片方向・削除まで行った親は source が manual 以外の余分も)。差の明細に events.master_change_events の**変更の候補** (ロードの始まり以降) を付ける
- **判定できない (blocked)**: 夜間ロード (host = render-nightly・success・complete) が無い・今日 (JST) でない / 0029・0030 が無い / 材料が matched でない / 規則の指紋がこのコードと違う (LOAD_RULE_FILES に config/master-ownership.mjs も入る = 持ち主の設定を変えた日も) / 判断の記録が無い・形が違う / 控えが無い・壊れている・戻した中身がロードの読んだ中身と違う
- **出すもの**: 全件 JSON = DATA_DIR/cdb-master-compare/<日付>/<compare_run_id>.json (不変・35 日。比べた SKU の一覧と対象外の理由も) / 証跡 master-compare (始めに running で前の結果を無効に → complete で JSON の場所・sha256・件数・判定。書けなければ exit 1)
- **見張りの W13** (config/watch-checks.mjs。評価キー 2 = load (①) と ne (②。下の「②外との照合」)・severity info・depends なし・案件 = SKU × 問題の種類 = `<種類>:<code_norm>`): 証跡 (今朝の実行 ID・complete・今日) と全件 JSON (sha256・ID・判定・件数・ロード) を確かめて全案件を渡す。開いている案件が今回の明細に無いとき、その種類 × SKU を比べていれば回復・比べていなければ「監視期間外」(回復にしない)
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
- **見張りの W13:ne** (C2b。CHECKS_VERSION v12・評価キー 46): 全件 JSON の ne 節の items を案件 (`<種類>:<code_norm>`・info) に。② が判定できない・落ちた・節が無い・証跡と食い違う = blocked (案件は全部保持)。**明示の回復** = 明細に無い open の案件は ne.recoverable にあるものだけ回復・ne.held にある = 保持 (理由つき)・どちらにも無い = 保持 (not_confirmed)・ne.out_of_scope = 監視期間外 (engine.mjs の reconcileIssues が r.explicitRecovery / r.held / r.recoverable を見る)。① が判定できない朝も ② は判定する。朝の要約では W13:ne の案件を「新・継続」に混ぜず「NE との差 N 件 (新 M)」にまとめる (config の SUMMARY_SEPARATE)。W13:load は mc-v1 / mc-v2 の両方を読む。試験 = `node scripts/test-watch-w13-ne.mjs`
- 試験 = `node scripts/test-master-compare-ne.mjs`

### マスタの照合 ② の判断の台帳 (0032。10 §6.1.1「D1 判断の台帳の契約 v3」)

照合 ② の判断の一覧 (税率の補い・例外原価・NE に値が無い など) を Company DB に残し、人の判断 (差を残す / NE を直す / CDB を直す / 材料を直す / 仕様を決める) を記録する。

- **表**: `ops.master_decision_candidates` (候補。承認の指紋が主キー・指紋の元 print・選べる解決・意味の版は不変・消せない) / `ops.master_decision_observations` (指紋 × 照合の回。入れ直しで二重に数えない) / `ops.master_decision_events` (出来事。**追記だけ**: approved (解決 = その候補の選べる解決の中から (DB が拒む) と、NE / CDB を直すなら目標値) / rejected / revoked / action_done (どの approved の完了か))
- **書く人**: 照合 (miniPC・watch_writer) = `ops.record_decision_candidates(jsonb)`・`ops.record_decision_done(bigint, text, jsonb)` の**実行だけ** (表へ直接は書けない = 承認つきの行を作れない。create-watch-roles.mjs を流し直しても実行権は残る) / 人の判断 = ポータルの画面「⚖️ マスタの判断 (NE との差)」(apps/master-decisions・D2'。承認 / 却下 / 取り消しを出来事に。候補の行を指紋の順に for update → 今の回に出ている・画面が見た回・最新の判断が画面と同じ・選べる解決・直す目標の値の型を確かめる)。Render の env: **MASTER_DECISIONS_ENABLED=1** (載せる。miniPC には付けない) / **MASTER_DECISION_APPROVERS** (決められる人のメール・カンマ区切り。空 = 誰も決められない・admin でも名簿に無ければ不可) / COMPANY_DB_URL。試験 = `node scripts/test-master-decisions-ui.mjs` と test-master-concurrency-pg.mjs の [6][7]
- **照合での使い方**: 列の分類はそのまま、判断の状態 (pending / approved:<解決> / rejected) を重ねる。**非一致の列が全部「差を残す (accept_difference)」の有効な承認で、比べられない・判定できない列が無い案件だけ閉じる** (out_of_scope approved_exception = 見張りの W13:ne は監視期間外)。「直す (fix_ne / fix_cdb)」の承認は、**目標の単位の値が承認した目標値と等しくなったときだけ**照合が action_done を書く (NE と CDB が一致しただけでは完了にしない。関数は、その承認がまだ最新の判断で、まだ完了していないときだけ書く)。完了の後に同じ差が出た = 判断し直し
- **読めない**台帳 = 「承認なし」と読まず ② ごと blocked (decisions_unreadable)。表が無い (0032 の前) = 今までどおり
- **書けない** = 要約の先頭に「⚠️ ②: 判断の台帳を書けない」。全件 JSON の ne.decisions (指紋の元・解決・意味の版) と ne.decisions_observed から `node -r dotenv/config apps/company-db/master-compare/replay-decisions.mjs --from <全件 JSON>` で入れ直す (再計算しない・冪等)
- env: miniPC の COMPANY_DB_WATCH_WRITER_URL (見張りと同じ)。**台帳があるのに無い = 書けないのと同じ = 要約の先頭に ⚠️** (decisions_write = not_configured)
- 完了の観測は、承認の目標 (側・単位・値) と関数の中で照らす (食い違えば拒む)。NE の値なし (空・0)・不正・不明・行が落ちた回・種類の判定を保留した回は完了を確かめない。子を消す目標の値 = `"__absent__"`
- 試験 = `node scripts/test-master-decisions.mjs` (権限は Render と同じ条件の実行者で・本番と同じ「ロールが先・0032 が後」の順も)

### マスタの照合 ② の「最後に一致した値」(0033。10 §6.1.1「D2 最後に一致した値の契約 v2」)

切替 (持ち主を company にする日) の後に、差が「Company DB 側が変わった (NE への反映待ち = to_ne)」「NE 側で変わった (逆流の疑い = ne_changed)」「両方 (conflict)」かを見分ける基準を、**切替の前から**貯める。切替の前は影運転 = ② の分類・verdict・W13:ne・判断の台帳は変えない (方向は全件 JSON の `ne.baseline` と列の `direction` に付けて数えるだけ)。

- **表**: `ops.master_ne_baseline` (単位 = SKU × 列・構成は親ごとの子 × 数量の集合。値・関数が計算した hash・正規化の版・**その値で一致を初めて見た回** (since_run。最後に再確認した回ではない)・その回の NE の印と CDB の読みの時刻・SKU の version (補助の証跡だけ)) / `ops.master_ne_baseline_mark` (1 行 = 最後に受け付けた回 = 札)
- **D2 の意味の一致** (② の分類とは別): 売価・原価の 0・空・null と CDB の 0・null = 同じ「値なし」/ 取扱区分の空 = 'unknown' / 代表の仕入先の空 = [] / 構成は子で並べた集合。NE の値が不明・不正の列は使わない (held)。片側にしか無い SKU は有無だけ
- **書く** (照合 = miniPC・watch_writer は `ops.record_ne_baseline(jsonb)` の実行だけ): 読み取りの取引の最初の文で CDB の読みの時刻を取り、同じ取引で基準と札を読む → 取引の後に**値が変わった・新しい・版が違う単位だけ**を 1 つの取引で送る (5,000 ずつ・変更ゼロでも呼ぶ)。関数は advisory lock → 札を照らす (読んだ札の後に別の回が受け付けられた = mark_moved) → 世代の 5 成分 (単品・セットの印の時刻と番号・CDB の読みの時刻) のどれかが札より古い = stale_run → 単位ごとに読んだ時の hash と照らす (違う = unit_conflict) → 札を進める。拒む = 全部巻き戻る
- **確かめられない** (書かない・方向 = held): ② が blocked / error の回・基準や札が読めない・札より古い観測 (stale_observation)・NE の取得 (単品・セットそれぞれ) と CDB の読みの差が 4 時間を超える回 (gap) / 正規化の衝突・取込の整合・例外の SKU・NE の行が落ちた回の「NE に無い」・**セットの表の行が落ちた回の、単品に見える SKU の種類と値の列** (本当はセットかもしれない。書くと行が戻った朝に前からの差を ne_changed と読み違える) と構成・構成の行が落ちた回の構成・種類が違う SKU の値の列
- **要約の先頭に ⚠️**: 基準を読めない・書けない・拒まれた (その回の方向は全部 held)・書く接続が無い・札より古い観測。4 時間を超える回は ⚠️ にしない (その回は照らさないだけ。要約の ② に「基準は照らさず」と出る = 続くなら daily-sync の順番を見直す)
- 入れ直し (replay) は無い。書けなかった回の一致は推測で補わない (翌朝の照合がまた書く)
- **復旧** (warehouse.db を戻した等で stale_run / stale_observation が続く): 🚨 札だけを下げない (残った基準より古い観測を受け付けてしまう)
  1. 照合を止める (daily-sync のマスタ照合の段を外す)・走っている照合が無いことを確かめる
  2. NE の完全な取得を 1 回やり直す
  3. owner の接続で基準と札を**両方**捨てる (同じ取引・書く関数と同じ lock):
     `begin; select pg_advisory_xact_lock(hashtext('ops.master_ne_baseline')); delete from ops.master_ne_baseline; delete from ops.master_ne_baseline_mark; commit;`
  4. 照合を戻し、次の回で札ができた (`select * from ops.master_ne_baseline_mark`) ことと、方向が unknown から貯まり始めたことを確かめる
- 試験 = `node scripts/test-master-baseline.mjs` (関数: 初回・札・世代の後退・初回の競合・続き・単位の整合・版・入力・権限・分けた送り) / `node scripts/test-master-compare-ne.mjs` の [25] (照合に組み込んだ形)
- **同時実行** (PGlite は 1 接続なので書けない) = 使い捨ての実 PostgreSQL で `TEST_PG_URL=postgres://postgres:pw@localhost:<port>/postgres node scripts/test-master-concurrency-pg.mjs` (新しい DB を作って消す・localhost 以外は拒む・package.json の試験には入れない)。0033 の初回の競合・分けた送りの途中・札を読んだ後の書き込み / 0032 の候補の並行 (デッドロックしない・見た回数)。2026-09-26 に embedded-postgres (PostgreSQL 18) で 5 件 PASS

### 照合の回の記録と関数の直し (0034。Codex #1481 R1 High・#1479 マージ後 Low 2)

- `ops.master_compare_runs` = 判断の台帳に書けた照合の回 (**候補 0 件の回も**)。照合が `ops.record_decision_candidates` を呼ぶと同じ文の中で記録する (入れ直しで二重にしない)。判断の画面 (apps/master-decisions) の「今朝の照合に出ている差か」はこの最後の回で決める (0034 の前は観測の最後 = 差が全部消えた朝が分からなかった)
- 🚨 blocked・台帳に書けなかった回は入らない = 画面の「今朝の照合」は最後に判定して書けた回のまま (画面の上にその日時が出る)
- `ops.record_ne_baseline` = 同じ回 (同じ取引の分けた送りも) で同じ単位を 2 度送ったら unit_conflict (基準の行の touched_txid = その単位を最後に触った取引で見る。「同じ値 → 別の値」の順の重複も拒む。🚨 security definer の関数では一時の表を使わない = 呼び手が同じ名前の一時の表と trigger を先に作ると持ち主の権限で動かされる (Codex #1481 R2 High)。試験 test-master-baseline [14])。ほかは 0033 と同じ
- 2 つの関数は `create or replace` (持ち主・watch_writer の実行権はそのまま)。search_path の最後に pg_temp

### 代表関係 (親子) の帰属と守り (0036。10 §6.1.1「D3 代表関係の契約 v3」)

**帰属**

- `core.products.parent_set_by` は、今の親子を誰が決めたかを表す。
  - 親あり × `load` = 夜間ロードが NE の代表から付けた。付け替え・外してよい。
  - 親あり × `manual` = 人が付けた。
  - 親なし × `manual` = 人が外した。
  - 親あり × `null` = 帰属が不明。
  - 親なし × `null` = 親なし。
- 夜間ロードは `manual` と「親ありで `null`」を触らない。付け替えも外しもしない (保持)。
- backfill は、変更の記録 (0026) で「その子の親の最後の変更が夜間ロード (`company_db_load`) で、値が今の親」と言える行だけ `load` にした。
  - 記録の前 (2026-09-10〜09-24) に付いた親は `null` (保護)。
  - 残りを `load` に移すかは中原さんの判断。移すときは、承認した一覧 (子・親・帰属) を鍵の下で照らしてから、人の名前で行う。

**DB の守り**

- trigger `trg_products_parent_guard_*` は、`parent_product_id` / `parent_set_by` を変える取引に 2 つを求める。
  - ① `set_config('core.parent_protocol', '1', true)`
  - ② `pg_advisory_xact_lock(core.parent_lock_key())`
- どちらかが無ければ `parent_protocol_required` で拒む。
  - 0036 の後に古いコードの夜間ロードが走ると、取引ごと失敗する。黙って保護を上書きしない。
  - 手で親子を直すときも、この 2 つを付ける。
- 🚨 鍵は取引の鍵を使い、**商品の行を更新・ロックする前**に取る。夜間ロードは取引の冒頭で取る。鍵 → 行の順をそろえると、書き手どうしが待ち合わない。
- trigger が確かめるのは「この接続が今、固定の鍵を排他で持っている」ことまで。bigint の形・今の DB・この接続・ExclusiveLock を見る。
- 鍵の数は `core.parent_lock_key()` = 4705310036 (classid 1・objid 410342740)。0036 の中のコメントにある objid 410342739 は、0035 → 0036 に付け直す前の数 (適用済みの migration は書き換えない)。
- 2026-09-27: 0036 を本適用。backfill = load 2 件・帰属不明 2,163 件 → 同じ日に中原さんの判断で 2,163 件を load に再帰属 (変更の記録 = source_system reattribute_d3・actor 中原さん・request_id reattr_4d2a5dc1b1a07ecb。承認した一覧 = AI_reference CompanyDB構想/_raw/d3_再帰属の一覧_20260927.json)。
- バックアップの復元は user trigger を止めて戻すので当たらない。戻した後はまた効く。

**夜間ロード (engine.mjs)**

- 付ける・付け替えるときは、帰属 `load` と一緒に書く。
- 外すのは、帰属 `load` の親を、「外せる材料」が明示のなし (空・自分自身) と言うときだけ。
  - 外せる材料 = matched・完了した NE の取得から・代表の意味の版 `src1`。
- 1 回で外す数が max(20, 帰属 load の親の 2%) を超えたら、1 件も外さない (report の先頭に ⚠️)。
- 循環は、外す辺を明示の null 辺として、最終のグラフで確かめる。
- 判断は `ops.load_decisions` の section `variation_parents` に残す。
  - `targets` = ロードが最終の状態を決めた単品。`held` = 保持した単品と理由・今の親・帰属。
  - 採用した単品は、どちらか一方に必ず 1 回入る。
- 持ち主 `products.parent` が `company` なら、名札も親子も触らない。

**代表の意味の版**

- NE の取得は `raw_ne_products.代表商品コード_src` に元の値を残す。
- 送る形 (readMasterMaterial) の 代表商品コード は次の 3 通り。世代の products に `semantics: { rep: 'src1' }` を付けて送る。
  - `NULL` = 不明。NE の単品に無い・記録なし・null。
  - `''` = NE が空文字を返した。
  - 値 = NE の代表商品コード。
- Render の受け手は、意味の版を `mirror_material_generations.semantics` に残す。
- 版の無い材料 (古い送り手) の `''` は不明と読み、外さない。

**照合 ① と試験**

- 照合 ① は、`load_conditions.has0036 = true` の回だけ代表を比べる。
  - 記録の漏れ・余り・材料の証跡の食い違い = blocked。
  - targets の親の product_id と帰属が今と違えば、種類 `parent` の差。
  - 0036 の前のロードは比べない。blocked にもしない。
- 試験:
  - `node apps/company-db/test-master-parent.mjs` (backfill・守り・表の各マス・外せる材料・外しすぎ・循環・持ち主・0036 の前・送る形)
  - `scripts/test-master-compare.mjs` の [7]〜[9]
  - 実 PostgreSQL の `scripts/test-master-concurrency-pg.mjs` の [8]・[9] (ほかの接続の鍵・鍵 → 行の順)

### 照合 ② の代表 (親子) と最後に一致した値の parent (0037。10 §6.1.1「D3b の契約 v3」)

**照合 ② の列 `parent`** (単品だけ。問題の種類 `parent`)

- n (NE) = 代表商品コード と `代表商品コード_src` から決める。
  - 空でない値: 自分自身なら親なし (null)、他はその norm。
  - 空: 元の値が空の文字列なら親なし、記録が無い・null なら不明 (incomparable)。
  - 親なしは比べられる値。
- c (Company DB) = 親の display_code の norm。親があるのにコードが読めないときは保持 (`cdb_parent_unresolved`)。
- t (材料) = 代表の値 / 自分自身・明示の空は null / 不明は PRESERVE。
  - 控えから材料を作るときは、控えの世代の意味の版を渡す (① = ロードが読んだ世代、② = 今朝の世代)。
- 昨夜の適用 (A) の順:
  1. ① の差があれば load_mismatch。
  2. ロードの保持の記録 (`variation_parents.held`) があれば、記録した親・帰属と今を照らす。
     - 違う = unexplained (`held_state_changed`)。
     - manual = rule (`parent_manual`)。
     - 代表が不明 × PRESERVE = applied。
     - それ以外 = held_by_load (記録の理由)。
  3. 記録が無いときだけ、材料と比べる。
- セット同士は out_of_scope (`set_not_compared`)。種類違いなどは保持。

**判断の候補と画面 (Render)**

- 判断の候補にするのは held_by_load と `parent_manual` (差を残す / NE を直す / CDB を直す)。unexplained は見張り (W13:ne) で扱う。
- 画面の「直す値」:
  - 代表は入力が必須 (提案の値を使わない)。
  - 空・「なし」・null = 親なし。自分自身のコード = 親なし。
- 完了の確かめは単品同士だけ。NE の不明は、目標の「親なし」とも一致させない。

**0037**

- `ops.master_ne_baseline.col` の CHECK に `parent` を足した。
- `record_ne_baseline` は 0034 の関数を写し、`parent` (文字 か null) だけ足した。正規化の版は据え置き。
- 照合は、列の CHECK に `parent` があるときだけ代表の単位を送る (0037 の前の DB には送らない)。

**試験**

- `scripts/test-master-compare-ne.mjs` [26] (lag → 一致・代表がセット・保持の後の変更・manual・不明・自分自身・基準・完了・外した後の保持)
- `scripts/test-master-decisions-ui.mjs` [16] (直す値)
- `scripts/test-master-baseline.mjs` [15] (0037)

### NE に取り込む CSV (0040。10 §6.1.1「③b NE に取り込む CSV の契約 v3」)

判断の画面で **NE を直す** (fix_ne) と承認した差を、NE の一括登録 (商品管理の一括登録) で取り込む CSV にする。画面 = `/apps/master-decisions/csv` (Render だけ・操作は名簿の人だけ)。

**CSV に入れてよい承認** (全部を満たすもの)

- (SKU・列・子) の単位で、どの指紋でも**最後の判断**が fix_ne の承認。後から別の指紋で却下・取り消し・別の承認をした単位は、同じ値の組が戻ってきても入れない (もう一度承認が要る)。
- まだ完了していない (`action_done` が無い)。
- その指紋が**今日 (JST) の照合の回**に出ている。今日の回が無い日は、作る・確かめるができない。
- 列の表にあり、コードと値が CSV に書ける。書けないものは画面の「NE の画面で直す」に並ぶ。
- その単位に有効な予約が無い。

**列の表** (1 つのファイルに NE の列は 1 つ + 商品コード。在庫・予約在庫・入出庫理由・表示・非表示の列は表に無い)

| 列 | 単品 (商品 CSV) | セット (セット商品 CSV) | 書き方 |
|---|---|---|---|
| name | syohin_name | set_syohin_name | 1〜255 文字・改行・制御文字・絵文字・「empty」は不可 |
| handling | toriatukai_kbn | — | active = 0 / discontinued = 1 |
| tax_rate | tax_rate | — | 0.1 = 10 / 0.08 = 8 |
| standard_price_jpy | baika_tnk | set_baika_tnk | 1〜999,999,999 の整数 |
| cost | genka_tnk | — | 同上 |
| primary_supplier | sire_code | — | 4 桁の数字 (9999 もそのまま) |
| parent | daihyo_syohin_code | — | 親のコード / 親なし = empty |

- UTF-8 (BOM なし)・CRLF (最後の行にも)。カンマ・引用符・前後の空白を含むセルだけ引用符で囲む。コード = 半角の英小文字・数字・`-`・`_` で 30 文字まで。
- 構成・セットの税率・有無・種類は CSV にしない (NE の画面で直す)。

**0040 の表**

- `ops.ne_csv_exports` = ファイル 1 つ。配る byte 列と sha256・作った時の今日の回・状態 (made → checked → declared / void)。
  - 確かめ (checked) はその日 (JST) のうちだけ有効。申告は、今日確かめて、その後に新しい照合の回が無いファイルだけ (新しい回があれば確かめ直す)。
  - 作る・確かめるは、候補の行を取った後に照合の回を読む (途中で入った新しい回で判定する)。
  - 申告したファイルをもう一度使える (配る・申告のし直し) のは、申告と同じ日・確かめた後に新しい回が無い・予約が外れた行が無い ときだけ。それ以外は「もう使えない」(ダウンロードは 410・申告は断る。取込の試みの記録は残す)。
  - 全部拒まれた = void。申告したファイルは void にしない。
- `ops.ne_csv_export_rows` = ファイルの行 (出どころ fix_ne / to_ne・承認の出来事・固定した目標・CSV に書いた文字・前に入れたファイルの行)。
  - 有効な予約 (`reserved`) は (SKU・列・子) ごとに 1 つ (部分の一意の索引)。
- `ops.ne_csv_attempts` (取込の試み)・`ops.ne_csv_verified` (実機の確かめ) = 追記だけ。
  - 実機で確かめていない (種類・列・文字コード・見出し・変換の版) の組は「試し用」= 1 ファイル 5 行まで。
- 中身は書き換えない・消さない (trigger)。変えてよいのは状態と予約の列だけ。
- 書くのはポータル (持ち主の役) だけ。watcher は読むだけ。

**鍵の順番** (判断の API も CSV の操作も同じ)

1. CSV の鍵 `pg_advisory_xact_lock(hashtext('ops.ne_csv'))`
2. 候補の行を指紋の順に `for update`
3. 読み直して判定
4. 書く

- 判断の API は、判断を書いた同じ取引で、その単位の予約を外す (`superseded`)。まだ申告していないファイルは void にする。
- 0040 の前の DB では、判断の API は今までどおり動く (鍵も予約も触らない)。

**行の届き方** (判定の順)

1. 確認済み = 承認に `action_done` がある。
2. 取り消し = 後から別の判断をした・ファイルを void にした。
3. 申告したファイルの行は、申告の**次の日 (JST) 以降の最初の照合の回**で見る。
   - その回が無い = 確認できない。
   - 同じ指紋が出ている = 反映されていない。
   - 出ていない = 確かめが要る。
4. それ以外 = 予約中。

- 届き方が決まった行 (確認済み・反映されていない・確かめが要る) の予約は、次に CSV を作るときに外す (作り直しの行は `prev_row_id` で前の行を指す)。

**試験**

- `scripts/test-master-ne-csv.mjs` (18 件・PGlite・HTTP 越し・時計を動かす)
- `scripts/test-master-concurrency-pg.mjs` [11]〜[14] (本物の Postgres)
  - CSV の鍵の順番
  - 照合の完了との取り合い
  - バックアップ → 復元の byte 列の往復 (PGlite は byte 列を文字で受けないので本物で)

### NE のコードの元の書き方 (0041。10 §6.1.1「③b-1b NE の元のコードの契約 v3」)

**なぜ**: NE の商品コードは大文字・小文字を区別する (2026-09-27 に 1,150 / 5,008 件が大文字入り)。私たちの NE の取得はコードを小文字にして保存し、0040 の CSV も小文字で書いていた = 大文字のコードの商品で、NE の一括登録が別の商品の新規登録になるおそれ。

**流れ**

1. **取得** (miniPC・`apps/warehouse/ne-api.js`)
   - 保存 (小文字・上書き) の前に、元の書き方の集合を集める。1 ページ目の `ABC` と 2 ページ目の `abc` も消えない。
   - warehouse.db `raw_ne_code_spellings` に残す (kind = single / rep / set / child / set_rep)。
   - 取得の完了の印と同じ取引で「集め終えた印」`ne_code_spelling_marks` を付ける (0 件でも)。
   - 古い世代は印の新しい 3 つを残す。
2. **照合** (毎朝の ②・`compare-ne.mjs` の `readNeSide`)
   - NE を読む同じ読み取りの取引で、照合に使う取得の世代の書き方を読む。
   - 商品の側・セットの側の**両方**に「集め終えた印」があるときだけ使う。
   - 名前空間は 2 つ:
     - 商品のコード = single・set・child
     - 代表の名札 = rep・set_rep
   - norm ごとに決める:
     - 書き方が 1 つ = ok
     - 2 つ以上 = collided
     - 使えない文字・norm と合わない = invalid
   - 判断の台帳にこの回が書けたときだけ、`ops.record_ne_codes` で Company DB に書く。
3. **Company DB** (0041)
   - `ops.master_ne_codes` (code_norm・kind (product / rep)・state・ne_code・spellings) と印 `ops.master_ne_code_mark` (1 行)。
   - 印は照合の回への外部キーで、時刻は照合の回の表の値を使う。
   - `record_ne_codes` = 1 回 = 1 つの取引で全部を入れ替えて印を進める。
     - 固定の鍵 `hashtext('ops.ne_codes')` で書き手を並べる。
     - (observed_at, compare_run_id) が今の印より新しいときだけ受ける。
     - 同じ回の再送は、中身のハッシュ (行の順に依らない) が同じなら `unchanged`、違えば `run_conflict`。
     - 入力の重複・形の誤りは拒む。
   - security definer・search_path の最後に pg_temp・public の実行権なし。watch_writer は実行だけ・watcher は読むだけ。
4. **CSV** (`apps/master-decisions/ne-csv.mjs`・変換の版 `ne-csv-v2`)
   - `syohin_code` / `set_syohin_code` / 親の名札 (`daihyo_syohin_code`) に**元の書き方**を書く。
   - 使うのは、**印の回 = 最新の照合の回 = 候補の最後に見た回**のときだけ。
   - CSV の鍵の後・候補の行の前に、元のコードの鍵を**共有**で取る (読んでいる間に照合が入れ替えない)。
   - 次のときは「NE の画面で直す」:
     - `ne_code_pending` = まだ記録が無い・今日の回でない
     - `ne_code_unknown` = 取得に無い
     - `ne_code_collided` = 書き方が 2 つ
     - `ne_code_invalid` = 使えない
     - `parent_code_unknown` = 親の名札が決まらない
   - 親の名札の決め方:
     - 今回の名札の書き方があれば、それ。
     - 名札が無い (完全な取得で、どの代表にも無い) ときだけ、親の商品の書き方。
     - それ以外 = 画面で。
   - `ne_csv_export_rows.ne_code` の CHECK = `^[A-Za-z0-9_-]{1,30}$` かつ小文字にして code_norm と同じ。
5. **発注アプリの入荷予定の貼り付け** (Render・`apps/purchase-orders/ne-codes.js`。10 §6.2「M6」契約 v1〜v3)
   - ロジザードに貼る商品ID の書き方: 覚えた書き方 `po_product_code_canonical` (ロジザードの在庫 CSV・NE の CSV の取込で見たもの) が最優先。無いときだけ `ops.master_ne_codes` (kind = product・state = ok・小文字にして鍵と同じ)。それも無ければ今までの予備 (PML → 対応表・PO 明細・仮コード)。
   - 🚨 collided (NE に大文字・小文字だけ違うコードが 2 つ) は、覚えた書き方があっても**貼り付けから外す** (別の商品に入荷するおそれ)。発注書参照の画面では ☑ を押せず、減数の候補にも入れない。
   - 注意 (`caseWarnings`): collided / invalid / 覚えた書き方と Company DB が違う (`ne_api_differs`)。出どころ (`caseSource`) は canonical / ne_api / fallback。
   - 読み方: `COMPANY_DB_URL`・印と書き方を 1 つの読み取りの取引で・全体 3 秒で打ち切って接続を捨てる。読めない・印が無い = 今までの動き (変換は止めない)。印が 7 日より古ければ画面に出す。
   - 試験: `npm run test:po-ne-codes` (読み手 = `scripts/test-po-ne-codes.mjs`・3 つの経路と画面 = `apps/purchase-orders/scripts/smoke.mjs` の「M6」)。

**入れる順番**

1. マージ → miniPC の pull → 0041 の本適用 → Render の反映を確かめる。
2. 翌朝の NE の取得で書き方が集まる → 照合が書く → その回から CSV に入る。
3. それまでは全部「NE の画面で直す」になる (小文字では書かない)。

**試験**

- `scripts/test-ne-src.mjs` [8] (取得で書き方を集める)
- `scripts/test-master-compare-ne.mjs` [28] (照合が書く)
- `scripts/test-master-ne-csv.mjs` [24]・[25] (CSV と関数)
- `scripts/test-master-concurrency-pg.mjs` [16] (鍵の順番・本物の Postgres)

### ロジザード用 CSV の影運転 (③b-2a。10 §6.2「③b-2」契約 v1〜v3)

**なぜ**: 今は GAS がロジザード用の 2 つの CSV (毎日の商品マスタ・新商品) を作っている。切替 (③c) の前に、同じものをこちらでも作れることを、GAS の出力と 1 行ずつ突き合わせて確かめる。**本番のフォルダには何も置かない**。

**合格と言えること** (契約 v3 H1)
- NE の取得の値から、GAS と同じ変換ができる、まで。
- Company DB の値から作る道は ③c で別に決め、その突き合わせの合格を切替の必須条件にする。

**変換の決まり** (`apps/master-decisions/lz-csv.mjs`・版 `lz-v1`。2026-09-24 の GAS の出力 5,008 行で実測)
- Shift_JIS (CP932 の表・iconv-lite 0.6.3)・CRLF・最後の行に改行なし・BOM なし。
- NEC 特殊文字 (`①` `㎏` など) と CP932 に無い文字は `?`。`～` `－` は Windows の対応のまま。
- 引用符 = カンマ・`"`・改行を含む値だけ。中の `"` は `""`。
- 仕入単価 = NE の原価の元の値 `"N.00"` の N。取引先 = NE の仕入先コード (4 桁)。
- 並び = 元のコードの順 (GAS は NE の出力の順。並びだけの違いは許す差 = 中原さん L-3)。
- 新商品の人の 3 列 (有効期限区分・入荷日管理フラグ・バーコード) は空で書き、比べない (L-2)。
- 実測していない形 (IBM 拡張・半角カナ・原価の小数・数字に見える値など) は印を付けて推測で書く。
  - GAS が読んだ入力をこちらの変換に通して、同じ文字・形の印と同じ出力になったときだけ「初めて確かめた形」。
  - 出力が同じだけでは判定できない (GAS の入力がもともと `?` だったかもしれない)。違えば判定できない。

**流し方** (GAS の出力が新しくなった日だけ。Claude が手で = L-1。定期実行ではない)
1. miniPC (読むだけ・リポジトリ直下): `node scripts/company-db/lz-shadow-snapshot.mjs --data-dir C:\Users\bfaith\bfaith-portal\data --out <ファイル>` → PC に持ってくる。
   - miniPC の .env に DATA_DIR は無い = `--data-dir` が要る。
   - 照合がその朝の元のコードを書いた後に流す (印が無い = 全部「作れない」)。
   - NE の取得の完了の世代 1 つ (印・件数・通し番号が今の中身と合うときだけ)。
   - 元のコード = Company DB の `ops.master_ne_codes` と、この世代の書き方から照合と同じ決め方で作った対応が**同じ商品だけ**。違う・無い・衝突 = その商品は作れない (理由つき)。
2. PC: `node scripts/company-db/lz-shadow.mjs --snapshot <ファイル> --lz-list "G:\共有ドライブ\入荷バーコード発行\バーコードマスタ.csv" --gas-input "G:\共有ドライブ\入荷バーコード発行\ネクストエンジンDL【商品データ】\logi_hinban.csv"`
   - GAS の入力 2 つは読むだけ (中原さんの許可 2026-09-28。許可はこの 2 ファイルだけ = 同じフォルダのほかのファイルは読まない)。
   - GAS の出力 (`G:\共有ドライブ\入荷バーコード発行\ロジザードアップロード`) は読むだけ。
   - 記録の置き場所が GAS のフォルダの親 (`入荷バーコード発行`) の中・または GAS のフォルダを含む場所なら止める (リンクも実体で見る)。
   - `--gas-input` = GAS が読んだ NE の品番マスタをこちらの変換に通し、GAS と同じものが作れた差だけを「時刻のずれ」にする (③b-2b)。写しも残す。
   - 🚨 GAS の入力が GAS の毎日の商品マスタより新しい = GAS が読んだものではない = 使わない (logi_hinban.csv = 再現しない / バーコードマスタ.csv = どれが載るかは判定できない)。
   - 時刻が前でも、中身でも確かめる (自己点検): logi_hinban.csv から作った毎日の商品マスタが GAS の出力と全部つじつまが合うときだけ再現に使う。バーコードマスタ.csv は、GAS の入力と組み合わせて作った新商品の集合が GAS の新商品の CSV と同じときだけ使う (GAS の入力が使えなければ使わない)。
   - 分かっている限界: GAS が読んだ後・出力の前に上書きされた入力が、たまたま GAS の出力と全部つじつまが合う場合は、ファイルだけでは見分けられない (GAS に読んだ時点の記録が無い)。
   - 記録 = AI_reference の `CompanyDB構想\_raw\LZ影運転\<実行 ID>\` (写し・こちらの CSV・報告・manifest)。
   - 🚨 **写しの名前は全部 `shadow_<実行 ID>_…`** (GAS は Drive 全体からファイル名で探す = 元の名前の写しを置くと本番の GAS が読むおそれ)。
   - GAS の出力が前の回と同じなら流さない (`--force` で流す)。
3. 合格 (2 つのファイルとも pass) したら:
   - miniPC で完了の ping を 1 回: `powershell -NoProfile -ExecutionPolicy Bypass -File C:\Users\bfaith\bfaith-portal\scripts\jobs-monitor\ping.ps1 -Id lz-shadow-compare -Status ok -Note "<実行 ID>"`
   - 台帳の `lz-shadow-compare` を `RETIRED_JOBS` へ移す PR を作る。
   - 途中の (不合格の) 回では ping を打たない (期限が延びるので)。

**差の分け方と合否** (`apps/master-decisions/lz-compare.mjs`)
- 形 (見出し・改行・BOM・引用符の付き方) / 時刻のずれ (GAS が読んだ入力で再現できた差だけ) / 許す差 (並び) / 判定できない / 説明できない。
- 同じコードの行が 2 つあれば上書きせずに数える。閉じ引用符のあとに普通の文字がある CSV は壊れた CSV (合格にしない)。
- 報告 (`report.json`) には「?」にした文字の位置・元の文字・理由 (NE の側ですでに化けていた / こちらの変換) と推測の印を行ごとに残す。
- 新商品:
  - 比べるのは 5 列だけ (商品ID・商品名・検索名称・仕入単価・取引先コード)。人の 3 列は比べないことを要約と JSON に出す。
  - 「どれが載るか」の合格が言うのは、GAS と同じ一覧 (直近 30 日の書き出し) から同じものが作れた、まで。正しい集合かは ③c で全件の一覧で確かめる (manifest の `new_set.certified = false`)。
  - ロジザードに大文字・小文字だけ違う商品ID がある商品は、新商品にせず作れない行として止める (別の商品として新規登録になるおそれ)。
- 合格 = 説明できない 0・形の差 0・判定できない 0。
- GAS の入力での再現 (③b-2b・`lz-compare.mjs` の `itemsFromLogiHinban`):
  - `logi_hinban.csv` = Shift_JIS・CRLF・全部の値が引用符つき・31 列。使う列 = H 形式/型番・B 商品名・V 取引先id・X 仕入単価 (整数 = そのまま)。見出しが違えば使わない。
  - 2026-09-24 の GAS の入力から GAS の出力 5,008 行が全部バイトまで再現できた (並びだけ違う)。① ㎏ の「?」は NE の書き出しの時点ですでに「?」。
- 🚨 `バーコードマスタ.csv` は GAS の後に auto-barcode.js ② が書き出し直すことが多い (9/24 は GAS 17:52 → 17:55 に書き換わった)。新商品の「どれが載るか」を確かめるには、GAS を押した直後 (② の前) に流す。

**台帳**: `lz-shadow-compare` (human_obligation・P3・期限 = 台帳に載ってから 14 日)。
- 見張りは台帳に載った時から数える (契約の「初回の突き合わせから」とは違う。初回はマージの翌朝の予定 = ほぼ同じ)。途中の回で ping すると延びるので打たない。

**試験**: `scripts/test-lz-shadow.mjs` [1]〜[14] (実ファイルの見出しのバイト・材料は warehouse.db と PGlite・[14] = 見張りの期限の計算)

### ロジザードの毎日の商品マスタ (③c。10 §6.3「③c 契約 v1〜v3」・中原さんの答え L-4〜L-8)

**なぜ**: ロジザードの毎日の商品マスタの取込を、人が押す GAS から Company DB の自動に切り替える。③c-1a (このステップ) は**作って突き合わせるだけで、まだ取り込まない**。

**分け方** (NE の取得の商品 = ③b-2 と同じ集合を 4 つに。`apps/master-decisions/lz-cdb.mjs`)
- **比べる** = ロジザードにある (商品ID が文字の完全一致) かつ Company DB の値が全部そろう。
- **新商品待ち** = ロジザードに無い。ロジザードに無い ID の行は取込でエラーになる (L-7) = 出さない。① の新商品の登録の翌晩から対象。
- **Company DB 待ち** = ロジザードにあるが Company DB にまだ無い新しい商品で、同じ朝の照合 ② が「NE だけにある・lag」(夜間ロードの材料が NE の取込より古い) と言っているもの。翌朝のロードで入る = 出さない・不合格に数えない (2026-09-29 中原さん。9/29 の朝は新商品 23 件で不合格になっていた)。照合 ② が lag と言わない「Company DB に無い」は不正のまま。
- **不正** = 出さない (理由つき。0 や空で埋めない): 元のコードが無い・大文字小文字だけ違う ID がロジザードにある・ロジザードで削除・Company DB に無い・NE の名前が空 (コードで補った名前の疑い)・名前 / 原価 / 仕入先が無いか形が違う (原価は無い・負・整数でない)。

**値の出どころ**: 形式/型番 = NE の元の書き方 (`ops.master_ne_codes`)・商品名 = `core.skus.name` (前後の空白を削った形 = L-4)・仕入単価 = 今の原価 (円の整数。**0 はそのまま出す** = 中原さん L-9 C (2026-09-28)。Company DB の 0 は NE に 0 と入っている値で、原価が決まっていない商品は原価の行が無い = 不正 `cdb_cost_missing`。今の GAS も NE の 0 をそのまま書いている。ロジザードに 0 でない原価があるのに Company DB が 0 の商品は、止めずに報告の `cost_zero_over_lz` と件数 `counts.cost_zero_over_lz` に残す)・取引先 = 代表の仕入先 (1 つ・4 桁)。Company DB は 1 つの読み取りの取引で読む。

**突き合わせ**: Company DB の道と NE の取得の道 (GAS と同じ変換と確かめ済み) を `lz-compare.mjs` で比べ、許す差は 2 つだけ。
- `name_trim` = NE の名前の前後の空白を削ると Company DB の名前 (L-4)。
- `compare_ne` = その朝の照合 ② の全件 JSON に、同じ商品・同じ列・同じ両側の値で載っている差 (例: NE の原価 0 / Company DB の例外の原価)。照合 ② は名前を前後の空白を削った形で持つ = NE の名前も削って比べる。
- NE の取得の道で推測で書いたセル (GAS で確かめていない形 = 原価 `"100"`・仕入先 `"1"`・半角カナなど) は、同じ値でも差でも**判定できない** (`ne_unverified`)。GAS の出力が無いので確かめようがない。
- 合格 = 説明できない差 0・判定できない 0・形の差 0・**不正 0**・作れない行 0・比べる商品が 1 つ以上 (不正が残れば、その商品を中原さんが認めるまで合格にしない)。証跡の `fail_by` に不合格の理由。

**材料の条件** (1 つでも欠ける = 作らない = ⏭️ 理由つき・exit 3 = daily-sync と朝の再試行では失敗)
- その朝の照合の証跡 `master-compare` が complete・全件 JSON の sha256 が合う。
- Company DB の元のコードの印がその照合の回・NE の取得の世代がその朝 (JST)。
- ロジザードの全件の一覧 = miniPC の `C:\tools\logizard-automation\out\shohin_master.csv` (auto-shohin-csv.js の 00:20 の書き出し・全期間・有効 + 無効) が**その日の成功した書き出し**:
  - 成功の印 `C:\tools\logizard-automation\logs\shohin-last-success.txt` (auto-shohin-csv.js が 保存 → Drive への転送 → 印 の順に書く) の中身がその日・印が一覧の後 15 分以内に書かれた (同じ回。2026-09-28 の実測は 45 秒)。日付だけでは、転送で落ちた回や別の書き出し・写しを見分けられない。
  - 読むあいだに変わらない・見出しが実ファイルと同じ・列の数・商品ID が空の行が無い・4,000 行以上・同じ ID が無い・前回 (7 日以内の完了の印。`--out-dir` で出す場所を分けた試しでも本番の DATA_DIR の履歴で見る) の半分以上。

**出すもの** (daily-sync の「マスタ照合」の直後・`scripts/company-db/lz-daily.mjs --daily`)
- `DATA_DIR/lz-daily/<日付>/<実行 ID>/cdb_logizard_shohinmaster_upload.csv` (変えない = 新しく作るだけ) と `report.json` (3 つの分け方・差・「?」にした文字)。
- 証跡 `lz-daily` (完了の印) = 入力の世代 (照合の回・NE の取得・ロジザードの一覧の時刻と sha256・成功の印・前回の行数)・CSV の sha256 と行数・取込の期限 (翌日 01:00 = 翌日 00:20 の取込の回まで)・合否と理由。③c-1b の取込はこの印と CSV が合うときだけ取り込む。
  - 始めに `running` を書く = 同じ日の前の回の完了の印を無効にする。証跡を書けない = 作ること自体の失敗 (❌・前の合格を残さない)。
- **成果物をポータルに送る** (③c-1b-3b-3・契約 K3-1・証跡の版 `lzd-v3`): daily-sync の回は、作った CSV を読み直して (sha256 と大きさが証跡と同じ) ポータルの口 (`POST /apps/logizard-import-state/api/artifacts`・`LZ_LOCK_TOKEN`) に送る。届かない・5xx = 5 秒・10 秒待って 3 回まで / 4xx = すぐ失敗。口の答えの識別 (実行 ID・対象の日・判定・sha256・行数) も照らす。結果は証跡の `portal` (送れない回も)。影の取込・少数件の試験の計画・切替の判定は `portal.ok = true` の回だけ使う (送る前の版 `lzd-v2` は影だけ許す = 経過措置)。
- 終わり方: 作れた (合格でも不合格でも) **かつポータルが受け取れた** = exit 0 (✅ / ⚠️) + 成功の ping (台帳 `lz-daily-build`)。材料が無い・未設定 = ⏭️ exit 3 + fail の ping (理由つき。未設定の回も `skipped` を書いて前の回の完了の印を無効にする)。作ること自体の失敗・**成果物をポータルに送れない (LZ_LOCK_TOKEN が無いも)** = ❌ exit 1 + fail の ping (朝の再試行で作り直して送り直す)。ping と送りは `--daily` で `--out-dir` が無い回だけ。
- 手で試すとき (本番の証跡を書かない): `node scripts/company-db/lz-daily.mjs --data-dir C:\Users\bfaith\bfaith-portal\data --out-dir <一時の場所>`

**2026-09-28 の試し (本番のデータを読むだけ)**: 比べる 5,006・新商品待ち 5・不正 2 (NE の大文字小文字の衝突 = 9/28 に NE で直した)・同じ 4,909 行・許す差 159 (名前の空白 62 商品 × 2 列・原価 35 商品)・説明できない 0。
原価 0 を出さないようにした後 (Codex R1): 比べる 4,932・不正 76 (**原価 0 が 74** = メルカリ訳アリ品 50・オオクワガタのセット 20 ほか。NE・ロジザード・Company DB とも 0 / 衝突 2)・許す差 157・説明できない 0・判定できない 0 (NE の道の推測の形も 0)。
原価 0 をそのまま出すようにした後 (L-9 C): 比べる 5,006 (原価 0 を出す 74・ロジザードに 0 でない原価があるもの 0)・新商品待ち 5・不正 2 (衝突)・許す差 159・説明できない 0・判定できない 0。

**取込 (③c-1b)**: `scripts/logizard-import/lz-daily-import.mjs` (miniPC の 00:20 の定時 `run-nyuka-csv-scheduled.bat` の 1.5 ステップ目)。切替 (下の「毎晩の本番の切替と GAS への戻し」) から `LZ_DAILY_IMPORT=on` = 毎晩の本番。それまで (と戻した後) は下の影 (実行ボタンは押さない)。
- **毎晩の影は `LZ_DAILY_IMPORT_SHADOW=on` (miniPC のリポジトリ直下の .env) のときだけ動く。既定 = 止めてある**。手の道 (Stream Deck の auto-barcode.js) が 00:00〜01:30 に動かない版 (③c-1b-3a) を Stream Deck の PC に写してから on にする (同じ共通アカウントなので、影のログインが手の取込のセッションを切らないように時刻で分ける。専用アカウントは作らない = 中原さん 2026-09-28)。
- 00:15〜00:55 の回だけ・1 日 1 回。対象 = **前の日の lz-daily の正式な証跡 1 つだけ** (daily-sync の回・complete・CSV の sha256 と行数・期限 = 翌日 01:00)。
- ポータルの取込の状態 (`apps/logizard-import-state`) と、この PC の初期化の印 (`DATA_DIR/lz-import/init.json`) を照合。
- ロジザードの商品マスタを取込の直前に書き出し (`DATA_DIR/lz-import/<日付>/<実行 ID>/pre.csv`)・CSV の全部の商品があり・削除されていないか → インポート画面で**プレビューまで**。記録 = 同じフォルダの `shadow.json`・プレビューの画面 `preview.html` / `preview.png`。
- プレビューが「できた」 = ファイルを渡した後に処理が始まった・画面が変わった・入力欄に今回のファイル名・エラーのモーダルが無い (全部そろったときだけ)。サーバーエラー (「エラーが発生しました」) は OK を押さずに止める (画面 `preview-failed.html` を残す。やり直しは人)。
- **毎晩の本番 (③c-1b-2b-2)** = `LZ_DAILY_IMPORT=on` のとき、`lz-daily-import.mjs` が `scripts/logizard-import/lz-nightly.mjs` の `nightlyMain` を呼ぶ (影はしない)。
  - **確かめの列の決まり = RULES_2B2 (2b-2b・9/30 の実機の少数件の試験で決めた)**: ふりがな → 検索名称・仕入単価は文字のまま・取り込んだ商品は変更日時とインポート日時が取込の時刻に変わる (import_stamp = 同じ 14 桁・実在する JST の日時・前より後・**取込の時刻の窓の中** (押した時刻 − 10 分 〜 結果を読んだ時刻 + 10 分・Render の時計。記録 import.json の stamp_window に残し、次の夜の確かめのやり直しも同じ窓。窓の無い記録 = 記録の壊れ) ・登録日時は同じ)・取り込まなかった商品は全部の列が同じ。**毎晩の本番が動くのは切替の PR の後** (ポータルの旗 `LZ_MANUAL_V4=on`・`cutover_phase=cutover`・miniPC の `LZ_DAILY_IMPORT=on`)。それまで on にしても、ポータルの旗が無い = ❌ で取り込まない。
  - 呼ぶ前に見る順: 要対応スペースの送り先 `GCHAT_WEBHOOK_JOBS` (miniPC のリポジトリ直下の .env・無い・https の URL として読めない = ❌) → 決まり → `DATA_DIR`。
  - 時刻の元は **Render の時計** (`/api/status` の `clock.server_now` を単調な時計に写す。miniPC の壁時計は判断に使わない)。
  - どの回も最初に知らせの送り直し: 止め・要確認の outbox → 今の止まった状態の知らせ (窓の中の回は再適用待ちを最後の回に回す = 止めを先に)。予算 = **1 回の起動で共通** 50 件・60 秒 (前後の送り直しとエンジンの知らせで分け合う)。**08:40 / 11:45 (Render の時刻で窓の外) は知らせだけ** (ロジザードに入らない・ping しない)。
  - 00:20 の回 (Render の時刻で JST 00:15〜00:50)。その夜の済みの印 `DATA_DIR/lz-import/<JST の日>/nightly-done.json` は**新しい取込を始める門だけ** (残った importing の回収と前の夜の未確かめの確かめのやり直しは印より先・印がぶつかった = もう動いている = 静かに終わる)。本当の 1 回だけの守りはポータルの nightly_done:
    - ポータルの手の取込の旗 (`manual.v4`) が無い = ❌。止めてある・手の取込が開いている・unknown / partial / verify_failed・試験の回の imported_unverified = 始めない。
    - 止まった状態でも同じ回の鍵が生きている (前の起動が取込の直後の確かめ・確かめのやり直しの途中) = 動いている = 何もしない (止まったと知らせない・確かめない)。
    - importing が残っている: 鍵が生きている = 動いている (何もしない) / 鍵が無い = mark-unknown → どの結末でも読み直して報告して**終わる** (この起動では取込に進まない・ロジザードに入らない)。
    - 前の夜の毎晩の回が未確かめ (imported_unverified) = **その夜は確かめのやり直しだけ** (L-25・記録 = `DATA_DIR/lz-import/runs/<実行 ID>/`)。止まった状態の知らせが知らせ済みになるまでは確かめない (知らせが届かないまま verified になって故障が隠れない)。
    - 同じ対象の日がもう始まった (`nightly_last`) = 何もしない。対象 = 前の日の lz-daily (合格・ポータルに送れた) → ポータルの成果物の識別と同じか → `nightly-readiness` (副作用なし) → 済みの印 → 取込 (エンジン・商品とバーコードの両方を比べる = L-24)。
      - **見張りの商品 (K4)**: 毎晩の CSV はほぼ全商品 = 書き出しの最後の商品 (商品ID の順の最後・2026-10-03 = `zuko5`) が毎晩入り、「比べる商品が最後の商品 = 確かめられない (本物の書き出しは行の間に改行・末尾に改行なし = 最後の商品の 2 本目以降の行の切れ目 (改行の前) で切れても分からない)」に当たる (10/3 00:20 の本番の最初の夜は押す前に止まった・`lzim_night_20261002T152040_dbcb19`)。
        ロジザードにだけ見張りの商品 `LZ_SENTINEL_ID` (`apps/master-decisions/lz-import-verify.mjs` の 1 か所 = `zzzzzzzzzz`・バーコード 1 本・Company DB / NE には登録しない) を置き、毎晩 (`POLICIES.nightly.sentinel`) は直前・直後の商品マスタとバーコードの書き出しの**最後の行が見張り**・見張りは取り込む CSV に無い、を確かめる (直前 = 押さない / 直後・確かめのやり直し = verify_failed)。試験 (`POLICIES.test`) は見張りを使わない (最後の商品を試験から外す = 今までどおり)。
        見張りの商品が無い夜 = 「見張りの商品 … がロジザードの書き出しに無い (見張りの商品の登録が要る)」で押さない (ロジザードに書かない・状態は動かない)。
        見張りの商品の条件: 商品ID = `zzzzzzzzzz`・削除フラグ 0・バーコード 1 本 (今あるどれとも重ならない英数字だけ・8 桁 / 13 桁の数字にしない・JAN / FNSKU (X00…) に似せない。**必ず `LZGUARD0001`** = コードがこの値 1 本だけを求める。違う値・2 本以上は毎晩押さない。入荷検品のバーコードマスタには fnsku として入るが、mirror_products に無いので検索・表示の対象外。Company DB の JAN のロード (barcode_type = jan だけ) の対象外)・商品名は Shift_JIS で戻せる文字だけ (案「【システム用・触らない】取込の見張り」)・在庫 0 (入荷・出荷しない)・NE / Company DB には登録しない・登録や直しは JST 00:00〜01:30 を避ける。
        ほかの仕組み (lz-daily の比べ・lz-shadow・見張り W4 / W13・入荷検品・在庫の写し・Stream Deck の ①②) は、今の「ロジザードにだけある商品」と同じく黙って対象外 (除外の手当ては要らない)。
        毎晩の確かめ (押す前 = 押さない / 押した後・確かめのやり直し = verify_failed): 最後の行が見張り・商品マスタとバーコードの商品ID のかたまりが **CP932 のバイト順で厳密に増えていく** (本物の 10/3 の書き出しで確かめた順。同じ ID の 2 つ目のかたまりも崩れ)・見張りのバーコードが `LZ_SENTINEL_BARCODE` (`LZGUARD0001`・ID と同じ 1 か所) の 1 本だけでほかの商品と重ならない・見張りは取り込む CSV に無い。
        **残るリスク**: ロジザードの商品ID の長さの上限・使える文字・書き出しの並び順の**正式な記述が無い** (リポジトリにも AI_reference にも無い。見張りが最後に来るのは本物の書き出しで確かめたバイト順と、今の ID の文字 (`+ - . 0-9 A-Z _ a-z`・最長 29 文字) からの推定)。`zzzzzzzzzz` で始まるもっと長い ID・`z` より後ろのバイトの文字で始まる ID が登録された・並び順が変わった、は崩れが書き出しに見えれば止まる (安全側) が、「並びの前提が崩れた」と「見張りの直後で切れた」が同じ回に重なると崩れが見えない。そのとき比べる商品が隠れれば precheck (直前) と商品マスタの確かめ (直後) が「無い」で止めるが、比べる商品でない商品の変化は見えない。
  - ping `lz-daily-import` の ok = その夜の取込 (か確かめのやり直し) が verified **かつ** 前後どの回にも未送・知らせ済みにできない止まった状態が無いときだけ。on を見た後の途中の失敗の fail も `lz-daily-import` へ。ほか = ping しない (dead-man が拾う)・途中の例外 = fail。台帳への登録は切替の PR。
  - 本物のロジザードの包みは試験・毎晩・影で共用 (`scripts/logizard-import/lz-real-session.mjs`・1 つの鍵とページの中で 商品 → バーコード → プレビュー → 実行 → 商品 → バーコード・影は押す部品を渡さない)。
  - 確かめのやり直し (試験も毎晩も): 鍵を取ってから記録を読む (無い = evidence_missing・壊れた = evidence_broken・違う回 = evidence_mismatch = どれも verify_failed = 人が見る)・鍵を 30 秒ごとに延ばす・書き出しの前ごと・結果を書く前に締め切り (試験 = 次の 00:00 の前・毎晩 = 00:55) を見る (過ぎた・鍵を失った = 未確かめのまま)。取込の後の直後の書き出しと確かめの結果も同じ。
- ③c-1b-2b の部品 (2b-1a・まだ押す道は無い): `apps/master-decisions/lz-import-check.mjs` = 取り込む CSV の確かめ (見出し・5 列・CRLF・文字が戻る・重複 = 文字でも小文字でも)・結果の文字の読み方 (総件数 = CSV の行数・処理 + 処理不要 = 総件数・エラー 0 だけが成功 / 件数違い・エラー = partial / 無い・2 つ = unknown)・試験の CSV (5 列を独立に・文字を「?」に落とさない) / `lz-import-verify.mjs` = 取込の後の確かめ (取り込んだ商品 = CSV のとおり + 対象外の列が前と同じ・取り込まなかった商品 = 全部の列が前と同じ・増えた / 消えた・決まっていない列 (ふりがなの列・仕入単価の書き方・システムの列) は観察だけ = 本番の合格と数えない)・バーコードの前後。設計 = AI_reference 10 §6.3「③c-1b-2b 契約 v3」。
- ③c-1b-2b の画面の部品 (2b-1b): `tools/logizard-automation/lz-import-screen.js executeImport` (実行 → 決まった文言のモーダルの中の OK だけ・ほかのモーダル / dialog は押さずに止める・押す前に結果の表示があれば押さない・押した後に新しく出た結果だけ返す) / `import-guard.js` (止める旗・締め切り・押す持ち時間・止めたらページを閉じる)。呼ぶのは 2b-1c のランナーの `--test` (中原さんと) だけ。影の取込は呼ばない。
- **少数件の実機の試験 (2b-1c・中原さんと・昼)**: `scripts/logizard-import/lz-import-test.mjs` (miniPC のリポジトリ直下で)
  1. 共通アカウントでロジザードを使う人・作業が止まっていることを中原さんと確かめる (L-16)。00:00〜01:30 は動かない。
  2. `node scripts/logizard-import/lz-import-test.mjs plan --normal <商品ID,…> --occupancy "<確かめたこと>"` = その日の lz-daily の正式な証跡と直前の書き出しから計画 → `DATA_DIR/lz-import-test/<計画 ID>/summary.txt` (取り込む値・今の値・承認の印)。
     失敗の試験 (`--missing N-1:A-1`・`--deleted D-4`・`--case Abc-1:abc-1`) は `--mapping <json>` (ふりがなの列・仕入単価の書き方) が決まってから (K1)。
  3. 中原さんが一覧を見て認めたら `run --plan <計画 ID> --sha256 <承認の印> --occupancy "…"` = 鍵 → 直前の書き出し (商品・バーコード) → 照らし直し → 押す前の記録 → プレビュー → importing → 押す → 結果 → 直後 → 確かめ → verified / verify_failed。記録 = `…/runs/<実行 ID>/`。
  4. 止まった (unknown / partial / verify_failed / imported_unverified) = GChat (要対応スペース `GCHAT_WEBHOOK_JOBS`・**リポジトリ直下の .env に無い・壊れている = `run` / `verify` / `notify` は始めない**)。解除は人 (ロジザードのインポート履歴を確かめてから `import-state-cli.js resolve`・先に解除して戻すはしない = K3)。未確かめ = `verify --run <実行 ID> --occupancy "…"`。知らせの送り直し = `notify` (ポータルが importing のまま鍵が無い回 = 押した後に結果を書けなかった回も知らせる)。00:00 の 1 分前を過ぎたら押さない (始めた後に越えても)。
  5. `run` の終わりに出る「この回の結末」が verified のときだけ終了コード 0 (ポータルの今の状態は別に出す = 前の回の verified と取り違えない)。直後の書き出しの中身が壊れていた (検証に落ちた) = verify_failed・通信やログイン切れ = 未確かめのまま (`verify` でやり直す)。
  6. バーコードの書き出しの部品 (`C:\tools\logizard-automation\barcode-export.js`・miniPC に deploy で写す) が無いうちは `run` / `verify` は断る (K4)。書き出すのは SKU のバーコード情報の全件 (登録日で開始日なし・有効 + 無効)。単独で確かめる = `node export-barcode-to.js --out <ファイル>` (2026-09-29 に本物で 5,188 行・12 秒)。
- 手で試す = `node scripts/logizard-import/lz-daily-import.mjs --force-window [--as-of YYYY-MM-DD]` (止めてあっても動く・ping しない・その日の済みの印を書かない・期限の内だけ。昼に試す = `--as-of` にその朝の日付。Stream Deck を押さない間に)。

#### 毎晩の本番の切替と GAS への戻し (切替の PR #1558・2b-2 契約 v3 N2・N3・N8・N9・3b 契約 K3-8)

**分け方**
- 準備の PR (この節・確かめのスクリプト・戻しの版の台帳 `lz-gas-rollback`・readiness の GAS の判定) は先にマージしてある。切替の前から miniPC で確かめを使える (Codex #1558 R1 High)。
- 切替の PR (#1558) は、次を含む。**切替の日の手順の中でマージする。**
  - Stream Deck の ③ を外す。
  - 台帳 `lz-daily-import` を載せ、影を退役させる。

**切替の前にそろえるもの**
- lz-daily が 3 日続けて合格 **かつ** 成果物をポータルに送れた (台帳 `lz-daily-cutover` ①・2026-09-30 が 1 日目)。
- 少数件の実機の取込が verified (2026-09-30 済み・`lzim_test_20260930T053735_f97c97`)。
- 戻しの版 = tag `lz-gas-rollback-20260930` (= commit 69999181・③ のある最後の master・台帳 `lz-gas-rollback` に 11 ファイルの sha256)。
  #1558 をマージするまでに master の `tools/logizard-automation` が変わったら、`git diff lz-gas-rollback-20260930 origin/master -- tools/logizard-automation` を見る。streamdeck のファイルに変更があれば、tag を作り直して台帳を直す。
- 下の「戻しの練習」を 1 回。

**いつ・どこで**
- 切替も戻しも**夜の窓の外 (Render の時計で JST 01:30〜23:30)** に行う (N8。00:15〜00:55 は毎晩の取込・影の時間、00:00〜01:30 は Stream Deck の夜の止め)。確かめのスクリプトも、この時刻の外では ❌ になる。
- miniPC のコマンドは、リポジトリ直下 (`C:\Users\bfaith\bfaith-portal`) の PowerShell 5.1 で打つ。
- Stream Deck の PC = 中原さんの PC (manifest の `streamdeck` はこの 1 台)。
- 確かめ = `node scripts\logizard-import\lz-cutover-check.mjs --expect <段階>` (miniPC のリポジトリ直下)。
  - ポータルと .env を読むだけで、値は出さない。
  - 全部 ✅ のときだけ exit 0。❌ があれば次の手順に進まない。

**戻しの練習** (切替の前に 1 回・昼・N9 = `LZ_MANUAL_V4=off` まで)
1. 止める: `node C:\tools\logizard-automation\import-state-cli.js halt --by <名前> --reason "戻しの練習"` → `lz-cutover-check.mjs --expect before` が全部 ✅。
2. Stream Deck の PC で、#1558 の版 (①② だけ) を配る。**配る元を固定する** (古い作業場所から配ると、①②③ の版を配って ①②③ に戻す = 戻しの練習にならない。Codex #1558 R3 Medium)。
   - レビュー済みの #1558 の head の SHA (= `<head>`) を控える。
   - `git fetch origin` → `git worktree add --detach C:\tmp\lz-cutover-rehearsal <head>`。
   - `git -C C:\tmp\lz-cutover-rehearsal rev-parse HEAD` が `<head>` で、`git -C C:\tmp\lz-cutover-rehearsal status --porcelain` が空。
   - その作業場所で `node tools/logizard-automation/deploy.mjs --pc streamdeck --apply` → `--check` (drift 0)。
   - **読み戻す**: `node C:\tools\logizard-automation\auto-barcode.js --show-mode` が「① 新商品の取込 → ② バーコード情報の書き出し (③ 毎日の商品マスタは miniPC の自動が取り込む)」を出して exit 0。「知らない引数」なら ①②③ の版のまま = やり直す。
3. 中原さん: Render の環境変数に `LZ_MANUAL_V4=on` → 反映を待つ → `import-state-cli.js status` の `manual.v4` が true。
4. 下の「GAS への戻し」の 3〜7 をする (旗 off → `--expect rollback` → 固定の版を配る → `--check` → `--dry`)。
5. 止めの解除: `status` の `halt_revision` を見て `import-state-cli.js resume --by <名前> --note "戻しの練習の後" --halt-revision <番号>`。
6. 片付け:
   - 固定の版の作業場所を消す: `git worktree remove C:\tmp\lz-gas-rollback`。本当の戻しで同じ場所に作り直すため。
   - #1558 の版の作業場所も消す: `git worktree remove C:\tmp\lz-cutover-rehearsal`。
   - 練習の後に #1558 の `tools/logizard-automation` (streamdeck のファイル) が変わったら、練習をやり直す (`git diff <head> <新しい head> -- tools/logizard-automation`)。
   - 終わりの形は今と同じ (Stream Deck は ①②③ の固定の版・旗 off・毎晩の本番 off)。
   - `cutover_phase` は練習では変えない (一方通行)。

**切替の手順** (N3。1 つ終わるごとに確かめる)
1. 自動の取込を止める: `import-state-cli.js halt --by <名前> --reason "切替"`。
2. `lz-cutover-check.mjs --expect before` = 全部 ✅ (夜の窓の外・止め・生きた鍵なし・手の取込なし・未解決の取込なし・旗 off・`LZ_DAILY_IMPORT` off)。
3. 中原さん: Render の環境変数 `LZ_MANUAL_V4=on` → 反映 (再起動) を待つ。
4. ポータルの画面「ロジザードの取込の状態」の設定で `cutover_phase` = cutover を**押す**。
   - 一方通行 = 元に戻せない。
   - 既定でも cutover と読むが、人が設定したことを次で確かめる。
5. `lz-cutover-check.mjs --expect cutover` = 全部 ✅。見るもの:
   - 旗 on の読み戻し (status と readiness の両方)。
   - `cutover_phase` を人が cutover に設定した。
   - GAS の CSV の手の取込を断る = 本当の道と同じ判定で gas_closed。
   - 成果物の無い毎晩を断る (artifact_missing)。
   - 旧い手の ③ を断る (manual_daily → retired・DB に何も書かない)。
   - DATA_DIR がこの miniPC のもの (初期化の印がポータルと同じ)。
   - 次の夜の済みの印が無い。
6. #1558 をマージする → Render の反映を待つ (台帳が `lz-daily-import` に・影は RETIRED_JOBS。読み戻しは 8 の `--expect ready`)。
   - `RETIRED_JOBS` の `lz-daily-import-shadow` の `retired_at` は、マージの日に直してからマージする。
   - マージの commit (GitHub の #1558 の merge commit の SHA) を控える = 下の `<merge>`。
7. 配る (**配る元が #1558 の後か確かめてから**。`deploy.mjs --check` は「配った先が配る元と同じ」しか見ない = 古い作業場所から配っても drift 0 になる。Codex #1558 R2 High):
   - miniPC:
     - `git pull --ff-only` → `git merge-base --is-ancestor <merge> HEAD` が exit 0 (#1558 が入っている) → `git status --porcelain` が空。
     - `node tools/logizard-automation/deploy.mjs --pc minipc --apply` → `--check` (bat の見出し・drift 0)。
   - Stream Deck の PC:
     - 配るための作業場所を merge の commit で作る: `git fetch origin` → `git worktree add --detach C:\tmp\lz-cutover-deploy <merge>`。
     - その作業場所で `node tools/logizard-automation/deploy.mjs --pc streamdeck --apply` → `--check` (drift 0)。
     - **読み戻す**: `node C:\tools\logizard-automation\auto-barcode.js --show-mode` が「① 新商品の取込 → ② バーコード情報の書き出し (③ 毎日の商品マスタは miniPC の自動が取り込む)」と「③ … この版には無い」を出して exit 0。
       - ログイン・CSV・鍵に触らない。
       - 「知らない引数です: --show-mode」= ③ のある古い版を配った = 8 に進まない。
     - 片付け: `git worktree remove C:\tmp\lz-cutover-deploy`。
8. miniPC のリポジトリ直下の .env に `LZ_DAILY_IMPORT=on` を足す (`LZ_DAILY_IMPORT_SHADOW` の行は消す。.env は 1 つだけ)。
   - → `lz-cutover-check.mjs --expect ready` = 全部 ✅。見るもの:
     - cutover の全部。
     - 毎晩の本番 on。
     - 送り先 `GCHAT_WEBHOOK_JOBS` が本番と同じ判定で使える。
     - この miniPC のリポジトリの台帳が #1558 の後 (`lz-daily-import` = P2・00:20・猶予 40 分・影は退役)。
     - **Render の見張り (`/apps/jobs-monitor/status`) も同じ台帳** (反映を待った = 01:00 の締切が効く。見張りは台帳に無い id の ping も 200 で受けるので、ping の成功では分からない)。
9. 止めの解除: `status` の `halt_revision` を見て `import-state-cli.js resume --by <名前> --note "切替" --halt-revision <番号>`。
10. 次の夜 00:20 の後:
    - `C:\tools\logizard-automation\logs\scheduled.log` の `[lz-daily-import]` が ✅ verified になり、台帳 `lz-daily-import` の ok が来ている。
    - → `lz-daily-cutover` の完了の ping を 1 回 → RETIRED_JOBS へ移す PR。
    - 止まった (⏭️・❌) = 要対応スペースの知らせと台帳 `lz-daily-import` の runbook を見る。

**GAS への戻し** (K3-8・N2。システム全体を旧方式に戻すときだけ。**台帳 `lz-gas-rollback` の期限 (remove_by) の後は使わない** = `--expect rollback` が断る・期限を延ばすなら理由を書いた PR で)
1. 止める: `import-state-cli.js halt --by <名前> --reason "GAS への戻し"`。
2. 生きた鍵・開いている手の取込・未解決の取込が無いことを確かめる (未解決があれば、ロジザードのインポート履歴を見て先に resolve)。
3. miniPC の .env の `LZ_DAILY_IMPORT` の行を消す (off)。
4. 中原さん: Render の `LZ_MANUAL_V4` を消す (off) → 反映を待つ。
5. `lz-cutover-check.mjs --expect rollback` = 全部 ✅ (夜の窓の外・止め・鍵なし・手の取込なし・未解決なし・旗 off・`LZ_DAILY_IMPORT` off・戻しの版の期限の内)。
6. Stream Deck の PC に固定の版を配る。
   - 作業場所が無ければ作る: `git fetch origin tag lz-gas-rollback-20260930` → `git worktree add --detach C:\tmp\lz-gas-rollback lz-gas-rollback-20260930`。
   - もうあるなら、`git -C C:\tmp\lz-gas-rollback rev-parse HEAD` が 69999181aae38bded0ad162cad523e0a4f796c1b で、`git -C C:\tmp\lz-gas-rollback status --porcelain` が空のときだけ使う。違えば消して作り直す。
   - その作業場所で `node tools/logizard-automation/deploy.mjs --pc streamdeck --apply` → `--check`。drift 0 なら、台帳 `lz-gas-rollback` の 11 ファイルの sha256 と同じ版。
7. 旧方式を有効にする。
   - Stream Deck の PC の `C:\tools\logizard-automation\.env` に `LOGIZARD_BC_DAILY=auto` があれば、その行を消す (固定の版は auto だと ③ をしない)。
   - `node C:\tools\logizard-automation\auto-barcode.js --show-mode` が「知らない引数です: --show-mode」で止まる (= ③ のある固定の版が入っている。①② だけの版なら見出しが出る)。
   - `node C:\tools\logizard-automation\auto-barcode.js --dry` で「→ ③ 毎日の商品マスタの取込」と出て、③ の CSV (GAS の出力) を見る。
   - 終わった後に `C:\tools\logizard-automation\logs\logizard-session.lock` が残っていない (鍵を取って返せた)。
   - `--dry` はロジザードにログインする (押さない)。共通アカウントを使う人がいない時間に (L-16)。
8. 自動の取込の止めは**解かない** (同じ夜に自動と GAS の ③ が両方取り込まない。GAS の ③ は時刻で分ける = 00:00〜01:30 は動かない)。
   - 台帳 `lz-daily-import` は毎晩の締切で鳴る。戻しが 1 日を超えるなら、台帳を戻す PR を作る (lz-daily-import を外す・理由を書く)。

**台帳**: `lz-daily-build` (scheduled_job・P3・毎日 07:00 + 猶予 7 時間 = 作れた回の ok が来なければ気づく) / `lz-daily-import` (scheduled_job・P2・00:20 + 猶予 40 分 = 01:00 までに verified の ok が来なければ気づく。切替の PR で影 `lz-daily-import-shadow` を置き換えた = RETIRED_JOBS) / `lz-gas-rollback` (temporary_asset・GAS への戻しの固定の版・2026-11-30 まで) / `lz-daily-cutover` (human_obligation・P3・30 日) = 3 日続けて合格 → ③c-1b の後に少数件の実機の取込 → 切替日。

**試験**: `scripts/test-lz-nightly.mjs` (毎晩の本番の miniPC 側) / `scripts/test-lz-cutover-check.mjs` (切替・戻しの確かめ・本物のポータルの状態の機械・戻しの版の sha256 と tag) / `scripts/test-lz-daily.mjs` [1]〜[12] (ロジザードの一覧の見出しは実ファイルの 1 行目のバイト。[11][12] = 成果物をポータルへ送る・入口)・`scripts/test-retry-rerun.mjs` (照合が直ったら作り直す)

### マスタの古い入口の門 (⑤-3。Company DB構想 14 §5・§9 v2 M2・§10 契約 v3 H1・PR #1565 Codex R1・中間レビュー 2 回)

古い入口 = NE の写しにマスタを書く API・画面・手の CLI・人の操作で動く取込 (miniPC の `/apps/warehouse/register` と SKU マスタ・会計アプリ 5 つの `POST /register`・fba-profitability の原価・product-hub の税率と古い新商品の作り方 (`/new`・NE のコードから登録・NE が先の自動取込・Notion の画像の取込)・Notion の取込・profit-calculator の NE 用 CSV と仕入れ先・発注アプリの仕入先・売れ筋共有の表示名・手の取込)。

- **一覧 = `config/master-legacy-entries.mjs`** (閉じる入口・閉じない口 (写し・閉じ済み・手の入口の 3 種類)・CLI の mode・書かない試しの見分け方 `dry_run`)。一覧がそのまま門の設定。id は英数字と `_.:/-` だけ (⑤-1 の manifest の形)。
- **門 = `lib/master-legacy-gate.mjs`**。切替の段階 (`ops.master_cutover_state`・0051) を**読めて** `legacy_open` のときは今までどおり書ける (frozen 以降は下の ⑤-3b = owner_cols の持ち主が C の入口だけ閉じる)。
  - 🆕 **⑤-3b (2026-10-04・列を分けて切り替える)**: `frozen` 以降は、**その入口が書く列 (一覧の `owner_cols`) のどれかの持ち主が C の入口だけ**閉じる。列が全部 load の入口 (10/5 なら SKU タブ = Amazon SKU の対応・発注アプリの仕入先と一括取込・売れ筋共有の表示名・profit-calculator の仕入れ先・SKU マスタの取込) は開けたまま。
    - 持ち主 = **Company DB の epoch** (`ops.master_ownership_state` の **active と prepared** の C の列を合わせたもの。段階が legacy_open の間は持ち主を読まない = prepare しただけでは閉じない・閉じ始めるのは frozen にした時点。cancel で prepared が消えると、frozen のままでもその列の入口は再び開く)。🚨 `config/master-ownership.mjs` (configured) は見ない (デプロイの成果物 = 場所ごとに切り替わる時刻が違う)。
    - 新商品の古い作り方 (product-hub の `/new`・NE のコードから登録・自動取込を手で回す・Notion の画像の取込・intake-cron) は `owner_match: 'all'` = 新しい登録の列 (`NEW_PRODUCT_COLS` = `NEW_ENTRY_KEYS` の単品 + セット) が**全部** C のときだけ閉じる。product-hub の `/new` の案内 (`newEntryGate`) も同じ門の答え (開いている = 今までの画面)。
    - 🚨 段階が legacy_open 以外で**持ち主を読めない** (0055 の表が無い・門のログインに `ops.master_ownership_state` の select が無い・記録が壊れた) = **全部の入口を閉じる** (503 `{error:'master_owner_unreadable'}`・CLI は終了コード 3)。門のログインの権限は `create-master-edit-roles.mjs` を 0055 の後に流し直すと付く。legacy_open の間は持ち主を読まない (切替の前の動きは ⑤-3 と同じ)。
    - 画面は閉じた列の部品だけ隠す (`legacyBannerHtml(info, { parts })`。/register = `REGISTER_WRITE_PARTS`・データウェアハウスの画面 = `DASHBOARD_WRITE_PARTS`)。
    - 🚨 **切替の日の順番 (Codex #1610 R1 High)**: 門を通った古い書き込みは prepare をまたげる (HTTP・定期実行は持ち主の epoch の鍵を持たない・CLI が持つのは段階の鍵だけ = prepare は待たない)。prepare の後に書き終えた値は `--use-prepared` のロードに入らず、C の列は既にある行を上書きしないので**黙って消える**。書きかけ 0 を見るだけでは防げない。→ 次の順番だけで進める:
      1. `master-ownership-epoch.mjs prepare` (段階は legacy_open のまま = 入口はまだ開いている)
      2. `master-cutover.mjs --to frozen` (段階を変える関数は CLI の共有の鍵を待つ。この瞬間から C の列の入口だけ閉じる)。証拠の `owner_hash` は**新しい門の記録 (ack) のハッシュ** = 配ったコードの configured (13 キー = `c36e3d0f5f56cb01e7acdf71f73a0cf6608826ffeabfa58506734e41bbb6b3dd`)。active (全部 load = `4f53cda1…`) を入れると `acks_invalid` で止まる
      3. 全部のプロセスの書きかけ 0 (`master-legacy-instance.mjs --list` の inflight・`pg_locks` の `hashtext('ops.master_cutover')` の共有の鍵 0 = CLI が書いていない)
      4. **active (全部 load) の最後のロード** (NE の取得 → m_products の作り直し → Render の同期 → `remote-load.mjs load --apply --wait` = `--use-prepared` を付けない) = prepare をまたいで書き終えた古い入口の値を Company DB に回収する → そのロードが返した run_id の report が成功・照合 ② (今の値) で未説明の差 0 を確かめる (🚨照合 ① は host = render-nightly の 02:00 のロードだけを選ぶ = この手動のロードは見ない。① はその朝の基準として見る)
      5. `remote-load.mjs load --apply --wait --use-prepared` → 写し → 作り直し → 確かめ → activate
      試験 = `scripts/test-master-legacy-gate-pg.mjs` [19] (2 つの接続で交差を再現・3 を飛ばして 5 = 値が消える・4 の後 = 残る)。
    - 配る前の確かめ (`master-legacy-readiness.mjs`) は門のログインで `ops.master_ownership_state` の有無・SELECT の権限・門と同じ読み方を確かめる (legacy_open の間は門は持ち主を読まない = ここで見ないと frozen にした瞬間に全部 503)。
  - API = 410 `{error:'master_frozen', message, url}`。段階が読めない = 503 `{error:'master_phase_unreadable'}` (閉じる側)。何も書かない。画面は `message` を出す (`master_frozen` の文字は出さない)。
  - 書かない試し (NE のコードから登録・自動取込を手で回す・Notion の画像の取込・Notion の状態から取込 の dry run) は、切替の状態 (段階または持ち主) を読めないときだけ注意つきで通す (応答の見出し `X-Master-Legacy-Warning: phase_unreadable`)。閉じた後は 410 のまま。 (🆕 #1610 R6: 持ち主を読めないときも書かない試しは通し、ヘッダは今のところ `phase_unreadable` のまま = 書き込みは起きない。専用の warning にするのは後の PR)
  - 門で待っている間に相手が切れた (画面を閉じた) = 書かない (書きかけにも数えない)。
  - 画面 = 帯「マスタは新しい画面で直します ↗」+ 書く部品を隠す。
  - product-hub の税率:
    - 手入力は閉じたら 410。画面は税率を**変えたときだけ**送る (保存できたら送った値を「元の値」にする) = 名前・売価・メモの保存は Company DB が止まっていても通る。
    - 詳細画面: 切替前 = 今までどおり / 切替の状態を読めて閉じている = Company DB の税率を見せるだけ (代表コードは構成の SKU から。混ざる・無い・読めない = 決められない・試算は「税率が決まっていません」= 0% で計算しない) / **切替の状態 (段階または持ち主) を読めない = 今の値を見るだけ** (Company DB を読みに行かない・「出品は止まります」と言わない)。
    - 楽天の登録: 切替の状態 (段階または持ち主) を読めない = 止める (切替前の瞬断で Company DB の税率に黙って切り替えない)・閉じた後に Company DB の税率を決められない = 止める。プレビュー (送らない) は、切替の状態 (段階または持ち主) を読めないとき今の税率で見せて注意を添える。
    - AI の生成の材料 (`/generation-queue`・claim) は、切替前だけ `yahoo.tax_rate` を渡す (閉じた後・切替の状態 (段階または持ち主) を読めない = null)。
    - セットを作る: 切替の状態 (段階または持ち主) を読めない = 503 (作らない)・読めて閉じている = 作るが親の税率は写さない・legacy_open = 今までどおり。
  - 発注アプリは仕入先 (`kind = suppliers` の追加・削除・CSV・宛先の CSV) と一括取込 (`POST /api/import` = 中に仕入先があるので**丸ごと**) を、仕入先の列 (`suppliers.*`) が C になったら閉じる (⑤-3b。10/5 は load = 開いたまま) = 切替の手順で書き込み先を Company DB に替えるまで仕入先は見るだけ。発注条件・資材・先方品番の画面は止めない。
  - CLI = mode が分かったらすぐ (引数・ファイルの検査より前・DB を開く前) に終了コード 3。書く間は段階の**共有の鍵** (`pg_advisory_lock_shared(hashtext('ops.master_cutover'))`) を持ち、鍵を取ってから段階を読み直す = 段階を変える側 (排他の鍵) は CLI が書き終わるまで待つ・変えている最中に始めた CLI は待ってから読む。csv-import.js は `product_shipping`・`exception_genka` だけ閉じる (受注・ロジザード・NE の写し・送料の表は止めない)。CLI が書くのは miniPC の warehouse.db だけ = ⑤-1 の夜間ロードの鍵 (`core.master_write_lock_key`) は取らない。
  - 定期実行: product-hub の NE が先の自動取込 (intake-cron) は丸ごと止める (閉じている = ok の ping で「止めた」・読めない = fail の ping)。
- **段階の読み方**:
  - 接続先 = この場所の門のログイン (下) → 無ければ `COMPANY_DB_URL` (表の持ち主)。🚨 見張りの `COMPANY_DB_WATCH_URL` (watcher・接続 3 本まで) には落ちない。
  - 🚨 **env と 0051 の 3 つの場合** (Codex #1565 R4 Low):
    1. 0051 が本番に無い・段階を読める接続が無い (門のログインも `COMPANY_DB_URL` も無い・つながらない) = 読めない = 古い入口は全部 503 / CLI は終了コード 3。
    2. 場所ごとの門のログイン (`COMPANY_DB_MASTER_GATE_*_URL`) が無いが `COMPANY_DB_URL` で段階を読める = **古い入口は legacy_open の間は今までどおり動く**。ただし門の記録は書けない (`precheck_failed`) = readiness が落ちる・⑤-1 の段階の関数は全部の場所の新しい記録を求めるので**切替は進められない**。
    3. 両方の場所の門のログイン + readiness が両方とも終了コード 0 = 配ってよい (この PR のマージの条件)。
  - 🚨 書き込み・CLI は**毎回**読む。前に読めた値は使わない。プロセスごとに接続 **1 本**のプールを 10 分つないだままにする (miniPC → Render はつなぎ直すと TLS と認証で 0.5〜0.8 秒) と 1 回の読み直し (200ms 後)。門の記録もこの 1 本で書く = プロセスあたり 1 本 (配り直しで古い + 新しいが重なっても 2 本。門のログインの接続の上限は ⑤-1 で 8)。読めない = 503 / 終了コード 3。
  - 画面の帯だけ前の結果を使う (読めた = 30 秒・読めない = 5 秒)。古ければ同時の画面で 1 回の読みを分け合い、1 秒待つ (読み直さない・表示だけ)。1 秒で返らない = その画面だけ 5 分前までの読めた結果を見せる (無ければ読めない扱い)。打ち切りは使い回さず、遅れて返った結果を使い回しに入れる。
  - 読めなかった回数・断った回数 (410・503)・切れた相手・画面の打ち切り・書かない試しを通した回数・読むのにかかった時間は読み戻しに出す。1 秒を超えた読みと読めなかった回はログに出す。
  - 読む時間を測る (読むだけ): `node -r dotenv/config scripts/company-db/master-legacy-latency.mjs --host minipc` = つなぎ直し + 読む / つないだまま読む の p50・p95・いちばん遅い。
- **drain (書きかけを流し終える)**: 門を通った書き込みは、終わるまでプロセスの中で数える (件数・いちばん古い開始・入口ごと)。CSV の取込は受け取った後・書く前にもう一度段階を読む (受け取っている間に frozen になっても書かない・受け取ったファイルも消す)。
  - 🚨 終わり = ハンドラが応答を返したとき (`res.end`)。相手が切れた (`close`) では減らさない (async のハンドラは相手が切れても動き続けて書く)。外の API (Notion など) を待ってから書くハンドラは `legacyHandler` で包み (promise が終わるまで数える)、書く直前に `legacyWriteFence(res)` (相手が切れた・段階が変わった・止めている途中 = 書かない)。応答を返さないまま終わらないハンドラは数えが残る = 段階を進められない (安全側・読み戻しの `oldest_started_at` で見える)。
  - 定期実行 (product-hub の自動取込) は `runLegacyJob` の中 = 段階を毎回読み、取込の間は書きかけに数える (`inflight.by_entry`)。
- **門の記録 (ack) = ⑤-1 の `ops.record_legacy_gate_ack` (`lib/master-cutover.mjs` の `recordLegacyGateAck`)**:
  - 書くのは**場所ごとの門のログイン** (⑤-1 の `master_gate` の中の LOGIN ロール。関数は `session_user` と場所が違えば拒む):
    - Render = `COMPANY_DB_MASTER_GATE_RENDER_URL` (`master_gate_render`)
    - miniPC = `COMPANY_DB_MASTER_GATE_MINIPC_URL` (`master_gate_minipc`)
    - 🚨 ほかの場所のログインは使わない (miniPC は Render の URL があっても書かない)。ログインと場所が違う = DB が `gate_host_mismatch` (42501) で拒む (env の取り違え)。
    - 場所の判定は server.js と読み戻しの API で同じ (`legacyAckHost()` = Render / miniPC の WarehouseServer (`PORTAL_VARIANT=warehouse`) / それ以外 = 書かない)。
  - いつ: 起動のとき・要求が来たついでに 5 分おき (新しい定期実行は作らない)・読み戻しを呼んだとき・止めるとき (SIGTERM / SIGINT = 受付を閉じる → 記録の書き直しをやめ、新しい書き込みは 503 (`master_shutting_down`)・生きている書き込みの切符を止める → 書いている途中の普通の記録を待つ → **書きかけ (HTTP の切符・定期実行) が 0 になるまで待つ** (止めた切符は書く直前の確かめで止まる) → 「止めた」と理由 (200 字まで)・書きかけ 0 を **1 回だけ**。全部で長くても 10 秒 (Render は 30 秒・WinSW は既定 15 秒待つ)。途中の普通の記録か書きかけが終わらない = 「止めた」は書かない (順番を守る = 最後の記録が stopped = false に戻らない・本当は書きかけがあるのに 0 と書かない)。返事の stopped も確かめる)。
  - 🚨 止めている途中の確かめは、段階を読む (await) の**後**・切符を出す直前・書く直前の確かめ (`legacyWriteFence`・CSV の `legacyRecheck`・定期実行の `fence`) の全部で見る = 段階を読んでいる間・受け取っている間に止め始めた要求は書かない (受け取ったファイルも消す)。定期実行は `runLegacyJob(id, ({ signal, fence }) => …)` = 待ってから書くときは書く直前に `fence()`。
  - miniPC の WarehouseServer は WinSW のサービス (止めるときは Ctrl+C = Node の SIGINT のはずだが、届いたか・2 秒で書けたかは分からない) → **次の起動で、前の起動のプロセスが居なければ「止めた」を書く** (名札は `DATA_DIR/master-legacy-instance.json`)。前の pid がまだある (使い回しを含む)・別の PC = 書かない (安全側)。Render は止めるとき SIGTERM を送り待つので使わない。
  - 中身: host・プロセスの名札 (`RENDER_INSTANCE_ID` か PC 名 + pid + 起動の乱数)・build の番号 (Render = `RENDER_GIT_COMMIT`・miniPC = git の HEAD)・一覧 (manifest `{ entries: [{ id, kind: code | manual }] }`・ハッシュは DB が計算)・持ち主表・見た段階・書きかけの件数といちばん古い開始。
  - 書く前に確かめる (場所・build の番号・段階を読める・門のログインがある・関数がある)。書いた後に返事 (`ack_id`・DB が同じ一覧から計算した `manifest_hash`・`acked_at`・`stopped`) を確かめてから `acked`。だめなら書かずに理由をログ (同じ理由は 1 回) と読み戻しに出す (関数が無い = 0051 の前 = 注意 1 回)。書く間に段階が変わった (`stale_phase`) = 読み直して 1 回だけ書き直す。
  - 止まり方が分からないプロセス (落ちた・電源・2 秒で書けなかった) = ⑤-1 の段階を進める関数は「今までに 1 回でも記録を書いたプロセスで、最後の記録が 15 分より前 (何日前でも) で『止めた』でもない」プロセスがあると進めない (年齢では外れない = 止めたプロセスには必ず「止めた」が要る) → 人が止まったのを確かめて `node -r dotenv/config scripts/company-db/master-legacy-instance.mjs --list` / `--stop --host minipc --instance <名札> --reason "…" --yes` (手の操作・定期実行にしない)。🚨 15 分以内に記録があるプロセスは `--force` が無いと拒む (動いているかもしれない)。
- **読み戻し**: `GET /apps/warehouse/api/master-legacy-gate` (miniPC と Render の両方にある) = その環境・**その 1 つのプロセス**が見ている段階・書けるか・manifest_hash (最後に DB が受け取った一覧)・一覧の数・持ち主表のハッシュ・build の番号・名札・書きかけ (`inflight.count`・`oldest_started_at`)・数・門の記録 (呼ぶと記録も書き直す)。全部のプロセスは `master-legacy-instance.mjs --list` で見る。
- 🚨 **マージの前に** (PR の本文のチェックリスト。miniPC の PowerShell 5.1 で。まだ流さない → 中原さんの OK の後):
  - ⚠️ **この PR は後方互換ではない**: 0051 の本適用・**両方**の門のログインの env (Render の `COMPANY_DB_MASTER_GATE_RENDER_URL`・miniPC の `COMPANY_DB_MASTER_GATE_MINIPC_URL`)・配る前の確かめ (readiness が両方とも終了コード 0) が**そろうまでマージしない**。欠けたまま配ると、上の 3 つの場合の (1) = 0051 が無い・読める接続が無い場所は古い入口 (/register・会計アプリ・税率・仕入先・手の取込) が全部 503 / 終了コード 3 / (2) = 門のログインだけ無い場所は古い入口は動くが門の記録が書けず切替が進められない。
  - **中原さんの手順 (この PR をマージできる状態にする)**。miniPC の PowerShell 5.1 (`&&` と `cd /d` は使わない)。🚨 印は「まだ流さない」= 中原さんの OK の後:
    1. 🚨 **0051 を本番の Company DB に入れる** (Claude が SSH で miniPC から。中原さんの OK の後・本番で使っていない worktree から dry-run → 本適用):
       ```
       # まだ流さない (中原さんの OK の後に Claude が SSH で)
       Set-Location C:\Users\bfaith\bfaith-portal
       git fetch origin
       git worktree add C:\tmp\sor51-migrate origin/master
       Set-Location C:\tmp\sor51-migrate
       npm ci
       $env:DOTENV_CONFIG_PATH = 'C:\Users\bfaith\bfaith-portal\.env'
       node -r dotenv/config scripts\company-db\migrate.mjs --dry-run     # 0051_master_edit だけが出ること (0050_finance_coverage がまだなら 0050 も)
       node -r dotenv/config scripts\company-db\migrate.mjs               # 本適用
       ```
    2. 🚨 **ロールを作る** (中原さんが miniPC の画面で。パスワードを含む接続文字列は**この画面にだけ**出る = Claude・チャットに貼らない):
       ```
       # まだ流さない (1 の後)
       Set-Location C:\tmp\sor51-migrate
       $env:DOTENV_CONFIG_PATH = 'C:\Users\bfaith\bfaith-portal\.env'
       node -r dotenv/config scripts\company-db\create-master-edit-roles.mjs --dry-run   # 流す文を見る (パスワードは出ない)
       node -r dotenv/config scripts\company-db\create-master-edit-roles.mjs             # 作る = 初めて作ったロールの接続文字列だけが出る
       ```
       出た行の置き場所 (画面からコピーしてそのまま入れる。チャット・メモ・共有ドライブに貼らない): `COMPANY_DB_MASTER_GATE_MINIPC_URL`・`COMPANY_DB_MASTER_OPS_URL`・`COMPANY_DB_MASTER_OBSERVER_URL` = miniPC の `C:\Users\bfaith\bfaith-portal\.env` の末尾に足す (`notepad C:\Users\bfaith\bfaith-portal\.env`) / `COMPANY_DB_MASTER_GATE_RENDER_URL`・`COMPANY_DB_MASTER_EDIT_URL` = Render の bfaith-portal の Environment に足す (Render は保存すると配り直す)。もうあるロールは何も出ない (パスワードは変えない) = 接続文字列が分からないときだけ `--rotate-password master_gate_minipc` (または `master_gate_render`) で変えて、同じ日にその場所の env を書き換える。
    3. 🚨 **WarehouseServer を再起動** (.env を読み直す。管理者の PowerShell): `Restart-Service WarehouseServer`
    4. 🚨 **配る前の確かめ** (このブランチの worktree で。読むだけ・何も書かない):
       ```
       # まだ流さない (2・3 の後)
       Set-Location C:\Users\bfaith\bfaith-portal
       git fetch origin
       git worktree add C:\tmp\sor53-check origin/feat/sor5-3-close
       Set-Location C:\tmp\sor53-check
       npm ci
       $env:DOTENV_CONFIG_PATH = 'C:\Users\bfaith\bfaith-portal\.env'
       node -r dotenv/config scripts\company-db\master-legacy-readiness.mjs --host minipc     # 終了コード 0 (✅ そろっている)
       $env:COMPANY_DB_MASTER_GATE_RENDER_URL = Read-Host 'Render の門のログイン (Render の Environment からコピー)'
       node -r dotenv/config scripts\company-db\master-legacy-readiness.mjs --host render      # 終了コード 0 (役 = master_gate_render)
       Remove-Item Env:COMPANY_DB_MASTER_GATE_RENDER_URL
       node -r dotenv/config scripts\company-db\master-legacy-latency.mjs --host minipc       # p50 / p95 / いちばん遅い を PR に書く (接続文字列は出ない)
       Set-Location C:\Users\bfaith\bfaith-portal
       git worktree remove C:\tmp\sor53-check
       git worktree remove C:\tmp\sor51-migrate
       ```
    5. 4 が両方とも終了コード 0 になったら、この PR をマージしてよい (マージの後は下の 4 の `--list`)。
  1. 0051 (⑤-1) が本番に本適用済み (無い = 段階を読めない = 古い入口が全部 503 で閉じる = 上の 3 つの場合の (1))。
  2. ⑤-1 の `create-master-edit-roles.mjs` を流し、出た `COMPANY_DB_MASTER_GATE_RENDER_URL` を Render の env に、`COMPANY_DB_MASTER_GATE_MINIPC_URL` を miniPC の .env に入れた (miniPC は `Restart-Service WarehouseServer`)。パスワードが出るのはロールを初めて作ったときだけ。もうあって接続文字列が分からない = `--rotate-password master_gate_render` (または `master_gate_minipc`) で変えて、その場所の env を同じ日に書き換える。
  3. **このブランチの miniPC の worktree** で配る前の確かめ (読むだけ・何も書かない。Render の Shell にはマージ前はこのスクリプトが無い):
     - miniPC: `node -r dotenv/config scripts/company-db/master-legacy-readiness.mjs --host minipc` が終了コード 0。
     - Render: Render の門のログインをこの 1 回だけ渡す: `$env:COMPANY_DB_MASTER_GATE_RENDER_URL = '<Render の門のログイン>'; node -r dotenv/config scripts/company-db/master-legacy-readiness.mjs --host render; Remove-Item Env:COMPANY_DB_MASTER_GATE_RENDER_URL` が終了コード 0 (役 = master_gate_render・関数の実行権・一覧の形)。
     - 読む時間: `node -r dotenv/config scripts/company-db/master-legacy-latency.mjs --host minipc` の p95 を PR に残す (つなぎ直しの p95 が 1 秒を超える = 起動の直後などは画面が 5 分前の結果か帯になる。書き込みは待つので止まらない)。
  4. マージして配った後: `node -r dotenv/config scripts/company-db/master-legacy-instance.mjs --list` で、Render と miniPC の**全部のプロセス**が「新しい」・段階 `legacy_open`・書きかけ 0・build = 配った commit。配る前の古いプロセスは全部「止めた」(何日前のプロセスでも、止めたが無ければ段階を進められない)。黙っている古いプロセスが残る = 止まったのを確かめて `--stop` (再起動の後は毎回見る)。
  5. **戻し方**: 配った後に古い入口が 503 のまま・門の記録が書けない = このマージを revert する PR → Render は自動で配り直し・miniPC は `git pull` → `Restart-Service WarehouseServer`。DB は何も変えていない (段階は legacy_open のまま・門の記録は追記だけで残っても害が無い)。🚨 **段階を frozen に進めた後は revert しない** (門の無いコードに戻る = 古い入口が開く)。
- **切替の手順の門の条件 (legacy_open → frozen。⑤-1 の関数の求めに合わせる)**: 🚨 当日の完全な順番は epoch の節「切替の日の順番」と AI_reference 17 §4.2 の表が正 (readiness → **prepare** → frozen → 書きかけ 0 → 最後の active (全部 load) のロード → そのロードの run_id の report の成功 + 照合 ② → --use-prepared → 写し・作り直し・確かめ → activate → company_owner)。ここは frozen の証拠の条件だけ。🚨 **prepare をしないで frozen にすると、持ち主がまだ全部 load = 13 キーの入口は閉じない**。
  0. 先に `master-ownership-epoch.mjs prepare` (段階は legacy_open のまま)。
  1. 上の 4 がそろっている (全部のプロセスが新しい記録・同じ build と一覧。今までに記録を書いて止めたプロセスは全部「止めた」)。
  2. NE の画面・GAS など機械で閉じられない入口 (manifest の `kind: manual` = `ne:item-screen`・`ne:set-kind` (🆕 広げる道 PR-6)・`gas:logizard-sheet-and-sku-map`。手の入口も `owner_cols` (人が書くキー) を持ち manifest に残る = 広げる (widen) ときは足すキーと重なる手の入口だけを止める・`manualEntriesForKeys`) を止め、止めた人と時刻を証拠 (`manual_entries_stopped` の `at`) に書く。🚨 `at` は**今の段階に入った後・サーバーの今以前** (先の日付・前の試みの証拠は ⑤-1 の関数が拒む = 進める日に止めて、その時刻を書く)。
  3. miniPC で手の取込 (csv-import ほか) が動いていないのを確かめ、`--list` で全部のプロセスが「新しい」かつ書きかけ 0 = 証拠の `drain` (`{ done: true, checked_by, checked_at }`。`checked_at` も今の段階に入った後・今以前) を書いて、段階を `frozen` に進める。⑤-1 の関数が確かめるもの: 全部の場所・全部のプロセスの新しい記録・build・一覧・持ち主表・**書きかけ 0 (→ frozen でも)**・黙っているプロセスが無いこと (何日前でも)・証拠。
  4. 🚨 **frozen の後の本当の drain**: `--list` で全部のプロセスに**段階 `frozen` の新しい記録**が来て、書きかけが 0 になるまで待つ (記録は要求のついでに 5 分おき。読み戻しを呼べばすぐ書く)。⑤-1 の関数は frozen → company_owner のときに「frozen に入った後の記録・書きかけ 0」を求める。CLI は書いている間は共有の鍵を持つので、段階を変える側が待つ (段階をまたいで書かない)。
  5. そこで初めて最後の同期 (active = 全部 load のロード・`--use-prepared` を付けない) に進み、そのロードの run_id の report の成功 + 照合 ② → `--use-prepared` のロード → 写し・作り直し・確かめ → activate の後に company_owner に進める (epoch の節の順番)。
- 書き込みの猶予 (legacy_open を最後に読めてから 60 秒などは書かせる) は**入れていない** (約束を変えるので中原さんが決める。案と良し悪しは PR の本文)。
- 試験: `scripts/test-master-legacy-entries.mjs` (ルートと関数の呼び出しをたどって、一覧に無いマスタの書き込みの口を落とす) / `scripts/test-master-legacy-gate.mjs` (入口ごとに legacy_open・閉じた・読めない・途中で閉じた・切れた相手・書かない試し・CLI・画面の遅い読み・PGlite の本物の記録の関数・前の起動) / `scripts/test-master-legacy-gate-pg.mjs` (実 PostgreSQL = 門のログインのプール 1 本・毎回読む・切断・打ち切り・CLI の共有の鍵・場所ごとのログインで本物の記録・止めた・--force・配る前の確かめ・frozen に進める・読む時間。`cd C:/tmp/pg-embed && node run-conc.mjs scripts/test-master-legacy-gate-pg.mjs <リポジトリ>`)。

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

## Amazon 財務の受け口 (0012 → 0043 で作り直し。F2b-1。設計 = AI_reference『CompanyDB構想/12_Amazon財務のCompanyDB取込_設計_20260929.md』)

決済レポートの明細は Company DB に置かない (D-37)。miniPC が明細を **SQLite の日次の財務と同じ出現順つきの重複除去** の後で「注文 × 計上日 × SKU × 行の種類」にまとめて送る (送り手 = `push/amazon-finance.mjs`・F2b-2)。
🚨 0012 の旧互換 (legacy_* の明細ごとの ABS・`v_finance_daily_legacy` (単価は trunc)・`finance_daily` の「≥ 0」の CHECK) は、2026-09-29 に SQLite の日次の財務の決まりが変わった (#1522 / #1525) ので 0043 で捨てた (D-49。本番の 0012 の表は 0 行だった。0043 は 4 つの表を排他で押さえて空でなければ止まる)。

- `core.finance_source_policy` = 会社 × モール × scope × 期間 [from, to) → 採用する取得元。**期間の重複は trigger が拒む**。0043 で amazon / jp に `amazon_settlement_unified` [2026-01-01, 無期限) を入れた (D-51 = miniPC の重複除去の後 = V1 / V2 の混ざりは 1 つ)。`core.finance_policy_gaps` / `core.assert_finance_policy_covered` は 0012 のまま
- `core.order_finance_receipts` = 注文 (疑似注文も) 単位の受領状態 (世代・集合の指紋・版・行数)。集合が空になっても残る = 遅れて届いた古い世代を拒む根拠
- `core.order_finance_daily` = **注文 × 計上日 × SKU × 行の種類 (`line_kind`) × 取得元 = 1 行**。金額は **整数円・決済の符号のまま** (売上 +・手数料 / 返金 / 値引き −・補てんは符号のまま)
  - 注文番号の無い行 (保管料・補てんなど) = **日ごとの疑似注文 `-:YYYY-MM-DD`** (D-50。月でまとめると 1 か月 714 行で 1 回に送れる 500 行を超える)。SKU はあれば持つ・無ければ `-`
  - `line_kind` = `sku` (SKU のある行) / SKU の無い行は手数料の種類 (storage / long_term_storage / removal / inbound_defect / low_inventory / subscription / easy_ship / other_account_fee) + not_account_fee + unknown
  - 金額 20 列 (commission = Commission + RefundCommission・**points** = PointsGranted / Returned・refund_principal = 返金のすべて) + `unmapped_jpy` (20 列のどれにも入らない金額) → **net = 20 列 + unmapped (CHECK)**
  - 内訳 (net に入らない): `promotion_tax_jpy`・`refund_principal_customer_jpy` / `_atoz_jpy` (返品数の推定用)・`account_fee_amount_jpy` (SKU の無い行の other_amount + item_related_fee = 月の手数料の build と同じ足し方)
- `core.apply_order_finance_batch(会社, モール, scope, 注文番号, 世代, 集合の指紋, 版, 行の jsonb 配列)` = 集合を丸ごと置き換える: 古い世代は `'stale'` / **指紋と版がどちらも同じ** なら世代だけ進めて `'same'` / 同じ世代で内容か版が違えば例外 / それ以外は削除 → 挿入 → 受領状態で `'applied'`。整数でない金額・未知の line_kind・疑似注文と計上日の違い・SKU と種類の食い違いは例外 (表の CHECK)
- **集合の指紋** = `apps/company-db/finance/order-finance-checksum.mjs` の 1 つの関数を **送り手と受け口の両方** が使う (並べ方 = UTF-8 のバイト順・列の順・NFC・安全な整数)。受け口 (`ingest/order-finance.mjs`) は内容から計算し直して送り手の申告と比べる (違えば 400)
- `mart.v_finance_daily` = **SQLite の `f_amazon_finance_sku_daily_v1` と同じ列名・同じ式** (line_kind = sku の行・日 × SKU。費用の列 = −Σ(符号のまま)・単価 = ROUND(Σ Order の本体 ÷ Σ Order の数量)・返品数 = ROUND(本体の返金 ÷ 単価)・closing_fee と units_marketplace_guarantee は 0・`profit_before_cogs_jpy` = build の profit_amount から原価を除いた式)。原価と利益は D7b
- `mart.v_finance_account_fees_monthly` = SQLite の `f_amazon_account_fees_monthly_v1` と同じ (月 × 手数料の種類・Σ account_fee_amount・負 = 費用・本物の注文の Easy Ship も入る)
- `mart.v_order_finance_summary` = 注文の累計 (疑似注文は入らない) / `mart.v_order_finance_uncovered` = 採用されない行 (reason = `no_policy` / `source_mismatch`。0 件が正常)
- `mart.finance_daily` = 日次の公開の表 (run_id publish・D7b で使う。0043 では作り直しただけ)
- Render の受け口 (`router.mjs`): `POST /apps/company-db/sync/order-finance`・`GET .../status` (件数・世代・run・DB の大きさ = pg_database_size と読めれば WAL)・`/receipt`・`/keys` (受領状態の注文番号・collate "C")・`/daily` (v_finance_daily・62 日まで)・`/account-fees`・`/uncovered`

試験 = `node scripts/test-company-db-finance.mjs` (PGlite 16 件: 0043 の安全の手順・旧互換が無い・policy / apply の applied・stale・same・版違い・空集合 / SQL の歯止め / net / **v_finance_daily = SQLite の試験 (test-finance-promotion-tax.js) と同じ場面の手で計算した値** (返品・カードの支払い取り消し・ポイント・値引きの税・注文番号の無い補てん)・単価の ROUND / 月の手数料 / uncovered / 注文の累計 / 指紋の決め / 受け口の検証)。🚨 policy の重複検査と受領行の for update の 2 接続の並行は PGlite では書けない

### Amazon 財務の送り手 (miniPC・F2b-2 = `apps/company-db/push/amazon-finance.mjs`)

- **集約** = `push/amazon-finance-transform.mjs`。SQLite の日次の財務の build (`sql/amazon/build_f_amazon_finance_sku_daily_v1.sql`) と月の手数料の build (`rebuild-amazon-account-fees.js`) の **別の実装** → **同じ決済の行を両方に通して全列一致** の試験で守る (build の CASE を変えたら集約も変えて試験を流す)。手数料の分け方は `apps/warehouse/amazon-account-fee-rules.js` の 1 か所 (build と送り手が共用)
  - 重複除去 = build と同じ出現順つき・注文 (疑似注文 = 注文番号の無いその計上日の行) ごとに全期間の全部の行を読む (business_line_key は注文番号・posted_date・SKU・金額を含む = 絞っても build と同じ)
  - SKU のある BuyerRecharge・預かり金 = build が日次の財務から除く → SKU を `-` にして not_account_fee (金額は net に残す)
  - 円未満の端数・読めない計上日・JPY 以外・空白だけの SKU・1 注文 500 行超 = その注文はまるごと送らない (台帳の「送れない鍵」・次の回に必ず読み直す)
  - 注文番号も計上日も読めない行 = 台帳の「鍵の分からない不正な行」→ その間は **疑似注文を 1 つも送らない** (空の集合も)・❌
  - どの列にも入らない金額 (shipment_fee・order_fee・direct_payment・SKU の行の知らない手数料の種類) = unmapped_jpy。「元の行に 0 でない金額があったか」を行で数えて ⚠️
- **鍵の選び方**: `--from/--to` = 計上日がその範囲にある行を持つ注文 (選んだ注文は全期間の全部の行を送る)・`--incremental` = 台帳の watermark (前回そろって終わった回の ingested_at の最大) の 3 日前から後に入った行を持つ注文 + 送れなかった鍵 (watermark が無い・変換の版が変わった = 全部)・`--full` = 全部を集約し直して指紋が変わったもの + **Render にだけある鍵に空の集合**
- **容量の見張り** (D-W5): chunk の前に「Render の DB の大きさ (+ WAL・読めなければ見込み) + 送った分 + 次の chunk の行 × 1 行の大きさ × 置き換えの倍率 + 余裕」が `CDB_DB_LIMIT_BYTES` の 80% を超えるなら送らずに止まる。🚨 `CDB_DB_LIMIT_BYTES` が無ければ送らない。1 行の大きさ・倍率 (`CDB_FINANCE_ROW_BYTES` / `CDB_FINANCE_REPLACE_FACTOR`) は `node scripts/company-db/measure-amazon-finance.mjs --from … --to …` (PGlite に入れて測る・Render に触れない) の値
- **突き合わせ** `--reconcile [--all]` = 直近 45 日 + 台帳の「未照合の月」(送った集合の新旧の計上日の月) の 日 × SKU (鍵の和集合・Easy Ship の割り振りだけの行は除く・数量 5 列 + 金額 21 列 + profit_before_cogs) と月 × 手数料の種類 (金額・行数) と uncovered。差の月 → 日次の財務のやり残し (`amazon-finance-pending.json`) / 月の手数料のやり残し (`amazon-account-fees-pending.json`・daily-sync の手数料の build / sync がその月までさかのぼり、両方が通ったら消す)。差が 1 回目 ⚠️・2 回続けば ❌
- 受領記録の指紋は受け口が作り直した行で計算する (`receiptRows`) = 次の回に「Render が復元された」と誤判定しない
- **daily-sync の工程 (F2b-3)**: 手数料の build / sync の後に `amazon-finance.mjs --incremental --require-backfilled` (日曜 = `--full`・`amazonFinanceDailyArgs`) → 送れたら `--reconcile --require-backfilled`。バックフィルの完了印の前はどちらも「⏭️ バックフィル前」で何もしない。送り手は retry の対象 (`--full`)・突き合わせは retry に載せない (差の続いた回数を数えている)
  - 🆕 2026-10-01 (D7b-1b-3): スイッチ `CDB_FINANCE_COORDINATOR=1` があるとき **送信は下の coordinator の中** (daily-sync の「Amazon決済と財務」・coverage の回は `--full`)・単独の `--incremental` / `--full` は送らない (dry-run だけ)。スイッチが無いとき = 今までどおり daily-sync の「CompanyDB財務(Amazon)」と単独の `--incremental` / `--full` が送る (token の無い chunk = Render は complete を無効にする・要約は ⚠️) — ただし coordinator に一度でも切り替えた環境 (coverage の世代がある) では送る前に ❌ (一方向・#1567 Codex R6 High)。突き合わせは今までどおり手数料の build / sync の後 (送れた朝だけ)
- **バックフィルの手順** (miniPC・人が。daily-sync の 07:00〜09:10 は避ける。送り手の lock があるので重なっても片方は見送る):
  1. `--from 月初 --to 月末 --dry-run` (1 注文の最大の行数 ≤ 500・最大の JSON・拾われない金額・鍵の分からない行) と `node scripts/company-db/measure-amazon-finance.mjs --from … --to …` (1 行・1 注文の大きさ・置き換えの倍率)
  2. 中原さんが D-W5 (Render の Postgres のプラン) を決める → miniPC の `.env` に `CDB_DB_LIMIT_BYTES` (と測った `CDB_FINANCE_ROW_BYTES` / `CDB_FINANCE_ORDER_BYTES` / `CDB_FINANCE_REPLACE_FACTOR`) を置く
  3. 1 か月ずつ `--from 月初 --to 月末` → `--reconcile` (差 0 を見る。差の月は翌朝の build が作り直す) を 2026-01 から当月まで
  4. `--mark-backfilled` (送れない鍵・鍵の分からない行が 0 で、全期間の突き合わせ `--reconcile --all` が一致したときだけ印を付ける) → 翌朝から daily-sync が送る
- 試験 = `node scripts/test-company-db-amazon-finance.mjs` (二重の実装の一致・送り手の通し (本物の router を HTTP で)・突き合わせ・手数料のやり残し)

### Amazon の決済と財務の coordinator (miniPC・D7b-1b-3 = `apps/warehouse/amazon-finance-coverage-run.js`。設計 = AI_reference『CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』§3.1・D-65・D-66)

- **なに**: 決済の取込 (SP-API の V2・手で積んだファイル) と上の送り手と「決済のそろい」(Render の `core.finance_coverage`・受け口 = PR #1561) を **1 回** として回す。daily-sync の 1 工程「Amazon決済と財務」(前の「Amazon Settlement」と「CompanyDB財務(Amazon)」をまとめた)・retry の単位も同じ
- **lease** = warehouse.db の `amazon_finance_coverage_lease` の 1 行。取る・放すのは coordinator の親だけ (持ち主の判定 = `retry-lock.js` と同じ・心拍の期限では奪わない)。生の表の取引はどれも「lease が自分の世代・token のまま」を同じ取引の中で確かめてから書く = 古い回 (死んだ持ち主) の子は書けない
- **1 回の順**: ① 過去の決済の行に版の無い行があれば **❌ で止まる** (重い版付けは流さない = 夜に手で `migrate-settlement-document-versions.js --commit`・#1567 Codex R1 Medium。新しく取り込む行の版は取込の取引の中で付く) ② mode を **生の表に書く前に** 決める → Render の今の世代を読み、台帳 (`company-db-push.db` の `order_finance:amazon:coverage_generation`) を少なくともそこまで進めて新しい世代と token を **HTTP の前に** 台帳と lease に保存 → `updating` (失敗なら取込を始めない) ③ 順番待ちの初期の印 (updating の後・lease の下) → 手で積んだファイル → SP-API の取込 (一覧の回に世代・token・最新の初期の印の epoch) ④ 送り手 = `--full` (全部の注文を変換 = receipt digest)・全部の chunk に `coverage_generation` / `run_token`・走査の同じ読み取りの取引の中で manifest (`amazon-finance-coverage.js`)・送れた注文の「読み直す注文」を読み取りの版 R 以下だけ消す ⑤ 完成の判定 → **complete の直前に source_revision と初期の印 (id・epoch・digest・順番待ち) を読み直す** (manifest と違えば送らない) → lease を確かめる → `complete` (1 回 120 秒・3 回まで)
- **採る版の列** (#1567 Codex R2 High 1): 版を選ぶ所 (送り手・判定・印・調べ) はどこも `VERSION_SELECT_COLUMNS` (detail_valid・header_count を含む) を全部渡す。足りなければ `selectDocumentVersions` が throw (黙って SQLite の view と別の版を採らない)
- **文書の版 (D-66)**: 決済のレポート 1 本 = 版 1 つ (`amazon_settlement_document_versions`・`document_version_id` = {source_layer, report_type, report_id, report_document_id, file_hash, normalization_version} の正規の JSON の SHA-256)。生の行は整数の鍵 `document_version_seq` で版を参照。決済ごとに採る版は 1 つ (層 (sp_api_v2 → sp_api_v1 → manual_csv → ほか) → 新しい順 → ID のバイトの順・SQL の view `v_amazon_settlement_selected_documents` と JS が同じ規則) = SQLite の build (日次の財務・月の手数料・月の mart・表示用の view) と送り手の変換は **採った版の行だけ**。V2 は V1 で取込済みの決済でも必ず版として保存。採る版が変わったら旧い版と新しい版の全部の注文を「読み直す注文」に
- **source_revision と読み直す注文**: `amazon_settlement_source_revision` (1 行) と `amazon_settlement_dirty_orders`。lines / headers の INSERT / UPDATE / DELETE の trigger (lines の UPDATE は OLD と NEW の両方の注文)。過去の行の backfill (版の鍵の null → 値) は数えない
- **complete にする条件** (どれか欠ければ Render は `updating` のまま = 正式な利益は null): 採った版の見出しがちょうど 1 行・期間が読める・明細の部品の合計 = 見出しの total・通貨 JPY (原文。過去の行は初期の印で代える) / 起点 (policy の period_from の JST 00:00) から途切れずにつながる end (🚨 区間に数えるのは初期の印か一覧の鎖の期待の report で裏付けのある採った決済だけ。**[起点, frontier) に重なる採った決済は全部** 裏付けが要る = 区間を延ばす決済も内側に収まる決済も。裏付けが無い = 今回の一覧の窓の中 (見出しの end ≥ 窓の createdSince) なら `not_in_inventory` (❌・API の食い違い待ち = retry)・窓より前か手のファイルなら `not_in_inventory_outside_window` (⚠️ もう一覧に出ない = 印を作り直す)・#1567 Codex R3 High 1・R4 High・Medium 1) / **初期の印** (`initial_marker_headers` / `initial_marker_settlements`・追記だけ) がある・印の決済が採った見出しと一致 / 印の後の成功した一覧の回が空白なく重なる (85 日以上あけない)・今回の回が最後 / 期待の report (必須の type = V2・report ID ごとに最新の観測) が全部 imported か satisfied_by_selected_settlement (detail_digest の完全な一致)・CANCELLED / FATAL / DONE でない / 期間の分からない report は未充足 / 送れない注文・整形できない・stale・読み直す鍵・読み直す注文 (R 以下)・鍵の分からない行 = 0 / source_revision が R のまま
- **Render を読めない朝は取込も始めない** (設計どおり = 生の表を書く前に coverage を無効にする)。取込は 85 日の固定の窓 (report の作成の時刻) = 翌朝 (か retry) に取り戻せる (長く止まって窓の外に出た report は一覧の鎖の切れ目 ⚠️)。取込だけ (財務のバックフィルの完了印の前) にするのは **coverage で一度も回ったことが無いとローカルと Render の両方で言えるときだけ** (#1567 Codex R1 High 1・R8 High。ローカルの証拠 = 台帳の coverage の世代 **か** warehouse.db の一覧の回・手のファイル・印の順番待ちの世代 = 台帳を失くしても分かる / Render = 決済のそろいの status が 200 で coverage = null = 台帳・warehouse.db を切り替えの前に戻した・新しい DATA_DIR でも分かる。Render を読めない = 判定できない = ❌)。Render の決済のそろいが 409 not_migrated・404 (Render が 0050 / #1561 の前に戻った疑い) は **いつも ❌** (#1561・0050 は本番に入っている = 今までの送り方 (legacy) の保険は消した・R8)。完了印の無い台帳 (台帳を失くした疑い) と合わせ、Render に古い complete が残っているかもしれない = **取込も送信もしない** (❌。台帳を戻す・財務のバックフィルをやり直す・Render を確かめる)
- **採る版の並び** (#1567 R1・R2・R12): 中身の確かな版 (detail_valid = 見出し 1 行・部品の合計 = total・通貨 JPY) が先 → 見出しがちょうど 1 行の版が先 → 層 (**V2 → V1** → 手のファイル → ほか) → 新しい順 → ID。🆕 V2 を V1 より先にした (#1567 Codex R12 Medium 1・前は同じ順位): 退避の `--source v1` (11/11 の V1 の廃止まで) が有効な V2 の版を追い出さない = V2 の取込に戻せば手で選び直さなくてよい (同じ順位だと、新しい V1 が採られ、同じ V2 の文書は入れ直しで版の時刻が変わらず V2 に戻らない)。V2 の版が壊れていれば V1 が採られる。中身の悪い新しい版・見出しの無い一部だけの版は、良い旧い版を押しのけない
- **止めるのは次の回で直る一時の状態だけ**: ① 版の無い行 (過去の行の版付けの前・途中) ② 要約の古い版 (版付け・取込が途中で止まった・行を手で直した)。どちらかがあれば SQLite の build (日次の財務・月の手数料・月の mart) も送り手も ❌ (行を黙って落とさない) = ① は夜に手で `migrate-settlement-document-versions.js --commit` (coordinator は流さない)・② は coordinator の次の回で直る
- **人が直すもの** (止めない・要約の頭が ⚠️🚨・retry しない): 良い版が無い決済 = 中身の悪い版の中で一番の版を **仮に採る** (「🚨 仮に採った壊れた版」provisional_broken_version・正式な値は null) / 決済 ID の決まらない版 (version_unresolved_settlement = 下の --allow-unresolved の後だけ)
- **見出しが 2 行以上の文書** (連結・壊れた文書・#1567 Codex R3 High 2): parser は見出しを全部返し、取込は拒む (V2 = blocked で一覧に理由つき・V1 = 同じく blocked・手のファイルは積まない・`ingestSettlement` も throw)。版の見出しの数は **物理の行の数**。過去の行の版付けも、見出しの物理の行が 2 行以上の文書があれば版を付けずに止まる (`--allow-unresolved` でも進めない = 正しい文書で入れ直す)。取込の一覧は証拠の一覧と **同じ固定の窓** (`createdSince` = 回の開始の 85 日前・`createdUntil` = 回の開始。#1567 Codex R4) = 一覧に出ない report を取込まない (85〜90 日前の report も取込まない = 毎朝の回なら 85 日の窓の中で取込済み。長く止まって窓の外に出た report は一覧の鎖の切れ目 ⚠️ = complete にしない側)
- **決済 ID が 2 つ以上ある過去の文書** (見出しと明細の決済 ID の和集合が 2 つ以上・#1567 R3): 版付け (migrate) は **版を付けずに止まる** = 行は版の無いまま = build と送り手も止まる (S-U2 の行を黙って落とさない・Render に墓石を送らない)。文書を確かめた上で進めるときだけ `migrate-settlement-document-versions.js --commit --allow-unresolved` (その版はどの決済にも採られない)。🚨 本番のコピーの版付けの dry-run で「決済 ID が 2 つ以上ある文書 0」を確かめてから初回を流す (必須)
- daily-sync は「財務を送った回か」を coordinator の小さな記録 `DATA_DIR/amazon-finance-coverage-last.json` (同じ DAILY_SYNC_RUN_ID・finance_pushed・finance_push_ok) で決め、送信がそろって終わった朝だけ突き合わせる (retry の見送りは終了コードで決まる = 人が直す理由だけなら exit 0)
- 🚨 **デプロイの前に本番の DB のコピーで測る (必須)**: コピー = `warehouse.db` と `company-db-push.db` (台帳) を同じ時点で写したフォルダ。`$env:DATA_DIR = '<コピー>'` で ① `migrate-settlement-document-versions.js` (dry-run) = 🚨 **最初の行の「[versions] 索引 … を作った: … ms」= 初回の schema の準備の索引の作成の時間 = その間の warehouse.db の書き込みの lock の長さ** (版付けの「1 取引の最長」には入らない・#1567 Codex R4 Medium 2。本番では pull の後の初回の initDB で起きる = 夜に daily-sync・retry と重ねない別の作業として、まずこの dry-run を単独で流す。🆕 本番の写しの実測 (2026-10-01 夜) = `idx_settle_lines_docver` **32,911 ms** = 約 33 秒 warehouse.db に書けない)・「決済 ID が 2 つ以上ある文書 0」「見出しの行が 2 行以上ある文書 0」 ② `--commit` = 最後の行の「所要 … 分・最大メモリ (RSS) … MB」と「1 取引の最長 … ms (取引の名前)」(🆕 本番の写しの実測 (2026-10-01 夜) = 所要 6.7 分・RSS 120 MB・文書 18・明細 4,428,382 行・**1 取引の最長 14,957 ms** (要約の作り直し (版 #7)・書き込みの取引 861) = warehouse の busy_timeout 5 秒を超える。15 秒までは許容と決めた (10/1)。🚨 **版付けの間は daily-sync・retry だけでなく倉庫の画面 (ポータル) などの warehouse.db への書き込みも busy でエラーになりうる = 深夜に単独で流す**。最長は版の登録・行の版の UPDATE・版の +1・要約の作り直し (1 版の全行を読み並べ digest を作る) の **全部** の書き込みの取引の最長 (#1567 Codex R5 Medium 1)。索引の作成は ① の別の行) ③ `amazon-finance-coverage-run.js --measure` (= dry-run + Render の鍵の取得 (GET だけ) と変わった注文の chunk の serialize まで。Render には書かない・送らない。SP-API の一覧とダウンロードは走る。🆕 R5 Medium 2) = 所要 **60 分以内** (daily-sync の上限 90 分の余裕)・最大メモリ **1,200 MB 以下** (下の根拠。🆕 本番の写し (2026-10-01 夜・head 2ca0906c) = 15.2 分・**1,560 MB で超えた** → メモリの持ち方を直した = V2 を 1 行ずつ並べ直す・dry-run は明細を持たない・決済のレポートは 1 本ずつ・台帳の指紋は 1 つずつ引く・受領記録は一時の SQLite・読み直す注文の記録を消す候補は記録のある注文だけ。直した後の目標 = `--measure` で **800 MB 以下**・送り直しの多い初回の実の回でも 1,200 MB 以下) ④ 空き容量 = 版付けの前後の warehouse.db と WAL の大きさの増えた分の 2 倍以上の空き。どれか満たさなければデプロイしない (相談)。🚨 **定期実行の前のハードゲート = スイッチ** (#1567 Codex R5 Medium 2): daily-sync・retry が coordinator を使うのは miniPC の `.env` に `CDB_FINANCE_COORDINATOR=1` があるときだけ (`apps/warehouse/finance-coordinator-switch.js`)。無ければ今までどおり「Amazon Settlement」(`fetch-amazon-settlements.js --days 14` = 書く取込・coverage の lease を取る) → 「CompanyDB財務(Amazon)」(`amazon-finance.mjs`) の 2 工程 = 本体がほかの PR の deploy で pull されても coordinator は動き出さない。dry-run (`--measure` でも) は送信・台帳・受領の記録の書き込みをしないので毎朝の実の `--full` と同じではない = daily-sync・retry の動いていない夜に実の回 (`amazon-finance-coverage-run.js`) を手で 1 回流し、**exit 0・所要 60 分以内・最大メモリ (RSS) 1,200 MB 以下** を全部満たしたときだけ、中原さんの指示の後に **同じ保守の枠の中ですぐ** `.env` に `CDB_FINANCE_COORDINATOR=1` を足す (下の手順・満たさなければ足さない = 相談)。🚨 **不合格のときも一方向**: 実の回は coverage の世代を作る = 足さなくても今までの 2 工程には戻らない = 翌朝から「Amazon Settlement」が ❌ で止まる (決済の取込と財務の送信が止まる = 正式な利益は出ない側・可用性の停止)。その日のうちに相談して (a) 上限・分け方を直して実の回をもう一度 か (b) 上限 90 分の中なら `.env` に足して coordinator で回す のどちらかを決める (決済のレポートは 85 日の固定の窓 = 数日の停止は次の回で取り戻せる)。不合格の見込みを下げるため、実の回の前に本番の DB のコピーで `--measure` が目安 (60 分・1,200 MB) を満たすのを確かめておく。🚨 スイッチは **一方向** (#1567 Codex R6 High): 手の実の回が coverage の世代を作った後は、スイッチが無い朝 (足す前に朝が来た・足した後に消えた) の daily-sync・retry・単独の入口は今までの取込・送り手を **生の表を書く前・送る前に ❌ で止める** (古い complete を残さない = 安全側・勝手に coordinator も起動しない)。証拠 = ローカル (財務の台帳・warehouse.db の coverage の世代) **と Render** (決済のそろいの行 = coordinator が一度でも updating を送った・状態は問わない) の両方。ローカルの DB を失くした・切り替えの前のバックアップに戻した・新しい DATA_DIR でも Render に行があれば ❌。**Render を読めない (網の失敗・404・401・409・5xx・形が違う) = 判定できない = ❌** (fail-closed・#1567 Codex R7 High 1)。両方とも「無い」と確かに分かったときだけ今までの 2 工程を許す = 🚨 切り替えの前でも、朝に Render が落ちていると今までの取込も止まる (可用性の代わりに正しさ・Render が戻れば次の回 / retry で動く)。足す前に朝が来たら、その朝の「Amazon Settlement」は ❌ (足せば翌朝から戻る)。🚨 `.env` は miniPC のリポジトリの直下の 1 つだけ・足す 1 行だけ書き、ほかの行を書き直さない (2026-09-30 に書き直して `CDB_DB_LIMIT_BYTES` など 4 つが消えた)。**Restart-Service は要らない** (daily-sync と自動再試行 Retry1〜3 は Task Scheduler が毎回 `cd /d C:\Users\bfaith\bfaith-portal` から新しい node で起こし、`dotenv/config` がそのつど `.env` を読む = 台帳 warehouse-daily-sync の where。常駐のサービス (WarehouseServer など) はこの工程を動かさない)。翌朝の「Amazon決済と財務」を見る。スイッチは一時物 (台帳 `cdb-finance-coordinator-switch`・2026-11-30 までに消して常に coordinator にする)。1,200 MB の根拠 = miniPC は物理 7.9 GB・2026-09-01 の実測で空きは 1.4 GB (手で開いたブラウザが残っていた時) 〜 2.8 GB・常駐のサーバーの合計 464 MB = 一番少ない空き 1.4 GB を超えず OS とページキャッシュに 200 MB 残す値。デプロイの後は、毎朝の記録 `amazon-finance-coverage-last.json` の `elapsed_minutes`・`max_rss_mb` (coverage の回 = --full) を最初の 1 週間見る
- 🚨 **`.env` に `CDB_FINANCE_COORDINATOR=1` を足す手順** (#1567 Codex R6 Medium 2・miniPC の PowerShell 5.1・`&&` は使えない・値は表示しない)。🚨 まだ流さない = 手の実の --full の合格の直後・中原さんの指示の後・同じ保守の枠の中:
  ```powershell
  Set-Location C:\Users\bfaith\bfaith-portal
  $pat = '^\s*CDB_FINANCE_COORDINATOR\s*='
  # ① 足す前: キーがまだ 0 行 (1 以上なら止めて相談 = 重ねて足さない)・行の数を控える
  (Select-String -Path .env -Pattern $pat).Count
  $before = (Get-Content .env).Count
  # ② 末尾が改行で終わっていなければ改行を 1 つ足してから、1 行だけ足す (ほかの行は書き直さない = 2026-09-30 の件)
  $raw = [IO.File]::ReadAllText((Resolve-Path .env).Path)
  if ($raw.Length -gt 0 -and -not $raw.EndsWith("`n")) { Add-Content -Path .env -Value '' -Encoding ASCII }
  Add-Content -Path .env -Value 'CDB_FINANCE_COORDINATOR=1' -Encoding ASCII
  # ③ 足した後: キーがちょうど 1 行・行の数はちょうど 1 つ増えた (最後の行に改行が無かったときも 1)
  (Select-String -Path .env -Pattern $pat).Count
  (Get-Content .env).Count - $before
  # ④ 新しい node のプロセスから '1' と読める (OK switch)・ほかの必須の鍵が残っている (yes / no だけ・値は出さない)
  node -r dotenv/config -e "console.log(process.env.CDB_FINANCE_COORDINATOR === '1' ? 'OK switch' : 'NG switch')"
  node -r dotenv/config -e "for (const k of ['MIRROR_SYNC_KEY','RENDER_MIRROR_URL','RENDER_PORTAL_URL','SP_API_CLIENT_ID','SP_API_CLIENT_SECRET','SP_API_REFRESH_TOKEN','CDB_DB_LIMIT_BYTES','CDB_FINANCE_ROW_BYTES','CDB_FINANCE_ORDER_BYTES','CDB_FINANCE_REPLACE_FACTOR']) console.log(k, process.env[k] ? 'yes' : 'no')"
  ```
  ① が 0・③ が 1 と 1・④ が `OK switch` で、④ の鍵が足す前と同じ (足す前にも ④ の 2 行目を流して控える。RENDER_MIRROR_URL / RENDER_PORTAL_URL はどちらか一方) なら済み。違えば `.env` を元に戻して相談。その日の後半と翌朝にも ④ を流して残っているかを見る (9/30 の件)
- **V1 と V2 の中身を先に比べる** (読むだけ・本番の DB のコピーで): コピーに `migrate-settlement-document-versions.js --commit` → `check-settlement-v1-v2.js` (SP-API の V2 を読んで採っている版の detail_digest と比べる)。版付けの後、`SELECT settlement_id, COUNT(*) FROM amazon_settlement_document_versions GROUP BY 1 HAVING SUM(header_count = 0) > 0 AND COUNT(*) > 1` が 0 件 (見出しの無い版とほかの版が同じ決済に無い) を確かめてから初回を流す
- **最後の行**: `✅` complete / `⚠️` 人が直すまで complete にしない (初期の印が無い・印の決済が無い・CANCELLED・窓の空白 など = exit 0) / `❌` 失敗 (retry) / 終了コード 3 = 取り込めない V2 (規則を足す)。財務のバックフィルの完了印の前 = 取込だけ (「財務 push: ⏭️」・ローカルと Render の両方で一度も回っていないときだけ)
- **初期の印を作る** (D-65 案 a・中原さんが Seller Central の「過去の決済情報」を書き出した後): JSON か CSV を `node apps/warehouse/amazon-finance-initial-marker.js --file 印.json` (dry-run = SQLite の採った見出しと突き合わせる) → `--queue` (順番待ちに積むだけ。次の coordinator の回が Render を updating にした後・同じ lease の下で新しい epoch として入れる = 印を変える前に古い complete を無効にする・#1567 Codex R1 High 2。回の途中で積まれた印・manifest の後に変わった印があれば complete にしない)。🚨 coordinator の回が動いている間 (生きている lease) は `--queue` も手のファイルの `--queue` も **積まずに拒む** = 「回が終わってから積み直す」(#1567 Codex R2 High 2)。coordinator は complete の POST が終わるまで lease を持ち、POST の後にもう一度 印・順番待ち・source_revision を読み直して、変わっていれば新しい世代の updating で complete を取り消す (❌)。形 = `{ evidence_kind, verified_from (起点以前), verified_through, captured_at (時差つき), settlements: [{ settlement_id, start, end (日付 = JST の日), total (円), currency, report_id? }] }`。🚨 CANCELLED・窓の空白が出た・印の決済を直す = 新しい印を作り直す (積み上げは新しい印の後の回だけ)
- 🚨 **失敗した順番待ちは complete を止め続ける** (#1567 Codex R9 High・永続の状態を毎回読む = 失敗した回を忘れない): ① 入れられなかった初期の印 (順番待ちの `failed_at` = 積んだ後に中身が壊れた・digest が違う。二度と入れ直されない) = 理由 `marker_queue_failed` ② 取り込めていない手のファイル (保管物が無い・hash が積んだときと違う・形が違う = 毎回読み直す) = 理由 `manual_file_not_ingested`。どちらも人が直す理由 (⚠️・exit 0・retry しない)。
  - 見る: `node apps/warehouse/amazon-finance-initial-marker.js --list` / `node apps/warehouse/amazon-settlement-manual-file.js --list` (番号 #・理由・解決の印)
  - 直す (どれか): 印 = Seller Central の決済の一覧から印を作り直して `--queue` (より新しい印が入れば外れる) / 手のファイル = 保管物を直す (次の回で入る) か、同じ決済の正しいファイルを積み直す (同じ決済のより新しいファイルが入れば外れる)
  - 諦める (解決の印・理由は必ず・coordinator の回が動いている間は付けない): `node apps/warehouse/amazon-finance-initial-marker.js --resolve-failed <番号> --note "理由"` / `node apps/warehouse/amazon-settlement-manual-file.js --resolve <番号> --note "理由"` (解決の印の付いたファイルは次の回から取り込まない)。付けた後の最初の coordinator の回で complete に戻れる (ほかの理由が無ければ)
- **SQLite に無い決済を手で入れる** (API は 90 日より前を返さない): Seller Central から落とした V2 (か V1) を `node apps/warehouse/amazon-settlement-manual-file.js --file x.txt` (dry-run = 決済 ID・期間・合計の一致) → `--queue` (順番待ち)。次の coordinator の回が manual_csv の版として入れる
- **手で流す**: `node apps/warehouse/amazon-finance-coverage-run.js --dry-run` (書かない・送らない・判定の理由を出す)。過去の行の版付けは `node apps/warehouse/migrate-settlement-document-versions.js --commit` (夜に手で・coordinator は流さない。所要時間と最大メモリを最後に出す。🚨 本番の写しで 1 取引の最長 約 15 秒・初回の索引 約 33 秒 = その間は倉庫の画面などの warehouse.db への書き込みもエラーになりうる = 深夜に daily-sync・retry・ポータルの作業と重ねず単独で)。取込・送り手の単独の実行は **スイッチ次第**: `CDB_FINANCE_COORDINATOR=1` がある = dry-run だけ (`fetch-amazon-settlements.js` は常に dry-run・`amazon-finance.mjs --incremental/--full` は送らない) / 無い = 今までどおり書く・送る (取込は coverage の lease を取る) — ただし coordinator に一度でも切り替えた環境では、書く前・送る前に ❌ (`FINANCE_SWITCHED_BACK` = .env を確かめる・勝手に coordinator を起動しない)。`--from/--to` のバックフィルはどちらでも token の無い chunk = Render は complete を無効にする (要約は ⚠️・切り替え済みの環境は ❌ exit 1・#1567 Codex R6 Medium 1)。🚨 `--from/--to` も **送る前に** 同じ門 (ローカル + Render) を通る = 切り替え済み・判定できないなら送らずに ❌ (送った後の exit 1 では、Render の受け口が #1561 の前に戻っていると古い complete を防げない・#1567 Codex R7 High 2) = 財務のバックフィル (`--from/--to` → `--mark-backfilled`) は coordinator を回す前にだけ流せる。🚨 スイッチが無いときの daily-sync・retry の工程は **入口・引数・時間の上限が master (#1567 の前) と同じ** というだけで、中身は D-66 の文書の版・85 日の固定の窓・coverage の lease・V2 の版の保存に変わっている (完全に同じ動きではない)
- 試験 = `node scripts/test-amazon-finance-coverage-run.mjs` (本物の router + PR #1561 の受け口を PGlite で) と `node apps/warehouse/test-settlement-document-versions.js` (daily-sync の冒頭でも)

### Amazon の利益の mart の下ごしらえ (0047。D7b-1a。設計 = AI_reference『CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』§3.2・§3.5b・§3.6・§3.7)

D7b-1 のうち coverage (決済のそろい・coordinator・レポートの一覧・lease・文書の版) を除いた部分。coverage は D7b-1b・利益の関数は D7b-3。

- **分けられない決済の部品の 4 列** (`core.order_finance_daily`・既存の行は 0): `unclassified_component_count` (0 でない「分けられない」生の部品の数・全部の line_kind) /
  `unclassified_mapped_jpy` (その符号つきの合計・決済の符号のまま) / `unclassified_abs_jpy` (**部品ごとの** 絶対値の合計) / `unmapped_component_count` (unmapped_jpy に入った 0 でない部品の数)。net には入らない
  - 🚨 **集約の後では復元できない** (+100 と −100 は金額 0 でも部品 2・絶対値 200) → 送り手の変換 (`amazon-finance-transform.mjs` の `classifyComponent`) が生の部品を分類するときに数える。D7b-3 の正式な利益は **金額でなく数** で止める (`finance_unclassified`)
  - 分類の優先順位 (§3.7・部品を 1 回だけ消費): ① 月の手数料の行の手数料の材料 (other_amount + item_related_fee = `account_fee_amount_jpy`) → ② not_account_fee の行 → ③ unknown の行 → ④ **どれにも消費されない = 分けられない** → ⑤ unmapped_jpy (別に数える)。
    SKU の行の分けられない = `misc_fee` / `other_fee` (MFNPostageFee を含む) / `other_amount` の列に入った部品 (SKU の行ではこの 3 列の和 = `unclassified_mapped_jpy`)。月の手数料の行の分けられない = 手数料の材料以外 (price (税も)・promotion・misc_fee・other_fee = fail-closed)
  - 形の確かめ (`order-finance-checksum.mjs`・送り手と受け口が同じ): 4 列は全部あるか全部無いか・数 0 ⇔ 絶対値 0・|符号つき| ≤ 絶対値・今の形なら unmapped の金額 ≠ 0 は部品 ≥ 1・SKU の行は 3 列の和 = 符号つき / 絶対値 ≥ 3 列の絶対値の和 (ほかは下の「今の形の版の行の等式」)。
    表の CHECK は 3 つ = 数と絶対値の形 (`ck_order_finance_daily_unclassified` / `_unmapped_count`・どの版の行にも・既存の行 = 0 で満たす) と今の形の版の行の等式 (`ck_order_finance_daily_class_form`・旧い版の行は対象の外)
  - **旧い形の送り手** (0047 の前 = 4 列の鍵が無い) は受け口がそのまま受ける: 集合の指紋は旧い列 (`LEGACY_CONTENT_COLUMNS`) で計算し、正規化した行からも 4 列を外す (= Render の deploy と miniPC の deploy の間も 400 にせず、受領記録の指紋も変わらない = 「Render が復元された」と誤って台帳を空にしない)。保存は 4 列 = 0
  - **今の形の行が 0047 の適用前に届いたら 409 `NOT_MIGRATED`** (0043 の apply は知らない鍵を黙って捨てる = 4 列が落ちたまま受領記録の指紋には入り、送り直しても 'same' で直らない)
  - 変換の版 = `amazon_finance_v2` (全部の注文の payload が変わる = **全部の送り直しが要る**。版が変わると `--incremental` も全部を選ぶ = 下の手順の夜の `--full` を先に済ませる)
  - 🚨 **版と行の形を結び付ける** (#1554 Codex R1 High): 今の形の版 = `amazon_finance_v2` 以上 (`_名前` が付いても同じ・JS `versionHasClass` = SQL `core.finance_version_has_class`) ⇔ 全部の行に 4 列 (null 不可)。違えば 400 (SQL の apply も例外)。空の集合 (墓石) はどちらの版でもよい
  - 🚨 **旧い版への戻し (downgrade) を拒む**: 受領記録が今の形の版になった注文を旧い版 (4 列なし) で置き換えると 4 列が 0 に戻る (分けられない部品が消えて正式な利益が fail-open) → 受け口が chunk ごと **409 `DOWNGRADE`** (送り手は ❌)・SQL の apply も例外。**miniPC を旧いコード (amazon_finance_v1) に戻さない**。
    受け口の事前の照会の後に別の送信が受領記録を v2 にした競合でも、SQL の downgrade の例外は行の failed に吸収せず **chunk 全体を rollback して 409** (`ingest/chunk.mjs` の `fatalRowError`。ほかの受け口は今までどおり行ごと)
  - 🚨 **今の形の版の行の等式** (#1554 Codex R2・JS の形の確かめと表の CHECK `ck_order_finance_daily_class_form` の両方): unmapped の金額 ≠ 0 なら部品 ≥ 1 / SKU の行 = 分けられない 3 列の和 = 符号つき・絶対値 ≥ 3 列の絶対値の和 /
    **月の手数料の行 = `net = account_fee_amount + unclassified_mapped + unmapped`** (4 列を 0 と偽って分類の漏れた金額を隠せない) / not_account_fee・unknown の行 = 分けられない部品 0。旧い版の行は対象の外。
    SKU の行と月の手数料の行は **部品が別の「箱」の間で相殺しても隠せない** (#1554 Codex R3): SKU の行 = 0 でない分けられない列の数 ≤ 部品の数 / 月の手数料の行 = 「材料の入らない 12 列」と「材料の入りうる 8 列 (commission・fba_fulfillment・fba_storage・chargeback 2 つ・points・other_fee・other_amount) の和 − account_fee_amount」の
    絶対値の和 ≤ `unclassified_abs_jpy`・0 でないものの数 ≤ `unclassified_component_count` (例 storage 行の misc_fee +3・promotion −3 を 4 列 0 と申告できない = 正しくは部品 2・符号つき 0・絶対値 6)。
    🚨 **同じ箱の中の相殺は受け口では復元できない = 送り手の変換の試験で守る** (#1554 Codex R4)。箱 = SKU の行の 3 列のそれぞれ / 月の手数料の行の 12 列のそれぞれ (例 misc_fee の +3 と −3 = 列は 0) と **8 列の合計の残差 1 つ**
    (例 材料 −100・other_fee +3・other_amount −3 = 残差 0 = 別の列の間の相殺でも見分けられない)。試験 = `scripts/test-company-db-amazon-finance.mjs` の手で書いた固定の期待値 (同じ列の中・残差の箱の中・SKU の行の別の列の間) と乱数の決済の行 400 注文
  - CHECK (3 つ) は 0047 で `NOT VALID` (新しい行には効く)・既存の行の検査は **0048** (`VALIDATE CONSTRAINT` = 読み書きを止めない lock。59 万行の検査を 0047 の ACCESS EXCLUSIVE の中でしない)
- **`mart.finance_daily_sku_range(会社, モール, scope, from, to)`** = **日 × 正規化 seller SKU (`core.norm_code`) の子の粒度** (§3.2・R13 H1)。D7b-3 の利益の関数が計算のときに **今のマスタ** で出品に結び直してまとめる材料 (D-64)。
  今の `mart.finance_daily_range` (0045) は画面・突き合わせのため残す (戻りの型も変えない)
  - 列 = 0045 の全部の金額と数量の列 + `seller_sku_norm`・`received_listing_ids` (受け取りのときの listing_id・ID の昇順・診断だけ)・`received_listing_unresolved_count` (受け取りのとき未解決だった行の数)・`net_jpy`・`unmapped_jpy`・4 列・`refund_units_status`・`units_refunded_customer_unrounded` / `units_a_to_z_refund_unrounded` (丸める前の返品数・小数 6 桁)・`refund_unestimated_jpy` (推定できない返品の額)
  - 0045 との違い: 粒度 / `closing_fee_jpy` = −Σ closing_fee で `profit_before_cogs_jpy` からも引く (§3.5b・0045 は固定の 0。今の決済には 0 = 金額は同じ) / net・unmapped・4 列は **決済の符号のまま** (0045 の費用を正にした列とは向きが逆)
  - 返品数は 0045 と同じ「計上日の月 × **受け取った** seller SKU の単価」で子ごとに計算してから正規化 SKU にまとめる (合計は 0045 と同じ)
  - `refund_units_status` (子の状態・1 つの子に表記の違う seller SKU が 2 つ以上なら弱い方): `no_refund` (本体の customer / A-to-z の返金なし) / `estimated_monthly_unit_price` / `estimated_partial_month_unit_price` (その月がまだ終わっていない = 単価が動く) / `unit_price_missing` (返金があるのに単価なし = 返品数 0 個・丸める前は null・額は `refund_unestimated_jpy`)。
    🚨 **partial の判定は 0050 で coverage 基準に置き換えた** (計上日の月の全部の日の決済がそろっていない = `core.finance_month_settled` が false。下の「決済のそろい」の節)。0047 の当面の「今日」基準はもう使わない
  - 契約: `from <= to`・最大 400 日 (両端を含む)・違えば例外 (22023 `invalid_input`)。期間の月だけを読む・関数の中だけ nested loop を使わない (0045 と同じ)
- 試験 = `node scripts/test-company-db-finance.mjs` (0047 の節: 形の確かめ・表の CHECK・旧い形 / 今の形の受け口・版と行の形の結び付け (JS と SQL の規則の突き合わせ)・旧い版への戻しは 409 / 例外で 4 列が残る・0047 の適用前の NOT_MIGRATED と適用後の既存の行・手で計算した子の値・返品の状態の 4 つ・今の関数と同じ期間の合計が一致・契約) と
  `node scripts/test-company-db-amazon-finance.mjs` (決済の行から: 相殺する +100 / −100 と misc_fee・MFNPostageFee = 部品 4・月の手数料の行の保存則・二つの関数の合計の一致・二重の実装の一致は既存の列のまま)

**マージの後の手順 (二段。🚨 まだ流さない = migrate は中原さんの指示の後)**。台帳 (jobs-registry の warehouse-daily-sync) にも同じ注意を書いた
1. マージ → Render は自動で deploy (旧い形の送り手の payload はそのまま受ける = 朝の daily-sync は今までどおり)。**miniPC の本体 (`C:\Users\bfaith\bfaith-portal`) はまだ pull しない** (pull すると v2 の送り手が有効になり、0047 の前は 409 `NOT_MIGRATED` = ❌・0047 の後でも朝の `--incremental` が全部 (約 51 万注文) を選んで 30 分の上限に当たりうる)
2. 新しいコードを **本番で使っていない worktree** に取り、そこから migrate (0047 → 0048)
3. Render の新しい版が動いていることを確かめる (`/order-finance/status` が返る・Render の Events の最新の deploy が master の新しいコミット)
4. **同じ夜に** miniPC の本体を pull (= v2 の送り手が有効) → `--full` (daily-sync の 07:00〜09:10 を避ける) → `--reconcile --all`
5. 完了の条件 = `--full` の最後の行が ✅ (failed 0・整形できない 0・stale 0)・下の SQL で `lines > 0` の受領記録が全部 `amazon_finance_v2`・`--reconcile --all` が一致

```
# ② 本番で使っていない worktree から migrate (miniPC の PowerShell。.env は本体の 1 つを読む = -r dotenv/config に DOTENV_CONFIG_PATH)
cd C:\Users\bfaith\bfaith-portal
git fetch origin
git worktree add C:\tmp\d7b1a origin/master
cd C:\tmp\d7b1a
npm ci
$env:DOTENV_CONFIG_PATH = 'C:\Users\bfaith\bfaith-portal\.env'
node -r dotenv/config scripts\company-db\migrate.mjs --dry-run                 # 0047 と 0048 だけが出ること
node -r dotenv/config scripts\company-db\migrate.mjs                           # 0047 → 0048 (applied=2)
# ④ 同じ夜に本体を pull して全部を送り直す
cd C:\Users\bfaith\bfaith-portal
git pull
node apps\company-db\push\amazon-finance.mjs --full                            # 変換の版 v2 で全部を送り直す (約 51 万注文)
node apps\company-db\push\amazon-finance.mjs --reconcile --all                 # 全期間の突き合わせ (既存の列が SQLite と一致のまま)
# 後片付け
git worktree remove C:\tmp\d7b1a
```

```sql
-- 送り直しの進み (版ごとの注文の数。lines > 0 が全部 amazon_finance_v2 になれば済み・空の集合の墓石は旧い版のままでよい)
select transform_version, lines > 0 as has_lines, count(*) from core.order_finance_receipts where company_id = 1 and mall = 'amazon' and scope_key = 'jp' group by 1, 2 order by 1, 2;
-- 分けられない部品のある SKU の行 (D7b-3 で正式な利益が null になる行)
select economic_date_jst, seller_sku_norm, unclassified_component_count, unclassified_mapped_jpy, unclassified_abs_jpy, unmapped_component_count
  from mart.finance_daily_sku_range(1::smallint, 'amazon', 'jp', '2026-09-01', '2026-09-30') where unclassified_component_count > 0 or unmapped_component_count > 0;
```

### 決済のレポートの一覧 (miniPC の SQLite・D7b-1b の下ごしらえ・Company DB構想 13 §3.1 / D-65)

- 書き手 = `apps/warehouse/fetch-amazon-settlements.js` (部品 `apps/warehouse/amazon-settlement-inventory.js`)。表 = warehouse.db の `amazon_settlement_report_inventory_runs` (回) / `amazon_settlement_report_inventory` (report ごと)。読み手 = coverage の判定 (`amazon-finance-coverage.js`)。🆕 2026-10-01 (#1567 Codex R4) から **取込の一覧も同じ固定の窓** = この窓に出ない report (85〜90 日前に作られた・回の開始の後に作られた) は取込まない (前 = 取込は日時の境なし = Amazon の既定の 90 日)
- 呼ぶ順 = 取込の一覧の要求 (証拠の一覧と同じ固定の窓 = `createdSince` 回の開始の 85 日前・`createdUntil` 回の開始。今までの取込の接続) → 取込のダウンロードのループ (結果は report ごとにメモリ) → **ループが全部終わった後** に別の getReports で一覧を取る (窓 = `createdUntil` = 回の開始の時刻・`createdSince` = その 85 日前・時間の上限 120 秒) → 一覧の行・取込の結果・完了を **1 つの取引** で書く。最後のページまで取れない・応答の形が違う・時間切れ = `last_page_reached = 0` / `list_error` (取込の結果・終了コードは変わらない)
- 一覧の要求は **専用の SP-API の接続** (amazon-sp-api の `auto_request_tokens: false`・`auto_request_throttled: false`・`retry_remote_timeout: false`・要求ごとに残り時間の `timeouts`) = 期限で socket を破棄し、429 でも待って再試行しない。アクセストークンは最初に全体の期限の中で 1 回だけ取り、403 (expired) でも取り直さず一覧の失敗にする (取込が済んだら node が自分で終わる)。取込の接続の設定は変えない
- 🚨 **途中で落ちた回**: 取込が例外で止まった回は一覧を記録するが `completed_at` は null・`ingest_error` に理由。**daily-sync の時間切れなどで kill された回は一覧の記録が無い** = その回は coverage の証拠に使えない (安全側・次の回で取り直す)
- 🚨 **記録の失敗**: 一覧の行・取込の結果のどこかを書けなければ取引ごと戻し、見出しだけを `record_error` つき・`completed_at` null で書く (行は無い = 「成功した回」に見せない)。見出しも書けなければ回は残らない
- 回の所属 = `company_id` / `mall` / `scope_key` (今は 1 / `amazon` / `jp` 固定)
- 🚨 **`evidence_epoch` の規則**: 初期の印 (D-65 = Seller Central の決済の一覧を書き出した印) を作るたびに採番し、その後の回に入れる。**`evidence_epoch` が null の回はどの印の鎖にも属さない = 期待の report の集合の積み上げに使わない** (今の回は全部 null)。印を作り直したら、積み上げは新しい epoch の回だけ
- 回の完了 = `completed_at` がある・`last_page_reached = 1`・`list_error` / `ingest_error` / `record_error` が null。これを満たさない回は後の coverage で「失敗した回」として扱う
- 試験 = `node apps/warehouse/test-settlement-inventory.js` (一時 DB・SP-API は差し替え・daily-sync の冒頭でも「Settlement 一覧テスト」として走る)

### 決済のそろい `core.finance_coverage` (0050。D7b-1b-2 = Render 側。Company DB構想 13 §3.1・D-65・D-66)

「Amazon 側で決済がそろった」と「その全部の行が Company DB に入った」の **両方** を満たす最後の日 = `complete_to`。これが来るまで 0049 の正式な利益は全部 null。
**値を作って送るのは miniPC の coordinator (D7b-1b-3・後の PR = 一覧・初期の印・文書の版・lease・SQLite の `source_revision`)**。この PR は受け皿と受け口と、財務の chunk を世代に縛るところだけ。**今の送り手 (daily-sync) は変えていない = coverage を送らない**。

- 表 = 会社 × モール × scope × source の **1 行 = 今の世代の状態** (`state` = `updating` / `complete`・`generation` = coverage 専用の連番・`run_token`)。列は §3.1 の一覧 (manifest = `complete_to`・`settlements_through`・`source_revision`・見出し・受領・一覧・初期の印・採った文書・証拠の鎖・期待の report の数と digest) + `request_hash`・`completed_at` + 無効の印 (`invalidated_at` / `invalidated_reason`)。
  CHECK = complete なら manifest が全部そろう・`complete_to` = `settlements_through` の JST の日の前日・無効の印の組
- `core.finance_coverage_state(会社, モール, scope, source)` を **差し替えた** (0049 の差し込み口。同じ形): `complete_to` と `source_revision` = **complete かつ policy の指紋が今と同じときだけ** / `generation` = 今の世代 (updating でも) / 行なし = 全部 null
- 🚨 **policy の指紋** `core.finance_policy_fingerprint(会社, モール, scope)` = その会社 × モール × scope の `finance_source_policy` の全期間 (source・period_from・period_to) の正規の JSON `{"format":"fpf-v1","policies":[…]}` の SHA-256 (JS の `policyFingerprint` と同じ値)。
  complete の manifest に `policy_fingerprint` (送り手が回の始めに status で読んだ値) を入れ、今の指紋と違えば 409 `POLICY_MISMATCH`。保存した指紋が **後で今の policy と違えば** (起点を広げる・狭める・source を変える・終わりを付ける) complete_to は null と読む
  (前の complete を新しい期間に流用しない = 財務も証拠も無い日を確定の 0 にしない・#1561 Codex R2 High)。**policy を変えたら、次の回の updating → complete (新しい指紋) でやり直す**。policy を元に戻せば (同じ指紋) 前の complete がまた効く
  - complete の受け口は、policy の更新の trigger (0012) と同じ advisory lock を取ってから、**起点・source の有無・指紋を 1 つの文 (同じスナップショット)** で読む (`core.finance_policy_snapshot`・#1561 Codex R3 High 2)。
    前は起点と指紋を READ COMMITTED の別々の文で読んでいた = 間に policy が変わると、古い狭い起点で検査して新しい広い指紋を保存できた
- 🚨 **月の決済がそろったか** `core.finance_month_settled(会社, モール, scope, 月)` = その月の **全部の日** で policy がちょうど 1 つ・その日の source の coverage の complete_to ≥ その日 (1 日でも欠ければ false)。
  返品の単価は月 × SKU の全部の日 (全部の source) の平均 = 月の途中で policy の source が切り替わると、返品の日の source だけ見ては足りない (#1561 Codex R3 High 1)
- 🚨 **`mart.finance_daily_sku_range` (0047) と `mart._amazon_profit_rows` (0049・本番に入っている) を 0050 で差し替えた**: 返品の状態の `estimated_partial_month_unit_price` の判定を
  0047 の当面の「今日」基準・0049 の「返品の日の source の complete_to ≥ 月末」から **`core.finance_month_settled` でない月は partial** に (§3.2・#1561 Codex R2 Medium・R3 High 1)。
  2 つとも同じ関数 (矛盾しない)。引数・戻り・金額の式は元のまま。**coverage が complete になるまでは返品のある行は全部 partial** (0049 の正式な利益は元々 null)
- **状態の移り方** (受け口 `ingest/finance-coverage.mjs` が 1 取引・財務の chunk と同じ advisory lock の中で):
  - 古い世代 → `stale` (何もしない) / 新しい世代は **updating からだけ** (直接の complete = 409)。新しい世代の updating は前の complete を無効にする (manifest の列を空に)
  - 同じ世代・同じ token の updating の再送 = `same` / 違う token = 409 / 同じ世代の complete → updating = 409
  - 同じ世代・同じ token の updating → complete は **1 回だけ**: lock → **今の受領記録から receipt digest を計算** → manifest と違えば 409 `RECEIPT_MISMATCH` (`detail.render` = Render の数・行の数・digest) → complete
  - 同じ世代の complete の再送 = `request_hash` が同じなら `same` (応答だけ失われた)・違えば 409
- **receipt digest** (`apps/company-db/finance/coverage-manifest.mjs` = 送り手と受け口が同じ関数): 会社 × モール × scope の `lines > 0` の受領記録 (墓石は除く・疑似注文を含む・期間では絞らない) を
  `mall_order_no` の **UTF-8 のバイトの順** (PostgreSQL は `collate "C"`) に並べた正規の JSON `{"format":"frd-v1","receipts":[{lines, mall_order_no, set_checksum, transform_version}…]}` の SHA-256 + 数 + 行の数。
  Render は cursor で 1 万行ずつ読んで 1 行ずつ hash に足す (51 万注文でも手元に全部を持たない)。run ID・token は入れない
- **request_hash** = 正規の JSON `{format: "fcr-v1", company_id, mall, scope_key, source, state: "complete", generation (10 進の文字列), run_token, manifest の全部}` の SHA-256。送り手が付けたら受け口の計算と比べる (違えば 400)
- manifest の形 (400): 全部の列が必須・日時は UTC の `YYYY-MM-DDTHH:MM:SSZ`・bigint と ID は 10 進の文字列 (`source_revision` は数でもよい)・digest は 64 桁の小文字の 16 進・未来の時刻は不可 (+10 分)・
  `selected_documents_count = headers_count`・`receipt_lines ≥ receipt_count`・知らない鍵は不可 / `settlements_through` が policy の起点 (その source の `period_from` の JST 00:00) より後でなければ 400 /
  **`evidence_chain_from` が policy の起点以前でなければ 400** (UTC の瞬間で比べる = 証拠の鎖が起点まで届いている・#1561 Codex R1 High 2。鎖の窓が途切れずにつながることは coordinator = D7b-1b-3 が確かめる) / policy に無い source = 409 `NO_POLICY`
- 🚨 Render が確かめられるのは **受領記録 (receipt digest) と形だけ**。一覧・初期の印・採った文書・期待の report の集合は miniPC の SQLite にしか無い = 送り手の申告を保存するだけ (D7b-1b-3 が作る・試験する)
- **財務の chunk を世代に縛る** (§3.1 R10 H1・R23 M5):
  - 全部の財務の chunk・updating・complete が **同じ advisory lock** (`company-db:order-finance:会社:モール:scope`) を取引の最初に取る (待つ。chunk は lock_timeout 10 秒・coverage は 20 秒で 503 `LOCKED` = 送り手がやり直す)
  - chunk の body に `coverage_generation` と `run_token` (両方) = **その世代・その token の coverage が updating のときだけ適用** (complete の後・別の世代・別の token・行の source が coverage の source と違う・
    **置き換え / 墓石で消える既存の行の source が coverage の source と違う** = 409 `COVERAGE_MISMATCH`・何も消さない・書かない。#1561 Codex R1 High 1)
  - 🚨 **付いていない chunk (今の送り手) は今までどおり受ける (互換)**
  - 🚨 **どちらの chunk も、受領記録を 1 つでも変えたら (applied > 0・墓石と置き換えを含む)、その会社 × モール × scope の complete を全部 updating に落とす** (無効の印・応答の `coverage_invalidated`)。
    受領記録と receipt digest は source で分かれていない = fail-closed。理由 = `untokened_finance_write` (token の無い chunk) / `other_coverage_finance_write` (token 付きの chunk = 落ちるのはほかの source か別の世代の complete。
    例 過去の source A が complete・今の source B が updating で B の token の追加・墓石 → A は updating)。無効の印のある世代は complete に戻れない (409) = 次の世代の updating から。same / stale (受領記録が変わらない) では落とさない
  - 🚨 **coordinator (D7b-1b-3) ができたら token の無い chunk は拒む契約にする** (今の daily-sync の送り手を止めないために当面は受ける)
  - 0050 の前 (Render の deploy が migrate より先): token の無い chunk は今までどおり・token 付きの chunk と coverage の口は 409 `not_migrated`
- 受け口 (`router.mjs`・鍵は server.js の `/apps/company-db/sync/order-finance` の前方一致に入る):
  - `POST /apps/company-db/sync/order-finance/coverage` `{ state, mall, scope, source, generation, run_token, manifest? (complete だけ), request_hash? }` → `{ status: applied | same | stale, state, generation, current_generation?, complete_to?, receipt? }`
  - `GET /apps/company-db/sync/order-finance/coverage/status?mall&scope&source[&receipts=1]` → `{ coverage: 行 | null, effective: core.finance_coverage_state の値, policy: { fingerprint, rows }, receipts? }` (送り手が回の始めに Render の今の世代と policy の指紋を読む・世代と版は 10 進の文字列)
- 試験 = `node scripts/test-company-db-finance-coverage.mjs` (PGlite・本物の router を HTTP でも: 状態の移り方の全部の場面・receipt digest の一致 / 不一致 と JS / cursor の一致・manifest の形・token 付きの chunk の 409・順序の逆転・3 つの受け口が同じ lock・token 無しの chunk で complete が落ちる・0049 の mart が正式な値を出す・0050 の前)。
  🚨 PGlite は 1 接続 = 「lock を持ったまま止まった chunk を新しい世代の updating が待つ」の 2 接続の待ちは書けない (同じ lock を持つことと、lock の前で止めた chunk が 409 になることで確かめた)

**マージの後の手順 (🚨 まだ流さない = migrate は中原さんの指示の後)**:
Render の deploy が先でも今の送り手は今までどおり (token の無い chunk は 0050 の前も受ける)。migrate は本番で使っていない worktree から dry-run → 本適用 (0050 だけが出ること)。
coverage が complete になるのは D7b-1b-3 (coordinator) が動いた後 = それまで正式な利益は全部 null のまま。

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
- **売上日次 (mart.sales_daily) に公開する** (2026-09-28〜)。9/26〜27 は止めていた = モール負担が null の注文を mart が 0 として「払った額」を出す (約 1 割の注文で多く出る。#1465 Codex R1)。
  #1476 で VPS の orderInfo の Field に TotalMallCouponDiscount を足して取込が取り、2025-01〜2026-09 を取り直して null が残っていないのを確かめてから開けた (見張りの W9 に Yahoo・W8 は売上も・W6 の公開の確認にも入る = CHECKS_VERSION v15)。
  🚨 **作り直しの前に確かめる** (`salesDailyBlocker`): ① その回の注文の送信が全部通った ② raw_yahoo_orders にモール負担 null の注文 (2025-01 以降) が無い ③ Company DB に、取消でないのにモール負担 null の注文が無い (`GET …/orders/status` の `counts.mall_coupon_unknown`)。
  1 つでも外れたら作り直さず ❌ (証跡の sales.ok = false → 見張りの W9 が breach。売上日次の状態がまだ無ければ blocked)。直し方 = VPS の Field と取込を確かめ、その期間を `yahoo-orders.js backfill <from> <to>` で取り直す → 翌朝の push が送り直す。
  初めて開けた日: `node apps/company-db/push/mall-orders.mjs --mall yahoo --refresh-sales --all` で全部の日を作る (手で流す作り直しも ③ を確かめる)。打ち切られたら `--all` を外して流し直す (同じ回の続きから)。
  作り終えたら `node apps/company-db/push/mall-orders.mjs --mall yahoo --check-sales --days 400` で食い違い 0。作る前は W8 が未公開の日を標本から外し・W6 が公開の穴で判定を保留する。au PAY も同じ形 (モール負担 null) が残っている = 宿題
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

## 広告費の日次 (0035。Company DB構想 11 の ②)

設計の正本 = AI_reference『CompanyDB構想/11_広告費の日次_設計_20260927.md』(§5 = Codex 設計レビュー D1)。まず **Amazon の SP** だけ (楽天 RPP・au PAY は取込が動いてから同じ受け皿に mall / ad_type を足す)。元 = miniPC の warehouse.db `fact_ad_spend` (日 × キャンペーン × 対象) と `ads_fetch_days` (取込が「その日を最後まで取れて置き換えた」記録。#1483 の作り直しで入った)。

- **表**: `core.ad_spend_days` = 日の状態 (取得の世代 `source_generation` = 取込がレポートを頼んだ時刻 ms・`source_report_id`・指紋・行数・費用の合計) / `core.ad_spend_daily` = 日 × キャンペーン × 粒度 (sku / asin / none) × 対象の行。読む口 = `mart.v_ad_spend_daily` (日 × 出品。結べなかった行は粒度 + コードで分ける)
- **金額** = `ad_cost` / `ad_sales_1d` は numeric(14,2) (Amazon の費用は円未満の端数がある = 45.7 万行中 9.2 万行。03 §10 の「`*_jpy` は bigint」に当たらないよう名前に _jpy を付けない)。送り手は REAL を小数 2 桁の十進の文字列にして送り、2 桁より細かい値は ❌ (黙って丸めない)
- **広告経由の売上・数量は 1 日の帰属** (`sales1d` / `unitsSoldClicks1d`)。分からない行は null (0 にしない)。mart は `sales_unknown_rows` / `units_unknown_rows` で数を出す
- **送り手 (miniPC)**: `apps/company-db/push/ad-spend.mjs --mall amazon --days 35` (昨日から 35 日)。daily-sync の「Amazon Ads (SKU)」が成功した朝だけ走る (失敗した朝は ⏭️ で retry に載り、取込の再試行が成功した回に送る = `UPSTREAM_OF`)。
  **送るのは取得の記録がある日だけ** (記録の無い日 = 取れていない・作り直しより前 → 送らず ⚠️。「0 円の日」を作らない)。記録があって行が 0 の日は 0 行で送る (空の集合で置き換え)。行と記録は同じ読み取り取引で読み、記録の行数・費用の合計と合わない日は ❌。
  **台帳を持たない** = Render に日ごとの世代・レポート・指紋を聞き、同じ日は送らない。送った後に毎回 relink (マスタが後から増えた SKU の行を出品に結び直す)。証跡 = `ad-spend-amazon` (昨日の日が手元にあるか・Render に届いたか・世代)
- **受け口 (Render)**: `POST /apps/company-db/sync/ad-spend/day` (`apps/company-db/ingest/ad-spend.mjs`)。1 日 = 1 要求 = 1 取引で、その日の行を消して入れ直す。
  🚨 **世代で古い要求を拒む**: Render より古い世代 = `stale` (書かない) / 同じ世代・レポート・指紋 = `same` / 同じ世代で違う = 409 / 新しい世代で中身が同じ = `refreshed` (世代だけ進める) / 新しい世代で中身が違う = 置き換え。
  指紋は受け口が届いた行から計算し直す (送り手と同じ関数 `adSpendChecksum`・版 `ad-v1`)。出品は粒度 sku の行だけ `core.resolve_listing_id` で結ぶ。`GET …/ad-spend/status` / `POST …/ad-spend/relink`
- 🚨 35 日より古い日の取り直し・欠けは毎朝の送信では戻らない → 下の「過去の日を入れ直す」
- **古い取込の行** (中原さん 2026-09-27「過去分が取れないなら印を付けて入れる」): 2026-02-05 から取得の記録の最初の日の前日までは、作り直す前の取込 (UPSERT だけ・SKU も ASIN も無い行は捨てていた) が書いた行しか無い (Amazon は約 95 日より前を取り直させてくれない)。
  `--legacy` で 1 回だけ送る。**印 = `core.ad_spend_days` の `source_generation = 1` + `source_report_id = 'legacy:upsert-v1'`** (受け口はこの組だけを受ける・本物の取得が来れば必ず置き換わる)。
  - 🚨 対象が大文字の行は送らない: 2026-05-03〜04 の取込が小文字にする前の形で書いた行が残り、**3/1〜5/3 は全部が小文字の行と二重** (9/27 実測 76,344 行・約 119 万円。小文字の行だけの合計がキャンペーンの合計と月ごとに一致)
  - 外すのは同じ日・キャンペーン・粒度に小文字の対がある大文字の行だけ (対の無い大文字の行がある日は送らない)。**キャンペーンごとに** SKU 別の合計がキャンペーンの合計 (`fact_ad_spend_campaign`) と 1 円以内の日だけ送る (日の合計だけだと相殺して通る)。合わない日・行が無い日は送らず ⚠️ (推測で埋めない)
  - 読むとき: 古い行の日は SKU も ASIN も無い費用 (粒度 none) が入っていない・大文字の重複は外してある。`join core.ad_spend_days using (…) where source_report_id like 'legacy:%'` で見分ける

```
# 初回 (miniPC の PowerShell。0035 の適用 → 取込で過去の日の記録を作る → 全部送る)
cd C:\Users\bfaith\bfaith-portal
node -r dotenv/config scripts\company-db\migrate.mjs                          # 0035 (applied=1)
node apps\warehouse\fetch-amazon-ads.js --days 90       # 作り直した取込で直近 90 日を取り直す (ads_fetch_days ができる。Amazon の広告レポートは約 95 日より前を取れない)
node apps\company-db\push\ad-spend.mjs --mall amazon --all --dry-run
node apps\company-db\push\ad-spend.mjs --mall amazon --all
node apps\company-db\push\ad-spend.mjs --mall amazon --legacy --dry-run            # 古い取込の行 (2/5 〜 記録の最初の日の前日)
node apps\company-db\push\ad-spend.mjs --mall amazon --legacy

# 過去の日を入れ直す (Amazon が過去の値を直した・35 日より前が欠けた。約 95 日の内側だけ)
node apps\warehouse\fetch-amazon-ads.js --from 2026-07-01 --to 2026-07-31
node apps\company-db\push\ad-spend.mjs --mall amazon --from 2026-07-01 --to 2026-07-31
```

試験 = `node scripts/test-company-db-ad-spend.mjs` (16 件: 古い取込の行 (印の組・対のある大文字の重複だけ外す・キャンペーンごとの合計との検算・記録のある日には送らない) / 金額の文字列と指紋 (12 と 12.00・null と 0・日付) / 検証 / applied と出品の結び (sku だけ) / same・409・stale / refreshed と置き換え / 0 行の日 / 途中で落ちたら巻き戻る / relink / HTTP の受け口と server.js の配線 / 送り手 = 記録のある日だけ・2 回目は送らない・取り直しだけ送る・記録と行の食い違いは ❌・Render の方が新しい日は ⚠️・dry-run・プロファイル 2 つは拒む)。🚨 advisory lock の 2 接続の並行は PGlite では書けない

### 出品ごとの広告の効き目 (0038・0039)

広告費 (`core.ad_spend_daily`) と売上日次 (`mart.v_sales_daily`) を **出品 (listing_id)** で結ぶ関数。広告の SKU の行と注文の明細は同じ `core.resolve_listing_id` で出品に当たる = 同じ出品に集まる。

```sql
-- まず材料がそろっているか (広告費の無い日・売上日次が未公開の日・公開の値が材料と食い違う日・開いた回・古い取込の行の日)
select * from mart.ad_efficiency_coverage(1::smallint, 'amazon', 'jp', '2026-08-28', '2026-09-26');
-- 出品ごと (期間まとめ)。最後の引数 true で日ごと
select listing_code, title, ad_cost, sales_jpy, tacos, acos_1d, ad_sales_share
  from mart.ad_efficiency(1::smallint, 'amazon', 'jp', '2026-08-28', '2026-09-26', false) order by ad_cost desc limit 20;
```

- 列: 広告費 `ad_cost`・クリック・表示・広告経由の売上 `ad_sales_1d` / 数量 (1 日の帰属)・売上 `sales_jpy` (売上日次 = 取消を引く・送料を含み店負担の値引を引く。自社発送 + FBA)・正味の数量・`order_grains` (売上日次の粒度ごとの注文数の **延べ** = 同じ注文が SKU・出荷元で分かれると重複する。注文数そのものではない)
- **TACoS** = 広告費 ÷ 売上 / **ACoS** (`acos_1d`) = 広告費 ÷ 広告経由の売上 / **広告経由の割合** (`ad_sales_share`) = 広告経由の売上 ÷ 売上。分母が 0 か分からないときは null (0 で割らない・0 と読ませない)。🚨 **一部だけ分かっている和では比率を出さない**: 広告経由の売上が分からない行 (`ad_unknown_rows` > 0) があれば ACoS・広告経由の割合は null / 売上に効く金額不明の明細 (`sales_amount_unknown_lines` > 0 = 取り消されていない注文の、全部は取り消されていない明細で金額が null。core から数える。Amazon の取消の明細 (数量 0・金額 null) は売上に効かないので数えない = 0039) があれば TACoS・広告経由の割合は null
- 🚨 広告経由の売上は Amazon の帰属 (広告をクリックした 1 日以内の購入。広告した SKU 以外の購入も入りうる) = 出品の売上の内訳ではない → 広告経由の割合は 1 を超えることがある
- 出品に当たらない行は捨てずに `unresolved_key` でまとめる (`ad:asin:<ASIN>` / `ad:sku:<SKU>` / `ad:none` / `sales:unresolved`)。広告経由の売上が分からない行は `ad_unknown_rows`
- 関数にしてある (view にしない) = 期間で先に絞る (売上日次は 60 万行超)。本番の直近 30 日 = 3,248 出品・0.6 秒 (2026-09-27)
- 古い取込の行の日 (2/5〜6/28) は SKU も ASIN も無い広告費 (粒度 none) が入っていない = `coverage.ad_legacy_days` で分かる

- `coverage.sales_stale_days` = 公開の値が古い日 = ① 公開の値と材料を今そのまま足した値が **実際に食い違う日** (`mart.sales_daily_check`。日の合計。遅れて commit した取込も出る) ∪ ② 公開した回が始まった後に注文が動いた日 (日の合計が変わらない出品の付け替えも出る。遡らない = 回の前の push の更新は出さない)。0038 の「watermark − 15 分の後に注文が動いた日」は毎朝の push の直後でも出続けた (本番で直近 30 日のうち 22 日) ので替えた (控えめ = 作り直し済みでも出ることがある。次の注文の push の後で消える)

試験 = `node scripts/test-company-db-ad-efficiency.mjs` (9 件: 同じ出品に集まる・比率の null・一部だけ分かる和で比率を出さない (0021 の式で売上に効く明細だけ = 取消の明細は数えない・数量 0 と一部取消は数える)・延べの注文数・出品に当たらない行を捨てない (合計が材料と一致)・日ごと・期間の外を読まない・材料のそろい方・本物の作り直しの後で食い違いの日が出ない / 注文が変わった日だけ出る・他のモール / scope が混ざらない)

## SKU ごとの動き (0042。商品 360 の「売れ方・広告・在庫」)

`mart.v_product_360` (名前・原価・JAN・出品の一覧 = 静的な属性) に、期間の動きを足す関数。期間を引数に取る (売上日次は 60 万行超 = view にしない)。

```sql
-- 🚨 先に割り振れなかった分の大きさを見る
select * from mart.sku_activity_gaps(1::smallint, '2026-08-28', '2026-09-26');
-- SKU ごと (期間に売れたか広告のあった SKU だけ)
select sku_code, sku_name, units_net, units_by_mall, sales_jpy, amazon_ad_cost, stock_qty, cover_days
  from mart.sku_activity(1::smallint, '2026-08-28', '2026-09-26') order by units_net desc limit 20;
```

- **数量** (`units_net` = 注文 − 取消・`units_by_mall`) = 売上日次を見張り W6 と同じ規則で末端の SKU まで展開 (`mart.sales_expanded_to_skus`): SKU の分かる明細はその SKU / 出品だけの明細は出品の構成 × 数量 / NE のセット商品は構成品まで (入れ子 5 段・循環は止める)。セット SKU そのものは返さない (在庫は構成品側)
- **セット経由の数量** (`units_via_sets`) = 複数の SKU の品物 (出品のセット) か、NE のセット SKU を通った数量 (1 つの構成品だけの NE のセット = 10 本組なども入る)。まとめ売りの出品 (出品の構成が 1 SKU × N 個) は入れない
- 🚨 **展開しきれない行** (出品に当たらない・出品の構成が無い / 構成の無いセット / 深すぎる・循環するセット = W6 の bad) は、届いた末端の数量は数える (W6 と同じ) が **売上・広告費は付けない** (一部だけ見えた構成で「1 SKU だけ」と決めつけない) → gaps の `units_unexpanded`・`sales_unexpanded`・`ad_unlinked`
- 🚨 **売上** (`sales_jpy`・`sales_by_mall`・`amazon_sales_jpy`) と **Amazon の広告費** (`amazon_ad_cost`・`amazon_ad_sales_1d`) は **展開しきった上で 1 つの SKU だけでできている品物にだけ** 付ける (まとめ売り・1 つの構成品だけの NE のセットも付ける)。複数の SKU のセットは按分の決まりが無い = 推測で割らない → `sku_activity_gaps` の `sales_on_sets`・`ad_on_sets` に出る
- 🚨 売上日次は金額の分からない明細を 0 として足す → **売上に効く金額不明の明細の数** (`sales_amount_unknown_lines`。0039 と同じ条件で core から = 取消の注文・全部取り消された明細は数えない) を SKU ごとに返す。0 でなければ `sales_jpy` は確定額ではない (少なく出ている)。gaps にも全体 (`sales_amount_unknown_lines`) と SKU に付いた分 (`…_attributed`)。店は null と空文字を分けて結ぶ (0021 は別の粒度)
- 🚨 **公開の値が古い日**: 売上は公開済みの売上日次・金額不明の数は今の core から = 公開の後に注文が動くと食い違う (例: 金額が後から分かると、金額不明の数は 0 になるが売上は少ないまま)。→ 0039 と同じ判定 (検算の食い違い ∪ 公開した回が始まった後に注文が動いた日) で、SKU ごとに `sales_stale_rows` (その SKU の売上のうち古い日の行の数)・gaps に `sales_stale_days` を返す。0 でなければ売上も金額不明の数も今の値と違いうる (次の作り直しで直る。毎朝の push の直後は直近の日が出る = 控えめ)
- 広告経由の売上が分からない広告の行 (`ad_sales_1d` null) が 1 行でもあれば `amazon_ad_sales_1d` は null (一部だけの和を出さない)。その行数 = `amazon_ad_unknown_rows`
- **在庫** = `warehouse_qty` (倉庫 = ロジザード) + `fba_jp_available` (FBA JP の販売可能) = `stock_qty` (W6 と同じ)。🚨 どちらかが不明 (`mart.v_sku_stock` が null = complete な日が 1 度も無い) なら `stock_qty`・`stock_as_of`・`cover_days` は null (不明を 0 と読まない。取れていて行が無い SKU は 0)。`fba_jp_inbound` は別の列。**何日もつか** (`cover_days`) = 在庫 ÷ (期間の正味数量 ÷ 期間の日数)。売れていなければ null
- 🚨 FBA JP の在庫で SKU に結び付かない行 (`snapshots.sku_stock_daily.sku_id` null) は入らない = 本番 2026-09-28 で個数の 7.4%
- gaps: 数量 (`units_total` / 展開しきれない `units_unexpanded`)・売上 (`sales_total` = `sales_attributed` + `sales_on_sets` + `sales_unexpanded`・金額不明の明細の数)・広告費 (`ad_total` = `ad_attributed` + `ad_on_sets` + `ad_unlinked` = 出品が分からない / 出品に構成が無い / 展開しきれない)
- 本番の直近 30 日 (2026-08-28〜09-26。0042 の前に中身を埋め込んで読み取りで) = 約 2,400 SKU・1.6 秒。SKU に付いた割合 = 売上の 98.9% (残りはセット 0.9%・展開しきれない 0.2%)・広告費の 99.9% (金額そのものは書かない = 経営数値は 会社情報/経営数値.md だけ)

試験 = `node scripts/test-company-db-sku-activity.mjs` (11 件: 公開の値が古い日 (本物の作り直し)・売上に効く金額不明の明細の数 (店の null と空文字)・ 数量の展開 (出品の構成・NE のセット・取消)・売上と広告費は 1 SKU だけの品物にだけ・在庫の不明は null / 取れていて行が無いのは 0・何日もつか・gaps の合計が材料と一致・期間の外を読まない・一部だけ展開できる品物 (構成の無いセット・循環)・1 つの構成品だけの NE のセットと入れ子もセット経由・広告経由の売上の不明を一部の和にしない)

## 観測の原価 (0046。D7b-2。Company DB構想 13 §3.3・§3.4・D-57)

設計の正本 = AI_reference『CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』。`core.sku_costs` (夜間ロードの原価) は有効期間の始まりが全部 2026-09-10 以降 = それより前の Amazon の利益 (D7b-3) に原価が付かない。
→ miniPC の SQLite の原価の履歴 `m_products_history` (5/5 の最初の写しから・`changed_at` = 日次の処理が気づいた UTC の時刻) から **SKU × 原価の期間** を作って **別の表** に持つ (`core.sku_costs` には触らない = マスタ正本切替 (10) の持ち主・監査・版と混ぜない)。

- **表**: `core.sku_cost_observed_loads` = 受領の見出し (会社 × 送り元 `warehouse_sqlite` × 世代 = 1 行・**追記だけ** = 古い世代の見出しは監査の履歴として残る・今の世代 = 最大の generation。manifest = checksum・行の数・結びつかない商品コードの数・曖昧な商品コードの数) /
  `core.sku_cost_observed` = SKU × 期間 `[valid_from, valid_to]` (両端を含む・null = 今も)・`cost_jpy` (整数円)・`cost_status` (COMPLETE / OVERRIDDEN)・`backfill_method`・`first_observed_at`・`source_history_id`・`product_code` (履歴の原文)。行は今の世代の見出しのものだけ
- **読む口** = `mart.v_sku_cost_observed_effective`: **SKU ごとに `core.sku_costs` の最初の `valid_from` より前だけ** (終わりを前日で切る・後に始まる行は出さない・sku_costs の無い SKU はそのまま・`cost_basis` = observed / estimated)。
  境目は送り手で切らない = sku_costs が後から始まる SKU・夜間ロードとの前後でも読むときに正しく切れる。🚨 D7b-3 (利益の関数) は表を直に読まずにこの口を読む
- **期間の作り方** (送り手 `apps/company-db/push/sku-cost-observed.mjs`): `changed_at` の **JST の日の翌日から** 有効 (その日の注文は前の原価)。🚨 例外 = **最初の BASELINE_RESET (5/5)** はその日から `observed_daily_diff`・それより前は同じ値を `estimated_before_first_snapshot` で 2026-01-01 から推定。
  5/5 の写しに無く後で初めて出たコードは、それより前を推定しない。同じ有効日の複数の変化は最後の値 (changed_at → history_id)。DELETE = 原価不明の始まり (再 INSERT はその翌日から)。
  値は夜間ロードと同じ (状態は COMPLETE / OVERRIDDEN だけ = `mapCost`・`Math.round`・数でない / 負は不明 = `costForLoad`)。原価と状態が変わらない履歴の行は区切りにしない。最初の写しより前の履歴の行は使わない (要約に数が出る)
- **商品コード → SKU**: 夜間ロードと同じ正規化 (`normSku` = `core.norm_code`)。**正規化で 2 つ以上の履歴のコードが同じになる = 曖昧 = どれも送らない**。Render の `core.skus` に無い (`GET …/sku-cost-observed/sku-codes` で読む)・形が不正 = 結びつかない = 送らない。数は見出しに残る
- **世代** (送り手の台帳 `DATA_DIR/company-db-push.db` の `sku_cost_observed:` の連番): 回の始めに Render の今の世代まで進め、Render の今の manifest と同じなら送らない。違えば新しい世代を **HTTP の前に** 台帳に書いて (送る manifest も `pending`) 送る。
  応答が失われた = 同じ回の再送は同じ body (same)・次の回は pending が Render より新しく中身も同じなら同じ世代で送る
- **受け口 (Render)**: `POST /apps/company-db/sync/sku-cost-observed` (`apps/company-db/ingest/sku-cost-observed.mjs`)。全部 = 1 要求 = 1 取引で、その会社 × 送り元の行を消して新しい見出しと行を入れる。
  🚨 古い世代 = `stale` (書かない) / 同じ世代で manifest が全部同じ = `same` / 違う = 409 / Render に無い SKU の商品コード = 409 `SKU_UNRESOLVED` (送り手の SKU の一覧が古い = 次の回で読み直す)。
  checksum = 共通の部品 `apps/company-db/canonical-hash.mjs` (正規の JSON の SHA-256・鍵の順は固定・数は整数だけ) を受け口が届いた行から計算し直す (版 `sco-v1`)。`GET …/sku-cost-observed/status` / `GET …/sku-cost-observed/sku-codes`
- **毎朝**: daily-sync の「m_products 履歴記録」の直後に `--send` (新しい定期実行は無い。台帳 jobs-registry の warehouse-daily-sync に記載)。送信の失敗・409・別の送り手の見送り = ❌ (exit 1) = retry (`CompanyDB観測原価 --send`)。**「m_products 履歴記録」が失敗した朝は送らない** (その日の原価の変化を取り逃さないよう、retry で履歴の記録 (`m_products_history`) → 観測の原価の順に走らせる = 上流)。正規化の後に ASCII でない商品コードが 1 つでもあれば送らない (⚠️・JS と DB で同じ SKU と言い切れない = そのときに扱いを決める)。行は UPDATE できない (trigger)。
  Render に 0046 がまだ無い (status が 409 `not_migrated`) = ⚠️ (exit 0・送らない・世代も採らない) = マージから migrate までの朝を ❌ にしない・⏭️ ではなく ⚠️ = migrate を忘れても毎朝見える。
  🛑 安全弁: 前の世代があり、新しい中身が 0 行 / 行が前の 80% 未満 / 結びつかない + 曖昧の数が前より max(20, 前の数) を超えて増える = 送らずに ❌ (既存の行を消さない)。履歴と SKU の一覧を確かめ、わざと減らすときだけ手で `--send --force`。**migrate の後も ⚠️ が続いたら** Render のデプロイと migrate を確かめる

```
# 初回 (miniPC の PowerShell。🚨 migrate は中原さんの指示の後に dry-run → 本適用)
cd C:\Users\bfaith\bfaith-portal
node -r dotenv/config scripts\company-db\migrate.mjs --dry-run                 # 0046 だけが出ること
node -r dotenv/config scripts\company-db\migrate.mjs                           # 0046 (applied=1)
node apps\company-db\push\sku-cost-observed.mjs --dry-run                      # 送る予定の行・SKU・推定の行・結びつかない / 曖昧な商品コードの数 (台帳は開かない・Render は読むだけ)
node apps\company-db\push\sku-cost-observed.mjs --send                         # 1 回で全部 (約 2 万行・1 要求)

# ふだん = 何もしない (daily-sync が毎朝)。同じ中身なら「変わりなし」で送らない
```

```sql
-- 見出し (今の世代 = 最大の generation) と行の数
select generation, checksum, row_count, unresolved_code_count, ambiguous_code_count, sent_at from core.sku_cost_observed_loads order by generation desc limit 3;
-- ある SKU の 9/10 より前の原価 (sku_costs の境目で切った後)
select valid_from, valid_to, cost_jpy, cost_status, cost_basis from mart.v_sku_cost_observed_effective v join core.skus s using (sku_id) where s.code = 'xxx' order by valid_from;
```

試験 = `node scripts/test-company-db-sku-cost-observed.mjs` (32 件: 0046 の適用前は ⚠️ / 🛑 安全弁 / 見出しと実際の行の数のずれを直す / 期間の作り方 (JST の翌日・写しの日の例外と推定・同じ changed_at / 同じ日の最後・DELETE と再 INSERT・状態・丸め・まとめる・後で出たコードは推定しない・写しの前の行) / 衝突の隔離と結びつかない数 / 正規の JSON と checksum / 検証 / applied・same・409・stale・入れ替え・見出しは追記だけ / SKU_UNRESOLVED・巻き戻し / 読む口の境目 / HTTP と server.js の配線 / 送り手 = 台帳の世代・変わりなし・応答が失われた (同じ回・回をまたぐ)・409・stale・dry-run は何も書かない・lock / CLI の失敗 = exit 1 / daily-sync・retry・jobs-registry の配線)

## Amazon の利益の mart (0049。D7b-3。Company DB構想 13 §3.3・§3.5・§3.6・§3.7)

設計の正本 = AI_reference『CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』v26。Company DB に入った Amazon の財務 (0043・0047) に **原価・広告費・Easy Ship** を足し、**日 × 出品の利益** を関数で都度計算する (表は作らない・D-60)。
🚨 **分からないもの (決済のそろい・原価・広告費・返品数) を 0 として利益を確定しない**: 正式な列は null + 理由のコード。0 と仮定した値は別の名前 (`…_assuming_incomplete_zero_…`) = AI はこれを「利益」と読まない。
🚨 **決済のそろい (coverage)** = `core.finance_coverage_state()` を **0050 (D7b-1b-2) が表 `core.finance_coverage` を読む形に差し替えた** (下の「決済のそろい」の節)。
complete の行が無い間 (= coordinator の D7b-1b-3 が complete を送るまで) は **complete_to が null** = 全部の日が `day_finance_status = provisional / missing` = **正式な利益は全部 null** (0 と仮定の値は出る)。
この関数だけの差し替え (同じ引数・同じ戻り = `complete_to`・`generation`・`source_revision` の 1 行。0 行 / 2 行以上は全部 null と読む = fail-closed)。この mart は変えない。

- **関数**:
  - `mart.amazon_profit_daily_range(会社, モール, scope, from, to)` = **日 × 出品の寄与の利益** (月の手数料を引く前)。行の鍵 = `listing_id` / `seller_sku_norm` / `listing_resolution` (resolved = 出品あり・seller_sku_norm は null / unresolved = 出品に結びつかない正規化 seller SKU・listing_id は null)。その期間に財務・広告・Easy Ship のどの行も無ければ 0 行
  - `mart.amazon_profit_day_totals_range(会社, モール, scope, from, to)` = `row_kind` = `day` (取引の無い日も 1 行) / `calendar_month` (期間が暦の月をまるごと含む) / `range_month_subtotal` (期間の端の一部だけの月) / `range_total` (重ならない)。`period_from` / `period_to` (両端を含む)・`economic_date_jst` (day だけ)・`month_start` (月の行だけ)
  - 契約 = `from <= to`・最大 400 日 (両端を含む)・**今は amazon / jp だけ** (受け取り時の出品の決め方が shop_code を見ない = Amazon のアカウントが 1 つの間だけ正しい)・違えば例外 (22023 `invalid_input`)
  - 内部の部品 (直に呼ばない): `mart._amazon_profit_finance_days` (日の決済の状態) / `_amazon_profit_ad_days` (広告の日の状態) / `_amazon_profit_ad_children` (広告の子を今のマスタで結び直す) / `_amazon_easy_ship_alloc` (Easy Ship の割り振り) / `_amazon_profit_rows` / `_amazon_profit_totals`。
    🚨 材料 (日の状態・広告・Easy Ship の割り振り) は **1 回の呼び出しで 1 回だけ** 計算し、型 (`mart.amazon_profit_finance_day` など) の配列で行の本体に渡す = 日の合計も同じ材料を使い回す (Easy Ship の割り振りは期間の外の日も読む = 重い・#1559 Codex R1 Medium 1)
  - `core.finance_coverage_state(会社, モール, scope, source)` (0050 で差し替え = complete のときだけ complete_to) / `mart.amazon_profit_composition_audit_since()` (0049 の適用の時刻) / `mart.amazon_account_fee_tax_rate(line_kind)` (月の手数料の税の表)
  - 行には子 (0047) の財務の列も出品にまとめて出す (`units_marketplace_guarantee`・丸める前の返品数 `units_refunded_customer_unrounded` / `units_a_to_z_refund_unrounded` = 単価の無い子は null)
- **今のマスタで結び直す (D-64・`master_basis = 'current'`)**: 財務 (`mart.finance_daily_sku_range` の正規化 seller SKU の子) は今の `core.listings` の `listing_norm` の **直接の一致** (0043 の受け口と同じ = 会社 × モールで 1 件のときだけ・0 件 / 2 件以上 = 未解決)。
  広告は `target_granularity = 'sku'` の行だけ `core.resolve_listing_id` (external_ids の別名も含む)・**asin / none は常に未解決** (ASIN が出品のコードと同じ文字でも)。保存済みの `listing_id` は診断 (`received_listing_ids`) だけ。構成も今の `core.listing_components`
- **原価** (§3.3) = 構成 × SKU ごとに **その日を覆う 1 行**: `core.sku_costs` と観測の原価 (`mart.v_sku_cost_observed_effective`) から `valid_from DESC, created_at DESC, 行の ID DESC` の最初 (同じ日に 2 回変わった取込の行を二重に数えない)。
  採る状態 = COMPLETE / OVERRIDDEN だけ (override_zero の 0 円は正しい 0)・PARTIAL / MISSING / 行なし = 原価不明。`cost_basis` = 構成の中の最も弱いもの (missing > estimated > observed > sku_costs)・`missing_cost_sku_ids`・`cost_sku_cost_ids` / `cost_observed_ids`
- **金額の式** (§3.5b・固定): `cogs_jpy = units_net_sold × Σ(qty × 原価)` (原価が 1 つでも不明なら null・数 0 でも null) / `contribution_before_ad_incl_jpy = profit_before_cogs_jpy − cogs_jpy` /
  `contribution_before_ad_excl = 上 + taxable_sku_fee_cost_jpy / 11 + promotion_tax_jpy` (課税の 6 手数料) / **`contribution_after_ad_incl = before_incl − ad_cost × 1.1`** (✅ 広告費は税抜 = D-61) / `contribution_after_ad_excl = before_excl − ad_cost`。
  税抜・広告・0 と仮定・合計は numeric を途中で丸めず、返すときだけ小数 2 桁 (`_jpy` を付けない)
- **理由のコード** (`profit_incomplete_reasons`・固定の順) と **列ごとの null** (試験で固定):

| コード | 意味 | 行の寄与 (before ad) | 行の広告の後 | 日の合計 before / after ad / after fees |
|---|---|---|---|---|
| `finance_incomplete` | 日が complete でない (coverage) | null | null | null / null / null |
| `finance_unclassified` | 分けられない部品の数 > 0 (SKU の行: unclassified + unmapped の部品・旧い形の版の行)。**金額でなく数** | null | null | null / null / null (手数料の側だけなら after fees だけ null) |
| `refund_units_unknown` | 返品の単価が無い (unit_price_missing) | null | null | null / null / null |
| `refund_units_partial_month` | 返品数が月の推定で、月末まで決済がそろっていない | null | null | null / null / null |
| `listing_unresolved` / `composition_missing` / `cost_missing` | 出品が 1 つに決まらない / 構成 0 件 / 原価不明 | null | null | null / null / null |
| `ad_not_collected` / `ad_missing` / `ad_legacy_unverified` | 2026-02-05 より前 / 広告の日の記録 (親) が無い / 古い取込の日 | 値 | null | 値 / null / null |
| `ad_unresolved` | その日に出品に結びつかない広告の行がある | 値 | null | 値 / **値** (ad_spend_days の全額を引く) / 値 |

  - `assumed_zero_reasons` = 上から `refund_units_partial_month` を除いたもの (0 と仮定の値で 0 と置いたもの。partial は推定の返品数を使う)
  - `master_notes` (情報の印・**値を止めない**・固定の順): `pre_audit_unverifiable` (前日の JST 00:00 が `composition_audit_since` = 0049 の適用より前) / `current_after_recorded_change` (前日の JST 00:00 以降にその出品の構成か識別 (INSERT・DELETE・mall・shop_code・listing_code) の監査の記録) / `listing_changed_since_received` (受け取り時の出品 (保存済みの `listing_id`) と今の結び直しが違う = **わかる範囲の印**。下の「受け取り時の出品」)。
    どの印も「無い」ことは「変わっていない」の証明ではない。
    `composition_basis` = listing_unresolved → missing → pre_audit_unverifiable → current_after_recorded_change → current_no_recorded_change (前の 2 つだけがゲート)。日 / 月 / 期間は `master_note_counts` (jsonb・鍵は 3 つ)
  - `composition_hash` / `cost_input_hash` = 正規の JSON の SHA-256 (`apps/company-db/canonical-hash.mjs` と同じ規則・ID は 10 進の文字列)
- **Easy Ship** (D-59) = 注文 × 計上日で正味にしてから同じ注文の SKU の本体売上の割合で 1 円単位・端数は小数部の大きい順。
  重み = **max(本体売上, 0)** (負の SKU には配らない・正の重みの合計が 0 以下なら売上の行のある SKU で等分) = どの場合も配った合計 = 元の額 (#1559 Codex R3・SQLite の build と同じ規則)。**期間に依らない** (本体売上は期間の外の日も含む全部の日)。
  行の `easy_ship_alloc_jpy` は内訳 (寄与から引かない)・日の合計は月の手数料で引く。売上の行が無い注文は `easy_ship_unallocated_jpy` / `_count`
- **日の合計**: 月の手数料 `account_fee_cost_jpy = −Σ account_fee_amount_jpy` (8 種類の列も)・税抜は種類ごとに ÷ 1.1 (Amazon の決済の手数料は全部税込)・`profit_after_account_fees_* = contribution_after_ad_* − 手数料`。
  分けられない金額は 3 区分 (`unknown_line_mapped_jpy` / `unclassified_mapped_jpy` / `unmapped_jpy`)。**保存則** = `net_jpy = profit_before_cogs_jpy + sales_tax_jpy − account_fee_cost_jpy + unknown_line_mapped_jpy + unclassified_mapped_jpy + unmapped_jpy + not_account_fee_mapped_jpy`。
  不完全な日 = `before_ad_incomplete_days` / `after_ad_incomplete_days` / `after_account_fees_incomplete_days` (と数)
- **読む口 (Render)**: `GET /apps/company-db/sync/amazon-profit/daily?mall=amazon&scope=jp&from&to` / `GET …/amazon-profit/totals?…` (x-sync-key・statement_timeout 120s・0049 の前は 409 `not_migrated`)。
  🚨 **10/1 から 503 (封じ込め・§3.10)** = どちらも 503 `{ ok: false, code: 'PROFIT_ROUTE_DISABLED', error }` を返し、0049 の関数を呼ばない・DB に接続もしない (鍵が無ければ 401 のまま)。
  理由 = 10/1 00:10 に 93 日分の計算で本番の Postgres (1GB) が落ちた・本番は `temp_file_limit = -1` (一時ファイルが無制限) で SET の権限も無い。
  設計 (AI_reference『CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』§3.10 D-60 v3.1 = 保存しないで 1 か月ずつ計算) で作り直すまで止める。再び有効にする設定は無い (戻すのはコードの変更)。以下のこの口の説明は止める前の形
  🚨 設計書 (13 §4) の `/apps/company-db/api/...` ではなく、**Company DB の既存の読む口の流儀** (`/sync` の下 + x-sync-key。例 `/order-finance/daily`) にそろえた (#1559 Codex R1 Medium 4)。
  **`/daily` は 1 回 93 日まで** (行が多い = メモリに全部を載せて返す。長い期間は日の範囲で区切って何回かに分けて読む)・`/totals` は 400 日まで (日 + 月 + 合計だけ)。
  JSON の形: ID と ID の配列は 10 進の文字列 / 円 (`*_jpy`)・個数・件数は数 / **金額の numeric (税抜・広告費・0 と仮定・手数料の後) は小数 2 桁の文字列** (例 `"225.45"`) /
  **`units_*_unrounded` (丸める前の返品数) は小数 最大 6 桁の文字列** (例 `"1.499999"`・返品が無い = `"0"`・単価の無い子 = null) / 日付は YYYY-MM-DD / 時刻は UTC の ISO (ミリ秒)
- `finance_coverage_generation` / `finance_source_revision` = `core.finance_coverage_state` の値 (世代 = 今の世代・updating でも出る / 版 = complete のときだけ)。
  合計は対象の日で **(source・generation・source_revision) の組が 1 つに決まるときだけ** (null の日がある・source が違う = null。#1559 Codex R2 Low 1)・`calculation_version = 'amazon_profit_v1'`・`calculated_at` = 1 回の呼び出しで同じ値
- **受け取り時の出品** (診断・値を止めない): 財務の `received_listing_ids` / `received_listing_unresolved_count` と広告の `ad_received_listing_ids` / `ad_received_unresolved_rows` (保存済みの `listing_id`)。
  出品の行は、受け取りの記録があって「今の出品 1 つだけ・受け取り時の未解決 0」と **集合で** 一致しなければ `listing_changed_since_received` (#1559 Codex R1 Medium 2)。
  🚨 **わかる範囲の印 (best-effort)** (#1559 Codex R2 Medium 1): 広告の保存済みの `listing_id` は **relink (0035 `core.relink_ad_spend_listings`) の後の値**。
  relink は受け取り時に null だった行を後から埋める = 「受け取り時は未解決 → 別名 (external_ids) を足す → relink」の行は、受け取り時の未解決を区別できない (**印が付かない = 仕様**・試験で固定)。
  スキーマは変えない (受け取り時の値を別の列に残さない)。印は情報だけ = **正式な利益は止めない** (D-64)

**マージの後の手順 (🚨 まだ流さない = migrate は中原さんの指示の後に dry-run → 本適用)**。0049 は関数だけ (表・既存の関数に触らない) = Render は旧いコードのままでも困らない (読む口が 409 `not_migrated` になるだけ)

```
# 本番で使っていない worktree から (miniPC の PowerShell。.env は本体の 1 つを読む)
cd C:\Users\bfaith\bfaith-portal
git fetch origin
git worktree add C:\tmp\d7b3 origin/master
cd C:\tmp\d7b3
npm ci
$env:DOTENV_CONFIG_PATH = 'C:\Users\bfaith\bfaith-portal\.env'
node -r dotenv/config scripts\company-db\migrate.mjs --dry-run                 # 0049 だけが出ること
node -r dotenv/config scripts\company-db\migrate.mjs                           # 0049 (applied=1)
cd C:\Users\bfaith\bfaith-portal
git worktree remove C:\tmp\d7b3
```

```sql
-- 読むだけの確かめ (本適用の後)。今は正式な値は全部 null (D7b-1b の前) = 0 と仮定の値と理由を見る。所要時間も見る (1 か月・400 日)
select economic_date_jst, listing_code, day_finance_status, contribution_before_ad_incl_jpy, contribution_after_ad_assuming_incomplete_zero_incl, profit_incomplete_reasons
  from mart.amazon_profit_daily_range(1::smallint, 'amazon', 'jp', '2026-09-01', '2026-09-07') order by 1, 2 limit 50;
select row_kind, period_from, period_to, profit_after_account_fees_assuming_incomplete_zero_incl, profit_incomplete_reasons, master_note_counts
  from mart.amazon_profit_day_totals_range(1::smallint, 'amazon', 'jp', '2026-08-01', '2026-09-30') where row_kind <> 'day';
```

**🚨 受け入れの条件 = 本番の所要時間を読むだけで測る** (#1559 Codex R1 Medium 1・R2 Medium 2。PGlite では本番の件数の時間を測れない):
🚨 **10/1 からこの節の測り方は流さない** = 10/1 00:10 に 93 日分の計算で本番の Postgres (1GB) が落ちた。読む口は 503 (下の `Invoke-RestMethod` は 503 が返る)。測り方は §3.10 (D-60 v3.1) の初回の校正の手順に従う
- **0049 を本適用した直後に**、読むだけで次の 3 つの期間を測る = **1 か月** / **93 日** (`/daily` の上限) / **400 日** (`/totals` の上限)。`begin read only` の中で・120 秒で打ち切り・daily-sync の 07:00〜09:10 を避ける
- 合格 = どれも 120 秒 (読む口の statement_timeout) より十分短い (目安 = 1 か月 数秒・上限の期間で 60 秒以内)
- **遅ければ、AI・画面 (読む口の使い手) に使う前に直す** (次の番号の migration で計画を直す = 0045 と同じ流儀)。測るまでは読む口を AI・画面から呼ばない

```sql
-- Render の Shell の psql (または miniPC から External URL)。\timing on で時間を出す
\timing on
begin read only;
set local statement_timeout = '120s';
-- ① 行の関数: 1 か月 / 読む口の上限 (93 日)
explain (analyze, buffers) select count(*) from mart.amazon_profit_daily_range(1::smallint, 'amazon', 'jp', '2026-08-01', '2026-08-31');
explain (analyze, buffers) select count(*) from mart.amazon_profit_daily_range(1::smallint, 'amazon', 'jp', '2026-06-30', '2026-09-30');
-- ② 日の合計: 1 か月 / 上限 (400 日 = 財務の policy の起点から)
explain (analyze, buffers) select count(*) from mart.amazon_profit_day_totals_range(1::smallint, 'amazon', 'jp', '2026-08-01', '2026-08-31');
explain (analyze, buffers) select count(*) from mart.amazon_profit_day_totals_range(1::smallint, 'amazon', 'jp', '2026-01-01', '2027-02-04');
rollback;
```

```
# 読む口を通しての時間 (miniPC の PowerShell。鍵は本体の .env の MIRROR_SYNC_KEY を人が入れる)
$h = @{ 'x-sync-key' = '<MIRROR_SYNC_KEY>' }
Measure-Command { Invoke-RestMethod -Headers $h 'https://<Render の URL>/apps/company-db/sync/amazon-profit/daily?mall=amazon&scope=jp&from=2026-06-30&to=2026-09-30' | Out-Null }
Measure-Command { Invoke-RestMethod -Headers $h 'https://<Render の URL>/apps/company-db/sync/amazon-profit/daily?mall=amazon&scope=jp&from=2026-08-01&to=2026-08-31' | Out-Null }
Measure-Command { Invoke-RestMethod -Headers $h 'https://<Render の URL>/apps/company-db/sync/amazon-profit/totals?mall=amazon&scope=jp&from=2026-01-01&to=2027-02-04' | Out-Null }
```

試験 = `node scripts/test-company-db-amazon-profit.mjs` (32 件: 材料は 1 回だけ計算 (関数の本体を数えて固定) / 受け取り時の出品を集合で比べる (財務・広告) / relink の後は印が付かない (わかる範囲の印の限界を固定) / coverage の関数の世代と版 (合計は source を含めて 1 つのときだけ) / coverage が null なら正式な値は全部 null / 差し替えた後の手で計算した値 (税込・税抜・値引きの税・広告 × 1.1・返品の推定・負の手数料・override_zero と原価不明) / 構成 0 件・候補 2 件・出品なし / 広告の状態 (legacy・missing・not_collected) / 分けられない部品の相殺・旧い形の行・単価の無い返品 / 同じ日に 2 回変わった原価・観測と推定 / hash が JS と一致 / ASIN は未解決・別名は結ぶ・未解決は出品の行だけ止める / Easy Ship (割合・等分・端数・返金・期間に依らない・配れない額・負の重み (0 にする)・全部が非正 (等分)・保存則) / master_notes (受け取りとの違い・監査の記録・タイトルは数えない) / 理由の順と列ごとの null (3 つの coverage で全行) / 日の合計 (列の組ごとの条件・税の表・保存則・row_kind が重ならない・取引の無い日) / 契約 / HTTP (10/1 から 503 `PROFIT_ROUTE_DISABLED`・DB に接続しない (pg の client を作らない)・鍵が無ければ 401))

### 重い関数の権限と重い入口の棚卸し (0056・D-60 の緊急の封鎖 = PR 1a。Codex R-D60-v3-4 H2 / R-D60-v3-5)

🚨 **0056 から、D-60 の重い関数 10 個は誰も直接呼べない** (持ち主・watcher・profit_reader・PUBLIC のどれでも `permission denied for function` = 42501)。
受け口の 503 (#1570) は HTTP だけの封じ込めで、DB に直接つなげば (持ち主の `COMPANY_DB_URL`・watcher・PUBLIC) 呼べた = 10/1 の停止をもう一度起こせた。受け口は 503 のまま (この PR で 1 文字も変えない)。
上の「受け入れの条件」「読むだけの確かめ」の SQL も **本番では 42501 で止まる** (流さない)。校正は使い捨ての同じ条件の DB で (§3.10)。

- **重い入口の棚卸し = `scripts/company-db/heavy-entry-manifest.mjs` (`HEAVY_ENTRY_MANIFEST`)**。署名つきの固定の一覧で、0056 の migration とは別に持つ。分け方:
  - `revoke` (10) = 0056 で権限の表 (`proacl`) を **空** にした = `mart.amazon_profit_daily_range`・`mart.amazon_profit_day_totals_range`・`mart._amazon_profit_totals`・`mart._amazon_profit_rows`・
    `mart._amazon_profit_finance_days`・`mart._amazon_profit_ad_days`・`mart._amazon_profit_ad_children`・`mart._amazon_easy_ship_alloc`・`mart.amazon_profit_assert_args`・`mart.finance_daily_sku_range`
    (0047 / 0050 の watcher の GRANT も外した)。PUBLIC・watcher・profit_reader (あれば)・持ち主・表に載っていた全部の役割から外し、最後に空を確かめる (違えば例外 = 取引ごと巻き戻す。持ち主でない役割で流すと `d60_revoke_incomplete`)
  - `guard_later` = 重いが正当な呼び手がいる (か、外すかを人が決める) = **権限は変えない** (後の PR で共通の lock `company_db_heavy` に参加させるか外す)。
    `mart.finance_daily_range` (受け口 `GET /order-finance/daily`・watcher にも明示の GRANT)・`mart.ad_efficiency` / `_coverage`・`mart.sku_activity` / `_gaps`・`mart.sales_expanded_to_skus`・`mart.listings_to_skus` (人・AI が読む)・
    `mart.sales_daily_check` / `refresh_sales_daily` / `build_sales_daily_dates` / `purge_sales_daily` (受け口)・`core.relink_shipments_bulk` / `reresolve_order_lines` / `merge_duplicate_suppliers` (一時の表 = TEMP)・
    `core.relink_shipments`・`core.relink_ad_spend_listings`・`raw.purge_superseded_observations`・`ops.amazon_map_sales_coverage` / `unmapped_recent` (理由は manifest)。
    🚨 **これらは今も DB に直接つなげば呼べる** (「repo の中から呼んでいない」は直呼びを防いだ証明ではない)
  - `light` = 規則にかかるが重くない (定数・policy の表だけ・DDL の補助・coverage の 1 行など)。権限は変えない
  - 期間の形でない既知の入口も手で `guard_later` に足した = `core.apply_order_finance_batch` (POST /order-finance の chunk)・`core.apply_order_batch` (POST /orders)・`core.apply_shipment_batch` (POST /shipments) (jsonb の大きさに SQL の上限は無い)
- 🚨 **保証の範囲** (#1601 Codex R1 M1) = 「Company DB の全部の重い入口」**ではない**。機械で漏れを止めるのは 3 つだけ:
  (a) 関数 = 下の見つける規則 (期間の集計の関数・mart の関数・D-60 の関数に依る関数) + 手で足した既知の入口 /
  (b) view = **mart / ops の全部の view** (`HEAVY_VIEWS` に heavy / light と理由・pg_class と突き合わせる。heavy = `mart.v_finance_daily`・`v_finance_account_fees_monthly`・`v_order_finance_summary`・`v_order_finance_uncovered`・`v_sales_daily`・`v_shipments_daily`・`v_shipments_unlinked`・`v_ad_spend_daily`・`v_cross_mall_diff`。権限は変えない) /
  (c) アプリの側の重い処理 (`APP_HEAVY_ENTRIES` = POST /order-finance の chunk・coverage の complete の計算・バックアップ・見張り・夜間ロード。ファイルがあることだけ試験)。
  規則にかからない関数 (jsonb の batch を受ける ops の SECURITY DEFINER など) は、重くても機械では見つけない = 足すときは手で。D-60 の共通の lock に参加させる相手の正本は後の PR (PR 4) で、この 3 つの一覧から始める
- **見つける規則** (試験と `--verify` が pg_proc と突き合わせる): core / mart / ops / raw / snapshots / events の関数 (trigger を除く) で、① schema が mart ② 入力の引数が期間・件数・保持の形
  (名前 `p_from` / `p_to` / `p_since` / `p_after` / `p_upto` × 日付・時刻の型、`p_days` / `p_limit` / `p_keep_days` × 整数、`p_dates` × date[]) ③ 本体が revoke の関数を名前で呼ぶ、のどれか
  = manifest に無ければ「分けていない」で落ちる。revoke の関数にあとから GRANT しても・guard_later / light の PUBLIC の可否が manifest と違っても落ちる。
- **呼び手の調べ (10/3)** = アプリ (router・ingest・watch・miniPC の送り手・measure-amazon-finance) で revoke の関数を呼ぶ所は無い。revoke の関数を呼ぶ関数は revoke の関数だけ・SECURITY DEFINER の呼び手も無い。
  coverage の complete の道 (財務の chunk → updating → complete → `finance_coverage_state`) は revoke の関数を使わない (本物の PG の試験で通す)。
- **持ち主自身から外しても効く** (PostgreSQL 18.4 で確かめた)。ただし持ち主は **付け直せる** (持ち主は常に GRANT の権限を持つ) = この封鎖は「うっかり・ほかの接続から呼べない」まで。
  SECURITY DEFINER の関数 (定義者 = 持ち主) の中から呼んでも 42501 = 抜け道にならない。superuser は権限を見ない (PGlite の試験の接続は superuser = ほかの試験は今までどおり呼べる)。
- **PR 1a でしないこと** (PR 1b = 別の管理主体が要る): 役割 (`profit_definer`・migration の deployer) を作らない・持ち主を移さない・全体の既定の権限を変えない・**TEMP の権限を外さない**
  (持ち主の relink・reresolve_order_lines・merge_duplicate_suppliers が一時の表を使う)。TEMP は `--verify` が **監査の結果を出すだけ** (PUBLIC・役割ごとの TEMP・一時の表を作る関数)。
  PG 16 以降、CREATEROLE の役割が作った役割には ADMIN だけが付き SET が無い = `alter function … owner to` は `must be able to SET ROLE` で止まる (実機で確かめた)。
- 🚨 **約束 1 (revoke の関数を直す)**: 持ち主にも EXECUTE が無いので、`create or replace` は関数の検査 (validator) が 42501 で止まる。直す migration は **同じ取引で**
  `grant execute on function <署名> to current_user` → `create or replace` → 0056 と同じに全員から外す (`revoke … from public` / `from current_user`) → 権限の表が空を確かめる。
- 🚨 **約束 2 (これから作る関数)**: 関数の既定は PUBLIC EXECUTE。`alter default privileges … in schema mart revoke … from public` は **効かない** (schema ごとの既定は全体の既定に足すだけ・実機で確かめた)。
  → 重い入口の規則にかかる関数は **作った取引で署名ごとに REVOKE** (閉じるなら) し、manifest に分け方と理由を足す (足さないと試験が落ちる)。
- 🚨 `create-watch-roles.mjs` は SECURITY DEFINER の関数の全部に **持ち主の EXECUTE を付け直す** (`grant execute … to <owner>`)。今の revoke の関数は SECURITY INVOKER なので当たらない (PR 1a の今の形では問題なし)。
  **後の PR で D-60 の関数を SECURITY DEFINER にするときは、① スクリプトの対象から D-60 の関数を外す処理 ② 流し直しても revoke の関数の権限の表が空のままの回帰の試験 の 2 つが必須** (#1601 Codex R1 Low。
  コメントだけでは防げない: 持ち主が runtime のままなら封鎖が開き、専用の持ち主に移した後ならスクリプト全体が権限の誤りで巻き戻る)。

本番の確かめ (読むだけ・カタログの SELECT だけで重い関数は呼ばない): `node -r dotenv/config scripts/company-db/heavy-entry-manifest.mjs --verify` (問題があれば exit 1・TEMP の監査も出す)。

試験 = `node scripts/test-company-db-profit-fn-revoke.mjs` (PGlite・14 件: 0055 までの姿で watcher・profit_reader・PUBLIC が呼べる前提 / 0056 の後は revoke の 10 の権限の表が空・3 つの役割は 42501 /
guard_later・light の権限の表は前と 1 文字も同じ / 2 回流しても同じ / TEMP の監査は前と同じ / 棚卸しの突き合わせ (関数・view・アプリ) と漏れ止め 4 つ / 受け口は 503 で DB に接続しない) +
`scripts/test-company-db-profit-fn-revoke-pg.mjs` (本物の PG・11 件。**試験が自分で使い捨てのクラスタを起動して最後に必ず止めて消す** = 外の PostgreSQL にはつながない・watcher / profit_reader が既にあれば止まる・
`persistent: false` + 起動の後の全部 (役割・DB・接続の準備・試験) を外側の try/finally で覆う = どこで落ちてもクラスタを消す・フォルダが残れば失敗。
**embedded-postgres は devDependencies に正確な版 (`18.4.0-beta.17`) で入っている** (lockfile で固定・#1601 Codex R2 M) = clean な checkout + `npm ci` だけで流れる。
Render の Dockerfile は `npm ci --production` = dev の依存は入らない (容量・起動は変わらない)。miniPC の本適用の手順も `npm ci --omit=dev` でよい (migrate.mjs と --verify は `pg`・`dotenv` = 本番の依存だけ)。
外の置き場 (環境変数 `EMBEDDED_PG_DIR` → `C:/tmp/pg-embed`) は **版が同じときだけ** 使う (node_modules を別の作業の木へのジャンクションにした worktree で npm ci をし直さずに流すため。違う版は使わない = 同じコミットで同じ版)。
見つからない・版が違う・起動できない = **失敗 (飛ばさない)**。
🚨 終わりは `process.exit` を明示: embedded-postgres が入れる async-exit-hook が beforeExit で `process.exit(0)` を呼び `process.exitCode = 1` を上書きする (失敗しても exit 0 になっていた・10/3 に見つけた)。
どちらも `npm run test:company-db` に入っている (飛ばさない)。**マージの前に必ず流す**: 持ち主 = superuser でない CREATEROLE の login の役割で全部の migration を流す /
持ち主・watcher・profit_reader・PUBLIC だけの役割の全部が 42501 / guard_later・light と持ち主と TEMP は変わらない / coverage の complete の道と `finance_daily_range` は今までどおり /
SECURITY DEFINER の中からも 42501 / create or replace の約束 / 2 回流しても同じ / `--verify` が前 ❌・後 ✅ / 持ち主でない役割で流すと止まる)。

**マージの後の手順 (🚨 まだ流さない = migrate は中原さんの指示の後に miniPC で dry-run → 本適用)**。0056 は権限だけ (表・関数の中身は変えない・利益の値は計算しない)。
Render のコードは変わらない (受け口は 503 のまま) = Render の deploy と順番は無い。

```
# 本番で使っていない worktree から (miniPC の PowerShell 5.1。.env は本体の 1 つを読む)
cd C:\Users\bfaith\bfaith-portal
git fetch origin
git worktree add C:\tmp\d60-revoke origin/master
cd C:\tmp\d60-revoke
npm ci --omit=dev                                                                # 本適用には dev の依存 (embedded-postgres・PGlite・playwright) は要らない
$env:DOTENV_CONFIG_PATH = 'C:\Users\bfaith\bfaith-portal\.env'
node -r dotenv/config scripts\company-db\heavy-entry-manifest.mjs --verify      # 前: ❌ (revoke の関数を watcher・PUBLIC・持ち主が呼べる) が出ること。TEMP の監査を控える
node -r dotenv/config scripts\company-db\migrate.mjs --dry-run                  # 0056 だけが出ること
node -r dotenv/config scripts\company-db\migrate.mjs                            # 0056 (applied=1)
node -r dotenv/config scripts\company-db\heavy-entry-manifest.mjs --verify      # 後: ✅
cd C:\Users\bfaith\bfaith-portal
git worktree remove C:\tmp\d60-revoke
```

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
- **W14 (9/27)** 広告費の取込の完了と検算 (Company DB構想 11 の ③。評価キー `amazon/jp`・前提なし・2026-10-11 まで info)。今朝の証跡 `ad-spend-amazon` (同じ daily-sync の回だけ。無い = 取込「Amazon Ads (SKU)」が失敗して見送った → blocked) で送信の失敗・Render の方が新しい取得で書かなかった日 (stale) が無い・昨日の取得の記録が miniPC にある・Company DB の昨日の日 (`core.ad_spend_days`) が今朝の取得の世代・**Company DB の昨日の合計がキャンペーンの合計と 10 円 / 0.5% の大きい方まで** (キャンペーンの合計は miniPC の `fact_ad_spend_campaign` にしか無い = 送り手が数えて証跡の `campaign_check` に世代つきで入れる。送った取得と同じ世代・同じ SKU 別の合計のものだけ使う = 送った後に取り直されたら blocked。「昨日」の日付・数が読めない・キャンペーンの合計が無いも blocked)・SKU なのに出品が分からない費用が 5% 以下
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
- マスタの入力のロール (`scripts\company-db\create-master-edit-roles.mjs`・0051・14 ⑤-1) も同じ作り: master_edit (画面) / master_ops (切替の段階) / master_observer (NE の観測) / 門のまとめ master_gate (ログインなし) と場所ごとのログイン master_gate_render・master_gate_minipc (門の記録の場所はログインで決まる)。env = `COMPANY_DB_MASTER_EDIT_URL` / `COMPANY_DB_MASTER_OPS_URL` / `COMPANY_DB_MASTER_OBSERVER_URL` / `COMPANY_DB_MASTER_GATE_RENDER_URL` (Render) / `COMPANY_DB_MASTER_GATE_MINIPC_URL` (miniPC)。🚨 **流し直しても、もうあるロールのパスワードは変えない** (門のログインを黙って変えると門の記録が書けず、切替が進められない)。変えるのは `--rotate-password <ロール>` を付けたときだけ (出た接続文字列を同じ日にそのロールの env へ)

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
- 生成列 (`code_norm` など) は入れない (復元時に自動で入る)。
- 自己参照のうち主キー (1 列) を指すもの (`parent_product_id`・判断の台帳の `approved_event_id` など) は、**指す行が入ってから値のまま入れる**。
  - 指す行がまだ無い行は後回しにし、指す行が入ったらその場で入れる (2 つ以上の値を指す行は全部そろってから)。ダンプの並びに頼らない。
  - 最後まで指す行が現れない行 (輪・ダンプに無い行) と、後回しの上限 (20 万行 / 表) を超えた分は、null で入れて全部入ってから埋める。
  - 主キー以外を指す自己参照も、null で入れて全部入ってから埋める。
  - 🚨 action_done は「承認の番号がある」ことを CHECK (`ck_mde_done`) で縛っている = null で入れると拒まれる。2026-09-27 (#1494) までは全部 null で入れていたので、action_done が 1 行でもあるダンプは戻せなかった。
  - 🚨 ダンプは元の型の主キーの順に並べる。#1494 の前のダンプは `::text` の文字の順 (10 が 9 より前) = 後回しで戻す
- append-only の表は trigger を外して消し、終わったら戻す (同じトランザクション内)。わざと止めてあった trigger は止まったまま戻る (パーティションの子も 1 つずつ扱う)
- 取ったあと・戻したあとに行数を照合する。合わなければ失敗して巻き戻す
- 検証 (`verify`) も復元も **1 行ずつ** 読む。ダンプ全体を 1 つの文字列にしない (Node の文字列は約 512 MB が上限。2026-09-26 の Company DB は gzip 前で 1.4 GB)。復元はファイルを 2 回読む = 1 回目は検証だけ (おかしければ何も消さずに止まる)、2 回目で流し込む。1 回目と 2 回目で行数が違えば巻き戻す (`RESTORE_SOURCE_CHANGED`)
- 採番 (identity / serial) の記録がダンプと復元先で食い違っていたら、**何も消さずに** 止まる。抜けたまま戻すと次の登録が主キー重複で落ちるため
- `ops.schema_migrations` は戻さない (復元先の履歴のまま) = 復元の行数は verify の行数より migration の数だけ少ない (正常)
- 🚨 **mart は取っていない** (`dump.mjs` の SCHEMAS)。mart の実体の表 = 売上日次 (`mart.sales_daily` ほか 5 表) と `finance_daily` は core から作り直せる派生データ → **復元の後に売上日次を作り直す** (売上日次を公開しているモールごとに `mall-orders.mjs --mall <m> --refresh-sales --all`。Render の受け口経由 = 復元した DB が本番の `COMPANY_DB_URL` になってから。途中で打ち切られたら `--all` を外して流し直す = 同じ回の続きから)。作り直すまで `mart.v_sales_daily` は空・見張りの W9 / W6 / W8 は blocked か breach になる。
  `finance_daily` (0012・Amazon 財務) は今は空で、この作り直しの対象ではない = 財務 (F2b) を入れるときに作り直し方を決める

**復元訓練** (Codex の条件。年 1 回 + DDL を大きく変えたとき):
1. Render で新しい Postgres を作る (名前は `company-db-drill` など。最小プランでよい)
2. その External URL を控える (中原さん。Claude は値を見ない)
3. Drive から最新のダンプを 1 つ落とし、先頭の `-- migrations:` 行を見る (`gzip -dc <file> | head -5`)
4. `COMPANY_DB_URL=<drill の URL> node scripts/company-db/migrate.mjs --to <ダンプの最後の番号>` で **ダンプと同じ版まで** 表を作る
   (🚨 全部当てると復元先のほうが新しくなり、`RESTORE_MIGRATIONS` で拒否される。新しい migration は復元のあとに当てる)
5. `node scripts/company-db/backup-cli.mjs verify <file>` で行数を見る
6. `COMPANY_DB_URL=<drill の URL> node scripts/company-db/backup-cli.mjs restore <file> --yes`
7. 残りの migration を当てる (`migrate.mjs` を番号なしで)。そのあと `/status?counts=1` 相当で件数を本番と見比べる
8. 売上日次を作り直す (上の「mart は取っていない」)。drill では関数を直接、売上日次を公開しているモール (rakuten/main・amazon/jp・aupay/main・linegift/main・qoo10/main) ごとに:
   `select * from mart.refresh_sales_daily(1::smallint, '<mall>', '<scope>', 100, <1 回目だけ true>, 'drill')` を `remaining = 0` まで →
   `select count(*) from mart.sales_daily_check(1::smallint, '<mall>', '<scope>', '2025-01-01'::date, <ダンプの前日>::date)` が 0 (全期間で材料との食い違いなし)
9. 確認できたら drill の DB を消す。かかった時間と件数を `07_初期ロード_名寄せレポート` に追記する

**miniPC だけで訓練する** (2026-09-27 に実施。Render に DB を作らない = 費用なし・中原さんの操作なし):
- PostgreSQL の持ち運び版 (EnterpriseDB の `postgresql-<Render と同じ版>-windows-x64-binaries.zip`。版は `select version()` で見る) を一時フォルダ (例 `C:\Users\bfaith\drill-<日付>`) に展開して:
  ```
  pgsql\bin\initdb.exe -D <一時フォルダ>\data -U postgres -A trust -E UTF8 --locale=C
  (<一時フォルダ>\data\postgresql.conf に listen_addresses = '127.0.0.1' と port = 55432 を足す)
  pgsql\bin\pg_ctl.exe start -D <一時フォルダ>\data -l <一時フォルダ>\server.log -w     ← SSH が切れても止まらないよう Win32_Process の Create で起動
  pgsql\bin\createdb.exe -h 127.0.0.1 -p 55432 -U postgres drill
  ```
  🚨 `trust` = この PC の利用者なら誰でもパスワードなしで入れる (127.0.0.1 だけ・訓練のあいだだけなので許す)。miniPC を他の人が使う時間帯は避ける
- ダンプは `rclone copy gdrive:bfaith-backup/render/daily/<file> <一時フォルダ>`。接続先 `postgres://postgres@127.0.0.1:55432/drill` (localhost なので TLS なし = `pgClientOptions` の約束どおり)
- 終わったら `pgsql\bin\pg_ctl.exe stop -D <一時フォルダ>\data -m fast -w` → 一時フォルダごと消す (データの置き場所とダンプの写しも = 会社のデータ)
- **2026-09-27 の結果**: ダンプ (9/27 03:30・0033・gzip 261 MB) の verify 5,737,687 行・29 秒 → 0033 まで作って restore **5,737,654 行 (差 33 = schema_migrations)・1,148 秒** → 残りの migration → 売上日次の作り直し 5 モール 377 秒・検算の食い違い 0 → 本番と比べて 2025 年の注文・出荷・在庫の日次・マスタの件数・`v_sku_stock` が一致。8 月の売上日次は Amazon だけ 980 円違う = ダンプの後に本番で 8 月の注文 1 件がキャンセル (差の理由まで確かめた)

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

## 持ち主の epoch (0055。マスタ正本切替 ④a・Codex #1564 R1 H1・R2 High 3)

列ごとの持ち主 (`config/master-ownership.mjs`) を 3 つに分ける。**config を書き換えてデプロイしただけでは何も変わらない**:
- **configured** = config/master-ownership.mjs (コードに書いた「こうしたい」)
- **prepared** = 人が `master-ownership-epoch.mjs prepare` で記録した「次にこれにする」。明示して頼んだロード (`--use-prepared`) と、その後の写しの世代だけが使う
- **active** = 今使っている持ち主 (`ops.master_ownership_state`)。毎晩の夜間ロード・miniPC の写し (fetch.mjs)・m_products の作り直しはこれ。**行が無い = 全部 load** (今)
- 変更の記録 = `ops.master_ownership_events` (足すだけ。init / prepare / cancel_prepare / activate と、activate のときの確かめの証拠)
- 記録の後に足した列 (後の PR で `OWNED_COLUMNS` に足した列 = 記録した持ち主に無い列) = **'load' として足す** (夜間ロード・写しは止まらない。`status` の `filled_as_load` に出る)。知らない列・知らない値・ハッシュが中身と違う = 壊れ (推測しない = 止める)

切替の日の順番 (🆕 #1610 R1〜R3 で直した = 正本は AI_reference 17 §4.2 の表。🚨 **古い書き込み口 (/register など) を閉じるのは ⑤-3b = 持ち主が C の列の入口だけ・prepare の後の frozen で閉じる**):
1. config/master-ownership.mjs を書き換えてデプロイ (ここでは何も変わらない)。配る前の確かめ `master-legacy-readiness.mjs` が両方の環境で終了コード 0
2. miniPC: `node scripts/company-db/master-ownership-epoch.mjs prepare` (一緒に切り替える組・④a が写さない列を確かめて記録。段階は legacy_open のまま = 入口はまだ開いている)
2a. `master-cutover.mjs --to frozen` (この瞬間から prepared の C の列の入口だけ閉じる) → 全部のプロセスの書きかけ 0 (`master-legacy-instance.mjs --list`・`pg_locks` の段階の共有の鍵 0)
2b. **最後の同期 = active (全部 load) のロード** (NE の取得 → 作り直し → Render の同期 → `remote-load.mjs load --apply --wait` = `--use-prepared` を付けない) = prepare をまたいで書き終えた古い入口の値を回収 → そのロードの run_id の report の成功 + 照合 ② (照合 ① は 02:00 のロードだけを見る)
3. `node scripts/company-db/remote-load.mjs load --apply --wait --use-prepared` (prepared の持ち主で 1 回だけロード)
4. miniPC: `node apps/company-db/publish/fetch.mjs` → `node apps/warehouse/rebuild-m-products.js` → `node apps/company-db/publish/fetch.mjs --verify-apply` (prepared の世代を入れて確かめる)
5. miniPC: `node scripts/company-db/master-ownership-epoch.mjs activate` (最新の作り直しが prepared の世代・その世代が prepare の後に Company DB を読んだ・今朝の確かめが通った・読み直しても同じ、
   かつ **⑤-1 の切替の段階 (`ops.master_cutover_state`) が `frozen`** (prepared の C の列の古い入口を止めた後・持ち主を C にする前) のときだけ active に。足りなければ理由を出して断る。段階の表が無い = 断る。
   証拠を集めたときの prepare の時刻を行の鍵の後に比べる = その間に prepare をやり直したら `PREPARED_CHANGED` で断る (やり直しは **2a から** = 書きかけ 0 → 2b の最後の active のロード → 照合 ② → 3。新しく C に入った列の入口は、やり直しの prepare までは開いていたため)。
   証拠の世代が読んだ夜間ロードが最後のロードでない (証拠の後に毎晩のロードなどが入った。DB の commit の番号で比べる) = `LOAD_AFTER_EVIDENCE` で断る (やり直しは **2b から** = 最後の active (全部 load) のロード → その run_id の report の成功 + 照合 ② → 3 の --use-prepared。介在したロードの結果を確かめないまま 3 に進まない。#1564 Codex R3 High 1・R4・#1610 R5)
- 途中で止める = `master-ownership-epoch.mjs cancel` (prepared を消す。active はそのまま = 毎晩は前の持ち主)。🚨 cancel は prepared だけを消す (active はそのまま) = **activate の前**なら、今回 prepared で C にした列 (= active ではまだ load の列) の古い入口が、段階が frozen のままでも再び開く (10/5 = active は全部 load なので 13 キーすべて)。**activate の後**は prepared が無いのが正常 = active が C なので入口は閉じたまま・cancel は対象外 (門は active ∪ prepared で見る)。入口を閉じたまま夜を越すなら cancel しない (prepared を残す)。cancel したらスタッフに知らせ、翌日は prepare からやり直す。prepared を残して 02:00 のロードが走ったら activate は LOAD_AFTER_EVIDENCE で止まる = 2b (最後の同期) からやり直す。今の状態 = `master-ownership-epoch.mjs status`

**マージの後の手順 (🚨 まだ流さない = migrate は中原さんの指示の後に dry-run → 本適用)**。0055 は表を 3 つ (epoch・その記録・夜間ロードの commit の順) と、⑤-1 の切替の段階・画面の保存の門に「持ち主の epoch と同じ」の確かめを足すだけ (行は作らない = 全部 load のまま = 何も変わらない)。
あわせて ⑤-1 の `ops.ownership_hash` の式を「load の列は数えない」に作り直す (下の「1 つの式」)。🚨 段階が company_owner / new_open の DB では 0055 は止まる (本番は legacy_open = 当たらない)。
🚨 **番号**: master 0050 (finance_coverage) → ⑤-1 0051 (master_edit) → ⑤-2a 0052 (master_registrations) → ⑤-2b 0053 (ne_registration_csv) → ⑦-1 0054 (amazon_sku_maps) → この 0055 の順に積む (migrate.mjs は欠番・重複を拒む)。
🚨 **デプロイは Render と miniPC を同じ日に**: 夜間ロードの規則の指紋 (`engine.mjs` の `LOAD_RULE_FILES`) に `apps/company-db/load/ownership-state.mjs` が入った (engine.mjs も変わった)。
Render (夜間ロード) と miniPC (朝の照合 ①) のコードが違う日は、朝の照合 ① が「規則の指紋がこのコードと違う」で判定できない (blocked) になる。同じ日の夜間ロードの前に両方をそろえる

```
node -r dotenv/config scripts\company-db\migrate.mjs --dry-run                 # 0055 だけが出ること (0051〜0054 は先に入っている)
node -r dotenv/config scripts\company-db\migrate.mjs                           # 0055 (applied=1)
node -r dotenv/config scripts\company-db\master-ownership-epoch.mjs status     # state = missing (行が無い = 全部 load)
```

### epoch の鍵・持ち主表のハッシュの 1 つの式 (#1564 Codex R3)
- **epoch の鍵** `ops.master_ownership_lock_key()` = **4705310055** (0036 の親子の鍵 4705310036・0051 のマスタの書き込みの鍵 4705310051 と同じ作り。2^31 より大きい = `hashtext()` の鍵とも重ならない)。
  - 夜間ロード (`engine.mjs`・`--use-prepared` も) = 取引の冒頭に**共有**で取ってから、取引の中で epoch を読む (書き終わるまで持つ)。prepare / activate / cancel (`ownership-state.mjs`) = **排他** = ロードの途中で epoch が変わらない
  - activate は鍵の後に「証拠の世代が読んだ夜間ロード = 最後に commit したロード」を見る (古い active で走ったロードは activate より前に commit している = 証拠の世代に入っていない = 断る)
- **夜間ロードの commit の順** `ops.master_load_commits` (#1564 Codex R4): 本適用のロード 1 回 = 1 行 (dry-run は無し・足すだけ)。番号 `commit_seq` は DB が commit の直前に振る
  (epoch の鍵 (共有) とマスタの書き込みの鍵 (排他) を持ったまま = 番号の順 = commit の順)。送り手の時計 (`started_at` / `finished_at`)・場所 (`host`) では並べない。
  - 写し (`publish/fetch.mjs` の `selectPublishLoad`) = 番号の一番大きいロード (毎晩の cron = `render-nightly` も、切替の日に HTTP で流した `--use-prepared` のロード = `render` も)。
    行がまだ無い (0055 の後に本適用のロードが無い) = 今までどおり毎晩の cron の最新。照合 ① (`compare-load.mjs` の `selectNightlyLoad`) は毎晩の cron の回のまま (別の目的)
  - 世代 (warehouse.db の `cdb_publish_generations.load_commit_seq`) と証跡 (`master-publish.load_commit_seq`) に番号を残す。activate はその番号 = 一番大きい番号か
    (`LOAD_AFTER_EVIDENCE`)・最後のロードの持ち主が prepared か (`LOAD_EPOCH_MISMATCH`) を鍵の後に見る
  - 🚨 **鍵の順** (全部の書き手で同じ = デッドロックしない): epoch (0055) → 切替の段階 (0051 の `hashtext('ops.master_cutover')`) → マスタの書き込み (0051) → 親子 (0036) → 行。
    夜間ロード = epoch 共有 → 書き込み 排他 → 親子 / activate = epoch 排他 → 段階 共有 → 行 / 画面の保存・登録 = 段階 共有 → 書き込み 共有 (epoch は取らない)
- **持ち主表のハッシュは 1 つの式** = 持ち主が `load` でない列だけを `[キー, 値]` にしてキーの順に並べた JSON の sha256 (= 記録に無い列は load と同じ)。
  `lib/master-cutover.mjs` の `ownershipHash` (画面・門の記録)・`ownership-state.mjs` の `ownershipHashOf` (epoch)・`master-publish.js` の `ownershipHash` (写しの世代・作り直しの記録)・
  夜間ロードの記録 (`ops.load_materials.ownership_hash`)・DB の `ops.ownership_hash` (0055 で作り直し) が全部これ。
  切替の後に `OWNED_COLUMNS` に列を足しても (足した列は load)、段階の記録 (`owner_hash`)・epoch・画面の保存のハッシュは変わらない (保存・登録が `before_cutover` にならない)。
  写しは夜間ロードが記録した持ち主表から今の式でハッシュを作って比べる (式を変えた日に前の式で記録したロードがあっても偽の食い違いにしない)
