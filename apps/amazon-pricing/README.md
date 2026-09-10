# Amazon 価格管理 (自社プライスター) — `/apps/amazon-pricing`

Amazon の出品ごとに「いまの価格・カート・原価・手数料・粗利・販売」を 1 行で見て、
値付けの方針 (追従モード・赤字/高値ストッパー) を**変更履歴つきで記録**し、
ルールが出した「もし動くならこうする」(判定) を人が採点する道具。

**🚨 このバージョン (M1 + M2) は Amazon に何も書き込まない。** kill switch で止めているのではなく、
**書き込むコードが存在しない**。`node apps/amazon-pricing/test-no-write-path.mjs` が機械的に確かめる。

- 設計の正本 = AI_reference『システム設計/Amazon価格管理_自社プライスター_要件定義_20260907.md』
- 操作マニュアル = [`docs/runbooks/amazon-pricing-manual.md`](../../docs/runbooks/amazon-pricing-manual.md)
- 経緯: 2026-03 に旧ツール (profit-calculator の SQS ワーカー) が誤った価格を自動送信する事故。
  今回はその作り直し。旧ツールの書き込み口 (`POST /api/amazon/update-price`・ワーカー起動 API) は同じ PR で削除した。

## 何ができるか

| 画面 | できること |
| --- | --- |
| 在庫一覧 `/` | 全出品の 360 行を**プライスターの在庫一覧と同じ並び**で (ステータス / ASIN / 商品名・SKU / 数量 / 出品価格 / 最低価格 / 価格の自動変更のボタン列 / 仕入れ価格 / 高値・赤字ストッパーの直接入力 / 手数料・粗利益 / 売れた個数 / 自社の判定 / リサーチ)。詳細検索 (追従設定・配送区分・こだわり・価格範囲)・並び替え・チェックして一括・CSV |
| 出品 `/listings/:sku` | 1 出品の全部。方針の変更・変更履歴・判定の記録・価格の推移 (90日) |
| カスタムの型 `/custom-types` | プライスターの「オリジナルボタン」。名前付きの型 (基準・値動き・上乗せ・Amazon 本体・ポイント・独占時) を作って出品に割り当てる。型の変更は理由必須・追記のみの履歴 |
| 今日の判定 `/evaluations` | ルールの判定 (提案) を run 単位で見て 👍/👎/❓ で採点。作り直しボタン |
| 変更履歴 `/history` | 方針の変更 (誰が・いつ・何を・前後・なぜ)。追記のみ |

## 書き込み経路が無いことの保証

1. `apps/amazon-pricing/` に SP-API の書き込み関数・miniPC の書き込み口・通信・**動的実行** が無い (`test-no-write-path.mjs`)。
   検査は部分文字列ではなく、コメントを除いて正規化したソースに対して **危険な名前そのもの** (fetch / eval / Function /
   globalThis / window / self / Reflect / process / require / http …) を語として禁止する。名前の組み立て (計算プロパティに
   引用符・+・${})・optional call・.call/.apply/.bind も禁止。静的 import は許可リストだけで、**アプリから到達できる相対
   import を再帰的にたどり外部モジュール (warehouse-mirror/db.js 等) も同じ検査**。画面は `<script>` と `on*=` を取り出して
   同様に検査し、HTML の外部読み込み (script src / iframe / form action / img …) も禁止。自分の API への fetch だけを、
   文字列リテラルの連結 (`..` `//` `%` `?` `#` を含まない) に限って許す。EJS タグ (`<% %>`) の中身もサーバ JS として検査し、
   `\uXXXX` 等のエスケープは復号してから見る。**回避コードの見本 52 種を検査に掛けて必ず落ちる**ことも同じテストで確かめる
2. `ap_evaluations.autonomy_level` は `CHECK(autonomy_level = 0)` — 「人承認後に実行」「自動実行」の段階を表す値を表に入れられない
3. 実行要求の表 (`ai.actions` 相当) は**作っていない**。実行段階 (M3) で、設計書の関所を全部つけて別 PR で足す
4. 旧ツールの書き込み口は削除済み (`test-no-write-path.mjs` §3 で監視)。miniPC 側の価格更新ルート
   (apps/warehouse/research-service.js) は残っているが、Render から呼ぶコードはもう無い

**限界 (脅威モデル)**: 1 は構文木ではなく正規化した文字列に対する静的検査で (リポジトリに JS パーサが無いため)、実行環境の
外向き通信を遮断するものではない。DB のトリガ・CHECK が防ぐのは
普通のアプリ接続からの DML (UPDATE / DELETE / INSERT OR REPLACE / 履歴を伴わない方針の書き換え) で、DB ファイルの
DDL 権限を持つ人 (DROP TRIGGER・表の作り直し) からは防げない。それは render-backup の日次退避と、Company DB (Postgres)
移行後のロール分離で扱う。

## データ (warehouse-mirror.db の `ap_*`)

price-update の `pu_*` と同じく warehouse-mirror.db に同居 (同期で作り直されない・render-backup に自動で入る・mirror 表と JOIN できる)。
Company DB 構想 (03 DDL 草案) の形を SQLite で先取りしている。

| 表 | 何か | Company DB での対応 | 書き換え |
| --- | --- | --- | --- |
| `ap_policies` | 出品ごとの値付け方針の現在値 | `core.listings` の属性 | 上書き (履歴は下の表) |
| `ap_policy_events` | 方針の変更履歴 (誰が・いつ・列・前後・理由) | `events.*` | **追記のみ (トリガ)** |
| `ap_custom_types` | カスタムの型 (名前付きの設定のまとまり)。`ap_policies.mode='custom'` + `custom_type_id` が参照。消せない (使わない = `archived_at`)。使っている出品がある間は「使わない」にできない | `core.pricing_profiles` (仮) | 上書き (履歴は下の表。方針と同じトリガで履歴を先に要求) |
| `ap_custom_type_events` | 型の変更履歴 (誰が・いつ・列・前後・理由)。型を直すとその型を使う出品全部に効くので必須 | `events.*` | **追記のみ (トリガ)** |
| `ap_evaluation_runs` | 判定を作った回 | `ops.job_runs` | running → success/failed の 1 回だけ |
| `ap_evaluations` | 判定 = 提案 (action / 提案価格 / 理由コード / 確信度 / 入力の写し) | `ai.decisions` | **追記のみ**。`autonomy_level = 0` 固定 |
| `ap_evaluation_reviews` | 人の採点 (妥当 / 違う / わからない + コメント) | `ai.decision_reviews` | **追記のみ** |
| `v_ap_listing_360` (view) | 1 出品の全部を 1 行で (AI の入口) | `mart.v_sku_360` の価格版 | — |

- 時刻は UTC ISO (末尾 Z) で保存し、画面で JST にする
- 外部 ID (`seller_sku`) を鍵にしている。Company DB では `listing_id` に置き換える (external_ids で対応)
- **2026-09-10 の作り直し**: `ap_policies` の `mode` CHECK に `custom` を足し `custom_type_id` を追加した。SQLite は CHECK を ALTER できないので、
  起動時 (`createTables`) に古い形の表を見つけたら **1 トランザクションで写して入れ替える** (`migratePoliciesForCustom`: 件数と中身が EXCEPT 両方向で一致しなければ何も変えずに例外)。
  view は無い表を参照したまま残ると以後の DDL を壊すので先に消し、**トリガと view の復元まで同じトランザクションの中で**行う (途中で止まっても「トリガの無い新表」は残らない)。
  要否の判定はロックを取ってから見直す (別の接続が先に作り直していれば何もしない)。方針の履歴は触らない
- view は参照先の mirror 表が全部そろっている時だけ作る (無い表を参照する view は DB 全体の DDL を壊す — incidents/2026-05-15)

### AI が読むときの入口

```sql
-- いま赤字の疑いがある出品 (今の価格 < 原価+手数料から計算した下限)
SELECT seller_sku, ne_name, my_price, cost_incl_tax, referral_fee_rate, fba_fee FROM v_ap_listing_360 WHERE my_price IS NOT NULL;
-- ルールの判定に人がどれだけ同意したか (run ごと)
SELECT e.run_id, e.action, rv.verdict, COUNT(*) FROM ap_evaluations e
  JOIN ap_evaluation_reviews rv ON rv.review_id = (SELECT MAX(review_id) FROM ap_evaluation_reviews WHERE decision_id = e.decision_id)
 GROUP BY 1,2,3;
-- ある SKU の方針が、誰の手で、なぜ変わってきたか
SELECT at, actor_id, field, old_value, new_value, reason_code, reason_text FROM ap_policy_events WHERE seller_sku = ? ORDER BY event_id;
```

JSON で欲しいときは `GET /api/listings.json` (画面と同じ絞り込みが効く)、CSV は `GET /api/export.csv`。

## 判定ルール `rule:ap-v1` (engine.js)

- 下限 = ceil((原価税込 + 固定費) ÷ (1 − 販売手数料率 − 最低粗利率))。固定費 = FBA なら FBA 手数料、自己発送なら送料 (+ 1 品ごとの手数料)
- 実効下限 = 人の赤字ストッパーと計算した下限の**高い方**。ストッパーが空でも原価があれば下限は効く。**ストッパー 0 は入力自体を拒否**
- 下限を計算できない (原価不明・FBA 手数料不明や 0・送料不明・発送区分不明・販売手数料率が無い/範囲外) 行では**値下げを出さない** (hold)。
  **人の赤字ストッパーがあっても免除しない** (原価・手数料を知らずに下げてよい値は決められない)。値上げは上限まで
- カートの持ち主が分からない (buybox_is_mine が null) 行では値下げしない (自社カートを追いかけて下げ続けない)。上限 < 実効下限の方針は矛盾として保留
- `buybox` モード: カート価格 + 上乗せ → 下限・上限で止める → 変更幅 −30%〜+100% / +10 万円を超えたら保留
- カートが自分の価格の半分未満なら別物を疑って保留 / カート自社なら維持 / カート無しなら保留 (赤字なら下限まで上げる)
- `fba_lowest` / `lowest` は競合オファーの取得 (Phase 2) まで保留 (`NO_OFFER_DATA`)
- `custom` (型で決める): 型の「基準 = カート価格」「値動き (上下 / 値上げのみ / 値下げのみ)」「上乗せ (円 or 整数 %。方針の offset_jpy より優先)」だけが今の判定に効く。
  最安値・Amazon 本体を無視・実質価格 (ポイント)・独占時の値上げ を含む型は `CUSTOM_NEEDS_OFFERS` で保留 (M2.5 まで)。
  値動きの制限は競合追従の向きにだけ効き、**赤字の疑いで下限まで上げる提案 (RAISE_TO_FLOOR) は「値下げのみ」の型でも出す** (下限は方針より強い)。
  型が読めない (消された / 「使わない」) なら `NO_CUSTOM_TYPE` で保留。判定の inputs には判定時点の型の中身を写す (型は後から変わるため)
- すべての判定に `reason_code` + 日本語の `reason_text` + `inputs_json` (参照した値と出どころ) が付く

## 判定はいつ作られるか

- 一覧か「今日の判定」を開いたとき、**いまの入力** (最新日の価格の行数・同期時刻、手数料・原価の行数・取得時刻の指紋) に対する成功 run がまだ無ければ自動で 1 回 (`page_open`)。同じ日でも同期が進めば作り直す
- 「いま判定を作り直す」ボタン (`manual`、同じ日でも作り直す)
- cron は使わない (定期実行を増やすと台帳登録と監視が要る。入力が日次なので開いた時で足りる)
- 同時に開いても 2 本作らない (判定→保存を 1 つの immediate トランザクションで)

## 環境変数

無い。kill switch も無い (止めるものが無い)。閲覧の権限は `/admin` の画面権限 `amazon-pricing`。

## テスト

```
npm run test:amazon-pricing                        # 下の 4 本をまとめて
node apps/amazon-pricing/test-no-write-path.mjs   # ★書き込み経路が無い
node apps/amazon-pricing/test-engine.mjs          # 判定ルール (事故ルール 4 つを固定)
node apps/amazon-pricing/test-db.mjs              # 表・追記のみ・方針の履歴・run・読み取りモデル
node apps/amazon-pricing/test-views.mjs           # 画面 (テンプレ単体 + 実物のルート)
```

GitHub Actions の定義は [`docs/ci/amazon-pricing-tests.yml`](../../docs/ci/amazon-pricing-tests.yml) (PR で同じ 4 本 + `node --check server.js`)。
**★中原さんの作業 (1 回・2 手)**:
1. `docs/ci/amazon-pricing-tests.yml` を `.github/workflows/` へ移してコミット (Claude Code の GitHub トークンには workflow scope が無く、
   `.github/workflows/` 配下を push できない — 2026-09-07 に実際に拒否された)
2. repo の Settings → Branches → master の branch protection で `amazon-pricing tests` を必須ステータスチェックにする。
   これで「書き込み経路が無い」テストを落とす PR はマージできなくなる

## 次 (別 PR)

- Phase 2: 競合オファー (最安値 FBA/FBM) の日次取得 → `ap_price_observations` (miniPC の既存経路に 1 ジョブ足す。台帳登録)
- M3: 人が承認した 1 件だけ miniPC 経由で送る。price-update の関所 (kill switch・実行者名簿・claim・試運転・ブレーカー・読み戻し照合・unknown 非再送) を全部つける。`ap_actions` (= `ai.actions`) はそのときに作る
- 旧 `profit.db` の `listings.price_tracking / loss_stopper / high_stopper` を方針として取り込むか (設計書 D-5)
- PR-2 (profit-calculator 側): 出品が成功した時に、出品画面で選んだ追従モード (型を含む) とストッパーを `savePolicy` で方針として記録する (設計書 D-14)。
  型の一覧は `GET /api/custom-types.json` か `listCustomTypes(db)`
