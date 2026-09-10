> 2026-09-10 現行：案数目標なし。ただし対象・需要・既定NG・ブランド・自社/既出重複の選別を必須とする。R01発案→R03選別→代表の判断→次回へ反映。運用は[KW_RUNBOOK.md](KW_RUNBOOK.md)。旧88案版はフィルターを通っておらず公開不可。

﻿# 商品案KWスカウト W02 / W03 実装（2026-09-09）

要件/W01 v1.2、W04 v1.1 (`w04-*-2`, `w04-20260909`) の引き継ぎを受けた部品。
2026-09-10: 現行のKW案ルートは [KW_RUNBOOK.md](KW_RUNBOOK.md) と kw-*.cjs。以下のW02/W03部品の説明は旧メーカー相談用契約の履歴を含む。製造・採算・全Q通過をKW案の条件に使わない。

## 内容

| ファイル | 担当 |
|---|---|
| cli.cjs | W02。native版Claude / npm版Codexをshellなしで起動。認証検査、課金経路の環境変数名検査、モデル/effort固定、stdin、タイムアウト、usage、エラー分類 |
| packet.cjs | W03-a。選択した最大5候補の型付き根拠、内部原価・仕入先コード・販売数量列を除くallowlist、鮮度欠測、キャッシュ、候補分布、usage区分 |
| validate.cjs | W04検証。ID・候補間根拠・事実の根拠・許可された取得要求・信号・数値/日付・公開前Q検証・掲載区分・省略理由を検査。規則番号を返す |
| budget.cjs | 通常7回＋再試行1回、工程別上限、90分/締切、枠切れ後の停止。再開時にsnapshotを渡す |
| stage.cjs | R03のパケット→キャッシュ→CLI→検査を接続。検証済み出力だけをキャッシュ。次runではrun_idを付け替えて再検査 |
| warehouse-input.cjs | W03-b入力。warehouse.dbをreadonly/fileMustExist/query_onlyで開く。意味ラベル不足時は件数だけを返して停止 |
| sealed-set.cjs | W03-b。再現可能なseed、低価格帯、素材/形態の分散、同ファミリー/同用途の除外、日付フィルタ、RSA公開鍵による正解封印 |
| test-ai.cjs | 合成データ33試験。API・推論・本番DB更新なし。テスト鍵はメモリ内だけ |
| seeds.cjs | R00。代表入力と能力根拠を、訂正履歴・入力由来・未確認フラグを残したR01探索種へ正規化。モデル呼出・外部取得・保存をしない |
| test-seeds.cjs | R00の訂正、重複、除外語、性能表現、C1/C2根拠、R01入力の試験 |
| run-ledger.cjs | W07。runごとの入力ハッシュ・期限・AI呼出予約・安全な実行メタデータを永続化し、同時実行と予約後クラッシュ時の自動再呼出を止める |
| r01-stage.cjs | R01。固定Sonnet 5プロンプト、run台帳の予算保存、形式検証を接続。検証済み候補だけを呼出元へ返す |
| test-run-ledger.cjs / test-r01-stage.cjs | W07/R01のロック、障害停止、結果保存範囲、固定プロンプト、JSON検証の試験 |

## 検証

```powershell
node --test scripts/product-idea-scout/ai/test-ai.cjs
node --test scripts/product-idea-scout/ai/test-seeds.cjs
node --test scripts/product-idea-scout/ai/test-run-ledger.cjs
node --test scripts/product-idea-scout/ai/test-r01-stage.cjs
node scripts/product-idea-scout/ai/cli.cjs
```

後者はバージョンと正規のログイン状態だけを検査し、モデルは呼ばない。認証ファイル・APIキー値を表示しない。
2026-09-09: ローカルNode v24.13.1 / miniPC v24.14.1の両方で33/33成功。
miniPC: Claude Code 2.1.252 / codex-cli 0.150.1。両方 `READY_FOR_BILLING_CHECK`。

## R00 探索KW入力

`seeds.cjs` は標準入力のJSONをR01（Claude Sonnet 5 / low）の型付き入力へ変換する。代表入力は `original_kw` と訂正後の `normalized_kw` を併記し、単語が一つしかない語・性能表現・カテゴリ名だけの語を勝手に補完や削除しない。性能・規制表現は後工程で確認すべきフラグとして残す。

- 指定済みの代表例はAI_referenceの `データ/product-scout-design/representative_seed_keywords_v1.json` に保存する。例は探索種であり、需要・製造可否・朝カード掲載を意味しない。
- C1/C2の能力根拠だけを製造起点の種にする。C3は「不可」とせず、能力の証明としてR01へ渡さない。
- `excluded_terms` に一致する語はR01へ渡さず、監査出力に除外理由を残す。既存案・見送り履歴は重複抑止のため別配列で渡す。
- 入力は最大50件、R01へ渡す種は最大12件、R01の出力候補枠も最大12件。入力がない場合はモデルを呼ばず停止する。

## W07 実行台帳

`RunLedger` は専用状態ディレクトリを受け取り、`run_id`、対象日、締切、入力ハッシュ、モデル計画、予算スナップショット、モデルID・usage等のメタデータだけを保存する。プロンプト本文、応答本文、認証情報、APIキー、社内原価は保存しない。

- 同じrunのロックがある間は `RUN_LOCKED` で停止する。期限を超えたロックだけを明示的な復旧操作で解除できる。
- モデル呼出前の予約は先に保存する。予約保存後にプロセスが落ちた場合は `RUN_NEEDS_REVIEW` で停止し、同じ呼出を自動で繰り返さない。
- R01は固定プロンプトと `claude-sonnet-5` / `low` のルーティングだけを使う。構造不正は `VALIDATION_FAILED` として台帳へ残し、後工程へ渡さない。
- 台帳は実行状態を扱うだけであり、定期実行・公開・ポータルDBの変更を行わない。

## 実推論を開始する条件

`invoke(stage, prompt, options)` は次を必要とする。

- `cwd`: 専用の絶対パス。`command` を指定しない場合、Windowsの正規npmインストール先を解決。
- `billing_attestation`: `{provider, additional_usage_disabled:true, checked_by, checked_at}`。正規UIでの実確認に基づく記録。2026-09-10のユーザー確認を保持し、取消まで有効。サブスク認証成功だけでは作らない。
- `budget`: RunBudgetインスタンス。`save_budget(snapshot)` は開始前の予約と終了結果を永続化。保存失敗なら推論しない。W07で既存ランナーとロック/再開管理へ接続する。

2026-09-10: 追加クレジット消費無効はユーザー確認済み。Sonnet/Opus/Codexの実応答を確認。Codexの実モデルIDはunknownのまま。枠切れ/認証切れの網羅的な実機試験は完了実績に数えない。
CodexのJSONLに実モデルが無ければ `MODEL_UNVERIFIED`。モデルが自己申告した名前を証拠にしない。
Claude/Codexのバージョン違い、管理者設定等を含む実行の最終確認はW02の残件。

CLIには任意のコマンド/URL取得機能を開放しない。取得要求はデータのみとして、別の取得プログラムが許可リストで処理する。
`by_candidate[id].search_terms / asins / doc_ids` が許可リスト。既存観測のASIN・能力資料IDも当該候補だけ参照可。新しい検索語は収集側で許可してから実行する。

## 根拠パケットとキャッシュ

`buildPacket` は原始商品マスタ全量でなく、W04形式へ整理された入力を受け取る。自由記述中の秘密情報を自動で見つけて除去できるという保証ではない。外部AIへの社内情報の許可範囲は未確定なので、本番資料はまだ推論へ渡していない。
観測は7日、有効期限のない能力資料はmissing_fields。元データの確認日を新しい日付で補完しない。
キャッシュキーは入力・ルール・プロンプト・モデル・effort・ソース版・判断版・取得許可リスト。`stage.cjs` は必要根拠の最短期限までに限定し、期限不明時は保存しない。
候補分布は `candidateDistribution(rows)` で工程/素材/価格帯とunknownを生成する。research_runへの永続化はW07の実行記録と接続する。多素材の案は各素材に数えるため軸の合計は案数と一致しない場合がある。
usage推定はUnicode文字数による計画用の値。プロバイダーのtoken上限や残量を厳密に測った値ではない。

## W04例の検査結果

更新後の実文書からJSONを読み取り、R01・R03の形式検査は通過（R03の要求検索語をテスト許可リストへ明示）。
R06の `amc_message` に情報源Keepaがないため `R06 D / MESSAGE_MISSING_SOURCE`。
R03のEV-771にはexpires_atが無いので `EV-771:freshness` を欠測として保持。
プロンプト本文は編集していない。

機械検査の成功は意味の正しさ・掲載許可ではない。文章内で入力にある数値を別の意味へ転用した場合などは機械比較だけで証明できない。
`semantic_review_required:true` を返す。R05の意味検査とR07の最終検査を省略しない。R06への入力はプログラムが検証済みrevisionから組み立てる。AIが作ったprogram_validationを信用しない。

## 封印抽出の残件

本番 `m_products` をreadonlyで確認: 分類1 2,081行、発売日あり1,767行、2024年以降884行。
`new_product_launch_date` は存在する。一方 `family_id/use/target/material/form/spec` の正規化列はないため `METADATA_REQUIRED`。商品名の語だけから意味ラベルを推定して試験を成立させない。

1. 分類1全体に、同用途を照合できる用途・対象物とファミリー/素材/形態/仕様の参照データを用意する（独自の商品マスタは作らない）。
2. 資料・履歴の `product_ids/use/target/effective_at` 対応を用意する。
3. 採点側が保持するRSA公開鍵（2048bit以上）を渡す。秘密鍵をプロンプト作成側へ置かない。
4. `selection={seed,count,low_price_max}` を記録し、既存ディレクトリ `answer_directory/input_directory/w04_directory` を指定した入力JSONをstdinへ渡す。

```powershell
Get-Content -Raw -Encoding UTF8 <準備した入力JSON> | node scripts/product-idea-scout/ai/sealed-set.cjs
```

本番の正解セットは未抽出。スクリプトは答案を平文stdoutへ出さず、暗号化してW04ディレクトリの外へ保存する。
同一出力の上書きを拒否。manifestは最後に保存する。
時点再現を保証する過去版入力がまだないため、生成モードは常に `current-input-target-exclusion`。モデル学習由来の漏れは排除したとしない。
現在のスキーマ/資料不足で意味的な除外が保証できない間は、合成試験の成功を実データ抽出完了と呼ばない。

## 配置と変更範囲

- ローカル: `C:/tmp/product-scout-ai-work`, branch `feat/product-scout-w02-w03`, base `1ff80633`。
- miniPC: 同パス、branch `feat/product-scout-ai-20260909`, base `955680d9`。新規ai部品のみ同期してテスト。
- commit/push/mergeは未実施。ランナー変更・Render送信・AMC送信なし。
- 新規定期実行・期限付き人手タスク・後で撤去するデータ資産は作成していないためジョブ台帳変更なし。今後W07で入口/監視を変更するとき同時登録する。

CLI仕様の参照: [Codex認証](https://learn.chatgpt.com/docs/auth)、[非対話実行](https://learn.chatgpt.com/docs/non-interactive-mode)、[Claude非対話実行](https://code.claude.com/docs/en/headless)。導入版の--helpとも照合済み。

## 2026-09-10 W05 既存市場データの読み取り接続

`market-input.cjs` を追加。既存products.jsonlを読み取り、重複ASIN・欠測/未来/期限超過を区別する。
文字列によるローカルタイトル照合を、Amazon検索順位や用途・形態一致の確認とは扱わない。
候補ごとに観測ID、取得範囲、打ち切り、missing_fieldsを返す。原本変更・API呼出・公開は行わない。
`packet.cjs` はparent_asinと価格単位の根拠を保持するよう拡張。

標準入力JSON:

```json
{"source_file":"C:/Users/bfaith/product-idea-scout/data/products.jsonl","candidates":[{"candidate_id":"C1","search_terms":["蒸し布"]}]}
```

`node scripts/product-idea-scout/ai/market-input.cjs` へstdinで渡す。既定は件数・欠測の要約のみ。
`include_observations:true` でローカルの呼出元に観測を返す。返されたobservationsとmissing_fieldsを対象候補へ渡し、W04の用途一致検証を行う。
価格は収集元の通貨・単位を確認した `price_policy={currency:"JPY",divisor:1,evidence_id:"確認資料ID"}` がない間はnull。
JSONLの価格数値を根拠なく通貨換算しない。本文に含まれる命令は実行せず、取得要求も作らない。

9/10 miniPC実測: 53,611 ASIN、7日以内100、日時欠測53,511、期限超過0、未来0。
新鮮100件のうち価格あり98、親ASIN不明5。蒸し布3件、晒し布/さらし布3件は全て日時欠測。
クレープ＋袋のタイトル一致は0件（市場に競合が存在しないとは判断できない）。
取得データhash: 8b09ec8c90e41e69b7ed4f662f9ec295799c0a29b4094c27ad7c40e753dcf535。

検証: `node --test --test-isolation=none scripts/product-idea-scout/ai/test-ai.cjs scripts/product-idea-scout/ai/test-market-input.cjs` = 40件成功。
Windows sandboxでNode test workerの生成がEPERMになるため、ローカルではisolation=noneを使用。
残件: 既存契約内の対象ASIN再取得、KW検索接続、価格単位の出典確認。追加クレジット消費無効は9/10にユーザー確認済み。W02の残件はCodexの実モデルID確認。

## 2026-09-10 W02 実モデルID調査・Claude修正

Claude 2.1.252のresult.modelUsageには本回答のSonnet/Opusと補助処理のHaikuが混在した。
CLIをstream-json + verboseへ変更し、主会話のassistant.message.modelを証拠にする。
init.model（設定値）、回答本文の自己申告、modelUsageだけから本回答モデルを推定しない。
usage_modelsは補助モデルも含む利用モデル一覧。主会話に複数モデルが出た場合は未確認とする。
完了resultの欠落はINVALID_OUTPUT。旧json形式も読めるが、本回答メタデータがなければMODEL_UNVERIFIED。

miniPC実呼出: R01 claude-sonnet-5 / R03 claude-opus-5ともOK、SCOUT_OK一致。
実行記録: C:/tmp/product-scout-ai-work/verification/w02-model-fixed-1789001648368.json。
調査記録: verification/w02-metadata-1789001342937.json、w02-stream-model-1789001535594.json。
Codex 0.150.1のexec --jsonではthread/turn/item/usageのみで実モデルIDなし。
gpt-5.6-terra指定・応答成功は確認済みだが、actual_modelはunknownのまま。設定値を実モデルの証明へ読み替えない。
Codex R05のMODEL_UNVERIFIEDを通過扱いにする変更はしていない。
CLIのcostUSDは見積値で、追加課金の発生を証明する値ではない（[Claude公式仕様](https://code.claude.com/docs/en/headless)）。
Codexのイベント仕様は[公式非対話実行ドキュメント](https://learn.chatgpt.com/docs/non-interactive-mode)も照合。

検証: ローカルtest-ai 35件、miniPC test-ai + test-market-input 42件成功。
追加課金無効はユーザー確認済み。実呼出は固定の短文テストだけで、商品データを送信していない。

## 2026-09-10 W02 Codexの別経路調査

miniPCの導入版0.150.1からapp-server generate-ts --experimentalで通信仕様を生成し、公式App Server資料と照合。
確認結果は次のとおり。

| 経路 | 取得できる情報 | 本回答の実モデル確認 |
| --- | --- | --- |
| thread/start、settings | 設定モデル | 応答の証拠にはしない |
| turn/completed | 完了状態・items | model欄なし |
| rawResponse/completed（内部向け） | responseId・usage | model欄なし |
| rawResponseItem/completed | 応答の内容要素 | model欄なし |
| model/rerouted | 切り替え元・切り替え先 | 条件付き通知。通知なしから指定モデルを確定できない |
| model/safetyBuffering/updated | バッファリング中のモデル | 条件付き通知で、完了応答の証明ではない |
| ThreadUsageBreakdownGroup | モデル別集計の型 | 生成されたClientRequestにthread usage取得メソッドなし |

調査記録（miniPC）: verification/w02-codex-alternate-model-audit-20260910.json。
生成仕様: verification/codex-0.150.1-protocol/。対象ファイルのSHA-256も記録。
今回の調査は仕様確認で、App Serverの実推論は行っていない（追加推論0回）。
現時点で既存サブスク経路から毎回の実モデルIDを確定できる方法は見つからず、R05はMODEL_UNVERIFIEDを維持。
内部の全経路で取得不可能と断定するものではない。CLIのバージョン更新・完了通知へのmodel追加があれば再検証する。
判定条件・指定モデル・ランナーは変更していない。市場データ整備はこの未確認事項と独立して進められる。
参照: [OpenAI公式App Server仕様](https://learn.chatgpt.com/docs/app-server)。

## 2026-09-10 W05 KW検索・対象ASIN再取得の実機確認

w05-collection.cjsを追加。既存r02-search.cjsとminiPCのlib/keepa.jsを接続し、設計書の代表例3語で実取得。
代表例由来の接続試験であり、R01が生成・承認した候補とは扱わない。
collectionPlanは最大3検索＋既存ASIN最大6件。検索10トークン/語＋詳細1トークン/ASINの上限を算出。
collectは呼出前予約・検索ごとの結果を永続化し、応答不正・保存失敗・取得漏れを区別する。
1要求の締切45秒。既存helperの60秒リトライ待機には入らない。offers/rating/buybox/update=0なし。
本番products.jsは完了時に外部通知するため使用せず、取得helperだけを呼び出した。

記録: ローカル・miniPC共通 C:/tmp/product-scout-ai-work/verification/w05-market-20260910.json。
元products.jsonlは変更せず、検索結果の位置・観測・取得漏れ・呼出別消費をこの記録へ保存。

| KW | 検索結果 | 鮮度内価格あり | 鮮度内購入観測あり |
| --- | ---: | ---: | ---: |
| 赤飯 蒸し布 | 20 | 20 | 4 |
| 晒し布 | 20 | 19 | 13 |
| クレープ 袋 | 20 | 20 | 1 |

既存の蒸し布/さらし布一致は各3件だが同じASINのため再取得は3ユニークASIN。
検索60枠と既存3件の和集合62 ASIN、詳細62件取得、取得漏れ0。
全62件で価格61件・鮮度内購入観測20件。消費92トークン（検索30＋詳細62）、開始300→終了208、計画上限93。
日時欠測だった3 ASINの新しい観測を取得。原本全53,611件を更新した意味ではない。

価格はdomainId=5の円単位（divisor=1）。NEWとBUY_BOX_SHIPPINGの価格種別を保持。
observed_atはKeepa lastUpdate、retrieved_atは取得時刻、demand_observed_atはlastSoldUpdate。
monthly_unitsはAmazon過去1か月購入表示の下限で、実売個数の確定値ではない。古い/不明な需要はnull。
packet.cjsは購入値の種類・需要日時・取得日時・価格種別を保持するよう拡張。
unitCountを確定入数とはせずpack_qty=null。候補組立時は各観測のmissing_fieldsを候補へ引き継ぐこと。
先頭最大20件で市場全体の調査完了ではない。用途/形態一致・入数・親ASIN単位の集計は未検証。

実行直前と各呼出前に既存node collectorの停止を確認し、残量に計画上限＋20の余裕を確認。
単発接続試験の衝突確認で、共有ロック・定期ランナー組込みは未実装。自動運用はW07で扱う。
ローカル・miniPCで関連51テスト成功（新規は収集5＋packet保持1）。AI推論・外部通知・公開・commit/pushなし。
次は候補ごとの用途/形態/入数確認、同一親ASIN整理、根拠パケット組立。R05実モデル未確認は別途未解決。

公式根拠: [Product Search](https://keepa.com/api-docs/product-search.html)、[Product Request](https://keepa.com/api-docs/product.html)、[Product Object](https://keepa.com/api-docs/product-object.html)。
## 2026-09-10 W05 保存観測の用途・入数・親ASIN点検

market-review.cjsでタイトル点検の注記を観測ASINに結び付け、数量矛盾と親ASIN別のグループを保持する。
タイトル明記の入数はtitle_claim_onlyで、商品ページ確認済みにしない。単位が枚/Countの数量欄と衝突すればconflict、単価null。
パック/セット/mを枚に換算しない。親不明を独立競合と確定せず、既知親の子ASINも削除しない。
判断の入力はverification/w05-market-annotations-20260910.json。保存観測のhashをレビュー結果へ記録する。

結果: 蒸し布20件はタイトル上関連。晒し布は調理用3/別用途14/保留3。クレープ袋は関連17/形態保留3。
全62件は既知親25グループ＋親不明25件。独立競合総数・市場シェアは算出しない。
B0GNY6ZXWLはタイトル100枚と数量10枚が衝突。B0D634KVCLの(100)は単位不明で、入数確定・単価計算を行わない。
蒸し布は40〜50cm・2枚組を比較仮説に置く。晒し布は蒸す/濾すを分離。クレープ袋は購入観測ありSKUの入数を優先確認。

人向け資料: verification/商品案_市場根拠整理_20260910.md。
機械可読結果: verification/w05-market-review-20260910.json。再生成: node verification/build-market-review-20260910.cjs。
新規4テスト成功。追加API/別AI呼出0回。商品ページ/画像の実確認は未実施。R03準備完了ではなくready_for_R03=false。
次は蒸し布の能力根拠・自社商品重複確認と、主要競合の選択SKU/入数確認。採算入力が揃ってからR03へ組み立てる。
## 2026-09-10 蒸し布の能力根拠・自社照合・競合入数

本番m_productsをreadonlyで分類1の2,081件に名称照合。びわこふきんアルファ2件(product_id 834/835)、蚊帳ふきん1件(2221)が一致。
17語による検索で、意味上の重複なしの証明ではない。自社公開販売ページも参照し、蚊帳ふきんは台拭き/食器拭きの掲載を確認。
AMC取扱リストと現行資料・自社マスタの布製品を確認したが、綿45cm角の裁断・端処理に適用できるC1/加工経路は確認できずQ05保留。
旧出口設計の繊維品一律除外より、現行上位要件の能力根拠別判断を優先する。
サンベルム公式ではK42129/K42229の標準内容量1枚。取得タイトル2枚組のB005IXG6E8/B0CKL26QYRは販売者セットの可能性を残して保留し、単価nullへ。
market-review.cjsが品番に結び付く外部数量根拠を保持するよう拡張。別品番TU30429に1枚情報を流用しない。
新規回帰1件を含むmarket-review 5テスト成功。レビュー再生成済み。詳細はverification/商品案_市場根拠整理_20260910.md追記。
DB照合記録はverification/steam-cloth-own-check-20260910.json。能力確認質問案も資料へ記載、送信していない。
次は既存加工先・加工経路の確認資料/回答が必要。2枚組のAmazon販売単位確認と採算入力も未完了。
## 2026-09-10 蒸し布の加工経路確認依頼を具体化

AMC公式会社概要を直接取得し、OEM相談窓口を確認。綿布裁断/端処理の受託記載は確認できず、能力確認済みにはしない。
verification/蒸し布_加工経路確認依頼_20260910.mdに送信文案・仮仕様・回答後の確認項目を作成。
verification/steam-cloth-capability-inquiry-20260910.jsonに経路候補・回答欄・W01正本hashを保持。
45cm角・綿100%・2枚組は比較仮説。個包装/端処理/ロット/価格は未決定。
ユーザーへ既存の確認資料・口頭確認の有無を照会済み、今回の作業完了時点で回答未受領。
外部送信は未実施。具体的な加工経路の回答または資料を得るまでQ05とR03準備は保留。
## 2026-09-10 ユーザー訂正：製造先を確定する前に案を提示する

目的は商品案作成。製造先未定でも案を残し、今後の製造先開拓を許容する。加工先の回答を案作成の前提にした進め方を撤回。
成果物: verification/商品案3案_20260910.md と .json。大小蒸し布2枚/料理用さらし3枚/家庭用クレープ袋20枚を仮仕様として具体化。
いずれも市場根拠・買う理由の仮説・未確認事項を併記。既知の代表例からの具体化で、未知KWの発見数に数えない。
加工経路確認依頼はparked_not_sentへ変更。ユーザーへの加工先資料質問も案作成を止める条件として扱わない。
案の状態はidea_draft/ready_for_idea_discussion。従来のready_for_R03=falseは厳格な自動実行入力の未完了状態であり、案を提示できない意味には使わない。
今後、案生成・比較の入口にC1/既存加工先確定を要求しない。量産可否や費用の未確認はそのまま明記する。