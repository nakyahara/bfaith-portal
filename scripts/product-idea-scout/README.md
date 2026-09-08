# Product Scout 品質修正の配布元

このディレクトリは、miniPCで従来Git管理外だった収集側コードのうち、今回修正するファイルの正本です。
2026-09-07にminiPCの稼働中ソースを読み取り取得しました。config.json・lib/keepa・秘密情報・実データは含みません。
既存タスクのスケジュール・成功pingを維持し、起動先をworktreeのrun-products.batへ切り替えます。

## 修正内容

- 医薬品を形態より先に除外。グミ・カプセル・替えブラシ等は別形態として要確認にする。
- 工程は商品名から判定。上位カテゴリの「オイル」等を現物の形態にしない。製造能力は証明しない。
- 寸法は3辺と重量が正の実数の場合のみ判定。JSONL破損を黙って捨てず、再取得ASINは重複集計しない。
- 商品ごとのobservedAt・parentAsinを今後の収集に保存。観測日不明・30日超は鮮度未確認。
- 親ASINが分かる購入表示は親ごとの最大値。購入表示集計は市場月販や下限とは呼ばない。
- 収集進捗はfinder対象ASINとの集合照合。カテゴリ付け替えや対象外商品の混入による分子誤りを防ぐ。
- export-own-products.cjsは売上分類1と2を分離し、発売日不明・セット・例外・終売も含める。
- ASINはm_sku_componentsとamazon_sku_feesを根拠に結合。複数構成SKUを単品のASINとして推定しない。
- 商品コード・仕入先・原価状態等を保持。初回180日不明をゼロにせず、一部SKUのみの値を全体と呼ばない。
- ポータルは自社=1かつ元の商品DBの更新が48時間以内の照合元のみ参照。JSON再生成時刻を鮮度にしない。旧行は保持し最新取込IDで対象を絞る。採否履歴は削除しない。
- 空要素・重複・不正な根拠を拒否。過去スナップショットへの巻き戻しと同一再送による鮮度偽装を防ぐ。

## 検証

リポジトリ直下で実行します。テストDBはメモリ内です。

```powershell
node apps/product-scout/test-schema.mjs
node apps/product-scout/test-ingest.mjs
node apps/product-scout/test-render.mjs
node --test apps/product-scout/test-quality.mjs
```

2026-09-07: 9 + 23 + 8 + 15 = 55件成功。
miniPCで本番ファイルの書き込みを無効化したメモリ内評価も実施。自社=1の全商品が抽出されることと、誤例3件が通過しないことを確認。

## 本番反映順（まだ未実施）

1. ポータル側の変更を先に反映。schema.jsの追加列・取込履歴表はトランザクションで作成。既存行と採否イベントを保持する。
2. miniPCでは本番masterを直接編集せず、反映済みブランチのworktreeを配布元にする。
3. 既存ProductIdeaScoutのActionだけを `C:\tmp\product-scout-quality-work\scripts\product-idea-scout\run-products.bat` へ変更する。実行中のインスタンスには影響させず、次回からこのworktreeの新コードを起動する。
4. 新ランナーはSCOUT_HOMEに既存product-idea-scout、WAREHOUSE_DBに本番warehouse.dbを指定。export-own-products.cjsを含むコードは全てworktreeから実行し、masterへ直接書き込まない。依存パッケージはworktreeに必要。現在の旧ランナーが後で旧テーマを送信した場合、v2より古い取り込みとして拒否され、公開データは巻き戻らない。
5. 自社抽出 → own.js --build → concepts.js → push.jsの順で反映する。データ生成・API送信を伴うため、公開作業として実施する。
6. Render画面で旧判定バナーが消えたこと、分類2が自社表示に混ざらないこと、採否履歴が残ること、jobs-monitorの既存pingを確認する。

新しい定期タスクは作りません。既存台帳はconfig/jobs-registry.mjsのproduct-idea-scoutに追記済みです。
実行中のバッチと実行ファイルは変更せず、次回起動のActionだけ切り替えます。algorithmVersion=2の取り込み後、旧版テーマの再送は拒否されます。

## 既存データの鮮度と残る課題

過去のproducts.jsonlには商品ごとの観測日時がありません。ファイル更新日時やfinderの抽出日を商品観測日へコピーしてはいけません。
実データ再評価ではこのため通過0件。機会なしではなく評価未完了です。対象カテゴリ・価格・重量等の既存条件は引き続き残っています。

既存実行ディレクトリで `node products.js --refresh-limit 100` を実行すると、finder対象内の観測日不明・30日超の商品を最大100件再取得できます。
1〜1000の整数を指定。これはKeepa APIを使用します。既存バッチと同時実行しないこと。今回API再取得は未実施。
再取得後concepts.jsで再集計します。通常実行は従来通り未取得ASINの取得を継続します。自動リフレッシュの定期化は今回の範囲外です。

今回の修正で、KW×用途×仕様の企画生成、AMCの製造実績の証明、見積・原価・広告費控除前利益の判定まで完成したわけではありません。
カテゴリ×工程の照合は類似品の不存在や重複なしを証明しません。ASIN対応がない商品も保持しますが、Amazonカテゴリへの配置は未完了です。

## 動作確認のみ

`SCOUT_HOME`に既存product-idea-scoutのディレクトリを指定し、Git管理のconcepts.jsを`--check`付きで実行すると生成物を書かず標準出力で確認できます。
本番の秘密情報は読み込みません。configと既存JSONLのみ読みます。--check出力はログ行とJSONを含みます。
