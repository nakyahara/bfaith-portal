# tools/logizard-automation — ロジザードの自動化の正本 (の一部)

ロジザード (倉庫システム) の画面を自動で操作するスクリプトは、各 PC の `C:\tools\logizard-automation` で動く (git 管理外だった)。
マスタ正本切替 ③c-1b (ロジザードの毎日の商品マスタを自動で取り込む) で手を入れるファイルから、**このフォルダを正本にする** (中原さんの答え L-10・2026-09-28)。
設計 = AI_reference `CompanyDB構想/10` §6.3「③c-1b 契約 v3」。

## 入っているもの (manifest.json)

| ファイル | 写す PC | 何か |
|---|---|---|
| `logizard-common.js` | miniPC・Stream Deck の PC | .env の読み込み・セッションの鍵・ログイン・画面待ち (共通の部品) |
| `csv-util.js` | 両方 | CSV の読み書き |
| `shohin-export.js` | 両方 | 商品マスタの全件の書き出し (エクスポート[FM08_01]) と検証。auto-shohin-csv.js と ③c-1b の取込が使う (契約 v3 H8) |
| `auto-shohin-csv.js` | 両方 | 商品マスタの書き出し (miniPC の 00:20 の定時 `Logizard-NyukaCSV` の 2 ステップ目) |
| `export-shohin-to.js` | miniPC | 商品マスタの全件を好きな場所へ書き出すだけ (`--out <ファイル>`。本番の保存先・Drive・その日の成功の印に触らない)。書き出しの部品の実機の確かめ・③c-1b の少数件の試験 |
| `run-nyuka-csv-scheduled.bat` | miniPC | 00:20 / 08:40 / 11:45 の定時の入口 |
| `auto-barcode.js` | Stream Deck の PC | 入荷バーコード連携 (① 新商品の取込 → ② バーコード情報の書き出し → ③ 毎日の商品マスタの取込) |
| `run-barcode.bat` | Stream Deck の PC | Stream Deck から叩く入口 |

**ここに無いファイル** (auto-zaiko.js・auto-nefuda.js・auto-kinkyu.js・auto-nyuka-csv.js・auto-hikiate.js ほか・package.json・.env) は、まだ各 PC のまま (正本はこのリポジトリに無い)。
`.env` (ID・パスワード) は写さない・読まない。

2026-09-28 に入れたときの版は、miniPC と Stream Deck の PC で中身が同じだったもの (sha256 で確かめた) をそのまま入れた。その後の変更はこのフォルダで PR にする。

## 写し方 (その PC で)

```
git pull                                                   # リポジトリを最新に
node tools/logizard-automation/deploy.mjs --pc minipc      # 何が変わるかを見るだけ
node tools/logizard-automation/deploy.mjs --pc minipc --apply
node tools/logizard-automation/deploy.mjs --pc minipc --check   # 写したものとリポジトリが合っているか
```

- Stream Deck の PC は `--pc streamdeck`。写す先を変えるときは `--target <dir>`。
- 写すのはコミット済みの中身だけ。前のファイルは写す先の `deploy-backup\<実行 ID>\` に残る。どのコミットを写したかは `DEPLOYED.json`。
- 戻す = `--rollback <実行 ID>`。**戻せるのはいちばん新しい回だけ** (2 回前へ = 1 回ずつ)。その回の後に写す先で直されていたら戻さない。
- **写す・戻すあいだは、ロジザードの自動化の鍵 (`logs\logizard-session.lock`・`logizard-barcode.lock`) を deploy.mjs が自分で持つ** = そのあいだ定時や Stream Deck は始まらない (00:20 の bat は最大 10 分待つ)。鍵がすでにある (動いている) = 断る。miniPC は 00:20 / 08:40 / 08:50 / 11:45 と毎時 00 分を避ける。動いていないのに鍵が残っている = 中の pid を確かめて消す。
- 途中で失敗 = それまでに替えたものを戻す。戻しきれなかったら、そのファイルの名前・理由・前のファイルの場所 (`deploy-backup\<実行 ID>\`) を出して 1 で終わる。
- 戻すときは、替えたファイルを**全部**前に戻せたときだけ、足したファイル (例: shohin-export.js) を消す (替えたものが戻らないまま部品を消すと、次の定時が読み込みで止まる)。戻しきれなかった = 原因を直してから同じ `--rollback <実行 ID>` をもう一度 (もう戻したもの・もう消したものはそのまま続きから)。
- 写す途中で失敗して戻しきれなかった回は、DEPLOYED.json に `state: failed_partial` として残る。その間は次を写さない (`--apply` が断る)。原因を直してから、出てきた実行 ID で `--rollback` する。
- `--check` が 1 で終わる = 写した後に写す先のファイルが直された、またはリポジトリのほうが新しい。

## 試験

`node scripts/test-logizard-automation.mjs` (書き出しの検証・auto-shohin-csv.js の切り出し・写し方)
