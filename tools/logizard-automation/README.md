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
| `run-nyuka-csv-scheduled.bat` | miniPC | 00:20 / 08:40 / 11:45 の定時の入口 (入荷受付 CSV → 毎日の商品マスタの取込 (影) → 商品マスタの書き出し) |
| `auto-barcode.js` | Stream Deck の PC | 入荷バーコード連携 (① 新商品の取込 → ② バーコード情報の書き出し → ③ 毎日の商品マスタの取込)。**JST 00:00〜01:30 は動かない**・`LOGIZARD_BC_DAILY=auto` で ①② だけ (下の「入荷バーコード連携の決まり」) |
| `barcode-mode.js` | Stream Deck の PC | auto-barcode.js の起動の決まり (夜の止め・①②③ か ①② か・引数) (③c-1b-3a) |
| `run-barcode.bat` | Stream Deck の PC | Stream Deck から叩く入口 |
| `import-state-client.js` | 両方 | ポータルの「ロジザードの取込の状態」の口を呼ぶ・手元の初期化の印の読み書きと照合 (③c-1b-1) |
| `lz-import-screen.js` | 両方 | インポート画面 [PM07/FM07_01] の操作の部品。`previewImport` = 取込パターンを選んで CSV のプレビューまで (③c-1b-2a) / `executeImport` = 実行ボタン → 「ファイルアップロードを開始します」のモーダルの中の OK だけ → 押した後に新しく出た結果の表示 (③c-1b-2b-1b。呼ぶのは中原さんと一緒の試験のランナー = 2b-1c から。影の取込は呼ばない)。画面全体の「最初の OK」は押さない |
| `import-guard.js` | 両方 | 押してよいかの旗 (止める理由・締め切り・押す持ち時間)。止めたら待っているクリックもページを閉じて中断 (③c-1b-2b-1b・契約 v3 K7) |
| `import-state-cli.js` | 両方 | 人が取込の状態を見る・直す (status / init / adopt / recover / halt / resume / resolve) |

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

## 取込の状態 (③c-1b-1・`apps/logizard-import-state`)

自動の ③ (miniPC) と戻し方の手の ③ (Stream Deck の PC) は、ポータル (Render) の 1 つの状態と鍵を共用する。決まり = `apps/logizard-import-state/README.md`。

- **token `LZ_LOCK_TOKEN`** (中原さんが入れる・Claude は中身を見ない): Render の env / miniPC = リポジトリ直下の `.env` (取込はリポジトリのスクリプトが読む) / Stream Deck の PC = `C:\tools\logizard-automation\.env` (auto-barcode.js が読む)。
- 手元の初期化の印: miniPC = `DATA_DIR\lz-import\init.json` / Stream Deck の PC = `C:\tools\logizard-automation\logs\lz-import-init.json`。
- 最初の 1 回 (token を入れた後):
  - miniPC: `node --env-file=C:\Users\bfaith\bfaith-portal\.env import-state-cli.js init --by 中原 --local C:\Users\bfaith\bfaith-portal\data\lz-import\init.json`
  - Stream Deck の PC: `node import-state-cli.js adopt --by 中原 --local C:\tools\logizard-automation\logs\lz-import-init.json --note "Stream Deck の PC"`
- 見る = `status`。止める = `halt --reason`。再開 = `resume --note` (未解決が無いときだけ)。解除 = `resolve --run <実行 ID> --outcome imported|not_imported|partial --note` (ロジザードのインポート履歴を確かめてから)。

## 入荷バーコード連携の決まり (auto-barcode.js・③c-1b-3a)

- **JST 00:00〜01:30 は動かない**。押しても理由を出して何もしない (CSV・鍵・ブラウザに触る前)。ログインの前・①②③ の各ステップの前・実行ボタンの直前でも時刻を見て、止めの中なら押す前に止める。
  - 理由: miniPC がロジザードの毎日の商品マスタを 00:15〜00:55 に同じ共通アカウントで扱う (同じ ID で 2 か所からログインするとセッションを追い出し合う)。専用アカウントは作らない (中原さん 2026-09-28)。
  - 止まった回の続き = 01:30 を過ぎてからもう一度押す (① ③ は前の成功と同じ中身なら飛ばす)。
- **どこまで動かすか = この PC の `C:\tools\logizard-automation\.env` の `LOGIZARD_BC_DAILY`**
  - 無い / `manual` = 今までどおり ①②③ (③ = GAS が作る毎日の商品マスタの CSV)。**切替日まではこのまま**。
  - `auto` = ①② だけ (③ の CSV を見ない・取り込まない)。切替日に中原さんが入れる (毎日の商品マスタは miniPC の自動が取り込む)。
- 引数は `--dry` だけ。知らない引数は断る (打ち間違いで本番が動かないように)。戻し方の手の ③ (`--only-daily`) は ③c-1b-3b (まだ無い)。

## 試験

`node scripts/test-logizard-automation.mjs` (書き出しの検証・auto-shohin-csv.js の切り出し・写し方・入荷バーコード連携の決まり) / `node scripts/test-logizard-import-state.mjs` (取込の状態)
