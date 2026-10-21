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
| `barcode-export.js` | miniPC | SKU のバーコード情報の**全件**の書き出し (エクスポート[FM08_01]・種類 SKU・抽出パターン バーコード情報・登録日で開始日なし・有効 + 無効) と検証。取込の試験 (③c-1b-2b・K4) が取り込む前と後に使う。auto-barcode.js の ② (直近 N 日でバーコードマスタ.csv を上書き) とは別 = ② の動きは変えない。2026-09-29 に本物で 5,188 行・12 秒・② の出力の 5,038 商品とバーコードが全部一致 |
| `export-barcode-to.js` | miniPC | バーコード情報の全件を好きな場所へ書き出すだけ (`--out <ファイル>`・`--dry` = 条件の設定まで)。バーコードマスタ.csv・既存のファイルには書かない |
| `export-shohin-to.js` | miniPC | 商品マスタの全件を好きな場所へ書き出すだけ (`--out <ファイル>`。本番の保存先・Drive・その日の成功の印に触らない)。書き出しの部品の実機の確かめ・③c-1b の少数件の試験 |
| `run-nyuka-csv-scheduled.bat` | miniPC | 00:20 / 08:40 / 11:45 の定時の入口 (入荷受付 CSV → 毎日の商品マスタの取込 (影) → 商品マスタの書き出し) |
| `auto-barcode.js` | 両方 | 入荷バーコード連携 (① 新商品の取込 → ② バーコード情報の書き出し) の CLI (Stream Deck・手)。**JST 00:00〜01:30 は動かない**。③ 毎日の商品マスタの取込は切替で外した (miniPC の自動・下の「入荷バーコード連携の決まり」)。2026-10-17 からポータルのボタン (下の「ロジザードに取り込む」) が本道 = この CLI は予備 |
| `barcode-mode.js` | 両方 | auto-barcode.js の起動の決まり (夜の止め・①② だけ・引数) (③c-1b-3a・切替の PR) |
| `barcode-steps.js` | 両方 | ① ② の画面の手順 (auto-barcode.js v0.1 から移した・dialog の扱い・夜の止めの押し方・取込はフェイルクローズ)。auto-barcode.js と bc-import-runner.js が同じものを使う。足したのは「① の実行ボタンの直前の呼び (beforeExecute)」と結果の件数だけ |
| `bc-import-runner.js` | miniPC | ポータルの「ロジザードに取り込む」の回を動かす (1 分おきの定時 `Logizard-BcImport`・下の「ロジザードに取り込む」) |
| `bc-import-client.js` | miniPC | ポータルの機械の口 `/apps/lz-bc-import/api` を呼ぶ (Bearer `LZ_BC_RUNNER_TOKEN`) |
| `run-bc-import-poll.bat` | miniPC | 定時 `Logizard-BcImport` の入口 (`logs\bc-import.log` に追記) |
| `run-barcode.bat` | Stream Deck の PC | Stream Deck から叩く入口 |
| `import-state-client.js` | 両方 | ポータルの「ロジザードの取込の状態」の口を呼ぶ・手元の初期化の印の読み書きと照合 (③c-1b-1) |
| `lz-import-screen.js` | 両方 | インポート画面 [PM07/FM07_01] の操作の部品。`previewImport` = 取込パターンを選んで CSV のプレビューまで (③c-1b-2a。サーバーエラーのモーダルは OK を押さずに止める・画面を残す) / `executeImport` = 実行ボタン → 「ファイルアップロードを開始します」のモーダルの中の OK だけ (本物 = jAlerts の確認の箱 #popup_container > #popup_content.confirm > #popup_message + #popup_panel (#popup_ok・#popup_cancel)。形がぴったりのときだけ枠と認める = お知らせの箱・入力欄つき・ボタン違いは押さない。2026-09-30 の実機の試験で見た形) → 押した後に新しく出た結果の表示 (③c-1b-2b-1b。呼ぶのは中原さんと一緒の試験のランナー = 2b-1c から。影の取込は呼ばない)。画面全体の「最初の OK」は押さない。押すのは、確かめと押すを同じページの中の処理で・click 1 回だけ (mousedown を出さない)・押してよい最後の時刻を過ぎていない・ボタンが一番上 (覆われていない) ときだけ |
| `import-guard.js` | 両方 | 押してよいかの旗 (止める理由・締め切り・押す持ち時間)。止めたら、押す段階の間はページを閉じて、まだ始まっていない押す処理を始めさせない (送った後に始まった押す処理は取り消せない = afterStop として押したと扱う。③c-1b-2b-1b・契約 v3 K7) |
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

毎晩の自動の ③ (miniPC) と、自動が止まったときの手の取込 (ポータルの画面) は、ポータル (Render) の 1 つの状態と鍵を共用する。決まり = `apps/logizard-import-state/README.md`。

- **token `LZ_LOCK_TOKEN`** (中原さんが入れる・Claude は中身を見ない): Render の env / miniPC = リポジトリ直下の `.env` (取込はリポジトリのスクリプトが読む) / Stream Deck の PC = `C:\tools\logizard-automation\.env` (auto-barcode.js が読む)。
- 手元の初期化の印: miniPC = `DATA_DIR\lz-import\init.json` / Stream Deck の PC = `C:\tools\logizard-automation\logs\lz-import-init.json`。
- 最初の 1 回 (token を入れた後):
  - miniPC: `node --env-file=C:\Users\bfaith\bfaith-portal\.env import-state-cli.js init --by 中原 --local C:\Users\bfaith\bfaith-portal\data\lz-import\init.json`
  - Stream Deck の PC: `node import-state-cli.js adopt --by 中原 --local C:\tools\logizard-automation\logs\lz-import-init.json --note "Stream Deck の PC"`
- 見る = `status`。止める = `halt --reason`。再開 = `resume --note "…" --halt-revision <番号>` (未解決が無いときだけ・番号 = `status` の `halt_revision` = 見た止めと今の止めが同じときだけ。違う = stale = 見直してから。③c-1b-3b)。解除 = `resolve --run <実行 ID> --outcome imported|not_imported|partial --note` (ロジザードのインポート履歴を確かめてから)。

## 入荷バーコード連携の決まり (auto-barcode.js・③c-1b-3a)

- **JST 00:00〜01:30 は動かない**。押しても理由を出して何もしない (CSV・鍵・ブラウザに触る前)。ログインの前・①②③ の各ステップの前・実行ボタンの直前でも時刻を見て、止めの中なら押す前に止める。
  - 理由: miniPC がロジザードの毎日の商品マスタを 00:15〜00:55 に同じ共通アカウントで扱う (同じ ID で 2 か所からログインするとセッションを追い出し合う)。専用アカウントは作らない (中原さん 2026-09-28)。
  - 止まった回の続き = 01:30 を過ぎてからもう一度押す (① は前の成功と同じ中身なら飛ばす)。
- **動かすのは ①② だけ** (切替の PR・L-23)。③ 毎日の商品マスタの取込 (GAS の CSV) はこの道具から外した = 設定に依らない。
  - 毎日の商品マスタは miniPC の自動 (00:20・台帳 `lz-daily-import`) が取り込む。自動が止まったときに人が取り込むのは、ポータルの画面「ロジザードの取込の状態」の手の取込。
  - 前の設定 `LOGIZARD_BC_DAILY` が .env に残っていても ①② だけ (起動のときに「消してかまいません」と出る)。
  - GAS の ③ に戻すのはシステム全体を旧方式に戻すときだけ = 固定の版 (tag `lz-gas-rollback-20260930`・台帳 `lz-gas-rollback`) を配る。手順 = `db/company/README.md`「毎晩の本番の切替と GAS への戻し」。
- 引数は `--dry` と `--show-mode` だけ。知らない引数は断る (打ち間違いで本番が動かないように)。
  - `--show-mode` = 配った版の読み戻し (①② だけの見出しを出して終わる・ログイン・CSV・鍵に触らない・夜でも動く)。③ のある古い版は「知らない引数」で止まる。
  - `--only-daily` = 「③ はこの道具から外した・手の取込はポータルの画面で」と出して止まる。

## ロジザードに取り込む (ポータルのボタン・2026-10-17)

中原さん「ロジザードに新商品の登録自動アップロードって動いているの？ できればボタン押してその都度動かすものもほしい」。
事務所のメイン PC を作り直して `C:\tools\logizard-automation` が無くなり、Stream Deck の道 (auto-barcode.js) は今は動かない → **miniPC** (ロジザードの自動化が動いている PC) がポータルの画面のボタンの回を動かす。

- **運び方 = miniPC が取りに来る** (miniPC に新しい受け口を開けない)。画面 (`/apps/master-edit/lz-csv`) のボタン → ポータル (Render) の帳面 `DATA_DIR/lz-bc-import.db` に「待ち」→ miniPC の `bc-import-runner.js` (1 分おき) が `POST /apps/lz-bc-import/api/claim` (Bearer `LZ_BC_RUNNER_TOKEN`) で取る。
  ロジザードの取込の状態の口 (`LZ_LOCK_TOKEN`) と同じ「miniPC → Render の機械の口」の形。token は別 (この口では取込の状態を変えられない)。
- 渡すファイル = Company DB の帳面 (ops.lz_new_exports) のファイルそのもの。渡す前にポータルが「取り消していない・sha256 が積んだときと同じ」を見て、miniPC も sha256・Shift-JIS・見出し 8 列・行数を確かめてから置く (`work\bc-import\job-<番号>\logizard_bc_upload.csv`・共有ドライブに置かない)。
- ① の実行ボタンを押す直前に `/executing` を呼ぶ (ポータルの帳面を「押した」にする)。断られた (やめた・夜・取込の状態の止め・期限切れ) / 届かない = 押さない。押した後に途切れた = 「取り込めたか分からない」= 人がロジザードのインポート履歴で確かめて画面で記す。
- 鍵: ロジザードのセッションの鍵 `logs\logizard-session.lock` (毎晩の取込・在庫・値札と共用)。使われている間は取らない (画面に「忙しい」)。鍵があると待たずに終わる定時の前後も取らない (毎時 :50〜:02 = 在庫の毎時の取込・08:26〜08:34 = 値札の CSV)。自分の二重起動は `logs\bc-import-runner.lock`。
- 夜: miniPC は JST 23:50〜01:30 は取らない (ポータルは 23:45 から積まない・23:50 から渡さない / 押させない)。auto-barcode.js の夜の止め (00:00〜01:30) も barcode-steps.js の中でそのまま効く。
- この PC で同じ中身を前に取り込んだ (`logs\barcode_imported.json` の import1・auto-barcode.js と同じ記録) = 画面で「もう一度」(理由) を選んでいなければ押さない。ボタンで取り込んだら同じ記録に書く (miniPC の CLI も同じ中身を飛ばす)。
- **② (バーコードマスタの書き出し) はポータルが共有ドライブに書く** (2026-10-21〜・miniPC には G: が無い)。miniPC は ② を `work\bc-import\job-<番号>\barcode_master.csv` に置き、ロジザードを閉じた後に `POST /apps/lz-bc-import/api/jobs/<番号>/export-file` で送る →
  ポータル (`apps/lz-bc-import/drive-export.js`) が中身を確かめて (Shift_JIS のまま・見出し 商品ID / バーコード・4,000 行以上・今のファイルの半分より小さくない)、共有ドライブ「入荷バーコード発行」の `バーコードマスタ.csv` (同じファイル ID) を上書き。
  ポータルが書けない (Render の env `LZ_BC_DRIVE_WRITE=1` が無い・サービスアカウントが閲覧者のまま) = 画面の ② の印が外れて押せない (① だけ)。② が書けなくても ① は取り込んだまま。
  claim で `export_upload: true` と言わない古い runner (v1) には、ポータルが ② の印を外して渡す (① だけ)。
- 終わりの知らせが届かない = `logs\bc-import-pending\job-<番号>.json` に残し、次の回が先に送る (送れるまで新しい回は取らない)。
- 台帳 = `config/jobs-registry.mjs` の `lz-bc-import-runner` (heartbeat・1 時間に 1 回 `ping.ps1`)。

### 配り方 (中原さんの承認の後)

1. **Render の env**: `LZ_BC_RUNNER_TOKEN` = 長い乱数 (例 `node -e "console.log(require('crypto').randomBytes(32).toString('hex'))"`)。マスタの入力 (`MASTER_EDIT_ENABLED=1`) と同じ Render にだけ。無い = 機械の口は 503 (miniPC が来ない = ボタンは押せない)。
   押せる人 = `MASTER_DECISION_APPROVERS` と `MASTER_EDITORS` の両方に入っている人。
2. マージ (Render が自動で更新)。
3. **miniPC** (`git pull` の後):
   ```
   node tools/logizard-automation/deploy.mjs --pc minipc            (何が変わるか)
   node tools/logizard-automation/deploy.mjs --pc minipc --apply
   node tools/logizard-automation/deploy.mjs --pc minipc --check
   ```
   (写すあいだはセッションの鍵を deploy.mjs が持つ = 00:20 / 08:40 / 08:50 / 11:45 と毎時 00 分を避ける)
4. miniPC の `C:\tools\logizard-automation\.env` に `LZ_BC_RUNNER_TOKEN=<1 と同じ値>` を足す (中原さんが入れる)。ほかは今の値のまま (`LOGIZARD_USER_ID` / `LOGIZARD_PASSWORD` = 共通アカウント・`LOGIZARD_HEADLESS`)。ボタンの道は `LOGIZARD_BC_OUT` を使わない (② はポータルが共有ドライブに書く・Stream Deck の auto-barcode.js は今までどおり)。
5. 手で 1 回: `cd C:\tools\logizard-automation` → `node bc-import-runner.js` (待ちが無い = 何も出さずに 0 で終わる)。画面の「③ ロジザードへの取り込み」が「miniPC: 来ている」になる。
6. 定時を作る (定時 `Logizard-NyukaCSV` と同じユーザー・「ログオンしているときだけ」= 共有ドライブ G: が見える形):
   ```
   schtasks /Create /TN "Logizard-BcImport" /TR "C:\tools\logizard-automation\run-bc-import-poll.bat" /SC MINUTE /MO 1 /RU bfaith /IT /F
   ```
   作った後にタスクのプロパティで「既に実行中の場合 = 新しいインスタンスを開始しない」を確かめる。
7. Stream Deck の道は任意 (使うなら、その PC で `deploy.mjs --pc streamdeck --apply` = barcode-steps.js も写る)。

### 初めての本物の試し (1 件だけ)

1. 01:30〜23:45 の間・ロジザードの毎日の商品マスタの取込の状態が idle / verified のときに、新商品を **1 件だけ** 登録して「1 件の CSV を作る」。
2. その CSV の「ロジザードに取り込む」→ **② のチェックを外して** (バーコードマスタ.csv に触らない) → 取り込む。
3. 画面の「③」で 待っています → 準備中 → 取り込んでいます → **取り込みました (総件数 1 · エラー件数 0)** を見る。ログに「ポータルに「押す」を知らせた」→「取込完了」。
4. ロジザードの商品マスタとインポート履歴で、その 1 件が入ったこと (商品ID・バーコード) を確かめる。
5. 次の回から ② をつけたまま使う (② の結果 = 「書き出した (N 行)」・バーコードマスタ.csv の更新時刻)。
   うまくいかない = 画面のログ・miniPC の `logs\bc-import.log`・`error-shots\` を Claude に渡す。分からないで終わった回は、インポート履歴を見て画面で記す。

## 試験

`node scripts/test-logizard-automation.mjs` (書き出しの検証・auto-shohin-csv.js の切り出し・写し方・入荷バーコード連携の決まり) / `node scripts/test-logizard-import-state.mjs` (取込の状態) /
`node scripts/test-lz-bc-import.mjs` (ロジザードに取り込む: 帳面・口・miniPC の 1 回・偽物のロジザードの画面で ①・画面)
