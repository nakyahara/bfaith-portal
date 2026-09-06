# 🏷 保管箱ラベル 印刷係 (印刷エージェント) — いろはPC × Brother QL-800

いろは在庫化作業アプリ (iPad) の詳細画面で「🏷 箱ラベル」を押したら、**人手ゼロで いろはPC の Brother QL-800 から保管箱ラベルが出る**ようにするための常駐プログラム一式。

- 中原さん 2026-09-06「この画面から QL-800 で保管箱に貼るラベルを印字したい。『バーコード印字.lbx』のレイアウトで。入荷受付アプリでやった内容と全く同じだから参考にして。子会社の PC からは `G:\共有ドライブ\入荷バーコード発行\` しか見えないのでここにテストなどを置く」
- 元になった実績 = 倉庫PCの値札 (BCシール) 印刷エージェント (`AI_reference\システム設計\_tools\倉庫PC_値札印刷エージェント\`、2026-09-05 本番稼働)。**「同じものを二度刷らない」の設計と印刷経路 (b-PAC で描画 → GDI で印刷) はそのまま**、テンプレ・出力先・サーバー側の口だけ違う
- サーバー側 (ポータルの印刷キュー) = bfaith-portal `apps/iroha-work/print-queue.js` + `print-worker.js` + router `/print/*` `/api/print/*` (PR 2026-09-06)
- 中原さん向けの手順 (専門用語なし) = 同じフォルダの **`やること.md`**

## 🚨 このフォルダは 2 か所にある (正本はリポジトリ側)

| 場所 | 役割 |
|---|---|
| `bfaith-portal/scripts/iroha-label-agent/` | **正本**。ここを直して PR でレビューする (出荷PCの `scripts/slip-print-agent/` と同じ扱い) |
| `G:\共有ドライブ\入荷バーコード発行\箱ラベル印刷係\` | いろはPC 用の配布コピー。**いろはPC からはこのフォルダしか見えない** (中原さん 2026-09-06)。ここから `1_設置する.bat` を実行する |

サーバー側 (`apps/iroha-work/print-queue.js` の `leaseNextJob` が返す JSON) を変えたら、**両方**を更新して、
いろはPC で `1_設置する.bat` を実行し直すこと。

- `templates\*.lbx` は `make-auto-lbx.ps1` が元テンプレ (共有ドライブの `バーコード印字.lbx`) から作る生成物なので、リポジトリには置かない
- `config.json` (トークン入り) も置かない。リポジトリにあるのは `config.example.json` だけ

## 🚨 このフォルダは 2 か所にある (正本はリポジトリ側)

| 場所 | 役割 |
|---|---|
| `bfaith-portal/scripts/iroha-label-agent/` | **正本**。ここを直して PR でレビューする (出荷PCの `scripts/slip-print-agent/` と同じ扱い) |
| `G:\共有ドライブ\入荷バーコード発行\箱ラベル印刷係\` | いろはPC 用の配布コピー。**いろはPC からはこのフォルダしか見えない** (中原さん 2026-09-06)。ここから `1_設置する.bat` を実行する |

サーバー側 (`apps/iroha-work/print-queue.js` の `leaseNextJob` が返す JSON) を変えたら、**両方**を更新して、
いろはPC で `1_設置する.bat` を実行し直すこと。

- `templates\*.lbx` は `make-auto-lbx.ps1` が元テンプレ (共有ドライブの `バーコード印字.lbx`) から作る生成物なので、リポジトリには置かない
- `config.json` (トークン入り) も置かない。リポジトリにあるのは `config.example.json` だけ

## 全体像

```
iPad いろは在庫化作業アプリ 詳細「🏷 箱ラベル」(1 箱に何個・期限・枚数・出力先を確かめて発行)
   ↓ ポータル (Render) が印刷キュー f_iroha_print_jobs に積む (商品名・商品コード・バーコードはカードの行から)
   ↑ ①「刷るものある?」4秒ごと HTTPS (pull)      ↓ ② ジョブ JSON をそのまま渡す
いろはPC 印刷係 (タスクスケジューラ・SYSTEM・サインイン不要)  C:\tools\iroha-label-agent\
   └ b-PAC で templates\hakolabel_auto_JAN.lbx (または _FNSKU.lbx) を開き、用紙「箱ラベル 62x67」を選び、
     商品名 (テキスト6) / 数量=1 箱に何個 (テキスト13) / 期限 (テキスト10) / バーコード (バーコード2, CODE128) に値を入れて 300dpi の BMP に描画 (Export)
   └ その BMP を Windows 標準の印刷 (GDI) で QL-800 へ (用紙 = ドライバー番号 259「62mm」長尺 × 67.2mm・横)
   └ ③ スプーラーを追いかけて「刷れた / 分からない」を報告 + 45秒ごと heartbeat
```

### なぜこの作りか (値札・送り状の印刷係と同じ)

1. **pull 型** — サーバーから いろはPC へは一切つながない。固定IP・受信ポート不要
2. **同じラベルを二度出さない** — 台帳 `work\ledger\<ジョブID>.json` に段階を書く `leased → received → submitting → submitted → done`。`received` より先に進んだジョブは再起動しても二度と刷らない。`submitting` の途中で落ちたら「紙が出たか分からない」ので **❓結果不明** で報告 (「もう1回」とは言わない — 二重に貼られる)
3. **「刷った」と言えるのはスプーラーから消えたときだけ** — 投入したジョブを **文書名 `hakolabel-<ジョブID>`** で特定して追いかける
4. **プリンターはサーバーが決める** — ジョブの `printerName` をそのまま使う。既定プリンターへのフォールバック無し

### 箱ラベル固有のところ (値札との違い)

- **テンプレ = `G:\共有ドライブ\入荷バーコード発行\バーコード印字.lbx`** (現場が P-touch Editor で手で使っているもの。**触らない**)。62mm 長尺 × 67.2mm・横 (`175.7pt × 190.6pt`、値札と同じ寸法)。オブジェクト:

  | オブジェクト名 | 中身 | ジョブの項目 |
  |---|---|---|
  | `テキスト6` | 商品名 (メイリオ 17.3pt・自動縮小) | `productName` |
  | `バーコード2` | CODE128 (人間可読あり) | `barcode` (`barcodeType` jan / fnsku どちらもここ) |
  | `テキスト13` | 「数量」の右の数字 = **1 箱に何個** | `packQty` (空なら空欄) |
  | `テキスト10` | 数量の右の枠 = **期限** | `expiry` (空なら空欄) |
  | `テキスト15` | 固定文字「数量」 | (触らない) |
- **元テンプレは `バーコードマスタ.csv` に DB リンク**されている (商品ID・商品名・検索名称・バーコード・有効期限区分 01/02)。b-PAC でそのまま開くと CSV を読みに行って値が上書きされる恐れがあるため、`make-auto-lbx.ps1` が **DB リンクと差し込み属性を外したコピー**を `templates\` に作る
- **バーコードは 1 本** (CODE128 は数字も英数字も通る) → JAN 用 / FNSKU 用のコピーは**同じ中身**。2 本あるのはエージェントの契約 (`barcodeType` でテンプレを選ぶ) を値札と揃えるため
- **`expiry` が増えた** — `config.json` の `objects.expiry` にオブジェクト名があるときだけ書く (無ければ触らない = 値札の config でもそのまま動く)
- 用紙名は **`箱ラベル 62x67`** (QL-800 ドライバーに 1 回登録。「用紙」節)
- 通知先 = いろはアプリの職員向け GChat (`GCHAT_WEBHOOK_IROHA`。資材不足の連絡と同じ)

## ファイル

| ファイル | 役割 | 文字 |
|---|---|---|
| `agent.ps1` | 常駐本体 (ポーリング・台帳・スプーラー追跡・報告)。ミューテックス `BFaith-IrohaLabelAgent`・文書名 `hakolabel-<id>` | ASCII のみ |
| `print-label.ps1` | 印刷本体 (ライブラリ兼・手動テスト CLI): b-PAC で描画→BMP + GDI で印刷。`-Expiry` あり | ASCII のみ |
| `make-auto-lbx.ps1` + `templates.json` | 元テンプレ → DB リンク無しのコピー 2 本を `templates\` に生成 (`barcodeObjects` / `outNames` で名前を指定) | ASCII / JSON は日本語可 |
| `templates\hakolabel_auto_JAN.lbx` `hakolabel_auto_FNSKU.lbx` | 生成物 (2026-09-06 生成・元 = 8/27 10:11 版の `バーコード印字.lbx`) | — |
| `config.example.json` | 設定の雛形 (**オブジェクト名・用紙名はここ** = .ps1 を ASCII に保つため)。`1_設置する.bat` がトークンを入れて `C:\tools\iroha-label-agent\config.json` にする | JSON |
| `install.ps1` | タスクスケジューラ登録 `BFaith-IrohaLabelAgent` (SYSTEM・起動時・10分ごと再起動・バッテリーで止めない)。b-PAC が見える powershell.exe (64/32bit) を自動選択 | ASCII |
| `setup.ps1` / `1_設置する.bat` | 一発設置 (b-PAC 確認 → `C:\tools\iroha-label-agent` へコピー → トークン → 1回ポーリング → タスク登録 → スリープ無効)。結果 `RESULT_setup.txt` | ASCII |
| `status.ps1` / `2_状態を見る.bat` | 状態出力 `RESULT_status.txt` (タスク・b-PAC 64/32・QL-800 キュー・電源・ログ・台帳) | ASCII |
| `9_外す.bat` | タスク解除 (`C:\tools\iroha-label-agent\install.ps1 -Uninstall`) | — |
| `test-print.ps1` + `messages.json` / `3_テストする.bat` | **サーバー不要の実機テスト** (ダブルクリック)。b-PAC 確認 → 用紙確認 → `バーコードマスタ.csv` の 1 行でプレビュー BMP → y で 1 枚 → y で 3 枚 (カット確認)。結果 `RESULT_test.txt` | ASCII / 文言は JSON |
| `やること.md` | 中原さん向けの手順 (① b-PAC 導入 ② テスト ③ 設置)。専門用語なし | 日本語 |

🚨 **`.ps1` は ASCII だけ**。タスクスケジューラ + PowerShell 5.1 が日本語入り .ps1 を壊す (出荷PCで 2 回ハマった)。日本語は **config.json / templates.json / messages.json / HTTP の JSON** で渡す。

🚨 **G ドライブは SYSTEM タスクから見えない** (ユーザーマウント)。実行に必要なものは全部 `C:\tools\iroha-label-agent\` に置く (setup.ps1 がコピーする)。`config.json` (トークン入り) は C: にだけ置き、G には置かない。

## 用紙 — b-PAC の癖 (値札で 2026-09-05 に実測。ここが一番の罠)

1. **b-PAC は `.lbx` に書いてある用紙サイズ (62mm × 67.2mm) を無視する。** 文書の用紙はプリンターの「現在の既定用紙」になる (既定が「62mm x 29mm」だと 29mm に切れる)
2. `IDocument.Length` の書き込みは無視される。**効くのは `SetMediaByName`** = ドライバーの用紙一覧から選ぶ
3. 62 × 67.2 の用紙はドライバーに無い → **QL-800 の 印刷設定 → [拡張設定] → 「長尺テープフォーマット(N)」[設定...] → [新規] で 名前 `箱ラベル 62x67` / 幅 62mm (長尺) / 長さ 67.2mm を 1 回登録**する
4. 🚨🚨 **QL-800 実測 (2026-09-06 いろはPC): 登録した長尺テープフォーマットは b-PAC から一切使えない。** `GetSupportedMediaNames()` に出てこないだけでなく、**`SetMediaByName('箱ラベル 62x67')` が ErrorCode 17367041 で失敗する**。b-PAC が受け付けるのはドライバー標準の名前だけ (実測の全一覧: 17mm x 54mm / 17mm x 87mm / 23mm x 23mm / 29mm x 42mm / 29mm x 90mm / 38mm x 90mm / 39mm x 48mm / 52mm x 29mm / 54mm x 29mm / 60mm x 86mm / 62mm x 29mm / 62mm x 60mm / 62mm x 75mm / 62mm x 100mm / 12mm Dia / 24mm Dia / 58mm Dia / **12mm / 29mm / 38mm / 50mm / 54mm / 62mm** (= 長尺) / 12mm x 2 … 62mm x 4)。QL-700 では登録した名前が使えた
4b. 🚨 **連続 (長尺) 用紙を選ぶと `IDocument.Length` は 0 を返す** (2026-09-06 実測)。長尺ロールに固定長は無く、b-PAC は中身に合わせて自動で長さを決める。`Length` のセッターも一応試すが、無視されたら **auto length として受け入れる**。書き出す BMP は 67.2mm より長いことも短いこともある。BMP の検証は max/min で長短を決めず、**テープ幅に一致する辺**を探して、もう一方を長さとして扱う (自動長は 62mm より短くなりうるため)
5. → **キャンバスの長さは合わせない。合わせるのは印刷の紙。** `config.label.fallbackMediaName` = `62mm` (ドライバー標準の長尺・既定 89.8mm) に落として描画し、**印刷 (`Invoke-GdiPrint`) は必ず `lengthMm` = 67.2mm の紙に出す**ので、うしろの余白は切り落とされる。テンプレの中身は先頭 60mm に収まっているので欠けない
6. → 判定は **幅 = `widthMm` ± `toleranceMm` で一致必須** (テープ幅は物理)、**長さ = `lengthMm` 以上ならよい** (短いとバーコードが切れるので致命)。一覧も `SetMediaByName` の戻り値も判定に使わない (前者は欠ける・後者は無い名前でも TRUE)。`3_テストする.bat` は `[2/5]` で △ を出して進み、`[5/5]` の実寸で判定して「長い分は切り落とされる」と説明する。heartbeat の `paperFormatOk` も `Test-LabelMediaUsable` (テンプレを開いて用紙を選び実寸を測るだけ・印刷なし・10 分キャッシュ) で答え、動かなければ `null` (不明) を送る。**iPad に嘘の「用紙未登録」を出し続けないため**
6. **b-PAC 自身の `PrintOut` は使わない** (QL-700 で ErrorCode 11。再調査に時間を使わない)。b-PAC は Export (300dpi BMP) だけ、印刷は .NET `System.Drawing.Printing` (GDI)。用紙 = `PaperSize.RawKind 259` (62mm 長尺)・67.2mm・横、`(-HardMarginX, -HardMarginY)` に等倍で描く。BMP は `work\render\hakolabel-<id>.bmp` に 14 日残す (証拠)

QL-800 は QL-700 と同じドライバーの仲間なので同じ手順のはずだが、**実機で初めて使う** → 「実機で必ず確かめること」を見る。

## 導入手順 (中原さん) — 詳細は `やること.md`

0. b-PAC Client Component (64bit) を入れる (無料・要ユーザー登録)
1. QL-800 に用紙「箱ラベル 62x67」を登録 → `3_テストする.bat` (サーバー不要) でプレビュー → 1 枚 → 3 枚
2. ポータル `https://bfaith-portal.onrender.com/apps/iroha-work/admin` の「🏷 保管箱ラベル」節で 端末名 `いろはPC` / プリンター名 `Brother QL-800` → トークン発行 (1 回だけ表示)
3. `1_設置する.bat` を右クリック → 管理者として実行 → トークンを貼る → `----- done -----`
4. iPad の詳細画面「🏷 箱ラベル」→ QL-800 から出る → iPad に ✅

手で細かく試すとき (紙を使わずプレビュー):
```powershell
cd "G:\共有ドライブ\入荷バーコード発行\箱ラベル印刷係"
powershell -ExecutionPolicy Bypass -File print-label.ps1 -BarcodeType jan -Barcode 4573473360422 -ProductName "テスト商品" -PackQty 24 -Expiry "2027-03" -ExportBmp "$env:TEMP\hakolabel_preview.bmp"
```
⚠ `config.example.json` の templates は `C:\tools\...` を指しているので、設置前は `-ConfigPath` で templates を G 上の `templates\` に向けた config を渡す (`3_テストする.bat` はこれを自動でやる)。

## サーバー側の契約 (bfaith-portal `apps/iroha-work`)

ベース URL = `https://bfaith-portal.onrender.com/apps/iroha-work`、認証 = `Authorization: Bearer <端末トークン>` (ハッシュ保存・失効可・400日・`kind='agent'` の端末だけ。iPad の端末Cookieでは通らない)。

| メソッド | パス | 内容 |
|---|---|---|
| GET | `/print/next` | queued を原子的に lease して 200 `{job}` / 無ければ **204** / 401 トークン不正 / **409** この端末にプリンター未登録 |
| POST | `/print/:id/submitted` | `{lease, spool_job_id}` スプーラー投入報告。同じ報告の再送は 200 (`replayed:true`) |
| POST | `/print/:id/completed` | `{lease, ok:true}` または `{lease, ok:false, error, uncertain?:true}`。投入後の失敗は `uncertain` に関係なく state=`unknown` (通知は「実物を確認」) |
| GET | `/print/:id/status` | `{job:{state}}` (起動時の台帳突合に使う。自分が lease した分だけ) |
| POST | `/print/heartbeat` | `{note, version, bpac:bool, host, paperFormat, paperFormatOk, printerReports}` 45秒ごと |

`GET /print/next` が返すジョブ (値札 + `expiry` / `taskId`):

```json
{ "job": {
  "id": 12, "leaseToken": "…", "printerName": "Brother QL-800",
  "productCode": "0726-001970", "productName": "【木工用 60g】 みつろうクリーム 木工用 60g _梱機プ",
  "barcode": "X000T1GS6F", "barcodeType": "fnsku",
  "packQty": "120", "extraPackQty": "80", "expiry": "2027-03", "copies": 2,
  "taskId": 345, "requestedBy": "たなか"
} }
```

- `barcodeType` = 数字だけなら `jan`、英字を含めば `fnsku`。空・英数字以外は積まない (iPad にボタンも出ない)
- `packQty` = iPad で確かめた「1 箱に何個」(既定 = 作業のやり方の入数)。`expiry` = 「期限」の文字 (既定 = この入荷の有効期限、無ければ期限シールありの商品だけ印。40 字まで)。どちらも空 = 空欄で刷る
- `copies` = 1..50 (既定 = 満杯の箱の数)。エージェントは範囲外を失敗で返す
- ⭐`extraPackQty` = **端数の箱の入数** (空 = 端数なし)。あるときは `copies` 枚のあとに **もう 1 枚だけ**この数量で刷る。
  🚨 2 回刷るときの安全側 (2026-09-06 Codex R2):
  ① **1 回目を渡した後の失敗は必ず uncertain** (紙が出たかもしれないので「もう一度」とは言わない)
  ② **1 回ごとにスプールを追う** (最後にまとめて見ると、速く消えた 1 回目を見落として 2 回目の成功で全体を printed にしてしまう)
  ③ **両方が綺麗にキューから消えたときだけ printed**。片方でも怪しければその結果を残す
  ④ `Test-JobData` も `copies + (端数あれば 1) <= maxCopies` で見る (古いジョブが 51 枚を通り抜けないように)
  ⑤ 追跡が 2 回になるぶん最悪の待ちが伸びる (5+120 秒 × 2)。サーバーの報告受付は 300 秒なので、
     超えたときは報告が弾かれて `unknown` (= 実物を確認) になる。**安全な側に倒れる**ので、これは仕様として許容する
  必要保管箱 6 箱 = 70×5＋10 なら「70 個のラベル 5 枚 ＋ 10 個のラベル 1 枚」(中原さん 2026-09-06)。
  2 回目の印刷も**同じ文書名** `hakolabel-<id>` なので、スプールの追跡は 1 ジョブぶんとして両方を追う。
  証拠の BMP だけ `hakolabel-<id>-r.bmp` と別名にする。空なら今までと 1 バイトも変わらない動き
- 状態: `queued → leased → submitted → completed` / `failed` (刷る前の失敗。もう一度押してよい) / `unknown` (報告なし・投入後の失敗。**実物を確認**。再発行は iPad で「実物を確認した」の証跡が要る) / `manual` (3 分たっても誰も取りに来ない = いろはPC が寝ている。自動印刷は取り消し、手で刷る)
- 見張り = `print-worker.js` (Render 内 30 秒間隔・`IROHA_WORK_PRINT_ENABLED`)。滞留→manual / 報告なし→unknown / 結果を `GCHAT_WEBHOOK_IROHA` へ / heartbeat が 10 分以内なら jobs-monitor の台帳 `iroha-label-print-agent` へ ok を中継 (いろはPC に JOBS_MONITOR_TOKEN を配らない)

## 🚨 実機で必ず確かめること (QL-800 は初めて)

| # | 確かめること | なぜ |
|---|---|---|
| 1 | b-PAC が SYSTEM アカウントで動く (タスクから 1 枚出る) | 値札 (QL-700) では OK。プリンターが違う |
| 2 | 印刷キューの文書名に `hakolabel-<ジョブID>` が載る | エージェントは同名ジョブを追いかける |
| 3 | 3 枚を 1 ジョブで刷って 1 枚ずつカットされる (カットはドライバー既定「指定枚数ごとにカット 1 枚」) | SYSTEM 常駐ではプリンター既定 DEVMODE を見る |
| 4 | プレビュー BMP と手作業のラベルが同じ見た目 (メイリオ・長い商品名の自動縮小・数量と期限の位置) | 期限 (テキスト10) は手作業では空欄のことが多い → 文字が枠に収まるか |
| 5 | 用紙「箱ラベル 62x67」登録後、`[2/5] 用紙 OK` → プレビューが 67.2 × 62mm | b-PAC の用紙の癖 |
| 6 | 存在しないプリンター名のジョブでどこからも紙が出ない | 投入前に完全一致で存在確認 |
| 7 | 印刷の直前・直後に電源を切り、復帰後に自動で刷り直さない + iPad が「❓ 実物を確認」 | 台帳が効いているか。一番大事 |
| 8 | USB を抜いた状態 / テープ切れで「❓結果不明」になる (「✅」にならない) | 戻り値 true だけで成功にしない |

## うまく動かないとき

```powershell
Get-Content C:\tools\iroha-label-agent\work\agent.log -Tail 30 -Encoding UTF8
```

| ログ | 意味 | どうする |
|---|---|---|
| `b-PAC not available … Class not registered` / `jobs will NOT be taken until it is installed` | b-PAC 未導入、または 32bit 版しか無い | Client Component 64bit を入れる |
| `heartbeat got HTTP 404` | ポータルに印刷キューがまだ無い (サーバー側 PR 未デプロイ) | サーバー側を先に |
| `not authorised (401)` | トークンが違う / 失効 | 管理画面で再発行して `1_設置する.bat` |
| `server refused to hand out work` | サーバー側にこの PC のプリンターが未登録 | 管理画面で `Brother QL-800` を登録 |
| `this PC has no printer named '…'` | 管理画面の名前と実機の名前が違う | 「プリンターとスキャナー」の表記どおりに |
| `template file is missing on this PC` | `C:\tools\iroha-label-agent\templates\` にコピーが無い | `make-auto-lbx.ps1` → `1_設置する.bat` |
| `b-PAC failed: [filling] template has no object named` | テンプレのオブジェクト名が config と違う (テンプレを作り直した等) | 元テンプレでオブジェクト名 (`テキスト6` `テキスト13` `テキスト10` `バーコード2`) を確認 → `config.example.json` / `templates.json` を直す → 作り直す |
| `[2/5] 用紙 △ 「箱ラベル 62x67」はプリンターの用紙一覧に出てきませんでした` | **QL-800 では登録済みでも出ない (正常)** | そのまま進んでよい。[5/5] の実寸が合えば OK |
| `b-PAC failed: [checking media] label width is …mm but the design needs 62mm` | 62mm 幅のロールが入っていない / ダイカットのロール | 62mm 連続長尺ロール (DK-22205 互換) を入れる |
| `b-PAC failed: [checking media] label length is …mm but the design needs at least 67.2mm` | **固定長**の用紙が選ばれていて 67.2mm より短い (ダイカット等) | 62mm 連続 (長尺) ロールを入れる。または 印刷設定 [基本設定] の用紙サイズを `箱ラベル 62x67` にする |
| `[checking media] label length is 0mm` | 長尺用紙の Length = 0 を「短い」と誤判定していた (2026-09-06 の旧版) | auto length を受け入れる形に修正済み。キットを配置し直す |
| `rendered image is … - neither side is the 62mm tape width` | 62mm 幅のロールが入っていない | プリンターのロールを見る |
| `b-PAC failed: [checking media] label length is 89.8mm but the design needs 67.2mm` | 用紙は選べたが長さが違う | 用紙サイズ設定で「箱ラベル 62x67」を編集 → 長さ 67.2 |
| `b-PAC failed: [printing] …` (uncertain=True) | 印刷中に失敗。紙が出たか分からない | **実物を確認**。USB・電源・テープ |
| `the print queue could not be read before printing` | スプーラーが読めない (値札 v.4 のバグは v.5 で修正済み。この kit は v.5 ベース) | `agent.log` の `Get-PrintJob failed (...)` の行を Claude に |
| `another iroha label agent is already running` | 二重起動を弾いた | 正常 |

タスク: `Get-ScheduledTask -TaskName BFaith-IrohaLabelAgent | Select State` / 外す: `9_外す.bat`

## 元テンプレを変えたとき

`バーコード印字.lbx` (現場用) のレイアウトを変えたら、このフォルダで

```powershell
powershell -ExecutionPolicy Bypass -File make-auto-lbx.ps1     # templates\ を作り直す
```

→ `1_設置する.bat` を管理者実行 (C:\tools へコピーし直す)。オブジェクト名を変えた場合は `config.example.json` の `objects` と `templates.json` の `textObjects` / `barcodeObjects` も直す。
