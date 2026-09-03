# 🖨 送り状 印刷エージェント (出荷PC)

梱包iPadで「🖨 伝票再印刷」を押したら、**人手ゼロで出荷PCのサーマルプリンターから送り状が出る**ようにするための常駐プログラムです。

正本 = `g:\共有ドライブ\AI_reference\システム設計\送り状自動印刷_要件定義_20260827.md`

## なぜこの作りなのか

**① 出荷PCから聞きに行く（pull）**

miniPC から出荷PCへは一切つなぎません。出荷PCが数秒ごとに「刷るものある?」と聞きに行くだけです。
そのため出荷PCの固定IPも、受信ポートを開けることも要りません。

**② 同じ送り状を二度出さない**

これが一番大事です。ネットワークが切れたりPCが落ちたりしたときに、**同じ人の送り状が2枚出る**ほうが、1枚出ないより困ります（2枚目が別の箱に貼られると誤発送になる）。

そこでサーバ側とエージェント側の両方で止めます。

- サーバ側 … PDFを渡した時点で「もう他のPCには配らない」
- エージェント側 … 台帳（`work\ledger\<ジョブID>.json`）に進み具合を書く

```
leased  →  downloaded  →  submitting  →  submitted  →  done
（受けた）  （PDFを保存）  （これから刷る） （スプーラーに入った）
```

`downloaded` より先に進んだジョブは、**再起動しても二度とダウンロードも印刷もしません。**

`submitting` の途中で落ちた場合は「紙が出たかどうか分からない」ので、勝手に刷り直さず**「結果不明」としてサーバへ報告**します。チャットには「❓ 印刷結果が不明です — 実物を確認してください」が出ます。

> ⚠ ここで「印刷できませんでした（手動で印刷してください）」と伝えてはいけません。
> 実は紙が出ていた場合、現場がもう1枚刷って**2枚になる**からです。
> 「確実に出ていない」（プリンター名が無い・PDFが壊れている・印刷前に落ちた）ときだけ「手動で印刷してください」と伝えます。

**③ 「刷った」と言えるのは、実際にスプーラーから消えたときだけ**

印刷コマンドの終了コード0は「スプーラーに渡した」ことしか保証しません。そのままでは、USBが抜けていても・紙が切れていても「印刷しました」と通知され、**出ていないことに誰も気づきません。**

そこで投入したジョブをスプーラーで追いかけ、

- エラーなくキューから消えた → ✅ 印刷しました
- スプーラーがエラー（用紙切れ・オフライン等） → ❓ 結果不明（実物を確認）
- **そもそもキューに現れなかった / 追いかけられなかった → ❓ 結果不明（実物を確認）**

> 最後のケースは「速すぎて見えなかった」のか「スプーラーが受け付けなかった」のか区別できないので、
> 出たことにしません。**「結果不明」は二度刷りを誘発しないので安全側**です。
> ただし実運用でこれが毎回出るようなら観測の待ち時間を調整します（導入直後は要観察）。

**④ どのプリンターに出すかはサーバが決める**

エージェントに「このプリンターを使う」という設定はありません。ジョブに書かれたプリンター名をそのまま使います。既定のプリンターへ勝手に出すこともしません。
出荷PCの設定ミスで**別のプリンターから他人の送り状が出る**のを防ぐためです。

引当分類（AES / ネコポス / 50・60サイズ…）とプリンターの対応は、ポータルの管理画面で決めます。

## 導入手順

### 1. ポータルで出荷PCを登録する

https://picking.bfaith-wh.uk/apps/packing/admin/devices

「🖨 出荷PCの印刷エージェントを登録する」で、**このPCから出せるプリンター名を1行に1つ**入れます。

```
Munbyn ITPP941(300DPI)
ネコポス
発払
```

> プリンター名は「設定 → プリンターとスキャナー」に出ている**表記どおり**に入れてください。
> **トークンは1回しか表示されません。** コピーして次の手順で貼り付けてください。

### 2. ファイルを置く

このフォルダごと **`C:\tools\slip-print-agent\`** にコピーします。

`SumatraPDF.exe` も同じフォルダに置いてください（印刷テストキット `_tools\出荷PC_印刷テスト\` に入っています）。

### 3. 設定ファイルを作る

`config.example.json` を **`config.json`** という名前でコピーし、`token` に手順1のトークンを貼ります。

```powershell
Copy-Item C:\tools\slip-print-agent\config.example.json C:\tools\slip-print-agent\config.json
notepad C:\tools\slip-print-agent\config.json
```

> `config.json` にはトークンが入るので、**Gドライブや git には置かないでください。**

### 4. 動くか1回だけ試す

```powershell
powershell -ExecutionPolicy Bypass -File C:\tools\slip-print-agent\agent.ps1 -Once
```

`heartbeat` が通れば接続はOKです。刷るものが無ければ何も起きずに終わります。

### 5. 常駐させる

**管理者の PowerShell** で:

```powershell
powershell -ExecutionPolicy Bypass -File C:\tools\slip-print-agent\install.ps1
```

「サインインしていなくても動く」設定（SYSTEM 実行・起動時トリガー・バッテリーでも止めない）で登録されます。

### 6. スリープを切る

寝ていると印刷されません。

```powershell
powercfg /change standby-timeout-ac 0
powercfg /change standby-timeout-dc 0
```

## うまく動かないとき

まずログを見ます。

```powershell
Get-Content C:\tools\slip-print-agent\work\agent.log -Tail 30
```

| ログ | 意味 | どうする |
|---|---|---|
| `not authorised (401)` | トークンが違う / 失効している | 管理画面で登録し直してトークンを貼り直す |
| `server refused to hand out work` | サーバ側にこのPCのプリンターが登録されていない | 管理画面の「登録済み端末」で出力先を入れる |
| `SumatraPDF not found` | 実行ファイルが無い | `SumatraPDF.exe` を同じフォルダに置く |
| `print command exited with N` | 印刷コマンドが失敗 | プリンター名が実物と一致しているか確認 |
| `this PC has no printer named '...'` | 管理画面の対応表と実機の名前が違う | 「プリンターとスキャナー」の表記どおりに直す |
| `did not print cleanly` | スプーラー側で止まった（用紙切れ・オフライン等） | 実物を確認。プリンターの状態を直してから再印刷 |
| `another slip print agent is already running` | 二重起動を弾いた | 正常。常駐中に手で起動したときに出る |
| `does not match the expected checksum` | PDFが途中で壊れた | 自動で失敗報告されるので、フォルダから手動印刷 |
| `pdf download failed: HTTP 0` | **PassThru 修正前の agent.ps1** (PS5.1 の `Invoke-WebRequest -OutFile` は `-PassThru` が無いと何も返さず、実際は保存できているのに失敗扱いになる) | 最新版の agent.ps1 に差し替える |
| `Cannot bind argument to parameter 'Path' because it is an empty string` | **`$PSScriptRoot` 修正前の agent.ps1** (PS5.1 は `powershell -File` 起動時、param() の既定値評価中に `$PSScriptRoot` が空) | 最新版の agent.ps1 に差し替える |
| `Access to the path 'Global\BFaith-SlipPrintAgent' is denied` | 常駐 (SYSTEM) が動いている最中に手で `-Once` を実行した | 正常。最新版は「既に動いています」と案内して終わる。テストしたいときは先に `Stop-ScheduledTask -TaskName BFaith-SlipPrintAgent` |
| 送り状が**大きすぎて端が切れる / ラベル2枚にまたがって出る** | **`-print-settings noscale` 時代の agent.ps1**。AES の送り状は並び替えツールから **A4 (210×297mm・全面画像1枚)** で出てくるが、ラベルは 100×150mm。等倍のまま置くとはみ出す (2026-09-01 実機) | 最新版に差し替える (`shrink` = 用紙より大きいときだけ縮小)。ラベルサイズのPDFは従来どおり等倍 |

### 印刷の拡大縮小 (`printScaling`)

`config.json` の `printScaling` で SumatraPDF の `-print-settings` を変えられます (既定 `shrink`)。

| 値 | 動き | 使うとき |
|---|---|---|
| `shrink` (既定) | 用紙より**大きいページだけ縮小**。小さいページは等倍 | A4 の送り状とラベルサイズの送り状が混在する通常運用 |
| `noscale` | 常に 100% | すべての送り状がラベル実寸で届くと分かっているとき |
| `fit` | 用紙に合わせて拡大も縮小もする | 小さいPDFを引き伸ばしたいとき (バーコードが荒れるので非推奨) |

タスクの状態:

```powershell
Get-ScheduledTask -TaskName BFaith-SlipPrintAgent | Select-Object State
Get-ScheduledTaskInfo -TaskName BFaith-SlipPrintAgent
```

外すとき:

```powershell
powershell -ExecutionPolicy Bypass -File C:\tools\slip-print-agent\install.ps1 -Uninstall
```

## 🚨 実機で必ず確かめること (Codexレビュー指摘)

この環境では PowerShell からローカルのHTTPサーバに届かないため、**通しテストは出荷PCでしかできません。**
ASCII検査・構文検査・「プリンター名が分断されないか」は自動テスト済みですが、以下は実機で見てください。

| # | 確かめること | なぜ |
|---|---|---|
| 1 | 存在しないプリンター名のジョブで、**どこからも紙が出ない** | 印刷ソフトが既定プリンターへ勝手に出すと、別の送り状が混ざる。エージェントは投入前に完全一致で存在確認するが、実機で裏を取る |
| 2 | `ネコポス` `発払` `Munbyn ITPP941(300DPI)` の3つで実際に出る | 名前の一致は大文字小文字のみ無視。前後の空白や部分一致は通さない |
| 3 | **印刷の直前・直後に電源を切り、復帰後に自動で刷り直さない** | 二重印刷を防ぐ台帳が効いているか。これが一番大事 |
| 4 | 3のとき通知が「**実物を確認してください**」になる (「手動で印刷してください」ではない) | 「手動で」と言われると、既に出ていた紙と合わせて2枚になる |
| 5 | **USBを抜いた状態 / 用紙切れで刷ると「❓結果不明」になる**（「✅印刷しました」にならない） | 終了コード0だけで成功と報告すると、出ていないのに誰も気づかない |
| 6 | **サインアウトした状態**で出ること | SYSTEM 実行でプリンターが見えるか (2026-08-27 に単発では実証済み) |
| 7 | 常駐中に `-Once` を手で実行しても二重に動かない | 名前付きミューテックスで弾く |
| 8 | 等倍で出ているか (定規で実測) | 数%縮むとバーコードが読めない |
| 9 | **1枚刷って「✅印刷しました」になるか**。毎回「❓結果不明」になるなら連絡してください | 印刷キューの文書名に `slip-<ジョブID>.pdf` が載る前提で自分のジョブを特定しています。載らない環境だと毎回「結果不明」になるので、別の目印に作り替える必要があります |

## 注意

- **`agent.ps1` と `install.ps1` は ASCII だけで書いてあります。** タスクスケジューラ + PowerShell 5.1 は日本語を含む .ps1 を壊すことがあり、実際に2回ハマりました。修正するときも日本語を入れないでください（説明はこの README に書く）。
- 日本語のプリンター名（`ネコポス` / `発払`）はサーバから UTF-8 で届くので問題ありません。スクリプトに直接書かないでください。
- この定期実行は台帳 `bfaith-portal/config/jobs-registry.mjs` の `slip-print-agent` に登録済みです。生存が途切れると監視で分かります。
