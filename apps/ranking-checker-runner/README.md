# 楽天順位チェッカー Runner (miniPC版)

Phase 2 (2026-04-21〜) で Render から切り離した順位チェック実行部。miniPC 上で
Windows Task Scheduler + PowerShell ラッパー経由で日次起動する。

- コード: `apps/ranking-checker-runner/runner.js`
- ラッパー: `scripts/run-rankcheck-safe.ps1`
- DB: `C:\tools\rankcheck-runner\data\ranking-checker.db`
- ログ: `C:\tools\rankcheck-runner\logs\runner-YYYY-MM-DD.log`

## アーキテクチャ (案B: DB もminiPC寄せ)

```
Render (UI専用)                         miniPC (C:\tools\rankcheck-runner\)
┌────────────────────┐               ┌──────────────────────────────┐
│ /apps/ranking-checker/              │                                │
│   router.js (proxy)  ─────────────▶ │ WarehouseServer (winsw)        │
│   csv-export.js ─────────┐          │   /service-api/rankcheck/* ──┐ │
│   scheduler.js (CSVのみ) │          │                              │ │
└────────────────────┘    │          │                              ▼ │
                          │          │                         ranking-checker.db
                          │          │                              ▲ │
                          ▼          │                              │ │
                   GET /data         │ Task Scheduler (13:00 JST)   │ │
                   POST /data        │   └─ run-rankcheck-safe.ps1  │ │
                   POST /data/import │       └─ runner.js ──────────┘ │
                   POST /run-check   │           (直接DB書き込み)       │
                   ...               │                                │
                                     └──────────────────────────────┘
```

- **Runner** (CLI): Task Scheduler 起動、ranking-checker.db に直接書き込み、終了
- **WarehouseServer** (常駐): `/service-api/rankcheck/*` を Render UI に公開
- **Render**: UI + CSV cron のみ。DB無し。`/data` は miniPC に proxy

## miniPC 初回セットアップ

### 1. ディレクトリ作成

```powershell
New-Item -ItemType Directory -Path 'C:\tools\rankcheck-runner\data' -Force
New-Item -ItemType Directory -Path 'C:\tools\rankcheck-runner\logs' -Force
```

### 2. リポジトリ clone (既に warehouse 用があるならそれを流用可)

```powershell
cd 'C:\tools\rankcheck-runner'
git clone https://github.com/nakyahara/bfaith-portal.git
cd bfaith-portal
npm install --omit=dev
```

### 3. .env 作成

`C:\tools\rankcheck-runner\.env` に以下を書く（値は本番値に）：

```
RAKUTEN_APP_ID=...
RAKUTEN_ACCESS_KEY=...
RAKUTEN_SHOP_CODE=b-faith
AMAZON_ACCESS_KEY=...
AMAZON_SECRET_KEY=...
AMAZON_ASSOCIATE_TAG=...
# Yahoo は現在無効化中 (サーバーIPアクセス不可)

# GChat通知 (失敗時、任意)
GCHAT_WEBHOOK_URL=...

# RANKCHECK_AUTO_ENABLED は PS1 が強制的に false にするので不要
```

**重要**: `.env` は git 管理外。手作業で配置し、NTFS 権限で読み取り制限。

### 4. Render から ranking-checker.json を取得して migrate

```powershell
cd 'C:\tools\rankcheck-runner\bfaith-portal'
# Render Persistent Disk から ranking-checker.json を取り出し、data-migrate/ に置く
$env:DATA_DIR = 'C:\tools\rankcheck-runner\data'
node scripts/migrate-json-to-sqlite.mjs 'C:\tools\rankcheck-runner\data-migrate\ranking-checker.json'
# 完了後、C:\tools\rankcheck-runner\data\ranking-checker.db が生成される
```

### 5. Task Scheduler 登録

```powershell
$action  = New-ScheduledTaskAction -Execute 'powershell.exe' `
  -Argument '-NoProfile -ExecutionPolicy Bypass -File "C:\tools\rankcheck-runner\run-rankcheck-safe.ps1"' `
  -WorkingDirectory 'C:\tools\rankcheck-runner'
$trigger = New-ScheduledTaskTrigger -Daily -At 1:00pm   # 13:00 JST
$settings = New-ScheduledTaskSettingsSet `
  -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
  -StartWhenAvailable -MultipleInstances IgnoreNew `
  -ExecutionTimeLimit (New-TimeSpan -Hours 3)
$principal = New-ScheduledTaskPrincipal -UserId 'bfaith' -LogonType Password -RunLevel Highest
Register-ScheduledTask -TaskName 'RankCheckRunner' -Action $action -Trigger $trigger -Settings $settings -Principal $principal
```

※ `-LogonType Password` のためタスク作成時にパスワードを対話で要求される。winsw と同様。

### 6. 手動テスト

```powershell
# 本日既にチェック済みならスキップされる (通常動作)
powershell -NoProfile -ExecutionPolicy Bypass -File 'C:\tools\rankcheck-runner\run-rankcheck-safe.ps1'

# 強制再実行 (UI「順位チェック開始」ボタン相当)
powershell -NoProfile -ExecutionPolicy Bypass -File 'C:\tools\rankcheck-runner\run-rankcheck-safe.ps1' -Force
```

### 7. WarehouseServer 側 service-api の有効化

`C:\tools\watchdog\` 配下の WarehouseServer が既に動いているはず。
最新コードを pull して再起動すると `/service-api/rankcheck/*` が自動で有効化される:

```powershell
cd 'C:\Users\bfaith\bfaith-portal'   # WarehouseServer の clone 先
git pull
npm install --omit=dev
Restart-Service WarehouseServer
```

#### **【重要】WarehouseServer の env に DB パスを追加**

Runner は `C:\tools\rankcheck-runner\data\ranking-checker.db` に書き込むが、
WarehouseServer も **同じファイル** を開かないと `/service-api/rankcheck/*` と
Runner が別 DB を見てしまい (split-brain)、UI に反映されない。

WarehouseServer の `.env` (`winsw` 起動時のサービス環境変数) に必ず追加:

```
RANKCHECK_DB_FILE=C:\tools\rankcheck-runner\data\ranking-checker.db
```

設定後、`Restart-Service WarehouseServer` を実行して環境変数を反映する。

動作確認:

```powershell
# WarehouseServer のプロセスから DB が正しく参照できているか
curl -H "Authorization: Bearer $env:WAREHOUSE_SERVICE_TOKEN" `
     -H "CF-Access-Client-Id: $env:CF_ACCESS_CLIENT_ID" `
     -H "CF-Access-Client-Secret: $env:CF_ACCESS_CLIENT_SECRET" `
     https://wh.bfaith-wh.uk/service-api/rankcheck/master
# → { ok: true, count: N, products: [...] } が返れば OK
```

### 8. Render 側 env 設定

Render 管理画面で以下の env を設定:

```
RANKCHECK_MINIPC_URL=https://wh.bfaith-wh.uk
WAREHOUSE_SERVICE_TOKEN=<既存と同じ>
CF_ACCESS_CLIENT_ID=<既存と同じ>
CF_ACCESS_CLIENT_SECRET=<既存と同じ>
RANKCHECK_AUTO_ENABLED=false   # 明示的に無効化（miniPC Runner が担当）
```

Render 再デプロイで proxy モードが有効化される。

## 運用

### 失敗検知
- PS1 ラッパーが exit code != 0 を GChat webhook に通知
- `C:\tools\rankcheck-runner\logs\runner-YYYY-MM-DD.log` に全実行が日次集約

### DB 直接確認

```powershell
sqlite3 'C:\tools\rankcheck-runner\data\ranking-checker.db' "SELECT status, done, total, started_at FROM run_state ORDER BY started_at DESC LIMIT 5;"
```

### 手動運用

- 実行中断: PS1 起動時に生成される `C:\tools\rankcheck-runner\runner.lock` を削除すれば次回起動可
- ログ確認: `C:\tools\rankcheck-runner\logs\runner-$(Get-Date -f 'yyyy-MM-dd').log`
- Render側ログ確認: `GET /apps/ranking-checker/logs` (proxy経由で miniPC の同名ログを返す)

## トラブルシュート

| 症状 | 原因候補 | 対処 |
|---|---|---|
| PS1 が exit 73 | 既に Runner 実行中 (lockfile) | Get-Process で確認、不要なら lockfile 削除 |
| Runner 起動するが即 exit 1 | .env 不足、API キー不正 | `C:\tools\rankcheck-runner\.env` 確認 |
| run_state が stale 'running' のまま | 前回クラッシュ | 次回 Runner 起動時に markStaleRunning で failed 遷移 |
| Render UI で「minipc_unreachable」 | CF Tunnel 切断 / WarehouseServer 停止 | `Get-Service WarehouseServer` / tunnel ログ確認 |
| schema fingerprint mismatch | コード版と DB 版の不一致 | `git pull` して `npm install` 後 `Restart-Service WarehouseServer` |

## 参考

- Phase 1 (SQLite化) commit: `6d8526a`
- feedback_rankcheck_runner.md: Task Scheduler + PS1 を選ぶ理由
- feedback_windows_service.md: Windows 常駐サービスは winsw の方針（Runner は常駐ではないので PS1）
- WarehouseWatchdog: 別基盤 (`C:\tools\watchdog\`) で warehouse server.js の生存監視を実施
