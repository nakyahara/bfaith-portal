# logizard-stock — ロジザード在庫スナップショットの毎時パイプライン (miniPC)

ロジザード (クラウドWMS) の在庫CSV (SKU×ロケ×在庫数×引当数、約8,400行) を
**9:00〜18:00 の毎時** ダウンロードし、miniPC と Render の両方で使える状態にする。

```
Logizard (毎時 9-18時)
  ↓ ① auto-zaiko.js (C:\tools\logizard-automation、Edgeヘッドレス、多段検証つきDL)
C:\tools\logizard-automation\out\logizard_zaikosu.csv
  ↓ ② apps/warehouse/csv-import.js logizard  → warehouse.db raw_lz_inventory (全置換・単一トランザクション)
  ├→ 欠品LINE通知: PickingServer が service-api (/service-api/logizard-stock/locations) 経由で
  │   「同一SKUの他ロケ在庫」を通知本文に載せる (ローカル参照・fail-soft)
  └→ ③ apps/warehouse/sync-to-render.js --logizard-only → Render mirror_logizard_stock (全置換+件数検証)
```

## 設計の要点

- **欠品通知はローカル参照** (miniPC内で完結)。Render/回線障害が現場の通知を止めない。
- **時系列は持たない**。常に最新スナップショットのみ (`raw_lz_inventory` 全置換)。
  取込は DELETE+INSERT を**単一トランザクション**で行い、通知が取込中に空テーブルを見ることはない。
- **鮮度は必ず表示**。通知に「HH:MM時点」(sync_meta.logizard_last_import 由来) を添え、
  180分超は「⚠古い可能性」。**0件 (なし) と取得不能 (取得できず) は別表示**。
- mirror push は **取込90分以内のときだけ** 送る (古い snapshot の再送で synced_at だけ
  新しく見せない)。送信後に /api/status で件数+captured_at を突合 (fail-closed)。
- mirror 側 DDL は **fail-soft** (2026-07-12 mirror全停止の教訓)。この表の初期化失敗は
  この表の sync だけ 503 になり、他テーブルを道連れにしない。
- ping semantics: `ok` = 3ステップ完走 / `partial` = mirror push のみ失敗 (ローカルは最新。
  翌時間のランで自己回復) / `fail` = DL または取込失敗 (既存データは全域で温存)。

## 台帳 (jobs-registry)

`logizard-stock-hourly` (P2, anchor 09:00 +grace 3h, partial_max_days 2)。
毎時ジョブだが dead-man 判定は「当日どれか1回成功したか」。日中の停止は
欠品通知の「HH:MM時点」表示で現場からも見える。

## miniPC セットアップ (デプロイ手順)

1. `C:\tools\logizard-automation` に `auto-zaiko.js` (Edgeヘッドレス対応版) と
   `run-zaiko-scheduled.bat` 相当は不要 — 本ランナー (ps1) が直接 node を呼ぶ。
   会社PC版との差分は `LOGIZARD_HEADLESS` 対応のみ (会社PCは env 未設定で挙動不変)。
2. miniPC の `C:\tools\logizard-automation\.env` 末尾 (override は後勝ち) に:
   ```
   LOGIZARD_ZAIKO_OUT=C:\tools\logizard-automation\out\logizard_zaikosu.csv
   LOGIZARD_HEADLESS=1
   ```
   (⚠ .env 追記は末尾改行の有無を先に確認すること)
3. Task Scheduler 登録 (SYSTEM、毎日9:00開始・1時間間隔×10回):
   ```
   schtasks /Create /TN LogizardZaikoHourly /RU SYSTEM /SC DAILY /ST 09:00 ^
     /RI 60 /DU 09:10 ^
     /TR "powershell -NoProfile -ExecutionPolicy Bypass -File C:\Users\bfaith\bfaith-portal\scripts\logizard-stock\run-hourly.ps1"
   ```
4. 初回は手動実行して `logs\logizard-stock-hourly.log` と jobs-monitor の ping 到達を確認。

## 運用・トラブルシュート

- ログ: `C:\Users\bfaith\bfaith-portal\logs\logizard-stock-hourly.log` (3ステップの全出力) /
  `C:\tools\logizard-automation\logs\run.log` (DL結果) / error-shots (DL失敗時のスクショ)
- 手動再実行: `powershell -NoProfile -ExecutionPolicy Bypass -File C:\Users\bfaith\bfaith-portal\scripts\logizard-stock\run-hourly.ps1`
- Render 側の確認: `/apps/mirror/api/status` の `logizard_stock_count` / `logizard_stock_captured_at`
- ロジザードのセッション競合: 自動DLの定期実行主体は **miniPCのみ**。会社PCの
  Stream Deck「在庫CSV」(run-zaiko.bat) は手動バックアップ用に残っているが、
  毎時運用開始後は普段押さない (同一アカウントの同時ログインは追い出し合いになる。
  マシン内ロック logizard-session.lock は値札CSVと共有 = miniPC内は直列化済み)。

## 関連

- 欠品通知本文の組み立て: `apps/picking/stock-locations.js` (+ `notify.js`)
- service-api: `apps/warehouse/logizard-stock-service.js`
- mirror 受信: `apps/warehouse-mirror/router.js` の `logizard_stock` 分岐 / DDL は同 `db.js`
- テスト: `apps/picking/tests/test-notify.mjs` / `apps/warehouse-mirror/test-logizard-stock-sync.mjs`
