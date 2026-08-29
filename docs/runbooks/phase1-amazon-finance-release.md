# Phase 1 Amazon Finance — Release Runbook

**Status**: production (Phase 1 mini-PC 完結版、2026-05-08)
**対象**: `f_amazon_finance_sku_daily_v1` daily fact build pipeline
**スコープ**: miniPC 内の build + validation + alert (Render 側 sync は Phase 1.x で追記)

このドキュメントは Phase 1 Amazon Finance の **本番運用手順** を凍結したもの。Phase 1 で実装した 4 ticket (#1-0a, #1-0, #1-1, #1-7a, #1-8a) を組み合わせて backfill/release/backout/replay を行う。

## 前提環境

- ホスト: miniPC (192.168.68.50、`bfaith` ユーザー、旧 .65 だったが 2026-05-18 防犯カメラ配線変更で DHCP 再割当)
- DB: `C:\Users\bfaith\bfaith-portal\data\warehouse.db` (better-sqlite3 + WAL)
- Node: 24.x
- 必須環境変数: `DATA_DIR=C:\Users\bfaith\bfaith-portal\data`
- オプション環境変数: `GCHAT_WEBHOOK_INSIGHT` (DQ alert 通知先)

`DATA_DIR` 未指定時は全 script が **fail-fast** で停止 (worktree DB 事故防止)。

## 関連ドキュメント

- 設計: `docs/amazon-finance/phase1-plan.md`
- Source contract: `docs/contracts/raw_amazon_settlement_lines.contract.md` (v1.1)
- Canonical metric mapping: `docs/amazon-finance/canonical-metric-mapping.md` (v1)
- Rehearsal 実績: `docs/runbooks/phase1-amazon-finance-rehearsal.md`
- Release 判定 SQL: `sql/runbooks/phase1_release_checks.sql`

## 1. Preflight (実施前チェック)

```powershell
# 1-A. source contract が変わっていないか
$env:DATA_DIR="C:\Users\bfaith\bfaith-portal\data"
node apps/warehouse/scripts/contracts/assert-raw-amazon-source.js
# → exit 0 で PASS、それ以外は contract drift

# 1-B. job_locks に未解放の lock が残っていないか
node apps/warehouse/show-job-locks.js
# → 古い stale lock があれば cleanup
node apps/warehouse/show-job-locks.js --cleanup

# 1-C. 既存 fact 行数の baseline を控える (差分検証用)
sqlite3 "$env:DATA_DIR\warehouse.db" "
  SELECT substr(date_jst,1,7) AS month, COUNT(*) AS rows, ROUND(SUM(profit_amount), 0) AS profit_jpy
  FROM f_amazon_finance_sku_daily_v1
  GROUP BY 1 ORDER BY 1;
"
```

成否判定:
- assert-raw-amazon-source.js exit 0
- 既存 lock 無し
- baseline 取得済

## 2. Backfill (月単位)

```powershell
# 2-A. 単月 backfill (新規月、既存 row なし)
node apps/warehouse/build-daily-fact.js `
  --data-dir "C:\Users\bfaith\bfaith-portal\data" `
  --month 2026-04 `
  --ddl-path "C:\Users\bfaith\bfaith-portal\f_amazon_finance_sku_daily_v1.sql" `
  --build-sql-path "C:\Users\bfaith\bfaith-portal\build_f_amazon_finance_sku_daily_v1.sql"

# 2-B. force-rebuild (既存月の上書き、snapshot 不変条件は破壊するので注意)
node apps/warehouse/build-daily-fact.js `
  --data-dir "C:\Users\bfaith\bfaith-portal\data" `
  --month 2026-04 `
  --force-rebuild `
  --ddl-path ... `
  --build-sql-path ...

# 2-C. dry-run (既存値を見るだけ、書き込まない)
node apps/warehouse/build-daily-fact.js --month 2026-04 --dry-run ...
```

成否判定:
- exit 0
- 末尾の summary で daily 比較 (v4 との差) が許容内
- snapshot 不変条件: force-rebuild を使わない限り既存 row は上書きされない

期待値 (5 ヶ月合計、2026-05-08 計測):
- units: 217,624 (v4 と完全一致)
- revenue: 220.45M (v4 220.73M、-0.13%)
- profit (gross+補填): 27.78M (v4 27.73M、+0.18%)

## 3. Validation (DQ gate)

```powershell
# 3-A. DQ gate を月単位で実行
node apps/warehouse/run-amazon-finance-dq.js --month 2026-04
# → 6 check + 7 bucket、exit 1 で gate failure

# 3-B. 結果確認 (DB 内)
sqlite3 "$env:DATA_DIR\warehouse.db" "
  SELECT check_name, severity, ROUND(actual_value, 3) AS actual, threshold_value
  FROM dq_run_results
  WHERE run_id LIKE 'dq-2026-04-%'
  ORDER BY checked_at DESC, severity DESC, check_name;
"

# 3-C. bucket 詳細
sqlite3 "$env:DATA_DIR\warehouse.db" "
  SELECT bucket_code, ROUND(SUM(bucket_amount), 0) AS total_jpy, SUM(row_count) AS total_rows
  FROM accounting_diff_buckets
  WHERE run_id LIKE 'dq-2026-04-%'
  GROUP BY 1 ORDER BY 1;
"
```

threshold (Phase 1 暫定値):
- `row_count_drift`: 0 (厳密一致)
- `monthly_total_diff_pct`: warn 0.1% / error 0.5%
- `unbucketed_diff_jpy`: warn 500 / error 5000 (Phase 1.x で精度改善予定、現状 ~20K 残差)
- `missing_cost_rate_pct`: warn 5% / error 10%
- `sku_null_count`: 0
- `cost_late_binding_pct`: warn 1% / error 2%

## 4. Concurrency Guard (重複起動防止)

`job_locks` を使った排他制御。daily-sync / build / DQ runner それぞれで `acquireLock()` → `releaseLock()` の慣習。

```powershell
# 4-A. lock 状態確認
node apps/warehouse/show-job-locks.js

# 4-B. stale lock 一括削除 (TTL 切れのみ)
node apps/warehouse/show-job-locks.js --cleanup

# 4-C. smoke test
node apps/warehouse/test-job-locks.js
# → 19/19 PASS が期待値
```

Job 名規約 (Phase 1):
- `daily-sync`: 全体 daily-sync orchestrator (Phase 1.x で実装)
- `build-amazon-finance-{YYYY-MM}`: 月単位 build
- `rebuild-amazon-settlement-mart-{YYYY-MM}`: rebuild
- `run-amazon-finance-dq-{YYYY-MM}`: DQ runner

## 5. Backout (失敗時の取り消し)

snapshot 不変条件のため、`f_amazon_finance_sku_daily_v1` の **個別 row 削除は通常やらない**。月単位の取り消しが必要な場合のみ実行。

```powershell
# 5-A. 対象月の row を確認
sqlite3 "$env:DATA_DIR\warehouse.db" "
  SELECT COUNT(*) FROM f_amazon_finance_sku_daily_v1 WHERE substr(date_jst,1,7) = '2026-04';
"

# 5-B. 月単位 backout (DELETE)
sqlite3 "$env:DATA_DIR\warehouse.db" "
  DELETE FROM f_amazon_finance_sku_daily_v1 WHERE substr(date_jst,1,7) = '2026-04';
"

# 5-C. (緊急時) 全 row 削除
sqlite3 "$env:DATA_DIR\warehouse.db" "DROP TABLE IF EXISTS f_amazon_finance_sku_daily_v1;"
# → 次の build-daily-fact.js 実行で IF NOT EXISTS DDL が走り再作成される
```

backout 条件:
- DQ gate で `severity='error'` が出た & 原因が build SQL の不具合と判明
- snapshot 原価が不正な値で書かれた

backout 後は必ず再 backfill + DQ gate 再実行。

## 6. Replay (再実行)

```powershell
# 6-A. 単月 replay (force-rebuild)
node apps/warehouse/build-daily-fact.js `
  --data-dir "C:\Users\bfaith\bfaith-portal\data" `
  --month 2026-04 `
  --force-rebuild `
  --ddl-path ... `
  --build-sql-path ...

# 6-B. 5 ヶ月分 replay
foreach ($ym in @('2026-01','2026-02','2026-03','2026-04','2026-05')) {
  node apps/warehouse/build-daily-fact.js --data-dir "..." --month $ym --force-rebuild ...
}

# 6-C. replay 後 DQ gate 再実行
node apps/warehouse/run-amazon-finance-dq.js --month 2026-04
```

replay の冪等性:
- `--force-rebuild` で月単位 DELETE → 再 INSERT
- 同じ source DB の状態なら結果も同じ (snapshot 原価のため `unit_cost_snapshot` は build 時点の v_sku_costed 値で固定)

## Phase 1.x で追記予定の section

- **5.x Mirror Sync**: Render 側 `mirror_amazon_finance_sku_daily` への sync (#1-4 / #1-4a 完成後)
- **5.y Consumer Cutover**: Render 側 view 切替 (#1-5 完成後)
- **5.z Push Rebuild Trigger**: miniPC → Render rebuild trigger (#1-7 完成後)

## トラブルシューティング

### DQ gate で `unbucketed_diff_jpy > 5000`

- 通常の `Phase 1 #1-1` 残差 (silver dedup の微小差 + v_sku_costed の四捨五入) → 約 20K 円程度なら正常
- 大きく超える場合は v4 の rebuild が新しい month で走った可能性。`fact_amazon_settlement_monthly_wide` を確認

### `assert-raw-amazon-source.js` が FAIL

- raw_amazon_settlement_lines のスキーマが変わった (Settlement Report 仕様変更等)
- → `docs/contracts/raw_amazon_settlement_lines.contract.md` を v1.2 に更新して再凍結

### `cost_status='missing_cost'` 多発

- 新規 SKU 追加で原価マスタ未登録の SKU が増えた
- → m_products に原価登録、replay で更新

### lock が長期間 active

- 別 instance が走っているか、または release 漏れ
- → `show-job-locks.js` で確認、`--cleanup` で stale 削除

## 改訂履歴

| Version | Date | Changes |
|---|---|---|
| v1 | 2026-05-08 | Phase 1 #1-6 として初回作成。#1-1/#1-7a/#1-8a の 3 ticket を組み合わせた miniPC 内完結版 runbook |
