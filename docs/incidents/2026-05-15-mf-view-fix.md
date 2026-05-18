# Incident: MF view 6 個 `_pre_pk_swap` 参照 (2026-05-15)

## 概要

MF (会計) ダッシュボード用の latest view 6 個が、存在しない `<table>_pre_pk_swap` テーブルを参照しており、SQLite の DDL 時 view validation で**任意の `ALTER TABLE` / `CREATE TABLE` が失敗**する状態になっていた。

LINEギフト Phase 1 A-1 backfill (`apps/warehouse/linegift-orders.js`) 中に発覚。

## 影響範囲

- **読取系 (UI)**: `apps/exec-dashboard/` が 6 view を参照しているため、対応前は MF executive_top / channel_sales / pl_monthly / cash_events_daily / balance_snapshot_monthly / anomaly_signals が **全て表示不能** だった可能性 (詳細未調査)
- **書込系 (DDL)**: 任意の `ALTER TABLE` / `CREATE TABLE` が view validation で巻き添え失敗。今回 LINEギフト の legacy 退避 rename が hit
- **データ**: 実体表 `mart_mf_*` 6 個は無事、データ損失なし

## 原因

memory: `feedback_append_only_run_id_pk.md` MF Phase 1a で append-only 表の PK swap 戦略を実施した時、view 側の rename が漏れたまま放置されていた。

実体表は `_pre_pk_swap` suffix なし (`mart_mf_executive_top` 等) で存在、view だけ古い名前を参照。

## 検出

```
SqliteError: error in view v_mart_mf_executive_top_latest:
  no such table: main.mart_mf_executive_top_pre_pk_swap
  at Database.exec ...
  at ensureTables (apps/warehouse/linegift-orders.js:506:8)
```

## 一次対応 (2026-05-15 14:30 JST)

1. `data/mf_view_backup_20260515.sql` に旧 view 定義 6 個をダンプ (miniPC ローカル退避)
2. 単一トランザクションで 6 view を `DROP` → `CREATE` 再作成 (FROM 句から `_pre_pk_swap` 除去)
3. 各 view の SELECT 動作確認

### 検証結果

| view | 行数 |
|---|---|
| `v_mart_mf_executive_top_latest` | 1 |
| `v_mart_mf_channel_sales_latest` | 199 |
| `v_mart_mf_pl_monthly_latest` | 912 |
| `v_mart_mf_cash_events_daily_latest` | 1267 |
| `v_mart_mf_balance_snapshot_monthly_latest` | 4277 |
| `v_mart_mf_anomaly_signals_latest` | 0 |

その後 LINEギフト backfill 再実行 → 完全成功 (inserted=2416 / skip 0 / fail-closed 全パス)。

## 適用 SQL

`sql/incidents/2026-05-15-mf-view-fix.sql` 参照 (一時退避 backup は miniPC 上 `data/mf_view_backup_20260515.sql`)。

## 再発防止 (今後)

- [ ] `mf-research` repo (MF view を生成している側) に追跡 issue を起こす — view 定義側の `_pre_pk_swap` ハードコード除去 + migration 化
- [ ] `bfaith-portal` mirror sync 時の schema integrity check 検討 (view の FROM 句が実在する table を参照しているか確認するスクリプト)
- [ ] MF Phase 1a swap 戦略の手順書を見直し、view 側 rename を必須ステップに

## 関連

- LINEギフト Phase 1 A-1 PR: (このコミットを含む PR にて関連付け)
- memory: `feedback_append_only_run_id_pk.md`
- memory: `feedback_publish_run_finalize_pattern.md`
