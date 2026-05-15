-- =============================================================================
-- MF view _pre_pk_swap 参照修正 (2026-05-15)
-- =============================================================================
-- 適用理由: 6 view が存在しない `<table>_pre_pk_swap` を参照していたため、
--           無関係な ALTER TABLE 実行時に SQLite の view validation で全 DDL が
--           失敗していた (LINEギフト Phase 1 A-1 backfill 中に発覚)。
--           MF Phase 1a の append-only PK swap 戦略実施時、view 側の rename が
--           漏れていた痕跡 (実体表は suffix なしで存在)。
--
-- 適用方法: 単一トランザクションで全 6 view を DROP → CREATE
-- 適用結果: 6 view 全て SELECT 可能、行数:
--   v_mart_mf_executive_top_latest:           1
--   v_mart_mf_channel_sales_latest:         199
--   v_mart_mf_pl_monthly_latest:            912
--   v_mart_mf_cash_events_daily_latest:    1267
--   v_mart_mf_balance_snapshot_monthly_latest: 4277
--   v_mart_mf_anomaly_signals_latest:         0
--
-- 旧定義 (適用前): 各 view の FROM 句が "mart_..._pre_pk_swap" を参照
-- 新定義 (適用後): FROM 句から `_pre_pk_swap` 除去、実体表に直接当てる
-- =============================================================================

BEGIN;

DROP VIEW IF EXISTS v_mart_mf_executive_top_latest;
CREATE VIEW v_mart_mf_executive_top_latest AS
SELECT m.*
FROM mart_mf_executive_top m
WHERE m.run_id = (
  SELECT MAX(run_id) FROM mf_publish_runs WHERE status = 'success' AND scope IN ('all','executive_top')
);

DROP VIEW IF EXISTS v_mart_mf_channel_sales_latest;
CREATE VIEW v_mart_mf_channel_sales_latest AS
SELECT m.*
FROM mart_mf_channel_sales m
WHERE m.run_id = (
  SELECT MAX(run_id) FROM mf_publish_runs WHERE status = 'success' AND scope IN ('all','channel_sales')
);

DROP VIEW IF EXISTS v_mart_mf_pl_monthly_latest;
CREATE VIEW v_mart_mf_pl_monthly_latest AS
SELECT m.*
FROM mart_mf_pl_monthly m
WHERE m.run_id = (
  SELECT MAX(run_id) FROM mf_publish_runs WHERE status = 'success' AND scope IN ('all','base','pl_monthly')
);

DROP VIEW IF EXISTS v_mart_mf_cash_events_daily_latest;
CREATE VIEW v_mart_mf_cash_events_daily_latest AS
SELECT m.*
FROM mart_mf_cash_events_daily m
WHERE m.run_id = (
  SELECT MAX(run_id) FROM mf_publish_runs WHERE status = 'success' AND scope IN ('all','base','cash_events_daily')
);

DROP VIEW IF EXISTS v_mart_mf_balance_snapshot_monthly_latest;
CREATE VIEW v_mart_mf_balance_snapshot_monthly_latest AS
SELECT m.*
FROM mart_mf_balance_snapshot_monthly m
WHERE m.run_id = (
  SELECT MAX(run_id) FROM mf_publish_runs WHERE status = 'success' AND scope IN ('all','base','balance_snapshot_monthly')
);

DROP VIEW IF EXISTS v_mart_mf_anomaly_signals_latest;
CREATE VIEW v_mart_mf_anomaly_signals_latest AS
SELECT m.*
FROM mart_mf_anomaly_signals m
WHERE m.run_id = (
  SELECT MAX(run_id) FROM mf_publish_runs WHERE status = 'success' AND scope IN ('all','anomaly_signals')
);

COMMIT;
