/**
 * MFクラウド経営トップダッシュボード — Phase 1a
 *
 * mirror_mf_* (miniPC build-marts.js から sync 受信) の VIEW を読んで表示。
 *   v_mirror_mf_executive_top_latest      経営トップ (PL/Cash/危険信号)
 *   v_mirror_mf_pl_monthly_latest         月次PL
 *   v_mirror_mf_channel_sales_latest      モール別売上純額
 *   v_mirror_mf_cash_events_daily_latest  日次現金イベント (過去90日)
 *   v_mirror_mf_balance_snapshot_monthly_latest  月末残高
 *   v_mirror_mf_anomaly_signals_latest    異常検知 (state JOIN 済)
 *
 * 全 VIEW は status='success' の最新 run のみ公開 (Phase 1a finalize 設計)
 */
import { Router } from 'express';
import { getMirrorDB } from '../warehouse-mirror/db.js';

const router = Router();

// ─── メイン画面 ───
router.get('/', (req, res) => {
  res.render('exec-dashboard', {
    title: 'MF経営トップダッシュボード',
    username: req.session?.email,
    displayName: req.session?.displayName,
  });
});

// ─── API: スナップショット (executive_top + 関連サマリ) ───
router.get('/api/snapshot', (req, res) => {
  try {
    const db = getMirrorDB();

    // 1. 経営トップ (1 row 想定)
    const exec = db.prepare(`SELECT * FROM v_mirror_mf_executive_top_latest`).get();
    if (!exec) {
      return res.json({
        ok: true, available: false,
        message: 'まだ MF データが Render に sync されていません。miniPC で daily-sync を待ってください。',
      });
    }

    // 2. publish run メタ
    const runMeta = db.prepare(`
      SELECT run_id, scope, status, started_at, finished_at, finalized_at, synced_at
      FROM mirror_mf_publish_runs
      WHERE run_id = ?
    `).get(exec.run_id);

    // 3. PL 月次 (直近24ヶ月、role 別)
    const plRows = db.prepare(`SELECT * FROM v_mirror_mf_pl_monthly_latest ORDER BY month_ym DESC, role_key`).all();

    // 4. チャネル売上 (直近12ヶ月)
    const channelRows = db.prepare(`
      SELECT * FROM v_mirror_mf_channel_sales_latest
      WHERE month_ym >= date('now', 'start of month', '-12 months')
      ORDER BY month_ym DESC, gross_sales_excl_tax DESC
    `).all();

    // 5. 月末残高 (直近12ヶ月、現金系のみ)
    const balanceRows = db.prepare(`
      SELECT month_ym, account_name, sub_account_name, closing_balance_excl_tax
      FROM v_mirror_mf_balance_snapshot_monthly_latest
      WHERE month_ym >= date('now', 'start of month', '-12 months')
        AND account_name IN ('普通預金', '当座預金', '現金', '小口現金', '普通預金_PayPal', '普通預金_ペイオニア')
      ORDER BY month_ym DESC, account_name, sub_account_name
    `).all();

    // 6. 過去90日 現金流入出
    const cashRows = db.prepare(`
      SELECT bank_account_key,
        SUM(CASE WHEN direction='in' THEN amount_excl_tax ELSE 0 END) as in_total,
        SUM(CASE WHEN direction='out' THEN amount_excl_tax ELSE 0 END) as out_total,
        SUM(CASE WHEN direction='in' THEN event_count ELSE 0 END) as in_cnt,
        SUM(CASE WHEN direction='out' THEN event_count ELSE 0 END) as out_cnt
      FROM v_mirror_mf_cash_events_daily_latest
      WHERE movement_date >= date('now', '-90 days')
      GROUP BY bank_account_key
      ORDER BY (in_total + out_total) DESC
    `).all();

    // 7. 異常検知 (open のみ、severity_rank 降順)
    const anomalies = db.prepare(`
      SELECT signal_id, signal_code, severity, severity_rank, title, description,
             observed_value, threshold_value, recommended_action,
             state_status, suppress_until, acked_by, acked_at
      FROM v_mirror_mf_anomaly_signals_latest
      WHERE state_status IS NULL OR state_status NOT IN ('closed', 'snoozed')
      ORDER BY severity_rank DESC, detected_at DESC
      LIMIT 10
    `).all();

    res.json({
      ok: true, available: true,
      exec, runMeta, plRows, channelRows, balanceRows, cashRows, anomalies,
    });
  } catch (e) {
    console.error('[exec-dashboard] snapshot error:', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

export default router;
