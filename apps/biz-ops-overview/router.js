/**
 * 業務オペ概要ダッシュボード (biz-ops-overview) — Router
 *
 * 2026-05-19 中原さん要望 + Codex consultation 反映:
 *   - 全モール売上 (前日 / 今月累計 / 過去30日累計) を 1 ページに集約
 *   - 将来: 出荷率 (mis-shipment Phase E 連携) / 在庫サマリ要約 / 利益率 / KPI
 *   - 棲み分け: exec-dashboard=月次会計、biz-ops-overview=日次業務、sales-analytics-{mall}=SKU 詳細
 *
 * 売上定義: 顧客支払額 (税込、手数料引かず)
 *   - Amazon: sales_principal_jpy × 1.10 (税抜 → 税込換算、軽減税率 SKU は過大評価の可能性あり)
 *   - 楽天/Yahoo/au PAY/LINEギフト: sales_principal_jpy_incl そのまま
 *   - Qoo10: gmv_list_price_jpy_incl そのまま
 *   - メルカリ: fact 無し、Phase 2 で NE 連携で追加
 *
 * データソース: v_mall_sales_daily_unified (warehouse-mirror.db、initDB で auto-create)
 */
import { Router } from 'express';
import { getMirrorDB } from '../warehouse-mirror/db.js';
import { getSalesSummary } from './lib/sales-summary.js';

const router = Router();

router.get('/', (req, res) => {
  res.render('biz-ops-overview', {
    title: '業務オペ概要',
    username: req.session?.email,
    displayName: req.session?.displayName,
  });
});

// API: 売上サマリ JSON
router.get('/api/sales-summary', (req, res) => {
  try {
    const db = getMirrorDB();
    const summary = getSalesSummary(db);
    res.json({ ok: true, summary });
  } catch (e) {
    console.error('[biz-ops-overview] sales-summary error:', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

export default router;
