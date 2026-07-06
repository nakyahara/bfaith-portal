/**
 * amazon-dashboard router — Amazon統合管理ダッシュボード (売上・広告・利益)
 *
 * 要件定義: g:/共有ドライブ/AI_reference/システム設計/Amazon統合管理ダッシュボード_要件定義_20260706.md
 *
 * 設計原則 (§9):
 *   - Render 完結: warehouse-mirror.db 読み取りのみ (集計 SQL は queries.js に分離)
 *   - 精度ラベル: 確定 (settlement) / 速報 (受注) / 推定 を API が明示、UI がバッジ表示
 *   - API は /api/v1/ prefix (将来拡張用)
 *   - 認可は server.js 側 requireAppAccess('amazon-dashboard')
 */
import { Router } from 'express';
import {
  resolvePeriod, isValidDate,
  getOverview, getTrend, getWaterfall, getSkuProfit,
  getAdsAnalysis, getBestsellers, getDiagnosis, getInventory,
  getSettings, saveSettings, listExpenses, addExpense, deleteExpense,
} from './queries.js';

const router = Router();

// 500 を JSON で返す共通 wrapper (API は画面から fetch されるため HTML error page を返さない)
function api(handler) {
  return (req, res) => {
    try {
      res.json(handler(req));
    } catch (e) {
      console.error(`[amazon-dashboard] ${req.method} ${req.originalUrl}: ${e.message}`);
      res.status(500).json({ error: e.message });
    }
  };
}

router.get('/', (req, res) => {
  res.render('amazon-dashboard', {
    username: req.session?.email,
    displayName: req.session?.displayName,
  });
});

// ─── 概要 ───
router.get('/api/v1/overview', api(() => getOverview()));

router.get('/api/v1/trend', api((req) => {
  const { from, to } = resolvePeriod(req.query.preset, req.query.from, req.query.to);
  const granularity = ['day', 'week', 'month'].includes(req.query.granularity) ? req.query.granularity : 'day';
  return getTrend(from, to, granularity);
}));

// ─── 利益分析 ───
router.get('/api/v1/waterfall', api((req) => {
  const { from, to } = resolvePeriod(req.query.preset, req.query.from, req.query.to);
  const sku = (req.query.sku || '').trim() || null;
  return getWaterfall(from, to, sku);
}));

router.get('/api/v1/sku-profit', api((req) => {
  const { from, to } = resolvePeriod(req.query.preset, req.query.from, req.query.to);
  return getSkuProfit(from, to, {
    q: req.query.q, sort: req.query.sort, dir: req.query.dir,
    limit: req.query.limit, offset: req.query.offset,
  });
}));

// CSV 出力 (precision_level 列付き、要件定義 F-4)
router.get('/api/v1/sku-profit.csv', (req, res) => {
  try {
    const { from, to } = resolvePeriod(req.query.preset, req.query.from, req.query.to);
    const data = getSkuProfit(from, to, { q: req.query.q, sort: req.query.sort, dir: req.query.dir, limit: 10000 });
    const header = ['seller_sku', 'asin', 'product_name', 'units_net', 'revenue_excl', 'fees', 'promotion', 'refunds', 'reimbursements', 'cogs', 'profit_before_ads', 'ad_direct', 'ad_allocated', 'profit_after_ads', 'margin_pct', 'cost_status', 'precision_level'];
    const lines = [header.join(',')];
    for (const r of data.rows) {
      const precision = r.ad_allocated > 0 ? 'settled+allocated_ads' : 'settled';
      lines.push([
        r.seller_sku, r.asin || '', `"${(r.product_name || '').replace(/"/g, '""')}"`,
        r.units_net, r.revenue_excl, r.fees, r.promotion, r.refunds, r.reimbursements,
        r.cogs, r.profit_before_ads, r.ad_direct, r.ad_allocated, r.profit_after_ads,
        r.margin_pct ?? '', r.cost_status, precision,
      ].join(','));
    }
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="amazon-sku-profit_${from}_${to}.csv"`);
    // Excel 向け BOM
    res.send(String.fromCharCode(0xFEFF) + lines.join('\r\n'));
  } catch (e) {
    console.error(`[amazon-dashboard] csv: ${e.message}`);
    res.status(500).json({ error: e.message });
  }
});

// ─── 広告 ───
router.get('/api/v1/ads', api((req) => {
  const { from, to } = resolvePeriod(req.query.preset, req.query.from, req.query.to);
  return getAdsAnalysis(from, to);
}));

// ─── 売れ筋 ───
router.get('/api/v1/bestsellers', api((req) => {
  const { from, to } = resolvePeriod(req.query.preset, req.query.from, req.query.to);
  const axis = ['units', 'sales', 'profit'].includes(req.query.axis) ? req.query.axis : 'sales';
  return getBestsellers(from, to, axis);
}));

// ─── 診断 ───
router.get('/api/v1/diagnosis', api(() => getDiagnosis()));

// ─── 在庫 (v1) ───
router.get('/api/v1/inventory', api(() => getInventory()));

// ─── 設定 (カスタム経費 + 診断閾値) ───
router.get('/api/v1/settings', api(() => getSettings()));
router.put('/api/v1/settings', api((req) => saveSettings(req.body || {})));

router.get('/api/v1/expenses', api(() => ({ items: listExpenses() })));
router.post('/api/v1/expenses', api((req) => addExpense(req.body || {})));
router.delete('/api/v1/expenses/:id', api((req) => deleteExpense(req.params.id)));

export default router;
