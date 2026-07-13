/**
 * yahoo-analytics router — ヤフー分析ツール (売上・広告・利益・検索順位の統合管理)
 *
 * 要件定義: g:/共有ドライブ/AI_reference/システム設計/Yahoo統合管理ダッシュボード_要件定義_20260706.md
 *
 * 設計原則 (§11):
 *   - Render 完結: warehouse-mirror.db 読み取りのみ (集計 SQL は queries.js に分離)
 *   - 精度ラベル: 速報 (受注ベース) / 推定 (mall_fee 10%一律) / 確定 (月次請求明細) を API が明示
 *   - API は /api/v1/ prefix (将来拡張用)
 *   - 認可は server.js 側 requireAppAccess('yahoo-analytics')
 *   - 既存アプリ (yahoo-accounting = 会計用途) とは独立。mirror の読み取り参照のみ (§11-5)
 *
 * P1 スコープ: アプリ骨格 + 概要タブ (タイル4枚 + トレンド)。
 * 利益分析 (P2) / 売れ筋・設定 (P3) / 取込 (P5) / 広告 (P6) / 検索・順位 (P7-P8) /
 * キャンペーン・診断 (P9) は準備中タブ。
 */
import { Router } from 'express';
import {
  resolvePeriod, getOverview, getTrend,
  getWaterfall, getSkuProfit, getSkuDetail, getUnresolved,
  getBestsellers, getSettings, saveSettings, getRates, addRate, deleteRate,
  getSearchKeywords, getAcquisition, getFlashLatest,
} from './queries.js';
import { runInsights, listInsights, setInsightStatus } from './insights.js';

const router = Router();

// 500 を JSON で返す共通 wrapper (API は画面から fetch されるため HTML error page を返さない)
// エラー詳細 (SQL/テーブル名等) はログのみ。クライアントには固定文言。
// 例外: e.status=400 (validationError) はユーザー起因なのでメッセージをそのまま返す
function api(handler) {
  return (req, res) => {
    try {
      res.json(handler(req));
    } catch (e) {
      if (e.status === 400) {
        res.status(400).json({ error: e.message });
        return;
      }
      console.error(`[yahoo-analytics] ${req.method} ${req.originalUrl}: ${e.stack || e.message}`);
      res.status(500).json({ error: 'サーバー内部エラー (詳細はサーバーログ参照)' });
    }
  };
}

router.get('/', (req, res) => {
  res.render('yahoo-analytics', {
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

// ─── 利益分析 (P2) ───
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

router.get('/api/v1/sku-detail', api((req) => {
  const { from, to } = resolvePeriod(req.query.preset, req.query.from, req.query.to);
  const sku = (req.query.sku || '').trim();
  if (!sku) throw new Error('sku is required');
  return getSkuDetail(sku, from, to);
}));

router.get('/api/v1/unresolved', api((req) => getUnresolved(req.query.days)));

// ─── 売れ筋 (P3) ───
router.get('/api/v1/bestsellers', api((req) => {
  const { from, to } = resolvePeriod(req.query.preset, req.query.from, req.query.to);
  const axis = ['units', 'sales', 'margin'].includes(req.query.axis) ? req.query.axis : 'sales';
  return getBestsellers(from, to, axis);
}));

// ─── 設定 (P3: 分析閾値 + 料率マスタ) ───
router.get('/api/v1/settings', api(() => getSettings()));
router.put('/api/v1/settings', api((req) => saveSettings(req.body || {})));

// ─── 統計統合 (P6: mall-csv-fetcher 自動取得データ) ───
router.get('/api/v1/search-keywords', api((req) => {
  const { from, to } = resolvePeriod(req.query.preset, req.query.from, req.query.to);
  return getSearchKeywords(from, to);
}));
router.get('/api/v1/acquisition', api((req) => {
  const { from, to } = resolvePeriod(req.query.preset, req.query.from, req.query.to);
  return getAcquisition(from, to);
}));
router.get('/api/v1/flash-latest', api(() => getFlashLatest()));

// ─── 診断 = インサイト基盤 (P6)。GET /insights が AI エージェント向け契約 ───
router.get('/api/v1/insights', api((req) => listInsights({ status: req.query.status, include_resolved: req.query.include_resolved === '1' })));
router.post('/api/v1/insights/run', api(() => runInsights('manual')));
router.put('/api/v1/insights/:id/status', api((req) => setInsightStatus(req.params.id, req.body?.status, req.body?.memo)));

router.get('/api/v1/rates', api(() => getRates()));
router.post('/api/v1/rates', api((req) => addRate(req.body || {})));
router.delete('/api/v1/rates/:id', api((req) => deleteRate(req.params.id)));

// CSV セルの安全化: 全セル quote + 式実行の危険先頭文字 (=+-@, tab, CR, LF) は ' を前置 (CSV injection 対策)
function csvCell(v) {
  let s = v === null || v === undefined ? '' : String(v);
  if (/^[=+\-@\t\r\n]/.test(s)) s = "'" + s;
  return `"${s.replace(/"/g, '""')}"`;
}
function sendCsv(res, filename, header, rows) {
  const lines = [header.map(csvCell).join(',')];
  for (const r of rows) lines.push(r.map(csvCell).join(','));
  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
  // Excel 向け BOM
  res.send(String.fromCharCode(0xFEFF) + lines.join('\r\n'));
}

// CSV 出力 (precision_level 列付き — 要件 F-4)
router.get('/api/v1/sku-profit.csv', (req, res) => {
  try {
    const { from, to } = resolvePeriod(req.query.preset, req.query.from, req.query.to);
    const data = getSkuProfit(from, to, { q: req.query.q, sort: req.query.sort, dir: req.query.dir, limit: 20000 });
    sendCsv(res, `yahoo-sku-profit_${from}_${to}.csv`,
      ['yahoo_sku_key', 'ne_code', 'product_name', 'units_net', 'sales_incl', 'coupon_shop', 'use_point', 'mall_fee_est', 'shipping', 'cogs', 'variable_margin', 'margin_pct', 'cost_status', 'resolution_method', 'precision_level'],
      data.rows.map(r => [
        r.yahoo_sku_key, r.ne_code || '', r.product_name, r.units_net,
        r.sales_incl, r.coupon_shop, r.use_point, r.mall_fee_est, r.shipping, r.cogs,
        r.variable_margin, r.margin_pct ?? '', r.cost_status, r.resolution_method,
        'flash+fee_estimated_10pct',
      ]));
  } catch (e) {
    console.error(`[yahoo-analytics] sku-profit.csv: ${e.stack || e.message}`);
    res.status(500).json({ error: 'サーバー内部エラー (詳細はサーバーログ参照)' });
  }
});

// 未解決 SKU の CSV (f_yahoo_sku_map 登録作業用: yahoo_key 列がそのまま登録キー。ne_code 列を埋めて miniPC 側で取込)
router.get('/api/v1/unresolved.csv', (req, res) => {
  try {
    const data = getUnresolved(req.query.days);
    sendCsv(res, `yahoo-unresolved-sku_${data.from}_.csv`,
      ['yahoo_key(=f_yahoo_sku_map登録キー)', 'sub_code', 'product_name', 'last_seen', 'units_net', 'sales_incl', 'candidate_ne_codes', 'ne_code(記入欄)'],
      data.rows.map(r => [
        r.yahoo_sku_key, r.variant_key, r.product_name,
        r.last_seen, r.units_net, r.sales_incl,
        r.candidates.map(c => c.ne_code).join(' / '), '',
      ]));
  } catch (e) {
    console.error(`[yahoo-analytics] unresolved.csv: ${e.stack || e.message}`);
    res.status(500).json({ error: 'サーバー内部エラー (詳細はサーバーログ参照)' });
  }
});

export default router;
