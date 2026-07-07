/**
 * qoo10-analytics router — Qoo10分析ツール (売上・広告・利益・メガ割損益の統合管理)
 *
 * 要件定義: g:/共有ドライブ/AI_reference/システム設計/Qoo10統合管理ダッシュボード_要件定義_20260706.md
 *
 * 設計原則 (§11):
 *   - Render 完結: warehouse-mirror.db 読み取りのみ (集計 SQL は queries.js に分離)
 *   - 精度ラベル: 速報 (NE受注ベース) / 自動(日次) (配送完了fact・手数料実額) / 推定 (メガ割
 *     セラー負担 50%) を API が明示
 *   - API は /api/v1/ prefix (将来拡張用)
 *   - 認可は server.js 側 requireAppAccess('qoo10-analytics')
 *   - 既存アプリ (qoo10-accounting = 会計用途) とは独立。mirror の読み取り参照のみ (§13-⑤)
 *
 * P1: アプリ骨格 + 概要タブ (タイル4枚 + トレンド)。速報は既存
 * mirror_f_sales_by_listing (mall='qoo10'、NE受注由来) を使用 = miniPC 変更なしで実現。
 * P3: 利益分析タブ (WF + SKUテーブル + SKU詳細) / 売れ筋タブ / 設定タブ (負担率・
 * メガ割開催回マスタ・APIキー期限)。
 * 取込・精算突合 (P4) / 広告 (P5) / メガ割 (P6) / 診断 (P8) / 返品・出品 (P9) は準備中タブ。
 */
import { Router } from 'express';
import {
  resolvePeriod, getOverview, getTrend,
  getWaterfall, getSkuProfit, getSkuDetail, getBestsellers,
  getSettings, saveSettings, getMegaEvents, addMegaEvent, deleteMegaEvent,
} from './queries.js';

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
      console.error(`[qoo10-analytics] ${req.method} ${req.originalUrl}: ${e.stack || e.message}`);
      res.status(500).json({ error: 'サーバー内部エラー (詳細はサーバーログ参照)' });
    }
  };
}

router.get('/', (req, res) => {
  res.render('qoo10-analytics', {
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

// ─── 利益分析 (P3) ───
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
  if (!sku) throw Object.assign(new Error('sku is required'), { status: 400 });
  return getSkuDetail(sku, from, to);
}));

// ─── 売れ筋 (P3) ───
router.get('/api/v1/bestsellers', api((req) => {
  const { from, to } = resolvePeriod(req.query.preset, req.query.from, req.query.to);
  const axis = ['units', 'sales', 'margin'].includes(req.query.axis) ? req.query.axis : 'sales';
  return getBestsellers(from, to, axis);
}));

// ─── 設定 (P3: 負担率・分析閾値・APIキー期限 + メガ割開催回マスタ) ───
router.get('/api/v1/settings', api(() => getSettings()));
router.put('/api/v1/settings', api((req) => saveSettings(req.body || {})));

router.get('/api/v1/mega-events', api(() => getMegaEvents()));
router.post('/api/v1/mega-events', api((req) => addMegaEvent(req.body || {})));
router.delete('/api/v1/mega-events/:id', api((req) => deleteMegaEvent(req.params.id)));

// CSV セルの安全化: 全セル quote + 式実行の危険先頭文字 (=+-@, tab, CR, LF) は ' を前置 (CSV injection 対策)
function csvCell(v) {
  let s = v === null || v === undefined ? '' : String(v);
  if (/^[=+\-@\t\r\n]/.test(s)) s = "'" + s;
  return `"${s.replace(/"/g, '""')}"`;
}

// CSV 出力 (precision_level 列付き — 要件 F-4)
router.get('/api/v1/sku-profit.csv', (req, res) => {
  try {
    const { from, to } = resolvePeriod(req.query.preset, req.query.from, req.query.to);
    const data = getSkuProfit(from, to, { q: req.query.q, sort: req.query.sort, dir: req.query.dir, limit: 20000 });
    const header = ['sku_code', 'ne_code', 'product_name', 'units_net', 'gmv', 'platform_fee', 'fee_pct', 'net_settlement', 'shipping', 'cogs', 'variable_margin_L1', 'megawari_discount', 'megapo_discount', 'burden_est', 'L2_ref', 'margin_pct', 'mega_dependency_pct', 'cost_status', 'match_tier', 'precision_level'];
    const lines = [header.map(csvCell).join(',')];
    for (const r of data.rows) {
      lines.push([
        r.sku_code, r.ne_code || '', r.product_name, r.units_net,
        r.gmv, r.platform_fee, r.fee_pct ?? '', r.net_settlement, r.shipping, r.cogs,
        r.variable_margin, r.megawari_discount, r.megapo_discount, r.burden_est, r.l2,
        r.margin_pct ?? '', r.mega_dependency_pct ?? '', r.cost_status, r.match_tier,
        'fact_fee_actual+burden_estimated',
      ].map(csvCell).join(','));
    }
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="qoo10-sku-profit_${from}_${to}.csv"`);
    // Excel 向け BOM
    res.send(String.fromCharCode(0xFEFF) + lines.join('\r\n'));
  } catch (e) {
    console.error(`[qoo10-analytics] sku-profit.csv: ${e.stack || e.message}`);
    res.status(500).json({ error: 'サーバー内部エラー (詳細はサーバーログ参照)' });
  }
});

export default router;
