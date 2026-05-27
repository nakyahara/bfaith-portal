/**
 * apps/sales-analytics-linegift/router.js — 手動分類 UI + API
 *
 * 設計書: g:/共有ドライブ/AI_reference/システム設計/LINEギフトPhase1設計書_v0.9_20260526.md §13-7
 *
 * URL:
 *   /apps/sales-analytics-linegift/                UI (未分類キュー画面)
 *   /apps/sales-analytics-linegift/api/counters    GET  全体カウンタ
 *   /apps/sales-analytics-linegift/api/unmapped    GET  未分類キュー (UNMAPPED 一覧、売上順)
 *   /apps/sales-analytics-linegift/api/price-bands GET  価格帯ドロップダウン用
 *   /apps/sales-analytics-linegift/api/lookup      GET  ne_product_code から商品名 lookup (オートコンプリート)
 *   /apps/sales-analytics-linegift/api/mapping     POST 手動分類: UNMAPPED → CONFIRMED 昇格
 *   /apps/sales-analytics-linegift/api/classification POST 手動 m_product_classifications (MANUAL ソース)
 *
 * 認証: server.js で requireAppAccess('sales-analytics-linegift') を mount 時に適用。
 */
import { Router } from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import {
  listUnmapped, setManualMapping, setManualClassification,
  getCounters, listPriceBands, lookupNeProduct,
  getKpiSummary, getMonthlyTrend, getSkuRanking,
  getPriceBandSummaryByWindow, listAvailableGenres, listAvailableSeasons,
} from './db.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = Router();

function currentUser(req) {
  return req.session?.email || req.session?.displayName || 'unknown';
}

// ─── UI ───
// デフォルトはダッシュボード画面 (A-6)、手動分類画面は /classify で別ルート
router.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'views', 'dashboard.html'));
});
router.get('/classify', (req, res) => {
  res.sendFile(path.join(__dirname, 'views', 'index.html'));
});

// ─── API: 全体カウンタ ───
router.get('/api/counters', (req, res) => {
  try {
    res.json({ ok: true, result: getCounters() });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ─── API: 未分類キュー (UNMAPPED 一覧) ───
router.get('/api/unmapped', (req, res) => {
  try {
    const limit = Math.max(1, Math.min(500, Number(req.query.limit) || 200));
    res.json({ ok: true, result: listUnmapped({ limit }) });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ─── API: 価格帯 ───
router.get('/api/price-bands', (req, res) => {
  try {
    res.json({ ok: true, result: listPriceBands() });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ─── API: NE商品コード lookup (オートコンプリート用) ───
router.get('/api/lookup', (req, res) => {
  try {
    const code = String(req.query.code || '').trim();
    if (!code) return res.status(400).json({ ok: false, error: 'code is required' });
    const product = lookupNeProduct(code);
    if (!product) return res.status(404).json({ ok: false, error: 'not found' });
    res.json({ ok: true, result: product });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ─── API: 手動分類 (UNMAPPED → CONFIRMED) ───
router.post('/api/mapping', (req, res) => {
  try {
    const { rakuten_shop_code, rakuten_item_code, ne_product_code, reason } = req.body || {};
    if (!rakuten_shop_code || !rakuten_item_code || !ne_product_code || !reason) {
      return res.status(400).json({ ok: false, error: 'rakuten_shop_code, rakuten_item_code, ne_product_code, reason are required' });
    }
    const r = setManualMapping({
      rakuten_shop_code, rakuten_item_code, ne_product_code, reason,
      user: currentUser(req),
    });
    res.json({ ok: true, result: r });
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message });
  }
});

// ─── API: 手動 m_product_classifications (MANUAL ソース) ───
router.post('/api/classification', (req, res) => {
  try {
    const { ne_product_code, main_genre_id, main_genre_name, price_band_code_master, reason } = req.body || {};
    if (!ne_product_code || !reason) {
      return res.status(400).json({ ok: false, error: 'ne_product_code and reason are required' });
    }
    const r = setManualClassification({
      ne_product_code, main_genre_id, main_genre_name, price_band_code_master, reason,
      user: currentUser(req),
    });
    res.json({ ok: true, result: r });
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message });
  }
});

// ─── A-6 ダッシュボード API ───

router.get('/api/dashboard/kpi-summary', (req, res) => {
  try {
    const w = Number(req.query.window) || 90;
    res.json({ ok: true, result: getKpiSummary(w) });
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message });
  }
});

router.get('/api/dashboard/monthly-trend', (req, res) => {
  try {
    res.json({ ok: true, result: getMonthlyTrend() });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

router.get('/api/dashboard/sku-ranking', (req, res) => {
  try {
    const filters = {
      window: req.query.window ? Number(req.query.window) : 90,
      genre: req.query.genre || null,
      price_band: req.query.price_band || null,
      season_code: req.query.season_code || null,
      season_year: req.query.season_year ? Number(req.query.season_year) : null,
      sort: req.query.sort || 'sales',
      limit: req.query.limit ? Number(req.query.limit) : 100,
    };
    res.json({ ok: true, result: getSkuRanking(filters) });
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message });
  }
});

router.get('/api/dashboard/price-band-summary', (req, res) => {
  try {
    const w = Number(req.query.window) || 90;
    res.json({ ok: true, result: getPriceBandSummaryByWindow(w) });
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message });
  }
});

router.get('/api/dashboard/genres', (req, res) => {
  try {
    res.json({ ok: true, result: listAvailableGenres() });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

router.get('/api/dashboard/seasons', (req, res) => {
  try {
    res.json({ ok: true, result: listAvailableSeasons() });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

export default router;
