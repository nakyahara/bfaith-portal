/**
 * apps/sales-analytics-linegift/router.js — v1.0 Render 完結 UI + API
 *
 * 設計書: g:/共有ドライブ/AI_reference/システム設計/LINEギフトPhase1設計書_v1.0_20260527.md §6 / §7
 *
 * URL:
 *   GET  /apps/sales-analytics-linegift/                          UI (ダッシュボード)
 *   GET  /apps/sales-analytics-linegift/api/dashboard/kpi-summary
 *   GET  /apps/sales-analytics-linegift/api/dashboard/monthly-trend
 *   GET  /apps/sales-analytics-linegift/api/dashboard/sku-ranking
 *   GET  /apps/sales-analytics-linegift/api/dashboard/price-band-summary
 *   GET  /apps/sales-analytics-linegift/api/dashboard/seasons
 *   GET  /apps/sales-analytics-linegift/api/build-status         build lock 状態
 *   POST /apps/sales-analytics-linegift/api/build-trigger        build スクリプト起動 (409 対応)
 *
 * 認証: server.js で requireAppAccess('sales-analytics-linegift') を mount 時に適用。
 */
import { Router } from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import { spawn } from 'node:child_process';
import {
  getKpiSummary, getKpiSummaryByPeriod, getKpiWithPrevious,
  getMonthlyTrend, getSkuRanking,
  getPriceBandSummary, getPriceBandSummaryByPeriod, listSeasons, listAvailableMonths,
  getBuildStatus, resolvePeriod,
  getDeadStockSkus, getNewItemRamps, getSeasonComparison,
  getTrend, getWeekdaySummary, getDowHourHeatmap, getSkuTrend,
} from './db.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = Router();

// ─── UI ───
router.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'views', 'dashboard.html'));
});

// ─── API ───
router.get('/api/dashboard/kpi-summary', (req, res) => {
  try {
    const period = resolvePeriod(req.query);
    if (period) {
      res.json({ ok: true, result: getKpiSummaryByPeriod(period.from, period.to) });
      return;
    }
    const w = Number(req.query.window) || 90;
    res.json({ ok: true, result: getKpiSummary(w) });
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message });
  }
});

// 前期比つき KPI (中原さん指摘 2026-05-28)
// クエリ: window=7/28/90 / month=YYYY-MM / from=&to= / season_code+season_year のいずれか
// season_code 指定時は「シーン期間 vs 前年同シーン期間」(PR-D Codex Round 2 High 修正)
router.get('/api/dashboard/kpi-comparison', (req, res) => {
  try {
    const spec = {
      window: req.query.window ? Number(req.query.window) : 90,
      month: req.query.month || null,
      from: req.query.from || null,
      to: req.query.to || null,
      season_code: req.query.season_code || null,
      season_year: req.query.season_year ? Number(req.query.season_year) : null,
    };
    res.json({ ok: true, result: getKpiWithPrevious(spec) });
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
    const period = resolvePeriod(req.query);
    // period (month/custom) と season_code の同時指定は経路が異なり共存できないため 400 で reject
    // (UI 側でも season は disable しているが API 直叩き対策、Codex Round 1 medium)
    if (period && req.query.season_code) {
      res.status(400).json({ ok: false, error: 'season_code と period (month/custom) は同時指定できません。シーン絞り込みは window モード専用です' });
      return;
    }
    const filters = {
      window: req.query.window ? Number(req.query.window) : 90,
      season_code: req.query.season_code || null,
      season_year: req.query.season_year ? Number(req.query.season_year) : null,
      sort: req.query.sort || 'sales',
      limit: req.query.limit ? Number(req.query.limit) : 500,
      from: period?.from || null,
      to: period?.to || null,
    };
    res.json({ ok: true, result: getSkuRanking(filters) });
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message });
  }
});

router.get('/api/dashboard/price-band-summary', (req, res) => {
  try {
    const period = resolvePeriod(req.query);
    if (period) {
      res.json({ ok: true, result: getPriceBandSummaryByPeriod(period.from, period.to) });
      return;
    }
    const w = Number(req.query.window) || 90;
    res.json({ ok: true, result: getPriceBandSummary(w) });
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message });
  }
});

router.get('/api/dashboard/available-months', (req, res) => {
  try {
    res.json({ ok: true, result: listAvailableMonths() });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

router.get('/api/dashboard/seasons', (req, res) => {
  try {
    res.json({ ok: true, result: listSeasons() });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// 死筋 SKU (撤退判断支援、PR-E)
router.get('/api/dashboard/dead-stock', (req, res) => {
  try {
    const spec = {
      window: req.query.window ? Number(req.query.window) : 90,
      month: req.query.month || null,
      from: req.query.from || null,
      to: req.query.to || null,
      season_code: req.query.season_code || null,
      season_year: req.query.season_year ? Number(req.query.season_year) : null,
    };
    res.json({ ok: true, result: getDeadStockSkus(spec) });
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message });
  }
});

// 新商品立ち上がり ramp (PR-E)
router.get('/api/dashboard/new-item-ramp', (req, res) => {
  try {
    const lookback = req.query.launch_lookback_days ? Number(req.query.launch_lookback_days) : 90;
    res.json({ ok: true, result: getNewItemRamps({ launch_lookback_days: lookback }) });
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message });
  }
});

// シーン × 前年同シーン SKU 比較 (PR-F)
router.get('/api/dashboard/season-comparison', (req, res) => {
  try {
    if (!req.query.season_code) {
      res.status(400).json({ ok: false, error: 'season_code is required' });
      return;
    }
    res.json({
      ok: true,
      result: getSeasonComparison(
        req.query.season_code,
        req.query.season_year ? Number(req.query.season_year) : null,
      ),
    });
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message });
  }
});

// 期間モード連動トレンド (PR-G ⑥): granularity=day/week/month (auto)
router.get('/api/dashboard/trend', (req, res) => {
  try {
    const spec = {
      window: req.query.window ? Number(req.query.window) : 90,
      month: req.query.month || null,
      from: req.query.from || null,
      to: req.query.to || null,
      season_code: req.query.season_code || null,
      season_year: req.query.season_year ? Number(req.query.season_year) : null,
      granularity: req.query.granularity || null,
    };
    res.json({ ok: true, result: getTrend(spec) });
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message });
  }
});

// 曜日別売上サマリ (PR-G ⑧、時間帯は v1.2 課題)
router.get('/api/dashboard/weekday-summary', (req, res) => {
  try {
    const spec = {
      window: req.query.window ? Number(req.query.window) : 90,
      month: req.query.month || null,
      from: req.query.from || null,
      to: req.query.to || null,
      season_code: req.query.season_code || null,
      season_year: req.query.season_year ? Number(req.query.season_year) : null,
    };
    res.json({ ok: true, result: getWeekdaySummary(spec) });
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message });
  }
});

// 商品別トレンド (PR-J、SKU 個別時系列)
router.get('/api/dashboard/sku-trend', (req, res) => {
  try {
    if (!req.query.sku_code) {
      res.status(400).json({ ok: false, error: 'sku_code is required' });
      return;
    }
    const spec = {
      window: req.query.window ? Number(req.query.window) : 90,
      month: req.query.month || null,
      from: req.query.from || null,
      to: req.query.to || null,
      season_code: req.query.season_code || null,
      season_year: req.query.season_year ? Number(req.query.season_year) : null,
      granularity: req.query.granularity || null,
    };
    res.json({ ok: true, result: getSkuTrend(String(req.query.sku_code), spec) });
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message });
  }
});

// 曜日 × 時間帯ヒートマップ (PR-H、v1.2)
router.get('/api/dashboard/dow-hour-heatmap', (req, res) => {
  try {
    const spec = {
      window: req.query.window ? Number(req.query.window) : 90,
      month: req.query.month || null,
      from: req.query.from || null,
      to: req.query.to || null,
      season_code: req.query.season_code || null,
      season_year: req.query.season_year ? Number(req.query.season_year) : null,
    };
    res.json({ ok: true, result: getDowHourHeatmap(spec) });
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message });
  }
});

router.get('/api/build-status', (req, res) => {
  try {
    res.json({ ok: true, result: getBuildStatus() });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// build トリガー (UI ボタン or miniPC daily-sync から HTTP 起動)
// 同時呼出は build スクリプト内の acquireLock で 409 ガード
router.post('/api/build-trigger', (req, res) => {
  try {
    const scriptPath = path.resolve(__dirname, '..', 'warehouse-mirror', 'build-linegift-analytics-mart.js');
    const child = spawn(process.execPath, [scriptPath], {
      detached: true,
      stdio: 'ignore',
      cwd: process.cwd(),
    });
    child.unref();
    res.json({ ok: true, result: { triggered: true, pid: child.pid, note: 'build スクリプトを起動しました。状態は GET /api/build-status で確認してください' } });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

export default router;
