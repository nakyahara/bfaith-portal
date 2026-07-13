/**
 * rakuten-analytics router — 楽天分析ツール (売上・広告・利益・検索順位の統合管理)
 *
 * 要件定義: g:/共有ドライブ/AI_reference/システム設計/楽天統合管理ダッシュボード_要件定義_20260706.md
 * P3 設計: 2026-07-07 Codex 2ラウンド意見交換 (アクションキュー+AI土台) — rules.js ヘッダ参照
 *
 * 設計原則:
 *   - Render 完結: warehouse-mirror.db 読み取り + radash_* (アプリ固有表) のみ
 *   - データは mall-csv-fetcher (miniPC) が毎朝自動取得 → warehouse.db → mirror 同期
 *   - API は /api/v1/ prefix。認可は server.js 側 requireAppAccess('rakuten-analytics')
 *
 * P3 スコープ: 🎯アクション (ルール診断+キュー+効果測定) / 📣広告 (損益分岐ROAS) /
 * 📈売上方程式 (逐次差分分解) / 📡データ (鮮度) / ⚙️設定 (診断閾値)
 */
import express, { Router } from 'express';
import {
  resolvePeriod, getOverview, getTrend,
  getAdsAnalysis, getEquation, getEquationMovers,
} from './queries.js';
import {
  ensureDailyRun, listActions, actOnAction, getActionDetail, getRunStatus,
  getDatasetFreshness, getThresholds, saveThresholds,
} from './rules.js';

const router = Router();
router.use(express.json({ limit: '256kb' }));

// 500 を JSON で返す共通 wrapper。エラー詳細はログのみ、クライアントには固定文言
function api(handler) {
  return (req, res) => {
    try {
      res.json(handler(req));
    } catch (e) {
      console.error(`[rakuten-analytics] ${req.method} ${req.originalUrl}: ${e.stack || e.message}`);
      res.status(500).json({ error: 'サーバー内部エラー (詳細はサーバーログ参照)' });
    }
  };
}

router.get('/', (req, res) => {
  res.render('rakuten-analytics', {
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

// ─── 🎯 アクション (診断ルール + キュー) ───
// 一覧取得時に当日 JST 未実行なら lazy 実行 (実行失敗しても前回結果は返す)
router.get('/api/v1/actions', api((req) => {
  let runError = null;
  if (req.query.ensure_run !== '0') {
    try { ensureDailyRun({ trigger: 'lazy' }); }
    catch (e) {
      runError = '本日の診断実行に失敗しました (前回結果を表示中)。データタブで鮮度を確認してください';
      console.error(`[rakuten-analytics] lazy rule run failed: ${e.stack || e.message}`);
    }
  }
  return {
    ...getRunStatus(),
    run_error: runError,
    items: listActions({ status: req.query.status || 'open,acked', limit: req.query.limit }),
  };
}));

router.post('/api/v1/actions/run', api((req) => {
  ensureDailyRun({ trigger: 'manual', force: true, actor: req.session?.email || '' });
  return getRunStatus();
}));

router.get('/api/v1/actions/:id', api((req) => {
  const d = getActionDetail(req.params.id);
  if (!d) throw new Error('action not found');
  return d;
}));

// 人の操作のみ (ack/done/dismiss/reopen)。将来の AI レーンは source='ai' の提案作成専用
// エンドポイントを別途設ける契約 (人の状態遷移を AI は変更できない — Codex R1)
router.post('/api/v1/actions/:id/act', api((req) => {
  const b = req.body || {};
  return actOnAction(req.params.id, {
    event: b.event,
    actor: req.session?.email || '',
    note: b.note,
    dismissScope: b.dismiss_scope,
    suppressUntil: b.suppress_until,
    implementedDate: b.implemented_date,
  });
}));

// ─── 📣 広告 (RPP × 利益 = 損益分岐ROAS) ───
router.get('/api/v1/ads', api((req) => getAdsAnalysis(req.query.month)));

// ─── 📈 売上方程式 ───
router.get('/api/v1/equation', api((req) => getEquation({
  level: req.query.level === 'sku' ? 'sku' : 'store',
  sku: (req.query.sku || '').trim() || null,
  windowDays: req.query.window,
})));
router.get('/api/v1/equation/movers', api(() => getEquationMovers({})));

// ─── 📡 データ鮮度 ───
router.get('/api/v1/freshness', api(() => ({ datasets: getDatasetFreshness() })));

// ─── ⚙️ 設定 (診断閾値) ───
router.get('/api/v1/settings', api(() => getThresholds()));
router.put('/api/v1/settings', api((req) => saveThresholds(req.body || {})));

export default router;
