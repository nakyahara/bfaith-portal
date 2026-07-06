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
import { resolvePeriod, getOverview, getTrend } from './queries.js';

const router = Router();

// 500 を JSON で返す共通 wrapper (API は画面から fetch されるため HTML error page を返さない)
// エラー詳細 (SQL/テーブル名等) はログのみ。クライアントには固定文言
function api(handler) {
  return (req, res) => {
    try {
      res.json(handler(req));
    } catch (e) {
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

export default router;
