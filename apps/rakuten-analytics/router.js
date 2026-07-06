/**
 * rakuten-analytics router — 楽天分析ツール (売上・広告・利益・検索順位の統合管理)
 *
 * 要件定義: g:/共有ドライブ/AI_reference/システム設計/楽天統合管理ダッシュボード_要件定義_20260706.md
 *
 * 設計原則 (§9):
 *   - Render 完結: warehouse-mirror.db 読み取りのみ (集計 SQL は queries.js に分離)
 *   - 精度ラベル: 速報 (受注ベース) / 推定 (mall_fee 10%一律) / 確定 (月次仕訳書) を API が明示
 *   - API は /api/v1/ prefix (将来拡張用)
 *   - 認可は server.js 側 requireAppAccess('rakuten-analytics')
 *   - 既存アプリ (rakuten-accounting 等) とは独立。mirror の読み取り参照のみ (§9-4)
 *
 * P1 スコープ: アプリ骨格 + 概要タブ (タイル4枚 + トレンド)。
 * 取込 (P2) / 広告 (P3) / 利益分析 (P4) / 売れ筋 (P5) / 検索順位 (P6) / 診断・設定 (P7) は準備中タブ。
 */
import { Router } from 'express';
import { resolvePeriod, getOverview, getTrend } from './queries.js';

const router = Router();

// 500 を JSON で返す共通 wrapper (API は画面から fetch されるため HTML error page を返さない)
// エラー詳細 (SQL/テーブル名等) はログのみ。クライアントには固定文言 (Codex R1 Medium)
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

export default router;
