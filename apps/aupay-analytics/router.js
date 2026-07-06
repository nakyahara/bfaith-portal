/**
 * aupay-analytics router — auPAY分析ツール (売上・利益・キャンペーンの統合管理)
 *
 * 要件定義: g:/共有ドライブ/AI_reference/システム設計/auPAY統合管理ダッシュボード_要件定義_20260706.md
 *
 * 設計原則 (§11):
 *   - Render 完結: warehouse-mirror.db 読み取りのみ (集計 SQL は queries.js に分離)
 *   - 精度ラベル: 速報 (NE受注) / 自動(日次) (完了ベースfact) / 実額(API) (付与pt) / 推定13% (手数料) を API が明示
 *   - API は /api/v1/ prefix (将来拡張用)
 *   - 認可は server.js 側 requireAppAccess('aupay-analytics') (§13-⑥: 既存 allowedApps 運用)
 *   - 既存アプリ (aupay-accounting = 会計用途) とは独立。mirror の読み取り参照のみ
 *
 * P1 スコープ: アプリ骨格 + 概要タブ (タイル4枚 + トレンド + 三太郎の日ハイライト)。
 * 売れ筋・利益分析 (P2) / デバイス・決済 (P3) / 取込・設定・月次確定 (P4) /
 * キャンペーン・診断 (P5) は準備中タブ。広告はプラチナマッチ未出稿のため将来枠 (§13-②)。
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
      console.error(`[aupay-analytics] ${req.method} ${req.originalUrl}: ${e.stack || e.message}`);
      res.status(500).json({ error: 'サーバー内部エラー (詳細はサーバーログ参照)' });
    }
  };
}

router.get('/', (req, res) => {
  res.render('aupay-analytics', {
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
