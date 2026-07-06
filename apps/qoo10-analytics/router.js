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
 * P1 スコープ: アプリ骨格 + 概要タブ (タイル4枚 + トレンド)。速報は既存
 * mirror_f_sales_by_listing (mall='qoo10'、NE受注由来) を使用 = miniPC 変更なしで実現。
 * 利益分析・売れ筋・設定 (P3) / 取込・精算突合 (P4) / 広告 (P5) / メガ割 (P6) /
 * 診断 (P8) / 返品・出品 (P9) は準備中タブ。
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

export default router;
