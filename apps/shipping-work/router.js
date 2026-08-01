/**
 * 出荷作業管理アプリ router (bfaith-portal)
 *
 * URL: /apps/shipping-work/ (カンバン) — PR1 は表示のみ。
 *      作業API (リース/開始/完了/保留/印刷トラブル) は PR3、管理者画面は PR2/PR5。
 *
 * 認証: server.js で requireAppAccess('shipping-work') を mount 時に適用。
 *       管理者専用操作は router 内で req.session.role === 'admin' を check (PR2〜)。
 *
 * 設計書: AI_reference/システム設計/出荷作業管理アプリ_要件定義_20260801.md (v2)
 *         AI_reference/システム設計/出荷作業管理アプリ_実装計画_20260801.md
 */
import { Router } from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import { getDB, initShippingWorkDB, jstToday, listKanbanBatches, BATCH_STATUSES, STATUS_LABELS } from './db.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = Router();

// import 時 (= server.js boot 時) に DB を初期化する。
// migration 失敗時はここで throw し、旧スキーマのまま起動を継続させない (Codex PR1レビュー#3)
initShippingWorkDB();

// カンバンの列構成 (表示順)。hold/stock_return は横断列として末尾に置く。
// cancelled はカンバンに出さない (管理画面のみ・PR5)。
const KANBAN_COLUMNS = [
  'ready', 'picking', 'picked', 'sorting', 'sorted', 'packing', 'done', 'hold', 'stock_return',
];

// ─── カンバン (全員・表示専用。ドラッグ機能は作らない) ───
router.get('/', (req, res) => {
  const workDate = /^\d{4}-\d{2}-\d{2}$/.test(String(req.query.date || ''))
    ? String(req.query.date)
    : jstToday();
  const batches = listKanbanBatches(workDate);

  const columns = KANBAN_COLUMNS.map((status) => ({
    status,
    label: STATUS_LABELS[status],
    batches: batches.filter((b) => b.status === status),
  }));

  res.render(path.join(__dirname, 'views/kanban'), {
    title: '出荷作業管理',
    username: req.session.username,
    displayName: req.session.displayName,
    isAdmin: req.session.role === 'admin',
    workDate,
    columns,
    totalCount: batches.length,
  });
});

// ─── ヘルスチェック (デプロイ確認用) ───
router.get('/api/health', (req, res) => {
  const masters = getDB().prepare('SELECT COUNT(*) AS c FROM sw_masters').get();
  res.json({ ok: true, masters: masters.c, statuses: BATCH_STATUSES.length });
});

export default router;
