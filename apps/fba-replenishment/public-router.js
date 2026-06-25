/**
 * FBA納品ピッキング準備 — 公開印刷ルーター (ログイン不要・固定URL)
 *
 * 子会社がポータルにログインせずプラン別シートを印刷できるよう、固定URLで公開する。
 * - GET /print/picking        : 最新実行のプラン別シート + 過去実行の一覧 (固定の入口URL)
 * - GET /print/picking/:id     : 指定した過去実行のプラン別シート
 * requireAppAccess/requireAuth の外側 (server.js で /print にマウント) で動く。
 * 中身(プラン別シート: 商品名/FNSKU/数量)は重要情報でなく誰が見ても問題ない方針 (中原さん)。
 */
import express from 'express';
import { getPickingRun, getPickingRuns } from './db.js';

const router = express.Router();

function parseJson(s, fallback) {
  try { return s ? JSON.parse(s) : fallback; } catch { return fallback; }
}

// 過去実行の一覧 (印刷ビューのナビ用、新しい順)
function buildRunsNav() {
  return getPickingRuns(50).map(r => ({
    id: r.id,
    run_at: r.run_at,
    plan_files: parseJson(r.plan_files, []),
  }));
}

function renderRun(res, rec, runs) {
  const result = parseJson(rec.result, {});
  res.render('fba-picking-print', {
    title: 'FBA納品 プラン別シート（公開）',
    runId: rec.id,
    runAt: rec.run_at,
    planSheets: result.planSheets || [],
    isPublic: true,
    runs,
    currentRunId: rec.id,
  });
}

// 固定の入口URL: 最新実行を表示 + 過去一覧
router.get('/picking', (req, res) => {
  let runs;
  try { runs = buildRunsNav(); } catch (e) { return res.status(503).send('準備中です。しばらくしてから再度お試しください。'); }
  if (!runs.length) {
    return res.render('fba-picking-print', {
      title: 'FBA納品 プラン別シート（公開）',
      runId: null, runAt: '', planSheets: [], isPublic: true, runs: [], currentRunId: null,
    });
  }
  const latest = getPickingRun(runs[0].id);
  if (!latest) return res.status(404).send('実行履歴が見つかりません。');
  renderRun(res, latest, runs);
});

// 過去の指定実行
router.get('/picking/:id', (req, res) => {
  if (!/^\d+$/.test(req.params.id)) return res.status(404).send('無効なURLです。');
  const id = Number(req.params.id);
  let rec, runs;
  try {
    rec = getPickingRun(id);
    runs = buildRunsNav();
  } catch (e) {
    return res.status(503).send('準備中です。しばらくしてから再度お試しください。');
  }
  if (!rec) return res.status(404).send('該当の回が見つかりません。');
  renderRun(res, rec, runs);
});

export default router;
