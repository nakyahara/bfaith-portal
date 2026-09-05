/**
 * jobs-monitor 受付API
 *
 *   POST /apps/jobs-monitor/ping/:id?status=ok|fail|start|partial[&note=...]
 *        — ジョブからの報告 (Bearer認証)。partial = 「動いたが終わっていない」
 *          (長時間バッチが実行時間の上限で中断したとき。生存は進むが完了は進まない)
 *   GET  /apps/jobs-monitor/health                          — 監視自身の生存確認 (認証なし・情報最小)
 *   GET  /apps/jobs-monitor/status                          — 全評価のJSON (Bearer認証)
 *
 * health を認証なしにする理由: 外部の見張り (GAS/UptimeRobot) が叩くため。
 * 返すのは ok/評価時刻だけで、ジョブ名や業務情報は一切含めない。
 */
import { Router } from 'express';
import crypto from 'crypto';
import { JOBS_REGISTRY, RETIRED_JOBS } from '../../config/jobs-registry.mjs';
import { recordPing, getStates, getMeta } from './store.js';
import { evaluateAll } from './evaluate.js';

const router = Router();

const ID_RE = /^[a-z0-9][a-z0-9-]{1,63}$/;
const REGISTERED = new Set(JOBS_REGISTRY.map((e) => e.id));
// 退役した id (台帳から外したもの)。ping が来ても「台帳に無い id」としては扱わない (毎朝鳴らさない)
const RETIRED = new Set(RETIRED_JOBS.map((e) => e.id));
// 評価ループ (5分間隔) がこの時間止まっていたら health を 503 にする
const EVAL_STALE_MS = 15 * 60 * 1000;

function timingSafeEq(a, b) {
  const ab = Buffer.from(String(a));
  const bb = Buffer.from(String(b));
  if (ab.length !== bb.length) return false;
  return crypto.timingSafeEqual(ab, bb);
}

function requireToken(req, res, next) {
  const token = process.env.JOBS_MONITOR_TOKEN;
  if (!token) return res.status(503).json({ error: 'JOBS_MONITOR_TOKEN 未設定' });
  const got = (req.headers.authorization || '').replace(/^Bearer\s+/i, '');
  if (!got || !timingSafeEq(got, token)) return res.status(401).json({ error: 'unauthorized' });
  return next();
}

// ── 生存確認 (認証なし・情報最小) ──
// ⭐「判定が動いているか」だけでなく「通知が届く状態か」まで見る (Codex R1 critical の反映)。
// 判定できても誰にも伝わらなければ、監視としては死んでいるのと同じ。
//   unhealthy 条件:
//     - 評価ループが15分止まっている
//     - GCHAT_WEBHOOK_JOBS 未設定
//     - 最後の通知試行が失敗のまま (成功で上書きされるまで unhealthy)
//     - 毎朝のサマリが26時間出ていない (稼働26時間未満の導入直後は除く)
//     - 台帳 (jobs-registry) がバリデーションエラー
router.get('/health', (req, res) => {
  const now = Date.now();
  const n = (k) => Number(getMeta(k) || 0);
  const problems = [];
  const lastEval = n('last_eval_at');
  if (!(lastEval > 0 && now - lastEval < EVAL_STALE_MS)) problems.push('eval_stale');
  if (!process.env.GCHAT_WEBHOOK_JOBS) problems.push('webhook_missing');
  const notifyOk = n('last_notify_ok_at');
  const notifyFail = n('last_notify_fail_at');
  if (notifyFail > 0 && notifyFail > notifyOk) problems.push('notify_failing');
  const since = n('monitoring_since');
  const digestOk = n('last_digest_ok_at');
  const DIGEST_STALE_MS = 26 * 3600 * 1000;
  if (since > 0 && now - since > DIGEST_STALE_MS && (!digestOk || now - digestOk > DIGEST_STALE_MS)) {
    problems.push('digest_stale');
  }
  if (getMeta('registry_error')) problems.push('registry_error');

  const ok = problems.length === 0;
  res.status(ok ? 200 : 503).json({
    ok,
    problems,
    lastEvalAt: lastEval ? new Date(lastEval).toISOString() : null,
  });
});

// ── ping 受付 ──
router.post('/ping/:id', requireToken, (req, res) => {
  const id = String(req.params.id || '');
  if (!ID_RE.test(id)) return res.status(400).json({ error: 'id が不正' });
  const status = String(req.query.status || 'ok');
  if (!['ok', 'fail', 'start', 'partial'].includes(status)) {
    return res.status(400).json({ error: 'status は ok|fail|start|partial' });
  }
  const note = typeof req.query.note === 'string' ? req.query.note.slice(0, 200) : null;
  recordPing(id, status, note, Date.now());
  // 台帳に無い id も 200 で受ける (拒否するとジョブ側が失敗扱いになる)。
  // 未登録は毎朝の要対応サマリが「登録するか止めるか」を要求する。
  res.json({ ok: true, registered: REGISTERED.has(id) });
});

// ── 全評価 (デバッグ・将来の一覧画面用) ──
router.get('/status', requireToken, (req, res) => {
  const states = getStates();
  const results = evaluateAll(JOBS_REGISTRY, states, Date.now());
  const unknown = Object.keys(states).filter((id) => !REGISTERED.has(id) && !RETIRED.has(id));
  res.json({ evaluatedAt: new Date().toISOString(), results, unknownIds: unknown, retiredIds: RETIRED_JOBS.map((e) => e.id) });
});

export default router;
