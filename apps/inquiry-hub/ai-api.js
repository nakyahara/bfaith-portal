/**
 * AI返信案 連携API (設計書§9.1) — ローカルClaude Codeランナー専用
 *
 * 認証: X-AI-Key ヘッダ = env INQUIRY_HUB_AI_KEY (timing-safe比較・未設定はfail-closed 503)。
 * 権限分離 (§9.2): このルーターは queue/claim/result/fail/run-log のみ。
 * 送信系・設定系・画面系エンドポイントは一切持たない (ポータルセッション認証の外に別mount)。
 *
 * 契約:
 *   GET  /queue                     → { jobs: [{id, inquiryId, channel, subject, aiFlag}] } (sweep込み)
 *   POST /claim  { job_ids }        → { jobs: [payload+leaseToken], qa: [...] }
 *   POST /result { job_id, lease_token, input_rev, summary?, category?, draft_body, notes?, confirmation_items?, model_info? }
 *                                   → { outcome: done|discarded|rejected|not_found, reason? }
 *   POST /fail   { job_id, lease_token, error }
 *   POST /run-log { runner_info, started_at, finished_at, claimed, done, failed, discarded, error? }
 */
import crypto from 'crypto';
import { Router } from 'express';
import { listAiQueue, claimAiJobs, submitAiResult, failAiJob, logAiRun } from './ai-jobs.js';

const router = Router();

router.use((req, res, next) => {
  const key = process.env.INQUIRY_HUB_AI_KEY;
  if (!key) return res.status(503).json({ error: 'INQUIRY_HUB_AI_KEY が未設定です (fail-closed)' });
  const provided = String(req.headers['x-ai-key'] || '');
  const a = Buffer.from(provided), b = Buffer.from(key);
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) {
    return res.status(401).json({ error: 'AIキーが不正です' });
  }
  next();
});

router.get('/queue', (req, res) => {
  try {
    const jobs = listAiQueue().map(j => ({
      id: j.id, inquiryId: j.inquiry_id, channel: j.channel_type,
      subject: String(j.subject || '').slice(0, 100), aiFlag: j.ai_needed, createdAt: j.created_at,
    }));
    res.json({ jobs });
  } catch (e) { res.status(500).json({ error: String(e?.message || e).slice(0, 300) }); }
});

router.post('/claim', (req, res) => {
  try {
    const jobIds = (req.body || {}).job_ids;
    res.json(claimAiJobs(jobIds));
  } catch (e) { res.status(400).json({ error: String(e?.message || e).slice(0, 300) }); }
});

router.post('/result', (req, res) => {
  const b = req.body || {};
  try {
    const r = submitAiResult({
      jobId: b.job_id, leaseToken: b.lease_token, inputRev: b.input_rev,
      summary: b.summary, category: b.category, draftBody: b.draft_body,
      notes: b.notes, confirmationItems: b.confirmation_items, modelInfo: b.model_info,
    });
    res.status(r.outcome === 'not_found' ? 404 : 200).json(r);
  } catch (e) { res.status(400).json({ error: String(e?.message || e).slice(0, 300) }); }
});

router.post('/fail', (req, res) => {
  const b = req.body || {};
  const ok = failAiJob({ jobId: b.job_id, leaseToken: b.lease_token, error: b.error });
  res.status(ok ? 200 : 404).json({ ok });
});

router.post('/run-log', (req, res) => {
  const b = req.body || {};
  logAiRun({ runnerInfo: b.runner_info, startedAt: b.started_at, finishedAt: b.finished_at,
    claimed: b.claimed, done: b.done, failed: b.failed, discarded: b.discarded, error: b.error });
  res.json({ ok: true });
});

export default router;
