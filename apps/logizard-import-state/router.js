/**
 * logizard-import-state/router.js — ロジザードの毎日の商品マスタの取込の「状態」の口 (マスタ正本切替 ③c-1b-1)
 *
 * Render だけ (server.js の JOBS_MONITOR_ENABLED の中で mount = miniPC は同じ server.js でも口を立てない = 状態が 2 つにならない)。
 * 認証 = Bearer LZ_LOCK_TOKEN (無ければ 503 = 閉じる)。呼ぶのは miniPC の取込 (自動の ③) と Stream Deck の PC の auto-barcode.js (手の ③) と人の CLI。
 *
 *   GET  /apps/logizard-import-state/api/status
 *   POST /apps/logizard-import-state/api/init        { by, note }
 *   POST /apps/logizard-import-state/api/recover     { by, note }
 *   POST /apps/logizard-import-state/api/lock/acquire { init_id, holder, purpose, run_id, ttl_sec, by }
 *   POST /apps/logizard-import-state/api/lock/extend  { lock_token, ttl_sec }
 *   POST /apps/logizard-import-state/api/lock/release { lock_token, by }
 *   POST /apps/logizard-import-state/api/transition   { lock_token, run_id, to, detail, by }
 *   POST /apps/logizard-import-state/api/mark-unknown { run_id, by, reason }
 *   POST /apps/logizard-import-state/api/resolve      { run_id, outcome, note, by, partial_check, repaired }
 *   POST /apps/logizard-import-state/api/halt         { by, reason }
 *   POST /apps/logizard-import-state/api/resume       { by, note }
 *   POST /apps/logizard-import-state/api/notified     { run_id, by }
 * 断る = 409 (状態・鍵) / 400 (形) / 404 (まだ初期化していない)。{ error: code, message }
 */
import { Router } from 'express';
import express from 'express';
import crypto from 'crypto';
import * as S from './store.js';

function timingSafeEq(a, b) {
  const ab = Buffer.from(String(a)), bb = Buffer.from(String(b));
  if (ab.length !== bb.length) return false;
  return crypto.timingSafeEqual(ab, bb);
}

/**
 * @param {object} [opts]
 * @param {() => import('better-sqlite3').Database} [opts.getDb]  試験で差し替える
 * @param {() => number} [opts.now]
 */
export function createImportStateRouter({ getDb = null, now = () => Date.now(), token = () => process.env.LZ_LOCK_TOKEN } = {}) {
  let db = null;
  const dbOf = () => (getDb ? getDb() : (db ||= S.openImportStateDb()));
  const router = Router();
  router.use((req, res, next) => {
    const t = token();
    if (!t) return res.status(503).json({ error: 'not_configured', message: 'LZ_LOCK_TOKEN 未設定' });
    const got = (req.headers.authorization || '').replace(/^Bearer\s+/i, '');
    if (!got || !timingSafeEq(got, t)) return res.status(401).json({ error: 'unauthorized' });
    return next();
  });
  router.use(express.json({ limit: '64kb' }));
  // 本文が大きすぎる・JSON でない = 短い JSON で返す (スタックを出さない)
  router.use((err, req, res, next) => {
    if (!err) return next();
    return res.status(err.status === 413 ? 413 : 400).json({ ok: false, error: err.status === 413 ? 'too_large' : 'bad_json', message: String(err.message).slice(0, 120) });
  });
  const handle = (fn) => (req, res) => {
    try {
      res.json({ ok: true, ...fn(req.body || {}, req) });
    } catch (e) {
      if (e instanceof S.ImportStateError) return res.status(e.status).json({ ok: false, error: e.code, message: e.message });
      console.error('[logizard-import-state]', e);
      return res.status(500).json({ ok: false, error: 'internal', message: String(e && e.message).slice(0, 200) });
    }
  };
  router.get('/api/status', handle((_b, req) => S.getStatus(dbOf(), { now: now(), events: Number(req.query.events) || 20 })));
  router.post('/api/init', handle((b) => S.init(dbOf(), { by: b.by, note: b.note, now: now() })));
  router.post('/api/recover', handle((b) => S.recover(dbOf(), { by: b.by, note: b.note, now: now() })));
  router.post('/api/lock/acquire', handle((b) => S.acquire(dbOf(), { initId: b.init_id, holder: b.holder, purpose: b.purpose, runId: b.run_id, ttlSec: b.ttl_sec, by: b.by, now: now() })));
  router.post('/api/lock/extend', handle((b) => S.extend(dbOf(), { lockToken: b.lock_token, ttlSec: b.ttl_sec, now: now() })));
  router.post('/api/lock/release', handle((b) => S.release(dbOf(), { lockToken: b.lock_token, by: b.by, now: now() })));
  router.post('/api/transition', handle((b) => S.transition(dbOf(), { lockToken: b.lock_token, runId: b.run_id, to: b.to, detail: b.detail ?? null, by: b.by, now: now() })));
  router.post('/api/mark-unknown', handle((b) => S.markUnknown(dbOf(), { runId: b.run_id, by: b.by, reason: b.reason ?? null, now: now() })));
  router.post('/api/resolve', handle((b) => S.resolve(dbOf(), { runId: b.run_id, outcome: b.outcome, note: b.note, by: b.by, partialCheck: b.partial_check ?? null, repaired: b.repaired === true, now: now() })));
  router.post('/api/halt', handle((b) => S.halt(dbOf(), { by: b.by, reason: b.reason, now: now() })));
  router.post('/api/resume', handle((b) => S.resume(dbOf(), { by: b.by, note: b.note, now: now() })));
  router.post('/api/notified', handle((b) => S.markNotified(dbOf(), { runId: b.run_id, by: b.by, now: now() })));
  return router;
}

export default createImportStateRouter();
