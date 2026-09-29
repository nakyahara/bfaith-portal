/**
 * logizard-import-state/router.js — ロジザードの毎日の商品マスタの取込の「状態」の口 (マスタ正本切替 ③c-1b-1)
 *
 * Render だけ (server.js の JOBS_MONITOR_ENABLED の中で mount = miniPC は同じ server.js でも口を立てない = 状態が 2 つにならない)。
 * **どの body parser よりも前に mount** する (method・Content-Type によらず、認証の前に本文を読まない。Codex #1513 R1 Medium)。
 * 認証 = Bearer LZ_LOCK_TOKEN (無ければ 503 = 閉じる)。呼ぶのは miniPC の取込 (自動の ③・少数件の試験・毎晩の成果物の送り) と人の CLI (機械の口)。
 * 手の取込・設定・waiver はここに出さない (ログインして使う画面の口 = ③c-1b-3b-4)。
 *
 *   GET  /apps/logizard-import-state/api/status
 *   POST /apps/logizard-import-state/api/init        { by, note }
 *   POST /apps/logizard-import-state/api/recover     { by, note }
 *   POST /apps/logizard-import-state/api/lock/acquire { init_id, holder, purpose, run_id, ttl_sec, by }
 *   POST /apps/logizard-import-state/api/lock/extend  { lock_token, ttl_sec }
 *   POST /apps/logizard-import-state/api/lock/release { lock_token, by }
 *   POST /apps/logizard-import-state/api/transition   { lock_token, run_id, to, detail, by }   (to = importing / imported_unverified / partial / unknown / failed_before_execute / verified / verify_failed)
 *   POST /apps/logizard-import-state/api/mark-unknown { run_id, by, reason }
 *   POST /apps/logizard-import-state/api/resolve      { run_id, outcome, note, by, partial_check, repaired }
 *   POST /apps/logizard-import-state/api/halt         { by, reason }
 *   POST /apps/logizard-import-state/api/resume       { by, note, expected_halt_revision }   (見ていた止めの番号 = status の halt_revision。違う = 409 stale)
 *   POST /apps/logizard-import-state/api/notified     { run_id, state, state_event_id, by }   (知らせたのが今の状態のときだけ。③c-1b-2b K9)
 *   ③c-1b-3b-2b (契約 v4 + 設計 R1 K3-1・K3-4):
 *   POST /apps/logizard-import-state/api/artifacts?source_run_id&target_as_of&verdict&sha256&rows&by   本文 = 毎晩の成果物の CSV のバイト列
 *        (application/octet-stream・4MB まで)。ポータルが中身から sha256・行数・形を計算し直す (申告と違う = 400 mismatch・同じ ID の違う中身 = 409 conflict)
 *   GET  /apps/logizard-import-state/api/artifacts[?limit]   成果物の識別の一覧 (中身は返さない)
 *   GET  /apps/logizard-import-state/api/artifacts/:source_run_id   1 つの識別 (無い = 404)
 *   GET  /apps/logizard-import-state/api/outbox[?limit]       まだ送れていない知らせ (halt・残った再適用待ち・確認待ち)
 *   POST /apps/logizard-import-state/api/outbox/sent          { id, by }   送れた (1 回だけ・もう送れた = already)
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
  // Bearer は /api だけ (同じ前置きの画面の口 /admin-api は、ここを素通りしてセッションの後の admin-router.js へ。③c-1b-3b-4a・K3-7)
  router.use('/api', (req, res, next) => {
    const t = token();
    if (!t) return res.status(503).json({ error: 'not_configured', message: 'LZ_LOCK_TOKEN 未設定' });
    const got = (req.headers.authorization || '').replace(/^Bearer\s+/i, '');
    if (!got || !timingSafeEq(got, t)) return res.status(401).json({ error: 'unauthorized' });
    return next();
  });
  router.use('/api', express.json({ limit: '64kb' }));
  // 本文が大きすぎる・JSON でない = 短い JSON で返す (スタックを出さない)
  // 決まった文言だけ返す (本文の断片を返さない。Codex #1513 R1 Low)
  const bodyError = (err, req, res, next) => {
    if (!err) return next();
    return res.status(err.status === 413 ? 413 : 400).json({ ok: false, error: err.status === 413 ? 'too_large' : 'bad_json', message: err.status === 413 ? '本文が大きすぎる' : 'JSON として読めない' });
  };
  router.use('/api', bodyError);
  const handle = (fn) => (req, res) => {
    try {
      res.json({ ok: true, ...fn(req.body || {}, req) });
    } catch (e) {
      if (e instanceof S.ImportStateError) return res.status(e.status).json({ ok: false, error: e.code, message: e.message });
      console.error('[logizard-import-state]', e);   // 詳しいことはサーバーのログだけ (内部のパスなどを返さない。Codex #1513 R1 Low)
      return res.status(500).json({ ok: false, error: 'internal', message: 'ポータルの中で失敗した (ログを見る)' });
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
  router.post('/api/resume', handle((b) => S.resume(dbOf(), { by: b.by, note: b.note, expectedHaltRevision: b.expected_halt_revision, now: now() })));   // 見ていた止めの番号が要る (Codex #1542 R2 High)
  router.post('/api/notified', handle((b) => S.markNotified(dbOf(), { runId: b.run_id, state: b.state, stateEventId: b.state_event_id, by: b.by, now: now() })));
  // ── ③c-1b-3b-2b: 毎晩の成果物 (K3-1)・知らせの outbox (K3-4) ──
  // 成果物の本文は Bearer の後に、この口だけの parser (octet-stream・4MB) で読む
  const rawCsv = express.raw({ type: 'application/octet-stream', limit: S.LIMITS.csvBytes });
  const INT_RE = /^[0-9]{1,6}$/;
  // limit = 無い (既定) か、1 つの 10 進の正の整数で上限まで。ほか (小数・0・負・文字・2 つ) = 400 (Codex #1539 R1 Medium)
  const limitOf = (req, dflt, max) => {
    const v = req.query ? req.query.limit : undefined;
    if (v === undefined) return dflt;
    if (typeof v !== 'string' || !/^[1-9][0-9]{0,2}$/.test(v) || Number(v) > max) throw new S.ImportStateError('bad_request', `limit は 1〜${max} の整数`, 400);
    return Number(v);
  };
  router.post('/api/artifacts', rawCsv, handle((body, req) => {
    if (!req.is('application/octet-stream') || !Buffer.isBuffer(body) || !body.length) throw new S.ImportStateError('bad_request', '本文は成果物の CSV のバイト列 (application/octet-stream)', 400);
    const q = req.query || {};
    const one = (k) => (typeof q[k] === 'string' ? q[k] : undefined);   // 同じ名前が 2 つ (配列) = 無い扱い
    if (!INT_RE.test(one('rows') || '')) throw new S.ImportStateError('bad_request', 'rows (行数) が要る', 400);
    return S.putArtifact(dbOf(), { sourceRunId: one('source_run_id'), targetAsOf: one('target_as_of'), verdict: one('verdict'), csvBuf: body, sha256: one('sha256'), rows: Number(one('rows')), by: one('by'), now: now() });
  }));
  router.get('/api/artifacts', handle((_b, req) => ({ artifacts: S.listArtifacts(dbOf(), { limit: limitOf(req, 14, 60) }) })));
  router.get('/api/artifacts/:id', handle((_b, req) => {
    const a = S.getArtifact(dbOf(), { sourceRunId: req.params.id });
    if (!a) throw new S.ImportStateError('not_found', 'その成果物は無い', 404);
    return { artifact: a };
  }));
  router.get('/api/outbox', handle((_b, req) => ({ outbox: S.outboxPending(dbOf(), { limit: limitOf(req, 20, 100) }) })));
  router.post('/api/outbox/sent', handle((b) => S.outboxMarkSent(dbOf(), { id: b.id, by: b.by, now: now() })));
  router.use(bodyError);   // この口だけの parser の失敗 (大きすぎる) も同じ短い JSON で
  return router;
}

export default createImportStateRouter();
