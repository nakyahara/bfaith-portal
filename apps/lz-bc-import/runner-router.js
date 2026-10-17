/**
 * lz-bc-import/runner-router.js — 「ロジザードに取り込む」の機械の口 (miniPC の bc-import-runner.js が取りに来る・Render だけ)
 *
 * server.js で **どの body parser よりも前に** mount する (Bearer LZ_BC_RUNNER_TOKEN の認証の前に本文を読まない = logizard-import-state と同じ)。
 * token が無い = 503 (閉じる)。人の画面の口は apps/master-edit/router.mjs (/api/lz-csv/...・名簿の人だけ・Origin の守り)。
 *
 *   POST /apps/lz-bc-import/api/claim              { runner_id, version, busy }  → { job | null, why, claim_token, file_b64 }
 *        来た印 (画面の「miniPC: 来ている」)。busy = miniPC がほかのロジザードの作業中・夜 など (渡さない)。
 *        渡す前に Company DB の帳面のファイルを読み直す: 取り消していない・sha256 (byte 列と文字) が積んだときと同じ (違う = その回は押さずに終わり)
 *   POST /apps/lz-bc-import/api/jobs/:id/executing { claim_token }               ① の実行ボタンを押す直前 (断る = 押さない)
 *   POST /apps/lz-bc-import/api/jobs/:id/import    { claim_token, import_result, log }   ① の結果 (途中の知らせ)
 *   POST /apps/lz-bc-import/api/jobs/:id/final     { claim_token, executed, import_result, export_result, reason, detail, log }  終わり (送り直してよい)
 * 断る = 409 (状態) / 403 (鍵) / 400 (形) / 404。{ ok: false, error: code, message }
 */
import express, { Router } from 'express';
import crypto from 'node:crypto';
import * as S from './store.js';
import { importStateBlock } from './lz-state.js';

function timingSafeEq(a, b) {
  const ab = Buffer.from(String(a)), bb = Buffer.from(String(b));
  if (ab.length !== bb.length) return false;
  return crypto.timingSafeEqual(ab, bb);
}
const ID_RE = /^[1-9]\d{0,12}$/;
const int = (v) => (Number.isSafeInteger(v) && v >= 0 ? v : null);
const str = (v, max) => (typeof v === 'string' ? v.slice(0, max) : null);
/** miniPC の申告の形をそろえる (知らない形 = 分からない) */
function importResultOf(v) {
  if (!v || typeof v !== 'object') return null;
  const status = ['ok', 'error', 'unknown'].includes(v.status) ? v.status : 'unknown';
  const c = v.counts && typeof v.counts === 'object' ? v.counts : {};
  return { status, summary: str(v.summary, 300), counts: { total: int(c.total), processed: int(c.processed), unneeded: int(c.unneeded), errors: int(c.errors) } };
}
function exportResultOf(v) {
  if (!v || typeof v !== 'object' || !['ok', 'failed', 'skipped'].includes(v.status)) return null;
  return { status: v.status, rows: int(v.rows), detail: str(v.detail, 300) };
}

/**
 * @param {object} o
 * @param {(exportId: string) => Promise<null | { state, file_name, file_bytes: Buffer, file_sha256, text_sha256 }>} o.readExportFile  Company DB の帳面のファイル
 * @param {() => import('better-sqlite3').Database} [o.getDb]
 * @param {() => number} [o.now]
 * @param {() => string | undefined} [o.token]
 * @param {(now: number) => null | { code, message }} [o.importBlock]
 */
export function createLzBcRunnerRouter({ readExportFile, getDb = S.getLzBcDb, now = () => Date.now(), token = () => process.env.LZ_BC_RUNNER_TOKEN, importBlock = importStateBlock } = {}) {
  const router = Router();
  router.use('/api', (req, res, next) => {
    const t = token();
    if (!t) return res.status(503).json({ ok: false, error: 'not_configured', message: 'LZ_BC_RUNNER_TOKEN 未設定' });
    const got = (req.headers.authorization || '').replace(/^Bearer\s+/i, '');
    if (!got || !timingSafeEq(got, t)) return res.status(401).json({ ok: false, error: 'unauthorized' });
    return next();
  });
  router.use('/api', (req, res, next) => {
    if (req.method === 'POST' && !/^application\/json\b/i.test(String(req.headers['content-type'] || ''))) return res.status(415).json({ ok: false, error: 'bad_content_type', message: 'application/json だけ' });
    return next();
  });
  router.use('/api', express.json({ limit: '256kb' }));
  router.use('/api', (err, req, res, next) => {
    if (!err) return next();
    return res.status(err.status === 413 ? 413 : 400).json({ ok: false, error: err.status === 413 ? 'too_large' : 'bad_json', message: err.status === 413 ? '本文が大きすぎる' : 'JSON として読めない' });
  });
  const handle = (fn) => async (req, res) => {
    try {
      res.json({ ok: true, ...(await fn(req.body || {}, req)) });
    } catch (e) {
      if (e instanceof S.BcImportError) return res.status(e.status).json({ ok: false, error: e.code, message: e.message });
      console.error('[lz-bc-import]', e);
      return res.status(500).json({ ok: false, error: 'internal', message: 'ポータルの中で失敗した (ログを見る)' });
    }
  };
  const jobId = (req) => {
    if (!ID_RE.test(String(req.params.id))) throw new S.BcImportError('not_found', 'その回はありません', 404);
    return Number(req.params.id);
  };

  router.post('/api/claim', handle(async (b) => {
    const db = getDb();
    const t = now();
    const runnerId = str(b.runner_id, 80) || 'minipc';
    const busy = b.busy == null || b.busy === '' ? null : String(b.busy).slice(0, 200);
    S.touchRunner(db, { runnerId, version: str(b.version, 80), busy, now: t });
    S.sweep(db, t);
    if (busy) return { job: null, why: 'busy' };
    if (S.nightOf(t).claim) return { job: null, why: 'night' };
    const j = S.nextQueued(db, t);
    if (!j) return { job: null, why: 'none' };
    if (S.voidHeld(db, j.export_id, t)) return { job: null, why: 'voiding' };   // 取り消しの途中 (待ちは残す・取り消せたら渡すときに断る)
    const blk = importBlock(t);
    if (blk) return { job: null, why: blk.code };   // 待ちは残す (15 分で expired)
    let f;
    try { f = await readExportFile(j.export_id); } catch (e) {
      console.error(`[lz-bc-import] Company DB の帳面を読めない: ${e && e.message}`);
      return { job: null, why: 'db_unreachable' };   // 待ちは残す
    }
    if (!f) { S.refuseQueued(db, { jobId: j.job_id, reason: 'not_found', detail: 'Company DB の帳面にファイルが無い (取り込んでいない)', now: now() }); return { job: null, why: 'refused' }; }
    if (f.state !== 'issued') { S.refuseQueued(db, { jobId: j.job_id, reason: 'voided', detail: '積んだ後に CSV が取り消された (取り込んでいない)', now: now() }); return { job: null, why: 'refused' }; }
    const fileSha = crypto.createHash('sha256').update(f.file_bytes).digest('hex');
    if (fileSha !== j.file_sha256 || f.file_sha256 !== j.file_sha256 || f.text_sha256 !== j.text_sha256) {
      S.refuseQueued(db, { jobId: j.job_id, reason: 'file_changed', detail: '積んだときとファイルの中身 (sha256) が違う (取り込んでいない)', now: now() });
      return { job: null, why: 'refused' };
    }
    const c = S.claimJob(db, { jobId: j.job_id, runnerId, now: now() });
    if (!c) return { job: null, why: 'none' };
    const cj = c.job;
    return {
      job: { job_id: cj.job_id, export_id: cj.export_id, rows: cj.rows, with_export: !!cj.with_export, rerun: !!cj.rerun_reason, file_name: f.file_name, file_sha256: cj.file_sha256, lease_until: cj.lease_until },
      claim_token: c.claimToken, file_b64: f.file_bytes.toString('base64'),
    };
  }));
  router.post('/api/jobs/:id/executing', handle((b, req) => S.markExecuting(getDb(), { jobId: jobId(req), claimToken: b.claim_token, importBlock: importBlock(now()), now: now() })));
  router.post('/api/jobs/:id/import', handle((b, req) => S.reportImport(getDb(), { jobId: jobId(req), claimToken: b.claim_token, importResult: importResultOf(b.import_result), log: str(b.log, 64 * 1024), now: now() })));
  router.post('/api/jobs/:id/final', handle((b, req) => S.reportFinal(getDb(), {
    jobId: jobId(req), claimToken: b.claim_token, executed: b.executed === true, importResult: importResultOf(b.import_result), exportResult: exportResultOf(b.export_result),
    reason: str(b.reason, 60), detail: str(b.detail, 300), log: str(b.log, 64 * 1024), now: now(),
  })));
  // この前置きの要求は全部ここで答える (後ろのフォーム・共通の JSON の parser に本文を読ませない)
  router.use((req, res) => res.status(404).json({ ok: false, error: 'not_found' }));
  return router;
}
