/**
 * lz-bc-import/runner-router.js — 「ロジザードに取り込む」の機械の口 (miniPC の bc-import-runner.js が取りに来る・Render だけ)
 *
 * server.js で **どの body parser よりも前に** mount する (Bearer LZ_BC_RUNNER_TOKEN の認証の前に本文を読まない = logizard-import-state と同じ)。
 * token が無い = 503 (閉じる)。人の画面の口は apps/master-edit/router.mjs (/api/lz-csv/...・名簿の人だけ・Origin の守り)。
 *
 *   POST /apps/lz-bc-import/api/claim              { runner_id, version, busy, export_upload }  → { job | null, why, claim_token, file_b64 }
 *        来た印 (画面の「miniPC: 来ている」)。busy = miniPC がほかのロジザードの作業中・夜 など (渡さない)。
 *        🆕 export_upload = true: ② の書き出しをポータルに送れる runner (2026-10-21〜)。言わない古い runner には ② の印を外して渡す (① だけ)
 *        渡す前に Company DB の帳面のファイルを読み直す: 取り消していない・sha256 (byte 列と文字) が積んだときと同じ (違う = その回は押さずに終わり)
 *   POST /apps/lz-bc-import/api/jobs/:id/executing { claim_token }               ① の実行ボタンを押す直前 (断る = 押さない)
 *   POST /apps/lz-bc-import/api/jobs/:id/import    { claim_token, import_result, log }   ① の結果 (途中の知らせ)
 *   🆕 POST /apps/lz-bc-import/api/jobs/:id/export-file { claim_token, file_b64, sha256 }  ② の書き出し (バーコードマスタ.csv・Shift_JIS のまま・約 0.8 MB)
 *        → ポータルが確かめて共有ドライブの同じファイルを上書き (drive-export.js) → { export: { status: ok | failed, rows, detail } }。
 *        書けない (権限・設定が無い・中身がおかしい) = 200 で status failed (① は取り込んだまま)
 *   POST /apps/lz-bc-import/api/jobs/:id/final     { claim_token, executed, import_result, export_result, reason, detail, log }  終わり (送り直してよい)
 * 断る = 409 (状態) / 403 (鍵) / 400 (形) / 404。{ ok: false, error: code, message }
 */
import express, { Router } from 'express';
import crypto from 'node:crypto';
import * as S from './store.js';
import { importStateBlock } from './lz-state.js';
import { getDriveTarget, checkBarcodeMasterBytes, jstOf, EXPORT_MIN_ROWS, DRIVE_NAME, DRIVE_FILE_NAME } from './drive-export.js';

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
export function createLzBcRunnerRouter({ readExportFile, getDb = S.getLzBcDb, now = () => Date.now(), token = () => process.env.LZ_BC_RUNNER_TOKEN, importBlock = importStateBlock, drive = getDriveTarget, exportMinRows = EXPORT_MIN_ROWS } = {}) {
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
  // ② のファイル (約 0.8 MB・base64 で約 1.1 MB) だけ大きい本文を受ける。ほかは今までどおり 256kb
  const bigJson = express.json({ limit: '4mb' });
  const smallJson = express.json({ limit: '256kb' });
  router.use('/api', (req, res, next) => (/^\/jobs\/[^/]+\/export-file$/.test(req.path) ? bigJson : smallJson)(req, res, next));
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
    const exportUpload = typeof b.export_upload === 'boolean' ? b.export_upload : null;   // 言わない = 古い runner (分からない)
    S.touchRunner(db, { runnerId, version: str(b.version, 80), busy, exportUpload, now: t });
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
      // ② の印は ② のファイルを送れる runner にだけ渡す (古い runner = G: の保存先が無いと押す前に止まる = ① だけさせる。帳面の回は ② の結果「しなかった」で終わる)
      job: { job_id: cj.job_id, export_id: cj.export_id, rows: cj.rows, with_export: !!cj.with_export && exportUpload === true, rerun: !!cj.rerun_reason, file_name: f.file_name, file_sha256: cj.file_sha256, lease_until: cj.lease_until },
      claim_token: c.claimToken, file_b64: f.file_bytes.toString('base64'),
    };
  }));
  router.post('/api/jobs/:id/executing', handle((b, req) => S.markExecuting(getDb(), { jobId: jobId(req), claimToken: b.claim_token, importBlock: importBlock(now()), now: now() })));
  router.post('/api/jobs/:id/import', handle((b, req) => S.reportImport(getDb(), { jobId: jobId(req), claimToken: b.claim_token, importResult: importResultOf(b.import_result), log: str(b.log, 64 * 1024), now: now() })));
  let uploading = false;   // Drive に書くのは 1 つずつ (回をまたいでも。古い回の中身で新しい回を巻き戻さない。Codex R2 High)
  router.post('/api/jobs/:id/export-file', handle(async (b, req) => {
    const id = jobId(req);
    const tok = b.claim_token;
    if (uploading) throw new S.BcImportError('busy', 'ほかの ② を共有ドライブに書いている途中です', 409);
    uploading = true;
    try {
      S.beginExportUpload(getDb(), { jobId: id, claimToken: tok, now: now() });   // 鍵・状態・新しい回がもう書いた (断る = 409 / 403。Drive には触らない)
      const said = (status, detail, rows = null, modifiedTime = null) => {
        S.recordExportUpload(getDb(), { jobId: id, claimToken: tok, status, rows, detail, modifiedTime, now: now() });
        return { export: { status, rows, detail } };
      };
      const b64 = typeof b.file_b64 === 'string' ? b.file_b64 : '';
      const buf = Buffer.from(b64, 'base64');
      if (!buf.length || buf.toString('base64') !== b64.replace(/\s+/g, '')) return said('failed', '② のファイルが届いたが形が違う (base64)。共有ドライブは前のまま');
      if (typeof b.sha256 !== 'string' || crypto.createHash('sha256').update(buf).digest('hex') !== b.sha256) return said('failed', '② のファイルの sha256 が miniPC の値と違う (途中で欠けた)。共有ドライブは前のまま');
      const c = checkBarcodeMasterBytes(buf, { minRows: exportMinRows });
      if (!c.ok) return said('failed', `② の中身がおかしいので書かない (${c.reason})。共有ドライブは前のまま`);
      let u;
      try { u = await drive().upload(buf); } catch (e) {
        console.error(`[lz-bc-import] ② を Drive に書けない (#${id}): ${e && e.code} ${e && e.message}`);
        // 書きに行った後に答えが無い・中身の印を確かめられない = 書けたかもしれない (「前のまま」と言わない)
        if (e && e.code === 'uncertain') return said('unknown', `書けたか分からない: ${String(e.message).slice(0, 200)}。共有ドライブのバーコードマスタ.csv の更新時刻を確かめる`);
        const why = e && e.code === 'no_permission' ? '権限' : e && e.code === 'off' ? '設定がまだ' : 'Drive';
        return said('failed', `書けなかった (${why}): ${String((e && e.message) || e).slice(0, 200)}。共有ドライブは前のまま`);
      }
      // Drive に書けた後の帳面の失敗を「書けなかった」にしない (500 = miniPC は失敗と言う → 帳面は「受けた・結果なし」= 分からない。Codex R2 Medium)
      return said('ok', `共有ドライブ「${DRIVE_NAME}」の${DRIVE_FILE_NAME}を更新 (${jstOf(u.modified_time) || '時刻不明'})`, c.rows, u.modified_time);
    } finally { uploading = false; }
  }));
  router.post('/api/jobs/:id/final', handle((b, req) => S.reportFinal(getDb(), {
    jobId: jobId(req), claimToken: b.claim_token, executed: b.executed === true, importResult: importResultOf(b.import_result), exportResult: exportResultOf(b.export_result),
    reason: str(b.reason, 60), detail: str(b.detail, 300), log: str(b.log, 64 * 1024), now: now(),
  })));
  // この前置きの要求は全部ここで答える (後ろのフォーム・共通の JSON の parser に本文を読ませない)
  router.use((req, res) => res.status(404).json({ ok: false, error: 'not_found' }));
  return router;
}
