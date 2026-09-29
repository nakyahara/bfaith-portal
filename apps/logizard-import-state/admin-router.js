/**
 * logizard-import-state/admin-router.js — ロジザードの取込の状態の「画面の口」(マスタ正本切替 ③c-1b-3b-4a)
 *
 * 設計 = AI_reference CompanyDB構想/10 §6.3「③c-1b-3b 設計 v4」「③c-1b-3b 契約 (v4 + 設計 R1)」K3-3・K3-4・K3-6・K3-7。
 * 人がどの端末でもブラウザで使う口 (手の取込・止める / 再開・解除・待ちの waiver・設定)。機械の口 (router.js の /api・Bearer) とは別。
 *
 * mount (server.js・Render だけ・セッションの後):
 *   app.use('/apps/logizard-import-state/admin-api', adminApiGate, createAdminRouter())
 *   app.get('/apps/logizard-import-state/admin', requireAdmin, renderAdminPage)   画面 + 手順 (views/admin.ejs・③c-1b-3b-4b)
 * 守りの順番 (本文を読む前に全部):
 *   1. ログイン + 管理者 (adminApiGate。違う = JSON の 401 / 403。画面の redirect はしない)
 *   2. 書く口は Origin = Host (ブラウザから。cookie の sameSite lax と組で CSRF を防ぐ)
 *   3. Content-Type を口ごとに固定 (JSON / CSV のバイト列) → 4. この口だけの parser (JSON 64KB・CSV 4MB)
 * 誰 = セッションのメール (本文から受けない)。
 *
 *   GET  /status                         今の状態の全部 (状態・止め・鍵・開いた手の取込・確認待ち・待ち・知らせ・設定・成果物・最近の手の取込・出来事)
 *   POST /halt {reason} / /resume {note} / /resolve {run_id, outcome, note, partial_check, repaired} / /mark-unknown {run_id, reason}
 *   POST /manual/open {lz_account, source_run_id}               毎晩の成果物で手の取込を始める
 *   POST /manual/open-gas?lz_account&target_as_of               本文 = GAS の CSV のバイト列 (移行の段階の間だけ)
 *   GET  /manual/:id/csv                                         手の取込の CSV (ロジザードに置くのはこれだけ・attachment・no-store)
 *   POST /manual/:id/complete {result_text, history: {file_name, at, account}, note}
 *   POST /manual/:id/cancel {note} / /manual/:id/ack {note}
 *   GET  /pending[?after&limit&product]                         待ちの義務 (番号の後から・商品で探す)
 *   POST /waive {obligation_ids, note}
 *   POST /settings {key, value}                                  cutover_phase (一方通行)・lz_accounts
 * halt・終える (needs_review) の後は、積んだ知らせをすぐ送る (GCHAT_WEBHOOK_JOBS・送れない = 定時の入口が送り直す。K3-4)。
 */
import { Router } from 'express';
import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import * as S from './store.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
/** 画面 (③c-1b-3b-4b)。server.js が requireAdmin の後に呼ぶ。値は画面の JS が /admin-api から読んで textContent で出す */
export function renderAdminPage(req, res) {
  res.set('Cache-Control', 'no-store');
  res.render(path.join(__dirname, 'views', 'admin.ejs'), { username: (req.session && req.session.email) || '', displayName: (req.session && req.session.displayName) || '' });
}

/** ログイン + 管理者 (違う = JSON)。本文は読まない */
export function adminApiGate(req, res, next) {
  if (!req.session || !req.session.authenticated) return res.status(401).json({ ok: false, error: 'session_expired', message: 'ログインしてください' });
  if (req.session.role !== 'admin') return res.status(403).json({ ok: false, error: 'forbidden', message: '管理者だけが使える' });
  return next();
}

/** 既定の知らせ = 要対応スペース (GCHAT_WEBHOOK_JOBS)。無い・送れない = false (定時の入口が送り直す) */
async function defaultNotify(text) {
  const hook = String(process.env.GCHAT_WEBHOOK_JOBS || '').trim();
  if (!/^https:\/\//.test(hook)) return false;
  try {
    const { sendGChatMessage } = await import('../profit-analysis/gchat-client.js');
    await sendGChatMessage(hook, text);
    return true;
  } catch (e) { console.warn('[logizard-import-state admin] 知らせを送れない:', String(e && e.message).slice(0, 160)); return false; }
}

/**
 * @param {object} [opts]
 * @param {() => import('better-sqlite3').Database} [opts.getDb]
 * @param {() => number} [opts.now]
 * @param {(text: string) => Promise<boolean>} [opts.notify]
 */
export function createAdminRouter({ getDb = null, now = () => Date.now(), notify = defaultNotify } = {}) {
  let db = null;
  const dbOf = () => (getDb ? getDb() : (db ||= S.openImportStateDb()));
  const router = Router();
  // 書く口 = ブラウザから (Origin = Host)。GET は読むだけ
  router.use((req, res, next) => {
    if (['GET', 'HEAD', 'OPTIONS'].includes(req.method)) return next();
    let host = null;
    try { host = req.headers.origin ? new URL(req.headers.origin).host : null; } catch { /* 壊れた Origin = 不一致 */ }
    if (!host || host !== req.headers.host) return res.status(403).json({ ok: false, error: 'origin_mismatch', message: 'ブラウザから操作してください (Origin ヘッダが必要です)' });
    return next();
  });
  const typeIs = (re, name) => (req, res, next) => (re.test(String(req.headers['content-type'] || '')) ? next()
    : res.status(415).json({ ok: false, error: 'unsupported_media_type', message: `Content-Type は ${name}` }));
  // 型はちょうどその名前だけ (後ろに ; の引数は可・application/json-patch+json などの似た型 = 415。Codex #1541 R1 Low)
  const json = [typeIs(/^application\/json\s*(;|$)/i, 'application/json'), express.json({ limit: '64kb' })];
  const csv = [typeIs(/^application\/octet-stream\s*(;|$)/i, 'application/octet-stream'), express.raw({ type: 'application/octet-stream', limit: S.LIMITS.csvBytes })];
  const who = (req) => String((req.session && req.session.email) || '').slice(0, 60);
  const one = (q, k) => (typeof q[k] === 'string' ? q[k] : undefined);   // 同じ名前が 2 つ = 無い扱い
  const bad = (m) => new S.ImportStateError('bad_request', m, 400);
  const handle = (fn) => async (req, res) => {
    try {
      res.json({ ok: true, ...(await fn(req.body || {}, req)) });
    } catch (e) {
      if (e instanceof S.ImportStateError) return res.status(e.status).json({ ok: false, error: e.code, message: e.message });
      console.error('[logizard-import-state admin]', e);   // 詳しいことはサーバーのログだけ
      return res.status(500).json({ ok: false, error: 'internal', message: 'ポータルの中で失敗した (ログを見る)' });
    }
  };
  /**
   * 積んだ知らせを送る。今回積んだ知らせ (firstId) を真っ先に (前の知らせが溜まっていても。Codex #1541 R1 Medium)・ほかは古い順に 20 件まで。
   * 送れた = 送れた印。送れない = 定時の入口が送り直す。取引の外 = 状態の書き込みは先に済んでいる。
   * @returns {Promise<boolean|null>} 今回の知らせを送れたか (無い = null)
   */
  const flushOutbox = async (firstId = null) => {
    const send = async (o) => { const ok = await notify(o.text).catch(() => false); if (ok) S.outboxMarkSent(dbOf(), { id: o.id, by: 'portal', now: now() }); return ok; };
    let mine = null;
    if (firstId != null) { const o = S.outboxGet(dbOf(), { id: firstId }); mine = o ? await send(o) : null; if (mine === false) return false; }
    for (const o of S.outboxPending(dbOf(), { limit: 20 })) if (!(await send(o))) break;
    return mine;
  };

  router.get('/status', handle(() => S.getAdminOverview(dbOf(), { now: now() })));
  router.post('/halt', json, handle(async (b, req) => { const r = S.halt(dbOf(), { by: who(req), reason: b.reason, now: now() }); return { ...r, notified: await flushOutbox(r.outbox_id) }; }));
  router.post('/resume', json, handle((b, req) => S.resume(dbOf(), { by: who(req), note: b.note, now: now() })));
  router.post('/resolve', json, handle((b, req) => S.resolve(dbOf(), { runId: b.run_id, outcome: b.outcome, note: b.note, by: who(req), partialCheck: b.partial_check ?? null, repaired: b.repaired === true, now: now() })));
  router.post('/mark-unknown', json, handle((b, req) => S.markUnknown(dbOf(), { runId: b.run_id, by: who(req), reason: b.reason ?? null, now: now() })));
  router.post('/manual/open', json, handle((b, req) => {
    if (typeof b.source_run_id !== 'string') throw bad('source_run_id (毎晩の成果物) が要る');
    return S.openManualSession(dbOf(), { by: who(req), lzAccount: b.lz_account, source: { kind: 'cdb_artifact', sourceRunId: b.source_run_id }, now: now() });
  }));
  router.post('/manual/open-gas', csv, handle((body, req) => {
    if (!Buffer.isBuffer(body) || !body.length) throw bad('本文は GAS の CSV のバイト列 (application/octet-stream)');
    const q = req.query || {};
    return S.openManualSession(dbOf(), { by: who(req), lzAccount: one(q, 'lz_account'), source: { kind: 'gas_upload', csvBuf: body, targetAsOf: one(q, 'target_as_of') }, now: now() });
  }));
  router.get('/manual/:id/csv', (req, res) => {
    try {
      const x = S.manualSessionCsv(dbOf(), { sessionId: req.params.id });
      // ファイル名はサーバーが作った ID (lzm_…) だけ = 見出しに入れて安全
      res.set({ 'Content-Type': 'application/octet-stream', 'Content-Disposition': `attachment; filename="${x.download_name}"`, 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff' });
      return res.send(x.csv);
    } catch (e) {
      if (e instanceof S.ImportStateError) return res.status(e.status).json({ ok: false, error: e.code, message: e.message });
      console.error('[logizard-import-state admin]', e);
      return res.status(500).json({ ok: false, error: 'internal', message: 'ポータルの中で失敗した (ログを見る)' });
    }
  });
  router.post('/manual/:id/complete', json, handle(async (b, req) => {
    const h = b.history && typeof b.history === 'object' ? { fileName: b.history.file_name, at: b.history.at, account: b.history.account } : null;
    const r = S.completeManualSession(dbOf(), { sessionId: req.params.id, resultText: b.result_text, history: h, note: b.note ?? null, by: who(req), now: now() });
    return { ...r, notified: r.outbox_id != null ? await flushOutbox(r.outbox_id) : null };
  }));
  router.post('/manual/:id/cancel', json, handle((b, req) => S.cancelManualSession(dbOf(), { sessionId: req.params.id, note: b.note, by: who(req), now: now() })));
  router.post('/manual/:id/ack', json, handle((b, req) => S.acknowledgeManualSession(dbOf(), { sessionId: req.params.id, note: b.note, by: who(req), now: now() })));
  // 待ちの義務 (番号の後から・商品で探す = 何件あっても全部に届く)
  router.get('/pending', handle((_b, req) => {
    const q = req.query || {};
    for (const k of ['after', 'limit', 'product']) if (q[k] !== undefined && typeof q[k] !== 'string') throw bad(`${k} は 1 つだけ`);   // 同じ名前が 2 つ = 断る
    const after = one(q, 'after'), limit = one(q, 'limit'), product = one(q, 'product');
    if (after !== undefined && !/^[0-9]{1,12}$/.test(after)) throw bad('after は番号');
    if (limit !== undefined && (!/^[1-9][0-9]{0,3}$/.test(limit) || Number(limit) > 5000)) throw bad('limit は 1〜5000 の整数');
    if (product !== undefined && (!product || product.length > 200)) throw bad('product は 1〜200 文字');
    return S.listPending(dbOf(), { afterId: after === undefined ? 0 : Number(after), limit: limit === undefined ? 500 : Number(limit), productId: product ?? null });
  }));
  router.post('/waive', json, handle((b, req) => S.waiveObligations(dbOf(), { obligationIds: b.obligation_ids, note: b.note, by: who(req), now: now() })));
  router.post('/settings', json, handle((b, req) => S.setSetting(dbOf(), { key: b.key, value: b.value, by: who(req), now: now() })));
  // 本文が大きすぎる・JSON でない = 短い決まった JSON (本文の断片を返さない)
  router.use((err, req, res, next) => {
    if (!err) return next();
    return res.status(err.status === 413 ? 413 : 400).json({ ok: false, error: err.status === 413 ? 'too_large' : 'bad_json', message: err.status === 413 ? '本文が大きすぎる' : 'JSON として読めない' });
  });
  return router;
}

export default createAdminRouter();
