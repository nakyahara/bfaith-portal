/**
 * router.mjs — マスタの判断 (NE との差) の画面と API (D2'。Company DB構想 10 §6.1.1「D2' 判断の画面と API の契約 v1」)
 *
 * 載せ方 (server.js): env MASTER_DECISIONS_ENABLED = 1 のときだけ (Render。miniPC は同じ server.js を動かすが載せない = 書く口を 1 つに)
 *   app.use('/apps/master-decisions', requireAppAccess('master-decisions'), router)
 * 見る = アプリの利用権がある人 / 決める (承認・却下・取り消し) = env MASTER_DECISION_APPROVERS の名簿のメールだけ
 *   (空 = 誰も決められない。admin でも名簿に無ければ不可。画面で隠すだけでなく API で止める = 価格更新と同じ)
 *   GET  /                          画面
 *   GET  /manual                    つかいかた
 *   GET  /api/summary               最新の照合の回・理由の種類 × 状態の件数・自分が決められるか
 *   GET  /api/candidates            候補の一覧 (view・status・reason・cls・q・limit・offset)
 *   GET  /api/candidates/:fp/events 1 つの候補の出来事の履歴
 *   POST /api/decisions             決める { kind, resolution?, note?, items: [{ fingerprint, shown_last_seen_run, shown_event_id, target_value? }] }
 *   NE に取り込む CSV (③b-1・0040。作る・配る・確かめる・申告・void・実機の確かめは全部名簿の人だけ):
 *   GET  /csv                       画面
 *   GET  /api/csv/summary           今日の照合の回・列ごとの件数・NE の画面で直す一覧・最近のファイル
 *   GET  /api/csv/exports/:id       1 つのファイルの行と届き方・取込の試み
 *   GET  /api/csv/exports/:id/file  CSV の byte 列 (void は 410)
 *   POST /api/csv/exports           作る { kind, col, fingerprints? }
 *   POST /api/csv/exports/:id/check    取り込む直前に確かめる (外れたら void)
 *   POST /api/csv/exports/:id/declare  取り込んだと申告 { result: ok|partial|rejected_all, note? }
 *   POST /api/csv/exports/:id/void     使わない
 *   POST /api/csv/verified          実機で確かめた { kind, col, result: ok|ng, note?, export_id? }
 * env: COMPANY_DB_URL (表の持ち主のロール。無い = 503)
 */
import express from 'express';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { openPgClient, pgAdapter } from '../../scripts/company-db/migrate.mjs';
import { summary, listCandidates, candidateEvents, applyDecisions, DecideError } from './decide.mjs';
import { csvSummary, exportDetail, exportFile, createExport, checkExport, declareExport, voidExport, recordVerified } from './ne-csv.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const view = (name) => path.join(__dirname, 'views', name);
const router = express.Router();

/** Postgres の接続の作り方 (試験は PGlite に差し替える。本番では触らない) */
let pgClientFactory = openPgClient;
export function __setPgClientFactory(fn) { pgClientFactory = fn || openPgClient; }
/** 今の時刻 (試験は日をまたぐために差し替える) */
let clock = () => Date.now();
export function __setClock(fn) { clock = fn || (() => Date.now()); }

// ─── CSRF 二段ガード (価格更新と同じ): 書く API は Origin 必須で Host と一致・Content-Type は JSON ───
router.use('/api/', (req, res, next) => {
  if (['GET', 'HEAD', 'OPTIONS'].includes(req.method)) return next();
  const origin = req.headers.origin;
  let host = null;
  try { host = origin ? new URL(origin).host : null; } catch { /* 壊れた Origin は不一致 */ }
  if (!host || host !== req.headers.host) return res.status(403).json({ ok: false, error: 'origin_mismatch', message: 'ブラウザから操作してください (Origin ヘッダが必要です)' });
  if (!/^application\/json\b/i.test(String(req.headers['content-type'] || ''))) return res.status(415).json({ ok: false, error: 'Content-Type は application/json にしてください' });
  next();
});
router.use(express.json({ limit: '512kb' }));

/**
 * 決められる人か。env MASTER_DECISION_APPROVERS にメールをカンマ区切りで。名簿がすべて (admin でも名簿に無ければ不可・空なら誰も決められない)
 */
export function approverGate(req) {
  const list = String(process.env.MASTER_DECISION_APPROVERS || '').split(',').map((s) => s.trim().toLowerCase()).filter(Boolean);
  if (!list.length) return { ok: false, message: '決められる人がまだ設定されていません (環境変数 MASTER_DECISION_APPROVERS)。設定されるまで誰も決められません (見るのはできます)' };
  const email = String(req.session?.email || '').trim().toLowerCase();
  if (!email || !list.includes(email)) return { ok: false, message: '決める (承認・却下・取り消し) のは名簿の人だけです。見るのはできます' };
  return { ok: true, message: null };
}

/** 1 回の要求で 1 つの接続 (pool なし)。文・ロック・取引内の空きに上限 = 照合の書き込みと取り合っても待ち続けない */
async function withPg(res, fn) {
  const url = process.env.COMPANY_DB_URL;
  if (!url) return res.status(503).json({ ok: false, error: 'Company DB につながっていません (COMPANY_DB_URL が無い)' });
  let client;
  try {
    client = await pgClientFactory(url, { application_name: 'master-decisions' });
    if (client.on) client.on('error', (e) => console.error(`[master-decisions] 接続のエラー: ${e.message}`));   // 切れた接続でプロセスを落とさない
    await client.query(`set statement_timeout = '20s'`);
    await client.query(`set lock_timeout = '10s'`);
    await client.query(`set idle_in_transaction_session_timeout = '60s'`);
    await fn(pgAdapter(client));
  } catch (e) {
    if (e && e.code === 'VALIDATION') return res.status(400).json({ ok: false, error: e.message, reason: e.reason || null, ...(e.details ? { details: e.details } : {}) });
    if (e && e.code === '55P03') return res.status(409).json({ ok: false, error: 'ほかの処理 (朝の照合など) が同じ候補を使っています。少し待ってからもう一度', reason: 'locked' });
    console.error(`[master-decisions] ${e && e.message}`);
    if (!res.headersSent) res.status(500).json({ ok: false, error: 'サーバーエラーが発生しました' });
  } finally { if (client) { try { await client.end(); } catch { /* */ } } }
}

const pageLocals = (req) => {
  const gate = approverGate(req);
  return { title: 'マスタの判断 (NE との差)', username: req.session?.email || '', displayName: req.session?.displayName || '', canDecide: gate.ok, gateMessage: gate.message || '' };
};
router.get('/', (req, res) => {
  // 画面の中のリンク・API は相対 (api/…・manual) = 末尾の / が無いと 1 つ上を指す
  if (!String(req.originalUrl || '').split('?')[0].endsWith('/')) return res.redirect(301, `${req.baseUrl}/`);
  res.render(view('index.ejs'), pageLocals(req));
});
router.get('/manual', (req, res) => res.render(view('manual.ejs'), pageLocals(req)));
router.get('/csv', (req, res) => res.render(view('csv.ejs'), { ...pageLocals(req), title: 'NE に取り込む CSV' }));

router.get('/api/summary', (req, res) => withPg(res, async (db) => {
  const s = await summary(db);
  const gate = approverGate(req);
  res.json({ ok: true, ...s, can_decide: gate.ok, gate_message: gate.message });
}));
router.get('/api/candidates', (req, res) => withPg(res, async (db) => {
  const q = req.query;
  res.json({ ok: true, ...(await listCandidates(db, { view: q.view, status: q.status, reason: q.reason ? String(q.reason) : null, cls: q.cls ? String(q.cls) : null, q: q.q, limit: q.limit, offset: q.offset })) });
}));
router.get('/api/candidates/:fp/events', (req, res) => withPg(res, async (db) => {
  const r = await candidateEvents(db, req.params.fp);
  if (!r) return res.status(404).json({ ok: false, error: '候補が無い' });
  res.json({ ok: true, ...r });
}));
router.post('/api/decisions', (req, res) => {
  const gate = approverGate(req);
  if (!gate.ok) return res.status(403).json({ ok: false, error: gate.message, reason: 'not_approver' });
  const b = req.body || {};
  return withPg(res, async (db) => {
    const r = await applyDecisions(db, { actor: String(req.session.email).trim().toLowerCase(), kind: b.kind, resolution: b.resolution ?? null, note: b.note ?? null, items: b.items });
    res.json({ ok: true, ...r });
  });
});

// ─────────── NE に取り込む CSV (③b-1) ───────────
const exportIdOf = (req) => { const n = Number(req.params.id); return Number.isInteger(n) && n > 0 ? n : 0; };
const actorOf = (req) => String(req.session.email).trim().toLowerCase();
/** 名簿の人だけ (見るのは誰でも・作る / 配る / 確かめる / 申告 / void / 実機の確かめは名簿の人) */
const gated = (fn) => (req, res) => {
  const gate = approverGate(req);
  if (!gate.ok) return res.status(403).json({ ok: false, error: gate.message, reason: 'not_approver' });
  return withPg(res, (db) => fn(db, req, res));
};
router.get('/api/csv/summary', (req, res) => withPg(res, async (db) => {
  const gate = approverGate(req);
  res.json({ ok: true, ...(await csvSummary(db, { nowMs: clock() })), can_decide: gate.ok, gate_message: gate.message });
}));
router.get('/api/csv/exports/:id', (req, res) => withPg(res, async (db) => {
  const r = await exportDetail(db, exportIdOf(req), { nowMs: clock() });
  if (!r) return res.status(404).json({ ok: false, error: 'ファイルが無い' });
  res.json({ ok: true, ...r });
}));
router.get('/api/csv/exports/:id/file', gated(async (db, req, res) => {
  const f = await exportFile(db, exportIdOf(req), { nowMs: clock() });
  if (!f) return res.status(404).json({ ok: false, error: 'ファイルが無い' });
  if (f.state === 'void') return res.status(410).json({ ok: false, error: 'このファイルは使えません (void)。取り込まないでください', reason: 'void' });
  if (f.state === 'retired') return res.status(410).json({ ok: false, error: 'この申告済みのファイルはもう使えません。取り込み直すなら作り直してください', reason: 'retired' });
  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="${f.file_name}"`);
  res.setHeader('Cache-Control', 'no-store');
  res.end(f.bytes);
}));
router.post('/api/csv/exports', gated(async (db, req, res) => {
  const b = req.body || {};
  res.json({ ok: true, ...(await createExport(db, { actor: actorOf(req), kind: b.kind, col: b.col, fingerprints: b.fingerprints ?? null, nowMs: clock() })) });
}));
router.post('/api/csv/exports/:id/check', gated(async (db, req, res) => {
  res.json({ ok: true, ...(await checkExport(db, { actor: actorOf(req), exportId: exportIdOf(req), nowMs: clock() })) });
}));
router.post('/api/csv/exports/:id/declare', gated(async (db, req, res) => {
  const b = req.body || {};
  res.json({ ok: true, ...(await declareExport(db, { actor: actorOf(req), exportId: exportIdOf(req), result: b.result, note: b.note ?? null, nowMs: clock() })) });
}));
router.post('/api/csv/exports/:id/void', gated(async (db, req, res) => {
  res.json({ ok: true, ...(await voidExport(db, { actor: actorOf(req), exportId: exportIdOf(req), nowMs: clock() })) });
}));
router.post('/api/csv/verified', gated(async (db, req, res) => {
  const b = req.body || {};
  res.json({ ok: true, ...(await recordVerified(db, { actor: actorOf(req), kind: b.kind, col: b.col, result: b.result, note: b.note ?? null, exportId: b.export_id ?? null })) });
}));

export default router;
