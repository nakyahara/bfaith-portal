/**
 * test-lz-bc-import.mjs — 「ロジザードに取り込む」ボタン (apps/lz-bc-import・tools/logizard-automation/bc-import-runner.js・barcode-steps.js)
 *
 * Company DB = PGlite (scripts/fixtures/master-variation-db.mjs)。本物の master-edit の router と機械の口を HTTP 越しに通す。ロジザードには触らない
 * (miniPC の画面の操作は偽物の session か、偽物のロジザードの画面 = playwright の route で全部の通信を止めて返す)。
 * 固定する契約:
 *   S1 積む: miniPC が来ていない・夜 (23:45〜01:30)・取り消した・ロジザードの取込の状態の止め・ほかの回が動いている・分からない回が残る = 断る /
 *      もう取り込んだ = 理由 (4〜200 字) が要る / ダウンロードした = 「手で取り込んでいない」が要る / 同じ押し直し = 前の答え・ほかのファイル = 断る / 開いた後に中身が変わった = 断る
 *   S2 状態: 取る → 押す → ① 結果 → ② → 取り込んだ / 件数が合わない・エラー = import_error / 押した後に分からない = unknown / 押す前の終わり = not_executed / 食い違い = unknown
 *   S3 期限: 待ち 15 分 = expired / 取った後 10 分 = not_executed / 押した後 8 分 = unknown (後から届いた本当の結果で直る) / ② の途中 = 取り込んだ・② は分からない
 *   S4 押す: やめた・夜・取込の状態の止め = 断る (その回は押していないと残る) / 鍵が違う = 403 / 2 回目 = 断る
 *   S5 動いているのは 1 つだけ (表の一意の索引)・出来事は追記だけ
 *   S6 取り消し: 動いている・分からない・取り込んだ = 断る / エラーのあった回は取り消せる
 *   I1 権限: 名簿 (マスタの判断) と マスタの入力の名簿 の両方 / Origin・JSON の守り / 機械の口は Bearer (無い = 503・違う = 401・本文を読む前)
 *   I2 本物の流れ: 画面で積む → miniPC が取る (帳面のファイルと 1 byte も違わない・sha256) → 押す → 結果 → 画面に件数とログ → もう一度は理由
 *   I3 すり替え: 積んだ後に取り消した・中身が違う = 渡さない (押していない)
 *   R  bc-import-runner.js の 1 回 (偽物の session): 成功 / 押すを断られた = 押さない / 押した後に止まった = unknown / エラー / ファイルの確かめ / この PC の取込済み /
 *      終わりの知らせが届かない = 残して次の回に送る / 忙しい = 取らない / 途中で process が終わった = 残す
 *   P  本物の barcode-steps.js の ① を偽物のロジザードの画面で: 押す前にポータルへ「押す」・断られたら実行ボタンを押さない・結果の件数を読む
 *   U  画面 (本物のブラウザ): ボタンの状態 (miniPC が来ていない・夜・押せる・もう一度)・確かめの欄・積んだ後の様子・終わった後・分からない回を記す
 * 使い方: node --import file:///…/use-chrome.mjs scripts/test-lz-bc-import.mjs (playwright の chromium の代わりに入れてある Chrome を使うとき)
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import express from 'express';
import iconv from 'iconv-lite';

const killer = setTimeout(() => { console.error('時間切れ (600 秒)'); process.exit(3); }, 600e3);
killer.unref();
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);
process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'lzbc-import-'));

const BC = await import('../apps/lz-bc-import/store.js');
BC.__setLzBcDb(BC.openLzBcDb(':memory:'));
const { createLzBcRunnerRouter } = await import('../apps/lz-bc-import/runner-router.js');
const LS = await import('../apps/lz-bc-import/lz-state.js');
const RUN = await import('../tools/logizard-automation/bc-import-runner.js');
const { createBcImportClient } = await import('../tools/logizard-automation/bc-import-client.js');
const STEPS = await import('../tools/logizard-automation/barcode-steps.js');

const { setupVariationDb } = await import('./fixtures/master-variation-db.mjs');
const RB = await import('../lib/master-register-blanks.mjs');
const LZE = await import('../lib/lz-new-export.mjs');
const T = await setupVariationDb();
const { pg, db, asEditor } = T;
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const { initPurchaseOrders } = await import('../apps/purchase-orders/db.js');
initPurchaseOrders();

process.env.COMPANY_DB_URL = 'postgres://owner@localhost/lzbc';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost/lzbc';
process.env.MASTER_EDITORS = 'naka@test,editor-only@test';
process.env.MASTER_EDIT_OPEN = '1';
process.env.MASTER_DECISION_APPROVERS = 'naka@test,approver-only@test';
const MR = await import('../apps/master-edit/router.mjs');
let chain = Promise.resolve();
MR.__setPgClientFactory(async (url) => {
  let release; const prev = chain; chain = new Promise((r) => { release = r; }); await prev;
  await pg.query(`set role ${/master_edit@/.test(url) ? 'master_edit' : 'deploy'}`);
  return { query: (t, p) => pg.query(t, p), end: async () => { await pg.query('set role deploy'); release(); }, on: () => {} };
});
MR.__setShippingRatesProvider(async () => new Map([['B2', { method: '宅急便コンパクト', cost: 520 }]]));
MR.__setPhDraftLookup(async () => null);
const mirrorOf = ({ keys = [] } = {}) => ({ ok: true, reason: null, keys: new Set(keys.map((k) => k.toLowerCase())), ids: new Map(), barcodes: new Map(), list_as_of: new Date().toISOString(), bc_as_of: new Date().toISOString(), rows: Math.max(1, keys.length), bc: { ok: true, reason: null, rows: 1, empty: false } });
let MIRROR = mirrorOf({ keys: ['zz-old'] });
MR.__setLzMirrorProvider(async () => MIRROR);

// 時計 (帳面と機械の口で同じ)・ロジザードの取込の状態の止め・機械の口の token
const jst = (hhmm, day = '2030-01-10') => Date.parse(`${day}T${hhmm}:00+09:00`);
let NOW = jst('10:00');
const clock = () => NOW;
MR.__setLzBcClock(clock);
let BLOCK = null;
MR.__setLzBcImportBlock(() => BLOCK);
const TOKEN = 'test-runner-token-' + crypto.randomBytes(8).toString('hex');
let TOKEN_ENV = TOKEN;
let READ = (id) => MR.lzExportFileForRunner(id);
const SESS = {
  naka: { authenticated: true, email: 'naka@test', displayName: '中原', role: 'user', allowedApps: ['master-edit', 'purchase-orders'] },
  editorOnly: { authenticated: true, email: 'editor-only@test', displayName: '入力だけ', role: 'user', allowedApps: ['master-edit'] },
  approverOnly: { authenticated: true, email: 'approver-only@test', displayName: '判断だけ', role: 'user', allowedApps: ['master-edit'] },
};
let who = 'naka';
const app = express();
app.set('view engine', 'ejs');
// 機械の口 = セッションの前・どの parser よりも前 (server.js と同じ)
app.use('/apps/lz-bc-import', createLzBcRunnerRouter({ readExportFile: (id) => READ(id), now: clock, token: () => TOKEN_ENV, importBlock: () => BLOCK }));
app.use(express.json({ limit: '10mb' }));   // 後ろの共通の parser (機械の口には届かないこと)
app.use((req, res, next) => { req.session = SESS[who]; next(); });
app.use('/apps/master-edit', MR.default);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const ORIGIN = `http://127.0.0.1:${server.address().port}`;
async function call(method, url, body, { origin = ORIGIN, ct = 'application/json' } = {}) {
  const headers = { Accept: 'application/json' };
  if (ct) headers['Content-Type'] = ct;
  if (origin) headers.Origin = origin;
  const r = await fetch(ORIGIN + '/apps/master-edit' + url, { method, headers, body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(60e3) });
  const text = await r.text();
  let j = null; try { j = JSON.parse(text); } catch { /* */ }
  return { status: r.status, j, text, headers: r.headers };
}
async function mcall(p, body, { token = TOKEN, raw = null, ct = 'application/json' } = {}) {
  const r = await fetch(`${ORIGIN}/apps/lz-bc-import/api${p}`, { method: 'POST', headers: { ...(token ? { Authorization: `Bearer ${token}` } : {}), ...(ct ? { 'Content-Type': ct } : {}) }, body: raw ?? JSON.stringify(body || {}) });
  const text = await r.text();
  let j = null; try { j = JSON.parse(text); } catch { /* */ }
  return { status: r.status, j, text };
}
const VALUES = { standard_price: '3980', cost: { jpy: '1650' }, tax_rate: '0.1', sales_class: '3', primary_supplier: '0001', reorder_months: '2', expiry_managed: '0', inbound_date_managed: '0', shipping_code: 'B2' };
async function regApi(code, lz) {
  const b = { request_id: crypto.randomUUID(), kind: 'single', code, values: { ...VALUES, name: `試し ${code}` }, card: { create: false }, logizard_barcode: lz };
  b.blanks = RB.missingBlankKeys('single', b, { orderApplicable: true });
  const prev = who; who = 'naka';
  try { return await call('POST', '/api/new', b); } finally { who = prev; }
}
/** 新商品を登録して CSV を 1 つ作る (rows 件) */
let seq = 0;
async function newExport(n = 1) {
  const codes = [];
  for (let i = 0; i < n; i++) {
    const code = `lzb-${++seq}`;
    const r = await regApi(code, { kind: 'lz_assign' });
    assert.equal(r.status, 200, `${code}: ${r.text}`);
    codes.push(code);
  }
  const c = await asEditor(() => LZE.lzNewCandidates(db, MIRROR, Date.now()));
  const want = c.ready.filter((x) => codes.includes(x.code)).map((x) => ({ sku_id: x.sku_id, rule_row: x.rule_row }));
  assert.equal(want.length, n);
  const e = await asEditor(() => LZE.issueLzNewExport(db, { requestId: crypto.randomUUID(), actor: 'naka@test', want, mirror: MIRROR }));
  const f = await asEditor(() => LZE.lzNewExportFile(db, e.export_id));
  return { id: e.export_id, rows: n, sha: f.text_sha256, file: f };
}
const runnerSeen = (busy = null) => mcall('/claim', { runner_id: 'test-pc', version: 'v', busy });   // 来た印 (待ちがあれば取る)
const importBody = (e, x = {}) => ({ request_id: crypto.randomUUID(), text_sha256: e.sha, with_export: true, ...x });
const sj = (s) => iconv.encode(s, 'cp932');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
console.log('ロジザードに取り込む (lz-bc-import)');

// ───────────── 帳面の関数 (S) ─────────────
const U = () => BC.openLzBcDb(':memory:');
const H = (c) => crypto.createHash('sha256').update(String(c)).digest('hex');
const base = (x = {}) => ({ requestId: crypto.randomUUID(), exportId: '7', exportState: 'issued', fileSha256: H('f'), textSha256: H('t'), rows: 2, by: 'naka@test', now: jst('10:00'), exportCreatedMs: Date.now() + 1000, ...x });
const alive = (u, t = jst('10:00')) => BC.touchRunner(u, { runnerId: 'pc', now: t });
const err = (fn) => { try { fn(); } catch (e) { return e; } return null; };

await ta('[S1] 積む: miniPC・夜・取り消し・取込の状態・ほかの回・分からない回 = 断る / もう取り込んだ = 理由 / ダウンロード = 確かめ / 押し直し = 前の答え', async () => {
  const u = U();
  assert.equal(err(() => BC.enqueue(u, base())).code, 'runner_offline');
  alive(u);
  for (const t of ['23:45', '23:59', '00:00', '00:30', '01:29']) {
    alive(u, jst(t)); assert.equal(err(() => BC.enqueue(u, base({ now: jst(t) }))).code, 'night', t);
  }
  alive(u, jst('23:44'));
  const okLate = BC.enqueue(u, base({ now: jst('23:44'), exportId: '99' }));
  assert.equal(okLate.state, 'queued');
  BC.cancelJob(u, { jobId: okLate.job_id, by: 'x', now: jst('23:44') });
  alive(u, jst('01:30', '2030-01-11'));
  assert.equal(BC.enqueue(u, base({ now: jst('01:30', '2030-01-11'), exportId: '98' })).state, 'queued', '01:30 から積める');
  const u2 = U(); alive(u2);
  assert.equal(err(() => BC.enqueue(u2, base({ exportState: 'voided' }))).code, 'voided');
  assert.equal(err(() => BC.enqueue(u2, base({ seenTextSha256: H('other') }))).code, 'changed');
  assert.equal(err(() => BC.enqueue(u2, base({ importBlock: { code: 'lz_import_locked', message: '鍵' } }))).code, 'lz_import_locked');
  const rid = crypto.randomUUID();
  const a = BC.enqueue(u2, base({ requestId: rid }));
  assert.deepEqual([a.state, a.replayed], ['queued', false]);
  assert.deepEqual(BC.enqueue(u2, base({ requestId: rid })), { job_id: a.job_id, state: 'queued', replayed: true }, '押し直し = 前の答え');
  assert.equal(err(() => BC.enqueue(u2, base({ requestId: rid, exportId: '8' }))).code, 'request_id_reused');
  assert.equal(err(() => BC.enqueue(u2, base({ requestId: rid, by: 'other@test' }))).code, 'request_id_reused');
  assert.equal(err(() => BC.enqueue(u2, base({ exportId: '8' }))).code, 'busy', '動いているのは 1 つだけ');
  // 終える → もう取り込んだ = 理由が要る
  const c = BC.claimJob(u2, { jobId: a.job_id, runnerId: 'pc', now: jst('10:00') });
  BC.markExecuting(u2, { jobId: a.job_id, claimToken: c.claimToken, now: jst('10:01') });
  BC.reportFinal(u2, { jobId: a.job_id, claimToken: c.claimToken, executed: true, importResult: { status: 'ok', counts: { total: 2, errors: 0 } }, exportResult: { status: 'ok', rows: 9 }, now: jst('10:02') });
  assert.equal(err(() => BC.enqueue(u2, base({ now: jst('10:03') }))).code, 'already_imported');
  assert.equal(err(() => BC.enqueue(u2, base({ now: jst('10:03'), rerunReason: 'abc' }))).code, 'invalid_input', '3 字は短い');
  assert.equal(err(() => BC.enqueue(u2, base({ now: jst('10:03'), rerunReason: 'x'.repeat(201) }))).code, 'invalid_input');
  const again = BC.enqueue(u2, base({ now: jst('10:03'), rerunReason: '入れ直しの試し' }));
  assert.equal(again.state, 'queued');
  BC.cancelJob(u2, { jobId: again.job_id, by: 'naka@test', now: jst('10:03') });
  // ダウンロードした = 確かめが要る
  BC.recordDownload(u2, { exportId: '12', by: 'naka@test', now: jst('10:04') });
  alive(u2, jst('10:04'));
  assert.equal(err(() => BC.enqueue(u2, base({ exportId: '12', now: jst('10:04') }))).code, 'downloaded');
  assert.equal(BC.enqueue(u2, base({ exportId: '12', now: jst('10:04'), confirmNotManual: true })).state, 'queued');
  // ボタンができる前に作った CSV (ダウンロードの記録が無い)・作った時刻が分からない = 確かめが要る (Codex R3 High)
  const u4 = U(); alive(u4);
  for (const ms of [BC.startedAt(u4) - 1, NaN]) {
    const e4 = err(() => BC.enqueue(u4, base({ exportCreatedMs: ms })));
    assert.deepEqual([e4.code, e4.extra.legacy], ['downloaded', true], String(ms));
  }
  assert.equal(BC.enqueue(u4, base({ exportCreatedMs: BC.startedAt(u4) - 1, confirmNotManual: true })).state, 'queued');
  assert.equal(BC.legacyExport(u4, BC.startedAt(u4)), false);
});

await ta('[S2] 状態: 取る → 押す → ① → ② = 取り込んだ / 件数違い・エラー = import_error / 押した後に分からない = unknown / 押す前 = not_executed / 食い違い = unknown', async () => {
  const flow = (fin, { rows = 2, withExport = true, viaImport = false } = {}) => {
    const u = U(); alive(u);
    const a = BC.enqueue(u, base({ rows, withExport }));
    const c = BC.claimJob(u, { jobId: a.job_id, runnerId: 'pc', now: jst('10:00') });
    if (fin.executed !== false) BC.markExecuting(u, { jobId: a.job_id, claimToken: c.claimToken, now: jst('10:01') });
    if (viaImport) BC.reportImport(u, { jobId: a.job_id, claimToken: c.claimToken, importResult: fin.importResult, now: jst('10:02') });
    BC.reportFinal(u, { jobId: a.job_id, claimToken: c.claimToken, now: jst('10:03'), ...fin });
    return u.prepare('select * from bc_jobs where job_id = ?').get(a.job_id);
  };
  let j = flow({ executed: true, importResult: { status: 'ok', summary: 'インポート結果 総件数 : 2 エラー件数 : 0', counts: { total: 2, errors: 0 } }, exportResult: { status: 'ok', rows: 120 } }, { viaImport: true });
  assert.deepEqual([j.state, j.import_total, j.import_errors, j.export_state, j.export_rows], ['imported', 2, 0, 'ok', 120]);
  j = flow({ executed: true, importResult: { status: 'ok', counts: { total: 1, errors: 0 } } });
  assert.deepEqual([j.state, j.export_state], ['import_error', 'skipped'], '総件数が行数と違う');
  assert.match(j.detail, /件数が合いません/);
  j = flow({ executed: true, importResult: { status: 'ok', counts: {} } });
  assert.equal(j.state, 'import_error', '件数を読めない成功の文');
  j = flow({ executed: true, importResult: { status: 'error', counts: { total: 2, errors: 1 } } });
  assert.equal(j.state, 'import_error');
  j = flow({ executed: true, importResult: { status: 'unknown' } });
  assert.equal(j.state, 'unknown');
  j = flow({ executed: true, importResult: null });
  assert.equal(j.state, 'unknown', '押した後に結果が無い');
  j = flow({ executed: false, reason: 'precheck', detail: 'ブラウザが開いている' }, { viaImport: false, ...{} });
  assert.deepEqual([j.state, j.reason], ['not_executed', 'precheck']);
  // 「押す」はポータルで通ったが応答が届かず、miniPC は押す前に止めた = 押していない
  {
    const u = U(); alive(u);
    const a = BC.enqueue(u, base());
    const c = BC.claimJob(u, { jobId: a.job_id, runnerId: 'pc', now: jst('10:00') });
    BC.markExecuting(u, { jobId: a.job_id, claimToken: c.claimToken, now: jst('10:01') });
    assert.deepEqual(Object.values(BC.reportFinal(u, { jobId: a.job_id, claimToken: c.claimToken, executed: false, reason: 'unreachable', now: jst('10:02') })), [true, 'not_executed']);
  }
  j = flow({ executed: true }, { viaImport: false, withExport: true, rows: 2, ...{} });
  assert.equal(j.state, 'unknown');
  // 押すと言っていないのに押したと言う = 食い違い = 分からない側
  {
    const u = U(); alive(u);
    const a = BC.enqueue(u, base());
    const c = BC.claimJob(u, { jobId: a.job_id, runnerId: 'pc', now: jst('10:00') });
    assert.equal(BC.reportFinal(u, { jobId: a.job_id, claimToken: c.claimToken, executed: true, importResult: { status: 'ok', counts: { total: 2, errors: 0 } }, now: jst('10:01') }).state, 'unknown');
  }
  // ② をしない回 = ① の後すぐ取り込んだ・② = skipped
  j = flow({ executed: true, importResult: { status: 'ok', counts: { total: 2, errors: 0 } } }, { withExport: false, viaImport: true });
  assert.deepEqual([j.state, j.export_state], ['imported', 'skipped']);
  // ② をする回で ② が失敗 = 取り込んだ・② failed
  j = flow({ executed: true, importResult: { status: 'ok', counts: { total: 2, errors: 0 } }, exportResult: { status: 'failed', detail: '0件' } }, { viaImport: true });
  assert.deepEqual([j.state, j.export_state, j.export_detail], ['imported', 'failed', '0件']);
});

await ta('[S3] 期限: 待ち 15 分 = expired / 取った後 10 分 = not_executed / 押した後 8 分 = unknown (後から届いた結果で直る) / ② の途中 = 取り込んだ・② 分からない', async () => {
  const u = U(); alive(u);
  let a = BC.enqueue(u, base());
  BC.sweep(u, jst('10:15') + 1);
  assert.equal(u.prepare('select state from bc_jobs where job_id = ?').get(a.job_id).state, 'expired');
  alive(u, jst('10:20'));
  a = BC.enqueue(u, base({ now: jst('10:20') }));
  BC.claimJob(u, { jobId: a.job_id, runnerId: 'pc', now: jst('10:20') });
  const c0 = u.prepare('select claim_token from bc_jobs where job_id = ?').get(a.job_id).claim_token;
  assert.equal(err(() => BC.markExecuting(u, { jobId: a.job_id, claimToken: c0, now: jst('10:30') + 1 })).code, 'bad_state', '期限の後は押させない');
  assert.equal(u.prepare('select state, reason from bc_jobs where job_id = ?').get(a.job_id).state, 'not_executed');
  alive(u, jst('11:00'));
  a = BC.enqueue(u, base({ now: jst('11:00') }));
  const c = BC.claimJob(u, { jobId: a.job_id, runnerId: 'pc', now: jst('11:00') });
  BC.markExecuting(u, { jobId: a.job_id, claimToken: c.claimToken, now: jst('11:01') });
  BC.sweep(u, jst('11:09') + 1);
  assert.equal(u.prepare('select state, reason from bc_jobs where job_id = ?').get(a.job_id).reason, 'no_result');
  // 後から届いた本当の結果 = 直る
  assert.equal(BC.reportFinal(u, { jobId: a.job_id, claimToken: c.claimToken, executed: true, importResult: { status: 'ok', counts: { total: 2, errors: 0 } }, exportResult: { status: 'ok', rows: 3 }, now: jst('11:12') }).state, 'imported');
  alive(u, jst('12:00'));
  a = BC.enqueue(u, base({ now: jst('12:00'), rerunReason: '期限の試し' }));
  const c2 = BC.claimJob(u, { jobId: a.job_id, runnerId: 'pc', now: jst('12:00') });
  BC.markExecuting(u, { jobId: a.job_id, claimToken: c2.claimToken, now: jst('12:00') });
  BC.reportImport(u, { jobId: a.job_id, claimToken: c2.claimToken, importResult: { status: 'ok', counts: { total: 2, errors: 0 } }, now: jst('12:01') });
  assert.equal(u.prepare('select state from bc_jobs where job_id = ?').get(a.job_id).state, 'exporting');
  BC.sweep(u, jst('12:11') + 1);
  assert.deepEqual(Object.values(u.prepare('select state, export_state from bc_jobs where job_id = ?').get(a.job_id)), ['imported', 'unknown']);
});

await ta('[S4] 押す: やめた・夜・取込の状態の止め = 断る (押していないと残る) / 鍵が違う = 403 / 2 回目 = 断る / やめるの段階', async () => {
  const one = (fn, t = jst('10:00')) => {
    const u = U(); alive(u, t);
    const a = BC.enqueue(u, base({ now: t }));
    const c = BC.claimJob(u, { jobId: a.job_id, runnerId: 'pc', now: t });
    return fn(u, a.job_id, c.claimToken);
  };
  one((u, id, tok) => {
    assert.deepEqual(BC.cancelJob(u, { jobId: id, by: 'naka@test', now: jst('10:00') }), { ok: true, state: 'claimed', cancel_requested: true });
    assert.equal(err(() => BC.markExecuting(u, { jobId: id, claimToken: tok, now: jst('10:01') })).code, 'cancelled');
    assert.deepEqual(Object.values(u.prepare('select state, reason from bc_jobs where job_id = ?').get(id)), ['not_executed', 'cancelled'], '断った回は押していないと残る (取引で戻らない)');
  });
  // 押してよい間はポータル (Render の時計) が 23:50 の 5 秒前までに短くする・10 秒も無い = 押させない (Codex R4 Medium)
  one((u, id, tok) => { assert.equal(BC.markExecuting(u, { jobId: id, claimToken: tok, now: jst('23:49') + 30e3 }).press_window_ms, 25e3); }, jst('23:44'));
  one((u, id, tok) => { assert.equal(BC.markExecuting(u, { jobId: id, claimToken: tok, now: jst('10:00') }).press_window_ms, 90e3); });
  one((u, id, tok) => { assert.equal(err(() => BC.markExecuting(u, { jobId: id, claimToken: tok, now: jst('23:49') + 46e3 })).code, 'night'); }, jst('23:44'));
  one((u, id, tok) => {   // 23:44 に積んで取った回を 23:50 に押そうとした
    assert.equal(err(() => BC.markExecuting(u, { jobId: id, claimToken: tok, now: jst('23:50') })).code, 'night');
    assert.equal(u.prepare('select state from bc_jobs where job_id = ?').get(id).state, 'not_executed');
  }, jst('23:44'));
  one((u, id, tok) => {
    assert.equal(err(() => BC.markExecuting(u, { jobId: id, claimToken: tok, importBlock: { code: 'lz_import_in_progress', message: '途中' }, now: jst('10:01') })).code, 'lz_import_in_progress');
  });
  one((u, id, tok) => {
    assert.equal(err(() => BC.markExecuting(u, { jobId: id, claimToken: 'x' + tok.slice(1), now: jst('10:01') })).status, 403);
    assert.equal(BC.markExecuting(u, { jobId: id, claimToken: tok, now: jst('10:01') }).state, 'executing');
    assert.equal(err(() => BC.markExecuting(u, { jobId: id, claimToken: tok, now: jst('10:01') })).code, 'bad_state', '押すのは 1 回だけ');
    assert.equal(err(() => BC.cancelJob(u, { jobId: id, by: 'naka@test', now: jst('10:02') })).code, 'too_late');
  });
});

await ta('[S5] 動いているのは 1 つだけ (表の索引で)・出来事は追記だけ', async () => {
  const u = U();
  const ins = (rid, st) => u.prepare(`insert into bc_jobs (request_id, export_id, file_sha256, text_sha256, rows, with_export, requested_by, requested_at, state) values (?, '1', ?, ?, 1, 1, 'x', 1, ?)`).run(rid, H(1), H(2), st);
  ins('a', 'queued');
  ins('b', 'imported');
  assert.throws(() => ins('c', 'executing'), /UNIQUE/);
  u.prepare("insert into bc_events (job_id, at, kind) values (1, 1, 'x')").run();
  assert.throws(() => u.prepare("update bc_events set kind = 'y'").run(), /追記だけ/);
  assert.throws(() => u.prepare('delete from bc_events').run(), /追記だけ/);
});

await ta('[S6] 取り消し: 動いている・分からない・取り込んだ・エラーのあった回 (一部入った見込み) = 断る / 分からない回を記す / 取り消しの途中 = 積む・渡す・押すを断る / 積んだ CSV はダウンロードさせない', async () => {
  const u = U(); alive(u);
  const a = BC.enqueue(u, base());
  assert.equal(BC.voidBlock(u, '7', jst('10:00')).code, 'import_running');
  const c = BC.claimJob(u, { jobId: a.job_id, runnerId: 'pc', now: jst('10:00') });
  BC.markExecuting(u, { jobId: a.job_id, claimToken: c.claimToken, now: jst('10:00') });
  BC.reportFinal(u, { jobId: a.job_id, claimToken: c.claimToken, executed: true, importResult: { status: 'unknown' }, now: jst('10:01') });
  assert.equal(BC.voidBlock(u, '7', jst('10:01')).code, 'import_unresolved');
  assert.equal(err(() => BC.enqueue(u, base({ now: jst('10:02') }))).code, 'unresolved');
  assert.equal(err(() => BC.resolveJob(u, { jobId: a.job_id, outcome: 'maybe', note: 'みたところ', by: 'n', now: jst('10:02') })).code, 'invalid_input');
  assert.equal(err(() => BC.resolveJob(u, { jobId: a.job_id, outcome: 'imported', note: 'み', by: 'n', now: jst('10:02') })).code, 'invalid_input');
  BC.resolveJob(u, { jobId: a.job_id, outcome: 'imported', note: '履歴 10:00 logizard_bc_upload.csv 2 件', by: 'naka@test', now: jst('10:02') });
  assert.equal(BC.voidBlock(u, '7', jst('10:02')).code, 'imported', '取り込んだと記した = 取り消さない');
  assert.equal(err(() => BC.enqueue(u, base({ now: jst('10:03') }))).code, 'already_imported');
  // エラーのあった回 (2 件中 1 件は入った見込み) = 取り消さない (Codex R1 High)。前に成功した回の後のもう一度がエラーでも同じ
  const u2 = U(); alive(u2);
  const b = BC.enqueue(u2, base());
  const c2 = BC.claimJob(u2, { jobId: b.job_id, runnerId: 'pc', now: jst('10:00') });
  BC.markExecuting(u2, { jobId: b.job_id, claimToken: c2.claimToken, now: jst('10:00') });
  BC.reportFinal(u2, { jobId: b.job_id, claimToken: c2.claimToken, executed: true, importResult: { status: 'error', counts: { total: 2, errors: 1 } }, now: jst('10:01') });
  assert.equal(BC.voidBlock(u2, '7', jst('10:02')).code, 'imported');
  // 押す前に止まった回だけ = 取り消せる
  const u3 = U(); alive(u3);
  const d3 = BC.enqueue(u3, base());
  BC.cancelJob(u3, { jobId: d3.job_id, by: 'naka@test', now: jst('10:00') });
  assert.equal(BC.voidBlock(u3, '7', jst('10:00')), null);
  // 取り消しの途中の印: 確かめと印は 1 つの取引 → 積む・押すを断る / 外すと積める
  const t7 = BC.reserveVoid(u3, { exportId: '7', by: 'naka@test', now: jst('10:00') });
  assert.equal(err(() => BC.enqueue(u3, base({ now: jst('10:00') }))).code, 'voiding');
  BC.releaseVoid(u3, '7', t7);
  const d4 = BC.enqueue(u3, base({ now: jst('10:00') }));
  assert.equal(err(() => BC.reserveVoid(u3, { exportId: '7', by: 'naka@test', now: jst('10:00') })).code, 'import_running', '積んだ後は印を付けない');
  const c4 = BC.claimJob(u3, { jobId: d4.job_id, runnerId: 'pc', now: jst('10:00') });
  u3.prepare("insert into bc_void_holds (export_id, by, token, until) values ('7', 'x', 't', ?)").run(jst('10:05'));   // (もしも) 取った後に印が付いた
  assert.equal(err(() => BC.markExecuting(u3, { jobId: d4.job_id, claimToken: c4.claimToken, now: jst('10:01') })).code, 'voiding');
  assert.equal(u3.prepare('select state from bc_jobs where job_id = ?').get(d4.job_id).state, 'not_executed');
  u3.prepare("delete from bc_void_holds").run();
  // 積んだ・取り込んでいる CSV はダウンロードさせない (手とボタンで 2 回取り込まない。Codex R1 High)
  alive(u3, jst('10:02'));
  BC.enqueue(u3, base({ now: jst('10:02'), exportId: '30' }));
  assert.equal(err(() => BC.recordDownload(u3, { exportId: '30', by: 'naka@test', now: jst('10:02') })).code, 'import_running');
  assert.equal(u3.prepare("select count(*) as n from bc_downloads where export_id = '30'").get().n, 0);
  assert.doesNotThrow(() => BC.recordDownload(u3, { exportId: '31', by: 'naka@test', now: jst('10:02') }));
  // 取り込めたか分からない回が残る CSV = 記すまでダウンロードさせない (手で取り込み直さない。Codex R4 High)
  {
    alive(u3, jst('10:03'));
    BC.cancelJob(u3, { jobId: u3.prepare("select job_id from bc_jobs where export_id = '30'").get().job_id, by: 'x', now: jst('10:03') });
    const q = BC.enqueue(u3, base({ now: jst('10:03'), exportId: '32' }));
    const cq = BC.claimJob(u3, { jobId: q.job_id, runnerId: 'pc', now: jst('10:03') });
    BC.markExecuting(u3, { jobId: q.job_id, claimToken: cq.claimToken, now: jst('10:03') });
    BC.reportFinal(u3, { jobId: q.job_id, claimToken: cq.claimToken, executed: true, importResult: { status: 'unknown' }, now: jst('10:04') });
    assert.equal(err(() => BC.recordDownload(u3, { exportId: '32', by: 'naka@test', now: jst('10:04') })).code, 'import_unresolved');
    BC.resolveJob(u3, { jobId: q.job_id, outcome: 'not_imported', note: '履歴に無かった', by: 'naka@test', now: jst('10:05') });
    assert.doesNotThrow(() => BC.recordDownload(u3, { exportId: '32', by: 'naka@test', now: jst('10:05') }));
  }
});

await ta('[S7] ロジザードの取込の状態の止め (lz-state.js): 鍵・取込の途中・確かめ待ち・手の取込 = 止める / 読めない = 止める / 初期化前・止めてある = 止めない', async () => {
  const cases = [
    [{ initialized: false }, null], [{ initialized: true, state: 'idle', halted: true, manual: { open: false } }, null], [{ initialized: true, state: 'verify_failed', manual: { open: false } }, null],
    [{ initialized: true, state: 'idle', lock: { purpose: 'nightly', expires_at: 1 }, manual: {} }, 'lz_import_locked'], [{ initialized: true, state: 'importing', manual: {} }, 'lz_import_in_progress'],
    [{ initialized: true, state: 'imported_unverified', manual: {} }, 'lz_import_in_progress'], [{ initialized: true, state: 'verified', manual: { open: true } }, 'lz_manual_open'],
  ];
  for (const [st, code] of cases) { LS.__setImportStateReader(() => st); const b = LS.importStateBlock(1); assert.equal(b ? b.code : null, code, JSON.stringify(st)); }
  LS.__setImportStateReader(() => { throw new Error('壊れた'); });
  assert.equal(LS.importStateBlock(1).code, 'lz_state_unreadable');
  LS.__setImportStateReader(null);
});

// ───────────── 本物の口 (I) ─────────────
await ta('[I1] 権限: 両方の名簿の人だけ / Origin・JSON の守り / 機械の口は Bearer (無い = 503・違う = 401・本文を読む前)', async () => {
  const e = await newExport(1);
  await runnerSeen();
  who = 'editorOnly';
  let r = await call('POST', `/api/lz-csv/${e.id}/import`, importBody(e));
  assert.deepEqual([r.status, r.j && r.j.reason], [403, 'not_allowed'], r.text);
  who = 'approverOnly';
  r = await call('POST', `/api/lz-csv/${e.id}/import`, importBody(e));
  assert.deepEqual([r.status, r.j && r.j.reason], [403, 'not_allowed'], r.text);
  assert.match(r.j.error, /MASTER_EDITORS/);
  who = 'naka';
  r = await call('POST', `/api/lz-csv/${e.id}/import`, importBody(e), { origin: null });
  assert.deepEqual([r.status, r.j && r.j.error], [403, 'origin_mismatch']);
  r = await call('POST', `/api/lz-csv/${e.id}/import`, importBody(e), { origin: 'https://evil.example' });
  assert.equal(r.status, 403);
  r = await call('POST', `/api/lz-csv/${e.id}/import`, importBody(e), { ct: 'text/plain' });
  assert.equal(r.status, 415);
  r = await call('POST', `/api/lz-csv/${e.id}/import`, { ...importBody(e), text_sha256: 'x' });
  assert.equal(r.status, 400);
  // 機械の口
  assert.equal((await mcall('/claim', {}, { token: null })).status, 401);
  assert.equal((await mcall('/claim', {}, { token: 'wrong' })).status, 401);
  assert.equal((await mcall('/claim', null, { token: null, raw: 'x'.repeat(300 * 1024) })).status, 401, '大きい本文でも認証の前に読まない (413 にならない)');
  assert.equal((await mcall('/claim', null, { raw: 'x', ct: 'text/plain' })).status, 415);
  assert.equal((await mcall('/nothing', {})).status, 404);
  TOKEN_ENV = '';
  assert.equal((await mcall('/claim', {})).status, 503);
  TOKEN_ENV = TOKEN;
  // 画面の口を機械の token で叩いても通らない (セッションの名簿)
  who = 'editorOnly';
  r = await call('POST', `/api/lz-csv/import/1/cancel`, {});
  assert.equal(r.status, 403);
  who = 'naka';
});

let E_MAIN = null;
await ta('[I2] 本物の流れ: 積む → 取る (帳面と同じ byte 列) → 押す → 結果 → 画面に件数とログ → もう一度は理由 / ダウンロードした = 確かめ / 取り込んだ CSV は取り消さない', async () => {
  const e = await newExport(2);
  E_MAIN = e;
  NOW = jst('10:10');   // 来た印 (I1 の 10:00) から 3 分より後 = 来ていない
  let r = await call('POST', `/api/lz-csv/${e.id}/import`, importBody(e));
  assert.deepEqual([r.status, r.j && r.j.reason], [409, 'runner_offline'], r.text);
  assert.match(r.j.error, /miniPC がポータルに来ていません/);
  assert.equal((await runnerSeen()).j.job, null);
  const body = importBody(e);
  r = await call('POST', `/api/lz-csv/${e.id}/import`, body);
  assert.equal(r.status, 200, r.text);
  const jobId = r.j.job_id;
  assert.equal((await call('POST', `/api/lz-csv/${e.id}/import`, body)).j.replayed, true, '押し直し = 前の答え');
  const other = await newExport(1);
  r = await call('POST', `/api/lz-csv/${other.id}/import`, importBody(other));
  assert.deepEqual([r.status, r.j.reason], [409, 'busy']);
  r = await call('POST', `/api/lz-csv/${e.id}/void`, { reason: '試しの取り消し' });
  assert.deepEqual([r.status, r.j.reason], [409, 'import_running'], '取り込みの途中は取り消せない');
  // miniPC が取る
  const cl = await runnerSeen();
  assert.equal(cl.status, 200, cl.text);
  assert.equal(cl.j.job.job_id, jobId);
  const bytes = Buffer.from(cl.j.file_b64, 'base64');
  assert.ok(bytes.equals(e.file.file_bytes), '帳面のファイルと 1 byte も違わない');
  assert.equal(crypto.createHash('sha256').update(bytes).digest('hex'), cl.j.job.file_sha256);
  assert.equal(cl.j.job.file_name, 'logizard_bc_upload.csv');
  assert.doesNotThrow(() => RUN.verifyJobFile(bytes, cl.j.job, cl.j.job.file_sha256), 'miniPC の確かめを通る');
  const tok = cl.j.claim_token;
  assert.equal((await mcall(`/jobs/${jobId}/executing`, { claim_token: 'bad' })).status, 403);
  assert.equal((await mcall(`/jobs/${jobId}/executing`, { claim_token: tok })).status, 200);
  assert.equal((await mcall(`/jobs/${jobId}/import`, { claim_token: tok, import_result: { status: 'ok', summary: 'インポート結果 総件数 : 2 処理件数 : 2 エラー件数 : 0', counts: { total: 2, processed: 2, errors: 0 } }, log: '① ok\n' })).j.state, 'exporting');
  const fin = await mcall(`/jobs/${jobId}/final`, { claim_token: tok, executed: true, import_result: { status: 'ok', counts: { total: 2, errors: 0 } }, export_result: { status: 'ok', rows: 5100 }, log: '② ok\n' });
  assert.equal(fin.j.state, 'imported', fin.text);
  assert.equal((await mcall(`/jobs/${jobId}/final`, { claim_token: tok, executed: true })).j.already, true, '送り直し = もう書いてある');
  // 画面の口の様子
  const st = await call('GET', `/api/lz-csv/import-status?ids=${e.id},${other.id}`);
  assert.equal(st.status, 200);
  assert.deepEqual([st.j.latest.state, st.j.latest.import_total, st.j.latest.import_errors, st.j.latest.export_state, st.j.latest.export_rows], ['imported', 2, 0, 'ok', 5100]);
  assert.match(st.j.latest.log, /① ok[\s\S]*② ok/);
  assert.equal(st.j.byExport[e.id].imported_job, jobId);
  assert.equal(st.j.runner.alive, true);
  // もう一度 = 理由
  r = await call('POST', `/api/lz-csv/${e.id}/import`, importBody(e));
  assert.deepEqual([r.status, r.j.reason], [409, 'already_imported']);
  r = await call('POST', `/api/lz-csv/${e.id}/void`, { reason: '試しの取り消し' });
  assert.deepEqual([r.status, r.j.reason], [409, 'imported'], '取り込んだ CSV は取り消さない');
  // ボタンができる前に作った CSV = 画面の口でも確かめが要る (帳面を始めた時刻を先へずらして試す)
  {
    const L = await newExport(1);
    const bdb = BC.getLzBcDb();
    const saved = BC.startedAt(bdb);
    bdb.prepare("update bc_meta set value = ? where key = 'started_at'").run(String(Date.now() + 3600e3));
    try {
      r = await call('POST', `/api/lz-csv/${L.id}/import`, importBody(L));
      assert.deepEqual([r.status, r.j.reason, r.j.legacy], [409, 'downloaded', true], r.text);
      const pg2 = await call('GET', '/lz-csv');
      const d2 = JSON.parse(/<script type="application\/json" id="me-lzc">([\s\S]*?)<\/script>/.exec(pg2.text)[1].replace(/\\u003c/g, '<'));
      assert.equal(d2.bc.byExport[L.id].legacy, true, '画面にも「前に作った」の印');
    } finally { bdb.prepare("update bc_meta set value = ? where key = 'started_at'").run(String(saved)); }
  }
  // ダウンロードした CSV = 「手で取り込んでいない」が要る
  const d = await fetch(`${ORIGIN}/apps/master-edit/lz-csv/${other.id}/file`);
  assert.equal(d.status, 200);
  r = await call('POST', `/api/lz-csv/${other.id}/import`, importBody(other));
  assert.deepEqual([r.status, r.j.reason], [409, 'downloaded']);
  r = await call('POST', `/api/lz-csv/${other.id}/import`, importBody(other, { confirm_not_manual: true, with_export: false }));
  assert.equal(r.status, 200, r.text);
  // 積んだ後はダウンロードさせない (Codex R1 High)
  const d2 = await fetch(`${ORIGIN}/apps/master-edit/lz-csv/${other.id}/file`);
  assert.equal(d2.status, 409);
  assert.equal((await d2.json()).reason, 'import_running');
  assert.equal((await call('POST', `/api/lz-csv/import/${r.j.job_id}/cancel`, {})).j.state, 'cancelled');
  // 取り消し (押す前にやめた回だけ = 取り消せる) → 印は残って積めない (Company DB でも取り消し済み)
  const vv = await newExport(1);
  r = await call('POST', `/api/lz-csv/${vv.id}/void`, { reason: '試しの取り消し' });
  assert.equal(r.status, 200, r.text);
  r = await call('POST', `/api/lz-csv/${vv.id}/import`, importBody(vv));
  assert.equal(r.status, 409);
  // 取り消しが DB で断られた (理由なし) = 印を外す (積める)
  const vw = await newExport(1);
  r = await call('POST', `/api/lz-csv/${vw.id}/void`, { reason: '' });
  assert.equal(r.status, 400, r.text);
  assert.equal(BC.voidHeld(BC.getLzBcDb(), vw.id, NOW), false, '取り消せなかった = 印を外した');
  // 画面
  const page = await call('GET', '/lz-csv');
  for (const w of ['id="lzc-bc"', 'ロジザードへの取り込み (miniPC)', `data-bc-cell="${e.id}"`, 'id="lzc-manual"', 'id="lzc-gas"', 'me-lzcsv.js']) assert.ok(page.text.includes(w), `画面に「${w}」が無い`);
  const data = JSON.parse(/<script type="application\/json" id="me-lzc">([\s\S]*?)<\/script>/.exec(page.text)[1].replace(/\\u003c/g, '<'));
  assert.equal(data.canImport, true);
  assert.equal(data.bc.byExport[e.id].imported_job, jobId);
  assert.ok(!/claim_token/.test(page.text), '画面に鍵を出さない');
});

await ta('[I3] すり替え: 積んだ後に取り消した・中身が違う・取込の状態が止め = 渡さない (押していない) / Company DB に届かない = 待ちのまま', async () => {
  NOW = jst('11:00');
  await runnerSeen();
  // 取り消した (画面の取り消しは断るので、持ち主で直に)
  const e = await newExport(1);
  let r = await call('POST', `/api/lz-csv/${e.id}/import`, importBody(e));
  assert.equal(r.status, 200, r.text);
  await asEditor(() => LZE.voidLzNewExport(db, { actor: 'naka@test', exportId: e.id, reason: '直に取り消す' }));
  let cl = await runnerSeen();
  assert.deepEqual([cl.j.job, cl.j.why], [null, 'refused']);
  let st = (await call('GET', '/api/lz-csv/import-status')).j;
  assert.deepEqual([st.latest.state, st.latest.reason], ['not_executed', 'voided']);
  // 中身が違う
  const e2 = await newExport(1);
  r = await call('POST', `/api/lz-csv/${e2.id}/import`, importBody(e2));
  assert.equal(r.status, 200);
  READ = async (id) => { const f = await MR.lzExportFileForRunner(id); return { ...f, file_bytes: Buffer.concat([f.file_bytes, Buffer.from('\r\nX,Y')]) }; };
  cl = await runnerSeen();
  assert.equal(cl.j.why, 'refused');
  st = (await call('GET', '/api/lz-csv/import-status')).j;
  assert.deepEqual([st.latest.state, st.latest.reason], ['not_executed', 'file_changed']);
  // Company DB に届かない = 待ちのまま (渡さない)
  r = await call('POST', `/api/lz-csv/${e2.id}/import`, importBody(e2));
  assert.equal(r.status, 200, r.text);
  READ = async () => { throw new Error('つながらない'); };
  cl = await runnerSeen();
  assert.deepEqual([cl.j.job, cl.j.why], [null, 'db_unreachable']);
  READ = (id) => MR.lzExportFileForRunner(id);
  // 取込の状態の止め = 渡さない (待ちのまま) → 外れたら渡す → 押す前にまた止め = 押させない
  BLOCK = { code: 'lz_import_locked', message: '毎晩の取込の鍵' };
  cl = await runnerSeen();
  assert.deepEqual([cl.j.job, cl.j.why], [null, 'lz_import_locked']);
  BLOCK = null;
  cl = await runnerSeen();
  assert.ok(cl.j.job, cl.text);
  BLOCK = { code: 'lz_import_in_progress', message: '取込の途中' };
  const ex = await mcall(`/jobs/${cl.j.job.job_id}/executing`, { claim_token: cl.j.claim_token });
  assert.deepEqual([ex.status, ex.j.error], [409, 'lz_import_in_progress']);
  BLOCK = null;
  r = await call('POST', `/api/lz-csv/${e2.id}/import`, importBody(e2));
  assert.equal(r.status, 200, '押していない回の後はまた積める');
  await call('POST', `/api/lz-csv/import/${r.j.job_id}/cancel`, {});
  // 画面の口でも止めを見る
  BLOCK = { code: 'lz_manual_open', message: '手の取込が開いている' };
  r = await call('POST', `/api/lz-csv/${e2.id}/import`, importBody(e2));
  assert.deepEqual([r.status, r.j.reason], [409, 'lz_manual_open']);
  BLOCK = null;
});

// ───────────── miniPC の 1 回 (R・偽物の session) ─────────────
const RTMP = fs.mkdtempSync(path.join(os.tmpdir(), 'lzbc-runner-'));
let rseq = 0;
function runnerEnv() {
  const d = path.join(RTMP, `r${++rseq}`);
  fs.mkdirSync(d, { recursive: true });
  let imported = {};
  return { logDir: path.join(d, 'logs'), workDir: path.join(d, 'work'), imported: { read: () => imported, record: (i) => { imported = { import1: i }; } }, peek: () => imported };
}
const client = () => createBcImportClient({ url: ORIGIN, token: TOKEN });
async function queue(e, x = {}) {
  await runnerSeen();
  const r = await call('POST', `/api/lz-csv/${e.id}/import`, importBody(e, x));
  assert.equal(r.status, 200, r.text);
  return r.j.job_id;
}
const jobRow = (id) => BC.getLzBcDb().prepare('select * from bc_jobs where job_id = ?').get(id);
const noExit = { onExitHook: () => {}, offExitHook: () => {}, sleep: async () => {} };

await ta('[R] miniPC の 1 回: 成功 / 押すを断られた = 押さない / 押した後に止まった = unknown → 記す / エラー / ファイルの確かめ / この PC の取込済み / 届かない = 残して次に送る / 忙しい / 途中で終わった', async () => {
  NOW = jst('13:00');
  // 成功 (① → ②)
  let e = await newExport(1);
  let id = await queue(e);
  let env = runnerEnv();
  const calls = [];
  const okSession = async (o) => {
    calls.push(['file', fs.readFileSync(o.csvPath).equals(e.file.file_bytes), o.withExport]);
    await o.beforeExecute('①');
    calls.push(['press']);
    await o.onImported({ status: 'SUCCESS', summary: 'インポート結果 総件数 : 1 エラー件数 : 0', counts: STEPS.parseImportSummary('インポート結果 総件数 : 1 処理件数 : 1 処理不要件数 : 0 エラー件数 : 0') });
    console.log('② 書き出しの偽物');
    return { export: { status: 'SUCCESS', rows: 4321 } };
  };
  let out = await RUN.runOnce({ client: client(), ...env, localBusy: () => null, session: okSession, log: () => {}, ...noExit });
  assert.deepEqual([out.state, out.executed, calls[0], calls[1]], ['ran', true, ['file', true, true], ['press']]);
  let j = jobRow(id);
  assert.deepEqual([j.state, j.import_total, j.import_errors, j.export_state, j.export_rows], ['imported', 1, 0, 'ok', 4321]);
  assert.match(j.log, /ファイルの確かめ[\s\S]*「押す」を知らせた[\s\S]*② 書き出しの偽物/);
  assert.equal(env.peek().import1.sha256, e.file.file_sha256, 'この PC の取込済みの記録 (CLI も飛ばす)');
  // この PC で同じ中身を取り込んだ = もう一度でなければ押さない
  id = await queue(e, { rerun_reason: 'もう一度の試し' });
  out = await RUN.runOnce({ client: client(), ...env, localBusy: () => null, session: okSession, log: () => {}, ...noExit });
  assert.equal(jobRow(id).state, 'imported', 'もう一度 (理由つき) は押す');
  {
    const u = BC.getLzBcDb();
    // 同じ中身を前に取り込んだ記録があり、帳面では初めて (rerun でない) = 押さない
    const e3 = await newExport(1);
    const id3 = await queue(e3);
    const env3 = runnerEnv();
    env3.imported.record({ sha256: e3.file.file_sha256, importedAt: '前' });
    let pressed = false;
    out = await RUN.runOnce({ client: client(), ...env3, localBusy: () => null, session: async (o) => { pressed = true; await o.beforeExecute(); }, log: () => {}, ...noExit });
    assert.deepEqual([pressed, jobRow(id3).state, jobRow(id3).reason], [false, 'not_executed', 'local_already_imported']);
    assert.ok(u);
  }
  // 押すを断られた (やめると言われた) = 押さない
  e = await newExport(1);
  id = await queue(e);
  let pressed = 0;
  const cancelSession = async (o) => {
    await call('POST', `/api/lz-csv/import/${id}/cancel`, {});   // 取った後・押す前に人がやめた
    await o.beforeExecute('①');
    pressed++;
  };
  out = await RUN.runOnce({ client: client(), ...runnerEnv(), localBusy: () => null, session: cancelSession, log: () => {}, ...noExit });
  assert.deepEqual([pressed, out.executed, jobRow(id).state, jobRow(id).reason], [0, false, 'not_executed', 'cancelled']);
  // 押した後に止まった (セッション切れ) = unknown → 画面で記す
  e = await newExport(1);
  id = await queue(e);
  out = await RUN.runOnce({ client: client(), ...runnerEnv(), localBusy: () => null, session: async (o) => { await o.beforeExecute(); const x = new Error('取込中にセッション切れ'); x.importUnknown = true; throw x; }, log: () => {}, ...noExit });
  assert.deepEqual([jobRow(id).state, out.executed], ['unknown', true]);
  let r = await call('POST', `/api/lz-csv/${e.id}/import`, importBody(e));
  assert.deepEqual([r.status, r.j.reason], [409, 'unresolved']);
  r = await call('POST', `/api/lz-csv/import/${id}/resolve`, { outcome: 'not_imported', note: '履歴に 13:00 の取込なし' });
  assert.equal(r.status, 200, r.text);
  assert.equal((await call('POST', `/api/lz-csv/${e.id}/import`, importBody(e))).status, 200, '取り込んでいないと記した = 積める');
  // ロジザードのエラー = import_error (② はしない)
  out = await RUN.runOnce({ client: client(), ...runnerEnv(), localBusy: () => null, session: async (o) => { await o.beforeExecute(); const x = new Error('取込エラー'); x.importResult = { status: 'error', summary: '総件数 : 1 エラー件数 : 1', counts: { total: 1, errors: 1 } }; throw x; }, log: () => {}, ...noExit });
  assert.deepEqual([jobRow(out.job_id).state, jobRow(out.job_id).import_errors, jobRow(out.job_id).export_state], ['import_error', 1, 'skipped']);
  // ファイルの確かめ (すり替えた byte 列) = 押さない
  e = await newExport(1);
  id = await queue(e);
  const tamper = client();
  const realClaim = tamper.claim;
  tamper.claim = async (b) => { const x = await realClaim(b); if (x.job) x.file_b64 = Buffer.from(Buffer.from(x.file_b64, 'base64').toString('latin1').replace('lzb', 'LZB'), 'latin1').toString('base64'); return x; };
  let ran = false;
  out = await RUN.runOnce({ client: tamper, ...runnerEnv(), localBusy: () => null, session: async () => { ran = true; }, log: () => {}, ...noExit });
  assert.deepEqual([ran, jobRow(id).state, jobRow(id).reason], [false, 'not_executed', 'file_check']);
  assert.match(jobRow(id).detail, /sha256 が帳面と違う/);
  // 終わりの知らせが届かない = 残して次の回に送る (送れるまで新しい回は取らない)
  e = await newExport(1);
  id = await queue(e);
  env = runnerEnv();
  const flaky = client();
  const realFinal = flaky.final;
  let down = true;
  flaky.final = async (jid, b) => { if (down) { const x = new Error('届かない'); x.code = 'unreachable'; throw x; } return realFinal(jid, b); };
  out = await RUN.runOnce({ client: flaky, ...env, localBusy: () => null, session: okSession, log: () => {}, ...noExit });
  assert.equal(jobRow(id).state, 'exporting', '① の結果は途中の知らせで届いている');
  assert.ok(fs.existsSync(path.join(env.logDir, 'bc-import-pending', `job-${id}.json`)));
  out = await RUN.runOnce({ client: flaky, ...env, localBusy: () => null, session: okSession, log: () => {}, ...noExit });
  assert.equal(out.state, 'pending_unsent', '送れないうちは取らない');
  down = false;
  out = await RUN.runOnce({ client: flaky, ...env, localBusy: () => null, session: okSession, log: () => {}, ...noExit });
  assert.deepEqual([out.state, jobRow(id).state, jobRow(id).export_state], ['idle', 'imported', 'ok']);
  assert.ok(!fs.existsSync(path.join(env.logDir, 'bc-import-pending', `job-${id}.json`)));
  // 忙しい = 取らない (画面に理由)
  e = await newExport(1);
  id = await queue(e);
  out = await RUN.runOnce({ client: client(), ...runnerEnv(), localBusy: () => 'ほかのロジザードの作業中 (logizard-session.lock)', session: okSession, log: () => {}, ...noExit });
  assert.deepEqual([out.state, out.why, jobRow(id).state], ['idle', 'busy', 'queued']);
  assert.match((await call('GET', '/api/lz-csv/import-status')).j.runner.busy, /logizard-session\.lock/);
  // 途中で process が終わった = 終わりの知らせを残す (押したかどうかもそのまま)
  env = runnerEnv();
  let hook = null;
  out = await RUN.runOnce({ client: client(), ...env, localBusy: () => null, onExitHook: (f) => { hook = f; }, offExitHook: () => {}, sleep: async () => {}, log: () => {},
    session: async (o) => { await o.beforeExecute(); hook(); throw new Error('ここで process.exit したことにする'); } });
  const pend = JSON.parse(fs.readFileSync(path.join(env.logDir, 'bc-import-pending', `job-${id}.json`), 'utf8'));
  assert.deepEqual([pend.body.executed, pend.body.reason], [true, 'process_exit']);
  assert.equal(jobRow(id).state, 'unknown', '本当の終わりの知らせ = 押した後に止まった');
  // 「押す」の後に押してよい間 (ポータルの press_window_ms) を過ぎた = 押さない (止まった miniPC が後で押さない。Codex R1 High)
  {
    e = await newExport(1);
    id = await queue(e);
    let local = Date.now();
    let seen = null;
    out = await RUN.runOnce({ client: client(), ...runnerEnv(), localBusy: () => null, log: () => {}, ...noExit, now: () => local,
      session: async (o) => {
        assert.equal(o.pressGuard('ログインの前'), 30000, '「押す」の前は夜の止めの持ち時間だけ');
        await o.beforeExecute();
        seen = o.pressGuard('①の実行ボタンの前');
        local += 90e3;   // ここで 90 秒止まった
        o.pressGuard('①の確認の OK の前');
      } });
    assert.ok(seen > 80e3 && seen <= 85e3, String(seen));
    assert.match(jobRow(id).detail, /押してよい間を過ぎた/);
  }
  // ① の件数が合わない = ② はしない (② は skipped・ポータルは import_error)
  e = await newExport(1);
  id = await queue(e);
  let exported = false;
  out = await RUN.runOnce({ client: client(), ...runnerEnv(), localBusy: () => null, log: () => {}, ...noExit,
    session: async (o) => { await o.beforeExecute(); await o.onImported({ summary: '総件数 : 2 エラー件数 : 0', counts: { total: 2, errors: 0 } }); exported = true; return { export: { status: 'SUCCESS', rows: 1 } }; } });
  assert.deepEqual([exported, jobRow(id).state, jobRow(id).export_state], [false, 'import_error', 'skipped']);
  // ポータルに送るログは byte で切る (日本語でも本文の上限に入る)
  const big = 'あ'.repeat(40000);
  assert.ok(Buffer.byteLength(RUN.tailBytes(big, RUN.LOG_BYTES), 'utf8') <= RUN.LOG_BYTES + 40);
  assert.equal(RUN.tailBytes('短い', 100), '短い');
  // 23:50 の手前で「押す」が通った = 押してよいのは 23:50 の 2 秒前まで (ポータルが押させない時間をまたがない。Codex R3 Medium)
  {
    NOW = jst('23:44');   // 積めるのは 23:45 の前
    e = await newExport(1);
    id = await queue(e);
    NOW = jst('23:49');   // 渡す・押すのは 23:50 の前
    let local = jst('23:49') + 50e3;   // この PC の時計 23:49:50
    let first = null;
    out = await RUN.runOnce({ client: client(), ...runnerEnv(), localBusy: () => null, log: () => {}, ...noExit, now: () => local,
      session: async (o) => { await o.beforeExecute(); first = o.pressGuard('①の実行ボタンの前'); local += 9e3; o.pressGuard('①の確認の OK の前'); } });
    assert.ok(first > 0 && first <= 8000, String(first));
    assert.match(jobRow(id).detail, /押してよい間を過ぎた/);
    assert.deepEqual([RUN.msUntilNightBusy(jst('23:49')), RUN.msUntilNightBusy(jst('23:50')), RUN.msUntilNightBusy(jst('01:30'))], [60e3, 0, (22 * 60 + 20) * 60e3]);
    NOW = jst('13:30');
  }
  // この PC の時計がずれていても、押してよい間はポータル (Render の時計) の 23:50 の手前で切れる (Codex R4 Medium)
  {
    NOW = jst('23:44');
    e = await newExport(1);
    id = await queue(e);
    NOW = jst('23:49') + 30e3;   // Render 23:49:30 = 押してよい間 25 秒
    let local = jst('21:00');   // この PC は 2 時間 50 分 遅れている (自分の時計では夜まで遠い)
    let first = null;
    out = await RUN.runOnce({ client: client(), ...runnerEnv(), localBusy: () => null, log: () => {}, ...noExit, now: () => local,
      session: async (o) => { await o.beforeExecute(); first = o.pressGuard('①の実行ボタンの前'); local += 21e3; o.pressGuard('①の確認の OK の前'); } });
    assert.ok(first > 19e3 && first <= 20e3, String(first));
    assert.match(jobRow(id).detail, /押してよい間を過ぎた/);
    NOW = jst('13:30');
  }
  // 夜・セッションの鍵 (この PC の印)
  assert.deepEqual(['23:49', '23:50', '00:20', '01:29', '01:30'].map((t) => RUN.nightBusy(jst(t))), [false, true, true, true, false]);
  // ほかの定時の前後 (毎時 00 分の在庫・08:30 の値札は鍵があると待たずに終わる)
  assert.deepEqual(['10:49', '10:50', '11:00', '11:02', '11:03', '08:25', '08:26', '08:34', '08:35'].map((t) => !!RUN.quietBusy(jst(t))), [false, true, true, true, false, false, true, true, false]);
  const ld = fs.mkdtempSync(path.join(RTMP, 'lock-'));
  assert.equal(RUN.sessionLockBusy(ld), null);
  fs.writeFileSync(path.join(ld, 'logizard-session.lock'), JSON.stringify({ pid: process.pid }));
  assert.match(RUN.sessionLockBusy(ld), /logizard-session\.lock/);
  fs.writeFileSync(path.join(ld, 'logizard-session.lock'), JSON.stringify({ pid: 999999 }));
  const old = new Date(Date.now() - 31 * 60e3); fs.utimesSync(path.join(ld, 'logizard-session.lock'), old, old);
  assert.equal(RUN.sessionLockBusy(ld), null, '古くて持ち主のいない鍵 = 使われていない (acquireLock が片付ける)');
  // ファイルの確かめ (見出し・行数)
  const job = { file_sha256: null, rows: 1 };
  const bad = sj('商品ID,バーコード\r\nA,1');
  job.file_sha256 = crypto.createHash('sha256').update(bad).digest('hex');
  assert.throws(() => RUN.verifyJobFile(bad, job, job.file_sha256), /見出しがロジザード登録の CSV と違う/);
  const good = sj(`${STEPS.LZ_BC_HEADER.join(',')}\r\nA,あ,あ,1,01,0,0001,`);
  job.file_sha256 = crypto.createHash('sha256').update(good).digest('hex');
  assert.equal(RUN.verifyJobFile(good, job, job.file_sha256).dataRows, 1);
  assert.throws(() => RUN.verifyJobFile(good, { ...job, rows: 2 }, job.file_sha256), /行数が帳面と違う/);
  assert.deepEqual(STEPS.parseImportSummary('インポート結果 総件数 : 12 処理件数 : 10 処理不要件数 : 2 エラー件数 : 0'), { total: 12, processed: 10, unneeded: 2, errors: 0 });
});

// ───────────── 本物の barcode-steps.js の ① (P・偽物のロジザードの画面) ─────────────
// native = 実行ボタンの後にブラウザの確認 (confirm) を出す形 (承認しない = SUSPENDED へ = 結果は分からない側ですぐ終わる)
const PM07 = (total, errors, native = false, slow = false) => `<html><body><a onclick="openFunctionBar('FM07_01')" href="#">import</a>
<div id="FM07_01_FORM"><select id="FM07_01_fileId"><option value="">-</option><option value="5">商品マスタ</option></select>
<select id="FM07_01_ptrnId"><option value="">-</option><option value="1">新商品バーコード登録</option></select>
<input type="file" id="FM07_01_impFile"><input type="button" id="FM07_01_executeBtn" value="実行"${slow ? ' disabled' : ''}></div>
<div id="cfm" class="ui-dialog" style="display:none">ファイルアップロードを開始します <input type="button" value="OK" id="cfmOk"></div>
<div id="res" class="ui-dialog" style="display:none">インポート結果 総件数 : ${total} 処理件数 : ${total} 処理不要件数 : 0 エラー件数 : ${errors} <input type="button" value="OK" onclick="this.parentNode.style.display='none'"></div>
<script>function openFunctionBar() {}
${slow ? "window.__enableExec = () => { document.getElementById('FM07_01_executeBtn').disabled = false; };" : ''}
document.getElementById('FM07_01_executeBtn').onclick = async () => { await fetch('/LPSTD405/mock/exec', { method: 'POST' });
  if (${native ? 'true' : 'false'}) { if (confirm('ファイルアップロードを開始します。よろしいですか')) { await fetch('/LPSTD405/mock/ok', { method: 'POST' }); document.getElementById('res').style.display = 'block'; } else { location.href = '/LPSTD405/SUSPENDED'; } return; }
  document.getElementById('cfm').style.display = 'block'; };
document.getElementById('cfmOk').onclick = async () => { document.getElementById('cfm').style.display = 'none'; await fetch('/LPSTD405/mock/ok', { method: 'POST' }); document.getElementById('res').style.display = 'block'; };
</script></body></html>`;
await ta('[P] 本物の barcode-steps.js の ①: 実行ボタンの前にポータルへ「押す」・断られたら押さない・結果の件数を読む (偽物のロジザード・ほかへの通信は止める)', async () => {
  const BM = await import('../tools/logizard-automation/barcode-mode.js');
  if (BM.nearNightBlock(new Date()) || RUN.nightBusy(Date.now())) { console.log('      (いまは夜 = 本物の時計で押さない時間。この試験はとばす)'); passed--; return; }
  // 部品は一時フォルダ (リポジトリの中 = playwright-core を読める) に写して使う (エラーの画面の写しを正本のフォルダに残さない)
  const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
  const tmp = fs.mkdtempSync(path.join(ROOT, '.tmp-lzbc-steps-'));
  for (const f of ['barcode-steps.js', 'barcode-mode.js', 'logizard-common.js', 'csv-util.js']) fs.copyFileSync(path.join(ROOT, 'tools', 'logizard-automation', f), path.join(tmp, f));
  const ST = await import(pathToFileURL(path.join(tmp, 'barcode-steps.js')).href);
  const { chromium } = await import('playwright');
  const browser = await chromium.launch({ headless: true });
  try {
    const mk = async (total, errors, { native = false, onExec = () => {}, slow = false } = {}) => {
      const ctx = await browser.newContext();
      const st = { exec: 0, ok: 0, other: [] };
      await ctx.route('**/*', async (route) => {
        const u = new URL(route.request().url());
        if (u.host !== 'ap003.logizard.net') { st.other.push(u.href); return route.abort(); }
        if (u.pathname === '/LPSTD405/PM07/Index') return route.fulfill({ status: 200, contentType: 'text/html; charset=utf-8', body: PM07(total, errors, native, slow) });
        if (u.pathname === '/LPSTD405/SUSPENDED') return route.fulfill({ status: 200, contentType: 'text/html; charset=utf-8', body: '<html><body>suspended</body></html>' });
        if (u.pathname === '/LPSTD405/mock/exec') { st.exec++; onExec(); return route.fulfill({ status: 200, body: 'ok' }); }
        if (u.pathname === '/LPSTD405/mock/ok') { st.ok++; return route.fulfill({ status: 200, body: 'ok' }); }
        st.other.push(u.href); return route.abort();
      });
      return { page: await ctx.newPage(), st, ctx };
    };
    const sessionWith = (m) => async (o) => {
      const steps = ST.createBarcodeSteps({ page: m.page, cfg: {}, beforeExecute: o.beforeExecute, pressGuard: o.pressGuard });
      const imp = await steps.runImport(o.csvPath, ST.IMPORT1_PATTERN_DEFAULT, '①新商品バーコード登録');
      await o.onImported(imp);
      return { export: null };
    };
    NOW = jst('14:00');
    // 成功: 押す (ポータル) → 実行 → OK → 結果 総件数 1 エラー 0 = 取り込んだ
    let e = await newExport(1);
    let id = await queue(e, { with_export: false });
    let m = await mk(1, 0);
    const order = [];
    const c = client();
    const realExec = c.executing;
    c.executing = async (jid, tok) => { order.push(['portal', m.st.exec]); return realExec(jid, tok); };
    let out = await RUN.runOnce({ client: c, ...runnerEnv(), localBusy: () => null, session: sessionWith(m), log: () => {}, ...noExit });
    assert.deepEqual(order, [['portal', 0]], 'ポータルへの「押す」は実行ボタンより前');
    assert.deepEqual([m.st.exec, m.st.ok, m.st.other], [1, 1, []]);
    let j = jobRow(id);
    assert.deepEqual([out.executed, j.state, j.import_total, j.import_errors, j.export_state], [true, 'imported', 1, 0, 'skipped'], JSON.stringify(j));
    await m.ctx.close();
    // ポータルが断る (取込の状態の止め) = 実行ボタンを押さない
    e = await newExport(1);
    id = await queue(e, { with_export: false });
    m = await mk(1, 0);
    const c2 = client();
    const realExec2 = c2.executing;
    c2.executing = async (jid, tok) => { BLOCK = { code: 'lz_import_locked', message: '毎晩の取込が鍵を取った' }; try { return await realExec2(jid, tok); } finally { BLOCK = null; } };
    out = await RUN.runOnce({ client: c2, ...runnerEnv(), localBusy: () => null, session: sessionWith(m), log: () => {}, ...noExit });
    assert.deepEqual([m.st.exec, m.st.ok, out.executed, jobRow(id).state, jobRow(id).reason], [0, 0, false, 'not_executed', 'lz_import_locked']);
    await m.ctx.close();
    // ロジザードの結果にエラー = import_error
    e = await newExport(1);
    id = await queue(e, { with_export: false });
    m = await mk(1, 1);
    out = await RUN.runOnce({ client: client(), ...runnerEnv(), localBusy: () => null, session: sessionWith(m), log: () => {}, ...noExit });
    j = jobRow(id);
    assert.deepEqual([m.st.exec, j.state, j.import_errors], [1, 'import_error', 1]);
    assert.ok(fs.readdirSync(path.join(tmp, 'error-shots')).some((f) => /bc-import-error/.test(f)), 'エラーの画面は写しのフォルダに残る');
    await m.ctx.close();
    // ブラウザの確認 (confirm) が出るまでに「押してよい間」を過ぎた = 承認しない (Codex R2 High)
    e = await newExport(1);
    id = await queue(e, { with_export: false });
    let local = Date.now();
    m = await mk(1, 0, { native: true, onExec: () => { local += 100e3; } });
    const logs = [];
    out = await RUN.runOnce({ client: client(), ...runnerEnv(), localBusy: () => null, session: sessionWith(m), log: () => {}, ...noExit, now: () => local });
    assert.deepEqual([m.st.exec, m.st.ok, out.executed, jobRow(id).state], [1, 0, true, 'unknown'], '確認を承認しない = 取込は始まっていない (押した後なので分からない側)');
    assert.match(jobRow(id).log, /押してよい間を過ぎたので dialog を承認しない/);
    await m.ctx.close();
    // 間の中なら承認する (同じ形で時計を進めない = 取り込んだ)
    e = await newExport(1);
    id = await queue(e, { with_export: false });
    m = await mk(1, 0, { native: true });
    out = await RUN.runOnce({ client: client(), ...runnerEnv(), localBusy: () => null, session: sessionWith(m), log: () => {}, ...noExit });
    assert.deepEqual([m.st.exec, m.st.ok, jobRow(id).state], [1, 1, 'imported']);
    assert.ok(logs);
    await m.ctx.close();
    // 確かめの後に node が止まり、古い持ち時間で押し始めても、ページの中で押した瞬間の時刻を見て捨てる (Codex R4 High)
    {
      m = await mk(1, 0, { slow: true });
      const csv = path.join(tmp, 'stall.csv');
      fs.writeFileSync(csv, sj(`${STEPS.LZ_BC_HEADER.join(',')}\r\nA,あ,あ,1,01,0,0001,`));
      let deadline = null;
      const guard = () => 30000;   // 古い (止まる前に計った) 持ち時間
      guard.deadlineEpoch = () => deadline;
      const steps = ST.createBarcodeSteps({ page: m.page, cfg: {}, pressGuard: guard,
        beforeExecute: async () => {
          deadline = Date.now() + 500;   // 押してよいのは 0.5 秒だけ
          setTimeout(() => { m.page.evaluate(() => window.__enableExec()).catch(() => {}); }, 1500);   // ボタンが押せるようになるのは 1.5 秒後 (= 締め切りの後)
        } });
      const er = await steps.runImport(csv, ST.IMPORT1_PATTERN_DEFAULT, '①新商品バーコード登録').then(() => null, (x) => x);
      assert.ok(er && er.pressStop, String(er && er.message));
      assert.match(er.message, /ページの中で止めた/);
      assert.deepEqual([m.st.exec, m.st.ok], [0, 0], '実行ボタンの処理まで届いていない');
      await m.ctx.close();
    }
  } finally {
    await browser.close();
    fs.rmSync(tmp, { recursive: true, force: true, maxRetries: 10, retryDelay: 500 });   // この試験が作った一時フォルダだけ
  }
});

// ───────────── 画面 (U・本物のブラウザ) ─────────────
await ta('[U] 画面: miniPC が来ていない = 押せない (理由) / 来ている = 押せる → 確かめの欄 → 積む → 様子 → 取り込んだ・もう一度 / 夜 = 押せない / 分からない回を記す', async () => {
  const { chromium } = await import('playwright');
  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage();
    const errs = [];
    page.on('pageerror', (x) => errs.push(String(x)));
    NOW = jst('15:00');   // 来た印が切れている (前の回は 14:00 台)
    const e = await newExport(1);
    await page.goto(`${ORIGIN}/apps/master-edit/lz-csv`);
    const btn = page.locator(`button[data-bc-import="${e.id}"]`);
    await btn.waitFor();
    assert.equal(await btn.isDisabled(), true, '来ていない = 押せない');
    assert.match(await page.locator(`[data-bc-why="${e.id}"]`).textContent(), /miniPC が来ていません/);
    assert.equal(await page.locator('[data-bc-runner]').getAttribute('data-bc-runner'), 'offline');
    await runnerSeen();
    await page.reload();
    await btn.waitFor();
    assert.equal(await btn.isDisabled(), false);
    assert.equal(await btn.textContent(), 'ロジザードに取り込む');
    await btn.click();
    const form = page.locator(`tr[data-bc-form="${e.id}"]`);
    await form.waitFor();
    assert.equal(await form.locator(`#bc-nm-${e.id}`).count(), 0, 'ダウンロードしていない = 手の確かめは出ない');
    assert.equal(await form.locator(`#bc-exp-${e.id}`).isChecked(), true, '② は既定で書き出す');
    await form.locator(`button[data-bc-go="${e.id}"]`).click();
    await page.locator(`[data-bc-cell="${e.id}"] [data-bc-state="queued"]`).waitFor({ timeout: 10000 });
    assert.equal(await page.locator(`[data-bc-cell="${e.id}"] button[data-bc-cancel]`).count(), 1, '待ちの間は「やめる」');
    // ほかの CSV のボタンは止まる
    const e2 = await newExport(1);
    await page.reload();
    await page.locator(`button[data-bc-import="${e2.id}"]`).waitFor();
    assert.equal(await page.locator(`button[data-bc-import="${e2.id}"]`).isDisabled(), true);
    assert.match(await page.locator(`[data-bc-why="${e2.id}"]`).textContent(), /ほかの取り込み/);
    // miniPC が取り込む → 画面が読み直して「取り込みました」・もう一度
    const out = await RUN.runOnce({ client: client(), ...runnerEnv(), localBusy: () => null, log: () => {}, ...noExit,
      session: async (o) => { await o.beforeExecute(); await o.onImported({ summary: 'インポート結果 総件数 : 1 エラー件数 : 0', counts: { total: 1, errors: 0 } }); return { export: { status: 'SUCCESS', rows: 77 } }; } });
    assert.equal(jobRow(out.job_id).state, 'imported');
    await page.locator(`[data-bc-cell="${e.id}"] [data-bc-state="imported"]`).waitFor({ timeout: 15000 });
    assert.equal(await page.locator(`button[data-bc-import="${e.id}"]`).textContent(), 'もう一度取り込む');
    assert.match(await page.locator(`[data-bc-result="${out.job_id}"]`).textContent(), /総件数 1 · エラー件数 0/);
    // もう一度 = 理由が要る
    await page.locator(`button[data-bc-import="${e.id}"]`).click();
    await page.locator(`#bc-rr-${e.id}`).waitFor();
    await page.locator(`button[data-bc-go="${e.id}"]`).click();
    assert.match(await page.locator(`[data-bc-form-msg="${e.id}"]`).textContent(), /理由を 4 字以上/);
    // ダウンロードした CSV = 手の確かめ
    await fetch(`${ORIGIN}/apps/master-edit/lz-csv/${e2.id}/file`);
    await page.reload();
    await page.locator(`button[data-bc-import="${e2.id}"]`).click();
    await page.locator(`#bc-nm-${e2.id}`).waitFor();
    await page.locator(`button[data-bc-go="${e2.id}"]`).click();
    assert.match(await page.locator(`[data-bc-form-msg="${e2.id}"]`).textContent(), /手で取り込んでいない/);
    // 夜 = 押せない
    NOW = jst('23:50');
    await runnerSeen();
    await page.reload();
    await page.locator(`button[data-bc-import="${e2.id}"]`).waitFor();
    assert.equal(await page.locator(`button[data-bc-import="${e2.id}"]`).isDisabled(), true);
    assert.match(await page.locator(`[data-bc-why="${e2.id}"]`).textContent(), /23:45〜01:30/);
    assert.equal(await page.locator('[data-bc-night]').count(), 1);
    // 分からない回 = 下の欄で記す
    NOW = jst('16:00', '2030-01-11');
    const id = await queue(e2, { confirm_not_manual: true });
    await RUN.runOnce({ client: client(), ...runnerEnv(), localBusy: () => null, log: () => {}, ...noExit, session: async (o) => { await o.beforeExecute(); throw new Error('取込の完了表示を確認できず'); } });
    assert.equal(jobRow(id).state, 'unknown');
    await page.reload();
    await page.locator(`[data-bc-resolve-form="${id}"]`).waitFor();   // 最後の回 = 下の③にも出る
    // 後からほかの CSV の回が来た (最後の回でなくなった) = その CSV の行で記す (Codex R1 Medium)
    const e3 = await newExport(1);
    const id3 = await queue(e3);
    await call('POST', `/api/lz-csv/import/${id3}/cancel`, {});
    await page.reload();
    const rf = page.locator(`[data-bc-resolve-cell="${id}"]`);
    await rf.waitFor();
    assert.equal(await page.locator(`[data-bc-resolve-form="${id}"]`).count(), 0, '下の③は最後の回だけ');
    assert.match(await page.locator(`[data-bc-why="${e2.id}"]`).textContent(), /取り込めたか分からない/);
    await rf.locator('input[value="imported"]').check();
    await rf.locator('textarea').fill('履歴 16:00 logizard_bc_upload.csv 1 件 エラー 0');
    await page.waitForTimeout(31000);   // 読み直し (30 秒) の後も打ったメモが残る
    assert.equal(await rf.locator('textarea').inputValue(), '履歴 16:00 logizard_bc_upload.csv 1 件 エラー 0');
    await rf.locator(`button[data-bc-resolve-cellbtn="${id}"]`).click();
    await page.waitForFunction((jid) => !document.querySelector(`[data-bc-resolve-cell="${jid}"]`), id, { timeout: 10000 });
    assert.equal(jobRow(id).resolved_outcome, 'imported');
    assert.equal(await page.locator(`button[data-bc-import="${e2.id}"]`).textContent(), 'もう一度取り込む');
    // 信用しない値は文字のまま (ログに HTML)
    assert.deepEqual(errs, [], errs.join('\n'));
  } finally { await browser.close(); }
});

await ta('[D] 配り方・台帳・つかいかた: miniPC に runner と手順の部品・Stream Deck に barcode-steps.js / 台帳 lz-bc-import-runner / server.js は parser の前に機械の口 / つかいかた', async () => {
  const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
  const m = JSON.parse(fs.readFileSync(path.join(ROOT, 'tools', 'logizard-automation', 'manifest.json'), 'utf8'));
  for (const f of ['bc-import-runner.js', 'bc-import-client.js', 'barcode-steps.js', 'barcode-mode.js', 'logizard-common.js', 'csv-util.js', 'run-bc-import-poll.bat']) assert.ok(m.pcs.minipc.includes(f), `minipc: ${f}`);
  assert.ok(m.pcs.streamdeck.includes('barcode-steps.js'), 'Stream Deck の auto-barcode.js が読む部品');
  const bat = fs.readFileSync(path.join(ROOT, 'tools', 'logizard-automation', 'run-bc-import-poll.bat'), 'latin1');
  assert.ok(bat.includes('node bc-import-runner.js') && !/[^\x00-\x7f]/.test(bat), 'bat は ASCII');
  const { JOBS_REGISTRY, validateRegistry } = await import('../config/jobs-registry.mjs');
  const ent = JOBS_REGISTRY.find((x) => x.id === 'lz-bc-import-runner');
  assert.ok(ent && ent.type === 'heartbeat' && /Logizard-BcImport/.test(ent.where) && /bc-import\.log/.test(ent.runbook));
  assert.deepEqual(validateRegistry(), []);
  assert.ok(fs.readFileSync(path.join(ROOT, 'tools', 'logizard-automation', 'bc-import-runner.js'), 'utf8').includes("'-Id', 'lz-bc-import-runner'"), 'ping の id が台帳と同じ');
  const s = fs.readFileSync(path.join(ROOT, 'server.js'), 'utf8');
  const at = s.indexOf("app.use('/apps/lz-bc-import', createLzBcRunnerRouter(");
  assert.ok(at > 0 && at < s.indexOf('const urlencodedParser = express.urlencoded(') && at < s.indexOf('return globalJsonParser(req, res, next);') && at < s.indexOf('const sessionMiddleware = session({'), 'どの parser・セッションよりも前');
  assert.ok(s.includes("if (normalizedPath.toLowerCase().startsWith('/apps/lz-bc-import')) return next();"));
  const guard = s.lastIndexOf("if (process.env.MASTER_EDIT_ENABLED === '1' && (process.env.PORTAL_VARIANT || 'render').toLowerCase() === 'render') {", at);
  assert.ok(guard > 0 && !s.slice(guard, at).includes('}'), 'マスタの入力と同じ Render だけ');
  const man = await call('GET', '/manual');
  for (const w of ['id="lz-import"', 'ロジザードに取り込む', '取り込めたか分かりません', 'インポート履歴', '23:45〜01:30', '両方に入っている人', 'ロジザード登録の CSV</a>」の画面で作ります', 'CSV に出した商品を GAS からも出さない']) assert.ok(man.text.includes(w), `つかいかたに「${w}」が無い`);
});

server.close();
console.log(`\n${passed} 件 ok${process.exitCode ? ' (NG あり)' : ''}`);
process.exit(process.exitCode || 0);
