/**
 * test-logizard-import-admin.mjs — ロジザードの取込の状態の「画面の口」(apps/logizard-import-state/admin-router.js。マスタ正本切替 ③c-1b-3b-4a)
 *
 * 固定する契約 (設計 = AI_reference CompanyDB構想/10 §6.3「③c-1b-3b 契約 (v4 + 設計 R1)」K3-3・K3-4・K3-6・K3-7):
 *   1 門: ログインなし = JSON の 401・管理者でない = 403・機械の口の Bearer では開かない / 機械の口 (/api) はセッションでは開かない
 *   2 書く口: Origin = Host だけ・Content-Type を口ごとに固定・本文は門の後に読む (大きな本文でも 401 / 403 が先)
 *   3 手の取込の流れ: 始める (毎晩の成果物) → CSV のダウンロード (同じバイト列・attachment・no-store・nosniff) → 終える (照合が合う = completed_ok) → 再開
 *      誰 = セッションのメール (本文の by は使わない)
 *   4 照合が合わない = needs_review → 知らせをすぐ送る → 確認 (ack) まで再開できない
 *   5 GAS の CSV (移行の段階だけ・バイト列・大きすぎる = 413)・設定・waiver・解除・unknown
 *   6 知らせを送れない = outbox に残る (定時の入口が送り直す)・旗が無い = 手の取込は disabled
 * 使い方: node scripts/test-logizard-import-admin.mjs
 */
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';

process.env.LZ_MANUAL_V4 = 'on';
const S = await import('../apps/logizard-import-state/store.js');
const { createImportStateRouter } = await import('../apps/logizard-import-state/router.js');
const { createAdminRouter, adminApiGate, adminPageGate, renderAdminPage } = await import('../apps/logizard-import-state/admin-router.js');
const C = await import('../tools/logizard-automation/import-state-client.js');
const CLI = await import('../tools/logizard-automation/import-state-cli.js');
const LZC = await import('../apps/master-decisions/lz-import-check.mjs');
const { default: express } = await import('express');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const MIN = 60000;
const T0 = Date.UTC(2030, 0, 16, 3, 0);   // JST 2030-01-16 12:00 (分ちょうど)
const csvOf = (ids) => LZC.buildLosslessCsv(ids.map((id) => [id, `名前${id}`, 'なまえ', '100', '0001'])).bytes;
const shaOf = (b) => createHash('sha256').update(b).digest('hex');
const RESULT_OK = (n) => `インポート結果 総件数 : ${n} 処理件数 : ${n} 処理不要件数 : 0 エラー件数 : 0`;

/** server.js と同じ並び: 機械の口 (前置き全体) → フォームの parser (この前置きは読まない) → セッション (試験は見出しで) → 画面の口 (門 → router) */
async function withApp(fn, { notify = async () => true } = {}) {
  const db = S.openImportStateDb(':memory:');
  let clock = T0;
  const sent = [];
  const app = express();
  app.use('/apps/logizard-import-state', createImportStateRouter({ getDb: () => db, now: () => clock, token: () => 'tok' }));
  const urlencodedParser = express.urlencoded({ extended: true, limit: '1kb' });
  app.use((req, res, next) => (String(req.path || '').toLowerCase().startsWith('/apps/logizard-import-state') ? next() : urlencodedParser(req, res, next)));
  app.use((req, res, next) => {   // 試験のセッション: x-test-user = admin / staff / なし
    const u = req.headers['x-test-user'];
    req.session = u ? { authenticated: true, email: `${u}@b-faith.biz`, role: u === 'admin' ? 'admin' : 'user' } : {};
    next();
  });
  app.use('/apps/logizard-import-state/admin-api', adminApiGate, createAdminRouter({ getDb: () => db, now: () => clock, notify: async (t) => { sent.push(t); return notify(t); } }));
  app.get('/apps/logizard-import-state/admin', adminPageGate, renderAdminPage);   // server.js と同じ門と画面
  const srv = await new Promise((resolve) => { const s = app.listen(0, '127.0.0.1', () => resolve(s)); });
  const base = `http://127.0.0.1:${srv.address().port}`;
  const host = `127.0.0.1:${srv.address().port}`;
  const api = (p, { method = 'GET', user = 'admin', body, type = 'application/json', origin = `http://${host}`, headers = {} } = {}) => fetch(`${base}/apps/logizard-import-state/admin-api${p}`, {
    method, headers: { ...(user ? { 'x-test-user': user } : {}), ...(method !== 'GET' && origin ? { Origin: origin } : {}), ...(body !== undefined ? { 'Content-Type': type } : {}), ...headers },
    body: body === undefined ? undefined : (Buffer.isBuffer(body) || typeof body === 'string' ? body : JSON.stringify(body)),
  });
  const j = async (p, o) => { const r = await api(p, o); return { status: r.status, body: await r.json() }; };
  const rev = async () => (await j('/status')).body.status.halt_revision;   // 画面が見ている止めの番号
  try { await fn({ db, base, api, j, rev, sent, tick: (ms) => { clock += ms; }, now: () => clock }); } finally { await new Promise((r) => srv.close(r)); }
}
const artifact = (db, ids = ['A-1', 'B-2'], id = 'lzd_20300115_a') => {
  const buf = csvOf(ids);
  S.putArtifact(db, { sourceRunId: id, targetAsOf: '2030-01-15', verdict: 'pass', csvBuf: buf, sha256: shaOf(buf), rows: ids.length, by: 'lz-daily', now: T0 });
  return buf;
};

console.log('test-logizard-import-admin');

await ta('[1] 門: ログインなし = 401 (JSON)・管理者でない = 403・機械の口の Bearer では開かない / 機械の口 (/api) はセッションでは開かない', async () => {
  await withApp(async ({ db, api, j, base }) => {
    S.init(db, { by: 'x', now: T0 });
    let r = await j('/status', { user: null });
    assert.deepEqual([r.status, r.body.error], [401, 'session_expired']);
    r = await j('/status', { user: 'staff' });
    assert.deepEqual([r.status, r.body.error], [403, 'forbidden']);
    r = await j('/status', { user: null, headers: { Authorization: 'Bearer tok' } });
    assert.equal(r.status, 401);
    r = await j('/halt', { method: 'POST', user: 'staff', body: { reason: '止めたい理由' } });
    assert.equal(r.status, 403);
    assert.equal(S.getStatus(db).halted, false);
    // 機械の口はセッションでは開かない (Bearer が要る)
    const m = await fetch(`${base}/apps/logizard-import-state/api/status`, { headers: { 'x-test-user': 'admin' } });
    assert.equal(m.status, 401);
    r = await j('/status');
    assert.deepEqual([r.status, r.body.ok, r.body.status.state], [200, true, 'idle']);
  });
});

await ta('[2] 書く口: Origin = Host だけ (無い・違う・壊れた = 403)・Content-Type を固定 (415)・本文は門の後 (大きな本文でも 401 / 403 が先)', async () => {
  await withApp(async ({ db, j, api }) => {
    S.init(db, { by: 'x', now: T0 });
    for (const origin of [null, 'http://evil.example', 'not a url']) {
      const r = await j('/halt', { method: 'POST', body: { reason: '止めたい理由' }, origin });
      assert.deepEqual([r.status, r.body.error], [403, 'origin_mismatch'], String(origin));
    }
    let r = await j('/halt', { method: 'POST', body: 'reason=x', type: 'application/x-www-form-urlencoded' });
    assert.equal(r.status, 415);
    r = await j('/halt', { method: 'POST', body: '{bad json' });
    assert.deepEqual([r.status, r.body.error, JSON.stringify(r.body).includes('bad json')], [400, 'bad_json', false]);
    const big = 'x'.repeat(200 * 1024);
    let res = await api('/halt', { method: 'POST', user: null, body: JSON.stringify({ reason: big }) });
    assert.equal(res.status, 401);
    res = await api('/halt', { method: 'POST', user: 'staff', body: JSON.stringify({ reason: big }) });
    assert.equal(res.status, 403);
    res = await api('/halt', { method: 'POST', body: JSON.stringify({ reason: big }) });
    assert.equal(res.status, 413);
    assert.equal(S.getStatus(db).halted, false);
  });
});

await ta('[3] 手の取込の流れ: 止める (知らせをすぐ送る) → 成果物で始める (誰 = セッション) → CSV のダウンロード (同じバイト列・attachment・no-store・nosniff) → 終える (completed_ok) → 再開', async () => {
  await withApp(async ({ db, j, api, rev, sent, tick, now }) => {
    S.init(db, { by: 'x', now: T0 });
    const buf = artifact(db);
    let r = await j('/settings', { method: 'POST', body: { key: 'lz_accounts', value: ['nakahara'], by: 'attacker' } });
    assert.deepEqual([r.status, r.body.value], [200, ['nakahara']]);
    r = await j('/halt', { method: 'POST', body: { reason: '自動の取込がおかしい', by: 'attacker' } });
    assert.deepEqual([r.status, r.body.halted, r.body.notified, sent.length], [200, true, true, 1]);
    assert.match(sent[0], /止めた \(admin@b-faith\.biz\)/);
    assert.ok(sent[0].includes('画面 ▶ https://bfaith-portal.onrender.com/apps/logizard-import-state/admin'), '知らせから画面を開ける (ダッシュボードのカードは作らない)');
    assert.deepEqual(S.outboxPending(db), []);   // 送れた = 送れた印
    r = await j('/manual/open', { method: 'POST', body: { lz_account: 'nakahara', source_run_id: 'lzd_20300115_a', by: 'attacker', expected_halt_revision: await rev() } });
    assert.equal(r.status, 200, JSON.stringify(r.body));
    const m = r.body;
    assert.deepEqual([m.rows, m.csv_sha256, m.source_run_id], [2, shaOf(buf), 'lzd_20300115_a']);
    assert.equal(S.getManualSession(db, { sessionId: m.session_id }).opened_by, 'admin@b-faith.biz');   // 本文の by は使わない
    const d = await api(`/manual/${m.session_id}/csv`);
    assert.equal(d.status, 200);
    assert.ok(Buffer.from(await d.arrayBuffer()).equals(buf), '成果物と同じバイト列');
    assert.deepEqual([d.headers.get('content-disposition'), d.headers.get('cache-control'), d.headers.get('x-content-type-options'), d.headers.get('content-type')],
      [`attachment; filename="${m.download_name}"`, 'no-store', 'nosniff', 'application/octet-stream']);
    assert.equal((await api('/manual/lzm_nothing/csv')).status, 404);
    r = await j('/status');
    assert.deepEqual([r.body.manual_sessions.open.session_id, r.body.manual_sessions.open.opened_by, r.body.pending.count, r.body.settings.lz_accounts], [m.session_id, 'admin@b-faith.biz', 2, ['nakahara']]);
    tick(3 * MIN);
    r = await j(`/manual/${m.session_id}/complete`, { method: 'POST', body: { result_text: RESULT_OK(2), history: { file_name: m.download_name, at: T0 + MIN, account: 'nakahara' }, note: '取り込んだ' } });
    assert.deepEqual([r.status, r.body.status, r.body.mismatches], [200, 'completed_ok', []]);
    r = await j('/resume', { method: 'POST', body: { expected_halt_revision: await rev(), note: '自動を直したので再開' } });
    assert.deepEqual([r.status, r.body.halted], [200, false]);
    // 管理者の見え方には出来事の中身も (機械の口には出さない)
    r = await j('/status');
    assert.ok(r.body.status.events.some((e) => e.kind === 'manual_open' && e.by === 'admin@b-faith.biz'));
    assert.ok(S.getStatus(db, { now: now() }).events.some((e) => e.kind === 'manual_open' && e.by === null));
  });
});

await ta('[4] 照合が合わない = needs_review → 知らせをすぐ送る → 確認 (ack) まで再開できない / 知らせを送れない = outbox に残る', async () => {
  let fail = false;
  await withApp(async ({ db, j, api, rev, sent, tick }) => {
    S.init(db, { by: 'x', now: T0 });
    artifact(db);
    S.setSetting(db, { key: 'lz_accounts', value: ['nakahara'], by: 'x', now: T0 });
    await j('/halt', { method: 'POST', body: { reason: '自動の取込がおかしい' } });
    const m = (await j('/manual/open', { method: 'POST', body: { lz_account: 'nakahara', source_run_id: 'lzd_20300115_a', expected_halt_revision: await rev() } })).body;
    tick(MIN);
    fail = true;   // この後の知らせは送れない
    let r = await j(`/manual/${m.session_id}/complete`, { method: 'POST', body: { result_text: RESULT_OK(2), history: { file_name: 'logizard_shohinmaster_upload.csv', at: T0 + MIN, account: 'nakahara' } } });
    assert.deepEqual([r.status, r.body.status, r.body.mismatches, r.body.notified], [200, 'needs_review', ['file_name'], false]);
    assert.deepEqual(S.outboxPending(db).map((o) => o.kind), ['manual_review']);   // 送れない = 残る (定時の入口が送り直す)
    assert.ok(sent.some((t) => t.includes(m.session_id)));
    r = await j('/resume', { method: 'POST', body: { expected_halt_revision: await rev(), note: '再開したい' } });
    assert.deepEqual([r.status, r.body.error], [409, 'needs_review']);
    r = await j(`/manual/${m.session_id}/ack`, { method: 'POST', body: { note: '' } });
    assert.equal(r.status, 400);
    r = await j(`/manual/${m.session_id}/ack`, { method: 'POST', body: { note: 'ロジザードの履歴を見た' } });
    assert.equal(r.status, 200);
    assert.equal((await j('/resume', { method: 'POST', body: { expected_halt_revision: await rev(), note: '確認したので再開' } })).status, 200);
    // 形の誤り
    for (const b of [{ result_text: '' }, { result_text: RESULT_OK(2), history: null }, { result_text: RESULT_OK(2), history: { file_name: 'x', at: T0 + 1234, account: 'nakahara' } }]) {
      assert.equal((await j(`/manual/${m.session_id}/complete`, { method: 'POST', body: b })).status, 400, JSON.stringify(b));
    }
  }, { notify: async () => !fail });
});

await ta('[5] GAS の CSV (移行の段階だけ・バイト列・JSON は 415・大きすぎる 413・一方通行) / 取り消し・waiver・解除・unknown / 旗が無い = 手の取込は disabled', async () => {
  await withApp(async ({ db, j, api, rev, tick }) => {
    S.init(db, { by: 'x', now: T0 });
    S.setSetting(db, { key: 'lz_accounts', value: ['nakahara'], by: 'x', now: T0 });
    await j('/halt', { method: 'POST', body: { reason: '手で取り込む' } });
    const gas = csvOf(['A-1', 'C-3']);
    let r = await j(`/manual/open-gas?lz_account=nakahara&target_as_of=2030-01-16&expected_halt_revision=${await rev()}`, { method: 'POST', body: gas, type: 'application/octet-stream' });
    assert.deepEqual([r.status, r.body.error], [409, 'gas_closed']);   // 設定が無い = cutover
    assert.equal((await j('/settings', { method: 'POST', body: { key: 'cutover_phase', value: 'transition' } })).status, 200);
    r = await j(`/manual/open-gas?lz_account=nakahara&target_as_of=2030-01-16&expected_halt_revision=${await rev()}`, { method: 'POST', body: { csv: 'x' } });
    assert.equal(r.status, 415);
    const big = await api(`/manual/open-gas?lz_account=nakahara&target_as_of=2030-01-16&expected_halt_revision=${await rev()}`, { method: 'POST', body: Buffer.alloc(S.LIMITS.csvBytes + 1, 0x41), type: 'application/octet-stream' });
    assert.equal(big.status, 413);
    r = await j(`/manual/open-gas?lz_account=nakahara&target_as_of=2030-01-16&target_as_of=2030-01-15&expected_halt_revision=${await rev()}`, { method: 'POST', body: gas, type: 'application/octet-stream' });
    assert.equal(r.status, 400);   // 同じ名前が 2 つ = 無い扱い
    r = await j(`/manual/open-gas?lz_account=nakahara&target_as_of=2030-01-16&expected_halt_revision=${await rev()}`, { method: 'POST', body: gas, type: 'application/octet-stream' });
    assert.deepEqual([r.status, r.body.source_kind, r.body.rows], [200, 'gas_upload', 2]);
    assert.equal((await j(`/manual/${r.body.session_id}/cancel`, { method: 'POST', body: { note: 'ロジザードに置かなかった' } })).status, 200);
    // waiver (特定の義務)
    const ids = S.listPending(db).items.map((o) => o.id);
    r = await j('/waive', { method: 'POST', body: { obligation_ids: [ids[0]], note: 'Company DB の対象外' } });
    assert.deepEqual([r.status, r.body.waived, S.listPending(db).count], [200, 1, 1]);
    // 一方通行
    assert.equal((await j('/settings', { method: 'POST', body: { key: 'cutover_phase', value: 'cutover' } })).status, 200);
    r = await j('/settings', { method: 'POST', body: { key: 'cutover_phase', value: 'transition' } });
    assert.deepEqual([r.status, r.body.error], [409, 'one_way']);
    // 解除・unknown (自動の回を作って)
    await j('/resume', { method: 'POST', body: { expected_halt_revision: await rev(), note: '取り消したので再開' } });
    const L = S.acquire(db, { initId: S.getStatus(db).init_id, holder: 'auto', purpose: 'import', runId: 'lzim_a', ttlSec: 60, by: 'auto', now: T0 });
    S.transition(db, { lockToken: L.lock_token, runId: 'lzim_a', to: 'importing', detail: { csv_sha256: 'a'.repeat(64), rows: 2, mode: 'test', target_as_of: '2030-01-16' }, by: 'auto', now: T0 });
    r = await j('/mark-unknown', { method: 'POST', body: { run_id: 'lzim_a', reason: '鍵が切れた' } });
    assert.deepEqual([r.status, r.body.error], [409, 'busy']);   // 鍵が生きている間はしない
    tick(2 * MIN);
    assert.equal((await j('/mark-unknown', { method: 'POST', body: { run_id: 'lzim_a', reason: '鍵が切れた' } })).status, 200);
    r = await j('/resolve', { method: 'POST', body: { run_id: 'lzim_a', outcome: 'not_imported', note: 'ロジザードの履歴を見た' } });
    assert.deepEqual([r.status, r.body.state], [200, 'idle']);
    // 旗が無い = 手の取込は disabled
    delete process.env.LZ_MANUAL_V4;
    try {
      artifact(db);
      await j('/halt', { method: 'POST', body: { reason: '旗なしで止める' } });
      r = await j('/manual/open', { method: 'POST', body: { lz_account: 'nakahara', source_run_id: 'lzd_20300115_a', expected_halt_revision: await rev() } });
      assert.deepEqual([r.status, r.body.error], [409, 'disabled']);
    } finally { process.env.LZ_MANUAL_V4 = 'on'; }
  });
});

await ta('[6] メモ・理由は文字で 500 字まで (オブジェクト・配列・501 字 = 400) / 似た型 (application/json-patch+json・octet-stream+csv) = 415 (Codex #1541 R1)', async () => {
  await withApp(async ({ db, j, api, rev }) => {
    S.init(db, { by: 'x', now: T0 });
    S.setSetting(db, { key: 'lz_accounts', value: ['nakahara'], by: 'x', now: T0 });
    for (const v of [{ a: 1 }, ['理由の配列'], 'x'.repeat(501), 12345]) {
      assert.equal((await j('/halt', { method: 'POST', body: { reason: v } })).status, 400, JSON.stringify(v).slice(0, 30));
      assert.equal((await j('/resume', { method: 'POST', body: { note: v } })).status, 400);
      assert.equal((await j('/resolve', { method: 'POST', body: { run_id: 'lzim_x', outcome: 'imported', note: v } })).status, 400);
      assert.equal((await j('/mark-unknown', { method: 'POST', body: { run_id: 'lzim_x', reason: v } })).status, 400);
      assert.equal((await j('/waive', { method: 'POST', body: { obligation_ids: [1], note: v } })).status, 400);
    }
    assert.equal(S.getStatus(db).halted, false);
    await j('/halt', { method: 'POST', body: { reason: '手で取り込む' } });
    artifact(db);
    const m = (await j('/manual/open', { method: 'POST', body: { lz_account: 'nakahara', source_run_id: 'lzd_20300115_a', expected_halt_revision: await rev() } })).body;
    for (const v of [{ a: 1 }, ['x'], 'x'.repeat(501)]) {
      assert.equal((await j(`/manual/${m.session_id}/cancel`, { method: 'POST', body: { note: v } })).status, 400);
      assert.equal((await j(`/manual/${m.session_id}/complete`, { method: 'POST', body: { result_text: RESULT_OK(2), history: { file_name: m.download_name, at: T0 + MIN, account: 'nakahara' }, note: v } })).status, 400);
    }
    for (const type of ['application/json-patch+json', 'application/jsonx', 'text/json']) {
      assert.equal((await j('/halt', { method: 'POST', body: JSON.stringify({ reason: '止めたい理由' }), type })).status, 415, type);
    }
    assert.equal((await j('/halt', { method: 'POST', body: JSON.stringify({ reason: '止めたい理由' }), type: 'application/json; charset=utf-8' })).status, 200);
    assert.equal((await j(`/manual/open-gas?lz_account=nakahara&target_as_of=2030-01-16&expected_halt_revision=${await rev()}`, { method: 'POST', body: csvOf(['A-1']), type: 'application/octet-stream+csv' })).status, 415);
  });
});

await ta('[7] 今回の知らせを真っ先に送る (前の知らせが 20 件以上溜まっていても)・notified = 今回の知らせを送れたか (Codex #1541 R1 Medium)', async () => {
  let up = false;
  await withApp(async ({ db, j, sent }) => {
    S.init(db, { by: 'x', now: T0 });
    for (let i = 0; i < 25; i++) S.halt(db, { by: 'cli', reason: `溜まった止め ${i}`, now: T0 + i });   // 送れないまま溜まった 25 件
    up = true;
    const r = await j('/halt', { method: 'POST', body: { reason: '今回の止め' } });
    assert.deepEqual([r.status, r.body.notified, /今回の止め/.test(sent[0])], [200, true, true]);
    assert.ok(!S.outboxPending(db, { limit: 100 }).some((o) => o.id === r.body.outbox_id), '今回の知らせは送れた印');
  }, { notify: async () => up });
});

await ta('[8] 待ちの義務は番号の後から・商品で探せる (500 件を超えても全部に届く)・形の誤り (Codex #1541 R1 Medium)', async () => {
  await withApp(async ({ db, j, api, rev }) => {
    S.init(db, { by: 'x', now: T0 });
    S.setSetting(db, { key: 'lz_accounts', value: ['nakahara'], by: 'x', now: T0 });
    const ids = Array.from({ length: 620 }, (_, i) => `P-${String(i).padStart(4, '0')}`);
    artifact(db, ids, 'lzd_20300115_big');
    await j('/halt', { method: 'POST', body: { reason: '手で取り込む' } });
    await j('/manual/open', { method: 'POST', body: { lz_account: 'nakahara', source_run_id: 'lzd_20300115_big', expected_halt_revision: await rev() } });
    let r = await j('/pending');
    assert.deepEqual([r.body.count, r.body.items.length, r.body.next_after != null], [620, 500, true]);
    r = await j(`/pending?after=${r.body.next_after}`);
    assert.deepEqual([r.body.items.length, r.body.items[0].product_id, r.body.next_after], [120, 'P-0500', null]);
    r = await j('/pending?product=P-0600');
    assert.deepEqual(r.body.items.map((x) => x.product_id), ['P-0600']);
    assert.equal((await j('/waive', { method: 'POST', body: { obligation_ids: [r.body.items[0].id], note: '501 件目より後だけ閉じる' } })).body.waived, 1);
    assert.equal((await j('/pending?product=P-0600')).body.items.length, 0);
    for (const q of ['after=x', 'limit=0', 'limit=5001', 'product=', 'after=1&after=2']) assert.equal((await j(`/pending?${q}`)).status, 400, q);
  });
});

await ta('[9] 画面 (③c-1b-3b-4b): 描ける・中の JS が組み立てられる・値は textContent だけ (innerHTML などを使わない)・JS が使う id が全部ある・手順がある・server.js は管理者だけ', async () => {
  const fs = await import('node:fs');
  const path = await import('node:path');
  const { fileURLToPath } = await import('node:url');
  const { default: ejs } = await import('ejs');
  const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
  const file = path.join(root, 'apps', 'logizard-import-state', 'views', 'admin.ejs');
  const html = await ejs.renderFile(file, { username: 'admin@b-faith.biz', displayName: '<script>x</script>' });
  assert.ok(html.includes('&lt;script&gt;x&lt;/script&gt;'), '名前は escape');
  const scripts = [...html.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]);
  assert.equal(scripts.length, 1);
  new Function(scripts[0]);   // 組み立てられる (構文の誤りが無い)。自分の画面の JS を組み立てるだけで動かさない (外からの値は入らない)
  for (const bad of ['innerHTML', 'outerHTML', 'insertAdjacentHTML', 'document.write', 'eval(']) assert.ok(!scripts[0].includes(bad), bad);
  const ids = new Set([...html.matchAll(/ id="([^"]+)"/g)].map((m) => m[1]));
  // JS が使う id = $('…')・act('ボタン', '知らせ欄', …)・say('知らせ欄', …) の全部
  const used = [...scripts[0].matchAll(/\$\('([^']+)'\)/g)].map((m) => m[1])
    .concat([...scripts[0].matchAll(/\bact\('([^']+)', '([^']+)'/g)].flatMap((m) => [m[1], m[2]]))
    .concat([...scripts[0].matchAll(/\bsay\('([^']+)'/g)].map((m) => m[1]));
  assert.ok(used.length > 30);
  for (const id of used) assert.ok(ids.has(id), `JS が使う id が画面に無い: ${id}`);
  assert.ok(scripts[0].includes("const API = '/apps/logizard-import-state/admin-api';"));
  for (const w of ['手順', '自分のアカウント', 'ファイル名を変えない', '結果の文', '取込の履歴', '要確認', '自動を再開', 'Render が止まっている']) assert.ok(html.includes(w), w);
  // server.js: 画面は requireAdmin の後・JOBS_MONITOR_ENABLED の中
  const sv = fs.readFileSync(path.join(root, 'server.js'), 'utf8');
  const at = sv.indexOf("app.get('/apps/logizard-import-state/admin', logizardImportAdminPageGate, renderLogizardImportAdminPage);");
  assert.ok(at > 0);
  const g = sv.lastIndexOf("if (process.env.JOBS_MONITOR_ENABLED === '1') {", at);
  assert.ok(g > 0 && !sv.slice(g, at).includes('}'), 'JOBS_MONITOR_ENABLED の中');
  // 画面を返す関数: no-store と管理者の名前
  const { renderAdminPage } = await import('../apps/logizard-import-state/admin-router.js');
  const got = {};
  renderAdminPage({ session: { email: 'a@b', displayName: 'A' } }, { set: (k, v) => { got[k] = v; }, render: (p, l) => { got.path = p; got.locals = l; } });
  assert.deepEqual([got['Cache-Control'], path.basename(got.path), got.locals], ['no-store', 'admin.ejs', { username: 'a@b', displayName: 'A' }]);
});

await ta('[10] 画面が見ていた止めの番号 (Codex #1542 R1 High): 無い = 400・古い (止め直された) = 409 stale・止めてないのに再開 = 409 / 閉じた手の取込の CSV は取れない (409) / 成果物は対象の日の新しい順', async () => {
  await withApp(async ({ db, j, api, rev }) => {
    S.init(db, { by: 'x', now: T0 });
    S.setSetting(db, { key: 'lz_accounts', value: ['nakahara'], by: 'x', now: T0 });
    await j('/halt', { method: 'POST', body: { reason: '一度目の止め' } });
    const r1 = await rev();
    assert.ok(Number.isSafeInteger(r1));
    assert.deepEqual([(await j('/resume', { method: 'POST', body: { note: '再開したい' } })).status, S.getStatus(db).halted], [400, true]);
    // 別の画面で「再開 → 別の理由で止め直し」= 番号が変わる → 古い画面の再開は断る
    S.resume(db, { by: 'other', note: '別の画面で再開', expectedHaltRevision: S.getStatus(db).halt_revision, now: T0 + 1 });
    S.halt(db, { by: 'other', reason: '別の障害で止め直し', now: T0 + 2 });
    let r = await j('/resume', { method: 'POST', body: { note: '古い画面から再開', expected_halt_revision: r1 } });
    assert.deepEqual([r.status, r.body.error, S.getStatus(db).halted], [409, 'stale', true]);
    artifact(db);
    r = await j('/manual/open', { method: 'POST', body: { lz_account: 'nakahara', source_run_id: 'lzd_20300115_a', expected_halt_revision: r1 } });
    assert.deepEqual([r.status, r.body.error], [409, 'stale']);
    r = await j('/manual/open-gas?lz_account=nakahara&target_as_of=2030-01-16', { method: 'POST', body: csvOf(['A-1']), type: 'application/octet-stream' });
    assert.equal(r.status, 400);   // 番号なし
    // 今の番号なら通る
    const m = (await j('/manual/open', { method: 'POST', body: { lz_account: 'nakahara', source_run_id: 'lzd_20300115_a', expected_halt_revision: await rev() } })).body;
    assert.ok(m.session_id);
    assert.equal((await api(`/manual/${m.session_id}/csv`)).status, 200);
    await j(`/manual/${m.session_id}/cancel`, { method: 'POST', body: { note: 'ロジザードに置かなかった' } });
    r = await j(`/manual/${m.session_id}/csv`);
    assert.deepEqual([r.status, r.body.error], [409, 'not_open']);   // 閉じた後に古い画面から取れない
    assert.equal((await j('/resume', { method: 'POST', body: { note: '今の止めを見て再開', expected_halt_revision: await rev() } })).status, 200);
    r = await j('/resume', { method: 'POST', body: { note: 'もう一度再開', expected_halt_revision: 1 } });
    assert.deepEqual([r.status, r.body.error], [409, 'not_halted']);
    assert.equal((await j('/status')).body.status.halt_revision, null);
    // 成果物は対象の日の新しい順 (古い日の成果物が後から届いても先頭にしない)
    const b1 = csvOf(['A-1']), b2 = csvOf(['B-2']);
    S.putArtifact(db, { sourceRunId: 'lzd_20300117_x', targetAsOf: '2030-01-17', verdict: 'pass', csvBuf: b1, sha256: shaOf(b1), rows: 1, by: 'lz-daily', now: T0 + 10 });
    S.putArtifact(db, { sourceRunId: 'lzd_20300110_old', targetAsOf: '2030-01-10', verdict: 'pass', csvBuf: b2, sha256: shaOf(b2), rows: 1, by: 'lz-daily', now: T0 + 20 });
    assert.deepEqual((await j('/status')).body.artifacts.map((x) => x.target_as_of), ['2030-01-17', '2030-01-15', '2030-01-10']);
  });
});

await ta('[11] 画面を本物のブラウザで動かす (Codex #1542 R1 Medium): 悪い値は文字のまま・連打で 2 つ始めない・入力欄は空から・履歴の日時は日本時間 (端末はニューヨーク)・GAS の CSV は同じバイト列・古い画面の再開は断る', async () => {
  let chromium;
  ({ chromium } = await import('playwright'));   // 無い = 失敗 (画面の試験は要る。Codex #1542 R2 Medium)
  const { renderAdminPage } = await import('../apps/logizard-import-state/admin-router.js');
  const db = S.openImportStateDb(':memory:');
  let clock = T0;
  const app = express();
  app.use((req, res, next) => { req.session = { authenticated: true, email: 'admin@b-faith.biz', displayName: '管理者', role: 'admin' }; next(); });   // ブラウザ = 管理者
  app.get('/apps/logizard-import-state/admin', renderAdminPage);
  app.use('/apps/logizard-import-state/admin-api', adminApiGate, createAdminRouter({ getDb: () => db, now: () => clock, notify: async () => true }));
  const srv = await new Promise((resolve) => { const x = app.listen(0, '127.0.0.1', () => resolve(x)); });
  const base = `http://127.0.0.1:${srv.address().port}`;
  const browser = await chromium.launch({ headless: true });
  try {
    S.init(db, { by: 'x', now: T0 });
    const EVIL = '<img src=x onerror="window.__xss=1">';
    S.setSetting(db, { key: 'lz_accounts', value: ['nakahara', 'staff1', EVIL], by: 'x', now: T0 });
    artifact(db);
    const ctx = await browser.newContext({ timezoneId: 'America/New_York' });
    const page = await ctx.newPage();
    const waitMsg = (id, re) => page.waitForFunction(([i, r]) => new RegExp(r).test(document.getElementById(i).textContent), [id, re.source], { timeout: 10000 });
    await page.goto(`${base}/apps/logizard-import-state/admin`);
    await page.waitForFunction(() => /取込の状態/.test(document.getElementById('summary').textContent));
    // 止める (悪い値の理由)
    await page.fill('#halt-reason', EVIL + ' 止める理由');
    await page.click('#btn-halt');
    await waitMsg('msg-halt', /止めた/);
    await page.waitForFunction(() => /止めてある/.test(document.getElementById('summary').textContent));
    assert.equal(S.getStatus(db).halted, true);
    assert.ok(!(await page.textContent('#msg-halt')).includes('理由が変わった'), '自分で止めた = 知らせない');
    assert.ok((await page.textContent('#summary')).includes(EVIL), '理由は文字のまま出る');
    assert.deepEqual(await page.evaluate(() => [window.__xss, document.querySelectorAll('img').length]), [undefined, 0]);
    // 始める (連打しても 1 つ)
    assert.deepEqual(await page.evaluate(() => [...document.querySelectorAll('input[name=st-src]')].map((x) => x.checked)), [false, false], '出どころも空から (Codex #1542 R3 Low)');
    await page.selectOption('#st-account', 'nakahara');
    await page.selectOption('#st-artifact', 'lzd_20300115_a');
    await page.click('#btn-start');
    await waitMsg('msg-manual', /出どころ/);
    assert.equal(db.prepare('SELECT COUNT(*) AS n FROM manual_sessions').get().n, 0, '出どころを選ばない = 始めない');
    await page.check('input[name=st-src][value=artifact]');
    await page.dblclick('#btn-start');
    await waitMsg('msg-manual', /始めた/);
    assert.equal(db.prepare('SELECT COUNT(*) AS n FROM manual_sessions').get().n, 1);
    const m = db.prepare("SELECT session_id, download_name FROM manual_sessions WHERE status = 'open'").get();
    await page.waitForFunction(() => !document.getElementById('manual-open').classList.contains('hidden'));
    assert.deepEqual(await page.evaluate(() => [document.getElementById('cp-file').value, document.getElementById('cp-account').value, document.getElementById('open-download').getAttribute('href')]),
      ['', '', `/apps/logizard-import-state/admin-api/manual/${m.session_id}/csv`]);   // 入力欄に正解を入れない
    assert.ok((await page.textContent('#open-info')).includes(m.download_name), '比べる用に出す');
    // 終える (履歴の日時 = 日本時間 12:03。端末はニューヨーク)
    clock = T0 + 5 * MIN;
    await page.fill('#cp-result', RESULT_OK(2));
    await page.fill('#cp-file', m.download_name);
    await page.fill('#cp-at', '2030-01-16T12:03');
    await page.selectOption('#cp-account', 'nakahara');
    await page.click('#btn-complete');
    await waitMsg('msg-manual', /完了/);
    const done = S.getManualSession(db, { sessionId: m.session_id });
    // 閉じた = 始める入力は空へ (毎回自分で選ぶ)
    await page.waitForFunction(() => !document.getElementById('manual-start').classList.contains('hidden'));
    const startInputs = () => page.evaluate(() => [document.getElementById('st-account').value, document.getElementById('st-artifact').value, ...[...document.querySelectorAll('input[name=st-src]')].map((x) => x.checked)]);
    assert.deepEqual(await startInputs(), ['', '', false, false]);
    const reload = async () => {   // 1 分ごとの読み直しと同じ
      await page.evaluate(() => { document.getElementById('summary').textContent = '読み直し中'; document.dispatchEvent(new Event('lz-reload')); });
      await page.waitForFunction(() => /取込の状態/.test(document.getElementById('summary').textContent));
    };
    const fillStart = async () => {
      await page.selectOption('#st-account', 'staff1');   // 先頭でないアカウント (読み直しで先頭に戻したら分かる)
      await page.check('input[name=st-src][value=artifact]');
      await page.selectOption('#st-artifact', 'lzd_20300115_a');
    };
    // 読み直し (止めの番号・手の取込は変わらない) = 選んだもの・再開のメモを保つ
    await fillStart();
    await page.fill('#resume-note', '書きかけのメモ');
    await reload();
    assert.deepEqual(await startInputs(), ['staff1', 'lzd_20300115_a', true, false]);
    assert.equal(await page.inputValue('#resume-note'), '書きかけのメモ');
    // 別の画面で手の取込を始めて閉じた (読み直しの前後とも開いているものは無い) = 始める入力は空へ (Codex #1542 R3 Medium)
    const other = S.openManualSession(db, { expectedHaltRevision: S.getStatus(db).halt_revision, by: 'other', lzAccount: 'staff1', source: { kind: 'cdb_artifact', sourceRunId: 'lzd_20300115_a' }, now: clock });
    S.cancelManualSession(db, { sessionId: other.session_id, note: '別の画面で取り消した', by: 'other', now: clock });
    await reload();
    assert.deepEqual(await startInputs(), ['', '', false, false], '別の画面の手の取込の後 = 空へ');
    assert.equal(await page.inputValue('#resume-note'), '書きかけのメモ', '止めの番号は同じ = 再開のメモは保つ');
    assert.deepEqual([done.status, done.close_detail.history.at], ['completed_ok', Date.parse('2030-01-16T12:03:00+09:00')]);
    assert.deepEqual(await page.evaluate(() => [window.__xss, document.querySelectorAll('img').length]), [undefined, 0]);
    // 古い画面: 別のところで再開 → 止め直し = 画面の再開は断る
    S.resume(db, { by: 'other', note: '別の画面で再開', expectedHaltRevision: S.getStatus(db).halt_revision, now: clock });
    S.halt(db, { by: 'other', reason: '別の障害で止め直し', now: clock });
    await page.fill('#resume-note', '古い画面から再開');
    await page.click('#btn-resume');
    await waitMsg('msg-halt', /読み直/);
    assert.equal(S.getStatus(db).halted, true);
    // 読み直しで止めの番号が変わった = 再開のメモと始める入力を消して知らせる (新しい止めを黙って引き継がない。Codex #1542 R3 High)
    await fillStart();
    await reload();
    await waitMsg('msg-halt', /理由が変わった/);
    assert.equal(await page.inputValue('#resume-note'), '', '再開のメモは消す');
    assert.deepEqual(await startInputs(), ['', '', false, false], '始める入力も消す');
    await page.click('#btn-resume');
    await waitMsg('msg-halt', /note|4〜/);
    assert.equal(S.getStatus(db).halted, true, 'メモが空 = 再開しない');
    // GAS の CSV (移行の段階) = 同じバイト列
    S.setSetting(db, { key: 'cutover_phase', value: 'transition', by: 'x', now: clock });
    await page.reload();
    await page.waitForFunction(() => !document.getElementById('row-gas').classList.contains('hidden'));
    assert.deepEqual(await startInputs(), ['', '', false, false], '止めてある画面を開き直した = 始める入力は空から (出どころも)');
    const gas = csvOf(['A-1', 'C-3']);
    await page.selectOption('#st-account', 'nakahara');
    await page.check('input[name=st-src][value=gas]');
    await page.setInputFiles('#st-gas-file', { name: 'logizard_shohinmaster_upload.csv', mimeType: 'text/csv', buffer: gas });
    await page.fill('#st-gas-date', '2030-01-16');
    await page.click('#btn-start');
    await waitMsg('msg-manual', /始めた/);
    const g = db.prepare("SELECT session_id, source_kind FROM manual_sessions WHERE status = 'open'").get();
    assert.equal(g.source_kind, 'gas_upload');
    assert.ok(S.manualSessionCsv(db, { sessionId: g.session_id }).csv.equals(gas), 'GAS の CSV は同じバイト列');
    await ctx.close();
  } finally {
    await browser.close();
    await new Promise((r) => srv.close(r));
  }
});

await ta('[12] 画面の門を本物の HTTP で (ログインなし = /login へ・管理者でない = 403・管理者 = 画面) / 機械の口と CLI の再開も見た止めの番号が要る (無い = 400・古い = 409。Codex #1542 R2)', async () => {
  await withApp(async ({ db, base }) => {
    S.init(db, { by: 'x', now: T0 });
    const page = (user) => fetch(`${base}/apps/logizard-import-state/admin`, { redirect: 'manual', headers: user ? { 'x-test-user': user } : {} });
    let r = await page(null);
    assert.deepEqual([r.status, r.headers.get('location')], [302, '/login']);
    r = await page('staff');
    assert.equal(r.status, 403);
    r = await page('admin');
    assert.deepEqual([r.status, r.headers.get('cache-control'), (await r.text()).includes('ロジザードの取込 (戻し方)')], [200, 'no-store', true]);
    // 機械の口 (Bearer) の再開
    const c = C.createImportStateClient({ url: base, token: 'tok' });
    await c.halt({ by: '中原', reason: '機械の口の試験で止める' });
    const codeOf = async (p) => { try { await p; return 'ok'; } catch (e) { return `${e.status}:${e.code}`; } };
    assert.equal(await codeOf(c.resume({ by: '中原', note: '番号なしで再開' })), '400:bad_request');
    assert.equal(await codeOf(c.resume({ by: '中原', note: '古い番号で再開', expected_halt_revision: 1 })), '409:stale');
    assert.equal(S.getStatus(db).halted, true);
    // CLI
    const run = (args) => CLI.main(args, { client: c, log: () => {} });
    await assert.rejects(run(['resume', '--by', '中原', '--note', '番号なしで再開']));
    await assert.rejects(run(['resume', '--by', '中原', '--note', '形の違う番号', '--halt-revision', 'abc']), /halt-revision/);
    await run(['resume', '--by', '中原', '--note', '今の番号で再開', '--halt-revision', String((await c.status()).halt_revision)]);
    assert.equal(S.getStatus(db).halted, false);
  });
});

await ta('[13] 古い読み直しの答えは画面に出さない (操作の前に出した読み直しの答えが操作の後に届いても、止めた・始めた画面が前の状態に戻らない。Codex #1542 R4 Medium)', async () => {
  const { chromium } = await import('playwright');
  const { renderAdminPage } = await import('../apps/logizard-import-state/admin-router.js');
  const db = S.openImportStateDb(':memory:');
  const app = express();
  app.use((req, res, next) => { req.session = { authenticated: true, email: 'admin@b-faith.biz', displayName: '管理者', role: 'admin' }; next(); });
  app.get('/apps/logizard-import-state/admin', renderAdminPage);
  app.use('/apps/logizard-import-state/admin-api', adminApiGate, createAdminRouter({ getDb: () => db, now: () => T0, notify: async () => true }));
  const srv = await new Promise((resolve) => { const x = app.listen(0, '127.0.0.1', () => resolve(x)); });
  const base = `http://127.0.0.1:${srv.address().port}`;
  const browser = await chromium.launch({ headless: true });
  try {
    S.init(db, { by: 'x', now: T0 });
    S.setSetting(db, { key: 'lz_accounts', value: ['nakahara'], by: 'x', now: T0 });
    artifact(db);
    const page = await (await browser.newContext()).newPage();
    const waitMsg = (id, re) => page.waitForFunction(([i, r]) => new RegExp(r).test(document.getElementById(i).textContent), [id, re.source], { timeout: 10000 });
    const deferred = () => { let resolve; const promise = new Promise((r) => { resolve = r; }); return { promise, resolve }; };
    // 次の読み直しの答えを 1 つ預かる (その時の状態で取っておき、放すまで画面に渡さない = 遅れて届く古い答え)
    let hold = null;
    await page.route('**/admin-api/status', async (route) => {
      if (!hold || hold.taken) return route.continue();
      const h = hold; h.taken = true;
      const resp = await route.fetch();
      h.fetched.resolve();
      await h.release.promise;
      await route.fulfill({ response: resp });
      h.done.resolve();
    });
    const holdNext = () => (hold = { taken: false, fetched: deferred(), release: deferred(), done: deferred() });
    const reloadHeld = async () => { const h = holdNext(); await page.evaluate(() => document.dispatchEvent(new Event('lz-reload'))); await h.fetched.promise; return h; };
    const letGo = async (h) => { h.release.resolve(); await h.done.promise; await page.waitForTimeout(300); };
    const view = () => page.evaluate(() => ({
      halted: document.getElementById('summary').textContent.includes('止めてある'),
      haltRow: !document.getElementById('row-halt').classList.contains('hidden'),
      open: !document.getElementById('manual-open').classList.contains('hidden'),
      start: !document.getElementById('manual-start').classList.contains('hidden'),
    }));
    await page.goto(`${base}/apps/logizard-import-state/admin`);
    await page.waitForFunction(() => /取込の状態/.test(document.getElementById('summary').textContent));
    // ① 止める前の読み直しを預かる → 止める → 預かった答え (動いている) を放す = 止めた画面のまま
    let h = await reloadHeld();
    await page.fill('#halt-reason', '古い読み直しの試験で止める');
    await page.click('#btn-halt');
    await waitMsg('msg-halt', /止めた/);
    assert.deepEqual(await view(), { halted: true, haltRow: false, open: false, start: true });
    await letGo(h);
    assert.deepEqual(await view(), { halted: true, haltRow: false, open: false, start: true }, '止めた画面が動いている画面に戻らない');
    assert.ok(!(await page.textContent('#msg-halt')).includes('理由が変わった'), '古い答えで止めの番号が戻って「変わった」と出ない');
    // ② 始める前の読み直しを預かる → 手の取込を始める → 放す = ダウンロードの欄は消えない
    h = await reloadHeld();
    await page.selectOption('#st-account', 'nakahara');
    await page.check('input[name=st-src][value=artifact]');
    await page.selectOption('#st-artifact', 'lzd_20300115_a');
    await page.click('#btn-start');
    await waitMsg('msg-manual', /始めた/);
    assert.deepEqual(await view(), { halted: true, haltRow: false, open: true, start: false });
    await letGo(h);
    assert.deepEqual(await view(), { halted: true, haltRow: false, open: true, start: false }, '始めた画面が始める前の画面に戻らない');
    // 画面が見ている止めの番号も戻っていない = 取り消して再開できる (番号が古い答えの null に戻っていたら 400)
    await page.fill('#cancel-note', 'ロジザードに置かなかった');
    await page.click('#btn-cancel');
    await waitMsg('msg-manual', /取り消|済み/);
    await page.fill('#resume-note', '古い読み直しの試験の後に再開');
    await page.click('#btn-resume');
    await waitMsg('msg-halt', /再開した/);
    assert.equal(S.getStatus(db).halted, false);
  } finally {
    await browser.close();
    await new Promise((r) => srv.close(r));
  }
});

console.log(`\n${passed} 件 PASS${process.exitCode ? ' (NG あり)' : ''}`);
process.exit(process.exitCode || 0);
