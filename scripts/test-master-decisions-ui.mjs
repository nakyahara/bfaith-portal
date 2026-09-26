/**
 * test-master-decisions-ui.mjs — マスタの判断の画面と API (apps/master-decisions。D2'。Company DB構想 10 §6.1.1「D2' 判断の画面と API の契約 v1」)
 *
 * 本物の router を HTTP 越しに通す。Company DB = PGlite (Render と同じ条件の持ち主のロール deploy で migration)。セッションは x-test-session で模擬
 *   (approver = 名簿の人 / admin = 名簿に無い管理者 / user = 名簿に無い利用者)
 * 固定する契約:
 *   1 画面・つかいかた・末尾の / ・決められるかの印 / つかいかたに画面のボタンの言葉が全部ある
 *   2 件数 (最新の照合の回・理由の種類 × 状態・今の回に出ている数)
 *   3 一覧 (今の回だけ / 全部・状態・理由・分類・SKU の検索)
 *   4 名簿 (名簿に無い admin も不可・空なら誰も不可)・Origin・Content-Type・COMPANY_DB_URL が無い = 503
 *   5 差を残す → 承認済み・出来事に決めた人 / ほかの人が先に決めた = decided_meanwhile
 *   6 NE を直す: 提案の値で目標 / 提案が無い = needs_target → 1 件ずつ値を入れる / 型違い = invalid_target
 *   7 社内の値を直す (manual): NE の値で目標 / 選べない解決 = resolution_not_allowed (DB の trigger の前に)
 *   8 今の回に出ていない = not_current / 画面の後に新しい照合 = stale_view / 取り消す判断が無い = nothing_to_revoke
 *   9 取り消し → 判断待ちに戻る / 取り消しの取り消しは不可
 *  10 完了と再発: 直す承認に完了 → 今の回に出ている = 再発 (判断待ち) / 出ていない = 完了
 *  11 入力の検証 (400)
 *  12 server.js: Render だけ (env)・requireAppAccess・機械用の口とは別
 * 使い方: node scripts/test-master-decisions-ui.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import http from 'node:http';
import vm from 'node:vm';
import express from 'express';

const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { createRoles } = await import('./company-db/create-watch-roles.mjs');
const { writeDecisions } = await import('../apps/company-db/master-compare/decisions.mjs');
const { default: router, __setPgClientFactory } = await import('../apps/master-decisions/router.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};
const run = (i) => `mc_20300101T0000000${String(i).padStart(2, '0')}Z_abcdef`;
const fpOf = (c) => c.repeat(64);

// ── Company DB (Render と同じ = 持ち主のロールで) ──
const pg = new PGlite();
await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
await pg.query('set role deploy');
const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });
const cand = (fp, o) => ({ fingerprint: fp, subject_key: o.subject_key, code_norm: o.subject_key.split(':')[1], col: o.col, child: o.child ?? null, cls: o.cls, reason_kind: o.reason_kind,
  semantic: `${o.reason_kind}@1`, print: { code_norm: o.subject_key.split(':')[1], col: o.col, n: o.n ?? null, n_state: o.n_state ?? null, c: o.c ?? null, reason: { reason: o.reason_kind } },
  resolutions: o.resolutions, proposal: o.proposal });
const A = fpOf('a'), Bf = fpOf('b'), C = fpOf('c'), D = fpOf('d'), E = fpOf('e');
const CANDS = {
  [A]: cand(A, { subject_key: 'value:a001', col: 'tax_rate', cls: 'ne_no_value', reason_kind: 'tax_fallback', n: null, n_state: 'empty', c: 0.1, resolutions: ['accept_difference', 'fix_ne'], proposal: { op: 'set_ne_value', value: 0.1 } }),
  [Bf]: cand(Bf, { subject_key: 'value:b002', col: 'standard_price_jpy', cls: 'rule', reason_kind: 'set_price_from_goods', n: 900, c: 1000, resolutions: ['fix_ne', 'accept_difference'], proposal: { op: 'set_ne_value', value: 1000 } }),
  [C]: cand(C, { subject_key: 'value:c003', col: 'tax_rate', cls: 'incomparable', reason_kind: 'none', n: '0', n_state: 'zero', c: 0.1, resolutions: ['fix_ne'], proposal: { op: 'decide' } }),
  [D]: cand(D, { subject_key: 'components:s001', col: 'components', child: 'a001', cls: 'rule', reason_kind: 'manual', n: 2, c: 3, resolutions: ['accept_difference', 'fix_cdb'], proposal: { op: 'decide_manual_priority' } }),
  [E]: cand(E, { subject_key: 'value:e005', col: 'name', cls: 'spec_undecided', reason_kind: 'spec_undecided', n: 'E', c: 'E2', resolutions: ['spec', 'accept_difference'], proposal: { op: 'decide_spec' } }),
};
await writeDecisions(db, { compareRunId: run(1), observedAt: '2030-01-01T00:00:00Z', decisions: Object.values(CANDS) });
await writeDecisions(db, { compareRunId: run(2), observedAt: '2030-01-02T00:00:00Z', decisions: [CANDS[A], CANDS[Bf], CANDS[C], CANDS[D]] });   // E は今の回に出ていない

// ── ポータル (本物の router・セッションは模擬) ──
process.env.COMPANY_DB_URL = 'postgres://test@localhost:5432/test';
process.env.MASTER_DECISION_APPROVERS = 'Naka@Test, other@test';
__setPgClientFactory(async () => ({ query: (t, p) => pg.query(t, p), end: async () => {}, on: () => {} }));
const app = express();
app.set('view engine', 'ejs');
app.use((req, res, next) => {
  const s = req.headers['x-test-session'];
  req.session = s === 'approver' ? { authenticated: true, email: 'naka@test', displayName: '中原', role: 'user', allowedApps: ['master-decisions'] }
    : s === 'admin' ? { authenticated: true, email: 'admin@test', role: 'admin', allowedApps: '*' }
      : s === 'user' ? { authenticated: true, email: 'user@test', role: 'user', allowedApps: ['master-decisions'] } : null;
  next();
});
app.use('/apps/master-decisions', router);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const ORIGIN = `http://127.0.0.1:${server.address().port}`;
const BASE = `${ORIGIN}/apps/master-decisions`;
async function call(method, url, { body, session = 'approver', origin = true, ctype = true } = {}) {
  const headers = { Accept: 'application/json', 'x-test-session': session };
  if (body !== undefined && ctype) headers['Content-Type'] = 'application/json';
  if (origin) headers.Origin = ORIGIN;
  const r = await fetch(BASE + url, { method, headers, body: body === undefined ? undefined : JSON.stringify(body), redirect: 'manual' });
  const text = await r.text();
  let j = null; try { j = JSON.parse(text); } catch { /* HTML */ }
  return { status: r.status, j, text, r };
}
const list = async (qs = '') => (await call('GET', `/api/candidates${qs}`)).j;
const item = (c, extra = {}) => ({ fingerprint: c.fingerprint, shown_last_seen_run: c.last_seen_run, shown_event_id: c.decision ? c.decision.event_id : null, ...extra });
const find = async (fp, qs = '?status=any&view=all') => (await list(qs)).items.find((c) => c.fingerprint === fp);
const decide = (body, session = 'approver') => call('POST', '/api/decisions', { body, session });

await ta('[1] 画面・つかいかた・末尾の / ・決められるかの印 / つかいかたに画面のボタンの言葉が全部ある', async () => {
  let r = await call('GET', '/');
  assert.equal(r.status, 200); assert.match(r.text, /マスタの判断/); assert.match(r.text, /data-can-decide="1"/);
  // 画面の JS が文法として読める (描画の試験は通っても画面の JS が壊れていることがある)
  const scripts = [...r.text.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((x) => x[1]);
  assert.equal(scripts.length, 1);
  new vm.Script(scripts[0]);   // 組み立てるだけ (実行しない)
  for (const api of ['api/summary', 'api/candidates?', 'api/candidates/', 'api/decisions']) assert.ok(scripts[0].includes(`'${api}`), `画面が ${api} を呼んでいない`);
  r = await call('GET', '/', { session: 'admin' });
  assert.match(r.text, /data-can-decide="0"/); assert.match(r.text, /名簿の人だけ/);
  const bare = await fetch(`${ORIGIN}/apps/master-decisions`, { headers: { 'x-test-session': 'approver' }, redirect: 'manual' });
  assert.equal(bare.status, 301); assert.equal(bare.headers.get('location'), '/apps/master-decisions/');
  const m = await call('GET', '/manual');
  assert.equal(m.status, 200);
  const page = fs.readFileSync(new URL('../apps/master-decisions/views/index.ejs', import.meta.url), 'utf8');
  const buttons = [...page.matchAll(/<button class="btn-sm" data-act="[^"]+">([^<]+)<\/button>/g)].map((x) => x[1]);
  assert.deepEqual(buttons, ['差を残す', 'NE を直す (提案の値で)', '却下']);
  for (const b of [...buttons, '判断を取り消す']) assert.ok(m.text.includes(b), `つかいかたに「${b}」が無い`);
});

await ta('[2] 件数: 最新の照合の回・理由の種類 × 状態・今の回に出ている数・決められるか', async () => {
  const s = (await call('GET', '/api/summary')).j;
  assert.equal(s.latest.compare_run_id, run(2));
  assert.deepEqual([s.candidates, s.current, s.total.pending], [5, 4, 4]);
  assert.deepEqual(s.by_reason.tax_fallback, { pending: 1, approved: 0, rejected: 0, done: 0 });
  assert.equal(s.by_reason.spec_undecided, undefined);   // 今の回に出ていない
  assert.equal(s.can_decide, true);
  const a = (await call('GET', '/api/summary', { session: 'admin' })).j;
  assert.equal(a.can_decide, false); assert.match(a.gate_message, /名簿/);
});

await ta('[3] 一覧: 今の回だけ / 全部・状態・理由・分類・SKU の検索', async () => {
  assert.deepEqual((await list()).items.map((c) => c.code_norm).sort(), ['a001', 'b002', 'c003', 's001']);
  assert.equal((await list('?view=all')).total, 5);
  assert.deepEqual((await list('?reason=manual')).items.map((c) => c.fingerprint), [D]);
  assert.deepEqual((await list('?cls=incomparable')).items.map((c) => c.fingerprint), [C]);
  assert.deepEqual((await list('?q=B002')).items.map((c) => c.fingerprint), [Bf]);
  assert.equal((await list('?status=approved')).total, 0);
  const a = (await list()).items.find((c) => c.fingerprint === A);
  assert.deepEqual([a.n, a.c, a.status, a.current, a.seen_count, a.last_seen_run], [null, 0.1, 'pending', true, 2, run(2)]);
});

await ta('[4] 名簿 (名簿に無い admin も不可・空なら誰も不可)・Origin・Content-Type・COMPANY_DB_URL が無い = 503', async () => {
  const a = (await find(A));
  const body = { kind: 'approved', resolution: 'accept_difference', items: [item(a)] };
  let r = await decide(body, 'admin');
  assert.deepEqual([r.status, r.j.reason], [403, 'not_approver']);
  assert.equal((await decide(body, 'user')).status, 403);
  const keep = process.env.MASTER_DECISION_APPROVERS;
  process.env.MASTER_DECISION_APPROVERS = ' , ';
  r = await decide(body);
  assert.equal(r.status, 403); assert.match(r.j.error, /誰も決められません/);
  process.env.MASTER_DECISION_APPROVERS = keep;
  assert.deepEqual([(await call('POST', '/api/decisions', { body, origin: false })).status, (await call('POST', '/api/decisions', { body, origin: false })).j.error], [403, 'origin_mismatch']);
  assert.equal((await call('POST', '/api/decisions', { body, ctype: false })).status, 415);
  const url = process.env.COMPANY_DB_URL; delete process.env.COMPANY_DB_URL;
  assert.equal((await call('GET', '/api/summary')).status, 503);
  process.env.COMPANY_DB_URL = url;
  assert.equal((await find(A)).status, 'pending');   // どれも書いていない
});

await ta('[5] 差を残す → 承認済み・出来事に決めた人とメモ / 同じ画面のままもう一度 = decided_meanwhile', async () => {
  const a = await find(A);
  const r = await decide({ kind: 'approved', resolution: 'accept_difference', note: '社内の値で持つ', items: [item(a)] });
  assert.equal(r.status, 200); assert.equal(r.j.applied.length, 1);
  const x = await find(A);
  assert.deepEqual([x.status, x.decision.resolution, x.decision.actor, x.decision.note], ['approved', 'accept_difference', 'naka@test', '社内の値で持つ']);
  const ev = (await call('GET', `/api/candidates/${A}/events`)).j.events;
  assert.deepEqual(ev.map((e) => [e.kind, e.actor_type, e.actor]), [['approved', 'user', 'naka@test']]);
  const again = await decide({ kind: 'rejected', items: [item(a)] });   // a = 決める前に見た画面 (shown_event_id = null)
  assert.deepEqual(again.j.skipped, [{ fingerprint: A, reason: 'decided_meanwhile' }]);
});

await ta('[6] NE を直す: 提案の値で目標 / 提案が無い = needs_target → 1 件ずつ値を入れる / 型違い = invalid_target', async () => {
  const b = await find(Bf), c = await find(C);
  let r = await decide({ kind: 'approved', resolution: 'fix_ne', items: [item(b), item(c)] });
  assert.deepEqual(r.j.applied.map((x) => x.fingerprint), [Bf]);
  assert.deepEqual(r.j.skipped, [{ fingerprint: C, reason: 'needs_target' }]);
  assert.deepEqual((await find(Bf)).decision.target, { subject_key: 'value:b002', col: 'standard_price_jpy', child: null, value: 1000 });
  r = await decide({ kind: 'approved', resolution: 'fix_ne', items: [item(c, { target_value: 'abc' })] });
  assert.deepEqual(r.j.skipped, [{ fingerprint: C, reason: 'invalid_target' }]);
  r = await decide({ kind: 'approved', resolution: 'fix_ne', items: [item(c, { target_value: 0.05 })] });
  assert.deepEqual(r.j.skipped.map((x) => x.reason), ['invalid_target']);   // 税率は 0.1 / 0.08 だけ
  r = await decide({ kind: 'approved', resolution: 'fix_ne', items: [item(c, { target_value: 0.1 })] });
  assert.equal(r.j.applied.length, 1);
  assert.deepEqual((await find(C)).decision.target.value, 0.1);
});

await ta('[7] 社内の値を直す (manual): NE の値で目標 / 選べない解決 = resolution_not_allowed (DB の trigger の前に)', async () => {
  const d = await find(D);
  let r = await decide({ kind: 'approved', resolution: 'fix_ne', items: [item(d)] });
  assert.deepEqual(r.j.skipped, [{ fingerprint: D, reason: 'resolution_not_allowed' }]);
  r = await decide({ kind: 'approved', resolution: 'fix_cdb', items: [item(d)] });
  assert.equal(r.j.applied.length, 1);
  assert.deepEqual((await find(D)).decision.target, { subject_key: 'components:s001', col: 'components', child: 'a001', value: 2 });
});

await ta('[8] 今の回に出ていない = not_current / 画面の後に新しい照合 = stale_view / 取り消す判断が無い = nothing_to_revoke', async () => {
  const e = await find(E);
  assert.equal(e.current, false);
  let r = await decide({ kind: 'approved', resolution: 'accept_difference', items: [item(e)] });
  assert.deepEqual(r.j.skipped, [{ fingerprint: E, reason: 'not_current' }]);
  r = await decide({ kind: 'revoked', items: [item(e)] });
  assert.deepEqual(r.j.skipped, [{ fingerprint: E, reason: 'nothing_to_revoke' }]);
  const a = await find(A);
  r = await decide({ kind: 'rejected', items: [{ ...item(a), shown_last_seen_run: run(1) }] });
  assert.deepEqual(r.j.skipped, [{ fingerprint: A, reason: 'stale_view' }]);
});

await ta('[9] 取り消し → 判断待ちに戻る (記録は残る) / 取り消しの取り消しは不可', async () => {
  const a = await find(A);
  let r = await decide({ kind: 'revoked', items: [item(a)] });
  assert.equal(r.j.applied.length, 1);
  const x = await find(A);
  assert.deepEqual([x.status, x.decision.kind], ['pending', 'revoked']);
  r = await decide({ kind: 'revoked', items: [item(x)] });
  assert.deepEqual(r.j.skipped, [{ fingerprint: A, reason: 'nothing_to_revoke' }]);
  assert.equal((await call('GET', `/api/candidates/${A}/events`)).j.events.length, 2);
});

await ta('[10] 完了と再発: 直す承認に完了 → 今の回に出ている = 再発 (判断待ち) / 出ていない = 完了', async () => {
  const b = await find(Bf);
  const ok = (await pg.query('select ops.record_decision_done($1::bigint, $2, $3::jsonb) as ok', [b.decision.event_id, run(2),
    JSON.stringify({ side: 'ne', subject_key: 'value:b002', col: 'standard_price_jpy', child: null, value: 1000 })])).rows[0].ok;
  assert.equal(ok, true);
  let x = await find(Bf);
  assert.deepEqual([x.status, x.reoccurred, !!x.done_at], ['pending', true, true]);   // 今の回 (run 2) にまだ出ている = 再発
  await writeDecisions(db, { compareRunId: run(3), observedAt: '2030-01-03T00:00:00Z', decisions: [CANDS[A]] });   // 次の照合で B は出ない
  x = await find(Bf);
  assert.deepEqual([x.status, x.current], ['done', false]);
  assert.equal((await call('GET', '/api/summary')).j.latest.compare_run_id, run(3));
});

await ta('[11] 入力の検証 (400): kind・解決・メモ・指紋・重複・500 件・目標の値は 1 件ずつ・画面が見た回', async () => {
  const a = await find(A);
  const bad = async (body, re) => { const r = await decide(body); assert.equal(r.status, 400, JSON.stringify(body).slice(0, 80)); if (re) assert.match(r.j.error, re); };
  await bad({ kind: 'approve', items: [item(a)] }, /kind/);
  await bad({ kind: 'approved', items: [item(a)] }, /解決/);
  await bad({ kind: 'approved', resolution: 'approve_all', items: [item(a)] }, /解決/);
  await bad({ kind: 'rejected', resolution: 'accept_difference', items: [item(a)] }, /解決は付けない/);
  await bad({ kind: 'rejected', note: 'x'.repeat(501), items: [item(a)] }, /500 字/);
  await bad({ kind: 'rejected', items: [] }, /空/);
  await bad({ kind: 'rejected', items: [{ ...item(a), fingerprint: 'zz' }] }, /指紋/);
  await bad({ kind: 'rejected', items: [item(a), item(a)] }, /2 回/);
  await bad({ kind: 'rejected', items: Array.from({ length: 501 }, (_, i) => ({ fingerprint: i.toString(16).padStart(64, '0'), shown_last_seen_run: run(3), shown_event_id: null })) }, /500 件/);
  const b = await find(Bf);
  await bad({ kind: 'approved', resolution: 'fix_ne', items: [item(a, { target_value: 0.1 }), item(b)] }, /1 件ずつ/);
  await bad({ kind: 'rejected', items: [{ fingerprint: A, shown_event_id: null }] }, /画面が見た回/);
  assert.equal((await call('GET', '/api/candidates/zz/events')).status, 400);
  assert.equal((await call('GET', `/api/candidates/${'f'.repeat(64)}/events`)).status, 404);
});

await ta('[12] server.js: Render だけ (env)・requireAppAccess・機械用の口 (/apps/company-db/sync) とは別', async () => {
  const s = fs.readFileSync(new URL('../server.js', import.meta.url), 'utf8');
  assert.match(s, /if \(process\.env\.MASTER_DECISIONS_ENABLED === '1'\) \{\r?\n\s+app\.use\('\/apps\/master-decisions', requireAppAccess\('master-decisions'\), masterDecisionsRouter\);/);
  assert.equal((s.match(/masterDecisionsRouter/g) || []).length, 2);   // import と mount だけ
});

server.close();
await pg.close();
console.log(`\n${passed} 件 PASS`);
