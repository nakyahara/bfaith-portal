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
 *  12 server.js: Render だけ (env)・requireAppAccess・機械用の口とは別・共通の JSON parser を通さない
 *  13 候補が 0 件の照合の回も「今朝の照合」(0034) / 14 直す値は照合の形 (円は整数・税率・仕入先の正規化・構成の「無い」・商品名は文字) / 15 1 件が DB に拒まれてもほかは保存
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
  // 共通の 10MB の JSON parser は通さない (認証の前に本文を読まない・router の 512kb が効く。Codex #1481 R1 Medium)
  const skip = s.indexOf("if (normalizedPath.toLowerCase().startsWith('/apps/master-decisions')) return next();");
  assert.ok(skip > 0 && skip < s.indexOf('return globalJsonParser(req, res, next);'), '共通の JSON parser の除外に master-decisions が無い');
});

await ta('[13] 候補が 0 件の照合の回も「今朝の照合」= 前の回の候補は今出ていない (承認できない) (Codex #1481 R1 High)', async () => {
  await writeDecisions(db, { compareRunId: run(4), observedAt: '2030-01-04T00:00:00Z', decisions: [] });   // 差が全部消えた朝
  const s = (await call('GET', '/api/summary')).j;
  assert.deepEqual([s.latest.compare_run_id, s.latest.candidates, s.current], [run(4), 0, 0]);
  assert.equal((await list()).total, 0);
  const a = await find(A);
  assert.equal(a.current, false);
  const r = await decide({ kind: 'approved', resolution: 'accept_difference', items: [item(a)] });
  assert.deepEqual(r.j.skipped, [{ fingerprint: A, reason: 'not_current' }]);
  await writeDecisions(db, { compareRunId: run(4), observedAt: '2030-01-04T00:00:00Z', decisions: [] });   // 入れ直しで二重にしない
  assert.equal(Number((await pg.query(`select count(*)::int as n from ops.master_compare_runs where compare_run_id = $1`, [run(4)])).rows[0].n), 1);
});

await ta('[14] 直す値は照合の形にそろえる: 円は整数・税率 10 / 10%・仕入先は照合と同じ正規化 (1 つの配列はほどく・複数は値を入れて)・構成の「無い」・商品名は文字のまま', async () => {
  const { numState, textState } = await import('../apps/company-db/master-compare/compare-ne.mjs');
  const P = fpOf('1'), S1 = fpOf('2'), S2 = fpOf('3'), N = fpOf('4'), K = fpOf('5'), T = fpOf('6');
  const more = {
    [P]: cand(P, { subject_key: 'value:p001', col: 'standard_price_jpy', cls: 'ne_no_value', reason_kind: 'ne_no_value', n: null, c: 1200, resolutions: ['fix_ne', 'accept_difference'], proposal: { op: 'set_ne_value', value: 1200 } }),
    [S1]: cand(S1, { subject_key: 'primary_supplier:p002', col: 'primary_supplier', cls: 'ne_no_value', reason_kind: 'ne_no_value', n: null, c: ['0001'], resolutions: ['fix_ne', 'accept_difference'], proposal: { op: 'set_ne_value', value: ['0001'] } }),
    [S2]: cand(S2, { subject_key: 'primary_supplier:p003', col: 'primary_supplier', cls: 'ne_no_value', reason_kind: 'ne_no_value', n: null, c: ['0001', '0002'], resolutions: ['fix_ne', 'accept_difference'], proposal: { op: 'set_ne_value', value: ['0001', '0002'] } }),
    [N]: cand(N, { subject_key: 'value:p004', col: 'name', cls: 'rule', reason_kind: 'set_name_blank', n: null, c: 'x', resolutions: ['fix_ne', 'accept_difference'], proposal: { op: 'decide' } }),
    [K]: cand(K, { subject_key: 'components:s009', col: 'components', child: 'a001', cls: 'rule', reason_kind: 'manual', n: '(無い)', c: 2, resolutions: ['accept_difference', 'fix_cdb'], proposal: { op: 'decide_manual_priority' } }),
    [T]: cand(T, { subject_key: 'value:p006', col: 'tax_rate', cls: 'incomparable', reason_kind: 'none', n: '0', c: 0.1, resolutions: ['fix_ne'], proposal: { op: 'decide' } }),
  };
  await writeDecisions(db, { compareRunId: run(5), observedAt: '2030-01-05T00:00:00Z', decisions: Object.values(more) });
  const one = async (fp, resolution, extra) => (await decide({ kind: 'approved', resolution, items: [item(await find(fp), extra)] })).j;
  const target = async (fp) => (await find(fp)).decision.target.value;
  assert.deepEqual((await one(P, 'fix_ne', { target_text: '100.5' })).skipped.map((x) => x.reason), ['invalid_target']);   // 照合は円を整数で比べる
  assert.deepEqual((await one(P, 'fix_ne', { target_value: 100.5 })).skipped.map((x) => x.reason), ['invalid_target']);   // 型つきで渡しても同じ
  assert.equal((await one(P, 'fix_ne', { target_text: '1,300' })).applied.length, 1);
  assert.equal(await target(P), 1300);
  assert.equal(await target(P), numState(JSON.stringify('1300'), 'yen').value);   // NE にその値を入れたときに照合が読む値と同じ
  assert.equal((await one(S1, 'fix_ne')).applied.length, 1);   // 提案 ['0001'] = 1 つだけ = ほどく
  assert.equal(await target(S1), '0001');
  assert.deepEqual((await one(S2, 'fix_ne')).skipped.map((x) => x.reason), ['needs_target']);   // 複数の仕入先を黙って 1 つに絞らない
  assert.equal((await one(S2, 'fix_ne', { target_text: '2' })).applied.length, 1);
  assert.equal(await target(S2), textState('2', 'supplier').value);   // 照合と同じ正規化 (0002)
  assert.equal((await one(N, 'fix_ne', { target_text: '123' })).applied.length, 1);
  assert.equal(await target(N), '123');   // 商品名は文字のまま
  assert.equal((await one(K, 'fix_cdb')).applied.length, 1);   // NE に無い子 = 目標は「無い」
  assert.equal(await target(K), '__absent__');
  assert.equal((await one(T, 'fix_ne', { target_text: '10%' })).applied.length, 1);
  assert.equal(await target(T), numState(JSON.stringify('10'), 'tax').value);
  const r = await decide({ kind: 'approved', resolution: 'fix_ne', items: [item(await find(P), { target_text: 5 })] });
  assert.equal(r.status, 400);   // target_text は文字
});

await ta('[15] 1 件が DB に拒まれても、ほかの件は保存する (1 件ずつ savepoint)', async () => {
  const X = fpOf('7'), Y = fpOf('8');
  const c2 = { [X]: cand(X, { subject_key: 'value:q001', col: 'name', cls: 'rule', reason_kind: 'set_name_blank', n: null, c: 'q', resolutions: ['accept_difference'], proposal: { op: 'decide' } }),
    [Y]: cand(Y, { subject_key: 'value:q002', col: 'name', cls: 'rule', reason_kind: 'set_name_blank', n: null, c: 'q', resolutions: ['accept_difference'], proposal: { op: 'decide' } }) };
  await writeDecisions(db, { compareRunId: run(6), observedAt: '2030-01-06T00:00:00Z', decisions: Object.values(c2) });
  await pg.query(`create function ops.test_reject_x() returns trigger language plpgsql as $$ begin if new.fingerprint = '${X}' then raise exception '試験で拒む'; end if; return new; end $$`);
  await pg.query(`create trigger trg_test_reject_x before insert on ops.master_decision_events for each row execute function ops.test_reject_x()`);
  try {
    const r = await decide({ kind: 'approved', resolution: 'accept_difference', items: [item(await find(X)), item(await find(Y))] });
    assert.deepEqual(r.j.applied.map((x) => x.fingerprint), [Y]);
    assert.deepEqual(r.j.skipped.map((x) => [x.fingerprint, x.reason]), [[X, 'db_rejected']]);
    assert.equal((await find(Y)).status, 'approved');
    assert.equal((await find(X)).status, 'pending');
  } finally {
    await pg.query('drop trigger trg_test_reject_x on ops.master_decision_events');
    await pg.query('drop function ops.test_reject_x()');
  }
});

await ta('[16] 代表 (親。D3b) の直す値: 目標の入力が必須 (提案の値を使わない)・空 / なし / null = 親なし・自分自身のコード = 親なし・コードは norm・数は不可 / 画面は空も送る', async () => {
  const Q = ['9', '0', 'f'].map(fpOf).concat(['f'.repeat(63) + 'e', '9'.repeat(63) + '8']);
  const mk = (fp, code) => cand(fp, { subject_key: `parent:${code}`, col: 'parent', cls: 'rule', reason_kind: 'parent_manual', n: 'grp1', c: 'grp2',
    resolutions: ['accept_difference', 'fix_ne', 'fix_cdb'], proposal: { op: 'set_ne_value', value: 'grp2' } });   // 提案に値があっても目標には使わない
  const codes = ['p101', 'p102', 'p103', 'p104', 'p105'];
  await writeDecisions(db, { compareRunId: run(7), observedAt: '2030-01-07T00:00:00Z', decisions: Q.map((fp, i) => mk(fp, codes[i])) });
  const one = async (fp, resolution, extra) => (await decide({ kind: 'approved', resolution, items: [item(await find(fp), extra)] })).j;
  const target = async (fp) => (await find(fp)).decision.target.value;
  assert.deepEqual((await one(Q[0], 'fix_ne')).skipped.map((x) => x.reason), ['needs_target']);   // 目標を省いた = 拒む
  assert.equal((await one(Q[0], 'fix_ne', { target_text: '' })).applied.length, 1);             // 画面の空 = 親なし
  assert.equal(await target(Q[0]), null);
  assert.equal((await one(Q[1], 'fix_cdb', { target_text: ' GRP9 ' })).applied.length, 1);
  assert.equal(await target(Q[1]), 'grp9');
  assert.equal((await one(Q[2], 'fix_ne', { target_text: 'P103' })).applied.length, 1);          // 自分自身のコード = 親なし
  assert.equal(await target(Q[2]), null);
  assert.equal((await one(Q[3], 'fix_ne', { target_value: null })).applied.length, 1);           // API の JSON の null = 親なし
  assert.equal(await target(Q[3]), null);
  assert.deepEqual((await one(Q[4], 'fix_ne', { target_value: 5 })).skipped.map((x) => x.reason), ['invalid_target']);
  assert.equal((await one(Q[4], 'fix_ne', { target_text: 'なし' })).applied.length, 1);
  assert.equal(await target(Q[4]), null);
  const { normalizeTarget, defaultTargetValue } = await import('../apps/master-decisions/decide.mjs');
  assert.deepEqual(normalizeTarget('parent', 'Ｇｒｐ１', { selfNorm: 'p1' }), { ok: true, value: 'grp1' });
  for (const bad of [false, true, 0, {}, [], ['grp1']]) assert.deepEqual(normalizeTarget('parent', bad), { ok: false }, JSON.stringify(bad));   // 文字でない値を「親なし」にしない (Codex #1490 R1)
  assert.equal(defaultTargetValue({ col: 'parent', proposal: { op: 'set_ne_value', value: 'x' }, print: { n: 'y' } }, 'fix_ne'), undefined);
  // 画面: 代表は空の入力も target_text で送る・入力欄の説明は「空 = 親なし」
  const page = (await call('GET', '/')).text;
  assert.match(page, /if \(raw \|\| c\.col === 'parent'\) extra = \{ target_text: raw \}/);
  assert.match(page, /空 = 親なし/);
});

await ta('[17] 名前 = 商品コード は NE に入れない (2026-10-05): 社内の名前がコードのまま (cdb_name_is_code) は NE を直すを選べない・社内を直す (NE の名前が既定) / 古い形の候補 (NE をコードに) も承認で拒む (name_is_code) / 税率は % で見せる', async () => {
  const N1 = '1'.repeat(63) + 'a', N2 = '1'.repeat(63) + 'b', N3 = '1'.repeat(63) + 'c';
  const more = {
    [N1]: cand(N1, { subject_key: 'value:akadama-big-2l-2', col: 'name', cls: 'rule', reason_kind: 'cdb_name_is_code', n: null, n_state: 'empty', c: 'akadama-big-2l-2', resolutions: ['fix_cdb', 'accept_difference'], proposal: { op: 'fill_cdb_name' } }),
    [N2]: cand(N2, { subject_key: 'value:b-002', col: 'name', cls: 'rule', reason_kind: 'cdb_name_is_code', n: 'NE の名前', c: 'b-002', resolutions: ['fix_cdb', 'accept_difference'], proposal: { op: 'set_cdb_value', value: 'NE の名前' } }),
    // 照合を直す前の形 (company_owned で「NE を akadama-x に」) が今の回に残っていても、NE をコードにする承認は作らない
    [N3]: cand(N3, { subject_key: 'value:akadama-x', col: 'name', cls: 'rule', reason_kind: 'company_owned', n: null, n_state: 'empty', c: 'akadama-x', resolutions: ['accept_difference', 'fix_ne'], proposal: { op: 'set_ne_value', value: 'akadama-x' } }),
  };
  await writeDecisions(db, { compareRunId: run(8), observedAt: '2030-01-08T00:00:00Z', decisions: Object.values(more) });
  const one = async (fp, resolution, extra) => (await decide({ kind: 'approved', resolution, items: [item(await find(fp), extra)] })).j;
  assert.deepEqual((await one(N1, 'fix_ne', { target_text: '赤玉土 大粒 2L 2 個' })).skipped.map((x) => x.reason), ['resolution_not_allowed']);   // NE を直すは選べない
  assert.deepEqual((await one(N1, 'fix_cdb')).skipped.map((x) => x.reason), ['needs_target']);   // NE も空 = 本当の名前を入れる
  assert.equal((await one(N1, 'fix_cdb', { target_text: '赤玉土 大粒 2L 2 個' })).applied.length, 1);
  assert.equal((await find(N1)).decision.target.value, '赤玉土 大粒 2L 2 個');
  assert.equal((await one(N2, 'fix_cdb')).applied.length, 1);   // 既定 = NE の名前
  assert.equal((await find(N2)).decision.target.value, 'NE の名前');
  assert.deepEqual((await one(N3, 'fix_ne')).skipped.map((x) => x.reason), ['name_is_code']);   // 提案の値 (コード)
  assert.deepEqual((await one(N3, 'fix_ne', { target_text: 'AKADAMA-X' })).skipped.map((x) => x.reason), ['name_is_code']);   // 大文字・全角でも同じコード
  assert.deepEqual((await one(N3, 'fix_ne', { target_text: 'ＡＫＡＤＡＭＡ－Ｘ' })).skipped.map((x) => x.reason), ['name_is_code']);
  assert.equal((await one(N3, 'fix_ne', { target_text: '赤玉土 X' })).applied.length, 1);
  // 画面: 税率は NE の書き方 (10 / 8 = %)・社内は 10% (0.1)・提案は「NE を 10 (%) に」/ 新しい理由・提案・拒んだ理由の言葉
  const page = (await call('GET', '/')).text;
  const src = page.match(/<script>([\s\S]*?)<\/script>\s*<\/body>/)[1];
  const fns = new vm.Script(`(function () { const esc = (s) => String(s ?? ''); ${src.match(/const show = [\s\S]*?\n  const proposalText = [\s\S]*?return '決める'; };/)[0]}; return { showNe, showCdb, proposalText }; })()`).runInNewContext({});
  assert.deepEqual([fns.showNe('tax_rate', 0.1), fns.showCdb('tax_rate', 0.1), fns.showCdb('tax_rate', 0.08), fns.proposalText({ col: 'tax_rate', proposal: { op: 'set_ne_value', value: 0.1 } })],
    ['10%', '10% (0.1)', '8% (0.08)', 'NE を 10 (%) に']);
  assert.deepEqual([fns.showCdb('name', 'x'), fns.proposalText({ col: 'name', proposal: { op: 'fill_cdb_name' } }), fns.proposalText({ col: 'name', proposal: { op: 'set_cdb_value', value: 'NE の名前' } })],
    ['x', '社内 (ポータル) で本当の名前を入れる (NE も空)', '社内 (ポータル) の名前を NE の名前 NE の名前 に']);
  for (const w of ["cdb_name_is_code: '社内の名前がコードのまま", "name_is_code: '商品コードは名前ではない"]) assert.ok(page.includes(w), w);
});

server.close();
await pg.close();
console.log(`\n${passed} 件 PASS`);
