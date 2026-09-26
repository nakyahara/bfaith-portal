/**
 * test-master-decisions.mjs — 照合 ② の判断の台帳 (migration 0032・apps/company-db/master-compare/decisions.mjs。Company DB構想 10 §6.1.1「D1 判断の台帳の契約 v3」)
 *
 * 固定する契約:
 *   1 候補: 同じ回の入れ直しで二重に数えない / 古い回の入れ直しで最後に見た日時を巻き戻さない / 不変の列は変えられない・消せない
 *   2 出来事は追記だけ (直す・消すは拒む)。approved は解決つき (その候補の選べる解決の中から)・直す解決は目標値つき・完了は system だけ・判断は user だけ
 *   3 record_decision_done: その approved がまだその指紋の最新の判断で、まだ完了していないときだけ書く (取り消し・別の承認・二重・差を残す承認には書かない)
 *   4 権限: watch_writer は表に直接書けない (承認つきの行を作れない)・関数は実行できる / watcher は読めるが関数は実行できない (Render と同じ条件の実行者でロールを作る)
 *   5 読む: 表が無い = not_applied / 読めない = unreadable (取引は壊さない) / 最新の判断と完了の一覧
 *   6 照合 ②: 台帳が読めない = blocked (decisions_unreadable)
 *   7 本番と同じ順 (ロールが先・0032 が後) の権限 / 8 完了の観測は承認の目標と照らす (空・側・単位・値の違いを拒む)
 * 使い方: node scripts/test-master-decisions.mjs
 */
import assert from 'node:assert/strict';

const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { createRoles } = await import('./company-db/create-watch-roles.mjs');
const { readDecisionLedger, writeDecisions } = await import('../apps/company-db/master-compare/decisions.mjs');
const { compareNe } = await import('../apps/company-db/master-compare/compare-ne.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};
const run = (i) => `mc_20300101T0000000${String(i).padStart(2, '0')}Z_abcdef`;
const fpOf = (c) => c.repeat(64);
const cand = (fp, extra = {}) => ({ fingerprint: fp, subject_key: 'value:x1', code_norm: 'x1', col: 'tax_rate', child: null, cls: 'ne_no_value', reason_kind: 'tax_fallback', semantic: 'tax_fallback@1',
  print: { code_norm: 'x1', semantic: 'tax_fallback@1' }, resolutions: ['accept_difference', 'fix_ne'], proposal: { op: 'decide' }, ...extra });

// 実行者 deploy = Render の default user の形 (superuser でない・CREATEROLE だけ)。DB と表の owner
const pg = new PGlite();
await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
await pg.query('set role deploy');
const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });
const q = (sql, p) => pg.query(sql, p);
const cands = async () => (await q('select fingerprint, first_seen_run, last_seen_run, seen_count from ops.master_decision_candidates order by fingerprint')).rows;
const approve = async (fp, resolution, target = null, actor = 'naka@example.com') => Number((await q(`insert into ops.master_decision_events (fingerprint, kind, resolution, target, actor_type, actor) values ($1, 'approved', $2, $3::jsonb, 'user', $4) returning event_id`, [fp, resolution, target ? JSON.stringify(target) : null, actor])).rows[0].event_id);
const judge = async (fp, kind) => q(`insert into ops.master_decision_events (fingerprint, kind, actor_type, actor) values ($1, $2, 'user', 'naka@example.com')`, [fp, kind]);
/** 完了を書く (観測 = 承認の目標そのもの。側は解決から) */
const done = async (id, r = run(9), obs = null) => {
  const ev = (await q('select resolution, target from ops.master_decision_events where event_id = $1', [id])).rows[0];
  const o = obs ?? (ev && ev.target ? { side: ev.resolution === 'fix_cdb' ? 'cdb' : 'ne', subject_key: ev.target.subject_key, col: ev.target.col, child: ev.target.child ?? null, value: ev.target.value } : { side: 'ne' });
  return (await q('select ops.record_decision_done($1::bigint, $2::text, $3::jsonb) as ok', [id, r, JSON.stringify(o)])).rows[0].ok;
};

await ta('[1] 候補: 同じ回は二重に数えない・古い回で最後に見た日時を巻き戻さない・不変の列は変えられない・消せない', async () => {
  const A = fpOf('a');
  await writeDecisions(db, { compareRunId: run(2), observedAt: '2030-01-02T00:00:00Z', decisions: [cand(A)] });
  await writeDecisions(db, { compareRunId: run(2), observedAt: '2030-01-02T00:00:00Z', decisions: [cand(A)] });
  await writeDecisions(db, { compareRunId: run(1), observedAt: '2030-01-01T00:00:00Z', decisions: [cand(A)] });   // 古い回の入れ直し
  assert.deepEqual(await cands(), [{ fingerprint: A, first_seen_run: run(1), last_seen_run: run(2), seen_count: 2 }]);
  await assert.rejects(q(`update ops.master_decision_candidates set print = '{"x":1}'::jsonb where fingerprint = $1`, [A]), /不変の列/);
  await assert.rejects(q(`update ops.master_decision_candidates set last_seen_at = '2020-01-01' where fingerprint = $1`, [A]), /巻き戻さない/);
  await assert.rejects(q(`delete from ops.master_decision_candidates where fingerprint = $1`, [A]), /消さない/);
  await assert.rejects(writeDecisions(db, { compareRunId: 'bad', observedAt: '2030-01-02T00:00:00Z', decisions: [cand(A)] }), /compare_run_id/);
  await assert.rejects(writeDecisions(db, { compareRunId: run(3), observedAt: '2030-01-03T00:00:00Z', decisions: [cand(A, { resolutions: ['approve_all'] })] }), /知らない解決/);
});

await ta('[2] 出来事は追記だけ・approved は解決つき・直す解決は目標値つき・完了は system・判断は user', async () => {
  const A = fpOf('a');
  const id = await approve(A, 'accept_difference');
  await assert.rejects(q('update ops.master_decision_events set note = $2 where event_id = $1', [id, 'x']), /追記だけ/);
  await assert.rejects(q('delete from ops.master_decision_events where event_id = $1', [id]), /追記だけ/);
  await assert.rejects(q(`insert into ops.master_decision_events (fingerprint, kind, actor_type, actor) values ($1, 'approved', 'user', 'x')`, [A]), /ck_mde_resolution/);
  await assert.rejects(approve(A, 'fix_ne'), /ck_mde_fix_target/);
  await assert.rejects(approve(A, 'spec'), /選べる解決に無い/);   // 候補 A の選べる解決 = accept_difference / fix_ne だけ
  await assert.rejects(approve(A, 'fix_cdb', { subject_key: 'value:x1', col: 'tax_rate', value: 0.1 }), /選べる解決に無い/);
  await assert.rejects(q(`insert into ops.master_decision_events (fingerprint, kind, approved_event_id, actor_type, actor) values ($1, 'action_done', $2, 'user', 'x')`, [A, id]), /ck_mde_done_system/);
  await assert.rejects(q(`insert into ops.master_decision_events (fingerprint, kind, resolution, actor_type, actor) values ($1, 'approved', 'accept_difference', 'system', 'x')`, [A]), /ck_mde_user_judgment/);
});

await ta('[3] record_decision_done: 最新の判断で・まだ完了していない直す承認にだけ書く (取り消し・別の承認・二重・差を残す承認・無い番号は書かない)', async () => {
  const B = fpOf('b');
  await writeDecisions(db, { compareRunId: run(4), observedAt: '2030-01-04T00:00:00Z', decisions: [cand(B, { resolutions: ['accept_difference', 'fix_ne', 'fix_cdb'] })] });
  const a1 = await approve(B, 'fix_ne', { subject_key: 'value:x1', col: 'tax_rate', value: 0.1 });
  assert.equal(await done(a1), true);
  assert.equal(await done(a1), false);   // 二重
  const a2 = await approve(B, 'fix_ne', { subject_key: 'value:x1', col: 'tax_rate', value: 0.08 });
  await judge(B, 'revoked');
  assert.equal(await done(a2), false);   // 取り消した
  const a3 = await approve(B, 'fix_cdb', { subject_key: 'value:x1', col: 'tax_rate', value: 0.08 });
  assert.equal(await done(a2), false);   // 別の承認が後にある (遅れて届いた完了を新しい承認に効かせない)
  assert.equal(await done(a3), true);
  const a4 = await approve(B, 'accept_difference');
  assert.equal(await done(a4), false);   // 差を残す承認に完了は無い
  assert.equal(await done(999999), false);
  await assert.rejects(q('select ops.record_decision_done($1::bigint, $2::text, $3::jsonb)', [a4, 'bad', '{}']), /compare_run_id/);
  const n = (await q(`select count(*)::int as n from ops.master_decision_events where kind = 'action_done' and fingerprint = $1`, [B])).rows[0].n;
  assert.equal(n, 2);
});

await ta('[4] 権限: watch_writer は表に直接書けない (承認つきの行を作れない)・関数は実行できる / watcher は読めるが関数は実行できない', async () => {
  const C = fpOf('c');
  await q('reset role'); await q('set role watch_writer');
  try {
    await assert.rejects(q(`insert into ops.master_decision_candidates (fingerprint, subject_key, code_norm, col, cls, reason_kind, semantic, print, resolutions, first_seen_run, first_seen_at, last_seen_run, last_seen_at)
      values ($1, 'value:x', 'x', 'name', 'rule', 'none', 'none@1', '{}', '[]', $2, now(), $2, now())`, [C, run(5)]), /permission denied/);
    await assert.rejects(q(`insert into ops.master_decision_events (fingerprint, kind, resolution, actor_type, actor) values ($1, 'approved', 'accept_difference', 'user', 'forged')`, [fpOf('a')]), /permission denied/);
    await assert.rejects(q(`update ops.master_decision_candidates set seen_count = 99`), /permission denied/);
    assert.equal((await q('select ops.record_decision_candidates($1::jsonb) as n', [JSON.stringify({ compare_run_id: run(5), observed_at: '2030-01-05T00:00:00Z', decisions: [cand(C)] })])).rows[0].n, 1);
    assert.equal((await q('select ops.record_decision_done($1::bigint, $2::text, $3::jsonb) as ok', [1, run(5), '{}'])).rows[0].ok, false);
  } finally { await q('reset role'); }
  await q('set role watcher');
  try {
    assert.ok((await q('select count(*)::int as n from ops.master_decision_events')).rows[0].n >= 1);
    await assert.rejects(q('select ops.record_decision_candidates($1::jsonb)', [JSON.stringify({ compare_run_id: run(6), observed_at: '2030-01-06T00:00:00Z', decisions: [] })]), /permission denied/);
  } finally { await q('reset role'); await q('set role deploy'); }
});

await ta('[5] 読む: 表が無い = not_applied / 最新の判断と完了の一覧 / 読めない = unreadable (取引は壊さない)', async () => {
  const pg0 = new PGlite(); const db0 = pgliteAdapter(pg0);
  await applyMigrations(db0, { log: quiet, to: '0031' });
  await db0.query('begin');
  assert.equal((await readDecisionLedger(db0)).state, 'not_applied');
  await db0.query('rollback'); await pg0.close();
  await db.query('begin transaction isolation level repeatable read read only');
  const L = await readDecisionLedger(db);
  await db.query('rollback');
  assert.equal(L.state, 'ok');
  assert.deepEqual([L.latest.get(fpOf('b')).kind, L.latest.get(fpOf('b')).resolution], ['approved', 'accept_difference']);
  assert.equal(L.done.size, 2);
  // 読めない (問い合わせが落ちる) = unreadable。savepoint で取引を壊さない = 後の問い合わせが通る
  await db.query('begin');
  const broken = { query: (sql, p) => (/master_decision_events where kind in/.test(sql) ? db.query('select * from no_such_table') : db.query(sql, p)) };
  const U = await readDecisionLedger(broken);
  assert.equal(U.state, 'unreadable');
  assert.equal((await db.query('select 1 as x')).rows[0].x, 1);
  await db.query('rollback');
});

await ta('[6] 照合 ②: 台帳が読めない = blocked (decisions_unreadable)。「承認なし」と読まない', async () => {
  const r = compareNe({ dataDir: '/nonexistent', asOfJst: '2030-01-01', cdb: null, ledger: null, decisionLedger: { state: 'unreadable', reason: 'x' } });
  assert.deepEqual([r.result.verdict, r.result.blocked_reason, r.result.decisions_read], ['blocked', 'decisions_unreadable', 'unreadable']);
});

await ta('[7] 本番と同じ順 (ロールが先・0032 が後): migration だけで watch_writer は関数を実行でき、watcher・ほかのロールは実行できない・watcher は読める', async () => {
  const p2 = new PGlite();
  try {
    await p2.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
    await p2.query(`alter database ${(await p2.query('select current_database() as d')).rows[0].d} owner to deploy`);
    await p2.query('set role deploy');
    const d2 = pgliteAdapter(p2);
    await applyMigrations(d2, { log: quiet, to: '0031' });
    await createRoles(p2, { watcherPw: 'a', writerPw: 'b' });
    await p2.query(`create role someone login`);
    await applyMigrations(d2, { log: quiet });   // 0032 だけ
    const payload = JSON.stringify({ compare_run_id: run(7), observed_at: '2030-01-07T00:00:00Z', decisions: [cand(fpOf('d'))] });
    await p2.query('reset role'); await p2.query('set role watch_writer');
    assert.equal((await p2.query('select ops.record_decision_candidates($1::jsonb) as n', [payload])).rows[0].n, 1);
    await p2.query('reset role'); await p2.query('set role watcher');
    assert.equal((await p2.query('select count(*)::int as n from ops.master_decision_candidates')).rows[0].n, 1);
    await assert.rejects(p2.query('select ops.record_decision_candidates($1::jsonb)', [payload]), /permission denied/);
    await p2.query('reset role'); await p2.query('set role someone');
    await assert.rejects(p2.query('select ops.record_decision_done(1, $1, $2::jsonb)', [run(7), '{}']), /permission denied/);
  } finally { await p2.close(); }
});

await ta('[8] 完了の観測は承認の目標と照らす: 空・側違い・単位違い・値違いは拒む (呼び手の誤りを完了にしない)', async () => {
  const E = fpOf('e');
  await writeDecisions(db, { compareRunId: run(8), observedAt: '2030-01-08T00:00:00Z', decisions: [cand(E)] });
  const id = await approve(E, 'fix_ne', { subject_key: 'value:x1', col: 'tax_rate', child: null, value: 0.1 });
  const call = (o) => q('select ops.record_decision_done($1::bigint, $2::text, $3::jsonb) as ok', [id, run(8), o === undefined ? null : JSON.stringify(o)]);
  const ok = { side: 'ne', subject_key: 'value:x1', col: 'tax_rate', child: null, value: 0.1 };
  await assert.rejects(call(undefined), /観測が無い/);
  await assert.rejects(call([]), /観測が無い/);
  await assert.rejects(call({ ...ok, side: 'cdb' }), /側が承認と違う/);
  await assert.rejects(call({ ...ok, col: 'name' }), /単位が目標と違う/);
  await assert.rejects(call({ ...ok, child: 'c1' }), /単位が目標と違う/);
  await assert.rejects(call({ ...ok, value: 999 }), /目標値と違う/);
  assert.equal((await call(ok)).rows[0].ok, true);
});

await pg.close();
console.log(`\n${passed} 件 PASS`);
process.exit(process.exitCode || 0);
