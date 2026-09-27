/**
 * test-master-concurrency-pg.mjs — 照合の判断の台帳 (0032) と最後に一致した値 (0033) の**同時実行**を、実 PostgreSQL の独立した 2 接続で確かめる
 *   (PGlite は 1 接続なので書けない。Codex D2-R1 の合格条件・#1475 R2 / R3 の残り)
 *
 * 固定する契約:
 *   1 0033 初回の競合: 両方が札なしを読む → 先の回が commit = 後の回は待ってから mark_moved
 *   2 0033 初回の競合: 先の回が rollback = 後の回が待ってから初回として通る
 *   3 0033 分けて送る途中に別の回が来る = 別の回は待ち、先の回の続きは通り、commit の後に別の回は mark_moved
 *   4 0033 札を読んだ後に別の回が受け付けられた (Codex D2-R0 High の順序) = 変更ゼロの回も mark_moved
 *   5 0032 候補の並行: 同じ指紋の組を逆の順で 2 つの取引が書く = デッドロックしない・見た回数を少なく数えない
 *   6 0032 × 画面 (D2'): 照合の完了と画面の承認が同じ候補を取り合う = 画面は待ち・完了は古い承認にだけ
 *   7 画面どうし: 2 人が同じ画面から同じ差を決める = 後の人は待ってから decided_meanwhile (両方は書かない)
 * 使い方: TEST_PG_URL=postgres://postgres:pw@localhost:54329/postgres node scripts/test-master-concurrency-pg.mjs
 *   🚨 使い捨ての PostgreSQL だけ (新しい DB を作って最後に消す)。localhost 以外の URL は拒む (本番を渡さない)。package.json の試験には入れない (PostgreSQL が要る)
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { openPgClient, pgAdapter, applyMigrations } from './company-db/migrate.mjs';

const url = process.env.TEST_PG_URL || '';
if (!url) { console.log('⏭️ TEST_PG_URL が無い (実 PostgreSQL の同時実行の試験は飛ばす)'); process.exit(0); }
const u0 = new URL(url);
if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(u0.hostname)) { console.error('localhost 以外の PostgreSQL には流さない'); process.exit(2); }

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
/** 結果を待たずに投げる (待っている = done が false のまま) */
const launch = (p) => { const s = { done: false }; s.promise = p.then((r) => { s.done = true; return { ok: r }; }, (e) => { s.done = true; return { err: e }; }); return s; };

const dbName = `cdb_conc_${crypto.randomBytes(4).toString('hex')}`;
const admin = await openPgClient(url);
await admin.query(`create database ${dbName}`);
const u = new URL(url); u.pathname = `/${dbName}`;
const M = await openPgClient(u.toString()), A = await openPgClient(u.toString()), Bc = await openPgClient(u.toString());
try {
  await applyMigrations(pgAdapter(M), { log: () => {} });
  const run = (i) => `mc_20300101T0000000${String(i).padStart(2, '0')}Z_abcdef`;
  const dayOf = (i) => new Date(Date.UTC(2030, 0, 10) + i * 86400000).toISOString().slice(0, 10);
  const gen = (i) => ({ products_at: `${dayOf(i)} 07:00:00`, products_rev: String(100 + i), sets_at: `${dayOf(i)} 07:00:01`, sets_rev: String(200 + i), cdb_read_at: `${dayOf(i)}T08:40:00.000Z` });
  const call = (c, p) => c.query('select ops.record_ne_baseline($1::jsonb) as r', [JSON.stringify({ norm_version: 1, units: [], ...p })]);
  const unit = (code, value) => ({ code_norm: code, col: 'name', value, cdb_version: null, prev_hash: null, prev_version: null });
  const markRun = async () => (await M.query('select compare_run_id from ops.master_ne_baseline_mark')).rows[0]?.compare_run_id ?? null;
  const reset = async () => { await M.query('delete from ops.master_ne_baseline'); await M.query('delete from ops.master_ne_baseline_mark'); };

  await ta('[1] 0033 初回の競合: 両方が札なしを読む → 先が commit = 後は待ってから mark_moved', async () => {
    await A.query('begin');
    await call(A, { compare_run_id: run(1), expected_mark: null, generation: gen(1), units: [unit('a1', 'A')] });
    const b = launch(call(Bc, { compare_run_id: run(2), expected_mark: null, generation: gen(2) }));
    await sleep(400);
    assert.equal(b.done, false, '後の回が待っていない (初回が直列になっていない)');
    await A.query('commit');
    const r = await b.promise;
    assert.ok(r.err && /mark_moved/.test(r.err.message), String(r.err?.message ?? JSON.stringify(r.ok?.rows)));
    assert.equal(await markRun(), run(1));
  });

  await ta('[2] 0033 初回の競合: 先が rollback = 後は待ってから初回として通る (units = [])', async () => {
    await reset();
    await A.query('begin');
    await call(A, { compare_run_id: run(3), expected_mark: null, generation: gen(3), units: [unit('a1', 'A')] });
    const b = launch(call(Bc, { compare_run_id: run(4), expected_mark: null, generation: gen(4) }));
    await sleep(400);
    assert.equal(b.done, false);
    await A.query('rollback');
    const r = await b.promise;
    assert.ok(r.ok, r.err?.message);
    assert.equal(await markRun(), run(4));
    assert.equal(Number((await M.query('select count(*)::int as n from ops.master_ne_baseline')).rows[0].n), 0);
  });

  await ta('[3] 0033 分けて送る途中に別の回 = 別の回は待ち・先の続きは通り・commit の後に別の回は mark_moved', async () => {
    await A.query('begin');
    await call(A, { compare_run_id: run(5), expected_mark: run(4), generation: gen(5), units: [unit('a1', 'A')] });
    const b = launch(call(Bc, { compare_run_id: run(6), expected_mark: run(4), generation: gen(6) }));
    await sleep(400);
    assert.equal(b.done, false);
    await call(A, { compare_run_id: run(5), expected_mark: run(4), generation: gen(5), units: [unit('b2', 'B')] });   // 続き
    await A.query('commit');
    const r = await b.promise;
    assert.ok(r.err && /mark_moved/.test(r.err.message), r.err?.message);
    assert.equal(await markRun(), run(5));
    assert.equal(Number((await M.query('select count(*)::int as n from ops.master_ne_baseline')).rows[0].n), 2);
  });

  await ta('[4] 0033 札を読んだ後に別の回が受け付けられた = 変更ゼロの回も mark_moved (Codex D2-R0 High の順序を別の接続で)', async () => {
    const readA = await markRun(), readB = await markRun();   // 両方が run(5) を読んだ
    await call(A, { compare_run_id: run(7), expected_mark: readA, generation: gen(7), units: [{ ...unit('a1', 'Y'), prev_hash: (await M.query(`select value_hash from ops.master_ne_baseline where code_norm = 'a1'`)).rows[0].value_hash, prev_version: 1 }] });
    await assert.rejects(call(Bc, { compare_run_id: run(8), expected_mark: readB, generation: gen(8) }), /mark_moved/);
    assert.equal((await M.query(`select value from ops.master_ne_baseline where code_norm = 'a1'`)).rows[0].value, 'Y');
  });

  await ta('[5] 0032 候補の並行: 同じ指紋の組を逆の順で 2 つの取引が書く = デッドロックしない・見た回数を少なく数えない', async () => {
    const fp = (c) => c.repeat(64);
    const cand = (f) => ({ fingerprint: f, subject_key: 'value:x1', code_norm: 'x1', col: 'tax_rate', child: null, cls: 'ne_no_value', reason_kind: 'tax_fallback', semantic: 'tax_fallback@1',
      print: { f }, resolutions: ['accept_difference', 'fix_ne'], proposal: { op: 'decide' } });
    const rec = (c, runId, list) => c.query('select ops.record_decision_candidates($1::jsonb) as n', [JSON.stringify({ compare_run_id: runId, observed_at: '2030-01-02T00:00:00Z', decisions: list })]);
    await rec(M, run(20), [cand(fp('a')), cand(fp('b'))]);   // 候補を先に作る (観測 1 回ずつ)
    // A が a を持ったまま → B が [b, a] を書き始める → A が b を書く。
    //   関数が指紋の順 (a → b) に処理しないと、B が b を持って a を待ち・A が b を待つ = デッドロック。
    //   候補の行を先に for update しないと、B の数え直しが A の観測を見ずに少なく数える
    await A.query('begin');
    await rec(A, run(21), [cand(fp('a'))]);
    await Bc.query('begin');
    const b = launch(rec(Bc, run(22), [cand(fp('b')), cand(fp('a'))]));
    await sleep(400);
    assert.equal(b.done, false, 'B が A の候補の行を待っていない');
    await rec(A, run(21), [cand(fp('b'))]);
    await A.query('commit');
    const r = await b.promise;
    assert.ok(r.ok, r.err?.message);   // デッドロック (40P01) にならない
    await Bc.query('commit');
    const rows = (await M.query(`select fingerprint, seen_count from ops.master_decision_candidates order by fingerprint`)).rows;
    assert.deepEqual(rows.map((x) => Number(x.seen_count)), [3, 3], JSON.stringify(rows));   // 観測 3 回 (20・21・22) を少なく数えない
  });

  await ta('[6] 0032 × 画面 (D2\'): 照合の完了と画面の承認が同じ候補を取り合う = 画面は待ち・完了は古い承認にだけ・画面の新しい承認は通る', async () => {
    const { applyDecisions } = await import('../apps/master-decisions/decide.mjs');
    const f = 'c'.repeat(64);
    await M.query('select ops.record_decision_candidates($1::jsonb)', [JSON.stringify({ compare_run_id: run(30), observed_at: '2030-02-01T00:00:00Z', decisions: [{ fingerprint: f, subject_key: 'value:c1', code_norm: 'c1',
      col: 'tax_rate', child: null, cls: 'ne_no_value', reason_kind: 'tax_fallback', semantic: 'tax_fallback@1', print: { f }, resolutions: ['accept_difference', 'fix_ne'], proposal: { op: 'set_ne_value', value: 0.1 } }] })]);
    const e1 = Number((await M.query(`insert into ops.master_decision_events (fingerprint, kind, resolution, target, actor_type, actor) values ($1, 'approved', 'fix_ne', $2::jsonb, 'user', 'naka@test') returning event_id`,
      [f, JSON.stringify({ subject_key: 'value:c1', col: 'tax_rate', child: null, value: 0.1 })])).rows[0].event_id);
    await A.query('begin');
    const ok = (await A.query('select ops.record_decision_done($1::bigint, $2, $3::jsonb) as ok', [e1, run(31), JSON.stringify({ side: 'ne', subject_key: 'value:c1', col: 'tax_rate', child: null, value: 0.1 })])).rows[0].ok;
    assert.equal(ok, true);
    const b = launch(applyDecisions(pgAdapter(Bc), { actor: 'naka@test', kind: 'approved', resolution: 'accept_difference', items: [{ fingerprint: f, shown_last_seen_run: run(30), shown_event_id: e1 }] }));
    await sleep(400);
    assert.equal(b.done, false, '画面の承認が候補の行を待っていない (for update が無い)');
    await A.query('commit');
    const r = await b.promise;
    assert.ok(r.ok, r.err?.message);
    assert.equal(r.ok.applied.length, 1, JSON.stringify(r.ok));
    const ev = (await M.query(`select event_id, kind, approved_event_id from ops.master_decision_events where fingerprint = $1 order by event_id`, [f])).rows.map((x) => [x.kind, x.approved_event_id == null ? null : Number(x.approved_event_id)]);
    assert.deepEqual(ev, [['approved', null], ['action_done', e1], ['approved', null]]);   // 完了は古い承認 (e1) にだけ
  });

  await ta('[7] 画面 (D2\') どうし: 2 人が同じ画面 (同じ最新の判断) から同じ差を決める = 後の人は待ってから decided_meanwhile (両方は書かない)', async () => {
    const { applyDecisions } = await import('../apps/master-decisions/decide.mjs');
    const f = 'c'.repeat(64);
    const last = Number((await M.query(`select max(event_id) as e from ops.master_decision_events where fingerprint = $1 and kind in ('approved', 'rejected', 'revoked')`, [f])).rows[0].e);
    // 先の人の決定の途中 (候補の行を取って出来事を書いた・まだ commit していない) を A で作る
    await A.query('begin');
    await A.query('select 1 from ops.master_decision_candidates where fingerprint = $1 for update', [f]);
    await A.query(`insert into ops.master_decision_events (fingerprint, kind, actor_type, actor, shown_fingerprint) values ($1, 'rejected', 'user', 'first@test', $1)`, [f]);
    const b = launch(applyDecisions(pgAdapter(Bc), { actor: 'second@test', kind: 'rejected', items: [{ fingerprint: f, shown_last_seen_run: run(30), shown_event_id: last }] }));
    await sleep(400);
    assert.equal(b.done, false, '後の人が候補の行を待っていない (for update が無い)');
    await A.query('commit');
    const r = await b.promise;
    assert.ok(r.ok, r.err?.message);
    assert.deepEqual([r.ok.applied.length, r.ok.skipped.map((x) => x.reason)], [0, ['decided_meanwhile']]);
    const n = Number((await M.query(`select count(*)::int as n from ops.master_decision_events where fingerprint = $1 and actor = 'second@test'`, [f])).rows[0].n);
    assert.equal(n, 0);
  });
} finally {
  for (const c of [A, Bc, M]) { try { await c.end(); } catch { /* */ } }
  try { await admin.query(`drop database ${dbName}`); } catch (e) { console.error(`DB を消せない: ${e.message}`); }
  await admin.end();
}
console.log(`\n${passed} 件 PASS`);
