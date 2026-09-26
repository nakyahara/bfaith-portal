/**
 * test-master-baseline.mjs — 照合 ② の「最後に一致した値」(migration 0033・apps/company-db/master-compare/baseline.mjs。Company DB構想 10 §6.1.1「D2 最後に一致した値の契約 v2」+ D2-R1)
 *
 * 固定する契約 (関数 ops.record_ne_baseline):
 *   1 初回 (札なし・基準なし) に書ける・hash は関数が計算する
 *   2 札: 読んだ札の後に別の回が受け付けられた = 全部拒む (mark_moved)。送らなかった単位・変更ゼロの回も (Codex D2-R0 High の順序)
 *   3 世代が 1 成分でも後退した回は拒む (stale_run)。同値は可
 *   4 初回の競合: 先の回が commit = 後の回 (札なしを読んだ) は mark_moved / 先の回が rollback = 後の回が初回として通る / 札なし・基準あり = baseline_without_mark
 *   5 続き: 同じ回 ID・同じ取引・同じ前の札・世代だけ。別の取引で同じ ID = run_reused / 世代が違う = continuation_mismatch / 分けた送りの間の重複 = unit_conflict / 途中の失敗で全部巻き戻る
 *   6 単位: 読んだ時の hash と違う = unit_conflict / 同じ値は書かない (札は進む)
 *   7 正規化の版: 関数が受け付けない版 = norm_version_rejected / 版だけ違う単位は書き換える
 *   8 入力の検証 (回 ID・col・値の型・世代)
 *   9 権限: watch_writer は関数だけ・表は読めない書けない / watcher は読めるが関数は実行できない / public は実行できない / ロールが先・0033 が後・0033 の後のロールの作り直しでも残る
 *  10 writeBaseline: 5,000 単位ずつ同じ取引・後半の失敗で前半も巻き戻る
 *  11 値の正規化と方向 (baseline.mjs の純粋な関数)
 * 🚨 実 PostgreSQL の独立した 2 接続での同時実行は PGlite では書けない (順序の組み合わせで確かめる)
 * 使い方: node scripts/test-master-baseline.mjs
 */
import assert from 'node:assert/strict';

const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { createRoles } = await import('./company-db/create-watch-roles.mjs');
const B = await import('../apps/company-db/master-compare/baseline.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};
const run = (i) => `mc_20300101T0000000${String(i).padStart(2, '0')}Z_abcdef`;
/** 世代 (i が大きいほど新しい) */
const dayOf = (i) => new Date(Date.UTC(2030, 0, 10) + i * 86400000).toISOString().slice(0, 10);
const gen = (i, over = {}) => ({ products_at: `${dayOf(i)} 07:00:00`, products_rev: String(100 + i), sets_at: `${dayOf(i)} 07:00:01`,
  sets_rev: String(200 + i), cdb_read_at: `${dayOf(i)}T08:40:00.000Z`, ...over });

const pg = new PGlite();
await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
await pg.query('set role deploy');
const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });
const q = (sql, p) => pg.query(sql, p);
const call = async (p) => (await q('select ops.record_ne_baseline($1::jsonb) as r', [JSON.stringify({ norm_version: 1, units: [], ...p })])).rows[0].r;
const row = async (code, col) => (await q('select value, value_hash, norm_version, since_run from ops.master_ne_baseline where code_norm = $1 and col = $2', [code, col])).rows[0] || null;
const mark = async () => (await q('select compare_run_id, prev_run from ops.master_ne_baseline_mark where id = 1')).rows[0] || null;
const count = async () => Number((await q('select count(*)::int as n from ops.master_ne_baseline')).rows[0].n);
/** 単位 (前の値 = 今の行から読む) */
const unit = async (code, col, value, prevOverride) => {
  const r = prevOverride !== undefined ? prevOverride : await row(code, col);
  return { code_norm: code, col, value, cdb_version: 7, prev_hash: r ? r.value_hash : null, prev_version: r ? r.norm_version : null };
};
const reset = async () => { await q('delete from ops.master_ne_baseline'); await q('delete from ops.master_ne_baseline_mark'); };

await ta('[1] 初回 (札なし・基準なし) に書ける・hash は関数が計算する (呼び手の hash は信じない)', async () => {
  const r = await call({ compare_run_id: run(1), expected_mark: null, generation: gen(1), units: [await unit('a001', 'name', '単品A'), await unit('a001', 'cost', 100), await unit('s001', 'components', [['a001', 2], ['b002', 1]])] });
  assert.deepEqual(r, { inserted: 3, updated: 0 });
  const x = await row('a001', 'name');
  assert.equal(x.value, '単品A'); assert.equal(x.since_run, run(1));
  const h = (await q(`select encode(sha256(convert_to('"単品A"'::jsonb::text, 'UTF8')), 'hex') as h`)).rows[0].h;
  assert.equal(x.value_hash, h);
  assert.deepEqual(await mark(), { compare_run_id: run(1), prev_run: null });
});

await ta('[2] 札: 読んだ札の後に別の回が受け付けられた = 全部拒む (送らなかった単位・変更ゼロの回も。Codex D2-R0 High の順序)', async () => {
  // A・B とも札 run(1) を読んだ。A (古い観測で name = Y) が先に書く → B (新しい観測・変更ゼロ) は mark_moved
  await call({ compare_run_id: run(2), expected_mark: run(1), generation: gen(2), units: [await unit('a001', 'name', 'Y')] });
  await assert.rejects(call({ compare_run_id: run(3), expected_mark: run(1), generation: gen(3), units: [] }), /^error: mark_moved|mark_moved/);
  // 逆の順: B (変更ゼロ) が先 → A (name を Z に) は mark_moved = 古い観測で基準を書けない
  await call({ compare_run_id: run(4), expected_mark: run(2), generation: gen(4), units: [] });
  await assert.rejects(call({ compare_run_id: run(5), expected_mark: run(2), generation: gen(5), units: [await unit('a001', 'name', 'Z')] }), /mark_moved/);
  assert.equal((await row('a001', 'name')).value, 'Y');
  assert.deepEqual(await mark(), { compare_run_id: run(4), prev_run: run(2) });
});

await ta('[3] 世代が 1 成分でも後退した回は拒む (stale_run)。同値は可', async () => {
  for (const k of ['products_rev', 'sets_rev', 'products_at', 'sets_at', 'cdb_read_at']) {
    const g = gen(4, { [k]: gen(3)[k] });
    await assert.rejects(call({ compare_run_id: run(6), expected_mark: run(4), generation: g }), /stale_run/, k);
  }
  await call({ compare_run_id: run(6), expected_mark: run(4), generation: gen(4) });   // 同値
  assert.equal((await mark()).compare_run_id, run(6));
});

await ta('[4] 初回の競合: 先の回が commit = 後の回は mark_moved / rollback = 後の回が初回として通る (units = [] でも) / 札なし・基準あり = baseline_without_mark', async () => {
  await reset();
  // 両方が札なしを読んだ。先の回が commit
  await call({ compare_run_id: run(10), expected_mark: null, generation: gen(10), units: [await unit('a001', 'name', 'A')] });
  await assert.rejects(call({ compare_run_id: run(11), expected_mark: null, generation: gen(11), units: [] }), /mark_moved/);
  // 先の回が rollback
  await reset();
  await q('begin');
  await call({ compare_run_id: run(12), expected_mark: null, generation: gen(12), units: [await unit('a001', 'name', 'A')] });
  await q('rollback');
  assert.equal(await mark(), null); assert.equal(await count(), 0);
  assert.deepEqual(await call({ compare_run_id: run(13), expected_mark: null, generation: gen(13), units: [] }), { inserted: 0, updated: 0 });
  assert.equal((await mark()).compare_run_id, run(13));
  // 札なし・基準あり (復旧の途中など) = 拒む
  await call({ compare_run_id: run(14), expected_mark: run(13), generation: gen(14), units: [await unit('a001', 'name', 'A')] });
  await q('delete from ops.master_ne_baseline_mark');
  await assert.rejects(call({ compare_run_id: run(15), expected_mark: null, generation: gen(15), units: [] }), /baseline_without_mark/);
  // 読んだ札が今は無い (捨てた後) = mark_moved
  await q('delete from ops.master_ne_baseline');
  await assert.rejects(call({ compare_run_id: run(15), expected_mark: run(14), generation: gen(15), units: [] }), /mark_moved/);
});

await ta('[5] 続き: 同じ回 ID・同じ取引・同じ前の札と世代だけ / 別の取引 = run_reused / 世代違い = continuation_mismatch / 分けた送りの重複 = unit_conflict / 途中の失敗で全部巻き戻る', async () => {
  await reset();
  await call({ compare_run_id: run(20), expected_mark: null, generation: gen(20) });
  await q('begin');
  await call({ compare_run_id: run(21), expected_mark: run(20), generation: gen(21), units: [await unit('a001', 'name', 'A')] });
  await call({ compare_run_id: run(21), expected_mark: run(20), generation: gen(21), units: [await unit('b002', 'name', 'B')] });
  await q('commit');
  assert.equal(await count(), 2);
  await assert.rejects(call({ compare_run_id: run(21), expected_mark: run(20), generation: gen(21) }), /run_reused/);
  // 世代が最初の送りと違う
  await q('begin');
  await call({ compare_run_id: run(22), expected_mark: run(21), generation: gen(22), units: [await unit('c003', 'name', 'C')] });
  await assert.rejects(call({ compare_run_id: run(22), expected_mark: run(21), generation: gen(23) }), /continuation_mismatch/);
  await q('rollback');
  assert.equal(await row('c003', 'name'), null); assert.equal((await mark()).compare_run_id, run(21));
  // 前の札が最初の送りと違う
  await q('begin');
  await call({ compare_run_id: run(22), expected_mark: run(21), generation: gen(22) });
  await assert.rejects(call({ compare_run_id: run(22), expected_mark: run(20), generation: gen(22) }), /continuation_mismatch/);
  await q('rollback');
  // 分けた送りの間で同じ単位 (読んだ時の hash で 2 回) = unit_conflict → 全部巻き戻る
  const u1 = await unit('a001', 'name', 'A2');
  await q('begin');
  await call({ compare_run_id: run(23), expected_mark: run(21), generation: gen(23), units: [u1] });
  await assert.rejects(call({ compare_run_id: run(23), expected_mark: run(21), generation: gen(23), units: [{ ...u1, value: 'A3' }] }), /unit_conflict/);
  await q('rollback');
  assert.equal((await row('a001', 'name')).value, 'A'); assert.equal((await mark()).compare_run_id, run(21));
  // 同じ送りの中で同じ単位の新規 2 回 = 主キー違反 = 全部巻き戻る
  await assert.rejects(call({ compare_run_id: run(24), expected_mark: run(21), generation: gen(24), units: [await unit('d004', 'name', 'D'), await unit('d004', 'name', 'D')] }));
  assert.equal(await row('d004', 'name'), null); assert.equal((await mark()).compare_run_id, run(21));
});

await ta('[6] 単位: 読んだ時の hash と違う = unit_conflict / 読んだ時にあったのに無い = unit_conflict / 同じ値は書かない (札は進む)', async () => {
  const x = await row('a001', 'name');
  await assert.rejects(call({ compare_run_id: run(30), expected_mark: run(21), generation: gen(30), units: [await unit('a001', 'name', 'X', { value_hash: 'f'.repeat(64), norm_version: 1 })] }), /unit_conflict/);
  await assert.rejects(call({ compare_run_id: run(30), expected_mark: run(21), generation: gen(30), units: [await unit('zz99', 'name', 'X', x)] }), /unit_conflict/);
  await assert.rejects(call({ compare_run_id: run(30), expected_mark: run(21), generation: gen(30), units: [await unit('a001', 'name', 'X', null)] }), /unit_conflict/);   // 読んだ時に無かったのに今はある
  assert.deepEqual(await call({ compare_run_id: run(30), expected_mark: run(21), generation: gen(30), units: [await unit('a001', 'name', 'A')] }), { inserted: 0, updated: 0 });
  assert.equal((await row('a001', 'name')).since_run, run(21));   // 同じ値 = 行は書かない (初めて一致を見た回のまま)
  assert.equal((await mark()).compare_run_id, run(30));
});

await ta('[7] 正規化の版: 関数が受け付けない版 = norm_version_rejected / 版だけ違う単位は書き換える', async () => {
  await assert.rejects(call({ compare_run_id: run(31), expected_mark: run(30), generation: gen(31), norm_version: 2 }), /norm_version_rejected/);
  await q(`update ops.master_ne_baseline set norm_version = 9 where code_norm = 'a001' and col = 'name'`);   // 古い版の行 (の代わり)
  assert.deepEqual(await call({ compare_run_id: run(31), expected_mark: run(30), generation: gen(31), units: [await unit('a001', 'name', 'A')] }), { inserted: 0, updated: 1 });
  const x = await row('a001', 'name');
  assert.deepEqual([x.norm_version, x.since_run], [1, run(31)]);
});

await ta('[8] 入力の検証 (回 ID・札・col・値の型・世代・units)', async () => {
  const base = { compare_run_id: run(32), expected_mark: run(31), generation: gen(32) };
  const bad = async (p) => assert.rejects(call({ ...base, ...p }), /invalid_input/, JSON.stringify(p).slice(0, 120));
  await bad({ compare_run_id: 'x' });
  await bad({ expected_mark: 'y' });
  await bad({ generation: { ...gen(32), products_rev: '1.5' } });
  await bad({ generation: { ...gen(32), sets_at: '2030/01/01' } });
  await bad({ generation: { ...gen(32), cdb_read_at: null } });
  await bad({ generation: { ...gen(32), products_at: '2030-01-32 07:00:00' } });   // 形は合うが読めない日時
  await bad({ generation: { ...gen(32), cdb_read_at: 'きのう' } });
  await bad({ units: 'x' });
  const u = (col, value) => ({ units: [{ code_norm: 'q1', col, value, prev_hash: null, prev_version: null }] });
  await bad(u('price', 1));
  await bad(u('name', 3));
  await bad(u('exists', 'true'));
  await bad(u('kind', 'combo'));
  await bad(u('handling', null));
  await bad(u('tax_rate', '0.1'));
  await bad(u('cost', 0)); await bad(u('cost', -5)); await bad(u('standard_price_jpy', '100'));
  await bad(u('primary_supplier', [1])); await bad(u('primary_supplier', 'x'));
  await bad(u('components', [])); await bad(u('components', [['a', 0]])); await bad(u('components', [['a']])); await bad(u('components', [[1, 2]]));
  await bad({ units: [{ code_norm: '', col: 'name', value: 'x' }] });
  await assert.rejects(call({ ...base, norm_version: '1' }), /invalid_input/);
  // 型が正しければ通る (値なし = JSON の null)
  const ok = await call({ ...base, units: [...u('cost', null).units, { ...u('standard_price_jpy', 1200).units[0], code_norm: 'q2' }, { ...u('primary_supplier', []).units[0], code_norm: 'q3' },
    { ...u('exists', true).units[0], code_norm: 'q4' }, { ...u('kind', 'set').units[0], code_norm: 'q5' }, { ...u('tax_rate', 0.08).units[0], code_norm: 'q6' }] });
  assert.deepEqual(ok, { inserted: 6, updated: 0 });
});

await ta('[9] 権限: watch_writer は関数だけ・表は読めない書けない / watcher は読めるが実行できない / public は実行できない / ロールが先・0033 が後・0033 の後の作り直しでも残る', async () => {
  await q('reset role'); await q('set role watch_writer');
  try {
    assert.ok(await call({ compare_run_id: run(33), expected_mark: run(32), generation: gen(33) }));
    await assert.rejects(q('select * from ops.master_ne_baseline'), /permission denied/);
    await assert.rejects(q(`insert into ops.master_ne_baseline_mark (id, compare_run_id, accepted_txid, norm_version, ne_products_at, ne_products_rev, ne_sets_at, ne_sets_rev, cdb_read_at)
      values (1, $1, 1, 1, now(), 1, now(), 1, now())`, [run(34)]), /permission denied/);
    await assert.rejects(q(`update ops.master_ne_baseline set value = '"x"'`), /permission denied/);
  } finally { await q('reset role'); }
  await q('set role watcher');
  try {
    assert.ok((await q('select count(*)::int as n from ops.master_ne_baseline')).rows[0].n >= 1);
    assert.equal((await q('select compare_run_id from ops.master_ne_baseline_mark')).rows[0].compare_run_id, run(33));
    await assert.rejects(call({ compare_run_id: run(34), expected_mark: run(33), generation: gen(34) }), /permission denied/);
  } finally { await q('reset role'); await q('set role deploy'); }
  // 本番と同じ順 (ロールが先・0033 が後) と、0033 の後にロールを作り直しても実行権が残る
  const p2 = new PGlite();
  try {
    await p2.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
    await p2.query(`alter database ${(await p2.query('select current_database() as d')).rows[0].d} owner to deploy`);
    await p2.query('set role deploy');
    const d2 = pgliteAdapter(p2);
    await applyMigrations(d2, { log: quiet, to: '0032' });
    await createRoles(p2, { watcherPw: 'a', writerPw: 'b' });
    await p2.query('create role someone login');
    await applyMigrations(d2, { log: quiet });   // 0033 だけ
    const c2 = (p) => p2.query('select ops.record_ne_baseline($1::jsonb) as r', [JSON.stringify({ norm_version: 1, units: [], ...p })]);
    await p2.query('reset role'); await p2.query('set role watch_writer');
    await c2({ compare_run_id: run(40), expected_mark: null, generation: gen(40) });
    await p2.query('reset role'); await p2.query('set role someone');
    await assert.rejects(c2({ compare_run_id: run(41), expected_mark: run(40), generation: gen(41) }), /permission denied/);
    await p2.query('reset role'); await p2.query('set role deploy');
    await createRoles(p2, { watcherPw: 'a2', writerPw: 'b2' });   // 流し直し
    await p2.query('reset role'); await p2.query('set role watch_writer');
    await c2({ compare_run_id: run(41), expected_mark: run(40), generation: gen(41) });
    await p2.query('reset role'); await p2.query('set role watcher');
    await assert.rejects(c2({ compare_run_id: run(42), expected_mark: run(41), generation: gen(42) }), /permission denied/);
    assert.equal((await p2.query('select compare_run_id from ops.master_ne_baseline_mark')).rows[0].compare_run_id, run(41));
  } finally { await p2.close(); }
});

await ta('[10] writeBaseline: 5,000 単位ずつ同じ取引 (12,000 単位 = 3 回)・後半の失敗で前半も札も巻き戻る', async () => {
  await reset();
  const units = Array.from({ length: 12000 }, (_, i) => ({ code_norm: `z${String(i).padStart(5, '0')}`, col: 'name', value: `n${i}`, cdb_version: null, prev_hash: null, prev_version: null }));
  const calls = [];
  const spy = { query: (sql, p) => { if (/record_ne_baseline/.test(sql)) calls.push(JSON.parse(p[0]).units.length); return db.query(sql, p); } };
  const r = await B.writeBaseline(spy, { compareRunId: run(50), expectedMark: null, generation: gen(50), units });
  assert.deepEqual(calls, [5000, 5000, 2000]);
  assert.deepEqual(r, { inserted: 12000, updated: 0, units: 12000 });
  assert.equal(await count(), 12000);
  // 後半 (3 回目) の単位が不正 = 1・2 回目の書き換えも札も巻き戻る
  const next = units.map((u) => ({ ...u, value: `${u.value}x` }));
  for (const u of next) { const x = await row(u.code_norm, 'name'); u.prev_hash = x.value_hash; u.prev_version = 1; }
  next[11000].value = 5;
  await assert.rejects(B.writeBaseline(db, { compareRunId: run(51), expectedMark: run(50), generation: gen(51), units: next }), /invalid_input/);
  assert.equal((await row('z00000', 'name')).value, 'n0');
  assert.equal((await mark()).compare_run_id, run(50));
  // 変更ゼロでも関数を 1 回呼んで札を進める
  calls.length = 0;
  await B.writeBaseline(spy, { compareRunId: run(52), expectedMark: run(50), generation: gen(52), units: [] });
  assert.deepEqual(calls, [0]);
  assert.equal((await mark()).compare_run_id, run(52));
});

await ta('[11] 値の正規化と方向 (0 と空は同じ値なし・CDB の 0 も null・仕入先の空 = []・構成は並べる・世代・4 時間)', async () => {
  const st = (raw, value, validity = 'ok') => ({ raw, value, validity });
  assert.deepEqual(B.neValue('standard_price_jpy', st('zero', 0)), { ok: true, value: null });
  assert.deepEqual(B.neValue('cost', st('empty', null)), { ok: true, value: null });
  assert.equal(B.cdbValue('cost', 0), null);
  assert.deepEqual(B.neValue('tax_rate', st('zero', null, 'invalid')), { ok: false });
  assert.deepEqual(B.neValue('handling', st('empty', null)), { ok: true, value: 'unknown' });
  assert.equal(B.cdbValue('handling', null), 'unknown');
  assert.deepEqual(B.neValue('primary_supplier', st('empty', null)), { ok: true, value: [] });
  assert.deepEqual(B.cdbValue('primary_supplier', ['b', 'a']), ['a', 'b']);
  assert.deepEqual(B.neValue('name', st('unknown', null, 'invalid')), { ok: false });
  assert.deepEqual(B.componentsValue([['b2', 1], ['a1', 2]]), [['a1', 2], ['b2', 1]]);
  const base = (v) => ({ value: v, norm_version: B.BASELINE_NORM_VERSION });
  assert.equal(B.directionOf(null, null, null), null);
  assert.equal(B.directionOf(1, 2, null), 'unknown');
  assert.equal(B.directionOf(1, 2, { value: 1, norm_version: 99 }), 'unknown');
  assert.equal(B.directionOf(1, 2, base(1)), 'to_ne');
  assert.equal(B.directionOf(1, 2, base(2)), 'ne_changed');
  assert.equal(B.directionOf(1, 2, base(3)), 'conflict');
  assert.equal(B.directionOf([['a', 1], ['b', 2]], [['a', 1], ['b', 3]], base([['a', 1], ['b', 2]])), 'to_ne');
  // 世代: 5 成分・同値は可・読めない = unreadable・rev は大きな整数でも精度を落とさない
  const g = { products_at: '2030-01-10 07:00:00', products_rev: '9007199254740993', sets_at: '2030-01-10 07:00:01', sets_rev: '5', cdb_read_at: '2030-01-09T23:40:00.000Z' };
  assert.deepEqual(B.regressedComponents(g, g), []);
  assert.deepEqual(B.regressedComponents({ ...g, products_rev: '9007199254740992' }, g), ['products_rev']);
  assert.deepEqual(B.regressedComponents({ ...g, cdb_read_at: 'x' }, g), ['unreadable']);
  assert.deepEqual(B.regressedComponents(g, { ...g, products_at: '2030-01-09T22:00:00.000Z' }), []);   // DB の札は ISO
  // 4 時間: 単品・セットのそれぞれ。ちょうど = 可
  const g4 = { ...g, products_at: '2030-01-10 04:40:00', sets_at: '2030-01-10 04:40:00', cdb_read_at: '2030-01-10T08:40:00.000Z' };
  assert.equal(B.withinGap(g4), true);
  assert.equal(B.withinGap({ ...g4, products_at: '2030-01-10 04:39:59' }), false);
  assert.equal(B.withinGap({ ...g4, sets_at: '2030-01-10 04:39:59' }), false);
  assert.equal(B.withinGap({ ...g4, cdb_read_at: null }), false);
});

await pg.close();
console.log(`\n${passed} 件 PASS`);
