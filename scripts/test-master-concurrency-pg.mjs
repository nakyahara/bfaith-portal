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
 *   8 0036 親子の守り: ほかの接続だけが鍵を持つ = 自分の書き込みは拒む (鍵を借りられない)。鍵を取りに行った接続は、持ち主の commit まで待つ
 *   9 0036 鍵 → 行の順: 夜間ロード (取引の冒頭で鍵 → 商品の行の UPDATE → 親子) の途中に人の付け外しが来ても、人は鍵で待つ = 待ち合わない (デッドロックしない)
 *  10 0036 本物の夜間ロード (runInitialLoad): 人が鍵を持っている間は、ロードは**商品の行に触る前**に鍵で待つ (行の鍵を持ったまま待たない)。人の commit の後に最後まで通る
 *  11 0040 NE 用 CSV × 画面の判断: CSV の操作が鍵を持つ間、判断は候補の行より前に CSV の鍵で待つ → CSV の commit の後に判断が予約を外し、まだ申告していないファイルを void (③b H3)
 *  12 0040 画面の判断 × CSV を作る: 判断の途中 (鍵 → 候補 → 出来事) は CSV を作る側が鍵で待つ → 判断の commit の後に作る側は取り消した承認を入れない
 *  13 0040 バックアップ → 復元 (本物の Postgres・node-postgres): CSV の byte 列 (0x00・0xff・CRLF) がそのまま戻る (PGlite は byte 列を文字で受けないので試せない)
 *  14 0040 照合の完了 × CSV を作る: 照合が完了を書いている途中は、CSV を作る側が候補の行で待つ → 完了の commit の後は、完了した承認を入れない (候補の行を取った後に読み直す)
 *  15 0040 同じ単位の別の指紋: 画面が別の指紋を却下している途中 (候補の行は重ならない) でも、CSV を作る側は CSV の鍵で待つ → 置き換わった承認を予約しない (H1・H3)
 * 使い方: TEST_PG_URL=postgres://postgres:pw@localhost:54329/postgres node scripts/test-master-concurrency-pg.mjs
 *   🚨 使い捨ての PostgreSQL だけ (新しい DB を作って最後に消す)。localhost 以外の URL は拒む (本番を渡さない)。package.json の試験には入れない (PostgreSQL が要る)
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { openPgClient, pgAdapter, applyMigrations } from './company-db/migrate.mjs';
import { runInitialLoad } from '../apps/company-db/load/engine.mjs';

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
  const LOCK = "select set_config('core.parent_protocol', '1', true), pg_advisory_xact_lock(core.parent_lock_key())";
  const mkProduct = async (code) => Number((await M.query("insert into core.products (company_id, display_code, name, status, created_by_type, created_by_id) values (1, $1, $1, 'active', 'system', 't') returning product_id", [code])).rows[0].product_id);
  const gp = await mkProduct('pg_g'); const cp = await mkProduct('pg_c');

  await ta('[8] 0036 親子の守り: ほかの接続だけが鍵を持つ = 自分の書き込みは拒む。鍵を取りに行くと持ち主の commit まで待つ', async () => {
    await A.query('begin'); await A.query(LOCK);
    await Bc.query('begin');
    await Bc.query("select set_config('core.parent_protocol', '1', true)");
    await assert.rejects(Bc.query("update core.products set parent_product_id = $2, parent_set_by = 'manual' where product_id = $1", [cp, gp]), /parent_protocol_required/);
    await Bc.query('rollback');
    await Bc.query('begin');
    const b = launch(Bc.query(LOCK));
    await sleep(400);
    assert.equal(b.done, false, '鍵を取りに行った接続が待っていない');
    await A.query('commit');
    assert.ok((await b.promise).ok);
    await Bc.query("update core.products set parent_product_id = $2, parent_set_by = 'manual' where product_id = $1", [cp, gp]);
    await Bc.query('commit');
    assert.deepEqual((await M.query('select parent_product_id::int as p, parent_set_by as by from core.products where product_id = $1', [cp])).rows[0], { p: gp, by: 'manual' });
  });

  await ta('[9] 0036 鍵 → 行の順: 夜間ロードの途中 (鍵 → 商品の行の UPDATE) に人の付け外しが来ても、人は鍵で待つ = デッドロックしない', async () => {
    // 夜間ロード = 取引の冒頭で鍵 → 先の段で商品の行を UPDATE (行の鍵) → 後の段で親子
    await A.query('begin'); await A.query(LOCK);
    await A.query("update core.products set name = 'ロードが直した名前' where product_id = $1", [cp]);
    // 人の付け外し (同じ決まり: 鍵を先に取る) は鍵で待つ = 行の鍵を持ったまま鍵を待つことが無い
    await Bc.query('begin');
    const b = launch((async () => { await Bc.query(LOCK); await Bc.query('update core.products set parent_product_id = null, parent_set_by = null where product_id = $1', [cp]); return true; })());
    await sleep(400);
    assert.equal(b.done, false, '人の付け外しが鍵で待っていない');
    await A.query("update core.products set parent_product_id = $2, parent_set_by = 'load' where product_id = $1", [cp, gp]);   // ロードの後の段
    await A.query('commit');
    const r = await b.promise;
    assert.ok(r.ok, r.err?.message);
    await Bc.query('commit');
    assert.deepEqual((await M.query('select parent_product_id as p, parent_set_by as by, name from core.products where product_id = $1', [cp])).rows[0], { p: null, by: null, name: 'ロードが直した名前' });
  });
  await ta('[10] 0036 本物の夜間ロード: 人が鍵を持つ間、ロードは商品の行に触る前に鍵で待つ。人の commit の後に最後まで通る', async () => {
    const planOf = (name) => ({ skus: [{ code: 'rl1', name, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: null, representativeCode: 'rlg', representativeState: 'value', cost: null },
      { code: 'rl2', name: 'rl2', kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: null, representativeCode: null, representativeState: 'unknown', cost: null }],
      variationGroups: [{ code: 'rlg', name: 'まとまり', childCodes: ['rl1'], status: 'active' }],
      setComponents: [], listings: [], observations: [], physicals: [], compliance: [], suppliers: [], supplierSkus: [], workers: [], primarySuppliers: [], reorder: { available: false, reason: '試験' }, sources: {} });
    const r0 = await runInitialLoad(pgAdapter(A), planOf('はじめの名前'), { log: () => {}, runId: 'load_pg_rl0', host: 'test' });
    assert.equal(r0.ok, true, r0.error);
    const rl1 = Number((await M.query("select product_id from core.skus where code = 'rl1'")).rows[0].product_id);
    // 人の付け外し (鍵を先に取る) が鍵を持ったまま → 夜間ロードを始める (rl1 の商品の名前を直す予定)
    await Bc.query('begin'); await Bc.query(LOCK);
    let load = null;
    try {
    load = launch(runInitialLoad(pgAdapter(A), planOf('ロードが直す名前'), { log: () => {}, runId: 'load_pg_rl1', host: 'test' }));
    await sleep(600);
    assert.equal(load.done, false, 'ロードが鍵で待っていない');
    // ロードが商品の行をまだ触っていない = ほかの接続が行の鍵をすぐ取れる (鍵を行の後に取るように戻すと、ここで行の鍵に当たる)
    await M.query('begin');
    await M.query('select 1 from core.products where product_id = $1 for update nowait', [rl1]);
    await M.query('rollback');
    await Bc.query('update core.products set parent_product_id = null, parent_set_by = null where product_id = $1', [rl1]);
    await Bc.query('commit');
    const r = await load.promise;
    assert.ok(r.ok && r.ok.ok, r.err?.message || r.ok?.error);
    const row = (await M.query('select p.name, pp.display_code as parent, p.parent_set_by as by from core.products p left join core.products pp on pp.product_id = p.parent_product_id where p.product_id = $1', [rl1])).rows[0];
    assert.deepEqual(row, { name: 'ロードが直す名前', parent: 'rlg', by: 'load' });
    } finally {
      // 途中で落ちても人の取引を閉じる (閉じないとロードが鍵を待ったまま = 試験が止まる)
      try { await Bc.query('rollback'); } catch { /* */ }
      try { await M.query('rollback'); } catch { /* */ }
      if (load) await load.promise;
    }
  });
  // ── 0040 NE 用 CSV (③b-1) ──
  const csv = await import('../apps/master-decisions/ne-csv.mjs');
  const { applyDecisions: decideCsv } = await import('../apps/master-decisions/decide.mjs');
  const csvCand = (f, code) => ({ fingerprint: f, subject_key: `value:${code}`, code_norm: code, col: 'name', child: null, cls: 'rule', reason_kind: 'none', semantic: 'none@1',
    print: { f, sku_kind: 'single', n: 'old' }, resolutions: ['accept_difference', 'fix_ne'], proposal: { op: 'decide' } });
  const csvNow = Date.parse('2030-03-01T10:00:00+09:00');
  const csvRun = 'mc_20300301T000000001Z_abcdef';
  const approveName = async (f, code, value) => Number((await M.query(`insert into ops.master_decision_events (fingerprint, kind, resolution, target, actor_type, actor) values ($1, 'approved', 'fix_ne', $2::jsonb, 'user', 'naka@test') returning event_id`,
    [f, JSON.stringify({ subject_key: `value:${code}`, col: 'name', child: null, value })])).rows[0].event_id);
  const f1 = crypto.createHash('sha256').update('csv1').digest('hex'), f2 = crypto.createHash('sha256').update('csv2').digest('hex');
  await M.query('select ops.record_decision_candidates($1::jsonb)', [JSON.stringify({ compare_run_id: csvRun, observed_at: '2030-03-01T08:00:00+09:00', decisions: [csvCand(f1, 'cv1'), csvCand(f2, 'cv2')] })]);
  const ev1 = await approveName(f1, 'cv1', '新しい名前 1');
  await approveName(f2, 'cv2', '新しい名前 2');

  await ta('[11] 0040 CSV × 画面の判断: CSV の操作が鍵を持つ間、判断は CSV の鍵で待つ → CSV の commit の後に判断が予約を外し、まだ申告していないファイルを void', async () => {
    // A = CSV を作る途中 (CSV の鍵 → 候補の行 → ファイルと予約を書いた・まだ commit していない)
    await A.query('begin');
    await A.query("select pg_advisory_xact_lock(hashtext('ops.ne_csv'))");
    await A.query('select 1 from ops.master_decision_candidates where fingerprint = $1 for update', [f1]);
    const ex = Number((await A.query(`insert into ops.ne_csv_exports (kind, col, ne_column, converter_version, encoding, trial, row_count, sha256, file_bytes, compare_run_id, created_by)
      values ('products', 'name', 'syohin_name', 'ne-csv-v1', 'utf8', true, 1, repeat('a', 64), $2, $1, 'naka@test') returning export_id`, [csvRun, Buffer.from('A')])).rows[0].export_id);
    await A.query(`insert into ops.ne_csv_export_rows (export_id, source, approved_event_id, fingerprint, code_norm, col, ne_code, target, cell) values ($1, 'fix_ne', $2, $3, 'cv1', 'name', 'cv1', '{}', 'x')`, [ex, ev1, f1]);
    // B = 画面で cv1 の承認を取り消す。CSV の鍵で待つ (候補の行の前 = A と同じ順)
    const b = launch(decideCsv(pgAdapter(Bc), { actor: 'naka@test', kind: 'revoked', items: [{ fingerprint: f1, shown_event_id: ev1 }] }));
    await sleep(500);
    assert.equal(b.done, false, '判断が CSV の鍵を待っていない');
    const waiting = (await M.query(`select locktype from pg_locks where not granted`)).rows.map((x) => x.locktype);
    assert.deepEqual(waiting, ['advisory'], '判断は CSV の鍵 (advisory) で待つはず (候補の行ではなく)');
    await A.query('commit');
    const r = await b.promise;
    assert.ok(r.ok, r.err?.message);
    assert.deepEqual(r.ok.applied[0].csv_voided, [ex]);
    const e = (await M.query('select state, void_reason from ops.ne_csv_exports where export_id = $1', [ex])).rows[0];
    assert.deepEqual(e, { state: 'void', void_reason: 'superseded' });
    const row = (await M.query('select reserved, release_reason from ops.ne_csv_export_rows where export_id = $1', [ex])).rows[0];
    assert.deepEqual(row, { reserved: false, release_reason: 'superseded' });
  });

  await ta('[12] 0040 画面の判断 × CSV を作る: 判断の途中は CSV を作る側が鍵で待つ → commit の後は取り消した承認を入れない', async () => {
    // A = 画面の判断の途中 (CSV の鍵 → 候補の行 → 取り消しの出来事を書いた・まだ commit していない)
    await A.query('begin');
    await A.query("select pg_advisory_xact_lock(hashtext('ops.ne_csv'))");
    await A.query('select 1 from ops.master_decision_candidates where fingerprint = $1 for update', [f2]);
    await A.query(`insert into ops.master_decision_events (fingerprint, kind, actor_type, actor, shown_fingerprint) values ($1, 'revoked', 'user', 'naka@test', $1)`, [f2]);
    const b = launch(csv.createExport(pgAdapter(Bc), { actor: 'naka@test', kind: 'products', col: 'name', nowMs: csvNow }));
    await sleep(500);
    assert.equal(b.done, false, 'CSV を作る側が鍵を待っていない');
    await A.query('commit');
    const r = await b.promise;
    assert.ok(r.err && r.err.reason === 'nothing_to_export', r.err?.message || JSON.stringify(r.ok));   // cv1・cv2 とも取り消し = 入れる承認が無い
    assert.equal((await M.query(`select count(*)::int as n from ops.ne_csv_export_rows where code_norm = 'cv2'`)).rows[0].n, 0);
  });

  await ta('[14] 0040 照合の完了 × CSV を作る: 完了の書き込みの途中は CSV を作る側が候補の行で待つ → commit の後は完了した承認を入れない', async () => {
    const f3 = crypto.createHash('sha256').update('csv3').digest('hex');
    await M.query('select ops.record_decision_candidates($1::jsonb)', [JSON.stringify({ compare_run_id: 'mc_20300301T000000002Z_abcdef', observed_at: '2030-03-01T08:30:00+09:00',
      decisions: [csvCand(f1, 'cv1'), csvCand(f2, 'cv2'), csvCand(f3, 'cv3')] })]);
    const ev3 = await approveName(f3, 'cv3', '新しい名前 3');
    // A = 照合の完了の関数 (候補の行を for update して action_done を書いた・まだ commit していない)
    await A.query('begin');
    const ok = (await A.query('select ops.record_decision_done($1::bigint, $2, $3::jsonb) as ok', [ev3, 'mc_20300301T000000002Z_abcdef',
      JSON.stringify({ side: 'ne', subject_key: 'value:cv3', col: 'name', child: null, value: '新しい名前 3' })])).rows[0].ok;
    assert.equal(ok, true);
    const b = launch(csv.createExport(pgAdapter(Bc), { actor: 'naka@test', kind: 'products', col: 'name', nowMs: csvNow }));
    await sleep(500);
    assert.equal(b.done, false, 'CSV を作る側が候補の行を待っていない');
    await A.query('commit');
    const r = await b.promise;
    assert.ok(r.err && r.err.reason === 'nothing_to_export', r.err?.message || JSON.stringify(r.ok));   // 完了した cv3 を入れない (cv1・cv2 は取り消し済み)
    assert.equal((await M.query(`select count(*)::int as n from ops.ne_csv_export_rows where code_norm = 'cv3'`)).rows[0].n, 0);
  });

  await ta('[15] 0040 同じ単位の別の指紋: 画面が別の指紋を却下している途中でも CSV を作る側は CSV の鍵で待つ → 置き換わった承認を予約しない', async () => {
    const f4a = crypto.createHash('sha256').update('csv4a').digest('hex'), f4b = crypto.createHash('sha256').update('csv4b').digest('hex');
    const c4b = { ...csvCand(f4b, 'cv4'), print: { f: f4b, sku_kind: 'single', n: 'もう 1 つの値' } };
    await M.query('select ops.record_decision_candidates($1::jsonb)', [JSON.stringify({ compare_run_id: 'mc_20300301T000000003Z_abcdef', observed_at: '2030-03-01T09:00:00+09:00',
      decisions: [csvCand(f4a, 'cv4'), c4b] })]);
    await approveName(f4a, 'cv4', '新しい名前 4');
    // A = 画面が同じ単位 (cv4 の名前) の別の指紋 f4b を却下している途中 (CSV の鍵 → f4b の行 → 出来事。f4a の行は取らない)
    await A.query('begin');
    await A.query("select pg_advisory_xact_lock(hashtext('ops.ne_csv'))");
    await A.query('select 1 from ops.master_decision_candidates where fingerprint = $1 for update', [f4b]);
    await A.query(`insert into ops.master_decision_events (fingerprint, kind, actor_type, actor, shown_fingerprint) values ($1, 'rejected', 'user', 'naka@test', $1)`, [f4b]);
    const b = launch(csv.createExport(pgAdapter(Bc), { actor: 'naka@test', kind: 'products', col: 'name', nowMs: csvNow }));
    await sleep(500);
    assert.equal(b.done, false, 'CSV を作る側が CSV の鍵を待っていない (候補の行は重ならない)');
    await A.query('commit');
    const r = await b.promise;
    assert.ok(r.err && r.err.reason === 'nothing_to_export', r.err?.message || JSON.stringify(r.ok));   // f4a の承認は後の却下 (f4b) で置き換わった
    assert.equal((await M.query(`select count(*)::int as n from ops.ne_csv_export_rows where code_norm = 'cv4'`)).rows[0].n, 0);
  });

  await ta('[13] 0040 バックアップ → 復元 (本物の Postgres): CSV の byte 列 (0x00・0xff・CRLF・バックスラッシュ) がそのまま戻る', async () => {
    const { dumpCompanyDb, restoreCompanyDb } = await import('../apps/company-db/backup/dump.mjs');
    // 判断の台帳に完了の行がある DB は今の復元が戻せない (master の穴・別の PR) = 完了の行の無い別の DB で確かめる
    const names = [`cdb_bk_${crypto.randomBytes(4).toString('hex')}`, `cdb_rs_${crypto.randomBytes(4).toString('hex')}`];
    for (const n of names) await admin.query(`create database ${n}`);
    const conn = async (n) => { const x = new URL(url); x.pathname = `/${n}`; return openPgClient(x.toString()); };
    const S = await conn(names[0]), D = await conn(names[1]);
    try {
      await applyMigrations(pgAdapter(S), { log: () => {} });
      await applyMigrations(pgAdapter(D), { log: () => {} });
      const odd = Buffer.from([0x00, 0xff, 0x0d, 0x0a, 0x5c, 0x78, 0x41, 0xe3, 0x81]);
      const h = crypto.createHash('sha256').update(odd).digest('hex');
      await S.query(`insert into ops.ne_csv_exports (kind, col, ne_column, converter_version, encoding, trial, row_count, sha256, file_bytes, compare_run_id, created_by, state, void_at, void_reason)
        values ('products', 'name', 'syohin_name', 'ne-csv-v1', 'utf8', true, 1, $1, $2, $3, 'test', 'void', now(), 'by_user')`, [h, odd, csvRun]);
      const lines = [];
      await dumpCompanyDb(pgAdapter(S), (l) => lines.push(l), { log: () => {} });
      await restoreCompanyDb(pgAdapter(D), lines.join(String.fromCharCode(10)), { log: () => {} });
      const got = (await D.query('select file_bytes, sha256, state from ops.ne_csv_exports')).rows;
      assert.equal(got.length, 1);
      assert.ok(Buffer.from(got[0].file_bytes).equals(odd), Buffer.from(got[0].file_bytes).toString('hex'));
      assert.deepEqual([got[0].sha256, got[0].state], [h, 'void']);
      const f = await csv.exportFile(pgAdapter(D), 1);
      assert.ok(f.bytes.equals(odd));
    } finally {
      for (const c of [S, D]) { try { await c.end(); } catch { /* */ } }
      for (const n of names) { try { await admin.query(`drop database ${n}`); } catch (e) { console.error(`DB を消せない: ${e.message}`); } }
    }
  });
} finally {
  for (const c of [A, Bc, M]) { try { await c.end(); } catch { /* */ } }
  try { await admin.query(`drop database ${dbName}`); } catch (e) { console.error(`DB を消せない: ${e.message}`); }
  await admin.end();
}
console.log(`\n${passed} 件 PASS`);
