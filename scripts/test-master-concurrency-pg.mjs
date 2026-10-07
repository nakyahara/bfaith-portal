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
 *  16 0041 NE の元のコード: 照合が元のコードを書いている途中は CSV を作る側が共有の鍵で待ち、書き終えた新しい回の書き方で作る / CSV の操作の途中は照合の書き手が待つ (③b-1b H2)
 *  17 0055 持ち主の epoch: 2 人が同時に最初の prepare = 後の人は前の人の commit を待ってから通る (重複で落ちない・init は 1 回) (④a・Codex #1564 R1 H1)
 *  18 0055 activate × cancel: cancel の途中は activate が行の鍵で待ち、cancel の後は「prepared が無い」で断る (active は変わらない)
 *  19 0055 activate × activate: 同時に 2 回 = 1 回だけ通る (記録も 1 件)・夜間ロードは新しい active を読む・変更の記録は消せない
 *  20 0055 activate × 同じ持ち主の prepare のやり直し: やり直しの途中は activate が行の鍵で待ち、やり直しの後は前の証拠 (前の prepare の時刻) では断る (#1564 Codex R2 Medium 4)
 *  21 0055 古い active を読んだ夜間ロードの途中に activate = ロードの commit まで待ち、その後「証拠の後にロードが入った」で断る (#1564 Codex R3 High 1)
 *  22 0055 --use-prepared のロードの途中に cancel = ロードの commit まで待つ / 23 ロードの途中に prepare のやり直し = ロードの commit まで待つ
 *     (activate の証拠 = prepared の明示のロードの commit の番号 (0055 の ops.master_load_commits)。[21] の後のロードは番号が 1 つ後。#1564 Codex R4 Medium 2)
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
  // NE の元のコード (0041): 照合の回ごとに記録する (無いと全部 NE の画面で直す = 下の試験が何も確かめない)
  const csvCodes = (run, extra = []) => M.query('select ops.record_ne_codes($1::jsonb)', [JSON.stringify({ compare_run_id: run,
    entries: [...['cv1', 'cv2', 'cv3', 'cv4'].map((n) => ({ code_norm: n, kind: 'product', state: 'ok', ne_code: n, spellings: [n] })), ...extra] })]);
  await csvCodes(csvRun);
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
    await csvCodes('mc_20300301T000000002Z_abcdef');
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
    await csvCodes('mc_20300301T000000003Z_abcdef');
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

  await ta('[16] 0041 NE の元のコード: 書いている途中は CSV を作る側が共有の鍵で待つ → 新しい回の書き方で作る / CSV の操作の途中は書き手が待つ', async () => {
    const f5 = crypto.createHash('sha256').update('csv5').digest('hex');
    const run4 = 'mc_20300301T000000004Z_abcdef', run5 = 'mc_20300301T000000005Z_abcdef';
    await M.query('select ops.record_decision_candidates($1::jsonb)', [JSON.stringify({ compare_run_id: run4, observed_at: '2030-03-01T09:30:00+09:00', decisions: [csvCand(f5, 'cv5')] })]);
    await approveName(f5, 'cv5', '新しい名前 5');
    // A = 照合が run4 の元のコードを書いている途中 (排他の鍵を持ったまま)
    await A.query('begin');
    await A.query('select ops.record_ne_codes($1::jsonb)', [JSON.stringify({ compare_run_id: run4, entries: [{ code_norm: 'cv5', kind: 'product', state: 'ok', ne_code: 'CV5', spellings: ['CV5'] }] })]).catch(async (e) => { await A.query('rollback'); throw e; });
    const b = launch(csv.createExport(pgAdapter(Bc), { actor: 'naka@test', kind: 'products', col: 'name', nowMs: csvNow }));
    await sleep(500);
    assert.equal(b.done, false, 'CSV を作る側が元のコードの鍵を待っていない');
    await A.query('commit');
    const r = await b.promise;
    assert.ok(r.ok, r.err?.message);
    const row = (await M.query('select ne_code from ops.ne_csv_export_rows where export_id = $1', [r.ok.export.export_id])).rows;
    assert.deepEqual(row, [{ ne_code: 'CV5' }]);   // 書き終えた run4 の書き方
    // 逆: CSV の操作の途中 (CSV の鍵 → 共有の鍵を持つ) は、照合の書き手が待つ
    await M.query('select ops.record_decision_candidates($1::jsonb)', [JSON.stringify({ compare_run_id: run5, observed_at: '2030-03-01T09:40:00+09:00', decisions: [csvCand(f5, 'cv5')] })]);
    await A.query('begin');
    await A.query("select pg_advisory_xact_lock(hashtext('ops.ne_csv'))");
    await A.query("select pg_advisory_xact_lock_shared(hashtext('ops.ne_codes'))");
    const w = launch(Bc.query('select ops.record_ne_codes($1::jsonb) as r', [JSON.stringify({ compare_run_id: run5, entries: [{ code_norm: 'cv5', kind: 'product', state: 'ok', ne_code: 'CV5', spellings: ['CV5'] }] })]));
    await sleep(500);
    assert.equal(w.done, false, '照合の書き手が共有の鍵を待っていない');
    await A.query('commit');
    const wr = await w.promise;
    assert.ok(wr.ok, wr.err?.message);
    assert.equal((await M.query('select compare_run_id from ops.master_ne_code_mark')).rows[0].compare_run_id, run5);
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
  // ── 0055 持ち主の epoch (④a・Codex #1564 R1 H1) ──
  const OS = await import('../apps/company-db/load/ownership-state.mjs');
  const ownOf = (k) => ({ ...OS.ALL_LOAD, [k]: 'company' });
  const allHash = OS.ownershipHashOf(OS.ALL_LOAD);
  // 証拠の世代が読んだ夜間ロードの commit の番号 (0055 の ops.master_load_commits。試験では今の最後のロード)
  const lastSeq = async () => (await OS.latestLoadCommit(pgAdapter(M)))?.commit_seq ?? null;
  const epPlan = (name) => ({ skus: [{ code: 'ep1', name, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: null, representativeCode: null, representativeState: 'unknown', cost: null }],
    variationGroups: [], setComponents: [], listings: [], observations: [], physicals: [], compliance: [], suppliers: [], supplierSkus: [], workers: [], primarySuppliers: [], reorder: { available: false, reason: '試験' }, sources: {} });
  /** 切替の日の明示のロード (prepared の持ち主)。activate の証拠の世代はこのロードを読む = 最後に commit したロードの持ち主が prepared */
  const preparedLoad = async (runId) => { const r = await runInitialLoad(pgAdapter(M), epPlan(runId), { log: () => {}, runId, host: 'test', usePrepared: true }); assert.equal(r.ok, true, r.error); return r; };
  // activate は切替の段階 (⑤-1 の ops.master_cutover_state) が frozen のときだけ = 最小の形で用意する (⑤-1 の表があれば守りの印を立てて直す)
  //   (⑤-1 の 0051 の表 = 段階の守りを通さずに置く試験の近道: この取引だけ trigger を止める。本番は ops.set_master_cutover_phase だけ)
  await M.query('begin');
  await M.query('set local session_replication_role = replica');
  await M.query("update ops.master_cutover_state set phase = 'frozen', owner_hash = null where id = 1");
  await M.query('commit');
  // 0001〜0055 が本物の PostgreSQL でそろって入る (master 0050 → ⑤-1 0051 → ⑤-2a 0052 → ⑤-2b 0053 → ⑦-1 0054 → ④a 0055)
  assert.deepEqual((await M.query("select version from ops.schema_migrations where version in ('0051', '0052', '0053', '0054', '0055') order by 1")).rows.map((r) => r.version), ['0051', '0052', '0053', '0054', '0055']);
  assert.deepEqual((await M.query('select name from ops.master_cutover_prereq_checks order by 1')).rows.map((r) => r.name), ['0052_registrations', '0054_amazon_map', '0055_ownership_epoch']);
  await ta('[17] 0055 2 人が同時に最初の prepare = 後の人は前の人の commit を待ってから通る (重複で落ちない・init は 1 回)', async () => {
    await A.query('begin');
    await A.query(`insert into ops.master_ownership_state (id, active_hash, active_map, activated_by) values (1, $1, $2::jsonb, 'A') on conflict (id) do nothing`, [allHash, JSON.stringify(OS.sortedOwnership(OS.ALL_LOAD))]);
    await A.query(`insert into ops.master_ownership_events (action, ownership_hash, ownership, actor) values ('init', $1, $2::jsonb, 'A')`, [allHash, JSON.stringify(OS.sortedOwnership(OS.ALL_LOAD))]);
    const b = launch(OS.prepareOwnership(pgAdapter(Bc), { map: ownOf('sku_costs'), actor: 'B' }));
    await sleep(400);
    assert.equal(b.done, false, 'B が A の行を待っていない');
    await A.query('commit');
    const rb = await b.promise;
    assert.ok(rb.ok, rb.err && rb.err.message);
    const st = await OS.readOwnershipState(pgAdapter(M));
    assert.deepEqual([st.active.hash, st.prepared.hash], [allHash, OS.ownershipHashOf(ownOf('sku_costs'))]);
    assert.deepEqual((await M.query('select action from ops.master_ownership_events order by event_id')).rows.map((r) => r.action), ['init', 'prepare']);
  });
  await ta('[18] 0055 activate × cancel: cancel の途中は activate が待ち、cancel の後は「prepared が無い」で断る (active は変わらない)', async () => {
    await A.query('begin');
    await A.query('select 1 from ops.master_ownership_state where id = 1 for update');
    const pAt = (await OS.readOwnershipState(pgAdapter(M))).prepared.prepared_at;
    const b = launch(OS.activateOwnership(pgAdapter(Bc), { expectHash: OS.ownershipHashOf(ownOf('sku_costs')), expectPreparedAt: pAt, expectLoadCommitSeq: await lastSeq(), actor: 'B', evidence: { build_id: 'x' } }));
    await sleep(400);
    assert.equal(b.done, false, 'activate が行の鍵で待っていない');
    await A.query('update ops.master_ownership_state set prepared_hash = null, prepared_map = null, prepared_at = null, prepared_by = null where id = 1');
    await A.query(`insert into ops.master_ownership_events (action, ownership_hash, ownership, actor) values ('cancel_prepare', $1, $2::jsonb, 'A')`, [OS.ownershipHashOf(ownOf('sku_costs')), JSON.stringify(ownOf('sku_costs'))]);
    await A.query('commit');
    const rb = await b.promise;
    assert.equal(rb.err && rb.err.code, 'NO_PREPARED_OWNERSHIP', rb.err ? rb.err.message : 'activate が通ってしまった');
    const st = await OS.readOwnershipState(pgAdapter(M));
    assert.deepEqual([st.active.hash, st.prepared], [allHash, null]);
  });
  await ta('[19] 0055 activate × activate: 同時に 2 回 = 1 回だけ通る・夜間ロードは新しい active を読む・変更の記録は消せない', async () => {
    const want = OS.ownershipHashOf(ownOf('skus.shipping'));
    await OS.prepareOwnership(pgAdapter(M), { map: ownOf('skus.shipping'), actor: 'M' });
    const pAt = (await OS.readOwnershipState(pgAdapter(M))).prepared.prepared_at;
    await preparedLoad('load_pg_p19');
    const ll = await lastSeq();
    const a = launch(OS.activateOwnership(pgAdapter(A), { expectHash: want, expectPreparedAt: pAt, expectLoadCommitSeq: ll, actor: 'A', evidence: { build_id: 'a' } }));
    const b = launch(OS.activateOwnership(pgAdapter(Bc), { expectHash: want, expectPreparedAt: pAt, expectLoadCommitSeq: ll, actor: 'B', evidence: { build_id: 'b' } }));
    const [ra, rb] = [await a.promise, await b.promise];
    assert.equal([ra, rb].filter((r) => r.ok).length, 1, JSON.stringify([ra.err && ra.err.message, rb.err && rb.err.message]));
    assert.equal([ra, rb].find((r) => r.err).err.code, 'NO_PREPARED_OWNERSHIP');
    const st = await OS.readOwnershipState(pgAdapter(M));
    assert.deepEqual([st.active.hash, st.prepared], [want, null]);
    assert.equal((await M.query("select count(*)::int as n from ops.master_ownership_events where action = 'activate'")).rows[0].n, 1);
    const ep = await OS.resolveLoadOwnership(pgAdapter(M));
    assert.deepEqual([ep.epoch, ep.hash, ep.ownership['skus.shipping']], ['active', want, 'company']);
    await assert.rejects(M.query('delete from ops.master_ownership_events'), /足すだけ/);
    await assert.rejects(M.query("update ops.master_ownership_state set prepared_hash = active_hash, prepared_map = active_map, prepared_at = now() where id = 1"), /ck_master_ownership_prepared_differs/);
  });
  await ta('[20] 0055 activate × 同じ持ち主の prepare のやり直し: やり直しの途中は activate が待ち、やり直しの後は前の証拠では断る (active は変わらない)', async () => {
    const map = ownOf('skus.tax_class');
    await OS.prepareOwnership(pgAdapter(M), { map, actor: 'M' });
    const before = (await OS.readOwnershipState(pgAdapter(M)));
    const oldAt = before.prepared.prepared_at, activeBefore = before.active.hash;
    // A が同じ持ち主で prepare をやり直している (行の鍵を持ったまま)
    await A.query('begin');
    await A.query('select 1 from ops.master_ownership_state where id = 1 for update');
    await A.query("update ops.master_ownership_state set prepared_at = now() + interval '1 second' where id = 1");
    const b = launch(OS.activateOwnership(pgAdapter(Bc), { expectHash: OS.ownershipHashOf(map), expectPreparedAt: oldAt, expectLoadCommitSeq: await lastSeq(), actor: 'B', evidence: { build_id: 'old' } }));
    await sleep(400);
    assert.equal(b.done, false, 'activate が行の鍵で待っていない');
    await A.query('commit');
    const rb = await b.promise;
    assert.equal(rb.err && rb.err.code, 'PREPARED_CHANGED', rb.err ? rb.err.message : 'activate が通ってしまった');
    const st = await OS.readOwnershipState(pgAdapter(M));
    assert.deepEqual([st.active.hash, st.prepared.hash], [activeBefore, OS.ownershipHashOf(map)]);
    assert.ok(st.prepared.prepared_at > oldAt);
    // 新しい時刻で集め直した証拠なら通る (証拠の世代 = prepared の明示のロード)
    await preparedLoad('load_pg_p20');
    const ok = await OS.activateOwnership(pgAdapter(Bc), { expectHash: OS.ownershipHashOf(map), expectPreparedAt: st.prepared.prepared_at, expectLoadCommitSeq: await lastSeq(), actor: 'B', evidence: { build_id: 'new' } });
    assert.equal(ok.active_hash, OS.ownershipHashOf(map));
  });
  // ── 夜間ロードと epoch を変えるコマンドは epoch の鍵で並ぶ (#1564 Codex R3 High 1) ──
  //   ロードは取引の中で epoch の鍵 (共有) を取ってから epoch を読む (afterEpochRead = 読んだ後・書く前で止める試験の口)。prepare / activate / cancel は排他で待つ
  /** ロードを epoch を読んだ後で止めて流す。release() で続ける */
  const pausedLoad = (runId, opts = {}) => {
    let paused, release;
    const atPause = new Promise((r) => { paused = r; });
    const go = new Promise((r) => { release = r; });
    const load = launch(runInitialLoad(pgAdapter(A), epPlan(runId), { log: () => {}, runId, host: 'test', ...opts, afterEpochRead: async (ep) => { paused(ep); await go; } }));
    return { load, atPause, release };
  };
  const otherMap = ownOf('skus.standard_price');
  await ta('[21] 0055 古い active を読んだロードの途中に activate = activate はロードの commit まで待ち、その後「証拠の後にロードが入った」で断る (新しい active の後に古い epoch のロードが commit しない・C の列を NE で上書きしない)', async () => {
    const before = await OS.readOwnershipState(pgAdapter(M));
    await OS.prepareOwnership(pgAdapter(M), { map: otherMap, actor: 'M' });
    await preparedLoad('load_pg_p21');
    const evidenceLoad = await lastSeq();   // 証拠の世代が読んだロード (prepared の明示のロード) の commit の番号
    const pAt = (await OS.readOwnershipState(pgAdapter(M))).prepared.prepared_at;
    const L1 = pausedLoad('load_pg_ep1');
    const ep = await L1.atPause;
    assert.deepEqual([ep.epoch, L1.load.done], ['active', false]);   // 古い active を読んで止まっている (epoch の鍵 = 共有を持ったまま)
    const act = launch(OS.activateOwnership(pgAdapter(Bc), { expectHash: OS.ownershipHashOf(otherMap), expectPreparedAt: pAt, expectLoadCommitSeq: evidenceLoad, actor: 'B', evidence: { build_id: 'race' } }));
    await sleep(500);
    assert.equal(act.done, false, 'activate がロードの epoch の鍵を待っていない');
    L1.release();
    const rl = await L1.load.promise;
    assert.ok(rl.ok && rl.ok.ok, rl.err?.message || rl.ok?.error);
    assert.deepEqual(rl.ok.company_owned, ['skus.tax_class']);   // 読んだ epoch (古い active) のまま書き終わった
    const ra = await act.promise;
    assert.equal(ra.err && ra.err.code, 'LOAD_AFTER_EVIDENCE', ra.err ? ra.err.message : 'activate が通ってしまった');
    assert.deepEqual([ra.err.last_load, ra.err.last_commit_seq], ['load_pg_ep1', String(BigInt(evidenceLoad) + 1n)]);   // 番号 = commit の順 (証拠のロードの次)
    const st = await OS.readOwnershipState(pgAdapter(M));
    assert.deepEqual([st.active.hash, st.prepared.hash], [before.active.hash, OS.ownershipHashOf(otherMap)]);   // active は変わらない
    // 証拠を取り直した (prepared のロードから) activate は通る・その後のロードは新しい active を読む (取引の中で読む)
    await preparedLoad('load_pg_p21b');
    const okAct = await OS.activateOwnership(pgAdapter(Bc), { expectHash: OS.ownershipHashOf(otherMap), expectPreparedAt: pAt, expectLoadCommitSeq: await lastSeq(), actor: 'B', evidence: { build_id: 'after' } });
    assert.equal(okAct.active_hash, OS.ownershipHashOf(otherMap));
    const L2 = pausedLoad('load_pg_ep2');
    assert.equal((await L2.atPause).epoch, 'active');
    L2.release();
    const r2 = await L2.load.promise;
    assert.deepEqual(r2.ok && r2.ok.company_owned, ['skus.standard_price'], r2.err?.message);
  });
  await ta('[22] 0055 --use-prepared のロードの途中に cancel = cancel はロードの commit まで待つ (ロードは読んだ prepared のまま書き終わる・その後 prepared が消える)', async () => {
    const pm = ownOf('skus.shipping');
    await OS.prepareOwnership(pgAdapter(M), { map: pm, actor: 'M' });
    const L3 = pausedLoad('load_pg_ep3', { usePrepared: true });
    assert.equal((await L3.atPause).epoch, 'prepared');
    const c = launch(OS.cancelPrepared(pgAdapter(Bc), { actor: 'B' }));
    await sleep(500);
    assert.equal(c.done, false, 'cancel がロードの epoch の鍵を待っていない');
    L3.release();
    const rl = await L3.load.promise;
    assert.deepEqual(rl.ok && rl.ok.company_owned, ['skus.shipping'], rl.err?.message);
    const rc = await c.promise;
    assert.deepEqual(rc.ok, { cancelled: true }, rc.err?.message);
    assert.equal((await OS.readOwnershipState(pgAdapter(M))).prepared, null);
  });
  await ta('[23] 0055 ロードの途中に prepare のやり直し = prepare はロードの commit まで待つ (ロードは読んだ epoch のまま・prepare は後で通る = 時刻はロードの後)', async () => {
    const L4 = pausedLoad('load_pg_ep4');
    assert.equal((await L4.atPause).epoch, 'active');
    const p = launch(OS.prepareOwnership(pgAdapter(Bc), { map: ownOf('skus.name'), actor: 'B' }));
    await sleep(500);
    assert.equal(p.done, false, 'prepare がロードの epoch の鍵を待っていない');
    L4.release();
    const rl = await L4.load.promise;
    assert.deepEqual(rl.ok && rl.ok.company_owned, ['skus.standard_price'], rl.err?.message);
    const rp = await p.promise;
    assert.ok(rp.ok, rp.err?.message);
    const st = await OS.readOwnershipState(pgAdapter(M));
    assert.equal(st.prepared.hash, OS.ownershipHashOf(ownOf('skus.name')));
    const fin = (await M.query("select finished_at from ops.ingest_runs where ingest_run_id = 'load_pg_ep4'")).rows[0].finished_at;
    assert.ok(new Date(st.prepared.prepared_at) >= new Date(fin), `prepare (${st.prepared.prepared_at}) がロード (${fin}) より前`);
    await OS.cancelPrepared(pgAdapter(M), { actor: 'M' });
  });
} finally {
  for (const c of [A, Bc, M]) { try { await c.end(); } catch { /* */ } }
  try { await admin.query(`drop database ${dbName}`); } catch (e) { console.error(`DB を消せない: ${e.message}`); }
  await admin.end();
}
console.log(`\n${passed} 件 PASS`);
