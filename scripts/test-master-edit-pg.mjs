/**
 * test-master-edit-pg.mjs — マスタの入力 (lib/master-write.mjs・0050) の**同時実行**を、実 PostgreSQL の独立した接続で確かめる
 *   (PGlite は 1 接続なので書けない。Codex ⑤-R1 H4・PR #1563 R1 H3 / M6 の試験)
 *
 * 固定する契約:
 *   1 同じ SKU を 2 人が同じ画面から保存: 後の人は SKU の鍵で待ち、前の人の commit の後に 409 (編集の印が違う)。両方は書かない
 *   2 同じ request_id が 2 つ並んで来る (押し直し): 後の方は request_id の鍵で待ち、前の方の結果をそのまま返す (記録は 1 行・変更も 1 回)
 *   3 同じ request_id を違う SKU へ同時に: 後の方は request_id の鍵で待ち、409 request_id_reused (500 にしない)
 *   4 画面を開いた後に別の接続が仕入先ごとの商品の行を足した (phantom) = 409
 *   5 CSV を作る側が CSV の鍵を持っている間は保存が待ち、その CSV (作った = 出ている) の列を変える保存は 409 csv_issued
 *   6 保存の途中で接続が切れた: 何も残らない (処理中の行を作らない) = 同じ request_id でもう一度保存できる
 *   7 切替の段階を変える取引 (排他の鍵) の途中は、保存が段階の共有の鍵で待つ
 *   8 構成の依頼を上げる × 単品の保存 (足す構成品): 単品が先 = 上げる側は単品の鍵で待ち、新しい単品の値でセットを計算する /
 *     上げる側が先 = 単品の保存は鍵で待ち、含むセットが増えたことに気づいて 409 (古い値のままのセットを残さない)
 * 使い方: TEST_PG_URL=postgres://postgres:pw@localhost:54329/postgres node scripts/test-master-edit-pg.mjs
 *   🚨 使い捨ての PostgreSQL だけ (新しい DB を作って最後に消す)。localhost 以外の URL は拒む (本番を渡さない)。package.json の test:company-db には入れない (PostgreSQL が要る)
 *   ロール (master_edit) の権限は PGlite の試験 (test-master-edit.mjs [14]) で確かめる (ここではクラスタにロールを作らない)
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { openPgClient, pgAdapter, applyMigrations } from './company-db/migrate.mjs';
import { runInitialLoad } from '../apps/company-db/load/engine.mjs';
import { MASTER_OWNERSHIP } from '../config/master-ownership.mjs';

const url = process.env.TEST_PG_URL || '';
if (!url) { console.log('⏭️ TEST_PG_URL が無い (実 PostgreSQL の同時実行の試験は飛ばす。PGlite の試験は scripts/test-master-edit.mjs)'); process.exit(0); }
const u0 = new URL(url);
if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(u0.hostname)) { console.error('localhost 以外の PostgreSQL には流さない'); process.exit(2); }

const W = await import('../lib/master-write.mjs');
const C = await import('../lib/master-cutover.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
/** 結果を待たずに投げる (待っている = done が false のまま) */
const launch = (p) => { const s = { done: false }; s.promise = p.then((r) => { s.done = true; return { ok: r }; }, (e) => { s.done = true; return { err: e }; }); return s; };
/** 外から開ける扉 (commit の直前で止める) */
const gate = () => { let open; const p = new Promise((r) => { open = r; }); return { wait: () => p, open }; };

const ALL_COMPANY = Object.fromEntries(Object.keys(MASTER_OWNERSHIP).map((k) => [k, 'company']));
const NOW = new Date('2030-01-10T03:00:00Z');
const dbName = `cdb_me_${crypto.randomBytes(4).toString('hex')}`;
const admin = await openPgClient(url);
await admin.query(`create database ${dbName}`);
const u = new URL(url); u.pathname = `/${dbName}`;
const M = await openPgClient(u.toString()), A = await openPgClient(u.toString()), B = await openPgClient(u.toString());
for (const c of [M, A, B]) c.on('error', (e) => console.error(`[pg] ${e.message}`));
const dbM = pgAdapter(M), dbA = pgAdapter(A), dbB = pgAdapter(B);
try {
  await applyMigrations(dbM, { log: () => {} });
  const single = (code, cost) => ({ code, name: code, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3, cost: { jpy: cost, source: 'ne', status: 'COMPLETE' } });
  const plan = {
    skus: [single('p001', 100), single('p002', 200), single('p003', 300),
      { code: 'ps01', name: 'セット', kind: 'set', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: null, cost: { jpy: 100, source: 'set_calc', status: 'COMPLETE' } }],
    variationGroups: [], setComponents: [{ parentCode: 'ps01', childCode: 'p001', qty: 1, source: 'ne' }], listings: [], observations: [], physicals: [], compliance: [], workers: [],
    suppliers: [{ code: '0001', name: 'AMC' }, { code: '0002', name: 'ビーフリー' }], supplierSkus: [{ supplierCode: '0001', skuCode: 'p001' }],
  };
  const r0 = await runInitialLoad(dbM, plan, { log: () => {}, runId: 'load_pg_1', now: new Date('2030-01-05T03:00:00Z') });
  assert.equal(r0.ok, true, r0.error);
  // 試験だけの切替 (本番の関数そのまま・門の記録と証拠を足す)
  const h = C.ownershipHash(ALL_COMPANY);
  const ack = (phase) => M.query(`insert into ops.master_legacy_gate_acks (host, build_id, owner_hash, legacy_gates_version, phase_seen) select x, 't', $1, 1, $2 from unnest(array['render', 'minipc']) x`, [h, phase]);
  await ack('legacy_open');
  await C.advanceCutoverPhase(dbM, { to: 'frozen', actor: 't@test', evidence: { drain: { done: true, checked_by: 't', checked_at: '2030-01-09' }, manual_entries_stopped: [{ entry: 'NE', stopped_by: 't', stopped_at: '2030-01-09' }] } });
  await ack('frozen');
  await C.advanceCutoverPhase(dbM, { to: 'company_owner', actor: 't@test', evidence: { owner_hash: h } });
  await ack('company_owner');
  await C.advanceCutoverPhase(dbM, { to: 'new_open', actor: 't@test', evidence: { owner_hash: h } });

  const q = async (sql, p) => (await M.query(sql, p)).rows;
  const tokenOf = async (code) => W.editTokenOf(await W.readCurrent(dbM, (await q('select sku_id::text as id from core.skus where code = $1', [code]))[0].id, '2030-01-10'));
  const save = (db, code, values, { token, requestId = crypto.randomUUID(), beforeCommit } = {}) =>
    W.saveSku(db, { actor: 'naka@test', requestId, code, reason: 'pg', seen: { token }, values }, { ownership: ALL_COMPANY, open: true, now: NOW, beforeCommit });

  await ta('[1] 同じ SKU を 2 人が同じ画面から保存 = 後の人は待ってから 409 (両方は書かない)', async () => {
    const token = await tokenOf('p001');
    const g = gate();
    const a = launch(save(dbA, 'p001', { name: 'A の名前' }, { token, beforeCommit: g.wait }));
    await sleep(300);
    const b = launch(save(dbB, 'p001', { name: 'B の名前' }, { token }));
    await sleep(500);
    assert.equal(b.done, false, 'B は SKU の鍵で待つ');
    g.open();
    assert.ok((await a.promise).ok);
    const rb = await b.promise;
    assert.equal(rb.err?.reason, 'version_conflict', rb.err?.message);
    assert.equal((await q("select name from core.skus where code = 'p001'"))[0].name, 'A の名前');
  });

  await ta('[2] 同じ request_id が 2 つ並んで来る = 後の方は前の方の結果 (記録 1 行・変更 1 回)', async () => {
    const token = await tokenOf('p002');
    const id = crypto.randomUUID();
    const g = gate();
    const a = launch(save(dbA, 'p002', { standard_price: '1234' }, { token, requestId: id, beforeCommit: g.wait }));
    await sleep(300);
    const b = launch(save(dbB, 'p002', { standard_price: '1234' }, { token, requestId: id }));
    await sleep(500);
    assert.equal(b.done, false);
    g.open();
    assert.ok((await a.promise).ok);
    const rb = await b.promise;
    assert.equal(rb.ok?.replayed, true, rb.err?.message);
    assert.equal(Number((await q('select count(*)::int as n from ops.master_edit_requests where request_id = $1', [id]))[0].n), 1);
    assert.equal(Number((await q("select count(*)::int as n from events.master_change_events where request_id = $1 and attribute = 'standard_price_jpy'", [id]))[0].n), 1);
  });

  await ta('[3] 同じ request_id を違う SKU へ同時に = 後の方は待ってから 409 request_id_reused (500 にしない)', async () => {
    const id = crypto.randomUUID();
    const g = gate();
    const a = launch(save(dbA, 'p002', { standard_price: '1300' }, { token: await tokenOf('p002'), requestId: id, beforeCommit: g.wait }));
    await sleep(300);
    const b = launch(save(dbB, 'p003', { standard_price: '1300' }, { token: await tokenOf('p003'), requestId: id }));
    await sleep(500);
    assert.equal(b.done, false, 'B は request_id の鍵で待つ');
    g.open();
    assert.ok((await a.promise).ok);
    const rb = await b.promise;
    assert.equal(rb.err?.reason, 'request_id_reused', rb.err?.message);
    assert.notEqual(Number((await q("select standard_price_jpy from core.skus where code = 'p003'"))[0].standard_price_jpy), 1300);
  });

  await ta('[4] 画面を開いた後に別の接続が行を足した (phantom: 仕入先ごとの商品) = 409', async () => {
    const token = await tokenOf('p002');
    await B.query("insert into core.supplier_skus (company_id, supplier_id, sku_id) select 1, s.supplier_id, k.sku_id from core.suppliers s, core.skus k where s.code = '0002' and k.code = 'p002'");
    await assert.rejects(() => save(dbA, 'p002', { name: '直す' }, { token }), (e) => e.reason === 'version_conflict');
  });

  await ta('[5] CSV を作る側が CSV の鍵を持っている間は保存が待つ → 作った CSV (出ている) の列は 409 csv_issued', async () => {
    const token = await tokenOf('p001');
    await B.query('begin');
    await B.query("select pg_advisory_xact_lock(hashtext('ops.ne_csv'))");
    const ex = (await B.query(`insert into ops.ne_csv_exports (kind, col, ne_column, converter_version, encoding, trial, row_count, sha256, file_bytes, compare_run_id, created_by)
      values ('products', 'name', 'syohin_name', 'v1', 'utf8', true, 1, repeat('a', 64), decode('00', 'hex'), 'mc_20300101T000000000Z_abcdef', 'x@test') returning export_id`)).rows[0].export_id;
    await B.query(`insert into ops.ne_csv_export_rows (export_id, source, code_norm, col, ne_code, target, cell, cdb_version, evidence) values ($1, 'to_ne', 'p001', 'name', 'p001', '{"value":"x"}', 'x', 1, '{}')`, [ex]);
    const a = launch(save(dbA, 'p001', { name: 'CSV の後' }, { token }));
    await sleep(500);
    assert.equal(a.done, false, '保存は CSV の鍵で待つ');
    await B.query('commit');
    const ra = await a.promise;
    assert.equal(ra.err?.reason, 'csv_issued', ra.err?.message);
  });

  await ta('[6] 保存の途中で接続が切れた = 何も残らない (処理中の行なし) → 同じ request_id でもう一度保存できる', async () => {
    const X = await openPgClient(u.toString());
    X.on('error', () => {});
    const token = await tokenOf('p002');
    const id = crypto.randomUUID();
    const pid = (await X.query('select pg_backend_pid() as p')).rows[0].p;
    const x = launch(save(pgAdapter(X), 'p002', { name: '切れる保存' }, { token, requestId: id, beforeCommit: async () => { await M.query('select pg_terminate_backend($1)', [pid]); await sleep(300); } }));
    const rx = await x.promise;
    assert.ok(rx.err, '切れた保存は失敗');
    assert.equal(Number((await q('select count(*)::int as n from ops.master_edit_requests where request_id = $1', [id]))[0].n), 0);
    assert.notEqual((await q("select name from core.skus where code = 'p002'"))[0].name, '切れる保存');
    const again = await save(dbA, 'p002', { name: '切れる保存' }, { token: await tokenOf('p002'), requestId: id });
    assert.equal(again.ok, true);
  });

  await ta('[7] 切替の段階を変える取引 (排他の鍵) の途中は、保存が段階の共有の鍵で待つ', async () => {
    await B.query('begin');
    await B.query("select pg_advisory_xact_lock(hashtext('ops.master_cutover'))");
    const a = launch(save(dbA, 'p001', { reorder_months: '3' }, { token: await tokenOf('p001') }));
    await sleep(500);
    assert.equal(a.done, false, '保存は段階の鍵で待つ');
    await B.query('rollback');
    const ra = await a.promise;
    assert.ok(ra.ok, ra.err?.message);
  });

  const observe = async (runId, rows, at) => {
    await W.recordNeSetObservations(dbM, { run_id: runId, observed_at: at, complete: true, sets: [{ set_code: 'ps01', rows }] });
    return (await q('select observation_id::text as id from ops.ne_set_observations where run_id = $1', [runId]))[0].id;
  };
  const setCost = async () => Number((await q("select c.cost_jpy from core.sku_costs c join core.skus s on s.sku_id = c.sku_id where s.code = 'ps01' and c.valid_to is null"))[0]?.cost_jpy);

  await ta('[8] 単品の保存が先 → 上げる側は単品の鍵で待ち、新しい単品の値でセットを計算する', async () => {
    await save(dbA, 'ps01', { components: [{ code: 'p001', qty: 1 }, { code: 'p002', qty: 1 }] }, { token: await tokenOf('ps01') });
    const obsId = await observe('ne_pg_1', [{ code: 'p001', qty: 1, sort: 1 }, { code: 'p002', qty: 1, sort: 2 }], new Date(Date.now() + 60000).toISOString());
    const g = gate();
    const a = launch(save(dbA, 'p002', { cost: { jpy: '250', reason: '先に直す' } }, { token: await tokenOf('p002'), beforeCommit: g.wait }));
    await sleep(300);
    const b = launch(W.promoteComponentRequest(dbB, obsId, { ownership: ALL_COMPANY, now: NOW }));
    await sleep(500);
    assert.equal(b.done, false, '上げる側は p002 の鍵で待つ');
    g.open();
    assert.ok((await a.promise).ok);
    const rb = await b.promise;
    assert.equal(rb.ok?.promoted, true, JSON.stringify(rb.ok || rb.err?.message));
    assert.equal(await setCost(), 100 + 250);
  });

  await ta('[8] 上げる側が先 → 単品の保存は鍵で待ち、含むセットが増えたことに気づいて 409 (古い値のままのセットを残さない)', async () => {
    await save(dbA, 'ps01', { components: [{ code: 'p001', qty: 1 }, { code: 'p002', qty: 1 }, { code: 'p003', qty: 1 }] }, { token: await tokenOf('ps01') });
    const obsId = await observe('ne_pg_2', [{ code: 'p001', qty: 1, sort: 1 }, { code: 'p002', qty: 1, sort: 2 }, { code: 'p003', qty: 1, sort: 3 }], new Date(Date.now() + 120000).toISOString());
    const token = await tokenOf('p003');
    const g = gate();
    const b = launch(W.promoteComponentRequest(dbB, obsId, { ownership: ALL_COMPANY, now: NOW, beforeCommit: g.wait }));
    await sleep(300);
    const a = launch(save(dbA, 'p003', { cost: { jpy: '333', reason: '後から直す' } }, { token }));
    await sleep(500);
    assert.equal(a.done, false, '単品の保存は p003 の鍵で待つ');
    g.open();
    assert.equal((await b.promise).ok?.promoted, true);
    const ra = await a.promise;
    assert.ok(['retry', 'version_conflict'].includes(ra.err?.reason), ra.err?.message || 'ok になった');
    assert.equal(await setCost(), 100 + 250 + 300);
    // 読み直した画面から保存 = 増えたセットも計算し直す
    const s = await save(dbA, 'p003', { cost: { jpy: '333', reason: '読み直した' } }, { token: await tokenOf('p003') });
    assert.ok(s.derived.some((d) => d.code === 'ps01' && d.col === 'cost' && d.to === 100 + 250 + 333), JSON.stringify(s.derived));
  });
} finally {
  for (const c of [A, B, M]) { try { await c.end(); } catch { /* */ } }
  try { await admin.query(`drop database ${dbName} with (force)`); } catch (e) { console.error(`DB を消せなかった: ${e.message}`); }
  await admin.end();
}
console.log(`\n${passed} 件 ok`);
