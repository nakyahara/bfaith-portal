/**
 * test-master-edit-pg.mjs — マスタの入力 (lib/master-write.mjs・0050) の**同時実行**を、実 PostgreSQL の独立した接続で確かめる
 *   (PGlite は 1 接続なので書けない。Codex ⑤-R1 H4 の試験)
 *
 * 固定する契約:
 *   1 同じ SKU を 2 人が同じ画面から保存: 後の人は SKU の鍵で待ち、前の人の commit の後に 409 (編集の印が違う)。両方は書かない
 *   2 同じ request_id が 2 つ並んで来る (押し直し): 後の方は鍵で待ち、前の方の結果をそのまま返す (記録は 1 行・変更も 1 回)
 *   3 画面を開いた後に別の接続が仕入先ごとの商品の行を足した (phantom) = 409
 *   4 CSV を作る側が CSV の鍵を持っている間は保存が待ち、その CSV (作った = 出ている) の列を変える保存は 409 csv_issued
 *   5 保存の途中で接続が切れた: 何も残らない (処理中の行を作らない) = 同じ request_id でもう一度保存できる
 *   6 切替の段階を進める取引の途中は保存が段階の行で待つ / 段階の行を読めないと保存しない (fail-closed)
 * 使い方: TEST_PG_URL=postgres://postgres:pw@localhost:54329/postgres node scripts/test-master-edit-pg.mjs
 *   🚨 使い捨ての PostgreSQL だけ (新しい DB を作って最後に消す)。localhost 以外の URL は拒む (本番を渡さない)。package.json の試験には入れない (PostgreSQL が要る)
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
/** 外から開ける扉 (保存の commit の直前で止める) */
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
  const plan = {
    skus: [
      { code: 'p001', name: '単品 1', kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3, cost: { jpy: 100, source: 'ne', status: 'COMPLETE' } },
      { code: 'p002', name: '単品 2', kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3, cost: { jpy: 200, source: 'ne', status: 'COMPLETE' } },
    ],
    variationGroups: [], setComponents: [], listings: [], observations: [], physicals: [], compliance: [], workers: [],
    suppliers: [{ code: '0001', name: 'AMC' }, { code: '0002', name: 'ビーフリー' }], supplierSkus: [{ supplierCode: '0001', skuCode: 'p001' }],
  };
  const r0 = await runInitialLoad(dbM, plan, { log: () => {}, runId: 'load_pg_1', now: new Date('2030-01-05T03:00:00Z') });
  assert.equal(r0.ok, true, r0.error);
  for (const to of ['frozen', 'company_owner', 'new_open']) await C.advanceCutoverPhase(dbM, { to, actor: 'test@test' });
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

  await ta('[3] 画面を開いた後に別の接続が行を足した (phantom: 仕入先ごとの商品) = 409', async () => {
    const token = await tokenOf('p002');
    await B.query("insert into core.supplier_skus (company_id, supplier_id, sku_id) select 1, s.supplier_id, k.sku_id from core.suppliers s, core.skus k where s.code = '0002' and k.code = 'p002'");
    await assert.rejects(() => save(dbA, 'p002', { name: '直す' }, { token }), (e) => e.reason === 'version_conflict');
  });

  await ta('[4] CSV を作る側が CSV の鍵を持っている間は保存が待つ → 作った CSV (出ている) の列は 409 csv_issued', async () => {
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

  await ta('[5] 保存の途中で接続が切れた = 何も残らない (処理中の行なし) → 同じ request_id でもう一度保存できる', async () => {
    const X = await openPgClient(u.toString());
    X.on('error', () => {});   // 切った接続の error でプロセスを落とさない
    const token = await tokenOf('p002');
    const id = crypto.randomUUID();
    const pid = (await X.query('select pg_backend_pid() as p')).rows[0].p;
    // commit の直前 (保存の記録の done も書いた後) に接続を切る
    const x = launch(save(pgAdapter(X), 'p002', { name: '切れる保存' }, { token, requestId: id, beforeCommit: async () => { await M.query('select pg_terminate_backend($1)', [pid]); await sleep(300); } }));
    const rx = await x.promise;
    assert.ok(rx.err, '切れた保存は失敗');
    assert.equal(Number((await q('select count(*)::int as n from ops.master_edit_requests where request_id = $1', [id]))[0].n), 0);
    assert.notEqual((await q("select name from core.skus where code = 'p002'"))[0].name, '切れる保存');
    const again = await save(dbA, 'p002', { name: '切れる保存' }, { token: await tokenOf('p002'), requestId: id });
    assert.equal(again.ok, true);
  });

  await ta('[6] 段階を進める取引の途中は保存が段階の行で待つ (fail-closed の読み)', async () => {
    // new_open の先は無い = 進める関数は失敗する。ここでは段階の行に排他の鍵を持つ取引で待つことだけ確かめる
    await B.query('begin');
    await B.query('select 1 from ops.master_cutover_state where id = 1 for update');
    const a = launch(save(dbA, 'p001', { reorder_months: '3' }, { token: await tokenOf('p001') }));
    await sleep(500);
    assert.equal(a.done, false, '保存は段階の行で待つ');
    await B.query('rollback');
    const ra = await a.promise;
    assert.ok(ra.ok, ra.err?.message);
  });
} finally {
  for (const c of [A, B, M]) { try { await c.end(); } catch { /* */ } }
  try { await admin.query(`drop database ${dbName} with (force)`); } catch (e) { console.error(`DB を消せなかった: ${e.message}`); }
  await admin.end();
}
console.log(`\n${passed} 件 ok`);
