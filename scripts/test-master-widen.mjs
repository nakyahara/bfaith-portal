/**
 * test-master-widen.mjs — 広げる道 PR-1 (0058) の DB の部品を PGlite で確かめる (設計 = 広げる道 v11 §3・§13)
 *
 *   1  0058 を当てる。sku_kind の持ち主が load の間は何も変わらない (区分の変更・SKU の削除・最終形の崩れも通る = G18 / G19 は何もしない)・
 *      夜間ロードの番号 (report.load_commit_seq・latestLoadCommit) は文字 (bigint を JS の Number にしない)
 *   G5 段階 new_open では持ち主の epoch を 0058 の関数でだけ変える (今の prepare / 直接の UPDATE は拒む)・足すだけ・広げてよいキーは skus.sku_kind だけ・会社 1 だけ・
 *      知らない古い入口の一覧は拒む・開いている試みがあれば次の prepare は拒む
 *   10 同じ持ち主表で cancel → prepare = 別の試み・新しい base (前の試みのロードは数えない)・試みの表は関数でだけ書く
 *   15 照合の封: started → completed / failed は 1 回だけ・seq と run_id の組・payload の検査・直接の書き込みは拒む
 *   17 取得の件数の守り: 0 件・ページ欠け・直前の completed より 10% 以上の減り = untrusted・減りだけは DB の持ち主が受け入れられる
 *   24 保守の印: 理由が空・GUC だけ・直接の INSERT は拒む / 本物の印は同じ取引だけ有効
 * 使い方: node scripts/test-master-widen.mjs   (実 PostgreSQL の同時実行・ロール・widen 本体・G18 / G19・復元は scripts/test-master-widen-pg.mjs)
 */
import assert from 'node:assert/strict';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';
import { runInitialLoad } from '../apps/company-db/load/engine.mjs';
import * as OS from '../apps/company-db/load/ownership-state.mjs';
import * as W from '../apps/company-db/load/widen-state.mjs';
import { OWNED_COLUMNS } from '../config/master-ownership.mjs';
import { ownershipHash } from '../lib/master-cutover.mjs';
import { forceNewOpen, fakeLoad, hex, nowIso, ZERO_GATE } from './fixtures/master-widen.mjs';

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};

const ALL_LOAD = Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'load']));
const BASE = { ...ALL_LOAD, 'skus.name': 'company', 'products.name': 'company' };   // 切替の後 (new_open) の active の例
const WIDEN = { ...BASE, 'skus.sku_kind': 'company' };
const MANIFEST = { entries: [{ id: 'warehouse.register.post', kind: 'code' }, { id: 'ne:item-screen', kind: 'manual', owner_cols: ['skus.name'] }, { id: 'ne:set-kind', kind: 'manual', owner_cols: ['skus.sku_kind'] }] };
const single = (code) => ({ code, name: code, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3, cost: { jpy: 100, source: 'ne', status: 'COMPLETE' } });
const plan = {
  skus: [single('p001'), single('p002'), single('p003'),
    { code: 's001', name: 'セット', kind: 'set', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: null, cost: { jpy: 200, source: 'set_calc', status: 'COMPLETE' } }],
  variationGroups: [], setComponents: [{ parentCode: 's001', childCode: 'p001', qty: 1, source: 'ne' }, { parentCode: 's001', childCode: 'p002', qty: 1, source: 'ne' }],
  listings: [], observations: [], physicals: [], compliance: [], workers: [], suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: [],
};

const pg = new PGlite();
const db = pgliteAdapter(pg);
const q = async (sql, p) => (await db.query(sql, p)).rows;
const codeOf = async (sql, p) => { try { await db.query(sql, p); } catch (e) { return `${e.code}:${e.message}`; } return null; };
const inTxRollback = async (fn) => { await db.query('begin'); try { return await fn(); } finally { await db.query('rollback'); } };
await applyMigrations(db, { log: quiet });

await ta('[1] 0058 を当てる。sku_kind が load の間は G18 / G19 は何もしない・夜間ロードの番号は文字', async () => {
  const r = await runInitialLoad(db, plan, { log: quiet, runId: 'wd_load_1', host: 'test' });
  assert.equal(r.ok, true, r.error);
  assert.equal(typeof r.load_commit_seq, 'string'); assert.match(r.load_commit_seq, /^[1-9][0-9]*$/);
  const last = await OS.latestLoadCommit(db);
  assert.equal(last.commit_seq, r.load_commit_seq);
  assert.equal(OS.isCommitSeqText(last.commit_seq), true);
  assert.equal(OS.isCommitSeqText(Number(last.commit_seq)), false);   // Number は受けない
  assert.equal(OS.isCommitSeqText('9223372036854775808'), false); assert.equal(OS.isCommitSeqText('9223372036854775807'), true); assert.equal(OS.isCommitSeqText('01'), false);
  assert.equal((await q('select ops.sku_kind_locked() as l'))[0].l, false);
  await inTxRollback(async () => {
    await db.query("update core.skus set sku_kind = 'set' where code = 'p003'");   // 区分の変更 (load の持ち主 = 今までどおり通る)
    await db.query("insert into core.skus (company_id, sku_kind, code, name) values (1, 'set', 'z001', 'z')");
    await db.query("delete from core.skus where code = 'z001'");   // 削除も通る
    await db.query("update core.skus set product_id = null where code = 'p002' and false");
    await db.query("insert into core.sku_components (company_id, parent_sku_id, child_sku_id, qty, source) select 1, p.sku_id, c.sku_id, 1, 'ne' from core.skus p, core.skus c where p.code = 'p002' and c.code = 'p003'");
    await db.query('set constraints all immediate');   // G19 (deferred) も何もしない
  });
  assert.deepEqual((await q('select ops.assert_sku_kind_shape_after_restore() as r'))[0].r, { single_product_mismatch: 0, non_set_parent_components: 0, locked: false });
});

let attempt1;
await ta('[G5] 段階 new_open では持ち主の epoch を 0058 の関数でだけ変える・足すだけ・skus.sku_kind だけ・会社 1 だけ・知らない一覧は拒む', async () => {
  await forceNewOpen(db, BASE);
  assert.equal((await q('select phase from ops.master_cutover_state'))[0].phase, 'new_open');
  // 今の prepare (④a・JS の直接の UPDATE) は DB が拒む
  await assert.rejects(OS.prepareOwnership(db, { map: WIDEN, actor: 't' }), /widen_protocol_required/);
  assert.match(await codeOf(`update ops.master_ownership_state set active_map = active_map || '{"skus.sku_kind":"company"}'::jsonb where id = 1`), /42501:widen_protocol_required/);
  assert.match(await codeOf('delete from ops.master_ownership_state'), /42501:widen_protocol_required/);
  const prep = (map, extra = {}) => W.prepareWiden(db, { companyId: 1, map, loaderFingerprint: hex('f'), manifest: MANIFEST, actor: 't', ...extra });
  await assert.rejects(prep(WIDEN), /manifest_unknown/);   // まだどのプロセスも見ていない一覧
  await db.query('insert into ops.master_legacy_manifests (manifest_hash, entries) values (ops.legacy_manifest_hash($1::jsonb), $1::jsonb)', [JSON.stringify(MANIFEST)]);
  await assert.rejects(prep({ ...WIDEN, 'skus.name': 'load' }), /widen_not_additive/);
  await assert.rejects(prep({ ...WIDEN, 'products.status': 'company' }), /widen_key_not_allowed/);
  await assert.rejects(prep(BASE), /widen_nothing/);
  await assert.rejects(prep(WIDEN, { companyId: 2 }), /unsupported_company/);
  await assert.rejects(prep(WIDEN, { loaderFingerprint: 'x' }), /loader_fingerprint/);
  await assert.rejects(prep({ ...WIDEN, 'skus.sku_kind': 'yes' }), /invalid_input/);
  attempt1 = await prep(WIDEN);
  assert.deepEqual(attempt1.added_keys, ['skus.sku_kind']);
  assert.deepEqual(attempt1.required_manual_entries, ['ne:set-kind']);   // 足すキーと owner_cols が重なる手の入口だけ
  assert.equal(attempt1.base_commit_seq, (await OS.latestLoadCommit(db)).commit_seq);   // 文字のまま
  const st = await OS.readOwnershipState(db);
  assert.equal(st.prepared.hash, ownershipHash(WIDEN)); assert.equal(st.active.hash, ownershipHash(BASE));
  await assert.rejects(prep(WIDEN), /prepared_exists/);   // 開いている試みがある
  const open = await W.readOpenWidenAttempt(db);
  assert.equal(open.widen_prepare_id, attempt1.widen_prepare_id); assert.equal(open.base_commit_seq, attempt1.base_commit_seq); assert.equal(open.loader_fingerprint, hex('f'));
  // 試みの表・出来事は関数でだけ書く
  assert.match(await codeOf("update ops.master_widen_attempts set state = 'widened'"), /42501/);
  assert.match(await codeOf('delete from ops.master_widen_attempts'), /消さない/);
  assert.match(await codeOf('delete from ops.master_widen_events'), /append-only/);
  // 手の入口の停止: 要る入口だけ・1 回だけ
  await assert.rejects(W.recordWidenManualStop(db, { attemptId: attempt1.widen_prepare_id, entryId: 'ne:item-screen', stoppedBy: 't' }), /entry_not_required/);
  await assert.rejects(W.recordWidenManualStop(db, { attemptId: attempt1.widen_prepare_id, entryId: 'ne:set-kind', stoppedBy: ' ' }), /止めた人/);
  const s = await W.recordWidenManualStop(db, { attemptId: attempt1.widen_prepare_id, entryId: 'ne:set-kind', stoppedBy: 't' });
  assert.equal(s.entry_id, 'ne:set-kind');
  await assert.rejects(W.recordWidenManualStop(db, { attemptId: attempt1.widen_prepare_id, entryId: 'ne:set-kind', stoppedBy: 't' }), /duplicate|一意|unique/i);
});

await ta('[10] 同じ持ち主表で cancel → prepare = 別の試み・新しい base (前の試みのロードは数えない)・判定は試みの中の commit だけを数える', async () => {
  // 前の試みの中に 2 つのロード
  await fakeLoad(db, { epoch: 'active', hash: ownershipHash(BASE) });
  await fakeLoad(db, { epoch: 'prepared', hash: ownershipHash(WIDEN) });
  const c1 = await W.widenCheck(db, { attemptId: attempt1.widen_prepare_id, companyId: 1 });
  assert.equal(c1.counts.commits_after_base, 2);
  await W.cancelWiden(db, { attemptId: attempt1.widen_prepare_id, actor: 't' });
  assert.equal((await OS.readOwnershipState(db)).prepared, null);
  await assert.rejects(W.cancelWiden(db, { attemptId: attempt1.widen_prepare_id, actor: 't' }), /attempt_not_prepared/);
  const a2 = await W.prepareWiden(db, { companyId: 1, map: WIDEN, loaderFingerprint: hex('f'), manifest: MANIFEST, actor: 't' });
  assert.notEqual(a2.widen_prepare_id, attempt1.widen_prepare_id);
  assert.equal(BigInt(a2.base_commit_seq), BigInt(attempt1.base_commit_seq) + 2n);
  const c2 = await W.widenCheck(db, { attemptId: a2.widen_prepare_id, companyId: 1 });
  assert.equal(c2.ok, false); assert.equal(c2.counts.commits_after_base, 0);
  assert.ok(c2.problems.some((p) => /試みの中の commit が 0 個/.test(p)), JSON.stringify(c2.problems));
  assert.ok(c2.problems.some((p) => /manual_stops/.test(p)));   // 前の試みの停止は数えない
  const old = await W.widenCheck(db, { attemptId: attempt1.widen_prepare_id, companyId: 1 });
  assert.ok(old.problems.some((p) => /attempt_not_prepared: 試みの状態が cancelled/.test(p)));
  await assert.rejects(W.widenCheck(db, { attemptId: a2.widen_prepare_id, companyId: 2 }), /unsupported_company/);
  await W.cancelWiden(db, { attemptId: a2.widen_prepare_id, actor: 't' });
});

await ta('[15] 照合 ② の新商品のゲートの結果: 5 つの数の形・取得の完了は今日 (JST) で今より前・同じ回は 1 回だけ・最終形は DB が数える・直接は書けない / 許可は一番新しい行が全部 0 で widen の後のときだけ', async () => {
  const at = (ms = -1000) => new Date(Date.now() + ms).toISOString();
  const rec = (run, kg = ZERO_GATE, p = at(), s = p) => q('select ops.record_new_entry_gate($1, $2, $3, $4::jsonb) as r', [run, p, s, JSON.stringify(kg)]).then((x) => x[0].r);
  for (const [kg, why] of [[{ ...ZERO_GATE, extra: 0 }, '知らない鍵'], [(({ unknown_kind: _, ...r }) => r)(ZERO_GATE), '鍵が足りない'], [{ ...ZERO_GATE, raw_mismatch: -1 }, '負'],
    [{ ...ZERO_GATE, raw_mismatch: 1.5 }, '小数'], [{ ...ZERO_GATE, raw_mismatch: 'x' }, '文字'], [[], '配列']]) {
    await assert.rejects(rec('mc_gate_bad', kg), /invalid_input: kind_gate/, why);
  }
  await assert.rejects(rec('bad id!'), /invalid_input/);
  await assert.rejects(rec('mc_gate_t', ZERO_GATE, '2030-01-01 00:00:00'), /RFC 3339/);                       // timezone なし
  await assert.rejects(rec('mc_gate_t', ZERO_GATE, at(60000)), /今より後/);
  await assert.rejects(rec('mc_gate_t', ZERO_GATE, at(-36 * 3600e3)), /stale_fetch/);                           // 古い取得の数は残さない
  const r1 = await rec('mc_gate_1', { ...ZERO_GATE, unknown_kind: '2' });
  assert.equal(typeof r1.result_id, 'string'); assert.deepEqual(r1.shape, { single_product_mismatch: 0, non_set_parent_components: 0 });
  assert.deepEqual(await q("select kind_gate, single_product_mismatch::int as a, non_set_parent_components::int as b from ops.new_entry_gate_results where compare_run_id = 'mc_gate_1'"),
    [{ kind_gate: { ...ZERO_GATE, unknown_kind: 2 }, a: 0, b: 0 }]);                                               // 数字の文字は数に直して残す
  await assert.rejects(rec('mc_gate_1'), /duplicate|unique|一意/i);                                               // 同じ回は 1 回だけ
  assert.match(await codeOf("insert into ops.new_entry_gate_results (compare_run_id, products_complete_at, setproducts_complete_at, kind_gate, single_product_mismatch, non_set_parent_components, recorded_by) values ('x', now(), now(), '{}', 0, 0, 'x')"), /42501/);
  assert.match(await codeOf("update ops.new_entry_gate_results set kind_gate = '{}'"), /append-only/);
  // 許可: 5 つの数が 0 でない・区分の持ち主が company でない・widen の記録が無い = 出さない (理由を全部)
  await assert.rejects(q("select ops.grant_new_entry_lease('single', 'mc_gate_1')"), (e) => /lease_denied/.test(e.message) && /kind_gate: .*unknown_kind/.test(e.message) && /sku_kind_not_company/.test(e.message) && /not_widened/.test(e.message));
  await assert.rejects(q("select ops.grant_new_entry_lease('set', 'mc_gate_1')"), /invalid_input/);
  await rec('mc_gate_2');
  await assert.rejects(q("select ops.grant_new_entry_lease('single', 'mc_gate_2')"), (e) => !/kind_gate/.test(e.message) && !/compare_run_mismatch/.test(e.message) && /sku_kind_not_company/.test(e.message));   // 一番新しい行を見る
  await assert.rejects(q("select ops.grant_new_entry_lease('single', 'mc_gate_1')"), /compare_run_mismatch/);   // 今回の回が一番新しい行でない = 出さない (結果を書く前に落ちた再実行)
  // 照合 ② の始めに閉じる (watch_writer): 許可が無くても停止の床を全部の種類に足す・どの回か残す → 前の行ではもう出せない
  const cl = (await q("select ops.close_new_entry_for_compare('mc_gate_3') as r"))[0].r;
  const maxId = (await q('select max(result_id)::text as m from ops.new_entry_gate_results'))[0].m;
  assert.deepEqual([cl.revoked, cl.floor_result_id], [0, maxId]);
  assert.deepEqual(await q("select kind, floor_result_id::text as f from ops.master_new_entry_stop_floors where closed_by_compare_run_id = 'mc_gate_3' order by kind"), [{ kind: 'set', f: maxId }, { kind: 'single', f: maxId }]);
  await assert.rejects(q("select ops.grant_new_entry_lease('single', 'mc_gate_2')"), /stop_floor/);
  await assert.rejects(q("select ops.close_new_entry_for_compare('bad id!')"), /invalid_input/);
  assert.equal((await q("select ops.new_entry_lease_valid('single') as v"))[0].v, false);
  // 取り消し = 許可が無くても停止の床 (一番新しい結果の行の番号) を足す
  const rv = (await q("select ops.revoke_new_entry_lease('single', '試験') as r"))[0].r;
  assert.deepEqual([rv.revoked, rv.floor_result_id], [0, (await q('select max(result_id)::text as m from ops.new_entry_gate_results'))[0].m]);
  await assert.rejects(q("select ops.revoke_new_entry_lease('single', ' ')"), /理由/);
  // 期限 = 東京の今日の翌日 10:00 (session の TimeZone に左右されない)
  await db.query("set timezone = 'America/Los_Angeles'");
  const exp = async (t) => (await q("select to_char(ops.new_entry_lease_expiry($1::timestamptz) at time zone 'Asia/Tokyo', 'YYYY-MM-DD HH24:MI') as e", [t]))[0].e;
  assert.equal(await exp('2030-01-10T23:59:00+09:00'), '2030-01-11 10:00'); assert.equal(await exp('2030-01-10T00:01:00+09:00'), '2030-01-11 10:00');
  await db.query('reset timezone');
  // アプリの鍵の入口: 知らない種類は拒む・今の許可が無い = false
  assert.equal((await q("select ops.acquire_new_entry_locks('single') as v"))[0].v, false);
  await assert.rejects(q("select ops.acquire_new_entry_locks('nope')"), /知らない新商品の種類/);
});

await ta('[21] NE で一度でも見たコードの履歴: 0058 の時に今の NE のコードを seed・毎日の照合が足す・今朝の取得から消えても新しいコードにしない・追記だけ', async () => {
  // 別の DB: 0058 を当てる前に NE のコードがある = seed で履歴に入る
  const pg2 = new PGlite(); const db2 = pgliteAdapter(pg2);
  const files = (await import('node:fs')).readdirSync(new URL('../db/company/migrations/', import.meta.url)).filter((f) => /^\d{4}_.*\.sql$/.test(f)).sort();
  await applyMigrations(db2, { log: quiet, only: files.filter((f) => f < '0058').map((f) => f.slice(0, 4)) }).catch(() => null);
  const before58 = (await db2.query(`select to_regclass('ops.master_ne_code_history') is null as ok`)).rows[0].ok;
  if (before58) {
    await db2.query("insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ('mc_20301009T000000000Z_abcdef', now(), 0)");
    await db2.query('select ops.record_ne_codes($1::jsonb)', [JSON.stringify({ compare_run_id: 'mc_20301009T000000000Z_abcdef', entries: [{ code_norm: 'seed-1', kind: 'product', state: 'ok', ne_code: 'SEED-1', spellings: ['SEED-1'] }] })]);
    await applyMigrations(db2, { log: quiet });
    assert.deepEqual((await db2.query("select source, first_seen_compare_run_id from ops.master_ne_code_history where code_norm = 'seed-1'")).rows, [{ source: 'seed', first_seen_compare_run_id: 'mc_20301009T000000000Z_abcdef' }]);
    assert.equal((await db2.query("select ops.new_sku_code_problem('seed-1') as p")).rows[0].p, 'code_in_ne');
  }
  await pg2.close();
  // 毎日の照合が足す → 次の朝の取得で消えても新しいコードにしない
  const run = (i) => `mc_20301010T00000000${i}Z_abcdef`;
  for (const i of [1, 2]) await db.query("insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, now() + ($2::int * interval '1 minute'), 0)", [run(i), i]);
  const rec = (i, entries) => db.query('select ops.record_ne_codes($1::jsonb) as r', [JSON.stringify({ compare_run_id: run(i), entries })]);
  await rec(1, [{ code_norm: 'ne-only-1', kind: 'product', state: 'ok', ne_code: 'ne-only-1', spellings: ['ne-only-1'] }]);
  assert.deepEqual(await q("select source, first_seen_compare_run_id from ops.master_ne_code_history where code_norm = 'ne-only-1'"), [{ source: 'record_ne_codes', first_seen_compare_run_id: run(1) }]);
  await rec(2, []);   // 今朝の取得 (小さな欠け) から消えた
  assert.equal((await q("select count(*)::int as n from ops.master_ne_codes where code_norm = 'ne-only-1'"))[0].n, 0);
  assert.equal((await q("select ops.new_sku_code_problem('ne-only-1') as p"))[0].p, 'code_in_ne');
  assert.equal((await q("select ops.new_sku_code_problem('never-seen-1') as p"))[0].p, null);
  assert.match(await codeOf("delete from ops.master_ne_code_history where code_norm = 'ne-only-1'"), /append-only/);
  assert.match(await codeOf("insert into ops.master_ne_code_history (code_norm, kind, source) values ('x-1', 'product', 'bootstrap')"), /check|違反/i);   // 源は seed / record_ne_codes だけ
});


await ta('[24] 保守の印: 理由が空・GUC だけ・直接の INSERT は拒む / 本物の印は同じ取引だけ有効', async () => {
  assert.match(await codeOf("select ops.begin_master_maintenance('  ')"), /22023/);
  await inTxRollback(async () => {
    await db.query("select set_config('ops.master_maintenance', gen_random_uuid()::text, true)");
    assert.equal((await q('select ops.master_maintenance_active() as a'))[0].a, false);   // GUC だけ
    assert.match(await codeOf("insert into ops.master_maintenance_marks (mark_id, txid, session_role, reason) values (gen_random_uuid(), txid_current(), session_user, 'x')"), /maintenance_mark_forged/);
  });
  let id;
  await inTxRollback(async () => {
    id = (await q("select ops.begin_master_maintenance('試験') as id"))[0].id;
    assert.equal((await q('select ops.master_maintenance_active() as a'))[0].a, true);
  });
  // 前の取引の印 (commit した印でも) を GUC に置いても、今の取引では有効でない
  await db.query("select ops.begin_master_maintenance('commit する印')");
  const kept = (await q('select mark_id::text as id from ops.master_maintenance_marks order by created_at desc limit 1'))[0].id;
  await inTxRollback(async () => {
    await db.query("select set_config('ops.master_maintenance', $1, true)", [kept]);
    assert.equal((await q('select ops.master_maintenance_active() as a'))[0].a, false);
  });
  assert.ok(id);
});

console.log(`${passed} 件 PASS`);
