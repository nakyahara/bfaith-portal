/**
 * test-po-settings-snapshot.mjs — 発注の設定の読むだけの写し (0076・apps/company-db/load/po-settings-snapshot.mjs・AI_reference CompanyDB構想/21 v4 の C2)
 *
 * Company DB = PGlite (全部の migration・夜間ロードで商品と代表の仕入先)。発注アプリ = 一時の DATA_DIR の warehouse-mirror.db (本番の DB に触らない)。
 * 固定する契約:
 *   S1 初めの写し: 3 つの表を入れる・SKU と代表の仕入先を写した時の Company DB で結ぶ・世代 (読んだ時刻・件数・hash)・今の写しの view・商品 360 の列
 *   S2 同じ値 = unchanged (行は触らない・世代は残る)・値が変わる = 足す / 直す / 消す (行の世代 = 変えた回)・代表の仕入先が変わる = 直す
 *   S3 完全に読めない回は入れ替えない: 表が無い・列が無い・ファイルが無い・前の世代より大きく減った = rejected (前の世代のまま・段の失敗)
 *   S4 1 つの読む取引: 1 つめの表を読んだ後に別の接続が書いても、同じ読みの 2 つめの表には入らない (同じ時点の値)
 *   S5 入れる途中で落ちた = 全部巻き戻す (前の世代のまま・世代の行も残らない)
 *   S6 古い写し (36 時間超) = stale・それでも商品 360 は値を出す (決定 5 = b)・時刻が見える
 *   S9 遅れて来た古い読み = 入れ替えない
 *   S10 正規化した後の鍵の重なり = 入れ替えない・S11 鍵を取ってから読む・同じ時刻で中身の違う読み = あいまい = 断る・減りの受け入れは 1 回だけ (S3)
 *   S7 権限: 書けるのは持ち主 (夜間ロード) だけ・見張りは読める・画面のロールは書けない・世代は追記だけ・重い入口の棚卸しに漏れが無い・夜間の段の順 (知らせ → 写し)
 * 使い方: node scripts/test-po-settings-snapshot.mjs
 */
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import Database from 'better-sqlite3';
import { temporaryTestDataDir } from './test-temp-dir.mjs';

const killer = setTimeout(() => { console.error('時間切れ (300 秒)'); process.exit(3); }, 300e3);
killer.unref();
const DATA_DIR = await temporaryTestDataDir(import.meta.url, 'posync-snap-');
process.env.DATA_DIR = DATA_DIR;
const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { createRoles } = await import('./company-db/create-watch-roles.mjs');
const { createMasterEditRoles } = await import('./company-db/create-master-edit-roles.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
const S = await import('../apps/company-db/load/po-settings-snapshot.mjs');
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const POD = await import('../apps/purchase-orders/db.js');
const podb = POD.getDB();
const FILE = path.join(DATA_DIR, 'warehouse-mirror.db');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e && e.stack || e}`); process.exitCode = 1; } }
const errOf = async (p) => { try { await p; } catch (e) { return e; } return null; };

const pg = new PGlite();
await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
await pg.query('set role deploy');
const db = pgliteAdapter(pg);
await applyMigrations(db, { log: () => {} });
await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });
await createMasterEditRoles(pg, {});
const one = async (sql, p) => (await pg.query(sql, p)).rows[0];
const rows = async (sql, p) => (await pg.query(sql, p)).rows;
const sku = (code) => ({ code, name: `商品 ${code}`, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3, cost: { jpy: 100, source: 'ne', status: 'COMPLETE' } });
const CODES = Array.from({ length: 30 }, (_, i) => `snap-${String(i + 1).padStart(2, '0')}`);
const planOf = (primary = '0001') => ({
  skus: CODES.map(sku), variationGroups: [], setComponents: [], listings: [], observations: [], physicals: [], compliance: [], workers: [],
  suppliers: [{ code: '0001', name: 'AMC' }, { code: '0034', name: '三河' }],
  supplierSkus: CODES.map((c) => ({ supplierCode: c === 'snap-01' ? primary : '0001', skuCode: c })),
  primarySuppliers: CODES.map((c) => ({ skuCode: c, supplierCode: c === 'snap-01' ? primary : '0001' })), reorder: { available: true, runId: 'pml_snap' },
});
const lr = await runInitialLoad(db, planOf(), { log: () => {}, runId: 'load_snap_1', now: new Date(Date.now() - 86400e3) });
assert.equal(lr.ok, true, lr.error);

// 発注アプリの値 (商品 30・条件 25・原料 22 = 減り方を見る下限 20 を超える)
const T0 = '2026-10-01T00:00:00.000Z';
const insAttr = podb.prepare('INSERT INTO po_product_attrs (product_key, product_code, order_lot, condition_id, material_group_id, capacity_per_unit, case_group, case_lot, created_via, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)');
CODES.forEach((c, i) => insAttr.run(c, c, i + 1, i % 2 ? 'c-01' : null, i % 3 ? null : 'm-01', i === 0 ? 1.5 : null, null, i === 1 ? 24 : null, 'po-admin', T0, T0));
insAttr.run('ne-only-x', 'NE-ONLY-X', 6, null, null, null, null, null, null, T0, T0);   // Company DB に無い商品 = sku_id は null
const insCond = podb.prepare('INSERT INTO po_order_conditions (condition_id, supplier_code, maker_name, display_name, condition_type, condition_value, unit, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?)');
for (let i = 1; i <= 25; i++) insCond.run(`c-${String(i).padStart(2, '0')}`, '1', null, `条件 ${i}`, '金額', 1000 * i + 0.5, '円', T0, T0);
const insMat = podb.prepare('INSERT INTO po_material_groups (group_id, name, min_order_qty, unit, created_at, updated_at) VALUES (?,?,?,?,?,?)');
for (let i = 1; i <= 22; i++) insMat.run(`m-${String(i).padStart(2, '0')}`, `原料 ${i}`, i === 1 ? null : 10 * i, 'kg', T0, T0);

const pgConnect = async () => ({ query: (t, p) => pg.query(t, p), exec: (t) => pg.exec(t) });
const step = (opts = {}) => S.runPoSettingsSnapshotStep({ url: 'x', dataDir: DATA_DIR, connect: pgConnect, ...opts });
const runs = () => rows(`select status, counts, problems, changes from purchasing.snapshot_runs order by finished_at`);

console.log('写し');
let RUN1 = null;
await ta('[S1] 初めの写し: 3 つの表・SKU と代表の仕入先を結ぶ (無い商品は null)・世代 (読んだ時刻・件数・hash)・今の写しの view・商品 360 の列', async () => {
  const r = await step();
  assert.equal(r.ok, true, JSON.stringify(r));
  assert.equal(r.status, 'applied');
  assert.match(r.note, /\+78 ~0 -0 \(31\/25\/22 行/);
  const run = (await runs())[0];
  assert.deepEqual([run.status, run.counts, run.changes.attrs], ['applied', { attrs: 31, conditions: 25, materials: 22 }, { added: 31, changed: 0, removed: 0 }]);
  RUN1 = (await one(`select run_id, read_at from purchasing.snapshot_runs`));
  const a = await one(`select o.*, s.code from purchasing.sku_order_settings o left join core.skus s on s.sku_id = o.sku_id where o.product_key = 'snap-01'`);
  assert.deepEqual([a.code, Number(a.order_lot), Number(a.capacity_per_unit), a.material_group_id, a.primary_supplier_code, a.source_updated_at, a.run_id],
    ['snap-01', 1, 1.5, 'm-01', '0001', T0, RUN1.run_id]);
  assert.equal((await one(`select sku_id from purchasing.sku_order_settings where product_key = 'ne-only-x'`)).sku_id, null);
  assert.equal(Number((await one(`select condition_value from purchasing.order_condition_groups where condition_id = 'c-03'`)).condition_value), 3000.5);
  assert.equal((await one(`select min_order_qty from purchasing.material_groups where group_id = 'm-01'`)).min_order_qty, null);
  const st = await one('select run_id, stale, age_hours, rejected_since_at from purchasing.v_snapshot_status');
  assert.deepEqual([st.run_id, st.stale, Number(st.age_hours) < 1, st.rejected_since_at], [RUN1.run_id, false, true, null]);
  const v = await one(`select po_order_lot, po_condition_id, po_case_lot, po_settings_read_at, po_settings_age_hours from mart.v_product_360 where sku_code = 'snap-02'`);
  assert.deepEqual([Number(v.po_order_lot), v.po_condition_id, Number(v.po_case_lot), new Date(v.po_settings_read_at).toISOString()], [2, 'c-01', 24, new Date(RUN1.read_at).toISOString()]);
  // 商品 360 の行は増えない (SKU に 1 行)
  assert.equal((await one('select count(*)::int as n from mart.v_product_360')).n, (await one('select count(*)::int as n from core.skus')).n);
});

await ta('[S2] 同じ値 = unchanged (行の世代は変わらない)・値が変わる = 足す / 直す / 消す (変えた行だけ新しい世代)・代表の仕入先が変わる = 直す', async () => {
  let r = await step();
  assert.deepEqual([r.ok, r.status], [true, 'unchanged'], JSON.stringify(r));
  assert.match(r.note, /変化なし/);
  assert.equal((await one(`select count(*)::int as n from purchasing.sku_order_settings where run_id <> $1`, [RUN1.run_id])).n, 0);
  const T1 = '2026-10-02T00:00:00.000Z';
  podb.prepare(`UPDATE po_product_attrs SET order_lot = 48, updated_at = ? WHERE product_key = 'snap-03'`).run(T1);
  podb.prepare(`DELETE FROM po_product_attrs WHERE product_key = 'snap-04'`).run();
  insAttr.run('snap-new', 'snap-new', 3, null, null, null, null, null, 'master-edit:new', T1, T1);
  podb.prepare(`UPDATE po_order_conditions SET display_name = '条件 2 (直した)', updated_at = ? WHERE condition_id = 'c-02'`).run(T1);
  r = await step();
  assert.deepEqual([r.ok, r.status], [true, 'applied'], JSON.stringify(r));
  const last = (await runs()).at(-1);
  assert.deepEqual(last.changes, { attrs: { added: 1, changed: 1, removed: 1 }, conditions: { added: 0, changed: 1, removed: 0 }, materials: { added: 0, changed: 0, removed: 0 } });
  const run2 = (await one(`select run_id from purchasing.snapshot_runs order by finished_at desc limit 1`)).run_id;
  assert.deepEqual((await rows(`select product_key from purchasing.sku_order_settings where run_id = $1 order by 1`, [run2])).map((x) => x.product_key), ['snap-03', 'snap-new']);
  assert.equal(await one(`select 1 from purchasing.sku_order_settings where product_key = 'snap-04'`), undefined, '消えた行が残った');
  assert.equal(Number((await one(`select order_lot from purchasing.sku_order_settings where product_key = 'snap-03'`)).order_lot), 48);
  // 代表の仕入先が Company DB で変わった (夜間ロード) = 写しの primary_supplier_code も直す (発注アプリの値は同じ)
  const lr2 = await runInitialLoad(db, planOf('0034'), { log: () => {}, runId: 'load_snap_2', now: new Date() });
  assert.equal(lr2.ok, true, lr2.error);
  r = await step();
  assert.equal(r.status, 'applied', JSON.stringify(r));
  assert.equal((await one(`select primary_supplier_code from purchasing.sku_order_settings where product_key = 'snap-01'`)).primary_supplier_code, '0034');
  assert.deepEqual((await runs()).at(-1).changes.attrs, { added: 0, changed: 1, removed: 0 });
});

await ta('[S3] 完全に読めない回は入れ替えない (前の世代のまま・rejected の世代・段の失敗): 表が無い・列が無い・ファイルが無い・大きく減った', async () => {
  const before = await rows(`select * from purchasing.sku_order_settings order by product_key`);
  const cur = (await one('select run_id from purchasing.v_snapshot_status')).run_id;
  // 表が無い (移行の途中) = 別のファイルで
  const broken = path.join(DATA_DIR, 'broken'); fs.mkdirSync(broken, { recursive: true });
  fs.copyFileSync(FILE, path.join(broken, 'warehouse-mirror.db'));
  for (const f of ['-wal', '-shm']) if (fs.existsSync(FILE + f)) fs.copyFileSync(FILE + f, path.join(broken, 'warehouse-mirror.db' + f));
  const b = new Database(path.join(broken, 'warehouse-mirror.db'));
  b.exec('ALTER TABLE po_material_groups RENAME TO po_material_groups_old');
  b.close();
  let r = await step({ dataDir: broken });
  assert.equal(r.ok, false); assert.match(r.note, /入れ替えない.*no_table: po_material_groups/);
  // 列が無い
  const b2 = new Database(path.join(broken, 'warehouse-mirror.db'));
  b2.exec('ALTER TABLE po_material_groups_old RENAME TO po_material_groups; ALTER TABLE po_product_attrs DROP COLUMN order_lot');
  b2.close();
  r = await step({ dataDir: broken });
  assert.equal(r.ok, false); assert.match(r.note, /no_column: po_product_attrs\.order_lot/);
  // ファイルが無い
  r = await step({ dataDir: path.join(DATA_DIR, 'nothing-here') });
  assert.equal(r.ok, false); assert.match(r.note, /no_file/);
  // 大きく減った (商品の行が半分未満)
  const b3 = new Database(path.join(broken, 'warehouse-mirror.db'));
  b3.exec(`ALTER TABLE po_product_attrs ADD COLUMN order_lot REAL; DELETE FROM po_product_attrs WHERE product_key > 'snap-10'`);
  b3.close();
  r = await step({ dataDir: broken });
  assert.equal(r.ok, false); assert.match(r.note, /shrink: po_product_attrs が 31 → 10 行/);
  assert.deepEqual(await rows(`select * from purchasing.sku_order_settings order by product_key`), before, '不完全な回で写しを変えた');
  const st = await one('select run_id, rejected_since_at from purchasing.v_snapshot_status');
  assert.equal(st.run_id, cur);
  assert.ok(st.rejected_since_at, '断った回が今の写しの様子に出ない');
  assert.equal((await rows(`select status from purchasing.snapshot_runs where status = 'rejected'`)).length, 4);
  // Codex #1708 R1 M2: 人が決めた時だけ減った表を受け入れる = env「表:件数」・1 回だけ・使った回を世代に残す
  assert.deepEqual(S.parseShrinkOverride({ PO_SNAPSHOT_ACCEPT_SHRINK: 'attrs:10' }), { token: 'attrs:10', tables: { attrs: 10 } });
  assert.deepEqual(S.parseShrinkOverride({ PO_SNAPSHOT_ACCEPT_SHRINK: 'attrs:10, materials:3' }).tables, { attrs: 10, materials: 3 });
  for (const bad of ['', 'attrs', 'attrs:x', 'items:10', 'attrs:10,attrs:11', 'x'.repeat(201)]) assert.equal(S.parseShrinkOverride({ PO_SNAPSHOT_ACCEPT_SHRINK: bad }), null, bad);
  const bfile = path.join(broken, 'warehouse-mirror.db');
  // 件数が違う env = 受け入れない
  let acc = await S.applyPoSettingsSnapshot(db, () => S.readPoSettingsSnapshot(bfile), { shrinkOverride: S.parseShrinkOverride({ PO_SNAPSHOT_ACCEPT_SHRINK: 'attrs:11' }) });
  assert.deepEqual([acc.ok, acc.status], [false, 'rejected']); assert.match(acc.problems.join(), /件数が違う/);
  acc = await S.applyPoSettingsSnapshot(db, () => S.readPoSettingsSnapshot(bfile), { shrinkOverride: S.parseShrinkOverride({ PO_SNAPSHOT_ACCEPT_SHRINK: 'attrs:10' }) });
  assert.deepEqual([acc.ok, acc.status, acc.changes.attrs.removed, acc.override.token], [true, 'applied', 21, 'attrs:10']);
  assert.deepEqual((await one(`select override from purchasing.snapshot_runs where run_id = $1`, [acc.run_id])).override,
    { kind: 'accept_shrink', token: 'attrs:10', tables: { attrs: 10 }, accepted: [{ table: 'attrs', was: 31, now: 10 }] });
  // 直れば入る (元の 31 行に戻る = 受け入れの後は前の世代 10 行から増えるだけ)
  // 直れば入る (元の 31 行に戻る = 受け入れの後は前の世代 10 行から増えるだけ)
  r = await step();
  assert.deepEqual([r.ok, r.status], [true, 'applied']);
  r = await step();
  assert.deepEqual([r.ok, r.status], [true, 'unchanged']);
  assert.equal((await one('select rejected_since_at from purchasing.v_snapshot_status')).rejected_since_at, null);
  // 同じ env の 2 回目 = 効かない (env を消し忘れても次の減りは守る)
  acc = await S.applyPoSettingsSnapshot(db, () => S.readPoSettingsSnapshot(bfile), { shrinkOverride: S.parseShrinkOverride({ PO_SNAPSHOT_ACCEPT_SHRINK: 'attrs:10' }) });
  assert.deepEqual([acc.ok, acc.status], [false, 'rejected']); assert.match(acc.problems.join(), /使用済み/);
  // 段の道でも env を読む (1 回だけ = もう使った値)
  process.env.PO_SNAPSHOT_ACCEPT_SHRINK = 'attrs:10';
  try { r = await step({ dataDir: broken }); } finally { delete process.env.PO_SNAPSHOT_ACCEPT_SHRINK; }
  assert.equal(r.ok, false); assert.match(r.note, /使用済み/);
  assert.equal(Number((await one(`select count(*)::int as n from purchasing.sku_order_settings`)).n), 31);
});

await ta('[S10] (Codex #1708 R1 M3) 正規化した後に鍵が重なる (大文字小文字・前後の空白だけ違う商品コード) = 入れ替えない (黙って 1 行を捨てない・どの鍵か)', async () => {
  const before = await rows(`select * from purchasing.sku_order_settings order by product_key`);
  podb.prepare('INSERT INTO po_product_attrs (product_key, product_code, order_lot, created_at, updated_at) VALUES (?,?,?,?,?)').run('SNAP-01', 'SNAP-01', 99, T0, T0);
  try {
    const r = await step();
    assert.equal(r.ok, false); assert.match(r.note, /duplicate_key: po_product_attrs の鍵が正規化で重なる \(snap-01\)/);
    assert.deepEqual(await rows(`select * from purchasing.sku_order_settings order by product_key`), before);
  } finally { podb.prepare(`DELETE FROM po_product_attrs WHERE product_key = 'SNAP-01'`).run(); }
  assert.equal((await step()).ok, true);
});

await ta('[S11] (Codex #1708 R1 M1) 鍵を取ってから読む (段の道)・同じ時刻 (ミリ秒) で中身の違う読み = あいまい = 断る', async () => {
  // 読む関数は鍵の後に呼ばれる (取引の中・鍵を持っている)
  let lockedWhenRead = null;
  const r = await S.applyPoSettingsSnapshot(db, () => { lockedWhenRead = true; return S.readPoSettingsSnapshot(FILE); });
  assert.equal(lockedWhenRead, true); assert.ok(r.ok);
  const lk = await rows(`select count(*)::int as n from pg_locks where locktype = 'advisory'`);
  assert.equal(lk[0].n, 0, '取引の後も鍵が残った');
  // 同じ時刻・違う中身
  const cur = await one(`select read_at from purchasing.v_snapshot_status`);
  podb.prepare(`UPDATE po_product_attrs SET order_lot = 77, updated_at = ? WHERE product_key = 'snap-06'`).run('2026-10-05T00:00:00.000Z');
  const odd = S.readPoSettingsSnapshot(FILE);
  odd.readAt = new Date(cur.read_at);
  const amb = await S.applyPoSettingsSnapshot(db, odd);
  assert.deepEqual([amb.ok, amb.status], [false, 'rejected']); assert.match(amb.problems.join(), /^ambiguous_read/);
  // 同じ時刻・同じ中身 = unchanged (あいまいでない)
  podb.prepare(`UPDATE po_product_attrs SET order_lot = 6, updated_at = ? WHERE product_key = 'snap-06'`).run(T0);
  const same = S.readPoSettingsSnapshot(FILE);
  same.readAt = new Date(cur.read_at);
  assert.equal((await S.applyPoSettingsSnapshot(db, same)).status, 'unchanged');
});

await ta('[S4] 1 つの読む取引: 1 つめの表を読んだ後に別の接続が書いても、同じ読みのほかの表には入らない (同じ時点の値)', async () => {
  const snap = S.readPoSettingsSnapshot(FILE, { betweenTables: () => {
    const w = new Database(FILE); w.pragma('busy_timeout = 2000');
    w.prepare('INSERT INTO po_material_groups (group_id, name, min_order_qty, unit, created_at, updated_at) VALUES (?,?,?,?,?,?)').run('m-late', '後から', 1, 'kg', T0, T0);
    w.close();
  } });
  assert.deepEqual(snap.problems, []);
  assert.equal(snap.rows.materials.some((m) => m.group_id === 'm-late'), false, '読みの途中の書き込みが同じ読みに入った (同じ時点でない)');
  assert.equal(snap.counts.materials, 22);
  const again = S.readPoSettingsSnapshot(FILE);
  assert.equal(again.counts.materials, 23);
  podb.prepare(`DELETE FROM po_material_groups WHERE group_id = 'm-late'`).run();
});

await ta('[S5] 入れる途中で落ちた = 全部巻き戻す (前の世代のまま・その回の世代の行も残らない)', async () => {
  const before = await rows(`select * from purchasing.order_condition_groups order by condition_id`);
  const nRuns = (await one('select count(*)::int as n from purchasing.snapshot_runs')).n;
  podb.prepare(`UPDATE po_order_conditions SET display_name = 'X', updated_at = ? WHERE condition_id = 'c-05'`).run('2026-10-03T00:00:00.000Z');
  const snap = S.readPoSettingsSnapshot(FILE);
  snap.rows.materials[0] = { ...snap.rows.materials[0], name: null };   // name は not null = 入れる途中で落ちる
  const e = await errOf(S.applyPoSettingsSnapshot(db, snap));
  assert.ok(e, '落ちなかった');
  assert.deepEqual(await rows(`select * from purchasing.order_condition_groups order by condition_id`), before);
  assert.equal((await one('select count(*)::int as n from purchasing.snapshot_runs')).n, nRuns);
  const r = await step();
  assert.equal(r.status, 'applied');
  assert.equal((await one(`select display_name from purchasing.order_condition_groups where condition_id = 'c-05'`)).display_name, 'X');
});

await ta('[S6] 古い写し (36 時間超) = stale・商品 360 は値を出したまま (決定 5 = b)・いつの値かが見える', async () => {
  await pg.query('alter table purchasing.snapshot_runs disable trigger trg_append_only_row');
  try { await pg.query(`update purchasing.snapshot_runs set read_at = read_at - interval '40 hours'`); } finally { await pg.query('alter table purchasing.snapshot_runs enable trigger trg_append_only_row'); }
  const st = await one('select stale, age_hours from purchasing.v_snapshot_status');
  assert.deepEqual([st.stale, Number(st.age_hours) >= 40], [true, true]);
  const v = await one(`select po_order_lot, po_settings_age_hours from mart.v_product_360 where sku_code = 'snap-03'`);
  assert.deepEqual([Number(v.po_order_lot), Number(v.po_settings_age_hours) >= 40], [48, true]);
});

await ta('[S7] 権限: 書けるのは持ち主だけ (見張り・画面のロールは読むだけ / 読めない)・世代は追記だけ・重い入口の棚卸しに漏れが無い・夜間の段の順 (知らせ → 写し)', async () => {
  const tb = async (role, t, p) => (await one(`select has_table_privilege($1, $2, $3) as ok`, [role, t, p])).ok;
  for (const t of ['purchasing.snapshot_runs', 'purchasing.sku_order_settings', 'purchasing.order_condition_groups', 'purchasing.material_groups', 'purchasing.v_snapshot_status']) {
    assert.equal(await tb('watcher', t, 'select'), true, `watcher ${t}`);
    for (const p of ['insert', 'update', 'delete']) {
      if (t.startsWith('purchasing.v_')) continue;
      assert.equal(await tb('watcher', t, p), false, `watcher ${p} ${t}`);
      assert.equal(await tb('master_edit', t, p), false, `master_edit ${p} ${t}`);
    }
  }
  assert.equal((await one(`select has_schema_privilege('master_edit', 'purchasing', 'usage') as ok`)).ok, false);
  // Codex #1708 R1 L1: 画面のロールは mart も使えない (v_product_360 の po_* 経由でも読めない)・画面のコードは v_product_360 を読んでいない
  assert.equal((await one(`select has_schema_privilege('master_edit', 'mart', 'usage') as ok`)).ok, false);
  const meSrc = fs.readdirSync(new URL('../apps/master-edit/', import.meta.url)).filter((f) => /\.m?js$/.test(f)).map((f) => fs.readFileSync(new URL(`../apps/master-edit/${f}`, import.meta.url), 'utf8')).join('\n');
  assert.ok(!/v_product_360|purchasing\./.test(meSrc), 'マスタの入力のコードが v_product_360 / purchasing を読んでいる');
  assert.equal((await one(`select has_schema_privilege('public', 'purchasing', 'create') as ok`)).ok, false);
  await pg.query('set role watcher');
  try {
    assert.ok((await one('select count(*)::int as n from purchasing.sku_order_settings')).n > 0);
    assert.ok((await one(`select po_order_lot from mart.v_product_360 where sku_code = 'snap-03'`)).po_order_lot);
  } finally { await pg.query('set role deploy'); }
  assert.match(String((await errOf(pg.query(`update purchasing.snapshot_runs set status = 'applied'`)))?.message), /append|reject|変え|mutation/i);
  // 見張りのロールを流し直しても読める (0076 の後) / 0076 の前の DB でも流せる (purchasing が無い = 飛ばす)
  const { roleStatements } = await import('./company-db/create-watch-roles.mjs');
  const st = roleStatements({ dbName: 'x', owner: 'deploy' }).filter((x) => x.includes('purchasing'));
  assert.equal(st.length, 1); assert.match(st[0], /if exists \(select 1 from pg_namespace where nspname = 'purchasing'\)/);
  const { heavyEntryFindings } = await import('./company-db/heavy-entry-manifest.mjs');
  assert.deepEqual(await heavyEntryFindings(db), []);
  const N = await import('../apps/company-db/nightly.mjs');
  assert.deepEqual(N.DEFAULT_STEPS.map((s) => s.id), ['po-settings-outbox', 'po-settings-snapshot']);
});

await ta('[S8] 0076 の前の DB = 段は飛ばす (ok)・URL / DATA_DIR が無い = 段の失敗', async () => {
  const fake = async () => ({ query: async (t) => (/to_regclass\('purchasing\.snapshot_runs'\)/.test(t) ? { rows: [{ ok: false }] } : { rows: [] }), exec: async () => {} });
  assert.deepEqual(await S.runPoSettingsSnapshotStep({ url: 'x', dataDir: DATA_DIR, connect: fake }), { ok: true, skipped: true, note: '0076 の前 (写しの表が無い)' });
  assert.equal((await S.runPoSettingsSnapshotStep({ url: '', dataDir: DATA_DIR, connect: fake })).ok, false);
  assert.equal((await S.runPoSettingsSnapshotStep({ url: 'x', dataDir: '', connect: fake })).ok, false);
});

await ta('[S9] 今の写しより前に読んだ値 (遅れて来た読み) = 入れ替えない (新しい値を古い値で戻さない)', async () => {
  const early = S.readPoSettingsSnapshot(FILE);
  podb.prepare(`UPDATE po_product_attrs SET order_lot = 96, updated_at = ? WHERE product_key = 'snap-05'`).run('2026-10-04T00:00:00.000Z');
  await new Promise((r) => setTimeout(r, 5));
  assert.equal((await step()).status, 'applied');
  const r = await S.applyPoSettingsSnapshot(db, early);
  assert.deepEqual([r.ok, r.status], [false, 'rejected']);
  assert.match(r.problems.join(), /^older_read/);
  assert.equal(Number((await one(`select order_lot from purchasing.sku_order_settings where product_key = 'snap-05'`)).order_lot), 96);
});

console.log(`\n${passed} 件 ok`);
if (process.exitCode) console.log('NG があります');
setTimeout(() => process.exit(process.exitCode || 0), 50).unref();
