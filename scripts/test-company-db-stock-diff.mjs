/**
 * test-company-db-stock-diff.mjs — 在庫の「増えた / 減った」(events.inventory_events の inferred) を、完走した日どうしの差から作る (08 §3.2 / §3.3 ②。D2c)
 *
 * 流れは本物で通す: 取込 (captureLogizardInventory) → 日の締め (closeStockDays) → 差 (inferStockDiffs)。
 * 細かい形 (SKU の付け替え・あふれ) だけは、日次の表に直接 1 日を作って確かめる。
 *
 * 使い方: node scripts/test-company-db-stock-diff.mjs
 */
import assert from 'node:assert/strict';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';
import { captureLogizardInventory, closeStockDays, SOURCE, SCOPE } from '../apps/company-db/inventory/logizard.mjs';
import { inferStockDiffs, inferStockDiffDay, prevDay, CALC_VERSION, SOURCE_SYSTEM } from '../apps/company-db/inventory/stock-diff.mjs';

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e).split('\n').slice(0, 6).join('\n      ')); } };
const quiet = () => {};

const pg = new PGlite();
const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
const one = async (sql, p = []) => (await pg.query(sql, p)).rows[0];
const all = async (sql, p = []) => (await pg.query(sql, p)).rows;
const co = (await one(`select company_id from core.companies order by 1 limit 1`)).company_id;
await pg.query(`insert into core.products (company_id, name) values ($1, '見本A'), ($1, '見本B'), ($1, '見本C')`, [co]);
await pg.query(`insert into core.skus (company_id, product_id, sku_kind, code, name) select company_id, product_id, 'single', case name when '見本A' then 'AAA-1' when '見本B' then 'bbb-2' else 'CCC-3' end, name from core.products`);
const skuOf = async (code) => (await one(`select sku_id from core.skus where code = $1`, [code])).sku_id;
const skuA = await skuOf('AAA-1'), skuB = await skuOf('bbb-2'), skuC = await skuOf('CCC-3');

const row = (id, loke, qty) => ({ '商品ID': id, '商品名': `名前 ${id}`, 'バーコード': '4900000000001', 'ブロック略称': 'P3FA', 'ロケ': loke, '品質区分名': '良品', '有効期限': '', '入荷日': '2026/08/01', '在庫数': qty, '引当数': 0, 'ロケ業務区分': 'ピック', '最終入荷日': '2026/08/01', '最終出荷日': null, '在庫日': '2026/09/01' });
const capture = (day, rows) => captureLogizardInventory(db, { rows, capturedAt: `${day}T01:00:00.000Z` /* JST 10:00 */, log: quiet });
const eventsOf = (to) => all(`select sku_id, qty_delta, qty_after, confidence, actor_type, source_system, source_ref, reason_code, location_id, occurred_at, idempotency_key, payload from events.inventory_events where source_ref like $1 order by sku_id`, [`%..${to}`]);
const marks = async () => (await all(`select to_date::text as d, from_date::text as f, status, skip_reason, events, unresolved_changed from snapshots.stock_diff_days order by to_date`)).map((m) => [m.d, m.f, m.status, m.skip_reason, m.events, m.unresolved_changed]);

console.log('差の本体');
await t('prevDay は月初・年初をまたぐ', () => { assert.deepEqual([prevDay('2026-10-01'), prevDay('2027-01-01'), prevDay('2028-03-01')], ['2026-09-30', '2026-12-31', '2028-02-29']); });
await t('🚨 取込 → 締め → 差: 最初の日は first_day・前日が missing なら prev_not_complete (間に取れなかった日がある区間は作らない)・動かなかった日は 0 件でも done の印が残る。差は SKU 単位 (ロケを合算)・行が無い側は 0・SKU が分からないコードは数だけ', async () => {
  await capture('2026-09-01', [row('AAA-1', '001', 7), row('AAA-1', '002', 3), row('bbb-2', '003', 7), row('CCC-3', '004', 5), row('zzz-9', '005', 1)]);
  await capture('2026-09-02', [row('AAA-1', '001', 7), row('AAA-1', '002', 1), row('bbb-2', '003', 7), row('zzz-9', '005', 4), row('ddd-4', '006', 2)]);   // A −2・B 同じ・C 消えた・zzz 変わった (SKU 不明)・ddd 新しい (SKU 不明)
  // 9/03 は取得なし → missing
  await capture('2026-09-04', [row('AAA-1', '001', 8), row('bbb-2', '003', 7), row('zzz-9', '005', 4)]);
  await capture('2026-09-05', [row('AAA-1', '009', 8), row('bbb-2', '003', 7), row('zzz-9', '005', 4)]);   // A は棚を移しただけ = SKU 単位では動いていない
  const closed = await closeStockDays(db, { todayJst: '2026-09-06', log: quiet });
  assert.deepEqual(closed.closed.map((c) => `${c.day.slice(8)}:${c.status}`), ['01:complete', '02:complete', '03:missing', '04:complete', '05:complete']);
  const r = await inferStockDiffs(db, { log: quiet });
  assert.deepEqual([r.status, r.backlog, r.days.map((d) => [d.day.slice(8), d.status, d.skipReason, d.events, d.unresolvedChanged])],
    ['ok', false, [['01', 'skipped', 'first_day', 0, 0], ['02', 'done', null, 2, 2], ['04', 'skipped', 'prev_not_complete', 0, 0], ['05', 'done', null, 0, 0]]]);
  assert.deepEqual(await marks(), [['2026-09-01', null, 'skipped', 'first_day', 0, 0], ['2026-09-02', '2026-09-01', 'done', null, 2, 2], ['2026-09-04', null, 'skipped', 'prev_not_complete', 0, 0], ['2026-09-05', '2026-09-04', 'done', null, 0, 0]]);
  const ev = await eventsOf('2026-09-02');
  assert.deepEqual(ev.map((e) => [e.sku_id, e.qty_delta, e.qty_after, e.confidence, e.actor_type, e.source_system, e.source_ref, e.reason_code, e.location_id]),
    [[skuA, -2, 8, 'inferred', 'system', SOURCE_SYSTEM, '2026-09-01..2026-09-02', null, null], [skuC, -5, 0, 'inferred', 'system', SOURCE_SYSTEM, '2026-09-01..2026-09-02', null, null]]);
  assert.equal(ev[0].idempotency_key, `${CALC_VERSION}:${SCOPE}:${skuA}:2026-09-01:2026-09-02`);
  assert.equal(new Date(ev[0].occurred_at).toISOString(), '2026-09-02T01:00:00.000Z', 'occurred_at は当日の世代');
  const p = typeof ev[0].payload === 'string' ? JSON.parse(ev[0].payload) : ev[0].payload;
  assert.deepEqual([p.calc, p.source, p.scope, p.from, p.to, Number(p.qty_before), p.from_generation, p.to_generation], [CALC_VERSION, SOURCE, SCOPE, '2026-09-01', '2026-09-02', 10, '2026-09-01T01:00:00.000Z', '2026-09-02T01:00:00.000Z']);
  assert.equal((await all(`select 1 from events.inventory_events`)).length, 2, '動いていない日・skipped の日にイベントを作っている');
});
await t('二度目は何もしない (印のある日は探さない)。1 日を名指しで呼んでも exists。印だけ消して作り直しても、イベントは増えず・印の件数は同じ (追記できた行数ではなく、変わった SKU の数)', async () => {
  const again = await inferStockDiffs(db, { log: quiet });
  assert.deepEqual([again.status, again.days, again.backlog], ['ok', [], false]);
  assert.equal((await inferStockDiffDay(db, '2026-09-02', { log: quiet })).status, 'exists');
  await pg.query(`delete from snapshots.stock_diff_days where to_date = date '2026-09-02'`);
  const redo = await inferStockDiffs(db, { log: quiet });
  assert.deepEqual(redo.days.map((d) => [d.day, d.status, d.events, d.unresolvedChanged]), [['2026-09-02', 'done', 2, 2]]);
  assert.equal((await all(`select 1 from events.inventory_events`)).length, 2);
});
await t('🚨 間に SKU が登録されたコードを「+全量」と読まない (コードで突き合わせてから SKU に寄せる): 9/05 は SKU 不明の 4 個 → 9/06 は SKU が分かって 6 個 = +2。表記だけ変わったコード (bbb-2 → BBB-2) も同じ SKU の +2 が 1 件', async () => {
  await pg.query(`insert into core.products (company_id, name) values ($1, '見本Z')`, [co]);
  await pg.query(`insert into core.skus (company_id, product_id, sku_kind, code, name) select company_id, product_id, 'single', 'zzz-9', name from core.products where name = '見本Z'`);
  const skuZ = await skuOf('zzz-9');
  await capture('2026-09-06', [row('AAA-1', '009', 8), row('BBB-2', '003', 9), row('zzz-9', '005', 6)]);
  await closeStockDays(db, { todayJst: '2026-09-07', log: quiet });
  const r = await inferStockDiffs(db, { log: quiet });
  assert.deepEqual(r.days.map((d) => [d.day, d.status, d.events, d.unresolvedChanged]), [['2026-09-06', 'done', 2, 0]]);
  assert.deepEqual((await eventsOf('2026-09-06')).map((e) => [e.sku_id, e.qty_delta, e.qty_after]).sort((x, y) => Number(x[0]) - Number(y[0])), [[skuB, 2, 9], [skuZ, 2, 6]].sort((x, y) => Number(x[0]) - Number(y[0])));
});

// ── 日次の表に直接 1 日を作る (SKU の付け替え・あふれ・待ち)
let seq = 0;
async function mkDay(day, rows, { status = 'complete' } = {}) {
  const runId = `t-${day}-${++seq}`;
  await pg.query(`insert into ops.ingest_runs (ingest_run_id, source_system, entity, scope_key, host, started_at, finished_at, status, complete, rows_seen, source_tz, checksum) values ($1, $2, 'inventory', $3, 'test', now(), now(), 'success', true, $4, 'UTC', $5)`, [runId, SOURCE, SCOPE, rows.length, `${day}T01:00:00.000Z`]);
  await pg.query(`insert into snapshots.stock_capture_days (snapshot_date, source, scope_key, company_id, status, ingest_run_id) values ($1::date, $2, $3, $4, 'building', $5)`, [day, SOURCE, SCOPE, co, runId]);
  for (const [code, skuId, qty] of rows) await pg.query(`insert into snapshots.sku_stock_daily (snapshot_date, source, scope_key, source_code, company_id, sku_id, qty, captured_at, ingest_run_id) values ($1::date, $2, $3, $4, $5, $6, $7, $8::timestamptz, $9)`, [day, SOURCE, SCOPE, code, co, skuId, qty, `${day}T01:00:00.000Z`, runId]);
  if (status === 'complete') await pg.query(`update snapshots.stock_capture_days set status = 'complete', completed_at = now() where snapshot_date = $1::date and source = $2 and scope_key = $3`, [day, SOURCE, SCOPE]);
}
await t('前日と当日で別の SKU に当たるコード (マスタの付け替え) は、前の SKU の −全量 と 新しい SKU の +全量。同じ SKU に当たる 2 つのコードは合算してから比べる', async () => {
  // 本番では締めが毎日 1 行ずつ作る (complete か missing) ので、日付の穴は無い。ここでも 9/30 を missing で置く
  await pg.query(`insert into snapshots.stock_capture_days (snapshot_date, source, scope_key, company_id, status, ingest_run_id) values (date '2026-09-30', $1, $2, $3, 'missing', null)`, [SOURCE, SCOPE, co]);
  await mkDay('2026-10-01', [['X-1', skuA, 10], ['x-1b', skuA, 5], ['Y-1', skuB, 3]]);
  await mkDay('2026-10-02', [['X-1', skuC, 10], ['x-1b', skuA, 6], ['Y-1', skuB, 3]]);   // X-1 が A → C に付け替わった。x-1b は +1
  const r = await inferStockDiffs(db, { log: quiet });
  assert.deepEqual(r.days.map((d) => [d.day, d.status, d.skipReason, d.events]), [['2026-10-01', 'skipped', 'prev_not_complete', 0], ['2026-10-02', 'done', null, 2]]);
  assert.deepEqual((await eventsOf('2026-10-02')).map((e) => [e.sku_id, e.qty_delta, e.qty_after]).sort((x, y) => Number(x[0]) - Number(y[0])), [[skuA, -9, 6], [skuC, 10, 10]].sort((x, y) => Number(x[0]) - Number(y[0])));
});
await t('🚨 あふれる差は例外 (黙って回り込まない)。その日は印もイベントも残らず、次の回にまた拾われる', async () => {
  await mkDay('2026-10-03', [['X-1', skuC, 2000000000], ['x-1c', skuC, 2000000000], ['Y-1', skuB, 3]]);
  const n0 = (await all(`select 1 from events.inventory_events`)).length;
  await assert.rejects(inferStockDiffs(db, { log: quiet }), (e) => /2026-10-03 の在庫の差を作れない/.test(e.message));
  assert.deepEqual([(await all(`select 1 from events.inventory_events`)).length, (await marks()).some((m) => m[0] === '2026-10-03')], [n0, false]);
  await pg.query(`delete from ops.ingest_runs where false`);   // (取引が壊れたままでないこと = 次の問い合わせが通る)
});
await t('前日が作りかけ (building) の日は待つ = 印を付けない・backlog。後ろの日は、前日がそろっていれば先に作れる', async () => {
  await pg.exec(`begin; set local snapshots.maintenance = 'on'; delete from snapshots.sku_stock_daily where snapshot_date = date '2026-10-03'; delete from snapshots.stock_capture_days where snapshot_date = date '2026-10-03'; commit;`);
  await mkDay('2026-10-03', [['Y-1', skuB, 3]], { status: 'building' });
  await mkDay('2026-10-04', [['Y-1', skuB, 4]]);
  await mkDay('2026-10-05', [['Y-1', skuB, 9]]);
  const r = await inferStockDiffs(db, { log: quiet });
  assert.deepEqual([r.backlog, r.days.map((d) => [d.day, d.status, d.events])], [true, [['2026-10-05', 'done', 1]]]);
  await pg.query(`update snapshots.stock_capture_days set status = 'complete', completed_at = now() where snapshot_date = date '2026-10-03'`);
  const r2 = await inferStockDiffs(db, { log: quiet });
  assert.deepEqual([r2.backlog, r2.days.map((d) => [d.day, d.status, d.events])], [false, [['2026-10-03', 'done', 2], ['2026-10-04', 'done', 1]]]);
});
await t('maxDays で打ち切ると backlog。ロジザード以外の source は拒む (世代が取得時刻、という前提が成り立たない)', async () => {
  await mkDay('2026-10-06', [['Y-1', skuB, 9]]); await mkDay('2026-10-07', [['Y-1', skuB, 9]]);
  const r = await inferStockDiffs(db, { log: quiet, maxDays: 1 });
  assert.deepEqual([r.backlog, r.days.map((d) => d.day)], [true, ['2026-10-06']]);
  assert.deepEqual([(await inferStockDiffs(db, { log: quiet })).days.map((d) => [d.day, d.events]), (await inferStockDiffs(db, { log: quiet })).days], [[['2026-10-07', 0]], []]);
  await assert.rejects(inferStockDiffDay(db, '2026-10-07', { source: 'ne', log: quiet }), /いま差を作れるのは logizard だけ/);
});
await t('🚨 0022 が未適用の DB では何もしない (not_migrated)。例外にしない = 毎時ジョブを落とさない', async () => {
  const pg2 = new PGlite(); const db2 = pgliteAdapter(pg2);
  await applyMigrations(db2, { log: quiet, to: '0021' });
  assert.deepEqual(await inferStockDiffs(db2, { log: quiet }), { status: 'not_migrated', days: [], backlog: false, locked: false });
  await pg2.close();
});
await t('印の表の制約: done は前日つき・skipped は理由つきで 0 件・前日は当日の 1 日前・取得記録の無い日には付けられない', async () => {
  const ins = (cols) => pg.query(`insert into snapshots.stock_diff_days (to_date, source, scope_key, calc_version, company_id, from_date, status, skip_reason, events) values ($1::date, 'logizard', 'main', 'x:v9', $2, $3::date, $4, $5, $6)`, cols);
  await assert.rejects(ins(['2026-10-07', co, null, 'done', null, 0]));
  await assert.rejects(ins(['2026-10-07', co, '2026-10-05', 'done', null, 0]));
  await assert.rejects(ins(['2026-10-07', co, null, 'skipped', null, 0]));
  await assert.rejects(ins(['2026-10-07', co, null, 'skipped', 'first_day', 3]));
  await assert.rejects(ins(['2026-11-30', co, null, 'skipped', 'first_day', 0]));
  await ins(['2026-10-07', co, '2026-10-06', 'done', null, 0]);   // 版が違えば同じ日にもう 1 つ (式を変えたときの作り直し)
});

console.log(`\n${ok} ok / ${ng} NG`);
process.exitCode = ng ? 1 : 0;
