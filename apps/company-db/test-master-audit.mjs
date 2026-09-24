/**
 * test-master-audit.mjs — マスタの変更の記録 (events.master_change_events) と版番号 (0026。Company DB構想 10 §5.2 / PR ②c-1)
 *
 * 固定する契約:
 *   1 夜間ロードの初回: INSERT が行全体で 1 件ずつ記録され、source_system = company_db_load・run_id・actor_id (host) が付く
 *   2 値が同じ 2 回目: 記録も version も増えない (skus・suppliers・supplier_skus・sku_components・listing_components とも UPDATE しない)
 *   3 値が変わった列だけ 1 列 1 行 (同じ行の変更は change_id で束ねる)。version は通し番号の次の値 (前より大きい別の値)
 *   4 人の編集: set_config で入れた actor / source / request_id / reason が残る。入れなければ system / sql。db_user は必ず残る
 *   5 空にした (json の null) と 行が無い (SQL の null) を区別する。DELETE は行全体を old_value に
 *   6 子の表 (セット構成・原価・出品の構成) の変更で親 (SKU・出品) の version が上がる。親を上げた印は同じ取引の後の UPDATE に漏れない
 *   7 version は入力を信じない (値が変わらない UPDATE で version を書いても戻る・INSERT で書いても通し番号)。
 *     消して同じキーで入れ直しても前の version に戻らない (Codex #1444 R1 Medium)
 *   9 子の表の「値が同じ UPDATE」「管理用の列だけの UPDATE」では親の version を変えない (Codex #1444 R1 Medium)
 *   8 記録は append-only。本体が巻き戻れば記録も残らない。set_config は取引の外に漏れない
 * 使い方: node apps/company-db/test-master-audit.mjs
 */
import assert from 'node:assert/strict';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from '../../scripts/company-db/migrate.mjs';
import { runInitialLoad } from './load/engine.mjs';

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};
const AMZ = 'main@A1VC38T7YXB528';
function makePlan() {
  return {
    skus: [
      { code: 'aud001', name: 'NE の名前 1', kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3, cost: { jpy: 100, source: 'ne', status: 'COMPLETE' } },
      { code: 'aud002', name: 'NE の名前 2', kind: 'single', taxRate: 0.08, taxClass: 'REDUCED_8', handling: 'active', salesClass: 2, cost: { jpy: 200, source: 'ne', status: 'COMPLETE' } },
      { code: 'audset', name: 'NE のセット', kind: 'set', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: null, cost: { jpy: 250, source: 'set_calc', status: 'COMPLETE' } },
    ],
    variationGroups: [],
    setComponents: [{ parentCode: 'audset', childCode: 'aud001', qty: 2, source: 'ne' }],
    listings: [
      { mall: 'amazon', shopCode: AMZ, marketplaceId: 'A1VC38T7YXB528', listingCode: 'pr_aud001', title: 'Amazon', status: 'active', components: [{ code: 'aud001', qty: 1, resolution: 'imported', evidence: { source: 'm_sku_master' } }], asinCandidates: [], fnskuCandidates: [] },
    ],
    observations: [], physicals: [], compliance: [], workers: [],
    suppliers: [{ code: '0001', name: 'AMC', orderMethod: 'fax', leadTimeDays: 10 }],
    supplierSkus: [{ supplierCode: '0001', skuCode: 'aud001', vendorCode: 'AMC-001', stockUnitsPerOrderUnit: 12 }],
  };
}

const pg = new PGlite();
const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
const q = async (sql, params) => (await db.query(sql, params)).rows;
const events = async (where = 'true', params = []) => q(`select * from events.master_change_events where ${where} order by event_id`, params);
const nEvents = async () => (await q('select count(*)::int as n from events.master_change_events'))[0].n;
const ver = async (table, keyCol, code) => Number((await q(`select version from core.${table} where ${keyCol} = $1`, [code]))[0].version);
const skuId = async (code) => Number((await q('select sku_id from core.skus where code = $1', [code]))[0].sku_id);
const run = async (plan, runId) => { const r = await runInitialLoad(db, plan, { log: quiet, runId, host: 'test-host' }); assert.equal(r.ok, true, r.error); return r; };

await ta('[1] 初回の夜間ロード: INSERT が行全体で記録され、誰が (company_db_load・run_id・host) が付く', async () => {
  await run(makePlan(), 'aud_1');
  const ev = await events("operation = 'INSERT' and entity_type = 'sku'");
  assert.equal(ev.length, 3);
  for (const e of ev) {
    assert.equal(e.attribute, null); assert.equal(e.old_value, null); assert.equal(e.source_system, 'company_db_load');
    assert.equal(e.run_id, 'aud_1'); assert.equal(e.actor_id, 'test-host'); assert.equal(e.actor_type, 'system'); assert.ok(e.db_user);
    assert.ok(!('updated_at' in e.new_value) && !('version' in e.new_value) && !('code_norm' in e.new_value));   // 管理用の列は入れない
  }
  const sku1 = ev.find((e) => e.new_value.code === 'aud001');
  assert.equal(sku1.new_value.name, 'NE の名前 1'); assert.equal(Number(sku1.entity_id), await skuId('aud001'));
  assert.deepEqual(sku1.entity_key, { sku_id: await skuId('aud001') });
  const types = new Set((await events("operation = 'INSERT'")).map((e) => e.entity_type));
  for (const t of ['product', 'sku', 'supplier', 'supplier_sku', 'sku_component', 'sku_cost', 'listing', 'listing_component']) assert.ok(types.has(t), t);
  const ss = (await events("entity_type = 'supplier_sku'"))[0];
  assert.equal(ss.entity_id, null); assert.deepEqual(Object.keys(ss.entity_key).sort(), ['sku_id', 'supplier_id']);   // 複合キーは entity_key だけ
});

await ta('[2] 値が同じ 2 回目: 記録も version も増えない (UPDATE しない)', async () => {
  const before = await nEvents();
  const v = [await ver('skus', 'code', 'aud001'), await ver('skus', 'code', 'audset'), await ver('suppliers', 'code', '0001')];
  const r = await run(makePlan(), 'aud_2');
  assert.equal(await nEvents(), before);
  assert.deepEqual([await ver('skus', 'code', 'aud001'), await ver('skus', 'code', 'audset'), await ver('suppliers', 'code', '0001')], v);
  for (const k of ['skus', 'suppliers', 'supplier_skus', 'set_components', 'listings', 'listing_components', 'sku_costs']) assert.equal(r.summary[k].applied, 0, k);
});

await ta('[3] 変わった列だけ 1 列 1 行・change_id で束ねる・version は通し番号の次の値', async () => {
  const v0 = await ver('skus', 'code', 'aud001');
  const p = makePlan(); Object.assign(p.skus[0], { name: 'NE の名前 1 (改)', taxRate: 0.08, taxClass: 'REDUCED_8' });
  await run(p, 'aud_3');
  const ev = await events("run_id = 'aud_3' and entity_type = 'sku'");
  assert.deepEqual(ev.map((e) => e.attribute).sort(), ['name', 'tax_class', 'tax_rate']);
  assert.equal(new Set(ev.map((e) => e.change_id)).size, 1);
  const nm = ev.find((e) => e.attribute === 'name');
  assert.equal(nm.old_value, 'NE の名前 1'); assert.equal(nm.new_value, 'NE の名前 1 (改)');
  const v1 = await ver('skus', 'code', 'aud001');
  assert.ok(v1 > v0, `${v1} > ${v0}`);
});

await ta('[4] 人の編集: set_config の actor / source / request_id / reason が残る。入れなければ system / sql', async () => {
  await db.exec('begin');
  await db.query("select set_config('core.actor_type', 'human', true), set_config('core.actor_id', 'd.nakahara', true), set_config('core.source_system', 'portal', true), set_config('core.request_id', 'req-1', true), set_config('core.reason', '表記ゆれを直した', true)");
  await db.query("update core.suppliers set name = 'アメージングクラフト' where code = '0001'");
  await db.exec('commit');
  const e = (await events("entity_type = 'supplier' and attribute = 'name' and request_id = 'req-1'"))[0];
  assert.equal(e.actor_type, 'human'); assert.equal(e.actor_id, 'd.nakahara'); assert.equal(e.source_system, 'portal'); assert.equal(e.reason_text, '表記ゆれを直した');
  // [8] 取引の外に漏れない: 次の取引は何も入れないので system / sql
  await db.query("update core.suppliers set lead_time_days = 7 where code = '0001'");
  const e2 = (await events("entity_type = 'supplier' and attribute = 'lead_time_days'")).at(-1);
  assert.equal(e2.actor_type, 'system'); assert.equal(e2.source_system, 'sql'); assert.equal(e2.request_id, null); assert.equal(e2.actor_id, null); assert.ok(e2.db_user);
});

await ta('[5] 空にした (json の null) と 行が無い (SQL の null) を区別する。DELETE は行全体を old_value に', async () => {
  await db.query("update core.suppliers set order_method = null where code = '0001'");
  const e = (await events("entity_type = 'supplier' and attribute = 'order_method'")).at(-1);
  assert.equal(e.old_value, 'fax');
  assert.equal((await q("select new_value is null as sql_null, new_value = 'null'::jsonb as json_null from events.master_change_events where event_id = $1", [e.event_id]))[0].json_null, true);
  const sid = await skuId('aud001');
  await db.query('delete from core.supplier_skus where sku_id = $1', [sid]);
  const d = (await events("entity_type = 'supplier_sku' and operation = 'DELETE'"))[0];
  assert.equal(d.new_value, null); assert.equal(d.old_value.vendor_code, 'AMC-001'); assert.equal(d.old_value.stock_units_per_order_unit, 12);
});

await ta('[6] 子の表の変更で親の version が上がる (セット構成・原価 → SKU / 出品の構成 → 出品)。印は後の UPDATE に漏れない', async () => {
  const vs = await ver('skus', 'code', 'audset'); const va = await ver('skus', 'code', 'aud002');
  const lv = Number((await q("select version from core.listings where listing_code = 'pr_aud001'"))[0].version);
  const p = makePlan(); p.setComponents[0].qty = 3; p.skus[1].cost.jpy = 210; p.listings[0].components[0].qty = 2;
  await run(p, 'aud_6');
  assert.ok(await ver('skus', 'code', 'audset') > vs, 'セット構成 → セットの SKU');
  assert.ok(await ver('skus', 'code', 'aud002') > va, '原価 → SKU');
  assert.ok(Number((await q("select version from core.listings where listing_code = 'pr_aud001'"))[0].version) > lv, '出品の構成 → 出品');
  assert.ok((await events("run_id = 'aud_6' and entity_type = 'sku_component' and attribute = 'qty'")).length === 1);
  // 同じ取引で子を変えた後、関係ない行の「値が同じ UPDATE」は version を上げない
  const vOther = await ver('skus', 'code', 'aud001');
  await db.exec('begin');
  await db.query("update core.sku_components set qty = 4 where parent_sku_id = $1", [await skuId('audset')]);
  await db.query("update core.skus set name = name where code = 'aud001'");
  await db.exec('commit');
  assert.equal(await ver('skus', 'code', 'aud001'), vOther);
});

await ta('[7] version は入力を信じない (値が変わらない UPDATE・INSERT で書いても効かない)。消して入れ直しても前の値に戻らない', async () => {
  const v = await ver('skus', 'code', 'aud002');
  await db.query("update core.skus set version = 999 where code = 'aud002'");
  assert.equal(await ver('skus', 'code', 'aud002'), v);
  await db.query("update core.skus set version = 999, name = 'ポータルで直した' where code = 'aud002'");
  const v2 = await ver('skus', 'code', 'aud002');
  assert.ok(v2 > v && v2 !== 999, String(v2));
  // INSERT で version を書いても通し番号
  const sup = Number((await q("insert into core.suppliers (company_id, code, name, version) values (1, '0777', '入力の version', 1) returning version"))[0].version);
  assert.notEqual(sup, 1);
  // 消して同じキーで入れ直しても前の version に戻らない (古い画面の「version = 前の値」の保存が通らない)
  const sid = await skuId('aud002'); const supId = Number((await q("select supplier_id from core.suppliers where code = '0001'"))[0].supplier_id);
  await db.query("insert into core.supplier_skus (company_id, supplier_id, sku_id, vendor_code) values (1, $1, $2, 'V-OLD')", [supId, sid]);
  const vOld = Number((await q('select version from core.supplier_skus where supplier_id = $1 and sku_id = $2', [supId, sid]))[0].version);
  await db.query('delete from core.supplier_skus where supplier_id = $1 and sku_id = $2', [supId, sid]);
  await db.query("insert into core.supplier_skus (company_id, supplier_id, sku_id, vendor_code) values (1, $1, $2, 'V-NEW')", [supId, sid]);
  const vNew = Number((await q('select version from core.supplier_skus where supplier_id = $1 and sku_id = $2', [supId, sid]))[0].version);
  assert.notEqual(vNew, vOld);
  const stale = await db.query("update core.supplier_skus set vendor_code = '古い画面の保存' where supplier_id = $1 and sku_id = $2 and version = $3", [supId, sid, vOld]);
  assert.equal(stale.affectedRows ?? stale.rowCount ?? 0, 0);
  assert.equal((await q('select vendor_code from core.supplier_skus where supplier_id = $1 and sku_id = $2', [supId, sid]))[0].vendor_code, 'V-NEW');
});

await ta('[9] 子の表の「値が同じ UPDATE」「管理用の列だけの UPDATE」では親の version を変えない', async () => {
  const setId = await skuId('audset'); const sid2 = await skuId('aud002');
  const lid = Number((await q("select listing_id from core.listings where listing_code = 'pr_aud001'"))[0].listing_id);
  const lv = async () => Number((await q('select version from core.listings where listing_id = $1', [lid]))[0].version);
  const before = [await ver('skus', 'code', 'audset'), await ver('skus', 'code', 'aud002'), await lv()];
  const nBefore = await nEvents();
  await db.query('update core.sku_components set qty = qty where parent_sku_id = $1', [setId]);
  await db.query('update core.sku_costs set cost_jpy = cost_jpy where sku_id = $1', [sid2]);
  await db.query("update core.sku_costs set created_by_id = 'x' where sku_id = $1", [sid2]);
  await db.query('update core.listing_components set qty = qty where listing_id = $1', [lid]);
  await db.query("update core.listing_components set resolved_by_id = 'someone' where listing_id = $1", [lid]);
  assert.deepEqual([await ver('skus', 'code', 'audset'), await ver('skus', 'code', 'aud002'), await lv()], before);
  assert.equal(await nEvents(), nBefore);
  // 値が変われば変わる (比べ方の確認)
  await db.query('update core.listing_components set qty = qty + 1 where listing_id = $1', [lid]);
  assert.ok(await lv() > before[2]);
});

await ta('[8] 記録は append-only。本体が巻き戻れば記録も残らない', async () => {
  await assert.rejects(db.query('update events.master_change_events set reason_text = null'), /append-only/);
  await assert.rejects(db.query('delete from events.master_change_events'), /append-only/);
  const before = await nEvents();
  const nameBefore = (await q("select name from core.skus where code = 'aud001'"))[0].name;
  const p = makePlan(); p.skus[0].name = '巻き戻る名前'; p.skus.push({ code: 'audbad', name: 'x', kind: 'single', taxRate: 0.05, taxClass: 'STANDARD_10', handling: 'active', salesClass: 1, cost: null });   // 税率の CHECK 違反
  await assert.rejects(runInitialLoad(db, p, { log: quiet, runId: 'aud_bad', host: 'test-host' }), /skus_tax_rate_check/);
  assert.equal(await nEvents(), before);
  assert.equal((await q("select name from core.skus where code = 'aud001'"))[0].name, nameBefore);
  assert.notEqual(nameBefore, '巻き戻る名前');
});

await pg.close();
console.log(`\n${passed} 件 PASS`);
