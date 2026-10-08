/**
 * test-master-bulk.mjs — 一覧で選んで、まとめて変える (apps/master-edit/bulk.mjs・PR2・10/8)
 *
 * Company DB = PGlite (段階 new_open・持ち主 = 全部 company)。保存は画面だけのロール master_edit で流す。本物の router も HTTP 越しに通す
 * 固定する契約:
 *   1 対象外 / 保存できない の判定 (値の前 = 項目の板の数): 例外の SKU は常に対象外・セットの原価 / 税率 / 仕入先 / 売上分類・取扱を中止した商品 (取扱を戻す以外)・
 *     登録をやめた・無いコード / 先の日の原価 (含むセットも)・NE に取り込む CSV が出ている = 最初から「保存できない」(Codex M5)
 *   2 200 件の上限 (選ぶ時点・API も 413)・重なりは 1 つ・20 件ずつ
 *   3 全部の項目 (原価・売価・取扱・売上分類・税率・仕入先) の一括の保存 = 1 件の保存 (saveSku) と同じ記録 (人・request_id・理由「一括 ◯ 件」)
 *   4 部分の成功: 途中で 1 件だめでも、できた分は変えたまま。だめな分は理由と直し方の組 (Codex M7)
 *   5 二重押し: 同じ一括の番号でもう一度送っても二重に書かない (前の結果が返る・やり直した分も)
 *   6 同じセットの構成品を続けて変える = version_conflict → この一括の分だけなら読み直して 1 回だけやり直す / ほかの人の変更があれば「だめ」
 *   7 一緒に変わるセット (Codex High 3・M3): 確かめと保存で同じ計算 (混ざった税率・一部の構成品だけ)・構成品の失敗 = 保存の後の本当の値をサーバーが返す
 *   8 サーバーでも対象外を止める (画面が送ってきても書かない)・理由が要る項目 (原価・中止)・選べない仕入先
 *   9 権限: 名簿にない人は確かめも保存も 403・Origin・保存を開いていない = 左のチェックを出さない
 *  10 画面の描画 (本物の router = 渡し忘れを見る): 左のチェックの列・帯と引き出し・設定の JSON・画面の JS が読める・名簿にない人は出さない
 * 使い方: node scripts/test-master-bulk.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import http from 'node:http';
import vm from 'node:vm';
import express from 'express';
const OG = await import('../lib/master-owner-gate.mjs');
const { OWNED_COLUMNS } = await import('../config/master-ownership.mjs');
OG.__setCapableForTest(OWNED_COLUMNS);
const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { createRoles } = await import('./company-db/create-watch-roles.mjs');
const { createMasterEditRoles } = await import('./company-db/create-master-edit-roles.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
const { forceNewOpen } = await import('./fixtures/master-widen.mjs');
const W = await import('../lib/master-write.mjs');
const C = await import('../lib/master-cutover.mjs');
const B = await import('../apps/master-edit/bulk.mjs');
const { default: router, __setPgClientFactory, __setClock, __setShippingRatesProvider } = await import('../apps/master-edit/router.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};
const ALL = Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'company']));
const NOW = new Date('2030-01-10T03:00:00Z');
const TODAY = '2030-01-10';
const RATES = new Map([['S02', { method: '宅急便', cost: 520 }]]);

// ── Company DB ──
const pg = new PGlite();
await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
await pg.query('set role deploy');
const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });
await createMasterEditRoles(pg, {});
const sku = (code, name, kind, taxRate, salesClass, cost, extra = {}) => ({
  code, name, kind, taxRate, taxClass: taxRate === 0.08 ? 'REDUCED_8' : taxRate === 0.1 ? 'STANDARD_10' : null, handling: 'active', salesClass,
  cost: cost == null ? null : { jpy: cost, source: kind === 'set' ? 'set_calc' : 'ne', status: 'COMPLETE' },
  standardPriceJpy: 1000, shippingCode: 'S02', shippingMethod: '宅急便', shippingCostJpy: 520, reorderMonths: 2, ...extra,
});
const singles = [
  sku('a001', 'エプロン ピンク', 'single', 0.1, 3, 100), sku('a002', 'エプロン ブルー', 'single', 0.1, 3, 200), sku('a003', '三角巾', 'single', 0.08, 1, 300),
  sku('a004', 'エプロン 黄', 'single', 0.1, 3, 400), sku('a005', 'エプロン 緑', 'single', 0.1, 3, 500), sku('a006', 'エプロン 赤', 'single', 0.1, 3, 600),
  sku('b001', '先の日の原価', 'single', 0.1, 3, 700), sku('b002', 'CSV が出ている', 'single', 0.1, 3, 800), sku('b003', '先の日の原価のセットに入る', 'single', 0.1, 3, 90),
  sku('d001', '中止した単品', 'single', 0.1, 3, 150, { handling: 'discontinued' }), sku('x001', '例外の SKU', 'exception', 0.1, null, 50),
  ...Array.from({ length: 30 }, (_, i) => sku(`m${String(i).padStart(3, '0')}`, `たくさん ${i}`, 'single', 0.1, 3, 100 + i)),
];
const lr = await runInitialLoad(db, {
  skus: [...singles, sku('s001', 'セット エプロン 2 色', 'set', 0.1, null, 300), sku('s002', 'セット エプロン + 三角巾', 'set', 0.08, null, 500, { taxClass: 'MIXED' }),
    sku('s003', '先の日の原価のセット', 'set', 0.1, null, 90)],
  variationGroups: [],
  setComponents: [
    { parentCode: 's001', childCode: 'a001', qty: 1, source: 'ne' }, { parentCode: 's001', childCode: 'a002', qty: 1, source: 'ne' },
    { parentCode: 's002', childCode: 'a001', qty: 2, source: 'ne' }, { parentCode: 's002', childCode: 'a003', qty: 1, source: 'ne' },
    { parentCode: 's003', childCode: 'b003', qty: 1, source: 'ne' },
  ],
  listings: [], observations: [], physicals: [], compliance: [], workers: [],
  suppliers: [{ code: '0001', name: 'AMC' }, { code: '0002', name: 'ビーフリー' }, { code: '0003', name: '止めた仕入先' }],
  supplierSkus: singles.filter((s) => s.kind === 'single').map((s) => ({ supplierCode: '0001', skuCode: s.code })),
  primarySuppliers: singles.filter((s) => s.kind === 'single').map((s) => ({ skuCode: s.code, supplierCode: '0001' })),
  reorder: { available: true, runId: 'pml_bulk' },
}, { log: quiet, runId: 'load_bulk', now: new Date('2030-01-05T03:00:00Z') });
assert.equal(lr.ok, true, lr.error);
await pg.query("update core.suppliers set active = false where code = '0003'");
await forceNewOpen(db, ALL);
const sid = async (code) => (await pg.query('select sku_id::text as id from core.skus where code = $1', [code])).rows[0].id;
// 先の日の原価 (b001 = 自分・s003 = b003 を含むセット)
for (const code of ['b001', 's003']) {
  const id = await sid(code);
  await pg.query("update core.sku_costs set valid_to = '2030-01-20' where sku_id = $1 and valid_to is null", [id]);
  await pg.query(`insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from, created_by_type, created_by_id)
    values (1, $1, 999, $2, 'COMPLETE', '2030-01-21', 'human', 't')`, [id, code === 's003' ? 'set_calc' : 'ne']);
}
// NE に取り込む CSV が出ている (b002 の原価・売価の列)
{
  const ex = (await pg.query(`insert into ops.ne_csv_exports (kind, col, ne_column, converter_version, encoding, trial, row_count, sha256, file_bytes, compare_run_id, created_by)
    values ('products', 'cost', 'genka_tanka', 'v1', 'utf8', true, 1, repeat('a', 64), decode('00', 'hex'), 'mc_20300101T000000000Z_abcdef', 'x@test') returning export_id`)).rows[0].export_id;
  for (const col of ['cost', 'standard_price_jpy']) {
    await pg.query(`insert into ops.ne_csv_export_rows (export_id, source, code_norm, col, ne_code, target, cell, cdb_version, evidence) values ($1, 'to_ne', 'b002', $2, 'b002', '{"value":"x"}', 'x', 1, '{}')`, [ex, col]);
  }
}

async function as(role, fn) { await pg.query(`set role ${role}`); try { return await fn(); } finally { await pg.query('set role deploy'); } }
const editor = (fn) => as('master_edit', fn);
const editableNow = async () => B.editableOf(await C.readCutoverPhase(db), await OG.readScreenOwnership(db), true);
const inspect = async (codes) => editor(async () => B.bulkInspect(db, { codes }, { now: NOW, editable: await editableNow(), suppliers: [{ code: '0001', name: 'AMC' }, { code: '0002', name: 'ビーフリー' }] }));
const preview = async (codes, field, value, { reason = null, actor = 'naka@test' } = {}) => editor(async () => B.bulkPreview(db, { codes, field, value, reason },
  { now: NOW, editable: await editableNow(), suppliers: [{ code: '0001', name: 'AMC' }, { code: '0002', name: 'ビーフリー' }], actor }));
/** 保存の 1 回分 (切符 + 商品の印)。over = 送る中身を書き換える (差し替えの試験) */
const applyChunk = (pv, part, { actor = 'naka@test', over = {}, nowMs } = {}) => editor(() => B.bulkApplyChunk(db, {
  actor, ticket: pv.ticket, field: pv.field, value: pv.value, reason: pv.reason, total: pv.counts.chg,
  items: part.map((x) => ({ code: x.code, token: x.token, self: x.self, mac: x.mac })), ...over }, { open: true, now: NOW, nowMs }));
/** 確かめ → 保存 (20 件ずつ)。before = 確かめと保存の間にすること */
async function run(codes, field, value, { reason = null, actor = 'naka@test', before = null, p = null } = {}) {
  const pv = p || await preview(codes, field, value, { reason, actor });
  if (before) await before(pv);
  const todo = pv.items.filter((x) => x.verdict === 'chg');
  const results = [];
  for (let i = 0; i < todo.length; i += B.BULK_CHUNK) results.push(...(await applyChunk(pv, todo.slice(i, i + B.BULK_CHUNK), { actor })).results);
  const bulkId = pv.ticket ? B.opKeyOf(B.readTicket(pv.ticket)) : null;   // request_id の元 = 一括の操作の印
  return { pv, results, bulkId, by: Object.fromEntries(results.map((r) => [r.code, r])) };
}
const row = async (code) => (await pg.query(`select s.tax_rate::float8 as tax, s.tax_class, s.handling, s.handling_own, s.standard_price_jpy::int as price, p.sales_class, p.status,
    (select sp.code from core.supplier_skus x join core.suppliers sp on sp.supplier_id = x.supplier_id where x.sku_id = s.sku_id and x.is_primary) as sup,
    (select c.cost_jpy::int from core.sku_costs c where c.sku_id = s.sku_id and c.valid_from <= $2::date and (c.valid_to is null or c.valid_to >= $2::date) order by c.valid_from desc, c.sku_cost_id desc limit 1) as cost
  from core.skus s left join core.products p on p.product_id = s.product_id where s.code = $1`, [code, TODAY])).rows[0];
const nEvents = async () => Number((await pg.query('select count(*)::int as n from events.master_change_events')).rows[0].n);
/** 画面の外で 1 件の保存 (ほかの人) */
async function otherSave(code, values) {
  const token = W.editTokenOf(await W.readCurrent(db, await sid(code), TODAY));
  return editor(() => W.saveSku(db, { actor: 'other@test', requestId: crypto.randomUUID(), code, reason: 'ほかの人', seen: { token }, values }, { open: true, now: NOW, shippingRates: RATES }));
}

console.log('判定 (対象外・保存できない)');

await ta('[1] 項目の板の判定: 例外は常に対象外・セット・中止・無いコード / 先の日の原価 (自分・含むセット)・CSV は最初から保存できない (M5)', async () => {
  const r = await inspect(['a001', 's001', 'd001', 'x001', 'b001', 'b002', 'b003', 'nope-1']);
  const v = (code, f) => r.items.find((x) => x.code === code || (code === 'nope-1' && x.code === 'nope-1')).fields[f].verdict;
  for (const f of Object.keys(B.BULK_FIELDS)) {
    assert.equal(v('a001', f), 'ok', `a001 ${f}`);
    assert.equal(v('x001', f), 'out', `例外 ${f}`);
    assert.equal(v('nope-1', f), 'out', `無い ${f}`);
  }
  assert.deepEqual(Object.fromEntries(Object.keys(B.BULK_FIELDS).map((f) => [f, v('s001', f)])),
    { cost: 'out', standard_price: 'ok', handling: 'ok', sales_class: 'out', tax_rate: 'out', primary_supplier: 'out' });
  assert.deepEqual(Object.fromEntries(Object.keys(B.BULK_FIELDS).map((f) => [f, v('d001', f)])),
    { cost: 'out', standard_price: 'out', handling: 'ok', sales_class: 'out', tax_rate: 'out', primary_supplier: 'out' });
  assert.equal(v('b001', 'cost'), 'block'); assert.equal(v('b001', 'standard_price'), 'ok');
  assert.equal(v('b003', 'cost'), 'block'); assert.match(r.items.find((x) => x.code === 'b003').fields.cost.why, /含むセット s003/);
  assert.equal(v('b002', 'cost'), 'block'); assert.equal(v('b002', 'standard_price'), 'block'); assert.equal(v('b002', 'tax_rate'), 'ok');
  assert.equal(r.items.find((x) => x.code === 'b001').fields.cost.reason, 'cost_future');
  assert.equal(r.items.find((x) => x.code === 'x001').fields.handling.reason, 'exception_sku');
  // 今の値 (値の欄の「今の値」)
  assert.equal(r.items.find((x) => x.code === 'a001').fields.cost.now, 100);
  assert.equal(r.items.find((x) => x.code === 'a001').fields.primary_supplier.now, '0001');
  assert.deepEqual(r.costReasons, B.COST_REASONS);
});

await ta('[2] 200 件の上限 (API も 413)・重なりは 1 つ・20 件ずつ・形の検査', async () => {
  const many = Array.from({ length: 201 }, (_, i) => `c${i}`);
  await assert.rejects(() => inspect(many), (e) => e.status === 413 && e.reason === 'too_many' && /200 件まで/.test(e.message) && /1 件減らして/.test(e.message));
  await assert.rejects(() => preview(many, 'standard_price', 1000), (e) => e.status === 413);
  assert.equal(B.parseCodes(['a001', 'A001', ' a001 ', 'a002']).length, 2);
  assert.equal(B.parseCodes(Array.from({ length: 200 }, (_, i) => `c${i}`)).length, 200);
  const p21 = await preview(Array.from({ length: 21 }, (_, i) => `m${String(i).padStart(3, '0')}`), 'standard_price', 1777);
  assert.equal(p21.counts.chg, 21);
  await assert.rejects(() => applyChunk(p21, p21.items), (e) => e.status === 400 && /20 件まで/.test(e.message));
  assert.throws(() => B.parseFieldValue('name', 'x'), /まとめて変えられない項目/);
  assert.throws(() => B.parseFieldValue('cost', '0'), /原価は 1/);
  assert.equal(B.parseFieldValue('standard_price', '１，２００'), 1200);
  assert.equal(B.parseFieldValue('tax_rate', '8'), 0.08);
  assert.equal(B.parseFieldValue('primary_supplier', '2'), '0002');
});

console.log('保存 (全部の項目)');

await ta('[3] 売価: 単品とセットを同じ値に・変わらない商品は送らない・記録 (人・request_id = 一括の番号から・理由「一括 ◯ 件」)', async () => {
  await otherSave('a004', { standard_price: 1500 });
  const r = await run(['a004', 'a005', 's001', 'x001'], 'standard_price', 1500);
  assert.deepEqual(r.pv.items.map((x) => [x.code, x.verdict]), [['a004', 'same'], ['a005', 'chg'], ['s001', 'chg'], ['x001', 'out']]);
  assert.deepEqual(r.pv.counts, { chg: 2, same: 1, out: 1, block: 0, linked: 0 });
  assert.ok(r.pv.items.find((x) => x.code === 'a005').profit.after > r.pv.items.find((x) => x.code === 'a005').profit.before, '利益の前後');
  assert.deepEqual(r.results.map((x) => [x.code, x.ok]), [['a005', true], ['s001', true]]);
  assert.equal((await row('a005')).price, 1500); assert.equal((await row('s001')).price, 1500);
  const ev = (await pg.query(`select actor_id, request_id, reason_text, source_system from events.master_change_events where entity_type = 'sku' and attribute = 'standard_price_jpy' and entity_id = $1`, [await sid('a005')])).rows.at(-1);
  assert.deepEqual([ev.actor_id, ev.request_id, ev.reason_text, ev.source_system], ['naka@test', B.bulkRequestId(r.bulkId, 'a005', 0), '一括 2 件', 'portal_master_edit']);
});

await ta('[3] 原価: 理由が要る (無い = 400)・今日から・含むセットの原価も計算し直す (確かめの予定 = 保存の後の本当の値)', async () => {
  await assert.rejects(() => preview(['a005'], 'cost', 550), (e) => e.status === 400 && /理由/.test(e.message));
  const p = await preview(['a005', 'a006'], 'cost', 550, { reason: 'メーカーからの値上げ通知' });
  assert.deepEqual(p.items.map((x) => [x.code, x.verdict, x.before, x.after]), [['a005', 'chg', 500, 550], ['a006', 'chg', 600, 550]]);
  const r = await run(['a005', 'a006'], 'cost', 550, { p });
  assert.ok(r.results.every((x) => x.ok), JSON.stringify(r.results));
  assert.equal((await row('a005')).cost, 550); assert.equal((await row('a006')).cost, 550);
  const c = (await pg.query(`select valid_from::text as f, reason, cost_source from core.sku_costs where sku_id = $1 and valid_to is null`, [await sid('a005')])).rows[0];
  assert.deepEqual(c, { f: TODAY, reason: 'メーカーからの値上げ通知', cost_source: 'manual' });
  // 含むセット: a001 の原価を変える = s001 (a001 + a002) と s002 (a001×2 + a003) の合計
  const p2 = await preview(['a001'], 'cost', 120, { reason: '入力の誤りを直す' });
  assert.deepEqual(p2.linked.map((l) => [l.code, l.col, l.before, l.after]).sort(), [['s001', 'cost', 300, 320], ['s002', 'cost', 500, 540]]);
  const r2 = await run(['a001'], 'cost', 120, { p: p2 });
  assert.deepEqual(r2.results[0].derived.map((d) => [d.code, d.col, d.to]).sort(), [['s001', 'cost', 320], ['s002', 'cost', 540]]);
  assert.equal((await row('s001')).cost, 320); assert.equal((await row('s002')).cost, 540);
});

await ta('[3] 取扱: 中止は理由が必須・単品を中止すると含むセットも中止 (確かめに出る)・中止した商品は取扱を戻すだけ・戻す', async () => {
  await assert.rejects(() => preview(['a006'], 'handling', 'discontinued'), (e) => e.status === 400 && /中止にする理由/.test(e.message));
  const p = await preview(['a006', 'd001'], 'handling', 'discontinued', { reason: 'メーカーの廃番' });
  assert.deepEqual(p.items.map((x) => [x.code, x.verdict]), [['a006', 'chg'], ['d001', 'same']]);
  const r = await run(['a006'], 'handling', 'discontinued', { p });
  assert.ok(r.results[0].ok);
  const a = await row('a006'); assert.deepEqual([a.handling, a.status], ['discontinued', 'discontinued']);
  const ev = (await pg.query(`select reason_text from events.master_change_events where entity_type = 'sku' and attribute = 'handling' and entity_id = $1 order by event_id desc limit 1`, [await sid('a006')])).rows[0];
  assert.equal(ev.reason_text, 'メーカーの廃番 · 一括 1 件');
  // 中止した商品 = 売価は対象外・取扱は戻せる
  const p2 = await preview(['a006'], 'standard_price', 999);
  assert.equal(p2.items[0].verdict, 'out'); assert.match(p2.items[0].why, /取扱を中止した商品/);
  const r2 = await run(['a006'], 'handling', 'active');
  assert.ok(r2.results[0].ok); assert.equal((await row('a006')).handling, 'active');
  // 構成品を中止 = セットも中止 (確かめの予定と保存の後が同じ)
  const p3 = await preview(['a002'], 'handling', 'discontinued', { reason: '試験' });
  assert.deepEqual(p3.linked.map((l) => [l.code, l.col, l.before, l.after]), [['s001', 'handling', 'active', 'discontinued']]);
  const r3 = await run(['a002'], 'handling', 'discontinued', { p: p3 });
  assert.deepEqual(r3.results[0].derived.map((d) => [d.code, d.col, d.to]), [['s001', 'handling', 'discontinued']]);
  assert.equal((await row('s001')).handling, 'discontinued');
  // 戻す: 単品は取扱中・セットは自身の取扱が決まっていない = 中止のまま (メモ)。セット自身を取扱中にすると戻る
  await run(['a002'], 'handling', 'active');
  assert.equal((await row('s001')).handling, 'discontinued');
  const p4 = await preview(['s001'], 'handling', 'active');
  assert.equal(p4.items[0].verdict, 'chg'); assert.equal(p4.items[0].after, 'active');
  await run(['s001'], 'handling', 'active', { p: p4 });
  const s = await row('s001'); assert.deepEqual([s.handling, s.handling_own], ['active', 'active']);
});

await ta('[3] 売上分類・仕入先: 単品だけ・選べない仕入先は 400・変わる', async () => {
  const r = await run(['a004', 'a005', 's001'], 'sales_class', 2);
  assert.deepEqual(r.pv.items.map((x) => [x.code, x.verdict]), [['a004', 'chg'], ['a005', 'chg'], ['s001', 'out']]);
  assert.ok(r.results.every((x) => x.ok)); assert.equal((await row('a004')).sales_class, 2);
  await assert.rejects(() => preview(['a004'], 'primary_supplier', '0003'), (e) => e.status === 400 && /選べません/.test(e.message));
  const r2 = await run(['a004', 'a005'], 'primary_supplier', '2');
  assert.ok(r2.results.every((x) => x.ok), JSON.stringify(r2.results));
  assert.equal((await row('a004')).sup, '0002'); assert.equal((await row('a005')).sup, '0002');
});

console.log('やり直し・二重押し・部分の成功');

await ta('[6] 税率: 同じセットの構成品を続けて変える = 2 件目は version_conflict → この一括の分だけ = 読み直して 1 回だけやり直す・確かめのセットの値 = 保存の後', async () => {
  // s001 = a001 (10%) + a002 (10%)・s002 = a001 (10%) ×2 + a003 (8%) = MIXED 8%
  const p = await preview(['a001', 'a002'], 'tax_rate', 0.08);
  assert.deepEqual(p.linked.map((l) => [l.code, l.after.rate, l.after.class]).sort(), [['s001', 0.08, 'REDUCED_8'], ['s002', 0.08, 'REDUCED_8']]);
  const r = await run(['a001', 'a002'], 'tax_rate', 0.08, { p });
  assert.deepEqual(r.results.map((x) => [x.code, x.ok, !!x.retried]), [['a001', true, false], ['a002', true, true]]);
  const s1 = await row('s001'); const s2 = await row('s002');
  assert.deepEqual([s1.tax, s1.tax_class, s2.tax, s2.tax_class], [0.08, 'REDUCED_8', 0.08, 'REDUCED_8']);
  // やり直した分の記録: 1 回目 = failed (version_conflict)・2 回目 = done
  const st = async (rid) => (await pg.query('select status, error ->> \'reason\' as why from ops.master_edit_requests where request_id = $1', [rid])).rows[0];
  assert.deepEqual(await st(B.bulkRequestId(r.bulkId, 'a002', 0)), { status: 'failed', why: 'version_conflict' });
  assert.deepEqual(await st(B.bulkRequestId(r.bulkId, 'a002', 1)), { status: 'done', why: null });
  // 実際に変わったセット (サーバーの答え・M3)
  assert.deepEqual(r.results.flatMap((x) => x.derived).map((d) => `${d.code}:${d.to.class}`).sort(), ['s001:MIXED', 's001:REDUCED_8', 's002:REDUCED_8']);
});

await ta('[5] 二重押し: 同じ一括の番号でもう一度送る = 前の結果 (やり直した分も)・変更の記録は増えない', async () => {
  const p = await preview(['a001', 'a002'], 'tax_rate', 0.1);
  const r1 = await run(['a001', 'a002'], 'tax_rate', 0.1, { p });
  assert.deepEqual(r1.results.map((x) => [x.ok, !!x.retried]), [[true, false], [true, true]]);
  const n = await nEvents();
  const r2 = await run(['a001', 'a002'], 'tax_rate', 0.1, { p });
  assert.deepEqual(r2.results.map((x) => [x.ok, x.replayed, !!x.retried]), [[true, true, false], [true, true, true]]);
  assert.equal(await nEvents(), n, '二重に書かない');
  assert.deepEqual(r2.results.map((x) => x.changed.map((c) => c.field)), r1.results.map((x) => x.changed.map((c) => c.field)));
});

await ta('[4][6] 部分の成功: 確かめの後にほかの人が直した商品 = だめ (やり直さない・latest)・ほかの商品は変えたまま', async () => {
  const r = await run(['a004', 'a005', 'm000'], 'standard_price', 2222, { before: async () => { await otherSave('a005', { name: 'ほかの人が名前を直した' }); } });
  assert.deepEqual(r.results.map((x) => [x.code, x.ok]), [['a004', true], ['a005', false], ['m000', true]]);
  assert.equal(r.by.a005.error.reason, 'version_conflict'); assert.equal(r.by.a005.error.group, 'latest');
  assert.equal((await row('a004')).price, 2222); assert.equal((await row('a005')).price, 1500); assert.equal((await row('m000')).price, 2222);
});

await ta('[6] 同じセットの構成品でも、その間にほかの人がセットを直していれば「だめ」(この一括の分だけのときだけやり直す)', async () => {
  const r = await run(['a001', 'a002'], 'tax_rate', 0.08, { before: async () => { await otherSave('s001', { standard_price: 3333 }); } });
  assert.deepEqual(r.results.map((x) => [x.code, x.ok]), [['a001', false], ['a002', false]]);
  assert.ok(r.results.every((x) => x.error.reason === 'version_conflict'));
});

await ta('[7] 一緒に変わるセット: 一部の構成品だけ = 混ざった税率 (MIXED) を確かめに出す = 保存の後と同じ / 構成品の失敗 = 本当の値はサーバーの答え (M3)', async () => {
  // 今: a001 = 10%・a002 = 10% → a001 だけ 8% = s001 は MIXED 8%
  const p = await preview(['a001'], 'tax_rate', 0.08);
  const l = p.linked.find((x) => x.code === 's001');
  assert.deepEqual([l.after.rate, l.after.class], [0.08, 'MIXED']); assert.match(l.why, /混ざる/);
  const r = await run(['a001'], 'tax_rate', 0.08, { p });
  const s1 = await row('s001'); assert.deepEqual([s1.tax, s1.tax_class], [l.after.rate, l.after.class]);
  // a002 と a004 を 8% に (確かめの予定 = s001 は REDUCED_8) → a002 がほかの人の変更で失敗 = s001 は MIXED のまま (答えに出ない・予定と違う)
  const p2 = await preview(['a002', 'a004'], 'tax_rate', 0.08);
  assert.deepEqual(p2.linked.map((x) => [x.code, x.after.class]), [['s001', 'REDUCED_8']]);
  const r2 = await run(['a002', 'a004'], 'tax_rate', 0.08, { p: p2, before: async () => { await otherSave('a002', { name: '別の名前' }); } });
  assert.deepEqual(r2.results.map((x) => [x.code, x.ok]), [['a002', false], ['a004', true]]);
  assert.deepEqual(r2.results.flatMap((x) => x.derived || []), [], '実際に変わったセットは無い');
  assert.equal((await row('s001')).tax_class, 'MIXED');
  void r;
});

await ta('[6] 変更の記録に残らない変更 (登録の状態) も「この商品そのもの」の印で見る = やり直さない', async () => {
  // 今: a001 = 8%・a003 = 8% (どちらも s002 に入る)。a003 は a001 の保存で s002 の版が上がる = やり直しの道。その前に a003 の登録の状態が変わった
  const r = await run(['a001', 'a003'], 'tax_rate', 0.1, { before: async () => {
    await pg.query('begin');
    await pg.query("select set_config('ops.registration_protocol', '1', true)");
    await pg.query('update ops.master_registrations set state_changed_at = now() where sku_id = $1', [await sid('a003')]);
    await pg.query('commit');
  } });
  assert.deepEqual(r.results.map((x) => [x.code, x.ok]), [['a001', true], ['a003', false]]);
  assert.equal(r.by.a003.error.reason, 'version_conflict');
});

await ta('[8] サーバーでも対象外を止める (切符を作れても書かない)・印の無い送り方・切符の無い送り方', async () => {
  // 確かめでは「変わる」にならない商品 (例外・セットの原価・中止の商品の売価) の切符を直接作って送る (画面からは来ない形)
  const forged = async (code, field, value) => {
    const cur = await W.readCurrent(db, await sid(code), TODAY);
    const it = { code, token: W.editTokenOf(cur), self: B.bulkSelfToken(cur) };
    const t = B.issueTicket({ actor: 'naka@test', field, value, reason: 'r', seen: '0', items: [it] });
    return (await editor(() => B.bulkApplyChunk(db, { actor: 'naka@test', ticket: t.ticket, items: [{ ...it, mac: t.macs.get(code) }] }, { open: true, now: NOW }))).results[0];
  };
  const x = await forged('x001', 'standard_price', 4444);
  assert.deepEqual([x.ok, x.error.reason], [false, 'exception_sku']);
  const s1 = await forged('s001', 'cost', 4444);
  assert.deepEqual([s1.ok, s1.error.reason], [false, 'out']);
  const d = await forged('d001', 'standard_price', 4444);
  assert.deepEqual([d.ok, d.error.reason], [false, 'out']);
  assert.notEqual((await row('d001')).price, 4444);
  const p = await preview(['a004'], 'standard_price', 4444);
  await assert.rejects(() => editor(() => B.bulkApplyChunk(db, { actor: 'naka@test', ticket: p.ticket, items: [{ code: 'a004' }] }, { open: true })), /編集の印が無い/);
  await assert.rejects(() => editor(() => B.bulkApplyChunk(db, { actor: 'naka@test', items: [] }, { open: true })), (e) => e.status === 409 && e.reason === 'ticket_invalid');
});

await ta('[11] 確かめの切符 (Codex R1 High 1): 項目・値・理由・件数の差し替え・ほかの切符の商品・書き換えた切符・ほかの人・期限切れ = 断る (何も書かない)', async () => {
  const p = await preview(['a004', 'm003'], 'standard_price', 4321);
  const n0 = await nEvents();
  const no = async (over, re, opts = {}) => assert.rejects(() => applyChunk(p, p.items.filter((x) => x.verdict === 'chg'), { over, ...opts }), (e) => re.test(`${e.status} ${e.reason}`), JSON.stringify(over).slice(0, 80));
  await no({ field: 'cost' }, /400 ticket_mismatch/);
  await no({ value: 1 }, /400 ticket_mismatch/);
  await no({ reason: '別の理由' }, /400 ticket_mismatch/);
  await no({ total: 1 }, /400 ticket_mismatch/);
  // 別の確かめの商品の印を混ぜる (印は切符の番号に結び付く)
  const other = await preview(['m004'], 'standard_price', 4321);
  await no({ items: [...p.items, ...other.items].map((x) => ({ code: x.code, token: x.token, self: x.self, mac: x.mac })) }, /400 ticket_mismatch/);
  // 確かめで「変わる」でない商品 (印が無い)
  await no({ items: [{ code: 'm005', token: p.items[0].token, self: p.items[0].self, mac: p.items[0].mac }] }, /400 ticket_mismatch/);
  // 切符の中身を書き換える (原価 1 円) = 署名が合わない
  const [body, sig] = p.ticket.split('.');
  const t = JSON.parse(Buffer.from(body, 'base64url').toString());
  const evil = `${Buffer.from(JSON.stringify({ ...t, field: 'cost', value: 1 })).toString('base64url')}.${sig}`;
  await no({ ticket: evil, field: undefined, value: undefined, reason: undefined, total: undefined }, /409 ticket_invalid/);
  // ほかの人 / 期限切れ (30 分)
  await no({}, /403 ticket_actor/, { actor: 'other@test' });
  await no({}, /409 ticket_expired/, { nowMs: Date.now() + B.TICKET_TTL_MS + 1000 });
  assert.equal(await nEvents(), n0, '何も書いていない');
  assert.notEqual((await row('a004')).price, 4321);
  // そのままなら通る
  const ok = await applyChunk(p, p.items);
  assert.ok(ok.results.every((r) => r.ok)); assert.equal((await row('a004')).price, 4321);
});

await ta('[12] 200 件の上限 (Codex R1 M2): 切符の件数より多くは送れない (同じ切符で送り直しても前の結果だけ)・201 件の切符は読めない・理由の件数 = 本当の数', async () => {
  const p = await preview(['m006'], 'standard_price', 2468);
  const r1 = await applyChunk(p, p.items);
  assert.ok(r1.results[0].ok);
  const n0 = await nEvents();
  for (let i = 0; i < 11; i++) assert.equal((await applyChunk(p, p.items)).results[0].replayed, true);
  assert.equal(await nEvents(), n0, '同じ切符を 11 回送っても書くのは 1 回');
  // 切符に無い 20 件は送れない
  await assert.rejects(() => applyChunk(p, Array.from({ length: 20 }, (_, i) => ({ code: `m0${10 + i}`, token: p.items[0].token, self: p.items[0].self, mac: p.items[0].mac }))), (e) => e.reason === 'ticket_mismatch');
  const big = B.issueTicket({ actor: 'naka@test', field: 'standard_price', value: 1, reason: null, seen: '0', items: Array.from({ length: 201 }, (_, i) => ({ code: `c${i}`, token: 'a'.repeat(64), self: 'b'.repeat(64) })) });
  assert.throws(() => B.readTicket(big.ticket), (e) => e.reason === 'ticket_invalid');
  const ev = (await pg.query('select reason_text from events.master_change_events where request_id = $1 limit 1', [B.bulkRequestId(B.opKeyOf(B.readTicket(p.ticket)), 'm006', 0)])).rows[0];
  assert.equal(ev.reason_text, '一括 1 件');
});

await ta('[13] やり直しの request_id (Codex R1 M3): 操作の印 (切符・人・項目・値・理由) から決まる = 中身の違う操作は前の結果を受け取らない・違う商品の記録は request_id_reused', async () => {
  const t = B.readTicket((await preview(['m007'], 'standard_price', 1357)).ticket);
  const k = B.opKeyOf(t);
  for (const over of [{ value: 1358 }, { field: 'cost' }, { reason: 'x' }, { id: crypto.randomUUID() }, { actor: 'other@test' }]) {
    assert.notEqual(B.bulkRequestId(B.opKeyOf({ ...t, ...over }), 'm007', 1), B.bulkRequestId(k, 'm007', 1), JSON.stringify(over));
  }
  // 同じ商品を 2 回 (違う値) まとめて変える = 2 回目は 2 回目の値を本当に書く (前の結果を返さない)
  await run(['m007'], 'standard_price', 1357);
  const r = await run(['m007'], 'standard_price', 1359);
  assert.deepEqual([r.results[0].ok, !!r.results[0].replayed], [true, false]); assert.equal((await row('m007')).price, 1359);
  // やり直しの request_id の記録が違う商品 (target_code) = 前の結果として返さない
  const p = await preview(['m008'], 'standard_price', 1111);
  const opk = B.opKeyOf(B.readTicket(p.ticket));
  await pg.query(`insert into ops.master_edit_requests (request_id, company_id, operation, target_code, sku_id, actor_id, payload_hash, status, error, started_at)
    values ($1, 1, 'sku_edit', 'm009', $2, 'naka@test', repeat('a', 64), 'failed', '{"status":409,"reason":"x"}'::jsonb, now())`, [B.bulkRequestId(opk, 'm008', 1), await sid('m009')]);
  const x = (await applyChunk(p, p.items)).results[0];
  assert.deepEqual([x.ok, x.error && x.error.reason], [false, 'request_id_reused']);
  assert.notEqual((await row('m008')).price, 1111);
});

await ta('[14] 商品の行が無い単品の売上分類 (Codex R1 M4): DB が単品に商品の行を必須にしている (CHECK と commit の trigger の 2 重)・判定は本物の product_id で「保存できない」', async () => {
  // 本物の DB では起きない: 単品の product_id を空にする = CHECK で断る / CHECK を外しても commit の trigger で断る (試験の DB の中だけ外して戻す)
  await assert.rejects(() => pg.query("update core.skus set product_id = null where code = 'm010'"), /ck_skus_single_has_product/);
  await pg.query('alter table core.skus drop constraint ck_skus_single_has_product');
  try {
    await assert.rejects(() => pg.query("update core.skus set product_id = null where code = 'm010'"), /sku_kind_shape/);
  } finally {
    await pg.query("alter table core.skus add constraint ck_skus_single_has_product check (sku_kind <> 'single' or product_id is not null)");
  }
  // 保存 (apply) の判定は確かめと同じ本物の値 = 商品の行が無ければ「保存できない」(前の仮の product_id 'x' はやめた)
  const j = B.judgeBulk({ found: true, cur: { sku_kind: 'single', handling: 'active', registration: null, product_id: null } }, 'sales_class', undefined);
  assert.deepEqual([j.verdict, j.reason], ['block', 'no_product']);
  assert.equal(B.judgeBulk({ found: true, cur: { sku_kind: 'single', handling: 'active', registration: null, product_id: '5' } }, 'sales_class', undefined).verdict, 'ok');
  // 1 件の保存も、商品の行に書けなかったら成功にしない (守りの文があること = 下の「守りを外すと赤」で見る)
  const src = (await import('node:fs')).readFileSync(new URL('../lib/master-write.mjs', import.meta.url), 'utf8');
  assert.match(src, /if \(!cur\.product_id\) throw bad\('商品の行が無いので売上分類を持てません/);
  assert.match(src, /if \(hit !== 1\) throw new MasterWriteError\(409, 'no_product'/);
});

await ta('[4] 保存できない (先の日の原価・CSV) は送らない・直し方の組 (M7)・保存を開いていない = 全部だめ (later)', async () => {
  const p = await preview(['b001', 'b002', 'b003', 'm001'], 'cost', 777, { reason: 'x' });
  assert.deepEqual(p.items.map((x) => [x.code, x.verdict, x.group]), [['b001', 'block', 'one'], ['b002', 'block', 'csv'], ['b003', 'block', 'one'], ['m001', 'chg', null]]);
  const r = await run(['m001'], 'cost', 777, { p });
  assert.deepEqual(r.results.map((x) => [x.code, x.ok]), [['m001', true]]);
  // 保存を開いていない (MASTER_EDIT_OPEN が無い) = 409 before_cutover・何も書かない
  const p2 = await preview(['m002'], 'standard_price', 1234);
  const c = await editor(() => B.bulkApplyChunk(db, { actor: 'naka@test', ticket: p2.ticket,
    items: [{ code: 'm002', token: p2.items[0].token, self: p2.items[0].self, mac: p2.items[0].mac }] }, { open: false, now: NOW }));
  assert.deepEqual([c.results[0].ok, c.results[0].error.reason, c.results[0].error.group], [false, 'before_cutover', 'later']);
  assert.notEqual((await row('m002')).price, 1234);
  assert.equal(B.FIX_GROUPS.one.retry, false); assert.equal(B.FIX_GROUPS.latest.retry, true);
});

await ta('[3] 30 件 (20 件ずつ 2 回) をまとめて = 全部変わる・記録の理由「一括 30 件」', async () => {
  const codes = Array.from({ length: 30 }, (_, i) => `m${String(i).padStart(3, '0')}`);
  const r = await run(codes, 'sales_class', 4);
  assert.equal(r.results.length, 30); assert.ok(r.results.every((x) => x.ok));
  assert.equal((await row('m029')).sales_class, 4);
  const ev = (await pg.query(`select reason_text from events.master_change_events where entity_type = 'product' and attribute = 'sales_class' and request_id = $1`, [B.bulkRequestId(r.bulkId, 'm029', 0)])).rows[0];
  assert.equal(ev.reason_text, '一括 30 件');
});

console.log('\n画面 (router)');

process.env.COMPANY_DB_URL = 'postgres://owner@localhost:5432/test';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost:5432/test';
process.env.MASTER_EDITORS = 'naka@test';
process.env.MASTER_EDIT_OPEN = '1';
let chain = Promise.resolve();
__setPgClientFactory(async (url) => {
  let release; const prev = chain; chain = new Promise((r) => { release = r; }); await prev;
  await pg.query(`set role ${/master_edit@/.test(url) ? 'master_edit' : 'deploy'}`);
  return { query: (t, p) => pg.query(t, p), end: async () => { await pg.query('set role deploy'); release(); }, on: () => {} };
});
__setClock(() => NOW.getTime());
__setShippingRatesProvider(async () => RATES);
const app = express();
app.set('view engine', 'ejs');
app.use((req, res, next) => {
  const s = req.headers['x-test-session'];
  req.session = s === 'editor' ? { authenticated: true, email: 'Naka@Test', displayName: '中原', role: 'user', allowedApps: ['master-edit'] }
    : s === 'viewer' ? { authenticated: true, email: 'viewer@test', role: 'user', allowedApps: ['master-edit'] } : null;
  next();
});
app.use('/apps/master-edit', router);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const ORIGIN = `http://127.0.0.1:${server.address().port}`;
const BASE = `${ORIGIN}/apps/master-edit`;
async function call(method, url, { body, session = 'editor', origin = true } = {}) {
  const headers = { Accept: 'application/json', 'x-test-session': session };
  if (body !== undefined) headers['Content-Type'] = 'application/json';
  if (origin) headers.Origin = ORIGIN;
  const r = await fetch(BASE + url, { method, headers, body: body === undefined ? undefined : JSON.stringify(body), redirect: 'manual' });
  const text = await r.text();
  let j = null; try { j = JSON.parse(text); } catch { /* HTML */ }
  return { status: r.status, j, text };
}

await ta('[9] 権限: 名簿にない人は確かめも保存も 403・Origin が無い = 403・保存は書き込み用の接続で', async () => {
  for (const path of ['/api/bulk/inspect', '/api/bulk/preview', '/api/bulk/apply']) {
    const r = await call('POST', path, { body: { codes: ['a004'] }, session: 'viewer' });
    assert.equal(r.status, 403, path); assert.equal(r.j.reason, 'not_editor');
    const o = await call('POST', path, { body: { codes: ['a004'] }, origin: false });
    assert.equal(o.status, 403, `${path} Origin`);
  }
  const i = await call('POST', '/api/bulk/inspect', { body: { codes: ['a004', 'x001'] } });
  assert.equal(i.status, 200, i.text); assert.equal(i.j.items.length, 2);
  assert.ok(i.j.suppliers.some((s) => s.code === '0002') && !i.j.suppliers.some((s) => s.code === '0003'), '選べる仕入先 = 取引中だけ');
  const big = await call('POST', '/api/bulk/inspect', { body: { codes: Array.from({ length: 201 }, (_, k) => `z${k}`) } });
  assert.equal(big.status, 413); assert.equal(big.j.reason, 'too_many');
  // HTTP で確かめ → 保存
  const p = await call('POST', '/api/bulk/preview', { body: { codes: ['a004', 'x001'], field: 'standard_price', value: '5,555' } });
  assert.equal(p.status, 200, p.text); assert.equal(p.j.value, 5555);
  const it = p.j.items.find((x) => x.code === 'a004');
  assert.equal(B.readTicket(p.j.ticket).actor, 'naka@test', '切符 = ログインの人');
  const bulkId = B.opKeyOf(B.readTicket(p.j.ticket));
  // 項目の差し替えは HTTP でも断る
  const sw = await call('POST', '/api/bulk/apply', { body: { ticket: p.j.ticket, field: 'cost', value: 1, items: [{ code: 'a004', token: it.token, self: it.self, mac: it.mac }] } });
  assert.equal(sw.status, 400); assert.equal(sw.j.reason, 'ticket_mismatch');
  const a = await call('POST', '/api/bulk/apply', { body: { ticket: p.j.ticket, field: 'standard_price', value: 5555, total: 1, items: [{ code: 'a004', token: it.token, self: it.self, mac: it.mac }] } });
  assert.equal(a.status, 200, a.text); assert.equal(a.j.results[0].ok, true);
  assert.equal((await row('a004')).price, 5555);
  const ev = (await pg.query(`select actor_id, db_user from events.master_change_events where request_id = $1 limit 1`, [B.bulkRequestId(bulkId, 'a004', 0)])).rows[0];
  assert.deepEqual(ev, { actor_id: 'naka@test', db_user: 'master_edit' });
  // 書き込み用の接続が無い = 保存は 503 (確かめは読むだけ = 通る)
  const keep = process.env.COMPANY_DB_MASTER_EDIT_URL; delete process.env.COMPANY_DB_MASTER_EDIT_URL;
  try {
    const n = await call('POST', '/api/bulk/apply', { body: { ticket: p.j.ticket, items: [{ code: 'a004', token: it.token, self: it.self, mac: it.mac }] } });
    assert.equal(n.status, 503); assert.equal(n.j.reason, 'no_write_role');
  } finally { process.env.COMPANY_DB_MASTER_EDIT_URL = keep; }
});

await ta('[10] 一覧の描画 (本物の router): 左のチェック・帯と引き出し・設定の JSON (件数・絞り込みの鍵・コードの URL)・画面の JS と CSS・名簿にない人 / 保存を開いていない = 出さない', async () => {
  const r = await call('GET', '/?q=' + encodeURIComponent('エプロン'));
  assert.equal(r.status, 200);
  assert.match(r.text, /id="ck-page" aria-label="このページの \d+ 件を全部選ぶ"/);
  assert.match(r.text, /<input type="checkbox" class="ck rowck" data-code="a001" data-kind="single" data-state="available" aria-label="a001 を選ぶ">/);
  assert.match(r.text, /data-code="s001" data-kind="set"/);
  for (const id of ['bk-selbar', 'bk-drawer', 'bk-go', 'bk-sel-all', 'bk-prev', 'bk-steps']) assert.ok(r.text.includes(`id="${id}"`), id);
  const m = /<script type="application\/json" id="me-bulk">([\s\S]*?)<\/script>/.exec(r.text);
  const cfg = JSON.parse(m[1]);
  assert.equal(cfg.max, 200); assert.equal(cfg.chunk, 20);
  assert.equal(cfg.codesUrl, 'api/codes?q=' + encodeURIComponent('エプロン'));
  assert.equal(cfg.fkey, JSON.stringify([['q', 'エプロン']]));
  assert.equal(cfg.total, (r.text.match(/class="ck rowck"/g) || []).length);
  assert.deepEqual(cfg.costReasons, B.COST_REASONS);
  assert.equal(cfg.groups.one.retry, false);
  // 並びを変えても絞り込みの鍵は同じ (並び・ページ送りでは選んだものはそのまま)
  const r2 = await call('GET', '/?q=' + encodeURIComponent('エプロン') + '&sort=profit_asc');
  assert.equal(JSON.parse(/id="me-bulk">([\s\S]*?)<\/script>/.exec(r2.text)[1]).fkey, cfg.fkey);
  // 画面の JS・CSS (JS は文法として読める・EJS のタグが残っていない)
  const js = /<script src="(\/apps\/master-edit\/public\/me-bulk\.js\?v=[0-9a-f]{12})" defer><\/script>/.exec(r.text);
  assert.ok(js, 'me-bulk.js を読んでいない');
  const src = await (await fetch(ORIGIN + js[1], { headers: { 'x-test-session': 'editor' } })).text();
  new vm.Script(src); assert.ok(!/<%|%>/.test(src));
  assert.match(r.text, /href="\/apps\/master-edit\/public\/me-bulk\.css\?v=[0-9a-f]{12}"/);
  const css = await fetch(ORIGIN + '/apps/master-edit/public/me-bulk.css', { headers: { 'x-test-session': 'editor' } });
  assert.equal(css.status, 200);
  // 空の一覧の colspan (列が 1 つ多い。この人は発注アプリの利用権が無い = 注文残の列なし = 15 + 1)
  const e = await call('GET', '/?q=' + encodeURIComponent('ないないない'));
  assert.equal(Number(/<td colspan="(\d+)" class="muted"/.exec(e.text)[1]), (e.text.match(/<th scope="col"/g) || []).length, '列の数 (チェックの列も入れて)');
  assert.ok(e.text.includes('class="c-chk"'));
  // 名簿にない人 = 左のチェックも帯も出さない
  const v = await call('GET', '/', { session: 'viewer' });
  assert.equal(v.status, 200);
  assert.ok(!v.text.includes('rowck') && !v.text.includes('id="bk-selbar"') && !v.text.includes('me-bulk.js'), '名簿にない人');
  assert.match(v.text, /<a class="rowlink" href="sku\/a001"/);
  const ve = await call('GET', '/?q=' + encodeURIComponent('ないないない'), { session: 'viewer' });
  assert.equal(Number(/<td colspan="(\d+)" class="muted"/.exec(ve.text)[1]), (ve.text.match(/<th scope="col"/g) || []).length);
  assert.ok(!ve.text.includes('class="c-chk"'));
  // 保存を開いていない = 出さない
  process.env.MASTER_EDIT_OPEN = '0';
  try {
    const c = await call('GET', '/');
    assert.ok(!c.text.includes('rowck') && c.text.includes('いまは保存できません'));
  } finally { process.env.MASTER_EDIT_OPEN = '1'; }
});

await ta('[10] つかいかた: まとめて変える の節', async () => {
  const m = await call('GET', '/manual');
  for (const w of ['まとめて変える', '200 件まで', '前と後', '確かめました', '一緒に変わるセット', '選び直せる', '続きを送る', '対象外', '保存できない', '先の日の原価']) assert.ok(m.text.includes(w), `つかいかたに「${w}」が無い`);
});

server.close();
console.log(`\n${passed} 件 ok`);
