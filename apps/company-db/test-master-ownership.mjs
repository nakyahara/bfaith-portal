/**
 * test-master-ownership.mjs — 夜間ロードの「列ごとの持ち主」(config/master-ownership.mjs。Company DB構想 10 §5.2) の試験
 *
 * 固定する契約:
 *   1 既定 (全部 'load') は今までどおり SQLite の値に合わせる
 *   2 'company' にした列は、Company DB で直した値を夜間ロードが上書きしない (空欄にしても埋め戻さない)
 *   3 'company' でも、新しく見つかった行には最初の値を入れる
 *   4 原価・セット構成・Amazon の構成を 'company' にすると、夜間ロードはその区分に触らない (予定 0 件・メモつき)
 *   5 値が変わらない行は UPDATE しない (updated_at が進まない = 人が直した行を見分けられる)。sku_id / supplier_id は読み直して後段が使える
 *   6 'load' に戻すと、次のロードで SQLite の値に合わせ直す (切替の取り消しが効く)
 *   7 知らないキー・値・書き漏れは落とす (typo で守ったつもりを作らない)
 * 使い方: node apps/company-db/test-master-ownership.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from '../../scripts/company-db/migrate.mjs';
import { runInitialLoad } from './load/engine.mjs';
import { MASTER_OWNERSHIP, OWNED_COLUMNS, validateOwnership, companyOwned } from '../../config/master-ownership.mjs';

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};

const AMZ = 'main@A1VC38T7YXB528';
/** 小さなロード計画 (sources.mjs が作る形と同じ) */
function makePlan() {
  return {
    skus: [
      { code: 'own001', name: 'NE の名前 1', kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3, cost: { jpy: 100, source: 'ne', status: 'COMPLETE' } },
      { code: 'own002', name: 'NE の名前 2', kind: 'single', taxRate: 0.08, taxClass: 'REDUCED_8', handling: 'active', salesClass: 2, cost: { jpy: 200, source: 'ne', status: 'COMPLETE' } },
      { code: 'set001', name: 'NE のセット', kind: 'set', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: null, cost: { jpy: 250, source: 'set_calc', status: 'COMPLETE' } },
    ],
    variationGroups: [],
    setComponents: [{ parentCode: 'set001', childCode: 'own001', qty: 2, source: 'ne' }],
    listings: [
      { mall: 'amazon', shopCode: AMZ, marketplaceId: 'A1VC38T7YXB528', listingCode: 'pr_own001', title: 'Amazon の出品', status: 'active', components: [{ code: 'own001', qty: 1, resolution: 'imported', evidence: { source: 'm_sku_master' } }], asinCandidates: [], fnskuCandidates: [] },
      { mall: 'rakuten', shopCode: 'main', listingCode: 'own002', status: 'active', components: [{ code: 'own002', qty: 1, resolution: 'exact', evidence: { source: 'rakuten_sku_map' } }] },
    ],
    observations: [], physicals: [], compliance: [], workers: [],
    suppliers: [{ code: '0001', name: 'AMC (発注アプリ)', orderMethod: 'fax', leadTimeDays: 10 }],
    supplierSkus: [{ supplierCode: '0001', skuCode: 'own001', vendorCode: 'AMC-001', stockUnitsPerOrderUnit: 12 }],
  };
}
const with_ = (over) => ({ ...MASTER_OWNERSHIP, ...over });

const pg = new PGlite();
const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
const q = async (sql, params) => (await db.query(sql, params)).rows;
const balanced = (r) => { for (const [k, v] of Object.entries(r.sections)) assert.equal(v.expected, v.applied + v.same + v.skipped.length, `${k} が釣り合わない`); };
const run = async (plan, runId, ownership) => { const r = await runInitialLoad(db, plan, { log: quiet, runId, ownership }); assert.equal(r.ok, true, r.error); balanced(r); return r; };
const sku = async (code) => (await q('select s.name, s.tax_rate::float8 as tax_rate, s.tax_class, s.handling, p.name as pname, p.sales_class, p.status from core.skus s left join core.products p on p.product_id = s.product_id where s.code = $1', [code]))[0];
const activeCost = async (code) => (await q('select c.cost_jpy::int as jpy, c.cost_source from core.sku_costs c join core.skus s on s.sku_id = c.sku_id where s.code = $1 and c.valid_to is null', [code]))[0];
const setComps = async () => (await q("select c.qty from core.sku_components c join core.skus p on p.sku_id = c.parent_sku_id where p.code = 'set001'")).map((r) => Number(r.qty));
const amzComps = async () => (await q("select lc.qty, lc.resolution from core.listing_components lc join core.listings l on l.listing_id = lc.listing_id where l.mall = 'amazon' and l.listing_code = 'pr_own001'")).map((r) => [Number(r.qty), r.resolution]);
const supplier = async () => (await q("select name, order_method, lead_time_days from core.suppliers where code = '0001'"))[0];

await ta('[1] 既定 (全部 load): SQLite の値で作る。report.company_owned は空', async () => {
  assert.deepEqual(companyOwned(MASTER_OWNERSHIP), []);
  const r = await run(makePlan(), 'own_1');
  assert.deepEqual(r.company_owned, []);
  assert.deepEqual(await sku('own001'), { name: 'NE の名前 1', tax_rate: 0.1, tax_class: 'STANDARD_10', handling: 'active', pname: 'NE の名前 1', sales_class: 3, status: 'active' });
  assert.deepEqual(await activeCost('own001'), { jpy: 100, cost_source: 'ne' });
  assert.deepEqual(await setComps(), [2]);
  assert.deepEqual(await amzComps(), [[1, 'imported']]);
  assert.deepEqual(await supplier(), { name: 'AMC (発注アプリ)', order_method: 'fax', lead_time_days: 10 });
});

await ta('[5] 値が変わらない 2 回目は UPDATE しない: skus / suppliers は applied 0・same = 全件、updated_at は進まない', async () => {
  const before = await q('select code, updated_at from core.skus order by code');
  const beforeSup = await q('select code, updated_at from core.suppliers order by code');
  const r = await run(makePlan(), 'own_2');
  assert.equal(r.summary.skus.applied, 0); assert.equal(r.summary.skus.same, 3);
  assert.equal(r.summary.suppliers.applied, 0); assert.equal(r.summary.suppliers.same, 1);
  assert.equal(r.summary.products.applied, 0);
  assert.deepEqual(await q('select code, updated_at from core.skus order by code'), before);
  assert.deepEqual(await q('select code, updated_at from core.suppliers order by code'), beforeSup);
  // 後段 (supplier_skus・構成) は読み直した id で動いている
  assert.equal(r.summary.supplier_skus.skipped, 0);
  assert.equal((await q("select vendor_code from core.supplier_skus x join core.skus s on s.sku_id = x.sku_id where s.code = 'own001'"))[0].vendor_code, 'AMC-001');
});

// 人が Company DB で直した (ポータルの編集画面の代わりに SQL で直す)
await db.query("update core.skus set name = '人が直した名前', tax_rate = 0.08, tax_class = 'REDUCED_8', handling = 'discontinued' where code = 'own001'");
await db.query("update core.products p set name = '人が直した商品名', sales_class = 1, status = 'discontinued' from core.skus s where s.product_id = p.product_id and s.code = 'own001'");
await db.query("update core.suppliers set name = 'アメージングクラフト', order_method = null, lead_time_days = 7 where code = '0001'");
await db.query("update core.sku_costs set valid_to = valid_from where valid_to is null and sku_id = (select sku_id from core.skus where code = 'own001')");
await db.query("insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from, reason, created_by_type, created_by_id) select 1, sku_id, 999, 'manual', 'COMPLETE', current_date + 1, 'test: 人が直した', 'human', 'test' from core.skus where code = 'own001'");
await db.query("update core.sku_components set qty = 5 where parent_sku_id = (select sku_id from core.skus where code = 'set001')");
await db.query("update core.listing_components set qty = 3 where listing_id = (select listing_id from core.listings where listing_code = 'pr_own001')");

const ALL_COMPANY = with_(Object.fromEntries(Object.keys(MASTER_OWNERSHIP).map((k) => [k, 'company'])));

await ta('[2][4] 全部 company: 人が直した名前・税率・取扱・分類・状態・仕入先・原価・セット構成・Amazon の構成が夜間ロードで戻らない', async () => {
  const plan = makePlan();
  plan.skus.push({ code: 'own003', name: 'NE の新商品', kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 4, cost: { jpy: 300, source: 'ne', status: 'COMPLETE' } });
  const r = await run(plan, 'own_3', ALL_COMPANY);
  assert.equal(r.company_owned.length, Object.keys(MASTER_OWNERSHIP).length);
  assert.deepEqual(await sku('own001'), { name: '人が直した名前', tax_rate: 0.08, tax_class: 'REDUCED_8', handling: 'discontinued', pname: '人が直した商品名', sales_class: 1, status: 'discontinued' });
  assert.deepEqual(await supplier(), { name: 'アメージングクラフト', order_method: null, lead_time_days: 7 });   // 空欄にした発注方法も埋め戻さない
  assert.deepEqual(await activeCost('own001'), { jpy: 999, cost_source: 'manual' });
  assert.deepEqual(await setComps(), [5]);
  assert.deepEqual(await amzComps(), [[3, 'imported']]);
  // [4] 区分ごと見送った印
  assert.equal(r.summary.sku_costs.expected, 0); assert.ok(r.sections.sku_costs.notes.some((n) => /Company DB が正/.test(n)));
  assert.equal(r.summary.set_components.expected, 0); assert.ok(r.sections.set_components.notes.some((n) => /Company DB が正/.test(n)));
  assert.equal(r.summary.listing_components.expected, 1);   // 楽天の 1 行だけ (Amazon は見送り)
  assert.ok(r.sections.listing_components.notes.some((n) => /Amazon の構成 1 行は見送り/.test(n)));
  // [3] 新しく見つかった商品には最初の値が入る (原価は company なので作らない)
  assert.deepEqual(await sku('own003'), { name: 'NE の新商品', tax_rate: 0.1, tax_class: 'STANDARD_10', handling: 'active', pname: 'NE の新商品', sales_class: 4, status: 'active' });
  assert.equal(await activeCost('own003'), undefined);
  // 出品そのもの・仕入先ごとの先方品番 (発注アプリが正) は今までどおり
  assert.equal((await q("select vendor_code from core.supplier_skus x join core.skus s on s.sku_id = x.sku_id where s.code = 'own001'"))[0].vendor_code, 'AMC-001');
});

await ta('[2] 一部だけ company: 名前だけ守り、税率は SQLite に合わせる', async () => {
  await db.query("update core.skus set tax_rate = 0.08, tax_class = 'REDUCED_8' where code = 'own002'");
  await db.query("update core.skus set name = '人が直した名前 2' where code = 'own002'");
  await run(makePlan(), 'own_4', with_({ 'skus.name': 'company' }));
  const s = await sku('own002');
  assert.equal(s.name, '人が直した名前 2');
  assert.equal(s.tax_rate, 0.08);   // own002 の NE 値は 0.08 なので同じ
  await db.query("update core.skus set tax_rate = 0.1, tax_class = 'STANDARD_10' where code = 'own002'");
  await run(makePlan(), 'own_5', with_({ 'skus.name': 'company' }));
  const s2 = await sku('own002');
  assert.equal(s2.name, '人が直した名前 2');
  assert.equal(s2.tax_rate, 0.08); assert.equal(s2.tax_class, 'REDUCED_8');   // 税率は load なので NE (0.08) に戻る
});

await ta('[6] load に戻すと SQLite の値に合わせ直す (切替の取り消し)', async () => {
  const r = await run(makePlan(), 'own_6');
  assert.deepEqual(r.company_owned, []);
  assert.deepEqual(await sku('own001'), { name: 'NE の名前 1', tax_rate: 0.1, tax_class: 'STANDARD_10', handling: 'active', pname: 'NE の名前 1', sales_class: 3, status: 'active' });
  assert.deepEqual(await supplier(), { name: 'AMC (発注アプリ)', order_method: 'fax', lead_time_days: 10 });
  assert.deepEqual(await activeCost('own001'), { jpy: 100, cost_source: 'ne' });
  assert.deepEqual(await setComps(), [2]);
  assert.deepEqual(await amzComps(), [[1, 'imported']]);
});

await ta('[5] 仕入先の列を全部 company にしても (do nothing)、supplier_skus は読み直した id で付く', async () => {
  const plan = makePlan();
  plan.skus.push({ code: 'own004', name: 'NE の新商品 4', kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 1, cost: null });
  plan.suppliers.push({ code: '0002', name: '新しい仕入先', orderMethod: 'email', leadTimeDays: 5 });
  plan.supplierSkus.push({ supplierCode: '0002', skuCode: 'own004', vendorCode: 'B-004', stockUnitsPerOrderUnit: 6 });
  const r = await run(plan, 'own_7', with_({ 'suppliers.name': 'company', 'suppliers.order_method': 'company', 'suppliers.lead_time_days': 'company' }));
  assert.equal(r.summary.suppliers.applied, 1); assert.equal(r.summary.suppliers.same, 1);   // 0002 は新規 (最初の値)、0001 は触らない
  assert.equal(r.summary.supplier_skus.skipped, 0);
  assert.equal((await q("select name from core.suppliers where code = '0002'"))[0].name, '新しい仕入先');
  assert.equal((await q("select vendor_code from core.supplier_skus x join core.skus s on s.sku_id = x.sku_id where s.code = 'own004'"))[0].vendor_code, 'B-004');
});

await ta('[7] 知らないキー・知らない値・書き漏れは OWNERSHIP_INVALID で落とす (ロードも始めない)', async () => {
  assert.throws(() => validateOwnership(with_({ 'skus.nmae': 'company' })), (e) => e.code === 'OWNERSHIP_INVALID' && /知らない列: skus\.nmae/.test(e.message));
  assert.throws(() => validateOwnership(with_({ 'skus.name': 'Company' })), (e) => e.code === 'OWNERSHIP_INVALID' && /持ち主が不正/.test(e.message));
  const missing = { ...MASTER_OWNERSHIP }; delete missing['sku_costs'];
  assert.throws(() => validateOwnership(missing), (e) => e.code === 'OWNERSHIP_INVALID' && /書かれていない列: sku_costs/.test(e.message));
  const before = (await q('select count(*)::int as n from ops.ingest_runs'))[0].n;
  await assert.rejects(runInitialLoad(db, makePlan(), { log: quiet, runId: 'own_bad', ownership: with_({ 'skus.name': 'nobody' }) }), (e) => e.code === 'OWNERSHIP_INVALID');
  assert.equal((await q('select count(*)::int as n from ops.ingest_runs'))[0].n, before);
});

await ta('[7] 設定ファイルそのものの typo も落とす: 正しいキーの一覧は設定とは別 (OWNED_COLUMNS) に持つ', async () => {
  // 設定に typo のキーを足し、本物のキーは load のまま = 「守ったつもり」の形 (Codex PR #1440 R1 Medium)
  const typoConfig = { ...MASTER_OWNERSHIP, 'products.nmae': 'company' };
  assert.throws(() => validateOwnership(typoConfig), (e) => e.code === 'OWNERSHIP_INVALID' && /知らない列: products\.nmae/.test(e.message));
  // 本物のキーを typo に置き換えた形 (書き漏れと知らない列の両方で落ちる)
  const renamed = { ...MASTER_OWNERSHIP }; delete renamed['products.name']; renamed['products.nmae'] = 'company';
  assert.throws(() => validateOwnership(renamed), (e) => /知らない列: products\.nmae/.test(e.message) && /書かれていない列: products\.name/.test(e.message));
  // 一覧と設定のキーが同じ
  assert.deepEqual([...OWNED_COLUMNS].sort(), Object.keys(MASTER_OWNERSHIP).sort());
});

await ta('[7] engine.mjs が見ている列 = OWNED_COLUMNS (engine だけに足して一覧に足し忘れる・その逆を機械で見る)', async () => {
  const src = fs.readFileSync(new URL('./load/engine.mjs', import.meta.url), 'utf8');
  const used = new Set();
  for (const m of src.matchAll(/loadOwns\('([^']+)'\)/g)) used.add(m[1]);
  // 持ち主のキーの形をした文字列 ('skus.name' など。map でまとめて書いた連絡先の 'suppliers.contacts' も拾う)
  for (const m of src.matchAll(/'((?:products|skus|suppliers|supplier_skus)\.[a-z_]+|listing_components\.amazon)'/g)) used.add(m[1]);
  assert.deepEqual([...used].sort(), [...OWNED_COLUMNS].sort());
});

await pg.close();
console.log(`\n${passed} 件 PASS`);
