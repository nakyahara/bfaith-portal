/**
 * test-master-columns.mjs — 商品・仕入先マスタに足した列を夜間ロードが埋める (0027 / sources.mjs / engine.mjs。Company DB構想 10 §3 / PR ②c-2)
 *
 * 固定する契約:
 *   1 標準売価・送料 (コード・配送方法・円)・連絡先 6 列・代表の仕入先・Amazon の構成の並び (sort_order) を SQLite の写しから埋める
 *   2 推奨保有月数は商品管理リストの公開 snapshot (status ok/partial・行数が合う) が使えた日だけ。行が無い商品・使えない日は触らない (null にしない)。snapshot の空欄は null
 *   3 代表の仕入先は NE の仕入先コードが変われば付け替わる (旧い代表を外してから新しい代表を付ける = 部分 unique に当たらない)。コードが空の商品は触らない
 *   4 連絡先は発注アプリで空にしたら空になる (coalesce で戻さない)
 *   5 持ち主を 'company' にした新しいキーは夜間ロードが触らない
 *   6 仕入先をまとめる関数 (0027 で追従) が寄せる行にだけある連絡先・代表の印を失わない
 *   7 足した列の変更も変更の記録 (0026) に残る。値が同じ 2 回目は何も増えない
 * 使い方: node apps/company-db/test-master-columns.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from '../../scripts/company-db/migrate.mjs';
import { buildPlanFromRender } from './load/sources.mjs';
import { runInitialLoad } from './load/engine.mjs';
import { MASTER_OWNERSHIP } from '../../config/master-ownership.mjs';

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};

// ── 夜間ロードの材料 (Render の warehouse-mirror.db。列名は各アプリの db.js と同じ) ──
const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-cols-'));
const mirrorPath = path.join(dataDir, 'warehouse-mirror.db');
{
  const m = new Database(mirrorPath);
  m.exec(`
    CREATE TABLE mirror_products (product_id INTEGER PRIMARY KEY, 商品コード TEXT UNIQUE NOT NULL, 商品名 TEXT, 商品区分 TEXT NOT NULL, 取扱区分 TEXT, 標準売価 REAL, 原価 REAL, 原価ソース TEXT, 原価状態 TEXT NOT NULL, 送料 REAL, 送料コード TEXT, 配送方法 TEXT, 消費税率 REAL, 税区分 TEXT, 在庫数 INTEGER, 引当数 INTEGER, 仕入先コード TEXT, セット構成品数 INTEGER, 売上分類 INTEGER, 代表商品コード TEXT, updated_at TEXT NOT NULL);
    CREATE TABLE mirror_sku_master (seller_sku TEXT NOT NULL PRIMARY KEY, 商品名 TEXT, source_created_at TEXT, source_updated_at TEXT, synced_at TEXT NOT NULL);
    CREATE TABLE mirror_sku_resolved (seller_sku TEXT NOT NULL, ne_code TEXT NOT NULL, quantity INTEGER NOT NULL, source TEXT NOT NULL, 商品名 TEXT, source_updated_at TEXT, sort_order INTEGER NOT NULL DEFAULT 0, synced_at TEXT NOT NULL, PRIMARY KEY (seller_sku, ne_code));
    CREATE TABLE po_suppliers (supplier_code TEXT PRIMARY KEY, name TEXT NOT NULL, order_memo TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, email_to TEXT, email_cc TEXT, contact_name TEXT, send_method TEXT, lead_days INTEGER, fax_number TEXT, relay_to TEXT);
    CREATE TABLE po_vendor_code_map (supplier_code TEXT NOT NULL, product_key TEXT NOT NULL, product_code TEXT NOT NULL, vendor_code TEXT NOT NULL, updated_at TEXT NOT NULL, qty_per_unit REAL, PRIMARY KEY (supplier_code, product_key));
    CREATE TABLE mirror_pml_published (id INTEGER PRIMARY KEY CHECK (id = 1), run_id TEXT NOT NULL, status TEXT NOT NULL, as_of_date TEXT, row_count INTEGER, synced_at TEXT NOT NULL);
    CREATE TABLE mirror_pml_snapshot_rows (run_id TEXT NOT NULL, 商品コード TEXT NOT NULL, 推奨保有月数 REAL, PRIMARY KEY (run_id, 商品コード));
  `);
  const ins = (sql, rowsArr) => { const st = m.prepare(sql); for (const r of rowsArr) st.run(...r); };
  ins('insert into mirror_products (商品コード, 商品名, 商品区分, 取扱区分, 標準売価, 原価状態, 送料, 送料コード, 配送方法, 消費税率, 税区分, 仕入先コード, updated_at) values (?,?,?,?,?,?,?,?,?,?,?,?,?)', [
    ['col001', '商品 1', '単品', '取扱中', 1980.4, 'MISSING', 210, 'S01', 'ゆうパケット', 0.1, 'STANDARD_10', '0001', 'x'],
    ['col002', '商品 2', '単品', '取扱中', 980, 'MISSING', null, null, null, 0.1, 'STANDARD_10', '0002', 'x'],
    ['col003', '商品 3', '単品', '取扱中', -5, 'MISSING', 520, 'S02', '宅急便', 0.08, 'REDUCED_8', null, 'x'],   // 売価が負 → null / 仕入先コード空
    ['col004', '商品 4', '単品', '取扱中', 500, 'MISSING', 0, 'S00', '送料無料', 0.1, 'STANDARD_10', '0001', 'x'],   // 送料 0 円は 0 のまま
  ]);
  ins('insert into mirror_sku_master values (?,?,?,?,?)', [['pr_set2', '2 点セット', null, null, 'x']]);
  ins('insert into mirror_sku_resolved (seller_sku, ne_code, quantity, source, sort_order, synced_at) values (?,?,?,?,?,?)', [['pr_set2', 'col002', 1, 'master', 1, 'x'], ['pr_set2', 'col001', 2, 'master', 0, 'x']]);
  ins('insert into po_suppliers (supplier_code, name, order_memo, created_at, updated_at, email_to, email_cc, contact_name, send_method, lead_days, fax_number, relay_to) values (?,?,?,?,?,?,?,?,?,?,?,?)', [
    ['1', 'アメージングクラフト様', '月曜締め', 'x', 'x', 'order@amc.example', 'cc@amc.example', '山田', 'email', 10, null, null],
    ['2', 'ビーフリー様', null, 'x', 'x', null, null, null, 'fax', null, '06-0000-0000', null],
  ]);
  ins('insert into po_vendor_code_map (supplier_code, product_key, product_code, vendor_code, updated_at, qty_per_unit) values (?,?,?,?,?,?)', [['1', 'col001', 'col001', 'AMC-001', 'x', 12]]);
  ins('insert into mirror_pml_published values (?,?,?,?,?,?)', [[1, 'pml_1', 'ok', '2026-09-24', 3, 'x']]);
  ins('insert into mirror_pml_snapshot_rows values (?,?,?)', [['pml_1', 'col001', 3], ['pml_1', 'col002', null], ['pml_1', 'col004', 1.25]]);   // col003 は行なし
  m.close();
}
const mirrorExec = (sql, params = []) => { const m = new Database(mirrorPath); try { m.prepare(sql).run(...params); } finally { m.close(); } };

const pg = new PGlite();
const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
const q = async (sql, params) => (await db.query(sql, params)).rows;
const plan = () => buildPlanFromRender({ dataDir, log: quiet });
const run = async (runId, ownership) => { const r = await runInitialLoad(db, plan(), { log: quiet, runId, host: 'test-host', ownership }); assert.equal(r.ok, true, r.error); return r; };
const sku = async (code) => (await q('select standard_price_jpy::int as price, shipping_code, shipping_method, shipping_cost_jpy::int as ship, reorder_months::float8 as months from core.skus where code = $1', [code]))[0];
const primaryOf = async (code) => (await q('select s.code from core.supplier_skus x join core.suppliers s on s.supplier_id = x.supplier_id join core.skus k on k.sku_id = x.sku_id where k.code = $1 and x.is_primary', [code])).map((r) => r.code);

await ta('[1][2] 足した列を写しから埋める (売価・送料・推奨保有月数・連絡先・代表の仕入先・構成の並び)', async () => {
  const r = await run('col_1');
  assert.deepEqual(await sku('col001'), { price: 1980, shipping_code: 'S01', shipping_method: 'ゆうパケット', ship: 210, months: 3 });
  assert.deepEqual(await sku('col002'), { price: 980, shipping_code: null, shipping_method: null, ship: null, months: null });   // snapshot の空欄 = null
  assert.deepEqual(await sku('col003'), { price: null, shipping_code: 'S02', shipping_method: '宅急便', ship: 520, months: null });   // 負の売価は null・snapshot に行なし
  assert.deepEqual(await sku('col004'), { price: 500, shipping_code: 'S00', shipping_method: '送料無料', ship: 0, months: 1.3 });   // 0 円は 0・1.25 → 小数 1 桁
  const sup = (await q("select name, email_to, email_cc, contact_name, fax_number, relay_to, order_memo, order_method from core.suppliers where code = '0001'"))[0];
  assert.deepEqual(sup, { name: 'アメージングクラフト様', email_to: 'order@amc.example', email_cc: 'cc@amc.example', contact_name: '山田', fax_number: null, relay_to: null, order_memo: '月曜締め', order_method: 'email' });
  assert.equal((await q("select fax_number from core.suppliers where code = '0002'"))[0].fax_number, '06-0000-0000');
  assert.deepEqual(await primaryOf('col001'), ['0001']); assert.deepEqual(await primaryOf('col002'), ['0002']); assert.deepEqual(await primaryOf('col003'), []);
  const comps = await q("select k.code, lc.sort_order from core.listing_components lc join core.listings l on l.listing_id = lc.listing_id join core.skus k on k.sku_id = lc.sku_id where l.listing_code = 'pr_set2' order by lc.sort_order");
  assert.deepEqual(comps.map((c) => [c.code, Number(c.sort_order)]), [['col001', 0], ['col002', 1]]);
  assert.ok(r.sections.skus.notes.some((x) => /推奨保有月数: 変更/.test(x)), JSON.stringify(r.sections.skus.notes));
  assert.ok(r.sections.supplier_skus.notes.some((x) => /代表の仕入先: 外した 0 \/ 付けた 3/.test(x)), JSON.stringify(r.sections.supplier_skus.notes));
});

await ta('[7] 値が同じ 2 回目は何も変わらない (記録も増えない)', async () => {
  const n0 = (await q('select count(*)::int as n from events.master_change_events'))[0].n;
  const r = await run('col_2');
  assert.equal((await q('select count(*)::int as n from events.master_change_events'))[0].n, n0);
  for (const k of ['skus', 'suppliers', 'supplier_skus', 'listing_components']) assert.equal(r.summary[k].applied, 0, k);
  assert.ok(r.sections.supplier_skus.notes.some((x) => /外した 0 \/ 付けた 0/.test(x)));
});

await ta('[2] snapshot が使えない日 (status failed・行数が合わない) は推奨保有月数に触らない。行が無い商品も触らない', async () => {
  await db.query("update core.skus set reorder_months = 6 where code = 'col003'");   // 人が入れた値 (snapshot に行が無い商品)
  mirrorExec("update mirror_pml_snapshot_rows set 推奨保有月数 = 9 where 商品コード = 'col001'");
  mirrorExec("update mirror_pml_published set status = 'failed'");
  let r = await run('col_3');
  assert.equal((await sku('col001')).months, 3);
  assert.ok(r.sections.skus.notes.some((x) => /推奨保有月数: 触らない \(公開 snapshot の status = failed\)/.test(x)), JSON.stringify(r.sections.skus.notes));
  mirrorExec("update mirror_pml_published set status = 'ok', row_count = 99");
  r = await run('col_4');
  assert.equal((await sku('col001')).months, 3);
  assert.ok(r.sections.skus.notes.some((x) => /行数が合わない/.test(x)));
  mirrorExec("update mirror_pml_published set row_count = 3");
  await run('col_5');
  assert.equal((await sku('col001')).months, 9);
  assert.equal((await sku('col003')).months, 6);   // snapshot に行が無い = 触らない
});

await ta('[3] 代表の仕入先の付け替え (旧い代表を外してから付ける)。NE の仕入先コードが空になった商品は触らない', async () => {
  mirrorExec("update mirror_products set 仕入先コード = '0002' where 商品コード = 'col001'");
  mirrorExec("update mirror_products set 仕入先コード = null where 商品コード = 'col004'");
  const r = await run('col_6');
  assert.deepEqual(await primaryOf('col001'), ['0002']);
  assert.deepEqual(await primaryOf('col004'), ['0001']);   // コードが空 = 保留 (外さない)
  assert.ok(r.sections.supplier_skus.notes.some((x) => /外した 1 \/ 付けた 1/.test(x)), JSON.stringify(r.sections.supplier_skus.notes));
  const ev = await q("select attribute, old_value, new_value from events.master_change_events where run_id = 'col_6' and entity_type = 'supplier_sku' and attribute = 'is_primary' order by event_id");
  assert.deepEqual(ev.map((e) => [e.old_value, e.new_value]), [[true, false], [false, true]]);
});

await ta('[4] 連絡先は発注アプリで空にしたら空になる (coalesce で戻さない)', async () => {
  mirrorExec("update po_suppliers set email_cc = null, contact_name = '佐藤' where supplier_code = '1'");
  await run('col_7');
  const sup = (await q("select email_cc, contact_name from core.suppliers where code = '0001'"))[0];
  assert.deepEqual(sup, { email_cc: null, contact_name: '佐藤' });
});

await ta('[5] 持ち主を company にした新しいキーは触らない', async () => {
  await db.query("update core.skus set standard_price_jpy = 7777, shipping_code = '人', reorder_months = 12 where code = 'col001'");
  await db.query("update core.suppliers set email_to = '人が直した@example' where code = '0001'");
  mirrorExec("update mirror_products set 仕入先コード = '0001' where 商品コード = 'col001'");
  const company = { ...MASTER_OWNERSHIP, 'skus.standard_price': 'company', 'skus.shipping': 'company', 'skus.reorder_months': 'company', 'suppliers.contacts': 'company', 'supplier_skus.is_primary': 'company' };
  const r = await run('col_8', company);
  const s1 = await sku('col001');
  assert.equal(s1.price, 7777); assert.equal(s1.shipping_code, '人'); assert.equal(s1.months, 12);
  assert.equal((await q("select email_to from core.suppliers where code = '0001'"))[0].email_to, '人が直した@example');
  assert.deepEqual(await primaryOf('col001'), ['0002']);   // NE では 0001 に戻したが、代表の持ち主が Company DB なので動かない
  assert.ok(r.sections.skus.notes.some((x) => /推奨保有月数: Company DB が正/.test(x)));
  assert.ok(r.sections.supplier_skus.notes.some((x) => /代表の仕入先: Company DB が正/.test(x)));
});

await ta('[6] 仕入先をまとめる関数が、寄せる行にだけある連絡先・代表の印を失わない', async () => {
  const sid = Number((await q("insert into core.skus (company_id, sku_kind, code, name) values (1, 'exception', 'col900', 'まとめ試験') returning sku_id"))[0].sku_id);
  const keep = Number((await q("insert into core.suppliers (company_id, code, name) values (1, '0005', '0005') returning supplier_id"))[0].supplier_id);
  const drop = Number((await q("insert into core.suppliers (company_id, code, name, email_to, fax_number) values (1, '5', 'サンスター技研様', 'order@sunstar.example', '03-1111-2222') returning supplier_id"))[0].supplier_id);
  await db.query('insert into core.supplier_skus (company_id, supplier_id, sku_id, is_primary) values (1, $1, $2, true)', [drop, sid]);
  const r = (await q('select * from core.merge_duplicate_suppliers()'))[0];
  assert.equal(r.merged_suppliers, 1);
  const k = (await q('select name, email_to, fax_number from core.suppliers where supplier_id = $1', [keep]))[0];
  assert.deepEqual(k, { name: 'サンスター技研様', email_to: 'order@sunstar.example', fax_number: '03-1111-2222' });
  assert.equal((await q('select is_primary from core.supplier_skus where supplier_id = $1 and sku_id = $2', [keep, sid]))[0].is_primary, true);
});

await pg.close();
try { fs.rmSync(dataDir, { recursive: true, force: true }); } catch { /* Windows は OS に任せる */ }
console.log(`\n${passed} 件 PASS`);
