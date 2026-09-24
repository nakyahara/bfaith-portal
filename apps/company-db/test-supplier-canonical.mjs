/**
 * test-supplier-canonical.mjs — 仕入先コードを揃え、先頭 0 の食い違いで二重になった仕入先をまとめる (0025 / sources.mjs。Company DB構想 10 §9 D)
 *
 * 固定する契約:
 *   1 数字だけのコードは 4 桁の 0 埋め (NE の形)。4 桁より長い数字は先頭の 0 を外すだけ。数字以外はそのまま。JS と SQL で同じ
 *   2 夜間ロードの材料: 発注アプリ '1' と NE '0001' と共有マスタ '0002' が、揃えた形で 1 社ずつになる (名前は発注アプリ優先)
 *   3 0025: 残す行 = 正しい形の行。名前がコードのままなら寄せる行の名前を使う。発注方法・リードタイムは空欄だけ補う
 *   4 0025: 仕入先ごとの商品は、重なれば空欄だけ埋めて寄せる行を消し、重ならなければ付け替える。3 行が 1 つのまとまり ('0001'/'1'/'01') でも崩れない
 *   5 0025: 発注・外部 ID・文書の紐付けを付け替える。1 行しか無い短い形 ('7') はコードを '0007' に書き換える。数字以外・長い数字はそのまま
 *   6 0025 の後の夜間ロード (揃えた材料) で二重が戻らない
 * 使い方: node apps/company-db/test-supplier-canonical.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from '../../scripts/company-db/migrate.mjs';
import { canonicalSupplierCode, buildPlanFromRender } from './load/sources.mjs';
import { runInitialLoad } from './load/engine.mjs';

let passed = 0;
function t(name, fn) { try { fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};

const CASES = [['1', '0001'], ['0001', '0001'], ['01', '0001'], ['00001', '0001'], ['99', '0099'], ['900', '0900'], ['9999', '9999'],
  ['12345', '12345'], ['012345', '12345'], ['0', '0000'], [' 7 ', '0007'], ['abc', 'abc'], ['A-01', 'A-01'], ['', '']];

t('[1] canonicalSupplierCode (JS)', () => {
  for (const [a, b] of CASES) assert.equal(canonicalSupplierCode(a), b, a);
});

const pg = new PGlite();
const db = pgliteAdapter(pg);
const q = async (sql, params) => (await db.query(sql, params)).rows;

// 0024 まで = 本番の今の姿
await applyMigrations(db, { log: quiet, to: '0024' });
// 二重の仕入先 (本番の形: '0001' = 名前がコードのまま・NE の商品 / '1' = 名前・発注方法・先方品番) + 3 重 ('01') + 単独
const sup = async (code, name, om = null, lt = null) => Number((await q("insert into core.suppliers (company_id, code, name, order_method, lead_time_days) values (1, $1, $2, $3, $4) returning supplier_id", [code, name, om, lt]))[0].supplier_id);
const skuId = async (code) => Number((await q("insert into core.skus (company_id, sku_kind, code, name) values (1, 'set', $1, $1) returning sku_id", [code]))[0].sku_id);
const ss = (sid, kid, vendor = null, units = null) => db.query('insert into core.supplier_skus (company_id, supplier_id, sku_id, vendor_code, stock_units_per_order_unit) values (1, $1, $2, $3, $4)', [sid, kid, vendor, units]);
const S = {
  l0001: await sup('0001', '0001', null, 10), s1: await sup('1', 'アメージングクラフト様', 'email', 7), s01: await sup('01', '01'),
  l0099: await sup('0099', '0099'), s99: await sup('99', '千年前の食品舎様', 'email'),
  s900: await sup('900', 'トイズファン様', 'web'), l0900: await sup('0900', '0900'),
  l9999: await sup('9999', 'B-Faith株式会社'), s7: await sup('7', '単独の短いコード'), abc: await sup('abc', '数字以外'), long: await sup('12345', '長い数字'),
};
const K = { a: await skuId('ska'), b: await skuId('skb'), c: await skuId('skc'), d: await skuId('skd') };
await ss(S.l0001, K.a);                 // 残す行: a (先方品番なし)・b
await ss(S.l0001, K.b);
await ss(S.s1, K.a, 'V-A', 12);         // 寄せる行: a (重なる → 空欄だけ埋める)・c (付け替え)
await ss(S.s1, K.c, 'V-C');
await ss(S.s01, K.c, 'V-C2');           // 3 行目: c は寄せる行どうしで重なる (supplier_id の小さい '1' が残る)・d は付け替え
await ss(S.s01, K.d, 'V-D');
await ss(S.s900, K.a, 'T-A');
await q("insert into core.purchase_orders (company_id, source_ref, supplier_id, supplier_code, supplier_name, status, source_updated_at) values (1, 'po_orders:1', $1, '1', 'アメージングクラフト様', 'draft', now())", [S.s1]);
await q("insert into core.external_ids (company_id, entity_type, entity_id, system, id_kind, external_value, resolution, resolved_by_type, resolved_by_id) values (1, 'supplier', $1, 'purchase_orders', 'supplier_code', '99', 'imported', 'system', 'test')", [S.s99]);

await ta('[1] canonical_supplier_code (SQL) が JS と同じ', async () => {
  await applyMigrations(db, { log: quiet });   // 0025 を流す (関数もここで入る)
  for (const [a, b] of CASES.filter(([x]) => x !== '')) assert.equal((await q('select core.canonical_supplier_code($1) as c', [a]))[0].c, b, a);
});

await ta('[3] 二重がまとまり、正しい形の行が残る。名前・発注方法・リードタイムを補う', async () => {
  const rows = await q('select supplier_id, code, name, order_method, lead_time_days from core.suppliers order by code');
  assert.deepEqual(rows.map((r) => r.code), ['0001', '0007', '0099', '0900', '12345', '9999', 'abc']);
  const by = Object.fromEntries(rows.map((r) => [r.code, r]));
  assert.equal(Number(by['0001'].supplier_id), S.l0001);                 // 残すのは正しい形の行
  assert.equal(by['0001'].name, 'アメージングクラフト様');               // コードのままの名前 → 寄せる行の名前
  assert.equal(by['0001'].order_method, 'email');                         // 空欄だけ補う
  assert.equal(by['0001'].lead_time_days, 10);                            // 残す行に値があれば残す行 (7 で上書きしない)
  assert.equal(by['0099'].name, '千年前の食品舎様');
  assert.equal(by['0900'].name, 'トイズファン様'); assert.equal(by['0900'].order_method, 'web');
  assert.equal(Number(by['0900'].supplier_id), S.l0900);
  assert.equal(by['0007'].name, '単独の短いコード'); assert.equal(Number(by['0007'].supplier_id), S.s7);   // 1 行だけの短い形はコードを書き換え
  assert.equal(by['9999'].name, 'B-Faith株式会社');
});

await ta('[4] 仕入先ごとの商品: 重なれば空欄を埋めて 1 行、重ならなければ付け替え (3 重のまとまりでも)', async () => {
  const rows = await q("select s.code as sku, x.vendor_code, x.stock_units_per_order_unit as units from core.supplier_skus x join core.skus s on s.sku_id = x.sku_id where x.supplier_id = $1 order by s.code", [S.l0001]);
  assert.deepEqual(rows.map((r) => [r.sku, r.vendor_code, r.units]), [['ska', 'V-A', 12], ['skb', null, null], ['skc', 'V-C', null], ['skd', 'V-D', null]]);
  assert.equal((await q('select count(*)::int as n from core.supplier_skus where supplier_id = any($1::bigint[])', [[S.s1, S.s01, S.s99, S.s900]]))[0].n, 0);
  assert.deepEqual((await q('select vendor_code from core.supplier_skus where supplier_id = $1', [S.l0900])).map((r) => r.vendor_code), ['T-A']);
});

await ta('[5] 発注・外部 ID は残す行へ付け替え、寄せた行は消える', async () => {
  assert.equal(Number((await q("select supplier_id from core.purchase_orders where source_ref = 'po_orders:1'"))[0].supplier_id), S.l0001);
  assert.equal(Number((await q("select entity_id from core.external_ids where entity_type = 'supplier'"))[0].entity_id), S.l0099);
  assert.equal((await q('select count(*)::int as n from core.suppliers where supplier_id = any($1::bigint[])', [[S.s1, S.s01, S.s99, S.s900]]))[0].n, 0);
  assert.equal((await q("select count(*)::int as n from ops.schema_migrations where version = '0025'"))[0].n, 1);
});

// 夜間ロードの材料 (SQLite) を作る
const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'sup-canon-'));
{
  const m = new Database(path.join(dataDir, 'warehouse-mirror.db'));
  m.exec(`
    CREATE TABLE mirror_products (product_id INTEGER PRIMARY KEY, 商品コード TEXT UNIQUE NOT NULL, 商品名 TEXT, 商品区分 TEXT NOT NULL, 取扱区分 TEXT, 標準売価 REAL, 原価 REAL, 原価ソース TEXT, 原価状態 TEXT NOT NULL, 送料 REAL, 送料コード TEXT, 配送方法 TEXT, 消費税率 REAL, 税区分 TEXT, 在庫数 INTEGER, 引当数 INTEGER, 仕入先コード TEXT, セット構成品数 INTEGER, 売上分類 INTEGER, 代表商品コード TEXT, updated_at TEXT NOT NULL);
    CREATE TABLE po_suppliers (supplier_code TEXT PRIMARY KEY, name TEXT NOT NULL, order_memo TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, send_method TEXT, lead_days INTEGER);
    CREATE TABLE po_vendor_code_map (supplier_code TEXT NOT NULL, product_key TEXT NOT NULL, product_code TEXT NOT NULL, vendor_code TEXT NOT NULL, updated_at TEXT NOT NULL, qty_per_unit REAL, PRIMARY KEY (supplier_code, product_key));
    CREATE TABLE supplier_share_master (仕入先コード TEXT PRIMARY KEY, 表示名 TEXT NOT NULL, memo TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
  `);
  const ins = (sql, rowsArr) => { const st = m.prepare(sql); for (const r of rowsArr) st.run(...r); };
  ins('insert into mirror_products (商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 仕入先コード, updated_at) values (?,?,?,?,?,?,?)', [
    ['ska', 'A', 'セット', '取扱中', 'MISSING', '0001', 'x'], ['ske', 'E', 'セット', '取扱中', 'MISSING', '0099', 'x'], ['skf', 'F', 'セット', '取扱中', 'MISSING', '0002', 'x'],
  ]);
  ins('insert into po_suppliers (supplier_code, name, created_at, updated_at, send_method, lead_days) values (?,?,?,?,?,?)', [['1', 'アメージングクラフト様', 'x', 'x', 'email', null], ['99', '千年前の食品舎様', 'x', 'x', 'email', null]]);
  ins('insert into po_vendor_code_map (supplier_code, product_key, product_code, vendor_code, updated_at, qty_per_unit) values (?,?,?,?,?,?)', [['1', 'ska', 'ska', 'V-A', 'x', 12]]);
  ins('insert into supplier_share_master values (?,?,?,?,?)', [['0002', '共有マスタの仕入先', null, 'x', 'x']]);
  m.close();
}

await ta('[2] 夜間ロードの材料: 発注アプリ \'1\' と NE \'0001\' は 1 社 (名前は発注アプリ)、先方品番も揃えた形', async () => {
  const plan = buildPlanFromRender({ dataDir, log: quiet });
  const codes = plan.suppliers.map((x) => x.code).sort();
  assert.deepEqual(codes, ['0001', '0002', '0099']);
  assert.equal(plan.suppliers.find((x) => x.code === '0001').name, 'アメージングクラフト様');
  assert.equal(plan.suppliers.find((x) => x.code === '0002').name, '共有マスタの仕入先');
  assert.ok(plan.supplierSkus.every((x) => /^\d{4}$/.test(x.supplierCode)), JSON.stringify(plan.supplierSkus));
  assert.deepEqual(plan.supplierSkus.filter((x) => x.skuCode === 'ska').map((x) => [x.supplierCode, x.vendorCode ?? null]), [['0001', 'V-A']]);
});

await ta('[6] 0025 の後に夜間ロードを流しても二重は戻らない (揃えた形の行に入る)', async () => {
  const before = (await q('select count(*)::int as n from core.suppliers'))[0].n;
  const r = await runInitialLoad(db, buildPlanFromRender({ dataDir, log: quiet }), { log: quiet, runId: 'sup_canon_load' });
  assert.equal(r.ok, true, r.error);
  const rows = await q('select code from core.suppliers order by code');
  assert.deepEqual(rows.map((x) => x.code), ['0001', '0002', '0007', '0099', '0900', '12345', '9999', 'abc']);   // 増えたのは共有マスタの 0002 だけ
  assert.equal(rows.length, before + 1);
  assert.equal((await q("select count(*)::int as n from core.suppliers group by core.canonical_supplier_code(code) having count(*) > 1")).length, 0);
});

await pg.close();
try { fs.rmSync(dataDir, { recursive: true, force: true }); } catch { /* Windows は OS に任せる */ }
console.log(`\n${passed} 件 PASS`);
