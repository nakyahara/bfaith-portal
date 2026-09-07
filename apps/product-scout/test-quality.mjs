import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import path from 'node:path';
import { createRequire } from 'node:module';
import Database from 'better-sqlite3';
import { createProductScoutTables } from './schema.js';
import { ingestSnapshot, ingestOwnFamilies, getLatestSnapshot, listConcepts, getOwnImport, recordDecision, getConcept, getIngestStatus } from './db.js';
import { validateSnapshot } from './validation.js';
import quality from '../../scripts/product-idea-scout/quality.cjs';

const now = () => new Date().toISOString();
const snapshot = (extra = {}) => ({ generatedAt: now(), algorithmVersion: 1, concepts: [
  { concept: '補修 × シート裁断', categoryPath: 'DIY > 補修', form: 'シート裁断', hardGate: 'pass', rank: 1, examples: [] },
], ...extra });
const memory = () => { const db = new Database(':memory:'); createProductScoutTables(db); return db; };
const family = (key, salesClass = 1, extra = {}) => ({ familyKey: key, salesClass,
  categoryPath: 'DIY > 補修', form: 'シート裁断', outcome: 'active', activeSkus: 1,
  products: [{ code: 'code-' + key }], ...extra });
const own = (families, extra = {}) => { const timestamp = now(); return { generatedAt: timestamp, sourceGeneratedAt: timestamp, algorithmVersion: 2, families, sourceUpdatedAt: extra.sourceUpdatedAt ?? extra.sourceGeneratedAt ?? timestamp, ...extra }; };

test('実例の誤分類を防ぎ、普通の原料・裁断商品を調査対象に残す', () => {
  for (const title of ['【第2類医薬品】ユーシップFRテープ', '【指定第２類医薬品】テープ']) assert.equal(quality.detectForm({ title }).amc, false);
  assert.equal(quality.detectForm({ title: '付替ブラシ', categoryPath: 'オーラルケア' }).amc, null);
  assert.equal(quality.detectForm({ title: 'DHAドロップグミ 90粒', categoryPath: '必須脂肪酸・オイル > DHA' }).amc, null);
  assert.notEqual(quality.detectForm({ title: 'DHA', categoryPath: '必須脂肪酸・オイル > DHA' }).key, 'oil');
  for (const title of ['補修テープ', '晒し布', '蒸し布', '無地 手ぬぐい']) assert.equal(quality.detectForm({ title }).key, 'sheet');
  assert.equal(quality.detectForm({ title: '重曹 粉末 食品用' }).key, 'powder');
  assert.equal(quality.detectForm({ title: 'ひまし油 100ml' }).key, 'oil');
});
test('寸法が一辺でも欠測・ゼロ・負なら小型と断定しない', () => {
  const tier = { l: 250, w: 180, h: 20, weightG: 250 };
  for (const dims of [[100, null, 10], [100, 0, 10], [100, -1, 10], [100], [100, 10, NaN]]) assert.equal(quality.isSmall({ packageMm: dims, packageWeightG: 50 }, tier), null);
  assert.equal(quality.isSmall({ packageMm: [10, 100, 100], packageWeightG: 50 }, tier), true);
  assert.equal(quality.isSmall({ packageMm: [100, 100, 30], packageWeightG: 50 }, tier), false);
});
test('再取得のASINを二重集計せず、壊れたJSONLは無視しない', () => {
  const old = { asin: 'BTEST', observedAt: '2025-01-01T00:00:00Z', title: 'old' };
  const current = { ...old, observedAt: now(), title: 'new' };
  assert.deepEqual(quality.parseProducts([current, old].map(JSON.stringify).join('\n')), [current]);
  assert.throws(() => quality.parseProducts(JSON.stringify(old) + '\n{broken'), /2行目/);
});
test('観測日欠損・古いデータ・未来日時は鮮度未確認', () => {
  for (const observedAt of [undefined, '2020-01-01T00:00:00Z', '2999-01-01T00:00:00Z']) assert.equal(quality.evidence([{ observedAt }]).freshness, 'unknown');
  assert.equal(quality.evidence([{ observedAt: now() }]).freshness, 'pass');
});
test('空要素・重複テーマ・偽のゲート通過を入力段階で拒否', () => {
  assert.throws(() => validateSnapshot(snapshot({ concepts: [null] })), /オブジェクト/);
  const p = snapshot(); p.concepts.push(p.concepts[0]); assert.throws(() => validateSnapshot(p), /重複/);
  assert.throws(() => validateSnapshot(snapshot({ algorithmVersion: 2 })), /根拠/);
});
test('再送で更新日時が新しくならず、古いデータに巻き戻らず、採否は保持', () => {
  const db = memory(); const p = snapshot();
  const r = ingestSnapshot(p, db); const first = getLatestSnapshot(db);
  const c = listConcepts({ gate: 'all' }, db)[0];
  recordDecision({ conceptId: c.concept_id, decision: 'hold', decidedBy: 'test' }, db);
  assert.equal(ingestSnapshot(p, db).snapshotId, r.snapshotId);
  assert.equal(getLatestSnapshot(db).ingested_at, first.ingested_at);
  assert.throws(() => ingestSnapshot(snapshot({ generatedAt: '2020-01-01T00:00:00Z' }), db), /古い/);
  assert.equal(getConcept(c.concept_id, db).history.length, 1);
  db.close();
});
test('自社1とAMC参考2を分離し、一部終売でも販売中に数え、不明な実績をゼロにしない', () => {
  const db = memory(); ingestSnapshot(snapshot(), db);
  ingestOwnFamilies(own([family('自社', 1, { outcome: 'shrinking', qty180: null }), family('AMC参考', 2)]), db);
  const c = listConcepts({ gate: 'all' }, db)[0];
  assert.equal(c.own_count, 1); assert.equal(c.own_active, 1); assert.equal(c.own_qty180, null);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM scout_own_families').get().n, 2);
  assert.ok(db.prepare('SELECT products_json FROM scout_own_families WHERE sales_class=1').get().products_json.includes('code-'));
  db.close();
});
test('新しい完全スナップショットにない行は表示しないが旧行は削除しない', () => {
  const db = memory(); ingestSnapshot(snapshot(), db);
  const t = new Date(Date.now() - 1000).toISOString();
  ingestOwnFamilies(own([family('前回だけ')], { generatedAt: t, sourceGeneratedAt: t }), db);
  ingestOwnFamilies(own([]), db);
  assert.equal(listConcepts({ gate: 'all' }, db)[0].own_count, null);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM scout_own_families').get().n, 1);
  assert.equal(getOwnImport(db).family_count, 0); db.close();
});
test('古い自社データと分類不明の旧データは自社照合の根拠にしない', () => {
  const db = memory(); ingestSnapshot(snapshot(), db);
  ingestOwnFamilies(own([family('古い')], { sourceGeneratedAt: '2020-01-01T00:00:00Z' }), db);
  assert.equal(listConcepts({ gate: 'all' }, db)[0].own_count, null);
  assert.throws(() => ingestOwnFamilies(own([family('再送')], { generatedAt: '2020-01-01T00:00:00Z', sourceGeneratedAt: '2020-01-01T00:00:00Z' }), db), /古い/);
  db.close();
});
test('旧スキーマの移行は行を保持し、二度実行できる', () => {
  const db = new Database(':memory:');
  db.exec("CREATE TABLE scout_own_families (family_key TEXT PRIMARY KEY,concept_id TEXT,outcome TEXT); INSERT INTO scout_own_families VALUES ('old',NULL,'active')");
  createProductScoutTables(db); createProductScoutTables(db);
  assert.equal(db.prepare('SELECT sales_class FROM scout_own_families').get().sales_class, null);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM scout_own_families').get().n, 1); db.close();
});
test('自社抽出は発売日欠損・セット・例外を含め、分類を混ぜず旧ASIN表に依存しない', () => {
  const db = new Database(':memory:');
  db.exec(`CREATE TABLE m_products (商品コード TEXT, 商品名 TEXT, 取扱区分 TEXT, 標準売価 REAL, 原価 REAL,
    売上分類 INTEGER, new_product_launch_date TEXT, 商品区分 TEXT, 仕入先コード TEXT, 原価状態 TEXT, 送料 REAL, updated_at TEXT);
    CREATE TABLE amazon_sku_fees (seller_sku TEXT, asin TEXT);
    CREATE TABLE m_sku_components (seller_sku TEXT, ne_code TEXT);
    CREATE TABLE f_sales_by_product (商品コード TEXT, 数量 REAL, 日付 TEXT);
    INSERT INTO m_products VALUES ('a','同名','取扱中',500,NULL,1,NULL,'単品','supplier','UNKNOWN',NULL,'2026-09-07');
    INSERT INTO m_products VALUES ('b','同名','取扱中',500,100,2,NULL,'単品','supplier','OK',100,'2026-09-07');
    INSERT INTO m_products VALUES ('c','セット','取扱中',900,200,1,NULL,'セット',NULL,'OK',100,'2026-09-07');
    INSERT INTO m_products VALUES ('d','例外','取扱中',900,200,1,NULL,'例外',NULL,'OK',100,'2026-09-07');
    INSERT INTO amazon_sku_fees VALUES ('seller-a','BEXACT');
    INSERT INTO m_sku_components VALUES ('seller-a','a');`);
  let result;
  const require = createRequire(import.meta.url);
  const file = path.resolve('scripts/export-own-products.cjs');
  vm.runInNewContext(fs.readFileSync(file, 'utf8'), {
    require(name) {
      if (name === 'better-sqlite3') return function () { return db; };
      if (name === 'fs') return { existsSync: () => true, mkdirSync() {}, writeFileSync(file, text) { result = JSON.parse(text); } };
      return require(name);
    }, __dirname: path.dirname(file), process: { env: {}, argv: [] }, console: { log() {} },
  });
  assert.equal(result.skuCount, 4); assert.equal(result.families.length, 4);
  const f = result.families.find(f => f.salesClass === 1 && f.familyKey === '同名');
  assert.deepEqual(Array.from(f.asins), ['BEXACT']); assert.equal(f.products[0].code, 'a'); assert.equal(f.qty180, null);
  assert.ok(result.families.some(f => f.salesClass === 2));
});

test('同じファミリーの再取り込みで分類・商品コード・最新所属を更新する', () => {
  const db = memory(); ingestSnapshot(snapshot(), db);
  const old = new Date(Date.now()-1000).toISOString();
  ingestOwnFamilies(own([family('same', 2)], {generatedAt:old,sourceGeneratedAt:old}), db);
  ingestOwnFamilies(own([family('same', 1, { products: [{code:'new-code'}] })]), db);
  const c=listConcepts({gate:'all'},db)[0]; assert.equal(c.own_count,1);
  const f=db.prepare('SELECT * FROM scout_own_families').get();
  assert.equal(f.sales_class,1); assert.equal(JSON.parse(f.products_json)[0].code,'new-code');
  assert.equal(f.own_batch_id,getOwnImport(db).batch_id); db.close();
});

test('手動再取得は上限件数と対象ASINを守り、新しい観測を取り直さない', () => {
  const rows=[{asin:'old'}, {asin:'fresh',observedAt:now()}, {asin:'outside'}];
  assert.deepEqual(quality.refreshTargets(rows,['old','fresh'],1),['old']);
  assert.throws(()=>quality.refreshTargets(rows,['old'],1001),/1〜1000/);
});

test('同じ親ASINの購入表示を重複加算しない', () => {
  assert.equal(quality.purchaseSignal([{asin:'a',parentAsin:'parent',monthlySold:100},
    {asin:'b',parentAsin:'parent',monthlySold:200},{asin:'c',monthlySold:50}]),250);
});

test('JSONを書き直しても元の商品DBが古ければ自社照合を新鮮としない', () => {
  const db=memory(); ingestSnapshot(snapshot(),db);
  ingestOwnFamilies(own([family('stale-db')], {sourceUpdatedAt:'2020-01-01T00:00:00Z'}),db);
  assert.equal(listConcepts({gate:'all'},db)[0].own_count,null); db.close();
});

test('既存タスクの新ランナーはCRLFで保存し、worktreeコードと本番データを明示する', () => {
  const bat=fs.readFileSync('scripts/product-idea-scout/run-products.bat','utf8');
  assert.ok(bat.includes('\r\n')); assert.ok(!/(?<!\r)\n/.test(bat));
  for (const file of ['own.js','products.js','concepts.js']) assert.ok(bat.includes('node "%SCOUT_CODE_ROOT%'+file+'"'));
  assert.ok(bat.includes('pushd "%SCOUT_PORTAL_ROOT%"')); assert.ok(bat.includes('set "WAREHOUSE_DB='));
  assert.ok(bat.includes('-Id product-idea-scout -Status ok'));
});

test('反映確認は現行版・件数・採否件数のみを返し商品明細を返さない', () => {
  const db=memory(); ingestSnapshot(snapshot(),db);
  ingestOwnFamilies(own([family('sample')]),db);
  const status=getIngestStatus(db);
  assert.equal(status.qualityVersion,2);assert.equal(status.ownCounts[0].families,1);
  assert.equal(status.decisionCount,0);assert.ok(!JSON.stringify(status).includes('code-sample'));db.close();
});
