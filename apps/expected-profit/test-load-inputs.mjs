/**
 * test-load-inputs.mjs — warehouse からの入力読み出し 受入試験
 *
 * 🚨 ここは「本番コードを通す」ためのテスト。
 *    実データ検証 (2026-09-07) で loadSkuMap が v_sku_resolved.数量 を
 *    読み捨てていたが、他のテストは skuMap を手で作っていたので気づけなかった。
 *    実際に SQLite を作って本番の SQL を通す。
 *
 * 実行: node apps/expected-profit/test-load-inputs.mjs
 */
import assert from 'node:assert/strict';
import fs from 'fs';
import path from 'path';
import os from 'os';
import Database from 'better-sqlite3';

const { loadSkuMap, normalizeQty, loadProducts, loadShippingRates } = await import('./load-inputs.js');

let passed = 0;
function t(name, fn) {
  try { fn(); passed++; console.log(`  ok  ${name}`); }
  catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; }
}

const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ep-load-'));
const wdb = new Database(path.join(dir, 'w.db'));

// 実物と同じ列名で作る (列名がズレたら本番の SQL が落ちる = 検出したい)
wdb.exec(`CREATE TABLE v_sku_resolved (seller_sku TEXT, ne_code TEXT, 数量 INTEGER, sort_order INTEGER, source TEXT)`);
wdb.exec(`CREATE TABLE f_rakuten_sku_map (rakuten_code TEXT, ne_code TEXT, source TEXT, updated_at TEXT, manage_number TEXT)`);
wdb.prepare('INSERT INTO v_sku_resolved VALUES (?,?,?,?,?)').run('PR_A_0001', 'OPBS454', 12, 0, 'master');
wdb.prepare('INSERT INTO v_sku_resolved VALUES (?,?,?,?,?)').run('b010', 'cobon525', 1, 0, 'master');
wdb.prepare('INSERT INTO v_sku_resolved VALUES (?,?,?,?,?)').run('bad', 'x1', 0, 0, 'master');
wdb.prepare('INSERT INTO v_sku_resolved VALUES (?,?,?,?,?)').run('nul', 'x2', null, 0, 'master');
// 1SKU が複数 NE を指す (multiple_ne_codes になるべき)
wdb.prepare('INSERT INTO v_sku_resolved VALUES (?,?,?,?,?)').run('multi', 'n1', 2, 0, 'master');
wdb.prepare('INSERT INTO v_sku_resolved VALUES (?,?,?,?,?)').run('multi', 'n2', 1, 1, 'master');
wdb.prepare('INSERT INTO f_rakuten_sku_map VALUES (?,?,?,?,?)').run('RAK-1', 'NE-R1', 'master', null, 'item1');

console.log('Amazon の SKU マップ');

t('[!] 数量 を落とさずに読む (まとめ買いSKU の原価を数量倍するために要る)', () => {
  const m = loadSkuMap(wdb, 'amazon');
  assert.equal(m.get('pr_a_0001')[0].qty, 12);
  assert.equal(m.get('pr_a_0001')[0].ne_code, 'opbs454');
});

t('数量1 はそのまま 1 で返る (null にしない)', () => {
  assert.equal(loadSkuMap(wdb, 'amazon').get('b010')[0].qty, 1);
});

t('[!] 数量が 0 / NULL の行は「数量不明」= null にする (1 で埋めない)', () => {
  const m = loadSkuMap(wdb, 'amazon');
  assert.equal(m.get('bad')[0].qty, null);
  assert.equal(m.get('nul')[0].qty, null);
});

t('キーは小文字に揃える (レポートの SKU と大小が違う)', () => {
  const m = loadSkuMap(wdb, 'amazon');
  assert.ok(m.has('pr_a_0001'));
  assert.ok(!m.has('PR_A_0001'));
});

t('1SKU が複数 NE を指すときは両方保持する (呼び出し側が ambiguous と判定する)', () => {
  assert.equal(loadSkuMap(wdb, 'amazon').get('multi').length, 2);
});

console.log('');
console.log('楽天の SKU マップ');

t('[!] 楽天の対応表には数量列が無いので qty は null (実測)', () => {
  const m = loadSkuMap(wdb, 'rakuten');
  assert.equal(m.get('rak-1')[0].ne_code, 'ne-r1');
  assert.equal(m.get('rak-1')[0].qty, null);
});

t('対応表がまだ無い環境でも落ちない (空マップ)', () => {
  const empty = new Database(path.join(dir, 'empty.db'));
  assert.equal(loadSkuMap(empty, 'rakuten').size, 0);
  empty.close();
});

console.log('');
console.log('数量の正規化');

t('normalizeQty は 1以上の整数だけ通す', () => {
  assert.equal(normalizeQty(12), 12);
  assert.equal(normalizeQty('3'), 3);
  assert.equal(normalizeQty(0), null);
  assert.equal(normalizeQty(1.5), null);
  assert.equal(normalizeQty(null), null);
});

console.log('');
console.log('他の読み出しも本番の SQL を通す');

t('商品マスタと配送マスタが実 DB から読める (列名のズレを検出する)', () => {
  wdb.exec(`CREATE TABLE m_products (商品コード TEXT, 商品名 TEXT, 原価 REAL, 原価ソース TEXT,
    原価状態 TEXT, 消費税率 REAL, 送料コード TEXT, 配送方法 TEXT, 売上分類 INTEGER, 標準売価 REAL,
    取扱区分 TEXT, 商品区分 TEXT, 在庫数 INTEGER, 引当数 INTEGER, 仕入先コード TEXT,
    セット構成品数 INTEGER, updated_at TEXT, 税区分 TEXT, 送料 REAL, seasonality_flag INTEGER,
    season_months TEXT, new_product_flag INTEGER, new_product_launch_date TEXT, product_id INTEGER)`);
  wdb.prepare(`INSERT INTO m_products (商品コード, 商品名, 原価, 原価ソース, 原価状態, 消費税率,
    送料コード, 配送方法, 売上分類) VALUES (?,?,?,?,?,?,?,?,?)`)
    .run('OPBS454', 'ピーナッツバター', 1001, 'NE', 'COMPLETE', 0.1, '501', 'ネコポス', 3);
  // 実物の列名 (miniPC 実測 2026-09-07)。日本語列と英語列が混在しているので写し間違えやすい
  wdb.exec(`CREATE TABLE shipping_rates (shipping_code TEXT, 大分類区分 TEXT, 運送会社 TEXT,
    小分類区分名称 TEXT, 梱包サイズ TEXT, 最大重量 REAL, 追跡有無 TEXT, 送料 REAL, 出荷作業料 REAL,
    想定梱包資材費 REAL, 想定人件費 REAL, 配送関係費合計 REAL, 備考 TEXT, synced_at TEXT)`);
  wdb.prepare(`INSERT INTO shipping_rates (shipping_code, 大分類区分, 小分類区分名称, 送料,
    出荷作業料, 想定梱包資材費, 想定人件費, 配送関係費合計) VALUES (?,?,?,?,?,?,?,?)`)
    .run('501', 'ネコポス', 'ネコポス', 198, 20, 10, 9, 237);

  const products = loadProducts(wdb);
  assert.equal(products.get('opbs454').原価, 1001);
  const rates = loadShippingRates(wdb);
  assert.equal(rates.get('501').送料, 198);
});

wdb.close();
fs.rmSync(dir, { recursive: true, force: true });
console.log(`\n${passed} 件 PASS`);
