/**
 * test-price-calc.mjs — 「売価を変えて試算」の入力 (price-calc.js) の試験
 *
 * 式は商品ハブの基本情報タブと全く同じ (中原さん指定 2026-09-14)。
 * ここでは「商品ハブと同じ入力が、出品の品番そのものの行から取れているか」を固定する。
 *
 * 実行: node apps/profit-analysis/test-price-calc.mjs
 */
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { priceCalcInputs, DEFAULT_TAX_PERCENT } from './price-calc.js';
import { computeProfit, TAKE_RATE } from '../product-hub/lib/profit.js';

let passed = 0;
function t(name, fn) {
  try { fn(); passed++; console.log(`  ok  ${name}`); }
  catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; }
}

const db = new Database(':memory:');
db.exec(`CREATE TABLE mirror_products (
  商品コード TEXT, 商品名 TEXT, 原価 REAL, 送料 REAL, 配送方法 TEXT, 消費税率 REAL, 取扱区分 TEXT, 代表商品コード TEXT)`);
const ins = db.prepare('INSERT INTO mirror_products VALUES (?, ?, ?, ?, ?, ?, ?, ?)');
ins.run('abc', '商品A', 660, 237, 'ネコポス', 0.1, '取扱中', null);
ins.run('abc-2', '商品A 2個', 1320, 237, 'ネコポス', 0.1, '取扱中', null);
ins.run('food', '食品', 2100, 700, '宅急便60', 0.08, 'ﾒｰｶｰ取扱中止', null);
ins.run('big', '大きい', 1000, 945, '宅急便100', 0.1, '取扱中', null);
ins.run('notax', '税率なし', 500, 300, 'ネコポス', null, '取扱中', null);
ins.run('nocost', '原価なし', null, 300, 'ネコポス', 0.1, '取扱中', null);
ins.run('noship', '送料なし', 500, null, null, 0.1, '取扱中', null);
ins.run('nomethod', '配送方法なし', 500, 410, null, 0.1, '取扱中', null);
ins.run('zero', '原価0', 0, 237, 'ネコポス', 0.1, '取扱中', null);
ins.run('Dup', '重複A', 100, 237, 'ネコポス', 0.1, '取扱中', null);
ins.run('dup', '重複B', 200, 237, 'ネコポス', 0.1, '取扱中', null);
// バリエーションの子 (代表商品コード = fam)。商品ハブの基本情報は子を集計するが、ここは子そのものを見る
ins.run('fam-red', '赤', 800, 237, 'ネコポス', 0.1, '取扱中', 'fam');
ins.run('fam-blue', '青', 900, 237, 'ネコポス', 0.1, '取扱中', 'fam');

console.log('priceCalcInputs');

t('[!] NE の原価・送料・税率・配送方法を出品の品番の行から返す', () => {
  const r = priceCalcInputs(db, 'abc');
  assert.equal(r.found, true);
  assert.equal(r.ne_code, 'abc');
  assert.equal(r.cost_ex_tax, 660);
  assert.equal(r.shipping_cost, 237);
  assert.equal(r.shipping_method, 'ネコポス');
  assert.equal(r.tax_percent, 10);
  assert.equal(r.tax_source, 'ne');
  assert.equal(r.take_rate, TAKE_RATE, '手数料等の割合は商品ハブの正本 (profit.js) から取る');
});

t('[!] 配送方法の選択肢は NE の登録値 (実際の送料) が選ばれた状態で入っている', () => {
  const r = priceCalcInputs(db, 'food');
  const cur = r.ship_choices.filter(o => o.isCurrent);
  assert.equal(cur.length, 1);
  assert.equal(cur[0].method, '宅急便60');
  assert.equal(cur[0].cost, 700, '登録値の送料が代表送料に置き換わっている');
  assert.ok(r.ship_choices.some(o => o.method === '宅急便100' && o.cost === 945), '他の配送方法も選べる');
  // 送料の安い順 (商品ハブと同じ並び)
  const costs = r.ship_choices.filter(o => !o.isCurrent).map(o => o.cost);
  assert.deepEqual(costs, [...costs].sort((a, b) => a - b));
});

t('[!] 配送方法名が無くても送料があれば、その送料で計算できる', () => {
  const r = priceCalcInputs(db, 'nomethod');
  const cur = r.ship_choices.find(o => o.isCurrent);
  assert.ok(cur, '登録送料が選択肢に無い');
  assert.equal(cur.cost, 410);
});

t('税率 0.08 は 8%', () => {
  assert.equal(priceCalcInputs(db, 'food').tax_percent, 8);
});

t('[!] NE に税率が無ければ 10% (商品ハブの既定値と同じ) で、そう分かるように返す', () => {
  const r = priceCalcInputs(db, 'notax');
  assert.equal(r.tax_percent, DEFAULT_TAX_PERCENT);
  assert.equal(r.tax_source, 'default');
});

t('[!] 原価が無いものは null のまま (0 円にしない)', () => {
  assert.equal(priceCalcInputs(db, 'nocost').cost_ex_tax, null);
});

t('[!] 送料が無いものは null のまま・選択肢も出さない (商品ハブは計算しない)', () => {
  const r = priceCalcInputs(db, 'noship');
  assert.equal(r.shipping_cost, null);
  assert.deepEqual(r.ship_choices, []);
});

t('原価 0 は 0 として返す (商品ハブの読み方と同じ)', () => {
  assert.equal(priceCalcInputs(db, 'zero').cost_ex_tax, 0);
});

t('品番は前後の空白・大文字小文字を無視して引く', () => {
  assert.equal(priceCalcInputs(db, '  ABC ').ne_code, 'abc');
});

t('[!] バリエーションの子は、子そのものの原価を返す (家族で集計しない)', () => {
  assert.equal(priceCalcInputs(db, 'fam-red').cost_ex_tax, 800);
  assert.equal(priceCalcInputs(db, 'fam-blue').cost_ex_tax, 900);
});

t('[!] 大文字小文字だけ違う品番が 2 つあれば、どちらの原価か決めずに返す', () => {
  const r = priceCalcInputs(db, 'dup');
  assert.equal(r.found, false);
  assert.equal(r.reason, 'ambiguous');
});

t('無い品番・空の品番', () => {
  assert.equal(priceCalcInputs(db, 'nosuch').reason, 'not_found');
  assert.equal(priceCalcInputs(db, '  ').reason, 'no_code');
});

t('mirror_products が無い DB でも落ちない', () => {
  const empty = new Database(':memory:');
  assert.equal(priceCalcInputs(empty, 'abc').reason, 'mirror_missing');
  empty.close();
});

t('[!] 返した入力を商品ハブの computeProfit に通すと、商品ハブの試験と同じ数字になる', () => {
  // 商品ハブ smoke.mjs と同じ例: 1280円 / 原価660 / 税10% / 送料237 → 利益 189 / 14.8%
  const r = priceCalcInputs(db, 'abc');
  const p = computeProfit({ price: 1280, costExTax: r.cost_ex_tax, taxPercent: r.tax_percent, shippingCost: r.shipping_cost });
  assert.deepEqual(p, { profit: 189, marginPct: 14.8, costIncTax: 726 });
});

db.close();
console.log(`\n${passed} 件 PASS`);
