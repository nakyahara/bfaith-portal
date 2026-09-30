/**
 * test-master-set-rules.mjs — セットの導く値の規則 (lib/master-set-rules.js) の試験 (Company DB構想 14 §6 ⑤-1・Codex ⑤-R0 Medium 8)
 *
 * 固定する契約:
 *   1 rebuild-m-products.js は規則を lib から export し直しているだけ (同じ関数) = 規則は 1 か所
 *   2 Company DB の形の規則 (deriveSet*Cdb) は、作り直し (NE の形) の答えを夜間ロードの写し方 (sources.mjs の mapHandling / mapTaxClass) で写したものと同じ
 *   3 deriveSetCdb の決まった例 (輸出 4 の混在・税率の混在・未入力・中止の構成品・数量 1 の構成品が無い = 何も言わない・上書き・例外原価)
 *   4 構成が同じか (compositionEquals) = 構成品・数量・並び・行の数まで。並びが決められない = 同じと言わない
 *   5 作り直しのセットの原価 (共用の setCostFromComponents に変えた) は今までと同じ: 全部そろう = 合計 (小数 2 桁) / 一部 = PARTIAL / 無い = MISSING / 例外 = OVERRIDDEN
 * 使い方: node scripts/test-master-set-rules.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

// ★ db.js は import 時に DATA_DIR を読むため、動的 import より前に設定する
const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'master-set-rules-test-'));
process.env.DATA_DIR = tmpDir;

const L = await import('../lib/master-set-rules.js');
const RB = await import('../apps/warehouse/rebuild-m-products.js');
const { mapHandling, mapTaxClass } = await import('../apps/company-db/load/sources.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }

await ta('[1] rebuild-m-products.js の規則は lib と同じ関数 (export し直しただけ)', () => {
  for (const k of ['TAX_RATES', 'KNOWN_NE_RATES', 'KNOWN_DECIMAL_RATES', 'resolveTaxRate', 'resolveSetTaxRate', 'SALES_CLASSES', 'EXPORT_SALES_CLASS',
    'resolveSetSalesClass', 'HANDLING_ACTIVE', 'HANDLING_STOPPED', 'HANDLING_MAKER_STOPPED', 'resolveSetHandlingClass']) {
    assert.ok(RB[k] !== undefined, `${k} が rebuild に無い`);
    assert.equal(RB[k], L[k], `${k} が別もの`);
  }
});

await ta('[2] 税率: Company DB の形 = 作り直しの答えを夜間ロードの写し方で写したもの (全部の組み合わせ)', () => {
  const rates = [0.08, 0.1, null];
  const combos = [[]];
  for (let n = 1; n <= 3; n++) for (const base of combos.filter((c) => c.length === n - 1)) for (const r of rates) combos.push([...base, r]);
  for (const c of combos) {
    const cdb = L.deriveSetTaxCdb(c);
    // 作り直し: 構成品の NE の税率 (整数)。NE が未登録 (0) のときは手動の小数
    const ne = L.resolveSetTaxRate(c.map((r) => ({ neTaxRate: r == null ? 0 : Math.round(r * 100), manualTaxRate: undefined, componentExists: true })));
    assert.deepEqual([cdb.taxRate, cdb.taxClass], [ne.taxRate, mapTaxClass(ne.taxCategory, ne.taxRate)], JSON.stringify(c));
  }
  assert.deepEqual(L.deriveSetTaxCdb(['0.10', '0.08']), { taxRate: 0.08, taxClass: 'MIXED' });   // Postgres の numeric は文字
});

await ta('[2] 取扱区分: セット自身が決まっている = 作り直しの答え (mapHandling で写す)。決まっていない = 止まっているセットを戻さない', () => {
  const H = { active: '取扱中', discontinued: '取扱中止', unknown: null };
  const hs = ['active', 'discontinued', 'unknown'];
  for (const own of ['active', 'discontinued']) {
    for (const a of hs) for (const b of hs) {
      const cdb = L.deriveSetHandlingCdb(own, [a, b], 'active');
      const ne = L.resolveSetHandlingClass(H[own], [a, b].map((x) => ({ handlingClass: H[x], componentExists: true })));
      assert.equal(cdb, mapHandling(ne), `${own} ${a} ${b}`);
    }
  }
  assert.equal(L.deriveSetHandlingCdb(null, ['active', 'discontinued'], 'active'), 'discontinued');
  assert.equal(L.deriveSetHandlingCdb(null, ['active', 'active'], 'discontinued'), 'discontinued');   // 戻さない
  assert.equal(L.deriveSetHandlingCdb(null, ['active', 'unknown'], 'active'), 'active');
  assert.equal(L.deriveSetHandlingCdb(null, [], null), 'unknown');
});

await ta('[3] deriveSetCdb の決まった例: 輸出 4 の混在・税率の混在・未入力・中止の構成品・数量 1 が無い・上書き・例外原価', () => {
  const c = (code, qty, tax, sales, cost, handling = 'active') => ({ code, qty, tax_rate: tax, sales_class: sales, cost_jpy: cost, handling });
  // ふつう: 全部決まる
  let d = L.deriveSetCdb([c('a', 1, 0.1, 3, 100), c('b', 2, 0.1, 1, 50)], { handlingOwn: 'active' });
  assert.deepEqual([d.tax, d.salesClass, d.salesSource, d.handling, d.cost, d.blockers, d.warnings],
    [{ taxRate: 0.1, taxClass: 'STANDARD_10' }, 1, 'components', 'active', { status: 'COMPLETE', jpy: 200 }, [], []]);
  // 輸出 4 と 1〜3 の混在 = 分類が決まらない (MIN にしない)
  d = L.deriveSetCdb([c('a', 1, 0.1, 4, 100), c('b', 1, 0.1, 2, 50)]);
  assert.equal(d.salesClass, null); assert.match(d.blockers.join(), /輸出 4 と 1〜3/);
  d = L.deriveSetCdb([c('a', 1, 0.1, 4, 100), c('b', 1, 0.1, 4, 50)]);
  assert.equal(d.salesClass, 4); assert.deepEqual(d.blockers, []);
  d = L.deriveSetCdb([c('a', 1, 0.1, 4, 100), c('b', 1, 0.1, 2, 50)], { override: 2 });
  assert.deepEqual([d.salesClass, d.salesSource, d.blockers], [2, 'override', []]);
  // 税率の混在 = 決まる (低い方・MIXED) + 気をつけること
  d = L.deriveSetCdb([c('a', 1, 0.1, 3, 100), c('b', 1, 0.08, 3, 50)]);
  assert.deepEqual(d.tax, { taxRate: 0.08, taxClass: 'MIXED' }); assert.deepEqual(d.blockers, []); assert.match(d.warnings.join(), /8% と 10%/);
  // 未入力: 税率 (上書きなし) / 分類 / 原価 (例外原価で通る)
  d = L.deriveSetCdb([c('a', 1, null, null, null), c('b', 1, 0.1, 3, 0)]);
  assert.equal(d.blockers.length, 3);
  assert.match(d.blockers[0], /a の税率/); assert.match(d.blockers[1], /未入力: a/); assert.match(d.blockers[2], /a・b の原価/);
  d = L.deriveSetCdb([c('a', 1, 0.1, null, null)], { override: 3, exceptionCost: true });
  assert.deepEqual(d.blockers, []);
  // 中止の構成品 = セットも中止 + 気をつけること
  d = L.deriveSetCdb([c('a', 1, 0.1, 3, 100), c('b', 1, 0.1, 3, 50, 'discontinued')], { handlingOwn: 'active' });
  assert.equal(d.handling, 'discontinued'); assert.match(d.warnings.join(), /中止の構成品 \(b\)/);
  // 数量 1 の構成品が無い / 先頭でない = NE の決まりではない (中原さん 2026-10-01) = 止めない・気をつけることも出さない
  d = L.deriveSetCdb([c('a', 2, 0.1, 3, 100)], { handlingOwn: 'active' });
  assert.deepEqual([d.blockers, d.warnings], [[], []]);
  d = L.deriveSetCdb([c('a', 2, 0.1, 3, 100), c('b', 1, 0.1, 3, 100)], { handlingOwn: 'active' });
  assert.deepEqual([d.blockers, d.warnings], [[], []]);
  // 構成品なし
  d = L.deriveSetCdb([]);
  assert.match(d.blockers.join(), /構成品がありません/);
});

await ta('[4] 構成が同じか = 構成品・数量・並び・行の数まで。並びが決められない = 同じと言わない', () => {
  const r = (key, qty, sort) => ({ key, qty, sort });
  assert.equal(L.compositionEquals([r(1, 2, 1), r(2, 1, 2)], [r('1', 2, 10), r('2', 1, 20)]), true);   // sort の値そのものは見ない (順だけ)
  assert.equal(L.compositionEquals([r(1, 2, 1), r(2, 1, 2)], [r(2, 1, 1), r(1, 2, 2)]), false);        // 並びが違う
  assert.equal(L.compositionEquals([r(1, 2, 1)], [r(1, 3, 1)]), false);                                 // 数量が違う
  assert.equal(L.compositionEquals([r(1, 2, 1)], [r(1, 2, 1), r(2, 1, 2)]), false);                     // 行が多い
  assert.equal(L.compositionEquals([r(1, 2, 1), r(2, 1, 2)], [r(1, 2, 1)]), false);                     // 行が足りない
  assert.equal(L.compositionEquals([r(1, 2, 1), r(2, 1, 1)], [r(1, 2, 1), r(2, 1, 1)]), false);         // 同じ sort = 決められない
  assert.equal(L.compositionEquals([r(1, 2, 'x')], [r(1, 2, 'x')]), false);
  assert.equal(L.compositionEquals([], []), true);
});

await ta('[5] setCostFromComponents = 作り直しの今までの決め方 (全部 > 0 = 合計・小数 2 桁 / 一部 = PARTIAL / 無い = MISSING / 数量の空は 1)', () => {
  assert.deepEqual(L.setCostFromComponents([{ cost: 100.123, qty: 2 }, { cost: 50, qty: null }]), { status: 'COMPLETE', jpy: 250.25 });
  assert.deepEqual(L.setCostFromComponents([{ cost: 100, qty: 1 }, { cost: 0, qty: 1 }]), { status: 'PARTIAL', jpy: null });
  assert.deepEqual(L.setCostFromComponents([{ cost: null, qty: 1 }]), { status: 'MISSING', jpy: null });
  assert.deepEqual(L.setCostFromComponents([]), { status: 'MISSING', jpy: null });
  assert.deepEqual(L.deriveSetCostCdb([{ costJpy: '100', qty: 2 }, { costJpy: 51, qty: 1 }]), { status: 'COMPLETE', jpy: 251 });
});

await ta('[5] 作り直し (rebuildMProducts) のセットの原価は今までと同じ (COMPLETE / PARTIAL / MISSING / 例外)', async () => {
  const { initDB, getDB } = await import('../apps/warehouse/db.js');
  await initDB();
  const db = getDB();
  const NOW = '2026-09-30 12:00:00';
  const insNe = db.prepare(`INSERT OR REPLACE INTO raw_ne_products (商品コード, 商品名, 原価, 売価, 取扱区分, 在庫数, 引当数, 消費税率, 作成日, synced_at) VALUES (?, ?, ?, ?, '取扱中', 0, 0, 10, '2026-01-01', ?)`);
  const insSet = db.prepare(`INSERT OR REPLACE INTO raw_ne_set_products (セット商品コード, セット商品名, セット販売価格, 商品コード, 数量, synced_at) VALUES (?, ?, ?, ?, ?, ?)`);
  db.transaction(() => { for (let i = 0; i < 3200; i++) insNe.run(`filler-${i}`, `ダミー${i}`, 100, 200, NOW); })();   // 品質ゲート (3,000 件) を通す
  insNe.run('c1', '構成 1', 100.5, 300, NOW);
  insNe.run('c2', '構成 2', 40, 100, NOW);
  insNe.run('c0', '原価なし', 0, 100, NOW);
  for (const [set, code, qty] of [['st-full', 'c1', 2], ['st-full', 'c2', 1], ['st-part', 'c1', 1], ['st-part', 'c0', 1], ['st-miss', 'c0', 2], ['st-exc', 'c1', 1]]) insSet.run(set, set, 1000, code, qty, NOW);
  db.prepare(`INSERT OR REPLACE INTO exception_genka (sku, genka, 商品名, synced_at) VALUES ('st-exc', 77, '', ?)`).run(NOW);
  const { rebuildMProducts } = RB;
  const r = await rebuildMProducts();
  assert.ok(r === undefined || r === null || r.ok !== false, JSON.stringify(r));
  const got = Object.fromEntries(db.prepare(`SELECT 商品コード AS c, 原価 AS g, 原価ソース AS s, 原価状態 AS st FROM m_products WHERE 商品コード LIKE 'st-%'`).all().map((x) => [x.c, [x.g, x.s, x.st]]));
  assert.deepEqual(got, {
    'st-full': [241, 'セット計算', 'COMPLETE'],   // 100.5 × 2 + 40
    'st-part': [null, '不明', 'PARTIAL'],
    'st-miss': [null, '不明', 'MISSING'],
    'st-exc': [77, '例外', 'OVERRIDDEN'],
  });
});

try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* Windows で開いたままのことがある */ }
console.log(`\n${passed} 件 ok`);
