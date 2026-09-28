/**
 * test-fba-daily-cap.mjs — FBA 補充「決まりの変更 v3-4 1 日の上限」の受入試験
 *
 * 中原さん 9/28: 1 日 6,000 個・120 SKU、超えたら欠品が近い順に残す (残りは翌日)。
 *   - 提案として記録できる行を 在庫日数の短い順 (同じなら 30 日販売の多い順・早めに送る分は最後) に積む
 *   - 境目の SKU は残りの枠が最低出荷日数以上なら枠まで (入数の丸めは切り下げ)、足りなければ翌日へ
 *   - 保留・恒久除外は数えない・触らない / 上限以内は何もしない / off で止める / v2 には効かない
 * 使い方: node scripts/test-fba-daily-cap.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'fba-daily-cap-'));
const db = await import('../apps/fba-replenishment/db.js');
await db.initDb();
const { applyDailyCap, generateRecommendations } = await import('../apps/fba-replenishment/calculation-engine.js');
const { calmReason, rationaleOf, pickDraftRows, inputsOf } = await import('../apps/fba-replenishment/shadow-draft.mjs');
const { compareRuleResults } = await import('../apps/fba-replenishment/decision-job.js');

let passed = 0;
function t(name, fn) { try { fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack}`); process.exitCode = 1; } }

const it = (sku, qty, dos, o = {}) => ({
  amazon_sku: sku, ne_code: sku, adjusted_qty: qty, recommended_qty: qty, days_of_supply: dos, daily_sales: 100, units_sold_30d: 3000,
  needs_replenishment: true, data_gaps: {}, ...o,
});
const S = (o = {}) => ({ v3_daily_cap_units: '6000', v3_daily_cap_skus: '120', min_shipment_cover_days: '7', round_unit: '5', round_threshold: '20', ...o });
const qtys = (items) => Object.fromEntries(items.map((i) => [i.amazon_sku, i.adjusted_qty]));

console.log('1 日の上限');
t('上限以内なら何もしない', () => {
  const items = [it('a', 3000, 5), it('b', 2000, 10)];
  const r = applyDailyCap(items, S());
  assert.deepEqual([r.reason, r.before_units, r.after_units], ['within', 5000, 5000]);
  assert.deepEqual(qtys(items), { a: 3000, b: 2000 });
});

t('個数の上限: 在庫日数の短い順に 6,000 個まで残し、残りはその日 0 (翌日へ)', () => {
  const items = [it('d30', 3000, 30), it('d5', 3000, 5), it('d20', 3000, 20), it('d10', 3000, 10)];
  const r = applyDailyCap(items, S());
  assert.deepEqual(qtys(items), { d30: 0, d5: 3000, d20: 0, d10: 3000 });
  assert.deepEqual([r.reason, r.after_units, r.after_skus, r.deferred_skus, r.deferred_units], ['capped', 6000, 2, 2, 6000]);
  assert.deepEqual(items.find((i) => i.amazon_sku === 'd20').daily_cap, { before: 3000, after: 0 });
  assert.deepEqual(r.deferred_top.map((d) => d.sku), ['d20', 'd30']);
});

t('境目の SKU: 残りの枠が最低出荷日数以上なら枠まで (丸めは切り下げ 2,003 → 2,000)。足りなければ翌日へ', () => {
  const items = [it('a', 3997, 5), it('b', 3000, 10), it('c', 500, 20)];
  const r = applyDailyCap(items, S());
  assert.deepEqual(qtys(items), { a: 3997, b: 2000, c: 0 });
  assert.deepEqual(items[1].daily_cap, { before: 3000, after: 2000 });
  assert.equal(r.partial, 1);
  const items2 = [it('a', 5700, 5), it('b', 3000, 10, { daily_sales: 100 })];   // 残り 300 = 3 日分 < 7
  applyDailyCap(items2, S());
  assert.deepEqual(qtys(items2), { a: 5700, b: 0 });
});

t('SKU 数の上限: 個数に余裕があっても 120 SKU (ここでは 2) で止める', () => {
  const items = [it('a', 10, 1), it('b', 10, 2), it('c', 10, 3)];
  const r = applyDailyCap(items, S({ v3_daily_cap_skus: '2' }));
  assert.deepEqual(qtys(items), { a: 10, b: 10, c: 0 });
  assert.equal(r.after_skus, 2);
});

t('同じ在庫日数なら 30 日販売の多い方を先に・早めに送る分 (ならし) は最後', () => {
  const items = [it('slow', 4000, 10, { units_sold_30d: 100 }), it('fast', 4000, 10, { units_sold_30d: 900 }), it('pull', 1000, 1, { pull_forward: true })];
  applyDailyCap(items, S());
  assert.equal(items.find((i) => i.amazon_sku === 'fast').adjusted_qty, 4000);
  assert.equal(items.find((i) => i.amazon_sku === 'slow').adjusted_qty, 2000, '境目 (残り 2,000 = 20 日分)');
  assert.equal(items.find((i) => i.amazon_sku === 'pull').adjusted_qty, 0, '在庫日数が短くても早めに送る分は最後');
});

t('保留・恒久除外の行は数えない・触らない', () => {
  const items = [it('a', 5000, 5), it('blk', 5000, 1, { data_gaps: { planning_missing: true } }), it('exc', 5000, 1, { is_excluded: true }), it('b', 900, 10)];
  const r = applyDailyCap(items, S());
  assert.equal(r.reason, 'within');
  assert.deepEqual(qtys(items), { a: 5000, blk: 5000, exc: 5000, b: 900 });
});

t('off で止める', () => {
  const items = [it('a', 9000, 5)];
  assert.equal(applyDailyCap(items, S({ v3_daily_cap: 'off' })).enabled, false);
  assert.equal(items[0].adjusted_qty, 9000);
});

console.log('記録');
t('翌日へ回した行は「送らなくてよい」の理由 daily_cap・枠まで送る行は理由の文に書く・v2 との差の理由に daily_cap', () => {
  const items = [it('a', 4000, 5), it('b', 3000, 10), it('c', 3000, 20)];
  applyDailyCap(items, S());
  const c = items.find((i) => i.amazon_sku === 'c');
  assert.equal(calmReason(c), 'daily_cap');
  const { proposals, calm } = pickDraftRows(items);
  assert.deepEqual(proposals.map((p) => p.amazon_sku), ['a', 'b']);
  assert.equal(calm.find((x) => x.item.amazon_sku === 'c').reason, 'daily_cap');
  assert.match(rationaleOf(items[1]), /1 日の上限のため 3000 → 2000 個 \(残りは翌日\)/);
  assert.deepEqual(inputsOf(items[1], { runId: 'r' }).daily_cap, { before: 3000, after: 2000 });
  const v2 = { items: [it('a', 4000, 5), it('b', 3000, 10), it('c', 3000, 20)].map((x) => ({ ...x, reorder_point_days: 21, target_days: 40 })) };
  const v3 = { items: items.map((x) => ({ ...x, reorder_point_days: 21, target_days: 40 })), data_quality: { daily_cap: { reason: 'capped' } } };
  const cmp = compareRuleResults(v2, v3);
  assert.ok(cmp.top.find((d) => d.sku === 'c').why.includes('daily_cap'));
  assert.equal(cmp.daily_cap.reason, 'capped');
});

console.log('エンジンを通して');
t('v3 だけにかかる (v2 はかからない)・上限は設定で変えられる', () => {
  const SKUS = [['e1', 5], ['e2', 8], ['e3', 12]];   // 日販 10・FBA 在庫の日数
  db.upsertSkuMappings(SKUS.map(([s]) => ({ amazon_sku: s, product_name: s, ne_code: s, logizard_code: s })));
  db.saveRestockLatest(SKUS.map(([s, d]) => ({ amazon_sku: s, product_name: s, fba_available: d * 10, units_sold_30d: 300, units_sold_7d: 70, amazon_recommended_qty: null })));
  db.savePlanningLatest(SKUS.map(([s]) => ({ sku: s, units_sold_7d: 70, per_unit_volume: 300, low_inv_fee_exempt: 'Yes' })));
  db.replaceWarehouseInventory(SKUS.map(([s]) => ({ logizard_code: s, product_name: s, location: `P-${s}`, quantity: 5000, reserved: 0, available_qty: 5000, expiry_date: '', block_alloc_order: 1 })));
  db.updateSetting('self_reserve_mode', 'off');
  db.updateSetting('v3_daily_cap_units', '600');
  const run = (rules) => generateRecommendations(false, {}, { rules, pendingSlips: { status: 'ok', slips: [], byCode: new Map() } });
  const v3 = run('v3');
  assert.equal(v3.data_quality.daily_cap.reason, 'capped');
  const kept = v3.items.filter((i) => i.adjusted_qty > 0).map((i) => i.amazon_sku);
  assert.ok(kept.includes('e1'), '在庫日数のいちばん短い e1 は残る');
  assert.ok(v3.items.filter((i) => i.adjusted_qty > 0).reduce((s, i) => s + i.adjusted_qty, 0) <= 600);
  const v2 = run('v2');
  assert.equal(v2.data_quality.daily_cap, undefined);
  assert.ok(v2.items.reduce((s, i) => s + i.adjusted_qty, 0) > 600, 'v2 は削らない');
});

fs.rmSync(process.env.DATA_DIR, { recursive: true, force: true });
console.log(`\n${passed} 件 ok${process.exitCode ? ' (NG あり)' : ''}`);
