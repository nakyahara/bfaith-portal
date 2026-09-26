/**
 * test-fba-rules-v3.mjs — FBA 補充「決まりの変更 v3-1」の受入試験
 *
 * 一時 DB (DATA_DIR=os tmp) に SKU・RESTOCK・PLANNING (低在庫手数料の欄)・倉庫在庫を入れ、同じ入力で v2 と v3 を計算して比べる。
 *   - 低在庫手数料の見張り: 免除でない・売れている SKU は発注点 28 (14 + 7 + 7)。免除・分からない は上げない
 *   - 高回転: 発注点 28・目標 小型 42 / 大型 35。低回転: 目標 70。中回転は区分どおり (手数料の見張りで上がるものはある)
 *   - 🚨 v3 を計算しても既存の設定 (画面の数字) は変わらない。v3_* の設定で数字を変えられる
 *   - v2 と v3 の差の集計 (記録する提案と同じ adjusted_qty で数える)
 * 使い方: node scripts/test-fba-rules-v3.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'fba-rules-v3-'));
const db = await import('../apps/fba-replenishment/db.js');
await db.initDb();
const { generateRecommendations, rulesSettings, feeGuardDays, V3_DEFAULTS } = await import('../apps/fba-replenishment/calculation-engine.js');
const { compareRuleResults, decisionRulesOf } = await import('../apps/fba-replenishment/decision-job.js');
const { inputsOf } = await import('../apps/fba-replenishment/shadow-draft.mjs');

let passed = 0;
function t(name, fn) { try { fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack}`); process.exitCode = 1; } }

// 30 日販売: 高回転 > 100 / 中 20〜100 / 低 < 20。体積: 小型 300cm3 / 大型 8000cm3
const SKUS = [
  ['hi-small-elig', 150, 300, 'No'], ['hi-large-elig', 150, 8000, 'No'], ['hi-small-exempt', 150, 300, 'Yes'],
  ['mid-elig', 60, 300, 'No'], ['mid-exempt', 60, 300, 'Yes'], ['mid-unknown', 60, 300, null],
  ['lo-elig', 10, 300, 'No'], ['lo-exempt-small', 10, 300, 'Yes'], ['lo-exempt-large', 10, 8000, 'Yes'],
  // 季節商品 (目標 50 日) / 30 日販売は低回転だが 7 日販売 12 (≥ 10) = 発注点だけ中回転扱い
  ['season-elig', 60, 300, 'No', { seasonal: 'Yes' }], ['weekly-exempt', 15, 300, 'Yes', { sold7: 12 }],
];
db.upsertSkuMappings(SKUS.map(([sku]) => ({ amazon_sku: sku, product_name: sku, ne_code: sku, logizard_code: sku })));
// FBA 在庫は 25 日分 (v2 の発注点 21 は超えるが、v3 の 28 は下回る)
const sold7Of = (sold30, o) => o?.sold7 ?? Math.round(sold30 / 30 * 7);
db.saveRestockLatest(SKUS.map(([sku, sold30, , , o]) => ({
  amazon_sku: sku, product_name: sku, fba_available: Math.round(sold30 / 30 * 25), units_sold_30d: sold30, units_sold_7d: sold7Of(sold30, o),
  amazon_recommended_qty: null,
})));
db.savePlanningLatest(SKUS.filter(([, , , ex]) => ex !== null).map(([sku, sold30, vol, ex, o]) => ({
  sku, units_sold_7d: sold7Of(sold30, o), per_unit_volume: vol, low_inv_fee_exempt: ex, low_inv_fee_applied: 'No',
  short_term_dos: 25, long_term_dos: 30, is_seasonal: o?.seasonal || null,
})));
db.replaceWarehouseInventory(SKUS.map(([sku]) => ({ logizard_code: sku, product_name: sku, location: `P-${sku}`, quantity: 5000, reserved: 0, available_qty: 5000, expiry_date: '', block_alloc_order: 1 })));
// 自社出荷の日販は読めない環境なので配分は切る (ここで見たいのは発注点・目標の決まり)
db.updateSetting('self_reserve_mode', 'off');

const run = (rules) => generateRecommendations(false, {}, { rules, selfShipSales: { status: 'ok', map: new Map() }, pendingSlips: { status: 'ok', slips: [], byCode: new Map() } });
const v2 = run('v2');
const v3 = run('v3');
const at = (r, sku) => r.items.find((i) => i.amazon_sku === sku);

console.log('決まり');
t('手数料の見張りの発注点 = 14 + 7 + 7 = 28 (設定で変えられる)', () => {
  assert.equal(feeGuardDays(rulesSettings({}, 'v3')), 28);
  assert.equal(feeGuardDays(rulesSettings({ v3_fee_safety_days: '3', low_inventory_fee_threshold_days: '14' }, 'v3')), 24);
});

t('高回転: 発注点 21 → 28、目標 小型 40 → 42 / 大型 30 → 35', () => {
  assert.deepEqual([at(v2, 'hi-small-elig').reorder_point_days, at(v2, 'hi-small-elig').target_days], [21, 40]);
  assert.deepEqual([at(v3, 'hi-small-elig').reorder_point_days, at(v3, 'hi-small-elig').target_days], [28, 42]);
  assert.deepEqual([at(v3, 'hi-large-elig').reorder_point_days, at(v3, 'hi-large-elig').target_days], [28, 35]);
  assert.equal(at(v3, 'hi-small-exempt').reorder_point_days, 28, '高回転は免除でも 28 (区分の発注点)');
  assert.equal(at(v3, 'hi-small-exempt').reorder_point_reason, 'tier');
});

t('中回転: 免除でない → 手数料の見張りで 28 / 免除・分からない → 区分どおり 21', () => {
  assert.deepEqual([at(v3, 'mid-elig').reorder_point_days, at(v3, 'mid-elig').reorder_point_reason, at(v3, 'mid-elig').fee_status], [28, 'fee_guard', 'eligible']);
  assert.deepEqual([at(v3, 'mid-exempt').reorder_point_days, at(v3, 'mid-exempt').fee_status], [21, 'exempt']);
  assert.deepEqual([at(v3, 'mid-unknown').reorder_point_days, at(v3, 'mid-unknown').fee_status], [21, 'unknown'], '分からないのに上げない (記録だけ)');
  assert.equal(at(v3, 'mid-elig').target_days, 35, '目標は 35 のまま (28 + 最低出荷 7 = 35 以上)');
});

t('低回転: 目標 小型 180・大型 90 → 70。免除でない低回転は発注点 14 → 28', () => {
  assert.deepEqual([at(v2, 'lo-exempt-small').target_days, at(v2, 'lo-exempt-large').target_days], [180, 90]);
  assert.deepEqual([at(v3, 'lo-exempt-small').target_days, at(v3, 'lo-exempt-large').target_days], [70, 70]);
  assert.deepEqual([at(v3, 'lo-exempt-small').reorder_point_days, at(v3, 'lo-elig').reorder_point_days], [14, 28]);
});

t('v3 で発注点を下回った SKU は提案に上がる (25 日分 < 28)。v2 では 21 を超えているので上がらない', () => {
  assert.equal(at(v2, 'mid-elig').needs_replenishment, false);
  assert.equal(at(v3, 'mid-elig').needs_replenishment, true);
  assert.ok(at(v3, 'mid-elig').adjusted_qty > 0);
  assert.equal(at(v3, 'mid-exempt').needs_replenishment, false);
});

t('🚨 v3 を計算しても既存の設定は変わらない (画面・米国補充に効かせない)', () => {
  const s = db.getSettings();
  assert.equal(s.reorder_point_high_volume, '21');
  assert.equal(s.target_days_low_volume_small, '180');
  assert.equal(v2.rules, 'v2'); assert.equal(v3.rules, 'v3');
  assert.equal(at(v2, 'mid-elig').reorder_point_reason, 'tier', 'v2 は手数料の見張りをしない');
});

t('v3_* の設定で v3 の数字を変えられる', () => {
  db.updateSetting('v3_target_days_low_volume_small', '60');
  const r = run('v3');
  assert.equal(at(r, 'lo-exempt-small').target_days, 60);
  assert.equal(at(run('v2'), 'lo-exempt-small').target_days, 180);
  db.updateSetting('v3_target_days_low_volume_small', String(V3_DEFAULTS.v3_target_days_low_volume_small));
});

t('季節商品: 目標 50 のまま・免除でなければ手数料の見張りで発注点 28 / 7 日販売で中回転扱いの低回転: 発注点 21・目標は低回転の 70', () => {
  assert.deepEqual([at(v3, 'season-elig').reorder_point_days, at(v3, 'season-elig').target_days, at(v3, 'season-elig').reorder_point_reason], [28, 50, 'fee_guard']);
  assert.deepEqual([at(v2, 'season-elig').reorder_point_days, at(v2, 'season-elig').target_days], [21, 50]);
  assert.deepEqual([at(v3, 'weekly-exempt').reorder_point_days, at(v3, 'weekly-exempt').target_days], [21, 70]);
  assert.deepEqual([at(v2, 'weekly-exempt').reorder_point_days, at(v2, 'weekly-exempt').target_days], [21, 180]);
});

t('目標の下限 (発注点 + 最低出荷日数) が実際に効く: 最低出荷 14 日なら高回転・大型の目標は 35 → 42 (v2 は 30 のまま)', () => {
  db.updateSetting('min_shipment_cover_days', '14');
  try {
    assert.equal(at(run('v3'), 'hi-large-elig').target_days, 42);
    assert.equal(at(run('v2'), 'hi-large-elig').target_days, 30);
  } finally { db.updateSetting('min_shipment_cover_days', '7'); }
});

t('🚨 v2 → v3 → v2 と回しても v2 の結果は変わらない (同じプロセスで 2 回回す 9:40 の自動決定)', () => {
  const key = (r) => r.items.map((i) => [i.amazon_sku, i.adjusted_qty, i.reorder_point_days, i.target_days, i.warehouse_available, i.allocation?.fba_cap ?? null]);
  const first = run('v2');
  run('v3');
  assert.deepEqual(key(run('v2')), key(first));
});

t('自社出荷ぶんを残す配分を入れても v3 は動き、配分の記録が付く', () => {
  db.updateSetting('self_reserve_mode', 'equal_days');
  try {
    const selfSales = { status: 'ok', map: new Map(SKUS.map(([sku]) => [sku, 30])) };
    const opt = (rules) => generateRecommendations(false, {}, { rules, selfShipSales: selfSales, pendingSlips: { status: 'ok', slips: [], byCode: new Map() } });
    const a = opt('v2'), b = opt('v3');
    assert.equal(b.data_quality.allocation.mode, 'equal_days');
    assert.ok(at(b, 'mid-elig').adjusted_qty > 0);
    assert.ok(at(b, 'mid-elig').adjusted_qty <= at(b, 'mid-elig').warehouse_available);
    assert.equal(at(a, 'mid-elig').needs_replenishment, false);
  } finally { db.updateSetting('self_reserve_mode', 'off'); }
});

t('記録 (inputs_ref) に決まりの版・発注点の理由・手数料の根拠が残る (Codex PR #1466 R1 Medium 1)', () => {
  const x = inputsOf(at(v3, 'mid-elig'), { runId: 'r' });
  assert.deepEqual([x.rules, x.reorder_point_reason, x.fee_status, x.fee_short_term_dos, x.fee_long_term_dos, x.fee_applied, x.reorder_point_days],
    ['v3', 'fee_guard', 'eligible', 25, 30, 'No', 28]);
});

console.log('v2 と v3 の差');
t('差の集計: 増えた・減った・新しく上がった・理由 (手数料の見張り・発注点・目標)・手数料の状態', () => {
  const c = compareRuleResults(v2, v3);
  assert.equal(c.v2.proposals + c.added - c.removed, c.v3.proposals);
  assert.ok(c.added >= 1, JSON.stringify(c));
  const mid = c.top.find((d) => d.sku === 'mid-elig');
  assert.deepEqual([mid.v2, mid.v3 > 0, mid.why.includes('fee_guard'), mid.fee_status], [0, true, true, 'eligible']);
  assert.ok(c.reasons.fee_guard >= 1);
  assert.equal(c.fee.unknown, 1);
  assert.ok(c.fee.guarded >= 2, JSON.stringify(c.fee));
});

t('数は同じでも発注点・目標が変わった SKU は rule_only に残す (数の変化の件数には入れない。Codex PR #1466 R1 Medium 2)', () => {
  const fake = (items) => ({ items });
  const item = (o) => ({ amazon_sku: 'x', adjusted_qty: 10, needs_replenishment: true, reorder_point_days: 21, target_days: 40, data_gaps: {}, ...o });
  const c = compareRuleResults(fake([item({})]), fake([item({ reorder_point_days: 28, reorder_point_reason: 'fee_guard', target_days: 42, amazon_reco_capped: true })]));
  assert.deepEqual([c.changed, c.top.length, c.rule_only.count], [0, 0, 1]);
  assert.deepEqual(c.rule_only.top[0].why, ['fee_guard', 'target_days']);
  assert.equal(c.rule_only.top[0].amazon_capped, true);
});

t('Amazon 推奨で切られた SKU を数える (今は切る。どれくらい止まるかを見る)', () => {
  const fake = (items) => ({ items });
  const item = (o) => ({ amazon_sku: 'x', adjusted_qty: 0, needs_replenishment: true, reorder_point_days: 28, target_days: 42, data_gaps: {}, ...o });
  const c = compareRuleResults(fake([item({ amazon_sku: 'a', adjusted_qty: 10 })]),
    fake([item({ amazon_sku: 'a', adjusted_qty: 5, amazon_reco_capped: true, raw_needed_before_amazon_cap: 30, amazon_recommended_qty: 5 }),
      item({ amazon_sku: 'b', amazon_reco_capped: true, raw_needed_before_amazon_cap: 12, amazon_recommended_qty: 0 })]));
  assert.deepEqual([c.amazon_capped_v3.count, c.amazon_capped_v3.units_cut, c.amazon_capped_v3.zeroed], [2, 37, 1]);
});

t('自動決定で記録する決まり: 設定 decision_rules (既定 v3、v2 に戻せる)', () => {
  assert.equal(decisionRulesOf({ readSettings: () => ({}) }), 'v3');
  assert.equal(decisionRulesOf({ readSettings: () => ({ decision_rules: 'V2' }) }), 'v2');
  assert.equal(decisionRulesOf({ readSettings: () => { throw new Error('x'); } }), 'v3');
});

fs.rmSync(process.env.DATA_DIR, { recursive: true, force: true });
console.log(`\n${passed} 件 ok${process.exitCode ? ' (NG あり)' : ''}`);
