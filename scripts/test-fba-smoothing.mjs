/**
 * test-fba-smoothing.mjs — FBA 補充「決まりの変更 v3-2 推奨が少ない日のならし」の受入試験
 *
 * 一時 DB に SKU・RESTOCK・PLANNING・倉庫在庫を入れ、自社出荷ぶんを残す配分 (等日数) を入れた v3 で見る:
 *   - 合計が目安に届かない日だけ、「発注点を下回っていないだけ」の SKU を在庫日数の短い順に、残りの枠まで足す
 *   - 候補にしない: 在庫日数が発注点 + 10 日以上・Amazon 推奨 0・データの欠け・最低出荷日数に満たない量
 *   - 🚨 通常の補充を先に配り、早めに送る分は残りから (同じ構成品を取り合っても通常の補充は減らない)
 *   - SKU 数の上限・目安に届いている日・止める設定・v2 ではならさない
 * 使い方: node scripts/test-fba-smoothing.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'fba-smoothing-'));
const db = await import('../apps/fba-replenishment/db.js');
await db.initDb();
const { generateRecommendations, planSmoothing } = await import('../apps/fba-replenishment/calculation-engine.js');
const { compareRuleResults } = await import('../apps/fba-replenishment/decision-job.js');
const { pickDraftRows, rationaleOf, inputsOf } = await import('../apps/fba-replenishment/shadow-draft.mjs');

let passed = 0;
function t(name, fn) { try { fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack}`); process.exitCode = 1; } }

// どれも高回転 (30 日販売 300 = 日販 10)・小型・免除 → v3 の発注点 28 日・目標 42 日 (目標在庫 420 個)
//   [SKU, FBA 在庫の日数, Amazon 推奨, PLANNING あり, 倉庫のコード]
const SKUS = [
  ['reg1', 10, null, true, 'reg1'],       // 発注点を下回った = 通常の補充 (420 − 100 = 320 個)
  ['pullA', 30, null, true, 'pullA'],     // 30 < 28 + 10 → 候補 (420 − 300 = 120 個まで)
  ['pullB', 32, null, true, 'pullB'],     // 候補 (2 番目。420 − 320 = 100 個 = 10 日分)
  ['farC', 60, null, true, 'farC'],       // 60 ≥ 38 → 候補にしない
  ['zeroAmz', 30, 0, true, 'zeroAmz'],    // Amazon 推奨 0 → 候補にしない
  ['gapD', 30, null, false, 'gapD'],      // PLANNING が無い = データの欠け → 候補にしない
];
db.upsertSkuMappings(SKUS.map(([sku, , , , code]) => ({ amazon_sku: sku, product_name: sku, ne_code: code, logizard_code: code })));
db.saveRestockLatest(SKUS.map(([sku, days, amz]) => ({
  amazon_sku: sku, product_name: sku, fba_available: days * 10, units_sold_30d: 300, units_sold_7d: 70, amazon_recommended_qty: amz,
})));
db.savePlanningLatest(SKUS.filter(([, , , pl]) => pl).map(([sku]) => ({
  sku, units_sold_7d: 70, per_unit_volume: 300, low_inv_fee_exempt: 'Yes', low_inv_fee_applied: 'No', short_term_dos: 30, long_term_dos: 30,
})));
const stock = (code, qty) => ({ logizard_code: code, product_name: code, location: `P-${code}`, quantity: qty, reserved: 0, available_qty: qty, expiry_date: '', block_alloc_order: 1 });
db.replaceWarehouseInventory(SKUS.map(([, , , , code]) => stock(code, 5000)));
db.updateSetting('self_reserve_mode', 'equal_days');

// 自社日販 1 個/日。キーはエンジンが引く形 (小文字) にそろえる
const selfSales = (codes = SKUS.map(([, , , , c]) => c)) => ({ status: 'ok', map: new Map(codes.map((c) => [c.toLowerCase(), 30])) });
const run = (rules, o = {}) => generateRecommendations(false, {}, { rules, selfShipSales: o.selfSales || selfSales(), pendingSlips: { status: 'ok', slips: [], byCode: new Map() }, ...o });
const at = (r, sku) => r.items.find((i) => i.amazon_sku === sku);
const setAll = (kv) => { for (const [k, v] of Object.entries(kv)) db.updateSetting(k, String(v)); };
setAll({ v3_smooth_target_units: 400, v3_smooth_max_add_units: 1500, v3_smooth_max_skus: 100, v3_pull_forward_days: 10, v3_smoothing: 'on' });

console.log('ならし');
t('目安 400 に届かない (通常の補充 320) → 在庫日数の短い pullA を残りの枠 80 個まで足す。pullB は枠が無いので足さない', () => {
  const r = run('v3');
  assert.equal(at(r, 'reg1').adjusted_qty, 320);
  assert.equal(at(r, 'reg1').pull_forward, false);
  assert.deepEqual([at(r, 'pullA').adjusted_qty, at(r, 'pullA').pull_forward, at(r, 'pullA').needs_replenishment], [80, true, true]);
  assert.equal(at(r, 'pullB').adjusted_qty, 0);
  const s = r.data_quality.smoothing;
  assert.deepEqual([s.enabled, s.reason, s.regular_units, s.budget_units, s.picked, s.pulled_units, s.regular_changed, s.stopped], [true, 'smoothed', 320, 80, 1, 80, 0, 'budget']);
});

t('候補にしない: 在庫日数が発注点 + 10 日以上・Amazon 推奨 0・データの欠け', () => {
  setAll({ v3_smooth_target_units: 5000 });
  try {
    const r = run('v3');
    assert.ok(at(r, 'pullA').adjusted_qty > 0 && at(r, 'pullB').adjusted_qty > 0, '枠が大きければ両方');
    for (const sku of ['farC', 'zeroAmz', 'gapD']) assert.equal(at(r, sku).adjusted_qty, 0, sku);
    assert.equal(r.data_quality.smoothing.candidates, 2);
  } finally { setAll({ v3_smooth_target_units: 400 }); }
});

t('最低出荷日数 (7 日) に満たない量しか枠が無ければ足さない (目安に届かなくても終わる)', () => {
  setAll({ v3_smooth_target_units: 370 });   // 枠 50 個 = 5 日分
  try {
    const r = run('v3');
    assert.equal(at(r, 'pullA').adjusted_qty, 0);
    assert.deepEqual([r.data_quality.smoothing.reason, r.data_quality.smoothing.skipped_small], ['no_candidate', 2]);
  } finally { setAll({ v3_smooth_target_units: 400 }); }
});

t('目安に届いている日・SKU 数の上限・止める設定・v2 ではならさない', () => {
  setAll({ v3_smooth_target_units: 300 });
  assert.equal(run('v3').data_quality.smoothing.reason, 'enough');
  setAll({ v3_smooth_target_units: 400, v3_smooth_max_skus: 1 });
  const m = run('v3');
  assert.deepEqual([m.data_quality.smoothing.picked, m.data_quality.smoothing.stopped, at(m, 'reg1').adjusted_qty], [0, 'max_skus', 320], '通常の補充は削らない');
  setAll({ v3_smooth_max_skus: 100, v3_smoothing: 'off' });
  assert.equal(run('v3').data_quality.smoothing.reason, 'off');
  setAll({ v3_smoothing: 'on' });
  const v2 = run('v2');
  assert.equal(v2.data_quality.smoothing, undefined);
  assert.ok(v2.items.every((i) => !i.pull_forward));
});

t('自社出荷の日販を使えない日 (配分を切った) はならさない', () => {
  db.updateSetting('self_reserve_mode', 'off');
  try { assert.equal(run('v3').data_quality.smoothing.reason, 'self_sales_not_used'); }
  finally { db.updateSetting('self_reserve_mode', 'equal_days'); }
});

t('🚨 同じ構成品を取り合う: 通常の補充を先に配り、早めに送る分は残りから (通常の補充は減らない)', () => {
  // pullA を reg1 と同じ構成品にし、倉庫を 350 個に。自社日販は 0 (分かっていて 0 = 自社ぶんの上限なし) にして取り合いだけを見る:
  //   reg1 に 320 を配ったあとの残り 30 個 (3 日分) は最低出荷日数に満たないので pullA は 0
  db.upsertSkuMappings([{ amazon_sku: 'pullA', product_name: 'pullA', ne_code: 'reg1', logizard_code: 'reg1' }]);
  db.replaceWarehouseInventory([stock('reg1', 350), ...SKUS.filter(([, , , , c]) => c !== 'reg1' && c !== 'pullA').map(([, , , , c]) => stock(c, 5000))]);
  setAll({ v3_smooth_target_units: 500 });   // 枠を広げて pullA を選ばせる (選んだうえで配分が削るのを見る)
  try {
    const sales = { status: 'ok', map: new Map([['reg1', 0], ['pullb', 30], ['farc', 30], ['zeroamz', 30], ['gapd', 30]]) };
    const r = run('v3', { selfSales: sales });
    const plain = run('v3', { selfSales: sales, smoothing: false });
    assert.equal(at(r, 'reg1').adjusted_qty, at(plain, 'reg1').adjusted_qty, '通常の補充は先に配る (ならしが無いときと同じ数)');
    assert.ok(at(r, 'reg1').adjusted_qty >= 320, 'ロケ補正で棚の 350 個にそろうこともある');
    assert.equal(at(r, 'pullA').pull_forward, true);
    assert.equal(at(r, 'pullA').adjusted_qty, 0, '早めに送る分は残りから (最低出荷日数に満たず 0)');
    assert.equal(at(r, 'pullA').allocation.shared_cut + at(r, 'pullA').allocation.min_days_cut > 0, true);
    assert.equal(r.data_quality.smoothing.regular_changed, 0);
    // 自社ぶんを残す配分がかかる場合: 等日数は全 SKU の不足から決まるので、ならしがあっても無くても通常の補充の数は同じ
    const withSelf = { status: 'ok', map: new Map([['reg1', 30], ['pullb', 30], ['farc', 30], ['zeroamz', 30], ['gapd', 30]]) };
    assert.equal(at(run('v3', { selfSales: withSelf }), 'reg1').adjusted_qty, at(run('v3', { selfSales: withSelf, smoothing: false }), 'reg1').adjusted_qty);
  } finally {
    setAll({ v3_smooth_target_units: 400 });
    db.upsertSkuMappings([{ amazon_sku: 'pullA', product_name: 'pullA', ne_code: 'pullA', logizard_code: 'pullA' }]);
    db.replaceWarehouseInventory(SKUS.map(([, , , , code]) => stock(code, 5000)));
  }
});

t('記録: 提案に入り、理由の文と inputs_ref に「早めに送る」が残る・v2 との差の理由に pull_forward', () => {
  const v3 = run('v3'), v2 = run('v2');
  const { proposals } = pickDraftRows(v3.items);
  const p = proposals.find((i) => i.amazon_sku === 'pullA');
  assert.ok(p);
  assert.match(rationaleOf(p), /まだ下回っていないが、推奨が少ない日なので早めに送る/);
  assert.deepEqual([inputsOf(p, { runId: 'r' }).pull_forward, inputsOf(p, { runId: 'r' }).pull_forward_cap], [true, 80]);
  const c = compareRuleResults(v2, v3);
  assert.ok(c.top.find((d) => d.sku === 'pullA').why.includes('pull_forward'));
  assert.equal(c.smoothing.pulled_units, 80);
});

t('🚨 恒久除外の SKU は候補にしない・提案にしない (Codex PR #1471 R1 High)', () => {
  const r = run('v3', { excluded: ['pullA'] });
  assert.equal(at(r, 'pullA').is_excluded, true);
  assert.equal(at(r, 'pullA').adjusted_qty, 0, '除外は早めに送らない');
  assert.equal(at(r, 'pullB').adjusted_qty, 80, '代わりに次の候補');
  const r2 = run('v3', { excluded: ['reg1'] });
  const { proposals, calm } = pickDraftRows(r2.items);
  assert.ok(!proposals.some((i) => i.amazon_sku === 'reg1'), '恒久除外は提案に入れない (画面と同じ)');
  assert.equal(calm.find((c) => c.item.amazon_sku === 'reg1').reason, 'excluded');
  assert.equal(r2.data_quality.smoothing.regular_units, 0, '除外した通常の補充は枠に数えない');
});

t('🚨 丸め・ロケ補正で増えても、早めに送る数は選んだ数 (残りの枠) まで (Codex PR #1471 R1 Medium 2)', () => {
  // pullA の棚が 88 個 = 選んだ 80 個の ±10% に棚の区切りがあるので、ロケ補正は 88 に寄せようとする
  db.replaceWarehouseInventory([...SKUS.filter(([, , , , c]) => c !== 'pullA').map(([, , , , c]) => stock(c, 5000)), stock('pullA', 88)]);
  try {
    // pullA の自社日販は 0 (分かっていて 0) = 自社ぶんを残す上限はかからない。枠と補正だけを見る
    const sales = { status: 'ok', map: new Map(SKUS.map(([, , , , c]) => [c.toLowerCase(), c === 'pullA' ? 0 : 30])) };
    const r = run('v3', { selfSales: sales });
    assert.equal(at(r, 'pullA').adjusted_qty, 80);
    assert.equal(r.data_quality.smoothing.pulled_units, r.data_quality.smoothing.planned_units);
  } finally { db.replaceWarehouseInventory(SKUS.map(([, , , , code]) => stock(code, 5000))); }
});

t('🚨 提案できない通常の補充 (データの欠け) は枠に数えない (Codex PR #1471 R1 Medium 3)', () => {
  // gapD を発注点より下げる (10 日分): 数量は出るが PLANNING が無いので記録では保留 = 通常の補充に数えない
  db.saveRestockLatest(SKUS.map(([sku, days, amz]) => ({
    amazon_sku: sku, product_name: sku, fba_available: (sku === 'gapD' ? 10 : days) * 10, units_sold_30d: 300, units_sold_7d: 70, amazon_recommended_qty: amz,
  })));
  try {
    const r = run('v3');
    assert.ok(at(r, 'gapD').adjusted_qty > 0);
    assert.equal(r.data_quality.smoothing.regular_units, 320, '保留の gapD は数えない');
    assert.equal(r.data_quality.smoothing.regular_skus, 1);
    assert.equal(at(r, 'pullA').adjusted_qty, 80);
  } finally {
    db.saveRestockLatest(SKUS.map(([sku, days, amz]) => ({
      amazon_sku: sku, product_name: sku, fba_available: days * 10, units_sold_30d: 300, units_sold_7d: 70, amazon_recommended_qty: amz,
    })));
  }
});

t('🚨 枠の数え方は記録の判定 (blockedReason) と同じ: 7 日販売だけが欠けた通常の補充は提案になるので数える (Codex PR #1471 R2 Medium)', () => {
  const al = { mode: 'equal_days', self_sales: { used: true } };
  const reg = (o) => ({ amazon_sku: 'r', ne_code: 'r', adjusted_qty: 2500, needs_replenishment: true, stock_state: 'normal', data_gaps: {}, ...o });
  const cand = { amazon_sku: 'c', ne_code: 'c', adjusted_qty: 0, needs_replenishment: false, stock_state: 'normal', daily_sales: 10,
    days_of_supply: 30, reorder_point_days: 28, target_days: 42, effective_fba_stock: 300, warehouse_available: 999, data_gaps: {} };
  const s = { v3_smooth_target_units: '2500', min_shipment_cover_days: '7' };
  const p1 = planSmoothing({ items: [reg({ data_gaps: { sales_7d_missing: true } }), cand], data_quality: { allocation: al } }, s);
  assert.equal(p1.summary.reason, 'enough', '7 日販売の欠けは記録では保留にならない = 通常の補充 2,500 個');
  const p2 = planSmoothing({ items: [reg({ data_gaps: { planning_missing: true } }), cand], data_quality: { allocation: al } }, s);
  assert.deepEqual([p2.summary.regular_units, [...p2.picks.keys()]], [0, ['c']], 'PLANNING の欠けは保留 = 数えない');
  const p3 = planSmoothing({ items: [reg({ adjusted_qty: 100 }), { ...cand, data_gaps: { sales_7d_missing: true } }], data_quality: { allocation: al } }, s);
  assert.equal(p3.picks.size, 0, '候補の側は 7 日販売の欠けも外す (記録より厳しく)');
});

t('planSmoothing は結果だけから選ぶ (計算し直さない)', () => {
  const r = run('v3', { smoothing: false });
  const plan = planSmoothing(r, { v3_smooth_target_units: '400', min_shipment_cover_days: '7' });
  assert.deepEqual([...plan.picks.entries()], [['pullA', 80]]);
});

fs.rmSync(process.env.DATA_DIR, { recursive: true, force: true });
console.log(`\n${passed} 件 ok${process.exitCode ? ' (NG あり)' : ''}`);
