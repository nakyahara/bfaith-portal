/**
 * test-fba-trials.mjs — FBA 補充「決まりの変更 v3-3 長期欠品の復活・新規出品の試す候補」の受入試験
 *
 * 一時 DB に SKU・RESTOCK・PLANNING・倉庫在庫・日次の記録を入れ、自社出荷ぶんを残す配分を入れた v3 で見る:
 *   - 復活: 欠品前 (FBA に在庫があった最新の日) の 30 日販売から 30 日分・上限 30・Amazon 推奨・倉庫の空きの小さい方。
 *     履歴が無い・180 日より古い → 10 個。入荷待ち・出荷待ち伝票・恒久除外・自社日販が分からない は出さない
 *   - 新規: FBA で一度も在庫・入荷を見ていない SKU を 10 個で。非表示・恒久除外・準備中・伝票・空き無し は出さない。1 日の件数の上限
 *   - 倉庫の空き = 倉庫 − 出荷待ち伝票 − その日の提案で使う数 − 自社日販 × 30 日。候補どうしも取り合う
 *   - 🚨 提案 (proposal) には入れない。影の下書きには要確認 (finding) として 1 SKU 1 行
 * 使い方: node scripts/test-fba-trials.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'fba-trials-'));
const db = await import('../apps/fba-replenishment/db.js');
await db.initDb();
const { generateRecommendations } = await import('../apps/fba-replenishment/calculation-engine.js');
const { recordShadowDraft, pickDraftRows, GENERATOR } = await import('../apps/fba-replenishment/shadow-draft.mjs');

let passed = 0;
async function t(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack}`); process.exitCode = 1; } }
const quiet = () => {};

const NOW = Date.parse('2026-09-27T00:40:00Z');
const daysAgo = (n) => new Date(NOW - n * 86400e3 + 9 * 3600e3).toISOString().slice(0, 10);

// 長期欠品 (FBA 0・30 日販売 0・Amazon 推奨 20 → revivable_long_oos)。[SKU, 倉庫在庫, restock の追加]
const REVIVE = [
  ['rv-hist', 500], ['rv-nohist', 500], ['rv-old', 500], ['rv-inbound', 500, { fba_inbound_shipped: 5 }],
  ['rv-pending', 500], ['rv-excluded', 500], ['rv-selfunk', 500], ['rv-keep', 35],
];
// 新規 (SKU マスタだけ。RESTOCK に無い)
const NEW = [['nw-a', 300], ['nw-b', 200], ['nw-hidden', 300], ['nw-excl', 300], ['nw-inbound', 300], ['nw-nostock', 0]];
db.upsertSkuMappings([...REVIVE, ...NEW].map(([sku]) => ({ amazon_sku: sku, product_name: `商品 ${sku}`, ne_code: sku, logizard_code: sku })));
db.saveRestockLatest(REVIVE.map(([sku, , extra]) => ({ amazon_sku: sku, product_name: sku, fba_available: 0, units_sold_30d: 0, units_sold_7d: 0, amazon_recommended_qty: 20, ...(extra || {}) })));
db.savePlanningLatest(REVIVE.map(([sku]) => ({ sku, units_sold_7d: 0, per_unit_volume: 300, low_inv_fee_exempt: 'Yes' })));
const stock = (code, qty) => ({ logizard_code: code, product_name: code, location: `P-${code}`, quantity: qty, reserved: 0, available_qty: qty, expiry_date: '', block_alloc_order: 1 });
db.replaceWarehouseInventory([...REVIVE, ...NEW].filter(([, q]) => q > 0).map(([code, q]) => stock(code, q)));
db.updateSetting('self_reserve_mode', 'equal_days');
// 欠品前の記録: rv-hist は 20 日前に在庫あり・30 日販売 60 (= 日販 2)、rv-old は 200 日前、rv-inbound 等も在庫を見たことがある
db.savePlanningData([{ sku: 'rv-hist', fba_available: 5, units_sold_30d: 60 }], daysAgo(20));
db.savePlanningData([{ sku: 'rv-old', fba_available: 5, units_sold_30d: 90 }], daysAgo(200));
for (const [sku] of REVIVE.filter(([s]) => !['rv-hist', 'rv-old'].includes(s))) db.savePlanningData([{ sku, fba_available: 3, units_sold_30d: 0 }], daysAgo(60));
db.hideNewProductSkuBulk(['nw-hidden']);

const codes = [...REVIVE, ...NEW].map(([c]) => c).filter((c) => c !== 'rv-selfunk');
const selfSales = { status: 'ok', map: new Map(codes.map((c) => [c.toLowerCase(), 30])) };   // 自社日販 1 個/日 (rv-selfunk だけ分からない)
const pendingSlips = { status: 'ok', slips: [], byCode: new Map([['rv-pending', 4]]) };
const run = (rules = 'v3', o = {}) => generateRecommendations(false, { 'nw-inbound': 12 }, {
  rules, selfShipSales: selfSales, pendingSlips, excluded: ['rv-excluded', 'nw-excl'], nowMs: NOW, ...o,
});
const trialsOf = (r) => r.data_quality.allocation.trials;

console.log('長期欠品の復活');
await t('欠品前の記録から: rv-hist は 30 日販売 60 → 60 個・上限 30・Amazon 推奨 20 の小さい方 = 20', () => {
  const tr = trialsOf(run());
  const x = tr.revive.find((r) => r.sku === 'rv-hist');
  assert.deepEqual([x.qty, x.history_usable, x.by_history, x.last_in_stock_sold_30d], [20, true, 60, 60]);
});

await t('履歴が無い・180 日より古い → 10 個', () => {
  const tr = trialsOf(run());
  assert.equal(tr.revive.find((r) => r.sku === 'rv-nohist').qty, 10);
  const old = tr.revive.find((r) => r.sku === 'rv-old');
  assert.deepEqual([old.qty, old.history_usable], [10, false]);
});

await t('🚨 出さない: 入荷待ち・出荷待ち伝票・恒久除外・自社日販が分からない (毎朝また候補になるのを止める)', () => {
  const tr = trialsOf(run());
  for (const sku of ['rv-inbound', 'rv-pending', 'rv-excluded', 'rv-selfunk']) assert.ok(!tr.revive.some((r) => r.sku === sku), sku);
  assert.ok(tr.skipped.revive_inbound >= 1 && tr.skipped.revive_pending_slip >= 1 && tr.skipped.revive_excluded >= 1 && tr.skipped.revive_self_unknown >= 1, JSON.stringify(tr.skipped));
});

await t('自社出荷ぶんを残す: 倉庫 35・自社日販 1 × 30 日 → 空き 5 → 5 個', () => {
  const x = trialsOf(run()).revive.find((r) => r.sku === 'rv-keep');
  assert.deepEqual([x.qty, x.free_cap], [5, 5]);
});

console.log('新規出品');
await t('FBA で一度も在庫・入荷を見ていない SKU を 10 個で。非表示・恒久除外・準備中・倉庫の空き無し は出さない', () => {
  const tr = trialsOf(run());
  assert.deepEqual(tr.new_listing.map((n) => [n.sku, n.qty]).sort(), [['nw-a', 10], ['nw-b', 10]]);
  assert.ok(tr.skipped.new_hidden >= 1 && tr.skipped.new_excluded >= 1 && tr.skipped.new_inbound >= 1 && tr.skipped.new_no_free_stock >= 1, JSON.stringify(tr.skipped));
  assert.ok(!tr.new_listing.some((n) => n.sku.startsWith('rv-')), '長期欠品 (在庫を見たことがある) は新規にしない = 排他');
});

await t('1 日の件数の上限 (倉庫の空きの大きい順) / 止める設定 / v2 では出さない', () => {
  db.updateSetting('v3_new_trial_max_skus', '1');
  try {
    const tr = trialsOf(run());
    assert.deepEqual(tr.new_listing.map((n) => n.sku), ['nw-a'], '空き 300 − 30 の方');
    assert.ok(tr.skipped.new_over_max_skus >= 1);
  } finally { db.updateSetting('v3_new_trial_max_skus', '50'); }
  db.updateSetting('v3_trials', 'off');
  try { assert.equal(trialsOf(run()).enabled, false); } finally { db.updateSetting('v3_trials', 'on'); }
  assert.equal(trialsOf(run('v2')), null);
});

await t('候補どうしも倉庫の空きを取り合う (出した分を引いていく)', () => {
  // nw-b を nw-a と同じ構成品にし、倉庫を 45 個に
  db.upsertSkuMappings([{ amazon_sku: 'nw-b', product_name: 'nw-b', ne_code: 'nw-a', logizard_code: 'nw-a' }]);
  db.replaceWarehouseInventory([...REVIVE, ...NEW].filter(([c, q]) => q > 0 && c !== 'nw-a').map(([code, q]) => stock(code, q)).concat([stock('nw-a', 45)]));
  try {
    // 空き = 45 − 30 = 15 → 先の 1 件が 10 個、次は 5 個
    const tr = trialsOf(run());
    const got = tr.new_listing.filter((n) => ['nw-a', 'nw-b'].includes(n.sku)).map((n) => n.qty).sort((a, b) => b - a);
    assert.deepEqual(got, [10, 5]);
  } finally {
    db.upsertSkuMappings([{ amazon_sku: 'nw-b', product_name: 'nw-b', ne_code: 'nw-b', logizard_code: 'nw-b' }]);
    db.replaceWarehouseInventory([...REVIVE, ...NEW].filter(([, q]) => q > 0).map(([code, q]) => stock(code, q)));
  }
});

await t('🚨 材料が読めない日は出さない (全 SKU が新規に見えるのを防ぐ)', () => {
  const tr = trialsOf(run('v3', { trialInputs: { everStocked: null, lastInStock: null, hidden: null, error: 'boom' } }));
  assert.deepEqual([tr.enabled, tr.reason, tr.new_listing.length], [false, 'inputs_unavailable', 0]);
});

console.log('記録');
await t('🚨 提案には入れない。影の下書きには要確認 (finding) として 1 SKU 1 行・試す数と根拠・承認しても何も動かない action', async () => {
  const r = run();
  const { proposals } = pickDraftRows(r.items);
  assert.ok(!proposals.some((p) => p.amazon_sku.startsWith('rv-') || p.amazon_sku.startsWith('nw-')), '試す候補は提案に入らない');
  const pg = new PGlite(); const pdb = pgliteAdapter(pg);
  await applyMigrations(pdb, { log: quiet });
  const rec = await recordShadowDraft(pdb, r, { log: quiet, now: new Date(NOW) });
  assert.equal(rec.trials, 6, '復活 4 (rv-hist・rv-nohist・rv-old・rv-keep) + 新規 2 (nw-a・nw-b)');
  assert.match(rec.summary, /試す候補 \d+ 件 \(復活 \d+ \/ 新規 \d+\)/);
  const { rows } = await pdb.query(`select decision_kind, summary, severity, proposed_action, inputs_ref, dedupe_key from ai.decisions
    where inputs_ref->>'generator' = $1 and proposed_action->>'action_type' = 'fba_trial_replenish' order by dedupe_key`, [GENERATOR]);
  assert.equal(rows.length, rec.trials);
  assert.ok(rows.every((x) => x.decision_kind === 'finding' && x.severity === 'info' && x.proposed_action.requires_approval === true));
  const hist = rows.find((x) => x.inputs_ref.amazon_sku === 'rv-hist');
  assert.match(hist.summary, /長期欠品の復活候補: .* を 20 個で試す/);
  assert.equal(hist.inputs_ref.trial.last_in_stock_sold_30d, 60);
  const nw = rows.find((x) => x.inputs_ref.amazon_sku === 'nw-a');
  assert.match(nw.summary, /新規出品の候補: .* を 10 個で試す/);
  assert.equal(new Set(rows.map((x) => x.dedupe_key)).size, rows.length, '1 SKU 1 行');
  const { rows: dup } = await pdb.query(`select dedupe_key, count(*)::int n from ai.decisions where status = 'new' and inputs_ref->>'generator' = $1 group by 1 having count(*) > 1`, [GENERATOR]);
  assert.deepEqual(dup, [], '同じ SKU に 2 行 (試す候補と「消えた行」など) を書かない');
});

console.log('Codex PR #1480 R1 の指摘');
await t('🚨 欠品前 = 在庫があった最新の日。その日に売れていなければ、昔売れていた日まで飛ばさず控えめな数 (Medium 4)', () => {
  // rv-old: 200 日前に売れていた行 (古い) のあと、5 日前に在庫あり・売れていない行を足す (日次の記録は在庫の欄を上書きしないので戻さない)
  db.savePlanningData([{ sku: 'rv-old', fba_available: 2, units_sold_30d: 0 }], daysAgo(5));
  const x = trialsOf(run()).revive.find((r) => r.sku === 'rv-old');
  assert.deepEqual([x.qty, x.history_state, x.last_in_stock_date, x.by_history], [10, 'unsold_in_stock', daysAgo(5), null]);
  assert.equal(trialsOf(run()).revive.find((r) => r.sku === 'rv-nohist').history_state, 'unsold_in_stock');
});

await t('🚨 新規セットで同じ構成品が 2 行: 構成数を合わせて空きを見る (倉庫 40・自社ぶん 30 → 5 セット。High)', () => {
  db.upsertSkuMappings([{ amazon_sku: 'nw-set', product_name: 'セット', ne_code: 'nw-part', logizard_code: 'nw-part', is_set: true,
    set_components: [{ ne_code: 'nw-part', qty: 1 }, { ne_code: 'nw-part', qty: 1 }] }]);
  db.replaceWarehouseInventory([...REVIVE, ...NEW].filter(([, q]) => q > 0).map(([code, q]) => stock(code, q)).concat([stock('nw-part', 40)]));
  try {
    const sales = { status: 'ok', map: new Map([...codes.map((c) => [c.toLowerCase(), 30]), ['nw-part', 30]]) };
    const x = trialsOf(run('v3', { selfShipSales: sales })).new_listing.find((n) => n.sku === 'nw-set');
    assert.deepEqual([x.qty, x.units], [5, [{ code: 'nw-part', qty: 2 }]]);
    assert.deepEqual([x.free_detail[0].warehouse, x.free_detail[0].self_keep], [40, 30], '空きの内訳を残す (Low)');
  } finally {
    db.replaceWarehouseInventory([...REVIVE, ...NEW].filter(([, q]) => q > 0).map(([code, q]) => stock(code, q)));
  }
});

await t('🚨 RESTOCK にある SKU (エンジンの計算対象) は新規にしない = 保留の行と 2 行にならない (Medium 2)', () => {
  // nw-a を RESTOCK に入れる (PLANNING は無い = 保留)。FBA で在庫を見たことはまだ無い
  db.saveRestockLatest([...REVIVE.map(([sku, , extra]) => ({ amazon_sku: sku, product_name: sku, fba_available: 0, units_sold_30d: 0, units_sold_7d: 0, amazon_recommended_qty: 20, ...(extra || {}) })),
    { amazon_sku: 'nw-a', product_name: 'nw-a', fba_available: 0, units_sold_30d: 0, units_sold_7d: 0, amazon_recommended_qty: null }]);
  try {
    const tr = trialsOf(run());
    assert.ok(!tr.new_listing.some((n) => n.sku === 'nw-a'));
    assert.ok(tr.skipped.new_in_restock >= 1);
  } finally {
    db.saveRestockLatest(REVIVE.map(([sku, , extra]) => ({ amazon_sku: sku, product_name: sku, fba_available: 0, units_sold_30d: 0, units_sold_7d: 0, amazon_recommended_qty: 20, ...(extra || {}) })));
  }
});

await t('🚨 その日の提案で使う数を先に引く: 同じ構成品の通常の補充 470 個のあと、倉庫 520・自社ぶん 30 → 空き 20 (Medium 5)', () => {
  // rv-keep の構成品を使う通常の補充 SKU を足す (FBA 在庫 5 日分・日販 10 → 目標 42 日分 = 420−50 = 370… ロケ補正込みで数える)
  db.upsertSkuMappings([{ amazon_sku: 'reg-x', product_name: 'reg-x', ne_code: 'rv-keep', logizard_code: 'rv-keep' }]);
  db.saveRestockLatest([...REVIVE.map(([sku, , extra]) => ({ amazon_sku: sku, product_name: sku, fba_available: 0, units_sold_30d: 0, units_sold_7d: 0, amazon_recommended_qty: 20, ...(extra || {}) })),
    { amazon_sku: 'reg-x', product_name: 'reg-x', fba_available: 50, units_sold_30d: 300, units_sold_7d: 70, amazon_recommended_qty: null }]);
  db.savePlanningLatest([...REVIVE.map(([sku]) => ({ sku, units_sold_7d: 0, per_unit_volume: 300, low_inv_fee_exempt: 'Yes' })), { sku: 'reg-x', units_sold_7d: 70, per_unit_volume: 300, low_inv_fee_exempt: 'Yes' }]);
  db.replaceWarehouseInventory([...REVIVE, ...NEW].filter(([c, q]) => q > 0 && c !== 'rv-keep').map(([code, q]) => stock(code, q)).concat([stock('rv-keep', 520)]));
  try {
    const r = run('v3', { smoothing: false });
    const regQty = r.items.find((i) => i.amazon_sku === 'reg-x').adjusted_qty;
    assert.ok(regQty > 0, `${regQty}`);
    const x = trialsOf(r).revive.find((t2) => t2.sku === 'rv-keep');
    const expectFree = Math.max(0, 520 - regQty - 30);
    if (expectFree >= 1) {
      assert.equal(x.free_cap, expectFree);
      assert.equal(x.free_detail[0].used_by_proposals, regQty);
    } else assert.equal(x, undefined);
  } finally {
    db.saveRestockLatest(REVIVE.map(([sku, , extra]) => ({ amazon_sku: sku, product_name: sku, fba_available: 0, units_sold_30d: 0, units_sold_7d: 0, amazon_recommended_qty: 20, ...(extra || {}) })));
    db.replaceWarehouseInventory([...REVIVE, ...NEW].filter(([, q]) => q > 0).map(([code, q]) => stock(code, q)));
  }
});

await t('🚨 翌日: 続く候補は書き直す・消えた候補は前日の行が無効・また出た候補も 1 行 / 材料が読めない日は partial (Medium 3・5)', async () => {
  const pg = new PGlite(); const pdb = pgliteAdapter(pg);
  await applyMigrations(pdb, { log: quiet });
  const open = async () => (await pdb.query(`select inputs_ref->>'amazon_sku' sku from ai.decisions where status = 'new' and proposed_action->>'action_type' = 'fba_trial_replenish' order by 1`)).rows.map((x) => x.sku);
  await recordShadowDraft(pdb, run(), { log: quiet, now: new Date(NOW) });
  const day1 = await open();
  assert.ok(day1.includes('nw-a') && day1.includes('rv-hist'));
  // 2 日目: nw-a を非表示にした → 消える。rv-hist は続く
  db.hideNewProductSkuBulk(['nw-a']);
  await recordShadowDraft(pdb, run(), { log: quiet, now: new Date(NOW + 86400e3) });
  const day2 = await open();
  assert.ok(!day2.includes('nw-a') && day2.includes('rv-hist'));
  // 3 日目: 非表示を戻した → また出る (1 行)
  db.unhideNewProductSku('nw-a');
  await recordShadowDraft(pdb, run(), { log: quiet, now: new Date(NOW + 2 * 86400e3) });
  const day3 = await open();
  assert.equal(day3.filter((s2) => s2 === 'nw-a').length, 1);
  const { rows: dup } = await pdb.query(`select dedupe_key from ai.decisions where status = 'new' and inputs_ref->>'generator' = $1 group by 1 having count(*) > 1`, [GENERATOR]);
  assert.deepEqual(dup, []);
  // 4 日目: 材料が読めない → 候補は出さない・partial・理由を要約に
  const rec = await recordShadowDraft(pdb, run('v3', { trialInputs: { everStocked: null, lastInStock: null, hidden: null, error: 'disk' } }), { log: quiet, now: new Date(NOW + 3 * 86400e3) });
  assert.deepEqual([rec.status, rec.trialsFailed, rec.trials], ['partial', true, 0]);
  assert.match(rec.summary, /試す候補を計算できなかった \(disk\)/);
  assert.deepEqual(await open(), [], '前日の候補は無効 (使える状態で残さない)');
});

await t('🚨 セット品なのに構成が空 (null・"null") なら候補にしない (代表コード 1 個の単品として数えない。Codex PR #1480 R2 Medium)', () => {
  db.upsertSkuMappings([
    { amazon_sku: 'nw-set-null', product_name: 'セット null', ne_code: 'nw-a', logizard_code: 'nw-a', is_set: true, set_components: null },
    { amazon_sku: 'nw-set-str', product_name: 'セット "null"', ne_code: 'nw-a', logizard_code: 'nw-a', is_set: true, set_components: 'null' },
  ]);
  const tr = trialsOf(run());
  assert.ok(!tr.new_listing.some((n) => n.sku.startsWith('nw-set-')));
  assert.ok(tr.skipped.new_invalid_mapping >= 2, JSON.stringify(tr.skipped));
  db.hideNewProductSkuBulk(['nw-set-null', 'nw-set-str']);   // 以降の試験に響かないように
});

await t('理由の文は欠品前の履歴の状態ごと (在庫はあったが売れていない を「古い・無い」と書かない。Codex PR #1480 R2 Low)', async () => {
  const r = run();
  const pg = new PGlite(); const pdb = pgliteAdapter(pg);
  await applyMigrations(pdb, { log: quiet });
  await recordShadowDraft(pdb, r, { log: quiet, now: new Date(NOW) });
  const { rows } = await pdb.query(`select inputs_ref->>'amazon_sku' sku, rationale from ai.decisions where proposed_action->>'action_type' = 'fba_trial_replenish'`);
  const by = Object.fromEntries(rows.map((x) => [x.sku, x.rationale]));
  assert.match(by['rv-nohist'], /在庫があった最新の日 \(\d{4}-\d{2}-\d{2}\) は売れていなかった ので 10 個/);
  assert.match(by['rv-hist'], /在庫があった最新の日 \(\d{4}-\d{2}-\d{2}\) の 30 日販売 60 個から 30 日分/);
});

await t('getTrialInputs: 在庫を見たことがある SKU・在庫があった最新の日の 30 日販売 (足さない)・非表示', () => {
  const x = db.getTrialInputs();
  assert.equal(x.error, null);
  assert.ok(x.everStocked.includes('rv-hist') && !x.everStocked.includes('nw-a'));
  assert.deepEqual(x.lastInStock.get('rv-hist'), { snapshot_date: daysAgo(20), units_sold_30d: 60 });
  assert.deepEqual(x.lastInStock.get('rv-nohist'), { snapshot_date: daysAgo(60), units_sold_30d: 0 }, '在庫があった最新の日の行 (売れていなくても) を使う');
  assert.ok(x.hidden.includes('nw-hidden'), JSON.stringify(x.hidden));
});

fs.rmSync(process.env.DATA_DIR, { recursive: true, force: true });
console.log(`\n${passed} 件 ok${process.exitCode ? ' (NG あり)' : ''}`);
