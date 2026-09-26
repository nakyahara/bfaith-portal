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

await t('候補どうしも倉庫の空きを取り合う・その日の提案で使う数を先に引く', () => {
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

await t('getTrialInputs: 在庫を見たことがある SKU・在庫があった最新の日の 30 日販売 (足さない)・非表示', () => {
  const x = db.getTrialInputs();
  assert.equal(x.error, null);
  assert.ok(x.everStocked.includes('rv-hist') && !x.everStocked.includes('nw-a'));
  assert.deepEqual(x.lastInStock.get('rv-hist'), { snapshot_date: daysAgo(20), units_sold_30d: 60 });
  assert.equal(x.lastInStock.has('rv-nohist'), false, '在庫はあったが売れていない行は使わない');
  assert.deepEqual(x.hidden, ['nw-hidden']);
});

fs.rmSync(process.env.DATA_DIR, { recursive: true, force: true });
console.log(`\n${passed} 件 ok${process.exitCode ? ' (NG あり)' : ''}`);
