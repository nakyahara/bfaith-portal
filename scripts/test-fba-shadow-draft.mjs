/**
 * test-fba-shadow-draft.mjs — FBA 補充「影の下書き」の受入試験
 *
 * 本物の Postgres も fba.db も要らない。DDL を流した PGlite に、計算エンジンの出力を真似た値を渡して、
 * 何が記録され・何が記録されないか・翌日どうなるかを見る。
 * 使い方: node scripts/test-fba-shadow-draft.mjs
 */
import assert from 'node:assert/strict';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';
import {
  recordShadowDraft, pickDraftRows, blockedReason, calmReason, cautionsOf, rationaleOf, dedupeKeyOf, inputGate, zeroReasonsOf, supersedeOpenRowsSafely, recordConnectFailure,
  newShadowRunId, resolveListings, RULE_VERSION, DOMAIN, JOB_ID, EXPIRES_HOURS, GENERATOR, AMAZON_SHOP_CODE,
} from '../apps/fba-replenishment/shadow-draft.mjs';
import { mergeRestockWithPlanning } from '../apps/fba-replenishment/calculation-engine.js';
import { normalizeRestockRow, normalizePlanningRow } from '../apps/fba-replenishment/sp-api-reports.js';
import { usSalesOf } from '../apps/fba-replenishment/db.js';
import { judgeInboundFetch } from '../apps/fba-replenishment/inbound-state.js';

let passed = 0;
function t(name, fn) { try { fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; } }
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; } }
const quiet = () => {};

/** 計算エンジンが出す 1 行の形 (要るところだけ) */
const GAPS_OK = {
  sales_7d_missing: false, sales_30d_missing: false, planning_missing: false,
  warehouse_row_missing: false, warehouse_missing_components: [], amazon_reco_missing: true,
  inbound_working_source: 'api', zero_filled: [],
};
const item = (o = {}) => ({
  amazon_sku: 'abc001', product_name: 'テスト商品', ne_code: 'abc001', asin: 'B000TEST01',
  is_set: false, invalid_mapping: false, stock_state: 'normal',
  fba_available: 3, effective_fba_stock: 3, fba_inbound_working_effective: 0,
  units_sold_7d: 7, units_sold_30d: 30, daily_sales: 1, days_of_supply: 3,
  reorder_point: 14, reorder_point_days: 14, target_days: 60, target_stock: 60,
  warehouse_available: 100, recommended_qty: 57, rounded_qty: 60, adjusted_qty: 60,
  amazon_recommended_qty: null, amazon_reco_capped: false, expiry_limited: false,
  location_adjusted: false, recent_arrival_adjusted: false, urgency_score: 88.5,
  alerts: [], exception_type: null, needs_replenishment: true, skipped_min_days: false,
  ...o,
  data_gaps: { ...GAPS_OK, ...(o.data_gaps || {}) },
});
const engineResult = (items, o = {}) => ({
  items, generated_at: '2026-09-10T21:00:00.000Z', snapshot_date: '2026-09-10',
  total_skus: items.length, errors: [],
  data_quality: { data_source: 'sp_api', unmapped_active_count: 0, unmapped_active: [], invalid_mapping_count: 0 },
  ...o,
});

console.log('どの行を記録するか');

t('提案 / 提案不能 / 送らなくてよい を分ける', () => {
  const { proposals, blocked, calm } = pickDraftRows([
    item({ amazon_sku: 'a', adjusted_qty: 60 }),                       // 提案
    item({ amazon_sku: 'b', adjusted_qty: 0, needs_replenishment: false }), // 送らなくてよい
    item({ amazon_sku: 'c', invalid_mapping: true, adjusted_qty: 10 }), // 提案不能
    item({ amazon_sku: 'd', data_gaps: { warehouse_row_missing: true } }), // 自社在庫が取れていない
  ]);
  assert.deepEqual(proposals.map((p) => p.amazon_sku), ['a']);
  assert.deepEqual(blocked.map((b) => [b.item.amazon_sku, b.reason]), [['c', 'invalid_mapping'], ['d', 'warehouse_unknown']]);
  assert.deepEqual(calm.map((c) => [c.item.amazon_sku, c.reason]), [['b', 'above_reorder_point']]);
});

t('🚨「0 個でよい」と「計算できなかった」を混ぜない (エンジンが 0 で埋めた**前**を見る)', () => {
  // 🚨 エンジンは取れなかった値を `|| 0` で 0 にしてしまう。だから 0 になった値ではなく data_gaps を見る
  assert.equal(blockedReason(item({ adjusted_qty: 0 })), null, '計算できて 0 個は「不能」ではない');
  assert.equal(blockedReason(item({ warehouse_available: 0, data_gaps: { warehouse_row_missing: true } })), 'warehouse_unknown',
    '倉庫に行が無い → 0 に見えても「取れていない」');
  assert.equal(blockedReason(item({ warehouse_available: 0 })), null, '行があって在庫 0 は「不明」ではない');
  assert.equal(blockedReason(item({ units_sold_30d: 0, data_gaps: { sales_30d_missing: true } })), 'sales_unknown');
  assert.equal(blockedReason(item({ units_sold_30d: 0 })), null, '売れていない は「取れていない」ではない');
  assert.equal(blockedReason(item({ data_gaps: { planning_missing: true } })), 'planning_missing',
    'PLANNING に無い = 販売数が 0 扱いになっている');
  assert.equal(blockedReason(item({ ne_code: null })), 'no_ne_code');
  assert.equal(blockedReason(item({ invalid_mapping: true })), 'invalid_mapping');
});

t('0 の理由を 1 つにまとめない (状態を先に見る)', () => {
  // 🚨 長期欠品・廃番候補は 30 日販売が 0 なので needs_replenishment も false になる。
  //    順番を間違えると全部「まだ発注点を下回っていない」に吸われて見えなくなる (Codex R2)
  assert.equal(calmReason(item({ stock_state: 'dead_candidate', needs_replenishment: false })), 'dead_candidate');
  assert.equal(calmReason(item({ stock_state: 'revivable_long_oos', needs_replenishment: false })), 'long_oos');
  assert.equal(calmReason(item({ skipped_min_days: true })), 'skipped_min_days');
  assert.equal(calmReason(item({ needs_replenishment: false })), 'above_reorder_point');
  assert.equal(calmReason(item({ warehouse_available: 0 })), 'no_warehouse_stock');
  assert.equal(calmReason(item({ warehouse_available: 5 })), 'zero_after_caps');
  // 例外の印はエンジンで数量を止めていないので、0 の理由にはしない
  assert.equal(calmReason(item({ exception_type: 'no_fba', needs_replenishment: false })), 'above_reorder_point');
});

t('数量は出せたが素性が怪しい点は cautions に残る', () => {
  assert.deepEqual(cautionsOf(item()), ['amazon_reco_missing'], '既定の fixture は Amazon 推奨だけ未取得');
  const c = cautionsOf(item({ data_gaps: { sales_7d_missing: true, per_unit_volume_missing: true, warehouse_missing_components: ['x'] } }));
  assert.ok(c.includes('sales_7d_missing'));
  assert.ok(c.includes('per_unit_volume_missing'));
  assert.ok(c.includes('warehouse_components_missing'));
});

t('なぜその数量かが言葉で残る', () => {
  const r = rationaleOf(item({ amazon_reco_capped: true, amazon_recommended_qty: 40 }));
  assert.match(r, /あと 3 日分/);
  assert.match(r, /発注点 14 日/);
  assert.match(r, /目標 60 日分/);
  assert.match(r, /自社在庫 100 個/);
  assert.match(r, /Amazon の推奨 40 個で頭打ち/);
});

t('実行の名前は時刻順に並ぶ', () => {
  const a = newShadowRunId(new Date('2026-09-10T21:00:00Z'));
  const b = newShadowRunId(new Date('2026-09-11T21:00:00Z'));
  assert.match(a, /^fbasd_\d{15}_[0-9a-f]{6}$/);
  assert.ok(a < b, `${a} < ${b}`);
});

t('同じ出品は大文字小文字が違っても同じ鍵になる', () => {
  assert.equal(dedupeKeyOf('ABC001'), dedupeKeyOf('abc001'));
  assert.match(dedupeKeyOf('abc001'), /^fba_replenishment:/);
});

console.log('\n取り込みの時点で「取れなかった」を 0 にしない');

t('🚨 RESTOCK の解析: 列が無い/空なら null (0 と混ぜない)', () => {
  // Codex R1〜R3 で 3 回やり直した根っこ。ここで 0 にすると以降どこでも区別できない
  const withVal = normalizeRestockRow({ 'SKU': 'a', 'Units Sold Last 30 Days': '30' });
  assert.equal(withVal.units_sold_30d, 30);
  const zero = normalizeRestockRow({ 'SKU': 'a', 'Units Sold Last 30 Days': '0' });
  assert.equal(zero.units_sold_30d, 0, '本当に 0 なら 0');
  const empty = normalizeRestockRow({ 'SKU': 'a', 'Units Sold Last 30 Days': '' });
  assert.equal(empty.units_sold_30d, null, '空欄は「取れていない」');
  const noCol = normalizeRestockRow({ 'SKU': 'a' });
  assert.equal(noCol.units_sold_30d, null, '列そのものが無いのも「取れていない」');
  // 前からある正解 (Amazon 推奨数) と同じ扱いになっている
  assert.equal(noCol.amazon_recommended_qty, null);
});

t('🚨 PLANNING の解析: 7 日販売も同じ', () => {
  assert.equal(normalizePlanningRow({ sku: 'a', 'units-shipped-t7': '7' }).units_sold_7d, 7);
  assert.equal(normalizePlanningRow({ sku: 'a', 'units-shipped-t7': '0' }).units_sold_7d, 0);
  assert.equal(normalizePlanningRow({ sku: 'a', 'units-shipped-t7': '' }).units_sold_7d, null);
  assert.equal(normalizePlanningRow({ sku: 'a' }).units_sold_7d, null);
});

t('取り込み → 結合 → 判定 が通しで「取れていない」を運ぶ', () => {
  // 実物の解析関数の出力を、実物の結合関数に通し、実物の判定にかける
  const restock = normalizeRestockRow({ 'SKU': 'abc001', 'Units Sold Last 30 Days': '', 'Available': '3' });
  const planning = normalizePlanningRow({ sku: 'abc001', 'units-shipped-t7': '7' });
  const snap = mergeRestockWithPlanning(restock, planning);
  assert.equal(snap.units_sold_30d, 0, '数値は今までどおり 0 に埋まる');
  assert.equal(snap._gaps.units_sold_30d, true, '🚨 印は「取れていない」のまま届く');
  const reason = blockedReason({ ne_code: 'x', invalid_mapping: false, data_gaps: {
    sales_30d_missing: snap._gaps.units_sold_30d, planning_missing: snap._gaps.planning_row_missing, warehouse_row_missing: false,
  } });
  assert.equal(reason, 'sales_unknown', '🚨 ここまで通って初めて「数量を出せない」と言える');

  // 本当に 0 のときは通らない (提案が出る)
  const ok = mergeRestockWithPlanning(normalizeRestockRow({ 'SKU': 'abc001', 'Units Sold Last 30 Days': '0' }), planning);
  assert.equal(ok._gaps.units_sold_30d, false);
  assert.equal(blockedReason({ ne_code: 'x', invalid_mapping: false, data_gaps: { sales_30d_missing: ok._gaps.units_sold_30d } }), null);
});

t('🚨 米国向けの保存値は以前と 1 つも変わらない (空欄・数字でない値・PLANNING 行なし を全部)', () => {
  // Codex R4: PLANNING に行あり・30 日販売が空欄・RESTOCK は 30 → 以前は 0 (30 にしてはいけない)
  const restock30 = normalizeRestockRow({ 'SKU': 'u1', 'Units Sold Last 30 Days': '30' });
  assert.equal(usSalesOf(normalizePlanningRow({ sku: 'u1', 'units-shipped-t30': '' }), restock30).sold30d, 0);
  // Codex R5: "--" 等の数字でない値は、以前は NaN (DB には NULL) だった。0 にしてはいけない
  const dash = usSalesOf(normalizePlanningRow({ sku: 'u1', 'units-shipped-t7': '--', 'units-shipped-t30': 'N/A' }), restock30);
  assert.ok(Number.isNaN(dash.sold7d), '7 日販売 "--" は以前どおり NaN (DB には NULL)');
  assert.ok(Number.isNaN(dash.sold30d), '30 日販売 "N/A" は以前どおり NaN (RESTOCK へは落ちない)');

  // 以前の解析 parseInt(v || 0) と 以前の式 で出していた値と、全部の組み合わせで一致する
  const oldParse = (v) => parseInt(v || 0);
  const planningValues = [undefined, '', ' ', '0', '12', '--', 'N/A'];   // undefined = PLANNING に行が無い
  const cellValues = [undefined, '', ' ', '0', '7', '--'];
  const restockValues = [undefined, '', ' ', '0', '30', '--', 'N/A'];
  let checked = 0;
  for (const pv30 of planningValues) {
    for (const pv7 of cellValues) {
      for (const rv of restockValues) {
        const planningPresent = pv30 !== undefined;
        const rawP = planningPresent ? { sku: 'u', 'units-shipped-t30': pv30, ...(pv7 === undefined ? {} : { 'units-shipped-t7': pv7 }) } : null;
        const rawR = { 'SKU': 'u', ...(rv === undefined ? {} : { 'Units Sold Last 30 Days': rv }) };
        // 以前: 解析は parseInt(v || 0)、保存は p.units_sold_7d ?? 0 / p.units_sold_30d ?? r.units_sold_30d ?? 0
        const oldP = planningPresent ? { units_sold_7d: oldParse(rawP['units-shipped-t7']), units_sold_30d: oldParse(pv30) } : {};
        const oldR = { units_sold_30d: oldParse(rawR['Units Sold Last 30 Days']) };
        const before = { sold7d: oldP.units_sold_7d ?? 0, sold30d: oldP.units_sold_30d ?? oldR.units_sold_30d ?? 0 };
        // 今: 新しい解析 → usSalesOf
        const now = usSalesOf(planningPresent ? normalizePlanningRow(rawP) : {}, normalizeRestockRow(rawR));
        const label = `planning30=${JSON.stringify(pv30)} planning7=${JSON.stringify(pv7)} restock30=${JSON.stringify(rv)}`;
        assert.ok(Object.is(now.sold7d, before.sold7d), `7日 ${label}: 以前 ${before.sold7d} / 今 ${now.sold7d}`);
        assert.ok(Object.is(now.sold30d, before.sold30d), `30日 ${label}: 以前 ${before.sold30d} / 今 ${now.sold30d}`);
        checked++;
      }
    }
  }
  assert.equal(checked, planningValues.length * cellValues.length * restockValues.length, '全組み合わせを見た');

  // 影の下書き側 (新しい値) は、数字でない値も「取れていない」として扱う
  assert.equal(normalizePlanningRow({ sku: 'u', 'units-shipped-t7': '--' }).units_sold_7d, null);
  assert.equal(normalizeRestockRow({ 'SKU': 'u', 'Units Sold Last 30 Days': 'N/A' }).units_sold_30d, null);
});

console.log('\n実物の計算エンジンを通す (0 化の前に印が取れているか)');

t('🚨 merge が 0 にする前に「取れていたか」を覚えている', () => {
  // Codex R2 の指摘: 欠損の判定より前に mergeRestockWithPlanning が 0 にしていた。
  // だから blockedReason は「0 になった値」ではなく、ここで作る印だけを見る
  const restock = {
    amazon_sku: 'abc001', product_name: 'テスト', asin: 'B01', fnsku: 'X1',
    fba_available: 3, fba_inbound_working: 0, fba_inbound_shipped: 0, fba_inbound_received: 0,
    days_of_supply: 3, your_price: 1980, amazon_recommended_qty: null, updated_at: '2026-09-10T00:00:00Z',
  };
  const planning = { units_sold_7d: 7, per_unit_volume: 0.01 };

  const missing30 = mergeRestockWithPlanning({ ...restock, units_sold_30d: null }, planning);
  assert.equal(missing30.units_sold_30d, 0, '数値は今までどおり 0 に埋まる');
  assert.equal(missing30._gaps.units_sold_30d, true, '🚨 でも「取れていなかった」印が残る');

  const zero30 = mergeRestockWithPlanning({ ...restock, units_sold_30d: 0 }, planning);
  assert.equal(zero30.units_sold_30d, 0);
  assert.equal(zero30._gaps.units_sold_30d, false, '本当に 0 (売れていない) は印が付かない');

  const noPlanning = mergeRestockWithPlanning({ ...restock, units_sold_30d: 30 }, null);
  assert.equal(noPlanning.units_sold_7d, 0);
  assert.equal(noPlanning._gaps.units_sold_7d, true, 'PLANNING が丸ごと無ければ 7 日販売も「取れていない」');
  assert.equal(noPlanning._gaps.planning_row_missing, true);

  const zero7 = mergeRestockWithPlanning({ ...restock, units_sold_30d: 30 }, { units_sold_7d: 0, per_unit_volume: 0.01 });
  assert.equal(zero7._gaps.units_sold_7d, false, '7 日販売が本当に 0 のときは印を付けない');
});

t('印から作った判定が「売れていない」と「取れていない」を分ける', () => {
  const fromGaps = (gaps) => blockedReason({ ne_code: 'x', invalid_mapping: false, data_gaps: {
    sales_30d_missing: !!gaps.units_sold_30d,
    planning_missing: !!gaps.planning_row_missing,
    warehouse_row_missing: false,
  } });
  const restock = { amazon_sku: 'a', updated_at: '2026-09-10T00:00:00Z' };
  assert.equal(fromGaps(mergeRestockWithPlanning({ ...restock, units_sold_30d: null }, { units_sold_7d: 1 })._gaps), 'sales_unknown');
  assert.equal(fromGaps(mergeRestockWithPlanning({ ...restock, units_sold_30d: 0 }, { units_sold_7d: 1 })._gaps), null);
  assert.equal(fromGaps(mergeRestockWithPlanning({ ...restock, units_sold_30d: 5 }, null)._gaps), 'planning_missing');
});

console.log('\nCompany DB への記録 (PGlite)');

const pg = new PGlite(); const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
const q = async (sql, p) => (await db.query(sql, p)).rows;

// Amazon の出品を 2 つ用意する (1 つは Company DB に無い SKU の試験に使う)
await q(`insert into core.listings (company_id, mall, shop_code, listing_code, title, status, created_by_type, created_by_id)
  values (1, 'amazon', 'main@A1VC38T7YXB528', 'abc001', 'テスト商品', 'active', 'system', 'test'),
         (1, 'amazon', 'main@A1VC38T7YXB528', 'abc002', 'テスト商品2', 'active', 'system', 'test')`);

await ta('Amazon の SKU を Company DB の出品に結び付けられる (大文字小文字を吸収)', async () => {
  const m = await resolveListings(db, ['ABC001', 'abc002', 'no-such-sku']);
  assert.equal(m.size, 2);
  assert.ok(m.get('abc001') > 0);
  assert.ok(!m.has('no-such-sku'));
});

let day1;
await ta('1 日目: 提案と提案不能が記録され、実行の記録が 1 行残る', async () => {
  day1 = await recordShadowDraft(db, engineResult([
    item({ amazon_sku: 'abc001', adjusted_qty: 60 }),
    item({ amazon_sku: 'abc002', adjusted_qty: 0, needs_replenishment: false }),   // 送らなくてよい → 記録しない
    item({ amazon_sku: 'abc003', invalid_mapping: true }),              // 提案不能 (Company DB にも無い)
  ]), { log: quiet, now: new Date('2026-09-10T21:00:00Z') });

  assert.equal(day1.proposals, 1);
  assert.equal(day1.blocked, 1);
  assert.equal(day1.calm, 1);
  assert.deepEqual(day1.calmByReason, { above_reorder_point: 1 });
  assert.equal(day1.status, 'partial', '提案不能があるので partial');

  const rows = await q(`select decision_kind, subject_type, subject_id, summary, severity, status, dedupe_key,
                               rule_version, generated_by, autonomy_level, proposed_action, inputs_ref, expires_at
                          from ai.decisions where domain = $1 order by decision_kind, dedupe_key`, [DOMAIN]);
  assert.equal(rows.length, 3, '提案 1 + 出せない 1 + run 単位の記録 1 (送らなくてよい行は 1 件ずつは記録しない)');
  const runRow = rows.find((r) => r.dedupe_key === `${DOMAIN}:__run__`);
  assert.ok(runRow, '🚨 全部 0 の日でも入力が残るように、run 単位の記録を必ず 1 行置く');
  assert.equal(runRow.inputs_ref.run_summary, true);
  assert.equal(runRow.inputs_ref.counts.proposals, 1);
  assert.ok(runRow.inputs_ref.data_quality, 'その日のデータ品質も残る');

  const finding = rows.find((r) => r.decision_kind === 'finding' && r.inputs_ref.blocked_reason);
  assert.equal(finding.severity, 'warn');
  assert.equal(finding.subject_id, null, 'Company DB に無い出品は結び付けない (が、記録は残す)');
  assert.match(finding.summary, /数量を出せない \(invalid_mapping\)/);
  assert.equal(finding.inputs_ref.blocked_reason, 'invalid_mapping');
  assert.equal(finding.inputs_ref.listing_resolved, false);

  const p = rows.find((r) => r.decision_kind === 'proposal');
  assert.equal(p.subject_type, 'listing');
  assert.ok(Number(p.subject_id) > 0, '出品に結び付いている');
  assert.equal(p.status, 'new');
  assert.equal(p.autonomy_level, 0, '🚨 影の段階なので必ず 0');
  assert.equal(p.generated_by, 'rule', '🚨 数量を決めるのは AI ではなく決定論的エンジン');
  assert.equal(p.rule_version, RULE_VERSION);
  assert.match(p.summary, /60 個/);
  assert.equal(p.proposed_action.action_type, 'fba_replenish');
  assert.equal(p.proposed_action.requires_approval, true);
  assert.equal(p.proposed_action.parameters.qty, 60);
  assert.equal(p.inputs_ref.data_as_of, '2026-09-10', 'どの日のデータで計算したかが残る');
  assert.equal(p.inputs_ref.run_id, day1.runId);
  assert.equal(p.inputs_ref.recommended_qty, 57, '丸める前の数量も残る');
  assert.equal(p.inputs_ref.rounded_qty, 60);
  assert.equal(p.inputs_ref.prev_qty, null, '1 日目なので前回は無い');
  // 期限 (承認できる時間) が入っている
  const hours = (new Date(p.expires_at) - new Date('2026-09-10T21:00:00Z')) / 3600000;
  assert.equal(Math.round(hours), EXPIRES_HOURS);

  const runs = await q(`select job_id, status, summary, host from ops.job_runs where job_id = $1`, [JOB_ID]);
  assert.equal(runs.length, 1);
  assert.equal(runs[0].status, 'partial');
  assert.match(runs[0].summary, /提案 1 件/);
  assert.match(runs[0].summary, /送らなくてよい 1 件/);
  assert.match(runs[0].summary, /数量を出せない 1 件/);
  assert.match(runs[0].summary, /above_reorder_point:1/, '0 の理由も内訳で残る');
  assert.match(runs[0].summary, /出どころ sp_api/);
  assert.match(runs[0].summary, /対象日 2026-09-10/);
});

await ta('2 日目: 前日ぶんは差し替え済みになり、数量の変化が数えられる', async () => {
  const day2 = await recordShadowDraft(db, engineResult([
    item({ amazon_sku: 'abc001', adjusted_qty: 24 }),                   // 数量が変わった
    item({ amazon_sku: 'abc002', adjusted_qty: 12 }),                   // 今日から提案に上がった
    // abc003 は今日は出てこない (消えた)
  ]), { log: quiet, now: new Date('2026-09-11T21:00:00Z') });

  assert.equal(day2.proposals, 2);
  assert.equal(day2.added, 1, 'abc002 が新しく上がった');
  assert.equal(day2.changed, 1, 'abc001 の数量が変わった');
  assert.equal(day2.gone, 1, 'abc003 が消えた');
  assert.equal(day2.goneSame, 0, '1 回目なので「同じ状態のまま」は無い');
  assert.equal(day2.status, 'ok', '提案不能が無くなった');

  const open = await q(`select dedupe_key, decision_kind, (inputs_ref->>'adjusted_qty')::int as qty,
                                (inputs_ref->>'prev_qty')::int as prev, inputs_ref->>'calm_reason' as calm_reason
                          from ai.decisions where domain = $1 and status = 'new' order by dedupe_key`, [DOMAIN]);
  assert.equal(open.length, 4, '今日の提案 2 件 + 昨日から消えた 1 件 + run 単位の記録 1 件');
  const p1 = open.find((r) => r.dedupe_key.endsWith('abc001'));
  assert.equal(p1.qty, 24);
  assert.equal(p1.prev, 60, '🚨 前回いくつだったかが残る (昨日と何が変わったかを追える)');
  const p2 = open.find((r) => r.dedupe_key.endsWith('abc002'));
  assert.equal(p2.prev, null, '前回は提案に上がっていなかった');
  // 🚨 昨日は出ていたのに今日は出ない行も残す (黙って消えると「なぜ消えたか」を追えない)
  const g = open.find((r) => r.dedupe_key.endsWith('abc003'));
  assert.equal(g.decision_kind, 'finding');
  assert.equal(g.calm_reason, 'not_in_items');

  const superseded = await q(`select count(*)::int as n from ai.decisions where domain = $1 and status = 'superseded'`, [DOMAIN]);
  assert.equal(superseded[0].n, 3, '🚨 前日の 3 件は差し替え済み (古い数量が残らない)');
});

await ta('人の承認は付いていない (影の段階では誰も承認しない)', async () => {
  const n = (await q('select count(*)::int as n from ai.decision_reviews'))[0].n;
  assert.equal(n, 0);
  const acted = (await q('select count(*)::int as n from ai.actions'))[0].n;
  assert.equal(acted, 0, '🚨 実行の要求も作らない (記録するだけ)');
});

await ta('途中で失敗したら何も残らない (1 回ぶんが中途半端に入らない)', async () => {
  const before = (await q(`select count(*)::int as n from ai.decisions where domain = $1`, [DOMAIN]))[0].n;
  const runsBefore = (await q(`select count(*)::int as n from ops.job_runs`))[0].n;
  // 途中の 1 件だけ DB に断らせる (印を含む summary を拒む制約を一時的に足す)
  await db.exec("alter table ai.decisions add constraint zz_test_reject check (summary not like '%ZZ-FAIL%')");
  const broken = engineResult([
    item({ amazon_sku: 'abc001', adjusted_qty: 5 }),
    item({ amazon_sku: 'abc002', adjusted_qty: 5, product_name: 'ZZ-FAIL' }),
  ]);
  await assert.rejects(() => recordShadowDraft(db, broken, { log: quiet, now: new Date('2026-09-12T21:00:00Z') }));
  await db.exec('alter table ai.decisions drop constraint zz_test_reject');
  assert.equal((await q(`select count(*)::int as n from ai.decisions where domain = $1`, [DOMAIN]))[0].n, before);
  // 今日の行は 1 行も入らない (巻き戻る)。ただし前日以前の提案は、古い数量で使われないよう無効にしてある
  //   (2026-09-24 変更。以前は「前日ぶんも new に巻き戻る」だった。Codex PR #1438 R1 High 2)
  const stillOpen = (await q(`select count(*)::int as n from ai.decisions where domain = $1 and status = 'new'`, [DOMAIN]))[0].n;
  assert.equal(stillOpen, 0, '前日以前の提案は無効 (superseded) のまま');
  // 🚨 中身は巻き戻すが「この日は失敗した」事実は残す (連続成功を数えられるように)
  const runs = await q(`select status, summary from ops.job_runs order by job_run_id desc limit 1`);
  assert.equal(runs.length ? runs[0].status : null, 'fail');
  assert.match(runs[0].summary, /記録に失敗/);
  assert.equal((await q(`select count(*)::int as n from ops.job_runs`))[0].n, runsBefore + 1);
});

await ta('データが取れていない日は partial として残る', async () => {
  const r = await recordShadowDraft(db, engineResult(
    [item({ amazon_sku: 'abc001', adjusted_qty: 10 })],
    { data_quality: { data_source: 'cache', unmapped_active_count: 3, unmapped_active: ['x', 'y', 'z'], invalid_mapping_count: 0 } },
  ), { log: quiet, now: new Date('2026-09-13T21:00:00Z') });
  assert.equal(r.status, 'partial');
  assert.match(r.summary, /未マップ\(実績あり\) 3 件/);
  const last = (await q(`select summary, status from ops.job_runs order by job_run_id desc limit 1`))[0];
  assert.equal(last.status, 'partial');
  assert.match(last.summary, /cache|未マップ/);
});

await ta('🚨 計算そのものが失敗した日は「何も要らない」とは記録せず、前日以前の提案も使えない状態にする', async () => {
  // スナップショットが無い等でエンジンが { items: [], errors: [...] } を返す日。
  // 以前は前日の提案を残していたが、自動で使うと古い数量で送ってしまう (Codex 2026-09-24 設計レビュー 2 High 2)。
  // → 提案を 0 件で「今日は要らない」とも書かず、失敗として残し、前日以前の未処理の提案は superseded にする
  const openBefore = (await q(`select count(*)::int as n from ai.decisions where domain = $1 and status = 'new'`, [DOMAIN]))[0].n;
  assert.ok(openBefore > 0, '前提: 開いている提案がある');
  const r = await recordShadowDraft(db, {
    items: [], errors: ['スナップショットがありません。SP-APIレポートを取得してください。'],
  }, { log: quiet, now: new Date('2026-09-14T21:00:00Z') });
  assert.equal(r.ok, false);
  assert.equal(r.engineFailed, true);
  assert.equal(r.status, 'fail');
  const openAfter = (await q(`select count(*)::int as n from ai.decisions where domain = $1 and status = 'new'`, [DOMAIN]))[0].n;
  assert.equal(openAfter, 0, '🚨 前日以前の提案は使えない (superseded)');
  const proposalsToday = (await q(`select count(*)::int as n from ai.decisions where domain = $1 and inputs_ref->>'run_id' = $2`, [DOMAIN, r.runId]))[0].n;
  assert.equal(proposalsToday, 0, '「今日は何も要らない」という行は書かない');
  const last = (await q(`select status, summary from ops.job_runs order by job_run_id desc limit 1`))[0];
  assert.equal(last.status, 'fail', '失敗として履歴に残る (連続成功を数えられる)');
  assert.match(last.summary, /スナップショットがありません/);
});

await ta('未マッピング (計算対象にすら入らない SKU) も残す', async () => {
  const r = await recordShadowDraft(db, engineResult([item({ amazon_sku: 'abc001', adjusted_qty: 30 })], {
    data_quality: {
      data_source: 'restock',
      unmapped_active_count: 1,
      unmapped_active: [{ sku: 'zzz999', units_sold_30d: 12, fba_available: 3, fba_inbound: 0, amazon_recommended_qty: null, non_fba_sales_30d: 0 }],
      unmapped_inactive_count: 0, unmapped_inactive_skus: [], invalid_mapping_count: 0,
    },
  }), { log: quiet, now: new Date('2026-09-15T21:00:00Z') });
  assert.equal(r.unmappedActive, 1);
  assert.equal(r.status, 'partial');
  const f2 = (await q(`select summary, severity, inputs_ref->>'blocked_reason' as reason
                        from ai.decisions where domain = $1 and status = 'new' and inputs_ref->>'blocked_reason' = 'unmapped_active'`, [DOMAIN]))[0];
  assert.ok(f2, '未マッピングが 1 件記録されている');
  assert.equal(f2.severity, 'warn');
  assert.match(f2.summary, /SKU 対応表に無い/);
});

await ta('同じ店舗に同じ SKU は 2 つ作れない (DB が止める) ので、店舗内では迷わない', async () => {
  // 大文字違いでも listing_norm は同じ → 一意制約に当たる。だから「後勝ちで別の出品に付く」は起きない
  await assert.rejects(
    () => q(`insert into core.listings (company_id, mall, shop_code, listing_code, title, status, created_by_type, created_by_id)
             values (1, 'amazon', 'main@A1VC38T7YXB528', 'ABC002', '大文字の別出品', 'active', 'system', 'test')`),
    /listings_mall_shop_code_listing_norm_key|duplicate key/);
  // それでも念のため、候補が 2 つ以上なら結び付けない作りにしてある (店舗をまたぐ取り違えの保険)
  const m = await resolveListings(db, ['abc001']);
  assert.ok(m.has('abc001'));
});

await ta('別の店舗の同じ SKU に結び付けない', async () => {
  await q(`insert into core.listings (company_id, mall, shop_code, listing_code, title, status, created_by_type, created_by_id)
    values (1, 'amazon', 'other-shop', 'zzz111', '別店舗', 'active', 'system', 'test')`);
  const m = await resolveListings(db, ['zzz111']);
  assert.equal(m.size, 0, '日本店だけを見る');
  const m2 = await resolveListings(db, ['zzz111'], { shopCode: 'other-shop' });
  assert.equal(m2.size, 1, '店舗を指定すれば引ける');
  await q(`delete from core.listings where listing_code = 'zzz111'`);
});

await ta('🚨 人が触った行と、他の出どころの行は差し替えない', async () => {
  // 人が見始めた行 (reviewable) と、別の仕組みが作った行を置いておく
  await q(`insert into ai.decisions (company_id, domain, decision_kind, summary, inputs_ref, generated_by, status, dedupe_key)
           values (1, $1, 'proposal', '人が見ている行', '{"generator":"fba-shadow-draft"}'::jsonb, 'rule', 'reviewable', 'x:human'),
                  (1, $1, 'proposal', '別の仕組みの行', '{"generator":"someone-else"}'::jsonb, 'llm', 'new', 'x:other')`, [DOMAIN]);
  await recordShadowDraft(db, engineResult([item({ amazon_sku: 'abc001', adjusted_qty: 7 })]),
    { log: quiet, now: new Date('2026-09-16T21:00:00Z') });
  const human = (await q(`select status from ai.decisions where dedupe_key = 'x:human'`))[0];
  const other = (await q(`select status from ai.decisions where dedupe_key = 'x:other'`))[0];
  assert.equal(human.status, 'reviewable', '人が見始めた行はそのまま');
  assert.equal(other.status, 'new', '別の出どころの行はそのまま');
});

await ta('準備中数量の取れ方が残る (取れていない日は partial)', async () => {
  const r = await recordShadowDraft(db, engineResult([item({ amazon_sku: 'abc001', adjusted_qty: 9 })]), {
    log: quiet, now: new Date('2026-09-17T21:00:00Z'),
    inboundState: { source: 'failed', at: '2026-09-17T21:00:00Z', count: 0, error: 'miniPC 応答なし' },
    settings: { target_days_default: 60 },
  });
  assert.equal(r.status, 'partial', '🚨 2026-06-30 の事故と同じ形 (静かに欠けて過小評価) を partial で見せる');
  assert.match(r.summary, /準備中=failed/);
  const p = (await q(`select inputs_ref from ai.decisions where domain = $1 and status = 'new' and decision_kind = 'proposal' limit 1`, [DOMAIN]))[0];
  assert.equal(p.inputs_ref.inbound_working_state.source, 'failed');
  assert.equal(p.inputs_ref.settings.target_days_default, 60, 'その日の設定も残る');
  assert.equal(p.inputs_ref.generator, GENERATOR, 'どの仕組みが作った行かが残る');
  assert.ok(p.inputs_ref.data_gaps, '0 で埋めた項目の印も残る');
});

await ta('🚨 取れなかった日は、保存済みを使い回しても正常に化けない', async () => {
  // 10 分以内の再呼び出しで source が 'cache' に上書きされると、取れなかった事実が消える (Codex R2/R3)
  const r = await recordShadowDraft(db, engineResult([item({ amazon_sku: 'abc001', adjusted_qty: 4 })]), {
    log: quiet, now: new Date('2026-09-18T21:00:00Z'),
    inboundState: { source: 'failed', at: '2026-09-18T20:00:00Z', count: 0, error: 'miniPC 応答なし', reused_cache: true, reused_at: '2026-09-18T21:00:00Z', last_success_at: '2026-09-17T21:00:00Z' },
  });
  assert.equal(r.status, 'partial', '使い回しても「取れなかった」は残る');
  assert.match(r.summary, /準備中=failed\(使い回し\)/);
  const p = (await q(`select inputs_ref from ai.decisions where domain = $1 and status = 'new' and decision_kind = 'proposal' limit 1`, [DOMAIN]))[0];
  assert.equal(p.inputs_ref.inbound_working_state.source, 'failed');
  assert.equal(p.inputs_ref.inbound_working_state.reused_cache, true);
  assert.equal(p.inputs_ref.inbound_working_state.last_success_at, '2026-09-17T21:00:00Z', '最後に取れた時刻が残る');
});

await ta('取れている日は ok のまま', async () => {
  const r = await recordShadowDraft(db, engineResult([item({ amazon_sku: 'abc001', adjusted_qty: 4 })]), {
    log: quiet, now: new Date('2026-09-19T21:00:00Z'),
    inboundState: { source: 'fresh', at: '2026-09-19T21:00:00Z', count: 120, error: null, reused_cache: false, reused_at: null, last_success_at: '2026-09-19T21:00:00Z' },
  });
  assert.equal(r.status, 'ok');
  assert.match(r.summary, /準備中=fresh\(120\)/);
});

await ta('🚨 同じ状態のまま持ち越した行が比較元になり、4 日目の変化を見落とさない', async () => {
  // Codex R4 の再現: 1 日目 提案 → 2 日目 補充不要 → 3 日目 同じ (書かない) → 4 日目 廃番候補。
  // 3 日目に比較元を差し替えてしまうと、4 日目の「理由が変わった」が記録されなかった
  const sku = 'carry001';
  await q(`insert into core.listings (company_id, mall, shop_code, listing_code, title, status, created_by_type, created_by_id)
    values (1, 'amazon', 'main@A1VC38T7YXB528', $1, '持ち越し試験', 'active', 'system', 'test')`, [sku]);
  const key = dedupeKeyOf(sku);
  const openRows = async () => (await q(`select decision_kind, inputs_ref->>'calm_reason' as reason, status
                                            from ai.decisions where dedupe_key = $1 order by decision_id`, [key]));
  const day = (d) => new Date(`2026-10-0${d}T21:00:00Z`);

  await recordShadowDraft(db, engineResult([item({ amazon_sku: sku, adjusted_qty: 20 })]), { log: quiet, now: day(1) });
  await recordShadowDraft(db, engineResult([item({ amazon_sku: sku, adjusted_qty: 0, needs_replenishment: false })]), { log: quiet, now: day(2) });
  let rows = await openRows();
  assert.equal(rows.filter((r) => r.status === 'new').length, 1);
  assert.equal(rows.find((r) => r.status === 'new').reason, 'above_reorder_point', '2 日目: 提案から「補充不要」へ');

  const d3 = await recordShadowDraft(db, engineResult([item({ amazon_sku: sku, adjusted_qty: 0, needs_replenishment: false })]), { log: quiet, now: day(3) });
  assert.ok(d3.goneSame >= 1, '3 日目: 同じ状態なので書かない');
  rows = await openRows();
  const open3 = rows.filter((r) => r.status === 'new');
  assert.equal(open3.length, 1, '🚨 3 日目: 2 日目の行を差し替えずに開いたまま持ち越す (比較元を残す)');
  assert.equal(open3[0].reason, 'above_reorder_point');

  await recordShadowDraft(db, engineResult([item({ amazon_sku: sku, adjusted_qty: 0, needs_replenishment: false, stock_state: 'dead_candidate' })]), { log: quiet, now: day(4) });
  rows = await openRows();
  const open4 = rows.filter((r) => r.status === 'new');
  assert.equal(open4.length, 1);
  assert.equal(open4[0].reason, 'dead_candidate', '🚨 4 日目: 「補充不要 → 廃番候補」の変化を記録できる');
  assert.equal(rows.filter((r) => r.reason === 'above_reorder_point' && r.status === 'superseded').length, 1,
    '持ち越していた行は、変化があった日に差し替え済みになる');
});

await ta('前回が「提案」でなかったものは、数量の変化ではなく「新しく上がった」と数える', async () => {
  const sku = 'revive001';
  await recordShadowDraft(db, engineResult([item({ amazon_sku: sku, invalid_mapping: true })]), { log: quiet, now: new Date('2026-10-10T21:00:00Z') });
  const r = await recordShadowDraft(db, engineResult([item({ amazon_sku: sku, adjusted_qty: 8 })]), { log: quiet, now: new Date('2026-10-11T21:00:00Z') });
  assert.equal(r.added, 1, '前回は「数量を出せない」だった → 今回は新しく上がった');
  assert.equal(r.changed, 0);
});

await ta('補正に使った棚と、入力ごとの取り込み時刻が残る (あとから同じ計算をやり直せる)', async () => {
  const locs = { abc001: [
    { location: 'P-01-01', block: 'A', qty: 30, expiry: '2027-03-31', biz_type: 'normal', order: 1 },
    { location: 'P-01-02', block: 'A', qty: 50, expiry: '2027-03-31', biz_type: 'normal', order: 2 },
  ] };
  const freshness = {
    restock_updated_at: '2026-10-12 06:01:00', restock_rows: 1200,
    planning_updated_at: '2026-10-11 06:02:00', planning_rows: 1180,     // PLANNING だけ 1 日古い
    warehouse_uploaded_at: '2026-10-12 05:40:00', warehouse_rows: 8000,
  };
  await recordShadowDraft(db, engineResult([item({ amazon_sku: 'abc001', adjusted_qty: 80, location_adjusted: true, location_detail: 'P-01-02まで累積80個', location_inputs: locs })]), {
    log: quiet, now: new Date('2026-10-12T21:00:00Z'), inputFreshness: freshness,
  });
  const p = (await q(`select inputs_ref from ai.decisions where domain = $1 and status = 'new' and decision_kind = 'proposal' and dedupe_key = $2`, [DOMAIN, dedupeKeyOf('abc001')]))[0];
  assert.deepEqual(p.inputs_ref.location_inputs, locs, '🚨 補正後の数量 (80 = 30 + 50) を棚の一覧から再現できる');
  const run = (await q(`select inputs_ref from ai.decisions where dedupe_key = $1 and status = 'new'`, [`${DOMAIN}:__run__`]))[0];
  assert.equal(run.inputs_ref.input_freshness.planning_updated_at, '2026-10-11 06:02:00', 'PLANNING だけ古い日を見分けられる');
  assert.equal(run.inputs_ref.input_freshness.warehouse_rows, 8000);
  assert.equal(run.inputs_ref.run_id, p.inputs_ref.run_id, '各行と run 単位の記録は run_id でつながる');
});

console.log('\n入力の関所と 0 の理由 (2026-09-24)');

const NOW = new Date('2026-09-24T00:40:00Z');   // 09:40 JST
// 元データを取った時刻は UTC ('2026-09-23 22:53' = 9/24 07:53 JST)。倉庫の取り込みもこのプロセスの localtime (試験は UTC 想定の文字列)
const FRESH = { restock_source_at: '2026-09-23 22:53:00', restock_source_missing: 0, planning_source_at: '2026-09-23 22:53:00', planning_source_missing: 0, warehouse_uploaded_at: '2026-09-23 23:50:00' };
const DQ_OK = { allocation: { mode: 'equal_days', self_sales: { used: true, status: 'ok' }, pending_slips: { status: 'ok' } } };
const IN_OK = { source: 'fresh', count: 120 };

t('関所: すべてそろっていれば止めない (Amazon のレポートは前日の朝に取ったものでもよい = 朝の取得前)', () => {
  const whLocal = (utc) => new Date(Date.parse(utc.replace(' ', 'T') + 'Z')).toLocaleString('sv-SE').replace('T', ' ');   // このプロセスの localtime で保存される
  const f = { ...FRESH, warehouse_uploaded_at: whLocal('2026-09-23 23:50:00') };
  assert.deepEqual(inputGate({ inboundState: IN_OK, inputFreshness: f, dq: DQ_OK, now: NOW }).reasons, []);
  const yday = { ...f, restock_source_at: '2026-09-22 22:53:00', planning_source_at: '2026-09-22 22:53:00' };   // 26 時間前
  assert.deepEqual(inputGate({ inboundState: IN_OK, inputFreshness: yday, dq: DQ_OK, now: NOW }).reasons, []);
});
t('🚨 関所: 準備中が取れていない (9/17 型) / 空 / 古いのを使い回した は止める', () => {
  for (const source of ['failed', 'empty', 'stale_cache', 'none', 'inconsistent']) {
    const g = inputGate({ inboundState: { source, error: 'Access to requested resource is denied.' }, inputFreshness: FRESH, dq: DQ_OK, now: NOW });
    assert.deepEqual(g.reasons.map((r) => r.code), ['inbound_working_not_fresh'], source);
  }
  assert.deepEqual(inputGate({ inboundState: null, inputFreshness: FRESH, dq: DQ_OK, now: NOW }).reasons.map((r) => r.code), ['inbound_working_not_fresh']);
});
t('🚨 関所: RESTOCK・PLANNING の片方でも古い / 取得時刻の無い行がある (9/19〜9/22 型)・倉庫在庫が 36 時間より古い は止める (Codex PR #1438 R1 High 1)', () => {
  const whLocal = (utc) => new Date(Date.parse(utc.replace(' ', 'T') + 'Z')).toLocaleString('sv-SE').replace('T', ' ');
  const base = { ...FRESH, warehouse_uploaded_at: whLocal('2026-09-23 23:50:00') };
  const codes = (f) => inputGate({ inboundState: IN_OK, inputFreshness: { ...base, ...f }, dq: DQ_OK, now: NOW }).reasons.map((r) => r.code);
  assert.deepEqual(codes({ restock_source_at: '2026-09-17 22:53:00' }), ['fba_report_stale'], 'RESTOCK だけ古い');
  assert.deepEqual(codes({ planning_source_at: '2026-09-17 22:53:00' }), ['fba_report_stale'], 'PLANNING だけ古い');
  assert.deepEqual(codes({ planning_source_at: null }), ['fba_report_stale']);
  assert.deepEqual(codes({ restock_source_missing: 12 }), ['fba_report_stale'], '取得時刻の無い行がある (移行直後)');
  assert.deepEqual(codes({ warehouse_uploaded_at: whLocal('2026-09-22 11:00:00') }), ['warehouse_stale']);
  assert.deepEqual(codes({ warehouse_uploaded_at: null }), ['warehouse_stale']);
});
t('関所: 自社日販が使えない / 出荷待ちの FBA 伝票が数えられない は止める。自社ぶんを残さない設定なら自社日販は見ない', () => {
  const whLocal = (utc) => new Date(Date.parse(utc.replace(' ', 'T') + 'Z')).toLocaleString('sv-SE').replace('T', ' ');
  const f = { ...FRESH, warehouse_uploaded_at: whLocal('2026-09-23 23:50:00') };
  const codes = (al) => inputGate({ inboundState: IN_OK, inputFreshness: f, dq: { allocation: al }, now: NOW }).reasons.map((r) => r.code);
  assert.deepEqual(codes({ mode: 'equal_days', self_sales: { used: false, status: 'stale' }, pending_slips: { status: 'ok' } }), ['self_sales_unavailable']);
  assert.deepEqual(codes({ mode: 'off', self_sales: { used: false, status: 'off' }, pending_slips: { status: 'ok' } }), []);
  assert.deepEqual(codes({ mode: 'equal_days', self_sales: { used: true }, pending_slips: { status: 'error', error: 'x' } }), ['pending_slips_unknown']);
  assert.deepEqual(codes({ mode: 'equal_days', self_sales: { used: true }, pending_slips: { status: 'inbound_stale' } }), [], '納品実績が古い = 多めに引く側なので止めない');
});
t('自社日販が分からない構成品を使う SKU は「数量を出せない」/ 新しい 0 の理由', () => {
  assert.equal(blockedReason(item({ data_gaps: { self_sales_missing: true } })), 'self_sales_unknown');
  assert.equal(calmReason(item({ adjusted_qty: 0, amazon_reco_capped: true, amazon_recommended_qty: 0 })), 'amazon_reco_zero');
  assert.equal(calmReason(item({ adjusted_qty: 0, expiry_limited: true, expiry_same_qty: 0 })), 'expiry_zero');
});
t('0 の理由を残す対象 = 30 日販売あり・Amazon 推奨あり・長期欠品で倉庫にあるもの。段階ごとの数量つき', () => {
  const rows = zeroReasonsOf([
    { item: item({ amazon_sku: 'sold', adjusted_qty: 0, raw_needed_before_amazon_cap: 40, amazon_recommended_qty: 0, amazon_reco_capped: true,
      allocation: { before: 0, after: 0, self_cut: 0, shared_cut: 0, min_days_cut: 0 } }), reason: 'amazon_reco_zero' },
    { item: item({ amazon_sku: 'oos', units_sold_30d: 0, stock_state: 'revivable_long_oos', warehouse_available: 50, adjusted_qty: 0 }), reason: 'long_oos' },
    { item: item({ amazon_sku: 'dead_nostock', units_sold_30d: 0, stock_state: 'dead_candidate', warehouse_available: 0, adjusted_qty: 0 }), reason: 'dead_candidate' },
    { item: item({ amazon_sku: 'quiet', units_sold_30d: 0, adjusted_qty: 0 }), reason: 'above_reorder_point' },
  ]);
  assert.deepEqual(rows.map((r) => r.sku), ['sold', 'oos']);
  const s = rows[0];
  assert.equal(s.need, 40); assert.equal(s.reco, 0); assert.equal(s.reco_capped, true); assert.equal(s.before_alloc, 0);
  assert.equal(s.reason, 'amazon_reco_zero');
});
await ta('🚨 関所に当たった日は提案を出さず、前日以前の提案も使えない状態にし、理由と 0 の理由を残す', async () => {
  await recordShadowDraft(db, engineResult([item({ amazon_sku: 'gate001', adjusted_qty: 30 })]), { log: quiet, now: new Date('2026-10-20T21:00:00Z') });
  assert.ok((await q(`select count(*)::int as n from ai.decisions where domain = $1 and status = 'new' and decision_kind = 'proposal'`, [DOMAIN]))[0].n > 0, '前提: 前日の提案がある');
  const gate = { reasons: [{ code: 'inbound_working_not_fresh', detail: '準備中の数量 = failed' }] };
  const r = await recordShadowDraft(db, engineResult([
    item({ amazon_sku: 'gate001', adjusted_qty: 30 }),
    item({ amazon_sku: 'gate002', adjusted_qty: 0, needs_replenishment: false }),
  ]), { log: quiet, now: new Date('2026-10-21T21:00:00Z'), gate });
  assert.equal(r.gated, true);
  assert.equal(r.proposals, 0);
  assert.equal(r.status, 'partial');
  const open = await q(`select decision_kind, dedupe_key, inputs_ref from ai.decisions where domain = $1 and status = 'new' and inputs_ref->>'generator' = $2`, [DOMAIN, GENERATOR]);
  assert.equal(open.length, 1, '開いているのは「今日は決められない」の 1 行だけ (この仕組みの行。人が触った行・他の出どころは触らない)');
  assert.equal(open[0].dedupe_key, `${DOMAIN}:__run__`);
  assert.equal(open[0].inputs_ref.gated, true);
  assert.deepEqual(open[0].inputs_ref.gate, gate);
  assert.deepEqual(open[0].inputs_ref.zero_reasons.map((z) => z.sku), ['gate002']);
  const last = (await q(`select status, summary from ops.job_runs order by job_run_id desc limit 1`))[0];
  assert.equal(last.status, 'partial');
  assert.match(last.summary, /今日は決められない: inbound_working_not_fresh/);
});
await ta('ふつうの日の要約行にも 0 の理由と関所の結果 (空) が入る', async () => {
  await recordShadowDraft(db, engineResult([
    item({ amazon_sku: 'z001', adjusted_qty: 20 }),
    item({ amazon_sku: 'z002', adjusted_qty: 0, needs_replenishment: false, days_of_supply: 40 }),
  ]), { log: quiet, now: new Date('2026-10-22T21:00:00Z'), gate: { reasons: [] } });
  const run = (await q(`select inputs_ref from ai.decisions where dedupe_key = $1 and status = 'new'`, [`${DOMAIN}:__run__`]))[0];
  assert.deepEqual(run.inputs_ref.gate, { reasons: [] });
  assert.deepEqual(run.inputs_ref.zero_reasons.map((z) => [z.sku, z.reason, z.dos]), [['z002', 'above_reorder_point', 40]]);
});

console.log('\n準備中の取得結果と、前日以前の提案の無効化 (Codex PR #1438 R1)');

const T0 = Date.parse('2026-09-24T00:40:00Z');
t('🚨 準備中: 取り直しは 120 件なのにキャッシュが空・取得時刻なし (miniPC 再起動) は fresh にしない → 関所で止まる', () => {
  const j = judgeInboundFetch({ refresh: { ok: true, count: 120 }, cache: { ok: true, data: {}, count: 0, cachedAt: null, ageMs: null }, nowMs: T0 });
  assert.equal(j.source, 'inconsistent');
  assert.match(j.error, /取得時刻が無い/);
  const g = inputGate({ inboundState: { source: j.source, error: j.error, count: j.count }, inputFreshness: FRESH, dq: DQ_OK, now: NOW });
  assert.deepEqual(g.reasons.map((r) => r.code), ['inbound_working_not_fresh']);
});
t('準備中: 件数が合わない / キャッシュが古い / 値が数でない / 取り直しが count を返さない / キャッシュが取れない は fresh にしない', () => {
  const ok = { ok: true, count: 2 };
  assert.equal(judgeInboundFetch({ refresh: ok, cache: { data: { a: 1 }, cachedAt: T0 }, nowMs: T0 }).source, 'inconsistent');
  assert.equal(judgeInboundFetch({ refresh: ok, cache: { data: { a: 1, b: 2 }, cachedAt: T0 - 10 * 60e3 }, nowMs: T0 }).source, 'inconsistent');
  assert.equal(judgeInboundFetch({ refresh: ok, cache: { data: { a: 1, b: 'x' }, cachedAt: T0 }, nowMs: T0 }).source, 'inconsistent');
  assert.equal(judgeInboundFetch({ refresh: { ok: true }, cache: null, nowMs: T0 }).source, 'empty');
  const e = judgeInboundFetch({ refresh: ok, cache: null, cacheError: 'timeout', nowMs: T0 });
  assert.equal(e.source, 'empty'); assert.equal(e.error, 'timeout');
});
t('準備中: そろっていれば fresh。本当に準備中ゼロの日 (0 件・空・取得時刻あり) も fresh', () => {
  const j = judgeInboundFetch({ refresh: { ok: true, count: 2 }, cache: { data: { a: 1, b: 0 }, cachedAt: T0 - 30e3 }, nowMs: T0 });
  assert.equal(j.source, 'fresh'); assert.equal(j.count, 2); assert.deepEqual(j.data, { a: 1, b: 0 });
  assert.equal(judgeInboundFetch({ refresh: { ok: true, count: 0 }, cache: { data: {}, cachedAt: T0 - 30e3 }, nowMs: T0 }).source, 'fresh');
});
await ta('🚨 前日以前の提案の無効化: 今の接続で駄目なら別の接続で。両方駄目なら false (成功扱いにしない)', async () => {
  await recordShadowDraft(db, engineResult([item({ amazon_sku: 'sup001', adjusted_qty: 9 })]), { log: quiet, now: new Date('2026-10-25T21:00:00Z') });
  const dead = { query: async () => { throw new Error('connection terminated'); } };
  assert.equal(await supersedeOpenRowsSafely(dead, { openFresh: null }), false);
  assert.equal(await supersedeOpenRowsSafely(dead, { openFresh: async () => { throw new Error('cannot connect'); } }), false);
  assert.equal(await supersedeOpenRowsSafely(dead, { openFresh: async () => ({ db, close: async () => {} }) }), true);
  const open = (await q(`select count(*)::int as n from ai.decisions where domain = $1 and status = 'new' and inputs_ref->>'generator' = $2`, [DOMAIN, GENERATOR]))[0].n;
  assert.equal(open, 0);
});
await ta('🚨 関所の日に要約の記録が失敗しても、前日以前の提案は無効のまま (巻き戻らない)', async () => {
  await recordShadowDraft(db, engineResult([item({ amazon_sku: 'sup002', adjusted_qty: 9 })]), { log: quiet, now: new Date('2026-10-26T21:00:00Z') });
  await db.exec("alter table ai.decisions add constraint zz_gate_reject check (summary not like '%は決められない%') not valid");
  try {
    await assert.rejects(() => recordShadowDraft(db, engineResult([item({ amazon_sku: 'sup002', adjusted_qty: 9 })]), {
      log: quiet, now: new Date('2026-10-27T21:00:00Z'), gate: { reasons: [{ code: 'warehouse_stale', detail: 'x' }] },
    }));
  } finally {
    await db.exec('alter table ai.decisions drop constraint zz_gate_reject');
  }
  const open = (await q(`select count(*)::int as n from ai.decisions where domain = $1 and status = 'new' and inputs_ref->>'generator' = $2`, [DOMAIN, GENERATOR]))[0].n;
  assert.equal(open, 0, '前日の提案は new に戻らない');
  const last = (await q(`select status from ops.job_runs order by job_run_id desc limit 1`))[0];
  assert.equal(last.status, 'fail');
});
await ta('🚨 ふつうの日に記録が途中で失敗しても、前日以前の提案は無効にしてから失敗を残す', async () => {
  await recordShadowDraft(db, engineResult([item({ amazon_sku: 'sup003', adjusted_qty: 9 })]), { log: quiet, now: new Date('2026-10-28T21:00:00Z') });
  await db.exec("alter table ai.decisions add constraint zz_main_reject check (summary not like '%ZZ-FAIL2%')");
  try {
    await assert.rejects(() => recordShadowDraft(db, engineResult([
      item({ amazon_sku: 'sup003', adjusted_qty: 9 }), item({ amazon_sku: 'sup004', adjusted_qty: 5, product_name: 'ZZ-FAIL2' }),
    ]), { log: quiet, now: new Date('2026-10-29T21:00:00Z') }));
  } finally {
    await db.exec('alter table ai.decisions drop constraint zz_main_reject');
  }
  const open = (await q(`select count(*)::int as n from ai.decisions where domain = $1 and status = 'new' and inputs_ref->>'generator' = $2`, [DOMAIN, GENERATOR]))[0].n;
  assert.equal(open, 0);
});

await ta('🚨 最初の接続に失敗した日も、別の接続で前日以前の提案を無効にしてから失敗を記録する (Codex PR #1438 R2)', async () => {
  await recordShadowDraft(db, engineResult([item({ amazon_sku: 'con001', adjusted_qty: 9 })]), { log: quiet, now: new Date('2026-10-30T21:00:00Z') });
  const runsBefore = (await q(`select count(*)::int as n from ops.job_runs`))[0].n;
  const r = await recordConnectFailure({ error: new Error('ECONNRESET'), openFresh: async () => ({ db, close: async () => {} }), log: quiet });
  assert.deepEqual(r, { superseded: true, recorded: true });
  const open = (await q(`select count(*)::int as n from ai.decisions where domain = $1 and status = 'new' and inputs_ref->>'generator' = $2`, [DOMAIN, GENERATOR]))[0].n;
  assert.equal(open, 0);
  const last = (await q(`select status, summary from ops.job_runs order by job_run_id desc limit 1`))[0];
  assert.equal(last.status, 'fail'); assert.match(last.summary, /接続できない: ECONNRESET/);
  assert.equal((await q(`select count(*)::int as n from ops.job_runs`))[0].n, runsBefore + 1);
  // 別の接続も開けない日は「無効にできなかった」と分かる (記録もできないので false/false)
  const r2 = await recordConnectFailure({ error: new Error('down'), openFresh: async () => { throw new Error('down'); }, log: quiet });
  assert.deepEqual(r2, { superseded: false, recorded: false });
});

await pg.close();
console.log(`\n${passed} 件 PASS`);
