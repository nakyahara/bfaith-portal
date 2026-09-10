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
  recordShadowDraft, pickDraftRows, blockedReason, calmReason, cautionsOf, rationaleOf, dedupeKeyOf,
  newShadowRunId, resolveListings, RULE_VERSION, DOMAIN, JOB_ID, EXPIRES_HOURS, GENERATOR, AMAZON_SHOP_CODE,
} from '../apps/fba-replenishment/shadow-draft.mjs';
import { mergeRestockWithPlanning } from '../apps/fba-replenishment/calculation-engine.js';

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
  const stillOpen = (await q(`select count(*)::int as n from ai.decisions where domain = $1 and status = 'new'`, [DOMAIN]))[0].n;
  assert.equal(stillOpen, 4, '前日ぶんを差し替え済みにしたのも巻き戻る');
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

await ta('🚨 計算そのものが失敗した日は、前日の提案を消さない', async () => {
  // スナップショットが無い等でエンジンが { items: [], errors: [...] } を返す日。
  // 「今日は何も要らない」と混ぜると、翌朝いきなり提案が消える (Codex 2026-09-10)
  const openBefore = (await q(`select count(*)::int as n from ai.decisions where domain = $1 and status = 'new'`, [DOMAIN]))[0].n;
  assert.ok(openBefore > 0, '前提: 開いている提案がある');
  const r = await recordShadowDraft(db, {
    items: [], errors: ['スナップショットがありません。SP-APIレポートを取得してください。'],
  }, { log: quiet, now: new Date('2026-09-14T21:00:00Z') });
  assert.equal(r.ok, false);
  assert.equal(r.engineFailed, true);
  assert.equal(r.status, 'fail');
  const openAfter = (await q(`select count(*)::int as n from ai.decisions where domain = $1 and status = 'new'`, [DOMAIN]))[0].n;
  assert.equal(openAfter, openBefore, '🚨 前日の提案はそのまま残る');
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

await pg.close();
console.log(`\n${passed} 件 PASS`);
