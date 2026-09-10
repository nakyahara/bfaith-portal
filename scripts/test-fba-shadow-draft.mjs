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
  recordShadowDraft, pickDraftRows, blockedReason, rationaleOf, dedupeKeyOf,
  newShadowRunId, resolveListings, RULE_VERSION, DOMAIN, JOB_ID, EXPIRES_HOURS,
} from '../apps/fba-replenishment/shadow-draft.mjs';

let passed = 0;
function t(name, fn) { try { fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; } }
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; } }
const quiet = () => {};

/** 計算エンジンが出す 1 行の形 (要るところだけ) */
const item = (o = {}) => ({
  amazon_sku: 'abc001', product_name: 'テスト商品', ne_code: 'abc001', asin: 'B000TEST01',
  is_set: false, invalid_mapping: false, stock_state: 'normal',
  fba_available: 3, effective_fba_stock: 3, fba_inbound_working_effective: 0,
  units_sold_7d: 7, units_sold_30d: 30, daily_sales: 1, days_of_supply: 3,
  reorder_point: 14, reorder_point_days: 14, target_days: 60, target_stock: 60,
  warehouse_available: 100, recommended_qty: 57, rounded_qty: 60, adjusted_qty: 60,
  amazon_recommended_qty: null, amazon_reco_capped: false, expiry_limited: false,
  location_adjusted: false, recent_arrival_adjusted: false, urgency_score: 88.5,
  alerts: [], exception_type: null, ...o,
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
    item({ amazon_sku: 'b', adjusted_qty: 0 }),                        // 送らなくてよい
    item({ amazon_sku: 'c', invalid_mapping: true, adjusted_qty: 10 }), // 提案不能
    item({ amazon_sku: 'd', warehouse_available: null }),               // 自社在庫が不明
  ]);
  assert.deepEqual(proposals.map((p) => p.amazon_sku), ['a']);
  assert.deepEqual(blocked.map((b) => [b.item.amazon_sku, b.reason]), [['c', 'invalid_mapping'], ['d', 'warehouse_unknown']]);
  assert.equal(calm, 1);
});

t('🚨「0 個でよい」と「計算できなかった」を混ぜない', () => {
  assert.equal(blockedReason(item({ adjusted_qty: 0 })), null, '計算できて 0 個は「不能」ではない');
  assert.equal(blockedReason(item({ warehouse_available: null })), 'warehouse_unknown');
  assert.equal(blockedReason(item({ warehouse_available: 0 })), null, '自社在庫 0 は「不明」ではない');
  assert.equal(blockedReason(item({ units_sold_30d: null })), 'sales_unknown');
  assert.equal(blockedReason(item({ units_sold_30d: 0 })), null, '売れていない は「取れていない」ではない');
  assert.equal(blockedReason(item({ ne_code: null })), 'no_ne_code');
  assert.equal(blockedReason(item({ invalid_mapping: true })), 'invalid_mapping');
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
    item({ amazon_sku: 'abc002', adjusted_qty: 0 }),                    // 送らなくてよい → 記録しない
    item({ amazon_sku: 'abc003', invalid_mapping: true }),              // 提案不能 (Company DB にも無い)
  ]), { log: quiet, now: new Date('2026-09-10T21:00:00Z') });

  assert.equal(day1.proposals, 1);
  assert.equal(day1.blocked, 1);
  assert.equal(day1.calm, 1);
  assert.equal(day1.status, 'partial', '提案不能があるので partial');

  const rows = await q(`select decision_kind, subject_type, subject_id, summary, severity, status, dedupe_key,
                               rule_version, generated_by, autonomy_level, proposed_action, inputs_ref, expires_at
                          from ai.decisions where domain = $1 order by decision_kind, dedupe_key`, [DOMAIN]);
  assert.equal(rows.length, 2, '送らなくてよい行は記録しない');

  const finding = rows.find((r) => r.decision_kind === 'finding');
  assert.equal(finding.severity, 'warn');
  assert.equal(finding.subject_id, null, 'Company DB に無い出品は結び付けない (が、記録は残す)');
  assert.match(finding.summary, /提案を出せない \(invalid_mapping\)/);
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
  assert.match(runs[0].summary, /提案不能 1 件/);
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
  assert.equal(day2.status, 'ok', '提案不能が無くなった');

  const open = await q(`select dedupe_key, (inputs_ref->>'adjusted_qty')::int as qty, (inputs_ref->>'prev_qty')::int as prev
                          from ai.decisions where domain = $1 and status = 'new' order by dedupe_key`, [DOMAIN]);
  assert.equal(open.length, 2, '開いているのは今日の 2 件だけ');
  assert.equal(open[0].qty, 24);
  assert.equal(open[0].prev, 60, '🚨 前回いくつだったかが残る (昨日と何が変わったかを追える)');
  assert.equal(open[1].prev, null, '前回は提案に上がっていなかった');

  const superseded = await q(`select count(*)::int as n from ai.decisions where domain = $1 and status = 'superseded'`, [DOMAIN]);
  assert.equal(superseded[0].n, 2, '🚨 前日の 2 件は差し替え済み (古い数量が残らない)');
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
  assert.equal((await q(`select count(*)::int as n from ops.job_runs`))[0].n, runsBefore, '実行の記録も残さない');
  const stillOpen = (await q(`select count(*)::int as n from ai.decisions where domain = $1 and status = 'new'`, [DOMAIN]))[0].n;
  assert.equal(stillOpen, 2, '前日ぶんを差し替え済みにしたのも巻き戻る');
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

await pg.close();
console.log(`\n${passed} 件 PASS`);
