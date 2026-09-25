/**
 * test-company-db-watch.mjs — Company DB の見張り (apps/company-db/watch + config/watch-checks.mjs + 0023 + 証跡)。設計 = 09
 *
 * PGlite で migration を当て、完了の印 (stock_capture_days / stock_diff_days / ingest_runs / 売上日次の state) と証跡 (JSON) を作って、
 * 判定 4 値・前提で blocked・案件の 新 / 継続 / 回復 / 監視期間外・保存・期限 を確かめる。
 *
 * 使い方: node scripts/test-company-db-watch.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import crypto from 'node:crypto';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';
import * as REAL_CONFIG from '../config/watch-checks.mjs';
import { plannedKeys, addDays, partialAllowed, evalW6, evalW8, w8Days } from '../apps/company-db/watch/checks.mjs';
import { runWatch, reconcileIssues, pickItems, MAX_GENERATION_RETRIES } from '../apps/company-db/watch/engine.mjs';
import { writeEvidence, readEvidence, purgeOldEvidence, EVIDENCE_KEEP_DAYS } from '../apps/company-db/push/evidence.mjs';
import { roleStatements, createRoles, urlFor, verifyRole, WATCH_TABLES } from './company-db/create-watch-roles.mjs';
import { parseArgs } from '../apps/company-db/watch/run.mjs';

const root = path.join(path.dirname(fileURLToPath(import.meta.url)), '..');
// 試験の「今日」(9/23) の昨日 9/22 は本番の祝日の一覧に入っている = W4 が判定しない → 試験では空にする (祝日の扱いは W4 の試験で本番の一覧を使って確かめる)
const CONFIG = { ...REAL_CONFIG, NON_BUSINESS_DAYS: [] };
let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e).split('\n').filter((l) => !/^\s+at /.test(l)).slice(0, 30).join('\n      ') + (e && e.detail ? '\n      detail: ' + e.detail : '')); } };
const quiet = () => {};

const pg = new PGlite();
const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
const one = async (sql, p = []) => (await pg.query(sql, p)).rows[0];
const all = async (sql, p = []) => (await pg.query(sql, p)).rows;

await pg.query(`insert into core.products (company_id, name) values (1, '見本A'), (1, '見本B'), (1, '見本C')`);
await pg.query(`insert into core.skus (company_id, product_id, sku_kind, code, name) select company_id, product_id, 'single', case name when '見本A' then 'AAA-1' when '見本B' then 'bbb-2' else 'CCC-3' end, name from core.products`);
const skuOf = async (code) => (await one(`select sku_id from core.skus where code = $1`, [code])).sku_id;
const skuA = await skuOf('AAA-1'), skuB = await skuOf('bbb-2');

const ASOF = '2026-09-23';
const NOW = new Date('2026-09-23T00:30:00Z');   // 09:30 JST
const SYNC = 'ds_20260922T220000';                // daily-sync の実行 ID (証跡と見張りを結びつける)
const D = (n) => addDays(ASOF, n);
const TS = (d, h = 1) => `${d}T${String(h).padStart(2, '0')}:00:00Z`;
let seq = 0;

/** 取得記録 1 日 (missing 以外は ingest_runs も作る) */
async function capture(day, source, scope, status, { builtAt = TS(day, 2) } = {}) {
  if (status === 'missing') { await pg.query(`insert into snapshots.stock_capture_days (snapshot_date, source, scope_key, company_id, status, ingest_run_id, built_at) values ($1::date, $2, $3, 1, 'missing', null, $4::timestamptz)`, [day, source, scope, builtAt]); return null; }
  const runId = `t_${source}_${day}_${++seq}`;
  await pg.query(`insert into ops.ingest_runs (ingest_run_id, source_system, entity, scope_key, host, started_at, finished_at, status, complete, rows_seen, source_tz, checksum) values ($1, $2, $3, $4, 'test', $5::timestamptz, $5::timestamptz, 'success', true, 1, 'UTC', $5)`,
    [runId, source === 'logizard' ? 'logizard' : source === 'ne' ? 'ne' : 'amazon', source === 'logizard' ? 'inventory' : 'stock_daily', scope, builtAt]);
  await pg.query(`insert into snapshots.stock_capture_days (snapshot_date, source, scope_key, company_id, status, ingest_run_id, completed_at, built_at) values ($1::date, $2, $3, 1, $4, $5, $6::timestamptz, $7::timestamptz)`,
    [day, source, scope, status, runId, status === 'complete' ? builtAt : null, builtAt]);
  return runId;
}
const delCapture = (day, source) => pg.query(`delete from snapshots.stock_capture_days where snapshot_date = $1::date and source = $2`, [day, source]);
async function diffDay(to, status, { from = addDays(to, -1), skip = null, events = 0 } = {}) {
  await pg.query(`insert into snapshots.stock_diff_days (to_date, source, scope_key, calc_version, company_id, from_date, status, skip_reason, events, unresolved_changed) values ($1::date, 'logizard', 'main', 'lzdiff:v1', 1, $2::date, $3, $4, $5, 0)`, [to, status === 'done' ? from : null, status, skip, events]);
}
async function order(mall, scope, day, no) {
  await pg.query(`insert into core.orders (company_id, mall, scope_key, mall_order_no, source_system, ordered_at, order_date_jst, status, received_batch_seq, source_updated_at, transform_version, content_hash)
    values (1, $1, $2, $3, 'mall_api', $4::timestamptz, $5::date, 'new', 1, $4::timestamptz, 'v1', 'h')`, [mall, scope, no, TS(day), day]);
}
async function salesState(mall, scope, { watermark = TS(ASOF, 0), sessionId = null } = {}) {
  await pg.query(`insert into mart.sales_daily_state (company_id, mall, scope_key, watermark, session_id, session_started_at) values (1, $1, $2, $3::timestamptz, $4, $5::timestamptz)
    on conflict (company_id, mall, scope_key) do update set watermark = excluded.watermark, session_id = excluded.session_id, session_started_at = excluded.session_started_at`, [mall, scope, watermark, sessionId, sessionId ? watermark : null]);
}
async function published(mall, scope, day) {
  const ex = await one(`select run_id from mart.sales_daily_published where company_id = 1 and mall = $1 and scope_key = $2 and date_jst = $3::date`, [mall, scope, day]);
  if (ex) return ex.run_id;
  const runId = `sd_${mall}_${++seq}`;
  await pg.query(`insert into mart.sales_daily_runs (run_id, company_id, mall, scope_key, session_id, started_at, finished_at, n_dates, n_rows, n_orders) values ($1, 1, $2, $3, 's', now(), now(), 1, 0, 0)`, [runId, mall, scope]);
  await pg.query(`insert into mart.sales_daily_published (company_id, mall, scope_key, date_jst, run_id) values (1, $1, $2, $3::date, $4)`, [mall, scope, day, runId]);
  return runId;
}
async function ordersBulk(mall, scope, day, n, cancelled = 0, from = 1) {
  await pg.query(`insert into core.orders (company_id, mall, scope_key, mall_order_no, source_system, ordered_at, order_date_jst, status, is_cancelled, received_batch_seq, source_updated_at, transform_version, content_hash)
    select 1, $1, $2, 'w8-' || $1 || '-' || $3 || '-' || g, 'mall_api', $3::date::timestamptz, $3::date, case when g < $6 + $5 then 'cancelled' else 'new' end, g < $6 + $5, 1, $3::date::timestamptz, 'v1', 'h' from generate_series($6::int, $6::int + $4::int - 1) g`, [mall, scope, day, n, cancelled, from]);
}
async function salesDay(runId, mall, scope, day, salesJpy, lines, unknown) {
  await pg.query(`insert into mart.sales_daily (run_id, company_id, date_jst, mall, scope_key, orders, orders_cancelled, lines, units_ordered, units_cancelled, items_amount_jpy, cancelled_items_amount_jpy, sales_jpy, customer_paid_jpy, lines_amount_unknown)
    values ($1, 1, $2::date, $3, $4, $5, 0, $5, 0, 0, $6, 0, $6, $6, $7)`, [runId, day, mall, scope, lines, salesJpy, unknown]);
}
const r4 = (x) => Math.round(x * 10000) / 10000;
/** W4 の見本: 取得記録が無ければ作る (差の印は取得記録を FK で指す) / 区間 (to の前日 → to) の在庫の差のイベント */
async function ensureCapture(day) { if (!(await one(`select 1 as x from snapshots.stock_capture_days where snapshot_date = $1::date and source = 'logizard'`, [day]))) await capture(day, 'logizard', 'main', 'complete'); }
async function w4Events(to, deltas) {
  const ref = `main:${addDays(to, -1)}..${to}`;
  for (let i = 0; i < deltas.length; i++) {
    await pg.query(`insert into events.inventory_events (company_id, occurred_at, actor_type, source_system, source_ref, idempotency_key, payload, sku_id, qty_delta, qty_after, confidence)
      values (1, $1::timestamptz, 'system', 'logizard_diff', $2, $3, $4::jsonb, $5, $6, 100, 'inferred')`,
      [`${to}T09:00:00Z`, ref, `lzdiff:v1:test:${ref}:${i}:${++seq}`, JSON.stringify({ calc: 'lzdiff:v1', source: 'logizard', scope: 'main', from: addDays(to, -1), to }), i % 2 ? skuA : skuB, deltas[i]]);
  }
}
/** 区間のイベントを入れ直す (README「締めをやり直す」と同じ = この取引の中だけ追記専用の trigger を外して消す)。印の件数もそろえる */
async function w4Replace(to, deltas) {
  await pg.exec(`begin; alter table events.inventory_events disable trigger trg_append_only_row; delete from events.inventory_events where source_system = 'logizard_diff' and source_ref = 'main:${addDays(to, -1)}..${to}'; alter table events.inventory_events enable trigger trg_append_only_row; commit;`);
  await w4Events(to, deltas);
  await pg.query(`update snapshots.stock_diff_days set events = $2 where to_date = $1::date and source = 'logizard'`, [to, deltas.length]);
}
const orderRun = async (runId, { status = 'success', complete = true } = {}) => pg.query(`insert into ops.ingest_runs (ingest_run_id, source_system, entity, scope_key, host, started_at, finished_at, status, complete, rows_seen, source_tz) values ($1, 'rakuten', 'orders', 'main', 'test', now(), now(), $2, $3, 1, 'UTC')`, [runId, status, complete]);
const ev = (mall, scope, extra = {}) => ({ name: `orders-${mall}`, kind: 'orders', mall, scope, mode: 'incremental', sync_run_id: SYNC, ok: true, push_ok: true, locked: false, run_id: null, batch_seq: 1, started_at: '2026-09-22T22:05:00Z', scanned: 100, in_scope: 100, unchanged: 100, changed: 0, applied: 0, same: 0, stale: 0, failed: 0, transform_errors: 0, sales: { ok: true, complete: true, dates: 0, skipped: null, error: null }, written_at: '2026-09-22T22:06:00Z', ...extra });
const shipEvidence = (extra = {}) => ({ name: 'shipments', kind: 'shipments', scope: 'main', mode: 'incremental', sync_run_id: SYNC, ok: true, push_ok: true, locked: false, run_id: null, batch_seq: null, started_at: '2026-09-22T22:02:00Z', scanned: 100, in_scope: 100, unchanged: 100, changed: 0, applied: 0, same: 0, stale: 0, failed: 0, transform_errors: 0, written_at: '2026-09-22T22:03:00Z', ...extra });
// W13 (マスタの照合): 照合の実行口が書く証跡と全件 JSON (差 0) を一時の DATA_DIR に置く
const W13_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'watch-w13-'));
process.env.DATA_DIR = W13_DIR;
function masterCompareEvidence({ items = [], verdict = items.length ? 'breach' : 'pass', blockedReason = null, compared = {}, exclusions = {}, runId = 'mc_20260922T221000000Z_aaaaaa', extra = {} } = {}) {
  const res = { format: 'mc-v1', as_of: ASOF, compare_run_id: runId, verdict, blocked_reason: blockedReason, load: { ingest_run_id: 'load_n1', started_at: '2026-09-22T17:00:00Z' },
    counts: { items: items.length, by_type: {}, compared: { value: 3 } }, items, compared, exclusions, finished_at: '2026-09-22T22:11:00Z' };
  const rel = `cdb-master-compare/${ASOF}/${runId}.json`;
  fs.mkdirSync(path.join(W13_DIR, 'cdb-master-compare', ASOF), { recursive: true });
  const buf = Buffer.from(JSON.stringify(res), 'utf8');
  fs.writeFileSync(path.join(W13_DIR, rel), buf);
  const sha = crypto.createHash('sha256').update(buf).digest('hex');
  return { name: 'master-compare', state: 'complete', compare_run_id: runId, as_of: ASOF, json_path: rel, sha256: sha, verdict, blocked_reason: blockedReason, counts: res.counts, load: { ingest_run_id: 'load_n1' }, sync_run_id: SYNC, ...extra };
}
const goodEvidence = () => ({ ...Object.fromEntries(CONFIG.ORDER_MALLS.map((m) => [`orders-${m.mall}`, ev(m.mall, m.scope)])), shipments: shipEvidence(), 'master-compare': masterCompareEvidence() });
/** W13 は照合の証跡をファイルから読む (Codex #1456 R1 High-2) → 渡した証跡の master-compare をその日の証跡ファイルに写す (無ければ消す) */
function mirrorW13(asOf, evidence) {
  const file = path.join(W13_DIR, 'company-db-evidence', asOf, 'master-compare.json');
  if (evidence && evidence['master-compare']) { fs.mkdirSync(path.dirname(file), { recursive: true }); fs.writeFileSync(file, JSON.stringify(evidence['master-compare'])); }
  else fs.rmSync(file, { force: true });
}
const run = (opts = {}) => { mirrorW13(opts.asOf || ASOF, opts.evidence ?? goodEvidence()); return runWatch({ db, writer: opts.dryRun ? null : (opts.writer || db), config: opts.config || CONFIG, asOf: opts.asOf || ASOF, evidence: opts.evidence ?? goodEvidence(), evidenceHistory: opts.evidenceHistory || {}, now: opts.now || NOW, host: 'test', log: opts.log || quiet, syncRunId: 'syncRunId' in opts ? opts.syncRunId : SYNC, hooks: opts.hooks }); };
const verdictOf = (r, id, scope) => { const x = r.results.find((y) => y.checkId === id && y.scopeKey === scope); return x ? x.verdict : undefined; };
const resultOf = (r, id, scope) => r.results.find((y) => y.checkId === id && y.scopeKey === scope);

// ── 「そろった朝」の見本: 在庫 4 scope が complete (fba_us は partial = 例外)・差が done・注文は変更ゼロ・売上日次は閉じている
async function seedGoodMorning() {
  for (const s of CONFIG.STOCK_SCOPES) for (let n = -8; n <= 0; n++) await capture(D(s.dayOffset + n), s.source, s.scope, s.source === 'fba_us' ? 'partial' : 'complete');
  for (let n = -7; n <= -1; n++) await diffDay(D(n), 'done', { events: 10 });
  // W4 の平常: 昨日と同じ曜日の過去 8 週 = 減った数 3,000 (10 SKU × 300。本番は 1 日 約 2,900)。過去は ±100 のゆらぎ (MAD = 100)
  await w4Events(D(-1), Array.from({ length: 10 }, () => -300));
  for (let k = 1; k <= CONFIG.W4_BASELINE_WEEKS; k++) {
    const b = D(-1 - 7 * k);
    await ensureCapture(addDays(b, -1)); await ensureCapture(b);
    if (!(await one(`select 1 as x from snapshots.stock_diff_days where to_date = $1::date`, [b]))) await diffDay(b, 'done', { events: 10 });
    await w4Events(b, Array.from({ length: 10 }, (_, i) => -300 + (i === 0 ? (k % 3) - 1 : 0) * 100));
  }
  // W12 の平常: 毎晩の締めの記録 = 1 日 1 MB ずつ増える (D(-7)〜D(0))
  for (let n = -7; n <= 0; n++) await pg.query(`insert into ops.job_runs (job_id, host, started_at, finished_at, status, summary) values ('company-db-inventory-hourly', 'test', $1::timestamptz, now(), 'ok', $2)`, [`${D(n)}T00:35:00+09:00`, JSON.stringify({ step: 'maintain', purged_observations: 0, keep_days: 30, db_bytes: (40 + n) * 1048576, db_mb: 40 + n })]);
  for (const m of CONFIG.ORDER_MALLS) await salesState(m.mall, m.scope);
  // W8 の平常: 大 (rakuten / amazon / aupay) = 40 件・取消 2・40 万円・金額不明 4/40、小 (linegift / qoo10) = 10 件・10 万円・不明 1/10。平常の日は ±1 件のゆらぎ (MAD = 1)
  const { day, baseline } = w8Days(CONFIG, ASOF);
  for (const m of CONFIG.ORDER_MALLS) {
    const large = ['rakuten', 'amazon', 'aupay'].includes(m.mall);
    for (const [d, j] of [[day, 0], ...baseline.map((b, i) => [b, (i % 3) - 1])]) {
      const n = (large ? 40 : 10) + j;
      await ordersBulk(m.mall, m.scope, d, n, large ? 2 : 0);
      await salesDay(await published(m.mall, m.scope, d), m.mall, m.scope, d, n * 10000, n, large ? 4 : 1);
    }
  }
}

console.log('定義と評価キー');
await t('評価キーは scope に展開した後の数 (4 + 4 + 1 + 5 + 5 + 1 + 1 + 5 + W10 11 + W11 5 + W4 1 + W12 1 + W13 1 = 45)。定義の版・順番・depends', () => {
  const keys = plannedKeys(CONFIG);
  assert.equal(keys.length, 45);
  assert.deepEqual(CONFIG.CHECKS.map((c) => c.id), ['W1', 'W2', 'W3', 'W7', 'W9', 'W5', 'W6', 'W8', 'W10', 'W11', 'W4', 'W12', 'W13']);
  assert.deepEqual([CONFIG.checkById('W13').depends, CONFIG.checkById('W13').issuePerItem, CONFIG.checkById('W13').severity, keys.filter((k) => k.checkId === 'W13').map((k) => k.scopeKey)], [[], true, 'info', ['load']]);
  assert.deepEqual([CONFIG.checkById('W8').depends, keys.filter((k) => k.checkId === 'W8').length], [['W7', 'W9'], 5]);
  assert.deepEqual([CONFIG.checkById('W3').depends, CONFIG.checkById('W9').depends, CONFIG.checkById('W2').issuePerItem, CONFIG.checkById('W5').depends, CONFIG.checkById('W6').depends, CONFIG.checkById('W6').issuePerItem], [['W1'], ['W7'], true, ['W3'], ['W1:*', 'W7:*', 'W9:*'], true]);
  assert.throws(() => plannedKeys({ ...CONFIG, CHECKS: [CONFIG.checkById('W3'), CONFIG.checkById('W1')] }), /定義の順番/);   // 前提は先に評価される
  assert.deepEqual(keys.filter((k) => k.checkId === 'W5' || k.checkId === 'W6').map((k) => k.scopeKey), ['logizard/main', 'all/jp']);
  assert.equal(CONFIG.CHECKS_VERSION, 'v11');
  for (const s of CONFIG.STOCK_SCOPES) if (s.since) assert.match(s.since, /^\d{4}-\d{2}-\d{2}$/, `${s.source} の since は YYYY-MM-DD`);
  for (const m of CONFIG.ORDER_MALLS) { assert.match(m.ordersSince, /^\d{4}-\d{2}-\d{2}$/, `${m.mall} の ordersSince`); assert.match(m.reconciledThrough, /^\d{4}-\d{2}-\d{2}$/, `${m.mall} の reconciledThrough`); assert.ok(m.ordersSince <= m.reconciledThrough, `${m.mall} の範囲`); }
});
await t('partial の例外は期限つき (until を過ぎたら効かない)', () => {
  const s = CONFIG.STOCK_SCOPES.find((x) => x.source === 'fba_us');
  assert.deepEqual([partialAllowed(s, '2026-12-31'), partialAllowed(s, '2027-01-01'), partialAllowed(CONFIG.STOCK_SCOPES[0], ASOF)], [true, false, false]);
});

console.log('そろった朝');
await seedGoodMorning();
await t('🚨 全部 pass・案件なし・run が保存される (予定 45 / 完了 45)。fba_us の partial は例外として pass (理由が観測値に残る)', async () => {
  const r = await run();
  assert.deepEqual([r.counts.pass, r.counts.breach, r.counts.blocked, r.counts.execution_error, r.counts.completed, r.counts.planned, r.exitCode], [45, 0, 0, 0, 45, 45, 0], JSON.stringify(r.results.filter((x) => x.verdict !== 'pass').map((x) => [x.checkId, x.scopeKey, x.verdict, x.reason])));
  assert.match(r.lastLine, /^✅ Company DB 見張り 2026-09-23: 異常 0 \(新 0 \/ 継続 0\) \/ 判定保留 0 \/ 回復 0 \/ 評価 45\/45$/);
  const us = resultOf(r, 'W1', 'fba_us/us');
  assert.deepEqual([us.observed.status, us.observed.partial_allowed, /例外/.test(us.reason)], ['partial', true, true]);
  assert.deepEqual([resultOf(r, 'W7', 'rakuten/main').observed.contract, resultOf(r, 'W3', 'logizard/main').observed.status], ['zero_change', 'done']);
  const runRow = await one(`select planned_keys, completed_keys, summary, last_line, evidence from ops.watch_runs where watch_run_id = $1`, [r.runId]);
  assert.deepEqual([runRow.planned_keys, runRow.completed_keys, runRow.summary.pass, Object.keys(runRow.evidence).length], [45, 45, 45, 7]);
  assert.equal((await one(`select count(*)::int as n from ops.watch_results where watch_run_id = $1`, [r.runId])).n, 45);
  assert.equal((await one(`select count(*)::int as n from ops.watch_issues`)).n, 0);
});
await t('dry-run (writer なし) は何も書かない', async () => {
  const before = (await one(`select count(*)::int as n from ops.watch_runs`)).n;
  const r = await run({ dryRun: true });
  assert.deepEqual([r.persisted, r.counts.pass, (await one(`select count(*)::int as n from ops.watch_runs`)).n], [false, 45, before]);
});

console.log('W1 / W2 / W3: 前提と案件の遷移');
await t('🚨 今日の fba_jp の取得記録が無い → W1 breach (新しい案件)。同じ朝をもう一度 → 案件は増えず days_seen も増えない (同じ日)。翌朝も無い → 継続 2 日。直った → 回復 (transitions 2)', async () => {
  await delCapture(D(0), 'fba_jp');
  const r1 = await run();
  assert.deepEqual([verdictOf(r1, 'W1', 'fba_jp/jp'), r1.counts.new, r1.counts.breach], ['breach', 1, 1]);
  assert.match(r1.lastLine, /^⚠️ .*異常 1 \(新 1 \/ 継続 0\).* — W1 fba_jp\/jp: 対象日 2026-09-23 の取得記録が無い.*\(新\)/);
  const i1 = await one(`select watch_issue_id, state, days_seen, transitions, subject_key, first_result_id, last_result_id from ops.watch_issues where check_id = 'W1' and scope_key = 'fba_jp/jp'`);
  assert.deepEqual([i1.state, i1.days_seen, i1.transitions, i1.subject_key, i1.first_result_id === i1.last_result_id], ['open', 1, 1, '', true]);
  const r1b = await run({ now: new Date('2026-09-23T02:00:00Z') });   // 同じ朝の再実行 (retry)
  assert.deepEqual([r1b.counts.new, r1b.counts.continued, (await one(`select days_seen from ops.watch_issues where watch_issue_id = $1`, [i1.watch_issue_id])).days_seen, (await one(`select count(*)::int as n from ops.watch_issues`)).n], [0, 1, 1, 1]);
  // 翌朝 (asOf を進める。翌朝の分の取得記録を作り、fba_jp だけ無いまま)
  for (const s of CONFIG.STOCK_SCOPES) if (s.source !== 'fba_jp') await capture(D(s.dayOffset + 1), s.source, s.scope, s.source === 'fba_us' ? 'partial' : 'complete');
  await diffDay(D(0), 'done', { events: 3 });
  const r2 = await run({ asOf: D(1), now: new Date('2026-09-24T00:30:00Z') });
  // 翌朝: W1 (今日 = D(1) も無い) は継続 2 日。昨日 (D(0)) の穴は W2 の窓に入る = W2 の日付の案件が新しく 1 つ
  assert.deepEqual([r2.counts.new, r2.counts.continued, r2.counts.recovered, r2.notes.new[0].checkId, r2.notes.new[0].subjectKey], [1, 1, 0, 'W2', D(0)]);
  assert.match(r2.lastLine, /継続 2 日/);
  assert.deepEqual((await one(`select days_seen, transitions, state from ops.watch_issues where watch_issue_id = $1`, [i1.watch_issue_id])), { days_seen: 2, transitions: 1, state: 'open' });
  await capture(D(1), 'fba_jp', 'jp', 'complete');
  const r3 = await run({ asOf: D(1), now: new Date('2026-09-24T02:00:00Z') });
  assert.deepEqual([r3.counts.recovered, r3.counts.breach, r3.counts.continued], [1, 1, 1]);   // W1 は回復。W2 の D(0) の穴は残る (継続)
  const i3 = await one(`select state, transitions, recovered_at is not null as rec from ops.watch_issues where watch_issue_id = $1`, [i1.watch_issue_id]);
  assert.deepEqual([i3.state, i3.transitions, i3.rec], ['recovered', 2, true]);
  // 元に戻す (今日の分)
  await capture(D(0), 'fba_jp', 'jp', 'complete');
  await pg.query(`delete from snapshots.stock_diff_days where to_date = $1::date`, [D(0)]);
  for (const s of CONFIG.STOCK_SCOPES) await delCapture(D(s.dayOffset + 1), s.source);
  await pg.query(`delete from ops.watch_issues`);   // 次の試験のために案件を空に
});
await t('🚨 前提が欠ければ blocked (pass にしない)・上流の障害は 1 件: 昨日の logizard が missing → W1 breach・W3 は blocked (前提 W1)・案件は W1 だけ。W3 の案件は作られない', async () => {
  await pg.query(`delete from snapshots.stock_diff_days where to_date = $1::date or from_date = $1::date`, [D(-1)]);   // 差の印は取得記録を FK で指す = 先に消す
  await delCapture(D(-1), 'logizard'); await capture(D(-1), 'logizard', 'main', 'missing');
  const r = await run();
  const w3 = resultOf(r, 'W3', 'logizard/main');
  assert.deepEqual([verdictOf(r, 'W1', 'logizard/main'), w3.verdict, w3.blockedBy, /前提 W1/.test(w3.reason), r.counts.blocked, r.counts.new, verdictOf(r, 'W5', 'logizard/main'), resultOf(r, 'W5', 'logizard/main').blockedBy, verdictOf(r, 'W6', 'all/jp'), resultOf(r, 'W6', 'all/jp').blockedBy], ['breach', 'blocked', 'W1:logizard/main', true, 4, 1, 'blocked', 'W3:logizard/main', 'blocked', 'W1:logizard/main']);   // 判定保留 = W3・W5・W6・W4 (前提 W3)   // new 1 = W1 logizard だけ (W2 の窓は logizard では D(-8)〜D(-2) = 昨日はまだ入らない。明日から)
  assert.equal((await one(`select count(*)::int as n from ops.watch_issues where check_id = 'W3' and state = 'open'`)).n, 0);
  assert.match(r.lastLine, /判定保留 4/);
  await delCapture(D(-1), 'logizard'); await capture(D(-1), 'logizard', 'main', 'complete'); await diffDay(D(-1), 'done', { events: 10 });
  await pg.query(`delete from ops.watch_issues`);   // 次の試験のために案件を空に
});
await t('W3: 昨日の差の印が無い → breach / skipped (前日の欠測) は pass (理由つき) / 0022 が無い DB では blocked', async () => {
  await pg.query(`delete from snapshots.stock_diff_days where to_date = $1::date`, [D(-1)]);
  let r = await run({ dryRun: true });
  assert.deepEqual([verdictOf(r, 'W3', 'logizard/main'), /印が無い/.test(resultOf(r, 'W3', 'logizard/main').reason)], ['breach', true]);
  await diffDay(D(-1), 'skipped', { skip: 'prev_not_complete' });
  r = await run({ dryRun: true });
  assert.deepEqual([verdictOf(r, 'W3', 'logizard/main'), resultOf(r, 'W3', 'logizard/main').observed.skip_reason], ['pass', 'prev_not_complete']);
  await pg.query(`delete from snapshots.stock_diff_days where to_date = $1::date`, [D(-1)]); await diffDay(D(-1), 'done', { events: 10 });
  const pg2 = new PGlite(); await applyMigrations(pgliteAdapter(pg2), { log: quiet, to: '0021' });
  // W1 (logizard の昨日) を pass にしておく = W3 が「前提」ではなく「0022 未適用」で blocked になることを確かめる
  await pg2.query(`insert into ops.ingest_runs (ingest_run_id, source_system, entity, scope_key, host, started_at, finished_at, status, complete, rows_seen, source_tz, checksum) values ('r', 'logizard', 'inventory', 'main', 't', now(), now(), 'success', true, 1, 'UTC', 'g')`);
  await pg2.query(`insert into snapshots.stock_capture_days (snapshot_date, source, scope_key, company_id, status, ingest_run_id, completed_at) values ($1::date, 'logizard', 'main', 1, 'complete', 'r', now())`, [D(-1)]);
  const r2 = await runWatch({ db: pgliteAdapter(pg2), writer: null, config: CONFIG, asOf: ASOF, evidence: {}, now: NOW, log: quiet });
  assert.deepEqual([verdictOf(r2, 'W1', 'logizard/main'), verdictOf(r2, 'W3', 'logizard/main'), /0022/.test(resultOf(r2, 'W3', 'logizard/main').reason)], ['pass', 'blocked', true]);
  await assert.rejects(runWatch({ db: pgliteAdapter(pg2), writer: pgliteAdapter(pg2), config: CONFIG, asOf: ASOF, evidence: {}, now: NOW, log: quiet, syncRunId: SYNC }), /0023/);   // 記録しようとすれば止まる
  await pg2.close();
});
await t('W1: building の滞留 (2 時間超) は breach。2 時間以内なら見ない', async () => {
  await delCapture(D(0), 'ne'); await capture(D(0), 'ne', 'main', 'building', { builtAt: '2026-09-22T21:00:00Z' });   // 3.5 時間前
  let r = await run({ dryRun: true });
  assert.deepEqual([verdictOf(r, 'W1', 'ne/main'), /滞留/.test(resultOf(r, 'W1', 'ne/main').reason)], ['breach', true]);
  await delCapture(D(0), 'ne'); await capture(D(0), 'ne', 'main', 'building', { builtAt: '2026-09-23T00:00:00Z' });   // 30 分前
  r = await run({ dryRun: true });
  assert.deepEqual([verdictOf(r, 'W1', 'ne/main'), /building のまま/.test(resultOf(r, 'W1', 'ne/main').reason), /滞留/.test(resultOf(r, 'W1', 'ne/main').reason)], ['breach', true, false]);
  await delCapture(D(0), 'ne'); await capture(D(0), 'ne', 'main', 'complete');
});
await t('🚨 W2: 窓 (7 日) の中の missing / partial / 行なし を日ごとの案件に。fba_us の partial は数えない (例外)・例外の期限が切れれば数える。窓から外れた日は「監視期間外」(回復ではない)', async () => {
  await delCapture(D(-3), 'ne'); await capture(D(-3), 'ne', 'main', 'missing');
  await delCapture(D(-5), 'ne'); await capture(D(-5), 'ne', 'main', 'partial');
  await delCapture(D(-6), 'ne');   // 行なし
  let r = await run();
  const w2 = resultOf(r, 'W2', 'ne/main');
  assert.deepEqual([w2.verdict, w2.items.map((i) => `${i.subjectKey.slice(5)}:${i.payload.status}`), w2.periodFrom, w2.periodTo, verdictOf(r, 'W2', 'fba_us/us')], ['breach', [`${D(-6).slice(5)}:absent`, `${D(-5).slice(5)}:partial`, `${D(-3).slice(5)}:missing`], D(-7), D(-1), 'pass']);
  assert.equal((await one(`select count(*)::int as n from ops.watch_issues where check_id = 'W2' and scope_key = 'ne/main' and state = 'open'`)).n, 3);
  assert.equal((await one(`select count(*)::int as n from ops.watch_result_items where watch_result_id = (select watch_result_id from ops.watch_results where watch_run_id = $1 and check_id = 'W2' and scope_key = 'ne/main')`, [r.runId])).n, 3);
  // 例外の期限切れ: fba_us の partial が数えられる
  const expired = { ...CONFIG, STOCK_SCOPES: CONFIG.STOCK_SCOPES.map((s) => (s.source === 'fba_us' ? { ...s, allowPartial: { ...s.allowPartial, until: '2026-09-01' } } : s)) };
  const rx = await run({ dryRun: true, config: expired });
  assert.deepEqual([verdictOf(rx, 'W2', 'fba_us/us'), verdictOf(rx, 'W1', 'fba_us/us'), /期限切れ/.test(resultOf(rx, 'W1', 'fba_us/us').reason)], ['breach', 'breach', true]);
  // 8 日後: 3 つの日は窓の外 → 監視期間外 (回復ではない)。その間の日は complete
  for (const s of CONFIG.STOCK_SCOPES) for (let n = 1; n <= 8; n++) await capture(D(s.dayOffset + n), s.source, s.scope, s.source === 'fba_us' ? 'partial' : 'complete');
  for (let n = 0; n <= 7; n++) await diffDay(D(n), 'done', { events: 1 });
  const r8 = await run({ asOf: D(8), now: new Date('2026-10-01T00:30:00Z') });
  assert.deepEqual([verdictOf(r8, 'W2', 'ne/main'), r8.counts.out_of_window, r8.counts.recovered], ['pass', 3, 0]);
  assert.match(r8.lastLine, /監視期間外 3/);
  assert.deepEqual((await all(`select state, transitions from ops.watch_issues where check_id = 'W2' and scope_key = 'ne/main' order by subject_key`)).map((x) => `${x.state}:${x.transitions}`), ['out_of_window:2', 'out_of_window:2', 'out_of_window:2']);
  // 元に戻す (差の印は取得記録を FK で指す = 先に消す)
  await pg.query(`delete from snapshots.stock_diff_days where to_date >= $1::date`, [D(0)]);
  for (const s of CONFIG.STOCK_SCOPES) for (let n = 1; n <= 8; n++) await delCapture(D(s.dayOffset + n), s.source);
  await pg.query(`delete from ops.watch_issues`);
  for (const n of [-3, -5]) { await delCapture(D(n), 'ne'); await capture(D(n), 'ne', 'main', 'complete'); }
  await capture(D(-6), 'ne', 'main', 'complete');
});

await t('W2: 監視の開始日 (since) より前の日は数えない (在庫日次を作る前・バックフィルで埋まらない履歴)。窓の頭が since で切れる・全部が since より前なら評価する日は 0 で pass', async () => {
  // ne/main の D(-7)〜D(-4) を消す = since が無ければ 4 日の欠測
  for (let n = -7; n <= -4; n++) await delCapture(D(n), 'ne');
  let r = await run({ dryRun: true });
  assert.deepEqual([verdictOf(r, 'W2', 'ne/main'), resultOf(r, 'W2', 'ne/main').items.length], ['breach', 4]);
  const withSince = (since) => ({ ...CONFIG, STOCK_SCOPES: CONFIG.STOCK_SCOPES.map((s) => (s.source === 'ne' ? { ...s, since } : s)) });
  r = await run({ dryRun: true, config: withSince(D(-3)) });
  const w2 = resultOf(r, 'W2', 'ne/main');
  assert.deepEqual([w2.verdict, w2.items.length, w2.periodFrom, w2.periodTo, w2.observed.days_evaluated, w2.observed.since, w2.sampleSize], ['pass', 0, D(-3), D(-1), 3, D(-3), 3]);
  r = await run({ dryRun: true, config: withSince(D(-5)) });
  assert.deepEqual([verdictOf(r, 'W2', 'ne/main'), resultOf(r, 'W2', 'ne/main').items.map((i) => i.subjectKey)], ['breach', [D(-5), D(-4)]]);
  r = await run({ dryRun: true, config: withSince(D(1)) });   // 全部 since より前 (明日から監視) = 評価する日 0 で pass・期間なし
  assert.deepEqual([verdictOf(r, 'W2', 'ne/main'), resultOf(r, 'W2', 'ne/main').observed.days_evaluated, resultOf(r, 'W2', 'ne/main').periodFrom], ['pass', 0, null]);
  assert.equal(plannedKeys(withSince(D(1))).length, 45);   // since は評価キーを減らさない
  for (let n = -7; n <= -4; n++) await capture(D(n), 'ne', 'main', 'complete');
});

console.log('W5 / W6: 解決できない在庫の差・売れ筋 SKU の欠品');
/** 日次の行を直接作る (取得記録を一時的に building に戻して入れる = trigger の約束)。rows = [code, skuId, qty, { fba_available }] */
async function dailyRows(day, source, scope, rows) {
  const cap = await one(`select ingest_run_id, status from snapshots.stock_capture_days where snapshot_date = $1::date and source = $2 and scope_key = $3`, [day, source, scope]);
  await pg.query(`update snapshots.stock_capture_days set status = 'building', completed_at = null where snapshot_date = $1::date and source = $2 and scope_key = $3`, [day, source, scope]);
  for (const [code, skuId, qty, extra = {}] of rows) await pg.query(`insert into snapshots.sku_stock_daily (snapshot_date, source, scope_key, source_code, company_id, sku_id, qty, fba_available, captured_at, ingest_run_id) values ($1::date, $2, $3, $4, 1, $5, $6, $7, $8::timestamptz, $9)
    on conflict (snapshot_date, source, scope_key, source_code) do update set sku_id = excluded.sku_id, qty = excluded.qty, fba_available = excluded.fba_available`, [day, source, scope, code, skuId, qty, extra.fba_available ?? null, TS(day, 2), cap.ingest_run_id]);
  await pg.query(`update snapshots.stock_capture_days set status = $4, completed_at = case when $4 = 'complete' then $5::timestamptz end where snapshot_date = $1::date and source = $2 and scope_key = $3`, [day, source, scope, cap.status, TS(day, 2)]);
}
const delDaily = (days, source) => pg.exec(`begin; set local snapshots.maintenance = 'on'; delete from snapshots.sku_stock_daily where source = '${source}' and snapshot_date in (${days.map((d) => `date '${d}'`).join(', ')}); commit;`);
async function salesRow(runId, mall, scope, day, skuId, units, cancelled = 0, listingId = null) {
  const allCancelled = cancelled >= units;
  await pg.query(`insert into mart.sales_daily (run_id, company_id, date_jst, mall, scope_key, sku_id, listing_id, orders, orders_cancelled, lines, units_ordered, units_cancelled, items_amount_jpy, cancelled_items_amount_jpy, sales_jpy, customer_paid_jpy)
    values ($1, 1, $2::date, $3, $4, $5, $11, 1, $6, 1, $7, $8, 1000, $9, $10, $10)`, [runId, day, mall, scope, skuId, allCancelled ? 1 : 0, units, cancelled, allCancelled ? 1000 : 0, allCancelled ? 0 : 1000, listingId]);
}
await t('W5: 昨日の差の unresolved_changed (件数) と数量の割合 (日次の元から計算)。上限以内は pass / 印と今の日次が食い違えば blocked / 件数超え → breach (info) / done の日で 3 日続けば warn / skipped が挟まれば連続は切れる / 割合超え → breach / 昨日が skipped なら pass (理由つき) / W3 が pass でなければ blocked', async () => {
  let r = await run({ dryRun: true });
  let w = resultOf(r, 'W5', 'logizard/main');
  assert.deepEqual([w.verdict, w.observed.unresolved_changed, w.observed.share, w.observed.streak, w.severity, w.observed.days.length], ['pass', 0, 0, 0, 'info', 3]);
  const setUnresolved = (day, n) => pg.query(`update snapshots.stock_diff_days set unresolved_changed = $2 where to_date = $1::date and source = 'logizard'`, [day, n]);
  // 🚨 印 (11 件) と今の日次 (0 件) が食い違う → 混ぜて pass にしない = blocked
  await setUnresolved(D(-1), 11);
  r = await run({ dryRun: true }); w = resultOf(r, 'W5', 'logizard/main');
  assert.deepEqual([w.verdict, /食い違う/.test(w.reason), w.observed.unresolved_codes_now], ['blocked', true, 0]);
  // 件数超え: D(-4)〜D(-1) の日次に SKU の分からないコード 11 個が毎日 1 ずつ動く (+ 解決済みの大きな動き = 割合は 11 / 911 = 1.2% で上限以内)
  const U = Array.from({ length: 11 }, (_, i) => `U-${String(i + 1).padStart(2, '0')}`);
  for (let n = -4; n <= -1; n++) await dailyRows(D(n), 'logizard', 'main', [['AAA-1', skuA, n % 2 ? 1000 : 100], ...U.map((c) => [c, null, n + 4])]);
  r = await run({ dryRun: true }); w = resultOf(r, 'W5', 'logizard/main');
  assert.deepEqual([w.verdict, w.severity, w.observed.streak, w.observed.unresolved_codes_now, w.observed.changed_codes, w.observed.unresolved_qty, w.observed.changed_qty, w.observed.share, /11 件/.test(w.reason)], ['breach', 'info', 1, 11, 12, 11, 911, 0.0121, true]);
  await setUnresolved(D(-2), 11); await setUnresolved(D(-3), 11);
  r = await run({ dryRun: true }); w = resultOf(r, 'W5', 'logizard/main');
  assert.deepEqual([w.verdict, w.severity, w.observed.streak, w.observed.days.length, /3 日連続/.test(w.reason)], ['breach', 'warn', 3, 3, true]);
  // D(-2) が skipped → 連続が切れる (info に戻る)
  await pg.query(`update snapshots.stock_diff_days set status = 'skipped', skip_reason = 'prev_not_complete', from_date = null, events = 0, unresolved_changed = 0 where to_date = $1::date and source = 'logizard'`, [D(-2)]);
  r = await run({ dryRun: true }); w = resultOf(r, 'W5', 'logizard/main');
  assert.deepEqual([w.verdict, w.severity, w.observed.streak, w.observed.days.length], ['breach', 'info', 1, 1]);
  await pg.query(`update snapshots.stock_diff_days set status = 'done', skip_reason = null, from_date = $2::date, events = 10, unresolved_changed = 11 where to_date = $1::date and source = 'logizard'`, [D(-2), D(-3)]);
  // 過去の日の食い違いは連続を切るだけ (昨日は評価する)
  await setUnresolved(D(-2), 5);
  r = await run({ dryRun: true }); w = resultOf(r, 'W5', 'logizard/main');
  assert.deepEqual([w.verdict, w.observed.streak, w.observed.days.length], ['breach', 1, 1]);
  await delDaily([D(-4), D(-3), D(-2), D(-1)], 'logizard'); for (let n = -3; n <= -1; n++) await setUnresolved(D(n), 0);
  // 数量の割合: 件数は 1 (上限以内) でも、SKU の分からないコードの数量が 50 / 52 = 96% → breach
  await dailyRows(D(-2), 'logizard', 'main', [['AAA-1', skuA, 100], ['bbb-2', skuB, 10], ['U-1', null, 0]]);
  await dailyRows(D(-1), 'logizard', 'main', [['AAA-1', skuA, 100], ['bbb-2', skuB, 12], ['U-1', null, 50]]);
  await setUnresolved(D(-1), 1);
  r = await run({ dryRun: true }); w = resultOf(r, 'W5', 'logizard/main');
  assert.deepEqual([w.verdict, w.observed.unresolved_changed, w.observed.unresolved_codes_now, w.observed.changed_codes, w.observed.unresolved_qty, w.observed.changed_qty, w.observed.share, w.sampleSize, w.inputGeneration], ['breach', 1, 1, 2, 50, 52, 0.9615, 2, { from: D(-2), to: D(-1) }]);
  await delDaily([D(-2), D(-1)], 'logizard'); await setUnresolved(D(-1), 0);
  // 昨日が skipped → pass (理由つき = W2 で見る)
  await pg.query(`update snapshots.stock_diff_days set status = 'skipped', skip_reason = 'prev_not_complete', from_date = null, events = 0, unresolved_changed = 0 where to_date = $1::date and source = 'logizard'`, [D(-1)]);
  r = await run({ dryRun: true }); w = resultOf(r, 'W5', 'logizard/main');
  assert.deepEqual([w.verdict, /W2 で見る/.test(w.reason)], ['pass', true]);
  await pg.query(`update snapshots.stock_diff_days set status = 'done', skip_reason = null, from_date = $2::date, events = 10, unresolved_changed = 0 where to_date = $1::date and source = 'logizard'`, [D(-1), D(-2)]);
  // W3 が pass でなければ blocked (印が無い)
  await pg.query(`delete from snapshots.stock_diff_days where to_date = $1::date`, [D(-1)]);
  r = await run({ dryRun: true }); w = resultOf(r, 'W5', 'logizard/main');
  assert.deepEqual([verdictOf(r, 'W3', 'logizard/main'), w.verdict, w.blockedBy], ['breach', 'blocked', 'W3:logizard/main']);
  await diffDay(D(-1), 'done', { events: 10 });
  r = await run({ dryRun: true });
  assert.equal(verdictOf(r, 'W5', 'logizard/main'), 'pass');
});
await t('🚨 W6: 直近 28 日に売れた SKU で 倉庫 + FBA JP が 0 → SKU ごとの案件 (新 = 発生) / 全部取消なら売れ筋ではない / セットは構成 SKU × 数量に展開 / 展開できない販売が多ければ blocked / 注文があるのに未公開の日・開いた session があれば blocked / 在庫が入れば回復 / 廃番・窓から外れた SKU は「監視対象外」(回復ではない) / FBA だけにあっても在庫あり / 2 週間後は warn / W1 のどれかが pass でなければ blocked / 在庫が不明なら blocked / 廃番にした変化は世代で捕まえる', async () => {
  let r = await run({ dryRun: true });
  let w = resultOf(r, 'W6', 'all/jp');
  assert.deepEqual([w.verdict, w.severity, w.observed.skus, w.observed.sold_skus, w.periodFrom, w.periodTo, w.observed.unpublished_days], ['pass', 'info', 3, 0, D(-28), D(-1), 0]);
  const runId = await published('aupay', 'main', D(-3));
  await salesRow(runId, 'aupay', 'main', D(-3), skuA, 5, 0);
  await salesRow(runId, 'aupay', 'main', D(-3), skuB, 2, 2);   // 全部取消 = 売れていない
  r = await run(); w = resultOf(r, 'W6', 'all/jp');
  assert.deepEqual([w.verdict, w.items.map((i) => [i.subjectKey, i.payload.code, i.payload.units, i.payload.warehouse_qty, i.payload.fba_jp_available]), w.sampleSize, w.itemTotal, r.counts.new, w.observed.unexpanded_units], ['breach', [[String(skuA), 'AAA-1', 5, 0, 0]], 1, 1, 1, 0]);
  assert.deepEqual(await one(`select subject_type, subject_key, severity, state from ops.watch_issues where check_id = 'W6' and state = 'open'`), { subject_type: 'sku', subject_key: String(skuA), severity: 'info', state: 'open' });
  assert.match(r.lastLine, /異常 1/);
  // セット (sku_id null・listing あり) は listing_components で構成 SKU × 数量に展開 (bbb-2 × 2 × 3 個 = 6)
  await pg.query(`insert into core.listings (company_id, mall, listing_code) values (1, 'aupay', 'SET-1')`);
  const listingId = (await one(`select listing_id from core.listings where listing_code = 'SET-1'`)).listing_id;
  await pg.query(`insert into core.listing_components (company_id, listing_id, sku_id, qty, resolution, resolved_by_type) values (1, $1, $2, 2, 'manual', 'human')`, [listingId, skuB]);
  await salesRow(runId, 'aupay', 'main', D(-3), null, 3, 0, listingId);
  const runId4 = await published('aupay', 'main', D(-4));
  await salesRow(runId4, 'aupay', 'main', D(-4), skuB, 1, 0);   // 別の日に直接も売れた = 販売日数は和集合で 2
  r = await run({ dryRun: true }); w = resultOf(r, 'W6', 'all/jp');
  assert.deepEqual([w.items.map((i) => [i.payload.code, i.payload.units, i.payload.days_sold, i.payload.last_sold]), w.sampleSize, w.observed.total_units, w.observed.unexpanded_units], [[['bbb-2', 7, 2, D(-3)], ['AAA-1', 5, 1, D(-3)]], 2, 9, 0]);
  await pg.query(`delete from mart.sales_daily where run_id = $1 and listing_id = $2`, [runId, listingId]); await pg.query(`delete from mart.sales_daily where run_id = $1`, [runId4]);
  // 展開できない販売 (listing にも当たらない) が正味数量の 10% を超えれば blocked (販売履歴が不完全)。少なければ観測に残して続ける
  await salesRow(runId, 'aupay', 'main', D(-3), null, 100, 0);
  r = await run({ dryRun: true }); w = resultOf(r, 'W6', 'all/jp');
  assert.deepEqual([w.verdict, /展開できない/.test(w.reason), w.observed.unexpanded_share], ['blocked', true, 0.9524]);
  await pg.query(`delete from mart.sales_daily where run_id = $1 and sku_id is null`, [runId]);
  // 🚨 注文があるのに未公開の日が窓の中にある → blocked (未公開の売上を「売れていない」と読まない)
  await order('aupay', 'main', D(-5), 'w6-gap');
  r = await run({ dryRun: true }); w = resultOf(r, 'W6', 'all/jp');
  assert.deepEqual([w.verdict, /未公開の日 1/.test(w.reason), w.observed.unpublished_days], ['blocked', true, 1]);
  const run5 = await published('aupay', 'main', D(-5));
  r = await run({ dryRun: true });
  assert.equal(verdictOf(r, 'W6', 'all/jp'), 'breach');
  // 開いた session = W9 (前提 W9:*) で止まる。公開行があっても作り直しが失敗した朝 (証跡 sales.ok=false) も W9 → W6 は blocked (公開済みの古い売上で pass にしない。Codex R2)
  await salesState('rakuten', 'main', { sessionId: 'w6-open' });
  r = await run({ dryRun: true }); w = resultOf(r, 'W6', 'all/jp');
  assert.deepEqual([w.verdict, w.blockedBy, /開いたまま/.test(w.reason)], ['blocked', 'W9:rakuten/main', true]);
  await salesState('rakuten', 'main');
  r = await run({ dryRun: true, evidence: { ...goodEvidence(), 'orders-aupay': ev('aupay', 'main', { sales: { ok: false, error: 'timeout' } }) } });   // 変更ゼロ (W7 pass) だが作り直しが失敗
  w = resultOf(r, 'W6', 'all/jp');
  assert.deepEqual([verdictOf(r, 'W7', 'aupay/main'), verdictOf(r, 'W9', 'aupay/main'), w.verdict, w.blockedBy], ['pass', 'breach', 'blocked', 'W9:aupay/main']);
  // 評価そのものにも守りがある (前提を外して直接呼んでも開いた session で止まる)
  await salesState('rakuten', 'main', { sessionId: 'w6-open' });
  const w6direct = (await evalW6({ db, config: CONFIG, asOf: ASOF, now: NOW }, CONFIG.checkById('W6')))[0];
  assert.deepEqual([w6direct.verdict, /開いた session: rakuten\/main/.test(w6direct.reason)], ['blocked', true]);
  await salesState('rakuten', 'main');
  // 倉庫に入った (最新の complete の日 = D(-1)) → 回復 (解消)
  await dailyRows(D(-1), 'logizard', 'main', [['AAA-1', skuA, 3]]);
  r = await run();
  assert.deepEqual([verdictOf(r, 'W6', 'all/jp'), r.counts.recovered, r.notes.recovered[0].reason, (await one(`select state from ops.watch_issues where check_id = 'W6' and subject_key = $1 order by watch_issue_id desc limit 1`, [String(skuA)])).state], ['pass', 1, null, 'recovered']);
  await delDaily([D(-1)], 'logizard');
  // 🚨 在庫 0 のまま廃番にした → 「監視対象外」(回復ではない)。窓から売上が外れたときも同じ
  r = await run(); assert.equal(r.counts.new, 1);   // また発生 (新しい案件)
  await pg.query(`update core.skus set handling = 'discontinued' where sku_id = $1`, [skuA]);
  r = await run(); w = resultOf(r, 'W6', 'all/jp');
  assert.deepEqual([w.verdict, r.counts.recovered, r.counts.out_of_window, r.notes.outOfWindow[0].reason, w.observed.out_of_scope, (await one(`select state, summary from ops.watch_issues where check_id = 'W6' and subject_key = $1 order by watch_issue_id desc limit 1`, [String(skuA)]))], ['pass', 0, 1, 'discontinued', { [String(skuA)]: 'discontinued' }, { state: 'out_of_window', summary: `W6 all/jp ${skuA}: 監視対象外 (discontinued)` }]);
  await pg.query(`update core.skus set handling = 'active' where sku_id = $1`, [skuA]);
  r = await run(); assert.equal(r.counts.new, 1);
  await pg.query(`delete from mart.sales_daily where run_id = $1`, [runId]);   // 売上が窓から消えた (在庫は 0 のまま)
  r = await run();
  assert.deepEqual([r.counts.recovered, r.counts.out_of_window, r.notes.outOfWindow[0].reason], [0, 1, 'no_sales_in_window']);
  await salesRow(runId, 'aupay', 'main', D(-3), skuA, 5, 0);
  // FBA だけにある (fba_available > 0) → 在庫あり。available 0 なら在庫なし
  await dailyRows(D(0), 'fba_jp', 'jp', [['AAA-1', skuA, 2, { fba_available: 2 }]]);
  r = await run({ dryRun: true });
  assert.equal(verdictOf(r, 'W6', 'all/jp'), 'pass');
  await dailyRows(D(0), 'fba_jp', 'jp', [['AAA-1', skuA, 0, { fba_available: 0 }]]);
  r = await run({ dryRun: true });
  assert.deepEqual([verdictOf(r, 'W6', 'all/jp'), resultOf(r, 'W6', 'all/jp').items.length], ['breach', 1]);
  await delDaily([D(0)], 'fba_jp');
  // 2 週間を過ぎれば warn
  r = await run({ dryRun: true, config: { ...CONFIG, W6_INFO_UNTIL: '2026-09-01' } });
  assert.deepEqual([verdictOf(r, 'W6', 'all/jp'), resultOf(r, 'W6', 'all/jp').severity], ['breach', 'warn']);
  // 前提: W1 のどれか (fba_jp の今日) が無い → blocked (SKU の在庫を「0」と読まない)
  await delCapture(D(0), 'fba_jp');
  r = await run({ dryRun: true }); w = resultOf(r, 'W6', 'all/jp');
  assert.deepEqual([w.verdict, w.blockedBy], ['blocked', 'W1:fba_jp/jp']);
  await capture(D(0), 'fba_jp', 'jp', 'complete');
  // 世代: snapshot の後に廃番にされた → 指紋が変わり再評価 (attempts 2)
  r = await run({ dryRun: true, hooks: { afterSnapshot: async (n) => { if (n === 1) await pg.query(`update core.skus set handling = 'discontinued' where sku_id = $1`, [skuA]); } } });
  assert.deepEqual([r.attempts, verdictOf(r, 'W6', 'all/jp')], [2, 'pass']);
  await pg.query(`update core.skus set handling = 'active' where sku_id = $1`, [skuA]);
  // 在庫が不明 (complete な日が 1 つも無い DB) = view の as_of が null → blocked。評価だけを直接呼ぶ
  const pg4 = new PGlite(); await applyMigrations(pgliteAdapter(pg4), { log: quiet });
  await pg4.query(`insert into core.products (company_id, name) values (1, 'p')`); await pg4.query(`insert into core.skus (company_id, product_id, sku_kind, code, name) select 1, product_id, 'single', 'X-1', 'x' from core.products`);
  const w4 = (await evalW6({ db: pgliteAdapter(pg4), config: CONFIG, asOf: ASOF, now: NOW }, CONFIG.checkById('W6')))[0];
  assert.deepEqual([w4.verdict, /在庫が不明/.test(w4.reason), /倉庫/.test(w4.reason), /FBA JP/.test(w4.reason)], ['blocked', true, true, true]);
  await pg4.close();
  // 片づけ (売上の行・公開・注文・案件)
  for (const rid of [runId, runId4, run5]) { await pg.query(`delete from mart.sales_daily where run_id = $1`, [rid]); await pg.query(`delete from mart.sales_daily_published where run_id = $1`, [rid]); await pg.query(`delete from mart.sales_daily_runs where run_id = $1`, [rid]); }
  await pg.query(`delete from core.orders where mall = 'aupay' and mall_order_no = 'w6-gap'`);
  await pg.query(`delete from core.listing_components where listing_id = $1`, [listingId]); await pg.query(`delete from core.listings where listing_id = $1`, [listingId]);
  await pg.query(`delete from ops.watch_issues where check_id = 'W6'`);
});
await t('🚨 W6 の NE のセット商品 (9/24 本番: セット 618 件が「在庫 0」で案件になった): セットの SKU が売れたら sku_components で構成品 × 数量に展開・セット自体は判定しない / 構成の無いセットは「展開できない販売」/ 開いていたセットの案件は「監視対象外 (set_sku)」= 回復ではない', async () => {
  await pg.query(`insert into core.skus (company_id, product_id, sku_kind, code, name) values (1, null, 'set', 'NESET-1', 'セット 1'), (1, null, 'set', 'NESET-2', 'セット 2 (構成なし)')`);
  const set1 = await skuOf('NESET-1'), set2 = await skuOf('NESET-2'), skuC = await skuOf('CCC-3');
  await pg.query(`insert into core.sku_components (company_id, parent_sku_id, child_sku_id, qty, source) values (1, $1, $2, 2, 'ne')`, [set1, skuC]);
  const runId = await published('aupay', 'main', D(-5));
  await salesRow(runId, 'aupay', 'main', D(-5), set1, 40, 0);   // セット 1 が 40 個 = CCC-3 が 80 個 (展開できない 1 個が 10% を超えない量)
  await salesRow(runId, 'aupay', 'main', D(-5), set2, 1, 0);   // 構成の無いセット = 展開できない
  // 前の朝にできたセットの案件 (9/24 本番と同じ状態)
  await pg.query(`insert into ops.watch_issues (company_id, check_id, scope_key, subject_type, subject_key, state, severity, first_seen_at, last_seen_at, summary) values (1, 'W6', 'all/jp', 'sku', $1, 'open', 'info', now(), now(), 'W6 all/jp セット')`, [String(set1)]);
  const r = await run();
  const w = resultOf(r, 'W6', 'all/jp');
  const codes = w.items.map((i) => `${i.payload.code}:${i.payload.units}`);
  assert.deepEqual([w.verdict, codes.includes('CCC-3:80'), codes.some((c) => c.startsWith('NESET')), w.observed.unexpanded_units, w.observed.sets_without_components, w.observed.sold_skus], ['breach', true, false, 1, 1, 1]);   // 売れた SKU = CCC-3 だけ (セットは数えない)
  const iss = await one(`select state, summary from ops.watch_issues where check_id = 'W6' and subject_key = $1`, [String(set1)]);
  assert.deepEqual([iss.state, /監視対象外 \(set_sku\)/.test(iss.summary)], ['out_of_window', true]);
  await pg.query(`delete from mart.sales_daily where run_id = $1`, [runId]);
  await pg.query(`delete from ops.watch_issues where check_id = 'W6'`);
  // 🚨 Codex #1433 R1 #1: 入れ子のセット (NESET-A → NESET-B × 2 → bbb-2 × 3) は末端の単品まで展開する = A が 10 個 → bbb-2 が 60 個
  await pg.query(`insert into core.skus (company_id, product_id, sku_kind, code, name) values (1, null, 'set', 'NESET-A', 'セット A'), (1, null, 'set', 'NESET-B', 'セット B'), (1, null, 'set', 'NESET-X', 'セット X (循環)'), (1, null, 'set', 'NESET-Y', 'セット Y (循環)')`);
  const setA = await skuOf('NESET-A'), setB = await skuOf('NESET-B'), setX = await skuOf('NESET-X'), setY = await skuOf('NESET-Y');
  await pg.query(`insert into core.sku_components (company_id, parent_sku_id, child_sku_id, qty, source) values (1, $1, $2, 2, 'ne'), (1, $2, $3, 3, 'ne'), (1, $4, $5, 1, 'ne'), (1, $5, $4, 1, 'ne')`, [setA, setB, skuB, setX, setY]);
  const runId2 = await published('aupay', 'main', D(-5));
  await salesRow(runId2, 'aupay', 'main', D(-5), setA, 10, 0);
  await salesRow(runId2, 'aupay', 'main', D(-5), skuC, 190, 0);
  let r2 = await run({ dryRun: true });
  let w2 = resultOf(r2, 'W6', 'all/jp');
  assert.deepEqual([w2.items.map((i) => `${i.payload.code}:${i.payload.units}`).filter((c) => c.startsWith('bbb-2')), w2.observed.sold_skus, w2.observed.unexpanded_units], [['bbb-2:60'], 2, 0]);
  // 🚨 Codex #1433 R1 #2: 出品の構成 (× 20) → 構成の無いセット の販売 1 個は、元の数量 1 個で「展開できない」(20 個ではない) / 循環するセットも展開できない (元の 1 個)
  await pg.query(`insert into core.listings (company_id, mall, listing_code) values (1, 'aupay', 'SET-20')`);
  const l20 = (await one(`select listing_id from core.listings where listing_code = 'SET-20'`)).listing_id;
  await pg.query(`insert into core.listing_components (company_id, listing_id, sku_id, qty, resolution, resolved_by_type) values (1, $1, $2, 20, 'manual', 'human')`, [l20, set2]);
  await salesRow(runId2, 'aupay', 'main', D(-5), null, 1, 0, l20);
  await salesRow(runId2, 'aupay', 'main', D(-5), setX, 1, 0);
  r2 = await run({ dryRun: true }); w2 = resultOf(r2, 'W6', 'all/jp');
  assert.deepEqual([w2.verdict, w2.observed.total_units, w2.observed.unexpanded_units, w2.observed.unexpanded_rows], ['breach', 202, 2, 2]);   // 202 = 10 + 190 + 1 + 1。展開できない = SET-20 の 1 + 循環の 1
  await pg.query(`delete from mart.sales_daily where run_id = $1`, [runId2]);
  await pg.query(`delete from core.listing_components where listing_id = $1`, [l20]); await pg.query(`delete from core.listings where listing_id = $1`, [l20]);
});

await t('🚨 W8: 昨日の 件数・売上・取消率・金額不明率 を同じ曜日の過去 8 週の中央値 ± 3×MAD (+ 絶対差) で判定 / 0 件は必ず異常 / 小規模モールは統計なし・取消率と金額不明率の上限だけ / 標本は取込の完了が確かめられた日だけ (突合済みの範囲・翌朝の W7 pass。取込 run・翌々朝の pass は証跡ではない) / 有効標本 4 未満は blocked / 未公開は W9 経由で blocked / 2 週間後は warn / 平常の日が変われば世代で再評価', async () => {
  const w8 = (r, m) => resultOf(r, 'W8', m);
  let r = await run({ dryRun: true });
  let w = w8(r, 'aupay/main');
  assert.deepEqual([w.verdict, w.severity, w.observed.samples, w.observed.stats.small, w.observed.stats.orders.median, w.observed.stats.orders.mad, w.observed.yesterday.orders, w.observed.yesterday.sales, r4(w.observed.yesterday.cancel_rate), w.periodFrom], ['pass', 'info', 8, false, 40, 1, 40, 400000, 0.05, D(-1)]);
  assert.deepEqual([verdictOf(r, 'W8', 'qoo10/main'), w8(r, 'qoo10/main').observed.stats.small, /小規模/.test(w8(r, 'qoo10/main').reason)], ['pass', true, true]);
  assert.deepEqual(CONFIG.ORDER_MALLS.map((m) => verdictOf(r, 'W8', `${m.mall}/${m.scope}`)), ['pass', 'pass', 'pass', 'pass', 'pass']);
  // 件数が少ない: 昨日を 15 件に (平常 40 ± 3 = 差 25 ≥ 20)
  const idxOf = "substring(mall_order_no from '[0-9]+$')::int";
  await pg.query(`delete from core.orders where mall = 'aupay' and order_date_jst = $1::date and mall_order_no like 'w8-%' and ${idxOf} > 15`, [D(-1)]);
  r = await run({ dryRun: true }); w = w8(r, 'aupay/main');
  assert.deepEqual([w.verdict, /件数 15 \(平常 40 ± 3\)/.test(w.reason)], ['breach', true]);
  // 少し少ない (38 件 = 差 2) は騒がない
  await ordersBulk('aupay', 'main', D(-1), 23, 0, 100);
  r = await run({ dryRun: true }); assert.equal(verdictOf(r, 'W8', 'aupay/main'), 'pass');
  // 0 件は必ず異常
  await pg.query(`delete from core.orders where mall = 'aupay' and order_date_jst = $1::date and mall_order_no like 'w8-%'`, [D(-1)]);
  r = await run({ dryRun: true }); w = w8(r, 'aupay/main');
  assert.deepEqual([w.verdict, /注文 0 件 \(平常の中央値 40\)/.test(w.reason)], ['breach', true]);
  await ordersBulk('aupay', 'main', D(-1), 40, 2);
  // 売上が少ない (5 万円 vs 平常 40 万円)。件数は平常どおり
  await pg.query(`update mart.sales_daily set items_amount_jpy = 50000, sales_jpy = 50000, customer_paid_jpy = 50000 where mall = 'aupay' and date_jst = $1::date`, [D(-1)]);
  r = await run({ dryRun: true }); w = w8(r, 'aupay/main');
  assert.deepEqual([w.verdict, /売上 50,000 円 \(平常 400,000/.test(w.reason), /件数/.test(w.reason)], ['breach', true, false]);
  await pg.query(`update mart.sales_daily set items_amount_jpy = 400000, sales_jpy = 400000, customer_paid_jpy = 400000 where mall = 'aupay' and date_jst = $1::date`, [D(-1)]);
  // 取消率 50% (平常 5%)
  await pg.query(`update core.orders set is_cancelled = true, status = 'cancelled' where mall = 'aupay' and order_date_jst = $1::date and mall_order_no like 'w8-%' and ${idxOf} <= 20`, [D(-1)]);
  r = await run({ dryRun: true }); w = w8(r, 'aupay/main');
  assert.deepEqual([w.verdict, /取消率 50% \(平常 5%\)/.test(w.reason)], ['breach', true]);
  await pg.query(`update core.orders set is_cancelled = false, status = 'new' where mall = 'aupay' and order_date_jst = $1::date and mall_order_no like 'w8-%' and ${idxOf} > 2`, [D(-1)]);
  // 金額不明の明細 75% (平常 10%)
  await pg.query(`update mart.sales_daily set lines_amount_unknown = 30 where mall = 'aupay' and date_jst = $1::date`, [D(-1)]);
  r = await run({ dryRun: true }); w = w8(r, 'aupay/main');
  assert.deepEqual([w.verdict, /金額不明の明細 75% \(平常 10%\)/.test(w.reason)], ['breach', true]);
  await pg.query(`update mart.sales_daily set lines_amount_unknown = 4 where mall = 'aupay' and date_jst = $1::date`, [D(-1)]);
  r = await run({ dryRun: true }); assert.equal(verdictOf(r, 'W8', 'aupay/main'), 'pass');
  // 小規模モール (qoo10 = 10 件/日): 3 件でも統計では騒がない / 0 件は異常 / 取消率 50% > 上限 30% は異常
  await pg.query(`delete from core.orders where mall = 'qoo10' and order_date_jst = $1::date and mall_order_no like 'w8-%' and ${idxOf} > 3`, [D(-1)]);
  r = await run({ dryRun: true }); assert.equal(verdictOf(r, 'W8', 'qoo10/main'), 'pass');
  await pg.query(`delete from core.orders where mall = 'qoo10' and order_date_jst = $1::date and mall_order_no like 'w8-%'`, [D(-1)]);
  r = await run({ dryRun: true }); w = w8(r, 'qoo10/main');
  assert.deepEqual([w.verdict, /注文 0 件/.test(w.reason), /小規模/.test(w.reason)], ['breach', true, true]);
  await ordersBulk('qoo10', 'main', D(-1), 10, 5);
  r = await run({ dryRun: true }); w = w8(r, 'qoo10/main');
  assert.deepEqual([w.verdict, /取消率 50% \(上限 30%\)/.test(w.reason)], ['breach', true]);
  await pg.query(`update core.orders set is_cancelled = false, status = 'new' where mall = 'qoo10' and order_date_jst = $1::date and mall_order_no like 'w8-%'`, [D(-1)]);
  // 🚨 平常の標本の完全性: 注文があるのに未公開の日は除外 (売上 0 で平常を下に引かない)。5 日除外すれば標本 3 = blocked
  const baseDays = w8Days(CONFIG, ASOF).baseline;
  const pubOf = async (d) => (await one(`select run_id from mart.sales_daily_published where mall = 'aupay' and date_jst = $1::date`, [d])).run_id;
  const kept = new Map(); for (const d of baseDays.slice(0, 5)) kept.set(d, await pubOf(d));
  await pg.query(`delete from mart.sales_daily_published where mall = 'aupay' and date_jst = $1::date`, [baseDays[0]]);
  r = await run({ dryRun: true }); w = w8(r, 'aupay/main');
  assert.deepEqual([w.verdict, w.observed.samples, w.observed.excluded], ['pass', 7, [`${baseDays[0].slice(5)}:unpublished`]]);
  for (const d of baseDays.slice(1, 5)) await pg.query(`delete from mart.sales_daily_published where mall = 'aupay' and date_jst = $1::date`, [d]);
  r = await run({ dryRun: true }); w = w8(r, 'aupay/main');
  assert.deepEqual([w.verdict, w.observed.samples, /有効標本 3 < 4 \(除外 5: .*unpublished/.test(w.reason)], ['blocked', 3, true]);
  for (const [d, rid] of kept) await pg.query(`insert into mart.sales_daily_published (company_id, mall, scope_key, date_jst, run_id) values (1, 'aupay', 'main', $1::date, $2)`, [d, rid]);
  // 🚨 注文ゼロの日 (Codex R1/R2): 突合済みの範囲 (ordersSince ≤ 日 ≤ reconciledThrough) なら正当なゼロとして標本に (0 件)。範囲の外で証跡も無ければ除外 (取込の穴を平常に混ぜない)
  assert.ok(CONFIG.ORDER_MALLS.every((x) => x.ordersSince <= baseDays[7] && x.reconciledThrough >= baseDays[0]), '見本の標本日 8 つは全部 突合済みの範囲の中 (前提)');
  await pg.query(`delete from core.orders where mall = 'aupay' and order_date_jst = $1::date and mall_order_no like 'w8-%'`, [D(-8)]);
  r = await run({ dryRun: true }); w = w8(r, 'aupay/main');
  assert.deepEqual([w.verdict, w.observed.samples, w.observed.baseline[0].startsWith(`${D(-8).slice(5)}:0/`), w.observed.stats.orders.median, w.observed.reconciled_through], ['pass', 8, true, 40, CONFIG.ORDER_MALLS[2].reconciledThrough]);
  const auMall = (patch) => ({ ...CONFIG, ORDER_MALLS: CONFIG.ORDER_MALLS.map((x) => (x.mall === 'aupay' ? patch(x) : x)) });
  const noRange = auMall((x) => ({ mall: x.mall, scope: x.scope }));
  r = await run({ dryRun: true, config: noRange }); w = w8(r, 'aupay/main');
  assert.deepEqual([w.verdict, w.observed.samples, w.observed.excluded.length, w.observed.excluded[0], w.observed.orders_since, w.observed.reconciled_through, /有効標本 0 < 4 \(除外 8: .* unverified/.test(w.reason)], ['blocked', 0, 8, `${D(-8).slice(5)}:unverified`, null, null, true]);
  await ordersBulk('aupay', 'main', D(-8), 39, 2);
  // 🚨 突合済みの範囲の後の日 (Codex R2 High): 固定の開始日だけで完了と認めない。翌朝の W7 pass (ops.watch_results) だけが証跡
  //   reconciledThrough = D(-30) → D(-8)・D(-15)・D(-22)・D(-29) の 4 日が範囲の外 (証跡なし → unverified) → 標本 4 (ぎりぎり pass)
  const recTo30 = auMall((x) => ({ ...x, reconciledThrough: D(-30) }));
  r = await run({ dryRun: true, config: recTo30 }); w = w8(r, 'aupay/main');
  assert.deepEqual([w.verdict, w.observed.samples, w.observed.excluded, w.observed.evidence_days], ['pass', 4, [D(-8), D(-15), D(-22), D(-29)].map((d) => `${d.slice(5)}:unverified`), []]);
  //   途中取込 (D(-8) が 5 件しか無い = 0 ではないので R1 の規則では標本に入った) → 証跡が無ければ除外される = 中央値 40 のまま
  await pg.query(`delete from core.orders where mall = 'aupay' and order_date_jst = $1::date and mall_order_no like 'w8-%' and ${idxOf} > 5`, [D(-8)]);
  r = await run({ dryRun: true, config: recTo30 }); w = w8(r, 'aupay/main');
  assert.deepEqual([w.verdict, w.observed.samples, w.observed.stats.orders.median, w.observed.baseline.some((b) => b.startsWith(`${D(-8).slice(5)}:`))], ['pass', 4, 40, false]);
  //   証跡: D(-7) (= D(-8) の翌朝) の見張りで aupay の W7 が pass → D(-8) が採用される (5 件のまま入る = 「取込は完了した」と記録された日の値。標本 5・中央値 40)
  const w7At = async (scopeKey, asOf, verdict = 'pass', startedAt = null) => { const id = `wr_${asOf}_${++seq}`; startedAt = startedAt || new Date(new Date(`${asOf}T00:00:00Z`).getTime() + seq * 60000).toISOString(); await pg.query(`insert into ops.watch_runs (watch_run_id, company_id, as_of_date, started_at, checks_version, planned_keys) values ($1, 1, $2::date, $3::timestamptz, 'test', 1)`, [id, asOf, startedAt]); await pg.query(`insert into ops.watch_results (watch_run_id, company_id, check_id, check_version, scope_key, verdict, severity) values ($1, 1, 'W7', 'test', $2, $3, 'error')`, [id, scopeKey, verdict]); return id; };
  await w7At('aupay/main', D(-7));
  r = await run({ dryRun: true, config: recTo30 }); w = w8(r, 'aupay/main');
  assert.deepEqual([w.verdict, w.observed.samples, w.observed.stats.orders.median, w.observed.baseline[0].startsWith(`${D(-8).slice(5)}:5/`), w.observed.evidence_days], ['pass', 5, 40, true, [D(-7)]]);
  await pg.query(`delete from core.orders where mall = 'aupay' and order_date_jst = $1::date and mall_order_no like 'w8-%'`, [D(-8)]); await ordersBulk('aupay', 'main', D(-8), 39, 2);
  //   🚨 証跡にならないもの (Codex R3): W7 が breach / 別の scope (rakuten/main・aupay/sub) の pass / 翌々朝 (D+2) の W7 pass (D(-20) は D(-22) の 2 日後)
  //     / 注文の取込 run (ops.ingest_runs success・complete) だけ = 届いた chunk の処理が済んだだけで走査の完了ではない (整形に失敗した注文を飛ばして残りを送っても success = W7 breach と成功 run が共存する)
  await w7At('aupay/main', D(-14), 'breach'); await w7At('rakuten/main', D(-14)); await w7At('aupay/sub', D(-14));
  await w7At('aupay/main', D(-20));
  const orderRunAt = (mall, scope, day) => pg.query(`insert into ops.ingest_runs (ingest_run_id, source_system, entity, scope_key, host, started_at, finished_at, status, complete, rows_seen, source_tz) values ($1, $2, 'orders', $3, 'test', $4::timestamptz, $4::timestamptz, 'success', true, 5, 'UTC')`, [`t_orders_${mall}_${day}_${++seq}`, mall, scope, `${addDays(day, -1)}T18:00:00Z`]);   // 03:00 JST の day に始まった success・complete の run
  await orderRunAt('aupay', 'main', D(-14)); await orderRunAt('aupay', 'main', D(-21)); await orderRunAt('aupay', 'main', D(-28));
  r = await run({ dryRun: true, config: recTo30 }); w = w8(r, 'aupay/main');
  assert.deepEqual([w.verdict, w.observed.samples, w.observed.excluded, w.observed.evidence_days], ['pass', 5, [D(-15), D(-22), D(-29)].map((d) => `${d.slice(5)}:unverified`), [D(-7)]]);
  //   同じ日 (as_of) に見張りが 2 回あれば最後の回の判定: D(-14) は breach → 再実行で pass = 採用 / D(-21) は pass → 再実行で breach = 不採用
  await w7At('aupay/main', D(-14)); await w7At('aupay/main', D(-21)); await w7At('aupay/main', D(-21), 'breach');
  r = await run({ dryRun: true, config: recTo30 }); w = w8(r, 'aupay/main');
  assert.deepEqual([w.verdict, w.observed.samples, w.observed.excluded, w.observed.evidence_days], ['pass', 6, [D(-22), D(-29)].map((d) => `${d.slice(5)}:unverified`), [D(-14), D(-7)]]);
  //   証跡だけで 2 日 (突合済みの範囲を全部の標本日より前に) → 2 < 4 で blocked (理由に unverified)
  const recTo60 = auMall((x) => ({ ...x, reconciledThrough: D(-60) }));
  r = await run({ dryRun: true, config: recTo60 }); w = w8(r, 'aupay/main');
  assert.deepEqual([w.verdict, w.observed.samples, /有効標本 2 < 4 \(除外 6: .* unverified/.test(w.reason)], ['blocked', 2, true]);
  //   世代: snapshot の後に証跡 (W7 の記録) だけが増えた → 指紋が変わり再評価 (attempts 2)。再評価の結果は増えた証跡を含む (D(-29) が採用 = 標本 7)
  r = await run({ dryRun: true, config: recTo30, hooks: { afterSnapshot: async (n) => { if (n === 1) await w7At('aupay/main', D(-28)); } } }); w = w8(r, 'aupay/main');
  assert.deepEqual([r.attempts, w.observed.samples, w.observed.excluded, w.observed.evidence_days], [2, 7, [`${D(-22).slice(5)}:unverified`], [D(-28), D(-14), D(-7)]]);
  //   同じ started_at の 2 回 (Codex R4 Low): watch_result_id の大きい方 = 後に書いた回が勝つ (D(-35): pass → breach = 不採用 / D(-42): breach → pass = 採用)
  await w7At('aupay/main', D(-35), 'pass', `${D(-35)}T22:00:00Z`); await w7At('aupay/main', D(-35), 'breach', `${D(-35)}T22:00:00Z`);
  await w7At('aupay/main', D(-42), 'breach', `${D(-42)}T22:00:00Z`); await w7At('aupay/main', D(-42), 'pass', `${D(-42)}T22:00:00Z`);
  r = await run({ dryRun: true, config: recTo60 }); w = w8(r, 'aupay/main');
  assert.deepEqual([w.verdict, w.observed.samples, w.observed.evidence_days], ['pass', 4, [D(-42), D(-28), D(-14), D(-7)]]);
  await pg.query(`delete from ops.ingest_runs where entity = 'orders' and ingest_run_id like 't_orders_%'`);
  await pg.query(`delete from ops.watch_results where watch_run_id like 'wr_%'`); await pg.query(`delete from ops.watch_runs where watch_run_id like 'wr_%'`);
  //   境界 (証跡なし): reconciledThrough = D(-29) はその日を含む (標本 5。D(-30) なら 4 = 上) / ordersSince = D(-8) はその日を含む (標本 1)・D(-7) なら 0
  r = await run({ dryRun: true, config: auMall((x) => ({ ...x, reconciledThrough: D(-29) })) }); w = w8(r, 'aupay/main');
  assert.deepEqual([w.observed.samples, w.observed.excluded], [5, [D(-8), D(-15), D(-22)].map((d) => `${d.slice(5)}:unverified`)]);
  r = await run({ dryRun: true, config: auMall((x) => ({ ...x, ordersSince: D(-8) })) }); w = w8(r, 'aupay/main');
  assert.deepEqual([w.verdict, w.observed.samples, w.observed.baseline], ['blocked', 1, [`${D(-8).slice(5)}:39/390000/0.0513/0.1026`]]);
  r = await run({ dryRun: true, config: auMall((x) => ({ ...x, ordersSince: D(-7) })) }); w = w8(r, 'aupay/main');
  assert.deepEqual([w.verdict, w.observed.samples], ['blocked', 0]);
  // 有効標本 4 未満 = blocked (linegift の最初の注文が 3 週前 = それより前の日はゼロだが 突合済みの範囲の中 → 正当なゼロとして標本に入る = 8 のまま。ordersSince を 3 週前にすれば 3)
  await pg.query(`delete from core.orders where mall = 'linegift' and order_date_jst < $1::date`, [D(-22)]);
  r = await run({ dryRun: true }); w = w8(r, 'linegift/main');
  assert.deepEqual([w.verdict, w.observed.samples, w.observed.first_order_day, w.observed.stats.orders.median], ['pass', 8, D(-22), 0]);   // 5 日が 0 → 中央値 0 → 小規模
  const lgSince = { ...CONFIG, ORDER_MALLS: CONFIG.ORDER_MALLS.map((x) => (x.mall === 'linegift' ? { ...x, ordersSince: D(-22) } : x)) };
  r = await run({ dryRun: true, config: lgSince }); w = w8(r, 'linegift/main');
  assert.deepEqual([w.verdict, w.observed.samples, /有効標本 3 < 4/.test(w.reason), w.observed.excluded.length, w.observed.excluded.every((e) => e.endsWith(':unverified'))], ['blocked', 3, true, 5, true]);
  for (const d of baseDays.filter((d) => d < D(-22))) await ordersBulk('linegift', 'main', d, 10, 0);
  r = await run({ dryRun: true }); assert.equal(verdictOf(r, 'W8', 'linegift/main'), 'pass');
  // 昨日が未公開 (注文はある) → W9 の gap → W8 は前提で blocked
  const pubRun = (await one(`select run_id from mart.sales_daily_published where mall = 'amazon' and date_jst = $1::date`, [D(-1)])).run_id;
  await pg.query(`delete from mart.sales_daily_published where mall = 'amazon' and date_jst = $1::date`, [D(-1)]);
  r = await run({ dryRun: true }); w = w8(r, 'amazon/jp');
  assert.deepEqual([verdictOf(r, 'W9', 'amazon/jp'), w.verdict, w.blockedBy], ['breach', 'blocked', 'W9:amazon/jp']);
  await pg.query(`insert into mart.sales_daily_published (company_id, mall, scope_key, date_jst, run_id) values (1, 'amazon', 'jp', $1::date, $2)`, [D(-1), pubRun]);
  // 評価そのものにも守り (前提を外して直接呼んでも未公開で blocked)
  await pg.query(`delete from mart.sales_daily_published where mall = 'amazon' and date_jst = $1::date`, [D(-1)]);
  const direct = (await evalW8({ db, config: CONFIG, asOf: ASOF, now: NOW }, CONFIG.checkById('W8'))).find((x) => x.scopeKey === 'amazon/jp');
  assert.deepEqual([direct.verdict, /未公開/.test(direct.reason)], ['blocked', true]);
  await pg.query(`insert into mart.sales_daily_published (company_id, mall, scope_key, date_jst, run_id) values (1, 'amazon', 'jp', $1::date, $2)`, [D(-1), pubRun]);
  // 2 週間を過ぎれば warn
  r = await run({ dryRun: true, config: { ...CONFIG, W8_INFO_UNTIL: '2026-09-01' } });
  assert.equal(w8(r, 'aupay/main').severity, 'warn');
  // 世代: snapshot の後に平常の日 (D(-8)) の注文が増えた → 指紋が変わり再評価
  r = await run({ dryRun: true, hooks: { afterSnapshot: async (n) => { if (n === 1) await ordersBulk('aupay', 'main', D(-8), 1, 0, 900); } } });
  assert.equal(r.attempts, 2);
  await pg.query(`delete from core.orders where mall = 'aupay' and mall_order_no like 'w8-%-900'`);
  r = await run({ dryRun: true }); assert.equal(verdictOf(r, 'W8', 'aupay/main'), 'pass');
});

await t('🚨 W8 の祝日・年末年始: 昨日が祝日なら判定しない (9/22 のシルバーウィークを平日の火曜と比べた偽の異常) / 平常の日が祝日なら標本から外す / 一覧の期限切れ → blocked', async () => {
  // 本番の一覧 = 試験の昨日 9/22 は祝日 → W8 は全モール blocked (9/22 の amazon/jp の breach が出ない)
  let r = await run({ dryRun: true, config: { ...CONFIG, NON_BUSINESS_DAYS: REAL_CONFIG.NON_BUSINESS_DAYS } });
  assert.deepEqual(CONFIG.ORDER_MALLS.map((m) => verdictOf(r, 'W8', `${m.mall}/${m.scope}`)), ['blocked', 'blocked', 'blocked', 'blocked', 'blocked']);
  assert.match(resultOf(r, 'W8', 'amazon/jp').reason, /昨日 \(2026-09-22\) は祝日・年末年始 = 平日と比べない/);
  // 🚨 本番の一覧は W8 の過去 8 週 (7/28〜) をカバーする = 8/11 (山の日) が標本から外れる (Codex #1425 R1)
  assert.ok(resultOf(r, 'W8', 'rakuten/main').observed.excluded.includes('08-11:non_business_day'));
  assert.ok(['2026-07-20', '2026-08-11'].every((d) => REAL_CONFIG.NON_BUSINESS_DAYS.includes(d)));
  // 平常の日 (D(-8)・D(-15)) が祝日 → 標本から外す (楽天は 8 → 6)
  const base = resultOf(await run({ dryRun: true }), 'W8', 'rakuten/main').observed.samples;
  r = await run({ dryRun: true, config: { ...CONFIG, NON_BUSINESS_DAYS: [D(-8), D(-15)] } });
  const x = resultOf(r, 'W8', 'rakuten/main');
  assert.deepEqual([base, x.verdict, x.observed.samples, x.observed.excluded.filter((e) => e.endsWith(':non_business_day'))], [8, 'pass', 6, [`${D(-8).slice(5)}:non_business_day`, `${D(-15).slice(5)}:non_business_day`]]);
  // 祝日を外して有効標本が 4 → 3 = blocked (境目)
  r = await run({ dryRun: true, config: { ...CONFIG, NON_BUSINESS_DAYS: [D(-8), D(-15), D(-22), D(-29)] } });
  assert.deepEqual([verdictOf(r, 'W8', 'rakuten/main'), resultOf(r, 'W8', 'rakuten/main').sampleSize], ['pass', 4]);
  r = await run({ dryRun: true, config: { ...CONFIG, NON_BUSINESS_DAYS: [D(-8), D(-15), D(-22), D(-29), D(-36)] } });
  assert.deepEqual([verdictOf(r, 'W8', 'rakuten/main'), resultOf(r, 'W8', 'rakuten/main').sampleSize, /有効標本 3 < 4/.test(resultOf(r, 'W8', 'rakuten/main').reason)], ['blocked', 3, true]);
  // 一覧の期限: 当日 (昨日 = 期限) は判定する / 翌日から blocked
  r = await run({ dryRun: true, config: { ...CONFIG, NON_BUSINESS_DAYS_UNTIL: D(-1) } });
  assert.equal(verdictOf(r, 'W8', 'rakuten/main'), 'pass');
  r = await run({ dryRun: true, config: { ...CONFIG, NON_BUSINESS_DAYS_UNTIL: D(-2) } });
  assert.deepEqual([verdictOf(r, 'W8', 'rakuten/main'), /祝日の一覧 .* までしか無い/.test(resultOf(r, 'W8', 'rakuten/main').reason)], ['blocked', true]);
});

console.log('W7 / W9: 証跡と Render の run');
await t('🚨 W7: 証跡が無い → blocked / 見送り (not_backfilled) → blocked / 変更ゼロ → pass (契約) / 送ったのに Render に run が無い → breach / run が partial (complete=true でも) → breach / failed・stale・整形できない → breach / push が落ちた → breach', async () => {
  const E = goodEvidence();
  delete E['orders-qoo10'];
  E['orders-aupay'] = ev('aupay', 'main', { ok: null, skipped: 'not_backfilled', changed: undefined });
  E['orders-rakuten'] = ev('rakuten', 'main', { changed: 12, applied: 12, run_id: 'run_missing' });
  await orderRun('run_partial', { status: 'partial', complete: true });
  E['orders-amazon'] = ev('amazon', 'jp', { changed: 5, applied: 4, failed: 0, run_id: 'run_partial' });
  E['orders-linegift'] = ev('linegift', 'main', { changed: 3, applied: 2, failed: 1, run_id: 'run_ok' });
  await orderRun('run_ok');
  const r = await run({ dryRun: true, evidence: E });
  const v = (m) => [verdictOf(r, 'W7', m), (resultOf(r, 'W7', m).reason || '').slice(0, 40)];
  assert.deepEqual(v('qoo10/main')[0], 'blocked'); assert.match(v('qoo10/main')[1], /証跡が無い/);
  assert.deepEqual(v('aupay/main')[0], 'blocked'); assert.match(v('aupay/main')[1], /not_backfilled/);
  assert.deepEqual(v('rakuten/main')[0], 'breach'); assert.match(v('rakuten/main')[1], /run run_missing が無い/);
  assert.deepEqual(v('amazon/jp')[0], 'breach'); assert.match(resultOf(r, 'W7', 'amazon/jp').reason, /partial/);
  assert.deepEqual(v('linegift/main')[0], 'breach'); assert.match(resultOf(r, 'W7', 'linegift/main').reason, /failed 1/);
  // W9 は W7 に依存 = 全部 blocked (前提)
  assert.deepEqual(CONFIG.ORDER_MALLS.map((m) => verdictOf(r, 'W9', `${m.mall}/${m.scope}`)), ['blocked', 'blocked', 'blocked', 'blocked', 'blocked']);
  assert.deepEqual([r.counts.blocked, verdictOf(r, 'W6', 'all/jp'), resultOf(r, 'W6', 'all/jp').blockedBy], [18, 'blocked', 'W7:rakuten/main']);   // W9 5 + W6 + W8 5 + W11 5 (前提 W7) + W7 2
  assert.deepEqual(CONFIG.ORDER_MALLS.map((m) => verdictOf(r, 'W8', `${m.mall}/${m.scope}`)), ['blocked', 'blocked', 'blocked', 'blocked', 'blocked']);   // 前提の全部を見る = 評価順で最初に pass でなかった W7 (rakuten の breach) が理由
  const crashed = await run({ dryRun: true, evidence: { ...goodEvidence(), 'orders-qoo10': { kind: 'orders', mall: 'qoo10', scope: 'main', sync_run_id: SYNC, ok: false, error: 'DB が壊れている' } } });
  assert.deepEqual([verdictOf(crashed, 'W7', 'qoo10/main'), /push が落ちた/.test(resultOf(crashed, 'W7', 'qoo10/main').reason)], ['breach', true]);
  const good = await run({ dryRun: true, evidence: { ...goodEvidence(), 'orders-linegift': ev('linegift', 'main', { changed: 3, applied: 3, run_id: 'run_ok' }) } });
  assert.deepEqual([verdictOf(good, 'W7', 'linegift/main'), resultOf(good, 'W7', 'linegift/main').observed.run.status, resultOf(good, 'W7', 'linegift/main').inputGeneration.run_id], ['pass', 'success', 'run_ok']);
  await pg.query(`delete from ops.ingest_runs where ingest_run_id = 'run_partial'`);   // W10 が「回復していない partial」として後ろの試験で拾う = 片づける
});
await t('🚨 W7: 同じ実行 (sync_run_id) の証跡だけを採用する。別の回の証跡 → blocked / ID なし (手動の回) → blocked / mode が range → blocked / 記録する回で ID が無ければ止まる / dry-run で ID が無ければ「結びつけずに」今日の証跡を読む (observed.bound=false)', async () => {
  const E = goodEvidence();
  E['orders-rakuten'] = ev('rakuten', 'main', { sync_run_id: 'ds_20260922T050000' });   // 早朝の別の回 (上流が失敗して今朝は push を見送った、の形)
  E['orders-amazon'] = ev('amazon', 'jp', { sync_run_id: null });                       // 手で流した回
  E['orders-aupay'] = ev('aupay', 'main', { mode: 'range' });                            // 範囲を流した回 (走査を完了していない)
  const r = await run({ dryRun: true, evidence: E });
  const v = (m) => [verdictOf(r, 'W7', m), resultOf(r, 'W7', m).reason || ''];
  assert.deepEqual(v('rakuten/main')[0], 'blocked'); assert.match(v('rakuten/main')[1], /別の実行の証跡/);
  assert.deepEqual(v('amazon/jp')[0], 'blocked'); assert.match(v('amazon/jp')[1], /手動/);
  assert.deepEqual(v('aupay/main')[0], 'blocked'); assert.match(v('aupay/main')[1], /range/);
  assert.deepEqual([verdictOf(r, 'W7', 'linegift/main'), resultOf(r, 'W7', 'linegift/main').observed.bound, resultOf(r, 'W7', 'linegift/main').observed.sync_run_id], ['pass', true, SYNC]);
  assert.deepEqual([verdictOf(r, 'W9', 'rakuten/main'), verdictOf(r, 'W9', 'linegift/main')], ['blocked', 'pass']);   // 前提 W7 が blocked なら W9 も blocked
  // 記録する回に実行 ID が無い = 止まる (どの回の証跡か結びつけられない)
  await assert.rejects(run({ syncRunId: null }), /実行 ID/);
  // dry-run で ID が無い (人が手で確かめる) = 今日の証跡を結びつけずに読む。手動の回 (ID なし) の証跡も読める
  const u = await run({ dryRun: true, syncRunId: null, evidence: E });
  assert.deepEqual([verdictOf(u, 'W7', 'rakuten/main'), verdictOf(u, 'W7', 'amazon/jp'), verdictOf(u, 'W7', 'aupay/main'), resultOf(u, 'W7', 'rakuten/main').observed.bound], ['pass', 'pass', 'blocked', false]);
  // 記録する回で ID があれば、ID の無い証跡は採用しない (dry-run と違う)
  const b = await run({ dryRun: true, evidence: { ...goodEvidence(), 'orders-qoo10': ev('qoo10', 'main', { sync_run_id: null }) } });
  assert.equal(verdictOf(b, 'W7', 'qoo10/main'), 'blocked');
});
await t('🚨 W9: 回 (session) が開いたまま → breach / 変わった注文を送ったのに watermark が古い → breach / 注文があるのに公開されていない日 → breach / 注文ゼロの日は公開行が無くても pass / 作り直しに失敗 → breach / 状態が無い → blocked', async () => {
  await order('rakuten', 'main', D(-1), 'r1'); await order('rakuten', 'main', D(-2), 'r2');
  await published('rakuten', 'main', D(-1)); await published('rakuten', 'main', D(-2));   // D(-3) は注文ゼロ = 公開行なしが正
  let r = await run({ dryRun: true });
  assert.deepEqual([verdictOf(r, 'W9', 'rakuten/main'), resultOf(r, 'W9', 'rakuten/main').observed.days], ['pass', [`${D(-3).slice(5)}:zero`, `${D(-2).slice(5)}:pub`, `${D(-1).slice(5)}:pub`]]);
  await order('rakuten', 'main', D(-3), 'r3');   // 注文があるのに公開されていない
  r = await run({ dryRun: true });
  assert.deepEqual([verdictOf(r, 'W9', 'rakuten/main'), resultOf(r, 'W9', 'rakuten/main').observed.gaps], ['breach', [D(-3)]]);
  await published('rakuten', 'main', D(-3));
  await salesState('rakuten', 'main', { sessionId: 'sess-open' });
  r = await run({ dryRun: true });
  assert.deepEqual([verdictOf(r, 'W9', 'rakuten/main'), /開いたまま/.test(resultOf(r, 'W9', 'rakuten/main').reason)], ['breach', true]);
  await salesState('rakuten', 'main', { watermark: '2026-09-21T00:00:00Z' });   // 閉じているが古い
  r = await run({ dryRun: true, evidence: { ...goodEvidence(), 'orders-rakuten': ev('rakuten', 'main', { changed: 4, applied: 4, run_id: 'run_ok' }) } });
  assert.deepEqual([verdictOf(r, 'W9', 'rakuten/main'), /watermark が進んでいない/.test(resultOf(r, 'W9', 'rakuten/main').reason)], ['breach', true]);
  await salesState('rakuten', 'main', { watermark: '2026-09-22T22:00:00Z' });   // push の開始 22:05 の 5 分前 = 猶予 15 分の中
  r = await run({ dryRun: true, evidence: { ...goodEvidence(), 'orders-rakuten': ev('rakuten', 'main', { changed: 4, applied: 4, run_id: 'run_ok' }) } });
  assert.equal(verdictOf(r, 'W9', 'rakuten/main'), 'pass');
  r = await run({ dryRun: true, evidence: { ...goodEvidence(), 'orders-rakuten': ev('rakuten', 'main', { sales: { ok: false, error: 'timeout' } }) } });
  assert.deepEqual([verdictOf(r, 'W9', 'rakuten/main'), /作り直しに失敗/.test(resultOf(r, 'W9', 'rakuten/main').reason)], ['breach', true]);
  await pg.query(`delete from mart.sales_daily_state where mall = 'qoo10'`);
  r = await run({ dryRun: true });
  assert.deepEqual([verdictOf(r, 'W9', 'qoo10/main'), /状態が無い/.test(resultOf(r, 'W9', 'qoo10/main').reason)], ['blocked', true]);
  await salesState('qoo10', 'main');
});

console.log('W10: 回復していない取込の異常');
/** chunk で送る取込の run (checksum = batch_seq) と chunk 0 の応答 (result.failed = 失敗した行)。ops.ingest_chunks は追記専用 = 後片づけは run を success にして外す */
async function chunkRun(runId, source, entity, scope, { status = 'partial', batch = 5, startedAt = '2026-09-22T21:00:00Z', failed = [], failedRows = failed.length } = {}) {
  await pg.query(`insert into ops.ingest_runs (ingest_run_id, source_system, entity, scope_key, host, started_at, finished_at, status, complete, rows_seen, source_tz, checksum)
    values ($1, $2, $3, $4, 'test', $5::timestamptz, case when $6::text = 'running' then null else $5::timestamptz end, $6::text, $6::text <> 'running', $7, 'Asia/Tokyo', $8)`, [runId, source, entity, scope, startedAt, status, failedRows + 1, batch == null ? null : String(batch)]);
  await pg.query(`insert into ops.ingest_chunks (ingest_run_id, chunk_index, payload_checksum, rows_seen, rows_applied, rows_failed, result) values ($1, 0, 'x', $2, 1, $3, $4::jsonb)`, [runId, failedRows + 1, failedRows, JSON.stringify({ applied: 1, failed })]);
}
const plainRun = (runId, source, entity, scope, status, { startedAt = '2026-09-22T21:00:00Z', checksum = null } = {}) => pg.query(`insert into ops.ingest_runs (ingest_run_id, source_system, entity, scope_key, host, started_at, finished_at, status, complete, rows_seen, source_tz, checksum)
  values ($1, $2, $3, $4, 'test', $5::timestamptz, $5::timestamptz, $6::text, $6::text = 'success', 1, 'UTC', $7)`, [runId, source, entity, scope, startedAt, status, checksum]);
const setSeq = (no, seq) => pg.query(`update core.orders set received_batch_seq = $2 where mall = 'rakuten' and mall_order_no = $1`, [no, seq]);
const w10 = (r, scope) => resultOf(r, 'W10', scope);
const done10 = (...ids) => pg.query(`update ops.ingest_runs set status = 'success', complete = true where ingest_run_id = any($1::text[])`, [ids]);
// 今朝の差分送信の証跡 (見本の goodEvidence は 22:05Z に始まった変更ゼロの pass = 21:00Z に始まった悪い run の「後」の証明になる)
const otherSync = (mall, scope) => ev(mall, scope, { sync_run_id: 'ds_other' });   // 別の実行の証跡 = W7 は blocked = 証明にならない
await t('🚨 W10 (注文 partial): 失敗した行が 1 行ずつ「正規の差分送信の世代」(今朝の証跡・記録した履歴) で run より新しく当たるまで未回復 (同じ世代・行が無い・出どころの分からない世代 は残る)。鍵だけの応答も読む。一度証明した回復は翌朝の送信の成否で戻らない。案件は種類ごとに 1 つ。最初の 2 週間は info', async () => {
  for (const no of ['w10-a', 'w10-b', 'w10-c']) await order('rakuten', 'main', D(-2), no);   // received_batch_seq = 1 (D(-2) は公開済み = W9 に触らない)
  await chunkRun('w10_p1', 'rakuten', 'orders', 'main', { batch: 5, failed: [{ key: 'rakuten|main|w10-a', mall_order_no: 'w10-a', error: 'x' }, { key: 'rakuten|main|w10-b', mall_order_no: 'w10-b', error: 'y' }, { key: 'rakuten|main|w10-c', error: 'z' }, { key: 'rakuten|main|w10-none', mall_order_no: 'w10-none', error: 'w' }] });
  let r = await run();   // 見本の証跡は変更ゼロ = 世代を使っていない = 信頼できる世代は無い
  let x = w10(r, 'rakuten.orders/main');
  assert.deepEqual([x.verdict, x.severity, x.items.length, x.items[0].subjectKey, x.items[0].payload.failed_keys, x.items[0].payload.remaining_keys, x.periodFrom, r.counts.new, x.observed.todays_push.trusted], ['breach', 'info', 1, 'w10_p1', 4, 4, '2026-09-23', 1, false]);
  assert.match(x.reason, /w10_p1 partial: 正規の差分送信で送り直されていない行 4 \/ 4/);
  // 今朝の差分送信 (世代 6) が a・c を送り直した。b は別の送り手が世代 9 に進めた (出どころの分からない世代 = Codex R2 #2)。none は行が無い
  await chunkRun('w10_push6', 'rakuten', 'orders', 'main', { status: 'success', batch: 6 });
  await setSeq('w10-a', 6); await setSeq('w10-c', 6); await setSeq('w10-b', 9);
  r = await run({ now: new Date('2026-09-23T02:00:00Z'), evidence: { ...goodEvidence(), 'orders-rakuten': ev('rakuten', 'main', { changed: 2, applied: 2, batch_seq: 6, run_id: 'w10_push6' }) } });
  x = w10(r, 'rakuten.orders/main');
  assert.deepEqual([verdictOf(r, 'W7', 'rakuten/main'), x.verdict, x.items[0].payload.remaining_keys, [...x.items[0].payload.remaining_sample].sort(), r.counts.continued, r.counts.new, x.observed.todays_push.trusted], ['pass', 'breach', 2, ['rakuten|main|w10-b', 'rakuten|main|w10-none'], 1, 0, true]);
  // 次の差分送信 (世代 7) が b・none を送り直した。a・c の世代 6 は記録した履歴 (信頼できる世代) に残っている
  await chunkRun('w10_push7', 'rakuten', 'orders', 'main', { status: 'success', batch: 7 });
  await setSeq('w10-b', 7); await order('rakuten', 'main', D(-2), 'w10-none'); await setSeq('w10-none', 7);
  r = await run({ now: new Date('2026-09-23T03:00:00Z'), evidence: { ...goodEvidence(), 'orders-rakuten': ev('rakuten', 'main', { changed: 2, applied: 2, batch_seq: 7, run_id: 'w10_push7' }) } });
  assert.deepEqual([verdictOf(r, 'W10', 'rakuten.orders/main'), r.counts.recovered, w10(r, 'rakuten.orders/main').observed.proven], ['pass', 1, ['w10_p1']]);
  assert.equal((await one(`select state from ops.watch_issues where check_id = 'W10' and scope_key = 'rakuten.orders/main'`)).state, 'recovered');
  // 🚨 Codex R2 #3: 翌朝の送信が確かめられなくても (別の実行の証跡)・a の世代が後から出どころの分からない世代に変わっても、証明した回復は戻らない
  await setSeq('w10-a', 99);
  r = await run({ now: new Date('2026-09-23T04:00:00Z'), evidence: { ...goodEvidence(), 'orders-rakuten': otherSync('rakuten', 'main') } });
  assert.deepEqual([verdictOf(r, 'W7', 'rakuten/main'), verdictOf(r, 'W10', 'rakuten.orders/main'), w10(r, 'rakuten.orders/main').observed.proven, r.counts.new], ['blocked', 'pass', ['w10_p1'], 0]);
  // 2 週間を過ぎれば error (dry-run で期限を前に)。a は世代 99 (出どころが分からない) = 当たっていない
  await chunkRun('w10_p2', 'rakuten', 'orders', 'main', { batch: 8, failed: [{ key: 'rakuten|main|w10-a', mall_order_no: 'w10-a', error: 'x' }] });
  r = await run({ dryRun: true, config: { ...CONFIG, W10_INFO_UNTIL: '2026-09-01' } });
  assert.deepEqual([verdictOf(r, 'W10', 'rakuten.orders/main'), w10(r, 'rakuten.orders/main').severity, w10(r, 'rakuten.orders/main').items.map((i) => i.subjectKey)], ['breach', 'error', ['w10_p2']]);
  // 🚨 Codex R1 Low: 鍵の読めない失敗が 1 つでもあれば回復にしない
  await chunkRun('w10_p3', 'rakuten', 'orders', 'main', { batch: 8, startedAt: '2026-09-22T21:30:00Z', failed: [{ key: 'rakuten|main|w10-c', mall_order_no: 'w10-c', error: 'x' }, { error: '鍵なし' }] });
  r = await run({ dryRun: true });
  assert.deepEqual([w10(r, 'rakuten.orders/main').items.map((i) => i.subjectKey), /w10_p3 partial: 失敗 2 行のうち鍵が読めない 1/.test(w10(r, 'rakuten.orders/main').reason)], [['w10_p2', 'w10_p3'], true]);
  await done10('w10_p1', 'w10_p2', 'w10_p3');
  await pg.query(`delete from ops.watch_issues`);
});
await t('🚨 W10 (止まった running): 送れなかった行は Render から見えない = 後に閉じた run・今朝の差分送信の pass でも自動では回復にしない (Codex R1 #1 / R2 #1)。突合で確かめて受け入れる (run ごと / 種類 × 時刻より前) / 2 時間以内は数えない / 失敗の行数があるのに鍵が読めない → 未回復', async () => {
  await chunkRun('w10_s1', 'amazon', 'orders', 'jp', { status: 'running', batch: 10, startedAt: '2026-09-22T21:00:00Z' });   // 3.5 時間前から running (= 06:00 JST)
  await chunkRun('w10_s2', 'amazon', 'orders', 'jp', { status: 'running', batch: 11, startedAt: '2026-09-23T00:00:00Z' });   // 30 分前 = まだ止まったとは言わない
  await chunkRun('w10_range', 'amazon', 'orders', 'jp', { status: 'success', batch: 12, startedAt: '2026-09-22T23:00:00Z' });   // 後に閉じた run
  await chunkRun('w10_u', 'aupay', 'orders', 'main', { batch: 3, failed: [], failedRows: 2 });                                   // 失敗 2 行・鍵なし (応答の形が違う)
  let r = await run({ dryRun: true });   // 今朝の差分送信は pass
  assert.deepEqual([verdictOf(r, 'W7', 'amazon/jp'), verdictOf(r, 'W10', 'amazon.orders/jp'), w10(r, 'amazon.orders/jp').items.map((i) => i.subjectKey)], ['pass', 'breach', ['w10_s1']]);
  assert.match(w10(r, 'amazon.orders/jp').reason, /止まった run = 送れなかった行は Render から見えない/);
  assert.deepEqual([verdictOf(r, 'W10', 'aupay.orders/main'), /失敗 2 行のうち鍵が読めない 2/.test(w10(r, 'aupay.orders/main').reason)], ['breach', true]);
  // 受け入れ = 種類 × 突合した時刻より前 (06:30 JST)。それより後に始まった run は数える
  const acc = (startedBefore) => ({ ...CONFIG, W10_ACCEPTED_RUNS: [{ kind: 'amazon.orders/jp', startedBefore, reason: '試験', owner: '試験' }] });
  r = await run({ dryRun: true, config: acc('2026-09-23T06:30:00+09:00') });
  assert.deepEqual([verdictOf(r, 'W10', 'amazon.orders/jp'), w10(r, 'other/*').observed.skipped_accepted], ['pass', ['w10_s1']]);
  r = await run({ dryRun: true, config: acc('2026-09-23T05:59:59+09:00') });
  assert.equal(verdictOf(r, 'W10', 'amazon.orders/jp'), 'breach');
  await done10('w10_s1', 'w10_s2', 'w10_u');
});
await t('🚨 W10 (今朝の run と出荷): 今朝の push の run は W7 が判定したとき (pass / breach) だけ W7 に任せる。W7 が blocked (別の実行の証跡) なら W10 が数える (Codex R1 #3)。出荷は証跡 shipments の世代と伝票の世代で見る', async () => {
  await chunkRun('w10_today', 'qoo10', 'orders', 'main', { batch: 2, startedAt: '2026-09-22T22:05:00Z', failed: [{ key: 'qoo10|main|q1', mall_order_no: 'q1', error: 'e' }] });
  let r = await run({ dryRun: true, evidence: { ...goodEvidence(), 'orders-qoo10': ev('qoo10', 'main', { changed: 1, applied: 0, failed: 1, run_id: 'w10_today' }) } });
  assert.deepEqual([verdictOf(r, 'W7', 'qoo10/main'), verdictOf(r, 'W10', 'qoo10.orders/main'), w10(r, 'other/*').observed.skipped_todays_push], ['breach', 'pass', ['w10_today']]);   // 今朝の失敗は W7 だけが出す
  r = await run({ dryRun: true, evidence: { ...goodEvidence(), 'orders-qoo10': ev('qoo10', 'main', { sync_run_id: 'ds_other', changed: 1, applied: 0, failed: 1, run_id: 'w10_today' }) } });
  assert.deepEqual([verdictOf(r, 'W7', 'qoo10/main'), verdictOf(r, 'W10', 'qoo10.orders/main'), w10(r, 'other/*').observed.skipped_todays_push], ['blocked', 'breach', []]);
  // 出荷: 伝票が無い・証跡が無い → 未回復 / 差分送信 (shipments・世代 5) が伝票を送り直した → 回復
  await chunkRun('w10_ship', 'ne', 'shipments', 'main', { batch: 4, failed: [{ key: 'S-1', ne_slip_no: 'S-1', error: 'e' }] });
  r = await run({ dryRun: true });
  assert.deepEqual([verdictOf(r, 'W10', 'ne.shipments/main'), w10(r, 'ne.shipments/main').items[0].payload.remaining_keys, w10(r, 'ne.shipments/main').observed.todays_push.trusted], ['breach', 1, false]);   // 今朝の出荷の push は変更ゼロ = 世代を使っていない
  await chunkRun('w10_ship5', 'ne', 'shipments', 'main', { status: 'success', batch: 5, startedAt: '2026-09-22T22:10:00Z' });
  await pg.query(`insert into core.shipments (company_id, ne_slip_no, status, received_batch_seq, source_updated_at, transform_version, content_hash) values (1, 'S-1', 'new', 5, now(), 'v1', 'h')`);
  const shipEv = { kind: 'shipments', scope: 'main', mode: 'incremental', sync_run_id: SYNC, ok: true, push_ok: true, locked: false, run_id: 'w10_ship5', batch_seq: 5, started_at: '2026-09-22T22:10:00Z', changed: 1, applied: 1, same: 0, failed: 0, stale: 0, transform_errors: 0 };
  r = await run({ dryRun: true, evidence: { ...goodEvidence(), shipments: shipEv } });
  assert.deepEqual([verdictOf(r, 'W10', 'ne.shipments/main'), w10(r, 'ne.shipments/main').observed.todays_push.trusted, w10(r, 'ne.shipments/main').observed.proven], ['pass', true, ['w10_ship']]);
  await done10('w10_today', 'w10_ship');
});
await t('🚨 W10 (在庫の日次): run が指す日が W2 の窓から外れても partial のままなら未回復 (Codex R1 #4)。complete に上がれば回復・窓の中は W1 / W2 に任せる・since より前は数えない・fba_us は例外の期限の中だけ許す', async () => {
  // fba_jp の since を前に (見本の既定 9/22 のままだと窓の外の日は全部 since より前)
  const cfg = (over = {}) => ({ ...CONFIG, STOCK_SCOPES: CONFIG.STOCK_SCOPES.map((s) => (s.source === 'fba_jp' ? { ...s, since: '2026-09-01' } : s.source === 'fba_us' ? { ...s, since: '2026-09-01', ...over } : s)) });
  const partialRun = async (day, source, scope) => { const id = await capture(day, source, scope, 'partial'); await pg.query(`update ops.ingest_runs set status = 'partial', complete = false, started_at = '2026-09-22T21:00:00Z' where ingest_run_id = $1`, [id]); return id; };
  const old = await partialRun(D(-10), 'fba_jp', 'jp');     // 窓 (D(-7)〜) の外
  const inWin = await partialRun(D(-9), 'fba_us', 'us');    // fba_us も窓の外。例外 (〜12/31) の中
  let r = await run({ dryRun: true, config: cfg() });
  const jp = w10(r, 'amazon.stock_daily/jp');
  assert.deepEqual([jp.verdict, jp.items.map((i) => i.subjectKey), /2026-09-13 が partial のまま/.test(jp.reason), verdictOf(r, 'W10', 'amazon.stock_daily/us')], ['breach', [old], true, 'pass']);
  r = await run({ dryRun: true });   // since = 9/22 (既定) なら数えない
  assert.deepEqual([verdictOf(r, 'W10', 'amazon.stock_daily/jp'), w10(r, 'amazon.stock_daily/jp').observed.not_counted], ['pass', [`${old}:before_since`]]);
  r = await run({ dryRun: true, config: cfg({ allowPartial: { reason: '試験', owner: '試験', until: '2026-09-01' } }) });   // 例外の期限切れ
  assert.deepEqual([verdictOf(r, 'W10', 'amazon.stock_daily/us'), /例外の期限切れ/.test(w10(r, 'amazon.stock_daily/us').reason)], ['breach', true]);
  // 窓の中の日 (D(-3)) の partial は W2 が見る = W10 は数えない
  await delCapture(D(-3), 'fba_jp'); const win = await partialRun(D(-3), 'fba_jp', 'jp');
  r = await run({ dryRun: true, config: cfg() });
  assert.deepEqual([verdictOf(r, 'W2', 'fba_jp/jp'), w10(r, 'amazon.stock_daily/jp').items.map((i) => i.subjectKey), w10(r, 'amazon.stock_daily/jp').observed.not_counted], ['breach', [old], [`${win}:w1_w2`]]);
  // 回復: D(-10) が complete に上がった (新しい run に差し替わる = 古い run はどの日も指さない)
  await delCapture(D(-10), 'fba_jp'); await capture(D(-10), 'fba_jp', 'jp', 'complete');
  await delCapture(D(-3), 'fba_jp'); await capture(D(-3), 'fba_jp', 'jp', 'complete');
  r = await run({ dryRun: true, config: cfg() });
  assert.deepEqual([verdictOf(r, 'W10', 'amazon.stock_daily/jp'), w10(r, 'amazon.stock_daily/jp').observed.recovered], ['pass', 2]);
  await delCapture(D(-9), 'fba_us'); await delCapture(D(-10), 'fba_jp');
  await done10(old, inWin, win);
});
await t('W10 (ロジザード・定義に無い種類): 在庫の失敗は同じか新しい世代の success で回復 / 定義に無い種類は other/* で異常 / 見ない種類 (夜間ロード) は数えない / 受け入れた run・since より前の run は数えない (JST の日の境界)', async () => {
  // 見本の在庫の run (capture) は 9/30 の世代まである = それより新しい世代で失敗させる
  await plainRun('w10_lz', 'logizard', 'inventory', 'main', 'failed', { checksum: '2026-10-05T00:00:00.000Z' });
  await plainRun('w10_yahoo', 'yahoo', 'orders', 'main', 'partial');
  await plainRun('w10_yahoo_edge', 'yahoo', 'orders', 'main', 'partial', { startedAt: '2026-09-15T15:00:00Z' });   // = 9/16 00:00 JST (since の日の始まり = 数える)
  await plainRun('w10_yahoo_old', 'yahoo', 'orders', 'main', 'partial', { startedAt: '2026-09-15T14:59:59Z' });    // = 9/15 23:59:59 JST (数えない)
  await plainRun('w10_load', 'sqlite_initial_load', 'products', 'render', 'partial');
  let r = await run({ dryRun: true });
  assert.deepEqual([verdictOf(r, 'W10', 'logizard.inventory/main'), /同じか新しい世代の success が無い/.test(w10(r, 'logizard.inventory/main').reason)], ['breach', true]);
  const o = w10(r, 'other/*');
  assert.deepEqual([o.verdict, o.items.map((i) => i.subjectKey), o.observed.skipped_delegated, /W10_KINDS か W10_DELEGATED に足す/.test(o.reason)], ['breach', ['w10_yahoo_edge', 'w10_yahoo'], 1, true]);
  await plainRun('w10_lz_ok', 'logizard', 'inventory', 'main', 'success', { checksum: '2026-10-05T01:00:00.000Z' });
  r = await run({ dryRun: true, config: { ...CONFIG, W10_ACCEPTED_RUNS: [{ runId: 'w10_yahoo', reason: '試験', owner: '試験' }, { runId: 'w10_yahoo_edge', reason: '試験', owner: '試験' }] } });
  assert.deepEqual([verdictOf(r, 'W10', 'logizard.inventory/main'), verdictOf(r, 'W10', 'other/*'), w10(r, 'other/*').observed.skipped_accepted], ['pass', 'pass', ['w10_yahoo_edge', 'w10_yahoo']]);
  await done10('w10_yahoo', 'w10_yahoo_edge', 'w10_yahoo_old', 'w10_load');
});
await t('🚨 W10 の世代の指紋: 評価の後に失敗した行が取り直された (run の数も時刻も変わらない) / 止まった run に途中 chunk が届いた のを見つけて再評価する', async () => {
  await order('rakuten', 'main', D(-2), 'w10-g');
  await chunkRun('w10_g', 'rakuten', 'orders', 'main', { batch: 20, failed: [{ key: 'rakuten|main|w10-g', mall_order_no: 'w10-g', error: 'x' }] });
  await chunkRun('w10_push21', 'rakuten', 'orders', 'main', { status: 'success', batch: 21, startedAt: '2026-09-22T22:05:00Z' });
  const E21 = { ...goodEvidence(), 'orders-rakuten': ev('rakuten', 'main', { changed: 1, applied: 1, batch_seq: 21, run_id: 'w10_push21' }) };
  let r = await run({ dryRun: true, evidence: E21, hooks: { afterSnapshot: async (n) => { if (n === 1) await setSeq('w10-g', 21); } } });
  assert.deepEqual([r.attempts, verdictOf(r, 'W10', 'rakuten.orders/main')], [2, 'pass']);
  await chunkRun('w10_h', 'aupay', 'orders', 'main', { status: 'running', batch: 30 });
  r = await run({ dryRun: true, hooks: { afterSnapshot: async (n) => { if (n === 1) await pg.query(`insert into ops.ingest_chunks (ingest_run_id, chunk_index, payload_checksum, rows_seen, rows_applied, rows_failed, result) values ('w10_h', 1, 'y', 1, 0, 1, '{"failed":[{"key":"aupay|main|zz","mall_order_no":"zz","error":"e"}]}'::jsonb)`); } } });
  assert.deepEqual([r.attempts, w10(r, 'aupay.orders/main').items[0].payload.remaining_keys], [2, 1]);
  await pg.query(`update ops.ingest_runs set status = 'success', complete = true where ingest_run_id like 'w10\\_%'`);
});

await t('🚨 W10 (Codex R3 #1): 自動 retry が送り直した世代は見張りが記録していなくても、過去の日の証跡 (miniPC に 14 日) から信頼できる世代として拾う。手で流した回 (実行 ID なし)・範囲送信の証跡は拾わない', async () => {
  await order('rakuten', 'main', D(-2), 'w10-r');
  await chunkRun('w10_rt', 'rakuten', 'orders', 'main', { batch: 40, startedAt: '2026-09-21T22:05:00Z', failed: [{ key: 'rakuten|main|w10-r', mall_order_no: 'w10-r', error: 'x' }] });
  await setSeq('w10-r', 41);   // 昨日 8:30 の retry (世代 41) が送り直して当たった。見張りは retry の後に流れていない
  const past = (extra) => ({ [D(-1)]: { 'orders-rakuten': ev('rakuten', 'main', { sync_run_id: 'ds_prev', changed: 1, applied: 1, batch_seq: 41, run_id: 'w10_retry41', ...extra }) } });
  let r = await run({ dryRun: true });
  assert.equal(verdictOf(r, 'W10', 'rakuten.orders/main'), 'breach');   // 今朝の証跡は変更ゼロ = 世代 41 を知らない
  r = await run({ dryRun: true, evidenceHistory: past({}) });
  assert.deepEqual([verdictOf(r, 'W10', 'rakuten.orders/main'), w10(r, 'rakuten.orders/main').observed.proven.includes('w10_rt')], ['pass', true]);
  r = await run({ dryRun: true, evidenceHistory: past({ sync_run_id: null }) });   // 手で流した回
  assert.equal(verdictOf(r, 'W10', 'rakuten.orders/main'), 'breach');
  r = await run({ dryRun: true, evidenceHistory: past({ mode: 'range' }) });       // 範囲送信
  assert.equal(verdictOf(r, 'W10', 'rakuten.orders/main'), 'breach');
  await done10('w10_rt');
});
await t('🚨 W10 (Codex R3 #2 / Low): 信頼できる世代と証明した回復は、最後の記録から引き継ぐ (見張りの古い記録が整理されても残る・まだ証明できていない run の世代より古い世代は捨てる)。監視の範囲 (W10_SINCE) を動かして戻しても証明は残る', async () => {
  for (const no of ['w10-m1', 'w10-m2']) await order('rakuten', 'main', D(-2), no);
  await chunkRun('w10_m', 'rakuten', 'orders', 'main', { batch: 60, failed: [{ key: 'rakuten|main|w10-m1', mall_order_no: 'w10-m1', error: 'x' }, { key: 'rakuten|main|w10-m2', mall_order_no: 'w10-m2', error: 'y' }] });
  await chunkRun('w10_push61', 'rakuten', 'orders', 'main', { status: 'success', batch: 61, startedAt: '2026-09-22T22:05:00Z' });
  await setSeq('w10-m1', 61);   // m1 だけ世代 61 で当たった。m2 はまだ
  let r = await run({ now: new Date('2026-09-23T05:00:00Z'), evidence: { ...goodEvidence(), 'orders-rakuten': ev('rakuten', 'main', { changed: 1, applied: 1, batch_seq: 61, run_id: 'w10_push61' }) } });
  let x = w10(r, 'rakuten.orders/main');
  assert.deepEqual([x.verdict, x.items[0].payload.remaining_keys, x.observed.trusted_batches.includes(61), x.observed.trusted_batches.every((b) => b > 60)], ['breach', 1, true, true]);
  // 古い記録の整理 (13 か月) を模す: 世代 61 を記録した結果の todays_push を消しても、引き継いだ一覧 (trusted_batches) に残っている
  r = await run({ now: new Date('2026-09-23T05:10:00Z') });   // 今朝の証跡は変更ゼロ (世代なし)
  assert.deepEqual([w10(r, 'rakuten.orders/main').observed.trusted_batches.includes(61)], [true]);
  await pg.query(`update ops.watch_results set observed = observed - 'todays_push' where check_id = 'W10' and scope_key = 'rakuten.orders/main'`);
  // m2 を世代 62 の差分送信が送り直した → m1 (世代 61 = 引き継いだ一覧) と合わせて回復
  await chunkRun('w10_push62', 'rakuten', 'orders', 'main', { status: 'success', batch: 62, startedAt: '2026-09-22T22:05:00Z' });
  await setSeq('w10-m2', 62);
  r = await run({ now: new Date('2026-09-23T05:20:00Z'), evidence: { ...goodEvidence(), 'orders-rakuten': ev('rakuten', 'main', { changed: 1, applied: 1, batch_seq: 62, run_id: 'w10_push62' }) } });
  x = w10(r, 'rakuten.orders/main');
  assert.deepEqual([x.verdict, x.observed.proven.includes('w10_m'), x.observed.trusted_batches], ['pass', true, []]);   // 証明できていない run が無い = 世代の一覧は空でよい
  // 監視の範囲を先へ動かして記録 → 戻す: 証明は残る (m1 の世代が後から出どころの分からない世代に変わっても回復のまま)
  r = await run({ now: new Date('2026-09-23T05:30:00Z'), config: { ...CONFIG, W10_SINCE: '2026-09-30' } });
  assert.equal(w10(r, 'rakuten.orders/main').observed.proven.includes('w10_m'), true);
  await setSeq('w10-m1', 99);
  r = await run({ now: new Date('2026-09-23T05:40:00Z') });
  assert.deepEqual([verdictOf(r, 'W10', 'rakuten.orders/main'), w10(r, 'rakuten.orders/main').observed.proven.includes('w10_m')], ['pass', true]);
  await done10('w10_m');
  await pg.query(`delete from ops.watch_issues`);
});

console.log('W11: 注文と出荷の未リンク・発送遅れ');
// 日付は W8 の平常の日 (D(-8)・D(-15)…) と W9 の窓 (D(-3)〜D(-1)) を避ける。su = 状態が最後に変わった日 (既定は注文日)
const o11 = async (mall, scope, no, day, status, shop, { su = day, src = null } = {}) => (await one(`insert into core.orders (company_id, mall, scope_key, mall_order_no, source_system, shop_code, ordered_at, order_date_jst, status, status_source, received_batch_seq, source_updated_at, transform_version, content_hash)
  values (1, $1, $2, $3, 'mall_api', $6, $4::date::timestamptz, $4::date, $5, $8, 1, ($7::date::timestamp + interval '12 hours') at time zone 'Asia/Tokyo', 'v1', 'h') returning order_id`, [mall, scope, no, day, status, shop, su, src])).order_id;
const slip11 = (no, shop, { orderId = null, cancelled = false, shipDate = null, status = null } = {}) => pg.query(`insert into core.shipments (company_id, ne_slip_no, order_id, ne_order_no, shop_code, status, is_cancelled, ship_date_jst, shipped_at, received_batch_seq, source_updated_at, transform_version, content_hash)
  values (1, $1, $2, $3, $4, coalesce($7, case when $5 then 'cancelled' when $6::date is not null then 'shipped' else 'confirmed' end), $5, $6::date, $6::date::timestamptz, 1, now(), 'v1', 'h')`, [`SL-${no}-${++seq}`, orderId, no, shop, cancelled, shipDate, status]);
const w11 = (r, scope) => resultOf(r, 'W11', scope);
const kinds = (r, scope) => w11(r, scope).items.map((i) => `${i.payload.mall_order_no}:${i.payload.kind}`).sort();
const clean11 = async () => { await pg.query(`delete from core.shipments where ne_slip_no like 'SL-w11%'`); await pg.query(`delete from core.orders where mall_order_no like 'w11%'`); };
await t('🚨 W11 A: モールで出荷済みなのに NE の伝票が 1 つも結び付いていない注文 (番号の合う伝票が結べていない も) = 1 件でも異常 / 有効な伝票あり・自社発送でない (FBA)・注文から 5 日たっていない・30 日より前 は数えない。最初の 2 週間は info', async () => {
  const a1 = await o11('rakuten', 'main', 'w11-a1', D(-10), 'shipped', '1');     // 伝票なし
  const a3 = await o11('rakuten', 'main', 'w11-a3', D(-12), 'shipped', '1');     // 有効な伝票あり
  await slip11('w11-a3', '1', { orderId: a3, shipDate: D(-11) });
  await o11('rakuten', 'main', 'w11-a4', D(-12), 'shipped', '1');                // 番号の合う伝票はあるのに結べていない
  await slip11('w11-a4', '1');
  await o11('rakuten', 'main', 'w11-a5', D(-4), 'shipped', '1');                 // 5 日たっていない
  await o11('rakuten', 'main', 'w11-a6', D(-40), 'shipped', '1');                // 30 日より前
  await o11('amazon', 'jp', 'w11-a7', D(-10), 'shipped', null);                  // FBA (shop_code なし)
  let r = await run({ dryRun: true });
  const x = w11(r, 'rakuten/main');
  assert.deepEqual([x.verdict, x.severity, kinds(r, 'rakuten/main'), x.observed.a, x.observed.a_slip_not_linked, x.periodFrom, x.periodTo, verdictOf(r, 'W11', 'amazon/jp')], ['breach', 'info', ['w11-a1:A_no_slip', 'w11-a4:A_slip_not_linked'], 2, 1, D(-30), D(-5), 'pass']);
  assert.match(x.reason, /モールで出荷済みなのに NE の伝票なし 2 \(うち番号の合う伝票が結べていない 1\)/);
  r = await run({ dryRun: true, config: { ...CONFIG, W11_INFO_UNTIL: '2026-09-01' } });
  assert.equal(w11(r, 'rakuten/main').severity, 'warn');
  await slip11('w11-a1', '1', { orderId: a1, shipDate: D(-9) });
  await pg.query(`update core.shipments set order_id = (select order_id from core.orders where mall_order_no = 'w11-a4') where ne_order_no = 'w11-a4'`);
  r = await run({ dryRun: true });
  assert.equal(verdictOf(r, 'W11', 'rakuten/main'), 'pass');
  await clean11();
});
await t("🚨 W11 A' (Codex #1419 R1 High): 結び付いた伝票がキャンセルだけの注文は、同梱と見分けられない = 黙って正常にしない。毎回数えて明細に残し、上限 (同梱の目安) を超えたら異常", async () => {
  for (const n of [1, 2, 3]) { const id = await o11('rakuten', 'main', `w11-c${n}`, D(-10), 'delivered', '1'); await slip11(`w11-c${n}`, '1', { orderId: id, cancelled: true }); }
  let r = await run({ dryRun: true });
  let x = w11(r, 'rakuten/main');
  assert.deepEqual([x.verdict, x.observed.a_cancelled_only, x.observed.a, x.items.length, /キャンセルだけ 3 \(上限 48 以内/.test(x.reason)], ['pass', 3, 0, 3, true]);
  r = await run({ dryRun: true, config: { ...CONFIG, W11_CANCELLED_ONLY_MAX: { ...CONFIG.W11_CANCELLED_ONLY_MAX, rakuten: 2 } } });
  x = w11(r, 'rakuten/main');
  assert.deepEqual([x.verdict, /結び付いた伝票がキャンセルだけ 3 \(上限 2 = 同梱の目安を超えた/.test(x.reason)], ['breach', true]);
  await clean11();
});
await t('🚨 W11 B / B2: Amazon 自社発送・LINE ギフトで、モールでも NE でも未発送のまま状態が 5 日動いていない (B) / NE で出荷して 2 日たつのに Amazon が未発送 (B2)。Amazon は new も未発送 (Pending の次が Shipped)・LINE ギフトの new は正当な待ち・楽天などは既存の未発送アラート (Codex R1 Medium: 状態が後から変わった注文はそこから数える / B2 に猶予 / LINE ギフトは状態の新しさが分からない = B2 しない)', async () => {
  const b1 = await o11('amazon', 'jp', 'w11-b1', D(-7), 'new', '4');                        // Amazon 自社発送・Pending のまま 7 日・伝票なし
  await o11('amazon', 'jp', 'w11-b1b', D(-10), 'new', '4', { su: D(-2) });                  // 状態が 2 日前に変わった (支払いが後から済んだ) = まだ数えない
  const b2 = await o11('amazon', 'jp', 'w11-b2', D(-7), 'new', '4');                        // NE で 3 日前に出荷済み = B2
  await slip11('w11-b2', '4', { orderId: b2, shipDate: D(-3) });
  const b2b = await o11('amazon', 'jp', 'w11-b2b', D(-7), 'new', '4');                      // NE で昨日出荷 = 通知の反映待ち (猶予 2 日)
  await slip11('w11-b2b', '4', { orderId: b2b, shipDate: D(-1) });
  await o11('linegift', 'main', 'w11-b3', D(-10), 'confirmed', '14');                       // 支払い済み・伝票なし・10 日動いていない
  await o11('linegift', 'main', 'w11-b3b', D(-12), 'confirmed', '14', { su: D(-3) });       // 住所入力が 3 日前 = まだ数えない
  await o11('linegift', 'main', 'w11-b4', D(-10), 'new', '14');                             // 受取人の住所入力待ち = 数えない
  const b5 = await o11('linegift', 'main', 'w11-b5', D(-10), 'confirmed', '14');            // NE では出荷済み = LINE ギフトは B2 しない
  await slip11('w11-b5', '14', { orderId: b5, shipDate: D(-9) });
  const b6 = await o11('linegift', 'main', 'w11-b6', D(-10), 'confirmed', '14');            // NE の伝票はあるが未出荷 = B
  await slip11('w11-b6', '14', { orderId: b6 });
  await o11('rakuten', 'main', 'w11-b7', D(-10), 'confirmed', '1');                         // 楽天 = 既存の未発送アラート
  let r = await run({ dryRun: true });
  assert.deepEqual([kinds(r, 'amazon/jp'), kinds(r, 'linegift/main'), verdictOf(r, 'W11', 'rakuten/main')], [['w11-b1:B_unshipped', 'w11-b2:B2_mall_not_notified'], ['w11-b3:B_unshipped', 'w11-b6:B_unshipped'], 'pass']);
  assert.match(w11(r, 'amazon/jp').reason, /モールでも NE でも未発送のまま \(内容が 5 日変わっていない \/ 注文から 14 日\) 1 \/ NE で出荷して 2 日たつのにモールが未発送 1/);
  // 🚨 Codex R2 Medium: 内容の訂正が続いて起算日が進んでも、注文日から 14 日で必ず出す (安全網) / 境目 = 内容が変わってちょうど 5 日・出荷確定からちょうど 2 日で出す
  await o11('amazon', 'jp', 'w11-b8', D(-14), 'new', '4', { su: D(-1) });                    // 昨日訂正・注文から 14 日
  await o11('amazon', 'jp', 'w11-b9', D(-13), 'new', '4', { su: D(-1) });                    // 昨日訂正・注文から 13 日 = まだ
  await o11('amazon', 'jp', 'w11-b10', D(-12), 'new', '4', { su: D(-5) });                   // 内容が変わってちょうど 5 日
  const b11 = await o11('amazon', 'jp', 'w11-b11', D(-7), 'new', '4');                         // NE の出荷確定からちょうど 2 日
  await slip11('w11-b11', '4', { orderId: b11, shipDate: D(-2) });
  r = await run({ dryRun: true });
  assert.deepEqual(kinds(r, 'amazon/jp'), ['w11-b10:B_unshipped', 'w11-b11:B2_mall_not_notified', 'w11-b1:B_unshipped', 'w11-b2:B2_mall_not_notified', 'w11-b8:B_unshipped']);   // b9 (注文から 13 日・昨日訂正) は出ない
  await pg.query(`delete from core.shipments where ne_order_no = 'w11-b11'`); await pg.query(`delete from core.orders where mall_order_no in ('w11-b8', 'w11-b9', 'w11-b10', 'w11-b11')`);
  await pg.query(`update core.orders set status = 'shipped' where mall_order_no in ('w11-b1', 'w11-b2')`);
  await slip11('w11-b1', '4', { orderId: b1, shipDate: D(-1) });
  r = await run({ dryRun: true });
  assert.equal(verdictOf(r, 'W11', 'amazon/jp'), 'pass');
  await clean11();
});
await t('🚨 W11 P (9/25・中原さん確認): Amazon で Pending かつ有効な伝票が全部 NE で受注メール取込済 (new) のまま = 支払い待ちの保留 → 注文から 7 日未満は異常にしない (observed に残す)。7 日目から B (NE の受注メール取込済は起票が止まった注文も同じ形 = Codex #1454 R1) / NE で起票済み (confirmed) なのに未出荷・伝票なし・状態の原文が Pending でない は今まで通り B', async () => {
  const p1 = await o11('amazon', 'jp', 'w11-p1', D(-6), 'new', '4', { src: 'Pending' });     // 支払い待ち (NE = 受注メール取込済)・注文から 6 日
  await slip11('w11-p1', '4', { orderId: p1, status: 'new' });
  const p2 = await o11('amazon', 'jp', 'w11-p2', D(-7), 'new', '4', { src: 'Pending' });     // NE で起票済みなのに未出荷 = B (250-5900546-7297417 の形)
  await slip11('w11-p2', '4', { orderId: p2 });
  const p3 = await o11('amazon', 'jp', 'w11-p3', D(-7), 'new', '4', { src: 'Pending' });     // 注文から 7 日たっても支払い待ち = B (境目)
  await slip11('w11-p3', '4', { orderId: p3, status: 'new' });
  await o11('amazon', 'jp', 'w11-p4', D(-7), 'new', '4', { src: 'Pending' });                // 伝票なし = B (NE に取り込まれていない)
  const p5 = await o11('amazon', 'jp', 'w11-p5', D(-7), 'new', '4', { src: 'Unshipped' });   // 原文が Pending でない = B
  await slip11('w11-p5', '4', { orderId: p5, status: 'new' });
  const p6 = await o11('amazon', 'jp', 'w11-p6', D(-8), 'new', '4', { src: 'Pending' });     // 有効な伝票のうち 1 つが起票済み = 支払い待ちと言えない = B
  await slip11('w11-p6', '4', { orderId: p6, status: 'new' });
  await slip11('w11-p6', '4', { orderId: p6 });
  const p7 = await o11('amazon', 'jp', 'w11-p7', D(-5), 'new', '4', { src: 'Pending' });     // 有効な伝票が 2 つとも受注メール取込済 + キャンセル済みの起票済み = 支払い待ち
  await slip11('w11-p7', '4', { orderId: p7, status: 'new' });
  await slip11('w11-p7', '4', { orderId: p7, status: 'new' });
  await slip11('w11-p7', '4', { orderId: p7, cancelled: true });
  const p8 = await o11('amazon', 'jp', 'w11-p8', D(-6), 'new', '4', { src: 'Pending' });     // 伝票がキャンセルだけ = 有効な伝票なし = B
  await slip11('w11-p8', '4', { orderId: p8, cancelled: true });
  const p9 = await o11('amazon', 'jp', 'w11-p9', D(-6), 'new', '4', { src: 'Pending' });     // NE で出荷済み (2 日前) = B2 (支払い待ちにしない)
  await slip11('w11-p9', '4', { orderId: p9, shipDate: D(-2), status: 'new' });
  const p10 = await o11('amazon', 'jp', 'w11-p10', D(-7), 'new', '4', { src: 'Pending', su: D(-1) });   // 注文から 7 日・内容が昨日変わった = それでも B (Codex #1454 R2)
  await slip11('w11-p10', '4', { orderId: p10, status: 'new' });
  const p11 = await o11('amazon', 'jp', 'w11-p11', D(-6), 'new', '4', { src: 'Pending', su: D(-1) });   // 注文から 6 日・内容が昨日変わった = まだ数えない (P でも B でもない)
  await slip11('w11-p11', '4', { orderId: p11, status: 'new' });
  let r = await run({ dryRun: true });
  const x = w11(r, 'amazon/jp');
  assert.deepEqual([x.verdict, kinds(r, 'amazon/jp'), x.observed.payment_pending, x.observed.payment_pending_orders.map((o) => o.mall_order_no).sort()],
    ['breach', ['w11-p2:B_unshipped', 'w11-p3:B_unshipped', 'w11-p4:B_unshipped', 'w11-p5:B_unshipped', 'w11-p6:B_unshipped', 'w11-p8:B_unshipped', 'w11-p9:B2_mall_not_notified', 'w11-p10:B_unshipped'].sort(), 2, ['w11-p1', 'w11-p7']]);
  assert.match(x.reason, /支払い待ち \(モールで Pending・NE で受注メール取込済のまま\) 2 \(注文から 7 日未満は数えない/);
  // 支払い待ちだけなら pass (理由に件数を残す)
  await pg.query(`delete from core.shipments where ne_order_no in ('w11-p2', 'w11-p3', 'w11-p5', 'w11-p6', 'w11-p8', 'w11-p9', 'w11-p10', 'w11-p11')`); await pg.query(`delete from core.orders where mall_order_no in ('w11-p2', 'w11-p3', 'w11-p4', 'w11-p5', 'w11-p6', 'w11-p8', 'w11-p9', 'w11-p10', 'w11-p11')`);
  r = await run({ dryRun: true });
  assert.deepEqual([verdictOf(r, 'W11', 'amazon/jp'), w11(r, 'amazon/jp').items.length, /支払い待ち .* 2/.test(w11(r, 'amazon/jp').reason)], ['pass', 0, true]);
  await clean11();
});
await t('🚨 W11 の前提: 今朝の出荷の push が確かめられない (証跡なし・失敗・別の実行) → blocked (NE の伝票がそろっているか分からない) / そのモールの結び直しが途中・失敗 → そのモールだけ blocked (結び直しを回さなかった朝 = relink なし は見る) / W7 が pass でなければ blocked', async () => {
  await o11('rakuten', 'main', 'w11-c1', D(-10), 'shipped', '1');
  const E = goodEvidence(); delete E.shipments;
  let r = await run({ dryRun: true, evidence: E });
  assert.deepEqual([verdictOf(r, 'W11', 'rakuten/main'), /今朝の出荷の push が確かめられない/.test(w11(r, 'rakuten/main').reason)], ['blocked', true]);
  r = await run({ dryRun: true, evidence: { ...goodEvidence(), shipments: shipEvidence({ failed: 2 }) } });
  assert.equal(verdictOf(r, 'W11', 'rakuten/main'), 'blocked');
  r = await run({ dryRun: true, evidence: { ...goodEvidence(), shipments: shipEvidence({ sync_run_id: 'ds_other' }) } });
  assert.deepEqual([verdictOf(r, 'W11', 'rakuten/main'), /別の実行の証跡/.test(w11(r, 'rakuten/main').reason)], ['blocked', true]);
  r = await run({ dryRun: true, evidence: { ...goodEvidence(), 'orders-rakuten': ev('rakuten', 'main', { relink: { ok: true, linked: 10, pending: true } }) } });
  assert.deepEqual([verdictOf(r, 'W11', 'rakuten/main'), /結び直しが途中/.test(w11(r, 'rakuten/main').reason), verdictOf(r, 'W11', 'amazon/jp')], ['blocked', true, 'pass']);
  r = await run({ dryRun: true, evidence: { ...goodEvidence(), 'orders-rakuten': ev('rakuten', 'main', { relink: { ok: false, linked: null, pending: false } }) } });
  assert.deepEqual([verdictOf(r, 'W11', 'rakuten/main'), /結び直しが失敗した/.test(w11(r, 'rakuten/main').reason)], ['blocked', true]);
  r = await run({ dryRun: true, evidence: { ...goodEvidence(), 'orders-rakuten': ev('rakuten', 'main', { relink: null }) } });
  assert.equal(verdictOf(r, 'W11', 'rakuten/main'), 'breach');   // 結び直しを回さなかった朝 (残りなし) = 見る
  r = await run({ dryRun: true, evidence: { ...goodEvidence(), 'orders-rakuten': ev('rakuten', 'main', { sync_run_id: 'ds_other' }) } });
  assert.deepEqual([verdictOf(r, 'W11', 'rakuten/main'), w11(r, 'rakuten/main').blockedBy], ['blocked', 'W7:rakuten/main']);
  await clean11();
});
await t('🚨 W11 の世代の指紋: 評価の後に伝票が結び付いた (注文の件数は変わらない) / 状態が変わった日だけ動いた のを見つけて再評価する', async () => {
  const id = await o11('rakuten', 'main', 'w11-d1', D(-10), 'shipped', '1');
  let r = await run({ dryRun: true, hooks: { afterSnapshot: async (n) => { if (n === 1) await slip11('w11-d1', '1', { orderId: id, shipDate: D(-9) }); } } });
  assert.deepEqual([r.attempts, verdictOf(r, 'W11', 'rakuten/main')], [2, 'pass']);
  await o11('amazon', 'jp', 'w11-d2', D(-10), 'new', '4');
  r = await run({ dryRun: true, hooks: { afterSnapshot: async (n) => { if (n === 1) await pg.query(`update core.orders set source_updated_at = now() where mall_order_no = 'w11-d2'`); } } });
  assert.deepEqual([r.attempts, verdictOf(r, 'W11', 'amazon/jp')], [2, 'pass']);   // 状態が今日変わった = まだ数えない
  await clean11();
  // Codex #1454 R1 P2: 評価の後に NE の伝票が受注メール取込済 → 起票済み に変わった (伝票の数・出荷日は同じ) / 状態の原文だけが変わった → 再評価して B にする
  const d3 = await o11('amazon', 'jp', 'w11-d3', D(-6), 'new', '4', { src: 'Pending' });
  await slip11('w11-d3', '4', { orderId: d3, status: 'new' });
  r = await run({ dryRun: true, hooks: { afterSnapshot: async (n) => { if (n === 1) await pg.query(`update core.shipments set status = 'confirmed' where ne_order_no = 'w11-d3'`); } } });
  assert.deepEqual([r.attempts, verdictOf(r, 'W11', 'amazon/jp'), w11(r, 'amazon/jp').observed.payment_pending], [2, 'breach', 0]);
  await pg.query(`update core.shipments set status = 'new' where ne_order_no = 'w11-d3'`);
  r = await run({ dryRun: true, hooks: { afterSnapshot: async (n) => { if (n === 1) await pg.query(`update core.orders set status_source = 'Unshipped' where mall_order_no = 'w11-d3'`); } } });
  assert.deepEqual([r.attempts, verdictOf(r, 'W11', 'amazon/jp')], [2, 'breach']);
  await clean11();
});

console.log('W4 / W12: 在庫の純減の異常・DB の容量');
const w4 = (r) => resultOf(r, 'W4', 'logizard/main');
const w12 = (r) => resultOf(r, 'W12', 'db/company');
await t('🚨 W4: 昨日の在庫の差 (SKU の和) の減った数を、同じ曜日の過去 8 週の中央値 ± 3×MAD かつ 500 個以上で判定 (多すぎ・少なすぎ)。差し引きは大きく減ったときだけ。最初は info', async () => {
  let r = await run({ dryRun: true });
  let x = w4(r);
  assert.deepEqual([x.verdict, x.severity, x.observed.yesterday.out, x.observed.samples, x.observed.stats.out.median, x.observed.stats.out.mad], ['pass', 'info', 3000, 8, 3000, 100]);
  // 大量の減少: 昨日の差を作り直す (印とイベントを別の区間の番号で入れ直す = 見本の区間を消して新しいイベント)
  await w4Replace(D(-1), Array.from({ length: 10 }, () => -2000));   // 減った数 20,000・差し引き −20,000
  r = await run({ dryRun: true });
  x = w4(r);
  assert.deepEqual([x.verdict, /減った数 20,000 個 .*多すぎ = 大量の減少/.test(x.reason), /差し引き -20,000 個 .*大きく減った/.test(x.reason)], ['breach', true, true]);
  // 少なすぎ (出荷が在庫に反映されていない疑い): 減った数 0・増えた数だけ → 差し引きは増えた = 差し引きの判定は出さない
  await w4Replace(D(-1), Array.from({ length: 10 }, () => 5));
  r = await run({ dryRun: true });
  x = w4(r);
  assert.deepEqual([x.verdict, /減った数 0 個 .*少なすぎ/.test(x.reason), /差し引き/.test(x.reason)], ['breach', true, false]);
  // ゆらぎの中 (3×MAD = 300 を超えても 500 個未満の差は騒がない)
  await w4Replace(D(-1), Array.from({ length: 10 }, () => -340));   // 3,400 = 平常 3,000 から +400 (> 300 だが < 500)
  r = await run({ dryRun: true });
  assert.equal(w4(r).verdict, 'pass');
  await w4Replace(D(-1), Array.from({ length: 10 }, () => -300));
});
await t('🚨 W4 の前提と標本: 昨日の差の印が無い・差を作っていない日 (skipped) → blocked / 印の件数とイベントの数が合わない → blocked (その日は標本からも外す) / 標本 < 4 → blocked (在庫の差は 9/20 から) / 祝日・年末年始 = 昨日なら判定しない・標本から外す (本番の一覧に 9/21〜9/23)', async () => {
  // 昨日が skipped
  await pg.query(`delete from snapshots.stock_diff_days where to_date = $1::date`, [D(-1)]); await diffDay(D(-1), 'skipped', { skip: 'prev_not_complete' });
  let r = await run({ dryRun: true });
  assert.deepEqual([verdictOf(r, 'W3', 'logizard/main'), verdictOf(r, 'W4', 'logizard/main'), /差を作っていない \(skipped = W2 が見る\)/.test(w4(r).reason)], ['pass', 'blocked', true]);
  await pg.query(`delete from snapshots.stock_diff_days where to_date = $1::date`, [D(-1)]); await diffDay(D(-1), 'done', { events: 10 });
  // 印の件数とイベントの数が合わない (昨日)
  await pg.query(`update snapshots.stock_diff_days set events = 11 where to_date = $1::date`, [D(-1)]);
  r = await run({ dryRun: true });
  assert.deepEqual([verdictOf(r, 'W4', 'logizard/main'), /差の印とイベントの数が合わない \(11\/10/.test(w4(r).reason)], ['blocked', true]);
  await pg.query(`update snapshots.stock_diff_days set events = 10 where to_date = $1::date`, [D(-1)]);
  // 標本 < 4: 過去 8 週のうち 5 週の印を消す
  for (const k of [2, 3, 4, 5, 6]) await pg.query(`delete from snapshots.stock_diff_days where to_date = $1::date`, [D(-1 - 7 * k)]);
  r = await run({ dryRun: true });
  assert.deepEqual([verdictOf(r, 'W4', 'logizard/main'), w4(r).sampleSize, /有効標本 3 < 4/.test(w4(r).reason)], ['blocked', 3, true]);
  for (const k of [2, 3, 4, 5, 6]) await diffDay(D(-1 - 7 * k), 'done', { events: 10 });
  // 祝日: 本番の一覧 (9/22 = 試験の昨日) なら判定しない / 標本の日が祝日なら外す
  assert.ok(['2026-09-21', '2026-09-22', '2026-09-23'].every((d) => REAL_CONFIG.NON_BUSINESS_DAYS.includes(d)));
  r = await run({ dryRun: true, config: { ...CONFIG, NON_BUSINESS_DAYS: [D(-1)] } });
  assert.deepEqual([verdictOf(r, 'W4', 'logizard/main'), /祝日・年末年始/.test(w4(r).reason)], ['blocked', true]);
  r = await run({ dryRun: true, config: { ...CONFIG, NON_BUSINESS_DAYS: [D(-8), D(-15)] } });
  assert.deepEqual([verdictOf(r, 'W4', 'logizard/main'), w4(r).sampleSize, w4(r).observed.excluded], ['pass', 6, [`${D(-8).slice(5)}:non_business_day`, `${D(-15).slice(5)}:non_business_day`]]);
});
await t('🚨 W12: 今の大きさと、毎晩の締めの記録 (ops.job_runs) の日ごとの増え分の中央値から、容量まで 90 日を切る・7 GB を超えたら異常。一度きりの急増 (バックフィル) には引きずられない / 記録が足りなければ blocked', async () => {
  let r = await run({ dryRun: true });
  let x = w12(r);
  assert.deepEqual([x.verdict, x.severity, x.observed.growth_mb_per_day, x.sampleSize], ['pass', 'info', 1, 7]);
  // 一度きりの急増 (1.5 GB) が混ざっても中央値は 1 MB/日
  await pg.query(`insert into ops.job_runs (job_id, host, started_at, finished_at, status, summary) values ('company-db-inventory-hourly', 'test', $1::timestamptz, now(), 'ok', $2)`, [`${D(0)}T00:40:00+09:00`, JSON.stringify({ step: 'maintain', db_bytes: 40 * 1048576 + 1536 * 1048576, db_mb: 1576 })]);
  r = await run({ dryRun: true });
  x = w12(r);
  assert.deepEqual([x.verdict, x.observed.growth_mb_per_day, x.observed.max_delta_mb > 1000], ['pass', 1, true]);
  // 容量が小さければ (残り 90 日を切る) 異常 / 7 GB (ここでは小さく) を超えたら異常
  r = await run({ dryRun: true, config: { ...CONFIG, W12_DISK_BYTES: 60 * 1048576 + (await one(`select pg_database_size(current_database())::bigint as b`)).b * 1 } });
  assert.deepEqual([w12(r).verdict, /容量 .* まで あと \d+ 日/.test(w12(r).reason)], ['breach', true]);
  r = await run({ dryRun: true, config: { ...CONFIG, W12_WARN_BYTES: 1048576 } });
  assert.deepEqual([w12(r).verdict, /を超えた/.test(w12(r).reason)], ['breach', true]);
  // 記録が足りない (直近 3 日だけ見る = 増え分 3 < 5)
  r = await run({ dryRun: true, config: { ...CONFIG, W12_HISTORY_DAYS: 3 } });
  assert.deepEqual([w12(r).verdict, /記録が足りない/.test(w12(r).reason)], ['blocked', true]);
  // 急増の記録は残したまま (ops.job_runs は追記専用)。以後の試験も中央値 = 1 MB/日で pass のまま
});
await t('🚨 W12 (Codex #1423 R1): 最新の大きさの記録が 3 日より古い (毎晩の締めが止まった) → 残り日数を推計しない (blocked)。ただし 7 GB を超えていれば記録に関係なく異常 / 壊れた記録 (JSON でない・数字でない) で評価ごと落ちない / 祝日の一覧の期限切れ → W4 は blocked', async () => {
  // 14 日前〜9 日前にだけ記録がある (以後は締めが止まった) = 増え分は 5 個あるが最新が古い。ops.job_runs は追記専用 = 別のジョブ ID に入れて config で読ませる
  const stale = { ...CONFIG, W12_HISTORY_DAYS: 30, W12_JOB_ID: 'test-w12-stale' };
  for (let n = -14; n <= -9; n++) await pg.query(`insert into ops.job_runs (job_id, host, started_at, finished_at, status, summary) values ('test-w12-stale', 'test', $1::timestamptz, now(), 'ok', $2)`, [`${D(n)}T00:35:00+09:00`, JSON.stringify({ step: 'maintain', db_bytes: (40 + n) * 1048576, db_mb: 40 + n })]);
  let r = await run({ dryRun: true, config: stale });
  assert.deepEqual([w12(r).verdict, /記録が途絶えている \(最新 .*9/.test(w12(r).reason), w12(r).observed.latest_record], ['blocked', true, D(-9)]);
  r = await run({ dryRun: true, config: { ...stale, W12_WARN_BYTES: 1048576 } });
  assert.deepEqual([w12(r).verdict, /を超えた/.test(w12(r).reason)], ['breach', true]);
  // 壊れた記録 (締めの行らしいが JSON でない・db_bytes が数字でない) = その行を読み飛ばすだけ
  await pg.query(`insert into ops.job_runs (job_id, host, started_at, finished_at, status, summary) values ('test-w12-stale', 'test', $1::timestamptz, now(), 'ok', $2), ('test-w12-stale', 'test', $3::timestamptz, now(), 'ok', $4)`,
    [`${D(-8)}T00:35:00+09:00`, '{"step":"maintain", broken', `${D(-7)}T00:35:00+09:00`, '{"step":"maintain","db_bytes":"x"}']);
  r = await run({ dryRun: true, config: stale });
  assert.deepEqual([w12(r).verdict, w12(r).observed.latest_record], ['blocked', D(-9)]);
  r = await run({ dryRun: true });
  assert.equal(w12(r).verdict, 'pass');
  // W4: 祝日の一覧の期限を昨日より前に = 足し忘れ → blocked
  r = await run({ dryRun: true, config: { ...CONFIG, NON_BUSINESS_DAYS_UNTIL: D(-2) } });
  assert.deepEqual([verdictOf(r, 'W4', 'logizard/main'), /祝日の一覧 .* までしか無い/.test(w4(r).reason)], ['blocked', true]);
  assert.ok(REAL_CONFIG.NON_BUSINESS_DAYS.includes('2027-03-21') && REAL_CONFIG.NON_BUSINESS_DAYS.every((d) => d <= REAL_CONFIG.NON_BUSINESS_DAYS_UNTIL));
});
await t('🚨 W4 / W12 の世代の指紋: 評価の後に昨日の差のイベントが入れ直された・大きさの記録が増えた のを見つけて再評価する (今の大きさそのものは指紋に入れない = 読むたびに変わる)', async () => {
  let r = await run({ dryRun: true, hooks: { afterSnapshot: async (n) => { if (n === 1) await w4Replace(D(-1), Array.from({ length: 10 }, () => -310)); } } });
  assert.deepEqual([r.attempts, w4(r).observed.yesterday.out], [2, 3100]);
  r = await run({ dryRun: true, hooks: { afterSnapshot: async (n) => { if (n === 1) await pg.query(`insert into ops.job_runs (job_id, host, started_at, finished_at, status, summary) values ('company-db-inventory-hourly', 'test', $1::timestamptz, now(), 'ok', $2)`, [`${D(0)}T00:50:00+09:00`, JSON.stringify({ step: 'maintain', db_bytes: 41 * 1048576 })]); } } });
  assert.equal(r.attempts, 2);
  r = await run({ dryRun: true });
  assert.equal(r.attempts, 1);   // 何も変わらなければ 1 回 (今の大きさが指紋に入っていない)
  await w4Replace(D(-1), Array.from({ length: 10 }, () => -300));
});

console.log('実行器の守り');
await t('🚨 評価の 1 つが例外 → その項目の評価キーだけ execution_error・ほかは続く・最後の行は ❌ で exit 1。予定した評価キーが 1 つでも欠ければ ❌', async () => {
  await pg.query(`alter table snapshots.stock_diff_days rename to stock_diff_days_x`);
  let r;
  try { r = await run({ dryRun: true }); } finally { await pg.query(`alter table snapshots.stock_diff_days_x rename to stock_diff_days`); }
  // to_regclass が null → blocked (例外ではない)。例外の経路は W1 の表を壊して確かめる
  assert.equal(verdictOf(r, 'W3', 'logizard/main'), 'blocked');
  await pg.query(`alter table snapshots.stock_capture_days rename to stock_capture_days_x`);
  try { r = await run({ dryRun: true }); } finally { await pg.query(`alter table snapshots.stock_capture_days_x rename to stock_capture_days`); }
  // W1 4 + W2 4 = 8 が execution_error。W3 の評価そのものは通る (別の表) が、前提 W1 が execution_error → blocked。W7 / W9 は savepoint で守られて続く (pass)
  assert.deepEqual([r.counts.execution_error, verdictOf(r, 'W1', 'ne/main'), verdictOf(r, 'W3', 'logizard/main'), verdictOf(r, 'W7', 'rakuten/main'), verdictOf(r, 'W9', 'rakuten/main'), r.exitCode, r.lastLine.startsWith('❌')], [8, 'execution_error', 'blocked', 'pass', 'pass', 1, true]);
});
await t('🚨 記録する回は as_of = 今日 (JST) だけ (過去の日を評価して今の案件を回復させない)。dry-run なら過去の日も見られる', async () => {
  await assert.rejects(run({ asOf: D(-3) }), /as_of は今日/);
  const r = await run({ asOf: D(-3), dryRun: true });
  assert.equal(r.persisted, false);
  assert.equal((await one(`select count(*)::int as n from ops.watch_runs where as_of_date = $1::date`, [D(-3)])).n, 0);
});
await t('🚨 会社単位の advisory lock: 取れなければ止まる (2 本が同時に走らない)。取れた回は最後に外す', async () => {
  const seen = [];
  const noLock = { query: async (sql, p) => { if (/pg_try_advisory_lock/.test(sql)) { seen.push('try'); return { rows: [{ got: false }] }; } if (/pg_advisory_unlock/.test(sql)) seen.push('unlock'); return db.query(sql, p); }, exec: (sql) => db.exec(sql) };
  await assert.rejects(run({ writer: noLock }), /別の見張りが走っている/);
  assert.deepEqual(seen, ['try']);   // 取れなかったら unlock しない (他人の lock を外さない)
  const spy = { query: async (sql, p) => { if (/pg_try_advisory_lock/.test(sql)) seen.push('try2'); if (/pg_advisory_unlock/.test(sql)) seen.push('unlock2'); return db.query(sql, p); }, exec: (sql) => db.exec(sql) };
  const r = await run({ writer: spy });
  assert.deepEqual([r.persisted, seen.slice(1)], [true, ['try2', 'unlock2']]);
  const before = (await one(`select count(*)::int as n from ops.watch_runs`)).n;
  // 例外で落ちても lock は外す
  const boom = { query: async (sql, p) => { if (/pg_try_advisory_lock/.test(sql)) seen.push('try3'); if (/pg_advisory_unlock/.test(sql)) seen.push('unlock3'); if (/insert into ops.watch_runs/.test(sql)) throw new Error('disk full'); return db.query(sql, p); }, exec: (sql) => db.exec(sql) };
  await assert.rejects(run({ writer: boom }), /disk full/);
  assert.deepEqual([seen.slice(3), (await one(`select count(*)::int as n from ops.watch_runs`)).n], [['try3', 'unlock3'], before]);
});
await t('🚨 open の案件は 会社 × check × scope × 対象 で 1 つだけ (部分 unique。並行実行で二重に作れない)。recovered なら同じ鍵をもう一度 open にできる', async () => {
  const ins = (state) => pg.query(`insert into ops.watch_issues (company_id, check_id, scope_key, subject_type, subject_key, state, severity, first_seen_at, last_seen_at, recovered_at) values (1, 'W0', 'x/y', 'day', '2026-09-01', $1, 'warn', now(), now(), case when $1 = 'recovered' then now() end)`, [state]);
  await ins('open');
  await assert.rejects(ins('open'), (e) => e.code === '23505' || /ux_watch_issues_open|duplicate/i.test(String(e.message)));
  await pg.query(`update ops.watch_issues set state = 'recovered', recovered_at = now() where check_id = 'W0'`);
  await ins('open');
  assert.equal((await one(`select count(*)::int as n from ops.watch_issues where check_id = 'W0'`)).n, 2);
  await pg.query(`delete from ops.watch_issues where check_id = 'W0'`);
});
await t('🚨 snapshot を閉じた後に世代が変わっていれば再評価する (1 回変われば attempts 2)。変わり続ければ pass を blocked に落とし、要約に残す (黙って古い snapshot の pass を保存しない)', async () => {
  const bump = () => pg.query(`update mart.sales_daily_state set watermark = watermark + interval '1 second' where company_id = 1 and mall = 'rakuten'`);
  let r = await run({ hooks: { afterSnapshot: async (n) => { if (n === 1) await bump(); } } });
  assert.deepEqual([r.attempts, r.unstable, r.counts.pass, r.counts.blocked], [2, false, 45, 0]);
  r = await run({ hooks: { afterSnapshot: async () => { await bump(); } } });
  assert.deepEqual([r.attempts, r.unstable, r.counts.pass, r.counts.blocked, /世代が変わり続けた/.test(r.lastLine)], [MAX_GENERATION_RETRIES, true, 0, 45, true]);
  assert.match(resultOf(r, 'W1', 'ne/main').reason, /世代が変わり続けた/);
  assert.equal((await one(`select count(*)::int as n from ops.watch_issues where state = 'open'`)).n, 0);   // blocked = 案件に触らない
  const saved = await one(`select summary->>'attempts' as a, summary->>'unstable' as u, last_line from ops.watch_runs where watch_run_id = $1`, [r.runId]);
  assert.deepEqual([saved.a, saved.u, /世代が変わり続けた/.test(saved.last_line)], [String(MAX_GENERATION_RETRIES), 'true', true]);
  r = await run();
  assert.deepEqual([r.attempts, r.counts.pass], [1, 45]);
});
await t('🚨 Codex R2 #1: run が増えない途中 chunk で注文が増えた (未公開の日) のを世代の確認が見つける (指紋は W9 と同じ「日ごとの注文の有無」)', async () => {
  await orderRun('r2_running', { status: 'running', complete: false });
  let r = await run({ hooks: { afterSnapshot: async (n) => { if (n === 1) await order('aupay', 'main', D(-2), 'r2-new'); } } });
  assert.deepEqual([r.attempts, verdictOf(r, 'W9', 'aupay/main'), r.counts.new], [2, 'breach', 1]);
  assert.equal((await one(`select state from ops.watch_issues where check_id = 'W9' and scope_key = 'aupay/main' order by watch_issue_id desc limit 1`)).state, 'open');
  await pg.query(`delete from core.orders where mall = 'aupay' and mall_order_no = 'r2-new'`);
  await pg.query(`delete from ops.ingest_runs where ingest_run_id = 'r2_running'`);
  r = await run();
  assert.deepEqual([r.attempts, r.counts.recovered, verdictOf(r, 'W9', 'aupay/main')], [1, 1, 'pass']);
});
await t('🚨 Codex R2 #2: 世代が変わり続けた回は回復も保留 (breach の明細に無い案件を回復にしない)。安定した回で回復する', async () => {
  const bump = () => pg.query(`update mart.sales_daily_state set watermark = watermark + interval '1 second' where company_id = 1 and mall = 'rakuten'`);
  await delCapture(D(-2), 'ne'); await delCapture(D(-3), 'ne');
  let r = await run();
  assert.equal(r.counts.new, 2);   // W2 ne/main の D(-2)・D(-3)
  await capture(D(-2), 'ne', 'main', 'complete');   // D(-2) は直った
  r = await run({ hooks: { afterSnapshot: async () => { await bump(); } } });
  assert.deepEqual([r.unstable, verdictOf(r, 'W2', 'ne/main'), r.counts.recovered, r.counts.continued, r.notes.held.filter((h) => h.reason === 'unstable').length, /回復は保留/.test(r.lastLine)], [true, 'breach', 0, 1, 1, true]);
  assert.equal((await one(`select state from ops.watch_issues where check_id = 'W2' and scope_key = 'ne/main' and subject_key = $1`, [D(-2)])).state, 'open');
  r = await run();
  assert.deepEqual([r.unstable, r.counts.recovered, r.counts.continued], [false, 1, 1]);
  await capture(D(-3), 'ne', 'main', 'complete');
  r = await run();
  assert.deepEqual([r.counts.recovered, (await one(`select count(*)::int as n from ops.watch_issues where state = 'open'`)).n], [1, 0]);
});
await t('評価した日が 1 つも無い (since が窓の全部より後 = periodFrom が null) とき、日付の案件は「監視期間外」であって回復ではない', () => {
  const open = [{ watch_issue_id: 7, check_id: 'W2', scope_key: 'ne/main', subject_type: 'day', subject_key: '2026-09-15', severity: 'warn', first_seen_at: '2026-09-20T00:00:00Z', last_seen_at: '2026-09-22T00:00:00Z', days_seen: 3, transitions: 1 }];
  const rc = reconcileIssues({ config: CONFIG, results: [{ checkId: 'W2', scopeKey: 'ne/main', verdict: 'pass', severity: 'warn', items: [], periodFrom: null, periodTo: null }], openIssues: open, asOf: ASOF, now: NOW });
  assert.deepEqual([rc.updates.length, rc.updates[0].set.state, rc.notes.recovered.length, rc.notes.outOfWindow.length], [1, 'out_of_window', 0, 1]);
});
await t('評価の範囲より未来側の日の案件 (過去の日を評価しているとき) には触らない = 判定保留', () => {
  const open = [{ watch_issue_id: 5, check_id: 'W2', scope_key: 'ne/main', subject_type: 'day', subject_key: '2026-09-25', severity: 'warn', first_seen_at: '2026-09-26T00:00:00Z', last_seen_at: '2026-09-26T00:00:00Z', days_seen: 1, transitions: 1 }];
  const rc = reconcileIssues({ config: CONFIG, results: [{ checkId: 'W2', scopeKey: 'ne/main', verdict: 'pass', severity: 'warn', items: [], periodFrom: '2026-09-15', periodTo: '2026-09-21' }], openIssues: open, asOf: '2026-09-22', now: NOW });
  assert.deepEqual([rc.updates.length, rc.notes.recovered.length, rc.notes.held.length, rc.notes.held[0].reason], [0, 0, 1, 'beyond_period']);
});
await t('全体の期限を過ぎたら残りは execution_error (黙って pass にしない)', async () => {
  const r = await run({ dryRun: true, config: { ...CONFIG, RUN_DEADLINE_MS: -1 } });
  assert.deepEqual([r.counts.execution_error, r.counts.pass, r.exitCode], [45, 0, 1]);
});
await t('明細の抜粋は上限つき (行・バイト)。案件の管理は全件 (reconcileIssues は items 全部を見る)', () => {
  const items = Array.from({ length: 300 }, (_, i) => ({ subjectType: 'sku', subjectKey: String(i), payload: { weight: i } }));
  const p = pickItems(items, { maxRows: 200, maxBytes: 1 << 20 });
  assert.deepEqual([p.saved.length, p.total, p.omitted, p.saved[0].subjectKey], [200, 300, 100, '299']);
  const p2 = pickItems(items, { maxRows: 200, maxBytes: 200 });
  assert.ok(p2.saved.length < 10 && p2.omitted === 300 - p2.saved.length);
  const check = { ...CONFIG.checkById('W2') };
  const res = [{ checkId: 'W2', scopeKey: 'ne/main', verdict: 'breach', severity: 'warn', items, periodFrom: '2026-09-16', periodTo: '2026-09-22' }];
  const rc = reconcileIssues({ config: { ...CONFIG, CHECKS: [check] }, results: res, openIssues: [], asOf: ASOF, now: NOW });
  assert.equal(rc.inserts.length, 300);
});
await t('blocked の間は案件に触らない (継続にも回復にもしない = 判定保留)', () => {
  const open = [{ watch_issue_id: 9, check_id: 'W3', scope_key: 'logizard/main', subject_type: 'scope', subject_key: '', severity: 'error', first_seen_at: '2026-09-20T00:00:00Z', last_seen_at: '2026-09-22T00:00:00Z', days_seen: 3, transitions: 1 }];
  const rc = reconcileIssues({ config: CONFIG, results: [{ checkId: 'W3', scopeKey: 'logizard/main', verdict: 'blocked', severity: 'error', items: [] }], openIssues: open, asOf: ASOF, now: NOW });
  assert.deepEqual([rc.inserts.length, rc.updates.length, rc.notes.held.length, rc.notes.held[0].issueId], [0, 0, 1, 9]);
});

console.log('証跡 (evidence)');
await t('書く・読む・古いものを消す。本文は入らない・失敗しても投げない (警告だけ)', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-ev-'));
  try {
    const saved = process.env.DAILY_SYNC_RUN_ID;
    process.env.DAILY_SYNC_RUN_ID = SYNC;
    const p = writeEvidence(dir, 'orders-rakuten', { kind: 'orders', changed: 3 }, { now: NOW, warn: quiet });
    assert.ok(p && p.endsWith(path.join('2026-09-23', 'orders-rakuten.json')));
    assert.equal(readEvidence(dir, '2026-09-23')['orders-rakuten'].sync_run_id, SYNC);
    // 実行 ID の無い回 (人が手で流した) は <name>.manual.json = 朝の証跡を上書きしない。見張りは orders-rakuten だけを見る
    delete process.env.DAILY_SYNC_RUN_ID;
    const pm = writeEvidence(dir, 'orders-rakuten', { kind: 'orders', changed: 99 }, { now: NOW, warn: quiet });
    assert.ok(pm.endsWith('orders-rakuten.manual.json'));
    assert.deepEqual([readEvidence(dir, '2026-09-23')['orders-rakuten'].changed, readEvidence(dir, '2026-09-23')['orders-rakuten.manual'].sync_run_id], [3, null]);
    process.env.DAILY_SYNC_RUN_ID = SYNC;
    writeEvidence(dir, 'orders-rakuten', { kind: 'orders', changed: 5 }, { now: NOW, warn: quiet });   // 上書き = 再実行した回が正
    const got = readEvidence(dir, '2026-09-23');
    assert.deepEqual([got['orders-rakuten'].changed, got['orders-rakuten'].name, got['orders-rakuten'].date, typeof got['orders-rakuten'].written_at], [5, 'orders-rakuten', '2026-09-23', 'string']);
    const warns = [];
    assert.equal(writeEvidence(dir, 'bad name!', {}, { now: NOW, warn: (m) => warns.push(m) }), null);
    assert.equal(writeEvidence('', 'x', {}, { now: NOW, warn: (m) => warns.push(m) }), null);
    assert.equal(warns.length, 2);
    fs.mkdirSync(path.join(dir, 'company-db-evidence', '2026-09-01'), { recursive: true });
    fs.writeFileSync(path.join(dir, 'company-db-evidence', '2026-09-23', 'broken.json'), '{not json');
    fs.mkdirSync(path.join(dir, 'company-db-evidence', '2026-09-09'), { recursive: true });   // 15 日前 = 消える
    fs.mkdirSync(path.join(dir, 'company-db-evidence', '2026-09-10'), { recursive: true });   // 14 日前 = 残る (今日を含めて 14 日ぶん)
    assert.equal(purgeOldEvidence(dir, { now: NOW, keepDays: EVIDENCE_KEEP_DAYS }), 2);
    assert.deepEqual([fs.existsSync(path.join(dir, 'company-db-evidence', '2026-09-01')), fs.existsSync(path.join(dir, 'company-db-evidence', '2026-09-09')), fs.existsSync(path.join(dir, 'company-db-evidence', '2026-09-10')), /読めない/.test(readEvidence(dir, '2026-09-23').broken.error), Object.keys(readEvidence(dir, '2026-01-01')).length], [false, false, true, true, 0]);
    if (saved === undefined) delete process.env.DAILY_SYNC_RUN_ID; else process.env.DAILY_SYNC_RUN_ID = saved;
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
});
await t('送り手の証跡の形 (mall-orders の evidenceOf): 件数だけ・注文の中身は入らない', async () => {
  const { evidenceOf } = await import('../apps/company-db/push/mall-orders.mjs');
  const r = { ok: true, mode: 'incremental', runId: 'run1', batchSeq: 7, scanned: 10, inScope: 9, unchanged: 8, changed: 1, sent: 1, applied: 1, same: 0, stale: 0, failed: [], transformErrors: [], lockedBy: null, ledgerReset: null, ledgerRebuilt: 0 };
  const e = evidenceOf('rakuten', r, { startedAt: NOW, success: true, relink: { ran: true, error: null, result: { linked: 2 }, pending: false }, sales: { ok: true, complete: true, dates: 2 } });
  assert.deepEqual([e.kind, e.mall, e.scope, e.push_ok, e.run_id, e.changed, e.failed, e.transform_errors, e.relink.linked, e.sales.dates, 'rows' in e], ['orders', 'rakuten', 'main', true, 'run1', 1, 0, 0, 2, 2, false]);
});
await t('送り手の引数: --incremental と --from/--to は一緒に指定できない (範囲を流した回の証跡を「今朝の走査」と読まない)', async () => {
  const mo = await import('../apps/company-db/push/mall-orders.mjs');
  const ne = await import('../apps/company-db/push/ne-shipments.mjs');
  assert.throws(() => mo.parseArgs(['--mall', 'rakuten', '--incremental', '--from', '2026-09-01', '--to', '2026-09-02']), /--incremental と --from/);
  assert.throws(() => ne.parseArgs(['--incremental', '--to', '2026-09-02']), /--incremental と --from/);
  assert.equal(mo.parseArgs(['--mall', 'rakuten', '--incremental']).incremental, true);
  assert.equal(ne.parseArgs(['--from', '2026-09-01', '--to', '2026-09-02']).from, '2026-09-01');
});
await t('daily-sync の配線: 実行 ID を発行して子に渡す・retry-state に残す・retry が復元する・見張りの ❌ は retry に載る', () => {
  const ds = fs.readFileSync(path.join(root, 'apps', 'warehouse', 'daily-sync.js'), 'utf8');
  const rt = fs.readFileSync(path.join(root, 'apps', 'warehouse', 'retry-failed-jobs.js'), 'utf8');
  assert.ok(/process\.env\.DAILY_SYNC_RUN_ID = /.test(ds) && /daily_sync_run_id: process\.env\.DAILY_SYNC_RUN_ID/.test(ds));
  assert.ok(/const RETRYABLE_JOBS = \[[^\]]*'CompanyDB見張り'/.test(ds));
  assert.ok(/process\.env\.DAILY_SYNC_RUN_ID = String\(state\.daily_sync_run_id\)/.test(rt));
  const at = ds.indexOf('process.env.DAILY_SYNC_RUN_ID = '), base = ds.indexOf('process.env.WAREHOUSE_BUSINESS_DATE = businessDate');
  assert.ok(base > 0 && at > base && at - base < 600, '実行 ID は main の冒頭 (業務日付の直後) で決める = 最初のステップより前');
});

console.log('ロールと CLI');
await t('ロールの SQL: watcher は select だけ・schema を限定した default privileges・writer は ops.watch_* の insert と限定 update・security definer の public execute を外す', () => {
  const s = roleStatements({ dbName: 'cdb', owner: 'cdb_user', watcherPw: "p'w", writerPw: 'w', secdefFunctions: ['core.resolve_listing_id(smallint, text, text)'] }).join('\n');
  // 🚨 nosuperuser を書かない (PG16+ では書くだけで superuser でないと拒まれる = Render で "permission denied to alter role"。9/22 に本番で踏んだ)
  assert.ok(!/superuser/i.test(s), 'SUPERUSER 属性は書かない');
  assert.ok(!/createdb|bypassrls|replication/i.test(s), 'CREATEDB / BYPASSRLS / REPLICATION 属性も書かない (実行者に無いと拒まれる)');
  for (const frag of ["alter role watcher with login password 'p''w' nocreaterole noinherit connection limit 3", "alter role watch_writer with login password 'w' nocreaterole noinherit connection limit 2", "alter role watcher set default_transaction_read_only = on", 'grant select on all tables in schema core to watcher',
    'alter default privileges for role cdb_user in schema mart grant select on tables to watcher', 'revoke execute on function core.resolve_listing_id(smallint, text, text) from public', 'grant execute on function core.resolve_listing_id(smallint, text, text) to cdb_user',
    'grant select, insert on ops.watch_issues to watch_writer', 'grant update (finished_at, completed_keys, summary, last_line) on ops.watch_runs to watch_writer', 'grant usage on all sequences in schema ops to watch_writer']) assert.ok(s.includes(frag), frag);
  assert.ok(!/grant (insert|update|delete).* to watcher/.test(s) && !/grant .* on core.* to watch_writer/.test(s) && !/delete/.test(s));
  assert.deepEqual(WATCH_TABLES, ['watch_runs', 'watch_results', 'watch_issues', 'watch_result_items']);
  assert.equal(urlFor('postgres://u:p@host:5432/cdb?sslmode=require', 'watcher', 'x/y'), 'postgres://watcher:x%2Fy@host:5432/cdb?sslmode=require');
  assert.throws(() => roleStatements({ dbName: 'bad-name', owner: 'u', watcherPw: 'a', writerPw: 'b' }), /識別子/);
});
await t('🚨 Render と同じ条件 (superuser でない・CREATEROLE だけ・CREATEDB なし・DB とテーブルの owner) の実行者で createRoles を実際に流す: 初回 → 属性どおり・両ロールで verifyRole が通る / 流し直し (パスワード更新) も通る / 途中で落ちたら全部戻る / 既存ロールに bypassrls や membership が付いていれば止める', async () => {
  const pg3 = new PGlite();
  try {
    // 実行者 deploy = Render の default user の形。DB の owner にして、migration も deploy として流す (= テーブルの owner)
    await pg3.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
    await pg3.query(`alter database ${(await pg3.query('select current_database() as d')).rows[0].d} owner to deploy`);
    await pg3.query('set role deploy');
    await applyMigrations(pgliteAdapter(pg3), { log: quiet });
    assert.deepEqual((await pg3.query(`select current_user as u, r.rolcreaterole as c, r.rolsuper as s, r.rolcreatedb as d from pg_roles r where r.rolname = current_user`)).rows[0], { u: 'deploy', c: true, s: false, d: false });
    const countRoles = async () => (await pg3.query(`select count(*)::int as n from pg_roles where rolname in ('watcher', 'watch_writer')`)).rows[0].n;
    // 途中で落ちる (grant の 1 つで例外) → ロールは 1 つも残らない
    const boom = { query: (sql, p) => { if (/grant usage on schema mart to watcher/.test(sql)) throw new Error('boom'); return pg3.query(sql, p); } };
    await assert.rejects(createRoles(boom, { watcherPw: 'a', writerPw: 'b' }), /boom/);
    assert.equal(await countRoles(), 0);
    // 初回
    const r1 = await createRoles(pg3, { watcherPw: 'a', writerPw: 'b' });
    assert.deepEqual(r1.roles.map((x) => [x.rolname, x.rolsuper, x.rolcreaterole, x.rolcreatedb, x.rolbypassrls, x.rolcanlogin, x.rolinherit, Number(x.rolconnlimit), x.memberships]), [['watch_writer', false, false, false, false, true, false, 2, ''], ['watcher', false, false, false, false, true, false, 3, '']]);
    for (const [role, kind] of [['watcher', 'watcher'], ['watch_writer', 'writer']]) {
      await pg3.query(`set role ${role}`); await pg3.query(`set default_transaction_read_only = ${role === 'watcher' ? 'on' : 'off'}`);
      assert.deepEqual((await verifyRole(pg3, kind)).findings, [], role);
      await pg3.query('reset role'); await pg3.query('set role deploy');
    }
    // 流し直し (パスワードを変える・権限をそろえ直す) も通る
    const r2 = await createRoles(pg3, { watcherPw: 'a2', writerPw: 'b2' });
    assert.deepEqual([r2.roles.length, await countRoles()], [2, 2]);
    // 既存のロールに危険な属性 / membership が付いていたら止める (superuser が付けたものは deploy には外せない = 人が見る)
    await pg3.query('reset role'); await pg3.query(`alter role watcher with bypassrls`); await pg3.query('set role deploy');
    await assert.rejects(createRoles(pg3, { watcherPw: 'a3', writerPw: 'b3' }), /bypassrls/);
    await pg3.query('reset role'); await pg3.query(`alter role watcher with nobypassrls`); await pg3.query(`grant pg_read_all_data to watch_writer`); await pg3.query('set role deploy');
    await assert.rejects(createRoles(pg3, { watcherPw: 'a3', writerPw: 'b3' }), /メンバー \(pg_read_all_data\)/);
    await pg3.query('reset role'); await pg3.query(`revoke pg_read_all_data from watch_writer`); await pg3.query('set role deploy');
    assert.equal((await createRoles(pg3, { watcherPw: 'a4', writerPw: 'b4' })).roles.length, 2);
    // dry-run は流さない (文を返すだけ)
    const d = await createRoles(pg3, { watcherPw: 'x', writerPw: 'y', dryRun: true });
    assert.ok(d.stmts.length > 20 && d.roles.length === 0 && d.info.owner === 'deploy');
  } finally { await pg3.close(); }
});
await t('🚨 --verify は権限そのものを見る: watcher は read write の取引で書いて拒まれる (42501) / writer は記録の経路が通り・禁止の列・core の select・delete は拒まれる。期待と違えば findings に残る (exit 1 の材料)', async () => {
  // 疑似の接続: 文ごとに ok か 42501 を返す。savepoint / begin / rollback は通す
  const fake = (user, rule) => ({ log: [], query(sql, params) { this.log.push(sql); if (/^select current_user/.test(sql)) return { rows: [{ u: user, st: '10s' }] }; if (/^(begin|savepoint|release|rollback)/.test(sql)) return { rows: [] }; if (/select watch_result_id from ops.watch_results/.test(sql)) return { rows: [{ watch_result_id: 1 }] }; const code = rule(sql, params); if (code === 'ok') return { rows: [] }; const e = new Error('permission denied'); e.code = code; throw e; } });
  const goodWatcher = fake('watcher', (sql) => (/^select/.test(sql) ? 'ok' : '42501'));
  assert.deepEqual((await verifyRole(goodWatcher, 'watcher')).findings, []);
  assert.ok(goodWatcher.log.includes('begin read write'), 'read only の保険を外して権限そのもので拒まれるのを見る');
  const badWatcher = fake('watcher', () => 'ok');   // 何でも書ける = 誤設定
  const f1 = (await verifyRole(badWatcher, 'watcher')).findings;
  assert.ok(f1.length >= 3 && f1.some((x) => /insert/.test(x)) && f1.some((x) => /delete/.test(x)), f1.join(' | '));
  const wrongUser = fake('cdb_user', (sql) => (/^select/.test(sql) ? 'ok' : '42501'));
  assert.ok((await verifyRole(wrongUser, 'watcher')).findings.some((x) => /ロールが watcher ではない/.test(x)));
  const writerRule = (sql) => {
    if (/from core\./.test(sql) || /^delete/.test(sql)) return '42501';
    if (/^update ops.watch_issues set (check_id)/.test(sql) || /^update ops.watch_runs set (planned_keys)/.test(sql)) return '42501';
    return 'ok';
  };
  const goodWriter = fake('watch_writer', writerRule);
  assert.deepEqual((await verifyRole(goodWriter, 'writer')).findings, []);
  assert.ok(goodWriter.log.some((x) => /insert into ops.watch_result_items/.test(x)) && goodWriter.log.some((x) => /insert into ops.watch_issues/.test(x)) && goodWriter.log[goodWriter.log.length - 1] === 'rollback');
  const leakyWriter = fake('watch_writer', (sql) => (/^delete/.test(sql) ? '42501' : 'ok'));   // core が読める・禁止の列が書ける
  const f2 = (await verifyRole(leakyWriter, 'writer')).findings;
  assert.ok(f2.some((x) => /core\.orders/.test(x)) && f2.some((x) => /禁止の列/.test(x)), f2.join(' | '));
  const weakWriter = fake('watch_writer', (sql) => (/^insert into ops.watch_issues/.test(sql) ? '42501' : writerRule(sql)));   // 記録の経路が通らない
  assert.ok((await verifyRole(weakWriter, 'writer')).findings.some((x) => /watch_issues の insert/.test(x)));
  // 保存で使う列 (回復の recovered_at など) の権限が欠けていれば見つかる (Codex R2 Low)
  const noRecover = fake('watch_writer', (sql) => (/recovered_at = now\(\)/.test(sql) ? '42501' : writerRule(sql)));
  assert.ok((await verifyRole(noRecover, 'writer')).findings.some((x) => /継続・回復の update/.test(x)));
  assert.ok(goodWriter.log.some((x) => /update ops.watch_issues set state = 'recovered', severity = 'warn', last_seen_at = now\(\), days_seen = days_seen \+ 1, recovered_at = now\(\), transitions = transitions \+ 1, last_result_id = last_result_id, summary = 'v', updated_at = now\(\)/.test(x)), '許した列を全部使う update');
});
await t('CLI: 引数 (daily-sync の "7" を許す・--sync-run-id) / env が無ければ ⏭️ で exit 0・出力 1 行', () => {
  assert.deepEqual(parseArgs(['--as-of', '2026-09-23', '7', '--dry-run', '--sync-run-id', 'ds_x']), { dataDir: null, asOf: '2026-09-23', dryRun: true, json: false, syncRunId: 'ds_x' });
  assert.throws(() => parseArgs(['--as-of', '2026/09/23']), /YYYY-MM-DD/);
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-watch-'));
  try {
    const env = { ...process.env }; delete env.COMPANY_DB_WATCH_URL; delete env.COMPANY_DB_WATCH_WRITER_URL; delete env.COMPANY_DB_URL;
    const r = spawnSync(process.execPath, [path.join(root, 'apps', 'company-db', 'watch', 'run.mjs'), '--data-dir', dir, '7'], { encoding: 'utf8', env, timeout: 60000 });
    // 子プロセスが起動できなかった (sandbox など) ときは、その原因を出す (stdout が無いまま .trim() で落ちると原因が隠れる。Codex R3 Low)
    if (r.error || typeof r.stdout !== 'string') throw new Error(`run.mjs を起動できない: ${r.error ? r.error.message : `status=${r.status} signal=${r.signal}`} ${String(r.stderr || '').slice(0, 300)}`);
    const lines = r.stdout.trim().split(/\r?\n/);
    assert.deepEqual([r.status, lines.length, lines[0].startsWith('⏭️ Company DB 見張り: 未設定')], [0, 1, true], r.stdout + r.stderr);
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
});

console.log(`\n${ok} ok / ${ng} NG`);
process.exitCode = ng ? 1 : 0;
