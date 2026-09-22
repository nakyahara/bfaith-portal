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
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';
import * as CONFIG from '../config/watch-checks.mjs';
import { plannedKeys, addDays, partialAllowed } from '../apps/company-db/watch/checks.mjs';
import { runWatch, reconcileIssues, pickItems, MAX_GENERATION_RETRIES } from '../apps/company-db/watch/engine.mjs';
import { writeEvidence, readEvidence, purgeOldEvidence, EVIDENCE_KEEP_DAYS } from '../apps/company-db/push/evidence.mjs';
import { roleStatements, createRoles, urlFor, verifyRole, WATCH_TABLES } from './company-db/create-watch-roles.mjs';
import { parseArgs } from '../apps/company-db/watch/run.mjs';

const root = path.join(path.dirname(fileURLToPath(import.meta.url)), '..');
let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e).split('\n').filter((l) => !/^\s+at /.test(l)).slice(0, 30).join('\n      ') + (e && e.detail ? '\n      detail: ' + e.detail : '')); } };
const quiet = () => {};

const pg = new PGlite();
const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
const one = async (sql, p = []) => (await pg.query(sql, p)).rows[0];
const all = async (sql, p = []) => (await pg.query(sql, p)).rows;

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
  const runId = `sd_${mall}_${++seq}`;
  await pg.query(`insert into mart.sales_daily_runs (run_id, company_id, mall, scope_key, session_id, started_at, finished_at, n_dates, n_rows, n_orders) values ($1, 1, $2, $3, 's', now(), now(), 1, 0, 0)`, [runId, mall, scope]);
  await pg.query(`insert into mart.sales_daily_published (company_id, mall, scope_key, date_jst, run_id) values (1, $1, $2, $3::date, $4)`, [mall, scope, day, runId]);
}
const orderRun = async (runId, { status = 'success', complete = true } = {}) => pg.query(`insert into ops.ingest_runs (ingest_run_id, source_system, entity, scope_key, host, started_at, finished_at, status, complete, rows_seen, source_tz) values ($1, 'rakuten', 'orders', 'main', 'test', now(), now(), $2, $3, 1, 'UTC')`, [runId, status, complete]);
const ev = (mall, scope, extra = {}) => ({ name: `orders-${mall}`, kind: 'orders', mall, scope, mode: 'incremental', sync_run_id: SYNC, ok: true, push_ok: true, locked: false, run_id: null, batch_seq: 1, started_at: '2026-09-22T22:05:00Z', scanned: 100, in_scope: 100, unchanged: 100, changed: 0, applied: 0, same: 0, stale: 0, failed: 0, transform_errors: 0, sales: { ok: true, complete: true, dates: 0, skipped: null, error: null }, written_at: '2026-09-22T22:06:00Z', ...extra });
const goodEvidence = () => Object.fromEntries(CONFIG.ORDER_MALLS.map((m) => [`orders-${m.mall}`, ev(m.mall, m.scope)]));
const run = (opts = {}) => runWatch({ db, writer: opts.dryRun ? null : (opts.writer || db), config: opts.config || CONFIG, asOf: opts.asOf || ASOF, evidence: opts.evidence ?? goodEvidence(), now: opts.now || NOW, host: 'test', log: opts.log || quiet, syncRunId: 'syncRunId' in opts ? opts.syncRunId : SYNC, hooks: opts.hooks });
const verdictOf = (r, id, scope) => { const x = r.results.find((y) => y.checkId === id && y.scopeKey === scope); return x ? x.verdict : undefined; };
const resultOf = (r, id, scope) => r.results.find((y) => y.checkId === id && y.scopeKey === scope);

// ── 「そろった朝」の見本: 在庫 4 scope が complete (fba_us は partial = 例外)・差が done・注文は変更ゼロ・売上日次は閉じている
async function seedGoodMorning() {
  for (const s of CONFIG.STOCK_SCOPES) for (let n = -8; n <= 0; n++) await capture(D(s.dayOffset + n), s.source, s.scope, s.source === 'fba_us' ? 'partial' : 'complete');
  for (let n = -7; n <= -1; n++) await diffDay(D(n), 'done', { events: 10 });
  for (const m of CONFIG.ORDER_MALLS) await salesState(m.mall, m.scope);
}

console.log('定義と評価キー');
await t('評価キーは scope に展開した後の数 (4 + 4 + 1 + 5 + 5 = 19)。定義の版・順番・depends', () => {
  const keys = plannedKeys(CONFIG);
  assert.equal(keys.length, 19);
  assert.deepEqual(CONFIG.CHECKS.map((c) => c.id), ['W1', 'W2', 'W3', 'W7', 'W9']);
  assert.deepEqual([CONFIG.checkById('W3').depends, CONFIG.checkById('W9').depends, CONFIG.checkById('W2').issuePerItem], [['W1'], ['W7'], true]);
  assert.equal(CONFIG.CHECKS_VERSION, 'v2');
  for (const s of CONFIG.STOCK_SCOPES) if (s.since) assert.match(s.since, /^\d{4}-\d{2}-\d{2}$/, `${s.source} の since は YYYY-MM-DD`);
});
await t('partial の例外は期限つき (until を過ぎたら効かない)', () => {
  const s = CONFIG.STOCK_SCOPES.find((x) => x.source === 'fba_us');
  assert.deepEqual([partialAllowed(s, '2026-12-31'), partialAllowed(s, '2027-01-01'), partialAllowed(CONFIG.STOCK_SCOPES[0], ASOF)], [true, false, false]);
});

console.log('そろった朝');
await seedGoodMorning();
await t('🚨 全部 pass・案件なし・run が保存される (予定 19 / 完了 19)。fba_us の partial は例外として pass (理由が観測値に残る)', async () => {
  const r = await run();
  assert.deepEqual([r.counts.pass, r.counts.breach, r.counts.blocked, r.counts.execution_error, r.counts.completed, r.counts.planned, r.exitCode], [19, 0, 0, 0, 19, 19, 0], JSON.stringify(r.results.filter((x) => x.verdict !== 'pass').map((x) => [x.checkId, x.scopeKey, x.verdict, x.reason])));
  assert.match(r.lastLine, /^✅ Company DB 見張り 2026-09-23: 異常 0 \(新 0 \/ 継続 0\) \/ 判定保留 0 \/ 回復 0 \/ 評価 19\/19$/);
  const us = resultOf(r, 'W1', 'fba_us/us');
  assert.deepEqual([us.observed.status, us.observed.partial_allowed, /例外/.test(us.reason)], ['partial', true, true]);
  assert.deepEqual([resultOf(r, 'W7', 'rakuten/main').observed.contract, resultOf(r, 'W3', 'logizard/main').observed.status], ['zero_change', 'done']);
  const runRow = await one(`select planned_keys, completed_keys, summary, last_line, evidence from ops.watch_runs where watch_run_id = $1`, [r.runId]);
  assert.deepEqual([runRow.planned_keys, runRow.completed_keys, runRow.summary.pass, Object.keys(runRow.evidence).length], [19, 19, 19, 5]);
  assert.equal((await one(`select count(*)::int as n from ops.watch_results where watch_run_id = $1`, [r.runId])).n, 19);
  assert.equal((await one(`select count(*)::int as n from ops.watch_issues`)).n, 0);
});
await t('dry-run (writer なし) は何も書かない', async () => {
  const before = (await one(`select count(*)::int as n from ops.watch_runs`)).n;
  const r = await run({ dryRun: true });
  assert.deepEqual([r.persisted, r.counts.pass, (await one(`select count(*)::int as n from ops.watch_runs`)).n], [false, 19, before]);
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
  assert.deepEqual([verdictOf(r, 'W1', 'logizard/main'), w3.verdict, w3.blockedBy, /前提 W1/.test(w3.reason), r.counts.blocked, r.counts.new], ['breach', 'blocked', 'W1:logizard/main', true, 1, 1]);   // new 1 = W1 logizard だけ (W2 の窓は logizard では D(-8)〜D(-2) = 昨日はまだ入らない。明日から)
  assert.equal((await one(`select count(*)::int as n from ops.watch_issues where check_id = 'W3' and state = 'open'`)).n, 0);
  assert.match(r.lastLine, /判定保留 1/);
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
  assert.equal(plannedKeys(withSince(D(1))).length, 19);   // since は評価キーを減らさない
  for (let n = -7; n <= -4; n++) await capture(D(n), 'ne', 'main', 'complete');
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
  assert.equal(r.counts.blocked, 7);
  const crashed = await run({ dryRun: true, evidence: { ...goodEvidence(), 'orders-qoo10': { kind: 'orders', mall: 'qoo10', scope: 'main', sync_run_id: SYNC, ok: false, error: 'DB が壊れている' } } });
  assert.deepEqual([verdictOf(crashed, 'W7', 'qoo10/main'), /push が落ちた/.test(resultOf(crashed, 'W7', 'qoo10/main').reason)], ['breach', true]);
  const good = await run({ dryRun: true, evidence: { ...goodEvidence(), 'orders-linegift': ev('linegift', 'main', { changed: 3, applied: 3, run_id: 'run_ok' }) } });
  assert.deepEqual([verdictOf(good, 'W7', 'linegift/main'), resultOf(good, 'W7', 'linegift/main').observed.run.status, resultOf(good, 'W7', 'linegift/main').inputGeneration.run_id], ['pass', 'success', 'run_ok']);
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
  assert.deepEqual([r.attempts, r.unstable, r.counts.pass, r.counts.blocked], [2, false, 19, 0]);
  r = await run({ hooks: { afterSnapshot: async () => { await bump(); } } });
  assert.deepEqual([r.attempts, r.unstable, r.counts.pass, r.counts.blocked, /世代が変わり続けた/.test(r.lastLine)], [MAX_GENERATION_RETRIES, true, 0, 19, true]);
  assert.match(resultOf(r, 'W1', 'ne/main').reason, /世代が変わり続けた/);
  assert.equal((await one(`select count(*)::int as n from ops.watch_issues where state = 'open'`)).n, 0);   // blocked = 案件に触らない
  const saved = await one(`select summary->>'attempts' as a, summary->>'unstable' as u, last_line from ops.watch_runs where watch_run_id = $1`, [r.runId]);
  assert.deepEqual([saved.a, saved.u, /世代が変わり続けた/.test(saved.last_line)], [String(MAX_GENERATION_RETRIES), 'true', true]);
  r = await run();
  assert.deepEqual([r.attempts, r.counts.pass], [1, 19]);
});
await t('🚨 Codex R2 #1: run が増えない途中 chunk で注文が増えた (未公開の日) のを世代の確認が見つける (指紋は W9 と同じ「日ごとの注文の有無」)', async () => {
  await orderRun('r2_running', { status: 'running', complete: false });
  let r = await run({ hooks: { afterSnapshot: async (n) => { if (n === 1) await order('aupay', 'main', D(-1), 'r2-new'); } } });
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
  assert.deepEqual([r.counts.execution_error, r.counts.pass, r.exitCode], [19, 0, 1]);
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
