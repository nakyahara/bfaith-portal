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
import { runWatch, reconcileIssues, pickItems } from '../apps/company-db/watch/engine.mjs';
import { writeEvidence, readEvidence, purgeOldEvidence, EVIDENCE_KEEP_DAYS } from '../apps/company-db/push/evidence.mjs';
import { roleStatements, urlFor, WATCH_TABLES } from './company-db/create-watch-roles.mjs';
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
const ev = (mall, scope, extra = {}) => ({ name: `orders-${mall}`, kind: 'orders', mall, scope, mode: 'incremental', ok: true, push_ok: true, locked: false, run_id: null, batch_seq: 1, started_at: '2026-09-22T22:05:00Z', scanned: 100, in_scope: 100, unchanged: 100, changed: 0, applied: 0, same: 0, stale: 0, failed: 0, transform_errors: 0, sales: { ok: true, complete: true, dates: 0, skipped: null, error: null }, written_at: '2026-09-22T22:06:00Z', ...extra });
const goodEvidence = () => Object.fromEntries(CONFIG.ORDER_MALLS.map((m) => [`orders-${m.mall}`, ev(m.mall, m.scope)]));
const run = (opts = {}) => runWatch({ db, writer: opts.dryRun ? null : db, config: opts.config || CONFIG, asOf: opts.asOf || ASOF, evidence: opts.evidence ?? goodEvidence(), now: opts.now || NOW, host: 'test', log: quiet });
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
  assert.equal(CONFIG.CHECKS_VERSION, 'v1');
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
  await assert.rejects(runWatch({ db: pgliteAdapter(pg2), writer: pgliteAdapter(pg2), config: CONFIG, asOf: ASOF, evidence: {}, now: NOW, log: quiet }), /0023/);   // 記録しようとすれば止まる
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
  const crashed = await run({ dryRun: true, evidence: { ...goodEvidence(), 'orders-qoo10': { kind: 'orders', mall: 'qoo10', scope: 'main', ok: false, error: 'DB が壊れている' } } });
  assert.deepEqual([verdictOf(crashed, 'W7', 'qoo10/main'), /push が落ちた/.test(resultOf(crashed, 'W7', 'qoo10/main').reason)], ['breach', true]);
  const good = await run({ dryRun: true, evidence: { ...goodEvidence(), 'orders-linegift': ev('linegift', 'main', { changed: 3, applied: 3, run_id: 'run_ok' }) } });
  assert.deepEqual([verdictOf(good, 'W7', 'linegift/main'), resultOf(good, 'W7', 'linegift/main').observed.run.status, resultOf(good, 'W7', 'linegift/main').inputGeneration.run_id], ['pass', 'success', 'run_ok']);
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
    const p = writeEvidence(dir, 'orders-rakuten', { kind: 'orders', changed: 3 }, { now: NOW, warn: quiet });
    assert.ok(p && p.endsWith(path.join('2026-09-23', 'orders-rakuten.json')));
    writeEvidence(dir, 'orders-rakuten', { kind: 'orders', changed: 5 }, { now: NOW, warn: quiet });   // 上書き = 再実行した回が正
    const got = readEvidence(dir, '2026-09-23');
    assert.deepEqual([got['orders-rakuten'].changed, got['orders-rakuten'].name, got['orders-rakuten'].date, typeof got['orders-rakuten'].written_at], [5, 'orders-rakuten', '2026-09-23', 'string']);
    const warns = [];
    assert.equal(writeEvidence(dir, 'bad name!', {}, { now: NOW, warn: (m) => warns.push(m) }), null);
    assert.equal(writeEvidence('', 'x', {}, { now: NOW, warn: (m) => warns.push(m) }), null);
    assert.equal(warns.length, 2);
    fs.mkdirSync(path.join(dir, 'company-db-evidence', '2026-09-01'), { recursive: true });
    fs.writeFileSync(path.join(dir, 'company-db-evidence', '2026-09-23', 'broken.json'), '{not json');
    assert.equal(purgeOldEvidence(dir, { now: NOW, keepDays: EVIDENCE_KEEP_DAYS }), 1);
    assert.deepEqual([fs.existsSync(path.join(dir, 'company-db-evidence', '2026-09-01')), /読めない/.test(readEvidence(dir, '2026-09-23').broken.error), Object.keys(readEvidence(dir, '2026-01-01')).length], [false, true, 0]);
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
});
await t('送り手の証跡の形 (mall-orders の evidenceOf): 件数だけ・注文の中身は入らない', async () => {
  const { evidenceOf } = await import('../apps/company-db/push/mall-orders.mjs');
  const r = { ok: true, mode: 'incremental', runId: 'run1', batchSeq: 7, scanned: 10, inScope: 9, unchanged: 8, changed: 1, sent: 1, applied: 1, same: 0, stale: 0, failed: [], transformErrors: [], lockedBy: null, ledgerReset: null, ledgerRebuilt: 0 };
  const e = evidenceOf('rakuten', r, { startedAt: NOW, success: true, relink: { ran: true, error: null, result: { linked: 2 }, pending: false }, sales: { ok: true, complete: true, dates: 2 } });
  assert.deepEqual([e.kind, e.mall, e.scope, e.push_ok, e.run_id, e.changed, e.failed, e.transform_errors, e.relink.linked, e.sales.dates, 'rows' in e], ['orders', 'rakuten', 'main', true, 'run1', 1, 0, 0, 2, 2, false]);
});

console.log('ロールと CLI');
await t('ロールの SQL: watcher は select だけ・schema を限定した default privileges・writer は ops.watch_* の insert と限定 update・security definer の public execute を外す', () => {
  const s = roleStatements({ dbName: 'cdb', owner: 'cdb_user', watcherPw: "p'w", writerPw: 'w', secdefFunctions: ['core.resolve_listing_id(smallint, text, text)'] }).join('\n');
  for (const frag of ["alter role watcher with login password 'p''w' nosuperuser nocreatedb nocreaterole noinherit connection limit 3", "alter role watcher set default_transaction_read_only = on", 'grant select on all tables in schema core to watcher',
    'alter default privileges for role cdb_user in schema mart grant select on tables to watcher', 'revoke execute on function core.resolve_listing_id(smallint, text, text) from public', 'grant execute on function core.resolve_listing_id(smallint, text, text) to cdb_user',
    'grant select, insert on ops.watch_issues to watch_writer', 'grant update (finished_at, completed_keys, summary, last_line) on ops.watch_runs to watch_writer', 'grant usage on all sequences in schema ops to watch_writer']) assert.ok(s.includes(frag), frag);
  assert.ok(!/grant (insert|update|delete).* to watcher/.test(s) && !/grant .* on core.* to watch_writer/.test(s) && !/delete/.test(s));
  assert.deepEqual(WATCH_TABLES, ['watch_runs', 'watch_results', 'watch_issues', 'watch_result_items']);
  assert.equal(urlFor('postgres://u:p@host:5432/cdb?sslmode=require', 'watcher', 'x/y'), 'postgres://watcher:x%2Fy@host:5432/cdb?sslmode=require');
  assert.throws(() => roleStatements({ dbName: 'bad-name', owner: 'u', watcherPw: 'a', writerPw: 'b' }), /識別子/);
});
await t('CLI: 引数 (daily-sync の "7" を許す) / env が無ければ ⏭️ で exit 0・出力 1 行', () => {
  assert.deepEqual(parseArgs(['--as-of', '2026-09-23', '7', '--dry-run']), { dataDir: null, asOf: '2026-09-23', dryRun: true, json: false });
  assert.throws(() => parseArgs(['--as-of', '2026/09/23']), /YYYY-MM-DD/);
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-watch-'));
  try {
    const env = { ...process.env }; delete env.COMPANY_DB_WATCH_URL; delete env.COMPANY_DB_WATCH_WRITER_URL; delete env.COMPANY_DB_URL;
    const r = spawnSync(process.execPath, [path.join(root, 'apps', 'company-db', 'watch', 'run.mjs'), '--data-dir', dir, '7'], { encoding: 'utf8', env, timeout: 60000 });
    const lines = r.stdout.trim().split(/\r?\n/);
    assert.deepEqual([r.status, lines.length, lines[0].startsWith('⏭️ Company DB 見張り: 未設定')], [0, 1, true], r.stdout + r.stderr);
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
});

console.log(`\n${ok} ok / ${ng} NG`);
process.exitCode = ng ? 1 : 0;
