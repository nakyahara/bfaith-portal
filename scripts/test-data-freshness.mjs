#!/usr/bin/env node
/**
 * test-data-freshness.mjs — データの新しさの見張り (apps/warehouse/monitor-data-freshness.js・2026-10-24) と
 *   モールCSV自動取得の最後の回の記録 (scripts/mall-csv-fetcher/lib-fetch-status.mjs) の試験。本番 DB・API・GChat には繋がない (:memory: の SQLite と見本の送り手)
 *   - 日付の読み方 (YYYY-MM-DD / 時刻つき / Z・+0900 / YYYY/MM/DD / 月)
 *   - ふつう・止まった・まだ始まっていない (表が無い・空)・読めない
 *   - 意図して止めている (台帳の temporary_asset・期限内 / 期限切れ / MALL_FETCH_ONLY だけで台帳に無い / 止めていても新しい = ふつう)
 *   - 週次・月次の表 (しきい値が長い)
 *   - 同じものを毎日鳴らさない (新しく止まった日 → 7 日ごとの念押し → 戻った・送れなかった日は翌朝もう一度)
 *   - 今朝の自動取得の exit 3 (ログイン切れ) が要約に ⚠️ で出る・記録が古い・無い
 *   - 一覧の表と列がコードの CREATE TABLE にある・台帳の id・取得の名前が実在する
 *   - daily-sync への組み込み (置き場所・再試行に載せない・写しの反映の門で止めない)・fetch-all が記録を書く
 * 使い方: node scripts/test-data-freshness.mjs
 */
import { temporaryTestRoot } from './test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url, 'test-data-freshness-');

const assert = (await import('node:assert/strict')).default;
const fs = await import('node:fs');
const os = await import('node:os');
const path = await import('node:path');
const { fileURLToPath } = await import('node:url');
const Database = (await import('better-sqlite3')).default;
const F = await import('../apps/warehouse/monitor-data-freshness.js');
const FS = await import('./mall-csv-fetcher/lib-fetch-status.mjs');
const { isWarnSummary } = await import('../apps/warehouse/amazon-fees-outcome.js');
const { JOBS_REGISTRY } = await import('../config/jobs-registry.mjs');

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
let ok = 0, ng = 0;
async function t(name, fn) { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message)); } }

const NOW = Date.parse('2026-10-24T08:00:00+09:00');   // JST 2026-10-24 (土)
const DAY = 86400000;
const day = (n) => new Date(NOW + 9 * 3600000 - n * DAY).toISOString().slice(0, 10);   // n 日前 (JST)

/** 小さな一覧 (本物の FEEDS を使う) */
const T = (feed, table, column, maxAgeDays, kind = 'date') => ({ ...F.FRESHNESS_TABLES.find((x) => x.feed === feed), table, column, kind, maxAgeDays });
const yahooTables = ['fact_yahoo_store_device_daily', 'fact_yahoo_inflow_daily'].map((n) => T('yahoo-data', n, 'date_jst', 4));
const TABLES = [
  ...yahooTables,
  T('qoo10-data', 'fact_qoo10_cvr_daily', 'date_jst', 4),
  T('rakuten-rpp', 'fact_rakuten_ads_rpp_daily', 'date_jst', 4),
  T('rakuten-rpp', 'fact_rakuten_ads_rpp', 'month_ym', 40, 'month'),
  T('aupay-data', 'fact_aupay_pm_query_weekly', 'date_jst', 21),
  T('aupay-data', 'fact_aupay_sales_monthly', 'date_jst', 70),
  T('amazon-traffic', 'amazon_sales_traffic_daily', 'date_jst', 5),
  T('amazon-returns', 'amazon_fba_returns', 'fetched_at', 3),
  T('orders-amazon', 'raw_sp_orders', 'purchase_date', 3),
];
/** :memory: の DB に一覧の表を作り、表ごとの値を入れる (vals = { 表: [値…] }。undefined = 表を作らない) */
function makeDb(vals) {
  const db = new Database(':memory:');
  for (const tb of TABLES) {
    const v = vals[tb.table];
    if (v === undefined) continue;
    db.exec(`CREATE TABLE ${tb.table} ("${tb.column}" TEXT)`);
    const ins = db.prepare(`INSERT INTO ${tb.table} ("${tb.column}") VALUES (?)`);
    for (const x of v) ins.run(x);
  }
  return db;
}
const FRESH = {
  fact_yahoo_store_device_daily: [day(3), day(1)], fact_yahoo_inflow_daily: [day(1)], fact_qoo10_cvr_daily: [day(1)],
  fact_rakuten_ads_rpp_daily: [day(2)], fact_rakuten_ads_rpp: ['2026-09', '2026-10'],
  fact_aupay_pm_query_weekly: [day(12)], fact_aupay_sales_monthly: ['2026-09-01'],
  amazon_sales_traffic_daily: [day(3)], amazon_fba_returns: [new Date(NOW - 20 * 3600000).toISOString()], raw_sp_orders: ['2026-10-23T22:10:00Z'],
};
const NO_PAUSE = [];   // 台帳に止めている項目が無い
const REG_PAUSE = (removeBy) => [{ id: 'mall-fetch-skip-rakuten-blocked', type: 'temporary_asset', remove_by: removeBy }];
const NO_FETCH = { status: null, problem: 'missing' };
const statusOf = (r, table) => r.items.find((i) => i.table === table).status;

function sender() {
  const sent = [];
  const fn = async (webhook, text) => { sent.push(text); return fn.ok; };
  fn.ok = true;
  fn.sent = sent;
  return fn;
}
const run = (db, o = {}) => F.runFreshness(db, { nowMs: NOW, webhook: 'https://example.invalid/hook', tables: TABLES, registry: NO_PAUSE, fetchOnly: [], fetchStatus: NO_FETCH, log: () => {}, ...o });
/** 見る表を絞った run (日を進める試験 = 見本のほかの表が古くならないように) */
const runOnly = (...names) => (db, o = {}) => run(db, { tables: TABLES.filter((x) => names.includes(x.table)), ...o });

console.log('日付の読み方');
await t('YYYY-MM-DD・時刻つき・Z/+09:00/+0900 は JST の日・YYYY/MM/DD・月・読めない値', () => {
  assert.equal(F.toJstDay('2026-10-23'), '2026-10-23');
  assert.equal(F.toJstDay('2026-10-23 23:59:59'), '2026-10-23');                // 時差なし = JST
  assert.equal(F.toJstDay('2026-10-23T15:30:00Z'), '2026-10-24');               // UTC 15:30 = JST 0:30
  assert.equal(F.toJstDay('2026-10-23T14:59:59.123Z'), '2026-10-23');
  assert.equal(F.toJstDay('2026-10-23T23:10:00+09:00'), '2026-10-23');
  assert.equal(F.toJstDay('2026-10-23T23:10:00+0900'), '2026-10-23');           // 楽天の orderDatetime
  assert.equal(F.toJstDay('2026/10/03 12:34'), '2026-10-03');                   // au PAY の order_date
  assert.equal(F.toJstDay('2026-10', 'month'), '2026-10-01');
  assert.equal(F.toJstDay('2026-10'), null);                                     // 月は kind = month のときだけ
  for (const bad of ['', null, 'abc', '2026-13-01', '20261023', '2026-02-30']) assert.equal(F.toJstDay(bad), null, String(bad));
  assert.equal(F.daysBetween('2026-10-13', '2026-10-24'), 11);
});

console.log('ふつう・止まった・まだ始まっていない');
await t('ふつう: 全部しきい値の内 = ✅・GChat は送らない・記録は空', async () => {
  const db = makeDb(FRESH);
  const s = sender();
  const r = await run(db, { send: s });
  assert.deepEqual([...new Set(r.items.map((i) => i.status))], ['ok']);
  assert.match(r.lastLine, /^✅ データの新しさ: 止まっているもの無し/);
  assert.equal(isWarnSummary(r.lastLine), false);
  assert.equal(s.sent.length, 0);
  assert.equal(db.prepare('SELECT COUNT(*) n FROM data_freshness_alert_state').get().n, 0);
});
await t('しきい値ちょうどはふつう・1 日超えたら止まっている', () => {
  const db = makeDb({ ...FRESH, fact_qoo10_cvr_daily: [day(4)], fact_yahoo_inflow_daily: [day(5)] });
  const r = F.evaluateFreshness(db, { nowMs: NOW, tables: TABLES, registry: NO_PAUSE });
  assert.equal(statusOf(r, 'fact_qoo10_cvr_daily'), 'ok');
  assert.equal(statusOf(r, 'fact_yahoo_inflow_daily'), 'stale');
  assert.equal(r.items.find((i) => i.table === 'fact_yahoo_inflow_daily').ageDays, 5);
});
await t('止まった: Yahoo統計 10/13・Qoo10 10/3 = ⚠️・GChat 1 通 (feed ごと・人がやること・最新の日)・記録に残る', async () => {
  const db = makeDb({ ...FRESH, fact_yahoo_store_device_daily: ['2026-10-12', '2026-10-13'], fact_yahoo_inflow_daily: ['2026-10-13'], fact_qoo10_cvr_daily: ['2026-10-03'] });
  const s = sender();
  const r = await run(db, { send: s });
  assert.equal(statusOf(r, 'fact_yahoo_store_device_daily'), 'stale');
  assert.match(r.lastLine, /^⚠️ データの新しさ: 止まっている 3 表 \(Yahoo統計 最新 10\/13 = 11 日前・Qoo10分析 最新 10\/3 = 21 日前\)/);
  assert.ok(isWarnSummary(r.lastLine));
  assert.equal(s.sent.length, 1);
  const m = s.sent[0];
  assert.match(m, /^⚠️ \*データが止まっている\* \(2026-10-24 daily-sync/);
  assert.match(m, /🆕 \*新しく止まった\*/);
  assert.match(m, /\*Yahoo統計\* \(Yahoo・2 表\) — 最新 10\/13 \(11 日前・しきい値 4 日\)/);
  assert.match(m, /Yahoo-Relogin\.bat/);
  assert.match(m, /\*Qoo10分析\* \(Qoo10・1 表\) — 最新 10\/3 \(21 日前/);
  assert.match(m, /qoo10-login-spike\.mjs/);
  assert.equal(m.split('Yahoo統計').length - 1, 1, 'Yahoo統計 は 1 行にまとめる');
  const st = db.prepare('SELECT item_key, status, since_on, last_notified_on FROM data_freshness_alert_state ORDER BY item_key').all();
  assert.deepEqual(st.map((x) => [x.item_key, x.status, x.since_on, x.last_notified_on]), [
    ['table:fact_qoo10_cvr_daily', 'stale', '2026-10-24', '2026-10-24'],
    ['table:fact_yahoo_inflow_daily', 'stale', '2026-10-24', '2026-10-24'],
    ['table:fact_yahoo_store_device_daily', 'stale', '2026-10-24', '2026-10-24'],
  ]);
});
await t('まだ始まっていない: 表が無い・行が無い (空文字だけも) = 止まった扱いにしない (Amazon の新しい表)', async () => {
  const db = makeDb({ ...FRESH, amazon_sales_traffic_daily: undefined, amazon_fba_returns: ['', '  '] });
  const s = sender();
  const r = await run(db, { send: s });
  assert.equal(statusOf(r, 'amazon_sales_traffic_daily'), 'not_started');
  assert.equal(r.items.find((i) => i.table === 'amazon_sales_traffic_daily').note, '表が無い');
  assert.equal(statusOf(r, 'amazon_fba_returns'), 'not_started');
  assert.match(r.lastLine, /^✅ .*まだ始まっていない 2 表/);
  assert.equal(s.sent.length, 0);
});
await t('読めない: 列が無い・日付として読めない値 = ⚠️ (一覧の誤り)', async () => {
  const db = makeDb({ ...FRESH, fact_qoo10_cvr_daily: ['20261023'] });
  db.exec('DROP TABLE fact_yahoo_inflow_daily; CREATE TABLE fact_yahoo_inflow_daily (other TEXT)');
  const s = sender();
  const r = await run(db, { send: s });
  assert.equal(statusOf(r, 'fact_yahoo_inflow_daily'), 'unreadable');
  assert.equal(statusOf(r, 'fact_qoo10_cvr_daily'), 'unreadable');
  assert.ok(isWarnSummary(r.lastLine));
  assert.match(s.sent[0], /❓ \*Qoo10分析\* — 読めない: fact_qoo10_cvr_daily\.date_jst \(日付として読めない値: 20261023\)/);
});

console.log('意図して止めている');
const RAKUTEN_OLD = { ...FRESH, fact_rakuten_ads_rpp_daily: ['2026-07-14'], fact_rakuten_ads_rpp: ['2026-07'] };
await t('台帳に temporary_asset があり期限内 = ⏸️ 止めている (短く・✅ のまま・GChat は送らない)', async () => {
  const db = makeDb(RAKUTEN_OLD);
  const s = sender();
  const r = await run(db, { send: s, registry: REG_PAUSE('2026-11-30') });
  assert.equal(statusOf(r, 'fact_rakuten_ads_rpp_daily'), 'paused');
  assert.equal(statusOf(r, 'fact_rakuten_ads_rpp'), 'paused');
  assert.match(r.lastLine, /^✅ データの新しさ: 止まっているもの無し \/ ⏸️ 止めている: 楽天RPP広告 \(台帳 mall-fetch-skip-rakuten-blocked・期限 2026-11-30\)/);
  assert.equal(s.sent.length, 0);
});
await t('台帳の期限 (remove_by) を過ぎた = ⚠️ で目立たせる・GChat は feed ごとに 1 つ・翌日は念押ししない', async () => {
  const db = makeDb(RAKUTEN_OLD);
  const s = sender();
  const r = await run(db, { send: s, registry: REG_PAUSE('2026-09-30') });
  assert.equal(statusOf(r, 'fact_rakuten_ads_rpp_daily'), 'paused_overdue');
  assert.match(r.lastLine, /^⚠️ データの新しさ: ⏸️ 止めている: 楽天RPP広告 \(台帳 mall-fetch-skip-rakuten-blocked の期限 2026-09-30 を過ぎた\)/);
  assert.equal(s.sent.length, 1);
  assert.match(s.sent[0], /⏸️ \*楽天RPP広告\* — 止めている \(台帳の期限切れ\): 台帳 mall-fetch-skip-rakuten-blocked の期限 2026-09-30 を 24 日過ぎた \(最新 7\/14\)/);
  assert.match(s.sent[0], /サービス別権限設定/);
  assert.deepEqual(db.prepare('SELECT item_key FROM data_freshness_alert_state').all().map((x) => x.item_key), ['pause:mall-fetch-skip-rakuten-blocked']);
  const r2 = await run(db, { send: s, registry: REG_PAUSE('2026-09-30'), nowMs: NOW + DAY });
  assert.equal(s.sent.length, 1, '翌日は送らない');
  assert.ok(isWarnSummary(r2.lastLine), '要約は毎朝 ⚠️');
});
await t('MALL_FETCH_ONLY で外している (台帳なし) = ⚠️ 台帳に無い / 外していない取得は止まった扱い / 止めていても新しい = ふつう', async () => {
  const db = makeDb({ ...RAKUTEN_OLD, fact_qoo10_cvr_daily: ['2026-10-03'] });
  const only = ['rakuten-review', 'yahoo', 'yahoo-review', 'aupay'];   // qoo10・rakuten を外している
  const r = F.evaluateFreshness(db, { nowMs: NOW, tables: TABLES, registry: NO_PAUSE, fetchOnly: only });
  assert.equal(statusOf(r, 'fact_qoo10_cvr_daily'), 'paused_unregistered');
  assert.equal(statusOf(r, 'fact_rakuten_ads_rpp_daily'), 'paused_unregistered');
  assert.equal(statusOf(r, 'fact_yahoo_inflow_daily'), 'ok');
  const r2 = F.evaluateFreshness(db, { nowMs: NOW, tables: TABLES, registry: REG_PAUSE('2026-11-30'), fetchOnly: only });
  assert.equal(statusOf(r2, 'fact_rakuten_ads_rpp_daily'), 'paused', '台帳と env の両方 = 止めている (期限内)');
  // 手で入れた = 止めていても新しい = ふつう
  const db3 = makeDb(FRESH);
  assert.equal(statusOf(F.evaluateFreshness(db3, { nowMs: NOW, tables: TABLES, registry: REG_PAUSE('2026-09-30') }), 'fact_rakuten_ads_rpp_daily'), 'ok');
  // 台帳の項目が消えた (止めるのをやめた) = ふつうに止まった扱い
  assert.equal(statusOf(F.evaluateFreshness(makeDb(RAKUTEN_OLD), { nowMs: NOW, tables: TABLES, registry: NO_PAUSE }), 'fact_rakuten_ads_rpp_daily'), 'stale');
  const line = F.buildSummaryLine({ items: r.items, fetch: null });
  assert.match(line, /^⚠️ .*⏸️ 止めている: Qoo10分析・楽天RPP広告 \(MALL_FETCH_ONLY で外しているが台帳に無い\)/);
});

await t('同じ台帳の項目で止めている 2 つの取得 (楽天RPP・データ分析) は 1 行・記録も 1 つ', async () => {
  const two = [...TABLES, T('rakuten-data', 'fact_rakuten_store_daily', 'date_jst', 4)];
  const db = makeDb(RAKUTEN_OLD);
  db.exec(`CREATE TABLE fact_rakuten_store_daily (date_jst TEXT); INSERT INTO fact_rakuten_store_daily VALUES ('2026-07-15')`);
  const s = sender();
  await run(db, { send: s, tables: two, registry: REG_PAUSE('2026-09-30') });
  assert.match(s.sent[0], /⏸️ \*楽天RPP広告・楽天データ分析\* — 止めている \(台帳の期限切れ\)[^\n]*\(最新 7\/15\)/);
  assert.equal(s.sent[0].split('サービス別権限設定').length - 1, 1, '人がやることは 1 回だけ');
  assert.equal(db.prepare('SELECT COUNT(*) n FROM data_freshness_alert_state').get().n, 1);
});
await t('feed の「最新」は日次の表の日 (月次の古い日を最新と言わない)', async () => {
  const db = makeDb({ ...FRESH, fact_aupay_pm_query_weekly: ['2026-09-01'], fact_aupay_sales_monthly: ['2026-06-01'] });
  const s = sender();
  const r = await run(db, { send: s });
  assert.match(s.sent[0], /\*au PAY分析\* \(au PAY・2 表\) — 最新 9\/1 \(53 日前・しきい値 21 日\)/);
  assert.match(r.lastLine, /au PAY分析 最新 9\/1 = 53 日前/);
});

console.log('週次・月次');
await t('週次・月次の境目 (日数で確かめる)', () => {
  const ev = (v) => F.evaluateFreshness(makeDb({ ...FRESH, ...v }), { nowMs: NOW, tables: TABLES, registry: NO_PAUSE });
  assert.equal(F.daysBetween('2026-08-15', '2026-10-24'), 70);
  assert.equal(statusOf(ev({ fact_aupay_sales_monthly: ['2026-08-15'] }), 'fact_aupay_sales_monthly'), 'ok');
  assert.equal(statusOf(ev({ fact_aupay_sales_monthly: ['2026-08-14'] }), 'fact_aupay_sales_monthly'), 'stale');
  assert.equal(statusOf(ev({ fact_aupay_sales_monthly: ['2026-09-01'] }), 'fact_aupay_sales_monthly'), 'ok');   // 月初の前月 (53 日前)
  assert.equal(statusOf(ev({ fact_rakuten_ads_rpp: ['2026-10'] }), 'fact_rakuten_ads_rpp'), 'ok');               // 今月 (23 日前)
  assert.equal(statusOf(ev({ fact_rakuten_ads_rpp: ['2026-09'] }), 'fact_rakuten_ads_rpp'), 'stale');            // 先月 (53 日前 > 40)
  assert.equal(statusOf(ev({ fact_aupay_pm_query_weekly: [day(21)] }), 'fact_aupay_pm_query_weekly'), 'ok');
  assert.equal(statusOf(ev({ fact_aupay_pm_query_weekly: [day(22)] }), 'fact_aupay_pm_query_weekly'), 'stale');
  // 時刻の列 (取得の時刻・UTC): 3 日しきい値
  assert.equal(statusOf(ev({ amazon_fba_returns: [new Date(NOW - 3 * DAY).toISOString()] }), 'amazon_fba_returns'), 'ok');
  assert.equal(statusOf(ev({ amazon_fba_returns: [new Date(NOW - 4 * DAY).toISOString()] }), 'amazon_fba_returns'), 'stale');
});

console.log('同じものを毎日鳴らさない');
await t('新しく止まった日に 1 通 → 翌日〜6 日後は送らない (要約は ⚠️) → 7 日後に念押し → 戻ったら 1 回「戻った」・記録は消える', async () => {
  const STALE = { ...FRESH, fact_yahoo_store_device_daily: ['2026-10-13'], fact_yahoo_inflow_daily: ['2026-10-13'] };
  const run = runOnly('fact_yahoo_store_device_daily', 'fact_yahoo_inflow_daily');   // 日が進むと見本のほかの表も古くなる = 見る表を絞る
  const db = makeDb(STALE);
  const s = sender();
  await run(db, { send: s });
  assert.equal(s.sent.length, 1);
  for (let d = 1; d <= 6; d++) {
    const r = await run(db, { send: s, nowMs: NOW + d * DAY });
    assert.equal(s.sent.length, 1, `${d} 日後は送らない`);
    assert.ok(isWarnSummary(r.lastLine));
    assert.match(r.lastLine, /GChat は 7 日ごとの念押しの日まで送らない/);
  }
  const r7 = await run(db, { send: s, nowMs: NOW + 7 * DAY });
  assert.equal(s.sent.length, 2);
  assert.match(s.sent[1], /🔁 \*止まったまま \(7 日ごとの念押し\)\*/);
  assert.match(s.sent[1], /\(10\/24 から\)/);
  assert.doesNotMatch(s.sent[1], /🆕/);
  assert.equal(db.prepare("SELECT since_on FROM data_freshness_alert_state WHERE item_key = 'table:fact_yahoo_inflow_daily'").get().since_on, '2026-10-24', '止まった日は変えない');
  assert.match(r7.lastLine, /GChat に送った \(2 件\)/);
  // 戻った (取込が動いた)
  db.exec(`INSERT INTO fact_yahoo_store_device_daily (date_jst) VALUES ('2026-10-30'); INSERT INTO fact_yahoo_inflow_daily (date_jst) VALUES ('2026-10-30')`);
  const rb = await run(db, { send: s, nowMs: NOW + 8 * DAY });
  assert.equal(s.sent.length, 3);
  assert.match(s.sent[2], /^✅ \*止まっていたデータが戻った\*/);
  assert.match(s.sent[2], /✅ \*戻った\*: Yahoo統計/);
  assert.match(rb.lastLine, /^✅/);
  assert.equal(db.prepare('SELECT COUNT(*) n FROM data_freshness_alert_state').get().n, 0);
  await run(db, { send: s, nowMs: NOW + 9 * DAY });
  assert.equal(s.sent.length, 3, '戻ったのは 1 回だけ');
});
await t('別の表が新しく止まった日は、その表だけ 🆕・前からのものは「ほかに止まったまま」に名前だけ', async () => {
  const run = runOnly('fact_yahoo_inflow_daily', 'fact_qoo10_cvr_daily');
  const db = makeDb({ ...FRESH, fact_yahoo_inflow_daily: ['2026-10-13'] });
  const s = sender();
  await run(db, { send: s });
  db.exec(`DELETE FROM fact_qoo10_cvr_daily; INSERT INTO fact_qoo10_cvr_daily (date_jst) VALUES ('${day(6)}')`);
  await run(db, { send: s, nowMs: NOW + 2 * DAY });
  assert.equal(s.sent.length, 2);
  assert.match(s.sent[1], /🆕 \*新しく止まった\*\n• \*Qoo10分析\*/);
  assert.match(s.sent[1], /ほかに止まったまま \(今日は念押ししない\): Yahoo統計 \(10\/24 から\)/);
  assert.doesNotMatch(s.sent[1], /Yahoo-Relogin/);
});
await t('送れなかった日・GCHAT_WEBHOOK が無い日は記録しない = 翌朝もう一度 (止まった日は最初の日のまま)', async () => {
  const run = runOnly('fact_qoo10_cvr_daily');
  const db = makeDb({ ...FRESH, fact_qoo10_cvr_daily: ['2026-10-03'] });
  const s = sender();
  s.ok = false;
  const r = await run(db, { send: s });
  assert.equal(r.status, 'send_failed');
  assert.match(r.lastLine, /GChat に送れなかった = 明日もう一度/);
  const r0 = await run(db, { send: s, webhook: undefined, nowMs: NOW + DAY });
  assert.equal(r0.status, 'no_webhook');
  s.ok = true;
  await run(db, { send: s, nowMs: NOW + 2 * DAY });
  assert.equal(s.sent.length, 2, '1 回目は失敗・2 日目は webhook なし (送らない)・3 日目に送れた');
  assert.match(s.sent[1], /🆕 \*新しく止まった\*/);
  const st = db.prepare("SELECT since_on, last_notified_on FROM data_freshness_alert_state WHERE item_key = 'table:fact_qoo10_cvr_daily'").get();
  assert.deepEqual([st.since_on, st.last_notified_on], ['2026-10-24', '2026-10-26']);
  await run(db, { send: s, nowMs: NOW + 3 * DAY });
  assert.equal(s.sent.length, 2);
});
await t('「戻った」を送れなかった = 記録を残して翌朝もう一度 / 知らせていないものは黙って消す', async () => {
  const run = runOnly('fact_qoo10_cvr_daily');
  const db = makeDb({ ...FRESH, fact_qoo10_cvr_daily: ['2026-10-03'] });
  const s = sender();
  await run(db, { send: s });
  db.exec(`INSERT INTO fact_qoo10_cvr_daily (date_jst) VALUES ('${day(0)}')`);
  s.ok = false;
  await run(db, { send: s, nowMs: NOW + DAY });
  assert.equal(db.prepare('SELECT COUNT(*) n FROM data_freshness_alert_state').get().n, 1);
  s.ok = true;
  await run(db, { send: s, nowMs: NOW + 2 * DAY });
  assert.match(s.sent[s.sent.length - 1], /戻った\*: Qoo10分析/);
  assert.equal(db.prepare('SELECT COUNT(*) n FROM data_freshness_alert_state').get().n, 0);
  // webhook なしで記録だけされた (知らせていない) ものは、戻ったら黙って消える
  const db2 = makeDb({ ...FRESH, fact_qoo10_cvr_daily: ['2026-10-03'] });
  await run(db2, { send: s, webhook: undefined });
  db2.exec(`INSERT INTO fact_qoo10_cvr_daily (date_jst) VALUES ('${day(0)}')`);
  const n0 = s.sent.length;
  await run(db2, { send: s, nowMs: NOW + DAY });
  assert.equal(s.sent.length, n0);
  assert.equal(db2.prepare('SELECT COUNT(*) n FROM data_freshness_alert_state').get().n, 0);
});
await t('--dry-run = 送らない・記録の表も作らない', async () => {
  const db = makeDb({ ...FRESH, fact_qoo10_cvr_daily: ['2026-10-03'] });
  const s = sender();
  const r = await run(db, { send: s, dryRun: true });
  assert.equal(s.sent.length, 0);
  assert.match(r.message, /Qoo10分析/);
  assert.match(r.lastLine, /^⚠️ .*--dry-run/);
  assert.equal(db.prepare("SELECT COUNT(*) n FROM sqlite_master WHERE name = 'data_freshness_alert_state'").get().n, 0);
});

console.log('今朝の自動取得 (fetch-all) の記録');
const fetchRead = (results, finishedMs = NOW - 2 * 3600000) => ({ status: FS.buildFetchStatus(results, { now: new Date(finishedMs) }), problem: null });
await t('exit 3 (ログイン切れ) = データがまだ新しい 1 日目から要約が ⚠️・GChat は送らない (子が送っている)・止まった feed の行に添える', async () => {
  const db = makeDb(FRESH);
  const s = sender();
  const fst = fetchRead([{ mall: 'yahoo', code: 3 }, { mall: 'aupay', code: 0 }, { mall: 'qoo10', code: null, timedOut: true }]);
  const r = await run(db, { send: s, fetchStatus: fst });
  assert.match(r.lastLine, /^⚠️ データの新しさ: 自動取得 \(10\/24 06:00 の回\) exit 3 = ログイン切れなど手で直すもの: yahoo \/ 失敗: qoo10=タイムアウト/);
  assert.equal(s.sent.length, 0);
  const db2 = makeDb({ ...FRESH, fact_yahoo_inflow_daily: ['2026-10-13'] });
  await run(db2, { send: s, fetchStatus: fst });
  assert.match(s.sent[0], /\*Yahoo統計\*[^\n]*\n   今朝の自動取得 \(10\/24 06:00\): ログイン切れなど手で直すもの \(exit 3・再試行しない\)\n   → ミニPCの画面でデスクトップの「Yahoo-Relogin\.bat」/);
});
await t('全部成功 = ✅ / 記録が無い = 何も言わない / 記録が 30 時間より古い = ⚠️ (05:30 の取得が動いていない疑い)', async () => {
  const db = makeDb(FRESH);
  const okRun = await run(db, { fetchStatus: fetchRead([{ mall: 'yahoo', code: 0 }]) });
  assert.match(okRun.lastLine, /^✅/);
  assert.match((await run(db, { fetchStatus: NO_FETCH })).lastLine, /^✅/);
  const old = await run(db, { fetchStatus: fetchRead([{ mall: 'yahoo', code: 0 }], NOW - 31 * 3600000) });
  assert.match(old.lastLine, /^⚠️ データの新しさ: 自動取得の最後の記録が古い \(10\/23 01:00・30 時間より前/);
  assert.equal(F.summarizeFetchStatus({ status: null, problem: 'unreadable: x' }).problem, 'unreadable: x');
});
await t('lib-fetch-status: 書く (別名 → 名前を替える)・読む・無い・壊れた・終了コードの意味', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'fetch-status-'));
  const file = path.join(dir, 'logs', 'fetch-all-last.json');
  assert.deepEqual(FS.readFetchStatus(file), { status: null, problem: 'missing' });
  const st = FS.buildFetchStatus([{ mall: 'yahoo', code: 3, signal: null, timedOut: false, secs: 12, spawnError: null }], { only: ['yahoo'], now: new Date(NOW) });
  assert.deepEqual(st, { version: 1, finished_at: new Date(NOW).toISOString(), only: ['yahoo'], results: [{ mall: 'yahoo', code: 3, signal: null, timedOut: false, secs: 12 }] });
  assert.equal(FS.writeFetchStatus(st, file).ok, true);
  assert.deepEqual(FS.readFetchStatus(file).status, st);
  assert.deepEqual(fs.readdirSync(path.dirname(file)), ['fetch-all-last.json'], '書きかけ (.tmp) を残さない');
  fs.writeFileSync(file, '{bad');
  assert.match(FS.readFetchStatus(file).problem, /^unreadable/);
  fs.writeFileSync(file, '{"x":1}');
  assert.equal(FS.readFetchStatus(file).problem, 'unreadable: 形が違う');
  assert.equal(FS.describeFetchExit({ code: 0 }), '成功');
  assert.match(FS.describeFetchExit({ code: 3 }), /ログイン切れ/);
  assert.match(FS.describeFetchExit({ code: 2 }), /env/);
  assert.match(FS.describeFetchExit({ code: 1 }), /再試行でも直らなかった/);
  assert.equal(FS.describeFetchExit({ code: null, timedOut: true }), 'タイムアウト');
  assert.equal(FS.describeFetchExit({ code: null, signal: 'SIGKILL' }), '強制終了 (SIGKILL)');
  // 書けない場所 = ok: false (投げない)
  const blocker = path.join(dir, 'file-not-dir');
  fs.writeFileSync(blocker, 'x');
  assert.equal(FS.writeFetchStatus(st, path.join(blocker, 'a.json')).ok, false);
});

console.log('一覧の中身');
await t('一覧の表と列がコードにある (ensure の関数で :memory: に作って読める・それ以外は CREATE TABLE の文に列がある)', async () => {
  const db = new Database(':memory:');
  const libs = [
    ['../apps/warehouse/rakuten-ads-rpp-lib.js', 'ensureRppTables'], ['../apps/warehouse/rakuten-data-lib.js', 'ensureRakutenDataTables'],
    ['../apps/warehouse/rakuten-dd-lib.js', 'ensureRakutenDdTables'], ['../apps/warehouse/rakuten-review-lib.js', 'ensureRakutenReviewTables'],
    ['../apps/warehouse/yahoo-data-lib.js', 'ensureYahooDataTables'], ['../apps/warehouse/yahoo-review-lib.js', 'ensureYahooReviewTables'],
    ['../apps/warehouse/aupay-data-lib.js', 'ensureAupayDataTables'], ['../apps/warehouse/qoo10-data-lib.js', 'ensureQoo10DataTables'],
    ['../apps/warehouse/fetch-amazon-sales-traffic.js', 'ensureSalesTrafficTables'], ['../apps/warehouse/fetch-amazon-listing-issues.js', 'ensureListingTables'],
    ['../apps/warehouse/fetch-amazon-bsr.js', 'ensureRankTables'], ['../apps/warehouse/fetch-amazon-returns.js', 'ensureReturnsTable'],
    ['../apps/warehouse/fetch-amazon-reimbursements.js', 'ensureReimbursementsTable'], ['../apps/warehouse/fetch-amazon-ledger.js', 'ensureLedgerTable'],
  ];
  for (const [mod, fn] of libs) (await import(mod))[fn](db);
  const srcFiles = ['db.js', 'rakuten-orders.js', 'aupay-orders.js', 'qoo10-orders.js', 'linegift-orders.js'].map((f) => fs.readFileSync(path.join(ROOT, 'apps/warehouse', f), 'utf8'));
  const viaText = [];
  for (const tb of F.FRESHNESS_TABLES) {
    const r = F.readLatest(db, tb);
    if (r.state !== 'missing') { assert.equal(r.state, 'empty', `${tb.table}.${tb.column}: ${r.state} ${r.error || ''}`); continue; }
    const src = srcFiles.find((s) => s.includes(`CREATE TABLE IF NOT EXISTS ${tb.table} (`));
    assert.ok(src, `CREATE TABLE がコードに無い: ${tb.table}`);
    const i = src.indexOf(`CREATE TABLE IF NOT EXISTS ${tb.table} (`);
    const ddl = src.slice(i, src.indexOf(')`', i));
    assert.match(ddl, new RegExp(`\\n\\s*${tb.column}\\s+TEXT`), `${tb.table} に列 ${tb.column} が無い`);
    viaText.push(tb.table);
  }
  assert.deepEqual(viaText.sort(), ['fact_ad_spend_campaign', 'raw_aupay_orders', 'raw_linegift_orders', 'raw_ne_orders', 'raw_qoo10_orders', 'raw_rakuten_orders', 'raw_sp_orders', 'raw_yahoo_orders']);
  assert.ok(!F.FRESHNESS_TABLES.some((x) => x.table === 'fact_ad_spend'), 'CREATE TABLE がコードに無い fact_ad_spend は入れない');
});
await t('1 行 = 表・列・しきい値・モール・何のデータか・人がやること。表は重ならない・止める台帳の id と取得の名前が実在する', () => {
  const names = F.FRESHNESS_TABLES.map((x) => x.table);
  assert.equal(new Set(names).size, names.length);
  assert.ok(names.length >= 40, String(names.length));
  for (const x of F.FRESHNESS_TABLES) {
    assert.ok(x.table && x.column && Number.isInteger(x.maxAgeDays) && x.maxAgeDays >= 3 && x.mall && x.what && x.action, JSON.stringify(x));
    assert.ok(['date', 'month'].includes(x.kind));
  }
  const fetchAll = fs.readFileSync(path.join(ROOT, 'scripts/mall-csv-fetcher/fetch-all.mjs'), 'utf8');
  for (const f of Object.values(F.FEEDS)) {
    if (f.fetcher) assert.ok(fetchAll.includes(`mall: '${f.fetcher}'`), `fetch-all に無い取得: ${f.fetcher}`);
    if (f.pause) assert.ok(JOBS_REGISTRY.find((e) => e.id === f.pause && e.type === 'temporary_asset'), `台帳に無い: ${f.pause}`);
  }
  // 2026-10-23 に止まっていたもの (楽天RPP・データ分析・Yahoo統計 6 種・Yahooレビュー・au PAY・Qoo10) が全部入っている
  const feedsOf = (feed) => F.FRESHNESS_TABLES.filter((x) => x.feed === feed).length;
  assert.equal(feedsOf('yahoo-data'), 6);
  for (const feed of ['rakuten-rpp', 'rakuten-data', 'yahoo-review', 'aupay-data', 'qoo10-data']) assert.ok(feedsOf(feed) >= 1, feed);
  // 週次・月次はしきい値が長い
  for (const x of F.FRESHNESS_TABLES.filter((y) => /weekly/.test(y.table))) assert.ok(x.maxAgeDays >= 14, x.table);
  for (const x of F.FRESHNESS_TABLES.filter((y) => /monthly|_rpp$/.test(y.table))) assert.ok(x.maxAgeDays >= 40, x.table);
});
await t('今の本物の台帳で: 楽天RPP・データ分析は 2026-09-30 の期限切れ = ⏸️ 期限切れ (止まった扱いにはしない)', () => {
  const db = new Database(':memory:');
  db.exec(`CREATE TABLE fact_rakuten_store_daily (date_jst TEXT); INSERT INTO fact_rakuten_store_daily VALUES ('2026-07-15')`);
  const one = F.FRESHNESS_TABLES.filter((x) => x.table === 'fact_rakuten_store_daily');
  const r = F.evaluateFreshness(db, { nowMs: NOW, tables: one });
  const reg = JOBS_REGISTRY.find((e) => e.id === 'mall-fetch-skip-rakuten-blocked');
  assert.equal(r.items[0].status, reg && reg.remove_by < '2026-10-24' ? 'paused_overdue' : 'paused');
});

console.log('daily-sync・publish-gate・fetch-all への組み込み');
await t('daily-sync: 「FBA長期在庫アラート」の後に 1 段 (引数なし = runScript が足す 7 は無視)・⚠️ = warn・再試行に載せない・照合の後・影/見張り/バックアップの前', () => {
  const ds = fs.readFileSync(path.join(ROOT, 'apps/warehouse/daily-sync.js'), 'utf8').replace(/\r\n/g, '\n');
  const call = "const freshnessResult = runScript('apps/warehouse/monitor-data-freshness.js', 'データの新しさ', 120000);";
  const i = ds.indexOf(call);
  assert.ok(i > 0);
  assert.equal(ds.indexOf('monitor-data-freshness.js', i + call.length), -1, '1 か所だけ');
  assert.match(ds.slice(i), /^[^\n]*\n\s*results\.push\(\{ name: 'データの新しさ', \.\.\.freshnessResult, warn: freshnessResult\.success && isWarnSummary\(freshnessResult\.summary\) \}\);/);
  const at = (s) => { const k = ds.indexOf(s); assert.ok(k > 0, s); return k; };
  assert.ok(at("results.push({ name: 'FBA長期在庫アラート'") < i);
  assert.ok(at("runScript('apps/warehouse/monitor-fee-coverage.js'") < i);
  for (const s of ["'楽天RPP広告 取込'", "'Yahoo統計 取込'", "'auPAY分析 取込'", "'Qoo10分析 取込'", "'Yahooレビュー 取込'", "runScript('apps/company-db/master-compare/run.mjs --daily'"]) assert.ok(at(s) < i, s);
  for (const s of ["runScript('scripts/company-db/lz-daily.mjs", "runScript('apps/company-db/watch/run.mjs'", "runScript('apps/warehouse/backup-warehouse.js'"]) assert.ok(at(s) > i, s);
  const retry = ds.slice(ds.indexOf('const RETRYABLE_JOBS = ['), ds.indexOf('];', ds.indexOf('const RETRYABLE_JOBS = [')));
  assert.doesNotMatch(retry, /データの新しさ/);
});
await t('publish-gate: 止めない工程 (写しの反映が世代と違う朝も見る)・止める一覧には無い', async () => {
  const G = await import('../apps/warehouse/publish-gate.js');
  assert.ok(Object.hasOwn(G.PUBLISH_UNGATED_SCRIPTS, 'apps/warehouse/monitor-data-freshness.js'));
  assert.ok(!Object.hasOwn(G.PUBLISH_GATED_SCRIPTS, 'apps/warehouse/monitor-data-freshness.js'));
  assert.equal(G.publishGateDecision('apps/warehouse/monitor-data-freshness.js', { broken: true }).skip, false);
});
await t('fetch-all: 最後の summary の後に記録を書く・書けなくても終了コード・再試行・通知の決まりは前のまま', () => {
  const src = fs.readFileSync(path.join(ROOT, 'scripts/mall-csv-fetcher/fetch-all.mjs'), 'utf8').replace(/\r\n/g, '\n');
  assert.match(src, /\nimport \{ buildFetchStatus, writeFetchStatus \} from '\.\/lib-fetch-status\.mjs';\n/);
  const iSum = src.indexOf("console.log(`\\n=== summary: ${results.map");
  const iWrite = src.indexOf('const st = writeFetchStatus(buildFetchStatus(results, { only }));');
  assert.ok(iSum > 0 && iWrite > iSum && iWrite < src.indexOf('const unreported = failed.filter'), '最終結果 (再試行の後) を書く');
  assert.match(src, /return r && r\.code !== 0 && r\.code !== 2 && r\.code !== 3;/, '再試行の対象は前のまま');
  assert.match(src, /const unreported = failed\.filter\(\(r\) => r\.code !== 1 && r\.code !== 2 && r\.code !== 3\);/, '通知は前のまま');
  assert.match(src, /process\.exitCode = failed\.length > 0 \? 1 : 0;/, '終了コードは前のまま');
  assert.doesNotMatch(src.slice(iWrite, iWrite + 300), /process\.exitCode|throw /);
});

console.log(`\n${ok} ok / ${ng} NG`);
if (ng) process.exitCode = 1;
