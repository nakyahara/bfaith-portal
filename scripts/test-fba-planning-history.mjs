#!/usr/bin/env node
/**
 * test-fba-planning-history.mjs — FBA 在庫計画の日 × SKU の履歴 (apps/warehouse/fba-planning-history.js) と
 *   週 1 回の長期在庫アラート (apps/warehouse/alert-fba-aged-inventory.js) の試験。
 *   SP-API・GChat・本物の DB には繋がない (warehouse.db の代わりに :memory: の SQLite。取得と送り手は差し替え)。
 *   ① 列の読み方 (列が無い・空・"--" は null・列名の大文字小文字や空白の違い) ② 置き換えの冪等・古い取得で戻さない・急に減った取得で置き換えない・0 行で消さない・古い日の掃除
 *   ③ 毎朝のスナップショット (fba-report-snapshot.js) からの呼び出し: 履歴が失敗しても今までの保存と結果は変わらない・渡さなければ今までどおり
 *   ④ アラート: 曜日・今週は送った・履歴が古い・対象の選び方・1 通の文面・送れなかったら記録しない
 */
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import {
  normalizePlanningHistoryRow, missingPlanningColumns, savePlanningHistory, hasStock, normHeader,
  PLANNING_HISTORY_COLUMNS, PLANNING_HISTORY_KEEP_DAYS, makePlanningHistorySaver,
} from '../apps/warehouse/fba-planning-history.js';
import { runFbaReportSnapshot } from '../apps/warehouse/fba-report-snapshot.js';
import {
  runAgedAlert, pickAgedCandidates, buildAgedAlertMessage, weekKeyJst, jstWeekday, ALERT_WEEKDAY_JST, AGED_MIN_QTY, MAX_LIST,
} from '../apps/warehouse/alert-fba-aged-inventory.js';

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message)); } };
const throws = (fn, re) => { let e = null; try { fn(); } catch (x) { e = x; } if (!e) throw new Error('did not throw'); if (re && !re.test(e.message)) throw new Error(`wrong error: ${e.message}`); };
const quiet = () => {};

/** PLANNING の生の行 (parseTsv の形 = 列名 → 文字列) */
const raw = (sku, over = {}) => ({
  'snapshot-date': '2026-10-20', sku, fnsku: `X0${sku}`, asin: `B0${sku}`, currency: 'JPY', 'your-price': '1980',
  available: '50', 'Total Reserved Quantity': '2', 'unfulfillable-quantity': '0', 'inbound-quantity': '0',
  'inv-age-0-to-90-days': '20', 'inv-age-91-to-180-days': '10', 'inv-age-181-to-270-days': '15', 'inv-age-271-to-365-days': '5', 'inv-age-365-plus-days': '2',
  'units-shipped-t7': '3', 'units-shipped-t30': '12', 'units-shipped-t60': '20', 'units-shipped-t90': '30',
  'estimated-storage-cost-next-month': '123.45', 'estimated-excess-quantity': '8', 'recommended-action': 'CreateRemovalOrder', 'recommended-removal-quantity': '5',
  ...over,
});
const NOW = new Date('2026-10-22T00:30:00Z');   // JST 10/22 09:30
const FETCHED = '2026-10-21T22:40:00.000Z';
const openDb = () => new Database(':memory:');
const rowsOf = (db, d, m = 'jp') => db.prepare('SELECT * FROM fba_planning_daily WHERE snapshot_date = ? AND market = ? ORDER BY amazon_sku').all(d, m);
const dayOf = (db, d, m = 'jp') => db.prepare('SELECT * FROM fba_planning_daily_days WHERE snapshot_date = ? AND market = ?').get(d, m);

console.log('① 列の読み方');
await t('レポートの列 → 表の列。数は数・文字は文字。列名の大文字小文字・空白・_ の違いは同じ列 (Total Reserved Quantity / reserved-quantity)', () => {
  const src = raw('sku-a', { 'RESERVED_QUANTITY': '4', ' Inv-Age-365-Plus-Days ': '7' });
  delete src['Total Reserved Quantity']; delete src['inv-age-365-plus-days'];
  const r = normalizePlanningHistoryRow(src);
  assert.equal(r.amazon_sku, 'sku-a');
  assert.deepEqual([r.available, r.reserved_qty, r.inv_age_181_270, r.inv_age_365_plus, r.units_shipped_t30, r.estimated_storage_cost_next_month, r.your_price, r.recommended_action, r.report_snapshot_date],
    [50, 4, 15, 7, 12, 123.45, 1980, 'CreateRemovalOrder', '2026-10-20']);
  assert.equal(normHeader('Recommended ship-in quantity'), normHeader('recommended-ship-in-quantity'));
});
await t('🚨 列が無い・空・数でない ("--" / "N/A")・整数の列に小数 → null (0 にしない)。SKU の無い行は null', () => {
  const r = normalizePlanningHistoryRow(raw('sku-b', { 'inv-age-181-to-270-days': '', 'inv-age-271-to-365-days': '--', 'estimated-storage-cost-next-month': 'N/A', available: '1.5' }));
  assert.deepEqual([r.inv_age_181_270, r.inv_age_271_365, r.estimated_storage_cost_next_month, r.available], [null, null, null, null]);
  for (const c of ['ais_qty_181_210', 'ais_est_365_plus', 'sell_through', 'healthy_inventory_level', 'inventory_health_status']) assert.equal(r[c], null, c);   // この行に列が無い
  assert.equal(normalizePlanningHistoryRow({ sku: '  ', available: '3' }), null);
  assert.equal(normalizePlanningHistoryRow(null), null);
  assert.equal(normalizePlanningHistoryRow({ 'merchant-sku': 'm1', available: '0' }).amazon_sku, 'm1');
});
await t('この回のレポートに無い列の一覧 (どの行の列名にも無い)', () => {
  const miss = missingPlanningColumns([raw('a'), raw('b')]);
  assert.ok(miss.includes('ais_qty_181_210') && miss.includes('sell_through'));
  assert.ok(!miss.includes('inv_age_181_270') && !miss.includes('reserved_qty') && !miss.includes('available'));
  assert.equal(missingPlanningColumns([]).length, PLANNING_HISTORY_COLUMNS.length);
});
await t('在庫がある: どれか 1 以上 → 残す / 分かる数がどれも 0 → 残さない / どれも分からない → 残す', () => {
  assert.equal(hasStock(normalizePlanningHistoryRow(raw('a'))), true);
  const zero = Object.fromEntries(Object.entries(raw('z')).map(([k, v]) => [k, /^\d/.test(v) && !['your-price', 'snapshot-date'].includes(k) ? '0' : v]));
  assert.equal(hasStock(normalizePlanningHistoryRow(zero)), false);
  assert.equal(hasStock(normalizePlanningHistoryRow({ sku: 'u', 'units-shipped-t30': '3' })), true);
});

console.log('② 保存');
await t('日 × market を置き換え・印 (取得時刻・レポートの行数・残した行数・在庫 0 で残さない行数・無かった列)。同じ入力をもう一度 = 同じ中身 (冪等)', () => {
  const db = openDb();
  const zero = { available: '0', 'Total Reserved Quantity': '0', 'inv-age-0-to-90-days': '0', 'inv-age-91-to-180-days': '0', 'inv-age-181-to-270-days': '0', 'inv-age-271-to-365-days': '0', 'inv-age-365-plus-days': '0' };
  const input = [raw('sku-a'), raw('sku-b', { available: '3' }), raw('sku-z', zero), { sku: '' }];
  const r1 = savePlanningHistory(db, { snapshotDate: '2026-10-22', market: 'jp', rawRows: input, fetchedAt: FETCHED, now: NOW });
  assert.deepEqual([r1.saved, r1.reportRows, r1.rows, r1.skippedNoStock], [true, 3, 2, 1]);
  const first = rowsOf(db, '2026-10-22');
  assert.deepEqual(first.map((x) => [x.amazon_sku, x.available, x.inv_age_181_270]), [['sku-a', 50, 15], ['sku-b', 3, 15]]);
  const d1 = dayOf(db, '2026-10-22');
  assert.deepEqual([d1.fetched_at, d1.report_rows, d1.saved_rows, d1.skipped_no_stock], [FETCHED, 3, 2, 1]);
  assert.ok(JSON.parse(d1.missing_columns).includes('ais_est_181_210'));
  const r2 = savePlanningHistory(db, { snapshotDate: '2026-10-22', market: 'jp', rawRows: input, fetchedAt: FETCHED, now: NOW });
  assert.equal(r2.saved, true);
  assert.deepEqual(rowsOf(db, '2026-10-22'), first);
});
await t('🚨 同じ日を新しい取得で取り直す: 丸ごと入れ替わる (消えた SKU は消える・値は新しい方)。ほかの日・US は触らない', () => {
  const db = openDb();
  savePlanningHistory(db, { snapshotDate: '2026-10-21', market: 'jp', rawRows: [raw('old')], fetchedAt: '2026-10-20T22:00:00Z', now: NOW });
  savePlanningHistory(db, { snapshotDate: '2026-10-22', market: 'us', rawRows: [raw('us-1')], fetchedAt: FETCHED, now: NOW });
  savePlanningHistory(db, { snapshotDate: '2026-10-22', market: 'jp', rawRows: [raw('a'), raw('b'), raw('c')], fetchedAt: FETCHED, now: NOW });
  const r = savePlanningHistory(db, { snapshotDate: '2026-10-22', market: 'jp', rawRows: [raw('a', { available: '9' }), raw('c')], fetchedAt: '2026-10-22T00:10:00Z', now: NOW });
  assert.equal(r.saved, true);
  assert.deepEqual(rowsOf(db, '2026-10-22').map((x) => [x.amazon_sku, x.available]), [['a', 9], ['c', 50]]);
  assert.deepEqual([rowsOf(db, '2026-10-21').length, rowsOf(db, '2026-10-22', 'us').length], [1, 1]);
});
await t('🚨 古い取得 (取得時刻が印より前) では戻さない・前の 30% 未満に急に減った取得では置き換えない・0 行の取得では消さない', () => {
  const db = openDb();
  const ten = Array.from({ length: 10 }, (_, i) => raw(`s${i}`));
  savePlanningHistory(db, { snapshotDate: '2026-10-22', market: 'jp', rawRows: ten, fetchedAt: FETCHED, now: NOW });
  const older = savePlanningHistory(db, { snapshotDate: '2026-10-22', market: 'jp', rawRows: [raw('s0', { available: '1' })], fetchedAt: '2026-10-21T20:00:00Z', now: NOW });
  assert.equal(older.reason, 'older_fetch');
  const few = savePlanningHistory(db, { snapshotDate: '2026-10-22', market: 'jp', rawRows: [raw('s0'), raw('s1')], fetchedAt: '2026-10-22T00:00:00Z', now: NOW });
  assert.deepEqual([few.saved, few.reason, few.prev], [false, 'count_guard', 10]);
  const none = savePlanningHistory(db, { snapshotDate: '2026-10-22', market: 'jp', rawRows: [], fetchedAt: '2026-10-22T00:00:00Z', now: NOW });
  assert.deepEqual([none.saved, none.reason], [false, 'no_rows']);
  assert.equal(rowsOf(db, '2026-10-22').length, 10);
  assert.equal(dayOf(db, '2026-10-22').fetched_at, FETCHED);
});
await t('🚨 古い日の掃除: 「いまの JST の日付」から KEEP_DAYS 日より前の行と印を消す (入力の日付では動かない)。既定は 400 日', () => {
  assert.equal(PLANNING_HISTORY_KEEP_DAYS, 400);
  const db = openDb();
  savePlanningHistory(db, { snapshotDate: '2026-09-01', market: 'jp', rawRows: [raw('a')], fetchedAt: '2026-09-01T00:00:00Z', now: new Date('2026-09-01T03:00:00Z') });
  savePlanningHistory(db, { snapshotDate: '2026-10-10', market: 'jp', rawRows: [raw('a')], fetchedAt: '2026-10-10T00:00:00Z', now: new Date('2026-10-10T03:00:00Z') });
  // 過去の日付を入れ直しても、基準は今日 (10/22) = 10/22 - 30 日 = 9/22 より前を消す
  const r = savePlanningHistory(db, { snapshotDate: '2026-10-01', market: 'jp', rawRows: [raw('a')], fetchedAt: FETCHED, now: NOW, keepDays: 30 });
  assert.equal(r.deleted, 1);
  assert.deepEqual(db.prepare('SELECT snapshot_date d FROM fba_planning_daily_days ORDER BY 1').all().map((x) => x.d), ['2026-10-01', '2026-10-10']);
  assert.equal(rowsOf(db, '2026-09-01').length, 0);
});
await t('入力の検査: 日付が不正・未来 (JST の今日より後)・market・取得時刻 (Z か ±HH:MM) は例外 (何も書かない)', () => {
  const db = openDb();
  throws(() => savePlanningHistory(db, { snapshotDate: '2026-02-30', market: 'jp', rawRows: [raw('a')], fetchedAt: FETCHED, now: NOW }), /snapshotDate/);
  throws(() => savePlanningHistory(db, { snapshotDate: '2026-10-23', market: 'jp', rawRows: [raw('a')], fetchedAt: FETCHED, now: NOW }), /未来/);
  throws(() => savePlanningHistory(db, { snapshotDate: '2026-10-22', market: 'eu', rawRows: [raw('a')], fetchedAt: FETCHED, now: NOW }), /market/);
  throws(() => savePlanningHistory(db, { snapshotDate: '2026-10-22', market: 'jp', rawRows: [raw('a')], fetchedAt: '2026-10-22 09:00:00', now: NOW }), /fetchedAt/);
});

console.log('③ 毎朝のスナップショットからの呼び出し');
/** fba.db の代わり (saveJpReports が使う関数だけ) */
const fakeFbaDb = () => ({
  saveRestockInventoryToDailySnapshot: (rows) => ({ updated: 0, inserted: rows.length }), saveRestockLatest: (rows) => ({ saved: rows.length }), updateFnskuBatch: () => {},
  savePlanningData: (rows) => rows.length, savePlanningLatest: (rows) => ({ saved: rows.length }), syncFnskuBatch: () => {},
  saveStockExport: () => ({ saved: true }), getInputFreshness: () => null, saveUsDailySnapshots: ({ planningRows }) => ({ inserted: planningRows.length, updated: 0 }),
});
const restockRaw = (sku) => ({ 'Merchant SKU': sku, Available: '5' });
const reports = (planning) => async () => ({ planning, restock: [restockRaw('a')], errors: [], market: 'jp' });
await t('JP の PLANNING の生の行を、その回の business_date・取得時刻で渡す。US は usContext があるときだけ US の行で', async () => {
  const calls = [];
  const fetchReports = async (ctx) => (ctx ? { planning: [raw('us-a')], restock: [], errors: [] } : { planning: [raw('a'), raw('b')], restock: [restockRaw('a')], errors: [] });
  const r = await runFbaReportSnapshot({ db: fakeFbaDb(), businessDate: '2026-10-22', fetchReports, usContext: { refresh_token: 'x', client_id: 'x', client_secret: 'x' }, saveUsRaw: () => ({}),
    log: quiet, warn: quiet, planningHistory: (a) => { calls.push(a); return { saved: true }; } });
  assert.equal(r.ok, true);
  assert.deepEqual(calls.map((c) => [c.market, c.snapshotDate, c.rawRows.map((x) => x.sku)]), [['jp', '2026-10-22', ['a', 'b']], ['us', '2026-10-22', ['us-a']]]);
  assert.ok(calls.every((c) => /Z$/.test(c.fetchedAt)));
  assert.doesNotMatch(r.lastLine, /在庫計画の履歴/);
});
await t('🚨 履歴が投げても・置き換えなくても: 今までの保存と結果 (ok・先頭の印) は変わらず、最後の 1 行に注記だけ。PLANNING が取れない回は呼ばない', async () => {
  const base = await runFbaReportSnapshot({ db: fakeFbaDb(), businessDate: '2026-10-22', fetchReports: reports([raw('a')]), usContext: null, log: quiet, warn: quiet });
  const boom = await runFbaReportSnapshot({ db: fakeFbaDb(), businessDate: '2026-10-22', fetchReports: reports([raw('a')]), usContext: null, log: quiet, warn: quiet,
    planningHistory: () => { throw new Error('disk full'); } });
  assert.deepEqual([boom.ok, boom.jp], [base.ok, base.jp]);
  assert.equal(boom.lastLine, `${base.lastLine} / ⚠️ 在庫計画の履歴: JP 失敗: disk full`);
  const guard = await runFbaReportSnapshot({ db: fakeFbaDb(), businessDate: '2026-10-22', fetchReports: reports([raw('a')]), usContext: null, log: quiet, warn: quiet,
    planningHistory: () => ({ saved: false, reason: 'count_guard' }) });
  assert.match(guard.lastLine, /在庫計画の履歴: JP 置き換えない \(count_guard\)$/);
  let called = 0;
  const noPlanning = await runFbaReportSnapshot({ db: fakeFbaDb(), businessDate: '2026-10-22', fetchReports: reports(null), usContext: null, log: quiet, warn: quiet, planningHistory: () => { called++; return { saved: true }; } });
  assert.deepEqual([called, noPlanning.ok], [0, true]);
});
await t('渡さない (null) = 今までどおり (呼ばない・最後の 1 行も同じ)。本物の保存 (makePlanningHistorySaver) を通すと warehouse の表に入る', async () => {
  const a = await runFbaReportSnapshot({ db: fakeFbaDb(), businessDate: '2026-10-22', fetchReports: reports([raw('a')]), usContext: null, log: quiet, warn: quiet });
  const b = await runFbaReportSnapshot({ db: fakeFbaDb(), businessDate: '2026-10-22', fetchReports: reports([raw('a')]), usContext: null, log: quiet, warn: quiet, planningHistory: null });
  assert.equal(a.lastLine, b.lastLine);
  const wdb = openDb();
  const today = new Date(Date.now() + 9 * 3600000).toISOString().slice(0, 10);
  const r = await runFbaReportSnapshot({ db: fakeFbaDb(), businessDate: today, fetchReports: reports([raw('a'), raw('b')]), usContext: null, log: quiet, warn: quiet,
    planningHistory: makePlanningHistorySaver(() => wdb, { log: quiet }) });
  assert.equal(r.ok, true);
  assert.equal(rowsOf(wdb, today).length, 2);
});

console.log('④ 長期在庫アラート');
// 2026-10-26 は月曜 (JST)
const MON = Date.parse('2026-10-26T00:00:00Z');   // JST 月曜 09:00
const TUE = Date.parse('2026-10-27T00:00:00Z');
const seed = (db, date, rows) => savePlanningHistory(db, { snapshotDate: date, market: 'jp', rawRows: rows, fetchedAt: `${date}T00:00:00Z`, now: new Date(`${date}T03:00:00Z`) });
const agedRows = () => [
  raw('big', { 'inv-age-181-to-270-days': '30', 'inv-age-271-to-365-days': '10', 'inv-age-365-plus-days': '5', 'your-price': '2000' }),     // 45 個 × 2000 = 90,000
  raw('cheap', { 'inv-age-181-to-270-days': '20', 'inv-age-271-to-365-days': '0', 'inv-age-365-plus-days': '0', 'your-price': '100' }),     // 20 個 × 100 = 2,000 → 金額が小さい
  raw('few', { 'inv-age-181-to-270-days': '3', 'inv-age-271-to-365-days': '1', 'inv-age-365-plus-days': '0', 'your-price': '50000' }),     // 4 個 → 個数が少ない
  raw('ais', { 'inv-age-181-to-270-days': '1', 'inv-age-271-to-365-days': '0', 'inv-age-365-plus-days': '0', 'estimated-ais-181-210-days': '800', 'estimated-ais-211-240-days': '700' }),   // 長期保管料の見込み 1,500
  raw('noprice', { 'inv-age-181-to-270-days': '12', 'inv-age-271-to-365-days': '', 'inv-age-365-plus-days': '', 'your-price': '' }),   // 価格が分からない = 個数だけで
  raw('unknown', { 'inv-age-181-to-270-days': '', 'inv-age-271-to-365-days': '', 'inv-age-365-plus-days': '' }),   // 年齢の列が無い = 対象にしない
];
await t('週の鍵 = JST の月曜・曜日 (朝 7 時の JST は UTC の前日)', () => {
  assert.equal(jstWeekday(MON), 1);
  assert.equal(ALERT_WEEKDAY_JST, 1);
  assert.equal(weekKeyJst(MON), '2026-10-26');
  assert.equal(weekKeyJst(Date.parse('2026-11-01T14:00:00Z')), '2026-10-26');   // JST 11/01 (日) 23:00
  assert.equal(weekKeyJst(Date.parse('2026-10-25T22:00:00Z')), '2026-10-26');   // JST 10/26 (月) 07:00
});
await t('対象の選び方: 181 日以上が N 個以上かつ金額が大きい (価格が無ければ個数だけ)・または長期保管料の見込みが大きい。金額の大きい順', () => {
  const db = openDb();
  seed(db, '2026-10-26', agedRows());
  const c = pickAgedCandidates(db, '2026-10-26');
  assert.deepEqual(c.map((x) => x.sku), ['big', 'ais', 'noprice']);
  assert.deepEqual([c[0].aged181, c[0].over365, c[0].agedValue, c[1].aisEst, c[2].aged181, c[2].agedValue], [45, 5, 90000, 1500, 12, null]);
  assert.ok(AGED_MIN_QTY === 10);
  const msg = buildAgedAlertMessage(c, '2026-10-26');
  assert.match(msg, /3 件/);
  assert.match(msg, /• big — 181日以上 45個 \(365日超 5\)/);
  assert.match(msg, /長期保管料の見込み ¥1,500/);
  const many = Array.from({ length: MAX_LIST + 5 }, (_, i) => ({ ...c[0], sku: `s${i}` }));
  assert.match(buildAgedAlertMessage(many, '2026-10-26'), /ほか 5 件/);
});
await t('🚨 控えめ: 月曜だけ・同じ週に 2 回送らない (記録)。送れなかった回は記録しない (翌朝にもう一度)。webhook が無ければ送らない', async () => {
  const db = openDb();
  seed(db, '2026-10-26', agedRows());
  const sent = [];
  const send = async (url, text) => { sent.push(text); return true; };
  assert.equal((await runAgedAlert(db, { nowMs: TUE, webhook: 'https://x', send, log: quiet })).status, 'not_today');
  assert.equal((await runAgedAlert(db, { nowMs: MON, webhook: '', send, log: quiet })).status, 'no_webhook');
  assert.equal((await runAgedAlert(db, { nowMs: MON, webhook: 'https://x', send: async () => false, log: quiet })).status, 'send_failed');
  const r = await runAgedAlert(db, { nowMs: MON, webhook: 'https://x', send, log: quiet });
  assert.deepEqual([r.status, r.candidates, sent.length], ['sent', 3, 1]);
  assert.equal((await runAgedAlert(db, { nowMs: MON + 3600000, webhook: 'https://x', send, log: quiet })).status, 'already_sent');
  assert.equal(sent.length, 1);
  assert.equal((await runAgedAlert(db, { nowMs: MON, webhook: 'https://x', send, force: true, log: quiet })).status, 'sent');   // --force は送る
  assert.equal((await runAgedAlert(db, { nowMs: TUE, webhook: 'https://x', send, dryRun: true, log: quiet })).status, 'dry_run');
  assert.equal(sent.length, 2);
});
await t('履歴が無い・古い (最新が 3 日より前) → 送らない。該当なし → 送らない', async () => {
  const send = async () => { throw new Error('送ってはいけない'); };
  assert.equal((await runAgedAlert(openDb(), { nowMs: MON, webhook: 'https://x', send, log: quiet })).status, 'no_history');
  const db = openDb();
  seed(db, '2026-10-20', agedRows());
  assert.equal((await runAgedAlert(db, { nowMs: MON, webhook: 'https://x', send, log: quiet })).status, 'stale');
  const db2 = openDb();
  seed(db2, '2026-10-26', [raw('fresh', { 'inv-age-181-to-270-days': '0', 'inv-age-271-to-365-days': '0', 'inv-age-365-plus-days': '0' })]);
  assert.equal((await runAgedAlert(db2, { nowMs: MON, webhook: 'https://x', send, log: quiet })).status, 'none');
});

console.log(`\n${ok} ok / ${ng} NG`);
process.exitCode = ng ? 1 : 0;
