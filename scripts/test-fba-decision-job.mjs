/**
 * test-fba-decision-job.mjs — FBA 補充「9:40 の自動決定」(A2b) の受入試験
 *
 * 1. ロジザードの写しから組んだ倉庫在庫が、手動 CSV (warehouse-csv.js → warehouse_inventory → db.js の SQL) と同じになるか
 * 2. 写しを使わない条件 (古い・未来・読み飛ばし・世代違い・おかしな数量)
 * 3. 「今朝のレポート」・取り込みの結果・準備中の取り直しの判定
 * 4. 1 回の試行の流れ (待機 / 決めた / もう決めた / 最後の回で決められない / ロック競合 / 計算の失敗 / 時間切れ)
 *    = PGlite (DDL を流した本物の Postgres) に記録して見る
 * 使い方: node scripts/test-fba-decision-job.mjs
 */
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';
import { buildWarehouseFromMirror, diffWarehouse, readMirrorWarehouse } from '../apps/fba-replenishment/mirror-warehouse.js';
import { parseWarehouseCsv } from '../apps/fba-replenishment/warehouse-csv.js';
import {
  runDecisionAttempt, runDecisionAttemptSafe, reportsFromThisMorning, syncReasons, jstClock, isFinalAttempt,
  shouldCatchUpAtStartup, DECISION_JOB_ID, ATTEMPT_TIMEOUT_MS,
} from '../apps/fba-replenishment/decision-job.js';
import { inputGate, GENERATOR, RUN_SUMMARY_KEY } from '../apps/fba-replenishment/shadow-draft.mjs';
import { judgeInboundFetch } from '../apps/fba-replenishment/inbound-state.js';

let passed = 0;
function t(name, fn) { try { fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack}`); process.exitCode = 1; } }
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack}`); process.exitCode = 1; } }
const quiet = () => {};

// ---------------------------------------------------------------------------------------------
console.log('写し → 倉庫在庫 (手動 CSV と同じになるか)');

/** 写しの 1 行 (mirror_logizard_stock の列) */
const CAP = '2026-10-05T00:20:00.000Z';
const mrow = (o) => ({
  商品ID: 'abc001', 商品名: 'テスト', ブロック略称: 'A', ロケ: 'P-01-01', 有効期限: '', 在庫数: 10, 引当数: 0,
  ロケ業務区分: '通販', 最終入荷日: '20260901', ブロック引当順: 5, captured_at: CAP, ...o,
});
const FIXTURE = [
  mrow({ 商品ID: 'ABC001', ロケ: 'P-01-02', 在庫数: 12, 引当数: 2, 有効期限: '20270131', ロケ業務区分: '卸し', ブロック引当順: 3 }),
  mrow({ 商品ID: 'abc001 ', 商品名: 'テストB', ロケ: 'P-01-01', 在庫数: 5, 引当数: 5, 有効期限: '2027/1/5', 最終入荷日: '20260915' }),
  mrow({ 商品ID: 'abc001', ロケ: 'P-02-01', 在庫数: 8, 引当数: 1, 有効期限: '2027-03-01', ブロック引当順: 1 }),
  mrow({ 商品ID: 'abc001', ブロック略称: 'YYY', ロケ: 'Y-01', 在庫数: 30, 引当数: 0 }),
  mrow({ 商品ID: 'xyz999', 商品名: '', ロケ: '', 在庫数: 4, 引当数: 0, ロケ業務区分: '', 最終入荷日: '' }),
  mrow({ 商品ID: 'xyz999', ロケ: 'Y-99', ブロック略称: 'B', 在庫数: 2, 引当数: 0 }),
  mrow({ 商品ID: 'def002', ロケ: 'R-01-01', 在庫数: 3, 引当数: 0, ロケ業務区分: 'その他', ブロック引当順: null }),
];
const META = (o = {}) => ({ captured_at: CAP, source_at: '2026-10-05T00:05:00.000Z', rows_read: FIXTURE.length, skipped_rows: 0, row_count: FIXTURE.length, ...o });
const NOW = Date.parse('2026-10-05T00:40:00Z');   // 09:40 JST

/** 同じ中身を手動 CSV → parseWarehouseCsv → replaceWarehouseInventory と同じ変換 → db.js と同じ SQL で読む */
function manualFromCsv(rows) {
  const head = ['商品ID', '商品名', 'ブロック略称', 'ロケ', '有効期限', '在庫数', '引当数', 'ロケ業務区分', '最終入荷日', 'ブロック引当順'];
  const q = (v) => `"${String(v ?? '').replace(/"/g, '""')}"`;
  const csv = [head.join(','), ...rows.map((r) => head.map((h) => q(r[h])).join(','))].join('\n');
  const parsed = parseWarehouseCsv(Buffer.from(csv, 'utf8'));
  if (parsed.error) throw new Error(parsed.error);
  const db = new Database(':memory:');
  db.exec(`CREATE TABLE warehouse_inventory (logizard_code TEXT, product_name TEXT, location TEXT, block TEXT, quantity INTEGER,
    reserved INTEGER, available_qty INTEGER, expiry_date TEXT, is_y_location INTEGER, last_arrival_date TEXT,
    location_biz_type TEXT, block_alloc_order INTEGER)`);
  const ins = db.prepare('INSERT INTO warehouse_inventory VALUES (?,?,?,?,?,?,?,?,?,?,?,?)');
  // db.js replaceWarehouseInventory と同じ変換 ('' → null、ブロック引当順は || 9999)
  for (const it of parsed.items) {
    ins.run(it.logizard_code, it.product_name || null, it.location || null, it.block || null, parseInt(it.quantity || 0),
      parseInt(it.reserved || 0), parseInt(it.available_qty || 0), it.expiry_date || null, it.is_y_location ? 1 : 0,
      it.last_arrival_date || null, it.location_biz_type || null, parseInt(it.block_alloc_order || 9999));
  }
  // db.js getWarehouseSummary / getWarehouseLocationsByCode と同じ SQL (変えたらここも直す)
  const summary = db.prepare(`
    SELECT MIN(logizard_code) as logizard_code, MAX(product_name) as product_name,
      SUM(CASE WHEN is_y_location = 0 THEN quantity ELSE 0 END) as warehouse_qty,
      SUM(CASE WHEN is_y_location = 0 THEN available_qty ELSE 0 END) as warehouse_available,
      SUM(CASE WHEN is_y_location = 1 THEN quantity ELSE 0 END) as y_location_qty,
      MIN(CASE WHEN expiry_date != '' AND expiry_date IS NOT NULL THEN expiry_date END) as earliest_expiry,
      MAX(last_arrival_date) as last_arrival_date, COUNT(DISTINCT location) as location_count
    FROM warehouse_inventory GROUP BY LOWER(TRIM(logizard_code)) ORDER BY 1`).all();
  const locs = (code) => db.prepare(`
    SELECT location, block, available_qty, location_biz_type, block_alloc_order, expiry_date
    FROM warehouse_inventory
    WHERE LOWER(TRIM(logizard_code)) = LOWER(TRIM(?)) AND is_y_location = 0 AND available_qty > 0
    ORDER BY CASE WHEN location_biz_type = '卸し' THEN 0 WHEN location_biz_type = '通販' THEN 1 ELSE 2 END,
      block_alloc_order ASC, location ASC`).all(code);
  return { summary, locs };
}

t('合計 (getWarehouseSummary) が手動 CSV と同じ (大小文字・空白・Y ロケ・期限の表記ゆれ・最終入荷日・空のロケ)', () => {
  const wh = buildWarehouseFromMirror({ rows: FIXTURE, meta: META(), nowMs: NOW });
  assert.equal(wh.ok, true, wh.reasons.join(' / '));
  const manual = manualFromCsv(FIXTURE);
  assert.deepEqual(wh.summaryRows, manual.summary);
  assert.equal(wh.baseAtMs, Date.parse('2026-10-05T00:05:00.000Z'), '基準の時刻 = 在庫を取った時刻');
});

t('ロケ (getWarehouseLocationsByCode) が手動 CSV と同じ並び・中身 (卸し → 通販 → その他、ブロック引当順、ロケ)', () => {
  const wh = buildWarehouseFromMirror({ rows: FIXTURE, meta: META(), nowMs: NOW });
  const manual = manualFromCsv(FIXTURE);
  for (const code of ['abc001', 'ABC001', ' abc001', 'xyz999', 'def002', 'none']) {
    assert.deepEqual(wh.locationsByCode(code), manual.locs(code), code);
  }
  assert.deepEqual(wh.locationsByCode('abc001').map((l) => l.location), ['P-01-02', 'P-02-01'], '引当が全部の P-01-01 は出ない');
});

t('ブロック引当順 0 は 0 のまま (手動 CSV の保存は 9999 になる = 既知の違い)', () => {
  const rows = [mrow({ ロケ: 'P-1', ブロック引当順: 0 }), mrow({ ロケ: 'P-2', ブロック引当順: 1 })];
  const wh = buildWarehouseFromMirror({ rows, meta: META({ rows_read: 2, row_count: 2 }), nowMs: NOW });
  assert.deepEqual(wh.locationsByCode('abc001').map((l) => [l.location, l.block_alloc_order]), [['P-1', 0], ['P-2', 1]]);
});

t('ロケを返すたびに写しを複製する (エンジンが書き換えても次に響かない)', () => {
  const wh = buildWarehouseFromMirror({ rows: FIXTURE, meta: META(), nowMs: NOW });
  wh.locationsByCode('abc001')[0].available_qty = -1;
  assert.equal(wh.locationsByCode('abc001')[0].available_qty, 10);
});

console.log('写しを使わない条件 (直さずに止める)');
const stop = (rows, meta, nowMs = NOW) => buildWarehouseFromMirror({ rows, meta, nowMs });
t('素性が無い・在庫を取った時刻が不明・古い (3 時間超)・未来', () => {
  assert.match(stop(FIXTURE, null).reasons.join(), /素性が無い/);
  assert.match(stop(FIXTURE, META({ source_at: null })).reasons.join(), /時刻」が不明/);
  assert.match(stop(FIXTURE, META({ source_at: '2026-10-04T21:30:00Z' })).reasons.join(), /写しが古い/);
  assert.equal(stop(FIXTURE, META({ source_at: '2026-10-04T21:45:00Z' })).ok, true, '2 時間 55 分前は使う');
  assert.match(stop(FIXTURE, META({ source_at: '2026-10-05T00:45:00Z' })).reasons.join(), /未来/);
});
t('行 0 件・行数が素性と合わない・読み飛ばしあり・CSV の行数と合わない', () => {
  assert.match(stop([], META({ row_count: 0, rows_read: 0 })).reasons.join(), /0 件/);
  assert.match(stop(FIXTURE, META({ row_count: 99 })).reasons.join(), /素性と合わない/);
  assert.match(stop(FIXTURE, META({ skipped_rows: 1 })).reasons.join(), /読み飛ばした行/);
  assert.match(stop(FIXTURE, META({ skipped_rows: null })).reasons.join(), /読み飛ばした行がある \(不明/);
  assert.match(stop(FIXTURE, META({ rows_read: 6 })).reasons.join(), /CSV の行数/);
});
t('世代が混ざっている・素性と世代が違う', () => {
  const mixed = FIXTURE.map((r, i) => (i ? r : { ...r, captured_at: '2026-10-04T23:20:00.000Z' }));
  assert.match(stop(mixed, META()).reasons.join(), /複数の世代/);
  assert.match(stop(FIXTURE, META({ captured_at: '2026-10-04T23:20:00.000Z' })).reasons.join(), /世代が違う/);
});
t('🚨 負の在庫・負の引当・引当が在庫より多い・整数でない は 0 に直さず止める (Codex A2b High 2)', () => {
  for (const bad of [{ 在庫数: -1 }, { 引当数: -1 }, { 在庫数: 3, 引当数: 4 }, { 在庫数: 1.5 }, { 引当数: null }, { 商品ID: '' }]) {
    const rows = [...FIXTURE.slice(1), mrow(bad)];
    const r = stop(rows, META());
    assert.equal(r.ok, false, JSON.stringify(bad));
    assert.match(r.reasons.join(), /おかしい行 1 行/);
    assert.equal(r.summaryRows, undefined, '止めたら在庫を返さない');
  }
});

t('手動 CSV との差: 増えた・減った・絶対差・片方にしか無い を分ける (相殺して見えなくしない)', () => {
  const d = diffWarehouse(
    [{ logizard_code: 'A', warehouse_available: 10 }, { logizard_code: 'B', warehouse_available: 0 }, { logizard_code: 'C', warehouse_available: 4 }],
    [{ logizard_code: 'a', warehouse_available: 5 }, { logizard_code: 'B', warehouse_available: 5 }, { logizard_code: 'D', warehouse_available: 1 }]);
  assert.deepEqual({ ...d, top: undefined }, { codes_changed: 4, plus: 9, minus: 6, abs: 15, only_mirror: 1, only_manual: 1, top: undefined });
  assert.deepEqual(d.top.map((x) => [x.code, x.diff]), [['a', 5], ['b', -5], ['c', 4], ['d', -1]]);
});

t('写しと素性を 1 回の読み取りトランザクションで読む', () => {
  const m = new Database(':memory:');
  m.exec(`CREATE TABLE mirror_logizard_stock (商品ID TEXT, 商品名 TEXT, ブロック略称 TEXT, ロケ TEXT, 有効期限 TEXT, 在庫数 INTEGER, 引当数 INTEGER,
    ロケ業務区分 TEXT, 最終入荷日 TEXT, ブロック引当順 INTEGER, captured_at TEXT);
    CREATE TABLE mirror_logizard_stock_meta (id INTEGER PRIMARY KEY, captured_at TEXT, source_at TEXT, rows_read INTEGER, skipped_rows INTEGER, row_count INTEGER);`);
  const ins = m.prepare('INSERT INTO mirror_logizard_stock VALUES (?,?,?,?,?,?,?,?,?,?,?)');
  for (const r of FIXTURE) ins.run(r.商品ID, r.商品名, r.ブロック略称, r.ロケ, r.有効期限, r.在庫数, r.引当数, r.ロケ業務区分, r.最終入荷日, r.ブロック引当順, r.captured_at);
  const mt = META();
  m.prepare('INSERT INTO mirror_logizard_stock_meta VALUES (1,?,?,?,?,?)').run(mt.captured_at, mt.source_at, mt.rows_read, mt.skipped_rows, mt.row_count);
  const got = readMirrorWarehouse(m);
  assert.equal(got.rows.length, FIXTURE.length);
  assert.equal(buildWarehouseFromMirror({ ...got, nowMs: NOW }).ok, true);
});

// ---------------------------------------------------------------------------------------------
console.log('今朝のレポート・取り込み・準備中・関所');

const FRESH = (o = {}) => ({
  restock_source_at: '2026-10-04 22:42:00', restock_source_max: '2026-10-04 22:42:00', restock_source_missing: 0,
  planning_source_at: '2026-10-04 22:43:10', planning_source_max: '2026-10-04 22:43:10', planning_source_missing: 0,
  warehouse_uploaded_at: '2026-10-04 18:00:00', ...o,
});
t('JST の日付と時刻・最後の回・起動時の追いつき', () => {
  assert.deepEqual(jstClock(Date.parse('2026-10-04T15:30:00Z')), { date: '2026-10-05', minutes: 30 });
  assert.equal(isFinalAttempt(Date.parse('2026-10-05T02:39:59Z')), false);
  assert.equal(isFinalAttempt(Date.parse('2026-10-05T02:40:00Z')), true, '11:40 JST');
  assert.equal(shouldCatchUpAtStartup(Date.parse('2026-10-05T00:39:00Z')), false, '09:39 は cron に任せる');
  assert.equal(shouldCatchUpAtStartup(Date.parse('2026-10-05T00:40:00Z')), true);
});
t('今朝のレポート: 両方とも 今日 05:00 JST ≤ 取った時刻 ≤ いま', () => {
  assert.deepEqual(reportsFromThisMorning(FRESH(), NOW), []);
  const old = reportsFromThisMorning(FRESH({ planning_source_at: '2026-10-04 19:59:59' }), NOW);
  assert.deepEqual(old.map((r) => r.code), ['report_not_this_morning'], '04:59:59 JST は前日扱い');
  assert.match(old[0].detail, /PLANNING/);
  assert.deepEqual(reportsFromThisMorning(FRESH({ restock_source_at: '2026-10-04 20:00:00' }), NOW), [], '05:00 ちょうどは今朝');
  assert.deepEqual(reportsFromThisMorning(FRESH({ restock_source_max: '2026-10-05 00:45:00' }), NOW).map((r) => r.code), ['report_future'],
    '🚨 未来の行が 1 行でも混ざっていたら止める (いちばん古い行だけ見ない)');
  assert.deepEqual(reportsFromThisMorning(FRESH({ restock_source_missing: 3 }), NOW).map((r) => r.code), ['report_not_this_morning']);
  assert.equal(reportsFromThisMorning(FRESH({ planning_source_at: null, planning_source_max: null }), NOW).length, 1);
});
t('取り込みの結果: 失敗・落ちた・保存しなかった (件数急減ガード) を理由にする', () => {
  assert.deepEqual(syncReasons({ ok: true }), []);
  assert.equal(syncReasons(null)[0].code, 'report_sync_failed');
  assert.equal(syncReasons({ ok: false, thrown: 'x' })[0].code, 'report_sync_failed');
  assert.equal(syncReasons({ ok: false, error: 'ミニPC' })[0].code, 'report_sync_failed');
  assert.deepEqual(syncReasons({ ok: true, restock_skip_reason: '急減', planning_latest_error: 'disk' }).map((r) => r.code),
    ['report_sync_skipped', 'report_sync_failed']);
});
t('準備中: キャッシュが取り直しより前・未来 なら fresh にしない (Codex A2b High 3)', () => {
  const refresh = { ok: true, count: 1 };
  const cache = (at) => ({ ok: true, data: { s1: 3 }, cachedAt: at });
  const recv = Date.parse('2026-10-05T00:41:00Z');
  const req = recv - 30e3;
  assert.equal(judgeInboundFetch({ refresh, cache: cache(recv - 10e3), nowMs: recv, requestedMs: req }).source, 'fresh');
  assert.equal(judgeInboundFetch({ refresh, cache: cache(req - 60e3), nowMs: recv, requestedMs: req }).source, 'inconsistent');
  assert.equal(judgeInboundFetch({ refresh, cache: cache(recv + 120e3), nowMs: recv, requestedMs: req }).source, 'inconsistent');
});
t('関所: 写しの時刻 (warehouse_source_at) で倉庫の鮮度を見る・未来なら止める', () => {
  const base = { inboundState: { source: 'fresh' }, dq: {}, now: new Date(NOW) };
  const fr = { ...FRESH(), warehouse_uploaded_at: '2026-09-01 10:00:00' };
  assert.deepEqual(inputGate({ ...base, inputFreshness: { ...fr, warehouse_source_at: '2026-10-05T00:05:00.000Z' } }).reasons, [],
    '手動 CSV が古くても、写しで計算した日は写しの時刻で見る');
  assert.deepEqual(inputGate({ ...base, inputFreshness: { ...fr, warehouse_source_at: '2026-10-05T01:00:00.000Z' } }).reasons.map((r) => r.code), ['warehouse_stale']);
  assert.deepEqual(inputGate({ ...base, inputFreshness: fr }).reasons.map((r) => r.code), ['warehouse_stale'], '写しが無ければ手動 CSV の時刻');
});

// ---------------------------------------------------------------------------------------------
console.log('1 回の試行の流れ (PGlite)');

const pg = new PGlite(); const pdb = pgliteAdapter(pg);
await applyMigrations(pdb, { log: quiet });
const q = (sql, p) => pdb.query(sql, p);

const GAPS_OK = {
  sales_7d_missing: false, sales_30d_missing: false, planning_missing: false, warehouse_row_missing: false,
  warehouse_missing_components: [], amazon_reco_missing: true, inbound_working_source: 'api', zero_filled: [],
};
const item = (o = {}) => ({
  amazon_sku: 'abc001', product_name: 'テスト商品', ne_code: 'abc001', is_set: false, invalid_mapping: false, stock_state: 'normal',
  fba_available: 3, effective_fba_stock: 3, fba_inbound_working_effective: 0, units_sold_7d: 7, units_sold_30d: 30, daily_sales: 1,
  days_of_supply: 3, reorder_point: 14, reorder_point_days: 14, target_days: 60, target_stock: 60, warehouse_available: 100,
  recommended_qty: 57, rounded_qty: 60, adjusted_qty: 60, amazon_recommended_qty: null, alerts: [], needs_replenishment: true,
  ...o, data_gaps: { ...GAPS_OK, ...(o.data_gaps || {}) },
});
const engineResult = (items, o = {}) => ({
  items, generated_at: '2026-10-05T00:40:00.000Z', snapshot_date: '2026-10-05', total_skus: items.length, errors: [],
  data_quality: { data_source: 'sp_api', unmapped_active: [] }, ...o,
});

/** 外の世界の偽物。呼ばれた回数と ping を記録する */
function makeDeps(o = {}) {
  const calls = { ping: [], generate: 0, sync: 0, inbound: 0, closed: 0, queries: [] };
  let freshCalls = 0;
  const deps = {
    openClient: async () => ({
      db: {
        query: async (sql, p) => {
          calls.queries.push(sql);
          if (o.lockBusy && /pg_try_advisory_lock/.test(sql)) return { rows: [{ ok: false }] };
          return pdb.query(sql, p);
        },
      },
      close: async () => { calls.closed++; },
    }),
    syncReports: async () => { calls.sync++; if (o.syncThrows) throw new Error('miniPC 応答なし'); return o.sync ?? { ok: true, snapshot_date: '2026-10-05' }; },
    fetchInbound: async () => { calls.inbound++; return o.inbound ?? { data: { abc001: 0 }, state: { source: 'fresh', count: 1, at: 'x' } }; },
    readMirror: () => { if (o.mirrorThrows) throw new Error('warehouse-mirror.db が初期化されていません'); return o.mirror ?? { rows: FIXTURE, meta: META() }; },
    readInputFreshness: () => { freshCalls++; return (o.freshness2 && freshCalls > 1) ? o.freshness2 : (o.freshness ?? FRESH()); },
    readManualWarehouseSummary: () => [{ logizard_code: 'abc001', warehouse_available: 7 }],
    generate: (inbound, opts) => {
      calls.generate++;
      calls.generateOpts = opts;
      if (o.generateThrows) throw new Error('snapshot なし');
      return o.result ?? engineResult([item()]);
    },
    readSettings: () => ({ self_reserve_mode: 'equal_days' }),
    ping: (status, note) => calls.ping.push([status, note]),
  };
  return { deps, calls };
}
const at = (iso) => { const ms = Date.parse(iso); return () => ms; };
const runSummaries = async () => (await q(
  `select status, inputs_ref from ai.decisions where dedupe_key = $1 and inputs_ref->>'generator' = $2 order by decision_id`,
  [RUN_SUMMARY_KEY, GENERATOR])).rows;
const jobRuns = async () => (await q(`select status, summary from ops.job_runs where job_id = $1 order by job_run_id`, [DECISION_JOB_ID])).rows;
const openProposals = async () => (await q(
  `select dedupe_key, (inputs_ref->>'adjusted_qty')::int as qty, inputs_ref->>'run_id' as run_id from ai.decisions
    where status = 'new' and decision_kind = 'proposal' and inputs_ref->>'generator' = $1`, [GENERATOR])).rows;

// 前日の提案 (これが「決めた日」「決められない日」に superseded になるか・「待機」で残るかを見る)
{
  const { deps } = makeDeps({ freshness: FRESH({
    restock_source_at: '2026-10-03 22:42:00', restock_source_max: '2026-10-03 22:42:00',
    planning_source_at: '2026-10-03 22:43:00', planning_source_max: '2026-10-03 22:43:00',
  }), mirror: { rows: FIXTURE.map((r) => ({ ...r, captured_at: '2026-10-04T00:20:00.000Z' })), meta: META({ captured_at: '2026-10-04T00:20:00.000Z', source_at: '2026-10-04T00:05:00.000Z' }) },
  result: engineResult([item({ adjusted_qty: 11 })]) });
  const r = await runDecisionAttempt(deps, { nowMs: at('2026-10-04T00:40:00Z'), log: quiet });
  assert.equal(r.outcome, 'decided', `前日の準備 (${JSON.stringify(r.detail)})`);
}

await ta('09:40 に入力がそろっていない (レポートが前日) → 待つ: ai.decisions は書かない・前日の提案は残す・ping しない・job_runs に「待機」', async () => {
  const before = (await runSummaries()).length;
  const { deps, calls } = makeDeps({ freshness: FRESH({ restock_source_at: '2026-10-04 12:00:00' }) });
  const r = await runDecisionAttempt(deps, { nowMs: at('2026-10-05T00:40:00Z'), log: quiet });
  assert.equal(r.outcome, 'waiting');
  assert.deepEqual(r.detail.reasons.map((x) => x.code), ['report_not_this_morning']);
  assert.equal((await runSummaries()).length, before);
  assert.deepEqual((await openProposals()).map((p) => p.qty), [11], '前日の提案は待機では消さない');
  assert.deepEqual(calls.ping, []);
  const jr = (await jobRuns()).at(-1);
  assert.equal(jr.status, 'partial');
  assert.match(jr.summary, /^待機 2026-10-05 \(cron\): report_not_this_morning/);
  assert.ok(calls.queries.some((s) => /pg_advisory_unlock/.test(s)), 'ロックを外す');
  assert.equal(calls.closed, 1, '接続を閉じる');
});

await ta('10:40 にそろった → 決める: 写しの倉庫在庫でエンジンを回し、business_date・decision_final・手動 CSV との差を残す / ping ok', async () => {
  const { deps, calls } = makeDeps();
  const r = await runDecisionAttempt(deps, { nowMs: at('2026-10-05T01:40:00Z'), log: quiet, trigger: 'cron' });
  assert.equal(r.outcome, 'decided');
  assert.equal(calls.generate, 1);
  assert.equal(calls.generateOpts.warehouse.baseAtMs, Date.parse('2026-10-05T00:05:00.000Z'), 'エンジンへ写しの倉庫在庫と基準時刻');
  const last = (await runSummaries()).at(-1).inputs_ref;
  assert.equal(last.business_date, '2026-10-05');
  assert.equal(last.decision_final, true);
  assert.equal(last.job_id, DECISION_JOB_ID);
  assert.equal(last.warehouse_input.source, 'logizard_mirror');
  assert.equal(last.warehouse_input.source_at, '2026-10-05T00:05:00.000Z');
  assert.equal(last.warehouse_input.diff_vs_manual.only_mirror, 2, '手動 CSV に無い構成品 (def002, xyz999)');
  assert.equal(last.input_freshness.warehouse_source_at, '2026-10-05T00:05:00.000Z');
  assert.deepEqual((await openProposals()).map((p) => p.qty), [60], '前日の提案は superseded、今日の提案だけ');
  assert.deepEqual(calls.ping.map((p) => p[0]), ['ok'], '提案を記録できたら ok (Company DB に出品が無い・未マップがあって job_runs が partial でも)');
  assert.match((await jobRuns()).at(-1).summary, /提案 1 件/);
});

await ta('11:40 (もう決めた日) → 何もしない: 取り込みも計算もしない・書かない・ping しない', async () => {
  const before = { s: (await runSummaries()).length, j: (await jobRuns()).length };
  const { deps, calls } = makeDeps({ generateThrows: true });
  const r = await runDecisionAttempt(deps, { nowMs: at('2026-10-05T02:40:00Z'), log: quiet });
  assert.equal(r.outcome, 'already_decided');
  assert.equal(calls.sync + calls.inbound + calls.generate, 0);
  assert.deepEqual(calls.ping, []);
  assert.equal((await runSummaries()).length, before.s);
  assert.equal((await jobRuns()).length, before.j);
  assert.deepEqual((await openProposals()).map((p) => p.qty), [60], '🚨 あとの回の失敗が、決めた提案を消さない (Codex A2b High 1)');
});

await ta('🚨 ロックを別の試行が持っている → 何もしない (Render の新旧が重なったとき)', async () => {
  const before = (await jobRuns()).length;
  const { deps, calls } = makeDeps({ lockBusy: true });
  const r = await runDecisionAttempt(deps, { nowMs: at('2026-10-06T00:40:00Z'), log: quiet });
  assert.equal(r.outcome, 'locked');
  assert.equal(calls.sync + calls.generate, 0);
  assert.equal((await jobRuns()).length, before);
  assert.ok(!calls.queries.some((s) => /pg_advisory_unlock/.test(s)), '取れていないロックは外さない');
  assert.equal(calls.closed, 1);
});

await ta('計算が落ちた回 → 失敗を記録 (前日以前の提案も無効)・ping fail・決めたことにしない → 次の回で決める', async () => {
  const { deps, calls } = makeDeps({ generateThrows: true, freshness: FRESH({
    restock_source_at: '2026-10-05 22:42:00', restock_source_max: '2026-10-05 22:42:00',
    planning_source_at: '2026-10-05 22:43:00', planning_source_max: '2026-10-05 22:43:00',
  }), mirror: { rows: FIXTURE.map((r) => ({ ...r, captured_at: '2026-10-06T00:20:00.000Z' })), meta: META({ captured_at: '2026-10-06T00:20:00.000Z', source_at: '2026-10-06T00:05:00.000Z' }) } });
  const r = await runDecisionAttempt(deps, { nowMs: at('2026-10-06T00:40:00Z'), log: quiet });
  assert.equal(r.outcome, 'engine_failed');
  assert.deepEqual(calls.ping.map((p) => p[0]), ['fail']);
  assert.match(calls.ping[0][1], /計算が落ちた: snapshot なし/);
  assert.equal((await jobRuns()).at(-1).status, 'fail');
  assert.deepEqual(await openProposals(), [], '計算できなかった日は前日の提案も使えない');
  const r2 = await runDecisionAttempt(makeDeps({ freshness: FRESH({
    restock_source_at: '2026-10-05 22:42:00', restock_source_max: '2026-10-05 22:42:00',
    planning_source_at: '2026-10-05 22:43:00', planning_source_max: '2026-10-05 22:43:00',
  }), mirror: { rows: FIXTURE.map((x) => ({ ...x, captured_at: '2026-10-06T01:20:00.000Z' })), meta: META({ captured_at: '2026-10-06T01:20:00.000Z', source_at: '2026-10-06T01:05:00.000Z' }) } }).deps,
  { nowMs: at('2026-10-06T01:40:00Z'), log: quiet });
  assert.equal(r2.outcome, 'decided');
});

await ta('11:40 でもそろわない (写しが古い) → 「今日は決められない」を記録・前日の提案を無効・ping partial・計算しない', async () => {
  const { deps, calls } = makeDeps({ freshness: FRESH({
    restock_source_at: '2026-10-06 22:42:00', restock_source_max: '2026-10-06 22:42:00',
    planning_source_at: '2026-10-06 22:43:00', planning_source_max: '2026-10-06 22:43:00',
  }) });   // 写しは 10/5 のまま
  const r = await runDecisionAttempt(deps, { nowMs: at('2026-10-07T02:40:00Z'), log: quiet });
  assert.equal(r.outcome, 'gated_final');
  assert.equal(calls.generate, 0);
  assert.deepEqual(calls.ping.map((p) => p[0]), ['partial']);
  assert.match(calls.ping[0][1], /warehouse_mirror_not_ready/);
  const last = (await runSummaries()).at(-1).inputs_ref;
  assert.equal(last.gated, true);
  assert.equal(last.business_date, '2026-10-07');
  assert.equal(last.decision_final, true, '決められない も「その日の結論」');
  assert.deepEqual(await openProposals(), []);
  // 13:00 に再起動して追いつき → もう結論が出ているので何もしない
  const again = await runDecisionAttempt(makeDeps().deps, { nowMs: at('2026-10-07T04:00:00Z'), log: quiet, trigger: 'startup' });
  assert.equal(again.outcome, 'already_decided');
});

await ta('写しを読めない (DB の準備前) → 投げずに理由にする', async () => {
  const { deps, calls } = makeDeps({ mirrorThrows: true });
  const r = await runDecisionAttempt(deps, { nowMs: at('2026-10-08T00:40:00Z'), log: quiet });
  assert.equal(r.outcome, 'waiting');
  assert.match(r.detail.reasons.map((x) => x.detail).join(), /写しを読めない/);
  assert.equal(calls.generate, 0);
});

await ta('取り込みが落ちた・準備中が取れない → 理由を全部残す', async () => {
  const { deps } = makeDeps({ syncThrows: true, inbound: { data: {}, state: { source: 'failed', count: 0, error: 'Access denied' } } });
  const r = await runDecisionAttempt(deps, { nowMs: at('2026-10-08T00:41:00Z'), log: quiet });
  assert.equal(r.outcome, 'waiting');
  const codes = r.detail.reasons.map((x) => x.code);
  assert.ok(codes.includes('report_sync_failed') && codes.includes('inbound_working_not_fresh'), codes.join());
});

await ta('計算の途中で表が替わった (あり得ないはずだが) → 決めない', async () => {
  const fr = FRESH({ restock_source_at: '2026-10-07 22:42:00', restock_source_max: '2026-10-07 22:42:00', planning_source_at: '2026-10-07 22:43:00', planning_source_max: '2026-10-07 22:43:00' });
  const mirror = { rows: FIXTURE.map((x) => ({ ...x, captured_at: '2026-10-08T00:20:00.000Z' })), meta: META({ captured_at: '2026-10-08T00:20:00.000Z', source_at: '2026-10-08T00:05:00.000Z' }) };
  const { deps } = makeDeps({ freshness: fr, freshness2: { ...fr, restock_source_max: '2026-10-08 00:10:00' }, mirror });
  const r = await runDecisionAttempt(deps, { nowMs: at('2026-10-08T00:42:00Z'), log: quiet });
  assert.equal(r.outcome, 'waiting');
  assert.deepEqual(r.detail.reasons.map((x) => x.code), ['report_changed_during_compute']);
});

await ta('時間切れ (試行が 25 分を超えた) → 記録しない・ping fail', async () => {
  const fr = FRESH({ restock_source_at: '2026-10-07 22:42:00', restock_source_max: '2026-10-07 22:42:00', planning_source_at: '2026-10-07 22:43:00', planning_source_max: '2026-10-07 22:43:00' });
  const mirror = { rows: FIXTURE.map((x) => ({ ...x, captured_at: '2026-10-08T00:20:00.000Z' })), meta: META({ captured_at: '2026-10-08T00:20:00.000Z', source_at: '2026-10-08T00:05:00.000Z' }) };
  const { deps, calls } = makeDeps({ freshness: fr, mirror });
  const start = Date.parse('2026-10-08T00:40:00Z');
  let n = 0;
  const clock = () => (n++ === 0 ? start : start + ATTEMPT_TIMEOUT_MS + 1000);
  const before = (await runSummaries()).length;
  const r = await runDecisionAttempt(deps, { nowMs: clock, log: quiet });
  assert.equal(r.outcome, 'timeout');
  assert.equal((await runSummaries()).length, before);
  assert.deepEqual(calls.ping.map((p) => p[0]), ['fail']);
});

await ta('runDecisionAttemptSafe: 接続できない → 投げずに ping fail / 同じプロセスで重ねない', async () => {
  const { deps, calls } = makeDeps();
  const r = await runDecisionAttemptSafe({ ...deps, openClient: async () => { throw new Error('ECONNREFUSED'); } }, { log: quiet });
  assert.equal(r.outcome, 'error');
  assert.deepEqual(calls.ping.map((p) => p[0]), ['fail']);
  let release;
  const slow = { ...makeDeps().deps, openClient: () => new Promise((res) => { release = () => res({ db: { query: async () => ({ rows: [{ ok: false }] }) }, close: async () => {} }); }) };
  const p1 = runDecisionAttemptSafe(slow, { log: quiet });
  const p2 = await runDecisionAttemptSafe(slow, { log: quiet });
  assert.equal(p2.outcome, 'busy');
  release();
  assert.equal((await p1).outcome, 'locked');
});

console.log(`\n${passed} 件 ok${process.exitCode ? ' (NG あり)' : ''}`);
