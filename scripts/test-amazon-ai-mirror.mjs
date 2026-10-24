/**
 * test-amazon-ai-mirror.mjs — Amazon AI の分析の表を Render の mirror へ送る (lib/amazon-ai-mirror.js・送り手 sync-amazon-ai-mirror.js・受け手 warehouse-mirror)
 *
 * 一時の DATA_DIR (mirror) と :memory: の warehouse.db (元の表は取得の ensure* で作る) だけ。本番に触れない。
 * 固定する契約:
 *   A 決まり: 送り手の「残す日」と受け手の消す SQL が同じ / 送る期間 (毎朝・初回・止まった後・--full) / run の分け方
 *   B 列: 送る列は元の表にあり型が同じ / mirror の表の列 = 送る列 + 監査の 3 列 / facts-amazon.js の TABLES の mirror_ の名前を全部作る
 *   C 送る行: ERROR だけ・JP だけ・月末でない古い写しは送らない・返品と補てんは注文番号などを送らず row_key
 *   D 往復: 初回 (残す日数ぶん・run を小分け・stage を通る) / 翌日 (置き換え・行の消えた日を mirror でも消す) / 止まった後 (埋め直す・古い写しを消す)
 *   E 失敗: 1 つの表が送れない = その表だけ ❌ (他は送る)・次の回に続きから / 表が無い・空 = ⏭️ (送らない・mirror はそのまま・失敗にしない)
 *   F 受け手: 未来の scope_to・日付の形の悪い行を断る
 *   G facts-amazon.js (ai-insights。このリポジトリにある or env AMAZON_FACTS_PATH) が mirror を読んで、元の表を読んだときと同じ答え (週次・月次)
 * 使い方: node scripts/test-amazon-ai-mirror.mjs   (facts の確かめ: AMAZON_FACTS_PATH=<.../apps/ai-insights/facts-amazon.js>)
 */
import { temporaryTestDataDir } from './test-temp-dir.mjs';
await temporaryTestDataDir(import.meta.url, 'amz-ai-mirror-');

import assert from 'node:assert/strict';
import fs from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath, pathToFileURL } from 'node:url';
import express from 'express';
import Database from 'better-sqlite3';

process.env.ALLOW_INSECURE_MIRROR_SYNC = '1';   // 認証はこの試験の対象外
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

const L = await import('../lib/amazon-ai-mirror.js');
const S = await import('../apps/warehouse/sync-amazon-ai-mirror.js');
const { ensureSalesTrafficTables } = await import('../apps/warehouse/fetch-amazon-sales-traffic.js');
const { ensureListingTables } = await import('../apps/warehouse/fetch-amazon-listing-issues.js');
const { ensureReturnsTable } = await import('../apps/warehouse/fetch-amazon-returns.js');
const { ensureReimbursementsTable } = await import('../apps/warehouse/fetch-amazon-reimbursements.js');
const { ensureReportFetchDays } = await import('../apps/warehouse/sp-api-report.js');
const { ensureRankTables } = await import('../apps/warehouse/fetch-amazon-bsr.js');
const { ensurePlanningHistoryTables } = await import('../apps/warehouse/fba-planning-history.js');
const { ensureAttribTables } = await import('../apps/warehouse/fetch-amazon-ads.js');
const { initMirrorDB, getMirrorDB } = await import('../apps/warehouse-mirror/db.js');
const mirrorRouter = (await import('../apps/warehouse-mirror/router.js')).default;

let passed = 0, failed = 0;
async function ta(name, fn) {
  try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { failed++; console.error(`  NG  ${name}\n      ${e.stack || e.message}`); }
}

initMirrorDB();
const mdb = getMirrorDB();
const app = express();
app.use('/apps/mirror', express.json({ limit: '32mb' }), mirrorRouter);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const base = `http://127.0.0.1:${server.address().port}/apps/mirror`;
const httpPost = S.makeHttpPost({ renderUrl: base, syncKey: '' });
const quiet = () => {};

// ── 元の warehouse.db (:memory:) ──
function syncTables(db) {
  // apps/warehouse/db.js と同じ定義 (送り手が使う 2 表)
  db.exec(`CREATE TABLE IF NOT EXISTS sync_contracts (
    entity TEXT PRIMARY KEY, contract_version INTEGER NOT NULL, source_system TEXT NOT NULL, source_object TEXT NOT NULL,
    target_table TEXT NOT NULL, grain_definition TEXT NOT NULL, key_columns_json TEXT NOT NULL, payload_schema_json TEXT NOT NULL,
    clear_strategy TEXT NOT NULL, apply_mode TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1, owner TEXT NOT NULL,
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL)`);
  db.exec(`CREATE TABLE IF NOT EXISTS sync_runs (
    run_id TEXT PRIMARY KEY, entity TEXT NOT NULL, contract_version INTEGER NOT NULL, source_host TEXT NOT NULL,
    scope_from TEXT NOT NULL CHECK(scope_from GLOB '????-??-??'), scope_to TEXT NOT NULL CHECK(scope_to GLOB '????-??-??'),
    chunk_count_expected INTEGER NOT NULL, chunk_count_received INTEGER NOT NULL DEFAULT 0, row_count_received INTEGER NOT NULL DEFAULT 0,
    payload_checksum_manifest TEXT,
    status TEXT NOT NULL CHECK (status IN ('started','applied','failed','backed_out')),
    replay_of_run_id TEXT, error_message TEXT, started_at TEXT NOT NULL, completed_at TEXT, applied_at TEXT,
    FOREIGN KEY (entity) REFERENCES sync_contracts(entity))`);
}
function sourceDb() {
  const db = new Database(':memory:');
  syncTables(db);
  ensureSalesTrafficTables(db); ensureListingTables(db); ensureReturnsTable(db); ensureReimbursementsTable(db);
  ensureReportFetchDays(db); ensureRankTables(db); ensurePlanningHistoryTables(db); ensureAttribTables(db);
  return db;
}
/** 行を入れる。NOT NULL で値の無い列は型に合わせて埋める (取得の監査の列など) */
function insertRows(db, table, rows) {
  const info = db.prepare(`PRAGMA table_info(${table})`).all();
  const cache = new Map();
  db.transaction(() => {
    for (const r of rows) {
      const o = {};
      for (const c of info) {
        if (Object.hasOwn(r, c.name)) o[c.name] = r[c.name];
        else if (c.notnull && c.dflt_value == null) o[c.name] = /INT|REAL/i.test(c.type) ? 0 : 'x';
      }
      const keys = Object.keys(o);
      const sig = keys.join(',');
      if (!cache.has(sig)) cache.set(sig, db.prepare(`INSERT OR REPLACE INTO ${table} (${keys.join(', ')}) VALUES (${keys.map((k) => '@' + k).join(', ')})`));
      cache.get(sig).run(o);
    }
  })();
}

const TODAY = '2026-08-12';   // 過去の日 (受け手は未来の scope_to を断る)
const DATA_FROM = '2026-03-25';
const N = 30;
const pad = (n) => String(n).padStart(4, '0');
const skuOf = (n) => `SKU-${n}`;
const asinOf = (n) => `B0TEST${pad(n)}`;
const RETURNS = 'GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA';
const REIMB = 'GET_FBA_REIMBURSEMENTS_DATA';

/** 元の表に from〜to の日の行を入れる (同じ日を入れ直すと置き換え) */
function seed(db, from, to, { bsrBoost = 1 } = {}) {
  const days = L.datesBetween(from, to);
  const R = { st: [], sku: [], ls: [], li: [], lr: [], ret: [], rb: [], sp: [], bsr: [], pl: [], pd: [], ad: [], af: [] };
  days.forEach((d, i) => {
    const idx = Math.round((Date.parse(`${d}T00:00:00Z`) - Date.parse(`${DATA_FROM}T00:00:00Z`)) / 86400000);
    const sessions = 1000 + (idx % 7) * 13;
    R.st.push({ date_jst: d, sessions, page_views: sessions * 2, units_ordered: 50 + (idx % 5), ordered_product_sales: (50 + (idx % 5)) * 2000, buy_box_percentage: 90 + (idx % 3), unit_session_percentage: 5, report_id: 'r', generation: 1, fetched_at: `${d}T22:00:00Z` });
    R.lr.push({ date_jst: d, status: 'complete', listings: N, started_at: `${d}T21:00:00Z`, finished_at: `${d}T21:10:00Z` });
    for (const rt of [RETURNS, REIMB, 'GET_SALES_AND_TRAFFIC_REPORT']) R.sp.push({ report_type: rt, date_jst: d, generation: 1, report_id: 'r', window_from: d, window_to: d, row_count: 1, fetched_at: `${d}T22:00:00Z` });
    for (const market of ['jp', 'us']) R.pd.push({ snapshot_date: d, market, fetched_at: `${d}T20:00:00Z`, saved_at: `${d}T20:01:00Z`, report_rows: N, saved_rows: N, skipped_no_stock: 0, missing_columns: '[]' });
    const fetched = L.addDaysYmd(d, 30) < TODAY ? L.addDaysYmd(d, 30) : TODAY;
    R.af.push({ report_type: 'spAdvertisedProduct', profile_id: 'p1', date_jst: d, generation: 1, report_id: 'r', window_from: d, window_to: d, row_count: 10, cost_total: 1, fetched_at: `${fetched}T01:00:00Z` });
    for (let n = 1; n <= N; n++) {
      const s = 20 + n;
      let units = Math.round(s * 0.1);
      if (n === 1 && d >= '2026-08-03') units = 0;            // 週次: 転換率が落ちた
      if (n === 2 && d >= '2026-07-01' && d < '2026-08-01') units = 0;   // 月次: 転換率が落ちた
      const bb = n === 3 && d >= '2026-07-01' ? 70 : 95;     // カート獲得率が落ちた
      R.sku.push({ date_jst: d, seller_sku: skuOf(n), child_asin: asinOf(n), parent_asin: 'B0PARENT00', sessions: s, page_views: s * 2, buy_box_percentage: bb, units_ordered: units, ordered_product_sales: units * 1500, report_id: 'r', generation: 1, fetched_at: `${d}T22:00:00Z` });
      const broken = n === 4 && d >= '2026-07-20';
      R.ls.push({ date_jst: d, sku_key: skuOf(n).toLowerCase(), seller_sku: skuOf(n), marketplace_id: 'A1VC38T7YXB528', asin: asinOf(n), item_name: `商品${n}`,
        status: broken ? 'DISCOVERABLE' : 'BUYABLE,DISCOVERABLE', is_buyable: broken ? 0 : 1, is_discoverable: 1, error_count: broken ? 1 : 0, warning_count: n === 5 ? 1 : 0,
        suppressed: 0, search_suppressed: 0, source: 'scan', fetched_at: `${d}T21:05:00Z` });
      if (broken) R.li.push({ date_jst: d, sku_key: 'sku-4', issue_key: '8541|ERROR|', code: '8541', severity: 'ERROR', message: '出品できません', fetched_at: `${d}T21:05:00Z` });
      if (broken || n === 5) R.li.push({ date_jst: d, sku_key: skuOf(n).toLowerCase(), issue_key: '99|WARNING|', code: '99', severity: 'WARNING', message: '注意', fetched_at: `${d}T21:05:00Z` });
      const worse = (n === 6 && d >= '2026-07-01') ? 3 * bsrBoost : (n === 8 && d >= '2026-08-03') ? 3 : 1;   // 6 = 月次 (7 月から)・8 = 週次 (8/3 から)
      R.bsr.push({ date_jst: d, marketplace_id: 'A1VC38T7YXB528', asin: asinOf(n), rank_type: 'display_group', category_id: 'home_display_on_website', category_title: 'ホーム&キッチン', rank: Math.round((1000 + n * 100) * worse), observed_at: `${d}T21:00:00Z` });
      R.bsr.push({ date_jst: d, marketplace_id: 'A1VC38T7YXB528', asin: asinOf(n), rank_type: 'classification', category_id: 'cls-1', category_title: '収納', rank: 200 + n * 10, observed_at: `${d}T21:00:00Z` });
      for (const market of ['jp', 'us']) {
        R.pl.push({ snapshot_date: d, market, amazon_sku: skuOf(n), asin: asinOf(n), available: 10, your_price: 1980, estimated_excess_qty: n % 3, estimated_storage_cost_next_month: 12.5,
          recommended_action: 'NoAction', units_shipped_t30: 5, inv_age_181_270: n % 4, inv_age_271_365: n % 2, inv_age_365_plus: 0, ais_est_181_210: n * 1.5, ais_est_365_plus: n === 7 ? 300 : null });
      }
      if (n <= 10) {
        const cost = 500 + n * 10;
        R.ad.push({ date_jst: d, campaign_id: 'c1', ad_group_id: 'g1', target: skuOf(n).toLowerCase(), target_granularity: 'sku', campaign_name: 'キャンペーン', ad_group_name: 'G',
          advertised_sku: skuOf(n), advertised_asin: asinOf(n), impressions: 100, clicks: 10, cost, sales_14d: n === 1 ? 200 : cost * 4, source_rows: 1, generation: 1, report_id: 'r', ingested_at: `${fetched}T01:00:00Z` });
      }
    }
    if (idx % 3 === 0) {
      const n = (idx % 5) + 1;
      R.ret.push({ order_id: `250-${idx}-1`, seller_sku: skuOf(n), fnsku: 'X00', return_date: `${d}T10:00:00+09:00`, license_plate_number: `LPN${idx}`, occurrence: 1, return_date_jst: d,
        asin: asinOf(n), product_name: `商品${n}`, quantity: 1, reason: idx % 2 ? 'DEFECTIVE' : 'UNWANTED_ITEM', customer_comments: '個人の言葉', report_id: 'r', generation: 1, first_seen_at: `${d}T22:00:00Z`, fetched_at: `${d}T22:00:00Z` });
    }
    if (idx % 5 === 0) {
      R.rb.push({ reimbursement_id: `R${idx}`, seller_sku: skuOf(1), fnsku: 'X00', occurrence: 1, approval_date: `${d}T10:00:00+09:00`, approval_date_jst: d, case_id: 'C1', amazon_order_id: `250-${idx}-9`,
        reason: 'Lost_Warehouse', asin: asinOf(1), amount_total: 1200, quantity_reimbursed_total: 1, report_id: 'r', generation: 1, first_seen_at: `${d}T22:00:00Z`, fetched_at: `${d}T22:00:00Z` });
    }
  });
  insertRows(db, 'amazon_sales_traffic_daily', R.st);
  insertRows(db, 'amazon_sales_traffic_sku_daily', R.sku);
  insertRows(db, 'amazon_listing_status_daily', R.ls);
  insertRows(db, 'amazon_listing_issue_daily', R.li);
  insertRows(db, 'amazon_listing_fetch_runs', R.lr);
  insertRows(db, 'amazon_fba_returns', R.ret);
  insertRows(db, 'amazon_fba_reimbursements', R.rb);
  insertRows(db, 'sp_report_fetch_days', R.sp);
  insertRows(db, 'amazon_catalog_rank_daily', R.bsr);
  insertRows(db, 'fba_planning_daily', R.pl);
  insertRows(db, 'fba_planning_daily_days', R.pd);
  insertRows(db, 'fact_amazon_ads_sku_attrib', R.ad);
  insertRows(db, 'ads_attrib_fetch_days', R.af);
}

const count = (db, sql, ...p) => db.prepare(sql).get(...p).n;
/** 元の表から「mirror にあるべき行の数」(送る行の絞り込み + 残す日) */
function expectedCount(src, spec, today) {
  const { keepFrom } = L.retentionBounds(spec, today);
  const w = spec.where ? `AND ${spec.where}` : '';
  const dates = src.prepare(`SELECT ${spec.dateCol} AS d, COUNT(*) AS n FROM ${spec.source} WHERE ${spec.dateCol} >= ? AND ${spec.dateCol} <= ? ${w} GROUP BY 1`).all(keepFrom, today);
  return dates.filter((r) => L.isKeptDate(spec, r.d, today)).reduce((s, r) => s + r.n, 0);
}
const mirrorCount = (spec) => count(mdb, `SELECT COUNT(*) AS n FROM ${spec.table}`);
const spec = (name) => L.amazonAiEntity(name);

// ════════ A. 決まり ════════
await ta('[A1] 送り手の「残す日」(isKeptDate) と受け手の消す SQL (retentionDeleteSql) が同じ答え (月の長さ・うるう年・境目)', async () => {
  const db = new Database(':memory:');
  for (const sp of L.AMAZON_AI_MIRROR_ENTITIES) {
    for (const scopeTo of ['2026-11-12', '2026-03-01', '2028-03-02', '2026-12-31', '2027-01-03']) {
      db.exec(`DROP TABLE IF EXISTS ${sp.table}`);
      db.exec(`CREATE TABLE ${sp.table} (${sp.dateCol} TEXT)`);
      const all = L.datesBetween(L.addDaysYmd(scopeTo, -(sp.keepDays + 40)), scopeTo);
      const ins = db.prepare(`INSERT INTO ${sp.table} VALUES (?)`);
      for (const d of all) ins.run(d);
      const { keepFrom, recentFrom } = L.retentionBounds(sp, scopeTo);
      db.prepare(L.retentionDeleteSql(sp)).run(sp.keepRecentDays ? { keepFrom, recentFrom } : { keepFrom });
      const left = db.prepare(`SELECT ${sp.dateCol} AS d FROM ${sp.table} ORDER BY 1`).all().map((r) => r.d);
      assert.deepEqual(left, all.filter((d) => L.isKeptDate(sp, d, scopeTo)), `${sp.entity} @ ${scopeTo}`);
    }
  }
  // 毎日の写しの表: 14 日より前は月の最後の 4 日だけ
  const ls = spec('amazon_listing_status_daily');
  assert.equal(L.isKeptDate(ls, '2026-10-31', '2026-11-12'), true);
  assert.equal(L.isKeptDate(ls, '2026-10-28', '2026-11-12'), true);
  assert.equal(L.isKeptDate(ls, '2026-10-27', '2026-11-12'), false);
  assert.equal(L.isKeptDate(ls, '2026-10-30', '2026-11-12'), true);
  assert.equal(L.isKeptDate(ls, '2026-10-29', '2026-11-12'), true);
  assert.equal(L.isKeptDate(ls, '2026-10-15', '2026-11-12'), false);
  assert.equal(L.isKeptDate(ls, '2026-07-31', '2026-11-12'), true);    // 120 日の中
  assert.equal(L.isKeptDate(ls, '2026-06-30', '2026-11-12'), false);   // 120 日より前は月末も消す
  assert.equal(L.isKeptDate(ls, '2026-02-25', '2026-03-31'), true);   // 2 月 (28 日) は 25〜28 日
  assert.equal(L.isKeptDate(ls, '2026-02-24', '2026-03-31'), false);
});

await ta('[A2] 送る期間: 初回 = 残す日数ぶん / 毎朝 = 直近 sendDays 日 / 止まった後 = 前に送れた次の日から / --full / 残す日数より前には戻らない', async () => {
  const sp = spec('amazon_catalog_rank_daily');   // sendDays 3・keepDays 100
  const T0 = '2026-11-12';
  assert.deepEqual(S.planScope(sp, { today: T0 }), { from: L.addDaysYmd(T0, -(sp.keepDays - 1)), to: T0, mode: 'first' });
  assert.deepEqual(S.planScope(sp, { today: T0, lastTo: '2026-11-11' }), { from: '2026-11-10', to: T0, mode: 'daily' });
  assert.deepEqual(S.planScope(sp, { today: T0, lastTo: T0 }), { from: '2026-11-10', to: T0, mode: 'daily' });   // 同じ日の 2 回目
  assert.deepEqual(S.planScope(sp, { today: T0, lastTo: '2026-11-01' }), { from: '2026-10-31', to: T0, mode: 'catch_up' });
  assert.deepEqual(S.planScope(sp, { today: T0, lastTo: '2026-01-01' }), { from: L.addDaysYmd(T0, -(sp.keepDays - 1)), to: T0, mode: 'catch_up' });
  assert.deepEqual(S.planScope(sp, { today: T0, lastTo: '2026-11-11', full: true }), { from: L.addDaysYmd(T0, -(sp.keepDays - 1)), to: T0, mode: 'full' });
  // 前の回が途中で失敗 (--full の途中など) = その run の最初の日から今日まで
  assert.deepEqual(S.planScope(sp, { today: T0, lastTo: '2026-11-11', resume: '2026-09-20' }), { from: '2026-09-20', to: T0, mode: 'catch_up' });
  assert.deepEqual(S.planScope(sp, { today: T0, lastTo: '2026-11-11', resume: '2026-11-12' }), { from: '2026-11-10', to: T0, mode: 'daily' });
  assert.deepEqual(S.planScope(sp, { today: T0, lastTo: '2026-11-11', resume: '2026-01-01' }), { from: L.addDaysYmd(T0, -(sp.keepDays - 1)), to: T0, mode: 'catch_up' });
});

await ta('[A3] run の分け方: 古い順・すきまも重なりも無い・runDays 日まで', async () => {
  const runs = S.splitRuns('2026-08-30', '2026-11-12', 5);
  assert.equal(runs[0].from, '2026-08-30');
  assert.equal(runs.at(-1).to, '2026-11-12');
  for (let i = 1; i < runs.length; i++) assert.equal(runs[i].from, L.addDaysYmd(runs[i - 1].to, 1));
  for (const r of runs) assert.ok(L.datesBetween(r.from, r.to).length <= 5);
  assert.deepEqual(S.splitRuns('2026-11-12', '2026-11-12', 5), [{ from: '2026-11-12', to: '2026-11-12' }]);
});

// ════════ B. 列 ════════
const SRC = sourceDb();
await ta('[B1] 送る列・主キー・row_key の元の列は元の表 (取得の ensure* が作る) にあり、型が同じ', async () => {
  for (const sp of L.AMAZON_AI_MIRROR_ENTITIES) {
    const info = new Map(SRC.prepare(`PRAGMA table_info(${sp.source})`).all().map((c) => [c.name, c]));
    assert.ok(info.size > 0, `元の表が無い: ${sp.source}`);
    for (const [c, type] of sp.cols) {
      if (c === 'row_key') continue;
      assert.ok(info.has(c), `${sp.source}.${c} が元の表に無い`);
      assert.equal(info.get(c).type.toUpperCase(), type.split(' ')[0], `${sp.source}.${c} の型`);
      assert.equal(info.get(c).notnull === 1, / NOT NULL/.test(type), `${sp.source}.${c} の NOT NULL`);
    }
    for (const c of sp.rowKey || []) assert.ok(info.has(c), `${sp.source}.${c} (row_key の元) が元の表に無い`);
    // row_key の元 = 元の表の主キー (同じ行 = 同じ row_key)
    if (sp.rowKey) assert.deepEqual([...info.values()].filter((c) => c.pk > 0).sort((a, b) => a.pk - b.pk).map((c) => c.name), sp.rowKey, `${sp.source} の主キー`);
    else assert.deepEqual([...info.values()].filter((c) => c.pk > 0).sort((a, b) => a.pk - b.pk).map((c) => c.name), sp.pk, `${sp.source} の主キー = mirror の主キー`);
  }
});

await ta('[B2] mirror の表 = 送る列 (型も同じ) + 監査の 3 列・主キー同じ・日付の索引', async () => {
  for (const sp of L.AMAZON_AI_MIRROR_ENTITIES) {
    const cols = mdb.prepare(`PRAGMA table_info(${sp.table})`).all();
    assert.deepEqual(cols.map((c) => c.name), [...sp.cols.map(([c]) => c), 'source_run_id', 'source_row_hash', 'synced_at'], sp.table);
    for (const [c, type] of sp.cols) assert.equal(cols.find((x) => x.name === c).type, type.split(' ')[0], `${sp.table}.${c}`);
    assert.deepEqual(cols.filter((c) => c.pk > 0).sort((a, b) => a.pk - b.pk).map((c) => c.name), sp.pk);
    if (sp.pk[0] !== sp.dateCol) {
      assert.ok(mdb.prepare(`SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?`).get(`idx_${sp.table}_date`), `${sp.table} の日付の索引`);
    }
  }
  // 返品・補てんは注文番号・顧客のコメントの列が無い
  for (const t of ['mirror_amazon_fba_returns', 'mirror_amazon_fba_reimbursements']) {
    const names = mdb.prepare(`PRAGMA table_info(${t})`).all().map((c) => c.name);
    for (const bad of ['order_id', 'amazon_order_id', 'customer_comments', 'license_plate_number', 'case_id']) assert.ok(!names.includes(bad), `${t}.${bad}`);
  }
});

// facts-amazon.js (ai-insights のブランチ。マージ後はこのリポジトリ・前は env で場所を渡す)
const factsPath = process.env.AMAZON_FACTS_PATH || path.join(ROOT, 'apps/ai-insights/facts-amazon.js');
const FA = fs.existsSync(factsPath) ? await import(pathToFileURL(factsPath).href) : null;
if (!FA) console.log(`  --  facts-amazon.js が無い (${factsPath}) = [B3] [G*] は飛ばす (AMAZON_FACTS_PATH で場所を渡せる)`);

await ta('[B3] facts-amazon.js の TABLES の mirror_ の名前を全部作る (カート価格は前から mirror にある)', async () => {
  if (!FA) return;
  for (const [key, cands] of Object.entries(FA.TABLES)) {
    assert.ok(cands[0].startsWith('mirror_'), key);
    if (key === 'price') { assert.equal(cands[0], 'mirror_amazon_price_snapshot_daily'); continue; }
    assert.ok(L.AMAZON_AI_MIRROR_ENTITIES.some((sp) => sp.table === cands[0] && cands.includes(sp.source)), `${key}: ${cands[0]} を送っていない`);
  }
  assert.equal(L.AMAZON_AI_MIRROR_ENTITIES.length, Object.keys(FA.TABLES).length - 1);
});

// ════════ C. 送る行 ════════
seed(SRC, DATA_FROM, TODAY);
await ta('[C1] 送る行の絞り込み: ERROR だけ・JP だけ・月末でない古い写しは送らない・row_key (16 桁) で注文番号を送らない', async () => {
  const keepTo = TODAY;
  const issues = S.selectRunRows(SRC, spec('amazon_listing_issue_daily'), { from: '2026-08-01', to: TODAY, keepTo });
  assert.ok(issues.length > 0 && issues.every((r) => r.severity === 'ERROR'));
  assert.deepEqual(Object.keys(issues[0]), ['date_jst', 'sku_key', 'issue_key', 'code', 'severity', 'message']);
  const pl = S.selectRunRows(SRC, spec('fba_planning_daily'), { from: TODAY, to: TODAY, keepTo });
  assert.equal(pl.length, N);
  assert.ok(pl.every((r) => r.market === 'jp'));
  const ls = S.selectRunRows(SRC, spec('amazon_listing_status_daily'), { from: '2026-07-20', to: '2026-08-01', keepTo });
  assert.deepEqual([...new Set(ls.map((r) => r.date_jst))], ['2026-07-28', '2026-07-29', '2026-07-30', '2026-07-31', '2026-08-01']);   // 14 日 (7/30〜) より前 = 月末の 4 日だけ
  const ret = S.selectRunRows(SRC, spec('amazon_fba_returns'), { from: DATA_FROM, to: TODAY, keepTo });
  assert.ok(ret.length > 0);
  assert.deepEqual(Object.keys(ret[0]), ['row_key', 'return_date_jst', 'seller_sku', 'asin', 'product_name', 'quantity', 'reason']);
  assert.ok(ret.every((r) => /^[0-9a-f]{16}$/.test(r.row_key)));
  assert.equal(new Set(ret.map((r) => r.row_key)).size, ret.length, 'row_key が重ならない');
  assert.ok(!JSON.stringify(ret).includes('250-') && !JSON.stringify(ret).includes('個人の言葉'), '注文番号・コメントを送らない');
  const rb = S.selectRunRows(SRC, spec('amazon_fba_reimbursements'), { from: DATA_FROM, to: TODAY, keepTo });
  assert.ok(rb.length > 0 && !JSON.stringify(rb).includes('250-'));
});

// ════════ D. 往復 ════════
/** 送り口を数える (chunk の数・表ごと) */
function countingPost(inner = httpPost, failFor = null) {
  const calls = [];
  const post = async (entity, body) => {
    calls.push({ entity, rows: body.row_count, chunk: body.chunk_index, of: body.chunk_count, from: body.scope_from, to: body.scope_to });
    if (failFor && failFor(entity, body)) return { ok: false, status: 500, json: null, text: 'injected failure' };
    return inner(entity, body);
  };
  return { post, calls };
}

let first;
await ta('[D1] 初回: 全部の表を残す日数ぶん送る (run は runDays 日ずつ・小さい chunk = stage を通る)・mirror の行 = 元の表の送る行', async () => {
  const c = countingPost();
  const outcomes = await S.syncAmazonAiMirror(SRC, { today: TODAY, post: c.post, chunkSize: 100, log: quiet });
  first = { outcomes, calls: c.calls };
  assert.deepEqual(outcomes.filter((o) => o.status !== 'sent'), []);
  for (const sp of L.AMAZON_AI_MIRROR_ENTITIES) {
    assert.equal(mirrorCount(sp), expectedCount(SRC, sp, TODAY), sp.entity);
    assert.ok(mirrorCount(sp) > 0, sp.entity);
    const o = outcomes.find((x) => x.entity === sp.entity);
    assert.equal(o.scope.from, L.addDaysYmd(TODAY, -(sp.keepDays - 1)), `${sp.entity} は残す日数ぶん`);
    assert.equal(o.runs, S.splitRuns(o.scope.from, TODAY, sp.runDays).length);
    // sync_runs: run ごとに applied
    assert.equal(count(SRC, `SELECT COUNT(*) AS n FROM sync_runs WHERE entity = ? AND status = 'applied'`, sp.entity), o.runs);
  }
  // 小さい chunk = 複数 chunk の run があった (受け手の stage を通る)
  assert.ok(c.calls.some((x) => x.of > 1));
  assert.equal(count(mdb, 'SELECT COUNT(*) AS n FROM sync_stage_rows'), 0, 'stage は空に戻る');
  // 残す日の決まり
  assert.equal(count(mdb, `SELECT COUNT(*) AS n FROM mirror_amazon_listing_status_daily WHERE date_jst = '2026-06-15'`), 0);
  assert.equal(count(mdb, `SELECT COUNT(*) AS n FROM mirror_amazon_listing_status_daily WHERE date_jst = '2026-06-30'`), N);
  assert.equal(count(mdb, `SELECT COUNT(*) AS n FROM mirror_amazon_catalog_rank_daily WHERE date_jst < ?`, L.addDaysYmd(TODAY, -(spec('amazon_catalog_rank_daily').keepDays - 1))), 0);
  assert.equal(count(mdb, `SELECT COUNT(*) AS n FROM mirror_amazon_sales_traffic_daily`), L.datesBetween(DATA_FROM, TODAY).length, '店全体は 400 日 = 全部');
  assert.equal(count(mdb, `SELECT COUNT(*) AS n FROM mirror_fba_planning_daily WHERE market <> 'jp'`), 0);
  assert.equal(count(mdb, `SELECT COUNT(*) AS n FROM mirror_amazon_listing_issue_daily WHERE severity <> 'ERROR'`), 0);
  // 監査の列
  const r = mdb.prepare('SELECT source_run_id, source_row_hash, synced_at FROM mirror_amazon_fba_returns LIMIT 1').get();
  assert.match(r.source_run_id, /^amazon_fba_returns-v1-/);
  assert.match(r.source_row_hash, /^[0-9a-f]{16}$/);
  // 値はそのまま (型も)
  const a = SRC.prepare(`SELECT sessions, buy_box_percentage, ordered_product_sales FROM amazon_sales_traffic_sku_daily WHERE date_jst = ? AND seller_sku = 'SKU-3'`).get(TODAY);
  const b = mdb.prepare(`SELECT sessions, buy_box_percentage, ordered_product_sales, typeof(sessions) AS t1, typeof(buy_box_percentage) AS t2 FROM mirror_amazon_sales_traffic_sku_daily WHERE date_jst = ? AND seller_sku = 'SKU-3'`).get(TODAY);
  assert.deepEqual([b.sessions, b.buy_box_percentage, b.ordered_product_sales], [a.sessions, a.buy_box_percentage, a.ordered_product_sales]);
  assert.deepEqual([b.t1, b.t2], ['integer', 'real']);
});

await ta('[D2] 1 回の送りの大きさ: run ごとの行と JSON のバイト数を数える (受け口 12MB・chunk 3000 行)', async () => {
  const o = first.outcomes;
  for (const x of o) assert.ok(x.bytes > 0 && Number.isInteger(x.rows));
  // 1 chunk の JSON は受け口 (12MB) よりずっと小さい (このデータの 1 行の大きさ × 3000 行で見積もる)
  for (const x of o) {
    if (!x.rows) continue;
    const perRow = x.bytes / x.rows;
    if (process.env.SHOW_BYTES) console.log(`      ${x.entity}: 1 行 ${perRow.toFixed(0)} bytes (JSON)`);
    assert.ok(perRow * S.DEFAULT_CHUNK_SIZE < 4 * 1024 * 1024, `${x.entity}: 1 行 ${perRow.toFixed(0)} bytes`);
  }
});

// G (facts) は元の表と mirror が同じ日まである初回の後に見る
const WEEK = { periodStart: '2026-08-03', periodEnd: '2026-08-10', prevStart: '2026-07-27', granularity: 'week',
  baseline: [1, 2, 3, 4].map((w) => [L.addDaysYmd('2026-08-03', -7 * w), L.addDaysYmd('2026-08-03', -7 * (w - 1))]) };
const MONTH = { periodStart: '2026-07-01', periodEnd: '2026-08-01', prevStart: '2026-06-01', granularity: 'month',
  baseline: [['2026-06-01', '2026-07-01'], ['2026-05-01', '2026-06-01'], ['2026-04-01', '2026-05-01']] };
const ITEMS = ['store_traffic', 'sku_conversion_drops', 'listing_problems', 'returns', 'reimbursements', 'bsr_drops', 'long_term_storage', 'ads_poor_roas'];
for (const [label, p] of [['週次', WEEK], ['月次', MONTH]]) {
  await ta(`[G] facts-amazon.js が mirror を読める・元の表を読んだときと同じ答え (${label})`, async () => {
    if (!FA) return;
    const onMirror = FA.buildAmazonHealthFacts(mdb, p);
    const onSource = FA.buildAmazonHealthFacts(SRC, p);
    for (const k of ITEMS) {
      assert.equal(onMirror[k].status, 'ok', `${label} ${k}: ${JSON.stringify(onMirror[k]).slice(0, 300)}`);
      assert.deepEqual(onMirror[k], onSource[k], `${label} ${k}`);
    }
    // 中身がある (見つかる SKU・ASIN がある = 比べた意味がある)
    assert.ok(onMirror.sku_conversion_drops.items.length > 0, 'SKU の転換率');
    assert.ok(onMirror.listing_problems.problem_skus_total > 0, '出品の問題');
    assert.ok(onMirror.listing_problems.items[0].top_error, 'ERROR の中身');
    assert.ok(onMirror.bsr_drops.items.length > 0, 'BSR');
    assert.ok(onMirror.ads_poor_roas.items.length > 0, '広告');
    assert.ok(onMirror.long_term_storage.top_skus.length > 0, '長期保管料');
    assert.ok(onMirror.returns.returned_units > 0 && onMirror.reimbursements.cases > 0, '返品・補てん');
    assert.equal(onMirror.listing_problems.snapshot, 'complete');
    assert.equal(onMirror.listing_problems.as_of, p.granularity === 'month' ? '2026-07-31' : '2026-08-09');
  });
}

await ta('[D3] 翌日: 直近だけ送り直す・変わった値を置き換え・元で消えた行を mirror でも消す・残す日数より古い行を受け手が消す', async () => {
  const day2 = L.addDaysYmd(TODAY, 1);
  seed(SRC, TODAY, day2, { bsrBoost: 2 });   // 今日の BSR が変わる + 明日の行
  SRC.prepare(`DELETE FROM amazon_sales_traffic_sku_daily WHERE date_jst = ? AND seller_sku = 'SKU-9'`).run(TODAY);
  const c = countingPost();
  const outcomes = await S.syncAmazonAiMirror(SRC, { today: day2, post: c.post, chunkSize: 100, log: quiet });
  assert.deepEqual(outcomes.filter((o) => o.status !== 'sent'), []);
  for (const o of outcomes) {
    const sp = spec(o.entity);
    assert.equal(o.scope.from, L.addDaysYmd(day2, -(sp.sendDays - 1)), `${o.entity}: 直近 ${sp.sendDays} 日`);
    assert.equal(o.scope.to, day2);
    assert.equal(mirrorCount(sp), expectedCount(SRC, sp, day2), o.entity);
  }
  assert.equal(count(mdb, `SELECT COUNT(*) AS n FROM mirror_amazon_sales_traffic_sku_daily WHERE date_jst = ? AND seller_sku = 'SKU-9'`, TODAY), 0, '元で消えた行');
  assert.equal(mdb.prepare(`SELECT rank FROM mirror_amazon_catalog_rank_daily WHERE date_jst = ? AND asin = ? AND rank_type = 'display_group'`).get(TODAY, asinOf(6)).rank, (1000 + 600) * 6);
  assert.equal(count(mdb, `SELECT COUNT(*) AS n FROM mirror_amazon_catalog_rank_daily WHERE date_jst < ?`, L.addDaysYmd(day2, -(spec('amazon_catalog_rank_daily').keepDays - 1))), 0, 'BSR の残す日数より古い行');
  assert.equal(count(mdb, `SELECT COUNT(*) AS n FROM mirror_amazon_catalog_rank_daily WHERE date_jst = ?`, L.addDaysYmd(day2, -(spec('amazon_catalog_rank_daily').keepDays - 1))), 2 * N);
  // 1 日の送りの大きさ = 直近の日数だけ
  const rowsSent = c.calls.reduce((s, x) => s + x.rows, 0);
  assert.ok(rowsSent < first.calls.reduce((s, x) => s + x.rows, 0) / 3, `翌日の送り ${rowsSent} 行`);
});

await ta('[D4] 期間の中に行が無い日 = mirror でもその日を空にする (元が正)', async () => {
  const day2 = L.addDaysYmd(TODAY, 1);
  const sp = spec('amazon_fba_reimbursements');
  const before = mirrorCount(sp);
  const cut = L.addDaysYmd(day2, -(sp.sendDays - 1));
  const gone = SRC.prepare(`DELETE FROM amazon_fba_reimbursements WHERE approval_date_jst >= ?`).run(cut).changes;
  assert.ok(gone > 0);
  const outcomes = await S.syncAmazonAiMirror(SRC, { today: day2, post: httpPost, entities: [sp.entity], log: quiet });
  assert.equal(outcomes[0].status, 'sent');
  assert.equal(count(mdb, `SELECT COUNT(*) AS n FROM mirror_amazon_fba_reimbursements WHERE approval_date_jst >= ?`, cut), 0);
  assert.equal(mirrorCount(sp), before - gone);
  assert.ok(mirrorCount(sp) > 0, '期間より前の行は残る');
});

// ════════ E. 失敗・表が無い・空 ════════
await ta('[E1] 1 つの表が送れない = その表だけ失敗 (他の表は送る・要約は ❌)・次の回に続きから送り直す', async () => {
  const day3 = '2026-08-20';
  seed(SRC, L.addDaysYmd(TODAY, 2), day3);
  const bsr = spec('amazon_catalog_rank_daily');
  const lastBefore = S.lastAppliedTo(SRC, bsr.entity);
  const c = countingPost(httpPost, (entity) => entity === bsr.entity);
  const outcomes = await S.syncAmazonAiMirror(SRC, { today: day3, post: c.post, log: quiet });
  const bad = outcomes.filter((o) => o.status === 'failed');
  assert.deepEqual(bad.map((o) => o.entity), [bsr.entity]);
  assert.match(bad[0].error, /HTTP 500/);
  assert.equal(outcomes.filter((o) => o.status === 'sent').length, L.AMAZON_AI_MIRROR_ENTITIES.length - 1);
  assert.equal(S.lastAppliedTo(SRC, bsr.entity), lastBefore, '失敗した run は applied にしない');
  assert.equal(count(SRC, `SELECT COUNT(*) AS n FROM sync_runs WHERE entity = ? AND status = 'failed'`, bsr.entity), 1);
  assert.equal(count(mdb, `SELECT COUNT(*) AS n FROM mirror_amazon_catalog_rank_daily WHERE date_jst > ?`, lastBefore), 0, 'mirror の BSR はそのまま');
  const line = S.summaryLine(outcomes);
  assert.match(line, /^❌ Amazon AI の mirror: 送った 12\/13 表/);
  assert.match(line, /失敗 1 \(amazon_catalog_rank_daily: HTTP 500/);
  // 止まっていた日 (catch-up) と、毎日の写しの古い日の掃除
  const again = await S.syncAmazonAiMirror(SRC, { today: day3, post: httpPost, entities: [bsr.entity], log: quiet });
  assert.equal(again[0].status, 'sent');
  assert.equal(again[0].scope.from, L.addDaysYmd(L.addDaysYmd(lastBefore, 1), -(bsr.sendDays - 1)), '前に送れた次の日から');
  assert.equal(mirrorCount(bsr), expectedCount(SRC, bsr, day3));
  // 出品の写し: 8/7 より前の 8 月の日 (月末でない) は消えた・7 月末は残る
  const ls = spec('amazon_listing_status_daily');
  assert.equal(count(mdb, `SELECT COUNT(*) AS n FROM mirror_amazon_listing_status_daily WHERE date_jst BETWEEN '2026-08-01' AND '2026-08-06'`), 0);
  assert.equal(count(mdb, `SELECT COUNT(*) AS n FROM mirror_amazon_listing_status_daily WHERE date_jst = '2026-07-31'`), N);
  assert.equal(mirrorCount(ls), expectedCount(SRC, ls, day3));
});

await ta('[E2] 表が無い・行が無い (まだ取り始めていない) = 送らない (⏭️)・mirror はそのまま・失敗にしない', async () => {
  const before = Object.fromEntries(L.AMAZON_AI_MIRROR_ENTITIES.map((sp) => [sp.entity, mirrorCount(sp)]));
  const bare = new Database(':memory:');
  syncTables(bare);
  ensureReturnsTable(bare);   // 表はあるが空
  ensureRankTables(bare);
  insertRows(bare, 'amazon_catalog_rank_daily', []);
  ensurePlanningHistoryTables(bare);   // 在庫計画の表はあるが空
  const c = countingPost();
  const outcomes = await S.syncAmazonAiMirror(bare, { today: TODAY, post: c.post, log: quiet });
  assert.equal(c.calls.length, 0, '何も送らない');
  assert.ok(outcomes.every((o) => o.status === 'skipped'), JSON.stringify(outcomes));
  assert.equal(outcomes.find((o) => o.entity === 'amazon_fba_returns').reason, 'empty');
  assert.equal(outcomes.find((o) => o.entity === 'fba_planning_daily').reason, 'empty');
  assert.equal(S.sourceState(bare, spec('fba_planning_daily')), 'empty');
  assert.equal(outcomes.find((o) => o.entity === 'amazon_sales_traffic_daily').reason, 'missing');
  assert.deepEqual(Object.fromEntries(L.AMAZON_AI_MIRROR_ENTITIES.map((sp) => [sp.entity, mirrorCount(sp)])), before, 'mirror はそのまま');
  const line = S.summaryLine(outcomes);
  assert.match(line, /^✅ Amazon AI の mirror: 送った 0\/13 表/);
  assert.match(line, /⏭️ まだ無い 13/);
  // dry-run は書かない (sync_runs も契約も)
  const dry = await S.syncAmazonAiMirror(SRC, { today: '2026-08-20', dryRun: true, log: quiet });
  assert.ok(dry.every((o) => o.status === 'dry_run' && o.bytes > 0));
  assert.match(S.summaryLine(dry, { dryRun: true }), /^✅ Amazon AI の mirror: 送る見込み 13\/13 表/);
});

await ta('[E3] 契約 (sync_contracts) は送り手が入れる (initDB の前でも送れる)・版の違う契約の表は送らない', async () => {
  const db = sourceDb();
  insertRows(db, 'amazon_listing_fetch_runs', [{ date_jst: TODAY, status: 'complete' }]);
  const ok = await S.syncAmazonAiMirror(db, { today: TODAY, post: httpPost, entities: ['amazon_listing_fetch_runs'], log: quiet });
  assert.equal(ok[0].status, 'sent');
  assert.equal(count(db, `SELECT COUNT(*) AS n FROM sync_contracts WHERE owner = 'ai-insights'`), L.AMAZON_AI_MIRROR_ENTITIES.length);
  db.prepare(`UPDATE sync_contracts SET contract_version = 99 WHERE entity = 'amazon_listing_fetch_runs'`).run();
  const r = await S.syncEntity(db, spec('amazon_listing_fetch_runs'), { today: TODAY, post: httpPost, log: quiet });
  assert.equal(r.status, 'failed');
  assert.match(r.error, /版が違う/);
});

await ta('[E4] 一度送った表は、元が空になっても送る (ERROR が全部直った・表が空になった) = mirror のその日の行を消す', async () => {
  const day3 = '2026-08-20';
  const iss = spec('amazon_listing_issue_daily');
  const scopeFrom = L.addDaysYmd(day3, -(iss.sendDays - 1));
  assert.ok(count(mdb, `SELECT COUNT(*) AS n FROM mirror_amazon_listing_issue_daily WHERE date_jst >= ?`, scopeFrom) > 0);
  SRC.prepare(`DELETE FROM amazon_listing_issue_daily WHERE severity = 'ERROR'`).run();   // WARNING は残る (送る行は 0)
  let o = await S.syncAmazonAiMirror(SRC, { today: day3, post: httpPost, entities: [iss.entity], log: quiet });
  assert.equal(o[0].status, 'sent');
  assert.equal(count(mdb, `SELECT COUNT(*) AS n FROM mirror_amazon_listing_issue_daily WHERE date_jst >= ?`, scopeFrom), 0, '直った ERROR を残さない');
  assert.ok(count(mdb, `SELECT COUNT(*) AS n FROM mirror_amazon_listing_issue_daily WHERE date_jst < ?`, scopeFrom) > 0, '送る期間より前は残る');
  // 表が丸ごと空 (一度は送った) = 空ではなく ok (clear だけ送る)
  const rb = spec('amazon_fba_reimbursements');
  SRC.prepare('DELETE FROM amazon_fba_reimbursements').run();
  assert.equal(S.sourceState(SRC, rb), 'ok');
  o = await S.syncAmazonAiMirror(SRC, { today: day3, post: httpPost, entities: [rb.entity], log: quiet });
  assert.equal(o[0].status, 'sent');
  assert.equal(o[0].rows, 0);
  assert.equal(count(mdb, `SELECT COUNT(*) AS n FROM mirror_amazon_fba_reimbursements WHERE approval_date_jst >= ?`, L.addDaysYmd(day3, -(rb.sendDays - 1))), 0);
});

await ta('[E5] --full が途中で失敗 = 次の毎朝の回は失敗した run の最初の日から今日まで送り直す (古い日を欠けたままにしない)', async () => {
  const day3 = '2026-08-20';
  const bsr = spec('amazon_catalog_rank_daily');
  const runs = S.splitRuns(L.addDaysYmd(day3, -(bsr.keepDays - 1)), day3, bsr.runDays);
  mdb.prepare(`DELETE FROM ${bsr.table}`).run();   // mirror を作り直した (空) の想定
  const c = countingPost(httpPost, (entity, body) => entity === bsr.entity && body.scope_from === runs[2].from);
  let o = await S.syncAmazonAiMirror(SRC, { today: day3, full: true, post: c.post, entities: [bsr.entity], log: quiet });
  assert.equal(o[0].status, 'failed');
  assert.equal(S.resumeFrom(SRC, bsr.entity), runs[2].from);
  assert.equal(count(mdb, `SELECT COUNT(*) AS n FROM ${bsr.table} WHERE date_jst >= ?`, runs[2].from), 0, '失敗した run より後は送っていない');
  o = await S.syncAmazonAiMirror(SRC, { today: day3, post: httpPost, entities: [bsr.entity], log: quiet });
  assert.equal(o[0].status, 'sent');
  assert.equal(o[0].scope.from, runs[2].from);
  assert.equal(S.resumeFrom(SRC, bsr.entity), null);
  assert.equal(mirrorCount(bsr), expectedCount(SRC, bsr, day3));
});

// ════════ F. 受け手 ════════
await ta('[F1] 受け手: 未来の scope_to (古い行を消す基準) は 400・日付の形の悪い行は 400・mirror は変えない', async () => {
  const sp = spec('amazon_fba_returns');
  const before = mirrorCount(sp);
  const runId = 'amazon_fba_returns-v1-test-future';
  const mk = (rows, to) => {
    const chunks = [rows.map((r) => ({ ...r, source_run_id: runId, source_row_hash: 'h', synced_at: 't' }))];
    return S.chunkBody(sp, chunks, 0, { runId, from: to, to });
  };
  const future = L.addDaysYmd(S.jstToday(), 5);
  let r = await httpPost(sp.entity, mk([], future));
  assert.equal(r.status, 400);
  assert.equal(r.json.error, 'scope_to_in_future');
  r = await httpPost(sp.entity, mk([{ row_key: 'abc', return_date_jst: '2026/11/01', seller_sku: 'S', quantity: 1 }], S.jstToday()));
  assert.equal(r.status, 400);
  assert.equal(r.json.error, 'bad_row');
  assert.equal(mirrorCount(sp), before);
  // 古い契約の版は断る
  const body = mk([], S.jstToday());
  r = await httpPost(sp.entity, { ...body, contract_version: 2 });
  assert.equal(r.status, 400);
  assert.equal(r.json.error, 'contract_version_mismatch');
});

// ════════ H. daily-sync の置き場所 ════════
await ta('[H1] daily-sync: 取得 (広告の帰属・在庫計画の履歴・出品・BSR・レポート) の後・DBバックアップの前に 1 回・引数つき・再試行に載せない・写しの反映の門で止めない', async () => {
  const ds = fs.readFileSync(path.join(ROOT, 'apps/warehouse/daily-sync.js'), 'utf8').replace(/\r\n/g, '\n');
  const at = (s) => { const i = ds.indexOf(s); assert.ok(i > 0, s); return i; };
  const iStep = at("runScript('apps/warehouse/sync-amazon-ai-mirror.js --daily', 'Amazon AI mirror sync'");
  assert.equal(ds.split('sync-amazon-ai-mirror.js').length - 1, 1, '1 回だけ');
  for (const before of ["runScript('apps/warehouse/fetch-amazon-ads.js'", "runScript('apps/warehouse/snapshot-fba-stock.js'",
    "runScript('apps/warehouse/fetch-amazon-listing-issues.js'", "runScript('apps/warehouse/fetch-amazon-bsr.js'",
    "runScript('apps/warehouse/fetch-amazon-sales-traffic.js", "runScript('apps/warehouse/fetch-amazon-reimbursements.js",
    '  // ── ここまで Amazon AI 分析用のレポート']) assert.ok(at(before) < iStep, before);
  assert.ok(iStep < at("const backupResult = runScript('apps/warehouse/backup-warehouse.js'"));
  // SP-API の成否の if の外 (失敗の朝も元の表の今の中身を送る)
  const block = ds.slice(at('  // ── Amazon AI 分析用の表を Render の mirror へ (2026-10-24'), at('  // ── Amazon AI 分析用の表を Render の mirror へ ここまで'));
  assert.doesNotMatch(block.replace(/\/\/[^\n]*/g, ''), /if \(|\{\s*$/m);
  const retryable = ds.match(/const RETRYABLE_JOBS = \[([^\]]*)\]/)[1];
  assert.ok(!retryable.includes('Amazon AI mirror sync'));
  const G = await import('../apps/warehouse/publish-gate.js');
  assert.ok(Object.hasOwn(G.PUBLISH_UNGATED_SCRIPTS, 'apps/warehouse/sync-amazon-ai-mirror.js'));
  assert.equal(G.publishGateDecision('apps/warehouse/sync-amazon-ai-mirror.js --daily', { broken: true }).skip, false);
});

await ta('[E6] データの表 (返品・広告の帰属) の送りが失敗した回は、取得の記録 (sp_report_fetch_days・ads_attrib_fetch_days) を送らない = facts が「0 件で正常」と誤らない (Codex round2 P1)', async () => {
  const deps = ['amazon_fba_returns', 'fact_amazon_ads_sku_attrib'];
  const marks = ['sp_report_fetch_days', 'ads_attrib_fetch_days'];
  const before = Object.fromEntries(marks.map((m) => [m, S.lastAppliedTo(SRC, m)]));
  const c = countingPost(httpPost, (entity) => deps.includes(entity));
  const outcomes = await S.syncAmazonAiMirror(SRC, { today: '2026-08-21', post: c.post, entities: [...marks, ...deps, 'amazon_fba_reimbursements'], log: quiet });
  for (const m of marks) {
    const o = outcomes.find((x) => x.entity === m);
    assert.equal(o.status, 'failed', m);
    assert.match(o.error, /データの表が失敗/);
    assert.equal(S.lastAppliedTo(SRC, m), before[m], `${m} は送らない`);
    assert.ok(!c.calls.some((x) => x.entity === m), `${m} の要求は出さない`);
  }
  // 記録は データの表の後に送る順 (返品が通れば、同じ回の補てんの失敗が無い限り記録も送る)
  const idx = (e) => outcomes.findIndex((x) => x.entity === e);
  assert.ok(idx('amazon_fba_reimbursements') < idx('sp_report_fetch_days'));
});

server.close();
console.log(`\n${failed === 0 ? `✅ ${passed} 件すべて通過` : `❌ ${failed} 件失敗 (通過 ${passed})`}`);
process.exitCode = failed === 0 ? 0 : 1;
