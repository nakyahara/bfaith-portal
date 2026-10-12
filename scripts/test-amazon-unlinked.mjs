#!/usr/bin/env node
/**
 * test-amazon-unlinked.mjs — Amazon の出品一覧 (0076 (仮の番号)) と「まだつないでいない Amazon の SKU」(10/12 中原さん) の試験。
 *   PGlite (持ち主のロール deploy で migration・画面のロール master_edit で読む / 隠す) + 本物の router (Company DB の受け口・マスタの入力) を HTTP で +
 *   送り手 (push/amazon-listings.mjs) を本物の履歴の保存 (scripts/mall-items/archive-items.mjs の archiveItems) で作った gz と manifest で回す。
 * 固定する契約:
 *   1 TSV の読み方: 日本語の見出し (日本のマーケットプレイスの今の形)・英語の見出し (SP-API の説明の 1 行目) のどちらでも同じ行になる。出品日 (- と / の表記)・FBA / 自社 (知らない値は null)・
 *     状態・価格 (, を外す)・ASIN (商品ID は ASIN の形のときだけ)。SKU が空・同じ SKU の 2 行目・列の数が違う行は外して数える。seller SKU の列が無い = 例外
 *   2 受け口: 世代 = 取得の時刻。applied → same (同じ世代・中身) / 同じ世代で違う中身 = 409 / 古い世代 = stale / 中身が同じ新しい世代 = refreshed (行は触らない) /
 *     0 行・指紋の食い違い・重複は 400 / 途中で落ちたら全部巻き戻る / 鍵が無ければ 401 / 0076 の前は 409 not_migrated
 *   3 送り手: manifest の最後の complete の TSV の回だけ・sha256 が違えば ❌・Render と同じなら送らない・🛑 行が前の 80% 未満は ❌ (--force で越える)・
 *     外した行が多すぎる = ❌・商品名の列が無い = ⚠️ (送る)・取得が古い = ⚠️・0076 の前 = ⚠️ exit 0
 *   4 画面の読み: 「つないでいる」= 出品に構成がある (切替の前の SKU マスタの構成も)。無い・読めない (0076 の前・権限なし・まだ届いていない) を混ぜない。
 *     絞り込み (区分・状態・出品日・言葉 = かなの同一視・ASIN)・並び (出品日・最後に売れた日・値の無い行は後ろ)・最後に売れた日 (取り消しを除く)・FBA の在庫 (出品 SKU で結ぶ)
 *   5 隠す: 画面のロールは関数だけ (表に直に書けない)・同じ操作 = same・出品一覧に無い = 404・「その他」は文が要る・記録は追記だけ・元に戻す
 *   6 画面: 一覧 (隠すのボタンは名簿の人だけ・Origin が要る)・seller SKU の画面の「Amazon の出品一覧では」(無い = 何も出さない・読めない = 読めない)・つかいかた・一覧からの入口
 *   7 毎朝のランナー: daily-sync の工程・retry の定義と順番・台帳の文
 * 🚨 試験に無いもの: 2 接続の並行 (advisory lock) = test-amazon-unlinked-pg.mjs (本物の PostgreSQL)・本物のレポートの件数 (7,600 行) の所要時間・スマホの見た目 (目視)
 * 使い方: node scripts/test-amazon-unlinked.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import vm from 'node:vm';
import zlib from 'node:zlib';
import { fileURLToPath } from 'node:url';
import express from 'express';
// 広げる道 PR-2: 画面は DB の active に従う (test-amazon-map.mjs と同じ準備)
await import('./fixtures/master-widen-pr1.mjs');
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);
const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries((await import('../config/master-ownership.mjs')).OWNED_COLUMNS.map((k) => [k, 'load'])));

const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { createRoles } = await import('./company-db/create-watch-roles.mjs');
const { createMasterEditRoles, AMAZON_LISTING_SELECT, AMAZON_LISTING_EDIT_FUNCTIONS } = await import('./company-db/create-master-edit-roles.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
const F = await import('./fixtures/amazon-unlinked.mjs');
const I = await import('../apps/company-db/ingest/amazon-listings.mjs');
const P = await import('../apps/company-db/push/amazon-listings.mjs');
const U = await import('../apps/master-edit/amazon-unlinked.mjs');
const { archiveItems } = await import('./mall-items/archive-items.mjs');
const { ingestStockDay } = await import('../apps/company-db/ingest/stock-daily.mjs');
const { default: cdbRouter, __setPgClientFactory: setCdbFactory } = await import('../apps/company-db/router.mjs');
const { default: meRouter, __setPgClientFactory: setMeFactory, __setAmazonChannelsProvider } = await import('../apps/master-edit/router.mjs');
const { MasterWriteError } = await import('../lib/master-write.mjs');

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
let passed = 0, failed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { failed++; console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};
const TMP = fs.mkdtempSync(path.join(os.tmpdir(), 'amzunlinked-test-'));
const MIG = F.migrationsDir();
const cleanups = [() => fs.rmSync(TMP, { recursive: true, force: true })];
if (MIG !== (await import('./company-db/migrate.mjs')).DEFAULT_DIR) cleanups.push(() => fs.rmSync(MIG, { recursive: true, force: true }));
const NOW = Date.now();
const TODAY = F.jstToday(NOW);
const ROWS = F.reportRows(TODAY);

/** 1 つの Company DB (PGlite): 持ち主のロール deploy で migration・見張りと画面のロール・夜間ロード (to = その番号まで) */
async function setupDb({ to = null, load = true } = {}) {
  const pg = new PGlite();
  await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
  await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
  await pg.query('set role deploy');
  const db = pgliteAdapter(pg);
  await applyMigrations(db, { log: quiet, dir: MIG, ...(to ? { to } : {}) });
  await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });
  await createMasterEditRoles(pg, {});
  if (load) {
    const r = await runInitialLoad(db, F.makePlan(), { log: quiet, runId: 'load_fixed', ownership: MASTER_OWNERSHIP, now: new Date(NOW - 3600e3), host: 'test' });
    assert.equal(r.ok, true, r.error);
  }
  return { pg, db };
}
async function asRole(pg, role, fn) {
  await pg.query(`set role ${role}`);
  try { return await fn(); } finally { await pg.query('set role deploy'); }
}
const body = (rows, x = {}) => {
  const norm = rows.map((r) => I.listingRowOf(r));
  const gen = x.generation ?? NOW - 9 * 3600e3;
  return { shop_code: I.SHOP_CODE, generation: gen, snapshot_at: new Date(gen).toISOString(), report_run_id: 'r_test_1', checksum: I.listingChecksum(norm), row_count: rows.length, skipped_count: 0, rows, ...x };
};

// ─────────────────────────────────────────────────────────
console.log('1 TSV の読み方');
await ta('日本語の見出しと英語の見出し (SP-API の説明の 1 行目) で同じ行になる / 出品日・区分・状態・価格・ASIN の読み方', async () => {
  const jp = P.parseListingReportTsv(F.buildTsv(ROWS, F.HEADER_JP));
  const en = P.parseListingReportTsv(F.buildTsv(ROWS, F.HEADER_EN));
  assert.deepEqual(jp.rows, en.rows);
  assert.deepEqual([jp.missing, en.missing, jp.skipped, en.skipped], [[], [], [], []]);
  assert.equal(jp.rows.length, 6);
  const by = Object.fromEntries(jp.rows.map((r) => [r.seller_sku, r]));
  assert.deepEqual(by.fbm_1, { seller_sku: 'fbm_1', item_name: '自社発送の商品', asin: 'B00000A005', open_date: F.addDays(TODAY, -60), open_date_raw: `${F.addDays(TODAY, -60)} 11:11:11 JST`,
    fulfillment: 'FBM', channel_raw: 'DEFAULT', status: 'active', status_raw: 'Active', quantity: 5, price: '1280.00' });
  assert.equal(by.fba_old.open_date, F.addDays(TODAY, -800), '/ 区切りの出品日');
  assert.equal(by.parent_1.price, null); assert.equal(by.parent_1.status, 'incomplete'); assert.equal(by.parent_1.fulfillment, 'FBA');
  assert.equal(by.PR_NOCOMP.seller_sku, 'PR_NOCOMP', 'seller SKU は原文のまま (小文字にしない)');
  assert.deepEqual(jp.rows.map((r) => r.seller_sku), [...jp.rows.map((r) => r.seller_sku)].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0)), 'seller SKU の順');
  // 単体の読み方
  assert.deepEqual(['2023-05-12 10:22:33 JST', '2023/5/2 1:00:00 JST', '2019-05-14T03:34:53+00:00', '2023-02-30 00:00:00 JST', '12/05/2023', ''].map(P.openDateOf), ['2023-05-12', '2023-05-02', '2019-05-14', null, null, null]);
  assert.deepEqual(['AMAZON_JP', 'AFN', 'DEFAULT', 'MFN', 'Merchant', '', 'XYZ'].map(P.fulfillmentOf), ['FBA', 'FBA', 'FBM', 'FBM', 'FBM', null, null]);
  assert.deepEqual(['Active', 'inactive', 'Incomplete', '', '出品中'].map(P.statusOf), ['active', 'inactive', 'incomplete', 'unknown', 'unknown']);
});
await ta('商品ID が JAN (数字だけ) = ASIN にしない / 外す行 (SKU が空・同じ SKU の 2 行目・列の数が違う・255 文字超) は数える / seller SKU の列が無い = 例外 / 商品名の列が無い = missing', async () => {
  const rows = [{ ...ROWS[0], asin: '4901234567894' }, { ...ROWS[1], sku: '' }, { ...ROWS[2] }, { ...ROWS[2], name: '2 行目' }, { ...ROWS[3], sku: 'x'.repeat(256) }];
  let text = F.buildTsv(rows, F.HEADER_JP);
  text = text.replace(/\r\n$/, '\r\n途中の改行\tだけの行\r\n');
  const p = P.parseListingReportTsv(text);
  assert.equal(p.rows.length, 2);
  assert.equal(p.rows.find((r) => r.seller_sku === 'pr_a001').asin, null, 'JAN は ASIN にしない');
  assert.equal(p.rows.find((r) => r.seller_sku === 'fba_new1').item_name, '新しい FBA の商品 (まだ納品前)', '同じ SKU は 1 行目を使う');
  assert.deepEqual(p.skipped.map((s) => s.reason.slice(0, 8)), ['seller S', '同じ seller', 'xxxxxxxx', '列の数が 2 ('].map((x) => x.slice(0, 8)));
  assert.throws(() => P.parseListingReportTsv('商品名\t価格\r\nx\t1\r\n'), /seller SKU の列/);
  const noName = P.parseListingReportTsv(F.buildTsv(ROWS, F.HEADER_JP.filter((h) => h !== '商品名')));
  assert.deepEqual(noName.missing, ['item_name']);
  assert.ok(noName.rows.every((r) => r.item_name === null));
  // BOM つき・LF だけでも読める
  assert.equal(P.parseListingReportTsv('\uFEFF' + F.buildTsv(ROWS).replace(/\r\n/g, '\n')).rows.length, 6);
});

// ─────────────────────────────────────────────────────────
console.log('\n2 受け口 (ingest/amazon-listings.mjs)');
const E = await setupDb();
const pg = E.pg, db = E.db;
const parsed = P.parseListingReportTsv(F.buildTsv(ROWS));
await ta('検証: 0 行・指紋の食い違い・重複・未来の世代・snapshot_at と世代の食い違い・知らない店・形の外れた行 = 400', async () => {
  const bad = (b, re) => assert.throws(() => I.validateListingBody(b, { nowMs: NOW }), (e) => e.code === 'BAD_REQUEST' && re.test(e.message), re);
  bad(body([]), /0 行/);
  bad({ ...body(parsed.rows), checksum: 'a'.repeat(64) }, /checksum が届いた行/);
  bad(body([...parsed.rows, parsed.rows[0]]), /重複/);
  bad(body(parsed.rows, { generation: NOW + 3600e3 }), /未来/);
  bad({ ...body(parsed.rows), snapshot_at: new Date(NOW - 1000).toISOString() }, /snapshot_at/);
  bad(body(parsed.rows, { shop_code: 'main@XX' }), /shop_code/);
  const one = (x) => ({ ...body(parsed.rows.slice(0, 1)), rows: [{ ...parsed.rows[0], ...x }] });   // 指紋の前に行の形で断る
  bad(one({ asin: 'b0abc' }), /asin/);
  bad(one({ price: 1980 }), /price/);
  bad(one({ price: '1.234' }), /price/);
  bad(one({ seller_sku: ' a' }), /seller_sku/);
  bad(one({ status: 'Active' }), /status/);
  bad(one({ quantity: -1 }), /quantity/);
  bad(one({ open_date: '2026-02-30' }), /open_date/);
  bad(one({ fulfillment: 'AFN' }), /fulfillment/);
  bad(one({ item_name: 'a' + String.fromCharCode(10) + 'b' }), /item_name/);
  bad({ ...body(parsed.rows), row_count: 3 }, /row_count/);
  // 指紋は行の順によらない・値が変われば変わる
  assert.equal(I.listingChecksum([...parsed.rows].reverse()), I.listingChecksum(parsed.rows));
  assert.notEqual(I.listingChecksum([{ ...parsed.rows[0], item_name: 'x' }, ...parsed.rows.slice(1)]), I.listingChecksum(parsed.rows));
});
const G1 = NOW - 9 * 3600e3;
await ta('applied → same (同じ世代・run・指紋) / 同じ世代で違う中身 = CONFLICT / 古い世代 = stale / 中身が同じ新しい世代 = refreshed (行は触らない)', async () => {
  const r1 = await I.ingestAmazonListings(db, body(parsed.rows, { generation: G1 }), { nowMs: NOW });
  assert.equal(r1.status, 'applied'); assert.equal(r1.rows, 6);
  const rows = (await pg.query(`select seller_sku, sku_norm, item_name, asin, open_date::text as od, fulfillment, status, quantity, price_jpy::text as price from ops.amazon_listing_report order by seller_sku`)).rows;
  assert.equal(rows.length, 6);
  assert.deepEqual(rows.find((r) => r.seller_sku === 'PR_NOCOMP'), { seller_sku: 'PR_NOCOMP', sku_norm: 'pr_nocomp', item_name: 'ハチミツ 500g (出品だけ)', asin: 'B00000A002', od: F.addDays(TODAY, -20), fulfillment: 'FBA', status: 'active', quantity: null, price: '2980.00' });
  const run = (await pg.query(`select status, complete, rows_inserted, source_system, entity, scope_key from ops.ingest_runs where ingest_run_id = $1`, [r1.run_id])).rows[0];
  assert.deepEqual(run, { status: 'success', complete: true, rows_inserted: 6, source_system: 'amazon', entity: 'listing_report', scope_key: I.SHOP_CODE });
  assert.equal((await I.ingestAmazonListings(db, body(parsed.rows, { generation: G1 }), { nowMs: NOW })).status, 'same');
  await assert.rejects(I.ingestAmazonListings(db, body(parsed.rows.slice(1), { generation: G1 }), { nowMs: NOW }), (e) => e.code === 'CONFLICT');
  await assert.rejects(I.ingestAmazonListings(db, body(parsed.rows, { generation: G1, report_run_id: 'other' }), { nowMs: NOW }), (e) => e.code === 'CONFLICT' && /別のレポート/.test(e.message));
  const st = await I.ingestAmazonListings(db, body(parsed.rows.slice(1), { generation: G1 - 1000 }), { nowMs: NOW });
  assert.equal(st.status, 'stale'); assert.equal(st.remote_generation, G1);
  assert.equal(Number((await pg.query('select count(*)::int as n from ops.amazon_listing_report')).rows[0].n), 6, 'stale は書かない');
  const loadsBefore = Number((await pg.query('select count(*)::int as n from ops.amazon_listing_report_loads')).rows[0].n);
  const rf = await I.ingestAmazonListings(db, body(parsed.rows, { generation: G1 + 60000, report_run_id: 'r_test_2' }), { nowMs: NOW });
  assert.equal(rf.status, 'refreshed'); assert.equal(rf.load_id, r1.load_id);
  const st2 = await I.amazonListingsStatus(db);
  assert.equal(st2.load.generation, G1 + 60000); assert.equal(st2.load.report_run_id, 'r_test_2'); assert.ok(st2.load.refreshed_at); assert.equal(st2.rows, 6);
  assert.equal(Number((await pg.query('select count(*)::int as n from ops.amazon_listing_report_loads')).rows[0].n), loadsBefore, 'refreshed は見出しを足さない');
});
await ta('新しい世代で中身が違う = 入れ替え (消えた行は消える) / 途中で落ちたら全部巻き戻る (行・見出し・取込の記録)', async () => {
  const G2 = G1 + 120000;
  const r = await I.ingestAmazonListings(db, body(parsed.rows.filter((x) => x.seller_sku !== 'fba_old'), { generation: G2, report_run_id: 'r_test_3' }), { nowMs: NOW });
  assert.equal(r.status, 'applied'); assert.equal(r.replaced, 6); assert.equal(r.rows, 5);
  assert.equal((await pg.query(`select count(*)::int as n from ops.amazon_listing_report where seller_sku = 'fba_old'`)).rows[0].n, 0);
  const before = (await pg.query(`select (select count(*) from ops.amazon_listing_report)::int as a, (select count(*) from ops.amazon_listing_report_loads)::int as b, (select count(*) from ops.ingest_runs where entity = 'listing_report')::int as c`)).rows[0];
  await assert.rejects(I.ingestAmazonListings(db, body(parsed.rows, { generation: G2 + 1, report_run_id: 'r_test_4' }), { nowMs: NOW, afterWrite: async () => { throw new Error('わざと落とす'); } }), /わざと落とす/);
  const after = (await pg.query(`select (select count(*) from ops.amazon_listing_report)::int as a, (select count(*) from ops.amazon_listing_report_loads)::int as b, (select count(*) from ops.ingest_runs where entity = 'listing_report')::int as c`)).rows[0];
  assert.deepEqual(after, before);
  // 戻す (このあとの試験は 6 行で)
  assert.equal((await I.ingestAmazonListings(db, body(parsed.rows, { generation: G2 + 2, report_run_id: 'r_test_5' }), { nowMs: NOW })).status, 'applied');
});
await ta('0076 の前の DB = NOT_MIGRATED (受け口・状態・画面の読み = 読めない)', async () => {
  const E0 = await setupDb({ to: F.versionBefore0076(MIG), load: false });
  await assert.rejects(I.ingestAmazonListings(E0.db, body(parsed.rows), { nowMs: NOW }), (e) => e.code === 'NOT_MIGRATED');
  await assert.rejects(I.amazonListingsStatus(E0.db), (e) => e.code === 'NOT_MIGRATED');
  const r = await asRole(E0.pg, 'master_edit', () => U.readAmazonReportFor(E0.db, 'pr_a001'));
  assert.deepEqual([r.state, r.reason], ['unreadable', 'not_migrated']);
  const l = await asRole(E0.pg, 'master_edit', () => U.listUnlinked(E0.db, U.normalizeUnlinkedFilters({}), { nowMs: NOW }));
  assert.deepEqual([l.state, l.reason], ['unreadable', 'not_migrated']);
  await E0.pg.close();
});

// ─────────────────────────────────────────────────────────
console.log('\n3 Company DB の受け口 (HTTP) と送り手 (push/amazon-listings.mjs)');
process.env.MIRROR_SYNC_KEY = 'k-test';
process.env.COMPANY_DB_URL = 'postgres://owner@localhost:5432/test';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost:5432/test';
process.env.MASTER_EDITORS = 'Naka@Test';
let cdbTarget = E;   // 受け口がつなぐ DB (0076 の前の試験で差し替える)
setCdbFactory(async () => ({
  query: async (t, p) => {
    if (p && p.length) return cdbTarget.pg.query(t, p);
    if (t.includes(';')) { await cdbTarget.pg.exec(t); return { rows: [] }; }   // set statement_timeout ...; set ... (PGlite の query は 1 文だけ)
    return cdbTarget.pg.query(t);
  },
  end: async () => {}, on: () => {},
}));
const capp = express();
capp.use('/apps/company-db/sync', cdbRouter);
const cserver = http.createServer(capp);
await new Promise((r) => cserver.listen(0, '127.0.0.1', r));
cleanups.push(() => cserver.close());
const CBASE = `http://127.0.0.1:${cserver.address().port}/apps/company-db/sync`;
const HIST = path.join(TMP, 'mall-items-history');
let runN = 0;
const SHOP_ID = 'A6HMLHKUUJC27@A1VC38T7YXB528';   // fetch-listings.js の amazonShopId の形 (<セラーID>@<日本のマーケットプレイス>)
/** 想定利益の夜間と同じ道で履歴を残す (archiveItems・offsite なし)。戻り値 = manifest の行 */
async function archive(text, { at = new Date(NOW - 8 * 3600e3), complete = true, enumStatus = 'ok' } = {}) {
  runN++;
  const r = await archiveItems({ mall: 'amazon', shopId: SHOP_ID, source: 'merchant_listings_all_data', runId: `r_${runN}_${crypto.randomBytes(2).toString('hex')}`, fetchedAt: at.toISOString(),
    format: 'tsv', payload: text, items: text.split('\r\n').length - 2, dest: HIST, noOffsite: true, log: quiet, meta: { complete, enum_status: enumStatus } });
  assert.ok(['archived', 'exists_same'].includes(r.code), JSON.stringify(r));
  return r;
}
const push = (x = {}) => P.pushAmazonListings({ historyDir: HIST, base: CBASE, syncKey: 'k-test', log: quiet, sleep: async () => {}, ...x }).catch((e) => e.result || { ok: false, lastLine: e.message });
await ta('受け口 (HTTP): 鍵が無ければ 401 / 検証に通らなければ 400 / 状態 / server.js は鍵の検査と共通 parser の素通りに amazon-listings を入れている', async () => {
  let r = await fetch(`${CBASE}/amazon-listings/status`);
  assert.equal(r.status, 401);
  r = await fetch(`${CBASE}/amazon-listings`, { method: 'POST', headers: { 'content-type': 'application/json', 'x-sync-key': 'k-test' }, body: JSON.stringify(body([])) });
  assert.equal(r.status, 400);
  r = await fetch(`${CBASE}/amazon-listings/status`, { headers: { 'x-sync-key': 'k-test' } });
  assert.equal(r.status, 200);
  const j = await r.json();
  assert.equal(j.rows, 6); assert.equal(j.load.report_run_id, 'r_test_5');
  const srv = fs.readFileSync(path.join(ROOT, 'server.js'), 'utf8');
  assert.match(srv, /'\/apps\/company-db\/sync\/sku-cost-observed', '\/apps\/company-db\/sync\/amazon-listings'\], companyDbRequireSyncKey\)/);
  assert.match(srv, /startsWith\('\/apps\/company-db\/sync\/amazon-listings'\)\) return next\(\);/);
});
await ta('送り手: 履歴が無い = ❌ / 最後の complete の回を送る (applied) / 2 回目は変わりなし (送らない) / 中身が同じ次の夜 = refreshed', async () => {
  let r = await push();
  assert.equal(r.ok, false); assert.match(r.lastLine, /^❌ .*履歴が無い/);
  // 前の世代の試験の値を消して、送り手の最初の回から
  await pg.query('delete from ops.amazon_listing_report'); await pg.query('delete from ops.amazon_listing_report_loads');
  const text = F.buildTsv(ROWS);
  await archive(text, { at: new Date(NOW - 30 * 3600e3) });
  await archive(F.buildTsv(ROWS.slice(0, 2)), { at: new Date(NOW - 29 * 3600e3), complete: false });   // 部分取得 = 送らない
  const a = await archive(text, { at: new Date(NOW - 8 * 3600e3) });
  r = await push();
  assert.equal(r.ok, true, r.lastLine); assert.equal(r.status, 'applied'); assert.match(r.lastLine, /^✅ Company DB 出品一覧 \(Amazon\): 入れ替えた .*行 6/);
  const st = await I.amazonListingsStatus(db);
  assert.equal(st.load.generation, Date.parse(JSON.parse(fs.readFileSync(path.join(HIST, 'amazon', 'manifest.jsonl'), 'utf8').trim().split('\n').pop()).snapshot_at));
  assert.equal(st.load.report_run_id, JSON.parse(fs.readFileSync(path.join(HIST, 'amazon', 'manifest.jsonl'), 'utf8').trim().split('\n').pop()).run_id);
  assert.ok(a.relFile);
  r = await push();
  assert.equal(r.status, 'unchanged'); assert.match(r.lastLine, /^✅ .*変わりなし/);
  await archive(text, { at: new Date(NOW - 2 * 3600e3) });
  r = await push();
  assert.equal(r.status, 'refreshed'); assert.match(r.lastLine, /^✅ .*取得の時刻だけ進めた/);
  // dry-run は POST しない
  await archive(F.buildTsv(ROWS.slice(0, 5)), { at: new Date(NOW - 1 * 3600e3) });
  r = await push({ dryRun: true });
  assert.equal(r.status, 'dry-run'); assert.equal((await I.amazonListingsStatus(db)).rows, 6);
  r = await push();
  assert.equal(r.status, 'applied'); assert.equal((await I.amazonListingsStatus(db)).rows, 5);
});
await ta('🛑 送り手: 行が前の 80% 未満は送らない (❌・既存の行は残る) / --force で越える (⚠️) / 外した行が多すぎる = ❌ / sha256 が manifest と違う = ❌', async () => {
  await archive(F.buildTsv(ROWS.slice(0, 2)), { at: new Date(NOW - 50 * 60e3) });
  let r = await push();
  assert.equal(r.ok, false); assert.match(r.lastLine, /^❌ .*🛑 安全弁: 行が前の回 5 の 80% 未満 \(2\)/);
  assert.equal((await I.amazonListingsStatus(db)).rows, 5);
  r = await push({ force: true });
  assert.equal(r.ok, true); assert.equal(r.status, 'applied'); assert.match(r.lastLine, /^⚠️ .*--force で安全弁を越えた/);
  assert.equal((await I.amazonListingsStatus(db)).rows, 2);
  // 外した行が多すぎる (20 行 + 2% を超える): SKU が空の行を 30 行
  const many = [...ROWS, ...Array.from({ length: 30 }, (_, i) => ({ ...ROWS[0], sku: '', name: `空 ${i}` }))];
  await archive(F.buildTsv(many), { at: new Date(NOW - 40 * 60e3) });
  r = await push();
  assert.equal(r.ok, false); assert.match(r.lastLine, /外した行が多すぎる \(30 > 20/);
  // sha256 が違う (履歴のファイルを書き換えた)
  const rec = await archive(F.buildTsv(ROWS), { at: new Date(NOW - 30 * 60e3) });
  fs.writeFileSync(rec.file, zlib.gzipSync(Buffer.from(F.buildTsv(ROWS.slice(0, 3)), 'utf8')));
  r = await push();
  assert.equal(r.ok, false); assert.match(r.lastLine, /sha256 が manifest と違う/);
  // 正しい回で戻す (このあとの試験は 6 行)
  await archive(F.buildTsv(ROWS), { at: new Date(NOW - 20 * 60e3) });
  r = await push();
  assert.equal(r.status, 'applied', r.lastLine); assert.equal((await I.amazonListingsStatus(db)).rows, 6);
});
await ta('送り手: 商品名の列が無い = 送るが ⚠️ / 取得が古い (36 時間より前) = ⚠️ / 0076 の前の Render = ⚠️ (exit 0 の扱い) / Render の方が新しい = ⚠️ 送らない', async () => {
  const H2 = path.join(TMP, 'hist2');
  const arch2 = (text, at) => archiveItems({ mall: 'amazon', shopId: SHOP_ID, source: 'merchant_listings_all_data', runId: `h2_${crypto.randomBytes(3).toString('hex')}`, fetchedAt: at.toISOString(), format: 'tsv', payload: text, items: 6, dest: H2, noOffsite: true, log: quiet, meta: { enum_status: 'ok' } });
  await arch2(F.buildTsv(ROWS, F.HEADER_JP.filter((h) => h !== '商品名')), new Date(NOW - 40 * 3600e3));
  // Render の方が新しい
  let r = await push({ historyDir: H2 });
  assert.equal(r.ok, true, r.lastLine); assert.equal(r.status, 'remote_newer'); assert.match(r.lastLine, /^⚠️ .*Render の方が新しい/);
  // 0076 の前の Render
  const E0 = await setupDb({ to: F.versionBefore0076(MIG), load: false });
  cdbTarget = E0;
  try {
    r = await push({ historyDir: H2 });
    assert.equal(r.ok, true, r.lastLine); assert.equal(r.status, 'not_migrated'); assert.match(r.lastLine, /^⚠️ .*migration 0076 がまだ無い/);
  } finally { cdbTarget = E; await E0.pg.close(); }
  // 空の Render へ (別の DB): 商品名の列が無い・古い = ⚠️ で送る
  const E3 = await setupDb({ load: false });
  cdbTarget = E3;
  try {
    r = await push({ historyDir: H2 });
    assert.equal(r.ok, true, r.lastLine); assert.equal(r.status, 'applied');
    assert.match(r.lastLine, /^⚠️ .*入れ替えた.*⚠️ 取得が古い .*見出しに無い列: 商品名/);
    assert.equal((await E3.pg.query('select count(*)::int as n from ops.amazon_listing_report where item_name is null')).rows[0].n, 6);
  } finally { cdbTarget = E; await E3.pg.close(); }
});
await ta('🛑 受け口の安全弁 (#1691 Codex R1 M2): 同じ取引の中で今の行と比べ 80% 未満は SHRINK (409・何も変わらない) / force: true だけ通す / 今の行が 0 = 比べない / force は boolean だけ', async () => {
  const E5 = await setupDb({ load: false });
  try {
    const G = NOW - 5 * 3600e3;
    assert.equal((await I.ingestAmazonListings(E5.db, body(parsed.rows, { generation: G, report_run_id: 's1' }), { nowMs: NOW })).status, 'applied', '今の行が 0 = 比べない');
    await assert.rejects(I.ingestAmazonListings(E5.db, body(parsed.rows.slice(0, 4), { generation: G + 1, report_run_id: 's2' }), { nowMs: NOW }), (e) => e.code === 'SHRINK' && /今の 6 の 80% 未満 \(4\)/.test(e.message));
    assert.equal((await E5.pg.query('select count(*)::int as n from ops.amazon_listing_report')).rows[0].n, 6, 'SHRINK は何も変えない');
    assert.equal((await E5.pg.query(`select count(*)::int as n from ops.ingest_runs where entity = 'listing_report'`)).rows[0].n, 1, 'SHRINK は取込の記録も残さない (巻き戻す)');
    assert.equal((await I.ingestAmazonListings(E5.db, body(parsed.rows.slice(0, 5), { generation: G + 2, report_run_id: 's3' }), { nowMs: NOW })).status, 'applied', '5 / 6 = 83% は通る');
    const r = await I.ingestAmazonListings(E5.db, body(parsed.rows.slice(0, 2), { generation: G + 3, report_run_id: 's4', force: true }), { nowMs: NOW });
    assert.deepEqual([r.status, r.rows, r.forced], ['applied', 2, true]);
    assert.throws(() => I.validateListingBody(body(parsed.rows, { force: 'yes' }), { nowMs: NOW }), (e) => e.code === 'BAD_REQUEST' && /force/.test(e.message));
    // HTTP = 409 code SHRINK
    cdbTarget = E5;
    const h = await fetch(`${CBASE}/amazon-listings`, { method: 'POST', headers: { 'content-type': 'application/json', 'x-sync-key': 'k-test' }, body: JSON.stringify(body(parsed.rows.slice(0, 1), { generation: G + 4, report_run_id: 's5' })) });
    assert.equal(h.status, 409); assert.equal((await h.json()).code, 'SHRINK');
  } finally { cdbTarget = E; await E5.pg.close(); }
});
await ta('送り手の回の選び方 (#1691 Codex R1 M3): 日本の店・取得元 merchant_listings_all_data・complete・TSV・64 桁の sha256 の回だけ・取得の時刻がいちばん新しい回 (manifest の並びによらない)・sha256 が無い回は読まない', async () => {
  const H3 = path.join(TMP, 'hist3');
  fs.mkdirSync(path.join(H3, 'amazon'), { recursive: true });
  const base = { mall: 'amazon', source: 'merchant_listings_all_data', shop_id: SHOP_ID, complete: true, format: 'tsv', file: 'amazon/2026/10/x.tsv.gz', sha256: 'a'.repeat(64), items: 6, run_id: 'r' };
  const at = (h) => new Date(NOW - h * 3600e3).toISOString();
  const lines = [
    { ...base, run_id: 'newest', snapshot_at: at(2) },                                    // 新しい回が先に書かれている
    { ...base, run_id: 'older', snapshot_at: at(26) },
    { ...base, run_id: 'other_source', source: 'rms_items_search', snapshot_at: at(1) },
    { ...base, run_id: 'other_market', shop_id: 'A6HMLHKUUJC27@A1F83G8C2ARO7P', snapshot_at: at(1) },
    { ...base, run_id: 'unknown_seller', shop_id: 'unknown@A1VC38T7YXB528', snapshot_at: at(1) },
    { ...base, run_id: 'no_sha', sha256: undefined, snapshot_at: at(1) },
    { ...base, run_id: 'short_sha', sha256: 'abc', snapshot_at: at(1) },
    { ...base, run_id: 'partial', complete: false, snapshot_at: at(1) },
    { ...base, run_id: 'ndjson', format: 'ndjson', snapshot_at: at(1) },
    { ...base, run_id: 'outside', file: '../x.tsv.gz', snapshot_at: at(1) },
    { ...base, run_id: 'bad_time', snapshot_at: 'きのう' },
  ];
  fs.writeFileSync(path.join(H3, 'amazon', 'manifest.jsonl'), lines.map((l) => JSON.stringify(l)).join('\n') + '\n{壊れた行\n');
  const got = P.latestReportRecord(H3);
  assert.deepEqual([got.rec.run_id, got.prev.run_id], ['newest', 'older']);
  for (const l of lines.slice(2)) assert.equal(P.usableRecord(l), false, l.run_id);
  assert.throws(() => P.readReportText(H3, { ...base, sha256: undefined }), /sha256 が無い/);
});
await ta('🛑 送り手の初回 (Render に前の回が無い) の安全弁 (#1691 Codex R1 M2): 想定利益の夜間の判定が ok でない回・1 つ前の回の 80% 未満の回は送らない / --force で送る', async () => {
  const E6 = await setupDb({ load: false });
  cdbTarget = E6;
  try {
    const H4 = path.join(TMP, 'hist4');
    const arch4 = (text, at, enumStatus = 'ok') => archiveItems({ mall: 'amazon', shopId: SHOP_ID, source: 'merchant_listings_all_data', runId: `h4_${crypto.randomBytes(3).toString('hex')}`, fetchedAt: at.toISOString(),
      format: 'tsv', payload: text, items: text.split('\r\n').length - 2, dest: H4, noOffsite: true, log: quiet, meta: { enum_status: enumStatus } });
    await arch4(F.buildTsv(ROWS), new Date(NOW - 30 * 3600e3));
    await arch4(F.buildTsv(ROWS.slice(0, 2)), new Date(NOW - 6 * 3600e3));
    let r = await push({ historyDir: H4 });
    assert.equal(r.ok, false); assert.match(r.lastLine, /^❌ .*🛑 安全弁: 初回なのに、1 つ前の取得 6 行の 80% 未満 \(2\)/);
    assert.equal((await E6.pg.query('select count(*)::int as n from ops.amazon_listing_report')).rows[0].n, 0);
    await arch4(F.buildTsv(ROWS), new Date(NOW - 5 * 3600e3), 'partial');
    r = await push({ historyDir: H4 });
    assert.equal(r.ok, false); assert.match(r.lastLine, /初回なのに、想定利益の夜間の判定 \(enum_status\) が ok でない \(partial/);
    r = await push({ historyDir: H4, force: true });
    assert.equal(r.ok, true, r.lastLine); assert.equal(r.status, 'applied'); assert.match(r.lastLine, /^⚠️ .*--force で安全弁を越えた/);
    assert.equal((await E6.pg.query('select count(*)::int as n from ops.amazon_listing_report')).rows[0].n, 6);
  } finally { cdbTarget = E; await E6.pg.close(); }
});
await ta('送り手の引数: --send か --dry-run のどちらか 1 つ・--force は --send と一緒だけ・知らない引数は例外', async () => {
  assert.deepEqual(P.parseArgs(['--send']), { send: true, dryRun: false, force: false, historyDir: null });
  assert.deepEqual(P.parseArgs(['--dry-run', '--history-dir', 'x']), { send: false, dryRun: true, force: false, historyDir: 'x' });
  for (const a of [[], ['--send', '--dry-run'], ['--dry-run', '--force'], ['--sendx'], ['--send', '--history-dir']]) assert.throws(() => P.parseArgs(a));
});

// ─────────────────────────────────────────────────────────
console.log('\n4 画面の読み (amazon-unlinked.mjs・画面のロール master_edit)');
// 注文 (最後に売れた日): PR_NOCOMP は出品で・fba_new1 は出品の無いコードで売れた。取り消した注文・全部取り消した明細は数えない・別のモールは数えない
{
  const lid = Number((await pg.query(`select listing_id from core.listings where mall = 'amazon' and listing_norm = 'pr_nocomp'`)).rows[0].listing_id);
  let n = 0;
  const order = async (date, lines, { cancelled = false, mall = 'amazon', scope = 'jp' } = {}) => {
    n++;
    const o = (await pg.query(`insert into core.orders (company_id, mall, scope_key, mall_order_no, source_system, ordered_at, order_date_jst, status, is_cancelled, received_batch_seq, source_updated_at, transform_version, content_hash)
      values (1, $5, $6, $1, 'mall_api', $2::date, $2::date, $3, $4, 1, now(), 't', 'h') returning order_id`, [`o${n}`, date, cancelled ? 'cancelled' : 'shipped', cancelled, mall, scope])).rows[0].order_id;
    let i = 0;
    for (const [l, code, qty, cq = 0] of lines) { i++; await pg.query(`insert into core.order_lines (company_id, order_id, line_key, listing_id, unresolved_code, qty, cancelled_qty, received_batch_seq) values (1, $1, $2, $3, $4, $5, $6, 1)`, [o, `l${i}`, l, code, qty, cq]); }
  };
  await order(F.addDays(TODAY, -10), [[lid, null, 1]]);
  await order(F.addDays(TODAY, -3), [[lid, null, 2]]);
  await order(F.addDays(TODAY, -1), [[lid, null, 1]], { cancelled: true });
  await order(F.addDays(TODAY, -5), [[null, 'FBA_NEW1', 1]]);
  await order(F.addDays(TODAY, -2), [[null, 'fba_new1', 2, 2]]);
  await order(F.addDays(TODAY, -1), [[null, 'fbm_1', 1]], { mall: 'rakuten', scope: 'main' });
  await order(F.addDays(TODAY, -400), [[null, 'parent_1', 1]]);
  // FBA の在庫 (朝の FBA の在庫の日次・出品 SKU で)
  const fr = (code, a) => ({ code, fba_available: a, fba_fc_transfer: 0, fba_fc_processing: 0, fba_customer_order: 0, fba_inbound_working: 0, fba_inbound_shipped: 0, fba_inbound_received: 0 });
  const got = await ingestStockDay(db, { source: 'fba_jp', snapshot_date: TODAY, captured_at: new Date(NOW - 3600e3).toISOString(), rows: [fr('PR_NOCOMP', 7), fr('pr_a001', 3)] }, { todayJst: TODAY });
  assert.equal(got.day_status, 'complete');
}
const asEdit = (fn) => asRole(pg, 'master_edit', fn);
const list = (q = {}) => asEdit(() => U.listUnlinked(db, U.normalizeUnlinkedFilters(q, { today: TODAY }), { nowMs: NOW }));
await ta('「つないでいる」= 出品に構成がある (切替の前の SKU マスタの構成も) = 出さない / 既定 = FBA・出品日の新しい順 (出品日の無い行は後ろ)・数・最後に売れた日・FBA の在庫', async () => {
  const r = await list();
  assert.equal(r.state, 'ok'); assert.equal(r.stale, false);
  assert.deepEqual(r.counts, { report: 6, unlinked: 5, hidden: 0, linked: 1, fba: 4, fbm: 1 });
  assert.deepEqual(r.rows.map((x) => x.seller_sku), ['fba_new1', 'PR_NOCOMP', 'parent_1', 'fba_old']);
  const by = Object.fromEntries(r.rows.map((x) => [x.seller_sku, x]));
  assert.equal(by.PR_NOCOMP.listing_id !== null, true, '出品はある (構成なし)');
  assert.equal(by.fba_new1.listing_id, null, 'Company DB に出品が無い');
  assert.deepEqual([by.PR_NOCOMP.last_sold, by.PR_NOCOMP.sold_units], [F.addDays(TODAY, -3), 3], '取り消した注文は数えない');
  assert.deepEqual([by.fba_new1.last_sold, by.fba_new1.sold_units], [F.addDays(TODAY, -5), 1], '出品の無いコード (大文字も) で結ぶ・全部取り消した明細は数えない');
  assert.equal(by.parent_1.last_sold, null, '1 年より前は出さない');
  assert.deepEqual([by.PR_NOCOMP.fba_available, by.PR_NOCOMP.fba_row], [7, true], 'FBA の在庫は出品 SKU で結ぶ (大文字小文字によらない)');
  assert.deepEqual([by.fba_new1.fba_available, by.fba_new1.fba_row], [null, false], 'レポートに無い = 0 ではない');
  assert.equal(by.PR_NOCOMP.open_sku, 'pr_nocomp', '開く seller SKU は小文字');
  assert.equal(r.sold.ok, true); assert.equal(r.fba.ok, true);
});
await ta('絞り込み: 区分 (自社発送・全部)・状態 (出品中だけ・停止中)・出品日 (直近 30 日・範囲)・言葉 (SKU・商品名のかな・ASIN) / 並び (古い順・最後に売れた日・SKU)', async () => {
  const skus = async (q) => (await list(q)).rows.map((x) => x.seller_sku);
  assert.deepEqual(await skus({ ch: 'FBM' }), ['fbm_1']);
  assert.deepEqual(await skus({ ch: 'all' }), ['fba_new1', 'PR_NOCOMP', 'parent_1', 'fbm_1', 'fba_old']);
  assert.deepEqual(await skus({ st: 'active' }), ['PR_NOCOMP']);
  assert.deepEqual(await skus({ st: 'inactive' }), ['fba_new1', 'parent_1', 'fba_old']);
  assert.deepEqual(await skus({ days: '30' }), ['fba_new1', 'PR_NOCOMP']);
  assert.deepEqual(await skus({ ch: 'all', from: F.addDays(TODAY, -61), to: F.addDays(TODAY, -21) }), ['parent_1', 'fbm_1']);
  assert.deepEqual(await skus({ ch: 'all', from: F.addDays(TODAY, -21), to: F.addDays(TODAY, -61) }), ['parent_1', 'fbm_1'], 'from と to が逆でも');
  assert.deepEqual(await skus({ q: 'はちみつ' }), ['PR_NOCOMP'], 'ひらがなで カタカナ の商品名');
  assert.deepEqual(await skus({ q: 'ＦＢＡ＿ＯＬＤ' }), ['fba_old'], '全角の SKU');
  assert.deepEqual(await skus({ q: 'b00000a003' }), ['fba_new1'], 'ASIN');
  assert.deepEqual(await skus({ sort: 'open_asc' }), ['fba_old', 'parent_1', 'PR_NOCOMP', 'fba_new1']);
  assert.deepEqual(await skus({ sort: 'sold_desc' }), ['PR_NOCOMP', 'fba_new1', 'fba_old', 'parent_1'], '売れていない行は後ろ (SKU の順)');
  assert.deepEqual(await skus({ sort: 'sku' }), ['PR_NOCOMP', 'fba_new1', 'fba_old', 'parent_1']);
  // 知らない値は捨てる
  assert.deepEqual(U.normalizeUnlinkedFilters({ ch: 'x', st: 'y', sort: 'z', from: '2026-02-30', offset: '-5', hidden: 'yes' }), { q: '', ch: 'FBA', st: 'all', sort: 'open_desc', from: '', to: '', hidden: '', offset: 0 });
});
await ta('seller SKU の画面: ある = 行 (正規化で同じ SKU)・無い = absent・古い (36 時間より前) = stale', async () => {
  let r = await asEdit(() => U.readAmazonReportFor(db, 'pr_nocomp', { nowMs: NOW }));
  assert.equal(r.state, 'ok'); assert.equal(r.rows[0].seller_sku, 'PR_NOCOMP'); assert.equal(r.rows[0].item_name, 'ハチミツ 500g (出品だけ)'); assert.equal(r.stale, false);
  r = await asEdit(() => U.readAmazonReportFor(db, 'no_such', { nowMs: NOW }));
  assert.equal(r.state, 'absent');
  r = await asEdit(() => U.readAmazonReportFor(db, 'pr_nocomp', { nowMs: NOW + 40 * 3600e3 }));
  assert.equal(r.stale, true);
});
await ta('読めない: 画面のロールに表の権限が無い = no_privilege / 表はあるがまだ届いていない = empty / 最後に売れた日の権限が無い = 列だけ「読めない」(並びは出品日に戻す)', async () => {
  await pg.query('revoke select on ops.amazon_listing_report from master_edit');
  try {
    const r = await asEdit(() => U.readAmazonReportFor(db, 'pr_nocomp', { nowMs: NOW }));
    assert.deepEqual([r.state, r.reason], ['unreadable', 'no_privilege']);
  } finally { await pg.query('grant select on ops.amazon_listing_report to master_edit'); }
  await pg.query('revoke execute on function ops.amazon_sku_last_sold(text[], date) from master_edit');
  try {
    const r = await list({ sort: 'sold_desc' });
    assert.equal(r.sold.ok, false); assert.match(r.sold.error, /権限/); assert.equal(r.filters.sort, 'open_desc');
    assert.deepEqual(r.rows.map((x) => x.seller_sku), ['fba_new1', 'PR_NOCOMP', 'parent_1', 'fba_old']);
  } finally { await pg.query('grant execute on function ops.amazon_sku_last_sold(text[], date) to master_edit'); }
  const E4 = await setupDb({ load: false });
  const r = await asRole(E4.pg, 'master_edit', () => U.readAmazonReportFor(E4.db, 'x'));
  assert.deepEqual([r.state, r.reason], ['unreadable', 'empty']);
  await E4.pg.close();
});

// ─────────────────────────────────────────────────────────
console.log('\n5 隠す (ops.set_amazon_sku_hidden)');
await ta('画面のロールは関数だけ: 隠す印・記録・出品一覧の表に直に書けない / 部品でない 2 つの関数を実行できる / ロールの一覧の定数', async () => {
  for (const sql of [`insert into ops.amazon_sku_hidden (shop_code, sku_norm, seller_sku, reason, hidden_by) values ('x', 'y', 'y', 'unused', 'z')`,
    `insert into ops.amazon_sku_hidden_events (company_id, shop_code, sku_norm, seller_sku, action, actor) values (1, 'x', 'y', 'y', 'hide', 'z')`,
    `delete from ops.amazon_listing_report`, `update ops.amazon_listing_report_loads set row_count = 0`]) {
    await assert.rejects(asEdit(() => pg.query(sql)), (e) => e.code === '42501', sql);
  }
  assert.deepEqual([...AMAZON_LISTING_SELECT], ['ops.amazon_listing_report', 'ops.amazon_listing_report_loads', 'ops.amazon_sku_hidden']);
  assert.equal(AMAZON_LISTING_EDIT_FUNCTIONS.length, 2);
  // public から外した = 見張りのロールは呼べない
  await assert.rejects(asRole(pg, 'watcher', () => pg.query(`select ops.amazon_sku_last_sold(array['x'], current_date)`)), (e) => e.code === '42501');
});
await ta('隠す → same → 一覧から外れて「隠した SKU」に出る (誰・いつ・理由) → 元に戻す / 出品一覧に無い = 404 / 「その他」は文が要る / 理由が無い = 400 / 記録は追記だけ', async () => {
  const hide = (x) => asEdit(() => U.setHidden(db, { actor: 'Naka@Test', ...x }));
  let r = await hide({ seller_sku: 'parent_1', hide: true, reason: 'variation_parent', note: null });
  assert.equal(r.status, 'hidden'); assert.equal(r.by, 'naka@test');
  r = await hide({ seller_sku: 'PARENT_1', hide: true, reason: 'unused', note: null });
  assert.equal(r.status, 'same', '正規化で同じ SKU'); assert.equal(r.reason, 'variation_parent');
  let l = await list();
  assert.deepEqual(l.rows.map((x) => x.seller_sku), ['fba_new1', 'PR_NOCOMP', 'fba_old']);
  assert.equal(l.counts.hidden, 1); assert.equal(l.counts.unlinked, 4);
  l = await list({ hidden: '1' });
  assert.deepEqual(l.rows.map((x) => [x.seller_sku, x.hidden_reason, x.hidden_by]), [['parent_1', 'variation_parent', 'naka@test']]);
  await assert.rejects(hide({ seller_sku: 'no_such', hide: true, reason: 'unused' }), (e) => e instanceof MasterWriteError && e.status === 404);
  // つないでいる SKU (出品に構成がある pr_a001) には付けない (#1691 Codex R1 M4 = 古い画面・API の直呼び)。DB の関数が確かめ直す
  await assert.rejects(hide({ seller_sku: 'PR_A001', hide: true, reason: 'unused' }), (e) => e instanceof MasterWriteError && e.status === 409 && e.reason === 'linked');
  await assert.rejects(asEdit(() => pg.query("select ops.set_amazon_sku_hidden('n@t', 'pr_a001', true, 'unused', null)")), (e) => e.code === 'P0001' && /^linked: /.test(e.message));
  assert.equal((await pg.query("select count(*)::int as n from ops.amazon_sku_hidden where sku_norm = 'pr_a001'")).rows[0].n, 0);
  await assert.rejects(hide({ seller_sku: 'fba_old', hide: true, reason: 'other' }), (e) => e instanceof MasterWriteError && e.status === 400 && /その他/.test(e.message));
  await assert.rejects(hide({ seller_sku: 'fba_old', hide: true }), (e) => e instanceof MasterWriteError && e.status === 400);
  // DB の関数も同じことを確かめる (画面の検査を通らない呼び方)
  await assert.rejects(asEdit(() => pg.query(`select ops.set_amazon_sku_hidden('n@t', 'fba_old', true, 'other', null)`)), (e) => e.code === '22023');
  await assert.rejects(asEdit(() => pg.query(`select ops.set_amazon_sku_hidden('', 'fba_old', true, 'unused', null)`)), (e) => e.code === '22023');
  r = await hide({ seller_sku: 'fba_old', hide: true, reason: 'other', note: '  もう売らない  ' });
  assert.equal(r.note, 'もう売らない');
  r = await hide({ seller_sku: 'parent_1', hide: false });
  assert.equal(r.status, 'shown');
  r = await hide({ seller_sku: 'parent_1', hide: false });
  assert.equal(r.status, 'same');
  const ev = (await pg.query(`select seller_sku, action, reason, note, actor from ops.amazon_sku_hidden_events order by event_id`)).rows;
  assert.deepEqual(ev.map((e) => [e.seller_sku, e.action, e.reason, e.actor]), [['parent_1', 'hide', 'variation_parent', 'naka@test'], ['fba_old', 'hide', 'other', 'naka@test'], ['parent_1', 'show', 'variation_parent', 'naka@test']]);
  await assert.rejects(pg.query(`update ops.amazon_sku_hidden_events set actor = 'x'`));
  await assert.rejects(pg.query(`delete from ops.amazon_sku_hidden_events`));
  await hide({ seller_sku: 'fba_old', hide: false });
});

// ─────────────────────────────────────────────────────────
console.log('\n6 画面 (マスタの入力の router)');
__setAmazonChannelsProvider(async () => new Map());
setMeFactory(async (url) => {
  const role = /master_edit@/.test(url) ? 'master_edit' : 'deploy';
  await pg.query(`set role ${role}`);
  return { query: (t, p) => pg.query(t, p), end: async () => { await pg.query('set role deploy'); }, on: () => {} };
});
const app = express();
app.set('view engine', 'ejs');
app.use((req, res, next) => {
  const s = req.headers['x-test-session'];
  req.session = s === 'editor' ? { authenticated: true, email: 'naka@test', displayName: '中原', role: 'user', allowedApps: ['master-edit'] }
    : s === 'viewer' ? { authenticated: true, email: 'viewer@test', role: 'user', allowedApps: ['master-edit'] } : null;
  next();
});
app.use('/apps/master-edit', meRouter);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
cleanups.push(() => server.close());
const ORIGIN = `http://127.0.0.1:${server.address().port}`;
const BASE = `${ORIGIN}/apps/master-edit`;
async function call(method, url, { body: b, session = 'editor', origin = true } = {}) {
  const headers = { Accept: 'application/json', 'x-test-session': session };
  if (b !== undefined) headers['Content-Type'] = 'application/json';
  if (origin) headers.Origin = ORIGIN;
  const r = await fetch(BASE + url, { method, headers, body: b === undefined ? undefined : JSON.stringify(b), redirect: 'manual' });
  const text = await r.text();
  let j = null; try { j = JSON.parse(text); } catch { /* HTML */ }
  return { status: r.status, j, text };
}
async function checkScripts(html) {
  const all = [...html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)];
  for (const m of all.filter((x) => /type="application\/json"/.test(x[1]))) JSON.parse(m[2]);
  const out = [];
  for (const m of all.filter((x) => /\bsrc="\/apps\/master-edit\/public\//.test(x[1]))) {
    const src = /\bsrc="([^"]+)"/.exec(m[1])[1];
    const r = await fetch(ORIGIN + src.replace(/&amp;/g, '&'), { headers: { 'x-test-session': 'editor' } });
    assert.equal(r.status, 200, src);
    const t = await r.text(); new vm.Script(t); assert.ok(!/<%|%>/.test(t)); out.push(src);
  }
  return out;
}
await ta('一覧の画面: 行・商品名・入口のリンク・隠すのボタン (名簿の人だけ)・画面の JS / 絞り込みの札 / 隠した SKU / 見る人には隠すが出ない', async () => {
  let r = await call('GET', '/amazon/unlinked');
  assert.equal(r.status, 200);
  const js = await checkScripts(r.text);
  assert.ok(js.some((s) => s.includes('me-amazon-unlinked.js')));
  assert.ok(r.text.includes('まだつないでいない Amazon の SKU'));
  for (const s of ['fba_new1', 'PR_NOCOMP', 'parent_1', 'fba_old']) assert.ok(r.text.includes(`data-sku="${s}"`), s);
  assert.ok(!r.text.includes('data-sku="pr_a001"'), 'つないでいる SKU は出ない');
  assert.ok(!r.text.includes('data-sku="fbm_1"'), '既定は FBA だけ');
  assert.ok(r.text.includes('href="/apps/master-edit/amazon/sku?sku=pr_nocomp"'), '行を押すと対応を作る画面');
  assert.ok(r.text.includes('ハチミツ 500g (出品だけ)') && r.text.includes('B00000A003'));
  assert.ok(r.text.includes('data-act="hide"') && r.text.includes('id="hide-bg"'));
  assert.ok(r.text.indexOf('data-sku="fba_new1"') < r.text.indexOf('data-sku="PR_NOCOMP"'), '出品日の新しい順');
  assert.ok(r.text.includes('7 個'), 'FBA の在庫');
  r = await call('GET', '/amazon/unlinked?ch=FBM');
  assert.ok(r.text.includes('data-sku="fbm_1"') && !r.text.includes('data-sku="fba_new1"'));
  assert.ok(r.text.includes('1,280 円'));
  r = await call('GET', '/amazon/unlinked?q=%3Cscript%3E');
  assert.ok(!r.text.includes('<script>alert') && r.text.includes('value="&lt;script&gt;"'), '言葉はエスケープ');
  r = await call('GET', '/amazon/unlinked', { session: 'viewer' });
  assert.equal(r.status, 200); assert.ok(!r.text.includes('data-act="hide"') && !r.text.includes('me-amazon-unlinked.js'));
});
await ta('隠す API: Origin が無い = 403 / 見る人 = 403 / 名簿の人 = 隠す → 一覧から消えて「隠した SKU」に出る → 元に戻す / 形の外れ = 400 / 無い SKU = 404', async () => {
  let r = await call('POST', '/api/amazon/unlinked/hide', { body: { seller_sku: 'fba_old', hide: true, reason: 'unused' }, origin: false });
  assert.equal(r.status, 403);
  r = await call('POST', '/api/amazon/unlinked/hide', { body: { seller_sku: 'fba_old', hide: true, reason: 'unused' }, session: 'viewer' });
  assert.equal(r.status, 403); assert.equal(r.j.reason, 'not_editor');
  r = await call('POST', '/api/amazon/unlinked/hide', { body: { seller_sku: 'fba_old', hide: true, reason: 'unused' } });
  assert.equal(r.status, 200); assert.equal(r.j.status, 'hidden');
  r = await call('GET', '/amazon/unlinked');
  assert.ok(!r.text.includes('data-sku="fba_old"'));
  r = await call('GET', '/amazon/unlinked?hidden=1');
  assert.ok(r.text.includes('data-sku="fba_old"') && r.text.includes('使っていない・古い SKU') && r.text.includes('naka@test') && r.text.includes('data-act="show"'));
  r = await call('POST', '/api/amazon/unlinked/hide', { body: { seller_sku: 'fba_old', hide: false } });
  assert.equal(r.j.status, 'shown');
  r = await call('POST', '/api/amazon/unlinked/hide', { body: { seller_sku: 'fba_old', hide: 'yes' } });
  assert.equal(r.status, 400);
  r = await call('POST', '/api/amazon/unlinked/hide', { body: { seller_sku: 'nope', hide: true, reason: 'unused' } });
  assert.equal(r.status, 404); assert.equal(r.j.reason, 'not_in_report');
});
await ta('seller SKU の画面: 出品一覧にある = 「Amazon の出品一覧では」(商品名・ASIN・状態) / 無い = 何も出さない / 読めない = 読めない と一言', async () => {
  let r = await call('GET', '/amazon/sku?sku=pr_nocomp');
  assert.equal(r.status, 200);
  assert.ok(r.text.includes('id="amazon-report"') && r.text.includes('Amazon の出品一覧では') && r.text.includes('ハチミツ 500g (出品だけ)') && r.text.includes('B00000A002'));
  r = await call('GET', '/amazon/sku?sku=fba_new1');
  assert.ok(r.text.includes('新しい FBA の商品 (まだ納品前)') && r.text.includes('停止中'), 'Company DB に出品が無い SKU でも出す');
  r = await call('GET', '/amazon/sku?sku=zz_none');
  assert.ok(!r.text.includes('id="amazon-report"') && !r.text.includes('id="amazon-report-off"'), '無い = 何も出さない');
  await pg.query('revoke select on ops.amazon_listing_report_loads from master_edit');
  try {
    r = await call('GET', '/amazon/sku?sku=pr_nocomp');
    assert.ok(r.text.includes('id="amazon-report-off"') && r.text.includes('読めません'));
    r = await call('GET', '/amazon/unlinked');
    assert.ok(r.text.includes('id="unlinked-unreadable"') && r.text.includes('0 件」ではありません'));
  } finally { await pg.query('grant select on ops.amazon_listing_report_loads to master_edit'); }
});
await ta('つかいかた (#amazon-unlinked)・Amazon SKU の一覧の右上の入口・古い出品一覧の帯', async () => {
  let r = await call('GET', '/manual');
  assert.ok(r.text.includes('id="amazon-unlinked"') && r.text.includes('まだつないでいない SKU'));
  r = await call('GET', '/amazon/');
  assert.ok(r.text.includes('href="unlinked"'));
  const keep = (await pg.query('select load_id, snapshot_at from ops.amazon_listing_report_loads order by generation desc limit 1')).rows[0];
  await pg.query(`update ops.amazon_listing_report_loads set snapshot_at = now() - interval '40 hours' where load_id = $1`, [keep.load_id]);
  try {
    r = await call('GET', '/amazon/unlinked');
    assert.ok(r.text.includes('id="unlinked-stale"'));
    r = await call('GET', '/amazon/sku?sku=pr_nocomp');
    assert.ok(r.text.includes('>古い<'));
  } finally { await pg.query('update ops.amazon_listing_report_loads set snapshot_at = $2 where load_id = $1', [keep.load_id, keep.snapshot_at]); }
});

// ─────────────────────────────────────────────────────────
console.log('\n7 毎朝のランナー・台帳');
await ta('daily-sync の工程 (FBA の在庫の日次の後)・retry の定義と順番・台帳の文 (新しい定期実行は無い)', async () => {
  const ds = fs.readFileSync(path.join(ROOT, 'apps/warehouse/daily-sync.js'), 'utf8');
  assert.match(ds, /runScript\('apps\/company-db\/push\/amazon-listings\.mjs --send', 'Company DB 出品一覧 \(Amazon\)', 300000\)/);
  assert.match(ds, /name: 'CompanyDB出品一覧\(Amazon\)'/);
  assert.ok(ds.indexOf("'CompanyDB在庫(FBA US)']]") < ds.indexOf('amazon-listings.mjs --send'), 'FBA の在庫の日次の後');
  assert.match(/const RETRYABLE_JOBS = \[[^\]]*\]/.exec(ds)[0], /'CompanyDB出品一覧\(Amazon\)'/);
  const R = await import('../apps/warehouse/retry-failed-jobs.js');
  assert.deepEqual({ ...R.JOB_DEFINITIONS['CompanyDB出品一覧(Amazon)'] }, { script: 'apps/company-db/push/amazon-listings.mjs', args: ['--send'], timeoutMs: 300000 });
  assert.ok(R.RETRY_ORDER.includes('CompanyDB出品一覧(Amazon)'));
  const { JOBS_REGISTRY } = await import('../config/jobs-registry.mjs');
  const daily = JOBS_REGISTRY.find((j) => j.id === 'warehouse-daily-sync');
  assert.match(daily.purpose, /Company DB 出品一覧 \(Amazon\)/); assert.match(daily.purpose, /SP-API は呼ばない/);
  assert.match(JOBS_REGISTRY.find((j) => j.id === 'expected-profit-nightly').purpose, /amazon-listings\.mjs/);
});

console.log(`\n${passed} ok / ${failed} NG`);
for (const c of cleanups.reverse()) { try { c(); } catch { /* */ } }
await pg.close();
process.exitCode = failed ? 1 : 0;
setTimeout(() => process.exit(process.exitCode), 2000).unref();
