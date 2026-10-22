#!/usr/bin/env node
/**
 * test-amazon-ai-listing.mjs — Amazon の出品の状態・出品アラート・カートを取られたアラート・BSR の履歴 (2026-10-22・AI の分析とアラート) の試験。
 *   API には繋がない (見本の応答と :memory: の SQLite)。
 *   apps/warehouse/fetch-amazon-listing-issues.js / alert-amazon-listing.js / fetch-amazon-bsr.js / amazon-listing-common.js
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';
import { createRateGate, callWithRetry, loadWatchSkus, resolveSellerId, businessDate, addDays } from '../apps/warehouse/amazon-listing-common.js';
import { parseListingItem, notFoundRow, scanAllListings, lookupSkus, saveListingSnapshot, lookupTargets, missingRows, ensureListingTables, MAX_PAGEABLE } from '../apps/warehouse/fetch-amazon-listing-issues.js';
import { buildListingAlerts, buildBuyboxAlerts, runAlert, formatBuyboxMessage } from '../apps/warehouse/alert-amazon-listing.js';
import { parseSalesRanks, saveRanks, fetchBatch, fetchAllRanks, loadTargetAsins, ensureRankTables } from '../apps/warehouse/fetch-amazon-bsr.js';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const MP = 'A1VC38T7YXB528';
let ok = 0, ng = 0;
const queue = [];
const t = (name, fn) => queue.push([name, fn]);
const section = (s) => queue.push([null, s]);
const throwsAsync = async (fn, re) => { let e = null; try { await fn(); } catch (x) { e = x; } if (!e) throw new Error('did not throw'); if (re && !re.test(e.message)) throw new Error(`wrong error: ${e.message}`); };

function openDb() {
  const db = new Database(':memory:');
  // 本番 (warehouse.db) の表のうち読むところだけ
  db.exec(`CREATE TABLE raw_sp_orders (id INTEGER PRIMARY KEY AUTOINCREMENT, amazon_order_id TEXT NOT NULL, purchase_date TEXT, order_status TEXT, fulfillment_channel TEXT,
    asin TEXT, seller_sku TEXT, quantity INTEGER, item_price REAL)`);
  db.exec(`CREATE TABLE inv_daily_detail (business_date TEXT NOT NULL, market TEXT NOT NULL DEFAULT 'jp', category TEXT NOT NULL, source_system TEXT NOT NULL,
    source_item_code TEXT NOT NULL, ne_code TEXT NOT NULL, qty INTEGER NOT NULL)`);
  db.exec(`CREATE TABLE amazon_sku_fees (seller_sku TEXT NOT NULL PRIMARY KEY, asin TEXT, fulfillment_channel TEXT, fetched_at TEXT NOT NULL DEFAULT 'T')`);
  db.exec(`CREATE TABLE fact_amazon_price_snapshot (date_jst TEXT NOT NULL, seller_sku TEXT NOT NULL, asin TEXT NOT NULL, channel TEXT, my_price REAL, buybox_price REAL,
    buybox_is_mine INTEGER, fetched_at TEXT NOT NULL, PRIMARY KEY (date_jst, seller_sku))`);
  return db;
}
const TODAY = '2026-10-22';
const PREV = '2026-10-21';
function sold(db, sku, { date = '2026-10-10T03:00:00Z', qty = 1, price = 1000, status = 'Shipped', asin = 'B000000001' } = {}) {
  db.prepare(`INSERT INTO raw_sp_orders (amazon_order_id, purchase_date, order_status, asin, seller_sku, quantity, item_price) VALUES (?, ?, ?, ?, ?, ?, ?)`)
    .run(`O-${Math.random()}`, date, status, asin, sku, qty, price);
}
const item = (sku, { status = ['BUYABLE', 'DISCOVERABLE'], issues = [], asin = 'B000000001', mp = MP } = {}) => ({
  sku, summaries: [{ marketplaceId: mp, asin, productType: 'HEALTH', conditionType: 'new_new', status, itemName: `name ${sku}`, createdDate: '2024-01-01T00:00:00Z', lastUpdatedDate: '2026-10-01T00:00:00Z' }],
  issues,
});
const issue = (code, severity = 'ERROR', extra = {}) => ({ code, message: `msg ${code}`, severity, categories: ['INVALID_ATTRIBUTE'], attributeNames: ['item_name'], ...extra });
const run = (status = 'complete') => ({ status, windows: 1, capped: 0, calls: 1, errors: 0, lookupRequested: 0 });
const snap = (db, date, items, status = 'complete') => saveListingSnapshot(db, { date, rows: items.map((x) => (x.__notFound ? notFoundRow(x.sku, MP) : parseListingItem(x, MP))), run: run(status), fetchedAt: `${date}T00:00:00Z` });

section('出品の解釈');
t('summaries の状態 (並べて保存)・BUYABLE / DISCOVERABLE・ほかの marketplace の summary と issue は外す・SKU の鍵は小文字', () => {
  const r = parseListingItem({
    sku: 'Pr_ABC', summaries: [{ marketplaceId: 'ATVPDKIKX0DER', status: ['BUYABLE'] }, { marketplaceId: MP, asin: 'B0X', status: ['DISCOVERABLE', 'BUYABLE'], productType: 'P', fnSku: 'X001' }],
    issues: [issue('100', 'ERROR', { marketplaceIds: ['ATVPDKIKX0DER'] }), issue('200', 'WARNING'), issue('300', 'ERROR', { marketplaceIds: [MP] })],
  }, MP);
  assert.deepEqual([r.sku_key, r.seller_sku, r.status, r.is_buyable, r.is_discoverable, r.asin, r.fn_sku, r.error_count, r.warning_count],
    ['pr_abc', 'Pr_ABC', 'BUYABLE,DISCOVERABLE', 1, 1, 'B0X', 'X001', 1, 1]);
  assert.deepEqual(r.issues.map((i) => i.issue_key), ['200|WARNING|item_name', '300|ERROR|item_name']);
});
t('issue の鍵 = code|severity|attributeNames (同じ鍵の 2 つ目は #2)・enforcements の LISTING_SUPPRESSED = 出品停止・SEARCH_SUPPRESSED は別の列・summary が無い = 状態なし', () => {
  const r = parseListingItem({ sku: 'S1', issues: [
    issue('18027', 'ERROR', { enforcements: { actions: [{ action: 'LISTING_SUPPRESSED' }], exemption: { status: 'NOT_EXEMPT' } } }),
    issue('18027', 'ERROR'),
    issue('99', 'WARNING', { attributeNames: ['b', 'a'], enforcements: { actions: [{ action: 'SEARCH_SUPPRESSED' }] } }),
  ] }, MP);
  assert.deepEqual(r.issues.map((i) => i.issue_key), ['18027|ERROR|item_name', '18027|ERROR|item_name#2', '99|WARNING|a,b']);
  assert.deepEqual([r.suppressed, r.search_suppressed, r.status, r.is_buyable, JSON.parse(r.issues[0].enforcement_actions)], [1, 1, '', 0, ['LISTING_SUPPRESSED']]);
  assert.equal(parseListingItem({ sku: '  ' }, MP), null);
  assert.deepEqual([notFoundRow('Ab', MP).status, notFoundRow('Ab', MP).is_buyable, notFoundRow('Ab', MP).sku_key], ['NOT_FOUND', 0, 'ab']);
});

section('ページ送りと窓');
/** 見本の searchListingsItems: 作成日時の窓 (両端を含む) で絞り、sku で並べ、pageToken = 位置。1 回の検索で辿れるのは 1,000 件まで (公式) */
function fakeSearch(listings, { failOn = () => false } = {}) {
  const calls = [];
  const fn = async (q) => {
    calls.push(q);
    if (failOn(q, calls.length)) throw new Error('InternalFailure');
    if (q.identifiers) {
      const want = new Set(q.identifiers.map((s) => s.toLowerCase()));
      return { numberOfResults: 0, items: listings.filter((l) => want.has(l.sku.toLowerCase())).map((l) => ({ sku: l.sku })) };
    }
    const a = Date.parse(q.createdAfter), b = Date.parse(q.createdBefore);
    const hit = listings.filter((l) => l.created >= a && l.created <= b).sort((x, y) => (x.sku < y.sku ? -1 : 1));
    const off = q.pageToken ? Number(q.pageToken) : 0;
    const page = hit.slice(off, Math.min(off + q.pageSize, MAX_PAGEABLE));
    const next = off + q.pageSize < Math.min(hit.length, MAX_PAGEABLE) ? String(off + q.pageSize) : undefined;
    return { numberOfResults: hit.length, pagination: next ? { nextToken: next } : {}, items: page.map((l) => ({ sku: l.sku })) };
  };
  fn.calls = calls;
  return fn;
}
const T0 = Date.parse('2015-01-01T00:00:00Z');
const spread = (n, prefix = 'S') => Array.from({ length: n }, (_, i) => ({ sku: `${prefix}${String(i).padStart(5, '0')}`, created: T0 + i * 3600000 * 24 }));
t('1,000 件以下 = 1 つの窓をページ送りで全部 (pageSize 20・sortBy sku)', async () => {
  const f = fakeSearch(spread(45));
  const r = await scanAllListings(f, { toIso: '2026-10-22T00:00:00Z' });
  assert.deepEqual([r.items.size, r.windows, r.capped, r.errors, r.calls], [45, 1, 0, 0, 3]);
  assert.deepEqual([f.calls[0].pageSize, f.calls[0].sortBy, f.calls[1].pageToken], [20, 'sku', '20']);
});
t('🚨 1,000 件を超える窓は割る (重ねた境の重複は外す) → 2,500 件を全部・上限に当たった窓なし', async () => {
  const f = fakeSearch(spread(2500));
  const r = await scanAllListings(f, { toIso: '2026-10-22T00:00:00Z' });
  assert.equal(r.items.size, 2500);
  assert.equal(r.capped, 0);
  assert.ok(r.windows >= 3, `windows=${r.windows}`);
  for (const q of f.calls.filter((x) => !x.pageToken)) assert.ok(Date.parse(q.createdBefore) > Date.parse(q.createdAfter));
});
t('🚨 同じ時刻に 1,000 件を超える (割れない) = 上限に当たったと数える (1,000 件で止まる)', async () => {
  const same = Array.from({ length: 1200 }, (_, i) => ({ sku: `C${String(i).padStart(5, '0')}`, created: T0 }));
  const r = await scanAllListings(fakeSearch(same), { toIso: '2026-10-22T00:00:00Z' });
  assert.deepEqual([r.items.size, r.capped], [1000, 1]);
});
t('ページの途中の失敗 = 失敗に数えて次の窓へ (取れた分は残す)・--limit で止める', async () => {
  const f = fakeSearch(spread(45), { failOn: (q) => q.pageToken === '20' });
  const r = await scanAllListings(f, { toIso: '2026-10-22T00:00:00Z' });
  assert.deepEqual([r.items.size, r.errors], [20, 1]);
  const r2 = await scanAllListings(fakeSearch(spread(100)), { toIso: '2026-10-22T00:00:00Z', limit: 30 });
  assert.deepEqual([r2.items.size, r2.stopped], [40, true]);
});
t('SKU の引き直し: 20 件ずつ・返らない = 見つからない・失敗した batch は分からない (書かない)・カンマ入りは送らない', async () => {
  const listings = [{ sku: 'Aa1', created: T0 }, { sku: 'b2', created: T0 }];
  const wanted = [{ key: 'aa1', raw: 'Aa1' }, { key: 'zz', raw: 'zz' }, { key: 'x,y', raw: 'x,y' }, ...Array.from({ length: 20 }, (_, i) => ({ key: `f${i}`, raw: `f${i}` }))];
  const f = fakeSearch(listings, { failOn: (q) => q.identifiers && q.identifiers.includes('f19') });
  const r = await lookupSkus(f, wanted);
  assert.deepEqual([[...r.found.keys()], r.notFound.length, r.failed, r.skipped, r.calls], [['aa1'], 19, ['f18', 'f19'], ['x,y'], 2]);
  assert.deepEqual([f.calls[0].identifiersType, f.calls[0].identifiers.length], ['SKU', 20]);
});
t('引き直しの対象 = アラートの対象 + amazon_sku_fees − 走査に出た SKU・🚨 綴りは前の日までの写しで Amazon が返した形 (trusted)・無ければ手元の小文字 (trusted でない)', () => {
  const db = openDb();
  sold(db, 'pr_sold'); sold(db, 'pr_new');   // sp-api-orders.js は小文字にそろえて入れる
  db.prepare(`INSERT INTO amazon_sku_fees (seller_sku, asin) VALUES ('fee-only', 'B1'), ('pr_sold', 'B1'), ('scanned', 'B1')`).run();
  snap(db, '2026-10-01', [item('PR_Sold')]);
  snap(db, PREV, [item('Pr_Sold'), { sku: 'pr_new', __notFound: true }]);   // 見つからない行の綴りは使わない
  const watch = loadWatchSkus(db, { today: TODAY });
  assert.deepEqual(lookupTargets(db, watch, new Set(['scanned'])), [
    { key: 'fee-only', raw: 'fee-only', trusted: false }, { key: 'pr_new', raw: 'pr_new', trusted: false }, { key: 'pr_sold', raw: 'Pr_Sold', trusted: true }]);
});
t('🚨 引いても返らない SKU: 走査が全部終わった or 綴りが確か = NOT_FOUND・綴りが分からず走査も途中 = 分からない (書かない = 偽の「BUYABLE でなくなった」を出さない)', () => {
  const nf = [{ key: 'a', raw: 'A', trusted: true }, { key: 'b', raw: 'b', trusted: false }];
  const p = missingRows(nf, { scanComplete: false, marketplaceId: MP });
  assert.deepEqual([p.rows.map((r) => [r.sku_key, r.seller_sku, r.status]), p.unknown], [[['a', 'A', 'NOT_FOUND']], ['b']]);
  assert.deepEqual(missingRows(nf, { scanComplete: true, marketplaceId: MP }).rows.map((r) => r.sku_key), ['a', 'b']);
});

section('写しの保存');
t('その日の分を置き換え (冪等)・issue も置き換え・回の記録・90 日より古い写しを消す', () => {
  const db = openDb();
  snap(db, '2026-07-01', [item('OLD')]);
  snap(db, TODAY, [item('A', { issues: [issue('1')] }), item('B')]);
  const r = snap(db, TODAY, [item('A')]);
  assert.deepEqual([r.saved, r.issues], [1, 0]);
  assert.deepEqual(db.prepare(`SELECT sku_key FROM amazon_listing_status_daily WHERE date_jst = ?`).all(TODAY).map((x) => x.sku_key), ['a']);
  assert.equal(db.prepare(`SELECT COUNT(*) c FROM amazon_listing_issue_daily`).get().c, 0);
  assert.equal(db.prepare(`SELECT COUNT(*) c FROM amazon_listing_status_daily WHERE date_jst = '2026-07-01'`).get().c, 0);
  assert.equal(db.prepare(`SELECT status FROM amazon_listing_fetch_runs WHERE date_jst = ?`).get(TODAY).status, 'complete');
});
t('🚨 その日に complete があれば partial の回で置き換えない・partial は complete で置き換わる', () => {
  const db = openDb();
  snap(db, TODAY, [item('A'), item('B')]);
  assert.equal(snap(db, TODAY, [item('A')], 'partial').skipped, 'complete_exists');
  assert.equal(db.prepare(`SELECT COUNT(*) c FROM amazon_listing_status_daily WHERE date_jst = ?`).get(TODAY).c, 2);
  snap(db, PREV, [item('A')], 'partial');
  snap(db, PREV, [item('A'), item('C')]);
  assert.equal(db.prepare(`SELECT COUNT(*) c FROM amazon_listing_status_daily WHERE date_jst = ?`).get(PREV).c, 2);
});

section('出品のアラート');
function listingFixture() {
  const db = openDb();
  for (const s of ['Lost', 'Err', 'Same', 'Gone', 'Warn2Err', 'OnlyToday']) sold(db, s, { price: 5000 });
  sold(db, 'Cancelled1', { status: 'Cancelled' });
  sold(db, 'OldSale', { date: '2026-08-01T00:00:00Z' });
  db.prepare(`INSERT INTO inv_daily_detail VALUES ('2026-10-21', 'jp', 'fba_warehouse', 'fba', 'stockonly', 'X', 5)`).run();
  snap(db, PREV, [item('Lost'), item('Err', { issues: [issue('1')] }), item('Same', { status: [], issues: [issue('9')] }), item('Gone'), item('Warn2Err', { issues: [issue('5', 'WARNING')] }),
    item('Unsold'), item('Cancelled1'), item('OldSale'), item('StockOnly')]);
  snap(db, TODAY, [item('Lost', { status: ['DISCOVERABLE'], issues: [issue('18027', 'ERROR', { enforcements: { actions: [{ action: 'LISTING_SUPPRESSED' }] } })] }),
    item('Err', { issues: [issue('1'), issue('2')] }), item('Same', { status: [], issues: [issue('9')] }), { sku: 'Gone', __notFound: true },
    item('Warn2Err', { issues: [issue('5', 'ERROR')] }), item('OnlyToday', { status: [], issues: [issue('7')] }),
    item('Unsold', { status: [], issues: [issue('3')] }), item('Cancelled1', { status: [] }), item('OldSale', { status: [] }), item('StockOnly', { status: [] })]);
  return db;
}
t('売れた / FBA 在庫の SKU だけ: BUYABLE でなくなった (見つからないを含む)・新しい ERROR (WARNING → ERROR も新しい)・前からある ERROR と片方の日に無い SKU は言わない', () => {
  const db = listingFixture();
  const r = buildListingAlerts(db, { today: TODAY, watch: loadWatchSkus(db, { today: TODAY }) });
  assert.equal(r.state, 'ok');
  assert.equal(r.prev, PREV);
  assert.deepEqual(r.notBuyable.map((x) => [x.sku_key, x.status, x.suppressed]).sort(), [['gone', 'NOT_FOUND', 0], ['lost', 'DISCOVERABLE', 1], ['stockonly', '', 0]]);
  assert.deepEqual(r.newErrors.map((x) => [x.sku_key, x.code]).sort(), [['err', '2'], ['lost', '18027'], ['warn2err', '5']]);
});
t('🚨 初回 (前の回の写しが無い) は通知しない・今日の写しが無い = 比べない・前の回が 7 日より古い = 初回と同じ', async () => {
  const db = openDb();
  sold(db, 'A');
  assert.equal(buildListingAlerts(db, { today: TODAY, watch: loadWatchSkus(db, { today: TODAY }) }).state, 'no_today');
  snap(db, TODAY, [item('A', { status: [], issues: [issue('1')] })]);
  let sent = 0;
  const r = await runAlert(db, { kind: 'listing', today: TODAY, webhook: 'https://x', send: async () => { sent++; return true; } });
  assert.match(r.summary, /初回/);
  assert.equal(sent, 0);
  snap(db, '2026-10-10', [item('A')]);
  assert.equal(buildListingAlerts(db, { today: TODAY, watch: loadWatchSkus(db, { today: TODAY }) }).state, 'first');
});
t('1 通にまとめて送る・送れた分を記録 = 同じ日に流し直しても二重に鳴らない・送れない / webhook 無し = 記録しない (次の回にまた送る)', async () => {
  const db = listingFixture();
  const msgs = [];
  const send = async (m) => { msgs.push(m); return true; };
  const r1 = await runAlert(db, { kind: 'listing', today: TODAY, webhook: '', send });
  assert.match(r1.summary, /^⚠️.*GCHAT_WEBHOOK/);
  const r2 = await runAlert(db, { kind: 'listing', today: TODAY, webhook: 'https://x', send: async () => false });
  assert.match(r2.summary, /^⚠️.*送れなかった/);
  const r3 = await runAlert(db, { kind: 'listing', today: TODAY, webhook: 'https://x', send });
  assert.match(r3.summary, /6 件を通知/);
  assert.equal(msgs.length, 1);
  assert.match(msgs[0], /BUYABLE でなくなった 3 件/);
  assert.match(msgs[0], /新しい ERROR 3 件/);
  assert.match(msgs[0], /出品停止/);
  const r4 = await runAlert(db, { kind: 'listing', today: TODAY, webhook: 'https://x', send });
  assert.match(r4.summary, /新しく送るもの無し/);
  assert.equal(msgs.length, 1);
});

section('カートを取られたアラート');
function priceRow(db, date, sku, mine, my = 1000, bb = 1000) {
  db.prepare(`INSERT INTO fact_amazon_price_snapshot (date_jst, seller_sku, asin, channel, my_price, buybox_price, buybox_is_mine, fetched_at) VALUES (?, ?, 'B000000001', 'FBA', ?, ?, ?, 'T')`).run(date, sku, my, bb, mine);
}
t('前の回 自社 (1) → 今日 他社 (0) で売れた SKU だけ・🚨 カート不在 (NULL) は取られたにしない・差 = 自社 − カート', () => {
  const db = openDb();
  for (const s of ['lost', 'tonull', 'fromnull', 'stillmine', 'pr_upper']) sold(db, s, { price: 2000 });
  sold(db, 'PR_UPPER');
  priceRow(db, PREV, 'lost', 1); priceRow(db, TODAY, 'lost', 0, 1200, 1100);
  priceRow(db, PREV, 'tonull', 1); priceRow(db, TODAY, 'tonull', null, 1000, null);
  priceRow(db, PREV, 'fromnull', null); priceRow(db, TODAY, 'fromnull', 0);
  priceRow(db, PREV, 'stillmine', 1); priceRow(db, TODAY, 'stillmine', 1);
  priceRow(db, PREV, 'unsold', 1); priceRow(db, TODAY, 'unsold', 0);
  priceRow(db, PREV, 'pr_upper', 1); priceRow(db, TODAY, 'pr_upper', 0, 900, 950);
  const r = buildBuyboxAlerts(db, { today: TODAY, watch: loadWatchSkus(db, { today: TODAY }) });
  assert.deepEqual(r.lost.map((x) => [x.sku_key, x.diff, x.units]), [['pr_upper', -50, 2], ['lost', 100, 1]]);
  const m = formatBuyboxMessage({ today: TODAY, prev: PREV, lost: r.lost });
  assert.match(m, /差 \+¥100/);
  assert.match(m, /差 -¥50/);
});
t('前の回が無い = 通知しない・今日の写しが無い = ⚠️ (比べない)・送った分は二重に鳴らない', async () => {
  const db = openDb();
  sold(db, 'a');
  priceRow(db, TODAY, 'a', 0);
  assert.match((await runAlert(db, { kind: 'buybox', today: TODAY, webhook: 'https://x', send: async () => true })).summary, /通知しない/);
  assert.match((await runAlert(db, { kind: 'buybox', today: '2026-10-23', webhook: 'https://x', send: async () => true })).summary, /^⚠️/);
  priceRow(db, PREV, 'a', 1);
  let n = 0;
  const send = async () => { n++; return true; };
  assert.match((await runAlert(db, { kind: 'buybox', today: TODAY, webhook: 'https://x', send })).summary, /1 件を通知/);
  await runAlert(db, { kind: 'buybox', today: TODAY, webhook: 'https://x', send });
  assert.equal(n, 1);
  assert.equal((await runAlert(db, { kind: 'nope', today: TODAY })).exitCode, 1);
});

section('アラートの対象 (売れた・在庫)');
t('30 日に売れた (取消・0 個・30 日より前を除く) + FBA 在庫 (3 日以内の最新の日・fba_warehouse・qty > 0)・鍵は小文字・登録どおりの形を残す', () => {
  const db = openDb();
  sold(db, 'Pr_A', { qty: 2, price: 3000 }); sold(db, 'pr_a', { qty: 1, price: 1000 });
  sold(db, 'B', { status: 'Cancelled' }); sold(db, 'C', { qty: 0 }); sold(db, 'D', { date: '2026-09-01T00:00:00Z' });
  db.prepare(`INSERT INTO inv_daily_detail VALUES ('2026-10-21', 'jp', 'fba_warehouse', 'fba', 'stk', 'X', 4), ('2026-10-21', 'jp', 'fba_inbound', 'fba', 'inb', 'X', 4),
    ('2026-10-15', 'jp', 'fba_warehouse', 'fba', 'old', 'X', 4), ('2026-10-21', 'us', 'fba_us_warehouse', 'fba', 'us', 'X', 4)`).run();
  const w = loadWatchSkus(db, { today: TODAY });
  assert.deepEqual([...w.keys()].sort(), ['pr_a', 'stk']);
  assert.equal(w.get('pr_a').units, 3);
  assert.equal(w.get('pr_a').sales_jpy, 4000);
  assert.ok(['Pr_A', 'pr_a'].includes(w.get('pr_a').raw));
  assert.deepEqual([w.get('stk').fba_qty, w.get('stk').fba_date], [4, '2026-10-21']);
  assert.equal(loadWatchSkus(new Database(':memory:'), { today: TODAY }).size, 0);   // 表が無い DB
});

section('BSR');
const catItem = (asin, ranks) => ({ asin, salesRanks: ranks });
t('classificationRanks と displayGroupRanks の両方・marketplace が違う・順位が正の整数でない・分類が無いものは外す・同じ分類は小さい順位', () => {
  const rows = parseSalesRanks(catItem('B0A', [
    { marketplaceId: MP, classificationRanks: [{ classificationId: '123', title: 'サプリ', rank: 15 }, { classificationId: '123', title: 'サプリ', rank: 9 }, { classificationId: '', rank: 1 }, { classificationId: '9', rank: 0 }],
      displayGroupRanks: [{ websiteDisplayGroup: 'hpc_display_on_website', title: 'ドラッグストア', rank: 1200 }, { websiteDisplayGroup: 'x', rank: 1.5 }] },
    { marketplaceId: 'ATVPDKIKX0DER', classificationRanks: [{ classificationId: '777', rank: 3 }] },
  ]), MP);
  assert.deepEqual(rows.map((r) => [r.rank_type, r.category_id, r.category_title, r.rank]),
    [['classification', '123', 'サプリ', 9], ['display_group', 'hpc_display_on_website', 'ドラッグストア', 1200]]);
});
t('保存: 同じ日の取り直しは置き換え (冪等)・返らない ASIN = not_returned (順位を消す)・順位なし = no_rank・失敗した batch の ASIN は触らない・400 日より古い行を消す', () => {
  const db = openDb();
  ensureRankTables(db);
  const r1 = { asins: ['B0A', 'B0B', 'B0C'], items: new Map([['B0A', catItem('B0A', [{ marketplaceId: MP, classificationRanks: [{ classificationId: '1', rank: 5 }] }])], ['B0B', catItem('B0B', [])]]) };
  assert.deepEqual(saveRanks(db, { date: TODAY, marketplaceId: MP, results: [r1], observedAt: 'T1' }), { ranked: 1, no_rank: 1, not_returned: 1, rows: 1 });
  saveRanks(db, { date: TODAY, marketplaceId: MP, results: [r1], observedAt: 'T1' });
  assert.equal(db.prepare(`SELECT COUNT(*) c FROM amazon_catalog_rank_daily`).get().c, 1);
  saveRanks(db, { date: TODAY, marketplaceId: MP, results: [{ asins: ['B0A'], items: new Map() }], observedAt: 'T2' });
  assert.equal(db.prepare(`SELECT COUNT(*) c FROM amazon_catalog_rank_daily`).get().c, 0);
  assert.equal(db.prepare(`SELECT status FROM amazon_catalog_rank_fetch WHERE asin = 'B0A'`).get().status, 'not_returned');
  db.prepare(`INSERT INTO amazon_catalog_rank_daily VALUES ('2025-01-01', ?, 'B0Z', 'classification', '1', NULL, 3, 'T')`).run(MP);
  saveRanks(db, { date: TODAY, marketplaceId: MP, results: [], observedAt: 'T3' });
  assert.equal(db.prepare(`SELECT COUNT(*) c FROM amazon_catalog_rank_daily WHERE date_jst = '2025-01-01'`).get().c, 0);
});
t('取得: 20 ASIN ずつ・identifiersType ASIN・salesRanks・pageSize 20・nextToken を辿る・失敗した batch は結果に入れない (= 触らない)', async () => {
  const calls = [];
  const call = async (q) => {
    calls.push(q);
    if (q.identifiers.includes('B000000021')) throw new Error('InternalFailure');
    if (!q.pageToken && q.identifiers.includes('B000000001')) return { items: [catItem('B000000001', [])], pagination: { nextToken: 'p2' } };
    return { items: q.identifiers.filter((a) => a !== 'B000000001').map((a) => catItem(a, [])) };
  };
  const asins = Array.from({ length: 25 }, (_, i) => `B${String(i + 1).padStart(9, '0')}`);
  const r = await fetchAllRanks(call, asins, MP);
  assert.deepEqual([r.batches, r.errors, r.results.length, r.results[0].items.size], [2, 1, 1, 20]);
  assert.deepEqual([calls[0].identifiersType, calls[0].includedData, calls[0].pageSize, calls[0].identifiers.length, calls[1].pageToken], ['ASIN', ['salesRanks'], 20, 20, 'p2']);
  assert.equal((await fetchBatch(async () => ({ items: [] }), ['B0'], MP)).size, 0);
});
t('対象の ASIN = amazon_sku_fees + 30 日に売れた注文 (大文字・10 桁の形だけ・重複なし)', () => {
  const db = openDb();
  db.prepare(`INSERT INTO amazon_sku_fees (seller_sku, asin) VALUES ('a', 'b000000001'), ('b', 'B000000002'), ('c', ''), ('d', 'BAD')`).run();
  sold(db, 'x', { asin: 'B000000003' }); sold(db, 'y', { asin: 'B000000002' }); sold(db, 'z', { asin: 'B000000009', date: '2026-01-01T00:00:00Z' });
  assert.deepEqual(loadTargetAsins(db, { today: TODAY }), ['B000000001', 'B000000002', 'B000000003']);
});

section('レートと共通');
t('🚨 レートの門: 前の呼び出しから間隔を空ける (最初は待たない・時間が経っていれば待たない)', async () => {
  let now = 1000;
  const slept = [];
  const gate = createRateGate({ intervalMs: 250, now: () => now, sleepFn: async (ms) => { slept.push(ms); now += ms; } });
  await gate.wait(); now += 100; await gate.wait(); now += 300; await gate.wait(); await gate.wait();
  assert.deepEqual(slept, [150, 250]);
  assert.equal(gate.waits, 2);
  assert.throws(() => createRateGate({ intervalMs: 0 }));
});
t('やり直し: 失敗したら待ってやり直す・やり直しも門を通る・最後まで失敗したら例外', async () => {
  let n = 0, gates = 0;
  const gate = { wait: async () => { gates++; } };
  const r = await callWithRetry(async () => { if (++n < 3) throw new Error('QuotaExceeded'); return 'ok'; }, { gate, sleepFn: async () => {}, log: () => {} });
  assert.deepEqual([r, n, gates], ['ok', 3, 3]);
  await throwsAsync(() => callWithRetry(async () => { throw new Error('boom'); }, { sleepFn: async () => {}, log: () => {} }), /boom/);
});
t('セラーID: 想定利益の DB の覚え書き → env・無い / 形が違う = 例外にせず error (推測しない)', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'amzai-'));
  try {
    assert.match(resolveSellerId({ dataDir: dir, env: {} }).error, /セラーID が無い/);
    assert.deepEqual(resolveSellerId({ dataDir: dir, env: { SP_API_SELLER_ID: 'AENV12345' } }), { sellerId: 'AENV12345', from: 'env' });
    const db = new Database(path.join(dir, 'expected-profit.db'));
    db.exec(`CREATE TABLE expected_profit_setting (key TEXT PRIMARY KEY, value TEXT, updated_at TEXT)`);
    db.prepare(`INSERT INTO expected_profit_setting VALUES ('amazon_seller_id', 'ASET12345', 'T')`).run();
    db.close();
    assert.deepEqual(resolveSellerId({ dataDir: dir, env: { SP_API_SELLER_ID: 'AENV12345' } }), { sellerId: 'ASET12345', from: 'setting' });
    assert.match(resolveSellerId({ dataDir: path.join(dir, 'none'), env: { SP_API_SELLER_ID: 'bad id' } }).error, /形が違う/);
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
});
t('業務日付 = env WAREHOUSE_BUSINESS_DATE → JST (朝 7:00 JST = UTC の前日 22:00 でも JST)', () => {
  assert.equal(businessDate({ WAREHOUSE_BUSINESS_DATE: '2026-10-22' }), '2026-10-22');
  assert.equal(businessDate({}, Date.parse('2026-10-21T22:00:00Z')), '2026-10-22');
  assert.equal(addDays('2026-03-01', -1), '2026-02-28');
});

section('daily-sync の段');
t('「Amazonカート価格」の後に 出品の状態 → 出品アラート → カートアラート → BSR・アラートは取得が取れた朝だけ・retry に載せない', () => {
  const ds = fs.readFileSync(path.join(ROOT, 'apps/warehouse/daily-sync.js'), 'utf8');
  const at = (s) => { const i = ds.indexOf(s); assert.ok(i > 0, `無い: ${s}`); return i; };
  const iPrices = at("runScript('apps/warehouse/fetch-amazon-prices.js'");
  const iList = at("runScript('apps/warehouse/fetch-amazon-listing-issues.js'");
  const iListAlert = at("runScript('apps/warehouse/alert-amazon-listing.js --kind listing'");
  const iBb = at("runScript('apps/warehouse/alert-amazon-listing.js --kind buybox'");
  const iBsr = at("runScript('apps/warehouse/fetch-amazon-bsr.js'");
  assert.ok(iPrices < iList && iList < iListAlert && iListAlert < iBb && iBb < iBsr);
  assert.match(ds.slice(iList, iListAlert), /if \(listingResult\.success\)/);
  assert.match(ds.slice(iListAlert, iBb), /r\.name === 'Amazonカート価格' && r\.success/);
  const retryable = ds.match(/const RETRYABLE_JOBS = \[([^\]]*)\]/)[1];
  for (const n of ['Amazon出品の状態', 'Amazon出品アラート', 'Amazonカートアラート', 'Amazon BSR']) assert.ok(!retryable.includes(`'${n}'`), n);
  const reg = fs.readFileSync(path.join(ROOT, 'config/jobs-registry.mjs'), 'utf8');
  for (const s of ['fetch-amazon-listing-issues.js', 'alert-amazon-listing.js', 'fetch-amazon-bsr.js']) assert.ok(reg.includes(s), `台帳に ${s} が無い`);
});

for (const [name, fn] of queue) {
  if (name === null) { console.log(fn); continue; }
  try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message)); }
}
console.log(`\n${ok} ok / ${ng} NG`);
process.exitCode = ng ? 1 : 0;
