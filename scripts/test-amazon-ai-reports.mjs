#!/usr/bin/env node
/**
 * test-amazon-ai-reports.mjs — AI の分析とアラート用の SP-API レポートの取得 (2026-10-22) の試験。API・本番 DB には繋がない (見本のデータと :memory: の SQLite)
 *   - apps/warehouse/sp-api-report.js: createReport → polling → 文書 → gzip → 文字コード (UTF-8 / CP932) → TSV / JSON・FATAL / CANCELLED / 上限・429 の再送
 *   - fetch-amazon-sales-traffic.js: 1 日 = 1 レポートの解釈 (無い項目は null・数の検査・日付の検査)・日ごとの置き換え・世代・元の JSON
 *   - fetch-amazon-returns.js / fetch-amazon-reimbursements.js: 解釈・鍵 (occurrence)・UPSERT の冪等・古い取得で戻さない・壊れた行なら何も書かない
 *   - fetch-amazon-ledger.js: "" で囲んだ値・MM/DD/YYYY・期間の外の行を捨てる・日ごとの置き換え
 *   - monitor-amazon-returns.js: しきい値・再通知の間隔
 *   - daily-sync.js: SP-API の主取得が成功した朝だけ・引数つきで呼ぶ
 * 使い方: node scripts/test-amazon-ai-reports.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { gzipSync } from 'node:zlib';
import iconv from 'iconv-lite';
import Database from 'better-sqlite3';
import {
  fetchSpReport, fetchSpReportOrNoData, createRateGate, decodeReportBuffer, parseTsv, SpReportError, parseCommonArgs, looseDateToJst, isoToJstDate, numOrNull, gunzipText,
} from '../apps/warehouse/sp-api-report.js';
import * as ST from '../apps/warehouse/fetch-amazon-sales-traffic.js';
import * as RET from '../apps/warehouse/fetch-amazon-returns.js';
import * as RB from '../apps/warehouse/fetch-amazon-reimbursements.js';
import * as LG from '../apps/warehouse/fetch-amazon-ledger.js';
import * as MON from '../apps/warehouse/monitor-amazon-returns.js';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
let ok = 0, ng = 0;
async function t(name, fn) { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message)); } }
const throws = (fn, re) => { let e = null; try { fn(); } catch (x) { e = x; } if (!e) throw new Error('did not throw'); if (re && !re.test(e.message)) throw new Error(`wrong error: ${e.message}`); return e; };
async function rejects(p, re) { let e = null; try { await p; } catch (x) { e = x; } if (!e) throw new Error('did not reject'); if (re && !re.test(e.message)) throw new Error(`wrong error: ${e.message}`); return e; }

// ─── 見本の SP クライアントと fetch ───
/** statuses = getReport が順に返す processingStatus。docs = { [reportDocumentId]: { body: Buffer, compressionAlgorithm } } */
function fakeSp({ statuses = ['IN_QUEUE', 'IN_PROGRESS', 'DONE'], docId = 'DOC1', createErrors = [], getErrors = [] } = {}) {
  const calls = [];
  let polls = 0;
  return {
    calls,
    async callAPI(p) {
      calls.push(p);
      if (p.operation === 'createReport') { if (createErrors.length) throw createErrors.shift(); return { reportId: 'R1' }; }
      if (p.operation === 'getReport') {
        if (getErrors.length) throw getErrors.shift();
        const st = statuses[Math.min(polls++, statuses.length - 1)];
        return { reportId: 'R1', processingStatus: st, ...(st === 'DONE' || st === 'FATAL' ? { reportDocumentId: docId } : {}) };
      }
      if (p.operation === 'getReportDocument') return { reportDocumentId: p.path.reportDocumentId, url: `https://example.invalid/${p.path.reportDocumentId}`, ...(this.compression ? { compressionAlgorithm: this.compression } : {}) };
      throw new Error('unknown op ' + p.operation);
    },
  };
}
const fakeFetch = (buf, { status = 200 } = {}) => async () => ({ ok: status >= 200 && status < 300, status, arrayBuffer: async () => buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength) });
const noSleep = async () => {};
const quiet = () => {};
const base = (over = {}) => ({ sleepImpl: noSleep, log: quiet, pollIntervalMs: 1, now: () => 1_700_000_000_000, ...over });

console.log('共通の部品 (sp-api-report.js)');
await t('polling で DONE まで待ち、gzip の CP932 の TSV を読む (reportOptions・期間を createReport に渡す・世代 = 頼んだ時刻)', async () => {
  const tsv = 'sku\tproduct-name\tquantity\r\nA-1\t日本語の商品①\t2\r\n';
  const sp = fakeSp(); sp.compression = 'GZIP';
  const r = await fetchSpReport({ sp, reportType: 'X', reportOptions: { a: 1 }, dataStartTime: 'S', dataEndTime: 'E', fetchImpl: fakeFetch(gzipSync(iconv.encode(tsv, 'CP932'))), ...base() });
  assert.equal(r.encoding, 'cp932');
  assert.deepEqual(r.headers, ['sku', 'product-name', 'quantity']);
  assert.deepEqual(r.rows.map((x) => [x.sku, x['product-name'], x.quantity, x.__line]), [['A-1', '日本語の商品①', '2', 2]]);
  assert.equal(r.generation, 1_700_000_000_000);
  const create = sp.calls.find((c) => c.operation === 'createReport');
  assert.deepEqual(create.body, { reportType: 'X', marketplaceIds: ['A1VC38T7YXB528'], reportOptions: { a: 1 }, dataStartTime: 'S', dataEndTime: 'E' });
  assert.equal(sp.calls.filter((c) => c.operation === 'getReport').length, 3);
});
await t('JSON (UTF-8・BOM つき) を読む', async () => {
  const sp = fakeSp();
  const r = await fetchSpReport({ sp, reportType: 'X', format: 'json', fetchImpl: fakeFetch(Buffer.concat([Buffer.from([0xef, 0xbb, 0xbf]), Buffer.from('{"a":"日本"}')])), ...base() });
  assert.deepEqual([r.json, r.encoding], [{ a: '日本' }, 'utf-8']);
});
await t('🚨 FATAL は理由文つきの SpReportError (code FATAL)・CANCELLED も 0 行にせず失敗 (code CANCELLED)', async () => {
  const sp = fakeSp({ statuses: ['IN_PROGRESS', 'FATAL'] });
  const e = await rejects(fetchSpReport({ sp, reportType: 'X', fetchImpl: fakeFetch(Buffer.from('{"errorDetails":"dataStartTime is invalid"}')), ...base() }), /FATAL.*dataStartTime is invalid/);
  assert.ok(e instanceof SpReportError); assert.equal(e.code, 'FATAL'); assert.equal(e.reportId, 'R1');
  const e2 = await rejects(fetchSpReport({ sp: fakeSp({ statuses: ['CANCELLED'] }), reportType: 'X', fetchImpl: fakeFetch(Buffer.from('')), ...base() }), /CANCELLED/);
  assert.equal(e2.code, 'CANCELLED');
});
await t('🚨 polling の上限で終わらなければ TIMEOUT (上限は引数)', async () => {
  const sp = fakeSp({ statuses: ['IN_PROGRESS'] });
  const e = await rejects(fetchSpReport({ sp, reportType: 'X', maxPolls: 3, fetchImpl: fakeFetch(Buffer.from('')), ...base() }), /3 回の確認でも終わらない/);
  assert.equal(e.code, 'TIMEOUT');
  assert.equal(sp.calls.filter((c) => c.operation === 'getReport').length, 3);
});
await t('429 (QuotaExceeded) は待って再送・getReport の一時的な失敗は 2 回まで再送・3 回目は失敗', async () => {
  const waits = [];
  const sp = fakeSp({ createErrors: [Object.assign(new Error('QuotaExceeded'), { code: 'QuotaExceeded' })], getErrors: [new Error('socket hang up')] });
  const r = await fetchSpReport({ sp, reportType: 'X', fetchImpl: fakeFetch(Buffer.from('a\n1\n')), ...base({ sleepImpl: async (ms) => { waits.push(ms); } }), throttleWaitMs: 7 });
  assert.equal(r.rows.length, 1);
  assert.ok(waits.includes(7), JSON.stringify(waits));
  const sp2 = fakeSp({ getErrors: [new Error('e1'), new Error('e2'), new Error('e3')] });
  await rejects(fetchSpReport({ sp: sp2, reportType: 'X', fetchImpl: fakeFetch(Buffer.from('a\n')), ...base() }), /e3/);
});
await t('ダウンロードの HTTP エラー・GZIP と言うのに壊れた文書・列のずれた TSV・JSON でない文書は失敗 (code つき)', async () => {
  assert.equal((await rejects(fetchSpReport({ sp: fakeSp(), reportType: 'X', fetchImpl: fakeFetch(Buffer.from(''), { status: 403 }), ...base() }), /HTTP 403/)).code, 'DOWNLOAD');
  const sp = fakeSp(); sp.compression = 'GZIP';
  assert.equal((await rejects(fetchSpReport({ sp, reportType: 'X', fetchImpl: fakeFetch(Buffer.from('not gzip')), ...base() }), /解凍できない/)).code, 'DECODE');
  assert.equal((await rejects(fetchSpReport({ sp: fakeSp(), reportType: 'X', fetchImpl: fakeFetch(Buffer.from('a\tb\n1\t2\t3\n')), ...base() }), /列が見出しより多い/)).code, 'PARSE');
  assert.equal((await rejects(fetchSpReport({ sp: fakeSp(), reportType: 'X', format: 'json', fetchImpl: fakeFetch(Buffer.from('{oops')), ...base() }), /json として読めない/)).code, 'PARSE');
});
await t('文字コードの判定: UTF-8 / CP932 / 圧縮の印で gzip を解く / 知らない圧縮は失敗', () => {
  assert.deepEqual(decodeReportBuffer(Buffer.from('あ')), { text: 'あ', encoding: 'utf-8', bytes: 3 });
  assert.equal(decodeReportBuffer(iconv.encode('返品の理由', 'CP932')).text, '返品の理由');
  assert.equal(decodeReportBuffer(gzipSync(Buffer.from('x\ty'))).text, 'x\ty');   // compressionAlgorithm が無くても gzip の印で
  throws(() => decodeReportBuffer(Buffer.from('x'), 'ZSTD'), /知らない圧縮/);
});
await t('TSV: "" で囲んだ値を外す・CRLF・空行・足りない列は空・最後の列だけ余りをつなぐ (返品のコメント)・見出しの重複は失敗', () => {
  const p = parseTsv('"Date"\t"Quantity"\r\n"10/01/2026"\t"-3"\r\n\r\n"10/02/2026"\n');
  assert.deepEqual(p.rows.map((r) => [r.Date, r.Quantity]), [['10/01/2026', '-3'], ['10/02/2026', '']]);
  assert.equal(parseTsv('a\tcomments\n1\tx\ty\n', { joinExtraIntoLast: true }).rows[0].comments, 'x\ty');
  throws(() => parseTsv('a\ta\n'), /重複/);
  assert.deepEqual(parseTsv(''), { headers: [], rows: [] });
});
await t('日付と数: 時差つき ISO → JST の日 / YYYY/MM/DD / MM/DD/YYYY / 読めない = null・空の数は null (0 にしない)・負・小数・数でないは例外', () => {
  assert.equal(isoToJstDate('2026-09-30T16:00:00+00:00'), '2026-10-01');
  assert.equal(isoToJstDate('2026-09-30T16:00:00'), null);   // 時差が無い = ISO としては決められない
  assert.equal(looseDateToJst('2026-09-30T16:00:00Z'), '2026-10-01');
  assert.equal(looseDateToJst('2026/9/3'), '2026-09-03');
  assert.equal(looseDateToJst('09/03/2026'), '2026-09-03');
  assert.equal(looseDateToJst('2026-02-30'), null);
  assert.equal(looseDateToJst('abc'), null);
  assert.equal(numOrNull('', 'x'), null);
  assert.equal(numOrNull(' 1,234 ', 'x'), 1234);
  throws(() => numOrNull('-1', 'x'), /0 以上/);
  assert.equal(numOrNull('-1', 'x', { min: -Infinity }), -1);
  throws(() => numOrNull('1.5', 'x', { integer: true }), /整数/);
  throws(() => numOrNull('abc', 'x'), /数でない/);
});
await t('引数: ふだんは JST の昨日まで直近 N 日・--from/--to・未来や逆は失敗・daily-sync の "7" は無視', () => {
  const now = Date.parse('2026-10-21T22:30:00Z');   // JST 10/22 07:30
  assert.deepEqual((({ from, to, days }) => ({ from, to, days }))(parseCommonArgs(['7'], { defaultDays: 5, nowMs: now })), { from: '2026-10-17', to: '2026-10-21', days: 5 });
  assert.deepEqual((({ from, to }) => ({ from, to }))(parseCommonArgs(['--from', '2026-07-01', '--to', '2026-07-31'], { defaultDays: 5, nowMs: now })), { from: '2026-07-01', to: '2026-07-31' });
  throws(() => parseCommonArgs(['--from', '2026-10-01', '--to', '2026-10-22'], { defaultDays: 5, nowMs: now }), /昨日/);
  throws(() => parseCommonArgs(['--from', '2026-10-05', '--to', '2026-10-01'], { defaultDays: 5, nowMs: now }), /期間/);
  throws(() => parseCommonArgs(['--days', '0'], { defaultDays: 5, nowMs: now }), /days/);
  assert.equal(ST.parseArgs(['--boundary', 'utc'], now).boundary, 'utc');
  assert.deepEqual([ST.parseArgs([], now).concurrency, ST.parseArgs(['--concurrency', '5'], now).concurrency], [3, 5]);
  throws(() => ST.parseArgs(['--concurrency', '0'], now), /concurrency/);
  throws(() => ST.parseArgs(['--boundary', 'pst'], now), /boundary/);
  assert.deepEqual(ST.dayWindow('2026-10-01'), { dataStartTime: '2026-10-01T00:00:00+09:00', dataEndTime: '2026-10-01T23:59:59+09:00' });
});

await t('レポートの種類ごとの上限の門: 5 分に 3 回 = 4 つ目は 1 つ目から 5 分 (+余裕) 待つ・同時に呼んでも順に通す・createReport の前に待つ', async () => {
  let clock = 0; const waits = [];
  const gate = createRateGate({ ...ST.CREATE_QUOTA, now: () => clock, sleepImpl: async (ms) => { waits.push(ms); clock += ms; } });
  const at = [];
  await Promise.all([1, 2, 3, 4, 5].map(async () => { await gate(); at.push(clock); }));
  assert.deepEqual(at, [0, 0, 0, 315001, 315001]);
  assert.equal(ST.CREATE_QUOTA.max, 3); assert.ok(ST.CREATE_QUOTA.windowMs >= 300000);
  const order = []; const sp = fakeSp(); const orig = sp.callAPI.bind(sp);
  sp.callAPI = async (p) => { order.push(p.operation); return orig(p); };
  await fetchSpReport({ sp, reportType: 'X', fetchImpl: fakeFetch(Buffer.from('a\n')), ...base(), beforeCreate: async () => { order.push('gate'); } });
  assert.deepEqual(order.slice(0, 2), ['gate', 'createReport']);
});

console.log('アクセス・転換率 (GET_SALES_AND_TRAFFIC_REPORT)');
const D = '2026-10-01';
const stJson = (over = {}) => ({
  reportSpecification: { reportType: ST.REPORT_TYPE, dataStartTime: D, dataEndTime: D },
  salesAndTrafficByDate: [{ date: D,
    salesByDate: { orderedProductSales: { amount: 12345.5, currencyCode: 'JPY' }, unitsOrdered: 10, totalOrderItems: 9, unitsRefunded: 1, refundRate: 10 },
    trafficByDate: { pageViews: 500, sessions: 300, buyBoxPercentage: 95.5, unitSessionPercentage: 3.33 } }],
  salesAndTrafficByAsin: [
    { parentAsin: 'BP1', childAsin: 'B01', sku: 'SKU-A', salesByAsin: { unitsOrdered: 3, orderedProductSales: { amount: 3000, currencyCode: 'JPY' }, totalOrderItems: 3 },
      trafficByAsin: { sessions: 40, pageViews: 60, buyBoxPercentage: 100, unitSessionPercentage: 7.5, sessionsB2B: 2 } },
    { childAsin: 'B02', sku: 'sku-b', salesByAsin: { unitsOrdered: 0 }, trafficByAsin: { sessions: 5 } },
  ],
  ...over,
});
const memDb = () => new Database(':memory:');
await t('解釈: 公式の項目名で読む・無い項目は null (0 にしない)・B2B があれば持つ・親 ASIN は任意', () => {
  const p = ST.parseSalesTrafficDay(stJson(), D);
  assert.equal(p.store.sessions, 300); assert.equal(p.store.ordered_product_sales, 12345.5); assert.equal(p.store.currency_code, 'JPY');
  assert.equal(p.store.page_views_b2b, null); assert.equal(p.store.units_shipped, null);
  const a = p.skus.find((s) => s.seller_sku === 'SKU-A');
  assert.deepEqual([a.sessions, a.page_views, a.buy_box_percentage, a.unit_session_percentage, a.units_ordered, a.ordered_product_sales, a.total_order_items, a.sessions_b2b, a.parent_asin],
    [40, 60, 100, 7.5, 3, 3000, 3, 2, 'BP1']);
  const b = p.skus.find((s) => s.seller_sku === 'sku-b');
  assert.deepEqual([b.units_ordered, b.page_views, b.ordered_product_sales, b.buy_box_percentage, b.parent_asin], [0, null, null, null, null]);
});
await t('🚨 検査: 店全体の行の日付が頼んだ日でない・行が無い・2 日分ある / SKU・子 ASIN が無い / SKU の重複 / 負・小数のセッション・数でない売上 = 例外', () => {
  throws(() => ST.parseSalesTrafficDay(stJson(), '2026-10-02'), /頼んだ日/);
  throws(() => ST.parseSalesTrafficDay(stJson({ salesAndTrafficByDate: [] }), D), /頼んだ日/);
  throws(() => ST.parseSalesTrafficDay(stJson({ salesAndTrafficByDate: [{ date: '2026-09-30' }, { date: D }] }), D), /頼んだ日/);
  const j = (asin) => stJson({ salesAndTrafficByAsin: [asin] });
  throws(() => ST.parseSalesTrafficDay(j({ childAsin: 'B01' }), D), /sku が無い/);
  throws(() => ST.parseSalesTrafficDay(j({ sku: 'S' }), D), /childAsin/);
  throws(() => ST.parseSalesTrafficDay(stJson({ salesAndTrafficByAsin: [{ sku: 'S', childAsin: 'B' }, { sku: 'S', childAsin: 'B' }] }), D), /重複/);
  throws(() => ST.parseSalesTrafficDay(j({ sku: 'S', childAsin: 'B', trafficByAsin: { sessions: -1 } }), D), /sessions/);
  throws(() => ST.parseSalesTrafficDay(j({ sku: 'S', childAsin: 'B', trafficByAsin: { sessions: 1.5 } }), D), /sessions/);
  throws(() => ST.parseSalesTrafficDay(j({ sku: 'S', childAsin: 'B', salesByAsin: { orderedProductSales: { amount: 'abc' } } }), D), /orderedProductSales/);
  throws(() => ST.parseSalesTrafficDay(j({ sku: 'S', childAsin: 'B', salesByAsin: { orderedProductSales: 100 } }), D), /金額の形/);
  throws(() => ST.parseSalesTrafficDay([], D), /想定外/);
  // SKU の配列が無い = 途中のレポート (Codex R2) / 店全体に注文があるのに SKU が 0 行 = 食い違い。どちらも書かない。注文もセッションも 0 の日の [] は受ける
  const noAsin = stJson(); delete noAsin.salesAndTrafficByAsin;
  throws(() => ST.parseSalesTrafficDay(noAsin, D), /salesAndTrafficByAsin が無い/);
  throws(() => ST.parseSalesTrafficDay(stJson({ salesAndTrafficByAsin: [] }), D), /食い違い/);
  const quiet0 = stJson({ salesAndTrafficByAsin: [] }); quiet0.salesAndTrafficByDate[0].salesByDate.unitsOrdered = 0; quiet0.salesAndTrafficByDate[0].trafficByDate.sessions = 0;
  assert.equal(ST.parseSalesTrafficDay(quiet0, D).skus.length, 0);
});
await t('保存: 日ごとに置き換え (減った SKU は消える)・元の JSON を gzip で残す・取得の記録・同じ取得の再実行は冪等', () => {
  const db = memDb();
  const r1 = ST.saveSalesTrafficDay(db, { date: D, json: stJson(), generation: 1000, reportId: 'R1', now: () => 'T1' });
  assert.deepEqual(r1, { status: 'written', skuRows: 2 });
  const r2 = ST.saveSalesTrafficDay(db, { date: D, json: stJson({ salesAndTrafficByAsin: [stJson().salesAndTrafficByAsin[0]] }), rawText: '{"raw":1}', generation: 2000, reportId: 'R2', now: () => 'T2' });
  assert.equal(r2.skuRows, 1);
  assert.deepEqual(db.prepare('select seller_sku, report_id from amazon_sales_traffic_sku_daily where date_jst = ?').all(D), [{ seller_sku: 'SKU-A', report_id: 'R2' }]);
  const raw = db.prepare('select json_gzip, json_bytes, report_id from amazon_sales_traffic_raw where date_jst = ?').get(D);
  assert.deepEqual([gunzipText(raw.json_gzip), raw.json_bytes, raw.report_id], ['{"raw":1}', 9, 'R2']);
  assert.deepEqual(db.prepare("select generation, report_id, row_count from sp_report_fetch_days where report_type = ? and date_jst = ?").get(ST.REPORT_TYPE, D), { generation: 2000, report_id: 'R2', row_count: 1 });
  assert.equal(db.prepare('select sessions from amazon_sales_traffic_daily where date_jst = ?').get(D).sessions, 300);
  assert.equal(ST.saveSalesTrafficDay(db, { date: D, json: stJson(), generation: 2000, reportId: 'R2' }).status, 'same');
  assert.equal(db.prepare('select count(*) n from amazon_sales_traffic_sku_daily').get().n, 1);
});
await t('🚨 古い取得 (世代が小さい) では戻さない・壊れたレポートは何も書かない (前の行も記録も残る)・同じ世代の別のレポートは失敗', () => {
  const db = memDb();
  ST.saveSalesTrafficDay(db, { date: D, json: stJson(), generation: 2000, reportId: 'R2' });
  assert.equal(ST.saveSalesTrafficDay(db, { date: D, json: stJson(), generation: 1500, reportId: 'R1' }).status, 'older');
  throws(() => ST.saveSalesTrafficDay(db, { date: D, json: stJson({ salesAndTrafficByAsin: [{ sku: 'X' }] }), generation: 3000, reportId: 'R3' }), /childAsin/);
  throws(() => ST.saveSalesTrafficDay(db, { date: D, json: stJson(), generation: 2000, reportId: 'R9' }), /同じ世代/);
  assert.equal(db.prepare('select count(*) n from amazon_sales_traffic_sku_daily').get().n, 2);
  assert.equal(db.prepare('select report_id from sp_report_fetch_days where date_jst = ?').get(D).report_id, 'R2');
  throws(() => ST.saveSalesTrafficDay(db, { date: D, json: stJson(), generation: 0, reportId: 'R' }), /generation/);
});

console.log('返品 (GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA)');
const RH = 'return-date\torder-id\tsku\tasin\tfnsku\tproduct-name\tquantity\tfulfillment-center-id\tdetailed-disposition\treason\tstatus\tlicense-plate-number\tcustomer-comments';
const rline = (o = {}) => { const v = { date: '2026-09-30T16:10:00+00:00', order: '250-1', sku: 'SKU-A', asin: 'B01', fnsku: 'X01', name: '商品', qty: '1', fc: 'NRT5', disp: 'SELLABLE', reason: 'UNWANTED_ITEM', status: 'Unit returned to inventory', lpn: 'LPN1', comments: '', ...o };
  return [v.date, v.order, v.sku, v.asin, v.fnsku, v.name, v.qty, v.fc, v.disp, v.reason, v.status, v.lpn, v.comments].join('\t'); };
const rep = (lines, h = RH) => parseTsv([h, ...lines].join('\n'), { joinExtraIntoLast: true });
const W = { from: '2026-09-01', to: '2026-10-01' };
await t('解釈: JST の日付・空は null・LPN の無い同じ行は occurrence で分ける・コメントのタブはつなぐ', () => {
  const rows = RET.parseReturnsRows(rep([rline(), rline({ lpn: '', comments: 'a\tb' }), rline({ lpn: '' }), rline({ disp: '', reason: '' })]));
  assert.equal(rows[0].return_date_jst, '2026-10-01');
  assert.deepEqual(rows.map((r) => [r.license_plate_number, r.occurrence]), [['LPN1', 1], ['', 1], ['', 2], ['LPN1', 2]]);
  assert.equal(rows[1].customer_comments, 'a\tb');
  assert.deepEqual([rows[3].detailed_disposition, rows[3].reason, rows[0].customer_comments], [null, null, null]);
});
await t('🚨 検査: 必須の列が無ければ見出しを出して失敗・日付・注文番号・SKU・数量の読めない行は失敗', () => {
  throws(() => RET.parseReturnsRows(rep([], 'Return Date\tOrder ID')), /必須の列が無い.*Return Date/);
  throws(() => RET.parseReturnsRows(rep([rline({ date: 'yesterday' })])), /return-date/);
  throws(() => RET.parseReturnsRows(rep([rline({ order: '' })])), /order-id/);
  throws(() => RET.parseReturnsRows(rep([rline({ sku: '' })])), /sku/);
  throws(() => RET.parseReturnsRows(rep([rline({ qty: '' })])), /quantity/);
  throws(() => RET.parseReturnsRows(rep([rline({ qty: '1.5' })])), /quantity/);
});
await t('保存: UPSERT の冪等 (同じレポートを 2 回 = 行は増えない)・状態は新しい取得で更新・取得の記録は期間の日ごと (0 行の日も)', () => {
  const db = memDb();
  const a = RET.saveReturns(db, rep([rline(), rline({ order: '250-2', date: '2026-09-02T01:00:00Z' })]), { ...W, generation: 1000, reportId: 'R1' });
  assert.deepEqual([a.rows, a.upserted, a.days], [2, 2, 31]);
  RET.saveReturns(db, rep([rline(), rline({ order: '250-2', date: '2026-09-02T01:00:00Z' })]), { ...W, generation: 1000, reportId: 'R1' });
  assert.equal(db.prepare('select count(*) n from amazon_fba_returns').get().n, 2);
  RET.saveReturns(db, rep([rline({ status: 'Reimbursed' })]), { ...W, generation: 2000, reportId: 'R2' });
  assert.deepEqual(db.prepare("select status, report_id, first_seen_at <= fetched_at ok from amazon_fba_returns where order_id = '250-1'").get(), { status: 'Reimbursed', report_id: 'R2', ok: 1 });
  assert.equal(db.prepare('select count(*) n from amazon_fba_returns').get().n, 2);   // UPSERT = 消えた行は残る
  assert.deepEqual(db.prepare("select row_count, report_id from sp_report_fetch_days where report_type = ? and date_jst = '2026-10-01'").get(RET.REPORT_TYPE), { row_count: 1, report_id: 'R2' });
  assert.equal(db.prepare("select row_count from sp_report_fetch_days where report_type = ? and date_jst = '2026-09-15'").get(RET.REPORT_TYPE).row_count, 0);
});
await t('🚨 古い取得では行も記録も戻さない・壊れた行が 1 つでもあれば何も書かない', () => {
  const db = memDb();
  RET.saveReturns(db, rep([rline({ status: 'NEW' })]), { ...W, generation: 2000, reportId: 'R2' });
  const r = RET.saveReturns(db, rep([rline({ status: 'OLD' })]), { ...W, generation: 1000, reportId: 'R1' });
  assert.deepEqual([r.upserted, r.olderDays], [0, 31]);
  assert.equal(db.prepare('select status from amazon_fba_returns').get().status, 'NEW');
  throws(() => RET.saveReturns(db, rep([rline({ status: 'X', order: '250-9' }), rline({ qty: 'x' })]), { ...W, generation: 3000, reportId: 'R3' }), /quantity/);
  assert.deepEqual([db.prepare('select count(*) n from amazon_fba_returns').get().n, db.prepare('select status from amazon_fba_returns').get().status], [1, 'NEW']);
});

await t('CANCELLED (期間に返品が無い) は返品・補てんだけ 0 行と読む: 記録は 0 行の日・今の行は消さない。FATAL は 0 行と読まない', async () => {
  const db = memDb();
  RET.saveReturns(db, rep([rline()]), { ...W, generation: 1000, reportId: 'R1' });
  const nd = await fetchSpReportOrNoData({ sp: fakeSp({ statuses: ['CANCELLED'] }), reportType: RET.REPORT_TYPE, fetchImpl: fakeFetch(Buffer.from('')), ...base({ now: () => 2000 }) });
  assert.deepEqual([nd.noData, nd.reportId, nd.generation, nd.rows.length], [true, 'R1', 2000, 0]);
  const r = RET.saveReturns(db, { ...nd, reportId: 'RC' }, { ...W, generation: nd.generation, reportId: 'RC' });
  assert.deepEqual([r.rows, r.days], [0, 31]);
  assert.equal(db.prepare('select count(*) n from amazon_fba_returns').get().n, 1);
  assert.deepEqual(db.prepare("select row_count, report_id from sp_report_fetch_days where report_type = ? and date_jst = '2026-10-01'").get(RET.REPORT_TYPE), { row_count: 0, report_id: 'RC' });
  assert.deepEqual(RB.parseReimbursementRows({ ...nd }), []);
  await rejects(fetchSpReportOrNoData({ sp: fakeSp({ statuses: ['FATAL'] }), reportType: 'X', fetchImpl: fakeFetch(Buffer.from('')), ...base() }), /FATAL/);
});

console.log('補てん (GET_FBA_REIMBURSEMENTS_DATA)');
const BH = 'approval-date\treimbursement-id\tcase-id\tamazon-order-id\treason\tsku\tfnsku\tasin\tproduct-name\tcondition\tcurrency-unit\tamount-per-unit\tamount-total\tquantity-reimbursed-cash\tquantity-reimbursed-inventory\tquantity-reimbursed-total\toriginal-reimbursement-id\toriginal-reimbursement-type';
const bline = (o = {}) => { const v = { date: '2026-09-10T05:00:00+00:00', id: '100', reason: 'Lost_Warehouse', sku: 'SKU-A', fnsku: 'X01', per: '1500', total: '3000', cash: '2', inv: '0', qt: '2', ...o };
  return [v.date, v.id, '', '', v.reason, v.sku, v.fnsku, 'B01', '商品', 'NewItem', 'JPY', v.per, v.total, v.cash, v.inv, v.qt, '', ''].join('\t'); };
await t('解釈と保存: 空は null・取り消しの負の金額を許す・鍵 (補てん ID × SKU × FNSKU × occurrence)・UPSERT の冪等', () => {
  const r = RB.parseReimbursementRows(parseTsv([BH, bline(), bline({ sku: 'SKU-B', total: '-1500', qt: '-1', cash: '-1' }), bline({ per: '', total: '' })].join('\n')));
  assert.deepEqual(r.map((x) => [x.seller_sku, x.occurrence, x.amount_total, x.quantity_reimbursed_total, x.case_id]), [['SKU-A', 1, 3000, 2, null], ['SKU-B', 1, -1500, -1, null], ['SKU-A', 2, null, 2, null]]);
  const db = memDb();
  const lines = parseTsv([BH, bline(), bline({ sku: 'SKU-B' })].join('\n'));
  RB.saveReimbursements(db, lines, { ...W, generation: 1000, reportId: 'R1' });
  RB.saveReimbursements(db, lines, { ...W, generation: 1100, reportId: 'R2' });
  assert.equal(db.prepare('select count(*) n from amazon_fba_reimbursements').get().n, 2);
  assert.equal(db.prepare("select approval_date_jst d from amazon_fba_reimbursements limit 1").get().d, '2026-09-10');
});
await t('🚨 検査: 必須の列が無い・補てん ID が無い・日付・金額が数でない = 失敗 (何も書かない)', () => {
  throws(() => RB.parseReimbursementRows(parseTsv('sku\tamount-total\n')), /必須の列/);
  throws(() => RB.parseReimbursementRows(parseTsv([BH, bline({ id: '' })].join('\n'))), /reimbursement-id/);
  throws(() => RB.parseReimbursementRows(parseTsv([BH, bline({ date: '' })].join('\n'))), /approval-date/);
  const db = memDb();
  throws(() => RB.saveReimbursements(db, parseTsv([BH, bline(), bline({ total: '1,5x' })].join('\n')), { ...W, generation: 1, reportId: 'R' }), /amount-total/);
  RB.ensureReimbursementsTable(db);
  assert.equal(db.prepare('select count(*) n from amazon_fba_reimbursements').get().n, 0);
});

console.log('在庫の台帳 (GET_LEDGER_DETAIL_VIEW_DATA)');
const LH = '"Date"\t"FNSKU"\t"ASIN"\t"MSKU"\t"Title"\t"Event Type"\t"Reference ID"\t"Quantity"\t"Fulfillment Center"\t"Disposition"\t"Reason"\t"Country"\t"Reconciled Quantity"\t"Unreconciled Quantity"';
const lline = (o = {}) => { const v = { date: '09/30/2026', fnsku: 'X01', msku: 'SKU-A', type: 'Shipments', qty: '-1', reason: '', rec: '', unrec: '', ...o };
  return [v.date, v.fnsku, 'B01', v.msku, '商品', v.type, 'REF1', v.qty, 'NRT5', 'SELLABLE', v.reason, 'JP', v.rec, v.unrec].map((x) => `"${x}"`).join('\t'); };
const LW = { from: '2026-09-29', to: '2026-09-30' };
await t('解釈: "" を外す・MM/DD/YYYY・負の数量・空の Reconciled は null・期間の外の行は捨てて数える', () => {
  const { rows, outside } = LG.parseLedgerRows(parseTsv([LH, lline(), lline({ date: '09/28/2026' }), lline({ date: '10/01/2026' }), lline({ type: 'Adjustments', qty: '-2', reason: 'M', rec: '0', unrec: '-2' })].join('\n')), LW);
  assert.equal(outside, 2);
  assert.deepEqual(rows.map((r) => [r.event_date, r.event_type, r.quantity, r.reason, r.reconciled_quantity, r.unreconciled_quantity]),
    [['2026-09-30', 'Shipments', -1, null, null, null], ['2026-09-30', 'Adjustments', -2, 'M', 0, -2]]);
});
await t('🚨 検査: 必須の列が無い・日付・数量・イベントの種類の読めない行 = 失敗', () => {
  throws(() => LG.parseLedgerRows(parseTsv('"Date"\t"Quantity"\n'), LW), /必須の列/);
  throws(() => LG.parseLedgerRows(parseTsv([LH, lline({ date: '2026-13-01' })].join('\n')), LW), /Date/);
  throws(() => LG.parseLedgerRows(parseTsv([LH, lline({ qty: 'x' })].join('\n')), LW), /Quantity/);
  throws(() => LG.parseLedgerRows(parseTsv([LH, lline({ type: '' })].join('\n')), LW), /Event Type/);
});
await t('保存: 日ごとに置き換え (行の無い日も 0 行で記録)・同じ取得の再実行は冪等・古い取得では戻さない・壊れたら何も書かない', () => {
  const db = memDb();
  const r1 = LG.saveLedger(db, parseTsv([LH, lline(), lline(), lline({ date: '09/29/2026' })].join('\n')), { ...LW, generation: 1000, reportId: 'R1' });
  assert.deepEqual([r1.days, r1.rows], [2, 3]);   // 同じ中身の 2 行は 2 行のまま
  LG.saveLedger(db, parseTsv([LH, lline(), lline(), lline({ date: '09/29/2026' })].join('\n')), { ...LW, generation: 1000, reportId: 'R1' });
  assert.equal(db.prepare('select count(*) n from amazon_inventory_ledger').get().n, 3);
  const r2 = LG.saveLedger(db, parseTsv([LH, lline({ qty: '-5' })].join('\n')), { ...LW, generation: 2000, reportId: 'R2' });
  assert.deepEqual([r2.days, r2.rows], [2, 1]);
  assert.deepEqual(db.prepare('select event_date d, quantity q from amazon_inventory_ledger').all(), [{ d: '2026-09-30', q: -5 }]);
  assert.equal(db.prepare("select row_count from sp_report_fetch_days where report_type = ? and date_jst = '2026-09-29'").get(LG.REPORT_TYPE).row_count, 0);
  const r3 = LG.saveLedger(db, parseTsv([LH, lline({ qty: '-9' })].join('\n')), { ...LW, generation: 1500, reportId: 'R15' });
  assert.deepEqual([r3.days, r3.skippedOlder.length], [0, 2]);
  throws(() => LG.saveLedger(db, parseTsv([LH, lline({ qty: '-7' }), lline({ qty: '' })].join('\n')), { ...LW, generation: 3000, reportId: 'R3' }), /Quantity/);
  assert.deepEqual(db.prepare('select quantity q, report_id r from amazon_inventory_ledger').all(), [{ q: -5, r: 'R2' }]);
});
await t('頼む期間は前後に 1 日ずつ広げ、終わりは今の 2 分前まで', () => {
  const now = Date.parse('2026-10-21T22:30:00Z');
  assert.deepEqual(LG.requestWindow('2026-09-22', '2026-10-21', now), { dataStartTime: '2026-09-21T00:00:00+09:00', dataEndTime: '2026-10-21T22:28:00Z' });
  assert.deepEqual(LG.requestWindow('2026-09-01', '2026-09-30', now).dataEndTime, '2026-10-01T23:59:59+09:00');
});

console.log('返品のアラート (monitor-amazon-returns.js)');
function alertDb() {
  const db = memDb();
  RET.ensureReturnsTable(db);
  db.exec('CREATE TABLE raw_sp_orders (seller_sku TEXT, quantity INTEGER, fulfillment_channel TEXT, order_status TEXT, purchase_date TEXT, sales_channel TEXT)');
  const ins = db.prepare(`INSERT INTO amazon_fba_returns (order_id, seller_sku, fnsku, return_date, license_plate_number, occurrence, return_date_jst, asin, quantity, reason, report_id, generation, first_seen_at, fetched_at)
    VALUES (?, ?, '', ?, '', 1, ?, 'B', ?, ?, 'R', 1, 'T', 'T')`);
  let n = 0;
  const ret = (sku, d, qty, reason) => ins.run(`O${n++}`, sku, `${d}T00:00:00+09:00`, d, qty, reason);
  const sale = (sku, d, qty, ch = 'Amazon', st = 'Shipped', sc = 'Amazon.co.jp') => db.prepare('INSERT INTO raw_sp_orders VALUES (?, ?, ?, ?, ?, ?)').run(sku, qty, ch, st, `${d}T01:00:00Z`, sc);
  return { db, ret, sale };
}
const TODAY = '2026-10-22';
await t('A: 30 日で 3 個以上かつ返品率 10% 以上だけ (販売 0・2 個・率 10% 未満・FBM と取り消しの販売は数えない・31 日前は外)', () => {
  const { db, ret, sale } = alertDb();
  ret('HIGH', '2026-10-20', 2, 'DEFECTIVE'); ret('HIGH', '2026-10-01', 1, 'UNWANTED_ITEM'); sale('high', '2026-10-05', 20);   // 3/20 = 15% (SKU の大文字小文字は無視)
  sale('HIGH', '2026-10-06', 80, 'Amazon', 'Shipped', 'Non-Amazon JP');                                                    // マルチチャネル発送は分母に入れない (Codex R1)
  ret('LOW', '2026-10-20', 3, 'DEFECTIVE'); sale('LOW', '2026-10-05', 40);                                                // 7.5%
  ret('TWO', '2026-10-20', 2, 'DEFECTIVE'); sale('TWO', '2026-10-05', 2);                                                 // 2 個
  ret('NOSALE', '2026-10-20', 5, 'DEFECTIVE'); sale('NOSALE', '2026-10-05', 9, 'Merchant'); sale('NOSALE', '2026-10-06', 9, 'Amazon', 'Cancelled');
  ret('OLD', '2026-09-21', 3, 'DEFECTIVE'); sale('OLD', '2026-10-05', 3);                                                 // 31 日前
  ret('EDGE', '2026-09-22', 3, 'DEFECTIVE'); sale('EDGE', '2026-10-05', 30);                                             // ちょうど 30 日前・10%
  const { highRate } = MON.evaluateReturnAlerts(db, { today: TODAY });
  assert.deepEqual(highRate.map((a) => [a.sku, a.returnUnits, a.soldUnits, a.topReason]), [['HIGH', 3, 20, 'DEFECTIVE'], ['EDGE', 3, 30, 'DEFECTIVE']]);
});
await t('B: 直近 7 日の同じ理由が 3 個以上かつ前の 28 日の週平均の 3 倍以上 (前が 0 でも 3 個で出す)', () => {
  const { db, ret } = alertDb();
  ret('S1', '2026-10-20', 3, 'DEFECTIVE');                                                       // 前 0 → 出す
  ret('S2', '2026-10-20', 3, 'DEFECTIVE'); for (const d of ['2026-09-20', '2026-09-27', '2026-10-04', '2026-10-10']) ret('S2', d, 2, 'DEFECTIVE');   // 週 2 → 3 < 6 = 出さない
  ret('S3', '2026-10-21', 2, 'DEFECTIVE'); ret('S3', '2026-10-21', 2, 'NOT_AS_DESCRIBED');        // 理由ごとに 2 個 = 出さない
  ret('S4', '2026-10-15', 4, 'DEFECTIVE'); ret('S4', '2026-10-14', 9, 'DEFECTIVE');              // 10-15 は直近 / 10-14 は前 (週 2.25 → 4 < 6.75 = 出さない)
  ret('S5', '2026-10-22', 5, 'DEFECTIVE');                                                       // 今日の分は数えない
  const { reasonSpikes } = MON.evaluateReturnAlerts(db, { today: TODAY });
  assert.deepEqual(reasonSpikes.map((a) => [a.sku, a.reason, a.recentUnits]), [['S1', 'DEFECTIVE', 3]]);
});
await t('再通知の間隔: 送った記録から 7 日のあいだは外す・7 日たてば出す・まとめの 1 通は無ければ null', () => {
  const { db } = alertDb();
  const cand = { highRate: [{ sku: 'A', asin: 'B', returnUnits: 3, soldUnits: 10, rate: 0.3, topReason: 'DEFECTIVE', topReasonUnits: 3 }], reasonSpikes: [{ sku: 'A', reason: 'DEFECTIVE', recentUnits: 3, basePerWeek: 0 }] };
  assert.equal(MON.filterCooldown(db, cand, { today: TODAY }).highRate.length, 1);
  MON.markSent(db, cand, { today: '2026-10-16' });
  const f = MON.filterCooldown(db, cand, { today: TODAY });
  assert.deepEqual([f.highRate.length, f.reasonSpikes.length], [0, 0]);
  assert.equal(MON.filterCooldown(db, cand, { today: '2026-10-23' }).highRate.length, 1);
  const msg = MON.buildMessage(cand, { today: TODAY });
  assert.match(msg, /返品が多い SKU/); assert.match(msg, /A \(B\): 返品 3 \/ 販売 10 = 30%/); assert.match(msg, /DEFECTIVE 直近 3 個/);
  assert.equal(MON.buildMessage({ highRate: [], reasonSpikes: [] }), null);
  // 1 通に載せるのは上から TOP_N だけ = 送った記録もその分だけ (載せなかった 11 件目は次の朝にまた出る。Codex R1)
  const many = { highRate: Array.from({ length: 11 }, (_, i) => ({ sku: `S${i}`, returnUnits: 3, soldUnits: 10, rate: 0.3 })), reasonSpikes: [] };
  assert.equal(MON.buildMessage(many, { today: TODAY }).includes('S10:'), false);
  MON.markSent(db, MON.shownInMessage(many), { today: TODAY });
  assert.deepEqual(MON.filterCooldown(db, many, { today: '2026-10-23' }).highRate.map((a) => a.sku), ['S10']);
});

console.log('daily-sync への組み込み (見張りの後・DBバックアップの前)');
const AI_FILES = ['fetch-amazon-sales-traffic.js', 'fetch-amazon-returns.js', 'fetch-amazon-reimbursements.js', 'fetch-amazon-ledger.js', 'monitor-amazon-returns.js'];
await t('1 つの塊 (見出しのコメントで囲む) で 見張りの後・DBバックアップの前 (あいだに別の段なし)・前の置き場所 (カート価格の後) に残っていない・spResult はその位置で見える', () => {
  const ds = fs.readFileSync(path.join(ROOT, 'apps/warehouse/daily-sync.js'), 'utf8').replace(/\r\n/g, '\n');
  const iHead = ds.indexOf('  // ── Amazon AI 分析用のレポート (設計 28 の外・2026-10-22)');
  const iTail = ds.indexOf('  // ── ここまで Amazon AI 分析用のレポート');
  const iWatch = ds.indexOf("results.push({ name: 'CompanyDB見張り', ...watchResult");
  const iBackup = ds.indexOf("const backupResult = runScript('apps/warehouse/backup-warehouse.js'");
  assert.ok(iWatch > 0 && iWatch < iHead && iHead < iTail && iTail < iBackup, [iWatch, iHead, iTail, iBackup].join(','));
  // 見張りとこの塊のあいだに置いてよいのは、同じ日に足した「出品・カート・BSR」の塊 (feat/amazon-ai-listing) だけ
  const between = ds.slice(iWatch, iHead).split('\n').slice(1).join('\n')
    .replace(/  \/\/ ── Amazon AI 分析用の出品・カート・BSR \(2026-10-22\)[\s\S]*?  \/\/ ── Amazon AI 分析用の出品・カート・BSR ここまで[^\n]*\n?/, '');
  assert.doesNotMatch(between, /runScript\(|results\.push\(/);
  assert.doesNotMatch(ds.slice(iTail, iBackup), /runScript\(|results\.push\(/);
  for (const f of AI_FILES) assert.equal(ds.split(`'apps/warehouse/${f}`).length - 1, 1, `${f} が塊の外にもある / 無い`);
  // spResult は main の同じ深さ (塊の位置まで括弧が閉じない) で宣言されている
  const iSp = ds.indexOf("const spResult = runScript('apps/warehouse/sp-api-orders.js'");
  assert.ok(iSp > 0 && iSp < iHead);
  const code = ds.slice(iSp, iHead).replace(/\/\/[^\n]*/g, '').replace(/`(?:\\.|[^`\\])*`/g, '""').replace(/'(?:\\.|[^'\\\n])*'/g, '""').replace(/"(?:\\.|[^"\\\n])*"/g, '""');
  let depth = 0, min = 0;
  for (const c of code) { if (c === '{') depth++; else if (c === '}') { depth--; min = Math.min(min, depth); } }
  assert.deepEqual([depth, min], [0, 0]);
});
await t('SP-API の主取得が成功した朝だけ・引数つき・返品のアラートは返品の取得が成功した朝だけ・失敗の朝は ⏭️・写しの反映が違う朝も止めない工程 (publish-gate)', async () => {
  const ds = fs.readFileSync(path.join(ROOT, 'apps/warehouse/daily-sync.js'), 'utf8').replace(/\r\n/g, '\n');
  const all = ds.slice(ds.indexOf('  // ── Amazon AI 分析用のレポート'), ds.indexOf('  // ── ここまで Amazon AI 分析用のレポート'));
  const iBlock = all.indexOf('if (spResult.success) {');
  const iElse = all.indexOf("} else {\n    console.log('[DailySync] SP-API 失敗のため Amazon のアクセス");
  assert.ok(iBlock > 0 && iElse > iBlock);
  const block = all.slice(iBlock, iElse);
  for (const s of ['fetch-amazon-sales-traffic.js --days 5', 'fetch-amazon-returns.js --days 60', 'fetch-amazon-reimbursements.js --days 60', 'fetch-amazon-ledger.js --days 30', 'monitor-amazon-returns.js --daily']) {
    assert.ok(block.includes(`'apps/warehouse/${s}'`), s);
  }
  assert.ok(block.indexOf('if (returnsResult.success)') < block.indexOf('monitor-amazon-returns.js'));
  assert.match(all.slice(iElse), /'Amazonアクセス・転換率', 'Amazon返品', 'Amazon補てん', 'Amazon在庫台帳'/);
  const G = await import('../apps/warehouse/publish-gate.js');
  for (const f of AI_FILES) {
    assert.ok(fs.existsSync(path.join(ROOT, 'apps/warehouse', f)), f);
    assert.ok(Object.hasOwn(G.PUBLISH_UNGATED_SCRIPTS, `apps/warehouse/${f}`), f);
    assert.equal(G.publishGateDecision(`apps/warehouse/${f} --days 5`, { broken: true }).skip, false, f);
  }
});

console.log(`\n${ok} ok / ${ng} NG`);
process.exitCode = ng ? 1 : 0;
