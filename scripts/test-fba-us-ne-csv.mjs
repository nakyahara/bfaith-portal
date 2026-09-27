#!/usr/bin/env node
/**
 * 米国用 NE 受注 CSV と米国の伝票の台帳 (設計方針 §12.6・中原さん 9/27 = B) の試験。miniPC・SP-API に行かない。DATA_DIR は一時フォルダ。
 *   node scripts/test-fba-us-ne-csv.mjs
 * ① 台帳 (ledger.js)  ② CSV (ne-csv.js)  ③ 出す前の検査 (checkNeExport)  ④ 日本の計算への合算・影の関所  ⑤ API を通しで
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import http from 'node:http';
import { fileURLToPath, pathToFileURL } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'fba-us-ne-'));
process.env.DATA_DIR = tmp;
process.env.WAREHOUSE_URL = 'http://minipc.test';
delete process.env.RENDER;
const imp = (p) => import(pathToFileURL(path.join(root, p)).href);
const iconv = (await import('iconv-lite')).default;

let pass = 0, fail = 0;
async function t(name, fn) {
  try { await fn(); pass++; console.log(`  ✅ ${name}`); }
  catch (e) { fail++; console.log(`  ❌ ${name}\n     ${e.stack.split('\n').slice(0, 5).join('\n     ')}`); }
}

const ledger = await imp('apps/fba-replenishment-us/ledger.js');
const neCsv = await imp('apps/fba-replenishment-us/ne-csv.js');
const fakeCsv = ({ seq, now }) => ({ orderNo: neCsv.makeOrderNo(now, seq), csv: Buffer.from('x'), filename: `f${seq}.csv` });

console.log('① 台帳');
await t('Render でなければ開かない (not_available)・日本の計算は今までどおり / 出力は断る', async () => {
  ledger._useLedgerForTest(null);
  assert.equal(ledger.readUsReserved().status, 'not_available');
  assert.throws(() => ledger.insertSlip({ requestId: 'r1', items: [], units: [], buildCsv: fakeCsv }), (e) => e.code === 'US_LEDGER_NOT_AVAILABLE');
  assert.equal(ledger.listSlips().status, 'not_available');
});
const lf = path.join(tmp, 'l1', 'fba-us.db');
await t('まだ 1 枚も出していない = 正常な 0 件 (ファイルも作らない)', async () => {
  ledger._useLedgerForTest(lf);
  const r = ledger.readUsReserved();
  assert.deepEqual([r.status, r.count, r.byCode.size, fs.existsSync(lf)], ['ok', 0, 0, false]);
});
await t('伝票を保存 → 押さえ中 (reserved) は倉庫から引き・米国の在庫に足す / 版が進む / 同じ request_id で引ける', async () => {
  const s = ledger.insertSlip({ requestId: 'req-aaaaaaaa', items: [{ sku: 'cardstand-r-40', qty: 10 }], units: [{ code: 'cardstand-r', qty: 400, name: 'x' }], buildCsv: fakeCsv, by: '中原' });
  assert.match(s.order_no, /^USFBA\d{14}-1$/);
  const r = ledger.readUsReserved({ warehouseAtMs: Date.now() });
  assert.deepEqual([r.status, r.version, r.count, r.byCode.get('cardstand-r'), r.incomingBySku.get('cardstand-r-40')], ['ok', 1, 1, 400, 10]);
  assert.equal(ledger.findByRequest('req-aaaaaaaa').order_no, s.order_no);
  assert.equal(fs.existsSync(lf.replace(/\.db$/, '.ledger-created')), true, '作った印が無い');
});
await t('🚨 倉庫から出た (left): 倉庫在庫の時点が出た時刻より後になるまで引き続ける (時点が分からなければ引く)・米国の在庫には足したまま → 米国に載った (arrived) でどちらも外す', async () => {
  const [s] = ledger.listSlips().slips;
  const left = ledger.transition(s.order_no, 'left', { expect: 'reserved', by: '中原' });
  const leftMs = Date.parse(left.left_at);
  const before = ledger.readUsReserved({ warehouseAtMs: leftMs - 1000 });
  const unknown = ledger.readUsReserved({ warehouseAtMs: null });
  const after = ledger.readUsReserved({ warehouseAtMs: leftMs + 1000 });
  assert.deepEqual([before.byCode.get('cardstand-r'), unknown.byCode.get('cardstand-r'), after.byCode.get('cardstand-r') || 0], [400, 400, 0]);
  assert.equal(after.incomingBySku.get('cardstand-r-40'), 10, '倉庫から出た直後に米国の在庫から外している (再推奨になる)');
  ledger.transition(s.order_no, 'arrived', { expect: 'left' });
  const done = ledger.readUsReserved({ warehouseAtMs: leftMs - 1000 });
  assert.deepEqual([done.count, done.incomingBySku.size], [0, 0]);
});
await t('状態の変更: 期待の状態と違えば断る (画面が古い) / 出た後の取消・逆戻りはできない / 取消 (reserved → cancelled) で押さえを外す / 監査の記録', async () => {
  const s = ledger.insertSlip({ requestId: 'req-bbbbbbbb', items: [{ sku: 'a', qty: 1 }], units: [{ code: 'c', qty: 1 }], buildCsv: fakeCsv });
  assert.throws(() => ledger.transition(s.order_no, 'left', { expect: 'left' }), (e) => e.code === 'US_LEDGER_CONFLICT');
  assert.throws(() => ledger.transition(s.order_no, 'arrived', {}), (e) => e.code === 'US_LEDGER_BAD_TRANSITION');
  assert.throws(() => ledger.transition(s.order_no, 'reserved', {}), (e) => e.code === 'US_LEDGER_BAD_STATUS');
  ledger.transition(s.order_no, 'cancelled', { expect: 'reserved', by: 'x', note: 'NE で取消' });
  assert.equal(ledger.readUsReserved().byCode.get('c') || 0, 0);
  const [first] = ledger.listSlips().slips.filter((x) => x.status === 'arrived');
  assert.throws(() => ledger.transition(first.order_no, 'cancelled', {}), (e) => e.code === 'US_LEDGER_BAD_TRANSITION', '出た後の取消を受けた');
  const Database = (await import('better-sqlite3')).default;
  const d = new Database(lf, { readonly: true });
  assert.deepEqual(d.prepare(`SELECT event, to_status FROM us_ne_slip_events WHERE order_no = ? ORDER BY id`).all(s.order_no).map((e) => e.to_status), ['reserved', 'cancelled']);
  d.close();
});
await t('🚨 印があるのに DB が消えた = error (空の 0 件にしない) / 中身が壊れた伝票 = error', async () => {
  const f2 = path.join(tmp, 'l2', 'fba-us.db');
  ledger._useLedgerForTest(f2);
  ledger.insertSlip({ requestId: 'req-cccccccc', items: [{ sku: 'a', qty: 1 }], units: [{ code: 'c', qty: 1 }], buildCsv: fakeCsv });
  ledger._useLedgerForTest(null);
  for (const suf of ['', '-wal', '-shm']) { try { fs.unlinkSync(f2 + suf); } catch { /* 無ければよい */ } }
  ledger._useLedgerForTest(f2);
  const r = ledger.readUsReserved();
  assert.deepEqual([r.status, /消えている/.test(r.error)], ['error', true]);
  const f3 = path.join(tmp, 'l3', 'fba-us.db');
  ledger._useLedgerForTest(f3);
  ledger.insertSlip({ requestId: 'req-dddddddd', items: [{ sku: 'a', qty: 1 }], units: [{ code: 'c', qty: 1 }], buildCsv: fakeCsv });
  const Database = (await import('better-sqlite3')).default;
  ledger._useLedgerForTest(null);
  const d = new Database(f3); d.prepare(`UPDATE us_ne_slips SET units_json = '{broken'`).run(); d.close();
  ledger._useLedgerForTest(f3);
  assert.equal(ledger.readUsReserved().status, 'error');
});
await t('冪等の中身のハッシュ: SKU の大文字小文字・前後の空白・並び順は同じ中身', async () => {
  assert.equal(ledger.contentHashOf([{ sku: ' A ', qty: 1 }, { sku: 'b', qty: 2 }]), ledger.contentHashOf([{ sku: 'B', qty: 2 }, { sku: 'a', qty: '1' }]));
  assert.notEqual(ledger.contentHashOf([{ sku: 'a', qty: 1 }]), ledger.contentHashOf([{ sku: 'a', qty: 2 }]));
});

console.log('② CSV');
await t('日本と同じ 61 列・Shift_JIS・発送先はフォワーダー (会員 ID 2164)・発送方法 福山通運istar2・店舗伝票番号 USFBA + 日時 + 連番・構成品ごとの行', async () => {
  const r = neCsv.buildUsNeCsv([{ code: 'cardstand-r', qty: 400, name: 'カードスタンド' }, { code: 'a-first', qty: 1 }], { now: new Date('2026-09-27T03:04:05Z'), seq: 7 });
  assert.deepEqual([r.orderNo, r.filename], ['USFBA20260927120405-7', 'hanyo-jyuchu_invoice_US_20260927_7.csv']);
  const lines = iconv.decode(r.csv, 'Shift_JIS').split('\r\n');
  const head = lines[0].split(',');
  assert.equal(head.length, 61);
  const jp = fs.readFileSync(path.join(root, 'apps', 'fba-replenishment', 'router.js'), 'utf8');
  for (const h of ['店舗伝票番号', '発送先住所２', '発送方法', '受注数量', 'ラッピング']) assert.ok(jp.includes(`'${h}'`), `日本の見出しに ${h} が無い`);
  const row = lines[2].split(',');
  const col = (name) => row[head.indexOf(name)];
  assert.deepEqual([col('店舗伝票番号'), col('受注名'), col('発送郵便番号'), col('発送先住所１'), col('発送先住所２'), col('発送先名'), col('発送電話番号'), col('発送方法'), col('商品コード'), col('受注数量')],
    ['USFBA20260927120405-7', '20260927米国FBA納品', '2701369', '千葉県印西市鹿黒南1-2', 'グッドマンビジネスパーク・ウエスト5階 2164', '(GBFF)センコー株式会社 印西第二LC内ECMSジャパン', '052-325-2444', '福山通運istar2', 'cardstand-r', '400']);
  assert.equal(lines[1].split(',')[head.indexOf('商品コード')], 'a-first', 'コード順に並んでいない');
});
await t('Shift_JIS に無い文字 (商品名) があれば作らない / 数が 0・小数は作らない', async () => {
  assert.throws(() => neCsv.buildUsNeCsv([{ code: 'a', qty: 1, name: '😀' }], { now: new Date(), seq: 1 }), /Shift_JIS に直せない/);
  assert.throws(() => neCsv.buildUsNeCsv([{ code: 'a', qty: 0 }], { now: new Date(), seq: 1 }), /構成品の行がおかしい/);
  assert.throws(() => neCsv.buildUsNeCsv([], { now: new Date(), seq: 1 }), /直せる行がありません/);
});

console.log('③ 出す前の検査 (checkNeExport)');
const { checkNeExport } = await imp('apps/fba-replenishment-us/router.js');
const viewOf = (rows) => ({ rows: rows.map(([sku, code, per]) => ({ sku, mapping: { route: 'master', components: [{ ne_code: code, qty: per }] } })) });
const allocOf = (over = {}) => ({ reference: false, gates: [], unattributed_jp_loose_count: 0, us: [{ sku: 's-20', status: 'reco' }, { sku: 's-40', status: 'short' }], codes: [{ code: 'c', pool: 1000, unknown: [] }], ...over });
await t('要求の全行を構成品ごとに合算して「米国に回せる数」と比べる (c×20 と c×40 の合計) / 収まれば構成品の個数を返す', async () => {
  const v = viewOf([['s-20', 'c', 20], ['s-40', 'c', 40]]);
  assert.deepEqual(checkNeExport([{ sku: 's-20', qty: 10 }, { sku: 's-40', qty: 20 }], allocOf(), v), [{ code: 'c', qty: 1000 }]);
  assert.throws(() => checkNeExport([{ sku: 's-20', qty: 10 }, { sku: 's-40', qty: 21 }], allocOf(), v), /c: 1040 個は米国に回せる数 1000 個を超えます/);
});
await t('🚨 断る: 参考 / 構成が分からない日本 SKU がある / 判定できない SKU / 構成品が判定できない (期限管理品) / 配分に無い SKU', async () => {
  const v = viewOf([['s-20', 'c', 20]]);
  const rows = [{ sku: 's-20', qty: 1 }];
  assert.throws(() => checkNeExport(rows, allocOf({ reference: true, gates: [{ text: '倉庫 CSV が古い' }] }), v), /参考です \(倉庫 CSV が古い\)/);
  assert.throws(() => checkNeExport(rows, allocOf({ unattributed_jp_loose_count: 2 }), v), /構成が分からない日本の SKU が 2 件/);
  assert.throws(() => checkNeExport(rows, allocOf({ us: [{ sku: 's-20', status: 'unknown', reason: '結びつかない' }] }), v), /s-20: 判定できない \(結びつかない\)/);
  assert.throws(() => checkNeExport(rows, allocOf({ codes: [{ code: 'c', pool: null, unknown: ['期限管理品'] }] }), v), /c: 判定できない \(期限管理品\)/);
  assert.throws(() => checkNeExport([{ sku: 'zzz', qty: 1 }], allocOf(), v), /zzz: 判定できない \(配分に無い\)/);
});

console.log('④ 日本の計算への合算');
const jpDb = await imp('apps/fba-replenishment/db.js');
await jpDb.initDb();
jpDb.upsertSkuMappings([{ amazon_sku: 'JP-ONE', product_name: '単品', ne_code: 'shared', logizard_code: 'shared' }]);
jpDb.saveRestockLatest([{ amazon_sku: 'JP-ONE', product_name: '単品', fba_available: 0, units_sold_30d: 300, amazon_recommended_qty: null }]);
jpDb.replaceWarehouseInventory([{ logizard_code: 'shared', product_name: '共有', location: 'A-01', quantity: 400, reserved: 0, available_qty: 400, expiry_date: '', block_alloc_order: 1 }]);
const { generateRecommendations } = await imp('apps/fba-replenishment/calculation-engine.js');
const engine = (opts) => generateRecommendations(true, {}, { excluded: [], selfShipSales: { status: 'unavailable', map: null }, pendingSlips: { status: 'ok', slips: [], byCode: new Map() }, ...opts });
await t('🚨 米国の押さえ中 (構成品 300 個) は日本の倉庫在庫から引かれる・日本の出荷待ちの一覧 (slips) は日本だけのまま・data_quality に件数と版', async () => {
  const base = engine({ usReserved: { status: 'ok', version: 0, byCode: new Map(), count: 0, units: 0 } }).items.find((i) => i.amazon_sku === 'JP-ONE');
  const r = engine({ usReserved: { status: 'ok', version: 5, byCode: new Map([['shared', 300]]), count: 1, units: 300 } });
  const it = r.items.find((i) => i.amazon_sku === 'JP-ONE');
  assert.ok(base.adjusted_qty > 100, `前提: 引かなければ 100 個より多く出る (${base.adjusted_qty})`);
  assert.ok(it.adjusted_qty <= 100, `米国の押さえ中を引いていない (${it.adjusted_qty})`);
  const al = r.data_quality.allocation;
  assert.deepEqual([al.us_slips.status, al.us_slips.version, al.us_slips.count, al.us_slips.units, al.pending_slips.count], ['ok', 5, 1, 300, 0]);
});
await t('米国の台帳を読めない = us_slips.status error (日本の計算は出す) → 影の下書きの関所で止める / Render でない (not_available) は止めない', async () => {
  const r = engine({ usReserved: { status: 'error', error: '台帳が消えている', version: null, byCode: new Map(), count: 0, units: 0 } });
  assert.equal(r.data_quality.allocation.us_slips.status, 'error');
  const { inputGate } = await imp('apps/fba-replenishment/shadow-draft.mjs');
  const g = inputGate({ inboundState: { source: 'fresh' }, inputFreshness: {}, dq: r.data_quality });
  assert.ok(g.reasons.some((x) => x.code === 'us_slips_unknown' && /台帳が消えている/.test(x.detail)));
  const ok = engine({ usReserved: { status: 'not_available', version: null, byCode: new Map(), count: 0, units: 0 } });
  assert.ok(!inputGate({ inboundState: { source: 'fresh' }, inputFreshness: {}, dq: ok.data_quality }).reasons.some((x) => x.code === 'us_slips_unknown'));
});
await t('試す候補 (planTrials) にも同じ合算済みの出荷待ちが渡る (合算は allocateForItems の 1 か所)', async () => {
  const src = fs.readFileSync(path.join(root, 'apps', 'fba-replenishment', 'calculation-engine.js'), 'utf8');
  const fn = src.slice(src.indexOf('function allocateForItems'), src.indexOf('export function planTrials'));
  const iMerge = fn.indexOf('pending = { ...pending, byCode: merged }');
  assert.ok(iMerge > 0, '合算が無い');
  assert.ok(fn.indexOf('pendingOf: (code) => pending.byCode') > iMerge && fn.indexOf('settings, warehouseMap, normCode, pending,') > iMerge, '合算より前に pending を渡している');
  assert.equal((src.match(/readUsReserved\(/g) || []).length, 1, '米国の台帳を 2 か所で読んでいる');
});

console.log('⑤ API を通しで (miniPC の応答だけ差し替え・台帳は一時ファイル)');
const lf5 = path.join(tmp, 'api', 'fba-us.db');
ledger._useLedgerForTest(lf5);
const mirror = await imp('apps/warehouse-mirror/db.js');
mirror.initMirrorDB();
const mdb = mirror.getMirrorDB();
mdb.prepare("INSERT INTO mirror_sku_resolved (seller_sku, ne_code, quantity, source, 商品名, sort_order, synced_at) VALUES ('cardstand-r-40','cardstand-r',40,'master','x',0,'x')").run();
const todayJst = new Date(Date.now() + 9 * 3600e3).toISOString().slice(0, 10);
mdb.prepare("INSERT INTO mirror_pml_published (id, run_id, status, as_of_date, row_count, src_velocity_as_of, synced_at) VALUES (1,'run1','ok',?,2,?,'x')").run(todayJst, todayJst);
mdb.prepare("INSERT INTO mirror_pml_snapshot_rows (run_id, 商品コード, 販売数30日_FBA以外) VALUES ('run1','cardstand-r',0), ('run1','shared',0)").run();
const nowUtc = new Date().toISOString().slice(0, 19).replace('T', ' ');
jpDb.saveRestockLatest([{ amazon_sku: 'JP-ONE', product_name: '単品', fba_available: 0, units_sold_30d: 300, amazon_recommended_qty: null, source_fetched_at: nowUtc }]);
jpDb.replaceWarehouseInventory([
  { logizard_code: 'shared', product_name: '共有', location: 'A-01', quantity: 400, reserved: 0, available_qty: 400, expiry_date: '', block_alloc_order: 1 },
  { logizard_code: 'cardstand-r', product_name: 'カードスタンド', location: 'A-02', quantity: 5000, reserved: 0, available_qty: 5000, expiry_date: '', block_alloc_order: 1 },
]);
const jstNow = new Date(Date.now() + 9 * 3600e3).toISOString().slice(0, 19).replace('T', ' ');
jpDb.importInboundRows({ shipments: [{ shipment_id: 'FBA1TEST', shipment_name: 'x', created_at: jstNow.slice(0, 16), created_date: jstNow.slice(0, 10), shipment_status: 'RECEIVING', items_synced_at: null, updated_at: jstNow }], items: [] });
const nowIso = new Date().toISOString();
const miniPc = { ok: true, last_attempt: null, file_errors: [], save_failure: null, latest: { schema: 1, market: 'us', business_date: todayJst, reports: {
  restock: { ok: true, fetched_at: nowIso, rows: [{ 'Merchant SKU': 'cardstand-r-40', Available: '0', Working: '0', Shipped: '0', Receiving: '0', 'FC transfer': '0', 'FC Processing': '0', 'Customer Order': '0', Unfulfillable: '0', 'Units Sold Last 30 Days': '43', FNSKU: 'X', 'Recommended replenishment qty': '100' }] },
  planning: { ok: true, fetched_at: nowIso, rows: [{ sku: 'cardstand-r-40', 'units-shipped-t7': '5', 'units-shipped-t30': '43', 'units-shipped-t90': '120' }] } } } };
const realFetch = globalThis.fetch;
globalThis.fetch = async (url, init) => (String(url).startsWith('http://minipc.test/')
  ? { status: 200, ok: true, headers: { get: () => 'application/json' }, json: async () => miniPc, text: async () => '' }
  : realFetch(url, init));
const express = (await import('express')).default;
const router = (await imp('apps/fba-replenishment-us/router.js')).default;
const app = express();
app.use((req, _res, next) => { req.session = { displayName: '中原' }; next(); });
app.use('/apps/fba-replenishment-us', router);
const server = await new Promise((r) => { const s = app.listen(0, () => r(s)); });
const call = (method, p, body) => new Promise((resolve, reject) => {
  const data = body ? Buffer.from(JSON.stringify(body)) : null;
  const req = http.request({ port: server.address().port, path: '/apps/fba-replenishment-us' + p, method, headers: data ? { 'Content-Type': 'application/json', 'Content-Length': data.length } : {} }, (res) => {
    const c = []; res.on('data', (x) => c.push(x)); res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(c) }));
  });
  req.on('error', reject); req.end(data || undefined);
});
const json = (r) => JSON.parse(r.body.toString());
try {
  let orderNo;
  await t('配分が参考でない状態で NE 受注 CSV を出す → 台帳に保存してから CSV (伝票番号つき)・押さえ中に入る', async () => {
    const a = json(await call('GET', '/api/allocation'));
    assert.equal(a.reference, false, JSON.stringify(a.gates));
    const r = await call('POST', '/api/ne-csv', { request_id: 'req-api-00001', items: [{ sku: 'cardstand-r-40', qty: 10 }] });
    assert.equal(r.status, 200, r.body.toString());
    orderNo = r.headers['x-us-order-no'];
    assert.match(orderNo, /^USFBA\d{14}-1$/);
    const text = iconv.decode(r.body, 'Shift_JIS');
    assert.match(text, /,cardstand-r,0,400,/);
    assert.equal(ledger.readUsReserved().byCode.get('cardstand-r'), 400);
  });
  await t('同じ request_id の再送 (通信が切れた・二度押し) は同じ伝票の CSV を返し、二重に押さえない / 同じ request_id で中身が違えば 409', async () => {
    const again = await call('POST', '/api/ne-csv', { request_id: 'req-api-00001', items: [{ sku: 'CARDSTAND-R-40', qty: 10 }] });
    assert.deepEqual([again.status, again.headers['x-us-order-no']], [200, orderNo]);
    assert.equal(ledger.readUsReserved().byCode.get('cardstand-r'), 400, '二重に押さえた');
    const diff = await call('POST', '/api/ne-csv', { request_id: 'req-api-00001', items: [{ sku: 'cardstand-r-40', qty: 11 }] });
    assert.equal(diff.status, 409);
  });
  await t('🚨 既に出した米国の伝票を引いた後の「米国に回せる数」を超える依頼は断る (台帳に何も足さない)', async () => {
    const a = json(await call('GET', '/api/allocation'));
    const pool = a.codes.find((b) => b.code === 'cardstand-r').pool;
    assert.equal(a.codes.find((b) => b.code === 'cardstand-r').us_pending, 400);
    const over = Math.floor(pool / 40) + 1;
    const r = await call('POST', '/api/ne-csv', { request_id: 'req-api-00002', items: [{ sku: 'cardstand-r-40', qty: over }] });
    assert.equal(r.status, 409, r.body.toString());
    assert.match(json(r).message, /米国に回せる数/);
    assert.equal(ledger.listSlips().slips.length, 1);
  });
  await t('伝票の一覧・状態の変更 (期待の状態つき)・保存した CSV / その伝票の STA Excel を取り直せる', async () => {
    const list = json(await call('GET', '/api/slips'));
    assert.deepEqual([list.slips.length, list.slips[0].order_no, list.slips[0].status, list.slips[0].created_by], [1, orderNo, 'reserved', '中原']);
    const bad = await call('POST', `/api/slips/${orderNo}/transition`, { to: 'left', expect: 'left' });
    assert.equal(bad.status, 409);
    const ok = json(await call('POST', `/api/slips/${orderNo}/transition`, { to: 'left', expect: 'reserved' }));
    assert.deepEqual([ok.ok, ok.slip.status, ok.slip.left_by], [true, 'left', '中原']);
    const csv = await call('GET', `/api/slips/${orderNo}/csv`);
    assert.match(iconv.decode(csv.body, 'Shift_JIS'), new RegExp(orderNo));
    const sta = await call('GET', `/api/slips/${orderNo}/sta`);
    assert.equal(sta.status, 200);
    const ExcelJS = (await import('exceljs')).default;
    const wb = new ExcelJS.Workbook(); await wb.xlsx.load(sta.body);
    const ws = wb.getWorksheet('Create workflow – template');
    assert.deepEqual([ws.getCell('A9').value, ws.getCell('B9').value], ['cardstand-r-40', 10]);
  });
  await t('米国の在庫に「送っている途中」を足す: 伝票の 10 個が在庫日数に入り、同じ SKU を二重に推奨しない', async () => {
    const a = json(await call('GET', '/api/allocation'));
    const u = a.us.find((x) => x.sku === 'cardstand-r-40');
    assert.deepEqual([u.on_hand_amazon, u.incoming, u.on_hand], [0, 10, 10]);
  });
} finally {
  server.close();
  globalThis.fetch = realFetch;
  ledger._useLedgerForTest(null);
}

console.log(`\n${pass} passed / ${fail} failed`);
process.exit(fail ? 1 : 0);
