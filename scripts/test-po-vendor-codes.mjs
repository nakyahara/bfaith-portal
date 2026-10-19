/**
 * test-po-vendor-codes.mjs — 先方番号対応表の 1 行の書き込みの部品 (apps/purchase-orders/vendor-codes.js・CompanyDB構想 27 ⑤-1 / ⑤-2 §2.4)
 *
 * 発注アプリの SQLite = 一時の DATA_DIR の warehouse-mirror.db (本番の DB に触らない)。発注アプリの router を HTTP 越しにも通す。
 * 固定する契約:
 *   P 形: 先方品番・入数の欄 (空 = null・入数だけ = 断る・全角の数・改行・100 字・0 / 上限超え)
 *   U 足す・直す (発注アプリのマスタ管理と同じ確かめ): 仕入先が発注アプリにある・仕入先一致 (この商品の仕入先 = 商品管理リスト → 新商品)・
 *     先に読んだ版 (null = まだ無い / 違う = 409 で何も書かない)・入数は undefined = 変えない / 空 = 消す・同じ番号がほかの商品にも = 警告 (止めない)・
 *     記録 (po_audit_log に via・product_key)・版の印は同じミリ秒でも必ず進む (消して作り直しても前の印にならない)
 *   D 消す: 版が違う = 409・記録・二重に消す = 404 / Q 入数だけ: 404・記録 (via = po-inbound)
 *   H 「この商品の先方番号の記録」: 足す・消す・入数・CSV の changed_keys (足した / 消した / 変えた商品だけ)
 *   R 新商品の登録の先方品番 (writeRegistrationOrderSettings): 同じ取引で対応表と発注の設定・先方品番だけ = 発注の設定の行は作らない・
 *     やり直しは前の結果・違う先方品番 = request_id_reused・先方品番が無い = 今までと同じハッシュ・行がある / 記録がある = 409 stale (何も残らない)・
 *     代表の仕入先が無い / 発注アプリに無い = 400・仮登録の同じ番号 = 結ぶ候補
 *   S 画面に出す値 (全部の仕入先の行・仮登録の候補・リードタイム)・登録の前の確かめ
 *   A 発注アプリの API (マスタ管理・入荷予定) は部品を通る = 記録に via po-admin / po-inbound・CSV の記録に changed_keys
 * 使い方: node scripts/test-po-vendor-codes.mjs
 */
import assert from 'node:assert/strict';
import path from 'node:path';
import http from 'node:http';
import { createHash } from 'node:crypto';
import { pathToFileURL, fileURLToPath } from 'node:url';
import { temporaryTestDataDir } from './test-temp-dir.mjs';

const SCRATCH = await temporaryTestDataDir(import.meta.url, 'po-vendor-codes-');
process.env.DATA_DIR = SCRATCH;
const WORK = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const imp = (p) => import(pathToFileURL(path.join(WORK, p)).href);
const killer = setTimeout(() => { console.error('時間切れ (120 秒)'); process.exit(3); }, 120e3);
killer.unref();

const { initMirrorDB } = await imp('apps/warehouse-mirror/db.js');
initMirrorDB();
(await imp('lib/master-legacy-gate.mjs')).__setLegacyPhaseReader(async () => ({ readable: true, phase: 'legacy_open' }));
const { getDB } = await imp('apps/purchase-orders/db.js');
const VC = await imp('apps/purchase-orders/vendor-codes.js');
const OS = await imp('apps/purchase-orders/order-settings.js');
const NS = await imp('apps/purchase-orders/new-skus.js');
const ledger = await imp('apps/purchase-orders/ledger.js');
const router = (await imp('apps/purchase-orders/router.js')).default;
const express = (await import('express')).default;

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e && e.stack || e}`); process.exitCode = 1; } }

const db = getDB();
const T0 = new Date(Date.now() - 86400e3).toISOString();
db.prepare(`INSERT INTO mirror_pml_published (id, run_id, status, as_of_date, synced_at) VALUES (1, 'run_t', 'ok', ?, ?)`).run(T0.slice(0, 10), T0);
const insRow = db.prepare(`INSERT INTO mirror_pml_snapshot_rows (run_id, 商品コード, 商品名, 仕入先, 取扱区分, 商品区分, 総在庫数, 注残数, 販売数7日_合計, 販売数30日_合計, 発注ロット単位, 推奨保有月数, 売価, 原価)
  VALUES ('run_t', ?, ?, ?, '取扱中', '', 0, 0, 0, 0, 1, 1.5, 500, 200)`);
insRow.run('vc-a', '先方番号の商品 A', '0001');
insRow.run('VC-B', '先方番号の商品 B', '0001');
insRow.run('vc-other', 'ほかの仕入先の商品', '0002');
insRow.run('vc-nosup', '仕入先が空', '');
const insSup = db.prepare('INSERT INTO po_suppliers (supplier_code, name, created_at, updated_at) VALUES (?,?,?,?)');
insSup.run('1', 'アメージングクラフト', T0, T0);
insSup.run('2', 'ビーフリー', T0, T0);
db.prepare('UPDATE po_suppliers SET lead_days = 14 WHERE supplier_code = ?').run('1');

const row = (sup, key) => db.prepare('SELECT * FROM po_vendor_code_map WHERE supplier_code=? AND product_key=?').get(sup, key) || null;
const auditOf = (sup) => db.prepare('SELECT action, actor, request_id, detail_json FROM po_audit_log WHERE resource = ? ORDER BY id').all(`supplier:${sup}`).map((r) => ({ ...r, detail: JSON.parse(r.detail_json) }));
const prodA = { prodSup: '1', error: null, newSku: null };
const up = (over = {}) => VC.upsertVendorCode({ supplierCode: '1', productCode: 'vc-a', vendorCode: 'AC-1', prod: prodA, actor: 'naka@test', via: 'master-edit:sku', ...over });

// 発注アプリの router (HTTP)
const app = express();
app.use((req, res, next) => { req.session = { authenticated: true, email: 'naka@test', allowedApps: '*' }; next(); });
app.use('/apps/purchase-orders', express.json({ limit: '1mb' }), router);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const base = `http://127.0.0.1:${server.address().port}/apps/purchase-orders`;
const jpost = async (p, body, method = 'POST') => { const r = await fetch(base + p, { method, headers: { 'Content-Type': 'application/json' }, body: body == null ? undefined : JSON.stringify(body) }); return { status: r.status, body: await r.json() }; };

await ta('[P1] 形: 空 = null・入数だけ = 断る・全角の数・改行・100 字・0 / 上限超え', () => {
  assert.equal(VC.parseVendorInput(null), null);
  assert.equal(VC.parseVendorInput({ vendor_code: '  ', qty_per_unit: '' }), null);
  assert.deepEqual(VC.parseVendorInput({ vendor_code: ' AC-1 ', qty_per_unit: '２４０' }), { vendorCode: 'AC-1', qtyPerUnit: 240 });
  assert.deepEqual(VC.parseVendorInput({ vendor_code: 'AC-1', qty_per_unit: '1,000' }), { vendorCode: 'AC-1', qtyPerUnit: 1000 });
  assert.deepEqual(VC.parseVendorInput({ vendor_code: 'AC-1', qty_per_unit: '0.5' }), { vendorCode: 'AC-1', qtyPerUnit: 0.5 });
  assert.deepEqual(VC.parseVendorInput({ vendor_code: 'AC-1' }), { vendorCode: 'AC-1', qtyPerUnit: null });
  const err = (raw, field) => assert.throws(() => VC.parseVendorInput(raw), (e) => e instanceof VC.VendorCodeError && e.status === 400 && e.field === field);
  err({ vendor_code: '', qty_per_unit: '3' }, 'vendor_code');
  err({ vendor_code: 'A\nB' }, 'vendor_code');
  err({ vendor_code: 'x'.repeat(101) }, 'vendor_code');
  err({ vendor_code: 'A', qty_per_unit: '0' }, 'qty_per_unit');
  err({ vendor_code: 'A', qty_per_unit: '1000001' }, 'qty_per_unit');
  err({ vendor_code: 'A', qty_per_unit: 'abc' }, 'qty_per_unit');
  err({ vendor_code: { x: 1 } }, 'vendor_code');
  err([], 'vendor_code');
});

await ta('[U0] 版の印は表と記録のいちばん新しい時刻より後から始める (再起動の後に時計が戻っても前の版を出さない・Codex R1 M3)', () => {
  const future = new Date(Date.now() + 3600e3).toISOString();
  db.prepare("INSERT INTO po_vendor_code_map (supplier_code, product_key, product_code, vendor_code, updated_at) VALUES ('2', 'seed-x', 'seed-x', 'S', ?)").run(future);
  assert.ok(VC.vendorStamp() > future, '表の版より古い印を出した');
  db.prepare("DELETE FROM po_vendor_code_map WHERE product_key = 'seed-x'").run();
});

await ta('[U0b] 時計が止まったまま続けて直す → 消す → 起動し直す (時計は同じ) → 作り直す = 前の版の印にならない (古い画面の保存は 409・Codex R2 M3)', () => {
  const realNow = Date.now;
  const FIX = realNow() + 7200e3;
  Date.now = () => FIX;
  try {
    let r = up({ productCode: 'stamp-x', baseUpdatedAt: null, vendorCode: 'S1' });
    const v1 = r.row.updated_at;
    r = up({ productCode: 'stamp-x', baseUpdatedAt: v1, vendorCode: 'S2' });
    const v2 = r.row.updated_at;
    assert.ok(v2 > v1);
    VC.deleteVendorCode({ supplierCode: '1', productCode: 'stamp-x', base: v2, actor: 'naka@test', via: 'po-admin' });
    VC.__resetVendorStampForTest();
    r = up({ productCode: 'stamp-x', baseUpdatedAt: null, vendorCode: 'S3' });
    assert.ok(r.row.updated_at > v2, `作り直した版 ${r.row.updated_at} が前の版 ${v2} 以下`);
    assert.equal(up({ productCode: 'stamp-x', baseUpdatedAt: v2, vendorCode: 'OLD' }).status, 409);
    assert.equal(up({ productCode: 'stamp-x', baseUpdatedAt: v1, vendorCode: 'OLD' }).status, 409);
    VC.deleteVendorCode({ supplierCode: '1', productCode: 'stamp-x', base: r.row.updated_at, actor: 'naka@test', via: 'po-admin' });
  } finally { Date.now = realNow; }
});

await ta('[U1] 足す (版 null)・仕入先一致・未登録の仕入先・商品の確かめの誤り・記録 (via・product_key)', () => {
  let r = up({ supplierCode: '9' });
  assert.deepEqual([r.status, r.body.error], [400, '仕入先が未登録です: 9']);
  r = up({ prod: { prodSup: '2', error: null } });
  assert.equal(r.status, 400); assert.match(r.body.error, /この商品の仕入先は 2 です \(アメージングクラフト の対応表には登録できません\)/);
  r = up({ prod: { prodSup: '', error: '商品マスタに無い商品コードです: vc-a (x)' } });
  assert.deepEqual([r.status, r.body.error], [400, '商品マスタに無い商品コードです: vc-a (x)']);
  r = up({ vendorCode: 'A\rB' });
  assert.deepEqual([r.status, r.body.error], [400, '先方管理番号に改行は使えません']);
  r = up({ qtyPerUnit: '-1' });
  assert.equal(r.status, 400); assert.match(r.body.error, /入数は0より大きく/);
  assert.equal(row('1', 'vc-a'), null, '断ったのに書いた');
  r = up({ baseUpdatedAt: null, qtyPerUnit: 240, requestId: 'rid-1' });
  assert.deepEqual([r.status, r.body.ok, r.body.updated, r.body.warning], [200, true, false, null]);
  const w = row('1', 'vc-a');
  assert.deepEqual([w.product_code, w.vendor_code, w.qty_per_unit, w.updated_at], ['vc-a', 'AC-1', 240, r.row.updated_at]);
  const au = auditOf('1').at(-1);
  assert.deepEqual([au.action, au.actor, au.request_id, au.detail.via, au.detail.product_key, au.detail.vendorCode, au.detail.oldVendorCode, au.detail.qtyPerUnit], ['vendor_map_entry_upsert', 'naka@test', 'rid-1', 'master-edit:sku', 'vc-a', 'AC-1', null, 240]);
  // 書いた画面が分からない = 投げる (書かない)
  assert.throws(() => up({ via: 'nowhere', baseUpdatedAt: w.updated_at }), /via/);
  assert.equal(row('1', 'vc-a').vendor_code, 'AC-1');
});

await ta('[U2] 楽観ロック: 版 null で既にある = 409・古い版 = 409 (何も書かない)・合う版 = 直す・入数 undefined = そのまま / 空 = 消す・版は必ず進む', () => {
  const cur = row('1', 'vc-a');
  let r = up({ baseUpdatedAt: null, vendorCode: 'AC-DUP' });
  assert.deepEqual([r.status, r.body.conflict, r.body.current.vendor_code], [409, true, 'AC-1']);
  r = up({ baseUpdatedAt: '2020-01-01T00:00:00.000Z', vendorCode: 'AC-OLD' });
  assert.deepEqual([r.status, r.body.conflict], [409, true]);
  assert.equal(row('1', 'vc-a').vendor_code, 'AC-1');
  r = up({ baseUpdatedAt: cur.updated_at, vendorCode: 'AC-2' });
  assert.deepEqual([r.status, r.body.updated, r.body.oldVendorCode], [200, true, 'AC-1']);
  assert.deepEqual([row('1', 'vc-a').vendor_code, row('1', 'vc-a').qty_per_unit], ['AC-2', 240], '入数を指定しないのに変えた');
  assert.ok(row('1', 'vc-a').updated_at > cur.updated_at);
  r = up({ baseUpdatedAt: row('1', 'vc-a').updated_at, vendorCode: 'AC-2', qtyPerUnit: '' });
  assert.equal(row('1', 'vc-a').qty_per_unit, null);
  // 同じミリ秒に何回書いても印は前より新しい
  const stamps = Array.from({ length: 50 }, () => VC.vendorStamp());
  assert.ok(stamps.every((s, i) => i === 0 || s > stamps[i - 1]), '版の印が進まない');
  assert.ok(stamps[0] > row('1', 'vc-a').updated_at);
});

await ta('[U3] 同じ番号がほかの商品にも = 警告だけ (書く)', () => {
  const r = VC.upsertVendorCode({ supplierCode: '0001', productCode: 'VC-B', vendorCode: ' ac-2 ', baseUpdatedAt: null, prod: prodA, actor: 'naka@test', via: 'po-admin' });
  assert.deepEqual([r.status, r.body.ok], [200, true]);
  assert.match(r.body.warning, /この先方番号は商品 vc-a にも登録されています/);
  assert.equal(row('1', 'vc-b').product_code, 'VC-B');
});

await ta('[D1] 消す: 版が違う = 409・合う = 消す (記録)・二重 = 404 / [Q1] 入数だけ: 無い = 404・記録 (po-inbound)', () => {
  let r = VC.deleteVendorCode({ supplierCode: '1', productCode: 'VC-B', base: '2020-01-01T00:00:00.000Z', actor: 'naka@test', via: 'master-edit:sku' });
  assert.deepEqual([r.status, r.body.conflict], [409, true]);
  r = VC.deleteVendorCode({ supplierCode: '1', productCode: 'VC-B', base: row('1', 'vc-b').updated_at, actor: 'naka@test', via: 'master-edit:sku' });
  assert.deepEqual([r.status, r.body.ok, row('1', 'vc-b')], [200, true, null]);
  const au = auditOf('1').at(-1);
  assert.deepEqual([au.action, au.detail.via, au.detail.product_key, au.detail.vendorCode], ['vendor_map_entry_delete', 'master-edit:sku', 'vc-b', 'ac-2']);
  assert.equal(VC.deleteVendorCode({ supplierCode: '1', productCode: 'vc-b', base: 'x', actor: 'naka@test', via: 'master-edit:sku' }).status, 404);
  assert.equal(VC.setQtyPerUnit({ supplierCode: '1', productCode: 'vc-b', qtyPerUnit: '3', actor: 'naka@test' }).status, 404);
  r = VC.setQtyPerUnit({ supplierCode: '1', productCode: 'VC-A', qtyPerUnit: '12', actor: 'naka@test' });
  assert.deepEqual([r.status, r.body.qtyPerUnit, row('1', 'vc-a').qty_per_unit], [200, 12, 12]);
  assert.deepEqual([auditOf('1').at(-1).action, auditOf('1').at(-1).detail.via], ['vendor_map_qty_per_unit', 'po-inbound']);
});

await ta('[H1] この商品の先方番号の記録: 足す / 消す / 入数 / 古い形の記録 (productCode・productKey) / CSV の changed_keys', async () => {
  assert.equal(VC.vendorHistoryExists(db, '1', 'vc-a'), true);
  assert.equal(VC.vendorHistoryExists(db, '1', 'vc-b'), true, '足して消した商品の記録が見えない');
  assert.equal(VC.vendorHistoryExists(db, '2', 'vc-a'), false, 'ほかの仕入先の記録を見た');
  assert.equal(VC.vendorHistoryExists(db, '1', 'never'), false);
  ledger.audit(db, { actor: 'old', action: 'vendor_map_entry_upsert', resource: 'supplier:1', detail: { productCode: 'Old-Code', vendorCode: 'X' } });
  ledger.audit(db, { actor: 'old', action: 'vendor_map_qty_per_unit', resource: 'supplier:1', detail: { productKey: 'old-qpu', old: null, new: 2 } });
  assert.equal(VC.vendorHistoryExists(db, '1', 'old-code'), true);
  assert.equal(VC.vendorHistoryExists(db, '1', 'old-qpu'), true);
  // CSV の全入れ替え (発注アプリの API): 記録に足した・消した・変えた商品だけ (同じ値の商品は入れない)・版は全部新しい
  const before = row('1', 'vc-a').updated_at;
  const csv = (text) => { const fd = new FormData(); fd.append('supplier_code', '1'); fd.append('file', new Blob([text], { type: 'text/csv' }), 'map.csv'); return fetch(base + '/api/vendor-map/csv', { method: 'POST', body: fd }).then((r) => r.json()); };
  let j = await csv('仕入先管理番号,商品名,弊社管理番号,入数\nAC-2,,vc-a,12\nCSV-1,,csv-new,\n');
  assert.equal(j.ok, true, JSON.stringify(j));
  let au = auditOf('1').filter((a) => a.action === 'vendor_map_import').at(-1);
  assert.deepEqual([au.detail.via, au.detail.changed_keys], ['po-admin', ['csv-new']]);
  assert.ok(row('1', 'vc-a').updated_at > before, 'CSV の後に版が進まない');
  j = await csv('仕入先管理番号,商品名,弊社管理番号,入数\nAC-3,,vc-a,12\n');
  au = auditOf('1').filter((a) => a.action === 'vendor_map_import').at(-1);
  assert.deepEqual(au.detail.changed_keys, ['csv-new', 'vc-a']);
  assert.equal(VC.vendorHistoryExists(db, '1', 'csv-new'), true, 'CSV で足して消した商品の記録が見えない');
});

await ta('[A1] 発注アプリの API は部品を通る: 1 件の追加・修正・削除 (via po-admin)・入数だけ (po-inbound)・答えは今までの形', async () => {
  let r = await jpost('/api/vendor-map/entry', { supplier_code: '1', product_code: 'VC-B', vendor_code: 'AC-B', baseUpdatedAt: null });
  assert.deepEqual([r.status, r.body.ok, r.body.updated, r.body.oldVendorCode, r.body.warning], [200, true, false, null, null]);
  assert.ok(!('row' in r.body), '今までの答えに無い項目を足した');
  assert.equal(auditOf('1').at(-1).detail.via, 'po-admin');
  r = await jpost('/api/vendor-map/entry', { supplier_code: '1', product_code: 'vc-other', vendor_code: 'X' });
  assert.equal(r.status, 400); assert.match(r.body.error, /この商品の仕入先は 2 です/);
  r = await jpost('/api/vendor-map/entry', { supplier_code: '1', product_code: 'vc-nosup', vendor_code: 'X' });
  assert.equal(r.status, 400); assert.match(r.body.error, /仕入先が未設定/);
  r = await jpost('/api/vendor-map/qty-per-unit', { supplier_code: '1', product_code: 'vc-b', qty_per_unit: '6' });
  assert.deepEqual([r.status, r.body.qtyPerUnit, auditOf('1').at(-1).detail.via], [200, 6, 'po-inbound']);
  r = await jpost(`/api/vendor-map/entry?supplier=1&product=vc-b&base=${encodeURIComponent('2020-01-01')}`, null, 'DELETE');
  assert.deepEqual([r.status, r.body.conflict, Object.keys(r.body.current).sort().join(',')], [409, true, 'product_code,updated_at,vendor_code']);
  r = await jpost(`/api/vendor-map/entry?supplier=1&product=vc-b&base=${encodeURIComponent(row('1', 'vc-b').updated_at)}`, null, 'DELETE');
  assert.deepEqual([r.status, r.body.ok, auditOf('1').at(-1).detail.via], [200, true, 'po-admin']);
});

await ta('[N1] 仕入先一致の決まり (productForVendorMap): 商品管理リストの仕入先 → 無ければ新商品の代表の仕入先・どちらでもない = 断る', async () => {
  assert.deepEqual(await VC.productForVendorMap('VC-A', null, 'x'), { prodSup: '1', error: null, newSku: null });
  assert.match((await VC.productForVendorMap('vc-nosup', null, 'x')).error, /仕入先が未設定/);
  NS.setNewSkuReaderForTest(async () => ({ ok: true, map: new Map([['new-1', { code: 'NEW-1', name: '新商品', state: 'draft', supplier: '1' }], ['gone-1', { code: 'gone-1', name: 'やめた', state: 'cancelled', supplier: '1' }]]) }));
  try {
    const n = await VC.productForVendorMap('new-1', null, 'x');
    assert.deepEqual([n.prodSup, n.error, n.newSku.code], ['1', null, 'NEW-1']);
    assert.match((await VC.productForVendorMap('gone-1', null, 'x')).error, /登録をやめた/);
    assert.equal((await VC.productForVendorMap('nothing', null, 'ヒント')).error, '商品マスタに無い商品コードです: nothing (ヒント)');
  } finally { NS.setNewSkuReaderForTest(null); }
});

const regIn = (code, over = {}) => ({ code, patch: { order_lot: null, condition_id: null, material_group_id: null, capacity_per_unit: null, case_group: null, case_lot: null },
  newCondition: null, newMaterial: null, supplierCode: '0001', actor: 'naka@test', requestId: `reg-${code}`, ...over });
await ta('[R1] 新商品の登録の先方品番: 同じ取引で対応表 (記録 via master-edit:new)・先方品番だけ = 発注の設定の行は作らない・やり直し = 前の結果・違う先方品番 = request_id_reused', () => {
  const r = OS.writeRegistrationOrderSettings(regIn('reg-1', { vendor: { vendor_code: 'RG-1', qty_per_unit: '24' } }));
  assert.deepEqual([r.ok, r.changed, r.replayed, r.vendor.written, r.vendor.supplier_code, r.vendor.qty_per_unit, r.vendor.warning], [true, true, false, true, '1', 24, null]);
  assert.equal(db.prepare("SELECT 1 FROM po_product_attrs WHERE product_key = 'reg-1'").get(), undefined, '先方品番だけの登録で発注の設定の行を作った');
  assert.deepEqual([row('1', 'reg-1').vendor_code, row('1', 'reg-1').qty_per_unit], ['RG-1', 24]);
  const au = auditOf('1').at(-1);
  assert.deepEqual([au.action, au.request_id, au.detail.via, au.detail.product_key], ['vendor_map_entry_upsert', 'reg-reg-1', 'master-edit:new', 'reg-1']);
  // 届けた後に人が直した値を、やり直しで戻さない
  up({ productCode: 'reg-1', prod: prodA, baseUpdatedAt: row('1', 'reg-1').updated_at, vendorCode: 'RG-1-FIX' });
  const again = OS.writeRegistrationOrderSettings(regIn('reg-1', { vendor: { vendor_code: 'RG-1', qty_per_unit: '24' } }));
  assert.deepEqual([again.replayed, row('1', 'reg-1').vendor_code], [true, 'RG-1-FIX']);
  assert.throws(() => OS.writeRegistrationOrderSettings(regIn('reg-1', { vendor: { vendor_code: 'RG-OTHER', qty_per_unit: '24' } })), (e) => e.reason === 'request_id_reused');
  // 発注の設定と先方品番の両方 = 1 つの取引で両方
  const both = OS.writeRegistrationOrderSettings(regIn('reg-2', { patch: { order_lot: '6' }, vendor: { vendor_code: 'RG-2', qty_per_unit: null } }));
  assert.deepEqual([both.changed, db.prepare("SELECT order_lot FROM po_product_attrs WHERE product_key = 'reg-2'").get().order_lot, row('1', 'reg-2').qty_per_unit], [true, 6, null]);
});

await ta('[R2] 先方品番が無い = 今までと同じハッシュ (v1 のやり直しが変わらない)', () => {
  const inp = regIn('reg-v1', { patch: { order_lot: '3' } });
  OS.writeRegistrationOrderSettings(inp);
  const want = createHash('sha256').update(ledger.stableStringify({ key: 'reg-v1', patch: inp.patch, newCondition: null, newMaterial: null, supplier: '1' })).digest('hex');
  assert.equal(db.prepare("SELECT payload_hash FROM po_order_settings_requests WHERE request_id = 'reg-reg-v1'").get().payload_hash, want);
});

await ta('[R3] 行がある・記録がある (足して消した / CSV) = 409 stale (発注の設定も書かない・やり直しの記録も無い)・代表の仕入先が無い / 発注アプリに無い = 400', () => {
  const none = (code) => [db.prepare('SELECT 1 FROM po_product_attrs WHERE product_key = ?').get(code), db.prepare('SELECT 1 FROM po_order_settings_requests WHERE request_id = ?').get(`reg-${code}`)];
  // 行がある (ほかの画面が先に作った)
  up({ productCode: 'reg-3', baseUpdatedAt: null, vendorCode: 'HUMAN-3' });
  assert.throws(() => OS.writeRegistrationOrderSettings(regIn('reg-3', { patch: { order_lot: '2' }, vendor: { vendor_code: 'HUMAN-3', qty_per_unit: null } })), (e) => e.status === 409 && e.reason === 'stale' && e.extra.field === 'vendor_code');
  assert.deepEqual(none('reg-3'), [undefined, undefined], '断ったのに発注の設定かやり直しの記録を残した');
  // 足して消した (行は無いが記録がある) = 戻さない
  up({ productCode: 'reg-4', baseUpdatedAt: null, vendorCode: 'GONE-4' });
  VC.deleteVendorCode({ supplierCode: '1', productCode: 'reg-4', base: row('1', 'reg-4').updated_at, actor: 'naka@test', via: 'po-admin' });
  assert.throws(() => OS.writeRegistrationOrderSettings(regIn('reg-4', { vendor: { vendor_code: 'GONE-4', qty_per_unit: null } })), (e) => e.reason === 'stale');
  assert.equal(row('1', 'reg-4'), null, '消した先方品番を戻した');
  // CSV で足して消した (changed_keys) も同じ
  assert.throws(() => OS.writeRegistrationOrderSettings(regIn('csv-new', { vendor: { vendor_code: 'CSV-1', qty_per_unit: null } })), (e) => e.reason === 'stale');
  assert.throws(() => OS.writeRegistrationOrderSettings(regIn('reg-5', { supplierCode: null, vendor: { vendor_code: 'X', qty_per_unit: null } })), (e) => e.status === 400 && e.extra.field === 'vendor_code');
  assert.throws(() => OS.writeRegistrationOrderSettings(regIn('reg-5', { supplierCode: '0077', vendor: { vendor_code: 'X', qty_per_unit: null } })), (e) => e.status === 400 && /発注アプリにありません/.test(e.message));
  assert.throws(() => OS.writeRegistrationOrderSettings(regIn('reg-5', { vendor: { vendor_code: '', qty_per_unit: '3' } })), (e) => e.status === 400 && e.extra.field === 'vendor_code');
  assert.deepEqual([row('1', 'reg-5'), ...none('reg-5')], [null, undefined, undefined]);
});

await ta('[R4] 仮登録 (出荷明細で見た同じ番号) = 結ぶ候補の id (自動では結ばない)', () => {
  db.prepare(`INSERT INTO po_vendor_code_pending (supplier_code, vendor_code, vendor_code_norm, vendor_name, status, created_at, updated_at) VALUES ('1', 'pend-6', 'PEND-6', '先方の名前', 'pending', ?, ?)`).run(T0, T0);
  const id = db.prepare("SELECT id FROM po_vendor_code_pending WHERE vendor_code_norm = 'PEND-6'").get().id;
  const r = OS.writeRegistrationOrderSettings(regIn('reg-6', { vendor: { vendor_code: 'Pend-6', qty_per_unit: null } }));
  assert.deepEqual(r.vendor.pending_ids, [id]);
  assert.equal(db.prepare('SELECT status FROM po_vendor_code_pending WHERE id = ?').get(id).status, 'pending', '自動で結んだ');
});

await ta('[S1] 画面に出す値: この商品の全部の仕入先の行・仮登録の候補 (同じ商品の仮コード / 同じ番号)・仕入先の名前とリードタイム / 登録の前の確かめ', () => {
  db.prepare("INSERT INTO po_vendor_code_map (supplier_code, product_key, product_code, vendor_code, qty_per_unit, updated_at) VALUES ('2', 'vc-a', 'vc-a', 'BF-9', 0.5, ?)").run(T0);
  db.prepare(`INSERT INTO po_vendor_code_pending (supplier_code, vendor_code, vendor_code_norm, provisional_product_code, status, created_at, updated_at) VALUES ('2', 'p-x', 'P-X', 'VC-A', 'pending', ?, ?)`).run(T0, T0);
  db.prepare(`INSERT INTO po_vendor_code_pending (supplier_code, vendor_code, vendor_code_norm, status, created_at, updated_at) VALUES ('1', 'ac-3', 'AC-3', 'pending', ?, ?)`).run(T0, T0);
  db.prepare(`INSERT INTO po_vendor_code_pending (supplier_code, vendor_code, vendor_code_norm, status, created_at, updated_at) VALUES ('2', 'ac-3', 'AC-3', 'pending', ?, ?)`).run(T0, T0);
  const s = VC.readVendorCodesForProduct('VC-A', { extraSuppliers: ['0001'] });
  assert.equal(s.ok, true);
  assert.deepEqual(s.rows.map((r) => [r.supplier_code, r.vendor_code, r.qty_per_unit]), [['1', 'AC-3', 12], ['2', 'BF-9', 0.5]]);
  assert.deepEqual(s.suppliers['1'], { name: 'アメージングクラフト', lead_days: 14, known: true });
  assert.deepEqual(s.pending.map((p) => [p.supplier_code, p.vendor_code]).sort(), [['1', 'ac-3'], ['2', 'p-x']], 'ほかの仕入先の同じ番号を候補にした / 仮コードの候補が無い');
  assert.doesNotThrow(() => VC.checkRegistrationVendor({ supplierCode: '0001', code: 'brand-new' }));
  assert.throws(() => VC.checkRegistrationVendor({ supplierCode: '0001', code: 'vc-a' }), (e) => e.status === 409);
  assert.throws(() => VC.checkRegistrationVendor({ supplierCode: '', code: 'x' }), (e) => e.status === 400);
  assert.throws(() => VC.checkRegistrationVendor({ supplierCode: '0099', code: 'x' }), (e) => e.status === 400);
  // 行は無いが記録がある (足して消した) = 登録の前に 409 (Codex R1 M2)
  assert.throws(() => VC.checkRegistrationVendor({ supplierCode: '0001', code: 'reg-4' }), (e) => e.status === 409 && e.reason === 'stale');
});

server.close();
console.log(`\n${passed} 件 ok`);
if (process.exitCode) console.log('NG があります');
