/**
 * test-lz-csv-export.mjs — ロジザード登録の CSV (logizard_bc_upload.csv・0081・lib/lz-new-export.mjs・CompanyDB構想 23 v4 §5')
 *
 * Company DB = PGlite (scripts/fixtures/master-variation-db.mjs = 切替の後 new_open・新商品の登録が開いている形)。本物の router を HTTP 越しに通す。
 * 固定する契約:
 *   X1 候補: ポータルで登録した単品で、ロジザードにまだ無い・まだ出していない / 止まる (値なし・仕入先なし・ロジザードでほかの商品の値・大文字小文字だけ違う ID)・
 *      もうある = 出さない・E 列 = 有効期限 02 / 01・F 列 = 入荷日 1 / 0 (入力なし = 0)
 *   X2 CSV を出す: 8 列の形 (Shift_JIS・CRLF・最後に改行なし)・帳面 (ハッシュ・行)・押し直し = 前の答え・同じ商品は 2 回出さない・
 *      出した後は値を直せない (DB の書く関数 = lz_issued・商品の画面の API = 409)
 *   X3 照らせていない値 (保存のときに写しが読めない・空・古い) = 写しが照らせない間は止まる・新しい写しで照らし直せたら出せる (帳面に rechecked)・
 *      DB の関数を直に呼んでも、写しの時刻が無い / 古いと断る (lz_unchecked)
 *   X4 ops.lzbc_value_clash で重なる値 (取り込みでほかの商品の JAN と重なった) = 止まる (理由の一言)・DB の関数も断る (lzbc_taken)
 *   X5 画面を開いた後に値・有効期限が変わった = 409 changed・写しが無い = 409・古い = 確かめの印が要る・DB の関数も直接の偽りを断る
 *   X6 取り消す (理由が要る) = 品目が voided・また候補に出る・値を直せる・ファイルは出さない (409)・もう一度 = already
 *   X7 画面: ロジザード登録の CSV (作れる・止まっている・作った CSV・写しの時刻・予備の GAS)・作る API (名簿の人だけ)・ダウンロード (Shift_JIS)・取り消し・左の列に入口・つかいかた
 *   X8 (#1721 R1 High 1) CSV の中身は DB が品目から作る = アプリが同じ品目から作る byte 列と 1 byte も違わない (引用符・カンマ・半角カナ・～)・byte 列は受け取らない (前の 6 つの引数の関数は無い)
 *   X9 (#1721 R1 High 2) 保存のときに照らせた値も、出すときの写しが古い・空・読めない間は止まる・DB も写しの時刻が無い / 古いと断る
 *   X10 (#1721 R1 Medium) 同じ request_id で違う品目 = 409 request_id_reused (アプリ・DB の関数とも)
 *   X11 (#1721 R2 High) DB の関数を直に: 新商品の登録から来ていない商品 (origin が new_entry でない)・商品の一覧の写しの時刻が無い / 古い / 先 = 断る
 *   X12 (#1721 R2 Medium) 候補が 1 回の上限を超える = 先の N 件だけ (商品コードの順)・残りは次の回 (画面に「残り N 件は次の回」)
 *   X13 (#1721 R2 Low) 大文字と小文字のコードがまざる = DB とアプリが同じ並び (コードの文字の並び) = 同じ byte 列
 *   S1 守り: master_edit は帳面に直接書けない・持ち主も関数の外では書けない・取り消す以外は変えない・TRUNCATE できない・PUBLIC の実行権なし
 * 使い方: node scripts/test-lz-csv-export.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import express from 'express';
import iconv from 'iconv-lite';

const killer = setTimeout(() => { console.error('時間切れ (420 秒)'); process.exit(3); }, 420e3);
killer.unref();
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);
process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'lzbarcode2-csv-'));

const { setupVariationDb, jan13 } = await import('./fixtures/master-variation-db.mjs');
const RB = await import('../lib/master-register-blanks.mjs');
const LB = await import('../lib/logizard-barcode.mjs');
const LZE = await import('../lib/lz-new-export.mjs');
const T = await setupVariationDb();
const { pg, db, q, one, asEditor } = T;
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const { initPurchaseOrders } = await import('../apps/purchase-orders/db.js');
initPurchaseOrders();

process.env.COMPANY_DB_URL = 'postgres://owner@localhost/lzcsv';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost/lzcsv';
process.env.MASTER_EDITORS = 'naka@test';
process.env.MASTER_EDIT_OPEN = '1';
process.env.MASTER_DECISION_APPROVERS = 'naka@test';
const MR = await import('../apps/master-edit/router.mjs');
let chain = Promise.resolve();
MR.__setPgClientFactory(async (url) => {
  let release; const prev = chain; chain = new Promise((r) => { release = r; }); await prev;
  await pg.query(`set role ${/master_edit@/.test(url) ? 'master_edit' : 'deploy'}`);
  return { query: (t, p) => pg.query(t, p), end: async () => { await pg.query('set role deploy'); release(); }, on: () => {} };
});
MR.__setShippingRatesProvider(async () => new Map([['B2', { method: '宅急便コンパクト', cost: 520 }]]));
MR.__setPhDraftLookup(async () => null);
/** ロジザードの写し (試験): keys = 商品の一覧 (小文字)・bc = バーコードマスタ [商品ID (書いたとおり), 値]・state = バーコードマスタ checked / unavailable / empty / stale・list = 一覧の時刻 */
const mirrorOf = ({ keys = [], bc = [], state = 'checked', ok = true, list = new Date().toISOString() } = {}) => {
  const rows = state === 'unavailable' || state === 'empty' ? [] : bc;
  const at = state === 'stale' ? new Date(Date.now() - 40 * 3600e3).toISOString() : new Date().toISOString();
  return { ok, reason: ok ? null : '写しがありません', keys: new Set([...keys, ...rows.map(([id]) => id)].map((k) => k.toLowerCase())),
    ids: rows.reduce((m, [id]) => { const l = id.toLowerCase(); if (!m.has(l)) m.set(l, new Set()); m.get(l).add(id); return m; }, new Map()),
    barcodes: rows.reduce((m, [id, v]) => m.set(v, new Set([...(m.get(v) || []), id])), new Map()), list_as_of: list, bc_as_of: rows.length ? at : null, rows: Math.max(1, keys.length),
    bc: { ok: rows.length > 0 || state === 'checked', reason: null, rows: Math.max(1, rows.length), empty: state === 'empty' } };
};
let MIRROR = mirrorOf({ bc: [['zz-old', 'X00LZXWARE']] });
MR.__setLzMirrorProvider(async () => MIRROR);
const SESS = { editor: { authenticated: true, email: 'naka@test', displayName: '中原', role: 'user', allowedApps: ['master-edit', 'purchase-orders'] } };
const app = express();
app.set('view engine', 'ejs');
app.use((req, res, next) => { req.session = SESS.editor; next(); });
app.use('/apps/master-edit', MR.default);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const ORIGIN = `http://127.0.0.1:${server.address().port}`;
async function call(method, url, body) {
  const r = await fetch(ORIGIN + '/apps/master-edit' + url, { method, headers: { 'Content-Type': 'application/json', Accept: 'application/json', Origin: ORIGIN }, body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(60e3) });
  const text = await r.text();
  let j = null; try { j = JSON.parse(text); } catch { /* */ }
  return { status: r.status, j, text, headers: r.headers };
}
const VALUES = { standard_price: '3980', cost: { jpy: '1650' }, tax_rate: '0.1', sales_class: '3', primary_supplier: '0001', reorder_months: '2', expiry_managed: '0', inbound_date_managed: '0', shipping_code: 'B2' };
async function regApi(code, lz, values = {}, blanksDrop = []) {
  const v = { ...VALUES, name: `試し ${code}`, ...values };
  for (const k of Object.keys(v)) if (v[k] === undefined) delete v[k];
  const b = { request_id: crypto.randomUUID(), kind: 'single', code, values: v, card: { create: false }, ...(lz === undefined ? {} : { logizard_barcode: lz }) };
  b.blanks = RB.missingBlankKeys('single', b, { orderApplicable: true }).filter((k) => !blanksDrop.includes(k));
  return call('POST', '/api/new', b);
}
const skuId = async (code) => (await one('select sku_id::text as id from core.skus where code = $1', [code])).id;
const latest = async (id) => (await one('select max(rule_row)::text as r from ops.sku_logizard_barcode_rules where sku_id = $1', [id])).r;
const cand = async (m = MIRROR) => asEditor(() => LZE.lzNewCandidates(db, m, Date.now()));
const issue = (want, o = {}) => asEditor(() => LZE.issueLzNewExport(db, { requestId: o.requestId ?? crypto.randomUUID(), actor: 'naka@test', want, mirror: o.mirror ?? MIRROR }));
const pgErr = async (p) => { try { await p; } catch (e) { return e; } return null; };
const mine = (list) => list.filter((x) => /^lzx-/.test(x.code));

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
console.log('ロジザード登録の CSV');

await ta('[X1] 候補: ポータルで登録した単品・ロジザードにまだ無い・まだ出していない / 止まる (値なし・ロジザードでほかの商品の値・大文字小文字だけ違う ID)・もうある = 出さない・E 列 / F 列', async () => {
  for (const [code, lz, v] of [['lzx-a', { kind: 'value', value: 'x00lzx0001' }, { expiry_managed: '1', inbound_date_managed: '1' }], ['lzx-b', { kind: 'lz_assign' }, { inbound_date_managed: undefined }],
    ['lzx-c', { kind: 'value', value: '4912345678904' }, { cost: { jpy: '450' } }], ['lzx-d', { kind: 'lz_assign' }, {}], ['lzx-e', { kind: 'lz_assign' }, {}]]) {
    const r = await regApi(code, lz, v);
    assert.equal(r.status, 200, `${code}: ${r.text}`);
  }
  // lzx-b2 = 値なし (lib で登録 = 送らない)
  const R = await import('../lib/master-register.mjs');
  await asEditor(() => R.registerNewSku(db, { actor: 'naka@test', requestId: crypto.randomUUID(), kind: 'single', code: 'lzx-b2', values: { ...VALUES, name: '試し lzx-b2', cost: { jpy: '1650' } }, card: { create: false } },
    { ownership: T.ALL_COMPANY, open: true, shippingRates: new Map([['B2', { method: '宅急便コンパクト', cost: 520 }]]) }));
  MIRROR = mirrorOf({ keys: ['lzx-d'], bc: [['zz-old', 'X00LZXWARE'], ['LZX-E', '4900000000016']] });
  const c = await cand();
  assert.deepEqual(mine(c.ready).map((x) => x.code), ['lzx-a', 'lzx-b', 'lzx-c']);
  assert.deepEqual(Object.fromEntries(mine(c.held).map((x) => [x.code, x.holds])), { 'lzx-b2': ['no_barcode'], 'lzx-e': ['case_collision'] });
  assert.ok(c.in_lz >= 1);
  const a = c.ready.find((x) => x.code === 'lzx-a'), b = c.ready.find((x) => x.code === 'lzx-b');
  assert.deepEqual([a.kind, a.value, a.expiry_code, a.inbound_code, a.supplier], ['fnsku', 'X00LZX0001', '02', '1', '0001']);
  assert.deepEqual([b.kind, b.value, b.expiry_code, b.inbound_code, b.inbound_unknown], ['lz_assign', null, '01', '0', true], '入荷日の管理が入力なし = 0 (Q1 a)');
  // ロジザードのバーコードマスタでほかの商品の値 = 止まる
  const m2 = mirrorOf({ keys: ['lzx-d'], bc: [['zz-old', 'X00LZXWARE'], ['LZX-E', '4900000000016'], ['zz-other', '4912345678904']] });
  const held = (await cand(m2)).held.find((x) => x.code === 'lzx-c');
  assert.deepEqual(held.holds, ['lz_taken']);
  assert.match(held.hold_notes.lz_taken, /zz-other/);
});
await ta('[X9] 保存のときに照らせた値も、出すときの写しが古い・空・読めない間は止まる (#1721 R1 High 2)・空欄は止まらない・DB も写しの時刻が無い / 古いと断る', async () => {
  for (const st of ['stale', 'empty', 'unavailable']) {
    const c = await cand(mirrorOf({ keys: ['lzx-d'], state: st, bc: [['zz-old', 'X00LZXWARE'], ['LZX-E', '4900000000016']] }));
    const a = c.held.find((x) => x.code === 'lzx-a'), b = c.ready.find((x) => x.code === 'lzx-b');
    assert.deepEqual(a && a.holds, ['lz_unchecked'], `${st}: 照らせた値が止まらない`);
    assert.ok(b, `${st}: 空欄 (lzx-b) が止まった`);
  }
  const a = (await cand()).ready.find((x) => x.code === 'lzx-a');
  assert.equal(a.rechecked, false, '保存のときに照らせた値を照らし直したことにした');
  const items = [{ sku_id: a.sku_id, code: 'lzx-a', rule_row: String(a.rule_row), kind: 'fnsku', value: 'X00LZX0001', expiry_code: '02', inbound_code: '1' }];
  for (const at of [null, new Date(Date.now() - 40 * 3600e3).toISOString()]) {
    const e = await pgErr(asEditor(async () => { await pg.query('begin'); try { return await pg.query('select ops.lz_new_export_issue($1::uuid, $2, $3::jsonb, now(), $4::timestamptz)', [crypto.randomUUID(), 'naka@test', JSON.stringify(items), at]); } finally { await pg.query('rollback'); } }));
    assert.match(String(e && e.message), /^lz_unchecked: lzx-a/, String(at));
  }
});
let EXPORT1 = null;
await ta('[X2] CSV を出す: 8 列 (Shift_JIS・CRLF・E 列 / F 列 / H 列)・帳面・押し直し = 前の答え・同じ商品は 2 回出さない・出した後は値を直せない (DB = lz_issued・商品の画面の API = 409)', async () => {
  const c = await cand();
  const want = mine(c.ready).map((x) => ({ sku_id: x.sku_id, rule_row: x.rule_row }));
  const rid = crypto.randomUUID();
  const r = await issue(want, { requestId: rid });
  assert.equal(r.rows, 3);
  EXPORT1 = r.export_id;
  const f = await asEditor(() => LZE.lzNewExportFile(db, r.export_id));
  const text = iconv.decode(f.file_bytes, 'cp932');
  const lines = text.split('\r\n');
  assert.equal(lines[0], '商品ID,商品名,検索名称,仕入単価,有効期限区分,入荷日管理フラグ,取引先コード,バーコード');
  assert.deepEqual(lines.slice(1), ['lzx-a,試し lzx-a,試し lzx-a,1650,02,1,0001,X00LZX0001', 'lzx-b,試し lzx-b,試し lzx-b,1650,01,0,0001,', 'lzx-c,試し lzx-c,試し lzx-c,450,01,0,0001,4912345678904']);
  assert.ok(!text.endsWith('\r\n'), '最後の行に改行がある (GAS と違う)');
  assert.equal(f.file_sha256, crypto.createHash('sha256').update(f.file_bytes).digest('hex'));
  assert.equal((await issue(want, { requestId: rid })).replayed, true);
  // [X10] 同じ request_id で違う品目 (#1721 R1 Medium) = 409 request_id_reused (アプリ)・DB の関数も
  const eR = await pgErr(issue(want.slice(0, 2), { requestId: rid }));
  assert.deepEqual([eR && eR.status, eR && eR.reason], [409, 'request_id_reused']);
  const eD = await pgErr(asEditor(async () => { await pg.query('begin'); try { return await pg.query('select ops.lz_new_export_issue($1::uuid, $2, $3::jsonb, now(), now())', [rid, 'naka@test', JSON.stringify([{ ...c.ready.find((x) => x.code === 'lzx-b'), sku_id: c.ready.find((x) => x.code === 'lzx-b').sku_id, rule_row: String(c.ready.find((x) => x.code === 'lzx-b').rule_row) }])]); } finally { await pg.query('rollback'); } }));
  assert.match(String(eD && eD.message), /^request_id_reused/);
  const items = await q('select code, kind, value, expiry_code, inbound_code, rechecked, state from ops.lz_new_export_items where export_id = $1 order by code', [r.export_id]);
  assert.deepEqual(items.map((x) => [x.code, x.kind, x.value, x.expiry_code, x.inbound_code, x.rechecked, x.state]),
    [['lzx-a', 'fnsku', 'X00LZX0001', '02', '1', false, 'issued'], ['lzx-b', 'lz_assign', null, '01', '0', false, 'issued'], ['lzx-c', 'jan', '4912345678904', '01', '0', false, 'issued']]);
  assert.deepEqual(mine((await cand()).ready), [], '出した商品がまた候補に出た');
  const e = await pgErr(issue(want));
  assert.deepEqual([e && e.status, e && e.reason], [409, 'changed']);
  // 出した後は値を直せない: DB の書く関数 (SKU の鍵の下) と商品の画面の API
  const sa = await skuId('lzx-a');
  const e2 = await pgErr(asEditor(async () => { await pg.query('begin'); try { return await LB.setLzBarcodeInTx(db, { requestId: crypto.randomUUID(), actor: 'naka@test', skuId: sa, kind: 'lz_assign', via: 'sku_page', seenRow: await latest(sa) }); } finally { await pg.query('rollback'); } }));
  assert.deepEqual([e2 && e2.status, e2 && e2.reason], [409, 'lz_issued']);
  const w = await call('POST', '/api/sku/lzx-a/logizard-barcode', { request_id: crypto.randomUUID(), barcode: { kind: 'lz_assign' }, seen_row: await latest(sa) });
  assert.deepEqual([w.status, w.j && w.j.reason], [409, 'lz_issued'], w.text);
  assert.match(w.j.error, /ロジザード登録の CSV \(ファイル #\d+\) に出したので直せない/);
});
await ta('[X3] 照らせていない値: 写しが照らせない間は止まる (lz_unchecked)・新しい写しで照らし直せたら出せる (帳面に rechecked)・DB の関数も写しの時刻が無い / 古いと断る', async () => {
  MIRROR = mirrorOf({ state: 'stale', bc: [['zz-old', 'X00LZXWARE']] });
  assert.equal((await regApi('lzx-u', { kind: 'value', value: 'X00LZXU001' })).status, 200);
  assert.equal((await one("select r.lz_copy_state as s from ops.sku_logizard_barcode_rules r join core.skus k on k.sku_id = r.sku_id where k.code = 'lzx-u'")).s, 'stale');
  for (const st of ['stale', 'unavailable', 'empty']) {
    const h = (await cand(mirrorOf({ state: st, bc: [['zz-old', 'X00LZXWARE']] }))).held.find((x) => x.code === 'lzx-u');
    assert.deepEqual(h && h.holds, ['lz_unchecked'], st);
    assert.match(h.hold_notes.lz_unchecked, /ロジザードの写しと照らせていません/);
  }
  // DB の関数を直に (写しの時刻が無い / 40 時間前) = 断る
  const u = (await cand(mirrorOf({ bc: [['zz-old', 'X00LZXWARE']] }))).ready.find((x) => x.code === 'lzx-u');
  assert.ok(u && u.rechecked, '新しい写しで照らし直せない');
  const items = [{ sku_id: u.sku_id, code: 'lzx-u', rule_row: String(u.rule_row), kind: 'fnsku', value: 'X00LZXU001', expiry_code: '01', inbound_code: '0' }];
  for (const at of [null, new Date(Date.now() - 40 * 3600e3).toISOString()]) {
    const e = await pgErr(asEditor(async () => { await pg.query('begin'); try { return await pg.query('select ops.lz_new_export_issue($1::uuid, $2, $3::jsonb, now(), $4::timestamptz)', [crypto.randomUUID(), 'naka@test', JSON.stringify(items), at]); } finally { await pg.query('rollback'); } }));
    assert.match(String(e && e.message), /^lz_unchecked: lzx-u/, String(at));
  }
  MIRROR = mirrorOf({ bc: [['zz-old', 'X00LZXWARE']] });
  const r = await issue([{ sku_id: u.sku_id, rule_row: u.rule_row }]);
  assert.equal(r.rows, 1);
  const it = await one('select rechecked from ops.lz_new_export_items where export_id = $1', [r.export_id]);
  assert.equal(it.rechecked, true);
  assert.equal((await one('select lz_bc_as_of is not null as ok from ops.lz_new_exports where export_id = $1', [r.export_id])).ok, true);
});
await ta('[X4] Company DB で重なる値 (ops.lzbc_value_clash・取り込みでほかの商品の JAN と重なった) = 止まる (一言)・DB の関数も断る (lzbc_taken)', async () => {
  const J = jan13('458777700501');
  // s003 に JAN J を付ける (人の JAN の保存)
  const s3 = await skuId('s003');
  const jans = (await q(`select e.external_value as v from core.external_ids e join core.skus s on s.product_id = e.entity_id and e.entity_type = 'product' where s.sku_id = $1 and e.system = 'jan' and e.valid_to is null`, [s3])).map((x) => x.v);
  await asEditor(async () => { await pg.query('begin'); try { await pg.query('select ops.edit_sku_jan($1::uuid, $2, $3, $4::jsonb, $5::bigint, $6::jsonb, $7::jsonb)', [crypto.randomUUID(), 'naka@test', '試験', T.OWN, s3, JSON.stringify(jans), JSON.stringify([...jans, J])]); await pg.query('commit'); } catch (e) { await pg.query('rollback'); throw e; } });
  // lzx-k の値が J (取り込みで重なった前の状態 = 持ち主が印を立てて直に書く)
  assert.equal((await regApi('lzx-k', { kind: 'lz_assign' })).status, 200);
  const sk = await skuId('lzx-k');
  await pg.query('reset role');
  try {
    await pg.query("set ops.logizard_barcode_protocol = '1'");
    await pg.query("insert into ops.sku_logizard_barcode_rules (sku_id, rule, value, via, actor_id, request_id, db_user, lz_copy_state, lz_copy_as_of) values ($1, 'jan', $2, 'sku_page', 'naka@test', $3, 'master_edit', 'checked', now())", [sk, J, crypto.randomUUID()]);
    await pg.query("set ops.logizard_barcode_protocol = ''");
  } finally { await pg.query('set role deploy'); }
  const h = (await cand()).held.find((x) => x.code === 'lzx-k');
  assert.deepEqual(h && h.holds, ['clash']);
  assert.match(h.hold_notes.clash, /ほかの商品 \(s003\) の JAN と同じです/);
  const items = [{ sku_id: sk, code: 'lzx-k', rule_row: await latest(sk), kind: 'jan', value: J, expiry_code: '01', inbound_code: '0' }];
  const e = await pgErr(asEditor(async () => { await pg.query('begin'); try { return await pg.query('select ops.lz_new_export_issue($1::uuid, $2, $3::jsonb, now(), now())', [crypto.randomUUID(), 'naka@test', JSON.stringify(items)]); } finally { await pg.query('rollback'); } }));
  assert.match(String(e && e.message), /^lzbc_taken: lzx-k .*s003.*JAN/);
});
await ta('[X5] 画面を開いた後に値・有効期限が変わった = 409 changed (何も書かない)・写しが無い = 409・古い = 作れない (#1721 R2・確かめの印は無い)・DB の関数も直接の偽りを断る', async () => {
  for (const c of ['lzx-f', 'lzx-g']) assert.equal((await regApi(c, { kind: 'lz_assign' })).status, 200);
  const c = await cand();
  const f = c.ready.find((x) => x.code === 'lzx-f'), g = c.ready.find((x) => x.code === 'lzx-g');
  await asEditor(async () => { await pg.query('begin'); try { await LB.setLzBarcodeInTx(db, { requestId: crypto.randomUUID(), actor: 'naka@test', skuId: f.sku_id, kind: 'jan', value: '4512345678906', via: 'sku_page', seenRow: f.rule_row, copy: { state: 'checked', as_of: new Date().toISOString() } }); await pg.query('commit'); } catch (e) { await pg.query('rollback'); throw e; } });
  const n0 = (await one('select count(*)::int as n from ops.lz_new_exports')).n;
  let e = await pgErr(issue([{ sku_id: f.sku_id, rule_row: f.rule_row }]));
  assert.deepEqual([e && e.status, e && e.reason], [409, 'changed']);
  e = await pgErr(issue([{ sku_id: g.sku_id, rule_row: g.rule_row }], { mirror: mirrorOf({ ok: false }) }));
  assert.deepEqual([e && e.status, e && e.reason], [409, 'lz_list_unavailable']);
  const old = mirrorOf({ bc: [['zz-old', 'X00LZXWARE']], list: new Date(Date.now() - 40 * 3600e3).toISOString() });
  e = await pgErr(issue([{ sku_id: g.sku_id, rule_row: g.rule_row }], { mirror: old }));
  assert.deepEqual([e && e.status, e && e.reason], [409, 'lz_list_stale']);
  assert.equal((await one('select count(*)::int as n from ops.lz_new_exports')).n, n0, '断ったのに帳面に書いた');
  const raw = (items) => asEditor(async () => { await pg.query('begin'); try { return await pg.query('select ops.lz_new_export_issue($1::uuid, $2, $3::jsonb, now(), now())', [crypto.randomUUID(), 'naka@test', JSON.stringify(items)]); } finally { await pg.query('rollback'); } });
  const base = { sku_id: g.sku_id, code: 'lzx-g', rule_row: String(g.rule_row), kind: 'lz_assign', value: null, expiry_code: '01', inbound_code: '0' };
  for (const [x, re] of [[{ expiry_code: '02' }, /^stale/], [{ inbound_code: '1' }, /^stale/], [{ kind: 'jan', value: '4512345678906' }, /^stale/], [{ code: 'LZX-G' }, /^stale/], [{ rule_row: 'x' }, /^invalid_input/]]) {
    const er = await pgErr(raw([{ ...base, ...x }]));
    assert.match(String(er && er.message), re, JSON.stringify(x));
  }
  // 🆕 #1721 R2: 「古いまま作る」は無い (DB も断る)。新しい写しなら作れる
  const r = await issue([{ sku_id: g.sku_id, rule_row: g.rule_row }]);
  assert.equal(r.rows, 1);
});
await ta('[X6] 取り消す (理由が要る) = 品目が voided・また候補に出る・値を直せる・ファイルは出さない (409)・もう一度 = already', async () => {
  let e = await pgErr(asEditor(() => LZE.voidLzNewExport(db, { actor: 'naka@test', exportId: EXPORT1, reason: '' })));
  assert.deepEqual([e && e.status, e && e.reason], [400, 'invalid_input']);
  assert.equal((await asEditor(() => LZE.voidLzNewExport(db, { actor: 'naka@test', exportId: EXPORT1, reason: 'ロジザードで取り込めなかった' }))).ok, true);
  assert.equal((await asEditor(() => LZE.voidLzNewExport(db, { actor: 'naka@test', exportId: EXPORT1, reason: 'もう一度' }))).already, true);
  assert.deepEqual((await q('select distinct state from ops.lz_new_export_items where export_id = $1', [EXPORT1])).map((x) => x.state), ['voided']);
  assert.deepEqual(mine((await cand()).ready).filter((x) => ['lzx-a', 'lzx-b', 'lzx-c'].includes(x.code)).map((x) => x.code), ['lzx-a', 'lzx-b', 'lzx-c']);
  const sa = await skuId('lzx-a');
  const ok = await asEditor(async () => { await pg.query('begin'); try { const r = await LB.setLzBarcodeInTx(db, { requestId: crypto.randomUUID(), actor: 'naka@test', skuId: sa, kind: 'lz_assign', via: 'sku_page', seenRow: await latest(sa) }); await pg.query('commit'); return r; } catch (x) { await pg.query('rollback'); throw x; } });
  assert.equal(ok.changed, true);
  e = await pgErr(asEditor(() => LZE.voidLzNewExport(db, { actor: 'naka@test', exportId: '999999', reason: 'x' })));
  assert.deepEqual([e && e.status], [404]);
});
await ta('[X7] 画面: ロジザード登録の CSV (作れる・止まっている・作った CSV・写しの時刻・予備の GAS)・作る API (名簿の人だけ)・ダウンロード (Shift_JIS)・取り消し・左の列に入口・つかいかた', async () => {
  let r = await call('GET', '/lz-csv');
  assert.equal(r.status, 200, r.text.slice(0, 300));
  for (const w of ['ロジザード登録の CSV', 'id="lzc-ready"', 'lzx-a', 'id="lzc-held"', 'ロジザードのバーコードが入っていません', 'id="lzc-exports"', '取り消した', 'ロジザードの商品の一覧の写し',
    'id="lzc-build"', 'href="/apps/master-edit/lz-csv"', 'id="lzc-gas"', 'me-lzcsv.js', 'data-hold="clash"']) assert.ok(r.text.includes(w), `画面に「${w}」が無い`);
  const data = JSON.parse(/<script type="application\/json" id="me-lzc">([\s\S]*?)<\/script>/.exec(r.text)[1].replace(/\\u003c/g, '<'));
  const items = data.ready.filter((x) => ['lzx-a', 'lzx-b'].includes(x.code)).map((x) => ({ sku_id: x.sku_id, rule_row: x.rule_row }));
  assert.equal(items.length, 2);
  process.env.MASTER_DECISION_APPROVERS = 'other@test';
  assert.equal((await call('POST', '/api/lz-csv/build', { request_id: crypto.randomUUID(), items })).status, 403);
  assert.equal((await call('GET', '/lz-csv/1/file')).status, 403);
  process.env.MASTER_DECISION_APPROVERS = 'naka@test';
  r = await call('POST', '/api/lz-csv/build', { request_id: crypto.randomUUID(), items });
  assert.equal(r.status, 200, r.text);
  const id = r.j.export_id;
  const fr = await fetch(`${ORIGIN}/apps/master-edit/lz-csv/${id}/file`);
  assert.equal(fr.status, 200);
  assert.match(fr.headers.get('content-type'), /Shift_JIS/i);
  assert.match(fr.headers.get('content-disposition'), /logizard_bc_upload\.csv/);
  const bytes = Buffer.from(await fr.arrayBuffer());
  assert.equal(fr.headers.get('x-content-sha256'), crypto.createHash('sha256').update(bytes).digest('hex'));
  assert.equal(iconv.decode(bytes, 'cp932').split('\r\n').length, 3);
  r = await call('POST', `/api/lz-csv/${id}/void`, { reason: '試し' });
  assert.equal(r.status, 200, r.text);
  assert.equal((await fetch(`${ORIGIN}/apps/master-edit/lz-csv/${id}/file`)).status, 409);
  const man = await call('GET', '/manual');
  for (const w of ['ロジザード登録の CSV</a>」の画面で作ります', 'E 列', 'F 列', '予備', 'CSV に出した商品を GAS からも出さない']) assert.ok(man.text.includes(w), `つかいかたに「${w}」が無い`);
  assert.ok(!man.text.includes('ポータルから CSV を出す画面はこの後に足します'), 'つかいかたに前の文が残っている');
});
await ta('[X8] CSV の中身は DB が品目から作る (#1721 R1 High 1): アプリが同じ品目から作る byte 列と同じ (引用符・カンマ・半角カナ・～)・byte 列は受け取らない', async () => {
  const name = 'テスト, "引用" ～ｶﾅ';
  const r0 = await regApi('lzx-q', { kind: 'value', value: '4512345678913' }, { name, expiry_managed: '1', cost: { jpy: '12' } });
  assert.equal(r0.status, 200, r0.text);
  const c = await cand();
  const q1 = c.ready.find((x) => x.code === 'lzx-q');
  assert.ok(q1, JSON.stringify(c.held.filter((x) => x.code === 'lzx-q')));
  const others = mine(c.ready).filter((x) => x.code !== 'lzx-q').slice(0, 2);
  const items = [q1, ...others];
  const appBytes = LZE.buildLzNewCsvBytes(items).bytes;
  const r = await issue(items.map((x) => ({ sku_id: x.sku_id, rule_row: x.rule_row })));
  const f = await asEditor(() => LZE.lzNewExportFile(db, r.export_id));
  assert.equal(Buffer.compare(f.file_bytes, appBytes), 0, `DB の中身とアプリの byte 列が違う\nDB : ${f.file_bytes.toString('hex')}\napp: ${appBytes.toString('hex')}`);
  const text = iconv.decode(f.file_bytes, 'cp932');
  assert.ok(text.includes('lzx-q,"テスト, ""引用"" ～ｶﾅ","テスト, ""引用"" ～ｶﾅ",12,02,0,0001,4512345678913'), text);
  const t = await one('select file_text, text_sha256 from ops.lz_new_exports where export_id = $1', [r.export_id]);
  assert.equal(t.text_sha256, crypto.createHash('sha256').update(t.file_text, 'utf8').digest('hex'));
  // byte 列を渡す前の形 (6 つの引数) は無い = 直に呼んでも好きな中身を残せない
  const e = await pgErr(asEditor(async () => { await pg.query('begin'); try { return await pg.query('select ops.lz_new_export_issue($1::uuid, $2, $3::jsonb, $4::bytea, now(), now())', [crypto.randomUUID(), 'naka@test', '[]', Buffer.from('x')]); } finally { await pg.query('rollback'); } }));
  assert.match(String(e && e.message), /does not exist|存在しません/);
  // 画面のロールは中身を直接書けない
  const code = async (fn) => { const er = await pgErr(fn()); return er ? er.code : null; };
  assert.equal(await asEditor(() => code(() => pg.query("update ops.lz_new_exports set file_text = 'x' where export_id = $1", [r.export_id]))), '42501');
});
await ta('[X11] DB の関数を直に (#1721 R2 High): 新商品の登録から来ていない商品 = 断る・商品の一覧の写しの時刻が無い / 40 時間前 / 1 時間先 = 断る', async () => {
  const H = 3600e3;
  const s1 = await skuId('s001');   // 前からある商品 (backfill)
  await asEditor(async () => { await pg.query('begin'); try { await LB.setLzBarcodeInTx(db, { requestId: crypto.randomUUID(), actor: 'naka@test', skuId: s1, kind: 'lz_assign', via: 'sku_page', seenRow: await latest(s1) }); await pg.query('commit'); } catch (e) { await pg.query('rollback'); throw e; } });
  assert.notEqual((await one('select origin from ops.master_registrations where sku_id = $1', [s1])).origin, 'new_entry');
  const direct = (items, listAt) => pgErr(asEditor(async () => { await pg.query('begin'); try { return await pg.query('select ops.lz_new_export_issue($1::uuid, $2, $3::jsonb, $4::timestamptz, now())', [crypto.randomUUID(), 'naka@test', JSON.stringify(items), listAt]); } finally { await pg.query('rollback'); } }));
  let e = await direct([{ sku_id: s1, code: 's001', rule_row: await latest(s1), kind: 'lz_assign', value: null, expiry_code: '01', inbound_code: '0' }], new Date().toISOString());
  assert.match(String(e && e.message), /^invalid_input: s001 は新商品の登録から来た商品でない/);
  assert.equal((await regApi('lzx-o', { kind: 'lz_assign' })).status, 200);
  const so = await skuId('lzx-o');
  const ok = [{ sku_id: so, code: 'lzx-o', rule_row: await latest(so), kind: 'lz_assign', value: null, expiry_code: '01', inbound_code: '0' }];
  for (const at of [null, new Date(Date.now() - 40 * H).toISOString(), new Date(Date.now() + H).toISOString()]) {
    e = await direct(ok, at);
    assert.match(String(e && e.message), /^lz_list_stale/, String(at));
  }
  assert.equal(await direct(ok, new Date().toISOString()), null, '新しい写しの時刻で断った');
  // アプリも先の時刻は古いと同じ (作れない)
  e = await pgErr(issue([{ sku_id: so, rule_row: await latest(so) }], { mirror: mirrorOf({ bc: [['zz-old', 'X00LZXWARE']], list: new Date(Date.now() + H).toISOString() }) }));
  assert.deepEqual([e && e.status, e && e.reason], [409, 'lz_list_stale']);
});
await ta('[X12] 候補が 1 回の上限を超える (#1721 R2 Medium) = 先の N 件だけ・残りは次の回 (画面に「残り N 件は次の回」)・出すと次の N 件が出る', async () => {
  for (const c of ['lzx-m1', 'lzx-m2', 'lzx-m3']) assert.equal((await regApi(c, { kind: 'lz_assign' })).status, 200);
  const all = mine((await cand()).ready).map((x) => x.code);
  assert.ok(all.length >= 4, all.join(','));
  LZE.__setLzExportMaxRows(2);
  try {
    const c = await cand();
    assert.equal(c.ready.length, 2);
    assert.ok(c.rest >= 2, String(c.rest));
    const page = await call('GET', '/lz-csv');
    assert.ok(page.text.includes(`残り ${c.rest} 件は次の回 (1 回の CSV は 2 件まで)`), '画面に残りの件数が無い');
    const data = JSON.parse(/<script type="application\/json" id="me-lzc">([\s\S]*?)<\/script>/.exec(page.text)[1].replace(/\\u003c/g, '<'));
    assert.equal(data.ready.length, 2);
    const first = c.ready.map((x) => x.code);
    const r = await issue(c.ready.map((x) => ({ sku_id: x.sku_id, rule_row: x.rule_row })));
    assert.equal(r.rows, 2);
    const c2 = await cand();
    assert.equal(c2.ready.length, 2);
    assert.ok(!c2.ready.some((x) => first.includes(x.code)), '出した商品がまた出た');
    // 上限を超える要求 = 400
    const e = await pgErr(issue([...c2.ready, ...c2.ready].map((x, i) => ({ sku_id: x.sku_id, rule_row: x.rule_row, i }))));
    assert.deepEqual([e && e.status], [400]);
  } finally { LZE.__setLzExportMaxRows(null); }
});
await ta('[X13] 大文字と小文字のコードがまざる (#1721 R2 Low): DB とアプリが同じ並び (コードの文字の並び = 大文字が先)・同じ byte 列', async () => {
  for (const c of ['lzx-aa', 'lzx-Zb', 'lzx-B1']) assert.equal((await regApi(c, { kind: 'lz_assign' })).status, 200, c);
  const c = await cand();
  const items = c.ready.filter((x) => ['lzx-aa', 'lzx-Zb', 'lzx-B1'].includes(x.code));
  assert.equal(items.length, 3);
  const appBytes = LZE.buildLzNewCsvBytes(items).bytes;
  const r = await issue(items.map((x) => ({ sku_id: x.sku_id, rule_row: x.rule_row })));
  const f = await asEditor(() => LZE.lzNewExportFile(db, r.export_id));
  assert.equal(Buffer.compare(f.file_bytes, appBytes), 0, 'DB とアプリの並びが違う');
  assert.deepEqual(iconv.decode(f.file_bytes, 'cp932').split('\r\n').slice(1).map((l) => l.split(',')[0]), ['lzx-B1', 'lzx-Zb', 'lzx-aa']);
});
await ta('[S1] 守り: master_edit は帳面に直接書けない・持ち主も関数の外では書けない・取り消す以外は変えない・TRUNCATE できない・PUBLIC の実行権なし・master_edit だけ実行できる', async () => {
  const code = async (fn) => { const e = await pgErr(fn()); return e ? e.code : null; };
  assert.equal(await asEditor(() => code(() => pg.query("update ops.lz_new_exports set void_reason = 'x'"))), '42501');
  assert.equal(await asEditor(() => code(() => pg.query("delete from ops.lz_new_export_items"))), '42501');
  assert.equal(await code(() => pg.query("update ops.lz_new_exports set row_count = row_count")), '42501', '持ち主が関数の外で変えた');
  await pg.query('reset role');
  try {
    await pg.query('begin');
    await pg.query("set local ops.lz_new_export_protocol = '1'");
    assert.equal(await code(() => pg.query("update ops.lz_new_exports set row_count = row_count + 1 where state = 'issued'")), '42501', '中身を変えられた');
    await pg.query('rollback');
  } finally { await pg.query('set role deploy'); }
  assert.ok(await code(() => pg.query('truncate ops.lz_new_export_items')), 'TRUNCATE できた');
  for (const sig of ['ops.lz_new_export_issue(uuid, text, jsonb, timestamp with time zone, timestamp with time zone)', 'ops.lz_new_export_void(text, bigint, text)']) {
    const p = await one('select proacl::text as acl, prosecdef, proconfig::text as cfg from pg_proc where oid = $1::regprocedure', [sig]);
    assert.equal(p.prosecdef, true, sig); assert.match(p.cfg, /search_path=pg_catalog, pg_temp/, sig);
    assert.ok(!/(^|[{,])=X/.test(p.acl), `PUBLIC に実行権がある: ${sig} ${p.acl}`); assert.match(p.acl, /master_edit=X/, sig);
  }
});

server.close();
console.log(`\n${passed} 件 ok${process.exitCode ? ' (NG あり)' : ''}`);
process.exit(process.exitCode || 0);
