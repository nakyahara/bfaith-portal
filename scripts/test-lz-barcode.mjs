/**
 * test-lz-barcode.mjs — ロジザードに送るバーコードの「値」(CompanyDB構想 23 v4・0080・lib/logizard-barcode.mjs・lib/lz-mirror.mjs)
 *
 * Company DB = PGlite (持ち主のロール deploy で migration・夜間ロードで商品・出品・FNSKU・JAN を入れる・画面は master_edit のロール)。本物の router を HTTP 越しに通す。
 * (新商品の登録の API の値の確かめは scripts/test-master-register.mjs の [R-LZ]・[LZ-H*])
 * 固定する契約:
 *   U 値の形: FNSKU = X00 + 英数字 7 字 (大文字にそろえる・全角も)・JAN = 8 / 13 けた + チェック数字・入れ方 { kind: value | lz_assign } だけ・両方 = 400
 *   C CSV の部品: lz-csv.mjs の新商品のバーコードの列 = この値の決まりの形だけ
 *   F DB の関数 ops.set_sku_logizard_barcode (0080): 書く・押し直し = 前の答え (stale より先)・違う中身 = request_id_reused・stale・今と同じ = 書かない・
 *     ほかの商品の値 = lzbc_taken (ほかの SKU の今の値・ほかの商品の JAN・ほかの商品を含む出品の FNSKU)・自分の JAN / FNSKU = 通る・
 *     形 (DB でもチェック数字)・空欄に値 = invalid_input・セット = not_single・0078 の決め方だけの行 = 値なし (もう一度入れる)
 *   R 参考: Amazon の FNSKU・この商品の JAN と違う = 参考の一言だけ (断らない)・Amazon のデータが無くても入れられる
 *   S 守り: master_edit は値の表に直接書けない・持ち主も関数の外では書けない・TRUNCATE できない・watcher は書く関数を実行できない・PUBLIC の実行権なし・
 *     0078 の決め方だけの関数は master_edit から外す
 *   H 画面: 単品の商品の画面に「ロジザードのバーコード」(いま・欄・空欄の印・参考)・保存の API (名簿・Origin・形・stale・ロジザードの写しでほかの商品の値 = 409)・
 *     名簿でない = 見るだけ・セットには出さない・JAN の欄の文・Chromium で画面の動き (値を打つと空欄の印が外れる・送り直しは同じ request_id)・つかいかた
 * 使い方: node scripts/test-lz-barcode.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import vm from 'node:vm';
import iconv from 'iconv-lite';
import express from 'express';

const killer = setTimeout(() => { console.error('時間切れ (420 秒)'); process.exit(3); }, 420e3);
killer.unref();

const W2 = await import('./fixtures/master-widen-pr1.mjs');
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);

const DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'lzbarcode2-'));
process.env.DATA_DIR = DATA_DIR;
delete process.env.MASTER_EDIT_OPEN;

const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { createRoles } = await import('./company-db/create-watch-roles.mjs');
const { createMasterEditRoles } = await import('./company-db/create-master-edit-roles.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries((await import('../config/master-ownership.mjs')).OWNED_COLUMNS.map((k) => [k, 'load'])));
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
(await import('../lib/master-legacy-gate.mjs')).__setLegacyPhaseReader(async () => ({ readable: true, phase: 'legacy_open' }));
const LB = await import('../lib/logizard-barcode.mjs');
const LZ = await import('../apps/master-decisions/lz-csv.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};
const uuid = () => crypto.randomUUID();
const pgErr = async (p) => { try { await p; } catch (e) { return e; } return null; };

// ─── U 値の形 (純粋) ───
console.log('ロジザードのバーコードの値');
const F1 = 'X00LZB0001', J1 = '4901234567894', J9 = '4900000000009', JBAD = '4901234567890';
await ta('[U1] 値の形: FNSKU = X00 + 英数字 7 字 (大文字・全角をそろえる)・JAN = 8 / 13 けた + チェック数字・それ以外は何が違うかの一言', async () => {
  assert.deepEqual(LB.lzValueShape(' x00lzb0001 '), { kind: 'fnsku', value: F1 });
  assert.deepEqual(LB.lzValueShape('Ｘ００ＬＺＢ０００１'), { kind: 'fnsku', value: F1 });
  assert.deepEqual(LB.lzValueShape(J1), { kind: 'jan', value: J1 });
  assert.deepEqual(LB.lzValueShape('49123456'), { kind: 'jan', value: '49123456' }, 'JAN-8');
  assert.match(LB.lzValueShape(JBAD).problem, /チェック数字/);
  assert.match(LB.lzValueShape('123456789').problem, /8 けたか 13 けたです \(いま 9 けた\)/);
  assert.match(LB.lzValueShape('X0LZB00001').problem, /X00 \+ 英数字 7 字/, 'X0 + 8 字 (Amazon の形) でも X00 でなければ断る');
  assert.match(LB.lzValueShape('B0ABCDEFGH').problem, /FNSKU \(X00/);
  assert.match(LB.lzValueShape('').problem, /値がありません/);
});
await ta('[U2] 入れ方: { kind: value, value } / { kind: lz_assign } だけ・送らない = null・空欄に値 = 400・知らない形 = 400 (欄の名前と子のコードつき)', async () => {
  assert.equal(LB.lzBarcodeIn(undefined), null); assert.equal(LB.lzBarcodeIn(null), null);
  assert.deepEqual(LB.lzBarcodeIn({ kind: 'value', value: 'x00lzb0001' }), { kind: 'fnsku', value: F1 });
  assert.deepEqual(LB.lzBarcodeIn({ kind: 'lz_assign' }), { kind: 'lz_assign', value: null });
  assert.throws(() => LB.lzBarcodeIn({ kind: 'lz_assign', value: J1 }), (e) => e.status === 400 && /両方/.test(e.message));
  for (const bad of ['auto', F1, [], { kind: 'auto' }, { kind: 'value', value: JBAD }, { kind: 'value' }]) assert.throws(() => LB.lzBarcodeIn(bad), (e) => e.status === 400 && e.extra.field === 'logizard_barcode', JSON.stringify(bad));
  assert.throws(() => LB.lzBarcodeIn({ kind: 'value', value: 'zzz' }, { field: 'children', label: 'kid-1' }), (e) => e.extra.field === 'children' && /^kid-1: /.test(e.message));
});

// ─── C CSV の部品 (今の lz-csv.mjs のバーコードの列の形の確かめ = この値の決まり) ───
const item = (code, x = {}) => ({ code_norm: code.toLowerCase(), ne_code: code, name: `商品 ${code}`, cost_src: '100.00', supplier: '1', ...x });
await ta('[C1] 新商品の CSV のバーコードの列: X00 + 7 字の FNSKU・JAN (チェック数字)・空欄 = 書ける / X0 + 8 字・チェック数字の合わない JAN = 例外', async () => {
  const r = LZ.buildLzCsv([item('a1', { barcode: F1 }), item('a2', { barcode: J1 }), item('a3', { barcode: '' })], 'new');
  // 末尾の改行 (CRLF で終わる CSV) は空の行として数えない (#1719 R3 Low 2)
  assert.deepEqual(iconv.decode(r.bytes, 'cp932').replace(/\r\n$/, '').split('\r\n').slice(1).map((l) => l.split(',')[7]), [F1, J1, '']);
  for (const v of ['X0LZB00001', JBAD]) assert.throws(() => LZ.buildLzCsv([item('b1', { barcode: v })], 'new'), /バーコードの形/, v);
});

// ─── Company DB (PGlite・持ち主のロール deploy) ───
const pg = new PGlite();
await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
await pg.query('set role deploy');
const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });
await createMasterEditRoles(pg, {});
await W2.useReal0058(pg);
const q = async (sql, params) => (await db.query(sql, params)).rows;
async function asRole(role, fn) { await pg.query(`set role ${role}`); try { return await fn(); } finally { await pg.query('set role deploy'); } }
const asEditor = (fn) => asRole('master_edit', fn);

const AMZ = 'main@A1VC38T7YXB528';
{
  const sku = (code, kind = 'single') => ({ code, name: `商品 ${code}`, kind, taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 1,
    cost: { jpy: 100, source: kind === 'set' ? 'set_calc' : 'ne', status: 'COMPLETE' }, standardPriceJpy: 1000, shippingCode: 'S02', shippingMethod: '宅急便', shippingCostJpy: 520, reorderMonths: 2 });
  const comp = (code, qty = 1) => ({ code, qty, resolution: 'exact', evidence: { source: 'test' } });
  const amazon = (listingCode, comps, fnsku) => ({ mall: 'amazon', shopCode: AMZ, marketplaceId: 'A1VC38T7YXB528', listingCode, title: listingCode, status: 'active',
    components: comps, asinCandidates: [], fnskuCandidates: fnsku ? [{ fnsku, source: 'fba_sku_attrs' }] : [], evidenceSource: 'test' });
  const jan = (skuCode, v) => ({ skuCode, attribute: 'jan', scope: 'item', valueText: v, source: 'product_hub', sourceRef: `test:${skuCode}:${v}`, observedAt: '2030-01-01T00:00:00.000Z' });
  const plan = {
    skus: ['lz1', 'lz2', 'lz3', 'lz4', 'lz5', 'oth1'].map((x) => sku(x)).concat([sku('set1', 'set')]), variationGroups: [],
    setComponents: [{ parentCode: 'set1', childCode: 'lz1', qty: 2, source: 'ne' }],
    listings: [amazon('pr_lz1', [comp('lz1')], 'X00LZ10001'), amazon('pr_oth1', [comp('oth1')], 'X00OTH0001')],
    observations: [jan('lz1', J1), jan('oth1', J9)], physicals: [], compliance: [], workers: [],
    suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: [], primarySuppliers: [], reorder: { available: true, runId: 'pml_test' },
  };
  const r = await runInitialLoad(db, plan, { log: quiet, runId: 'load_lzb2', ownership: MASTER_OWNERSHIP, now: new Date('2030-01-05T03:00:00Z') });
  assert.equal(r.ok, true, r.error);
}
const sid = async (code) => (await q('select sku_id::text as id from core.skus where code = $1', [code]))[0]?.id;
const ids = Object.fromEntries(await Promise.all(['lz1', 'lz2', 'lz3', 'lz4', 'lz5', 'oth1', 'set1'].map(async (k) => [k, await sid(k)])));
const rowsOf = async (code) => q('select rule, value, via, actor_id, db_user from ops.sku_logizard_barcode_rules where sku_id = $1 order by rule_row', [ids[code]]);
const latestRow = async (code) => (await q('select max(rule_row)::text as r from ops.sku_logizard_barcode_rules where sku_id = $1', [ids[code]]))[0].r;
const setBc = (code, kind, value = null, o = {}) => asEditor(async () => {
  await pg.query('begin');
  try {
    const r = await LB.setLzBarcodeInTx(db, { requestId: o.requestId ?? uuid(), actor: o.actor ?? 'naka@test', skuId: ids[code], kind, value, via: o.via ?? 'sku_page', seenRow: o.seenRow ?? null });
    await pg.query('commit');
    return r;
  } catch (e) { await pg.query('rollback'); throw e; }
});
const read = async (code) => (await asEditor(() => LB.readLogizardBarcodes(db, [ids[code]]))).get(ids[code]);

await ta('[F1] 書く・押し直し = 前の答え (開いた行番号が古くても stale より先)・違う中身 = request_id_reused・stale・今と同じ = 書かない', async () => {
  assert.equal(await asEditor(() => LB.lzBarcodeReady(db)), true);
  const rid = uuid();
  const r1 = await setBc('lz3', 'fnsku', F1, { requestId: rid });
  assert.deepEqual([r1.kind, r1.value, r1.changed], ['fnsku', F1, true]);
  assert.deepEqual(await rowsOf('lz3'), [{ rule: 'fnsku', value: F1, via: 'sku_page', actor_id: 'naka@test', db_user: 'master_edit' }]);
  assert.equal((await setBc('lz3', 'fnsku', F1, { requestId: rid })).replayed, true);
  let e = await pgErr(setBc('lz3', 'jan', J1, { requestId: rid }));
  assert.equal(e && e.reason, 'request_id_reused');
  e = await pgErr(setBc('lz3', 'lz_assign', null, { seenRow: null }));
  assert.equal(e && e.reason, 'stale');
  const row = await latestRow('lz3');
  assert.equal((await setBc('lz3', 'fnsku', F1, { seenRow: row })).changed, false);
  assert.equal((await setBc('lz3', 'lz_assign', null, { seenRow: row })).changed, true);
  const cur = await read('lz3');
  assert.deepEqual([cur.kind, cur.value, cur.legacy, cur.issued], ['lz_assign', null, false, null]);
});
await ta('[F2] ほかの商品の値 = lzbc_taken (ほかの SKU の今の値・ほかの商品の JAN・ほかの商品の出品の FNSKU)・自分の JAN / FNSKU は通る・前の値は効かない', async () => {
  // lz4 に F2 を付ける → lz2 に同じ値 = 断る
  const F2 = 'X00LZB0002';
  await setBc('lz4', 'fnsku', F2);
  let e = await pgErr(setBc('lz2', 'fnsku', F2));
  assert.deepEqual([e && e.reason, /lz4/.test(e && e.message)], ['lzbc_taken', true]);
  e = await pgErr(setBc('lz2', 'jan', J9));   // oth1 の JAN
  assert.deepEqual([e && e.reason, /oth1/.test(e && e.message)], ['lzbc_taken', true]);
  e = await pgErr(setBc('lz2', 'fnsku', 'X00OTH0001'));   // oth1 の出品の FNSKU
  assert.deepEqual([e && e.reason, /oth1/.test(e && e.message)], ['lzbc_taken', true]);
  assert.equal((await setBc('lz1', 'jan', J1)).value, J1, '自分の JAN を断った');
  assert.equal((await setBc('lz1', 'fnsku', 'X00LZ10001', { seenRow: await latestRow('lz1') })).value, 'X00LZ10001', '自分の出品の FNSKU を断った');
  // lz4 が F2 から空欄に変えた = F2 はもう空き (前の行の値は効かない)
  await setBc('lz4', 'lz_assign', null, { seenRow: await latestRow('lz4') });
  assert.equal((await setBc('lz2', 'fnsku', F2)).value, F2);
});
await ta('[F3] DB の形の確かめ (チェック数字・X00・空欄に値)・セット = not_single・無い SKU・人の形', async () => {
  const raw = (kind, value, st = value == null ? null : 'checked') => asEditor(async () => { await pg.query('begin'); try { return await db.query('select ops.set_sku_logizard_barcode($1::uuid, $2, $3::bigint, $4, $5, $6, $7::bigint, $8, $9::timestamptz)', [uuid(), 'naka@test', ids.lz4, kind, value, 'sku_page', await latestRow('lz4'), st, st === 'checked' ? new Date().toISOString() : null]); } finally { await pg.query('rollback'); } });
  for (const [k, v] of [['jan', JBAD], ['fnsku', 'X0LZB00001'], ['fnsku', 'x00lzb0001'], ['lz_assign', J1], ['auto', null], ['jan', null]]) {
    const e = await pgErr(raw(k, v));
    assert.match(String(e && e.message), /^invalid_input/, `${k} ${v}`);
  }
  // 🆕 #1719 R1 High 2: 値があるときは写しと照らした結果が要る・空欄には要らない・知らない結果 = 断る
  for (const [k, v, st] of [['fnsku', 'X00LZB0077', null], ['lz_assign', null, 'checked'], ['fnsku', 'X00LZB0077', 'maybe']]) {
    const e = await pgErr(raw(k, v, st));
    assert.match(String(e && e.message), /^invalid_input/, `${k} ${v} ${st}`);
  }
  let e = await pgErr(setBc('set1', 'lz_assign'));
  assert.equal(e && e.reason, 'not_single');
  e = await pgErr(asEditor(async () => { await pg.query('begin'); try { return await LB.setLzBarcodeInTx(db, { requestId: uuid(), actor: 'naka@test', skuId: '999999999', kind: 'lz_assign', via: 'sku_page' }); } finally { await pg.query('rollback'); } }));
  assert.equal(e && e.reason, 'invalid_input');
  e = await pgErr(setBc('lz4', 'lz_assign', null, { actor: 'no-mail', seenRow: await latestRow('lz4') }));
  assert.equal(e && e.reason, 'invalid_input');
});
await ta('[F4] 0078 の「決め方」だけの行 (auto・値の無い fnsku) = 値なし (もう一度入れる・legacy)・0078 の空欄 (lz_assign) はそのまま空欄', async () => {
  await pg.query('reset role');
  try {
    await pg.query("set ops.logizard_barcode_protocol = '1'");
    await pg.query("insert into ops.sku_logizard_barcode_rules (sku_id, rule, via, actor_id, request_id, db_user) values ($1, 'auto', 'register', 'naka@test', $2, 'master_edit')", [ids.oth1, uuid()]);
    await pg.query("set ops.logizard_barcode_protocol = ''");
  } finally { await pg.query('set role deploy'); }
  const r = await read('oth1');
  assert.deepEqual([r.kind, r.value, r.legacy, !!r.rule_row], [null, null, true, true]);
  // 0078 の空欄 (lz_assign・value = null) = そのまま空欄 (legacy でない・照らす値なし) (#1719 R1 Low)
  await pg.query('reset role');
  try {
    await pg.query("set ops.logizard_barcode_protocol = '1'");
    await pg.query("insert into ops.sku_logizard_barcode_rules (sku_id, rule, via, actor_id, request_id, db_user) values ($1, 'lz_assign', 'register', 'naka@test', $2, 'master_edit')", [ids.lz5, uuid()]);
    await pg.query("set ops.logizard_barcode_protocol = ''");
  } finally { await pg.query('set role deploy'); }
  const r2 = await read('lz5');
  assert.deepEqual([r2.kind, r2.value, r2.legacy, r2.lz_copy_state, r2.unchecked], ['lz_assign', null, false, null, null]);
});
await ta('[F5] 写しと照らした結果 (#1719 R1 High 2): 値の行に残る (checked / unavailable / empty / stale・写しの時刻)・照らせていない = 一言・照らせた = 一言なし・同じ値でも照らせたら書く', async () => {
  // #1719 R2 Medium 1: checked = 新しい時刻・stale = 36 時間より前の時刻・unavailable / empty = 時刻なし
  const at = '2025-01-04T12:00:00.000Z', fresh = () => new Date(Date.now() - 3600e3).toISOString();
  const setC = (code, value, copy) => asEditor(async () => { await pg.query('begin'); try { const r = await LB.setLzBarcodeInTx(db, { requestId: uuid(), actor: 'naka@test', skuId: ids[code], kind: 'fnsku', value, via: 'sku_page', seenRow: await latestRow(code), copy }); await pg.query('commit'); return r; } catch (e) { await pg.query('rollback'); throw e; } });
  for (const st of ['unavailable', 'empty', 'stale']) {
    const r = await setC('lz4', `X00LZF${({ unavailable: '001', empty: '002', stale: '003' })[st]}0`, { state: st, as_of: st === 'stale' ? at : null });
    assert.equal(r.lz_copy_state, st);
    assert.equal(r.lz_copy_as_of == null, st !== 'stale', `${st} の答えの時刻`);
    const v = await read('lz4');
    assert.deepEqual([v.lz_copy_state, !!v.unchecked], [st, true], st);
    // #1719 R2 High: 人の手順の一文 (GAS の H 列に入れる前にロジザードの商品マスタで確かめる)
    assert.ok(v.unchecked.startsWith('ロジザードの写しと照らせていません。GAS の H 列に入れる前に、ロジザードの商品マスタで同じバーコードが無いか確かめてください'), v.unchecked);
    if (st === 'stale') assert.match(v.unchecked, /2025-01-04 21:00/, '写しの時刻 (日本時間)');
    else assert.match(v.unchecked, /写しの時刻 なし/);
  }
  // 同じ値・照らせた = 書く (行が増える)・一言なし
  const n0 = (await rowsOf('lz4')).length;
  const r = await setC('lz4', 'X00LZF0030', { state: 'checked', as_of: fresh() });
  assert.deepEqual([r.changed, (await rowsOf('lz4')).length], [true, n0 + 1]);
  assert.deepEqual([(await read('lz4')).lz_copy_state, (await read('lz4')).unchecked], ['checked', null]);
  // 渡さない (呼び手が照らしていない) = unavailable 扱い
  assert.equal((await setC('lz4', 'X00LZF0040', null)).lz_copy_state, 'unavailable');
  // 呼び手が checked / stale を時刻なしで渡した = unavailable 扱い (時刻は付けない)・empty に時刻 = 外して書く
  const r2 = await setC('lz4', 'X00LZF0050', { state: 'checked', as_of: null });
  assert.deepEqual([r2.lz_copy_state, r2.lz_copy_as_of ?? null], ['unavailable', null]);
  const r3 = await setC('lz4', 'X00LZF0060', { state: 'empty', as_of: fresh() });
  assert.deepEqual([r3.lz_copy_state, r3.lz_copy_as_of ?? null], ['empty', null]);
});
await ta('[F6] 結果と時刻の組み合わせ (#1719 R2 Medium 1): 直に関数を呼んでも偽れない (checked = 36 時間より新しい・stale = 36 時間より前・unavailable / empty = 時刻なし)・表の CHECK も', async () => {
  const H = 3600e3, iso = (ms) => new Date(Date.now() + ms).toISOString();
  const direct = (st, asOf, value = 'X00LZF6001') => asEditor(async () => {
    await pg.query('begin');
    try { return (await db.query('select ops.set_sku_logizard_barcode($1::uuid, $2, $3::bigint, $4, $5, $6, $7::bigint, $8, $9::timestamptz) as r', [uuid(), 'naka@test', ids.lz4, 'fnsku', value, 'sku_page', await latestRow('lz4'), st, asOf])).rows[0].r; } finally { await pg.query('rollback'); }
  });
  for (const [st, asOf, why] of [['checked', null, 'checked に時刻なし'], ['checked', iso(-40 * H), 'checked に 40 時間前'], ['checked', iso(-36 * H - 10 * 60e3), 'checked に 36 時間 10 分前'],
    ['checked', iso(H), 'checked に 1 時間先'], ['stale', null, 'stale に時刻なし'], ['stale', iso(-H), 'stale に 1 時間前'], ['stale', iso(H), 'stale に 1 時間先'],
    ['unavailable', iso(-H), 'unavailable に時刻'], ['empty', iso(-H), 'empty に時刻'], ['empty', iso(-40 * H), 'empty に古い時刻']]) {
    const e = await pgErr(direct(st, asOf));
    assert.match(String(e && e.message), /^invalid_input/, why);
  }
  for (const [st, asOf] of [['checked', iso(-H)], ['checked', iso(60e3)], ['stale', iso(-40 * H)], ['unavailable', null], ['empty', null]]) {
    const r = await direct(st, asOf);
    assert.deepEqual([r.ok, r.lz_copy_state, r.lz_copy_as_of == null], [true, st, asOf == null], `${st} ${asOf}`);
  }
  // 境目の前後 5 分はどちらでも通す (アプリと DB の時計のずれ・待ち時間で現場を止めない)
  for (const st of ['checked', 'stale']) assert.equal((await direct(st, iso(-36 * H))).lz_copy_state, st, `${st} 36 時間ちょうど`);
  // 表の CHECK (関数の外 = 持ち主が手順の印を立てて書いても組み合わせは守る)
  await pg.query('reset role');
  try {
    for (const [st, asOf] of [['empty', iso(-H)], ['unavailable', iso(-H)], ['checked', null], ['stale', null]]) {
      await pg.query('begin');
      try {
        await pg.query("set local ops.logizard_barcode_protocol = '1'");
        const e = await pgErr(pg.query("insert into ops.sku_logizard_barcode_rules (sku_id, rule, value, via, actor_id, request_id, db_user, lz_copy_state, lz_copy_as_of) values ($1, 'fnsku', 'X00LZF6009', 'sku_page', 'naka@test', $2, 'x', $3, $4::timestamptz)", [ids.lz4, uuid(), st, asOf]));
        assert.equal(e && e.code, '23514', `${st} ${asOf}`);
        assert.match(String(e && e.message), /ck_slbr_copy_as_of/);
      } finally { await pg.query('rollback'); }
    }
  } finally { await pg.query('set role deploy'); }
});
await ta('[F7] 書かなかった答え (changed: false) = 残っている行の時刻 (#1719 R3 Low 1): 同じ値・同じ stale で写しの時刻だけ違う = 書かない・答えは前の時刻', async () => {
  const setC = (copy) => asEditor(async () => { await pg.query('begin'); try { const r = await LB.setLzBarcodeInTx(db, { requestId: uuid(), actor: 'naka@test', skuId: ids.lz4, kind: 'fnsku', value: 'X00LZF7001', via: 'sku_page', seenRow: await latestRow('lz4'), copy }); await pg.query('commit'); return r; } catch (e) { await pg.query('rollback'); throw e; } });
  const t1 = '2025-02-01T00:00:00.000Z', t2 = '2025-03-01T00:00:00.000Z';
  const r1 = await setC({ state: 'stale', as_of: t1 });
  assert.equal(r1.changed, true);
  const r2 = await setC({ state: 'stale', as_of: t2 });
  assert.deepEqual([r2.changed, r2.lz_copy_state, Date.parse(r2.lz_copy_as_of)], [false, 'stale', Date.parse(t1)], JSON.stringify(r2));
  assert.equal((await q('select lz_copy_as_of from ops.sku_logizard_barcode_rules where sku_id = $1 order by rule_row desc limit 1', [ids.lz4]))[0].lz_copy_as_of.getTime(), Date.parse(t1));
});
await ta('[R1] 参考: Amazon の FNSKU・この商品の JAN と違う = 参考の一言だけ (保存はできる)・同じ / Amazon のデータが無い = 一言なし', async () => {
  // lz1 = JAN J1・Amazon の FNSKU X00LZ10001。違う FNSKU を打つ = 通る + 参考
  const F3 = 'X00LZB0003';
  assert.equal((await setBc('lz1', 'fnsku', F3, { seenRow: await latestRow('lz1') })).value, F3);
  let r = await read('lz1');
  assert.deepEqual(r.notes, ['参考: Amazon 日本の出品の FNSKU (X00LZ10001) と違います (ロジザードには打った値を送ります)']);
  await setBc('lz1', 'jan', '4912345678904', { seenRow: await latestRow('lz1') });
  r = await read('lz1');
  assert.match(r.notes[0], /この商品の JAN \(4901234567894\) と違います/);
  await setBc('lz1', 'jan', J1, { seenRow: await latestRow('lz1') });
  assert.deepEqual((await read('lz1')).notes, []);
  assert.deepEqual((await read('lz2')).notes, [], 'Amazon のデータが無い商品に参考の一言');
});
await ta('[S1] 守り: master_edit は値の表に直接書けない・持ち主も関数の外では書けない・TRUNCATE できない・watcher は実行できない・PUBLIC の実行権なし・0078 の関数は master_edit から外す', async () => {
  const code = async (fn) => { const e = await pgErr(fn()); return e ? e.code : null; };
  assert.equal(await asEditor(() => code(() => pg.query(`insert into ops.sku_logizard_barcode_rules (sku_id, rule, value, via, actor_id, request_id, db_user) values ($1, 'fnsku', 'X00AAAAAAA', 'sku_page', 'x@y', $2, 'x')`, [ids.lz4, uuid()]))), '42501');
  assert.equal(await asEditor(() => code(() => pg.query("update ops.sku_logizard_barcode_rules set value = null"))), '42501');
  assert.equal(await code(() => pg.query("update ops.sku_logizard_barcode_rules set value = null")), '42501', '持ち主が値を変えられた');
  assert.ok(await code(() => pg.query('truncate ops.sku_logizard_barcode_rules')), 'TRUNCATE できた');
  assert.equal(await asRole('watcher', () => code(() => pg.query("select ops.set_sku_logizard_barcode($1::uuid, 'x@y', $2::bigint, 'lz_assign', null, 'sku_page', null, null, null)", [uuid(), ids.lz4]))), '42501');
  for (const sig of ['ops.set_sku_logizard_barcode(uuid, text, bigint, text, text, text, bigint, text, timestamp with time zone)', 'ops.lzbc_value_holder(bigint, text)']) {
    const p = (await q(`select proacl::text as acl, prosecdef, proconfig::text as cfg from pg_proc where oid = $1::regprocedure`, [sig]))[0];
    assert.equal(p.prosecdef, true, sig); assert.match(p.cfg, /search_path=pg_catalog, pg_temp/, sig);
    assert.ok(!/(^|[{,])=X/.test(p.acl), `PUBLIC に実行権がある: ${sig} ${p.acl}`); assert.match(p.acl, /master_edit=X/, sig);
  }
  const old = (await q(`select proacl::text as acl from pg_proc where oid = 'ops.set_sku_logizard_barcode_rule(uuid, text, bigint, text, text, text, bigint)'::regprocedure`))[0];
  assert.ok(!/master_edit=X/.test(old.acl || ''), `0078 の決め方だけの関数を master_edit が実行できる: ${old.acl}`);
  // #1719 R1 Medium 2: migration 0080 自身が master_edit の実行を外す (ロールの script の流し直しを待たない) = 付けてから 0080 の文を流す
  await pg.query('reset role');
  try {
    await pg.query("grant execute on function ops.set_sku_logizard_barcode_rule(uuid, text, bigint, text, text, text, bigint) to master_edit");
    const mig = fs.readFileSync(new URL('../db/company/migrations/0080_logizard_barcode_values.sql', import.meta.url), 'utf8');
    const block = /do \$\$ begin\s+if exists \(select 1 from pg_catalog\.pg_roles where rolname = 'master_edit'\)[\s\S]*?end \$\$;/.exec(mig);
    assert.ok(block, '0080 に旧の関数の revoke が無い');
    await pg.query(block[0]);
  } finally { await pg.query('set role deploy'); }
  const old2 = (await q(`select proacl::text as acl from pg_proc where oid = 'ops.set_sku_logizard_barcode_rule(uuid, text, bigint, text, text, text, bigint)'::regprocedure`))[0];
  assert.ok(!/master_edit=X/.test(old2.acl || ''), `0080 の revoke が効かない: ${old2.acl}`);
});

// ─── H 画面 (本物の router) ───
const MR = await import('../apps/master-edit/router.mjs');
process.env.COMPANY_DB_URL = 'postgres://deploy@localhost:5432/test';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost:5432/test';
process.env.MASTER_EDITORS = 'Naka@Test';
MR.__setPgClientFactory(async (url) => {
  await pg.query(`set role ${/master_edit@/.test(url) ? 'master_edit' : 'deploy'}`);
  return { query: (t, p) => pg.query(t, p), end: async () => { await pg.query('set role deploy'); }, on: () => {} };
});
MR.__setClock(() => Date.now());
MR.__setShippingRatesProvider(async () => new Map([['S02', { method: '宅急便', cost: 520 }]]));
// ロジザードの写し (バーコードマスタ): X00LZWAREH = ロジザードでほかの商品 (zz-old) が使っている
const mirrorOf = ({ state = 'checked', bc = [['zz-old', 'X00LZWAREH']] } = {}) => {
  const fresh = new Date().toISOString(), old = new Date(Date.now() - 40 * 3600e3).toISOString();
  const rows = state === 'empty' || state === 'unavailable' ? [] : bc;
  return { ok: true, reason: null, keys: new Set(rows.map(([id]) => id.toLowerCase())), ids: new Map(rows.map(([id]) => [id.toLowerCase(), new Set([id])])), barcodes: rows.reduce((m, [id, v]) => m.set(v, new Set([...(m.get(v) || []), id])), new Map()),
    list_as_of: fresh, bc_as_of: state === 'stale' ? old : rows.length ? fresh : null, rows: 1,
    bc: { ok: state === 'checked' || state === 'stale', reason: state === 'unavailable' ? '写しがありません' : state === 'empty' ? '写しが空です' : null, rows: rows.length, empty: state === 'empty' } };
};
let LZ_MIRROR = mirrorOf();
MR.__setLzMirrorProvider(async () => LZ_MIRROR);
const SESS = {
  editor: { authenticated: true, email: 'naka@test', displayName: '中原', role: 'user', allowedApps: ['master-edit'] },
  viewer: { authenticated: true, email: 'viewer@test', role: 'user', allowedApps: ['master-edit'] },
};
const app = express();
app.set('view engine', 'ejs');
app.use((req, res, next) => { req.session = SESS[req.headers['x-test-session']] || null; next(); });
app.use('/apps/master-edit', MR.default);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const ORIGIN = `http://127.0.0.1:${server.address().port}`;
async function call(method, url, { body, session = 'editor', origin = true } = {}) {
  const headers = { Accept: 'application/json', 'x-test-session': session };
  if (body !== undefined) headers['Content-Type'] = 'application/json';
  if (origin) headers.Origin = ORIGIN;
  const r = await fetch(ORIGIN + url, { method, headers, body: body === undefined ? undefined : JSON.stringify(body), redirect: 'manual', signal: AbortSignal.timeout(30e3) });
  const text = await r.text();
  let j = null; try { j = JSON.parse(text); } catch { /* HTML */ }
  return { status: r.status, j, text };
}
async function scriptsOf(html) {
  const all = [...html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)];
  for (const m of all.filter((x) => /type="application\/json"/.test(x[1]))) JSON.parse(m[2]);
  const files = [];
  for (const m of all.filter((x) => /\bsrc="\/apps\/master-edit\/public\//.test(x[1]))) {
    const src = /\bsrc="([^"]+)"/.exec(m[1])[1];
    const r = await fetch(ORIGIN + src.replace(/&amp;/g, '&'), { headers: { 'x-test-session': 'editor' }, signal: AbortSignal.timeout(30e3) });
    assert.equal(r.status, 200, src);
    const t = await r.text();
    new vm.Script(t); assert.ok(!/<%|%>/.test(t), 'EJS のタグが JS に残っている');
    files.push({ src, text: t });
  }
  return files;
}
const section = (html) => (/<section class="panel" id="sec-lzbc"[\s\S]*?<\/section>/.exec(html) || [''])[0];
const lzData = (html) => JSON.parse((/<script type="application\/json" id="me-lzbc">([\s\S]*?)<\/script>/.exec(html) || [])[1] || 'null');
const api = (code) => `/apps/master-edit/api/sku/${code}/logizard-barcode`;

await ta('[H1] 単品の商品の画面: いま (値・種類・だれ)・欄 (値と空欄の印)・参考 (Company DB の FNSKU・JAN)・まだ入っていない帯・JSON と JS (me-sku.js の後)・JAN の欄の文', async () => {
  const r = await call('GET', '/apps/master-edit/sku/lz1');
  assert.equal(r.status, 200, r.text.slice(0, 300));
  const s = section(r.text);
  for (const w of ['ロジザードのバーコード', J1, 'JAN</span>', 'id="lzbc-in"', 'id="lzbc-blank"', '空欄 (ロジザードが番号を付ける)', 'id="lzbc-save" disabled>バーコードを保存', '参考: Company DB の FNSKU・JAN', 'X00LZ10001']) assert.ok(s.includes(w), `欄に「${w}」が無い`);
  assert.ok(!s.includes('まだ入っていません'));
  const d = lzData(r.text);
  assert.deepEqual([d.kind, d.value, !!d.seenRow, d.canWrite], ['jan', J1, true, true]);
  const files = await scriptsOf(r.text);
  const order = files.map((f) => f.src.replace(/\?.*$/, '').replace('/apps/master-edit/public/', ''));
  assert.ok(order.includes('me-lzbarcode.js') && order.indexOf('me-lzbarcode.js') > order.indexOf('me-sku.js'), `JS の順: ${order.join(', ')}`);
  const skuEjs = fs.readFileSync(new URL('../apps/master-edit/views/sku.ejs', import.meta.url), 'utf8');
  assert.ok(skuEjs.includes('ロジザードに送るバーコードは下の「ロジザードのバーコード」(FNSKU か JAN を打つ)') && !skuEjs.includes('ロジザード・product-hub でも使います'), 'JAN の欄の文');
  const r0 = await call('GET', '/apps/master-edit/sku/oth1');
  assert.ok(section(r0.text).includes('まだ入っていません') && section(r0.text).includes('前の「決め方」だけが残っています'), '決め方だけの行の帯');
});
await ta('[H2] 名簿でない = 見るだけ (欄・保存のボタン・JS なし)・セットには出さない', async () => {
  const r = await call('GET', '/apps/master-edit/sku/lz1', { session: 'viewer' });
  const s = section(r.text);
  assert.ok(s.includes('見るだけです') && !s.includes('id="lzbc-save"') && !s.includes('id="lzbc-in"'));
  assert.ok(!r.text.includes('me-lzbarcode.js'));
  assert.ok(!(await call('GET', '/apps/master-edit/sku/set1')).text.includes('id="sec-lzbc"'));
});
await ta('[H3] 保存の API: 名簿・Origin・形・stale = 409・ロジザードの写しでほかの商品の値 = 409・Company DB のほかの商品の値 = 409・セット = 400・無い商品 = 404', async () => {
  const seen = lzData((await call('GET', '/apps/master-edit/sku/lz4')).text).seenRow;
  const ok = { request_id: uuid(), barcode: { kind: 'value', value: 'x00lzb0009' }, seen_row: seen };
  assert.equal((await call('POST', api('lz4'), { body: ok, session: 'viewer' })).status, 403);
  assert.notEqual((await call('POST', api('lz4'), { body: ok, origin: false })).status, 200);
  for (const [body, why] of [[{ request_id: uuid(), barcode: { kind: 'value', value: JBAD } }, 'jan'], [{ request_id: uuid() }, 'none'], [{ request_id: 'x', barcode: { kind: 'lz_assign' } }, 'rid'],
    [{ request_id: uuid(), barcode: 'auto' }, 'old'], [{ request_id: uuid(), barcode: { kind: 'lz_assign' }, seen_row: 'abc' }, 'seen']]) {
    assert.equal((await call('POST', api('lz4'), { body })).status, 400, why);
  }
  let r = await call('POST', api('lz4'), { body: { request_id: uuid(), barcode: { kind: 'value', value: 'X00LZWAREH' }, seen_row: seen } });
  assert.deepEqual([r.status, r.j.reason, /zz-old/.test(r.j.error)], [409, 'lzbc_taken', true]);
  r = await call('POST', api('lz4'), { body: { request_id: uuid(), barcode: { kind: 'value', value: J9 }, seen_row: seen } });
  assert.deepEqual([r.status, r.j.reason], [409, 'lzbc_taken']);
  r = await call('POST', api('lz4'), { body: ok });
  assert.equal(r.status, 200, r.text);
  assert.deepEqual([r.j.kind, r.j.value, r.j.changed], ['fnsku', 'X00LZB0009', true]);
  r = await call('POST', api('lz4'), { body: { request_id: uuid(), barcode: { kind: 'lz_assign' }, seen_row: seen } });
  assert.deepEqual([r.status, r.j.reason], [409, 'stale']);
  assert.equal((await call('POST', api('set1'), { body: { request_id: uuid(), barcode: { kind: 'lz_assign' } } })).status, 400);
  assert.equal((await call('POST', api('nope-x'), { body: { request_id: uuid(), barcode: { kind: 'lz_assign' } } })).status, 404);
});
await ta('[H3b] 商品の画面の保存 × ロジザードの写し (#1719 R1 High 2): 読めない / 空 / 古い = 保存は通る (200)・答えに lz_unchecked・値の行に印・画面に「照らせていません」/ 照らせた = 印なし / 古い写しでもほかの商品の値 = 409', async () => {
  try {
    for (const [st, v] of [['unavailable', 'X00LZH3001'], ['empty', 'X00LZH3002'], ['stale', 'X00LZH3003']]) {
      LZ_MIRROR = mirrorOf({ state: st });
      const seen = lzData((await call('GET', '/apps/master-edit/sku/lz4')).text).seenRow;
      const r = await call('POST', api('lz4'), { body: { request_id: uuid(), barcode: { kind: 'value', value: v }, seen_row: seen } });
      assert.equal(r.status, 200, `${st}: ${r.text}`);
      assert.equal(r.j.lz_unchecked && r.j.lz_unchecked.state, st, st);
      assert.equal((await q('select lz_copy_state from ops.sku_logizard_barcode_rules where sku_id = $1 order by rule_row desc limit 1', [ids.lz4]))[0].lz_copy_state, st);
      const page = await call('GET', '/apps/master-edit/sku/lz4');
      assert.match(section(page.text), new RegExp(`id="lzbc-unchecked" data-state="${st}"`), st);
    }
    LZ_MIRROR = mirrorOf({ state: 'stale' });
    let seen = lzData((await call('GET', '/apps/master-edit/sku/lz4')).text).seenRow;
    let r = await call('POST', api('lz4'), { body: { request_id: uuid(), barcode: { kind: 'value', value: 'X00LZWAREH' }, seen_row: seen } });
    assert.deepEqual([r.status, r.j.reason], [409, 'lzbc_taken'], '古い写しにあるほかの商品の値を通した');
    LZ_MIRROR = mirrorOf();
    r = await call('POST', api('lz4'), { body: { request_id: uuid(), barcode: { kind: 'value', value: 'X00LZH3004' }, seen_row: seen } });
    assert.equal(r.status, 200, r.text);
    assert.equal(r.j.lz_unchecked, undefined);
    assert.ok(!section((await call('GET', '/apps/master-edit/sku/lz4')).text).includes('id="lzbc-unchecked"'));
    // 空欄 = 照らさない (写しが読めなくても印なし)
    LZ_MIRROR = mirrorOf({ state: 'unavailable' });
    seen = lzData((await call('GET', '/apps/master-edit/sku/lz4')).text).seenRow;
    r = await call('POST', api('lz4'), { body: { request_id: uuid(), barcode: { kind: 'lz_assign' }, seen_row: seen } });
    assert.deepEqual([r.status, r.j.lz_unchecked], [200, undefined]);
  } finally { LZ_MIRROR = mirrorOf(); }
});
await ta('[H3c] 押し直しの答えの印 = DB に残した結果と時刻 (#1719 R2 Low)・印は手順の一文 (R2 High)・商品の一覧の「ロジザードの確かめ待ち」で出せる', async () => {
  try {
    LZ_MIRROR = mirrorOf({ state: 'stale' });
    let seen = lzData((await call('GET', '/apps/master-edit/sku/lz4')).text).seenRow;
    const body = { request_id: uuid(), barcode: { kind: 'value', value: 'X00LZH3C01' }, seen_row: seen };
    const r1 = await call('POST', api('lz4'), { body });
    assert.equal(r1.status, 200, r1.text);
    assert.equal(r1.j.lz_unchecked.state, 'stale');
    assert.ok(r1.j.lz_unchecked.note.startsWith('ロジザードの写しと照らせていません。GAS の H 列に入れる前に、ロジザードの商品マスタで同じバーコードが無いか確かめてください'), r1.j.lz_unchecked.note);
    assert.ok(r1.j.lz_unchecked.note.includes(r1.j.lz_unchecked.as_of_jst), '一文に写しの時刻が無い');
    const page = section((await call('GET', '/apps/master-edit/sku/lz4')).text);
    assert.ok(page.includes('GAS の H 列に入れる前に、ロジザードの商品マスタで同じバーコードが無いか確かめてください'), '商品の画面に手順の一文が無い');
    // 写しが新しくなってから押し直す = 前に残した stale のまま (今の写しの checked で印を消さない)
    LZ_MIRROR = mirrorOf();
    const r2 = await call('POST', api('lz4'), { body });
    assert.deepEqual([r2.status, r2.j.replayed, r2.j.lz_unchecked && r2.j.lz_unchecked.state, r2.j.lz_unchecked && r2.j.lz_unchecked.as_of], [200, true, 'stale', r1.j.lz_unchecked.as_of], r2.text);
    // 商品の一覧の絞り込み: 今の行が照らせていない値 = 出る・照らせた値 / 空欄 = 出ない
    const R = await import('../apps/master-edit/read.mjs');
    // (lz1・lz2 = 上の試験で写しを渡さずに書いた値 = unavailable で出る)
    const codesOf = async () => (await asEditor(() => R.listSkus(db, { missing: 'lzbc' }))).rows.map((x) => x.code).filter((c) => c !== 'lz1' && c !== 'lz2');
    assert.deepEqual(await codesOf(), ['lz4']);
    // 逆: 照らせた値を、写しが読めなくなってから押し直す = 印なし (残した checked)
    seen = lzData((await call('GET', '/apps/master-edit/sku/lz4')).text).seenRow;
    const body2 = { request_id: uuid(), barcode: { kind: 'value', value: 'X00LZH3C01' }, seen_row: seen };
    const r3 = await call('POST', api('lz4'), { body: body2 });
    assert.deepEqual([r3.status, r3.j.changed, r3.j.lz_unchecked], [200, true, undefined], r3.text);
    assert.deepEqual(await codesOf(), [], '照らせた値が一覧に残った');
    LZ_MIRROR = mirrorOf({ state: 'unavailable' });
    const r4 = await call('POST', api('lz4'), { body: body2 });
    assert.deepEqual([r4.status, r4.j.replayed, r4.j.lz_unchecked], [200, true, undefined], r4.text);
    // 照らせていない値 = 出る → 空欄に変える = 照らさない = 出ない
    seen = lzData((await call('GET', '/apps/master-edit/sku/lz4')).text).seenRow;
    await call('POST', api('lz4'), { body: { request_id: uuid(), barcode: { kind: 'value', value: 'X00LZH3C02' }, seen_row: seen } });
    assert.deepEqual(await codesOf(), ['lz4']);
    seen = lzData((await call('GET', '/apps/master-edit/sku/lz4')).text).seenRow;
    await call('POST', api('lz4'), { body: { request_id: uuid(), barcode: { kind: 'lz_assign' }, seen_row: seen } });
    assert.deepEqual(await codesOf(), []);
    // 一覧の画面の「未入力」の選択肢
    const list = await call('GET', '/apps/master-edit/?missing=lzbc');
    assert.equal(list.status, 200);
    assert.ok(list.text.includes('<option value="lzbc" selected>ロジザードの確かめ待ち</option>'), '一覧に選択肢が無い');
  } finally { LZ_MIRROR = mirrorOf(); }
});
await ta('[H3d] 写しで同じ値を持つ商品が複数 (#1719 R3 Medium): この商品と ほかの商品の両方 = 断る (並びの順に左右されない)・この商品だけ = 通る', async () => {
  try {
    for (const bc of [[['lz4', 'X00LZDUP01'], ['zz-other', 'X00LZDUP01']], [['zz-other', 'X00LZDUP01'], ['lz4', 'X00LZDUP01']]]) {
      LZ_MIRROR = mirrorOf({ bc });
      const seen = lzData((await call('GET', '/apps/master-edit/sku/lz4')).text).seenRow;
      const r = await call('POST', api('lz4'), { body: { request_id: uuid(), barcode: { kind: 'value', value: 'X00LZDUP01' }, seen_row: seen } });
      assert.deepEqual([r.status, r.j.reason, /zz-other/.test(r.j.error)], [409, 'lzbc_taken', true], `${JSON.stringify(bc)}: ${r.text}`);
    }
    LZ_MIRROR = mirrorOf({ bc: [['LZ4', 'X00LZDUP02']] });
    const seen = lzData((await call('GET', '/apps/master-edit/sku/lz4')).text).seenRow;
    const r = await call('POST', api('lz4'), { body: { request_id: uuid(), barcode: { kind: 'value', value: 'X00LZDUP02' }, seen_row: seen } });
    assert.equal(r.status, 200, r.text);
  } finally { LZ_MIRROR = mirrorOf(); }
});
await ta('[H3e] 保存できた後の再送 (#1719 R4 High): 1 回目は保存できた → 写しに同じ値のほかの商品が現れる → 同じ request_id で再送 = 保存済みの答え (409 にならない)・新しい request_id = 409', async () => {
  try {
    LZ_MIRROR = mirrorOf();
    const seen = lzData((await call('GET', '/apps/master-edit/sku/lz4')).text).seenRow;
    const body = { request_id: uuid(), barcode: { kind: 'value', value: 'X00LZR4001' }, seen_row: seen };
    const r1 = await call('POST', api('lz4'), { body });   // この応答は届かなかったことにする
    assert.equal(r1.status, 200, r1.text);
    LZ_MIRROR = mirrorOf({ bc: [['zz-new', 'X00LZR4001']] });
    const r2 = await call('POST', api('lz4'), { body });
    assert.deepEqual([r2.status, r2.j.replayed, r2.j.kind, r2.j.value, r2.j.rule_row, r2.j.lz_unchecked], [200, true, 'fnsku', 'X00LZR4001', r1.j.rule_row, undefined], r2.text);
    const r3 = await call('POST', api('lz4'), { body: { ...body, request_id: uuid(), seen_row: r1.j.rule_row } });
    assert.deepEqual([r3.status, r3.j.reason], [409, 'lzbc_taken'], r3.text);
  } finally { LZ_MIRROR = mirrorOf(); }
});
await ta('[H4] 画面の動き (Chromium): 値を打つと空欄の印が外れる・空欄に印で値が消える・形の一言・通信が切れた保存は同じ request_id で送り直す (2 回書かない)', async () => {
  let chromium;
  try { ({ chromium } = await import('playwright')); } catch (e) { throw new Error(`Playwright が無い: ${e.message}`); }
  const browser = await chromium.launch();
  try {
    const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 }, locale: 'ja-JP', extraHTTPHeaders: { 'x-test-session': 'editor' } });
    const p = await ctx.newPage();
    const errors = []; p.on('pageerror', (x) => errors.push(x.message));
    await p.goto(`${ORIGIN}/apps/master-edit/sku/lz2`);
    await p.check('#lzbc-blank');
    assert.equal(await p.inputValue('#lzbc-in'), '');
    assert.equal(await p.isDisabled('#lzbc-in'), true);
    await p.uncheck('#lzbc-blank');
    await p.fill('#lzbc-in', JBAD);
    assert.match(await p.innerText('#lzbc-msg2'), /チェック数字/);
    assert.equal(await p.isDisabled('#lzbc-save'), true);
    await p.fill('#lzbc-in', 'x00lzc0001');
    assert.match(await p.innerText('#lzbc-msg2'), /FNSKU の形/);
    const n0 = (await rowsOf('lz2')).length;
    const sent = [];
    let lost = true;
    await p.route('**/logizard-barcode', async (route) => {
      sent.push(JSON.parse(route.request().postData()));
      if (lost) { lost = false; await route.fetch(); await route.fulfill({ status: 502, body: 'bad gateway' }); } else await route.continue();
    });
    await p.click('#lzbc-save');
    await p.waitForFunction(() => /保存されたか分かりません/.test(document.querySelector('#lzbc-msg').textContent));
    const [resp] = await Promise.all([p.waitForResponse((r) => /logizard-barcode$/.test(r.url())), p.click('#lzbc-save')]);
    assert.equal(resp.status(), 200);
    assert.equal(sent.length, 2); assert.equal(sent[0].request_id, sent[1].request_id, '送り直しで request_id が変わった');
    assert.deepEqual(sent[1].barcode, { kind: 'value', value: 'X00LZC0001' });
    assert.equal((await rowsOf('lz2')).length, n0 + 1, '2 行になった');
    assert.deepEqual(errors, [], '画面の JS の例外');
  } finally { await browser.close(); }
});
await ta('[H5] つかいかた: ロジザードのバーコードの節', async () => {
  const r = await call('GET', '/apps/master-edit/manual');
  for (const w of ['id="lz-barcode"', '新商品の登録で、ロジザードに送るバーコードを打ちます', 'Amazon の出品とは関係ありません', 'バーコードをまとめて貼る', 'ほかの商品と同じバーコードは保存できません',
    'H 列 (バーコード) には、ここで打った値を入れてください',
    'ロジザードの写しと照らせていません。GAS の H 列に入れる前に、ロジザードの商品マスタで同じバーコードが無いか確かめてください', 'ロジザードの確かめ待ち']) assert.ok(r.text.includes(w), `つかいかたに「${w}」が無い`);
});

server.close();
console.log(`\n${passed} 件 ok${process.exitCode ? ' (NG あり)' : ''}`);
process.exit(process.exitCode || 0);
