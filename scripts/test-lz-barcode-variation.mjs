/**
 * test-lz-barcode-variation.mjs — まとまり (色違い・サイズ違い) の登録のロジザードのバーコード (CompanyDB構想 23 v4・0080・#1719 Codex R1)
 *
 * Company DB = PGlite (scripts/fixtures/master-variation-db.mjs = products.parent も company = まとまりの API が開いている形)。本物の router を HTTP 越しに通す。
 * 固定する契約:
 *   K まとまりの確かめ (/api/variation/check): 打った値の持ち主 (Company DB のほかの商品・ロジザードの写し)・子の値がほかの子の JAN と同じ (lz_kid_clash)
 *   O 子の値がほかの子の JAN と同じ = 400 lzbc_dup (子の順番を両方の向きで・何も作らない = SKU・JAN・バーコードの行が残らない) (#1719 R1 High 1)
 *   T ほかの商品と同じ値 = 409 lzbc_taken (ロジザードの写し = router / Company DB のほかの商品の JAN = DB の関数)・全部戻る
 *   C ロジザードの写しが読めない / 空 / 古い = 登録は通る (200)・答えに lz_unchecked・値の子の行に印 (空欄の子は印なし) / 照らせた = 印なし (#1719 R1 High 2)
 * 使い方: node scripts/test-lz-barcode-variation.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import express from 'express';

const killer = setTimeout(() => { console.error('時間切れ (420 秒)'); process.exit(3); }, 420e3);
killer.unref();
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);
process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'lzbarcode2-var-'));

const { setupVariationDb, jan13 } = await import('./fixtures/master-variation-db.mjs');
const RB = await import('../lib/master-register-blanks.mjs');
const T = await setupVariationDb();
const { pg, q, one, asEditor } = T;
// ほかの商品の JAN (s003)
const TAKEN_JAN = jan13('490123456700');
{
  const s3 = (await one(`select sku_id::text as id from core.skus where code = 's003'`)).id;
  await asEditor(async () => { await pg.query('begin'); try { await pg.query('select ops.edit_sku_jan($1::uuid, $2, $3, $4::jsonb, $5::bigint, $6::jsonb, $7::jsonb)', [crypto.randomUUID(), 'naka@test', null, T.OWN, s3, '[]', JSON.stringify([TAKEN_JAN])]); await pg.query('commit'); } catch (e) { await pg.query('rollback'); throw e; } });
}
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const { initPurchaseOrders } = await import('../apps/purchase-orders/db.js');
initPurchaseOrders();

process.env.COMPANY_DB_URL = 'postgres://owner@localhost/lzv';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost/lzv';
process.env.MASTER_EDITORS = 'naka@test';
process.env.MASTER_EDIT_OPEN = '1';
const MR = await import('../apps/master-edit/router.mjs');
let chain = Promise.resolve();
MR.__setPgClientFactory(async (url) => {
  let release; const prev = chain; chain = new Promise((r) => { release = r; }); await prev;
  await pg.query(`set role ${/master_edit@/.test(url) ? 'master_edit' : 'deploy'}`);
  return { query: (t, p) => pg.query(t, p), end: async () => { await pg.query('set role deploy'); release(); }, on: () => {} };
});
const RATES = new Map([['B2', { method: '宅急便コンパクト', cost: 520 }]]);
MR.__setShippingRatesProvider(async () => RATES);
MR.__setPhDraftLookup(async () => null);
/** ロジザードの写し: state = checked (新しい) / unavailable / empty / stale (40 時間前)。X00LZVWARE = ロジザードでほかの商品 (zz-old) */
const mirrorOf = (state = 'checked') => {
  const rows = state === 'unavailable' || state === 'empty' ? [] : [['zz-old', 'X00LZVWARE']];
  const at = state === 'stale' ? new Date(Date.now() - 40 * 3600e3).toISOString() : new Date().toISOString();
  return { ok: true, reason: null, keys: new Set(rows.map(([id]) => id)), ids: new Map(rows.map(([id]) => [id, new Set([id])])), barcodes: rows.reduce((m, [id, v]) => m.set(v, new Set([...(m.get(v) || []), id])), new Map()),
    list_as_of: at, bc_as_of: rows.length ? at : null, rows: 1, bc: { ok: rows.length > 0, reason: state === 'unavailable' ? '写しがありません' : state === 'empty' ? '写しが空です' : null, rows: rows.length, empty: state === 'empty' } };
};
let MIRROR = mirrorOf();
MR.__setLzMirrorProvider(async () => MIRROR);
const app = express();
app.set('view engine', 'ejs');
app.use((req, res, next) => { req.session = { authenticated: true, email: 'naka@test', displayName: '中原', role: 'user', allowedApps: ['master-edit', 'purchase-orders'] }; next(); });
app.use('/apps/master-edit', MR.default);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const ORIGIN = `http://127.0.0.1:${server.address().port}`;
async function post(url, body) {
  const r = await fetch(ORIGIN + '/apps/master-edit' + url, { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json', Origin: ORIGIN }, body: JSON.stringify(body), signal: AbortSignal.timeout(60e3) });
  const text = await r.text();
  let j = null; try { j = JSON.parse(text); } catch { /* */ }
  return { status: r.status, j, text };
}
const VALUES = { standard_price: '3980', cost: { jpy: '1650' }, tax_rate: '0.1', sales_class: '3', primary_supplier: '0001', reorder_months: '2', expiry_managed: '0', inbound_date_managed: '0', shipping_code: 'B2' };
/** まとまりの登録の本文 (子 = [[文字, JAN, ロジザードのバーコード]])。必須でない項目は全部「入力なし」 */
function body(group, kids) {
  const b = {
    request_id: crypto.randomUUID(), group: { mode: 'new', code: group, name: `試し ${group}` }, axes: [{ axis: 1, name: 'カラー' }],
    options: kids.map(([c]) => ({ axis: 1, code: c, name: `色${c}` })),
    children: kids.map(([c, jan, lz]) => ({ code: group + c, choices: { 1: c }, name: `試し ${group}【色${c}】`, jan: jan || '', lz_barcode: lz })),
    values: { ...VALUES }, card: { official_url: '', amazon_url: '', asin: '' },
  };
  b.blanks = RB.missingBlankKeys('variation', b, { orderApplicable: true });
  return b;
}
const counts = async (group) => one(`select (select count(*) from core.skus where code_norm like $1)::int as skus,
    (select count(*) from core.external_ids e join core.skus s on s.product_id = e.entity_id and e.entity_type = 'product' where s.code_norm like $1 and e.system = 'jan')::int as jans,
    (select count(*) from ops.sku_logizard_barcode_rules r join core.skus s on s.sku_id = r.sku_id where s.code_norm like $1)::int as lz,
    (select count(*) from core.products where display_code = $2)::int as groups`, [`${group.toLowerCase()}-%`, group]);
const lzRows = async (group) => (await q(`select s.code, r.rule, r.value, r.lz_copy_state as st from ops.sku_logizard_barcode_rules r join core.skus s on s.sku_id = r.sku_id
  where s.code_norm like $1 order by s.code`, [`${group.toLowerCase()}-%`])).map((x) => [x.code, x.rule, x.value, x.st]);

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
console.log('まとまりの登録のロジザードのバーコード');
const JA = jan13('458777700001'), JB = jan13('458777700002');

await ta('[K1] まとまりの確かめ: 打った値の持ち主 (Company DB のほかの商品の JAN・ロジザードの写し) / 子の値がほかの子の JAN と同じ = lz_kid_clash (200・まとまりの API が開いている = 素通りしない)', async () => {
  const r = await post('/api/variation/check', { codes: [], jans: [], lz_values: [TAKEN_JAN, 'x00lzvware', 'X00LZV0001', 'bad'],
    lz_children: [{ code: 'k-A', jan: JA, lz: JB }, { code: 'k-B', jan: JB, lz: 'X00LZV0001' }, { code: 'k-C', jan: '', lz: JA }] });
  assert.equal(r.status, 200, r.text);
  assert.deepEqual(r.j.lz, { [TAKEN_JAN]: 's003', X00LZVWARE: 'zz-old' });
  assert.deepEqual(r.j.lz_kid_clash, { 'k-A': 'k-B', 'k-C': 'k-A' });
});
await ta('[O1] 子の値がほかの子の JAN と同じ = 400 lzbc_dup (子の順番を両方の向き)・何も作らない (まとまり・SKU・JAN・バーコードの行)', async () => {
  for (const [g, kids] of [['lzo1', [['-A', '', { kind: 'value', value: JB }], ['-B', JB, { kind: 'value', value: 'X00LZO0001' }]]],
    ['lzo2', [['-A', JB, { kind: 'value', value: 'X00LZO0002' }], ['-B', '', { kind: 'value', value: JB }]]]]) {
    const r = await post('/api/new/variation', body(g, kids));
    assert.deepEqual([r.status, r.j && r.j.reason], [400, 'lzbc_dup'], `${g}: ${r.text}`);
    assert.match(r.j.error, /の JAN と同じです/);
    assert.deepEqual(await counts(g), { skus: 0, jans: 0, lz: 0, groups: 0 }, g);
  }
  // 自分の JAN と同じ値 = 通る
  const ok = await post('/api/new/variation', body('lzo3', [['-A', JA, { kind: 'value', value: JA }], ['-B', '', { kind: 'lz_assign' }]]));
  assert.equal(ok.status, 200, ok.text);
  assert.deepEqual(await lzRows('lzo3'), [['lzo3-A', 'jan', JA, 'checked'], ['lzo3-B', 'lz_assign', null, null]]);
});
await ta('[T1] ほかの商品と同じ値 = 409 lzbc_taken (ロジザードの写し / Company DB のほかの商品の JAN = 子の順番を両方の向き)・全部戻る', async () => {
  for (const [g, kids] of [['lzt1', [['-A', '', { kind: 'value', value: 'X00LZVWARE' }], ['-B', '', { kind: 'lz_assign' }]]],
    ['lzt2', [['-A', '', { kind: 'lz_assign' }], ['-B', jan13('458777700009'), { kind: 'value', value: TAKEN_JAN }]]],
    ['lzt3', [['-A', jan13('458777700008'), { kind: 'value', value: TAKEN_JAN }], ['-B', '', { kind: 'lz_assign' }]]]]) {
    const r = await post('/api/new/variation', body(g, kids));
    assert.deepEqual([r.status, r.j && r.j.reason], [409, 'lzbc_taken'], `${g}: ${r.text}`);
    assert.deepEqual(await counts(g), { skus: 0, jans: 0, lz: 0, groups: 0 }, `${g} が残った`);
  }
});
await ta('[C1] ロジザードの写しが読めない / 空 / 古い = 登録は通る (200)・答えに lz_unchecked (写しの時刻)・値の子に印・空欄の子は印なし / 照らせた = 印なし', async () => {
  try {
    for (const [i, st] of ['unavailable', 'empty', 'stale'].entries()) {
      MIRROR = mirrorOf(st);
      const g = `lzc${i + 1}`;
      const r = await post('/api/new/variation', body(g, [['-A', '', { kind: 'value', value: `X00LZC${i}001` }], ['-B', '', { kind: 'lz_assign' }]]));
      assert.equal(r.status, 200, `${st}: ${r.text}`);
      assert.equal(r.j.lz_unchecked && r.j.lz_unchecked.state, st);
      if (st === 'stale') assert.match(r.j.lz_unchecked.as_of_jst, /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$/);
      assert.deepEqual(await lzRows(g), [[`${g}-A`, 'fnsku', `X00LZC${i}001`, st], [`${g}-B`, 'lz_assign', null, null]]);
      assert.deepEqual(r.j.lz_unchecked.codes, [`${g}-A`], '照らせていない子のコード');
    }
    MIRROR = mirrorOf('checked');
    const r = await post('/api/new/variation', body('lzc9', [['-A', '', { kind: 'value', value: 'X00LZC9001' }]]));
    assert.deepEqual([r.status, r.j.lz_unchecked], [200, undefined], r.text);
  } finally { MIRROR = mirrorOf(); }
});
await ta('[C2] 応答が失われた後の再送 (同じ request_id) = 印は DB に残した子の結果から (#1719 R3 High): 古い写しで登録 → 写しが新しくなってから再送 = 印が残る / 照らせた登録 → 写しが読めなくなってから再送 = 印なし', async () => {
  try {
    MIRROR = mirrorOf('stale');
    const b1 = body('lzr1', [['-A', '', { kind: 'value', value: 'X00LZR1001' }], ['-B', '', { kind: 'value', value: 'X00LZR1002' }], ['-C', '', { kind: 'lz_assign' }]]);
    const first = await post('/api/new/variation', b1);   // この応答は届かなかったことにする (画面は同じ request_id で送り直す)
    assert.equal(first.status, 200, first.text);
    MIRROR = mirrorOf('checked');
    const again = await post('/api/new/variation', b1);
    assert.equal(again.status, 200, again.text);
    assert.equal(again.j.replayed, true, again.text);
    assert.deepEqual([again.j.lz_unchecked && again.j.lz_unchecked.state, again.j.lz_unchecked && again.j.lz_unchecked.codes], ['stale', ['lzr1-A', 'lzr1-B']], again.text);
    assert.equal(again.j.lz_unchecked.as_of, first.j.lz_unchecked.as_of, '残した写しの時刻');
    assert.ok(again.j.lz_unchecked.note.includes('GAS の H 列に入れる前に'), again.j.lz_unchecked.note);
    // 逆: 照らせた登録を、写しが読めなくなってから再送 = 印なし (今の写しで印を付けない)
    const b2 = body('lzr2', [['-A', '', { kind: 'value', value: 'X00LZR2001' }]]);
    assert.deepEqual([(await post('/api/new/variation', b2)).j.lz_unchecked], [undefined]);
    MIRROR = mirrorOf('unavailable');
    const again2 = await post('/api/new/variation', b2);
    assert.deepEqual([again2.status, again2.j.replayed, again2.j.lz_unchecked], [200, true, undefined], again2.text);
    // 🆕 #1719 R4 High: 保存できた後に写しに同じ値のほかの商品が現れた → 同じ request_id で再送 = 保存済みの答え (409 にならない)
    MIRROR = mirrorOf('checked');
    const b3 = body('lzr3', [['-A', '', { kind: 'value', value: 'X00LZR3001' }], ['-B', '', { kind: 'lz_assign' }]]);
    const s1 = await post('/api/new/variation', b3);   // この応答は届かなかったことにする
    assert.equal(s1.status, 200, s1.text);
    const withNew = mirrorOf('checked');
    withNew.barcodes.set('X00LZR3001', new Set(['zz-new']));
    MIRROR = withNew;
    const s2 = await post('/api/new/variation', b3);
    assert.deepEqual([s2.status, s2.j.replayed, (s2.j.children || []).map((k) => k.code)], [200, true, ['lzr3-A', 'lzr3-B']], s2.text);
    const b4 = body('lzr4', [['-A', '', { kind: 'value', value: 'X00LZR3001' }]]);
    const s3 = await post('/api/new/variation', b4);
    assert.deepEqual([s3.status, s3.j.reason], [409, 'lzbc_taken'], s3.text);
  } finally { MIRROR = mirrorOf(); }
});

server.close();
console.log(`\n${passed} 件 ok${process.exitCode ? ' (NG あり)' : ''}`);
process.exit(process.exitCode || 0);
