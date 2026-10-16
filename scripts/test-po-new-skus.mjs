/**
 * test-po-new-skus.mjs — 発注アプリが Company DB の新商品 (新商品の登録で作った・NE 登録の前) を読む SQL (apps/purchase-orders/new-skus.js loadNewSkus)
 *
 * 固定する契約:
 *   1 画面のロール master_edit で読める (表と列の権限)・origin = new_entry の単品だけ (backfill・NE で直接作った商品は読まない)
 *   2 名前・今の原価・代表の仕入先 (4 桁)・登録の状態 (やめた商品も読む = 画面で外す)・ロジザード登録の CSV に出したか (0081 の後)
 *   3 newSkuMap: 照合キー (小文字) ごと・仕入先は発注アプリの形 (0001 → 1)・大文字小文字だけ違う 2 つ = ambiguous
 * HTTP の入口 (対応表・入荷予定の変換) の試験は apps/purchase-orders/scripts/smoke.mjs の「新商品 (PMLに無い)」
 * 使い方: node scripts/test-po-new-skus.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';

const killer = setTimeout(() => { console.error('時間切れ (300 秒)'); process.exit(3); }, 300e3);
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);
const { setupVariationDb } = await import('./fixtures/master-variation-db.mjs');
const NS = await import('../apps/purchase-orders/new-skus.js');
const MR = await import('../lib/master-register.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }

console.log('test-po-new-skus');
const T = await setupVariationDb();
const { pg, db, asEditor } = T;
/** 登録の状態を試験のために変える (持ち主のロールの手の DML = 印が要る。scripts/test-amazon-map.mjs の setReg と同じ) */
async function setReg(code, state) {
  await pg.query(`select set_config('ops.registration_protocol', '1', false)`);
  try { await pg.query('update ops.master_registrations set state = $2 where sku_id = (select sku_id from core.skus where code = $1)', [code, state]); }
  finally { await pg.query(`select set_config('ops.registration_protocol', '', false)`); }
}
const VALUES = { standard_price: '3980', tax_rate: '0.1', sales_class: '3', primary_supplier: '0001', reorder_months: '2', expiry_managed: '0', inbound_date_managed: '0', shipping_code: 'B2' };
/** 本物の新商品の登録 (lib・画面のロール) で単品を作る = origin new_entry・下書き */
const register = (code, name, cost) => asEditor(() => MR.registerNewSku(db, { actor: 'naka@test', requestId: crypto.randomUUID(), kind: 'single', code,
  values: { ...VALUES, name, cost: { jpy: cost } }, card: { create: false } }, { ownership: T.ALL_COMPANY, open: true, shippingRates: new Map([['B2', { method: '宅急便コンパクト', cost: 520 }]]) }));

await ta('[1] 画面のロールで読める・origin = new_entry の単品だけ・名前 / 原価 / 代表の仕入先 / 状態 / CSV に出したか', async () => {
  let rows = await asEditor(() => NS.loadNewSkus(db));
  assert.deepEqual(rows, [], 'backfill の商品 (s001 など) は新商品ではない');
  await register('NewPo-01', '新しい商品 1', '1650');
  await register('NewPo-02', '新しい商品 2', '700');
  await setReg('NewPo-02', 'cancelled');
  rows = await asEditor(() => NS.loadNewSkus(db));
  assert.deepEqual(rows.map((r) => [r.code, r.name, r.state, Number(r.cost), r.supplier, r.lz_issued]),
    [['NewPo-01', '新しい商品 1', 'draft', 1650, '0001', false], ['NewPo-02', '新しい商品 2', 'cancelled', 700, '0001', false]]);
});

await ta('[2] newSkuMap: 照合キーごと・仕入先は発注アプリの形・原価は数・大文字小文字だけ違う 2 つ = ambiguous', async () => {
  const rows = await asEditor(() => NS.loadNewSkus(db));
  const m = NS.newSkuMap(rows);
  assert.deepEqual(m.get('newpo-01'), { code: 'NewPo-01', name: '新しい商品 1', state: 'draft', cost: 1650, supplier: '1', lzIssued: false });
  assert.equal(m.get('newpo-02').state, 'cancelled');
  const m2 = NS.newSkuMap([{ code: 'Ab-1', name: 'x', state: 'draft', cost: null, supplier: '0002', lz_issued: null }, { code: 'AB-1', name: 'y', state: 'draft', cost: '5', supplier: '2', lz_issued: true }]);
  assert.equal(m2.get('ab-1').ambiguous, true);
  assert.equal(m2.get('ab-1').lzIssued, null);
});

await ta('[3] checkNewSku: 使える / やめた / 要確認 / 仕入先が違う / 仕入先なし / 2 つ / 新商品でない', async () => {
  const ns = { ok: true, map: NS.newSkuMap([
    { code: 'A-1', name: 'a', state: 'draft', cost: '1', supplier: '0002' },
    { code: 'B-1', name: 'b', state: 'cancelled', cost: '1', supplier: '2' },
    { code: 'C-1', name: 'c', state: 'quarantined', cost: '1', supplier: '2' },
    { code: 'D-1', name: 'd', state: 'ne_pending', cost: '1', supplier: null },
    { code: 'E-1', name: 'e', state: 'draft', cost: '1', supplier: '2' }, { code: 'e-1', name: 'e2', state: 'draft', cost: '1', supplier: '2' },
  ]) };
  assert.equal(NS.checkNewSku(ns, 'a-1', '2').sku.code, 'A-1');
  assert.equal(NS.checkNewSku(ns, 'a-1', null).sku.code, 'A-1', '仕入先を比べない呼び方 (紐づけ = 呼び手が仮登録の仕入先と比べる)');
  assert.match(NS.checkNewSku(ns, 'a-1', '3').error, /代表の仕入先は 2/);
  assert.match(NS.checkNewSku(ns, 'b-1', '2').error, /登録をやめた/);
  assert.match(NS.checkNewSku(ns, 'c-1', '2').error, /要確認/);
  assert.match(NS.checkNewSku(ns, 'd-1', '2').error, /代表の仕入先が未設定/);
  assert.match(NS.checkNewSku(ns, 'e-1', '2').error, /大文字・小文字だけ違う/);
  assert.deepEqual(NS.checkNewSku(ns, 'zz-1', '2'), { sku: null, error: null });
  assert.deepEqual(NS.checkNewSku({ ok: false, reason: 'timeout', map: new Map() }, 'a-1', '2'), { sku: null, error: null }, '読めない = 新商品でない扱い (今の動き)');
});

await ta('[4] 読み手: Company DB の URL が無い = not_configured (投げない)', async () => {
  const saved = [process.env.COMPANY_DB_URL, process.env.COMPANY_DB_MASTER_EDIT_URL];
  delete process.env.COMPANY_DB_URL; delete process.env.COMPANY_DB_MASTER_EDIT_URL;
  try {
    const r = await NS.readNewSkus();
    assert.deepEqual([r.ok, r.reason, r.map.size], [false, 'not_configured', 0]);
  } finally {
    if (saved[0] != null) process.env.COMPANY_DB_URL = saved[0];
    if (saved[1] != null) process.env.COMPANY_DB_MASTER_EDIT_URL = saved[1];
  }
});

clearTimeout(killer);
console.log(`\n${passed} passed${process.exitCode ? ' (失敗あり)' : ''}`);
process.exit(process.exitCode || 0);
