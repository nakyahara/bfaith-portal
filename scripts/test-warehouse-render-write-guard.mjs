/**
 * Render では /apps/warehouse の書き込みを断る (Company DB構想 10 §9 A) の試験
 *
 * Render の DATA_DIR にある warehouse.db は、m_products の作り直し (miniPC) にも Render への写しにも使われない。
 * そこへ送料・原価などを書いても誰にも届かないので、書き込みは 409 で断り、画面には案内を出す。
 * miniPC (RENDER 未設定) では今までどおり書ける。
 *
 * 使い方: node scripts/test-warehouse-render-write-guard.mjs
 */
import express from 'express';
import http from 'http';
import path from 'path';
import os from 'os';
import fs from 'fs';

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'wh-render-guard-'));
process.env.DATA_DIR = tmpDir;
process.env.WAREHOUSE_API_KEY = ''; // 認証スキップ
const savedRender = process.env.RENDER;
delete process.env.RENDER;

const { initDB, getDB } = await import('../apps/warehouse/db.js');
await initDB();
const db = getDB();
db.prepare('INSERT INTO raw_ne_products (商品コード, 商品名, 原価) VALUES (?, ?, ?)').run('ne-aaa', 'NE-A', 100);

const mod = await import('../apps/warehouse/router.js');
const router = mod.default;

const app = express();
app.use(express.json());
app.use('/', router);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, r));
const base = `http://127.0.0.1:${server.address().port}`;

// router の DB 初期化 (非同期) が終わるまで待つ
for (let i = 0; i < 100 && !mod.isWarehouseDbReady(); i++) await new Promise((r) => setTimeout(r, 20));

let pass = 0, fail = 0;
async function check(label, fn) {
  try { await fn(); console.log(`  ✓ ${label}`); pass++; } catch (e) { console.log(`  ✗ ${label}: ${e.message}`); fail++; }
}
const req = async (p, opts = {}) => {
  const res = await fetch(base + p, { ...opts, headers: { 'content-type': 'application/json', ...(opts.headers || {}) } });
  const text = await res.text();
  let body = null; try { body = JSON.parse(text); } catch { body = text; }
  return { status: res.status, body };
};
const assert = (cond, msg) => { if (!cond) throw new Error(msg); };

// 書き込みの口 (router.js の POST / DELETE と、同じ router に載る SKU マスタ API) を代表で並べる
const WRITES = [
  ['POST', '/api/shipping', { sku: 'ne-aaa', shipping_code: 'x' }],
  ['POST', '/api/genka', { sku: 'ne-aaa', genka: 120 }],
  ['POST', '/api/tax_rate', { sku: 'ne-aaa', tax_rate: '10' }],
  ['POST', '/api/sales_class', { sku: 'ne-aaa', sales_class: 1 }],
  ['POST', '/api/reorder_setting', { sku: 'ne-aaa', 推奨保有月数: 3 }],
  ['DELETE', '/api/genka/ne-aaa', null],
  ['POST', '/api/m-sku-master', { seller_sku: 'sku-x', 商品名: 'X', components: [{ ne_code: 'ne-aaa', 数量: 1 }] }],
  ['PUT', '/api/m-sku-master/sku-x', { 商品名: 'X2', components: [{ ne_code: 'ne-aaa', 数量: 1 }] }],
  ['POST', '/api/csv/shipping', null],
];

console.log('\n[1] Render (RENDER=true) では書き込みを断る');
process.env.RENDER = 'true';
for (const [method, p, body] of WRITES) {
  await check(`${method} ${p} → 409 WAREHOUSE_WRITE_ON_RENDER`, async () => {
    const r = await req(p, { method, body: body ? JSON.stringify(body) : undefined });
    assert(r.status === 409, `status=${r.status} body=${JSON.stringify(r.body).slice(0, 120)}`);
    assert(r.body?.code === 'WAREHOUSE_WRITE_ON_RENDER', `code=${r.body?.code}`);
    assert(String(r.body?.error || '').includes(mod.MASTER_REGISTER_URL), '案内の URL が無い');
  });
}
await check('断った後も DB に何も書かれていない', async () => {
  const n1 = db.prepare('SELECT COUNT(*) c FROM exception_genka').get().c;
  const n2 = db.prepare('SELECT COUNT(*) c FROM product_shipping').get().c;
  const n3 = db.prepare('SELECT COUNT(*) c FROM m_sku_master').get().c;
  assert(n1 === 0 && n2 === 0 && n3 === 0, `genka=${n1} shipping=${n2} sku_master=${n3}`);
});
await check('読むだけの GET は通る (/api/missing/counts → 200)', async () => {
  const r = await req('/api/missing/counts');
  assert(r.status === 200, `status=${r.status}`);
});
await check('マスタ登録の画面に Render 版の案内が出る', async () => {
  const r = await req('/register');
  assert(r.status === 200, `status=${r.status}`);
  assert(String(r.body).includes('この画面は Render 版です'), '案内が無い');
  assert(String(r.body).includes(mod.MASTER_REGISTER_URL), '案内の URL が無い');
});
await check('ダッシュボードにも案内が出る', async () => {
  const r = await req('/');
  assert(r.status === 200, `status=${r.status}`);
  assert(String(r.body).includes('この画面は Render 版です'), '案内が無い');
});
await check('RENDER の打ち間違い (tru) は Render 扱いにしない = 書ける', async () => {
  process.env.RENDER = 'tru';
  const r = await req('/api/genka', { method: 'POST', body: JSON.stringify({ sku: 'ne-aaa', genka: 120, product_name: 'NE-A' }) });
  process.env.RENDER = 'true';
  assert(r.status !== 409, `status=${r.status}`);
  db.prepare('DELETE FROM exception_genka').run();
});

console.log('\n[2] miniPC (RENDER 未設定) では今までどおり書ける');
delete process.env.RENDER;
await check('POST /api/genka → 409 にならず登録される', async () => {
  const r = await req('/api/genka', { method: 'POST', body: JSON.stringify({ sku: 'ne-aaa', genka: 150, product_name: 'NE-A' }) });
  assert(r.status !== 409, `status=${r.status} body=${JSON.stringify(r.body).slice(0, 120)}`);
  const row = db.prepare("SELECT genka FROM exception_genka WHERE sku = 'ne-aaa'").get();
  assert(row && Number(row.genka) === 150, `row=${JSON.stringify(row)}`);
});
await check('POST /api/m-sku-master → 201', async () => {
  const r = await req('/api/m-sku-master', { method: 'POST', body: JSON.stringify({ seller_sku: 'sku-y', 商品名: 'Y', components: [{ ne_code: 'ne-aaa', 数量: 1 }] }) });
  assert(r.status === 201, `status=${r.status} body=${JSON.stringify(r.body).slice(0, 120)}`);
});
await check('画面に Render 版の案内は出ない', async () => {
  const r = await req('/register');
  assert(!String(r.body).includes('この画面は Render 版です'), '案内が出ている');
});

server.close();
db.close();
// Windows では router 側の保存タイマーがファイルを掴んだままのことがある。後片付けの失敗は結果に含めない
try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* 一時フォルダは OS に任せる */ }
if (savedRender === undefined) delete process.env.RENDER; else process.env.RENDER = savedRender;

console.log(`\n結果: ${pass} pass / ${fail} fail`);
process.exit(fail > 0 ? 1 : 0);
