/**
 * SKUマスタCRUD APIのエンドツーエンドテスト
 * Express ルータをメモリ内で起動し、supertest 不在のため fetch 互換で叩く
 */
import express from 'express';
import http from 'http';
import path from 'path';
import os from 'os';
import fs from 'fs';

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'api-test-'));
process.env.DATA_DIR = tmpDir;
process.env.WAREHOUSE_API_KEY = ''; // 認証スキップ

const { initDB, getDB } = await import('../apps/warehouse/db.js');
await initDB();

// テスト用 raw_ne_products シード
const db = getDB();
db.prepare('INSERT INTO raw_ne_products (商品コード, 商品名, 原価) VALUES (?, ?, ?)').run('ne-aaa', 'NE-A', 100);
db.prepare('INSERT INTO raw_ne_products (商品コード, 商品名, 原価) VALUES (?, ?, ?)').run('ne-bbb', 'NE-B', 200);
db.prepare('INSERT INTO raw_ne_products (商品コード, 商品名, 原価) VALUES (?, ?, ?)').run('ne-ccc', 'NE-C', 300);

// router マウント
const router = (await import('../apps/warehouse/router.js')).default;

const app = express();
app.use(express.json());
app.use('/', router);

const server = http.createServer(app);
await new Promise(r => server.listen(0, r));
const port = server.address().port;
const base = `http://127.0.0.1:${port}`;

let pass = 0, fail = 0;
async function check(label, fn) {
  try {
    await fn();
    console.log(`  ✓ ${label}`);
    pass++;
  } catch (e) {
    console.log(`  ✗ ${label}: ${e.message}`);
    fail++;
  }
}

const fetchJSON = async (path, opts = {}) => {
  const res = await fetch(base + path, {
    ...opts,
    headers: { 'content-type': 'application/json', ...(opts.headers || {}) },
  });
  let body = null;
  try { body = await res.json(); } catch {}
  return { status: res.status, body };
};

console.log('\n[1] POST 新規登録');
await check('単品SKU登録 → 201', async () => {
  const r = await fetchJSON('/api/m-sku-master', {
    method: 'POST',
    body: JSON.stringify({
      seller_sku: 'sku-test-1',
      商品名: 'テスト単品',
      components: [{ ne_code: 'ne-aaa', 数量: 1 }],
      user: 'test-user',
    }),
  });
  if (r.status !== 201) throw new Error(`status=${r.status} body=${JSON.stringify(r.body)}`);
});

await check('セットSKU登録 → 201', async () => {
  const r = await fetchJSON('/api/m-sku-master', {
    method: 'POST',
    body: JSON.stringify({
      seller_sku: 'sku-test-set',
      商品名: 'テストセット',
      components: [
        { ne_code: 'ne-aaa', 数量: 1 },
        { ne_code: 'ne-bbb', 数量: 2 },
      ],
    }),
  });
  if (r.status !== 201) throw new Error(`status=${r.status}`);
  if (r.body.components.length !== 2) throw new Error(`components count = ${r.body.components.length}`);
});

console.log('\n[2] バリデーション');
await check('seller_sku空 → 400', async () => {
  const r = await fetchJSON('/api/m-sku-master', {
    method: 'POST', body: JSON.stringify({ seller_sku: '', 商品名: 'X', components: [{ ne_code: 'ne-aaa' }] }),
  });
  if (r.status !== 400) throw new Error(`status=${r.status}`);
});

await check('components空 → 400', async () => {
  const r = await fetchJSON('/api/m-sku-master', {
    method: 'POST', body: JSON.stringify({ seller_sku: 'sku-x', 商品名: 'X', components: [] }),
  });
  if (r.status !== 400) throw new Error(`status=${r.status}`);
});

await check('NE未存在 → 400', async () => {
  const r = await fetchJSON('/api/m-sku-master', {
    method: 'POST', body: JSON.stringify({ seller_sku: 'sku-y', 商品名: 'X', components: [{ ne_code: 'ne-not-exist' }] }),
  });
  if (r.status !== 400) throw new Error(`status=${r.status}`);
  if (!r.body.error.includes('存在しません')) throw new Error(`expected 存在しません, got: ${r.body.error}`);
});

await check('構成重複 → 400', async () => {
  const r = await fetchJSON('/api/m-sku-master', {
    method: 'POST', body: JSON.stringify({ seller_sku: 'sku-z', 商品名: 'X', components: [{ ne_code: 'ne-aaa' }, { ne_code: 'ne-aaa' }] }),
  });
  if (r.status !== 400) throw new Error(`status=${r.status}`);
});

await check('seller_sku重複登録 → 409', async () => {
  const r = await fetchJSON('/api/m-sku-master', {
    method: 'POST', body: JSON.stringify({ seller_sku: 'sku-test-1', 商品名: 'X', components: [{ ne_code: 'ne-aaa' }] }),
  });
  if (r.status !== 409) throw new Error(`status=${r.status}`);
});

await check('大文字SKU → 自動正規化されて409 (既に小文字で登録済み)', async () => {
  const r = await fetchJSON('/api/m-sku-master', {
    method: 'POST', body: JSON.stringify({ seller_sku: 'SKU-TEST-1', 商品名: 'X', components: [{ ne_code: 'ne-aaa' }] }),
  });
  if (r.status !== 409) throw new Error(`status=${r.status}`);
});

console.log('\n[3] GET');
await check('一覧取得', async () => {
  const r = await fetchJSON('/api/m-sku-master');
  if (r.status !== 200) throw new Error(`status=${r.status}`);
  if (r.body.total < 2) throw new Error(`total=${r.body.total}`);
});

await check('検索（q=set）', async () => {
  const r = await fetchJSON('/api/m-sku-master?q=set');
  if (r.status !== 200) throw new Error(`status=${r.status}`);
  if (r.body.items.length === 0) throw new Error('no hits for set');
});

await check('単票取得', async () => {
  const r = await fetchJSON('/api/m-sku-master/sku-test-set');
  if (r.status !== 200) throw new Error(`status=${r.status}`);
  if (!r.body.exists) throw new Error('not exists');
  if (r.body.components.length !== 2) throw new Error(`components=${r.body.components.length}`);
});

await check('単票取得（大文字でも引ける）', async () => {
  const r = await fetchJSON('/api/m-sku-master/SKU-TEST-1');
  if (r.status !== 200) throw new Error(`status=${r.status}`);
  if (!r.body.exists) throw new Error('not exists');
});

await check('未登録 → 404', async () => {
  const r = await fetchJSON('/api/m-sku-master/totally-unknown');
  if (r.status !== 404) throw new Error(`status=${r.status}`);
});

console.log('\n[4] PUT 更新');
await check('商品名と構成の更新', async () => {
  const r = await fetchJSON('/api/m-sku-master/sku-test-set', {
    method: 'PUT',
    body: JSON.stringify({
      商品名: 'テストセット 更新後',
      components: [
        { ne_code: 'ne-bbb', 数量: 3 },
        { ne_code: 'ne-ccc', 数量: 1 },
      ],
      user: 'updater',
    }),
  });
  if (r.status !== 200) throw new Error(`status=${r.status} ${JSON.stringify(r.body)}`);
  if (r.body.master.商品名 !== 'テストセット 更新後') throw new Error('name not updated');
  if (r.body.components.length !== 2) throw new Error(`components=${r.body.components.length}`);
  // 古い ne-aaa は消えてる、ne-ccc が増えている
  const codes = r.body.components.map(c => c.ne_code).sort();
  if (codes.join(',') !== 'ne-bbb,ne-ccc') throw new Error(`codes=${codes}`);
});

console.log('\n[5] 関連SKU');
// sku-test-set は ne-bbb, ne-ccc を持つ。sku-test-1 は ne-aaa。共通なし → 0件
// 別のSKUを追加して検証
await fetchJSON('/api/m-sku-master', {
  method: 'POST',
  body: JSON.stringify({
    seller_sku: 'sku-related',
    商品名: '関連テスト',
    components: [{ ne_code: 'ne-bbb', 数量: 1 }],
  }),
});
await check('related エンドポイント', async () => {
  const r = await fetchJSON('/api/m-sku-master/sku-test-set/related');
  if (r.status !== 200) throw new Error(`status=${r.status}`);
  if (r.body.items.length === 0) throw new Error('no related');
  if (!r.body.items.some(i => i.seller_sku === 'sku-related')) throw new Error('sku-related not in result');
});

console.log('\n[6] DELETE');
await check('削除 → 204', async () => {
  const r = await fetch(base + '/api/m-sku-master/sku-test-1', { method: 'DELETE' });
  if (r.status !== 204) throw new Error(`status=${r.status}`);
});

await check('削除後404', async () => {
  const r = await fetchJSON('/api/m-sku-master/sku-test-1');
  if (r.status !== 404) throw new Error(`status=${r.status}`);
});

await check('CASCADE削除確認 (componentsも消えてる)', async () => {
  const r = await fetchJSON('/api/m-sku-master/sku-test-set');
  if (r.status !== 200) throw new Error(`status=${r.status}`);
  // 削除して再確認
  await fetch(base + '/api/m-sku-master/sku-test-set', { method: 'DELETE' });
  const cnt = db.prepare("SELECT COUNT(*) c FROM m_sku_components WHERE seller_sku='sku-test-set'").get().c;
  if (cnt !== 0) throw new Error(`components remain: ${cnt}`);
});

server.close();
db.close();
fs.rmSync(tmpDir, { recursive: true, force: true });

console.log(`\n結果: ${pass} pass / ${fail} fail`);
process.exit(fail > 0 ? 1 : 0);
