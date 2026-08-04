/**
 * セット商品の消費税率解決 — テスト
 *
 * 実行: node scripts/test-set-tax-rate.mjs
 *   (DATA_DIR 未指定時は一時ディレクトリを作って使う)
 *
 * 背景: 構成品の NE 消費税率が未登録 (0) で product_tax_rate による手動登録で
 *   救済されている場合、セット商品側だけ税率が NULL に落ちていた (2026-08-04 発覚)。
 *   単品は resolveTaxRate 経由なので救済され、セットは NE 生値だけ見ていたための非対称。
 *
 * 検証項目:
 *   1. resolveSetTaxRate 単体: 手動税率フォールバック / MIXED / 未解決時 UNKNOWN
 *   2. rebuildMProducts 統合: 構成品が手動税率のみのセットに税率が入る
 *      (NE 生値が正しいセット・混在セット・未解決セットの既存挙動は変わらない)
 *   3. API 即時反映: 単品の税率を登録/削除すると親セットの m_products も追従する
 */
import fs from 'fs';
import os from 'os';
import path from 'path';
import http from 'http';
import assert from 'assert';
import express from 'express';

let ownsDataDir = false;
if (!process.env.DATA_DIR) {
  process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'set-tax-test-'));
  ownsDataDir = true;
}
console.log('DATA_DIR =', process.env.DATA_DIR);

const { initDB, getDB } = await import('../apps/warehouse/db.js');
const { resolveSetTaxRate, rebuildMProducts } = await import('../apps/warehouse/rebuild-m-products.js');

let pass = 0, fail = 0;
async function check(name, fn) {
  try { await fn(); console.log(`  ✅ ${name}`); pass++; }
  catch (e) { console.log(`  ❌ ${name}\n     ${e.message}`); fail++; }
}

// ─── 1. resolveSetTaxRate 単体 ───
console.log('\n[1] resolveSetTaxRate');

await check('NE税率のみで解決 (10%)', () => {
  assert.deepStrictEqual(resolveSetTaxRate([{ neTaxRate: 10, manualTaxRate: undefined }]),
    { taxRate: 0.1, taxCategory: 'STANDARD_10' });
});

await check('NE未登録(0)でも手動税率で解決 (8%) ← 本件の修正点', () => {
  assert.deepStrictEqual(resolveSetTaxRate([{ neTaxRate: 0, manualTaxRate: 0.08 }]),
    { taxRate: 0.08, taxCategory: 'REDUCED_8' });
});

await check('NE null でも手動税率で解決 (10%)', () => {
  assert.deepStrictEqual(resolveSetTaxRate([{ neTaxRate: null, manualTaxRate: 0.1 }]),
    { taxRate: 0.1, taxCategory: 'STANDARD_10' });
});

await check('複数構成品が同一税率 → その税率', () => {
  assert.deepStrictEqual(resolveSetTaxRate([
    { neTaxRate: 0, manualTaxRate: 0.1 },
    { neTaxRate: 10, manualTaxRate: undefined },
  ]), { taxRate: 0.1, taxCategory: 'STANDARD_10' });
});

await check('8%と10%の混在 → MIXED かつ最小値(軽減税率優先)', () => {
  assert.deepStrictEqual(resolveSetTaxRate([
    { neTaxRate: 10, manualTaxRate: undefined },
    { neTaxRate: 0, manualTaxRate: 0.08 },
  ]), { taxRate: 0.08, taxCategory: 'MIXED' });
});

await check('構成品が1つでも解決不能 → UNKNOWN (上流異常を握り潰さない)', () => {
  assert.deepStrictEqual(resolveSetTaxRate([
    { neTaxRate: 10, manualTaxRate: undefined },
    { neTaxRate: 0, manualTaxRate: undefined },
  ]), { taxRate: null, taxCategory: 'UNKNOWN' });
});

await check('NE が想定外値 → 手動があっても UNKNOWN', () => {
  assert.deepStrictEqual(resolveSetTaxRate([{ neTaxRate: 5, manualTaxRate: 0.1 }]),
    { taxRate: null, taxCategory: 'UNKNOWN' });
});

await check('構成品ゼロ → UNKNOWN', () => {
  assert.deepStrictEqual(resolveSetTaxRate([]), { taxRate: null, taxCategory: 'UNKNOWN' });
});

// ─── 2. rebuildMProducts 統合 ───
console.log('\n[2] rebuildMProducts (セット税率の反映)');

const db = await initDB();
const NOW = '2026-08-04 00:00:00';

// 単品 (raw_ne_products): 税率 0 = NE未登録 のものと、NE に正しく入っているもの
db.exec('DELETE FROM raw_ne_products; DELETE FROM raw_ne_set_products; DELETE FROM product_tax_rate;');
const insNe = db.prepare(`INSERT INTO raw_ne_products
  (商品コード, 商品名, 原価, 売価, 取扱区分, 消費税率, 在庫数, 引当数, synced_at)
  VALUES (?, ?, ?, ?, '取扱中', ?, 0, 0, ?)`);
insNe.run('oat1kg',   'オートミール 1kg',  648, 1280, 0,  NOW); // NE未登録 → 手動 8%
insNe.run('brass',    '真鍮ペーパーウェイト', 350, 780,  0,  NOW); // NE未登録 → 手動 10%
insNe.run('normal10', '通常商品',           100, 500,  10, NOW); // NE に 10%
insNe.run('nowhere',  '税率不明商品',        200, 900,  0,  NOW); // NE未登録・手動もなし

// 手動税率 (product_tax_rate)
const insTax = db.prepare('INSERT INTO product_tax_rate (sku, tax_rate, 商品名, synced_at) VALUES (?, ?, ?, ?)');
insTax.run('oat1kg', 0.08, 'オートミール 1kg', NOW);
insTax.run('brass',  0.1,  '真鍮ペーパーウェイト', NOW);

// セット (raw_ne_set_products)
const insSet = db.prepare(`INSERT INTO raw_ne_set_products
  (セット商品コード, セット商品名, セット販売価格, 商品コード, 数量, セット在庫数, 代表商品コード, synced_at)
  VALUES (?, ?, ?, ?, ?, 0, '', ?)`);
insSet.run('oat1kg-4',  'オートミール 4個セット', 4380, 'oat1kg',   4, NOW); // 手動8%のみ
insSet.run('brass-2',   '真鍮 2個セット',        1380, 'brass',    2, NOW); // 手動10%のみ
insSet.run('mix-set',   '混在セット',            2000, 'oat1kg',   1, NOW); // 8%
insSet.run('mix-set',   '混在セット',            2000, 'normal10', 1, NOW); // + 10% → MIXED
insSet.run('ne-set',    'NE税率セット',          1000, 'normal10', 2, NOW); // NE値のみ
insSet.run('broken-set','税率不明セット',        1000, 'nowhere',  2, NOW); // 解決不能

await rebuildMProducts();

// 検証は m_products_staging に対して行う。
// rebuild は「総件数3,000件未満なら反映中止」の品質ゲートを持つため、テスト規模の
// 数件では本番テーブルに反映されない。税率の算出結果は staging に入っているので、
// ゲートを緩めずに (= 本番の安全弁をテスト都合で動かさずに) 算出ロジックを検証する。
const getMp = (code) => db.prepare('SELECT 消費税率, 税区分, 商品区分 FROM m_products_staging WHERE 商品コード = ?').get(code);

await check('手動税率のみの構成品を持つセットに 8% が入る (回帰対象)', () => {
  const r = getMp('oat1kg-4');
  assert.strictEqual(r?.商品区分, 'セット');
  assert.strictEqual(r.消費税率, 0.08);
  assert.strictEqual(r.税区分, 'REDUCED_8');
});

await check('手動税率のみの構成品を持つセットに 10% が入る (回帰対象)', () => {
  const r = getMp('brass-2');
  assert.strictEqual(r.消費税率, 0.1);
  assert.strictEqual(r.税区分, 'STANDARD_10');
});

await check('NE税率のみのセットは従来どおり 10%', () => {
  const r = getMp('ne-set');
  assert.strictEqual(r.消費税率, 0.1);
  assert.strictEqual(r.税区分, 'STANDARD_10');
});

await check('8%と10%が混在するセットは MIXED / 0.08', () => {
  const r = getMp('mix-set');
  assert.strictEqual(r.消費税率, 0.08);
  assert.strictEqual(r.税区分, 'MIXED');
});

await check('構成品の税率が解決できないセットは UNKNOWN のまま', () => {
  const r = getMp('broken-set');
  assert.strictEqual(r.消費税率, null);
  assert.strictEqual(r.税区分, 'UNKNOWN');
});

await check('単品側の税率解決は従来どおり (手動フォールバック)', () => {
  assert.strictEqual(getMp('oat1kg').消費税率, 0.08);
  assert.strictEqual(getMp('normal10').消費税率, 0.1);
  assert.strictEqual(getMp('nowhere').消費税率, null);
});

// ─── 3. API 即時反映 (POST/DELETE /api/tax_rate → 親セットも追従) ───
console.log('\n[3] /api/tax_rate の親セット即時反映');

// staging は品質ゲートで本番反映されないので、API テスト用に m_products /
// m_set_components を直接シードする (rebuild 後の状態を模した最小構成)
db.exec('DELETE FROM m_products; DELETE FROM m_set_components; DELETE FROM product_tax_rate;');
const insMp = db.prepare(`INSERT INTO m_products
  (商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 消費税率, 税区分, updated_at)
  VALUES (?, ?, ?, '取扱中', 'COMPLETE', NULL, 'UNKNOWN', ?)`);
insMp.run('oat1kg', 'オートミール 1kg', '単品', NOW);
insMp.run('oat1kg-4', 'オートミール 4個セット', 'セット', NOW);
db.prepare(`INSERT INTO m_set_components
  (セット商品コード, 構成商品コード, 数量, 構成商品名, 構成商品原価, updated_at)
  VALUES (?, ?, ?, ?, ?, ?)`).run('oat1kg-4', 'oat1kg', 4, 'オートミール 1kg', 648, NOW);

process.env.WAREHOUSE_API_KEY = ''; // 認証スキップ
const router = (await import('../apps/warehouse/router.js')).default;
const app = express();
app.use(express.json());
app.use('/', router);
const server = http.createServer(app);
await new Promise(r => server.listen(0, r));
const base = `http://127.0.0.1:${server.address().port}`;

const api = async (path, opts = {}) => {
  const res = await fetch(base + path, {
    ...opts, headers: { 'content-type': 'application/json', ...(opts.headers || {}) },
  });
  let body = null;
  try { body = await res.json(); } catch {}
  return { status: res.status, body };
};
const mp = (code) => db.prepare('SELECT 消費税率, 税区分 FROM m_products WHERE 商品コード = ?').get(code);

await check('単品に 8% を登録 → 親セットも 8% に即時反映される', async () => {
  const r = await api('/api/tax_rate', {
    method: 'POST', body: JSON.stringify({ sku: 'oat1kg', tax_rate: 0.08 }),
  });
  assert.strictEqual(r.status, 200, `status=${r.status} body=${JSON.stringify(r.body)}`);
  assert.strictEqual(r.body.sets_updated, 1);
  assert.strictEqual(mp('oat1kg').消費税率, 0.08);
  assert.strictEqual(mp('oat1kg-4').消費税率, 0.08);
  assert.strictEqual(mp('oat1kg-4').税区分, 'REDUCED_8');
});

await check('単品の税率を削除 → 親セットも UNKNOWN に戻る', async () => {
  const r = await api('/api/tax_rate/oat1kg', { method: 'DELETE' });
  assert.strictEqual(r.status, 200);
  assert.strictEqual(r.body.sets_updated, 1);
  assert.strictEqual(mp('oat1kg').消費税率, null);
  assert.strictEqual(mp('oat1kg-4').消費税率, null);
  assert.strictEqual(mp('oat1kg-4').税区分, 'UNKNOWN');
});

await check('セット商品コードへの直接登録は従来どおり拒否される', async () => {
  const r = await api('/api/tax_rate', {
    method: 'POST', body: JSON.stringify({ sku: 'oat1kg-4', tax_rate: 0.08 }),
  });
  assert.strictEqual(r.status, 400);
});

await new Promise(r => server.close(r));
db.close(); // 先に閉じないと Windows で libuv の assert (UV_HANDLE_CLOSING) を踏む
if (ownsDataDir) { try { fs.rmSync(process.env.DATA_DIR, { recursive: true, force: true }); } catch {} }

console.log(`\n${fail === 0 ? '✅ 全パス' : '❌ 失敗あり'}: ${pass} passed, ${fail} failed`);
process.exitCode = fail === 0 ? 0 : 1;
