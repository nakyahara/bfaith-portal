/**
 * site-products API の契約テスト (bfaith-site sync-products.js とのデータ契約)
 *
 * 実行: node scripts/test-site-products.mjs
 *
 * 検証項目:
 *  1. 認証: 未設定→503 (ヘッダ有無に関わらず) / 不一致・空・query・Bearer→401 / 一致→200
 *  2. 契約: products[].code/name/price/stockFree/malls.{rakuten,yahoo,amazon,qoo10,aupay}
 *  3. モールURL解決: 楽天 (snapshot URL優先/フォールバック組み立て)・Yahoo (実在確認時のみ)・
 *     Amazon (sku_resolved→asin)・Qoo10 (seller_code)・auPAY (idのみ・url=null)
 *  4. 境界値: 引当>在庫→stockFree=0 / price・name null
 *  5. fail-soft: prepare後に表をDROPしても該当lookupのみnull降格 (500にしない・degradedLookupsに記録)
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import express from 'express';

// DATA_DIR は import 時にキャプチャされるため、モジュール import より前に設定する (test-shipping-log と同じ)
const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'site-products-test-'));
process.env.DATA_DIR = tmpDir;
delete process.env.SITE_PRODUCTS_READ_TOKEN;

const { initMirrorDB, getMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const db = getMirrorDB();

// ---------- seed ----------
const now = new Date().toISOString();
const insProduct = db.prepare(
  `INSERT INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 標準売価, 原価状態, 在庫数, 引当数, 売上分類, updated_at)
   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
);
insProduct.run(1, 'TEST1', 'テスト商品1', '通常', '取扱', 1980, 'ok', 10, 3, 5, now);
insProduct.run(2, 'TEST2', 'テスト商品2 (モール紐付けなし)', '通常', '取扱', 980, 'ok', 0, 0, 5, now);
insProduct.run(3, 'TEST3', 'テスト商品3 (楽天フォールバックURL+price_snapshot経由ASIN)', '通常', '取扱', 500, 'ok', 5, 0, 5, now);
insProduct.run(4, 'TEST4', null, '通常', '取扱', null, 'ok', 2, 5, 5, now); // 境界: name/price null・引当>在庫

const insRkMap = db.prepare(
  `INSERT INTO mirror_rakuten_sku_map (rakuten_code, ne_code, source, updated_at) VALUES (?, ?, ?, ?)`
);
insRkMap.run('test1-rk', 'TEST1', 'am', now);
insRkMap.run('TEST3-RK', 'TEST3', 'am', now);
insRkMap.run('test1-rk-bk-l', 'TEST4', 'am', now);
insRkMap.run('unknown-item-xyz', 'TEST2', 'am', now); // item_dailyに無い→URLを出さない // バリエーションSKU→親 test1-rk に解決される // 大文字→フォールバックURLはlower化される
// 楽天の商品ページは親商品単位。RMS由来のitem_daily に実在する管理番号だけURL化される
const insItemDaily = db.prepare(
  `INSERT INTO mirror_rakuten_item_daily (date_jst, item_manage_number, source_run_id, source_row_hash, synced_at)
   VALUES (?, ?, ?, ?, ?)`
);
insItemDaily.run('2026-07-30', 'test1-rk', 'r1', 'hd1', now);
insItemDaily.run('2026-07-30', 'TEST3-RK', 'r1', 'hd2', now);
db.prepare(
  `INSERT INTO mirror_rakuten_item_purchaser_snapshot
   (date_jst, item_manage_number, item_name, item_url, source_run_id, source_row_hash, synced_at)
   VALUES (?, ?, ?, ?, ?, ?, ?)`
).run('2026-07-30', 'test1-rk', 'テスト商品1', 'https://item.rakuten.co.jp/b-faith/test1-rk/', 'r1', 'h1', now);
db.prepare(
  `INSERT INTO mirror_yahoo_item_daily
   (date_jst, item_code, sub_code, source_run_id, source_row_hash, synced_at)
   VALUES (?, ?, ?, ?, ?, ?)`
).run('2026-07-30', 'test1-rk', '', 'r1', 'h2', now);
db.prepare(
  `INSERT INTO mirror_sku_resolved (seller_sku, ne_code, quantity, source, synced_at) VALUES (?, ?, ?, ?, ?)`
).run('pr_test1', 'TEST1', 1, 'master', now);
db.prepare(
  `INSERT INTO mirror_amazon_finance_sku_daily
   (date_jst, seller_sku, asin_norm, cost_status, source_run_id, source_row_hash, synced_at)
   VALUES (?, ?, ?, ?, ?, ?, ?)`
).run('2026-07-30', 'pr_test1', 'B0TESTASIN', 'complete', 'r1', 'h4', now);
db.prepare(
  `INSERT INTO mirror_qoo10_items (item_no, seller_code, source_run_id, source_row_hash, synced_at) VALUES (?, ?, ?, ?, ?)`
).run('123456789', 'TEST1', 'r1', 'h3', now);
db.prepare(
  `INSERT INTO mirror_aupay_finance_sku_daily
   (date_jst, aupay_sku_key, ne_code, resolution_method, shipping_quality, cost_status, source_run_id, source_row_hash, synced_at)
   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
).run('2026-07-30', 'aupay-test1', 'TEST1', 'master_match', 'actual', 'complete', 'r1', 'h5', now);

// TEST3: financeに実績が無く、seller_skuの大小が揺れているケース (LOWER(TRIM())突合+price_snapshotフォールバック)
db.prepare(
  `INSERT INTO mirror_sku_resolved (seller_sku, ne_code, quantity, source, synced_at) VALUES (?, ?, ?, ?, ?)`
).run('PR_TEST3_UPPER', 'test3', 1, 'master', now);
db.prepare(
  `INSERT INTO mirror_amazon_price_snapshot_daily
   (date_jst, seller_sku, asin, source_run_id, source_row_hash, synced_at)
   VALUES (?, ?, ?, ?, ?, ?)`
).run('2026-07-30', 'pr_test3_upper', 'B0PRICESNAP', 'r1', 'h6', now);

// ---------- server ----------
const { default: router } = await import('../apps/site-products/router.js');
const app = express();
app.use('/apps/site-products/api', router);
const server = app.listen(0);
const base = `http://127.0.0.1:${server.address().port}/apps/site-products/api`;

let pass = 0;
let fail = 0;
function ok(name, cond, detail = '') {
  if (cond) {
    pass++;
    console.log(`  ✓ ${name}`);
  } else {
    fail++;
    console.error(`  ✗ ${name} ${detail}`);
  }
}

// 1. 認証 — 未設定は「正しそうなヘッダがあっても」必ず503 (fail-closed)
{
  const r1 = await fetch(`${base}/products`);
  ok('token未設定 (ヘッダなし) → 503', r1.status === 503, `got ${r1.status}`);
  const r2 = await fetch(`${base}/products`, { headers: { 'x-read-token': 'anything' } });
  ok('token未設定 (ヘッダあり) → 503', r2.status === 503, `got ${r2.status}`);
}
process.env.SITE_PRODUCTS_READ_TOKEN = 'test-token-123';
{
  const cases = [
    ['不一致', `${base}/products`, { 'x-read-token': 'wrong' }],
    ['空ヘッダ', `${base}/products`, { 'x-read-token': '' }],
    ['queryは不可', `${base}/products?token=test-token-123`, {}],
    ['Bearerは不可', `${base}/products`, { authorization: 'Bearer test-token-123' }],
  ];
  for (const [name, url, headers] of cases) {
    const r = await fetch(url, { headers });
    ok(`${name} → 401`, r.status === 401, `got ${r.status}`);
  }
}
const res = await fetch(`${base}/products`, { headers: { 'x-read-token': 'test-token-123' } });
ok('token一致 → 200', res.status === 200, `got ${res.status}`);
ok('Cache-Control: no-store', res.headers.get('cache-control') === 'no-store');

// 2. 契約
const body = await res.json();
ok('ok=true / count=4', body.ok === true && body.count === 4, JSON.stringify({ ok: body.ok, count: body.count }));
ok('degradedLookups空 (全表あり)', Array.isArray(body.degradedLookups) && body.degradedLookups.length === 0, JSON.stringify(body.degradedLookups));
const p1 = body.products.find((p) => p.code === 'TEST1');
const p2 = body.products.find((p) => p.code === 'TEST2');
const p3 = body.products.find((p) => p.code === 'TEST3');
const p4 = body.products.find((p) => p.code === 'TEST4');
ok('TEST1が返る', !!p1);
ok(
  'Amazon: price_snapshot経由+SKU大小ゆれでも解決',
  p3?.malls?.amazon?.url === 'https://www.amazon.co.jp/dp/B0PRICESNAP',
  p3?.malls?.amazon?.url
);
ok(
  '診断: resolved件数とASIN解決元を返す',
  body.resolved?.amazon === 2 && body.resolved?.asinSources?.amazonAsinPrice === 1,
  JSON.stringify(body.resolved)
);
ok('name/price', p1?.name === 'テスト商品1' && p1?.price === 1980);
ok('stockFree=在庫-引当=7', p1?.stockFree === 7, `got ${p1?.stockFree}`);
ok('TEST2 stockFree=0', p2?.stockFree === 0, `got ${p2?.stockFree}`);

// 3. モールURL解決
ok('楽天: snapshot URL優先', p1?.malls?.rakuten?.url === 'https://item.rakuten.co.jp/b-faith/test1-rk/', p1?.malls?.rakuten?.url);
ok('Yahoo: 実在確認済→URL', p1?.malls?.yahoo?.url === 'https://store.shopping.yahoo.co.jp/b-faith01/test1-rk.html', p1?.malls?.yahoo?.url);
ok('Amazon: asin→dp URL', p1?.malls?.amazon?.url === 'https://www.amazon.co.jp/dp/B0TESTASIN', p1?.malls?.amazon?.url);
ok('Qoo10: item_no→URL', p1?.malls?.qoo10?.url === 'https://www.qoo10.jp/g/123456789', p1?.malls?.qoo10?.url);
ok('auPAY: idのみ・url=null', p1?.malls?.aupay?.skuKey === 'aupay-test1' && p1?.malls?.aupay?.url === null);
ok('楽天: item_dailyに無いコードはURLを出さない (404防止)', p2?.malls?.rakuten?.url === null, p2?.malls?.rakuten?.url);
ok('TEST2: 全モールnull', ['rakuten', 'yahoo', 'amazon', 'qoo10', 'aupay'].every((m) => p2?.malls?.[m]?.url === null));
ok('楽天: snapshotなし→フォールバックURL (lower化)', p3?.malls?.rakuten?.url === 'https://item.rakuten.co.jp/b-faith/test3-rk/', p3?.malls?.rakuten?.url);
ok('TEST3: Yahoo実在確認なし→URLなし', p3?.malls?.yahoo?.url === null, p3?.malls?.yahoo?.url);

// 4. 境界値
ok('引当>在庫 → stockFree=0', p4?.stockFree === 0, `got ${p4?.stockFree}`);
ok('name null → 空文字', p4?.name === '', JSON.stringify(p4?.name));
ok('price null → null', p4?.price === null, JSON.stringify(p4?.price));
ok('楽天: バリエーションSKU→親商品URLに解決', p4?.malls?.rakuten?.url === 'https://item.rakuten.co.jp/b-faith/test1-rk/', p4?.malls?.rakuten?.url);

// 5. fail-soft: prepare後に表をDROP → 該当lookupのみ降格・500にしない
db.exec('DROP TABLE mirror_qoo10_items');
{
  const r = await fetch(`${base}/products`, { headers: { 'x-read-token': 'test-token-123' } });
  ok('DROP後も200 (500にしない)', r.status === 200, `got ${r.status}`);
  const b = await r.json();
  const q1 = b.products.find((p) => p.code === 'TEST1');
  ok('Qoo10のみnull降格', q1?.malls?.qoo10?.url === null, JSON.stringify(q1?.malls?.qoo10));
  ok('他モールは維持', q1?.malls?.amazon?.url === 'https://www.amazon.co.jp/dp/B0TESTASIN');
  ok('degradedLookupsにqoo10Item', b.degradedLookups.includes('qoo10Items'), JSON.stringify(b.degradedLookups));
}

server.close();
console.log(`\n結果: pass=${pass} fail=${fail}`);
process.exit(fail === 0 ? 0 : 1);
