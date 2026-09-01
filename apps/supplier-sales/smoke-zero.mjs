// 売上ゼロ商品の表示 (2026-09-01) の合成データ smoke。
//   node apps/supplier-sales/smoke-zero.mjs
import Database from 'better-sqlite3';
import path from 'node:path';
import ejs from 'ejs';
import { getSupplierReport, MALL_LABELS, SOKUHO_MALL_COLUMNS } from './aggregate.js';
import { buildCsv } from './csv.js';

const db = new Database(':memory:');
db.exec(`
CREATE TABLE mirror_products (商品コード TEXT, 商品名 TEXT, 標準売価 REAL, 仕入先コード TEXT, 取扱区分 TEXT);
CREATE TABLE mirror_set_components (セット商品コード TEXT, 構成商品コード TEXT, 数量 INTEGER);
CREATE TABLE mirror_sku_resolved (seller_sku TEXT, ne_code TEXT, quantity INTEGER);
CREATE TABLE mirror_amazon_finance_sku_daily (date_jst TEXT, seller_sku TEXT, asin_norm TEXT, product_name TEXT, units_net_sold REAL, sales_principal_jpy REAL, fba_fulfillment_jpy REAL, fba_storage_jpy REAL);
CREATE TABLE mirror_rakuten_finance_sku_daily (date_jst TEXT, rakuten_code TEXT, ne_code TEXT, product_name TEXT, units_net_sold REAL, gross_sales_jpy_incl REAL);
CREATE TABLE mirror_yahoo_finance_sku_daily (date_jst TEXT, yahoo_sku_key TEXT, ne_code TEXT, product_name TEXT, units_net_sold REAL, gross_sales_jpy_incl REAL);
CREATE TABLE mirror_aupay_finance_sku_daily (date_jst TEXT, aupay_sku_key TEXT, ne_code TEXT, product_name TEXT, units_net_sold REAL, gross_sales_jpy_incl REAL);
CREATE TABLE mirror_linegift_finance_sku_daily (date_jst TEXT, sku_code TEXT, ne_code TEXT, product_name TEXT, units_net_sold REAL, gross_sales_jpy_incl REAL);
CREATE TABLE mirror_qoo10_finance_sku_daily (date_jst TEXT, sku_code TEXT, ne_code TEXT, product_name TEXT, units_net_sold REAL, customer_paid_jpy_incl REAL);
CREATE TABLE mirror_f_sales_velocity_by_product_mall (商品コード TEXT, mall TEXT, qty_7d INTEGER, qty_30d INTEGER, as_of_date TEXT);
`);
const S = '0109';
const ins = db.prepare('INSERT INTO mirror_products VALUES (?,?,?,?,?)');
ins.run('sold-a', '売れてる商品A', 1000, S, '取扱中');
ins.run('zero-b', '売上ゼロ商品B', 800, S, '取扱中');
ins.run('zero-c', '売上ゼロ商品C', 500, S, '取扱中');
ins.run('disc-d', '廃止商品D', 500, S, '廃止');
ins.run('disc-e', '廃止だが売れた商品E', 500, S, '廃止');
ins.run('set-ab', 'AとBのセット', 1800, S, '取扱中');   // 構成品へ展開されるセット → 行に出さない
ins.run('other-x', '他社商品X', 300, '9999', '取扱中');
db.prepare('INSERT INTO mirror_set_components VALUES (?,?,?)').run('set-ab', 'sold-a', 1);
db.prepare('INSERT INTO mirror_set_components VALUES (?,?,?)').run('set-ab', 'zero-b', 1);
// 楽天で A が 2個、廃止品 E が 1個 売れた (cutoff 内)
const today = '2026-08-20';
db.prepare('INSERT INTO mirror_rakuten_finance_sku_daily VALUES (?,?,?,?,?,?)').run('2026-08-10', 'rk-a', 'sold-a', '楽天A', 2, 2000);
db.prepare('INSERT INTO mirror_rakuten_finance_sku_daily VALUES (?,?,?,?,?,?)').run('2026-08-10', 'rk-e', 'disc-e', '楽天E', 1, 500);
db.prepare('INSERT INTO mirror_rakuten_finance_sku_daily VALUES (?,?,?,?,?,?)').run(today, 'rk-a', 'sold-a', '楽天A', 0, 0);
// 速報: A のみ
db.prepare('INSERT INTO mirror_f_sales_velocity_by_product_mall VALUES (?,?,?,?,?)').run('sold-a', 'rakuten', 3, 9, '2026-08-21');

let fail = 0;
const check = (label, ok, extra = '') => { console.log(`${ok ? 'OK ' : 'NG '} ${label}${extra ? ' ' + extra : ''}`); if (!ok) fail++; };

const rep = getSupplierReport(db, S, { period: '30d' });
const codes = rep.products.map(p => p.ne_code);
console.log('products:', rep.products.map(p => `${p.ne_code}(${p.product_name}) s30=${p.sokuho30} pieces=${p.pieces} sales=${p.sales}`).join(' | '));
check('売上ゼロの取扱中商品 B/C が並ぶ', codes.includes('zero-b') && codes.includes('zero-c'));
check('ゼロ商品にも商品名が入る', rep.products.find(p => p.ne_code === 'zero-b').product_name === '売上ゼロ商品B');
check('売れた商品 A は従来通り', rep.products.find(p => p.ne_code === 'sold-a').pieces === 2);
check('廃止で売上なし D は出ない', !codes.includes('disc-d'));
check('廃止でも売れた E は出る (和集合)', codes.includes('disc-e'));
check('構成品展開されるセット set-ab は出ない', !codes.includes('set-ab'));
check('他社商品は出ない', !codes.includes('other-x'));
check('並び: 売れた A が先頭、ゼロ行はコード順で末尾', codes[0] === 'sold-a' && codes.indexOf('zero-b') < codes.indexOf('zero-c'), codes.join(','));
check('totals.productCount=4 / soldProductCount=2', rep.totals.productCount === 4 && rep.totals.soldProductCount === 2, JSON.stringify(rep.totals));
check('合計は影響なし', rep.totals.pieces === 3 && rep.totals.sales === 2500 && rep.totals.sokuho30 === 9);

// finance 空 (period null) でも取扱中商品は並ぶ
db.exec('DELETE FROM mirror_rakuten_finance_sku_daily');
const rep2 = getSupplierReport(db, S, { period: '30d' });
check('finance 空でも取扱中商品 3点 + 速報のみ', rep2.period === null && rep2.products.length === 3, rep2.products.map(p => p.ne_code).join(','));

// CSV: ゼロ行が含まれる
const csv = buildCsv(rep, MALL_LABELS);
check('CSV にゼロ商品行', csv.includes('zero-b') && csv.includes('zero-c'));

// 公開 EJS が描画できる (rep / rep2 の両方)
const view = path.resolve('views/supplier-sales-public.ejs');
for (const [label, r] of [['通常', rep], ['finance空', rep2]]) {
  try {
    const html = await ejs.renderFile(view, {
      title: 't', supplierName: 'テスト仕入先', token: 'x'.repeat(43), mallLabels: MALL_LABELS, sokuhoMallDefs: SOKUHO_MALL_COLUMNS,
      period: r.period, sokuho: r.sokuho, products: r.products, totals: r.totals,
      query: { period: '30d', start: '', end: '', tab: 'sokuho' },
    });
    const nosale = (html.match(/class="[^"]*nosale/g) || []).length;
    check(`EJS 描画 (${label})`, html.includes('売上ゼロ商品B') && nosale > 0, `nosale=${nosale} len=${html.length}`);
    const openTags = (html.match(/<script\b/g) || []).length, closeTags = (html.match(/<\/script>/g) || []).length;
    check(`script タグ対応 (${label})`, openTags === closeTags, `${openTags}/${closeTags}`);
    if (label === '通常') check('確定タブに販売あり件数', /販売あり 1 点/.test(html) && /販売あり 1 点|販売あり 2 点/.test(html));
  } catch (e) { check(`EJS 描画 (${label})`, false, e.message); }
}

console.log(fail ? `\n${fail} 件 NG` : '\nALL OK');
process.exitCode = fail ? 1 : 0;
