// supplier-sales: Amazon の売上を他モールと同じ「税込の売上」で足すかの試験
//   node apps/supplier-sales/test-amazon-sales-tax.mjs
//
// 背景 (2026-10-03 Codex R-F4-1 High 6): 公開の口で「売上(税込)」と説明しながら、Amazon だけ
// sales_principal_jpy (本体・税抜・送料とギフト包装なし) を足していた (他モールは税込の gross)。
// → Amazon も AMAZON_SALES_GROSS_INCL_SQL (本体 + 送料 + ギフト包装 + 決済の消費税の実額) で足す。
//
// 試すこと (本物の mirror の DDL = initMirrorDB で作った表):
//   1. 式が統合 view (v_mall_finance_daily_unified) の Amazon の sales_gross_jpy_incl と同じ値
//   2. 仕入先レポート (getSupplierReport) の Amazon の出品・合計
//   3. 日次明細 (getSupplierDailyDetail = 公開の日次 CSV) の 1 行ずつ
//   4. 未解決率 (getUnresolvedStats) の分子・分母がどちらも税込
//   5. 公開ページの説明文 (「送料は含まず」と書かない・税込を書く)
import { temporaryTestRoot } from '../../scripts/test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);

import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import ejs from 'ejs';

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'supplier-sales-tax-'));
process.env.DATA_DIR = scratch;

const repoRoot = path.resolve(path.dirname(new URL(import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, '$1')), '../..');
const load = (rel) => import(pathToFileURL(path.join(repoRoot, rel)).href);

const { initMirrorDB, getMirrorDB, AMAZON_SALES_GROSS_INCL_SQL } = await load('apps/warehouse-mirror/db.js');
initMirrorDB();
const { getSupplierReport, getSupplierDailyDetail, getUnresolvedStats, MALL_LABELS, SOKUHO_MALL_COLUMNS } =
  await load('apps/supplier-sales/aggregate.js');

let pass = 0, fail = 0;
function ok(cond, name, extra) {
  if (cond) { pass++; console.log(`  ✅ ${name}`); }
  else { fail++; console.log(`  ❌ ${name}${extra !== undefined ? ` — ${JSON.stringify(extra)?.slice(0, 400)}` : ''}`); }
}

const db = getMirrorDB();

/** NOT NULL・既定値なしの列を、型と CHECK の最初の値で埋めて 1 行入れる */
function insertRow(table, values) {
  const cols = db.prepare(`PRAGMA table_info(${table})`).all();
  if (!cols.length) throw new Error(`表 ${table} が無い`);
  const sql = db.prepare('SELECT sql FROM sqlite_master WHERE type = ? AND name = ?').get('table', table)?.sql || '';
  const enums = {};
  for (const m of sql.matchAll(/(\w+)\s+IN\s*\(([^)]*)\)/g)) {
    if (!enums[m[1]]) enums[m[1]] = [...m[2].matchAll(/'([^']*)'/g)].map((x) => x[1]);
  }
  for (const k of Object.keys(values)) {
    if (!cols.some((c) => c.name === k)) throw new Error(`${table} に列 ${k} が無い`);
  }
  const row = {};
  for (const c of cols) {
    if (Object.hasOwn(values, c.name)) row[c.name] = values[c.name];
    else if (c.notnull && c.dflt_value == null) {
      row[c.name] = enums[c.name]?.[0] ?? (/INT|REAL|NUM/i.test(c.type) ? 0 : 'x');
    }
  }
  const names = Object.keys(row);
  db.prepare(`INSERT INTO ${table} (${names.map((n) => `"${n}"`).join(',')}) VALUES (${names.map((_, i) => '?').join(',')})`)
    .run(...names.map((n) => row[n]));
}

// ═══ fixture ═══
// 仕入先 0109 の単品 item-a (標準売価 1,080 円・軽減税率 8% の食品の想定)。
//   Amazon: pr_a = item-a × 1 (FBA) / pr_a3 = item-a × 3 (FBM・お客さまが送料とギフト包装を払う)
//   楽天:   rk-a = item-a (gross_sales_jpy_incl = 税込)
const S = '0109';
insertRow('mirror_products', { 商品コード: 'item-a', 商品名: '商品A', 標準売価: 1080, 仕入先コード: S, 取扱区分: '取扱中' });
insertRow('mirror_products', { 商品コード: 'other-x', 商品名: '他社X', 標準売価: 500, 仕入先コード: '9999', 取扱区分: '取扱中' });
insertRow('mirror_sku_resolved', { seller_sku: 'pr_a', ne_code: 'item-a', quantity: 1 });
insertRow('mirror_sku_resolved', { seller_sku: 'pr_a3', ne_code: 'item-a', quantity: 3 });

const amz = (date, sku, v) => insertRow('mirror_amazon_finance_sku_daily', {
  date_jst: date, seller_sku: sku, product_name: `Amazon ${sku}`, cost_status: 'complete',
  source_run_id: 'r', source_row_hash: `${date}-${sku}`, synced_at: '2026-09-13T00:00:00Z', ...v,
});
// 9/2 pr_a (FBA): 2 個・本体 2,000 (税抜)・税 160 (8%)
amz('2026-09-02', 'pr_a', { units_ordered: 2, units_net_sold: 2, sales_principal_jpy: 2000, sales_tax_jpy: 160, fba_fulfillment_jpy: 300, commission_jpy: 300 });
// 9/3 pr_a3 (FBM): 1 件・本体 2,700 + 送料 500 + ギフト包装 300 (税抜)・税 216 + 50 + 30 = 296
amz('2026-09-03', 'pr_a3', { units_ordered: 1, units_net_sold: 1, sales_principal_jpy: 2700, sales_shipping_jpy: 500, sales_giftwrap_jpy: 300, sales_tax_jpy: 296, commission_jpy: 400 });
// 9/4 pr_a の返品だけの日: 返金 1,000 (本体)・返金の税 −80 が sales_tax_jpy に入る (決済の正味)
amz('2026-09-04', 'pr_a', { units_refunded_customer: 1, units_net_sold: -1, refund_principal_jpy: 1000, sales_tax_jpy: -80 });
// 9/5 名寄せできない SKU (未解決率の分子): 本体 1,000 + 税 100
amz('2026-09-05', 'pr_unknown', { units_ordered: 1, units_net_sold: 1, sales_principal_jpy: 1000, sales_tax_jpy: 100 });
// 9/12 確定の境 (cutoff = 両表の MAX 9/12 − 2 = 9/10) を作るだけの行
amz('2026-09-12', 'pr_a', {});

const rk = (date, code, ne, units, gross) => insertRow('mirror_rakuten_finance_sku_daily', {
  date_jst: date, rakuten_code: code, ne_code: ne, product_name: `楽天 ${code}`, cost_status: 'complete',
  units_ordered: units, units_net_sold: units, sales_principal_jpy_incl: gross, gross_sales_jpy_incl: gross,
  source_run_id: 'r', source_row_hash: `${date}-${code}`, synced_at: '2026-09-13T00:00:00Z',
});
rk('2026-09-02', 'rk-a', 'item-a', 3, 3240);
rk('2026-09-12', 'rk-a', 'item-a', 0, 0);

const OPTS = { period: 'custom', start: '2026-09-01', end: '2026-09-10' };

// ═══ 1. 統合 view と同じ式 ═══
console.log('\n■ 式 = 統合 view の Amazon の sales_gross_jpy_incl');
ok(/sales_principal_jpy/.test(AMAZON_SALES_GROSS_INCL_SQL) && /sales_shipping_jpy/.test(AMAZON_SALES_GROSS_INCL_SQL)
  && /sales_giftwrap_jpy/.test(AMAZON_SALES_GROSS_INCL_SQL) && /sales_tax_jpy/.test(AMAZON_SALES_GROSS_INCL_SQL),
  '式は本体 + 送料 + ギフト包装 + 税', AMAZON_SALES_GROSS_INCL_SQL);
const viewRows = db.prepare(`
  SELECT v.date_jst d, v.sku_key k, v.sales_gross_jpy_incl g, a.e
  FROM v_mall_finance_daily_unified v
  JOIN (SELECT date_jst, LOWER(TRIM(seller_sku)) k, ${AMAZON_SALES_GROSS_INCL_SQL} e
        FROM mirror_amazon_finance_sku_daily) a ON a.date_jst = v.date_jst AND a.k = v.sku_key
  WHERE v.mall = 'amazon'`).all();
ok(viewRows.length === 5 && viewRows.every((r) => Math.abs(r.g - r.e) < 1e-9), 'view と式が全部の行で同じ', viewRows);

// ═══ 2. 仕入先レポート ═══
console.log('\n■ 仕入先レポート (公開ページ・CSV)');
const rep = getSupplierReport(db, S, OPTS);
ok(rep.period?.start === '2026-09-01' && rep.period?.end === '2026-09-10', '期間 9/1〜9/10', rep.period);
const a = rep.products.find((p) => p.ne_code === 'item-a');
const L = (mall, id) => a?.listings.find((x) => x.mall === mall && x.listingId === id);
console.log(`    item-a: pieces=${a?.pieces} sales=${a?.sales} / listings=${JSON.stringify(a?.listings.map((x) => [x.mall, x.listingId, x.pieces, x.sales]))}`);
ok(L('amazon', 'pr_a')?.sales === 2160 - 80, 'Amazon pr_a = 2,160 (9/2 税込) − 80 (9/4 返金の税) = 2,080', L('amazon', 'pr_a'));
ok(L('amazon', 'pr_a3')?.sales === 3796, 'Amazon pr_a3 = 2,700 + 送料 500 + ギフト包装 300 + 税 296 = 3,796', L('amazon', 'pr_a3'));
ok(L('rakuten', 'rk-a')?.sales === 3240, '楽天 rk-a = 3,240 (変わらない)', L('rakuten', 'rk-a'));
ok(a?.sales === 2080 + 3796 + 3240 && rep.totals.sales === 9116, '合計 9,116 円', rep.totals);
ok(a?.pieces === 2 + 3 - 1 + 3, 'ピース数は変わらない (2 + 3 − 1 + 3 = 7)', a?.pieces);

// ═══ 3. 日次明細 (公開の日次 CSV) ═══
console.log('\n■ 日次明細 (getSupplierDailyDetail)');
const det = getSupplierDailyDetail(db, S, OPTS);
const dRow = (date, mall, id) => det.rows.find((r) => r.date === date && r.mall === mall && r.listingId === id);
ok(dRow('2026-09-02', 'amazon', 'pr_a')?.sales === 2160, '9/2 Amazon pr_a 2,160 (税込)', dRow('2026-09-02', 'amazon', 'pr_a'));
ok(dRow('2026-09-03', 'amazon', 'pr_a3')?.sales === 3796, '9/3 Amazon pr_a3 3,796', dRow('2026-09-03', 'amazon', 'pr_a3'));
ok(dRow('2026-09-04', 'amazon', 'pr_a')?.sales === -80, '9/4 返品だけの日 = 返金の税 −80 (本体の返金は引かない)', dRow('2026-09-04', 'amazon', 'pr_a'));
ok(dRow('2026-09-02', 'rakuten', 'rk-a')?.sales === 3240, '9/2 楽天 3,240', dRow('2026-09-02', 'rakuten', 'rk-a'));
ok(det.rows.reduce((s, r) => s + r.sales, 0) === rep.totals.sales, '日次の合計 = レポートの合計', det.rows);

// ═══ 4. 未解決率 ═══
console.log('\n■ 未解決率 (社内の健全性)');
const st = getUnresolvedStats(db);
console.log(`    ${JSON.stringify(st)}`);
ok(st.unresolvedSales30 === 1100, '未解決 = pr_unknown の税込 1,100', st);
ok(st.totalSales30 === 2160 + 3796 - 80 + 1100 + 3240, '全社 = 税込の合計 10,216', st);

// ═══ 5. 公開ページの説明文 ═══
console.log('\n■ 公開ページの説明文');
const html = await ejs.renderFile(path.join(repoRoot, 'views/supplier-sales-public.ejs'), {
  title: 't', supplierName: 'テスト仕入先', token: 'x'.repeat(43), mallLabels: MALL_LABELS, sokuhoMallDefs: SOKUHO_MALL_COLUMNS,
  period: rep.period, sokuho: rep.sokuho, products: rep.products, totals: rep.totals,
  query: { period: 'custom', start: OPTS.start, end: OPTS.end, tab: 'kakutei' },
});
ok(!/送料は含まず/.test(html), '「送料は含まず」と書かない (楽天・au PAY・Amazon は送料を含む)');
ok(/商品代金・送料など、消費税込み/.test(html) && /全モール同じ基準/.test(html), '税込・送料込み・全モール同じ基準と書く');
ok(html.includes('9,116'), '公開ページの売上の合計 9,116', null);

console.log(`\n═══ 結果: ${pass} PASS / ${fail} FAIL ═══`);
try { db.close(); } catch { /* noop */ }
process.exitCode = fail > 0 ? 1 : 0;
