// rakuten-analytics 機能スモーク: fixture 投入 → 全 query 関数を実行して形を検証
// 使い方: DATA_DIR=<空ディレクトリ> node apps/rakuten-analytics/smoke.mjs
// ⚠️ DATA_DIR 内の warehouse-mirror.db の対象テーブルを DELETE するため本番 DATA_DIR で実行禁止
import fs from 'node:fs';
import path from 'node:path';
import AdmZip from 'adm-zip';
import iconv from 'iconv-lite';
import { initMirrorDB, getMirrorDB } from '../warehouse-mirror/db.js';
import * as q from './queries.js';
import * as imp from './import.js';

// 本番 DB 誤実行ガード (Codex R1 High → R2 High で決定的判定に強化):
// ① DATA_DIR 未指定 (= cwd の本番 DB に向く) なら即中断
// ② サンドボックスマーカー方式: DATA_DIR に既存 DB があるのに .ra-smoke-sandbox マーカーが
//    無ければ「smoke が作った DB ではない」ので中断。ヒューリスティック (行数閾値) に頼らない
const dataDir = process.env.DATA_DIR;
if (!dataDir) {
  console.error('FATAL: DATA_DIR が未指定です。smoke 専用の空ディレクトリを指定してください (例: DATA_DIR=c:/tmp/ra-smoke-data)');
  process.exit(2);
}
const marker = path.join(dataDir, '.ra-smoke-sandbox');
const dbFile = path.join(dataDir, 'warehouse-mirror.db');
if (fs.existsSync(dbFile) && !fs.existsSync(marker)) {
  console.error(`FATAL: ${dbFile} は smoke が作成した DB ではありません (マーカー ${marker} なし)。本番/既存 DB の可能性があるため中断します`);
  process.exit(2);
}
// DB がまだ無い場合も、マーカー無しの非空ディレクトリなら承認しない (本番 DATA_DIR の誤指定対策 — Codex R3 Medium)
if (!fs.existsSync(dbFile) && !fs.existsSync(marker) && fs.existsSync(dataDir)
    && fs.readdirSync(dataDir).length > 0) {
  console.error(`FATAL: ${dataDir} は空ではありません。smoke 専用の空ディレクトリを指定してください`);
  process.exit(2);
}
fs.mkdirSync(dataDir, { recursive: true });
fs.writeFileSync(marker, `rakuten-analytics smoke sandbox (created ${new Date().toISOString()})\n`);

initMirrorDB();
const db = getMirrorDB();

const today = q.jstToday();
const d = (n) => q.addDays(today, -n);
const ymNow = today.slice(0, 7);
const ymPrev = q.addMonths(ymNow, -1);

// ─── fixture ───
imp.ensureImportTables();
const tx = db.transaction(() => {
  for (const t of ['mirror_rakuten_finance_sku_daily', 'mart_rakuten_monthly_summary',
    'fact_rakuten_ads_rpp', 'fact_rakuten_ads_rpp_keyword', 'radash_import_log']) {
    try { db.exec(`DELETE FROM ${t}`); } catch {}
  }

  const ins = db.prepare(`INSERT INTO mirror_rakuten_finance_sku_daily (
    date_jst, rakuten_code, ne_code, sku_resolution, product_name,
    units_ordered, units_cancelled, units_net_sold, allocated_units_cancelled,
    sales_principal_jpy_incl, sales_postage_jpy_incl,
    coupon_shop_jpy_incl, coupon_all_jpy_incl, promotion_jpy_incl,
    refund_amount_jpy_incl, allocated_refund_amount_jpy_incl,
    mall_fee_jpy_incl, shipping_cost_jpy_incl, shipping_quality,
    unit_cost_snapshot_incl, cogs_amount_jpy_incl,
    gross_sales_jpy_incl, net_sales_jpy_incl, variable_margin_jpy_incl,
    refund_adjusted_net_sales_jpy_incl,
    cost_status, is_cost_complete, data_quality_score,
    source_run_id, source_row_hash, synced_at
  ) VALUES (?, ?, ?, 'resolved', ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, 'actual', ?, ?, ?, ?, ?, ?, 'complete', 1, 100, 'smoke', 'h', 't')`);

  // 90日分 2 SKU (税込)。alpha: 10個/日 単価1000、beta: 4個/日 単価2000
  for (let i = 0; i < 90; i++) {
    const date = d(i);
    {
      // alpha: gross=10500 (principal 10000 + postage 500)、クーポン店負300、返金200
      const gross = 10500, coupon = 300, refund = 200;
      const net = gross - coupon;                       // 10200
      const mallFee = Math.round((10000 + 500 - 500) * 0.10); // coupon_all=500 → 課金ベース10000 → 1000
      const shipping = 800, cogs = 3000;
      const vm = net - refund - mallFee - shipping - cogs;    // 5200
      ins.run(date, 'rk-alpha', 'NE-A', 'アルファ精油', 10, 1, 9, 1,
        10000, 500, coupon, 500, refund, refund,
        mallFee, shipping, 300, cogs, gross, net, vm, net - refund);
    }
    {
      // beta: gross=8200、クーポンなし、返金なし
      const gross = 8200, mallFee = 820, shipping = 600, cogs = 4000;
      const vm = gross - mallFee - shipping - cogs;     // 2780
      ins.run(date, 'rk-beta', 'NE-B', 'ベータ茶葉', 4, 0, 4, 0,
        8000, 200, 0, 0, 0, 0,
        mallFee, shipping, 1000, cogs, gross, gross, vm, gross);
    }
  }

  // 先月は仕訳書取込済み (確定): 広告費 50,000 / 楽天手数料実額 90,000
  db.prepare(`INSERT INTO mart_rakuten_monthly_summary (year_month, ad_cost, pf_fee, confirmed_at)
    VALUES (?, 50000, 90000, 't')`).run(ymPrev);
});
tx();

// ─── 実行 ───
let pass = 0, fail = 0;
function check(name, fn) {
  try {
    fn();
    console.log(`✓ ${name}`);
    pass++;
  } catch (e) {
    console.error(`✗ ${name}: ${e.message}`);
    fail++;
  }
}
function assert(cond, msg) { if (!cond) throw new Error(msg); }

check('resolvePeriod', () => {
  assert(q.resolvePeriod('last_month').from === q.monthStart(ymPrev), 'last_month from');
  assert(q.resolvePeriod('bogus').preset === '30d', 'fallback 30d');
  const c = q.resolvePeriod(null, '2026-01-01', '2026-01-31');
  assert(c.preset === 'custom' && c.from === '2026-01-01', 'custom range');
  assert(q.resolvePeriod(null, '2026-01-31', '2026-01-01').preset === '30d', '逆順は fallback');
  assert(!q.isValidDate('2026-02-30'), '存在しない日付');
  const wide = q.resolvePeriod(null, '2000-01-01', '2026-01-01');
  assert(wide.from === q.addDays('2026-01-01', -730), `巨大range clamp (got ${wide.from})`);
});

check('getTrend day→week 自動格上げ', () => {
  const r = q.getTrend(d(365), today, 'day');
  assert(r.granularity === 'week', `半年超の day は week に (got ${r.granularity})`);
});

check('getOverview', () => {
  const r = q.getOverview();
  assert(r.tiles.length === 4, 'tiles 4枚');
  assert(r.data_to === today, `data_to=${r.data_to}`);
  const tYest = r.tiles.find(t => t.key === 'yesterday');
  assert(tYest.sales_incl === 10500 + 8200, `昨日売上 (got ${tYest.sales_incl})`);
  assert(tYest.units_net === 13, `昨日販売数 (got ${tYest.units_net})`);
  assert(tYest.variable_margin === 5200 + 2780, `昨日粗利 (got ${tYest.variable_margin})`);
  assert(tYest.margin_pct !== null && tYest.cost_coverage_pct === 100, '率とカバー率');
  const tLast = r.tiles.find(t => t.key === 'last_month');
  assert(tLast.confirmed !== null && tLast.confirmed !== undefined, '先月は確定あり');
  assert(tLast.confirmed.ad_cost === 50000 && tLast.confirmed.pf_fee === 90000, '確定値');
  // 確定寄せ = VM + mall_fee(推定戻し) − pf_fee − ad_cost
  const days = tLast.days_with_data;
  const expected = (5200 + 2780) * days + (1000 + 820) * days - 90000 - 50000;
  assert(tLast.confirmed.full_margin === expected, `確定寄せ実質利益 (got ${tLast.confirmed.full_margin}, want ${expected})`);
  const tMonth = r.tiles.find(t => t.key === 'this_month');
  assert(tMonth.confirmed === null, '今月は未確定 (confirmed=null)');
});

check('getTrend day', () => {
  const r = q.getTrend(d(29), today, 'day');
  assert(r.rows.length === 30, `30日分 (got ${r.rows.length})`);
  const row = r.rows[0];
  assert(row.sales_incl === 18700, `日次売上 (got ${row.sales_incl})`);
  assert(row.margin_pct !== null, 'margin_pct');
  assert(row.cancel_rate_pct !== null, 'cancel_rate');
  assert(r.confirmed_months.length === 0, '日次では confirmed なし');
});

check('getTrend month + 確定オーバーレイ', () => {
  const r = q.getTrend(d(89), today, 'month');
  assert(r.rows.length >= 3, `3ヶ月分 (got ${r.rows.length})`);
  const c = r.confirmed_months.find(x => x.year_month === ymPrev);
  assert(c, '先月の確定行あり');
  assert(c.ad_cost === 50000 && c.pf_fee === 90000, '確定値');
  const prevRow = r.rows.find(x => x.bucket === ymPrev);
  assert(c.full_margin === prevRow.variable_margin + prevRow.mall_fee_est - 90000 - 50000, 'full_margin 整合');
});

check('getTrend week (bucket=月曜日始まり)', () => {
  const r = q.getTrend(d(29), today, 'week');
  assert(r.rows.length >= 4 && r.rows.length <= 6, `週次バケット (got ${r.rows.length})`);
  for (const row of r.rows) {
    assert(/^\d{4}-\d{2}-\d{2}$/.test(row.bucket), `bucket は日付 (got ${row.bucket})`);
    const dow = new Date(row.bucket + 'T00:00:00Z').getUTCDay();
    assert(dow === 1, `bucket は月曜 (got dow=${dow} for ${row.bucket})`);
  }
});

check('空期間 (データなし) は 0 で返る', () => {
  const r = q.getTrend('2020-01-01', '2020-01-31', 'day');
  assert(r.rows.length === 0, 'rows 空');
  const ov = q.getOverview();  // 今日タイルはデータあり得るので単に throw しないこと
  assert(ov.tiles.length === 4, 'overview は常に4枚');
});

// ============================================================
// P2: 取込 (RPP CSV/zip)
// ============================================================
const BOM = '﻿';
const productCsv = (dates) => BOM + [
  '日付,キャンペーンID,キャンペーン名,商品管理番号,商品名,クリック数(合計),実績額(合計),CPC実績,CTR,売上金額(720時間)(合計),売上件数(720時間)(合計),CVR(720時間),ROAS(720時間),売上金額(12時間)(合計),売上金額(720時間)(新規),売上金額(720時間)(既存)',
  ...dates.map(dt => `${dt},C001,"通常キャンペーン",rk-alpha,"アルファ精油, 10ml",25,"1,250",50,1.25%,"12,000",4,16%,960%,"6,000","5,000","7,000"`),
  ...dates.map(dt => `${dt},C001,"通常キャンペーン",rk-beta,ベータ茶葉,10,500,50,0.8%,0,0,-,-,-,-,-`),
].join('\r\n');

const keywordCsvSjis = (dates) => iconv.encode([
  '日付,キャンペーンID,商品管理番号,キーワード,キーワードCPC,クリック数,実績額,CPC実績,CTR,売上金額(720時間),売上件数(720時間),CVR(720時間),ROAS(720時間)',
  ...dates.map(dt => `${dt},C001,rk-alpha,アロマオイル,60,12,720,60,2.0%,"8,000",2,16.7%,1111%`),
  ...dates.map(dt => `${dt},C001,rk-alpha,精油 セット,55,5,275,55,1.1%,0,0,-,-`),
].join('\r\n'), 'Shift_JIS');

const mkFile = (name, content) => ({ originalname: name, buffer: Buffer.isBuffer(content) ? content : Buffer.from(content, 'utf8') });

check('import: RPP商品CSV (UTF-8 BOM、日付ゆらぎ 2026/7/3形式)', () => {
  const dates = [`${ymPrev}/01`.replace('-', '/'), `${ymPrev.split('-')[0]}/${Number(ymPrev.split('-')[1])}/2`];
  const r = imp.importFiles([mkFile('rpp_product.csv', productCsv(dates))], 'smoke@test');
  assert(r.imported === 1 && r.failed === 0, `結果 (${JSON.stringify(r.results)})`);
  const res = r.results[0];
  assert(res.type === 'rpp_product', 'type');
  assert(res.rows === 4 && res.inserted === 4 && res.updated === 0, `4行新規 (got ${JSON.stringify(res)})`);
  assert(res.date_from === `${ymPrev}-01` && res.date_to === `${ymPrev}-02`, `日付正規化 (got ${res.date_from}〜${res.date_to})`);
  const row = db.prepare(`SELECT * FROM fact_rakuten_ads_rpp WHERE item_manage_number='rk-alpha' AND date_jst=?`).get(`${ymPrev}-01`);
  assert(row.ad_cost === 1250 && row.clicks === 25, 'カンマ区切り数値');
  assert(row.sales_720h === 12000 && row.orders_720h === 4, '720h売上');
  assert(row.sales_12h === 6000 && row.sales_720h_new === 5000, '12h/新規列');
  assert(row.item_name === 'アルファ精油, 10ml', 'クォート内カンマ');
  const beta = db.prepare(`SELECT * FROM fact_rakuten_ads_rpp WHERE item_manage_number='rk-beta' AND date_jst=?`).get(`${ymPrev}-01`);
  assert(beta.cvr_720h_pct === null && beta.sales_720h === 0, `"-" は null / 売上0 (got ${JSON.stringify(beta)})`);
});

check('import: 再アップロード = UPSERT上書き (720h遡及対応)', () => {
  const dates = [`${ymPrev}-01`, `${ymPrev}-02`];
  const r = imp.importFiles([mkFile('rpp_product_v2.csv', productCsv(dates))], 'smoke@test');
  const res = r.results[0];
  assert(res.inserted === 0 && res.updated === 4, `全行上書き (got ${JSON.stringify(res)})`);
  const cnt = db.prepare(`SELECT COUNT(*) AS c FROM fact_rakuten_ads_rpp`).get().c;
  assert(cnt === 4, `行数増えない (got ${cnt})`);
});

check('import: RPPキーワードCSV (Shift_JIS)', () => {
  const r = imp.importFiles([mkFile('rpp_keyword.csv', keywordCsvSjis([`${ymPrev}-01`]))], 'smoke@test');
  const res = r.results[0];
  assert(res.type === 'rpp_keyword' && res.inserted === 2, `keyword 2行 (got ${JSON.stringify(res)})`);
  const kw = db.prepare(`SELECT * FROM fact_rakuten_ads_rpp_keyword WHERE keyword='アロマオイル'`).get();
  assert(kw.ad_cost === 720 && kw.sales_720h === 8000 && kw.keyword_cpc === 60, 'キーワード行の値');
});

check('import: zip展開 (商品+キーワード同梱)', () => {
  db.exec(`DELETE FROM fact_rakuten_ads_rpp; DELETE FROM fact_rakuten_ads_rpp_keyword`);
  const zip = new AdmZip();
  zip.addFile('product.csv', Buffer.from(productCsv([`${ymPrev}-05`]), 'utf8'));
  zip.addFile('keyword.csv', keywordCsvSjis([`${ymPrev}-05`]));
  zip.addFile('readme.txt', Buffer.from('ignore me'));
  const r = imp.importFiles([mkFile('report.zip', zip.toBuffer())], 'smoke@test');
  assert(r.imported === 2 && r.failed === 0, `zip内CSV 2件取込 (got ${JSON.stringify(r.results.map(x => x.file + ':' + (x.ok ? 'ok' : x.error)))})`);
});

check('import: 不正行で全件rollback', () => {
  const before = db.prepare(`SELECT COUNT(*) AS c FROM fact_rakuten_ads_rpp`).get().c;
  const bad = productCsv([`${ymPrev}-10`]) + '\r\n不正な日付,C001,x,rk-gamma,G,1,1,1,1%,0,0,-,-,-,-,-';
  const r = imp.importFiles([mkFile('broken.csv', bad)], 'smoke@test');
  assert(r.failed === 1, 'エラー扱い');
  assert(/日付が不正/.test(r.results[0].error), `理由 (got ${r.results[0].error})`);
  const after = db.prepare(`SELECT COUNT(*) AS c FROM fact_rakuten_ads_rpp`).get().c;
  assert(after === before, `rollback (before=${before} after=${after})`);
});

check('import: 未知の形式はヘッダ付きエラー', () => {
  const r = imp.importFiles([mkFile('mystery.csv', '謎列1,謎列2\n1,2')], 'smoke@test');
  assert(r.failed === 1 && /種類を判別できません/.test(r.results[0].error), r.results[0].error);
  assert(/謎列1/.test(r.results[0].error), 'ヘッダをエラーに含む');
});

check('import: 日付列なし (月別DL) は案内エラー', () => {
  const noDate = '商品管理番号,クリック数,実績額\nrk-alpha,10,500';
  const r = imp.importFiles([mkFile('monthly.csv', noDate)], 'smoke@test');
  assert(r.failed === 1 && /日別/.test(r.results[0].error), r.results[0].error);
});

check('import: 根幹メトリクス列欠落はエラー (0値で取り込まない)', () => {
  const noCost = `日付,商品管理番号,クリック数(合計),売上金額(720時間)(合計),売上件数(720時間)(合計)\n${ymPrev}-01,rk-alpha,10,100,1`;
  const r = imp.importFiles([mkFile('no_cost.csv', noCost)], 'smoke@test');
  assert(r.failed === 1 && /実績額/.test(r.results[0].error), r.results[0].error);
});

check('import: zip爆弾 (異常圧縮率) は丸ごと拒否・部分取込なし', () => {
  const before = db.prepare(`SELECT COUNT(*) AS c FROM fact_rakuten_ads_rpp`).get().c;
  const zip = new AdmZip();
  zip.addFile('good.csv', Buffer.from(productCsv([`${ymPrev}-20`]), 'utf8'));
  zip.addFile('bomb.csv', Buffer.alloc(10 * 1024 * 1024, 0x30));  // 10MBの'0'連続 → 超高圧縮率
  const r = imp.importFiles([mkFile('bomb.zip', zip.toBuffer())], 'smoke@test');
  assert(r.failed === 1 && r.imported === 0, `丸ごと拒否 (got ${JSON.stringify(r.results.map(x => x.ok))})`);
  assert(/爆弾|圧縮率/.test(r.results[0].error), r.results[0].error);
  const after = db.prepare(`SELECT COUNT(*) AS c FROM fact_rakuten_ads_rpp`).get().c;
  assert(after === before, `good.csv も取り込まれていない (before=${before} after=${after})`);
});

check('import: zip内に不良CSVがあれば正常CSVも取り込まない (グループ原子性)', () => {
  const before = db.prepare(`SELECT COUNT(*) AS c FROM fact_rakuten_ads_rpp`).get().c;
  const zip = new AdmZip();
  zip.addFile('good.csv', Buffer.from(productCsv([`${ymPrev}-25`]), 'utf8'));
  zip.addFile('broken.csv', Buffer.from(productCsv([`${ymPrev}-26`]) + '\r\n不正な日付,C001,x,rk-gamma,G,1,1,1,1%,0,0,-,-,-,-,-', 'utf8'));
  const r = imp.importFiles([mkFile('mixed.zip', zip.toBuffer())], 'smoke@test');
  assert(r.imported === 0 && r.failed === 1 && r.skipped === 1, `error1+skip1 (got imported=${r.imported} failed=${r.failed} skipped=${r.skipped})`);
  const good = r.results.find(x => x.file.includes('good.csv'));
  assert(good.skipped === true && /取込を中止/.test(good.error), `goodはskip理由 (got ${good.error})`);
  const after = db.prepare(`SELECT COUNT(*) AS c FROM fact_rakuten_ads_rpp`).get().c;
  assert(after === before, `1行も入らない (before=${before} after=${after})`);
  const log = imp.getImportLog(5);
  assert(log.some(l => l.status === 'skipped'), 'skippedログが残る');
});

check('import: キーワードレポートに商品管理番号列が無ければエラー', () => {
  const noItem = `日付,キーワード,クリック数,実績額,売上金額(720時間),売上件数(720時間)\n${ymPrev}-01,アロマ,1,50,0,0`;
  const r = imp.importFiles([mkFile('kw_no_item.csv', noItem)], 'smoke@test');
  assert(r.failed === 1 && /商品管理番号/.test(r.results[0].error), r.results[0].error);
});

check('import: 鮮度ボード + 未取込月 + 履歴', () => {
  const st = imp.getImportStatus();
  const prod = st.types.find(t => t.type === 'rpp_product');
  assert(prod.implemented && prod.row_count > 0, '商品レポート取込済');
  assert(prod.data_from === `${ymPrev}-05`, `data_from (got ${prod.data_from})`);
  // 先月〜今月の2ヶ月レンジでデータは先月のみ → 今月が未取込
  assert(prod.missing_months.includes(ymNow), `今月が未取込月 (got ${JSON.stringify(prod.missing_months)})`);
  const ca = st.types.find(t => t.type === 'ca');
  assert(ca.implemented === false, 'CAは準備中');
  const log = imp.getImportLog(10);
  assert(log.length > 0 && log[0].imported_at, '履歴あり');
  assert(log.some(l => l.status === 'error'), 'エラー履歴も残る');
});

console.log(`\n=== smoke: ${pass} PASS / ${fail} FAIL ===`);
process.exit(fail > 0 ? 1 : 0);
