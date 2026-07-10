#!/usr/bin/env node
/**
 * test-rakuten-data-import.mjs — 楽天RMSデータ分析取込のスモークテスト (mall-csv-fetcher P1-R2)
 *
 * 実DLファイル (2026-07-09/07-10 取得の商品分析・日次_分析用レポート) の実測ヘッダを
 * 再現した合成フィクスチャで、判別〜ガード〜UPSERT〜冪等〜原子性を検証する。DBはtemp (本番DB不触)。
 *
 * 実行: node apps/warehouse/test-rakuten-data-import.mjs
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import Database from 'better-sqlite3';
import AdmZip from 'adm-zip';
import iconv from 'iconv-lite';
import { ensureRakutenDataTables, importDataFile, prepareDataFile } from './rakuten-data-lib.js';

let passed = 0, failed = 0;
function check(name, cond, detail = '') {
  if (cond) { console.log(`  ✅ ${name}`); passed++; }
  else { console.log(`  ❌ ${name} ${detail}`); failed++; }
}

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'rdata-smoke-'));
const db = new Database(path.join(tmp, 'warehouse.db'));
db.pragma('journal_mode = WAL');
ensureRakutenDataTables(db);

const sha = (b) => crypto.createHash('sha256').update(b).digest('hex');
const utf8bom = (s) => Buffer.concat([Buffer.from([0xEF, 0xBB, 0xBF]), Buffer.from(s, 'utf8')]);
const sjis = (s) => iconv.encode(s, 'Shift_JIS');
function zipOf(entries) {
  const z = new AdmZip();
  for (const [name, buf] of entries) z.addFile(name, buf);
  return z.toBuffer();
}

// ─── フィクスチャ: 商品分析 (実測28列、UTF-8 BOM、前置4行+空行) ───
const ITEM_HEADER = '#,ジャンル,カタログID,商品ID,商品名,商品管理番号,商品番号,売上,売上件数,売上個数,アクセス人数,ユニークユーザー数,転換率,客単価,総購入件数,新規購入件数,リピート購入件数,未購入アクセス人数,レビュー投稿数,レビュー総合評価（点）,総レビュー数,滞在時間（秒）,直帰数,離脱数,離脱率,お気に入り登録ユーザ数,お気に入り総ユーザ数,在庫数';
function itemRow(n, sku, itemName, sales, access, stock) {
  return `${n},食品 > 菓子・スイーツ,,10001234,${itemName},${sku},${sku.toLowerCase()},"${sales}",2,3,${access},45,4.00%,6400,2,1,1,48,0,4.5,10,120,20,30,60.00%,1,5,${stock}`;
}
function itemCsv({ period = '2026年07月09日から2026年07月09日', keyword = '', device = 'すべて', rows }) {
  return utf8bom([
    '商品分析', `表示期間,${period}`, `キーワード,${keyword}`, `端末,${device}`, '',
    ITEM_HEADER, ...rows,
  ].join('\r\n'));
}

// ─── フィクスチャ: 日次_分析用レポート (店舗日次、CP932、月商クラスラベルにカンマ) ───
const BC = '月商別平均値（月商1,000万～2,999万）';
const STORE_HEADER = [
  '日付', '曜日',
  '売上金額（すべて）', '売上金額（PC）', '売上金額（楽天市場アプリ）', '売上金額（スマートフォン）',
  '売上件数（すべて）', '売上件数（PC）', '売上件数（楽天市場アプリ）', '売上件数（スマートフォン）',
  'アクセス人数（すべて）', 'アクセス人数（PC）', 'アクセス人数（楽天市場アプリ）', 'アクセス人数（スマートフォン）',
  '転換率（すべて）', '転換率（PC）', '転換率（楽天市場アプリ）', '転換率（スマートフォン）',
  '客単価（すべて）', '客単価（PC）', '客単価（楽天市場アプリ）', '客単価（スマートフォン）',
  'サブジャンルTOP10平均 売上金額（すべて）', 'サブジャンルTOP10平均 売上件数（すべて）',
  'サブジャンルTOP10平均 アクセス人数（すべて）', 'サブジャンルTOP10平均 転換率（すべて）',
  'サブジャンルTOP10平均 客単価（すべて）',
  `${BC} 売上金額（すべて）`, `${BC} 売上件数（すべて）`, `${BC} アクセス人数（すべて）`,
  `${BC} 転換率（すべて）`, `${BC} 客単価（すべて）`,
  '売上金額', '税額（外税額）', '送料額', 'クーポン値引額（店舗）', 'クーポン値引額（楽天）',
  '送料無料クーポン', 'のし・ラッピング代金', '決済手数料',
].map(h => `"${h}"`).join(',');
function storeRow(date, salesAll, accessAll, { future = false } = {}) {
  if (future) {
    // 未来日: 自店指標=0、ベンチマーク・費用は空
    return `"${date}","水","0","0","0","0","0","0","0","0","0","0","0","0","0.00","0.00","0.00","0.00","0","0","0","0","","","","","","","","","","","0","0","0","0","0","0","0","0"`;
  }
  return `"${date}","水","${salesAll}","50,000","60,000","40,000","30","10","12","8","${accessAll}","300","400","300","3.00","3.33","3.00","2.67","5,000","5,000","5,000","5,000","120000.5","25.2","900.1","2.80","4800.3","100000.1","20.5","800.9","2.50","5000.7","${salesAll}","13,636","5,000","2,000","3,000","500","300","4,500"`;
}
function storeCsv(rows) {
  return sjis(['"日次_分析用レポート"', '"データ対象期間,2026年07月01日～2026年07月31日"', STORE_HEADER, ...rows].join('\r\n'));
}

console.log('=== 1. 商品分析CSV取込 (UTF-8 BOM、SKU正規化、dim UPSERT) ===');
{
  const csv = itemCsv({ rows: [
    itemRow(1, 'ZZ1212-0002', 'テスト商品A', '12,800', 50, 99),
    itemRow(2, 'zuko10-sa', 'テスト商品B', '0', 10, 0),
  ] });
  const r = importDataFile(db, { name: '20260709_item_list.csv', buffer: csv, sha256: sha(csv), source: 'test' });
  check('取込ok', r.status === 'ok', JSON.stringify(r.results));
  const rows = db.prepare(`SELECT * FROM fact_rakuten_item_daily ORDER BY item_manage_number`).all();
  check('2行取込', rows.length === 2, `got ${rows.length}`);
  const a = rows.find(x => x.item_manage_number === 'zz1212-0002');
  check('SKUはLOWER正規化+raw保持', a?.raw_sku_code === 'ZZ1212-0002');
  check('カンマ入り売上がINTEGER円', a?.sales_yen === 12800, `got ${a?.sales_yen}`);
  check('date_jst=表示期間の日', a?.date_jst === '2026-07-09');
  check('%付き転換率がREAL', a?.cvr_pct === 4.0, `got ${a?.cvr_pct}`);
  check('在庫snapshot', a?.stock_qty === 99);
  const dim = db.prepare(`SELECT * FROM m_rakuten_items WHERE item_manage_number = 'zz1212-0002'`).get();
  check('dimに商品名', dim?.item_name === 'テスト商品A', JSON.stringify(dim));
  check('dimにジャンル', dim?.genre_path === '食品 > 菓子・スイーツ');
  check('dimに商品番号', dim?.item_number === 'zz1212-0002', `got ${dim?.item_number}`);
}

console.log('=== 2. 同一ファイル再投入 = duplicate (冪等) ===');
{
  const csv = itemCsv({ rows: [itemRow(1, 'ZZ1212-0002', 'テスト商品A', '12,800', 50, 99), itemRow(2, 'zuko10-sa', 'テスト商品B', '0', 10, 0)] });
  const r = importDataFile(db, { name: 'again.csv', buffer: csv, sha256: sha(csv), source: 'test' });
  check('duplicate', r.status === 'duplicate', r.status);
}

console.log('=== 3. 同日再DL (値変動) = UPSERT 上書き + dim 最新値化 ===');
{
  const csv = itemCsv({ rows: [itemRow(1, 'ZZ1212-0002', 'テスト商品A改', '15,000', 60, 88)] });
  const r = importDataFile(db, { name: 'redl.csv', buffer: csv, sha256: sha(csv), source: 'test' });
  check('取込ok', r.status === 'ok');
  check('updatedカウント', r.results[0].updated === 1 && r.results[0].inserted === 0, JSON.stringify(r.results[0]));
  const a = db.prepare(`SELECT * FROM fact_rakuten_item_daily WHERE item_manage_number = 'zz1212-0002'`).get();
  check('値が上書き', a.sales_yen === 15000 && a.stock_qty === 88);
  const dim = db.prepare(`SELECT item_name FROM m_rakuten_items WHERE item_manage_number = 'zz1212-0002'`).get();
  check('dim最新名', dim.item_name === 'テスト商品A改');
  const other = db.prepare(`SELECT * FROM fact_rakuten_item_daily WHERE item_manage_number = 'zuko10-sa'`).get();
  check('他行は不変', other?.access_users === 10);
}

console.log('=== 4. 商品分析ガード (複数日/キーワード/端末絞り込み) ===');
{
  const p1 = prepareDataFile('x.csv', itemCsv({ period: '2026年07月01日から2026年07月09日', rows: [itemRow(1, 'a-1', 'A', '0', 1, 1)] }));
  check('複数日period → error', !p1.ok && /1日単位/.test(p1.error), p1.error);
  const p2 = prepareDataFile('x.csv', itemCsv({ keyword: 'ぬいぐるみ', rows: [itemRow(1, 'a-1', 'A', '0', 1, 1)] }));
  check('キーワード絞り込み → error', !p2.ok && /キーワード/.test(p2.error), p2.error);
  const p3 = prepareDataFile('x.csv', itemCsv({ device: 'PC', rows: [itemRow(1, 'a-1', 'A', '0', 1, 1)] }));
  check('端末=PC → error', !p3.ok && /端末/.test(p3.error), p3.error);
  const p4 = prepareDataFile('x.csv', itemCsv({ rows: [itemRow(1, 'a-1', 'A', '0', 1, 1), itemRow(2, 'A-1', 'A2', '0', 2, 2)] }));
  check('ファイル内SKU重複 (case違い含む) → error', !p4.ok && /重複/.test(p4.error), p4.error);
  const p5 = prepareDataFile('x.csv', itemCsv({ rows: [itemRow(1, 'a-1', 'A', '0', 1, 1), '2,壊れた行'] }));
  check('列数不足の非空行 → error', !p5.ok && /列数不足/.test(p5.error), p5.error);
  const oneShort = itemRow(1, 'a-1', 'A', '0', 1, 1).replace(/,[^,]*$/, ''); // 末尾1列 (在庫数) 欠け
  const p5b = prepareDataFile('x.csv', itemCsv({ rows: [oneShort] }));
  check('末尾1列欠けも → error', !p5b.ok && /列数不足/.test(p5b.error), p5b.error);
  const p6 = prepareDataFile('x.csv', itemCsv({ rows: [itemRow(1, 'a-1', 'A', '0', 1, 1), ',,,'] }));
  check('全セル空行はスキップして正常取込', p6.ok && p6.records.length === 1, p6.error);
}

console.log('=== 5. 店舗日次CSV取込 (CP932、ベンチマークREAL、費用内訳、未来日/通算実績スキップ) ===');
{
  const csv = storeCsv([
    storeRow('2026年07月01日', '150,000', '1,000'),
    storeRow('2026年07月02日', '98,765', '876'),
    storeRow('2026年07月15日', '0', '0', { future: true }),
    `"通算実績","","5,000,000","1","1","1","100","1","1","1","10,000","1","1","1","1.0","1","1","1","5,000","1","1","1","","","","","","","","","","","5,000,000","454,545","10,000","5,000","8,000","1,000","500","150,000"`,
  ]);
  const r = importDataFile(db, { name: '20260701_20260731_日次_分析用レポート.csv', buffer: csv, sha256: sha(csv), source: 'test' });
  check('取込ok', r.status === 'ok', JSON.stringify(r.results));
  const rows = db.prepare(`SELECT * FROM fact_rakuten_store_daily ORDER BY date_jst`).all();
  check('実績2日のみ (未来日+通算実績はスキップ)', rows.length === 2, `got ${rows.length}: ${rows.map(x => x.date_jst).join(',')}`);
  const d1 = rows[0];
  check('date正規化', d1.date_jst === '2026-07-01');
  check('売上INTEGER', d1.sales_all_yen === 150000);
  check('端末別売上', d1.sales_pc_yen === 50000 && d1.sales_app_yen === 60000 && d1.sales_sp_yen === 40000);
  check('ベンチTOP10はREAL保持', d1.bench_top10_sales_yen === 120000.5, `got ${d1.bench_top10_sales_yen}`);
  check('月商クラスラベル抽出', d1.bench_class_label === '月商1,000万～2,999万', `got ${d1.bench_class_label}`);
  check('月商クラス値', d1.bench_class_sales_yen === 100000.1);
  check('費用内訳 (クーポン店舗/楽天/決済手数料)', d1.coupon_store_yen === 2000 && d1.coupon_rakuten_yen === 3000 && d1.settlement_fee_yen === 4500);
  check('税額 (外税)', d1.tax_out_yen === 13636);
}

console.log('=== 6. 店舗日次 再DL上書き + 全行未来日はエラー ===');
{
  const csv = storeCsv([storeRow('2026年07月01日', '160,000', '1,100')]);
  const r = importDataFile(db, { name: 'store-redl.csv', buffer: csv, sha256: sha(csv), source: 'test' });
  check('再DL取込ok', r.status === 'ok');
  check('updated=1', r.results[0].updated === 1 && r.results[0].inserted === 0, JSON.stringify(r.results[0]));
  const d1 = db.prepare(`SELECT sales_all_yen FROM fact_rakuten_store_daily WHERE date_jst = '2026-07-01'`).get();
  check('値上書き', d1.sales_all_yen === 160000);
  const p = prepareDataFile('future.csv', storeCsv([storeRow('2026年07月20日', '0', '0', { future: true })]));
  check('全行未来日 → error', !p.ok && /0件/.test(p.error), p.error);
  const p2 = prepareDataFile('mix.csv', storeCsv([storeRow('2026年07月03日', '1,000', '10'), storeRow('2026年07月20日', '0', '0', { future: true })]));
  check('未集計スキップ日数をlabelで可視化', p2.ok && /未集計1日スキップ/.test(p2.label), p2.label);
  const p3 = prepareDataFile('short.csv', storeCsv([`"2026年07月03日","水","1,000"`]));
  check('店舗日次の列数不足行 → error', !p3.ok && /列数不足/.test(p3.error), p3.error);
  const oneShort = storeRow('2026年07月03日', '1,000', '10').replace(/,"4,500"$/, ''); // 末尾1列 (決済手数料) 欠け
  const p4 = prepareDataFile('short1.csv', storeCsv([oneShort]));
  check('店舗日次の末尾1列欠けも → error', !p4.ok && /列数不足/.test(p4.error), p4.error);
}

console.log('=== 6b. 旧スキーマからの item_number 冪等 migration ===');
{
  const db2 = new Database(path.join(tmp, 'old-schema.db'));
  db2.exec(`CREATE TABLE m_rakuten_items (
    item_manage_number TEXT PRIMARY KEY, raw_sku_code TEXT, item_id INTEGER,
    catalog_id TEXT, item_name TEXT, genre_path TEXT, updated_at TEXT NOT NULL)`);
  ensureRakutenDataTables(db2);
  const cols = db2.prepare(`PRAGMA table_info(m_rakuten_items)`).all().map(x => x.name);
  check('item_number 列が追加される', cols.includes('item_number'), cols.join(','));
  ensureRakutenDataTables(db2); // 2回目も落ちない
  check('migration は冪等', true);
  db2.close();
}

console.log('=== 7. zip内1ファイル不正 = 全体rollback (1tx原子性) ===');
{
  const before = db.prepare(`SELECT COUNT(*) n FROM fact_rakuten_item_daily`).get().n;
  const good = itemCsv({ period: '2026年07月08日から2026年07月08日', rows: [itemRow(1, 'new-sku-1', 'N', '500', 5, 5)] });
  const bad = itemCsv({ keyword: '絞り込み', rows: [itemRow(1, 'new-sku-2', 'N2', '500', 5, 5)] });
  const buf = zipOf([['good.csv', good], ['bad.csv', bad]]);
  const r = importDataFile(db, { name: 'mixed.zip', buffer: buf, sha256: sha(buf), source: 'test' });
  check('status=error', r.status === 'error', r.status);
  const after = db.prepare(`SELECT COUNT(*) n FROM fact_rakuten_item_daily`).get().n;
  check('goodファイル分もrollback', after === before, `before=${before} after=${after}`);
  check('good側はskippedと記録', r.results.some(x => x.skipped && x.file.endsWith('good.csv')), JSON.stringify(r.results));
}

console.log('=== 8. 種別誤投入の案内 ===');
{
  const rpp = sjis(['"実行日時: 2026-07-09"', '"コントロールカラム","日付","商品管理番号"', '"","2026年06月","abc"'].join('\r\n'));
  const p = prepareDataFile('rpp.csv', rpp);
  check('RPPレポート → rakuten-ads/ 案内', !p.ok && /rakuten-ads/.test(p.error), p.error);
  const junk = utf8bom('あ,い,う\n1,2,3');
  const p2 = prepareDataFile('junk.csv', junk);
  check('不明CSV → 判別不能エラー', !p2.ok && /判別/.test(p2.error), p2.error);
}

console.log('=== 9. 取込ログ (raw_rakuten_data_import_log) ===');
{
  const logs = db.prepare(`SELECT status, COUNT(*) n FROM raw_rakuten_data_import_log GROUP BY status`).all();
  const m = Object.fromEntries(logs.map(x => [x.status, x.n]));
  check('ok/duplicate/error/skipped 全て記録', m.ok >= 4 && m.duplicate >= 1 && m.error >= 1 && m.skipped >= 1, JSON.stringify(m));
}

db.close();
fs.rmSync(tmp, { recursive: true, force: true });
console.log(`\n=== 結果: ${passed} passed / ${failed} failed ===`);
process.exitCode = failed > 0 ? 1 : 0;
