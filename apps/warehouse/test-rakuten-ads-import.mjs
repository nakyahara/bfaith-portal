#!/usr/bin/env node
/**
 * test-rakuten-ads-import.mjs — 楽天RPP取込のスモークテスト (mall-csv-fetcher P1)
 *
 * 実DLファイル (2026-07-09 取得の全商品レポートZIP) で実測したヘッダ46列を再現した
 * 合成フィクスチャで、パース〜UPSERT〜冪等〜原子性を検証する。DBはtempに作る (本番DB不触)。
 *
 * 実行: node apps/warehouse/test-rakuten-ads-import.mjs
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import Database from 'better-sqlite3';
import AdmZip from 'adm-zip';
import iconv from 'iconv-lite';
import { ensureRppTables, importPhysicalFile, prepareFile } from './rakuten-ads-rpp-lib.js';

let passed = 0, failed = 0;
function check(name, cond, detail = '') {
  if (cond) { console.log(`  ✅ ${name}`); passed++; }
  else { console.log(`  ❌ ${name} ${detail}`); failed++; }
}

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'rpp-smoke-'));
const db = new Database(path.join(tmp, 'warehouse.db'));
db.pragma('journal_mode = WAL');
ensureRppTables(db);

const sha = (b) => crypto.createHash('sha256').update(b).digest('hex');
const sjis = (s) => iconv.encode(s, 'Shift_JIS');

// 実測ヘッダ (2026-07-09 DL の全商品レポート、46列)
const PRODUCT_HEADER = '"コントロールカラム","日付","商品ページURL","商品管理番号","入札単価","CTR(%)","商品CPC","クリック数(合計)","実績額(合計)","CPC実績(合計)","クリック数(新規)","実績額(新規)","CPC実績(新規)","クリック数(既存)","実績額(既存)","CPC実績(既存)","売上金額(合計12時間)","売上件数(合計12時間)","CVR(合計12時間)(%)","ROAS(合計12時間)(%)","注文獲得単価(合計12時間)","売上金額(合計720時間)","売上件数(合計720時間)","CVR(合計720時間)(%)","ROAS(合計720時間)(%)","注文獲得単価(合計720時間)","売上金額(新規12時間)","売上件数(新規12時間)","CVR(新規12時間)(%)","ROAS(新規12時間)(%)","注文獲得単価(新規12時間)","売上金額(新規720時間)","売上件数(新規720時間)","CVR(新規720時間)(%)","ROAS(新規720時間)(%)","注文獲得単価(新規720時間)","売上金額(既存12時間)","売上件数(既存12時間)","CVR(既存12時間)(%)","ROAS(既存12時間)(%)","注文獲得単価(既存12時間)","売上金額(既存720時間)","売上件数(既存720時間)","CVR(既存720時間)(%)","ROAS(既存720時間)(%)","注文獲得単価(既存720時間)"';
function productRow(period, sku, clicks, adCost, sales720) {
  const z = '"0"';
  return `"","${period}","https://item.rakuten.co.jp/b-faith/${sku}/","${sku}","20","0.06","","${clicks}","${adCost}","10",${z},${z},${z},${z},${z},${z},"0","0","0.00","0.00","0","${sales720}","1","0.00","0.00","0",${z},${z},"0.00","0.00",${z},${z},${z},"0.00","0.00",${z},${z},${z},"0.00","0.00",${z},${z},${z},"0.00","0.00",${z}`;
}
function productCsv(periodLabel, rows) {
  return sjis([
    '"実行日時: 2026-07-09 22:00:00"', '',
    '"検索条件"', '"集計単位: 商品別"', `"集計期間: ${periodLabel}"`,
    '"■■■■■商品・キーワードCPCを設定される際にはここから上は行ごと削除してください■■■■■"',
    PRODUCT_HEADER, ...rows,
  ].join('\r\n'));
}
function zipOf(entries) {
  const z = new AdmZip();
  for (const [name, buf] of entries) z.addFile(name, buf);
  return z.toBuffer();
}

console.log('=== 1. 商品別×月ごと ZIP取込 (実測ヘッダ、カンマ入り数値、大文字SKU) ===');
{
  const csv = productCsv('月ごとに集計 2026-06-01 - 2026-06-30', [
    productRow('2026年06月', 'zz1212-0002', '10', '1,540', '12,800'),
    productRow('2026年06月', 'ZUKO10-SA', '3', '30', '0'),
  ]);
  const buf = zipOf([['rpp_item_202606_1.csv', csv]]);
  const r = importPhysicalFile(db, { name: 'rpp_item_202606.zip', buffer: buf, sha256: sha(buf) });
  check('取込ok', r.status === 'ok', JSON.stringify(r.results));
  const rows = db.prepare(`SELECT * FROM fact_rakuten_ads_rpp ORDER BY item_manage_number`).all();
  check('2行取込', rows.length === 2, `got ${rows.length}`);
  check('カンマ入り金額がINTEGER円', rows.find(x => x.item_manage_number === 'zz1212-0002')?.ad_cost_yen === 1540);
  check('SKUはLOWER正規化+raw保持', rows.find(x => x.item_manage_number === 'zuko10-sa')?.raw_sku_code === 'ZUKO10-SA');
  check('month_ym正規化', rows.every(x => x.month_ym === '2026-06'));
}

console.log('=== 2. 同一ファイル再投入 = duplicate (冪等) ===');
{
  const csv = productCsv('月ごとに集計 2026-06-01 - 2026-06-30', [
    productRow('2026年06月', 'zz1212-0002', '10', '1,540', '12,800'),
    productRow('2026年06月', 'ZUKO10-SA', '3', '30', '0'),
  ]);
  const buf = zipOf([['rpp_item_202606_1.csv', csv]]);
  // zip再圧縮はタイムスタンプでバイト列が変わり得るため、CSV単体で同一バイト列を再投入
  const r1 = importPhysicalFile(db, { name: 'same.csv', buffer: csv, sha256: sha(csv) });
  const r2 = importPhysicalFile(db, { name: 'same-again.csv', buffer: csv, sha256: sha(csv) });
  check('1回目ok', r1.status === 'ok');
  check('2回目duplicate', r2.status === 'duplicate', r2.status);
}

console.log('=== 3. 720h遡及UPSERT (同PK再取込で値更新、行は増えない) ===');
{
  const csv = productCsv('月ごとに集計 2026-06-01 - 2026-06-30', [
    productRow('2026年06月', 'zz1212-0002', '12', '1,600', '15,000'),
  ]);
  const r = importPhysicalFile(db, { name: 'revised.csv', buffer: csv, sha256: sha(csv) });
  check('取込ok', r.status === 'ok');
  check('updatedで上書き', r.results[0].updated === 1 && r.results[0].inserted === 0, JSON.stringify(r.results[0]));
  const row = db.prepare(`SELECT * FROM fact_rakuten_ads_rpp WHERE item_manage_number='zz1212-0002'`).get();
  check('値が更新', row.ad_cost_yen === 1600 && row.clicks === 12);
  check('行数不変', db.prepare(`SELECT COUNT(*) c FROM fact_rakuten_ads_rpp`).get().c === 2);
}

console.log('=== 4. 複数月にまたがる期間 = エラー (全期間DLの誤投入防止) ===');
{
  const csv = productCsv('全期間で集計 2026-06-01 - 2026-07-08', [
    productRow('2026年06月01日～2026年07月08日', 'span-item', '1', '10', '0'),
  ]);
  const r = importPhysicalFile(db, { name: 'span.csv', buffer: csv, sha256: sha(csv) });
  check('spansはerror', r.status === 'error', r.status);
  check('月ごと案内メッセージ', /月ごとに表示/.test(r.results[0].error), r.results[0].error);
}

console.log('=== 4b. 商品別ファイル内PK重複 = エラー (黙って後勝ち統合しない) ===');
{
  const csv = productCsv('月ごとに集計 2026-06-01 - 2026-06-30', [
    productRow('2026年06月', 'dup-item', '1', '10', '0'),
    productRow('2026年06月', 'DUP-ITEM', '2', '20', '0'), // LOWER正規化で同一PKに潰れるケース
  ]);
  const r = importPhysicalFile(db, { name: 'dup.csv', buffer: csv, sha256: sha(csv) });
  check('重複はerror', r.status === 'error', r.status);
  check('重複キー例示', /同一キーの行が複数/.test(r.results[0].error), r.results[0].error);
  check('取込されていない', !db.prepare(`SELECT 1 FROM fact_rakuten_ads_rpp WHERE item_manage_number='dup-item'`).get());
}

console.log('=== 5. zip原子性 (良CSV+不良CSV → 全体rollback) ===');
{
  const good = productCsv('月ごとに集計 2026-05-01 - 2026-05-31', [
    productRow('2026年05月', 'atomic-test', '1', '10', '0'),
  ]);
  const bad = sjis('"へんなファイル"\r\n"1","2"');
  const buf = zipOf([['good.csv', good], ['bad.csv', bad]]);
  const r = importPhysicalFile(db, { name: 'mixed.zip', buffer: buf, sha256: sha(buf) });
  check('zip全体がerror', r.status === 'error', r.status);
  check('良CSV分もrollback', !db.prepare(`SELECT 1 FROM fact_rakuten_ads_rpp WHERE item_manage_number='atomic-test'`).get());
}

console.log('=== 6. 日次 (すべての広告×日ごと) ===');
{
  const csv = sjis([
    '"実行日時: 2026-07-09 22:00:00"', '"検索条件"', '"集計単位: すべて"',
    '"集計期間: 日ごとに集計 2026-07-01 - 2026-07-08"',
    '"日付","クリック数(合計)","実績額(合計)","CPC実績(合計)","CTR(%)","売上金額(合計12時間)","売上件数(合計12時間)","売上金額(合計720時間)","売上件数(合計720時間)","CVR(合計720時間)(%)","ROAS(合計720時間)(%)"',
    '"2026年07月05日","120","1,540","12.8","0.55","3000","2","12,800","8","6.67","831.17"',
    '"2026年07月06日","15","170","11.3","0.21","0","0","1,200","1","6.67","705.88"',
  ].join('\r\n'));
  const r = importPhysicalFile(db, { name: 'daily.csv', buffer: csv, sha256: sha(csv) });
  check('取込ok', r.status === 'ok', JSON.stringify(r.results));
  const rows = db.prepare(`SELECT * FROM fact_rakuten_ads_rpp_daily ORDER BY date_jst`).all();
  check('2行 (date_jst正規化)', rows.length === 2 && rows[0].date_jst === '2026-07-05');
  check('金額INTEGER', rows[0].ad_cost_yen === 1540 && rows[0].sales_720h_yen === 12800);
}

console.log('=== 7. キーワードレポート = 明示スキップ (未使用) ===');
{
  const csv = sjis([
    '"日付","商品管理番号","キーワード","クリック数(合計)","実績額(合計)","売上金額(合計720時間)","売上件数(合計720時間)"',
    '"2026年06月","item-1","お香","1","10","0","0"',
  ].join('\r\n'));
  const r = importPhysicalFile(db, { name: 'keyword.csv', buffer: csv, sha256: sha(csv) });
  check('skipped', r.status === 'skipped', r.status);
  check('理由メッセージ', /キーワードレポートは取込対象外/.test(r.results[0].error), r.results[0].error);
}

console.log('=== 8. 種類不明 = エラー (ヘッダ一覧つき) ===');
{
  const csv = Buffer.from('hello,world,foo,bar,baz\n1,2,3,4,5\n');
  const r = importPhysicalFile(db, { name: 'garbage.csv', buffer: csv, sha256: sha(csv) });
  check('unknownはerror', r.status === 'error' && /判別できません/.test(r.results[0].error), JSON.stringify(r.results[0]));
}

console.log('=== 9. prepareFile: ファイルレベル期間の抽出 ===');
{
  const csv = productCsv('全期間で集計 2026-06-01 - 2026-06-30', [
    productRow('2026年06月', 'x-1', '1', '10', '0'),
  ]);
  const p = prepareFile('x.csv', csv);
  check('前置行の日付範囲から抽出', p.ok && p.reportStart === '2026-06-01' && p.reportEnd === '2026-06-30',
    JSON.stringify({ s: p.reportStart, e: p.reportEnd }));

  // 月ごとDLの実測形式「月ごとに集計 2026-07 - 2026-07」+ 自動DL保存名からのフォールバック
  const csvM = productCsv('月ごとに集計 2026-07 - 2026-07', [
    productRow('2026年07月', 'x-2', '1', '10', '0'),
  ]);
  const p2 = prepareFile('rpp_rpp_item_monthly_2026-07-01_2026-07-08_20260709.zip/inner.csv', csvM);
  check('ファイル名の日付範囲を優先', p2.ok && p2.reportStart === '2026-07-01' && p2.reportEnd === '2026-07-08',
    JSON.stringify({ s: p2.reportStart, e: p2.reportEnd }));
  const p3 = prepareFile('manual-upload.csv', csvM);
  check('月範囲のみ→月初/月末', p3.ok && p3.reportStart === '2026-07-01' && p3.reportEnd === '2026-07-31',
    JSON.stringify({ s: p3.reportStart, e: p3.reportEnd }));
}

console.log('=== 10. 取込ログ整合 ===');
{
  const logs = db.prepare(`SELECT status, COUNT(*) c FROM raw_rakuten_ads_import_log GROUP BY status ORDER BY status`).all();
  console.log('  log:', JSON.stringify(logs));
  const okLogs = db.prepare(`SELECT * FROM raw_rakuten_ads_import_log WHERE status='ok' AND file_type='rpp_product' LIMIT 1`).get();
  check('okログにsha/期間/件数', !!okLogs && okLogs.file_sha256.length === 64 && okLogs.date_from === '2026-06');
}

db.close();
fs.rmSync(tmp, { recursive: true, force: true });
console.log(`\n=== smoke: ${passed} passed / ${failed} failed ===`);
process.exit(failed > 0 ? 1 : 0);
