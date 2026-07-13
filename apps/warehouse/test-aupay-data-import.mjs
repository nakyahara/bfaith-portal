#!/usr/bin/env node
/**
 * test-aupay-data-import.mjs — au PAY分析CSV/JSON取込のスモークテスト (mall-csv-fetcher P1-A)
 *
 * 実DL物 (2026-07-12〜13 取得) の実測ヘッダ/前文/JSON構造を再現した合成フィクスチャで、
 * 判別〜セグメント正規化〜日次/月次分岐〜コホート縦持ち〜ガード〜UPSERT〜冪等〜原子性を検証。
 * DBはtemp (本番DB不触)。
 *
 * 実行: node apps/warehouse/test-aupay-data-import.mjs
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import Database from 'better-sqlite3';
import iconv from 'iconv-lite';
import { ensureAupayDataTables, importAupayFile, prepareAupayFile, segmentCode } from './aupay-data-lib.js';

let passed = 0, failed = 0;
function check(name, cond, detail = '') {
  if (cond) { console.log(`  ✅ ${name}`); passed++; }
  else { console.log(`  ❌ ${name} ${detail}`); failed++; }
}

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'adata-smoke-'));
const db = new Database(path.join(tmp, 'warehouse.db'));
db.pragma('journal_mode = WAL');
ensureAupayDataTables(db);

const sha = (b) => crypto.createHash('sha256').update(b).digest('hex');
const sjis = (s) => iconv.encode(s, 'Shift_JIS');
const CRLF = String.fromCharCode(13, 10);
const q = (arr) => arr.map((v) => `"${v}"`).join(',');

// 実測ヘッダ (逐語)
const SALES_HEADER = ['顧客セグメント名', '年月日', '売上高', '利用店舗原資クーポン', '利用モール原資クーポン',
  'Pontaポイント(au PAY マーケット限定含む)・auポイント利用合算', '利用ポイント', '購入回数', '購入個数',
  '来訪回数', '購入UU数', '訪問者(UU)', 'PV数', 'CVR', '平均購入個数', '平均購入単価', '平均来訪回数',
  '平均PV数(回遊数)', '売上に対する店舗原資クーポン利用額率(販促費率)',
  '売上に対するモール原資クーポン利用額率(販促費率)', 'ポイント利用額率(販促費率)'];

function salesCsv(dates, { preamble } = {}) {
  const lines = [
    `"抽出年月${dates[0].includes('/') && dates[0].split('/').length === 3 ? '日' : ''}：${preamble || `${dates[0]}～${dates[dates.length - 1]}`}"`,
    '"抽出条件："', '""',
    q(SALES_HEADER),
  ];
  for (const d of dates) {
    lines.push(q(['全顧客', d, '22971', '30', '100', '12576', '0', '22', '23', '202', '22', '197', '328', '10.89', '1.0', '1044', '1.0', '1.6', '0.13', '0.44', '54.75']));
    lines.push(q(['初回購入顧客(新規)', d, '18515', '30', '100', '9640', '0', '18', '19', '190', '18', '185', '311', '9.47', '1.1', '1029', '1.0', '1.6', '0.16', '0.54', '52.07']));
    lines.push(q(['2回購入以上(既存合算)', d, '4456', '0', '0', '2936', '0', '4', '4', '12', '4', '12', '17', '33.33', '1.0', '1114', '1.0', '1.4', '0.00', '0.00', '65.89']));
  }
  return sjis(lines.join(CRLF));
}

console.log('=== 0. セグメント正規化 ===');
{
  check('全顧客→all', segmentCode('全顧客').code === 'all');
  check('半角括弧', segmentCode('初回購入顧客(新規)').code === 'new');
  check('全角括弧も同一コード', segmentCode('初回購入顧客（新規）').code === 'new');
  check('N回購入は規則生成', segmentCode('5回購入顧客').code === 'repeat_5');
  check('N回以上(継続)', segmentCode('4回以上購入顧客（継続）').code === 'repeat_4plus');
  const u = segmentCode('謎の新セグメント');
  check('未知は unk_hash + warning', u.code.startsWith('unk_') && u.warning, JSON.stringify(u));
}

console.log('=== 1. 売上分析 (日次) ===');
{
  const csv = salesCsv(['2026/07/10']);
  const r = importAupayFile(db, { name: 'aupay_sales_D_d2026-07-10_x.csv', buffer: csv, sha256: sha(csv), source: 'test' });
  check('取込ok', r.status === 'ok', JSON.stringify(r.results));
  check('type=aupay_sales_daily', r.results[0]?.type === 'aupay_sales_daily', r.results[0]?.type);
  const rows = db.prepare(`SELECT * FROM fact_aupay_sales_daily ORDER BY segment_code`).all();
  check('3セグメント3行', rows.length === 3, `got ${rows.length}`);
  const all = rows.find((x) => x.segment_code === 'all');
  check('金額INTEGER/率REAL', all?.sales_yen === 22971 && all?.cvr_pct === 10.89 && all?.point_rate_pct === 54.75, JSON.stringify(all));
  check('segment_raw保持', all?.segment_raw === '全顧客');
  const monthlyN = db.prepare(`SELECT COUNT(*) n FROM fact_aupay_sales_monthly`).get().n;
  check('月次表には入らない', monthlyN === 0);
}

console.log('=== 2. 売上分析 (月次、複数月レンジ) ===');
{
  const csv = salesCsv(['2024/01', '2024/02'], { preamble: '2024/01～2024/02' });
  const r = importAupayFile(db, { name: 'aupay_sales_M_d2024-01-01_x.csv', buffer: csv, sha256: sha(csv), source: 'test' });
  check('取込ok', r.status === 'ok', JSON.stringify(r.results));
  check('type=aupay_sales_monthly', r.results[0]?.type === 'aupay_sales_monthly', r.results[0]?.type);
  const rows = db.prepare(`SELECT * FROM fact_aupay_sales_monthly ORDER BY date_jst, segment_code`).all();
  check('2月×3セグ=6行、月初日キー', rows.length === 6 && rows[0].date_jst === '2024-01-01' && rows[5].date_jst === '2024-02-01', JSON.stringify(rows.map((x) => x.date_jst)));
}

console.log('=== 3. 前文範囲と実データの突合 (クランプ検知) ===');
{
  const csv = salesCsv(['2026/07/09'], { preamble: '2026/07/10～2026/07/10' });
  const p = prepareAupayFile('aupay_sales_D_d2026-07-10_y.csv', csv);
  check('範囲外日付はエラー', !p.ok && /抽出範囲/.test(p.error), p.error);
}

console.log('=== 4. 参照元分析 ===');
{
  const header = ['顧客セグメント名', '年月日', '参照元チャネル', '来訪回数', '訪問者(UU)', '売上高', '購入回数', '購入個数', 'CVR', '平均購入個数', '平均購入単価'];
  const csv = sjis([
    '"抽出年月日：2026/07/10～2026/07/10"', '""',
    q(header),
    q(['全顧客', '2026/07/10', '検索', '9', '9', '2008', '2', '2', '22.22', '1.0', '1004']),
    q(['全顧客', '2026/07/10', 'その他', '98', '98', '20386', '19', '20', '19.39', '1.1', '1073']),
  ].join(CRLF));
  const r = importAupayFile(db, { name: 'aupay_referer_D_d2026-07-10_x.csv', buffer: csv, sha256: sha(csv), source: 'test' });
  check('取込ok', r.status === 'ok', JSON.stringify(r.results));
  const row = db.prepare(`SELECT * FROM fact_aupay_referer_daily WHERE channel='検索'`).get();
  check('チャネル別に行', row?.visits === 9 && row?.sales_yen === 2008, JSON.stringify(row));
}

console.log('=== 5. 検索分析 (流入KW) ===');
{
  const header = ['顧客セグメント名', '年月日', '流入キーワード名', '来訪回数', '訪問者(UU)'];
  const csv = sjis([
    '"抽出年月日：2026/07/10～2026/07/10"', '"抽出条件："', '""',
    q(header),
    q(['全顧客', '2026/07/10', 'アロマオイル', '1', '1']),
    q(['全顧客', '2026/07/10', 'サウナ ロウリュ', '3', '2']),
  ].join(CRLF));
  const r = importAupayFile(db, { name: 'aupay_search_D_d2026-07-10_x.csv', buffer: csv, sha256: sha(csv), source: 'test' });
  check('取込ok', r.status === 'ok', JSON.stringify(r.results));
  const n = db.prepare(`SELECT COUNT(*) n FROM fact_aupay_search_daily`).get().n;
  check('KW2行', n === 2, `got ${n}`);
}

console.log('=== 6. ページ分析 (セグメントなし) ===');
{
  const header = ['年月日', 'ページURL', 'PV数', '購入回数', '経由購入回数(経由CV)', '直帰率', '離脱率'];
  const csv = sjis([
    '"抽出年月日：2026/07/10～2026/07/10"', '""',
    q(header),
    q(['2026/07/10', 'https://wowma.jp/user/54318092', '7', '0', '2', '0', '0']),
    q(['2026/07/10', 'app:item758204052', '1', '0', '0', '0', '100']),
  ].join(CRLF));
  const r = importAupayFile(db, { name: 'aupay_page_D_d2026-07-10_x.csv', buffer: csv, sha256: sha(csv), source: 'test' });
  check('取込ok', r.status === 'ok', JSON.stringify(r.results));
  const row = db.prepare(`SELECT * FROM fact_aupay_page_daily WHERE page_url LIKE 'app:%'`).get();
  check('URL行+離脱率REAL', row?.exit_rate_pct === 100 && row?.via_orders === 0, JSON.stringify(row));
}

console.log('=== 7. 商品分析 ===');
{
  const header = ['顧客セグメント名', '年月日', '商品名', '商品カテゴリ', 'ロットナンバー', '売上高', '購入回数', '購入個数',
    '平均購入個数', '平均購入単価', '利用店舗原資クーポン', '利用モール原資クーポン', '購入UU数', '来訪回数',
    '来訪者数(UU)', 'PV数', 'CVR(訪問回数ベース)', 'CVR(UUベース)'];
  const csv = sjis([
    '"抽出年月日：2026/07/10～2026/07/10"', '"抽出条件："', '""',
    q(header),
    q(['全顧客', '2026/07/10', 'ロウリュ アロマウォーター 500ml', 'その他アロマグッズ', '756334596', '1500', '1', '1', '1.0', '1500', '0', '0', '1', '2', '2', '2', '50.00', '50.00']),
  ].join(CRLF));
  const r = importAupayFile(db, { name: 'aupay_product_D_d2026-07-10_x.csv', buffer: csv, sha256: sha(csv), source: 'test' });
  check('取込ok', r.status === 'ok', JSON.stringify(r.results));
  const row = db.prepare(`SELECT * FROM fact_aupay_product_daily WHERE lot_number='756334596'`).get();
  check('ロット行', row?.sales_yen === 1500 && row?.category === 'その他アロマグッズ', JSON.stringify(row));
}

console.log('=== 8. リピート分析 (コホート縦持ち+合計検算) ===');
{
  const csv = sjis([
    '"抽出年月：2026/05～2026/06"', '"抽出条件："', '""',
    '"2026/05起点"',
    q(['当月顧客セグメント名', '2025/05', '2025/06', '2026/05', 'リピート受注数合計']),
    q(['2回購入顧客', '1', '2', '3', '6']),
    '""',
    '"2026/06起点"',
    q(['当月顧客セグメント名', '2025/06', '2025/07', '2026/06', 'リピート受注数合計']),
    q(['2回購入顧客', '14', '3', '5', '22']),
    q(['4回以上購入顧客（継続）', '1', '0', '0', '1']),
  ].join(CRLF));
  const r = importAupayFile(db, { name: 'aupay_rf_M_d2026-05-01_x.csv', buffer: csv, sha256: sha(csv), source: 'test' });
  check('取込ok', r.status === 'ok', JSON.stringify(r.results));
  const rows = db.prepare(`SELECT * FROM fact_aupay_repeat_cohort_monthly ORDER BY date_jst, segment_code, target_month_jst`).all();
  check('縦持ち 3+3+3=9行', rows.length === 9, `got ${rows.length}`);
  const one = rows.find((x) => x.date_jst === '2026-06-01' && x.segment_code === 'repeat_2' && x.target_month_jst === '2025-06-01');
  check('起点×対象月×受注数', one?.orders === 14, JSON.stringify(one));
  check('4回以上(継続)=repeat_4plus', rows.some((x) => x.segment_code === 'repeat_4plus'));
  check('合計は行として格納しない', !rows.some((x) => x.target_month_jst === null));

  // 合計不一致は取込全体エラー
  const bad = sjis([
    '"抽出年月：2026/06～2026/06"', '""',
    '"2026/06起点"',
    q(['当月顧客セグメント名', '2025/06', 'リピート受注数合計']),
    q(['2回購入顧客', '2', '99']),
  ].join(CRLF));
  const rb = importAupayFile(db, { name: 'aupay_rf_M_d2026-06-01_bad.csv', buffer: bad, sha256: sha(bad), source: 'test' });
  check('合計不一致=error', rb.status === 'error' && /合計不一致/.test(rb.results[0].error), JSON.stringify(rb.results));
}

console.log('=== 9. PM広告 日次レポート (UTF-8 BOM) ===');
{
  const csv = Buffer.concat([Buffer.from([0xEF, 0xBB, 0xBF]), Buffer.from([
    '日付,広告表示回数,クリック回数,CTR,CPC,広告経由流通額,ROAS,広告費',
    '2026-06-01,104420,283,0.271,32.4,20800,2.27,9170',
    '2026-06-02,0,0,0.000,0.0,0,0.00,0',
  ].join('\n'), 'utf8')]);
  const r = importAupayFile(db, { name: 'aupay_pmad_d2026-06-01_x.csv', buffer: csv, sha256: sha(csv), source: 'test' });
  check('取込ok', r.status === 'ok', JSON.stringify(r.results));
  const row = db.prepare(`SELECT * FROM fact_aupay_pm_ad_daily WHERE date_jst='2026-06-01'`).get();
  check('imp/cost/ROAS', row?.impressions === 104420 && row?.cost_yen === 9170 && row?.roas === 2.27, JSON.stringify(row));

  // 期間集計行 (aggregate=monthly の予算期間) 混入はエラー
  const bad = Buffer.from('﻿日付,広告表示回数,クリック回数,CTR,CPC,広告経由流通額,ROAS,広告費\n"2020-08-26 - 2020-09-25",104420,283,0.0,32.4,20800,2.27,9170', 'utf8');
  const rb = importAupayFile(db, { name: 'aupay_pmad_d2020-08-26_bad.csv', buffer: bad, sha256: sha(bad), source: 'test' });
  check('予算期間行=error', rb.status === 'error' && /期間行/.test(rb.results[0].error), JSON.stringify(rb.results));
}

console.log('=== 10. PM検索クエリ (週次JSON) ===');
{
  const json = Buffer.from(JSON.stringify({
    kind: 'aupay_pm_query_weekly', week_start: '2026-07-06', week_end: '2026-07-12',
    fetched_at: '2026-07-13T00:00:00Z', source: 'test',
    rows: [
      { rank: 1, keyword: 'サンダル', impressions: 4529, clicks: 11, ctr_pct: 0.2 },
      { rank: 2, keyword: 'アロマストーン', impressions: 539, clicks: 15, ctr_pct: 2.8 },
    ],
  }), 'utf8');
  const r = importAupayFile(db, { name: 'aupay_pmquery_d2026-07-06_x.json', buffer: json, sha256: sha(json), source: 'test' });
  check('取込ok', r.status === 'ok', JSON.stringify(r.results));
  const row = db.prepare(`SELECT * FROM fact_aupay_pm_query_weekly WHERE keyword='サンダル'`).get();
  check('週次スナップ行', row?.date_jst === '2026-07-06' && row?.impressions === 4529, JSON.stringify(row));

  // ファイル名日付と week_start の不一致はエラー
  const r2 = importAupayFile(db, { name: 'aupay_pmquery_d2026-07-13_y.json', buffer: json, sha256: sha(json), source: 'test' });
  check('ファイル名日付不一致=error', r2.status === 'error' && /不一致/.test(r2.results[0].error), JSON.stringify(r2.results));
}

console.log('=== 11. 冪等 (sha256+論理日付の複合) ===');
{
  const csv = salesCsv(['2026/07/11']);
  const n1 = importAupayFile(db, { name: 'aupay_sales_D_d2026-07-11_a.csv', buffer: csv, sha256: sha(csv), source: 'test' });
  check('初回ok', n1.status === 'ok');
  const n2 = importAupayFile(db, { name: 'aupay_sales_D_d2026-07-11_b.csv', buffer: csv, sha256: sha(csv), source: 'test' });
  check('同一内容・同一日 = duplicate', n2.status === 'duplicate', n2.status);
  // 同一内容・別論理日はファイル名日付が違えば取り込む (前文で弾かれるのが正だが、冪等層では通す)
  const before = db.prepare(`SELECT COUNT(*) n FROM raw_aupay_data_import_log WHERE status='duplicate'`).get().n;
  check('duplicateログ記録', before >= 1);
}

console.log('=== 12. scope置換 (再取込で入れ替え・二重計上なし・消えた行の残留なし) ===');
{
  const v1 = salesCsv(['2026/07/12']);
  importAupayFile(db, { name: 'aupay_sales_D_d2026-07-12_v1.csv', buffer: v1, sha256: sha(v1), source: 'test' });
  // 補正版 (売上が変わった)
  const v2raw = iconv.decode(v1, 'Shift_JIS').replace('"22971"', '"30000"');
  const v2 = sjis(v2raw);
  const r = importAupayFile(db, { name: 'aupay_sales_D_d2026-07-12_v2.csv', buffer: v2, sha256: sha(v2), source: 'test' });
  check('再取込ok', r.status === 'ok', JSON.stringify(r.results));
  check('置換計上 (旧3行削除+3行insert)', r.results[0].updated === 3 && r.results[0].inserted === 3, JSON.stringify(r.results[0]));
  const row = db.prepare(`SELECT sales_yen FROM fact_aupay_sales_daily WHERE date_jst='2026-07-12' AND segment_code='all'`).get();
  check('値が置換される', row?.sales_yen === 30000, JSON.stringify(row));
  const n = db.prepare(`SELECT COUNT(*) n FROM fact_aupay_sales_daily WHERE date_jst='2026-07-12'`).get().n;
  check('行は増えない', n === 3, `got ${n}`);

  // 補正版でディメンション行が消えた場合、残留しない (Codex 実装レビュー R1 High)
  const PRH = ['顧客セグメント名', '年月日', '商品名', '商品カテゴリ', 'ロットナンバー', '売上高', '購入回数', '購入個数',
    '平均購入個数', '平均購入単価', '利用店舗原資クーポン', '利用モール原資クーポン', '購入UU数', '来訪回数',
    '来訪者数(UU)', 'PV数', 'CVR(訪問回数ベース)', 'CVR(UUベース)'];
  const pv1 = sjis([
    '"抽出年月日：2026/07/12～2026/07/12"', '""', q(PRH),
    q(['全顧客', '2026/07/12', '商品A', 'カテゴリ', 'lotA', ...Array(13).fill('1')]),
    q(['全顧客', '2026/07/12', '商品B', 'カテゴリ', 'lotB', ...Array(13).fill('1')]),
  ].join(CRLF));
  importAupayFile(db, { name: 'aupay_product_D_d2026-07-12_v1.csv', buffer: pv1, sha256: sha(pv1), source: 'test' });
  const pv2 = sjis([
    '"抽出年月日：2026/07/12～2026/07/12"', '""', q(PRH),
    q(['全顧客', '2026/07/12', '商品A', 'カテゴリ', 'lotA', ...Array(13).fill('2')]),
  ].join(CRLF));
  const r2 = importAupayFile(db, { name: 'aupay_product_D_d2026-07-12_v2.csv', buffer: pv2, sha256: sha(pv2), source: 'test' });
  check('補正版取込ok', r2.status === 'ok', JSON.stringify(r2.results));
  const lots = db.prepare(`SELECT lot_number FROM fact_aupay_product_daily WHERE date_jst='2026-07-12'`).all().map((x) => x.lot_number);
  check('消えたlotBが残留しない', lots.length === 1 && lots[0] === 'lotA', JSON.stringify(lots));
}

console.log('=== 13. ガード類 ===');
{
  // エラーCSV (シェア分析 PME1303 実測)
  const err = sjis('PME1303:抽出条件を満たす店舗シェア分析情報はありません。');
  const p1 = prepareAupayFile('x.csv', err);
  check('WOWエラーCSVを明示検出', !p1.ok && /エラーCSV/.test(p1.error), p1.error);

  // 未知ヘッダ
  const unk = sjis('a,b,c\n1,2,3');
  const p2 = prepareAupayFile('y.csv', unk);
  check('未知種別はエラー', !p2.ok && /判別できません/.test(p2.error), p2.error);

  // 列数不一致
  const badCols = sjis([
    '"抽出年月日：2026/07/10～2026/07/10"', '""',
    q(SALES_HEADER),
    '"全顧客","2026/07/10","1"',
  ].join(CRLF));
  const p3 = prepareAupayFile('z.csv', badCols);
  check('列数不一致はエラー', !p3.ok && /列数不一致/.test(p3.error), p3.error);

  // 日次と月次の混在
  const mixed = sjis([
    '"抽出年月日：2026/07/01～2026/07/10"', '""',
    q(SALES_HEADER),
    q(['全顧客', '2026/07/10', ...Array(19).fill('1')]),
    q(['全顧客', '2026/07', ...Array(19).fill('1')]),
  ].join(CRLF));
  const p4 = prepareAupayFile('w.csv', mixed);
  check('日次/月次混在はエラー', !p4.ok && /混在/.test(p4.error), p4.error);

  // 合計行はスキップ (エラーにしない)
  const withTotal = sjis([
    '"抽出年月日：2026/07/10～2026/07/10"', '""',
    q(SALES_HEADER),
    q(['全顧客', '2026/07/10', ...Array(19).fill('1')]),
    q(['全顧客', '合計', ...Array(19).fill('9999')]),
  ].join(CRLF));
  const p5 = prepareAupayFile('t.csv', withTotal);
  check('合計行はスキップ', p5.ok && p5.records.length === 1, p5.ok ? `${p5.records.length}件` : p5.error);
}

console.log('=== 14. 原子性 (zip内の1ファイル失敗で全ロールバック) ===');
{
  const { default: AdmZip } = await import('adm-zip');
  const zip = new AdmZip();
  zip.addFile('good.csv', salesCsv(['2026/07/13']));
  zip.addFile('bad.csv', sjis('a,b\n1,2'));
  const buf = zip.toBuffer();
  const before = db.prepare(`SELECT COUNT(*) n FROM fact_aupay_sales_daily`).get().n;
  const r = importAupayFile(db, { name: 'aupay_mixed_d2026-07-13_x.zip', buffer: buf, sha256: sha(buf), source: 'test' });
  check('zip取込はerror', r.status === 'error', r.status);
  const after = db.prepare(`SELECT COUNT(*) n FROM fact_aupay_sales_daily`).get().n;
  check('良ファイル分もロールバック', after === before, `${before}→${after}`);
}

console.log('=== 14b. zip内の同種別×期間重複はエラー (scope置換の相殺防止) ===');
{
  const { default: AdmZip } = await import('adm-zip');
  const zip = new AdmZip();
  const a = salesCsv(['2026/07/14']);
  const bRaw = iconv.decode(a, 'Shift_JIS').replace('"22971"', '"11111"');
  zip.addFile('a.csv', a);
  zip.addFile('b.csv', sjis(bRaw));
  const buf = zip.toBuffer();
  const r = importAupayFile(db, { name: 'aupay_dup_d2026-07-14_x.zip', buffer: buf, sha256: sha(buf), source: 'test' });
  check('期間重複zipはerror', r.status === 'error' && r.results.some((x) => /期間重複/.test(x.error || '')), JSON.stringify(r.results).slice(0, 200));
  const n = db.prepare(`SELECT COUNT(*) n FROM fact_aupay_sales_daily WHERE date_jst='2026-07-14'`).get().n;
  check('何も書き込まれない', n === 0, `got ${n}`);
}

console.log('=== 15. 未知セグメント warning (取込は成功) ===');
{
  const csv = sjis([
    '"抽出年月日：2026/07/09～2026/07/09"', '""',
    q(SALES_HEADER),
    q(['未来の新セグメント', '2026/07/09', ...Array(19).fill('1')]),
  ].join(CRLF));
  const r = importAupayFile(db, { name: 'aupay_sales_D_d2026-07-09_x.csv', buffer: csv, sha256: sha(csv), source: 'test' });
  check('取込ok', r.status === 'ok', JSON.stringify(r.results));
  check('warning付与', (r.results[0].warnings || []).some((w) => /未知の顧客セグメント/.test(w)), JSON.stringify(r.results[0].warnings));
  const log = db.prepare(`SELECT message FROM raw_aupay_data_import_log WHERE file_name LIKE '%2026-07-09%' AND status='ok'`).get();
  check('import logのmessageに記録', /未知の顧客セグメント/.test(log?.message || ''), log?.message);
}

db.close();
fs.rmSync(tmp, { recursive: true, force: true });
console.log(`\n=== 結果: ${passed} passed / ${failed} failed ===`);
process.exit(failed > 0 ? 1 : 0);
