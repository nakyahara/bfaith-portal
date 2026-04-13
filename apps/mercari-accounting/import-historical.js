/**
 * メルカリShops ヒストリカルデータ初期インポート
 *
 * Excelファイル「メルカリshops売上集計.xlsx」から過去の確定データを
 * mart_mercari_monthly_summary テーブルにインポートする。
 *
 * 注: 過去データは「セグメント1と2を半分ずつ」としていたxlsx運用に準拠。
 *     売上分類3・輸出セグメントは存在しない。送料・クーポンは全0。
 *     税率内訳はないため全額10%として格納。
 *
 * 使い方:
 *   node apps/mercari-accounting/import-historical.js
 *
 * リモート実行（Render等）:
 *   POST /apps/mercari-accounting/import-history
 *   Header: x-import-key: bfaith-import-2026
 *   Body: { months: [{ yearMonth, bySegment, pfFee, shippingFee, couponTotal }, ...] }
 */
import Database from 'better-sqlite3';
import path from 'path';
import fs from 'fs';

const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), 'data');
const DB_FILE = path.join(DATA_DIR, 'warehouse-mirror.db');

if (!fs.existsSync(DB_FILE)) {
  console.error('ERROR: warehouse-mirror.db が見つかりません:', DB_FILE);
  console.error('ポータルを一度起動してDBを初期化してから実行してください。');
  process.exit(1);
}

const db = new Database(DB_FILE);
db.pragma('journal_mode = WAL');

db.exec(`CREATE TABLE IF NOT EXISTS mart_mercari_monthly_summary (
  id                INTEGER PRIMARY KEY AUTOINCREMENT,
  year_month        TEXT NOT NULL UNIQUE,
  total_rows        INTEGER,
  resolved_count    INTEGER,
  unresolved_count  INTEGER,
  by_tax            TEXT,
  by_segment        TEXT,
  excluded          TEXT,
  mf_row            TEXT,
  pf_fee            REAL DEFAULT 0,
  shipping_fee      REAL DEFAULT 0,
  coupon_total      REAL DEFAULT 0,
  confirmed_at      TEXT NOT NULL
)`);

// ─── 売上ヒストリカル: [年月, セグメント, 売上, 原価] ───
const salesHistorical = [
  ['2022-07', 1, 40980, 18531], ['2022-07', 2, 40980, 20883],
  ['2022-08', 1, 33802, 14646], ['2022-08', 2, 33802, 15470],
  ['2022-09', 1, 44234, 19341], ['2022-09', 2, 44234, 20404],
  ['2022-10', 1, 64510, 27593], ['2022-10', 2, 64510, 29469],
  ['2022-11', 1, 52221, 22733], ['2022-11', 2, 52221, 23734],
  ['2022-12', 1, 47041, 20470], ['2022-12', 2, 47041, 21564],
  ['2023-01', 1, 70131, 30639], ['2023-01', 2, 70131, 32024],
  ['2023-02', 1, 45217, 19589], ['2023-02', 2, 45217, 20557],
  ['2023-03', 1, 41150, 17950], ['2023-03', 2, 41150, 18548],
  ['2023-04', 1, 40788, 17906], ['2023-04', 2, 40788, 18262],
  ['2023-05', 1, 89639, 40107], ['2023-05', 2, 89639, 50648],
  ['2023-06', 1, 171606, 75808], ['2023-06', 2, 171606, 79032],
  ['2023-07', 1, 156208, 71941], ['2023-07', 2, 156208, 71941],
  ['2023-08', 1, 88742, 39614], ['2023-08', 2, 88742, 39988],
  ['2023-09', 1, 69742, 30777], ['2023-09', 2, 69742, 31940],
  ['2023-10', 1, 120940, 52157], ['2023-10', 2, 120940, 55310],
  ['2023-11', 1, 102427, 44529], ['2023-11', 2, 102427, 47256],
  ['2023-12', 1, 216735, 94743], ['2023-12', 2, 216735, 99624],
  ['2024-01', 1, 311343, 135081], ['2024-01', 2, 311343, 140271],
  ['2024-02', 1, 258928, 112340], ['2024-02', 2, 258928, 116656],
  ['2024-03', 1, 492485, 211670], ['2024-03', 2, 492485, 221089],
  ['2024-04', 1, 367780, 158072], ['2024-04', 2, 367780, 165106],
  ['2024-05', 1, 358574, 154115], ['2024-05', 2, 358574, 160973],
  ['2024-06', 1, 561322, 241256], ['2024-06', 2, 561322, 251992],
  ['2024-07', 1, 388664, 167048], ['2024-07', 2, 388664, 174481],
  ['2024-08', 1, 341426, 146745], ['2024-08', 2, 341426, 153275],
  ['2024-09', 1, 568002, 244128], ['2024-09', 2, 568002, 254991],
  ['2024-10', 1, 279610, 120176], ['2024-10', 2, 279610, 125524],
  ['2024-11', 1, 290832, 125000], ['2024-11', 2, 290832, 130562],
  ['2024-12', 1, 448864, 192922], ['2024-12', 2, 448864, 201507],
  ['2025-01', 1, 301662, 129654], ['2025-01', 2, 301662, 135424],
  ['2025-02', 1, 305626, 131358], ['2025-02', 2, 305626, 137203],
  ['2025-03', 1, 364751, 156770], ['2025-03', 2, 364751, 163746],
  ['2025-04', 1, 335444, 144174], ['2025-04', 2, 335444, 150589],
  ['2025-05', 1, 383510, 164833], ['2025-05', 2, 383510, 172168],
  ['2025-06', 1, 370992, 159453], ['2025-06', 2, 370992, 166548],
  ['2025-07', 1, 333144, 143185], ['2025-07', 2, 333144, 149557],
  ['2025-08', 1, 300051, 128962], ['2025-08', 2, 300051, 134701],
  ['2025-09', 1, 276622, 118892], ['2025-09', 2, 276622, 124182],
  ['2025-10', 1, 274910, 118156], ['2025-10', 2, 274910, 123414],
  ['2025-11', 1, 287520, 123576], ['2025-11', 2, 287520, 129075],
  ['2025-12', 1, 310029, 133251], ['2025-12', 2, 310029, 139180],
  ['2026-01', 1, 355866, 152952], ['2026-01', 2, 355866, 159758],
  ['2026-02', 1, 317042, 136265], ['2026-02', 2, 317042, 142328],
];

// ─── 変動費ヒストリカル: [年月, PF手数料, 送料, クーポン] ───
const feeHistorical = [
  ['2022-07', 8176, 0, 0], ['2022-08', 6745, 0, 0], ['2022-09', 8834, 0, 0],
  ['2022-10', 12885, 0, 0], ['2022-11', 10421, 0, 0], ['2022-12', 9389, 0, 0],
  ['2023-01', 13992, 0, 0], ['2023-02', 9030, 0, 0], ['2023-03', 8221, 0, 0],
  ['2023-04', 8145, 0, 0], ['2023-05', 17898, 0, 0], ['2023-06', 34156, 0, 0],
  ['2023-07', 31156, 0, 0], ['2023-08', 17699, 0, 0], ['2023-09', 13910, 0, 0],
  ['2023-10', 24110, 0, 0], ['2023-11', 20435, 0, 0], ['2023-12', 43248, 0, 0],
  ['2024-01', 62130, 0, 0], ['2024-02', 51657, 0, 0], ['2024-03', 98219, 0, 0],
  ['2024-04', 73381, 0, 0], ['2024-05', 71508, 0, 0], ['2024-06', 111919, 0, 0],
  ['2024-07', 77448, 0, 0], ['2024-08', 67547, 0, 0], ['2024-09', 113230, 0, 0],
  ['2024-10', 55702, 0, 0], ['2024-11', 58532, 0, 0], ['2024-12', 89868, 0, 0],
  ['2025-01', 60121, 0, 0], ['2025-02', 60925, 0, 0], ['2025-03', 74382, 0, 0],
  ['2025-04', 68056, 0, 0], ['2025-05', 76479, 0, 0], ['2025-06', 74475, 0, 0],
  ['2025-07', 66399, 0, 0], ['2025-08', 60469, 0, 0], ['2025-09', 56017, 0, 0],
  ['2025-10', 54936, 0, 0], ['2025-11', 57305, 0, 0], ['2025-12', 62052, 0, 0],
  ['2026-01', 71081, 0, 0], ['2026-02', 63190, 0, 0],
];

// ─── 集計 ───

const monthlyData = new Map();

function emptyRow() {
  return { 売上合計: 0, クーポン値引額: 0, クーポン値引後売上: 0, 原価合計: 0, 行数: 0 };
}

for (const [ym, seg, sales, cost] of salesHistorical) {
  if (!monthlyData.has(ym)) {
    monthlyData.set(ym, { bySegment: {}, pfFee: 0, shippingFee: 0, couponTotal: 0 });
  }
  const d = monthlyData.get(ym);
  const segRow = emptyRow();
  segRow.売上合計 = sales;
  segRow.クーポン値引後売上 = sales;
  segRow.原価合計 = cost;
  segRow.原価率 = sales > 0 ? (cost / sales * 100).toFixed(1) : '0.0';
  d.bySegment[String(seg)] = segRow;
}

for (const [ym, pfFee, shippingFee, couponTotal] of feeHistorical) {
  if (!monthlyData.has(ym)) {
    monthlyData.set(ym, { bySegment: {}, pfFee: 0, shippingFee: 0, couponTotal: 0 });
  }
  const d = monthlyData.get(ym);
  d.pfFee = pfFee;
  d.shippingFee = shippingFee;
  d.couponTotal = couponTotal;
}

// ─── DB投入 ───

const insertStmt = db.prepare(`INSERT OR REPLACE INTO mart_mercari_monthly_summary
  (year_month, total_rows, resolved_count, unresolved_count,
   by_tax, by_segment, excluded, mf_row, pf_fee, shipping_fee, coupon_total, confirmed_at)
  VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
`);

const now = new Date().toISOString().replace('T', ' ').slice(0, 19);
let imported = 0, skipped = 0;

const insertMany = db.transaction(() => {
  for (const [ym, data] of [...monthlyData.entries()].sort()) {
    const totalSales = Object.values(data.bySegment).reduce((s, v) => s + (v.売上合計 || 0), 0);
    const totalCost = Object.values(data.bySegment).reduce((s, v) => s + (v.原価合計 || 0), 0);

    if (totalSales === 0 && data.pfFee === 0) {
      skipped++;
      continue;
    }

    // 全額10%扱い（過去データは税率内訳なし）
    const byTax = {
      '10': { 売上合計: totalSales, クーポン値引額: 0, クーポン値引後売上: totalSales, 原価合計: totalCost, 行数: 0 },
      '8': emptyRow(),
      'other': emptyRow(),
    };

    // セグメント3は未使用だがUIの整合性のため空行を入れる
    if (!data.bySegment['3']) data.bySegment['3'] = emptyRow();
    if (!data.bySegment['other']) data.bySegment['other'] = emptyRow();
    for (const seg of Object.values(data.bySegment)) {
      if (seg.原価率 == null) {
        seg.原価率 = seg.クーポン値引後売上 > 0 ? (seg.原価合計 / seg.クーポン値引後売上 * 100).toFixed(1) : '0.0';
      }
    }

    const mfRow = {
      '商品売上(10%)': Math.round(totalSales),
      '商品売上(8%)': 0,
      '商品売上(その他)': 0,
      '合計': Math.round(totalSales),
    };

    const excluded = { '4': emptyRow() };

    insertStmt.run(
      ym, 0, 0, 0,
      JSON.stringify(byTax),
      JSON.stringify(data.bySegment),
      JSON.stringify(excluded),
      JSON.stringify(mfRow),
      data.pfFee, data.shippingFee, data.couponTotal,
      now + ' [historical]'
    );
    imported++;
  }
});

insertMany();

console.log('[メルカリShops] ヒストリカルデータインポート完了');
console.log(`  インポート: ${imported}ヶ月`);
console.log(`  スキップ（データなし）: ${skipped}ヶ月`);
console.log(`  DB: ${DB_FILE}`);

db.close();
