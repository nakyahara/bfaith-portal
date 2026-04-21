/**
 * PR1 スモークテスト — 商品収益性ダッシュボード スキーマ追加の検証
 *
 * 検証内容:
 * Test 1: warehouse.db 旧スキーマ+既存データ → initDB でマイグレ成功・既存データ保持
 * Test 2: warehouse.db マイグレ後 initDB 2回目呼び出しで手動設定値が冪等に保持される
 * Test 3: warehouse-mirror.db 旧スキーマ+既存データ → initMirrorDB でマイグレ成功
 * Test 4: warehouse-mirror.db 2回目 initMirrorDB 呼び出しがエラーにならない（冪等性）
 * Test 5: rebuild-m-products.js の applyStagingToProduction が、物理列順が異なる
 *         staging → 本番テーブルへ正しく値を転記する（明示列INSERTの回帰防止）
 *
 * 注: DATA_DIR はモジュール読込時にキャプチャされるため、1プロセス内で
 *     同じDBファイルに対して "legacy DB 事前作成 → init 呼び出し → マイグレ検証" の順で行う。
 */
import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';
import os from 'os';

function expectColumns(db, table, expected) {
  const cols = db.prepare(`PRAGMA table_info(${table})`).all().map(c => c.name);
  const missing = expected.filter(c => !cols.includes(c));
  if (missing.length) {
    throw new Error(`[FAIL] ${table}: カラム欠落 ${missing.join(',')}\n   現状: ${cols.join(',')}`);
  }
  console.log(`[OK] ${table} — 必要カラム全て存在 (${expected.join(', ')})`);
}

function expectTable(db, table) {
  const row = db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`).get(table);
  if (!row) throw new Error(`[FAIL] テーブル未作成: ${table}`);
  console.log(`[OK] テーブル存在: ${table}`);
}

function expectEq(actual, expected, msg) {
  if (actual !== expected) throw new Error(`[FAIL] ${msg}: 期待=${JSON.stringify(expected)} 実際=${JSON.stringify(actual)}`);
  console.log(`[OK] ${msg} = ${JSON.stringify(actual)}`);
}

// ───────────────────────────────────────────────
// Phase A: warehouse.db
//   1. 旧スキーマDB + 既存データを事前作成
//   2. initDB() でマイグレ実行
//   3. データ保持確認 + 新カラム確認
// ───────────────────────────────────────────────
const TMP_WH = fs.mkdtempSync(path.join(os.tmpdir(), 'profit-wh-'));
console.log(`[Test] warehouse DATA_DIR: ${TMP_WH}`);

// A-0: 旧スキーマ warehouse.db を手で作る（新4カラム無し）
{
  const legacyDb = new Database(path.join(TMP_WH, 'warehouse.db'));
  legacyDb.pragma('journal_mode = WAL');
  legacyDb.exec(`CREATE TABLE m_products (
    product_id INTEGER PRIMARY KEY AUTOINCREMENT,
    商品コード TEXT UNIQUE NOT NULL,
    商品名 TEXT, 商品区分 TEXT NOT NULL, 取扱区分 TEXT,
    標準売価 REAL, 原価 REAL, 原価ソース TEXT, 原価状態 TEXT NOT NULL,
    送料 REAL, 送料コード TEXT, 配送方法 TEXT,
    消費税率 REAL, 税区分 TEXT,
    在庫数 INTEGER, 引当数 INTEGER, 仕入先コード TEXT,
    セット構成品数 INTEGER, 売上分類 INTEGER,
    updated_at TEXT NOT NULL
  )`);
  legacyDb.exec(`CREATE TABLE m_products_staging (
    product_id INTEGER PRIMARY KEY AUTOINCREMENT,
    商品コード TEXT UNIQUE NOT NULL,
    商品名 TEXT, 商品区分 TEXT NOT NULL, 取扱区分 TEXT,
    標準売価 REAL, 原価 REAL, 原価ソース TEXT, 原価状態 TEXT NOT NULL,
    送料 REAL, 送料コード TEXT, 配送方法 TEXT,
    消費税率 REAL, 税区分 TEXT,
    在庫数 INTEGER, 引当数 INTEGER, 仕入先コード TEXT,
    セット構成品数 INTEGER, 売上分類 INTEGER,
    updated_at TEXT NOT NULL
  )`);
  legacyDb.prepare(`INSERT INTO m_products (商品コード, 商品名, 商品区分, 原価状態, 標準売価, 原価, 売上分類, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
    .run('LEGACY001', '旧商品A', '単品', 'COMPLETE', 1000, 300, 2, '2026-04-20 10:00:00');
  legacyDb.prepare(`INSERT INTO m_products (商品コード, 商品名, 商品区分, 原価状態, 標準売価, 原価, 売上分類, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
    .run('LEGACY002', '旧商品B', '単品', 'COMPLETE', 2000, 600, 1, '2026-04-20 10:00:00');
  legacyDb.close();
}
console.log('[OK] 旧スキーマ warehouse.db を事前作成（LEGACY001, LEGACY002 投入済み）');

// A-1: DATA_DIR を設定して初回 import
process.env.DATA_DIR = TMP_WH;
const { initDB: initWarehouse, getDB } = await import('../apps/warehouse/db.js');
await initWarehouse();
const db = getDB();

// A-2: 旧データが保持されているか
console.log('\n=== Test 1: warehouse.db 旧スキーマ→新スキーマ マイグレーション ===');
const legacy1 = db.prepare('SELECT 商品コード, 商品名, 標準売価, 原価, 売上分類 FROM m_products WHERE 商品コード=?').get('LEGACY001');
expectEq(legacy1?.商品名, '旧商品A', '旧データ保持（LEGACY001 商品名）');
expectEq(legacy1?.標準売価, 1000, '旧データ保持（LEGACY001 標準売価）');
expectEq(legacy1?.売上分類, 2, '旧データ保持（LEGACY001 売上分類）');

// A-3: 新カラムが追加されている
expectColumns(db, 'm_products', ['seasonality_flag', 'season_months', 'new_product_flag', 'new_product_launch_date']);
expectColumns(db, 'm_products_staging', ['seasonality_flag', 'season_months', 'new_product_flag', 'new_product_launch_date']);

const legacyWithNew = db.prepare('SELECT seasonality_flag, season_months, new_product_flag, new_product_launch_date FROM m_products WHERE 商品コード=?').get('LEGACY001');
expectEq(legacyWithNew?.seasonality_flag, 0, 'マイグレ直後 seasonality_flag デフォルト');
expectEq(legacyWithNew?.season_months, null, 'マイグレ直後 season_months デフォルト');

// A-4: stock_monthly_snapshot テーブル存在
expectTable(db, 'stock_monthly_snapshot');
expectColumns(db, 'stock_monthly_snapshot', ['年月', '商品コード', '月末在庫数', '月末引当数', 'snapshot_source', 'captured_at', 'updated_at']);

// A-5: 手動で季節性フラグを更新（UIで操作される想定）
db.prepare(`UPDATE m_products SET seasonality_flag=?, season_months=? WHERE 商品コード=?`)
  .run(1, '6,7,8', 'LEGACY001');
db.prepare(`UPDATE m_products SET new_product_flag=?, new_product_launch_date=? WHERE 商品コード=?`)
  .run(1, '2026-01-15', 'LEGACY002');
console.log('[OK] 手動で LEGACY001=季節性, LEGACY002=新商品 を設定');

// A-6: 冪等性（2回目 initDB）
console.log('\n=== Test 2: warehouse.db 冪等性 ===');
db.close();
await initWarehouse();
const db2 = getDB();
const afterReInit = db2.prepare('SELECT seasonality_flag, season_months FROM m_products WHERE 商品コード=?').get('LEGACY001');
expectEq(afterReInit?.seasonality_flag, 1, '2回目initDB後も手動設定値保持');
expectEq(afterReInit?.season_months, '6,7,8', '2回目initDB後も season_months 保持');
db2.close();

// ───────────────────────────────────────────────
// Phase B: warehouse-mirror.db
// ───────────────────────────────────────────────
const TMP_MIR = fs.mkdtempSync(path.join(os.tmpdir(), 'profit-mir-'));
console.log(`\n[Test] mirror DATA_DIR: ${TMP_MIR}`);

// B-0: 旧スキーマ mirror_products を事前作成（売上分類 と 代表商品コード はあり、新4カラム無し）
{
  const legacyMir = new Database(path.join(TMP_MIR, 'warehouse-mirror.db'));
  legacyMir.pragma('journal_mode = WAL');
  legacyMir.exec(`CREATE TABLE mirror_products (
    product_id INTEGER PRIMARY KEY,
    商品コード TEXT UNIQUE NOT NULL,
    商品名 TEXT, 商品区分 TEXT NOT NULL, 取扱区分 TEXT,
    標準売価 REAL, 原価 REAL, 原価ソース TEXT, 原価状態 TEXT NOT NULL,
    送料 REAL, 送料コード TEXT, 配送方法 TEXT,
    消費税率 REAL, 税区分 TEXT,
    在庫数 INTEGER, 引当数 INTEGER, 仕入先コード TEXT, セット構成品数 INTEGER,
    売上分類 INTEGER, 代表商品コード TEXT,
    updated_at TEXT NOT NULL
  )`);
  legacyMir.prepare(`INSERT INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 原価状態, 売上分類, updated_at)
                     VALUES (?, ?, ?, ?, ?, ?, ?)`)
    .run(1, 'MIRLEG001', 'ミラー旧商品', '単品', 'COMPLETE', 2, '2026-04-20 10:00:00');
  legacyMir.close();
}
console.log('[OK] 旧スキーマ warehouse-mirror.db を事前作成（MIRLEG001 投入済み）');

process.env.DATA_DIR = TMP_MIR;
const { initMirrorDB, getMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const mdb = getMirrorDB();

console.log('\n=== Test 3: warehouse-mirror.db 旧スキーマ→新スキーマ マイグレーション ===');
const mirLegacy = mdb.prepare('SELECT 商品コード, 商品名, 売上分類 FROM mirror_products WHERE 商品コード=?').get('MIRLEG001');
expectEq(mirLegacy?.商品名, 'ミラー旧商品', 'mirror 旧データ保持');
expectEq(mirLegacy?.売上分類, 2, 'mirror 旧データ売上分類保持');

expectColumns(mdb, 'mirror_products', ['seasonality_flag', 'season_months', 'new_product_flag', 'new_product_launch_date']);
expectTable(mdb, 'mirror_stock_monthly_snapshot');
expectTable(mdb, 'product_retirement_status');
expectTable(mdb, 'dashboard_settings');
expectColumns(mdb, 'product_retirement_status', [
  'ne_product_code', 'status', 'decided_by', 'decided_at', 'reason',
  'next_review_date', 'plan_details_json', 'decision_metrics_json',
  'thresholds_json', 'disposal_rate', 'updated_at'
]);

// 挿入テスト（product_retirement_status）
mdb.prepare(`INSERT INTO product_retirement_status
  (ne_product_code, status, decided_by, decided_at, reason, next_review_date,
   plan_details_json, decision_metrics_json, thresholds_json, disposal_rate, updated_at)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`).run(
  'TEST001', '撤退検討', 'admin', '2026-04-21', '180日販売ゼロ', '2026-05-21',
  '{}', '{"gmroi": 0.3, "turnover_days": 200}', '{"warn_days": 180}', 0.5, new Date().toISOString()
);
const prs = mdb.prepare(`SELECT * FROM product_retirement_status WHERE ne_product_code='TEST001'`).get();
expectEq(prs?.status, '撤退検討', 'product_retirement_status 挿入・取得');

// 冪等性
console.log('\n=== Test 4: warehouse-mirror.db 冪等性 ===');
mdb.close();
initMirrorDB();
console.log('[OK] mirror: 2回目呼び出しもエラーなし');
getMirrorDB().close();

// ───────────────────────────────────────────────
// Test 5: applyStagingToProduction 回帰防止（Codex PR1 Round 3 High 反映確認）
//   rebuild-m-products.js の Phase C を抽出した applyStagingToProduction を直接呼び、
//   m_products と m_products_staging の物理列順が異なっていても値が正しく転記されることを検証。
//   将来 applyStagingToProduction が `SELECT *` に戻されると、このテストが失敗する。
// ───────────────────────────────────────────────
console.log('\n=== Test 5: applyStagingToProduction 列順破壊耐性（実装直結の回帰防止） ===');
{
  const { applyStagingToProduction, MP_COLS, MSC_COLS } = await import('../apps/warehouse/rebuild-m-products.js');

  const tdb = new Database(':memory:');

  // m_products: "現場の旧スキーマ + ALTER済" を模倣
  //   seasonality_flag 等が updated_at の後ろに追加された順序
  tdb.exec(`CREATE TABLE m_products (
    product_id INTEGER PRIMARY KEY AUTOINCREMENT,
    商品コード TEXT UNIQUE NOT NULL, 商品名 TEXT, 商品区分 TEXT NOT NULL, 取扱区分 TEXT,
    標準売価 REAL, 原価 REAL, 原価ソース TEXT, 原価状態 TEXT NOT NULL,
    送料 REAL, 送料コード TEXT, 配送方法 TEXT, 消費税率 REAL, 税区分 TEXT,
    在庫数 INTEGER, 引当数 INTEGER, 仕入先コード TEXT, セット構成品数 INTEGER, 売上分類 INTEGER,
    updated_at TEXT NOT NULL,
    seasonality_flag INTEGER DEFAULT 0,
    season_months TEXT,
    new_product_flag INTEGER DEFAULT 0,
    new_product_launch_date TEXT
  )`);

  // m_products_staging: "新規CREATE" を模倣（seasonality_flag 等が updated_at の前に並ぶ）
  tdb.exec(`CREATE TABLE m_products_staging (
    product_id INTEGER PRIMARY KEY AUTOINCREMENT,
    商品コード TEXT UNIQUE NOT NULL, 商品名 TEXT, 商品区分 TEXT NOT NULL, 取扱区分 TEXT,
    標準売価 REAL, 原価 REAL, 原価ソース TEXT, 原価状態 TEXT NOT NULL,
    送料 REAL, 送料コード TEXT, 配送方法 TEXT, 消費税率 REAL, 税区分 TEXT,
    在庫数 INTEGER, 引当数 INTEGER, 仕入先コード TEXT, セット構成品数 INTEGER, 売上分類 INTEGER,
    seasonality_flag INTEGER DEFAULT 0,
    season_months TEXT,
    new_product_flag INTEGER DEFAULT 0,
    new_product_launch_date TEXT,
    updated_at TEXT NOT NULL
  )`);

  // m_set_components / _staging（今回のテストでは中身は空）
  tdb.exec(`CREATE TABLE m_set_components (
    セット商品コード TEXT NOT NULL, 構成商品コード TEXT NOT NULL,
    数量 INTEGER NOT NULL DEFAULT 1, 構成商品名 TEXT, 構成商品原価 REAL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (セット商品コード, 構成商品コード)
  )`);
  tdb.exec(`CREATE TABLE m_set_components_staging (
    セット商品コード TEXT NOT NULL, 構成商品コード TEXT NOT NULL,
    数量 INTEGER NOT NULL DEFAULT 1, 構成商品名 TEXT, 構成商品原価 REAL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (セット商品コード, 構成商品コード)
  )`);

  // 物理列順が異なることを確認
  const mpColOrder = tdb.prepare('PRAGMA table_info(m_products)').all().map(c => c.name);
  const stgColOrder = tdb.prepare('PRAGMA table_info(m_products_staging)').all().map(c => c.name);
  if (JSON.stringify(mpColOrder) === JSON.stringify(stgColOrder)) {
    throw new Error('[FAIL] テスト前提: m_products と m_products_staging の物理列順が同一。テスト意義なし');
  }
  const mpUpdatedAtIdx = mpColOrder.indexOf('updated_at');
  const stgUpdatedAtIdx = stgColOrder.indexOf('updated_at');
  console.log(`[OK] 物理列順が異なる（m_products: updated_at=${mpUpdatedAtIdx}, staging: updated_at=${stgUpdatedAtIdx}）`);

  // staging に既知の値を入れる（名前付きINSERTなので物理列順に依存しない）
  tdb.prepare(`INSERT INTO m_products_staging
    (商品コード, 商品名, 商品区分, 原価状態, 標準売価, 原価, 売上分類,
     seasonality_flag, season_months, new_product_flag, new_product_launch_date, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
    .run('TEST001', '季節商品X', '単品', 'COMPLETE', 1500, 450, 1,
         1, '6,7,8', 0, null, '2026-04-21 10:00:00');

  // 本番反映関数を直接呼ぶ
  applyStagingToProduction(tdb);

  // 検証: m_products に値が「正しい列」に入っているか
  //   SELECT * にすると、staging の物理列順 (seasonality_flag=8番目以降, updated_at=末尾) で読まれ、
  //   m_products の物理列順 (updated_at=20番目, seasonality_flag=21番目以降) に挿入されて、
  //   updated_at に 1 (seasonality_flag値) が入るなどのデータ破壊が起きる。
  const row = tdb.prepare(`SELECT 商品コード, 商品名, 標準売価, 原価, 売上分類,
    seasonality_flag, season_months, new_product_flag, updated_at
    FROM m_products WHERE 商品コード = ?`).get('TEST001');

  expectEq(row?.商品コード, 'TEST001', 'm_products 商品コード');
  expectEq(row?.商品名, '季節商品X', 'm_products 商品名');
  expectEq(row?.標準売価, 1500, 'm_products 標準売価');
  expectEq(row?.原価, 450, 'm_products 原価');
  expectEq(row?.売上分類, 1, 'm_products 売上分類');
  expectEq(row?.seasonality_flag, 1, 'm_products seasonality_flag（列順破壊なし）');
  expectEq(row?.season_months, '6,7,8', 'm_products season_months（列順破壊なし）');
  expectEq(row?.new_product_flag, 0, 'm_products new_product_flag（列順破壊なし）');
  expectEq(row?.updated_at, '2026-04-21 10:00:00', 'm_products updated_at（列順破壊なし）');

  tdb.close();
  console.log('[OK] applyStagingToProduction は物理列順破壊に強い（明示列INSERT動作）');
  console.log('     将来 SELECT * に戻されるとこのテストが失敗するため、回帰防止として機能する');
}

// ───────────────────────────────────────────────
// クリーンアップ（Windows WAL handle 都合で失敗することがあるので best-effort）
// ───────────────────────────────────────────────
for (const dir of [TMP_WH, TMP_MIR]) {
  try { fs.rmSync(dir, { recursive: true, force: true }); }
  catch { /* 次回起動時の tmp クリーナーに任せる */ }
}

console.log('\n========================================');
console.log('✅ 全テストPASS');
console.log('========================================');
