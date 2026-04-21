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
 * Test 6: stock-snapshot.js captureMonthlyStockSnapshot が raw_lz_inventory を
 *         商品コード単位で集計して stock_monthly_snapshot へ UPSERT する（PR2a）
 * Test 7: stock_monthly_snapshot → mirror_stock_monthly_snapshot の sync round-trip（PR2a）
 * Test 8: 空Payload + clear_stock_snapshot で mirror の stale データが消える（PR2a fix Medium #1）
 * Test 9: SELECT 失敗時は clear を送らず mirror の前回状態を保持（PR2a fix Round 2 Medium）
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
// Test 6: stock-snapshot.js captureMonthlyStockSnapshot の動作確認（PR2a）
//   raw_lz_inventory を事前投入 → captureMonthlyStockSnapshot(YYYY-MM) 実行
//   → stock_monthly_snapshot に 商品コード単位で集計された結果が入ることを検証
//   複数ロケ（同一商品IDで複数行）の SUM も正しく行われるか確認
// ───────────────────────────────────────────────
console.log('\n=== Test 6: captureMonthlyStockSnapshot (raw_lz_inventory 集計) ===');
process.env.DATA_DIR = TMP_WH;
await initWarehouse();
const db6 = getDB();

// raw_lz_inventory に複数ロケ・複数商品を投入
db6.prepare('DELETE FROM raw_lz_inventory').run();
const insertLz = db6.prepare(`INSERT INTO raw_lz_inventory
  (商品ID, 商品名, ロケ, 在庫数, 引当数, synced_at)
  VALUES (?, ?, ?, ?, ?, ?)`);
// SKU_A: ロケ1に100個 + ロケ2に50個 = 150個、引当 10+5=15
insertLz.run('SKU_A', '商品A', 'LOC1', 100, 10, '2026-04-20 23:00:00');
insertLz.run('SKU_A', '商品A', 'LOC2', 50, 5, '2026-04-20 23:00:00');
// SKU_B: ロケ1に30個
insertLz.run('SKU_B', '商品B', 'LOC1', 30, 0, '2026-04-20 23:00:00');
// 商品IDなし（集計対象外）
insertLz.run('', '空ID', 'LOC1', 99, 0, '2026-04-20 23:00:00');
console.log('[OK] raw_lz_inventory にテストデータ4行投入');

// captureMonthlyStockSnapshot 実行
const { captureMonthlyStockSnapshot } = await import('../apps/warehouse/stock-snapshot.js');
const result = captureMonthlyStockSnapshot('2026-04', 'test');
expectEq(result.ok, true, 'captureMonthlyStockSnapshot 成功');
expectEq(result.count, 2, '集計商品数（空IDは除外）');

// stock_monthly_snapshot の中身検証
const snapA = db6.prepare('SELECT * FROM stock_monthly_snapshot WHERE 年月=? AND 商品コード=?').get('2026-04', 'SKU_A');
expectEq(snapA?.月末在庫数, 150, 'SKU_A 月末在庫数 = 100+50');
expectEq(snapA?.月末引当数, 15, 'SKU_A 月末引当数 = 10+5');
expectEq(snapA?.snapshot_source, 'test', 'SKU_A snapshot_source');

const snapB = db6.prepare('SELECT * FROM stock_monthly_snapshot WHERE 年月=? AND 商品コード=?').get('2026-04', 'SKU_B');
expectEq(snapB?.月末在庫数, 30, 'SKU_B 月末在庫数');

// UPSERT動作確認: 同じ yearMonth + 商品コードに対して再実行して更新されるか
// raw_lz_inventory を変えずに再実行 → 在庫数は同じ、updated_at だけ更新される想定
const result2 = captureMonthlyStockSnapshot('2026-04', 'test-retry');
expectEq(result2.count, 2, '2回目実行も同じ件数');
const snapAagain = db6.prepare('SELECT snapshot_source FROM stock_monthly_snapshot WHERE 年月=? AND 商品コード=?').get('2026-04', 'SKU_A');
expectEq(snapAagain?.snapshot_source, 'test-retry', 'UPSERT で source 更新');

// 不正年月形式の拒否
let threwError = false;
try { captureMonthlyStockSnapshot('2026/04'); }
catch { threwError = true; }
expectEq(threwError, true, '不正年月形式で throw');

// 月範囲外（Codex PR2a review Low #3）
let threwRange = false;
try { captureMonthlyStockSnapshot('2026-13'); }
catch { threwRange = true; }
expectEq(threwRange, true, '月=13 で throw');

let threwZero = false;
try { captureMonthlyStockSnapshot('2026-00'); }
catch { threwZero = true; }
expectEq(threwZero, true, '月=00 で throw');

db6.close();
console.log('[OK] captureMonthlyStockSnapshot 動作確認完了');

// ───────────────────────────────────────────────
// Test 7: sync round-trip 相当（stock_monthly_snapshot → mirror_stock_monthly_snapshot）
//   /api/sync の受信ロジック相当を直接 DB で実行、データ転送を検証。
//   （HTTPレイヤーは起動せず、SQL レベルで同等の INSERT を確認）
// ───────────────────────────────────────────────
console.log('\n=== Test 7: stock_monthly_snapshot sync round-trip ===');
process.env.DATA_DIR = TMP_MIR;
initMirrorDB();
const mdb7 = getMirrorDB();

// sync payload 相当を取得
process.env.DATA_DIR = TMP_WH;
await initWarehouse();
const db7 = getDB();
const payload = db7.prepare(`
  SELECT 年月, 商品コード, 月末在庫数, 月末引当数, snapshot_source, captured_at, updated_at
  FROM stock_monthly_snapshot
  WHERE 年月 >= '2024-05'
`).all();
expectEq(payload.length, 2, 'sync 送信 payload 件数');

// 受信側ロジック相当（DELETE + INSERT）
const now7 = new Date().toISOString().replace('T', ' ').slice(0, 19);
const tx = mdb7.transaction(() => {
  mdb7.exec('DELETE FROM mirror_stock_monthly_snapshot');
  const stmt = mdb7.prepare(`INSERT INTO mirror_stock_monthly_snapshot (
    年月, 商品コード, 月末在庫数, 月末引当数, snapshot_source, captured_at, updated_at
  ) VALUES (?,?,?,?,?,?,?)`);
  for (const s of payload) {
    stmt.run(s.年月, s.商品コード, s.月末在庫数, s.月末引当数, s.snapshot_source, s.captured_at, now7);
  }
});
tx();

// mirror 側に入っているか
const mirSnap = mdb7.prepare('SELECT COUNT(*) as cnt FROM mirror_stock_monthly_snapshot').get();
expectEq(mirSnap.cnt, 2, 'mirror 側に 2 件投入');

const mirA = mdb7.prepare('SELECT * FROM mirror_stock_monthly_snapshot WHERE 商品コード=?').get('SKU_A');
expectEq(mirA?.月末在庫数, 150, 'mirror 側 SKU_A 月末在庫数');
expectEq(mirA?.snapshot_source, 'test-retry', 'mirror 側 snapshot_source');

db7.close();
mdb7.close();
console.log('[OK] stock_monthly_snapshot sync round-trip 成功');

// ───────────────────────────────────────────────
// Test 8: 空Payload + clear で mirror が stale なく空になる（Codex PR2a Round 1 Medium #1）
//   ミニPC側で対象月の在庫が0件になったケースで、mirror に古いデータが残らないことを検証。
//   送信側: stock_monthly_snapshot.length === 0 でも clear_stock_snapshot=true で送る
//   受信側: meta.clear_stock_snapshot を配列長に関係なく処理する
// ───────────────────────────────────────────────
console.log('\n=== Test 8: 空Payload + clear で mirror の stale データが消える ===');
process.env.DATA_DIR = TMP_MIR;
initMirrorDB();
const mdb8 = getMirrorDB();

// Test 7 の残骸を消してから再セットアップ
mdb8.exec('DELETE FROM mirror_stock_monthly_snapshot');

// 事前状態: mirror に古いデータを2件入れておく
mdb8.prepare(`INSERT INTO mirror_stock_monthly_snapshot
  (年月, 商品コード, 月末在庫数, 月末引当数, snapshot_source, captured_at, updated_at)
  VALUES (?, ?, ?, ?, ?, ?, ?)`)
  .run('2026-02', 'OLD_SKU', 99, 0, 'logizard', '2026-02-28', '2026-02-28 23:00:00');
mdb8.prepare(`INSERT INTO mirror_stock_monthly_snapshot
  (年月, 商品コード, 月末在庫数, 月末引当数, snapshot_source, captured_at, updated_at)
  VALUES (?, ?, ?, ?, ?, ?, ?)`)
  .run('2026-03', 'OLD_SKU', 88, 0, 'logizard', '2026-03-31', '2026-03-31 23:00:00');

const beforeCount = mdb8.prepare('SELECT COUNT(*) as cnt FROM mirror_stock_monthly_snapshot').get().cnt;
expectEq(beforeCount, 2, 'Test 8 事前: mirror に 2件の stale データ');

// /api/sync の受信側ロジック相当を直接実行（空 payload + clear_stock_snapshot=true）
//   router.js の該当部分を simulate: req.body.stock_monthly_snapshot !== undefined で入る
{
  const reqBody = {
    stock_monthly_snapshot: [],   // 空配列
    meta: { clear_stock_snapshot: true },
  };
  const meta8 = reqBody.meta;
  const snapshotData = reqBody.stock_monthly_snapshot;
  const nowStr = new Date().toISOString().replace('T', ' ').slice(0, 19);

  if (snapshotData !== undefined) {
    const tx = mdb8.transaction(() => {
      if (meta8?.clear_stock_snapshot) mdb8.exec('DELETE FROM mirror_stock_monthly_snapshot');
      if (snapshotData.length > 0) {
        // ここは通らない想定
        throw new Error('空payload想定なのに INSERT が走ろうとした');
      }
    });
    tx();
  }
}

const afterCount = mdb8.prepare('SELECT COUNT(*) as cnt FROM mirror_stock_monthly_snapshot').get().cnt;
expectEq(afterCount, 0, '空Payload+clearで mirror は 0件（stale が消えた）');

mdb8.close();
console.log('[OK] 空Payload + clear で stale が消えることを確認（Medium #1 回帰防止）');

// ───────────────────────────────────────────────
// Test 9: buildStockSnapshotSyncParts 実装直結の回帰テスト（Codex PR2a Round 2-3 反映）
//   sync-to-render.js から抽出した buildStockSnapshotSyncParts() を直接呼ぶ形式。
//   PR1 の applyStagingToProduction と同じパターン。
//   将来 sync-to-render.js 側だけが修正を失っても、このテストが失敗する。
//
//   3ケース検証:
//     (a) SELECT 失敗（テーブル未作成）→ fetched=false, parts=[]
//     (b) SELECT 成功して 0件 → fetched=true, parts=[clear-only]
//     (c) SELECT 成功して N件 → fetched=true, parts=[初回clear + chunk]
// ───────────────────────────────────────────────
console.log('\n=== Test 9: buildStockSnapshotSyncParts 実装直結の回帰テスト ===');
{
  const { buildStockSnapshotSyncParts } = await import('../apps/warehouse/sync-to-render.js');

  // --- Case (a): SELECT 失敗 ---
  {
    const fakeDb = new Database(':memory:');
    // stock_monthly_snapshot テーブルが存在しない状態で呼ぶ
    const result = buildStockSnapshotSyncParts(fakeDb, '2024-04');
    expectEq(result.fetched, false, '(a) SELECT 失敗で fetched=false');
    expectEq(result.parts.length, 0, '(a) parts は空配列（送信なし）');
    if (!result.error) throw new Error('[FAIL] (a) error メッセージが入っていない');
    fakeDb.close();
    console.log('[OK] (a) SELECT 失敗パス: clear も chunk も送られない');
  }

  // --- Case (b): SELECT 成功、0件 ---
  {
    const fakeDb = new Database(':memory:');
    fakeDb.exec(`CREATE TABLE stock_monthly_snapshot (
      年月 TEXT, 商品コード TEXT, 月末在庫数 INTEGER, 月末引当数 INTEGER,
      snapshot_source TEXT, captured_at TEXT, updated_at TEXT,
      PRIMARY KEY (年月, 商品コード)
    )`);
    const result = buildStockSnapshotSyncParts(fakeDb, '2024-04');
    expectEq(result.fetched, true, '(b) SELECT 成功で fetched=true');
    expectEq(result.count, 0, '(b) 0件');
    expectEq(result.parts.length, 1, '(b) parts は clear-only 1件');
    expectEq(result.parts[0].payload.stock_monthly_snapshot.length, 0, '(b) payload は空配列');
    expectEq(result.parts[0].payload.meta?.clear_stock_snapshot, true, '(b) meta.clear_stock_snapshot=true');
    fakeDb.close();
    console.log('[OK] (b) 0件パス: clear-only part が生成される（stale 消去が機能する）');
  }

  // --- Case (c): SELECT 成功、複数件（チャンク分割境界確認） ---
  {
    const fakeDb = new Database(':memory:');
    fakeDb.exec(`CREATE TABLE stock_monthly_snapshot (
      年月 TEXT, 商品コード TEXT, 月末在庫数 INTEGER, 月末引当数 INTEGER,
      snapshot_source TEXT, captured_at TEXT, updated_at TEXT,
      PRIMARY KEY (年月, 商品コード)
    )`);
    const insertS = fakeDb.prepare(`INSERT INTO stock_monthly_snapshot VALUES (?, ?, ?, ?, ?, ?, ?)`);
    for (let i = 0; i < 30; i++) {
      insertS.run('2026-04', `SKU_${i}`, 10, 0, 'test', '2026-04-01', '2026-04-01 10:00:00');
    }
    // cutoff より古い行を 1件混入。WHERE 年月 >= ? が正しく効けば result.count=30。
    // もし将来 WHERE が消えたらこの行が含まれて count=31 になり、テスト失敗で検知できる。
    // （Codex PR2a Round 4 非ブロッカー #1 反映）
    insertS.run('2023-01', 'OLD_SKU', 5, 0, 'test', '2023-01-01', '2023-01-01 10:00:00');

    // chunkSize=10 を指定して、30件が3チャンクに分かれるか確認
    const result = buildStockSnapshotSyncParts(fakeDb, '2024-04', 10);
    expectEq(result.fetched, true, '(c) SELECT 成功');
    expectEq(result.count, 30, '(c) 30件（cutoff より古い OLD_SKU は除外される / WHERE 回帰検知）');
    expectEq(result.parts.length, 3, '(c) 30件は chunkSize=10 で 3 parts');
    expectEq(result.parts[0].payload.meta?.clear_stock_snapshot, true, '(c) 初回 chunk は clear=true');
    expectEq(result.parts[1].payload.meta, undefined, '(c) 2番目 chunk は meta なし');
    expectEq(result.parts[2].payload.meta, undefined, '(c) 3番目 chunk は meta なし');
    fakeDb.close();
    console.log('[OK] (c) 複数件パス: 初回 chunk のみ clear、残りは追記用');
  }

  // --- Case (d): chunkSize ガード（Codex PR2a Round 4 非ブロッカー #2） ---
  {
    const fakeDb = new Database(':memory:');
    fakeDb.exec(`CREATE TABLE stock_monthly_snapshot (
      年月 TEXT, 商品コード TEXT, 月末在庫数 INTEGER, 月末引当数 INTEGER,
      snapshot_source TEXT, captured_at TEXT, updated_at TEXT,
      PRIMARY KEY (年月, 商品コード)
    )`);
    let threw0 = false, threwNeg = false, threwFloat = false;
    try { buildStockSnapshotSyncParts(fakeDb, '2024-04', 0); } catch { threw0 = true; }
    try { buildStockSnapshotSyncParts(fakeDb, '2024-04', -5); } catch { threwNeg = true; }
    try { buildStockSnapshotSyncParts(fakeDb, '2024-04', 1.5); } catch { threwFloat = true; }
    expectEq(threw0, true, '(d) chunkSize=0 で throw');
    expectEq(threwNeg, true, '(d) chunkSize<0 で throw');
    expectEq(threwFloat, true, '(d) chunkSize が整数でない場合 throw');
    fakeDb.close();
    console.log('[OK] (d) chunkSize ガード: 不正値で無限ループする前に throw');
  }

  // --- 回帰シナリオの総合: SELECT 失敗で mirror の stale が残る ---
  process.env.DATA_DIR = TMP_MIR;
  initMirrorDB();
  const mdb9 = getMirrorDB();
  mdb9.exec('DELETE FROM mirror_stock_monthly_snapshot');
  mdb9.prepare(`INSERT INTO mirror_stock_monthly_snapshot
    (年月, 商品コード, 月末在庫数, 月末引当数, snapshot_source, captured_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)`)
    .run('2026-03', 'PREV_SKU', 50, 0, 'logizard', '2026-03-31', '2026-03-31 23:00:00');

  // 本物の buildStockSnapshotSyncParts が fetched=false を返すと、sync-to-render の実ロジックで
  // 送信 for-loop が parts=[] を反復せず、mirror は未変更
  const fakeDb = new Database(':memory:');
  const plan = buildStockSnapshotSyncParts(fakeDb, '2024-04');
  // 実コードと同じ送信ループを模倣
  for (const part of plan.parts) {
    // ここに入ると stale が消える。SELECT 失敗なら parts=[] で入らないはず
    mdb9.exec('DELETE FROM mirror_stock_monthly_snapshot'); // clear simulation
  }
  const after = mdb9.prepare('SELECT COUNT(*) as cnt FROM mirror_stock_monthly_snapshot').get().cnt;
  expectEq(after, 1, 'SELECT 失敗時 mirror の前回データ（1件）が保持される');

  fakeDb.close();
  mdb9.close();
  console.log('[OK] 総合シナリオ: SELECT 失敗で mirror の stale data は消えない');
}

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
