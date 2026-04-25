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
 * Test 10: retirement-thresholds モジュール (PR2b)
 * Test 11: inventory-decision feature flag middleware (PR2b)
 * Test 12: validateStatusBody バリデーション回帰 (PR2b fix)
 * Test 13: product_retirement_status UPSERT + snapshot 保存 (PR2b fix)
 * Test 14: classifyProduct 分類網羅 (PR2c)
 * Test 15: applyEarlyWarning ゼロ割・skip・drop (PR2c)
 * Test 16: fetchCandidatesRaw SQL 統合（売上分類フィルタ + 集計） (PR2c)
 * Test 17: EJS レンダリングで「商品収益性ダッシュボード」改称確認 (PR3)
 * Test 18: feature flag OFF で EJS がタブB DOM を出さない (PR3 Codex High 1 回帰防止)
 * Test 19: feature flag ON で EJS がタブB DOM を出す (PR3)
 * Test 20: XSS エスケープ (PR3)
 * Test 21: 処分率 ratio/percent 単位変換 (PR3 Codex §6-A 回帰防止)
 * Test 22: status POST payload 構造（契約キー名 + decision_metrics 網羅） (PR3 Codex R2 High 1/Medium 4)
 *
 * 注: DATA_DIR はモジュール読込時にキャプチャされるため、1プロセス内で
 *     同じDBファイルに対して "legacy DB 事前作成 → init 呼び出し → マイグレ検証" の順で行う。
 */
import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';
import os from 'os';
import ejs from 'ejs';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

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
// Test 10: retirement-thresholds モジュール（PR2b）
//   seedDefaultsIfMissing の冪等性、読み書きアクセサ、バリデーションを検証
// ───────────────────────────────────────────────
console.log('\n=== Test 10: retirement-thresholds モジュール ===');
{
  const {
    seedDefaultsIfMissing,
    getRetirementThresholds,
    getEarlyWarning,
    getDisposalRateDefault,
    getSetting,
    setSetting,
    validateRetirementThresholds,
    validateEarlyWarning,
    KEYS,
    DEFAULT_RETIREMENT_THRESHOLDS,
    DEFAULT_EARLY_WARNING,
    DEFAULT_DISPOSAL_RATE,
  } = await import('../apps/profit-analysis/retirement-thresholds.js');

  const tdb = new Database(':memory:');
  tdb.exec(`CREATE TABLE dashboard_settings (
    key TEXT PRIMARY KEY, value_json TEXT NOT NULL, updated_at TEXT NOT NULL, updated_by TEXT
  )`);

  // 10a. 初回 seed
  seedDefaultsIfMissing(tdb);
  const afterSeed = tdb.prepare('SELECT COUNT(*) as cnt FROM dashboard_settings').get().cnt;
  expectEq(afterSeed, 4, '10a seed 後は4キー投入 (retirement/classification/early_warning/disposal_rate)');

  // 10b. seed 冪等性（2回目の seed で件数増えない）
  seedDefaultsIfMissing(tdb);
  const afterSeed2 = tdb.prepare('SELECT COUNT(*) as cnt FROM dashboard_settings').get().cnt;
  expectEq(afterSeed2, 4, '10b 2回目 seed で件数増えず');

  // 10c. デフォルト値が読めているか
  const ret = getRetirementThresholds(tdb);
  expectEq(ret['1']?.warn_days_no_sales, 180, '10c 自社 warn 180');
  expectEq(ret['1']?.retire_days_no_sales, 365, '10c 自社 retire 365');
  expectEq(ret['2']?.warn_days_no_sales, 120, '10c 取引先限定 warn 120');
  expectEq(ret['3']?.warn_gmroi_lt, 50, '10c 仕入 warn_gmroi_lt 50');

  const ew = getEarlyWarning(tdb);
  expectEq(ew.past_period_days, 90, '10c early_warning.past_period_days 90');
  expectEq(ew.min_past_sales, 10, '10c early_warning.min_past_sales 10');
  expectEq(ew.drop_ratio, 0.33, '10c early_warning.drop_ratio 0.33');

  expectEq(getDisposalRateDefault(tdb), 0.5, '10c disposal_rate_default 0.5');

  // 10d. setSetting で更新 → get で反映
  const customRet = {
    '1': { warn_days_no_sales: 200, retire_days_no_sales: 400 },
    '2': { warn_days_no_sales: 100, retire_days_no_sales: 150 },
    '3': { warn_days_no_sales: 60, retire_days_no_sales: 120, warn_gmroi_lt: 40,
           retire_gmroi_lt: 20, retire_turnover_gt: 150 },
  };
  setSetting(tdb, KEYS.RETIREMENT, customRet, 'test-user');
  const retUpdated = getRetirementThresholds(tdb);
  expectEq(retUpdated['1']?.warn_days_no_sales, 200, '10d setSetting 後の値 (自社 warn)');
  const row = tdb.prepare('SELECT updated_by FROM dashboard_settings WHERE key = ?').get(KEYS.RETIREMENT);
  expectEq(row?.updated_by, 'test-user', '10d updated_by が保存される');

  // 10e. validateRetirementThresholds
  let throwFlag = false;
  try { validateRetirementThresholds({ '1': { warn_days_no_sales: 180 /* retire なし */ } }); }
  catch { throwFlag = true; }
  expectEq(throwFlag, true, '10e retire_days_no_sales 欠落で throw');

  throwFlag = false;
  try {
    validateRetirementThresholds({
      '1': { warn_days_no_sales: 500, retire_days_no_sales: 365 },  // warn > retire
      '2': { warn_days_no_sales: 120, retire_days_no_sales: 180 },
      '3': { warn_days_no_sales: 90, retire_days_no_sales: 180 },
    });
  } catch { throwFlag = true; }
  expectEq(throwFlag, true, '10e warn > retire で throw');

  throwFlag = false;
  try {
    validateRetirementThresholds({
      '1': { warn_days_no_sales: 180, retire_days_no_sales: 365 },
      // '2' 欠落
      '3': { warn_days_no_sales: 90, retire_days_no_sales: 180 },
    });
  } catch { throwFlag = true; }
  expectEq(throwFlag, true, '10e sales_class 2 欠落で throw');

  // 正常系
  let normalOk = true;
  try { validateRetirementThresholds(customRet); }
  catch { normalOk = false; }
  expectEq(normalOk, true, '10e 正常な retirement thresholds は通る');

  // 10f. validateEarlyWarning
  throwFlag = false;
  try { validateEarlyWarning({ past_period_days: 90, recent_period_days: 30, min_past_sales: 10, drop_ratio: 1.5 }); }
  catch { throwFlag = true; }
  expectEq(throwFlag, true, '10f drop_ratio > 1 で throw');

  throwFlag = false;
  try { validateEarlyWarning({ past_period_days: 30, recent_period_days: 90, min_past_sales: 10, drop_ratio: 0.33 }); }
  catch { throwFlag = true; }
  expectEq(throwFlag, true, '10f recent >= past で throw');

  normalOk = true;
  try { validateEarlyWarning(DEFAULT_EARLY_WARNING); } catch { normalOk = false; }
  expectEq(normalOk, true, '10f デフォルト early_warning は通る');

  // 10g: past_period_days != 90 / recent_period_days != 30 を拒否
  //      Codex PR2c Round 1 Medium #2 反映（applyEarlyWarning の SQL 集計固定制約）
  throwFlag = false;
  try { validateEarlyWarning({ ...DEFAULT_EARLY_WARNING, past_period_days: 60 }); }
  catch { throwFlag = true; }
  expectEq(throwFlag, true, '10g past_period_days=60 (≠90) で throw');

  throwFlag = false;
  try { validateEarlyWarning({ ...DEFAULT_EARLY_WARNING, recent_period_days: 14 }); }
  catch { throwFlag = true; }
  expectEq(throwFlag, true, '10g recent_period_days=14 (≠30) で throw');

  tdb.close();
  console.log('[OK] retirement-thresholds モジュール動作確認');
}

// ───────────────────────────────────────────────
// Test 11: inventory-decision feature flag（PR2b）
//   INVENTORY_DECISION_ENABLED が未設定/false の時、ミドルウェアで 503 を返すことを検証
// ───────────────────────────────────────────────
console.log('\n=== Test 11: inventory-decision feature flag ===');
{
  // Express ミドルウェアの挙動確認用にモック req/res を用意
  function mockRes() {
    return {
      _status: 200,
      _body: null,
      status(code) { this._status = code; return this; },
      json(obj) { this._body = obj; return this; },
    };
  }

  // Feature flag OFF（未設定）
  delete process.env.INVENTORY_DECISION_ENABLED;
  const routerOff = (await import('../apps/profit-analysis/inventory-decision.js?t=off' )).default;
  // 第1ミドルウェアが feature flag ガード
  const mw = routerOff.stack[0].handle;
  const reqOff = {};
  const resOff = mockRes();
  let calledNext = false;
  mw(reqOff, resOff, () => { calledNext = true; });
  expectEq(resOff._status, 503, '11 flag OFF で 503');
  expectEq(calledNext, false, '11 flag OFF で next 呼ばれない');

  // Feature flag ON
  process.env.INVENTORY_DECISION_ENABLED = 'true';
  const routerOn = (await import('../apps/profit-analysis/inventory-decision.js?t=on')).default;
  const mwOn = routerOn.stack[0].handle;
  const reqOn = {};
  const resOn = mockRes();
  let calledNext2 = false;
  mwOn(reqOn, resOn, () => { calledNext2 = true; });
  expectEq(calledNext2, true, '11 flag ON で next 呼ばれる');
  expectEq(resOn._status, 200, '11 flag ON で 503 は返されない');

  // 後続テストに影響しないようクリア
  delete process.env.INVENTORY_DECISION_ENABLED;
  console.log('[OK] feature flag middleware 動作確認');
}

// ───────────────────────────────────────────────
// Test 12: validateStatusBody 回帰防止（Codex PR2b Round 1 Medium 反映）
//   POST /status のバリデーション関数を直接呼んでエッジケースを検証
// ───────────────────────────────────────────────
console.log('\n=== Test 12: validateStatusBody (POST /status バリデーション) ===');
{
  const { validateStatusBody, VALID_STATUSES, REVIEW_REQUIRED_STATUSES } =
    await import('../apps/profit-analysis/inventory-decision.js');

  function expectThrow(fn, msg) {
    let threw = false;
    try { fn(); } catch { threw = true; }
    expectEq(threw, true, msg);
  }

  // ne_product_code 欠落
  expectThrow(() => validateStatusBody({ status: '継続' }), 'ne_product_code 欠落で throw');

  // 無効 status
  expectThrow(() => validateStatusBody({ ne_product_code: 'X', status: '不明なステータス' }), '無効 status で throw');

  // 撤退検討 / 撤退確定 で reason 欠落 → throw（Medium #1）
  expectThrow(() => validateStatusBody({ ne_product_code: 'X', status: '撤退検討' }),
    '撤退検討 + reason欠落で throw');
  expectThrow(() => validateStatusBody({ ne_product_code: 'X', status: '撤退確定' }),
    '撤退確定 + reason欠落で throw');

  // 追加3ステータスは next_review_date と reason 両方必須（Medium #1 設計書§14準拠）
  expectThrow(() => validateStatusBody({ ne_product_code: 'X', status: '消化計画中' }),
    '消化計画中 + next_review_date欠落で throw');
  expectThrow(() => validateStatusBody({
    ne_product_code: 'X', status: '消化計画中', next_review_date: '2026-05-01'
    // reason 欠落
  }), '消化計画中 + reason欠落で throw');
  expectThrow(() => validateStatusBody({ ne_product_code: 'X', status: 'リブランディング検討' }),
    'リブランディング検討 + next_review_date欠落で throw');
  expectThrow(() => validateStatusBody({
    ne_product_code: 'X', status: '再生産判断中', next_review_date: '2026-05-01'
    // reason 欠落
  }), '再生産判断中 + reason欠落で throw');

  // disposal_rate 範囲外（Medium #2）
  expectThrow(() => validateStatusBody({
    ne_product_code: 'X', status: '撤退検討', reason: '販売なし', disposal_rate: -1,
  }), 'disposal_rate=-1 で throw');
  expectThrow(() => validateStatusBody({
    ne_product_code: 'X', status: '撤退検討', reason: '販売なし', disposal_rate: 2,
  }), 'disposal_rate=2 で throw');
  expectThrow(() => validateStatusBody({
    ne_product_code: 'X', status: '撤退検討', reason: '販売なし', disposal_rate: 0,
  }), 'disposal_rate=0 で throw');

  // Codex PR3 R2 Medium 1 反映: 消化計画中 は plan_details 必須（server 側でも）
  expectThrow(() => validateStatusBody({
    ne_product_code: 'X', status: '消化計画中',
    next_review_date: '2026-05-01', reason: '計画的消化'
    // plan_details 欠落
  }), '消化計画中 + plan_details 欠落で throw');

  expectThrow(() => validateStatusBody({
    ne_product_code: 'X', status: '消化計画中',
    next_review_date: '2026-05-01', reason: '計画的消化',
    plan_details: { target_month: '2026-07' }
    // monthly_sales_target 欠落
  }), '消化計画中 + monthly_sales_target 欠落で throw');

  expectThrow(() => validateStatusBody({
    ne_product_code: 'X', status: '消化計画中',
    next_review_date: '2026-05-01', reason: '計画的消化',
    plan_details: { monthly_sales_target: 10 }
    // target_month 欠落
  }), '消化計画中 + target_month 欠落で throw');

  // Codex PR3 R2 Low-Medium 2 反映: 実在日 round-trip 検証
  expectThrow(() => validateStatusBody({
    ne_product_code: 'X', status: '撤退検討', reason: '撤退',
    next_review_date: '2026-02-31',  // 2月31日は存在しない
  }), '実在しない日付 2026-02-31 で throw');

  expectThrow(() => validateStatusBody({
    ne_product_code: 'X', status: '撤退検討', reason: '撤退',
    next_review_date: '2026-04-31',  // 4月31日は存在しない
  }), '実在しない日付 2026-04-31 で throw');

  // target_month の月範囲（regex でチェック、01-12 以外を拒否）
  expectThrow(() => validateStatusBody({
    ne_product_code: 'X', status: '撤退検討', reason: '撤退',
    plan_details: { target_month: '2026-99', monthly_sales_target: 100 }
  }), 'target_month=2026-99 で throw');

  // 正常系
  let threw = false;
  try {
    validateStatusBody({ ne_product_code: 'X', status: '継続' });
    validateStatusBody({ ne_product_code: 'X', status: '撤退検討', reason: '365日販売なし' });
    validateStatusBody({
      ne_product_code: 'X', status: '消化計画中',
      next_review_date: '2026-05-01', reason: '計画的消化',
      plan_details: { target_month: '2026-07', monthly_sales_target: 100 }
    });
    validateStatusBody({
      ne_product_code: 'X', status: '撤退確定', reason: '完全停止', disposal_rate: 0.7,
    });
    validateStatusBody({ ne_product_code: 'X', status: '値下げ検討' });
    validateStatusBody({
      ne_product_code: 'X', status: '撤退検討', reason: '撤退',
      next_review_date: '2026-02-28',  // 2月28日は有効
    });
  } catch (e) { threw = true; console.error('正常系 fail:', e.message); }
  expectEq(threw, false, '正常な status body は通る');

  console.log('[OK] validateStatusBody バリデーション回帰テスト（R2 反映済）');
}

// ───────────────────────────────────────────────
// Test 13: POST /status UPSERT の end-to-end（DB直呼び出し）
//   実際に product_retirement_status に値が入ってスナップショット保存されるか検証
// ───────────────────────────────────────────────
console.log('\n=== Test 13: product_retirement_status UPSERT snapshot ===');
{
  process.env.DATA_DIR = TMP_MIR;
  initMirrorDB();
  const mdb13 = getMirrorDB();
  mdb13.exec('DELETE FROM product_retirement_status');

  // UPSERT 相当の SQL を直接実行（inventory-decision.js の POST /status と同じ SQL）
  const ts = '2026-04-21 10:00:00';
  const stmt = mdb13.prepare(`INSERT INTO product_retirement_status
    (ne_product_code, status, decided_by, decided_at, reason, next_review_date,
     plan_details_json, decision_metrics_json, thresholds_json, disposal_rate, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(ne_product_code) DO UPDATE SET
      status = excluded.status, decided_by = excluded.decided_by,
      decided_at = excluded.decided_at, reason = excluded.reason,
      next_review_date = excluded.next_review_date,
      plan_details_json = excluded.plan_details_json,
      decision_metrics_json = excluded.decision_metrics_json,
      thresholds_json = excluded.thresholds_json,
      disposal_rate = excluded.disposal_rate,
      updated_at = excluded.updated_at`);

  // 初回 INSERT
  stmt.run('TESTPROD001', '消化計画中', 'nakyahara', ts, '計画的消化', '2026-07-01',
    JSON.stringify({ target_month: '2026-06', monthly_sales_target: 100 }),
    JSON.stringify({ gmroi: 120, turnover_days: 250 }),
    JSON.stringify({ warn_days: 180 }),
    0.5, ts);

  const row1 = mdb13.prepare('SELECT * FROM product_retirement_status WHERE ne_product_code = ?').get('TESTPROD001');
  expectEq(row1?.status, '消化計画中', '13 INSERT status');
  expectEq(row1?.disposal_rate, 0.5, '13 INSERT disposal_rate');
  expectEq(JSON.parse(row1.plan_details_json).target_month, '2026-06', '13 INSERT plan_details_json');
  expectEq(JSON.parse(row1.decision_metrics_json).gmroi, 120, '13 INSERT decision_metrics_json');
  expectEq(row1?.updated_at, ts, '13 INSERT updated_at');

  // 同じコードで UPDATE (撤退検討へ)
  const ts2 = '2026-05-01 15:00:00';
  stmt.run('TESTPROD001', '撤退検討', 'nakyahara', ts2, '消化計画進捗なし', null,
    null,
    JSON.stringify({ gmroi: 80, turnover_days: 400 }),
    JSON.stringify({ retire_days: 365 }),
    0.7, ts2);

  const row2 = mdb13.prepare('SELECT * FROM product_retirement_status WHERE ne_product_code = ?').get('TESTPROD001');
  expectEq(row2?.status, '撤退検討', '13 UPDATE status');
  expectEq(row2?.disposal_rate, 0.7, '13 UPDATE disposal_rate');
  expectEq(row2?.plan_details_json, null, '13 UPDATE plan_details_json → null');
  expectEq(row2?.updated_at, ts2, '13 UPDATE updated_at');

  mdb13.close();
  console.log('[OK] product_retirement_status UPSERT + snapshot 保存確認');
}

// ───────────────────────────────────────────────
// Test 14: classifyProduct 純関数 - 分類網羅（PR2c）
// ───────────────────────────────────────────────
console.log('\n=== Test 14: classifyProduct 分類網羅 ===');
{
  const { classifyProduct } = await import('../apps/profit-analysis/candidates.js');
  const {
    DEFAULT_RETIREMENT_THRESHOLDS,
    DEFAULT_CLASSIFICATION_THRESHOLDS,
  } = await import('../apps/profit-analysis/retirement-thresholds.js');
  const thresholds = {
    retirement: DEFAULT_RETIREMENT_THRESHOLDS,
    classification: DEFAULT_CLASSIFICATION_THRESHOLDS,
  };
  const opts = { today: '2026-04-21', periodDays: 90 };

  function mkRow(overrides = {}) {
    return {
      商品コード: 'TEST', 商品名: 'テスト商品', 売上分類: 1,
      標準売価: 1000, 原価: 300, 送料: 100, 消費税率: 10,
      seasonality_flag: 0, season_months: null,
      new_product_flag: 0, new_product_launch_date: null,
      仕入先コード: 'S1', 管理在庫数: 100,
      daily_last_sale: '2026-04-15',
      sales_period: 50, sales_30d: 20, sales_90d: 50,
      monthly_last_month: '2026-04',
      avg_stock: 100, stock_snapshot_months: 6, latest_stock: 100,
      retirement_status: null,
      ...overrides,
    };
  }

  // 評価不能: 売価 0
  let r = classifyProduct(mkRow({ 標準売価: 0 }), thresholds, opts);
  expectEq(r.classification, '評価不能', '14a 売価0 → 評価不能');
  expectEq(r.reason, '楽天売価未設定', '14a 理由');

  // 計算不能: 原価 0
  r = classifyProduct(mkRow({ 原価: 0 }), thresholds, opts);
  expectEq(r.classification, '分類外', '14b 原価0 → 分類外');
  expectEq(r.reason, '計算不能（原価未登録）', '14b 理由');

  // 新商品保留
  r = classifyProduct(mkRow({ new_product_flag: 1 }), thresholds, opts);
  expectEq(r.classification, '分類外', '14c 新商品 → 分類外');
  expectEq(r.reason, '新商品保留', '14c 理由');

  // 季節性保留（4月は 6,7,8 に含まれない）
  r = classifyProduct(mkRow({ seasonality_flag: 1, season_months: '6,7,8' }), thresholds, opts);
  expectEq(r.classification, '分類外', '14d 季節性オフシーズン → 分類外');
  expectEq(r.reason, '季節性保留（オフシーズン）', '14d 理由');

  // 季節性だが現在月(4月)が season_months に含まれる
  r = classifyProduct(mkRow({ seasonality_flag: 1, season_months: '3,4,5' }), thresholds, opts);
  if (r.classification === '分類外' && r.reason.includes('季節性')) {
    throw new Error('14e 季節性オンシーズンで分類外になった');
  }
  console.log(`[OK] 14e 季節性オンシーズン(4月 in 3,4,5) → 分類外扱いされない`);

  // 撤退候補: 自社で 365日販売なし
  //   実環境では mirror_sales_daily に 90日より前の記録は残らないので daily は null 想定
  //   monthly_last_month=2025-03 → 月末 2025-03-31、2026-04-21 から 386日後 > 365
  r = classifyProduct(mkRow({
    売上分類: 1,
    daily_last_sale: null,
    monthly_last_month: '2025-03',
    sales_period: 0, sales_30d: 0, sales_90d: 0,
  }), thresholds, opts);
  expectEq(r.classification, '撤退候補', '14f 自社 ~386日販売なし → 撤退候補');

  // 撤退警戒: 自社で 180-365日販売なし
  //   monthly_last_month=2025-09 → 月末 2025-09-30、2026-04-21 から ~203日後
  r = classifyProduct(mkRow({
    売上分類: 1,
    daily_last_sale: null,
    monthly_last_month: '2025-09',
    sales_period: 0, sales_30d: 0, sales_90d: 0,
  }), thresholds, opts);
  expectEq(r.classification, '撤退警戒', '14g 自社 ~203日販売なし → 撤退警戒');

  // 仕入特有: 販売ありでも GMROI<30% AND 回転>180日 で撤退候補
  r = classifyProduct(mkRow({
    売上分類: 3,
    標準売価: 1000, 原価: 900, 送料: 50,  // 利益単価 = 1000*0.9-900-50 = -50（赤字）
    daily_last_sale: '2026-04-01',
    sales_period: 1, sales_30d: 0, sales_90d: 1,  // 販売あるが超少ない
    avg_stock: 100, latest_stock: 100,
    // 回転 = 100 / (1/90) = 9000日 → 180日超
    // GMROI = (-50 × 1) / (100 × 900) × 100 ≈ 0% < 30%
  }), thresholds, opts);
  // 90日間内だが販売は 1 個のみ → daily_last_sale='2026-04-01' = 20日前、warn=90日には達していない
  // なので retire_gmroi_lt + retire_turnover_gt 条件がトリガー
  expectEq(r.classification, '撤退候補', '14h 仕入 GMROI<30% AND 回転>180日 → 撤退候補');

  // 優良在庫: GMROI > 200% AND 回転 30〜90日
  r = classifyProduct(mkRow({
    売上分類: 1,
    標準売価: 1000, 原価: 200, 送料: 50,  // 利益単価 = 900-250 = 650
    avg_stock: 10, latest_stock: 50,
    sales_period: 90, sales_30d: 30, sales_90d: 90,
    daily_last_sale: '2026-04-15',
    // 回転 = 50 / (90/90) = 50日 (30-90内)
    // GMROI = (650 × 90) / (10 × 200) × 100 = 29250% > 200%
  }), thresholds, opts);
  expectEq(r.classification, '優良在庫', '14i 優良在庫');

  // 観察継続: GMROI 100-200%
  r = classifyProduct(mkRow({
    売上分類: 1,
    標準売価: 1000, 原価: 600, 送料: 100,  // 利益単価 = 900-700 = 200
    avg_stock: 30, latest_stock: 30,
    sales_period: 45, sales_30d: 15, sales_90d: 45,
    daily_last_sale: '2026-04-15',
    // GMROI = (200 × 45) / (30 × 600) × 100 = 9000/18000 × 100 = 50% - 低すぎる
  }), thresholds, opts);
  // 50% は観察（100-200）の範囲外、分類外扱い
  if (r.classification === '観察継続') {
    throw new Error('14j 期待値 GMROI 50% だが観察継続になった');
  }

  // 観察継続 正例: GMROI 150% 狙い
  r = classifyProduct(mkRow({
    売上分類: 1,
    標準売価: 1000, 原価: 300, 送料: 100,  // 利益単価 = 900-400 = 500
    avg_stock: 15, latest_stock: 200,  // 大量在庫で回転遅め
    sales_period: 45, sales_30d: 15, sales_90d: 45,
    daily_last_sale: '2026-04-15',
    // GMROI = (500 × 45) / (15 × 300) × 100 = 22500/4500 × 100 = 500% (優良範囲)
    // 回転 = 200 / (45/90) = 400日 (優良範囲外)
  }), thresholds, opts);
  // 実際 GMROI 500% は good_stock 範囲 (gmroi_gt: 200) だが turnover 400日 は 30-90の範囲外
  // → 値下げ候補へ（回転>120日 + 粗利率50%>20%）
  expectEq(r.classification, '値下げ候補', '14k 高粗利・低回転 → 値下げ候補');

  // 分類外: 販売実績不足 + 在庫あり
  r = classifyProduct(mkRow({
    売上分類: 1,
    daily_last_sale: '2026-04-15',
    sales_period: 0, sales_30d: 0, sales_90d: 0,
    latest_stock: 50,
  }), thresholds, opts);
  expectEq(r.classification, '分類外', '14l 販売0+在庫あり → 分類外');
  // days_since_sale は 6日（2026-04-15 → 2026-04-21） → retirement 閾値未達
  expectEq(r.reason.includes('販売実績不足') || r.reason.includes('閾値外'), true, '14l 理由');

  // 観察継続 正例: 年率 GMROI 約 152%（100-200 範囲）
  //   売価800 原価200 送料50 → 利益単価 470, 粗利率 58.75%
  //   periodDays=90, sales_period 4, avg_stock 25
  //   period_profit = 470×4 = 1880
  //   avg_stock_value = 25×200 = 5000
  //   period gmroi = 1880/5000 × 100 = 37.6%
  //   年率 = 37.6 × (365/90) ≈ 152.49%  → 観察 (100-200) 範囲
  //   latest_stock 25, dailyAvg = 4/90 ≈ 0.0444 → 回転 25/0.0444 ≈ 562日（>120）
  //   観察 (GMROI 100-200) が 値下げ より先に判定されるので 観察継続 になる
  r = classifyProduct(mkRow({
    売上分類: 1,
    標準売価: 800, 原価: 200, 送料: 50,
    daily_last_sale: '2026-04-15',
    sales_period: 4, sales_30d: 1, sales_90d: 4,
    avg_stock: 25, latest_stock: 25,
  }), thresholds, opts);
  expectEq(r.classification, '観察継続', '14m 観察継続 正例（年率 GMROI ≈ 152%）');
  expectEq(Math.round(r.metrics.gmroi), 152, '14m 年率 GMROI ≈ 152');

  // 仕入 warn_gmroi_lt 単独: 年率 GMROI 約 28% < 50% (warn) → 撤退警戒
  //   売価1000 原価500 送料50 → 利益単価 350
  //   periodDays=90, sales_period 20
  //   period_profit = 350×20 = 7000
  //   avg_stock 200 → avg_stock_value = 200×500 = 100000
  //   period gmroi = 7%、年率 = 7 × 4.0556 ≈ 28%
  //   - 年率 28 < retire_gmroi_lt 30、turnover 135 < 180 → retire 複合条件 NOT トリガー
  //   - 年率 28 < warn_gmroi_lt 50 → 撤退警戒
  r = classifyProduct(mkRow({
    売上分類: 3,
    標準売価: 1000, 原価: 500, 送料: 50,
    daily_last_sale: '2026-04-15',
    sales_period: 20, sales_30d: 8, sales_90d: 20,
    avg_stock: 200, latest_stock: 30,  // turnover = 30/(20/90) = 135日
  }), thresholds, opts);
  expectEq(r.classification, '撤退警戒', '14n 仕入 年率 GMROI<50% 単独 → 撤退警戒');
  expectEq(r.reason.includes('GMROI') && r.reason.includes('50%'), true, '14n 理由に GMROI/50%');

  // ─── Test 14o: annualizeGmroiPercent ヘルパー + 閾値境界（Codex R1 Medium 反映） ───
  const { annualizeGmroiPercent, DAYS_PER_YEAR } = await import('../apps/profit-analysis/candidates.js');
  const close = (a, b, eps = 0.01) => Math.abs(a - b) <= eps;

  expectEq(DAYS_PER_YEAR, 365, '14o DAYS_PER_YEAR=365');
  expectEq(close(annualizeGmroiPercent(100, 100, 365), 100), true, '14o periodDays=365 で annualized = period');
  expectEq(close(annualizeGmroiPercent(100, 100, 90), 100 * 365 / 90), true, '14o periodDays=90 の年率化');
  expectEq(close(annualizeGmroiPercent(100, 100, 30), 100 * 365 / 30), true, '14o periodDays=30 の年率化');
  expectEq(annualizeGmroiPercent(0, 100, 90), 0, '14o period_profit=0');
  expectEq(annualizeGmroiPercent(100, 0, 90), null, '14o avg_stock_value=0 で null');
  expectEq(annualizeGmroiPercent(100, -1, 90), null, '14o avg_stock_value<0 で null');
  expectEq(annualizeGmroiPercent(100, 100, 0), null, '14o periodDays=0 で null');
  expectEq(annualizeGmroiPercent(100, 100, -5), null, '14o periodDays<0 で null');

  // 境界: GMROI = ちょうど 200 → 優良条件は 「GMROI > 200」strict なので 観察（100〜200 inclusive）へ
  //   profit 180, avg_stock_value 365, periodDays 90 → annualized 180 × 365/90 / 365 × 100 = 200.000
  //   売価 300, 原価 1, 送料 89 → 利益単価 = 270-90 = 180
  //   avg_stock 365, cost 1 → avg_stock_value = 365
  r = classifyProduct(mkRow({
    売上分類: 1,
    標準売価: 300, 原価: 1, 送料: 89,
    daily_last_sale: '2026-04-15',
    sales_period: 1, sales_30d: 1, sales_90d: 1,
    avg_stock: 365, latest_stock: 365,
  }), thresholds, opts);
  expectEq(close(r.metrics.gmroi, 200), true, '14o 境界 GMROI = 200.00');
  // turnover 365 / (1/90) = 32850日 → 30-90 範囲外、観察条件（GMROI 100-200）inclusive でマッチ
  expectEq(r.classification, '観察継続', '14o GMROI=200（優良は strict >200）→ 観察');

  // 境界: GMROI = ちょうど 100 → 観察下限 inclusive
  //   profit 90, avg_stock_value 365, periodDays 90 → annualized = 100
  r = classifyProduct(mkRow({
    売上分類: 1,
    標準売価: 200, 原価: 1, 送料: 89,  // 利益単価 = 180-90 = 90
    daily_last_sale: '2026-04-15',
    sales_period: 1, sales_30d: 1, sales_90d: 1,
    avg_stock: 365, latest_stock: 365,
  }), thresholds, opts);
  expectEq(close(r.metrics.gmroi, 100), true, '14o 境界 GMROI = 100.00');
  expectEq(r.classification, '観察継続', '14o GMROI=100（観察下限 inclusive）→ 観察');

  // 境界: GMROI = ちょうど 50 （仕入 warn 境界 strict <） → 警戒条件はトリガーしない
  //   profit 45, avg_stock_value 365, periodDays 90 → annualized = 50
  r = classifyProduct(mkRow({
    売上分類: 3,
    標準売価: 100, 原価: 1, 送料: 44,  // 利益単価 = 90-45 = 45
    daily_last_sale: '2026-04-15',  // 最近販売あり、retire 日数条件 NG
    sales_period: 1, sales_30d: 1, sales_90d: 1,
    avg_stock: 365, latest_stock: 50,  // turnover 50/(1/90) = 4500 → retire_turnover_gt 180 超
    // 警戒条件は warn_gmroi_lt=50 vs annualized 50 → NOT < 50 (strict) → warn トリガーしない
    // retire 条件は retire_gmroi_lt=30 vs 50 → NOT < 30 → retire-gmroi もトリガーしない
  }), thresholds, opts);
  expectEq(close(r.metrics.gmroi, 50), true, '14o 境界 GMROI = 50.00 (仕入)');
  // どの retire/warn もトリガーしない → 回転日数は大きいので 値下げ or セット 候補
  //   margin_rate = 45/100 * 100 = 45% > 20%、turnover > 120 → 値下げ候補
  expectEq(r.classification, '値下げ候補', '14o 仕入 GMROI=50（warn strict <50）→ 警戒せず 値下げ候補 へ');

  console.log('[OK] classifyProduct 分類網羅テスト完了（14a-14o）');
}

// ─── Test 14p: periodDays 一貫性（同じ販売・在庫レートで分類不変） ───
console.log('\n=== Test 14p: periodDays 30/60/90 で分類と GMROI がほぼ一致 ===');
{
  const { classifyProduct } = await import('../apps/profit-analysis/candidates.js');
  const {
    DEFAULT_RETIREMENT_THRESHOLDS,
    DEFAULT_CLASSIFICATION_THRESHOLDS,
  } = await import('../apps/profit-analysis/retirement-thresholds.js');
  const thresholds = {
    retirement: DEFAULT_RETIREMENT_THRESHOLDS,
    classification: DEFAULT_CLASSIFICATION_THRESHOLDS,
  };
  const close = (a, b, eps = 1.0) => Math.abs(a - b) <= eps;

  function run(periodDays, multiplier) {
    // 販売レート: 30日あたり 10個 を維持、期間に比例して sales_period が増える
    return classifyProduct({
      商品コード: 'RATE001', 商品名: 'periodDays一貫性テスト', 売上分類: 1,
      標準売価: 800, 原価: 200, 送料: 50, 消費税率: 10,
      seasonality_flag: 0, new_product_flag: 0,
      仕入先コード: 'S', 管理在庫数: 50,
      daily_last_sale: '2026-04-15',
      sales_period: 10 * multiplier,
      sales_30d: 10, sales_90d: 30,
      monthly_last_month: '2026-04',
      avg_stock: 50, stock_snapshot_months: 6, latest_stock: 50,
      retirement_status: null,
    }, thresholds, { today: '2026-04-21', periodDays });
  }

  const r30 = run(30, 1);
  const r60 = run(60, 2);
  const r90 = run(90, 3);

  // 同じ販売レート（30日 10個 = 60日 20個 = 90日 30個）で、年率 GMROI はほぼ同じはず
  expectEq(close(r30.metrics.gmroi, r60.metrics.gmroi), true,
    `14p GMROI は periodDays 30 (${r30.metrics.gmroi}) と 60 (${r60.metrics.gmroi}) で近い`);
  expectEq(close(r30.metrics.gmroi, r90.metrics.gmroi), true,
    `14p GMROI は periodDays 30 (${r30.metrics.gmroi}) と 90 (${r90.metrics.gmroi}) で近い`);

  // 分類も同一（販売レートが同じだから）
  expectEq(r30.classification === r60.classification, true,
    `14p 分類は periodDays 30 と 60 で一致（30:${r30.classification} / 60:${r60.classification}）`);
  expectEq(r30.classification === r90.classification, true,
    `14p 分類は periodDays 30 と 90 で一致（30:${r30.classification} / 90:${r90.classification}）`);

  console.log(`[OK] periodDays 一貫性：分類=${r30.classification}、年率 GMROI≈${Math.round(r30.metrics.gmroi)}%`);
}

// ───────────────────────────────────────────────
// Test 15: applyEarlyWarning（PR2c）
// ───────────────────────────────────────────────
console.log('\n=== Test 15: applyEarlyWarning ===');
{
  const { applyEarlyWarning } = await import('../apps/profit-analysis/candidates.js');
  const ew = { past_period_days: 90, recent_period_days: 30, min_past_sales: 10, drop_ratio: 0.33 };

  function mkCand(overrides) {
    return {
      ne_product_code: 'X',
      sales: { sales_period: 0, sales_30d: 0, sales_90d: 0 },
      flags: { seasonality_off_season: false, new_product: false },
      ...overrides,
    };
  }

  // drop: 過去30件、直近3件以下で急落（期待値 = 30*30/90 = 10、閾値 = 10*0.33 = 3.3）
  let [r] = applyEarlyWarning([mkCand({ sales: { sales_period: 30, sales_30d: 3, sales_90d: 30 } })], ew);
  expectEq(r.early_warning?.type, 'drop', '15a 急落検知');

  // 正常: 直近10件なら閾値超え、drop にならない
  [r] = applyEarlyWarning([mkCand({ sales: { sales_period: 30, sales_30d: 10, sales_90d: 30 } })], ew);
  expectEq(r.early_warning, null, '15b 正常範囲は null');

  // insufficient: 過去販売 < 10
  [r] = applyEarlyWarning([mkCand({ sales: { sales_period: 5, sales_30d: 0, sales_90d: 5 } })], ew);
  expectEq(r.early_warning?.type, 'insufficient', '15c 判定不足');

  // indeterminate: 過去0、直近のみ販売
  [r] = applyEarlyWarning([mkCand({ sales: { sales_period: 0, sales_30d: 5, sales_90d: 0 } })], ew);
  expectEq(r.early_warning?.type, 'indeterminate', '15d 判定不能');

  // 販売なし: 過去0 直近0 → null
  [r] = applyEarlyWarning([mkCand({ sales: { sales_period: 0, sales_30d: 0, sales_90d: 0 } })], ew);
  expectEq(r.early_warning, null, '15e 販売なしは null');

  // skip: 季節性 or 新商品
  [r] = applyEarlyWarning([mkCand({
    flags: { seasonality_off_season: true, new_product: false },
    sales: { sales_period: 30, sales_30d: 0, sales_90d: 30 },
  })], ew);
  expectEq(r.early_warning?.type, 'skip', '15f 季節性は skip');

  [r] = applyEarlyWarning([mkCand({
    flags: { seasonality_off_season: false, new_product: true },
    sales: { sales_period: 30, sales_30d: 0, sales_90d: 30 },
  })], ew);
  expectEq(r.early_warning?.type, 'skip', '15g 新商品は skip');

  console.log('[OK] applyEarlyWarning 分岐テスト完了');
}

// ───────────────────────────────────────────────
// Test 16: fetchCandidatesRaw SQL 取得（PR2c 統合テスト）
//   最小構成の mirror DB にデータを投入し、SQL が期待通り JOIN・集計するか検証
// ───────────────────────────────────────────────
console.log('\n=== Test 16: fetchCandidatesRaw SQL 統合 ===');
{
  const { fetchCandidatesRaw } = await import('../apps/profit-analysis/candidates.js');

  process.env.DATA_DIR = TMP_MIR;
  initMirrorDB();
  const mdb16 = getMirrorDB();

  // クリーンアップ
  mdb16.exec('DELETE FROM mirror_products');
  mdb16.exec('DELETE FROM mirror_sales_daily');
  mdb16.exec('DELETE FROM mirror_sales_monthly');
  mdb16.exec('DELETE FROM mirror_stock_monthly_snapshot');
  mdb16.exec('DELETE FROM product_retirement_status');

  // 3商品投入: 売上分類 1 が2件、2 が1件
  const ts = '2026-04-21 10:00:00';
  const insertP = mdb16.prepare(`INSERT INTO mirror_products
    (product_id, 商品コード, 商品名, 商品区分, 原価状態, 取扱区分, 標準売価, 原価, 送料, 売上分類,
     seasonality_flag, new_product_flag, updated_at)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`);
  insertP.run(1, 'P001', '自社商品A', '単品', 'COMPLETE', '取扱中', 1000, 300, 100, 1, 0, 0, ts);
  insertP.run(2, 'P002', '自社商品B', '単品', 'COMPLETE', '取扱中', 2000, 800, 100, 1, 0, 0, ts);
  insertP.run(3, 'P003', '仕入商品', '単品', 'COMPLETE', '取扱中', 1500, 500, 50, 3, 0, 0, ts);
  // 取扱中止はフィルタされる
  insertP.run(4, 'P004', '取扱中止', '単品', 'COMPLETE', '取扱中止', 1000, 300, 100, 1, 0, 0, ts);
  // セット商品: candidates に含めない（構成品単位で集計する設計、設計書§14）
  insertP.run(5, 'PSET001', '自社セット商品', 'セット', 'COMPLETE', '取扱中', 3000, 1200, 150, 1, 0, 0, ts);

  // sales_daily
  const insertSD = mdb16.prepare(`INSERT INTO mirror_sales_daily
    (日付, 商品コード, モール, 数量, データ種別, updated_at)
    VALUES (?, ?, 'rakuten', ?, 'by_product', ?)`);
  insertSD.run('2026-04-15', 'P001', 10, ts);
  insertSD.run('2026-04-10', 'P001', 5, ts);
  insertSD.run('2026-03-20', 'P001', 8, ts);  // 30日より前、90日内
  insertSD.run('2026-04-18', 'P002', 3, ts);

  // sales_monthly
  const insertSM = mdb16.prepare(`INSERT INTO mirror_sales_monthly
    (月, 商品コード, モール, 数量, データ種別, updated_at)
    VALUES (?, ?, 'rakuten', ?, 'by_product', ?)`);
  insertSM.run('2025-12', 'P001', 20, ts);  // 4ヶ月前

  // stock_snapshot
  const insertSS = mdb16.prepare(`INSERT INTO mirror_stock_monthly_snapshot
    (年月, 商品コード, 月末在庫数, snapshot_source, updated_at)
    VALUES (?, ?, ?, ?, ?)`);
  insertSS.run('2026-04', 'P001', 50, 'logizard', ts);
  insertSS.run('2026-03', 'P001', 60, 'logizard', ts);
  insertSS.run('2026-02', 'P001', 70, 'logizard', ts);

  // sales_class=1 を取得
  const rows = fetchCandidatesRaw(mdb16, { salesClass: '1', periodDays: 90, today: '2026-04-21' });
  expectEq(rows.length, 2, '16a 売上分類1 取扱中 は2件（P001, P002。P004 取扱中止 / PSET001 セット は除外）');
  expectEq(rows.some(r => r.商品コード === 'P001'), true, '16a P001 含まれる');
  expectEq(rows.some(r => r.商品コード === 'P004'), false, '16a P004 取扱中止は除外');
  expectEq(rows.some(r => r.商品コード === 'PSET001'), false, '16a PSET001 セット商品は除外（構成品単位で集計するため）');

  const p001 = rows.find(r => r.商品コード === 'P001');
  expectEq(p001.sales_period, 23, '16b P001 period 90日販売合計 10+5+8=23');
  expectEq(p001.sales_30d, 15, '16c P001 30日販売 10+5=15');  // 3/20 は 30日より前
  expectEq(p001.sales_90d, 23, '16d P001 90日販売 =period');
  expectEq(p001.daily_last_sale, '2026-04-15', '16e P001 daily_last_sale');
  expectEq(p001.monthly_last_month, '2025-12', '16f P001 monthly_last_month');
  expectEq(p001.latest_stock, 50, '16g P001 latest_stock (2026-04)');
  expectEq(Math.round(p001.avg_stock), 60, '16h P001 avg_stock = (50+60+70)/3');
  expectEq(p001.stock_snapshot_months, 3, '16i P001 snapshot_months');

  // sales_class=3（仕入）1件のみ
  const rows3 = fetchCandidatesRaw(mdb16, { salesClass: '3', periodDays: 90, today: '2026-04-21' });
  expectEq(rows3.length, 1, '16j 売上分類3 1件');
  expectEq(rows3[0].商品コード, 'P003', '16j P003');

  // 16k: 在庫 6ヶ月境界（Codex PR2c Round 1 Medium #3 反映）
  //   today=2026-04-21 なら setMonth(-5) → 2025-11 以降 = 6ヶ月（2025-11 〜 2026-04）
  //   2025-10 の行は除外されるべき
  mdb16.exec('DELETE FROM mirror_stock_monthly_snapshot');
  insertSS.run('2025-10', 'P001', 80, 'logizard', ts);   // 境界外（6ヶ月前より前）
  insertSS.run('2025-11', 'P001', 70, 'logizard', ts);   // 境界内
  insertSS.run('2025-12', 'P001', 60, 'logizard', ts);
  insertSS.run('2026-01', 'P001', 55, 'logizard', ts);
  insertSS.run('2026-02', 'P001', 50, 'logizard', ts);
  insertSS.run('2026-03', 'P001', 45, 'logizard', ts);
  insertSS.run('2026-04', 'P001', 40, 'logizard', ts);

  const rowsBoundary = fetchCandidatesRaw(mdb16, { salesClass: '1', periodDays: 90, today: '2026-04-21' });
  const p001b = rowsBoundary.find(r => r.商品コード === 'P001');
  expectEq(p001b.stock_snapshot_months, 6, '16k 境界内は6ヶ月（2025-11 〜 2026-04）、2025-10 は除外');
  expectEq(Math.round(p001b.avg_stock), Math.round((70+60+55+50+45+40)/6),
    '16k avg_stock = 6ヶ月平均');
  expectEq(p001b.latest_stock, 40, '16k latest_stock = 2026-04');

  // 16l: 月末日 overflow 対策の回帰検知（Codex PR2c Round 2 Medium 反映）
  //   today=2026-07-31 だと setMonth(-5) 経由は 2026-02-31 → 2026-03-03 にオーバーフロー。
  //   月初固定 Date.UTC(y, m-5, 1) で '2026-02' に正しく切れるか検証。
  //   2月の snapshot があるとき、stock_snapshot_months=6 で 2月が含まれることを確認。
  mdb16.exec('DELETE FROM mirror_stock_monthly_snapshot');
  insertSS.run('2026-01', 'P001', 100, 'logizard', ts);  // 境界外
  insertSS.run('2026-02', 'P001',  90, 'logizard', ts);  // 境界内 ← これが overflow の罠で除外されるバグ
  insertSS.run('2026-03', 'P001',  80, 'logizard', ts);
  insertSS.run('2026-04', 'P001',  70, 'logizard', ts);
  insertSS.run('2026-05', 'P001',  60, 'logizard', ts);
  insertSS.run('2026-06', 'P001',  50, 'logizard', ts);
  insertSS.run('2026-07', 'P001',  40, 'logizard', ts);

  const overflowCheck = fetchCandidatesRaw(mdb16, { salesClass: '1', periodDays: 90, today: '2026-07-31' });
  const p001c = overflowCheck.find(r => r.商品コード === 'P001');
  expectEq(p001c.stock_snapshot_months, 6, '16l 7/31 でも 2026-02〜07 の6ヶ月（month overflow 回避）');
  expectEq(Math.round(p001c.avg_stock), Math.round((90+80+70+60+50+40)/6),
    '16l avg_stock に 2月が含まれる（65）');

  // 16m: module API でも period_days > 90 を reject（Codex PR2c Round 2 Low 反映）
  let threw16m = false;
  try { fetchCandidatesRaw(mdb16, { salesClass: '1', periodDays: 180, today: '2026-04-21' }); }
  catch { threw16m = true; }
  expectEq(threw16m, true, '16m fetchCandidatesRaw periodDays=180 で throw');

  let threw16m2 = false;
  try { fetchCandidatesRaw(mdb16, { salesClass: '1', periodDays: 0, today: '2026-04-21' }); }
  catch { threw16m2 = true; }
  expectEq(threw16m2, true, '16m fetchCandidatesRaw periodDays=0 で throw');

  mdb16.close();
  console.log('[OK] fetchCandidatesRaw SQL 統合テスト完了');
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
// Test 17-22: PR3 (タブB UI) 回帰防止
// ───────────────────────────────────────────────
const EJS_PATH = path.join(__dirname, '..', 'views', 'profit-analysis.ejs');
const EJS_TEMPLATE = fs.readFileSync(EJS_PATH, 'utf8');
function renderView(opts) {
  return ejs.render(EJS_TEMPLATE, Object.assign({
    username: 'test@example.com',
    displayName: 'Test User',
    featureFlagEnabled: false,
  }, opts), { filename: EJS_PATH });
}

// Test 17: 改称確認
console.log('\n=== Test 17: EJS レンダリングで「商品収益性ダッシュボード」改称確認 ===');
{
  const html = renderView({ featureFlagEnabled: false });
  expectEq(html.includes('商品収益性ダッシュボード'), true, '17 タイトル・見出しに新名称が含まれる');
  expectEq(html.includes('<title>商品収益性ダッシュボード'), true, '17 <title> タグも改称');
  // 旧名称「粗利分析」が top-level 見出しに残っていないか（サブ見出し「モール別粗利分析」は残す）
  expectEq(html.includes('<h2>粗利分析</h2>'), false, '17 旧 <h2>粗利分析</h2> が残っていない');
  console.log('[OK] 改称確認');
}

// Test 18: feature flag OFF で タブB DOM が出ない（Codex PR3 実装 R1 High 1/2 反映強化）
console.log('\n=== Test 18: feature flag OFF で タブB 関連文字列が HTML に漏れない ===');
{
  const html = renderView({ featureFlagEnabled: false });

  // タブB 関連の機能を示唆する文字列を網羅的にチェック
  const forbiddenStrings = [
    // 機能名
    '在庫整理・撤退判断支援', '売上分類', '撤退判断', '消化計画中',
    'リブランディング検討', '再生産判断中',
    // DOM ID/class（pd- prefix 戦略）
    'id="pd-tab-b"', 'id="pd-status-modal"', 'id="pd-threshold-panel"',
    'id="pd-inv-subtabs"', 'id="pd-inv-table-container"',
    'pd-mega-tab', 'pd-segmented', 'pd-flag-chip', 'pd-impact-cards',
    'pd-modal-backdrop', 'pd-threshold-panel', 'pd-status-radio-group',
    // 関数・イベント名
    'initTabB', 'switchMegaTab', 'switchSalesClass', 'refreshCandidates',
    'openStatusModal', 'saveStatus', 'saveThresholds', 'renderImpactCards',
    'renderInventoryTable', 'exportInventoryCSV',
    'profit-dashboard:activate-section-b',
    // API URL
    'api/inventory/candidates', 'api/inventory/thresholds', 'api/inventory/status',
    // state 名
    'stateB',
    // 大タブデータ属性
    'data-section="inventory"',
  ];
  for (const s of forbiddenStrings) {
    if (html.includes(s)) {
      throw new Error(`[FAIL] 18 flag OFF HTML に漏れ: "${s}"`);
    }
  }
  console.log(`[OK] 18 flag OFF で禁則文字列 ${forbiddenStrings.length} 種が全て不在`);

  // タブA（粗利分析）側は通常表示されている
  expectEq(html.includes('id="filter-days"'), true, '18 タブA の期間フィルタは残る');
  expectEq(html.includes('data-view="worst"'), true, '18 タブA のサブタブは残る');
  expectEq(html.includes('data-view="trend"'), true, '18 タブA 前月比悪化サブタブ');
  // 大タブボタン DOM は出ない（flag ON 時のみ描画）
  expectEq(html.includes('data-section="profit"'), false, '18 大タブA ボタンも flag OFF では描画しない');
  console.log('[OK] 18 タブA は通常表示、大タブは出ない');
}

// Test 19: feature flag ON で タブB DOM が出る
console.log('\n=== Test 19: feature flag ON で タブB DOM が出る ===');
{
  const html = renderView({ featureFlagEnabled: true });
  expectEq(html.includes('在庫整理・撤退判断支援'), true, '19 大タブB ボタン見出し');
  expectEq(html.includes('id="pd-tab-b"'), true, '19 タブB パネル DOM');
  expectEq(html.includes('initTabB'), true, '19 タブB 初期化 JS');
  expectEq(html.includes('id="pd-status-modal"'), true, '19 ステータスモーダル DOM');
  expectEq(html.includes('id="pd-threshold-panel"'), true, '19 閾値パネル DOM');
  expectEq(html.includes('data-class="1"'), true, '19 売上分類セグメント 1');
  expectEq(html.includes('switchMegaTab'), true, '19 大タブ切替関数');
  expectEq(html.includes('api/inventory/candidates'), true, '19 候補 API 呼び出し URL');
  expectEq(html.includes('api/inventory/thresholds'), true, '19 閾値 API 呼び出し URL');
  expectEq(html.includes('api/inventory/status'), true, '19 status API 呼び出し URL');
  expectEq(html.includes('data-pd-open-status'), true, '19 data-attribute event delegation に移行済み');
  // Codex PR3 実装 R1 High 3 反映: inline onclick openStatusModal を使っていない
  expectEq(html.includes("onclick=\"openStatusModal("), false, '19 inline onclick="openStatusModal( は残っていない');
  console.log('[OK] feature flag ON で タブB の全主要要素が HTML に含まれる（inline onclick XSS 対策済み）');
}

// Test 19-B: PR4 で撤去（Dark Launch 解除、description にタブB 機能名を含めてよくなった）
//   オリジナルは「description に タブB 機能語が含まれないこと」を検証していたが、
//   PR4 で description を '商品別粗利分析 + 在庫整理・撤退判断支援' に更新したため役目終了。
//   Dark Launch 段階での漏れ検知は git 履歴 (PR3 コミット) に残っている。

// Test 20: XSS エスケープ（escapeHtml ヘルパーの回帰防止、EJS の <%= %> 自動エスケープ、注入確認）
console.log('\n=== Test 20: XSS エスケープ ===');
{
  // 20-a: EJS の <%= %> による displayName の自動エスケープ
  const xssHtml = renderView({ username: 'normal@example.com', displayName: '<script>alert(1)</script>' });
  expectEq(xssHtml.includes('<script>alert(1)</script>'), false, '20a displayName の XSS が生で出ない');
  expectEq(xssHtml.includes('&lt;script&gt;'), true, '20a escape された文字列が HTML 上にある');

  // 20-b: escapeHtml 関数（EJS 内の inline JS helper、client-side レンダリングで使う）の回帰防止
  // EJS テンプレート内に埋まっている想定だが、この test では同等実装で契約検証
  const HTML_ESCAPE_MAP = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' };
  function escapeHtml(s) {
    if (s === null || s === undefined) return '';
    return String(s).replace(/[&<>"']/g, ch => HTML_ESCAPE_MAP[ch]);
  }
  expectEq(escapeHtml('<script>alert(1)</script>'), '&lt;script&gt;alert(1)&lt;/script&gt;', '20b <script> タグの escape');
  expectEq(escapeHtml('&<>"\''), '&amp;&lt;&gt;&quot;&#39;', '20b 基本5文字');
  expectEq(escapeHtml(null), '', '20b null → 空文字');
  expectEq(escapeHtml(''), '', '20b 空 → 空');

  // 20-c: EJS テンプレートに直接 escapeHtml helper 定義が含まれていること（実装側の回帰防止）
  expectEq(EJS_TEMPLATE.includes('function escapeHtml'), true, '20c EJS 内に escapeHtml 関数定義が残っている');
  expectEq(EJS_TEMPLATE.includes('HTML_ESCAPE_MAP'), true, '20c HTML_ESCAPE_MAP 定義');
  console.log('[OK] XSS エスケープ契約');
}

// Test 21: 処分率 ratio/percent 単位変換（Codex §6-A 回帰防止）
console.log('\n=== Test 21: 処分率 ratio/percent 単位変換 ===');
{
  function ratioToPercent(r) {
    if (r === null || r === undefined || !Number.isFinite(Number(r))) return '';
    return Math.round(Number(r) * 1000) / 10;
  }
  function percentToRatio(p) {
    const n = Number(p);
    if (!Number.isFinite(n) || n <= 0 || n > 100) {
      throw new Error('処分率は 0% より大きく 100% 以下');
    }
    return n / 100;
  }

  expectEq(ratioToPercent(0.5), 50, '21 ratioToPercent(0.5)');
  expectEq(ratioToPercent(0.075), 7.5, '21 ratioToPercent(0.075)');
  expectEq(ratioToPercent(1), 100, '21 ratioToPercent(1) = 100（境界上限）');
  expectEq(percentToRatio(50), 0.5, '21 percentToRatio(50)');
  expectEq(percentToRatio(7.5), 0.075, '21 percentToRatio(7.5)');
  expectEq(percentToRatio(100), 1, '21 percentToRatio(100) = 1（境界上限）');

  let threw = false;
  try { percentToRatio(0); } catch { threw = true; }
  expectEq(threw, true, '21 percentToRatio(0) → throw');
  threw = false;
  try { percentToRatio(-1); } catch { threw = true; }
  expectEq(threw, true, '21 percentToRatio(-1) → throw');
  threw = false;
  try { percentToRatio(100.5); } catch { threw = true; }
  expectEq(threw, true, '21 percentToRatio(100.5) → throw');
  threw = false;
  try { percentToRatio('abc'); } catch { threw = true; }
  expectEq(threw, true, '21 percentToRatio("abc") → throw');

  // EJS テンプレートに関数が残っていること
  expectEq(EJS_TEMPLATE.includes('function ratioToPercent'), true, '21 EJS 内に ratioToPercent 定義');
  expectEq(EJS_TEMPLATE.includes('function percentToRatio'), true, '21 EJS 内に percentToRatio 定義');
  console.log('[OK] 処分率単位変換の契約');
}

// Test 22: status POST payload 構造
console.log('\n=== Test 22: status POST payload 構造（契約キー名 + decision_metrics 網羅） ===');
{
  // 実際の EJS 内 saveStatus() 関数と同じ payload 構造を生成するロジックを再現
  const candidate = {
    ne_product_code: 'SKU-001',
    product_name: '<商品名>',
    sales_class: 1,
    supplier_code: 'S1',
    classification: '撤退警戒',
    reason: '180日販売なし',
    metrics: {
      gmroi: 120.5, turnover_days: 95, latest_stock: 100, avg_stock: 110.5,
      rakuten_unit_profit: 350.0, stock_snapshot_months: 6,
    },
    sales: { sales_period: 8, sales_30d: 2, sales_90d: 8 },
    flags: { stock_data_insufficient: false, seasonality_off_season: false, new_product: false },
    retirement_status: null,
    early_warning: { type: 'drop', reason: '急落' },
  };
  const thresholds = {
    retirement: { '1': { warn_days_no_sales: 180, retire_days_no_sales: 365 } },
    classification: { good_stock: { gmroi_gt: 200, turnover_min: 30, turnover_max: 90 } },
    early_warning: { past_period_days: 90, recent_period_days: 30, min_past_sales: 10, drop_ratio: 0.33 },
  };
  const stateB = { salesClass: 1, periodDays: 30, thresholds };
  const disposal_rate = 0.5;  // ratio (0, 1]

  // saveStatus() の body 構築部と同等
  const decision_metrics = {
    sales_class: candidate.sales_class,
    classification: candidate.classification,
    gmroi: candidate.metrics.gmroi,
    turnover_days: candidate.metrics.turnover_days,
    latest_stock: candidate.metrics.latest_stock,
    avg_stock: candidate.metrics.avg_stock,
    rakuten_unit_profit: candidate.metrics.rakuten_unit_profit,
    period_days: stateB.periodDays,
    stock_snapshot_months: candidate.metrics.stock_snapshot_months,
    disposal_rate,
    early_warning: candidate.early_warning,
    stock_data_insufficient: !!candidate.flags.stock_data_insufficient,
  };
  const body = {
    ne_product_code: candidate.ne_product_code,
    status: '撤退検討',
    reason: '判断根拠',
    next_review_date: '2026-05-01',
    disposal_rate,
    plan_details: undefined,
    thresholds: { retirement: thresholds.retirement, classification: thresholds.classification, early_warning: thresholds.early_warning },
    decision_metrics,
  };

  // body の必須キーが全部揃っている
  expectEq(typeof body.ne_product_code, 'string', '22 ne_product_code');
  expectEq(body.status, '撤退検討', '22 status');
  expectEq(typeof body.reason, 'string', '22 reason');
  expectEq(typeof body.next_review_date, 'string', '22 next_review_date');
  expectEq(body.disposal_rate > 0 && body.disposal_rate <= 1, true, '22 disposal_rate は (0, 1]');

  // thresholds は retirement/classification/early_warning を含む
  expectEq(!!body.thresholds.retirement, true, '22 thresholds.retirement 含む');
  expectEq(!!body.thresholds.classification, true, '22 thresholds.classification 含む');
  expectEq(!!body.thresholds.early_warning, true, '22 thresholds.early_warning 含む');

  // decision_metrics は契約キー全部含む
  const dm = body.decision_metrics;
  const requiredKeys = ['sales_class', 'classification', 'gmroi', 'turnover_days',
                        'latest_stock', 'avg_stock', 'rakuten_unit_profit',
                        'period_days', 'stock_snapshot_months', 'disposal_rate',
                        'early_warning', 'stock_data_insufficient'];
  for (const k of requiredKeys) {
    if (!(k in dm)) throw new Error(`[FAIL] 22 decision_metrics に ${k} が欠落`);
  }
  console.log(`[OK] 22 decision_metrics 契約キー ${requiredKeys.length}個 網羅`);

  // EJS 内に実際にこの body 構築ロジックが残っていることを確認
  //   Note: ES6 shorthand `decision_metrics,` と通常形 `decision_metrics:` の両方を許容
  expectEq(EJS_TEMPLATE.includes("body = {"), true, '22 EJS 内に body 構築コードが存在');
  expectEq(/\bthresholds\s*[,:]/.test(EJS_TEMPLATE), true, '22 EJS 内で body に thresholds キー');
  expectEq(/\bdecision_metrics\s*[,:]/.test(EJS_TEMPLATE), true, '22 EJS 内で body に decision_metrics キー');
  expectEq(/\bplan_details\s*[,:]/.test(EJS_TEMPLATE), true, '22 EJS 内で body に plan_details キー');
  // 旧キー名（*_json）が body 構築部で使われていないこと
  expectEq(EJS_TEMPLATE.includes('thresholds_json:'), false, '22 EJS 内に thresholds_json: キー設定が残っていない');
  expectEq(EJS_TEMPLATE.includes('decision_metrics_json:'), false, '22 EJS 内に decision_metrics_json: キー設定が残っていない');
  expectEq(EJS_TEMPLATE.includes('plan_details_json:'), false, '22 EJS 内に plan_details_json: キー設定が残っていない');
  console.log('[OK] POST /status payload 構造の契約');
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
