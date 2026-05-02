/**
 * import-sku-master.js を一時DBで動作確認。
 *
 * 1. 一時DBを作成
 * 2. 必要な依存テーブル(raw_ne_products, sku_map)をスタブ作成
 * 3. m_sku_master/components テーブル＋ビューを作成
 * 4. 実CSVを取り込み、結果を出力
 */
import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';
import os from 'os';
import iconv from 'iconv-lite';
import { importSkuMasterCSV } from '../apps/warehouse/import-sku-master.js';

const TMP_DB = path.join(os.tmpdir(), `test-sku-import-${process.pid}.db`);
console.log(`[test] DB: ${TMP_DB}`);

if (fs.existsSync(TMP_DB)) fs.unlinkSync(TMP_DB);

// import-sku-master.js は process.env.DATA_DIR を見ない実装になってるので、
// db.js の getDB が動くように env を設定してから initDB を呼ぶ
process.env.DATA_DIR = path.dirname(TMP_DB);
process.env.DB_FILENAME_OVERRIDE = path.basename(TMP_DB); // 参考のため

const Database2 = (await import('better-sqlite3')).default;
const db = new Database2(TMP_DB);
db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');

// 必要な依存テーブルだけ作る
db.exec(`CREATE TABLE raw_ne_products (商品コード TEXT PRIMARY KEY, 原価 REAL)`);
db.exec(`CREATE TABLE sku_map (
  seller_sku TEXT NOT NULL, ne_code TEXT NOT NULL, 数量 INTEGER DEFAULT 1,
  PRIMARY KEY (seller_sku, ne_code)
)`);

// 本体スキーマ（db.js から複製）
db.exec(`CREATE TABLE m_sku_master (
  seller_sku  TEXT NOT NULL PRIMARY KEY,
  商品名      TEXT NOT NULL,
  created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  created_by  TEXT,
  updated_by  TEXT,
  CHECK (trim(seller_sku) <> ''),
  CHECK (trim(商品名) <> ''),
  CHECK (seller_sku = lower(seller_sku) AND trim(seller_sku) = seller_sku)
)`);

db.exec(`CREATE TABLE m_sku_components (
  seller_sku  TEXT NOT NULL,
  ne_code     TEXT NOT NULL,
  数量        INTEGER NOT NULL DEFAULT 1,
  sort_order  INTEGER NOT NULL DEFAULT 0,
  created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  PRIMARY KEY (seller_sku, ne_code),
  FOREIGN KEY (seller_sku) REFERENCES m_sku_master(seller_sku) ON DELETE CASCADE,
  CHECK (数量 > 0),
  CHECK (trim(seller_sku) <> ''),
  CHECK (trim(ne_code) <> ''),
  CHECK (seller_sku = lower(seller_sku) AND trim(seller_sku) = seller_sku),
  CHECK (ne_code = lower(ne_code) AND trim(ne_code) = ne_code)
)`);
db.close();

// db.js の DB_FILE は process.cwd() + 'data/warehouse.db' なので、
// import-sku-master.js の getDB() を使うため、直接DB呼び出しに切り替えた小さなwrapperを作る
// → やはり import-sku-master.js のロジックを引用してテストする方がシンプル
// import-sku-master.js は ./db.js から getDB を import するので、
// 対象テストではテスト用に initDB を呼んで DATA_DIR を上書きする方が筋
// が、手間なので以下では手動で同等のロジックを呼ぶ

// 直接 importSkuMasterCSV() を呼ぶには getDB() が動く必要がある
// db.js は process.env.DATA_DIR を尊重するのでそれを使う

// initDB() を呼んで warehouse.db のフルセットを作るのは依存が大きすぎるので、
// ここではテスト用に単純な手動シミュレーションをする

const csvPath = 'C:\\Users\\中原　大輔\\OneDrive\\デスクトップ\\Downloads\\FBA在庫補充シート - 商品コード変換テーブル.csv';
if (!fs.existsSync(csvPath)) {
  console.error(`CSV not found: ${csvPath}`);
  process.exit(1);
}

// importSkuMasterCSV を db.js なしで呼ぶため、関数のロジックを再利用しつつ
// getDB をモックする方法を取る
// 一番手っ取り早いのは、import-sku-master.js から関数のコア部分を切り出して
// db を引数で受け取る形にすること
// → 今回はテスト用に reimport で対応せず、importSkuMasterCSV を使えるよう
//   process.env.DATA_DIR を一時dirにして、initDB() で本物の warehouse.db を作る方針に変更

// やり直し: 一時DATA_DIRで initDB → そこに完全なスキーマができる → import実行
fs.unlinkSync(TMP_DB);
const wal = TMP_DB + '-wal'; if (fs.existsSync(wal)) fs.unlinkSync(wal);
const shm = TMP_DB + '-shm'; if (fs.existsSync(shm)) fs.unlinkSync(shm);

// initDB() の DATA_DIR を一時にする
const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'sku-import-test-'));
process.env.DATA_DIR = tmpDir;

const { initDB, getDB } = await import('../apps/warehouse/db.js');
await initDB();

// raw_ne_products を埋める（実際のwarehouse.dbと同等にしたいが、テストでは少しだけ）
// 実CSV内のNEコード全件を「存在する」と仮定しておく方がシンプル
const text = iconv.decode(fs.readFileSync(csvPath), 'utf-8');
const dbi = getDB();
const insNe = dbi.prepare('INSERT OR IGNORE INTO raw_ne_products (商品コード, 原価) VALUES (?, ?)');
const lines = text.split(/\r?\n/);
const seenNe = new Set();
for (let i = 1; i < lines.length; i++) {
  // 簡易抽出（quoted field 無視）。NEコード列は4列目
  const cols = lines[i].split(',');
  const ne = (cols[3] || '').trim().toLowerCase();
  if (ne && !seenNe.has(ne)) {
    insNe.run(ne, 100);
    seenNe.add(ne);
  }
}
console.log(`[test] raw_ne_products dummy populated: ${seenNe.size}件`);

// 本番Import実行
const result = importSkuMasterCSV(csvPath, { dryRun: false, encoding: 'utf-8' });

console.log('\n=== 結果 ===');
console.log(`m_sku_master  : ${result.masterCount}件`);
console.log(`m_sku_components: ${result.componentCount}件`);
console.log(`スキップ      : ${result.skipped}件`);
console.log(`商品名conflict: ${result.nameConflicts.length}件`);
console.log(`ne_code欠落   : ${result.missingNeCodes.length}件`);
console.log(`構成重複      : ${result.dupComponents.length}件`);
console.log(`警告総数      : ${result.warnings.length}件`);

// DBに実際入っているか確認
const masterCount = dbi.prepare('SELECT COUNT(*) c FROM m_sku_master').get().c;
const compCount = dbi.prepare('SELECT COUNT(*) c FROM m_sku_components').get().c;
console.log(`\nDB実体: m_sku_master=${masterCount}, m_sku_components=${compCount}`);

// セット商品サンプル確認
console.log('\n--- セット商品サンプル (構成数2件以上) ---');
const setSample = dbi.prepare(`
  SELECT m.seller_sku, m.商品名,
    (SELECT COUNT(*) FROM m_sku_components c WHERE c.seller_sku=m.seller_sku) AS 構成数,
    (SELECT GROUP_CONCAT(c.ne_code, ',') FROM m_sku_components c WHERE c.seller_sku=m.seller_sku) AS ne_codes
  FROM m_sku_master m
  WHERE 構成数 > 1
  ORDER BY 構成数 DESC
  LIMIT 5
`).all();
setSample.forEach(s => {
  console.log(`  [${s.seller_sku}] 構成${s.構成数}件: ${s.ne_codes}`);
  console.log(`    商品名: ${s.商品名}`);
});

// データの文字化け確認用に、商品名サンプルをUTF-8でファイル出力
const sampleOut = path.join(tmpDir, 'sample-names.txt');
const samples = dbi.prepare('SELECT seller_sku, 商品名 FROM m_sku_master ORDER BY seller_sku LIMIT 20').all();
fs.writeFileSync(sampleOut,
  samples.map(s => `${s.seller_sku}\t${s.商品名}`).join('\n'),
  'utf-8'
);
console.log(`\nサンプル20件を保存: ${sampleOut}`);

dbi.close();
console.log(`\n[NOTE] テストDBはクリーンアップしません: ${tmpDir}`);
console.log('OK');
