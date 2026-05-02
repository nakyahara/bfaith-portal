/**
 * m_sku_master / m_sku_components / v_sku_resolved / v_sku_costed の
 * スキーマがクリーンDBで作成できることと、CHECK/FK制約が想定通り効くかの試験。
 *
 * 使い方: node scripts/test-sku-master-schema.mjs
 *   一時ファイル C:\tmp\test-sku-master-{pid}.db を作って試験、終了時に削除
 */
import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';
import os from 'os';

const TMP_DB = path.join(os.tmpdir(), `test-sku-master-${process.pid}.db`);
console.log(`[test] DB: ${TMP_DB}`);

if (fs.existsSync(TMP_DB)) fs.unlinkSync(TMP_DB);

const db = new Database(TMP_DB);
db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');

let pass = 0, fail = 0;
function expect(label, fn) {
  try {
    fn();
    console.log(`  ✓ ${label}`);
    pass++;
  } catch (e) {
    console.log(`  ✗ ${label}`);
    console.log(`    ${e.message}`);
    fail++;
  }
}

function expectThrows(label, fn, msgPattern) {
  try {
    fn();
    console.log(`  ✗ ${label} (例外が出るはずだったが正常終了)`);
    fail++;
  } catch (e) {
    if (msgPattern && !msgPattern.test(e.message)) {
      console.log(`  ✗ ${label} (期待と違う例外: ${e.message})`);
      fail++;
    } else {
      console.log(`  ✓ ${label} (期待通り例外: ${e.message.slice(0, 60)}...)`);
      pass++;
    }
  }
}

// 依存ダミーテーブル（FK と JOIN 試験のため）
db.exec(`CREATE TABLE raw_ne_products (
  商品コード TEXT PRIMARY KEY,
  原価       REAL
)`);
db.exec(`CREATE TABLE sku_map (
  seller_sku TEXT NOT NULL,
  ne_code    TEXT NOT NULL,
  数量       INTEGER DEFAULT 1,
  PRIMARY KEY (seller_sku, ne_code)
)`);

// 本体テーブル
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

db.exec(`CREATE VIEW v_sku_resolved AS
  SELECT c.seller_sku, c.ne_code, c.数量, 'master' AS source FROM m_sku_components c
  UNION ALL
  SELECT s.seller_sku, s.ne_code, s.数量, 'auto' AS source FROM sku_map s
  WHERE NOT EXISTS (SELECT 1 FROM m_sku_master m WHERE m.seller_sku = s.seller_sku)
`);

db.exec(`CREATE VIEW v_sku_costed AS
  SELECT v.seller_sku, v.ne_code, v.数量, v.source, p.原価 AS 単価,
    CASE WHEN p.商品コード IS NULL THEN 'ne_missing'
         WHEN p.原価 IS NULL THEN 'cost_missing'
         ELSE 'ok' END AS cost_status
  FROM v_sku_resolved v
  LEFT JOIN raw_ne_products p ON v.ne_code = p.商品コード
`);

console.log('\n[1] 正常系: 単品SKU登録');
expect('m_sku_master 単品INSERT', () => {
  db.prepare('INSERT INTO m_sku_master (seller_sku, 商品名) VALUES (?, ?)').run('884389', 'サンリオ ハンドタオル3P');
});
expect('m_sku_components 単品INSERT', () => {
  db.prepare('INSERT INTO m_sku_components (seller_sku, ne_code, 数量) VALUES (?, ?, ?)').run('884389', '884389', 1);
});

console.log('\n[2] 正常系: セット商品（複数構成）');
expect('セット商品マスタINSERT', () => {
  db.prepare('INSERT INTO m_sku_master (seller_sku, 商品名) VALUES (?, ?)').run('b010100580205', 'エラバシェ シャンプー＆トリートメントセット');
});
expect('構成1 INSERT', () => {
  db.prepare('INSERT INTO m_sku_components (seller_sku, ne_code, 数量, sort_order) VALUES (?, ?, ?, ?)').run('b010100580205', 'ellabache-s', 1, 0);
});
expect('構成2 INSERT', () => {
  db.prepare('INSERT INTO m_sku_components (seller_sku, ne_code, 数量, sort_order) VALUES (?, ?, ?, ?)').run('b010100580205', 'ellabache-t', 1, 1);
});

console.log('\n[3] CHECK制約: 大文字seller_sku拒否');
expectThrows('seller_sku大文字 master', () => {
  db.prepare('INSERT INTO m_sku_master (seller_sku, 商品名) VALUES (?, ?)').run('ABCDEF', 'X');
}, /CHECK constraint/);

expectThrows('seller_sku前後空白 master', () => {
  db.prepare('INSERT INTO m_sku_master (seller_sku, 商品名) VALUES (?, ?)').run(' abc ', 'X');
}, /CHECK constraint/);

expectThrows('商品名空文字 master', () => {
  db.prepare('INSERT INTO m_sku_master (seller_sku, 商品名) VALUES (?, ?)').run('xyz', '');
}, /CHECK constraint/);

console.log('\n[4] CHECK制約: 構成側');
expectThrows('ne_code大文字 components', () => {
  db.prepare('INSERT INTO m_sku_components (seller_sku, ne_code, 数量) VALUES (?, ?, ?)').run('884389', 'ABC', 1);
}, /CHECK constraint/);

expectThrows('数量0 components', () => {
  db.prepare('INSERT INTO m_sku_components (seller_sku, ne_code, 数量) VALUES (?, ?, ?)').run('884389', 'xyz', 0);
}, /CHECK constraint/);

expectThrows('数量負 components', () => {
  db.prepare('INSERT INTO m_sku_components (seller_sku, ne_code, 数量) VALUES (?, ?, ?)').run('884389', 'xyz', -1);
}, /CHECK constraint/);

console.log('\n[5] FK制約: 親なし子NG');
expectThrows('親なしcomponents', () => {
  db.prepare('INSERT INTO m_sku_components (seller_sku, ne_code, 数量) VALUES (?, ?, ?)').run('not_exist', 'xyz', 1);
}, /FOREIGN KEY/);

console.log('\n[6] FK制約: CASCADE削除');
expect('master削除でcomponentsもCASCADE削除', () => {
  db.prepare("INSERT INTO m_sku_master (seller_sku, 商品名) VALUES ('cascade_test', 'X')").run();
  db.prepare("INSERT INTO m_sku_components (seller_sku, ne_code, 数量) VALUES ('cascade_test', 'a', 1)").run();
  db.prepare("INSERT INTO m_sku_components (seller_sku, ne_code, 数量) VALUES ('cascade_test', 'b', 1)").run();
  const before = db.prepare("SELECT COUNT(*) c FROM m_sku_components WHERE seller_sku='cascade_test'").get().c;
  if (before !== 2) throw new Error(`期待: 2, 実際: ${before}`);
  db.prepare("DELETE FROM m_sku_master WHERE seller_sku='cascade_test'").run();
  const after = db.prepare("SELECT COUNT(*) c FROM m_sku_components WHERE seller_sku='cascade_test'").get().c;
  if (after !== 0) throw new Error(`CASCADE失敗、残: ${after}`);
});

console.log('\n[7] PK制約: 同一(seller_sku, ne_code)重複NG');
expectThrows('構成重複INSERT', () => {
  db.prepare('INSERT INTO m_sku_components (seller_sku, ne_code, 数量) VALUES (?, ?, ?)').run('884389', '884389', 2);
}, /UNIQUE constraint/);

console.log('\n[8] v_sku_resolved: master優先 + sku_map fallback');
// 準備: sku_map に 884389 と未紐付けSKU を入れる
db.prepare("INSERT INTO sku_map (seller_sku, ne_code, 数量) VALUES ('884389', 'ne_old', 1)").run(); // master あり
db.prepare("INSERT INTO sku_map (seller_sku, ne_code, 数量) VALUES ('only_in_sku_map', 'fallback_ne', 3)").run(); // master なし

expect('884389: masterのみヒット (auto は遮断)', () => {
  const rows = db.prepare("SELECT * FROM v_sku_resolved WHERE seller_sku='884389'").all();
  if (rows.length !== 1) throw new Error(`期待1行, 実際${rows.length}行`);
  if (rows[0].source !== 'master') throw new Error(`source期待master, 実際${rows[0].source}`);
});

expect('only_in_sku_map: autoから取得', () => {
  const rows = db.prepare("SELECT * FROM v_sku_resolved WHERE seller_sku='only_in_sku_map'").all();
  if (rows.length !== 1) throw new Error(`期待1行, 実際${rows.length}行`);
  if (rows[0].source !== 'auto') throw new Error(`source期待auto, 実際${rows[0].source}`);
  if (rows[0].数量 !== 3) throw new Error(`数量期待3, 実際${rows[0].数量}`);
});

console.log('\n[9] v_sku_costed: 原価解決 + cost_status');
// 準備: 原価ありのNE商品 と 原価NULLのNE商品 と raw_ne_products不在
db.prepare("INSERT INTO raw_ne_products (商品コード, 原価) VALUES ('884389', 500)").run();
db.prepare("INSERT INTO raw_ne_products (商品コード, 原価) VALUES ('ellabache-s', 800)").run();
db.prepare("INSERT INTO raw_ne_products (商品コード, 原価) VALUES ('ellabache-t', NULL)").run(); // 原価NULL
// ellabache-... は raw_ne_products に存在
// fallback_ne は raw_ne_products に不在

expect('cost_status=ok 確認', () => {
  const r = db.prepare("SELECT cost_status, 単価 FROM v_sku_costed WHERE seller_sku='884389'").get();
  if (r.cost_status !== 'ok') throw new Error(`期待ok, 実際${r.cost_status}`);
  if (r.単価 !== 500) throw new Error(`単価期待500, 実際${r.単価}`);
});

expect('cost_status=cost_missing 確認 (NE商品はあるが原価NULL)', () => {
  const r = db.prepare("SELECT cost_status, 単価 FROM v_sku_costed WHERE seller_sku='b010100580205' AND ne_code='ellabache-t'").get();
  if (r.cost_status !== 'cost_missing') throw new Error(`期待cost_missing, 実際${r.cost_status}`);
});

expect('cost_status=ne_missing 確認 (raw_ne_productsに行なし)', () => {
  const r = db.prepare("SELECT cost_status FROM v_sku_costed WHERE seller_sku='only_in_sku_map'").get();
  if (r.cost_status !== 'ne_missing') throw new Error(`期待ne_missing, 実際${r.cost_status}`);
});

db.close();
fs.unlinkSync(TMP_DB);
const wal = TMP_DB + '-wal'; if (fs.existsSync(wal)) fs.unlinkSync(wal);
const shm = TMP_DB + '-shm'; if (fs.existsSync(shm)) fs.unlinkSync(shm);

console.log(`\n結果: ${pass} pass / ${fail} fail`);
process.exit(fail > 0 ? 1 : 0);
