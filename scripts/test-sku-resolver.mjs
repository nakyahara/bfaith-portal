/**
 * sku-resolver.js のユニットテスト
 */
import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';
import os from 'os';
import {
  resolveSkuToComponents,
  resolveCost,
  calcOrderCost,
  calcInventoryValue,
  getSkuMasterDetail,
} from '../apps/warehouse/sku-resolver.js';

const TMP_DB = path.join(os.tmpdir(), `test-resolver-${process.pid}.db`);
if (fs.existsSync(TMP_DB)) fs.unlinkSync(TMP_DB);

const db = new Database(TMP_DB);
db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');

// 必要スキーマ
db.exec(`CREATE TABLE raw_ne_products (商品コード TEXT PRIMARY KEY, 商品名 TEXT, 原価 REAL)`);
db.exec(`CREATE TABLE sku_map (seller_sku TEXT, ne_code TEXT, 数量 INTEGER, PRIMARY KEY(seller_sku, ne_code))`);
db.exec(`CREATE TABLE m_sku_master (seller_sku TEXT PRIMARY KEY, 商品名 TEXT NOT NULL,
  created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  created_by TEXT, updated_by TEXT)`);
db.exec(`CREATE TABLE m_sku_components (
  seller_sku TEXT NOT NULL, ne_code TEXT NOT NULL, 数量 INTEGER NOT NULL DEFAULT 1, sort_order INTEGER NOT NULL DEFAULT 0,
  created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  PRIMARY KEY (seller_sku, ne_code),
  FOREIGN KEY (seller_sku) REFERENCES m_sku_master(seller_sku) ON DELETE CASCADE)`);
db.exec(`CREATE VIEW v_sku_resolved AS
  SELECT c.seller_sku, c.ne_code, c.数量, 'master' AS source FROM m_sku_components c
  UNION ALL
  SELECT s.seller_sku, s.ne_code, s.数量, 'auto' AS source FROM sku_map s
  WHERE NOT EXISTS (SELECT 1 FROM m_sku_master m WHERE m.seller_sku = s.seller_sku)
`);
db.exec(`CREATE VIEW v_sku_costed AS
  SELECT v.seller_sku, v.ne_code, v.数量, v.source, p.原価 AS 単価,
    CASE WHEN p.商品コード IS NULL THEN 'ne_missing' WHEN p.原価 IS NULL THEN 'cost_missing' ELSE 'ok' END AS cost_status
  FROM v_sku_resolved v LEFT JOIN raw_ne_products p ON v.ne_code = p.商品コード`);

// シード
db.prepare('INSERT INTO raw_ne_products (商品コード, 商品名, 原価) VALUES (?, ?, ?)').run('ne-100', 'NE単品100', 100);
db.prepare('INSERT INTO raw_ne_products (商品コード, 商品名, 原価) VALUES (?, ?, ?)').run('ne-200', 'NEセット部品A', 200);
db.prepare('INSERT INTO raw_ne_products (商品コード, 商品名, 原価) VALUES (?, ?, ?)').run('ne-300', 'NEセット部品B', 300);
db.prepare('INSERT INTO raw_ne_products (商品コード, 商品名, 原価) VALUES (?, ?, ?)').run('ne-zero', 'タダ商品', 0);
db.prepare('INSERT INTO raw_ne_products (商品コード, 商品名, 原価) VALUES (?, ?, ?)').run('ne-nullcost', '原価未登録', null);

// SKU 1: 単品 → ne-100
db.prepare('INSERT INTO m_sku_master (seller_sku, 商品名) VALUES (?, ?)').run('sku-single', '社内: 単品テスト');
db.prepare('INSERT INTO m_sku_components (seller_sku, ne_code, 数量) VALUES (?, ?, ?)').run('sku-single', 'ne-100', 1);

// SKU 2: セット → ne-200×1, ne-300×2
db.prepare('INSERT INTO m_sku_master (seller_sku, 商品名) VALUES (?, ?)').run('sku-set', '社内: セットテスト');
db.prepare('INSERT INTO m_sku_components (seller_sku, ne_code, 数量, sort_order) VALUES (?, ?, ?, ?)').run('sku-set', 'ne-200', 1, 0);
db.prepare('INSERT INTO m_sku_components (seller_sku, ne_code, 数量, sort_order) VALUES (?, ?, ?, ?)').run('sku-set', 'ne-300', 2, 1);

// SKU 3: 0円商品（タダ）→ ne-zero
db.prepare('INSERT INTO m_sku_master (seller_sku, 商品名) VALUES (?, ?)').run('sku-zero', '社内: タダ商品テスト');
db.prepare('INSERT INTO m_sku_components (seller_sku, ne_code, 数量) VALUES (?, ?, ?)').run('sku-zero', 'ne-zero', 1);

// SKU 4: NE側原価NULL → ne-nullcost
db.prepare('INSERT INTO m_sku_master (seller_sku, 商品名) VALUES (?, ?)').run('sku-nullcost', '社内: 原価未登録テスト');
db.prepare('INSERT INTO m_sku_components (seller_sku, ne_code, 数量) VALUES (?, ?, ?)').run('sku-nullcost', 'ne-nullcost', 1);

// sku_map fallback 用
db.prepare('INSERT INTO sku_map (seller_sku, ne_code, 数量) VALUES (?, ?, ?)').run('sku-auto-only', 'ne-100', 1);

// テスト
let pass = 0, fail = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (ok) { console.log(`  ✓ ${label}`); pass++; }
  else {
    console.log(`  ✗ ${label}\n    expect: ${JSON.stringify(expected)}\n    actual: ${JSON.stringify(actual)}`);
    fail++;
  }
}

console.log('\n[resolveSkuToComponents]');
const c1 = resolveSkuToComponents(db, 'sku-single');
check('単品: 1構成', c1.length, 1);
check('単品: ne_code', c1[0].ne_code, 'ne-100');
check('単品: source=master', c1[0].source, 'master');

const c2 = resolveSkuToComponents(db, 'sku-set');
check('セット: 2構成', c2.length, 2);

const c3 = resolveSkuToComponents(db, 'sku-auto-only');
check('fallback: 1構成', c3.length, 1);
check('fallback: source=auto', c3[0].source, 'auto');

check('未登録SKU: 空配列', resolveSkuToComponents(db, 'unknown'), []);
check('null引数: 空配列', resolveSkuToComponents(db, null), []);

// 大文字入力 → 内部で normalize
const cUpper = resolveSkuToComponents(db, ' SKU-SINGLE ');
check('大文字+空白: 正規化されてヒット', cUpper.length, 1);

console.log('\n[resolveCost]');
check('原価100', resolveCost(db, 'ne-100'), { cost: 100, source: 'ne' });
check('原価0（タダ商品）も有効', resolveCost(db, 'ne-zero'), { cost: 0, source: 'ne' });
check('原価NULLは未解決', resolveCost(db, 'ne-nullcost'), { cost: null, source: 'unresolved' });
check('NEなしも未解決', resolveCost(db, 'ne-not-exist'), { cost: null, source: 'unresolved' });
check('null引数も未解決', resolveCost(db, null), { cost: null, source: 'unresolved' });

console.log('\n[calcOrderCost]');
const r1 = calcOrderCost(db, 'sku-single', 3);
check('単品×3: total=300', r1.totalCost, 300);
check('単品×3: status=ok', r1.status, 'ok');
check('単品×3: breakdown数', r1.breakdown.length, 1);

// セット: ne-200×1×受注3 = 200×3 + ne-300×2×受注3 = 600×3 = 600 + 1800 = 2400
const r2 = calcOrderCost(db, 'sku-set', 3);
check('セット×3: total=2400', r2.totalCost, 2400);
check('セット×3: breakdown 2件', r2.breakdown.length, 2);

// タダ商品（0円）: 0 が正しく合計されること
const r3 = calcOrderCost(db, 'sku-zero', 5);
check('タダ商品×5: total=0', r3.totalCost, 0);
check('タダ商品×5: status=ok', r3.status, 'ok');

// 原価NULL: total=null
const r4 = calcOrderCost(db, 'sku-nullcost', 1);
check('原価NULL: total=null', r4.totalCost, null);
check('原価NULL: status=cost_missing', r4.status, 'cost_missing');

// 未登録SKU
const r5 = calcOrderCost(db, 'totally-unknown', 1);
check('未登録SKU: status=sku_unmapped', r5.status, 'sku_unmapped');
check('未登録SKU: total=null', r5.totalCost, null);

console.log('\n[getSkuMasterDetail]');
const d1 = getSkuMasterDetail(db, 'sku-set');
check('exists=true', d1.exists, true);
check('master.商品名', d1.master.商品名, '社内: セットテスト');
check('components 2件', d1.components.length, 2);
check('NE側商品名表示', d1.components[0].ne_title, 'NEセット部品A');

const d2 = getSkuMasterDetail(db, 'unknown');
check('未登録: exists=false', d2.exists, false);

db.close();
fs.unlinkSync(TMP_DB);
const wal = TMP_DB + '-wal'; if (fs.existsSync(wal)) fs.unlinkSync(wal);
const shm = TMP_DB + '-shm'; if (fs.existsSync(shm)) fs.unlinkSync(shm);

console.log(`\n結果: ${pass} pass / ${fail} fail`);
process.exit(fail > 0 ? 1 : 0);
