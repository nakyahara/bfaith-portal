/**
 * fix-us-fba-skus.cjs — US FBA カートン SKU 緊急マスタ追加
 *
 * 背景:
 *   2026-05-05 の SKU 管理統合 Step 1〜6 で sku_map から m_sku_components への移行時、
 *   m_sku_master 未登録の SKU が FK 失敗で機械的に弾かれた。
 *   結果として US FBA の cardstand-* シリーズ 5 SKU (実在庫あり) が全部 unresolved に
 *   なり、月末棚卸し画面で「米国FBA倉庫=¥0」と誤表示が 14 日続いた。
 *
 * このスクリプト:
 *   1) m_sku_master + m_sku_components に 5 SKU を seed
 *   2) Codex Round 6 で確定した E (US FBA 5 SKU 遡及補正) の master 部分のみ実施
 *   3) 在庫集計の rebuild は別途 snapshot-inventory-aggregate.js を呼ぶ
 *
 * 実行:
 *   node fix-us-fba-skus.cjs            # dry-run (デフォルト、何も書かない)
 *   node fix-us-fba-skus.cjs --apply    # 実際に INSERT する
 *
 * 設計上のガード:
 *   - 既存 seller_sku があれば エラーで停止 (上書きしない)
 *   - 親 ne_code が m_products に居なければ エラーで停止
 *   - 全て transaction、途中失敗で rollback
 */
const path = require('path');
const Database = require('better-sqlite3');

const DB_PATH = process.env.DB_PATH || path.join(process.cwd(), 'data', 'warehouse.db');
const APPLY = process.argv.includes('--apply');
const CREATED_BY = 'fix/inv-monthly-fix-20260519';

// 5 SKU 定義: 旧 sku_map の (seller_sku, ne_code, 数量) と一致
// 商品名は親 ne_code の m_products.商品名 + 「[US FBA NN本入りカートン]」suffix
const SKUS_TO_INSERT = [
  { seller_sku: 'cardstand-r-40',        ne_code: 'cardstand-r',        数量: 40, name_suffix: '[US FBA 40本入りカートン]' },
  { seller_sku: 'cardstand-silver-r-40', ne_code: 'cardstand-silver-r', 数量: 40, name_suffix: '[US FBA 40本入りカートン]' },
  { seller_sku: 'cardstand-silver-w-10', ne_code: 'cardstand-silver-w', 数量: 10, name_suffix: '[US FBA 10本入りカートン]' },
  { seller_sku: 'cardstand-silver-w-20', ne_code: 'cardstand-silver-w', 数量: 20, name_suffix: '[US FBA 20本入りカートン]' },
  { seller_sku: 'cardstand-w-20',        ne_code: 'cardstand-w',        数量: 20, name_suffix: '[US FBA 20本入りカートン]' },
];

const db = new Database(DB_PATH);

console.log(`DB: ${DB_PATH}`);
console.log(`Mode: ${APPLY ? 'APPLY (writes)' : 'DRY-RUN (no writes)'}`);
console.log(`Created by: ${CREATED_BY}`);
console.log('');

// 1) 事前検証
const conflicts = [];
const missingParents = [];
const plans = [];

for (const sku of SKUS_TO_INSERT) {
  const existing = db.prepare('SELECT seller_sku FROM m_sku_master WHERE seller_sku = ?').get(sku.seller_sku);
  if (existing) {
    conflicts.push(`m_sku_master already has '${sku.seller_sku}' — refusing to overwrite`);
    continue;
  }
  const parent = db.prepare('SELECT 商品コード, 商品名, 原価, 原価状態 FROM m_products WHERE 商品コード = ?').get(sku.ne_code);
  if (!parent) {
    missingParents.push(`parent ne_code '${sku.ne_code}' not in m_products — cannot derive 商品名`);
    continue;
  }
  if (parent.原価状態 !== 'COMPLETE' && parent.原価状態 !== 'OVERRIDDEN') {
    console.warn(`WARN: parent ${sku.ne_code} has 原価状態=${parent.原価状態} (not COMPLETE/OVERRIDDEN). Aggregator will treat cost as MISSING.`);
  }
  plans.push({
    seller_sku: sku.seller_sku,
    商品名: `${parent.商品名} ${sku.name_suffix}`,
    ne_code: sku.ne_code,
    数量: sku.数量,
    parent_原価: parent.原価,
    parent_原価状態: parent.原価状態,
  });
}

if (conflicts.length > 0 || missingParents.length > 0) {
  console.error('=== ERRORS ===');
  for (const e of conflicts) console.error('  ' + e);
  for (const e of missingParents) console.error('  ' + e);
  process.exit(1);
}

console.log('=== Planned INSERTs ===');
for (const p of plans) {
  console.log(`  m_sku_master: seller_sku='${p.seller_sku}', 商品名='${p.商品名}'`);
  console.log(`    m_sku_components: ne_code='${p.ne_code}', 数量=${p.数量}, sort_order=0`);
  console.log(`    (parent: 原価=¥${p.parent_原価}, 状態=${p.parent_原価状態})`);
}

if (!APPLY) {
  console.log('\nDRY-RUN complete. Re-run with --apply to write.');
  process.exit(0);
}

// 2) APPLY
console.log('\n=== APPLYING ===');
db.pragma('foreign_keys = ON');

const insMaster = db.prepare(`
  INSERT INTO m_sku_master (seller_sku, 商品名, created_by)
  VALUES (?, ?, ?)
`);
const insComp = db.prepare(`
  INSERT INTO m_sku_components (seller_sku, ne_code, 数量, sort_order)
  VALUES (?, ?, ?, 0)
`);

const txn = db.transaction(() => {
  for (const p of plans) {
    insMaster.run(p.seller_sku, p.商品名, CREATED_BY);
    insComp.run(p.seller_sku, p.ne_code, p.数量);
    console.log(`  inserted ${p.seller_sku}`);
  }
});
try {
  txn();
  console.log('\nAll 5 SKUs inserted successfully (master + components).');
} catch (e) {
  console.error('\nTransaction failed (rolled back):', e.message);
  process.exit(2);
}

// 3) 確認
console.log('\n=== Verification ===');
for (const sku of SKUS_TO_INSERT) {
  const m = db.prepare('SELECT seller_sku, 商品名, created_by FROM m_sku_master WHERE seller_sku = ?').get(sku.seller_sku);
  const c = db.prepare('SELECT seller_sku, ne_code, 数量, sort_order FROM m_sku_components WHERE seller_sku = ?').all(sku.seller_sku);
  console.log(`  ${sku.seller_sku}: master=${m ? 'OK' : 'MISSING'}, components=${c.length}`);
}

console.log('\nNext step: run snapshot-inventory-aggregate.js for each US snapshot date');
console.log('  for d in 2026-05-05 2026-05-08 2026-05-09 2026-05-10 2026-05-15 2026-05-18 2026-05-19; do');
console.log('    node apps/warehouse/snapshot-inventory-aggregate.js --date=$d');
console.log('  done');
