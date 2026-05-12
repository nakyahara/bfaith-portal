/**
 * list-aupay-unresolved-skus.js — au PAY の item_code で m_products に直接マッチしないものを一覧化
 *
 * au PAY Phase 1 A-0 の補助スクリプト。f_aupay_sku_map に手動 entry すべき SKU の判断材料。
 * raw_aupay_orders の item_code を集計し、以下を出力:
 *   - m_products に CI マッチしない item_code
 *   - option_info に variant 情報がありそうか (= 「カラー=」「サイズ=」等を含む → unresolved 維持推奨)
 *   - 売上量・注文数 (entry の優先度)
 *   - 既に f_aupay_sku_map にあるか
 *
 * 使い方:
 *   DATA_DIR=... node apps/warehouse/list-aupay-unresolved-skus.js [--csv]
 *
 * 設計書: g:/共有ドライブ/AI_reference/システム設計/auPAYマーケットPhase1設計書_v0.4_20260512.md §5
 */
import path from 'node:path';
import fs from 'node:fs';
import Database from 'better-sqlite3';

const DATA_DIR = (process.env.DATA_DIR || '').trim();
const asCsv = process.argv.includes('--csv');
if (!DATA_DIR) { console.error('FATAL: DATA_DIR is required'); process.exit(2); }
const dbPath = path.join(DATA_DIR, 'warehouse.db');
if (!fs.existsSync(dbPath)) { console.error(`FATAL: warehouse.db not found at ${dbPath}`); process.exit(2); }

const db = new Database(dbPath, { readonly: true });

// f_aupay_sku_map が存在しなければ空扱い
let hasMapTable = false;
try { db.prepare('SELECT 1 FROM f_aupay_sku_map LIMIT 1').get(); hasMapTable = true; } catch {}

const rows = db.prepare(`
  WITH au_sku AS (
    SELECT
      item_code,
      MAX(item_name) AS item_name,
      COUNT(*) AS lines,
      COUNT(DISTINCT order_id) AS orders,
      SUM(unit) AS units,
      ROUND(SUM(total_item_price), 0) AS sales,
      -- option_info に variant 情報がありそうか (「=」を含み、配送同意系の決まり文句でない)
      MAX(CASE
        WHEN item_option IS NULL OR item_option = '' THEN 0
        WHEN item_option LIKE '%確認しました%' AND item_option NOT LIKE '%カラー=%' AND item_option NOT LIKE '%サイズ=%' THEN 0
        WHEN item_option LIKE '%=%' THEN 1
        ELSE 0
      END) AS has_variant_option,
      MAX(item_option) AS sample_option
    FROM raw_aupay_orders
    WHERE item_code IS NOT NULL AND item_code <> ''
    GROUP BY item_code
  )
  SELECT * FROM au_sku
  WHERE NOT EXISTS (SELECT 1 FROM m_products WHERE LOWER(TRIM(商品コード)) = LOWER(TRIM(au_sku.item_code)))
  ORDER BY units DESC
`).all();

// f_aupay_sku_map に既にあるものを除外
const mapped = new Set();
if (hasMapTable) {
  for (const r of db.prepare("SELECT LOWER(TRIM(aupay_key)) AS k FROM f_aupay_sku_map").all()) mapped.add(r.k);
}
const remaining = rows.filter(r => !mapped.has(String(r.item_code).toLowerCase().trim()));

const variantSuspect = remaining.filter(r => r.has_variant_option === 1);
const simpleSuspect = remaining.filter(r => r.has_variant_option === 0);

console.log(`=== au PAY: m_products に未マッチ item_code: ${rows.length} 件 (うち f_aupay_sku_map 既登録 ${rows.length - remaining.length} 件、残 ${remaining.length} 件) ===`);
console.log(`  variant 情報ありそう (unresolved 維持推奨): ${variantSuspect.length} 件 / 単純商品 (手動 entry 候補): ${simpleSuspect.length} 件\n`);

if (asCsv) {
  console.log('item_code,item_name,units,orders,sales,has_variant_option,sample_option');
  for (const r of remaining) {
    console.log([r.item_code, `"${(r.item_name||'').replace(/"/g,'""')}"`, r.units, r.orders, r.sales, r.has_variant_option, `"${(r.sample_option||'').replace(/"/g,'""')}"`].join(','));
  }
} else {
  console.log('--- 単純商品 (手動 entry 候補、top 30、売上量順) ---');
  console.table(simpleSuspect.slice(0, 30).map(r => ({ item_code: r.item_code, name: (r.item_name||'').slice(0,28), units: r.units, orders: r.orders, sales: r.sales })));
  console.log('\n--- variant 情報ありそう (unresolved 維持推奨、top 20) ---');
  console.table(variantSuspect.slice(0, 20).map(r => ({ item_code: r.item_code, name: (r.item_name||'').slice(0,24), units: r.units, sales: r.sales, sample_option: (r.sample_option||'').slice(0,30) })));
}

console.log('\n→ f_aupay_sku_map への手動 entry 例:');
console.log("  INSERT INTO f_aupay_sku_map (store_id, aupay_key, ne_code, resolution_source, notes) VALUES ('b-faith01', '<item_code>', '<ne_code>', 'manual', '<理由>');");
