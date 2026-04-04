/**
 * f_sales_by_listing / f_sales_by_product 再構築スクリプト
 *
 * 受注データから日次集計テーブルを再構築する。
 * daily-sync.js から呼び出す or 単体実行可能。
 *
 * 数値の意味:
 *   f_sales_by_listing.数量 = モール上の販売ページとしての販売数量（セット展開しない）
 *   f_sales_by_listing.売上金額 = モール実売金額（税込・送料別・ポイント値引き前・キャンセル除外）
 *   f_sales_by_product.数量 = 構成品換算後の数量 = 直接販売数 + セット経由数
 *   f_sales_by_product.直接販売数 = 単品ページ/SKUとして売れた数
 *   f_sales_by_product.セット経由数 = セット商品の構成品として売れた数
 */
import { getDB } from './db.js';

function now() {
  return new Date().toISOString().replace('T', ' ').slice(0, 19);
}

export async function rebuildFSales() {
  const db = getDB();
  const ts = now();
  const log = [];
  let unmappedCount = 0;

  console.log('[f_sales] 集計再構築開始...');

  // セット構成品マップ（セット商品コード → [{構成商品コード, 数量}]）
  const setComponents = new Map();
  for (const row of db.prepare('SELECT * FROM m_set_components').all()) {
    const key = row.セット商品コード;
    if (!setComponents.has(key)) setComponents.set(key, []);
    setComponents.get(key).push({ code: row.構成商品コード, qty: row.数量 });
  }

  // sku_map（seller_sku → [{ne_code, 数量}]）
  const skuMap = new Map();
  for (const row of db.prepare('SELECT * FROM sku_map').all()) {
    const key = row.seller_sku?.toLowerCase();
    if (!key) continue;
    if (!skuMap.has(key)) skuMap.set(key, []);
    skuMap.get(key).push({ ne_code: row.ne_code, qty: row.数量 || 1 });
  }

  // m_products のセット判定用 + 商品コード存在チェック用
  const productTypes = new Map();
  for (const row of db.prepare("SELECT 商品コード, 商品区分 FROM m_products").all()) {
    productTypes.set(row.商品コード, row.商品区分);
  }

  // Amazon SKU → NE商品コード変換ヘルパー
  // 1. sku_map → 2. m_products.商品コードと直接一致 → 3. unmapped
  function resolveAmazonSku(sku) {
    const mapped = skuMap.get(sku);
    if (mapped) return { mappings: mapped, source: 'sku_map' };
    if (productTypes.has(sku)) return { mappings: [{ ne_code: sku, qty: 1 }], source: 'direct' };
    return null;
  }

  // unmapped 退避用
  const insertUnmapped = db.prepare(`
    INSERT INTO unmapped_sales (日付, モール, モール商品コード, 商品名, 数量, 売上金額, 失敗理由, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `);

  // ─── f_sales_by_listing 再構築 ───

  console.log('[f_sales] f_sales_by_listing 投入中...');
  db.exec('DELETE FROM f_sales_by_listing');

  // 1. Amazon SP-API
  const amazonListingCount = db.prepare(`
    INSERT INTO f_sales_by_listing (日付, 月, モール, モール商品コード, チャネル, 商品名, 数量, 売上金額, 注文数, データソース, updated_at)
    SELECT
      SUBSTR(purchase_date, 1, 10) as 日付,
      SUBSTR(purchase_date, 1, 7) as 月,
      'amazon' as モール,
      LOWER(seller_sku) as モール商品コード,
      CASE WHEN fulfillment_channel = 'Amazon' THEN 'FBA' ELSE 'FBM' END as チャネル,
      MAX(title) as 商品名,
      SUM(quantity) as 数量,
      SUM(item_price) as 売上金額,
      COUNT(DISTINCT amazon_order_id) as 注文数,
      'sp_api' as データソース,
      ? as updated_at
    FROM raw_sp_orders
    WHERE order_status NOT IN ('Cancelled')
    GROUP BY 日付, モール, モール商品コード, チャネル
  `).run(ts);
  log.push(`Amazon listing: ${amazonListingCount.changes}行`);

  // 2. 楽天RMS
  const rakutenListingCount = db.prepare(`
    INSERT INTO f_sales_by_listing (日付, 月, モール, モール商品コード, チャネル, 商品名, 数量, 売上金額, 注文数, データソース, updated_at)
    SELECT
      SUBSTR(order_date, 1, 10) as 日付,
      SUBSTR(order_date, 1, 7) as 月,
      'rakuten' as モール,
      LOWER(item_number) as モール商品コード,
      '' as チャネル,
      MAX(item_name) as 商品名,
      SUM(units) as 数量,
      SUM(price_tax_incl * units) as 売上金額,
      COUNT(DISTINCT order_number) as 注文数,
      'rakuten_api' as データソース,
      ? as updated_at
    FROM raw_rakuten_orders
    WHERE delete_item_flag = 0 AND order_status != 900
    GROUP BY 日付, モール, モール商品コード
  `).run(ts);
  log.push(`楽天 listing: ${rakutenListingCount.changes}行`);

  // 3. NE受注（Yahoo/auPAY/メルカリ/LINEギフト等）
  const neListingCount = db.prepare(`
    INSERT INTO f_sales_by_listing (日付, 月, モール, モール商品コード, チャネル, 商品名, 数量, 売上金額, 注文数, データソース, updated_at)
    SELECT
      SUBSTR(o.受注日, 1, 10) as 日付,
      SUBSTR(o.受注日, 1, 7) as 月,
      s.platform as モール,
      LOWER(o.商品コード) as モール商品コード,
      '' as チャネル,
      MAX(o.商品名) as 商品名,
      SUM(o.受注数) as 数量,
      SUM(o.小計金額) as 売上金額,
      COUNT(DISTINCT o.伝票番号) as 注文数,
      'ne' as データソース,
      ? as updated_at
    FROM raw_ne_orders o
    INNER JOIN shops s ON o.店舗コード = s.shop_code
    WHERE o.キャンセル区分 = '有効'
      AND s.platform IS NOT NULL
      AND s.platform NOT IN ('_ignore', 'amazon_fbm', 'rakuten')
    GROUP BY 日付, s.platform, モール商品コード
  `).run(ts);
  log.push(`NE listing: ${neListingCount.changes}行`);

  const totalListing = db.prepare('SELECT COUNT(*) as cnt FROM f_sales_by_listing').get().cnt;
  log.push(`f_sales_by_listing 合計: ${totalListing}行`);

  // ─── f_sales_by_product 再構築 ───

  console.log('[f_sales] f_sales_by_product 投入中...');
  db.exec('DELETE FROM f_sales_by_product');
  // 既存の未マッピングデータも今回分でクリア
  db.exec('DELETE FROM unmapped_sales');

  // メモリ上に集計マップを構築: key = `日付|商品コード|モール`
  const productSales = new Map();

  function addProductSale(date, neCode, mall, productName, qty, isDirect) {
    const month = date.slice(0, 7);
    const key = `${date}|${neCode}|${mall}`;
    if (!productSales.has(key)) {
      productSales.set(key, { date, month, neCode, mall, productName, qty: 0, direct: 0, setQty: 0 });
    }
    const entry = productSales.get(key);
    entry.qty += qty;
    if (isDirect) entry.direct += qty;
    else entry.setQty += qty;
  }

  // セット展開ヘルパー
  function expandToProducts(date, neCode, mall, productName, saleQty) {
    const type = productTypes.get(neCode);
    const comps = setComponents.get(neCode);

    if (type === 'セット' && comps && comps.length > 0) {
      // セット → 構成品に展開
      for (const comp of comps) {
        addProductSale(date, comp.code, mall, '', saleQty * comp.qty, false);
      }
    } else {
      // 単品 or 例外 → そのまま
      addProductSale(date, neCode, mall, productName, saleQty, true);
    }
  }

  // 1. Amazon → sku_mapでNE商品コードに変換 → セット展開
  console.log('[f_sales]   Amazon → f_sales_by_product...');
  const amazonRows = db.prepare(`
    SELECT SUBSTR(purchase_date, 1, 10) as date, LOWER(seller_sku) as sku, MAX(title) as title,
           SUM(quantity) as qty, SUM(item_price) as amount
    FROM raw_sp_orders
    WHERE order_status NOT IN ('Cancelled')
    GROUP BY date, sku
  `).all();

  let directMatchCount = 0;
  for (const row of amazonRows) {
    const resolved = resolveAmazonSku(row.sku);
    if (!resolved) {
      insertUnmapped.run(row.date, 'amazon', row.sku, row.title, row.qty, row.amount, 'sku_map未登録・商品コード不一致', ts);
      unmappedCount++;
      continue;
    }
    if (resolved.source === 'direct') directMatchCount++;
    for (const m of resolved.mappings) {
      expandToProducts(row.date, m.ne_code, 'amazon', row.title, row.qty * m.qty);
    }
  }
  log.push(`Amazon直接マッチ（sku_map不要）: ${directMatchCount}件`);

  // 2. 楽天 → item_number ≒ NE商品コード → セット展開
  console.log('[f_sales]   楽天 → f_sales_by_product...');
  const rakutenRows = db.prepare(`
    SELECT SUBSTR(order_date, 1, 10) as date, LOWER(item_number) as item, MAX(item_name) as title,
           SUM(units) as qty
    FROM raw_rakuten_orders
    WHERE delete_item_flag = 0 AND order_status != 900
    GROUP BY date, item
  `).all();

  for (const row of rakutenRows) {
    expandToProducts(row.date, row.item, 'rakuten', row.title, row.qty);
  }

  // 3. NE受注（Yahoo/auPAY等）→ 既にセット展開済みなのでそのまま直接販売数扱い
  console.log('[f_sales]   NE受注 → f_sales_by_product...');
  const neRows = db.prepare(`
    SELECT SUBSTR(o.受注日, 1, 10) as date, LOWER(o.商品コード) as code, s.platform as mall,
           MAX(o.商品名) as title, SUM(o.受注数) as qty
    FROM raw_ne_orders o
    INNER JOIN shops s ON o.店舗コード = s.shop_code
    WHERE o.キャンセル区分 = '有効'
      AND s.platform IS NOT NULL
      AND s.platform NOT IN ('_ignore', 'amazon_fbm', 'rakuten')
    GROUP BY date, code, mall
  `).all();

  for (const row of neRows) {
    if (!row.mall || row.mall === '_ignore') continue;
    addProductSale(row.date, row.code, row.mall, row.title, row.qty, true);
  }

  // メモリ上の集計マップをDBに投入
  console.log('[f_sales]   f_sales_by_product 投入中...');
  const insertProduct = db.prepare(`
    INSERT INTO f_sales_by_product (日付, 商品コード, モール, 商品名, 数量, 直接販売数, セット経由数, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const insertTx = db.transaction(() => {
    for (const [, entry] of productSales) {
      insertProduct.run(
        entry.date, entry.neCode, entry.mall, entry.productName,
        entry.qty, entry.direct, entry.setQty, ts
      );
    }
  });
  insertTx();

  // WAL肥大化防止
  try { db.pragma('wal_checkpoint(TRUNCATE)'); } catch {}

  const totalProduct = db.prepare('SELECT COUNT(*) as cnt FROM f_sales_by_product').get().cnt;
  log.push(`f_sales_by_product 合計: ${totalProduct}行`);
  log.push(`未マッピング退避: ${unmappedCount}件`);

  // 数量整合チェック
  const qtyMismatch = db.prepare(`
    SELECT COUNT(*) as cnt FROM f_sales_by_product
    WHERE 数量 != 直接販売数 + セット経由数
  `).get().cnt;
  if (qtyMismatch > 0) {
    log.push(`⚠️ 数量整合エラー: ${qtyMismatch}件（数量 ≠ 直接販売数 + セット経由数）`);
  }

  console.log(`[f_sales] ✅ 完了: listing=${totalListing}行, product=${totalProduct}行, unmapped=${unmappedCount}件`);

  return { ok: true, log, listing: totalListing, product: totalProduct, unmapped: unmappedCount };
}

// ─── 単体実行 ───

import { initDB } from './db.js';

const isMain = !process.argv[1] || process.argv[1].includes('rebuild-f-sales');
if (isMain && process.argv[1]?.includes('rebuild-f-sales')) {
  await initDB();
  const result = await rebuildFSales();
  console.log('\n結果:', JSON.stringify(result, null, 2));
  process.exit(0);
}
