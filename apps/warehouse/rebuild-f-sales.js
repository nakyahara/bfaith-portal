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
  const t0 = Date.now();

  // ─── Phase 1+2: 入力読み取り（単一 read transaction で snapshot 固定） ───
  // better-sqlite3 の db.transaction() は SQLite の deferred transaction。
  // BEGIN DEFERRED 自体では snapshot は確定せず、WAL モードでは「本 transaction
  // 内で最初に実行された SELECT が開始した時点」で snapshot が確定する。
  // 確定後は同 transaction 内の後続 SELECT すべてが同じ snapshot を見る → COMMIT で解放。
  // これにより マスタ / 受注 / listing-用集計 のすべてが同一スナップショットに固定される。
  // 別プロセス（WarehouseServer）の書き込みはトランザクション中に進行可能だが、
  // ここで読む結果には反映されない（SQLite のスナップショット分離）。

  // SKU解決ソース選択:
  //   既定: v_sku_resolved (m_sku_master 優先 + sku_map fallback)
  //   WAREHOUSE_REBUILD_F_SALES_USE_LEGACY=1: 旧パス (sku_map 直参照)
  //   並行検証 (validate-sku-migration.js) で退化0/改善+3を確認後、新パスを既定にした。
  //   何かトラブルが出たら escape hatch として env で旧パスに戻せる。
  const useLegacy = process.env.WAREHOUSE_REBUILD_F_SALES_USE_LEGACY === '1';
  const skuResolutionSource = useLegacy ? 'sku_map (legacy)' : 'v_sku_resolved (master+fallback)';
  console.log(`[f_sales] SKU解決ソース: ${skuResolutionSource}`);

  let setComponentsRows, skuMapRows, productMpRows;
  let amazonRows, rakutenRows, neRows;
  let amazonListingRows, rakutenListingRows, neListingRows;

  const readTx = db.transaction(() => {
    // ─── マスタ系 ───
    setComponentsRows = db.prepare('SELECT * FROM m_set_components').all();
    skuMapRows = useLegacy
      ? db.prepare('SELECT seller_sku, ne_code, 数量 FROM sku_map').all()
      // v_sku_resolved は seller_sku + ne_code + 数量 (+ source) を返す。shape は sku_map と互換。
      : db.prepare('SELECT seller_sku, ne_code, 数量 FROM v_sku_resolved').all();
    productMpRows = db.prepare("SELECT 商品コード, 商品区分 FROM m_products").all();

    // ─── product 用 SELECT（粒度: 日付 × SKU/商品コード） ───

    // 1. Amazon → sku_mapでNE商品コードに変換 → セット展開
    amazonRows = db.prepare(`
      SELECT SUBSTR(purchase_date, 1, 10) as date, LOWER(seller_sku) as sku, MAX(title) as title,
             SUM(quantity) as qty, SUM(item_price) as amount
      FROM raw_sp_orders
      WHERE order_status NOT IN ('Cancelled')
      GROUP BY date, sku
    `).all();

    // 2. 楽天 → item_number ≒ NE商品コード
    rakutenRows = db.prepare(`
      SELECT SUBSTR(order_date, 1, 10) as date, LOWER(item_number) as item, MAX(item_name) as title,
             SUM(units) as qty,
             SUM(price_tax_incl * units) as amount
      FROM raw_rakuten_orders
      WHERE delete_item_flag = 0 AND order_status != 900
      GROUP BY date, item
    `).all();

    // 3. NE受注（Yahoo/auPAY等）→ 既にセット展開済み
    neRows = db.prepare(`
      SELECT SUBSTR(o.受注日, 1, 10) as date, LOWER(o.商品コード) as code, s.platform as mall,
             MAX(o.商品名) as title, SUM(o.受注数) as qty,
             SUM(o.小計金額) as amount
      FROM raw_ne_orders o
      INNER JOIN shops s ON o.店舗コード = s.shop_code
      WHERE o.キャンセル区分 = '有効'
        AND s.platform IS NOT NULL
        AND s.platform NOT IN ('_ignore', 'amazon_fbm', 'rakuten')
      GROUP BY date, code, mall
    `).all();

    // ─── listing 用 SELECT（粒度: 日付 × モール × 商品コード × チャネル） ───
    // product 用とは集計粒度が違うので別クエリ。Phase 3 では純粋な INSERT ループにする。

    amazonListingRows = db.prepare(`
      SELECT
        SUBSTR(purchase_date, 1, 10) as date,
        SUBSTR(purchase_date, 1, 7) as month,
        LOWER(seller_sku) as item_code,
        CASE WHEN fulfillment_channel = 'Amazon' THEN 'FBA' ELSE 'FBM' END as channel,
        MAX(title) as title,
        SUM(quantity) as qty,
        SUM(item_price) as amount,
        COUNT(DISTINCT amazon_order_id) as order_count
      FROM raw_sp_orders
      WHERE order_status NOT IN ('Cancelled')
      GROUP BY date, item_code, channel
    `).all();

    rakutenListingRows = db.prepare(`
      SELECT
        SUBSTR(order_date, 1, 10) as date,
        SUBSTR(order_date, 1, 7) as month,
        LOWER(item_number) as item_code,
        MAX(item_name) as title,
        SUM(units) as qty,
        SUM(price_tax_incl * units) as amount,
        COUNT(DISTINCT order_number) as order_count
      FROM raw_rakuten_orders
      WHERE delete_item_flag = 0 AND order_status != 900
      GROUP BY date, item_code
    `).all();

    neListingRows = db.prepare(`
      SELECT
        SUBSTR(o.受注日, 1, 10) as date,
        SUBSTR(o.受注日, 1, 7) as month,
        s.platform as mall,
        LOWER(o.商品コード) as item_code,
        MAX(o.商品名) as title,
        SUM(o.受注数) as qty,
        SUM(o.小計金額) as amount,
        COUNT(DISTINCT o.伝票番号) as order_count
      FROM raw_ne_orders o
      INNER JOIN shops s ON o.店舗コード = s.shop_code
      WHERE o.キャンセル区分 = '有効'
        AND s.platform IS NOT NULL
        AND s.platform NOT IN ('_ignore', 'amazon_fbm', 'rakuten')
      GROUP BY date, mall, item_code
    `).all();
  });
  readTx();

  const t1 = Date.now();
  console.log(`[f_sales] Phase 1+2 (read transaction): ${((t1 - t0) / 1000).toFixed(1)}秒`);

  // ─── マスタ Map 構築（読み取り済み配列から） ───

  // セット構成品マップ（セット商品コード → [{構成商品コード, 数量}]）
  const setComponents = new Map();
  for (const row of setComponentsRows) {
    const key = row.セット商品コード;
    if (!setComponents.has(key)) setComponents.set(key, []);
    setComponents.get(key).push({ code: row.構成商品コード, qty: row.数量 });
  }

  // SKU解決マップ（seller_sku → [{ne_code, 数量}]）
  // ソース: v_sku_resolved (新, 既定) または sku_map (旧, env で切替時)
  const skuMap = new Map();
  for (const row of skuMapRows) {
    const key = row.seller_sku?.toLowerCase();
    if (!key) continue;
    if (!skuMap.has(key)) skuMap.set(key, []);
    skuMap.get(key).push({ ne_code: row.ne_code, qty: row.数量 || 1 });
  }

  // m_products のセット判定用 + 商品コード存在チェック用
  const productTypes = new Map();
  for (const row of productMpRows) {
    productTypes.set(row.商品コード, row.商品区分);
  }

  // Amazon SKU → NE商品コード変換ヘルパー
  // 1. SKU解決マップ (v_sku_resolved or sku_map) → 2. m_products.商品コードと直接一致 → 3. unmapped
  function resolveAmazonSku(sku) {
    const mapped = skuMap.get(sku);
    if (mapped) return { mappings: mapped, source: useLegacy ? 'sku_map' : 'resolved' };
    if (productTypes.has(sku)) return { mappings: [{ ne_code: sku, qty: 1 }], source: 'direct' };
    return null;
  }

  // メモリ上に集計マップを構築: key = `日付|商品コード|モール`
  const productSales = new Map();
  // unmapped行も全部メモリに溜めて、Phase 3 でまとめて書き込む
  const unmappedRows = [];

  function addProductSale(date, neCode, mall, productName, qty, amount, isDirect) {
    const key = `${date}|${neCode}|${mall}`;
    if (!productSales.has(key)) {
      productSales.set(key, { date, neCode, mall, productName, qty: 0, direct: 0, setQty: 0, amount: 0 });
    }
    const entry = productSales.get(key);
    entry.qty += qty;
    if (amount != null && Number.isFinite(amount)) entry.amount += amount;
    if (isDirect) entry.direct += qty;
    else entry.setQty += qty;
  }

  // セット展開ヘルパー
  // saleAmount はセット販売の合計金額。セット → 構成品展開時は構成数比で按分。
  function expandToProducts(date, neCode, mall, productName, saleQty, saleAmount) {
    const type = productTypes.get(neCode);
    const comps = setComponents.get(neCode);

    if (type === 'セット' && comps && comps.length > 0) {
      // セット → 構成品に展開、金額は構成数 (qty) 比で按分
      const totalCompQty = comps.reduce((s, c) => s + (c.qty || 1), 0);
      for (const comp of comps) {
        const compQty = saleQty * comp.qty;
        const compAmount = (saleAmount != null && totalCompQty > 0)
          ? saleAmount * (comp.qty / totalCompQty)
          : null;
        addProductSale(date, comp.code, mall, '', compQty, compAmount, false);
      }
    } else {
      // 単品 or 例外 → そのまま
      addProductSale(date, neCode, mall, productName, saleQty, saleAmount, true);
    }
  }

  let directMatchCount = 0;
  for (const row of amazonRows) {
    const resolved = resolveAmazonSku(row.sku);
    if (!resolved) {
      unmappedRows.push({
        date: row.date, mall: 'amazon', sku: row.sku, title: row.title,
        qty: row.qty, amount: row.amount,
        reason: useLegacy ? 'sku_map未登録・商品コード不一致' : 'SKU未解決 (master/sku_map両方になし) ・商品コード不一致',
      });
      unmappedCount++;
      continue;
    }
    if (resolved.source === 'direct') directMatchCount++;
    // resolved.mappings 複数件 (1 SKU = N components) のときは元金額を mapping 数量比で按分
    const totalMapQty = resolved.mappings.reduce((s, m) => s + (m.qty || 1), 0);
    for (const m of resolved.mappings) {
      const expandQty = row.qty * m.qty;
      const expandAmount = (row.amount != null && totalMapQty > 0)
        ? row.amount * (m.qty / totalMapQty)
        : null;
      expandToProducts(row.date, m.ne_code, 'amazon', row.title, expandQty, expandAmount);
    }
  }
  log.push(`Amazon直接マッチ（sku_map不要）: ${directMatchCount}件`);

  for (const row of rakutenRows) {
    expandToProducts(row.date, row.item, 'rakuten', row.title, row.qty, row.amount);
  }

  for (const row of neRows) {
    if (!row.mall || row.mall === '_ignore') continue;
    // NE は既にセット展開済みなので直接 addProductSale
    addProductSale(row.date, row.code, row.mall, row.title, row.qty, row.amount, true);
  }

  const t2 = Date.now();
  console.log(`[f_sales] JS集計: ${((t2 - t1) / 1000).toFixed(1)}秒, productSales=${productSales.size}件, unmapped=${unmappedCount}件`);

  // ─── Phase 3: 全書き込みを単一トランザクションで原子的に実行 ───
  // ここで失敗・kill されても本テーブルは前日値のまま残る（中途半端な状態を晒さない）

  console.log('[f_sales] Phase 3: DB書き込み中（原子的）...');

  const insertListing = db.prepare(`
    INSERT INTO f_sales_by_listing (日付, 月, モール, モール商品コード, チャネル, 商品名, 数量, 売上金額, 注文数, データソース, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  const insertProduct = db.prepare(`
    INSERT INTO f_sales_by_product (日付, 商品コード, モール, 商品名, 数量, 直接販売数, セット経由数, 売上金額, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  const insertUnmapped = db.prepare(`
    INSERT INTO unmapped_sales (日付, モール, モール商品コード, 商品名, 数量, 売上金額, 失敗理由, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const rebuildTx = db.transaction(() => {
    db.exec('DELETE FROM f_sales_by_listing');
    db.exec('DELETE FROM f_sales_by_product');
    db.exec('DELETE FROM unmapped_sales');

    for (const r of amazonListingRows) {
      insertListing.run(r.date, r.month, 'amazon', r.item_code, r.channel, r.title, r.qty, r.amount, r.order_count, 'sp_api', ts);
    }
    for (const r of rakutenListingRows) {
      insertListing.run(r.date, r.month, 'rakuten', r.item_code, '', r.title, r.qty, r.amount, r.order_count, 'rakuten_api', ts);
    }
    for (const r of neListingRows) {
      insertListing.run(r.date, r.month, r.mall, r.item_code, '', r.title, r.qty, r.amount, r.order_count, 'ne', ts);
    }

    for (const u of unmappedRows) {
      insertUnmapped.run(u.date, u.mall, u.sku, u.title, u.qty, u.amount, u.reason, ts);
    }

    for (const [, entry] of productSales) {
      // 売上金額: 集計値が >0 なら値、=0 なら NULL (情報無し vs 確定ゼロ を区別)
      const amountVal = entry.amount > 0 ? Math.round(entry.amount) : null;
      insertProduct.run(
        entry.date, entry.neCode, entry.mall, entry.productName,
        entry.qty, entry.direct, entry.setQty, amountVal, ts
      );
    }
  });
  rebuildTx();

  // WAL肥大化防止: 大量 INSERT 後に明示的に checkpoint(TRUNCATE) で WAL ファイルを切り詰める。
  // auto-checkpoint は PASSIVE モードなので WAL ファイル自体は縮まないため、明示が必要。
  try { db.pragma('wal_checkpoint(TRUNCATE)'); } catch {}

  const t3 = Date.now();
  console.log(`[f_sales] Phase 3 (DB書き込み): ${((t3 - t2) / 1000).toFixed(1)}秒`);

  log.push(`Amazon listing: ${amazonListingRows.length}行`);
  log.push(`楽天 listing: ${rakutenListingRows.length}行`);
  log.push(`NE listing: ${neListingRows.length}行`);

  // ─── 結果サマリ + 整合性チェック（読み取り専用） ───

  const totalListing = db.prepare('SELECT COUNT(*) as cnt FROM f_sales_by_listing').get().cnt;
  log.push(`f_sales_by_listing 合計: ${totalListing}行`);

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

  console.log(`[f_sales] ✅ 完了 (${((t3 - t0) / 1000).toFixed(1)}秒): listing=${totalListing}行, product=${totalProduct}行, unmapped=${unmappedCount}件`);

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
