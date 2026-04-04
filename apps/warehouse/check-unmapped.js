import Database from 'better-sqlite3';
const db = new Database('data/warehouse.db', { readonly: true });

// 1. unmapped_salesのユニークSKU数
const uniqueSkus = db.prepare('SELECT COUNT(DISTINCT モール商品コード) as cnt FROM unmapped_sales').get();
console.log('unmapped_sales ユニークSKU数:', uniqueSkus.cnt);

// 2. そのうちNE商品マスタに存在するもの（商品コード一致）
const inNE = db.prepare(`
  SELECT COUNT(DISTINCT u.モール商品コード) as cnt
  FROM unmapped_sales u
  INNER JOIN raw_ne_products p ON u.モール商品コード = p.商品コード COLLATE NOCASE
`).get();
console.log('うちNE商品マスタに存在:', inNE.cnt);

// 3. セット商品コードに一致するもの
const inSet = db.prepare(`
  SELECT COUNT(DISTINCT u.モール商品コード) as cnt
  FROM unmapped_sales u
  INNER JOIN (SELECT DISTINCT セット商品コード FROM raw_ne_set_products) sp
    ON u.モール商品コード = sp.セット商品コード COLLATE NOCASE
`).get();
console.log('うちセット商品コードに一致:', inSet.cnt);

// 4. FBA vs FBM（raw_sp_ordersのfulfillment_channel）
const byChannel = db.prepare(`
  SELECT o.fulfillment_channel, COUNT(DISTINCT o.seller_sku) as sku_count, SUM(o.quantity) as total_qty
  FROM raw_sp_orders o
  LEFT JOIN sku_map sm ON o.seller_sku = sm.seller_sku COLLATE NOCASE
  WHERE sm.seller_sku IS NULL AND o.order_status NOT IN ('Cancelled')
  GROUP BY o.fulfillment_channel
`).all();
console.log('\n未マップSKU チャネル別:');
for (const r of byChannel) console.log(' ', r.fulfillment_channel || '不明', ': SKU', r.sku_count, '件, 数量', r.total_qty);

// 5. 未マップSKUのサンプル（NE商品コードと一致するもの）
const matchSamples = db.prepare(`
  SELECT DISTINCT u.モール商品コード as sku, p.商品コード as ne_code, p.商品名
  FROM unmapped_sales u
  INNER JOIN raw_ne_products p ON u.モール商品コード = p.商品コード COLLATE NOCASE
  LIMIT 10
`).all();
console.log('\n未マップだがNE商品コードと一致するもの:');
for (const r of matchSamples) console.log(' ', r.sku, '=', r.ne_code, (r.商品名||'').slice(0,40));

// 6. 未マップSKUのサンプル（セット商品コードと一致するもの）
const setMatchSamples = db.prepare(`
  SELECT DISTINCT u.モール商品コード as sku, sp.セット商品名
  FROM unmapped_sales u
  INNER JOIN (SELECT DISTINCT セット商品コード, MAX(セット商品名) as セット商品名 FROM raw_ne_set_products GROUP BY セット商品コード) sp
    ON u.モール商品コード = sp.セット商品コード COLLATE NOCASE
  LIMIT 10
`).all();
console.log('\n未マップだがセット商品コードと一致するもの:');
for (const r of setMatchSamples) console.log(' ', r.sku, (r.セット商品名||'').slice(0,50));

// 7. 未マップでNEにもセットにも無いもの（本当の未登録）
const trueUnmapped = db.prepare(`
  SELECT COUNT(DISTINCT u.モール商品コード) as cnt
  FROM unmapped_sales u
  LEFT JOIN raw_ne_products p ON u.モール商品コード = p.商品コード COLLATE NOCASE
  LEFT JOIN (SELECT DISTINCT セット商品コード FROM raw_ne_set_products) sp ON u.モール商品コード = sp.セット商品コード COLLATE NOCASE
  WHERE p.商品コード IS NULL AND sp.セット商品コード IS NULL
`).get();
console.log('\n本当の未登録（NE/セットどちらにも無い）:', trueUnmapped.cnt);

// 8. 本当の未登録サンプル
const trueUnmappedSamples = db.prepare(`
  SELECT DISTINCT u.モール商品コード as sku, u.商品名, SUM(u.数量) as total_qty
  FROM unmapped_sales u
  LEFT JOIN raw_ne_products p ON u.モール商品コード = p.商品コード COLLATE NOCASE
  LEFT JOIN (SELECT DISTINCT セット商品コード FROM raw_ne_set_products) sp ON u.モール商品コード = sp.セット商品コード COLLATE NOCASE
  WHERE p.商品コード IS NULL AND sp.セット商品コード IS NULL
  GROUP BY u.モール商品コード
  ORDER BY total_qty DESC
  LIMIT 15
`).all();
console.log('\n本当の未登録サンプル（数量多い順）:');
for (const r of trueUnmappedSamples) console.log(' ', r.sku, '数量:', r.total_qty, (r.商品名||'').slice(0,40));

db.close();
