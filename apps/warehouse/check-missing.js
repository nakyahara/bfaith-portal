import Database from 'better-sqlite3';
const db = new Database('data/warehouse.db', { readonly: true });

// 1. セット商品で送料未登録（raw_ne_productsに存在するもの）
const setInProducts = db.prepare(`
  SELECT COUNT(DISTINCT sp.セット商品コード) as cnt
  FROM raw_ne_set_products sp
  INNER JOIN raw_ne_products p ON sp.セット商品コード = p.商品コード COLLATE NOCASE
  LEFT JOIN product_shipping ps ON sp.セット商品コード = ps.sku COLLATE NOCASE
  WHERE p.取扱区分 = '取扱中' AND ps.sku IS NULL
`).get();
console.log('1. セット商品(NE商品マスタにある)で送料未登録:', setInProducts.cnt);

// 2. セット商品コードがraw_ne_productsに存在しないもの
const setNotInProducts = db.prepare(`
  SELECT COUNT(DISTINCT sp.セット商品コード) as cnt
  FROM raw_ne_set_products sp
  LEFT JOIN raw_ne_products p ON sp.セット商品コード = p.商品コード COLLATE NOCASE
  WHERE p.商品コード IS NULL
`).get();
console.log('2. セット商品でNE商品マスタに無い:', setNotInProducts.cnt);

// 3. セット商品(NE商品マスタに無い)で送料未登録
const setOrphanNoShip = db.prepare(`
  SELECT COUNT(DISTINCT sp.セット商品コード) as cnt
  FROM raw_ne_set_products sp
  LEFT JOIN raw_ne_products p ON sp.セット商品コード = p.商品コード COLLATE NOCASE
  LEFT JOIN product_shipping ps ON sp.セット商品コード = ps.sku COLLATE NOCASE
  WHERE p.商品コード IS NULL AND ps.sku IS NULL
`).get();
console.log('3. セット商品(NE無し)で送料未登録:', setOrphanNoShip.cnt);

// 4. 例外原価でraw_ne_productsに無い
const genkaNotInProducts = db.prepare(`
  SELECT COUNT(*) as cnt
  FROM exception_genka eg
  LEFT JOIN raw_ne_products p ON eg.sku = p.商品コード COLLATE NOCASE
  WHERE p.商品コード IS NULL
`).get();
console.log('4. 例外原価でNE商品マスタに無い:', genkaNotInProducts.cnt);

// 5. 例外原価(NE商品マスタに無い)で送料未登録
const genkaNoShip = db.prepare(`
  SELECT COUNT(*) as cnt
  FROM exception_genka eg
  LEFT JOIN raw_ne_products p ON eg.sku = p.商品コード COLLATE NOCASE
  LEFT JOIN product_shipping ps ON eg.sku = ps.sku COLLATE NOCASE
  WHERE p.商品コード IS NULL AND ps.sku IS NULL
`).get();
console.log('5. 例外原価(NE無し)で送料未登録:', genkaNoShip.cnt);

// 6. サンプル表示
console.log('\n--- セット商品(NE無し)送料未登録サンプル ---');
const setSamples = db.prepare(`
  SELECT DISTINCT sp.セット商品コード, sp.セット商品名
  FROM raw_ne_set_products sp
  LEFT JOIN raw_ne_products p ON sp.セット商品コード = p.商品コード COLLATE NOCASE
  LEFT JOIN product_shipping ps ON sp.セット商品コード = ps.sku COLLATE NOCASE
  WHERE p.商品コード IS NULL AND ps.sku IS NULL
  LIMIT 5
`).all();
for (const r of setSamples) console.log(' ', r.セット商品コード, r.セット商品名?.slice(0,40));

console.log('\n--- 例外原価(NE無し)送料未登録サンプル ---');
const genkaSamples = db.prepare(`
  SELECT eg.sku, eg.商品名
  FROM exception_genka eg
  LEFT JOIN raw_ne_products p ON eg.sku = p.商品コード COLLATE NOCASE
  LEFT JOIN product_shipping ps ON eg.sku = ps.sku COLLATE NOCASE
  WHERE p.商品コード IS NULL AND ps.sku IS NULL
  LIMIT 5
`).all();
for (const r of genkaSamples) console.log(' ', r.sku, r.商品名?.slice(0,40));

db.close();
