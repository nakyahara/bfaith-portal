import Database from 'better-sqlite3';
const db = new Database('data/warehouse.db', { readonly: true });

// 1. NE商品マスタの代表商品コード状況
const repStats = db.prepare(`
  SELECT COUNT(*) as total,
    SUM(CASE WHEN 代表商品コード IS NOT NULL AND 代表商品コード != '' AND 代表商品コード != 商品コード THEN 1 ELSE 0 END) as has_rep
  FROM raw_ne_products
`).get();
console.log('NE商品マスタ:', repStats.total, '件中、代表コードが自分と異なる:', repStats.has_rep, '件');

// 2. 送料未登録だが代表コードに送料あり（継承可能）
const canInherit = db.prepare(`
  SELECT COUNT(*) as cnt FROM raw_ne_products p
  LEFT JOIN product_shipping ps1 ON p.商品コード = ps1.sku COLLATE NOCASE
  LEFT JOIN product_shipping ps2 ON p.代表商品コード = ps2.sku COLLATE NOCASE
  WHERE p.取扱区分 = '取扱中' AND ps1.sku IS NULL AND ps2.sku IS NOT NULL
    AND p.代表商品コード IS NOT NULL AND p.代表商品コード != '' AND p.代表商品コード != p.商品コード
`).get();
console.log('送料未登録だが代表コードに送料あり（継承可能）:', canInherit.cnt, '件');

// 3. セット商品でNE商品マスタに存在するもの
const setInNE = db.prepare(`
  SELECT COUNT(DISTINCT sp.セット商品コード) as total,
    SUM(CASE WHEN p.代表商品コード IS NOT NULL AND p.代表商品コード != '' AND p.代表商品コード != p.商品コード THEN 1 ELSE 0 END) as has_rep
  FROM (SELECT DISTINCT セット商品コード FROM raw_ne_set_products) sp
  INNER JOIN raw_ne_products p ON sp.セット商品コード = p.商品コード COLLATE NOCASE
`).get();
console.log('\nセット商品(NEにある):', setInNE.total, '件中、代表コード異なる:', setInNE.has_rep, '件');

// 4. セット商品の代表コード例
const setExamples = db.prepare(`
  SELECT p.商品コード, p.代表商品コード, p.商品名
  FROM raw_ne_products p
  INNER JOIN (SELECT DISTINCT セット商品コード FROM raw_ne_set_products) sp
    ON p.商品コード = sp.セット商品コード COLLATE NOCASE
  WHERE p.代表商品コード IS NOT NULL AND p.代表商品コード != '' AND p.代表商品コード != p.商品コード
  LIMIT 5
`).all();
console.log('\nセット商品の代表コード例:');
for (const r of setExamples) console.log(' ', r.商品コード, '->', r.代表商品コード, (r.商品名 || '').slice(0, 40));

// 5. セット商品でNE商品マスタに無いもの
const setNotInNE = db.prepare(`
  SELECT COUNT(DISTINCT sp.セット商品コード) as cnt
  FROM raw_ne_set_products sp
  LEFT JOIN raw_ne_products p ON sp.セット商品コード = p.商品コード COLLATE NOCASE
  WHERE p.商品コード IS NULL
`).get();
console.log('\nセット商品でNEに無い:', setNotInNE.cnt, '件');

// 6. NEに無いセットコードのパターン確認
const setNotInNESamples = db.prepare(`
  SELECT DISTINCT sp.セット商品コード, sp.セット商品名
  FROM raw_ne_set_products sp
  LEFT JOIN raw_ne_products p ON sp.セット商品コード = p.商品コード COLLATE NOCASE
  WHERE p.商品コード IS NULL
  LIMIT 10
`).all();
console.log('\nNEに無いセットコード例:');
for (const r of setNotInNESamples) console.log(' ', r.セット商品コード, (r.セット商品名 || '').slice(0, 50));

// 7. 現在の送料未登録件数（m_products）
const missingShip = db.prepare("SELECT 商品区分, COUNT(*) as cnt FROM m_products WHERE 取扱区分 = '取扱中' AND 送料 IS NULL GROUP BY 商品区分").all();
console.log('\n現在の送料未登録（m_products、取扱中）:');
for (const r of missingShip) console.log(' ', r.商品区分, ':', r.cnt, '件');

// 8. 代表コード継承したら何件減るか見積もり
const afterInherit = db.prepare(`
  SELECT COUNT(*) as cnt FROM m_products m
  LEFT JOIN product_shipping ps1 ON m.商品コード = ps1.sku COLLATE NOCASE
  LEFT JOIN raw_ne_products p ON m.商品コード = p.商品コード COLLATE NOCASE
  LEFT JOIN product_shipping ps2 ON p.代表商品コード = ps2.sku COLLATE NOCASE
  WHERE m.取扱区分 = '取扱中' AND m.送料 IS NULL
    AND ps1.sku IS NULL AND ps2.sku IS NULL
`).get();
console.log('\n代表コード継承後も送料未登録のままの件数:', afterInherit.cnt, '件');

db.close();
