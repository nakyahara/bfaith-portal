/**
 * warehouse API ルーター
 *
 * REST APIで warehouse.db のデータを公開する。
 * 読み取り専用（データ投入はCSVスクリプトで行う）。
 *
 * エンドポイント:
 *   GET /api/stats              — DB統計
 *   GET /api/products           — 商品マスタ検索
 *   GET /api/products/:code     — 商品詳細
 *   GET /api/orders             — 受注明細検索
 *   GET /api/orders/daily       — 日別×商品別 販売数集計
 *   GET /api/orders/summary     — 店舗別・期間別サマリー
 *   GET /api/shops              — 店舗マスタ一覧
 *   POST /api/import/products   — 商品マスタCSVアップロード投入
 *   GET /api/query              — 任意SQLクエリ（SELECT限定）
 */
import { Router } from 'express';
import multer from 'multer';
import fs from 'fs';
import iconv from 'iconv-lite';
import { initDB, getDB, getStats, saveToFile, updateSyncMeta } from './db.js';

const router = Router();
const upload = multer({ dest: 'data/import/' });

// ─── DB初期化 ───
let dbReady = false;

(async () => {
  try {
    await initDB();
    dbReady = true;
  } catch (e) {
    console.error('[Warehouse] DB初期化失敗:', e.message);
  }
})();

function ensureDB(req, res, next) {
  if (!dbReady) return res.status(503).json({ error: 'warehouse.db が初期化されていません' });
  next();
}

// ─── API認証（簡易トークン）───
function requireApiKey(req, res, next) {
  const apiKey = process.env.WAREHOUSE_API_KEY;
  if (!apiKey) return next(); // キー未設定なら認証スキップ（開発用）

  const provided = req.headers['x-api-key'] || req.query.api_key;
  if (provided !== apiKey) {
    return res.status(401).json({ error: 'Invalid API key' });
  }
  next();
}

router.use(requireApiKey);
router.use(ensureDB);

// ─── ヘルパー ───

function execQuery(sql, params = []) {
  const db = getDB();
  return db.prepare(sql).all(...params);
}

function preparedQuery(sql, params = []) {
  const db = getDB();
  return db.prepare(sql).all(...params);
}

// ─── GET /api/stats ───

router.get('/api/stats', (req, res) => {
  res.json(getStats());
});

// ─── GET /api/products ───

router.get('/api/products', (req, res) => {
  const { search, status, supplier, limit = '100', offset = '0' } = req.query;

  let sql = 'SELECT * FROM raw_ne_products WHERE 1=1';
  const params = [];

  if (search) {
    sql += ' AND (商品コード LIKE ? OR 商品名 LIKE ?)';
    const term = `%${search}%`;
    params.push(term, term);
  }
  if (status) {
    sql += ' AND 取扱区分 = ?';
    params.push(status);
  }
  if (supplier) {
    sql += ' AND 仕入先コード = ?';
    params.push(supplier);
  }

  // 総件数
  const countSql = sql.replace('SELECT *', 'SELECT COUNT(*) as cnt');
  const countResult = execQuery(countSql, params);
  const total = countResult[0]?.cnt || 0;

  sql += ' ORDER BY 商品コード LIMIT ? OFFSET ?';
  params.push(parseInt(limit), parseInt(offset));

  const rows = preparedQuery(sql, params);
  res.json({ rows, total, limit: parseInt(limit), offset: parseInt(offset) });
});

// ─── GET /api/products/all ───
// 単品+セット商品 統合ビュー（二次加工）

router.get('/api/products/all', (req, res) => {
  const { search, status, limit = '100', offset = '0' } = req.query;

  let where = 'WHERE 1=1';
  const params = [];

  if (search) {
    where += ' AND (p.商品コード LIKE ? OR p.商品名 LIKE ?)';
    const term = `%${search}%`;
    params.push(term, term);
  }
  if (status) {
    where += ' AND p.取扱区分 = ?';
    params.push(status);
  }

  const countResult = execQuery(
    `SELECT COUNT(*) as cnt FROM raw_ne_products p ${where}`,
    params.slice()
  );
  const total = countResult[0]?.cnt || 0;

  const sql = `
    SELECT
      p.商品コード,
      p.商品名,
      p.原価,
      p.売価,
      p.取扱区分,
      p.在庫数,
      p.仕入先コード,
      p.消費税率,
      CASE WHEN s.セット商品コード IS NOT NULL THEN 1 ELSE 0 END as is_set,
      s.数量 as セット数量,
      s.商品コード as 構成品コード,
      child.原価 as 構成品原価,
      ROUND(COALESCE(child.原価, 0) * COALESCE(s.数量, 1), 2) as 計算原価
    FROM raw_ne_products p
    LEFT JOIN raw_ne_set_products s ON p.商品コード = s.セット商品コード
    LEFT JOIN raw_ne_products child ON s.商品コード = child.商品コード
    ${where}
    ORDER BY p.商品コード
    LIMIT ? OFFSET ?
  `;
  params.push(parseInt(limit), parseInt(offset));

  const rows = preparedQuery(sql, params);
  res.json({ rows, total, limit: parseInt(limit), offset: parseInt(offset) });
});

// ─── GET /api/products/:code ───

router.get('/api/products/:code', (req, res) => {
  const rows = preparedQuery('SELECT * FROM raw_ne_products WHERE 商品コード = ?', [req.params.code]);
  if (rows.length === 0) return res.status(404).json({ error: '商品が見つかりません' });
  res.json(rows[0]);
});

// ─── GET /api/orders ───

router.get('/api/orders', (req, res) => {
  const { product, shop, from, to, status, limit = '100', offset = '0' } = req.query;

  let sql = 'SELECT * FROM raw_ne_orders WHERE 1=1';
  const params = [];

  if (product) {
    sql += ' AND 商品コード LIKE ?';
    params.push(`%${product}%`);
  }
  if (shop) {
    sql += ' AND 店舗コード = ?';
    params.push(shop);
  }
  if (from) {
    sql += ' AND 受注日 >= ?';
    params.push(from);
  }
  if (to) {
    sql += ' AND 受注日 <= ?';
    params.push(to);
  }
  if (status) {
    sql += ' AND 受注状態 LIKE ?';
    params.push(`%${status}%`);
  }

  const countSql = sql.replace('SELECT *', 'SELECT COUNT(*) as cnt');
  const countResult = execQuery(countSql, params);
  const total = countResult[0]?.cnt || 0;

  sql += ' ORDER BY 受注日 DESC LIMIT ? OFFSET ?';
  params.push(parseInt(limit), parseInt(offset));

  const rows = preparedQuery(sql, params);
  res.json({ rows, total, limit: parseInt(limit), offset: parseInt(offset) });
});

// ─── GET /api/orders/daily ───
// 日別×商品別の販売数集計（AI分析の主力エンドポイント）

router.get('/api/orders/daily', (req, res) => {
  const { product, shop, platform, from, to, limit = '1000' } = req.query;

  let sql = `
    SELECT
      DATE(o.受注日) as date,
      o.商品コード,
      o.商品名,
      s.shop_name,
      s.platform,
      SUM(o.受注数) as total_units,
      COUNT(DISTINCT o.伝票番号) as order_count
    FROM raw_ne_orders o
    LEFT JOIN shops s ON o.店舗コード = s.shop_code
    WHERE o.キャンセル区分 = '有効'
      AND COALESCE(s.platform, '') != '_ignore'
  `;
  const params = [];

  if (product) {
    sql += ' AND o.商品コード LIKE ?';
    params.push(`%${product}%`);
  }
  if (shop) {
    sql += ' AND o.店舗コード = ?';
    params.push(shop);
  }
  if (platform) {
    sql += ' AND s.platform = ?';
    params.push(platform);
  }
  if (from) {
    sql += ' AND o.受注日 >= ?';
    params.push(from);
  }
  if (to) {
    sql += ' AND o.受注日 <= ?';
    params.push(to);
  }

  sql += ' GROUP BY DATE(o.受注日), o.商品コード, o.店舗コード';
  sql += ' ORDER BY date DESC, total_units DESC';
  sql += ' LIMIT ?';
  params.push(parseInt(limit));

  const rows = preparedQuery(sql, params);
  res.json(rows);
});

// ─── GET /api/orders/summary ───
// 店舗別・期間別サマリー

router.get('/api/orders/summary', (req, res) => {
  const { from, to, group_by = 'shop' } = req.query;

  let baseCond = "WHERE o.キャンセル区分 = '有効'";
  const params = [];

  if (from) {
    baseCond += ' AND o.受注日 >= ?';
    params.push(from);
  }
  if (to) {
    baseCond += ' AND o.受注日 <= ?';
    params.push(to);
  }

  // shops JOINがあるクエリでは _ignore を除外
  const dateCondWithIgnore = baseCond + " AND COALESCE(s.platform, '') != '_ignore'";

  let sql;
  if (group_by === 'product') {
    // product集計はshops JOINなし → _ignore除外は店舗コードで直接指定
    sql = `
      SELECT
        o.商品コード,
        MAX(o.商品名) as 商品名,
        SUM(o.受注数) as total_units,
        COUNT(DISTINCT o.伝票番号) as order_count,
        MIN(DATE(o.受注日)) as first_order,
        MAX(DATE(o.受注日)) as last_order
      FROM raw_ne_orders o
      ${baseCond} AND o.店舗コード NOT IN ('7', '15')
      GROUP BY o.商品コード
      ORDER BY total_units DESC
    `;
  } else if (group_by === 'month') {
    sql = `
      SELECT
        strftime('%Y-%m', o.受注日) as month,
        s.platform,
        SUM(o.受注数) as total_units,
        COUNT(DISTINCT o.伝票番号) as order_count
      FROM raw_ne_orders o
      LEFT JOIN shops s ON o.店舗コード = s.shop_code
      ${dateCondWithIgnore}
      GROUP BY month, s.platform
      ORDER BY month DESC, total_units DESC
    `;
  } else {
    // デフォルト: 店舗別
    sql = `
      SELECT
        o.店舗コード,
        s.shop_name,
        s.platform,
        SUM(o.受注数) as total_units,
        COUNT(DISTINCT o.伝票番号) as order_count
      FROM raw_ne_orders o
      LEFT JOIN shops s ON o.店舗コード = s.shop_code
      ${dateCondWithIgnore}
      GROUP BY o.店舗コード
      ORDER BY total_units DESC
    `;
  }

  const rows = preparedQuery(sql, params);
  res.json(rows);
});

// ─── GET /api/sets ───
// セット商品一覧（構成品の原価も結合）

router.get('/api/sets', (req, res) => {
  const { search, limit = '100', offset = '0' } = req.query;

  let where = 'WHERE 1=1';
  const params = [];

  if (search) {
    where += ' AND (s.セット商品コード LIKE ? OR s.セット商品名 LIKE ? OR s.商品コード LIKE ?)';
    const term = `%${search}%`;
    params.push(term, term, term);
  }

  const countResult = execQuery(
    `SELECT COUNT(DISTINCT s.セット商品コード) as cnt FROM raw_ne_set_products s ${where}`, params
  );
  const total = countResult[0]?.cnt || 0;

  const sql = `
    SELECT
      s.セット商品コード,
      s.セット商品名,
      s.セット販売価格,
      s.商品コード as 構成品コード,
      s.数量,
      s.セット在庫数,
      p.商品名 as 構成品名,
      p.原価 as 構成品原価,
      p.売価 as 構成品売価,
      ROUND(COALESCE(p.原価, 0) * s.数量, 2) as セット原価
    FROM raw_ne_set_products s
    LEFT JOIN raw_ne_products p ON s.商品コード = p.商品コード COLLATE NOCASE
    ${where}
    ORDER BY s.セット商品コード
    LIMIT ? OFFSET ?
  `;
  params.push(parseInt(limit), parseInt(offset));

  const rows = preparedQuery(sql, params);
  res.json({ rows, total, limit: parseInt(limit), offset: parseInt(offset) });
});

// ─── GET /api/sets/:code ───

router.get('/api/sets/:code', (req, res) => {
  const rows = preparedQuery(`
    SELECT
      s.セット商品コード,
      s.セット商品名,
      s.セット販売価格,
      s.商品コード as 構成品コード,
      s.数量,
      s.セット在庫数,
      p.商品名 as 構成品名,
      p.原価 as 構成品原価,
      p.売価 as 構成品売価,
      p.仕入先コード,
      ROUND(COALESCE(p.原価, 0) * s.数量, 2) as セット原価
    FROM raw_ne_set_products s
    LEFT JOIN raw_ne_products p ON s.商品コード = p.商品コード COLLATE NOCASE
    WHERE s.セット商品コード = ?
  `, [req.params.code]);

  if (rows.length === 0) return res.status(404).json({ error: 'セット商品が見つかりません' });
  res.json(rows);
});

// ─── GET /api/shops ───

router.get('/api/shops', (req, res) => {
  const rows = preparedQuery('SELECT * FROM shops ORDER BY shop_code');
  res.json(rows);
});

// ─── GET /api/query ───
// 任意SQLクエリ（SELECT限定、AI分析用）

router.get('/api/query', (req, res) => {
  let { sql } = req.query;
  if (!sql) return res.status(400).json({ error: 'sql パラメータが必要です' });

  // SELECT文のみ許可
  const normalized = sql.trim().toUpperCase();
  if (!normalized.startsWith('SELECT')) {
    return res.status(403).json({ error: 'SELECT文のみ実行可能です' });
  }

  // 危険なキーワードチェック
  const forbidden = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'ATTACH', 'DETACH', 'PRAGMA'];
  for (const kw of forbidden) {
    if (normalized.includes(kw)) {
      return res.status(403).json({ error: `${kw} は使用できません` });
    }
  }

  // LIMIT強制（指定がなければ500件に制限）
  if (!normalized.includes('LIMIT')) {
    sql = sql.trim().replace(/;$/, '') + ' LIMIT 500';
  }

  try {
    const db = getDB();
    // 5秒タイムアウト（重いクエリ防止）
    db.pragma('busy_timeout = 5000');
    const rows = execQuery(sql);
    res.json({ rows, count: rows.length });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

// ─── GET /api/master ───
// 統合商品マスタ（v_product_master）

router.get('/api/master', (req, res) => {
  const { search, status, cost_source, has_shipping, limit = '100', offset = '0' } = req.query;
  let sql = 'SELECT * FROM v_product_master WHERE 1=1';
  const params = [];
  if (search) { sql += ' AND (商品コード LIKE ? OR 商品名 LIKE ?)'; const t = `%${search}%`; params.push(t, t); }
  if (status) { sql += ' AND 取扱区分 = ?'; params.push(status); }
  if (cost_source) { sql += ' AND 原価ソース = ?'; params.push(cost_source); }
  if (has_shipping === '0') { sql += ' AND 送料 IS NULL'; }
  if (has_shipping === '1') { sql += ' AND 送料 IS NOT NULL'; }
  const countSql = sql.replace('SELECT *', 'SELECT COUNT(*) as cnt');
  const total = execQuery(countSql, params)[0]?.cnt || 0;
  sql += ' ORDER BY 商品コード LIMIT ? OFFSET ?';
  params.push(parseInt(limit), parseInt(offset));
  const rows = execQuery(sql, params);
  res.json({ rows, total });
});

// ─── GET /api/sales ───
// 商品別販売集計

router.get('/api/sales', (req, res) => {
  const { product, platform, month, limit = '100' } = req.query;
  let sql = 'SELECT 商品コード, 商品名, platform, SUM(数量) as total_qty, ROUND(SUM(売上金額)) as total_sales, SUM(注文数) as total_orders, data_source FROM v_sales_by_product WHERE 1=1';
  const params = [];
  if (product) { sql += ' AND 商品コード LIKE ?'; params.push(`%${product}%`); }
  if (platform) { sql += ' AND platform = ?'; params.push(platform); }
  if (month) { sql += ' AND month = ?'; params.push(month); }
  sql += ' GROUP BY 商品コード, platform ORDER BY total_sales DESC LIMIT ?';
  params.push(parseInt(limit));
  res.json(execQuery(sql, params));
});

// ─── GET /api/sales/monthly ───
// モール別月次売上

router.get('/api/sales/monthly', (req, res) => {
  const { months = '6' } = req.query;
  const rows = execQuery(`
    SELECT month, platform, data_source, SUM(数量) as total_qty, ROUND(SUM(売上金額)) as total_sales, SUM(注文数) as total_orders
    FROM v_sales_by_product
    GROUP BY month, platform
    ORDER BY month DESC, total_sales DESC
    LIMIT ?
  `, [parseInt(months) * 10]);
  res.json(rows);
});

// ─── GET /api/missing ───
// 未登録データ一覧

router.get('/api/missing', (req, res) => {
  const { type } = req.query;
  let sql = 'SELECT * FROM v_missing_data';
  const params = [];
  if (type) { sql += ' WHERE missing_type = ?'; params.push(type); }
  sql += ' ORDER BY missing_type, 商品コード';
  const rows = execQuery(sql, params);
  const summary = execQuery('SELECT missing_type, COUNT(*) as cnt FROM v_missing_data GROUP BY missing_type');
  res.json({ rows, summary });
});

// ─── 監査ログ ヘルパー ───

function auditLog(db, tableName, recordKey, operation, oldData, newData) {
  const ts = new Date().toISOString().replace('T', ' ').slice(0, 19);
  db.prepare('INSERT INTO audit_log (timestamp, table_name, record_key, operation, old_data, new_data) VALUES (?,?,?,?,?,?)').run(
    ts, tableName, recordKey, operation,
    oldData ? JSON.stringify(oldData) : null,
    newData ? JSON.stringify(newData) : null
  );
}

// ─── POST /api/shipping ───
// 送料登録・更新

router.post('/api/shipping', (req, res) => {
  const sku = (req.body.sku || '').toLowerCase();
  const { shipping_code, ship_method, ship_cost } = req.body;
  if (!sku || !ship_cost) return res.status(400).json({ error: 'sku と ship_cost は必須です' });
  const db = getDB();
  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);
  const old = db.prepare('SELECT * FROM product_shipping WHERE sku = ?').get(sku);
  const newData = { sku, shipping_code: shipping_code || '', ship_method: ship_method || '', ship_cost: parseFloat(ship_cost) };
  db.prepare('INSERT OR REPLACE INTO product_shipping (sku, product_name, shipping_code, ship_method, ship_cost, note, synced_at) VALUES (?, COALESCE((SELECT 商品名 FROM raw_ne_products WHERE 商品コード = ?), ?), ?, ?, ?, ?, ?)').run(sku, sku, old?.product_name || '', shipping_code || '', ship_method || '', parseFloat(ship_cost), old?.note || '', now);
  auditLog(db, 'product_shipping', sku, old ? 'UPDATE' : 'INSERT', old || null, newData);
  // m_productsにリアルタイム反映（該当行 + 代表コードが同じバリエーション）
  try {
    db.prepare('UPDATE m_products SET 送料 = ?, 送料コード = ?, 配送方法 = ?, updated_at = ? WHERE 商品コード = ?').run(parseFloat(ship_cost), shipping_code || '', ship_method || '', now, sku);
    // 代表商品コード経由でバリエーションにも反映
    db.prepare(`UPDATE m_products SET 送料 = ?, 送料コード = ?, 配送方法 = ?, updated_at = ?
      WHERE 商品コード IN (SELECT 商品コード FROM raw_ne_products WHERE 代表商品コード = ? COLLATE NOCASE)
      AND 送料 IS NULL`).run(parseFloat(ship_cost), shipping_code || '', ship_method || '', now, sku);
  } catch {}
  res.json({ ok: true, sku, ship_cost });
});

// ─── POST /api/genka ───
// 原価登録・更新

router.post('/api/genka', (req, res) => {
  const sku = (req.body.sku || '').toLowerCase();
  const { genka, product_name } = req.body;
  if (!sku || genka === undefined) return res.status(400).json({ error: 'sku と genka は必須です' });
  const db = getDB();
  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);
  const old = db.prepare('SELECT * FROM exception_genka WHERE sku = ?').get(sku);
  const newData = { sku, genka: parseFloat(genka), product_name: product_name || '' };
  db.prepare('INSERT OR REPLACE INTO exception_genka (sku, genka, 商品名, synced_at) VALUES (?, ?, ?, ?)').run(sku, parseFloat(genka), product_name || '', now);
  auditLog(db, 'exception_genka', sku, old ? 'UPDATE' : 'INSERT', old || null, newData);
  // m_productsにリアルタイム反映
  try {
    db.prepare("UPDATE m_products SET 原価 = ?, 原価ソース = '例外', 原価状態 = 'OVERRIDDEN', updated_at = ? WHERE 商品コード = ?").run(parseFloat(genka), now, sku);
  } catch {}
  res.json({ ok: true, sku, genka });
});

// ─── POST /api/skumap ───
// SKUマップ登録・更新

router.post('/api/skumap', (req, res) => {
  const seller_sku = (req.body.seller_sku || '').toLowerCase();
  const ne_code = (req.body.ne_code || '').toLowerCase();
  const { asin, product_name, quantity } = req.body;
  if (!seller_sku || !ne_code) return res.status(400).json({ error: 'seller_sku と ne_code は必須です' });
  const db = getDB();
  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);
  const old = db.prepare('SELECT * FROM sku_map WHERE seller_sku = ? AND ne_code = ?').get(seller_sku, ne_code);
  const newData = { seller_sku, asin: asin || '', ne_code, quantity: parseInt(quantity) || 1 };
  db.prepare('INSERT OR REPLACE INTO sku_map (seller_sku, asin, 商品名, ne_code, 数量, synced_at) VALUES (?, ?, ?, ?, ?, ?)').run(seller_sku, asin || '', product_name || '', ne_code, parseInt(quantity) || 1, now);
  auditLog(db, 'sku_map', seller_sku + ':' + ne_code, old ? 'UPDATE' : 'INSERT', old || null, newData);
  // unmapped_salesから該当SKUを削除（リアルタイム反映）
  try {
    db.prepare('DELETE FROM unmapped_sales WHERE モール商品コード = ?').run(seller_sku);
  } catch {}
  res.json({ ok: true, seller_sku, ne_code });
});

// ─── CSV アップロード（送料・原価・SKUマップ共通） ───

function parseCsvBuffer(buf) {
  // UTF-8 BOM or Shift-JIS 自動判定
  let text;
  if (buf[0] === 0xEF && buf[1] === 0xBB && buf[2] === 0xBF) {
    text = buf.toString('utf-8');
  } else {
    // Shift-JIS判定: 0x80以上のバイトが多ければcp932
    const highBytes = [...buf.slice(0, 1000)].filter(b => b > 0x7F).length;
    text = highBytes > 5 ? iconv.decode(buf, 'cp932') : buf.toString('utf-8');
  }
  const lines = text.split(/\r?\n/).filter(l => l.trim());
  return lines.map(l => l.split(',').map(c => c.replace(/^"|"$/g, '').trim()));
}

// POST /api/csv/shipping — 送料CSV一括登録（追加・更新、既存は消さない）
// CSV形式: 商品コード, 送料コード, 配送方法, 送料
router.post('/api/csv/shipping', upload.single('file'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'ファイルが必要です' });
  const db = getDB();
  const buf = fs.readFileSync(req.file.path);
  fs.unlinkSync(req.file.path);
  const rows = parseCsvBuffer(buf);
  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);

  // ヘッダー行スキップ（1行目に「商品コード」「sku」等が含まれる場合）
  let dataRows = rows;
  if (dataRows.length > 0 && /商品コード|sku|SKU/i.test(dataRows[0][0])) dataRows = dataRows.slice(1);

  const stmt = db.prepare('INSERT OR REPLACE INTO product_shipping (sku, product_name, shipping_code, ship_method, ship_cost, note, synced_at) VALUES (?, COALESCE((SELECT product_name FROM product_shipping WHERE sku = ?), (SELECT 商品名 FROM raw_ne_products WHERE 商品コード = ? COLLATE NOCASE), ?), ?, ?, ?, ?, ?)');
  const updateMp = db.prepare('UPDATE m_products SET 送料 = ?, 送料コード = ?, 配送方法 = ?, updated_at = ? WHERE 商品コード = ?');
  const updateVariants = db.prepare(`UPDATE m_products SET 送料 = ?, 送料コード = ?, 配送方法 = ?, updated_at = ?
    WHERE 商品コード IN (SELECT 商品コード FROM raw_ne_products WHERE 代表商品コード = ? COLLATE NOCASE) AND 送料 IS NULL`);

  // ヘッダーから送料コード列を自動判定
  let colSku = 0, colShipCode = 1;
  if (dataRows.length > 0 && rows.length > dataRows.length) {
    const hdr = rows[0].map(h => h.toLowerCase().trim());
    const ci = hdr.findIndex(h => h.includes('送料コード') || h.includes('shipping_code'));
    if (ci >= 0) colShipCode = ci;
  }

  // shipping_ratesテーブルから送料コード→配送方法・送料を引く
  const ratesMap = new Map();
  for (const r of db.prepare('SELECT shipping_code, 小分類区分名称, 配送関係費合計 FROM shipping_rates').all()) {
    ratesMap.set(r.shipping_code, { method: r.小分類区分名称 || '', cost: r.配送関係費合計 || 0 });
  }

  let count = 0, skipped = 0, invalidCode = 0;
  const tx = db.transaction(() => {
    for (const row of dataRows) {
      const sku = (row[colSku] || '').toLowerCase().trim();
      if (!sku) { skipped++; continue; }
      const shippingCode = (row[colShipCode] || '').trim();
      if (!shippingCode) { skipped++; continue; }
      const rate = ratesMap.get(shippingCode);
      if (!rate) { invalidCode++; skipped++; continue; }
      stmt.run(sku, sku, sku, '', shippingCode, rate.method, rate.cost, '', now);
      try {
        updateMp.run(rate.cost, shippingCode, rate.method, now, sku);
        updateVariants.run(rate.cost, shippingCode, rate.method, now, sku);
      } catch {}
      count++;
    }
  });
  tx();
  res.json({ ok: true, imported: count, skipped, invalidCode, total: dataRows.length });
});

// POST /api/csv/genka — 原価CSV一括登録
// CSV形式: 商品コード, 原価, 商品名（任意）
router.post('/api/csv/genka', upload.single('file'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'ファイルが必要です' });
  const db = getDB();
  const buf = fs.readFileSync(req.file.path);
  fs.unlinkSync(req.file.path);
  const rows = parseCsvBuffer(buf);
  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);

  let dataRows = rows;
  if (dataRows.length > 0 && /商品コード|sku|SKU/i.test(dataRows[0][0])) dataRows = dataRows.slice(1);

  const stmt = db.prepare('INSERT OR REPLACE INTO exception_genka (sku, genka, 商品名, synced_at) VALUES (?, ?, ?, ?)');
  const updateMp = db.prepare("UPDATE m_products SET 原価 = ?, 原価ソース = '例外', 原価状態 = 'OVERRIDDEN', updated_at = ? WHERE 商品コード = ?");

  let count = 0, skipped = 0;
  const tx = db.transaction(() => {
    for (const row of dataRows) {
      const sku = (row[0] || '').toLowerCase().trim();
      if (!sku) { skipped++; continue; }
      const genka = parseFloat(row[1]);
      if (isNaN(genka)) { skipped++; continue; }
      const name = (row[2] || '').trim();
      stmt.run(sku, genka, name, now);
      try { updateMp.run(genka, now, sku); } catch {}
      count++;
    }
  });
  tx();
  res.json({ ok: true, imported: count, skipped, total: dataRows.length });
});

// POST /api/csv/skumap — SKUマップCSV一括登録
// CSV形式: seller_sku, ne_code, 数量（任意、デフォルト1）, ASIN（任意）
router.post('/api/csv/skumap', upload.single('file'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'ファイルが必要です' });
  const db = getDB();
  const buf = fs.readFileSync(req.file.path);
  fs.unlinkSync(req.file.path);
  const rows = parseCsvBuffer(buf);
  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);

  let dataRows = rows;
  if (dataRows.length > 0 && /seller_sku|SKU|商品コード/i.test(dataRows[0][0])) dataRows = dataRows.slice(1);

  const stmt = db.prepare('INSERT OR REPLACE INTO sku_map (seller_sku, asin, 商品名, ne_code, 数量, synced_at) VALUES (?, ?, ?, ?, ?, ?)');
  const delUnmapped = db.prepare('DELETE FROM unmapped_sales WHERE モール商品コード = ?');

  let count = 0, skipped = 0;
  const tx = db.transaction(() => {
    for (const row of dataRows) {
      const sellerSku = (row[0] || '').toLowerCase().trim();
      const neCode = (row[1] || '').toLowerCase().trim();
      if (!sellerSku || !neCode) { skipped++; continue; }
      const qty = parseInt(row[2]) || 1;
      const asin = (row[3] || '').trim();
      stmt.run(sellerSku, asin, '', neCode, qty, now);
      try { delUnmapped.run(sellerSku); } catch {}
      count++;
    }
  });
  tx();
  res.json({ ok: true, imported: count, skipped, total: dataRows.length });
});

// ─── DELETE /api/shipping/:sku ───

router.delete('/api/shipping/:sku', (req, res) => {
  const db = getDB();
  const old = db.prepare('SELECT * FROM product_shipping WHERE sku = ?').get(req.params.sku);
  const result = db.prepare('DELETE FROM product_shipping WHERE sku = ?').run(req.params.sku);
  if (old) auditLog(db, 'product_shipping', req.params.sku, 'DELETE', old, null);
  res.json({ ok: true, deleted: result.changes });
});

// ─── DELETE /api/genka/:sku ───

router.delete('/api/genka/:sku', (req, res) => {
  const db = getDB();
  const old = db.prepare('SELECT * FROM exception_genka WHERE sku = ?').get(req.params.sku);
  const result = db.prepare('DELETE FROM exception_genka WHERE sku = ?').run(req.params.sku);
  if (old) auditLog(db, 'exception_genka', req.params.sku, 'DELETE', old, null);
  res.json({ ok: true, deleted: result.changes });
});

// ─── DELETE /api/skumap/:sku ───
// ne_codeが指定されていればその1行、なければSKU全行を削除

router.delete('/api/skumap/:sku', (req, res) => {
  const db = getDB();
  const ne_code = req.query.ne_code;
  if (ne_code) {
    const old = db.prepare('SELECT * FROM sku_map WHERE seller_sku = ? AND ne_code = ?').get(req.params.sku, ne_code);
    const result = db.prepare('DELETE FROM sku_map WHERE seller_sku = ? AND ne_code = ?').run(req.params.sku, ne_code);
    if (old) auditLog(db, 'sku_map', req.params.sku + ':' + ne_code, 'DELETE', old, null);
    res.json({ ok: true, deleted: result.changes });
  } else {
    const olds = db.prepare('SELECT * FROM sku_map WHERE seller_sku = ?').all(req.params.sku);
    const result = db.prepare('DELETE FROM sku_map WHERE seller_sku = ?').run(req.params.sku);
    for (const old of olds) auditLog(db, 'sku_map', req.params.sku + ':' + old.ne_code, 'DELETE', old, null);
    res.json({ ok: true, deleted: result.changes });
  }
});

// ─── GET /api/shipping/list ───
// 送料マスタ検索（編集用）

router.get('/api/shipping/list', (req, res) => {
  const { search, limit = '100', offset = '0' } = req.query;
  let sql = 'SELECT * FROM product_shipping WHERE 1=1';
  const params = [];
  if (search) { sql += ' AND (sku LIKE ? OR product_name LIKE ?)'; const t = `%${search}%`; params.push(t, t); }
  const countSql = sql.replace('SELECT *', 'SELECT COUNT(*) as cnt');
  const total = execQuery(countSql, params)[0]?.cnt || 0;
  sql += ' ORDER BY sku LIMIT ? OFFSET ?';
  params.push(parseInt(limit), parseInt(offset));
  res.json({ rows: execQuery(sql, params), total });
});

// ─── GET /api/genka/list ───

router.get('/api/genka/list', (req, res) => {
  const { search, limit = '100', offset = '0' } = req.query;
  let sql = 'SELECT * FROM exception_genka WHERE 1=1';
  const params = [];
  if (search) { sql += ' AND (sku LIKE ? OR 商品名 LIKE ?)'; const t = `%${search}%`; params.push(t, t); }
  const countSql = sql.replace('SELECT *', 'SELECT COUNT(*) as cnt');
  const total = execQuery(countSql, params)[0]?.cnt || 0;
  sql += ' ORDER BY sku LIMIT ? OFFSET ?';
  params.push(parseInt(limit), parseInt(offset));
  res.json({ rows: execQuery(sql, params), total });
});

// ─── GET /api/skumap/list ───

router.get('/api/skumap/list', (req, res) => {
  const { search, limit = '100', offset = '0' } = req.query;
  let sql = 'SELECT * FROM sku_map WHERE 1=1';
  const params = [];
  if (search) { sql += ' AND (seller_sku LIKE ? OR 商品名 LIKE ? OR ne_code LIKE ?)'; const t = `%${search}%`; params.push(t, t, t); }
  const countSql = sql.replace('SELECT *', 'SELECT COUNT(*) as cnt');
  const total = execQuery(countSql, params)[0]?.cnt || 0;
  sql += ' ORDER BY seller_sku LIMIT ? OFFSET ?';
  params.push(parseInt(limit), parseInt(offset));
  res.json({ rows: execQuery(sql, params), total });
});

// ─── GET /api/missing/prioritized ───
// m_productsベースの未登録データ（f_sales_by_productで売上優先度付け）

router.get('/api/missing/prioritized', (req, res) => {
  const { type } = req.query;
  const db = getDB();
  const now = new Date();
  const cutoff7 = new Date(now); cutoff7.setDate(cutoff7.getDate() - 7);
  const cutoff30 = new Date(now); cutoff30.setDate(cutoff30.getDate() - 30);
  const cutoff7Str = cutoff7.toISOString().slice(0, 10);
  const cutoff30Str = cutoff30.toISOString().slice(0, 10);

  let rows = [];

  try {
    if (!type || type === 'shipping') {
      rows = rows.concat(db.prepare(`
        SELECT 'shipping' as missing_type, m.商品コード, m.商品名, m.標準売価 as 売価, m.原価,
          m.取扱区分, m.商品区分,
          COALESCE(s7.qty, 0) as sales_7d,
          COALESCE(s30.qty, 0) as sales_30d,
          s30.last_sold,
          CASE WHEN COALESCE(s7.qty, 0) > 0 THEN 'A_7日以内' WHEN COALESCE(s30.qty, 0) > 0 THEN 'B_30日以内' ELSE 'C_実績なし' END as priority
        FROM m_products m
        LEFT JOIN (
          SELECT 商品コード, SUM(数量) as qty FROM f_sales_by_product WHERE 日付 >= ? GROUP BY 商品コード
        ) s7 ON m.商品コード = s7.商品コード
        LEFT JOIN (
          SELECT 商品コード, SUM(数量) as qty, MAX(日付) as last_sold FROM f_sales_by_product WHERE 日付 >= ? GROUP BY 商品コード
        ) s30 ON m.商品コード = s30.商品コード
        WHERE m.取扱区分 = '取扱中' AND m.送料 IS NULL
        ORDER BY priority, sales_7d DESC, sales_30d DESC
        LIMIT 200
      `).all(cutoff7Str, cutoff30Str));
    }

    if (!type || type === 'genka') {
      rows = rows.concat(db.prepare(`
        SELECT 'genka' as missing_type, m.商品コード, m.商品名, m.標準売価 as 売価, m.原価,
          m.取扱区分, m.商品区分,
          COALESCE(s7.qty, 0) as sales_7d,
          COALESCE(s30.qty, 0) as sales_30d,
          s30.last_sold,
          CASE WHEN COALESCE(s7.qty, 0) > 0 THEN 'A_7日以内' WHEN COALESCE(s30.qty, 0) > 0 THEN 'B_30日以内' ELSE 'C_実績なし' END as priority
        FROM m_products m
        LEFT JOIN (
          SELECT 商品コード, SUM(数量) as qty FROM f_sales_by_product WHERE 日付 >= ? GROUP BY 商品コード
        ) s7 ON m.商品コード = s7.商品コード
        LEFT JOIN (
          SELECT 商品コード, SUM(数量) as qty, MAX(日付) as last_sold FROM f_sales_by_product WHERE 日付 >= ? GROUP BY 商品コード
        ) s30 ON m.商品コード = s30.商品コード
        WHERE m.取扱区分 = '取扱中' AND m.原価状態 IN ('MISSING', 'PARTIAL')
        ORDER BY priority, sales_7d DESC, sales_30d DESC
        LIMIT 200
      `).all(cutoff7Str, cutoff30Str));
    }

    if (!type || type === 'sku_map') {
      rows = rows.concat(db.prepare(`
        SELECT 'sku_map' as missing_type, モール商品コード as 商品コード, 商品名,
          NULL as 売価, NULL as 原価, NULL as 取扱区分, NULL as 商品区分,
          SUM(数量) as sales_7d, SUM(数量) as sales_30d,
          MAX(日付) as last_sold,
          'B_30日以内' as priority
        FROM unmapped_sales
        GROUP BY モール商品コード, 商品名
        ORDER BY sales_30d DESC
        LIMIT 200
      `).all());
    }

    if (!type || type === 'sales_class') {
      rows = rows.concat(db.prepare(`
        SELECT 'sales_class' as missing_type, m.商品コード, m.商品名, m.標準売価 as 売価, m.原価,
          m.取扱区分, m.商品区分,
          COALESCE(s7.qty, 0) as sales_7d,
          COALESCE(s30.qty, 0) as sales_30d,
          s30.last_sold,
          CASE WHEN COALESCE(s7.qty, 0) > 0 THEN 'A_7日以内' WHEN COALESCE(s30.qty, 0) > 0 THEN 'B_30日以内' ELSE 'C_実績なし' END as priority
        FROM m_products m
        LEFT JOIN (
          SELECT 商品コード, SUM(数量) as qty FROM f_sales_by_product WHERE 日付 >= ? GROUP BY 商品コード
        ) s7 ON m.商品コード = s7.商品コード
        LEFT JOIN (
          SELECT 商品コード, SUM(数量) as qty, MAX(日付) as last_sold FROM f_sales_by_product WHERE 日付 >= ? GROUP BY 商品コード
        ) s30 ON m.商品コード = s30.商品コード
        WHERE m.商品区分 = '単品' AND m.売上分類 IS NULL
        ORDER BY priority, sales_7d DESC, sales_30d DESC
        LIMIT 200
      `).all(cutoff7Str, cutoff30Str));
    }
  } catch (e) {
    console.error('[missing/prioritized] m_products未構築?', e.message);
  }

  const summary = {};
  for (const r of rows) {
    summary[r.missing_type] = (summary[r.missing_type] || 0) + 1;
  }

  res.json({ rows, summary });
});

// ─── GET /api/audit ───
// 変更履歴ログ

router.get('/api/audit', (req, res) => {
  const { table_name, record_key, limit = '50' } = req.query;
  let sql = 'SELECT * FROM audit_log WHERE 1=1';
  const params = [];
  if (table_name) { sql += ' AND table_name = ?'; params.push(table_name); }
  if (record_key) { sql += ' AND record_key LIKE ?'; params.push(`%${record_key}%`); }
  sql += ' ORDER BY id DESC LIMIT ?';
  params.push(parseInt(limit));
  res.json(execQuery(sql, params));
});

// ─── GET /api/shipping_rates ───

router.get('/api/shipping_rates', (req, res) => {
  const { format } = req.query;
  if (format === 'csv') {
    const rows = execQuery('SELECT shipping_code, 小分類区分名称, 運送会社, 梱包サイズ, 送料, 出荷作業料, 想定梱包資材費, 想定人件費, 配送関係費合計 FROM shipping_rates ORDER BY shipping_code');
    const header = '送料コード,区分名称,運送会社,梱包サイズ,送料,出荷作業料,梱包資材費,人件費,配送関係費合計';
    const escapeCsv = v => { const s = String(v ?? ''); return s.includes(',') || s.includes('"') ? '"' + s.replace(/"/g, '""') + '"' : s; };
    const lines = [header, ...rows.map(r => Object.values(r).map(escapeCsv).join(','))];
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', 'attachment; filename="shipping_rates.csv"');
    return res.send('\uFEFF' + lines.join('\r\n'));
  }
  res.json(execQuery('SELECT * FROM shipping_rates ORDER BY shipping_code'));
});

// ─── POST /api/sales_class ───
// 売上分類登録・更新

router.post('/api/sales_class', (req, res) => {
  const sku = (req.body.sku || '').toLowerCase();
  const { sales_class, product_name } = req.body;
  if (!sku || !sales_class) return res.status(400).json({ error: 'sku と sales_class は必須です' });
  const sc = parseInt(sales_class);
  if (![1, 2, 3, 4].includes(sc)) return res.status(400).json({ error: 'sales_class は 1〜4 の整数です' });
  const db = getDB();
  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);
  const old = db.prepare('SELECT * FROM product_sales_class WHERE sku = ?').get(sku);
  db.prepare('INSERT OR REPLACE INTO product_sales_class (sku, sales_class, 商品名, synced_at) VALUES (?, ?, ?, ?)').run(sku, sc, product_name || '', now);
  auditLog(db, 'product_sales_class', sku, old ? 'UPDATE' : 'INSERT', old || null, { sku, sales_class: sc });
  // m_productsにリアルタイム反映
  try { db.prepare('UPDATE m_products SET 売上分類 = ?, updated_at = ? WHERE 商品コード = ?').run(sc, now, sku); } catch {}
  res.json({ ok: true, sku, sales_class: sc });
});

// ─── POST /api/csv/sales_class ───
// 売上分類CSV一括登録
// CSV形式: 商品コード, 売上分類(1-4)

router.post('/api/csv/sales_class', upload.single('file'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'ファイルが必要です' });
  const db = getDB();
  const buf = fs.readFileSync(req.file.path);
  fs.unlinkSync(req.file.path);
  const rows = parseCsvBuffer(buf);
  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);

  let dataRows = rows;
  if (dataRows.length > 0 && /商品コード|sku|SKU/i.test(dataRows[0][0])) dataRows = dataRows.slice(1);

  // ヘッダーから売上分類列を自動判定
  let colSku = 0, colClass = 1;
  if (dataRows.length > 0 && rows.length > dataRows.length) {
    const hdr = rows[0].map(h => h.toLowerCase().trim());
    const ci = hdr.findIndex(h => h.includes('売上分類') || h.includes('sales_class'));
    if (ci >= 0) colClass = ci;
  }

  const stmt = db.prepare('INSERT OR REPLACE INTO product_sales_class (sku, sales_class, 商品名, synced_at) VALUES (?, ?, ?, ?)');
  const updateMp = db.prepare('UPDATE m_products SET 売上分類 = ?, updated_at = ? WHERE 商品コード = ?');

  let count = 0, skipped = 0, invalid = 0;
  const tx = db.transaction(() => {
    for (const row of dataRows) {
      const sku = (row[colSku] || '').toLowerCase().trim();
      if (!sku) { skipped++; continue; }
      const sc = parseInt(row[colClass]);
      if (![1, 2, 3, 4].includes(sc)) { invalid++; skipped++; continue; }
      const name = db.prepare('SELECT 商品名 FROM m_products WHERE 商品コード = ?').get(sku)?.商品名 || '';
      stmt.run(sku, sc, name, now);
      try { updateMp.run(sc, now, sku); } catch {}
      count++;
    }
  });
  tx();
  res.json({ ok: true, imported: count, skipped, invalid, total: dataRows.length });
});

// ─── GET /api/missing/download ───
// 未登録データCSVダウンロード

router.get('/api/missing/download', (req, res) => {
  const { type } = req.query;
  const db = getDB();
  let rows = [];
  let filename = 'missing.csv';
  let header = '';

  try {
    if (type === 'shipping') {
      rows = db.prepare(`
        SELECT m.商品コード, m.商品名, m.商品区分, m.標準売価, m.原価, m.原価状態,
          COALESCE(p.代表商品コード, '') as 代表商品コード
        FROM m_products m
        LEFT JOIN raw_ne_products p ON m.商品コード = p.商品コード COLLATE NOCASE
        WHERE m.取扱区分 = '取扱中' AND m.送料 IS NULL
        ORDER BY m.商品区分, m.商品コード
      `).all();
      header = '商品コード,商品名,商品区分,標準売価,原価,原価状態,代表商品コード,送料コード';
      filename = 'shipping_missing.csv';
    } else if (type === 'genka') {
      rows = db.prepare(`
        SELECT m.商品コード, m.商品名, m.商品区分, m.標準売価, m.原価状態
        FROM m_products m
        WHERE m.取扱区分 = '取扱中' AND m.原価状態 IN ('MISSING','PARTIAL')
        ORDER BY m.商品区分, m.商品コード
      `).all();
      header = '商品コード,商品名,商品区分,標準売価,原価状態';
      filename = 'genka_missing.csv';
    } else if (type === 'sku_map') {
      rows = db.prepare('SELECT モール商品コード, 商品名, SUM(数量) as 数量, MAX(日付) as 最終日付 FROM unmapped_sales GROUP BY モール商品コード, 商品名 ORDER BY SUM(数量) DESC').all();
      header = 'モール商品コード,商品名,数量,最終日付';
      filename = 'sku_missing.csv';
    } else if (type === 'sales_class') {
      rows = db.prepare(`
        SELECT m.商品コード, m.商品名, m.商品区分, m.取扱区分, m.標準売価, m.原価, m.原価状態, m.売上分類
        FROM m_products m
        WHERE m.商品区分 = '単品' AND m.売上分類 IS NULL
        ORDER BY m.取扱区分, m.商品コード
      `).all();
      header = '商品コード,商品名,商品区分,取扱区分,標準売価,原価,原価状態,売上分類';
      filename = 'sales_class_missing.csv';
    } else {
      return res.status(400).json({ error: 'type パラメータが必要です（shipping / genka / sku_map / sales_class）' });
    }
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }

  // CSV生成（BOM付きUTF-8）
  const escapeCsv = v => {
    const s = String(v ?? '');
    return s.includes(',') || s.includes('"') || s.includes('\n') ? '"' + s.replace(/"/g, '""') + '"' : s;
  };
  const csvLines = [header];
  for (const row of rows) {
    csvLines.push(Object.values(row).map(escapeCsv).join(','));
  }
  const bom = '\uFEFF';
  const csv = bom + csvLines.join('\r\n');

  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
  res.send(csv);
});

// ─── GET /api/missing/counts ───
// m_productsベースの未登録件数（超軽量、5,000件テーブルからCOUNT）

router.get('/api/missing/counts', (req, res) => {
  const db = getDB();
  try {
    const shipping = db.prepare("SELECT COUNT(*) as cnt FROM m_products WHERE 取扱区分 = '取扱中' AND 送料 IS NULL").get().cnt;
    const genka = db.prepare("SELECT COUNT(*) as cnt FROM m_products WHERE 取扱区分 = '取扱中' AND 原価状態 IN ('MISSING','PARTIAL')").get().cnt;
    const sku_map = db.prepare("SELECT COUNT(*) as cnt FROM unmapped_sales").get().cnt;
    const sales_class = db.prepare("SELECT COUNT(*) as cnt FROM m_products WHERE 商品区分 = '単品' AND 売上分類 IS NULL").get().cnt;
    res.json({ shipping, genka, sku_map, sales_class });
  } catch {
    res.json({ shipping: 0, genka: 0, sku_map: 0, sales_class: 0 });
  }
});

// ─── GET /api/products/suggest ───
// 商品コードサジェスト（軽量、10件まで）

router.get('/api/products/suggest', (req, res) => {
  const q = (req.query.q || '').trim();
  if (!q || q.length < 2) return res.json([]);
  const term = `%${q}%`;
  const rows = execQuery(
    'SELECT 商品コード, 商品名, 原価, 売価, 取扱区分 FROM raw_ne_products WHERE (商品コード LIKE ? OR 商品名 LIKE ?) ORDER BY 取扱区分, 商品コード LIMIT 10',
    [term, term]
  );
  res.json(rows);
});

// ─── 登録専用ライトUI ───

router.get('/register', (req, res) => {
  const db = getDB();
  let shippingRates = [];
  try { shippingRates = db.prepare('SELECT shipping_code, 小分類区分名称, 配送関係費合計 FROM shipping_rates ORDER BY shipping_code').all(); } catch {}
  res.send(renderRegisterPage(shippingRates));
});

// ─── ダッシュボード（HTML）───

router.get('/', (req, res) => {
  const stats = getStats();
  res.send(renderDashboard(stats));
});

function renderRegisterPage(shippingRates) {
  const ratesJson = JSON.stringify(shippingRates);
  return `<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>マスタ登録 - Data Warehouse</title>
  <style>
    *{margin:0;padding:0;box-sizing:border-box}
    body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f5f5f5;color:#333;font-size:14px}
    .header{background:#1a5276;color:white;padding:12px 24px;display:flex;align-items:center;gap:16px}
    .header h1{font-size:18px}
    .header a{color:#aed6f1;text-decoration:none;font-size:13px}
    .header a:hover{color:white}
    .wrap{max-width:1100px;margin:16px auto;padding:0 16px}
    .card{background:white;border-radius:8px;padding:20px;margin-bottom:12px;box-shadow:0 1px 3px rgba(0,0,0,.1)}
    .card h2{font-size:15px;color:#555;margin-bottom:10px}
    .counts{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:12px}
    .cnt{padding:8px 16px;border-radius:6px;font-size:13px;cursor:pointer;border:2px solid transparent;transition:.15s}
    .cnt.active{border-color:#1a5276}
    .cnt-ship{background:#fadbd8;color:#c0392b}.cnt-genka{background:#fdebd0;color:#d35400}.cnt-sku{background:#d5f5e3;color:#27ae60}
    table{width:100%;border-collapse:collapse;font-size:13px}
    th{background:#f0f0f0;padding:7px 8px;text-align:left;position:sticky;top:0;z-index:1}
    td{padding:5px 8px;border-bottom:1px solid #eee}
    tr:hover{background:#f8f9fa}
    .pri-a{background:#e74c3c;color:white;padding:2px 6px;border-radius:3px;font-size:11px}
    .pri-b{background:#f39c12;color:white;padding:2px 6px;border-radius:3px;font-size:11px}
    .pri-c{color:#aaa;font-size:11px}
    .btn{padding:5px 14px;border:none;border-radius:4px;cursor:pointer;font-size:12px}
    .btn-p{background:#2980b9;color:white}.btn-p:hover{background:#1a6da0}
    .btn-d{background:#e74c3c;color:white}.btn-d:hover{background:#c0392b}
    .btn-s{background:#27ae60;color:white}.btn-s:hover{background:#1e8449}
    .btn:disabled{opacity:.5;cursor:default}
    input,select{padding:5px 8px;border:1px solid #ddd;border-radius:4px;font-size:13px}
    .row{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:8px}
    .meta{font-size:12px;color:#888;margin-top:6px}
    #toast{position:fixed;bottom:20px;right:20px;padding:12px 24px;background:#27ae60;color:white;border-radius:6px;display:none;z-index:999;font-size:14px}
    #toast.err{background:#e74c3c}
    .suggest-wrap{position:relative}
    .suggest-list{position:absolute;top:100%;left:0;background:white;border:1px solid #ddd;border-radius:4px;width:350px;max-height:200px;overflow-y:auto;z-index:10;display:none;box-shadow:0 4px 12px rgba(0,0,0,.15)}
    .suggest-list div{padding:6px 10px;cursor:pointer;font-size:12px;border-bottom:1px solid #f0f0f0}
    .suggest-list div:hover{background:#eaf2f8}
    .suggest-list .code{font-weight:600;color:#1a5276}
    .tabs{display:flex;gap:4px;margin-bottom:12px}
    .tabs button{padding:7px 14px;border:1px solid #ddd;background:white;cursor:pointer;border-radius:4px 4px 0 0;font-size:13px}
    .tabs button.active{background:#1a5276;color:white;border-color:#1a5276}
    .done-row{opacity:.4;transition:opacity .3s}
    .scroll-area{max-height:65vh;overflow-y:auto}
  </style>
</head>
<body>
  <div class="header">
    <h1>マスタ登録</h1>
    <a href="./">← ダッシュボードに戻る</a>
  </div>
  <div class="wrap">
    <!-- 未登録件数サマリ -->
    <div class="card">
      <div class="counts">
        <div class="cnt cnt-ship active" onclick="switchType('shipping',this)">送料未登録: <b id="c-ship">...</b></div>
        <div class="cnt cnt-genka" onclick="switchType('genka',this)">原価未登録: <b id="c-genka">...</b></div>
        <div class="cnt cnt-sku" onclick="switchType('sku_map',this)">SKU未登録: <b id="c-sku">...</b></div>
        <div class="cnt" style="background:#e8daef;color:#8e44ad" onclick="switchType('sales_class',this)">分類未登録: <b id="c-class">...</b></div>
      </div>
    </div>

    <!-- メイン: 未登録リスト -->
    <div class="card">
      <h2 id="list-title">送料未登録</h2>
      <div class="scroll-area">
        <table><thead id="table-head"></thead><tbody id="table-body"></tbody></table>
      </div>
      <div style="display:flex;justify-content:space-between;align-items:center;margin-top:6px">
        <div class="meta" id="list-meta"></div>
        <button class="btn btn-p" style="font-size:12px" onclick="downloadMissing()">CSVダウンロード</button>
        <button class="btn" style="font-size:11px;background:#7f8c8d;color:white" onclick="window.location.href=B+'/api/shipping_rates?format=csv'">送料コード一覧</button>
      </div>
    </div>

    <!-- マスタ検索・編集 -->
    <div class="card">
      <h2>登録済みマスタ検索</h2>
      <div class="tabs">
        <button class="active" onclick="switchManage('shipping',this)">送料</button>
        <button onclick="switchManage('genka',this)">原価</button>
        <button onclick="switchManage('skumap',this)">SKUマップ</button>
      </div>
      <div class="row">
        <input id="m-search" placeholder="商品コード or 商品名" style="width:280px" onkeydown="if(event.key==='Enter')searchManage()">
        <button class="btn btn-p" onclick="searchManage()">検索</button>
      </div>
      <div class="scroll-area">
        <table><thead id="m-head"></thead><tbody id="m-body"></tbody></table>
      </div>
      <div class="meta" id="m-meta"></div>
    </div>

    <!-- CSV一括アップロード -->
    <div class="card">
      <h2>CSV一括アップロード</h2>
      <div class="tabs">
        <button class="active" id="csv-tab-shipping" onclick="switchCsvType('shipping',this)">送料</button>
        <button id="csv-tab-genka" onclick="switchCsvType('genka',this)">原価</button>
        <button id="csv-tab-skumap" onclick="switchCsvType('skumap',this)">SKUマップ</button>
        <button id="csv-tab-salesclass" onclick="switchCsvType('sales_class',this)">売上分類</button>
      </div>
      <div id="csv-format" style="font-size:12px;color:#666;margin-bottom:8px"></div>
      <div class="row">
        <input type="file" id="csv-file" accept=".csv,.txt" style="font-size:13px">
        <button class="btn btn-s" id="csv-upload-btn" onclick="uploadCsv()">アップロード</button>
      </div>
      <div id="csv-result" class="meta"></div>
    </div>
  </div>

  <div id="toast"></div>

  <script>
    const B = location.pathname.replace(/\\/register$/, '');
    const RATES = ${ratesJson};
    let curType = 'shipping';
    let curManage = 'shipping';
    const he = s => (s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/"/g,'&quot;');

    // ── Toast ──
    function toast(msg, err) {
      const el = document.getElementById('toast');
      el.textContent = msg; el.className = err ? 'err' : ''; el.style.display = 'block';
      setTimeout(() => el.style.display = 'none', 2500);
    }

    async function api(path, opts) {
      const r = await fetch(B + path, opts);
      if (!r.ok) { const e = await r.json().catch(()=>({})); throw new Error(e.error || r.statusText); }
      return r.json();
    }

    // ── 配送区分ドロップダウンHTML ──
    function rateOptions(selected) {
      return RATES.map(r => {
        const v = r.shipping_code+'|'+he(r.小分類区分名称||'')+'|'+r.配送関係費合計;
        const sel = (selected && r.配送関係費合計 == selected) ? ' selected' : '';
        return '<option value="'+v+'"'+sel+'>'+(r.小分類区分名称||r.shipping_code)+' ('+r.配送関係費合計+'円)</option>';
      }).join('');
    }

    // ── 未登録タイプ切替 ──
    function switchType(type, el) {
      curType = type;
      document.querySelectorAll('.cnt').forEach(c => c.classList.remove('active'));
      if (el) el.classList.add('active');
      loadMissing();
    }

    // ── 未登録データ読み込み ──
    async function loadMissing() {
      const titles = { shipping: '送料未登録', genka: '原価未登録', sku_map: 'SKU未登録', sales_class: '分類未登録' };
      document.getElementById('list-title').textContent = titles[curType] || '';
      document.getElementById('table-body').innerHTML = '<tr><td colspan="8" style="text-align:center;padding:20px;color:#999">読み込み中...</td></tr>';

      const data = await api('/api/missing/prioritized?type=' + curType);
      const rows = data.rows || [];
      // ※ 件数バッジは上書きしない（初期表示の /api/missing/counts の値を維持）
      // prioritized は LIMIT 200 なので件数が不正確になるため

      // ヘッダー
      let head = '<tr><th>優先度</th><th>区分</th><th>商品コード</th><th>商品名</th><th>売価</th><th>7日</th><th>30日</th><th>最終販売</th><th style="min-width:220px">アクション</th></tr>';
      document.getElementById('table-head').innerHTML = head;

      // ボディ
      let html = '';
      for (const r of rows.slice(0, 150)) {
        const pri = r.priority === 'A_7日以内' ? '<span class="pri-a">7日</span>'
          : r.priority === 'B_30日以内' ? '<span class="pri-b">30日</span>'
          : '<span class="pri-c">-</span>';
        const typeBadge = r.商品区分 === 'セット' ? '<span style="background:#3498db;color:white;padding:1px 6px;border-radius:3px;font-size:11px">セット</span>'
          : r.商品区分 === '例外' ? '<span style="background:#9b59b6;color:white;padding:1px 6px;border-radius:3px;font-size:11px">例外</span>'
          : '<span style="color:#888;font-size:11px">単品</span>';
        html += '<tr id="row-'+he(r.商品コード)+'">';
        html += '<td>'+pri+'</td>';
        html += '<td>'+typeBadge+'</td>';
        html += '<td>'+he(r.商品コード)+'</td>';
        const statusBadge = r.取扱区分 && r.取扱区分 !== '取扱中' ? ' <span style="background:#e74c3c;color:white;padding:1px 5px;border-radius:3px;font-size:10px">'+he(r.取扱区分)+'</span>' : '';
        html += '<td>'+he((r.商品名||''))+statusBadge+'</td>';
        html += '<td>'+(r.売価||'-')+'</td>';
        html += '<td>'+(r.sales_7d||0)+'</td>';
        html += '<td>'+(r.sales_30d||0)+'</td>';
        html += '<td>'+(r.last_sold||'-')+'</td>';
        html += '<td>';
        if (curType === 'shipping') {
          html += '<select style="max-width:160px">'+rateOptions()+'</select> ';
          html += '<button class="btn btn-p" data-act="reg-ship" data-sku="'+he(r.商品コード)+'">登録</button>';
        } else if (curType === 'genka') {
          html += '<input placeholder="原価" style="width:80px" type="number" step="0.01"> ';
          html += '<button class="btn btn-p" data-act="reg-genka" data-sku="'+he(r.商品コード)+'" data-name="'+he(r.商品名)+'">登録</button>';
        } else if (curType === 'sales_class') {
          html += '<select style="width:120px"><option value="">--</option><option value="1">1:自社商品</option><option value="2">2:取扱限定</option><option value="3">3:仕入れ</option><option value="4">4:輸出</option></select> ';
          html += '<button class="btn btn-p" data-act="reg-class" data-sku="'+he(r.商品コード)+'" data-name="'+he(r.商品名)+'">登録</button>';
        } else {
          html += '<div class="sku-mapping-rows" data-seller-sku="'+he(r.商品コード)+'" data-name="'+he(r.商品名)+'">';
          html += '<div class="sku-row" style="display:flex;gap:4px;align-items:center;margin-bottom:3px">';
          html += '<div class="suggest-wrap" style="display:inline-block"><input placeholder="NE商品コード" style="width:130px" data-suggest="1" autocomplete="off"><div class="suggest-list"></div></div>';
          html += '<input placeholder="数量" style="width:45px" type="number" value="1" min="1">';
          html += '</div></div>';
          html += '<button class="btn btn-s" style="font-size:11px;padding:2px 8px;margin-right:4px" data-act="add-sku-row" data-sku="'+he(r.商品コード)+'">+構成品</button>';
          html += '<button class="btn btn-p" data-act="reg-sku-multi" data-sku="'+he(r.商品コード)+'" data-name="'+he(r.商品名)+'">登録</button>';
        }
        html += '</td></tr>';
      }
      if (!rows.length) html = '<tr><td colspan="9" style="text-align:center;padding:20px;color:#27ae60">全て登録済み ✓</td></tr>';
      document.getElementById('table-body').innerHTML = html;
      const totalMap = { shipping: 'c-ship', genka: 'c-genka', sku_map: 'c-sku' };
      const totalEl = document.getElementById(totalMap[curType]);
      const totalCount = totalEl ? totalEl.textContent : '?';
      document.getElementById('list-meta').textContent = rows.length >= 200 ? rows.length + '件表示 / 全' + totalCount + '件' : rows.length + '件';
    }

    // ── イベント委譲（登録ボタン）──
    document.addEventListener('click', async ev => {
      const btn = ev.target.closest('[data-act]');
      if (!btn) return;
      const act = btn.dataset.act;
      const sku = btn.dataset.sku;
      const name = btn.dataset.name || '';
      const tr = btn.closest('tr');
      btn.disabled = true;

      try {
        if (act === 'reg-ship') {
          const sel = tr.querySelector('select');
          const v = sel.value.split('|');
          await api('/api/shipping', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({sku,shipping_code:v[0],ship_method:v[1],ship_cost:v[2]})});
          toast(sku+' の送料を登録');
          tr.classList.add('done-row');
          setTimeout(() => tr.remove(), 600);
          updateCount('shipping', -1);
        } else if (act === 'reg-genka') {
          const inp = tr.querySelector('input');
          if (!inp.value) { btn.disabled=false; return; }
          await api('/api/genka', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({sku,genka:inp.value,product_name:name})});
          toast(sku+' の原価を登録');
          tr.classList.add('done-row');
          setTimeout(() => tr.remove(), 600);
          updateCount('genka', -1);
        } else if (act === 'reg-class') {
          const sel = tr.querySelector('select');
          if (!sel?.value) { btn.disabled=false; return; }
          await api('/api/sales_class', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({sku,sales_class:sel.value,product_name:name})});
          toast(sku+' の売上分類を登録');
          tr.classList.add('done-row');
          setTimeout(() => tr.remove(), 600);
          updateCount('sales_class', -1);
        } else if (act === 'remove-sku-row') {
          btn.closest('.sku-row')?.remove();
          btn.disabled = false;
          return;
        } else if (act === 'add-sku-row') {
          // +構成品ボタン: 入力行を追加
          const container = tr.querySelector('.sku-mapping-rows');
          if (!container) { btn.disabled=false; return; }
          const newRow = document.createElement('div');
          newRow.className = 'sku-row';
          newRow.style = 'display:flex;gap:4px;align-items:center;margin-bottom:3px';
          newRow.innerHTML = '<div class="suggest-wrap" style="display:inline-block"><input placeholder="NE商品コード" style="width:130px" data-suggest="1" autocomplete="off"><div class="suggest-list"></div></div><input placeholder="数量" style="width:45px" type="number" value="1" min="1"><button class="btn" style="font-size:10px;padding:1px 5px;background:#e74c3c;color:white" data-act="remove-sku-row">×</button>';
          container.appendChild(newRow);
          btn.disabled = false;
          return;
        } else if (act === 'reg-sku-multi') {
          // 複数NE商品コード一括登録
          const container = tr.querySelector('.sku-mapping-rows');
          if (!container) { btn.disabled=false; return; }
          const rows = container.querySelectorAll('.sku-row');
          let registered = 0;
          for (const row of rows) {
            const neInput = row.querySelector('input[data-suggest]');
            const qtyInput = row.querySelectorAll('input')[1];
            const neCode = neInput?.value?.trim();
            if (!neCode) continue;
            const qty = parseInt(qtyInput?.value) || 1;
            await api('/api/skumap', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({seller_sku:sku,ne_code:neCode,product_name:name,quantity:qty})});
            registered++;
          }
          if (registered === 0) { btn.disabled=false; return; }
          toast(sku+' のSKUマップを'+registered+'件登録');
          tr.classList.add('done-row');
          setTimeout(() => tr.remove(), 600);
          updateCount('sku_map', -1);
        } else if (act === 'reg-sku') {
          // 旧形式の互換性（マスタ管理画面等から）
          const inp = tr.querySelector('input[data-suggest]');
          if (!inp || !inp.value) { btn.disabled=false; return; }
          await api('/api/skumap', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({seller_sku:sku,ne_code:inp.value,product_name:name})});
          toast(sku+' のSKUマップを登録');
          tr.classList.add('done-row');
          setTimeout(() => tr.remove(), 600);
          updateCount('sku_map', -1);
        } else if (act === 'update-ship') {
          const sel = tr.querySelector('select');
          const v = sel.value.split('|');
          await api('/api/shipping', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({sku,shipping_code:v[0],ship_method:v[1],ship_cost:v[2]})});
          toast(sku+' の送料を更新');
        } else if (act === 'update-genka') {
          const inp = tr.querySelector('input');
          await api('/api/genka', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({sku,genka:inp.value,product_name:name})});
          toast(sku+' の原価を更新');
        } else if (act === 'update-sku') {
          const inputs = tr.querySelectorAll('input');
          const oldNe = btn.dataset.ne || '';
          if (oldNe && oldNe !== inputs[0].value) {
            await api('/api/skumap/'+encodeURIComponent(sku)+'?ne_code='+encodeURIComponent(oldNe), {method:'DELETE'});
          }
          await api('/api/skumap', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({seller_sku:sku,ne_code:inputs[0].value,product_name:name,quantity:inputs[1]?.value||1})});
          toast(sku+' のSKUマップを更新');
        } else if (act === 'del') {
          if (!confirm(sku+' を削除しますか？')) { btn.disabled=false; return; }
          const type = btn.dataset.type;
          const ne = btn.dataset.ne || '';
          let url = '/api/'+(type==='skumap'?'skumap':type)+'/'+encodeURIComponent(sku);
          if (type === 'skumap' && ne) url += '?ne_code='+encodeURIComponent(ne);
          await api(url, {method:'DELETE'});
          toast(sku+' を削除');
          tr.remove();
        }
      } catch(e) { toast('エラー: '+e.message, true); btn.disabled=false; }
    });

    function updateCount(type, delta) {
      const map = {shipping:'c-ship',genka:'c-genka',sku_map:'c-sku',sales_class:'c-class'};
      const el = document.getElementById(map[type]);
      if (el) el.textContent = Math.max(0, parseInt(el.textContent||'0') + delta);
    }

    // ── NE商品コードサジェスト ──
    let suggestTimer = null;
    document.addEventListener('input', ev => {
      const inp = ev.target;
      if (!inp.dataset?.suggest) return;
      clearTimeout(suggestTimer);
      const wrap = inp.closest('.suggest-wrap');
      const list = wrap?.querySelector('.suggest-list');
      if (!list) return;
      const q = inp.value.trim();
      if (q.length < 2) { list.style.display = 'none'; return; }
      suggestTimer = setTimeout(async () => {
        try {
          const rows = await api('/api/products/suggest?q='+encodeURIComponent(q));
          if (!rows.length) { list.style.display = 'none'; return; }
          list.innerHTML = rows.map(r =>
            '<div data-code="'+he(r.商品コード)+'"><span class="code">'+he(r.商品コード)+'</span> '+he((r.商品名||'').slice(0,30))+' <span style="color:#888;font-size:11px">'+(r.原価||'-')+'円</span></div>'
          ).join('');
          list.style.display = 'block';
        } catch { list.style.display = 'none'; }
      }, 250);
    });
    document.addEventListener('click', ev => {
      const item = ev.target.closest('.suggest-list div');
      if (item) {
        const code = item.dataset.code;
        const wrap = item.closest('.suggest-wrap');
        const inp = wrap?.querySelector('input');
        if (inp) inp.value = code;
        item.closest('.suggest-list').style.display = 'none';
        return;
      }
      document.querySelectorAll('.suggest-list').forEach(l => l.style.display = 'none');
    });

    // ── マスタ検索 ──
    function switchManage(type, el) {
      curManage = type;
      document.querySelectorAll('.tabs button').forEach(b => b.classList.remove('active'));
      if (el) el.classList.add('active');
      document.getElementById('m-body').innerHTML = '';
      document.getElementById('m-head').innerHTML = '';
      document.getElementById('m-meta').textContent = '';
    }
    async function searchManage() {
      const search = document.getElementById('m-search').value;
      const ep = curManage === 'shipping' ? '/api/shipping/list' : curManage === 'genka' ? '/api/genka/list' : '/api/skumap/list';
      const data = await api(ep + '?search=' + encodeURIComponent(search) + '&limit=100');
      const rows = data.rows || [];
      let head = '', html = '';

      if (curManage === 'shipping') {
        head = '<tr><th>商品コード</th><th>商品名</th><th>配送方法</th><th>送料</th><th>操作</th></tr>';
        for (const r of rows) {
          html += '<tr><td>'+he(r.sku)+'</td><td>'+he((r.product_name||'').slice(0,30))+'</td>';
          html += '<td><select>'+rateOptions(r.ship_cost)+'</select></td>';
          html += '<td>'+r.ship_cost+'円</td>';
          html += '<td><button class="btn btn-p" data-act="update-ship" data-sku="'+he(r.sku)+'">更新</button> <button class="btn btn-d" data-act="del" data-type="shipping" data-sku="'+he(r.sku)+'">削除</button></td></tr>';
        }
      } else if (curManage === 'genka') {
        head = '<tr><th>SKU</th><th>商品名</th><th>原価</th><th>操作</th></tr>';
        for (const r of rows) {
          html += '<tr><td>'+he(r.sku)+'</td><td>'+he((r.商品名||'').slice(0,30))+'</td>';
          html += '<td><input value="'+r.genka+'" style="width:80px" type="number" step="0.01"></td>';
          html += '<td><button class="btn btn-p" data-act="update-genka" data-sku="'+he(r.sku)+'" data-name="'+he(r.商品名)+'">更新</button> <button class="btn btn-d" data-act="del" data-type="genka" data-sku="'+he(r.sku)+'">削除</button></td></tr>';
        }
      } else {
        head = '<tr><th>Amazon SKU</th><th>ASIN</th><th>商品名</th><th>NE商品コード</th><th>数量</th><th>操作</th></tr>';
        for (const r of rows) {
          html += '<tr><td>'+he(r.seller_sku)+'</td><td>'+he(r.asin||'')+'</td><td>'+he((r.商品名||'').slice(0,25))+'</td>';
          html += '<td><input value="'+he(r.ne_code||'')+'" style="width:120px"></td>';
          html += '<td><input value="'+(r.数量||1)+'" style="width:40px" type="number"></td>';
          html += '<td><button class="btn btn-p" data-act="update-sku" data-sku="'+he(r.seller_sku)+'" data-ne="'+he(r.ne_code)+'" data-name="'+he(r.商品名)+'">更新</button> <button class="btn btn-d" data-act="del" data-type="skumap" data-sku="'+he(r.seller_sku)+'" data-ne="'+he(r.ne_code)+'">削除</button></td></tr>';
        }
      }
      if (!rows.length) html = '<tr><td colspan="6" style="text-align:center;padding:20px;color:#999">該当なし</td></tr>';
      document.getElementById('m-head').innerHTML = head;
      document.getElementById('m-body').innerHTML = html;
      document.getElementById('m-meta').textContent = (data.total||0) + '件';
    }

    // ── 初期読込（2段階: 送料・原価は即表示、SKUは遅延取得）──
    (async () => {
      document.getElementById('table-body').innerHTML = '<tr><td colspan="8" style="text-align:center;padding:30px;color:#888">上のタブをクリックすると未登録データを読み込みます</td></tr>';
      // 1段目: 送料・原価のカウント（軽い、即表示）
      try {
        const c = await api('/api/missing/counts?fast=1');
        document.getElementById('c-ship').textContent = c.shipping || 0;
        document.getElementById('c-genka').textContent = c.genka || 0;
      } catch(e) { console.error(e); }
      // 2段目: 全カウント
      api('/api/missing/counts').then(c => {
        document.getElementById('c-sku').textContent = c.sku_map || 0;
        document.getElementById('c-class').textContent = c.sales_class || 0;
      }).catch(() => { document.getElementById('c-sku').textContent = '-'; document.getElementById('c-class').textContent = '-'; });
    })();

    // ── CSV一括アップロード ──
    let curCsvType = 'shipping';
    const csvFormats = {
      shipping: 'CSV形式: 商品コード, 送料コード',
      genka: 'CSV形式: 商品コード, 原価, 商品名（任意）',
      skumap: 'CSV形式: seller_sku, ne_code, 数量（任意）, ASIN（任意）',
      sales_class: 'CSV形式: 商品コード, 売上分類(1:自社/2:取扱限定/3:仕入れ/4:輸出)'
    };
    function switchCsvType(type, el) {
      curCsvType = type;
      document.querySelectorAll('.card:last-of-type .tabs button').forEach(b => b.classList.remove('active'));
      if (el) el.classList.add('active');
      document.getElementById('csv-format').textContent = csvFormats[type] || '';
      document.getElementById('csv-result').textContent = '';
    }
    switchCsvType('shipping', document.getElementById('csv-tab-shipping'));

    function downloadMissing() {
      window.location.href = B + '/api/missing/download?type=' + curType;
    }

    async function uploadCsv() {
      const fileInput = document.getElementById('csv-file');
      if (!fileInput.files.length) { toast('ファイルを選択してください', true); return; }
      const btn = document.getElementById('csv-upload-btn');
      btn.disabled = true;
      btn.textContent = 'アップロード中...';
      const formData = new FormData();
      formData.append('file', fileInput.files[0]);
      try {
        const endpoint = '/api/csv/' + curCsvType;
        const r = await fetch(B + endpoint, { method: 'POST', body: formData });
        const data = await r.json();
        if (data.ok) {
          document.getElementById('csv-result').textContent = '✅ ' + data.imported + '件登録 / ' + data.skipped + '件スキップ（全' + data.total + '行）';
          toast(data.imported + '件を一括登録しました');
          // 件数を再取得
          try {
            const c = await api('/api/missing/counts');
            document.getElementById('c-ship').textContent = c.shipping || 0;
            document.getElementById('c-genka').textContent = c.genka || 0;
            document.getElementById('c-sku').textContent = c.sku_map || 0;
          } catch {}
        } else {
          document.getElementById('csv-result').textContent = '❌ エラー: ' + (data.error || '不明');
          toast('アップロード失敗', true);
        }
      } catch(e) {
        document.getElementById('csv-result').textContent = '❌ エラー: ' + e.message;
        toast('アップロード失敗', true);
      }
      btn.disabled = false;
      btn.textContent = 'アップロード';
      fileInput.value = '';
    }
  </script>
</body>
</html>`;
}

function renderDashboard(stats) {
  // 未登録データ件数は重いのでダッシュボード初期表示では取得しない（JSで非同期取得）
  const missingCounts = {};

  // 配送区分一覧（フォーム用、軽い）
  const db = getDB();
  let shippingRates = [];
  try { shippingRates = db.prepare('SELECT shipping_code, 小分類区分名称, 配送関係費合計 FROM shipping_rates ORDER BY shipping_code').all(); } catch {}

  return `<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>データウェアハウス - B-Faith</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #f5f5f5; color: #333; font-size: 14px; }
    .header { background: #1a5276; color: white; padding: 16px 24px; display: flex; align-items: center; gap: 24px; }
    .header h1 { font-size: 20px; }
    .header nav a { color: #aed6f1; text-decoration: none; font-size: 13px; }
    .header nav a:hover { color: white; }
    .container { max-width: 1200px; margin: 24px auto; padding: 0 24px; }
    .card { background: white; border-radius: 8px; padding: 24px; margin-bottom: 16px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
    .card h2 { font-size: 16px; color: #555; margin-bottom: 12px; }
    .stats { display: flex; gap: 12px; flex-wrap: wrap; }
    .stat { background: #f8f9fa; border-radius: 6px; padding: 12px; text-align: center; min-width: 120px; }
    .stat .label { font-size: 11px; color: #888; }
    .stat .value { font-size: 24px; font-weight: 700; color: #1a5276; }
    .stat.warn .value { color: #c0392b; }
    table.data { width: 100%; border-collapse: collapse; font-size: 13px; }
    table.data th { background: #f0f0f0; padding: 8px; text-align: left; position: sticky; top: 0; }
    table.data td { padding: 6px 8px; border-bottom: 1px solid #eee; }
    table.data tr:hover { background: #f8f9fa; }
    .badge { display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 600; }
    .badge.shipping { background: #fadbd8; color: #c0392b; }
    .badge.genka { background: #fdebd0; color: #d35400; }
    .badge.sku_map { background: #d5f5e3; color: #27ae60; }
    .btn { padding: 6px 16px; border: none; border-radius: 4px; cursor: pointer; font-size: 13px; }
    .btn-primary { background: #2980b9; color: white; }
    .btn-primary:hover { background: #1a6da0; }
    .btn-sm { padding: 3px 10px; font-size: 12px; }
    input, select { padding: 6px 10px; border: 1px solid #ddd; border-radius: 4px; font-size: 13px; }
    .form-row { display: flex; gap: 8px; align-items: center; margin-bottom: 8px; flex-wrap: wrap; }
    .tab-nav { display: flex; gap: 4px; margin-bottom: 16px; }
    .tab-nav button { padding: 8px 16px; border: 1px solid #ddd; background: white; cursor: pointer; border-radius: 4px 4px 0 0; }
    .tab-nav button.active { background: #1a5276; color: white; border-color: #1a5276; }
    .tab-content { display: none; }
    .tab-content.active { display: block; }
    .meta { font-size: 12px; color: #888; margin-top: 8px; }
    #toast { position: fixed; bottom: 20px; right: 20px; padding: 12px 24px; background: #27ae60; color: white; border-radius: 6px; display: none; z-index: 999; }
  </style>
</head>
<body>
  <div class="header">
    <h1>Data Warehouse</h1>
    <nav>
      <a href="#" onclick="showTab('overview')">概要</a> |
      <a href="#" onclick="showTab('missing')">未登録 (<span id="nav-missing-total">...</span>)</a> |
      <a href="#" onclick="showTab('manage')">マスタ管理</a> |
      <a href="#" onclick="showTab('master')">商品マスタ</a> |
      <a href="#" onclick="showTab('sales')">売上分析</a>
    </nav>
  </div>
  <div class="container">

    <!-- 概要タブ -->
    <div id="tab-overview" class="tab-content active">
      <div class="card">
        <h2>DB統計</h2>
        <div class="stats">
          <div class="stat"><div class="label">NE商品</div><div class="value">${stats.raw_ne_products||0}</div></div>
          <div class="stat"><div class="label">NE受注</div><div class="value">${(stats.raw_ne_orders||0).toLocaleString()}</div></div>
          <div class="stat"><div class="label">Amazon注文</div><div class="value">${(stats.raw_sp_orders||0).toLocaleString()}</div></div>
          <div class="stat"><div class="label">楽天注文</div><div class="value">${(stats.raw_rakuten_orders||0).toLocaleString()}</div></div>
          <div class="stat"><div class="label">ロジザード</div><div class="value">${stats.raw_lz_inventory||0}</div></div>
          <div class="stat"><div class="label">SKUマップ</div><div class="value">${stats.sku_map||0}</div></div>
        </div>
        <div class="stats" style="margin-top:12px">
          <div class="stat warn"><div class="label">送料未登録</div><div class="value" id="missing-shipping">...</div></div>
          <div class="stat warn"><div class="label">原価未登録</div><div class="value" id="missing-genka">...</div></div>
          <div class="stat warn"><div class="label">SKU未登録</div><div class="value" id="missing-sku">...</div></div>
        </div>
        <div class="meta">
          NE受注: ${stats.ne_order_date_range ? stats.ne_order_date_range.min?.slice(0,10) + ' ~ ' + stats.ne_order_date_range.max?.slice(0,10) : '-'}
          | Amazon: ${stats.sp_order_date_range ? stats.sp_order_date_range.min?.slice(0,10) + ' ~ ' + stats.sp_order_date_range.max?.slice(0,10) : '-'}
        </div>
      </div>
    </div>

    <!-- 未登録タブ -->
    <div id="tab-missing" class="tab-content">
      <div class="card">
        <h2>未登録データ</h2>
        <div class="tab-nav" id="missing-tabs">
          <button class="active" onclick="loadMissing('shipping', this)">送料未登録 (<span id="mc-shipping">...</span>)</button>
          <button onclick="loadMissing('genka', this)">原価未登録 (<span id="mc-genka">...</span>)</button>
          <button onclick="loadMissing('sku_map', this)">SKU未登録 (<span id="mc-sku">...</span>)</button>
        </div>
        <div id="missing-list"></div>
      </div>
    </div>

    <!-- マスタ管理タブ -->
    <div id="tab-manage" class="tab-content">
      <div class="card">
        <h2>マスタ管理（検索・編集・削除）</h2>
        <div class="tab-nav">
          <button class="active" onclick="loadManage('shipping', this)">送料マスタ (${stats.product_shipping||0})</button>
          <button onclick="loadManage('genka', this)">特殊原価 (${stats.exception_genka||0})</button>
          <button onclick="loadManage('skumap', this)">SKUマップ (${stats.sku_map||0})</button>
        </div>
        <div class="form-row">
          <input id="manage-search" placeholder="商品コード or 商品名で検索" style="width:300px">
          <button class="btn btn-primary" onclick="loadManage(currentManageType)">検索</button>
        </div>
        <div id="manage-list"></div>
      </div>
    </div>

    <!-- 商品マスタタブ -->
    <div id="tab-master" class="tab-content">
      <div class="card">
        <h2>統合商品マスタ</h2>
        <div class="form-row">
          <input id="master-search" placeholder="商品コード or 商品名" style="width:300px">
          <select id="master-status"><option value="">全て</option><option value="取扱中" selected>取扱中</option><option value="取扱中止">取扱中止</option></select>
          <select id="master-shipping"><option value="">送料全て</option><option value="0">送料なし</option><option value="1">送料あり</option></select>
          <button class="btn btn-primary" onclick="loadMaster()">検索</button>
        </div>
        <div id="master-list"></div>
      </div>
    </div>

    <!-- 売上分析タブ -->
    <div id="tab-sales" class="tab-content">
      <div class="card">
        <h2>モール別月次売上</h2>
        <div id="sales-monthly"></div>
      </div>
      <div class="card">
        <h2>商品別売上</h2>
        <div class="form-row">
          <input id="sales-product" placeholder="商品コード">
          <select id="sales-platform"><option value="">全モール</option><option value="amazon">Amazon</option><option value="rakuten">楽天</option><option value="yahoo">Yahoo</option></select>
          <input id="sales-month" placeholder="月 (例: 2026-03)" value="${new Date().toISOString().slice(0,7)}">
          <button class="btn btn-primary" onclick="loadSales()">検索</button>
        </div>
        <div id="sales-list"></div>
      </div>
    </div>
  </div>

  <div id="toast"></div>

  <script>
    const BASE = location.pathname.replace(/\\/$/, '');
    let RATES = [];

    // 初期表示時にRATES + 未登録件数を非同期取得
    (async function() {
      try { RATES = await api('/api/shipping_rates'); } catch {}
      try {
        const data = await api('/api/missing');
        const counts = {};
        for (const s of (data.summary || [])) counts[s.missing_type] = s.cnt;
        document.getElementById('missing-shipping').textContent = counts.shipping || 0;
        document.getElementById('missing-genka').textContent = counts.genka || 0;
        document.getElementById('missing-sku').textContent = counts.sku_map || 0;
        document.getElementById('nav-missing-total').textContent = (counts.shipping||0) + (counts.genka||0) + (counts.sku_map||0);
      } catch {}
    })();

    let currentManageType = 'shipping';

    function showTab(name) {
      document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
      document.getElementById('tab-' + name).classList.add('active');
      if (name === 'missing') loadMissing('shipping');
      if (name === 'manage') loadManage('shipping');
      if (name === 'master') loadMaster();
      if (name === 'sales') { loadMonthlySales(); loadSales(); }
    }

    function toast(msg) {
      const el = document.getElementById('toast');
      el.textContent = msg; el.style.display = 'block';
      setTimeout(() => el.style.display = 'none', 3000);
    }

    async function api(path, opts) {
      const res = await fetch(BASE + path, opts);
      return res.json();
    }

    // イベント委譲（data属性でパラメータを渡す。onclickの文字列エスケープ問題を根本解決）
    document.addEventListener('click', async (ev) => {
      const btn = ev.target.closest('[data-action]');
      if (!btn) return;
      const action = btn.dataset.action;
      const sku = btn.dataset.sku || '';
      const name = btn.dataset.name || '';
      const type = btn.dataset.type || '';

      if (action === 'reg-shipping') {
        const sel = btn.closest('tr')?.querySelector('select');
        if (!sel) return;
        const val = sel.value.split('|');
        await api('/api/shipping', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ sku, shipping_code: val[0], ship_method: val[1], ship_cost: val[2] }) });
        toast(sku + ' の送料を登録しました');
        loadMissing(currentMissingType);
      } else if (action === 'reg-genka') {
        const inp = btn.closest('tr')?.querySelector('input');
        if (!inp || !inp.value) return;
        await api('/api/genka', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ sku, genka: inp.value, product_name: name }) });
        toast(sku + ' の原価を登録しました');
        loadMissing(currentMissingType);
      } else if (action === 'reg-skumap') {
        const inp = btn.closest('tr')?.querySelector('input');
        if (!inp || !inp.value) return;
        await api('/api/skumap', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ seller_sku: sku, ne_code: inp.value, product_name: name }) });
        toast(sku + ' のSKUマップを登録しました');
        loadMissing(currentMissingType);
      } else if (action === 'update-shipping') {
        const sel = btn.closest('tr')?.querySelector('select');
        if (!sel) return;
        const val = sel.value.split('|');
        await api('/api/shipping', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ sku, shipping_code: val[0], ship_method: val[1], ship_cost: val[2] }) });
        toast(sku + ' の送料を更新しました');
        loadManage(currentManageType);
      } else if (action === 'update-genka') {
        const inp = btn.closest('tr')?.querySelector('input');
        if (!inp) return;
        await api('/api/genka', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ sku, genka: inp.value, product_name: name }) });
        toast(sku + ' の原価を更新しました');
        loadManage(currentManageType);
      } else if (action === 'update-skumap') {
        const inputs = btn.closest('tr')?.querySelectorAll('input');
        if (!inputs || inputs.length < 2) return;
        const oldNe = btn.dataset.ne || '';
        // 古いレコードを削除してから新しいのを挿入（ne_codeが変わる可能性）
        if (oldNe && oldNe !== inputs[0].value) {
          await api('/api/skumap/' + encodeURIComponent(sku) + '?ne_code=' + encodeURIComponent(oldNe), { method: 'DELETE' });
        }
        await api('/api/skumap', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ seller_sku: sku, ne_code: inputs[0].value, product_name: name, quantity: inputs[1].value }) });
        toast(sku + ' のSKUマップを更新しました');
        loadManage(currentManageType);
      } else if (action === 'delete') {
        if (!confirm(sku + ' を削除しますか？')) return;
        const ne = btn.dataset.ne || '';
        let endpoint = type === 'shipping' ? '/api/shipping/' : type === 'genka' ? '/api/genka/' : '/api/skumap/';
        let url = endpoint + encodeURIComponent(sku);
        if (type === 'skumap' && ne) url += '?ne_code=' + encodeURIComponent(ne);
        await api(url, { method: 'DELETE' });
        toast(sku + ' を削除しました');
        loadManage(currentManageType);
      }
    });

    let currentMissingType = 'shipping';

    // 未登録データ
    async function loadMissing(type, btn) {
      currentMissingType = type;
      if (btn) { document.querySelectorAll('#tab-missing .tab-nav button').forEach(b => b.classList.remove('active')); btn.classList.add('active'); }
      // タブ件数を非同期更新（全type分のsummary取得）
      api('/api/missing/prioritized').then(all => {
        if (all.summary) {
          const s = all.summary;
          const el1 = document.getElementById('mc-shipping'); if (el1) el1.textContent = s.shipping || 0;
          const el2 = document.getElementById('mc-genka'); if (el2) el2.textContent = s.genka || 0;
          const el3 = document.getElementById('mc-sku'); if (el3) el3.textContent = s.sku_map || 0;
        }
      }).catch(() => {});
      const data = await api('/api/missing/prioritized?type=' + type);
      const rows = data.rows || [];
      let html = '<table class="data"><tr><th>優先度</th><th>商品コード</th><th>商品名</th><th>売価</th><th>7日/30日</th><th>最終販売</th><th>アクション</th></tr>';
      for (const r of rows.slice(0, 100)) {
        const pb = r.priority === 'A_7日以内' ? '<span style="background:#e74c3c;color:white;padding:2px 6px;border-radius:3px;font-size:11px">7日</span>'
          : r.priority === 'B_30日以内' ? '<span style="background:#f39c12;color:white;padding:2px 6px;border-radius:3px;font-size:11px">30日</span>'
          : '<span style="color:#aaa;font-size:11px">-</span>';
        const sb = (r.sales_7d||0) + '/' + (r.sales_30d||0);
        const he = (s) => (s||'').replace(/&/g,'&amp;').replace(/"/g,'&quot;');
        html += '<tr><td>' + pb + '</td><td>' + he(r.商品コード) + '</td><td>' + he((r.商品名||'').slice(0,30)) + '</td><td>' + (r.売価||'-') + '</td><td>' + sb + '</td><td>' + (r.last_sold||'-') + '</td><td>';
        if (type === 'shipping') {
          html += '<select>' + RATES.map(rt => '<option value="' + rt.shipping_code + '|' + he(rt.小分類区分名称||'') + '|' + rt.配送関係費合計 + '">' + (rt.小分類区分名称||rt.shipping_code) + ' (' + rt.配送関係費合計 + '円)</option>').join('') + '</select> ';
          html += '<button class="btn btn-sm btn-primary" data-action="reg-shipping" data-sku="' + he(r.商品コード) + '">登録</button>';
        } else if (type === 'genka') {
          html += '<input placeholder="原価" style="width:80px"> ';
          html += '<button class="btn btn-sm btn-primary" data-action="reg-genka" data-sku="' + he(r.商品コード) + '" data-name="' + he(r.商品名) + '">登録</button>';
        } else if (type === 'sku_map') {
          html += '<input placeholder="NE商品コード" style="width:120px"> ';
          html += '<button class="btn btn-sm btn-primary" data-action="reg-skumap" data-sku="' + he(r.商品コード) + '" data-name="' + he(r.商品名) + '">登録</button>';
        }
        html += '</td></tr>';
      }
      html += '</table>';
      if (rows.length > 100) html += '<div class="meta">' + rows.length + '件中100件表示</div>';
      document.getElementById('missing-list').innerHTML = html;
    }

    // マスタ管理（CRUD）
    async function loadManage(type, btn) {
      currentManageType = type;
      if (btn) { document.querySelectorAll('#tab-manage .tab-nav button').forEach(b => b.classList.remove('active')); btn.classList.add('active'); }
      const search = document.getElementById('manage-search')?.value || '';
      const endpoint = type === 'shipping' ? '/api/shipping/list' : type === 'genka' ? '/api/genka/list' : '/api/skumap/list';
      const data = await api(endpoint + '?search=' + encodeURIComponent(search) + '&limit=100');
      const rows = data.rows || [];
      const he = (s) => (s||'').replace(/&/g,'&amp;').replace(/"/g,'&quot;');
      let html = '<div class="meta">' + (data.total||0) + '件</div>';

      if (type === 'shipping') {
        html += '<table class="data"><tr><th>商品コード</th><th>商品名</th><th>配送方法</th><th>送料</th><th>操作</th></tr>';
        for (const r of rows) {
          html += '<tr><td>' + he(r.sku) + '</td><td>' + he((r.product_name||'').slice(0,30)) + '</td>';
          html += '<td><select>' + RATES.map(rt => '<option value="' + rt.shipping_code + '|' + he(rt.小分類区分名称||'') + '|' + rt.配送関係費合計 + '"' + (rt.配送関係費合計 == r.ship_cost ? ' selected' : '') + '>' + (rt.小分類区分名称||rt.shipping_code) + ' (' + rt.配送関係費合計 + '円)</option>').join('') + '</select></td>';
          html += '<td>' + r.ship_cost + '円</td>';
          html += '<td><button class="btn btn-sm btn-primary" data-action="update-shipping" data-sku="' + he(r.sku) + '">更新</button> <button class="btn btn-sm" style="background:#e74c3c;color:white" data-action="delete" data-type="shipping" data-sku="' + he(r.sku) + '">削除</button></td></tr>';
        }
      } else if (type === 'genka') {
        html += '<table class="data"><tr><th>SKU</th><th>商品名</th><th>原価</th><th>操作</th></tr>';
        for (const r of rows) {
          html += '<tr><td>' + he(r.sku) + '</td><td>' + he((r.商品名||'').slice(0,30)) + '</td>';
          html += '<td><input value="' + r.genka + '" style="width:80px"></td>';
          html += '<td><button class="btn btn-sm btn-primary" data-action="update-genka" data-sku="' + he(r.sku) + '" data-name="' + he(r.商品名) + '">更新</button> <button class="btn btn-sm" style="background:#e74c3c;color:white" data-action="delete" data-type="genka" data-sku="' + he(r.sku) + '">削除</button></td></tr>';
        }
      } else if (type === 'skumap') {
        html += '<table class="data"><tr><th>Amazon SKU</th><th>ASIN</th><th>商品名</th><th>NE商品コード</th><th>数量</th><th>操作</th></tr>';
        for (const r of rows) {
          html += '<tr><td>' + he(r.seller_sku) + '</td><td>' + he(r.asin||'') + '</td><td>' + he((r.商品名||'').slice(0,25)) + '</td>';
          html += '<td><input value="' + he(r.ne_code||'') + '" style="width:120px"></td>';
          html += '<td><input value="' + (r.数量||1) + '" style="width:40px"></td>';
          html += '<td><button class="btn btn-sm btn-primary" data-action="update-skumap" data-sku="' + he(r.seller_sku) + '" data-ne="' + he(r.ne_code) + '" data-name="' + he(r.商品名) + '">更新</button> <button class="btn btn-sm" style="background:#e74c3c;color:white" data-action="delete" data-type="skumap" data-sku="' + he(r.seller_sku) + '" data-ne="' + he(r.ne_code) + '">削除</button></td></tr>';
        }
      }
      html += '</table>';
      document.getElementById('manage-list').innerHTML = html;
    }

    // 商品マスタ
    async function loadMaster() {
      const search = document.getElementById('master-search').value;
      const status = document.getElementById('master-status').value;
      const ship = document.getElementById('master-shipping').value;
      const data = await api('/api/master?search=' + encodeURIComponent(search) + '&status=' + status + '&has_shipping=' + ship + '&limit=100');
      let html = '<div class="meta">' + data.total + '件</div><table class="data"><tr><th>商品コード</th><th>商品名</th><th>売価</th><th>原価</th><th>ソース</th><th>送料</th><th>粗利</th><th>粗利率</th></tr>';
      for (const r of data.rows) {
        const profitClass = r.粗利率 !== null && r.粗利率 < 10 ? ' style="color:#c0392b;font-weight:bold"' : '';
        html += '<tr><td>' + r.商品コード + '</td><td>' + (r.商品名||'') + '</td><td>' + r.売価 + '</td><td>' + (r.原価||'-') + '</td><td>' + r.原価ソース + '</td><td>' + (r.送料||'-') + '</td><td' + profitClass + '>' + (r.粗利??'-') + '</td><td' + profitClass + '>' + (r.粗利率!==null?r.粗利率+'%':'-') + '</td></tr>';
      }
      html += '</table>';
      document.getElementById('master-list').innerHTML = html;
    }

    // 売上分析
    async function loadMonthlySales() {
      const data = await api('/api/sales/monthly?months=6');
      const months = [...new Set(data.map(r => r.month))].sort().reverse();
      let html = '<table class="data"><tr><th>月</th><th>モール</th><th>データソース</th><th>数量</th><th>売上</th></tr>';
      for (const r of data) {
        html += '<tr><td>' + r.month + '</td><td>' + r.platform + '</td><td>' + r.data_source + '</td><td>' + (r.total_qty||0).toLocaleString() + '</td><td>' + (r.total_sales||0).toLocaleString() + '円</td></tr>';
      }
      html += '</table>';
      document.getElementById('sales-monthly').innerHTML = html;
    }
    async function loadSales() {
      const product = document.getElementById('sales-product').value;
      const platform = document.getElementById('sales-platform').value;
      const month = document.getElementById('sales-month').value;
      const data = await api('/api/sales?product=' + encodeURIComponent(product) + '&platform=' + platform + '&month=' + month + '&limit=50');
      let html = '<table class="data"><tr><th>商品コード</th><th>商品名</th><th>モール</th><th>数量</th><th>売上</th><th>注文数</th></tr>';
      for (const r of data) {
        html += '<tr><td>' + r.商品コード + '</td><td>' + (r.商品名||'') + '</td><td>' + r.platform + '</td><td>' + (r.total_qty||0).toLocaleString() + '</td><td>' + (r.total_sales||0).toLocaleString() + '円</td><td>' + (r.total_orders||0) + '</td></tr>';
      }
      html += '</table>';
      document.getElementById('sales-list').innerHTML = html;
    }
  </script>
</body>
</html>`;
}

export default router;
