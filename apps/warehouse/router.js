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
    LEFT JOIN raw_ne_products p ON s.商品コード = p.商品コード
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
    LEFT JOIN raw_ne_products p ON s.商品コード = p.商品コード
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
  const { sql } = req.query;
  if (!sql) return res.status(400).json({ error: 'sql パラメータが必要です' });

  // SELECT文のみ許可
  const normalized = sql.trim().toUpperCase();
  if (!normalized.startsWith('SELECT')) {
    return res.status(403).json({ error: 'SELECT文のみ実行可能です' });
  }

  // 危険なキーワードチェック
  const forbidden = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'ATTACH', 'DETACH'];
  for (const kw of forbidden) {
    if (normalized.includes(kw)) {
      return res.status(403).json({ error: `${kw} は使用できません` });
    }
  }

  try {
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

// ─── POST /api/shipping ───
// 送料登録（個別）

router.post('/api/shipping', (req, res) => {
  const { sku, shipping_code, ship_method, ship_cost } = req.body;
  if (!sku || !ship_cost) return res.status(400).json({ error: 'sku と ship_cost は必須です' });
  const db = getDB();
  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);
  db.prepare('INSERT OR REPLACE INTO product_shipping (sku, product_name, shipping_code, ship_method, ship_cost, note, synced_at) VALUES (?, (SELECT 商品名 FROM raw_ne_products WHERE 商品コード = ?), ?, ?, ?, ?, ?)').run(sku, sku, shipping_code || '', ship_method || '', parseFloat(ship_cost), '', now);
  res.json({ ok: true, sku, ship_cost });
});

// ─── POST /api/genka ───
// 原価登録（例外原価）

router.post('/api/genka', (req, res) => {
  const { sku, genka, product_name } = req.body;
  if (!sku || genka === undefined) return res.status(400).json({ error: 'sku と genka は必須です' });
  const db = getDB();
  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);
  db.prepare('INSERT OR REPLACE INTO exception_genka (sku, genka, 商品名, synced_at) VALUES (?, ?, ?, ?)').run(sku, parseFloat(genka), product_name || '', now);
  res.json({ ok: true, sku, genka });
});

// ─── POST /api/skumap ───
// SKUマップ登録（個別）

router.post('/api/skumap', (req, res) => {
  const { seller_sku, asin, product_name, ne_code, quantity } = req.body;
  if (!seller_sku || !ne_code) return res.status(400).json({ error: 'seller_sku と ne_code は必須です' });
  const db = getDB();
  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);
  db.prepare('INSERT OR REPLACE INTO sku_map (seller_sku, asin, 商品名, ne_code, 数量, synced_at) VALUES (?, ?, ?, ?, ?, ?)').run(seller_sku, asin || '', product_name || '', ne_code, parseInt(quantity) || 1, now);
  res.json({ ok: true, seller_sku, ne_code });
});

// ─── GET /api/shipping_rates ───

router.get('/api/shipping_rates', (req, res) => {
  res.json(execQuery('SELECT * FROM shipping_rates ORDER BY shipping_code'));
});

// ─── ダッシュボード（HTML）───

router.get('/', (req, res) => {
  const stats = getStats();
  res.send(renderDashboard(stats));
});

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
        <div class="tab-nav">
          <button class="active" onclick="loadMissing('shipping', this)">送料未登録 (${missingCounts.shipping||0})</button>
          <button onclick="loadMissing('genka', this)">原価未登録 (${missingCounts.genka||0})</button>
          <button onclick="loadMissing('sku_map', this)">SKU未登録 (${missingCounts.sku_map||0})</button>
        </div>
        <div id="missing-list"></div>
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
    const RATES = ${JSON.stringify(shippingRates)};

    // 初期表示時に未登録件数を非同期取得
    (async function() {
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

    function showTab(name) {
      document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
      document.getElementById('tab-' + name).classList.add('active');
      if (name === 'missing') loadMissing('shipping');
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

    // 未登録データ
    async function loadMissing(type, btn) {
      if (btn) { document.querySelectorAll('.tab-nav button').forEach(b => b.classList.remove('active')); btn.classList.add('active'); }
      const data = await api('/api/missing?type=' + type);
      const rows = data.rows || [];
      let html = '<table class="data"><tr><th>商品コード</th><th>商品名</th><th>売価</th><th>原価</th><th>アクション</th></tr>';
      for (const r of rows.slice(0, 100)) {
        html += '<tr><td>' + r.商品コード + '</td><td>' + (r.商品名||'').slice(0,40) + '</td><td>' + (r.売価||'-') + '</td><td>' + (r.原価||'-') + '</td><td>';
        if (type === 'shipping') {
          html += '<select id="rate-' + r.商品コード + '">' + RATES.map(rt => '<option value="' + rt.shipping_code + '|' + rt.小分類区分名称 + '|' + rt.配送関係費合計 + '">' + rt.小分類区分名称 + ' (' + rt.配送関係費合計 + '円)</option>').join('') + '</select> ';
          html += '<button class="btn btn-sm btn-primary" onclick="registerShipping(\\'' + r.商品コード + '\\')">登録</button>';
        } else if (type === 'genka') {
          html += '<input id="genka-' + r.商品コード + '" placeholder="原価" style="width:80px"> ';
          html += '<button class="btn btn-sm btn-primary" onclick="registerGenka(\\'' + r.商品コード + '\\', \\'' + (r.商品名||'').replace(/'/g,'') + '\\')">登録</button>';
        } else if (type === 'sku_map') {
          html += '<input id="ne-' + r.商品コード + '" placeholder="NE商品コード" style="width:120px"> ';
          html += '<button class="btn btn-sm btn-primary" onclick="registerSkuMap(\\'' + r.商品コード + '\\', \\'' + (r.商品名||'').replace(/'/g,'') + '\\')">登録</button>';
        }
        html += '</td></tr>';
      }
      html += '</table>';
      if (rows.length > 100) html += '<div class="meta">' + rows.length + '件中100件表示</div>';
      document.getElementById('missing-list').innerHTML = html;
    }

    async function registerShipping(sku) {
      const val = document.getElementById('rate-' + sku).value.split('|');
      await api('/api/shipping', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ sku, shipping_code: val[0], ship_method: val[1], ship_cost: val[2] }) });
      toast(sku + ' の送料を登録しました');
      loadMissing('shipping');
    }
    async function registerGenka(sku, name) {
      const genka = document.getElementById('genka-' + sku).value;
      if (!genka) return;
      await api('/api/genka', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ sku, genka, product_name: name }) });
      toast(sku + ' の原価を登録しました');
      loadMissing('genka');
    }
    async function registerSkuMap(sku, name) {
      const ne = document.getElementById('ne-' + sku).value;
      if (!ne) return;
      await api('/api/skumap', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ seller_sku: sku, ne_code: ne, product_name: name }) });
      toast(sku + ' のSKUマップを登録しました');
      loadMissing('sku_map');
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
        html += '<tr><td>' + r.商品コード + '</td><td>' + (r.商品名||'').slice(0,35) + '</td><td>' + r.売価 + '</td><td>' + (r.原価||'-') + '</td><td>' + r.原価ソース + '</td><td>' + (r.送料||'-') + '</td><td' + profitClass + '>' + (r.粗利??'-') + '</td><td' + profitClass + '>' + (r.粗利率!==null?r.粗利率+'%':'-') + '</td></tr>';
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
        html += '<tr><td>' + r.商品コード + '</td><td>' + (r.商品名||'').slice(0,35) + '</td><td>' + r.platform + '</td><td>' + (r.total_qty||0).toLocaleString() + '</td><td>' + (r.total_sales||0).toLocaleString() + '円</td><td>' + (r.total_orders||0) + '</td></tr>';
      }
      html += '</table>';
      document.getElementById('sales-list').innerHTML = html;
    }
  </script>
</body>
</html>`;
}

export default router;
