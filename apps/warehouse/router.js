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

// ─── ダッシュボード（HTML）───

router.get('/', (req, res) => {
  const stats = getStats();
  res.send(renderDashboard(stats));
});

function renderDashboard(stats) {
  return `<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>データウェアハウス - B-Faith</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #f5f5f5; color: #333; }
    .header { background: #1a5276; color: white; padding: 16px 24px; }
    .header h1 { font-size: 20px; }
    .container { max-width: 1000px; margin: 24px auto; padding: 0 24px; }
    .card { background: white; border-radius: 8px; padding: 24px; margin-bottom: 16px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
    .card h2 { font-size: 16px; color: #555; margin-bottom: 12px; }
    .stats { display: flex; gap: 16px; flex-wrap: wrap; }
    .stat { background: #f8f9fa; border-radius: 6px; padding: 16px; text-align: center; min-width: 150px; }
    .stat .label { font-size: 12px; color: #888; }
    .stat .value { font-size: 28px; font-weight: 700; color: #1a5276; }
    .endpoints { font-size: 13px; }
    .endpoints table { width: 100%; border-collapse: collapse; }
    .endpoints td, .endpoints th { padding: 6px 8px; border-bottom: 1px solid #eee; text-align: left; }
    .endpoints code { background: #f0f0f0; padding: 2px 6px; border-radius: 3px; font-size: 12px; }
    .meta { font-size: 13px; color: #666; margin-top: 8px; }
  </style>
</head>
<body>
  <div class="header"><h1>Data Warehouse API</h1></div>
  <div class="container">
    <div class="card">
      <h2>DB統計</h2>
      <div class="stats">
        <div class="stat"><div class="label">商品マスタ</div><div class="value">${stats.raw_ne_products}</div></div>
        <div class="stat"><div class="label">受注明細</div><div class="value">${stats.raw_ne_orders}</div></div>
        <div class="stat"><div class="label">セット商品</div><div class="value">${stats.raw_ne_set_products}</div></div>
        <div class="stat"><div class="label">店舗</div><div class="value">${stats.shops}</div></div>
      </div>
      <div class="meta">
        ${stats.sync_meta?.products_last_import ? '商品最終同期: ' + stats.sync_meta.products_last_import : ''}
        ${stats.order_date_range ? ' | 受注期間: ' + stats.order_date_range.min + ' ~ ' + stats.order_date_range.max : ''}
      </div>
    </div>
    <div class="card endpoints">
      <h2>APIエンドポイント</h2>
      <table>
        <tr><th>メソッド</th><th>パス</th><th>説明</th></tr>
        <tr><td>GET</td><td><code>/api/stats</code></td><td>DB統計</td></tr>
        <tr><td>GET</td><td><code>/api/products?search=&status=&limit=</code></td><td>商品検索</td></tr>
        <tr><td>GET</td><td><code>/api/products/:code</code></td><td>商品詳細</td></tr>
        <tr><td>GET</td><td><code>/api/orders?product=&shop=&from=&to=</code></td><td>受注検索</td></tr>
        <tr><td>GET</td><td><code>/api/orders/daily?from=&to=&platform=</code></td><td>日別販売数集計</td></tr>
        <tr><td>GET</td><td><code>/api/orders/summary?group_by=shop|product|month</code></td><td>サマリー</td></tr>
        <tr><td>GET</td><td><code>/api/shops</code></td><td>店舗一覧</td></tr>
        <tr><td>GET</td><td><code>/api/query?sql=SELECT...</code></td><td>任意SQL（SELECT限定）</td></tr>
      </table>
    </div>
  </div>
</body>
</html>`;
}

export default router;
