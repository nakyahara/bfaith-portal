/**
 * warehouse-mirror API
 *
 * ミニPCからデータを受信し、mirrorテーブルに格納。
 * ツール用のデータ参照APIも提供。
 *
 * エンドポイント:
 *   POST /api/sync          — ミニPCからデータ受信（APIキー認証）
 *   GET  /api/products      — mirror_products 検索
 *   GET  /api/sales/monthly — mirror_sales_monthly 検索
 *   GET  /api/sales/daily   — mirror_sales_daily 検索
 *   GET  /api/status        — 同期状態
 */
import { Router } from 'express';
import { initMirrorDB, getMirrorDB } from './db.js';
import { bootStart, bootEnd, bootFail } from '../observability/boot-log.js';

const router = Router();

// DB初期化
let dbReady = false;
bootStart('mirror-db', 'warehouse-mirror.db');
(async () => {
  try {
    initMirrorDB();
    dbReady = true;
    bootEnd('mirror-db', 'warehouse-mirror.db');
  } catch (e) {
    bootFail('mirror-db', 'warehouse-mirror.db', e);
    console.error('[Mirror] DB初期化失敗:', e.message);
  }
})();

function ensureDB(req, res, next) {
  if (!dbReady) return res.status(503).json({ error: 'mirror DB 未初期化' });
  next();
}

// 同期APIキー認証
function requireSyncKey(req, res, next) {
  const key = process.env.MIRROR_SYNC_KEY;
  if (!key) return next(); // 未設定なら認証スキップ（開発用）
  const provided = req.headers['x-sync-key'] || req.query.sync_key;
  if (provided !== key) return res.status(401).json({ error: 'Invalid sync key' });
  next();
}

router.use(ensureDB);

// ─── POST /api/sync ───
// ミニPCからデータを受信して一括反映

router.post('/api/sync', requireSyncKey, (req, res) => {
  const db = getMirrorDB();
  const { products, set_components, sales_monthly, sales_daily, meta } = req.body;
  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);
  const log = [];

  try {
    // products（全件置換）
    if (products && products.length > 0) {
      const tx = db.transaction(() => {
        db.exec('DELETE FROM mirror_products');
        const stmt = db.prepare(`INSERT INTO mirror_products (
          product_id, 商品コード, 商品名, 商品区分, 取扱区分,
          標準売価, 原価, 原価ソース, 原価状態,
          送料, 送料コード, 配送方法, 消費税率, 税区分,
          在庫数, 引当数, 仕入先コード, セット構成品数, 売上分類, 代表商品コード, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
        for (const p of products) {
          stmt.run(p.product_id, p.商品コード, p.商品名, p.商品区分, p.取扱区分,
            p.標準売価, p.原価, p.原価ソース, p.原価状態,
            p.送料, p.送料コード, p.配送方法, p.消費税率, p.税区分,
            p.在庫数, p.引当数, p.仕入先コード, p.セット構成品数, p.売上分類 ?? null, p.代表商品コード ?? null, now);
        }
      });
      tx();
      log.push(`products: ${products.length}件`);
    }

    // set_components（全件置換）
    if (set_components && set_components.length > 0) {
      const tx = db.transaction(() => {
        db.exec('DELETE FROM mirror_set_components');
        const stmt = db.prepare(`INSERT INTO mirror_set_components (
          セット商品コード, 構成商品コード, 数量, 構成商品名, 構成商品原価, updated_at
        ) VALUES (?,?,?,?,?,?)`);
        for (const c of set_components) {
          stmt.run(c.セット商品コード, c.構成商品コード, c.数量, c.構成商品名, c.構成商品原価, now);
        }
      });
      tx();
      log.push(`set_components: ${set_components.length}件`);
    }

    // sku_map（全件置換）
    if (req.body.sku_map && req.body.sku_map.length > 0) {
      const skuMapData = req.body.sku_map;
      const tx = db.transaction(() => {
        db.exec('DELETE FROM mirror_sku_map');
        const stmt = db.prepare(`INSERT INTO mirror_sku_map (
          seller_sku, ne_code, asin, 商品名, 数量, updated_at
        ) VALUES (?,?,?,?,?,?)`);
        for (const s of skuMapData) {
          stmt.run(s.seller_sku, s.ne_code, s.asin || '', s.商品名 || '', s.数量 || 1, now);
        }
      });
      tx();
      log.push(`sku_map: ${skuMapData.length}件`);
    }

    // rakuten_sku_map（全件置換）
    if (req.body.rakuten_sku_map && req.body.rakuten_sku_map.length > 0) {
      const rskmData = req.body.rakuten_sku_map;
      const tx = db.transaction(() => {
        db.exec('DELETE FROM mirror_rakuten_sku_map');
        const stmt = db.prepare(`INSERT INTO mirror_rakuten_sku_map (
          rakuten_code, ne_code, source, updated_at
        ) VALUES (?,?,?,?)`);
        for (const m of rskmData) {
          stmt.run(m.rakuten_code, m.ne_code, m.source, now);
        }
      });
      tx();
      log.push(`rakuten_sku_map: ${rskmData.length}件`);
    }

    // amazon_sku_fees（全件置換）
    if (req.body.amazon_sku_fees && req.body.amazon_sku_fees.length > 0) {
      const feesData = req.body.amazon_sku_fees;
      const tx = db.transaction(() => {
        db.exec('DELETE FROM mirror_amazon_sku_fees');
        const stmt = db.prepare(`INSERT INTO mirror_amazon_sku_fees (
          seller_sku, asin, fulfillment_channel, referral_fee, referral_fee_rate,
          fba_fee, variable_closing_fee, per_item_fee, total_fee, price_used, fetched_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)`);
        for (const f of feesData) {
          stmt.run(f.seller_sku, f.asin, f.fulfillment_channel,
            f.referral_fee, f.referral_fee_rate,
            f.fba_fee, f.variable_closing_fee, f.per_item_fee,
            f.total_fee, f.price_used, f.fetched_at);
        }
      });
      tx();
      log.push(`amazon_sku_fees: ${feesData.length}件`);
    }

    // sales_monthly（初回チャンクでDELETE、以降は追記）
    if (sales_monthly && sales_monthly.length > 0) {
      const tx = db.transaction(() => {
        if (meta?.clear_monthly) db.exec('DELETE FROM mirror_sales_monthly');
        const stmt = db.prepare(`INSERT INTO mirror_sales_monthly (
          月, 商品コード, モール, 商品名, 数量, 直接販売数, セット経由数,
          売上金額, 注文数, データ種別, チャネル, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`);
        for (const s of sales_monthly) {
          stmt.run(s.月, s.商品コード, s.モール, s.商品名, s.数量,
            s.直接販売数 || 0, s.セット経由数 || 0,
            s.売上金額, s.注文数, s.データ種別, s.チャネル || '', now);
        }
      });
      tx();
      log.push(`sales_monthly: ${sales_monthly.length}件`);
    }

    // sales_daily（初回チャンクでDELETE、以降は追記）
    if (sales_daily && sales_daily.length > 0) {
      const tx = db.transaction(() => {
        if (meta?.clear_daily) db.exec('DELETE FROM mirror_sales_daily');
        const stmt = db.prepare(`INSERT INTO mirror_sales_daily (
          日付, 商品コード, モール, 商品名, 数量, 直接販売数, セット経由数,
          売上金額, 注文数, データ種別, チャネル, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`);
        for (const s of sales_daily) {
          stmt.run(s.日付, s.商品コード, s.モール, s.商品名, s.数量,
            s.直接販売数 || 0, s.セット経由数 || 0,
            s.売上金額, s.注文数, s.データ種別, s.チャネル || '', now);
        }
      });
      tx();
      log.push(`sales_daily: ${sales_daily.length}件`);
    }

    // 同期状態更新
    db.prepare('INSERT OR REPLACE INTO mirror_sync_status (key, value, updated_at) VALUES (?,?,?)').run('last_sync', now, now);
    if (meta) {
      for (const [k, v] of Object.entries(meta)) {
        db.prepare('INSERT OR REPLACE INTO mirror_sync_status (key, value, updated_at) VALUES (?,?,?)').run(k, String(v), now);
      }
    }

    // WALチェックポイント
    try { db.pragma('wal_checkpoint(TRUNCATE)'); } catch {}

    console.log('[Mirror] 同期完了:', log.join(', '));
    res.json({ ok: true, log, synced_at: now });
  } catch (e) {
    console.error('[Mirror] 同期エラー:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ─── GET /api/products ───

router.get('/api/products', (req, res) => {
  const db = getMirrorDB();
  const { search, status, type, limit = '100', offset = '0' } = req.query;
  let sql = 'SELECT * FROM mirror_products WHERE 1=1';
  const params = [];
  if (search) { sql += ' AND (商品コード LIKE ? OR 商品名 LIKE ?)'; const t = `%${search}%`; params.push(t, t); }
  if (status) { sql += ' AND 取扱区分 = ?'; params.push(status); }
  if (type) { sql += ' AND 商品区分 = ?'; params.push(type); }
  const countSql = sql.replace('SELECT *', 'SELECT COUNT(*) as cnt');
  const total = db.prepare(countSql).get(...params)?.cnt || 0;
  sql += ' ORDER BY 商品コード LIMIT ? OFFSET ?';
  params.push(parseInt(limit), parseInt(offset));
  res.json({ rows: db.prepare(sql).all(...params), total });
});

// ─── GET /api/sales/monthly ───

router.get('/api/sales/monthly', (req, res) => {
  const db = getMirrorDB();
  const { product, mall, type, months = '6' } = req.query;
  let sql = 'SELECT * FROM mirror_sales_monthly WHERE 1=1';
  const params = [];
  if (product) { sql += ' AND 商品コード LIKE ?'; params.push(`%${product}%`); }
  if (mall) { sql += ' AND モール = ?'; params.push(mall); }
  if (type) { sql += ' AND データ種別 = ?'; params.push(type); }
  sql += ' ORDER BY 月 DESC, 数量 DESC LIMIT 1000';
  res.json(db.prepare(sql).all(...params));
});

// ─── GET /api/sales/daily ───

router.get('/api/sales/daily', (req, res) => {
  const db = getMirrorDB();
  const { product, mall, type, days = '30' } = req.query;
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - parseInt(days));
  let sql = 'SELECT * FROM mirror_sales_daily WHERE 日付 >= ?';
  const params = [cutoff.toISOString().slice(0, 10)];
  if (product) { sql += ' AND 商品コード LIKE ?'; params.push(`%${product}%`); }
  if (mall) { sql += ' AND モール = ?'; params.push(mall); }
  if (type) { sql += ' AND データ種別 = ?'; params.push(type); }
  sql += ' ORDER BY 日付 DESC, 数量 DESC LIMIT 5000';
  res.json(db.prepare(sql).all(...params));
});

// ─── GET /api/status ───

router.get('/api/status', (req, res) => {
  const db = getMirrorDB();
  const status = {};
  try {
    for (const r of db.prepare('SELECT key, value, updated_at FROM mirror_sync_status').all()) {
      status[r.key] = { value: r.value, updated_at: r.updated_at };
    }
    status.products_count = db.prepare('SELECT COUNT(*) as cnt FROM mirror_products').get().cnt;
    status.sales_monthly_count = db.prepare('SELECT COUNT(*) as cnt FROM mirror_sales_monthly').get().cnt;
    status.sales_daily_count = db.prepare('SELECT COUNT(*) as cnt FROM mirror_sales_daily').get().cnt;
    status.sku_map_count = db.prepare('SELECT COUNT(*) as cnt FROM mirror_sku_map').get().cnt;
    try { status.amazon_sku_fees_count = db.prepare('SELECT COUNT(*) as cnt FROM mirror_amazon_sku_fees').get().cnt; } catch { status.amazon_sku_fees_count = 0; }
    try { status.rakuten_sku_map_count = db.prepare('SELECT COUNT(*) as cnt FROM mirror_rakuten_sku_map').get().cnt; } catch { status.rakuten_sku_map_count = 0; }
  } catch {}
  res.json(status);
});

// ─── GET /api/download/:table ───
// CSVダウンロード

router.get('/api/download/:table', (req, res) => {
  const db = getMirrorDB();
  const table = req.params.table;
  const allowed = ['products', 'set_components', 'sales_monthly', 'sales_daily', 'sku_map'];
  if (!allowed.includes(table)) return res.status(400).json({ error: '無効なテーブル名' });

  const tableName = 'mirror_' + table;
  const rows = db.prepare(`SELECT * FROM ${tableName}`).all();
  if (!rows.length) return res.status(404).json({ error: 'データなし' });

  const headers = Object.keys(rows[0]);
  const escapeCsv = v => { const s = String(v ?? ''); return s.includes(',') || s.includes('"') || s.includes('\n') ? '"' + s.replace(/"/g, '""') + '"' : s; };
  const lines = [headers.join(','), ...rows.map(r => headers.map(h => escapeCsv(r[h])).join(','))];

  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="mirror_${table}.csv"`);
  res.send('\uFEFF' + lines.join('\r\n'));
});

export default router;
