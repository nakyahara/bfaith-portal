/**
 * logizard-stock 向けの service-api (picking standalone → warehouse ローカル呼び出し)。
 *
 * ピッキング欠品通知に「同一SKUの他ロケーション在庫」を載せるための読み取り専用API。
 * データ源は毎時の在庫CSV取込 (scripts/logizard-stock/run-hourly.ps1) が洗い替える
 * raw_lz_inventory。鮮度は sync_meta.logizard_last_import で返す (通知側が「HH:MM時点」を表示)。
 *
 * - 認証は mount 側の serviceAuth (CF Access サービストークン + SERVICE_TOKEN Bearer)
 * - 返すのは ロケ/品質/在庫数/引当数 の集計だけ。顧客情報は一切通さない
 * - 書き込み系は作らない
 */
import { Router } from 'express';
import { getDB } from './db.js';

const router = Router();

router.get('/locations', (req, res) => {
  // 商品ID は取込時に trim + lowercase 済み (SKU正規化ルール)。照会側も同じ正規化で引く
  const code = String(req.query.code || '').trim().toLowerCase();
  if (!code) return res.status(400).json({ ok: false, error: 'CODE_REQUIRED' });
  try {
    const db = getDB();
    const locations = db.prepare(`
      SELECT ブロック略称 AS block, ロケ AS location, 品質区分名 AS quality,
             SUM(在庫数) AS qty, SUM(引当数) AS allocated,
             SUM(在庫数 - 引当数) AS free
      FROM raw_lz_inventory
      WHERE 商品ID = ?
      GROUP BY ブロック略称, ロケ, 品質区分名
    `).all(code);
    const name = db.prepare('SELECT MIN(商品名) AS n FROM raw_lz_inventory WHERE 商品ID = ?').get(code)?.n || null;
    const importedAt = db.prepare("SELECT value FROM sync_meta WHERE key = 'logizard_last_import'").get()?.value || null;
    const stockDate = db.prepare('SELECT 在庫日 AS d FROM raw_lz_inventory LIMIT 1').get()?.d || null;
    res.json({ ok: true, importedAt, stockDate, name, count: locations.length, locations });
  } catch (e) {
    console.error('[logizard-stock-service] locations error', e);
    // 上流のエラー文をそのまま返さない (呼び出し側は固定の形だけを期待する)
    res.status(500).json({ ok: false, error: 'DB_ERROR' });
  }
});

// SKU候補検索 (LINE在庫検索ボット用)。商品名/商品ID部分一致+バーコード完全一致、良品フリー降順
router.get('/search', (req, res) => {
  const q = String(req.query.q || '').trim();
  if (q.length < 2) return res.status(400).json({ ok: false, error: 'QUERY_TOO_SHORT' });
  try {
    const db = getDB();
    const like = `%${q.replace(/[\\%_]/g, (c) => `\\${c}`)}%`;
    const items = db.prepare(`
      SELECT 商品ID AS sku, MIN(商品名) AS name,
             SUM(CASE WHEN 品質区分名 = '良品' THEN 在庫数 - 引当数 ELSE 0 END) AS free
      FROM raw_lz_inventory
      WHERE 商品名 LIKE ? ESCAPE '\\' OR 商品ID LIKE ? ESCAPE '\\' OR バーコード = ?
      GROUP BY 商品ID
      ORDER BY free DESC, 商品ID
      LIMIT 11
    `).all(like, like, q);
    const importedAt = db.prepare("SELECT value FROM sync_meta WHERE key = 'logizard_last_import'").get()?.value || null;
    res.json({ ok: true, importedAt, count: items.length, items });
  } catch (e) {
    console.error('[logizard-stock-service] search error', e);
    res.status(500).json({ ok: false, error: 'DB_ERROR' });
  }
});

export default router;
