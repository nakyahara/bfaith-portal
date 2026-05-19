/**
 * /lookup-api/orders/lookup - 注文番号から受注情報を1件取得 (read-only)
 *
 * 用途: Render 側 /apps/mis-shipment/ の注文番号入力フォームから、
 *       wh.bfaith-wh.uk 経由でこの endpoint を叩き、mall/sku/商品名/数量/日付を取得する。
 *
 * 認証: WAREHOUSE_LOOKUP_TOKEN 専用 (既存 SERVICE_TOKEN / WAREHOUSE_API_KEY とは完全分離)
 *       未設定時は 503 で fail-closed (絶対に next() しない、Codex round 14/15 指摘)
 *       header: x-api-key
 *
 * Mount: server.js で `app.use('/lookup-api', lookupRouter)` で独立 mount。
 *        既存 /apps/warehouse (warehouseRouter, requireAppAccess) や
 *             /service-api (serviceRouter, serviceAuth) とは別 path、別 token。
 *        外部 URL: https://wh.bfaith-wh.uk/lookup-api/orders/lookup?order_id=XXX
 *
 * 関連:
 *   - 設計書: g:/共有ドライブ/AI_reference/システム設計/誤出荷管理システム_設計書_v5.md (中身 v7.3)
 *   - PR: feature/mis-shipment
 */
import { Router } from 'express';
import { initDB, getDB } from './db.js';

// ─── 認証 middleware (fail-closed、Codex round 15 指摘反映) ───
export function requireLookupToken(req, res, next) {
  const expected = process.env.WAREHOUSE_LOOKUP_TOKEN;
  if (!expected) {
    // 未設定なら絶対に next() しない (silent failure 防止)
    return res.status(503).json({ error: 'lookup_token_unset' });
  }
  const provided = req.headers['x-api-key'];
  if (provided !== expected) {
    return res.status(401).json({ error: 'invalid_lookup_token' });
  }
  next();
}

// ─── Router 本体 ───
const router = Router();

// このルーター配下は要 lookup token (router-level middleware で隔離)
router.use(requireLookupToken);

// DB 初期化保証
let dbReady = false;
(async () => {
  try {
    await initDB();
    dbReady = true;
  } catch (e) {
    console.error('[orders-lookup] DB init failed:', e.message);
  }
})();

router.use((req, res, next) => {
  if (!dbReady) return res.status(503).json({ error: 'db_not_ready' });
  next();
});

// ─── GET /lookup-api/orders/lookup?order_id=XXX ───
router.get('/orders/lookup', (req, res) => {
  const orderId = req.query.order_id;
  if (!orderId || typeof orderId !== 'string') {
    return res.status(400).json({ error: 'order_id_required' });
  }
  if (orderId.length > 100) {
    return res.status(400).json({ error: 'order_id_too_long' });
  }

  try {
    const db = getDB();
    // raw_ne_orders.受注番号 はモール側生 ID と一致 (Amazon: 503-xxx, 楽天: 373343-xxx 等)
    // raw_ne_orders.伝票番号 は NE 内部の伝票連番 (例: 1438443)
    // 現場の出荷指示書にどちらが書いてあるかケースバイケースなので、OR 両方検索。
    // matched_by で UI 上「NE 伝票番号でヒット」「モール受注番号でヒット」を区別表示。
    // 1 伝票に複数明細行ある場合は先頭 (明細行番号 ASC) の 1 件を返し、line_count で複数明細を示す。
    const row = db.prepare(`
      SELECT
        s.platform                                       AS mall,
        o.受注番号                                      AS mall_order_id,
        o.伝票番号                                      AS slip_no,
        o.商品コード                                    AS sku,
        o.商品名                                        AS product_name,
        o.受注数                                        AS ordered_qty,
        o.受注日                                        AS order_date,
        (CASE WHEN o.受注番号 = ? THEN 'order_no' ELSE 'slip_no' END) AS matched_by,
        (SELECT COUNT(*) FROM raw_ne_orders x
          WHERE x.伝票番号 = o.伝票番号
            AND x.キャンセル区分 = '有効')              AS line_count
      FROM raw_ne_orders o
      JOIN shops s ON o.店舗コード = s.shop_code
     WHERE (o.受注番号 = ? OR o.伝票番号 = ?)
       AND o.キャンセル区分 = '有効'
     ORDER BY o.明細行番号 ASC
     LIMIT 1
    `).get(orderId, orderId, orderId);

    if (!row) {
      return res.status(200).json({ found: false });
    }

    // order_date は 'YYYY-MM-DD HH:MM:SS' 形式で来るので 日付部分のみ返す
    const orderDateOnly = row.order_date ? String(row.order_date).slice(0, 10) : null;

    return res.json({
      found: true,
      mall: row.mall,
      mall_order_id: row.mall_order_id,
      slip_no: row.slip_no,
      matched_by: row.matched_by,  // 'order_no' (モール受注番号) | 'slip_no' (NE 伝票番号)
      sku: row.sku,
      product_name: row.product_name,
      ordered_qty: row.ordered_qty,
      order_date: orderDateOnly,
      line_count: row.line_count,  // フロントで「複数明細あり」表示用
    });
  } catch (e) {
    console.error('[orders-lookup] DB error:', e.message);
    return res.status(500).json({ error: 'internal_error' });
  }
});

export default router;
