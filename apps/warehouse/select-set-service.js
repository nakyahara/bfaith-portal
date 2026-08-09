/**
 * select-set 向けの service-api (Render → miniPC)。
 *
 * 選べるセットの展開に必要な「NE商品マスタと在庫」を Render 側へ渡すための読み取り専用API。
 * 拡張の接続先を Render に変えた際 (miniPC は Cloudflare Access の後ろで拡張から届かない
 * = 2026-08-09 実測)、Render には warehouse.db が無いのでここから取る。
 *
 * - 認証は mount 側の serviceAuth (CF Access サービストークン + SERVICE_TOKEN Bearer)
 * - 返すのは 商品コード/商品名/利用可能在庫/取扱区分 だけ。顧客情報は一切通さない
 * - 書き込み系は作らない (誤展開の防止はRender側のロジックが担う。ここは素材の供給だけ)
 */
import { Router } from 'express';
import { getDB } from './db.js';

const router = Router();

router.get('/products', (req, res) => {
  try {
    const rows = getDB().prepare(`
      SELECT 商品コード AS code, 商品名 AS name,
             (在庫数 - 引当数) AS available, 取扱区分 AS status
      FROM raw_ne_products
    `).all();
    res.json({ ok: true, count: rows.length, products: rows });
  } catch (e) {
    console.error('[select-set-service] products error', e);
    // 上流のエラー文をそのまま返さない (呼び出し側は固定の形だけを期待する)
    res.status(500).json({ ok: false, error: 'DB_ERROR' });
  }
});

export default router;
