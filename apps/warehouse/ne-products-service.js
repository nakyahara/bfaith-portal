/**
 * ne-products 向けの service-api (packing standalone → warehouse ローカル呼び出し)。
 *
 * 梱包画面の商品名表示用: NEから毎日取り込む商品マスタ (raw_ne_products) の
 * 「社内の単品商品名」を商品コードで一括解決する (中原さん指示 2026-08-22 —
 * 納品書CSVの印字商品名はセット品受注でモールのSEO長文になるため使わない)。
 *
 * - 認証は mount 側の serviceAuth (CF Access サービストークン + SERVICE_TOKEN Bearer)
 * - 返すのは 商品コード→商品名 のみ。読み取り専用・書き込み系は作らない
 */
import { Router } from 'express';
import { getDB } from './db.js';

const router = Router();

router.post('/names', (req, res) => {
  const codes = Array.isArray(req.body?.codes) ? req.body.codes : [];
  const uniq = [...new Set(codes.map((c) => String(c ?? '').trim()).filter(Boolean))].slice(0, 1000);
  if (uniq.length === 0) return res.json({ ok: true, names: {} });
  const db = getDB();
  const names = {};
  // raw_ne_products.商品コード はNEの表記そのまま。呼び出し側の表記ゆれに備えて
  // 大文字小文字を無視して突合する (COLLATE NOCASE = このリポジトリの既存JOINと同じ流儀)
  for (let i = 0; i < uniq.length; i += 500) {
    const chunk = uniq.slice(i, i + 500);
    const rows = db.prepare(`
      SELECT 商品コード AS code, 商品名 AS name FROM raw_ne_products
      WHERE 商品コード COLLATE NOCASE IN (${chunk.map(() => '?').join(',')})
        AND 商品名 IS NOT NULL AND TRIM(商品名) != ''
    `).all(...chunk);
    const byLower = new Map(rows.map((r) => [String(r.code).toLowerCase(), String(r.name).trim()]));
    for (const c of chunk) {
      const n = byLower.get(c.toLowerCase());
      if (n) names[c] = n;
    }
  }
  res.json({ ok: true, names });
});

export default router;
