/**
 * sku-norm — SKU/商品コード正規化の共通 util (設計監査 2026-07-06 PR-10 / M-7, INV-13)
 *
 * picking-prep の normCode() を全社共通に昇格したもの (実装は同一)。
 * 過去事故: 前後空白・全角ハイフン 1 個で SKU unresolved → cogs 0 → 粗利過大
 * (Yahoo Phase1.1 PR #94)。SKU 比較・JOIN は SQL 側 LOWER(TRIM()) + JS 側この関数で統一する。
 *
 * SQL 側の規約 (設計書「全社DDL・金額規約」§3):
 *   JOIN/IN の両辺を LOWER(TRIM(...)) で包む。頻出 JOIN 先には式 index を張る
 *   (例: mirror_products の idx_mirror_products_code_norm)。
 */

/** 商品コード/SKU の正規化: 全角→半角、各種ダッシュ→ハイフン、空白除去、小文字化 */
export function normSku(v) {
  if (v == null) return '';
  let t = String(v);
  t = t.replace(/[！-～]/g, ch => String.fromCharCode(ch.charCodeAt(0) - 0xFEE0)); // 全角英数記号→半角
  t = t.replace(/　/g, ' ');                                                  // 全角SP→半角
  t = t.replace(/[−‐-―﹘﹣－]/g, '-');                  // 各種ダッシュ→ハイフン
  t = t.trim().replace(/\s+/g, '').toLowerCase();
  return t;
}
