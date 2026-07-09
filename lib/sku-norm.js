/**
 * sku-norm — SKU/商品コード正規化の共通 util (設計監査 2026-07-06 PR-10 / M-7, INV-13)
 *
 * picking-prep の normCode() を全社共通に昇格したもの (実装は同一)。
 * 過去事故: 前後空白・全角ハイフン 1 個で SKU unresolved → cogs 0 → 粗利過大
 * (Yahoo Phase1.1 PR #94)。SKU 比較・JOIN は SQL 側 LOWER(TRIM()) + JS 側この関数で統一する。
 *
 * SQL 側の規約 (設計書「全社DDL・金額規約」§3 + Codex R1指摘):
 *   ・JOIN は「片側正規化」: マスタ側 (mirror_products.商品コード 等、NE由来で小文字
 *     at-rest が保証される正準列) は生のまま、汚れうる fact 側だけ LOWER(TRIM()) で包む。
 *     両辺を包むと、正規化後に衝突する行がマスタに 2 行あった場合に LEFT JOIN が
 *     fact 行を増殖させ集計が膨らむ (片側ならリスクは旧来の生 JOIN と同一のまま)。
 *     ついでにマスタ側の既存 index も効き続ける。
 *   ・IN フィルタは両辺正規化して良い (行増殖しない)。
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
