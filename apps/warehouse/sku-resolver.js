/**
 * SKU解決ヘルパー
 *
 * 単票表示・登録UI用の薄いラッパー。
 * バッチ集計（rebuild-f-sales.js 等）はSQL一括展開で書くこと（v_sku_costed を参照）。
 *
 * 設計方針:
 *   - 紐付け解決は v_sku_resolved（master優先 + sku_map fallback）に委譲
 *   - 原価解決は v_sku_costed（raw_ne_products JOIN）に委譲
 *   - cost == null の判定で「未解決」を表現（0円は有効値として扱う）
 *   - normalize（trim + lowercase）は呼び出し側で保証する前提
 */

/**
 * seller_sku から構成要素のリストを取得（紐付け解決のみ、原価なし）
 * @param {Database} db better-sqlite3 instance
 * @param {string} sellerSku
 * @returns {Array<{ne_code: string, 数量: number, source: 'master'|'auto'}>}
 */
export function resolveSkuToComponents(db, sellerSku) {
  if (!sellerSku) return [];
  const sku = String(sellerSku).trim().toLowerCase();
  return db.prepare(`
    SELECT ne_code, 数量, source
    FROM v_sku_resolved
    WHERE seller_sku = ?
    ORDER BY source DESC, ne_code
  `).all(sku);
}

/**
 * 1つのne_codeの原価を取得
 * @param {Database} db
 * @param {string} neCode
 * @returns {{cost: number|null, source: 'ne'|'unresolved'}}
 */
export function resolveCost(db, neCode) {
  if (!neCode) return { cost: null, source: 'unresolved' };
  const code = String(neCode).trim().toLowerCase();
  const row = db.prepare(`
    SELECT 原価 FROM raw_ne_products WHERE 商品コード = ?
  `).get(code);
  // null と 0 を区別する: row が無い、または 原価 IS NULL なら未解決
  if (!row || row.原価 == null) return { cost: null, source: 'unresolved' };
  return { cost: row.原価, source: 'ne' };
}

/**
 * 受注1件の原価を計算（単票用）
 *
 * @param {Database} db
 * @param {string} sellerSku
 * @param {number} orderQty 受注数量
 * @returns {{
 *   totalCost: number|null,    // 1つでも未解決があればnull、全解決なら合計
 *   status: 'ok'|'sku_unmapped'|'cost_missing',
 *   breakdown: Array<{ne_code, 数量, 単価, 原価, source, status}>
 * }}
 */
export function calcOrderCost(db, sellerSku, orderQty) {
  const components = resolveSkuToComponents(db, sellerSku);
  if (components.length === 0) {
    return { totalCost: null, status: 'sku_unmapped', breakdown: [] };
  }

  let totalCost = 0;
  let hasUnresolved = false;

  const breakdown = components.map(c => {
    const { cost, source: costSource } = resolveCost(db, c.ne_code);
    const qty = orderQty * c.数量;
    let lineCost = null;
    let lineStatus = 'ok';

    // cost == null（true 0 と区別）で未解決判定
    if (cost == null) {
      hasUnresolved = true;
      lineStatus = 'cost_missing';
    } else {
      lineCost = cost * qty;
      totalCost += lineCost;
    }

    return {
      ne_code: c.ne_code,
      数量: qty,
      単価: cost,
      原価: lineCost,
      source: c.source,
      cost_source: costSource,
      status: lineStatus,
    };
  });

  return {
    totalCost: hasUnresolved ? null : totalCost,
    status: hasUnresolved ? 'cost_missing' : 'ok',
    breakdown,
  };
}

/**
 * 在庫1点の評価金額を計算（単票用）
 * 在庫数がne_code単位の場合（例: raw_lz_inventory）はこちらではなく直接JOINでよい
 * SKU単位の在庫数（例: FBA在庫）に対して使う想定
 *
 * @param {Database} db
 * @param {string} sellerSku
 * @param {number} skuStockQty SKU単位の在庫数
 * @returns 同 calcOrderCost
 */
export function calcInventoryValue(db, sellerSku, skuStockQty) {
  // ロジックは calcOrderCost と同一
  return calcOrderCost(db, sellerSku, skuStockQty);
}

/**
 * 単票表示用に商品名と構成詳細をまとめて取得
 * @param {Database} db
 * @param {string} sellerSku
 * @returns {{exists: boolean, master?: object, components?: Array, ne_titles?: object}}
 */
export function getSkuMasterDetail(db, sellerSku) {
  if (!sellerSku) return { exists: false };
  const sku = String(sellerSku).trim().toLowerCase();

  const master = db.prepare(`
    SELECT seller_sku, 商品名, created_at, updated_at, created_by, updated_by
    FROM m_sku_master WHERE seller_sku = ?
  `).get(sku);

  if (!master) return { exists: false };

  // 構成 + 原価 + NE側商品名（参照表示用）
  const components = db.prepare(`
    SELECT
      c.ne_code,
      c.数量,
      c.sort_order,
      p.商品名 AS ne_title,
      p.原価 AS 単価,
      CASE
        WHEN p.商品コード IS NULL THEN 'ne_missing'
        WHEN p.原価 IS NULL THEN 'cost_missing'
        ELSE 'ok'
      END AS cost_status
    FROM m_sku_components c
    LEFT JOIN raw_ne_products p ON c.ne_code = p.商品コード
    WHERE c.seller_sku = ?
    ORDER BY c.sort_order, c.ne_code
  `).all(sku);

  return { exists: true, master, components };
}
