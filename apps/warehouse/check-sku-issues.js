/**
 * SKUマスタの健全性チェック（毎朝バッチ実行）
 *
 * 出力する3種類のレポート:
 *
 *   A. 完全未紐付け（緊急）
 *      直近7日に売れた seller_sku のうち、master/sku_map のどちらにも無い
 *      → 即時アラート
 *
 *   B. master未登録（移行進捗）
 *      sku_map で対応中だが m_sku_master に未登録
 *      → 週次サマリでOK、優先順位を直近売上数量で付ける
 *
 *   C. リンク切れ（NE側で消えた）
 *      m_sku_components.ne_code が raw_ne_products に存在しない
 *      → 即時アラート
 *
 * Google Chat 通知は別ファイル（呼び出し側）で実装する想定。
 * このスクリプトは集計結果を JSON で返すだけ。
 *
 * 使い方:
 *   node apps/warehouse/check-sku-issues.js [--days-a=7] [--days-b=30]
 */
import { getDB, initDB } from './db.js';

/**
 * @returns {{
 *   alertA: { count: number, items: Array<{seller_sku, 注文数, 最終受注日}> },
 *   alertB: { count: number, items: Array<{seller_sku, 注文数, 最終受注日}> },
 *   alertC: { count: number, items: Array<{seller_sku, ne_code}> }
 * }}
 */
export function checkSkuIssues(opts = {}) {
  const { daysA = 7, daysB = 30 } = opts;
  const db = getDB();

  // A: 完全未紐付け（v_sku_resolved にも無い、直近 daysA 日に売上あり）
  const alertA = db.prepare(`
    SELECT
      o.seller_sku,
      COUNT(*) AS 注文数,
      MAX(o.purchase_date) AS 最終受注日
    FROM raw_sp_orders o
    LEFT JOIN v_sku_resolved v ON o.seller_sku = v.seller_sku
    WHERE v.seller_sku IS NULL
      AND o.purchase_date >= date('now', ?)
      AND o.seller_sku IS NOT NULL
      AND o.seller_sku <> ''
    GROUP BY o.seller_sku
    ORDER BY 最終受注日 DESC, 注文数 DESC
  `).all(`-${daysA} days`);

  // B: master未登録だが sku_map で対応中
  const alertB = db.prepare(`
    SELECT
      o.seller_sku,
      COUNT(*) AS 注文数,
      MAX(o.purchase_date) AS 最終受注日
    FROM raw_sp_orders o
    LEFT JOIN m_sku_master m ON o.seller_sku = m.seller_sku
    INNER JOIN sku_map s ON o.seller_sku = s.seller_sku
    WHERE m.seller_sku IS NULL
      AND o.purchase_date >= date('now', ?)
    GROUP BY o.seller_sku
    ORDER BY 注文数 DESC
  `).all(`-${daysB} days`);

  // C: リンク切れ（components の ne_code が raw_ne_products に無い）
  const alertC = db.prepare(`
    SELECT
      c.seller_sku,
      c.ne_code,
      m.商品名 AS 社内商品名
    FROM m_sku_components c
    LEFT JOIN raw_ne_products p ON c.ne_code = p.商品コード
    INNER JOIN m_sku_master m ON c.seller_sku = m.seller_sku
    WHERE p.商品コード IS NULL
    ORDER BY c.seller_sku, c.ne_code
  `).all();

  return {
    alertA: { count: alertA.length, items: alertA },
    alertB: { count: alertB.length, items: alertB },
    alertC: { count: alertC.length, items: alertC },
  };
}

// ─── CLI ───
const isMain = process.argv[1]?.endsWith('check-sku-issues.js');
if (isMain) {
  const args = process.argv.slice(2);
  const daysA = parseInt(args.find(a => a.startsWith('--days-a='))?.split('=')[1] || '7', 10);
  const daysB = parseInt(args.find(a => a.startsWith('--days-b='))?.split('=')[1] || '30', 10);

  await initDB();
  const r = checkSkuIssues({ daysA, daysB });

  console.log('========================================');
  console.log(`【アラートA】 完全未紐付け (直近${daysA}日売上あり): ${r.alertA.count}件`);
  console.log('========================================');
  if (r.alertA.count > 0) {
    r.alertA.items.slice(0, 20).forEach(it => {
      console.log(`  ${it.seller_sku.padEnd(35)} 注文${String(it.注文数).padStart(5)}回  最終: ${it.最終受注日}`);
    });
    if (r.alertA.count > 20) console.log(`  ... (${r.alertA.count - 20}件省略)`);
  }

  console.log('');
  console.log('========================================');
  console.log(`【アラートB】 master未登録(sku_mapで対応中) 直近${daysB}日: ${r.alertB.count}件`);
  console.log('========================================');
  if (r.alertB.count > 0) {
    r.alertB.items.slice(0, 20).forEach(it => {
      console.log(`  ${it.seller_sku.padEnd(35)} 注文${String(it.注文数).padStart(5)}回  最終: ${it.最終受注日}`);
    });
    if (r.alertB.count > 20) console.log(`  ... (${r.alertB.count - 20}件省略)`);
  }

  console.log('');
  console.log('========================================');
  console.log(`【アラートC】 リンク切れ(NE消失): ${r.alertC.count}件`);
  console.log('========================================');
  if (r.alertC.count > 0) {
    r.alertC.items.slice(0, 20).forEach(it => {
      console.log(`  ${it.seller_sku} → ${it.ne_code}  (${it.社内商品名 ?? ''})`);
    });
    if (r.alertC.count > 20) console.log(`  ... (${r.alertC.count - 20}件省略)`);
  }

  // エラーコード: A>0 で 1, それ以外 0（B/C は警告扱い）
  process.exit(r.alertA.count > 0 ? 1 : 0);
}
