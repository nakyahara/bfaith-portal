/**
 * 発注設定（推奨保有月数）未登録チェック（毎朝バッチ実行）
 *
 * 商品管理リストの「推奨保有在庫」(= 何ヶ月分持つかの係数) は手動パラメータで、
 * 新商品が NE に入っても自動では埋まらない。未登録のまま放置されると発注計算から
 * 漏れるため、未登録の取扱中商品を毎朝レポートする。
 *
 *   未登録 = m_products(取扱中) のうち m_reorder_setting に登録なし
 *   → register UI (GET /api/reorder/unregistered) で登録
 *
 * Google Chat 通知は呼び出し側で実装する想定。本スクリプトは集計結果を返すだけ。
 *
 * 使い方:
 *   node apps/warehouse/check-reorder-unregistered.js [--limit=50]
 */
import { getDB, initDB } from './db.js';

/**
 * @returns {{ count: number, items: Array<{商品コード, 商品名, 取扱区分, 直近30日販売数}> }}
 */
export function checkReorderUnregistered(opts = {}) {
  const { limit = 50 } = opts;
  const db = getDB();

  const items = db.prepare(`
    SELECT
      p.商品コード,
      p.商品名,
      p.取扱区分,
      COALESCE(s30.qty, 0) AS 直近30日販売数
    FROM m_products p
    LEFT JOIN m_reorder_setting r ON p.商品コード = r.sku COLLATE NOCASE
    LEFT JOIN (
      SELECT 商品コード, SUM(数量) as qty FROM f_sales_by_product WHERE 日付 >= date('now', '-30 days') GROUP BY 商品コード
    ) s30 ON p.商品コード = s30.商品コード COLLATE NOCASE
    WHERE r.sku IS NULL
      AND p.商品区分 = '単品'
      AND p.取扱区分 = '取扱中'
    ORDER BY 直近30日販売数 DESC, p.new_product_launch_date DESC, p.商品コード
  `).all();

  // count は全件、items は上位 limit 件のみ返す（通知用）
  return { count: items.length, items: items.slice(0, limit) };
}

// ─── CLI ───
const isMain = process.argv[1]?.endsWith('check-reorder-unregistered.js');
if (isMain) {
  const args = process.argv.slice(2);
  const limit = parseInt(args.find(a => a.startsWith('--limit='))?.split('=')[1] || '50', 10);

  await initDB();
  const r = checkReorderUnregistered({ limit });

  console.log('========================================');
  console.log(`【発注設定 未登録】 推奨保有月数なしの取扱中商品: ${r.count}件`);
  console.log('========================================');
  r.items.forEach(it => {
    console.log(`  ${(it.商品コード || '').padEnd(24)} 30日販売${String(it.直近30日販売数).padStart(6)}  ${(it.商品名 || '').slice(0, 30)}`);
  });
  if (r.count > r.items.length) console.log(`  ... (${r.count - r.items.length}件省略)`);

  // 未登録があれば exit 1（警告扱い、運用通知のトリガー）
  process.exit(r.count > 0 ? 1 : 0);
}
