/**
 * 旧（sku_map 直参照）vs 新（v_sku_costed）の集計差分を出力する並行検証スクリプト
 *
 * 既存スクリプトを実際に v_sku_costed に切り替える前に、これを毎日走らせて
 * 「同じ期間、同じ受注データから、同じ数字が出るか」を粒度別に確認する。
 *
 * 比較粒度（Codex推奨の4種類）:
 *   1. 日次総原価
 *   2. 日次未解決件数
 *   3. seller_sku 単位の解決状態件数
 *   4. ne_code × 売上日 単位の数量差分
 *
 * 使い方:
 *   node apps/warehouse/validate-sku-migration.js [--days=30]
 */
import { getDB, initDB } from './db.js';

export function validateMigration(opts = {}) {
  const { days = 30 } = opts;
  const db = getDB();
  const sinceClause = `date('now', '-${days} days')`;

  // ===== 1. 日次総原価の比較 =====
  // 旧: sku_map 直参照
  const oldTotal = db.prepare(`
    SELECT
      date(o.purchase_date) AS 売上日,
      SUM(CASE WHEN p.原価 IS NULL THEN NULL ELSE o.quantity * COALESCE(s.数量, 1) * p.原価 END) AS 原価合計,
      SUM(CASE WHEN p.原価 IS NULL THEN 1 ELSE 0 END) AS 原価unresolved件数,
      COUNT(*) AS 総行数
    FROM raw_sp_orders o
    LEFT JOIN sku_map s ON o.seller_sku = s.seller_sku
    LEFT JOIN raw_ne_products p ON s.ne_code = p.商品コード
    WHERE o.purchase_date >= ${sinceClause}
    GROUP BY date(o.purchase_date)
    ORDER BY 売上日
  `).all();

  // 新: v_sku_costed
  const newTotal = db.prepare(`
    SELECT
      date(o.purchase_date) AS 売上日,
      SUM(CASE WHEN c.単価 IS NULL THEN NULL ELSE o.quantity * c.数量 * c.単価 END) AS 原価合計,
      SUM(CASE WHEN c.単価 IS NULL THEN 1 ELSE 0 END) AS 原価unresolved件数,
      COUNT(*) AS 総行数
    FROM raw_sp_orders o
    LEFT JOIN v_sku_costed c ON o.seller_sku = c.seller_sku
    WHERE o.purchase_date >= ${sinceClause}
    GROUP BY date(o.purchase_date)
    ORDER BY 売上日
  `).all();

  // 突き合わせ
  const dailyMap = new Map();
  oldTotal.forEach(r => dailyMap.set(r.売上日, { date: r.売上日, old: r, new: null }));
  newTotal.forEach(r => {
    const e = dailyMap.get(r.売上日);
    if (e) e.new = r;
    else dailyMap.set(r.売上日, { date: r.売上日, old: null, new: r });
  });
  const dailyDiff = [...dailyMap.values()]
    .map(d => ({
      date: d.date,
      old_total: d.old?.原価合計 ?? null,
      new_total: d.new?.原価合計 ?? null,
      diff: (d.new?.原価合計 ?? 0) - (d.old?.原価合計 ?? 0),
      old_unresolved: d.old?.原価unresolved件数 ?? 0,
      new_unresolved: d.new?.原価unresolved件数 ?? 0,
      old_rows: d.old?.総行数 ?? 0,
      new_rows: d.new?.総行数 ?? 0,
    }))
    .filter(d => d.diff !== 0 || d.old_unresolved !== d.new_unresolved || d.old_rows !== d.new_rows);

  // ===== 2. 解決状態のSKU単位サマリ =====
  const skuStatus = db.prepare(`
    SELECT
      old_status,
      new_status,
      COUNT(*) AS sku数
    FROM (
      SELECT
        o.seller_sku,
        CASE
          WHEN s.seller_sku IS NULL THEN 'old_unmapped'
          WHEN po.原価 IS NULL THEN 'old_cost_missing'
          ELSE 'old_ok'
        END AS old_status,
        CASE
          WHEN c.seller_sku IS NULL THEN 'new_unmapped'
          WHEN c.単価 IS NULL THEN 'new_cost_missing'
          ELSE 'new_ok'
        END AS new_status
      FROM (SELECT DISTINCT seller_sku FROM raw_sp_orders WHERE purchase_date >= ${sinceClause}) o
      LEFT JOIN sku_map s ON o.seller_sku = s.seller_sku
      LEFT JOIN raw_ne_products po ON s.ne_code = po.商品コード
      LEFT JOIN v_sku_costed c ON o.seller_sku = c.seller_sku
      GROUP BY o.seller_sku
    )
    GROUP BY old_status, new_status
    ORDER BY sku数 DESC
  `).all();

  // ===== 3. 旧→新で「解決→未解決」になったSKU（要注意） =====
  const regressed = db.prepare(`
    SELECT
      o.seller_sku,
      COUNT(*) AS 注文数
    FROM raw_sp_orders o
    INNER JOIN sku_map s ON o.seller_sku = s.seller_sku
    INNER JOIN raw_ne_products po ON s.ne_code = po.商品コード
    LEFT JOIN v_sku_costed c ON o.seller_sku = c.seller_sku
    WHERE o.purchase_date >= ${sinceClause}
      AND po.原価 IS NOT NULL  -- 旧で解決していた
      AND (c.seller_sku IS NULL OR c.単価 IS NULL)  -- 新で未解決
    GROUP BY o.seller_sku
    ORDER BY 注文数 DESC
    LIMIT 50
  `).all();

  // ===== 4. SKU別の構成展開行数差分（水増し検知） =====
  const expansionDiff = db.prepare(`
    SELECT
      o.seller_sku,
      old_rows,
      new_rows,
      new_rows - old_rows AS diff
    FROM (
      SELECT
        sub.seller_sku,
        sub.old_rows,
        sub.new_rows
      FROM (
        SELECT
          o.seller_sku AS seller_sku,
          (SELECT COUNT(*) FROM sku_map s WHERE s.seller_sku = o.seller_sku) AS old_rows,
          (SELECT COUNT(*) FROM v_sku_resolved v WHERE v.seller_sku = o.seller_sku) AS new_rows
        FROM (SELECT DISTINCT seller_sku FROM raw_sp_orders WHERE purchase_date >= ${sinceClause}) o
      ) sub
      WHERE sub.old_rows != sub.new_rows
    ) o
    ORDER BY ABS(diff) DESC
    LIMIT 50
  `).all();

  return {
    daily_diff: dailyDiff,
    sku_status_matrix: skuStatus,
    regressed_skus: regressed,
    expansion_diff: expansionDiff,
  };
}

// ─── CLI ───
const isMain = process.argv[1]?.endsWith('validate-sku-migration.js');
if (isMain) {
  const days = parseInt(process.argv.slice(2).find(a => a.startsWith('--days='))?.split('=')[1] || '30', 10);

  await initDB();
  const r = validateMigration({ days });

  console.log('========================================');
  console.log(`【1】日次差分（直近${days}日、差異がある日のみ）`);
  console.log('========================================');
  if (r.daily_diff.length === 0) {
    console.log('  完全一致 ✓');
  } else {
    console.log('  日付       旧総原価       新総原価       差分      旧unres 新unres 旧行 新行');
    r.daily_diff.slice(0, 30).forEach(d => {
      console.log(`  ${d.date}  ${String(Math.round(d.old_total ?? 0)).padStart(13)}  ${String(Math.round(d.new_total ?? 0)).padStart(13)}  ${String(Math.round(d.diff)).padStart(8)}  ${String(d.old_unresolved).padStart(6)}  ${String(d.new_unresolved).padStart(6)}  ${String(d.old_rows).padStart(4)} ${String(d.new_rows).padStart(4)}`);
    });
    if (r.daily_diff.length > 30) console.log(`  ... (${r.daily_diff.length - 30}日省略)`);
  }

  console.log('');
  console.log('========================================');
  console.log('【2】SKU解決状態の遷移マトリクス');
  console.log('========================================');
  console.log('  旧状態              → 新状態              SKU数');
  r.sku_status_matrix.forEach(s => {
    console.log(`  ${s.old_status.padEnd(20)} → ${s.new_status.padEnd(20)} ${String(s.sku数).padStart(7)}`);
  });

  console.log('');
  console.log('========================================');
  console.log(`【3】退化SKU（旧で解決→新で未解決）: ${r.regressed_skus.length}件`);
  console.log('========================================');
  if (r.regressed_skus.length > 0) {
    r.regressed_skus.slice(0, 20).forEach(s => {
      console.log(`  ${s.seller_sku.padEnd(35)} 注文${String(s.注文数).padStart(5)}回`);
    });
  }

  console.log('');
  console.log('========================================');
  console.log(`【4】構成展開の行数差分（水増し検知）: ${r.expansion_diff.length}件`);
  console.log('========================================');
  if (r.expansion_diff.length > 0) {
    console.log('  seller_sku                          旧行 → 新行  差分');
    r.expansion_diff.slice(0, 20).forEach(s => {
      const sign = s.diff > 0 ? '+' : '';
      console.log(`  ${s.seller_sku.padEnd(35)} ${String(s.old_rows).padStart(4)} → ${String(s.new_rows).padStart(4)}  ${sign}${s.diff}`);
    });
  }

  process.exit(0);
}
