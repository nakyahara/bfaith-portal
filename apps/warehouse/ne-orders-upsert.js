/**
 * ne-orders-upsert — raw_ne_orders への書き込みを「状態更新つき UPSERT」に統一する共通ヘルパー
 *
 * 設計監査 2026-07-06 S-5/V-3 対応:
 *   従来の 3 経路 (ne-api.js / csv-import.js / auto-import.js) は全て INSERT OR IGNORE で、
 *   初回取込後のキャンセル・受注状態変化が永久に反映されなかった
 *   (D+1 取込「有効」→ D+2 顧客キャンセル → raw は「有効」のまま →
 *    rebuild-f-sales の WHERE キャンセル区分='有効' を通過 → 発注推奨・商品管理リストの数量過大)。
 *   「NE は数量ベース分析の正」(INV-24) の前提を壊す穴だった。
 *
 * 方針:
 *   ・PK (伝票番号, 明細行番号) 衝突時、状態系 + 数量・金額列を UPDATE
 *   ・識別系 (受注番号/受注日/店舗コード/商品コード/商品名/商品OP/レコードナンバー) は
 *     初回スナップショットを保持 (受注のアイデンティティは不変)
 *   ・WHERE で「実際に値が変わった行だけ」UPDATE — 毎日の 7 日窓再取込で全行を
 *     無意味に書き換えない (WAL 肥大防止) + inserted/updated/unchanged を正しく計上
 *     (従来は skip も「N件挿入」に水増しされ冪等性を観測できなかった)
 *   ・NULL 安全比較のため IS NOT を使用
 *
 * 過去分の癒やし方: この UPSERT は「取込窓に入った行」しか直せない。
 *   窓 (API=直近7日) より古いキャンセルは、NE から広い期間の受注明細 CSV を
 *   エクスポートして csv-import で再投入すれば同じ UPSERT で遡及反映される。
 */

// 19 列 (VALUES の順序は従来の 3 経路と同一)
const COLS = [
  '伝票番号', '受注番号', '受注状態区分', '受注状態', '受注キャンセル',
  '受注キャンセル日', '受注日', '店舗コード', '出荷確定日',
  '明細行番号', 'レコードナンバー', 'キャンセル区分',
  '商品コード', '商品名', '商品OP', '受注数', '引当数', '小計金額', 'synced_at',
];

// 衝突時に更新する列 (状態系 + 数量・金額 + synced_at)
const UPDATE_COLS = [
  '受注状態区分', '受注状態', '受注キャンセル', '受注キャンセル日',
  '出荷確定日', 'キャンセル区分', '受注数', '引当数', '小計金額',
];

const UPSERT_SQL = `
  INSERT INTO raw_ne_orders (${COLS.join(', ')})
  VALUES (${COLS.map(() => '?').join(',')})
  ON CONFLICT(伝票番号, 明細行番号) DO UPDATE SET
    ${UPDATE_COLS.map(c => `${c} = excluded.${c}`).join(',\n    ')},
    synced_at = excluded.synced_at
  WHERE ${UPDATE_COLS.map(c => `raw_ne_orders.${c} IS NOT excluded.${c}`).join('\n     OR ')}
`;

/**
 * @param {import('better-sqlite3').Database} db
 * @returns {(...values: any[]) => 'inserted'|'updated'|'unchanged'}
 *   values は従来の 19 個の位置引数 (伝票番号 が [0]、明細行番号 が [9])
 */
export function makeNeOrdersUpserter(db) {
  const upsertStmt = db.prepare(UPSERT_SQL);
  const existsStmt = db.prepare(
    'SELECT 1 AS x FROM raw_ne_orders WHERE 伝票番号 = ? AND 明細行番号 = ?'
  );
  return function upsertOrderRow(...values) {
    const existed = existsStmt.get(values[0], values[9]) !== undefined;
    const info = upsertStmt.run(...values);
    if (!existed) return 'inserted';
    return info.changes > 0 ? 'updated' : 'unchanged';
  };
}
