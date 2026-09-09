/**
 * rakuten-cost-link-check.mjs — 楽天の「原価がどの NE 品番から来ているか」を調べる (読み取り専用)
 *
 * 🚨 2026-09-09 に紐づけルールを変えた (§16-19)。
 *    旧: システム連携用SKU番号 → SKU管理番号 → 商品番号 の順に**文字列一致**
 *    新: システム連携用SKU番号 → 空欄なら商品番号 (SKU管理番号では決めない)
 *    このスクリプトは **いま f_rakuten_sku_map に入っている旧ルールの結果**を見て、
 *    「SKU管理番号で決めていた = 別商品の原価だったかもしれない」件数を出す。
 *
 * 使い方 (miniPC で。warehouse.db がある機械):
 *   node scripts/expected-profit/rakuten-cost-link-check.mjs                 … 全体の内訳と疑わしい件数
 *   node scripts/expected-profit/rakuten-cost-link-check.mjs treemuddler200  … 1 件を追いかける
 *
 * WAREHOUSE_DB か DATA_DIR/warehouse.db を読む。**書き込みは一切しない**。
 */
import Database from 'better-sqlite3';
import path from 'path';

const file = process.env.WAREHOUSE_DB
  || path.join(process.env.DATA_DIR || path.join(process.cwd(), 'data'), 'warehouse.db');
const db = new Database(file, { readonly: true, fileMustExist: true });
db.pragma('busy_timeout = 5000');

const arg = (process.argv[2] || '').trim().toLowerCase();

const q = (sql, ...p) => { try { return db.prepare(sql).all(...p); } catch (e) { return [{ ERROR: e.message }]; } };
const show = (title, rows) => {
  console.log(`\n=== ${title}`);
  if (!rows.length) { console.log('  (0 件)'); return; }
  console.table(rows.slice(0, 40));
  if (rows.length > 40) console.log(`  … 他 ${rows.length - 40} 件`);
};

if (arg) {
  show(`f_rakuten_sku_map で ${arg} を含む行 (rakuten_code / ne_code / どのコードで決めたか)`,
    q(`SELECT rakuten_code, ne_code, source, manage_number FROM f_rakuten_sku_map
       WHERE LOWER(rakuten_code) LIKE ? OR LOWER(ne_code) LIKE ? OR LOWER(manage_number) LIKE ?`,
      `%${arg}%`, `%${arg}%`, `%${arg}%`));
  show(`m_products で ${arg} を含む商品 (原価はここから引かれる)`,
    q(`SELECT 商品コード, 商品名, 原価, 原価ソース, 原価状態, 取扱区分, 在庫数, 引当数
       FROM m_products WHERE LOWER(商品コード) LIKE ?`, `%${arg}%`));
} else {
  show('どのコードで紐づけたか (am=システム連携用SKU番号 / al=SKU管理番号 / w=商品番号)',
    q('SELECT source, COUNT(*) AS 件数 FROM f_rakuten_sku_map GROUP BY source ORDER BY 件数 DESC'));

  // 🚨 新ルールでは AL では決めない。旧ルールで AL の行のうち、
  //    rakuten_code と ne_code が違うもの = 「別商品を指していたかもしれない」側
  show('🚨 SKU管理番号 (al) で決めていて、指す先が別のコードだった行 = 原価が別商品だった可能性',
    q(`SELECT rakuten_code, ne_code, manage_number FROM f_rakuten_sku_map
       WHERE source = 'al' AND LOWER(rakuten_code) <> LOWER(ne_code)
       ORDER BY rakuten_code`));

  // 🚨 今回の事故と同じ形: SKU管理番号 = 商品管理番号 で、その文字列が NE の商品コードとしても実在する
  show('🚨 今回と同じ形 (SKU管理番号 = 商品管理番号 で、その文字列が NE の商品コードとしても実在)',
    q(`SELECT m.rakuten_code, m.ne_code, m.source, m.manage_number, p.商品名, p.原価
       FROM f_rakuten_sku_map m
       JOIN m_products p ON LOWER(p.商品コード) = LOWER(m.rakuten_code)
       WHERE LOWER(m.rakuten_code) = LOWER(m.manage_number)
       ORDER BY m.rakuten_code`));

  // 🚨 商品番号 (w) は 1 商品ページに 1 つ。同じページに複数の ne_code がぶら下がっていると、
  //    どの SKU の答えが w 行に入ったかで結果が変わる (Codex P1 の母数)
  show('🚨 同じ商品管理番号に複数の ne_code がぶら下がっている商品ページ (色違いで原価が混ざりうる)',
    q(`SELECT manage_number, COUNT(DISTINCT ne_code) AS ne件数, GROUP_CONCAT(DISTINCT ne_code) AS ne_codes
       FROM f_rakuten_sku_map WHERE manage_number IS NOT NULL
       GROUP BY manage_number HAVING ne件数 > 1 ORDER BY ne件数 DESC`));
}

db.close();
