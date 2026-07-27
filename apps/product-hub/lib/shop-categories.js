/**
 * 楽天の店舗内カテゴリ (お店の棚) マスタと、ドラフトへの割り当て。
 *
 * 一覧は RMS の店舗内カテゴリ画面/CSV からの貼り付けで取り込む (1行1カテゴリ)。
 * RMS Category API での自動取得・商品への自動紐付けは miniPC service-api への
 * ルート追加が必要なため未実装 (miniPC を触れるときに対応)。それまでこの欄は
 * 「非公開登録した商品を公開するとき、RMS 画面でどのカテゴリに載せるか」の指示として使う。
 */

export const MAX_SHOP_CATEGORY_LINES = 1000;
export const MAX_DRAFT_SHOP_CATEGORIES = 30;
const MAX_PATH_LEN = 300;

// 階層区切りは「>」(全角＞も可)。前後の空白を落として ' > ' に正規化する
function normalizePath(raw) {
  return String(raw)
    .split(/[>＞]/)
    .map((s) => s.trim())
    .filter((s) => s !== '')
    .join(' > ');
}

/**
 * 貼り付けテキスト → カテゴリ行。
 * 受ける形式 (1行1カテゴリ):
 *   - 「犬用品 > おやつ」                … パスのみ
 *   - 「123456,犬用品 > おやつ」        … カテゴリID + パス (カンマ or タブ区切り。IDは数字のみ)
 * 戻り値: { ok, rows: [{categoryId, path, pathKey}], duplicates } / { ok:false, error }
 */
export function parseShopCategoryText(text) {
  const lines = String(text || '').split(/\r?\n/);
  if (lines.length > MAX_SHOP_CATEGORY_LINES) {
    return { ok: false, error: `一度に取り込めるのは ${MAX_SHOP_CATEGORY_LINES} 行までです (${lines.length}行)` };
  }
  const rows = [];
  const seen = new Set();
  let duplicates = 0;
  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (!line) continue;
    let categoryId = null;
    let pathPart = line;
    const m = line.match(/^(\d+)\s*[\t,]\s*(.+)$/);
    if (m) { categoryId = m[1]; pathPart = m[2]; }
    const path = normalizePath(pathPart);
    if (!path) continue;
    if (path.length > MAX_PATH_LEN) {
      return { ok: false, error: `カテゴリ名が長すぎます (${MAX_PATH_LEN}文字まで): ${path.slice(0, 40)}…` };
    }
    const pathKey = path.toLowerCase();
    if (seen.has(pathKey)) { duplicates++; continue; }
    seen.add(pathKey);
    rows.push({ categoryId, path, pathKey });
  }
  return { ok: true, rows, duplicates };
}

/**
 * マスタを貼り付け内容で全置き換えする。
 * 行は消さず is_active で外す (ドラフトの選択が参照しているため)。
 * 既知のカテゴリIDは、ID無しで再取り込みされても保持する (COALESCE)。
 */
export function replaceShopCategories(db, rows) {
  const tx = db.transaction(() => {
    db.prepare('UPDATE ph_shop_categories SET is_active = 0').run();
    const upsert = db.prepare(`
      INSERT INTO ph_shop_categories (category_id, path, path_key, is_active, sort_order, imported_at)
      VALUES (?, ?, ?, 1, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
      ON CONFLICT(path_key) DO UPDATE SET
        category_id = COALESCE(excluded.category_id, category_id),
        path = excluded.path,
        is_active = 1,
        sort_order = excluded.sort_order,
        imported_at = excluded.imported_at
    `);
    rows.forEach((r, i) => upsert.run(r.categoryId, r.path, r.pathKey, i));
    return {
      active: db.prepare('SELECT COUNT(*) AS c FROM ph_shop_categories WHERE is_active = 1').get().c,
      deactivated: db.prepare('SELECT COUNT(*) AS c FROM ph_shop_categories WHERE is_active = 0').get().c,
    };
  });
  return tx();
}

export function countActiveShopCategories(db) {
  return db.prepare('SELECT COUNT(*) AS c FROM ph_shop_categories WHERE is_active = 1').get().c;
}

/**
 * 詳細画面用: 有効な全カテゴリ + (一覧から外れたが) このドラフトが選択中のカテゴリ。
 * selected は 0/1。
 */
export function listShopCategoriesForDraft(db, draftId) {
  return db.prepare(`
    SELECT c.id, c.category_id, c.path, c.is_active,
           CASE WHEN s.draft_id IS NULL THEN 0 ELSE 1 END AS selected
    FROM ph_shop_categories c
    LEFT JOIN draft_shop_categories s ON s.shop_category_id = c.id AND s.draft_id = ?
    WHERE c.is_active = 1 OR s.draft_id IS NOT NULL
    ORDER BY c.sort_order, c.id
  `).all(draftId);
}

export function selectedShopCategoryPaths(db, draftId) {
  return db.prepare(`
    SELECT c.path FROM draft_shop_categories s
    JOIN ph_shop_categories c ON c.id = s.shop_category_id
    WHERE s.draft_id = ?
    ORDER BY c.sort_order, c.id
  `).all(draftId).map((r) => r.path);
}

/** ドラフトの選択を丸ごと入れ替える (呼び出し側で存在チェック済みの id 配列)。 */
export function setDraftShopCategories(db, draftId, ids) {
  db.prepare('DELETE FROM draft_shop_categories WHERE draft_id = ?').run(draftId);
  const ins = db.prepare('INSERT INTO draft_shop_categories (draft_id, shop_category_id) VALUES (?, ?)');
  for (const id of ids) ins.run(draftId, id);
}
