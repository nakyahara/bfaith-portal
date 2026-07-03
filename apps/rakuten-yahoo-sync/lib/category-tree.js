/**
 * 再設計 R9: カテゴリツリー (階層ごとの絞り込み選択用)。
 *
 * 中原さん要望 (2026-07-03): 紐付け画面でキーワード検索だけでなく
 * 「カテゴリの階層ごとに絞り込んでいく」選択をしたい。店の棚も同様。
 *
 * データ源 (どちらもローカル、 外部 API 不要):
 *   - Yahoo!商品カテゴリ: yahoo_category_master 12,044 件の path_name ("A > B > C") から構築
 *   - 店の棚 (path):      category_default_path の既知 path ("A:B") から構築
 *     (真の棚一覧は Yahoo stCategoryList API が必要 = 将来課題。 当面は
 *      「実際に使われた実績のある棚」ツリー + 楽天カテゴリからの提案で運用)
 *
 * 設計:
 *   - module cache (category-master-search と同じ件数+MAX(imported_at) キー方式)
 *   - API は「親 path の直下の子」だけ返す (ツリー全体を一度に返さない = 応答小さく保つ)
 */

const YAHOO_SEP = ' > ';
const SHELF_SEP = ':';

let _yahooCache = null; // { key, root }
let _shelfCache = null; // { key, root }

function newNode(name) {
  return { name, children: new Map(), categoryId: null };
}

function insertPath(root, segments, categoryId = null) {
  let node = root;
  for (const seg of segments) {
    if (!node.children.has(seg)) node.children.set(seg, newNode(seg));
    node = node.children.get(seg);
  }
  if (categoryId != null && node.categoryId == null) node.categoryId = categoryId;
  return node;
}

function findNode(root, segments) {
  let node = root;
  for (const seg of segments) {
    node = node.children.get(seg);
    if (!node) return null;
  }
  return node;
}

function yahooCacheKey(db) {
  try {
    const r = db.prepare('SELECT COUNT(*) AS c, MAX(imported_at) AS m FROM yahoo_category_master WHERE is_active = 1').get();
    return `${r.c}:${r.m || ''}`;
  } catch (_) {
    return '0:';
  }
}

function buildYahooTree(db) {
  const root = newNode('');
  const rows = db.prepare(
    'SELECT product_category, name, path_name FROM yahoo_category_master WHERE is_active = 1'
  ).all();
  for (const r of rows) {
    const segs = (r.path_name || r.name || '').split(YAHOO_SEP).map((s) => s.trim()).filter(Boolean);
    if (segs.length === 0) continue;
    insertPath(root, segs, r.product_category);
  }
  return root;
}

function getYahooRoot(db) {
  const key = yahooCacheKey(db);
  if (!_yahooCache || _yahooCache.key !== key) _yahooCache = { key, root: buildYahooTree(db) };
  return _yahooCache.root;
}

function shelfCacheKey(db) {
  try {
    return String(db.prepare('SELECT COUNT(*) AS c FROM category_default_path').get().c);
  } catch (_) {
    return '0';
  }
}

function buildShelfTree(db) {
  const root = newNode('');
  const rows = db.prepare('SELECT DISTINCT yahoo_path FROM category_default_path WHERE yahoo_path IS NOT NULL').all();
  for (const r of rows) {
    const segs = String(r.yahoo_path).split(SHELF_SEP).map((s) => s.trim()).filter(Boolean);
    if (segs.length === 0) continue;
    insertPath(root, segs);
  }
  return root;
}

function getShelfRoot(db) {
  const key = shelfCacheKey(db);
  if (!_shelfCache || _shelfCache.key !== key) _shelfCache = { key, root: buildShelfTree(db) };
  return _shelfCache.root;
}

/** テスト用 */
export function resetCategoryTreeCache() {
  _yahooCache = null;
  _shelfCache = null;
}

/**
 * Yahoo!商品カテゴリツリー: parentPath (' > ' 連結、 '' はトップ) 直下の子一覧。
 *   defaultPath: そのカテゴリ ID に既知の店の棚があれば添付 (選択時に path 自動)
 * @returns {{ ok: true, children: [{ name, fullPath, categoryId, childCount, defaultPath }] } | { ok: false, error }}
 */
export function getYahooTreeChildren(db, parentPath = '') {
  let root;
  try {
    root = getYahooRoot(db);
  } catch (e) {
    return { ok: false, error: `yahoo_category_master 読込失敗: ${e.message}` };
  }
  const segs = String(parentPath || '').split(YAHOO_SEP).map((s) => s.trim()).filter(Boolean);
  const node = findNode(root, segs);
  if (!node) return { ok: false, error: `カテゴリが見つかりません: ${parentPath}` };

  let defaultPathStmt = null;
  try {
    defaultPathStmt = db.prepare('SELECT yahoo_path FROM category_default_path WHERE yahoo_category_id = ?');
  } catch (_) { /* migration 015 未適用 */ }

  const children = [...node.children.values()]
    .sort((a, b) => a.name.localeCompare(b.name, 'ja'))
    .map((c) => {
      let defaultPath = null;
      if (c.categoryId != null && defaultPathStmt) {
        try { defaultPath = defaultPathStmt.get(Number(c.categoryId))?.yahoo_path || null; } catch (_) {}
      }
      return {
        name: c.name,
        fullPath: [...segs, c.name].join(YAHOO_SEP),
        categoryId: c.categoryId,     // null = 中間ノードで ID なし (マスタに存在しない階層)
        childCount: c.children.size,  // 0 = 末端
        defaultPath,
      };
    });
  return { ok: true, children };
}

/**
 * 店の棚ツリー: parentPath (':' 連結、 '' はトップ) 直下の子一覧。
 *   既知 (= category_default_path に実績がある) 棚だけが出る。
 * @returns {{ ok: true, children: [{ name, fullPath, childCount }] } | { ok: false, error }}
 */
export function getShelfTreeChildren(db, parentPath = '') {
  let root;
  try {
    root = getShelfRoot(db);
  } catch (e) {
    return { ok: false, error: `category_default_path 読込失敗: ${e.message}` };
  }
  const segs = String(parentPath || '').split(SHELF_SEP).map((s) => s.trim()).filter(Boolean);
  const node = findNode(root, segs);
  if (!node) return { ok: false, error: `棚が見つかりません: ${parentPath}` };
  const children = [...node.children.values()]
    .sort((a, b) => a.name.localeCompare(b.name, 'ja'))
    .map((c) => ({
      name: c.name,
      fullPath: [...segs, c.name].join(SHELF_SEP),
      childCount: c.children.size,
    }));
  return { ok: true, children };
}
