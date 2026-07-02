/**
 * 再設計 R1: Yahoo 公式カテゴリマスタ (yahoo_category_master、 migration 019/020) の
 * キーワード lexical 検索。 紐付け画面 v2 の「候補から選ぶ」と、 AI 初期紐づけ (R2) の
 * 候補生成で使う。
 *
 * 設計:
 *   - 12,044 件を全走査しても数十 ms なので転置インデックスは持たない。
 *     ただし正規化 + バイグラム化は重いので module cache に持つ (行数変化で再構築)。
 *   - スコア: 名前への完全包含 > 名前バイグラム類似 + パス一致ボーナス。
 *     短いカテゴリ名 (正規化後 3 文字未満) は偶発一致するので完全包含でも 1.0 にしない
 *     (ローカル RYS Phase B の知見)。
 */

import { normalizeForMatch, charBigrams, jaccard, containsNormalized } from './text-normalize.js';

const MIN_CONTAIN_LEN = 3;

let _cache = null; // { count, list: [{ productCategory, name, pathName, nameNorm, nameBigrams, pathSegsNorm }] }

function buildIndex(db) {
  const rows = db.prepare(
    'SELECT product_category, name, path_name FROM yahoo_category_master WHERE is_active = 1'
  ).all();
  const list = rows.map((r) => {
    const nameNorm = normalizeForMatch(r.name);
    return {
      productCategory: r.product_category,
      name: r.name,
      pathName: r.path_name || '',
      nameNorm,
      nameBigrams: charBigrams(nameNorm),
      pathSegsNorm: (r.path_name || '').split('>').map((s) => normalizeForMatch(s)).filter(Boolean),
    };
  });
  return { count: rows.length, list };
}

function getIndex(db) {
  const count = countCategoryMaster(db);
  if (!_cache || _cache.count !== count) _cache = buildIndex(db);
  return _cache;
}

export function countCategoryMaster(db) {
  try {
    return db.prepare('SELECT COUNT(*) AS c FROM yahoo_category_master WHERE is_active = 1').get().c;
  } catch (_) {
    return 0; // migration 019 未適用
  }
}

/** テスト用: module cache を破棄 (別 DB を跨ぐテストで使う)。 */
export function resetCategoryMasterCache() {
  _cache = null;
}

function scoreEntry(entry, qNorm, qBigrams) {
  let score = 0;
  // 名前スコア
  const nameLen = entry.nameNorm.replace(/\s+/g, '').length;
  if (nameLen >= MIN_CONTAIN_LEN && (containsNormalized(entry.nameNorm, qNorm) || containsNormalized(qNorm, entry.nameNorm))) {
    score = 1;
  } else {
    score = jaccard(qBigrams, entry.nameBigrams);
  }
  // パス一致ボーナス (末端名以外の階層にクエリ語が現れたら少し持ち上げる)
  let pathHit = 0;
  for (const seg of entry.pathSegsNorm) {
    if (seg === entry.nameNorm) continue;
    if (seg.length >= MIN_CONTAIN_LEN && containsNormalized(seg, qNorm)) { pathHit = 0.15; break; }
    if (jaccard(qBigrams, charBigrams(seg)) >= 0.5) pathHit = Math.max(pathHit, 0.1);
  }
  return Math.min(1, score + pathHit);
}

/**
 * キーワード検索。 返りは score 降順 top-N。
 *   defaultPath: category_default_path に既知の店カテゴリ path があれば付ける
 *   (選択時に path を自動補完できる = 紐付け画面 v2 の「ID を選べば path も入る」動線)。
 */
export function searchCategoryMaster(db, query, { limit = 20 } = {}) {
  const qNorm = normalizeForMatch(query);
  if (!qNorm) return [];
  const qBigrams = charBigrams(qNorm);
  const { list } = getIndex(db);

  const scored = [];
  for (const entry of list) {
    const score = scoreEntry(entry, qNorm, qBigrams);
    if (score >= 0.15) scored.push({ entry, score });
  }
  scored.sort((a, b) => b.score - a.score || a.entry.name.localeCompare(b.entry.name, 'ja'));
  const top = scored.slice(0, limit);

  let defaultPathStmt = null;
  try {
    defaultPathStmt = db.prepare('SELECT yahoo_path FROM category_default_path WHERE yahoo_category_id = ?');
  } catch (_) { /* migration 015 未適用 */ }

  return top.map(({ entry, score }) => {
    let defaultPath = null;
    if (defaultPathStmt) {
      try {
        const row = defaultPathStmt.get(Number(entry.productCategory));
        defaultPath = row?.yahoo_path || null;
      } catch (_) { /* noop */ }
    }
    return {
      productCategory: entry.productCategory,
      name: entry.name,
      pathName: entry.pathName,
      score: Math.round(score * 100) / 100,
      defaultPath,
    };
  });
}
