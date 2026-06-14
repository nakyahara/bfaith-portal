/**
 * Phase E-11-c: 楽天 genre → Yahoo カテゴリ自動推定。
 *
 * 設計:
 *   - genre_yahoo_category_mapping から primary mapping を取得
 *   - resolved category + path を返す
 *   - 未学習 genre は null を返して publish-pipeline 側で blocked にする
 *
 *   E-11-d で Notion override (`notion_overrides.product_category` / `notion_overrides.path`)
 *   が入った場合は そちらを優先する優先順位:
 *     1. Notion override 値 (中原さん確定値)
 *     2. 学習辞書の primary (自動推定値)
 *     3. null (blocked、 学習要)
 *
 * 提供:
 *   - resolveCategoryAndPath({db, rakutenGenreId, notionOverride?}): {category, path, source}
 *     source: 'notion' | 'learned' | 'unresolved'
 *   - resolveByGenreId(db, genreId): {category, path, sample_count} | null
 */

export function resolveByGenreId(db, genreId) {
  if (!db || !genreId) return null;
  try {
    return db.prepare(`
      SELECT yahoo_category_id AS category, yahoo_path AS path, sample_count
      FROM genre_yahoo_category_mapping
      WHERE rakuten_genre_id = ? AND is_primary = 1
      LIMIT 1
    `).get(genreId) || null;
  } catch (_) {
    // migration 013 未適用
    return null;
  }
}

/**
 * Notion override > 学習辞書 > null の優先順位で category/path を返す。
 *
 * @param {object} opts
 * @param {Database} opts.db
 * @param {string|null} opts.rakutenGenreId
 * @param {object|null} [opts.notionOverride]  notion_overrides 行 (product_category, path 列を持つ可能性)
 * @returns {{ category: number|null, path: string|null, source: 'notion'|'learned'|'unresolved', sampleCount?: number }}
 */
export function resolveCategoryAndPath({ db, rakutenGenreId, notionOverride = null } = {}) {
  // 1. Notion override
  if (notionOverride) {
    const nc = notionOverride.product_category;
    const np = notionOverride.path;
    const hasCategory = nc != null && nc !== '' && Number.isFinite(Number(nc));
    const hasPath = typeof np === 'string' && np.trim().length > 0;
    if (hasCategory && hasPath) {
      return { category: Number(nc), path: np.trim(), source: 'notion' };
    }
  }
  // 2. 学習辞書 (rakuten_genre_id から primary mapping)
  if (rakutenGenreId) {
    const learned = resolveByGenreId(db, rakutenGenreId);
    if (learned && Number.isFinite(learned.category) && learned.path) {
      return { category: learned.category, path: learned.path, source: 'learned', sampleCount: learned.sample_count };
    }
  }
  // 3. 解決できず
  return { category: null, path: null, source: 'unresolved' };
}
