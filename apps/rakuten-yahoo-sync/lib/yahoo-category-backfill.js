/**
 * Phase E-11-b: Yahoo 既存出品商品の category/path を取得 + 学習辞書を構築する。
 *
 * 流れ:
 *   1. backfillYahooCategoriesAndPaths(db, { limit })
 *      yahoo_registered_items で yahoo_category_id IS NULL の row を N 件選び、
 *      VPS proxy 経由で Yahoo getItemDetail を叩いて category/path を保存。
 *   2. learnGenreCategoryMapping(db)
 *      yahoo_registered_items × migration_candidates (resolved も含む) を JOIN し、
 *      (rakuten_genre_id, yahoo_category_id, yahoo_path) の頻度を集計。
 *      各 genre で最頻 mapping に is_primary=1 を立てる。
 *
 * 提供:
 *   - countMissingYahooCategories(db): number
 *   - backfillYahooCategoriesAndPaths({db, limit, deps?}): {picked, updated, failed, errors}
 *   - learnGenreCategoryMapping(db): {genresLearned, mappingRowsUpserted, primariesSet}
 *   - countLearnedGenres(db): number
 */

import { fetchYahooItemDetailsBulk } from './yahoo-detail-proxy.js';

const DEFAULT_BATCH_LIMIT = 50;

function isoNow() { return new Date().toISOString(); }

export function countMissingYahooCategories(db) {
  return db.prepare(`
    SELECT COUNT(*) AS n
    FROM yahoo_registered_items
    WHERE yahoo_category_id IS NULL
  `).get().n;
}

export async function backfillYahooCategoriesAndPaths({ db, limit = DEFAULT_BATCH_LIMIT, deps = {} } = {}) {
  if (!db) throw new Error('backfillYahooCategoriesAndPaths: db required');
  const _bulk = deps.fetchYahooItemDetailsBulk || fetchYahooItemDetailsBulk;

  const targets = db.prepare(`
    SELECT item_code
    FROM yahoo_registered_items
    WHERE yahoo_category_id IS NULL
    ORDER BY last_seen_at DESC
    LIMIT ?
  `).all(limit).map((r) => r.item_code);

  if (targets.length === 0) return { picked: 0, updated: 0, failed: 0, errors: [] };

  const { items, failed } = await _bulk(targets, { concurrency: 3 });

  const ts = isoNow();
  const upd = db.prepare(`
    UPDATE yahoo_registered_items
    SET yahoo_category_id      = @category,
        yahoo_path             = @path,
        last_detail_synced_at  = @ts
    WHERE item_code = @code
  `);
  let updated = 0;
  const writeTx = db.transaction(() => {
    for (const it of items) {
      const code = it.ItemCode;
      if (!code) continue;
      // category が取れた行のみ更新 (path だけ取れて category null は学習に使えない)
      if (it.ProductCategory == null) continue;
      const r = upd.run({ code, category: it.ProductCategory, path: it.Path || '', ts });
      if (r.changes > 0) updated += r.changes;
    }
  });
  writeTx();

  const errors = (failed || []).map((f) => `${f.itemCode}: ${f.reason}`);
  return { picked: targets.length, updated, failed: failed.length, errors };
}

/**
 * (rakuten_genre_id, yahoo_category_id, yahoo_path) の頻度を集計して
 * 学習辞書を upsert。 各 genre で最頻のマッピングに is_primary=1 を立てる。
 *
 * 入力データ:
 *   - migration_candidates (resolved 含む) で rakuten_genre_id が入ってる row
 *   - yahoo_registered_items で yahoo_category_id が入ってる row
 *   - 両者を item_code でJOIN (= 同一商品)
 *
 *   ※ 楽天 manage_number と Yahoo item_code が一致するかどうかは別問題:
 *      migration_candidates.item_code が楽天 itemNumber、
 *      yahoo_registered_items.item_code が Yahoo ItemCode。
 *      命名規約が同一なら overlap が成立する (E-7-b の overlap check で確認済)。
 */
export function learnGenreCategoryMapping(db) {
  if (!db) throw new Error('learnGenreCategoryMapping: db required');

  const samples = db.prepare(`
    SELECT
      c.rakuten_genre_id AS genre,
      y.yahoo_category_id AS category,
      COALESCE(y.yahoo_path, '') AS path
    FROM yahoo_registered_items y
    INNER JOIN migration_candidates c ON c.item_code = y.item_code
    WHERE y.yahoo_category_id IS NOT NULL
      AND c.rakuten_genre_id IS NOT NULL
      AND c.rakuten_genre_id <> ''
  `).all();

  if (samples.length === 0) {
    return { genresLearned: 0, mappingRowsUpserted: 0, primariesSet: 0 };
  }

  // 集計: 各 (genre, category, path) の件数
  const freq = new Map();  // key = genre|category|path, val = count
  for (const s of samples) {
    const key = `${s.genre}|${s.category}|${s.path}`;
    freq.set(key, (freq.get(key) || 0) + 1);
  }

  const ts = isoNow();
  const upsert = db.prepare(`
    INSERT INTO genre_yahoo_category_mapping
      (rakuten_genre_id, yahoo_category_id, yahoo_path, sample_count, is_primary, first_learned_at, last_learned_at)
    VALUES (@genre, @category, @path, @count, 0, @ts, @ts)
    ON CONFLICT(rakuten_genre_id, yahoo_category_id, yahoo_path) DO UPDATE SET
      sample_count    = excluded.sample_count,
      last_learned_at = excluded.last_learned_at
  `);
  const resetPrimary = db.prepare(`
    UPDATE genre_yahoo_category_mapping SET is_primary = 0 WHERE rakuten_genre_id = ?
  `);
  const setPrimary = db.prepare(`
    UPDATE genre_yahoo_category_mapping
    SET is_primary = 1
    WHERE rakuten_genre_id = @genre AND yahoo_category_id = @category AND yahoo_path = @path
  `);

  let mappingRowsUpserted = 0;
  let primariesSet = 0;
  const genresSeen = new Set();
  const bestPerGenre = new Map();  // genre → {category, path, count}

  const writeTx = db.transaction(() => {
    for (const [key, count] of freq.entries()) {
      const [genre, categoryStr, ...pathParts] = key.split('|');
      const path = pathParts.join('|');  // path に '|' が入る可能性に念のため対応
      const category = parseInt(categoryStr, 10);
      if (!Number.isFinite(category)) continue;
      upsert.run({ genre, category, path, count, ts });
      mappingRowsUpserted += 1;
      genresSeen.add(genre);

      const best = bestPerGenre.get(genre);
      if (!best || count > best.count || (count === best.count && category < best.category)) {
        bestPerGenre.set(genre, { category, path, count });
      }
    }
    // 各 genre で最頻 mapping のみ is_primary=1 (他は 0 にリセット)
    for (const [genre, best] of bestPerGenre.entries()) {
      resetPrimary.run(genre);
      const r = setPrimary.run({ genre, category: best.category, path: best.path });
      if (r.changes > 0) primariesSet += 1;
    }
  });
  writeTx();

  return {
    genresLearned: genresSeen.size,
    mappingRowsUpserted,
    primariesSet,
  };
}

export function countLearnedGenres(db) {
  try {
    return db.prepare(`
      SELECT COUNT(DISTINCT rakuten_genre_id) AS n FROM genre_yahoo_category_mapping
    `).get().n;
  } catch (_) {
    return 0;
  }
}
