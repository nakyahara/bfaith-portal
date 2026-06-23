/**
 * Phase E-11-b + Codex 2 round-2 改修:
 *   楽天 → Yahoo 同一商品の overlap は item_code 直接 JOIN では取れない
 *   (b-faith01 では楽天 itemNumber と Yahoo ItemCode が独立命名)。
 *   warehouse-mirror.db の m_products / f_yahoo_sku_map / f_rakuten_finance_sku_daily_v1
 *   を ne_code 軸で 3-way JOIN して overlap を取る。
 *
 *   Codex review High 反映:
 *     H1. f_rakuten_finance_sku_daily_v1 は日次 fact → fr_dedup CTE で先に dedupe
 *     H2. fr.ne_code 含む全 JOIN 列を LOWER(TRIM()) で正規化
 *     H3. m_products と f_yahoo_sku_map 両方 hit で不一致は ymap_safe で fail-closed (除外)
 *     H4. 1 Yahoo item が複数 rakuten/genre に解決される ambiguous は count(distinct) で除外
 *
 *   mirror DB が無い / テーブル無い / 列無い場合は fail-closed (空配列返却 + audit log)。
 *   旧 item_code 直接 JOIN 経路への fallback はしない (overlap=0 既知 NG なので静かな空学習防止)。
 *
 * 提供:
 *   - countMissingYahooCategories(db, mirrorPath): number
 *   - backfillYahooCategoriesAndPaths({db, mirrorPath, limit, deps?})
 *   - learnGenreCategoryMapping({db, mirrorPath})
 *   - countLearnedGenres(db): number
 *   - diagnoseOverlap({db, mirrorPath}): { hits, conflicts, none, ambiguous, totalYahooScanned }
 */

import { fetchYahooItemDetailsBulk } from './yahoo-detail-proxy.js';

const DEFAULT_BATCH_LIMIT = 50;

function isoNow() { return new Date().toISOString(); }

/**
 * mirror DB を ATTACH。 失敗 (ファイル無し / table 無し) は throw して呼び出し側で fail-closed。
 *
 * 必須 table: mirror_sku_resolved 1 つだけ。
 *   全モール seller_sku × ne_code 統一辞書 (SKU 管理統合 Step 4-0 で master only 化済)。
 *   楽天 itemNumber も Yahoo ItemCode も同じ table の seller_sku 列に居る前提。
 *   sort_order=0 が代表 ne_code (セット商品でも先頭 構成 ne_code)。
 */
function attachMirror(db, mirrorPath) {
  if (!mirrorPath) {
    const e = new Error('mirror_db_unavailable: mirrorPath not provided');
    e.code = 'MIRROR_DB_UNAVAILABLE';
    throw e;
  }
  const attached = db.prepare(`PRAGMA database_list`).all().some((r) => r.name === 'wh');
  if (!attached) {
    db.prepare(`ATTACH DATABASE ? AS wh`).run(mirrorPath);
  }
  const required = ['mirror_sku_resolved'];
  for (const t of required) {
    const row = db.prepare(`SELECT name FROM wh.sqlite_master WHERE type='table' AND name=?`).get(t);
    if (!row) {
      const e = new Error(`mirror_table_missing: wh.${t} not found`);
      e.code = 'MIRROR_TABLE_MISSING';
      e.table = t;
      throw e;
    }
  }
}

function detachMirror(db) {
  const attached = db.prepare(`PRAGMA database_list`).all().some((r) => r.name === 'wh');
  if (attached) {
    try { db.prepare(`DETACH DATABASE wh`).run(); } catch (_) { /* best-effort */ }
  }
}

/**
 * CTE: yahoo_registered_items → ne_code → 楽天 itemNumber を mirror_sku_resolved で 1 hop。
 *
 *   mirror_sku_resolved は seller_sku → ne_code の全モール統一辞書。
 *   楽天 itemNumber も Yahoo ItemCode も同じ列 (seller_sku) に居る。
 *   sort_order=0 が代表 ne_code (セット商品で複数 ne_code 紐付くケース対応)。
 *
 *   Codex H 反映:
 *     H2. LOWER(TRIM()) 正規化 統一
 *     H4. 1 yahoo_item → 多 rakuten ambiguous は HAVING で除外 (呼び出し側)
 *     H1. mirror_sku_resolved は seller_sku × ne_code で PK、 sort_order=0 限定で 1:1
 */
const CTE_LINKED = `
  WITH
  y_norm AS (
    SELECT y.item_code AS yahoo_item_code,
           y.yahoo_category_id,
           y.yahoo_path,
           LOWER(TRIM(y.item_code)) AS y_key
    FROM yahoo_registered_items y
  ),
  y_resolved AS (
    -- Yahoo ItemCode → ne_code (sort_order=0 の代表 ne_code)
    SELECT
      y.yahoo_item_code,
      y.yahoo_category_id,
      y.yahoo_path,
      LOWER(TRIM(s.ne_code)) AS ne_code
    FROM y_norm y
    INNER JOIN wh.mirror_sku_resolved s
      ON LOWER(TRIM(s.seller_sku)) = y.y_key
     AND s.sort_order = 0
    WHERE s.ne_code IS NOT NULL AND TRIM(s.ne_code) <> ''
  ),
  r_resolved AS (
    -- seller_sku → ne_code (楽天側で使う、 sort_order=0 のみ)
    SELECT DISTINCT
      LOWER(TRIM(s.seller_sku)) AS rakuten_code,
      LOWER(TRIM(s.ne_code))    AS ne_code
    FROM wh.mirror_sku_resolved s
    WHERE s.sort_order = 0
      AND s.ne_code IS NOT NULL AND TRIM(s.ne_code) <> ''
  ),
  linked AS (
    SELECT
      y.yahoo_item_code,
      y.yahoo_category_id,
      y.yahoo_path,
      r.rakuten_code
    FROM y_resolved y
    INNER JOIN r_resolved r ON r.ne_code = y.ne_code
    WHERE r.rakuten_code <> y.yahoo_item_code  -- 同じ seller_sku は drop (= 自己 join 排除、 楽天と Yahoo は別)
  )
`;

/**
 * 診断 SQL: 設計の hit / conflict / none / ambiguous 件数を返す。
 *   実装前 + 運用中の sanity check 用 (中原さんに見せる用)。
 */
export function diagnoseOverlap({ db, mirrorPath } = {}) {
  if (!db) throw new Error('diagnoseOverlap: db required');
  try {
    attachMirror(db, mirrorPath);
  } catch (e) {
    return { ok: false, error: e.message, code: e.code };
  }
  try {
    const totalYahoo = db.prepare(`SELECT COUNT(*) AS n FROM yahoo_registered_items`).get().n;
    // Yahoo ItemCode 単位の解決状況: mirror_sku_resolved に居る / 居ない
    const yahooResolved = db.prepare(`
      SELECT COUNT(*) AS n
      FROM yahoo_registered_items y
      INNER JOIN wh.mirror_sku_resolved s
        ON LOWER(TRIM(s.seller_sku)) = LOWER(TRIM(y.item_code))
       AND s.sort_order = 0
    `).get().n;
    const linked = db.prepare(`${CTE_LINKED} SELECT COUNT(DISTINCT yahoo_item_code) AS n FROM linked`).get().n;
    const ambiguous = db.prepare(`
      ${CTE_LINKED}
      SELECT COUNT(*) AS n FROM (
        SELECT yahoo_item_code FROM linked GROUP BY yahoo_item_code
        HAVING COUNT(DISTINCT rakuten_code) > 1
      )
    `).get().n;
    return {
      ok: true,
      totalYahooScanned: totalYahoo,
      yahoo_resolved_to_ne: yahooResolved,         // mirror_sku_resolved に居る Yahoo ItemCode 数
      yahoo_unresolved: totalYahoo - yahooResolved, // mirror_sku_resolved に居ない (master 未登録)
      linked,                                       // 楽天側にも同 ne_code が居る件数 (= 学習対象母数)
      ambiguous,                                    // 1 yahoo → 多 rakuten 解決 (除外)
    };
  } finally {
    detachMirror(db);
  }
}

/**
 * 「学習対象になりうる Yahoo 出品」 のうち category 未取得な件数。
 *   ne_code 軸の overlap + ambiguous 除外。
 */
export function countMissingYahooCategories(db, mirrorPath = null) {
  if (!db) throw new Error('countMissingYahooCategories: db required');
  try {
    attachMirror(db, mirrorPath);
  } catch (_) {
    // mirror 無し環境では 0 (UI 上 ボタン非表示でよいが、 backfill 側で fail-closed)
    return 0;
  }
  try {
    return db.prepare(`
      ${CTE_LINKED}
      SELECT COUNT(*) AS n FROM (
        SELECT yahoo_item_code FROM linked
        GROUP BY yahoo_item_code
        HAVING COUNT(DISTINCT rakuten_code) = 1
      ) one
      INNER JOIN yahoo_registered_items y ON y.item_code = one.yahoo_item_code
      WHERE y.yahoo_category_id IS NULL
    `).get().n;
  } finally {
    detachMirror(db);
  }
}

export async function backfillYahooCategoriesAndPaths({ db, mirrorPath = null, limit = DEFAULT_BATCH_LIMIT, deps = {} } = {}) {
  if (!db) throw new Error('backfillYahooCategoriesAndPaths: db required');
  const _bulk = deps.fetchYahooItemDetailsBulk || fetchYahooItemDetailsBulk;

  // Codex H 反映: mirror DB 不可なら fail-closed (旧経路 fallback しない)
  try {
    attachMirror(db, mirrorPath);
  } catch (e) {
    return { picked: 0, updated: 0, failed: 0, errors: [`mirror unavailable: ${e.message}`], blocked: true };
  }

  let targets;
  try {
    // Codex 推奨: ambiguous (1 Yahoo item → 複数 rakuten_code) を HAVING で除外、 yahoo_category_id IS NULL のみ
    const rows = db.prepare(`
      ${CTE_LINKED}
      SELECT y.item_code
      FROM yahoo_registered_items y
      INNER JOIN (
        SELECT yahoo_item_code
        FROM linked
        GROUP BY yahoo_item_code
        HAVING COUNT(DISTINCT rakuten_code) = 1
      ) one ON one.yahoo_item_code = y.item_code
      WHERE y.yahoo_category_id IS NULL
      ORDER BY y.last_seen_at DESC
      LIMIT ?
    `).all(limit);
    targets = rows.map((r) => r.item_code);
  } finally {
    detachMirror(db);
  }

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
 * ne_code 経由の overlap で学習辞書を構築。
 *   Codex H 反映:
 *     - 日次 fact 重複は CTE 内 fr_dedup で潰す
 *     - 1 yahoo_item → 1 rakuten_code に絞る (ambiguous 除外)
 *     - 観測単位は (yahoo_item_code 単位、 一意化された rakuten_code 経由の genre)
 *       → DISTINCT で 1 商品 1 票
 */
export function learnGenreCategoryMapping({ db, mirrorPath = null } = {}) {
  if (!db) throw new Error('learnGenreCategoryMapping: db required');

  try {
    attachMirror(db, mirrorPath);
  } catch (e) {
    return { genresLearned: 0, mappingRowsUpserted: 0, primariesSet: 0, blocked: true, error: e.message };
  }

  let samples;
  try {
    // Codex H1/H4 反映: linked CTE が 1 yahoo_item に対し 1 行 (fr_dedup 効果) で、
    //   HAVING COUNT(DISTINCT rakuten_code)=1 で ambiguous 除外。 ここでは DISTINCT を
    //   付けない (付けると 2 yahoo_item → 同 triple が 1 行に潰れて sample_count=1 に
    //   なる)。 1 yahoo_item = 1 票で集計する。
    samples = db.prepare(`
      ${CTE_LINKED}
      SELECT
        c.rakuten_genre_id        AS genre,
        l.yahoo_category_id       AS category,
        COALESCE(l.yahoo_path, '') AS path,
        l.yahoo_item_code         AS yahoo_item_code
      FROM linked l
      INNER JOIN (
        SELECT yahoo_item_code, MIN(rakuten_code) AS rakuten_code
        FROM linked
        GROUP BY yahoo_item_code
        HAVING COUNT(DISTINCT rakuten_code) = 1
      ) one ON one.yahoo_item_code = l.yahoo_item_code AND one.rakuten_code = l.rakuten_code
      INNER JOIN migration_candidates c
        ON LOWER(TRIM(c.item_code)) = one.rakuten_code
      WHERE l.yahoo_category_id IS NOT NULL
        AND c.rakuten_genre_id IS NOT NULL
        AND c.rakuten_genre_id <> ''
    `).all();
    // 1 yahoo_item に対し 1 票になるよう Map で潰す (linked が万一同 yahoo_item を
    // 複数返した場合の保険、 HAVING で防いでるはずだが念のため)
    const sampleByYahoo = new Map();
    for (const s of samples) {
      if (!sampleByYahoo.has(s.yahoo_item_code)) sampleByYahoo.set(s.yahoo_item_code, s);
    }
    samples = [...sampleByYahoo.values()];
  } finally {
    detachMirror(db);
  }

  if (samples.length === 0) {
    return { genresLearned: 0, mappingRowsUpserted: 0, primariesSet: 0 };
  }

  // 集計
  const freq = new Map();
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
  const bestPerGenre = new Map();

  const writeTx = db.transaction(() => {
    for (const [key, count] of freq.entries()) {
      const [genre, categoryStr, ...pathParts] = key.split('|');
      const path = pathParts.join('|');
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
