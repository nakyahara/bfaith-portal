/**
 * Phase E-17 PR1: category_manual / category_decisions の path 欠落 (manual_path_missing) を
 *   Yahoo categorySearch から path を取得して category_default_path に補完し、 一括解消する。
 *
 * 背景 (Codex E-17 R1):
 *   resolveCategoryAndPath は yahoo_path を COALESCE(manual/decisions.yahoo_path, default_path.yahoo_path)
 *   で解決する。 manual_path_missing = その yahoo_category_id が category_default_path にも無い状態。
 *   → 該当 yahoo_category_id の path を categorySearch で取り、 category_default_path に入れれば一括解決。
 *   path は default_path で一元管理 (manual に閉じ込めない)。 取得した category は master にも保存 (PR2 の種)。
 *
 * 全ツリー取得は不要。 path 欠落の distinct yahoo_category_id (数十件) だけ個別 fetch する。
 */

import { fetchCategory } from '../lib/yahoo-category-client.js';

/** yahoo_category_master テーブルが存在するか (migration 018 適用済みか)。 */
function hasMasterTable(db) {
  try { db.prepare('SELECT 1 FROM yahoo_category_master LIMIT 0').get(); return true; }
  catch (_) { return false; }
}

/**
 * path 欠落 (manual/decisions に居るが default_path に無い) distinct yahoo_category_id を集める SQL。
 *   Codex E-17 R1 High: yahoo_path が '' / 空白でも欠落扱い (NULLIF(TRIM())) → COALESCE fallback を塞がない。
 *   Codex E-17 R1 Medium: 一度 categorySearch しても path を組めなかった category は master に
 *     fetch_status='partial' (path NULL) で記録し、 再選択を防ぐ (無限再試行対策)。 master 無い環境では除外しない。
 */
function buildPathMissingSql(excludeAttempted) {
  const attemptedExclusion = excludeAttempted
    ? `AND yahoo_category_id NOT IN (
         SELECT category_id FROM yahoo_category_master
         WHERE path IS NULL AND fetch_status = 'partial'
       )`
    : '';
  return `
    SELECT DISTINCT yahoo_category_id FROM (
      SELECT m.yahoo_category_id
        FROM category_manual m
        LEFT JOIN category_default_path dp ON dp.yahoo_category_id = m.yahoo_category_id
        WHERE NULLIF(TRIM(m.yahoo_path), '') IS NULL AND dp.yahoo_category_id IS NULL
      UNION
      SELECT cd.yahoo_category_id
        FROM category_decisions cd
        LEFT JOIN category_default_path dp ON dp.yahoo_category_id = cd.yahoo_category_id
        WHERE cd.locked = 1 AND cd.ambiguous = 0
          AND NULLIF(TRIM(cd.yahoo_path), '') IS NULL AND dp.yahoo_category_id IS NULL
    )
    WHERE yahoo_category_id IS NOT NULL
    ${attemptedExclusion}
    ORDER BY yahoo_category_id
  `;
}

/** path 欠落の distinct yahoo_category_id 件数 (試行済み partial は除外)。 */
export function countMissingCategoryPaths(db) {
  try {
    const sql = buildPathMissingSql(hasMasterTable(db));
    return db.prepare(`SELECT COUNT(*) AS n FROM (${sql})`).get().n;
  } catch (_) {
    return 0; // テーブル未整備環境
  }
}

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

/**
 * path 欠落 category を categorySearch で取得して category_default_path に補完。
 *
 * @param {object} opts
 * @param {Database} opts.db
 * @param {number}   [opts.limit=30]     1 回で処理する category 数 (Render timeout 回避、 UI で自動ループ)
 * @param {boolean}  [opts.dryRun=false] true なら DB 更新せず 「何件 path 取れるか」 を試算
 * @param {number}   [opts.sleepMs=300]  fetch 間隔 (Yahoo API 連続叩き緩和)
 * @param {object}   [opts.deps]         { fetchCategory } テスト用差し替え
 * @returns {Promise<{picked, processed, repaired, stillMissing, failed, errors}>}
 */
export async function repairMissingCategoryPaths({ db, limit = 30, dryRun = false, sleepMs = 300, deps = {} } = {}) {
  if (!db) throw new Error('repairMissingCategoryPaths: db required');
  const _fetch = deps.fetchCategory || fetchCategory;

  const masterExists = hasMasterTable(db);

  let targets;
  try {
    targets = db.prepare(buildPathMissingSql(masterExists)).all();
  } catch (e) {
    return { picked: 0, processed: 0, repaired: 0, stillMissing: 0, failed: 0, errors: [`query_failed: ${e.message}`] };
  }
  if (targets.length === 0) return { picked: 0, processed: 0, repaired: 0, stillMissing: 0, failed: 0, errors: [] };

  const batch = targets.slice(0, limit);

  const upsertDefaultPath = db.prepare(`
    INSERT INTO category_default_path (yahoo_category_id, yahoo_path)
    VALUES (@categoryId, @path)
    ON CONFLICT(yahoo_category_id) DO UPDATE SET yahoo_path = excluded.yahoo_path
  `);
  // Codex E-17 R1 High: migration 018 未適用環境では master upsert を行わない (500 回避)。
  const upsertMaster = masterExists ? db.prepare(`
    INSERT INTO yahoo_category_master
      (category_id, parent_id, title_short, title_medium, title_long, name, path, raw_json, fetch_status, fetched_at, updated_at)
    VALUES
      (@categoryId, @parentId, @titleShort, @titleMedium, @titleLong, @name, @path, @rawJson, @fetchStatus,
       strftime('%Y-%m-%dT%H:%M:%fZ','now'), strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    ON CONFLICT(category_id) DO UPDATE SET
      parent_id = excluded.parent_id,
      title_short = excluded.title_short,
      title_medium = excluded.title_medium,
      title_long = excluded.title_long,
      name = excluded.name,
      path = excluded.path,
      raw_json = excluded.raw_json,
      fetch_status = excluded.fetch_status,
      updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
  `) : null;

  let repaired = 0;
  let stillMissing = 0;
  let failed = 0;
  const errors = [];

  for (const { yahoo_category_id } of batch) {
    let result;
    try {
      result = await _fetch(yahoo_category_id);
    } catch (e) {
      failed += 1;
      errors.push(`${yahoo_category_id}: ${e.message}`);
      if (sleepMs > 0) await sleep(sleepMs);
      continue;
    }
    const cur = result?.current;
    const path = cur?.path;
    if (!path) {
      stillMissing += 1;          // categorySearch から path を組めなかった
      // Codex E-17 R1 Medium: 再選択防止のため master に partial 記録 (path NULL)。 master 無い環境は記録不可。
      if (!dryRun && masterExists) {
        upsertMaster.run({
          categoryId: yahoo_category_id,
          parentId: cur?.parentId ?? null,
          titleShort: cur?.titleShort ?? null,
          titleMedium: cur?.titleMedium ?? null,
          titleLong: cur?.titleLong ?? null,
          name: cur?.titleMedium ?? null,
          path: null,
          rawJson: result.raw ? JSON.stringify(result.raw) : null,
          fetchStatus: 'partial',
        });
      }
    } else if (!dryRun) {
      const tx = db.transaction(() => {
        upsertDefaultPath.run({ categoryId: yahoo_category_id, path });
        if (masterExists) {
          upsertMaster.run({
            categoryId: yahoo_category_id,
            parentId: cur.parentId ?? null,
            titleShort: cur.titleShort ?? null,
            titleMedium: cur.titleMedium ?? null,
            titleLong: cur.titleLong ?? null,
            name: cur.titleMedium ?? null,
            path,
            rawJson: result.raw ? JSON.stringify(result.raw) : null,
            fetchStatus: 'fetched',
          });
        }
      });
      tx();
      repaired += 1;
    } else {
      repaired += 1;              // dryRun: 取れる件数の試算
    }
    if (sleepMs > 0) await sleep(sleepMs);
  }

  return {
    picked: targets.length,
    processed: batch.length,
    repaired,
    stillMissing,
    failed,
    errors,
  };
}
