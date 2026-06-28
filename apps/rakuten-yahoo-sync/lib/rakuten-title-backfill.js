/**
 * Phase E-9 + E-11-a: migration_candidates.rakuten_title / rakuten_genre_id / rakuten_genre_path
 * を楽天 RMS から bulk fetch して埋める。
 *
 * 設計:
 *   - rakuten_title IS NULL の active 候補 (status='candidate' or 'stale') から N 件選ぶ
 *   - fetchItemDetailsBulkDetailed で manageNumber chunk fetch
 *   - 成功した item は item.itemName (RMS getItem の title 相当) + genre 関連を保存
 *   - 失敗した manage_number は何もしない (次回再試行)
 *   - resolved は触らない (terminal state、 表示に title 不要)
 *
 * E-11-a 追加: 楽天 RMS の response から genre 情報を 「あれば」 保存。
 *   RMS getItem の response shape は item.genreId / item.categories[] / item.tagIds[] の
 *   いずれかが入る可能性があるので、 tolerant extractor で全パターン拾う。
 *
 * 提供:
 *   - backfillRakutenTitles({ db, limit, deps? }): { picked, updated, failed }
 *   - countMissingRakutenTitles(db): number
 */

import { fetchItemDetailsBulkDetailed } from './rakuten-rms-proxy.js';

const DEFAULT_BATCH_LIMIT = 100;

function isoNow() { return new Date().toISOString(); }

/**
 * E-11-a: 楽天 RMS getItem の response から genreId / genrePath を tolerantly 抽出。
 *
 * 楽天 RMS の response shape は API バージョン / 経路で揺れる可能性がある:
 *   - 単純: item.genreId (number / string)
 *   - 旧: item.genres = [{ genreId, genrePath }]
 *   - 階層: item.categories[]?.[i].path
 *   - tag: item.tagIds[] (これは genre とは別、 拾わない)
 *
 * 未知のフィールドは null で返す (列は COALESCE で更新スキップ)。
 *
 * @returns {{ genreId: string|null, genrePath: string|null }}
 */
export function extractRakutenGenre(item) {
  if (!item || typeof item !== 'object') return { genreId: null, genrePath: null };
  let genreId = null;
  let genrePath = null;

  // 1) item.genreId (top-level)
  if (item.genreId != null && item.genreId !== '') {
    genreId = String(item.genreId);
  }
  // 2) item.genres[]
  if (Array.isArray(item.genres) && item.genres.length > 0) {
    const g0 = item.genres[0];
    if (!genreId && g0?.genreId != null) genreId = String(g0.genreId);
    if (!genrePath && typeof g0?.genrePath === 'string') genrePath = g0.genrePath.trim();
    if (!genrePath && typeof g0?.path === 'string') genrePath = g0.path.trim();
  }
  // 3) item.categories[]?.[0].path  (display category 階層、 path 候補に使う)
  if (!genrePath && Array.isArray(item.categories) && item.categories.length > 0) {
    const c0 = item.categories[0];
    if (typeof c0?.path === 'string') genrePath = c0.path.trim();
    if (!genreId && c0?.categoryId != null) genreId = String(c0.categoryId);
  }
  return { genreId: genreId || null, genrePath: genrePath || null };
}

/**
 * Phase E-16: active (candidate / stale) の中で rakuten_genre_id が NULL の件数。
 *   title は既に埋まっているが genre が NULL の取りこぼし (E-11-a 以前に title backfill された分) を数える。
 *   rakuten_manage_number があるものだけ (楽天 RMS で再取得可能なもの)。
 */
export function countMissingRakutenGenre(db) {
  return db.prepare(`
    SELECT COUNT(*) AS n
    FROM migration_candidates
    WHERE status IN ('candidate', 'stale')
      AND (rakuten_genre_id IS NULL OR rakuten_genre_id = '')
      AND rakuten_manage_number IS NOT NULL
      AND rakuten_manage_number <> ''
  `).get().n;
}

/**
 * Phase E-16: rakuten_genre_id が NULL の候補を楽天 RMS から再取得して埋める専用 backfill。
 *
 * 背景: backfillRakutenTitles は `rakuten_title IS NULL` のみを対象とするため、
 *   E-11-a (genre 抽出) 追加より前に title だけ埋まった候補は genre が永久に NULL のまま残る。
 *   この関数は genre_id NULL を起点に再取得し、 genre 列だけを更新する (title は touch しない)。
 *
 * 無限再取得対策: dryRun で事前検証でき、 本実行は中原さんの明示操作 (専用ボタン) のみ。
 *   楽天側に genre 設定が無い SKU は updated されず stillNull に計上され、 中原さんが把握できる。
 *   Codex R1 Medium 既知の制約: stillNull な行は更新されないため、 古い順 limit 件の先頭が
 *   全部 genre 無しだと後続に進めない。 ただし limit=200 + 実データの stillNull は少数の見込みで、
 *   ボタン連打すれば genre 取得済み行は対象から外れて進む。 恒久対策が要るなら試行マーカー列を追加する。
 *
 * @param {object} opts
 * @param {Database} opts.db
 * @param {number}   [opts.limit=100]
 * @param {boolean}  [opts.dryRun=false]  true なら DB 更新せず 「何件 genre 取れるか」 だけ返す
 * @param {object}   [opts.deps]          { fetchItemDetailsBulkDetailed } テスト用差し替え
 * @returns {Promise<{picked, updated, stillNull, failed, errors}>}
 */
export async function backfillRakutenGenre({ db, limit = DEFAULT_BATCH_LIMIT, dryRun = false, deps = {} } = {}) {
  if (!db) throw new Error('backfillRakutenGenre: db required');
  const _bulk = deps.fetchItemDetailsBulkDetailed || fetchItemDetailsBulkDetailed;

  // genre 列が存在しない (migration 012 未適用) 環境では何もしない
  try {
    db.prepare('SELECT rakuten_genre_id FROM migration_candidates LIMIT 0').get();
  } catch (_) {
    return { picked: 0, updated: 0, stillNull: 0, failed: 0, errors: ['genre_columns_missing'] };
  }

  const targets = db.prepare(`
    SELECT item_code, rakuten_manage_number
    FROM migration_candidates
    WHERE status IN ('candidate', 'stale')
      AND (rakuten_genre_id IS NULL OR rakuten_genre_id = '')
      AND rakuten_manage_number IS NOT NULL
      AND rakuten_manage_number <> ''
    ORDER BY first_detected_at ASC
    LIMIT ?
  `).all(limit);

  if (targets.length === 0) return { picked: 0, updated: 0, stillNull: 0, failed: 0, errors: [] };

  // Codex R1 Medium: 同一 manage_number が複数 item_code 行に存在しうる。
  //   batch 境界を picked 行に厳密に限定するため mn → item_code[] で持ち、
  //   UPDATE も item_code IN (...) で picked 行だけに絞る (updated > picked を防止)。
  const itemCodesByMn = new Map();
  for (const t of targets) {
    if (!itemCodesByMn.has(t.rakuten_manage_number)) itemCodesByMn.set(t.rakuten_manage_number, []);
    itemCodesByMn.get(t.rakuten_manage_number).push(t.item_code);
  }
  const manageNumbers = Array.from(itemCodesByMn.keys());

  const { items, failed } = await _bulk(manageNumbers);

  let updated = 0;
  let stillNull = 0;
  const apply = db.transaction(() => {
    for (const item of items) {
      const mn = item?.manageNumber || item?.itemNumber;
      if (!mn) continue;
      const itemCodes = itemCodesByMn.get(mn);
      if (!itemCodes || itemCodes.length === 0) continue;
      const { genreId, genrePath } = extractRakutenGenre(item);
      if (!genreId) { stillNull += 1; continue; }   // 楽天側に genre が無い → 埋められない
      if (dryRun) {
        // dryRun: 本実行の changes と同じ意味になるよう、 picked 対象行のうち実際に
        //   genre IS NULL のままの行数を数える (本実行の UPDATE と同条件)。
        const cnt = db.prepare(`
          SELECT COUNT(*) AS n FROM migration_candidates
          WHERE item_code IN (${itemCodes.map(() => '?').join(',')})
            AND status IN ('candidate', 'stale')
            AND (rakuten_genre_id IS NULL OR rakuten_genre_id = '')
        `).get(...itemCodes).n;
        updated += cnt;
      } else {
        // genre 列のみ更新。 title / last_title_synced_at は touch しない。 picked 行だけに限定。
        const r = db.prepare(`
          UPDATE migration_candidates
          SET rakuten_genre_id   = COALESCE(?, rakuten_genre_id),
              rakuten_genre_path = COALESCE(?, rakuten_genre_path)
          WHERE item_code IN (${itemCodes.map(() => '?').join(',')})
            AND status IN ('candidate', 'stale')
            AND (rakuten_genre_id IS NULL OR rakuten_genre_id = '')
        `).run(genreId, genrePath, ...itemCodes);
        updated += r.changes;
      }
    }
  });
  apply();

  const errors = (failed || []).map((f) => `${f.manageNumber}: ${f.reason}`);
  return {
    picked: targets.length,
    updated,
    stillNull,
    failed: (failed || []).length,
    errors,
  };
}

/**
 * active (candidate / stale) の中で rakuten_title が NULL or 空文字の件数。
 */
export function countMissingRakutenTitles(db) {
  return db.prepare(`
    SELECT COUNT(*) AS n
    FROM migration_candidates
    WHERE status IN ('candidate', 'stale')
      AND (rakuten_title IS NULL OR rakuten_title = '')
      AND rakuten_manage_number IS NOT NULL
      AND rakuten_manage_number <> ''
  `).get().n;
}

/**
 * rakuten_title をまとめて埋める。 楽天 RMS への呼び出しは miniPC proxy 経由。
 *
 * @param {object} opts
 * @param {Database} opts.db
 * @param {number}   [opts.limit=100]  1 回で何件まで埋めるか (proxy 呼び出し回数 = ceil(limit / chunk))
 * @param {object}   [opts.deps]       { fetchItemDetailsBulkDetailed } テスト用差し替え
 * @returns {Promise<{picked: number, updated: number, failed: number, errors: string[]}>}
 */
export async function backfillRakutenTitles({ db, limit = DEFAULT_BATCH_LIMIT, deps = {} } = {}) {
  if (!db) throw new Error('backfillRakutenTitles: db required');
  const _bulk = deps.fetchItemDetailsBulkDetailed || fetchItemDetailsBulkDetailed;

  const targets = db.prepare(`
    SELECT item_code, rakuten_manage_number
    FROM migration_candidates
    WHERE status IN ('candidate', 'stale')
      AND (rakuten_title IS NULL OR rakuten_title = '')
      AND rakuten_manage_number IS NOT NULL
      AND rakuten_manage_number <> ''
    ORDER BY first_detected_at ASC
    LIMIT ?
  `).all(limit);

  if (targets.length === 0) return { picked: 0, updated: 0, failed: 0, errors: [] };

  const byManageNumber = new Map();
  for (const t of targets) byManageNumber.set(t.rakuten_manage_number, t.item_code);

  const manageNumbers = Array.from(byManageNumber.keys());
  const { items, failed } = await _bulk(manageNumbers);

  const ts = isoNow();
  // E-11-a: title だけでなく genre 関連も同時に保存する。 列が migration 012 で追加済み前提だが、
  //         未適用環境では SQLite が 「no such column」 で throw する → fallback で title-only update を試す。
  const updWithGenre = db.prepare(`
    UPDATE migration_candidates
    SET rakuten_title       = @title,
        rakuten_genre_id    = COALESCE(@genreId, rakuten_genre_id),
        rakuten_genre_path  = COALESCE(@genrePath, rakuten_genre_path),
        last_title_synced_at = @ts
    WHERE rakuten_manage_number = @mn
      AND status IN ('candidate', 'stale')
  `);
  const updTitleOnly = db.prepare(`
    UPDATE migration_candidates
    SET rakuten_title = @title, last_title_synced_at = @ts
    WHERE rakuten_manage_number = @mn
      AND status IN ('candidate', 'stale')
  `);
  // どちらの prepared statement を使うか動的に決める
  let useGenre = true;
  try {
    db.prepare('SELECT rakuten_genre_id FROM migration_candidates LIMIT 0').get();
  } catch (_) {
    useGenre = false;
  }

  let updated = 0;
  const writeTx = db.transaction(() => {
    for (const item of items) {
      // RMS getItem の応答: item.itemName (またはレガシー item.title)
      const title = (item?.itemName || item?.title || '').toString().trim();
      const mn = item?.manageNumber || item?.itemNumber;
      if (!mn || !title) continue;
      if (useGenre) {
        const { genreId, genrePath } = extractRakutenGenre(item);
        const r = updWithGenre.run({ title, ts, mn, genreId, genrePath });
        if (r.changes > 0) updated += r.changes;
      } else {
        const r = updTitleOnly.run({ title, ts, mn });
        if (r.changes > 0) updated += r.changes;
      }
    }
  });
  writeTx();

  const errors = (failed || []).map((f) => `${f.manageNumber}: ${f.reason}`);
  return {
    picked: targets.length,
    updated,
    failed: (failed || []).length,
    errors,
  };
}
