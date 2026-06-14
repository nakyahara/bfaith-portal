/**
 * Phase E-9: migration_candidates.rakuten_title を楽天 RMS から bulk fetch して埋める。
 *
 * 設計:
 *   - rakuten_title IS NULL の active 候補 (status='candidate' or 'stale') から N 件選ぶ
 *   - fetchItemDetailsBulkDetailed で manageNumber chunk fetch
 *   - 成功した item は item.itemName (RMS getItem の title 相当) を保存
 *   - 失敗した manage_number は何もしない (次回再試行)
 *   - resolved は触らない (terminal state、 表示に title 不要)
 *
 * 提供:
 *   - backfillRakutenTitles({ db, limit, deps? }): { picked, updated, failed }
 *   - countMissingRakutenTitles(db): number
 */

import { fetchItemDetailsBulkDetailed } from './rakuten-rms-proxy.js';

const DEFAULT_BATCH_LIMIT = 100;

function isoNow() { return new Date().toISOString(); }

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
  const upd = db.prepare(`
    UPDATE migration_candidates
    SET rakuten_title = @title, last_title_synced_at = @ts
    WHERE rakuten_manage_number = @mn
      AND status IN ('candidate', 'stale')
  `);

  let updated = 0;
  const writeTx = db.transaction(() => {
    for (const item of items) {
      // RMS getItem の応答: item.itemName (またはレガシー item.title)
      const title = (item?.itemName || item?.title || '').toString().trim();
      const mn = item?.manageNumber || item?.itemNumber;
      if (!mn || !title) continue;
      const r = upd.run({ title, ts, mn });
      if (r.changes > 0) updated += r.changes;
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
