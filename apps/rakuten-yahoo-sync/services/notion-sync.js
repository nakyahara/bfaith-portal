/**
 * Notion 商品マスター DB → notion_overrides cache の sync orchestrator。
 * ローカル RYS src/services/notion-sync.js の ES module 翻訳 (Codex Phase B1 R1-R4 完成版)。
 *
 * 設計 (Codex Phase B1 提案 + R1-R4 修正):
 *   - syncNotionOverrides({ db, mode, since?, cursor?, dryRun?, fetcher? })
 *   - mode='full': 全件 pull → reconcile (削除検出 ON)
 *   - mode='delta' (将来): last_edited_time フィルター pull → reconcile (削除検出 OFF)
 *   - fetchNotionPages / reconcilePages を分離
 *   - sync 全体を transaction で囲む (snapshot 整合性)
 *   - 削除反映は ページング完走時 + mode='full' のみ
 *   - errors > 0 なら last_successful_sync_at は更新せず last_attempted のみ
 *   - 各行に sync_run_id を記録 (R1 H-2)
 *
 * Status フィルター: 他モール用 4 status を server-side で除外:
 *   ⑩Lineギフト / ⑪AUpay_大輔 / ⑩Qoo10登録 / ⑪メルカリshops登録
 */

import crypto from 'crypto';
import { queryDatabaseAll } from '../lib/notion-client.js';
import { buildSyncRecord } from '../lib/notion-property.js';

export const SYNC_SOURCE = 'notion_overrides';
export const ALLOWED_MODES = ['full', 'delta'];

export const EXCLUDED_STATUSES = ['⑩Lineギフト', '⑪AUpay_大輔', '⑩Qoo10登録', '⑪メルカリshops登録'];

export function buildStatusExclusionFilter() {
  return {
    and: EXCLUDED_STATUSES.map((s) => ({
      property: 'Status',
      select: { does_not_equal: s },
    })),
  };
}

export async function fetchNotionPages({ mode = 'full', since = null, cursor = null, client = null } = {}) {
  const q = client || queryDatabaseAll;
  const filter = buildStatusExclusionFilter();
  if (mode === 'delta' && since) {
    // 将来 C 拡張: filter に last_edited_time 条件を and で足す
    return q({ filter, pageSize: 100 });
  }
  void cursor; // 将来 resume 用
  return q({ filter, pageSize: 100 });
}

/**
 * Notion page 配列を notion_overrides に reconcile する (transaction 内推奨)。
 */
export function reconcilePages(db, pages, { allowDeleteDetection, runId }) {
  const stats = { scanned: pages.length, inserted: 0, updated: 0, skipped: 0, deleted: 0, errors: [] };
  const seenPageIds = new Set();

  const selectByPageId = db.prepare(
    `SELECT rakuten_manage_number, source_hash FROM notion_overrides WHERE notion_page_id = ?`
  );
  // Phase E-11-d: notion_product_category / notion_path 列を sync 経路に追加。
  //   - migration 014 未適用環境では prepare で例外 → fallback (旧 12 列のみ) で進める。
  const NEW_COLUMNS_SUPPORTED = (() => {
    try { db.prepare('SELECT notion_product_category FROM notion_overrides LIMIT 0').get(); return true; }
    catch (_) { return false; }
  })();
  const insertStmt = db.prepare(NEW_COLUMNS_SUPPORTED ? `
    INSERT INTO notion_overrides (
      rakuten_manage_number, yahoo_title, yahoo_price, yahoo_price_sagawa,
      notion_delivery_label, notion_tax_rate, notion_has_variation,
      yahoo_headline, yahoo_caption_text, yahoo_caption_html, yahoo_jan,
      notion_status, notion_product_category, notion_path,
      raw_properties_json, source_hash, notion_page_id,
      source_updated_at, sync_run_id, synced_at
    ) VALUES (
      @rakuten_manage_number, @yahoo_title, @yahoo_price, @yahoo_price_sagawa,
      @notion_delivery_label, @notion_tax_rate, @notion_has_variation,
      @yahoo_headline, @yahoo_caption_text, @yahoo_caption_html, @yahoo_jan,
      @notion_status, @notion_product_category, @notion_path,
      @raw_properties_json, @source_hash, @notion_page_id,
      @source_updated_at, @sync_run_id, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    )
  ` : `
    INSERT INTO notion_overrides (
      rakuten_manage_number, yahoo_title, yahoo_price, yahoo_price_sagawa,
      notion_delivery_label, notion_tax_rate, notion_has_variation,
      yahoo_headline, yahoo_caption_text, yahoo_caption_html, yahoo_jan,
      notion_status, raw_properties_json, source_hash, notion_page_id,
      source_updated_at, sync_run_id, synced_at
    ) VALUES (
      @rakuten_manage_number, @yahoo_title, @yahoo_price, @yahoo_price_sagawa,
      @notion_delivery_label, @notion_tax_rate, @notion_has_variation,
      @yahoo_headline, @yahoo_caption_text, @yahoo_caption_html, @yahoo_jan,
      @notion_status, @raw_properties_json, @source_hash, @notion_page_id,
      @source_updated_at, @sync_run_id, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    )
  `);
  const updateStmt = db.prepare(NEW_COLUMNS_SUPPORTED ? `
    UPDATE notion_overrides SET
      rakuten_manage_number = @rakuten_manage_number,
      yahoo_title           = @yahoo_title,
      yahoo_price           = @yahoo_price,
      yahoo_price_sagawa    = @yahoo_price_sagawa,
      notion_delivery_label = @notion_delivery_label,
      notion_tax_rate       = @notion_tax_rate,
      notion_has_variation  = @notion_has_variation,
      yahoo_headline        = @yahoo_headline,
      yahoo_caption_text    = @yahoo_caption_text,
      yahoo_caption_html    = @yahoo_caption_html,
      yahoo_jan             = @yahoo_jan,
      notion_status         = @notion_status,
      notion_product_category = @notion_product_category,
      notion_path           = @notion_path,
      raw_properties_json   = @raw_properties_json,
      source_hash           = @source_hash,
      source_updated_at     = @source_updated_at,
      sync_run_id           = @sync_run_id,
      synced_at             = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    WHERE notion_page_id = @notion_page_id
  ` : `
    UPDATE notion_overrides SET
      rakuten_manage_number = @rakuten_manage_number,
      yahoo_title           = @yahoo_title,
      yahoo_price           = @yahoo_price,
      yahoo_price_sagawa    = @yahoo_price_sagawa,
      notion_delivery_label = @notion_delivery_label,
      notion_tax_rate       = @notion_tax_rate,
      notion_has_variation  = @notion_has_variation,
      yahoo_headline        = @yahoo_headline,
      yahoo_caption_text    = @yahoo_caption_text,
      yahoo_caption_html    = @yahoo_caption_html,
      yahoo_jan             = @yahoo_jan,
      notion_status         = @notion_status,
      raw_properties_json   = @raw_properties_json,
      source_hash           = @source_hash,
      source_updated_at     = @source_updated_at,
      sync_run_id           = @sync_run_id,
      synced_at             = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    WHERE notion_page_id = @notion_page_id
  `);
  const touchStmt = db.prepare(`
    UPDATE notion_overrides
       SET synced_at    = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
           sync_run_id  = ?
     WHERE notion_page_id = ?
  `);

  for (const page of pages) {
    let record;
    try {
      record = buildSyncRecord(page);
    } catch (e) {
      stats.errors.push({ page_id: page?.id, error: String(e.message || e) });
      continue;
    }
    if (!record) {
      stats.skipped += 1;
      continue;
    }
    seenPageIds.add(record.notion_page_id);
    const recordWithRun = { ...record, sync_run_id: runId };
    try {
      const existing = selectByPageId.get(recordWithRun.notion_page_id);
      if (!existing) {
        insertStmt.run(recordWithRun);
        stats.inserted += 1;
      } else if (existing.source_hash === recordWithRun.source_hash) {
        touchStmt.run(runId, recordWithRun.notion_page_id);
        stats.skipped += 1;
      } else {
        updateStmt.run(recordWithRun);
        stats.updated += 1;
      }
    } catch (e) {
      stats.errors.push({
        page_id: record.notion_page_id,
        rakuten_manage_number: record.rakuten_manage_number,
        error: String(e.message || e),
      });
    }
  }

  // 削除検出: ページング完走 + full mode のみ
  if (allowDeleteDetection) {
    const allPageIdsInDb = db.prepare(`SELECT notion_page_id FROM notion_overrides`).all();
    const deletePks = [];
    for (const { notion_page_id } of allPageIdsInDb) {
      if (!seenPageIds.has(notion_page_id)) deletePks.push(notion_page_id);
    }
    if (deletePks.length > 0) {
      const delStmt = db.prepare(`DELETE FROM notion_overrides WHERE notion_page_id = ?`);
      for (const pid of deletePks) delStmt.run(pid);
      stats.deleted = deletePks.length;
    }
  }
  return stats;
}

/**
 * Notion 商品マスター DB → notion_overrides 同期のエントリポイント。
 */
export async function syncNotionOverrides(opts = {}) {
  const { db, mode = 'full', since = null, cursor = null, dryRun = false, fetcher = null } = opts;
  if (!db) throw new Error('syncNotionOverrides: db is required');
  if (!ALLOWED_MODES.includes(mode)) {
    throw new Error(`syncNotionOverrides: invalid mode "${mode}" (allowed: ${ALLOWED_MODES.join(', ')})`);
  }
  const runId = crypto.randomUUID();
  const startedAt = Date.now();
  const { pages, totalPages } = await fetchNotionPages({ mode, since, cursor, client: fetcher });

  const allowDeleteDetection = mode === 'full';
  const completedAt = new Date().toISOString();

  if (dryRun) {
    let stats;
    const ROLLBACK_SENTINEL = Symbol('dry-run-rollback');
    try {
      db.transaction(() => {
        stats = reconcilePages(db, pages, { allowDeleteDetection, runId });
        const e = new Error('dry-run rollback');
        e.__sentinel = ROLLBACK_SENTINEL;
        throw e;
      })();
    } catch (e) {
      if (e.__sentinel !== ROLLBACK_SENTINEL) throw e;
    }
    return {
      runId, mode, dryRun: true, totalPages,
      ...stats,
      durationMs: Date.now() - startedAt,
    };
  }

  let stats;
  const tx = db.transaction(() => {
    stats = reconcilePages(db, pages, { allowDeleteDetection, runId });
    const hasErrors = stats.errors.length > 0;
    const successfulAt = hasErrors ? null : completedAt;
    const fullAt = !hasErrors && mode === 'full' ? completedAt : null;
    const upsertState = db.prepare(`
      INSERT INTO sync_state (
        source, last_successful_sync_at, last_full_sync_at, last_sync_run_id,
        last_attempted_sync_at, last_error_count
      )
      VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT(source) DO UPDATE SET
        last_successful_sync_at = COALESCE(excluded.last_successful_sync_at, sync_state.last_successful_sync_at),
        last_full_sync_at       = COALESCE(excluded.last_full_sync_at,       sync_state.last_full_sync_at),
        last_sync_run_id        = excluded.last_sync_run_id,
        last_attempted_sync_at  = excluded.last_attempted_sync_at,
        last_error_count        = excluded.last_error_count,
        updated_at              = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    `);
    upsertState.run(SYNC_SOURCE, successfulAt, fullAt, runId, completedAt, stats.errors.length);
  });
  tx();

  return {
    runId, mode, totalPages,
    ...stats,
    durationMs: Date.now() - startedAt,
  };
}
