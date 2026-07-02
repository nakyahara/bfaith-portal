/**
 * Phase E-6-3: 一括 publish (順次実行 + 進捗追跡).
 *
 * 提供:
 *   - startBulkPublish({ db, itemCodes, triggeredBy }) → batch_id を返して即時実行 (async)
 *   - listFailedItemCodes(db, { limit })              → publish_idempotency.status='failed' で最新 N 件
 *   - getBatchStatus(db, batch_id)                    → 進捗集計
 *   - getBatchItems(db, batch_id)                     → item 単位
 *
 * 設計原則:
 *   - 1 batch = 複数商品の連続 publish。 順次 (Yahoo API レート + Render proxy 負荷)
 *   - publish-executor.executePublish を 1 商品ずつ呼ぶ (E-5b 実 publish と同じ経路、 dual check + idempotency 流用)
 *   - 失敗しても batch は継続、 結果を bulk_publish_batch_items に逐次記録
 *   - 同時 running batch は partial UNIQUE INDEX で 1 件まで (race 対策)
 */

import { executePublish, isPublishEnabled } from './publish-executor.js';
import { fetchAllItemCodes } from '../lib/rakuten-rms-proxy.js';
import { summarizeReasons } from '../lib/reason-translator.js';

function isoNow() { return new Date().toISOString(); }

function insertBatch(db, { total, triggeredBy }) {
  const r = db.prepare(`
    INSERT INTO bulk_publish_batches (total, in_progress, status, triggered_by, started_at)
    VALUES (?, ?, 'running', ?, ?)
  `).run(total, total, triggeredBy, isoNow());
  return r.lastInsertRowid;
}

function insertBatchItem(db, { batchId, itemCode }) {
  const r = db.prepare(`
    INSERT INTO bulk_publish_batch_items (batch_id, item_code, status)
    VALUES (?, ?, 'pending')
  `).run(batchId, itemCode);
  return r.lastInsertRowid;
}

function markItemRunning(db, itemId) {
  db.prepare(`UPDATE bulk_publish_batch_items SET status='running', started_at=? WHERE id=?`)
    .run(isoNow(), itemId);
}

function markItemDone(db, itemId, { status, publishStatus = null, idempotencyKey = null, errorMessage = null }) {
  db.prepare(`
    UPDATE bulk_publish_batch_items
    SET status=?, publish_status=?, idempotency_key=?, error_message=?, finished_at=?
    WHERE id=?
  `).run(status, publishStatus, idempotencyKey, errorMessage ? String(errorMessage).slice(0, 4000) : null, isoNow(), itemId);
}

function updateBatchProgress(db, batchId, { success = 0, failed = 0 }) {
  // -1 in_progress per item finished
  db.prepare(`
    UPDATE bulk_publish_batches
    SET succeeded = succeeded + ?, failed = failed + ?, in_progress = in_progress - ?
    WHERE batch_id = ?
  `).run(success, failed, success + failed, batchId);
}

function finishBatch(db, batchId, finalStatus) {
  db.prepare(`UPDATE bulk_publish_batches SET status=?, finished_at=? WHERE batch_id=?`)
    .run(finalStatus, isoNow(), batchId);
}

/**
 * itemCode (楽天 itemNumber) から manageNumber を解決する。
 *   Codex E-6-3 R1 Critical: 単独 publish の router は manageNumber を fetchAllItemCodes() + jobs.payload_json
 *   で resolve してから executePublish に渡している。 bulk から呼ぶ時も同じ resolve を経由しないと
 *   evaluateItemForPublish 内の RMS fetch / Notion 上書き lookup が失敗 (notion_overrides PK は manage_number)。
 *   batch 全体で fetchAllItemCodes() を 1 回だけ呼んで cache、 各 item は cache + jobs.payload_json fallback。
 */
function resolveManageNumber(db, itemCode, codeMappingCache) {
  // (1) cache (fetchAllItemCodes() の結果) でまず引く
  const fromCache = codeMappingCache?.[itemCode];
  if (typeof fromCache === 'string' && fromCache.trim()) return fromCache.trim();
  // (2) jobs.payload_json の rakuten_manage_number でフォールバック (単独 publish と同経路)
  try {
    const row = db.prepare('SELECT payload_json FROM jobs WHERE item_code = ?').get(itemCode);
    if (row?.payload_json) {
      const payload = JSON.parse(row.payload_json);
      const mn = payload?.rakuten_manage_number;
      if (typeof mn === 'string' && mn.trim()) return mn.trim();
    }
  } catch (_) { /* best-effort */ }
  return null;
}

/**
 * 1 商品 publish して bulk_publish_batch_items を更新する。
 * 返り値: { status, publishStatus, idempotencyKey?, error? }
 *   - status: 'success' / 'failed' / 'skipped' / 'dedupe' / 'flag_off' / 'readiness_blocked'
 */
async function publishOneItem({ db, itemCode, batchItemId, codeMappingCache }) {
  markItemRunning(db, batchItemId);
  try {
    // Codex R1 Critical: manageNumber を resolve してから executePublish へ
    const manageNumber = resolveManageNumber(db, itemCode, codeMappingCache);
    if (!manageNumber) {
      markItemDone(db, batchItemId, {
        status: 'failed',
        publishStatus: 'manage_number_unresolved',
        errorMessage: `Could not resolve manageNumber for itemCode=${itemCode} (not in rakuten-rms-proxy mapping nor jobs.payload_json)`,
      });
      return { status: 'failed', error: 'manage_number_unresolved' };
    }
    // executePublish は status: 'success' | 'failed' | 'publish_failed' | 'dedupe' | 'readiness_blocked' |
    //   'in_progress_conflict' | 'not_implemented' | 'flag_off' | 'lease_lost' | 'fail' を返す。
    const result = await executePublish({
      db, itemCode, manageNumber,
      createdBy: 'bulk',
    });
    // Codex R1 Medium-5: not_implemented を success にすると dashboard 集計が done 扱いしない、 'skipped' に正規化。
    let normalized;
    if (result.status === 'success') normalized = 'success';
    else if (result.status === 'dedupe') normalized = 'dedupe';
    else if (result.status === 'flag_off') normalized = 'flag_off';
    else if (result.status === 'readiness_blocked') normalized = 'readiness_blocked';
    else if (result.status === 'not_implemented') normalized = 'skipped';
    else normalized = 'failed';
    // R7: readiness_blocked は executePublish が返す reasons (本当の理由) を日本語で保存する。
    //   従来は null 保存 → モーダルに総称ラベルしか出ず「なぜ弾かれたか」が見えなかった
    //   (「出せる」一覧は Notion 項目+カテゴリの静的判定で、 出品時は画像/税率整合/バリエーション等の
    //    実チェックが加わるため、 ここで初めて弾かれる商品がある)。
    let errorMessage = result.error || result.message || null;
    if (result.status === 'readiness_blocked' && Array.isArray(result.reasons) && result.reasons.length > 0) {
      errorMessage = summarizeReasons(result.reasons).items.map((i) => i.message).join(' / ');
    }
    markItemDone(db, batchItemId, {
      status: normalized,
      publishStatus: result.status,
      idempotencyKey: result.idempotencyKey || result.idempotency_key || null,
      errorMessage,
    });
    return { status: normalized, publishStatus: result.status, idempotencyKey: result.idempotencyKey || null, error: result.error || null };
  } catch (e) {
    markItemDone(db, batchItemId, {
      status: 'failed',
      publishStatus: 'exception',
      errorMessage: e.message,
    });
    return { status: 'failed', error: e.message };
  }
}

/**
 * batch を作って async に publish を順次実行する。
 *   即時に batch_id を返し、 実行は背景で続行 (HTTP request は long-blocking しない設計)。
 *
 * @returns {{ batchId: number, total: number }}
 */
export function startBulkPublish({ db, itemCodes, triggeredBy = 'manual' }) {
  if (!Array.isArray(itemCodes) || itemCodes.length === 0) {
    const err = new Error('startBulkPublish: itemCodes must be a non-empty array');
    err.statusCode = 400;
    throw err;
  }
  // 重複除去 + trim
  const seen = new Set();
  const cleaned = [];
  for (const raw of itemCodes) {
    if (typeof raw !== 'string') continue;
    const c = raw.trim();
    if (!c || seen.has(c)) continue;
    seen.add(c);
    cleaned.push(c);
  }
  if (cleaned.length === 0) {
    const err = new Error('startBulkPublish: no valid itemCodes after cleaning');
    err.statusCode = 400;
    throw err;
  }

  // kill-switch dual check: bulk endpoint も RYS_PUBLISH_ENABLED=0 で fail-fast
  // (executor 側でも check するが、 早期に 403 を返すのが UX 上良い)
  if (!isPublishEnabled()) {
    const err = new Error('RYS_PUBLISH_ENABLED=0: real publish is disabled (kill-switch)');
    err.statusCode = 403;
    throw err;
  }

  // batch + items を 1 transaction で作る (race 対策、 UNIQUE INDEX は別 batch を防ぐ)
  const setupTx = db.transaction(() => {
    const batchId = insertBatch(db, { total: cleaned.length, triggeredBy });
    const batchItemIds = [];
    for (const code of cleaned) batchItemIds.push(insertBatchItem(db, { batchId, itemCode: code }));
    return { batchId, batchItemIds };
  });
  const { batchId, batchItemIds } = setupTx();

  // Codex R1 Critical: 背景実行の冒頭で 楽天 itemNumber→manageNumber マップを 1 回だけ取得 (cache)。
  // 各 item の publishOneItem 内で cache + jobs.payload_json fallback で resolve。
  // fetchAllItemCodes() 失敗時は cache 空 + fallback で進める (publishOneItem 内で resolve できなければ 'manage_number_unresolved' で failed)。
  (async () => {
    let success = 0, failed = 0;
    let codeMappingCache = null;
    try {
      codeMappingCache = await fetchAllItemCodes();
    } catch (e) {
      console.warn('[bulk-publish] fetchAllItemCodes failed, will fallback to jobs.payload_json per item:', e.message);
      codeMappingCache = {};
    }

    // 背景で順次実行 (Render は同一プロセス、 fire-and-forget OK、 失敗は batch_items + audit に記録)
    for (let i = 0; i < cleaned.length; i++) {
      try {
        const result = await publishOneItem({ db, itemCode: cleaned[i], batchItemId: batchItemIds[i], codeMappingCache });
        if (result.status === 'success' || result.status === 'dedupe') {
          updateBatchProgress(db, batchId, { success: 1 });
          success++;
        } else {
          updateBatchProgress(db, batchId, { failed: 1 });
          failed++;
        }
      } catch (e) {
        // publishOneItem 内で catch 済みだが、 念のため
        markItemDone(db, batchItemIds[i], { status: 'failed', publishStatus: 'exception', errorMessage: e.message });
        updateBatchProgress(db, batchId, { failed: 1 });
        failed++;
      }
    }
    // 全件完了 → batch finalize
    const finalStatus = failed === 0 ? 'completed' : (success === 0 ? 'failed' : 'completed');
    finishBatch(db, batchId, finalStatus);
  })().catch((e) => {
    // 想定外: batch を aborted で閉じる
    try { finishBatch(db, batchId, 'aborted'); } catch (_) {}
    console.error('[bulk-publish] unexpected error:', e);
  });

  return { batchId, total: cleaned.length };
}

/**
 * publish_idempotency.status='failed' な item を最新順に N 件返す。
 *   一度 success がある item は除く (再実行不要)。
 *   Codex R1 Medium-3: GROUP BY item_code + MAX(rowid) で 「item 毎の最新 failed」 を正しく取る。
 */
export function listFailedItemCodes(db, { limit = 100 } = {}) {
  return db.prepare(`
    SELECT item_code, MAX(rowid) AS latest_rowid
    FROM publish_idempotency
    WHERE status='failed'
      AND item_code NOT IN (
        SELECT item_code FROM publish_idempotency WHERE status='success'
      )
    GROUP BY item_code
    ORDER BY latest_rowid DESC
    LIMIT ?
  `).all(limit).map((r) => r.item_code);
}

export function getBatchStatus(db, batchId) {
  return db.prepare(`
    SELECT batch_id, total, succeeded, failed, in_progress, status, triggered_by, started_at, finished_at
    FROM bulk_publish_batches WHERE batch_id = ?
  `).get(batchId) || null;
}

export function getBatchItems(db, batchId) {
  return db.prepare(`
    SELECT id, item_code, status, publish_status, idempotency_key, error_message, started_at, finished_at
    FROM bulk_publish_batch_items
    WHERE batch_id = ?
    ORDER BY id
  `).all(batchId);
}

export function getRunningBatch(db) {
  return db.prepare(`
    SELECT batch_id, total, succeeded, failed, in_progress, started_at
    FROM bulk_publish_batches WHERE status='running'
    LIMIT 1
  `).get() || null;
}
