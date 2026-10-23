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

/**
 * 2026-10-22: 途中で止まった batch の片付け。
 *   旧実装は batch を裏で回すだけで、途中で Render が再起動 (master への push の自動デプロイ等) すると
 *   status='running' のまま残り、部分 UNIQUE INDEX (running は 1 件) のせいで以後の一括登録・失敗の再実行・
 *   全件再登録が全部 409 になって、DB を手で直すまで止まっていた。
 *   このプロセスが回している batch は ACTIVE_BATCHES に持つ。それ以外の running で、最後の動き (batch の開始・
 *   item の開始/終了) から STALE_BATCH_MS 経った batch は止まったものとみなして aborted にする。
 *   STALE_BATCH_MS は出品 1 件のロック (publish-executor の LEASE_TTL_MS = 30 分) + 余裕 — デプロイの切り替え中に
 *   古いプロセスがまだ回している batch を、新しいプロセスが早まって閉じないように。
 */
const ACTIVE_BATCHES = new Set();
export const STALE_BATCH_MS = 35 * 60 * 1000;

export function recoverStaleBatches(db, { now = Date.now() } = {}) {
  const rows = db.prepare(`
    SELECT b.batch_id,
           MAX(b.started_at, COALESCE(MAX(i.started_at), ''), COALESCE(MAX(i.finished_at), '')) AS last_activity
      FROM bulk_publish_batches b
      LEFT JOIN bulk_publish_batch_items i ON i.batch_id = b.batch_id
     WHERE b.status = 'running'
     GROUP BY b.batch_id
  `).all();
  let recovered = 0;
  for (const r of rows) {
    if (ACTIVE_BATCHES.has(Number(r.batch_id))) continue;
    const last = Date.parse(r.last_activity || '');
    if (Number.isFinite(last) && now - last < STALE_BATCH_MS) continue;
    db.transaction(() => {
      const msg = '中断 (アプリの再起動などで一括登録が途中で止まりました。この商品は登録されていない可能性があります — 「失敗中を再実行」で出し直せます)';
      const left = db.prepare(`
        UPDATE bulk_publish_batch_items SET status = 'failed', publish_status = COALESCE(publish_status, 'aborted'),
               error_message = COALESCE(error_message, ?), finished_at = ?
         WHERE batch_id = ? AND status IN ('pending', 'running')
      `).run(msg, new Date(now).toISOString(), r.batch_id).changes;
      db.prepare(`
        UPDATE bulk_publish_batches SET status = 'aborted', finished_at = ?,
               failed = failed + ?, in_progress = 0
         WHERE batch_id = ? AND status = 'running'
      `).run(new Date(now).toISOString(), left, r.batch_id);
    })();
    console.warn(`[bulk-publish] 止まっていた batch ${r.batch_id} を aborted にしました (最後の動き ${r.last_activity})`);
    recovered++;
  }
  return recovered;
}

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
async function publishOneItem({ db, itemCode, batchItemId, codeMappingCache, expectedValueToken }) {
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
      expectedValueToken,
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
export function startBulkPublish({ db, itemCodes, triggeredBy = 'manual', valueTokens = null }) {
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
    const err = new Error('本番スイッチが OFF (RYS_PUBLISH_ENABLED=0) なので一括登録はできません。1 商品ずつの「🧪 登録テスト」で出品前チェックだけ試せます');
    err.statusCode = 403;
    throw err;
  }

  // 途中で止まった batch が running のまま残っていたら先に片付ける (残っていると UNIQUE INDEX で永久に 409)
  recoverStaleBatches(db);

  // batch + items を 1 transaction で作る (race 対策、 UNIQUE INDEX は別 batch を防ぐ)
  const setupTx = db.transaction(() => {
    const batchId = insertBatch(db, { total: cleaned.length, triggeredBy });
    const batchItemIds = [];
    for (const code of cleaned) batchItemIds.push(insertBatchItem(db, { batchId, itemCode: code }));
    return { batchId, batchItemIds };
  });
  const { batchId, batchItemIds } = setupTx();
  ACTIVE_BATCHES.add(Number(batchId));

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
        const tok = valueTokens && typeof valueTokens[cleaned[i]] === 'string' ? valueTokens[cleaned[i]] : undefined;
        const result = await publishOneItem({ db, itemCode: cleaned[i], batchItemId: batchItemIds[i], codeMappingCache, expectedValueToken: tok });
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
  }).finally(() => {
    ACTIVE_BATCHES.delete(Number(batchId));
  });

  return { batchId, total: cleaned.length };
}

/**
 * **いちばん新しい試行が failed** の item を新しい順に N 件返す。
 *   2026-10-22: 旧実装は「一度でも success があれば除く」だったので、全件再登録 (楽天リンク除去の反映) で
 *   失敗した商品 (過去に一度は成功している) が再実行の対象から漏れ、楽天リンクが Yahoo に残ったままになった。
 *   最新の試行で判定すれば、成功のあとに失敗した商品も拾え、失敗のあとに成功した商品は拾わない。
 *   除外中・手で Yahoo に出した商品は executePublish 側 (publishTargetGuard) で止まる。
 */
export function listFailedItemCodes(db, { limit = 100 } = {}) {
  const failed = db.prepare(`
    SELECT p.item_code, p.rowid AS latest_rowid
    FROM publish_idempotency p
    JOIN (SELECT item_code, MAX(rowid) AS max_rowid FROM publish_idempotency GROUP BY item_code) l
      ON l.item_code = p.item_code AND l.max_rowid = p.rowid
    WHERE p.status = 'failed'
    ORDER BY p.rowid DESC
    LIMIT ?
  `).all(limit).map((r) => r.item_code);
  // 一括登録が中断 (recoverStaleBatches) で終わった商品も拾う (Codex 指摘)。
  //   未着手の商品は publish_idempotency に行が無く、途中だった商品は in_progress のままなので、上の条件では漏れる。
  //   「その商品のいちばん新しい一括の記録が中断」かつ「成功していない」ものだけ。途中だった商品の出品の行は
  //   lease (30 分) が切れていれば executePublish が引き継ぐので、二重に出ない (同じ idempotency key)
  let aborted = [];
  try {
    aborted = db.prepare(`
      SELECT bi.item_code
      FROM bulk_publish_batch_items bi
      JOIN (SELECT item_code, MAX(id) AS max_id FROM bulk_publish_batch_items GROUP BY item_code) lb
        ON lb.item_code = bi.item_code AND lb.max_id = bi.id
      WHERE bi.publish_status = 'aborted'
        -- 中断の「あと」に成功していないもの (全件再登録の中断なら、過去に成功していても拾う — Codex R2)
        AND NOT EXISTS (
          SELECT 1 FROM publish_idempotency p
           WHERE p.item_code = bi.item_code AND p.status = 'success'
             AND COALESCE(p.completed_at, p.updated_at, p.created_at) >= bi.finished_at
        )
      ORDER BY bi.id DESC
      LIMIT ?
    `).all(limit).map((r) => r.item_code);
  } catch (_) { /* migration 009 未適用 */ }
  return [...new Set([...failed, ...aborted])].slice(0, limit);
}

export function getBatchStatus(db, batchId) {
  // 進捗を見ている画面が、止まった batch を running のまま待ち続けないように
  try { recoverStaleBatches(db); } catch (e) { console.warn('[bulk-publish] recoverStaleBatches failed:', e.message); }
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
  try { recoverStaleBatches(db); } catch (e) { console.warn('[bulk-publish] recoverStaleBatches failed:', e.message); }
  return db.prepare(`
    SELECT batch_id, total, succeeded, failed, in_progress, started_at
    FROM bulk_publish_batches WHERE status='running'
    LIMIT 1
  `).get() || null;
}
