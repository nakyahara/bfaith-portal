/**
 * RakutenYahooSync (RYS) — bfaith-portal app router (Phase E-2: Notion sync 追加)
 *
 * 設計原則 (Codex Phase E R3/R4 確定):
 *   - 楽天 RMS は miniPC proxy 経由 (E-3 以降で実装)
 *   - Notion は Render 直接 (RYS_NOTION_TOKEN)
 *   - secret 値は UI / DB / log に出さない
 *   - RYS state は 専用 SQLite (rakuten-yahoo-sync.db)
 *   - 実 publish は RYS_PUBLISH_ENABLED=0 default
 *
 * E-2 で追加:
 *   - GET  /api/notion/sync/status  : sync_state + readiness summary
 *   - POST /api/notion/sync          : 手動 sync 実行 (body: {dryRun?, mode?})
 */

import { Router } from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import { inspectEnvStatus } from './env-check.js';
import { getDB } from './db.js';
import { acquire, SyncLockError } from './lib/sync-lock.js';
import { syncNotionOverrides } from './services/notion-sync.js';
import { evaluateItemForPublish } from './services/publish-pipeline.js';
import { fetchAllItemCodes } from './lib/rakuten-rms-proxy.js';
import { executePublish, isPublishEnabled, buildIdempotencyKey } from './services/publish-executor.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = Router();

function renderView(res, viewName, data = {}) {
  res.render(path.join(__dirname, 'views', viewName), data);
}

function getLockPath() {
  const dataDir = process.env.DATA_DIR || path.join(__dirname, '..', '..', 'data');
  return path.resolve(dataDir, 'rakuten-yahoo-sync.notion-sync.lock');
}

function audit(db, action, detail, { actor = 'http', result = 'success', errorMessage = null } = {}) {
  try {
    db.prepare(`
      INSERT INTO audit_log (actor, action, after_json, result, error_message)
      VALUES (?, ?, ?, ?, ?)
    `).run(actor, action, detail ? JSON.stringify(detail) : null, result, errorMessage);
  } catch (_) { /* best-effort */ }
}

function getPublishSummary(db) {
  let rows;
  try {
    rows = db.prepare(`
      SELECT status, COUNT(*) AS n FROM publish_idempotency GROUP BY status
    `).all();
  } catch (_) {
    return null; // table 未作成 (migration 003 未適用)
  }
  const out = { in_progress: 0, success: 0, failed: 0, not_implemented: 0, total: 0 };
  for (const r of rows) {
    if (Object.prototype.hasOwnProperty.call(out, r.status)) out[r.status] = r.n;
    out.total += r.n;
  }
  return out;
}

function getReadinessSummary(db) {
  const rows = db.prepare(`
    SELECT readiness_status, COUNT(*) AS n FROM jobs GROUP BY readiness_status
  `).all();
  const summary = { pending: 0, ok: 0, blocked: 0, total: 0 };
  for (const r of rows) {
    if (Object.prototype.hasOwnProperty.call(summary, r.readiness_status)) {
      summary[r.readiness_status] = r.n;
    }
    summary.total += r.n;
  }
  return summary;
}

function getNotionOverrideStats(db) {
  return db.prepare(`
    SELECT COUNT(*) AS total,
           SUM(CASE WHEN yahoo_title IS NOT NULL THEN 1 ELSE 0 END) AS with_title,
           SUM(CASE WHEN yahoo_price IS NOT NULL THEN 1 ELSE 0 END) AS with_price,
           SUM(CASE WHEN notion_delivery_label IS NOT NULL THEN 1 ELSE 0 END) AS with_delivery
      FROM notion_overrides
  `).get();
}

function getSyncState(db) {
  return db.prepare(`SELECT * FROM sync_state WHERE source = 'notion_overrides'`).get() || null;
}

// ───────────────── 画面 ─────────────────

router.get('/', (_req, res) => {
  const status = inspectEnvStatus();
  let syncState = null;
  let notionStats = null;
  let readiness = null;
  let publish = null;
  try {
    const db = getDB();
    syncState = getSyncState(db);
    notionStats = getNotionOverrideStats(db);
    readiness = getReadinessSummary(db);
    publish = getPublishSummary(db);
  } catch (_) {
    // DB 未初期化等は dashboard 表示自体は continue
  }
  renderView(res, 'dashboard', {
    status,
    phase: 'E-5a (publish infra)',
    syncState,
    notionStats,
    readiness,
    publish,
    publishEnabled: isPublishEnabled(),
  });
});

// ───────────────── API ─────────────────

router.get('/api/health', (_req, res) => {
  res.json(inspectEnvStatus());
});

router.get('/api/notion/sync/status', (_req, res) => {
  try {
    const db = getDB();
    res.json({
      sync_state: getSyncState(db),
      notion_overrides: getNotionOverrideStats(db),
      readiness: getReadinessSummary(db),
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

router.post('/api/notion/sync', async (req, res) => {
  const body = req.body || {};
  const mode = body.mode || 'full';
  const dryRun = !!body.dryRun;

  // Codex E-2 R1 M-2: delta mode は実装未完なので 400 reject (since/cursor 未配線)
  if (mode === 'delta') {
    return res.status(400).json({
      status: 'fail',
      error: 'mode=delta is experimental and not yet implemented (since/cursor wiring pending)',
    });
  }

  const lockPath = getLockPath();
  let release;
  try {
    try {
      release = acquire(lockPath);
    } catch (e) {
      if (e instanceof SyncLockError) {
        return res.status(409).json({ status: 'skip-locked', reason: e.reason, message: e.message });
      }
      return res.status(500).json({ status: 'fail', stage: 'lock', error: e.message });
    }
    try {
      const db = getDB();
      const result = await syncNotionOverrides({ db, mode, dryRun });
      const errorCount = result.errors.length;
      // Codex E-2 R1 M-1: 行レベル errors > 0 は partial fail として 207 で返す + audit failed
      if (errorCount > 0) {
        audit(db, 'notion_sync_partial_fail', {
          mode, dryRun, runId: result.runId,
          inserted: result.inserted, updated: result.updated,
          skipped: result.skipped, deleted: result.deleted,
          errors: errorCount,
        }, { result: 'failed', errorMessage: `partial-fail: ${errorCount} row error(s)` });
        return res.status(207).json({ status: 'partial-fail', ...result });
      }
      audit(db, 'notion_sync', {
        mode, dryRun, runId: result.runId,
        inserted: result.inserted, updated: result.updated,
        skipped: result.skipped, deleted: result.deleted,
        errors: 0,
      });
      return res.json({ status: 'ok', ...result });
    } catch (e) {
      try {
        const db = getDB();
        audit(db, 'notion_sync_fail', { mode, error: e.message }, { result: 'failed', errorMessage: e.message });
      } catch (_) { /* best-effort */ }
      return res.status(500).json({ status: 'fail', error: e.message });
    }
  } finally {
    if (release) {
      try { release(); } catch (_) {}
    }
  }
});

// ───────────────── Phase E-4: publish dry-run ─────────────────

/**
 * 単商品の Phase 0 publish-pipeline を dry-run。
 *   body: {
 *     manageNumber?: string,        // 省略時は jobs.payload_json の rakuten_manage_number
 *     productCategory?: string,     // E-4 では未解決でも OK (readiness 側で fail-closed)
 *     pathName?: string,
 *     yahooProductCategoryId?: number,
 *     aucPrefCode?: number,
 *     dryRun?: boolean              // default true (E-4 では false 不可、 E-5 で flip)
 *   }
 */
router.post('/api/publish/evaluate/:itemCode', async (req, res) => {
  try {
    const itemCode = req.params.itemCode;
    const body = req.body || {};
    const dryRun = body.dryRun !== false;
    if (!dryRun) {
      return res.status(403).json({
        status: 'fail',
        error: 'Real publish not yet implemented (E-5). Only dryRun=true is allowed.',
      });
    }
    const db = getDB();
    const job = db.prepare(`SELECT payload_json FROM jobs WHERE item_code = ?`).get(itemCode);
    let payload = {};
    if (job?.payload_json) {
      try { payload = JSON.parse(job.payload_json); } catch (_) {}
    }
    const bodyMn = typeof body.manageNumber === 'string' ? body.manageNumber.trim() : '';
    const payloadMn = typeof payload.rakuten_manage_number === 'string' ? payload.rakuten_manage_number.trim() : '';
    // Codex E-4 R1 M-2: itemCode (= itemNumber) を manageNumber に誤用すると false blocked
    // が出るので、 fallback は itemNumber → manageNumber map で解決。
    let manageNumber = bodyMn || payloadMn || null;
    if (!manageNumber) {
      try {
        const mapping = await fetchAllItemCodes();
        manageNumber = mapping[itemCode] || null;
      } catch (e) {
        return res.status(502).json({
          status: 'fail',
          error: `failed to resolve manage_number from itemNumber via warehouse proxy: ${e.message}`,
        });
      }
    }
    if (!manageNumber) {
      return res.status(400).json({
        error: 'manage_number_required',
        itemCode,
        hint: 'pass body.manageNumber explicitly, set payload.rakuten_manage_number in jobs, or ensure warehouse all-codes map includes this itemNumber',
      });
    }
    const result = await evaluateItemForPublish({
      db,
      itemCode,
      manageNumber,
      dryRun: true,
      productCategory: body.productCategory || null,
      pathName: body.pathName || null,
      yahooProductCategoryId: body.yahooProductCategoryId ?? null,
      aucPrefCode: body.aucPrefCode ?? null,
    });
    audit(db, 'publish_evaluate', {
      itemCode, manageNumber, dryRun: true, status: result.status,
      reason_count: result.reasons.length,
    });
    return res.json(result);
  } catch (e) {
    return res.status(500).json({ status: 'fail', error: e.message });
  }
});

// ───────────────── Phase E-5a: 実 publish (placeholder) ─────────────────

/**
 * 実 publish 実行。 Phase E-5a では Yahoo API 呼び出し本体は未実装、 readiness pass + idempotency 監査のみ。
 *
 * body: {
 *   manageNumber?: string,
 *   idempotencyKey?: string,    // 省略時は executor が生成 (caller 指定推奨)
 *   productCategory?, pathName?, yahooProductCategoryId?, aucPrefCode?
 * }
 */
router.post('/api/publish/execute/:itemCode', async (req, res) => {
  try {
    // 受理時 dual check #1
    if (!isPublishEnabled()) {
      return res.status(403).json({
        status: 'fail',
        error: 'RYS_PUBLISH_ENABLED=0 (kill-switch). Set env to 1 to enable real publish.',
      });
    }
    const itemCode = req.params.itemCode;
    const body = req.body || {};
    const db = getDB();

    // manageNumber resolve (E-4 と同じロジック)
    const job = db.prepare(`SELECT payload_json FROM jobs WHERE item_code = ?`).get(itemCode);
    let payload = {};
    if (job?.payload_json) { try { payload = JSON.parse(job.payload_json); } catch (_) {} }
    const bodyMn = typeof body.manageNumber === 'string' ? body.manageNumber.trim() : '';
    const payloadMn = typeof payload.rakuten_manage_number === 'string' ? payload.rakuten_manage_number.trim() : '';
    let manageNumber = bodyMn || payloadMn || null;
    if (!manageNumber) {
      try {
        const mapping = await fetchAllItemCodes();
        manageNumber = mapping[itemCode] || null;
      } catch (e) {
        return res.status(502).json({ status: 'fail', error: `warehouse proxy: ${e.message}` });
      }
    }
    if (!manageNumber) {
      return res.status(400).json({ error: 'manage_number_required', itemCode });
    }

    const result = await executePublish({
      db, itemCode, manageNumber,
      createdBy: 'http',
      idempotencyKey: body.idempotencyKey || null,
      publishOpts: {
        productCategory: body.productCategory || null,
        pathName: body.pathName || null,
        yahooProductCategoryId: body.yahooProductCategoryId ?? null,
        aucPrefCode: body.aucPrefCode ?? null,
      },
    });

    // status マッピング
    if (result.status === 'in_progress_conflict') return res.status(409).json(result);
    if (result.status === 'dedupe') return res.status(200).json(result);
    if (result.status === 'readiness_blocked') return res.status(422).json(result);
    if (result.status === 'not_implemented') return res.status(501).json(result);
    if (result.status === 'flag_off') return res.status(403).json(result);
    if (result.status === 'fail') return res.status(500).json(result);
    return res.json(result);
  } catch (e) {
    return res.status(500).json({ status: 'fail', error: e.message });
  }
});

// idempotency key を caller 側で確認できる helper
router.post('/api/publish/idempotency-key', (req, res) => {
  try {
    const { itemCode, manageNumber, scope, isoDate } = req.body || {};
    if (!itemCode) return res.status(400).json({ error: 'itemCode required' });
    const key = buildIdempotencyKey({ itemCode, manageNumber, scope, isoDate });
    return res.json({ idempotencyKey: key });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
});

export default router;
