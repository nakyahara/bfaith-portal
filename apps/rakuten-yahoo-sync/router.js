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
  try {
    const db = getDB();
    syncState = getSyncState(db);
    notionStats = getNotionOverrideStats(db);
    readiness = getReadinessSummary(db);
  } catch (_) {
    // DB 未初期化等は dashboard 表示自体は continue
  }
  renderView(res, 'dashboard', {
    status,
    phase: 'E-2 (Notion sync)',
    syncState,
    notionStats,
    readiness,
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

export default router;
