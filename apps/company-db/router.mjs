/**
 * router.mjs — Company DB の同期・管理 API (x-sync-key 認証。expected-profit の publish-api と同じ流儀)
 *
 *   POST /apps/company-db/sync/load            dry-run (全部やって巻き戻す)。report を DATA_DIR/company-db に残す
 *   POST /apps/company-db/sync/load?apply=1    本適用
 *   GET  /apps/company-db/sync/status          直近の report と Postgres の件数
 *
 * 🚨 Render 上で動かす前提 (読み込み元の SQLite が Render の DATA_DIR にある)。miniPC で叩いても mirror が無いので 409。
 * 🚨 同時に 2 本走らせない (単一飛行)。1 回 数十秒〜数分。
 */
import express from 'express';
import fs from 'node:fs';
import path from 'node:path';
import { runLoadOnce } from './load/run-initial-load.mjs';
import { openPgClient, pgAdapter } from '../../scripts/company-db/migrate.mjs';

const router = express.Router();

function requireSyncKey(req, res, next) {
  const key = process.env.MIRROR_SYNC_KEY;
  if (!key) return res.status(503).json({ error: 'MIRROR_SYNC_KEY not configured' });
  const provided = req.headers['x-sync-key'] || req.query.sync_key;
  if (provided !== key) return res.status(401).json({ error: 'invalid_sync_key' });
  next();
}

let inFlight = null;

router.post('/load', requireSyncKey, async (req, res) => {
  const dataDir = process.env.DATA_DIR;
  const url = process.env.COMPANY_DB_URL;
  if (!dataDir || !url) return res.status(503).json({ error: 'DATA_DIR / COMPANY_DB_URL not configured' });
  if (!fs.existsSync(path.join(dataDir, 'warehouse-mirror.db'))) return res.status(409).json({ error: 'warehouse-mirror.db not found (run on Render)' });
  if (inFlight) return res.status(409).json({ error: 'load already running', run_id: inFlight });
  const apply = String(req.query.apply || '') === '1';
  inFlight = 'starting';
  try {
    const report = await runLoadOnce({ dataDir, url, apply, log: (m) => console.log(m), host: 'render' });
    inFlight = null;
    res.json({ ok: report.ok, run_id: report.run_id, dry_run: report.dry_run, summary: report.summary, conflicts: report.conflicts.length, unresolved: Object.fromEntries(Object.entries(report.unresolved || {}).map(([k, v]) => [k, v.length])), sources: report.plan_sources });
  } catch (e) {
    inFlight = null;
    res.status(500).json({ ok: false, error: e.message, code: e.code || null, run_id: e.report?.run_id || null, summary: e.report?.summary || null });
  }
});

router.get('/status', requireSyncKey, async (req, res) => {
  const dataDir = process.env.DATA_DIR;
  const url = process.env.COMPANY_DB_URL;
  const out = { latest: null, counts: null };
  try {
    const latestPath = dataDir ? path.join(dataDir, 'company-db', 'latest.json') : null;
    if (latestPath && fs.existsSync(latestPath)) out.latest = JSON.parse(fs.readFileSync(latestPath, 'utf-8'));
  } catch (e) { out.latest_error = e.message; }
  if (url) {
    let client;
    try {
      client = await openPgClient(url);
      const db = pgAdapter(client);
      const tables = ['core.products', 'core.skus', 'core.sku_components', 'core.sku_costs', 'core.listings', 'core.listing_components', 'core.catalog_items', 'core.external_ids', 'core.product_attribute_observations', 'core.attribute_resolutions', 'core.product_physicals', 'core.product_compliance', 'core.suppliers', 'core.supplier_skus', 'core.workers'];
      out.counts = {};
      for (const t of tables) out.counts[t] = Number((await db.query(`select count(*)::bigint as n from ${t}`)).rows[0].n);
      out.migrations = (await db.query('select version from ops.schema_migrations order by version')).rows.map((r) => r.version);
    } catch (e) { out.counts_error = e.message; }
    finally { if (client) await client.end(); }
  }
  res.json(out);
});

export default router;
