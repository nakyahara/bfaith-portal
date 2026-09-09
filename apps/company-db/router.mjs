/**
 * router.mjs — Company DB の同期・管理 API (x-sync-key 認証。expected-profit の publish-api と同じ流儀)
 *
 *   POST /apps/company-db/sync/load            dry-run を開始 (全部やって巻き戻す)。202 + run_id を即返す
 *   POST /apps/company-db/sync/load?apply=1    本適用を開始。202 + run_id
 *   GET  /apps/company-db/sync/status          実行中の状態 (current) + 直近の report (latest.json) + Postgres の件数
 *
 * 🚨 Render 上で動かす前提 (読み込み元の SQLite が Render の DATA_DIR にある)。miniPC で叩いても mirror が無いので 409。
 * 🚨 同時に 2 本走らせない (単一飛行)。1 回 数十秒〜数分なので、HTTP は待たずに 202 を返し、結果は /status で見る
 *    (Render の HTTP は 100 秒程度で切れる。切れても処理は続くが、結果が受け取れないので 202 方式にする。Codex PR-B R1 M13)
 * 🚨 認証はヘッダ x-sync-key だけ。クエリ ?sync_key= は受けない (URL はログに残る。Codex M15)
 */
import express from 'express';
import fs from 'node:fs';
import path from 'node:path';
import { runLoadOnce } from './load/run-initial-load.mjs';
import { newLoadRunId } from './load/engine.mjs';
import { openPgClient, pgAdapter } from '../../scripts/company-db/migrate.mjs';

const router = express.Router();

function requireSyncKey(req, res, next) {
  const key = process.env.MIRROR_SYNC_KEY;
  if (!key) return res.status(503).json({ error: 'MIRROR_SYNC_KEY not configured' });
  const provided = req.headers['x-sync-key'];
  if (typeof provided !== 'string' || provided !== key) return res.status(401).json({ error: 'invalid_sync_key' });
  next();
}

/** 実行中 / 直近の 1 本の状態 (プロセス内。再起動で消えるが latest.json はディスクに残る) */
export const state = { current: null, last: null };

export function startLoad({ dataDir, url, apply, host = 'render', log = (m) => console.log(m) }) {
  const runId = newLoadRunId();
  const cur = { run_id: runId, dry_run: !apply, status: 'running', started_at: new Date().toISOString(), finished_at: null, summary: null, conflicts: null, unresolved: null, error: null, error_code: null };
  state.current = cur;
  const done = (patch) => { Object.assign(cur, patch, { finished_at: new Date().toISOString() }); state.last = cur; state.current = null; };
  runLoadOnce({ dataDir, url, apply, log, host, runId })
    .then((report) => done({ status: report.ok ? 'done' : 'failed', summary: report.summary || null, conflicts: (report.conflicts || []).length, unresolved: Object.fromEntries(Object.entries(report.unresolved || {}).map(([k, v]) => [k, v.length])), sources: report.plan_sources || null }))
    .catch((e) => { log(`[company-db load] FAILED ${runId}: ${e.message}`); done({ status: 'failed', error: String(e.message), error_code: e.code || null, summary: e.report?.summary || null }); });
  return cur;
}

router.post('/load', requireSyncKey, (req, res) => {
  const dataDir = process.env.DATA_DIR;
  const url = process.env.COMPANY_DB_URL;
  if (!dataDir || !url) return res.status(503).json({ error: 'DATA_DIR / COMPANY_DB_URL not configured' });
  if (!fs.existsSync(path.join(dataDir, 'warehouse-mirror.db'))) return res.status(409).json({ error: 'warehouse-mirror.db not found (run on Render)' });
  if (state.current) return res.status(409).json({ error: 'load already running', run_id: state.current.run_id, started_at: state.current.started_at });
  const apply = String(req.query.apply || '') === '1';
  const cur = startLoad({ dataDir, url, apply });
  res.status(202).json({ accepted: true, run_id: cur.run_id, dry_run: cur.dry_run, started_at: cur.started_at, status_url: '/apps/company-db/sync/status' });
});

router.get('/status', requireSyncKey, async (req, res) => {
  const dataDir = process.env.DATA_DIR;
  const url = process.env.COMPANY_DB_URL;
  const out = { current: state.current, last: state.last, latest: null, counts: null };
  try {
    const latestPath = dataDir ? path.join(dataDir, 'company-db', 'latest.json') : null;
    if (latestPath && fs.existsSync(latestPath)) out.latest = JSON.parse(fs.readFileSync(latestPath, 'utf-8'));
  } catch (e) { out.latest_error = e.message; }
  if (url && String(req.query.counts || '1') !== '0') {
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
