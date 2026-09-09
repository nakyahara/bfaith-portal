/**
 * router.mjs — Company DB の同期・管理 API (x-sync-key 認証。expected-profit の publish-api と同じ流儀)
 *
 *   POST /apps/company-db/sync/load            dry-run を開始 (全部やって巻き戻す)。202 + run_id を即返す
 *   POST /apps/company-db/sync/load?apply=1    本適用を開始。202 + run_id
 *   GET  /apps/company-db/sync/status          実行中 (current) / 終わった直近 (last) / latest.json / 途中で死んだ記録 (interrupted) / Postgres の件数
 *
 * 🚨 Render 上で動かす前提 (読み込み元の SQLite が Render の DATA_DIR にある)。miniPC で叩いても mirror が無いので 409。
 * 🚨 同時に 2 本走らせない (単一飛行)。1 回 数十秒〜数分なので、HTTP は待たずに 202 を返し、結果は /status で見る
 *    (Render の HTTP は 100 秒程度で切れる。切れても処理は続くが、結果が受け取れないので 202 方式にする。Codex PR-B R1 M13)
 * 🚨 プロセスが途中で再起動しても「始めたのに結果が無い」は running.json (ディスク) から分かり、本適用が commit 済みかは
 *    ops.ingest_runs で確かめる (/status の interrupted。Codex R2 M4)
 * 🚨 認証はヘッダ x-sync-key だけ。クエリ ?sync_key= は受けない (URL はログに残る。Codex M15)
 */
import express from 'express';
import fs from 'node:fs';
import path from 'node:path';
import { runLoadOnce, readRunning, reportDir } from './load/run-initial-load.mjs';
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

/** 実行中 / 直近の 1 本の状態 (プロセス内。再起動で消えるが running.json / latest.json はディスクに残る) */
const state = { current: null, last: null };
export const getLoadState = () => ({ current: state.current, last: state.last });

/** 開始 (単一飛行のガードはここ)。戻り値 = { started, current } */
export function startLoad({ dataDir, url, apply, host = 'render', log = (m) => console.log(m) }) {
  if (state.current) return { started: false, current: state.current };
  const runId = newLoadRunId();
  const cur = { run_id: runId, dry_run: !apply, status: 'running', started_at: new Date().toISOString(), finished_at: null, summary: null, conflicts: null, unresolved: null, error: null, error_code: null };
  state.current = cur;
  const done = (patch) => { Object.assign(cur, patch, { finished_at: new Date().toISOString() }); state.last = cur; state.current = null; };
  // 202 を先に返してから始める (SQLite の読み取りも応答の後)
  setImmediate(() => {
    runLoadOnce({ dataDir, url, apply, log, host, runId })
      .then((report) => done({ status: report.ok ? 'done' : 'failed', summary: report.summary || null, conflicts: (report.conflicts || []).length, unresolved: Object.fromEntries(Object.entries(report.unresolved || {}).map(([k, v]) => [k, v.length])), sources: report.plan_sources || null }))
      .catch((e) => { log(`[company-db load] FAILED ${runId}: ${e.message}`); done({ status: 'failed', error: String(e.message), error_code: e.code || null, summary: e.report?.summary || null }); });
  });
  return { started: true, current: cur };
}

/** running.json があり、それが今の current でなければ「途中で死んだ」記録 */
function interruptedRecord(dataDir) {
  const r = readRunning(reportDir(dataDir));
  if (!r) return null;
  if (state.current && state.current.run_id === r.run_id) return null;
  return r;
}

router.post('/load', requireSyncKey, (req, res) => {
  const dataDir = process.env.DATA_DIR;
  const url = process.env.COMPANY_DB_URL;
  if (!dataDir || !url) return res.status(503).json({ error: 'DATA_DIR / COMPANY_DB_URL not configured' });
  if (!fs.existsSync(path.join(dataDir, 'warehouse-mirror.db'))) return res.status(409).json({ error: 'warehouse-mirror.db not found (run on Render)' });
  const apply = String(req.query.apply || '') === '1';
  const interrupted = interruptedRecord(dataDir);
  const r = startLoad({ dataDir, url, apply });
  if (!r.started) return res.status(409).json({ error: 'load already running', run_id: r.current.run_id, started_at: r.current.started_at });
  res.status(202).json({ accepted: true, run_id: r.current.run_id, dry_run: r.current.dry_run, started_at: r.current.started_at, status_url: '/apps/company-db/sync/status', previous_interrupted: interrupted });
});

router.get('/status', requireSyncKey, async (req, res) => {
  const dataDir = process.env.DATA_DIR;
  const url = process.env.COMPANY_DB_URL;
  const out = { current: state.current, last: state.last, latest: null, interrupted: null, counts: null };
  // latest.json と running.json は別々に読む (latest が壊れていても interrupted と commit 照会は出す。Codex R4-3)
  try {
    const latestPath = dataDir ? path.join(reportDir(dataDir), 'latest.json') : null;
    if (latestPath && fs.existsSync(latestPath)) out.latest = JSON.parse(fs.readFileSync(latestPath, 'utf-8'));
  } catch (e) { out.latest_error = e.message; }
  try {
    if (dataDir) { const r = interruptedRecord(dataDir); if (r) out.interrupted = { ...r, committed: null, note: '始めたのに結果が無い (プロセスが途中で終わった、または結果を書けなかった)。committed が true なら本適用は済んでいる (report だけ無い)' }; }
  } catch (e) { out.interrupted_error = e.message; }
  if (url && String(req.query.counts || '1') !== '0') {
    let client;
    try {
      client = await openPgClient(url);
      const db = pgAdapter(client);
      const tables = ['core.products', 'core.skus', 'core.sku_components', 'core.sku_costs', 'core.listings', 'core.listing_components', 'core.catalog_items', 'core.external_ids', 'core.product_attribute_observations', 'core.attribute_resolutions', 'core.product_physicals', 'core.product_compliance', 'core.suppliers', 'core.supplier_skus', 'core.workers'];
      out.counts = {};
      for (const t of tables) out.counts[t] = Number((await db.query(`select count(*)::bigint as n from ${t}`)).rows[0].n);
      out.migrations = (await db.query('select version from ops.schema_migrations order by version')).rows.map((r) => r.version);
      if (out.interrupted) {
        const ir = (await db.query('select status, finished_at from ops.ingest_runs where ingest_run_id = $1', [out.interrupted.run_id])).rows[0];
        out.interrupted.committed = !!ir; if (ir) out.interrupted.ingest_run = { status: ir.status, finished_at: ir.finished_at };
      }
    } catch (e) { out.counts_error = e.message; }
    finally { if (client) await client.end(); }
  }
  res.json(out);
});

export default router;
