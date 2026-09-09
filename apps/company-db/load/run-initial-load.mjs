#!/usr/bin/env node
/**
 * run-initial-load.mjs — Company DB 初期ロードの CLI (Render の Shell、または miniPC から)
 *
 * 使い方 (Render の Shell。DATA_DIR と COMPANY_DB_URL は Render の env にある):
 *   node apps/company-db/load/run-initial-load.mjs              # dry-run (全部やって巻き戻す。report だけ残す)
 *   node apps/company-db/load/run-initial-load.mjs --apply      # 本適用
 * 任意: --data-dir <dir> / --url <postgres url> / --out <report dir>
 *
 * report は <DATA_DIR>/company-db/load-<run_id>.json と .md に残す (最新へのポインタ latest.json)。
 * 終了コード: 0 = ok / 1 = 失敗 (巻き戻し済) / 2 = 引数・環境不足
 */
import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { fileURLToPath } from 'node:url';
import { buildPlanFromRender } from './sources.mjs';
import { runInitialLoad, reportToMarkdown, newLoadRunId } from './engine.mjs';
import { openPgClient, pgAdapter } from '../../../scripts/company-db/migrate.mjs';

export async function runLoadOnce({ dataDir, url, apply = false, outDir, log = console.log, host } = {}) {
  const runId = newLoadRunId();
  const plan = buildPlanFromRender({ dataDir, log: (m) => log(`[company-db load] ${m}`) });
  const client = await openPgClient(url);
  const db = pgAdapter(client);
  let report;
  try {
    report = await runInitialLoad(db, plan, { runId, dryRun: !apply, log: (m) => log(`[company-db load] ${m}`), host: host || os.hostname() });
  } catch (e) {
    report = e.report || { run_id: runId, ok: false, error: String(e.message) };
    throw Object.assign(e, { report });
  } finally {
    await client.end();
    if (report) {
      report.plan_sources = plan.sources;
      const dir = outDir || path.join(dataDir, 'company-db');
      fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(path.join(dir, `load-${runId}.json`), JSON.stringify(report, null, 2));
      fs.writeFileSync(path.join(dir, `load-${runId}.md`), reportToMarkdown(report) + '\n');
      fs.writeFileSync(path.join(dir, 'latest.json'), JSON.stringify({ run_id: runId, ok: report.ok, dry_run: report.dry_run, finished_at: report.finished_at || null, summary: report.summary || null, conflicts: (report.conflicts || []).length }, null, 2));
    }
  }
  return report;
}

const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  const args = process.argv.slice(2);
  const getArg = (f) => { const i = args.indexOf(f); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; };
  const dataDir = getArg('--data-dir') || process.env.DATA_DIR;
  const url = getArg('--url') || process.env.COMPANY_DB_URL;
  if (!dataDir) { console.error('DATA_DIR (または --data-dir) が要る'); process.exit(2); }
  if (!url) { console.error('COMPANY_DB_URL (または --url) が要る'); process.exit(2); }
  const apply = args.includes('--apply');
  runLoadOnce({ dataDir, url, apply, outDir: getArg('--out') || undefined })
    .then((r) => { console.log(reportToMarkdown(r)); process.exit(r.ok ? 0 : 1); })
    .catch((e) => { console.error(`[company-db load] FAILED: ${e.message}`); if (e.report) console.log(reportToMarkdown(e.report)); process.exit(1); });
}
