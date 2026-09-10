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
 * 開始したことは running.json に永続化する (プロセスが途中で死んでも「始めたのに結果が無い」が分かる。
 * 本適用の commit は ops.ingest_runs にも 1 行残るので、結果不明なら ops.ingest_runs で「適用済みか」を確かめられる)。
 * plan を作る段階 (SQLite が無い等) や接続で落ちても latest.json に失敗を記録する (何も残らないと「動いたか分からない」)。
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

export const reportDir = (dataDir, outDir) => outDir || path.join(dataDir, 'company-db');
export const runningPath = (dir) => path.join(dir, 'running.json');

function writeReport(dir, runId, report) {
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, `load-${runId}.json`), JSON.stringify(report, null, 2));
  fs.writeFileSync(path.join(dir, `load-${runId}.md`), reportToMarkdown(report) + '\n');
  fs.writeFileSync(path.join(dir, 'latest.json'), JSON.stringify({
    run_id: runId, ok: report.ok, dry_run: report.dry_run ?? null, started_at: report.started_at || null, finished_at: report.finished_at || null,
    summary: report.summary || null, conflicts: (report.conflicts || []).length, error: report.error || null, error_code: report.error_code || null,
  }, null, 2));
}
/** 開始の記録 (running.json)。終わったら消す。残っていれば「始めたのに終わっていない」。strict なら「無い」以外の失敗 (壊れた JSON・読めない) は投げる (/status 用) */
export function readRunning(dir, { strict = false } = {}) {
  let text;
  try { text = fs.readFileSync(runningPath(dir), 'utf-8'); } catch (e) { if (e.code === 'ENOENT' || !strict) return null; throw e; }
  try { return JSON.parse(text); } catch (e) { if (!strict) return null; throw Object.assign(new Error(`running.json が壊れている: ${e.message}`), { code: 'RUNNING_CORRUPT' }); }
}
function markRunning(dir, info) { fs.mkdirSync(dir, { recursive: true }); fs.writeFileSync(runningPath(dir), JSON.stringify(info, null, 2)); }
function clearRunning(dir, runId) {
  const cur = readRunning(dir);
  if (cur && cur.run_id === runId) { try { fs.unlinkSync(runningPath(dir)); } catch { /* 無ければよい */ } }
}

export async function runLoadOnce({ dataDir, url, apply = false, outDir, log = console.log, host, runId: runIdIn } = {}) {
  const runId = runIdIn || newLoadRunId();
  const dir = reportDir(dataDir, outDir);
  const l = (m) => log(`[company-db load] ${m}`);
  const startedAt = new Date().toISOString();
  // 終了記録 (report / latest.json) を書けたときだけ running.json を消す。書けなければ残す (= 「始めたのに結果が無い」として /status の interrupted に出る。Codex R3-5)
  const finish = (report) => {
    let written = false;
    try { writeReport(dir, runId, report); written = true; } catch (e2) { l(`report を書けない (running.json は残す): ${e2.message}`); }
    if (written) clearRunning(dir, runId);
  };
  const fail = (stage, e, extra = {}) => {
    const report = { run_id: runId, dry_run: !apply, ok: false, started_at: startedAt, finished_at: new Date().toISOString(), error: `${stage}: ${e.message}`, error_code: e.code || `${stage.toUpperCase()}_FAILED`, sections: {}, conflicts: [], unresolved: {}, ...extra };
    finish(report);
    return Object.assign(e, { report });
  };
  // 開始記録を書けなければ始めない (記録の無い実行を作らない)
  try { markRunning(dir, { run_id: runId, dry_run: !apply, started_at: startedAt, host: host || os.hostname(), pid: process.pid }); }
  catch (e) { throw Object.assign(new Error(`running.json を書けないので始めない: ${e.message}`), { code: 'RUNNING_MARK_FAILED', report: { run_id: runId, dry_run: !apply, ok: false, started_at: startedAt, finished_at: new Date().toISOString(), error: `running: ${e.message}`, error_code: 'RUNNING_MARK_FAILED', sections: {}, conflicts: [], unresolved: {} } }); }
  let plan;
  try { plan = buildPlanFromRender({ dataDir, log: l }); } catch (e) { throw fail('plan', e); }
  let client;
  try { client = await openPgClient(url); } catch (e) { throw fail('connect', e, { plan_sources: plan.sources }); }
  const db = pgAdapter(client);
  let report;
  try {
    report = await runInitialLoad(db, plan, { runId, dryRun: !apply, log: l, host: host || os.hostname() });
  } catch (e) {
    report = e.report || { run_id: runId, dry_run: !apply, ok: false, started_at: startedAt, error: String(e.message), sections: {}, conflicts: [], unresolved: {} };
    throw Object.assign(e, { report });
  } finally {
    await client.end();
    if (report) { report.plan_sources = plan.sources; finish(report); }
  }
  return report;
}

/**
 * 走っている実行があるか調べる (**プロセスをまたぐ** 単一飛行)。
 * 🚨 Web プロセスの夜間 cron と、Shell から直接叩いた CLI は別プロセスなので、
 *    メモリ上の見張り (router.mjs の state.current) を共有しない。running.json の pid で見る。
 * @returns {{ blocked: boolean, reason: string } | null} null = 走っているものは無い
 */
export function runningElsewhere(dir, { alive = defaultAlive } = {}) {
  const r = readRunning(dir);
  if (!r) return null;
  const here = os.hostname();
  const fromServer = r.host === 'render' || r.host === 'render-nightly';   // Web プロセスが付ける名前
  if (r.host && r.host !== here && !fromServer) {
    return { blocked: true, reason: `別のホスト (${r.host}) で ${r.run_id} が走っている記録がある (${r.started_at})。ここからは生死を確かめられない` };
  }
  if (r.pid && alive(r.pid)) {
    return { blocked: true, reason: `${r.run_id} が走っている (pid ${r.pid}、${r.started_at} から)` };
  }
  return { blocked: false, reason: `前回の実行 ${r.run_id} (${r.started_at}) が終わっていない記録がある。本適用だったなら ops.ingest_runs に ${r.run_id} があるか確かめる` };
}

/** pid が生きているか (EPERM = 他人のプロセスだが存在する) */
function defaultAlive(pid) {
  try { process.kill(pid, 0); return true; } catch (e) { return e.code === 'EPERM'; }
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
  // 🚨 二重に流さない。Render では夜間の cron (apps/company-db/nightly.mjs) が同じことをする。
  //    走っている最中にここから叩くと、同じ材料を 2 本が別々に入れて running.json / latest.json を取り合う
  const busy = runningElsewhere(reportDir(dataDir, getArg('--out') || undefined));
  if (busy && busy.blocked && !args.includes('--force')) {
    console.error(`[company-db load] 始めない: ${busy.reason}`);
    console.error('  待つか、終わったのが確かなら --force。手で流すなら HTTP の口 (scripts/company-db/remote-load.mjs) を使う');
    process.exit(3);
  }
  if (busy) console.error(`[company-db load] ${busy.reason}`);
  runLoadOnce({ dataDir, url, apply, outDir: getArg('--out') || undefined })
    .then((r) => { console.log(reportToMarkdown(r)); process.exit(r.ok ? 0 : 1); })
    .catch((e) => { console.error(`[company-db load] FAILED: ${e.message}`); if (e.report) console.log(reportToMarkdown(e.report)); process.exit(1); });
}
