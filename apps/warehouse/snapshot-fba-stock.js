/**
 * snapshot-fba-stock.js — SP-API レポート (RESTOCK + PLANNING) を取って fba.db.daily_snapshots に保存する日次 cron スクリプト。
 * daily-sync.js から朝 7:30〜8:00 頃に呼ばれる。
 *
 * 🚨 fba.db の書き手は常駐の WarehouseServer 1 つ (2026-09-20。経緯は fba-report-snapshot.js の先頭):
 *   ① 常駐サーバが起動している → **自分では fba.db を開かず**、POST /service-api/fba/snapshot-reports で常駐サーバに頼み、ジョブの終わりを待つ
 *   ② 常駐サーバが起動していない (127.0.0.1:PORT に接続を拒否される) → 今までどおり自分で開いて書く
 *      (起動していないプロセスは古いメモリを持っていない = 次に起動したとき、このファイルを読む)
 *   ③ 常駐サーバは居るのに頼めない (認証・5xx・応答なし・ジョブの失敗) → **失敗で終わる**。自分では書かない
 *      (居るプロセスのメモリを知らないまま書くと、次の保存で消える / 常駐側が保存したものを消す)
 *
 * 排他: fba-fetch-lock.js の lockfile で手動実行 (UI からの /fetch-reports) と排他。既に走っていれば skip して通常終了 (daily-sync 全体は失敗扱いにしない)。
 * business_date: process.env.WAREHOUSE_BUSINESS_DATE (daily-sync が JST で確定) → --date=YYYY-MM-DD → 実行時刻の JST
 * 終了コード: JP のレポートが 1 つも取れなかった回は 1 (今までは 0 = 403 の朝も緑だった)。最後の 1 行が daily-sync の朝の通知に載る
 */
import 'dotenv/config';
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';
import { acquireFbaFetchLock, releaseFbaFetchLock } from './fba-fetch-lock.js';
import { runFbaReportSnapshot, toJstDate, isBusinessDate } from './fba-report-snapshot.js';

export const JOB_POLL_MS = 5000;
export const JOB_WAIT_MS = 14 * 60 * 1000;   // daily-sync のこのステップの枠は 15 分

export function resolveBusinessDate(env = process.env, argv = process.argv.slice(2), now = new Date()) {
  if (env.WAREHOUSE_BUSINESS_DATE) return env.WAREHOUSE_BUSINESS_DATE;
  const cliArg = argv.find((a) => a.startsWith('--date='));
  if (cliArg) return cliArg.split('=')[1];
  return toJstDate(now);
}

/** 接続を拒否された = そのポートで待っているプロセスが居ない (undici は cause に入れる。AggregateError のときは中の 1 つずつ) */
export function isConnectionRefused(e) {
  const seen = new Set();
  const walk = (x) => {
    if (!x || typeof x !== 'object' || seen.has(x)) return false;
    seen.add(x);
    if (x.code === 'ECONNREFUSED') return true;
    if (Array.isArray(x.errors) && x.errors.length > 0 && x.errors.every((y) => walk(y))) return true;
    return walk(x.cause);
  };
  return walk(e);
}

/**
 * 常駐サーバに頼む。
 * @returns {{ mode: 'server', result } | { mode: 'not_running' } | { mode: 'busy', holder }}  頼めない・失敗は例外
 */
export async function snapshotViaServer({ businessDate, base, token, fetchImpl = fetch, sleepFn = (ms) => new Promise((r) => setTimeout(r, ms)), pollMs = JOB_POLL_MS, waitMs = JOB_WAIT_MS, now = () => Date.now(), log = console.log }) {
  const headers = { 'content-type': 'application/json', 'x-service-token': token || '' };
  let res;
  try {
    res = await fetchImpl(`${base}/service-api/fba/snapshot-reports`, { method: 'POST', headers, body: JSON.stringify({ businessDate }), signal: AbortSignal.timeout(30000) });
  } catch (e) {
    if (isConnectionRefused(e)) return { mode: 'not_running' };
    throw new Error(`常駐サーバに頼めなかった (応答なし: ${e.message})。常駐サーバは居るかもしれないので、自分では fba.db に書かない`);
  }
  const body = await res.json().catch(() => null);
  const data = body && (body.data ?? body);
  if (!res.ok) throw new Error(`常駐サーバに頼めなかった (HTTP ${res.status}${res.status === 401 || res.status === 403 ? ' = SERVICE_TOKEN を確かめる' : res.status === 404 ? ' = 常駐サーバが古い版のまま。Restart-Service WarehouseServer' : ''})。自分では fba.db に書かない`);
  if (data && data.status === 'already_running') return { mode: 'busy', holder: data.holder || data.message || null };
  const jobId = data && data.jobId;
  if (!jobId) throw new Error('常駐サーバの応答に jobId が無い');
  log(`[fba-stock-snapshot] 常駐サーバに頼んだ (job ${jobId})。終わりを待つ`);
  const started = now();
  let lastStep = null;
  while (true) {
    if (now() - started >= waitMs) throw new Error(`常駐サーバのジョブ ${jobId} が ${Math.round(waitMs / 60000)} 分で終わらない (常駐側では続いているかもしれない。結果は /service-api/jobs/${jobId})`);
    await sleepFn(pollMs);
    let jr;
    try { jr = await fetchImpl(`${base}/service-api/jobs/${encodeURIComponent(jobId)}`, { headers, signal: AbortSignal.timeout(30000) }); }
    catch (e) { log(`[fba-stock-snapshot] ジョブの確認に失敗 (続ける): ${e.message}`); continue; }
    if (jr.status === 404) throw new Error(`常駐サーバがジョブ ${jobId} を知らない (途中で再起動した?)`);
    const jb = await jr.json().catch(() => null);
    const job = jb && (jb.data ?? jb);
    if (!jr.ok || !job) { log(`[fba-stock-snapshot] ジョブの確認が HTTP ${jr.status} (続ける)`); continue; }
    const step = job.progress && (job.progress.step || job.progress.message);
    if (step && step !== lastStep) { lastStep = step; log(`[fba-stock-snapshot] … ${step}`); }
    if (job.status === 'completed') return { mode: 'server', result: job.result };
    if (job.status === 'failed') throw new Error(`常駐サーバのジョブが失敗: ${(job.error && (job.error.message || job.error.code)) || '(理由なし)'}`);
  }
}

/** 自分で開いて書く (常駐サーバが起動していないときだけ) */
async function snapshotDirect(businessDate, log) {
  const lock = acquireFbaFetchLock('cron');
  if (!lock.acquired) return { mode: 'busy', holder: lock.holder };
  try {
    const db = await import('../fba-replenishment/db.js');
    await db.initDb();
    return { mode: 'direct', result: await runFbaReportSnapshot({ db, businessDate, log }) };
  } finally {
    releaseFbaFetchLock(lock);   // lock オブジェクト全体を渡して所有権 (ownerToken) チェックを有効化
  }
}

/** @returns {{ exitCode: 0|1, lastLine: string }} */
export async function runSnapshotCli({ env = process.env, argv = process.argv.slice(2), viaServer = snapshotViaServer, direct = snapshotDirect, log = console.log } = {}) {
  const businessDate = resolveBusinessDate(env, argv);
  if (!isBusinessDate(businessDate)) return { exitCode: 1, lastLine: `❌ FBA在庫スナップショット: business_date が不正 (${businessDate})` };
  log(`[fba-stock-snapshot] business_date=${businessDate} 開始`);
  const base = `http://127.0.0.1:${env.PORT || 3000}`;
  let out = await viaServer({ businessDate, base, token: env.SERVICE_TOKEN, log });
  if (out.mode === 'not_running') {
    log(`[fba-stock-snapshot] 常駐サーバが起動していない (${base} に接続を拒否された) → 自分で fba.db を開いて書く`);
    out = await direct(businessDate, log);
  }
  if (out.mode === 'busy') return { exitCode: 0, lastLine: `⏭️ FBA在庫スナップショット: 既に実行中のためスキップ (${typeof out.holder === 'string' ? out.holder : JSON.stringify(out.holder || {}).slice(0, 120)})` };
  const r = out.result;
  if (!r || typeof r.lastLine !== 'string') return { exitCode: 1, lastLine: '❌ FBA在庫スナップショット: 結果の形が違う' };
  return { exitCode: r.ok ? 0 : 1, lastLine: `${r.lastLine} [${out.mode === 'server' ? '常駐サーバ経由' : '直接 (常駐サーバは起動していない)'}]` };
}

// 起動の判定は実体パスで (リンク経由の起動でも main が走る。#1369 と同じ型)
const fold = (x) => (process.platform === 'win32' ? x.toLowerCase() : x);
const isMain = (() => { try { return !!process.argv[1] && fold(fs.realpathSync.native(process.argv[1])) === fold(fs.realpathSync.native(fileURLToPath(import.meta.url))); } catch { return false; } })();
if (isMain) {
  let code = 1;
  try {
    const r = await runSnapshotCli();
    code = r.exitCode;
    console.log(r.lastLine);
  } catch (e) {
    console.log(`❌ FBA在庫スナップショット: ${e.message}`);
  }
  process.stdout.write('', () => process.exit(code));   // 最後の行を書き終わってから終わる
}
