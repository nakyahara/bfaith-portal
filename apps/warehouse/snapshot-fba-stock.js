/**
 * snapshot-fba-stock.js — SP-API レポート (RESTOCK + PLANNING) を取って fba.db.daily_snapshots に保存する日次 cron スクリプト。
 * daily-sync.js から朝 7:30〜8:00 頃に呼ばれる。
 *
 * 🚨 fba.db の書き手は常駐の WarehouseServer 1 つ (2026-09-20。経緯は fba-report-snapshot.js の先頭):
 *   ① **自分では fba.db を開かない**。POST /service-api/fba/snapshot-reports で常駐サーバに頼み、ジョブの終わりを待つ
 *   ② ほかの取得 (UI の手動取得・別の日付のスナップショット) が実行中 → 終わるのを待って頼み直す (同じ日付のスナップショットなら、そのジョブの終わりを待つ)。
 *      待ち切れなければ失敗 (今までは「スキップ = 成功」で、その後に失敗しても緑だった。Codex #1376 R1 #4)
 *   ③ 常駐サーバに頼めない (起動していない = 接続拒否・認証・404・5xx・応答なし・ジョブの失敗・時間切れ) → **失敗で終わる。自分では書かない**
 *      (接続を拒否されても「DB を読み込み済みで listen の前」「PORT の食い違い」「この後すぐ起動する」かもしれない = 2 プロセスが同じ fba.db を持ち得る。Codex #1376 R1 #2)
 *   ④ `--direct` = 常駐サーバを **止めてあると分かっているとき** の手動用。127.0.0.1:PORT が接続を拒否するときだけ、自分で開いて書く
 *      (それでも保存はプロセス間の lock と世代の印で守られる = 黙って上書きはしない。fba-replenishment/db.js の saveToFile)
 *
 * business_date: process.env.WAREHOUSE_BUSINESS_DATE (daily-sync が JST で確定) → --date=YYYY-MM-DD → 実行時刻の JST
 * 終了コード: JP のレポートが 1 つも取れなかった回は 1 (今までは 0 = 403 の朝も緑だった)。最後の 1 行が daily-sync の朝の通知に載る
 */
import 'dotenv/config';
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';
import { acquireFbaFetchLock, releaseFbaFetchLock } from './fba-fetch-lock.js';
import { runFbaReportSnapshot, toJstDate, isBusinessDate } from './fba-report-snapshot.js';

export const JOB_POLL_MS = 5000;
export const BUSY_RETRY_MS = 30000;
export const TOTAL_WAIT_MS = 14 * 60 * 1000;   // daily-sync のこのステップの枠は 15 分 (頼み直しの待ちも、ジョブの待ちも、この中)

export function resolveBusinessDate(env = process.env, argv = process.argv.slice(2), now = new Date()) {
  if (env.WAREHOUSE_BUSINESS_DATE) return env.WAREHOUSE_BUSINESS_DATE;
  const cliArg = argv.find((a) => a.startsWith('--date='));
  if (cliArg) return cliArg.split('=')[1];
  return toJstDate(now);
}

/** 接続を拒否された = そのポートで待っているプロセスが居ない (undici は cause に入れる。AggregateError のときは中の全部) */
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
 * 常駐サーバに頼んで、終わりを待つ。
 * @returns {{ mode: 'server', result } | { mode: 'not_running' }}  頼めない・失敗・待ち切れないは例外
 */
export async function snapshotViaServer({ businessDate, base, token, fetchImpl = fetch, sleepFn = (ms) => new Promise((r) => setTimeout(r, ms)), pollMs = JOB_POLL_MS, busyRetryMs = BUSY_RETRY_MS, waitMs = TOTAL_WAIT_MS, now = () => Date.now(), log = console.log }) {
  const headers = { 'content-type': 'application/json', 'x-service-token': token || '' };
  const started = now();
  const left = () => waitMs - (now() - started);
  const reqTimeout = () => Math.max(1, Math.min(30000, left()));   // 1 回の要求も、残り時間を超えて待たない
  const nap = (ms) => sleepFn(Math.max(0, Math.min(ms, left())));
  let jobId = null;
  while (jobId === null) {
    let res;
    try {
      res = await fetchImpl(`${base}/service-api/fba/snapshot-reports`, { method: 'POST', headers, body: JSON.stringify({ businessDate }), signal: AbortSignal.timeout(reqTimeout()) });
    } catch (e) {
      if (isConnectionRefused(e)) return { mode: 'not_running' };
      throw new Error(`常駐サーバに頼めなかった (応答なし: ${e.message})。自分では fba.db に書かない`);
    }
    const data = await res.json().catch(() => null);
    if (!res.ok) throw new Error(`常駐サーバに頼めなかった (HTTP ${res.status}${res.status === 401 || res.status === 403 ? ' = SERVICE_TOKEN を確かめる' : res.status === 404 ? ' = 常駐サーバが古い版のまま。Restart-Service WarehouseServer' : ''}${data && data.message ? `: ${String(data.message).slice(0, 120)}` : ''})。自分では fba.db に書かない`);
    if (data && data.status === 'already_running') {
      if (data.jobId && data.businessDate === businessDate) { jobId = data.jobId; log(`[fba-stock-snapshot] 同じ日付 (${businessDate}) のスナップショットが実行中 (job ${jobId})。その終わりを待つ`); break; }
      if (left() <= busyRetryMs) throw new Error(`ほかの取得が ${Math.round(waitMs / 60000)} 分待っても終わらず、頼めなかった (${JSON.stringify(data.holder || data.message || {}).slice(0, 160)})`);
      log(`[fba-stock-snapshot] ほかの取得が実行中 (${JSON.stringify(data.holder || data.businessDate || data.message || {}).slice(0, 160)})。${Math.round(busyRetryMs / 1000)} 秒待って頼み直す`);
      await nap(busyRetryMs);
      if (left() <= 0) throw new Error(`ほかの取得が ${Math.round(waitMs / 60000)} 分待っても終わらず、頼めなかった`);   // 期限の後に要求を始めない (Codex #1376 R3 Low)
      continue;
    }
    if (!data || typeof data.jobId !== 'string' || !data.jobId) throw new Error('常駐サーバの応答に jobId が無い');
    jobId = data.jobId;
    log(`[fba-stock-snapshot] 常駐サーバに頼んだ (job ${jobId})。終わりを待つ`);
  }
  let lastStep = null;
  while (true) {
    if (left() <= 0) throw new Error(`常駐サーバのジョブ ${jobId} が ${Math.round(waitMs / 60000)} 分で終わらない (常駐側では続いているかもしれない。結果は /service-api/jobs/${jobId})`);
    await nap(pollMs);
    if (left() <= 0) continue;   // 期限の後に要求を始めない → 次の周の先頭で時間切れにする
    let jr;
    try { jr = await fetchImpl(`${base}/service-api/jobs/${encodeURIComponent(jobId)}`, { headers, signal: AbortSignal.timeout(reqTimeout()) }); }
    catch (e) { log(`[fba-stock-snapshot] ジョブの確認に失敗 (続ける): ${e.message}`); continue; }
    if (jr.status === 404) throw new Error(`常駐サーバがジョブ ${jobId} を知らない (途中で再起動した?)`);
    const jb = await jr.json().catch(() => null);
    if (!jr.ok) { log(`[fba-stock-snapshot] ジョブの確認が HTTP ${jr.status} (続ける)`); continue; }
    // GET /service-api/jobs/:id の応答は { ok: true, job: { jobId, status, progress, result, error } } (service-router.js)
    const job = jb && jb.job;
    if (!job || typeof job !== 'object' || typeof job.status !== 'string') throw new Error(`ジョブの応答の形が違う (${JSON.stringify(jb).slice(0, 160)})`);
    const step = job.progress && (job.progress.step || job.progress.message);
    if (step && step !== lastStep) { lastStep = step; log(`[fba-stock-snapshot] … ${step}`); }
    if (job.status === 'completed') return { mode: 'server', result: job.result };
    if (job.status === 'failed') throw new Error(`常駐サーバのジョブが失敗: ${(job.error && (job.error.message || job.error.code)) || '(理由なし)'}`);
    if (job.status !== 'running') throw new Error(`ジョブの状態が分からない: ${job.status}`);
  }
}

/** そのポートで待っているプロセスが居るか (何か応答があれば居る。接続拒否だけが「居ない」。それ以外は分からない = 例外) */
export async function isServerListening(base, fetchImpl = fetch) {
  try { await fetchImpl(`${base}/service-api/jobs`, { signal: AbortSignal.timeout(15000) }); return true; }
  catch (e) { if (isConnectionRefused(e)) return false; throw new Error(`常駐サーバが居るか分からない (${e.message})。自分では fba.db に書かない`); }
}

/** 自分で開いて書く (--direct のときだけ) */
async function snapshotDirect(businessDate, log) {
  const lock = acquireFbaFetchLock('cron-direct');
  if (!lock.acquired) throw new Error(`レポート取得の lock が取れない (${JSON.stringify(lock.holder || {}).slice(0, 160)})`);
  try {
    const db = await import('../fba-replenishment/db.js');
    await db.initDb();
    return { mode: 'direct', result: await runFbaReportSnapshot({ db, businessDate, log }) };
  } finally {
    releaseFbaFetchLock(lock);   // lock オブジェクト全体を渡して所有権 (ownerToken) チェックを有効化
  }
}

/** @returns {{ exitCode: 0|1, lastLine: string }} */
export async function runSnapshotCli({ env = process.env, argv = process.argv.slice(2), viaServer = snapshotViaServer, direct = snapshotDirect, listening = isServerListening, log = console.log } = {}) {
  const businessDate = resolveBusinessDate(env, argv);
  if (!isBusinessDate(businessDate)) return { exitCode: 1, lastLine: `❌ FBA在庫スナップショット: business_date が不正 (${businessDate})` };
  log(`[fba-stock-snapshot] business_date=${businessDate} 開始`);
  const base = `http://127.0.0.1:${env.PORT || 3000}`;
  let out;
  if (argv.includes('--direct')) {
    if (await listening(base)) return { exitCode: 1, lastLine: `❌ FBA在庫スナップショット: --direct は常駐サーバを止めてあるときだけ (${base} で待っているプロセスが居る)。--direct を外して流す` };
    log(`[fba-stock-snapshot] --direct: ${base} は接続を拒否 = 常駐サーバは起動していない → 自分で fba.db を開いて書く`);
    out = await direct(businessDate, log);
  } else {
    out = await viaServer({ businessDate, base, token: env.SERVICE_TOKEN, log });
    if (out.mode === 'not_running') {
      return { exitCode: 1, lastLine: `❌ FBA在庫スナップショット: 常駐サーバが起動していない (${base} に接続を拒否された)。fba.db の書き手は常駐サーバだけなので自分では書かない → 常駐サーバを起動してから流し直す (止めてあると分かっているときだけ --direct)` };
    }
  }
  const r = out.result;
  if (!r || typeof r.lastLine !== 'string' || typeof r.ok !== 'boolean') return { exitCode: 1, lastLine: '❌ FBA在庫スナップショット: 結果の形が違う' };
  return { exitCode: r.ok ? 0 : 1, lastLine: `${r.lastLine} [${out.mode === 'server' ? '常駐サーバ経由' : '直接 (--direct)'}]` };
}

// 起動の判定は実体パスで (リンク経由の起動でも main が走る。#1369 と同じ型)
const fold = (x) => (process.platform === 'win32' ? x.toLowerCase() : x);
const isMain = (() => { try { return !!process.argv[1] && fold(fs.realpathSync.native(process.argv[1])) === fold(fs.realpathSync.native(fileURLToPath(import.meta.url))); } catch { return false; } })();
if (isMain) {
  let code = 1;
  try {
    const r = await runSnapshotCli();
    code = r.exitCode;
    console.log(String(r.lastLine).replace(/\s+/g, ' '));   // 最後の 1 行を複数行にしない (US のエラー文などに改行が入り得る)
  } catch (e) {
    console.log(`❌ FBA在庫スナップショット: ${String(e.message).replace(/\s+/g, ' ').slice(0, 400)}`);   // 最後の 1 行を複数行にしない
  }
  // 🚨 fetch の直後に process.exit() しない: Windows の Node では libuv の assertion (`!(handle->flags & UV_HANDLE_CLOSING)`) で異常終了し、終了コードが 127 になる
  //    (2026-09-20 に本番の dry-run と手元で再現。成功の経路で起きれば、成功した朝が ❌ に見える)。ほかの送り手 (mall-orders / ne-shipments) と同じく exitCode を置いて自然に終わらせる。
  //    何かがイベントループを持ち続けたときの保険に、10 秒後に終わらせる (unref = このタイマー自体はループを延ばさない)
  process.exitCode = code;
  setTimeout(() => process.exit(code), 10000).unref();
}
