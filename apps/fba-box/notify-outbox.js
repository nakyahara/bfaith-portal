/**
 * FBA箱詰め記録 — 完了通知の送信待ち (outbox) を送る。Codex PR #1307 R1 P1
 *
 * 🚨 通知は「応答のあとに投げっぱなし」にしない。完了 (finishRun) と同じトランザクションで fbx_notify_outbox に積み、
 *    ここで送る。完了直後にデプロイ・再起動が入っても、次の起動で送り直す (取りこぼさない)。
 *    送れなかった (Chat の不調) ときは間隔を空けて再試行 (1→2→4→8→16→30 分、最大 8 回)。
 *    webhook 未設定は skipped (あとで設定しても昔の回は送らない — 古い知らせを急に流さない)。
 * 🚨 少なくとも 1 回 (at-least-once): 送った直後・記録する前に落ちると、5 分後に同じ知らせをもう一度送る。
 *    知らせが届かないより、2 回届く方がまし (本社は一覧を開けば同じものが見られる)
 *
 * 定期ジョブではない: 起動直後に 1 回・完了のたびに・再試行待ちがあるときだけその時刻にタイマーで動く
 * (いろはの写真キュー・picking の画像キューと同じ扱い。台帳対象の独立 cron ではない)
 */
import crypto from 'crypto';
import { listDueNotifies, claimNotify, settleNotify, nextNotifyDueAt, safeLogEvent } from './db.js';
import { buildRunReport, runDoneText } from './report.js';
import { notifyHq } from './notify.js';

const BASE = '/apps/fba-box';
export const MAX_ATTEMPTS = 8;
const backoffMs = (attempts) => Math.min(30 * 60 * 1000, 60 * 1000 * 2 ** attempts);

/**
 * 通知に載せるリンクの起点。🚨 リクエストの Host ヘッダーからは作らない (Codex PR #1307 R1 P1:
 * *.onrender.com を許すと Host: attacker.onrender.com で Chat のリンクを差し替えられる)。
 * PUBLIC_BASE_URL (設定があれば) → 本番のアドレス
 */
export function publicOrigin() {
  return String(process.env.PUBLIC_BASE_URL || 'https://bfaith-portal.onrender.com').replace(/\/+$/, '');
}
export const reportLink = (runId) => `${publicOrigin()}${BASE}/admin/runs/${runId}/report`;

let running = null;
let timer = null;
let buildReport = buildRunReport;
/** テスト用: まとめを作る関数を差し替える (一時的な失敗を作る)。null で元に戻す */
export function _setReportBuilderForTest(fn) { buildReport = fn || buildRunReport; }

/** 送信待ちを送る。同時に呼ばれても 1 本だけ走らせる (二重送信しない)。throw しない */
export function drainNotifyOutbox() {
  if (running) return running;
  running = (async () => {
    try { await drainOnce(); }
    catch (e) { console.error('[fba-box] 完了通知の送信処理でエラー', e); }
    finally { running = null; scheduleNext(); }
  })();
  return running;
}

async function drainOnce() {
  for (const job of listDueNotifies(new Date().toISOString())) {
    const token = crypto.randomBytes(8).toString('hex');
    if (!claimNotify(job.id, token)) continue;   // 別の処理が持っている / もう済んだ
    await sendOne(job, token);
  }
}

async function sendOne(job, token) {
  const log = (ok, error, status) => safeLogEvent({ runId: job.run_id, action: 'notify_run_done', targetType: 'run', targetId: job.run_id,
    deviceLabel: 'notify-outbox', ok, error, payload: { outboxId: job.id, status, attempts: job.attempts + 1 } });
  /** 送れなかった: 回数が残っていれば間隔を空けて再試行、使い切ったら打ち切り */
  const retryOrFail = (reason) => {
    if (job.attempts + 1 >= MAX_ATTEMPTS) { settleNotify(job.id, token, { status: 'failed', error: reason }); log(false, reason, 'failed'); return; }
    settleNotify(job.id, token, { status: 'pending', error: reason, nextTryAt: new Date(Date.now() + backoffMs(job.attempts)).toISOString() });
    log(false, reason, 'retry');
  };
  let text;
  try {
    const rep = buildReport(job.run_id);
    // 回が無い = 何度やっても送れない。ここだけ打ち切る
    if (!rep) { settleNotify(job.id, token, { status: 'failed', error: 'run_not_found' }); log(false, 'run_not_found', 'failed'); return; }
    // 時刻は「終えたとき」(再起動のあとに送っても、終えた時刻を出す)
    text = runDoneText(rep, { link: reportLink(job.run_id), doneBy: job.done_by, at: new Date(job.created_at) });
  } catch (e) {
    // 🚨 まとめを作れないのは一時的なことが多い (SQLite の busy・I/O)。即打ち切ると積んだ知らせが二度と出ない
    //    → 送信の失敗と同じく再試行する (Codex PR #1307 R2 P1)
    retryOrFail(`build: ${e.message}`);
    return;
  }
  const n = await notifyHq(text);
  if (n.sent) { settleNotify(job.id, token, { status: 'sent' }); log(true, null, 'sent'); return; }
  if (n.reason === 'no_webhook') { settleNotify(job.id, token, { status: 'skipped', error: 'no_webhook' }); log(false, 'no_webhook', 'skipped'); return; }
  retryOrFail(n.reason);
}

/** 再試行待ちがあれば、いちばん早い時刻にもう一度 (無ければ何もしない) */
function scheduleNext() {
  if (timer) { clearTimeout(timer); timer = null; }
  let due;
  try { due = nextNotifyDueAt(); } catch { return; }
  if (!due) return;
  const ms = Math.min(Math.max(1000, Date.parse(due) - Date.now()), 2 ** 31 - 1);
  timer = setTimeout(() => { timer = null; drainNotifyOutbox(); }, ms);
  timer.unref?.();
}

/** 起動時: 前のプロセスが送れずに残した分を送る (完了直後のデプロイ・再起動) */
export function startNotifyOutbox({ delayMs = 10 * 1000 } = {}) {
  const t = setTimeout(() => { drainNotifyOutbox(); }, delayMs);
  t.unref?.();
}

/** テスト用: 再試行のタイマーを止める */
export function _stopNotifyOutboxForTest() { if (timer) { clearTimeout(timer); timer = null; } }
