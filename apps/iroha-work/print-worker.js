/**
 * 🏷 保管箱ラベル印刷キューの見張り (Render 内・プロセス内 30 秒間隔)
 *
 * inbound-check の値札印刷 (sync-job.js startInboundCheckPrintQueueWorker) と同じ役目:
 *   ① 進まなくなったジョブを安全な状態へ (queued 3分 → manual / 報告なし 5分 → unknown)
 *   ② その結果を職員のチャットに知らせる (GCHAT_WEBHOOK_IROHA — 資材不足の連絡と同じ宛先。未設定なら通知なし。iPad の詳細には常に出る)
 *   ③ いろはPC の印刷エージェントの生存を jobs-monitor へ中継する (台帳 id=iroha-label-print-agent)
 * ⭐ping を**エージェント自身に打たせない**のは、いろはPC へ JOBS_MONITOR_TOKEN を配らずに済ませるため。
 *   一度も登録されていない間は ping しない (まだ導入していないものを「止まっている」と鳴らさない)。
 *
 * env:
 *   IROHA_WORK_PRINT_ENABLED … false/0/off/no で停止 (既定=有効)。非 Render では既定 OFF (true で有効化)
 */
import { sweepPrintJobs, pendingAlerts, markAlerted, alertTextFor, listPrintAgents } from './print-queue.js';
import { notifyStaff, WEBHOOK_ENV } from './notify.js';
import { pingJobThrottled } from '../jobs-monitor/ping-local.js';
import { isRender } from '../../lib/is-render.js';

export const PRINT_JOB_ID = 'iroha-label-print-agent';
const PRINT_TICK_MS = 30 * 1000;
const AGENT_ALIVE_MS = 10 * 60 * 1000;   // heartbeat 45 秒間隔の十数倍
const OFF = new Set(['false', '0', 'off', 'no']);
const FORCE_ON = new Set(['true', '1', 'on', 'yes']);
let printTimer = null;
let printTicking = false;

/**
 * 1 周期分。テストから直接呼べるよう export (notify / ping 差し替え可)。
 * 通知は**送れたときだけ**「通知済み」にする (webhook が落ちていた分が永久に鳴らなくなるのを避ける)。
 * webhook 未設定なら何もしない (iPad の詳細に出るのが主経路で、チャットは補助)
 */
export async function printQueueTick({ notify = notifyStaff, ping = pingJobThrottled, now = new Date().toISOString() } = {}) {
  const out = { swept: null, alerted: 0, pinged: false };
  try { out.swept = sweepPrintJobs({ now }); }
  catch (e) { console.warn(`[iroha-work print] キューの整理に失敗: ${e.message}`); }
  if (process.env[WEBHOOK_ENV]) {
    for (const job of pendingAlerts()) {
      try {
        const text = alertTextFor(job);
        if (!text) continue;
        const r = await notify(text);
        if (r && r.sent) { markAlerted(job.id, job.state); out.alerted++; }
      } catch (e) {
        console.warn(`[iroha-work print] 結果の通知に失敗 (job ${job.id}): ${e.message}`);
      }
    }
  }
  try {
    const nowMs = Date.parse(now);
    const alive = listPrintAgents({ now }).find(a => a.heartbeat_at && nowMs - Date.parse(a.heartbeat_at) <= AGENT_ALIVE_MS);
    if (alive) out.pinged = !!ping(PRINT_JOB_ID, `${alive.label} / ${alive.printer_name || '-'} / ${alive.heartbeat_note || ''}`);
  } catch (e) {
    console.warn(`[iroha-work print] 生存 ping に失敗: ${e.message}`);
  }
  return out;
}

export function startIrohaPrintQueueWorker() {
  if (printTimer) return printTimer;
  const raw = String(process.env.IROHA_WORK_PRINT_ENABLED ?? '').trim().toLowerCase();
  if (OFF.has(raw)) {
    console.log('[iroha-work] print queue worker disabled (IROHA_WORK_PRINT_ENABLED)');
    return null;
  }
  if (!isRender() && !FORCE_ON.has(raw)) {
    console.log('[iroha-work] print queue worker skipped (非Render環境。動かすなら IROHA_WORK_PRINT_ENABLED=true)');
    return null;
  }
  printTimer = setInterval(async () => {
    if (printTicking) return;
    printTicking = true;
    try { await printQueueTick(); }
    catch (e) { console.warn(`[iroha-work print] worker: ${e.message}`); }
    finally { printTicking = false; }
  }, PRINT_TICK_MS);
  printTimer.unref?.();
  console.log(`[iroha-work] print queue worker 起動 (${PRINT_TICK_MS / 1000}秒間隔)`);
  return printTimer;
}

export function stopIrohaPrintQueueWorker() {
  if (printTimer) { clearInterval(printTimer); printTimer = null; }
}
