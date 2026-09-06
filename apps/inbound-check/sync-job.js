/**
 * 入荷受付チェック — Drive からの定期取込 (Render 常駐)
 *
 * miniPC が 8:40 / 11:45 にロジザードから CSV を出して Drive へ置くので、その少し後に取りに行く。
 * 取りこぼし (miniPC 側の遅延・Drive の反映待ち) を吸収するため、日中は 30 分おきに巡回する。
 * 同じ内容なら取り込まない (duplicate_file) ので、巡回を増やしても実害はない。
 *
 * env:
 *   INBOUND_CHECK_SYNC_ENABLED   … false/0/off/no で停止 (既定=有効)。非 Render では既定 OFF
 *   INBOUND_CHECK_SYNC_CRON      … cron 式を上書き (既定 '*\/30 6-20 * * *' JST)
 *   INBOUND_CHECK_DRIVE_FOLDER_ID / INBOUND_CHECK_DRIVE_FILE … 取得先 (既定は値札CSVと同じ共有ドライブ)
 *   INBOUND_CHECK_MASTER_FILE    … 商品マスタのファイル名 (既定 shohin_master.csv)
 */
import cron from 'node-cron';
import { runScheduledFetch, runScheduledMasterFetch, runScheduledBarcodeFetch } from './drive-fetch.js';
import { sweepPrintJobs, pendingAlerts, markAlerted, alertTextFor, listPrintAgents } from './print-queue.js';
import { pingJobThrottled } from '../jobs-monitor/ping-local.js';
import { isRender } from '../../lib/is-render.js';

const OFF = new Set(['false', '0', 'off', 'no']);
const FORCE_ON = new Set(['true', '1', 'on', 'yes']);
const DEFAULT_CRON = '*/30 6-20 * * *';   // JST 6〜20時台の毎時0分・30分

let task = null;

function disabledByEnv() {
  return OFF.has(String(process.env.INBOUND_CHECK_SYNC_ENABLED ?? '').trim().toLowerCase());
}

export function startInboundCheckCron() {
  if (task) return task;   // 二重起動しない (Codex R6 Low-1)
  if (disabledByEnv()) {
    console.log('[inbound-check] cron disabled (INBOUND_CHECK_SYNC_ENABLED)');
    return null;
  }
  // 本番は Render。miniPC / ローカルでは既定 OFF (二重取込を作らない)。
  // 意図的に動かすときだけ INBOUND_CHECK_SYNC_ENABLED=true を明示する
  if (!isRender() && !FORCE_ON.has(String(process.env.INBOUND_CHECK_SYNC_ENABLED ?? '').trim().toLowerCase())) {
    console.log('[inbound-check] cron skipped (非Render環境。動かすなら INBOUND_CHECK_SYNC_ENABLED=true)');
    return null;
  }
  const expr = (process.env.INBOUND_CHECK_SYNC_CRON || DEFAULT_CRON).trim();
  if (!cron.validate(expr)) {
    console.error(`[inbound-check] cron 式が不正です: ${expr} (既定 ${DEFAULT_CRON} を使います)`);
  }
  const use = cron.validate(expr) ? expr : DEFAULT_CRON;
  // timezone を明示 (未指定だとプロセスのローカル TZ 依存になる)
  // runScheduledFetch 自身が実行中フラグを持つので重ならない。ここでは例外を握って巡回を止めない
  task = cron.schedule(use, async () => {
    try { await runScheduledFetch({ actor: 'cron' }); }
    catch (e) { console.warn(`[inbound-check] cron: ${e.message}`); }
    // 商品マスタ (期限管理あり/なし) も同じ巡回で見る。中身が変わっていなければ何もしない。
    // ⭐入口を増やさないため専用の cron は作らない (CLAUDE.md の定期実行ルール)
    try { await runScheduledMasterFetch({ actor: 'cron' }); }
    catch (e) { console.warn(`[inbound-check] cron(商品マスタ): ${e.message}`); }
    // 🚨 バーコードマスタ (値札に刷る JAN/FNSKU の正本)。これも同じ巡回に相乗りさせる (入口を増やさない)。
    // 中原さん 2026-09-06:「バーコードマスタ.csv は常に最新のバーコード情報を入れている」
    try { await runScheduledBarcodeFetch({ actor: 'cron' }); }
    catch (e) { console.warn(`[inbound-check] cron(バーコードマスタ): ${e.message}`); }
    // (2026-09-05 廃止) ここに相乗りしていた Notion カードの取消反映・再試行 (runNotionSweep mode='retry') は無くなった。
    // いろは行きの作業指示は「確認」と同じトランザクションで在庫化アプリ (f_iroha_tasks) に入り、取消も同じ経路で反映される
  }, { timezone: 'Asia/Tokyo' });
  console.log(`[inbound-check] cron 起動 (${use} JST)`);
  return task;
}

export function stopInboundCheckCron() {
  if (task) { task.stop(); task = null; }
}

// ─── (2026-09-05 廃止) Notion 作業カード (いろは行き) の 17:30 一括送信 ───
// Notion「在庫化作業管理」は運用廃止になり、いろは行きの作業指示は「確認」と同じトランザクションで
// 在庫化アプリ (f_iroha_tasks) の未着手に入る (task-intake.js)。17:30 の cron と台帳 'inbound-check-notion-cards' は
// 外した。sweep 本体 (notion-sync.js) は、在庫化アプリの /admin/source で正本を Notion に戻した退路のときだけ
// 管理画面・iPad の手動ボタンから動く (自動では動かない)。
// env INBOUND_CHECK_NOTION_ENABLED / INBOUND_CHECK_NOTION_CRON はもう読まない (残っていても無害)

// ─── 🏷 値札印刷キューの見張り (プロセス内 30 秒間隔) ───
// packing の印刷キュー (miniPC のドライブポーラーに相乗り) と同じ役目を Render 内で担う:
//   ① 進まなくなったジョブを安全な状態へ (queued 3分 → manual / 報告なし5分 → unknown)
//   ② その結果をチャットに知らせる (INBOUND_CHECK_PRINT_WEBHOOK を設定したときだけ。iPad の行には常に出る)
//   ③ 倉庫PCの印刷エージェントの生存を jobs-monitor へ中継する (台帳 id=nefuda-print-agent)
// ⭐ping を**エージェント自身に打たせない**のは、倉庫PCへ JOBS_MONITOR_TOKEN を配らずに済ませるため。
//   一度も登録されていない間は ping しない (まだ導入していないものを「止まっている」と鳴らさない)。
// 30分 cron に相乗りしないのは、3分/5分の期限を見るには粗すぎるため。台帳の対象は独立 cron ではなく
// heartbeat エントリ (nefuda-print-agent) の中継役として記載する。
//
// env:
//   INBOUND_CHECK_PRINT_ENABLED … false/0/off/no で停止 (既定=有効)。非 Render では既定 OFF
//   INBOUND_CHECK_PRINT_WEBHOOK … 結果通知先の GChat webhook (未設定なら通知なし)
export const PRINT_JOB_ID = 'nefuda-print-agent';
const PRINT_TICK_MS = 30 * 1000;
const AGENT_ALIVE_MS = 10 * 60 * 1000;   // heartbeat 45秒間隔の十数倍
let printTimer = null;
let printTicking = false;

export async function postPrintAlert(text, fetchFn = fetch) {
  const url = process.env.INBOUND_CHECK_PRINT_WEBHOOK;
  if (!url) return false;
  const res = await fetchFn(url, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ text }), signal: AbortSignal.timeout(8000),
  });
  if (!res.ok) throw new Error(`GChat webhook HTTP ${res.status}`);
  return true;
}

/** 1周期分。テストから直接呼べるよう export (fetchFn 差し替え可) */
export async function printQueueTick({ fetchFn = fetch, now = new Date().toISOString() } = {}) {
  const out = { swept: null, alerted: 0, pinged: false };
  try { out.swept = sweepPrintJobs({ now }); }
  catch (e) { console.warn(`[inbound-check print] キューの整理に失敗: ${e.message}`); }
  // 通知は**送れたときだけ**「通知済み」にする (webhook が落ちていた分が永久に鳴らなくなるのを避ける)。
  // webhook 未設定なら何もしない (iPad の行に出るのが主経路で、チャットは補助)
  if (process.env.INBOUND_CHECK_PRINT_WEBHOOK) {
    for (const job of pendingAlerts()) {
      try {
        const text = alertTextFor(job);
        if (!text) continue;
        if (await postPrintAlert(text, fetchFn)) { markAlerted(job.id, job.state); out.alerted++; }
      } catch (e) {
        console.warn(`[inbound-check print] 結果の通知に失敗 (job ${job.id}): ${e.message}`);
      }
    }
  }
  try {
    const nowMs = Date.parse(now);
    const alive = listPrintAgents({ now }).find(a => a.heartbeat_at && nowMs - Date.parse(a.heartbeat_at) <= AGENT_ALIVE_MS);
    if (alive) out.pinged = pingJobThrottled(PRINT_JOB_ID, `${alive.label} / ${alive.printer_name || '-'} / ${alive.heartbeat_note || ''}`);
  } catch (e) {
    console.warn(`[inbound-check print] 生存 ping に失敗: ${e.message}`);
  }
  return out;
}

export function startInboundCheckPrintQueueWorker() {
  if (printTimer) return printTimer;
  const raw = String(process.env.INBOUND_CHECK_PRINT_ENABLED ?? '').trim().toLowerCase();
  if (OFF.has(raw)) {
    console.log('[inbound-check] print queue worker disabled (INBOUND_CHECK_PRINT_ENABLED)');
    return null;
  }
  if (!isRender() && !FORCE_ON.has(raw)) {
    console.log('[inbound-check] print queue worker skipped (非Render環境。動かすなら INBOUND_CHECK_PRINT_ENABLED=true)');
    return null;
  }
  printTimer = setInterval(async () => {
    if (printTicking) return;
    printTicking = true;
    try { await printQueueTick(); }
    catch (e) { console.warn(`[inbound-check print] worker: ${e.message}`); }
    finally { printTicking = false; }
  }, PRINT_TICK_MS);
  printTimer.unref?.();
  console.log(`[inbound-check] print queue worker 起動 (${PRINT_TICK_MS / 1000}秒間隔)`);
  return printTimer;
}

export function stopInboundCheckPrintQueueWorker() {
  if (printTimer) { clearInterval(printTimer); printTimer = null; }
}
