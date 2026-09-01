/**
 * 入荷受付チェック — Drive からの定期取込 (Render 常駐)
 *
 * miniPC が 8:30 / 12:00 にロジザードから CSV を出して Drive へ置くので、その少し後に取りに行く。
 * 取りこぼし (miniPC 側の遅延・Drive の反映待ち) を吸収するため、日中は 30 分おきに巡回する。
 * 同じ内容なら取り込まない (duplicate_file) ので、巡回を増やしても実害はない。
 *
 * env:
 *   INBOUND_CHECK_SYNC_ENABLED   … false/0/off/no で停止 (既定=有効)。非 Render では既定 OFF
 *   INBOUND_CHECK_SYNC_CRON      … cron 式を上書き (既定 '*\/30 6-20 * * *' JST)
 *   INBOUND_CHECK_DRIVE_FOLDER_ID / INBOUND_CHECK_DRIVE_FILE … 取得先 (既定は値札CSVと同じ共有ドライブ)
 */
import cron from 'node-cron';
import { runScheduledFetch } from './drive-fetch.js';
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
  }, { timezone: 'Asia/Tokyo' });
  console.log(`[inbound-check] cron 起動 (${use} JST)`);
  return task;
}

export function stopInboundCheckCron() {
  if (task) { task.stop(); task = null; }
}
