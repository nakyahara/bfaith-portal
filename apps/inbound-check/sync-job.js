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
import { runScheduledFetch, runScheduledMasterFetch } from './drive-fetch.js';
import { runNotionSweep } from './notion-sync.js';
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
  }, { timezone: 'Asia/Tokyo' });
  console.log(`[inbound-check] cron 起動 (${use} JST)`);
  return task;
}

export function stopInboundCheckCron() {
  if (task) { task.stop(); task = null; }
}

// ─── Notion 作業カード (いろは行き) の一括送信 ───
// 中原さん 2026-09-02: 都度ではなく「入庫が終わってから1日1回」。当日中のやり直し
// (確認→取消→再確認) を送信前に収束させ、取消が Notion へ漏れるケースを最小化する。
// それでも残る「送信後の取消」は runNotionSweep 自身が保険で反映する。
// 台帳 = config/jobs-registry.mjs 'inbound-check-notion-cards'
//
// env:
//   INBOUND_CHECK_NOTION_ENABLED … false/0/off/no で停止 (既定=有効)。非 Render では既定 OFF
//   INBOUND_CHECK_NOTION_CRON    … cron 式を上書き (既定 '30 17 * * *' = 毎日 17:30 JST)
//   NOTION_TOKEN / INBOUND_CHECK_NOTION_DB_ID … 送信先 (notion.js)
const NOTION_DEFAULT_CRON = '30 17 * * *';

let notionTask = null;

export function startInboundCheckNotionCron() {
  if (notionTask) return notionTask;
  const raw = String(process.env.INBOUND_CHECK_NOTION_ENABLED ?? '').trim().toLowerCase();
  if (OFF.has(raw)) {
    console.log('[inbound-check] notion cron disabled (INBOUND_CHECK_NOTION_ENABLED)');
    return null;
  }
  if (!isRender() && !FORCE_ON.has(raw)) {
    console.log('[inbound-check] notion cron skipped (非Render環境。動かすなら INBOUND_CHECK_NOTION_ENABLED=true)');
    return null;
  }
  const expr = (process.env.INBOUND_CHECK_NOTION_CRON || NOTION_DEFAULT_CRON).trim();
  if (!cron.validate(expr)) {
    console.error(`[inbound-check] notion cron 式が不正です: ${expr} (既定 ${NOTION_DEFAULT_CRON} を使います)`);
  }
  const use = cron.validate(expr) ? expr : NOTION_DEFAULT_CRON;
  notionTask = cron.schedule(use, async () => {
    try { await runNotionSweep({ actor: 'cron' }); }
    catch (e) { console.warn(`[inbound-check] notion cron: ${e.message}`); }
  }, { timezone: 'Asia/Tokyo' });
  console.log(`[inbound-check] notion cron 起動 (${use} JST)`);
  return notionTask;
}

export function stopInboundCheckNotionCron() {
  if (notionTask) { notionTask.stop(); notionTask = null; }
}
