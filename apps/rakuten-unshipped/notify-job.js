/**
 * 楽天 未発送アラート — 毎朝 08:00 JST に GChat へ通知する
 *
 * 何を見ているか (詳細は service.js の冒頭コメント):
 *   前日12:00 (出荷の締め) までに入金確認 (受注確定) できていたのに、
 *   今朝まだ発送されていない楽天の注文。
 *
 * 環境変数:
 *   RAKUTEN_UNSHIPPED_ENABLED=1        … 起動する (既定OFF = Dark Launch)
 *   GCHAT_WEBHOOK_SHIPPING             … 通知先スペースの webhook (未設定なら送らずログのみ)
 *   RAKUTEN_UNSHIPPED_CRON             … cron 式の上書き (既定 '0 8 * * *' JST)
 *   RAKUTEN_UNSHIPPED_CUTOFF_HOUR      … 締め時刻 (既定 12)
 *   RAKUTEN_UNSHIPPED_SEARCH_DAYS      … 遡る日数 (既定 180。60日窓に分割して検索)
 *   RAKUTEN_UNSHIPPED_LEAD_DAYS        … お届け日指定を保留にできる猶予日数 (既定 2。
 *                                        お届け日が今日+この日数以内なら出荷漏れ扱い)
 *   RAKUTEN_SERVICE_SECRET / RAKUTEN_LICENSE_KEY … RMS API 認証 (rakuten-client.js)
 *
 * 手動実行:
 *   node apps/rakuten-unshipped/notify-job.js --once       … 1回だけ実行して GChat へ送る
 *   node apps/rakuten-unshipped/notify-job.js --dry-run    … 送らずに本文を標準出力へ
 *
 * 0件の日も通知する。「通知が来ない = ジョブが止まっている」と読めるようにするため。
 */
import cron from 'node-cron';
import { fileURLToPath } from 'node:url';
import {
  findUnshippedOrders,
  buildMessage,
  DEFAULT_CUTOFF_HOUR,
  DEFAULT_SEARCH_DAYS,
  DEFAULT_DELIVERY_LEAD_DAYS,
} from './service.js';
import { sendGChatMessage } from '../profit-analysis/gchat-client.js';
import { pingJob } from '../jobs-monitor/ping-local.js';
import { isRender } from '../../lib/is-render.js';

const JOB_ID = 'rakuten-unshipped-alert';
const DEFAULT_CRON = '0 8 * * *';
const ON = new Set(['true', '1', 'on', 'yes']);

function isEnabled() {
  return ON.has(String(process.env.RAKUTEN_UNSHIPPED_ENABLED ?? '').trim().toLowerCase());
}

/** env の数値を読む (不正値は既定にフォールバックして警告) */
function numEnv(name, fallback, { min, max }) {
  const raw = String(process.env[name] ?? '').trim();
  if (!raw) return fallback;
  const n = Number(raw);
  if (!Number.isFinite(n) || n < min || n > max) {
    console.warn(`[rakuten-unshipped] ${name}=${raw} は不正なため既定値 ${fallback} を使います`);
    return fallback;
  }
  return n;
}

/** GChat 送信。一時障害に備えて1回だけリトライする (Codexレビュー#3) */
async function sendWithRetry(webhook, text) {
  try {
    await sendGChatMessage(webhook, text);
  } catch (e) {
    console.warn(`[rakuten-unshipped] GChat送信に失敗、10秒後に再試行: ${e.message}`);
    await new Promise(r => setTimeout(r, 10_000));
    await sendGChatMessage(webhook, text);
  }
}

/**
 * 検知 → 通知。cron からも CLI からも呼ぶ。
 * @param {{dryRun?:boolean, now?:Date}} opts
 * @returns {Promise<{ok:boolean, partial:boolean, note:string, text:string, alerts:number, holds:number}>}
 */
export async function runUnshippedAlert(opts = {}) {
  const cutoffHour = numEnv('RAKUTEN_UNSHIPPED_CUTOFF_HOUR', DEFAULT_CUTOFF_HOUR, { min: 0, max: 23 });
  const searchDays = numEnv('RAKUTEN_UNSHIPPED_SEARCH_DAYS', DEFAULT_SEARCH_DAYS, { min: 1, max: 365 });
  const leadDays = numEnv('RAKUTEN_UNSHIPPED_LEAD_DAYS', DEFAULT_DELIVERY_LEAD_DAYS, { min: 0, max: 30 });

  const result = await findUnshippedOrders({
    now: opts.now,
    cutoffHour,
    searchDays,
    leadDays,
  });
  const { alerts, holds, scanned, truncated, badDatetimes, missingDetails } = result;
  const text = buildMessage(result);
  // 検索打ち切り・日時パース不能・明細欠落があった日は partial (結果が不完全)
  const partial = Boolean(truncated) || badDatetimes > 0 || missingDetails > 0;
  const note = `未発送${alerts.length}件 / 保留${holds.length}件 / 走査${scanned}件`
    + (partial ? ` / ⚠不完全(打切=${truncated ? 1 : 0} 日時不正=${badDatetimes} 明細欠落=${missingDetails})` : '');

  if (opts.dryRun) {
    console.log(`[rakuten-unshipped] dry-run (${note})\n${text}`);
    return { ok: true, partial, note: `${note} (dry-run)`, text, alerts: alerts.length, holds: holds.length };
  }

  const webhook = process.env.GCHAT_WEBHOOK_SHIPPING;
  if (!webhook) {
    // 送り先が無いのは設定漏れ。検知自体は成功しているが ok にはしない (静かに無通知にしない)
    console.warn(`[rakuten-unshipped] GCHAT_WEBHOOK_SHIPPING 未設定のため通知できません (${note})`);
    return { ok: false, partial, note: `${note} / webhook未設定`, text, alerts: alerts.length, holds: holds.length };
  }
  await sendWithRetry(webhook, text);
  console.log(`[rakuten-unshipped] 通知しました (${note})`);
  return { ok: true, partial, note, text, alerts: alerts.length, holds: holds.length };
}

/** cron から呼ぶ入口。dead-man 監視へ結果を残す */
async function runWithPing() {
  try {
    const r = await runUnshippedAlert();
    pingJob(JOB_ID, r.ok ? (r.partial ? 'partial' : 'ok') : 'fail', r.note);
  } catch (e) {
    console.error('[rakuten-unshipped] 失敗:', e.message);
    pingJob(JOB_ID, 'fail', e.message);
    // 落ちたこと自体も通知しておく (朝の通知が「来ない」より「失敗した」と分かる方が早い)
    const webhook = process.env.GCHAT_WEBHOOK_SHIPPING;
    if (webhook) {
      await sendGChatMessage(webhook, `⚠️ *楽天 未発送アラート* の取得に失敗しました\n${e.message}`)
        .catch(err => console.error('[rakuten-unshipped] 失敗通知も送れませんでした:', err.message));
    }
  }
}

let task = null;

/** server.js の起動時に呼ぶ。⚠ Render は 1 インスタンス運用前提 (他の node-cron 同様) */
export function startRakutenUnshippedCron() {
  // 同一プロセスからの二重呼び出しガード (Codexレビュー#4)
  if (task) {
    console.warn('[rakuten-unshipped] cron は既に起動済みです (二重登録をスキップ)');
    return { started: true, reason: 'already_started' };
  }
  if (!isEnabled()) {
    console.log('[rakuten-unshipped] cron disabled (RAKUTEN_UNSHIPPED_ENABLED)');
    return { started: false, reason: 'disabled_by_env' };
  }
  // ⚠️miniPC も同じ server.js を動かすため、無条件で張ると同じ通知が2回飛ぶ
  if (!isRender()) {
    console.log('[rakuten-unshipped] cron skipped (非Render環境)');
    return { started: false, reason: 'not_render' };
  }
  const expr = (process.env.RAKUTEN_UNSHIPPED_CRON || '').trim() || DEFAULT_CRON;
  if (!cron.validate(expr)) {
    console.error(`[rakuten-unshipped] invalid cron expr: ${expr} — 起動しません`);
    return { started: false, reason: 'invalid_expr' };
  }
  try {
    task = cron.schedule(expr, runWithPing, { timezone: 'Asia/Tokyo' });
  } catch (e) {
    console.error(`[rakuten-unshipped] cron の登録に失敗 (${expr}): ${e.message}`);
    return { started: false, reason: 'schedule_failed' };
  }
  console.log(`[rakuten-unshipped] cron started (${expr} JST)`);
  return { started: true, expr };
}

/** テスト用: 登録した cron を止める */
export function stopRakutenUnshippedCron() {
  if (!task) return false;
  try {
    if (typeof task.destroy === 'function') task.destroy();
    else task.stop();
  } catch (e) {
    console.error('[rakuten-unshipped] cron の停止に失敗:', e.message);
  }
  task = null;
  return true;
}

// ─── CLI ───
const isMain = process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1];
if (isMain) {
  const dryRun = process.argv.includes('--dry-run');
  if (!dryRun && !process.argv.includes('--once')) {
    console.log('使い方: node apps/rakuten-unshipped/notify-job.js --once | --dry-run');
    process.exit(1);
  }
  runUnshippedAlert({ dryRun })
    .then(r => {
      console.log(`[rakuten-unshipped] 完了: ${r.note}`);
      process.exit(r.ok ? 0 : 1);
    })
    .catch(e => {
      console.error('[rakuten-unshipped] 失敗:', e.message);
      process.exit(1);
    });
}
