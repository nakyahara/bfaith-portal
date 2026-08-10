/**
 * Yahoo!ショッピング 未発送アラート — 検知して GChat へ通知する (ミニPC実行)
 *
 * 何を見ているか (詳細は service.js の冒頭コメント):
 *   前日12:00 (出荷の締め) までに入金確認できていたのに、まだ発送されていない注文。
 *   warehouse.db で候補を絞り → Yahoo受注APIで最新状態を確認してから通知する
 *   (DBは直近7日窓でしか更新されないため、DBだけだと出荷済み・キャンセル済みを誤検知する)。
 *
 * どこで動くか:
 *   ミニPC の daily-sync (毎朝07:00) の1ステップ。**Yahoo受注の同期より後**に置くこと
 *   (DBの候補が古いと、その分だけAPI確認の空振りが増える)。
 *   失敗した日は retry-failed-jobs (08:30/10:00/11:30) が拾って当日中に通知する。
 *
 * 環境変数 (ミニPC の .env):
 *   GCHAT_WEBHOOK_SHIPPING          … 通知先スペースの webhook (楽天版と同じスペース)
 *   YAHOO_PROXY_URL / YAHOO_PROXY_SECRET … VPSプロキシ (既存のYahoo設定を使う)
 *   YAHOO_UNSHIPPED_CUTOFF_HOUR     … 締め時刻 (既定 12)
 *   YAHOO_UNSHIPPED_SEARCH_DAYS     … DBから候補を拾う期間 (日、既定 180)
 *   YAHOO_UNSHIPPED_MAX_VERIFY      … API確認する候補数の上限 (既定 60。1件1秒)
 *
 * 使い方:
 *   node apps/yahoo-unshipped/notify-job.js --once       … 実行して GChat へ送る
 *   node apps/yahoo-unshipped/notify-job.js --dry-run    … 送らずに本文を標準出力へ
 */
import 'dotenv/config';
import { fileURLToPath } from 'node:url';
import { initDB } from '../warehouse/db.js';
import {
  findUnshippedOrders,
  buildMessage,
  DEFAULT_CUTOFF_HOUR,
  DEFAULT_SEARCH_DAYS,
  MAX_VERIFY,
} from './service.js';
import { sendGChatMessage } from '../profit-analysis/gchat-client.js';

/** env の数値を読む (不正値は既定にフォールバックして警告) */
function numEnv(name, fallback, { min, max }) {
  const raw = String(process.env[name] ?? '').trim();
  if (!raw) return fallback;
  const n = Number(raw);
  if (!Number.isFinite(n) || n < min || n > max) {
    console.warn(`[yahoo-unshipped] ${name}=${raw} は不正なため既定値 ${fallback} を使います`);
    return fallback;
  }
  return n;
}

/** GChat 送信。一時障害に備えて1回だけリトライする */
async function sendWithRetry(webhook, text) {
  try {
    await sendGChatMessage(webhook, text);
  } catch (e) {
    console.warn(`[yahoo-unshipped] GChat送信に失敗、10秒後に再試行: ${e.message}`);
    await new Promise(r => setTimeout(r, 10_000));
    await sendGChatMessage(webhook, text);
  }
}

/**
 * 検知 → 通知。
 * @returns {Promise<{ok:boolean, partial:boolean, note:string, text:string, alerts:number}>}
 */
export async function runYahooUnshippedAlert(opts = {}) {
  const cutoffHour = numEnv('YAHOO_UNSHIPPED_CUTOFF_HOUR', DEFAULT_CUTOFF_HOUR, { min: 0, max: 23 });
  const searchDays = numEnv('YAHOO_UNSHIPPED_SEARCH_DAYS', DEFAULT_SEARCH_DAYS, { min: 1, max: 365 });
  const maxVerify = numEnv('YAHOO_UNSHIPPED_MAX_VERIFY', MAX_VERIFY, { min: 1, max: 300 });

  // 送り先が無いのは設定漏れ。DB/API を触る前に落とす (静かに無通知にしない)
  const webhook = process.env.GCHAT_WEBHOOK_SHIPPING;
  if (!opts.dryRun && !webhook) {
    throw new Error('GCHAT_WEBHOOK_SHIPPING 未設定のため通知できません');
  }

  initDB();
  const result = await findUnshippedOrders({ now: opts.now, cutoffHour, searchDays, maxVerify });
  const { alerts, candidates, apiFailed, truncated } = result;
  const text = buildMessage(result);
  // 確認できなかった注文・上限打ち切りがあった日は partial (結果が不完全)
  const partial = apiFailed > 0 || Boolean(truncated);
  const note = `未発送${alerts.length}件 / 候補${candidates}件`
    + (partial ? ` / ⚠不完全(確認不能=${apiFailed} 打切=${truncated ? 1 : 0})` : '');

  if (opts.dryRun) {
    console.log(`[yahoo-unshipped] dry-run (${note})\n${text}`);
    return { ok: true, partial, note: `${note} (dry-run)`, text, alerts: alerts.length };
  }

  await sendWithRetry(webhook, text);
  console.log(`[yahoo-unshipped] 通知しました (${note})`);
  return { ok: true, partial, note, text, alerts: alerts.length };
}

/**
 * 実行結果 → プロセス終了コード。
 *   0 … 正常 / 2 … 通知は送れたが結果が不完全 (daily-sync 側で blocked = retry しない失敗)
 * (完全な失敗 = 例外は呼び出し側で 1)
 */
export function exitCodeFor(result) {
  return result?.partial ? 2 : 0;
}

// ─── CLI ───
const isMain = process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1];
if (isMain) {
  const dryRun = process.argv.includes('--dry-run');
  if (!dryRun && !process.argv.includes('--once')) {
    console.log('使い方: node apps/yahoo-unshipped/notify-job.js --once | --dry-run');
    process.exit(1);
  }
  runYahooUnshippedAlert({ dryRun })
    .then(r => {
      // daily-sync はこの行の末尾をサマリに載せる
      console.log(`[yahoo-unshipped] 完了: ${r.note}`);
      process.exit(exitCodeFor(r));
    })
    .catch(e => {
      console.error('[yahoo-unshipped] 失敗:', e.message);
      process.exit(1);
    });
}
