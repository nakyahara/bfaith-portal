/**
 * 楽天 未発送アラート — 検知して GChat へ通知する (ミニPC実行)
 *
 * 何を見ているか (詳細は service.js の冒頭コメント):
 *   前日12:00 (出荷の締め) までに入金確認 (受注確定) できていたのに、
 *   まだ発送されていない楽天の注文。
 *
 * どこで動くか:
 *   ミニPC の daily-sync (毎朝07:00、apps/warehouse/daily-sync.js) の1ステップとして実行される。
 *   ⚠️Render では動かさない — 楽天RMSは「同じキーで受注変更もできるAPI」なので、
 *     API集約方針によりミニPC経由必須 (RenderにRMSキーを置かない)。
 *   失敗した日は retry-failed-jobs (08:30/10:00/11:30) が拾って当日中に通知する。
 *
 * 環境変数 (ミニPC の .env):
 *   GCHAT_WEBHOOK_SHIPPING             … 通知先スペースの webhook (未設定ならエラー終了)
 *   RAKUTEN_UNSHIPPED_CUTOFF_HOUR      … 締め時刻 (既定 12)
 *   RAKUTEN_UNSHIPPED_SEARCH_DAYS      … 遡る日数 (既定 180。60日窓に分割して検索)
 *   RAKUTEN_UNSHIPPED_LEAD_DAYS        … お届け日指定を保留にできる猶予日数 (既定 2。
 *                                        お届け日が今日+この日数以内なら出荷漏れ扱い)
 *   RAKUTEN_SERVICE_SECRET / RAKUTEN_LICENSE_KEY … RMS API 認証 (rakuten-client.js)
 *
 * 使い方:
 *   node apps/rakuten-unshipped/notify-job.js --once       … 実行して GChat へ送る (daily-sync はこれ)
 *   node apps/rakuten-unshipped/notify-job.js --dry-run    … 送らずに本文を標準出力へ
 *
 * 0件の日も通知する。「通知が来ない = 止まっている」と読めるようにするため。
 */
import 'dotenv/config';
import { fileURLToPath } from 'node:url';
import {
  findUnshippedOrders,
  buildMessage,
  DEFAULT_CUTOFF_HOUR,
  DEFAULT_SEARCH_DAYS,
  DEFAULT_DELIVERY_LEAD_DAYS,
} from './service.js';
import { sendGChatMessage } from '../profit-analysis/gchat-client.js';

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

/** GChat 送信。一時障害に備えて1回だけリトライする */
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
 * 検知 → 通知。
 * @param {{dryRun?:boolean, now?:Date}} opts
 * @returns {Promise<{ok:boolean, partial:boolean, note:string, text:string, alerts:number, holds:number}>}
 */
export async function runUnshippedAlert(opts = {}) {
  const cutoffHour = numEnv('RAKUTEN_UNSHIPPED_CUTOFF_HOUR', DEFAULT_CUTOFF_HOUR, { min: 0, max: 23 });
  const searchDays = numEnv('RAKUTEN_UNSHIPPED_SEARCH_DAYS', DEFAULT_SEARCH_DAYS, { min: 1, max: 365 });
  const leadDays = numEnv('RAKUTEN_UNSHIPPED_LEAD_DAYS', DEFAULT_DELIVERY_LEAD_DAYS, { min: 0, max: 30 });

  // 送り先が無いのは設定漏れ。RMS API を無駄に叩く前に落とす (静かに無通知にしない)
  const webhook = process.env.GCHAT_WEBHOOK_SHIPPING;
  if (!opts.dryRun && !webhook) {
    throw new Error('GCHAT_WEBHOOK_SHIPPING 未設定のため通知できません');
  }

  const result = await findUnshippedOrders({ now: opts.now, cutoffHour, searchDays, leadDays });
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

  await sendWithRetry(webhook, text);
  console.log(`[rakuten-unshipped] 通知しました (${note})`);
  return { ok: true, partial, note, text, alerts: alerts.length, holds: holds.length };
}

/**
 * 実行結果 → プロセス終了コード。
 *   0 … 正常
 *   2 … 通知は送れたが結果が不完全 (検索打ち切り・日時不正・明細欠落)。
 *       daily-sync 側はこれを blocked (retry しない失敗) として扱い、サマリに ❌ を出す。
 *       「不完全なのに ✅ で流れる」のを防ぐのが目的
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
    console.log('使い方: node apps/rakuten-unshipped/notify-job.js --once | --dry-run');
    process.exit(1);
  }
  runUnshippedAlert({ dryRun })
    .then(r => {
      // daily-sync はこの行の末尾をサマリに載せる
      console.log(`[rakuten-unshipped] 完了: ${r.note}`);
      process.exit(exitCodeFor(r));
    })
    .catch(e => {
      console.error('[rakuten-unshipped] 失敗:', e.message);
      process.exit(1);
    });
}
