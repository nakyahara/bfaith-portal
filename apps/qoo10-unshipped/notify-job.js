/**
 * Qoo10 未発送アラート — 検知して GChat へ通知する (ミニPC実行)
 *
 * 何を見ているか (詳細は service.js の冒頭コメント):
 *   前日12:00 (出荷の締め) より前の注文で、いま「Seller confirm(3) = 発送できる状態」の
 *   まま発送されていないもの。
 *   ⚠️Qoo10 は状態が変わった時刻を持たないので「締め時点で発送可能だったか」までは分からない
 *     (前払い・後払いで締め後に入金された注文も混ざりうる)。通知本文にその旨を明記している。
 *
 * ⭐️Qoo10 は API 再確認をしない。daily-sync の Qoo10 同期が直近90日を毎回取り直しており、
 *   さらに**キャンセル済みは API から消える = last_seen_at が古くなる**ので、
 *   「今日の同期で確認できた注文」に絞るだけで最新状態を判定できる。
 *   ⚠️今日の同期が0件なら判定を見送る (古いデータで誤報告しないため。exit 1)。
 *
 * どこで動くか:
 *   ミニPC の daily-sync (毎朝07:00) の1ステップ。**Qoo10受注の同期より後**に置くこと。
 *   失敗した日は retry-failed-jobs (08:30/10:00/11:30) が拾って当日中に通知する。
 *
 * 環境変数 (ミニPC の .env):
 *   GCHAT_WEBHOOK_SHIPPING          … 通知先スペースの webhook (3モール共通)
 *   QOO10_UNSHIPPED_CUTOFF_HOUR     … 締め時刻 (既定 12)
 *
 * 使い方:
 *   node apps/qoo10-unshipped/notify-job.js --once       … 実行して GChat へ送る
 *   node apps/qoo10-unshipped/notify-job.js --dry-run    … 送らずに本文を標準出力へ
 */
import 'dotenv/config';
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';
import { initDB, getDB } from '../warehouse/db.js';
import { findUnshippedOrders, buildMessage, DEFAULT_CUTOFF_HOUR } from './service.js';
import { sendGChatMessage } from '../profit-analysis/gchat-client.js';

/** env の数値を読む (不正値は既定にフォールバックして警告) */
function numEnv(name, fallback, { min, max }) {
  const raw = String(process.env[name] ?? '').trim();
  if (!raw) return fallback;
  const n = Number(raw);
  if (!Number.isFinite(n) || n < min || n > max) {
    console.warn(`[qoo10-unshipped] ${name}=${raw} は不正なため既定値 ${fallback} を使います`);
    return fallback;
  }
  return n;
}

/** GChat 送信。一時障害に備えて1回だけリトライする */
async function sendWithRetry(webhook, text) {
  try {
    await sendGChatMessage(webhook, text);
  } catch (e) {
    console.warn(`[qoo10-unshipped] GChat送信に失敗、10秒後に再試行: ${e.message}`);
    await new Promise(r => setTimeout(r, 10_000));
    await sendGChatMessage(webhook, text);
  }
}

/**
 * 検知 → 通知。
 * @returns {Promise<{ok:boolean, note:string, text:string, alerts:object[], stale:boolean}>}
 */
export async function runQoo10UnshippedAlert(opts = {}) {
  const cutoffHour = numEnv('QOO10_UNSHIPPED_CUTOFF_HOUR', DEFAULT_CUTOFF_HOUR, { min: 0, max: 23 });

  // 送り先が無いのは設定漏れ。DB を触る前に落とす (静かに無通知にしない)
  const webhook = process.env.GCHAT_WEBHOOK_SHIPPING;
  if (!opts.dryRun && !webhook) {
    throw new Error('GCHAT_WEBHOOK_SHIPPING 未設定のため通知できません');
  }

  initDB();
  const result = findUnshippedOrders({ now: opts.now, cutoffHour });
  const { alerts, awaitingPayment, seenToday, stale } = result;
  const text = buildMessage(result);
  const note = stale
    ? '⚠Qoo10の同期が未実行のため判定を見送り'
    : `未発送${alerts.length}件 / 今朝の確認${seenToday}件`
      + (awaitingPayment ? ` / 入金待ち${awaitingPayment}件` : '');

  if (opts.dryRun) {
    console.log(`[qoo10-unshipped] dry-run (${note})\n${text}`);
    return { ...result, ok: true, note: `${note} (dry-run)`, text };
  }

  await sendWithRetry(webhook, text);
  console.log(`[qoo10-unshipped] 通知しました (${note})`);
  return { ...result, ok: true, note, text };
}

/**
 * 実行結果 → プロセス終了コード (3モール共通の規約)。
 *   0 … 正常
 *   1 … 判定できなかった (Qoo10の同期が今日走っていない)。
 *       同期が終わってから retry (08:30/10:00/11:30) で拾い直したいので retry 対象にする
 * (完全な失敗 = 例外も呼び出し側で 1)
 */
export function exitCodeFor(result) {
  return result?.stale ? 1 : 0;
}

const TAG = 'qoo10-unshipped';

/**
 * 終了処理。
 * 🚨process.exit() を即座に呼ぶと、開いたままのハンドル (GChat送信で使った fetch の接続、SQLite)
 *   の終了処理と競合し、Windows で
 *   `Assertion failed: !(handle->flags & UV_HANDLE_CLOSING)` を出して abort することがある
 *   (2026-08-10 に Yahoo 版で実際に発生。通知は送れているのに終了コードが 0xC0000409 になり、
 *    daily-sync がステップ失敗と誤判定 → retry で通知が重複する)。
 *   → exitCode を立てて**自然終了**させ、万一終わらない時だけ強制終了する。
 */
const FORCE_EXIT_AFTER_MS = 5000;

export function finish(code) {
  try {
    getDB().close();
  } catch (e) {
    // 開いていなければ何もしない。閉じられない場合も終了は続けるが、黙らせない
    if (!/not open|closed/i.test(e.message)) console.warn(`[${TAG}] DBのclose に失敗: ${e.message}`);
  }
  process.exitCode = code;
  // このタイマー自体は unref するのでイベントループの終了を妨げない。
  // 発火した = 何かのハンドルが残っている = この強制終了で元のクラッシュが再発しうるので、
  // 「毎回ここを通っている」ことに気づけるよう必ず警告を残す (同期書き込み)
  setTimeout(() => {
    const msg = `[${TAG}] ${FORCE_EXIT_AFTER_MS}ms 待っても終了しないため強制終了します (残っているハンドルを調べること)`;
    try { fs.writeSync(2, msg + '\n'); } catch { /* 書けなくても終了は続ける */ }
    process.exit(code);
  }, FORCE_EXIT_AFTER_MS).unref();
}

// ─── CLI ───
const isMain = process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1];
if (isMain) {
  const dryRun = process.argv.includes('--dry-run');
  if (!dryRun && !process.argv.includes('--once')) {
    console.log('使い方: node apps/qoo10-unshipped/notify-job.js --once | --dry-run');
    process.exit(1);
  }
  runQoo10UnshippedAlert({ dryRun })
    .then(r => {
      // daily-sync はこの行の末尾をサマリに載せる
      console.log(`[qoo10-unshipped] 完了: ${r.note}`);
      finish(dryRun ? 0 : exitCodeFor(r));
    })
    .catch(e => {
      console.error('[qoo10-unshipped] 失敗:', e.message);
      finish(1);
    });
}
