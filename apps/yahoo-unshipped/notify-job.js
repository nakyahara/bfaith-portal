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
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';
import { initDB, getDB } from '../warehouse/db.js';
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
 * @returns {Promise<{ok:boolean, note:string, text:string, alerts:object[], recent:object[], apiFailed:number, truncated:boolean}>}
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
  // dry-run では解消済みキャッシュを書かない (確認だけのつもりが次回の候補を変えてしまわないように)
  const result = await findUnshippedOrders({
    now: opts.now, cutoffHour, searchDays, maxVerify, skipCacheWrite: Boolean(opts.dryRun),
  });
  const { alerts, recent, candidates, apiFailed, truncated } = result;
  const text = buildMessage(result);
  const note = `未発送${alerts.length}件 / 候補${candidates}件`
    + (recent.length ? ` / 締め後に動き${recent.length}件` : '')
    + (apiFailed ? ` / ⚠確認不能${apiFailed}件` : '')
    + (truncated ? ' / ⚠上限で打ち切り' : '');

  if (opts.dryRun) {
    console.log(`[yahoo-unshipped] dry-run (${note})\n${text}`);
    return { ...result, ok: true, note: `${note} (dry-run)`, text };
  }

  await sendWithRetry(webhook, text);
  console.log(`[yahoo-unshipped] 通知しました (${note})`);
  return { ...result, ok: true, note, text };
}

/**
 * 実行結果 → プロセス終了コード。
 *   0 … 正常
 *   1 … 一部の注文の最新状態を確認できなかった。**通知は送れているが retry したい** —
 *       確認できなかった中に出荷漏れが隠れている可能性があるので、当日中 (08:30/10:00/11:30) に
 *       もう一度確認する。再通知が重複するより、出荷漏れを翌日まで見逃す方が痛い
 *       (Codexレビュー High)
 *   2 … 候補が上限を超えて打ち切った。retry しても同じ結果なので daily-sync 側では blocked
 *       (サマリに ❌ は出るが再試行しない)。残りは翌日以降に回る
 * (完全な失敗 = 例外は呼び出し側で 1)
 */
export function exitCodeFor(result) {
  if ((result?.apiFailed ?? 0) > 0) return 1;
  if (result?.truncated) return 2;
  return 0;
}

const TAG = 'yahoo-unshipped';

/**
 * 終了処理。
 * 🚨process.exit() を即座に呼ぶと、開いたままのハンドル (GChat送信で使った fetch の接続、SQLite)
 *   の終了処理と競合し、Windows で
 *   `Assertion failed: !(handle->flags & UV_HANDLE_CLOSING)` を出して abort することがある
 *   (2026-08-10 の Yahoo 版初回本番実行で実際に発生。通知は送れているのに終了コードが
 *    0xC0000409 になり、daily-sync がステップ失敗と誤判定 → retry で通知が重複する)。
 *   ⚠️原因のハンドルは特定できていない (楽天版は SQLite を使わないので fetch 側が本命)。
 *     どちらであっても「開いているものを閉じてから自然終了させる」この形で回避できる。
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
  // 「毎回ここを通っている」ことに気づけるよう必ず警告を残す。
  // console.error は非同期に流れることがあり直後の process.exit で捨てられうるため同期で書く
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
    console.log('使い方: node apps/yahoo-unshipped/notify-job.js --once | --dry-run');
    process.exit(1);
  }
  runYahooUnshippedAlert({ dryRun })
    .then(r => {
      // daily-sync はこの行の末尾をサマリに載せる
      console.log(`[yahoo-unshipped] 完了: ${r.note}`);
      finish(dryRun ? 0 : exitCodeFor(r));
    })
    .catch(e => {
      console.error('[yahoo-unshipped] 失敗:', e.message);
      finish(1);
    });
}
