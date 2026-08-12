/**
 * Yahoo!ショッピング 問い合わせ対応漏れチェック — 検知して GChat へ通知する (ミニPC実行)
 *
 * 何を見ているか (詳細は service.js の冒頭コメント):
 *   ① 未返信 (出店者回答待ち) の問い合わせ
 *   ② 返信済みだが「完了する」(回答済み) 処理がされていない問い合わせ
 *   対象は「通常」と「オークション (落札前)」の両方。**該当が1件以上あるときだけ**通知する。
 *
 * どこで動くか:
 *   ミニPC の daily-sync (毎朝07:00) の1ステップ (未発送アラート群の直後)。
 *   warehouse.db には依存しない (Yahoo!の問い合わせAPIを直接読むだけ)。
 *   失敗した日は retry-failed-jobs (08:30/10:00/11:30) が拾って当日中に再実行する。
 *
 * 安全設計:
 *   - 照会のみ。返信・完了処理・既読化などデータ変更は一切しない
 *     (VPSプロキシの read-only passthrough しか叩かない)
 *   - 実行失敗 (認証切れ・API変更など) も同じ webhook に ❌ で通知する
 *     (該当ゼロ=無通知 の仕様なので、静かに止まると気づけないため)
 *   - 件数が読み取れないときは 0件と混同せずエラーにする (service.js の fail-closed 検証)
 *   - 通知は **at-least-once** (Codexレビュー2巡目 Medium): 送信後・終了判定前に
 *     プロセスが落ちると daily-sync は失敗と記録し、retry が同じ内容を再送する。
 *     未発送アラート群と同じ割り切り — 通知が重複する痛みより、対応漏れを
 *     翌日まで見逃す痛みの方が大きい (状態ファイルでの送信済み抑止はあえて持たない)
 *
 * 環境変数 (ミニPC の .env):
 *   GCHAT_WEBHOOK_YAHOO_INQUIRY     … 通知先スペースの webhook (必須)
 *   YAHOO_PROXY_URL / YAHOO_PROXY_SECRET … VPSプロキシ (既存のYahoo設定を使う)
 *
 * 使い方:
 *   node apps/yahoo-inquiry-alert/notify-job.js --once       … 実行して該当があれば GChat へ送る
 *   node apps/yahoo-inquiry-alert/notify-job.js --dry-run    … 送らずに結果を標準出力へ
 */
import 'dotenv/config';
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';
import { fetchInquiryStatus, buildMessage } from './service.js';
import { sendGChatMessage } from '../profit-analysis/gchat-client.js';

const TAG = 'yahoo-inquiry';

/** GChat 送信。一時障害に備えて1回だけリトライする */
async function sendWithRetry(webhook, text) {
  try {
    await sendGChatMessage(webhook, text);
  } catch (e) {
    console.warn(`[${TAG}] GChat送信に失敗、10秒後に再試行: ${e.message}`);
    await new Promise(r => setTimeout(r, 10_000));
    await sendGChatMessage(webhook, text);
  }
}

/**
 * 検知 → 通知。
 * @returns {Promise<{ok:boolean, note:string, text:string|null, unanswered:object, uncompleted:object}>}
 */
export async function runYahooInquiryAlert(opts = {}) {
  // 送り先が無いのは設定漏れ。API を触る前に落とす (静かに無通知にしない)
  const webhook = process.env.GCHAT_WEBHOOK_YAHOO_INQUIRY;
  if (!opts.dryRun && !webhook) {
    throw new Error('GCHAT_WEBHOOK_YAHOO_INQUIRY 未設定のため通知できません');
  }

  const result = await fetchInquiryStatus(opts);
  const text = buildMessage(result);
  const note = `未返信${result.unanswered.count}件 / 完了処理待ち${result.uncompleted.count}件`;

  if (opts.dryRun) {
    console.log(`[${TAG}] dry-run (${note})`);
    console.log(text ?? '(該当0件 — 通知なし)');
    return { ...result, ok: true, note: `${note} (dry-run)`, text };
  }

  if (text === null) {
    // 該当ゼロは通知しない仕様。「実行されたか」は daily-sync のサマリと実行ログで確認できる
    console.log(`[${TAG}] 該当0件のため通知なし (${note})`);
    return { ...result, ok: true, note, text };
  }

  await sendWithRetry(webhook, text);
  console.log(`[${TAG}] 通知しました (${note})`);
  return { ...result, ok: true, note, text };
}

/**
 * 実行失敗を同じ webhook へ知らせる (best effort — これ自体の失敗で元のエラーを隠さない)。
 * 該当ゼロ=無通知 の仕様なので、失敗を通知しないと「静かな停止」と「平和な0件」を
 * 受け手が区別できない。retry (08:30/10:00/11:30) が成功すればその日は正常に戻る。
 */
async function notifyFailure(error) {
  const webhook = process.env.GCHAT_WEBHOOK_YAHOO_INQUIRY;
  if (!webhook) return;
  const text = `❌ Yahoo!問い合わせチェックの実行に失敗: ${String(error?.message || error).slice(0, 300)}\n`
    + '(daily-sync の retry で当日中に自動再実行されます。連続して出る場合は要調査)';
  try {
    await sendGChatMessage(webhook, text);
  } catch (e) {
    console.warn(`[${TAG}] 失敗通知の送信もできませんでした: ${e.message}`);
  }
}

/**
 * 終了処理。
 * 🚨process.exit() を即座に呼ぶと、開いたままの fetch 接続の終了処理と競合して
 *   Windows で `Assertion failed: !(handle->flags & UV_HANDLE_CLOSING)` abort になることがある
 *   (2026-08-10 の yahoo-unshipped 初回本番で実発生。通知は送れているのに 0xC0000409 で
 *    終了し、daily-sync が失敗と誤判定 → retry で通知が重複する)。
 *   → exitCode を立てて自然終了させ、万一終わらない時だけ強制終了する (未発送アラート群と同じ形)。
 */
const FORCE_EXIT_AFTER_MS = 5000;

export function finish(code) {
  process.exitCode = code;
  // unref するのでイベントループの終了は妨げない。発火した = 何かのハンドルが残っている
  // 証拠なので、同期書き込みで必ず痕跡を残す (console.error は exit で捨てられうる)
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
    console.log('使い方: node apps/yahoo-inquiry-alert/notify-job.js --once | --dry-run');
    process.exit(1);
  }
  runYahooInquiryAlert({ dryRun })
    .then(r => {
      // daily-sync はこの行の末尾をサマリに載せる
      console.log(`[${TAG}] 完了: ${r.note}`);
      finish(0);
    })
    .catch(async e => {
      console.error(`[${TAG}] 失敗:`, e.message);
      if (!dryRun) await notifyFailure(e);
      finish(1);
    });
}
