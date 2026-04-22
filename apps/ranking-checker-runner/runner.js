#!/usr/bin/env node
/**
 * 楽天順位チェッカー Runner (miniPC版)
 *
 * 配置: Windows Task Scheduler → run-rankcheck-safe.ps1 → このスクリプト
 * 起動: 1日1回 (既定 13:00 JST) + 必要に応じて手動 (--force)
 *
 * 依存:
 *   - apps/ranking-checker/auto-check.js の runAutoCheck() を直接呼ぶ
 *   - ranking-checker.db は DATA_DIR / RANKCHECK_DB_FILE 指定先 (通常 C:\tools\rankcheck-runner\data)
 *   - .env から RAKUTEN_APP_ID / RAKUTEN_ACCESS_KEY / AMAZON_* / RANKCHECK_AUTO_ENABLED 等
 *
 * 特徴:
 *   - 起動→実行→終了。常駐しない (メモリ節約 + safe-debug 思想)
 *   - run_state / run_log で事後調査可
 *   - 終了コード: 0=正常、1=致命的エラー、2=既に実行中
 *
 * Usage:
 *   node apps/ranking-checker-runner/runner.js              # 通常実行
 *   node apps/ranking-checker-runner/runner.js --force      # 本日チェック済みも再実行
 *   node apps/ranking-checker-runner/runner.js --help       # ヘルプ
 */
import 'dotenv/config';
import { pathToFileURL } from 'url';
import { runAutoCheck } from '../ranking-checker/auto-check.js';
import { getLatestRun, getRunningRun } from '../ranking-checker/db.js';

function usage() {
  console.log(`Usage: node apps/ranking-checker-runner/runner.js [--force] [--help]

Options:
  --force    本日チェック済みの商品も再チェックする
  --help     このメッセージを表示`);
}

async function main() {
  const args = process.argv.slice(2);
  if (args.includes('--help') || args.includes('-h')) {
    usage();
    process.exit(0);
  }

  const force = args.includes('--force');
  const startedAt = new Date().toISOString();

  console.log(`[runner] 起動 ${startedAt} pid=${process.pid} force=${force}`);
  console.log(`[runner] DATA_DIR=${process.env.DATA_DIR || '(default)'}`);
  console.log(`[runner] RANKCHECK_DB_FILE=${process.env.RANKCHECK_DB_FILE || '(default)'}`);

  // 多重起動防止: getCheckProgress().running はプロセス内メモリなのでプロセス間では効かない。
  // 代わりに DB の run_state を確認する。
  // 注意: /service-api/rankcheck/run-check 等で別プロセスが進行中なら、この Runner は break せず
  //       そのまま動いてもよい (DB 書き込みは UPSERT なので破壊的ではない)。
  //       ただし誤って Task Scheduler が重複起動したケースは防ぐべき。
  //       markStaleRunning は新 run 開始直前に走るので、極端な古い running は上書きされる。

  const before = getLatestRun();
  if (before) {
    console.log(`[runner] 直前 run: ${before.run_id} status=${before.status} done=${before.done}/${before.total}`);
  }

  // 最低限の env 前提チェック。欠落は EX_CONFIG (78) で落とす。
  if (!process.env.RAKUTEN_APP_ID || !process.env.RAKUTEN_ACCESS_KEY) {
    console.error('[runner] 設定エラー: RAKUTEN_APP_ID / RAKUTEN_ACCESS_KEY 未設定');
    process.exit(78);
  }

  // 多重起動ガード (プロセス間): DB の run_state.status='running' があれば
  //   - 3時間以内の "fresh" → 別プロセスが動いているので黙って退く (exit 73)
  //   - 3時間以上の "stale" → そのまま進む (runAutoCheck 内で markStaleRunning が failed 化する)
  const running = getRunningRun();
  if (running) {
    const startedMs = Date.parse(running.started_at.replace(' ', 'T') + 'Z');
    const ageMs = Date.now() - startedMs;
    const FRESH_MS = 3 * 60 * 60 * 1000;
    if (ageMs >= 0 && ageMs < FRESH_MS) {
      console.error(`[runner] 既存 running (${running.run_id}, age=${Math.round(ageMs / 60000)}分) を検知、今回は skip`);
      process.exit(73);
    }
    console.log(`[runner] stale running (${running.run_id}, age=${Math.round(ageMs / 60000)}分) を検知、続行し markStaleRunning で failed 化する`);
  }

  try {
    await runAutoCheck({ force });
    const after = getLatestRun();
    console.log(`[runner] 完了 run_id=${after?.run_id} status=${after?.status} done=${after?.done}/${after?.total}`);
    // run_state が failed で終わっていれば非0で返す
    if (after && after.status === 'failed') {
      console.error(`[runner] 直前 run が failed: ${after.error || '(no error msg)'}`);
      process.exit(1);
    }
    process.exit(0);
  } catch (e) {
    console.error(`[runner] 致命的エラー: ${e.message}`);
    console.error(e.stack);
    process.exit(1);
  }
}

// Node で直接起動されたときだけ main() を実行。
// 他モジュールから import されたときは副作用を起こさない (テスト容易化)。
function isMain() {
  return import.meta.url === pathToFileURL(process.argv[1] || '').href;
}
if (isMain()) main();
