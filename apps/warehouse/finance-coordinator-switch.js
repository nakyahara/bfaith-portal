/**
 * finance-coordinator-switch.js — Amazon の決済と財務を coordinator で回すかのスイッチ (#1567・D7b-1b-3)
 *
 * env CDB_FINANCE_COORDINATOR=1 のときだけ coordinator (amazon-finance-coverage-run.js = 決済の取込 + Company DB の Amazon 財務 + 決済のそろい) で回す。
 * 無い (1 以外) = 今までの 2 工程 (「Amazon Settlement」= fetch-amazon-settlements.js --days 14 → 「CompanyDB財務(Amazon)」= amazon-finance.mjs) のまま。
 *   見る所 = daily-sync.js (どの工程を呼ぶか)・retry-failed-jobs.js (retry-state の工程の名前の読み替え)・
 *            fetch-amazon-settlements.js / amazon-finance.mjs の単独の入口 (無い = 今までどおり書く・ある = 書く回は coordinator だけ)
 * 🚨 足すのは定期実行の前のハードゲート (夜に手で実の --full を 1 回: exit 0・60 分以内・最大メモリ 1,200 MB 以下) に合格した後・中原さんの指示の後。
 *    .env は miniPC のリポジトリの直下の 1 つだけ (書き直して他の設定を消さない = 2026-09-30 の件)。
 * 一時物 = 台帳 config/jobs-registry.mjs の cdb-finance-coordinator-switch (remove_by 2026-11-30 = スイッチを消して常に coordinator にする)
 */
import { amazonFinanceDailyArgs } from './amazon-finance-months.js';

export const FINANCE_COORDINATOR_ENV = 'CDB_FINANCE_COORDINATOR';

/** coordinator で回すか (env がちょうど '1' のときだけ。空・0・ほかの値 = 今までの 2 工程) */
export function financeCoordinatorEnabled(env = process.env) {
  return String(env[FINANCE_COORDINATOR_ENV] ?? '').trim() === '1';
}

/**
 * daily-sync の決済の取込の工程 = { name (結果の名前 = retry の工程の名前), cmd (runScript に渡す), label, timeoutMs }
 *   スイッチが無い = master (PR #1567 の前) と同じ「Amazon Settlement」= fetch-amazon-settlements.js --days 14 (60 分)
 *   スイッチがある = 「Amazon決済と財務」= coordinator (90 分)。runScript は引数なしだと '7' を足すので必ず引数を渡す
 */
export function settlementStep({ coordinator = financeCoordinatorEnabled() } = {}) {
  return coordinator
    ? { name: 'Amazon決済と財務', cmd: 'apps/warehouse/amazon-finance-coverage-run.js --source v2', label: 'Amazon決済と財務', timeoutMs: 5400000 }
    : { name: 'Amazon Settlement', cmd: 'apps/warehouse/fetch-amazon-settlements.js --days 14', label: 'Amazon Settlement', timeoutMs: 3600000 };
}

/**
 * daily-sync の Company DB の Amazon 財務の送信の工程。スイッチがある = null (coordinator の工程の中で送る)
 *   スイッチが無い = master と同じ「CompanyDB財務(Amazon)」= amazon-finance.mjs (日曜は --full・ほかは --incremental・--require-backfilled・30 分)
 */
export function financePushStep(businessDate, { coordinator = financeCoordinatorEnabled() } = {}) {
  if (coordinator) return null;
  const financeArgs = amazonFinanceDailyArgs(businessDate);
  return { name: 'CompanyDB財務(Amazon)', cmd: `apps/company-db/push/amazon-finance.mjs ${financeArgs.join(' ')}`, label: `Company DB Amazon 財務 push (${financeArgs[0]})`, timeoutMs: 1800000 };
}
