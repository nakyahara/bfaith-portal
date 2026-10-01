/**
 * finance-coordinator-switch.js — Amazon の決済と財務を coordinator で回すかのスイッチ (#1567・D7b-1b-3)
 *
 * env CDB_FINANCE_COORDINATOR=1 (ちょうど '1') のときだけ coordinator (amazon-finance-coverage-run.js = 決済の取込 + Company DB の Amazon 財務 + 決済のそろい) で回す。
 * 無い = 今までの 2 工程 (「Amazon Settlement」= fetch-amazon-settlements.js --days 14 → 「CompanyDB財務(Amazon)」= amazon-finance.mjs) のまま。
 *   見る所 = daily-sync.js (どの工程を呼ぶか)・retry-failed-jobs.js (retry-state の工程の名前の読み替え)・
 *            fetch-amazon-settlements.js / amazon-finance.mjs の単独の入口 (無い = 今までどおり書く・ある = 書く回は coordinator だけ)
 * 🚨 **一方向** (#1567 Codex R6 High): 一度でも coordinator が coverage の回 (世代) で回った (= coverageEverRan) 後は、スイッチが無くても今までの 2 工程には戻らない =
 *    今までの取込 (生の表を書く前に Render を updating にしない) と送り手は、生の表を書く前・送る前に ❌ で止まる (勝手に coordinator も起動しない = 朝を赤くする)。
 *    手の実の --full (定期実行の前のハードゲート) が通った後に .env に足す前に朝が来た・足した後に .env から消えた、のどちらでも古い complete を残さない
 * 🚨 足すのはハードゲート (夜に手で実の --full を 1 回: exit 0・60 分以内・最大メモリ 1,200 MB 以下) に合格した直後・同じ保守の枠の中・中原さんの指示の後。
 *    .env は miniPC のリポジトリの直下の 1 つだけ (書き直して他の設定を消さない = 2026-09-30 の件)。手順 = db/company/README.md
 * 一時物 = 台帳 config/jobs-registry.mjs の cdb-finance-coordinator-switch (remove_by 2026-11-30 = スイッチを消して常に coordinator にする)
 */
import fs from 'node:fs';
import path from 'node:path';
import Database from 'better-sqlite3';
import { amazonFinanceDailyArgs } from './amazon-finance-months.js';

export const FINANCE_COORDINATOR_ENV = 'CDB_FINANCE_COORDINATOR';

/** coordinator で回すか (env がちょうど '1' のときだけ = 前後の空白も許さない (#1567 Codex R6 Low 1)。空・0・' 1 '・ほかの値 = 今までの 2 工程) */
export function financeCoordinatorEnabled(env = process.env) {
  return env[FINANCE_COORDINATOR_ENV] === '1';
}

// ── 一方向のスイッチの証拠 (#1567 Codex R6 High) ──
/** 財務の台帳 (company-db-push.db) の coverage の世代の鍵 = amazon-finance-coverage-run.js の LEDGER_META.generation */
export const COVERAGE_GENERATION_META = 'coverage_generation';
const LEDGER_FILE = 'company-db-push.db';               // = apps/company-db/push/ledger.mjs の LEDGER_FILE
const FINANCE_LEDGER_KIND = 'order_finance:amazon';     // = apps/company-db/push/amazon-finance.mjs の FINANCE_KIND (台帳の meta の鍵 = `${kind}:${key}`)

const tableExists = (db, name) => !!db.prepare(`SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?`).get(name);
/**
 * この環境が coverage (coordinator の世代) で回ったことがあるか。
 *   証拠 = 台帳 (company-db-push.db) の coverage の世代 **か** warehouse.db の一覧の回・手のファイル・初期の印の順番待ちに coverage の世代がある
 *   (台帳を失くしても warehouse.db で分かる。#1567 Codex R1 High 1)。表が無い = その表には証拠が無い (表は新しい initDB が作る = coordinator は必ずその後)
 *   ledger = { getMeta(key) } (openLedger の台帳 か ledgerMetaReader) / null
 */
export function coverageEverRan(db, ledger) {
  if (ledger && ledger.getMeta(COVERAGE_GENERATION_META) != null) return true;
  const q = [
    ['amazon_settlement_report_inventory_runs', 'coverage_generation'],
    ['amazon_settlement_manual_files', 'ingest_generation'],
    ['initial_marker_queue', 'applied_generation'],
  ].filter(([t]) => tableExists(db, t)).map(([t, c]) => `SELECT 1 FROM ${t} WHERE ${c} IS NOT NULL`);
  return q.length ? !!db.prepare(`${q.join(' UNION ALL ')} LIMIT 1`).get() : false;
}
/** 財務の台帳の meta を読むだけの口 (台帳のファイルを作らない・書かない)。ファイルが無い = null */
export function ledgerMetaReader(dataDir) {
  const file = path.join(dataDir, LEDGER_FILE);
  if (!fs.existsSync(file)) return null;
  const db = new Database(file, { readonly: true, fileMustExist: true, timeout: 30000 });
  try {
    const has = !!db.prepare(`SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'meta'`).get();
    const values = new Map(has ? db.prepare(`SELECT key, value FROM meta WHERE key LIKE ?`).all(`${FINANCE_LEDGER_KIND}:%`).map((r) => [r.key, r.value]) : []);
    return { getMeta: (k) => { const v = values.get(`${FINANCE_LEDGER_KIND}:${k}`); return v != null ? String(v) : null; } };
  } finally { db.close(); }
}
/** warehouse.db を読むだけで開いて coverageEverRan (retry・daily-sync 用)。読めない = null (分からない = 呼び手は安全側に倒す) */
export function coverageEverRanAt(dataDir) {
  try {
    const db = new Database(path.join(dataDir, 'warehouse.db'), { readonly: true, fileMustExist: true, timeout: 30000 });
    try { return coverageEverRan(db, ledgerMetaReader(dataDir)); } finally { db.close(); }
  } catch { return null; }
}

/** 一方向のスイッチを破ろうとした (coordinator で回ったことがあるのに CDB_FINANCE_COORDINATOR が無い) */
export class FinanceSwitchedBackError extends Error {
  constructor(what) {
    super(`🚨 coordinator に切り替え済み (coverage の世代がある) なのに ${FINANCE_COORDINATOR_ENV} が無い = ${what}をしない`
      + ` (.env に ${FINANCE_COORDINATOR_ENV}=1 をまだ足していない・消えた疑い。今までの 2 工程は生の表を書く前に Render を updating にしない = 古い complete が残りうる)。`
      + ` .env を確かめて足す (db/company/README.md の手順)。勝手に coordinator を起動しない`);
    this.code = 'FINANCE_SWITCHED_BACK';
  }
}
/** 今までの 2 工程 (書く取込・送る送り手) を始める前の門。coordinator で回ったことがあれば throw (#1567 Codex R6 High) */
export function assertLegacyAllowed(db, ledger, what) {
  if (coverageEverRan(db, ledger)) throw new FinanceSwitchedBackError(what);
}

/**
 * daily-sync の決済の取込の工程 = { name (結果の名前 = retry の工程の名前), cmd (runScript に渡す), label, timeoutMs }
 *   スイッチが無い = 入口・引数・時間の上限は master (PR #1567 の前) と同じ「Amazon Settlement」= fetch-amazon-settlements.js --days 14 (60 分)
 *     (中身は D-66 の版・85 日の固定の窓・coverage の lease・V2 の版の保存に変わっている = 完全に同じ動きではない)
 *   スイッチがある = 「Amazon決済と財務」= coordinator (90 分)。runScript は引数なしだと '7' を足すので必ず引数を渡す
 */
export function settlementStep({ coordinator = financeCoordinatorEnabled() } = {}) {
  return coordinator
    ? { name: 'Amazon決済と財務', cmd: 'apps/warehouse/amazon-finance-coverage-run.js --source v2', label: 'Amazon決済と財務', timeoutMs: 5400000 }
    : { name: 'Amazon Settlement', cmd: 'apps/warehouse/fetch-amazon-settlements.js --days 14', label: 'Amazon Settlement', timeoutMs: 3600000 };
}

/**
 * daily-sync の Company DB の Amazon 財務の送信の工程。スイッチがある = null (coordinator の工程の中で送る)
 *   スイッチが無い = 入口・引数・上限は master と同じ「CompanyDB財務(Amazon)」= amazon-finance.mjs (日曜は --full・ほかは --incremental・--require-backfilled・30 分)
 */
export function financePushStep(businessDate, { coordinator = financeCoordinatorEnabled() } = {}) {
  if (coordinator) return null;
  const financeArgs = amazonFinanceDailyArgs(businessDate);
  return { name: 'CompanyDB財務(Amazon)', cmd: `apps/company-db/push/amazon-finance.mjs ${financeArgs.join(' ')}`, label: `Company DB Amazon 財務 push (${financeArgs[0]})`, timeoutMs: 1800000 };
}
