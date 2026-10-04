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
 *    🚨 証拠はローカル (財務の台帳・warehouse.db) **と Render** (決済のそろいの行 = coordinator が一度でも updating を送った) の両方 (#1567 Codex R7 High 1):
 *    ローカルの DB を失くした・切り替えの前のバックアップに戻した・新しい DATA_DIR を指した、でも Render に行があれば ❌。
 *    Render を読めない (網の失敗・404・401・形が違う) = 判定できない = ❌ (fail-closed)。両方とも「無い」と確かに分かったときだけ今までの 2 工程を許す
 *    (= 切り替えの前の朝に Render が落ちていると今までの取込も止まる = 可用性の代わりに正しさ)
 * 🚨 足すのはハードゲート (夜に手で実の --full を 1 回: exit 0・60 分以内・最大メモリ 1,200 MB 以下) に合格した直後・同じ保守の枠の中・中原さんの指示の後。
 *    .env は miniPC のリポジトリの直下の 1 つだけ (書き直して他の設定を消さない = 2026-09-30 の件)。手順 = db/company/README.md
 * 一時物 = 台帳 config/jobs-registry.mjs の cdb-finance-coordinator-switch (remove_by 2026-11-30 = スイッチを消して常に coordinator にする)
 */
import fs from 'node:fs';
import path from 'node:path';
import Database from 'better-sqlite3';
import { amazonFinanceDailyArgs } from './amazon-finance-months.js';
import { baseOrigin } from '../../scripts/company-db/remote-load.mjs';

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

/** 一方向のスイッチを破ろうとした (coordinator で回ったことがある = ローカルか Render に coverage の世代がある) */
export class FinanceSwitchedBackError extends Error {
  constructor(what, where = 'ローカルの台帳・warehouse.db') {
    super(`🚨 coordinator に切り替え済み (${where} に coverage の世代がある) = ${what}をしない`
      + ` (今までの 2 工程・単独の送信は生の表を書く前・送る前に Render を updating にしない = 古い complete が残りうる。`
      + `.env に ${FINANCE_COORDINATOR_ENV}=1 をまだ足していない・消えた疑い = .env を確かめて足す (db/company/README.md の手順)。勝手に coordinator を起動しない)`);
    this.code = 'FINANCE_SWITCHED_BACK';
  }
}
/** 切り替え済みかを判定できない (Render の決済のそろいを読めない) = 今までの 2 工程を止める (fail-closed・#1567 Codex R7 High 1) */
export class FinanceSwitchUnknownError extends Error {
  constructor(what, why) {
    super(`🚨 coordinator に切り替え済みかを判定できない (Render の決済のそろいを読めない: ${why}) = ${what}をしない (fail-closed)。`
      + `Render が戻れば次の回で動く。切り替えの前の朝に Render が落ちていると今までの取込も止まる (可用性の代わりに正しさ)`);
    this.code = 'FINANCE_SWITCH_UNKNOWN';
  }
}
/** ローカルだけの門 (同期)。coordinator で回ったことがあれば throw (#1567 Codex R6 High)。今までの入口は Render も見る assertLegacyAllowedRemote を使う */
export function assertLegacyAllowed(db, ledger, what) {
  if (coverageEverRan(db, ledger)) throw new FinanceSwitchedBackError(what);
}

// ── Render の証拠 (#1567 Codex R7 High 1) ──
/** Render の決済のそろいの状態の口 (PR #1561 の受け口・読むだけ) = amazon-finance-coverage-run.js の STATUS_PATH と同じ (試験で縛る) */
export const COVERAGE_STATUS_PATH = '/order-finance/coverage/status?mall=amazon&scope=jp&source=amazon_settlement_unified';
const defaultSleep = (ms) => new Promise((r) => setTimeout(r, ms));
/** Render の送り先 (https の origin + /apps/company-db/sync) = push/ne-shipments.mjs の syncBase と同じ規則。無ければ '' */
export const renderSyncBase = (env = process.env) => { const o = baseOrigin(env); return o ? `${o}/apps/company-db/sync` : ''; };
/**
 * Render の決済のそろいに coverage の行 (世代) があるか = coordinator が一度でも updating を送った (行は updating の POST でだけ作られる・状態は問わない)。
 *   戻り = true (行がある = 切り替え済み) / false (200 で coverage が null = 確かに無い)。
 *   読めない (網の失敗・時間切れ・404・401・409 not_migrated・5xx・JSON でない・形が違う・送り先か鍵が無い) = throw (判定できない = 呼び手は ❌)
 *   網の失敗・408・429・5xx だけ間を空けて読み直す (attempts 回まで)
 */
export async function renderCoverageEverRan({ fetchImpl = fetch, env = process.env, base = null, syncKey = null, attempts = 3, timeoutMs = 30000, sleep = defaultSleep } = {}) {
  const b = base ?? renderSyncBase(env);
  const key = syncKey ?? (env.MIRROR_SYNC_KEY || '');
  if (!b || !key) throw new Error('Render の送り先 (RENDER_MIRROR_URL / RENDER_PORTAL_URL・https) か MIRROR_SYNC_KEY が無い');
  let last = null;
  for (let i = 1; i <= attempts; i++) {
    let retry = true;
    try {
      const res = await fetchImpl(`${b}${COVERAGE_STATUS_PATH}`, { headers: { 'x-sync-key': key }, signal: AbortSignal.timeout(timeoutMs) });
      const text = await res.text();
      if (res.ok) {
        retry = false;
        let j; try { j = JSON.parse(text); } catch { throw new Error('応答が JSON でない'); }
        if (!j || typeof j !== 'object' || !Object.hasOwn(j, 'coverage')) throw new Error('応答の形が違う (coverage が無い)');
        if (j.coverage === null) return false;
        if (typeof j.coverage === 'object' && j.coverage.generation != null) return true;
        throw new Error('応答の形が違う (coverage に世代が無い)');
      }
      retry = res.status === 408 || res.status === 429 || res.status >= 500;
      last = new Error(`HTTP ${res.status} ${text.replace(/\s+/g, ' ').slice(0, 120)}`);
    } catch (e) { last = e; }
    if (!retry || i >= attempts) break;
    await sleep(2000 * i);
  }
  throw new Error(String(last && last.message || last));
}
/**
 * 今までの 2 工程・単独の送信を始める前の門 (ローカル + Render・#1567 Codex R6 High / R7 High 1・2)。
 *   ローカルに証拠 / Render に行 = FinanceSwitchedBackError・Render を読めない = FinanceSwitchUnknownError。両方とも確かに無いときだけ通る
 *   remote = { fetchImpl, env, base, syncKey, sleep } (renderCoverageEverRan に渡す)
 */
export async function assertLegacyAllowedRemote(db, ledger, what, remote = {}) {
  assertLegacyAllowed(db, ledger, what);
  let has;
  try { has = await renderCoverageEverRan(remote); } catch (e) { throw new FinanceSwitchUnknownError(what, e.message); }
  if (has) throw new FinanceSwitchedBackError(what, 'Render の決済のそろい');
}
/**
 * daily-sync・retry 用の門 (DB を読むだけで開く)。戻り = { allowed, code, message }
 *   ローカルの DB が読めない (無い・壊れた) は「ローカルに証拠が無い」とは言えない = Render だけで決める (Render に行が無いと確かに分かれば通す。
 *   入口 (fetch / 送り手) は自分の DB で同じ門をもう一度通る)
 */
export async function legacyGateCheck({ dataDir, ...remote } = {}) {
  const local = coverageEverRanAt(dataDir);
  if (local === true) return { allowed: false, code: 'FINANCE_SWITCHED_BACK', message: new FinanceSwitchedBackError('今までの 2 工程').message };
  try {
    if (await renderCoverageEverRan(remote)) return { allowed: false, code: 'FINANCE_SWITCHED_BACK', message: new FinanceSwitchedBackError('今までの 2 工程', 'Render の決済のそろい').message };
  } catch (e) { return { allowed: false, code: 'FINANCE_SWITCH_UNKNOWN', message: new FinanceSwitchUnknownError('今までの 2 工程', e.message).message }; }
  return { allowed: true, code: null, message: null, localUnknown: local === null };
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
