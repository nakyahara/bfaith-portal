/**
 * amazon-finance-months.js — 日次の財務 (f_amazon_finance_sku_daily_v1) を毎朝どの月について作り直すか
 *
 * 🚨 2026-09-28: 5 月の日次の財務が半分欠けていた (5/18〜5/31 の行を含む決済は 6/3 着。当時の daily-sync は当月だけ作り直していた
 *   → 5 月は二度と作り直されなかった)。7/7 からの「毎月 20 日までは前月も」は、月末をまたぐ決済 (翌月 1〜15 日に締まり翌日着) に対して
 *   余裕が 5 日ほどしかなく、レポートの遅れや daily-sync の数日の停止でまた欠ける (✅ のまま気づけない)。
 * → 日付で区切らず、次の 3 つを合わせた月を全部作り直す:
 *   ① 当月
 *   ② 直近 N 日 (既定 35) に決済の行が入った月。決済の行の ingested_at は最初に入った時 (INSERT OR IGNORE) なので、
 *      同じレポートを毎朝取り直しても動かない = 新しい決済が入った月だけが N 日のあいだ対象になる (ふだん 1〜2 か月)
 *   ③ やり残し: 前の回に作り直しか Render への送信が失敗した月 (DATA_DIR/amazon-finance-pending.json)。成功するまで持ち越す
 *      = N 日を過ぎても消えない (Codex #1514 R1)
 *   月を決められない (索引が無い・DB が開けない) ときは 当月 + 前月 + やり残し に戻り、warn を返す (daily-sync で ⚠️ = 全部 OK に数えない)
 */
import fs from 'node:fs';
import path from 'node:path';
import Database from 'better-sqlite3';

export const FINANCE_DIRTY_DAYS = 35;
export const PENDING_FILE = 'amazon-finance-pending.json';

// 入った時刻の索引 (db.js の idx_settle_lines_ingested) で引く。指定しないと SQLite が月と SKU の索引を丸ごと読む (本番 97 秒)。
// 索引が無ければ例外 → planFinanceMonths は 当月 + 前月 に戻る (止めない・warn)
export const DIRTY_MONTHS_SQL = `SELECT DISTINCT year_month_int ym FROM raw_amazon_settlement_lines INDEXED BY idx_settle_lines_ingested WHERE ingested_at >= ? AND year_month_int IS NOT NULL`;

const ymOf = (ymi) => `${String(ymi).slice(0, 4)}-${String(ymi).slice(4, 6)}`;
const isYm = (m) => /^\d{4}-(0[1-9]|1[0-2])$/.test(String(m || ''));
const order = (currentMonth, months) => [currentMonth, ...[...new Set(months)].filter((m) => isYm(m) && m !== currentMonth && m <= currentMonth).sort().reverse()];
const prevMonthOf = (m) => { const d = new Date(Date.UTC(Number(m.slice(0, 4)), Number(m.slice(5, 7)) - 2, 1)); return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`; };

/** 作り直す月 ('YYYY-MM') の一覧 (① + ②)。当月が先頭、ほかは新しい月から。db = better-sqlite3 */
export function financeMonthsToBuild(db, { currentMonth, now = new Date(), days = FINANCE_DIRTY_DAYS } = {}) {
  if (!isYm(currentMonth)) throw new Error(`currentMonth は YYYY-MM: ${currentMonth}`);
  // ingested_at = new Date().toISOString() の 'YYYY-MM-DD HH:MM:SS' (UTC)。同じ書き方で比べる
  const since = new Date(now.getTime() - days * 86400000).toISOString().replace('T', ' ').slice(0, 19);
  const rows = db.prepare(DIRTY_MONTHS_SQL).all(since);
  return order(currentMonth, rows.map((r) => ymOf(r.ym)));
}

/** DATA_DIR の warehouse.db を読み取り専用で開いて決める (① + ②) */
export function pickFinanceMonths(dataDir, opts) {
  const db = new Database(path.join(dataDir, 'warehouse.db'), { readonly: true, fileMustExist: true });
  try {
    db.pragma('busy_timeout = 10000');
    return financeMonthsToBuild(db, opts);
  } finally {
    db.close();
  }
}

/** やり残し (③)。ファイルが無ければ空・壊れていれば空 + 理由 */
export function readPendingMonths(dataDir) {
  const f = path.join(dataDir, PENDING_FILE);
  if (!fs.existsSync(f)) return { months: [], error: null };
  try {
    const j = JSON.parse(fs.readFileSync(f, 'utf8'));
    return { months: (Array.isArray(j.months) ? j.months : []).filter(isYm), error: null };
  } catch (e) {
    return { months: [], error: `やり残しのファイルが読めない (${e.message})` };
  }
}

/**
 * 毎朝の計画: { months, warn, notes }。months = ① + ② + ③ (当月が先頭)。
 * warn = 月を決められず 当月 + 前月 に戻った / やり残しのファイルが読めない (daily-sync で ⚠️)
 */
export function planFinanceMonths(dataDir, { currentMonth, now = new Date(), days = FINANCE_DIRTY_DAYS, pick = pickFinanceMonths } = {}) {
  const notes = [];
  let warn = false;
  const pending = readPendingMonths(dataDir);
  if (pending.error) { warn = true; notes.push(pending.error); }
  let base;
  try {
    base = pick(dataDir, { currentMonth, now, days });
  } catch (e) {
    warn = true;
    base = [currentMonth, prevMonthOf(currentMonth)];
    notes.push(`作り直す月を決められない (${e.message}) → 当月 + 前月`);
  }
  if (pending.months.length) notes.push(`やり残し: ${pending.months.join(', ')}`);
  return { months: order(currentMonth, [...base, ...pending.months]), warn, notes };
}

/** 回の終わりに、作り直しか送信が失敗した月をやり残しとして書く (成功した月は消える)。書けなければ例外 */
export function writePendingMonths(dataDir, failedMonths, { now = new Date() } = {}) {
  const f = path.join(dataDir, PENDING_FILE);
  const months = [...new Set(failedMonths)].filter(isYm).sort();
  fs.writeFileSync(f, JSON.stringify({ months, updated_at: now.toISOString() }, null, 1));
  return months;
}
