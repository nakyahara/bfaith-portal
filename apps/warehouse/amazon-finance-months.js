/**
 * amazon-finance-months.js — 日次の財務 (f_amazon_finance_sku_daily_v1) を毎朝どの月について作り直すか
 *
 * 🚨 2026-09-28: 5 月の日次の財務が半分欠けていた (5/18〜5/31 の行を含む決済は 6/3 着。当時の daily-sync は当月だけ作り直していた
 *   → 5 月は二度と作り直されなかった)。7/7 からの「毎月 20 日までは前月も」は、月末をまたぐ決済 (翌月 1〜15 日に締まり翌日着) に対して
 *   余裕が 5 日ほどしかなく、レポートの遅れや daily-sync の数日の停止でまた欠ける (✅ のまま気づけない)。
 * → 日付で区切らず「当月 + 直近 N 日 (既定 35) に決済の行が入った月」を全部作り直す。決済の行の ingested_at は最初に入った時 (INSERT OR IGNORE) なので、
 *   同じレポートを毎朝取り直しても動かない = 新しい決済が入った月だけが N 日のあいだ対象になる (ふだん 1〜2 か月)。
 *   作り直しに失敗しても、N 日のあいだは翌朝また対象になる。
 */
import path from 'node:path';
import Database from 'better-sqlite3';

export const FINANCE_DIRTY_DAYS = 35;

// 入った時刻の索引 (db.js の idx_settle_lines_ingested) で引く。指定しないと SQLite が月と SKU の索引を丸ごと読む (本番 97 秒)。
// 索引が無ければ例外 → daily-sync は 当月 + 前月 に戻る (止めない)
export const DIRTY_MONTHS_SQL = `SELECT DISTINCT year_month_int ym FROM raw_amazon_settlement_lines INDEXED BY idx_settle_lines_ingested WHERE ingested_at >= ? AND year_month_int IS NOT NULL`;

const ymOf = (ymi) => `${String(ymi).slice(0, 4)}-${String(ymi).slice(4, 6)}`;

/** 作り直す月 ('YYYY-MM') の一覧。当月が先頭、ほかは新しい月から。db = better-sqlite3 */
export function financeMonthsToBuild(db, { currentMonth, now = new Date(), days = FINANCE_DIRTY_DAYS } = {}) {
  if (!/^\d{4}-\d{2}$/.test(String(currentMonth || ''))) throw new Error(`currentMonth は YYYY-MM: ${currentMonth}`);
  // ingested_at = new Date().toISOString() の 'YYYY-MM-DD HH:MM:SS' (UTC)。同じ書き方で比べる
  const since = new Date(now.getTime() - days * 86400000).toISOString().replace('T', ' ').slice(0, 19);
  const rows = db.prepare(DIRTY_MONTHS_SQL).all(since);
  const others = [...new Set(rows.map((r) => ymOf(r.ym)))].filter((m) => m !== currentMonth && m <= currentMonth).sort().reverse();
  return [currentMonth, ...others];
}

/** DATA_DIR の warehouse.db を読み取り専用で開いて決める */
export function pickFinanceMonths(dataDir, opts) {
  const db = new Database(path.join(dataDir, 'warehouse.db'), { readonly: true, fileMustExist: true });
  try {
    db.pragma('busy_timeout = 10000');
    return financeMonthsToBuild(db, opts);
  } finally {
    db.close();
  }
}
