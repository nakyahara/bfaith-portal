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
 *   ②' 直近 N 日に売上の行が入った注文の Easy Ship の料金の月 (割り振りが変わる。2026-09-28)
 *   ③ やり残し: 前の回に作り直しか Render への送信が失敗した月 (DATA_DIR/amazon-finance-pending.json)。成功するまで持ち越す
 *      = N 日を過ぎても消えない (Codex #1514 R1)
 *   月を決められない (索引が無い・DB が開けない) ときは 当月 + 前月 + やり残し に戻り、warn を返す (daily-sync で ⚠️ = 全部 OK に数えない)
 *   やり残しのファイルが読めないときは、書く前に amazon-finance-pending.corrupt-<日時>.json に名前を変えて残し (中の月を消さない)、
 *   その corrupt ファイルがあるあいだ毎朝 warn を出し続ける (人が中身を見て月を足すか消す。Codex #1514 R2)
 */
import fs from 'node:fs';
import path from 'node:path';
import Database from 'better-sqlite3';

export const FINANCE_DIRTY_DAYS = 35;
export const PENDING_FILE = 'amazon-finance-pending.json';
// 月の手数料 (f_amazon_account_fees_monthly_v1) のやり残し (2026-09-29 F2b-2)。Company DB との突き合わせで月の手数料に差が出た月を登録し、
//   daily-sync の手数料の build / sync がふだんの 14 か月に加えてその月までさかのぼって作り直す (14 か月より古い月の訂正も直る)。
//   build と sync の両方が通った後にだけ消す (日次の財務のやり残しと同じ読み書き = 下の関数に file を渡す)
export const ACCOUNT_FEES_PENDING_FILE = 'amazon-account-fees-pending.json';
export const ACCOUNT_FEES_BASE_MONTHS = 14;
export const ACCOUNT_FEES_MAX_MONTHS = 60;   // rebuild-amazon-account-fees.js / sync-amazon-account-fees.js の --months の上限

// 入った時刻の索引 (db.js の idx_settle_lines_ingested) で引く。指定しないと SQLite が月と SKU の索引を丸ごと読む (本番 97 秒)。
// 索引が無ければ例外 → planFinanceMonths は 当月 + 前月 に戻る (止めない・warn)
// 🆕 2026-09-28: Easy Ship の料金は同じ注文の売上の行の SKU に割り振る (日次の財務の easy_ship_jpy) =
//   売上の行があとから (別の月の決済で) 届いたら、料金の月の割り振りも変わる → その料金の月も作り直す (Codex #1520 R1)
export const EASY_SHIP_MONTHS_SQL = `SELECT DISTINCT es.year_month_int ym
  FROM (SELECT DISTINCT amazon_order_id FROM raw_amazon_settlement_lines INDEXED BY idx_settle_lines_ingested
         WHERE ingested_at >= ? AND transaction_type = 'Order' AND amazon_order_id IS NOT NULL
           AND seller_sku_normalized IS NOT NULL AND TRIM(seller_sku_normalized) <> '') o
  JOIN raw_amazon_settlement_lines es INDEXED BY idx_settle_lines_order
    ON es.amazon_order_id = o.amazon_order_id AND es.transaction_type = 'Amazon Easy Ship Charges'
 WHERE es.year_month_int IS NOT NULL`;
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
  const es = db.prepare(EASY_SHIP_MONTHS_SQL).all(since);
  return order(currentMonth, [...rows, ...es].map((r) => ymOf(r.ym)));
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

const stemOf = (file) => file.replace(/\.json$/, '');
const isCorruptOf = (file, name) => name.startsWith(`${stemOf(file)}.corrupt-`) && name.endsWith('.json');
/** やり残し (③)。ファイルが無ければ空・壊れていれば空 + 理由。corruptFiles = 前に読めずに名前を変えて残したファイル。file = 月の手数料なら ACCOUNT_FEES_PENDING_FILE */
export function readPendingMonths(dataDir, { file = PENDING_FILE } = {}) {
  const f = path.join(dataDir, file);
  let corruptFiles = [];
  try { corruptFiles = fs.readdirSync(dataDir).filter((x) => isCorruptOf(file, x)).sort(); } catch { corruptFiles = []; }
  if (!fs.existsSync(f)) return { months: [], error: null, corruptFiles };
  try {
    const j = JSON.parse(fs.readFileSync(f, 'utf8'));
    if (!j || !Array.isArray(j.months)) throw new Error('months が配列でない');
    return { months: j.months.filter(isYm), error: null, corruptFiles };
  } catch (e) {
    return { months: [], error: `やり残しのファイルが読めない (${e.message})`, corruptFiles };
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
  if (pending.error) { warn = true; notes.push(`${pending.error} = 中の月は作り直せない (回の終わりに corrupt-<日時>.json に名前を変えて残す)`); }
  if (pending.corruptFiles.length) { warn = true; notes.push(`読めなかったやり残しのファイルが残っている (${pending.corruptFiles.join(', ')}) = 中身を見て月を足すか消す`); }
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

/** 回の終わりに書くやり残し = 今回失敗した月 ∪ (今のファイルにあって今回作り直していない月)。
 *  attempted = 今回作り直した月 (成功した月はここで消える)。朝にだけ読めなかったファイルの月も、作り直していないので残る (Codex #1514 R3)。
 *  今のファイルが読めなければ、上書きせずに corrupt-<日時>.json に名前を変えて残す (中の月を消さない)。書けなければ例外 */
export function writePendingMonths(dataDir, failedMonths, { attempted = [], now = new Date(), file = PENDING_FILE } = {}) {
  const f = path.join(dataDir, file);
  let carry = [];
  if (fs.existsSync(f)) {
    const cur = readPendingMonths(dataDir, { file });
    if (cur.error) fs.renameSync(f, path.join(dataDir, `${stemOf(file)}.corrupt-${now.toISOString().replace(/[:.]/g, '-')}.json`));
    else carry = cur.months.filter((m) => !attempted.includes(m));
  }
  const months = [...new Set([...failedMonths, ...carry])].filter(isYm).sort();
  fs.writeFileSync(f, JSON.stringify({ months, updated_at: now.toISOString() }, null, 1));
  return months;
}

/** やり残しに月を足す (今のファイルの月は残す)。Company DB との突き合わせで差が出た月 (F2b-2)。file = 日次の財務 PENDING_FILE / 月の手数料 ACCOUNT_FEES_PENDING_FILE */
export function addPendingMonths(dataDir, months, { now = new Date(), file = PENDING_FILE } = {}) {
  return writePendingMonths(dataDir, months, { attempted: [], now, file });
}

/** 月の手数料の build / sync の --months (当月から数えた月数)。ふだん 14・手数料のやり残しがあれば一番古い月まで (上限 60)。
 *  戻り値 = { months, pending: [やり残しの月 (当月まで)], warn, notes }。やり残しのファイルが読めなければ 14 のまま warn */
export function accountFeesMonthsBack(dataDir, { currentMonth }) {
  if (!isYm(currentMonth)) throw new Error(`currentMonth は YYYY-MM: ${currentMonth}`);
  const p = readPendingMonths(dataDir, { file: ACCOUNT_FEES_PENDING_FILE });
  const notes = [];
  let warn = false;
  if (p.error) { warn = true; notes.push(`月の手数料の${p.error}`); }
  if (p.corruptFiles.length) { warn = true; notes.push(`読めなかった月の手数料のやり残しのファイルが残っている (${p.corruptFiles.join(', ')}) = 中身を見て月を足すか消す`); }
  const pending = p.months.filter((m) => m <= currentMonth).sort();
  let months = ACCOUNT_FEES_BASE_MONTHS;
  if (pending.length) {
    const [y, mo] = pending[0].split('-').map(Number), [cy, cm] = currentMonth.split('-').map(Number);
    const span = (cy - y) * 12 + (cm - mo) + 1;
    if (span > ACCOUNT_FEES_MAX_MONTHS) { warn = true; notes.push(`月の手数料のやり残し ${pending[0]} は ${ACCOUNT_FEES_MAX_MONTHS} か月より古い = 作り直せない (人が見る)`); }
    months = Math.min(Math.max(months, span), ACCOUNT_FEES_MAX_MONTHS);
    notes.push(`月の手数料のやり残し: ${pending.join(', ')} (${months} か月さかのぼる)`);
  }
  // covered = 今回の build / sync の範囲に入るやり残し (両方が通ったらこれだけを消す。範囲の外 = 60 か月より古い月は残す)
  const [cy, cm] = currentMonth.split('-').map(Number);
  const fromD = new Date(Date.UTC(cy, cm - 1 - (months - 1), 1));
  const fromMonth = `${fromD.getUTCFullYear()}-${String(fromD.getUTCMonth() + 1).padStart(2, '0')}`;
  return { months, fromMonth, pending, covered: pending.filter((m) => m >= fromMonth), warn, notes };
}
