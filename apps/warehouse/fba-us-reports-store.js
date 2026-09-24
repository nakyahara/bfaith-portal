/**
 * fba-us-reports-store.js — 米国 (NA) の RESTOCK / PLANNING レポートを「取れたままの行」で残す (miniPC)。
 *
 * なぜ fba.db の daily_snapshots_us とは別に持つか (米国FBA納品アプリ 設計方針 §8 H1・Codex R0 2026-09-24):
 *   daily_snapshots_us は RESTOCK と PLANNING を 1 行にまとめ、在庫の列が空なら 0、販売数も PLANNING が空なら 0 で保存している
 *   (以前の値を変えないための作り)。そこからは「0 個」と「レポートに無かった」を見分けられず、元のレポートにも戻せない。
 *   米国は 1 回 15 行ほどと小さいので、レポートの行をそのまま JSON で残し、読む側が列ごとに判断する。
 *
 * 置き場所 = DATA_DIR/fba-us-reports/
 *   - YYYY-MM-DD.json  その business_date に取れたレポート (行つき)。どちらのレポートも取れなかった回は書かない
 *                      同じ日に 2 回目が走ったら、レポートごとに「今回取れた方」を採る (今回失敗したレポートは前の回のまま)
 *   - last-attempt.json 最後の取得の結果 (行は持たない)。失敗した回も書く = 画面が「最新の取得は失敗」を出せる
 * fba.db (sql.js・書き手は常駐サーバ 1 つ) には入れない = ファイル全体の書き戻しの競合 (2026-09-20) の外に置く。
 * 書き込みは一時ファイル → rename (途中で落ちても読みかけの壊れた JSON を残さない)。
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const SCHEMA_VERSION = 1;
const REPORTS = ['restock', 'planning'];
const DATED_FILE = /^(\d{4}-\d{2}-\d{2})\.json$/;

/** 呼ばれた時点の DATA_DIR で決める (試験は import のあとで DATA_DIR を一時フォルダに向ける) */
export function usReportsDir() {
  return path.join(process.env.DATA_DIR || path.join(__dirname, '..', '..', 'data'), 'fba-us-reports');
}

function writeJsonAtomic(file, obj) {
  const tmp = `${file}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(obj));
  fs.renameSync(tmp, file);
}

function readJson(file) {
  return JSON.parse(fs.readFileSync(file, 'utf8'));
}

/** fetchAllReports の結果の 1 レポート分。行が 1 つも無い回は「取れた」にしない (米国は常に十数行ある) */
function reportPart(results, name, fetchError) {
  const rows = results ? results[name] : null;
  const err = ((results && results.errors) || []).find((e) => e && e.report === name);
  const hasRows = Array.isArray(rows) && rows.length > 0;
  let error = null;
  if (err) error = String(err.error ?? 'error').slice(0, 300);
  else if (fetchError) error = String(fetchError).slice(0, 300);
  else if (!hasRows) error = 'no_rows';
  return { ok: hasRows && !err, row_count: Array.isArray(rows) ? rows.length : 0, error, rows: hasRows ? rows : null };
}

/**
 * 1 回の米国取得を残す。
 * @param {object} a
 * @param {string} a.businessDate  'YYYY-MM-DD' (呼ぶ側で検査済み)
 * @param {string} a.attemptedAt   取得を始めた時刻 (ISO)
 * @param {string|null} a.fetchedAt 取り終えた時刻 (ISO)。取得そのものが例外で終わったら null
 * @param {object|null} a.results  fetchAllReports(ctx) の戻り値 ({ restock, planning, errors })
 * @param {string|null} a.error    取得そのものの例外 (results が無いとき)
 * @returns {{ dated: boolean, file: string|null, restock: boolean, planning: boolean }}
 */
export function saveUsReportRun({ businessDate, attemptedAt, fetchedAt = null, results = null, error = null, dir = usReportsDir(), keepDays = 400, now = new Date() }) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(businessDate))) throw new Error(`business_date が不正: ${businessDate}`);
  fs.mkdirSync(dir, { recursive: true });
  const parts = Object.fromEntries(REPORTS.map((n) => [n, reportPart(results, n, error)]));
  const meta = (p) => ({ ok: p.ok, row_count: p.row_count, error: p.error });

  writeJsonAtomic(path.join(dir, 'last-attempt.json'), {
    schema: SCHEMA_VERSION, market: 'us', business_date: businessDate, attempted_at: attemptedAt, fetched_at: fetchedAt,
    error: error ? String(error).slice(0, 300) : null,
    reports: Object.fromEntries(REPORTS.map((n) => [n, meta(parts[n])])),
  });

  if (!REPORTS.some((n) => parts[n].ok)) return { dated: false, file: null, restock: false, planning: false };

  const file = path.join(dir, `${businessDate}.json`);
  let prev = null;
  try { if (fs.existsSync(file)) prev = readJson(file); } catch { prev = null; }   // 壊れていたら今回の分で作り直す
  const reports = {};
  for (const n of REPORTS) {
    const p = parts[n];
    const old = prev && prev.reports && prev.reports[n];
    // 今回取れなかったレポートは、同じ日の前の回に取れていればそちらを残す (時刻も前の回のもの)
    reports[n] = p.ok || !(old && old.ok)
      ? { ...meta(p), fetched_at: p.ok ? fetchedAt : null, rows: p.rows }
      : old;
  }
  writeJsonAtomic(file, { schema: SCHEMA_VERSION, market: 'us', business_date: businessDate, saved_at: now.toISOString(), reports });
  pruneOld(dir, keepDays, now);
  return { dated: true, file, restock: reports.restock.ok, planning: reports.planning.ok };
}

function pruneOld(dir, keepDays, now) {
  const limit = new Date(now.getTime() - keepDays * 86400000).toISOString().slice(0, 10);
  for (const f of fs.readdirSync(dir)) {
    const m = DATED_FILE.exec(f);
    if (m && m[1] < limit) { try { fs.unlinkSync(path.join(dir, f)); } catch { /* 消せなくても次の回に */ } }
  }
}

/**
 * 画面向け: 最後の取得の結果と、いちばん新しい日のレポート (行つき)。
 * 読めない日付ファイルは飛ばして 1 つ前の日を返し、読めなかったことを file_errors に残す。
 */
export function readLatestUsReports({ dir = usReportsDir() } = {}) {
  const out = { schema: SCHEMA_VERSION, last_attempt: null, latest: null, file_errors: [] };
  if (!fs.existsSync(dir)) return out;
  try { out.last_attempt = readJson(path.join(dir, 'last-attempt.json')); }
  catch (e) { if (e.code !== 'ENOENT') out.file_errors.push({ file: 'last-attempt.json', error: String(e.message).slice(0, 200) }); }
  const dated = fs.readdirSync(dir).filter((f) => DATED_FILE.test(f)).sort().reverse();
  for (const f of dated) {
    try { out.latest = readJson(path.join(dir, f)); break; }
    catch (e) { out.file_errors.push({ file: f, error: String(e.message).slice(0, 200) }); }
  }
  return out;
}
