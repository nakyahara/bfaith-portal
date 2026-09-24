/**
 * fba-us-reports-store.js — 米国 (NA) の RESTOCK / PLANNING レポートを「取れたままの行」で残す (miniPC)。
 *
 * なぜ fba.db の daily_snapshots_us とは別に持つか (米国FBA納品アプリ 設計方針 §8 H1・Codex R0 2026-09-24):
 *   daily_snapshots_us は RESTOCK と PLANNING を 1 行にまとめ、在庫の列が空なら 0、販売数も PLANNING が空なら 0 で保存している
 *   (以前の値を変えないための作り)。そこからは「0 個」と「レポートに無かった」を見分けられず、元のレポートにも戻せない。
 *   米国は 1 回 15 行ほどと小さいので、レポートの行をそのまま JSON で残し、読む側が列ごとに判断する。
 *
 * 置き場所 = DATA_DIR/fba-us-reports/
 *   - YYYY-MM-DD.json  その business_date に取れたレポート (行つき) + その回の取得の結果 (attempt)。どちらのレポートも取れなかった回は書かない
 *                      同じ日に 2 回目が走ったら、レポートごとに「今回取れた方」を採る (今回失敗したレポートは前の回のまま)
 *   - last-attempt.json 最後の取得の結果 (行は持たない)。失敗した回も書く = 画面が「最新の取得は失敗」を出せる
 *   読む側は last-attempt.json と 日付のファイルの attempt の **新しい方** を「最後の取得」にする
 *   (last-attempt.json だけ書けなかった回も、日付のファイルから今回の失敗が分かる。Codex PR1 R2 Medium)
 *   どちらも書けなかった回は、このプロセス (常駐サーバ = 朝の取得も画面への口も同じプロセス) のメモリに残して画面へ返す
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

// 最後の保存の失敗 (このプロセスのメモリ)。保存できた回で消す。ファイルに書けないときの最後の知らせ先
let lastSaveFailure = null;
/** 試験用: メモリの保存失敗を消す */
export function _resetSaveFailure() { lastSaveFailure = null; }

/** 呼ばれた時点の DATA_DIR で決める (試験は import のあとで DATA_DIR を一時フォルダに向ける) */
export function usReportsDir() {
  return path.join(process.env.DATA_DIR || path.join(__dirname, '..', '..', 'data'), 'fba-us-reports');
}

function writeJsonAtomic(file, obj) {
  const tmp = `${file}.${process.pid}.${Date.now()}.tmp`;
  try {
    fs.writeFileSync(tmp, JSON.stringify(obj));
    fs.renameSync(tmp, file);
  } catch (e) {
    try { fs.unlinkSync(tmp); } catch { /* 作れていなければ消す物も無い */ }
    throw e;
  }
}

function readJson(file) {
  return JSON.parse(fs.readFileSync(file, 'utf8'));
}

const isIsoTime = (v) => typeof v === 'string' && /^\d{4}-\d{2}-\d{2}T/.test(v) && Number.isFinite(Date.parse(v));

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
 * 1 回の米国取得を残す。保存できなかったら例外 (朝の処理は警告に落とす) + このプロセスのメモリに残す。
 * @param {object} a
 * @param {string} a.businessDate  'YYYY-MM-DD' (呼ぶ側で検査済み)
 * @param {string} a.attemptedAt   取得を始めた時刻 (ISO)
 * @param {string|null} a.fetchedAt 取り終えた時刻 (ISO)。取得そのものが例外で終わったら null
 * @param {object|null} a.results  fetchAllReports(ctx) の戻り値 ({ restock, planning, errors })
 * @param {string|null} a.error    取得そのものの例外 (results が無いとき)
 * @returns {{ dated: boolean, file: string|null, restock: boolean, planning: boolean }}
 */
export function saveUsReportRun(args) {
  const now = args.now || new Date();
  try {
    const out = saveInner({ ...args, now });
    lastSaveFailure = null;
    return out;
  } catch (e) {
    lastSaveFailure = { at: now.toISOString(), business_date: args.businessDate, attempted_at: args.attemptedAt || null, error: String(e.message).slice(0, 300) };
    throw e;
  }
}

function saveInner({ businessDate, attemptedAt, fetchedAt = null, results = null, error = null, dir = usReportsDir(), keepDays = 400, now }) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(businessDate))) throw new Error(`business_date が不正: ${businessDate}`);
  fs.mkdirSync(dir, { recursive: true });
  const parts = Object.fromEntries(REPORTS.map((n) => [n, reportPart(results, n, error)]));
  const meta = (p) => ({ ok: p.ok, row_count: p.row_count, error: p.error });
  const attempt = {
    schema: SCHEMA_VERSION, market: 'us', business_date: businessDate, attempted_at: attemptedAt, fetched_at: fetchedAt,
    error: error ? String(error).slice(0, 300) : null,
    save_error: null,
    reports: Object.fromEntries(REPORTS.map((n) => [n, meta(parts[n])])),
  };

  // ① 日付のファイル (行 + この回の取得の結果) を先に書く。② そのあと「最後の取得」に、取れたか と 保存できたか を分けて書く
  //   (逆の順だと、日付のファイルが保存できなかった回も「最新の取得は成功」に見え、画面が前の日の分を黙って出す。Codex PR1 R1 Medium 3)
  let out = { dated: false, file: null, restock: false, planning: false };
  let saveError = null;
  if (REPORTS.some((n) => parts[n].ok)) {
    const file = path.join(dir, `${businessDate}.json`);
    try {
      let prev = null;
      try { if (fs.existsSync(file)) { prev = readJson(file); if (validateDated(prev, businessDate)) prev = null; } } catch { prev = null; }   // 壊れていたら今回の分で作り直す
      const reports = {};
      for (const n of REPORTS) {
        const p = parts[n];
        const old = prev && prev.reports[n];
        // 今回取れなかったレポートは、同じ日の前の回に取れていればそちらを残す (時刻も前の回のもの)
        reports[n] = p.ok || !(old && old.ok)
          ? { ...meta(p), fetched_at: p.ok ? fetchedAt : null, rows: p.rows }
          : old;
      }
      writeJsonAtomic(file, { schema: SCHEMA_VERSION, market: 'us', business_date: businessDate, saved_at: now.toISOString(), attempt, reports });
      out = { dated: true, file, restock: reports.restock.ok, planning: reports.planning.ok };
    } catch (e) {
      saveError = String(e.message).slice(0, 300);
    }
  }

  writeJsonAtomic(path.join(dir, 'last-attempt.json'), { ...attempt, save_error: saveError });   // 書けなければ例外 = 呼び出し元がメモリに残す
  if (saveError) throw new Error(`米国のレポートを保存できなかった: ${saveError}`);
  if (out.dated) pruneOld(dir, keepDays, now);
  return out;
}

/**
 * 日付のファイルの形を確かめる。おかしければ理由 (文字列)、正しければ null。
 * JSON として読めても中身が違うもの ({} など) を「最新の日」として採らない (Codex PR1 R1 Low 4 / R2 Low)
 */
export function validateDated(obj, businessDate) {
  if (!obj || typeof obj !== 'object') return '中身がオブジェクトでない';
  if (obj.schema !== SCHEMA_VERSION) return `schema が ${SCHEMA_VERSION} でない (${obj.schema})`;
  if (obj.market !== 'us') return `market が us でない (${obj.market})`;
  if (businessDate && obj.business_date !== businessDate) return `business_date がファイル名と違う (${obj.business_date})`;
  if (!obj.reports || typeof obj.reports !== 'object') return 'reports が無い';
  for (const n of REPORTS) {
    const r = obj.reports[n];
    if (!r || typeof r !== 'object') return `reports.${n} が無い`;
    if (typeof r.ok !== 'boolean') return `reports.${n}.ok が true/false でない`;
    if (r.rows !== null && !Array.isArray(r.rows)) return `reports.${n}.rows が配列でない`;
    if (r.ok && !(Array.isArray(r.rows) && r.rows.length > 0)) return `reports.${n} が ok なのに行が無い`;
    if (Array.isArray(r.rows) && r.rows.some((x) => !x || typeof x !== 'object' || Array.isArray(x))) return `reports.${n}.rows に行でないものがある`;
    if (r.ok && !isIsoTime(r.fetched_at)) return `reports.${n}.fetched_at が時刻でない (${r.fetched_at})`;
  }
  if (obj.attempt != null && validateAttempt(obj.attempt)) return `attempt: ${validateAttempt(obj.attempt)}`;
  return null;
}

/** 「最後の取得」の形。おかしければ理由 */
export function validateAttempt(a) {
  if (!a || typeof a !== 'object') return '中身がオブジェクトでない';
  if (!isIsoTime(a.attempted_at)) return `attempted_at が時刻でない (${a.attempted_at})`;
  if (!a.reports || typeof a.reports !== 'object') return 'reports が無い';
  for (const n of REPORTS) if (!a.reports[n] || typeof a.reports[n].ok !== 'boolean') return `reports.${n}.ok が true/false でない`;
  return null;
}

function pruneOld(dir, keepDays, now) {
  const limit = new Date(now.getTime() - keepDays * 86400000).toISOString().slice(0, 10);
  for (const f of fs.readdirSync(dir)) {
    const m = DATED_FILE.exec(f);
    if (m && m[1] < limit) { try { fs.unlinkSync(path.join(dir, f)); } catch { /* 消せなくても次の回に */ } }
  }
}

/**
 * 画面向け: 最後の取得の結果・いちばん新しい日のレポート (行つき)・このプロセスで起きた保存の失敗。
 * 読めない・形の違う日付ファイルは飛ばして 1 つ前の日を返し、理由を file_errors に残す。
 * 最後の取得 = last-attempt.json と 日付のファイルの attempt の新しい方。
 */
export function readLatestUsReports({ dir = usReportsDir() } = {}) {
  const out = { schema: SCHEMA_VERSION, last_attempt: null, latest: null, file_errors: [], save_failure: lastSaveFailure };
  if (!fs.existsSync(dir)) return out;
  let fileAttempt = null;
  try {
    const a = readJson(path.join(dir, 'last-attempt.json'));
    const bad = validateAttempt(a);
    if (bad) out.file_errors.push({ file: 'last-attempt.json', error: `形がおかしい: ${bad}` });
    else fileAttempt = a;
  } catch (e) { if (e.code !== 'ENOENT') out.file_errors.push({ file: 'last-attempt.json', error: String(e.message).slice(0, 200) }); }
  const dated = fs.readdirSync(dir).filter((f) => DATED_FILE.test(f)).sort().reverse();
  for (const f of dated) {
    try {
      const obj = readJson(path.join(dir, f));
      const bad = validateDated(obj, DATED_FILE.exec(f)[1]);
      if (bad) { out.file_errors.push({ file: f, error: `形がおかしい: ${bad}` }); continue; }
      out.latest = obj;
      break;
    } catch (e) { out.file_errors.push({ file: f, error: String(e.message).slice(0, 200) }); }
  }
  const datedAttempt = out.latest && out.latest.attempt ? out.latest.attempt : null;
  out.last_attempt = [fileAttempt, datedAttempt].filter(Boolean)
    .sort((a, b) => Date.parse(b.attempted_at) - Date.parse(a.attempted_at))[0] || null;
  return out;
}
