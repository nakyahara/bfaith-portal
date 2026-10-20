/**
 * ne-api-usage.js — NE (ネクストエンジン) の API の回数を数える (2026-10-19 中原さん「入れる」)
 *
 * なぜ: NE はアプリを「月の API の回数」で課金する (1〜1,000 回は無料・1,001〜2,000 回は 5,000 円 (税抜))。
 *   今までは回数を数える所が無く、ログ (14 日で消える) の「[NE] トークン保存完了」を数えるしかなかった。
 *
 * 数え方:
 *   - ne-api.js の callNE が NE に 1 回送るたびに 1 行 (= 課金の 1 回)。callNE を通る呼び出しは全部数える
 *     (毎朝の sync・NE へ送る・ne-sync-runner・受注ベースの取り直し (backfill)・手で流した取得)。認証 (/api_neauth) も 1 行
 *   - 置き場所: DATA_DIR/ne-api-usage/<JST の年月>.jsonl (追記だけ・1 行 = 1 回)。月は JST で切る
 *   - 消さない: 1 か月 1,000 行で 100KB 前後 = 13 か月以上の日ごとの回数がそのまま残る (日ごとの合計は読むときに数える)
 *   - 🚨 数えることで NE の取得を止めない・遅くしない: 書けない・読めないは全部ここで飲み込む (警告を 1 回出すだけ)
 *
 * 毎朝のまとめ (daily-sync.js):
 *   - 「NE の API: 今月 312 回 (無料は 1,000 回まで)」を 1 行
 *   - 今月の回数が NE_API_MONTHLY_WARN (既定 700,900,1000) を超えたら ⚠️ の行にし、その数ごとに月 1 回だけ GChat に別の知らせ
 *     (送れた数を ne-api-usage/alerts.json に書く。送れなかった朝は次の朝にまた送る)
 *
 * 見るだけ: node apps/warehouse/ne-api-usage.js [YYYY-MM]   → その月の回数 (日ごと・API ごと・呼んだもの ごと)
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export const NE_FREE_MONTHLY = 1000;
export const DEFAULT_WARN_THRESHOLDS = [700, 900, 1000];
const USAGE_SUBDIR = 'ne-api-usage';
const ALERT_FILE = 'alerts.json';
const JST_MS = 9 * 60 * 60 * 1000;

function defaultDataDir() { return process.env.DATA_DIR || path.join(process.cwd(), 'data'); }
export function usageDir(dataDir = defaultDataDir()) { return path.join(dataDir, USAGE_SUBDIR); }
/** Date → JST の YYYY-MM */
export function jstMonthOf(d) { return new Date(d.getTime() + JST_MS).toISOString().slice(0, 7); }
/** Date → JST の YYYY-MM-DD */
export function jstDateOf(d) { return new Date(d.getTime() + JST_MS).toISOString().slice(0, 10); }
/** YYYY-MM の前の月 */
export function prevMonth(month) {
  const [y, m] = month.split('-').map(Number);
  return m === 1 ? `${y - 1}-12` : `${y}-${String(m - 1).padStart(2, '0')}`;
}
const fmt = (n) => Number(n).toLocaleString('en-US');

/** 呼んだもの (ログで分かる名前)。env NE_API_CALLER があればそれ・無ければ スクリプト名 + 最初の引数 */
function callerName() {
  if (process.env.NE_API_CALLER) return String(process.env.NE_API_CALLER).slice(0, 80);
  const script = process.argv[1] ? path.basename(process.argv[1]) : 'node';
  const arg = process.argv[2] && !/^https?:/i.test(process.argv[2]) ? ` ${process.argv[2]}` : '';   // auth <callback_url> の URL は書かない
  return `${script}${arg}`.slice(0, 80);
}

let warnedWrite = false;
let sessionCount = 0;
const madeDirs = new Set();

/**
 * NE に 1 回送ったことを 1 行書く。🚨 throw しない (書けなければ警告を 1 回だけ出して続ける)。
 * @param {{ endpoint: string, ok: boolean, result?: string, ms?: number }} call
 * @param {{ dataDir?: string, now?: Date }} [opt]
 */
export function recordNeApiCall(call, opt = {}) {
  sessionCount++;
  try {
    const at = opt.now || new Date();
    const dir = usageDir(opt.dataDir || defaultDataDir());
    if (!madeDirs.has(dir)) { fs.mkdirSync(dir, { recursive: true }); madeDirs.add(dir); }
    const line = JSON.stringify({
      t: at.toISOString(),
      ep: String(call.endpoint || '').slice(0, 80),
      ok: call.ok === true,
      r: call.result ? String(call.result).slice(0, 20) : undefined,
      ms: Number.isFinite(call.ms) ? Math.round(call.ms) : undefined,
      job: callerName(),
      pid: process.pid,
    });
    fs.appendFileSync(path.join(dir, `${jstMonthOf(at)}.jsonl`), line + '\n');
  } catch (e) {
    if (!warnedWrite) { warnedWrite = true; console.warn(`[NE] ⚠️ API の回数を書けない (${e.message}) = 取得は続ける`); }
  }
}

/** この process で NE に送った回数 (書けなかった回も数える) */
export function neApiCallsThisProcess() { return sessionCount; }

/**
 * その月 (JST) の回数を読む。ファイルが無い = 0 回。壊れた行は数えず badLines に。読めない = throw (呼ぶ側が飲み込む)
 * @returns {{ month, total, ok, error, byDay: Object<string,number>, byEndpoint: Object<string,number>, byCaller: Object<string,number>, badLines }}
 */
export function readMonthUsage(month, { dataDir = defaultDataDir() } = {}) {
  const out = { month, total: 0, ok: 0, error: 0, byDay: {}, byEndpoint: {}, byCaller: {}, badLines: 0 };
  const file = path.join(usageDir(dataDir), `${month}.jsonl`);
  let text;
  try { text = fs.readFileSync(file, 'utf8'); } catch (e) { if (e.code === 'ENOENT') return out; throw e; }
  for (const raw of text.split('\n')) {
    if (!raw.trim()) continue;
    let r;
    try { r = JSON.parse(raw); } catch { out.badLines++; continue; }
    const at = r && typeof r.t === 'string' ? new Date(r.t) : null;
    if (!at || Number.isNaN(at.getTime()) || jstMonthOf(at) !== month) { out.badLines++; continue; }
    out.total++;
    if (r.ok === true) out.ok++; else out.error++;
    const day = jstDateOf(at);
    out.byDay[day] = (out.byDay[day] || 0) + 1;
    const ep = r.ep || '?';
    out.byEndpoint[ep] = (out.byEndpoint[ep] || 0) + 1;
    const job = r.job || '?';
    out.byCaller[job] = (out.byCaller[job] || 0) + 1;
  }
  return out;
}

/** env NE_API_MONTHLY_WARN (カンマ区切り。例 "700" / "700,900,1000") → 小さい順の数。読めなければ既定 */
export function warnThresholds(env = process.env) {
  const raw = env.NE_API_MONTHLY_WARN;
  if (raw == null || String(raw).trim() === '') return [...DEFAULT_WARN_THRESHOLDS];
  const nums = String(raw).split(',').map((s) => Number(s.trim())).filter((n) => Number.isInteger(n) && n > 0);
  if (!nums.length) { console.warn(`[NE] ⚠️ NE_API_MONTHLY_WARN を読めない (${raw}) → 既定 ${DEFAULT_WARN_THRESHOLDS.join(',')}`); return [...DEFAULT_WARN_THRESHOLDS]; }
  return [...new Set(nums)].sort((a, b) => a - b);
}

/**
 * まとめの 1 行と、今回知らせる数を決める (純粋な関数)
 * @param {{ total: number, thresholds: number[], alreadyNotified: number[], prevMonthTotal?: number|null }} p
 * @returns {{ line: string, warn: boolean, toNotify: number[] }}
 */
export function planUsageNotice({ total, thresholds, alreadyNotified = [], prevMonthTotal = null }) {
  const warn = thresholds.length > 0 && total >= thresholds[0];
  const over = total > NE_FREE_MONTHLY;
  const prev = prevMonthTotal != null ? ` (先月 ${fmt(prevMonthTotal)} 回)` : '';
  const line = over
    ? `⚠️ NE の API: 今月 ${fmt(total)} 回 = 無料の ${fmt(NE_FREE_MONTHLY)} 回を超えた (1,001〜2,000 回は 5,000 円・税抜)${prev}`
    : `${warn ? '⚠️ ' : ''}NE の API: 今月 ${fmt(total)} 回 (無料は ${fmt(NE_FREE_MONTHLY)} 回まで)${prev}`;
  const done = new Set(alreadyNotified);
  const toNotify = thresholds.filter((t) => total >= t && !done.has(t));
  return { line, warn, toNotify };
}

function readAlertState(dataDir) {
  try {
    const j = JSON.parse(fs.readFileSync(path.join(usageDir(dataDir), ALERT_FILE), 'utf8'));
    return j && typeof j === 'object' && !Array.isArray(j) ? j : {};
  } catch { return {}; }
}
/** 送れた数を書く (直近 13 か月分だけ残す)。書きかけを残さない (tmp → rename) */
function writeAlertState(dataDir, state) {
  const keep = Object.keys(state).sort().slice(-13);
  const next = Object.fromEntries(keep.map((k) => [k, state[k]]));
  const dir = usageDir(dataDir);
  fs.mkdirSync(dir, { recursive: true });
  const file = path.join(dir, ALERT_FILE);
  const tmp = `${file}.tmp-${process.pid}`;
  fs.writeFileSync(tmp, JSON.stringify(next, null, 2));
  fs.renameSync(tmp, file);
}

/**
 * 毎朝のまとめ用: 今月の回数の 1 行を返し、数を超えたら その数ごとに月 1 回だけ notify で知らせる。🚨 throw しない (読めない = 読めない旨の 1 行)
 * 先月もまだ知らせていない数があれば同じ知らせに入れる (月末の朝より後に超えた・月末の朝に送れなかった = 翌月 1 日の朝に知らせる)。
 * 1 つの知らせに 今月・先月の数をまとめる。送れた (notify が true) ときだけ印を書く = 送れなかった朝は次の朝にまた送る
 * @param {{ dataDir?: string, now?: Date, env?: object, notify?: (text: string) => Promise<boolean> }} opt
 * @returns {Promise<{ line: string, warn: boolean, total: number|null, notified: number[], notifiedPrev: number[] }>}
 */
export async function neApiUsageForDailySync({ dataDir = defaultDataDir(), now = new Date(), env = process.env, notify = null } = {}) {
  const month = jstMonthOf(now);
  const prev = prevMonth(month);
  let usage;
  try { usage = readMonthUsage(month, { dataDir }); } catch (e) {
    console.warn(`[NE] ⚠️ API の回数を読めない (${e.message})`);
    return { line: `⚠️ NE の API: 今月の回数を読めない (${String(e.message).slice(0, 80)})`, warn: true, total: null, notified: [], notifiedPrev: [] };
  }
  let prevUsage = null;
  try { prevUsage = readMonthUsage(prev, { dataDir }); } catch { /* 先月は見ないだけ */ }
  const thresholds = warnThresholds(env);
  const state = readAlertState(dataDir);
  const doneOf = (m) => (Array.isArray(state[m]) ? state[m] : []);
  const plan = planUsageNotice({ total: usage.total, thresholds, alreadyNotified: doneOf(month),
    prevMonthTotal: jstDateOf(now).endsWith('-01') && prevUsage ? prevUsage.total : null });   // 月の 1 日は先月の最後の回数も添える (請求の回数)
  const prevPlan = prevUsage ? planUsageNotice({ total: prevUsage.total, thresholds, alreadyNotified: doneOf(prev) }) : { toNotify: [] };
  console.log(`[NE] API の回数 ${month}: ${usage.total} 回 (失敗 ${usage.error} 回・読めない行 ${usage.badLines})`);

  const notified = [], notifiedPrev = [];
  if ((plan.toNotify.length || prevPlan.toNotify.length) && typeof notify === 'function') {
    const top = (u) => Object.entries(u.byCaller).sort((a, b) => b[1] - a[1]).slice(0, 3).map(([k, v]) => `${k} ${fmt(v)} 回`).join(' / ');
    const parts = [];
    if (plan.toNotify.length) {
      parts.push(`⚠️ *NE の API の回数が今月 ${fmt(usage.total)} 回になりました* (${month}・知らせる数 ${plan.toNotify.map(fmt).join(' / ')} 回)`);
      if (top(usage)) parts.push(`多いもの: ${top(usage)}`);
    }
    if (prevPlan.toNotify.length) {
      parts.push(`⚠️ *先月 (${prev}) の NE の API は ${fmt(prevUsage.total)} 回でした* (知らせる数 ${prevPlan.toNotify.map(fmt).join(' / ')} 回を超えていた)`);
      if (top(prevUsage)) parts.push(`先月の多いもの: ${top(prevUsage)}`);
    }
    parts.push(`無料は ${fmt(NE_FREE_MONTHLY)} 回まで。1,001〜2,000 回は 5,000 円 (税抜)。`);
    parts.push(`内訳: node apps/warehouse/ne-api-usage.js ${plan.toNotify.length ? month : prev}`);
    let sent = false;
    try { sent = (await notify(parts.join('\n'))) === true; } catch (e) { console.warn(`[NE] ⚠️ 回数の知らせを送れない (${e.message})`); }
    if (sent) {
      notified.push(...plan.toNotify);
      notifiedPrev.push(...prevPlan.toNotify);
      const merge = (m, add) => [...new Set([...doneOf(m), ...add])].sort((a, b) => a - b);
      const next = { ...state };
      if (plan.toNotify.length) next[month] = merge(month, plan.toNotify);
      if (prevPlan.toNotify.length) next[prev] = merge(prev, prevPlan.toNotify);
      try { writeAlertState(dataDir, next); }
      catch (e) { console.warn(`[NE] ⚠️ 知らせた印を書けない (${e.message}) = 次の朝にもう 1 回知らせる`); }
    }
  }
  return { line: plan.line, warn: plan.warn, total: usage.total, notified, notifiedPrev };
}

// ─── 見るだけ: node apps/warehouse/ne-api-usage.js [YYYY-MM] ───
const isDirectRun = process.argv[1] && fileURLToPath(import.meta.url) === path.resolve(process.argv[1]);
if (isDirectRun) {
  const month = /^\d{4}-\d{2}$/.test(process.argv[2] || '') ? process.argv[2] : jstMonthOf(new Date());
  const u = readMonthUsage(month);
  console.log(`NE の API ${month}: ${fmt(u.total)} 回 (成功 ${fmt(u.ok)}・失敗 ${fmt(u.error)}${u.badLines ? `・読めない行 ${u.badLines}` : ''}) / 無料は ${fmt(NE_FREE_MONTHLY)} 回まで`);
  const show = (title, obj, sortByKey) => {
    const rows = Object.entries(obj).sort(sortByKey ? (a, b) => a[0].localeCompare(b[0]) : (a, b) => b[1] - a[1]);
    if (!rows.length) return;
    console.log(`\n${title}`);
    for (const [k, v] of rows) console.log(`  ${k}  ${fmt(v)}`);
  };
  show('日ごと', u.byDay, true);
  show('API ごと', u.byEndpoint);
  show('呼んだもの ごと', u.byCaller);
}
