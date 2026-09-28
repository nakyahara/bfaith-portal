/**
 * lz-daily.mjs — ロジザードの毎日の商品マスタを Company DB の値で作り、NE の取得の値から作ったもの (GAS と同じ変換) と突き合わせる
 *   (マスタ正本切替 ③c-1a = 影運転の続き。**まだロジザードに取り込まない**。設計 = AI_reference CompanyDB構想/10 §6.3「③c 契約 v1〜v3」)
 *
 * 使い方 (miniPC・daily-sync の 1 ステップ。マスタ照合の後・見張りの前):
 *   node scripts/company-db/lz-daily.mjs --daily [--data-dir D] [--as-of YYYY-MM-DD] [--lz-master <shohin_master.csv>] [--lz-stamp <shohin-last-success.txt>] [--out-dir D]
 *   --out-dir = 出す場所 (CSV・報告・証跡)。既定 = DATA_DIR。手で試すときは別の場所にする (本番の証跡を書かない・ping も打たない)
 * env: DATA_DIR / COMPANY_DB_WATCH_URL (watcher = 読むだけ) / LZ_SHOHIN_MASTER_PATH (既定 C:\tools\logizard-automation\out\shohin_master.csv)
 *   / LZ_SHOHIN_STAMP_PATH (既定 C:\tools\logizard-automation\logs\shohin-last-success.txt) / JOBS_MONITOR_TOKEN・JOBS_MONITOR_URL (ping)
 *
 * 材料の条件 (v2 H2・H4・v3 H3・M6)。1 つでも欠ける = 作らない (⏭️ 理由つき):
 *   - その朝の照合の証跡が complete (DATA_DIR/company-db-evidence/<今日>/master-compare.json)・全件 JSON の sha256 が合う
 *   - Company DB の元のコードの印がその照合の回・NE の取得の世代がその朝 (JST)
 *   - ロジザードの全件の一覧 = auto-shohin-csv.js のその日の**成功した**書き出し:
 *       成功の印 (logs/shohin-last-success.txt = 保存 → 転送 → 印の順に書かれる) の中身がその日・印が一覧の後 15 分以内に書かれた (同じ回)・
 *       読むあいだに変わらない・壊れていない・商品ID が空の行が無い・行数の下限以上・前回 (7 日以内の完了の印) の半分以上
 * 出すもの (v3 H3 = 作ったものと取り込むものが同じであることの受け渡し):
 *   - DATA_DIR/lz-daily/<今日>/<実行 ID>/cdb_logizard_shohinmaster_upload.csv (変えない = 新しく作るだけ) と report.json
 *   - 証跡 lz-daily (完了の印) = 入力の世代・CSV の sha256・行数・取込の期限・合否とその理由・3 つの分け方の件数
 *   - 🚨 始めに running を書く = 同じ日の前の回の完了の印を無効にする。証跡を書けない = 作ること自体の失敗 (❌。前の合格を残さない)
 * 合否 (v2 H6・v3 M5): 説明できない差 0・判定できない 0 (NE の道の推測の形も)・形の差 0・不正で出さない 0・作れない行 0・比べる商品が 1 つ以上
 * 終わり方 (v2 M9・v3 M6。材料が無いを成功にしない):
 *   作れた (合格でも不合格でも) = exit 0 (✅ / ⚠️) + 成功の ping (台帳 lz-daily-build)
 *   材料が無い・未設定 = ⏭️ exit 3 (daily-sync と朝の再試行では失敗 = retry に載る) + fail の ping (理由つき)
 *   作ること自体の失敗 = ❌ exit 1 + fail の ping
 *   ping は --daily で --out-dir が無い回 (daily-sync の回) だけ
 */
import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { jstDateStr } from '../../lib/jst-date.js';
import { writeEvidence, readEvidence } from '../../apps/company-db/push/evidence.mjs';
import { readCdbMaster } from '../../apps/company-db/master-compare/compare-load.mjs';
import { jstDateOfUtcText } from '../../apps/company-db/master-compare/compare-ne.mjs';
import { connectWatcher } from '../../apps/company-db/master-compare/run.mjs';
import { readNeForLz, joinLzSnapshot } from '../../apps/master-decisions/lz-snapshot.mjs';
import { buildLzCsv, DAILY, LZ_CONVERTER_VERSION } from '../../apps/master-decisions/lz-csv.mjs';
import { compareLz, LZ_COMPARE_VERSION } from '../../apps/master-decisions/lz-compare.mjs';
import { readLzShohinMaster, classifyForLz, compareNeIndex, explainCdbDiffs } from '../../apps/master-decisions/lz-cdb.mjs';

export const EVIDENCE_NAME = 'lz-daily';
export const OUT_DIR = 'lz-daily';
export const JOB_ID = 'lz-daily-build';   // 台帳 (config/jobs-registry.mjs)
export const DEFAULT_LZ_MASTER = 'C:\\tools\\logizard-automation\\out\\shohin_master.csv';
export const DEFAULT_LZ_STAMP = 'C:\\tools\\logizard-automation\\logs\\shohin-last-success.txt';
export const STAMP_MAX_LAG_MS = 15 * 60 * 1000;   // 保存 → Drive への転送 (最大 約 7 分) → 印。2026-09-28 の実測は 45 秒
export const PREV_LOOKBACK_DAYS = 7;
export const EXIT = Object.freeze({ complete: 0, error: 1, skipped: 3 });   // 2 は使わない (朝の再試行が「通知済み・打ち切り」と読む)
export const LZ_DAILY_VERSION = 'lzd-v2';
const sha256 = (buf) => crypto.createHash('sha256').update(buf).digest('hex');
export const makeRunId = (now = new Date()) => `lzd_${now.toISOString().replace(/[-:.]/g, '')}_${crypto.randomBytes(3).toString('hex')}`;

/** Company DB を 1 つの読み取りの取引で読む (元のコードと値を同じ時点で。v2 H5) */
export async function readCdbForLz(db) {
  await db.query('begin transaction isolation level repeatable read read only');
  try {
    const cdb = await readCdbMaster(db);
    const mark = (await db.query(`select compare_run_id from ops.master_ne_code_mark where id = 1`)).rows[0] ?? null;
    const codes = (await db.query(`select code_norm, state, ne_code from ops.master_ne_codes where kind = 'product'`)).rows;
    await db.query('commit');
    return { cdb, mark, codes };
  } catch (e) { try { await db.query('rollback'); } catch { /* */ } throw e; }
}

/** 前の日 (7 日以内) の完了の印にあるロジザードの一覧の行数 (半減の見張り。v2 H2)。無ければ null */
export function previousLzRows(dir, asOf, days = PREV_LOOKBACK_DAYS) {
  const base = Date.parse(`${asOf}T12:00:00+09:00`);
  for (let i = 1; i <= days; i++) {
    const d = jstDateStr(new Date(base - i * 86400000));
    const e = readEvidence(dir, d)[EVIDENCE_NAME];
    const rows = e && e.state === 'complete' && e.inputs && e.inputs.lz_master ? e.inputs.lz_master.rows : null;
    if (Number.isSafeInteger(rows)) return { as_of: d, rows, run_id: e.run_id ?? null };
  }
  return null;
}

/**
 * 1 回分。戻り値 = { state: 'complete' | 'skipped', reason?, evidence, line, runId }。証跡を書けない = throw
 * @param {object} p
 * @param {() => Promise<{ db, close }>} p.connect  Company DB (watcher)
 */
export async function runLzDaily({ dataDir, outDir = dataDir, asOf, lzMasterPath, lzStampPath = DEFAULT_LZ_STAMP, connect, now = new Date(), readNe = readNeForLz,
  write = (d, n, p) => writeEvidence(d, n, p, { now }), lzMinRows = undefined,
  readCdb = async () => { const c = await connect(); try { return await readCdbForLz(c.db); } finally { await c.close(); } } }) {
  const runId = makeRunId(now);
  const save = (payload) => { if (!write(outDir, EVIDENCE_NAME, { as_of: asOf, run_id: runId, version: LZ_DAILY_VERSION, ...payload })) throw new Error('証跡 lz-daily を書けない'); };
  // 始めに「実行中」= 同じ日の前の回の完了の印を無効にする (途中で失敗しても、前の合格が今の完了の印として残らない。Codex #1507 R1 High)
  save({ state: 'running', started_at: now.toISOString() });
  const skip = (reason, detail = {}) => {
    const evidence = { state: 'skipped', reason, ...detail };
    save(evidence);
    return { state: 'skipped', reason, evidence, runId, line: `⏭️ ロジザード毎日の商品マスタ (影): 作らない (${reason})` };
  };
  // ── 1. その朝の照合 ──
  const ev = readEvidence(dataDir, asOf)['master-compare'];
  if (!ev || ev.state !== 'complete' || ev.as_of !== asOf || !ev.compare_run_id || !ev.json_path) return skip('compare_not_complete', { compare_state: ev ? ev.state ?? null : null });
  let compareJson;
  try {
    const buf = fs.readFileSync(path.join(dataDir, ev.json_path));
    if (sha256(buf) !== ev.sha256) return skip('compare_json_mismatch');
    compareJson = JSON.parse(buf.toString('utf8'));
  } catch { return skip('compare_json_unreadable'); }
  // ── 2. NE の取得 (その朝の世代) ──
  const ne = readNe(dataDir);
  if (!ne.ok) return skip(`ne_${ne.reason}`);
  if (jstDateOfUtcText(ne.marks.products.at) !== asOf || jstDateOfUtcText(ne.marks.sets.at) !== asOf) return skip('ne_not_today', { ne_products_at: ne.marks.products.at });
  // ── 3. ロジザードの全件の一覧 (その日の auto-shohin-csv.js の成功した書き出し) ──
  let lzBuf, lzInfo;
  try {
    const st0 = fs.statSync(lzMasterPath);
    lzBuf = fs.readFileSync(lzMasterPath);
    const st = fs.statSync(lzMasterPath);
    lzInfo = { path: lzMasterPath, mtime: st.mtime.toISOString(), bytes: lzBuf.length, sha256: sha256(lzBuf) };
    if (st0.mtimeMs !== st.mtimeMs || st.size !== lzBuf.length) return skip('lz_master_changing', { lz_master: lzInfo });   // 読むあいだに書き換わった
  } catch { return skip('lz_master_missing', { lz_master: lzMasterPath }); }
  if (jstDateStr(new Date(lzInfo.mtime)) !== asOf) return skip('lz_master_not_today', { lz_master: lzInfo });
  // 成功の印: auto-shohin-csv.js は 保存 → 転送 → 印 (その日の日付) の順に書く。印がその日で、一覧の後 15 分以内 = その回が成功した書き出し。
  // 日付だけで見ると、失敗した回 (保存の後の転送で落ちた) や別の書き出し・写しを採用してしまう (Codex #1507 R1 Medium)
  let stamp;
  try { const st = fs.statSync(lzStampPath); stamp = { path: lzStampPath, text: fs.readFileSync(lzStampPath, 'utf8').trim().slice(0, 40), mtime: st.mtime.toISOString() }; }
  catch { return skip('lz_stamp_missing', { lz_master: lzInfo, lz_stamp: lzStampPath }); }
  lzInfo.stamp = stamp;
  const lag = Date.parse(stamp.mtime) - Date.parse(lzInfo.mtime);
  if (stamp.text !== asOf || lag < 0 || lag > STAMP_MAX_LAG_MS) return skip('lz_export_not_confirmed', { lz_master: lzInfo });
  const lz = readLzShohinMaster(lzBuf, lzMinRows ? { minRows: lzMinRows } : {});
  if (!lz.ok) return skip(lz.reason, { lz_master: { ...lzInfo, rows: lz.rows } });
  lzInfo.rows = lz.rows;
  // 前回の半分より少ない = 抽出の事故の疑い (v2 H2)。前回 = 7 日以内の完了の印 (無ければ下限の行数だけ)。
  // 前回は本番の履歴 (dataDir) から読む = 出す場所を分けた手の試しでも同じ検査になる (Codex #1507 R2 Medium)
  lzInfo.prev = previousLzRows(dataDir, asOf);
  if (lzInfo.prev && lz.rows * 2 < lzInfo.prev.rows) return skip('lz_master_shrunk', { lz_master: lzInfo });
  // ── 4. Company DB (元のコードと値を 1 つの読み取りの取引で) ──
  const cdbRead = await readCdb();
  if (!cdbRead.mark || cdbRead.mark.compare_run_id !== ev.compare_run_id) return skip('codes_not_this_run', { code_mark: cdbRead.mark, compare_run_id: ev.compare_run_id });
  // ── 5. 分けて作って比べる ──
  const snap = joinLzSnapshot(ne, { mark: cdbRead.mark, rows: cdbRead.codes }, { takenAt: now.toISOString() });
  const cls = classifyForLz({ neItems: snap.items, cdb: cdbRead.cdb, lz });
  const cdbCsv = buildLzCsv(cls.compare.map((x) => x.cdb), 'daily');
  const neCsv = buildLzCsv(cls.compare.map((x) => x.ne), 'daily');
  const raw = compareLz({ gas: neCsv.bytes, ours: cdbCsv, compareCols: [0, 1, 2, 3, 4], header: DAILY.header });
  // NE の道の推測の形 (neCsv.rows の unverified) も判定に入れる (Codex #1507 R1 High)
  const result = explainCdbDiffs(raw, { compareIndex: compareNeIndex(compareJson), byKey: new Map(cls.compare.map((x) => [x.key, x])), neRows: neCsv.rows });
  const failBy = [
    ['shape', result.shape.length], ['undeterminable', result.undeterminable.length], ['unexplained', result.unexplained.length],
    ['invalid', cls.invalid.length],   // 不正で出さない商品が残る = 合格にしない (v3 M5)
    ['unmade', cdbCsv.unmade.length + neCsv.unmade.length],
    ['no_compare', cls.compare.length ? 0 : 1],   // 比べた商品が無い = 何も確かめていない
  ].filter(([, n]) => n > 0).map(([k]) => k);
  const verdict = failBy.length ? 'fail' : 'pass';
  // ── 6. 出す (変えない CSV + 報告 + 完了の印) ──
  const rel = path.join(OUT_DIR, asOf, runId);
  const dir = path.join(outDir, rel);
  fs.mkdirSync(dir, { recursive: true });
  const csvRel = path.join(rel, `cdb_${DAILY.file}`).replace(/\\/g, '/');
  fs.writeFileSync(path.join(outDir, csvRel), cdbCsv.bytes, { flag: 'wx' });
  const report = { run_id: runId, as_of: asOf, verdict, fail_by: failBy, classes: { counts: cls.counts, awaiting: cls.awaiting, invalid: cls.invalid, cost_zero_over_lz: cls.cost_zero_over_lz }, compare: result,
    build: { counts: cdbCsv.counts, unmade: cdbCsv.unmade, subs: cdbCsv.rows.flatMap((r) => r.subs.map((x) => ({ code: r.key, ...x }))) } };
  const reportBuf = Buffer.from(JSON.stringify(report, null, 1), 'utf8');
  fs.writeFileSync(path.join(dir, 'report.json'), reportBuf, { flag: 'wx' });
  const evidence = {
    state: 'complete', verdict, fail_by: failBy,
    versions: { converter: LZ_CONVERTER_VERSION, compare: LZ_COMPARE_VERSION },
    inputs: { compare_run_id: ev.compare_run_id, compare_json_sha256: ev.sha256, ne_marks: ne.marks, code_mark: cdbRead.mark, lz_master: lzInfo },
    csv: { path: csvRel, sha256: sha256(cdbCsv.bytes), rows: cdbCsv.counts.made, bytes: cdbCsv.bytes.length },
    report: { path: path.join(rel, 'report.json').replace(/\\/g, '/'), sha256: sha256(reportBuf) },
    deadline: `${asOf}T23:59:59+09:00`,   // 取込 (③c-1b) はこの期限の内・この CSV の sha256 と行数が合うときだけ
    counts: cls.counts,
    summary: { ...result.summary, same_rows: result.counts.same_rows, ne_rows: result.counts.gas_rows },
    allowed_by: result.allowed.reduce((m, a) => ((m[a.why || a.what] = (m[a.why || a.what] || 0) + 1), m), {}),
  };
  save(evidence);
  const s = evidence.summary;
  const line = `${verdict === 'pass' ? '✅' : '⚠️'} ロジザード毎日の商品マスタ (影): ${verdict === 'pass' ? '合格' : `不合格 (${failBy.join('・')})`}`
    + ` / 比べる ${cls.counts.compare}・新商品待ち ${cls.counts.awaiting}・不正 ${cls.counts.invalid}`
    + ` / 同じ ${s.same_rows}・許す差 ${s.allowed}・説明できない ${s.unexplained}・判定できない ${s.undeterminable}・形の差 ${s.shape}`;
  return { state: 'complete', evidence: { as_of: asOf, run_id: runId, version: LZ_DAILY_VERSION, ...evidence }, line, runId, report };
}

/**
 * runLzDaily を呼ばずに作らない回 (未設定) も、同じ日の前の回の完了の印を無効にする (前の合格を今の印として残さない。Codex #1507 R2 High)。
 * 書けない = throw (作ること自体の失敗)
 */
export function markSkipped({ outDir, asOf, reason, now = new Date(), write = (d, n, p) => writeEvidence(d, n, p, { now }) }) {
  const runId = makeRunId(now);
  if (!write(outDir, EVIDENCE_NAME, { as_of: asOf, run_id: runId, version: LZ_DAILY_VERSION, state: 'skipped', reason })) throw new Error('証跡 lz-daily を書けない');
  return runId;
}

/** 終わり方 → 監視への報告 (作れた回だけ ok。材料が無い・失敗は fail = ok が進まない = 締切で気づく) */
export function pingFor(code, line) {
  return { status: code === EXIT.complete ? 'ok' : 'fail', note: String(line || '').replace(/\s+/g, ' ').slice(0, 180) };
}

/**
 * 監視へ報告する (scripts/jobs-monitor/ping.ps1 と同じ口・同じ形。受け口はクエリの status だけを見る)。
 * 🚨 報告の失敗でステップを失敗にしない (戻り値 = 受け付けられたか・警告は stderr = daily-sync が読む最後の行を変えない)
 */
export async function sendPing(jobId, { status, note }, { env = process.env, fetchImpl = fetch, warn = console.warn } = {}) {
  const token = String(env.JOBS_MONITOR_TOKEN || '').trim();
  let u;
  try { u = new URL(String(env.JOBS_MONITOR_URL || 'https://bfaith-portal.onrender.com').trim()); } catch { return false; }
  if (!token || u.protocol !== 'https:') return false;   // Bearer を載せるので https だけ
  const q = new URLSearchParams({ status });
  if (note) q.set('note', note);
  try {
    const res = await fetchImpl(`${u.origin}/apps/jobs-monitor/ping/${encodeURIComponent(jobId)}?${q}`,
      { method: 'POST', headers: { Authorization: `Bearer ${token}` }, signal: AbortSignal.timeout(15000) });
    if (!res.ok) { warn(`[lz-daily] ping が受け付けられなかった: HTTP ${res.status}`); return false; }
    return true;
  } catch (e) { warn(`[lz-daily] ping 失敗: ${String(e && e.message).slice(0, 160)}`); return false; }
}

export function parseArgs(argv) {
  const out = { dataDir: null, asOf: null, lzMaster: null, lzStamp: null, outDir: null, daily: false };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--data-dir') out.dataDir = argv[++i];
    else if (a === '--as-of') out.asOf = argv[++i];
    else if (a === '--lz-master') out.lzMaster = argv[++i];
    else if (a === '--lz-stamp') out.lzStamp = argv[++i];
    else if (a === '--out-dir') out.outDir = argv[++i];
    else if (a === '--daily' || a === '7') out.daily = true;   // daily-sync・朝の再試行の印 (ping を打つ。'7' = 引数が無いときに daily-sync が足す)
    else throw new Error(`知らない引数: ${a}`);
  }
  if (out.asOf && !/^\d{4}-\d{2}-\d{2}$/.test(out.asOf)) throw new Error('--as-of は YYYY-MM-DD');
  return out;
}

const fold = (x) => (process.platform === 'win32' ? x.toLowerCase() : x);
const isMain = (() => { try { return !!process.argv[1] && fold(fs.realpathSync.native(process.argv[1])) === fold(fs.realpathSync.native(fileURLToPath(import.meta.url))); } catch { return false; } })();
if (isMain) {
  let code = EXIT.error, last = '', ping = false;
  try {
    const a = parseArgs(process.argv.slice(2));
    ping = a.daily && !a.outDir;   // 手の試し (--out-dir) は報告しない
    const dataDir = (a.dataDir || process.env.DATA_DIR || '').trim();
    if (!dataDir) throw new Error('DATA_DIR が無い (--data-dir でも可)');
    const asOf = a.asOf || jstDateStr(new Date());
    const outDir = (a.outDir || dataDir).trim();
    const url = (process.env.COMPANY_DB_WATCH_URL || '').trim();
    if (!url) {
      markSkipped({ outDir, asOf, reason: 'not_configured' });
      last = '⏭️ ロジザード毎日の商品マスタ (影): 作らない (未設定 COMPANY_DB_WATCH_URL)'; code = EXIT.skipped;
    } else {
      const lzMasterPath = (a.lzMaster || process.env.LZ_SHOHIN_MASTER_PATH || DEFAULT_LZ_MASTER).trim();
      const lzStampPath = (a.lzStamp || process.env.LZ_SHOHIN_STAMP_PATH || DEFAULT_LZ_STAMP).trim();
      const r = await runLzDaily({ dataDir, outDir, asOf, lzMasterPath, lzStampPath, connect: () => connectWatcher(url) });
      last = r.line; code = r.state === 'complete' ? EXIT.complete : EXIT.skipped;
    }
  } catch (e) {
    last = `❌ ロジザード毎日の商品マスタ (影): ${String(e && e.message).replace(/\s+/g, ' ').slice(0, 400)}`;
    code = EXIT.error;
  }
  console.log(String(last).replace(/\s+/g, ' '));
  if (ping) await sendPing(JOB_ID, pingFor(code, last));
  // pg・fetch の直後に process.exit() しない (Windows の Node は libuv の assertion で 127 になる。#1386)
  process.exitCode = code;
  setTimeout(() => process.exit(code), 10000).unref();
}
