/**
 * lz-daily.mjs — ロジザードの毎日の商品マスタを Company DB の値で作り、NE の取得の値から作ったもの (GAS と同じ変換) と突き合わせる
 *   (マスタ正本切替 ③c-1a = 影運転の続き。**まだロジザードに取り込まない**。設計 = AI_reference CompanyDB構想/10 §6.3「③c 契約 v1〜v3」)
 *
 * 使い方 (miniPC・daily-sync の 1 ステップ。マスタ照合の後・見張りの前):
 *   node scripts/company-db/lz-daily.mjs --daily [--data-dir D] [--as-of YYYY-MM-DD] [--lz-master <shohin_master.csv>] [--out-dir D]
 *   --out-dir = 出す場所 (CSV・報告・証跡)。既定 = DATA_DIR。手で試すときは別の場所にする (本番の証跡を書かない)
 * env: DATA_DIR / COMPANY_DB_WATCH_URL (watcher = 読むだけ) / LZ_SHOHIN_MASTER_PATH (既定 C:\tools\logizard-automation\out\shohin_master.csv)
 *
 * 材料の条件 (v2 H4・v3 H3)。1 つでも欠ける = 作らない (⏭️ 理由つき・exit 0。成功扱いにしない):
 *   - その朝の照合の証跡が complete (DATA_DIR/company-db-evidence/<今日>/master-compare.json)・全件 JSON の sha256 が合う
 *   - Company DB の元のコードの印がその照合の回・NE の取得の世代がその朝 (JST)
 *   - ロジザードの全件の一覧 (auto-shohin-csv.js の書き出し) がその日 (JST) に書かれたもの・読める・行数の下限以上
 * 出すもの (v3 H3 = 作ったものと取り込むものが同じであることの受け渡し):
 *   - DATA_DIR/lz-daily/<今日>/<実行 ID>/cdb_logizard_shohinmaster_upload.csv (変えない = 新しく作るだけ) と report.json
 *   - 証跡 lz-daily (完了の印) = 入力の世代・CSV の sha256・行数・取込の期限・合否・3 つの分け方の件数
 * 合否 (v2 H6・v3 M5): 説明できない差 0・判定できない 0・形の差 0・不正で出さない 0 (0 でなければ、その商品を中原さんが認めるまで合格にしない)
 * 終わり方: 作れた (合格でも不合格でも) = exit 0 (✅ / ⚠️)。材料が無い = ⏭️ (exit 0)。作ること自体の失敗だけ ❌ (exit 1)
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
export const DEFAULT_LZ_MASTER = 'C:\\tools\\logizard-automation\\out\\shohin_master.csv';
export const LZ_DAILY_VERSION = 'lzd-v1';
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

/**
 * 1 回分。戻り値 = { state: 'complete' | 'skipped', reason?, evidence, line }
 * @param {object} p
 * @param {() => Promise<{ db, close }>} p.connect  Company DB (watcher)
 */
export async function runLzDaily({ dataDir, outDir = dataDir, asOf, lzMasterPath, connect, now = new Date(), readNe = readNeForLz, write = (d, n, p) => writeEvidence(d, n, p, { now }) }) {
  const skip = (reason, detail = {}) => {
    const evidence = { state: 'skipped', as_of: asOf, reason, ...detail, version: LZ_DAILY_VERSION };
    write(outDir, EVIDENCE_NAME, evidence);
    return { state: 'skipped', reason, evidence, line: `⏭️ ロジザード毎日の商品マスタ (影): 作らない (${reason})` };
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
  // ── 3. ロジザードの全件の一覧 (その日の書き出し) ──
  let lzBuf, lzInfo;
  try {
    lzBuf = fs.readFileSync(lzMasterPath);
    const st = fs.statSync(lzMasterPath);
    lzInfo = { path: lzMasterPath, mtime: st.mtime.toISOString(), bytes: lzBuf.length, sha256: sha256(lzBuf) };
  } catch { return skip('lz_master_missing', { lz_master: lzMasterPath }); }
  if (jstDateStr(new Date(lzInfo.mtime)) !== asOf) return skip('lz_master_not_today', { lz_master: lzInfo });
  const lz = readLzShohinMaster(lzBuf);
  if (!lz.ok) return skip(lz.reason, { lz_master: { ...lzInfo, rows: lz.rows } });
  lzInfo.rows = lz.rows;
  // ── 4. Company DB (元のコードと値を 1 つの読み取りの取引で) ──
  const c = await connect();
  let cdbRead;
  try { cdbRead = await readCdbForLz(c.db); } finally { await c.close(); }
  if (!cdbRead.mark || cdbRead.mark.compare_run_id !== ev.compare_run_id) return skip('codes_not_this_run', { code_mark: cdbRead.mark, compare_run_id: ev.compare_run_id });
  // ── 5. 分けて作って比べる ──
  const snap = joinLzSnapshot(ne, { mark: cdbRead.mark, rows: cdbRead.codes }, { takenAt: now.toISOString() });
  const cls = classifyForLz({ neItems: snap.items, cdb: cdbRead.cdb, lz });
  const cdbCsv = buildLzCsv(cls.compare.map((x) => x.cdb), 'daily');
  const neCsv = buildLzCsv(cls.compare.map((x) => x.ne), 'daily');
  const raw = compareLz({ gas: neCsv.bytes, ours: cdbCsv, compareCols: [0, 1, 2, 3, 4], header: DAILY.header });
  const result = explainCdbDiffs(raw, { compareIndex: compareNeIndex(compareJson), byKey: new Map(cls.compare.map((x) => [x.key, x])) });
  // 不正で出さない商品が残る = 合格にしない (v3 M5)。作れない行 (元のコードが無い等) も同じ
  const verdict = result.verdict === 'pass' && cls.invalid.length === 0 && cdbCsv.unmade.length === 0 ? 'pass' : 'fail';
  // ── 6. 出す (変えない CSV + 報告 + 完了の印) ──
  const runId = makeRunId(now);
  const rel = path.join(OUT_DIR, asOf, runId);
  const dir = path.join(outDir, rel);
  fs.mkdirSync(dir, { recursive: true });
  const csvRel = path.join(rel, `cdb_${DAILY.file}`).replace(/\\/g, '/');
  fs.writeFileSync(path.join(outDir, csvRel), cdbCsv.bytes, { flag: 'wx' });
  const report = { run_id: runId, as_of: asOf, classes: { counts: cls.counts, awaiting: cls.awaiting, invalid: cls.invalid }, compare: result,
    build: { counts: cdbCsv.counts, unmade: cdbCsv.unmade, subs: cdbCsv.rows.flatMap((r) => r.subs.map((x) => ({ code: r.key, ...x }))) } };
  const reportBuf = Buffer.from(JSON.stringify(report, null, 1), 'utf8');
  fs.writeFileSync(path.join(dir, 'report.json'), reportBuf, { flag: 'wx' });
  const evidence = {
    state: 'complete', as_of: asOf, run_id: runId, version: LZ_DAILY_VERSION, verdict,
    versions: { converter: LZ_CONVERTER_VERSION, compare: LZ_COMPARE_VERSION },
    inputs: { compare_run_id: ev.compare_run_id, compare_json_sha256: ev.sha256, ne_marks: ne.marks, code_mark: cdbRead.mark, lz_master: lzInfo },
    csv: { path: csvRel, sha256: sha256(cdbCsv.bytes), rows: cdbCsv.counts.made, bytes: cdbCsv.bytes.length },
    report: { path: path.join(rel, 'report.json').replace(/\\/g, '/'), sha256: sha256(reportBuf) },
    deadline: `${asOf}T23:59:59+09:00`,   // 取込 (③c-1b) はこの期限の内・この CSV の sha256 と行数が合うときだけ
    counts: cls.counts,
    summary: { ...result.summary, same_rows: result.counts.same_rows, ne_rows: result.counts.gas_rows },
    allowed_by: result.allowed.reduce((m, a) => ((m[a.why || a.what] = (m[a.why || a.what] || 0) + 1), m), {}),
  };
  write(outDir, EVIDENCE_NAME, evidence);
  const s = evidence.summary;
  const line = `${verdict === 'pass' ? '✅' : '⚠️'} ロジザード毎日の商品マスタ (影): ${verdict === 'pass' ? '合格' : '不合格'}`
    + ` / 比べる ${cls.counts.compare}・新商品待ち ${cls.counts.awaiting}・不正 ${cls.counts.invalid}`
    + ` / 同じ ${s.same_rows}・許す差 ${s.allowed}・説明できない ${s.unexplained}・判定できない ${s.undeterminable}・形の差 ${s.shape}`;
  return { state: 'complete', evidence, line, runId, report };
}

export function parseArgs(argv) {
  const out = { dataDir: null, asOf: null, lzMaster: null, outDir: null };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--data-dir') out.dataDir = argv[++i];
    else if (a === '--as-of') out.asOf = argv[++i];
    else if (a === '--lz-master') out.lzMaster = argv[++i];
    else if (a === '--out-dir') out.outDir = argv[++i];
    else if (a === '--daily' || a === '7') { /* daily-sync の印 */ }
    else throw new Error(`知らない引数: ${a}`);
  }
  if (out.asOf && !/^\d{4}-\d{2}-\d{2}$/.test(out.asOf)) throw new Error('--as-of は YYYY-MM-DD');
  return out;
}

const fold = (x) => (process.platform === 'win32' ? x.toLowerCase() : x);
const isMain = (() => { try { return !!process.argv[1] && fold(fs.realpathSync.native(process.argv[1])) === fold(fs.realpathSync.native(fileURLToPath(import.meta.url))); } catch { return false; } })();
if (isMain) {
  let code = 1, last = '';
  try {
    const a = parseArgs(process.argv.slice(2));
    const dataDir = (a.dataDir || process.env.DATA_DIR || '').trim();
    if (!dataDir) throw new Error('DATA_DIR が無い (--data-dir でも可)');
    const asOf = a.asOf || jstDateStr(new Date());
    const url = (process.env.COMPANY_DB_WATCH_URL || '').trim();
    if (!url) { last = '⏭️ ロジザード毎日の商品マスタ (影): 未設定 (COMPANY_DB_WATCH_URL)'; code = 0; }
    else {
      const lzMasterPath = (a.lzMaster || process.env.LZ_SHOHIN_MASTER_PATH || DEFAULT_LZ_MASTER).trim();
      const r = await runLzDaily({ dataDir, outDir: (a.outDir || dataDir).trim(), asOf, lzMasterPath, connect: () => connectWatcher(url) });
      last = r.line; code = 0;
    }
  } catch (e) {
    last = `❌ ロジザード毎日の商品マスタ (影): ${String(e && e.message).replace(/\s+/g, ' ').slice(0, 400)}`;
    code = 1;
  }
  console.log(String(last).replace(/\s+/g, ' '));
  // pg の直後に process.exit() しない (Windows の Node は libuv の assertion で 127 になる。#1386)
  process.exitCode = code;
  setTimeout(() => process.exit(code), 10000).unref();
}
