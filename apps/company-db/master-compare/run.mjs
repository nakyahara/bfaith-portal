/**
 * run.mjs — 毎朝のマスタ照合 ①ロードの検証 (daily-sync の 1 ステップ。見張りの前。設計 = AI_reference CompanyDB構想/10 §6.1.1 B)
 *
 * 使い方 (miniPC):
 *   node apps/company-db/master-compare/run.mjs --daily [--data-dir D] [--as-of YYYY-MM-DD] [--json]
 *   (--daily は daily-sync の runScript が引数の無いときに '7' を足すのを避ける印。無くても動く)
 * env: COMPANY_DB_WATCH_URL (ロール watcher = select だけ)。無ければ「⏭️ 未設定」で exit 0。DATA_DIR (控え・証跡・全件 JSON)
 *
 * 証跡 (DATA_DIR/company-db-evidence/<日付>/master-compare.json) の順番 (Codex ③a-2 B-R0 #4 = 同じ実行 ID の古い成功を使わせない):
 *   1. 始めに state = running (この回の compare_run_id) を書いて、前の結果を無効にする。書けなければ exit 1
 *   2. 全件 JSON = DATA_DIR/cdb-master-compare/<日付>/<compare_run_id>.json (不変・35 日)
 *   3. state = complete (JSON の場所・sha256・件数・判定)。書けなければ exit 1。途中で落ちたら state = failed (書ければ)
 * 終わり方: 差がある (breach)・判定できない (blocked) も記録まで済めば exit 0 (⚠️)。exit 1 は照合そのものの失敗だけ (DB に届かない・証跡を書けない)
 */
import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { openPgClient, pgAdapter } from '../../../scripts/company-db/migrate.mjs';
import { jstDateStr } from '../../../lib/jst-date.js';
import { writeEvidence } from '../push/evidence.mjs';
import { compareLoad } from './compare-load.mjs';

export const EVIDENCE_NAME = 'master-compare';
export const RESULT_DIR = 'cdb-master-compare';
export const RESULT_KEEP_DAYS = 35;
export const COMPARE_RUN_ID_RE = /^mc_\d{8}T\d{9}Z_[0-9a-f]{6}$/;

export function makeCompareRunId(now = new Date()) {
  return `mc_${now.toISOString().replace(/[-:.]/g, '')}_${crypto.randomBytes(3).toString('hex')}`;
}
export const sha256 = (buf) => crypto.createHash('sha256').update(buf).digest('hex');

/** 全件 JSON を不変で書く (同じ名前があれば書かない)。戻り値 = DATA_DIR からの相対パスと sha256 */
export function writeResultJson(dataDir, asOf, compareRunId, result) {
  const rel = path.join(RESULT_DIR, asOf, `${compareRunId}.json`);
  const file = path.join(dataDir, rel);
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const buf = Buffer.from(JSON.stringify(result), 'utf8');
  const tmp = `${file}.${process.pid}.tmp`;
  try {
    fs.writeFileSync(tmp, buf, { flag: 'wx' });
    if (fs.existsSync(file)) throw new Error(`全件 JSON が既にある (上書きしない): ${rel}`);
    fs.renameSync(tmp, file);
  } finally { try { fs.rmSync(tmp, { force: true }); } catch { /* */ } }
  return { rel: rel.replace(/\\/g, '/'), sha256: sha256(buf), bytes: buf.length };
}

/** 35 日より古い日付のフォルダを消す (失敗しても投げない) */
export function pruneResults(dataDir, { now = new Date(), keepDays = RESULT_KEEP_DAYS } = {}) {
  const root = path.join(dataDir, RESULT_DIR);
  let names = [];
  try { names = fs.readdirSync(root); } catch { return; }
  const cutoff = new Date(now.getTime() - keepDays * 86400000).toISOString().slice(0, 10);
  for (const d of names) if (/^\d{4}-\d{2}-\d{2}$/.test(d) && d < cutoff) { try { fs.rmSync(path.join(root, d), { recursive: true, force: true }); } catch { /* */ } }
}

/** 最後の 1 行 (daily-sync の朝の要約に載る) */
export function summaryLine(r) {
  if (r.verdict === 'blocked') return `⚠️ マスタ照合 ①: 判定できない (${r.blocked_reason})`;
  const c = r.counts || {};
  if (r.verdict === 'pass') return `✅ マスタ照合 ①: ロード ${r.load?.ingest_run_id} の差 0 (SKU ${c.compared?.value ?? 0}・原価 ${c.compared?.cost ?? 0}・代表の仕入先 ${c.compared?.primary_supplier ?? 0}・構成の親 ${c.compared?.components ?? 0})`;
  const t = c.by_type || {};
  return `⚠️ マスタ照合 ①: 差 ${c.items} 件 (無い ${t.missing ?? 0} / 値 ${t.value ?? 0} / 原価 ${t.cost ?? 0} / 代表の仕入先 ${t.primary_supplier ?? 0} / 構成 ${t.components ?? 0})`;
}

/**
 * 1 回の照合 (証跡 → 照合 → 全件 JSON → 証跡)。db = { query } (pg の client でも PGlite でも)
 * @returns {{ result: object, evidence: object, line: string }}
 */
export async function runCompare({ db, dataDir, asOf, now = new Date(), compareRunId = makeCompareRunId(now), compare = compareLoad, write = writeEvidence }) {
  const startedAt = now.toISOString();
  if (!write(dataDir, EVIDENCE_NAME, { state: 'running', compare_run_id: compareRunId, as_of: asOf, started_at: startedAt })) {
    throw new Error('証跡 (実行中) を書けない = 前の回の結果を無効にできない');
  }
  let result;
  try {
    await db.query('begin transaction isolation level repeatable read read only');
    try { result = await compare({ db, dataDir, asOfJst: asOf }); }
    finally { try { await db.query('rollback'); } catch { /* */ } }
    Object.assign(result, { compare_run_id: compareRunId, started_at: startedAt, finished_at: new Date().toISOString() });
    const j = writeResultJson(dataDir, asOf, compareRunId, result);
    const evidence = {
      state: 'complete', compare_run_id: compareRunId, as_of: asOf, started_at: startedAt, finished_at: result.finished_at,
      json_path: j.rel, sha256: j.sha256, bytes: j.bytes, format: result.format,
      verdict: result.verdict, blocked_reason: result.blocked_reason, counts: result.counts,
      load: result.load ? { ingest_run_id: result.load.ingest_run_id, started_at: result.load.started_at } : null,
      materials: result.materials,
    };
    if (!write(dataDir, EVIDENCE_NAME, evidence)) throw new Error('証跡 (完了) を書けない');
    pruneResults(dataDir, { now });
    return { result, evidence, line: summaryLine(result) };
  } catch (e) {
    write(dataDir, EVIDENCE_NAME, { state: 'failed', compare_run_id: compareRunId, as_of: asOf, started_at: startedAt, error: String(e && e.message).slice(0, 300) });
    throw e;
  }
}

export function parseArgs(argv) {
  const out = { dataDir: null, asOf: null, json: false };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--data-dir') out.dataDir = argv[++i];
    else if (a === '--as-of') out.asOf = argv[++i];
    else if (a === '--json') out.json = true;
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
  let client = null;
  try {
    const a = parseArgs(process.argv.slice(2));
    const dataDir = (a.dataDir || process.env.DATA_DIR || '').trim();
    if (!dataDir) throw new Error('DATA_DIR が無い (--data-dir でも可)');
    const asOf = a.asOf || jstDateStr(new Date());
    const url = (process.env.COMPANY_DB_WATCH_URL || '').trim();
    if (!url) {
      last = '⏭️ マスタ照合 ①: 未設定 (COMPANY_DB_WATCH_URL)';
      code = 0;
    } else {
      client = await openPgClient(url);
      await client.query(`set statement_timeout = '60s'`);
      await client.query('set default_transaction_read_only = on');
      const r = await runCompare({ db: pgAdapter(client), dataDir, asOf });
      if (a.json) console.log(JSON.stringify({ evidence: r.evidence }, null, 1));
      last = r.line;
      code = 0;
    }
  } catch (e) {
    last = `❌ マスタ照合 ①: ${String(e && e.message).replace(/\s+/g, ' ').slice(0, 400)}`;
    code = 1;
  } finally {
    if (client) { try { await client.end(); } catch { /* */ } }
  }
  console.log(String(last).replace(/\s+/g, ' '));
  // pg の直後に process.exit() しない (Windows の Node は libuv の assertion で 127 になる。#1386)
  process.exitCode = code;
  setTimeout(() => process.exit(code), 10000).unref();
}
