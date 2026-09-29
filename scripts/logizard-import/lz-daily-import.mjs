/**
 * lz-daily-import.mjs — ロジザードの毎日の商品マスタの取込 (miniPC・00:20 の定時 `Logizard-NyukaCSV` の 1 ステップ。マスタ正本切替 ③c-1b-2a)
 *
 * **今は「影の取込」だけ** (実行ボタンは押さない。本番の取込は ③c-1b-2b):
 *   1. 00:15〜00:55 (JST) の回だけ動く。その日にもう影の取込ができていれば何もしない (08:40 / 11:45 の回)
 *   2. 対象 = 前の日の lz-daily の正式な証跡 1 つだけ (complete・CSV の sha256 と行数が合う・期限の内。合否は記録する)
 *   3. ポータルの取込の状態と、この PC の初期化の印を照合 (契約 v3 H5)
 *   4. miniPC のロジザードのセッションの鍵 → 共通アカウントでログイン → **直前の書き出し** (pre.csv) →
 *      CSV の全部の商品 ID が直前の書き出しにあり・削除されていないか → インポート画面で**プレビューまで**
 *   5. 記録 (DATA_DIR/lz-import/<日付>/<実行 ID>/pre.csv・shadow.json) と ping (台帳 lz-daily-import-shadow)
 *
 * 使い方: node scripts/logizard-import/lz-daily-import.mjs [--force-window [--as-of YYYY-MM-DD]] [--data-dir D]
 *   --force-window = 時刻の窓の外でも動く (手の試し。ping は打たない・その日の「済み」の印も書かない)。
 *   --as-of = 手の試しの対象の日 (昼に試すとき = その朝の lz-daily。期限の内だけ)。定時は必ず前の日
 * **毎晩の影は LZ_DAILY_IMPORT_SHADOW=on のときだけ動く** (既定 = 止めておく。Codex #1516 R1 High):
 *   手の道 (Stream Deck の auto-barcode.js) が専用アカウント必須・00:00〜01:30 に動かない版 (③c-1b-3) になるまでは、
 *   影のログイン (共通アカウント) が手の取込のセッションを切るおそれがある = on にしない (契約 v2 §6「手の新版を配ってから影を始める」)。
 *   止めてある間は、ランナーが動いたことだけ ok の ping (note = 止めてある) を打つ (bat が呼ばなくなったら締切で気づく)
 * env (リポジトリ直下の .env を読む。bat は C:\tools\logizard-automation から呼ぶので cwd の .env ではない):
 *   DATA_DIR・LZ_LOCK_TOKEN・LZ_IMPORT_STATE_URL・JOBS_MONITOR_TOKEN / URL・LOGIZARD_AUTOMATION_DIR (既定 C:\tools\logizard-automation)
 *   **LZ_DAILY_IMPORT = on = 毎晩の本番** (lz-nightly.mjs の nightlyMain・③c-1b-2b-2)。影はしない (同じ夜に両方は動かさない)。
 *     毎晩の確かめの列の決まりが無いうちは (2b-2a) ❌ で何もしない (ファイル・鍵・ログイン・知らせ = 0・fail の ping は影の項目に)
 * 終了コード: 0 = できた / 窓の外 / 済み・3 = 材料が無い・確かめで止めた・1 = 失敗
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath, pathToFileURL } from 'node:url';
import dotenv from 'dotenv';
import { readLzShohinMaster } from '../../apps/master-decisions/lz-cdb.mjs';
import { pickTarget, precheck, inWindow, jstDateOf } from '../../apps/master-decisions/lz-import-plan.mjs';
import { realSession } from './lz-real-session.mjs';

const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
export const JOB_SHADOW = 'lz-daily-import-shadow';
export const JOB_NIGHTLY = 'lz-daily-import';   // 毎晩の本番 (lz-nightly.mjs と同じ。台帳への登録は切替の PR)
export const EXIT = Object.freeze({ ok: 0, error: 1, skipped: 3 });
export const DEFAULT_AUTOMATION_DIR = 'C:\\tools\\logizard-automation';
const sha256 = (b) => crypto.createHash('sha256').update(b).digest('hex');
export const makeRunId = (now = new Date()) => `lzsh_${now.toISOString().replace(/[-:.]/g, '')}_${crypto.randomBytes(3).toString('hex')}`;

/** その日の影の取込が済んでいるか (08:40 / 11:45 の回は何もしない) */
const doneMarker = (dataDir, day) => path.join(dataDir, 'lz-import', day, 'shadow-done.json');

/**
 * 1 回分 (影の取込)。ロジザードの画面の操作は lzOps (試験で差し替える)。
 * @param {object} p
 * @param {(fn: (ops: { exportShohin: () => Promise<{ buf: Buffer }>, previewImport: (csvPath: string) => Promise<object> }) => Promise<any>) => Promise<any>} p.withSession
 * @param {{ status: Function } | (() => { status: Function })} p.client  ポータルの取込の状態の呼び手 (関数なら、窓・有効化・対象の判定の後に作る = 止めてある間はポータルの設定に頼らない。Codex #1516 R2)
 * @param {(client, localFile) => Promise<{ ok, reason, status }>} p.checkInit
 */
export async function runShadow({ dataDir, now = new Date(), forceWindow = false, asOf = null, enabled = false, localInitFile, client, checkInit, withSession, lzMinRows = 4000, log = console.log }) {
  if (asOf && !forceWindow) throw new Error('--as-of は --force-window (手の試し) のときだけ');
  if (!forceWindow && !inWindow(now)) return { state: 'outside_window', line: 'ℹ ロジザード毎日の商品マスタの取込 (影): 時刻の窓の外 (00:15〜00:55 だけ)' };
  if (!forceWindow && !enabled) return { state: 'disabled', line: 'ℹ ロジザード毎日の商品マスタの取込 (影): 止めてある (LZ_DAILY_IMPORT_SHADOW=on は手の道の新版 ③c-1b-3 の後)' };
  const day = jstDateOf(now);
  if (!forceWindow && fs.existsSync(doneMarker(dataDir, day))) return { state: 'already', line: `ℹ ロジザード毎日の商品マスタの取込 (影): ${day} は済み` };
  const runId = makeRunId(now);
  const dir = path.join(dataDir, 'lz-import', day, runId);
  fs.mkdirSync(dir, { recursive: true });
  const record = { run_id: runId, mode: 'shadow', started_at: now.toISOString(), force_window: forceWindow };
  const save = () => fs.writeFileSync(path.join(dir, 'shadow.json'), JSON.stringify(record, null, 1));
  const stop = (reason, extra = {}) => { Object.assign(record, { state: 'skipped', reason, ...extra }); save(); return { state: 'skipped', reason, runId, record, line: `⏭️ ロジザード毎日の商品マスタの取込 (影): しない (${reason})` }; };
  // ── 対象 (前の日の lz-daily) ──
  const t = pickTarget({ dataDir, now, requirePass: false, ...(asOf ? { asOf } : {}) });
  record.target = { as_of: t.asOf, ok: t.ok, reason: t.reason, lz_daily_run_id: t.evidence?.run_id ?? null, verdict: t.evidence?.verdict ?? null, csv: t.evidence?.csv ?? null };
  if (!t.ok) return stop(`target_${t.reason}`);
  // ── ポータルの取込の状態と、この PC の初期化の印 ──
  let init;
  try {
    const c = typeof client === 'function' ? client() : client;
    init = await checkInit(c, localInitFile);
  } catch (e) { return stop(e && e.code === 'no_token' ? 'portal_no_token' : e && e.code === 'bad_url' ? 'portal_bad_url' : 'portal_unreachable', { error: String(e && e.message).slice(0, 200) }); }
  record.portal = { init_ok: init.ok, reason: init.reason, state: init.status?.state ?? null, halted: init.status?.halted ?? null };
  if (!init.ok) return stop('init_mismatch');
  // ── ロジザード (直前の書き出し → 全部あるか → プレビューまで) ──
  let lzResult;
  // 共通部品 (logizard-common.js) が process.exit しても記録は残す (catch も ping も通らない。fail の ping は送れない = ok の ping が来ないことを dead-man で気づく。Codex #1516 R3 Low)
  const saveOnExit = (code) => {
    if (record.state) return;
    Object.assign(record, { state: 'error', error: `途中で process.exit(${code}) (ロジザードの共通部品が止めた)`, finished_at: new Date().toISOString() });
    try { save(); } catch { /* */ }
  };
  process.once('exit', saveOnExit);
  try {
    lzResult = await withSession(async (ops) => {
      const pre = await ops.exportShohin();
      fs.writeFileSync(path.join(dir, 'pre.csv'), pre.buf, { flag: 'wx' });
      const lz = readLzShohinMaster(pre.buf, { minRows: lzMinRows });
      record.pre = { sha256: sha256(pre.buf), bytes: pre.buf.length, rows: lz.rows, ok: lz.ok, reason: lz.reason };
      if (!lz.ok) return { stop: `pre_export_${lz.reason}` };
      const pc = precheck({ csvBuf: t.csvBuf, lz });
      record.precheck = { ok: pc.ok, rows: pc.rows, missing: pc.missing.slice(0, 50), missing_count: pc.missing.length, deleted: pc.deleted.slice(0, 50), deleted_count: pc.deleted.length };
      if (!pc.ok) return { stop: 'precheck_failed' };
      record.preview = await ops.previewImport(t.csvPath, { captureDir: dir });
      return { ok: true };
    });
  } catch (e) {
    Object.assign(record, { state: 'error', error: String(e && e.message).slice(0, 300), finished_at: new Date().toISOString() });
    save();   // 途中で失敗しても記録は残す
    throw e;
  } finally {
    process.removeListener('exit', saveOnExit);
  }
  if (lzResult.stop) return stop(lzResult.stop);
  Object.assign(record, { state: 'shadow_ok', finished_at: new Date().toISOString() });
  save();
  if (!forceWindow) fs.writeFileSync(doneMarker(dataDir, day), JSON.stringify({ run_id: runId, at: new Date().toISOString() }), { flag: 'wx' });
  log(`📄 対象 ${t.asOf} (${t.evidence.run_id}・${t.evidence.verdict})・直前の書き出し ${record.pre.rows} 行・CSV ${record.precheck.rows} 行が全部ある`);
  return { state: 'shadow_ok', runId, record,
    line: `✅ ロジザード毎日の商品マスタの取込 (影): プレビューまでできた (対象 ${t.asOf}・${t.evidence.verdict}・${record.precheck.rows} 行・実行ボタンは押していない)` };
}

/** 本物のロジザードの操作 (影)。共用の包み (lz-real-session.mjs) を押す部品なし (allowExecute: false) で使う = 影は押さない */
export function realWithSession({ automationDir }) {
  return realSession({ automationDir, label: '毎日の商品マスタの取込 (影)', allowExecute: false }).withSession;
}

export function parseArgs(argv) {
  const out = { forceWindow: false, dataDir: null, asOf: null };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--force-window') out.forceWindow = true;
    else if (a === '--data-dir') out.dataDir = argv[++i];
    else if (a === '--as-of') out.asOf = argv[++i];
    else throw new Error(`知らない引数: ${a}`);
  }
  if (out.asOf && !/^\d{4}-\d{2}-\d{2}$/.test(out.asOf)) throw new Error('--as-of は YYYY-MM-DD');
  if (out.asOf && !out.forceWindow) throw new Error('--as-of は --force-window (手の試し) のときだけ');
  return out;
}

const fold = (x) => (process.platform === 'win32' ? x.toLowerCase() : x);
const isMain = (() => { try { return !!process.argv[1] && fold(fs.realpathSync.native(process.argv[1])) === fold(fs.realpathSync.native(fileURLToPath(import.meta.url))); } catch { return false; } })();
if (isMain) {
  let code = EXIT.error, last = '', ping = false, job = JOB_SHADOW, pingStatus = null;
  try {
    dotenv.config({ path: path.join(REPO_ROOT, '.env') });   // リポジトリ直下の .env (bat の cwd ではない)
    const a = parseArgs(process.argv.slice(2));
    ping = !a.forceWindow;
    if ((process.env.LZ_DAILY_IMPORT || '').trim().toLowerCase() === 'on') {
      ping = true;   // 毎晩の本番の失敗は必ず知らせる (手の試しの引数が付いていても)
      // 毎晩の本番 (③c-1b-2b-2)。影はしない。時刻は Render の時計 (ここでは見ない)。on を見た時点で ping は毎晩の項目
      // (途中の例外の fail も lz-daily-import へ。決まりが無い = nightlyMain の戻り値で影の項目へ。Codex #1547 R1 Medium)
      job = JOB_NIGHTLY;
      if (a.forceWindow || a.asOf) throw new Error('--force-window / --as-of は影の手の試しだけ (毎晩の本番には無い)');
      const { nightlyMain } = await import('./lz-nightly.mjs');
      const r = await nightlyMain({ env: process.env });
      last = r.line; code = r.code; job = r.job;
      // ok の ping = その夜の取込が verified かつ未送の知らせ 0 だけ / 失敗 = fail / ほか = ping しない (dead-man が拾う)
      pingStatus = r.ping; ping = r.ping === 'ok' || r.ping === 'fail';
    } else if (!a.forceWindow && !inWindow(new Date())) {
      // 時刻の窓の外 (08:40 / 11:45 の回) = 何もしない・ping もしない。設定 (DATA_DIR など) を見る前に決める
      // (窓の外の回で設定の欠けを失敗の ping にしない。窓の中の回で欠けていれば失敗 = 気づく。2026-09-29 00:21 の DATA_DIR の件)
      last = 'ℹ ロジザード毎日の商品マスタの取込 (影): 時刻の窓の外 (00:15〜00:55 だけ)';
      code = EXIT.ok;
      ping = false;
    } else {
      const dataDir = (a.dataDir || process.env.DATA_DIR || '').trim();
      if (!dataDir) throw new Error('DATA_DIR が無い');
      if ((process.env.LZ_DAILY_IMPORT || '').trim().toLowerCase() === 'on') throw new Error('LZ_DAILY_IMPORT=on でも、この版は本番の取込をしない (③c-1b-2b まで)');
      const automationDir = (process.env.LOGIZARD_AUTOMATION_DIR || DEFAULT_AUTOMATION_DIR).trim();
      const { createImportStateClient, checkInit } = await import(pathToFileURL(path.join(automationDir, 'import-state-client.js')).href);   // 読み込むだけ (呼び手は判定の後に作る)
      const r = await runShadow({
        dataDir, forceWindow: a.forceWindow, asOf: a.asOf, enabled: (process.env.LZ_DAILY_IMPORT_SHADOW || '').trim().toLowerCase() === 'on',
        localInitFile: path.join(dataDir, 'lz-import', 'init.json'),
        client: () => createImportStateClient(), checkInit, withSession: realWithSession({ automationDir }),
      });
      last = r.line;
      if (r.state === 'outside_window' || r.state === 'already') ping = false;   // 止めてある (disabled) = ok (ランナーは動いた・note で分かる)
      code = r.state === 'skipped' ? EXIT.skipped : EXIT.ok;
    }
  } catch (e) {
    last = `❌ ロジザード毎日の商品マスタの取込 (${job === JOB_SHADOW ? '影' : '毎晩'}): ${String(e && e.message).replace(/\s+/g, ' ').slice(0, 300)}`;
    code = EXIT.error; pingStatus = 'fail';
  }
  console.log(String(last).replace(/\s+/g, ' '));
  if (ping) {
    const { sendPing } = await import('../company-db/lz-daily.mjs');
    await sendPing(job, { status: pingStatus || (code === EXIT.ok ? 'ok' : 'fail'), note: String(last).slice(0, 180) });
  }
  process.exitCode = code;
  setTimeout(() => process.exit(code), 10000).unref();
}
