/**
 * lz-import-test.mjs — ロジザードの毎日の商品マスタの取込: 中原さんと一緒の少数件の実機の試験 (マスタ正本切替 ③c-1b-2b-1c)
 *
 * 設計 = AI_reference CompanyDB構想/10 §6.3「③c-1b-2b 契約 v3」(K1〜K10・A〜I)。毎晩の本番の取込は 2b-2 (ここでは作らない・断る)。
 *
 * 使い方 (miniPC・リポジトリ直下で。**昼・中原さんと一緒に・L-16 の占有を確かめてから**):
 *   plan    --normal A-1,B-2 [--missing N-1:A-1] [--deleted D-4] [--case Abc-1:abc-1] [--mapping <json ファイル>] [--as-of YYYY-MM-DD] --occupancy "<確かめたこと>"
 *           = その日の lz-daily の正式な証跡と、直前の書き出しから試験の計画を作る (ロジザードには書き出しのログインだけ)。
 *             DATA_DIR/lz-import-test/<計画 ID>/ に plan.json・test.csv・restore-N.csv・summary.txt。中原さんに見せる一覧と計画の sha256 を出す
 *   run     --plan <計画 ID> --sha256 <計画の sha256> --occupancy "<確かめたこと>"
 *           = 承認した計画 (sha256 が同じ) だけ取り込む: 鍵 → 直前の書き出し (商品・バーコード) → 照らし直し → 押す前の記録 → プレビュー →
 *             importing → 実行 → 結果 → 直後の書き出し (商品・バーコード) → 確かめ → verified / verify_failed
 *   verify  --run <実行 ID> --occupancy "<確かめたこと>"   = imported_unverified の回の確かめのやり直し (昼・L-16 の後。K10)
 *   notify  = 止まった状態の知らせがまだなら送る (送れなかった回の送り直し。K9・I)
 * 00:00〜01:30 (JST) は動かない (毎晩の取込の時間)。止まった (unknown / partial / verify_failed / imported_unverified) = GChat。
 * 解除は人: import-state-cli.js resolve (ロジザードのインポート履歴を確かめてから。K3 = 先に解除して戻す、はしない)。
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { readLzShohinMaster } from '../../apps/master-decisions/lz-cdb.mjs';
import { pickTarget, jstDateOf } from '../../apps/master-decisions/lz-import-plan.mjs';
import { validateImportCsv } from '../../apps/master-decisions/lz-import-check.mjs';
import { buildTestPlan, planSha256, checkTestCsv } from '../../apps/master-decisions/lz-import-test-plan.mjs';
import { portalWrite } from './portal-io.mjs';
import { realSession } from './lz-real-session.mjs';
import { sendJobsChat, jobsHook } from './notify-jobs.mjs';
import { POLICIES, STOP_STATES, inNightBlock, nextNightStart, writeJsonAtomic, saveOnce, checkOccupancy, grabPostExports, isInvalidExport, newRunId as engineRunId, importOne, verifyAgain } from './lz-import-engine.mjs';
import { fileURLToPath, pathToFileURL } from 'node:url';
import dotenv from 'dotenv';

// 鍵・押す・確かめ・知らせは共通の仕組み (lz-import-engine.mjs・③c-1b-3b-1)。ここは試験だけの部分 (計画・承認の印・計画の照らし直し)
export { STOP_STATES, inNightBlock, nextNightStart, writeJsonAtomic, saveOnce, grabPostExports, isInvalidExport };
export const EXIT = Object.freeze({ ok: 0, error: 1, stopped: 3 });
const rand = () => crypto.randomBytes(3).toString('hex');
const stamp = (d) => d.toISOString().replace(/[-:.]/g, '').slice(0, 15);
export const newPlanId = (now = new Date()) => `lzt_${stamp(now)}_${rand()}`;
export const newRunId = (now = new Date()) => engineRunId(now, POLICIES.test);

/**
 * 計画を作る (ロジザードには書き出しのログインだけ)
 * @returns {Promise<{ planId, planSha256, dir, summary: string }>}
 */
export async function planTest({ lzMinRows = 4000, dataDir, now = new Date(), asOf = null, tests, mapping = null, occupancy, withSession, log = console.log }) {
  if (inNightBlock(now)) throw new Error('00:00〜01:30 は動かない (毎晩の取込の時間)');
  const occ = checkOccupancy(occupancy);
  const t = pickTarget({ dataDir, now, requirePass: true, asOf: asOf || jstDateOf(now) });
  if (!t.ok) throw new Error(`計画に使う lz-daily の正式な証跡が無い・使えない (${t.reason})`);
  const v = validateImportCsv(t.csvBuf);
  if (!v.ok) throw new Error(`lz-daily の CSV が確かめを通らない (${v.reason})`);
  const planId = newPlanId(now);
  const dir = path.join(dataDir, 'lz-import-test', planId);
  fs.mkdirSync(dir, { recursive: true });
  const built = await withSession(async (ops) => {
    const pre = await ops.exportShohin();
    saveOnce(path.join(dir, 'plan-pre.csv'), pre.buf);
    const lz = readLzShohinMaster(pre.buf, { minRows: lzMinRows });
    if (!lz.ok) throw new Error(`直前の一覧が読めない (${lz.reason})`);
    return buildTestPlan({ source: { run_id: t.evidence.run_id, as_of: t.asOf, csv_sha256: t.evidence.csv.sha256, table: v.table }, pre: lz, tests, mapping });
  });
  saveOnce(path.join(dir, 'test.csv'), built.testCsv);
  built.restoreCsvs.forEach((b, i) => saveOnce(path.join(dir, `restore-${i + 1}.csv`), b));
  const plan = { ...built.plan, plan_id: planId, created_at: now.toISOString(), occupancy: occ };
  // 承認の印は plan_id・created_at・occupancy を除いた計画の中身 (作り直しても同じ中身なら同じ印)
  writeJsonAtomic(path.join(dir, 'plan.json'), plan);
  const lines = [
    `計画 ${planId} (lz-daily ${plan.source.run_id}・${plan.source.as_of})`,
    `承認の印 (sha256) = ${built.planSha256}`,
    `取り込む CSV = ${plan.test_csv.rows} 行 (sha256 ${plan.test_csv.sha256})`,
    ...plan.rows.map((r) => {
      const before = plan.retained.find((x) => x.id === r.id);
      return `  [${r.kind}] ${r.id}: 取り込む = ${JSON.stringify(r.cells.slice(1))}${before ? ` / 今 = 商品名 ${JSON.stringify(before.cells[5])}・仕入単価 ${JSON.stringify(before.cells[8])}・取引先 ${JSON.stringify(before.cells[27])}` : ''} (期待 = ${r.expected})`;
    }),
    `戻しの資料 = ${plan.restore_csvs.length} 個 (自動で取り込まない)`,
    `取り込む = node scripts/logizard-import/lz-import-test.mjs run --plan ${planId} --sha256 ${built.planSha256} --occupancy "<確かめたこと>"`,
  ];
  fs.writeFileSync(path.join(dir, 'summary.txt'), lines.join('\n') + '\n');
  lines.forEach((l) => log(l));
  return { planId, planSha256: built.planSha256, dir, summary: lines.join('\n') };
}

/**
 * 承認した計画を取り込む
 * @param {object} p
 * @param {object} p.client   import-state-client (acquire / extend / release / transition / status / notified)
 * @param {(client, localFile) => Promise<{ ok, reason, status }>} p.checkInit
 * @param {(fn) => Promise} p.withSession  ops = { exportShohin, exportBarcodes, previewImport, executeImport }
 * @param {(text: string) => Promise<boolean>} p.notify  GChat (送れた = true)
 * @param {Function} p.createGuard  import-guard.js の createGuard
 * @param {{ exportBarcodes: boolean }} p.capabilities  バーコードの書き出しの部品があるか (無い = 試験はしない・K4)
 * @param {Function} [p.writeJson]  記録の書き込み (試験で差し替える)
 */
export async function runTest({ lzMinRows = 4000, dataDir, planId, sha256: approved, occupancy, now = new Date(), localInitFile, client, checkInit, withSession, capabilities, notify, createGuard, log = console.log, heartbeatMs = 30000, writeJson = writeJsonAtomic, nightMarginMs = 60000, save = saveOnce }) {
  if (inNightBlock(now)) throw new Error('00:00〜01:30 は動かない (毎晩の取込の時間)');
  const occ = checkOccupancy(occupancy);
  const dir = path.join(dataDir, 'lz-import-test', String(planId));
  const plan = JSON.parse(fs.readFileSync(path.join(dir, 'plan.json'), 'utf8'));
  const { plan_id: _pid, created_at: _cat, occupancy: _occ, ...body } = plan;
  if (planSha256(body) !== approved) throw new Error('承認の印 (sha256) が計画と違う = 取り込まない (計画を作り直して承認し直す)');
  const testCsv = fs.readFileSync(path.join(dir, 'test.csv'));
  const tc = checkTestCsv(body, testCsv);
  if (!tc.ok) throw new Error(`取り込む CSV が承認した CSV と違う (${tc.reason})`);
  // ここから先 (バーコードの部品・初期化の照合・鍵・押す・確かめ・知らせ) は共通の仕組み (3b-1)
  return importOne({
    policy: POLICIES.test, lzMinRows, runsDir: path.join(dir, 'runs'), csvBuf: testCsv,
    csv: { sha256: body.test_csv.sha256, rows: body.test_csv.rows, target_as_of: body.source.as_of, source_run_id: body.source.run_id },
    // 計画そのものを渡す (承認の印・CSV・計画の照らし直し (K2)・組の商品は共通の仕組みが計画から作る)
    context: { planId, planSha256: approved, plan: body },
    occupancy: occ, now, localInitFile, client, checkInit, withSession, capabilities, notify, createGuard, log, heartbeatMs, writeJson, nightMarginMs, save,
  });
}

/** 本物のロジザードの操作 (試験)。共用の包み (lz-real-session.mjs) を押す部品つき (allowExecute: true) で使う */
export function realTestSession({ automationDir, label = '取込の試験' }) {
  return realSession({ automationDir, label, allowExecute: true }).withSession;
}

/** 実行 ID の記録のフォルダを探す (DATA_DIR/lz-import-test/<計画 ID>/runs/<実行 ID>) */
export function findRunDir(dataDir, runId) {
  if (!/^lzim_test_[0-9A-Za-z_]+$/.test(String(runId))) throw new Error('実行 ID の形が違う');
  const root = path.join(dataDir, 'lz-import-test');
  const hits = fs.existsSync(root) ? fs.readdirSync(root).map((p) => path.join(root, p, 'runs', runId)).filter((d) => fs.existsSync(path.join(d, 'import.json'))) : [];
  if (hits.length !== 1) throw new Error(`実行 ID の記録が ${hits.length} 個 (1 つだけのはず)`);
  return hits[0];
}

/**
 * 確かめのやり直し (imported_unverified の回だけ・昼・L-16 の後。K10・H)。取り込んだ CSV と直前の書き出しはその回の記録から (sha256 を照らす)
 */
export async function verifyOnly({ lzMinRows = 4000, dataDir, runId, occupancy, now = new Date(), localInitFile, client, checkInit, withSession, capabilities, notify, createGuard = undefined, log = console.log, writeJson = writeJsonAtomic, save = saveOnce }) {
  return verifyAgain({
    policy: POLICIES.test, lzMinRows, runId, locateRun: () => findRunDir(dataDir, runId),
    // その回の計画 (<計画 ID>/plan.json) = 試験の組の商品も比べる
    context: { readPlan: (runDir) => JSON.parse(fs.readFileSync(path.join(runDir, '..', '..', 'plan.json'), 'utf8')) },
    occupancy, now, localInitFile, client, checkInit, withSession, capabilities, notify, ...(createGuard ? { createGuard } : {}), log, writeJson, save,
  });
}

/** 止まった状態の知らせがまだなら送る (送れなかった回の送り直し。K9・I) */
export async function notifyPending({ client, notify }) {
  const st = await client.status(1);
  // importing のまま鍵が無い (返した・切れた) = 押した後に止まった回 (結果を書けなかった) = 知らせる (mark-unknown は人。Codex #1524 R1 Medium)
  const stuck = st.initialized && st.state === 'importing' && !st.lock && st.run;
  if (stuck) {
    const sent = await notify(`⚠️ ロジザードの取込がポータルで importing のまま (鍵は切れている)・実行 ID ${st.run.run_id}\nロジザードのインポート履歴を確かめてから import-state-cli.js mark-unknown → resolve`).catch(() => false);
    return { sent, reason: sent ? 'stuck_importing' : 'send_failed', state: st.state };
  }
  if (!st.initialized || !STOP_STATES.includes(st.state) || !st.run || st.notified) return { sent: false, reason: st.notified ? 'already_notified' : 'nothing_to_notify', state: st.state };
  const d = st.run.detail || {};
  const sent = await notify(`⚠️ ロジザードの取込が止まっている: 状態 ${st.state}・実行 ID ${st.run.run_id}${d.plan_id ? `・計画 ${d.plan_id}` : ''}\n解除は人 (ロジザードのインポート履歴を確かめてから import-state-cli.js resolve)`).catch(() => false);
  if (!sent) return { sent: false, reason: 'send_failed', state: st.state };
  const r = await portalWrite(() => client.notified({ run_id: st.run.run_id, state: st.state, state_event_id: st.state_event_id, by: 'lz-import-test' }));
  return { sent: true, marked: r.outcome, state: st.state };
}

/** GChat = 要対応スペース (リポジトリ直下の .env の GCHAT_WEBHOOK_JOBS・Render の即時の知らせ・毎晩の本番と同じ。③c-1b-2b-2 契約 v3 H)。送れた = true */
export async function sendGChat(text, { env = process.env, fetchImpl = fetch } = {}) {
  return sendJobsChat(text, { env, fetchImpl });
}

export function parseArgs(argv) {
  const cmd = argv[0];
  if (!['plan', 'run', 'verify', 'notify'].includes(cmd)) throw new Error('使い方: plan | run | verify | notify');
  const out = { cmd, dataDir: null, tests: { normal: [], missing: [], deleted: [], case: [] }, mapping: null, asOf: null, occupancy: null, plan: null, sha256: null, run: null };
  const list = (v) => String(v || '').split(',').map((x) => x.trim()).filter(Boolean);
  const pairs = (v) => list(v).map((x) => { const [a, b] = x.split(':'); if (!a || !b) throw new Error(`ID:元の ID の形: ${x}`); return [a, b]; });
  for (let i = 1; i < argv.length; i++) {
    const a = argv[i], v = () => { const x = argv[++i]; if (x === undefined) throw new Error(`${a} の値が無い`); return x; };
    if (a === '--data-dir') out.dataDir = v();
    else if (a === '--normal') out.tests.normal = list(v());
    else if (a === '--missing') out.tests.missing = pairs(v()).map(([id, copy_from]) => ({ id, copy_from }));
    else if (a === '--deleted') out.tests.deleted = list(v());
    else if (a === '--case') out.tests.case = pairs(v()).map(([id, from]) => ({ id, from }));
    else if (a === '--mapping') out.mapping = JSON.parse(fs.readFileSync(v(), 'utf8'));
    else if (a === '--as-of') out.asOf = v();
    else if (a === '--occupancy') out.occupancy = v();
    else if (a === '--plan') out.plan = v();
    else if (a === '--sha256') out.sha256 = v();
    else if (a === '--run') out.run = v();
    else throw new Error(`知らない引数: ${a}`);
  }
  if (out.asOf && !/^\d{4}-\d{2}-\d{2}$/.test(out.asOf)) throw new Error('--as-of は YYYY-MM-DD');
  if (cmd === 'run' && (!out.plan || !/^[0-9a-f]{64}$/.test(String(out.sha256 || '')))) throw new Error('run には --plan と --sha256 (64 桁) が要る');
  if (cmd === 'verify' && !out.run) throw new Error('verify には --run が要る');
  return out;
}

const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
const fold = (x) => (process.platform === 'win32' ? x.toLowerCase() : x);
const isMain = (() => { try { return !!process.argv[1] && fold(fs.realpathSync.native(process.argv[1])) === fold(fs.realpathSync.native(fileURLToPath(import.meta.url))); } catch { return false; } })();
if (isMain) {
  let code = EXIT.error;
  try {
    dotenv.config({ path: path.join(REPO_ROOT, '.env') });
    if ((process.env.LZ_DAILY_IMPORT || '').trim().toLowerCase() === 'on') throw new Error('LZ_DAILY_IMPORT=on (毎晩の本番) は 2b-2 まで断る');
    const a = parseArgs(process.argv.slice(2));
    // 取り込む・確かめる・知らせる回は、要対応スペースの送り先が無い・壊れている = 始めない (止まったときに黙って知らせが届かない、をしない。毎晩の本番と同じ)
    if (a.cmd !== 'plan' && !jobsHook(process.env)) throw new Error('要対応スペースの送り先 GCHAT_WEBHOOK_JOBS がリポジトリ直下の .env に無い・壊れている = 止まったときに知らせられない = 始めない (Render の GCHAT_WEBHOOK_JOBS と同じ値を足す)');
    const automationDir = (process.env.LOGIZARD_AUTOMATION_DIR || 'C:\\tools\\logizard-automation').trim();
    const { createImportStateClient, checkInit } = await import(pathToFileURL(path.join(automationDir, 'import-state-client.js')).href);
    const client = createImportStateClient();
    const notify = (t) => sendGChat(t);
    if (a.cmd === 'notify') {
      const r = await notifyPending({ client, notify });
      console.log(JSON.stringify(r));
      code = r.sent || r.reason !== 'send_failed' ? EXIT.ok : EXIT.error;
    } else {
      const dataDir = (a.dataDir || process.env.DATA_DIR || '').trim();
      if (!dataDir) throw new Error('DATA_DIR が無い');
      const { withSession, capabilities } = realSession({ automationDir, label: '取込の試験', allowExecute: true });   // capabilities は包みの本当の ops から
      const localInitFile = path.join(dataDir, 'lz-import', 'init.json');
      if (a.cmd === 'plan') {
        await planTest({ dataDir, asOf: a.asOf, tests: a.tests, mapping: a.mapping, occupancy: a.occupancy, withSession });
        code = EXIT.ok;
      } else if (a.cmd === 'run') {
        const { createGuard } = await import(pathToFileURL(path.join(automationDir, 'import-guard.js')).href);
        const r = await runTest({ dataDir, planId: a.plan, sha256: a.sha256, occupancy: a.occupancy, localInitFile, client, checkInit, withSession, capabilities, notify, createGuard });
        console.log(`実行 ID ${r.runId}・この回の結末 ${r.state}・ポータルの今の状態 ${r.portalState}・記録 ${r.runDir}`);
        code = r.state === 'verified' ? EXIT.ok : EXIT.stopped;
      } else {
        const { createGuard } = await import(pathToFileURL(path.join(automationDir, 'import-guard.js')).href);
        const r = await verifyOnly({ dataDir, runId: a.run, occupancy: a.occupancy, localInitFile, client, checkInit, withSession, capabilities, notify, createGuard });
        console.log(JSON.stringify(r));
        code = r.state === 'verified' ? EXIT.ok : EXIT.stopped;
      }
    }
  } catch (e) {
    console.error(`❌ ${String(e && e.message).slice(0, 400)}`);
    code = EXIT.error;
  }
  process.exitCode = code;
  setTimeout(() => process.exit(code), 10000).unref();
}
