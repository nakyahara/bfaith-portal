/**
 * lz-nightly.mjs — ロジザードの毎日の商品マスタの取込の**毎晩の本番** (miniPC。マスタ正本切替 ③c-1b-2b-2)
 *
 * 契約 = AI_reference CompanyDB構想/10 §6.3「③c-1b-2b-2 契約 v3」(設計 v1・v2 と合わせて読む)。
 * lz-daily-import.mjs (bat の Logizard-NyukaCSV の 1 ステップ = 00:20・08:40・11:45) が **LZ_DAILY_IMPORT=on のときだけ**呼ぶ (off = 今までの影)。
 *
 * 呼ぶ前 (nightlyMain): 要対応スペースの送り先 (GCHAT_WEBHOOK_JOBS) → 毎晩の確かめの列の決まり (無い = ファイル・鍵・ログイン・知らせが全部ゼロ) → DATA_DIR。
 * 時刻の元 = **Render の時計** (status.clock の server_now を単調な時計 performance.now() に写す。miniPC の壁時計は判断に使わない。N1)。
 * 回ごと (runNightly):
 *   1. どの回も最初に知らせの送り直し (N5): outbox の止め・要確認 → 今の止まった状態の知らせ (K9) → outbox の再適用待ち (50 件・60 秒まで)
 *   2. Render の時刻で窓の外 (08:40 / 11:45) = 知らせだけ (ロジザードに入らない・ping しない。K3-5)
 *   3. 窓の中 (JST 00:15〜00:50): その夜の済みの印 → ポータルの手の取込の旗 (LZ_MANUAL_V4) → 状態で振り分け:
 *        importing で鍵が生きている = 動いている / 鍵なし = mark-unknown → どの結末でも読み直して振り分け直す (N7)
 *        止めてある・手の取込が開いている・unknown / partial / verify_failed・試験の回の imported_unverified = 始めない
 *        毎晩の回の imported_unverified = **その夜は確かめのやり直しだけ** (L-25)
 *        idle / verified = 同じ対象の日がもう始まっていれば何もしない (nightly_last) → 対象 (前の日の lz-daily・合格・ポータルに送れた) →
 *          ポータルの成果物の識別と同じか → nightly-readiness (副作用なし) → 済みの印 → 取込 (エンジン)
 *   4. 最後にもう一度知らせの送り直し (verified の取引で積まれた再適用待ち)
 *   ping (lz-daily-import) の ok = その夜の取込 (か確かめのやり直し) が verified **かつ** 未送の知らせが 0 だけ (ほか = ping しない = dead-man が拾う)
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { performance } from 'node:perf_hooks';
import { pickTarget } from '../../apps/master-decisions/lz-import-plan.mjs';
import { POLICIES, STOP_STATES, assertPolicyReady, importOne, verifyAgain } from './lz-import-engine.mjs';
import { portalWrite } from './portal-io.mjs';
import { jobsHook, sendJobsChat } from './notify-jobs.mjs';
import { realSession } from './lz-real-session.mjs';

export const JOB_NIGHTLY = 'lz-daily-import';
export const JOB_SHADOW = 'lz-daily-import-shadow';
export const EXIT = Object.freeze({ ok: 0, error: 1, skipped: 3 });
export const NIGHTLY_RUN_RE = /^lzim_night_\d{8}T\d{6}_[0-9a-f]{6}$/;
/** 1 回の回で送る知らせの予算 (N5) */
export const BUDGET = Object.freeze({ items: 50, ms: 60000 });
const ADMIN_PAGE_URL = 'https://bfaith-portal.onrender.com/apps/logizard-import-state/admin';
const BY = 'lz-daily-import';
const LABEL = 'ロジザード毎日の商品マスタの取込 (毎晩)';
const sha256 = (b) => crypto.createHash('sha256').update(b).digest('hex');

/** 毎晩の回の記録のフォルダ (実行 ID から直のパス・形を厳密に照らす。探さない) */
export function nightlyRunDir(dataDir, runId) {
  if (!NIGHTLY_RUN_RE.test(String(runId))) throw new Error(`毎晩の回の実行 ID の形が違う: ${String(runId).slice(0, 60)}`);
  return path.join(dataDir, 'lz-import', 'runs', runId);
}
/** その夜の済みの印 (Render の時刻の JST の日) */
export const nightlyMarker = (dataDir, jstDate) => path.join(dataDir, 'lz-import', jstDate, 'nightly-done.json');

/**
 * 知らせの送り直し (N5・K3-4・K9)。順番 = outbox の止め・要確認 → 今の止まった状態 → outbox の再適用待ち。予算 = 件数と時間。
 * 送れた outbox は sent の印 (応答が分からない = 未送のまま = 次の回に重複して送ってよい)。
 * @returns {Promise<{ sent: number, failed: number, skipped_by_budget: number, stop_notice: string|null, remaining: number }>}
 */
export async function sendNotices({ client, notify, perfNow = () => performance.now(), budget = BUDGET }) {
  const t0 = perfNow();
  const out = { sent: 0, failed: 0, skipped_by_budget: 0, stop_notice: null, remaining: 0 };
  const within = () => out.sent + out.failed < budget.items && perfNow() - t0 < budget.ms;
  const box = ((await client.outbox(100)) || {}).outbox || [];
  const urgent = box.filter((x) => x.kind !== 'pending_reapply'), later = box.filter((x) => x.kind === 'pending_reapply');
  const sendItem = async (x) => {
    if (!within()) { out.skipped_by_budget++; return; }
    const ok = await notify(x.text).catch(() => false);
    if (!ok) { out.failed++; return; }
    out.sent++;
    await portalWrite(() => client.outboxSent({ id: x.id, by: BY })).catch(() => null);   // 分からない = 未送のまま
  };
  for (const x of urgent) await sendItem(x);
  // 今の止まった状態の知らせ (まだ = 送る → 状態と出来事の番号に結んで知らせ済み)
  const st = await client.status(1);
  if (st.initialized && STOP_STATES.includes(st.state) && st.run && !st.notified) {
    if (!within()) { out.skipped_by_budget++; out.stop_notice = 'budget'; } else {
      const d = st.run.detail || {};
      const ok = await notify(`⚠️ ロジザードの毎日の商品マスタの取込が止まっている: 状態 ${st.state}・実行 ID ${st.run.run_id}${d.mode ? `・${d.mode}` : ''}${d.target_as_of ? `・対象 ${d.target_as_of}` : ''}\n解除は人 (ロジザードのインポート履歴を確かめてから画面で解除)\n画面 ▶ ${ADMIN_PAGE_URL}`).catch(() => false);
      if (ok) {
        out.sent++;
        const w = await portalWrite(() => client.notified({ run_id: st.run.run_id, state: st.state, state_event_id: st.state_event_id, by: BY })).catch(() => ({ outcome: 'unknown' }));
        out.stop_notice = w.outcome === 'ok' ? 'sent' : 'sent_not_marked';
      } else { out.failed++; out.stop_notice = 'failed'; }
    }
  }
  for (const x of later) await sendItem(x);
  // 残り = ポータルの未送 + 今の止まった状態の知らせがまだ
  const after = await client.status(1);
  const stopLeft = after.initialized && STOP_STATES.includes(after.state) && after.run && !after.notified ? 1 : 0;
  out.remaining = ((after.manual && after.manual.outbox_unsent) || 0) + stopLeft;
  return out;
}

/**
 * 1 回分 (試験では client・withSession・importOneFn・verifyAgainFn・perfNow を差し替える)
 * @returns {Promise<{ state: string, reason?: string, result?: string, runId?: string, ping: 'ok'|null, notices: object[], codes?: string[] }>}
 */
export async function runNightly({ dataDir, client, checkInit, localInitFile, withSession, capabilities, notify, createGuard, importOneFn = importOne, verifyAgainFn = verifyAgain, perfNow = () => performance.now(), log = console.log, budget = BUDGET }) {
  const st0 = await client.status(20);
  if (!st0.initialized) throw new Error('ポータルの取込の状態がまだ初期化されていない');
  if (!st0.clock || !Number.isFinite(st0.clock.server_now)) throw new Error('ポータルが時計 (clock) を返さない = 古いポータル = 毎晩の本番はしない');
  // Render の時計 = server_now + 単調な時計の経過 (miniPC の壁時計は使わない。N1)
  const base = { server: st0.clock.server_now, perf: perfNow() };
  const nowMs = () => base.server + (perfNow() - base.perf);
  const clk = st0.clock;
  const notices = [await sendNotices({ client, notify, perfNow, budget })];
  const done = (x) => ({ ...x, notices });
  if (!clk.in_start_window) return done({ state: 'notify_only', ping: null });   // 08:40 / 11:45 = 知らせだけ
  const marker = nightlyMarker(dataDir, clk.jst_date);
  if (fs.existsSync(marker)) return done({ state: 'already', ping: null });
  const writeMarker = (extra) => {
    fs.mkdirSync(path.dirname(marker), { recursive: true });
    fs.writeFileSync(marker, JSON.stringify({ at_server: nowMs(), ...extra }), { flag: 'wx' });
  };
  let st = await client.status(20);
  if (!st.manual || st.manual.v4 !== true) throw new Error('ポータルの手の取込の旗 (LZ_MANUAL_V4) が立っていない = 毎晩の本番はしない (成果物の照合・区切りが効かない。切替の順番の 3〜4)');
  let outcome = null;
  for (let pass = 0; pass < 2 && !outcome; pass++) {
    if (st.state === 'importing') {
      if (st.lock) { outcome = { state: 'running' }; break; }
      // 鍵が無い importing = 押した後に止まった回 = unknown に (自動では二度と押さない)。どの結末でも読み直して振り分け直す (N7)
      const runId = st.run && st.run.run_id;
      const w = await portalWrite(() => client.markUnknown({ run_id: runId, by: BY, reason: '毎晩の起動で importing が残っていた (鍵は切れている)' }), { expect: (x) => x.state === 'unknown' });
      const again = await client.status(20);
      if (again.state === 'importing' && again.lock) { outcome = { state: 'running', mark: w.outcome }; break; }
      if (again.state === 'importing') { outcome = { state: 'stopped', reason: 'mark_unknown_unconfirmed', mark: w.outcome }; break; }
      st = again;   // unknown (この回が付けた・ほかが付けた) / 進んだ状態 = 振り分け直す
      continue;
    }
    if (st.halted) { outcome = { state: 'halted' }; break; }
    if (st.manual && st.manual.open) { outcome = { state: 'manual_open' }; break; }
    if (st.state === 'imported_unverified') {
      if (!(st.run && st.run.by === 'auto' && st.run.detail && st.run.detail.mode === 'nightly')) { outcome = { state: 'stopped', reason: 'imported_unverified_not_nightly' }; break; }
      // 前の夜の回が未確かめ = その夜は確かめのやり直しだけ (L-25)
      const runId = st.run.run_id;
      writeMarker({ kind: 'verify_again', run_id: runId });
      const r = await verifyAgainFn({ policy: POLICIES.nightly, runId, locateRun: () => nightlyRunDir(dataDir, runId), context: {}, now: new Date(nowMs()),
        localInitFile, client, checkInit, withSession, capabilities, notify, createGuard, log });
      outcome = { state: 'verify_again', runId, result: r.state, reason: r.reason || null };
      break;
    }
    if (STOP_STATES.includes(st.state)) { outcome = { state: 'stopped', reason: st.state }; break; }
    // idle / verified
    const target = clk.expected_target_as_of;
    if (st.nightly_last && st.nightly_last.target_as_of === target) { outcome = { state: 'already_started', runId: st.nightly_last.run_id }; break; }
    const t = pickTarget({ dataDir, now: new Date(nowMs()), requirePass: true, asOf: target });
    if (!t.ok) { outcome = { state: 'skipped', reason: `target_${t.reason}` }; break; }
    const ident = { source_run_id: t.evidence.run_id, target_as_of: t.asOf, csv_sha256: sha256(t.csvBuf), rows: t.evidence.csv.rows };
    let art;
    try { art = await client.getArtifact(ident.source_run_id); } catch (e) {
      if (e && e.status === 404) { outcome = { state: 'skipped', reason: 'artifact_missing' }; break; }
      throw e;
    }
    const a = art && art.artifact ? art.artifact : art;
    const artifact = a ? { source_run_id: a.source_run_id, target_as_of: a.target_as_of, verdict: a.verdict, csv_sha256: a.csv_sha256, rows: a.rows } : null;
    if (!artifact || artifact.verdict !== 'pass' || artifact.source_run_id !== ident.source_run_id || artifact.target_as_of !== ident.target_as_of || artifact.csv_sha256 !== ident.csv_sha256 || artifact.rows !== ident.rows) {
      outcome = { state: 'skipped', reason: 'artifact_mismatch' }; break;
    }
    const rd = await client.nightlyReadiness(ident);
    if (!rd.ready) { outcome = { state: 'skipped', reason: 'not_ready', codes: rd.codes }; break; }
    writeMarker({ kind: 'import', target_as_of: target, source_run_id: ident.source_run_id });
    const r = await importOneFn({ policy: POLICIES.nightly, runsDir: path.join(dataDir, 'lz-import', 'runs'), csvBuf: t.csvBuf,
      csv: { sha256: ident.csv_sha256, rows: ident.rows, target_as_of: ident.target_as_of, source_run_id: ident.source_run_id },
      context: { artifact }, now: new Date(nowMs()), localInitFile, client, checkInit, withSession, capabilities, notify, createGuard, log });
    outcome = { state: 'imported', runId: r.runId, result: r.state };
  }
  if (!outcome) outcome = { state: 'stopped', reason: 'unresolved_after_mark_unknown' };
  notices.push(await sendNotices({ client, notify, perfNow, budget }));   // verified の取引で積まれた再適用待ちも同じ回で
  const verified = (outcome.state === 'imported' || outcome.state === 'verify_again') && outcome.result === 'verified';
  const unsent = notices[notices.length - 1].remaining;
  return { ...outcome, ping: verified && unsent === 0 ? 'ok' : null, unsent, notices };
}

/** 回の結末 → 終了コードと 1 行 */
export function summarize(r) {
  const tail = r.unsent ? `・未送の知らせ ${r.unsent} 件` : '';
  if (r.ping === 'ok') return { code: EXIT.ok, line: `✅ ${LABEL}: ${r.state === 'verify_again' ? '前の夜の回の確かめのやり直し' : '取込'} verified (${r.runId})` };
  if (['notify_only', 'already', 'running', 'already_started'].includes(r.state)) return { code: EXIT.ok, line: `ℹ ${LABEL}: ${r.state}${r.runId ? ` (${r.runId})` : ''}${tail}` };
  if (r.state === 'imported' || r.state === 'verify_again') return { code: EXIT.skipped, line: `⏭️ ${LABEL}: ${r.state} = ${r.result}${r.reason ? ` (${r.reason})` : ''} (${r.runId})${tail}` };
  return { code: EXIT.skipped, line: `⏭️ ${LABEL}: しない (${r.state}${r.reason ? `・${r.reason}` : ''}${r.codes ? `・${r.codes.join(',')}` : ''})${tail}` };
}

/**
 * 入口 (lz-daily-import.mjs の LZ_DAILY_IMPORT=on)。送り先 → 決まり → DATA_DIR を何もする前に見る (決まりが無い = ファイル・鍵・ログイン・知らせが全部ゼロ)。
 * @returns {Promise<{ code: number, line: string, ping: 'ok'|'fail'|null, job: string }>}
 */
export async function nightlyMain({ env = process.env, deps = {} } = {}) {
  if (!jobsHook(env)) return { code: EXIT.error, line: `❌ ${LABEL}: 要対応スペースの送り先 GCHAT_WEBHOOK_JOBS が .env に無い = ログインしない`, ping: 'fail', job: JOB_NIGHTLY };
  try { assertPolicyReady(POLICIES.nightly); } catch (e) {
    // 決まりが無いのに on = 早すぎる on (切替の前) = 影の項目に fail (影の見張りで気づく)。何もしない
    return { code: EXIT.error, line: `❌ ${LABEL}: ${String(e.message).slice(0, 200)} = 何もしない (LZ_DAILY_IMPORT=on は切替の PR の後)`, ping: 'fail', job: JOB_SHADOW };
  }
  const dataDir = String(env.DATA_DIR || '').trim();
  if (!dataDir) return { code: EXIT.error, line: `❌ ${LABEL}: DATA_DIR が無い`, ping: 'fail', job: JOB_NIGHTLY };
  const automationDir = String(env.LOGIZARD_AUTOMATION_DIR || 'C:\\tools\\logizard-automation').trim();
  const imp = (f) => import(pathToFileURL(path.join(automationDir, f)).href);
  const { createImportStateClient, checkInit } = deps.stateClient || await imp('import-state-client.js');
  const { createGuard } = deps.guard || await imp('import-guard.js');
  const session = deps.session || realSession({ automationDir, label: '毎日の商品マスタの取込 (毎晩)', allowExecute: true });
  const r = await runNightly({
    dataDir, client: deps.client || createImportStateClient(), checkInit, localInitFile: path.join(dataDir, 'lz-import', 'init.json'),
    withSession: session.withSession, capabilities: session.capabilities, notify: deps.notify || ((t) => sendJobsChat(t, { env })), createGuard,
    importOneFn: deps.importOneFn, verifyAgainFn: deps.verifyAgainFn, perfNow: deps.perfNow, log: deps.log,
  });
  const s = summarize(r);
  return { ...s, ping: r.ping, job: JOB_NIGHTLY, result: r };
}
