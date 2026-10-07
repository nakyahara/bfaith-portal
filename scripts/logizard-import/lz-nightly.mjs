/**
 * lz-nightly.mjs — ロジザードの毎日の商品マスタの取込の**毎晩の本番** (miniPC。マスタ正本切替 ③c-1b-2b-2)
 *
 * 契約 = AI_reference CompanyDB構想/10 §6.3「③c-1b-2b-2 契約 v3」(設計 v1・v2 と合わせて読む)。
 * lz-daily-import.mjs (bat の Logizard-NyukaCSV の 1 ステップ = 00:20・08:40・11:45) が **LZ_DAILY_IMPORT=on のときだけ**呼ぶ (off = 今までの影)。
 *
 * 呼ぶ前 (nightlyMain): 要対応スペースの送り先 (GCHAT_WEBHOOK_JOBS) → 毎晩の確かめの列の決まり (無い = ファイル・鍵・ログイン・知らせが全部ゼロ) → DATA_DIR。
 * 時刻の元 = **Render の時計** (status.clock の server_now を単調な時計 performance.now() に写す。miniPC の壁時計は判断に使わない。N1)。
 * 回ごと (runNightly):
 *   1. どの回も最初に知らせの送り直し (N5): outbox の止め・要確認 → 今の止まった状態の知らせ (K9) (→ 窓の外の回は再適用待ちの outbox も)。
 *      予算 = **1 回の起動で共通** 50 件・60 秒 (前後の送り直しとエンジンの知らせで分け合う。Codex #1547 R1 Medium)
 *   2. Render の時刻で窓の外 (08:40 / 11:45) = 知らせだけ (ロジザードに入らない・ping しない。K3-5)
 *   3. 窓の中 (JST 00:15〜00:50): ポータルの手の取込の旗 (LZ_MANUAL_V4) → 状態で振り分け (その夜の済みの印は「新しい取込を始める」門だけ = 残った importing の回収と
 *      前の夜の未確かめの確かめのやり直しは済みの印より先。印がぶつかった = もう動いている = 静かに終わる。Codex #1547 R2):
 *        importing で鍵が生きている = 動いている / 鍵なし = mark-unknown → どの結末でも読み直して報告して**終わる** (この起動では取込に進まない。N7・Codex #1547 R1 Medium)
 *        止めてある・手の取込が開いている・unknown / partial / verify_failed・試験の回の imported_unverified = 始めない
 *        毎晩の回の imported_unverified = **その夜は確かめのやり直しだけ** (L-25)。止まった状態の知らせが知らせ済みになるまでは確かめない
 *          (知らせが届かないまま verified になって故障が隠れない。Codex #1547 R1 High)
 *        idle / verified = 同じ対象の日がもう始まっていれば何もしない (nightly_last) → 対象 (前の日の lz-daily・合格・ポータルに送れた) →
 *          ポータルの成果物の識別と同じか → nightly-readiness (副作用なし) → 済みの印 → 取込 (エンジン)
 *   4. 最後にもう一度知らせの送り直し (止め・要確認 → 止まった状態 → 再適用待ち。verified の取引で積まれた再適用待ちも)
 *   ping (lz-daily-import) の ok = その夜の取込 (か確かめのやり直し) が verified **かつ** 前後どの回にも未送・知らせ済みにできない止まった状態が無いときだけ
 *   (ほか = ping しない = dead-man が拾う)
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { performance } from 'node:perf_hooks';
import { pickTarget } from '../../apps/master-decisions/lz-import-plan.mjs';
import { POLICIES, STOP_STATES, assertPolicyReady, importOne, verifyAgain, startAllowed } from './lz-import-engine.mjs';
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

/** 止まった状態だが同じ回の鍵が生きている = 前の起動がまだ動いている (取込の直後の確かめ・確かめのやり直し) = 止まったと知らせない (Codex #1547 R3 Low) */
export const inProgress = (st) => !!(st && st.lock && st.run && st.lock.run_id === st.run.run_id);
/** 知らせるべき止まった状態 (止まった状態・知らせ済みでない・動いている途中でない) */
const stopToNotify = (st) => !!(st && st.initialized && STOP_STATES.includes(st.state) && st.run && !st.notified && !inProgress(st));

/** 毎晩の回の記録のフォルダ (実行 ID から直のパス・形を厳密に照らす。探さない) */
export function nightlyRunDir(dataDir, runId) {
  if (!NIGHTLY_RUN_RE.test(String(runId))) throw new Error(`毎晩の回の実行 ID の形が違う: ${String(runId).slice(0, 60)}`);
  return path.join(dataDir, 'lz-import', 'runs', runId);
}
/** その夜の済みの印 (Render の時刻の JST の日) */
export const nightlyMarker = (dataDir, jstDate) => path.join(dataDir, 'lz-import', jstDate, 'nightly-done.json');

/** 1 回の起動で共通の知らせの予算 (件数と時間。前後の送り直しとエンジンの知らせで分け合う。N5・Codex #1547 R1 Medium) */
export function makeBudget({ perfNow = () => performance.now(), budget = BUDGET } = {}) {
  const b = { t0: perfNow(), used: 0, skipped: 0 };
  b.within = () => b.used < budget.items && perfNow() - b.t0 < budget.ms;
  /** 予算の中で送る (予算の外 = 送らない = false = 未送のまま) */
  b.wrap = (notify) => async (text) => {
    if (!b.within()) { b.skipped++; return false; }
    b.used++;
    return notify(text).catch(() => false);
  };
  return b;
}

/**
 * 知らせの送り直し (N5・K3-4・K9)。順番 = outbox の止め・要確認 → 今の止まった状態 → (includePending のとき) outbox の再適用待ち。
 * 送れた outbox は sent の印 (応答が分からない = 未送のまま = 次の回に重複して送ってよい)。
 * @returns {Promise<{ sent: number, failed: number, skipped_by_budget: number, stop_notice: string|null, stop_pending: boolean, remaining: number }>}
 */
export async function sendNotices({ client, notify, budget, includePending = true }) {
  const out = { sent: 0, failed: 0, skipped_by_budget: 0, stop_notice: null, stop_pending: false, remaining: 0 };
  const send = budget.wrap(notify);
  const box = ((await client.outbox(100)) || {}).outbox || [];
  const urgent = box.filter((x) => x.kind !== 'pending_reapply'), later = includePending ? box.filter((x) => x.kind === 'pending_reapply') : [];
  const sendItem = async (x) => {
    if (!budget.within()) { out.skipped_by_budget++; return; }
    const ok = await send(x.text);
    if (!ok) { out.failed++; return; }
    out.sent++;
    await portalWrite(() => client.outboxSent({ id: x.id, by: BY })).catch(() => null);   // 分からない = 未送のまま
  };
  for (const x of urgent) await sendItem(x);
  // 今の止まった状態の知らせ (まだ = 送る → 状態と出来事の番号に結んで知らせ済み)
  const st = await client.status(1);
  if (stopToNotify(st)) {
    if (!budget.within()) { out.skipped_by_budget++; out.stop_notice = 'budget'; } else {
      const d = st.run.detail || {};
      const ok = await send(`⚠️ ロジザードの毎日の商品マスタの取込が止まっている: 状態 ${st.state}・実行 ID ${st.run.run_id}${d.mode ? `・${d.mode}` : ''}${d.target_as_of ? `・対象 ${d.target_as_of}` : ''}\n解除は人 (ロジザードのインポート履歴を確かめてから画面で解除)\n画面 ▶ ${ADMIN_PAGE_URL}`);
      if (ok) {
        out.sent++;
        const w = await portalWrite(() => client.notified({ run_id: st.run.run_id, state: st.state, state_event_id: st.state_event_id, by: BY })).catch(() => ({ outcome: 'unknown' }));
        out.stop_notice = w.outcome === 'ok' ? 'sent' : 'sent_not_marked';
      } else { out.failed++; out.stop_notice = 'failed'; }
    }
  }
  for (const x of later) await sendItem(x);
  // 残り = ポータルの未送 + 今の止まった状態を知らせ済みにできていない
  const after = await client.status(1);
  out.stop_pending = stopToNotify(after);
  out.remaining = ((after.manual && after.manual.outbox_unsent) || 0) + (out.stop_pending ? 1 : 0);
  return out;
}

/**
 * 1 回分 (試験では client・withSession・importOneFn・verifyAgainFn・perfNow を差し替える)
 * @returns {Promise<{ state: string, reason?: string, result?: string, runId?: string, ping: 'ok'|null, unsent: number, notices: object[], codes?: string[] }>}
 */
export async function runNightly({ dataDir, client, checkInit, localInitFile, withSession, capabilities, notify, createGuard, importOneFn = importOne, verifyAgainFn = verifyAgain, perfNow = () => performance.now(), log = console.log, budget: budgetLimits = BUDGET, lzMinRows = undefined }) {
  const minRows = lzMinRows === undefined ? {} : { lzMinRows };   // 直前の一覧の行数の下限 (試験だけ小さくする。本番の入口は渡さない = エンジンの既定 4,000)
  const st0 = await client.status(20);
  if (!st0.initialized) throw new Error('ポータルの取込の状態がまだ初期化されていない');
  if (!st0.clock || !Number.isFinite(st0.clock.server_now)) throw new Error('ポータルが時計 (clock) を返さない = 古いポータル = 毎晩の本番はしない');
  // Render の時計 = server_now + 単調な時計の経過 (miniPC の壁時計は使わない。N1)
  const base = { server: st0.clock.server_now, perf: perfNow() };
  const nowMs = () => base.server + (perfNow() - base.perf);
  const clk = st0.clock;
  const budget = makeBudget({ perfNow, budget: budgetLimits });
  const engineNotify = budget.wrap(notify);   // エンジンの知らせも同じ予算から
  const notices = [];
  // 窓の外 (08:40 / 11:45) = 知らせだけ (全部)
  if (!clk.in_start_window) {
    notices.push(await sendNotices({ client, notify, budget, includePending: true }));
    return { state: 'notify_only', ping: null, unsent: notices[0].remaining, notices };
  }
  // 窓の中: 最初は止め・要確認と止まった状態だけ (再適用待ちは最後 = 止めの知らせを先に)
  notices.push(await sendNotices({ client, notify, budget, includePending: false }));
  const finish = async (outcome) => {
    notices.push(await sendNotices({ client, notify, budget, includePending: true }));   // verified の取引で積まれた再適用待ちも同じ回で
    const verified = (outcome.state === 'imported' || outcome.state === 'verify_again') && outcome.result === 'verified';
    const unsent = notices[notices.length - 1].remaining;
    const stopPending = notices.some((n) => n.stop_pending);
    return { ...outcome, ping: verified && unsent === 0 && !stopPending ? 'ok' : null, unsent, notices };
  };
  const marker = nightlyMarker(dataDir, clk.jst_date);
  /** 済みの印を書く (もうある = false = ほかの起動が先に書いた) */
  const writeMarker = (extra) => {
    fs.mkdirSync(path.dirname(marker), { recursive: true });
    try { fs.writeFileSync(marker, JSON.stringify({ at_server: nowMs(), ...extra }), { flag: 'wx' }); return true; } catch (e) {
      if (e && e.code === 'EEXIST') return false;
      throw e;
    }
  };
  const st = await client.status(20);
  if (!st.manual || st.manual.v4 !== true) throw new Error('ポータルの手の取込の旗 (LZ_MANUAL_V4) が立っていない = 毎晩の本番はしない (成果物の照合・区切りが効かない。切替の順番の 3〜4)');
  if (st.state === 'importing') {
    if (st.lock) return finish({ state: 'running' });
    // 鍵が無い importing = 押した後に止まった回 = unknown に (自動では二度と押さない)。どの結末でも読み直して報告して終わる (この起動では取込に進まない。N7)
    const runId = st.run && st.run.run_id;
    const w = await portalWrite(() => client.markUnknown({ run_id: runId, by: BY, reason: '毎晩の起動で importing が残っていた (鍵は切れている)' }), { expect: (x) => x.state === 'unknown' });
    const again = await client.status(20);
    if (again.state === 'importing' && again.lock) return finish({ state: 'running', mark: w.outcome });
    if (again.state === 'importing') return finish({ state: 'stopped', reason: 'mark_unknown_unconfirmed', mark: w.outcome });
    // この回に付いた unknown か (実行 ID・状態・状態の出来事の番号が進んだ) を照らす
    const ours = again.state === 'unknown' && again.run && again.run.run_id === runId && Number(again.state_event_id) > Number(st.state_event_id);
    return finish({ state: 'stopped', reason: ours ? 'marked_unknown' : 'state_changed_after_mark_unknown', mark: w.outcome, runId });
  }
  // 止まった状態でも同じ回の鍵が生きている = 前の起動がまだ動いている = 何もしない (知らせない・確かめない。Codex #1547 R3 Low)
  if (STOP_STATES.includes(st.state) && inProgress(st)) return finish({ state: 'running', runId: st.run.run_id });
  if (st.halted) return finish({ state: 'halted' });
  if (st.manual && st.manual.open) return finish({ state: 'manual_open' });
  if (st.state === 'imported_unverified') {
    if (!(st.run && st.run.by === 'auto' && st.run.detail && st.run.detail.mode === 'nightly')) return finish({ state: 'stopped', reason: 'imported_unverified_not_nightly' });
    // 止まった状態の知らせが知らせ済みになるまで確かめない (届かないまま verified になって故障が隠れない。Codex #1547 R1 High)
    if (!st.notified) return finish({ state: 'stopped', reason: 'stop_notice_pending' });
    // 前の夜の回が未確かめ = その夜は確かめのやり直しだけ (L-25)
    const runId = st.run.run_id;
    if (!startAllowed(POLICIES.nightly, 'verify', nowMs())) return finish({ state: 'skipped', reason: 'window_closed' });   // 振り分けの間に窓を過ぎた (Render の時計) = 済みの印を書かない
    writeMarker({ kind: 'verify_again', run_id: runId });   // もうある (同じ夜の 2 回目) でも確かめ直してよい (状態の機械が 1 回ずつにする)
    const r = await verifyAgainFn({ policy: POLICIES.nightly, runId, locateRun: () => nightlyRunDir(dataDir, runId), context: {}, now: new Date(nowMs()),
      localInitFile, client, checkInit, withSession, capabilities, notify: engineNotify, createGuard, log, perfNow, ...minRows });
    return finish({ state: 'verify_again', runId, result: r.state, reason: r.reason || null });
  }
  if (STOP_STATES.includes(st.state)) return finish({ state: 'stopped', reason: st.state });
  // idle / verified = 新しい取込を始める。その夜の済みの印が門 (前の夜の未確かめの確かめのやり直しをした夜も = L-25 の「その夜は確かめだけ」)
  if (fs.existsSync(marker)) return finish({ state: 'already' });
  const target = clk.expected_target_as_of;
  if (st.nightly_last && st.nightly_last.target_as_of === target) return finish({ state: 'already_started', runId: st.nightly_last.run_id });
  const t = pickTarget({ dataDir, now: new Date(nowMs()), requirePass: true, asOf: target });
  if (!t.ok) return finish({ state: 'skipped', reason: `target_${t.reason}` });
  const ident = { source_run_id: t.evidence.run_id, target_as_of: t.asOf, csv_sha256: sha256(t.csvBuf), rows: t.evidence.csv.rows };
  let art;
  try { art = await client.getArtifact(ident.source_run_id); } catch (e) {
    if (e && e.status === 404) return finish({ state: 'skipped', reason: 'artifact_missing' });
    throw e;
  }
  const a = art && art.artifact ? art.artifact : art;
  const artifact = a ? { source_run_id: a.source_run_id, target_as_of: a.target_as_of, verdict: a.verdict, csv_sha256: a.csv_sha256, rows: a.rows } : null;
  if (!artifact || artifact.verdict !== 'pass' || artifact.source_run_id !== ident.source_run_id || artifact.target_as_of !== ident.target_as_of || artifact.csv_sha256 !== ident.csv_sha256 || artifact.rows !== ident.rows) {
    return finish({ state: 'skipped', reason: 'artifact_mismatch' });
  }
  const rd = await client.nightlyReadiness(ident);
  if (!rd.ready) return finish({ state: 'skipped', reason: 'not_ready', codes: rd.codes });
  // 取り込む直前に Render の時計で窓をもう一度 (振り分け・readiness の間に 00:50 を過ぎた = 静かにしない。エンジンも照らす)
  if (!startAllowed(POLICIES.nightly, 'import', nowMs())) return finish({ state: 'skipped', reason: 'window_closed' });
  if (!writeMarker({ kind: 'import', target_as_of: target, source_run_id: ident.source_run_id })) return finish({ state: 'already', reason: 'concurrent' });   // 同時の起動 = ほかが先に書いた = 取り込まない
  const r = await importOneFn({ policy: POLICIES.nightly, runsDir: path.join(dataDir, 'lz-import', 'runs'), csvBuf: t.csvBuf,
    csv: { sha256: ident.csv_sha256, rows: ident.rows, target_as_of: ident.target_as_of, source_run_id: ident.source_run_id },
    context: { artifact }, now: new Date(nowMs()), localInitFile, client, checkInit, withSession, capabilities, notify: engineNotify, createGuard, log, perfNow, ...minRows });
  return finish({ state: 'imported', runId: r.runId, result: r.state });
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
