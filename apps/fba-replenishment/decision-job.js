/**
 * decision-job.js — FBA 補充の「9:40 の自動決定」(影だけ。2026-09-25 A2b)
 *
 * 何をするか:
 *   毎朝 09:40・10:40・11:40 (JST) に、その日の入力がそろっていれば、今の計算エンジンで「送る SKU と数量」を決めて
 *   Company DB (ai.decisions) に記録する。**記録するだけ** (画面には出さない・納品プランも CSV も作らない)。
 *   06:00 の定期同期に載っていた「影の下書き」をこちらへ移した (06:00 は Amazon のレポートがまだ前日のもの)。
 *
 * 入力 (どれも「今朝のもの」でなければ決めない):
 *   - Amazon のレポート (RESTOCK・PLANNING): miniPC から引いて保存し直す。どちらの表も
 *     「今日 05:00 JST ≤ 元データを取った時刻 ≤ いま」でなければ待つ (miniPC の朝の取得は daily-sync の途中、実測 07:42)
 *   - 倉庫在庫: ロジザードの写し (mirror_logizard_stock、毎時)。在庫を取った時刻が 3 時間以内・読み飛ばし 0 行 など
 *     (mirror-warehouse.js)。🚨 画面の倉庫在庫 (手動 CSV) は使わない・書き換えない。差だけ記録する
 *   - 準備中 (作成済みの納品プラン): 毎回取り直す。取り直しと同じ世代のデータと状態を組で使う
 *
 * 決まり (Codex A2b 設計レビュー High 1・Medium 4・7):
 *   - **1 日 1 回だけ決める**。決めた = run 要約行 (ai.decisions、dedupe_key=__run__) に business_date と
 *     decision_final=true が入った。決めたあとの回は何もしない (あとの回の失敗が、決めた提案を消さない)
 *   - 試行全体を Company DB の **session advisory lock** で 1 本に絞る (Render のデプロイで新旧が重なっても、
 *     同じプロセスで cron と起動時の追いつきが重なっても)。ロックを取ってから「今日もう決めたか」を見る
 *   - 09:40・10:40 は入力がそろっていなければ **待つ** (ai.decisions は書かない。ops.job_runs に「待機」を残す)。
 *     11:40 (以降) の回でもそろわなければ「今日は決められない」を記録して、前日以前の提案を無効にする
 *   - 計算そのものの失敗は、どの回でも記録する (前日以前の提案も無効)。次の回でまた試す
 *   - ping: 決めた = ok / 11:40 で決められなかった = partial / 例外・計算の失敗 = fail。待機は ping しない
 *
 * 🚨 画面への影響: レポートの取り込み (syncLatestPlanningFromMiniPC) は画面の「レポート全取得」と同じ処理なので、
 *    9:40 以降は画面の計算材料 (RESTOCK・PLANNING・履歴・FNSKU) も新しくなる (Codex A2b 設計レビュー Medium 6)
 */
import { buildWarehouseFromMirror, diffWarehouse } from './mirror-warehouse.js';
import {
  inputGate, recordShadowDraft, COMPANY_ID, DOMAIN, GENERATOR, RUN_SUMMARY_KEY,
} from './shadow-draft.mjs';

export const DECISION_JOB_ID = 'fba-decision-draft';
/** session advisory lock の鍵 (この仕組み専用の固定値。'FBAD') */
export const DECISION_LOCK_KEY = 0x46424144;
/** 「今朝のレポート」の境目 (JST)。miniPC の朝の取得 (daily-sync) より前 */
export const REPORT_DAY_START_HOUR_JST = 5;
/** この時刻 (JST) 以降の回は最後の回 = そろわなければ「今日は決められない」を記録する */
export const FINAL_MINUTE_JST = 11 * 60 + 40;
/** 1 回の試行の上限。これを過ぎたら記録しない (古い入力のまま遅れて書かない) */
export const ATTEMPT_TIMEOUT_MS = 25 * 60 * 1000;
/** 写しの「在庫を取った時刻」の上限 (毎時の取り込みが 2 回続けて落ちたら止まる) */
export const MIRROR_MAX_AGE_HOURS = 3;

const JST_OFFSET_MS = 9 * 3600e3;

/** JST の日付 (YYYY-MM-DD) と 0 時からの分 */
export function jstClock(ms) {
  const d = new Date(ms + JST_OFFSET_MS);
  return { date: d.toISOString().slice(0, 10), minutes: d.getUTCHours() * 60 + d.getUTCMinutes() };
}

export function isFinalAttempt(nowMs) {
  return jstClock(nowMs).minutes >= FINAL_MINUTE_JST;
}

/** fba.db の source_fetched_at は UTC の 'YYYY-MM-DD HH:MM:SS' で入っている */
const utcMs = (t) => (t ? Date.parse(String(t).replace(' ', 'T') + (/[zZ]|[+-]\d\d:?\d\d$/.test(String(t)) ? '' : 'Z')) : NaN);

/**
 * RESTOCK と PLANNING が **どちらも** 今朝取ったものか。
 * 各表の いちばん古い行 ≥ 今日 05:00 JST / いちばん新しい行 ≤ いま (+1 分) / 取得時刻の無い行 0 (Codex A2b Medium 5)
 * @returns {{code: string, detail: string}[]}
 */
export function reportsFromThisMorning(freshness, nowMs) {
  const out = [];
  const { date } = jstClock(nowMs);
  const dayStartMs = Date.parse(`${date}T00:00:00Z`) - JST_OFFSET_MS + REPORT_DAY_START_HOUR_JST * 3600e3;
  for (const [name, minAt, maxAt, missing] of [
    ['RESTOCK', freshness?.restock_source_at, freshness?.restock_source_max, freshness?.restock_source_missing],
    ['PLANNING', freshness?.planning_source_at, freshness?.planning_source_max, freshness?.planning_source_missing],
  ]) {
    const lo = utcMs(minAt), hi = utcMs(maxAt);
    if (!Number.isFinite(lo) || !Number.isFinite(hi)) out.push({ code: 'report_not_this_morning', detail: `${name} を取った時刻が無い` });
    else if (Number(missing) > 0) out.push({ code: 'report_not_this_morning', detail: `${name} に取得時刻の無い行 ${missing}` });
    else if (lo < dayStartMs) out.push({ code: 'report_not_this_morning', detail: `${name} を取った時刻 = ${minAt} (UTC)。今朝 ${REPORT_DAY_START_HOUR_JST} 時より前` });
    else if (hi > nowMs + 60e3) out.push({ code: 'report_future', detail: `${name} を取った時刻が未来 (${maxAt} UTC)` });
  }
  return out;
}

/** 保存の結果から「今朝の表に替わっていないかもしれない」理由を拾う (Codex A2b Medium 6) */
export function syncReasons(sync) {
  if (!sync) return [{ code: 'report_sync_failed', detail: 'レポートの取り込みを呼べなかった' }];
  if (sync.thrown) return [{ code: 'report_sync_failed', detail: `レポートの取り込みが落ちた: ${sync.thrown}` }];
  if (!sync.ok) return [{ code: 'report_sync_failed', detail: `${sync.error || 'レポートの取り込みに失敗'}` }];
  const out = [];
  for (const [name, skip, err] of [
    ['RESTOCK', sync.restock_skip_reason, sync.restock_error],
    ['PLANNING', sync.planning_latest_skip_reason, sync.planning_latest_error],
  ]) {
    if (err) out.push({ code: 'report_sync_failed', detail: `${name} の保存に失敗: ${err}` });
    else if (skip) out.push({ code: 'report_sync_skipped', detail: `${name} を保存しなかった: ${skip}` });
  }
  return out;
}

/** 今日 (business_date) もう決めたか。決めた = run 要約行に decision_final が入っている (status は問わない) */
export async function findFinalDecision(db, businessDate) {
  const { rows } = await db.query(
    `select decision_id, inputs_ref->>'run_id' as run_id, created_at
       from ai.decisions
      where company_id = $1 and domain = $2 and dedupe_key = $3 and inputs_ref->>'generator' = $4
        and inputs_ref->>'business_date' = $5 and inputs_ref->>'decision_final' = 'true'
      order by created_at desc limit 1`,
    [COMPANY_ID, DOMAIN, RUN_SUMMARY_KEY, GENERATOR, businessDate]);
  return rows[0] || null;
}

async function writeJobRun(db, { startedAt, status, summary }) {
  await db.query(
    `insert into ops.job_runs (job_id, host, started_at, finished_at, status, summary)
     values ($1,'render',$2,now(),$3,$4)`,
    [DECISION_JOB_ID, startedAt, status, String(summary).slice(0, 2000)]);
}

/**
 * 1 回の試行。
 * @param {object} deps  外の世界との出入り口 (試験から差し替える)
 * @param {() => Promise<{db: {query: Function}, close: () => Promise<void>}>} deps.openClient  専用の接続を開く
 * @param {() => Promise<object>} deps.syncReports          miniPC から RESTOCK/PLANNING を引いて保存 (router.syncLatestPlanningFromMiniPC)
 * @param {() => Promise<{data: object, state: object}>} deps.fetchInbound  準備中を取り直す (router.fetchInboundWorkingOnce)
 * @param {() => {meta: object|null, rows: object[]}} deps.readMirror       写しと素性 (mirror-warehouse.readMirrorWarehouse)
 * @param {() => object} deps.readInputFreshness            db.getInputFreshness
 * @param {() => object[]} deps.readManualWarehouseSummary  db.getWarehouseSummary (差を記録するだけ)
 * @param {(inbound: object, opts: object) => object} deps.generate  generateRecommendations(false, inbound, opts)
 * @param {() => object} deps.readSettings
 * @param {(status: string, note: string) => void} deps.ping
 * @param {object} [o]
 * @param {() => number} [o.nowMs]
 * @param {string} [o.trigger]  cron / startup / manual (記録に残すだけ)
 * @returns {Promise<{ outcome: 'locked'|'already_decided'|'waiting'|'decided'|'gated_final'|'engine_failed'|'timeout', detail?: object }>}
 */
export async function runDecisionAttempt(deps, { nowMs = () => Date.now(), trigger = 'cron', log = (m) => console.log(`[FBA-Decision] ${m}`) } = {}) {
  const t0 = nowMs();
  const startedAt = new Date(t0).toISOString();
  const businessDate = jstClock(t0).date;
  const final = isFinalAttempt(t0);
  const conn = await deps.openClient();   // 🚨 接続できなければ何も書けない = 投げる (呼び出し側が fail を ping)
  const db = conn.db;
  let locked = false;
  try {
    const { rows: lk } = await db.query('select pg_try_advisory_lock($1::bigint) as ok', [DECISION_LOCK_KEY]);
    locked = !!lk[0]?.ok;
    if (!locked) {
      log(`別の試行が動いている (${trigger})。この回は何もしない`);
      return { outcome: 'locked' };
    }
    // 🚨 ロックを取ってから確かめる (取る前に見ると、見たあとに別の試行が決めて、こちらが上書きする)
    const decided = await findFinalDecision(db, businessDate);
    if (decided) {
      log(`${businessDate} はもう決めた (run=${decided.run_id})。この回は何もしない`);
      return { outcome: 'already_decided', detail: decided };
    }

    // ① Amazon のレポートを引いて保存し直す (失敗・保存しなかった も理由に残す)
    let sync;
    try { sync = await deps.syncReports(); } catch (e) { sync = { ok: false, thrown: String(e.message).slice(0, 200) }; }
    // ② 準備中を取り直す (取り直した世代のデータと状態を組で持つ。Codex A2b High 3)
    let inbound;
    try { inbound = await deps.fetchInbound(); } catch (e) {
      inbound = { data: {}, state: { source: 'failed', at: new Date(nowMs()).toISOString(), count: 0, error: String(e.message).slice(0, 200) } };
    }

    // ③ ここから記録の直前までは await を挟まない = 読んだ表と計算した表が同じ (Node は 1 本で動く。Codex A2b Medium 5)
    const tc = nowMs();
    const freshness = deps.readInputFreshness();
    const extra = [...syncReasons(sync), ...reportsFromThisMorning(freshness, tc)];
    let mirror;
    try { mirror = deps.readMirror(); } catch (e) { mirror = { meta: null, rows: [], error: String(e.message).slice(0, 200) }; }
    const wh = mirror.error
      ? { ok: false, reasons: [`写しを読めない: ${mirror.error}`] }
      : buildWarehouseFromMirror({ rows: mirror.rows, meta: mirror.meta, nowMs: tc, maxAgeHours: MIRROR_MAX_AGE_HOURS });
    let result = null;
    let warehouseInfo = { source: 'logizard_mirror', ok: wh.ok, reasons: wh.reasons };
    if (!wh.ok) {
      extra.push({ code: 'warehouse_mirror_not_ready', detail: wh.reasons.join(' / ').slice(0, 300) });
    } else {
      // 計算が投げても「今日は計算できなかった」として記録する (前日以前の提案も無効にする)
      try { result = deps.generate(inbound.data, { warehouse: wh }); } catch (e) {
        result = { items: [], data_quality: {}, errors: [`計算が落ちた: ${String(e.message).slice(0, 200)}`] };
      }
      let diff = null;
      try { diff = diffWarehouse(wh.summaryRows, deps.readManualWarehouseSummary()); } catch (e) { diff = { error: String(e.message).slice(0, 200) }; }
      warehouseInfo = {
        ...warehouseInfo, source_at: wh.sourceAt, captured_at: wh.capturedAt, stats: wh.stats,
        manual_uploaded_at: freshness?.warehouse_uploaded_at || null, diff_vs_manual: diff,
      };
    }
    const after = deps.readInputFreshness();
    for (const k of ['restock_source_at', 'restock_source_max', 'planning_source_at', 'planning_source_max']) {
      if ((after?.[k] ?? null) !== (freshness?.[k] ?? null)) {
        extra.push({ code: 'report_changed_during_compute', detail: `${k} が計算の途中で替わった` });
        break;
      }
    }
    const inputFreshness = { ...freshness, warehouse_source_at: wh.ok ? wh.sourceAt : null };
    const dq = result?.data_quality || {};
    const gate = inputGate({ inboundState: inbound.state, inputFreshness, dq, now: new Date(tc) });
    gate.reasons.push(...extra);

    const engineFailed = Array.isArray(result?.errors) && result.errors.filter(Boolean).length > 0;
    const codes = [...new Set(gate.reasons.map((r) => r.code))];

    // ④ そろっていない & まだ最後の回ではない → 待つ (ai.decisions は書かない・前日の提案もそのまま)
    if (!engineFailed && codes.length && !final) {
      const summary = `待機 ${businessDate} (${trigger}): ${gate.reasons.map((r) => `${r.code} (${r.detail})`).join(' / ')}`;
      await writeJobRun(db, { startedAt, status: 'partial', summary });
      log(summary);
      return { outcome: 'waiting', detail: { reasons: gate.reasons } };
    }

    if (nowMs() - t0 > ATTEMPT_TIMEOUT_MS) {
      const summary = `時間切れ ${businessDate} (${trigger}): ${Math.round((nowMs() - t0) / 1000)} 秒。記録しない`;
      await writeJobRun(db, { startedAt, status: 'fail', summary });
      deps.ping('fail', summary);
      return { outcome: 'timeout' };
    }

    // ⑤ 記録。計算の失敗 = fail (決めていない)、関所 = 今日は決められない (最後の回だけ)、通った = 決めた
    let settings = null;
    try { settings = deps.readSettings(); } catch (e) { settings = { error: String(e.message).slice(0, 120) }; }
    const runMeta = {
      business_date: businessDate,
      decision_final: !engineFailed,
      job_id: DECISION_JOB_ID,
      trigger,
      attempt_final: final,
      warehouse_input: warehouseInfo,
      report_sync: sync ? {
        ok: !!sync.ok, error: sync.error || sync.thrown || null, snapshot_date: sync.snapshot_date || null,
        restock: sync.restock ?? null, planning_latest: sync.planning_latest ?? null,
        restock_skip_reason: sync.restock_skip_reason || null, planning_latest_skip_reason: sync.planning_latest_skip_reason || null,
      } : null,
    };
    const rec = await recordShadowDraft(db, result || { items: [], data_quality: {}, snapshot_date: null }, {
      host: 'render', log, now: new Date(tc), startedAt, jobId: DECISION_JOB_ID,
      inboundState: inbound.state, settings, inputFreshness, gate, runMeta,
      openFresh: null,   // 🚨 ロックの外の接続では書かない (ロックを持たない書き込みが、決めた提案を消さないように)
    });
    if (engineFailed) {
      deps.ping('fail', `計算できなかった: ${result.errors.join(' / ')}`);
      return { outcome: 'engine_failed', detail: rec };
    }
    if (rec.gated) {
      deps.ping('partial', `${businessDate} は決められない: ${codes.join(',')}`);
      return { outcome: 'gated_final', detail: rec };
    }
    deps.ping('ok', `${businessDate} 提案${rec.proposals}/不能${rec.blocked} (${trigger})`);
    return { outcome: 'decided', detail: rec };
  } finally {
    if (locked) { try { await db.query('select pg_advisory_unlock($1::bigint)', [DECISION_LOCK_KEY]); } catch { /* 接続を閉じればロックも外れる */ } }
    try { await conn.close(); } catch { /* もう閉じている */ }
  }
}

/**
 * プロセス内で重ならないようにして 1 回試す。**投げない** (cron・起動時の追いつきを巻き添えにしない)。
 * 例外は fail を ping して結果に入れる
 */
let running = false;
export async function runDecisionAttemptSafe(deps, o = {}) {
  if (running) return { outcome: 'busy' };
  running = true;
  try {
    return await runDecisionAttempt(deps, o);
  } catch (e) {
    console.error('[FBA-Decision] 試行が落ちた:', e);
    try { deps.ping('fail', `試行が落ちた: ${e.message}`); } catch { /* ping の失敗は無視 */ }
    return { outcome: 'error', error: String(e.message) };
  } finally {
    running = false;
  }
}

/** 起動したときに、今日の回を取りこぼしていたら 1 回試す (09:40 より前なら何もしない。cron が拾う) */
export function shouldCatchUpAtStartup(nowMs) {
  return jstClock(nowMs).minutes >= 9 * 60 + 40;
}
