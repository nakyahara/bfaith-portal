/**
 * lz-import-engine.mjs — ロジザードの毎日の商品マスタの取込の共通の仕組み (マスタ正本切替 ③c-1b-3b-1)
 *
 * 設計 = AI_reference CompanyDB構想/10 §6.3「③c-1b-3b 契約 (設計 R1)」。3b-1 = 試験のランナー (lz-import-test.mjs) の中身を取り出しただけ (動きは変えない)。
 * 取り込む CSV の出どころ (試験の計画・手の ③ の受け付け・毎晩の Company DB) に依らない部分:
 *   importOne   = 鍵 → 直前の書き出し (商品・バーコード) → 呼び手の照らし直し → 押す前の記録 → プレビュー → importing → 押す → 結果 →
 *                 直後の書き出し → 確かめ → verified / verify_failed・止まった = 知らせ
 *   verifyAgain = imported_unverified の回の確かめのやり直し
 * 持ち主・mode・名乗り・確かめの決まり・バーコードの要否・時刻は **閉じた決まり (POLICIES)** で組にして渡す (任意の引数の寄せ集めにしない。3b 契約 C8・R0-10)。
 * 時刻 (③c-1b-2b-2 契約 v3 N1・F): test = 00:00〜01:30 (JST) は動かない / nightly = Render の時計で JST 00:15〜00:50 に始め、00:55 が締め切り
 *   (nightly の now は呼び手が Render の server_now から作る = この中の時計は now + 単調な時計の経過・ポータルの期限はそのまま比べる)。
 * nightly の確かめの列の決まり (RULES_NIGHTLY) は実機の試験で決める (2b-2b) = それまで動かない。
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { readLzShohinMaster } from '../../apps/master-decisions/lz-cdb.mjs';
import { validateImportCsv, parseImportResult, judgeImportResult } from '../../apps/master-decisions/lz-import-check.mjs';
import { verifyImport, readBarcodeExport, compareBarcodes, barcodeMissing, compileRules, RULES_2B1, RULES_NIGHTLY } from '../../apps/master-decisions/lz-import-verify.mjs';
import { precheck } from '../../apps/master-decisions/lz-import-plan.mjs';
import { createGuard as defaultCreateGuard } from '../../tools/logizard-automation/import-guard.js';
import { planSha256 as planDigest, checkPlanAgainstPre, checkTestCsv } from '../../apps/master-decisions/lz-import-test-plan.mjs';
import { portalWrite, startHeartbeat } from './portal-io.mjs';
import { performance } from 'node:perf_hooks';

export const STOP_STATES = Object.freeze(['unknown', 'partial', 'verify_failed', 'imported_unverified']);

/**
 * 閉じた決まり。ここに無い決まりでは動かない (importOne / verifyAgain が照らす)。
 * test = 少数件の実機の試験 (持ち主 auto・mode test・バーコードも比べる・decided:false の決まりを許す = 試験で決めるため。3b 契約 R0-2)
 * nightly = 毎晩の本番 (③c-1b-2b-2・持ち主 auto・mode nightly・名乗り lz-daily-import・商品とバーコードの両方を比べる (L-24)・
 *   時刻 = Render の時計の夜の窓・占有の文は要らない (夜の窓は自動だけ = Stream Deck の ③ は 00:00〜01:30 に動かない #1518)・
 *   確かめの列の決まり = RULES_NIGHTLY (実機の試験で決めるまで null = 動かない)
 */
export const POLICIES = Object.freeze({
  test: Object.freeze({ name: 'test', holder: 'auto', mode: 'test', by: 'lz-import-test', runIdPrefix: 'lzim_test_', label: '取込の試験', verbImport: '試験', rules: RULES_2B1, allowUndecided: true, barcode: true, importTtlSec: 180, verifyTtlSec: 300, window: 'day', serverClock: false, occupancy: 'required' }),
  nightly: Object.freeze({ name: 'nightly', holder: 'auto', mode: 'nightly', by: 'lz-daily-import', runIdPrefix: 'lzim_night_', label: '毎晩の取込', verbImport: '取込', rules: RULES_NIGHTLY, allowUndecided: false, barcode: true, importTtlSec: 180, verifyTtlSec: 300, window: 'night', serverClock: true, occupancy: 'night_window' }),
});
function checkPolicy(policy) {
  if (!Object.values(POLICIES).includes(policy)) throw new Error('決まり (policy) が POLICIES に無い = 動かない');
  if (!policy.rules) throw new Error(`決まり ${policy.name} の確かめの列の決まりがまだ無い (実機の少数件の試験で決める = 2b-2b) = 動かない`);
  if (!policy.allowUndecided && !compileRules(policy.rules).decided) throw new Error(`決まり ${policy.name} は decided:true の確かめの決まりだけ (${policy.rules.version} は decided:false)`);
  return policy;
}
/** 決まりが動ける形か (動けない = 例外)。入口が何もする前に呼ぶ (決まりが無い = ファイル・鍵・ログイン・知らせが全部ゼロ。③c-1b-2b-2 契約 v3) */
export const assertPolicyReady = (policy) => { checkPolicy(policy); return true; };

/**
 * 決まりごとの呼び手の値 (context) の形。欠け・余計なキーは checkInit の前に断る (Codex #1535 R1 High・Medium)。
 * 記録の頭・importing の detail に足す値・知らせの文の後ろは、ここで context から作る (呼び手が自由に書けない = 固定の項目を上書きできない)
 */
const ctxError = (what) => new Error(`決まりの呼び手の値 (context) が違う: ${what} = 動かない`);
function exactKeys(c, want) {
  if (!c || typeof c !== 'object' || Array.isArray(c)) throw ctxError('context が無い');
  const got = Object.keys(c).sort();
  if (got.join(',') !== [...want].sort().join(',')) throw ctxError(`キーは ${want.join('・')} だけ (今 = ${got.join('・') || '-'})`);
}
/** 計画の組の商品 (空・文字でない = 比べる商品が分からない = 断る) */
function groupIds(plan) {
  const ids = (Array.isArray(plan && plan.groups) ? plan.groups : []).flatMap((g) => (g && Array.isArray(g.ids) ? g.ids : [null]));
  if (!ids.length || ids.some((x) => typeof x !== 'string' || !x)) throw ctxError('計画の組の商品 (groups) が空か形が違う');
  return ids;
}
const CONTEXTS = Object.freeze({
  test: Object.freeze({
    // 計画の ID・承認の印・計画 (plan.json から plan_id・created_at・occupancy を除いたもの)。
    // 承認の印・取り込む CSV・計画の照らし直し・計画の組の商品は、ここで計画から作る (呼び手が省けない。Codex #1535 R2 Medium)
    import: (c, { csvBuf, csv }) => {
      exactKeys(c, ['planId', 'planSha256', 'plan']);
      if (!/^lzt_[0-9A-Za-z_]{1,80}$/.test(String(c.planId))) throw ctxError('planId の形');
      if (!/^[0-9a-f]{64}$/.test(String(c.planSha256))) throw ctxError('planSha256 (64 桁)');
      const plan = c.plan;
      if (!plan || typeof plan !== 'object' || !plan.test_csv || !plan.source) throw ctxError('plan (計画) が無い・形が違う');
      if (planDigest(plan) !== c.planSha256) throw ctxError('計画が承認の印と違う');
      const tc = checkTestCsv(plan, csvBuf);
      if (!tc.ok) throw ctxError(`取り込む CSV が計画の CSV と違う (${tc.reason})`);
      if (csv.sha256 !== plan.test_csv.sha256 || csv.rows !== plan.test_csv.rows || csv.target_as_of !== plan.source.as_of || csv.source_run_id !== plan.source.run_id) throw ctxError('CSV の識別が計画と違う');
      const extraIds = groupIds(plan);
      // 承認のときから一覧が変わった = 取り込まない (K2)
      const preCheck = (lz) => {
        const again = checkPlanAgainstPre(plan, lz);
        return again.ok ? null : { stage: ['plan_changed', { diffs: again.diffs.slice(0, 50) }], error: `承認のときから一覧が変わった = 取り込まない (計画を作り直す): ${again.diffs.slice(0, 5).map((d) => `${d.kind}:${d.id}`).join(', ')}` };
      };
      return { tag: { plan_id: c.planId }, recExtra: { plan_id: c.planId, plan_sha256: c.planSha256 }, notifyTail: `・計画 ${c.planId}`, preCheck, extraIds };
    },
    // 確かめのやり直し: その回の計画を読む (記録の承認の印・計画の ID と照らしてから組の商品を使う)
    verify: (c) => {
      exactKeys(c, ['readPlan']);
      if (typeof c.readPlan !== 'function') throw ctxError('readPlan (その回の計画)');
      return {
        readExtraIds: (runDir, rec) => {
          const { plan_id: planId, created_at: _c, occupancy: _o, ...body } = c.readPlan(runDir) || {};
          if (planId !== rec.plan_id || planDigest(body) !== rec.plan_sha256) throw new Error('計画 (plan.json) が記録の計画の ID・承認の印と違う = 比べる商品が分からない');
          return groupIds(body);
        },
      };
    },
  }),
  // 毎晩の本番 (③c-1b-2b-2): 呼び手の値 = ポータルの成果物の識別だけ。取り込む CSV の識別と全部同じ・判定 pass (K3-1。ポータルも importing の取引で照らす = 二重)。
  // 直前の一覧で照らす = CSV の全部の商品がある・削除でない (L-7 = 無い商品の行はエラーになるので押さない)。比べる商品 = CSV の商品だけ
  nightly: Object.freeze({
    import: (c, { csvBuf, csv }) => {
      exactKeys(c, ['artifact']);
      const a = c.artifact;
      exactKeys(a, ['source_run_id', 'target_as_of', 'verdict', 'csv_sha256', 'rows']);
      if (a.verdict !== 'pass') throw ctxError('ポータルの成果物の判定が pass でない');
      if (a.source_run_id !== csv.source_run_id || a.target_as_of !== csv.target_as_of || a.csv_sha256 !== csv.sha256 || a.rows !== csv.rows) throw ctxError('取り込む CSV の識別がポータルの成果物と違う (K3-1)');
      const preCheck = (lz) => {
        const pc = precheck({ csvBuf, lz });
        return pc.ok ? null : {
          stage: ['precheck_failed', { missing: pc.missing.slice(0, 50), missing_count: pc.missing.length, deleted: pc.deleted.slice(0, 50), deleted_count: pc.deleted.length }],
          error: `CSV の商品がロジザードに無い・削除 = 押さない (L-7): 無い ${pc.missing.length} 件・削除 ${pc.deleted.length} 件 (${[...pc.missing, ...pc.deleted].slice(0, 5).join(', ')})`,
        };
      };
      return { tag: {}, recExtra: { artifact: a.source_run_id }, notifyTail: `・対象 ${a.target_as_of}`, preCheck, extraIds: [] };
    },
    verify: (c) => { exactKeys(c, []); return { readExtraIds: () => [] }; },
  }),
});
/** 決まりごとの呼び手の値の照らし (試験で直に見る。決まりを通す道ではない = 裏口にならない) */
export { CONTEXTS };
const isRealDate = (x) => /^\d{4}-\d{2}-\d{2}$/.test(String(x)) && new Date(`${x}T00:00:00Z`).toISOString().slice(0, 10) === x;
/** 取り込む CSV の識別の形と中身 (sha256・形・行数・実在の日・出どころ)。読んだ表を返す (後で使い回す。Codex #1535 R2 Medium) */
function checkCsv(csvBuf, csv) {
  exactKeys(csv, ['sha256', 'rows', 'target_as_of', 'source_run_id']);
  if (!Buffer.isBuffer(csvBuf) || sha256(csvBuf) !== csv.sha256) throw ctxError('csv.sha256 が取り込む CSV の中身と違う');
  const v = validateImportCsv(csvBuf);
  if (!v.ok) throw ctxError(`取り込む CSV の形が違う (${v.reason})`);
  if (csv.rows !== v.table.length) throw ctxError(`csv.rows (${csv.rows}) が CSV の行数 (${v.table.length}) と違う`);
  if (!isRealDate(csv.target_as_of)) throw ctxError('csv.target_as_of (実在の日 YYYY-MM-DD)');
  if (typeof csv.source_run_id !== 'string' || !/^[0-9A-Za-z_.-]{1,120}$/.test(csv.source_run_id)) throw ctxError('csv.source_run_id');
  return v.table;
}

const sha256 = (b) => crypto.createHash('sha256').update(b).digest('hex');
const rand = () => crypto.randomBytes(3).toString('hex');
const stamp = (d) => d.toISOString().replace(/[-:.]/g, '').slice(0, 15);
export const newRunId = (now = new Date(), policy = POLICIES.test) => `${policy.runIdPrefix}${stamp(now)}_${rand()}`;

/** 毎晩の本番の時刻 (Render の store.js の NIGHTLY と同じ値: 始めてよい [00:15, 00:50)・締め切り 00:55) */
export const NIGHT = Object.freeze({ startFromMin: 15, startToMin: 50, deadlineMin: 55 });
const JST_MS = 9 * 3600 * 1000, MIN_MS = 60000;
const jstDayStart = (ms) => { const d = new Date(ms + JST_MS); return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()) - JST_MS; };
/**
 * 始めてよい時刻か (③c-1b-2b-2 N1・F)。取込も確かめのやり直しも同じ窓 (purpose は記録のため)。
 * nightly = JST [00:15, 00:50) (nowMs は Render の時計から作った値) / test = 00:00〜01:30 の外
 */
export function startAllowed(policy, purpose, nowMs) {
  void purpose;
  if (policy.window === 'night') { const m = nowMs - jstDayStart(nowMs); return m >= NIGHT.startFromMin * MIN_MS && m < NIGHT.startToMin * MIN_MS; }
  return !inNightBlock(new Date(nowMs));
}
/** 押す・確かめの締め切り (旗の上限)。nightly = その日の 00:55 − 余白 / test = 次の 00:00 − 余白 */
export function deadlineAt(policy, nowMs, marginMs) {
  if (policy.window === 'night') return jstDayStart(nowMs) + NIGHT.deadlineMin * MIN_MS - marginMs;
  return nextNightStart(new Date(nowMs)) - marginMs;
}
/**
 * この回の時計 = now (nightly = Render の server_now から作った値) + 単調な時計の経過 (壁時計を使わない)。
 * ポータルの期限 (serverMs) の写し方: Render の時計の回 = そのまま比べる / 手元の時計の回 = 手元の今との差で写す
 */
function runClock(policy, now) {
  const p0 = performance.now();
  const clock = () => now.getTime() + (performance.now() - p0);
  const toClock = policy.serverClock ? (serverMs) => serverMs : (serverMs) => clock() + (serverMs - Date.now());
  return { clock, toClock };
}

/** JST 00:00〜01:30 = 毎晩の取込の時間 = 動かない */
export function inNightBlock(now = new Date()) {
  const d = new Date(now.getTime() + 9 * 3600 * 1000);
  const m = d.getUTCHours() * 60 + d.getUTCMinutes();
  return m < 90;
}

/** 次の 00:00 (JST) の時刻 (ms) */
export function nextNightStart(now = new Date()) {
  const j = new Date(now.getTime() + 9 * 3600 * 1000);
  return Date.UTC(j.getUTCFullYear(), j.getUTCMonth(), j.getUTCDate() + 1) - 9 * 3600 * 1000;
}

/** 一時ファイル → rename (書けない = 例外 = 進まない) */
export function writeJsonAtomic(file, obj) {
  const tmp = `${file}.${process.pid}.${rand()}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(obj, null, 1), { flag: 'wx' });
  fs.renameSync(tmp, file);
}
/** 1 回だけ書く (もうある = 例外) */
export const saveOnce = (file, buf) => fs.writeFileSync(file, buf, { flag: 'wx' });

export function checkOccupancy(occupancy) {
  if (!occupancy || String(occupancy).trim().length < 8) throw new Error('--occupancy に、共通アカウントを使う人・作業が止まっていることを確かめた内容を書く (L-16・8 文字以上)');
  return String(occupancy).trim().slice(0, 300);
}

/** 動いてよい時間か・占有の確かめ・バーコードの部品 (決まりが要るとき)。呼び手が先に呼んでもよい (同じ結果) */
export function preflight({ policy, now, occupancy, capabilities, purpose = 'import' }) {
  checkPolicy(policy);   // 決まりを照らす前に決まりの中身を読まない
  const verb = purpose === 'verify' ? '確かめ' : policy.verbImport;
  if (!startAllowed(policy, purpose, now.getTime())) {
    throw new Error(policy.window === 'night' ? `毎晩の${verb}は JST 00:${NIGHT.startFromMin}〜00:${NIGHT.startToMin} (Render の時刻) に始める` : '00:00〜01:30 は動かない (毎晩の取込の時間)');
  }
  const occ = policy.occupancy === 'night_window' ? 'night_window (夜の窓は自動だけ・Stream Deck の ③ は 00:00〜01:30 に動かない #1518)' : checkOccupancy(occupancy);
  if (policy.barcode && (!capabilities || !capabilities.exportBarcodes)) throw new Error(`バーコードの書き出しの部品が無い = ${verb}はしない (K4)`);
  // 押す部品の無い包み (影の包み) では取り込まない (包みの capabilities は本当の ops から作る = lz-real-session.mjs)
  if (purpose === 'import' && capabilities && capabilities.executeImport === false) throw new Error('押す部品の無い包み (影) では取り込まない');
  return occ;
}

/**
 * 1 回取り込む
 * @param {object} p
 * @param {object} p.policy      POLICIES のどれか
 * @param {string} p.runsDir     実行の記録を置くフォルダ (<runsDir>/<実行 ID>/)
 * @param {Buffer} p.csvBuf      取り込む CSV (呼び手が承認と照らしたもの)
 * @param {{ sha256, rows, target_as_of, source_run_id }} p.csv  取り込む CSV の識別 (importing に書く)
 * @param {object} p.context     決まりごとの呼び手の値 (CONTEXTS。試験 = { planId, planSha256, plan })
 * @param {object} p.client      import-state-client (acquire / extend / release / transition / status / notified)
 * @param {(client, localFile) => Promise<{ ok, reason, status }>} p.checkInit
 * @param {(fn) => Promise} p.withSession  ops = { exportShohin, exportBarcodes, previewImport, executeImport }
 * @param {(text: string) => Promise<boolean>} p.notify  GChat (送れた = true)
 * @param {Function} p.createGuard  import-guard.js の createGuard
 */
export async function importOne({ policy, lzMinRows = 4000, runsDir, csvBuf, csv, context, occupancy, now = new Date(), localInitFile, client, checkInit, withSession, capabilities, notify, createGuard, log = console.log, heartbeatMs = 30000, writeJson = writeJsonAtomic, nightMarginMs = 60000, save = saveOnce }) {
  const occ = preflight({ policy, now, occupancy, capabilities });
  const table0 = checkCsv(csvBuf, csv);
  const { tag, recExtra, notifyTail, preCheck, extraIds } = CONTEXTS[policy.name].import(context, { csvBuf, csv });
  const init = await checkInit(client, localInitFile);
  if (!init.ok) throw new Error(`ポータルの初期化の照合が合わない (${init.reason})`);

  // この回の時計 (now + 単調な時計の経過) と、ポータルの期限の写し方 (runClock)
  const { clock, toClock } = runClock(policy, now);
  // 締め切り = 旗の上限 (test = 次の 00:00 の nightMarginMs 前 (始めた後に 00:00 を迎えても押さない。Codex #1524 R1 High) / nightly = その日の 00:55 − 余白)
  const nightCap = deadlineAt(policy, now.getTime(), nightMarginMs);

  const prevState = init.status.state;
  const runId = newRunId(now, policy);
  const runDir = path.join(runsDir, runId);
  fs.mkdirSync(runDir, { recursive: true });
  const rec = { run_id: runId, ...recExtra, mode: policy.mode, started_at: now.toISOString(), occupancy: occ, stages: [], record_errors: [] };
  // 記録: 押す前 (required) は書けない = 止める (D)・押した後は書けなくても状態の書き込みと知らせは続ける (Codex #1524 R1 Medium)
  const stage = (name, extra = {}, { required = false } = {}) => {
    rec.stages.push({ name, at: new Date().toISOString(), ...extra });
    rec.stage = name;
    try { writeJson(path.join(runDir, 'import.json'), rec); return true; } catch (e) {
      rec.record_errors.push({ name, error: String(e && e.message).slice(0, 200) });
      if (required) throw new Error(`記録を書けない (${name}) = 押さない: ${String(e && e.message).slice(0, 120)}`);
      return false;
    }
  };
  stage('begin', {}, { required: true });

  // 鍵 (持ち主・import)。応答が分からない = token が分からない = 止める (K5)
  const acq = await portalWrite(() => client.acquire({ init_id: init.status.init_id, holder: policy.holder, purpose: 'import', run_id: runId, ttl_sec: policy.importTtlSec, by: policy.by }),
    { expect: (x) => typeof x.lock_token === 'string' && Number.isFinite(x.expires_at) });
  if (acq.outcome !== 'ok' || acq.confirmed) { stage('lock_not_acquired', { outcome: acq.outcome, code: acq.code || null }); throw new Error(`鍵を取れない (${acq.outcome}${acq.code ? `・${acq.code}` : ''})`); }
  const lockToken = acq.res.lock_token;
  const deadlineOf = (expiresAt) => Math.min(toClock(expiresAt) - 20000, nightCap);
  const guard = createGuard({ now: clock, deadlineMs: deadlineOf(acq.res.expires_at) });
  const hb = startHeartbeat({ client, lockToken, guard, everyMs: heartbeatMs, mapDeadline: deadlineOf, onEvent: (e) => { rec.heartbeat = e; } });
  let state = null, executeIssued = false;
  const notWritten = [];
  // 状態を進める (K5): 成功 = 行き先の状態の応答・応答不明 = 状態を読み直して、この回の実行 ID・行き先・中身 (sha256・行数など) で照らす
  const move = async (to, detail = null) => {
    const r = await portalWrite(() => client.transition({ lock_token: lockToken, run_id: runId, to, detail, by: policy.by }), {
      // failed_before_execute = importing の前の状態に戻る (違う応答 = 状態を読み直して照らす)
      expect: (x) => x.state === (to === 'failed_before_execute' ? prevState : to),
      confirm: async () => {
        const s = await client.status(1);
        if (!s.run || s.run.run_id !== runId) return false;
        const d = s.run.detail || {};
        if (to === 'importing') return s.state === 'importing' && d.csv_sha256 === detail.csv_sha256 && d.rows === detail.rows && d.mode === policy.mode && Object.entries(tag).every(([k, v]) => d[k] === v);
        if (to === 'failed_before_execute') return s.state !== 'importing' && d.result === 'failed_before_execute';
        if (to === 'verified' || to === 'verify_failed') return s.state === to && d.verify === to;
        return s.state === to && d.result === to;
      },
    });
    stage(`state_${to}`, { outcome: r.outcome, confirmed: !!r.confirmed, code: r.code || null });
    if (r.outcome === 'ok') state = to === 'failed_before_execute' ? 'reverted' : to;
    else if (to !== 'importing') { notWritten.push(to); stage('result_not_written', { to, outcome: r.outcome }); }   // importing を書けない = 押さない (下)
    return r;
  };
  try {
    await withSession(async (ops) => {
      // ── 直前の書き出し (商品・バーコード) と照らし直し (K2・K4) ──
      const pre = await ops.exportShohin();
      saveOnce(path.join(runDir, 'pre.csv'), pre.buf);
      const lz = readLzShohinMaster(pre.buf, { minRows: lzMinRows });
      if (!lz.ok) throw new Error(`直前の一覧が読めない (${lz.reason})`);
      const preBc = await ops.exportBarcodes();
      saveOnce(path.join(runDir, 'pre-barcode.csv'), preBc.buf);
      const bcPre = readBarcodeExport(preBc.buf);
      if (!bcPre.ok) throw new Error(`直前のバーコードが読めない (${bcPre.reason}) = 押さない (K4)`);
      const bcMiss = barcodeMissing(lz, bcPre);   // 直前の商品マスタの全商品が直前のバーコードにある = 途中で切れていない (Codex #1530 R2 High)
      if (bcMiss.length) throw new Error(`直前のバーコードの書き出しに無い商品がある = 途中で切れた疑い = 押さない (K4): ${bcMiss.length} 件 (${bcMiss.slice(0, 5).join(', ')})`);
      // 取込の後に確かめられる形か (押す前に見る。Codex #1530 R4 Medium): 商品ごとの行がひとまとまり・比べる商品が最後の商品でない
      const checkIds = new Set([...table0.map((r) => r[0]), ...extraIds]);
      if (bcPre.grouped === false) throw new Error('直前のバーコードの書き出しで商品ごとの行がひとまとまりでない = 取込の後に確かめられない = 押さない (K4)');
      if (checkIds.has(bcPre.lastId)) throw new Error(`比べる商品 ${bcPre.lastId} がバーコードの書き出しの最後の商品 = 取込の後に確かめられない = 押さない (K4・この商品を試験から外す)`);
      const again = preCheck(lz);
      if (again) { stage(...again.stage); throw new Error(again.error); }
      // ── 押す前にそろえる記録 (D) ──
      const importCsv = path.join(runDir, 'import.csv');
      saveOnce(importCsv, csvBuf);
      if (sha256(fs.readFileSync(importCsv)) !== csv.sha256) throw new Error('import.csv を書いたが sha256 が違う');
      rec.files = { pre: sha256(pre.buf), pre_barcode: sha256(preBc.buf), import_csv: csv.sha256 };
      stage('prepared', {}, { required: true });
      await ops.previewImport(importCsv, { captureDir: runDir });
      stage('previewed', {}, { required: true });
      guard.check('importing の前');
      // ── importing (書けない = 押さない) ──
      const imp = await move('importing', { csv_sha256: csv.sha256, rows: csv.rows, mode: policy.mode, target_as_of: csv.target_as_of, source_run_id: csv.source_run_id, ...tag });
      if (imp.outcome !== 'ok') throw new Error(`importing を書けない (${imp.outcome}) = 押さない`);
      // ── 押す (K7: 押す直前に記録。記録を書けない = 押さない) ──
      let exec;
      try {
        exec = await ops.executeImport({ guard, onExecuteIssued: () => { stage('execute_issued', {}, { required: true }); executeIssued = true; }, captureDir: runDir, log });
      } catch (e) {
        if (e && e.executeIssued) { stage('execute_error', { error: String(e.message).slice(0, 300), after_stop: e.afterStop || null }); await move('unknown', { reason: 'execute_error', error: String(e.message).slice(0, 200) }); return; }
        stage('execute_not_issued', { error: String(e && e.message).slice(0, 300) });
        await move('failed_before_execute', { reason: String(e && e.message).slice(0, 200) });
        return;
      }
      // after_stop = 止めた後に押された (ページに送った後の押す処理は取り消せない。#1521)
      rec.execute = { confirm: exec.confirm, reason: exec.reason, after_stop: exec.afterStop || null, result_text: exec.resultText ? exec.resultText.slice(0, 2000) : null };
      const parsed = exec.reason ? { found: false, reason: exec.reason } : parseImportResult(exec.resultText);
      const judged = judgeImportResult(parsed, csv.rows);
      rec.result = { parsed, judged };
      const w = await move(judged.to, { why: judged.why, total: parsed.total ?? null, processed: parsed.processed ?? null, noop: parsed.noop ?? null, errors: parsed.errors ?? null });
      if (w.outcome !== 'ok') return;   // 手元に残した・知らせる (下)
      if (judged.to === 'unknown') return;
      // ── 直後の書き出しと確かめ (K4: 商品 + バーコードの両方を比べてから verified) ──
      const g = await grabPostExports(ops, { save, files: { shohin: path.join(runDir, 'post.csv'), barcode: path.join(runDir, 'post-barcode.csv') } });
      for (const x of g.saveErrors) rec.record_errors.push({ name: x.file, error: x.error });
      // 中身が壊れていた (invalid_csv) = 読めない = 確かめの失敗 / 一時の失敗 (通信・ログイン・時間切れ) だけ = 未確かめのまま (verify でやり直す。取れた側は保存した。Codex #1524 R2・R3)
      if (g.invalid) stage('post_export_invalid', g.invalid);
      const bad = (which) => (g.invalid && g.invalid.which === which ? `書き出しの中身が壊れている (${g.invalid.reason})` : `書き出せなかった (${g.transient ? g.transient.error : '-'})`);
      const table = table0;
      const ids = new Set([...table.map((r) => r[0]), ...extraIds]);
      // 取れた側 (と中身の壊れ) は比べる。一時の失敗で取れなかった側 = null (Codex #1524 R4)
      const lzPost = g.post ? readLzShohinMaster(g.post.buf, { minRows: lzMinRows }) : null;
      const bcPost = g.postBc ? readBarcodeExport(g.postBc.buf) : null;
      const vr0 = lzPost ? (lzPost.ok ? verifyImport({ table, pre: lz, post: lzPost, rules: policy.rules }) : { ok: false, diffs: [{ kind: 'post_unreadable', reason: lzPost.reason }] })
        : (g.invalid && g.invalid.which === 'shohin' ? { ok: false, diffs: [{ kind: 'post_unreadable', reason: bad('shohin') }] } : null);
      const br0 = bcPost ? (bcPost.ok ? compareBarcodes({ pre: bcPre, post: bcPost, ids, cover: { pre: lz, post: lzPost && lzPost.ok ? lzPost : null } }) : { ok: false, diffs: [{ kind: 'post_barcode_unreadable', reason: bcPost.reason }] })
        : (g.invalid && g.invalid.which === 'barcode' ? { ok: false, diffs: [{ kind: 'post_barcode_unreadable', reason: bad('barcode') }] } : null);
      // 片方が一時の失敗で取れず、取れた側に差も壊れも無い = 確かめきれない = 未確かめのまま (verify でやり直す) / 取れた側に差・壊れ = verify_failed
      if ((!vr0 || !br0) && !((vr0 && !vr0.ok) || (br0 && !br0.ok))) { stage('post_export_failed', g.transient || {}); return; }
      const vr = vr0 || { ok: false, diffs: [{ kind: 'post_not_exported', reason: bad('shohin') }] };
      const br = br0 || { ok: false, diffs: [{ kind: 'post_barcode_not_exported', reason: bad('barcode') }] };
      try { writeJson(path.join(runDir, 'verify.json'), { product: vr, barcode: br }); } catch (e) { rec.record_errors.push({ name: 'verify.json', error: String(e && e.message).slice(0, 200) }); }
      rec.verify = { ok: vr.ok && br.ok, decided: vr.decided, rules_version: vr.rules_version, product_diffs: vr.diffs.length, barcode_diffs: br.diffs.length };
      stage('verified_checked');
      if (judged.to === 'imported_unverified') await move(vr.ok && br.ok ? 'verified' : 'verify_failed', rec.verify);
      // partial = 状態は partial のまま (差は verify.json = 解除の材料。H)
    });
  } catch (e) {
    rec.error = String(e && e.message).slice(0, 300);
    stage('error', { error: rec.error, execute_issued: executeIssued });
    // importing を書いた後で押す前の失敗 = failed_before_execute / 押した後 = unknown
    if (state === 'importing') await move(executeIssued ? 'unknown' : 'failed_before_execute', { reason: rec.error.slice(0, 200) }).catch(() => { notWritten.push('after_error'); });
  } finally {
    hb.stop();
    const rel = await portalWrite(() => client.release({ lock_token: lockToken, by: policy.by })).catch(() => ({ outcome: 'unknown' }));
    stage('released', { outcome: rel.outcome });
  }
  // ── 知らせ (K9) = 止まった状態・ポータルに書けなかった・押した後の記録を書けなかった ──
  const final = await client.status(1).catch(() => null);
  const stoppedState = final && STOP_STATES.includes(final.state) && final.run && final.run.run_id === runId ? final.state : (state && STOP_STATES.includes(state) ? state : null);
  const stuck = final && final.state === 'importing' && final.run && final.run.run_id === runId;
  if (stoppedState || notWritten.length || stuck || (executeIssued && rec.record_errors.length)) {
    const why = [stoppedState ? `状態 ${stoppedState}` : null, notWritten.length ? `ポータルに書けない (${notWritten.join('・')})` : null, stuck ? 'ポータルは importing のまま' : null,
      rec.record_errors.length ? `記録を書けない (${rec.record_errors.map((x) => x.name).join('・')})` : null].filter(Boolean).join('・');
    const text = `⚠️ ロジザードの${policy.label} ${runId} が止まった: ${why}${notifyTail}。記録 = ${runDir}\n解除は人 (ロジザードのインポート履歴を確かめてから import-state-cli.js resolve / mark-unknown)`;
    const sent = await notify(text).catch(() => false);
    rec.notified = sent;
    if (sent && final && final.state_event_id != null && STOP_STATES.includes(final.state)) await portalWrite(() => client.notified({ run_id: runId, state: final.state, state_event_id: final.state_event_id, by: policy.by })).catch(() => {});
    stage('notified', { sent });
  }
  // 返す状態 = この回が書けた結末だけ (前の回の verified・押す前の失敗で戻った前の状態を、この回の成功と返さない。Codex #1524 R2)
  const result = state === 'reverted' ? 'failed_before_execute' : (state || 'not_started');
  return { runId, runDir, state: result, portalState: final ? final.state : null, record: rec };
}

/**
 * 直後の書き出し (商品・バーコード) を 1 つずつ取る。片方が失敗してももう片方は取る (partial の戻しの証跡・K4)。取れたものはすぐ保存 (保存の失敗は saveErrors に残して続ける)。
 * invalid = 中身が壊れていた (最初の 1 つ) / transient = 一時の失敗 (最初の 1 つ)。(Codex #1524 R3)
 */
export async function grabPostExports(ops, { save, files, before = null }) {
  const g = { post: null, postBc: null, invalid: null, transient: null, saveErrors: [] };
  for (const [key, which, fn, file] of [['post', 'shohin', ops.exportShohin, files.shohin], ['postBc', 'barcode', ops.exportBarcodes, files.barcode]]) {
    if (before) before(which);   // 書き出しの前の旗 (締め切り・鍵を失った = 例外 = 書き出さない。確かめのやり直し。③c-1b-2b-2 契約 v3)
    try { g[key] = await fn(); } catch (e) {
      if (isInvalidExport(e)) { if (!g.invalid) g.invalid = { which, reason: String(e.reason || e.message).slice(0, 200) }; }
      else if (!g.transient) g.transient = { which, error: String(e && e.message).slice(0, 300) };
      continue;
    }
    try { save(file, g[key].buf); } catch (e) { g.saveErrors.push({ file: path.basename(file), error: String(e && e.message).slice(0, 200) }); }
  }
  return g;
}

/** 書き出した CSV の中身が壊れていた (shohin-export.js の invalidCsvError・barcode-export.js も同じ印) = 一時の失敗ではない */
export const isInvalidExport = (e) => !!e && e.code === 'invalid_csv';

/**
 * 確かめのやり直し (imported_unverified の回だけ。K10・H)。取り込んだ CSV と直前の書き出しはその回の記録から (sha256 を照らす)
 * 順番 (③c-1b-2b-2 契約 v3): 時刻と決まり → ポータルの状態 (この回・この持ち主・この mode) → **確かめの鍵** → 記録を探して読む
 *   (無い = evidence_missing・違う回 = evidence_mismatch・sha256 が合わない = evidence_broken = どれも verify_failed = 人が見る) →
 *   鍵を 30 秒ごとに延ばす・締め切りの旗 (test = 次の 00:00 の前 / nightly = 00:55) を書き出しの前ごとに見る → 比べる → 結果
 * @param {object} p
 * @param {object} p.policy      POLICIES のどれか (その回と同じ持ち主)
 * @param {() => string} p.locateRun  実行の記録のフォルダ (見つからない = 例外 = evidence_missing)
 * @param {object} p.context     決まりごとの呼び手の値 (CONTEXTS。試験 = { readPlan: (runDir) => その回の plan.json } / 毎晩 = {})
 */
export async function verifyAgain({ policy, lzMinRows = 4000, runId, locateRun, context, occupancy, now = new Date(), localInitFile, client, checkInit, withSession, capabilities, notify, createGuard = defaultCreateGuard, log = console.log, writeJson = writeJsonAtomic, save = saveOnce, heartbeatMs = 30000, nightMarginMs = 60000 }) {
  const occ = preflight({ policy, now, occupancy, capabilities, purpose: 'verify' });
  const { readExtraIds } = CONTEXTS[policy.name].verify(context);
  const { clock, toClock } = runClock(policy, now);
  const nightCap = deadlineAt(policy, now.getTime(), nightMarginMs);
  const init = await checkInit(client, localInitFile);
  if (!init.ok) throw new Error(`ポータルの初期化の照合が合わない (${init.reason})`);
  const st = init.status;
  if (st.state !== 'imported_unverified' || !st.run || st.run.run_id !== runId || st.run.by !== policy.holder) throw new Error(`確かめをやり直せる状態でない (今 = ${st.state}・${st.run ? st.run.run_id : '-'})`);
  // 同じ持ち主 (auto) でも mode が違う回は、この決まりでは確かめない (Codex #1535 R1 Medium)
  const runMode = st.run.detail && st.run.detail.mode;
  if (runMode !== policy.mode) throw new Error(`確かめをやり直せる状態でない (その回の mode = ${runMode}・この決まり = ${policy.mode})`);
  const acq = await portalWrite(() => client.acquire({ init_id: st.init_id, holder: policy.holder, purpose: 'verify', run_id: runId, ttl_sec: policy.verifyTtlSec, by: policy.by }),
    { expect: (x) => typeof x.lock_token === 'string' && Number.isFinite(x.expires_at) });
  if (acq.outcome !== 'ok' || acq.confirmed) throw new Error(`確かめの鍵を取れない (${acq.outcome}${acq.code ? `・${acq.code}` : ''})`);
  const lockToken = acq.res.lock_token;
  // 鍵の延長と締め切りの旗 (確かめの終わりまで。延ばせない・締め切り = 書き出しを始めない)
  const deadlineOf = (expiresAt) => Math.min(toClock(expiresAt) - 20000, nightCap);
  const guard = createGuard({ now: clock, deadlineMs: deadlineOf(acq.res.expires_at) });
  const hbEvents = [];
  const hb = startHeartbeat({ client, lockToken, guard, ttlSec: policy.verifyTtlSec, everyMs: heartbeatMs, mapDeadline: deadlineOf, onEvent: (e) => { hbEvents.push(e); } });
  let rec = null, runDir = null;
  const stageErrors = new Set();   // 書けなかった import.json (知らせに足す。Codex #1524 R4)
  const stage = (name, extra = {}, { required = false } = {}) => {
    if (!rec) return;
    rec.stages.push({ name, at: new Date().toISOString(), ...extra }); rec.stage = name;
    try { writeJson(path.join(runDir, 'import.json'), rec); } catch (e) { if (required) throw e; stageErrors.add(`import.json: ${String(e && e.message).slice(0, 200)}`); }
  };
  const move = async (to, detail) => {
    const r = await portalWrite(() => client.transition({ lock_token: lockToken, run_id: runId, to, detail, by: policy.by }),
      { expect: (x) => x.state === to, confirm: async () => { const s2 = await client.status(1); return !!(s2.run && s2.run.run_id === runId && s2.state === to && s2.run.detail && s2.run.detail.verify === to); } });
    stage(`verify_only_${to}`, { outcome: r.outcome, confirmed: !!r.confirmed });
    return { ...r, to };
  };
  const RV = policy.rules.version;
  let result = null, recordError = null, failure = null;
  try {
    // ── 記録を探して読む (鍵を取った後 = 無い・違う回でも verify_failed を書ける) ──
    let evidence = null;
    try { runDir = locateRun(); } catch (e) { evidence = { reason: 'evidence_missing', error: String(e && e.message).slice(0, 200) }; }
    if (!evidence) {
      try { rec = JSON.parse(fs.readFileSync(path.join(runDir, 'import.json'), 'utf8')); } catch (e) { rec = null; evidence = { reason: 'evidence_missing', error: `import.json を読めない: ${String(e && e.message).slice(0, 160)}` }; }
    }
    // 記録がこの回・この決まりの回か (違う決まりの確かめの決まりで verified にしない。Codex #1535 R1 Medium)
    if (!evidence && (!rec || rec.run_id !== runId || rec.mode !== policy.mode || !Array.isArray(rec.stages))) {
      evidence = { reason: 'evidence_mismatch', record: rec ? { run_id: rec.run_id ?? null, mode: rec.mode ?? null } : null };
      rec = null;   // 違う回の記録には書かない
    }
    if (evidence) {
      result = await move('verify_failed', evidence);
      return result.outcome === 'ok' ? { runId, state: 'verify_failed', reason: evidence.reason } : { runId, state: 'imported_unverified', reason: 'result_not_written', compared: evidence.reason };
    }
    stage('verify_only_begin', { occupancy: occ }, { required: true });
    // 記録を照らす (壊れた・sha256 が違う = 比べられない = verify_failed。H)
    const read = (f) => { try { return fs.readFileSync(path.join(runDir, f)); } catch { return null; } };
    const importCsv = read('import.csv'), preBuf = read('pre.csv'), preBcBuf = read('pre-barcode.csv');
    const broken = [];
    if (!importCsv || sha256(importCsv) !== (rec.files && rec.files.import_csv)) broken.push('import.csv');
    if (!preBuf || sha256(preBuf) !== (rec.files && rec.files.pre)) broken.push('pre.csv');
    if (!preBcBuf || sha256(preBcBuf) !== (rec.files && rec.files.pre_barcode)) broken.push('pre-barcode.csv');
    if (broken.length) {
      result = await move('verify_failed', { reason: 'evidence_broken', files: broken });
      return result.outcome === 'ok' ? { runId, state: 'verify_failed', reason: 'evidence_broken' } : { runId, state: 'imported_unverified', reason: 'result_not_written', compared: 'evidence_broken' };
    }
    const table = validateImportCsv(importCsv).table;
    const lz = readLzShohinMaster(preBuf, { minRows: lzMinRows }), bcPre = readBarcodeExport(preBcBuf);
    const ids = new Set([...table.map((r) => r[0]), ...readExtraIds(runDir, rec)]);
    const tag = `${new Date().toISOString().replace(/[-:.]/g, '').slice(0, 18)}_${crypto.randomBytes(3).toString('hex')}`;   // 続けてやり直しても名前がぶつからない
    // セッションを始める前・各書き出しの前に旗を見る (締め切り・鍵を失った = 書き出さない = 未確かめのまま)
    const beforeExport = (which) => guard.check(`確かめの書き出し (${which}) の前`);
    let got;
    try {
      guard.check('確かめのセッションを始める前');
      // 1 つずつ取る・取れたものはすぐ保存 (保存の失敗は知らせて続ける = 壊れの判定まで進む。Codex #1524 R3)
      got = await withSession((ops) => grabPostExports(ops, { save, before: beforeExport, files: { shohin: path.join(runDir, `post-${tag}.csv`), barcode: path.join(runDir, `post-barcode-${tag}.csv`) } }));
    } catch (e) {
      stage('verify_only_export_failed', { error: String(e && e.message).slice(0, 300), stopped: e && e.stopped ? e.reason : null });
      return { runId, state: 'imported_unverified', reason: e && e.stopped ? `stopped_${e.reason}` : 'post_export_failed' };   // 一時の失敗・締め切り・鍵を失った = 未確かめのまま
    }
    if (got.saveErrors.length) { recordError = got.saveErrors.map((x) => `${x.file}: ${x.error}`).join('・'); stage('verify_only_record_failed', { error: recordError }); }
    if (got.invalid) stage('verify_only_export_invalid', got.invalid);
    const bad = (which) => (got.invalid && got.invalid.which === which ? `書き出しの中身が壊れている (${got.invalid.reason})` : `書き出せなかった (${got.transient ? got.transient.error : '-'})`);
    // 取れた側 (と中身の壊れ) は比べる。一時の失敗で取れなかった側 = null (Codex #1524 R4)
    const lzPost = got.post ? readLzShohinMaster(got.post.buf, { minRows: lzMinRows }) : null;
    const bcPost = got.postBc ? readBarcodeExport(got.postBc.buf) : null;
    const vr0 = lzPost ? (lz.ok && lzPost.ok ? verifyImport({ table, pre: lz, post: lzPost, rules: policy.rules }) : { ok: false, diffs: [{ kind: 'unreadable', reason: lz.ok ? lzPost.reason : lz.reason }], decided: false, rules_version: RV })
      : (got.invalid && got.invalid.which === 'shohin' ? { ok: false, diffs: [{ kind: 'unreadable', reason: bad('shohin') }], decided: false, rules_version: RV } : null);
    const br0 = bcPost ? (bcPre.ok && bcPost.ok ? compareBarcodes({ pre: bcPre, post: bcPost, ids, cover: { pre: lz.ok ? lz : null, post: lzPost && lzPost.ok ? lzPost : null } }) : { ok: false, diffs: [{ kind: 'barcode_unreadable', reason: bcPre.ok ? bcPost.reason : bcPre.reason }] })
      : (got.invalid && got.invalid.which === 'barcode' ? { ok: false, diffs: [{ kind: 'barcode_unreadable', reason: bad('barcode') }] } : null);
    if ((!vr0 || !br0) && !((vr0 && !vr0.ok) || (br0 && !br0.ok))) {   // 片方が一時の失敗で取れず、取れた側に差も壊れも無い = 未確かめのまま
      stage('verify_only_export_failed', got.transient || {});
      return { runId, state: 'imported_unverified', reason: 'post_export_failed' };
    }
    const vr = vr0 || { ok: false, diffs: [{ kind: 'not_exported', reason: bad('shohin') }], decided: false, rules_version: RV };
    const br = br0 || { ok: false, diffs: [{ kind: 'barcode_not_exported', reason: bad('barcode') }] };
    // 記録を書けなくても、比べた結果の状態の書き込みと知らせは続ける (Codex #1524 R2)
    try { writeJson(path.join(runDir, `verify-${tag}.json`), { product: vr, barcode: br, note: '取り込んだ後に時間が経っている = 人の直しと区別がつかない (差があれば人が見る)' }); }
    catch (e) { recordError = [recordError, `verify-${tag}.json: ${String(e && e.message).slice(0, 200)}`].filter(Boolean).join('・'); stage('verify_only_record_failed', { error: recordError }); }
    const ok = vr.ok && br.ok;
    // 結果を書く前に旗を見る (記録に残す)。書き込みは同じ鍵 (token) のときだけポータルが通す = 鍵をほかが取っていれば断られる (止めない)
    stage('verify_only_before_result', { guard_stopped: guard.isStopped() ? guard.reason : null, heartbeat: hbEvents.slice(-3) });
    result = await move(ok ? 'verified' : 'verify_failed', { product_diffs: vr.diffs.length, barcode_diffs: br.diffs.length, decided: vr.decided, rules_version: vr.rules_version, late: true });
    // 結果をポータルに書けたかで返す (書けない = 未確かめのまま + 知らせ。比べた結果は手元の verify-*.json。Codex #1524 R1 High)
    if (result.outcome !== 'ok') return { runId, state: 'imported_unverified', reason: 'result_not_written', compared: ok ? 'verified' : 'verify_failed' };
    return { runId, state: ok ? 'verified' : 'verify_failed' };
  } catch (e) {
    failure = e;
    throw e;
  } finally {
    hb.stop();
    await portalWrite(() => client.release({ lock_token: lockToken, by: policy.by })).catch(() => {});
    const recErr = [recordError, ...stageErrors].filter(Boolean).join('・');
    const notes = [recErr ? `確かめの記録を書けない (${recErr})` : null, failure ? `途中で失敗: ${String(failure && failure.message).slice(0, 200)}` : null].filter(Boolean);
    const tail = notes.length ? `・${notes.join('・')}` : '';
    const where = runDir ? `記録 = ${runDir}` : '記録が見つからない';
    if (result && result.outcome === 'ok' && result.to === 'verify_failed') {
      const sent = await notify(`⚠️ ロジザードの${policy.label} ${runId} の確かめのやり直しが verify_failed${tail}。${where}`).catch(() => false);
      const eventId = result.res && result.res.state_event_id != null ? result.res.state_event_id : await client.status(1).then((x) => (x.state === 'verify_failed' && x.run && x.run.run_id === runId ? x.state_event_id : null)).catch(() => null);
      if (sent && eventId != null) await portalWrite(() => client.notified({ run_id: runId, state: 'verify_failed', state_event_id: eventId, by: policy.by })).catch(() => {});
    } else if (result && result.outcome !== 'ok') {
      await notify(`⚠️ ロジザードの${policy.label} ${runId} の確かめのやり直しの結果をポータルに書けない (未確かめのまま)${tail}。${where}`).catch(() => false);
    } else if (notes.length) {
      await notify(`⚠️ ロジザードの${policy.label} ${runId} の確かめのやり直し${tail}。${where}`).catch(() => false);
    }
  }
}
