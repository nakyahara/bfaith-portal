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
import { validateImportCsv, parseImportResult, judgeImportResult } from '../../apps/master-decisions/lz-import-check.mjs';
import { verifyImport, readBarcodeExport, compareBarcodes, RULES_2B1 } from '../../apps/master-decisions/lz-import-verify.mjs';
import { buildTestPlan, planSha256, checkPlanAgainstPre, checkTestCsv } from '../../apps/master-decisions/lz-import-test-plan.mjs';
import { portalWrite, startHeartbeat } from './portal-io.mjs';
import { fileURLToPath, pathToFileURL } from 'node:url';
import dotenv from 'dotenv';

export const EXIT = Object.freeze({ ok: 0, error: 1, stopped: 3 });
export const STOP_STATES = Object.freeze(['unknown', 'partial', 'verify_failed', 'imported_unverified']);
const sha256 = (b) => crypto.createHash('sha256').update(b).digest('hex');
const rand = () => crypto.randomBytes(3).toString('hex');
const stamp = (d) => d.toISOString().replace(/[-:.]/g, '').slice(0, 15);
export const newPlanId = (now = new Date()) => `lzt_${stamp(now)}_${rand()}`;
export const newRunId = (now = new Date()) => `lzim_test_${stamp(now)}_${rand()}`;

/** JST 00:00〜01:30 = 毎晩の取込の時間 = 試験は動かない */
export function inNightBlock(now = new Date()) {
  const d = new Date(now.getTime() + 9 * 3600 * 1000);
  const m = d.getUTCHours() * 60 + d.getUTCMinutes();
  return m < 90;
}

/** 一時ファイル → rename (書けない = 例外 = 進まない) */
export function writeJsonAtomic(file, obj) {
  const tmp = `${file}.${process.pid}.${rand()}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(obj, null, 1), { flag: 'wx' });
  fs.renameSync(tmp, file);
}
/** 1 回だけ書く (もうある = 例外) */
export const saveOnce = (file, buf) => fs.writeFileSync(file, buf, { flag: 'wx' });

function checkOccupancy(occupancy) {
  if (!occupancy || String(occupancy).trim().length < 8) throw new Error('--occupancy に、共通アカウントを使う人・作業が止まっていることを確かめた内容を書く (L-16・8 文字以上)');
  return String(occupancy).trim().slice(0, 300);
}

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
export async function runTest({ lzMinRows = 4000, dataDir, planId, sha256: approved, occupancy, now = new Date(), localInitFile, client, checkInit, withSession, capabilities, notify, createGuard, log = console.log, heartbeatMs = 30000, writeJson = writeJsonAtomic, nightMarginMs = 60000 }) {
  if (inNightBlock(now)) throw new Error('00:00〜01:30 は動かない (毎晩の取込の時間)');
  const occ = checkOccupancy(occupancy);
  const dir = path.join(dataDir, 'lz-import-test', String(planId));
  const plan = JSON.parse(fs.readFileSync(path.join(dir, 'plan.json'), 'utf8'));
  const { plan_id: _pid, created_at: _cat, occupancy: _occ, ...body } = plan;
  if (planSha256(body) !== approved) throw new Error('承認の印 (sha256) が計画と違う = 取り込まない (計画を作り直して承認し直す)');
  const testCsv = fs.readFileSync(path.join(dir, 'test.csv'));
  const tc = checkTestCsv(body, testCsv);
  if (!tc.ok) throw new Error(`取り込む CSV が承認した CSV と違う (${tc.reason})`);
  if (!capabilities || !capabilities.exportBarcodes) throw new Error('バーコードの書き出しの部品が無い = 試験はしない (K4)');
  const init = await checkInit(client, localInitFile);
  if (!init.ok) throw new Error(`ポータルの初期化の照合が合わない (${init.reason})`);

  // この回の時計 (試験では now を差し替える) と、ポータルの期限をこの回の時計に写す
  const t0 = Date.now();
  const clock = () => now.getTime() + (Date.now() - t0);
  const toClock = (serverMs) => clock() + (serverMs - Date.now());
  // 夜の止め: 次の 00:00 (JST) の nightMarginMs 前より後は押さない = 旗の締め切りの上限 (始めた後に 00:00 を迎えても押さない。Codex #1524 R1 High)
  const nightCap = nextNightStart(now) - nightMarginMs;

  const prevState = init.status.state;
  const runId = newRunId(now);
  const runDir = path.join(dir, 'runs', runId);
  fs.mkdirSync(runDir, { recursive: true });
  const rec = { run_id: runId, plan_id: planId, plan_sha256: approved, mode: 'test', started_at: now.toISOString(), occupancy: occ, stages: [], record_errors: [] };
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

  // 鍵 (auto・import)。応答が分からない = token が分からない = 止める (K5)
  const acq = await portalWrite(() => client.acquire({ init_id: init.status.init_id, holder: 'auto', purpose: 'import', run_id: runId, ttl_sec: 180, by: 'lz-import-test' }),
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
    const r = await portalWrite(() => client.transition({ lock_token: lockToken, run_id: runId, to, detail, by: 'lz-import-test' }), {
      // failed_before_execute = importing の前の状態に戻る (違う応答 = 状態を読み直して照らす)
      expect: (x) => x.state === (to === 'failed_before_execute' ? prevState : to),
      confirm: async () => {
        const s = await client.status(1);
        if (!s.run || s.run.run_id !== runId) return false;
        const d = s.run.detail || {};
        if (to === 'importing') return s.state === 'importing' && d.csv_sha256 === detail.csv_sha256 && d.rows === detail.rows && d.mode === 'test' && d.plan_id === planId;
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
      const again = checkPlanAgainstPre(body, lz);
      if (!again.ok) { stage('plan_changed', { diffs: again.diffs.slice(0, 50) }); throw new Error(`承認のときから一覧が変わった = 取り込まない (計画を作り直す): ${again.diffs.slice(0, 5).map((d) => `${d.kind}:${d.id}`).join(', ')}`); }
      // ── 押す前にそろえる記録 (D) ──
      const importCsv = path.join(runDir, 'import.csv');
      saveOnce(importCsv, testCsv);
      if (sha256(fs.readFileSync(importCsv)) !== body.test_csv.sha256) throw new Error('import.csv を書いたが sha256 が違う');
      rec.files = { pre: sha256(pre.buf), pre_barcode: sha256(preBc.buf), import_csv: body.test_csv.sha256 };
      stage('prepared', {}, { required: true });
      await ops.previewImport(importCsv, { captureDir: runDir });
      stage('previewed', {}, { required: true });
      guard.check('importing の前');
      // ── importing (書けない = 押さない) ──
      const imp = await move('importing', { csv_sha256: body.test_csv.sha256, rows: body.test_csv.rows, mode: 'test', target_as_of: body.source.as_of, source_run_id: body.source.run_id, plan_id: planId });
      if (imp.outcome !== 'ok') throw new Error(`importing を書けない (${imp.outcome}) = 押さない`);
      // ── 押す (K7: 押す直前に記録。記録を書けない = 押さない) ──
      let exec;
      try {
        exec = await ops.executeImport({ guard, onExecuteIssued: () => { stage('execute_issued', {}, { required: true }); executeIssued = true; }, captureDir: runDir, log });
      } catch (e) {
        if (e && e.executeIssued) { stage('execute_error', { error: String(e.message).slice(0, 300) }); await move('unknown', { reason: 'execute_error', error: String(e.message).slice(0, 200) }); return; }
        stage('execute_not_issued', { error: String(e && e.message).slice(0, 300) });
        await move('failed_before_execute', { reason: String(e && e.message).slice(0, 200) });
        return;
      }
      rec.execute = { confirm: exec.confirm, reason: exec.reason, result_text: exec.resultText ? exec.resultText.slice(0, 2000) : null };
      const parsed = exec.reason ? { found: false, reason: exec.reason } : parseImportResult(exec.resultText);
      const judged = judgeImportResult(parsed, body.test_csv.rows);
      rec.result = { parsed, judged };
      const w = await move(judged.to, { why: judged.why, total: parsed.total ?? null, processed: parsed.processed ?? null, noop: parsed.noop ?? null, errors: parsed.errors ?? null });
      if (w.outcome !== 'ok') return;   // 手元に残した・知らせる (下)
      if (judged.to === 'unknown') return;
      // ── 直後の書き出しと確かめ (K4: 商品 + バーコードの両方を比べてから verified) ──
      let post, postBc;
      try {
        post = await ops.exportShohin();
        saveOnce(path.join(runDir, 'post.csv'), post.buf);
        postBc = await ops.exportBarcodes();
        saveOnce(path.join(runDir, 'post-barcode.csv'), postBc.buf);
      } catch (e) {
        stage('post_export_failed', { error: String(e && e.message).slice(0, 300) });   // imported_unverified / partial のまま (verify でやり直す)
        return;
      }
      const lzPost = readLzShohinMaster(post.buf, { minRows: lzMinRows }), bcPost = readBarcodeExport(postBc.buf);
      const table = validateImportCsv(testCsv).table;
      const ids = new Set([...table.map((r) => r[0]), ...body.groups.flatMap((g) => g.ids)]);
      const vr = lzPost.ok ? verifyImport({ table, pre: lz, post: lzPost, rules: RULES_2B1 }) : { ok: false, diffs: [{ kind: 'post_unreadable', reason: lzPost.reason }] };
      const br = bcPost.ok ? compareBarcodes({ pre: bcPre, post: bcPost, ids }) : { ok: false, diffs: [{ kind: 'post_barcode_unreadable', reason: bcPost.reason }] };
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
    const rel = await portalWrite(() => client.release({ lock_token: lockToken, by: 'lz-import-test' })).catch(() => ({ outcome: 'unknown' }));
    stage('released', { outcome: rel.outcome });
  }
  // ── 知らせ (K9) = 止まった状態・ポータルに書けなかった・押した後の記録を書けなかった ──
  const final = await client.status(1).catch(() => null);
  const stoppedState = final && STOP_STATES.includes(final.state) && final.run && final.run.run_id === runId ? final.state : (state && STOP_STATES.includes(state) ? state : null);
  const stuck = final && final.state === 'importing' && final.run && final.run.run_id === runId;
  if (stoppedState || notWritten.length || stuck || (executeIssued && rec.record_errors.length)) {
    const why = [stoppedState ? `状態 ${stoppedState}` : null, notWritten.length ? `ポータルに書けない (${notWritten.join('・')})` : null, stuck ? 'ポータルは importing のまま' : null,
      rec.record_errors.length ? `記録を書けない (${rec.record_errors.map((x) => x.name).join('・')})` : null].filter(Boolean).join('・');
    const text = `⚠️ ロジザードの取込の試験 ${runId} が止まった: ${why}・計画 ${planId}。記録 = ${runDir}\n解除は人 (ロジザードのインポート履歴を確かめてから import-state-cli.js resolve / mark-unknown)`;
    const sent = await notify(text).catch(() => false);
    rec.notified = sent;
    if (sent && final && final.state_event_id != null && STOP_STATES.includes(final.state)) await portalWrite(() => client.notified({ run_id: runId, state: final.state, state_event_id: final.state_event_id, by: 'lz-import-test' })).catch(() => {});
    stage('notified', { sent });
  }
  return { runId, runDir, state: final ? final.state : state, record: rec };
}

/** 次の 00:00 (JST) の時刻 (ms) */
export function nextNightStart(now = new Date()) {
  const j = new Date(now.getTime() + 9 * 3600 * 1000);
  return Date.UTC(j.getUTCFullYear(), j.getUTCMonth(), j.getUTCDate() + 1) - 9 * 3600 * 1000;
}

/**
 * 本物のロジザードの操作 (試験用。miniPC の C:\tools\logizard-automation の部品・同じブラウザ・セッションの鍵を持ったまま)。
 * 影のランナー (lz-daily-import.mjs) の包みには押す部品を入れない (影は押さない = 試験で固定) ので、試験はここに専用の包みを持つ。
 * ops = exportShohin / previewImport / executeImport (lz-import-screen.js) / exportBarcodes (barcode-export.js があるときだけ・K4)
 * barcode-export.js の約束: exportBarcodeMaster(page, { dlDir, log }) → { buf } = バーコード情報の全件 (期間なし) を返すだけ (固定の出力先に書かない)
 */
export function realTestSession({ automationDir, label = '取込の試験' }) {
  return async (fn) => {
    const imp = (f) => import(pathToFileURL(path.join(automationDir, f)).href);
    const common = await imp('logizard-common.js');
    const { exportShohinMaster } = await imp('shohin-export.js');
    const screen = await imp('lz-import-screen.js');
    const barcode = fs.existsSync(path.join(automationDir, 'barcode-export.js')) ? await imp('barcode-export.js') : null;
    common.loadEnv();   // ロジザードの ID とパスワード (C:\tools\logizard-automation\.env。中身は読まない)
    common.assertLocalWriteDirs();
    if (!process.env.LOGIZARD_USER_ID || !process.env.LOGIZARD_PASSWORD) throw new Error('ロジザードの ID かパスワードが .env に無い');
    common.acquireLock({ name: 'logizard-session.lock' });
    const releaseOnExit = () => { try { common.releaseLock(); } catch { /* */ } };
    process.once('exit', releaseOnExit);
    try {
      const headless = (process.env.LOGIZARD_HEADLESS || '0') === '1';
      const { browser, page } = await common.launchBrowser({ headless });
      try {
        await common.login(page, { label });
        const dlDir = path.join(automationDir, 'downloads');
        return await fn({
          exportShohin: () => exportShohinMaster(page, { dlDir, minRows: 100 }),
          previewImport: (csvPath, o = {}) => screen.previewImport(page, { csvPath, ...o }),
          executeImport: (o) => screen.executeImport(page, o),
          ...(barcode ? { exportBarcodes: () => barcode.exportBarcodeMaster(page, { dlDir }) } : {}),
        });
      } finally {
        await browser.close().catch(() => {});
      }
    } finally {
      common.releaseLock();
      process.removeListener('exit', releaseOnExit);
    }
  };
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
export async function verifyOnly({ lzMinRows = 4000, dataDir, runId, occupancy, now = new Date(), localInitFile, client, checkInit, withSession, capabilities, notify, log = console.log, writeJson = writeJsonAtomic }) {
  if (inNightBlock(now)) throw new Error('00:00〜01:30 は動かない (毎晩の取込の時間)');
  const occ = checkOccupancy(occupancy);
  if (!capabilities || !capabilities.exportBarcodes) throw new Error('バーコードの書き出しの部品が無い = 確かめはしない (K4)');
  const runDir = findRunDir(dataDir, runId);
  const rec = JSON.parse(fs.readFileSync(path.join(runDir, 'import.json'), 'utf8'));
  // 記録: 始めの 1 つは書けない = 止める・後は書けなくても状態の書き込みと知らせは続ける (Codex #1524 R1 Medium)
  const stage = (name, extra = {}, { required = false } = {}) => {
    rec.stages.push({ name, at: new Date().toISOString(), ...extra }); rec.stage = name;
    try { writeJson(path.join(runDir, 'import.json'), rec); } catch (e) { if (required) throw e; }
  };
  const init = await checkInit(client, localInitFile);
  if (!init.ok) throw new Error(`ポータルの初期化の照合が合わない (${init.reason})`);
  const st = init.status;
  if (st.state !== 'imported_unverified' || !st.run || st.run.run_id !== runId || st.run.by !== 'auto') throw new Error(`確かめをやり直せる状態でない (今 = ${st.state}・${st.run ? st.run.run_id : '-'})`);
  const acq = await portalWrite(() => client.acquire({ init_id: st.init_id, holder: 'auto', purpose: 'verify', run_id: runId, ttl_sec: 300, by: 'lz-import-test' }),
    { expect: (x) => typeof x.lock_token === 'string' });
  if (acq.outcome !== 'ok' || acq.confirmed) throw new Error(`確かめの鍵を取れない (${acq.outcome}${acq.code ? `・${acq.code}` : ''})`);
  const lockToken = acq.res.lock_token;
  const move = async (to, detail) => {
    const r = await portalWrite(() => client.transition({ lock_token: lockToken, run_id: runId, to, detail, by: 'lz-import-test' }),
      { expect: (x) => x.state === to, confirm: async () => { const s2 = await client.status(1); return !!(s2.run && s2.run.run_id === runId && s2.state === to && s2.run.detail && s2.run.detail.verify === to); } });
    stage(`verify_only_${to}`, { outcome: r.outcome, confirmed: !!r.confirmed });
    return { ...r, to };
  };
  let result = null;
  try {
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
    const plan = JSON.parse(fs.readFileSync(path.join(runDir, '..', '..', 'plan.json'), 'utf8'));
    const ids = new Set([...table.map((r) => r[0]), ...plan.groups.flatMap((g) => g.ids)]);
    const got = await withSession(async (ops) => {
      const post = await ops.exportShohin();
      const postBc = await ops.exportBarcodes();
      return { post, postBc };
    }).catch((e) => { stage('verify_only_export_failed', { error: String(e && e.message).slice(0, 300) }); return null; });
    if (!got) return { runId, state: 'imported_unverified', reason: 'post_export_failed' };   // 一時の失敗 = 未確かめのまま
    const tag = `${new Date().toISOString().replace(/[-:.]/g, '').slice(0, 18)}_${crypto.randomBytes(3).toString('hex')}`;   // 続けてやり直しても名前がぶつからない
    saveOnce(path.join(runDir, `post-${tag}.csv`), got.post.buf);
    saveOnce(path.join(runDir, `post-barcode-${tag}.csv`), got.postBc.buf);
    const lzPost = readLzShohinMaster(got.post.buf, { minRows: lzMinRows }), bcPost = readBarcodeExport(got.postBc.buf);
    const vr = lz.ok && lzPost.ok ? verifyImport({ table, pre: lz, post: lzPost, rules: RULES_2B1 }) : { ok: false, diffs: [{ kind: 'unreadable' }], decided: false, rules_version: RULES_2B1.version };
    const br = bcPre.ok && bcPost.ok ? compareBarcodes({ pre: bcPre, post: bcPost, ids }) : { ok: false, diffs: [{ kind: 'barcode_unreadable' }] };
    writeJson(path.join(runDir, `verify-${tag}.json`), { product: vr, barcode: br, note: '取り込んだ後に時間が経っている = 人の直しと区別がつかない (差があれば人が見る)' });
    const ok = vr.ok && br.ok;
    result = await move(ok ? 'verified' : 'verify_failed', { product_diffs: vr.diffs.length, barcode_diffs: br.diffs.length, decided: vr.decided, rules_version: vr.rules_version, late: true });
    // 結果をポータルに書けたかで返す (書けない = 未確かめのまま + 知らせ。比べた結果は手元の verify-*.json。Codex #1524 R1 High)
    if (result.outcome !== 'ok') return { runId, state: 'imported_unverified', reason: 'result_not_written', compared: ok ? 'verified' : 'verify_failed' };
    return { runId, state: ok ? 'verified' : 'verify_failed' };
  } finally {
    await portalWrite(() => client.release({ lock_token: lockToken, by: 'lz-import-test' })).catch(() => {});
    if (result && result.outcome === 'ok' && result.to === 'verify_failed') {
      const sent = await notify(`⚠️ ロジザードの取込の試験 ${runId} の確かめのやり直しが verify_failed。記録 = ${runDir}`).catch(() => false);
      const eventId = result.res && result.res.state_event_id != null ? result.res.state_event_id : await client.status(1).then((x) => (x.state === 'verify_failed' && x.run && x.run.run_id === runId ? x.state_event_id : null)).catch(() => null);
      if (sent && eventId != null) await portalWrite(() => client.notified({ run_id: runId, state: 'verify_failed', state_event_id: eventId, by: 'lz-import-test' })).catch(() => {});
    } else if (result && result.outcome !== 'ok') {
      await notify(`⚠️ ロジザードの取込の試験 ${runId} の確かめのやり直しの結果をポータルに書けない (未確かめのまま)。記録 = ${runDir}`).catch(() => false);
    }
  }
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

/** GChat (リポジトリ直下の .env の GCHAT_WEBHOOK・https だけ)。送れた = true */
export async function sendGChat(text, { env = process.env, fetchImpl = fetch } = {}) {
  const hook = String(env.GCHAT_WEBHOOK || '').trim();
  if (!/^https:\/\//.test(hook)) return false;
  try {
    const res = await fetchImpl(hook, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ text: String(text).slice(0, 4000) }), signal: AbortSignal.timeout(15000) });
    return res.ok;
  } catch { return false; }
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
      const withSession = realTestSession({ automationDir });
      const capabilities = { exportBarcodes: fs.existsSync(path.join(automationDir, 'barcode-export.js')) };
      const localInitFile = path.join(dataDir, 'lz-import', 'init.json');
      if (a.cmd === 'plan') {
        await planTest({ dataDir, asOf: a.asOf, tests: a.tests, mapping: a.mapping, occupancy: a.occupancy, withSession });
        code = EXIT.ok;
      } else if (a.cmd === 'run') {
        const { createGuard } = await import(pathToFileURL(path.join(automationDir, 'import-guard.js')).href);
        const r = await runTest({ dataDir, planId: a.plan, sha256: a.sha256, occupancy: a.occupancy, localInitFile, client, checkInit, withSession, capabilities, notify, createGuard });
        console.log(`実行 ID ${r.runId}・状態 ${r.state}・記録 ${r.runDir}`);
        code = r.state === 'verified' ? EXIT.ok : EXIT.stopped;
      } else {
        const r = await verifyOnly({ dataDir, runId: a.run, occupancy: a.occupancy, localInitFile, client, checkInit, withSession, capabilities, notify });
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
