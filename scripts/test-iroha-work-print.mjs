/**
 * 🏷 保管箱ラベル印刷キュー — いろは在庫化作業アプリ (iPad) → いろはPC QL-800 (apps/iroha-work/print-queue.js)
 *
 * 実行: node scripts/test-iroha-work-print.mjs
 *
 * 守りたいのは値札 (scripts/test-inbound-check-print.mjs) と同じ 3 つ。
 *   ① 同じラベルが二重に出ない — lease した後は期限切れでも配り直さない / 冪等ID / 進行中は積めない / ❓のあとは実物確認の証跡
 *   ② 誰が刷れるか — 印刷ジョブは Authorization ヘッダーの kind='agent' 端末だけ。iPad Cookie では取れない。
 *      エージェントのトークンで作業画面には入れない。積めるのはアプリ正本 + 作業者を選んだ人だけ
 *   ③ 出ないことに誰も気づかない状態を作らない — 滞留は manual、報告なしは unknown、通知は送れるまで諦めない
 * DB 層 + 実 HTTP (express に router を mount) の両方で確かめる。
 */
import fs from 'fs';
import os from 'os';
import path from 'path';
import express from 'express';

if (!process.env.DATA_DIR) process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'iroha-print-'));
delete process.env.GCHAT_WEBHOOK_IROHA;

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };
const at = (base, sec) => new Date(Date.parse(base) + sec * 1000).toISOString();

const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const { default: router } = await import('../apps/iroha-work/router.js');
const { getDB, addIrohaWorker, setMetaValue, createDevice, verifyDevice, revokeDevice, listDevices, setAgentPrinter } = await import('../apps/iroha-work/db.js');
const { upsertTaskFromImport } = await import('../apps/iroha-work/tasks-db.js');
const { createTables: icCreateTables } = await import('../apps/inbound-check/db.js');
icCreateTables(getDB());
const pq = await import('../apps/iroha-work/print-queue.js');
const {
  enqueuePrintJob, leaseNextJob, markSubmitted, markFinished, getJobStatusFor, recordHeartbeat, sweepPrintJobs,
  pendingAlerts, markAlerted, alertTextFor, listPrintAgents, resolvePrintTarget, latestJobsByTask, listPrintJobs,
  barcodeTypeOf, publicJob, REPORT_DEADLINE_SEC, STALE_QUEUED_SEC, MAX_COPIES, MAX_EXPIRY_LEN,
} = pq;
const { printQueueTick, PRINT_JOB_ID } = await import('../apps/iroha-work/print-worker.js');
const { CAP, capabilitiesFor } = await import('../apps/iroha-work/capabilities.js');

setMetaValue('source_of_truth', 'app');
const db = getDB();
const stateOf = (id) => db.prepare('SELECT state FROM f_iroha_print_jobs WHERE id = ?').get(id)?.state;
const rowOf = (id) => db.prepare('SELECT * FROM f_iroha_print_jobs WHERE id = ?').get(id);

const W = addIrohaWorker({ displayName: 'たなか', workerType: 'member', actor: 'test' }).id;
let seq = 0;
const crid = () => `test-${Date.now().toString(36)}-${++seq}`;
const mkTask = (name, code, extra = {}) => upsertTaskFromImport({
  notion_page_id: 'print-' + code, status: 'not_started', facility_code: 'iroha',
  destination_id: null, product_code: code, product_name: name, qty: 200,
  arrival_date: '2026-09-02', barcode: 'X000T1GS6F', expiry: '2027-03',
  master_snapshot: { material_code: 'T10-15', units_per_container: 120 }, ...extra,
}, { batchId: 'test-print' }).id;
const T1 = mkTask('【木工用 60g】 みつろうクリーム 木工用 60g', '0726-001970');
const T2 = mkTask('ガラスボトル 500ml 6本セット', 'GB-500', { barcode: '4573473360422' });
const T_NOBC = mkTask('バーコード無し商品', 'NOBC', { barcode: null });
const T_BADBC = mkTask('記号入りバーコード', 'BADBC', { barcode: '4573-4733' });
const T_CLOSED = mkTask('終了したカード', 'CLOSED');
db.prepare("UPDATE f_iroha_tasks SET status = 'closed', close_reason = 'stocked', closed_at = ? WHERE id = ?").run(new Date().toISOString(), T_CLOSED);

console.log('\n[0] 許可 (capabilities)');
{
  ok(capabilitiesFor('app').includes(CAP.LABEL_PRINT), 'アプリ正本の利用者に 🏷 の許可がある');
  ok(!capabilitiesFor('notion').includes(CAP.LABEL_PRINT) && !capabilitiesFor('preview').includes(CAP.LABEL_PRINT), 'Notion 正本・下見には無い');
}

console.log('\n[1] バーコードの種別 (値札と同じ規則)');
{
  ok(barcodeTypeOf('4573473360422') === 'jan' && barcodeTypeOf('X000T1GS6F') === 'fnsku', '数字だけ=JAN / 英字を含む=FNSKU');
  ok(barcodeTypeOf('') === null && barcodeTypeOf(null) === null && barcodeTypeOf('4573-4733') === null, '空・記号入りは積まない');
}

console.log('\n[2] 印刷係 (エージェント端末) の登録');
let agentTok, agentId;
{
  let threw = null;
  try { createDevice('いろはPC', 'admin@test', { kind: 'agent' }); } catch (e) { threw = e.message; }
  ok(/プリンター名/.test(threw || ''), 'プリンター名なしでは登録できない');
  ok(resolvePrintTarget().ok === false && resolvePrintTarget().error === 'no_agent', '印刷係が 1 台も無ければ出力先は決まらない');
  const a = createDevice('いろはPC', 'admin@test', { kind: 'agent', printerName: ' Brother QL-800 ' });
  agentTok = a.token; agentId = a.id;
  const dev = verifyDevice(agentTok);
  ok(dev && dev.kind === 'agent' && dev.printer_name === 'Brother QL-800', '登録できた (プリンター名は前後の空白を除いて保存)');
  let dup = null;
  try { createDevice('いろはPC2', 'admin@test', { kind: 'agent', printerName: 'Brother QL-800' }); } catch (e) { dup = e.message; }
  ok(/登録済み/.test(dup || ''), '有効なエージェント同士で同じプリンター名は登録できない (どちらの実機か決められない)');
  const ipad = createDevice('いろはiPad', 'admin@test');
  ok(verifyDevice(ipad.token).kind === 'ipad', 'iPad は kind=ipad のまま (既定)');
  const list = listDevices();
  ok(list.some(d => d.id === agentId && d.kind === 'agent' && d.printer_name === 'Brother QL-800'), 'listDevices に種別とプリンター名が出る');
  const agents = listPrintAgents();
  ok(agents.length === 1 && agents[0].online === false && agents[0].bpac === null, '一覧: heartbeat が無いうちは応答なし');
  recordHeartbeat(agentId, { note: 'ready', version: 'v1', bpac: true, host: 'IROHA-PC', paperFormat: '箱ラベル 62x67', paperFormatOk: false });
  const a2 = listPrintAgents()[0];
  ok(a2.online === true && a2.bpac === true && a2.paper_ok === false && a2.host === 'IROHA-PC', 'heartbeat で 応答あり・b-PAC・用紙の有無が分かる');
  const tgt = resolvePrintTarget();
  ok(tgt.ok && tgt.agent.id === agentId, '1 台だけなら出力先は自動で決まる');
}

console.log('\n[3] 積む (入力の検査・カードの行から取る)');
let J1;
{
  const bad = [
    [{ taskId: T1, copies: 0, clientRequestId: crid() }, 'bad_copies'],
    [{ taskId: T1, copies: MAX_COPIES + 1, clientRequestId: crid() }, 'bad_copies'],
    [{ taskId: T1, copies: 2, packQty: '1.5', clientRequestId: crid() }, 'bad_pack_qty'],
    [{ taskId: T1, copies: 2, packQty: 0, clientRequestId: crid() }, 'bad_pack_qty'],
    [{ taskId: T1, copies: 2, expiry: 'x'.repeat(MAX_EXPIRY_LEN + 1), clientRequestId: crid() }, 'bad_expiry'],
    [{ taskId: T1, copies: 2, expiry: '2027\n03', clientRequestId: crid() }, 'bad_expiry'],
    [{ taskId: T1, copies: 2, clientRequestId: 'short' }, 'bad_request'],
    [{ taskId: 999999, copies: 2, clientRequestId: crid() }, 'not_found'],
    [{ taskId: T_NOBC, copies: 2, clientRequestId: crid() }, 'bad_barcode'],
    [{ taskId: T_BADBC, copies: 2, clientRequestId: crid() }, 'bad_barcode'],
    [{ taskId: T_CLOSED, copies: 2, clientRequestId: crid() }, 'closed_task'],
  ];
  for (const [p, err] of bad) {
    const r = enqueuePrintJob(p);
    ok(r.ok === false && r.error === err, `${err}: ${JSON.stringify({ ...p, clientRequestId: undefined })}`);
  }
  ok(db.prepare('SELECT COUNT(*) c FROM f_iroha_print_jobs').get().c === 0, 'ここまで 1 件も積まれていない');

  const id = crid();
  const r = enqueuePrintJob({ taskId: T1, copies: 2, packQty: '120', expiry: '2027-03', clientRequestId: id, requestedBy: 'たなか', requestedDevice: 'いろはiPad' });
  ok(r.ok && r.created && r.job.state === 'queued', '積めた (queued)');
  J1 = r.job.id;
  ok(r.job.product_name === '【木工用 60g】 みつろうクリーム 木工用 60g' && r.job.product_code === '0726-001970' && r.job.barcode === 'X000T1GS6F' && r.job.barcode_type === 'fnsku',
    '商品名・商品コード・バーコードはカードの行から (FNSKU と判定)');
  ok(r.job.pack_qty === '120' && r.job.expiry_text === '2027-03' && r.job.copies === 2 && r.job.printer_name === 'Brother QL-800', '1 箱に何個・期限・枚数・出力先が入る');
  ok(!('lease_token' in r.job), '画面に返す形に lease token は含めない');
  const again = enqueuePrintJob({ taskId: T1, copies: 2, packQty: '120', expiry: '2027-03', clientRequestId: id });
  ok(again.ok && again.replayed && again.job.id === J1, '同じ冪等 ID の再送は同じジョブ (2 枚出ない)');
  const clash = enqueuePrintJob({ taskId: T2, copies: 2, packQty: '120', expiry: '2027-03', clientRequestId: id });
  ok(clash.ok === false && clash.error === 'idempotency_conflict' && clash.job.id === J1, '同じ冪等 ID で違う内容 (別カード) は 409 相当 — 別のジョブを「積めた」と返さない (Codex PR #1220 R1 中)');
  const clash2 = enqueuePrintJob({ taskId: T1, copies: 3, packQty: '120', expiry: '2027-03', clientRequestId: id });
  const clash3 = enqueuePrintJob({ taskId: T1, copies: 2, packQty: '120', expiry: '2027-04', clientRequestId: id });
  ok(clash2.error === 'idempotency_conflict' && clash3.error === 'idempotency_conflict', '枚数・期限が違っても同じ');
  ok(db.prepare('SELECT COUNT(*) c FROM f_iroha_print_jobs').get().c === 1, '衝突しても新しいジョブは増えていない');
  const busy = enqueuePrintJob({ taskId: T1, copies: 1, clientRequestId: crid() });
  ok(busy.ok === false && busy.error === 'in_progress' && busy.job.id === J1, '進行中は同じカードに積めない');
  ok(latestJobsByTask().get(T1)?.id === J1 && !latestJobsByTask().has(T2), 'カードごとの最新ジョブ');
}

console.log('\n[4] lease → 投入 → 完了 (エージェント側の 1 本道)');
{
  const agent = verifyDevice(agentTok);
  const job = leaseNextJob(agent);
  ok(job && job.id === J1 && job.leaseToken && job.printerName === 'Brother QL-800', 'lease できた');
  ok(job.productName && job.barcode === 'X000T1GS6F' && job.barcodeType === 'fnsku' && job.packQty === '120' && job.expiry === '2027-03' && job.copies === 2 && job.taskId === T1,
    'ジョブ JSON = agent.ps1 が前提にする形 (+ expiry)');
  ok(leaseNextJob(agent) === null, '同じジョブは二度と配らない (leased 以降は拾わない)');
  ok(stateOf(J1) === 'leased', 'DB は leased');
  const st = getJobStatusFor(J1, agentId);
  ok(st && st.state === 'leased' && getJobStatusFor(J1, agentId + 99) === null, '状態照会は自分が lease した分だけ');
  const wrong = markSubmitted(J1, { deviceId: agentId, leaseToken: 'wrong', spoolJobId: '7' });
  ok(wrong.ok === false, '違う lease の投入報告は弾く');
  const sub = markSubmitted(J1, { deviceId: agentId, leaseToken: job.leaseToken, spoolJobId: '7' });
  ok(sub.ok && stateOf(J1) === 'submitted', '投入報告 → submitted');
  const sub2 = markSubmitted(J1, { deviceId: agentId, leaseToken: job.leaseToken, spoolJobId: '7' });
  ok(sub2.ok && sub2.replayed, '同じ投入報告の再送は成功 (replayed)');
  const conflict = markSubmitted(J1, { deviceId: agentId, leaseToken: job.leaseToken, spoolJobId: '8' });
  ok(conflict.ok === false && conflict.reason === 'submission_conflict', '違う spool_job_id の投入報告は 409 相当');
  const bad = markFinished(J1, { deviceId: agentId, leaseToken: job.leaseToken, ok: 'true' });
  ok(bad.ok === false && bad.reason === 'bad_ok', 'ok は真偽値でしか受け取らない');
  const done = markFinished(J1, { deviceId: agentId, leaseToken: job.leaseToken, ok: true });
  ok(done.ok && done.state === 'completed' && stateOf(J1) === 'completed', '完了報告 → completed');
  const done2 = markFinished(J1, { deviceId: agentId, leaseToken: job.leaseToken, ok: true });
  ok(done2.ok && done2.replayed, '同じ完了報告の再送は成功 (replayed)');
  const al = pendingAlerts();
  ok(al.length === 1 && al[0].id === J1 && /保管箱ラベル 2枚 を印刷しました/.test(alertTextFor(al[0])), '通知待ち 1 件 (✅ の文)');
  markAlerted(J1, 'completed');
  ok(pendingAlerts().length === 0, '通知済みにしたら消える');
  const more = enqueuePrintJob({ taskId: T1, copies: 1, clientRequestId: crid() });
  ok(more.ok && more.created, '完了していれば同じカードに追加で積める (人が判断して押す)');
  const j2 = leaseNextJob(agent);
  const f = markFinished(j2.id, { deviceId: agentId, leaseToken: j2.leaseToken, ok: false, error: 'this PC has no printer named X' });
  ok(f.ok && f.state === 'failed', '刷る前の失敗 (leased から ok:false) → failed (もう一度押してよい)');
  ok(/紙は出ていません/.test(alertTextFor(rowOf(j2.id))), '⚠ の文は「紙は出ていません」');
  markAlerted(j2.id, 'failed');
}

console.log('\n[5] 投入後の失敗・報告なし → unknown。❓ のあとは実物確認の証跡が要る');
let JU;
{
  const agent = verifyDevice(agentTok);
  const r = enqueuePrintJob({ taskId: T2, copies: 3, packQty: '24', clientRequestId: crid() });
  JU = r.job.id;
  const job = leaseNextJob(agent);
  markSubmitted(JU, { deviceId: agentId, leaseToken: job.leaseToken, spoolJobId: '9' });
  const f = markFinished(JU, { deviceId: agentId, leaseToken: job.leaseToken, ok: false, error: 'tape out', uncertain: false });
  ok(f.ok && f.state === 'unknown', '投入済み (submitted) からの失敗は uncertain に関係なく unknown (紙が出たかもしれない)');
  ok(/実物を確認/.test(alertTextFor(rowOf(JU))), '❓ の文は「実物を確認」');
  const no = enqueuePrintJob({ taskId: T2, copies: 1, clientRequestId: crid() });
  ok(no.ok === false && no.error === 'confirm_unknown' && no.job.id === JU, '❓ のまま証跡なしでは積めない');
  const wrongAck = enqueuePrintJob({ taskId: T2, copies: 1, clientRequestId: crid(), acknowledgeUnknownJobId: JU + 1000 });
  ok(wrongAck.ok === false && wrongAck.error === 'confirm_unknown', '別のジョブ ID の証跡は通らない');
  const yes = enqueuePrintJob({ taskId: T2, copies: 1, clientRequestId: crid(), acknowledgeUnknownJobId: JU });
  ok(yes.ok && yes.created && yes.job.acknowledged_job_id === JU, '「実物を見て出ていなかった」の証跡付きなら積める');
  const old = rowOf(JU);
  ok(old.lease_token === null && old.acknowledged_at, '旧ジョブの lease は消え、確認時刻が残る');
  const late = markFinished(JU, { deviceId: agentId, leaseToken: job.leaseToken, ok: true });
  ok(late.ok === false && stateOf(JU) === 'unknown', '再発行後に届いた旧 lease の遅延「刷れた」は受け付けない (2 枚になる穴)');
  const stale = enqueuePrintJob({ taskId: T1, copies: 1, clientRequestId: crid(), acknowledgeUnknownJobId: J1 });
  ok(stale.ok === false && stale.error === 'state_changed', '直前が ❓ でないのに証跡を付けて来たら state_changed (画面が古い)');
  // 新しいジョブ (yes) を lease → 期限切れ → unknown → 遅れて届いた同じ lease の報告は受け付ける (unknown → completed)
  const j3 = leaseNextJob(agent);
  ok(j3 && j3.id === yes.job.id, '再発行分を lease');
  const sw = sweepPrintJobs({ now: at(new Date().toISOString(), REPORT_DEADLINE_SEC + 5) });
  ok(sw.unknown === 1 && stateOf(j3.id) === 'unknown', '報告期限を過ぎたら unknown (自動では配り直さない)');
  ok(leaseNextJob(agent) === null, 'unknown になっても再配布しない');
  markSubmitted(j3.id, { deviceId: agentId, leaseToken: j3.leaseToken, spoolJobId: '10' });
  const lateOk = markFinished(j3.id, { deviceId: agentId, leaseToken: j3.leaseToken, ok: true });
  ok(lateOk.ok && lateOk.state === 'completed', '同じ lease の遅延報告 (再起動後の台帳突合) は unknown → completed に上書き');
}

console.log('\n[6] 誰も取りに来ない → manual (手で刷る)。🙋 のあとの再発行も証跡が要る / プリンター名の付け替え');
let JM;
{
  const r = enqueuePrintJob({ taskId: T1, copies: 1, clientRequestId: crid() });
  const sw = sweepPrintJobs({ now: at(new Date().toISOString(), STALE_QUEUED_SEC + 5) });
  ok(sw.manual === 1 && stateOf(r.job.id) === 'manual', `queued のまま ${STALE_QUEUED_SEC} 秒 → manual (手で刷る)`);
  ok(/自動印刷は取り消した/.test(alertTextFor(rowOf(r.job.id))), '🙋 の文は「自動印刷は取り消した」');
  const no = enqueuePrintJob({ taskId: T1, copies: 1, clientRequestId: crid() });
  ok(no.ok === false && no.error === 'confirm_manual' && no.job.id === r.job.id, '🙋 のまま証跡なしでは積めない (職員の手刷りと押し直しが競合して 2 枚になる — Codex PR #1220 R1 重要)');
  const yes = enqueuePrintJob({ taskId: T1, copies: 1, clientRequestId: crid(), acknowledgeUnknownJobId: r.job.id });
  ok(yes.ok && yes.created && yes.job.acknowledged_job_id === r.job.id && !!rowOf(r.job.id).acknowledged_at && /手で刷っていない/.test(rowOf(r.job.id).error || ''),
    '「手で刷っていない」の証跡付きなら積める (旧ジョブに確認時刻と理由が残る)');
  const ren = setAgentPrinter(agentId, 'Brother QL-800 (2)');
  ok(ren.ok && ren.cancelled === 1 && stateOf(yes.job.id) === 'manual', 'プリンター名を変えたら、その端末宛ての queued は manual に倒す (別のプリンターから出さない)');
  JM = yes.job.id;
  const back = setAgentPrinter(agentId, 'Brother QL-800');
  ok(back.ok && back.cancelled === 0, '名前を戻す (取り消す queued は無い)');
  const notAgent = setAgentPrinter(999999, 'X');
  ok(notAgent.ok === false && notAgent.error === 'not_agent', 'エージェントでない id は not_agent');
  const listed = listPrintJobs(5);
  ok(listed.length === 5 && listed[0].device_label === 'いろはPC', '管理画面用の一覧に端末名が付く');
}

console.log('\n[7] 見張り (通知は送れたときだけ通知済み・生存 ping の中継)');
{
  process.env.GCHAT_WEBHOOK_IROHA = 'https://example.invalid/hook';
  const sent = [];
  const before = pendingAlerts().length;
  ok(before >= 3, `通知待ちがある (${before} 件)`);
  const t1 = await printQueueTick({ notify: async (text) => { sent.push(text); return { sent: false, reason: 'down' }; }, ping: () => true });
  ok(t1.alerted === 0 && pendingAlerts().length === before, '送れなかった分は通知済みにしない (次の周期にまた送る)');
  const pings = [];
  const t2 = await printQueueTick({ notify: async (text) => { sent.push(text); return { sent: true }; }, ping: (id, note) => { pings.push([id, note]); return true; } });
  ok(t2.alerted === before && pendingAlerts().length === 0, '送れたら通知済み');
  ok(t2.pinged === true && pings[0][0] === PRINT_JOB_ID && /いろはPC/.test(pings[0][1]), `heartbeat が新しければ台帳 ${PRINT_JOB_ID} へ ok を中継`);
  delete process.env.GCHAT_WEBHOOK_IROHA;
  const r = enqueuePrintJob({ taskId: T1, copies: 1, clientRequestId: crid(), acknowledgeUnknownJobId: JM });   // 直前が 🙋 なので証跡付き
  const t3 = await printQueueTick({ notify: async () => { throw new Error('must not be called'); }, ping: () => false });
  ok(t3.alerted === 0, 'webhook 未設定なら通知しない (iPad の詳細に出るのが主経路)');
  markFinished(r.job.id, { deviceId: agentId, leaseToken: leaseNextJob(verifyDevice(agentTok)).leaseToken, ok: false, error: 'x' });
}

console.log('\n[8] HTTP: 誰が刷れるか (Bearer の kind=agent だけ) / iPad の積む口 / 管理画面');
{
  let sessionOn = true;
  const app = express();
  app.set('view engine', 'ejs');   // /admin は EJS (server.js と同じ)
  app.use(express.json());
  app.use((req, _res, next) => {
    req.session = sessionOn ? { authenticated: true, email: 'admin@b-faith.biz', displayName: '管理', allowedApps: '*', role: 'admin' } : {};
    next();
  });
  app.use('/apps/iroha-work', router);
  const server = await new Promise((resolve) => { const s = app.listen(0, () => resolve(s)); });
  const port = server.address().port;
  const HOST = `127.0.0.1:${port}`;
  const BASE = `http://${HOST}/apps/iroha-work`;
  const call = async (method, pathname, { body, headers = {}, cookie } = {}) => {
    const h = { Host: HOST, ...headers };
    if (body !== undefined) { h['Content-Type'] = 'application/json'; h.Origin = `http://${HOST}`; }
    if (cookie) h.Cookie = cookie;
    const r = await fetch(BASE + pathname, { method, headers: h, body: body === undefined ? undefined : JSON.stringify(body), redirect: 'manual' });
    const text = await r.text();
    let json = null; try { json = JSON.parse(text); } catch { /* not json */ }
    return { status: r.status, json, text, location: r.headers.get('location') };
  };

  // 管理者が印刷係を登録 → トークンは 1 回だけ返る (Cookie ではない)
  const reg = await call('POST', '/admin/devices', { body: { label: 'いろはPC 2号', kind: 'agent', printer_name: 'Brother QL-800 #2' } });
  ok(reg.status === 200 && reg.json.ok && reg.json.kind === 'agent' && typeof reg.json.token === 'string' && reg.json.token.length > 20, '管理画面から印刷係を登録するとトークンが返る');
  ok(!('loggedOut' in reg.json), 'エージェント登録では管理者セッションを破棄しない');
  const tok2 = reg.json.token, id2 = reg.json.id;
  const dupReg = await call('POST', '/admin/devices', { body: { label: 'だぶり', kind: 'agent', printer_name: 'Brother QL-800 #2' } });
  ok(dupReg.status === 400 && /登録済み/.test(dupReg.json.message || ''), '同じプリンター名は 400');
  const pr = await call('POST', `/admin/devices/${id2}/printer`, { body: { printer_name: 'Brother QL-800 #3' } });
  ok(pr.status === 200 && pr.json.ok && pr.json.printer_name === 'Brother QL-800 #3', 'プリンター名の付け替え');
  const pj = await call('GET', '/admin/print-jobs');
  ok(pj.status === 200 && Array.isArray(pj.json.jobs) && Array.isArray(pj.json.agents) && pj.json.agents.length === 2, '管理画面用 JSON (ジョブ + 印刷係)');
  const adminHtml = await call('GET', '/admin');
  ok(adminHtml.status === 200 && /sec-print/.test(adminHtml.text) && /いろはPC 2号/.test(adminHtml.text) && /Brother QL-800 #3/.test(adminHtml.text), '管理画面に 🏷 節と印刷係が出る');

  // iPad (端末Cookie) — セッション無し
  sessionOn = false;
  const ipad = createDevice('テストiPad', 'admin@test');
  const ck = `iw_device=${ipad.token}`;
  const noAuth = await call('GET', '/print/next');
  ok(noAuth.status === 401, '/print/next は認証なしで 401');
  const ipadNext = await call('GET', '/print/next', { cookie: ck });
  ok(ipadNext.status === 401, 'iPad の端末Cookieでは印刷ジョブを取れない (401)');
  const ipadBearer = await call('GET', '/print/next', { headers: { Authorization: `Bearer ${ipad.token}` } });
  ok(ipadBearer.status === 401, 'iPad のトークンを Bearer にしても 401 (kind=agent だけ)');
  const agentCookie = await call('GET', '/api/state', { cookie: `iw_device=${agentTok}` });
  ok(agentCookie.status === 401, 'エージェントのトークンを Cookie に入れても作業画面 (API) には入れない');
  const agentPage = await call('GET', '/', { cookie: `iw_device=${agentTok}` });
  ok(agentPage.status === 302 && /\/enroll/.test(agentPage.location || ''), '画面も端末登録へ送る (入れない)');

  // 印刷係 (Bearer) の 1 本道
  const H2 = { Authorization: `Bearer ${tok2}` };
  const hb = await call('POST', '/print/heartbeat', { body: { note: 'ready', version: 'v1', bpac: true, host: 'PC2', paperFormat: '箱ラベル 62x67', paperFormatOk: true }, headers: H2 });
  ok(hb.status === 200 && hb.json.ok && hb.json.lease_sec > 0, 'heartbeat 200');
  const none = await call('GET', '/print/next', { headers: H2 });
  ok(none.status === 204, '刷るものが無ければ 204');

  // iPad が積む: 作業者必須・アプリ正本・カードの行から
  const st0 = await call('GET', '/api/state', { cookie: ck });
  ok(st0.status === 200 && Array.isArray(st0.json.print_agents) && st0.json.print_agents.length === 2 && st0.json.capabilities.includes('task.label.print'), '/api/state に印刷係の一覧と 🏷 の許可');
  const card0 = st0.json.cards.find(c => Number(c.id) === T2);
  ok(card0 && card0.print_job && card0.print_job.state === 'completed' && !('lease_token' in card0.print_job), 'カードに最新の印刷ジョブ (lease token なし)');
  const noWorker = await call('POST', '/api/print/jobs', { body: { task_id: T2, copies: 1, client_request_id: crid(), target_device_id: id2 }, cookie: ck });
  ok(noWorker.status === 400 && noWorker.json.error === 'worker_required', '作業者を選ばないと積めない');
  const badId = await call('POST', '/api/print/jobs', { body: { task_id: 'abc', worker_id: W, copies: 1, client_request_id: crid(), target_device_id: id2 }, cookie: ck });
  ok(badId.status === 400, 'カード id が壊れていれば 400');
  const needTarget = await call('POST', '/api/print/jobs', { body: { task_id: T2, worker_id: W, copies: 1, client_request_id: crid() }, cookie: ck });
  ok(needTarget.status === 400 && needTarget.json.error === 'target_required', '印刷係が 2 台あるときは出力先を選ばないと積めない');
  const okReq = crid();
  const en = await call('POST', '/api/print/jobs', { body: { task_id: T2, worker_id: W, copies: 2, pack_qty: '24', expiry: '2027-03', client_request_id: okReq, target_device_id: id2 }, cookie: ck });
  ok(en.status === 200 && en.json.ok && en.json.job.state === 'queued' && en.json.job.printer_name === 'Brother QL-800 #3' && en.json.job.requested_by === 'たなか', 'iPad から積めた (出力先 2 号機・依頼者=作業者)');
  const ev = db.prepare("SELECT * FROM f_iroha_app_events WHERE action = 'label_print' ORDER BY id DESC LIMIT 1").get();
  ok(ev && ev.ok === 1 && /2枚/.test(ev.to_value || '') && ev.device_label === 'テストiPad' && ev.worker_name === 'たなか', '記録 (だれが・どの端末で・何枚)');
  const replay = await call('POST', '/api/print/jobs', { body: { task_id: T2, worker_id: W, copies: 2, pack_qty: '24', expiry: '2027-03', client_request_id: okReq, target_device_id: id2 }, cookie: ck });
  ok(replay.status === 200 && replay.json.replayed && replay.json.job.id === en.json.job.id, '同じ冪等 ID の再送は同じジョブ');
  const busy = await call('POST', '/api/print/jobs', { body: { task_id: T2, worker_id: W, copies: 1, client_request_id: crid(), target_device_id: id2 }, cookie: ck });
  ok(busy.status === 409 && busy.json.error === 'in_progress', '進行中は 409');
  const clashHttp = await call('POST', '/api/print/jobs', { body: { task_id: T2, worker_id: W, copies: 3, pack_qty: '24', expiry: '2027-03', client_request_id: okReq, target_device_id: id2 }, cookie: ck });
  ok(clashHttp.status === 409 && clashHttp.json.error === 'idempotency_conflict', '同じ冪等 ID で違う枚数は 409 idempotency_conflict');
  const noOrigin = await fetch(BASE + '/api/print/jobs', { method: 'POST', headers: { Host: HOST, 'Content-Type': 'application/json', Cookie: ck }, body: JSON.stringify({ task_id: T2, worker_id: W, copies: 1, client_request_id: crid(), target_device_id: id2 }) });
  ok(noOrigin.status === 403, 'Origin 無しの POST は 403 (checkOrigin)');

  // 印刷係が取りに来る → 投入 → 完了 → iPad の state に ✅
  const nx = await call('GET', '/print/next', { headers: H2 });
  ok(nx.status === 200 && nx.json.job.id === en.json.job.id && nx.json.job.expiry === '2027-03' && nx.json.job.packQty === '24', '2 号機宛てのジョブを lease (expiry 付き)');
  const other = await call('GET', '/print/next', { headers: { Authorization: `Bearer ${agentTok}` } });
  ok(other.status === 204, '1 号機には配らない (宛先が違う)');
  const lease = nx.json.job.leaseToken;
  const sub = await call('POST', `/print/${nx.json.job.id}/submitted`, { body: { lease, spool_job_id: '12' }, headers: H2 });
  ok(sub.status === 200 && sub.json.ok, '投入報告 200');
  const badOk = await call('POST', `/print/${nx.json.job.id}/completed`, { body: { lease, ok: 'true' }, headers: H2 });
  ok(badOk.status === 400 && badOk.json.error === 'bad_ok', 'ok が真偽値でなければ 400');
  const wrongLease = await call('POST', `/print/${nx.json.job.id}/completed`, { body: { lease: 'nope', ok: true }, headers: H2 });
  ok(wrongLease.status === 409, '違う lease の完了報告は 409');
  const comp = await call('POST', `/print/${nx.json.job.id}/completed`, { body: { lease, ok: true }, headers: H2 });
  ok(comp.status === 200 && comp.json.state === 'completed', '完了報告 200 → completed');
  const stt = await call('GET', `/print/${nx.json.job.id}/status`, { headers: H2 });
  ok(stt.status === 200 && stt.json.job.state === 'completed', '状態照会 (自分の lease 分)');
  const stt1 = await call('GET', `/print/${nx.json.job.id}/status`, { headers: { Authorization: `Bearer ${agentTok}` } });
  ok(stt1.status === 404, '他の端末のジョブは照会できない');
  const st1 = await call('GET', '/api/state', { cookie: ck });
  const card1 = st1.json.cards.find(c => Number(c.id) === T2);
  ok(card1.print_job.id === en.json.job.id && card1.print_job.state === 'completed' && /印刷しました/.test(card1.print_job.label), 'iPad の state に ✅ が乗る');

  // 失効したエージェントは何もできない
  revokeDevice(id2);
  const dead = await call('POST', '/print/heartbeat', { body: { note: 'x' }, headers: H2 });
  ok(dead.status === 401, '解除した印刷係は 401');
  ok(listPrintAgents().length === 1, '一覧からも消える');

  // Notion 正本のときは積めない (下見のカードに書き込まない)
  setMetaValue('source_of_truth', 'notion');
  const nm = await call('POST', '/api/print/jobs', { body: { task_id: T2, worker_id: W, copies: 1, client_request_id: crid() }, cookie: ck });
  ok(nm.status === 409 && nm.json.error === 'notion_mode', 'Notion 正本のときは 409 notion_mode');
  setMetaValue('source_of_truth', 'app');
  server.close();
}

console.log(`\n結果: ${pass} PASS / ${fail} FAIL`);
process.exit(fail ? 1 : 0);
