/**
 * 🏷 値札 (BCシール) 印刷キュー — 入荷受付チェック iPad → 倉庫PC QL-700 (apps/inbound-check/print-queue.js)
 *
 * 実行: node scripts/test-inbound-check-print.mjs
 *
 * 守りたいのは3つ。
 *   ① 同じシールが二重に出ない — lease した後は期限切れでも配り直さない / 冪等ID / 進行中は積めない
 *   ② 誰が刷れるか — 印刷ジョブは Authorization ヘッダーの kind='agent' 端末だけ。iPad Cookie では取れない。
 *      エージェントのトークンで作業画面には入れない
 *   ③ 出ないことに誰も気づかない状態を作らない — 滞留は manual、報告なしは unknown、通知は送れるまで諦めない
 * DB 層 + 実 HTTP (express に router を mount) の両方で確かめる。
 */
import fs from 'fs';
import os from 'os';
import path from 'path';
import http from 'http';
import iconv from 'iconv-lite';

if (!process.env.DATA_DIR) process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'ic-print-'));
delete process.env.INBOUND_CHECK_PRINT_WEBHOOK;
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const dbMod = await import('../apps/inbound-check/db.js');
const {
  getDB, importCsv, getActiveBatch, createDevice, verifyDevice, revokeDevice, listDevices, setAgentPrinter, cleanupOld,
} = dbMod;
const pq = await import('../apps/inbound-check/print-queue.js');
const {
  enqueuePrintJob, leaseNextJob, markSubmitted, markFinished, getJobStatusFor, recordHeartbeat, sweepPrintJobs,
  pendingAlerts, markAlerted, alertTextFor, listPrintAgents, resolvePrintTarget, latestJobsForBatch, listPrintJobs,
  barcodeTypeOf, publicJob, REPORT_DEADLINE_SEC, STALE_QUEUED_SEC, MAX_COPIES,
} = pq;
const { printQueueTick } = await import('../apps/inbound-check/sync-job.js');

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };
const eq = (a, b, l) => ok(JSON.stringify(a) === JSON.stringify(b), `${l} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);
const db = getDB();
const stateOf = (id) => db.prepare('SELECT state FROM f_inbound_check_print_jobs WHERE id = ?').get(id)?.state;
const at = (base, sec) => new Date(Date.parse(base) + sec * 1000).toISOString();
let seq = 0;
const rid = () => `req-${Date.now()}-${++seq}`;

// ─── 入荷受付 CSV を1つ取り込んで active バッチを作る ───
const HEADER = ['入荷管理番号', '入荷管理行番号', '入荷管理詳細行番号', 'ステータス', '荷主入荷NO', '入荷予定日', '入荷受付日', '入荷確定日',
  '取引先ID', '取引先名', '業務区分名', '商品ID', '商品名', '品質区分名', 'ロケーション', '予定数', '受付数', '検品数', '作成日時', '更新日時', 'バーコード', '備考'];
function makeCsv(rows) {
  const q = v => `"${String(v == null ? '' : v).replace(/"/g, '""')}"`;
  const lines = [HEADER.map(q).join(',')];
  for (const r of rows) lines.push(HEADER.map(h => q(r[h])).join(','));
  return iconv.encode(lines.join('\r\n') + '\r\n', 'cp932');
}
const row = (ar, no, pid, qty, extra = {}) => ({
  入荷管理番号: ar, 入荷管理行番号: no, 入荷管理詳細行番号: 1, ステータス: '受付済', 入荷予定日: '20260905', 入荷受付日: '20260905',
  取引先ID: '0002', 取引先名: 'BF', 業務区分名: '通常入荷', 商品ID: pid, 商品名: `商品 ${pid}`, 品質区分名: '良品',
  予定数: qty, 受付数: qty, 作成日時: '20260905082105', 更新日時: '20260905082311', バーコード: '4573473360422', ...extra,
});
const imp = importCsv(makeCsv([
  row('AR00110005259', 1, 'pashima-single-KI', 42, { 商品名: 'パシーマキルトケット【シングル】 【生成り】_K-60', バーコード: '4903357200047' }),
  row('AR00110005259', 2, 'toretatecap-NS', 10, { バーコード: 'X002ABCD1F' }),          // FNSKU
  row('AR00110005259', 3, 'nobarcode', 5, { バーコード: '' }),                           // バーコード無し
  row('AR00110005259', 4, 'badbarcode', 5, { バーコード: '4903-357' }),                  // 記号入り
  row('AR00110005260', 1, 'silicateclay800', 20, { バーコード: '4560371475815' }),
]), { fileName: 'CA04001_test.csv', source: 'manual_upload', actor: 'test' });
ok(imp.ok, `CSV 取込 (${imp.rowCount} 行)`);
const batch = getActiveBatch();
const L1 = 'AR00110005259|1|1', L2 = 'AR00110005259|2|1', L3 = 'AR00110005259|3|1', L4 = 'AR00110005259|4|1', L5 = 'AR00110005260|1|1';

console.log('\n[1] バーコードの種別 (値札CSVの JAN/FNSKU 振り分けと同じ)');
{
  eq(barcodeTypeOf('4903357200047'), 'jan', '数字だけ = JAN');
  eq(barcodeTypeOf('X002ABCD1F'), 'fnsku', '英字を含む英数字 = FNSKU');
  eq(barcodeTypeOf(' 4903357200047 '), 'jan', '前後の空白は無視');
  eq(barcodeTypeOf(''), null, '空は積めない');
  eq(barcodeTypeOf('4903-357'), null, '記号入りは積めない');
  eq(barcodeTypeOf(null), null, 'null');
}

console.log('\n[2] 印刷エージェント (倉庫PC) の登録');
let agent, agentRow;
{
  const r = resolvePrintTarget();
  ok(!r.ok && r.error === 'no_agent', 'エージェント未登録では出力先が決まらない');
  let threw = null;
  try { createDevice('倉庫PC', 'admin', { kind: 'agent', printerName: '' }); } catch (e) { threw = e.message; }
  ok(/プリンター名/.test(threw || ''), `プリンター名なしでは登録できない (${threw})`);
  agent = createDevice('倉庫PC', 'admin', { kind: 'agent', printerName: ' Brother QL-700 ' });
  agentRow = verifyDevice(agent.token);
  ok(agentRow && agentRow.kind === 'agent' && agentRow.printer_name === 'Brother QL-700', 'kind=agent + プリンター名 (trim) で登録される');
  let dup = null;
  try { createDevice('別PC', 'admin', { kind: 'agent', printerName: 'Brother QL-700' }); } catch (e) { dup = e.message; }
  ok(/別の端末/.test(dup || ''), `同じプリンター名を別の有効な端末に登録できない (${(dup || '').slice(0, 40)}…)`);
  const t = resolvePrintTarget();
  ok(t.ok && t.agent.id === agent.id, 'エージェントが1台なら自動でそれが出力先');
  const list = listPrintAgents();
  ok(list.length === 1 && list[0].online === false && list[0].bpac === null, '未接続 (heartbeat なし) = online:false / bpac:null');
  recordHeartbeat(agent.id, { note: 'printers: Brother QL-700', version: '2026-09-05.4', bpac: true, host: 'DESKTOP-HUIUSPG', paperFormat: '値札 62x67', paperFormatOk: true, printerReports: '274 62mm x 29mm' });
  const a2 = listPrintAgents()[0];
  ok(a2.online === true && a2.bpac === true && a2.paper_ok === true && a2.version === '2026-09-05.4' && a2.host === 'DESKTOP-HUIUSPG', 'heartbeat の内容 (b-PAC・用紙・版・PC名) が読める');
  recordHeartbeat(agent.id, { note: 'b-PAC MISSING', bpac: false, paperFormatOk: false });
  const a3 = listPrintAgents()[0];
  ok(a3.bpac === false && a3.paper_ok === false, 'b-PAC 未導入 / 用紙未登録 も読める');
  recordHeartbeat(agent.id, { note: 'ready', bpac: true, paperFormatOk: true });
  const dev = listDevices().find(d => d.id === agent.id);
  ok(dev && dev.kind === 'agent' && dev.printer_name === 'Brother QL-700' && dev.heartbeat_at, 'listDevices に kind / printer_name / heartbeat_at が出る');
}

console.log('\n[3] 積む (iPad) — 明細から取る・検査・冪等');
let job1;
{
  const base = { batchId: batch.id, lineKey: L1, copies: 3, packQty: 14, requestedBy: '山田', requestedDevice: '入荷iPad1' };
  let r = enqueuePrintJob({ ...base, clientRequestId: 'short' });
  ok(!r.ok && r.error === 'bad_request', '冪等IDが短すぎると積めない');
  r = enqueuePrintJob({ ...base, clientRequestId: rid(), copies: 0 });
  ok(!r.ok && r.error === 'bad_copies', '枚数 0 は積めない');
  r = enqueuePrintJob({ ...base, clientRequestId: rid(), copies: MAX_COPIES + 1 });
  ok(!r.ok && r.error === 'bad_copies', `枚数 ${MAX_COPIES + 1} は積めない (エージェントの上限と同じ)`);
  r = enqueuePrintJob({ ...base, clientRequestId: rid(), packQty: 'abc' });
  ok(!r.ok && r.error === 'bad_pack_qty', '入数 abc は積めない');
  r = enqueuePrintJob({ ...base, clientRequestId: rid(), batchId: batch.id + 99 });
  ok(!r.ok && r.error === 'stale_batch', '古い/知らないバッチは積めない');
  r = enqueuePrintJob({ ...base, clientRequestId: rid(), lineKey: 'AR9|9|9' });
  ok(!r.ok && r.error === 'not_found', '一覧にない明細は積めない');
  r = enqueuePrintJob({ ...base, clientRequestId: rid(), lineKey: L3 });
  ok(!r.ok && r.error === 'bad_barcode' && /登録されていない/.test(r.message), 'バーコード無しは積めない (理由つき)');
  r = enqueuePrintJob({ ...base, clientRequestId: rid(), lineKey: L4 });
  ok(!r.ok && r.error === 'bad_barcode' && /4903-357/.test(r.message), '記号入りバーコードは積めない (理由つき)');
  r = enqueuePrintJob({ ...base, clientRequestId: rid(), targetDeviceId: 999 });
  ok(!r.ok && r.error === 'no_agent', '存在しない出力先は積めない');

  const crid = rid();
  r = enqueuePrintJob({ ...base, clientRequestId: crid });
  ok(r.ok && r.created && r.job.state === 'queued', '積める');
  job1 = r.job;
  ok(job1.product_name === 'パシーマキルトケット【シングル】 【生成り】_K-60' && job1.product_code === 'pashima-single-KI'
    && job1.barcode === '4903357200047' && job1.barcode_type === 'jan' && job1.pack_qty === '14' && job1.copies === 3
    && job1.printer_name === 'Brother QL-700' && job1.target_device_id === agent.id && job1.requested_by === '山田',
    '商品名・商品ID・バーコードは明細から、入数・枚数は依頼から、出力先はエージェントから');
  ok(!('lease_token' in job1) && !('leaseToken' in job1), '公開形に lease token を含めない');
  const again = enqueuePrintJob({ ...base, clientRequestId: crid, copies: 9 });
  ok(again.ok && !again.created && again.replayed && again.job.id === job1.id && again.job.copies === 3, '同じ冪等IDの再送は同じジョブ (枚数を変えても増えない)');
  const dup = enqueuePrintJob({ ...base, clientRequestId: rid() });
  ok(!dup.ok && dup.error === 'in_progress' && dup.job.id === job1.id, '同じ明細のジョブが終わるまで新しく積めない (連打で2枚出ない)');
  const fn = enqueuePrintJob({ ...base, clientRequestId: rid(), lineKey: L2, packQty: null, copies: 1 });
  ok(fn.ok && fn.job.barcode_type === 'fnsku' && fn.job.pack_qty === '', 'FNSKU は fnsku / 入数なしは空文字で刷る');
  const m = latestJobsForBatch(batch.id);
  ok(m.get(L1)?.id === job1.id && m.get(L2)?.id === fn.job.id && !m.has(L3), '明細ごとの最新ジョブ');
}

console.log('\n[4] lease (エージェントが取りに来る) — その端末宛てだけ・1件ずつ・二度と配らない');
let lease1, job2;
{
  const ipad = createDevice('入荷iPad1', 'admin');
  const ipadRow = verifyDevice(ipad.token);
  ok(leaseNextJob(ipadRow) === null, 'iPad 端末 (kind=ipad) は lease できない');
  const noPrinter = createDevice('倉庫PC2', 'admin', { kind: 'agent', printerName: 'QL-720' });
  const npRow = verifyDevice(noPrinter.token);
  ok(leaseNextJob(npRow) === null, '宛先でない端末には渡さない');
  const j = leaseNextJob(agentRow);
  ok(j && j.id === job1.id && j.leaseToken && j.printerName === 'Brother QL-700', '宛先の端末が最初のジョブを lease できる');
  eq(Object.keys(j).sort(), ['barcode', 'barcodeType', 'copies', 'id', 'leaseExpiresAt', 'leaseToken', 'lineKey', 'packQty', 'printerName', 'productCode', 'productName', 'requestedBy'], 'エージェント (agent.ps1) が前提にする JSON の形');
  ok(j.packQty === '14' && typeof j.copies === 'number' && j.barcodeType === 'jan', 'packQty は文字列・copies は数値');
  lease1 = j;
  eq(stateOf(job1.id), 'leased', 'leased');
  const j2 = leaseNextJob(agentRow);
  ok(j2 && j2.id !== job1.id && j2.barcodeType === 'fnsku', '次の呼び出しは次のジョブ (同じジョブは二度と渡さない)');
  ok(leaseNextJob(agentRow) === null, '3回目は無し (204)');
  job2 = j2;   // 報告しないまま放置 → [6] で「期限切れは unknown (再配布しない)」を見る
  revokeDevice(noPrinter.id);
}

console.log('\n[5] 報告 (submitted → completed) と冪等な再送');
{
  const bad = markSubmitted(job1.id, { deviceId: agentRow.id, leaseToken: 'wrong' });
  ok(!bad.ok, '違う lease token の投入報告は弾く');
  const other = markSubmitted(job1.id, { deviceId: agentRow.id + 100, leaseToken: lease1.leaseToken });
  ok(!other.ok, '別の端末からの報告は弾く');
  const early = markFinished(job1.id, { deviceId: agentRow.id, leaseToken: lease1.leaseToken, ok: true });
  ok(!early.ok, 'スプーラーに入れる前の「刷れた」は受け付けない');
  const s = markSubmitted(job1.id, { deviceId: agentRow.id, leaseToken: lease1.leaseToken, spoolJobId: 'nefuda-1' });
  ok(s.ok && !s.replayed, '投入報告');
  const s2 = markSubmitted(job1.id, { deviceId: agentRow.id, leaseToken: lease1.leaseToken, spoolJobId: 'nefuda-1' });
  ok(s2.ok && s2.replayed, '同じ投入報告の再送は replayed:true で成功 (応答が消えても復旧できる)');
  const s3 = markSubmitted(job1.id, { deviceId: agentRow.id, leaseToken: lease1.leaseToken, spoolJobId: 'nefuda-99' });
  ok(!s3.ok && s3.reason === 'submission_conflict', '違う spool_job_id の再送は submission_conflict (別スプールへの二重投入を黙って成功にしない — Codex R1)');
  const st = getJobStatusFor(job1.id, agentRow.id);
  ok(st && st.state === 'submitted' && st.spool_job_id === 'nefuda-1' && st.submitted_at, '/status で submitted と spool_job_id が読める');
  ok(getJobStatusFor(job1.id, agentRow.id + 100) === null, '他の端末は /status を読めない');
  const c = markFinished(job1.id, { deviceId: agentRow.id, leaseToken: lease1.leaseToken, ok: true });
  ok(c.ok && c.state === 'completed', '完了報告');
  const c2 = markFinished(job1.id, { deviceId: agentRow.id, leaseToken: lease1.leaseToken, ok: true });
  ok(c2.ok && c2.replayed, '同じ完了報告の再送は replayed');
  const c3 = markFinished(job1.id, { deviceId: agentRow.id, leaseToken: lease1.leaseToken, ok: false, error: 'late' });
  ok(!c3.ok, 'completed の後に「刷れなかった」は受け付けない');
  ok(typeof markFinished(job1.id, { deviceId: agentRow.id, leaseToken: lease1.leaseToken, ok: 'true' }).reason === 'string', 'ok は真偽値のみ');
  const nxt = enqueuePrintJob({ batchId: batch.id, lineKey: L1, copies: 2, clientRequestId: rid() });
  ok(nxt.ok && nxt.created, '終わった明細には新しく積める (追加で発行)');
}

console.log('\n[6] 失敗 (failed) と結果不明 (unknown) を混ぜない');
{
  // 刷る前の失敗 (テンプレが無い等) = failed → もう一度押してよい
  const j = leaseNextJob(agentRow);
  const f = markFinished(j.id, { deviceId: agentRow.id, leaseToken: j.leaseToken, ok: false, error: 'template file is missing on this PC' });
  ok(f.ok && f.state === 'failed', 'leased からの ok:false (uncertain なし) は failed');
  ok(/もう一度/.test(alertTextFor(db.prepare('SELECT * FROM f_inbound_check_print_jobs WHERE id=?').get(j.id))), 'failed の通知は「もう一度押せる」');
  // スプーラーに渡した後の失敗 = unknown → 実物を確認
  const r2 = enqueuePrintJob({ batchId: batch.id, lineKey: L1, copies: 1, clientRequestId: rid() });
  const j2 = leaseNextJob(agentRow);
  markSubmitted(j2.id, { deviceId: agentRow.id, leaseToken: j2.leaseToken, spoolJobId: 'nefuda-9' });
  const u = markFinished(j2.id, { deviceId: agentRow.id, leaseToken: j2.leaseToken, ok: false, error: 'printing did not complete (spooler error)', uncertain: true });
  ok(u.ok && u.state === 'unknown', 'uncertain=true は unknown');
  ok(/実物を確認/.test(alertTextFor(db.prepare('SELECT * FROM f_inbound_check_print_jobs WHERE id=?').get(j2.id))), 'unknown の通知は「実物を確認」(「もう1回」と言わない)');
  // 🚨 投入済み (submitted) からの ok:false は uncertain が無くても unknown — 紙が出ているかもしれない (Codex R1 High-1)
  const r2c = enqueuePrintJob({ batchId: batch.id, lineKey: L1, copies: 1, clientRequestId: rid(), acknowledgeUnknownJobId: j2.id });
  ok(r2c.ok && r2c.job.acknowledged_job_id === j2.id, 'unknown の後は「実物を確認した」証跡 (そのジョブID) を付ければ積める');
  const j2c = leaseNextJob(agentRow);
  markSubmitted(j2c.id, { deviceId: agentRow.id, leaseToken: j2c.leaseToken, spoolJobId: 'nefuda-10' });
  const notUncertain = markFinished(j2c.id, { deviceId: agentRow.id, leaseToken: j2c.leaseToken, ok: false, error: 'agent says failed', uncertain: false });
  ok(notUncertain.ok && notUncertain.state === 'unknown' && stateOf(j2c.id) === 'unknown', 'submitted からの ok:false (uncertain:false) は failed ではなく unknown');
  // unknown の後、証跡なし / 違うIDでは積めない
  const noAck = enqueuePrintJob({ batchId: batch.id, lineKey: L1, copies: 1, clientRequestId: rid() });
  ok(!noAck.ok && noAck.error === 'confirm_unknown' && noAck.job.id === j2c.id, 'unknown の直後は証跡なしでは積めない (confirm_unknown + そのジョブ)');
  const wrongAck = enqueuePrintJob({ batchId: batch.id, lineKey: L1, copies: 1, clientRequestId: rid(), acknowledgeUnknownJobId: j2.id });
  ok(!wrongAck.ok && wrongAck.error === 'confirm_unknown', '古いジョブIDの証跡では積めない (画面が古い)');
  const withAck = enqueuePrintJob({ batchId: batch.id, lineKey: L1, copies: 1, clientRequestId: rid(), acknowledgeUnknownJobId: j2c.id });
  ok(withAck.ok && withAck.job.acknowledged_job_id === j2c.id, '最新の unknown ジョブIDを証跡にすれば積める');
  const j2d = leaseNextJob(agentRow);
  markFinished(j2d.id, { deviceId: agentRow.id, leaseToken: j2d.leaseToken, ok: false, error: 'template missing' });
  eq(stateOf(j2d.id), 'failed', 'leased からの ok:false (uncertain:false) は failed のまま');
  // 期限切れで unknown に倒した後、同じ lease の報告が遅れて届いたら上書き (より確かな情報)
  const r3 = enqueuePrintJob({ batchId: batch.id, lineKey: L5, copies: 1, clientRequestId: rid() });
  const j3 = leaseNextJob(agentRow);
  markSubmitted(j3.id, { deviceId: agentRow.id, leaseToken: j3.leaseToken });
  const sw = sweepPrintJobs({ now: at(new Date().toISOString(), REPORT_DEADLINE_SEC * 2 + 1) });
  eq(stateOf(j3.id), 'unknown', '投入後に報告が来なければ unknown');
  // 🚨 [4] で lease したまま報告が無い job2 も、期限が切れたら queued へ戻さず unknown (ジョブを渡した = 紙が出たかもしれない)
  eq(stateOf(job2.id), 'unknown', '報告が来ないまま期限を過ぎた leased は unknown (再配布しない)');
  ok(sw.unknown === 2 && leaseNextJob(agentRow) === null, 'unknown になったジョブは誰にも配られない');
  const late = markFinished(j3.id, { deviceId: agentRow.id, leaseToken: j3.leaseToken, ok: true });
  ok(late.ok && late.state === 'completed' && stateOf(j3.id) === 'completed', '遅れて届いた完了報告で unknown → completed');
  const lateWrong = markFinished(j3.id, { deviceId: agentRow.id, leaseToken: 'other', ok: false, error: 'x' });
  ok(!lateWrong.ok, '違う lease の遅い報告は弾く');
  void r2; void r3;
}

console.log('\n[7] 滞留 (誰も取りに来ない) → manual、通知は送れるまで諦めない');
{
  // L2 の直前 = [4] で lease したまま unknown になった job2 → 証跡が要る
  const r0 = enqueuePrintJob({ batchId: batch.id, lineKey: L2, copies: 1, clientRequestId: rid() });
  ok(!r0.ok && r0.error === 'confirm_unknown', '期限切れで unknown になった明細も、証跡なしでは積めない');
  const r = enqueuePrintJob({ batchId: batch.id, lineKey: L2, copies: 1, clientRequestId: rid(), acknowledgeUnknownJobId: job2.id });
  ok(r.ok, '積む');
  const sw0 = sweepPrintJobs({ now: at(new Date().toISOString(), STALE_QUEUED_SEC - 5) });
  ok(sw0.manual === 0 && stateOf(r.job.id) === 'queued', '3分未満は queued のまま');
  const sw = sweepPrintJobs({ now: at(new Date().toISOString(), STALE_QUEUED_SEC + 1) });
  ok(sw.manual === 1 && stateOf(r.job.id) === 'manual', '3分たっても取りに来なければ manual');
  ok(leaseNextJob(agentRow) === null, 'manual は復帰したエージェントにも配らない (手で刷った後に自動でも出て二重にならない)');
  ok(/手で刷って/.test(alertTextFor(db.prepare('SELECT * FROM f_inbound_check_print_jobs WHERE id=?').get(r.job.id))), 'manual の通知は「手で刷って」');
  const pend = pendingAlerts();
  ok(pend.length >= 1 && pend.every(j => ['completed', 'failed', 'manual', 'unknown'].includes(j.state)), `未通知の結果が並ぶ (${pend.length}件)`);
  const target = pend[0];
  markAlerted(target.id, 'queued');   // 違う状態で印を付けても効かない
  ok(pendingAlerts().some(j => j.id === target.id), '状態が違う「通知済み」は効かない');
  markAlerted(target.id, target.state);
  ok(!pendingAlerts().some(j => j.id === target.id), '正しい状態で通知済みにすると消える');
  // ワーカー1周: webhook 未設定なら通知しない・sweep は走る・エージェント生存で ping (JOBS_MONITOR 無効なら false)
  const t = await printQueueTick({ fetchFn: async () => { throw new Error('must not be called'); } });
  ok(t.swept && t.alerted === 0, 'webhook 未設定では通知を送らない (iPad の行が主経路)');
  process.env.INBOUND_CHECK_PRINT_WEBHOOK = 'https://chat.example/hook';
  const sent = [];
  const t2 = await printQueueTick({ fetchFn: async (url, opts) => { sent.push(JSON.parse(opts.body).text); return { ok: true, status: 200 }; } });
  ok(t2.alerted >= 1 && sent.length === t2.alerted, `webhook があれば未通知分を送る (${t2.alerted}件)`);
  ok(pendingAlerts().length === 0, '送れた分は通知済みになる');
  const r2 = enqueuePrintJob({ batchId: batch.id, lineKey: L2, copies: 1, clientRequestId: rid() });
  sweepPrintJobs({ now: at(new Date().toISOString(), STALE_QUEUED_SEC + 1) });
  const t3 = await printQueueTick({ fetchFn: async () => ({ ok: false, status: 500 }) });
  ok(t3.alerted === 0 && pendingAlerts().some(j => j.id === r2.job.id), 'webhook が落ちていれば通知済みにしない (次の周回で再送)');
  delete process.env.INBOUND_CHECK_PRINT_WEBHOOK;
  ok(listPrintJobs(5).length === 5 && listPrintJobs(5)[0].device_label === '倉庫PC', '管理画面の一覧 (端末名つき)');
}

console.log('\n[8] プリンター名の付け替え・失効');
{
  const r = enqueuePrintJob({ batchId: batch.id, lineKey: L5, copies: 1, clientRequestId: rid() });
  ok(r.ok && r.job.printer_name === 'Brother QL-700', '積んだ時点の出力先を持つ');
  const bad = setAgentPrinter(agent.id, '');
  ok(!bad.ok && bad.error === 'bad_printer', '空の名前には変えられない');
  const s = setAgentPrinter(agent.id, 'Brother QL-700 (2F)');
  ok(s.ok && s.printer_name === 'Brother QL-700 (2F)' && s.cancelled === 1, '付け替えると古い名前の印刷待ちは manual に倒す (別のプリンターから出さない)');
  eq(stateOf(r.job.id), 'manual', 'manual');
  agentRow = verifyDevice(agent.token);
  const r2 = enqueuePrintJob({ batchId: batch.id, lineKey: L5, copies: 1, clientRequestId: rid() });
  ok(r2.ok && r2.job.printer_name === 'Brother QL-700 (2F)', '新しい名前で積める');
  // lease 時にも名前を照合する (付け替えの合間に積まれたジョブの保険)
  db.prepare('UPDATE f_inbound_check_print_jobs SET printer_name = ? WHERE id = ?').run('Old', r2.job.id);
  ok(leaseNextJob(agentRow) === null && stateOf(r2.job.id) === 'manual', 'lease 時に名前が違えば manual にして渡さない');
  ok(setAgentPrinter(agent.id + 1000, 'x').error === 'not_agent', '存在しない端末は not_agent');
  ok(revokeDevice(agent.id), '失効');
  ok(verifyDevice(agent.token) === null, '失効したトークンは使えない');
  ok(resolvePrintTarget().error === 'no_agent', '失効すると出力先が無い');
  const again = createDevice('倉庫PC (再登録)', 'admin', { kind: 'agent', printerName: 'Brother QL-700 (2F)' });
  ok(!!again.token, '失効後は同じプリンター名で登録し直せる');
  agentRow = verifyDevice(again.token);
  agent = again;
}

console.log('\n[9] 保持期間の掃除は終わったジョブだけ');
{
  const before = db.prepare('SELECT COUNT(*) c FROM f_inbound_check_print_jobs').get().c;
  const r = enqueuePrintJob({ batchId: batch.id, lineKey: L5, copies: 1, clientRequestId: rid() });
  db.prepare("UPDATE f_inbound_check_print_jobs SET updated_at = '2020-01-01T00:00:00.000Z'").run();
  const c = cleanupOld(db, new Date());
  ok(c.printJobs === before && stateOf(r.job.id) === 'queued', `終わったジョブ ${c.printJobs} 件を消し、進行中 (queued) は残す`);
  db.prepare("UPDATE f_inbound_check_print_jobs SET state='failed', finished_at=updated_at, error='cleanup' WHERE id=?").run(r.job.id);
}

// ─── 実 HTTP: 認証境界と受け渡し ───
console.log('\n[10] HTTP — 誰が印刷ジョブを取れるか');
{
  const express = (await import('express')).default;
  const router = (await import('../apps/inbound-check/router.js')).default;
  const app = express();
  app.use(express.json());
  // x-test-session: admin = 管理者セッション / user = 一般セッション / 無し = 未ログイン
  app.use((req, res, next) => {
    const s = req.headers['x-test-session'];
    req.session = s ? { authenticated: true, email: 'tester@example.com', displayName: 'テスター', allowedApps: '*', role: s === 'admin' ? 'admin' : 'user', destroy: (cb) => cb() } : {};
    next();
  });
  app.use('/apps/inbound-check', router);
  const server = http.createServer(app);
  await new Promise(r => server.listen(0, '127.0.0.1', r));
  const origin = `http://127.0.0.1:${server.address().port}`;
  const base = `${origin}/apps/inbound-check`;
  const call = async (method, url, { token = null, cookie = null, body = null, session = null } = {}) => {
    const headers = {};
    if (token) headers.Authorization = `Bearer ${token}`;
    if (cookie) headers.Cookie = cookie;
    if (session) headers['x-test-session'] = session;
    if (body) { headers['Content-Type'] = 'application/json'; headers.Origin = origin; }
    const res = await fetch(base + url, { method, headers, body: body ? JSON.stringify(body) : undefined, redirect: 'manual' });
    const ct = res.headers.get('content-type') || '';
    return { status: res.status, body: ct.includes('json') ? await res.json().catch(() => ({})) : null, res };
  };
  const ipad = createDevice('入荷iPad2', 'admin');

  eq((await call('GET', '/print/next')).status, 401, '認証なしでは取れない');
  eq((await call('GET', '/print/next', { token: 'not-a-real-token' })).status, 401, '不正トークンは取れない');
  eq((await call('GET', '/print/next', { cookie: `ic_device=${ipad.token}` })).status, 401, 'iPad の端末Cookieでは取れない');
  eq((await call('GET', '/print/next', { token: ipad.token })).status, 401, 'iPad のトークンを Bearer にしても取れない (kind が違う)');
  eq((await call('GET', '/print/next', { session: 'admin' })).status, 401, '管理者セッションでも取れない (エージェント専用)');
  eq((await call('POST', '/print/heartbeat', { cookie: `ic_device=${ipad.token}`, body: { note: 'x' } })).status, 401, 'heartbeat も同じ境界');
  const st = await call('GET', '/api/state', { cookie: `ic_device=${agent.token}` });
  eq(st.status, 401, 'エージェントのトークンを Cookie にしても作業画面 API に入れない');

  // 管理画面からエージェントを登録 → トークンが1回返る
  const reg = await call('POST', '/admin/devices', { session: 'admin', body: { label: '倉庫PC(HTTP)', kind: 'agent', printer_name: 'QL-700 HTTP' } });
  ok(reg.status === 200 && reg.body.ok && reg.body.kind === 'agent' && typeof reg.body.token === 'string' && !reg.body.loggedOut, '管理者はエージェントを登録でき、トークンを受け取る (セッションは残る)');
  const regDup = await call('POST', '/admin/devices', { session: 'admin', body: { label: '倉庫PC(HTTP2)', kind: 'agent', printer_name: 'QL-700 HTTP' } });
  ok(regDup.status === 400 && /別の端末/.test(regDup.body.message || ''), '同名プリンターの二重登録は 400');
  eq((await call('POST', '/admin/devices', { session: 'user', body: { label: 'x', kind: 'agent', printer_name: 'y' } })).status, 403, '一般利用者は登録できない');
  const noOriginReg = await fetch(base + '/admin/devices', { method: 'POST', headers: { 'Content-Type': 'application/json', 'x-test-session': 'admin' }, body: JSON.stringify({ label: 'x', kind: 'agent', printer_name: 'y' }) });
  eq(noOriginReg.status, 403, 'Origin の無い登録 POST は 403 (CSRF)');
  const httpAgent = reg.body.token;
  const hb = await call('POST', '/print/heartbeat', { token: httpAgent, body: { note: 'printers: QL-700 HTTP', version: '2026-09-05.4', bpac: true, host: 'PC', paperFormat: '値札 62x67', paperFormatOk: true } });
  ok(hb.status === 200 && hb.body.ok && hb.body.lease_sec > 0, 'heartbeat 200 + lease_sec');
  const tg = await call('GET', '/api/print/targets', { session: 'user' });
  ok(tg.status === 200 && tg.body.agents.some(a => a.printer_name === 'QL-700 HTTP' && a.online && a.bpac), '/api/print/targets に登録した倉庫PCが online で出る');
  eq((await call('GET', '/print/next', { token: httpAgent })).status, 204, '刷るものが無ければ 204');

  // iPad (ここではセッション利用者) が積む → エージェントが取る → 報告
  const a1 = tg.body.agents.find(a => a.printer_name === 'Brother QL-700 (2F)');
  const a2 = tg.body.agents.find(a => a.printer_name === 'QL-700 HTTP');
  const noTarget = await call('POST', '/api/print/jobs', { session: 'user', body: { batch_id: batch.id, line_key: L1, copies: 2, pack_qty: 14, client_request_id: rid() } });
  ok(noTarget.status === 400 && noTarget.body.error === 'target_required' && noTarget.body.agents.length === 2, '倉庫PCが2台あれば出力先の指定が要る (勝手に選ばない)');
  const enq = await call('POST', '/api/print/jobs', { session: 'user', body: { batch_id: batch.id, line_key: L1, copies: 2, pack_qty: 14, target_device_id: a2.id, client_request_id: rid() } });
  ok(enq.status === 200 && enq.body.ok && enq.body.job.state === 'queued' && enq.body.job.requested_by === 'テスター' && enq.body.job.printer_name === 'QL-700 HTTP', 'セッション利用者が積める (依頼者はセッション名)');
  const enqDup = await call('POST', '/api/print/jobs', { session: 'user', body: { batch_id: batch.id, line_key: L1, copies: 2, target_device_id: a2.id, client_request_id: rid() } });
  ok(enqDup.status === 409 && enqDup.body.error === 'in_progress' && enqDup.body.job.id === enq.body.job.id, '進行中は 409 in_progress (ジョブつき)');
  eq((await call('POST', '/api/print/jobs', { body: { batch_id: batch.id, line_key: L1, copies: 1, client_request_id: rid() } })).status, 401, '未ログインは積めない');
  const noOrigin = await fetch(base + '/api/print/jobs', { method: 'POST', headers: { 'Content-Type': 'application/json', 'x-test-session': 'user' }, body: JSON.stringify({ batch_id: batch.id, line_key: L5, copies: 1, client_request_id: rid() }) });
  eq(noOrigin.status, 403, 'Origin の無い POST は 403 (CSRF)');
  const state1 = await call('GET', '/api/state', { session: 'user' });
  const line1 = state1.body.lines.find(l => l.line_key === L1);
  ok(line1 && line1.print_job && line1.print_job.id === enq.body.job.id && line1.print_job.state === 'queued' && state1.body.print_agents.length === 2, '/api/state の行に最新ジョブと倉庫PC一覧が付く');
  void a1;

  const other = await call('GET', '/print/next', { token: agent.token });
  eq(other.status, 204, '宛先でないエージェントには渡らない');
  const nx = await call('GET', '/print/next', { token: httpAgent });
  ok(nx.status === 200 && nx.body.job.id === enq.body.job.id && nx.body.job.leaseToken && nx.body.job.printerName === 'QL-700 HTTP' && nx.body.job.packQty === '14', '宛先のエージェントが lease できる (JSON にプリンター名・入数)');
  const lease = nx.body.job.leaseToken;
  eq((await call('POST', `/print/${nx.body.job.id}/completed`, { token: httpAgent, body: { lease, ok: 'yes' } })).status, 400, 'ok が真偽値でなければ 400');
  eq((await call('POST', `/print/${nx.body.job.id}/submitted`, { token: agent.token, body: { lease, spool_job_id: 'x' } })).status, 409, '別のエージェントの報告は 409');
  const sub = await call('POST', `/print/${nx.body.job.id}/submitted`, { token: httpAgent, body: { lease, spool_job_id: 'nefuda-http' } });
  ok(sub.status === 200 && sub.body.ok && sub.body.replayed === false, '投入報告 200');
  const sub2 = await call('POST', `/print/${nx.body.job.id}/submitted`, { token: httpAgent, body: { lease, spool_job_id: 'nefuda-http' } });
  ok(sub2.status === 200 && sub2.body.replayed === true, '再送は replayed:true');
  const sub3 = await call('POST', `/print/${nx.body.job.id}/submitted`, { token: httpAgent, body: { lease, spool_job_id: 'other' } });
  ok(sub3.status === 409 && sub3.body.error === 'submission_conflict', '違う spool_job_id は 409 submission_conflict');
  const stt = await call('GET', `/print/${nx.body.job.id}/status`, { token: httpAgent });
  ok(stt.status === 200 && stt.body.job.state === 'submitted', '/status');
  eq((await call('GET', `/print/${nx.body.job.id}/status`, { token: agent.token })).status, 404, '他のエージェントは /status を読めない');
  const done = await call('POST', `/print/${nx.body.job.id}/completed`, { token: httpAgent, body: { lease, ok: true } });
  ok(done.status === 200 && done.body.state === 'completed', '完了報告 200');
  const state2 = await call('GET', '/api/state', { session: 'user' });
  ok(state2.body.lines.find(l => l.line_key === L1).print_job.state === 'completed', 'iPad の行に ✅ が出る');
  // 管理画面
  const adm = await call('GET', '/admin/print-jobs', { session: 'admin' });
  ok(adm.status === 200 && adm.body.jobs.length > 0 && adm.body.agents.length === 2, '管理者の一覧 JSON');
  eq((await call('GET', '/admin/print-jobs', { session: 'user' })).status, 403, '一般利用者は見られない');
  const rn = await call('POST', `/admin/devices/${a2.id}/printer`, { session: 'admin', body: { printer_name: 'QL-700 HTTP 改' } });
  ok(rn.status === 200 && rn.body.ok && rn.body.printer_name === 'QL-700 HTTP 改', '管理者はプリンター名を付け替えられる');
  eq((await call('GET', '/print/next', { token: httpAgent })).status, 204, '付け替え後も 204 (印刷待ち無し)');
  const rv = await call('POST', `/admin/devices/${a2.id}/revoke`, { session: 'admin', body: {} });
  ok(rv.status === 200, '失効');
  eq((await call('GET', '/print/next', { token: httpAgent })).status, 401, '失効したトークンは 401');
  server.close();
}

console.log(`\n結果: ${pass} PASS / ${fail} FAIL`);
process.exit(fail ? 1 : 0);
