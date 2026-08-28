/**
 * test-print-queue.mjs — 🖨 送り状自動印刷 P2 印刷キュー (要件定義 送り状自動印刷_20260827 §6)
 *
 * 守りたいのは3つ。
 *   ① 印刷してよいものだけがキューに載る (位置推定で見つけたページは載せない)
 *   ② 同じ送り状が二重に出ない — **PDFを渡した後は期限切れでも自動で配り直さない**
 *   ③ 出てこないことに誰も気づかない状態を作らない (通知は送れるまで諦めない)
 * 実行: node apps/packing/tests/test-print-queue.mjs
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'packing-printq-test-'));
process.env.DATA_DIR = tmpDir;

const { initPackingDB, getDB, utcNow, createDevice } = await import('../db.js');
const {
  enqueuePrintJob, leaseNextJob, findLeasedJob, claimPdfForPrint, failBeforeDispatch,
  markSubmitted, markFinished, recordHeartbeat, isPrintable, sweepPrintJobs,
  reclaimExpiredLeases, pendingAlerts, markAlerted, alertTextFor, getJobStatusFor,
  listPrintJobs, MAX_ATTEMPTS, STALE_QUEUED_SEC, DISPATCHED_TIMEOUT_SEC, LEASE_SEC,
} = await import('../print-queue.js');

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const eq = (a, b, label) => ok(JSON.stringify(a) === JSON.stringify(b), `${label} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

initPackingDB();
const db = getDB();
const now = utcNow();
const at = (sec) => new Date(Date.parse(now) + sec * 1000).toISOString().slice(0, 19) + 'Z';
const stateOf = (id) => db.prepare('SELECT state FROM pk_print_jobs WHERE id=?').get(id)?.state;
const SHA = (c) => c.repeat(64);

db.prepare(`INSERT INTO pk_pack_batches (id, tb_key, folder_name, work_date, slip_count, line_count,
  total_qty, match_status, status, worker, csv_sha256, imported_by, created_at, updated_at)
  VALUES (1, 'TB1', '出荷_32', '2026-08-28', 1, 1, 1, 'ok', 'packing', '大場', 'x', 't', ?, ?)`).run(now, now);
let reprintSeq = 0;
/** 再印刷の行を作る。by/printable を変えて「印刷してよいか」の境界を試す */
const newReprint = ({ by = 'manifest', printable = 1, token = null, createdAt = now } = {}) => {
  const seq = ++reprintSeq;
  return Number(db.prepare(`INSERT INTO pk_pack_reprints (batch_id, slip_seq, ne_slip_no, site_order_no,
    folder_name, recipient_name, requested_by, created_at, kind, pdf_token, pdf_by, pdf_printable, pdf_ink_ratio)
    VALUES (1, ?, ?, '503-0000000-0000000', '出荷_32', '川野', '大場', ?, 'reprint', ?, ?, ?, 0.28)`)
    .run(seq, `15388${String(seq).padStart(2, '0')}`, createdAt,
      token ?? `tok${seq}`, by, printable).lastInsertRowid);
};

const agent = createDevice('出荷PC 印刷エージェント', 'test', {
  kind: 'agent', printerName: 'Munbyn ITPP941(300DPI)',
});
const agentRow = db.prepare('SELECT * FROM pk_pack_devices WHERE id=?').get(agent.id);
const agent2 = createDevice('出荷PC2', 'test', { kind: 'agent', printerName: 'Munbyn 2' });
const agent2Row = db.prepare('SELECT * FROM pk_pack_devices WHERE id=?').get(agent2.id);
const ipad = createDevice('梱包iPad', 'test');

console.log('── 🚨 印刷してよいものだけが積まれる (安全条件は enqueue の SQL 側) ──');
{
  ok(enqueuePrintJob(newReprint({ by: 'position' }), { pdfSha256: SHA('a') }) === null,
    '位置推定で特定したページは積めない (別人の送り状を掴み得る)');
  ok(enqueuePrintJob(newReprint({ printable: 0 }), { pdfSha256: SHA('a') }) === null,
    '白紙検査を通っていないものは積めない');
  ok(enqueuePrintJob(newReprint({ by: 'slip_no' }), { pdfSha256: SHA('a') }) === null,
    'テキスト照合で見つけたものも積めない (manifest だけ)');
  ok(enqueuePrintJob(999999, { pdfSha256: SHA('a') }) === null, '存在しない再印刷は積めない');
  ok(enqueuePrintJob(newReprint(), { pdfSha256: 'ぐちゃぐちゃ' }) === null, 'sha256 が不正なら積まない');
  // isPrintable は画面表示用 — 実際の境界は SQL 側にあることを明示しておく
  ok(isPrintable({ pdf_printable: 1, pdf_by: 'manifest', pdf_token: 'x' }) === true, 'isPrintable: 通る条件');
  ok(isPrintable({ pdf_printable: 1, pdf_by: 'position', pdf_token: 'x' }) === false, 'isPrintable: 位置推定は false');
}

console.log('\n── enqueue: 1再印刷につき1ジョブ (連打・通知再送で二重に出さない) ──');
{
  const rid = newReprint();
  const a = enqueuePrintJob(rid, { pdfSha256: SHA('a') });
  const b = enqueuePrintJob(rid, { pdfSha256: SHA('a') });
  ok(a.created === true, '1回目は積まれる');
  eq({ created: b.created, sameId: b.id === a.id }, { created: false, sameId: true }, '2回目は積まれず同じジョブを指す');
}

console.log('\n── 正常系: lease → PDF受け取り → 投入報告 → 完了 ──');
{
  const job = leaseNextJob(agentRow);
  ok(job !== null, 'queued を1件 lease できる');
  eq(job.printerName, 'Munbyn ITPP941(300DPI)', 'プリンター名は端末に紐づく (エージェントの申告ではない)');
  eq(stateOf(job.id), 'leased', 'state=leased');
  ok(leaseNextJob(agentRow) === null, '他に queued が無ければ null (204)');

  ok(claimPdfForPrint(job.id, { deviceId: agent2Row.id, leaseToken: job.leaseToken }) === null,
    '別端末にはPDFを渡さない');
  ok(claimPdfForPrint(job.id, { deviceId: agentRow.id, leaseToken: null }) === null,
    'lease token 無しではPDFを渡さない (端末認証だけでは不十分)');
  ok(claimPdfForPrint(job.id, { deviceId: agentRow.id, leaseToken: 'にせもの' }) === null,
    'lease token 不一致は弾く');
  ok(claimPdfForPrint(job.id, { deviceId: agentRow.id, leaseToken: job.leaseToken, now: at(9999) }) === null,
    'lease 期限切れ後はPDFを渡さない');

  ok(claimPdfForPrint(job.id, { deviceId: agentRow.id, leaseToken: job.leaseToken }) !== null, 'PDFを渡す');
  eq(stateOf(job.id), 'dispatched', 'PDFを渡した時点で dispatched (ここから自動再配布しない)');
  ok(claimPdfForPrint(job.id, { deviceId: agentRow.id, leaseToken: job.leaseToken }) !== null,
    '同じ lease の取り直し (通信断のリトライ) は許す');

  eq(markSubmitted(job.id, { deviceId: agentRow.id, leaseToken: 'にせもの' }).ok, false, '偽 lease の投入報告は拒否');
  eq(markSubmitted(job.id, { deviceId: agent2Row.id, leaseToken: job.leaseToken }).ok, false, '別端末の投入報告は拒否');
  eq(markSubmitted(job.id, { deviceId: agentRow.id, leaseToken: job.leaseToken, spoolJobId: '42' }).ok,
    true, 'スプーラー投入を報告できる');
  eq(stateOf(job.id), 'submitted', 'state=submitted');
  eq(markFinished(job.id, { deviceId: agentRow.id, leaseToken: job.leaseToken, ok: true }).ok, true, '完了報告');
  eq(stateOf(job.id), 'completed', 'state=completed');
}

console.log('\n── 🚨 PDFを渡した後は、報告前に落ちても自動で配り直さない (二重印刷防止) ──');
{
  const { id } = enqueuePrintJob(newReprint(), { pdfSha256: SHA('b') });
  const job = leaseNextJob(agentRow);
  claimPdfForPrint(id, { deviceId: agentRow.id, leaseToken: job.leaseToken });
  // ここでエージェントPCが落ちた (スプーラーには入っているかもしれない) 想定
  ok(leaseNextJob(agentRow, { now: at(9999) })?.id !== id, 'lease期限を過ぎても dispatched は再配布されない');
  ok(leaseNextJob(agent2Row, { now: at(9999) })?.id !== id, '別のエージェントにも配らない');
  eq(stateOf(id), 'dispatched', 'dispatched のまま');
  // 代わりに「結果不明」として人に知らせる
  sweepPrintJobs({ now: at(DISPATCHED_TIMEOUT_SEC + 1) });
  eq(stateOf(id), 'unknown', '一定時間で unknown (自動再投入はしない)');
  ok(alertTextFor({ ...db.prepare('SELECT * FROM pk_print_jobs WHERE id=?').get(id) })
    .includes('自動では刷り直していません'), '通知文で「刷り直していない」と伝える');
}

console.log('\n── 🚨 遅れて届いた古い報告が、新しい lease を乗っ取らない ──');
{
  const { id } = enqueuePrintJob(newReprint(), { pdfSha256: SHA('c') });
  const old = leaseNextJob(agentRow);
  // 期限切れ → 別のエージェントが取り直す (PDFはまだ渡していないので配り直してよい)
  const fresh = leaseNextJob(agent2Row, { now: at(9999) });
  eq(fresh.id, id, '期限切れの leased は配り直される');
  ok(old.leaseToken !== fresh.leaseToken, 'lease token は取り直しで変わる');
  eq(markSubmitted(id, { deviceId: agentRow.id, leaseToken: old.leaseToken, now: at(9999) }).ok, false,
    '古い lease の投入報告は通らない');
  eq(markFinished(id, { deviceId: agentRow.id, leaseToken: old.leaseToken, ok: true, now: at(9999) }).ok, false,
    '古い lease の完了報告も通らない');
  eq(stateOf(id), 'leased', '新しい lease の状態が壊されていない');
}

console.log('\n── lease 期限切れの繰り返し: 上限を超えたら人に投げる ──');
{
  const { id } = enqueuePrintJob(newReprint(), { pdfSha256: SHA('d') });
  // 先に積んである別ジョブを片付けてから (ORDER BY id で先頭を取るため)
  db.prepare("UPDATE pk_print_jobs SET state='completed', finished_at=?, alerted_state='completed' WHERE id<? AND state IN ('queued','leased')")
    .run(now, id);
  for (let i = 1; i <= MAX_ATTEMPTS; i++) {
    const j = leaseNextJob(agentRow, { now: at(i * 1000) });
    eq({ i, leased: j?.id === id, attempt: j?.attempt }, { i, leased: true, attempt: i },
      `${i}回目の lease (エージェントが報告せず落ちた想定)`);
  }
  ok(leaseNextJob(agentRow, { now: at(99999) }) === null, `${MAX_ATTEMPTS}回で打ち止め`);
  eq(stateOf(id), 'failed', 'state=failed (無限に配り続けない)');
  // 🚨 打ち止めが誰にも伝わらないと「出てこないのに気づかない」になる
  ok(pendingAlerts().some((j) => j.id === id), '打ち止めの failed も通知対象になる');
}

console.log('\n── 🚨 滞留: 手で刷ってもらう前に自動配布を止める ──');
{
  const { id } = enqueuePrintJob(newReprint(), { pdfSha256: SHA('e') });
  const s0 = sweepPrintJobs({ now: at(1) });
  eq({ manual: s0.manual, unknown: s0.unknown }, { manual: 0, unknown: 0 }, '積んだ直後は何もしない');
  sweepPrintJobs({ now: at(STALE_QUEUED_SEC + 1) });
  eq(stateOf(id), 'manual', `queued のまま ${STALE_QUEUED_SEC}秒で manual へ`);
  // ここが肝: 「手で刷って」と伝えた後にエージェントが復帰しても、もう自動では出ない
  ok(leaseNextJob(agentRow, { now: at(STALE_QUEUED_SEC + 2) })?.id !== id,
    '復帰したエージェントは manual のジョブを拾わない (手動印刷との二重を防ぐ)');
  const job = db.prepare('SELECT * FROM pk_print_jobs WHERE id=?').get(id);
  ok(alertTextFor(job).includes('自動印刷は取り消した'), '通知文で「二重には出ない」と伝える');
}

console.log('\n── 🚨 通知は送れるまで諦めない (webhook が落ちていた分を捨てない) ──');
{
  const { id } = enqueuePrintJob(newReprint(), { pdfSha256: SHA('f') });
  sweepPrintJobs({ now: at(STALE_QUEUED_SEC + 1) });
  ok(pendingAlerts().some((j) => j.id === id), '通知前は未通知として残る');
  // 送信に失敗した (markAlerted を呼ばなかった) → 次の周回でも対象のまま
  ok(pendingAlerts().some((j) => j.id === id), '送信に失敗した分は次の周回でも通知対象');
  markAlerted(id, 'manual');
  ok(!pendingAlerts().some((j) => j.id === id), '送れたら二度は鳴らさない');
  // 状態が変われば改めて知らせる
  db.prepare("UPDATE pk_print_jobs SET state='unknown' WHERE id=?").run(id);
  ok(pendingAlerts().some((j) => j.id === id), '状態が変わったら改めて通知対象');
}

console.log('\n── 再滞留も検知する ──');
{
  const { id } = enqueuePrintJob(newReprint(), { pdfSha256: SHA('0') });
  const j = leaseNextJob(agentRow);
  eq(j.id, id, '対象を lease');
  // lease 期限切れで queued に戻る → 通知済みマークが残っていると2度目が黙る
  db.prepare("UPDATE pk_print_jobs SET alerted_state='manual' WHERE id=?").run(id);
  sweepPrintJobs({ now: at(LEASE_SEC + 1) });              // 回収されて queued に戻る
  eq(stateOf(id), 'queued', '期限切れで queued に戻る');
  sweepPrintJobs({ now: at(LEASE_SEC + STALE_QUEUED_SEC + 2) });
  eq(stateOf(id), 'manual', '2度目の滞留');
  ok(pendingAlerts().some((x) => x.id === id), '再び滞留したら改めて通知対象になる');
}

console.log('\n── 🚨 古い依頼でも、初回の lease 失敗で自動印刷を諦めない ──');
{
  // 「積まれてから時間が経っているが、いま初めて lease されたジョブ」。
  // 滞留の起算点を created_at にすると、lease 期限切れで queued に戻った瞬間に
  // 同じ周回で manual にされ、試行上限に達する前に自動印刷を諦めてしまう
  const { id } = enqueuePrintJob(newReprint(), { pdfSha256: SHA('6') });
  db.prepare("UPDATE pk_print_jobs SET state='manual' WHERE id<? AND state='queued'").run(id);
  const T = STALE_QUEUED_SEC * 5;               // 積んでからだいぶ経っている
  eq(leaseNextJob(agentRow, { now: at(T) })?.id, id, '古いジョブでも lease できる');
  // lease が切れた直後の sweep。回収されて queued に戻るが、まだ諦めてはいけない
  sweepPrintJobs({ now: at(T + LEASE_SEC + 1) });
  eq(stateOf(id), 'queued', '回収された直後に manual にはしない (再試行の機会を残す)');
  eq(db.prepare('SELECT attempt_count FROM pk_print_jobs WHERE id=?').get(id).attempt_count, 1,
    '試行回数は1回目のまま');
  eq(leaseNextJob(agentRow, { now: at(T + LEASE_SEC + 2) })?.id, id, '2回目の lease ができる');
}

console.log('\n── 🚨 エージェントが全滅しても leased が残り続けない (dead-man) ──');
{
  const { id } = enqueuePrintJob(newReprint(), { pdfSha256: SHA('2') });
  db.prepare("UPDATE pk_print_jobs SET state='manual' WHERE id<? AND state='queued'").run(id);
  const j = leaseNextJob(agentRow);
  eq(j.id, id, '対象を lease');
  // ここでエージェントが全滅し、以後 /print/next が一度も呼ばれない。
  // 回収が lease 取得の中にしか無いと leased のまま永久に残り、通知も出ない
  sweepPrintJobs({ now: at(LEASE_SEC + 1) });
  eq(stateOf(id), 'queued', 'ポーラーだけでも期限切れ lease を回収する');
  sweepPrintJobs({ now: at(LEASE_SEC + STALE_QUEUED_SEC + 2) });
  eq(stateOf(id), 'manual', 'その後ちゃんと滞留として人に回る');
  ok(pendingAlerts().some((x) => x.id === id), '通知対象になる (誰も気づかない状態にならない)');
}

console.log('\n── 🚨 PDFを渡せなかったときは「結果不明」にしない ──');
{
  const { id } = enqueuePrintJob(newReprint(), { pdfSha256: SHA('3') });
  db.prepare("UPDATE pk_print_jobs SET state='manual' WHERE id<? AND state='queued'").run(id);
  const job = leaseNextJob(agentRow);
  ok(findLeasedJob(id, { deviceId: agentRow.id, leaseToken: job.leaseToken }) !== null,
    '読むだけの確認では状態を動かさない');
  eq(stateOf(id), 'leased', '確認しただけでは leased のまま');
  ok(failBeforeDispatch(id, { deviceId: agentRow.id, leaseToken: job.leaseToken, error: 'PDFがありません' }),
    'PDFを渡せないと分かったら failed にできる');
  eq(stateOf(id), 'failed', '1バイトも渡していないので「結果不明」ではなく失敗');
  ok(alertTextFor(db.prepare('SELECT * FROM pk_print_jobs WHERE id=?').get(id)).includes('手動で印刷'),
    '手で刷ってくださいと伝える');
}

console.log('\n── 🚨 印刷に手間取っても完了報告が弾かれない (報告期限は lease と別) ──');
{
  const { id } = enqueuePrintJob(newReprint(), { pdfSha256: SHA('4') });
  db.prepare("UPDATE pk_print_jobs SET state='manual' WHERE id<? AND state='queued'").run(id);
  const job = leaseNextJob(agentRow);
  // lease 期限ぎりぎりでPDFを受け取り、そこから印刷に時間がかかった想定
  ok(claimPdfForPrint(id, { deviceId: agentRow.id, leaseToken: job.leaseToken, now: at(LEASE_SEC - 10) }) !== null,
    '期限内ならPDFを渡す');
  eq(markSubmitted(id, { deviceId: agentRow.id, leaseToken: job.leaseToken, spoolJobId: '7', now: at(LEASE_SEC + 30) }).ok,
    true, '最初の lease 期限を過ぎても投入報告は通る');
  eq(markFinished(id, { deviceId: agentRow.id, leaseToken: job.leaseToken, ok: true, now: at(LEASE_SEC + 60) }).ok,
    true, '完了報告も通る (正常に出た送り状を結果不明にしない)');
  eq(stateOf(id), 'completed', 'state=completed');
}

console.log('\n── 応答が失われた再送を成功として受ける (冪等) ──');
{
  const { id } = enqueuePrintJob(newReprint(), { pdfSha256: SHA('5') });
  db.prepare("UPDATE pk_print_jobs SET state='manual' WHERE id<? AND state='queued'").run(id);
  const job = leaseNextJob(agentRow);
  claimPdfForPrint(id, { deviceId: agentRow.id, leaseToken: job.leaseToken });
  const a = markSubmitted(id, { deviceId: agentRow.id, leaseToken: job.leaseToken, spoolJobId: '8' });
  const b = markSubmitted(id, { deviceId: agentRow.id, leaseToken: job.leaseToken, spoolJobId: '8' });
  eq({ a: a.ok, b: b.ok, replayed: b.replayed }, { a: true, b: true, replayed: true },
    '同じ投入報告の再送は成功扱い (409で返すとエージェントが復旧できない)');
  markFinished(id, { deviceId: agentRow.id, leaseToken: job.leaseToken, ok: true });
  eq(markFinished(id, { deviceId: agentRow.id, leaseToken: job.leaseToken, ok: true }).replayed, true,
    '完了報告の再送も成功扱い');
  eq(markFinished(id, { deviceId: agent2Row.id, leaseToken: job.leaseToken, ok: true }).ok, false,
    '別端末の再送は受け付けない');
  eq(getJobStatusFor(id, agentRow.id).state, 'completed', '再起動後に状態を照会できる');
  ok(getJobStatusFor(id, agent2Row.id) === null, '自分が持っていないジョブは照会できない');
}


console.log('\n── 失敗報告 ──');
{
  const { id } = enqueuePrintJob(newReprint(), { pdfSha256: SHA('1') });
  db.prepare("UPDATE pk_print_jobs SET state='manual' WHERE id<? AND state='queued'").run(id);
  const job = leaseNextJob(agentRow);
  eq(job.id, id, '対象を lease');
  eq(markFinished(id, { deviceId: agentRow.id, leaseToken: job.leaseToken, ok: true }).ok, false,
    'PDFも受け取らずに「刷れた」は受け付けない');
  eq(markFinished(id, { deviceId: agentRow.id, leaseToken: job.leaseToken, ok: 'true' }).ok, false,
    'ok が真偽値でなければ受け付けない (欠落を成功扱いしない)');
  eq(markFinished(id, { deviceId: agentRow.id, leaseToken: job.leaseToken, ok: false, error: 'プリンターが見つかりません' }).ok,
    true, 'スプーラー投入前の失敗は報告できる');
  eq(stateOf(id), 'failed', 'state=failed');
  ok(leaseNextJob(agentRow, { now: at(99999) })?.id !== id, '失敗したジョブは自動で刷り直さない (人が判断する)');
  ok(String(listPrintJobs(50).find((x) => x.id === id).error).includes('プリンター'), '失敗理由が残る');
}

console.log('\n── DBが不正な状態を受け付けない ──');
{
  let threw = false;
  try { db.prepare("UPDATE pk_print_jobs SET state='てきとう' WHERE id=1").run(); } catch { threw = true; }
  ok(threw, '未知の state は CHECK で弾かれる (監視の対象外になるのを防ぐ)');
  threw = false;
  try { db.prepare("INSERT INTO pk_pack_devices (token_hash,label,created_by,created_at,kind) VALUES ('h','l','a',?, 'なぞ')").run(now); } catch { threw = true; }
  ok(threw, '未知の端末種別も弾かれる');
}

console.log('\n── heartbeat ──');
{
  recordHeartbeat(agentRow.id, '準備完了');
  const d = db.prepare('SELECT heartbeat_at, heartbeat_note, kind FROM pk_pack_devices WHERE id=?').get(agentRow.id);
  ok(!!d.heartbeat_at, '生存時刻が記録される');
  eq({ note: d.heartbeat_note, kind: d.kind }, { note: '準備完了', kind: 'agent' }, 'メモと種別');
  const i = db.prepare('SELECT kind, printer_name FROM pk_pack_devices WHERE id=?').get(ipad.id);
  eq({ kind: i.kind, printer: i.printer_name }, { kind: 'ipad', printer: null }, 'iPad は agent ではない');
}

try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* 無視 */ }
console.log(`\n${failed === 0 ? '✅ 全テスト PASS' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
