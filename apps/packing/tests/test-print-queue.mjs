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

const { initPackingDB, getDB, utcNow, createDevice, setAgentPrinters, revokeDevice } = await import('../db.js');
const {
  enqueuePrintJob, leaseNextJob, findLeasedJob, claimPdfForPrint, failBeforeDispatch,
  markSubmitted, markFinished, recordHeartbeat, isPrintable, sweepPrintJobs,
  reclaimExpiredLeases, pendingAlerts, markAlerted, alertTextFor, getJobStatusFor,
  listPrintJobs, setPrintRoute, printerForSlug, listPrintRoutes, normalizeSlug,
  MAX_ATTEMPTS, STALE_QUEUED_SEC, DISPATCHED_TIMEOUT_SEC, LEASE_SEC,
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
// 出荷PC = AES の Munbyn。倉庫PC = 定形外の QL-720 (別のPCに別のプリンターがぶら下がる)
setAgentPrinters(agent.id, ['Munbyn ITPP941(300DPI)']);
setAgentPrinters(agent2.id, ['Brother QL-720']);
setPrintRoute('aes', 'Munbyn ITPP941(300DPI)', 'test');
setPrintRoute('teikeigai', 'Brother QL-720', 'test');

console.log('── 🚨 印刷してよいものだけが積まれる (安全条件は enqueue の SQL 側) ──');
{
  ok(enqueuePrintJob(newReprint({ by: 'position' }), { pdfSha256: SHA('a'), slug: 'aes' }) === null,
    '位置推定で特定したページは積めない (別人の送り状を掴み得る)');
  ok(enqueuePrintJob(newReprint({ printable: 0 }), { pdfSha256: SHA('a'), slug: 'aes' }) === null,
    '白紙検査を通っていないものは積めない');
  ok(enqueuePrintJob(newReprint({ by: 'slip_no' }), { pdfSha256: SHA('a'), slug: 'aes' }) === null,
    'テキスト照合で見つけたものも積めない (manifest だけ)');
  ok(enqueuePrintJob(999999, { pdfSha256: SHA('a'), slug: 'aes' }) === null, '存在しない再印刷は積めない');
  ok(enqueuePrintJob(newReprint(), { pdfSha256: 'ぐちゃぐちゃ', slug: 'aes' }) === null, 'sha256 が不正なら積まない');
  // isPrintable は画面表示用 — 実際の境界は SQL 側にあることを明示しておく
  ok(isPrintable({ pdf_printable: 1, pdf_by: 'manifest', pdf_token: 'x' }) === true, 'isPrintable: 通る条件');
  ok(isPrintable({ pdf_printable: 1, pdf_by: 'position', pdf_token: 'x' }) === false, 'isPrintable: 位置推定は false');
}

console.log('\n── 🚨 出力先が決まっていない引当分類は自動印刷しない ──');
{
  // 引当分類によって送り状を出すソフトが違う (DENZOU / ヤマトB2 / ゆうプリR / 汎用送り状)。
  // 対応表に無いものを「とりあえずどこかに刷る」と、別のプリンターから他人の送り状が出る
  const r1 = enqueuePrintJob(newReprint(), { pdfSha256: SHA('7'), slug: 'nekoposu' });
  eq({ id: r1.id, reason: r1.reason }, { id: null, reason: 'no_route' },
    '対応表に無い分類 (ネコポス) は積まない');
  const r2 = enqueuePrintJob(newReprint(), { pdfSha256: SHA('7'), slug: null });
  eq({ id: r2.id, reason: r2.reason }, { id: null, reason: 'no_slug' },
    '引当分類が読めなかったら積まない (どのソフトの送り状か分からない)');

  // 対応表を入れれば載る。出力先はジョブに焼き付く
  setPrintRoute('nekoposu', 'ヤマトB2プリンター', 'test');
  const r3 = enqueuePrintJob(newReprint(), { pdfSha256: SHA('7'), slug: 'nekoposu' });
  eq(r3.printer, 'ヤマトB2プリンター', '対応表の出力先がジョブに入る');
  // 空にすると自動印刷が止まる (どこに出るか決まっていない状態に戻す)
  setPrintRoute('nekoposu', '', 'test');
  ok(printerForSlug('nekoposu') === null, '空欄にすると自動印刷しない状態に戻る');
  eq(enqueuePrintJob(newReprint(), { pdfSha256: SHA('7'), slug: 'nekoposu' }).reason, 'no_route',
    '外した分類は積まれない');
  // 入力の妥当性
  eq(setPrintRoute('../etc', 'x', 'test').ok, false, '変な slug は受け付けない');
  eq(setPrintRoute('unknown_soft', 'x', 'test').ok, false, '知らない引当分類は受け付けない');
  // 🚨 読み取り側 (CSVファイル名) は小文字化するので、登録側で大文字を許すと永久に一致せず
  //    「理由の見えない印刷されない」になる
  setPrintRoute('AES', 'Munbyn ITPP941(300DPI)', 'test');
  eq(printerForSlug('aes'), 'Munbyn ITPP941(300DPI)', '大文字で登録しても小文字の slug と一致する');
  eq(normalizeSlug(' NekoPosu '), 'nekoposu', '前後の空白と大文字を吸収する');
  ok(normalizeSlug('yamato') === null, '知らない分類は null');
  // その名前の端末が無い設定は「積んでも誰も取りに来ない」ので保存時に知らせる
  ok(setPrintRoute('60size', '誰も持っていないプリンター', 'test').orphan === true,
    '登録端末が無いプリンターを指定したら警告を返す');
  ok(setPrintRoute('60size', 'Munbyn ITPP941(300DPI)', 'test').orphan === false,
    '端末が持っているプリンターなら警告なし');
  setPrintRoute('60size', '', 'test');
  ok(listPrintRoutes().some((x) => x.slug === 'aes'), '一覧に出る');
}

console.log('\n── 🚨 プリンター名の持ち主が変わっても、積んだジョブは別の実機から出ない ──');
{
  // プリンター名はPCごとのローカル名。UNIQUE は「同時点で2台に無い」ことしか保証しないので、
  // PC-A から名前を外して PC-B に付け直すと、PC-A 向けに積んであったジョブを
  // PC-B が取れてしまう = 別の物理プリンターから他人の送り状が出る
  setPrintRoute('60size', 'Munbyn 予備', 'test');
  setAgentPrinters(agent.id, ['Munbyn ITPP941(300DPI)', 'Munbyn 予備']);
  const { id } = enqueuePrintJob(newReprint(), { pdfSha256: SHA('9'), slug: '60size' });
  db.prepare("UPDATE pk_print_jobs SET state='manual' WHERE id<? AND state='queued'").run(id);
  eq(db.prepare('SELECT target_device_id FROM pk_print_jobs WHERE id=?').get(id).target_device_id,
    agent.id, '積んだ時点の端末がジョブに焼き付く');

  // 未処理のジョブが残っている名前は手放せない (ここが最初の防波堤)
  const busy = setAgentPrinters(agent.id, ['Munbyn ITPP941(300DPI)']);
  eq({ ok: busy.ok, reason: busy.reason }, { ok: false, reason: 'printer_busy' },
    '印刷待ちが残っている名前は外せない');

  // 仮に外せてしまっても (ジョブが片づいた後に付け替える等)、別端末は取れない
  db.prepare('DELETE FROM pk_print_agent_printers WHERE device_id=? AND printer_name=?')
    .run(agent.id, 'Munbyn 予備');
  setAgentPrinters(agent2.id, ['Brother QL-720', 'Munbyn 予備']);
  ok(leaseNextJob(agent2Row)?.id !== id, '名前を引き継いだ別のPCでも、積んだ先が違うジョブは取れない');
  eq(stateOf(id), 'queued', 'ジョブは queued のまま (誤った実機から出るくらいなら出さない)');
  // 誰も取れないまま滞留するが、それは manual として人に回る (黙って消えない)
  sweepPrintJobs({ now: at(STALE_QUEUED_SEC + 1) });
  eq(stateOf(id), 'manual', '取り手がいなければ手動印刷へ回る');
  setAgentPrinters(agent2.id, ['Brother QL-720']);
  setPrintRoute('60size', '', 'test');
}

console.log('\n── 失効した端末はプリンター名を手放す ──');
{
  // 握ったままだと、代わりのPCを同じ名前で登録できない
  const old = createDevice('壊れたPC', 'test', { kind: 'agent' });
  eq(setAgentPrinters(old.id, ['予備プリンター']).ok, true, '登録できる');
  const other = createDevice('代わりのPC', 'test', { kind: 'agent' });
  eq(setAgentPrinters(other.id, ['予備プリンター']).reason, 'duplicate_printer', '使用中は登録できない');
  revokeDevice(old.id);
  eq(setAgentPrinters(other.id, ['予備プリンター']).ok, true, '失効させれば同じ名前で登録できる');
}

console.log('\n── 🚨 同じプリンター名を2台のPCに登録させない ──');
{
  // Windowsのプリンター名はPCごとのローカル名。出荷PCと倉庫PCの両方に同じ名前があると、
  // どちらも同じジョブを取れてしまい「別の物理プリンターから黙って送り状が出る」
  const other = createDevice('別のPC', 'test', { kind: 'agent' });
  const r = setAgentPrinters(other.id, ['Munbyn ITPP941(300DPI)']);
  eq({ ok: r.ok, reason: r.reason }, { ok: false, reason: 'duplicate_printer' },
    '他の端末が使っている名前は登録できない');
  ok(String(r.message).includes('別の端末'), '理由が人に分かる文言で返る');
  // 不正な項目を黙って捨てない (登録したつもりが入っていない = 印刷されないのに気づけない)
  eq(setAgentPrinters(other.id, ['ok-printer', '']).ok, false, '空文字が混ざったら全体を拒否');
  eq(setAgentPrinters(other.id, ['x'.repeat(121)]).ok, false, '長すぎる名前は拒否');
  eq(setAgentPrinters(ipad.id, ['どこか']).reason, 'not_agent', 'iPad にはプリンターを付けられない');
  eq(setAgentPrinters(999999, ['どこか']).reason, 'not_agent', '存在しない端末には付けられない');
  // 自分自身の付け替えはできる (増減のたびに登録し直さなくてよい)
  eq(setAgentPrinters(agent.id, ['Munbyn ITPP941(300DPI)', 'Munbyn 予備']).ok, true, '自分の分は付け替えられる');
  eq(setAgentPrinters(agent.id, ['Munbyn ITPP941(300DPI)']).printers, ['Munbyn ITPP941(300DPI)'], '減らせる');
}

console.log('\n── 🚨 そのPCに無いプリンター宛のジョブは渡さない ──');
{
  // 出荷PC (Munbyn) と 倉庫PC (QL-720) は別のPC。定形外のジョブを出荷PCに渡すと
  // 存在しないプリンター名で印刷することになる
  const { id } = enqueuePrintJob(newReprint(), { pdfSha256: SHA('8'), slug: 'teikeigai' });
  db.prepare("UPDATE pk_print_jobs SET state='manual' WHERE id<? AND state='queued'").run(id);
  ok(leaseNextJob(agentRow)?.id !== id, '出荷PC (Munbynだけ) は QL-720 宛のジョブを取れない');
  eq(leaseNextJob(agent2Row)?.id, id, '倉庫PC (QL-720) なら取れる');
  eq(db.prepare('SELECT printer_name, slug FROM pk_print_jobs WHERE id=?').get(id),
    { printer_name: 'Brother QL-720', slug: 'teikeigai' }, '出力先と分類がジョブに残る');
  // プリンターが1つも登録されていない端末には何も渡さない
  const bare = createDevice('設定途中のPC', 'test', { kind: 'agent' });
  const bareRow = db.prepare('SELECT * FROM pk_pack_devices WHERE id=?').get(bare.id);
  ok(leaseNextJob(bareRow) === null, 'プリンター未登録の端末には配らない');
}


console.log('\n── enqueue: 1再印刷につき1ジョブ (連打・通知再送で二重に出さない) ──');
{
  const rid = newReprint();
  const a = enqueuePrintJob(rid, { pdfSha256: SHA('a'), slug: 'aes' });
  const b = enqueuePrintJob(rid, { pdfSha256: SHA('a'), slug: 'aes' });
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
  const { id } = enqueuePrintJob(newReprint(), { pdfSha256: SHA('b'), slug: 'aes' });
  const job = leaseNextJob(agentRow);
  claimPdfForPrint(id, { deviceId: agentRow.id, leaseToken: job.leaseToken });
  // ここでエージェントPCが落ちた (スプーラーには入っているかもしれない) 想定
  ok(leaseNextJob(agentRow, { now: at(9999) })?.id !== id, 'lease期限を過ぎても dispatched は再配布されない');
  ok(leaseNextJob(agentRow, { now: at(9999) })?.id !== id, '同じエージェントにも配らない');
  eq(stateOf(id), 'dispatched', 'dispatched のまま');
  // 代わりに「結果不明」として人に知らせる
  sweepPrintJobs({ now: at(DISPATCHED_TIMEOUT_SEC + 1) });
  eq(stateOf(id), 'unknown', '一定時間で unknown (自動再投入はしない)');
  ok(alertTextFor({ ...db.prepare('SELECT * FROM pk_print_jobs WHERE id=?').get(id) })
    .includes('自動では刷り直していません'), '通知文で「刷り直していない」と伝える');
}

console.log('\n── 🚨 遅れて届いた古い報告が、新しい lease を乗っ取らない ──');
{
  const { id } = enqueuePrintJob(newReprint(), { pdfSha256: SHA('c'), slug: 'aes' });
  const old = leaseNextJob(agentRow);
  // 期限切れ → 別のエージェントが取り直す (PDFはまだ渡していないので配り直してよい)
  const fresh = leaseNextJob(agentRow, { now: at(9999) });
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
  const { id } = enqueuePrintJob(newReprint(), { pdfSha256: SHA('d'), slug: 'aes' });
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
  const { id } = enqueuePrintJob(newReprint(), { pdfSha256: SHA('e'), slug: 'aes' });
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
  const { id } = enqueuePrintJob(newReprint(), { pdfSha256: SHA('f'), slug: 'aes' });
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
  const { id } = enqueuePrintJob(newReprint(), { pdfSha256: SHA('0'), slug: 'aes' });
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
  const { id } = enqueuePrintJob(newReprint(), { pdfSha256: SHA('6'), slug: 'aes' });
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
  const { id } = enqueuePrintJob(newReprint(), { pdfSha256: SHA('2'), slug: 'aes' });
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
  const { id } = enqueuePrintJob(newReprint(), { pdfSha256: SHA('3'), slug: 'aes' });
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
  const { id } = enqueuePrintJob(newReprint(), { pdfSha256: SHA('4'), slug: 'aes' });
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
  const { id } = enqueuePrintJob(newReprint(), { pdfSha256: SHA('5'), slug: 'aes' });
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


console.log('\n── 🚨「刷れなかった」と「紙が出たか分からない」を混ぜない ──');
{
  // failed の通知は「フォルダから手動で印刷してください」と言う。実は紙が出ていたケースを
  // ここに流すと、現場がもう1枚刷って**二重印刷**になる (=最重要要件に反する)。
  // エージェントが確信を持てない場合は uncertain=true で報告させ、unknown にする
  const mk = (sha) => {
    const { id } = enqueuePrintJob(newReprint(), { pdfSha256: SHA(sha), slug: 'aes' });
    db.prepare("UPDATE pk_print_jobs SET state='manual' WHERE id<? AND state='queued'").run(id);
    const job = leaseNextJob(agentRow);
    claimPdfForPrint(id, { deviceId: agentRow.id, leaseToken: job.leaseToken });
    return { id, lease: job.leaseToken };
  };

  const a = mk('a');
  eq(markFinished(a.id, { deviceId: agentRow.id, leaseToken: a.lease, ok: false, error: 'プリンターがありません' }).state,
    'failed', '確実に出ていない失敗は failed');
  ok(alertTextFor(db.prepare('SELECT * FROM pk_print_jobs WHERE id=?').get(a.id)).includes('手動で印刷'),
    'failed の通知は「手動で印刷してください」');

  const b = mk('b');
  eq(markFinished(b.id, { deviceId: agentRow.id, leaseToken: b.lease, ok: false,
    error: 'スプーラーに渡した後で落ちました', uncertain: true }).state,
    'unknown', '🚨 紙が出たか分からない失敗は unknown (failed にしない)');
  const t = alertTextFor(db.prepare('SELECT * FROM pk_print_jobs WHERE id=?').get(b.id));
  ok(t.includes('実物を確認'), 'unknown の通知は「実物を確認してください」');
  ok(!t.includes('手動で印刷'), '🚨 二重印刷を誘発する「手動で印刷してください」を言わない');
  ok(leaseNextJob(agentRow, { now: at(99999) })?.id !== b.id, 'unknown は自動で刷り直さない');
}

console.log('\n── 失敗報告 ──');
{
  const { id } = enqueuePrintJob(newReprint(), { pdfSha256: SHA('1'), slug: 'aes' });
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
