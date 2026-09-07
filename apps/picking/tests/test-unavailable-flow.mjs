/**
 * test-unavailable-flow.mjs — 🔴再ピックバッチの結果を梱包タスクへ正しく伝える (例外処理監査 PR-1・2026-09-05)
 *
 * 守りたいこと:
 *   ① 再ピックバッチで「他ロケで全量確保」→ タスクは fulfilled (以前は unavailable になっていた — 9/3・9/5 実発生)
 *   ② 再ピックバッチでは「後で取りに行く」を使えない (依頼が pending_binding で迷子になる — 9/1 実発生)
 *   ③ 「どこにもない」→ unavailable + 1階の全端末へ赤バナー (task_id・link つき)。配賦は作らない。部分確保は内訳を保存
 *   ④ back / cancel で取り消したら claimed / requested へ戻り、バナーは閉じる (Codex R1 High: 後から fulfilled に化けない)
 *   ⑤ 同期は状態から収束する = replay・障害後にバナーを作り直す (task_id で1本に集約)
 *   ⑥ 1階が受け取り済みなら back できない
 *   ⑦ 棚戻しキューは棚戻しだけ (再ピックは 🔴バッチに一本化 — Q4)
 *
 * 実行: node apps/picking/tests/test-unavailable-flow.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'picking-unavail-test-'));

const { initPickingDB, getDB, jstToday } = await import('../db.js');
const {
  applyEvent, bindPendingLaterRequests, reconcileRepickBatches, syncRepickTask, listFloorAlerts,
} = await import('../service.js');
const { initPackingDB } = await import('../../packing/db.js');
const psvc = await import('../../packing/service.js');

initPickingDB();
initPackingDB();
const db = getDB();
const now = new Date().toISOString().slice(0, 19) + 'Z';
const today = jstToday();

let passed = 0;
function t(name, fn) { fn(); passed++; console.log(`  ok: ${name}`); }
function throwsCode(fn, code, name) {
  try { fn(); assert.fail(`${name}: エラーにならなかった`); }
  catch (e) { assert.equal(e.code, code, `${name} (実際=${e.code || e.message})`); }
  passed++; console.log(`  ok: ${name}`);
}

let op = 0;
const ev = (batchId, event, extra = {}, worker = '星立夏') =>
  applyEvent(batchId, { opId: `t${++op}`, event, ...extra }, worker);
const lastOp = () => `t${op}`;

function mkPickBatch(tbNo, { sku, lineQty, slipQtys }) {
  const info = db.prepare(`INSERT INTO pk_batches
    (tb_no, hikiate_class, folder_name, work_date, instruct_date, composition,
     delivery_method, invoice_soft, line_count, slip_count, total_qty, status, validity,
     csv_sha256, imported_by, created_at, updated_at)
    VALUES (?, 'AES《単品》', '出荷_88', ?, NULL, '単品', NULL, NULL, 1, ?, ?, 'ready', 'valid', ?, 'test', ?, ?)`)
    .run(tbNo, today, slipQtys.length, lineQty, `sha-${tbNo}`, now, now);
  const batchId = Number(info.lastInsertRowid);
  db.prepare(`INSERT INTO pk_lines (batch_id, seq, location, block, sku, product_name, barcode, qty)
    VALUES (?, 1, '00100101', 'P3FA', ?, 'テスト商品', NULL, ?)`).run(batchId, sku, lineQty);
  const ins = db.prepare(`INSERT INTO pk_slip_lines (batch_id, slip_no, picking_no, ne_slip_no, sku, qty, location)
    VALUES (?, ?, NULL, ?, ?, ?, '00100101')`);
  slipQtys.forEach((q, i) => ins.run(batchId, `SP${tbNo}-${i + 1}`, `${tbNo}-NE${i + 1}`, sku, q));
  return batchId;
}
function mkPackBatch(tbNo, { sku, slipQtys }) {
  const info = db.prepare(`INSERT INTO pk_pack_batches
    (tb_key, folder_name, work_date, slip_count, line_count, total_qty, match_status, status, worker, csv_sha256, imported_by, created_at, updated_at)
    VALUES (?, '出荷_88', ?, ?, ?, ?, 'ok', 'packing', '三宅晴菜', ?, 'test', ?, ?)`)
    .run(tbNo, today, slipQtys.length, slipQtys.length, slipQtys.reduce((a, b) => a + b, 0), `psha-${tbNo}`, now, now);
  const batchId = Number(info.lastInsertRowid);
  slipQtys.forEach((q, i) => {
    const sid = Number(db.prepare(`INSERT INTO pk_pack_slips (batch_id, seq, ne_slip_no, slip_no, recipient_name, site_order_no, status, delivery_method)
      VALUES (?, ?, ?, ?, ?, ?, 'pending', '箱')`)
      .run(batchId, i + 1, `${tbNo}-NE${i + 1}`, `SP${tbNo}-${i + 1}`, `客${i + 1}`, `SO-${i + 1}`).lastInsertRowid);
    db.prepare('INSERT INTO pk_pack_lines (slip_id, sku, product_name, qty) VALUES (?, ?, ?, ?)').run(sid, sku, 'テスト商品', q);
  });
  return batchId;
}
/** ピッカーが「後で取りに行く」(qty 個) → 展開 → 🔴再ピックバッチ を作って返す。 */
function laterToRepick(tbNo, sku, qty = 1) {
  const pk = mkPickBatch(tbNo, { sku, lineQty: qty, slipQtys: [qty] });
  const pb = mkPackBatch(tbNo, { sku, slipQtys: [qty] });
  ev(pk, 'start');
  ev(pk, 'shortage', { lineSeq: 1, shortageQty: qty, altQty: 0, remaining: 'later' });
  bindPendingLaterRequests();
  reconcileRepickBatches();
  const task = db.prepare("SELECT * FROM pk_pack_tasks WHERE batch_id=? AND kind='repick' ORDER BY id DESC LIMIT 1").get(pb);
  const rb = db.prepare('SELECT * FROM pk_batches WHERE pack_task_id=?').get(task.id);
  assert.ok(rb, '🔴再ピックバッチが作られる');
  return { pk, pb, task, rb };
}
const taskRow = (id) => db.prepare('SELECT * FROM pk_pack_tasks WHERE id=?').get(id);
const taskStatus = (id) => taskRow(id).status;
const batchStatus = (id) => db.prepare('SELECT status FROM pk_batches WHERE id=?').get(id).status;
const stockoutAlerts = (taskId) => listFloorAlerts('to_packing').filter((a) => a.kind === 'stockout' && a.task_id === taskId);
const sync = (rbId, event, worker = '田中美波') => syncRepickTask(rbId, { event }, worker, psvc);

// ═══ ① 他ロケで全量確保 → fulfilled ═══════════════════════════════════════════
console.log('── 再ピックバッチで他ロケから全量確保 ──');
{
  const { task, rb } = laterToRepick('U1', 'sku-u1');
  t('start → claimed', () => {
    ev(rb.id, 'start', {}, '田中美波');
    const r = sync(rb.id, 'start');
    assert.deepEqual(r.actions, ['claim']);
    assert.equal(taskStatus(task.id), 'claimed');
  });
  t('欠品ボタン → 他ロケで1個確保 (残り0) → バッチ done・タスク fulfilled (在庫なしにならない)', () => {
    ev(rb.id, 'shortage', { lineSeq: 1, shortageQty: 1, altQty: 1, altBlock: 'P3FA', altLocation: '004-020-05', remaining: null }, '田中美波');
    const r = sync(rb.id, 'shortage');
    assert.equal(r.unavailable, null, '在庫なし通知は出ない');
    assert.equal(batchStatus(rb.id), 'done');
    assert.equal(taskStatus(task.id), 'fulfilled');
    assert.equal(taskRow(task.id).fulfilled_qty, 1);
    assert.equal(stockoutAlerts(task.id).length, 0, '赤バナーは出ない');
  });
  t('梱包側は受領待ち (repickReady) として見える / 同期を再実行しても変わらない (replay)', () => {
    assert.ok(psvc.listRepickReady().some((g) => g.tasks.some((x) => x.id === task.id)), 'fulfilled = 緑バナーの元');
    const r = sync(rb.id, 'shortage');
    assert.deepEqual(r.actions, []);
    assert.equal(taskStatus(task.id), 'fulfilled');
  });
}

// ═══ ② 再ピックバッチで「後で取りに行く」は拒否 ═════════════════════════════
console.log('── 再ピックバッチでの「後で取りに行く」 ──');
{
  const { rb, task } = laterToRepick('U2', 'sku-u2');
  ev(rb.id, 'start', {}, '田中美波');
  sync(rb.id, 'start');
  throwsCode(() => ev(rb.id, 'shortage', { lineSeq: 1, shortageQty: 1, altQty: 0, remaining: 'later' }, '田中美波'),
    'later_in_repick', '再ピックバッチの「後で取りに行く」は 400 later_in_repick');
  t('拒否されたので依頼 (pk_later_requests) は増えていない・バッチも進んでいない', () => {
    assert.equal(db.prepare("SELECT COUNT(*) c FROM pk_later_requests WHERE batch_id=?").get(rb.id).c, 0);
    assert.equal(db.prepare('SELECT status FROM pk_lines WHERE batch_id=? AND seq=1').get(rb.id).status, 'pending');
    assert.equal(taskStatus(task.id), 'claimed');
  });

  // ═══ ③ どこにもない → unavailable + 赤バナー ═══════════════════════════════
  console.log('── 再ピックバッチで「どこにもない」 ──');
  t('shortage(none) → タスク unavailable・配賦なし・バッチ done でも fulfill しない', () => {
    ev(rb.id, 'shortage', { lineSeq: 1, shortageQty: 1, altQty: 0, remaining: 'none' }, '田中美波');
    const r = sync(rb.id, 'shortage');
    assert.ok(r.unavailable, '在庫なし通知の情報が返る');
    assert.equal(r.unavailable.remaining, 1);
    assert.equal(taskStatus(task.id), 'unavailable');
    assert.equal(taskRow(task.id).unavailable_qty, 1);
    assert.equal(taskRow(task.id).fulfilled_qty, 0);
    assert.equal(db.prepare('SELECT COUNT(*) c FROM pk_shortage_allocations WHERE batch_id=?').get(rb.id).c, 0, '再ピックバッチには配賦を作らない');
    assert.equal(batchStatus(rb.id), 'done');
    // done 後の同期 (replay) でも fulfill に化けない・通知情報も付かない
    const r2 = sync(rb.id, 'shortage');
    assert.deepEqual(r2.actions, []);
    assert.equal(r2.unavailable, null, 'replay では GChat 用の情報は付かない');
    assert.equal(taskStatus(task.id), 'unavailable');
  });
  t('1階の全端末へ赤バナー (商品・伝票・数量・誰が) + 対象伝票へのリンク + task_id', () => {
    const a = stockoutAlerts(task.id);
    assert.equal(a.length, 1, 'stockout バナーは1本');
    assert.match(a[0].message, /在庫なし/);
    assert.match(a[0].message, /出荷_88 #1/);
    assert.match(a[0].message, /田中美波/);
    assert.equal(a[0].link, `/apps/packing/work/${task.batch_id}?seq=1`);
  });
  t('梱包側 getWorkState に在庫なしの報告 (stockoutBySlip) と確認可否 (stockoutAckSeqs) が出る', () => {
    const st = psvc.getWorkState(task.batch_id);
    assert.deepEqual(Object.keys(st.stockoutBySlip), ['1']);
    assert.equal(st.stockoutBySlip[1][0].sku, 'sku-u2');
    assert.equal(st.stockoutBySlip[1][0].claimed_by, '田中美波');
    assert.deepEqual(st.stockoutAckSeqs, [1]);
  });
  // ═══ ⑤ 障害・replay 後の自己修復 ═══════════════════════════════════════
  t('バナーが消えていても (障害) 同期でもう一度作る。残っていれば増やさない', () => {
    db.prepare("DELETE FROM pk_floor_alerts WHERE task_id=? AND kind='stockout'").run(task.id);
    sync(rb.id, 'shortage');
    assert.equal(stockoutAlerts(task.id).length, 1);
    sync(rb.id, 'next');
    assert.equal(stockoutAlerts(task.id).length, 1);
  });
}

// ═══ ④ back / cancel で取り消す ═══════════════════════════════════════════════
console.log('── back / cancel の同期 ──');
{
  const { rb, task } = laterToRepick('U4', 'sku-u4');
  ev(rb.id, 'start', {}, '田中美波');
  sync(rb.id, 'start');
  ev(rb.id, 'shortage', { lineSeq: 1, shortageQty: 1, altQty: 0, remaining: 'none' }, '田中美波');
  const shortOp = lastOp();
  sync(rb.id, 'shortage');
  assert.equal(taskStatus(task.id), 'unavailable');
  assert.equal(stockoutAlerts(task.id).length, 1);
  t('back (done → picking) → タスク claimed (resume)・部分数量クリア・赤バナーは閉じる', () => {
    ev(rb.id, 'back', { lineSeq: 1, undoOpId: shortOp }, '田中美波');
    assert.equal(batchStatus(rb.id), 'picking');
    const r = sync(rb.id, 'back');
    assert.deepEqual(r.actions, ['resume']);
    assert.equal(taskStatus(task.id), 'claimed');
    assert.equal(taskRow(task.id).unavailable_qty, null);
    assert.equal(stockoutAlerts(task.id).length, 0, 'バナーは resolved');
  });
  t('cancel (picking → ready) → タスク requested (reopen)', () => {
    ev(rb.id, 'cancel', {}, '田中美波');
    assert.equal(batchStatus(rb.id), 'ready');
    const r = sync(rb.id, 'cancel');
    assert.deepEqual(r.actions, ['reopen']);
    assert.equal(taskStatus(task.id), 'requested');
  });
  t('やり直して通常完了 → fulfilled (取り消した在庫なしは残らない)', () => {
    ev(rb.id, 'start', {}, '田中美波');
    sync(rb.id, 'start');
    ev(rb.id, 'next', { lineSeq: 1 }, '田中美波');
    const r = sync(rb.id, 'next');
    assert.deepEqual(r.actions, ['fulfill']);
    assert.equal(taskStatus(task.id), 'fulfilled');
    assert.equal(stockoutAlerts(task.id).length, 0);
  });
  t('完了後の back (done → picking) → fulfilled → claimed に戻る', () => {
    ev(rb.id, 'back', { lineSeq: 1, undoOpId: lastOp() }, '田中美波');
    const r = sync(rb.id, 'back');
    assert.deepEqual(r.actions, ['resume']);
    assert.equal(taskStatus(task.id), 'claimed');
  });
}

console.log('── 部分確保 (3個中2個は他ロケで確保・1個は在庫なし) ──');
{
  const { rb, task } = laterToRepick('U5', 'sku-u5', 3);
  ev(rb.id, 'start', {}, '田中美波');
  sync(rb.id, 'start');
  t('unavailable_qty=1 / fulfilled_qty=2 が保存され、バナーと通知情報に内訳が出る', () => {
    ev(rb.id, 'shortage', { lineSeq: 1, shortageQty: 3, altQty: 2, altBlock: 'P3FB', altLocation: '002-013-03', remaining: 'none' }, '田中美波');
    const r = sync(rb.id, 'shortage');
    assert.equal(taskStatus(task.id), 'unavailable');
    assert.equal(taskRow(task.id).unavailable_qty, 1);
    assert.equal(taskRow(task.id).fulfilled_qty, 2);
    assert.equal(r.unavailable.remaining, 1);
    assert.equal(r.unavailable.altQty, 2);
    assert.match(stockoutAlerts(task.id)[0].message, /×1 \(2個は届けます\)/);
    const st = psvc.getWorkState(task.batch_id);
    assert.equal(st.stockoutBySlip[1][0].unavailable_qty, 1);
    assert.equal(st.stockoutBySlip[1][0].fulfilled_qty, 2);
  });
}

// ═══ ⑥ 1階が受け取り済みなら back できない ═══════════════════════════════════
console.log('── 受領後の back ──');
{
  const { rb, task, pb } = laterToRepick('U6', 'sku-u6');
  ev(rb.id, 'start', {}, '田中美波');
  sync(rb.id, 'start');
  ev(rb.id, 'next', { lineSeq: 1 }, '田中美波');
  sync(rb.id, 'next');
  assert.equal(taskStatus(task.id), 'fulfilled');
  psvc.applyEvent(pb, { opId: `p${++op}`, event: 'receive', slipSeq: 1 }, '三宅晴菜');
  assert.equal(taskStatus(task.id), 'received');
  throwsCode(() => ev(rb.id, 'back', { lineSeq: 1, undoOpId: `t${op - 1}` }, '田中美波'),
    'already_received', '1階が受け取った後は back できない (409 already_received)');
  t('received は終端: 同期しても触らない', () => {
    const r = sync(rb.id, 'next');
    assert.deepEqual(r.actions, []);
    assert.equal(taskStatus(task.id), 'received');
  });
}

// ═══ ⑦ 棚戻しキューは棚戻しだけ ═══════════════════════════════════════════════
console.log('── /tasks は棚戻し専用 ──');
{
  const { pb } = laterToRepick('U3', 'sku-u3');
  db.prepare(`INSERT INTO pk_pack_tasks (batch_id, slip_seq, kind, sku, product_name, req_qty, status, requested_by, created_at, updated_at)
    VALUES (?, 1, 'return', 'sku-ret', '戻す商品', 2, 'requested', '三宅晴菜', ?, ?)`).run(pb, now, now);
  t('listOpenTasks({kind:"return"}) は再ピックを含まない / 指定なしは従来どおり全部', () => {
    const ret = psvc.listOpenTasks({ kind: 'return' });
    assert.ok(ret.length >= 1 && ret.every((x) => x.kind === 'return'));
    assert.ok(psvc.listOpenTasks().some((x) => x.kind === 'repick'));
    assert.equal(psvc.countOpenTasks({ kind: 'return' }), 1);
  });
  t('getTask で種別を引ける (API の棚戻し限定ガード用)', () => {
    const ret = psvc.listOpenTasks({ kind: 'return' })[0];
    assert.equal(psvc.getTask(ret.id).kind, 'return');
    assert.equal(psvc.getTask(999999), null);
  });
}


// ═══ ⑧ PR-7: 同じ伝票の複数タスク = 1バッチ (行ごとに同期) ═════════════════════
console.log('── 同じ伝票の複数タスクを1バッチで運ぶ ──');
{
  const pk = mkPickBatch('U8', { sku: 'sku-u8a', lineQty: 1, slipQtys: [1] });
  const pb = mkPackBatch('U8', { sku: 'sku-u8a', slipQtys: [1] });
  db.prepare('UPDATE pk_pack_batches SET pk_batch_id=? WHERE id=?').run(pk, pb);
  const mk = (sku, qty) => Number(db.prepare(`INSERT INTO pk_pack_tasks (batch_id, slip_seq, kind, sku, product_name, req_qty, location, block, status, requested_by, created_at, updated_at)
    VALUES (?, 1, 'repick', ?, ?, ?, '00100101', 'P3FA', 'requested', '三宅晴菜', ?, ?)`).run(pb, sku, `商品${sku}`, qty, now, now).lastInsertRowid);
  const tA = mk('sku-u8a', 1), tB = mk('sku-u8b', 2), tC = mk('sku-u8c', 1);
  db.prepare("UPDATE pk_pack_slips SET status='held', hold_reason='repick' WHERE batch_id=? AND seq=1").run(pb);
  reconcileRepickBatches();
  const rb = db.prepare('SELECT * FROM pk_batches WHERE pack_task_id=?').get(tA);
  t('3タスク → 1バッチ 3行 (pack_task_id は行ごと)', () => {
    assert.ok(rb);
    const lines = db.prepare('SELECT seq, sku, qty, pack_task_id FROM pk_lines WHERE batch_id=? ORDER BY seq').all(rb.id);
    assert.deepEqual(lines, [{ seq: 1, sku: 'sku-u8a', qty: 1, pack_task_id: tA }, { seq: 2, sku: 'sku-u8b', qty: 2, pack_task_id: tB }, { seq: 3, sku: 'sku-u8c', qty: 1, pack_task_id: tC }]);
    assert.equal(db.prepare('SELECT COUNT(*) AS c FROM pk_batches WHERE origin=? AND pack_batch_id=?').get('repick', pb).c, 1, 'バッチは1本だけ');
  });
  t('start → 3タスクとも claimed', () => {
    ev(rb.id, 'start', {}, '田中美波');
    const r = sync(rb.id, 'start');
    assert.deepEqual(r.actions, ['claim', 'claim', 'claim']);
    assert.deepEqual([tA, tB, tC].map(taskStatus), ['claimed', 'claimed', 'claimed']);
    assert.deepEqual(r.perTask, { [tA]: 'claimed', [tB]: 'claimed', [tC]: 'claimed' });
  });
  t('途中 (1行目だけ完了) では何も変わらない', () => {
    ev(rb.id, 'next', { lineSeq: 1 }, '田中美波');
    const r = sync(rb.id, 'next');
    assert.deepEqual(r.actions, []);
    assert.equal(batchStatus(rb.id), 'picking');
  });
  t('2行目 = 在庫なし (2個中1個は他ロケ)・3行目 = 通常完了 → A/C fulfilled・B unavailable、通知は B だけ', () => {
    ev(rb.id, 'shortage', { lineSeq: 2, shortageQty: 2, altQty: 1, altBlock: 'P3FB', altLocation: '002-013-03', remaining: 'none' }, '田中美波');
    ev(rb.id, 'next', { lineSeq: 3 }, '田中美波');
    assert.equal(batchStatus(rb.id), 'done');
    const r = sync(rb.id, 'next');
    assert.deepEqual([tA, tB, tC].map(taskStatus), ['fulfilled', 'unavailable', 'fulfilled']);
    assert.equal(r.unavailables.length, 1);
    assert.equal(r.unavailables[0].task.id, tB);
    assert.equal(r.unavailables[0].remaining, 1);
    assert.equal(r.unavailables[0].altQty, 1);
    assert.equal(r.unavailable.task.id, tB, '互換フィールド');
    assert.equal(taskRow(tB).unavailable_qty, 1);
    assert.equal(taskRow(tB).fulfilled_qty, 1);
    assert.equal(stockoutAlerts(tB).length, 1, 'B だけ赤バナー');
    assert.equal(stockoutAlerts(tA).length + stockoutAlerts(tC).length, 0);
    assert.equal(r.status, 'fulfilled', 'status は同期キー (A) のもの');
  });
  t('同期を再実行しても変わらない (replay: unavailables は空)', () => {
    const r = sync(rb.id, 'next');
    assert.deepEqual(r.actions, []);
    assert.deepEqual(r.unavailables, []);
    assert.equal(r.unavailable, null);
  });
  t('back (done → picking) → 3タスクとも claimed に戻り、赤バナーは閉じる', () => {
    ev(rb.id, 'back', { lineSeq: 3, undoOpId: lastOp() }, '田中美波');
    const r = sync(rb.id, 'back');
    assert.deepEqual(r.actions.sort(), ['resume', 'resume', 'resume']);
    assert.deepEqual([tA, tB, tC].map(taskStatus), ['claimed', 'claimed', 'claimed']);
    assert.equal(stockoutAlerts(tB).length, 0);
  });
}

console.log('── 複数タスクのバッチ: 一部が1階で取下げ ──');
{
  const pk = mkPickBatch('U9', { sku: 'sku-u9a', lineQty: 1, slipQtys: [1] });
  const pb = mkPackBatch('U9', { sku: 'sku-u9a', slipQtys: [1] });
  db.prepare('UPDATE pk_pack_batches SET pk_batch_id=? WHERE id=?').run(pk, pb);
  const mk = (sku) => Number(db.prepare(`INSERT INTO pk_pack_tasks (batch_id, slip_seq, kind, sku, req_qty, status, requested_by, created_at, updated_at)
    VALUES (?, 1, 'repick', ?, 1, 'requested', '三宅晴菜', ?, ?)`).run(pb, sku, now, now).lastInsertRowid);
  const tA = mk('sku-u9a'), tB = mk('sku-u9b');
  reconcileRepickBatches();
  const rb = db.prepare('SELECT * FROM pk_batches WHERE pack_task_id=?').get(tA);
  t('片方が取下げでも start できる (残りの行は運ぶ)', () => {
    db.prepare("UPDATE pk_pack_tasks SET status='cancelled', close_reason='found' WHERE id=?").run(tB);
    ev(rb.id, 'start', {}, '田中美波');
    const r = sync(rb.id, 'start');
    assert.deepEqual(r.actions, ['claim']);
    assert.equal(taskStatus(tA), 'claimed');
    assert.equal(taskStatus(tB), 'cancelled', '取下げ済みは触らない');
  });
  t('全部取下げなら 409 repick_cancelled', () => {
    db.prepare("UPDATE pk_pack_tasks SET status='cancelled', close_reason='found' WHERE id=?").run(tA);
    throwsCode(() => ev(rb.id, 'next', { lineSeq: 1 }, '田中美波'), 'repick_cancelled', '全部取下げ → 409');
    assert.equal(reconcileRepickBatches(), 1, '一覧を開くと畳まれる');
    assert.equal(batchStatus(rb.id), 'cancelled');
  });
}

console.log(`\n${passed} tests passed`);
