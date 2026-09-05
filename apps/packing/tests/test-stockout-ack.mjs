/**
 * test-stockout-ack.mjs — 🚫 在庫なしを確認 (Q1 決定 2026-09-05 = 案a) と関連ガード
 *
 * 守りたいこと:
 *   ① 3階「在庫なし」の保留伝票を、1階が「在庫なしを確認」→ 伝票は cancelled/stockout・在庫なしタスクは cancelled+close_reason・
 *      届いていた分 (fulfilled) は received・事務通知は outbox (pk_pack_stockouts) に積む・最後の1枚ならバッチ done
 *   ② 再ピック中 (requested/claimed) や未送信の候補が残っていれば確認できない。確認後は fulfill できない
 *   ③ 在庫なしの報告がある商品は再依頼できない (409 stockout_reported — 手梱包・ライン両方)
 *   ④ 「見つかった」は在庫なし報告 (unavailable) も取り下げ、紐づく confirmed も withdrawn に (Q5)・バナーも閉じる
 *   ⑤ 受領は在庫なし報告が残っていれば拒否 (「在庫なしを確認」へ誘導)
 *   ⑥ ライン: 完了件数の確定後は確認できない / バッチ取消は unavailable も取消す
 *
 * 実行: node apps/packing/tests/test-stockout-ack.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'packing-stockout-test-'));

const { initPickingDB } = await import('../../picking/db.js');
const { createFloorAlert, listFloorAlerts } = await import('../../picking/service.js');
const { initPackingDB, getDB, utcNow, jstToday } = await import('../db.js');
const {
  applyEvent, getWorkState, applyTaskAction, PackError,
  claimStockoutNotify, markStockoutNotify, listPendingStockoutNotifies, countStaleStockoutNotifies,
} = await import('../service.js');

initPickingDB();
initPackingDB();
const db = getDB();
const now = utcNow();
const today = jstToday();

let passed = 0;
function t(name, fn) { fn(); passed++; console.log(`  ok: ${name}`); }
function throwsCode(fn, code, name) {
  try { fn(); assert.fail(`${name}: エラーにならなかった`); }
  catch (e) { assert.equal(e.code, code, `${name} (実際=${e.code || e.message})`); }
  passed++; console.log(`  ok: ${name}`);
}
let op = 0;
const ev = (batchId, event, extra = {}, worker = '三宅晴菜') =>
  applyEvent(batchId, { opId: `s${++op}`, event, ...extra }, worker);

/** 梱包バッチ。pkClass を渡すと pk_batches を作って引当分類を紐づける (ラインバッチ用)。 */
function mkPackBatch(tbKey, slips, { pkClass = null } = {}) {
  let pkId = null;
  if (pkClass) {
    pkId = Number(db.prepare(`INSERT INTO pk_batches (tb_no, hikiate_class, folder_name, work_date, composition, line_count, slip_count, total_qty,
      status, csv_sha256, imported_by, created_at, updated_at) VALUES (?, ?, '出荷_77', ?, '単品', 1, ?, ?, 'done', 'x', 'test', ?, ?)`)
      .run(tbKey, pkClass, today, slips.length, slips.length, now, now).lastInsertRowid);
  }
  const batchId = Number(db.prepare(`INSERT INTO pk_pack_batches
    (tb_key, folder_name, work_date, slip_count, line_count, total_qty, pk_batch_id, match_status, status, worker, started_at, csv_sha256, imported_by, created_at, updated_at)
    VALUES (?, '出荷_77', ?, ?, ?, ?, ?, 'ok', 'packing', '三宅晴菜', ?, ?, 'test', ?, ?)`)
    .run(tbKey, today, slips.length, slips.length, slips.length, pkId, now, `sha-${tbKey}`, now, now).lastInsertRowid);
  slips.forEach((s, i) => {
    const sid = Number(db.prepare(`INSERT INTO pk_pack_slips (batch_id, seq, ne_slip_no, slip_no, recipient_name, site_order_no, status, hold_reason, delivery_method, done_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, '箱', ?)`)
      .run(batchId, i + 1, `${tbKey}-NE${i + 1}`, `SP${tbKey}-${i + 1}`, `客${i + 1}`, `SO-${i + 1}`, s.status, s.holdReason || null, s.status === 'done' ? now : null).lastInsertRowid);
    for (const sku of [].concat(s.sku)) {
      db.prepare('INSERT INTO pk_pack_lines (slip_id, sku, product_name, qty) VALUES (?, ?, ?, 1)').run(sid, sku, `商品${sku}`);
    }
  });
  return batchId;
}
function mkTask(batchId, slipSeq, sku, status, { requestedBy = '有國陽', claimedBy = '田中美波', incidentId = null, unavailableQty = null, fulfilledQty = null } = {}) {
  return Number(db.prepare(`INSERT INTO pk_pack_tasks (batch_id, slip_seq, kind, sku, product_name, req_qty, location, block, folder_name, status, requested_by, claimed_by, incident_id, unavailable_qty, fulfilled_qty, created_at, updated_at)
    VALUES (?, ?, 'repick', ?, ?, 1, '00100101', 'P3FA', '出荷_77', ?, ?, ?, ?, ?, ?, ?, ?)`)
    .run(batchId, slipSeq, sku, `商品${sku}`, status, requestedBy, status === 'requested' ? null : claimedBy, incidentId, unavailableQty, fulfilledQty, now, now).lastInsertRowid);
}
const slipRow = (b, seq) => db.prepare('SELECT * FROM pk_pack_slips WHERE batch_id=? AND seq=?').get(b, seq);
const taskRow = (id) => db.prepare('SELECT * FROM pk_pack_tasks WHERE id=?').get(id);
const batchStatus = (b) => db.prepare('SELECT status FROM pk_pack_batches WHERE id=?').get(b).status;
const stockoutAlerts = (taskId) => listFloorAlerts('to_packing').filter((a) => a.kind === 'stockout' && a.task_id === taskId);

// ═══ ① ② 手梱包: 在庫なしを確認 ═══════════════════════════════════════════════
console.log('── 手梱包: 在庫なしを確認 ──');
{
  const b = mkPackBatch('S1', [
    { sku: ['a', 'a2'], status: 'held', holdReason: 'repick' },
    { sku: 'b', status: 'done' },
    { sku: 'c', status: 'done' },
  ]);
  const na = mkTask(b, 1, 'a', 'unavailable');
  createFloorAlert('stockout', '田中美波', '🚫 在庫なし: 出荷_77 #1 商品a ×1 — 3階 田中美波', `/apps/packing/work/${b}?seq=1`, na);
  t('getWorkState: 在庫なしの報告が保留伝票に付く (stockoutBySlip)・確認できる (stockoutAckSeqs)', () => {
    const st = getWorkState(b);
    assert.deepEqual(Object.keys(st.stockoutBySlip), ['1']);
    assert.equal(st.stockoutBySlip[1][0].id, na);
    assert.deepEqual(st.stockoutAckSeqs, [1]);
  });
  throwsCode(() => ev(b, 'stockout_ack', { slipSeq: 2 }), 'not_held', '保留でない伝票は確認できない');
  const inProg = mkTask(b, 1, 'a2', 'requested');
  t('別の商品が再ピック中なら確認できない (stockoutAckSeqs からも外れる)', () => {
    assert.deepEqual(getWorkState(b).stockoutAckSeqs, []);
  });
  throwsCode(() => ev(b, 'stockout_ack', { slipSeq: 1 }), 'repick_in_progress', '再ピック中の商品が残っていれば確認できない');
  throwsCode(() => ev(b, 'receive', { slipSeq: 1 }), 'stockout_reported', '在庫なし報告がある伝票は受領で解除できない (確認へ誘導)');
  // 再ピック中の商品が届いた (fulfilled) → 確認と同時に受領扱いになる
  db.prepare("UPDATE pk_pack_tasks SET status='fulfilled' WHERE id=?").run(inProg);
  db.prepare(`INSERT INTO pk_pack_incidents (batch_id, slip_seq, kind, sku, qty, status, detected_by, created_at, updated_at)
    VALUES (?, 1, 'shortage', 'a3', 1, 'candidate', '三宅晴菜', ?, ?)`).run(b, now, now);
  throwsCode(() => ev(b, 'stockout_ack', { slipSeq: 1 }), 'candidates_remain', '未送信の候補が残っていれば確認できない');
  db.prepare("UPDATE pk_pack_incidents SET status='withdrawn' WHERE batch_id=?").run(b);
  t('確認 → 伝票 cancelled/stockout・在庫なしタスクは cancelled+close_reason・届いた分は received・最後の1枚なのでバッチ done・outbox に行', () => {
    assert.deepEqual(getWorkState(b).stockoutAckSeqs, [1], 'fulfilled だけなら確認できる');
    const r = ev(b, 'stockout_ack', { slipSeq: 1 });
    const s = slipRow(b, 1);
    assert.equal(s.status, 'cancelled');
    assert.equal(s.hold_reason, 'stockout');
    assert.deepEqual(r.stockoutSeqs, [1]);
    assert.deepEqual(r.heldSeqs, []);
    assert.equal(r.batchStatus, 'done');
    assert.equal(batchStatus(b), 'done');
    assert.equal(taskRow(na).status, 'cancelled');
    assert.equal(taskRow(na).close_reason, 'stockout');
    assert.equal(taskRow(inProg).status, 'received');
    assert.equal(r.taskNotify.kind, 'stockout');
    assert.equal(r.taskNotify.neSlipNo, 'S1-NE1');
    assert.deepEqual(r.taskNotify.items.map((i) => i.sku), ['a']);
    assert.equal(r.taskNotify.items[0].claimedBy, '田中美波');
    const ob = db.prepare('SELECT * FROM pk_pack_stockouts WHERE id=?').get(r.taskNotify.stockoutId);
    assert.equal(ob.ne_slip_no, 'S1-NE1');
    assert.equal(ob.notified_at, null, '送信前は未通知 (router/ポーラーが送れたときだけ印を付ける)');
    assert.deepEqual(JSON.parse(ob.items_json).map((i) => i.sku), ['a']);
    assert.equal(stockoutAlerts(na).length, 0, '赤バナーは閉じる');
  });
  t('同じ op の再送は replayed (通知情報は付かない = 二重通知しない)', () => {
    const r = applyEvent(b, { opId: `s${op}`, event: 'stockout_ack', slipSeq: 1 }, '三宅晴菜');
    assert.equal(r.replayed, true);
    assert.equal(r.taskNotify, undefined);
  });
  throwsCode(() => ev(b, 'found', { slipSeq: 1 }), 'not_held', '閉じた伝票に「見つかった」は効かない');
  throwsCode(() => applyTaskAction(na, 'fulfill', '田中美波'), 'bad_transition', '確認して閉じたタスクは後から fulfill できない');
}

console.log('── 手梱包: 他に未処理があればバッチは続く / 再依頼ガード ──');
{
  const b = mkPackBatch('S2', [
    { sku: 'a', status: 'held', holdReason: 'repick' },
    { sku: 'b', status: 'pending' },
  ]);
  mkTask(b, 1, 'a', 'unavailable');
  const naB = mkTask(b, 2, 'b', 'unavailable');   // 未処理の伝票に在庫なし報告 (在庫なし後に伝票が戻った想定)
  throwsCode(() => ev(b, 'shortage', { slipSeq: 2, sku: 'b', qty: 1 }), 'stockout_reported',
    '手梱包でも在庫なしの報告がある商品は不足候補を作れない (409 stockout_reported)');
  db.prepare("UPDATE pk_pack_tasks SET status='cancelled' WHERE id=?").run(naB);
  t('確認しても未処理が残ればバッチは packing のまま・currentSeq は次の未処理', () => {
    const r = ev(b, 'stockout_ack', { slipSeq: 1 });
    assert.equal(r.batchStatus, 'packing');
    assert.equal(r.currentSeq, 2);
    const r2 = ev(b, 'next', { slipSeq: 2 });
    assert.equal(r2.batchStatus, 'done', 'cancelled (出荷保留) は完了を妨げない');
  });
}

// ═══ ③ ④ ⑤ ⑥ ライン ═══════════════════════════════════════════════════════
console.log('── ライン: 再依頼ガード・見つかった・在庫なしを確認・完了件数確定後 ──');
{
  const b = mkPackBatch('L1', [
    { sku: 'x', status: 'held', holdReason: 'repick' },
    { sku: 'y', status: 'held', holdReason: 'repick' },
    { sku: 'z', status: 'held', holdReason: 'repick' },
    { sku: 'w', status: 'pending' },
  ], { pkClass: 'ネコポス【梱包機PAS-LINE《3つ折り》】単品' });
  const xTask = mkTask(b, 1, 'x', 'unavailable');
  throwsCode(() => ev(b, 'shortage', { slipSeq: 1, sku: 'x', qty: 1 }), 'stockout_reported',
    '在庫なしの報告がある商品は再依頼できない (409 stockout_reported)');

  const incId = Number(db.prepare(`INSERT INTO pk_pack_incidents (batch_id, slip_seq, kind, sku, qty, status, attributed_worker, detected_by, confirmed_by, created_at, updated_at)
    VALUES (?, 2, 'shortage', 'y', 1, 'confirmed', '有國陽', '三宅晴菜', '三宅晴菜', ?, ?)`).run(b, now, now).lastInsertRowid);
  const yTask = mkTask(b, 2, 'y', 'requested', { requestedBy: '三宅晴菜', incidentId: incId });
  t('送信後に「見つかった」→ confirmed も withdrawn (ミスに残さない・Q5)・タスク取消・伝票は pending', () => {
    ev(b, 'found', { slipSeq: 2, sku: 'y' }, '星立夏');
    assert.equal(db.prepare('SELECT status FROM pk_pack_incidents WHERE id=?').get(incId).status, 'withdrawn');
    assert.equal(taskRow(yTask).status, 'cancelled');
    assert.equal(slipRow(b, 2).status, 'pending');
  });
  const incZ = Number(db.prepare(`INSERT INTO pk_pack_incidents (batch_id, slip_seq, kind, sku, qty, status, attributed_worker, detected_by, confirmed_by, created_at, updated_at)
    VALUES (?, 3, 'shortage', 'z', 1, 'confirmed', '有國陽', '三宅晴菜', '三宅晴菜', ?, ?)`).run(b, now, now).lastInsertRowid);
  const zTask = mkTask(b, 3, 'z', 'unavailable', { incidentId: incZ });
  createFloorAlert('stockout', '田中美波', '🚫 在庫なし: 出荷_77 #3 商品z ×1 — 3階 田中美波', `/apps/packing/work/${b}?seq=3`, zTask);
  t('在庫なし報告の後に手元で出てきた (found) → unavailable も取消・confirmed 取下げ・伝票 pending・バナー閉じる', () => {
    ev(b, 'found', { slipSeq: 3 }, '星立夏');
    assert.equal(taskRow(zTask).status, 'cancelled');
    assert.equal(db.prepare('SELECT status FROM pk_pack_incidents WHERE id=?').get(incZ).status, 'withdrawn');
    assert.equal(slipRow(b, 3).status, 'pending');
    assert.equal(stockoutAlerts(zTask).length, 0);
    assert.deepEqual(getWorkState(b).stockoutBySlip[3], undefined);
  });
  t('ラインからの在庫なし確認: 別の作業者でも可・バッチ状態は動かない (完了は line_done で決まる)', () => {
    const r = ev(b, 'stockout_ack', { slipSeq: 1 }, '星立夏');
    assert.equal(slipRow(b, 1).status, 'cancelled');
    assert.equal(slipRow(b, 1).hold_reason, 'stockout');
    assert.equal(taskRow(xTask).close_reason, 'stockout');
    assert.equal(r.taskNotify.kind, 'stockout');
    assert.equal(batchStatus(b), 'packing');
  });
  // 完了件数の確定後は確認できない
  const wTask = mkTask(b, 4, 'w', 'unavailable');
  db.prepare("UPDATE pk_pack_slips SET status='held', hold_reason='repick' WHERE batch_id=? AND seq=4").run(b);
  db.prepare(`INSERT INTO pk_pack_line_runs (batch_id, phase, started_at, finished_at, planned_count, final_count, manual_count, worker, updated_at)
    VALUES (?, 'run', ?, ?, 4, 3, 0, '三宅晴菜', ?)`).run(b, now, now, now);
  t('ライン完了件数の確定後は stockoutAckSeqs から外れる', () => {
    assert.deepEqual(getWorkState(b).stockoutAckSeqs, []);
  });
  throwsCode(() => ev(b, 'stockout_ack', { slipSeq: 4 }, '星立夏'), 'line_already_finalized',
    'ライン完了件数の確定後は確認できない (先に記録を取り消す)');
  void wTask;
  t('PackError の形 (status/code) が router で JSON に変換できる', () => {
    try { ev(b, 'stockout_ack', { slipSeq: 2 }); assert.fail(); }
    catch (e) { assert.ok(e instanceof PackError); assert.equal(e.status, 409); }
  });
}

console.log('── 事務通知の outbox (claim / 再送対象 / 表示状態) ──');
{
  const b = mkPackBatch('S4', [{ sku: 'a', status: 'held', holdReason: 'repick' }]);
  mkTask(b, 1, 'a', 'unavailable');
  const r = ev(b, 'stockout_ack', { slipSeq: 1 });
  const id = r.taskNotify.stockoutId;
  t('未通知の行は再送対象・古い行は期間で切られない', () => {
    assert.ok(listPendingStockoutNotifies(10).some((x) => x.id === id));
    db.prepare("UPDATE pk_pack_stockouts SET created_at=datetime('now','-5 days') WHERE id=?").run(id);
    assert.ok(listPendingStockoutNotifies(10).some((x) => x.id === id), '5日前でも再送対象');
    assert.ok(countStaleStockoutNotifies() >= 1, '2日以上の滞留として数える');
  });
  t('claim は1回だけ通る (router とポーラーの同時送信を防ぐ)。失敗を記録すると再送対象に戻る', () => {
    assert.equal(claimStockoutNotify(id), true);
    assert.equal(claimStockoutNotify(id), false, '送信中は取れない');
    assert.ok(!listPendingStockoutNotifies(10).some((x) => x.id === id), '送信中は再送対象から外れる');
    // 送信中に落ちた想定: 印が10分より古ければ失効して取り直せる (ISO の 'T' と datetime() の空白の比較罠 — Codex R3)
    db.prepare("UPDATE pk_pack_stockouts SET claimed_at=? WHERE id=?")
      .run(new Date(Date.now() - 20 * 60000).toISOString().slice(0, 19) + 'Z', id);
    assert.ok(listPendingStockoutNotifies(10).some((x) => x.id === id), '古い印は失効 = 再送対象');
    assert.equal(claimStockoutNotify(id), true, '失効した印は取り直せる');
    db.prepare("UPDATE pk_pack_stockouts SET claimed_at=? WHERE id=?")
      .run(new Date(Date.now() - 2 * 60000).toISOString().slice(0, 19) + 'Z', id);
    assert.equal(claimStockoutNotify(id), false, '2分前の印は有効');
    markStockoutNotify(id, false, 'HTTP 500');
    assert.equal(db.prepare('SELECT notify_error, claimed_at FROM pk_pack_stockouts WHERE id=?').get(id).claimed_at, null);
    assert.ok(listPendingStockoutNotifies(10).some((x) => x.id === id), '失敗後は再送対象');
    assert.equal(getWorkState(b).stockoutNotifyBySlip[1], 'pending', '画面には通知待ちと出る');
  });
  t('送れたら notified_at が付き、再送対象から外れ、画面は通知済み', () => {
    assert.equal(claimStockoutNotify(id), true);
    markStockoutNotify(id, true);
    const row = db.prepare('SELECT * FROM pk_pack_stockouts WHERE id=?').get(id);
    assert.ok(row.notified_at);
    assert.equal(row.notify_error, null);
    assert.ok(!listPendingStockoutNotifies(10).some((x) => x.id === id));
    assert.equal(claimStockoutNotify(id), false, '通知済みは claim できない');
    assert.equal(getWorkState(b).stockoutNotifyBySlip[1], 'sent');
  });
}

console.log('── バッチ取消は unavailable も取消す ──');
{
  const b = mkPackBatch('S3', [{ sku: 'a', status: 'held', holdReason: 'repick' }]);
  const na = mkTask(b, 1, 'a', 'unavailable');
  createFloorAlert('stockout', '田中美波', '🚫 在庫なし: 出荷_77 #1 商品a ×1 — 3階 田中美波', `/apps/packing/work/${b}?seq=1`, na);
  t('cancel → unavailable タスクも cancelled・バナー閉じる・伝票 pending', () => {
    ev(b, 'cancel', {});
    assert.equal(taskRow(na).status, 'cancelled');
    assert.equal(stockoutAlerts(na).length, 0);
    assert.equal(slipRow(b, 1).status, 'pending');
    assert.equal(batchStatus(b), 'ready');
  });
}

console.log(`\n${passed} tests passed`);
