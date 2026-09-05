/**
 * test-stockout-ack.mjs — 🚫 在庫なしを確認 (Q1 決定 2026-09-05 = 案a) と関連ガード
 *
 * 守りたいこと:
 *   ① 3階「在庫なし」の保留伝票を、1階が「在庫なしを確認」→ 伝票は cancelled/stockout・最後の1枚ならバッチ done・事務通知の情報が返る
 *   ② 再ピック中 (requested/claimed/fulfilled) や未送信の候補が残っていれば確認できない
 *   ③ 在庫なしの報告がある商品は再依頼できない (409 stockout_reported — 以前は 🔴バッチがもう1本立った)
 *   ④ ライン「送信後に見つかった」は confirmed も取り下げる (Q5: ピッカーのミスに残さない)
 *   ⑤ ラインバッチからも在庫なしを確認できる (所有者チェックなし・バッチ状態は動かさない)
 *
 * 実行: node apps/packing/tests/test-stockout-ack.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'packing-stockout-test-'));

const { initPickingDB } = await import('../../picking/db.js');
const { initPackingDB, getDB, utcNow, jstToday } = await import('../db.js');
const { applyEvent, getWorkState, PackError } = await import('../service.js');

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
    db.prepare('INSERT INTO pk_pack_lines (slip_id, sku, product_name, qty) VALUES (?, ?, ?, 1)').run(sid, s.sku, `商品${s.sku}`);
  });
  return batchId;
}
function mkTask(batchId, slipSeq, sku, status, { requestedBy = '有國陽', claimedBy = '田中美波', incidentId = null } = {}) {
  return Number(db.prepare(`INSERT INTO pk_pack_tasks (batch_id, slip_seq, kind, sku, product_name, req_qty, location, block, folder_name, status, requested_by, claimed_by, incident_id, created_at, updated_at)
    VALUES (?, ?, 'repick', ?, ?, 1, '00100101', 'P3FA', '出荷_77', ?, ?, ?, ?, ?, ?)`)
    .run(batchId, slipSeq, sku, `商品${sku}`, status, requestedBy, status === 'requested' ? null : claimedBy, incidentId, now, now).lastInsertRowid);
}
const slipRow = (b, seq) => db.prepare('SELECT * FROM pk_pack_slips WHERE batch_id=? AND seq=?').get(b, seq);
const batchStatus = (b) => db.prepare('SELECT status FROM pk_pack_batches WHERE id=?').get(b).status;

// ═══ ① ② 手梱包: 在庫なしを確認 ═══════════════════════════════════════════════
console.log('── 手梱包: 在庫なしを確認 ──');
{
  const b = mkPackBatch('S1', [
    { sku: 'a', status: 'held', holdReason: 'repick' },
    { sku: 'b', status: 'done' },
    { sku: 'c', status: 'done' },
  ]);
  const na = mkTask(b, 1, 'a', 'unavailable');
  t('getWorkState: 在庫なしの報告が保留伝票に付く (stockoutBySlip)', () => {
    const st = getWorkState(b);
    assert.deepEqual(Object.keys(st.stockoutBySlip), ['1']);
    assert.equal(st.stockoutBySlip[1][0].id, na);
  });
  throwsCode(() => ev(b, 'stockout_ack', { slipSeq: 2 }), 'not_held', '保留でない伝票は確認できない');
  const inProg = mkTask(b, 1, 'a2', 'requested');
  throwsCode(() => ev(b, 'stockout_ack', { slipSeq: 1 }), 'repick_in_progress', '再ピック中の商品が残っていれば確認できない');
  db.prepare("UPDATE pk_pack_tasks SET status='cancelled' WHERE id=?").run(inProg);
  db.prepare(`INSERT INTO pk_pack_incidents (batch_id, slip_seq, kind, sku, qty, status, detected_by, created_at, updated_at)
    VALUES (?, 1, 'shortage', 'a3', 1, 'candidate', '三宅晴菜', ?, ?)`).run(b, now, now);
  throwsCode(() => ev(b, 'stockout_ack', { slipSeq: 1 }), 'candidates_remain', '未送信の候補が残っていれば確認できない');
  db.prepare("UPDATE pk_pack_incidents SET status='withdrawn' WHERE batch_id=?").run(b);
  t('確認 → 伝票 cancelled/stockout・最後の1枚なのでバッチ done・事務通知の情報 (kind=stockout) が返る', () => {
    const r = ev(b, 'stockout_ack', { slipSeq: 1 });
    const s = slipRow(b, 1);
    assert.equal(s.status, 'cancelled');
    assert.equal(s.hold_reason, 'stockout');
    assert.deepEqual(r.stockoutSeqs, [1]);
    assert.deepEqual(r.heldSeqs, []);
    assert.equal(r.batchStatus, 'done');
    assert.equal(batchStatus(b), 'done');
    assert.equal(r.taskNotify.kind, 'stockout');
    assert.equal(r.taskNotify.neSlipNo, 'S1-NE1');
    assert.equal(r.taskNotify.items.length, 1);
    assert.equal(r.taskNotify.items[0].sku, 'a');
    assert.equal(r.taskNotify.items[0].claimedBy, '田中美波');
    assert.equal(db.prepare('SELECT status FROM pk_pack_tasks WHERE id=?').get(na).status, 'unavailable', 'タスクの記録はそのまま');
  });
  t('同じ op の再送は replayed (通知情報は付かない = 二重通知しない)', () => {
    const r = applyEvent(b, { opId: `s${op}`, event: 'stockout_ack', slipSeq: 1 }, '三宅晴菜');
    assert.equal(r.replayed, true);
    assert.equal(r.taskNotify, undefined);
  });
  throwsCode(() => ev(b, 'found', { slipSeq: 1 }), 'not_held', '閉じた伝票に「見つかった」は効かない');
}

console.log('── 手梱包: 他に未処理があればバッチは続く ──');
{
  const b = mkPackBatch('S2', [
    { sku: 'a', status: 'held', holdReason: 'repick' },
    { sku: 'b', status: 'pending' },
  ]);
  mkTask(b, 1, 'a', 'unavailable');
  t('確認しても未処理が残ればバッチは packing のまま・currentSeq は次の未処理', () => {
    const r = ev(b, 'stockout_ack', { slipSeq: 1 });
    assert.equal(r.batchStatus, 'packing');
    assert.equal(r.currentSeq, 2);
    // 次の伝票を完了 → 保留ゼロ・未処理ゼロ → done (cancelled は妨げない)
    const r2 = ev(b, 'next', { slipSeq: 2 });
    assert.equal(r2.batchStatus, 'done');
  });
}

// ═══ ③ ④ ⑤ ライン ═══════════════════════════════════════════════════════════
console.log('── ライン: 再依頼ガード・見つかった・在庫なしを確認 ──');
{
  const b = mkPackBatch('L1', [
    { sku: 'x', status: 'held', holdReason: 'repick' },
    { sku: 'y', status: 'held', holdReason: 'repick' },
    { sku: 'z', status: 'pending' },
  ], { pkClass: 'ネコポス【梱包機PAS-LINE《3つ折り》】単品' });
  mkTask(b, 1, 'x', 'unavailable');
  throwsCode(() => ev(b, 'shortage', { slipSeq: 1, sku: 'x', qty: 1 }), 'stockout_reported',
    '在庫なしの報告がある商品は再依頼できない (409 stockout_reported)');

  const incId = Number(db.prepare(`INSERT INTO pk_pack_incidents (batch_id, slip_seq, kind, sku, qty, status, attributed_worker, detected_by, confirmed_by, created_at, updated_at)
    VALUES (?, 2, 'shortage', 'y', 1, 'confirmed', '有國陽', '三宅晴菜', '三宅晴菜', ?, ?)`).run(b, now, now).lastInsertRowid);
  const yTask = mkTask(b, 2, 'y', 'requested', { requestedBy: '三宅晴菜', incidentId: incId });
  t('送信後に「見つかった」→ confirmed も withdrawn (ミスに残さない・Q5)・タスク取消・伝票は pending', () => {
    ev(b, 'found', { slipSeq: 2, sku: 'y' }, '星立夏');
    assert.equal(db.prepare('SELECT status FROM pk_pack_incidents WHERE id=?').get(incId).status, 'withdrawn');
    assert.equal(db.prepare('SELECT status FROM pk_pack_tasks WHERE id=?').get(yTask).status, 'cancelled');
    assert.equal(slipRow(b, 2).status, 'pending');
  });
  t('ラインからの在庫なし確認: 別の作業者でも可・バッチ状態は動かない (完了は line_done で決まる)', () => {
    const r = ev(b, 'stockout_ack', { slipSeq: 1 }, '星立夏');
    assert.equal(slipRow(b, 1).status, 'cancelled');
    assert.equal(slipRow(b, 1).hold_reason, 'stockout');
    assert.equal(r.taskNotify.kind, 'stockout');
    assert.equal(batchStatus(b), 'packing');
  });
  t('PackError の形 (status/code) が router で JSON に変換できる', () => {
    try { ev(b, 'stockout_ack', { slipSeq: 3 }); assert.fail(); }
    catch (e) { assert.ok(e instanceof PackError); assert.equal(e.status, 409); }
  });
}

console.log(`\n${passed} tests passed`);
