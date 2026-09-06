/**
 * test-later-admin.mjs — 例外処理監査 PR-6 (小物・後始末)
 *
 * 守りたいこと:
 *   ① 梱包に結べていない「後で取りに行く」依頼 (pending_binding = 迷子) を管理画面で一覧でき、取り下げると
 *      配賦・1階のバナーも一緒に消える (冪等・無い id は 404)
 *   ② 依頼が梱包へ展開済み (requested) でも未着手なら取り下げられ、タスク取消・伝票の保留解除まで戻る。
 *      ピッカーが対応を始めていれば 409 later_in_progress
 *   ③ 1階の「見つかった」で、向かっている (claimed) / 届ける途中 (fulfilled) の再ピックを取り下げると
 *      3階の全端末に取下げバナー (repick_cancelled) が出る。未着手 (requested) の取下げには出ない
 *
 * 実行: node apps/picking/tests/test-later-admin.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'picking-later-admin-test-'));

const { initPickingDB, getDB, jstToday } = await import('../db.js');
const pk = await import('../service.js');
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
const ev = (batchId, event, extra = {}, worker = '有國陽') => pk.applyEvent(batchId, { opId: `la${++op}`, event, ...extra }, worker);
const pev = (batchId, event, extra = {}, worker = '三宅晴菜') => psvc.applyEvent(batchId, { opId: `lp${++op}`, event, ...extra }, worker);

function mkPickBatch(tbNo, { sku, lineQty, slipQtys, name = 'テスト商品' }) {
  const info = db.prepare(`INSERT INTO pk_batches
    (tb_no, hikiate_class, folder_name, work_date, instruct_date, composition, delivery_method, invoice_soft,
     line_count, slip_count, total_qty, status, validity, csv_sha256, imported_by, created_at, updated_at)
    VALUES (?, 'AES《単品》', '出荷_99', ?, NULL, '単品', NULL, NULL, 1, ?, ?, 'ready', 'valid', ?, 'test', ?, ?)`)
    .run(tbNo, today, slipQtys.length, lineQty, `sha-${tbNo}`, now, now);
  const batchId = Number(info.lastInsertRowid);
  db.prepare(`INSERT INTO pk_lines (batch_id, seq, location, block, sku, product_name, barcode, qty)
    VALUES (?, 1, '00100101', 'P3FA', ?, ?, NULL, ?)`).run(batchId, sku, name, lineQty);
  const ins = db.prepare(`INSERT INTO pk_slip_lines (batch_id, slip_no, picking_no, ne_slip_no, sku, qty, location)
    VALUES (?, ?, NULL, ?, ?, ?, '00100101')`);
  slipQtys.forEach((q, i) => ins.run(batchId, `SP${tbNo}-${i + 1}`, `${tbNo}-NE${i + 1}`, sku, q));
  return batchId;
}
function mkPackBatch(tbNo, { sku, slipQtys, pkId = null, held = [] }) {
  const info = db.prepare(`INSERT INTO pk_pack_batches
    (tb_key, folder_name, work_date, slip_count, line_count, total_qty, pk_batch_id, match_status, status, worker, started_at, csv_sha256, imported_by, created_at, updated_at)
    VALUES (?, '出荷_99', ?, ?, ?, ?, ?, 'ok', 'packing', '三宅晴菜', ?, ?, 'test', ?, ?)`)
    .run(tbNo, today, slipQtys.length, slipQtys.length, slipQtys.reduce((a, b) => a + b, 0), pkId, now, `psha-${tbNo}`, now, now);
  const batchId = Number(info.lastInsertRowid);
  slipQtys.forEach((q, i) => {
    const isHeld = held.includes(i + 1);
    const sid = Number(db.prepare(`INSERT INTO pk_pack_slips (batch_id, seq, ne_slip_no, slip_no, recipient_name, site_order_no, status, hold_reason, delivery_method)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, '箱')`)
      .run(batchId, i + 1, `${tbNo}-NE${i + 1}`, `SP${tbNo}-${i + 1}`, `客${i + 1}`, `SO-${i + 1}`, isHeld ? 'held' : 'pending', isHeld ? 'repick' : null).lastInsertRowid);
    db.prepare('INSERT INTO pk_pack_lines (slip_id, sku, product_name, qty) VALUES (?, ?, ?, ?)').run(sid, sku, 'テスト商品', q);
  });
  return batchId;
}
const alerts = (dir) => pk.listFloorAlerts(dir);
const lr = (id) => db.prepare('SELECT * FROM pk_later_requests WHERE id=?').get(id);

// ═══ ① 迷子の依頼 (pending_binding) ════════════════════════════════════════
console.log('── ① 迷子の「後で取りに行く」依頼: 一覧と取り下げ ──');
{
  const b1 = mkPickBatch('L1', { sku: 'l-one', lineQty: 1, slipQtys: [1], name: '迷子の商品' });
  ev(b1, 'start');
  ev(b1, 'shortage', { lineSeq: 1, shortageQty: 1, altQty: 0, remaining: 'later' });   // 梱包が無い → pending_binding のまま
  pk.announceShortageToPacking(b1, 1, '有國陽');
  let id = null;
  t('一覧に出る (バッチ・商品・元ロケ・ピッカー)', () => {
    const rows = pk.listLaterRequests();
    assert.equal(rows.length, 1);
    id = rows[0].id;
    assert.equal(rows[0].status, 'pending_binding');
    assert.equal(rows[0].folder_name, '出荷_99');
    assert.equal(rows[0].product_name, '迷子の商品');
    assert.equal(rows[0].locationLabel, 'P3FA-001-001-01');
    assert.equal(rows[0].requested_by, '有國陽');
    assert.equal(db.prepare('SELECT COUNT(*) c FROM pk_shortage_allocations WHERE batch_id=?').get(b1).c, 1);
    assert.equal(alerts('to_packing').filter((a) => a.kind === 'picking_shortage').length, 1, '🕒 バナーが出ている');
  });
  t('取り下げ → 依頼 cancelled・配賦も消える・1階のバナーも閉じる・一覧から消える', () => {
    const r = pk.cancelLaterRequest(id, 'admin@test');
    assert.deepEqual([r.status, r.existed], ['cancelled', false]);
    assert.equal(lr(id).status, 'cancelled');
    assert.equal(db.prepare('SELECT COUNT(*) c FROM pk_shortage_allocations WHERE batch_id=?').get(b1).c, 0);
    assert.equal(alerts('to_packing').filter((a) => a.kind === 'picking_shortage').length, 0);
    assert.equal(pk.listLaterRequests().length, 0);
    assert.equal(db.prepare('SELECT status FROM pk_lines WHERE batch_id=? AND seq=1').get(b1).status, 'shortage', 'ピッカーの欠品記録は残す');
  });
  t('二度目は冪等 (existed)', () => {
    assert.deepEqual(pk.cancelLaterRequest(id, 'admin@test'), { id, status: 'cancelled', existed: true });
  });
  throwsCode(() => pk.cancelLaterRequest(99999, 'admin@test'), 'not_found', '無い id は 404');
}

// ═══ ② 展開済み (requested) の依頼 ════════════════════════════════════════
console.log('── ② 展開済みの依頼: 未着手なら取り下げ・着手済みは 409 ──');
{
  const b2 = mkPickBatch('L2', { sku: 'l-two', lineQty: 1, slipQtys: [1] });
  const pb2 = mkPackBatch('L2', { sku: 'l-two', slipQtys: [1], pkId: b2 });
  ev(b2, 'start');
  ev(b2, 'shortage', { lineSeq: 1, shortageQty: 1, altQty: 0, remaining: 'later' });
  assert.equal(pk.bindPendingLaterRequests(), 1);
  const req2 = pk.listLaterRequests({ status: 'requested' });
  const task2 = db.prepare("SELECT * FROM pk_pack_tasks WHERE batch_id=? AND kind='repick'").get(pb2);
  t('展開済み (requested) は status=requested で一覧できる。未着手なら取り下げ → タスク取消・伝票の保留解除', () => {
    assert.equal(req2.length, 1);
    assert.equal(task2.status, 'requested');
    assert.equal(db.prepare('SELECT status FROM pk_pack_slips WHERE batch_id=? AND seq=1').get(pb2).status, 'held');
    pk.cancelLaterRequest(req2[0].id, 'admin@test');
    assert.equal(lr(req2[0].id).status, 'cancelled');
    assert.equal(db.prepare('SELECT status FROM pk_pack_tasks WHERE id=?').get(task2.id).status, 'cancelled');
    assert.equal(db.prepare('SELECT status FROM pk_pack_slips WHERE batch_id=? AND seq=1').get(pb2).status, 'pending');
  });

  const b3 = mkPickBatch('L3', { sku: 'l-three', lineQty: 1, slipQtys: [1] });
  const pb3 = mkPackBatch('L3', { sku: 'l-three', slipQtys: [1], pkId: b3 });
  ev(b3, 'start');
  ev(b3, 'shortage', { lineSeq: 1, shortageQty: 1, altQty: 0, remaining: 'later' });
  pk.bindPendingLaterRequests();
  const req3 = pk.listLaterRequests({ status: 'requested' });
  const task3 = db.prepare("SELECT * FROM pk_pack_tasks WHERE batch_id=? AND kind='repick'").get(pb3);
  psvc.applyTaskAction(task3.id, 'claim', '田中美波');
  throwsCode(() => pk.cancelLaterRequest(req3[0].id, 'admin@test'), 'later_in_progress', 'ピッカーが対応を始めていれば取り下げできない');
  t('409 のときは何も変わらない', () => {
    assert.equal(lr(req3[0].id).status, 'requested');
    assert.equal(db.prepare('SELECT status FROM pk_pack_tasks WHERE id=?').get(task3.id).status, 'claimed');
  });
}

// ═══ ③ 見つかった → 向かっているピッカーへ取下げバナー ══════════════════════
console.log('── ③ 1階の「見つかった」で claimed/fulfilled の依頼を取り下げると 3階へバナー ──');
{
  const pb = mkPackBatch('F1', { sku: 'f-one', slipQtys: [1, 1, 1], held: [1, 2, 3] });
  const mkTask = (seq, status) => Number(db.prepare(`INSERT INTO pk_pack_tasks (batch_id, slip_seq, kind, sku, product_name, req_qty, location, block, folder_name, status, requested_by, claimed_by, created_at, updated_at)
    VALUES (?, ?, 'repick', 'f-one', '商品F', 2, '00100101', 'P3FA', '出荷_99', ?, '三宅晴菜', ?, ?, ?)`)
    .run(pb, seq, status, status === 'requested' ? null : '田中美波', now, now).lastInsertRowid);
  const t1 = mkTask(1, 'claimed');
  const t2 = mkTask(2, 'requested');
  const t3 = mkTask(3, 'fulfilled');
  t('claimed の依頼を取り下げ → to_picking に repick_cancelled (task_id 付き・商品と伝票が分かる)', () => {
    pev(pb, 'found', { slipSeq: 1 });
    const a = alerts('to_picking').filter((x) => x.kind === 'repick_cancelled');
    assert.equal(a.length, 1);
    assert.equal(a[0].task_id, t1);
    assert.match(a[0].message, /出荷_99 #1 の再ピック「商品F ×2」は1階で見つかったため取り下げ/);
    assert.equal(a[0].link, '/apps/picking/');
    assert.equal(db.prepare('SELECT status FROM pk_pack_tasks WHERE id=?').get(t1).status, 'cancelled');
  });
  t('requested (未着手) の取り下げにはバナーを出さない', () => {
    pev(pb, 'found', { slipSeq: 2 });
    assert.equal(alerts('to_picking').filter((x) => x.kind === 'repick_cancelled').length, 1);
    assert.equal(db.prepare('SELECT status FROM pk_pack_tasks WHERE id=?').get(t2).status, 'cancelled');
  });
  t('fulfilled (届ける途中) の取り下げにも出る', () => {
    pev(pb, 'found', { slipSeq: 3 });
    const a = alerts('to_picking').filter((x) => x.kind === 'repick_cancelled');
    assert.equal(a.length, 2);
    assert.ok(a.some((x) => x.task_id === t3));
  });
  t('同じタスクで二度は出ない (task_id で集約)', () => {
    pk.createFloorAlert('repick_cancelled', '三宅晴菜', 'dup', '/apps/picking/', t1, null);
    assert.equal(alerts('to_picking').filter((x) => x.kind === 'repick_cancelled').length, 2);
  });
}

console.log(`\n${passed} tests passed`);
