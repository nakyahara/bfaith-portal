/**
 * test-unavailable-flow.mjs — 🔴再ピックバッチの結果を梱包タスクへ正しく伝える (例外処理監査 PR-1・2026-09-05)
 *
 * 守りたいこと:
 *   ① 再ピックバッチで「他ロケで全量確保」→ タスクは fulfilled (以前は unavailable になっていた — 9/3・9/5 実発生)
 *   ② 再ピックバッチでは「後で取りに行く」を使えない (依頼が pending_binding で迷子になる — 9/1 実発生)
 *   ③ 「どこにもない」→ unavailable + 1階の全端末へ赤バナー (link つき)。配賦は作らない
 *   ④ unavailable → fulfill を許す (後で見つけた・届けた)
 *   ⑤ 棚戻しキューは棚戻しだけ (再ピックは 🔴バッチに一本化 — Q4)
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
/** ピッカーが「後で取りに行く」→ 展開 → 🔴再ピックバッチ を作って返す。 */
function laterToRepick(tbNo, sku) {
  const pk = mkPickBatch(tbNo, { sku, lineQty: 1, slipQtys: [1] });
  const pb = mkPackBatch(tbNo, { sku, slipQtys: [1] });
  ev(pk, 'start');
  ev(pk, 'shortage', { lineSeq: 1, shortageQty: 1, altQty: 0, remaining: 'later' });
  bindPendingLaterRequests();
  reconcileRepickBatches();
  const task = db.prepare("SELECT * FROM pk_pack_tasks WHERE batch_id=? AND kind='repick' ORDER BY id DESC LIMIT 1").get(pb);
  const rb = db.prepare('SELECT * FROM pk_batches WHERE pack_task_id=?').get(task.id);
  assert.ok(rb, '🔴再ピックバッチが作られる');
  return { pk, pb, task, rb };
}
const taskStatus = (id) => db.prepare('SELECT status FROM pk_pack_tasks WHERE id=?').get(id).status;

// ═══ ① 他ロケで全量確保 → fulfilled ═══════════════════════════════════════════
console.log('── 再ピックバッチで他ロケから全量確保 ──');
{
  const { task, rb } = laterToRepick('U1', 'sku-u1');
  t('start → claimed', () => {
    ev(rb.id, 'start', {}, '田中美波');
    syncRepickTask(rb.id, { event: 'start', lineSeq: null }, '田中美波', psvc);
    assert.equal(taskStatus(task.id), 'claimed');
  });
  t('欠品ボタン → 他ロケで1個確保 (残り0) → バッチ done・タスク fulfilled (在庫なしにならない)', () => {
    ev(rb.id, 'shortage', { lineSeq: 1, shortageQty: 1, altQty: 1, altBlock: 'P3FA', altLocation: '004-020-05', remaining: null }, '田中美波');
    const r = syncRepickTask(rb.id, { event: 'shortage', lineSeq: 1 }, '田中美波', psvc);
    assert.equal(r.unavailable, null, '在庫なし通知は出ない');
    assert.equal(db.prepare('SELECT status FROM pk_batches WHERE id=?').get(rb.id).status, 'done');
    assert.equal(taskStatus(task.id), 'fulfilled');
    assert.equal(listFloorAlerts('to_packing').filter((a) => a.kind === 'stockout').length, 0, '赤バナーは出ない');
  });
  t('梱包側は受領待ち (repickReady) として見える', () => {
    const ready = psvc.listRepickReady();
    assert.ok(ready.some((g) => g.tasks.some((x) => x.id === task.id)), 'fulfilled = 緑バナーの元');
  });
}

// ═══ ② 再ピックバッチで「後で取りに行く」は拒否 ═════════════════════════════
console.log('── 再ピックバッチでの「後で取りに行く」 ──');
{
  const { rb, task } = laterToRepick('U2', 'sku-u2');
  ev(rb.id, 'start', {}, '田中美波');
  throwsCode(() => ev(rb.id, 'shortage', { lineSeq: 1, shortageQty: 1, altQty: 0, remaining: 'later' }, '田中美波'),
    'later_in_repick', '再ピックバッチの「後で取りに行く」は 400 later_in_repick');
  t('拒否されたので依頼 (pk_later_requests) は増えていない・バッチも進んでいない', () => {
    assert.equal(db.prepare("SELECT COUNT(*) c FROM pk_later_requests WHERE batch_id=?").get(rb.id).c, 0);
    assert.equal(db.prepare('SELECT status FROM pk_lines WHERE batch_id=? AND seq=1').get(rb.id).status, 'pending');
    assert.equal(taskStatus(task.id), 'requested');
  });

  // ═══ ③ どこにもない → unavailable + 赤バナー ═══════════════════════════════
  console.log('── 再ピックバッチで「どこにもない」 ──');
  t('shortage(none) → タスク unavailable・配賦なし・バッチ done でも fulfill しない', () => {
    ev(rb.id, 'shortage', { lineSeq: 1, shortageQty: 1, altQty: 0, remaining: 'none' }, '田中美波');
    const r = syncRepickTask(rb.id, { event: 'shortage', lineSeq: 1 }, '田中美波', psvc);
    assert.ok(r.unavailable, '在庫なし通知の情報が返る');
    assert.equal(r.unavailable.remaining, 1);
    assert.equal(taskStatus(task.id), 'unavailable');
    assert.equal(db.prepare('SELECT COUNT(*) c FROM pk_shortage_allocations WHERE batch_id=?').get(rb.id).c, 0, '再ピックバッチには配賦を作らない');
    assert.equal(db.prepare('SELECT status FROM pk_batches WHERE id=?').get(rb.id).status, 'done');
    // done 後の同期でも fulfill に化けない (在庫なしの明細がある)
    syncRepickTask(rb.id, { event: 'next', lineSeq: 1 }, '田中美波', psvc);
    assert.equal(taskStatus(task.id), 'unavailable');
  });
  t('1階の全端末へ赤バナー (商品・伝票・数量・誰が) + 対象伝票へのリンク', () => {
    const a = listFloorAlerts('to_packing').find((x) => x.kind === 'stockout');
    assert.ok(a, 'stockout バナーがある');
    assert.match(a.message, /在庫なし/);
    assert.match(a.message, /出荷_88 #1/);
    assert.match(a.message, /田中美波/);
    assert.equal(a.link, `/apps/packing/work/${task.batch_id}?seq=1`);
  });
  t('梱包側 getWorkState に在庫なしの報告 (stockoutBySlip) が出る', () => {
    const st = psvc.getWorkState(task.batch_id);
    assert.deepEqual(Object.keys(st.stockoutBySlip), ['1']);
    assert.equal(st.stockoutBySlip[1][0].sku, 'sku-u2');
    assert.equal(st.stockoutBySlip[1][0].claimed_by, '田中美波');
  });
  // ═══ ④ unavailable → fulfill ═════════════════════════════════════════════
  t('在庫なしにした後で届けた: unavailable → fulfill を許す', () => {
    psvc.applyTaskAction(task.id, 'fulfill', '田中美波');
    assert.equal(taskStatus(task.id), 'fulfilled');
    assert.deepEqual(Object.keys(psvc.getWorkState(task.batch_id).stockoutBySlip), [], '在庫なしの報告は消える');
  });
}

// ═══ ⑤ 棚戻しキューは棚戻しだけ ═══════════════════════════════════════════════
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
}

console.log(`\n${passed} tests passed`);
