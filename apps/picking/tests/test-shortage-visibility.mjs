/**
 * test-shortage-visibility.mjs — ピッカーの欠品 (🕒 後で / ❌ どこにもない) を1階に見せる (例外処理監査 PR-2・2026-09-06)
 *
 * 守りたいこと:
 *   ① 「どこにもない」→ 1階の全端末に ❌ バナー (配賦した伝票ごと・対象伝票リンク・商品名・ピッカー)。冪等
 *   ② 「後で取りに行く」→ 🕒 バナー。受領 (receive) で閉じる
 *   ③ 他ロケで全量確保 → バナーは出ない
 *   ④ back で欠品を取り消したらバナーも閉じる
 *   ⑤ ❌ の未処理伝票は、再ピック依頼を経ずに1階が「在庫なしを確認」で出荷保留にできる (stockoutBySlip / stockoutAckSeqs / outbox)
 *   ⑥ ❌ の商品を1階が再依頼しようとすると 409 stockout_reported (3階を二度呼ばない)
 *   ⑦ バッチ一覧の件数 (shortageSummaryFor)・梱包画面のバッジに商品名が付く
 *
 * 実行: node apps/picking/tests/test-shortage-visibility.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'picking-shortvis-test-'));

const { initPickingDB, getDB, jstToday } = await import('../db.js');
const { applyEvent, bindPendingLaterRequests, reconcileRepickBatches, announceShortageToPacking, listFloorAlerts, syncRepickTask } = await import('../service.js');
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
const ev = (batchId, event, extra = {}, worker = '有國陽') => applyEvent(batchId, { opId: `v${++op}`, event, ...extra }, worker);
const pev = (batchId, event, extra = {}, worker = '三宅晴菜') => psvc.applyEvent(batchId, { opId: `pv${++op}`, event, ...extra }, worker);
const lastOp = () => `v${op}`;

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
function mkPackBatch(tbNo, { sku, slipQtys }) {
  const info = db.prepare(`INSERT INTO pk_pack_batches
    (tb_key, folder_name, work_date, slip_count, line_count, total_qty, match_status, status, worker, started_at, csv_sha256, imported_by, created_at, updated_at)
    VALUES (?, '出荷_99', ?, ?, ?, ?, 'ok', 'packing', '三宅晴菜', ?, ?, 'test', ?, ?)`)
    .run(tbNo, today, slipQtys.length, slipQtys.length, slipQtys.reduce((a, b) => a + b, 0), now, `psha-${tbNo}`, now, now);
  const batchId = Number(info.lastInsertRowid);
  slipQtys.forEach((q, i) => {
    const sid = Number(db.prepare(`INSERT INTO pk_pack_slips (batch_id, seq, ne_slip_no, slip_no, recipient_name, site_order_no, status, delivery_method)
      VALUES (?, ?, ?, ?, ?, ?, 'pending', '箱')`)
      .run(batchId, i + 1, `${tbNo}-NE${i + 1}`, `SP${tbNo}-${i + 1}`, `客${i + 1}`, `SO-${i + 1}`).lastInsertRowid);
    db.prepare('INSERT INTO pk_pack_lines (slip_id, sku, product_name, qty) VALUES (?, ?, ?, ?)').run(sid, sku, 'テスト商品', q);
  });
  return batchId;
}
const alerts = () => listFloorAlerts('to_packing').filter((a) => a.kind === 'picking_shortage');
const slipRow = (b, seq) => db.prepare('SELECT * FROM pk_pack_slips WHERE batch_id=? AND seq=?').get(b, seq);

// ═══ ① ❌ どこにもない → バナー ═══════════════════════════════════════════════
console.log('── ❌ どこにもない → 1階のバナー ──');
const pk1 = mkPickBatch('V1', { sku: 'v-none', lineQty: 2, slipQtys: [1, 1], name: 'くるみサンド 800g' });
const pb1 = mkPackBatch('V1', { sku: 'v-none', slipQtys: [1, 1] });
ev(pk1, 'start');
ev(pk1, 'shortage', { lineSeq: 1, shortageQty: 1, altQty: 0, remaining: 'none' });   // 1個だけ無い → 後ろの受注 (#2) に配賦
const noneOp = lastOp();
t('announceShortageToPacking: 配賦した伝票 #2 に ❌ バナー (商品名・数量・ピッカー・対象伝票リンク・ref_key)', () => {
  assert.equal(announceShortageToPacking(pk1, 1, '有國陽'), 1);
  const a = alerts();
  assert.equal(a.length, 1);
  assert.match(a[0].message, /^❌ 出荷_99 #2 くるみサンド 800g ×1 — どのロケにもありません/);
  assert.match(a[0].message, /有國陽/);
  assert.equal(a[0].link, `/apps/packing/work/${pb1}?seq=2`);
  assert.equal(a[0].ref_key, `alloc:${pk1}:1:V1-NE2`);
});
t('もう一度呼んでも増えない (冪等)', () => {
  assert.equal(announceShortageToPacking(pk1, 1, '有國陽'), 0);
  assert.equal(alerts().length, 1);
});

// ═══ ⑤ ⑥ ⑦ 1階側 ═══════════════════════════════════════════════════════════
console.log('── 1階: ❌ の伝票を「在庫なしを確認」で出荷保留に ──');
t('getWorkState: ❌ の伝票にバッジ (商品名つき)・stockoutBySlip (source=picking)・stockoutAckSeqs', () => {
  const st = psvc.getWorkState(pb1);
  const s2 = st.slips.find((s) => s.seq === 2);
  assert.equal(s2.pickingShortages.length, 1);
  assert.equal(s2.pickingShortages[0].kind, 'none');
  assert.equal(s2.pickingShortages[0].name, 'くるみサンド 800g');
  assert.equal(s2.pickingShortages[0].picker, '有國陽');
  assert.deepEqual(Object.keys(st.stockoutBySlip), ['2']);
  assert.equal(st.stockoutBySlip[2][0].source, 'picking');
  assert.equal(st.stockoutBySlip[2][0].claimed_by, '有國陽');
  assert.deepEqual(st.stockoutAckSeqs, [2]);
  assert.equal(st.slips.find((s) => s.seq === 1).pickingShortages.length, 0, '配賦されていない伝票には出ない');
});
t('バッチ一覧の件数: 在庫なし待ち 1', () => {
  const b = db.prepare('SELECT * FROM pk_pack_batches WHERE id=?').get(pb1);
  assert.deepEqual(psvc.shortageSummaryFor(b), { stockoutWait: 1, repickWait: 0, closed: 0 });
});
throwsCode(() => pev(pb1, 'shortage', { slipSeq: 2, sku: 'v-none', qty: 1 }), 'stockout_reported',
  '❌ の商品を1階が再依頼しようとすると 409 (3階を二度呼ばない)');
t('在庫なしを確認 → 伝票 cancelled/stockout・outbox にピッカー名つきの明細・バナーは閉じる・件数は 出荷保留 1', () => {
  pev(pb1, 'next', { slipSeq: 1 });   // #1 を先に完了
  const r = pev(pb1, 'stockout_ack', { slipSeq: 2 });
  assert.equal(slipRow(pb1, 2).status, 'cancelled');
  assert.equal(slipRow(pb1, 2).hold_reason, 'stockout');
  assert.equal(r.taskNotify.kind, 'stockout');
  assert.equal(r.taskNotify.items[0].sku, 'v-none');
  assert.equal(r.taskNotify.items[0].claimedBy, '有國陽');
  assert.equal(r.batchStatus, 'done', '最後の1枚だったのでバッチ完了');
  assert.equal(alerts().length, 0, '❌ バナーは閉じる');
  const b = db.prepare('SELECT * FROM pk_pack_batches WHERE id=?').get(pb1);
  assert.deepEqual(psvc.shortageSummaryFor(b), { stockoutWait: 0, repickWait: 0, closed: 1 });
});
throwsCode(() => pev(pb1, 'stockout_ack', { slipSeq: 1 }), 'not_held', '❌ の無い伝票 (完了済み) は確認できない');

// ═══ ④ back で取り消し ═══════════════════════════════════════════════════════
console.log('── back でバナーが閉じる ──');
{
  const pk = mkPickBatch('V2', { sku: 'v-back', lineQty: 1, slipQtys: [1] });
  mkPackBatch('V2', { sku: 'v-back', slipQtys: [1] });
  ev(pk, 'start');
  ev(pk, 'shortage', { lineSeq: 1, shortageQty: 1, altQty: 0, remaining: 'none' });
  const so = lastOp();
  announceShortageToPacking(pk, 1, '有國陽');
  assert.equal(alerts().length, 1);
  t('back → 配賦が消え、バナーも resolved', () => {
    ev(pk, 'back', { lineSeq: 1, undoOpId: so });
    assert.equal(alerts().length, 0);
    assert.equal(db.prepare('SELECT COUNT(*) c FROM pk_shortage_allocations WHERE batch_id=?').get(pk).c, 0);
  });
  t('他ロケで全量確保 → バナーは出ない', () => {
    ev(pk, 'shortage', { lineSeq: 1, shortageQty: 1, altQty: 1, altBlock: 'P3FB', altLocation: '002-013-03', remaining: null });
    assert.equal(announceShortageToPacking(pk, 1, '有國陽'), 0);
    assert.equal(alerts().length, 0);
  });
}
void noneOp;

// ═══ ② 🕒 後で取りに行く → 受領で閉じる ═══════════════════════════════════════
console.log('── 🕒 後で取りに行く ──');
{
  const pk = mkPickBatch('V3', { sku: 'v-later', lineQty: 1, slipQtys: [1], name: '肉球クリーム 15g' });
  const pb = mkPackBatch('V3', { sku: 'v-later', slipQtys: [1] });
  ev(pk, 'start');
  ev(pk, 'shortage', { lineSeq: 1, shortageQty: 1, altQty: 0, remaining: 'later' });
  bindPendingLaterRequests();
  reconcileRepickBatches();
  t('🕒 バナー (橙の文言)・伝票は保留 repick・一覧の件数は 再ピック待ち 1', () => {
    assert.equal(announceShortageToPacking(pk, 1, '有國陽'), 1);
    const a = alerts();
    assert.equal(a.length, 1);
    assert.match(a[0].message, /^🕒 出荷_99 #1 肉球クリーム 15g ×1 — 後で取りに行きます/);
    assert.equal(slipRow(pb, 1).status, 'held');
    const b = db.prepare('SELECT * FROM pk_pack_batches WHERE id=?').get(pb);
    assert.deepEqual(psvc.shortageSummaryFor(b), { stockoutWait: 0, repickWait: 1, closed: 0 });
    assert.deepEqual(psvc.getWorkState(pb).stockoutAckSeqs, [], '🕒 は在庫なし確認の対象ではない (3階が持ってくる)');
  });
  t('3階が届けて1階が受け取る → 🕒 バナーは閉じる', () => {
    const task = db.prepare("SELECT * FROM pk_pack_tasks WHERE batch_id=? AND kind='repick'").get(pb);
    const rb = db.prepare('SELECT * FROM pk_batches WHERE pack_task_id=?').get(task.id);
    ev(rb.id, 'start', {}, '田中美波');
    syncRepickTask(rb.id, { event: 'start' }, '田中美波', psvc);
    ev(rb.id, 'next', { lineSeq: 1 }, '田中美波');
    syncRepickTask(rb.id, { event: 'next' }, '田中美波', psvc);
    assert.equal(db.prepare('SELECT status FROM pk_pack_tasks WHERE id=?').get(task.id).status, 'fulfilled');
    pev(pb, 'receive', { slipSeq: 1 });
    assert.equal(slipRow(pb, 1).status, 'pending');
    assert.equal(alerts().length, 0);
  });
}

console.log(`\n${passed} tests passed`);
