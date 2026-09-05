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
const psvcPicking = await import('../service.js');
const { applyEvent, bindPendingLaterRequests, reconcileRepickBatches, announceShortageToPacking, listFloorAlerts, syncRepickTask } = psvcPicking;
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
  assert.deepEqual(psvc.shortageSummaryFor(b), { stockoutWait: 1, repickWait: 0, candidateWait: 0, closed: 0 });
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
  assert.deepEqual(psvc.shortageSummaryFor(b), { stockoutWait: 0, repickWait: 0, candidateWait: 0, closed: 1 });
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
    // reconcileRepickBatches が reconcileShortageAlerts 経由で既に出している → 直接呼んでも増えない (0)
    assert.equal(announceShortageToPacking(pk, 1, '有國陽'), 0);
    const a = alerts();
    assert.equal(a.length, 1);
    assert.match(a[0].message, /^🕒 出荷_99 #1 肉球クリーム 15g ×1 — 後で取りに行きます/);
    assert.equal(slipRow(pb, 1).status, 'held');
    const b = db.prepare('SELECT * FROM pk_pack_batches WHERE id=?').get(pb);
    assert.deepEqual(psvc.shortageSummaryFor(b), { stockoutWait: 0, repickWait: 1, candidateWait: 0, closed: 0 });
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

// ═══ Codex R1: ガードの迂回・作成漏れの収束・解決キーの厳密さ・取消との整合・退路 ═══════════
console.log('── Codex R1: ライン確定後は ❌ の伝票も確認できない ──');
{
  const pk = mkPickBatch('V4', { sku: 'v-line', lineQty: 1, slipQtys: [1] });
  const pb = mkPackBatch('V4', { sku: 'v-line', slipQtys: [1] });
  db.prepare(`UPDATE pk_batches SET hikiate_class='ネコポス【梱包機PAS-LINE《3つ折り》】単品' WHERE id=?`).run(pk);
  db.prepare('UPDATE pk_pack_batches SET pk_batch_id=? WHERE id=?').run(pk, pb);
  ev(pk, 'start');
  ev(pk, 'shortage', { lineSeq: 1, shortageQty: 1, altQty: 0, remaining: 'none' });
  announceShortageToPacking(pk, 1, '有國陽');
  db.prepare(`INSERT INTO pk_pack_line_runs (batch_id, phase, started_at, finished_at, planned_count, final_count, manual_count, worker, updated_at)
    VALUES (?, 'run', ?, ?, 1, 1, 0, '三宅晴菜', ?)`).run(pb, now, now, now);
  throwsCode(() => pev(pb, 'stockout_ack', { slipSeq: 1 }), 'line_already_finalized', 'pending 経路でもライン確定後は 409');
  assert.deepEqual(psvc.getWorkState(pb).stockoutAckSeqs, [], '確認可能にも含めない');
}

console.log('── Codex R1: 梱包取込前の欠品 → 取込後に収束 (リンクと #seq が付く) ──');
{
  const pk = mkPickBatch('V5', { sku: 'v-late', lineQty: 1, slipQtys: [1], name: 'あとから取込' });
  ev(pk, 'start');
  ev(pk, 'shortage', { lineSeq: 1, shortageQty: 1, altQty: 0, remaining: 'none' });
  t('取込前: リンクなし・NE 番号表示', () => {
    assert.equal(announceShortageToPacking(pk, 1, '有國陽'), 1);
    const a = alerts().find((x) => x.ref_key === `alloc:${pk}:1:V5-NE1`);
    assert.equal(a.link, null);
    assert.match(a.message, /\(NE V5-NE1\)/);
  });
  t('取込後 (reconcileShortageAlerts): 同じバナーが #1 とリンク付きに更新される (増えない)', () => {
    const pb = mkPackBatch('V5', { sku: 'v-late', slipQtys: [1] });
    db.prepare('UPDATE pk_pack_batches SET pk_batch_id=? WHERE id=?').run(pk, pb);
    const { reconcileShortageAlerts } = psvcPicking;
    assert.equal(reconcileShortageAlerts({ tbNo: 'V5' }), 1);
    const mine = alerts().filter((x) => x.ref_key === `alloc:${pk}:1:V5-NE1`);
    assert.equal(mine.length, 1);
    assert.equal(mine[0].link, `/apps/packing/work/${pb}?seq=1`);
    assert.match(mine[0].message, /出荷_99 #1 あとから取込/);
    assert.equal(reconcileShortageAlerts({ tbNo: 'V5' }), 0, '変化なしなら何もしない');
  });
  t('担当者を省略しても欠品イベントの担当者で作れる (replay・収束用)', () => {
    db.prepare("DELETE FROM pk_floor_alerts WHERE ref_key=?").run(`alloc:${pk}:1:V5-NE1`);
    assert.equal(announceShortageToPacking(pk, 1), 1);
    assert.match(alerts().find((x) => x.ref_key === `alloc:${pk}:1:V5-NE1`).message, /有國陽/);
  });
}

console.log('── 収束は1階が閉じたバナーを復活させない (× / 在庫なし確認) ──');
{
  const pk = mkPickBatch('V5B', { sku: 'v-ack', lineQty: 1, slipQtys: [1] });
  const pb = mkPackBatch('V5B', { sku: 'v-ack', slipQtys: [1] });
  db.prepare('UPDATE pk_pack_batches SET pk_batch_id=? WHERE id=?').run(pk, pb);
  ev(pk, 'start');
  ev(pk, 'shortage', { lineSeq: 1, shortageQty: 1, altQty: 0, remaining: 'none' });
  const ref = `alloc:${pk}:1:V5B-NE1`;
  t('1階が × で閉じたバナーは reconcile で復活しない', () => {
    announceShortageToPacking(pk, 1, '有國陽');
    const a = alerts().find((x) => x.ref_key === ref);
    psvcPicking.ackFloorAlert(a.id, '三宅晴菜', 'to_packing');
    assert.equal(psvcPicking.reconcileShortageAlerts({ tbNo: 'V5B' }), 0);
    assert.equal(announceShortageToPacking(pk, 1, '有國陽'), 0);
    assert.equal(alerts().filter((x) => x.ref_key === ref).length, 0);
    assert.equal(db.prepare('SELECT COUNT(*) c FROM pk_floor_alerts WHERE ref_key=?').get(ref).c, 1, '行も増えない');
  });
  t('在庫なし確認で閉じた後も復活しない (配賦は残っている)', () => {
    pev(pb, 'stockout_ack', { slipSeq: 1 });
    assert.equal(db.prepare('SELECT COUNT(*) c FROM pk_shortage_allocations WHERE batch_id=?').get(pk).c, 1);
    assert.equal(psvcPicking.reconcileShortageAlerts({ tbNo: 'V5B' }), 0);
    assert.equal(alerts().filter((x) => x.ref_key === ref).length, 0);
  });
}
{
  const pk = mkPickBatch('V5C', { sku: 'v-redo', lineQty: 1, slipQtys: [1] });
  const pb = mkPackBatch('V5C', { sku: 'v-redo', slipQtys: [1] });
  db.prepare('UPDATE pk_pack_batches SET pk_batch_id=? WHERE id=?').run(pk, pb);
  ev(pk, 'start');
  ev(pk, 'shortage', { lineSeq: 1, shortageQty: 1, altQty: 0, remaining: 'none' });
  const so = lastOp();
  const ref = `alloc:${pk}:1:V5C-NE1`;
  t('× で閉じた後に 戻る → 再度の欠品は新しい配賦なので、バナーは改めて出る', () => {
    announceShortageToPacking(pk, 1, '有國陽');
    psvcPicking.ackFloorAlert(alerts().find((x) => x.ref_key === ref).id, '三宅晴菜', 'to_packing');
    ev(pk, 'back', { lineSeq: 1, undoOpId: so });
    // 同一秒の比較を避ける (人の操作では数秒以上空く)
    db.prepare("UPDATE pk_floor_alerts SET resolved_at=datetime(resolved_at, '-5 seconds'), acked_at=datetime(acked_at, '-5 seconds') WHERE ref_key=?").run(ref);
    ev(pk, 'shortage', { lineSeq: 1, shortageQty: 1, altQty: 0, remaining: 'none' });
    assert.equal(announceShortageToPacking(pk, 1, '有國陽'), 1);
    assert.equal(alerts().filter((x) => x.ref_key === ref).length, 1);
  });
}

console.log('── Codex R1: 解決は配賦の正確なキー (別バッチの同じ NE 番号は閉じない) ──');
{
  // 同じ NE 番号 (再利用) を持つ2つの picking バッチ。片方の伝票を1階が閉じても、もう片方のバナーは残る
  const pkA = mkPickBatch('V6A', { sku: 'v-dup', lineQty: 1, slipQtys: [1] });
  const pkB = mkPickBatch('V6B', { sku: 'v-dup', lineQty: 1, slipQtys: [1] });
  db.prepare("UPDATE pk_slip_lines SET ne_slip_no='SAME-NE' WHERE batch_id IN (?, ?)").run(pkA, pkB);
  const pbA = mkPackBatch('V6A', { sku: 'v-dup', slipQtys: [1] });
  db.prepare("UPDATE pk_pack_slips SET ne_slip_no='SAME-NE' WHERE batch_id=?").run(pbA);
  db.prepare('UPDATE pk_pack_batches SET pk_batch_id=? WHERE id=?').run(pkA, pbA);
  for (const pk of [pkA, pkB]) {
    ev(pk, 'start');
    ev(pk, 'shortage', { lineSeq: 1, shortageQty: 1, altQty: 0, remaining: 'none' });
    announceShortageToPacking(pk, 1, '有國陽');
  }
  t('A の伝票を出荷保留にしても B のバナーは残る', () => {
    assert.equal(alerts().filter((x) => x.ref_key.endsWith(':SAME-NE')).length, 2);
    pev(pbA, 'stockout_ack', { slipSeq: 1 });
    const left = alerts().filter((x) => x.ref_key.endsWith(':SAME-NE'));
    assert.equal(left.length, 1);
    assert.equal(left[0].ref_key, `alloc:${pkB}:1:SAME-NE`);
  });
}

console.log('── Codex R1: バッチ取消は出荷保留 (在庫なし) の伝票を戻さない ──');
{
  const pk = mkPickBatch('V7', { sku: 'v-cancel', lineQty: 1, slipQtys: [1, 1] });
  const pb = mkPackBatch('V7', { sku: 'v-cancel', slipQtys: [1, 1] });
  db.prepare('UPDATE pk_pack_batches SET pk_batch_id=? WHERE id=?').run(pk, pb);
  ev(pk, 'start');
  ev(pk, 'shortage', { lineSeq: 1, shortageQty: 1, altQty: 0, remaining: 'none' });   // #2 に配賦
  const r = pev(pb, 'stockout_ack', { slipSeq: 2 });
  t('取消 → #1 は pending に戻るが、出荷保留の #2 はそのまま (outbox も残る)', () => {
    pev(pb, 'cancel', {});
    assert.equal(slipRow(pb, 1).status, 'pending');
    assert.equal(slipRow(pb, 2).status, 'cancelled');
    assert.equal(slipRow(pb, 2).hold_reason, 'stockout');
    assert.ok(db.prepare('SELECT 1 FROM pk_pack_stockouts WHERE id=?').get(r.taskNotify.stockoutId));
  });
}

console.log('── Codex R1: ❌ が誤りだった (1階で見つかった) → found で報告を取り消す ──');
{
  const pk = mkPickBatch('V8', { sku: 'v-found', lineQty: 2, slipQtys: [1, 1] });
  const pb = mkPackBatch('V8', { sku: 'v-found', slipQtys: [1, 1] });
  db.prepare('UPDATE pk_pack_batches SET pk_batch_id=? WHERE id=?').run(pk, pb);
  ev(pk, 'start');
  ev(pk, 'shortage', { lineSeq: 1, shortageQty: 2, altQty: 0, remaining: 'none' });   // #1 #2 両方に配賦
  announceShortageToPacking(pk, 1, '有國陽');
  assert.equal(alerts().filter((x) => x.ref_key.startsWith(`alloc:${pk}:`)).length, 2);
  t('found (pending・❌) → その受注の配賦だけ消える・バナーも閉じる・もう片方は残る・伝票は pending のまま', () => {
    pev(pb, 'found', { slipSeq: 2 });
    assert.equal(db.prepare('SELECT COUNT(*) c FROM pk_shortage_allocations WHERE batch_id=?').get(pk).c, 1);
    const left = alerts().filter((x) => x.ref_key.startsWith(`alloc:${pk}:`));
    assert.equal(left.length, 1);
    assert.equal(left[0].ref_key, `alloc:${pk}:1:V8-NE1`);
    assert.equal(slipRow(pb, 2).status, 'pending');
    const st = psvc.getWorkState(pb);
    assert.equal(st.slips.find((s) => s.seq === 2).pickingShortages.length, 0);
    assert.deepEqual(st.stockoutAckSeqs, [1]);
    assert.equal(db.prepare('SELECT status FROM pk_lines WHERE batch_id=? AND seq=1').get(pk).status, 'shortage', 'ピッカーの明細は触らない');
  });
  t('取り消した後は再ピック依頼が出せる (409 にならない)', () => {
    pev(pb, 'shortage', { slipSeq: 2, sku: 'v-found', qty: 1 });
    assert.equal(slipRow(pb, 2).status, 'held');
  });
  throwsCode(() => pev(pb, 'found', { slipSeq: 1, sku: 'other' }), 'no_stockout', '該当しない SKU の found は 409');
}

console.log('── Codex R1: 一覧の件数は操作可能条件と同じ ──');
{
  const pk = mkPickBatch('V9', { sku: 'v-sum', lineQty: 1, slipQtys: [1] });
  const pb = mkPackBatch('V9', { sku: 'v-sum', slipQtys: [1, 1, 1] });
  db.prepare('UPDATE pk_pack_batches SET pk_batch_id=? WHERE id=?').run(pk, pb);
  const mkTask = (seq, sku, status) => db.prepare(`INSERT INTO pk_pack_tasks (batch_id, slip_seq, kind, sku, req_qty, status, requested_by, claimed_by, created_at, updated_at)
    VALUES (?, ?, 'repick', ?, 1, ?, '三宅晴菜', '田中美波', ?, ?)`).run(pb, seq, sku, status, now, now);
  db.prepare("UPDATE pk_pack_slips SET status='held', hold_reason='repick' WHERE batch_id=? AND seq IN (1,2,3)").run(pb);
  mkTask(1, 'a', 'unavailable'); mkTask(1, 'b', 'claimed');    // #1: 在庫なし + 再ピック中 → 確認不可 = 再ピック待ち
  mkTask(2, 'a', 'unavailable');                               // #2: 在庫なしだけ → 要確認
  db.prepare(`INSERT INTO pk_pack_incidents (batch_id, slip_seq, kind, sku, qty, status, detected_by, created_at, updated_at)
    VALUES (?, 3, 'shortage', 'c', 1, 'candidate', '三宅晴菜', ?, ?)`).run(pb, now, now);   // #3: 未送信の候補だけ
  t('stockoutWait 1 / repickWait 1 / candidateWait 1 (getWorkState の stockoutAckSeqs と一致)', () => {
    const b = db.prepare('SELECT * FROM pk_pack_batches WHERE id=?').get(pb);
    assert.deepEqual(psvc.shortageSummaryFor(b), { stockoutWait: 1, repickWait: 1, candidateWait: 1, closed: 0 });
    assert.deepEqual(psvc.getWorkState(pb).stockoutAckSeqs, [2]);
  });
}

console.log(`\n${passed} tests passed`);
