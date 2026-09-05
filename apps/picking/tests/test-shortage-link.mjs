/**
 * test-shortage-link.mjs — 欠品フローv2 PR2 (picking↔packing 連携) の検証
 *
 * 守りたいこと:
 *   ① 不足は**配賦された伝票だけ**に伝わる (ピッキング順の後ろの受注から — Q2既定)
 *   ② 「後で取りに行く」は既存の再ピック機構に乗る (タスク→1行バッチ→伝票保留)
 *   ③ back で依頼が取り下がる。ただし対応が始まっていたら (claimed以降) back を拒否
 *   ④ 「どこにもない」はバッジだけ (タスクも保留も作らない)
 *
 * packing のテーブルは picking.db に同居しているので、両方の init で同じDBに揃う。
 * 実行: node apps/picking/tests/test-shortage-link.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'picking-shortlink-test-'));

const { initPickingDB, getDB, jstToday, listBatches } = await import('../db.js');
const {
  applyEvent, listShortageAllocations, bindPendingLaterRequests, reconcileRepickBatches,
  resetLaterBindingsForPackBatch, PkError,
} = await import('../service.js');
const { initPackingDB } = await import('../../packing/db.js');
const { getWorkState } = await import('../../packing/service.js');

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

// PR-2 で name/picker/at が付いたので、配賦の中身 (sku/qty/kind) だけを比べる
const core = (arr) => (arr || []).map(({ sku, qty, kind }) => ({ sku, qty, kind }));
let op = 0;
const ev = (batchId, event, extra = {}, worker = '星立夏') =>
  applyEvent(batchId, { opId: `t${++op}`, event, ...extra }, worker);

/** picking バッチ + 明細 + 受注別明細を作る。 */
function mkPickBatch(tbNo, { sku = 'testsku', lineQty, slipQtys }) {
  const info = db.prepare(`INSERT INTO pk_batches
    (tb_no, hikiate_class, folder_name, work_date, instruct_date, composition,
     delivery_method, invoice_soft, line_count, slip_count, total_qty, status, validity,
     csv_sha256, imported_by, created_at, updated_at)
    VALUES (?, 'AES《単品》', '出荷_88', ?, NULL, '単品', NULL, NULL, 1, ?, ?, 'ready', 'valid',
      ?, 'test', ?, ?)`)
    .run(tbNo, today, slipQtys.length, lineQty, `sha-${tbNo}`, now, now);
  const batchId = Number(info.lastInsertRowid);
  db.prepare(`INSERT INTO pk_lines (batch_id, seq, location, block, sku, product_name, barcode, qty)
    VALUES (?, 1, '001-001-01', 'P3FA', ?, 'テスト商品', NULL, ?)`).run(batchId, sku, lineQty);
  const ins = db.prepare(`INSERT INTO pk_slip_lines
    (batch_id, slip_no, picking_no, ne_slip_no, sku, qty, location)
    VALUES (?, ?, NULL, ?, ?, ?, '001-001-01')`);
  slipQtys.forEach((q, i) => ins.run(batchId, `SP${tbNo}-${i + 1}`, `${tbNo}-NE${i + 1}`, sku, q));
  return batchId;
}

/** packing バッチ + 伝票 + 明細を作る (ne_slip_no を picking 側と揃える)。 */
function mkPackBatch(tbNo, { sku = 'testsku', slipQtys }) {
  const info = db.prepare(`INSERT INTO pk_pack_batches
    (tb_key, folder_name, work_date, slip_count, line_count, total_qty,
     match_status, status, worker, csv_sha256, imported_by, created_at, updated_at)
    VALUES (?, '出荷_88', ?, ?, ?, ?, 'ok', 'ready', NULL, ?, 'test', ?, ?)`)
    .run(tbNo, today, slipQtys.length, slipQtys.length,
      slipQtys.reduce((a, b) => a + b, 0), `psha-${tbNo}`, now, now);
  const batchId = Number(info.lastInsertRowid);
  slipQtys.forEach((q, i) => {
    const sid = Number(db.prepare(`INSERT INTO pk_pack_slips
      (batch_id, seq, ne_slip_no, slip_no, recipient_name, site_order_no, status, delivery_method)
      VALUES (?, ?, ?, ?, ?, ?, 'pending', '箱')`)
      .run(batchId, i + 1, `${tbNo}-NE${i + 1}`, `SP${tbNo}-${i + 1}`, `客${i + 1}`, `SO-${i + 1}`).lastInsertRowid);
    db.prepare('INSERT INTO pk_pack_lines (slip_id, sku, product_name, qty) VALUES (?, ?, ?, ?)')
      .run(sid, sku, 'テスト商品', q);
  });
  return batchId;
}

// ═══ ①② 「後で取りに行く」: 配賦 → 展開 → 保留 ═══════════════════════════
console.log('── 後で取りに行く (3伝票×1個・2個不足) ──');
const pkA = mkPickBatch('TB-A', { lineQty: 3, slipQtys: [1, 1, 1] });
ev(pkA, 'start');
const shortOp = `t${++op}`;
applyEvent(pkA, { opId: shortOp, event: 'shortage', lineSeq: 1, shortageQty: 2, altQty: 0, remaining: 'later' }, '星立夏');

t('配賦はピッキング順の後ろの受注から (NE3, NE2 に1個ずつ・NE1は無傷)', () => {
  const a = listShortageAllocations(pkA, 1);
  assert.equal(a.length, 2);
  assert.deepEqual(a.map((x) => x.ne_slip_no).sort(), ['TB-A-NE2', 'TB-A-NE3']);
  assert.ok(a.every((x) => x.qty === 1 && x.kind === 'later'));
});

t('梱包が未取込のうちは pending_binding で待つ', () => {
  const lr = db.prepare('SELECT * FROM pk_later_requests WHERE batch_id=?').get(pkA);
  assert.equal(lr.status, 'pending_binding');
  assert.equal(lr.qty, 2);
  assert.equal(lr.requested_by, '星立夏');
  assert.equal(bindPendingLaterRequests(), 0, '取込前は展開されない');
});

const packA = mkPackBatch('TB-A', { slipQtys: [1, 1, 1] });

t('梱包取込後の展開: 配賦先の伝票ごとに repick タスク + 伝票保留', () => {
  assert.equal(bindPendingLaterRequests(), 1);
  const lr = db.prepare('SELECT * FROM pk_later_requests WHERE batch_id=?').get(pkA);
  assert.equal(lr.status, 'requested');
  const tasks = db.prepare("SELECT * FROM pk_pack_tasks WHERE later_request_id=? ORDER BY slip_seq").all(lr.id);
  assert.equal(tasks.length, 2);
  assert.deepEqual(tasks.map((x) => x.slip_seq), [2, 3]);
  assert.ok(tasks.every((x) => x.status === 'requested' && x.req_qty === 1 && x.requested_by === '星立夏'));
  const held = db.prepare("SELECT seq FROM pk_pack_slips WHERE batch_id=? AND status='held' AND hold_reason='repick' ORDER BY seq").all(packA);
  assert.deepEqual(held.map((x) => x.seq), [2, 3], '配賦された伝票だけ保留');
});

t('展開は冪等 (もう一度呼んでもタスクは増えない)', () => {
  bindPendingLaterRequests();
  reconcileRepickBatches();
  const n = db.prepare("SELECT COUNT(*) c FROM pk_pack_tasks WHERE batch_id=? AND kind='repick'").get(packA).c;
  assert.equal(n, 2);
});

t('reconcile で「後で取りに行く」1行バッチがピッキング一覧に出る', () => {
  const repicks = listBatches(today).filter((b) => b.origin === 'repick');
  assert.equal(repicks.length, 2, 'タスクごとに1行バッチ');
});

t('梱包画面: 配賦された伝票に 🕒 バッジ (kind=later)', () => {
  const st = getWorkState(packA);
  const bySeq = new Map(st.slips.map((s) => [s.seq, s.pickingShortages]));
  assert.equal(bySeq.get(1).length, 0, '無傷の伝票には出ない');
  assert.deepEqual(core(bySeq.get(2)), [{ sku: 'testsku', qty: 1, kind: 'later' }]);
  assert.deepEqual(core(bySeq.get(3)), [{ sku: 'testsku', qty: 1, kind: 'later' }]);
});

// ═══ ③ back: 未着手なら取り下げ・対応中なら拒否 ═══════════════════════════
console.log('── back (取り下げと拒否) ──');
t('back: 依頼取消・タスク取消・伝票の保留解除・配賦削除', () => {
  ev(pkA, 'back', { lineSeq: 1, undoOpId: shortOp });
  assert.equal(db.prepare('SELECT status FROM pk_later_requests WHERE batch_id=?').get(pkA).status, 'cancelled');
  const open = db.prepare("SELECT COUNT(*) c FROM pk_pack_tasks WHERE batch_id=? AND status IN ('requested','claimed','fulfilled')").get(packA).c;
  assert.equal(open, 0, 'タスクは全部 cancelled');
  const held = db.prepare("SELECT COUNT(*) c FROM pk_pack_slips WHERE batch_id=? AND status='held'").get(packA).c;
  assert.equal(held, 0, '保留も解除');
  assert.equal(listShortageAllocations(pkA, 1).length, 0, '配賦も消える');
  assert.equal(getWorkState(packA).slips.every((s) => s.pickingShortages.length === 0), true, 'バッジも消える');
});

t('やり直しの欠品で配賦・依頼が作り直される (二重にならない)', () => {
  applyEvent(pkA, { opId: `t${++op}`, event: 'shortage', lineSeq: 1, shortageQty: 2, altQty: 0, remaining: 'later' }, '星立夏');
  assert.equal(listShortageAllocations(pkA, 1).length, 2);
  const lrs = db.prepare("SELECT status, COUNT(*) c FROM pk_later_requests WHERE batch_id=? GROUP BY status").all(pkA);
  assert.deepEqual(Object.fromEntries(lrs.map((x) => [x.status, x.c])), { cancelled: 1, pending_binding: 1 });
  bindPendingLaterRequests();
});

throwsCode(() => {
  // ピッカーが既に取りに行き始めた (claimed) → back は拒否
  const lr = db.prepare("SELECT id FROM pk_later_requests WHERE batch_id=? AND status='requested'").get(pkA);
  db.prepare("UPDATE pk_pack_tasks SET status='claimed', claimed_by='星立夏' WHERE later_request_id=? AND slip_seq=2").run(lr.id);
  const lastOp = db.prepare("SELECT op_id FROM pk_events WHERE batch_id=? AND event='shortage' ORDER BY id DESC LIMIT 1").get(pkA).op_id;
  ev(pkA, 'back', { lineSeq: 1, undoOpId: lastOp });
}, 'later_in_progress', '対応が始まった later は back できない');

// ═══ ④ どこにもない: バッジだけ・タスクも保留も作らない ══════════════════
console.log('── どこにもない (バッジのみ) ──');
const pkB = mkPickBatch('TB-B', { lineQty: 1, slipQtys: [1] });
const packB = mkPackBatch('TB-B', { slipQtys: [1] });
ev(pkB, 'start', {}, '西川カナコ');   // shortage と同じ作業者で開始 (worker不一致は409)
applyEvent(pkB, { opId: `t${++op}`, event: 'shortage', lineSeq: 1, shortageQty: 1, altQty: 0, remaining: 'none' }, '西川カナコ');

t('none: 配賦は kind=none・依頼なし・展開なし', () => {
  const a = listShortageAllocations(pkB, 1);
  assert.deepEqual(a, [{ ne_slip_no: 'TB-B-NE1', qty: 1, kind: 'none' }]);
  assert.equal(db.prepare('SELECT COUNT(*) c FROM pk_later_requests WHERE batch_id=?').get(pkB).c, 0);
  assert.equal(bindPendingLaterRequests(), 0);
  assert.equal(db.prepare("SELECT COUNT(*) c FROM pk_pack_tasks WHERE batch_id=?").get(packB).c, 0, 'タスクなし');
});

t('none: 梱包画面は ❌ バッジ・伝票は保留にしない', () => {
  const st = getWorkState(packB);
  assert.deepEqual(core(st.slips[0].pickingShortages), [{ sku: 'testsku', qty: 1, kind: 'none' }]);
  assert.equal(st.slips[0].status, 'pending', '保留にはしない (表示のみ — 要件§4.4)');
});

// ═══ 部分欠品 + 他ロケ一部確保 ═══════════════════════════════════════════
console.log('── 一部を他ロケで確保 + 残り後で ──');
const pkC = mkPickBatch('TB-C', { lineQty: 3, slipQtys: [3] });
ev(pkC, 'start');
applyEvent(pkC, {
  opId: `t${++op}`, event: 'shortage', lineSeq: 1, shortageQty: 3,
  altBlock: 'P3FB', altLocation: '002-002-02', altQty: 2, remaining: 'later',
}, '星立夏');

t('配賦は残り (3不足−2確保=1個) だけ', () => {
  const a = listShortageAllocations(pkC, 1);
  assert.deepEqual(a, [{ ne_slip_no: 'TB-C-NE1', qty: 1, kind: 'later' }]);
  assert.equal(db.prepare('SELECT qty FROM pk_later_requests WHERE batch_id=?').get(pkC).qty, 1);
});

t('全量を他ロケで確保したら配賦も依頼も作らない', () => {
  const pkD = mkPickBatch('TB-D', { lineQty: 2, slipQtys: [2] });
  ev(pkD, 'start');
  applyEvent(pkD, {
    opId: `t${++op}`, event: 'shortage', lineSeq: 1, shortageQty: 2,
    altBlock: 'P3FB', altLocation: '002-002-02', altQty: 2, remaining: null,
  }, '星立夏');
  assert.equal(listShortageAllocations(pkD, 1).length, 0);
  assert.equal(db.prepare('SELECT COUNT(*) c FROM pk_later_requests WHERE batch_id=?').get(pkD).c, 0);
});

// ═══ Codex High1: 同一SKUが複数ロケにある場合の配賦 ════════════════════════
console.log('── 同一SKU × 複数ロケ (二重配賦の防止) ──');
const pkE = mkPickBatch('TB-E', { lineQty: 1, slipQtys: [1, 1] });
db.prepare(`INSERT INTO pk_lines (batch_id, seq, location, block, sku, product_name, barcode, qty)
  VALUES (?, 2, '003-003-03', 'P3FB', 'testsku', 'テスト商品', NULL, 1)`).run(pkE);
db.prepare('UPDATE pk_batches SET line_count=2, total_qty=2 WHERE id=?').run(pkE);
ev(pkE, 'start');
applyEvent(pkE, { opId: `t${++op}`, event: 'shortage', lineSeq: 1, shortageQty: 1, altQty: 0, remaining: 'none' }, '星立夏');
applyEvent(pkE, { opId: `t${++op}`, event: 'shortage', lineSeq: 2, shortageQty: 1, altQty: 0, remaining: 'none' }, '星立夏');
t('別ロケの欠品は別の受注へ配賦 (同じ受注に「注文1個に欠品2個」を作らない)', () => {
  assert.deepEqual(listShortageAllocations(pkE, 1).map((x) => x.ne_slip_no), ['TB-E-NE2'], '1本目は後ろの受注');
  assert.deepEqual(listShortageAllocations(pkE, 2).map((x) => x.ne_slip_no), ['TB-E-NE1'], '2本目は残った受注');
});

// ═══ Codex High2: bind より先に梱包が完了していた ═══════════════════════════
console.log('── bind より先に梱包が完了していた場合 ──');
const pkF = mkPickBatch('TB-F', { lineQty: 1, slipQtys: [1] });
const packF = mkPackBatch('TB-F', { slipQtys: [1] });
ev(pkF, 'start');
db.prepare("UPDATE pk_pack_slips SET status='done', done_at=? WHERE batch_id=?").run(now, packF);
applyEvent(pkF, { opId: `t${++op}`, event: 'shortage', lineSeq: 1, shortageQty: 1, altQty: 0, remaining: 'later' }, '星立夏');
t('完了済みの伝票も held に戻す (商品が無いまま出荷させない)', () => {
  assert.equal(bindPendingLaterRequests(), 1);
  const sl = db.prepare('SELECT status, hold_reason, done_at FROM pk_pack_slips WHERE batch_id=?').get(packF);
  assert.deepEqual(sl, { status: 'held', hold_reason: 'repick', done_at: null });
  assert.equal(db.prepare('SELECT slip_seq FROM pk_pack_tasks WHERE batch_id=?').get(packF).slip_seq, 1,
    '伝票なしタスクに落とさず、その伝票に紐づける');
});

// ═══ Codex High3: 再取込 (overwrite) 前のリセット ═════════════════════════
console.log('── 再取込 (overwrite) 前のリセット ──');
t('展開済みタスクを取消し、依頼を pending_binding へ戻す → 新しい伝票へ展開し直せる', () => {
  assert.equal(resetLaterBindingsForPackBatch(db, packF), 1);
  assert.equal(db.prepare('SELECT status FROM pk_pack_tasks WHERE batch_id=?').get(packF).status, 'cancelled');
  assert.equal(db.prepare('SELECT status FROM pk_later_requests WHERE batch_id=?').get(pkF).status, 'pending_binding');
  // 再取込後 (伝票が作り直されて pending) を模す
  db.prepare("UPDATE pk_pack_slips SET status='pending', hold_reason=NULL, done_at=NULL WHERE batch_id=?").run(packF);
  assert.equal(bindPendingLaterRequests(), 1, '新しい伝票に対して展開し直す');
  assert.equal(db.prepare("SELECT COUNT(*) c FROM pk_pack_tasks WHERE batch_id=? AND status='requested'").get(packF).c, 1);
});
throwsCode(() => {
  const lr = db.prepare("SELECT id FROM pk_later_requests WHERE batch_id=? AND status='requested'").get(pkF);
  db.prepare("UPDATE pk_pack_tasks SET status='claimed', claimed_by='星立夏' WHERE later_request_id=? AND status='requested'").run(lr.id);
  resetLaterBindingsForPackBatch(db, packF);
}, 'later_in_progress', '対応が始まっていたら再取込を止める');
t('リセット拒否のとき何も変わっていない (トランザクション外でも副作用なし)', () => {
  assert.equal(db.prepare("SELECT COUNT(*) c FROM pk_pack_tasks WHERE batch_id=? AND status='claimed'").get(packF).c, 1);
  assert.equal(db.prepare('SELECT status FROM pk_later_requests WHERE batch_id=?').get(pkF).status, 'requested');
});

// ═══ Codex Medium: 梱包側が先に出した再ピックに合流する ═══════════════════
console.log('── 梱包側の再ピックが既にある伝票 (合流・二重ピック防止) ──');
const pkG = mkPickBatch('TB-G', { lineQty: 1, slipQtys: [1] });
const packG = mkPackBatch('TB-G', { slipQtys: [1] });
db.prepare(`INSERT INTO pk_pack_tasks (batch_id, slip_seq, kind, sku, product_name, req_qty,
  location, block, folder_name, status, requested_by, created_at, updated_at)
  VALUES (?, 1, 'repick', 'testsku', 'テスト商品', 1, NULL, NULL, '出荷_88', 'requested', '大場江莉果', ?, ?)`)
  .run(packG, now, now);
db.prepare("UPDATE pk_pack_slips SET status='held', hold_reason='repick' WHERE batch_id=?").run(packG);
ev(pkG, 'start');
applyEvent(pkG, { opId: `t${++op}`, event: 'shortage', lineSeq: 1, shortageQty: 1, altQty: 0, remaining: 'later' }, '星立夏');
t('同じ伝票×SKUの生きた再ピックがあれば2本目を作らない (1行バッチが2本=二度取りに行くのを防ぐ)', () => {
  bindPendingLaterRequests();
  assert.equal(db.prepare("SELECT COUNT(*) c FROM pk_pack_tasks WHERE batch_id=? AND status='requested'").get(packG).c, 1);
  assert.equal(db.prepare('SELECT status FROM pk_later_requests WHERE batch_id=?').get(pkG).status, 'requested', '合流済みとして扱う');
});
t('合流した後の back は、梱包側の依頼を巻き込まない', () => {
  const lastOp = db.prepare("SELECT op_id FROM pk_events WHERE batch_id=? AND event='shortage' ORDER BY id DESC LIMIT 1").get(pkG).op_id;
  ev(pkG, 'back', { lineSeq: 1, undoOpId: lastOp });
  const tk = db.prepare('SELECT status, requested_by FROM pk_pack_tasks WHERE batch_id=?').get(packG);
  assert.deepEqual(tk, { status: 'requested', requested_by: '大場江莉果' }, '梱包側のタスクはそのまま');
  assert.equal(db.prepare('SELECT status FROM pk_pack_slips WHERE batch_id=?').get(packG).status, 'held', '保留も維持');
});

// ═══ Codex R2: 別理由の保留を上書きしない ═════════════════════════════════
console.log('── 配送方法変更で保留中の伝票 ──');
const pkH = mkPickBatch('TB-H', { lineQty: 1, slipQtys: [1] });
const packH = mkPackBatch('TB-H', { slipQtys: [1] });
db.prepare("UPDATE pk_pack_slips SET status='held', hold_reason='shipping_change' WHERE batch_id=?").run(packH);
ev(pkH, 'start');
applyEvent(pkH, { opId: `t${++op}`, event: 'shortage', lineSeq: 1, shortageQty: 1, altQty: 0, remaining: 'later' }, '星立夏');
t('shipping_change の保留は repick で上書きしない (伝票なしタスクで取りには行く)', () => {
  assert.equal(bindPendingLaterRequests(), 1);
  const sl = db.prepare('SELECT status, hold_reason FROM pk_pack_slips WHERE batch_id=?').get(packH);
  assert.deepEqual(sl, { status: 'held', hold_reason: 'shipping_change' }, '元の保留理由が残る');
  const tk = db.prepare('SELECT slip_seq, req_qty FROM pk_pack_tasks WHERE batch_id=?').get(packH);
  assert.deepEqual(tk, { slip_seq: null, req_qty: 1 }, '伝票なしタスク');
});

// ═══ Codex R2: 受領後は back できない ═══════════════════════════════════════
console.log('── 梱包者が受け取った後の back ──');
const pkI = mkPickBatch('TB-I', { lineQty: 1, slipQtys: [1] });
const packI = mkPackBatch('TB-I', { slipQtys: [1] });
ev(pkI, 'start');
const opI = `t${++op}`;
applyEvent(pkI, { opId: opI, event: 'shortage', lineSeq: 1, shortageQty: 1, altQty: 0, remaining: 'later' }, '星立夏');
bindPendingLaterRequests();
throwsCode(() => {
  db.prepare("UPDATE pk_pack_tasks SET status='received', claimed_by='星立夏' WHERE batch_id=?").run(packI);
  ev(pkI, 'back', { lineSeq: 1, undoOpId: opI });
}, 'later_in_progress', 'received (受け取った後) は back できない');
throwsCode(() => {
  db.prepare("UPDATE pk_pack_tasks SET status='unavailable' WHERE batch_id=?").run(packI);
  ev(pkI, 'back', { lineSeq: 1, undoOpId: opI });
}, 'later_in_progress', 'unavailable (在庫なし報告後) も back できない');

// ═══ Codex R2: 同一受注に同じSKUの行が複数 ══════════════════════════════════
console.log('── 同一受注に同じSKUの明細が2行 ──');
const pkJ = mkPickBatch('TB-J', { lineQty: 1, slipQtys: [1] });
// 同じ受注 NE1 に同じSKUがもう1行 (計2個)。ロケ2本目の明細も用意
db.prepare(`INSERT INTO pk_slip_lines (batch_id, slip_no, picking_no, ne_slip_no, sku, qty, location)
  VALUES (?, 'SPTB-J-1', NULL, 'TB-J-NE1', 'testsku', 1, '003-003-03')`).run(pkJ);
db.prepare(`INSERT INTO pk_lines (batch_id, seq, location, block, sku, product_name, barcode, qty)
  VALUES (?, 2, '003-003-03', 'P3FB', 'testsku', 'テスト商品', NULL, 1)`).run(pkJ);
db.prepare('UPDATE pk_batches SET line_count=2, total_qty=2 WHERE id=?').run(pkJ);
ev(pkJ, 'start');
applyEvent(pkJ, { opId: `t${++op}`, event: 'shortage', lineSeq: 1, shortageQty: 1, altQty: 0, remaining: 'none' }, '星立夏');
applyEvent(pkJ, { opId: `t${++op}`, event: 'shortage', lineSeq: 2, shortageQty: 1, altQty: 0, remaining: 'none' }, '星立夏');
t('受注単位で集約して控除 (2個注文の受注に 1+1 で2個配賦できる)', () => {
  assert.deepEqual(listShortageAllocations(pkJ, 1), [{ ne_slip_no: 'TB-J-NE1', qty: 1, kind: 'none' }]);
  assert.deepEqual(listShortageAllocations(pkJ, 2), [{ ne_slip_no: 'TB-J-NE1', qty: 1, kind: 'none' }],
    '2本目も同じ受注の残り1個へ (行ごとに控除が重複すると0になる)');
});

// ═══ Codex R3: 合流は数量で判断 ═════════════════════════════════════════════
console.log('── 梱包側の再ピック (1個) があるところへ 3個の「後で」 ──');
const pkK = mkPickBatch('TB-K', { lineQty: 3, slipQtys: [3] });
const packK = mkPackBatch('TB-K', { slipQtys: [3] });
db.prepare(`INSERT INTO pk_pack_tasks (batch_id, slip_seq, kind, sku, product_name, req_qty,
  location, block, folder_name, status, requested_by, created_at, updated_at)
  VALUES (?, 1, 'repick', 'testsku', 'テスト商品', 1, NULL, NULL, '出荷_88', 'requested', '大場江莉果', ?, ?)`)
  .run(packK, now, now);
db.prepare("UPDATE pk_pack_slips SET status='held', hold_reason='repick' WHERE batch_id=?").run(packK);
ev(pkK, 'start');
applyEvent(pkK, { opId: `t${++op}`, event: 'shortage', lineSeq: 1, shortageQty: 3, altQty: 0, remaining: 'later' }, '星立夏');
t('既存1個に対し不足3個 → 差分2個のタスクだけ追加 (1個で伝票を解除できないように)', () => {
  bindPendingLaterRequests();
  const rows = db.prepare("SELECT req_qty, later_request_id FROM pk_pack_tasks WHERE batch_id=? AND status='requested' ORDER BY id").all(packK);
  assert.deepEqual(rows.map((r) => r.req_qty), [1, 2]);
  assert.equal(rows[1].later_request_id != null, true, '追加分はピッカー依頼由来として記録');
});

// ═══ Codex R3: unavailable のまま再取込しない ═══════════════════════════════
console.log('── 在庫なし報告 (unavailable) 後の再取込 ──');
const pkL = mkPickBatch('TB-L', { lineQty: 1, slipQtys: [1] });
const packL = mkPackBatch('TB-L', { slipQtys: [1] });
ev(pkL, 'start');
applyEvent(pkL, { opId: `t${++op}`, event: 'shortage', lineSeq: 1, shortageQty: 1, altQty: 0, remaining: 'later' }, '星立夏');
bindPendingLaterRequests();
throwsCode(() => {
  db.prepare("UPDATE pk_pack_tasks SET status='unavailable' WHERE batch_id=?").run(packL);
  resetLaterBindingsForPackBatch(db, packL);
}, 'later_in_progress', 'unavailable のタスクが残るバッチは再取込を止める');

// ═══ Codex R4: 別ロケ由来の2つの依頼が同じ伝票へ配賦 ════════════════════════
console.log('── 同じ伝票×SKUに、別ロケ由来の「後で」が2件 ──');
const pkM = mkPickBatch('TB-M', { lineQty: 1, slipQtys: [2] });   // 受注1件に2個
db.prepare(`INSERT INTO pk_lines (batch_id, seq, location, block, sku, product_name, barcode, qty)
  VALUES (?, 2, '003-003-03', 'P3FB', 'testsku', 'テスト商品', NULL, 1)`).run(pkM);
db.prepare('UPDATE pk_batches SET line_count=2, total_qty=2 WHERE id=?').run(pkM);
const packM = mkPackBatch('TB-M', { slipQtys: [2] });
ev(pkM, 'start');
applyEvent(pkM, { opId: `t${++op}`, event: 'shortage', lineSeq: 1, shortageQty: 1, altQty: 0, remaining: 'later' }, '星立夏');
const opM2 = `t${++op}`;
applyEvent(pkM, { opId: opM2, event: 'shortage', lineSeq: 2, shortageQty: 1, altQty: 0, remaining: 'later' }, '星立夏');
const openQty = () => db.prepare(`SELECT COALESCE(SUM(req_qty),0) q FROM pk_pack_tasks
  WHERE batch_id=? AND slip_seq=1 AND status IN ('requested','claimed','fulfilled')`).get(packM).q;
t('2件の依頼ぶん (合計2個) のタスクが作られる (2件目の数量が落ちない)', () => {
  assert.equal(bindPendingLaterRequests(), 2);
  assert.equal(openQty(), 2);
  const st = db.prepare("SELECT status, COUNT(*) c FROM pk_later_requests WHERE batch_id=? GROUP BY status").all(pkM);
  assert.deepEqual(st, [{ status: 'requested', c: 2 }]);
});
t('タスクは依頼と1対1 (依頼ごとに自分の配賦ぶん)', () => {
  const rows = db.prepare(`SELECT later_request_id, req_qty FROM pk_pack_tasks
    WHERE batch_id=? AND status='requested' ORDER BY id`).all(packM);
  assert.equal(rows.length, 2);
  assert.ok(rows[0].later_request_id !== rows[1].later_request_id, '出自が別');
  assert.deepEqual(rows.map((r) => r.req_qty), [1, 1]);
});
throwsCode(() => {
  // 依頼1のタスクを着手 → 依頼1の back は拒否 (依頼2には影響しない)
  const lr1 = db.prepare('SELECT id FROM pk_later_requests WHERE batch_id=? AND line_seq=1').get(pkM);
  db.prepare("UPDATE pk_pack_tasks SET status='claimed', claimed_by='星立夏' WHERE later_request_id=?").run(lr1.id);
  const op1 = db.prepare("SELECT op_id FROM pk_events WHERE batch_id=? AND event='shortage' AND line_seq=1").get(pkM).op_id;
  // back は「処理済みの中で最大の seq」しか戻せないので、まず line2 を戻してから line1 を試す
  ev(pkM, 'back', { lineSeq: 2, undoOpId: opM2 });
  ev(pkM, 'back', { lineSeq: 1, undoOpId: op1 });
}, 'later_in_progress', '着手済みの依頼は back できない (兄弟の back に巻き込まれない)');
t('着手していない兄弟 (依頼2) の back は通り、依頼1のタスクはそのまま', () => {
  assert.equal(db.prepare('SELECT status FROM pk_later_requests WHERE batch_id=? AND line_seq=2').get(pkM).status, 'cancelled');
  assert.equal(db.prepare('SELECT status FROM pk_later_requests WHERE batch_id=? AND line_seq=1').get(pkM).status, 'requested');
  assert.equal(openQty(), 1, '依頼1の1個だけが生きている (数量を落とさない・二重にしない)');
});

// ═══ Codex R5: 受領済みの数量を再度タスク化しない ═══════════════════════════
console.log('── 受領済みのあと、別ロケの明細で同じ受注へ「後で」 ──');
const pkN = mkPickBatch('TB-N', { lineQty: 1, slipQtys: [2] });
db.prepare(`INSERT INTO pk_lines (batch_id, seq, location, block, sku, product_name, barcode, qty)
  VALUES (?, 2, '003-003-03', 'P3FB', 'testsku', 'テスト商品', NULL, 1)`).run(pkN);
db.prepare('UPDATE pk_batches SET line_count=2, total_qty=2 WHERE id=?').run(pkN);
const packN = mkPackBatch('TB-N', { slipQtys: [2] });
ev(pkN, 'start');
applyEvent(pkN, { opId: `t${++op}`, event: 'shortage', lineSeq: 1, shortageQty: 1, altQty: 0, remaining: 'later' }, '星立夏');
bindPendingLaterRequests();
// 1個目は取りに行って梱包者が受け取った
db.prepare("UPDATE pk_pack_tasks SET status='received' WHERE batch_id=?").run(packN);
db.prepare("UPDATE pk_pack_slips SET status='pending', hold_reason=NULL WHERE batch_id=?").run(packN);
applyEvent(pkN, { opId: `t${++op}`, event: 'shortage', lineSeq: 2, shortageQty: 1, altQty: 0, remaining: 'later' }, '星立夏');
t('2本目の依頼は自分の1個だけ (受領済みの1個を二度取りに行かない)', () => {
  assert.equal(bindPendingLaterRequests(), 1);
  const open = db.prepare(`SELECT req_qty FROM pk_pack_tasks WHERE batch_id=? AND status='requested'`).all(packN);
  assert.deepEqual(open.map((r) => r.req_qty), [1]);
});


// ═══ Codex R6: 梱包側タスクに全量吸収された1件目のあとの2件目 ═══════════════
console.log('── 梱包側1個 + 別ロケ由来の「後で」1個×2件 ──');
const pkO = mkPickBatch('TB-O', { lineQty: 1, slipQtys: [2] });
db.prepare(`INSERT INTO pk_lines (batch_id, seq, location, block, sku, product_name, barcode, qty)
  VALUES (?, 2, '003-003-03', 'P3FB', 'testsku', 'テスト商品', NULL, 1)`).run(pkO);
db.prepare('UPDATE pk_batches SET line_count=2, total_qty=2 WHERE id=?').run(pkO);
const packO = mkPackBatch('TB-O', { slipQtys: [2] });
db.prepare(`INSERT INTO pk_pack_tasks (batch_id, slip_seq, kind, sku, product_name, req_qty,
  location, block, folder_name, status, requested_by, created_at, updated_at)
  VALUES (?, 1, 'repick', 'testsku', 'テスト商品', 1, NULL, NULL, '出荷_88', 'requested', '大場江莉果', ?, ?)`)
  .run(packO, now, now);
db.prepare("UPDATE pk_pack_slips SET status='held', hold_reason='repick' WHERE batch_id=?").run(packO);
ev(pkO, 'start');
applyEvent(pkO, { opId: `t${++op}`, event: 'shortage', lineSeq: 1, shortageQty: 1, altQty: 0, remaining: 'later' }, '星立夏');
applyEvent(pkO, { opId: `t${++op}`, event: 'shortage', lineSeq: 2, shortageQty: 1, altQty: 0, remaining: 'later' }, '星立夏');
t('1件目は梱包側に全量合流 (タスク0本) でも、2件目は自分の1個を作る → 合計2個', () => {
  assert.equal(bindPendingLaterRequests(), 2);
  const total = db.prepare(`SELECT COALESCE(SUM(req_qty),0) q FROM pk_pack_tasks
    WHERE batch_id=? AND slip_seq=1 AND status='requested'`).get(packO).q;
  assert.equal(total, 2, '梱包側1 + 依頼2の1 (依頼1は合流で0)');
  const mine = db.prepare(`SELECT COUNT(*) c FROM pk_pack_tasks WHERE batch_id=? AND later_request_id IS NOT NULL`).get(packO).c;
  assert.equal(mine, 1, 'ピッカー依頼由来のタスクは1本だけ (二重に差し引いて0本にならない)');
});

// ═══ Codex R7: 全量合流した先が着手済みなら back できない ═══════════════════
console.log('── 梱包側の再ピックへ全量合流 → 合流先が着手済み ──');
const pkP = mkPickBatch('TB-P', { lineQty: 1, slipQtys: [1] });
const packP = mkPackBatch('TB-P', { slipQtys: [1] });
const packerTaskP = Number(db.prepare(`INSERT INTO pk_pack_tasks (batch_id, slip_seq, kind, sku, product_name, req_qty,
  location, block, folder_name, status, requested_by, created_at, updated_at)
  VALUES (?, 1, 'repick', 'testsku', 'テスト商品', 1, NULL, NULL, '出荷_88', 'requested', '大場江莉果', ?, ?)`)
  .run(packP, now, now).lastInsertRowid);
db.prepare("UPDATE pk_pack_slips SET status='held', hold_reason='repick' WHERE batch_id=?").run(packP);
ev(pkP, 'start');
const opP = `t${++op}`;
applyEvent(pkP, { opId: opP, event: 'shortage', lineSeq: 1, shortageQty: 1, altQty: 0, remaining: 'later' }, '星立夏');
t('全量合流: 自前のタスクは作らず、合流先の id を依頼に記録する', () => {
  bindPendingLaterRequests();
  const lr = db.prepare('SELECT * FROM pk_later_requests WHERE batch_id=?').get(pkP);
  assert.equal(lr.status, 'requested');
  assert.equal(lr.merged_task_ids, String(packerTaskP));
  assert.equal(db.prepare('SELECT COUNT(*) c FROM pk_pack_tasks WHERE later_request_id=?').get(lr.id).c, 0);
});
throwsCode(() => {
  db.prepare("UPDATE pk_pack_tasks SET status='claimed', claimed_by='大場江莉果' WHERE id=?").run(packerTaskP);
  ev(pkP, 'back', { lineSeq: 1, undoOpId: opP });
}, 'later_in_progress', '合流先 (梱包側の再ピック) が着手済みなら back できない');
t('合流先が未着手なら back できる (合流先のタスクは触らない)', () => {
  db.prepare("UPDATE pk_pack_tasks SET status='requested', claimed_by=NULL WHERE id=?").run(packerTaskP);
  ev(pkP, 'back', { lineSeq: 1, undoOpId: opP });
  assert.equal(db.prepare('SELECT status FROM pk_later_requests WHERE batch_id=?').get(pkP).status, 'cancelled');
  assert.equal(db.prepare('SELECT status FROM pk_pack_tasks WHERE id=?').get(packerTaskP).status, 'requested', '梱包側のタスクはそのまま');
});

// ═══ Codex R8: 全量合流した依頼のある梱包バッチの再取込 ═══════════════════
console.log('── 全量合流した依頼がある梱包バッチの再取込 ──');
const pkQ = mkPickBatch('TB-Q', { lineQty: 1, slipQtys: [1] });
const packQ = mkPackBatch('TB-Q', { slipQtys: [1] });
const packerTaskQ = Number(db.prepare(`INSERT INTO pk_pack_tasks (batch_id, slip_seq, kind, sku, product_name, req_qty,
  location, block, folder_name, status, requested_by, created_at, updated_at)
  VALUES (?, 1, 'repick', 'testsku', 'テスト商品', 1, NULL, NULL, '出荷_88', 'requested', '大場江莉果', ?, ?)`)
  .run(packQ, now, now).lastInsertRowid);
db.prepare("UPDATE pk_pack_slips SET status='held', hold_reason='repick' WHERE batch_id=?").run(packQ);
ev(pkQ, 'start');
applyEvent(pkQ, { opId: `t${++op}`, event: 'shortage', lineSeq: 1, shortageQty: 1, altQty: 0, remaining: 'later' }, '星立夏');
bindPendingLaterRequests();
throwsCode(() => {
  db.prepare("UPDATE pk_pack_tasks SET status='claimed', claimed_by='大場江莉果' WHERE id=?").run(packerTaskQ);
  resetLaterBindingsForPackBatch(db, packQ);
}, 'later_in_progress', '合流先が着手済みなら再取込を止める (自前タスクが0本でも)');
t('合流先が未着手なら、依頼を pending_binding へ戻し合流の記録を消す', () => {
  db.prepare("UPDATE pk_pack_tasks SET status='requested', claimed_by=NULL WHERE id=?").run(packerTaskQ);
  assert.equal(resetLaterBindingsForPackBatch(db, packQ), 1);
  const lr = db.prepare('SELECT status, merged_task_ids FROM pk_later_requests WHERE batch_id=?').get(pkQ);
  assert.deepEqual(lr, { status: 'pending_binding', merged_task_ids: null });
  assert.equal(db.prepare('SELECT status FROM pk_pack_tasks WHERE id=?').get(packerTaskQ).status, 'requested', '梱包側のタスクは触らない');
});

// ═══ Codex R9: 合流先の記録は数量ぶんだけ (requested 優先) ══════════════════
console.log('── 梱包側に requested 1個 + claimed 10個 があるところへ「後で」1個 ──');
const pkR = mkPickBatch('TB-R', { lineQty: 1, slipQtys: [11] });
const packR = mkPackBatch('TB-R', { slipQtys: [11] });
const insR = db.prepare(`INSERT INTO pk_pack_tasks (batch_id, slip_seq, kind, sku, product_name, req_qty,
  location, block, folder_name, status, requested_by, claimed_by, created_at, updated_at)
  VALUES (?, 1, 'repick', 'testsku', 'テスト商品', ?, NULL, NULL, '出荷_88', ?, '大場江莉果', ?, ?, ?)`);
const claimedR = Number(insR.run(packR, 10, 'claimed', '西川カナコ', now, now).lastInsertRowid);
const requestedR = Number(insR.run(packR, 1, 'requested', null, now, now).lastInsertRowid);
db.prepare("UPDATE pk_pack_slips SET status='held', hold_reason='repick' WHERE batch_id=?").run(packR);
ev(pkR, 'start');
const opR = `t${++op}`;
applyEvent(pkR, { opId: opR, event: 'shortage', lineSeq: 1, shortageQty: 1, altQty: 0, remaining: 'later' }, '星立夏');
t('合流先として記録するのは requested の1件だけ (claimed 10個は賄いに不要)', () => {
  bindPendingLaterRequests();
  const lr = db.prepare('SELECT merged_task_ids FROM pk_later_requests WHERE batch_id=?').get(pkR);
  assert.equal(lr.merged_task_ids, String(requestedR));
  assert.notEqual(lr.merged_task_ids, String(claimedR));
});
t('→ back は通る (着手済みの無関係なタスクに引きずられて 409 にならない)', () => {
  ev(pkR, 'back', { lineSeq: 1, undoOpId: opR });
  assert.equal(db.prepare('SELECT status FROM pk_later_requests WHERE batch_id=?').get(pkR).status, 'cancelled');
});

// ═══ Codex R10: バッチ取消 (cancel) の後始末 ═══════════════════════════════
console.log('── バッチ取消 (cancel) ──');
const pkS = mkPickBatch('TB-S', { lineQty: 1, slipQtys: [1] });
const packS = mkPackBatch('TB-S', { slipQtys: [1] });
// 1明細だけだと欠品でバッチが done になり cancel の対象外 (作業中のみ) なので、2明細目を足す
const addSecondLine = (batchId) => db.prepare(`INSERT INTO pk_lines (batch_id, seq, location, block, sku, product_name, barcode, qty)
  VALUES (?, 2, '001-002-01', 'P3FA', 'othersku', '別商品', NULL, 1)`).run(batchId);
addSecondLine(pkS);
ev(pkS, 'start');
applyEvent(pkS, { opId: `t${++op}`, event: 'shortage', lineSeq: 1, shortageQty: 1, altQty: 0, remaining: 'later' }, '星立夏');
bindPendingLaterRequests();
t('cancel: 配賦・依頼・タスク・梱包側の保留がすべて片づく', () => {
  ev(pkS, 'cancel');
  assert.equal(listShortageAllocations(pkS, 1).length, 0, '配賦');
  assert.equal(db.prepare('SELECT status FROM pk_later_requests WHERE batch_id=?').get(pkS).status, 'cancelled', '依頼');
  assert.equal(db.prepare('SELECT status FROM pk_pack_tasks WHERE batch_id=?').get(packS).status, 'cancelled', 'タスク');
  assert.equal(db.prepare('SELECT status FROM pk_pack_slips WHERE batch_id=?').get(packS).status, 'pending', '保留解除');
  assert.equal(getWorkState(packS).slips[0].pickingShortages.length, 0, 'バッジも消える');
  assert.equal(db.prepare('SELECT status FROM pk_batches WHERE id=?').get(pkS).status, 'ready');
});

const pkT = mkPickBatch('TB-T', { lineQty: 1, slipQtys: [1] });
const packT = mkPackBatch('TB-T', { slipQtys: [1] });
addSecondLine(pkT);
ev(pkT, 'start');
applyEvent(pkT, { opId: `t${++op}`, event: 'shortage', lineSeq: 1, shortageQty: 1, altQty: 0, remaining: 'later' }, '星立夏');
bindPendingLaterRequests();
throwsCode(() => {
  db.prepare("UPDATE pk_pack_tasks SET status='claimed', claimed_by='星立夏' WHERE batch_id=?").run(packT);
  ev(pkT, 'cancel');
}, 'later_in_progress', '着手済みの「後で」があるバッチは取消できない');
t('拒否された cancel は何も変えない (バッチは作業中のまま・保留も維持)', () => {
  assert.equal(db.prepare('SELECT status FROM pk_batches WHERE id=?').get(pkT).status, 'picking');
  assert.equal(db.prepare('SELECT status FROM pk_pack_slips WHERE batch_id=?').get(packT).status, 'held');
  assert.equal(db.prepare('SELECT status FROM pk_later_requests WHERE batch_id=?').get(pkT).status, 'requested');
});

console.log(`\n${passed} tests passed`);
try { fs.rmSync(process.env.DATA_DIR, { recursive: true, force: true }); } catch { /* 無視 */ }
