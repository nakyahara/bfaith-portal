/**
 * picking PR2 — 作業イベント (start/next/back) の状態機械・冪等・排他のテスト。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import iconv from 'iconv-lite';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'picking-test-'));

const { parseCs03002, importBatch, applyEvent, getWorkState, getDailySummary, PkError } = await import('../service.js');
const { initPickingDB, getDB } = await import('../db.js');

initPickingDB();

const HEADERS = [
  '出荷指示日', 'ブロック略称', 'ロケーション', '商品ID', '商品名', '出荷指示数', '出荷引当数',
  'ピッキングNO', '出荷伝票NO', '荷主出荷NO', 'バーコード',
  '送り状発行ソフト名', '配送方法名', 'トータルピッキングバッチ番号',
];
function makeCsv(rows) {
  const q = (v) => `"${String(v ?? '').replace(/"/g, '""')}"`;
  const lines = [HEADERS.map(q).join(',')];
  for (const r of rows) lines.push(HEADERS.map((h) => q(r[h] ?? '')).join(','));
  return iconv.encode(lines.join('\r\n') + '\r\n', 'Shift_JIS');
}
function row({ loc, sku, qty = 1, slip, tb }) {
  return {
    '出荷指示日': '20260811', 'ブロック略称': 'P3FB', 'ロケーション': loc,
    '商品ID': sku, '商品名': `商品${sku}_梱機プ`, '出荷指示数': String(qty), '出荷引当数': String(qty),
    'ピッキングNO': `PC${slip}`, '出荷伝票NO': `SP${slip}`, '荷主出荷NO': slip,
    'バーコード': 'X1', '送り状発行ソフト名': 'B2(Ver6.0)',
    '配送方法名': 'ネコポス 陸便 元払い 営業所止めなし', 'トータルピッキングバッチ番号': tb,
  };
}

let tbSeq = 0;
/** 3明細 (ロケ順: 00201604/a, 00400304/b, 00700803/c) のバッチを作る。 */
function makeBatch() {
  const tb = `TBW${String(++tbSeq).padStart(4, '0')}`;
  const csv = makeCsv([
    row({ loc: '00700803', sku: 'c', qty: 3, slip: '0001', tb }),
    row({ loc: '00201604', sku: 'a', qty: 1, slip: '0002', tb }),
    row({ loc: '00400304', sku: 'b', qty: 2, slip: '0003', tb }),
  ]);
  const { batchId } = importBatch(parseCs03002(csv), { hikiateClass: 'テスト' }, 'admin@b-faith.biz');
  return batchId;
}

let opSeq = 0;
const op = () => `test-op-${++opSeq}`;
const W1 = 'worker1@b-faith.biz';
const W2 = 'worker2@b-faith.biz';

function expectPkError(fn, code) {
  try {
    fn();
    assert.fail(`PkError(${code}) が投げられるはず`);
  } catch (e) {
    assert.ok(e instanceof PkError, `PkError のはずが ${e.constructor?.name}: ${e.message}`);
    assert.equal(e.code, code, `code=${code} のはずが ${e.code} (${e.message})`);
  }
}

let passed = 0;
function t(name, fn) { fn(); passed++; console.log(`  ok: ${name}`); }

t('start: ready→picking・worker/started_at・先頭明細のshown_at', () => {
  const id = makeBatch();
  const r = applyEvent(id, { opId: op(), event: 'start' }, W1);
  assert.equal(r.batchStatus, 'picking');
  assert.equal(r.currentSeq, 1);
  assert.equal(r.transition, 'started');
  const s = getWorkState(id);
  assert.equal(s.batch.worker, W1);
  assert.ok(s.batch.started_at);
  assert.ok(s.lines[0].shown_at, '明細1に表示時刻');
  assert.equal(s.lines[1].shown_at, null);
});

t('start: 同一opの再送は replayed で同一結果', () => {
  const id = makeBatch();
  const o = op();
  const r1 = applyEvent(id, { opId: o, event: 'start' }, W1);
  const r2 = applyEvent(id, { opId: o, event: 'start' }, W1);
  assert.equal(r2.replayed, true);
  assert.equal(r2.currentSeq, r1.currentSeq);
});

t('start: 他作業者は taken・同一作業者の再startはOK (リロード再開)', () => {
  const id = makeBatch();
  applyEvent(id, { opId: op(), event: 'start' }, W1);
  expectPkError(() => applyEvent(id, { opId: op(), event: 'start' }, W2), 'taken');
  const r = applyEvent(id, { opId: op(), event: 'start' }, W1);   // 新op・同一worker
  assert.equal(r.batchStatus, 'picking');
});

t('next: 順に完了し、最終明細でバッチ完了', () => {
  const id = makeBatch();
  applyEvent(id, { opId: op(), event: 'start' }, W1);
  let r = applyEvent(id, { opId: op(), event: 'next', lineSeq: 1 }, W1);
  assert.equal(r.currentSeq, 2);
  assert.equal(r.doneCount, 1);
  r = applyEvent(id, { opId: op(), event: 'next', lineSeq: 2 }, W1);
  assert.equal(r.currentSeq, 3);
  r = applyEvent(id, { opId: op(), event: 'next', lineSeq: 3 }, W1);
  assert.equal(r.batchStatus, 'done');
  assert.equal(r.currentSeq, null);
  assert.ok(r.finishedAt);
  assert.equal(r.transition, 'completed');
  const s = getWorkState(id);
  assert.ok(s.lines.every((l) => l.status === 'done' && l.done_at));
  assert.ok(s.lines[1].shown_at, '明細2は表示時刻が刻まれている');
});

t('next: 表示中以外の明細への next は out_of_order', () => {
  const id = makeBatch();
  applyEvent(id, { opId: op(), event: 'start' }, W1);
  expectPkError(() => applyEvent(id, { opId: op(), event: 'next', lineSeq: 2 }, W1), 'out_of_order');
});

t('next: 未開始バッチは not_picking・他作業者は taken', () => {
  const id = makeBatch();
  expectPkError(() => applyEvent(id, { opId: op(), event: 'next', lineSeq: 1 }, W1), 'not_picking');
  applyEvent(id, { opId: op(), event: 'start' }, W1);
  expectPkError(() => applyEvent(id, { opId: op(), event: 'next', lineSeq: 1 }, W2), 'taken');
});

t('back: 直前の完了明細だけ戻せる (done_atは消えstatusはpendingへ)', () => {
  const id = makeBatch();
  applyEvent(id, { opId: op(), event: 'start' }, W1);
  applyEvent(id, { opId: op(), event: 'next', lineSeq: 1 }, W1);
  const nextOp2 = op();
  applyEvent(id, { opId: nextOp2, event: 'next', lineSeq: 2 }, W1);
  expectPkError(() => applyEvent(id, { opId: op(), event: 'back', lineSeq: 1 }, W1), 'out_of_order');
  const r = applyEvent(id, { opId: op(), event: 'back', lineSeq: 2, undoOpId: nextOp2 }, W1);
  assert.equal(r.currentSeq, 2);
  assert.equal(r.doneCount, 1);
  const s = getWorkState(id);
  assert.equal(s.lines[1].status, 'pending');
  assert.equal(s.lines[1].done_at, null);
  assert.ok(s.lines[1].shown_at, 'shown_atは初回表示として残す');
});

t('back: 完了直後 (batch=done) の取り消しでバッチがpickingに戻る', () => {
  const id = makeBatch();
  applyEvent(id, { opId: op(), event: 'start' }, W1);
  let lastOp;
  for (const seq of [1, 2, 3]) applyEvent(id, { opId: (lastOp = op()), event: 'next', lineSeq: seq }, W1);
  assert.equal(getWorkState(id).batch.status, 'done');
  const r = applyEvent(id, { opId: op(), event: 'back', lineSeq: 3, undoOpId: lastOp }, W1);
  assert.equal(r.batchStatus, 'picking');
  assert.equal(r.currentSeq, 3);
  assert.equal(r.transition, 'reopened');
  const s = getWorkState(id);
  assert.equal(s.batch.finished_at, null);
  // 再完了できる
  const r2 = applyEvent(id, { opId: op(), event: 'next', lineSeq: 3 }, W1);
  assert.equal(r2.batchStatus, 'done');
});

t('next/back で shown_at は「直近に表示された時刻」に更新される', () => {
  const id = makeBatch();
  applyEvent(id, { opId: op(), event: 'start' }, W1);
  const nextOp1 = op();
  applyEvent(id, { opId: nextOp1, event: 'next', lineSeq: 1 }, W1);
  const shown2a = getWorkState(id).lines[1].shown_at;
  assert.ok(shown2a, '明細2に表示時刻');
  applyEvent(id, { opId: op(), event: 'back', lineSeq: 1, undoOpId: nextOp1 }, W1);
  const line1 = getWorkState(id).lines[0];
  assert.ok(line1.shown_at, 'backした明細1に再表示時刻が入る');
  assert.equal(line1.done_at, null);
});

t('back: 完了していない明細へは not_done', () => {
  const id = makeBatch();
  applyEvent(id, { opId: op(), event: 'start' }, W1);
  expectPkError(() => applyEvent(id, { opId: op(), event: 'back', lineSeq: 1 }, W1), 'not_done');
});

t('back: undo_op_id 無し/古い完了を指す back は stale_back (別端末の遅延back対策)', () => {
  const id = makeBatch();
  applyEvent(id, { opId: op(), event: 'start' }, W1);
  const firstNext = op();
  applyEvent(id, { opId: firstNext, event: 'next', lineSeq: 1 }, W1);
  // undo_op_id 無しは拒否
  expectPkError(() => applyEvent(id, { opId: op(), event: 'back', lineSeq: 1 }, W1), 'stale_back');
  // 端末Aが back→next で再完了
  applyEvent(id, { opId: op(), event: 'back', lineSeq: 1, undoOpId: firstNext }, W1);
  const secondNext = op();
  applyEvent(id, { opId: secondNext, event: 'next', lineSeq: 1 }, W1);
  // 端末Bに残っていた「最初の完了への back」が遅れて到着 → 新しい完了は取り消さない
  expectPkError(() => applyEvent(id, { opId: op(), event: 'back', lineSeq: 1, undoOpId: firstNext }, W1), 'stale_back');
  // 正しい対象なら戻せる
  const r = applyEvent(id, { opId: op(), event: 'back', lineSeq: 1, undoOpId: secondNext }, W1);
  assert.equal(r.currentSeq, 1);
});

t('takeover: 作業中の担当者を交代できる (選び間違いの救済)', () => {
  const id = makeBatch();
  applyEvent(id, { opId: op(), event: 'start' }, W1);
  applyEvent(id, { opId: op(), event: 'next', lineSeq: 1 }, W1);
  // W2はそのままでは操作できない
  expectPkError(() => applyEvent(id, { opId: op(), event: 'next', lineSeq: 2 }, W2), 'taken');
  // 交代 → W2が続きを操作でき、W1は逆にブロックされる
  const r = applyEvent(id, { opId: op(), event: 'takeover' }, W2);
  assert.equal(r.batchStatus, 'picking');
  assert.equal(r.transition, 'takeover', 'Notion担当者の追従トリガ');
  assert.equal(getWorkState(id).batch.worker, W2);
  applyEvent(id, { opId: op(), event: 'next', lineSeq: 2 }, W2);
  expectPkError(() => applyEvent(id, { opId: op(), event: 'next', lineSeq: 3 }, W1), 'taken');
  // 同一人の takeover は無害 (no-op)
  applyEvent(id, { opId: op(), event: 'takeover' }, W2);
  assert.equal(getWorkState(id).batch.worker, W2);
});

t('takeover: 未開始/完了後は not_picking', () => {
  const id = makeBatch();
  expectPkError(() => applyEvent(id, { opId: op(), event: 'takeover' }, W1), 'not_picking');
  applyEvent(id, { opId: op(), event: 'start' }, W1);
  for (const seq of [1, 2, 3]) applyEvent(id, { opId: op(), event: 'next', lineSeq: seq }, W1);
  expectPkError(() => applyEvent(id, { opId: op(), event: 'takeover' }, W2), 'not_picking');
});

t('op_conflict: 同一op_idを別内容で使い回すと409', () => {
  const id = makeBatch();
  const o = op();
  applyEvent(id, { opId: o, event: 'start' }, W1);
  expectPkError(() => applyEvent(id, { opId: o, event: 'next', lineSeq: 1 }, W1), 'op_conflict');
  expectPkError(() => applyEvent(id, { opId: o, event: 'start' }, W2), 'op_conflict');
});

t('完了後のnextは not_picking・完了後のstartは already_done', () => {
  const id = makeBatch();
  applyEvent(id, { opId: op(), event: 'start' }, W1);
  for (const seq of [1, 2, 3]) applyEvent(id, { opId: op(), event: 'next', lineSeq: seq }, W1);
  expectPkError(() => applyEvent(id, { opId: op(), event: 'next', lineSeq: 3 }, W1), 'not_picking');
  expectPkError(() => applyEvent(id, { opId: op(), event: 'start' }, W1), 'already_done');
});

t('next の replayed 再送は状態を進めない (二重完了しない)', () => {
  const id = makeBatch();
  applyEvent(id, { opId: op(), event: 'start' }, W1);
  const o = op();
  const r1 = applyEvent(id, { opId: o, event: 'next', lineSeq: 1 }, W1);
  const r2 = applyEvent(id, { opId: o, event: 'next', lineSeq: 1 }, W1);
  assert.equal(r2.replayed, true);
  assert.equal(r2.currentSeq, r1.currentSeq);
  assert.equal(getWorkState(id).doneCount, 1);
});

t('バッチ取消 (validity=invalid) 後のイベントは batch_invalid', () => {
  const id = makeBatch();
  getDB().prepare("UPDATE pk_batches SET status='cancelled' WHERE id=?").run(id);
  expectPkError(() => applyEvent(id, { opId: op(), event: 'start' }, W1), 'batch_invalid');
});

t('bad_op_id / bad_event / bad_line_seq の入力検証', () => {
  const id = makeBatch();
  expectPkError(() => applyEvent(id, { opId: '', event: 'start' }, W1), 'bad_op_id');
  expectPkError(() => applyEvent(id, { opId: 'x'.repeat(65), event: 'start' }, W1), 'bad_op_id');
  expectPkError(() => applyEvent(id, { opId: op(), event: 'jump' }, W1), 'bad_event');
  applyEvent(id, { opId: op(), event: 'start' }, W1);
  expectPkError(() => applyEvent(id, { opId: op(), event: 'next', lineSeq: 0 }, W1), 'bad_line_seq');
  expectPkError(() => applyEvent(id, { opId: op(), event: 'next', lineSeq: null }, W1), 'bad_line_seq');
});

// ─── PR4: 欠品・中断・取消 ───

t('shortage: 全量/一部欠品を記録して次へ進む・最終明細ならバッチ完了', () => {
  // 明細: seq1=qty1 / seq2=qty2 / seq3=qty3
  const id = makeBatch();
  applyEvent(id, { opId: op(), event: 'start' }, W1);
  applyEvent(id, { opId: op(), event: 'next', lineSeq: 1 }, W1);
  // 一部欠品 (指示2に対して欠品1)
  let r = applyEvent(id, { opId: op(), event: 'shortage', lineSeq: 2, shortageQty: 1 }, W1);
  assert.equal(r.currentSeq, 3);
  const s1 = getWorkState(id);
  assert.equal(s1.lines[1].status, 'shortage');
  assert.equal(s1.lines[1].shortage_qty, 1);
  // 全量欠品 (数量未指定=指示数)。最終明細なので完了
  r = applyEvent(id, { opId: op(), event: 'shortage', lineSeq: 3 }, W1);
  assert.equal(r.batchStatus, 'done');
  assert.equal(getWorkState(id).lines[2].shortage_qty, getWorkState(id).lines[2].qty);
});

t('shortage: 数量の範囲検証・表示中以外は out_of_order', () => {
  const id = makeBatch();
  applyEvent(id, { opId: op(), event: 'start' }, W1);
  expectPkError(() => applyEvent(id, { opId: op(), event: 'shortage', lineSeq: 1, shortageQty: 0 }, W1), 'bad_shortage_qty');
  expectPkError(() => applyEvent(id, { opId: op(), event: 'shortage', lineSeq: 1, shortageQty: 99 }, W1), 'bad_shortage_qty');
  expectPkError(() => applyEvent(id, { opId: op(), event: 'shortage', lineSeq: 2, shortageQty: 1 }, W1), 'out_of_order');
});

t('shortage v2: open→resolve の区間が paused_total_sec に入り、他ロケ確保と残りが記録される', () => {
  const id = makeBatch();
  applyEvent(id, { opId: op(), event: 'start' }, W1);
  applyEvent(id, { opId: op(), event: 'next', lineSeq: 1 }, W1);
  const t0 = Date.now() - 90 * 1000;
  applyEvent(id, { opId: op(), event: 'shortage_open', lineSeq: 2, clientAt: new Date(t0).toISOString() }, W1);
  let b = getDB().prepare('SELECT * FROM pk_batches WHERE id=?').get(id);
  assert.equal(b.shortage_open_seq, 2);
  // 開き直し (再送) は開始時刻を動かさない
  applyEvent(id, { opId: op(), event: 'shortage_open', lineSeq: 2, clientAt: new Date().toISOString() }, W1);
  assert.equal(getDB().prepare('SELECT shortage_open_at FROM pk_batches WHERE id=?').get(id).shortage_open_at, b.shortage_open_at);
  // 指示2: 他ロケで1個確保・残り1は後で取りに行く
  const r = applyEvent(id, { opId: op(), event: 'shortage', lineSeq: 2, shortageQty: 2,
    altBlock: 'P4FA', altLocation: '00100302', altQty: 1, remaining: 'later', clientAt: new Date().toISOString() }, W1);
  assert.equal(r.currentSeq, 3);
  const l = getDB().prepare('SELECT * FROM pk_lines WHERE batch_id=? AND seq=2').get(id);
  assert.deepEqual([l.status, l.shortage_qty, l.alt_block, l.alt_location, l.alt_qty, l.remaining_qty, l.remaining],
    ['shortage', 2, 'P4FA', '00100302', 1, 1, 'later']);
  b = getDB().prepare('SELECT * FROM pk_batches WHERE id=?').get(id);
  assert.equal(b.shortage_open_seq, null);
  assert.ok(b.paused_total_sec >= 85 && b.paused_total_sec <= 95, `欠品対応の約90秒が中断扱い (${b.paused_total_sec})`);
  // 明細の所要時間 (done_at - shown_at) からも除外される = shown_at が後ろへずれる (Codex R2)
  const lineSec = Math.round((Date.parse(l.done_at) - Date.parse(l.shown_at)) / 1000);
  assert.ok(lineSec <= 5, `明細時間に欠品対応が乗らない (${lineSec}s)`);
  // 全量を他ロケで確保 → remaining_qty=0・remaining=null (欠品には数えない)
  applyEvent(id, { opId: op(), event: 'shortage', lineSeq: 3, shortageQty: 3, altBlock: 'P4FA', altLocation: '00100303', altQty: 3 }, W1);
  const l3 = getDB().prepare('SELECT * FROM pk_lines WHERE batch_id=? AND seq=3').get(id);
  assert.deepEqual([l3.remaining_qty, l3.remaining, l3.alt_qty], [0, null, 3]);
  const sum = getDailySummary(getDB().prepare('SELECT work_date FROM pk_batches WHERE id=?').get(id).work_date);
  const mine = sum.shortages.filter((x) => x.batch_id === id);
  assert.deepEqual(mine.map((x) => x.seq), [2], '他ロケで全量確保した seq3 は欠品一覧に出ない');
});

t('shortage v2: cancel で計測再開・検証 (alt_qty範囲/ロケ必須/remaining値)', () => {
  const id = makeBatch();
  applyEvent(id, { opId: op(), event: 'start' }, W1);
  const t0 = Date.now() - 30 * 1000;
  applyEvent(id, { opId: op(), event: 'shortage_open', lineSeq: 1, clientAt: new Date(t0).toISOString() }, W1);
  applyEvent(id, { opId: op(), event: 'shortage_cancel', lineSeq: 1, clientAt: new Date().toISOString() }, W1);
  const b = getDB().prepare('SELECT * FROM pk_batches WHERE id=?').get(id);
  assert.equal(b.shortage_open_seq, null);
  assert.ok(b.paused_total_sec >= 25 && b.paused_total_sec <= 35, `やめた分も中断扱い (${b.paused_total_sec})`);
  assert.equal(getWorkState(id).lines[0].status, 'pending', '記録は残らない');
  expectPkError(() => applyEvent(id, { opId: op(), event: 'shortage_open', lineSeq: 2 }, W1), 'out_of_order');
  expectPkError(() => applyEvent(id, { opId: op(), event: 'shortage', lineSeq: 1, shortageQty: 1, altQty: 2, altLocation: 'x' }, W1), 'bad_alt_qty');
  expectPkError(() => applyEvent(id, { opId: op(), event: 'shortage', lineSeq: 1, shortageQty: 1, altQty: 1 }, W1), 'bad_alt_location');
  expectPkError(() => applyEvent(id, { opId: op(), event: 'shortage', lineSeq: 1, shortageQty: 1, remaining: 'maybe' }, W1), 'bad_remaining');
  // 旧クライアント (alt/remaining 無し) = 残り全量・none
  applyEvent(id, { opId: op(), event: 'shortage', lineSeq: 1, shortageQty: 1 }, W1);
  const l = getDB().prepare('SELECT * FROM pk_lines WHERE batch_id=? AND seq=1').get(id);
  assert.deepEqual([l.remaining_qty, l.remaining, l.alt_qty], [1, 'none', null]);
});

t('back: 欠品明細も取り消せる (shortage_qtyがクリアされる)', () => {
  const id = makeBatch();
  applyEvent(id, { opId: op(), event: 'start' }, W1);
  const shortOp = op();
  applyEvent(id, { opId: shortOp, event: 'shortage', lineSeq: 1, shortageQty: 1 }, W1);
  const r = applyEvent(id, { opId: op(), event: 'back', lineSeq: 1, undoOpId: shortOp }, W1);
  assert.equal(r.currentSeq, 1);
  const line = getWorkState(id).lines[0];
  assert.equal(line.status, 'pending');
  assert.equal(line.shortage_qty, null);
});

t('pause/resume: 中断時間が paused_total_sec に積まれ、状態が往復する', () => {
  const id = makeBatch();
  applyEvent(id, { opId: op(), event: 'start' }, W1);
  let r = applyEvent(id, { opId: op(), event: 'pause', pauseReason: '休憩' }, W1);
  assert.equal(r.batchStatus, 'paused');
  let b = getWorkState(id).batch;
  assert.equal(b.pause_reason, '休憩');
  assert.ok(b.pause_started_at);
  // 中断中は next 不可
  expectPkError(() => applyEvent(id, { opId: op(), event: 'next', lineSeq: 1 }, W1), 'not_picking');
  // 二重pauseは not_picking
  expectPkError(() => applyEvent(id, { opId: op(), event: 'pause', pauseReason: '休憩' }, W1), 'not_picking');
  r = applyEvent(id, { opId: op(), event: 'resume' }, W1);
  assert.equal(r.batchStatus, 'picking');
  b = getWorkState(id).batch;
  assert.equal(b.pause_started_at, null);
  assert.ok(b.paused_total_sec >= 0);
  // 再開前の resume は not_paused
  expectPkError(() => applyEvent(id, { opId: op(), event: 'resume' }, W1), 'not_paused');
});

t('pause/resume: オフライン再送でも client_at から中断時間を復元 (クランプ付き)', () => {
  const id = makeBatch();
  applyEvent(id, { opId: op(), event: 'start' }, W1);
  // 10分前に中断→今再開 (オフラインで積まれたキューが連続送信されたケース)
  const tenMinAgo = new Date(Date.now() - 600_000).toISOString();
  applyEvent(id, { opId: op(), event: 'pause', pauseReason: '休憩', clientAt: tenMinAgo }, W1);
  applyEvent(id, { opId: op(), event: 'resume', clientAt: new Date().toISOString() }, W1);
  const sec = getWorkState(id).batch.paused_total_sec;
  assert.ok(sec >= 590 && sec <= 610, `中断時間が約600秒のはず: ${sec}`);
  // 未来のclient_atは now に丸められる (細工防止)
  const future = new Date(Date.now() + 3600_000).toISOString();
  applyEvent(id, { opId: op(), event: 'pause', pauseReason: '休憩', clientAt: future }, W1);
  applyEvent(id, { opId: op(), event: 'resume' }, W1);
  const sec2 = getWorkState(id).batch.paused_total_sec;
  assert.ok(sec2 - sec < 5, `未来時刻は採用されない: ${sec2 - sec}`);
});

t('pause中の takeover → 交代した人が resume できる', () => {
  const id = makeBatch();
  applyEvent(id, { opId: op(), event: 'start' }, W1);
  applyEvent(id, { opId: op(), event: 'pause', pauseReason: '他作業への応援' }, W1);
  expectPkError(() => applyEvent(id, { opId: op(), event: 'resume' }, W2), 'taken');
  applyEvent(id, { opId: op(), event: 'takeover' }, W2);
  const r = applyEvent(id, { opId: op(), event: 'resume' }, W2);
  assert.equal(r.batchStatus, 'picking');
});

t('cancel: 進捗・時間・担当を初期化して ready に戻す', () => {
  const id = makeBatch();
  applyEvent(id, { opId: op(), event: 'start' }, W1);
  applyEvent(id, { opId: op(), event: 'next', lineSeq: 1 }, W1);
  applyEvent(id, { opId: op(), event: 'shortage', lineSeq: 2, shortageQty: 1 }, W1);
  const r = applyEvent(id, { opId: op(), event: 'cancel' }, W1);
  assert.equal(r.batchStatus, 'ready');
  const s = getWorkState(id);
  assert.equal(s.batch.worker, null);
  assert.equal(s.batch.started_at, null);
  assert.ok(s.lines.every((l) => l.status === 'pending' && l.done_at === null && l.shortage_qty === null));
  // 取り消し後にもう一度最初から開始できる
  const r2 = applyEvent(id, { opId: op(), event: 'start' }, W2);
  assert.equal(r2.batchStatus, 'picking');
  assert.equal(r2.currentSeq, 1);
});

t('getDailySummary: 完了バッチの集計と欠品一覧', () => {
  const today = (new Date()).toLocaleDateString('en-CA', { timeZone: 'Asia/Tokyo' });
  const id = makeBatch();
  applyEvent(id, { opId: op(), event: 'start' }, W1);
  applyEvent(id, { opId: op(), event: 'next', lineSeq: 1 }, W1);
  applyEvent(id, { opId: op(), event: 'shortage', lineSeq: 2, shortageQty: 2 }, W1);
  applyEvent(id, { opId: op(), event: 'next', lineSeq: 3 }, W1);
  const sum = getDailySummary(today);
  assert.ok(sum.total.doneCount >= 1);
  const w = sum.byWorker.find((g) => g.key === W1);
  assert.ok(w && w.lines >= 3);
  assert.ok(sum.shortages.some((s) => s.shortage_qty === 2));
  assert.ok(sum.shortages.every((s) => s.locationLabel.includes('-')));
});

console.log(`\ntest-work: ${passed} 件 pass`);
