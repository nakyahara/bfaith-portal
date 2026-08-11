/**
 * picking PR2 — 作業イベント (start/next/back) の状態機械・冪等・排他のテスト。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import iconv from 'iconv-lite';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'picking-test-'));

const { parseCs03002, importBatch, applyEvent, getWorkState, PkError } = await import('../service.js');
const { initPickingDB, getDB } = await import('../db.js');

initPickingDB();

const HEADERS = [
  '出荷指示日', 'ブロック略称', 'ロケーション', '商品ID', '商品名', '出荷指示数',
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
    '商品ID': sku, '商品名': `商品${sku}_梱機プ`, '出荷指示数': String(qty),
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
  applyEvent(id, { opId: op(), event: 'next', lineSeq: 2 }, W1);
  expectPkError(() => applyEvent(id, { opId: op(), event: 'back', lineSeq: 1 }, W1), 'out_of_order');
  const r = applyEvent(id, { opId: op(), event: 'back', lineSeq: 2 }, W1);
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
  for (const seq of [1, 2, 3]) applyEvent(id, { opId: op(), event: 'next', lineSeq: seq }, W1);
  assert.equal(getWorkState(id).batch.status, 'done');
  const r = applyEvent(id, { opId: op(), event: 'back', lineSeq: 3 }, W1);
  assert.equal(r.batchStatus, 'picking');
  assert.equal(r.currentSeq, 3);
  const s = getWorkState(id);
  assert.equal(s.batch.finished_at, null);
  // 再完了できる
  const r2 = applyEvent(id, { opId: op(), event: 'next', lineSeq: 3 }, W1);
  assert.equal(r2.batchStatus, 'done');
});

t('next/back で shown_at は「直近に表示された時刻」に更新される', () => {
  const id = makeBatch();
  applyEvent(id, { opId: op(), event: 'start' }, W1);
  applyEvent(id, { opId: op(), event: 'next', lineSeq: 1 }, W1);
  const shown2a = getWorkState(id).lines[1].shown_at;
  assert.ok(shown2a, '明細2に表示時刻');
  applyEvent(id, { opId: op(), event: 'back', lineSeq: 1 }, W1);
  const line1 = getWorkState(id).lines[0];
  assert.ok(line1.shown_at, 'backした明細1に再表示時刻が入る');
  assert.equal(line1.done_at, null);
});

t('back: 完了していない明細へは not_done', () => {
  const id = makeBatch();
  applyEvent(id, { opId: op(), event: 'start' }, W1);
  expectPkError(() => applyEvent(id, { opId: op(), event: 'back', lineSeq: 1 }, W1), 'not_done');
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

console.log(`\ntest-work: ${passed} 件 pass`);
