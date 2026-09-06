/**
 * test-repick.mjs — 🔴ピッキング漏れバッチ (2026-08-21 中原さん指示) の検証
 *
 * 実行: node apps/picking/tests/test-repick.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'picking-repick-test-'));

const { initPickingDB, getDB, listBatches, listLines, jstToday } = await import('../db.js');
const { createRepickBatch, reconcileRepickBatches, getDailySummary, getTodayProgress, repickReasonOf, REPICK_CLASS } = await import('../service.js');

initPickingDB();
const db = getDB();

let passed = 0;
function t(name, fn) { fn(); passed++; console.log(`  ok: ${name}`); }

const task = {
  id: 101, sku: 'kofunneil-0776', product_name: '胡粉ネイル【古代岱赭】', req_qty: 1,
  location: '00201604', block: 'P3FB', folder_name: '出荷_02', slip_seq: 95, requested_by: '大場江莉果',
};

t('createRepickBatch: バッチ+明細を生成 (ロケ・依頼元・依頼者・計測除外フラグ)', () => {
  const r = createRepickBatch(task);
  assert.equal(r.existed, false);
  const b = db.prepare('SELECT * FROM pk_batches WHERE id=?').get(r.batchId);
  assert.equal(b.origin, 'repick');
  assert.equal(b.hikiate_class, '🔴 ピッキング漏れ (梱包から・不足)', '梱包由来 (理由=不足) の名前');
  assert.equal(b.repick_reason, 'shortage');
  assert.equal(b.origin_ref, '出荷_02 #95');
  assert.equal(b.requested_by, '大場江莉果');
  assert.equal(b.pack_task_id, 101);
  assert.equal(b.folder_name, null);          // Notionカード・shipping-log を誤爆させない
  assert.equal(b.status, 'ready');
  const lines = listLines(r.batchId);
  assert.equal(lines.length, 1);
  assert.equal(lines[0].sku, 'kofunneil-0776');
  assert.equal(lines[0].location, '00201604');
  assert.equal(lines[0].qty, 1);
});

t('createRepickBatch: 同じタスクは再作成しない (tb_no冪等)', () => {
  const r = createRepickBatch(task);
  assert.equal(r.existed, true);
});

t('一覧には出る / 計測 (サマリ・当日進捗) からは除外', () => {
  const today = jstToday();
  assert.ok(listBatches(today).some((b) => b.origin === 'repick'), '一覧に出る');
  assert.equal(getDailySummary(today).total.batchCount, 0, 'サマリは0件 (repick除外)');
  const prog = getTodayProgress(today);
  assert.equal(prog.totalLines ?? 0, 0, '当日進捗にも入らない');
});

t('PR-5: 理由で名前が分かれる — 自分の「後で取りに行く」は 🕒 (漏れと呼ばない) / 品違いは 🔴 品違い', () => {
  // packing 無効環境 (pk_pack_incidents が無い) = 不足扱い。テーブルがあって候補が引けない = 未確定 (null)
  assert.equal(repickReasonOf({ incident_id: 5 }), 'shortage', 'テーブル無しは不足扱い');
  assert.equal(repickReasonOf({}), 'shortage', 'incident 無し (旧データ) は不足');
  const later = createRepickBatch({ ...task, id: 202, later_request_id: 7, requested_by: '有國陽' });
  const bl = db.prepare('SELECT * FROM pk_batches WHERE id=?').get(later.batchId);
  assert.equal(bl.repick_reason, 'later');
  assert.equal(bl.hikiate_class, REPICK_CLASS.later);
  assert.match(bl.hikiate_class, /^🕒 後で取りに行く/);
  // 品違い: pk_pack_incidents (packing 所有・参照のみ) の kind を見る
  db.exec('CREATE TABLE IF NOT EXISTS pk_pack_incidents (id INTEGER PRIMARY KEY, kind TEXT)');
  db.prepare("INSERT INTO pk_pack_incidents (id, kind) VALUES (55, 'wrong_item')").run();
  const wi = createRepickBatch({ ...task, id: 203, incident_id: 55 });
  const bw = db.prepare('SELECT * FROM pk_batches WHERE id=?').get(wi.batchId);
  assert.equal(bw.repick_reason, 'wrong_item');
  assert.match(bw.hikiate_class, /品違い/);
  db.prepare("INSERT INTO pk_pack_incidents (id, kind) VALUES (56, 'shortage')").run();
  assert.equal(repickReasonOf({ incident_id: 56 }), 'shortage', '不足の候補は不足');
  assert.equal(repickReasonOf({ incident_id: 999 }), null, '候補が引けなければ未確定 (不足に固定しない — Codex R1)');
  // 未確定のまま作ったバッチは表示は暫定「不足」、DB は NULL → 候補が現れたら reconcile が直す
  const pend = createRepickBatch({ ...task, id: 204, incident_id: 999 });
  const bp = db.prepare('SELECT * FROM pk_batches WHERE id=?').get(pend.batchId);
  assert.equal(bp.repick_reason, null);
  assert.equal(bp.hikiate_class, REPICK_CLASS.shortage);
});

t('reconcile: 梱包側でタスク取消 → 漏れバッチも取消', () => {
  // packing 所有の pk_pack_tasks を試験用に用意 (実環境では packing が作る)
  db.exec(`CREATE TABLE IF NOT EXISTS pk_pack_tasks (
    id INTEGER PRIMARY KEY, status TEXT, kind TEXT, sku TEXT, product_name TEXT,
    req_qty INTEGER DEFAULT 1, location TEXT, block TEXT, folder_name TEXT,
    slip_seq INTEGER, requested_by TEXT, later_request_id INTEGER, incident_id INTEGER)`);
  db.prepare("INSERT INTO pk_pack_tasks (id, status, kind, sku) VALUES (101, 'cancelled', 'repick', 'kofunneil-0776')").run();
  const n = reconcileRepickBatches();
  assert.equal(n, 1);
  const b = db.prepare('SELECT * FROM pk_batches WHERE pack_task_id=101').get();
  assert.equal(b.status, 'cancelled');
  assert.equal(b.validity, 'invalid');
});

t('reconcile: 進行中タスクのバッチは触らない', () => {
  db.prepare("INSERT INTO pk_pack_tasks (id, status, kind, sku) VALUES (102, 'requested', 'repick', 'aaa')").run();
  createRepickBatch({ ...task, id: 102, sku: 'aaa' });
  assert.equal(reconcileRepickBatches(), 0);
  assert.equal(db.prepare('SELECT status FROM pk_batches WHERE pack_task_id=102').get().status, 'ready');
});

t('reconcile: バッチ未生成の再ピックタスクを拾って生成 (resolve時の失敗から自己修復)', () => {
  db.prepare(`INSERT INTO pk_pack_tasks (id, status, kind, sku, product_name, req_qty, location, folder_name, slip_seq, requested_by)
    VALUES (103, 'requested', 'repick', 'bbb', '商品B', 2, '00300101', '出荷_05', 7, '大場')`).run();
  assert.equal(reconcileRepickBatches(), 1);
  const b = db.prepare('SELECT * FROM pk_batches WHERE pack_task_id=103').get();
  assert.equal(b.origin, 'repick');
  assert.equal(b.origin_ref, '出荷_05 #7');
  assert.equal(reconcileRepickBatches(), 0, '2回目は生成しない (冪等)');
});

t('PR-5: v16 以前のバッチ (repick_reason NULL) は reconcile で理由と名前が埋まる (未完了のものだけ)', () => {
  db.prepare("INSERT INTO pk_pack_tasks (id, status, kind, sku, later_request_id) VALUES (104, 'requested', 'repick', 'ccc', 9)").run();
  createRepickBatch({ ...task, id: 104, sku: 'ccc' });
  db.prepare("UPDATE pk_batches SET repick_reason=NULL, hikiate_class='ピッキング漏れ' WHERE pack_task_id=104").run();
  db.prepare("INSERT INTO pk_pack_tasks (id, status, kind, sku) VALUES (105, 'cancelled', 'repick', 'ddd')").run();
  createRepickBatch({ ...task, id: 105, sku: 'ddd' });
  db.prepare("UPDATE pk_batches SET repick_reason=NULL, hikiate_class='ピッキング漏れ', status='done' WHERE pack_task_id=105").run();
  // 未確定 (候補 999 が無い) のバッチ 204 も対象。タスク行を用意し、候補が現れたら品違いに直る
  db.prepare("INSERT INTO pk_pack_tasks (id, status, kind, sku, incident_id) VALUES (204, 'requested', 'repick', 'kofunneil-0776', 999)").run();
  reconcileRepickBatches();
  const b = db.prepare('SELECT * FROM pk_batches WHERE pack_task_id=104').get();
  assert.equal(b.repick_reason, 'later');
  assert.equal(b.hikiate_class, REPICK_CLASS.later);
  assert.equal(db.prepare('SELECT repick_reason FROM pk_batches WHERE pack_task_id=105').get().repick_reason, null, '完了済みは触らない');
  assert.equal(db.prepare('SELECT repick_reason FROM pk_batches WHERE pack_task_id=204').get().repick_reason, null, '候補がまだ無ければ未確定のまま');
  db.prepare("INSERT INTO pk_pack_incidents (id, kind) VALUES (999, 'wrong_item')").run();
  reconcileRepickBatches();
  const b204 = db.prepare('SELECT * FROM pk_batches WHERE pack_task_id=204').get();
  assert.equal(b204.repick_reason, 'wrong_item', '候補が現れたら再判定される');
  assert.equal(b204.hikiate_class, REPICK_CLASS.wrong_item, '名前も直る');
  const before = db.prepare('SELECT updated_at FROM pk_batches WHERE pack_task_id=104').get().updated_at;
  reconcileRepickBatches();
  assert.equal(db.prepare('SELECT updated_at FROM pk_batches WHERE pack_task_id=104').get().updated_at, before, '2回目は更新しない (冪等)');
});

try { fs.rmSync(process.env.DATA_DIR, { recursive: true, force: true }); } catch { /* 無視 */ }
console.log(`\ntest-repick: ${passed} 件 pass`);
