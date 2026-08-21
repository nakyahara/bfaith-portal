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
const { createRepickBatch, reconcileRepickBatches, getDailySummary, getTodayProgress } = await import('../service.js');

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
  assert.equal(b.hikiate_class, 'ピッキング漏れ');
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

t('reconcile: 梱包側でタスク取消 → 漏れバッチも取消', () => {
  // packing 所有の pk_pack_tasks を試験用に用意 (実環境では packing が作る)
  db.exec(`CREATE TABLE IF NOT EXISTS pk_pack_tasks (
    id INTEGER PRIMARY KEY, status TEXT, kind TEXT, sku TEXT, product_name TEXT,
    req_qty INTEGER DEFAULT 1, location TEXT, block TEXT, folder_name TEXT,
    slip_seq INTEGER, requested_by TEXT)`);
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

try { fs.rmSync(process.env.DATA_DIR, { recursive: true, force: true }); } catch { /* 無視 */ }
console.log(`\ntest-repick: ${passed} 件 pass`);
