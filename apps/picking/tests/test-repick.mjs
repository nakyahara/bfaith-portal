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
    slip_seq INTEGER, requested_by TEXT, later_request_id INTEGER, incident_id INTEGER, batch_id INTEGER)`);
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
  // 着手済み (picking) の未確定は表示を固定 = reconcile は触らない (開いたままの作業画面と DB が分裂しない — Codex R2)
  db.prepare("INSERT INTO pk_pack_tasks (id, status, kind, sku, incident_id) VALUES (205, 'claimed', 'repick', 'eee', 55)").run();
  createRepickBatch({ ...task, id: 205, sku: 'eee', incident_id: 55 });
  db.prepare("UPDATE pk_batches SET repick_reason=NULL, hikiate_class=?, status='picking' WHERE pack_task_id=205").run(REPICK_CLASS.shortage);
  reconcileRepickBatches();
  const b205 = db.prepare('SELECT repick_reason, hikiate_class FROM pk_batches WHERE pack_task_id=205').get();
  assert.equal(b205.repick_reason, null, '着手済みは触らない');
  assert.equal(b205.hikiate_class, REPICK_CLASS.shortage, '着手済みの名前も固定');
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


console.log('── PR-7: 同じ伝票の複数タスクは1つの 🔴 バッチに ──');
{
  const { repickTaskIdsOf } = await import('../service.js');
  const slip = { ...task, batch_id: 50, slip_seq: 9, folder_name: '出荷_07' };
  const ins = (id, sku, status = 'requested', extra = {}) => db.prepare(`INSERT INTO pk_pack_tasks (id, status, kind, sku, req_qty, batch_id, slip_seq, later_request_id)
    VALUES (?, ?, 'repick', ?, ?, 50, 9, ?)`).run(id, status, sku, extra.qty ?? 1, extra.later ?? null);
  ins(301, 'g-a'); ins(302, 'g-b', 'requested', { qty: 2 }); ins(303, 'g-c', 'requested', { later: 5 }); ins(304, 'g-d');
  let gid;
  t('1件目は新規バッチ (pack_batch_id / pack_slip_seq が入る)', () => {
    const r = createRepickBatch({ ...slip, id: 301, sku: 'g-a' });
    assert.equal(r.existed, false);
    gid = r.batchId;
    const b = db.prepare('SELECT * FROM pk_batches WHERE id=?').get(gid);
    assert.equal(b.pack_batch_id, 50);
    assert.equal(b.pack_slip_seq, 9);
    assert.equal(listLines(gid)[0].pack_task_id, 301, '行にタスク id');
  });
  t('同じ伝票の2件目は既存バッチに行が足される (件数・数量・構成が更新)', () => {
    const r = createRepickBatch({ ...slip, id: 302, sku: 'g-b', req_qty: 2 });
    assert.equal(r.batchId, gid);
    assert.equal(r.appended, true);
    const b = db.prepare('SELECT * FROM pk_batches WHERE id=?').get(gid);
    assert.equal(b.line_count, 2);
    assert.equal(b.total_qty, 3);
    assert.equal(b.composition, '複数SKU');
    assert.equal(b.pack_task_id, 301, '同期キーは最初のタスクのまま');
    const lines = listLines(gid);
    assert.deepEqual(lines.map((l) => [l.seq, l.sku, l.qty, l.pack_task_id]), [[1, 'g-a', 1, 301], [2, 'g-b', 2, 302]]);
    assert.deepEqual(repickTaskIdsOf(db, gid), [301, 302]);
  });
  t('合流済みのタスクをもう一度渡しても作らない (行の pack_task_id で冪等)', () => {
    const r = createRepickBatch({ ...slip, id: 302, sku: 'g-b', req_qty: 2 });
    assert.equal(r.existed, true);
    assert.equal(r.batchId, gid);
    assert.equal(listLines(gid).length, 2);
  });
  t('品違いのバッチに不足が合流すると名前は汎用の「不足」に (逆向きは変えない)', () => {
    const wi = { ...slip, batch_id: 51, slip_seq: 2 };
    db.prepare("INSERT INTO pk_pack_incidents (id, kind) VALUES (77, 'wrong_item')").run();
    const r1 = createRepickBatch({ ...wi, id: 311, sku: 'm-a', incident_id: 77 });
    assert.equal(db.prepare('SELECT repick_reason FROM pk_batches WHERE id=?').get(r1.batchId).repick_reason, 'wrong_item');
    const r2 = createRepickBatch({ ...wi, id: 312, sku: 'm-b' });
    assert.equal(r2.batchId, r1.batchId);
    const b = db.prepare('SELECT repick_reason, hikiate_class FROM pk_batches WHERE id=?').get(r1.batchId);
    assert.equal(b.repick_reason, 'shortage');
    assert.equal(b.hikiate_class, REPICK_CLASS.shortage);
    const r3 = createRepickBatch({ ...wi, id: 313, sku: 'm-c', incident_id: 77 });
    assert.equal(r3.batchId, r1.batchId);
    assert.equal(db.prepare('SELECT repick_reason FROM pk_batches WHERE id=?').get(r1.batchId).repick_reason, 'shortage', '不足に品違いが合流しても不足のまま');
  });
  t('自分の「後で取りに行く」(later) は同じ伝票でも別バッチ', () => {
    const r = createRepickBatch({ ...slip, id: 303, sku: 'g-c', later_request_id: 5 });
    assert.notEqual(r.batchId, gid);
    assert.equal(db.prepare('SELECT repick_reason FROM pk_batches WHERE id=?').get(r.batchId).repick_reason, 'later');
  });
  t('着手済み (picking) のバッチには足さず新しいバッチになる', () => {
    db.prepare("UPDATE pk_batches SET status='picking' WHERE id=?").run(gid);
    const r = createRepickBatch({ ...slip, id: 304, sku: 'g-d' });
    assert.notEqual(r.batchId, gid);
    assert.equal(r.appended, undefined);
    assert.equal(listLines(gid).length, 2, '着手済みの明細は変わらない');
    db.prepare("UPDATE pk_batches SET status='ready' WHERE id=?").run(gid);
  });
  t('reconcile ②: バッチ未生成のタスクも同じ伝票の未着手バッチへ合流する', () => {
    ins(305, 'g-e');
    reconcileRepickBatches();
    const l = db.prepare('SELECT batch_id FROM pk_lines WHERE pack_task_id=305').get();
    assert.ok(l, '行が作られる');
    // 304 のバッチ (id が大きい方) に合流する = ORDER BY id DESC
    const b304 = db.prepare('SELECT batch_id FROM pk_lines WHERE pack_task_id=304').get().batch_id;
    assert.equal(l.batch_id, b304);
    assert.equal(reconcileRepickBatches(), 0, '2回目は何もしない');
  });
  t('reconcile ①: 一部のタスクが取下げ → 未着手ならその行だけ外れ、件数と同期キーが直る', () => {
    db.prepare("UPDATE pk_pack_tasks SET status='cancelled' WHERE id=301").run();
    reconcileRepickBatches();
    const b = db.prepare('SELECT * FROM pk_batches WHERE id=?').get(gid);
    assert.equal(b.status, 'ready', 'バッチは残る');
    assert.equal(b.line_count, 1);
    assert.equal(b.total_qty, 2);
    assert.equal(b.composition, '単品');
    assert.equal(b.pack_task_id, 302, '同期キーが残った行のタスクに付け替わる');
    assert.deepEqual(listLines(gid).map((l) => l.pack_task_id), [302]);
  });
  t('reconcile ①: 着手済みなら行は外さない', () => {
    const b304 = db.prepare('SELECT batch_id FROM pk_lines WHERE pack_task_id=304').get().batch_id;
    db.prepare("UPDATE pk_batches SET status='picking' WHERE id=?").run(b304);
    db.prepare("UPDATE pk_pack_tasks SET status='cancelled' WHERE id=305").run();
    reconcileRepickBatches();
    assert.equal(listLines(b304).length, 2, '行はそのまま');
    assert.equal(db.prepare('SELECT status FROM pk_batches WHERE id=?').get(b304).status, 'picking');
  });
  t('Codex R1: 行のタスクが一部読めないうちは畳まない (fail-closed)', () => {
    const r1 = createRepickBatch({ ...slip, batch_id: 52, slip_seq: 1, id: 321, sku: 'x-a' });
    createRepickBatch({ ...slip, batch_id: 52, slip_seq: 1, id: 322, sku: 'x-b' });
    ins(321, 'x-a', 'cancelled');   // 322 の行は pk_pack_tasks に無い
    db.prepare('UPDATE pk_pack_tasks SET batch_id=52, slip_seq=1 WHERE id=321').run();
    assert.equal(reconcileRepickBatches(), 0);
    const b = db.prepare('SELECT status, line_count FROM pk_batches WHERE id=?').get(r1.batchId);
    assert.equal(b.status, 'ready', '畳まない');
    assert.equal(b.line_count, 2, '行も外さない');
  });
  t('reconcile ①: 全部取下げ → バッチごと取消', () => {
    db.prepare("UPDATE pk_pack_tasks SET status='cancelled' WHERE id=302").run();
    assert.equal(reconcileRepickBatches(), 1);
    const b = db.prepare('SELECT status, validity FROM pk_batches WHERE id=?').get(gid);
    assert.deepEqual([b.status, b.validity], ['cancelled', 'invalid']);
  });
}

console.log('── v18 マイグレーション (v17 形式の DB から) ──');
{
  // v17 形式に戻す: 追加列と索引を落とし、旧形式の1行バッチを置いてから初期化し直す (initPickingDB は再実行で migrate)
  db.exec('DROP INDEX IF EXISTS idx_pk_lines_pack_task');
  db.exec('DROP INDEX IF EXISTS idx_pk_batches_repick_slip');
  db.exec('ALTER TABLE pk_lines DROP COLUMN pack_task_id');
  db.exec('ALTER TABLE pk_batches DROP COLUMN pack_batch_id');
  db.exec('ALTER TABLE pk_batches DROP COLUMN pack_slip_seq');
  db.pragma('user_version = 17');
  db.prepare(`INSERT INTO pk_pack_tasks (id, status, kind, sku, batch_id, slip_seq) VALUES (401, 'requested', 'repick', 'mig-a', 60, 4)`).run();
  db.prepare(`INSERT INTO pk_batches (tb_no, hikiate_class, work_date, composition, line_count, slip_count, total_qty, status, validity,
    csv_sha256, imported_by, created_at, updated_at, origin, pack_task_id)
    VALUES ('REPICK-401', 'x', '2026-09-07', '単品', 1, 1, 1, 'ready', 'valid', 's401', 't', 'a', 'a', 'repick', 401)`).run();
  const mb = Number(db.prepare("SELECT id FROM pk_batches WHERE tb_no='REPICK-401'").get().id);
  db.prepare("INSERT INTO pk_lines (batch_id, seq, location, sku, qty) VALUES (?, 1, '00100101', 'mig-a', 1)").run(mb);
  const db2 = initPickingDB();
  t('v17 → v18: 既存の1行バッチは行に pack_task_id、バッチに合流キーが入る', () => {
    assert.equal(db2.pragma('user_version', { simple: true }), 18);
    assert.equal(db2.prepare('SELECT pack_task_id FROM pk_lines WHERE batch_id=?').get(mb).pack_task_id, 401);
    const b = db2.prepare('SELECT pack_batch_id, pack_slip_seq FROM pk_batches WHERE id=?').get(mb);
    assert.deepEqual([b.pack_batch_id, b.pack_slip_seq], [60, 4]);
    assert.equal(db2.prepare('SELECT COUNT(*) AS c FROM pk_lines WHERE pack_task_id IS NOT NULL').get().c > 1, true, '他の再ピック行も埋まる');
  });
  t('v17 → v18 (pk_pack_tasks が無い環境): 合流キーは NULL のまま通る', () => {
    db2.exec('DROP INDEX IF EXISTS idx_pk_lines_pack_task');
    db2.exec('DROP INDEX IF EXISTS idx_pk_batches_repick_slip');
    db2.exec('ALTER TABLE pk_lines DROP COLUMN pack_task_id');
    db2.exec('ALTER TABLE pk_batches DROP COLUMN pack_batch_id');
    db2.exec('ALTER TABLE pk_batches DROP COLUMN pack_slip_seq');
    db2.exec('ALTER TABLE pk_pack_tasks RENAME TO pk_pack_tasks_hidden');
    db2.pragma('user_version = 17');
    const db3 = initPickingDB();
    assert.equal(db3.pragma('user_version', { simple: true }), 18);
    assert.equal(db3.prepare('SELECT pack_task_id FROM pk_lines WHERE batch_id=?').get(mb).pack_task_id, 401);
    assert.equal(db3.prepare('SELECT pack_batch_id FROM pk_batches WHERE id=?').get(mb).pack_batch_id, null);
  });
}

try { fs.rmSync(process.env.DATA_DIR, { recursive: true, force: true }); } catch { /* 無視 */ }
console.log(`\ntest-repick: ${passed} 件 pass`);
