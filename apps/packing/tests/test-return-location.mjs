/**
 * test-return-location.mjs — ↩ 棚戻しの戻し先ロケ (例外処理監査 PR-4・Q3 決定 2026-09-05「戻したロケを記録する」)
 *
 * 守りたいこと:
 *   ① 余り (excess) の SKU は検証済みのものだけ (形式不正 = 商品名の断片は 400)。名前 (在庫検索由来) を持ち、棚戻しタスクに載る
 *   ② 参考ロケが無い棚戻しにロジザードの候補を後から入れられる (setTaskLocationHint)。入っていれば触らない
 *   ③ 棚戻しの完了 (fulfill) は「戻したロケ」が必須。記録 (returned_block/location/at/by) と通知フラグ。再ピックの fulfill は従来どおり
 *   ④ v21 の列がある (旧 DB にも足される)
 *
 * 実行: node apps/packing/tests/test-return-location.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'packing-return-test-'));

const { initPickingDB } = await import('../../picking/db.js');
const { initPackingDB, getDB, utcNow, jstToday } = await import('../db.js');
const { applyEvent, applyTaskAction, resolveIncident, setTaskLocationHint, getTaskDetail, getWorkState } = await import('../service.js');

initPickingDB();
initPackingDB();
const db = getDB();
const now = utcNow();
const today = jstToday();

let passed = 0;
function t(name, fn) { fn(); passed++; console.log(`  ok: ${name}`); }
function throwsCode(fn, code, name) {
  try { fn(); assert.fail(`${name}: エラーにならなかった`); }
  catch (e) { assert.equal(e.code, code, `${name} (実際=${e.code || e.message})`); }
  passed++; console.log(`  ok: ${name}`);
}
let op = 0;
const ev = (batchId, event, extra = {}, worker = '三宅晴菜') => applyEvent(batchId, { opId: `r${++op}`, event, ...extra }, worker);

function mkPackBatch(tbKey, slips) {
  const batchId = Number(db.prepare(`INSERT INTO pk_pack_batches
    (tb_key, folder_name, work_date, slip_count, line_count, total_qty, match_status, status, worker, started_at, csv_sha256, imported_by, created_at, updated_at)
    VALUES (?, '出荷_40', ?, ?, ?, ?, 'ok', 'packing', '三宅晴菜', ?, ?, 'test', ?, ?)`)
    .run(tbKey, today, slips.length, slips.length, slips.length, now, `sha-${tbKey}`, now, now).lastInsertRowid);
  slips.forEach((s, i) => {
    const sid = Number(db.prepare(`INSERT INTO pk_pack_slips (batch_id, seq, ne_slip_no, slip_no, recipient_name, site_order_no, status, delivery_method, done_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, '箱', ?)`)
      .run(batchId, i + 1, `${tbKey}-NE${i + 1}`, `SP${tbKey}-${i + 1}`, `客${i + 1}`, `SO-${i + 1}`, s.status, s.status === 'done' ? now : null).lastInsertRowid);
    for (const sku of [].concat(s.sku)) {
      db.prepare('INSERT INTO pk_pack_lines (slip_id, sku, product_name, qty) VALUES (?, ?, ?, 1)').run(sid, sku, `商品${sku}`);
    }
  });
  return batchId;
}
const taskRow = (id) => db.prepare('SELECT * FROM pk_pack_tasks WHERE id=?').get(id);

console.log('── ④ v21: pk_pack_tasks に戻したロケの列 ──');
t('returned_block / returned_location / returned_at / returned_by / location_source がある', () => {
  const cols = db.prepare('PRAGMA table_info(pk_pack_tasks)').all().map((c) => c.name);
  for (const c of ['returned_block', 'returned_location', 'returned_at', 'returned_by', 'location_source']) assert.ok(cols.includes(c), c);
  assert.equal(db.prepare("SELECT value FROM pk_pack_meta WHERE key='schema_version'").get()?.value ?? db.prepare('PRAGMA user_version').get()?.user_version, '21');
});

console.log('── ① 余りの SKU は検証済みのものだけ・名前を持つ ──');
const pb = mkPackBatch('R1', [{ sku: 'a-1', status: 'done' }]);
{
  throwsCode(() => ev(pb, 'excess', { sku: '耳かき', qty: 9 }), 'bad_sku', '商品名の断片 (形式不正) は 400');
  throwsCode(() => ev(pb, 'excess', { sku: '', qty: 1 }), 'bad_sku', '空は 400');
  throwsCode(() => ev(pb, 'excess', { sku: 'mimikaki-9', qty: 0 }), 'bad_qty', '数量 0 は 400');
  t('検証済み SKU (大文字・空白は正規化) と名前で候補が記録される', () => {
    ev(pb, 'excess', { sku: ' Mimikaki-9 ', qty: 9, actualName: '耳かき 9本セット' });
    const inc = db.prepare("SELECT * FROM pk_pack_incidents WHERE batch_id=? AND kind='excess'").get(pb);
    assert.equal(inc.sku, 'mimikaki-9');
    assert.equal(inc.actual_name, '耳かき 9本セット');
    assert.equal(inc.status, 'candidate');
  });
}
let returnTaskId = null;
t('確定 → 棚戻しタスクに名前 (pk_lines に無い商品は在庫検索由来の名前)・参考ロケなし (location_source NULL)', () => {
  const inc = db.prepare("SELECT id FROM pk_pack_incidents WHERE batch_id=? AND kind='excess'").get(pb);
  const r = resolveIncident(inc.id, 'confirm', '三宅晴菜', pb);
  assert.deepEqual(r.dispatchedTasks.map((x) => [x.kind, x.sku, x.name, x.qty]), [['return', 'mimikaki-9', '耳かき 9本セット', 9]]);
  const task = db.prepare("SELECT * FROM pk_pack_tasks WHERE batch_id=? AND kind='return'").get(pb);
  returnTaskId = task.id;
  assert.equal(task.product_name, '耳かき 9本セット');
  assert.equal(task.location, null);
  assert.equal(task.location_source, null);
  assert.equal(getTaskDetail(task.id).incident_kind, 'excess');
});

console.log('── ② ロジザードの候補を後から参考ロケに ──');
t('setTaskLocationHint: 参考ロケが無ければ入る (source=stock)。もう一度呼んでも上書きしない', () => {
  assert.equal(setTaskLocationHint(returnTaskId, { block: 'P3FA', location: '002-013-03', source: 'stock' }), true);
  let task = taskRow(returnTaskId);
  assert.deepEqual([task.block, task.location, task.location_source], ['P3FA', '002-013-03', 'stock']);
  assert.equal(setTaskLocationHint(returnTaskId, { block: 'P3FB', location: '999-999-99', source: 'stock' }), false);
  task = taskRow(returnTaskId);
  assert.equal(task.location, '002-013-03', '先に入っていたものが残る');
  assert.equal(setTaskLocationHint(returnTaskId, { location: '' }), false, '空は何もしない');
});

console.log('── ③ 棚戻しの完了は「戻したロケ」が必須 ──');
{
  throwsCode(() => applyTaskAction(returnTaskId, 'fulfill', '田中美波', { returnedLocation: '002-013-03' }), 'bad_transition', '未着手 (requested) から直接は完了できない (router が claim を先に入れる)');
  applyTaskAction(returnTaskId, 'claim', '田中美波');
  throwsCode(() => applyTaskAction(returnTaskId, 'fulfill', '田中美波'), 'returned_location_required', 'ロケ無しでは完了できない');
  throwsCode(() => applyTaskAction(returnTaskId, 'fulfill', '田中美波', { returnedLocation: '002 013' }), 'bad_location', '形式不正 (空白) は 400');
  throwsCode(() => applyTaskAction(returnTaskId, 'fulfill', '田中美波', { returnedBlock: 'P3 FA', returnedLocation: '002-013-03' }), 'bad_location', 'ブロックの形式不正も 400');
  t('候補と違う場所へ戻した → 戻したロケ・時刻・人を記録。通知フラグ', () => {
    const u = applyTaskAction(returnTaskId, 'fulfill', '田中美波', { returnedBlock: 'P3FB', returnedLocation: '001-002-03' });
    assert.equal(u.status, 'returned');
    assert.equal(u._notifyReturned, true);
    const task = taskRow(returnTaskId);
    assert.deepEqual([task.returned_block, task.returned_location, task.returned_by], ['P3FB', '001-002-03', '田中美波']);
    assert.ok(task.returned_at, 'returned_at');
    assert.equal(task.location, '002-013-03', '参考ロケ (候補) はそのまま残る = 通知で「候補は…でした」と出せる');
    assert.equal(task.fulfilled_qty, 9);
  });
  t('特殊ロケ (ZZZ-ZZZ-ZZ) もそのまま記録できる', () => {
    ev(pb, 'excess', { sku: 'zzz-item', qty: 1, actualName: '特殊' });
    const inc = db.prepare("SELECT id FROM pk_pack_incidents WHERE batch_id=? AND kind='excess' AND sku='zzz-item'").get(pb);
    resolveIncident(inc.id, 'confirm', '三宅晴菜', pb);
    const id = db.prepare("SELECT id FROM pk_pack_tasks WHERE batch_id=? AND sku='zzz-item'").get(pb).id;
    applyTaskAction(id, 'claim', '田中美波');
    const u = applyTaskAction(id, 'fulfill', '田中美波', { returnedBlock: 'ZZZ', returnedLocation: 'ZZZ-ZZZ-ZZ' });
    assert.deepEqual([u.returned_block, u.returned_location], ['ZZZ', 'ZZZ-ZZZ-ZZ']);
  });
  t('再ピック (repick) の fulfill はロケ無しで従来どおり fulfilled', () => {
    const pb2 = mkPackBatch('R2', [{ sku: 'b-1', status: 'pending' }]);
    ev(pb2, 'shortage', { slipSeq: 1, sku: 'b-1', qty: 1 });
    const inc = db.prepare("SELECT id FROM pk_pack_incidents WHERE batch_id=?").get(pb2);
    resolveIncident(inc.id, 'confirm', '三宅晴菜', pb2);
    const id = db.prepare("SELECT id FROM pk_pack_tasks WHERE batch_id=? AND kind='repick'").get(pb2).id;
    applyTaskAction(id, 'claim', '田中美波');
    const u = applyTaskAction(id, 'fulfill', '田中美波');
    assert.equal(u.status, 'fulfilled');
    assert.equal(u._notifyReturned, undefined);
    assert.equal(u.returned_location, null);
    assert.equal(getWorkState(pb2).repickBySlip[1], 'fulfilled');
  });
}

console.log(`\n${passed} tests passed`);
