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
process.env.PACKING_WEBHOOK_TIMEOUT_MS = '200';   // 末尾のタイムアウト検証用 (notify.js が起動時に読む)

const { initPickingDB } = await import('../../picking/db.js');
const { initPackingDB, getDB, utcNow, jstToday } = await import('../db.js');
const {
  applyEvent, applyTaskAction, resolveIncident, setTaskLocationHint, getTaskDetail, getWorkState,
  fulfillReturnTask, claimReturnedNotify, markReturnedNotify, listPendingReturnedNotifies, countStaleReturnedNotifies,
} = await import('../service.js');

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

console.log('── ③ 棚戻しの完了は「戻したロケ」が必須 (fulfillReturnTask に一本化) ──');
{
  throwsCode(() => applyTaskAction(returnTaskId, 'fulfill', '田中美波'), 'bad_transition', '未着手 (requested) から applyTaskAction では完了できない');
  applyTaskAction(returnTaskId, 'claim', '田中美波');
  throwsCode(() => applyTaskAction(returnTaskId, 'fulfill', '田中美波'), 'return_fulfill_requires_location_flow', '棚戻しは applyTaskAction(fulfill) を拒否 (二経路を残さない — Codex R2)');
  throwsCode(() => fulfillReturnTask(returnTaskId, '田中美波'), 'returned_location_required', 'ロケ無しでは完了できない');
  throwsCode(() => fulfillReturnTask(returnTaskId, '田中美波', { returnedLocation: '002 013' }), 'bad_location', '形式不正 (空白) は 400');
  throwsCode(() => fulfillReturnTask(returnTaskId, '田中美波', { returnedBlock: 'P3 FA', returnedLocation: '002-013-03' }), 'bad_location', 'ブロックの形式不正も 400');
  t('候補と違う場所へ戻した → 戻したロケ・時刻・人・出どころを記録。通知フラグ', () => {
    const u = fulfillReturnTask(returnTaskId, '田中美波', { returnedBlock: 'P3FB', returnedLocation: '001-002-03', returnedSource: 'manual' });
    assert.equal(u.status, 'returned');
    assert.equal(u._notifyReturned, true);
    const task = taskRow(returnTaskId);
    assert.deepEqual([task.returned_block, task.returned_location, task.returned_by, task.returned_source], ['P3FB', '001-002-03', '田中美波', 'manual']);
    assert.ok(task.returned_at, 'returned_at');
    assert.equal(task.location, '002-013-03', '参考ロケ (候補) はそのまま残る = 通知で「候補は…でした」と出せる');
    assert.equal(task.fulfilled_qty, 9);
  });
  t('特殊ロケ (ZZZ-ZZZ-ZZ) もそのまま記録できる', () => {
    ev(pb, 'excess', { sku: 'zzz-item', qty: 1, actualName: '特殊' });
    const inc = db.prepare("SELECT id FROM pk_pack_incidents WHERE batch_id=? AND kind='excess' AND sku='zzz-item'").get(pb);
    resolveIncident(inc.id, 'confirm', '三宅晴菜', pb);
    const id = db.prepare("SELECT id FROM pk_pack_tasks WHERE batch_id=? AND sku='zzz-item'").get(pb).id;
    const u = fulfillReturnTask(id, '田中美波', { returnedBlock: 'ZZZ', returnedLocation: 'ZZZ-ZZZ-ZZ' });
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

console.log('── Codex R1: 棚戻しの完了は1トランザクション (fulfillReturnTask) ──');
{
  const pb3 = mkPackBatch('R3', [{ sku: 'c-1', status: 'done' }]);
  const mkReturn = (sku) => {
    ev(pb3, 'excess', { sku, qty: 2, actualName: `名前${sku}` });
    const inc = db.prepare("SELECT id FROM pk_pack_incidents WHERE batch_id=? AND kind='excess' AND sku=?").get(pb3, sku);
    resolveIncident(inc.id, 'confirm', '三宅晴菜', pb3);
    return db.prepare("SELECT id FROM pk_pack_tasks WHERE batch_id=? AND sku=?").get(pb3, sku).id;
  };
  const id1 = mkReturn('ret-a');
  throwsCode(() => fulfillReturnTask(id1, '田中美波', { returnedLocation: '' }), 'returned_location_required', 'ロケ無しは 400');
  throwsCode(() => fulfillReturnTask(id1, '田中美波', { returnedLocation: '002 013' }), 'bad_location', 'ロケ不正は 400');
  t('ロケ不正のときは requested のまま (対応中だけ残らない)', () => {
    assert.equal(taskRow(id1).status, 'requested');
    assert.equal(taskRow(id1).claimed_by, null);
  });
  t('requested から1回で returned (担当も入る)。通知 outbox は未送信', () => {
    const u = fulfillReturnTask(id1, '田中美波', { returnedBlock: 'P3FA', returnedLocation: '002-013-03', returnedSource: 'stock' });
    assert.equal(u.status, 'returned');
    assert.equal(u._notifyReturned, true);
    const row = taskRow(id1);
    assert.deepEqual([row.claimed_by, row.returned_by, row.returned_source, row.returned_notified_at], ['田中美波', '田中美波', 'stock', null]);
    // 先に applyTaskAction で戻した分 (上のテスト) も未通知なので一緒に拾われる = ポーラーが追いつく対象
    assert.ok(listPendingReturnedNotifies(10).map((r) => r.id).includes(id1), '未通知として拾える');
  });
  t('同じ作業者・同じロケの再送は冪等成功 (replayed)。別人・別ロケは 409', () => {
    const r = fulfillReturnTask(id1, '田中美波', { returnedBlock: 'P3FA', returnedLocation: '002-013-03' });
    assert.equal(r._replayed, true);
    assert.equal(r._notifyReturned, undefined, '再送では通知フラグを立てない');
  });
  throwsCode(() => fulfillReturnTask(id1, '有國陽', { returnedBlock: 'P3FA', returnedLocation: '002-013-03' }), 'already_returned', '別の人の再完了は 409');
  throwsCode(() => fulfillReturnTask(id1, '田中美波', { returnedBlock: 'P3FB', returnedLocation: '001-001-01' }), 'already_returned', '別ロケへの再完了は 409');
  t('通知 outbox: claim は1回だけ通る → mark sent で未通知から消える', () => {
    assert.equal(claimReturnedNotify(id1), true);
    assert.equal(claimReturnedNotify(id1), false, '送信中は他が取れない');
    markReturnedNotify(id1, false, 'HTTP 500');
    assert.equal(taskRow(id1).returned_notify_error, 'HTTP 500');
    assert.equal(claimReturnedNotify(id1), true, '失敗後は再送のために取り直せる');
    markReturnedNotify(id1, true);
    assert.ok(taskRow(id1).returned_notified_at);
    assert.ok(!listPendingReturnedNotifies(10).map((r) => r.id).includes(id1), '通知済みは未通知から消える');
    assert.equal(countStaleReturnedNotifies(), 0);
  });
  const id2 = mkReturn('ret-b');
  t('取下げ (cancel) 後の完了は 409', () => {
    applyTaskAction(id2, 'cancel', '三宅晴菜');
    assert.throws(() => fulfillReturnTask(id2, '田中美波', { returnedLocation: '002-013-03' }), (e) => e.code === 'bad_transition');
  });
  t('claimed からも1回で returned (source 未指定は manual)', () => {
    const id3 = mkReturn('ret-c');
    applyTaskAction(id3, 'claim', '有國陽');
    const u = fulfillReturnTask(id3, '田中美波', { returnedLocation: 'ZZZ-ZZZ-ZZ' });
    assert.deepEqual([u.status, u.claimed_by, u.returned_by, u.returned_source], ['returned', '有國陽', '田中美波', 'manual']);
  });
}

console.log('── Codex R1: デプロイ前の自由入力の余り候補は確定できない ──');
{
  const pb4 = mkPackBatch('R4', [{ sku: 'd-1', status: 'done' }]);
  // 旧データを再現: 検証を通らない SKU (商品名の断片) の候補が直接 DB にある
  const incId = Number(db.prepare(`INSERT INTO pk_pack_incidents (batch_id, slip_seq, kind, sku, qty, status, detected_by, created_at, updated_at)
    VALUES (?, NULL, 'excess', '耳かき', 9, 'candidate', '三宅晴菜', ?, ?)`).run(pb4, now, now).lastInsertRowid);
  throwsCode(() => resolveIncident(incId, 'confirm', '三宅晴菜', pb4), 'bad_sku', '「耳かき」のままの確定は 400 (取り下げて選び直す)');
  t('router が在庫検索で確定した SKU/名前を渡せば、候補を直してから棚戻しにする', () => {
    const r = resolveIncident(incId, 'confirm', '三宅晴菜', pb4, { excessSku: 'mimikaki-9', excessName: '耳かき 9本セット' });
    assert.deepEqual(r.dispatchedTasks.map((x) => [x.sku, x.name]), [['mimikaki-9', '耳かき 9本セット']]);
    assert.equal(db.prepare('SELECT sku FROM pk_pack_incidents WHERE id=?').get(incId).sku, 'mimikaki-9');
  });
  t('取下げ (withdraw) は旧データでもできる', () => {
    const id2 = Number(db.prepare(`INSERT INTO pk_pack_incidents (batch_id, slip_seq, kind, sku, qty, status, detected_by, created_at, updated_at)
      VALUES (?, NULL, 'excess', 'レギュラー', 1, 'candidate', '三宅晴菜', ?, ?)`).run(pb4, now, now).lastInsertRowid);
    assert.equal(resolveIncident(id2, 'withdraw', '三宅晴菜', pb4).status, 'withdrawn');
  });
}

console.log('── Codex R2: webhook が応答しなくてもタイムアウトで戻る (ポーラーを止めない) ──');
{
  const { notifyReturned } = await import('../notify.js');
  process.env.PACKING_TASK_WEBHOOK = 'http://webhook.test/x';
  const origFetch = globalThis.fetch;
  // 応答しない webhook: abort されるまで永遠に待つ
  globalThis.fetch = (url, opts) => new Promise((_, reject) => {
    opts.signal.addEventListener('abort', () => reject(opts.signal.reason || new Error('aborted')));
  });
  const t0 = Date.now();
  // AbortSignal.timeout のタイマーは unref なので、テストでは event loop を生かしておく (本番はサーバーが生きている)
  const keepAlive = setInterval(() => {}, 50);
  try {
    await assert.rejects(notifyReturned({ id: 1, sku: 'x', req_qty: 1, returned_location: '001-001-01', returned_by: 'A' }, 'A'));
  } finally { clearInterval(keepAlive); }
  assert.ok(Date.now() - t0 < 5000, `タイムアウトで戻る (${Date.now() - t0}ms)`);
  passed++; console.log('  ok: 応答しない webhook はタイムアウトで reject (再送はポーラーが markReturnedNotify(false) で拾う)');
  globalThis.fetch = async () => ({ ok: true, status: 200, json: async () => ({}) });
  assert.equal(await notifyReturned({ id: 1, sku: 'x', req_qty: 1, returned_location: '001-001-01', returned_by: 'A' }, null, { retry: true }), true);
  passed++; console.log('  ok: 再送 (retry) も送れる');
  globalThis.fetch = origFetch;
}

console.log(`\n${passed} tests passed`);
