/**
 * picking — 共用端末認証 (pk_devices) と作業者マスタ (pk_workers) のテスト。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'picking-test-'));

const {
  initPickingDB, getDB, createDevice, verifyDevice, revokeDevice, listDevices,
  listWorkers, getWorker, addWorker, setWorkerActive,
} = await import('../db.js');

initPickingDB();

let passed = 0;
function t(name, fn) { fn(); passed++; console.log(`  ok: ${name}`); }

t('端末登録→トークン検証→失効', () => {
  const token = createDevice('倉庫iPhone1', 'admin@b-faith.biz');
  assert.ok(token.length >= 40, 'トークンは十分に長い');
  const dev = verifyDevice(token);
  assert.equal(dev.label, '倉庫iPhone1');
  assert.equal(verifyDevice('wrong-token'), null);
  assert.equal(verifyDevice(''), null);
  assert.equal(verifyDevice(null), null);

  assert.equal(revokeDevice(dev.id), true);
  assert.equal(verifyDevice(token), null, '失効後は使えない');
  assert.equal(revokeDevice(dev.id), false, '二重失効はfalse');
  const listed = listDevices().find((d) => d.id === dev.id);
  assert.ok(listed.revoked_at);
});

t('トークンは平文で保存されない (ハッシュのみ)', () => {
  const token = createDevice('倉庫iPhone2', 'admin@b-faith.biz');
  const rows = getDB().prepare('SELECT token_hash FROM pk_devices').all();
  assert.ok(rows.every((r) => r.token_hash !== token));
});

t('作業者: 追加→有効一覧→無効化', () => {
  const c1 = addWorker('星');
  const c2 = addWorker('三宅');
  assert.ok(/^w\d{2}$/.test(c1));
  assert.notEqual(c1, c2);
  assert.deepEqual(listWorkers().map((w) => w.name), ['星', '三宅']);
  assert.equal(getWorker(c1).name, '星');

  setWorkerActive(c1, false);
  assert.deepEqual(listWorkers().map((w) => w.name), ['三宅'], '無効化は一覧から消える');
  assert.equal(getWorker(c1).active, 0, 'getWorkerでは引ける (検証用)');
  setWorkerActive(c1, true);
  assert.equal(listWorkers().length, 2);
});

console.log(`\ntest-device: ${passed} 件 pass`);
