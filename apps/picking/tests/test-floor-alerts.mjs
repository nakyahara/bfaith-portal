/** test-floor-alerts.mjs — 現場間アラート (2026-08-21) の検証 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'picking-fa-test-'));
const { initPickingDB, getDB } = await import('../db.js');
const { createFloorAlert, listFloorAlerts, ackFloorAlert, PkError } = await import('../service.js');
initPickingDB();
let passed = 0;
const t = (name, fn) => { fn(); passed++; console.log(`  ok: ${name}`); };

t('発報→相手方向に表示される', () => {
  const r = createFloorAlert('cart', '星');
  assert.equal(r.existed, false);
  const list = listFloorAlerts('to_packing');
  assert.equal(list.length, 1);
  assert.ok(list[0].message.includes('ピッキングカート'));
  assert.equal(listFloorAlerts('to_picking').length, 0, '逆方向には出ない');
});
t('同種の未確認は重ねない (連打集約)', () => {
  assert.equal(createFloorAlert('cart', '倉田').existed, true);
  assert.equal(listFloorAlerts('to_packing').length, 1);
});
t('OKで消える→再発報できる', () => {
  const id = listFloorAlerts('to_packing')[0].id;
  assert.throws(() => ackFloorAlert(id, 'x', 'to_picking'), (e) => e.code === 'not_found', '相手方向からは消せない');
  ackFloorAlert(id, '梱包A', 'to_packing');
  assert.equal(listFloorAlerts('to_packing').length, 0);
  assert.equal(createFloorAlert('cart', '星').existed, false, 'ack後は新規');
  assert.throws(() => ackFloorAlert(9999, 'x', 'to_packing'), (e) => e instanceof PkError && e.code === 'not_found');
});
t('unload は to_picking 方向', () => {
  createFloorAlert('unload', '梱包B');
  const list = listFloorAlerts('to_picking');
  assert.equal(list.length, 1);
  assert.ok(list[0].message.includes('下してください'));
});
t('4時間超は自動で消える', () => {
  getDB().prepare("UPDATE pk_floor_alerts SET created_at = datetime('now', '-5 hours') WHERE acked_at IS NULL").run();
  assert.equal(listFloorAlerts('to_packing').length + listFloorAlerts('to_picking').length, 0);
});
t('不明な種別は拒否', () => {
  assert.throws(() => createFloorAlert('bad', 'x'), (e) => e instanceof PkError && e.code === 'bad_kind');
});
try { fs.rmSync(process.env.DATA_DIR, { recursive: true, force: true }); } catch { /* 無視 */ }
console.log(`\ntest-floor-alerts: ${passed} 件 pass`);

t('repick_done は動的メッセージ・依頼ごとに別バナー', () => {
  const a = createFloorAlert('repick_done', '星', '✅ 出荷_04 #36（依頼: 大場）の不足分のピッキングが完了しました');
  const b = createFloorAlert('repick_done', '星', '✅ 出荷_05 #2（依頼: 倉田）の不足分のピッキングが完了しました');
  assert.equal(a.existed, false);
  assert.equal(b.existed, false, 'メッセージが違えば別バナー');
  const again = createFloorAlert('repick_done', '星', '✅ 出荷_04 #36（依頼: 大場）の不足分のピッキングが完了しました');
  assert.equal(again.existed, true, '同一メッセージは集約');
  assert.equal(listFloorAlerts('to_packing').filter((x) => x.kind === 'repick_done').length, 2);
  assert.throws(() => createFloorAlert('repick_done', '星'), (e) => e.code === 'no_message', 'メッセージ必須');
});
