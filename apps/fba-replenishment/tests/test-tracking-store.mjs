/**
 * tracking-store.js / tracking-service.js の純粋関数のテスト。
 * DATA_DIR を一時ディレクトリに向けるので本番データに触らない。
 *   node apps/fba-replenishment/tests/test-tracking-store.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'fba-tracking-test-'));

const store = await import('../tracking-store.js');
const { checkDeadline } = await import('../tracking-service.js');

let pass = 0;
const t = (name, fn) => { fn(); pass++; console.log(`  ok  ${name}`); };

console.log('tracking-store / deadline');

t('記録が無ければ未投入', () => {
  assert.equal(store.findSuccess('FBA-NONE'), null);
  assert.equal(store.isSameAsRecorded('FBA-NONE', [{ boxId: 'B1', trackingId: '111' }]), false);
});

t('成功を追記すると投入済みになる', () => {
  store.append({
    runId: 'r1', shipmentConfirmationId: 'FBA-A', shipmentId: 'sh-a', inboundPlanId: 'wf-a',
    fcCode: 'HND2', matchedBy: '納品番号', result: 'success',
    items: [{ boxId: 'B1', trackingId: '66379134975' }, { boxId: 'B2', trackingId: '66379134986' }],
  });
  const rec = store.findSuccess('FBA-A');
  assert.ok(rec);
  assert.equal(rec.fcCode, 'HND2');
  assert.ok(rec.at, '追記時刻が入る');
});

t('🚨同じ内容の再投入は同値と判定される (二重投入を止める)', () => {
  const same = [{ boxId: 'B1', trackingId: '66379134975' }, { boxId: 'B2', trackingId: '66379134986' }];
  assert.equal(store.isSameAsRecorded('FBA-A', same), true);
});

t('並びが違えば同値ではない (箱と番号の対応が変わっている)', () => {
  const swapped = [{ boxId: 'B1', trackingId: '66379134986' }, { boxId: 'B2', trackingId: '66379134975' }];
  assert.equal(store.isSameAsRecorded('FBA-A', swapped), false);
});

t('失敗記録は「投入済み」にしない (再実行できる)', () => {
  store.append({ runId: 'r2', shipmentConfirmationId: 'FBA-B', result: 'failed', error: '期限切れ', items: [] });
  assert.equal(store.findSuccess('FBA-B'), null);
});

t('同じCSV(ハッシュ)を二度処理しない', () => {
  store.append({ runId: 'r3', shipmentConfirmationId: 'FBA-C', result: 'success', sourceHash: 'abc123', items: [{ boxId: 'X', trackingId: '12345678' }] });
  assert.ok(store.findBySourceHash('abc123'));
  assert.equal(store.findBySourceHash('zzz'), null);
  assert.equal(store.findBySourceHash(null), null);
});

t('壊れた行があっても他の記録は読める (追記専用の破損耐性)', () => {
  fs.appendFileSync(store.storePath(), '{壊れたJSON\n', 'utf-8');
  const { entries, brokenLines } = store.readAll();
  assert.equal(brokenLines.length, 1);
  assert.ok(entries.length >= 3);
  assert.ok(store.findSuccess('FBA-A'), '壊れた行の後でも既存記録は生きている');
});

// ── 期限チェック ──────────────────────────────────────────
const shipment = (rtsEnd, editableUntil) => ({
  dates: { readyToShipWindow: { start: rtsEnd, end: rtsEnd } },
  selectedDeliveryWindow: { editableUntil },
});

t('22:00 JST の実行は期限内 (当日23:59 JST / 翌日09:00 JST より前)', () => {
  // 2026-08-07 22:00 JST = 13:00Z
  const now = new Date('2026-08-07T13:00:00Z');
  const r = checkDeadline(shipment('2026-08-07T14:59Z', '2026-08-08T00:00Z'), now);
  assert.equal(r.ok, true);
  assert.equal(r.expired.length, 0);
});

t('🚨翌日に回すと期限切れ (8/7の失敗を再現)', () => {
  // 2026-08-07 13:00 JST = 04:00Z、対象は8/6出荷分
  const now = new Date('2026-08-07T04:00:00Z');
  const r = checkDeadline(shipment('2026-08-06T14:59Z', '2026-08-07T00:00Z'), now);
  assert.equal(r.ok, false);
  assert.equal(r.expired.length, 2);
  assert.match(r.note, /Seller Central画面からは入力できます/);
});

t('⭐当日23:59を過ぎても editableUntil が残っていれば送ってみる (可否の判断はAmazon側)', () => {
  const now = new Date('2026-08-07T15:00:00Z'); // 8/8 00:00 JST
  const r = checkDeadline(shipment('2026-08-07T14:59Z', '2026-08-08T00:00Z'), now);
  assert.equal(r.ok, true, 'どちらの期限が効くかは未実測。緩い側に倒して人手に回さない');
  assert.equal(r.expired.length, 1);
});

t('🚨すべての期限を過ぎたら期限切れ', () => {
  const now = new Date('2026-08-08T01:00:00Z'); // 8/8 10:00 JST
  const r = checkDeadline(shipment('2026-08-07T14:59Z', '2026-08-08T00:00Z'), now);
  assert.equal(r.ok, false);
  assert.equal(r.expired.length, 2);
  assert.match(r.note, /Seller Central画面からは入力できます/);
});

t('期限の情報が無いときは止めない (取れないだけで無効ではない)', () => {
  const r = checkDeadline({}, new Date());
  assert.equal(r.ok, true);
  assert.match(r.note, /期限の情報が取れませんでした/);
});

// ── #1 二重投入の防止 ────────────────────────────────
t('🚨pending が最後に残っていたら「結果不明」として扱う (自動で送り直さない)', () => {
  store.append({ runId: 'r9', shipmentConfirmationId: 'FBA-P', result: 'pending', items: [{ boxId: 'B1', trackingId: '12345678' }] });
  const latest = store.findLatest('FBA-P');
  assert.equal(latest.result, 'pending');
  assert.equal(store.findSuccess('FBA-P'), null, 'pendingを成功扱いしない');
});

t('pending のあとに success を書けば解消する', () => {
  store.append({ runId: 'r9', shipmentConfirmationId: 'FBA-P', result: 'success', items: [{ boxId: 'B1', trackingId: '12345678' }] });
  assert.equal(store.findLatest('FBA-P').result, 'success');
  assert.ok(store.findSuccess('FBA-P'));
});

t('🚨ロックが取れている間は二重起動できない', () => {
  assert.equal(store.acquireLock('run-1').ok, true);
  const second = store.acquireLock('run-2');
  assert.equal(second.ok, false);
  assert.match(second.reason, /別の実行が動いています/);
  store.releaseLock('run-1');
  assert.equal(store.acquireLock('run-3').ok, true, '解放後は取れる');
  store.releaseLock('run-3');
});

t('🚨自分が取っていないロックは消さない (他プロセスのロックを壊さない)', () => {
  assert.equal(store.acquireLock('owner').ok, true);
  store.releaseLock('someone-else');                       // 他人のつもりで解放を試みる
  assert.equal(store.acquireLock('intruder').ok, false, 'ロックは残っているべき');
  store.releaseLock('owner');
  assert.equal(store.acquireLock('next').ok, true);
  store.releaseLock('next');
});

fs.rmSync(process.env.DATA_DIR, { recursive: true, force: true });
console.log(`\n${pass} 件すべて通過`);
