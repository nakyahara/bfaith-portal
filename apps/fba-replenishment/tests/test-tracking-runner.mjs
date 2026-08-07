/**
 * tracking-runner.js の純粋関数 (JST日付 / 通知本文) のテスト。
 *   node apps/fba-replenishment/tests/test-tracking-runner.mjs
 */
import assert from 'node:assert/strict';
import { jstYmd, formatSummary } from '../tracking-runner.js';

let pass = 0;
const t = (name, fn) => { fn(); pass++; console.log(`  ok  ${name}`); };
console.log('tracking-runner');

t('JST日付: UTC深夜でも日本の日付になる (toISOStringの罠)', () => {
  assert.equal(jstYmd(new Date('2026-08-07T13:00:00Z')), '20260807'); // 22:00 JST
  assert.equal(jstYmd(new Date('2026-08-07T15:30:00Z')), '20260808'); // 翌日 00:30 JST
  assert.equal(jstYmd(new Date('2026-08-06T23:00:00Z')), '20260807');
});

const base = { runId: 'r', commit: true, expectYmd: '20260807', blocked: [], excluded: [], skipped: [], registered: [], failed: [], note: [] };

t('中断時は「登録は一切していません」と明記する', () => {
  const s = formatSummary({ ...base, blocked: ['個口合計 5 と輸送箱 6 が一致しません'] });
  assert.match(s, /🚨/);
  assert.match(s, /登録は一切していません/);
  assert.match(s, /個口合計 5 と輸送箱 6/);
  assert.match(s, /翌日 Seller Central 画面から手入力/);
});

t('成功時は納品ごとに箱数と照合方法を出す', () => {
  const s = formatSummary({ ...base, registered: [{ shipmentConfirmationId: 'FBA15GGL5J2X', fcCode: 'HND2', countBoxes: 5, matchedBy: '納品番号' }] });
  assert.match(s, /✅/);
  assert.match(s, /FBA15GGL5J2X \(HND2\) 5箱  照合=納品番号/);
});

t('期限切れは「画面から手入力してください」を添える', () => {
  const s = formatSummary({ ...base, failed: [{ shipmentConfirmationId: 'FBA-A', error: '編集期限を過ぎています', needsManual: true }] });
  assert.match(s, /⚠️/);
  assert.match(s, /画面から手入力してください/);
});

t('🚨除外は黙って捨てず件数を必ず出す', () => {
  const s = formatSummary({ ...base, excluded: [{ fcCode: 'AMRC', 件数: 4, reason: 'FBA以外' }] });
  assert.match(s, /除外 4件/);
  assert.match(s, /AMRC/);
});

t('プレビューはタイトルで分かる', () => {
  assert.match(formatSummary({ ...base, commit: false }), /追跡番号\(プレビュー\)/);
  assert.doesNotMatch(formatSummary({ ...base, commit: true }), /プレビュー/);
});

console.log(`\n${pass} 件すべて通過`);
