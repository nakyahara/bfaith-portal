/**
 * test-return-candidates.mjs — ↩ 棚戻しの戻し先候補 (例外処理監査 PR-4・D-2)
 *
 * 守りたいこと:
 *   ① 取った場所 (task.location) が先頭 (picked)。ロジザードの候補は良品・フリー在庫の多い順で、取った場所と同じロケは重複させない
 *   ② 在庫参照が未設定なら候補なし (configured=false) でも落ちない。取得失敗も fetched=false で落ちない
 *   ③ 参考ロケがロジザード候補由来 (location_source='stock') のときは source で区別できる
 *
 * 実行: node apps/picking/tests/test-return-candidates.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'picking-return-cand-test-'));
delete process.env.WAREHOUSE_URL;
delete process.env.WAREHOUSE_SERVICE_TOKEN;

const { initPickingDB } = await import('../db.js');
const { returnCandidates } = await import('../service.js');
initPickingDB();

let passed = 0;
async function t(name, fn) { await fn(); passed++; console.log(`  ok: ${name}`); }

const task = { id: 1, sku: 'mimikaki-9', location: '002-013-03', block: 'P3FA', location_source: 'picked' };
const stock = (locations) => async () => ({ ok: true, status: 200, json: async () => ({ ok: true, importedAt: new Date().toISOString(), stockDate: '20260906', name: '耳かき', locations }) });

await t('② 在庫参照が未設定: 取った場所だけ (configured=false・落ちない)', async () => {
  const c = await returnCandidates(task, { fetchFn: async () => { throw new Error('must not be called'); } });
  assert.equal(c.configured, false);
  assert.deepEqual(c.picked, { block: 'P3FA', location: '002-013-03', label: 'P3FA-002-013-03', source: 'picked' });
  assert.deepEqual(c.rows, []);
  assert.equal(c.fetched, false);
});

process.env.WAREHOUSE_URL = 'http://warehouse.test';
process.env.WAREHOUSE_SERVICE_TOKEN = 'x';

await t('① 取った場所が先頭・候補はフリー在庫の多い順・同じロケは重複しない・不良品と free 0 は出ない', async () => {
  const c = await returnCandidates(task, {
    fetchFn: stock([
      { block: 'P3FA', location: '002-013-03', quality: '良品', free: 5, allocated: 0, expiry: '20270101' },   // 取った場所 = 重複させない
      { block: 'P3FB', location: '001-002-03', quality: '良品', free: 30, allocated: 2, expiry: '20261231' },
      { block: 'P3FB', location: '001-002-03', quality: '良品', free: 10, allocated: 0, expiry: '20270301' },   // 同一ロケの別ロット → まとめる
      { block: 'P3FA', location: '005-001-01', quality: '不良品', free: 99, allocated: 0, expiry: null },
      { block: 'P3FA', location: '006-001-01', quality: '良品', free: 0, allocated: 0, expiry: null },
      { block: 'ZZZ', location: 'ZZZ-ZZZ-ZZ', quality: '良品', free: 3, allocated: 0, expiry: null },
      { block: 'P3FC', location: '003-003-03', quality: '良品', free: 8, allocated: 0, expiry: null },
    ]),
  });
  assert.equal(c.configured, true);
  assert.equal(c.fetched, true);
  assert.equal(c.picked.label, 'P3FA-002-013-03');
  assert.deepEqual(c.rows.map((r) => [r.label, r.free]), [['P3FB-001-002-03', 40], ['P3FC-003-003-03', 8], ['ZZZ-ZZZ-ZZ', 3]]);
  assert.equal(c.rows[0].expiry, '2026/12/31', '同一ロケは期限の近い方');
});

await t('③ 参考ロケがロジザード候補由来 (stock) なら source=stock。取った場所が無ければ picked=null', async () => {
  const c = await returnCandidates({ ...task, location_source: 'stock' }, { fetchFn: stock([]) });
  assert.equal(c.picked.source, 'stock');
  const c2 = await returnCandidates({ id: 2, sku: 'kofunneil-0837', location: null, block: null }, { fetchFn: stock([{ block: 'P3FA', location: '001-001-01', quality: '良品', free: 1, allocated: 0 }]) });
  assert.equal(c2.picked, null);
  assert.deepEqual(c2.rows.map((r) => r.label), ['P3FA-001-001-01']);
});

await t('② 取得失敗 (HTTP 500 / 例外) は fetched=false で落ちない', async () => {
  const c = await returnCandidates(task, { fetchFn: async () => ({ ok: false, status: 500 }) });
  assert.equal(c.fetched, false);
  assert.deepEqual(c.rows, []);
  const c2 = await returnCandidates(task, { fetchFn: async () => { throw new Error('boom'); } });
  assert.equal(c2.fetched, false);
});

await t('候補は最大 8 ロケ (超過分は truncated に件数)', async () => {
  const many = Array.from({ length: 12 }, (_, i) => ({ block: 'P3FB', location: `00${i}-001-01`, quality: '良品', free: 12 - i, allocated: 0 }));
  const c = await returnCandidates({ id: 3, sku: 'x', location: null, block: null }, { fetchFn: stock(many) });
  assert.equal(c.rows.length, 8);
  assert.equal(c.truncated, 4);
});

console.log(`\n${passed} tests passed`);
