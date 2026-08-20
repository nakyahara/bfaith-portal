/**
 * picking — フロアボードの PACKING_ENABLED=0 分離テスト。
 *
 * 掲示端末の GET /api/floor が packing の migration を走らせないこと (Codexレビュー high
 * 2026-08-20)。packing を切り離した環境では:
 *   - pk_pack_* テーブルが一切作られない (= DB書き込みが発生しない)
 *   - packing / eta は null、picking 部分だけで応答する
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'picking-floor-off-test-'));
process.env.PACKING_ENABLED = '0';
process.env.PICKING_STATS_MIN_DATE = '2026-08-01';

const { initPickingDB, getDB } = await import('../db.js');
const { getFloorData } = await import('../floor.js');

initPickingDB();   // packing は初期化しない (standalone の PACKING_ENABLED=0 相当)
const db = getDB();

let passed = 0;
function t(name, fn) { fn(); passed++; console.log(`  ok: ${name}`); }

const NOW = new Date('2026-08-20T05:00:00Z');
db.prepare(`
  INSERT INTO pk_batches (id, tb_no, hikiate_class, folder_name, work_date, composition,
    line_count, slip_count, total_qty, status, validity, csv_sha256, imported_by, created_at, updated_at)
  VALUES (1, 'TB1', 'ネコポス手動単品', '出荷_01', '2026-08-20', '単品',
    5, 5, 5, 'ready', 'valid', 'sha', 'test', '2026-08-20T00:00:00Z', '2026-08-20T00:00:00Z')
`).run();

const data = await getFloorData({ now: NOW });

t('packing / eta は null、picking 部分は応答する', () => {
  assert.equal(data.ok, true);
  assert.equal(data.packing, null);
  assert.equal(data.eta, null);
  assert.equal(data.flow.notStarted, 1);
  assert.equal(data.quality.misConfirmed, null);
});

t('pk_pack_* テーブルが作られていない (GETがDB書き込みにならない)', () => {
  const n = db.prepare(
    "SELECT COUNT(*) c FROM sqlite_master WHERE type='table' AND name LIKE 'pk_pack_%'"
  ).get().c;
  assert.equal(n, 0);
});

console.log(`\npicking test-floor-disabled: ${passed} 件 pass`);
