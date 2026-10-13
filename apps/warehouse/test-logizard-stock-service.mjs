import { temporaryTestRoot } from '../../scripts/test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
/**
 * test-logizard-stock-service.mjs — GET /service-api/logizard-stock/locations が「在庫を取った時刻」(sourceAt) を返す
 *
 * ピッキングの他ロケ候補は「写しの後に取り込んだピッキング」を引く (PR #1697)。境目は在庫を取った時刻でないと、
 * 取りに行ってから取り込み完了までの数分に取り込んだピッキングを取りこぼす (Codex R1 High)。
 *   - logizard_source_for が今の取り込み (logizard_last_import) と同じときだけ sourceAt を返す
 *   - 素性が別の回・空のときは null (ピッキング側は importedAt に戻る)
 *
 * 実行: node apps/warehouse/test-logizard-stock-service.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import express from 'express';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'lz-stock-service-test-'));
const { initDB, getDB, updateSyncMeta } = await import('./db.js');
await initDB();
const router = (await import('./logizard-stock-service.js')).default;

const db = getDB();
db.prepare(`INSERT INTO raw_lz_inventory (商品ID, 商品名, ブロック略称, ロケ, 品質区分名, 在庫数, 引当数, 在庫日)
  VALUES ('hiragushi240', '平串', 'P3FC', '002-003-05', '良品', 1, 0, '20261012')`).run();

const app = express();
app.use('/l', router);
const server = app.listen(0, '127.0.0.1');
await new Promise((r) => server.once('listening', r));
const get = async () => (await fetch(`http://127.0.0.1:${server.address().port}/l/locations?code=hiragushi240`)).json();

let passed = 0;
try {
  updateSyncMeta('logizard_last_import', '2026-10-12T05:04:00.000Z');
  updateSyncMeta('logizard_source_at', '2026-10-12T05:00:00.000Z');
  updateSyncMeta('logizard_source_for', '2026-10-12T05:04:00.000Z');
  let r = await get();
  assert.equal(r.ok, true);
  assert.equal(r.importedAt, '2026-10-12T05:04:00.000Z');
  assert.equal(r.sourceAt, '2026-10-12T05:00:00.000Z');
  assert.equal(r.locations[0].free, 1);
  passed++; console.log('  ok: 今の取り込みの「在庫を取った時刻」を sourceAt で返す');

  updateSyncMeta('logizard_last_import', '2026-10-12T06:04:00.000Z');   // 次の回の取り込み (素性は前の回のまま)
  r = await get();
  assert.equal(r.sourceAt, null);
  updateSyncMeta('logizard_source_for', '2026-10-12T06:04:00.000Z');
  updateSyncMeta('logizard_source_at', '');
  r = await get();
  assert.equal(r.sourceAt, null);
  passed++; console.log('  ok: 素性が別の回・空のときは sourceAt=null');
} finally {
  server.close();
}
console.log(`\ntest-logizard-stock-service: ${passed} passed`);
