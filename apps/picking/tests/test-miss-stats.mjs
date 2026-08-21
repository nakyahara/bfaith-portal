/** test-miss-stats.mjs — ピッキングミス集計 (2026-08-21) の検証 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'picking-miss-test-'));
const { initPickingDB, getDB, jstToday } = await import('../db.js');
const { getMissStats } = await import('../service.js');
initPickingDB();
const db = getDB();
let passed = 0;
const t = (name, fn) => { fn(); passed++; console.log(`  ok: ${name}`); };

t('packing未初期化 (表なし) でも空で返る', () => {
  const m = getMissStats({});
  assert.equal(m.total.total, 0);
});

// packing所有の pk_pack_incidents を試験用に用意
db.exec(`CREATE TABLE pk_pack_incidents (
  id INTEGER PRIMARY KEY, batch_id INTEGER, slip_seq INTEGER, kind TEXT, sku TEXT, actual_sku TEXT,
  qty INTEGER, status TEXT, attributed_worker TEXT, detected_by TEXT, confirmed_by TEXT,
  created_at TEXT, updated_at TEXT)`);
const now = new Date().toISOString().slice(0, 19) + 'Z';
const old = new Date(Date.now() - 40 * 864e5).toISOString().slice(0, 19) + 'Z';
const ins = db.prepare(`INSERT INTO pk_pack_incidents (kind, sku, qty, status, attributed_worker, detected_by, created_at, updated_at)
  VALUES (?, 'sku1', ?, ?, ?, '梱包A', ?, ?)`);
ins.run('shortage', 1, 'confirmed', '星', now, now);
ins.run('shortage', 2, 'confirmed', '星', now, now);
ins.run('wrong_item', 1, 'confirmed', '星', now, now);
ins.run('excess', 1, 'confirmed', '倉田', now, now);
ins.run('shortage', 1, 'withdrawn', '星', now, now);       // 取下げ=数えない
ins.run('shortage', 1, 'candidate', '星', now, now);        // 未確定=数えない
ins.run('shortage', 1, 'confirmed', '星', old, old);        // 窓外=数えない
ins.run('shortage', 1, 'confirmed', null, now, now);        // 帰責不明

t('確定分のみ・作業者×種別で集計 (取下げ/未確定/窓外は除外)', () => {
  const m = getMissStats({ until: jstToday(), days: 30 });
  const hoshi = m.byWorker.find((w) => w.worker === '星');
  assert.deepEqual([hoshi.total, hoshi.shortage, hoshi.wrong_item, hoshi.qty], [3, 2, 1, 4]);
  const kurata = m.byWorker.find((w) => w.worker === '倉田');
  assert.deepEqual([kurata.total, kurata.excess], [1, 1]);
  assert.ok(m.byWorker.find((w) => w.worker === '(担当不明)'), '帰責不明も見える');
  assert.equal(m.total.total, 5);
  assert.equal(m.byWorker[0].worker, '星', '件数降順');
});

try { fs.rmSync(process.env.DATA_DIR, { recursive: true, force: true }); } catch { /* 無視 */ }
console.log(`\ntest-miss-stats: ${passed} 件 pass`);
