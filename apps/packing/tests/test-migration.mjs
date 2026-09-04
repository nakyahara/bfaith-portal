/**
 * マイグレーションの回帰テスト。
 *   node apps/packing/tests/test-migration.mjs
 *
 * ⭐**新規DBのテストでは絶対に見つからない不具合**を捕まえるためのもの。
 *   マイグレーションは一度当たると再実行されないので、既存の番号の定義を後から書き換えると
 *   「新しいDBには列があるのに、当て済みのDBには無い」という食い違いが残る
 *   (2026-09-04: v18 の CREATE TABLE を直したせいで pk_batch_id が足りず、
 *    recordMisses が "no column named pk_batch_id" で落ちる状態になっていた — Codexレビュー)。
 *   ここでは**古い版のDBを手で作ってから**新コードを当て、直っているかを見る。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';

let passed = 0;
function t(name, fn) {
  try { fn(); console.log(`✅ ${name}`); passed++; } catch (e) { console.error(`❌ ${name}\n   ${e.message}`); process.exitCode = 1; }
}

/** 使い捨ての DATA_DIR を用意し、そこに picking.db を作る */
function freshDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'packing-mig-'));
}

/** packing の版数を name の版に見せかけた DB を作る (古い定義のまま) */
function makeOldV18(dir) {
  const db = new Database(path.join(dir, 'picking.db'));
  db.exec('CREATE TABLE IF NOT EXISTS pk_pack_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)');
  // v18 の**旧**定義 = pk_batch_id 列が無い / インデックスが work_date
  db.exec(`CREATE TABLE pk_pack_miss_alerts (
    alert_key   TEXT PRIMARY KEY,
    kind        TEXT NOT NULL,
    work_date   TEXT NOT NULL,
    folder_name TEXT NOT NULL,
    detail      TEXT,
    attempts    INTEGER NOT NULL DEFAULT 0,
    last_error  TEXT,
    notified_at TEXT,
    created_at  TEXT NOT NULL
  )`);
  db.exec('CREATE INDEX idx_pk_pack_miss_alerts_date ON pk_pack_miss_alerts(work_date)');
  db.prepare(`INSERT INTO pk_pack_miss_alerts (alert_key, kind, work_date, folder_name, detail, created_at)
    VALUES ('old:not_imported:1', 'not_imported', '2026-09-01', '出荷_01', '旧v18で入った行', '2026-09-01T00:00:00Z')`).run();
  db.prepare("INSERT INTO pk_pack_meta (key, value) VALUES ('schema_version', '18')").run();
  db.close();
}

// ─── 旧v18 → 最新 ───

const dirOld = freshDir();
makeOldV18(dirOld);
process.env.DATA_DIR = dirOld;
const { initPackingDB, getDB } = await import('../db.js');
const { initPickingDB } = await import('../../picking/db.js');
initPickingDB();
initPackingDB();

t('旧v18 のDBにも pk_batch_id が足される', () => {
  const cols = getDB().prepare('PRAGMA table_info(pk_pack_miss_alerts)').all().map((c) => c.name);
  assert.ok(cols.includes('pk_batch_id'), 'ALTER で列が足される (定義を書き換えただけでは足されない)');
});

t('旧v18 で入っていた行は消えない', () => {
  const r = getDB().prepare("SELECT detail FROM pk_pack_miss_alerts WHERE alert_key = 'old:not_imported:1'").get();
  assert.equal(r?.detail, '旧v18で入った行');
});

t('インデックスが張り替わる', () => {
  const idx = (name) => !!getDB().prepare('SELECT name FROM sqlite_master WHERE type = ? AND name = ?').get('index', name);
  assert.equal(idx('idx_pk_pack_miss_alerts_date'), false, '旧インデックスは消える');
  assert.ok(idx('idx_pk_pack_miss_alerts_pending'), '新インデックスができる');
});

t('列が足りた状態で実際に書き込める', () => {
  // これが今回の不具合の実害 (no column named pk_batch_id)
  getDB().prepare(`INSERT INTO pk_pack_miss_alerts (alert_key, kind, work_date, pk_batch_id, folder_name, detail, attempts, created_at)
    VALUES ('after:not_imported:5', 'not_imported', '2026-09-05', 5, '出荷_05', 'v19適用後', 0, '2026-09-05T00:00:00Z')`).run();
  const r = getDB().prepare("SELECT pk_batch_id FROM pk_pack_miss_alerts WHERE alert_key = 'after:not_imported:5'").get();
  assert.equal(r.pk_batch_id, 5);
});

t('もう一度起動しても壊れない (冪等)', () => {
  initPackingDB();
  const cols = getDB().prepare('PRAGMA table_info(pk_pack_miss_alerts)').all().map((c) => c.name);
  assert.equal(cols.filter((c) => c === 'pk_batch_id').length, 1, '列が二重に足されない');
  assert.equal(getDB().prepare('SELECT COUNT(*) c FROM pk_pack_miss_alerts').get().c, 2);
});

t('picking 側も v13 まで上がり class_source が使える', () => {
  const cols = getDB().prepare('PRAGMA table_info(pk_batches)').all().map((c) => c.name);
  assert.ok(cols.includes('class_source'));
});

console.log(`test-migration: ${passed} 件 pass`);
