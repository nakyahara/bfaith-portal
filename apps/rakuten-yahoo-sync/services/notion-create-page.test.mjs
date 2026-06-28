/**
 * notion-create-page の claim CAS + lease 管理 test (Codex Phase E-14 R7 stop)。
 * 実 Notion API は呼ばないので、 createPage / findPageByManageNumber を mock する。
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import Db from 'better-sqlite3';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

function loadMigration(db, fileName) {
  const p = path.join(__dirname, '..', 'db', 'migrations', fileName);
  db.exec(fs.readFileSync(p, 'utf8'));
}

function setupDb() {
  const db = new Db(':memory:');
  // 必要 migrations を適用 (依存順)
  loadMigration(db, '001_initial.sql');
  loadMigration(db, '003_publish_idempotency.sql');
  loadMigration(db, '005_yahoo_baseline.sql');
  loadMigration(db, '006_migration_candidates.sql');
  loadMigration(db, '017_notion_page_create_requests.sql');
  return db;
}

test('migration 017 creates notion_page_create_requests with CAS-friendly schema', () => {
  const db = setupDb();
  const cols = db.prepare('PRAGMA table_info(notion_page_create_requests)').all().map((c) => c.name);
  assert.ok(cols.includes('rakuten_manage_number'));
  assert.ok(cols.includes('status'));
  assert.ok(cols.includes('lease_token'));
  assert.ok(cols.includes('lease_expires_at'));
  assert.ok(cols.includes('notion_page_id'));
});

// claim CAS — INSERT は新規 / 既存 'failed' は奪取可能 / 'completed' は弾く
test('claim CAS: 新規 INSERT で running に', () => {
  const db = setupDb();
  const r = db.prepare(`
    INSERT INTO notion_page_create_requests (rakuten_manage_number, status, lease_token, lease_expires_at, attempt)
    VALUES (?, 'running', ?, ?, 1)
    ON CONFLICT(rakuten_manage_number) DO UPDATE SET status = 'running'
    WHERE status = 'failed' OR (status = 'running' AND lease_expires_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
  `).run('sku_a', 'token_1', new Date(Date.now() + 60_000).toISOString());
  assert.equal(r.changes, 1);
  const row = db.prepare(`SELECT * FROM notion_page_create_requests WHERE rakuten_manage_number = ?`).get('sku_a');
  assert.equal(row.status, 'running');
  assert.equal(row.lease_token, 'token_1');
});

test('claim CAS: status=completed は奪取不能', () => {
  const db = setupDb();
  db.prepare(`
    INSERT INTO notion_page_create_requests (rakuten_manage_number, status, lease_token, notion_page_id, attempt)
    VALUES ('sku_b', 'completed', 'old_token', 'notion_page_xyz', 1)
  `).run();
  const r = db.prepare(`
    INSERT INTO notion_page_create_requests (rakuten_manage_number, status, lease_token, lease_expires_at, attempt)
    VALUES (?, 'running', ?, ?, 1)
    ON CONFLICT(rakuten_manage_number) DO UPDATE SET status = 'running', lease_token = excluded.lease_token
    WHERE status = 'failed' OR (status = 'running' AND lease_expires_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
  `).run('sku_b', 'new_token', new Date(Date.now() + 60_000).toISOString());
  assert.equal(r.changes, 0);
  const row = db.prepare(`SELECT * FROM notion_page_create_requests WHERE rakuten_manage_number = ?`).get('sku_b');
  assert.equal(row.status, 'completed');
  assert.equal(row.lease_token, 'old_token');
});

test('claim CAS: status=failed は奪取可能', () => {
  const db = setupDb();
  db.prepare(`
    INSERT INTO notion_page_create_requests (rakuten_manage_number, status, lease_token, error_message, attempt)
    VALUES ('sku_c', 'failed', 'old_token', 'previous_err', 1)
  `).run();
  const r = db.prepare(`
    INSERT INTO notion_page_create_requests (rakuten_manage_number, status, lease_token, lease_expires_at, attempt)
    VALUES (?, 'running', ?, ?, 1)
    ON CONFLICT(rakuten_manage_number) DO UPDATE SET
      status = 'running',
      lease_token = excluded.lease_token,
      lease_expires_at = excluded.lease_expires_at,
      attempt = attempt + 1
    WHERE status = 'failed' OR (status = 'running' AND lease_expires_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
  `).run('sku_c', 'new_token', new Date(Date.now() + 60_000).toISOString());
  assert.equal(r.changes, 1);
  const row = db.prepare(`SELECT * FROM notion_page_create_requests WHERE rakuten_manage_number = ?`).get('sku_c');
  assert.equal(row.status, 'running');
  assert.equal(row.lease_token, 'new_token');
  assert.equal(row.attempt, 2);
});

test('claim CAS: lease 期限切れ running は奪取可能', () => {
  const db = setupDb();
  db.prepare(`
    INSERT INTO notion_page_create_requests (rakuten_manage_number, status, lease_token, lease_expires_at, attempt)
    VALUES ('sku_d', 'running', 'stale_token', ?, 1)
  `).run(new Date(Date.now() - 60_000).toISOString());  // 1分前
  const r = db.prepare(`
    INSERT INTO notion_page_create_requests (rakuten_manage_number, status, lease_token, lease_expires_at, attempt)
    VALUES (?, 'running', ?, ?, 1)
    ON CONFLICT(rakuten_manage_number) DO UPDATE SET
      status = 'running',
      lease_token = excluded.lease_token,
      lease_expires_at = excluded.lease_expires_at,
      attempt = attempt + 1
    WHERE status = 'failed' OR (status = 'running' AND lease_expires_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
  `).run('sku_d', 'new_token', new Date(Date.now() + 60_000).toISOString());
  assert.equal(r.changes, 1);
  const row = db.prepare(`SELECT * FROM notion_page_create_requests WHERE rakuten_manage_number = ?`).get('sku_d');
  assert.equal(row.lease_token, 'new_token');
});

test('claim CAS: 生きてる running (lease 有効) は奪取不能', () => {
  const db = setupDb();
  db.prepare(`
    INSERT INTO notion_page_create_requests (rakuten_manage_number, status, lease_token, lease_expires_at, attempt)
    VALUES ('sku_e', 'running', 'live_token', ?, 1)
  `).run(new Date(Date.now() + 60_000).toISOString());
  const r = db.prepare(`
    INSERT INTO notion_page_create_requests (rakuten_manage_number, status, lease_token, lease_expires_at, attempt)
    VALUES (?, 'running', ?, ?, 1)
    ON CONFLICT(rakuten_manage_number) DO UPDATE SET status = 'running'
    WHERE status = 'failed' OR (status = 'running' AND lease_expires_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
  `).run('sku_e', 'new_token', new Date(Date.now() + 60_000).toISOString());
  assert.equal(r.changes, 0);
  const row = db.prepare(`SELECT * FROM notion_page_create_requests WHERE rakuten_manage_number = ?`).get('sku_e');
  assert.equal(row.lease_token, 'live_token');
});

// finalize CAS — lease 一致時のみ書込 / 不一致は row 不変
test('finalize CAS: 自 lease 一致なら completed に', () => {
  const db = setupDb();
  db.prepare(`
    INSERT INTO notion_page_create_requests (rakuten_manage_number, status, lease_token, lease_expires_at, attempt)
    VALUES ('sku_f', 'running', 'mine', ?, 1)
  `).run(new Date(Date.now() + 60_000).toISOString());

  const r = db.prepare(`
    UPDATE notion_page_create_requests
       SET status = 'completed', notion_page_id = ?
     WHERE rakuten_manage_number = ?
       AND lease_token = ?
       AND status = 'running'
  `).run('notion_xyz', 'sku_f', 'mine');
  assert.equal(r.changes, 1);
  const row = db.prepare(`SELECT * FROM notion_page_create_requests WHERE rakuten_manage_number = 'sku_f'`).get();
  assert.equal(row.status, 'completed');
  assert.equal(row.notion_page_id, 'notion_xyz');
});

test('finalize CAS: lease 不一致は row 不変 (R5 H-2 / R4 H-2)', () => {
  const db = setupDb();
  db.prepare(`
    INSERT INTO notion_page_create_requests (rakuten_manage_number, status, lease_token, lease_expires_at, attempt)
    VALUES ('sku_g', 'running', 'other_owner_token', ?, 1)
  `).run(new Date(Date.now() + 60_000).toISOString());

  const r = db.prepare(`
    UPDATE notion_page_create_requests
       SET status = 'completed', notion_page_id = ?
     WHERE rakuten_manage_number = ?
       AND lease_token = ?
       AND status = 'running'
  `).run('notion_attempted', 'sku_g', 'my_lost_token');
  assert.equal(r.changes, 0);
  const row = db.prepare(`SELECT * FROM notion_page_create_requests WHERE rakuten_manage_number = 'sku_g'`).get();
  assert.equal(row.status, 'running');
  assert.equal(row.lease_token, 'other_owner_token');
  assert.equal(row.notion_page_id, null);
});
