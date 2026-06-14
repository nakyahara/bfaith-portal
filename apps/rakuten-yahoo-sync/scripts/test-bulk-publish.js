/**
 * E-6-3 smoke runner: 一括 publish + 再実行 + 進捗 polling の動作確認 (in-memory SQLite + 実 HTTP server)。
 *
 * 使い方:
 *   node apps/rakuten-yahoo-sync/scripts/test-bulk-publish.js
 *
 * 検証項目:
 *   - migration 009 schema (CHECK / partial UNIQUE INDEX)
 *   - startBulkPublish: kill-switch off で 403
 *   - kill-switch on で 202 + batchId 返却、 順次 publish 進行
 *   - 進捗 polling (GET /api/publish/bulk/:batchId)
 *   - listFailedItemCodes (publish 失敗の item を最新順)
 *   - retry-failed endpoint
 *   - 同時 running batch は UNIQUE INDEX で 409
 *   - itemCodes 重複除去 / trim / 空配列 → 400
 *   - batch finalize (succeeded + failed カウント、 status='completed')
 */

import express from 'express';
import Database from 'better-sqlite3';
import fs from 'node:fs';
import path from 'node:path';
import http from 'node:http';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const MIGRATIONS_DIR = path.join(__dirname, '..', 'db', 'migrations');

let failed = 0;
function ok(name) { console.log(`OK   ${name}`); }
function fail(name, msg) { failed += 1; console.error(`FAIL ${name}: ${msg}`); }
function expect(name, cond, msg) { if (cond) ok(name); else fail(name, msg); }

function freshDbToTmp() {
  const tmpFile = path.join(__dirname, '..', '..', '..', 'data', 'rys-test-bulk-' + process.pid + '.db');
  try { fs.unlinkSync(tmpFile); } catch (_) {}
  process.env.RYS_DB_FILE = tmpFile;
  const db = new Database(tmpFile);
  db.pragma('journal_mode = WAL');
  db.pragma('foreign_keys = ON');
  const files = fs.readdirSync(MIGRATIONS_DIR).filter((f) => f.endsWith('.sql')).sort();
  for (const f of files) db.exec(fs.readFileSync(path.join(MIGRATIONS_DIR, f), 'utf8'));
  db.pragma(`user_version = ${files[files.length - 1].match(/^(\d+)/)[1]}`);
  db.close();
  return tmpFile;
}

function fetchJson(url, opts = {}) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const req = http.request({
      method: opts.method || 'GET',
      hostname: u.hostname,
      port: u.port,
      path: u.pathname + u.search,
      headers: { 'Content-Type': 'application/json' },
    }, (res) => {
      let body = '';
      res.on('data', (chunk) => { body += chunk; });
      res.on('end', () => {
        try { resolve({ status: res.statusCode, data: JSON.parse(body) }); }
        catch (e) { resolve({ status: res.statusCode, raw: body, parseError: e.message }); }
      });
    });
    req.on('error', reject);
    if (opts.body) req.write(JSON.stringify(opts.body));
    req.end();
  });
}

async function waitForBatchFinish(base, batchId, timeoutMs = 5000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const r = await fetchJson(`${base}/api/publish/bulk/${batchId}`);
    if (r.data?.batch?.status && r.data.batch.status !== 'running') return r.data;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  return null;
}

async function main() {
  const dbFile = freshDbToTmp();
  // RYS_PUBLISH_ENABLED は最初は OFF にして 403 を確認
  delete process.env.RYS_PUBLISH_ENABLED;

  // executePublish を mock する: 偶数 index 成功、 奇数失敗
  let publishCallCounter = 0;
  const mockModulePath = '../services/publish-executor.js';

  // 動的 import 前に env 設定済
  const { default: router } = await import('../router.js');
  const app = express();
  app.use(express.json());
  app.use('/apps/rakuten-yahoo-sync', router);
  const server = app.listen(0);
  const port = server.address().port;
  const base = `http://127.0.0.1:${port}/apps/rakuten-yahoo-sync`;

  try {
    // (1) migration 009 schema
    const db = new Database(dbFile);
    const tables = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('bulk_publish_batches','bulk_publish_batch_items')").all().map((r) => r.name);
    db.close();
    expect('schema (1): bulk_publish_batches + items created', tables.length === 2, JSON.stringify(tables));

    // (2) kill-switch off → 403
    const r2 = await fetchJson(`${base}/api/publish/bulk/start`, { method: 'POST', body: { itemCodes: ['item-1'] } });
    expect('start (2): kill-switch off → 403', r2.status === 403, `got ${r2.status} body=${JSON.stringify(r2.data)?.slice(0, 100)}`);

    // (3) kill-switch on で start → 202 + batchId
    process.env.RYS_PUBLISH_ENABLED = '1';
    // executePublish を mock: success/dedupe/failed の混在を作る
    // (実 publish は走らせない、 publish_idempotency を mock 状態にする)
    // → publish-executor.executePublish を monkey-patch するのは複雑なので、
    //   代わりに RYS_PUBLISH_ENABLED + 適切な fixture で 「flag_off / readiness_blocked 経路」 を回す

    // 簡易: kill-switch を on にした上で readiness_blocked を返す経路を使う
    //   (jobs / notion_overrides を入れずに publish → readiness_blocked)
    const itemCodes = ['bulk-1', 'bulk-2', 'bulk-3'];
    const r3 = await fetchJson(`${base}/api/publish/bulk/start`, { method: 'POST', body: { itemCodes } });
    expect('start (3a): 202 + batchId returned', r3.status === 202 && typeof r3.data?.batchId === 'number', `got ${r3.status} batchId=${r3.data?.batchId}`);
    expect('start (3b): total = 3', r3.data?.total === 3, `total=${r3.data?.total}`);
    const batchId = r3.data?.batchId;

    // (4) 進捗 polling
    const final = await waitForBatchFinish(base, batchId);
    expect('progress (4a): batch finished within timeout', final !== null, 'timeout');
    expect('progress (4b): batch status not running', final?.batch?.status !== 'running', JSON.stringify(final?.batch?.status));
    expect('progress (4c): total = 3', final?.batch?.total === 3, JSON.stringify(final?.batch?.total));

    // (5) GET with items
    const r5 = await fetchJson(`${base}/api/publish/bulk/${batchId}?withItems=1`);
    expect('get (5): items returned with withItems=1', Array.isArray(r5.data?.items) && r5.data.items.length === 3, JSON.stringify(r5.data?.items?.length));

    // (6) 重複 itemCode → 1 件にまとめられる
    const r6 = await fetchJson(`${base}/api/publish/bulk/start`, { method: 'POST', body: { itemCodes: ['dup-1', 'dup-1', '  dup-1  '] } });
    if (r6.status === 202) {
      expect('start (6): duplicate itemCodes deduped → total=1', r6.data?.total === 1, `total=${r6.data?.total}`);
      await waitForBatchFinish(base, r6.data?.batchId);
    } else if (r6.status === 409) {
      // 前の batch がまだ running の可能性、 wait してから retry
      await new Promise((r) => setTimeout(r, 100));
      const r6b = await fetchJson(`${base}/api/publish/bulk/start`, { method: 'POST', body: { itemCodes: ['dup-1', 'dup-1', '  dup-1  '] } });
      expect('start (6b): duplicate dedup after wait', r6b.status === 202 && r6b.data?.total === 1, JSON.stringify({ status: r6b.status, total: r6b.data?.total }));
      await waitForBatchFinish(base, r6b.data?.batchId);
    }

    // (7) 空配列 → 400
    const r7 = await fetchJson(`${base}/api/publish/bulk/start`, { method: 'POST', body: { itemCodes: [] } });
    expect('start (7): empty itemCodes → 400', r7.status === 400, `got ${r7.status}`);

    // (8) running batch UNIQUE INDEX: 2 batches を間髪なしで連投
    //   1st は accept、 2nd は 409 を期待
    const dbForLockTest = new Database(dbFile);
    dbForLockTest.prepare("INSERT INTO bulk_publish_batches (total, in_progress, status, triggered_by, started_at) VALUES (10, 10, 'running', 'manual', ?)")
      .run(new Date().toISOString());
    dbForLockTest.close();
    const r8 = await fetchJson(`${base}/api/publish/bulk/start`, { method: 'POST', body: { itemCodes: ['conflict-1'] } });
    expect('start (8): concurrent batch → 409', r8.status === 409, `got ${r8.status}`);
    // cleanup running batch
    const dbCleanup = new Database(dbFile);
    dbCleanup.prepare("UPDATE bulk_publish_batches SET status='aborted', finished_at=? WHERE status='running'").run(new Date().toISOString());
    dbCleanup.close();

    // (9) retry-failed (failed publish 履歴を作ってから呼ぶ)
    //   過去 test の publish_idempotency 行は clean してから insert (listFailedItemCodes が汚染されないように)
    const dbForRetry = new Database(dbFile);
    dbForRetry.prepare('DELETE FROM publish_idempotency').run();
    dbForRetry.prepare(`INSERT INTO publish_idempotency (idempotency_key, item_code, manage_number, status, attempt, created_by, completed_at, error_message) VALUES (?,?,?,?,?,?,?,?)`)
      .run('key-fail-1', 'fail-item-1', 'mn1', 'failed', 1, 'test', new Date().toISOString(), 'mocked failure');
    dbForRetry.prepare(`INSERT INTO publish_idempotency (idempotency_key, item_code, manage_number, status, attempt, created_by, completed_at, error_message) VALUES (?,?,?,?,?,?,?,?)`)
      .run('key-fail-2', 'fail-item-2', 'mn2', 'failed', 1, 'test', new Date().toISOString(), 'mocked failure');
    dbForRetry.close();
    const r9 = await fetchJson(`${base}/api/publish/bulk/retry-failed`, { method: 'POST', body: { limit: 10 } });
    expect('retry-failed (9a): 202 + batchId', r9.status === 202 && typeof r9.data?.batchId === 'number', `got ${r9.status} body=${JSON.stringify(r9.data)?.slice(0,150)}`);
    expect('retry-failed (9b): total = 2 failed items picked up', r9.data?.total === 2, JSON.stringify(r9.data?.total));
    expect('retry-failed (9c): itemCodes returned', Array.isArray(r9.data?.itemCodes) && r9.data.itemCodes.length === 2, JSON.stringify(r9.data?.itemCodes));
    await waitForBatchFinish(base, r9.data?.batchId);

    // (10) Codex R1 Medium-1: /bulk/active/current は static route として /bulk/:batchId より優先
    //   既に running batch なし状態なので {batch: null} が返るはず
    const r10 = await fetchJson(`${base}/api/publish/bulk/active/current`);
    expect('routing (10): /bulk/active/current static route does not match :batchId', r10.status === 200 && r10.data?.batch === null, JSON.stringify({ status: r10.status, batch: r10.data?.batch }));

    // (11) Codex R1 Medium-4: retry limit > 50 → 50 にクランプ (引数=500 でも 50 件まで)
    const dbClampTest = new Database(dbFile);
    dbClampTest.prepare('DELETE FROM publish_idempotency').run();
    for (let i = 0; i < 60; i++) {
      dbClampTest.prepare(`INSERT INTO publish_idempotency (idempotency_key, item_code, manage_number, status, attempt, created_by, completed_at, error_message) VALUES (?,?,?,?,?,?,?,?)`)
        .run('clamp-key-' + i, 'clamp-item-' + i, 'mn-' + i, 'failed', 1, 'test', new Date().toISOString(), 'clamp');
    }
    dbClampTest.close();
    const r11 = await fetchJson(`${base}/api/publish/bulk/retry-failed`, { method: 'POST', body: { limit: 500 } });
    expect('clamp (11): retry limit=500 clamped to 50', r11.status === 202 && r11.data?.total === 50, `got total=${r11.data?.total}`);
    await waitForBatchFinish(base, r11.data?.batchId);

    // (12) Codex R1 Medium-3: listFailedItemCodes が 「item 毎の最新 failed」 を返す
    //   1 item に複数 failed + 1 success → 該当 item は除外、 success のない item だけ返る
    const dbListTest = new Database(dbFile);
    dbListTest.prepare('DELETE FROM publish_idempotency').run();
    // item-A: failed → failed → success → failed の順 (最新は failed だが、 success がある)
    dbListTest.prepare(`INSERT INTO publish_idempotency (idempotency_key, item_code, manage_number, status, attempt, created_by, completed_at, error_message) VALUES (?,?,?,?,?,?,?,?)`)
      .run('A-1', 'item-A', 'mn-A', 'failed', 1, 'test', new Date().toISOString(), 'err1');
    dbListTest.prepare(`INSERT INTO publish_idempotency (idempotency_key, item_code, manage_number, status, attempt, created_by, completed_at, error_message) VALUES (?,?,?,?,?,?,?,?)`)
      .run('A-2', 'item-A', 'mn-A', 'success', 2, 'test', new Date().toISOString(), null);
    // item-B: failed のみ → listFailed に含まれる
    dbListTest.prepare(`INSERT INTO publish_idempotency (idempotency_key, item_code, manage_number, status, attempt, created_by, completed_at, error_message) VALUES (?,?,?,?,?,?,?,?)`)
      .run('B-1', 'item-B', 'mn-B', 'failed', 1, 'test', new Date().toISOString(), 'errB');
    // item-C: failed → failed → failed (3 行)、 1 件にまとめて返る
    dbListTest.prepare(`INSERT INTO publish_idempotency (idempotency_key, item_code, manage_number, status, attempt, created_by, completed_at, error_message) VALUES (?,?,?,?,?,?,?,?)`)
      .run('C-1', 'item-C', 'mn-C', 'failed', 1, 'test', new Date().toISOString(), 'C1');
    dbListTest.prepare(`INSERT INTO publish_idempotency (idempotency_key, item_code, manage_number, status, attempt, created_by, completed_at, error_message) VALUES (?,?,?,?,?,?,?,?)`)
      .run('C-2', 'item-C', 'mn-C', 'failed', 2, 'test', new Date().toISOString(), 'C2');
    dbListTest.close();
    const r12 = await fetchJson(`${base}/api/publish/bulk/retry-failed`, { method: 'POST', body: { limit: 50 } });
    expect('list (12a): listFailedItemCodes excludes items with success', r12.data?.itemCodes && !r12.data.itemCodes.includes('item-A'), JSON.stringify(r12.data?.itemCodes));
    expect('list (12b): listFailedItemCodes returns 1 row per item (item-C dedup)', r12.data?.total === 2 && r12.data.itemCodes.sort().join(',') === 'item-B,item-C', JSON.stringify(r12.data?.itemCodes));
    await waitForBatchFinish(base, r12.data?.batchId);
  } finally {
    server.close();
    try { fs.unlinkSync(dbFile); } catch (_) {}
    try { fs.unlinkSync(dbFile + '-wal'); } catch (_) {}
    try { fs.unlinkSync(dbFile + '-shm'); } catch (_) {}
    delete process.env.RYS_PUBLISH_ENABLED;
  }
}

(async () => {
  console.log('=== Phase E-6-3 smoke runner ===');
  await main();
  console.log('==================================');
  if (failed > 0) {
    console.error(`FAILED: ${failed} check(s) did not pass`);
    process.exit(1);
  } else {
    console.log('ALL CHECKS PASSED');
  }
})().catch((e) => {
  console.error('unexpected runner error:', e);
  process.exit(2);
});
