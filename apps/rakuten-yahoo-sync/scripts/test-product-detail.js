/**
 * E-6-2 smoke runner: 商品詳細 API + ドロワー描画ロジックの動作確認 (in-memory SQLite + express)。
 *
 * 使い方:
 *   node apps/rakuten-yahoo-sync/scripts/test-product-detail.js
 *
 * 検証項目:
 *   (1) GET /api/products/:itemCode/detail で migration_candidates + notion + yahoo + publish + exclusion を返す
 *   (2) itemCode が存在しない → 404
 *   (3) candidate なしだが notion_overrides にある (orphan) → 200 + candidate=null
 *   (4) publishHistory が created_at DESC (最新が先)
 *   (5) exclusionHistory に active + restored 両方含む
 *   (6) readiness blocked_reasons の翻訳 + Notion ページ URL 生成
 *   (7) itemCode 空欄 → 400
 */

import express from 'express';
import Database from 'better-sqlite3';
import fs from 'node:fs';
import path from 'node:path';
import http from 'node:http';
import { fileURLToPath } from 'node:url';

// router.js は import 時に DB_FILE を評価するので、 RYS_DB_FILE 設定後に dynamic import する。
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const MIGRATIONS_DIR = path.join(__dirname, '..', 'db', 'migrations');

let failed = 0;
function ok(name) { console.log(`OK   ${name}`); }
function fail(name, msg) { failed += 1; console.error(`FAIL ${name}: ${msg}`); }
function expect(name, cond, msg) { if (cond) ok(name); else fail(name, msg); }

function freshDbToTmp() {
  const tmpFile = path.join(__dirname, '..', '..', '..', 'data', 'rys-test-detail-' + process.pid + '.db');
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

function seedFixtures(dbFile) {
  const db = new Database(dbFile);
  const now = new Date().toISOString();
  const earlier = new Date(Date.now() - 60000).toISOString();
  // candidate
  db.prepare(`INSERT INTO migration_candidates (item_code, rakuten_manage_number, first_detected_at, last_detected_at, last_seen_in_rakuten_at, last_checked_yahoo_baseline_at, status) VALUES (?, ?, ?, ?, ?, ?, ?)`)
    .run('item-1', 'mn-item-1', earlier, now, now, now, 'candidate');
  // notion
  db.prepare(`INSERT INTO notion_overrides (rakuten_manage_number, notion_page_id, yahoo_title, yahoo_price, yahoo_price_sagawa, notion_delivery_label, notion_tax_rate, notion_status, synced_at, raw_properties_json, source_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '{}', 'h')`)
    .run('mn-item-1', 'a'.repeat(32), 'タイトル', 1500, 1700, 'ネコポス', '10', 'OK', now);
  // yahoo (item-1 では yahoo にない、 orphan-1 では yahoo にある)
  // publish_idempotency 複数 row (最新が先で返る)
  db.prepare(`INSERT INTO publish_idempotency (idempotency_key, item_code, manage_number, status, attempt, created_by, completed_at, error_message) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
    .run('key-1-old', 'item-1', 'mn-item-1', 'failed', 1, 'test', earlier, 'old failure');
  db.prepare(`INSERT INTO publish_idempotency (idempotency_key, item_code, manage_number, status, attempt, created_by, completed_at, error_message) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
    .run('key-1-new', 'item-1', 'mn-item-1', 'success', 2, 'test', now, null);
  // exclusion 履歴 (除外 → 復元 → 再除外)
  db.prepare(`INSERT INTO migration_excluded (item_code, exclude_kind, reason, excluded_at, restored_at, restored_reason) VALUES (?, ?, ?, ?, ?, ?)`)
    .run('item-1', 'temporary', '一時保留', earlier, now, '再考');
  db.prepare(`INSERT INTO migration_excluded (item_code, exclude_kind, reason, excluded_at) VALUES (?, ?, ?, ?)`)
    .run('item-1', 'permanent', 'やはり除外', now);
  // jobs (readiness blocked)
  db.prepare(`INSERT INTO jobs (item_code, current_state, readiness_status, readiness_blocked_reasons, last_readiness_at) VALUES (?, ?, ?, ?, ?)`)
    .run('item-1', 'pending', 'blocked', '["product_category_unresolved", "delivery_mapping_unresolved"]', now);

  // orphan: notion override あり、 candidate なし
  db.prepare(`INSERT INTO notion_overrides (rakuten_manage_number, notion_page_id, yahoo_title, yahoo_price, yahoo_price_sagawa, notion_delivery_label, notion_tax_rate, notion_status, synced_at, raw_properties_json, source_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '{}', 'h')`)
    .run('orphan-1', 'b'.repeat(32), 'orphan-title', 2000, null, 'ネコポス', '10', 'OK', now);
  db.close();
}

async function fetchJson(url) {
  return new Promise((resolve, reject) => {
    http.get(url, (res) => {
      let body = '';
      res.on('data', (chunk) => { body += chunk; });
      res.on('end', () => {
        try { resolve({ status: res.statusCode, data: JSON.parse(body) }); }
        catch (e) { resolve({ status: res.statusCode, raw: body, parseError: e.message }); }
      });
    }).on('error', reject);
  });
}

async function main() {
  const dbFile = freshDbToTmp();
  seedFixtures(dbFile);
  // RYS_DB_FILE が設定された後に dynamic import (router.js は import 時に DB_FILE を const 評価する)
  const { default: router } = await import('../router.js');
  const app = express();
  app.use(express.json());
  app.use('/apps/rakuten-yahoo-sync', router);
  const server = app.listen(0);
  const port = server.address().port;
  const base = `http://127.0.0.1:${port}/apps/rakuten-yahoo-sync`;

  try {
    // (1) item-1: full context
    const r1 = await fetchJson(`${base}/api/products/item-1/detail`);
    expect('detail (1a): item-1 200 OK', r1.status === 200, `got ${r1.status} body=${JSON.stringify(r1.data)?.slice(0, 200)}`);
    expect('detail (1b): candidate present', r1.data?.candidate?.item_code === 'item-1', JSON.stringify(r1.data?.candidate?.item_code));
    expect('detail (1c): notion present', r1.data?.notion?.yahoo_title === 'タイトル', JSON.stringify(r1.data?.notion?.yahoo_title));
    expect('detail (1d): yahoo absent (null)', r1.data?.yahoo === null, JSON.stringify(r1.data?.yahoo));
    expect('detail (1e): publishHistory has 2 rows', r1.data?.publishHistory?.length === 2, `len=${r1.data?.publishHistory?.length}`);
    expect('detail (1f): exclusionHistory has 2 rows', r1.data?.exclusionHistory?.length === 2, `len=${r1.data?.exclusionHistory?.length}`);
    expect('detail (1g): readiness translated', Array.isArray(r1.data?.readiness?.blockedReasonsTranslated) && r1.data?.readiness?.blockedReasonsTranslated?.length === 2, JSON.stringify(r1.data?.readiness?.blockedReasonsTranslated?.length));
    expect('detail (1h): notionPageUrl built', typeof r1.data?.notionPageUrl === 'string' && r1.data.notionPageUrl.includes('notion.so'), r1.data?.notionPageUrl);

    // (2) 不存在 → 404
    const r2 = await fetchJson(`${base}/api/products/does-not-exist/detail`);
    expect('detail (2): unknown item → 404', r2.status === 404, `got ${r2.status}`);

    // (3) orphan-1: candidate なし、 notion あり
    const r3 = await fetchJson(`${base}/api/products/orphan-1/detail`);
    expect('detail (3a): orphan 200 OK', r3.status === 200, `got ${r3.status}`);
    expect('detail (3b): candidate=null for orphan', r3.data?.candidate === null, JSON.stringify(r3.data?.candidate));
    expect('detail (3c): notion present for orphan', r3.data?.notion?.yahoo_title === 'orphan-title', JSON.stringify(r3.data?.notion?.yahoo_title));

    // (4) publishHistory が created_at DESC (success が最新、 先頭)
    expect('detail (4): publishHistory[0] = newest (success)', r1.data.publishHistory[0]?.status === 'success', JSON.stringify(r1.data.publishHistory.map((p) => p.status)));

    // (5) exclusionHistory: active + restored
    const exclusions = r1.data.exclusionHistory;
    const hasRestored = exclusions.some((e) => e.restored_at !== null && e.restored_at !== undefined);
    const hasActive = exclusions.some((e) => e.restored_at === null || e.restored_at === undefined);
    expect('detail (5a): exclusionHistory has restored row', hasRestored, JSON.stringify(exclusions.map((e) => e.restored_at)));
    expect('detail (5b): exclusionHistory has active row', hasActive, JSON.stringify(exclusions.map((e) => e.restored_at)));

    // (6) readiness 翻訳の整合 (notionField が日本語ラベル)
    const r1Reasons = r1.data.readiness.blockedReasonsTranslated;
    expect('detail (6): readiness translated has notionField labels', r1Reasons.every((r) => typeof r.message === 'string' && r.message.length > 0), JSON.stringify(r1Reasons));

    // (7) 空 itemCode → 400 (URL path: //detail はマッチしないので、 別経路で空)
    // express の :itemCode で空文字は route match しないので、 空文字を試す代わりに スペースのみ
    const r7 = await fetchJson(`${base}/api/products/%20/detail`);
    expect('detail (7): whitespace itemCode → 400', r7.status === 400, `got ${r7.status}`);
  } finally {
    server.close();
    try { fs.unlinkSync(dbFile); } catch (_) {}
    try { fs.unlinkSync(dbFile + '-wal'); } catch (_) {}
    try { fs.unlinkSync(dbFile + '-shm'); } catch (_) {}
  }
}

(async () => {
  console.log('=== Phase E-6-2 smoke runner ===');
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
