/**
 * shipping-log スモークテスト — 出荷実績ログ取込 API
 *
 * 検証内容:
 * Test 1: ensureSchema で sl_shipping_slips + trigger が作成される
 * Test 2: ingestFolderSlips が INSERT し、同一 payload の再送は全件 ignored (冪等)
 * Test 3: append-only trigger — UPDATE / DELETE が RAISE(ABORT) で拒否される
 * Test 4: HTTP 認証 — env 未設定 503 / token 無し 401 / 不一致 403 / 一致 200 (fail-closed)
 * Test 5: バリデーション — rows 空 400、ship_date 形式不正 400、slip_no 欠落 400
 * Test 6: GET /recent が取込済み行を返す
 *
 * 実行: node scripts/test-shipping-log.mjs
 */
import fs from 'fs';
import os from 'os';
import path from 'path';
import express from 'express';

// DATA_DIR はモジュール読込時にキャプチャされるため import 前に設定する
const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'shipping-log-test-'));
process.env.DATA_DIR = tmpDir;

const { initMirrorDB, getMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const { ensureSchema, ingestFolderSlips, recentSlips } = await import('../apps/shipping-log/db.js');
const { default: shippingLogRouter } = await import('../apps/shipping-log/router.js');

let failed = 0;
function ok(msg) { console.log(`[OK] ${msg}`); }
function fail(msg) { console.error(`[FAIL] ${msg}`); failed++; }
function expectEq(actual, expected, msg) {
  if (actual === expected) ok(`${msg} = ${JSON.stringify(actual)}`);
  else fail(`${msg}: 期待=${JSON.stringify(expected)} 実際=${JSON.stringify(actual)}`);
}

// ── Test 1: スキーマ作成 ──
ensureSchema();
const db = getMirrorDB();
const table = db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name='sl_shipping_slips'`).get();
table ? ok('sl_shipping_slips 作成') : fail('sl_shipping_slips 未作成');
const triggers = db.prepare(`SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'trg_sl_slips%'`).all();
expectEq(triggers.length, 2, 'append-only trigger 数');

// ── Test 2: 取込 + 冪等再送 ──
const payload = {
  runId: '20260718-181600', folderName: '出荷_01', shipDate: '2026-07-18', extractedAt: '2026-07-18 18:16:00',
  rows: [
    { slip_no: 'SP00110324384', mgmt_no: '1498337', mall_order_no: null, source_file: '納品書_1.pdf' },
    { slip_no: 'SP00110324772', mgmt_no: '1498416', mall_order_no: 'AES250-3936214-7150201', source_file: '納品書_1.pdf' },
  ],
};
const r1 = ingestFolderSlips(payload);
expectEq(r1.inserted, 2, '初回 inserted');
const r2 = ingestFolderSlips({ ...payload, runId: '20260718-190000' }); // 再実行 (別run) でも業務キーで冪等
expectEq(r2.inserted, 0, '再送 inserted');
expectEq(r2.ignored, 2, '再送 ignored');

// ── Test 3: append-only trigger ──
try {
  db.prepare(`UPDATE sl_shipping_slips SET mgmt_no='x' WHERE slip_no='SP00110324384'`).run();
  fail('UPDATE が通ってしまった');
} catch (e) { /(UPDATE forbidden)/.test(e.message) ? ok('UPDATE 拒否') : fail(`UPDATE 拒否だが想定外メッセージ: ${e.message}`); }
try {
  db.prepare(`DELETE FROM sl_shipping_slips WHERE slip_no='SP00110324384'`).run();
  fail('DELETE が通ってしまった');
} catch (e) { /(DELETE forbidden)/.test(e.message) ? ok('DELETE 拒否') : fail(`DELETE 拒否だが想定外メッセージ: ${e.message}`); }

// ── Test 4-6: HTTP 層 ──
const app = express();
app.use('/apps/shipping-log/api', express.json({ limit: '2mb' }), shippingLogRouter);
const server = app.listen(0, '127.0.0.1');
await new Promise((r) => server.on('listening', r));
const base = `http://127.0.0.1:${server.address().port}/apps/shipping-log/api`;

async function post(pathName, body, token) {
  const headers = { 'content-type': 'application/json' };
  if (token) headers.authorization = `Bearer ${token}`;
  const res = await fetch(base + pathName, { method: 'POST', headers, body: JSON.stringify(body) });
  return { status: res.status, body: await res.json() };
}

const TOKEN = 'test-token-shipping-log';
const httpPayload = {
  run_id: '20260718-181600', folder: '出荷_02', ship_date: '2026-07-18',
  rows: [{ slip_no: 'SP00110399999', mgmt_no: '1498999' }],
};

delete process.env.SHIPPING_LOG_INGEST_TOKEN;
expectEq((await post('/ingest', httpPayload, TOKEN)).status, 503, 'env 未設定 → 503');
process.env.SHIPPING_LOG_INGEST_TOKEN = TOKEN;
expectEq((await post('/ingest', httpPayload, null)).status, 401, 'token 無し → 401');
expectEq((await post('/ingest', httpPayload, 'wrong-token-xxxxxxxxxxxx')).status, 403, 'token 不一致 → 403');
const okRes = await post('/ingest', httpPayload, TOKEN);
expectEq(okRes.status, 200, 'token 一致 → 200');
expectEq(okRes.body.inserted, 1, 'HTTP経由 inserted');

expectEq((await post('/ingest', { ...httpPayload, rows: [] }, TOKEN)).status, 400, 'rows 空 → 400');
expectEq((await post('/ingest', { ...httpPayload, ship_date: '2026/07/18' }, TOKEN)).status, 400, 'ship_date 形式不正 → 400');
expectEq((await post('/ingest', { ...httpPayload, rows: [{ mgmt_no: '1' }] }, TOKEN)).status, 400, 'slip_no 欠落 → 400');

const recentRes = await fetch(`${base}/recent?limit=10`, { headers: { authorization: `Bearer ${TOKEN}` } });
const recentBody = await recentRes.json();
expectEq(recentRes.status, 200, 'GET /recent → 200');
expectEq(recentBody.rows.length, 3, 'recent 行数');

// DB層 recentSlips も確認
expectEq(recentSlips(10).length, 3, 'recentSlips 行数');

await new Promise((r) => server.close(r));
try { db.close(); } catch { /* teardown */ }
console.log(failed === 0 ? '\n✅ 全テスト PASS' : `\n❌ ${failed} 件失敗`);
process.exitCode = failed === 0 ? 0 : 1;
