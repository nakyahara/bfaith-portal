/**
 * test-sku-map-sync.mjs — mirror /api/sync/:entity/chunk の SKUマップ受け口の検証テスト
 *
 * 価格一括改定ツール PR1。full_snapshot (全置換) が満たすべき性質を確認する:
 *   - miniPC で削除された map 行が mirror に残らない (= 別商品へ値付けする事故が起きない)
 *   - 拒否したとき (0件 / 分割 / 不正行) に既存 mirror を消さない
 *
 * 実行: node apps/warehouse-mirror/test-sku-map-sync.mjs
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import http from 'node:http';
import crypto from 'node:crypto';
import express from 'express';

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'skumapsync-test-'));
process.env.DATA_DIR = tmpDir;
process.env.ALLOW_INSECURE_MIRROR_SYNC = '1'; // 認証は本テストの対象外

const { initMirrorDB, getMirrorDB } = await import('./db.js');
const mirrorRouter = (await import('./router.js')).default;

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const eq = (a, b, label) => ok(JSON.stringify(a) === JSON.stringify(b), `${label} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

initMirrorDB();
const db = getMirrorDB();

const app = express();
app.use('/apps/mirror', express.json({ limit: '32mb' }), mirrorRouter);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const base = `http://127.0.0.1:${server.address().port}/apps/mirror`;

let runSeq = 0;
const newRunId = () => `yahoo_sku_map-v1-test-${++runSeq}`;

const row = (o = {}) => ({
  yahoo_key: 'abc-01', store_id: 'b-faith01', ne_code: 'ne0001',
  resolution_source: 'manual', notes: null, created_at: '2026-01-01', updated_at: '2026-01-01', ...o,
});

const post = async (rows, opts = {}) => {
  const runId = opts.runId || newRunId();
  const enriched = rows.map((r) => (r === null ? null : {
    ...r, source_run_id: runId, source_row_hash: 'h', synced_at: '2026-08-28T00:00:00.000Z',
  }));
  const payload = { rows: enriched };
  const body = {
    sync_run_id: runId,
    contract_version: 1,
    scope_from: '2026-08-28', scope_to: '2026-08-28',
    chunk_index: opts.chunkIndex ?? 0,
    chunk_count: opts.chunkCount ?? 1,
    is_first: opts.isFirst ?? true,
    is_last: opts.isLast ?? true,
    row_count: enriched.length,
    payload_checksum: crypto.createHash('sha256').update(JSON.stringify(payload)).digest('hex'),
    meta: opts.meta || {},
    payload,
  };
  const res = await fetch(`${base}/api/sync/${opts.entity || 'yahoo_sku_map'}/chunk`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
  });
  return { status: res.status, json: await res.json().catch(() => null), runId };
};

const keys = () => db.prepare('SELECT yahoo_key FROM mirror_yahoo_sku_map ORDER BY yahoo_key').all().map((r) => r.yahoo_key);

console.log('\n── 正常系 ──');
{
  const r = await post([row(), row({ yahoo_key: 'abc-02', ne_code: 'ne0002' })]);
  eq(r.status, 200, 'HTTP 200');
  eq(keys(), ['abc-01', 'abc-02'], '2行保存された');
  const saved = db.prepare("SELECT store_id, ne_code, resolution_source FROM mirror_yahoo_sku_map WHERE yahoo_key='abc-01'").get();
  eq([saved.store_id, saved.ne_code, saved.resolution_source], ['b-faith01', 'ne0001', 'manual'], '値がそのまま入る');
}

console.log('\n── 全置換: miniPC で削除された行は mirror にも残らない ──');
{
  const r = await post([row({ yahoo_key: 'abc-02', ne_code: 'ne0002' })]);
  eq(r.status, 200, 'HTTP 200');
  eq(keys(), ['abc-02'], 'abc-01 は消えた (no_clear upsert なら残ってしまう)');
}

console.log('\n── 同一 run_id の再送は冪等 (replayed) ──');
{
  const first = await post([row({ yahoo_key: 'abc-03', ne_code: 'ne0003' })]);
  eq(first.status, 200, '初回 200');
  const again = await post([row({ yahoo_key: 'abc-03', ne_code: 'ne0003' })], { runId: first.runId });
  eq(again.status, 200, '再送 200');
  eq(again.json?.replayed, true, 'replayed=true (二重適用しない)');
  eq(keys(), ['abc-03'], '行数は変わらない');
}

console.log('\n── 拒否ケース: 既存 mirror を消さない ──');
const before = keys();
const cases = [
  ['0件 (map 全消しの事故)', [], {}, 'empty_snapshot_rejected'],
  ['chunk 分割 (途中欠落で表が欠ける)', [row()], { chunkCount: 2, isLast: false }, 'full_snapshot_requires_single_chunk'],
  ['ne_code 欠落', [row({ ne_code: '' })], {}, 'bad_row'],
  ['yahoo_key 欠落', [row({ yahoo_key: null })], {}, 'bad_row'],
  ['resolution_source 欠落', [row({ resolution_source: undefined })], {}, 'bad_row'],
];
for (const [label, rows, opts, expectedError] of cases) {
  const r = await post(rows, opts);
  ok(r.status === 400, `${label} → 400 (実際 ${r.status})`);
  eq(r.json?.error, expectedError, `  error=${expectedError}`);
  eq(keys(), before, '  既存 mirror は無傷');
}

console.log('\n── 明示すれば 0 件も受ける (allow_empty_snapshot) ──');
{
  const r = await post([], { meta: { allow_empty_snapshot: true } });
  eq(r.status, 200, 'HTTP 200');
  eq(keys(), [], '空になった');
}

console.log('\n── au PAY 側 (複合PK) も同様に入る ──');
{
  const runId = 'aupay_sku_map-v1-test-1';
  const rows = [
    { store_id: 'b-faith01', aupay_key: 'item-a', ne_code: 'ne0001', resolution_source: 'manual', notes: null, created_at: null, updated_at: null },
    { store_id: 'b-faith01', aupay_key: 'item-b', ne_code: 'ne0002', resolution_source: 'manual', notes: null, created_at: null, updated_at: null },
  ];
  const r = await post(rows, { entity: 'aupay_sku_map', runId });
  eq(r.status, 200, 'HTTP 200');
  eq(db.prepare('SELECT COUNT(*) n FROM mirror_aupay_sku_map').get().n, 2, '2行保存された');
}

server.close();
try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* Windows のロック残りは無視 */ }
console.log(`\n${failed === 0 ? '✅ 全テスト通過' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
