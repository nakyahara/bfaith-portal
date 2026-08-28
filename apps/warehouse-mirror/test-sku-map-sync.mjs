/**
 * test-sku-map-sync.mjs — mirror /api/sync/:entity/chunk の SKUマップ受け口の検証テスト
 *
 * 価格一括改定ツール PR1。full_snapshot (全置換) が満たすべき性質を確認する:
 *   - miniPC で削除された map 行が mirror に残らない (= 別商品へ値付けする事故が起きない)
 *   - 拒否したとき (0件 / 大幅減 / 分割 / 不正行 / 古い世代) に既存 mirror を消さない
 *   - 監査メタ (source_run_id / source_row_hash / synced_at) は受け側が付け直す
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
let genSeq = 0;
const newRunId = () => `yahoo_sku_map-v1-test-${++runSeq}`;

const row = (o = {}) => ({
  yahoo_key: 'abc-01', store_id: 'b-faith01', ne_code: 'ne0001',
  resolution_source: 'manual', notes: null, created_at: '2026-01-01', updated_at: '2026-01-01', ...o,
});

const post = async (rows, opts = {}) => {
  const runId = opts.runId || newRunId();
  const generation = opts.generation ?? ++genSeq;
  const enriched = rows.map((r) => (r === null ? null : { ...r, source_run_id: opts.rowRunId ?? runId }));
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
    meta: { ...(opts.omitGeneration ? {} : { snapshot_generation: generation }), ...(opts.meta || {}) },
    payload,
  };
  const res = await fetch(`${base}/api/sync/${opts.entity || 'yahoo_sku_map'}/chunk`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
  });
  return { status: res.status, json: await res.json().catch(() => null), runId, generation };
};

const keys = () => db.prepare('SELECT yahoo_key FROM mirror_yahoo_sku_map ORDER BY yahoo_key').all().map((r) => r.yahoo_key);
const manyRows = (n, prefix = 'k') => Array.from({ length: n }, (_, i) => row({ yahoo_key: `${prefix}-${String(i).padStart(3, '0')}` }));

console.log('\n── 正常系 ──');
{
  const r = await post([row(), row({ yahoo_key: 'abc-02', ne_code: 'ne0002' })]);
  eq(r.status, 200, 'HTTP 200');
  eq(keys(), ['abc-01', 'abc-02'], '2行保存された');
  const saved = db.prepare("SELECT store_id, ne_code, resolution_source FROM mirror_yahoo_sku_map WHERE yahoo_key='abc-01'").get();
  eq([saved.store_id, saved.ne_code, saved.resolution_source], ['b-faith01', 'ne0001', 'manual'], '値がそのまま入る');
  const gen = db.prepare("SELECT generation, row_count FROM mirror_snapshot_generations WHERE entity='yahoo_sku_map'").get();
  eq([gen.generation, gen.row_count], [r.generation, 2], '世代が記録される');
}

console.log('\n── 監査メタは受け側が付け直す ──');
{
  // 送信元が偽の hash / synced_at を載せても、保存されるのは受け側が計算した値
  // (件数は減らさない。減らすと先に減少ゲートで弾かれてしまう)
  const r = await post([
    row({ source_row_hash: 'FAKEHASH', synced_at: '1999-01-01T00:00:00.000Z' }),
    row({ yahoo_key: 'abc-02', ne_code: 'ne0002' }),
  ]);
  eq(r.status, 200, 'HTTP 200');
  const saved = db.prepare("SELECT source_run_id, source_row_hash, synced_at FROM mirror_yahoo_sku_map WHERE yahoo_key='abc-01'").get();
  eq(saved.source_run_id, r.runId, 'source_run_id = この run');
  ok(saved.source_row_hash !== 'FAKEHASH' && /^[0-9a-f]{16}$/.test(saved.source_row_hash), '送信元申告の hash は使わない');
  ok(saved.synced_at.startsWith('20'), 'synced_at は受信時刻');
}

console.log('\n── 全置換: miniPC で削除された行は mirror にも残らない ──');
{
  const seed = await post(manyRows(10));
  eq(seed.status, 200, '10 行を投入');
  // 1 行だけ消す (減少ゲートに触れない範囲での削除が、ちゃんと mirror に伝わるか)
  const r = await post(manyRows(10).filter((x) => x.yahoo_key !== 'k-000'));
  eq(r.status, 200, 'HTTP 200');
  eq(keys().includes('k-000'), false, 'k-000 は消えた (no_clear upsert なら残ってしまう)');
  eq(keys().length, 9, '9 行になった');
}

console.log('\n── 同一 run_id の再送は冪等 (replayed) ──');
{
  const rows9 = manyRows(10).filter((x) => x.yahoo_key !== 'k-000');
  const first = await post(rows9);
  eq(first.status, 200, '初回 200');
  const again = await post(rows9, { runId: first.runId, generation: first.generation });
  eq(again.status, 200, '再送 200');
  eq(again.json?.replayed, true, 'replayed=true (二重適用しない)');
  eq(keys().length, 9, '行数は変わらない');
}

console.log('\n── 古い世代の遅延到着は 409 で拒否 ──');
{
  const rows9 = manyRows(10).filter((x) => x.yahoo_key !== 'k-000');
  const appliedGen = db.prepare("SELECT generation FROM mirror_snapshot_generations WHERE entity='yahoo_sku_map'").get().generation;
  const r = await post([...rows9, row({ yahoo_key: 'stale-01' })], { generation: appliedGen - 1 });
  eq(r.status, 409, 'HTTP 409');
  eq(r.json?.error, 'stale_snapshot', 'error=stale_snapshot');
  eq(keys().includes('stale-01'), false, '新しい map は巻き戻らない');
  const same = await post([...rows9, row({ yahoo_key: 'stale-02' })], { generation: appliedGen });
  eq(same.status, 409, '同じ世代も 409 (単調増加を要求)');
  const missing = await post([...rows9, row({ yahoo_key: 'stale-03' })], { omitGeneration: true });
  eq(missing.status, 400, '世代なしは 400');
  eq(missing.json?.error, 'snapshot_generation_required', 'error=snapshot_generation_required');
}

console.log('\n── 大幅減少 (欠損 snapshot) は拒否、明示すれば通る ──');
{
  const seed = await post(manyRows(100), { });
  eq(seed.status, 200, '100 行を投入');
  const shrink = await post(manyRows(50), { });
  eq(shrink.status, 400, '50 行 (50%減) は 400');
  eq(shrink.json?.error, 'snapshot_shrink_rejected', 'error=snapshot_shrink_rejected');
  eq(db.prepare('SELECT COUNT(*) n FROM mirror_yahoo_sku_map').get().n, 100, '100 行は無傷');
  const mild = await post(manyRows(85), { });
  eq(mild.status, 200, '85 行 (15%減) は通る');
  const forced = await post(manyRows(10), { meta: { allow_shrink: true } });
  eq(forced.status, 200, 'allow_shrink=true なら 10 行でも通る');
  eq(db.prepare('SELECT COUNT(*) n FROM mirror_yahoo_sku_map').get().n, 10, '10 行になった');
}

console.log('\n── 拒否ケース: 既存 mirror を消さない ──');
const before = db.prepare('SELECT COUNT(*) n FROM mirror_yahoo_sku_map').get().n;
// 不正行のケースは「正しい 9 行 + 壊れた 1 行」で送る (件数を減らさない = 減少ゲートより先に
// 行検証で落ちることを確かめる。1 行だけ送ると減少ゲートで弾かれて行検証まで届かない)
const withBad = (bad) => [...manyRows(9), bad];
const cases = [
  ['0件 (map 全消しの事故)', [], {}, 'empty_snapshot_rejected'],
  ['0件 + 件数を言い当てられない', [], { meta: { allow_empty_snapshot: true, expected_deleted_rows: 999 } }, 'empty_snapshot_confirmation_mismatch'],
  ['chunk 分割 (途中欠落で表が欠ける)', [row()], { chunkCount: 2, isLast: false }, 'full_snapshot_requires_single_chunk'],
  ['row.source_run_id 不一致', [row()], { rowRunId: 'other-run' }, 'source_run_id_mismatch'],
  ['ne_code 欠落', withBad(row({ yahoo_key: 'bad-1', ne_code: '' })), {}, 'bad_row'],
  ['yahoo_key 欠落', withBad(row({ yahoo_key: null })), {}, 'bad_row'],
  ['store_id 欠落 (default で埋めない)', withBad(row({ yahoo_key: 'bad-3', store_id: undefined })), {}, 'bad_row'],
  ['未知の store_id', withBad(row({ yahoo_key: 'bad-4', store_id: 'other-store' })), {}, 'bad_row'],
  ['未知の resolution_source', withBad(row({ yahoo_key: 'bad-5', resolution_source: 'manuaal' })), {}, 'bad_row'],
  ['ne_code に前後空白', withBad(row({ yahoo_key: 'bad-6', ne_code: ' ne0001 ' })), {}, 'bad_row'],
];
for (const [label, rows, opts, expectedError] of cases) {
  const r = await post(rows, opts);
  ok(r.status === 400, `${label} → 400 (実際 ${r.status})`);
  eq(r.json?.error, expectedError, `  error=${expectedError}`);
  eq(db.prepare('SELECT COUNT(*) n FROM mirror_yahoo_sku_map').get().n, before, '  既存 mirror は無傷');
}

console.log('\n── 全消しは「消える件数を言い当てられる」時だけ ──');
{
  const live = db.prepare('SELECT COUNT(*) n FROM mirror_yahoo_sku_map').get().n;
  const r = await post([], { meta: { allow_empty_snapshot: true, expected_deleted_rows: live } });
  eq(r.status, 200, 'HTTP 200');
  eq(keys(), [], '空になった');
}

console.log('\n── au PAY 側 (複合PK) も同様に入る ──');
{
  const rows = [
    { store_id: 'b-faith01', aupay_key: 'item-a', ne_code: 'ne0001', resolution_source: 'manual', notes: null, created_at: null, updated_at: null },
    { store_id: 'b-faith01', aupay_key: 'item-b', ne_code: 'ne0002', resolution_source: 'manual', notes: null, created_at: null, updated_at: null },
  ];
  const r = await post(rows, { entity: 'aupay_sku_map', runId: 'aupay_sku_map-v1-test-1', generation: 1 });
  eq(r.status, 200, 'HTTP 200');
  eq(db.prepare('SELECT COUNT(*) n FROM mirror_aupay_sku_map').get().n, 2, '2行保存された');
  eq(db.prepare("SELECT generation FROM mirror_snapshot_generations WHERE entity='aupay_sku_map'").get().generation, 1,
    '世代は entity ごとに独立');
}

console.log('\n── backout は full_snapshot を受け付けない ──');
{
  const res = await fetch(`${base}/api/sync/runs/aupay_sku_map-v1-test-1/backout`, { method: 'POST' });
  const json = await res.json().catch(() => null);
  eq(res.status, 409, 'HTTP 409');
  eq(json?.error, 'backout_not_supported_for_full_snapshot', 'error=backout_not_supported_for_full_snapshot');
  eq(db.prepare('SELECT COUNT(*) n FROM mirror_aupay_sku_map').get().n, 2, 'map は消えない');
  eq(db.prepare("SELECT COUNT(*) n FROM sync_run_chunks WHERE run_id='aupay_sku_map-v1-test-1'").get().n, 1,
    'ledger も消えない');
}

server.close();
try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* Windows のロック残りは無視 */ }
console.log(`\n${failed === 0 ? '✅ 全テスト通過' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
