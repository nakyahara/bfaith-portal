/**
 * test-sku-map-sender.mjs — sync-sku-maps.js の送信側 e2e テスト (価格一括改定ツール PR1)
 *
 * 一時 DATA_DIR に warehouse.db を initDB() で作り (= contract auto-seed も通す)、
 * f_yahoo_sku_map / f_aupay_sku_map に行を入れて、ローカルに立てた mirror 受け口へ実際に送る。
 *
 * 実行: node apps/warehouse/test-sku-map-sender.mjs
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import http from 'node:http';
import { fileURLToPath } from 'node:url';
import { spawn } from 'node:child_process';
import express from 'express';
import Database from 'better-sqlite3';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO = path.resolve(HERE, '..', '..');

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const eq = (a, b, label) => ok(JSON.stringify(a) === JSON.stringify(b), `${label} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

// ── 1. mirror 側 (受け口) を一時 DATA_DIR で起動 ──
const mirrorDir = fs.mkdtempSync(path.join(os.tmpdir(), 'skumap-mirror-'));
process.env.DATA_DIR = mirrorDir;
process.env.ALLOW_INSECURE_MIRROR_SYNC = '1';
const { initMirrorDB, getMirrorDB } = await import('../warehouse-mirror/db.js');
const mirrorRouter = (await import('../warehouse-mirror/router.js')).default;
initMirrorDB();
const mirrorDb = getMirrorDB();

const app = express();
app.use('/apps/mirror', express.json({ limit: '32mb' }), mirrorRouter);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const mirrorUrl = `http://127.0.0.1:${server.address().port}/apps/mirror`;

// ── 2. miniPC 側 (送信元) の warehouse.db を作る ──
const senderDir = fs.mkdtempSync(path.join(os.tmpdir(), 'skumap-sender-'));
{
  // db.js は module-level で DATA_DIR を読むので、import 直前に差し替える
  // (mirror 側は既に import 済みなので mirrorDir を掴んだまま)
  process.env.DATA_DIR = senderDir;
  const warehouseDb = await import('./db.js');
  await warehouseDb.initDB();
  warehouseDb.getDB().close();
}
const senderDb = new Database(path.join(senderDir, 'warehouse.db'));
const contracts = senderDb.prepare(
  "SELECT entity, source_object, target_table, clear_strategy FROM sync_contracts WHERE entity IN ('yahoo_sku_map','aupay_sku_map') ORDER BY entity"
).all();
eq(contracts.length, 2, 'contract が auto-seed された');
eq(contracts.map((c) => c.clear_strategy), ['full_snapshot', 'full_snapshot'], 'clear_strategy=full_snapshot');
eq(contracts.map((c) => c.target_table), ['mirror_aupay_sku_map', 'mirror_yahoo_sku_map'], 'target_table');

senderDb.exec(fs.readFileSync(path.join(REPO, 'sql', 'yahoo', 'f_yahoo_sku_map.sql'), 'utf8'));
senderDb.exec(fs.readFileSync(path.join(REPO, 'sql', 'aupay', 'f_aupay_sku_map.sql'), 'utf8'));
const insYahoo = senderDb.prepare(
  "INSERT INTO f_yahoo_sku_map (yahoo_key, store_id, ne_code, resolution_source, notes) VALUES (?, 'b-faith01', ?, 'manual', ?)"
);
insYahoo.run('abc-01', 'ne0001', 'テスト');
insYahoo.run('abc-02', 'ne0002', null);
senderDb.prepare(
  "INSERT INTO f_aupay_sku_map (store_id, aupay_key, ne_code, resolution_source, notes) VALUES ('b-faith01', ?, ?, 'manual', NULL)"
).run('item-a', 'ne0001');
senderDb.close();

// ── 3. 送信 ──
// ★同期実行 (execFileSync) は使えない: mirror 受け口が同じプロセスの express なので、
//   親をブロックすると子の fetch に応答できず deadlock する
const runSender = (extraArgs = []) => new Promise((resolve) => {
  const env = { ...process.env, DATA_DIR: senderDir, RENDER_MIRROR_URL: mirrorUrl, MIRROR_SYNC_KEY: 'dummy' };
  const child = spawn(process.execPath, [path.join('apps', 'warehouse', 'sync-sku-maps.js'), ...extraArgs],
    { cwd: REPO, env, stdio: ['ignore', 'pipe', 'pipe'] });
  let out = '';
  child.stdout.on('data', (d) => { out += d.toString(); });
  child.stderr.on('data', (d) => { out += d.toString(); });
  child.on('close', (code) => resolve({ code, out }));
});

console.log('\n── dry-run ──');
{
  const r = await runSender(['--dry-run']);
  eq(r.code, 0, 'exit 0');
  ok(r.out.includes('Would send 1 chunk (2 rows'), 'yahoo 2行を1chunkで送る予定と出る');
  eq(mirrorDb.prepare('SELECT COUNT(*) n FROM mirror_yahoo_sku_map').get().n, 0, 'dry-run では送っていない');
}

console.log('\n── 本送信 ──');
{
  const r = await runSender();
  eq(r.code, 0, 'exit 0');
  eq(mirrorDb.prepare('SELECT yahoo_key FROM mirror_yahoo_sku_map ORDER BY yahoo_key').all().map((x) => x.yahoo_key),
    ['abc-01', 'abc-02'], 'yahoo 2行が mirror に入った');
  eq(mirrorDb.prepare('SELECT COUNT(*) n FROM mirror_aupay_sku_map').get().n, 1, 'aupay 1行が mirror に入った');
  const saved = mirrorDb.prepare("SELECT ne_code, notes, store_id FROM mirror_yahoo_sku_map WHERE yahoo_key='abc-01'").get();
  eq([saved.ne_code, saved.notes, saved.store_id], ['ne0001', 'テスト', 'b-faith01'], '値がそのまま入る');
  const runs = new Database(path.join(senderDir, 'warehouse.db'), { readonly: true })
    .prepare("SELECT status, row_count_received FROM sync_runs WHERE entity='yahoo_sku_map'").all();
  eq(runs.map((x) => x.status), ['applied'], 'sync_runs が applied で残る');
}

console.log('\n── miniPC 側で 1 行削除 → 再送で mirror からも消える ──');
{
  const db2 = new Database(path.join(senderDir, 'warehouse.db'));
  db2.prepare("DELETE FROM f_yahoo_sku_map WHERE yahoo_key='abc-01'").run();
  db2.close();
  const r = await runSender(['--entity', 'yahoo_sku_map']);
  eq(r.code, 0, 'exit 0');
  eq(mirrorDb.prepare('SELECT yahoo_key FROM mirror_yahoo_sku_map ORDER BY yahoo_key').all().map((x) => x.yahoo_key),
    ['abc-02'], '削除した map は mirror にも残らない');
}

console.log('\n── source が 0 件なら送らず失敗させる (map 全消しの事故を ok で流さない) ──');
{
  const db3 = new Database(path.join(senderDir, 'warehouse.db'));
  db3.prepare('DELETE FROM f_yahoo_sku_map').run();
  db3.close();
  const r = await runSender(['--entity', 'yahoo_sku_map']);
  eq(r.code, 1, 'exit 1');
  ok(r.out.includes('0件のため送信しない'), '理由がログに出る');
  eq(mirrorDb.prepare('SELECT COUNT(*) n FROM mirror_yahoo_sku_map').get().n, 1, 'mirror は無傷');
}

server.close();
for (const d of [mirrorDir, senderDir]) {
  try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* Windows のロック残りは無視 */ }
}
console.log(`\n${failed === 0 ? '✅ 全テスト通過' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
