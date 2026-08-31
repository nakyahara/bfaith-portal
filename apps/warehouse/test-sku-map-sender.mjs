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
// ne_code は m_products に実在するものしか送れない (送信側の参照整合性チェック)
const insProduct = senderDb.prepare(
  "INSERT INTO m_products (商品コード, 商品名, 商品区分, 原価状態, updated_at) VALUES (?, ?, '単品', '確定', '2026-08-28T00:00:00Z')"
);
for (let i = 1; i <= 12; i++) insProduct.run(`ne${String(i).padStart(4, '0')}`, `商品${i}`);
const insYahoo = senderDb.prepare(
  "INSERT INTO f_yahoo_sku_map (yahoo_key, store_id, ne_code, resolution_source, notes) VALUES (?, 'b-faith01', ?, 'manual', ?)"
);
insYahoo.run('abc-01', 'ne0001', 'テスト');
insYahoo.run('abc-02', 'ne0002', null);
insYahoo.run('abc-03', 'ne0003', null);
insYahoo.run('abc-04', 'ne0004', null);
insYahoo.run('abc-05', 'ne0005', null);
senderDb.prepare(
  "INSERT INTO f_aupay_sku_map (store_id, aupay_key, ne_code, resolution_source, notes) VALUES ('b-faith01', ?, ?, 'manual', NULL)"
).run('item-a', 'ne0001');
// 送料マスタ (initDB が作る shipping_rates に数行入れる)
const insRate = senderDb.prepare(
  'INSERT INTO shipping_rates (shipping_code, 大分類区分, 運送会社, 小分類区分名称, 梱包サイズ, 最大重量,'
  + ' 追跡有無, 送料, 出荷作業料, 想定梱包資材費, 想定人件費, 配送関係費合計, 備考, synced_at)'
  + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?, '2026-08-31T00:00:00Z')"
);
insRate.run('103', '郵便', '日本郵便', '定形外規格内（50g以内）', '', '50', '無', 140, 20, 12, 10, 182, '');
insRate.run('501', '宅配', 'ヤマト運輸', 'ネコポス', '', '1000', '有', 198, 20, 9, 10, 237, '');
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

const senderConn = () => new Database(path.join(senderDir, 'warehouse.db'));
const yahooKeys = () => mirrorDb.prepare('SELECT yahoo_key FROM mirror_yahoo_sku_map ORDER BY yahoo_key').all().map((x) => x.yahoo_key);

console.log('\n── dry-run ──');
{
  const r = await runSender(['--dry-run']);
  eq(r.code, 0, 'exit 0');
  ok(r.out.includes('Would send 1 chunk (5 rows'), 'yahoo 5行を1chunkで送る予定と出る');
  eq(mirrorDb.prepare('SELECT COUNT(*) n FROM mirror_yahoo_sku_map').get().n, 0, 'dry-run では送っていない');
  const gen = senderConn().prepare("SELECT COUNT(*) n FROM sync_snapshot_generations").get().n;
  eq(gen, 0, 'dry-run では世代を消費しない');
}

console.log('\n── 本送信 ──');
{
  const r = await runSender();
  eq(r.code, 0, 'exit 0');
  eq(yahooKeys(), ['abc-01', 'abc-02', 'abc-03', 'abc-04', 'abc-05'], 'yahoo 5行が mirror に入った');
  eq(mirrorDb.prepare('SELECT COUNT(*) n FROM mirror_aupay_sku_map').get().n, 1, 'aupay 1行が mirror に入った');
  const saved = mirrorDb.prepare("SELECT ne_code, notes, store_id, source_row_hash FROM mirror_yahoo_sku_map WHERE yahoo_key='abc-01'").get();
  eq([saved.ne_code, saved.notes, saved.store_id], ['ne0001', 'テスト', 'b-faith01'], '値がそのまま入る');
  ok(/^[0-9a-f]{16}$/.test(saved.source_row_hash), 'source_row_hash は受け側が付与');
  const sdb = senderConn();
  eq(sdb.prepare("SELECT status FROM sync_runs WHERE entity='yahoo_sku_map'").all().map((x) => x.status),
    ['applied'], 'sync_runs が applied で残る');
  eq(sdb.prepare("SELECT generation FROM sync_snapshot_generations WHERE entity='yahoo_sku_map'").get().generation,
    1, '世代 1 を消費');
  eq(mirrorDb.prepare("SELECT generation FROM mirror_snapshot_generations WHERE entity='yahoo_sku_map'").get().generation,
    1, 'mirror 側も世代 1');
  sdb.close();
}

console.log('\n── 送料マスタも一緒に同期される ──');
{
  const rates = mirrorDb.prepare('SELECT shipping_code, 小分類区分名称 AS name, 配送関係費合計 AS total FROM mirror_shipping_rates ORDER BY shipping_code').all();
  eq(rates.map((r) => [r.shipping_code, r.name, r.total]),
    [['103', '定形外規格内（50g以内）', 182], ['501', 'ネコポス', 237]],
    '★配送関係費合計まで mirror に載る (モール別粗利の根拠)');
}

console.log('\n── miniPC 側で 1 行削除 → 再送で mirror からも消える ──');
{
  const db2 = senderConn();
  db2.prepare("DELETE FROM f_yahoo_sku_map WHERE yahoo_key='abc-01'").run();
  db2.close();
  const r = await runSender(['--entity', 'yahoo_sku_map']);
  eq(r.code, 0, 'exit 0');
  eq(yahooKeys(), ['abc-02', 'abc-03', 'abc-04', 'abc-05'], '削除した map は mirror にも残らない');
}

console.log('\n── 大幅減少は送らない (--allow-shrink で解除) ──');
{
  const db3 = senderConn();
  db3.prepare("DELETE FROM f_yahoo_sku_map WHERE yahoo_key IN ('abc-03','abc-04')").run();
  db3.close();
  const r = await runSender(['--entity', 'yahoo_sku_map']);
  eq(r.code, 1, 'exit 1');
  ok(r.out.includes('欠損の可能性があるため送信しない'), '理由がログに出る');
  eq(yahooKeys().length, 4, 'mirror は無傷');
  const forced = await runSender(['--entity', 'yahoo_sku_map', '--allow-shrink']);
  eq(forced.code, 0, '--allow-shrink なら通る');
  eq(yahooKeys(), ['abc-02', 'abc-05'], '意図した削除が反映される');
}

console.log('\n── 壊れた map は送らない (ne_code が m_products に無い等) ──');
{
  const db4 = senderConn();
  db4.prepare("INSERT INTO f_yahoo_sku_map (yahoo_key, store_id, ne_code, resolution_source) VALUES ('bad-1','b-faith01','ne9999','manual')").run();
  db4.prepare("INSERT INTO f_yahoo_sku_map (yahoo_key, store_id, ne_code, resolution_source) VALUES ('bad-2','b-faith01',' ne0002 ','manual')").run();
  db4.close();
  const r = await runSender(['--entity', 'yahoo_sku_map']);
  eq(r.code, 1, 'exit 1');
  ok(r.out.includes('m_products に無い'), '廃番/タイポの ne_code を指摘');
  ok(r.out.includes('前後空白あり'), '前後空白を指摘');
  eq(yahooKeys(), ['abc-02', 'abc-05'], 'mirror は無傷 (一部だけ送らない)');
  const db5 = senderConn();
  db5.prepare("DELETE FROM f_yahoo_sku_map WHERE yahoo_key IN ('bad-1','bad-2')").run();
  db5.close();
}

console.log('\n── ne_code の表記ゆれは m_products の正本表記に直して送る ──');
{
  const db7 = senderConn();
  // マスタは 'ne0006'。map 側が 'NE0006' でも突合は通る (SKU は LOWER(TRIM) が家ルール) が、
  // mirror に非正本表記が残ると価格改定側の完全一致 JOIN で「存在しないコード」になる
  db7.prepare("INSERT INTO f_yahoo_sku_map (yahoo_key, store_id, ne_code, resolution_source) VALUES ('abc-06','b-faith01','NE0006','manual')").run();
  db7.close();
  const r = await runSender(['--entity', 'yahoo_sku_map']);
  eq(r.code, 0, 'exit 0');
  ok(r.out.includes('正本表記に直した行: 1'), '直した件数がログに出る');
  eq(mirrorDb.prepare("SELECT ne_code FROM mirror_yahoo_sku_map WHERE yahoo_key='abc-06'").get().ne_code,
    'ne0006', 'mirror には正本表記で入る');
}

console.log('\n── 同期実績があるのに 0 件になったら失敗させる (map 消失の事故を ok で流さない) ──');
{
  const db6 = senderConn();
  db6.prepare('DELETE FROM f_yahoo_sku_map').run();
  db6.close();
  const r = await runSender(['--entity', 'yahoo_sku_map']);
  eq(r.code, 1, 'exit 1');
  ok(r.out.includes('0 件になりました'), '前回件数つきで理由がログに出る');
  eq(yahooKeys().length, 3, 'mirror は無傷');
}

console.log('\n── 一度も同期していない & 0 件は「未登録」として skip (毎日赤くしない) ──');
{
  // au PAY 側の map を空にして、同期実績も無い状態を作る (= 手動 map 未登録の実際の状態)
  const db7 = senderConn();
  db7.prepare('DELETE FROM f_aupay_sku_map').run();
  db7.prepare("DELETE FROM sync_runs WHERE entity = 'aupay_sku_map'").run();
  db7.close();
  const r = await runSender(['--entity', 'aupay_sku_map']);
  eq(r.code, 0, '★exit 0 (未登録は事故ではない)');
  ok(r.out.includes('未登録'), 'ログに「未登録」と出る');
  ok(r.out.includes('skip'), 'skip したと分かる');
  eq(mirrorDb.prepare('SELECT COUNT(*) n FROM mirror_aupay_sku_map').get().n, 1, 'mirror は前回のまま (消さない)');
}

server.close();
for (const d of [mirrorDir, senderDir]) {
  try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* Windows のロック残りは無視 */ }
}
console.log(`\n${failed === 0 ? '✅ 全テスト通過' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
