#!/usr/bin/env node
/**
 * fba-sheetless-parity.mjs — Sheet なしのモード (⑦-F) を「使わない」ときの動きが、前のコードとバイト単位で同じかを確かめる道具。
 *   同じ材料 (一時フォルダの fba.db と warehouse-mirror.db) で、計算 (v2・v3)・対応の一覧・除外一覧・他 CH スナップショット・
 *   FNSKU の二重書き (ASIN つきの行も渡す)・再起動の backfill・fba.db の表の形 を JSON に書き出す。
 *
 * 使い方 (本番の DB には触らない。DATA_DIR は一時フォルダ):
 *   git worktree add --detach <比べる元> origin/master   (node_modules は junction で)
 *   node scripts/fba-sheetless-parity.mjs <比べる元>  base.json
 *   node scripts/fba-sheetless-parity.mjs .            new.json
 *   cmp base.json new.json   → 同じなら何も出ない
 * 🚨 商品管理リストの 60 秒のメモが切れるのを待つので 1 分ほどかかる (前のコードにはメモを捨てる口が無い)。
 * 台帳 fba-sheetless-transition の一時物 (モードを既定にしたら消す)。
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { createRequire } from 'node:module';

const root = path.resolve(process.argv[2] || '.');
const out = process.argv[3] || 'fba-sheetless-parity.json';
const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'fba-parity-'));
process.env.DATA_DIR = dataDir;
for (const k of ['FBA_SHEETLESS_MODE', 'RENDER', 'GOOGLE_SERVICE_ACCOUNT_KEY', 'JOBS_MONITOR_ENABLED']) delete process.env[k];
process.env.FBA_SKU_MAPPING_SOURCE = 'mirror';
process.env.FBA_NONFBA_SOURCE = 'pml';
const imp = (p) => import(pathToFileURL(path.join(root, p)).href);
const Database = createRequire(path.join(root, 'package.json'))('better-sqlite3');

const db = await imp('apps/fba-replenishment/db.js');
const mirror = await imp('apps/warehouse-mirror/db.js');
mirror.initMirrorDB();
const mdb = mirror.getMirrorDB();
await db.initDb();
const { generateRecommendations } = await imp('apps/fba-replenishment/calculation-engine.js');

db.upsertSkuMappings([
  { amazon_sku: 'Alpha-1', asin: 'B0SHEETA', product_name: 'シートA', ne_code: 'alpha', logizard_code: 'alpha', non_fba_sales_7d: 70, non_fba_sales_30d: 300 },
  { amazon_sku: 'Beta-2', asin: 'B0SHEETB', product_name: 'シートB', ne_code: 'beta', logizard_code: 'beta', non_fba_sales_7d: 9, non_fba_sales_30d: 90, is_set: true, set_components: [{ ne_code: 'beta', qty: 2 }] },
  { amazon_sku: 'Sheetonly-9', asin: 'B0SHEET9', product_name: 'シートだけ', ne_code: 'zzz', logizard_code: 'zzz', non_fba_sales_30d: 5 },
  { amazon_sku: 'UPPER-7', asin: 'B0SHEETU', product_name: 'シートU', ne_code: 'alpha', logizard_code: 'alpha' },   // どのレポートにも無い大文字の SKU (大小文字の復元)
]);
db.saveNonFbaSalesSnapshot([{ amazon_sku: 'Unmapped-X', non_fba_sales_7d: 5, non_fba_sales_30d: 25 }, { amazon_sku: 'Alpha-1', non_fba_sales_7d: 1, non_fba_sales_30d: 40 }]);
db.updateFnskuBatch([{ sku: 'Alpha-1', fnsku: 'X0ALPHA', asin: 'B0REPORTA' }, { sku: 'Nomap-5', fnsku: 'X0NOMAP', asin: 'B0NOMAP' }, { sku: 'Upper-7', fnsku: 'X0UPPER', asin: 'B0UPPER' }]);
db.syncFnskuBatch([{ sku: 'Beta-2', fnsku: 'X0BETA', asin: 'B0REPORTB' }, { sku: 'Alpha-1', fnsku: null }]);
db.saveRestockLatest([
  { amazon_sku: 'Alpha-1', product_name: 'A', fba_available: 2, units_sold_30d: 60, amazon_recommended_qty: null },
  { amazon_sku: 'Beta-2', product_name: 'B', fba_available: 0, units_sold_30d: 30, amazon_recommended_qty: 20 },
  { amazon_sku: 'Gamma-3', product_name: 'G', fba_available: 1, units_sold_30d: 45, amazon_recommended_qty: null },
  { amazon_sku: 'Unmapped-X', product_name: 'X', fba_available: 0, units_sold_30d: 0, amazon_recommended_qty: null },
]);
db.replaceWarehouseInventory([
  { logizard_code: 'alpha', product_name: 'A', location: 'P-01', quantity: 200, reserved: 0, available_qty: 200, expiry_date: '', block_alloc_order: 1 },
  { logizard_code: 'beta', product_name: 'B', location: 'P-02', quantity: 80, reserved: 0, available_qty: 80, expiry_date: '2027-01-01', block_alloc_order: 1 },
  { logizard_code: 'gamma', product_name: 'G', location: 'P-03', quantity: 150, reserved: 0, available_qty: 150, expiry_date: '', block_alloc_order: 1 },
]);
db.excludeReplenishmentSku('Gamma-3', '試験');
const T = '2026-09-01T00:00:00.000Z';
for (const [sku, ne, qty, name] of [['alpha-1', 'alpha', 1, 'マスタA'], ['beta-2', 'beta', 2, 'マスタB'], ['gamma-3', 'gamma', 1, 'マスタG'], ['upper-7', 'alpha', 1, 'マスタU']]) {
  mdb.prepare(`INSERT INTO mirror_sku_resolved (seller_sku, ne_code, quantity, source, 商品名, source_updated_at, sort_order, synced_at) VALUES (?, ?, ?, 'master', ?, ?, 0, ?)`).run(sku, ne, qty, name, T, T);
  mdb.prepare(`INSERT INTO mirror_sku_master (seller_sku, 商品名, source_created_at, source_updated_at, synced_at) VALUES (?, ?, ?, ?, ?)`).run(sku, name, T, T, T);
}
const todayJst = new Date(Date.now() + 9 * 3600e3).toISOString().slice(0, 10);
const publish = () => {
  mdb.prepare('DELETE FROM mirror_pml_snapshot_rows').run(); mdb.prepare('DELETE FROM mirror_pml_published').run();
  const ins = mdb.prepare('INSERT INTO mirror_pml_snapshot_rows (run_id, 商品コード, 販売数7日_FBA以外, 販売数30日_FBA以外) VALUES (?, ?, ?, ?)');
  for (const [c, a, b] of [['alpha', 4, 12], ['beta', 0, 0], ['gamma', 1, 3]]) ins.run('run1', c, a, b);
  mdb.prepare(`INSERT INTO mirror_pml_published (id, run_id, status, as_of_date, src_velocity_as_of, row_count, synced_at) VALUES (1, 'run1', 'ok', ?, ?, 3, 'x')`).run(todayJst, todayJst);
};
const strip = (v) => JSON.parse(JSON.stringify(v, (k, x) => (['generated_at', 'updated_at', 'excluded_at'].includes(k) ? '<t>' : x)));
const O = { pendingSlips: { status: 'ok', slips: [], byCode: new Map() } };
const res = {};
publish();
res.pml_engine = strip(generateRecommendations(true, {}, O));
res.pml_engine_v3 = strip(generateRecommendations(false, {}, { ...O, rules: 'v3' }));
res.pml_map = strip(db.getSkuMappings());
res.pml_one = strip(db.getSkuMapping('Alpha-1'));
res.excluded = strip(db.getReplenishmentExcluded());
res.max60 = strip(db.getAllNonFbaMax60d());
res.max60_one = db.getNonFbaMax60d('Unmapped-X');
mdb.prepare('DELETE FROM mirror_pml_published').run();
await new Promise((r) => setTimeout(r, 61000));   // 商品管理リストの 60 秒メモが切れるのを待つ (master には捨てる口が無い)
res.nopml_engine = strip(generateRecommendations(true, {}, O));
res.nopml_map = strip(db.getSkuMappings());
res.nopml_one = strip(db.getSkuMapping('Beta-2'));
process.env.FBA_SKU_MAPPING_SOURCE = 'sheet';
res.sheet_engine = strip(generateRecommendations(true, {}, O));
res.sheet_map = strip(db.getSkuMappings());
res.sheet_one = strip(db.getSkuMapping('Beta-2'));
process.env.FBA_SKU_MAPPING_SOURCE = 'mirror';
db.upsertSkuMappings([{ amazon_sku: 'Delta-4', asin: 'B0DELTA', ne_code: 'delta' }]);
await db.initDb();
res.attrs_after_restart = strip(db.getFbaSkuAttrs().sort((a, b) => (a.amazon_sku < b.amazon_sku ? -1 : 1)));
res.sheet_after_restart = strip(db.getSkuMappingsFromSheet());
const f = new Database(path.join(dataDir, 'fba.db'), { readonly: true });
res.tables = f.prepare(`SELECT type, name, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name`).all();
f.close();
fs.writeFileSync(out, JSON.stringify(res, null, 1));
console.log('wrote', out, Object.keys(res).length);
process.exit(0);
