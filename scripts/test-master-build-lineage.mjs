/**
 * test-master-build-lineage.mjs — m_products の作り直しの記録と、送る材料の由来 (Company DB構想 10 §6.1.1 / ③a-2 の A1〜A3)
 *
 * 固定する契約:
 *   1 作り直し (rebuildMProducts) は入れ替えと同じ取引で m_products_builds に 1 行 = 読んだ NE の印・送る形のハッシュ・SKU ごとの採用理由
 *     (例外原価・税率の補い・セット名の空欄・今回の NE の取得に無い古い行)
 *   2 NE の印が作り始めと入れ替えの時で違えば、印を信用しない (changed_during_build) / 無ければ absent
 *   3 staging が作った後に変わっていれば入れ替えない (STAGING_CHANGED) / 記録が書けなければ入れ替えも巻き戻る
 *   4 送り手 (readMaterialWithLineage) は送る中身が最新の作り直しの記録と同じときだけ由来を付ける。作り直しの後に画面で直された
 *     (m_products の UPDATE) = build_id なし (changed_after_build)。過去の記録を探して代用しない
 *   5 「Render 到達」の証跡 (masterReceiptEvidence): recorded / mismatch / not_recorded / not_replaced / unconfirmed (古い受け手)
 * 使い方: node scripts/test-master-build-lineage.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'mpb-lineage-'));
process.env.DATA_DIR = tmp;
delete process.env.DAILY_SYNC_RUN_ID;

const { initDB, getDB } = await import('../apps/warehouse/db.js');
const { rebuildMProducts, applyStagingToProduction } = await import('../apps/warehouse/rebuild-m-products.js');
const { readMasterMaterial, readMaterialWithLineage, recordBuild, readNeMarks, latestBuild, BUILD_ID_RE, MASTER_BUILD_RULE_VERSION } = await import('../apps/warehouse/master-material.js');
const { materialDigest } = await import('../apps/warehouse/material-lineage.js');
const { masterReceiptEvidence } = await import('../apps/warehouse/sync-to-render.js');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quietly = async (fn) => { const l = console.log, w = console.warn; console.log = () => {}; console.warn = () => {}; try { return await fn(); } finally { console.log = l; console.warn = w; } };

await initDB();
const db = getDB();
const T1 = '2026-09-25 07:01:14';   // 今回の NE の取得
const T0 = '2026-09-20 07:01:00';   // 前の取得 (今回の取得に無い古い行)
const insNe = db.prepare(`INSERT OR REPLACE INTO raw_ne_products (商品コード, 商品名, 仕入先コード, 原価, 売価, 取扱区分, 代表商品コード, 在庫数, 引当数, 消費税率, 作成日, synced_at)
  VALUES (?, ?, '0001', ?, 300, '取扱中', ?, 0, 0, ?, '2026-01-01', ?)`);
const insSet = db.prepare(`INSERT OR REPLACE INTO raw_ne_set_products (セット商品コード, セット商品名, セット販売価格, 商品コード, 数量, synced_at) VALUES (?, ?, 900, ?, ?, ?)`);
const setMeta = (k, v) => (v == null ? db.prepare('DELETE FROM sync_meta WHERE key = ?').run(k) : db.prepare("INSERT OR REPLACE INTO sync_meta (key, value, updated_at) VALUES (?, ?, '')").run(k, v));

db.transaction(() => {
  for (let i = 0; i < 3200; i++) insNe.run(`filler-${i}`, `ダミー${i}`, 100, '', 10, T1);   // 品質ゲート (3,000 件) を通す
  insNe.run('taxfb', '税率が空の商品', 100, '', 0, T1);             // NE の税率 0 = 空 → product_tax_rate で補う
  insNe.run('exc', '原価が空の商品', 0, '', 10, T1);                // NE の原価 0 → 例外原価
  insNe.run('gone', '今回の取得に無い商品', 100, '', 10, T0);       // 古い synced_at
  insNe.run('var-a', '代表つきの子', 100, 'var-parent', 10, T1);    // 代表商品コード (送る形の JOIN に入る)
  insSet.run('set-named', '名前ありセット', 'filler-1', 2, T1);
  insSet.run('set-blank', '', 'filler-2', 1, T1);                  // セット名が空欄
})();
db.prepare("INSERT OR REPLACE INTO product_tax_rate (sku, tax_rate) VALUES ('taxfb', 0.08)").run();
db.prepare("INSERT OR REPLACE INTO exception_genka (sku, genka) VALUES ('exc', 555)").run();
setMeta('ne_api_products_complete_at', T1);
setMeta('ne_api_setproducts_complete_at', T1);

let build1;
await ta('[1] 作り直しは入れ替えと同じ取引で記録を残す (NE の印・送る形のハッシュ・SKU ごとの理由)', async () => {
  const r = await quietly(() => rebuildMProducts());
  assert.equal(r.ok, true, JSON.stringify(r.checks));
  build1 = latestBuild(db);
  assert.match(build1.build_id, BUILD_ID_RE);
  assert.equal(build1.rule_version, MASTER_BUILD_RULE_VERSION);
  assert.equal(build1.ne_products_complete_at, T1); assert.equal(build1.ne_products_mark_note, null);
  assert.equal(build1.ne_setproducts_complete_at, T1);
  // 送る形 (m_products + raw の代表商品コード) のハッシュ = 送り手が出すものと同じ
  const m = readMasterMaterial(db);
  assert.ok(m.products.find((p) => p.商品コード === 'var-a').代表商品コード === 'var-parent');
  assert.equal(build1.products_hash, materialDigest('products', m.products).content_hash);
  assert.equal(build1.products_rows, m.products.length);
  assert.equal(build1.set_components_hash, materialDigest('set_components', m.set_components).content_hash);
  const reasons = JSON.parse(build1.reasons);
  const has = (code, reason) => reasons.some((x) => x.code === code && x.reason === reason);
  assert.ok(has('taxfb', 'tax_fallback')); assert.equal(reasons.find((x) => x.code === 'taxfb').value, 0.08);
  assert.ok(has('exc', 'exception_cost')); assert.equal(reasons.find((x) => x.code === 'exc').value, 555);
  assert.ok(has('set-blank', 'set_name_blank')); assert.ok(!has('set-named', 'set_name_blank'));
  assert.ok(has('gone', 'not_in_latest_fetch')); assert.ok(!has('filler-1', 'not_in_latest_fetch'));
  const counts = JSON.parse(build1.reason_counts);
  assert.equal(counts.tax_fallback, 1); assert.equal(counts.exception_cost, 1); assert.equal(counts.set_name_blank, 1); assert.equal(counts.not_in_latest_fetch, 1);
});

await ta('[2] NE の印が作り直しの途中で変わった・無い = 印を信用しない (今回の取得に無い古い行も判定しない)', async () => {
  const b = db.transaction(() => recordBuild(db, { startMarks: { products: '2026-09-25 06:00:00', set_components: null }, startedAt: new Date().toISOString() }))();
  assert.deepEqual(b.marks.products, { value: null, note: 'changed_during_build' });
  assert.deepEqual(b.marks.set_components, { value: null, note: 'absent' });
  assert.equal(b.reason_counts.not_in_latest_fetch, null);   // 判定できない
  const row = db.prepare('SELECT * FROM m_products_builds WHERE build_id = ?').get(b.build_id);
  assert.equal(row.ne_products_complete_at, null); assert.equal(row.ne_products_mark_note, 'changed_during_build');
  db.prepare('DELETE FROM m_products_builds WHERE build_id = ?').run(b.build_id);
});

await ta('[3] staging が作った後に変わっていれば入れ替えない / 記録が書けなければ入れ替えも巻き戻る', async () => {
  const before = db.prepare('SELECT COUNT(*) AS n FROM m_products').get().n;
  db.prepare("UPDATE m_products_staging SET 商品名 = '別の作り直しが書いた' WHERE 商品コード = 'filler-3'").run();
  assert.throws(() => applyStagingToProduction(db, { build: { expectedStagingHash: '0'.repeat(64), startMarks: readNeMarks(db), startedAt: 'x' } }), (e) => e.code === 'STAGING_CHANGED');
  assert.equal(db.prepare("SELECT 商品名 FROM m_products WHERE 商品コード = 'filler-3'").get().商品名, 'ダミー3');   // 入れ替わっていない
  // 記録が書けない → 入れ替えも巻き戻る (その場だけのトリガーで INSERT を失敗させる)
  db.exec("CREATE TEMP TRIGGER fail_build BEFORE INSERT ON m_products_builds BEGIN SELECT RAISE(ABORT, 'test: 記録が書けない'); END");
  try {
    const { stagingHash } = await import('../apps/warehouse/master-material.js');
    assert.throws(() => applyStagingToProduction(db, { build: { expectedStagingHash: stagingHash(db), startMarks: readNeMarks(db), startedAt: 'x' } }));
    assert.equal(db.prepare("SELECT 商品名 FROM m_products WHERE 商品コード = 'filler-3'").get().商品名, 'ダミー3');
    assert.equal(db.prepare('SELECT COUNT(*) AS n FROM m_products').get().n, before);
  } finally { db.exec('DROP TRIGGER temp.fail_build'); }
  db.prepare("UPDATE m_products_staging SET 商品名 = 'ダミー3' WHERE 商品コード = 'filler-3'").run();
  // 記録を付けない呼び方 (既存の呼び出し) は今までどおり
  assert.equal(applyStagingToProduction(db), null);
});

await ta('[4] 送り手は最新の作り直しの記録と中身が同じときだけ由来を付ける。画面で直された後は由来なし (過去の記録で代用しない)', async () => {
  // 直前の [3] で記録なしの入れ替えをしたので、中身は build1 と同じ (staging を元に戻した)
  let r = readMaterialWithLineage(db);
  assert.equal(r.lineage.build_id, build1.build_id);
  assert.equal(r.lineage.ne_products_complete_at, T1);
  // /register の例外原価と同じ UPDATE (作り直しの後に画面で直された)
  db.prepare("UPDATE m_products SET 原価 = 777, 原価ソース = '例外', 原価状態 = 'OVERRIDDEN' WHERE 商品コード = 'filler-5'").run();
  r = readMaterialWithLineage(db);
  assert.equal(r.lineage.build_id, null);
  assert.equal(r.lineage.reason, 'changed_after_build');
  assert.deepEqual(r.lineage.differs, ['products']);
  assert.equal(r.lineage.latest_build_id, build1.build_id);
  // 作り直せば新しい記録で由来が戻る
  await quietly(() => rebuildMProducts());
  r = readMaterialWithLineage(db);
  assert.match(r.lineage.build_id, BUILD_ID_RE);
  assert.notEqual(r.lineage.build_id, build1.build_id);
  // 記録の表が無い (作り直しの記録より前の DB) / 表はあるが記録が 1 つも無い
  const { default: Database } = await import('better-sqlite3');
  const mem = new Database(':memory:');
  mem.exec("CREATE TABLE m_products (商品コード TEXT); CREATE TABLE m_set_components (セット商品コード TEXT); CREATE TABLE raw_ne_products (商品コード TEXT, 代表商品コード TEXT); INSERT INTO m_products VALUES ('a')");
  assert.equal(readMaterialWithLineage(mem).lineage.reason, 'no_build_table');
  mem.exec('CREATE TABLE m_products_builds (build_id TEXT, published_at TEXT)');
  assert.equal(readMaterialWithLineage(mem).lineage.reason, 'no_build_record');
  mem.close();
});

await ta('[5] 「Render 到達」の証跡: recorded / mismatch / not_recorded / not_replaced / unconfirmed', async () => {
  const gen = { generation_id: 'mat_20260925T000000000Z_00000000_000000', products: { row_count: 2, content_hash: 'a'.repeat(64) }, set_components: { row_count: 1, content_hash: 'b'.repeat(64) } };
  const lineage = { build_id: 'mpb_20260925T000000000Z_000000' };
  const masterPart = { products: [{}, {}], set_components: [{}] };
  let e = masterReceiptEvidence({ generation: gen, lineage, masterPart, response: { ok: true, material_recorded: {
    products: { recorded: true, generation_id: gen.generation_id, content_hash: 'a'.repeat(64) },
    set_components: { recorded: false, reason: '入れた中身が世代と合わない' } } } });
  assert.equal(e.entities.products.status, 'recorded');
  assert.equal(e.entities.set_components.status, 'not_recorded'); assert.match(e.entities.set_components.reason, /合わない/);
  assert.equal(e.build_id, lineage.build_id); assert.equal(e.entities.products.sent_rows, 2);
  e = masterReceiptEvidence({ generation: gen, lineage, masterPart, response: { ok: true, material_recorded: { products: { recorded: true, generation_id: 'mat_other', content_hash: 'a'.repeat(64) } } } });
  assert.equal(e.entities.products.status, 'mismatch');
  assert.equal(e.entities.set_components.status, 'not_replaced');   // 応答に無い = 入れ替えていない (記録の成功にしない)
  e = masterReceiptEvidence({ generation: gen, lineage: { build_id: null, reason: 'changed_after_build' }, masterPart, response: { ok: true } });
  assert.equal(e.entities.products.status, 'unconfirmed');   // 古い受け手 = 確認できない
  assert.equal(e.build_id, null); assert.equal(e.lineage_reason, 'changed_after_build');
});

try { getDB().close(); } catch { /* */ }
try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* Windows は OS に任せる */ }
console.log(`\n${passed} 件 PASS`);
process.exit(process.exitCode || 0);
