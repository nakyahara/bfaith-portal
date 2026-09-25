/**
 * test-master-build-lineage.mjs — m_products の作り直しの記録と、送る材料の由来 (Company DB構想 10 §6.1.1 / ③a-2 の A1〜A3)
 *
 * 固定する契約:
 *   1 作り直し (rebuildMProducts) は入れ替えと同じ取引で m_products_builds に 1 行 = 読んだ NE の印・送る形のハッシュ・
 *     値を決めたその場で集めた SKU ごとの理由 (例外原価・税率の補い / 決まらない・セット名の空欄・今回の NE の取得に無い古い行・例外の商品の税率)
 *   2 NE の印を信用するのは「印がある・印の通し番号 = 今の番号・作り始めから変わっていない」ときだけ
 *     (absent / written_after_complete = 印の後に別の取込が書いた / changed_during_build)。理由は後から raw を読み直して推定しない
 *   3 作り直しの札: 別の作り直しが実行中なら何もしない (REBUILD_LOCKED)・入れ替えの時に札が自分のものでなければ入れ替えない (LOCK_LOST)・
 *     staging が品質チェックの前と違えば入れ替えない (STAGING_CHANGED)・記録が書けなければ入れ替えも巻き戻る
 *   4 送り手 (readMaterialWithLineage) は送る中身が最新の作り直しの記録と同じときだけ由来を付ける。作り直しの後に画面で直された
 *     (m_products の UPDATE) = build_id なし (changed_after_build)。過去の記録を探して代用しない
 *   5 「Render 到達」の証跡 (masterReceiptEvidence): recorded / mismatch / not_recorded / not_replaced / unconfirmed (古い受け手・送信の失敗)
 * 使い方: node scripts/test-master-build-lineage.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'mpb-lineage-'));
process.env.DATA_DIR = tmp;
delete process.env.DAILY_SYNC_RUN_ID;

const { initDB, getDB, readNeRawRev } = await import('../apps/warehouse/db.js');
const { rebuildMProducts, applyStagingToProduction } = await import('../apps/warehouse/rebuild-m-products.js');
const {
  readMasterMaterial, readMaterialWithLineage, recordBuild, readNeMarks, judgeNeMark, latestBuild, stagingHash,
  acquireRebuildLock, releaseRebuildLock, holdsRebuildLock, REBUILD_LOCK_KEY, BUILD_ID_RE, MASTER_BUILD_RULE_VERSION,
} = await import('../apps/warehouse/master-material.js');
const { materialDigest } = await import('../apps/warehouse/material-lineage.js');
const { masterReceiptEvidence } = await import('../apps/warehouse/sync-to-render.js');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quietly = async (fn) => { const l = console.log, w = console.warn, er = console.error; console.log = () => {}; console.warn = () => {}; console.error = () => {}; try { return await fn(); } finally { console.log = l; console.warn = w; console.error = er; } };

await initDB();
const db = getDB();
const T1 = '2026-09-25 07:01:14';   // 今回の NE の取得
const T0 = '2026-09-20 07:01:00';   // 前の取得 (今回の取得に無い古い行)
const insNe = db.prepare(`INSERT OR REPLACE INTO raw_ne_products (商品コード, 商品名, 仕入先コード, 原価, 売価, 取扱区分, 代表商品コード, 在庫数, 引当数, 消費税率, 作成日, synced_at)
  VALUES (?, ?, '0001', ?, 300, '取扱中', ?, 0, 0, ?, '2026-01-01', ?)`);
const insSet = db.prepare(`INSERT OR REPLACE INTO raw_ne_set_products (セット商品コード, セット商品名, セット販売価格, 商品コード, 数量, synced_at) VALUES (?, ?, 900, ?, ?, ?)`);
const setMeta = (k, v) => db.prepare("INSERT OR REPLACE INTO sync_meta (key, value, updated_at) VALUES (?, ?, '')").run(k, v);
/** NE の取込が最後まで終わった状態にする (ne-api.js と同じ: 印の時刻と、その時点の通し番号) */
const markComplete = () => {
  setMeta('ne_api_products_complete_at', T1); setMeta('ne_api_products_complete_rev', String(readNeRawRev('products')));
  setMeta('ne_api_setproducts_complete_at', T1); setMeta('ne_api_setproducts_complete_rev', String(readNeRawRev('setproducts')));
};

db.transaction(() => {
  for (let i = 0; i < 3200; i++) insNe.run(`filler-${i}`, `ダミー${i}`, 100, '', 10, T1);   // 品質ゲート (3,000 件) を通す
  insNe.run('taxfb', '税率が空の商品', 100, '', 0, T1);             // NE の税率 0 = 空 → product_tax_rate で補う
  insNe.run('taxunk', '税率が空で補いも無い商品', 100, '', 0, T1);   // 決まらない
  insNe.run('exc', '原価が空の商品', 0, '', 10, T1);                // NE の原価 0 → 例外原価
  insNe.run('gone', '今回の取得に無い商品', 100, '', 10, T0);       // 古い synced_at
  insNe.run('var-a', '代表つきの子', 100, 'var-parent', 10, T1);    // 代表商品コード (送る形の JOIN に入る)
  insSet.run('set-named', '名前ありセット', 'filler-1', 2, T1);
  insSet.run('set-blank', '', 'filler-2', 1, T1);                  // セット名が空欄
})();
db.prepare("INSERT OR REPLACE INTO product_tax_rate (sku, tax_rate) VALUES ('taxfb', 0.08), ('exonly', 0.1)").run();
db.prepare("INSERT OR REPLACE INTO exception_genka (sku, genka) VALUES ('exc', 555), ('exonly', 321)").run();   // exonly = 例外にだけある商品
markComplete();

let build1;
await ta('[1] 作り直しは入れ替えと同じ取引で記録を残す (NE の印・送る形のハッシュ・値を決めたその場の理由)', async () => {
  const r = await quietly(() => rebuildMProducts());
  assert.equal(r.ok, true, JSON.stringify(r.checks));
  build1 = latestBuild(db);
  assert.match(build1.build_id, BUILD_ID_RE);
  assert.equal(build1.rule_version, MASTER_BUILD_RULE_VERSION);
  assert.equal(build1.ne_products_complete_at, T1); assert.equal(build1.ne_products_mark_note, null);
  assert.equal(build1.ne_setproducts_complete_at, T1);
  assert.equal(db.prepare('SELECT value FROM sync_meta WHERE key = ?').get(REBUILD_LOCK_KEY), undefined);   // 札は返した
  // 送る形 (m_products + raw の代表商品コード) のハッシュ = 送り手が出すものと同じ
  const m = readMasterMaterial(db);
  assert.equal(m.products.find((p) => p.商品コード === 'var-a').代表商品コード, 'var-parent');
  assert.equal(build1.products_hash, materialDigest('products', m.products).content_hash);
  assert.equal(build1.products_rows, m.products.length);
  assert.equal(build1.set_components_hash, materialDigest('set_components', m.set_components).content_hash);
  const reasons = JSON.parse(build1.reasons);
  const of = (code, reason) => reasons.filter((x) => x.code === code && x.reason === reason);
  assert.deepEqual(of('taxfb', 'tax_fallback').map((x) => [x.value, x.source, x.ne_value]), [[0.08, 'product_tax_rate', 0]]);
  assert.equal(of('taxunk', 'tax_unresolved').length, 1);
  assert.deepEqual(of('exc', 'exception_cost').map((x) => [x.kind, x.value, x.ne_value]), [['単品', 555, 0]]);
  assert.deepEqual(of('exonly', 'exception_cost').map((x) => x.kind), ['例外']);
  assert.deepEqual(of('exonly', 'exception_tax_manual').map((x) => x.value), [0.1]);
  assert.equal(of('set-blank', 'set_name_blank').length, 1); assert.equal(of('set-named', 'set_name_blank').length, 0);
  assert.equal(of('gone', 'not_in_latest_fetch').length, 1); assert.equal(of('filler-1', 'not_in_latest_fetch').length, 0);
  assert.equal(of('filler-1', 'tax_fallback').length, 0);
  const counts = JSON.parse(build1.reason_counts);
  assert.equal(counts.tax_fallback, 1); assert.equal(counts.not_in_latest_fetch, 1); assert.equal(counts.set_name_blank, 1);
});

await ta('[2] NE の印を信用するのは、印の番号 = 今の番号・作り始めから変わっていないときだけ。理由は渡されたものだけ (raw を読み直さない)', async () => {
  const S = { at: T1, completeRev: 10, rev: 10 };
  assert.deepEqual(judgeNeMark(S, S), { value: T1, note: null });
  assert.deepEqual(judgeNeMark({ at: null, completeRev: null, rev: 10 }, S), { value: null, note: 'absent' });
  assert.deepEqual(judgeNeMark({ at: T1, completeRev: null, rev: 10 }, S), { value: null, note: 'absent' });   // 番号の無い古い印
  assert.deepEqual(judgeNeMark({ ...S, rev: 11 }, { ...S, rev: 11 }), { value: null, note: 'written_after_complete' });
  assert.deepEqual(judgeNeMark(S, { ...S, rev: 11 }), { value: null, note: 'changed_during_build' });
  // Codex R1 High-2 の経路: 取込 B が印を付けた後に、並行する取込 A のページが raw に書かれた → 作り直しは印を信用しない
  insNe.run('filler-7', 'A のページ', 100, '', 10, '2026-09-25 07:02:00');
  const r = await quietly(() => rebuildMProducts());
  assert.equal(r.ok, true);
  const b = latestBuild(db);
  assert.equal(b.ne_products_complete_at, null); assert.equal(b.ne_products_mark_note, 'written_after_complete');
  assert.equal(JSON.parse(b.reason_counts).not_in_latest_fetch, null);   // 印が信用できない = 古い行を判定しない
  assert.ok(!JSON.parse(b.reasons).some((x) => x.reason === 'not_in_latest_fetch'));
  // 作り直しの途中で raw が書かれた (作り始めの印と入れ替えの時の番号が違う)
  insNe.run('filler-7', 'ダミー7', 100, '', 10, T1); markComplete();
  const start = readNeMarks(db);
  insNe.run('filler-8', '途中の書き込み', 100, '', 10, T1);
  const b2 = db.transaction(() => recordBuild(db, { startMarks: start, startedAt: 'x', reasons: [] }))();
  assert.deepEqual(b2.marks.products, { value: null, note: 'changed_during_build' });
  // 理由は渡されたものだけ: raw の taxfb は税率 0 のままでも、渡していなければ記録しない (Codex R1 Medium-3)
  assert.deepEqual(JSON.parse(db.prepare('SELECT reasons FROM m_products_builds WHERE build_id = ?').get(b2.build_id).reasons), []);
  db.prepare('DELETE FROM m_products_builds WHERE build_id = ?').run(b2.build_id);
  insNe.run('filler-8', 'ダミー8', 100, '', 10, T1); markComplete();
});

await ta('[3] 作り直しの札 (実行中なら何もしない・期限切れは取り直す・入れ替えの時に札を確かめる) / staging の変化 / 記録の失敗で巻き戻る', async () => {
  const name3 = () => db.prepare("SELECT 商品名 FROM m_products WHERE 商品コード = 'filler-3'").get().商品名;
  // 別の作り直しが札を持っている (Codex R1 High-1 の経路 = 共有の staging を他の作り直しに書かせない)
  assert.equal(acquireRebuildLock(db, 'mpb_other'), true);
  db.prepare("UPDATE raw_ne_products SET 商品名 = '札が無いと入る名前' WHERE 商品コード = 'filler-3'").run();
  const locked = await quietly(() => rebuildMProducts());
  assert.equal(locked.ok, false); assert.equal(locked.error, 'REBUILD_LOCKED');
  assert.equal(name3(), 'ダミー3');   // staging も m_products も触っていない
  assert.equal(holdsRebuildLock(db, 'mpb_other'), true);
  // 落ちたまま残った札 (期限切れ) は取り直す
  db.prepare("UPDATE sync_meta SET updated_at = '2026-01-01T00:00:00.000Z' WHERE key = ?").run(REBUILD_LOCK_KEY);
  assert.equal((await quietly(() => rebuildMProducts())).ok, true);
  assert.equal(name3(), '札が無いと入る名前');
  db.prepare("UPDATE raw_ne_products SET 商品名 = 'ダミー3' WHERE 商品コード = 'filler-3'").run(); markComplete();
  await quietly(() => rebuildMProducts());
  // 入れ替えの時に札が自分のものでない
  assert.throws(() => applyStagingToProduction(db, { build: { buildId: 'mpb_not_mine', expectedStagingHash: stagingHash(db), startMarks: readNeMarks(db), startedAt: 'x', reasons: [] } }), (e) => e.code === 'LOCK_LOST');
  // 札を持っていても staging が変わっていれば入れ替えない
  const me = 'mpb_20260925T000000000Z_aaaaaa';
  assert.equal(acquireRebuildLock(db, me), true);
  try {
    const expected = stagingHash(db);
    db.prepare("UPDATE m_products_staging SET 商品名 = '別の作り直しが書いた' WHERE 商品コード = 'filler-3'").run();
    assert.throws(() => applyStagingToProduction(db, { build: { buildId: me, expectedStagingHash: expected, startMarks: readNeMarks(db), startedAt: 'x', reasons: [] } }), (e) => e.code === 'STAGING_CHANGED');
    assert.equal(name3(), 'ダミー3');
    db.prepare("UPDATE m_products_staging SET 商品名 = 'ダミー3' WHERE 商品コード = 'filler-3'").run();
    // 記録が書けない → 入れ替えも巻き戻る (その場だけのトリガーで INSERT を失敗させる)
    const before = db.prepare('SELECT COUNT(*) AS n FROM m_products').get().n;
    db.prepare("UPDATE m_products_staging SET 商品名 = '巻き戻るはずの名前' WHERE 商品コード = 'filler-3'").run();
    db.exec("CREATE TEMP TRIGGER fail_build BEFORE INSERT ON m_products_builds BEGIN SELECT RAISE(ABORT, 'test: 記録が書けない'); END");
    try {
      assert.throws(() => applyStagingToProduction(db, { build: { buildId: me, expectedStagingHash: stagingHash(db), startMarks: readNeMarks(db), startedAt: 'x', reasons: [] } }));
      assert.equal(name3(), 'ダミー3');
      assert.equal(db.prepare('SELECT COUNT(*) AS n FROM m_products').get().n, before);
    } finally { db.exec('DROP TRIGGER temp.fail_build'); }
    db.prepare("UPDATE m_products_staging SET 商品名 = 'ダミー3' WHERE 商品コード = 'filler-3'").run();
  } finally { releaseRebuildLock(db, me); }
  // 記録を付けない呼び方 (既存の呼び出し) は今までどおり
  assert.equal(applyStagingToProduction(db), null);
});

await ta('[4] 送り手は最新の作り直しの記録と中身が同じときだけ由来を付ける。画面で直された後は由来なし (過去の記録で代用しない)', async () => {
  await quietly(() => rebuildMProducts());
  const latest = latestBuild(db);
  let r = readMaterialWithLineage(db);
  assert.equal(r.lineage.build_id, latest.build_id);
  assert.equal(r.lineage.ne_products_complete_at, T1);
  // /register の例外原価と同じ UPDATE (作り直しの後に画面で直された)
  db.prepare("UPDATE m_products SET 原価 = 777, 原価ソース = '例外', 原価状態 = 'OVERRIDDEN' WHERE 商品コード = 'filler-5'").run();
  r = readMaterialWithLineage(db);
  assert.equal(r.lineage.build_id, null);
  assert.equal(r.lineage.reason, 'changed_after_build');
  assert.deepEqual(r.lineage.differs, ['products']);
  assert.equal(r.lineage.latest_build_id, latest.build_id);
  // 作り直せば新しい記録で由来が戻る
  await quietly(() => rebuildMProducts());
  r = readMaterialWithLineage(db);
  assert.match(r.lineage.build_id, BUILD_ID_RE);
  assert.notEqual(r.lineage.build_id, latest.build_id);
  // 記録の表が無い (作り直しの記録より前の DB) / 表はあるが記録が 1 つも無い
  const { default: Database } = await import('better-sqlite3');
  const mem = new Database(':memory:');
  mem.exec("CREATE TABLE m_products (商品コード TEXT); CREATE TABLE m_set_components (セット商品コード TEXT); CREATE TABLE raw_ne_products (商品コード TEXT, 代表商品コード TEXT); INSERT INTO m_products VALUES ('a')");
  assert.equal(readMaterialWithLineage(mem).lineage.reason, 'no_build_table');
  mem.exec('CREATE TABLE m_products_builds (build_id TEXT, published_at TEXT)');
  assert.equal(readMaterialWithLineage(mem).lineage.reason, 'no_build_record');
  mem.close();
});

await ta('[5] 「Render 到達」の証跡: recorded / mismatch / not_recorded / not_replaced / unconfirmed (古い受け手・送信の失敗)', async () => {
  const gen = { generation_id: 'mat_20260925T000000000Z_00000000_000000', products: { row_count: 2, content_hash: 'a'.repeat(64) }, set_components: { row_count: 1, content_hash: 'b'.repeat(64) } };
  const lineage = { build_id: 'mpb_20260925T000000000Z_000000' };
  const masterPart = { products: [{}, {}], set_components: [{}] };
  let e = masterReceiptEvidence({ generation: gen, lineage, masterPart, response: { ok: true, material_recorded: {
    products: { recorded: true, generation_id: gen.generation_id, content_hash: 'a'.repeat(64) },
    set_components: { recorded: false, reason: '入れた中身が世代と合わない' } } } });
  assert.equal(e.entities.products.status, 'recorded');
  assert.equal(e.entities.set_components.status, 'not_recorded'); assert.match(e.entities.set_components.reason, /合わない/);
  assert.equal(e.build_id, lineage.build_id); assert.equal(e.entities.products.sent_rows, 2); assert.equal(e.send_error, null);
  e = masterReceiptEvidence({ generation: gen, lineage, masterPart, response: { ok: true, material_recorded: { products: { recorded: true, generation_id: 'mat_other', content_hash: 'a'.repeat(64) } } } });
  assert.equal(e.entities.products.status, 'mismatch');
  assert.equal(e.entities.set_components.status, 'not_replaced');   // 応答に無い = 入れ替えていない (記録の成功にしない)
  e = masterReceiptEvidence({ generation: gen, lineage: { build_id: null, reason: 'changed_after_build' }, masterPart, response: { ok: true } });
  assert.equal(e.entities.products.status, 'unconfirmed');   // 古い受け手 = 確認できない
  assert.equal(e.build_id, null); assert.equal(e.lineage_reason, 'changed_after_build');
  // 送信が失敗した (timeout など) = 受け手に反映済みかもしれない → 確認できない (前の回の recorded を残さないように、この回の証跡を書く)
  e = masterReceiptEvidence({ generation: gen, lineage, masterPart, response: null, error: new Error('マスタ: HTTP 504') });
  assert.deepEqual([e.entities.products.status, e.entities.set_components.status], ['unconfirmed', 'unconfirmed']);
  assert.match(e.entities.products.reason, /送信が失敗/); assert.match(e.send_error, /504/);
});

try { getDB().close(); } catch { /* */ }
try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* Windows は OS に任せる */ }
console.log(`\n${passed} 件 PASS`);
process.exit(process.exitCode || 0);
