/**
 * test-publish.mjs — 世代の転送と公開 受入試験 (§10.1 障害 / §15-7)
 *
 * HTTP は使わず、受け口の関数を直接呼ぶ (送信側と受信側を同じ経路で通す)。
 * 実行: node apps/expected-profit/test-publish.mjs
 */
import assert from 'node:assert/strict';
import fs from 'fs';
import path from 'path';
import os from 'os';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'ep-pub-'));

const { initExpectedProfitDB } = await import('./db.js');
const { receiveChunk, publishGeneration, getPublished, chunkChecksum, pruneGenerations, generationContentHash } = await import('./publish-api.js');
const { makeChunks, makeManifest, publishToRender } = await import('./publish.js');
const { hashRows } = await import('./generation-hash.js');

let passed = 0;
function t(name, fn) {
  try { fn(); passed++; console.log(`  ok  ${name}`); }
  catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; }
}
async function ta(name, fn) {
  try { await fn(); passed++; console.log(`  ok  ${name}`); }
  catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; }
}

const db = initExpectedProfitDB();

const mkRow = (key, over = {}) => ({
  generation_id: null, mall: 'rakuten', shop_id: '1', mall_item_key: key, ne_code: 'ne001',
  product_name: 'x', sales_class: 3, fulfillment: 'self', listing_status: 'active',
  price_incl_tax: 1100, price_ex_tax: 1000, postage_revenue_ex_tax: 0, revenue_ex_tax: 1000, tax_rate: 0.1,
  cost_ex_tax: 600, cost_method: 'single', shipping_code: '501', shipping_method: 'ネコポス',
  shipping_fee_ex_tax: 180, shipping_work_ex_tax: 20, shipping_material_ex_tax: 10, shipping_labor_ex_tax: 9,
  shipping_total_ex_tax: 219, fba_fee_ex_tax: 0, referral_fee_ex_tax: null, closing_fee_ex_tax: null,
  per_item_fee_ex_tax: null, fee_total_ex_tax: 100, fee_rate_display: 0.1, fee_breakdown: null,
  expected_profit: 81, expected_margin_rate: 0.081,
  listing_enum_status: 'ok', listing_enum_valid_until: '2099-01-01T00:00:00Z',
  price_status: 'ok', price_valid_until: '2099-01-01T00:00:00Z', fee_status: 'not_applicable', fee_valid_until: null,
  cost_status: 'ok', cost_valid_until: '2099-01-01T00:00:00Z',
  shipping_master_status: 'ok', shipping_master_valid_until: '2099-01-01T00:00:00Z',
  shipping_revenue_status: 'included', scenario_fit: 'ok', calculation_status: 'ok',
  incomplete_reason: null, rank_eligible: 1, rank_exclusion_reason: null, expense_scope_version: 'self_v1',
  input_snapshot: '{}', formula_version: 'v1', scenario_version: 'v1', fee_rate_version: 'v1',
  code_version: 'test', price_run_id: 'r1', built_at: '2026-09-07T00:00:00Z', ...over,
});

// 🚨 本番と同じ流れ: 送信側が自分の行から計算したハッシュを manifest に入れて送る。
//    受信側はそれを保存し、全チャンク受信後に自分で再計算して照合する
const manifestOf = (rowCount, rows = null, over = {}) => ({
  built_at: '2026-09-07T00:00:00Z', row_count: rowCount, ok_count: rowCount,
  incomplete_count: 0, rank_eligible_count: rowCount, malls_included: ['rakuten'],
  malls_degraded: [], content_hash: rows ? hashRows(rows) : undefined, ...over,
});
// 公開時は manifest を省略できる (受信時に保存した値が基準)
const publishOf = (genId, over = {}) => ({ generation_id: genId, ...over });

console.log('チャンクの受け取り');

t('checksum が合えば受け取る', () => {
  const rows = [mkRow('a'), mkRow('b')];
  const r = receiveChunk(db, {
    generationId: 'g1', seq: 1, chunkIndex: 0,
    checksum: chunkChecksum(rows), rows, manifest: manifestOf(2, rows),
  });
  assert.equal(r.ok, true);
  assert.equal(r.total, 2);
});

t('[!] checksum が合わなければ拒否する', () => {
  const rows = [mkRow('c')];
  const r = receiveChunk(db, { generationId: 'g1', seq: 1, chunkIndex: 1, checksum: 'wrong', rows });
  assert.equal(r.ok, false);
  assert.equal(r.error, 'checksum_mismatch');
});

t('[!] 同じチャンクを再送しても壊れない (冪等)', () => {
  const rows = [mkRow('a'), mkRow('b')];
  const r = receiveChunk(db, {
    generationId: 'g1', seq: 1, chunkIndex: 0, checksum: chunkChecksum(rows), rows,
  });
  assert.equal(r.ok, true);
  assert.equal(r.total, 2);      // 増えない
});

console.log('\n公開');

t('公開するとポインタが立つ', () => {
  const r = publishGeneration(db, { generationId: 'g1', seq: 1 });
  assert.equal(r.ok, true, JSON.stringify(r));
  const p = getPublished(db);
  assert.equal(p.generation_id, 'g1');
  assert.equal(p.seq, 1);
});

t('[!] 同じ世代の再 publish は 200 (確認応答が失われた再送)', () => {
  const r = publishGeneration(db, { generationId: 'g1', seq: 1 });
  assert.equal(r.ok, true);
  assert.equal(r.idempotent, true);
});

t('[!] 公開済みの世代へ後から追記できない', () => {
  const rows = [mkRow('z')];
  const r = receiveChunk(db, { generationId: 'g1', seq: 1, chunkIndex: 9, checksum: chunkChecksum(rows), rows });
  assert.equal(r.ok, false);
  assert.equal(r.error, 'generation_already_published');
});

t('[!] seq が古い世代は公開できない (逆転公開の防止)', () => {
  const rows = [mkRow('old1')];
  receiveChunk(db, { generationId: 'gOld', seq: 0, chunkIndex: 0, checksum: chunkChecksum(rows), rows, manifest: manifestOf(1, rows) });
  const r = publishGeneration(db, { generationId: 'gOld', seq: 0 });
  assert.equal(r.ok, false);
  assert.equal(r.error, 'seq_not_newer');
  // 公開中は g1 のまま
  assert.equal(getPublished(db).generation_id, 'g1');
});

t('新しい seq なら公開でき、前世代は superseded になる', () => {
  const rows = [mkRow('n1'), mkRow('n2')];
  receiveChunk(db, { generationId: 'g2', seq: 2, chunkIndex: 0, checksum: chunkChecksum(rows), rows, manifest: manifestOf(2, rows) });
  const r = publishGeneration(db, { generationId: 'g2', seq: 2 });
  assert.equal(r.ok, true);
  assert.equal(r.supersededId, 'g1');
  const prev = db.prepare("SELECT remote_status FROM expected_profit_generation WHERE generation_id='g1'").get();
  assert.equal(prev.remote_status, 'superseded');
});

t('[!] 件数が manifest と合わなければ公開しない', () => {
  const rows = [mkRow('m1')];
  receiveChunk(db, { generationId: 'g3', seq: 3, chunkIndex: 0, checksum: chunkChecksum(rows), rows, manifest: manifestOf(1, rows) });
  const r = publishGeneration(db, { generationId: 'g3', seq: 3, manifest: { row_count: 999 } });
  assert.equal(r.ok, false);
  assert.equal(r.error, 'manifest_row_count_mismatch');
  assert.equal(getPublished(db).generation_id, 'g2');   // 公開中は変わらない
});

t('[!] 内容が manifest と違う世代は公開しない (件数が同じでも)', () => {
  const rows = [mkRow('h1'), mkRow('h2')];
  receiveChunk(db, { generationId: 'gHash', seq: 4, chunkIndex: 0, checksum: chunkChecksum(rows), rows, manifest: manifestOf(2, rows) });
  // 件数はそのままに、利益だけ書き換える → ハッシュが変わる
  db.prepare("UPDATE mart_listing_expected_profit SET expected_profit = 99999 WHERE generation_id = 'gHash'").run();
  const r = publishGeneration(db, { generationId: 'gHash', seq: 4 });
  assert.equal(r.ok, false);
  assert.equal(r.error, 'content_hash_mismatch');
});

t('[!] 利益率だけ変わってもハッシュで検出する (順位が変わる改変)', () => {
  const rows = [mkRow('m1'), mkRow('m2')];
  receiveChunk(db, { generationId: 'gRate', seq: 5, chunkIndex: 0, checksum: chunkChecksum(rows), rows, manifest: manifestOf(2, rows) });
  db.prepare("UPDATE mart_listing_expected_profit SET expected_margin_rate = 0.99 WHERE generation_id = 'gRate'").run();
  const r = publishGeneration(db, { generationId: 'gRate', seq: 5 });
  assert.equal(r.ok, false);
  assert.equal(r.error, 'content_hash_mismatch');
});

t('[!] expense_scope_version を書き換えたらハッシュで検出する (FBAが自社ランキングに混入する経路)', () => {
  const rows = [mkRow('sc1'), mkRow('sc2')];
  receiveChunk(db, { generationId: 'gScope', seq: 7, chunkIndex: 0, checksum: chunkChecksum(rows), rows, manifest: manifestOf(2, rows) });
  db.prepare("UPDATE mart_listing_expected_profit SET expense_scope_version = 'fba_v1' WHERE generation_id = 'gScope'").run();
  const r = publishGeneration(db, { generationId: 'gScope', seq: 7 });
  assert.equal(r.ok, false);
  assert.equal(r.error, 'content_hash_mismatch');
});

t('[!] 失効期限を書き換えてもハッシュで検出する (表示時の判定が変わる)', () => {
  const rows = [mkRow('ex1')];
  receiveChunk(db, { generationId: 'gExp', seq: 8, chunkIndex: 0, checksum: chunkChecksum(rows), rows, manifest: manifestOf(1, rows) });
  db.prepare("UPDATE mart_listing_expected_profit SET price_valid_until = '2099-12-31T00:00:00Z' WHERE generation_id = 'gExp'").run();
  const r = publishGeneration(db, { generationId: 'gExp', seq: 8 });
  assert.equal(r.ok, false);
  assert.equal(r.error, 'content_hash_mismatch');
});

t('[!] ハッシュを持たない世代は公開しない (照合を飛ばさせない)', () => {
  const rows = [mkRow('nh1')];
  // manifest に content_hash を入れずに受信させる
  receiveChunk(db, { generationId: 'gNoHash', seq: 10, chunkIndex: 0, checksum: chunkChecksum(rows), rows,
    manifest: { row_count: 1, built_at: '2026-09-07T00:00:00Z' } });
  const r = publishGeneration(db, { generationId: 'gNoHash', seq: 10 });
  assert.equal(r.ok, false);
  assert.equal(r.error, 'content_hash_missing');
});

t('[!] 要求に空の content_hash を送っても照合を飛ばせない', () => {
  const rows = [mkRow('sk1')];
  receiveChunk(db, { generationId: 'gSkip', seq: 11, chunkIndex: 0, checksum: chunkChecksum(rows), rows, manifest: manifestOf(1, rows) });
  db.prepare("UPDATE mart_listing_expected_profit SET expected_profit = 12345 WHERE generation_id = 'gSkip'").run();
  const r = publishGeneration(db, { generationId: 'gSkip', seq: 11, manifest: { content_hash: '' } });
  assert.equal(r.ok, false);
  // 要求のハッシュが保存済みと違う時点で弾く (照合そのものは飛ばない)
  assert.ok(['manifest_hash_mismatch', 'content_hash_mismatch'].includes(r.error), r.error);
});

t('[!] 要求 seq が保存済みと違えば拒否する (要求を信用しない)', () => {
  const rows = [mkRow('s1')];
  receiveChunk(db, { generationId: 'gSeq', seq: 6, chunkIndex: 0, checksum: chunkChecksum(rows), rows, manifest: manifestOf(1, rows) });
  const r = publishGeneration(db, { generationId: 'gSeq', seq: 9999 });
  assert.equal(r.ok, false);
  assert.equal(r.error, 'seq_mismatch');
});

t('[!] 行が0件の世代は公開しない', () => {
  db.prepare(`INSERT INTO expected_profit_generation
    (generation_id, seq, built_at, local_status, remote_status, row_count)
    VALUES ('gEmpty', 9, '2026-09-07T00:00:00Z', 'validated', 'sending', 0)`).run();
  const r = publishGeneration(db, { generationId: 'gEmpty', seq: 9 });
  assert.equal(r.ok, false);
  assert.equal(r.error, 'no_rows');
});

console.log('\n送信側 (読み戻して確認するまで成功にしない)');

const seedLocal = (id, seq, n, status = 'validated') => {
  db.prepare(`INSERT OR REPLACE INTO expected_profit_generation
    (generation_id, seq, built_at, local_status, remote_status, row_count, ok_count, incomplete_count,
     rank_eligible_count, content_hash, malls_included, malls_degraded)
    VALUES (?, ?, '2026-09-07T00:00:00Z', ?, 'not_sent', ?, ?, 0, ?, 'h', '["rakuten"]', '[]')`)
    .run(id, seq, status, n, n, n);
  const ins = db.prepare(`INSERT OR REPLACE INTO mart_listing_expected_profit
    (generation_id, mall, shop_id, mall_item_key, listing_enum_status, price_status, fee_status,
     cost_status, shipping_master_status, scenario_fit, calculation_status, rank_eligible,
     expense_scope_version, input_snapshot, formula_version, scenario_version, fee_rate_version,
     code_version, built_at, expected_profit)
    VALUES (?, 'rakuten', '1', ?, 'ok', 'ok', 'not_applicable', 'ok', 'ok', 'ok', 'ok', 1,
     'self_v1', '{}', 'v1', 'v1', 'v1', 'test', '2026-09-07T00:00:00Z', 81)`);
  for (let i = 0; i < n; i++) ins.run(id, `k${i}`);
};

await ta('チャンクに分けて送り、読み戻して確認できたら published', async () => {
  seedLocal('gp1', 100, 3);
  const calls = { chunks: 0, publish: 0 };
  const r = await publishToRender(db, 'gp1', {
    chunkSize: 2,
    postChunk: async () => { calls.chunks++; return { ok: true }; },
    postPublish: async () => { calls.publish++; return { ok: true }; },
    getPublished: async () => ({ generation_id: 'gp1', seq: 100 }),
  });
  assert.equal(r.ok, true);
  assert.equal(r.confirmed, true);
  assert.equal(calls.chunks, 2);     // 3行 ÷ 2 = 2チャンク
  const gen = db.prepare("SELECT remote_status FROM expected_profit_generation WHERE generation_id='gp1'").get();
  assert.equal(gen.remote_status, 'published');
});

await ta('[!] 読み戻しで別の世代が返ったら成功にしない', async () => {
  seedLocal('gp2', 101, 1);
  const r = await publishToRender(db, 'gp2', {
    postChunk: async () => ({ ok: true }),
    postPublish: async () => ({ ok: true }),
    getPublished: async () => ({ generation_id: '別の世代', seq: 999 }),
  });
  assert.equal(r.ok, false);
  assert.equal(r.error, 'publish_not_confirmed');
  const gen = db.prepare("SELECT remote_status FROM expected_profit_generation WHERE generation_id='gp2'").get();
  assert.equal(gen.remote_status, 'received');   // published にしない
});

await ta('[!] 転送が途中で失敗したら公開しない', async () => {
  seedLocal('gp3', 102, 4);
  let n = 0;
  const r = await publishToRender(db, 'gp3', {
    chunkSize: 1,
    postChunk: async () => { n++; return n === 2 ? { ok: false, error: 'boom' } : { ok: true }; },
    postPublish: async () => { throw new Error('publish を呼んではいけない'); },
    getPublished: async () => null,
  });
  assert.equal(r.ok, false);
  assert.equal(r.error, 'chunk_failed');
});

await ta('[!] 検証を通っていない世代は送らない', async () => {
  seedLocal('gp4', 103, 1, 'rejected');
  const r = await publishToRender(db, 'gp4', {
    postChunk: async () => { throw new Error('送ってはいけない'); },
    postPublish: async () => { throw new Error('送ってはいけない'); },
    getPublished: async () => null,
  });
  assert.equal(r.ok, false);
  assert.equal(r.error, 'local_status_rejected');
});

t('チャンクの分割は決定的 (同じ入力なら同じ checksum)', () => {
  const rows = [mkRow('a'), mkRow('b'), mkRow('c')];
  const a = makeChunks(rows, 2);
  const b = makeChunks(rows, 2);
  assert.equal(a.length, 2);
  assert.equal(a[0].checksum, b[0].checksum);
  assert.notEqual(a[0].checksum, a[1].checksum);
});

t('manifest に件数と内容ハッシュが入る', () => {
  const gen = db.prepare("SELECT * FROM expected_profit_generation WHERE generation_id='gp1'").get();
  const m = makeManifest(gen);
  assert.equal(m.row_count, 3);
  assert.deepEqual(m.malls_included, ['rakuten']);
});

console.log('\n古い世代の掃除');

t('[!] 公開中の世代は消さない', () => {
  const before = db.prepare('SELECT COUNT(*) n FROM expected_profit_generation').get().n;
  pruneGenerations(db, 1);
  const p = getPublished(db);
  const still = db.prepare('SELECT COUNT(*) n FROM expected_profit_generation WHERE generation_id = ?').get(p.generation_id).n;
  assert.equal(still, 1);
  const after = db.prepare('SELECT COUNT(*) n FROM expected_profit_generation').get().n;
  assert.ok(after < before);
});

db.close();
fs.rmSync(process.env.DATA_DIR, { recursive: true, force: true });
console.log(`\n${passed} 件 PASS`);
