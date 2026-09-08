/**
 * test-publish.mjs — 世代の転送と公開 受入試験 (§10.1 障害 / §15-7)
 *
 * HTTP は使わず、受け口の関数を直接呼ぶ (送信側と受信側を同じ経路で通す)。
 * 実行: node apps/expected-profit/test-publish.mjs
 */
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';
import os from 'os';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'ep-pub-'));

const { httpDeps } = await import('./publish.js');
const { initExpectedProfitDB, getExpectedProfitDB, addColumnIfMissing, MIGRATED_COLUMNS,
  createExpectedProfitSchema } = await import('./db.js');
const { receiveChunk, publishGeneration, getPublished, chunkChecksum, pruneGenerations, generationContentHash } = await import('./publish-api.js');
const { makeChunks, makeManifest, publishToRender } = await import('./publish.js');
const { hashRows } = await import('./generation-hash.js');

const cols = (d) => d.prepare('PRAGMA table_info(mart_listing_expected_profit)').all().map(c => c.name);

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
  ne_code_source: 'sku_map',
  cost_ex_tax: 600, cost_method: 'single', unit_quantity: 1, shipping_code: '501', shipping_method: 'ネコポス',
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

await ta('[!] 最後のチャンクの応答中に期限を跨いだら、公開を始めない (Codex R6-2)', async () => {
  // 送信開始時点では期限内。最後のチャンクの応答が返る頃に期限を越える
  seedLocal('gLate', 200, 2);
  const deadline = new Date(Date.now() + 60);
  let published = false;
  const r = await publishToRender(db, 'gLate', {
    chunkSize: 1,
    deadline,
    postChunk: async () => { await new Promise(res => setTimeout(res, 40)); return { ok: true }; },
    postPublish: async () => { published = true; return { ok: true }; },
    getPublished: async () => null,
  });
  assert.equal(published, false, '期限を過ぎてから公開を始めた');
  assert.equal(r.ok, false);
  assert.equal(r.error, 'deadline_exceeded');
  assert.equal(r.phase, 'before_publish');
});

await ta('期限内に終われば普通に公開する (期限判定が過剰でないこと)', async () => {
  seedLocal('gInTime', 201, 1);
  const r = await publishToRender(db, 'gInTime', {
    chunkSize: 1,
    deadline: new Date(Date.now() + 60_000),
    postChunk: async () => ({ ok: true }),
    postPublish: async () => ({ ok: true }),
    getPublished: async () => ({ generation_id: 'gInTime', seq: 201 }),
  });
  assert.equal(r.ok, true, r.error);
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


console.log('');
console.log('既にあるDBへの列追加 (miniPC と Render の両方に既存DBがある)');

t('[!] 列が無い古いDBでも初期化を通せば足される', () => {
  const p = path.join(process.env.DATA_DIR, 'old.db');
  const old = new Database(p);
  // 列が1つ足りない状態を作る (unit_quantity だけ無い)
  old.exec(`CREATE TABLE mart_listing_expected_profit (
    generation_id TEXT NOT NULL, mall TEXT NOT NULL, shop_id TEXT NOT NULL,
    mall_item_key TEXT NOT NULL, cost_method TEXT,
    PRIMARY KEY (generation_id, mall, shop_id, mall_item_key))`);
  assert.equal(cols(old).includes('unit_quantity'), false, '前提: まだ列が無い');
  assert.equal(addColumnIfMissing(old, 'mart_listing_expected_profit', 'unit_quantity', 'INTEGER'), true);
  assert.ok(cols(old).includes('unit_quantity'));
  old.close();
});

t('2回流しても壊れない (冪等)', () => {
  const p = path.join(process.env.DATA_DIR, 'old2.db');
  const old = new Database(p);
  old.exec('CREATE TABLE mart_listing_expected_profit (generation_id TEXT PRIMARY KEY)');
  assert.equal(addColumnIfMissing(old, 'mart_listing_expected_profit', 'unit_quantity', 'INTEGER'), true);
  assert.equal(addColumnIfMissing(old, 'mart_listing_expected_profit', 'unit_quantity', 'INTEGER'), false);
  assert.equal(cols(old).filter(c => c === 'unit_quantity').length, 1);
  old.close();
});

t('[!] 列追加に失敗したら例外を投げる (握り潰さない)', () => {
  const p = path.join(process.env.DATA_DIR, 'old3.db');
  const old = new Database(p);
  assert.throws(() => addColumnIfMissing(old, 'no_such_table', 'x', 'INTEGER'));
  old.close();
});

t('新規DBは初期化の時点で列を持っている', () => {
  assert.ok(cols(db).includes('unit_quantity'));
});

t('[!] 本番の初期化 (initExpectedProfitDB) が既存DBに列を足す', () => {
  // 🚨 addColumnIfMissing を直接呼ぶだけのテストでは「migrate を呼び忘れている」を
  //    検出できない。既存DBから列を落として、本番の入口を通し直して確かめる。
  // 🚨 列名を決め打ちせず MIGRATED_COLUMNS を全部見る。決め打ちだと、
  //    新しい列を足したときに「migrate への追加忘れ」を素通りさせる
  // 🚨 テーブルも決め打ちしない。別テーブルへの列追加を足したときに素通りする
  assert.ok(MIGRATED_COLUMNS.length > 0);
  const colsOf = (d, table) => d.prepare(`PRAGMA table_info(${table})`).all().map(c => c.name);
  for (const [table, column] of MIGRATED_COLUMNS) {
    db.exec(`ALTER TABLE ${table} DROP COLUMN ${column}`);
    assert.equal(colsOf(db, table).includes(column), false, `前提: ${table}.${column} を落とせている`);
  }
  const reopened = initExpectedProfitDB();     // 本番と同じ入口
  for (const [table, column] of MIGRATED_COLUMNS) {
    assert.ok(colsOf(reopened, table).includes(column), `初期化を通したのに ${table}.${column} が足されていない`);
  }
});

t('[!] 既存DBに列が足りないまま publish すると必ず落ちる (静かに欠けない)', () => {
  // 🚨 MIGRATED_COLUMNS を巡回するだけの試験は「一覧への登録漏れ」を検出できない (Codex R12)。
  //    本当に守りたいのは「受け取る側の列が足りないまま公開されないこと」なので、
  //    列を落としたDBに本番の INSERT を通して、**落ちる**ことを確かめる
  const p = path.join(process.env.DATA_DIR, 'missingcol.db');
  const old = new Database(p);
  createExpectedProfitSchema(old);
  old.exec('ALTER TABLE mart_listing_expected_profit DROP COLUMN ne_code_source');
  assert.throws(
    () => old.prepare(`INSERT INTO mart_listing_expected_profit
      (generation_id, mall, shop_id, mall_item_key, ne_code_source) VALUES ('g','amazon','s','k','sku_map')`).run(),
    /ne_code_source/,
    '列が無いのに INSERT が通ってしまう = 静かに欠ける');
  old.close();
});

t('[!] MIGRATED_COLUMNS に書いた列は実在する (古い記述が残らない)', () => {
  // 新規DBの列と、移行で足せる列を突き合わせる
  const live = getExpectedProfitDB();   // 直前の試験で開き直しているので、いまのハンドルを使う
  const colsOf = (table) => live.prepare(`PRAGMA table_info(${table})`).all().map(c => c.name);
  for (const [table, column] of [
    ['mart_listing_expected_profit', 'unit_quantity'],
    ['mart_listing_expected_profit', 'ne_code_source'],
    ['mall_price_snapshot', 'shipping_group'],
  ]) {
    assert.ok(colsOf(table).includes(column), `新規DBに ${table}.${column} が無い`);
    assert.ok(MIGRATED_COLUMNS.some(([t, c]) => t === table && c === column),
      `${table}.${column} が MIGRATED_COLUMNS に無い = 既存DBには足されない`);
  }
});

console.log('');
console.log('Render の URL は既存の env を使う (2026-09-08 初回公開が env 名の食い違いで止まった)');

const ENV_KEYS = ['RENDER_PORTAL_URL', 'RENDER_MIRROR_URL', 'MIRROR_SYNC_KEY'];
const restoreEnv = (saved) => {
  for (let i = 0; i < ENV_KEYS.length; i++) {
    if (saved[i] === undefined) delete process.env[ENV_KEYS[i]];
    else process.env[ENV_KEYS[i]] = saved[i];
  }
};

const withEnv = (vals, fn) => {
  const keys = ['RENDER_PORTAL_URL', 'RENDER_MIRROR_URL', 'MIRROR_SYNC_KEY'];
  const saved = keys.map(k => process.env[k]);
  try {
    for (const k of keys) delete process.env[k];
    for (const [k, v] of Object.entries(vals)) process.env[k] = v;
    fn();
  } finally {
    for (let i = 0; i < keys.length; i++) {
      if (saved[i] === undefined) delete process.env[keys[i]];
      else process.env[keys[i]] = saved[i];
    }
  }
};

// 🚨 「例外が出ない」だけでは、どちらの env を使ったか分からない (Codex R14)。
//    fetch を差し替えて **実際に叩く URL** を捕まえる
const urlUsed = async () => {
  const real = globalThis.fetch;
  let seen = null;
  globalThis.fetch = async (u) => { seen = String(u); return { json: async () => ({ ok: true }) }; };
  try { await httpDeps().postPublish({}); } finally { globalThis.fetch = real; }
  return seen;
};

await ta('[!] RENDER_MIRROR_URL だけでも転送先を組み立てられる', async () => {
  // 🚨 同じ Render を指すのに新しい env を増やしたせいで、初回の公開が黙って止まった
  let url = null;
  withEnv({ RENDER_MIRROR_URL: 'https://example.test/', MIRROR_SYNC_KEY: 'k' }, () => {});
  const saved = [process.env.RENDER_PORTAL_URL, process.env.RENDER_MIRROR_URL, process.env.MIRROR_SYNC_KEY];
  try {
    delete process.env.RENDER_PORTAL_URL;
    process.env.RENDER_MIRROR_URL = 'https://example.test/';
    process.env.MIRROR_SYNC_KEY = 'k';
    url = await urlUsed();
  } finally { restoreEnv(saved); }
  assert.equal(url, 'https://example.test/apps/expected-profit/sync/publish',
    '既存 env だけで組み立てられない = 転送が止まる');
});

await ta('[!] RENDER_PORTAL_URL があればそちらを叩く (移行用・優先順位そのものを見る)', async () => {
  const saved = [process.env.RENDER_PORTAL_URL, process.env.RENDER_MIRROR_URL, process.env.MIRROR_SYNC_KEY];
  let url = null;
  try {
    process.env.RENDER_PORTAL_URL = 'https://new.test';
    process.env.RENDER_MIRROR_URL = 'https://old.test';
    process.env.MIRROR_SYNC_KEY = 'k';
    url = await urlUsed();
  } finally { restoreEnv(saved); }
  assert.ok(url.startsWith('https://new.test/'), `優先順位が逆になっている: ${url}`);
});

t('[!] どちらも無ければ、設定すべき env 名を挙げて止まる', () => {
  withEnv({ MIRROR_SYNC_KEY: 'k' }, () => {
    assert.throws(() => httpDeps(), /RENDER_MIRROR_URL/, '古い名前で怒られると何を設定すべきか分からない');
  });
});

// 🚨 再オープンしていることがあるので、いま開いているハンドルを閉じる (Windows は開いたままだと消せない)
try { getExpectedProfitDB().close(); } catch { /* 既に閉じている */ }
try { db.close(); } catch { /* 再オープン済み */ }
fs.rmSync(process.env.DATA_DIR, { recursive: true, force: true });
console.log(`\n${passed} 件 PASS`);
