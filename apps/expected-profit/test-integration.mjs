/**
 * test-integration.mjs — 層と層のつなぎ目を通す接続テスト
 *
 * 🚨 Codex R4 の指摘:
 *    「手作り Map・模擬成功レスポンスを使わない接続テストの結果が必要」
 *    単体のガードをいくら逆検証しても、層のつなぎ目 (キーの作り方、値の形) は検出できない。
 *    実際 feeCacheKey の区切り文字が無く、本番では見積を1件も引けない状態だった。
 *
 * ここでは実際の関数を順に通す:
 *   出品レポート → snapshot(DB) → 手数料見積(DB) → 世代生成 → 検証 → 転送 → 公開 → 画面用の読み取り
 *
 * 実行: node apps/expected-profit/test-integration.mjs
 */
import assert from 'node:assert/strict';
import fs from 'fs';
import path from 'path';
import os from 'os';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'ep-it-'));
process.env.SP_API_MARKETPLACE_ID = 'MKT1';
process.env.SP_API_SELLER_ID = 'SELLER1';

const { initExpectedProfitDB, createExpectedProfitSchema } = await import('./db.js');
const { fetchAmazonListings, fetchRakutenListings } = await import('./fetch-listings.js');
const { refreshFees } = await import('./refresh-fees.js');
const { buildGeneration, validateGeneration } = await import('./build-generation.js');
const { publishToRender } = await import('./publish.js');
const { receiveChunk, publishGeneration, getPublished } = await import('./publish-api.js');
const { queryPublished } = await import('./query.js');
const { feeTargetsFrom } = await import('./nightly.js');

let passed = 0;
async function ta(name, fn) {
  try { await fn(); passed++; console.log(`  ok  ${name}`); }
  catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; }
}
const near = (a, b, eps = 1e-6) => Math.abs(a - b) < eps;

const db = initExpectedProfitDB();

// 🚨 受信側 (Render 相当) は **別の DB ファイル**にする (Codex R5)。
//    同じ DB を使うと、受信時の世代作成を通らず、送信前から全行が存在してしまう。
//    「公開済み世代への同一チャンク再送」の分岐も検証できない
const Database = (await import('better-sqlite3')).default;
const remoteDb = new Database(path.join(process.env.DATA_DIR, 'remote.db'));
remoteDb.pragma('foreign_keys = ON');
// 受信側にも同じスキーマを作る。
// 🚨 db.js を正規表現で読んで CREATE 文を拾う作りだと、ALTER 文が1つ増えただけで壊れ、
//    列追加 (migrate) も通らない。本番と同じ関数を呼ぶ
createExpectedProfitSchema(remoteDb);

const NOW = new Date('2026-09-07T15:00:00Z');
const FUTURE = '2099-01-01T00:00:00Z';

// warehouse.db の代わり (読み取りだけなので最小の形で足りる)
const warehouseInputs = {
  products: new Map([
    ['ne-fba', { 商品コード: 'ne-fba', 商品名: 'FBA商品', 原価: 600, 原価ソース: 'NE', 原価状態: 'COMPLETE', 消費税率: 0.1, 送料コード: null, 配送方法: null, 売上分類: 3 }],
    ['ne-rak', { 商品コード: 'ne-rak', 商品名: '楽天商品', 原価: 600, 原価ソース: 'NE', 原価状態: 'COMPLETE', 消費税率: 0.1, 送料コード: '501', 配送方法: 'ネコポス', 売上分類: 3 }],
  ]),
  shippingRates: new Map([['501', { 送料: 198, 出荷作業料: 20, 想定梱包資材費: 10, 想定人件費: 9 }]]),
  skuMaps: {
    amazon: new Map([['sku1', [{ ne_code: 'ne-fba' }]]]),
    rakuten: new Map([['rsku1', [{ ne_code: 'ne-rak' }]]]),
  },
  masterFreshness: { costValidUntil: FUTURE, shippingMasterValidUntil: FUTURE },
};

console.log('取得 → 見積 → 世代 → 公開 → 画面 を通す');

await ta('1. 出品レポートから snapshot が DB に入る', async () => {
  const r = await fetchAmazonListings(db, {
    getActiveListingsReport: async () => ({
      listings: [{
        '出品者SKU': 'sku1', '商品ID': 'B001', '価格': '1980',
        'フルフィルメント・チャンネル': 'AMAZON_JP', 'ステータス': 'Active', 'ポイント': '0',
      }],
    }),
  });
  assert.equal(r.status, 'ok');
  const row = db.prepare("SELECT * FROM mall_price_snapshot WHERE mall='amazon'").get();
  assert.equal(row.mall_item_ref, 'B001');       // ASIN が届く
  assert.equal(row.fulfillment, 'FBA');
  assert.equal(row.postage_included, 1);         // FBA は送料込みで確定 (§R4-3)
  assert.equal(row.points, 0);
});

await ta('2. snapshot から見積対象を作り、見積を DB に保存する', async () => {
  const snapshots = db.prepare("SELECT * FROM mall_price_snapshot WHERE mall='amazon'").all();
  const targets = feeTargetsFrom(snapshots, { sellerId: 'SELLER1', marketplaceId: 'MKT1' });
  assert.equal(targets.length, 1);
  const r = await refreshFees(db, targets, {
    sleepMs: 0,
    callFeesApi: async (body) => ([{
      Status: 'Success',
      FeesEstimateIdentifier: { SellerInputIdentifier: body[0].FeesEstimateRequest.Identifier },
      FeesEstimate: {
        TotalFeesEstimate: { Amount: 628 },
        FeeDetailList: [
          { FeeType: 'ReferralFee', FinalFee: { Amount: 166 } },
          { FeeType: 'FBAFees', FinalFee: { Amount: 462 } },
        ],
      },
    }]),
  });
  assert.equal(r.refreshed, 1, JSON.stringify(r));
  const saved = db.prepare('SELECT * FROM amazon_fee_estimate').get();
  assert.equal(saved.fee_status, 'ok');
  assert.equal(saved.referral_fee_ex_tax, 166);
});

await ta('🚨 3. 世代生成が DB の見積を実際に引けている (キーのズレを検出する)', async () => {
  // ここが Codex R4-2 の本丸。手作り Map ではなく、保存された見積を読む
  await fetchRakutenListings(db, {
    searchPage: async () => ({
      results: [{ item: { manageNumber: 'item1', variants: {
        rsku1: { standardPrice: '1100', merchantDefinedSkuId: 'rsku1',
          payment: { taxIncluded: true, taxRate: '0.1' }, shipping: { postageIncluded: true } },
      } } }],
      nextCursorMark: null,
    }),
  });
  const gen = buildGeneration(db, {
    warehouseInputs, now: NOW, sellerId: 'SELLER1', marketplaceId: 'MKT1',
    malls: ['amazon', 'rakuten'], codeVersion: 'it',
  });
  const amazonRow = db.prepare(
    "SELECT * FROM mart_listing_expected_profit WHERE generation_id = ? AND mall='amazon'").get(gen.generationId);
  assert.equal(amazonRow.fee_status, 'ok', `見積を引けていない: ${amazonRow.fee_status} / ${amazonRow.incomplete_reason}`);
  assert.equal(amazonRow.calculation_status, 'ok', amazonRow.incomplete_reason);
  // 税抜1800 − 原価600 − FBA420(462÷1.1) − 手数料166 = 614
  assert.ok(near(amazonRow.expected_profit, 614), `期待614, 実際 ${amazonRow.expected_profit}`);
  assert.equal(amazonRow.rank_eligible, 1, `FBA がランキングから外れている: ${amazonRow.rank_exclusion_reason}`);
});

await ta('🚨 4. 楽天も同じ世代で計算されている', async () => {
  const gen = db.prepare('SELECT * FROM expected_profit_generation ORDER BY seq DESC LIMIT 1').get();
  const row = db.prepare(
    "SELECT * FROM mart_listing_expected_profit WHERE generation_id = ? AND mall='rakuten'").get(gen.generation_id);
  assert.equal(row.calculation_status, 'ok', row.incomplete_reason);
  // 税抜1000 − 原価600 − 配送219 − 手数料100 = 81
  assert.ok(near(row.expected_profit, 81), `期待81, 実際 ${row.expected_profit}`);
  assert.equal(row.expense_scope_version, 'self_v1');
});

await ta('5. 検証を通る', async () => {
  const gen = db.prepare('SELECT * FROM expected_profit_generation ORDER BY seq DESC LIMIT 1').get();
  const v = validateGeneration(db, gen.generation_id);
  assert.equal(v.ok, true, JSON.stringify(v.errors));
});

// 送信側 (db) → 受信側 (remoteDb) を実際の関数で結ぶ
const remoteDeps = (over = {}) => ({
  chunkSize: 1,
  postChunk: async (id, body) => receiveChunk(remoteDb, {
    generationId: id, seq: body.seq, chunkIndex: body.chunk_index,
    checksum: body.checksum, rows: body.rows, manifest: body.manifest,
  }),
  postPublish: async (body) => publishGeneration(remoteDb, {
    generationId: body.generation_id, seq: body.seq, manifest: body.manifest,
  }),
  getPublished: async () => getPublished(remoteDb),
  ...over,
});

await ta('🚨 6. 送信 → 受信 → 公開 を別DBで通す (受信側の世代作成も経由する)', async () => {
  const gen = db.prepare('SELECT * FROM expected_profit_generation ORDER BY seq DESC LIMIT 1').get();
  assert.equal(remoteDb.prepare('SELECT COUNT(*) n FROM mart_listing_expected_profit').get().n, 0);
  const r = await publishToRender(db, gen.generation_id, remoteDeps());
  assert.equal(r.ok, true, `公開できていない: ${r.error} ${JSON.stringify(r.detail || {})}`);
  assert.equal(r.confirmed, true);
  assert.equal(remoteDb.prepare('SELECT COUNT(*) n FROM mart_listing_expected_profit').get().n, gen.row_count);
});

await ta('🚨 7. 内容ハッシュが送信側と受信側で一致する (別DBで計算しても同じ)', async () => {
  const gen = db.prepare('SELECT * FROM expected_profit_generation ORDER BY seq DESC LIMIT 1').get();
  assert.equal(gen.remote_status, 'published');
  const { generationContentHash } = await import('./publish-api.js');
  assert.equal(generationContentHash(remoteDb, gen.generation_id), gen.content_hash);
});

await ta('🚨 7b. 大文字小文字が混ざった SKU でもハッシュが一致する', async () => {
  // JS の localeCompare は a → B、SQLite の BINARY は B → a で順序が違う。
  // 揃っていないと、そういう SKU がある夜は正常な世代が公開できない (Codex R5-1)
  const { hashRows, hashGeneration } = await import('./generation-hash.js');
  const { chunkChecksum } = await import('./publish-api.js');
  const src = db.prepare('SELECT * FROM mart_listing_expected_profit LIMIT 1').get();
  const mixed = [
    { ...src, generation_id: 'gCase', mall_item_key: 'rakuten1a' },
    { ...src, generation_id: 'gCase', mall_item_key: 'rakuten1B' },
  ];
  receiveChunk(remoteDb, {
    generationId: 'gCase', seq: 9000, chunkIndex: 0,
    checksum: chunkChecksum(mixed), rows: mixed,
    manifest: { row_count: 2, content_hash: hashRows(mixed), built_at: '2026-09-07T00:00:00Z' },
  });
  assert.equal(hashGeneration(remoteDb, 'gCase'), hashRows(mixed), 'JS と DB で並び順が食い違っている');

  // 公開まで通ることも確認する (照合で弾かれないこと)
  const before = remoteDb.prepare('SELECT * FROM expected_profit_publish_pointer WHERE id = 1').get();
  const r = publishGeneration(remoteDb, { generationId: 'gCase', seq: 9000 });
  assert.equal(r.ok, true, `大小文字混在で公開できない: ${r.error}`);
  // 後続のテストのためにポインタを元へ戻す (この世代は検証用なので残さない)
  remoteDb.prepare('UPDATE expected_profit_publish_pointer SET generation_id = ?, seq = ? WHERE id = 1')
    .run(before.generation_id, before.seq);
  remoteDb.prepare("UPDATE expected_profit_generation SET remote_status = 'published' WHERE generation_id = ?")
    .run(before.generation_id);
  remoteDb.prepare("DELETE FROM mart_listing_expected_profit WHERE generation_id = 'gCase'").run();
  remoteDb.prepare("DELETE FROM expected_profit_generation WHERE generation_id = 'gCase'").run();
});

await ta('🚨 8. 画面用の読み取りが、公開世代から行を返せる', async () => {
  // 画面は受信側 (Render) の DB を見る
  const r = queryPublished({ db: remoteDb, now: NOW });
  assert.ok(r.published, '公開世代が見えない');
  // malls_degraded は配列で返る (文字列だと画面の .map が落ちる — Codex R4-4)
  assert.ok(Array.isArray(r.published.malls_degraded), 'malls_degraded が配列でない');
  assert.ok(Array.isArray(r.published.malls_included));
  // 既定 scope は self_v1 なので楽天だけが返る (FBA と混ぜない — Codex R4-10)
  assert.equal(r.rows.length, 1);
  assert.equal(r.rows[0].mall, 'rakuten');
});

await ta('🚨 9. scope を fba_v1 にすると FBA の行が返る', async () => {
  const r = queryPublished({ db: remoteDb, now: NOW, expenseScope: 'fba_v1' });
  assert.equal(r.rows.length, 1);
  assert.equal(r.rows[0].mall, 'amazon');
  assert.ok(near(r.rows[0].expected_profit, 614));
});

await ta('scope を all にすると混ぜて返る (明示したときだけ)', async () => {
  const r = queryPublished({ db: remoteDb, now: NOW, expenseScope: 'all' });
  assert.equal(r.rows.length, 2);
});

await ta('🚨 10. 公開成功後に応答を失っても、同じ世代を再送できる (冪等)', async () => {
  const gen = db.prepare('SELECT * FROM expected_profit_generation ORDER BY seq DESC LIMIT 1').get();
  // 公開済みの世代へ、同じチャンクを送り直す → 409 ではなく 200 で受ける
  // 受信側は published のまま。送信側の状態だけ戻して最初からやり直す
  db.prepare("UPDATE expected_profit_generation SET remote_status = 'not_sent' WHERE generation_id = ?")
    .run(gen.generation_id);
  assert.equal(getPublished(remoteDb).generation_id, gen.generation_id);
  const r = await publishToRender(db, gen.generation_id, remoteDeps());
  assert.equal(r.ok, true, `再送で失敗: ${r.error} ${JSON.stringify(r.detail || {})}`);
  assert.equal(r.confirmed, true);
});

await ta('🚨 11. 同じ index を別内容で送ると拒否する (混ざった世代を作らせない)', async () => {
  const { chunkChecksum } = await import('./publish-api.js');
  // 実際に保存されている行をコピーして使う (ダミーだと列が足りずに insert で落ちる)
  const src = db.prepare('SELECT * FROM mart_listing_expected_profit LIMIT 1').get();
  const rows = [{ ...src, generation_id: 'gConf', mall_item_key: 'conf-a' }];
  const rows2 = [{ ...src, generation_id: 'gConf', mall_item_key: 'conf-b' }];

  const first = receiveChunk(remoteDb, { generationId: 'gConf', seq: 999, chunkIndex: 0, checksum: chunkChecksum(rows), rows });
  assert.equal(first.ok, true, JSON.stringify(first));

  // 同じ index に別内容を送る
  const r = receiveChunk(remoteDb, { generationId: 'gConf', seq: 999, chunkIndex: 0, checksum: chunkChecksum(rows2), rows: rows2 });
  assert.equal(r.ok, false);
  assert.equal(r.error, 'chunk_conflict');

  // 同じ内容の再送は通る (冪等)
  const again = receiveChunk(remoteDb, { generationId: 'gConf', seq: 999, chunkIndex: 0, checksum: chunkChecksum(rows), rows });
  assert.equal(again.ok, true);
});

await ta('🚨 12. 公開済み世代への「別内容」チャンクは拒否する', async () => {
  const { chunkChecksum } = await import('./publish-api.js');
  const gen = remoteDb.prepare("SELECT * FROM expected_profit_generation WHERE remote_status='published' LIMIT 1").get();
  const src = remoteDb.prepare('SELECT * FROM mart_listing_expected_profit LIMIT 1').get();
  const rows = [{ ...src, generation_id: gen.generation_id, mall_item_key: 'sneak' }];
  const r = receiveChunk(remoteDb, {
    generationId: gen.generation_id, seq: gen.seq, chunkIndex: 99,
    checksum: chunkChecksum(rows), rows,
  });
  assert.equal(r.ok, false);
  assert.equal(r.error, 'generation_already_published');
});

db.close();
remoteDb.close();
fs.rmSync(process.env.DATA_DIR, { recursive: true, force: true });
console.log(`\n${passed} 件 PASS`);
