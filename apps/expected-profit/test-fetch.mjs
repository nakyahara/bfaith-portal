/**
 * test-fetch.mjs — 出品列挙・価格取得の受入試験 (§10.1-12〜16 母集団 / 障害)
 *
 * API は叩かない。deps 差し替えで純粋に検証する。
 * 実行: node apps/expected-profit/test-fetch.mjs
 */
import assert from 'node:assert/strict';
import fs from 'fs';
import path from 'path';
import os from 'os';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'ep-test-'));

const { initExpectedProfitDB, setSetting, SETTING_AMAZON_SELLER_ID, createExpectedProfitSchema } = await import('./db.js');
const { default: Database } = await import('better-sqlite3');
const { canonicalShopId } = await import('./util.js');
const {
  toIntPrice, amazonListingStatus, amazonRowToSnapshot, rakutenItemToSnapshots,
  evaluateEnumeration, loadLastCompleteKeys, fetchAmazonListings, fetchRakutenListings, amazonShopId,
  enumStatusWithParseFailures, amazonFulfillment, rakutenItemToSnapshotsDetailed,
  amazonPostageIncluded, AMAZON_POSTAGE_INCLUDED_GROUPS,
  yahooDetailToSnapshotsDetailed, yahooPostageIncluded, fetchYahooListings, YAHOO_QUERIES, snapshotKey,
  aupayItemToSnapshotsDetailed, aupayPostageIncluded, parseAupayItemsXml, parseAupayStocksXml, fetchAupayListings,
} = await import('./fetch-listings.js');
// 🚨 Yahoo の網羅集合は RYS が正本。写しではなく本物を読み込んで突き合わせる
const { CANONICAL_QUERIES: RYS_CANONICAL_QUERIES } = await import('../rakuten-yahoo-sync/lib/yahoo-store-sync.js');
const { reportWaitUntil, getActiveListingsReport } = await import('../profit-calculator/sp-api.js');

let passed = 0;
function t(name, fn) {
  try { fn(); passed++; console.log(`  ok  ${name}`); }
  catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; }
}
async function ta(name, fn) {
  try { await fn(); passed++; console.log(`  ok  ${name}`); }
  catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; }
}

const meta = { runId: 'r1', shopId: 'shop1', fetchedAt: '2026-09-07T00:00:00Z', validUntil: '2026-09-10T00:00:00Z' };

console.log('価格の読み取り');

t('🚨 整数円だけ受ける (読めない値は NULL で「読めたことにしない」)', () => {
  assert.equal(toIntPrice('1080'), 1080);
  assert.equal(toIntPrice(1080), 1080);
  assert.equal(toIntPrice('1,080'), 1080);
  assert.equal(toIntPrice('1080.5'), null);
  assert.equal(toIntPrice(1080.5), null);
  assert.equal(toIntPrice(''), null);
  assert.equal(toIntPrice(null), null);
  assert.equal(toIntPrice('価格未設定'), null);
});

console.log('\nAmazon 出品レポート');

t('Active / Inactive / Incomplete をそのまま持つ', () => {
  assert.equal(amazonListingStatus('Active'), 'active');
  assert.equal(amazonListingStatus('Inactive'), 'inactive');
  assert.equal(amazonListingStatus('Incomplete'), 'incomplete');
  assert.equal(amazonListingStatus(''), 'unknown');
});

t('FBA / FBM をチャンネル列から判定する', () => {
  const fba = amazonRowToSnapshot({ '出品者SKU': 'a', '価格': '1000', 'フルフィルメント・チャンネル': 'AMAZON_JP', 'ステータス': 'Active' }, meta);
  const fbm = amazonRowToSnapshot({ '出品者SKU': 'b', '価格': '1000', 'フルフィルメント・チャンネル': 'DEFAULT', 'ステータス': 'Active' }, meta);
  assert.equal(fba.fulfillment, 'FBA');
  assert.equal(fbm.fulfillment, 'FBM');
});

t('ポイントを手数料見積の入力として持つ', () => {
  const r = amazonRowToSnapshot({ '出品者SKU': 'a', '価格': '1000', 'ポイント': '5', 'ステータス': 'Active' }, meta);
  assert.equal(r.points, 5);
});

t('🚨 ASIN を保存する (手数料見積は ASIN 単位で引くので失うと再取得になる)', () => {
  const r = amazonRowToSnapshot({ '出品者SKU': 'a', '商品ID': 'B00TEST', '価格': '1000', 'ステータス': 'Active' }, meta);
  assert.equal(r.mall_item_ref, 'B00TEST');
});

t('🚨 価格が読めない行も残す (行を消さない)', () => {
  const r = amazonRowToSnapshot({ '出品者SKU': 'a', '価格': '', 'ステータス': 'Inactive' }, meta);
  assert.equal(r.price_incl_tax, null);
  assert.equal(r.fetch_status, 'not_found');
  assert.equal(r.mall_item_key, 'a');       // 行自体は残る
});

console.log('\n楽天 items/search');

const rakutenItem = {
  manageNumber: 'item001',
  variants: {
    'sku-a': { standardPrice: '1080', payment: { taxRate: '0.08', taxIncluded: true }, shipping: { postageIncluded: true } },
    'sku-b': { standardPrice: '2200', payment: { taxRate: '0.1' }, shipping: { postageIncluded: false, singleItemShipping: 300 } },
  },
};

t('variant ごとに1行。キーは manageNumber/variantKey', () => {
  const rows = rakutenItemToSnapshots(rakutenItem, meta);
  assert.equal(rows.length, 2);
  assert.equal(rows[0].mall_item_key, 'item001/sku-a');
});

t('🚨 standardPrice は文字列で返るので整数化する', () => {
  const rows = rakutenItemToSnapshots(rakutenItem, meta);
  assert.equal(rows[0].price_incl_tax, 1080);
  assert.strictEqual(typeof rows[0].price_incl_tax, 'number');
});

t('モール側の税率を持つ (商品マスタとの食い違い検出に使う)', () => {
  const rows = rakutenItemToSnapshots(rakutenItem, meta);
  assert.equal(rows[0].mall_tax_rate, 0.08);
  assert.equal(rows[1].mall_tax_rate, 0.1);
});

t('送料込みなら送料収入0 / 別途なら singleItemShipping', () => {
  const rows = rakutenItemToSnapshots(rakutenItem, meta);
  assert.equal(rows[0].postage_included, 1);
  assert.equal(rows[0].postage_revenue_incl_tax, 0);
  assert.equal(rows[1].postage_included, 0);
  assert.equal(rows[1].postage_revenue_incl_tax, 300);
});

t('別途送料で金額が取れなければ NULL (0で埋めない)', () => {
  const rows = rakutenItemToSnapshots({
    manageNumber: 'x', variants: { v: { standardPrice: '100', shipping: { postageIncluded: false } } },
  }, meta);
  assert.equal(rows[0].postage_revenue_incl_tax, null);
});

t('hideItem / variant.hidden は hidden として持つ', () => {
  const rows = rakutenItemToSnapshots({ manageNumber: 'x', hideItem: true, variants: { v: { standardPrice: '100' } } }, meta);
  assert.equal(rows[0].listing_status, 'hidden');
});

console.log('\n§10.1-14/16 列挙の欠落検知');

t('初回 (前回集合なし) は ok', () => {
  const r = evaluateEnumeration(new Set(), new Set(['a', 'b']));
  assert.equal(r.status, 'ok');
});

t('少し消えた程度なら ok', () => {
  const prev = new Set(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']);
  const r = evaluateEnumeration(prev, new Set(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i']));
  assert.equal(r.status, 'ok');
  assert.equal(r.disappeared, 1);
});

t('🚨 2割超が消えたら partial (レポート破損の疑い)', () => {
  const prev = new Set(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']);
  const r = evaluateEnumeration(prev, new Set(['a', 'b', 'c', 'd', 'e', 'f', 'g']));
  assert.equal(r.status, 'partial');
  assert.equal(r.disappeared, 3);
});

t('🚨 0件は failed (正常として通さない)', () => {
  const prev = new Set(['a', 'b', 'c']);
  const r = evaluateEnumeration(prev, new Set());
  assert.equal(r.status, 'failed');
  assert.equal(r.reason, 'empty_enumeration');
});

t('🚨 初回でも0件なら failed (前回集合が無くても ok にしない)', () => {
  // ここを ok にすると、空レポートを受けた初回に世代が全消えする
  const r = evaluateEnumeration(new Set(), new Set());
  assert.equal(r.status, 'failed');
  assert.equal(r.reason, 'empty_enumeration');
});

console.log('解析失敗・重複キー (Codex R1-2)');

t('[!] 解析できない行が1つでもあれば完全集合を名乗らせない', () => {
  const base = evaluateEnumeration(new Set(['a']), new Set(['a']));
  assert.equal(base.status, 'ok');
  const r = enumStatusWithParseFailures(base, 1, 0);
  assert.equal(r.status, 'partial');
  assert.equal(r.reason, 'parse_failure');
});

t('[!] 重複キーがあれば partial (INSERT OR REPLACE で隠さない)', () => {
  const base = evaluateEnumeration(new Set(['a']), new Set(['a']));
  assert.equal(enumStatusWithParseFailures(base, 0, 2).status, 'partial');
});

t('解析失敗が無ければ判定はそのまま', () => {
  const base = evaluateEnumeration(new Set(['a']), new Set(['a']));
  assert.equal(enumStatusWithParseFailures(base, 0, 0).status, 'ok');
});

t('0件 (failed) は解析失敗より重い', () => {
  const base = evaluateEnumeration(new Set(['a']), new Set());
  assert.equal(enumStatusWithParseFailures(base, 5, 0).status, 'failed');
});

console.log('ポイント・税区分の欠損 (Codex R1-3 / R1-5)');

t('[!] ポイント列が無ければ null (0で埋めない)', () => {
  const r = amazonRowToSnapshot({ '出品者SKU': 'a', '価格': '1000', 'ステータス': 'Active' }, meta);
  assert.equal(r.points, null);
});

t('ポイント列が空欄なら明示的な0', () => {
  const r = amazonRowToSnapshot({ '出品者SKU': 'a', '価格': '1000', 'ポイント': '', 'ステータス': 'Active' }, meta);
  assert.equal(r.points, 0);
});

t('ポイントが読めない値なら null', () => {
  const r = amazonRowToSnapshot({ '出品者SKU': 'a', '価格': '1000', 'ポイント': 'あり', 'ステータス': 'Active' }, meta);
  assert.equal(r.points, null);
});

t('[!] payment は item レベルにある (実データの形)', () => {
  // 🚨 実データ確認 (2026-09-07): items/search の variant に payment は無く、item にある。
  //    ここを variant から読むと全出品が tax_included_unknown になり、1件も計算できない
  const rows = rakutenItemToSnapshots({
    manageNumber: 'real',
    payment: { taxIncluded: true, taxRate: '0.1', cashOnDeliveryFeeIncluded: false },
    variants: {
      sku: { standardPrice: '798', merchantDefinedSkuId: 'meimeishi3',
        shipping: { shippingMethodGroup: '5', postageIncluded: true, singleItemShipping: 0 } },
    },
  }, meta);
  assert.equal(rows[0].price_incl_tax, 798);
  assert.equal(rows[0].price_tax_included, 1);
  assert.equal(rows[0].mall_tax_rate, 0.1);
  assert.equal(rows[0].fetch_status, 'ok');
});

t('item に taxRate が無い商品でも税込なら採用する (非課税など)', () => {
  const rows = rakutenItemToSnapshots({
    manageNumber: 'notax',
    payment: { taxIncluded: true, cashOnDeliveryFeeIncluded: false },
    variants: { sku: { standardPrice: '500', shipping: { postageIncluded: true } } },
  }, meta);
  assert.equal(rows[0].price_incl_tax, 500);
  assert.equal(rows[0].mall_tax_rate, null);   // 税率は商品マスタ側を使う
  assert.equal(rows[0].fetch_status, 'ok');
});

t('variant 側に payment があればそちらを優先する (details-bulk 互換)', () => {
  const rows = rakutenItemToSnapshots({
    manageNumber: 'both',
    payment: { taxIncluded: true, taxRate: '0.1' },
    variants: { sku: { standardPrice: '1080', payment: { taxIncluded: true, taxRate: '0.08' } } },
  }, meta);
  assert.equal(rows[0].mall_tax_rate, 0.08);
});

t('[!] 楽天 taxIncluded=false の価格を税込として保存しない', () => {
  const rows = rakutenItemToSnapshots({
    manageNumber: 'x', variants: { v: { standardPrice: '1000', payment: { taxIncluded: false, taxRate: '0.1' } } },
  }, meta);
  assert.equal(rows[0].price_incl_tax, null);
  assert.equal(rows[0].price_tax_included, 0);
  assert.equal(rows[0].price_raw, 1000);
  assert.equal(rows[0].fetch_status, 'tax_included_unknown');
});

t('[!] item にも variant にも payment が無ければ税込と決めつけない', () => {
  const rows = rakutenItemToSnapshots({ manageNumber: 'x', variants: { v: { standardPrice: '1000' } } }, meta);
  assert.equal(rows[0].price_incl_tax, null);
  assert.equal(rows[0].fetch_status, 'tax_included_unknown');
});

t('楽天 taxIncluded=true なら税込として採用する', () => {
  const rows = rakutenItemToSnapshots({
    manageNumber: 'x', variants: { v: { standardPrice: '1080', payment: { taxIncluded: true, taxRate: '0.08' } } },
  }, meta);
  assert.equal(rows[0].price_incl_tax, 1080);
  assert.equal(rows[0].fetch_status, 'ok');
});

console.log('\n実行 (deps 差し替え・API は叩かない)');

const db = initExpectedProfitDB();

await ta('Amazon: レポートを取り込み run に記録する', async () => {
  const r = await fetchAmazonListings(db, {
    getActiveListingsReport: async () => ({
      listings: [
        { '出品者SKU': 'sku1', '商品ID': 'B001', '価格': '1980', 'フルフィルメント・チャンネル': 'AMAZON_JP', 'ステータス': 'Active', 'ポイント': '0' },
        { '出品者SKU': 'sku2', '商品ID': 'B002', '価格': '980', 'フルフィルメント・チャンネル': 'DEFAULT', 'ステータス': 'Inactive', 'ポイント': '0' },
      ],
    }),
  });
  assert.equal(r.count, 2);
  assert.equal(r.status, 'ok');
  const run = db.prepare('SELECT * FROM price_fetch_run WHERE run_id = ?').get(r.runId);
  assert.equal(run.status, 'ok');
  assert.equal(run.listing_enum_status, 'ok');
});

await ta('🚨 §10.1-12/13 未販売・在庫切れの出品も母集団に残る', async () => {
  const rows = db.prepare("SELECT mall_item_key, listing_status, mall_item_ref FROM mall_price_snapshot WHERE mall='amazon' ORDER BY mall_item_key").all();
  assert.equal(rows.length, 2);
  assert.equal(rows[1].listing_status, 'inactive');   // Inactive も落とさない
  assert.equal(rows[0].mall_item_ref, 'B001');        // ASIN が DB まで届いている
});

await ta('🚨 §10.1-14 2回目で大量に消えたら partial (行は消さない)', async () => {
  const r = await fetchAmazonListings(db, {
    getActiveListingsReport: async () => ({
      listings: [{ '出品者SKU': 'sku1', '商品ID': 'B001', '価格': '1980', 'ステータス': 'Active' }],
    }),
  });
  assert.equal(r.status, 'partial');       // 2件 → 1件 = 50% 消失
  assert.equal(r.disappeared, 1);
  const run = db.prepare('SELECT status FROM price_fetch_run WHERE run_id = ?').get(r.runId);
  assert.equal(run.status, 'partial');
});

await ta('完全列挙できた最新 run の集合を「完全集合」として引ける', async () => {
  const prev = loadLastCompleteKeys(db, 'amazon');
  assert.equal(prev.keys.size, 2);         // partial の run ではなく ok の run を見る
});

await ta('🚨 レポートの形式が不正なら例外 ({} を0件として通さない)', async () => {
  await assert.rejects(
    () => fetchAmazonListings(db, { getActiveListingsReport: async () => ({}) }),
    /形式が不正/,
  );
});

await ta('API が落ちたら run を failed にして例外を投げる (静かに成功にしない)', async () => {
  const before = db.prepare("SELECT COUNT(*) n FROM price_fetch_run WHERE status='failed'").get().n;
  await assert.rejects(
    () => fetchAmazonListings(db, { getActiveListingsReport: async () => { throw new Error('SP-API 500'); } }),
    /SP-API 500/,
  );
  const after = db.prepare("SELECT COUNT(*) n FROM price_fetch_run WHERE status='failed'").get().n;
  assert.equal(after, before + 1);
  const run = db.prepare("SELECT * FROM price_fetch_run WHERE status='failed' ORDER BY rowid DESC LIMIT 1").get();
  assert.equal(run.listing_enum_status, 'failed');
  assert.match(run.error_summary, /SP-API 500/);
});

await ta('楽天: cursorMark を回して全ページ取る', async () => {
  let calls = 0;
  const r = await fetchRakutenListings(db, {
    searchPage: async (cursor) => {
      calls++;
      if (cursor === '*') return { results: [{ item: rakutenItem }], nextCursorMark: 'c2' };
      return { results: [{ item: { manageNumber: 'item002', variants: { v: { standardPrice: '500' } } } }], nextCursorMark: null };
    },
  });
  assert.equal(calls, 2);
  assert.equal(r.count, 3);                // item001 の2 variant + item002 の1
  assert.equal(r.truncated, false);
});

await ta('🚨 ページ上限に達したら partial (打ち切りを隠さない)', async () => {
  let page = 0;
  const r = await fetchRakutenListings(db, {
    maxPages: 3,
    // 実際の RMS のように、毎ページ違う cursorMark を返し続ける (終わりが来ない)
    searchPage: async () => {
      page++;
      return {
        results: [{ item: { manageNumber: `m${page}`, variants: { v: { standardPrice: '100' } } } }],
        nextCursorMark: `cursor-${page}`,
      };
    },
  });
  assert.equal(r.truncated, true);
  assert.equal(r.status, 'partial');
  assert.equal(page, 3);
  const run = db.prepare('SELECT error_summary FROM price_fetch_run WHERE run_id = ?').get(r.runId);
  assert.match(run.error_summary, /ページ上限/);
});

await ta('nextCursorMark が同じ値を返し続けたら止まる (無限ループ防止)', async () => {
  let page = 0;
  const r = await fetchRakutenListings(db, {
    maxPages: 100,
    searchPage: async () => {
      page++;
      return { results: [{ item: { manageNumber: `z${page}`, variants: { v: { standardPrice: '100' } } } }], nextCursorMark: 'same' };
    },
  });
  assert.ok(page <= 2, `同じ cursor で止まるはず (実際 ${page} 回)`);
  assert.equal(r.truncated, false);
});

await ta('[!] 解析失敗が fetch 経由で DB の listing_enum_status まで届く', async () => {
  // enumStatusWithParseFailures の適用を fetch から外したら、この試験が落ちる
  const r = await fetchAmazonListings(db, {
    getActiveListingsReport: async () => ({
      listings: [
        { '出品者SKU': 'sku1', '商品ID': 'B001', '価格': '1980', 'ステータス': 'Active' },
        { '出品者SKU': '', '商品ID': 'B009', '価格': '100', 'ステータス': 'Active' },   // SKU が読めない
      ],
    }),
  });
  assert.equal(r.unparsable, 1);
  assert.equal(r.status, 'partial');
  const run = db.prepare('SELECT listing_enum_status, error_summary FROM price_fetch_run WHERE run_id = ?').get(r.runId);
  assert.equal(run.listing_enum_status, 'partial');
  assert.match(run.error_summary, /解析できない行/);
});

await ta('[!] 重複キーが fetch 経由で partial になる (INSERT OR REPLACE で隠さない)', async () => {
  const r = await fetchAmazonListings(db, {
    getActiveListingsReport: async () => ({
      listings: [
        { '出品者SKU': 'dup', '商品ID': 'B001', '価格': '100', 'ステータス': 'Active' },
        { '出品者SKU': 'dup', '商品ID': 'B002', '価格': '200', 'ステータス': 'Active' },
      ],
    }),
  });
  assert.equal(r.duplicates, 1);
  assert.equal(r.status, 'partial');
});

await ta('[!] 楽天の mall_item_ref (merchantDefinedSkuId) も DB まで届く', async () => {
  const r = await fetchRakutenListings(db, {
    searchPage: async () => ({
      results: [{ item: { manageNumber: 'refItem', variants: {
        v1: { standardPrice: '1000', merchantDefinedSkuId: 'AM-12345', payment: { taxIncluded: true } },
      } } }],
      nextCursorMark: null,
    }),
  });
  const row = db.prepare("SELECT mall_item_ref FROM mall_price_snapshot WHERE run_id = ? AND mall_item_key = 'refItem/v1'").get(r.runId);
  assert.equal(row.mall_item_ref, 'AM-12345');
});

await ta('[!] 楽天の商品番号 (itemNumber) も DB まで届く — 原価の紐づけ先', async () => {
  // 🚨 システム連携用SKU番号が空欄のときの紐づけ先 (中原さん 2026-09-09)。
  //    ここで落とすと、SKU管理番号で拾い直す旧挙動に戻る = 別商品の原価が付く
  const r = await fetchRakutenListings(db, {
    searchPage: async () => ({
      results: [{ item: { manageNumber: 'treemuddler200', itemNumber: 'treemuddler100-2', variants: {
        treemuddler200: { standardPrice: '648', payment: { taxIncluded: true } },
      } } }],
      nextCursorMark: null,
    }),
  });
  const row = db.prepare(`SELECT mall_item_ref, mall_item_number FROM mall_price_snapshot
    WHERE run_id = ? AND mall_item_key = 'treemuddler200/treemuddler200'`).get(r.runId);
  assert.equal(row.mall_item_number, 'treemuddler100-2', '商品番号が保存されていない');
  assert.equal(row.mall_item_ref, null, 'システム連携用SKU番号が無い出品は null のまま');
});

await ta('商品番号が無い応答でも落ちない (null で入る)', async () => {
  const r = await fetchRakutenListings(db, {
    searchPage: async () => ({
      results: [{ item: { manageNumber: 'noItemNum', variants: {
        v1: { standardPrice: '500', payment: { taxIncluded: true } },
      } } }],
      nextCursorMark: null,
    }),
  });
  const row = db.prepare("SELECT mall_item_number FROM mall_price_snapshot WHERE run_id = ? AND mall_item_key = 'noItemNum/v1'").get(r.runId);
  assert.equal(row.mall_item_number, null);
});

await ta('[!] 楽天 variants が配列なら解析失敗として数える (添字を SKU にしない)', async () => {
  const r = await fetchRakutenListings(db, {
    searchPage: async () => ({
      results: [
        { item: { manageNumber: 'arrItem', variants: [{ standardPrice: '1000', payment: { taxIncluded: true } }] } },
        { item: { manageNumber: 'okItem', variants: { v: { standardPrice: '500', payment: { taxIncluded: true } } } } },
      ],
      nextCursorMark: null,
    }),
  });
  assert.equal(r.unparsable, 1);
  assert.equal(r.status, 'partial');
  const bad = db.prepare("SELECT COUNT(*) n FROM mall_price_snapshot WHERE mall_item_key LIKE 'arrItem/%'").get();
  assert.equal(bad.n, 0);      // 添字キーの行を作らない
});

await ta('[!] 正常と異常の variant が混在しても解析失敗を数える (欠落を完全集合にしない)', async () => {
  // 行が1つできれば OK とすると、壊れた variant が静かに消えて次の完全集合が欠落する
  const r = await fetchRakutenListings(db, {
    searchPage: async () => ({
      results: [{ item: { manageNumber: 'mixItem', variants: { good: { standardPrice: '100', payment: { taxIncluded: true } }, bad: 'not-an-object' } } }],
      nextCursorMark: null,
    }),
  });
  const rows = db.prepare("SELECT mall_item_key FROM mall_price_snapshot WHERE run_id = ?").all(r.runId);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].mall_item_key, 'mixItem/good');   // 正常行は残す
  assert.equal(r.unparsable, 1);                          // 異常 variant を数える
  assert.equal(r.status, 'partial');
  const run = db.prepare('SELECT listing_enum_status FROM price_fetch_run WHERE run_id = ?').get(r.runId);
  assert.equal(run.listing_enum_status, 'partial');       // DB まで届く
});

t('rakutenItemToSnapshotsDetailed は variant 単位の失敗数を返す', () => {
  const r = rakutenItemToSnapshotsDetailed({
    manageNumber: 'm', variants: { a: { standardPrice: '1' }, b: 'x', c: null },
  }, meta);
  assert.equal(r.rows.length, 1);
  assert.equal(r.unparsable, 2);
});


console.log('フルフィルメントの確定 (Codex R3-2)');

t('[!] チャンネル列が無ければ FBM と決めつけない (未解決にする)', () => {
  // FBA の行を FBM として計算すると、配送費も費用範囲も変わり順位が狂う
  assert.equal(amazonFulfillment(undefined), null);
  assert.equal(amazonFulfillment(''), null);
  assert.equal(amazonFulfillment('   '), null);
});

t('[!] 未知のチャンネル値も未解決にする', () => {
  assert.equal(amazonFulfillment('SOMETHING_NEW'), null);
});

t('確認済みの値だけ FBA / FBM に変換する', () => {
  assert.equal(amazonFulfillment('AMAZON_JP'), 'FBA');
  assert.equal(amazonFulfillment('AFN'), 'FBA');
  assert.equal(amazonFulfillment('DEFAULT'), 'FBM');
  assert.equal(amazonFulfillment('MFN'), 'FBM');
  assert.equal(amazonFulfillment('MERCHANT'), 'FBM');
});

t('[!] チャンネル欠損の行は fulfillment が null のまま保存される', () => {
  const r = amazonRowToSnapshot({ '出品者SKU': 'a', '商品ID': 'B1', '価格': '1000', 'ステータス': 'Active' }, meta);
  assert.equal(r.fulfillment, null);
});



console.log('');
console.log('出品レポートを待てる時間 (実データ: Amazon 側が混んでいて5分で諦めた)');

t('既定は5分 (既存の呼び出し元の挙動を変えない)', () => {
  const now = new Date('2026-09-07T15:00:00Z');
  assert.equal(reportWaitUntil({ now }).toISOString(), '2026-09-07T15:05:00.000Z');
});

t('[!] 期限が渡されたら期限まで待つ (夜間バッチは7時間の余裕がある)', () => {
  const now = new Date('2026-09-07T15:00:00Z');
  const until = reportWaitUntil({ now, deadline: new Date('2026-09-07T15:20:00Z') });
  assert.equal(until.toISOString(), '2026-09-07T15:20:00.000Z');
});

t('[!] 期限が遠すぎても上限30分で止める (無限に待たない)', () => {
  const now = new Date('2026-09-07T15:00:00Z');
  const until = reportWaitUntil({ now, deadline: new Date('2026-09-08T06:00:00Z') });
  assert.equal(until.toISOString(), '2026-09-07T15:30:00.000Z');
});

t('[!] 期限が既に過ぎていたら待たない (すぐ諦める)', () => {
  const now = new Date('2026-09-07T15:00:00Z');
  const until = reportWaitUntil({ now, deadline: new Date('2026-09-07T14:00:00Z') });
  assert.ok(until <= now);
});

t('[!] 期限が読めない値なら既定に戻す (NaN で即諦めない)', () => {
  const now = new Date('2026-09-07T15:00:00Z');
  assert.equal(reportWaitUntil({ now, deadline: 'ごみ' }).toISOString(), '2026-09-07T15:05:00.000Z');
  assert.equal(reportWaitUntil({ now, deadline: null }).toISOString(), '2026-09-07T15:05:00.000Z');
});

t('maxWaitMs を渡せば従う (上限は超えない)', () => {
  const now = new Date('2026-09-07T15:00:00Z');
  assert.equal(reportWaitUntil({ now, maxWaitMs: 60000 }).toISOString(), '2026-09-07T15:01:00.000Z');
  assert.equal(reportWaitUntil({ now, maxWaitMs: 99 * 60 * 1000 }).toISOString(), '2026-09-07T15:30:00.000Z');
});

await ta('[!] fetchAmazonListings は期限をレポート取得まで渡す (渡し忘れると5分で諦める)', async () => {
  let seen = null;
  // 🚨 固定日時の期限を渡すと、その日を過ぎた翌日から落ちる = 書いた日にしか通らない試験
  //    (実際に 3 回これで落ちた)。fetchAmazonListings は実時計で期限を見るので相対時刻にする
  const deadline = new Date(Date.now() + 6 * 60 * 60 * 1000);
  await fetchAmazonListings(db, {
    deadline,
    getActiveListingsReport: async (o) => { seen = o; return { listings: [] }; },
  });
  assert.ok(seen, 'オプションが渡っていない');
  assert.equal(new Date(seen.deadline).toISOString(), deadline.toISOString());
});


// 本番の待ちループそのものを通す (時計と sleep を差し替える)
function fakeSp(statuses) {
  let i = 0;
  return {
    polls: 0,
    async callAPI(req) {
      if (req.operation === 'createReport') return { reportId: 'R1' };
      if (req.operation === 'getReport') {
        this.polls++;
        return { processingStatus: statuses[Math.min(i++, statuses.length - 1)] };
      }
      throw new Error('想定外の operation: ' + req.operation);
    },
  };
}

await ta('[!] 待ちループが期限を使う (期限なしの5分では諦める回数だけ回る)', async () => {
  let t = new Date('2026-09-07T15:00:00Z').getTime();
  const client = fakeSp(['IN_PROGRESS']);
  await assert.rejects(
    () => getActiveListingsReport({
      client, marketplaceId: 'M1',
      now: () => new Date(t), log: () => {},
      sleep: async (ms) => { t += ms; },     // 時計を進めるだけ
    }),
    /レポート取得タイムアウト/);
  // 5分 / 5秒 = 60回ぶん眠るが、**期限ちょうどでは API を呼ばない** ので 59 回 (Codex R7-4)
  assert.equal(client.polls, 59, `59回のはず (実際 ${client.polls})`);
});

await ta('[!] 期限を渡すとその分だけ長く待つ (実データ: 5分では足りなかった)', async () => {
  let t = new Date('2026-09-07T15:00:00Z').getTime();
  const client = fakeSp(['IN_PROGRESS']);
  await assert.rejects(
    () => getActiveListingsReport({
      client, marketplaceId: 'M1',
      deadline: new Date('2026-09-07T15:20:00Z'),
      now: () => new Date(t), log: () => {},
      sleep: async (ms) => { t += ms; },
    }),
    /レポート取得タイムアウト/);
  assert.equal(client.polls, 239, `20分ぶん (期限ちょうどは呼ばない) = 239回のはず (実際 ${client.polls})`);
});

await ta('[!] 残り時間より長く眠らない (期限の1秒前に入ったら1秒だけ眠って終わる)', async () => {
  let t = new Date('2026-09-07T15:00:00Z').getTime();
  const slept = [];
  const client = fakeSp(['IN_PROGRESS']);
  await assert.rejects(
    () => getActiveListingsReport({
      client, marketplaceId: 'M1',
      deadline: new Date('2026-09-07T15:00:01Z'),
      now: () => new Date(t),
      sleep: async (ms) => { slept.push(ms); t += ms; }, log: () => {},
    }),
    /レポート取得タイムアウト/);
  assert.deepEqual(slept, [1000], '5秒眠ると期限を4秒過ぎてから API を呼ぶ');
  assert.equal(client.polls, 0);
});

await ta('[!] DONE でも期限を過ぎていたら本体を取りに行かない (数MBある)', async () => {
  let t = new Date('2026-09-07T15:00:00Z').getTime();
  await assert.rejects(
    () => getActiveListingsReport({
      client: {
        async callAPI(req) {
          if (req.operation === 'createReport') return { reportId: 'R1' };
          if (req.operation === 'getReport') { t += 10 * 60 * 1000; return { processingStatus: 'DONE' }; }
          throw new Error('本体を取りに行ってしまった: ' + req.operation);
        },
      },
      marketplaceId: 'M1',
      deadline: new Date('2026-09-07T15:05:00Z'),
      now: () => new Date(t),
      sleep: async (ms) => { t += ms; }, log: () => {},
    }),
    /期限を過ぎたので取得しない/);
});

await ta('[!] ドキュメント情報の取得中に期限を越えたら、本体 (数MB) を取りに行かない', async () => {
  // 🚨 getReportDocument の前で見るだけでは足りない。この API に時間がかかると、
  //    期限を過ぎてから fetch() を始めてしまう (Codex R8-2)
  let t = new Date('2026-09-07T15:00:00Z').getTime();
  let fetched = false;
  await assert.rejects(
    () => getActiveListingsReport({
      client: {
        async callAPI(req) {
          if (req.operation === 'createReport') return { reportId: 'R1' };
          if (req.operation === 'getReport') return { processingStatus: 'DONE', reportDocumentId: 'D1' };
          if (req.operation === 'getReportDocument') { t += 10 * 60 * 1000; return { url: 'http://x/y' }; }
          throw new Error('想定外: ' + req.operation);
        },
      },
      marketplaceId: 'M1',
      deadline: new Date('2026-09-07T15:05:00Z'),
      now: () => new Date(t),
      sleep: async (ms) => { t += ms; }, log: () => {},
      fetchImpl: async () => { fetched = true; return { arrayBuffer: async () => new ArrayBuffer(0) }; },
    }),
    /本体を取りに行く前に期限を過ぎた/);
  assert.equal(fetched, false, '期限を過ぎているのにダウンロードを始めた');
});

await ta('[!] 期限を過ぎていたらレポートを作りにも行かない', async () => {
  let created = false;
  await assert.rejects(
    () => getActiveListingsReport({
      client: { async callAPI(req) { if (req.operation === 'createReport') created = true; return { reportId: 'R1' }; } },
      marketplaceId: 'M1',
      deadline: new Date('2026-09-07T14:00:00Z'),
      now: () => new Date('2026-09-07T15:00:00Z'),
      sleep: async () => {}, log: () => {},
    }),
    /期限を過ぎているのでレポートを作らない/);
  assert.equal(created, false);
});

await ta('DONE になったらそこで待つのをやめる', async () => {
  let t = new Date('2026-09-07T15:00:00Z').getTime();
  const client = fakeSp(['IN_QUEUE', 'IN_PROGRESS', 'DONE']);
  // DONE の後は本物の getReportDocument に進むので、そこで落ちるのが正しい
  await assert.rejects(
    () => getActiveListingsReport({
      client, marketplaceId: 'M1',
      deadline: new Date('2026-09-07T15:20:00Z'),
      now: () => new Date(t), log: () => {}, sleep: async (ms) => { t += ms; },
    }),
    /想定外の operation: getReportDocument/);
  assert.equal(client.polls, 3);
});

await ta('[!] FATAL は待たずに失敗させる (期限まで粘らない)', async () => {
  let t = new Date('2026-09-07T15:00:00Z').getTime();
  const client = fakeSp(['FATAL']);
  await assert.rejects(
    () => getActiveListingsReport({
      client, marketplaceId: 'M1',
      deadline: new Date('2026-09-07T15:20:00Z'),
      now: () => new Date(t), log: () => {}, sleep: async (ms) => { t += ms; },
    }),
    /レポート処理失敗: FATAL/);
  assert.equal(client.polls, 1);
});

await ta('タイムアウトの文言に、どれだけ待って最後がどの状態だったかを残す', async () => {
  let t = new Date('2026-09-07T15:00:00Z').getTime();
  await assert.rejects(
    () => getActiveListingsReport({
      client: fakeSp(['IN_PROGRESS']), marketplaceId: 'M1',
      now: () => new Date(t), log: () => {}, sleep: async (ms) => { t += ms; },
    }),
    /300秒待った \/ 最後の状態=IN_PROGRESS/);
});


console.log('');
console.log('Amazon FBM の送料込み判定 (中原さん決定 2026-09-08)');

t('FBA は常に送料込み (プライム配送)', () => {
  assert.equal(amazonPostageIncluded('FBA', null), true);
  assert.equal(amazonPostageIncluded('FBA', '移行された配送パターン'), true);
});

t('[!] FBM のマケプレプライム設定は送料込み (プライム会員への配送料無料が条件)', () => {
  // 実測: FBM 3,466 件のうち 3,377 件がこの配送パターン
  assert.equal(amazonPostageIncluded('FBM', 'ネコポスマケプレプライム設定'), true);
  assert.equal(amazonPostageIncluded('FBM', 'プライム配送パターン'), true);
});

t('[!] 知らない配送パターンは「不明」にする (勝手に送料込みへ倒さない)', () => {
  // 🚨 新しい配送パターンを作ったときに、黙って利益を高く見せないため
  assert.equal(amazonPostageIncluded('FBM', 'ヤマト北海道・沖縄送料別途設定'), null);
  assert.equal(amazonPostageIncluded('FBM', '来年つくる新しいパターン'), null);
});

t('[!] 「移行された配送パターン」は送料込みにしない (根拠が無い・Codex R12)', () => {
  // 🚨 名前からは何も分からない。中原さんが承認したのはマケプレプライムだけ。
  //    根拠なく入れると、送料を別途もらっている出品の利益を高く見せてしまう
  assert.equal(amazonPostageIncluded('FBM', '移行された配送パターン'), null);
});

t('[!] 許可リストに載せてよいのはプライム扱いのものだけ', () => {
  for (const g of AMAZON_POSTAGE_INCLUDED_GROUPS) {
    assert.ok(/プライム|Prime/i.test(g), `根拠の言えない配送パターンが混ざっている: ${g}`);
  }
});

t('配送パターンが読めなければ不明', () => {
  assert.equal(amazonPostageIncluded('FBM', ''), null);
  assert.equal(amazonPostageIncluded('FBM', null), null);
  assert.equal(amazonPostageIncluded('FBM', undefined), null);
});

t('[!] 出荷区分が未解決なら判断しない', () => {
  assert.equal(amazonPostageIncluded(null, 'ネコポスマケプレプライム設定'), null);
});

t('前後の空白を吸収する', () => {
  assert.equal(amazonPostageIncluded('FBM', '  ネコポスマケプレプライム設定  '), true);
});

t('[!] レポートの行から snapshot まで届く', () => {
  const snap = amazonRowToSnapshot({
    '出品者SKU': 'sku1', '商品ID': 'B001', '価格': '980', 'ポイント': '0',
    'フルフィルメント・チャンネル': 'DEFAULT', 'ステータス': 'Active',
    'merchant-shipping-group': 'ネコポスマケプレプライム設定',
  }, { runId: 'r1', shopId: 'S1@M1', fetchedAt: '2026-09-08T00:00:00Z', validUntil: '2099-01-01T00:00:00Z' });
  assert.equal(snap.fulfillment, 'FBM');
  assert.equal(snap.postage_included, 1);
  assert.equal(snap.postage_revenue_incl_tax, 0);
  assert.equal(snap.shipping_group, 'ネコポスマケプレプライム設定', '判断の根拠を残す');
});

t('[!] 知らない配送パターンの行は送料不明のまま届く', () => {
  const snap = amazonRowToSnapshot({
    '出品者SKU': 'sku2', '商品ID': 'B002', '価格': '980', 'ポイント': '0',
    'フルフィルメント・チャンネル': 'DEFAULT', 'ステータス': 'Active',
    'merchant-shipping-group': 'ヤマト北海道・沖縄送料別途設定',
  }, { runId: 'r1', shopId: 'S1@M1', fetchedAt: '2026-09-08T00:00:00Z', validUntil: '2099-01-01T00:00:00Z' });
  assert.equal(snap.postage_included, null);
  assert.equal(snap.postage_revenue_incl_tax, null);
  assert.equal(snap.shipping_group, 'ヤマト北海道・沖縄送料別途設定');
});

t('送料込みと判断する配送パターンの一覧は空でない (実測に基づく)', () => {
  assert.ok(AMAZON_POSTAGE_INCLUDED_GROUPS.includes('ネコポスマケプレプライム設定'));
});

await ta('[!] DB まで shipping_group が保存される', async () => {
  const r = await fetchAmazonListings(db, {
    getActiveListingsReport: async () => ({
      listings: [{
        '出品者SKU': 'shipgrp', '商品ID': 'B009', '価格': '1500', 'ポイント': '0',
        'フルフィルメント・チャンネル': 'DEFAULT', 'ステータス': 'Active',
        'merchant-shipping-group': 'ネコポスマケプレプライム設定',
      }],
    }),
  });
  const row = db.prepare("SELECT * FROM mall_price_snapshot WHERE run_id = ? AND mall_item_key = 'shipgrp'").get(r.runId);
  assert.equal(row.shipping_group, 'ネコポスマケプレプライム設定');
  assert.equal(row.postage_included, 1);
});

console.log('\n商品一覧の履歴保存 (Company DB構想 06 Step 0)');

const RAW_TSV = 'seller-sku\tasin1\tprice\tfulfillment-channel\tstatus\nsku9\tB009\t1500\tDEFAULT\tActive\n';

await ta('[!] Amazon: 原文 (rawText) を tsv のまま保存に渡す。complete=true・enum_status・api_version も', async () => {
  const calls = [];
  const r = await fetchAmazonListings(db, {
    getActiveListingsReport: async () => ({
      listings: [{ 'seller-sku': 'sku9', 'asin1': 'B009', 'price': '1500', 'fulfillment-channel': 'DEFAULT', 'status': 'Active' }],
      rawText: RAW_TSV, apiVersion: 'reports/2021-06-30',
    }),
    archive: async (args) => { calls.push(args); return { action: 'archived', code: 'archived', relFile: 'amazon/x.tsv.gz', items: 1, sameAsPrevious: false, complete: true, offsite: 'skipped' }; },
  });
  assert.equal(calls.length, 1);
  assert.equal(calls[0].mall, 'amazon');
  assert.equal(calls[0].format, 'tsv');
  assert.equal(calls[0].payload, RAW_TSV);              // 解析済みの行ではなく原文
  assert.equal(calls[0].runId, r.runId);                // price_fetch_run と突き合わせる鍵
  assert.equal(calls[0].source, 'merchant_listings_all_data');
  assert.equal(calls[0].meta.complete, true);
  assert.equal(calls[0].meta.enum_status, r.status);
  assert.equal(calls[0].meta.api_version, 'reports/2021-06-30');
  assert.equal(calls[0].noOffsite, true);               // 🚨 取得の途中で rclone を待たない (offsite は nightly が公開後に)
  assert.equal(r.archive.code, 'archived');
  assert.equal(r.archive.file, 'amazon/x.tsv.gz');
});

await ta('[!] レポート取得には includeRawText を頼む (画面向けの経路には原文を付けない)', async () => {
  let seen = null;
  await fetchAmazonListings(db, {
    getActiveListingsReport: async (o) => { seen = o; return { listings: [{ 'seller-sku': 'sku9', 'price': '1500', 'status': 'Active' }] }; },
    archive: false,
  });
  assert.equal(seen.includeRawText, true);
  const rep = await getActiveListingsReport({
    client: { callAPI: async ({ operation }) => (operation === 'createReport' ? { reportId: 'r' }
      : operation === 'getReport' ? { processingStatus: 'DONE', reportDocumentId: 'd' } : { url: 'http://x', compressionAlgorithm: null }) },
    sleep: async () => {}, log: () => {},
    fetchImpl: async () => ({ arrayBuffer: async () => Buffer.from('seller-sku\tprice\nsku1\t100\n', 'utf-8') }),
  });
  assert.equal(rep.rawText, undefined);                  // 頼まなければ付かない
  assert.equal(rep.listings.length, 1);
  const rep2 = await getActiveListingsReport({
    client: { callAPI: async ({ operation }) => (operation === 'createReport' ? { reportId: 'r' }
      : operation === 'getReport' ? { processingStatus: 'DONE', reportDocumentId: 'd' } : { url: 'http://x', compressionAlgorithm: null }) },
    sleep: async () => {}, log: () => {}, includeRawText: true,
    fetchImpl: async () => ({ arrayBuffer: async () => Buffer.from('seller-sku\tprice\nsku1\t100\n', 'utf-8') }),
  });
  assert.equal(rep2.rawText, 'seller-sku\tprice\nsku1\t100\n');
  assert.equal(rep2.apiVersion, 'reports/2021-06-30');
});

await ta('[!] 楽天: 途中のページで落ちても、取れた分は complete=false で残す (取得は失敗のまま)', async () => {
  const calls = [];
  let page = 0;
  await assert.rejects(
    () => fetchRakutenListings(db, {
      searchPage: async () => {
        page++;
        if (page === 2) throw new Error('HTTP 503');
        return { results: [{ item: { manageNumber: 'p1', variants: { v: { standardPrice: '100' } } } }], nextCursorMark: 'c1' };
      },
      archive: async (args) => { calls.push(args); return { action: 'archived', code: 'archived', complete: false }; },
    }),
    /HTTP 503/,
  );
  assert.equal(calls.length, 1);
  assert.equal(calls[0].items, 1);
  assert.equal(calls[0].meta.complete, false);
  assert.equal(calls[0].meta.enum_status, 'failed');
  assert.equal(calls[0].meta.pages, 1);
  assert.match(calls[0].meta.note, /途中で失敗/);
  const run = db.prepare("SELECT status, listing_enum_status FROM price_fetch_run WHERE mall='rakuten' ORDER BY started_at DESC, rowid DESC LIMIT 1").get();
  assert.equal(run.status, 'failed');                    // DB の記録は従来どおり失敗
});

await ta('楽天: 1 ページ目で落ちたら (取れた分が無い) 保存は呼ばない', async () => {
  const calls = [];
  await assert.rejects(
    () => fetchRakutenListings(db, {
      searchPage: async () => { throw new Error('HTTP 500'); },
      archive: async (args) => { calls.push(args); return { action: 'archived', code: 'archived' }; },
    }),
    /HTTP 500/,
  );
  assert.equal(calls.length, 0);
});

await ta('[!] 保存が落ちても取得結果は変わらない (fail-soft)。理由は archive に残る', async () => {
  const r = await fetchAmazonListings(db, {
    getActiveListingsReport: async () => ({
      listings: [{ 'seller-sku': 'sku9', 'asin1': 'B009', 'price': '1500', 'fulfillment-channel': 'DEFAULT', 'status': 'Active' }],
      rawText: RAW_TSV,
    }),
    archive: async () => { throw Object.assign(new Error('disk full'), { code: 'ENOSPC' }); },
  });
  assert.notEqual(r.status, 'failed');
  const run = db.prepare('SELECT status, listing_enum_status FROM price_fetch_run WHERE run_id = ?').get(r.runId);
  assert.equal(run.listing_enum_status, r.status);      // DB の記録も従来どおり
  assert.equal(r.archive.action, 'error');
  assert.equal(r.archive.code, 'ENOSPC');
  assert.match(r.archive.error, /disk full/);
});

await ta('archive: false で止められる (試験・手動)', async () => {
  const r = await fetchAmazonListings(db, {
    getActiveListingsReport: async () => ({ listings: [{ 'seller-sku': 'sku9', 'price': '1500', 'status': 'Active' }] }),
    archive: false,
  });
  assert.equal(r.archive.code, 'disabled');
});

await ta('[!] 楽天: 応答の要素をそのまま (item の包みごと) 渡す。打ち切りの夜は complete=false・pages・truncated', async () => {
  const calls = [];
  let page = 0;
  const r = await fetchRakutenListings(db, {
    maxPages: 2,
    searchPage: async () => {
      page++;
      return { results: [{ item: { manageNumber: `raw${page}`, title: `商品${page}`, images: [{ location: '/a.jpg' }], variants: { v: { standardPrice: '100' } } } }], nextCursorMark: `c${page}` };
    },
    archive: async (args) => { calls.push(args); return { action: 'archived', code: 'archived', relFile: 'rakuten/y.ndjson.gz', items: args.items, complete: args.meta.complete, offsite: 'skipped' }; },
  });
  assert.equal(calls.length, 1);
  const a = calls[0];
  assert.equal(a.mall, 'rakuten');
  assert.equal(a.format, 'ndjson');
  assert.equal(a.source, 'rms_items_search');
  assert.equal(a.items, 2);
  assert.equal(a.payload[0].item.title, '商品1');       // title・images を落とさない (snapshot 行には無い列)
  assert.equal(a.sortKey(a.payload[1]), 'raw2');
  assert.equal(a.meta.complete, false);                   // ページ上限 = 完走していない
  assert.equal(a.meta.truncated, true);
  assert.equal(a.meta.pages, 2);
  assert.equal(a.meta.enum_status, 'partial');
  assert.equal(r.archive.complete, false);
});

await ta('[!] 0 件の夜は保存しない (既定の保存器で code=empty。ファイルも manifest も作らない)', async () => {
  const r = await fetchRakutenListings(db, {
    searchPage: async () => ({ results: [], nextCursorMark: null }),
  });
  assert.equal(r.status, 'failed');
  assert.equal(r.archive.action, 'skipped');
  assert.equal(r.archive.code, 'empty');
  // 前の試験で既定の保存器が rakuten の履歴を作っている。この run の行と gz が無いことを見る
  const mfPath = path.join(process.env.DATA_DIR, 'mall-items-history', 'rakuten', 'manifest.jsonl');
  const recs = fs.existsSync(mfPath) ? fs.readFileSync(mfPath, 'utf-8').trim().split('\n').filter(Boolean).map(l => JSON.parse(l)) : [];
  assert.equal(recs.some(m => m.run_id === r.runId), false);
  assert.equal(r.archive.file, null);
});

await ta('[!] 既定の保存器で通す: gz と manifest が DATA_DIR/mall-items-history/amazon にでき、run_id が一致する', async () => {
  const r = await fetchAmazonListings(db, {
    getActiveListingsReport: async () => ({
      listings: [{ 'seller-sku': 'sku9', 'asin1': 'B009', 'price': '1500', 'fulfillment-channel': 'DEFAULT', 'status': 'Active' }],
      rawText: RAW_TSV, apiVersion: 'reports/2021-06-30',
    }),
    // offsite は env が無いので skipped。rclone は呼ばれない
  });
  assert.equal(r.archive.code, 'archived');
  const file = path.join(process.env.DATA_DIR, 'mall-items-history', r.archive.file);
  assert.ok(fs.existsSync(file), file);
  const { gunzipSync } = await import('zlib');
  assert.equal(gunzipSync(fs.readFileSync(file)).toString('utf-8'), RAW_TSV);
  const mf = fs.readFileSync(path.join(process.env.DATA_DIR, 'mall-items-history', 'amazon', 'manifest.jsonl'), 'utf-8').trim().split('\n').map(l => JSON.parse(l));
  const rec = mf.find(m => m.run_id === r.runId);
  assert.ok(rec, 'manifest に run_id の行がある');
  assert.equal(rec.complete, true);
  assert.equal(rec.items, 1);
  assert.equal(rec.enum_status, r.status);
});

console.log('\nセラーID の取り違え (2026-09-14 発覚: 9/9 夜から Amazon が全部「判定できない」)');

const MKT = 'A1VC38T7YXB528';
const skuList = (n) => Array.from({ length: n }, (_, i) => `sid${i + 1}`);
const amazonReportOf = (skus) => async () => ({
  listings: skus.map(s => ({ '出品者SKU': s, '商品ID': `B${s}`, '価格': '1000', 'フルフィルメント・チャンネル': 'DEFAULT', 'ステータス': 'Active', 'ポイント': '0' })),
});
/** 直近の「完全に列挙できた」run を置く (本番の 9/9 13:22 の回 = unknown@ で記録された回を再現する) */
function seedAmazonCompleteRun(shopId, skus) {
  const runId = `r_seed_${Math.random().toString(36).slice(2)}`;
  const at = new Date(Date.now() + 1000).toISOString();   // それまでの ok run より新しくする
  db.prepare(`INSERT INTO price_fetch_run (run_id, mall, started_at, finished_at, status, listing_enum_status)
              VALUES (?, 'amazon', ?, ?, 'ok', 'ok')`).run(runId, at, at);
  const stmt = db.prepare(`INSERT INTO mall_price_snapshot
    (run_id, mall, shop_id, mall_item_key, mall_item_ref, fulfillment, price_incl_tax, price_tax_included, mall_tax_rate,
     postage_included, postage_revenue_incl_tax, points, listing_status, fetch_status, resolve_status, valid_until, source, fetched_at)
    VALUES (?, 'amazon', ?, ?, ?, 'FBM', 1000, 1, 0.1, 1, 0, 0, 'active', 'ok', 'unresolved', ?, 'test', ?)`);
  for (const s of skus) stmt.run(runId, shopId, s, `B${s}`, at, at);
  return runId;
}

t('canonicalShopId: 同じ市場なら今の shop_id に揃える (unknown も実セラーも)', () => {
  assert.equal(canonicalShopId(`unknown@${MKT}`, `S1@${MKT}`), `S1@${MKT}`);
  assert.equal(canonicalShopId(`S2@${MKT}`, `S1@${MKT}`), `S1@${MKT}`, '実セラー同士でも揃える (Codex R1-P1: 覚え書きの自動更新で ID が変わる)');
  assert.equal(canonicalShopId(`unknown@${MKT}`, 'S1@OTHER'), `unknown@${MKT}`, '別の市場は揃えない');
  assert.equal(canonicalShopId(`S2@${MKT}`, 'S1@OTHER'), `S2@${MKT}`, '別の市場は揃えない');
  assert.equal(canonicalShopId('1', '1'), '1', '楽天 (shop_id に @ が無い) はそのまま');
  assert.equal(canonicalShopId(`unknown@${MKT}`, null), `unknown@${MKT}`, '今の shop_id を渡さなければ何もしない');
});

t('[!] Amazon の shop_id のセラーは 覚え書き → env の順 (手数料の見積と同じ出どころ)', () => {
  const saved = process.env.SP_API_SELLER_ID;
  try {
    delete process.env.SP_API_SELLER_ID;
    assert.equal(amazonShopId(db), `unknown@${MKT}`, 'どちらも無ければ unknown (従来どおり)');
    process.env.SP_API_SELLER_ID = 'ENV1';
    assert.equal(amazonShopId(db), `ENV1@${MKT}`);
    setSetting(db, SETTING_AMAZON_SELLER_ID, 'A6HMLHKUUJC27', new Date().toISOString());
    assert.equal(amazonShopId(db), `A6HMLHKUUJC27@${MKT}`, '覚え書きが env より優先');
  } finally {
    if (saved === undefined) delete process.env.SP_API_SELLER_ID; else process.env.SP_API_SELLER_ID = saved;
  }
});

await ta('[!] 前回の完全集合が unknown@ で記録されていても、同じ出品なら「消えた」にしない (9/9 夜の再現)', async () => {
  const skus = skuList(10);
  seedAmazonCompleteRun(`unknown@${MKT}`, skus);
  const r = await fetchAmazonListings(db, { getActiveListingsReport: amazonReportOf(skus), archive: false });
  assert.equal(r.status, 'ok', `partial になった (消えた ${r.disappeared} 件)`);
  assert.equal(r.disappeared, 0);
  const shops = db.prepare('SELECT DISTINCT shop_id FROM mall_price_snapshot WHERE run_id = ?').all(r.runId).map(x => x.shop_id);
  assert.deepEqual(shops, [`A6HMLHKUUJC27@${MKT}`], '今夜の行は覚え書きのセラーで記録する');
});

await ta('[!] 揃えても、本当に消えた出品は partial として見える (歯止めは弱めない)', async () => {
  seedAmazonCompleteRun(`unknown@${MKT}`, skuList(10));
  const r = await fetchAmazonListings(db, { getActiveListingsReport: amazonReportOf(skuList(5)), archive: false });
  assert.equal(r.status, 'partial');
  assert.equal(r.disappeared, 5);
});

await ta('[!] 前回が別の実セラー ID でも、同じ市場・同じ出品なら「消えた」にしない (Codex R1-P1)', async () => {
  seedAmazonCompleteRun(`OTHER@${MKT}`, skuList(10));
  const r = await fetchAmazonListings(db, { getActiveListingsReport: amazonReportOf(skuList(10)), archive: false });
  assert.equal(r.status, 'ok', `partial になった (消えた ${r.disappeared} 件)`);
  assert.equal(r.disappeared, 0);
});

await ta('[!] セラーの覚え書きが夜のあいだに変わっても、次の夜もその次の夜も ok のまま (Codex R1-P1 の再現: 直す前は ok → partial → partial)', async () => {
  const setSeller = (id) => setSetting(db, SETTING_AMAZON_SELLER_ID, id, new Date().toISOString());
  try {
    setSeller('OLDSELLER');
    seedAmazonCompleteRun(`OLDSELLER@${MKT}`, skuList(10));
    const first = await fetchAmazonListings(db, { getActiveListingsReport: amazonReportOf(skuList(10)), archive: false });
    setSeller('NEWSELLER');   // 手数料 API の応答で覚え書きが自動更新された、を模す
    const second = await fetchAmazonListings(db, { getActiveListingsReport: amazonReportOf(skuList(10)), archive: false });
    const third = await fetchAmazonListings(db, { getActiveListingsReport: amazonReportOf(skuList(10)), archive: false });
    assert.deepEqual([first.status, second.status, third.status], ['ok', 'ok', 'ok']);
  } finally {
    setSeller('A6HMLHKUUJC27');
  }
});

await ta('[!] 別の市場の集合とは揃えない (本当に別の店なら「消えた」として見える)', async () => {
  seedAmazonCompleteRun('A6HMLHKUUJC27@OTHERMARKET', skuList(10));
  const r = await fetchAmazonListings(db, { getActiveListingsReport: amazonReportOf(skuList(10)), archive: false });
  assert.equal(r.status, 'partial');
  assert.equal(r.disappeared, 10);
});

console.log('');
console.log('Yahoo!ショッピング (2026-09-19 中原さん「Yahoo ショッピングも入れたい」)');

const ySnap = (detail) => yahooDetailToSnapshotsDetailed(detail, meta);
const yDetail = (over = {}) => ({
  ok: true, ItemCode: 'aromainb', Name: 'テスト', Price: 1080,
  SubCodes: [], SalePrice: null, SalePriceReadable: true,
  Delivery: '1', PostageSet: '12', ShipWeight: null, ...over,
});

t('[!] 送料込みの判定: Delivery=1 だけを送料無料とみなす (知らない値は倒さない)', () => {
  assert.equal(yahooPostageIncluded('1'), true);
  assert.equal(yahooPostageIncluded('2'), null);
  assert.equal(yahooPostageIncluded(''), null);
  assert.equal(yahooPostageIncluded(null), null);
  assert.equal(yahooPostageIncluded(undefined), null);
});

t('[!] 送料無料なら収入 0、それ以外は「不明」(0 円と書かない)', () => {
  const free = ySnap(yDetail()).rows[0];
  assert.equal(free.postage_included, 1);
  assert.equal(free.postage_revenue_incl_tax, 0);
  const other = ySnap(yDetail({ Delivery: '2' })).rows[0];
  assert.equal(other.postage_included, null);
  assert.equal(other.postage_revenue_incl_tax, null, '不明な配送設定を 0 円にしている');
});

t('[!] SubCode が無い商品は 1 行。鍵は商品コードそのもの', () => {
  const { rows, unparsable } = ySnap(yDetail());
  assert.equal(unparsable, 0);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].mall_item_key, 'aromainb');
  assert.equal(rows[0].mall, 'yahoo');
  assert.equal(rows[0].fulfillment, 'self');
  assert.equal(rows[0].price_incl_tax, 1080);
  assert.equal(rows[0].price_tax_included, 1, 'Yahoo の価格は税込');
  assert.equal(rows[0].mall_tax_rate, null, '税率は API が返さない (NE 側を使う)');
  assert.equal(rows[0].fetch_status, 'ok');
});

t('[!] SubCode がある商品は SubCode ごとに 1 行。親の行は作らない (別商品の原価で計算しないため)', () => {
  const { rows } = ySnap(yDetail({
    SubCodes: [{ SubCode: 'ankis-gmrs', Price: null }, { SubCode: 'ankis-omrs', Price: 1280 }],
  }));
  assert.deepEqual(rows.map(r => r.mall_item_key), ['aromainb/ankis-gmrs', 'aromainb/ankis-omrs']);
  // 🚨 SubCode の価格が null = 親と同額。0 円にも「取れなかった」にもしない
  assert.equal(rows[0].price_incl_tax, 1080, 'SubCode の価格 null を親の価格にしていない');
  assert.equal(rows[1].price_incl_tax, 1280);
  // 親コードは mall_item_number に残す (SubCode 行から親をたどれる)
  assert.equal(rows[0].mall_item_number, 'aromainb');
});

t('[!] SubCode の中身が読めない要素は数える (黙って飛ばさない)', () => {
  const { rows, unparsable } = ySnap(yDetail({
    SubCodes: [{ SubCode: '', Price: null }, { SubCode: 'ok-1', Price: null }],
  }));
  assert.equal(rows.length, 1);
  assert.equal(unparsable, 1);
});

t('[!] SubCode がすべて読めない商品は行を作らない (親の行で代用しない)', () => {
  const { rows, unparsable } = ySnap(yDetail({ SubCodes: [{ SubCode: '' }] }));
  assert.equal(rows.length, 0);
  assert.ok(unparsable >= 1);
});

t('[!] SubCodes が配列でなければ解析失敗 (「SubCode 0 件」と混同しない)', () => {
  assert.deepEqual(ySnap(yDetail({ SubCodes: { a: 1 } })), { rows: [], unparsable: 1 });
});

t('[!] 詳細が取れなかった商品 (ok:false) は行を作らず解析失敗に数える', () => {
  assert.deepEqual(ySnap({ ok: false, ItemCode: 'x' }), { rows: [], unparsable: 1 });
  assert.deepEqual(ySnap(null), { rows: [], unparsable: 1 });
  assert.deepEqual(ySnap(yDetail({ ItemCode: '  ' })), { rows: [], unparsable: 1 });
});

t('[!] 価格が読めない商品は not_found (0 円にしない)', () => {
  const r = ySnap(yDetail({ Price: null })).rows[0];
  assert.equal(r.price_incl_tax, null);
  assert.equal(r.fetch_status, 'not_found');
});

t('[!] セール価格が読めない商品は通常価格も使わない (price-update と同じ判断)', () => {
  const r = ySnap(yDetail({ SalePriceReadable: false })).rows[0];
  assert.equal(r.price_incl_tax, null);
  assert.equal(r.fetch_status, 'sale_price_unreadable');
});

t('[!] セールが設定されていても通常価格で計算し、設定があることは残す (§3.2・楽天と同じ)', () => {
  const r = ySnap(yDetail({ SalePrice: 880 })).rows[0];
  assert.equal(r.price_incl_tax, 1080, 'セール価格を採用してしまっている');
  assert.equal(r.price_type, 'normal_sale_set');
  assert.equal(ySnap(yDetail()).rows[0].price_type, 'normal');
});

t('[!] 送料設定の番号を画面に出せる形で残す (送料無料でない出品が出たときに何番か分かる)', () => {
  assert.equal(ySnap(yDetail({ PostageSet: '6' })).rows[0].shipping_group, '送料設定6');
  assert.equal(ySnap(yDetail({ PostageSet: null })).rows[0].shipping_group, null);
});

t('[!] 網羅集合は RYS の正本 (yahoo-store-sync の CANONICAL_QUERIES) と同じ 36 本', () => {
  // 🚨 片方だけ変えると Yahoo の出品を静かに取りこぼす
  assert.equal(YAHOO_QUERIES.length, 36);
  assert.deepEqual([...YAHOO_QUERIES].sort(), [...RYS_CANONICAL_QUERIES].sort());
});

// ── 実行 (deps 差し替え・API は叩かない) ──
const yPages = (map) => async function* (q) {
  const items = map[q] || [];
  yield { items, totalResultsAvailable: items.length, totalResultsReturned: items.length, firstResultPosition: 1, page: 0, offset: 0 };
};
const yDetails = (byCode) => async (code) => byCode[code] || { ok: false, ItemCode: code };

await ta('[!] Yahoo: 36 本の query を union して詳細を引き、run に記録する', async () => {
  const r = await fetchYahooListings(db, {
    yahooListPage: yPages({ a: [{ ItemCode: 'aaa' }], b: [{ ItemCode: 'bbb' }] }),
    yahooDetail: yDetails({
      aaa: yDetail({ ItemCode: 'aaa', Price: 1000 }),
      bbb: yDetail({ ItemCode: 'bbb', Price: 2000 }),
    }),
    archive: false,
  });
  assert.equal(r.items, 2);
  assert.equal(r.count, 2);
  assert.equal(r.status, 'ok');
  assert.equal(r.detailCalls, 2);
  const run = db.prepare('SELECT * FROM price_fetch_run WHERE run_id = ?').get(r.runId);
  assert.equal(run.listing_enum_status, 'ok');
  const rows = db.prepare('SELECT mall_item_key, price_incl_tax FROM mall_price_snapshot WHERE run_id = ? ORDER BY mall_item_key').all(r.runId);
  assert.deepEqual(rows, [{ mall_item_key: 'aaa', price_incl_tax: 1000 }, { mall_item_key: 'bbb', price_incl_tax: 2000 }]);
});

await ta('[!] Yahoo: モールが言った件数と受け取った件数が違えば partial (取りこぼしを隠さない)', async () => {
  const r = await fetchYahooListings(db, {
    yahooListPage: async function* (q) {
      const items = q === 'a' ? [{ ItemCode: 'aaa' }, { ItemCode: 'bbb' }] : [];
      // 🚨 「3 件ある」と言われたのに 2 件しか返ってこない
      yield { items, totalResultsAvailable: q === 'a' ? 3 : 0, totalResultsReturned: items.length, firstResultPosition: 1, page: 0, offset: 0 };
    },
    yahooDetail: yDetails({ aaa: yDetail({ ItemCode: 'aaa' }), bbb: yDetail({ ItemCode: 'bbb' }) }),
    archive: false,
  });
  assert.equal(r.status, 'partial');
  assert.equal(r.incompleteQueries, 1);
});

await ta('[!] Yahoo: 詳細が 1 件でも取れなければ partial (欠けた集合を完全集合にしない)', async () => {
  const r = await fetchYahooListings(db, {
    yahooListPage: yPages({ a: [{ ItemCode: 'aaa' }, { ItemCode: 'zzz' }] }),
    yahooDetail: yDetails({ aaa: yDetail({ ItemCode: 'aaa' }) }),   // zzz は取れない
    archive: false,
  });
  assert.equal(r.status, 'partial');
  assert.equal(r.unparsable, 1);
  assert.equal(r.count, 1);
});

await ta('[!] Yahoo: 詳細の呼び出しが例外でも夜を落とさず、その 1 件だけ失敗にする', async () => {
  const r = await fetchYahooListings(db, {
    yahooListPage: yPages({ a: [{ ItemCode: 'aaa' }, { ItemCode: 'boom' }] }),
    yahooDetail: async (code) => {
      if (code === 'boom') throw new Error('proxy 503');
      return yDetail({ ItemCode: code });
    },
    archive: false,
  });
  assert.equal(r.count, 1);
  assert.equal(r.unparsable, 1);
  assert.equal(r.status, 'partial');
});

await ta('[!] Yahoo: 出品が 0 件で返ったら失敗にする (0 件を正常として通さない)', async () => {
  const r = await fetchYahooListings(db, { yahooListPage: yPages({}), yahooDetail: yDetails({}), archive: false });
  assert.equal(r.status, 'failed');
  assert.equal(r.reason, 'empty_enumeration');
});

await ta('[!] Yahoo: 期限を過ぎたら詳細の取得を打ち切り、取れていないことを隠さない', async () => {
  const r = await fetchYahooListings(db, {
    deadline: new Date(Date.now() - 1000),           // すでに過ぎている
    yahooListPage: yPages({ a: [{ ItemCode: 'aaa' }] }),
    yahooDetail: yDetails({ aaa: yDetail({ ItemCode: 'aaa' }) }),
    archive: false,
  });


console.log('');
console.log('Yahoo: Codex R1 で見つかった穴');

t('[!] 出品の鍵は共通関数だけで作る (見えない区切り文字を写し間違えない)', () => {
  // 🚨 Yahoo を足したとき、既存行を目で写して区切りの U+001F が落ち、
  //    2 回目以降が必ず partial になった。4 か所すべてが同じ関数を通ることを固定する
  const k = snapshotKey('shop', 'item');
  assert.equal(k, 'shop' + String.fromCharCode(0x1f) + 'item');
  assert.notEqual(k, 'shopitem');
  const src = fs.readFileSync(new URL('./fetch-listings.js', import.meta.url), 'utf8');
  const raw = src.match(/new Set\(rows\.map\(r => `/g) || [];
  assert.equal(raw.length, 0, '鍵をテンプレート文字列で組み立てている場所が残っている');
});

await ta('[!] 同じ集合を 2 回取ったら 2 回とも ok (前回集合と突き合わせられている)', async () => {
  // 🚨 ほかの試験が入れた run と混ざらないよう、この試験だけ別 DB を使う
  //    (混ざると「前回の集合」が別物になり、product の不具合か試験の都合か分からなくなる)
  const fresh = createExpectedProfitSchema(new Database(path.join(process.env.DATA_DIR, 'twice.db')));
  try {
    const pages = yPages({ a: [{ ItemCode: 'twice-1' }] });
    const detail = yDetails({ 'twice-1': yDetail({ ItemCode: 'twice-1' }) });
    const a = await fetchYahooListings(fresh, { yahooListPage: pages, yahooDetail: detail, archive: false });
    const b = await fetchYahooListings(fresh, { yahooListPage: pages, yahooDetail: detail, archive: false });
    assert.equal(a.status, 'ok', '1 回目が ok でない');
    assert.equal(b.status, 'ok', `2 回目が ${b.status} になった (消えた ${b.disappeared} 件)`);
    assert.equal(b.disappeared, 0);
  } finally { fresh.close(); }
});

await ta('[!] 同じ商品を 2 回返されたら partial (ユニーク数で取りこぼしを隠さない)', async () => {
  const r = await fetchYahooListings(db, {
    // 「2 件ある」と言いながら同じ商品を 2 回返す = 1 件しか取れていない
    yahooListPage: async function* (q) {
      const items = q === 'a' ? [{ ItemCode: 'dup-1' }, { ItemCode: 'dup-1' }] : [];
      yield { items, totalResultsAvailable: q === 'a' ? 2 : 0, totalResultsReturned: items.length, firstResultPosition: 1, page: 0, offset: 0 };
    },
    yahooDetail: yDetails({ 'dup-1': yDetail({ ItemCode: 'dup-1' }) }),
    archive: false,
  });
  assert.equal(r.status, 'partial', '重複で件数が合ってしまい ok になっている');
  assert.ok(r.incompleteQueries >= 1);
});

await ta('[!] 一覧が途中の query で落ちても、そこまで集めた商品は捨てない (partial として進む)', async () => {
  const r = await fetchYahooListings(db, {
    yahooListPage: async function* (q) {
      if (q === 'a') { yield { items: [{ ItemCode: 'keep-1' }], totalResultsAvailable: 1, totalResultsReturned: 1, firstResultPosition: 1, page: 0, offset: 0 }; return; }
      if (q === 'b') throw new Error('proxy 503');
      yield { items: [], totalResultsAvailable: 0, totalResultsReturned: 0, firstResultPosition: 1, page: 0, offset: 0 };
    },
    yahooDetail: yDetails({ 'keep-1': yDetail({ ItemCode: 'keep-1' }) }),
    archive: false,
  });
  assert.equal(r.count, 1, '落ちた query のせいで取れた分まで捨てている');
  assert.equal(r.status, 'partial');
});

await ta('[!] 一覧のページの切れ目でも期限を見る (1 query が期限をまたいで回り続けない)', async () => {
  let pagesServed = 0;
  const deadline = new Date(Date.now() + 50);
  const r = await fetchYahooListings(db, {
    deadline,
    yahooListPage: async function* (q) {
      if (q !== 'a') { yield { items: [], totalResultsAvailable: 0, totalResultsReturned: 0, firstResultPosition: 1, page: 0, offset: 0 }; return; }
      for (let i = 0; i < 5; i++) {
        pagesServed++;
        await new Promise(res => setTimeout(res, 40));
        yield { items: [{ ItemCode: `page-${i}` }], totalResultsAvailable: 5, totalResultsReturned: 1, firstResultPosition: i + 1, page: i, offset: i };
      }
    },
    yahooDetail: yDetails({}),
    archive: false,
  });
  assert.ok(pagesServed < 5, `期限を過ぎてもページを取り続けた (${pagesServed} ページ)`);
  assert.equal(r.deadlineHit, true);
});

await ta('[!] 詳細が全滅した夜は、履歴も件数も「取れなかった」と言う', async () => {
  let archived = null;
  const r = await fetchYahooListings(db, {
    yahooListPage: yPages({ a: [{ ItemCode: 'gone-1' }, { ItemCode: 'gone-2' }] }),
    yahooDetail: async () => { throw new Error('proxy down'); },
    archive: async (args) => { archived = args; return { code: 'ok', action: 'saved' }; },
  });
  assert.equal(r.status, 'failed');           // 1 行も作れていない = 0 件を通さない
  assert.equal(r.detailFailed, 2);
  assert.equal(archived.meta.complete, false, '詳細が全滅したのに complete: true になっている');
  // 🚨 内訳は details に入れる (manifest は既定の項目しか残さない — Codex R2 P2)
  assert.equal(archived.meta.details.items_enumerated, 2);
  assert.equal(archived.meta.details.detail_failed, 2);
  assert.deepEqual(archived.meta.details.failed_items, ['gone-1', 'gone-2']);
  const run = db.prepare('SELECT expected_count, failed_count FROM price_fetch_run WHERE run_id = ?').get(r.runId);
  assert.equal(run.expected_count, 2, '列挙できた商品数が記録されていない');
  assert.equal(run.failed_count, 2, '取れなかった件数が 0 のままになっている');
});

await ta('[!] 期限で詳細を聞けなかった商品も「取れなかった」に数える', async () => {
  let archived = null;
  const r = await fetchYahooListings(db, {
    deadline: new Date(Date.now() + 30),
    yahooListPage: yPages({ a: [{ ItemCode: 'slow-1' }, { ItemCode: 'slow-2' }] }),
    yahooDetail: async (code) => { await new Promise(res => setTimeout(res, 60)); return yDetail({ ItemCode: code }); },
    yahooConcurrency: 1,
    archive: async (args) => { archived = args; return { code: 'ok', action: 'saved' }; },
  });
  assert.ok(r.notAsked >= 1, '聞けていない商品を数えていない');
  assert.equal(archived.meta.complete, false);
});

await ta('[!] SubCode が一部だけ壊れた商品も「取れなかった」に数える (行があるから ok にしない)', async () => {
  // 🚨 「行が 1 つでもできたか」で数えると、SubCode 3 つのうち 1 つ壊れた商品が
  //    「取れた」に数えられ、履歴も件数も何も問題が無かったように見える (Codex R2 P2)
  let archived = null;
  const r = await fetchYahooListings(db, {
    yahooListPage: yPages({ a: [{ ItemCode: 'half-1' }] }),
    yahooDetail: yDetails({ 'half-1': yDetail({
      ItemCode: 'half-1', SubCodes: [{ SubCode: 'ok-1', Price: null }, { SubCode: 'bad-1' }],
    }) }),
    archive: async (args) => { archived = args; return { code: 'ok', action: 'saved' }; },
  });
  assert.equal(r.count, 1, '読めた SubCode の行は残す');
  assert.equal(r.unparsable, 1);
  assert.equal(archived.meta.complete, false, '一部が壊れているのに complete: true になっている');
  assert.equal(archived.meta.details.detail_failed, 1);
  assert.ok(String(archived.meta.details.failed_items[0]).startsWith('half-1'), archived.meta.details.failed_items[0]);
  const run = db.prepare('SELECT failed_count FROM price_fetch_run WHERE run_id = ?').get(r.runId);
  assert.ok(run.failed_count >= 1, '取れなかった件数が 0 のままになっている');
});

t('[!] 実クライアントが SubCodes を undefined に均しても、親 1 行に化けない', () => {
  // 🚨 RYS の詳細クライアントは非配列の SubCodes を undefined にする。
  //    「配列でなければ解析失敗」にしないと、子商品の価格・原価が親に置き換わる (Codex R1 P1)
  assert.deepEqual(ySnap(yDetail({ SubCodes: undefined })), { rows: [], unparsable: 1 });
  const { SubCodes, ...noKey } = yDetail();
  assert.deepEqual(ySnap(noKey), { rows: [], unparsable: 1 });
  // SubCode を持たない商品は空配列で返る (実測)。これは正常
  assert.equal(ySnap(yDetail({ SubCodes: [] })).rows.length, 1);
});

t('[!] SubCode に Price のキーが無い応答は、親の価格で埋めない', () => {
  // 明示的な null だけが「親と同額」。キーごと無いのは応答の形が変わった証拠
  const { rows, unparsable } = ySnap(yDetail({ SubCodes: [{ SubCode: 'a-1' }, { SubCode: 'a-2', Price: null }] }));
  assert.equal(unparsable, 1);
  assert.deepEqual(rows.map(r => r.mall_item_key), ['aromainb/a-2']);
  assert.equal(rows[0].price_incl_tax, 1080);
});

t('[!] SubCodes の要素が object でなければ数える (黙って飛ばさない)', () => {
  const { rows, unparsable } = ySnap(yDetail({ SubCodes: ['a-1', { SubCode: 'a-2', Price: null }] }));
  assert.equal(unparsable, 1);
  assert.equal(rows.length, 1);
});


console.log('');
console.log('au PAY マーケット (2026-09-19 中原さん「auPAY もやってよ」)');

const auSnap = (item, choices) => aupayItemToSnapshotsDetailed(item, choices, meta);
const auItem = (over = {}) => ({
  itemCode: 'konaicleaner12-2', itemName: 'テスト', itemPrice: '2298',
  taxSegment: '1', postageSegment: '2', postage: '',
  deliveryMethodName: '追跡可能メール便', ...over,
});

t('[!] 送料込みの判定: postageSegment=2 だけを送料無料とみなす (知らない値は倒さない)', () => {
  assert.equal(aupayPostageIncluded('2'), true);
  assert.equal(aupayPostageIncluded('1'), null);
  assert.equal(aupayPostageIncluded(''), null);
  assert.equal(aupayPostageIncluded(null), null);
});

t('[!] 送料無料なら収入 0、それ以外は「不明」(0 円と書かない)', () => {
  const free = auSnap(auItem(), null).rows[0];
  assert.equal(free.postage_included, 1);
  assert.equal(free.postage_revenue_incl_tax, 0);
  const other = auSnap(auItem({ postageSegment: '1' }), null).rows[0];
  assert.equal(other.postage_included, null);
  assert.equal(other.postage_revenue_incl_tax, null, '不明な送料区分を 0 円にしている');
});

t('[!] カラバリが無い商品は 1 行。鍵は商品コードそのもの', () => {
  const { rows, unparsable } = auSnap(auItem(), null);
  assert.equal(unparsable, 0);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].mall_item_key, 'konaicleaner12-2');
  assert.equal(rows[0].mall, 'aupay');
  assert.equal(rows[0].fulfillment, 'self');
  assert.equal(rows[0].price_incl_tax, 2298);
  assert.equal(rows[0].price_tax_included, 1, 'au PAY の価格は税込');
  assert.equal(rows[0].mall_tax_rate, null, '税率は返らない (NE 側を使う)');
  assert.equal(rows[0].shipping_group, '追跡可能メール便');
  assert.equal(rows[0].fetch_status, 'ok');
});

t('[!] カラバリがある商品は子コードごとに 1 行。親の行は作らない (別商品の原価で計算しないため)', () => {
  const { rows } = auSnap(auItem({ itemCode: 'nyanmag' }), new Set(['-GR', '-WH']));
  assert.deepEqual(rows.map(r => r.mall_item_key), ['nyanmag/-GR', 'nyanmag/-WH']);
  // au PAY は商品に 1 つの価格しか持たない (カラバリは在庫だけ) ので、子も親の価格
  assert.equal(rows[0].price_incl_tax, 2298);
  assert.equal(rows[0].mall_item_number, 'nyanmag', '親コードをたどれない');
});

t('[!] 価格が読めない商品は not_found (0 円にしない)', () => {
  const r = auSnap(auItem({ itemPrice: '' }), null).rows[0];
  assert.equal(r.price_incl_tax, null);
  assert.equal(r.fetch_status, 'not_found');
});

t('[!] 商品コードが読めない応答は解析失敗 (行を作らない)', () => {
  assert.deepEqual(auSnap(auItem({ itemCode: '  ' }), null), { rows: [], unparsable: 1 });
  assert.deepEqual(auSnap(null, null), { rows: [], unparsable: 1 });
});

t('[!] 子コードが空の要素は数える。全部空なら親の行で代用しない', () => {
  const partial = auSnap(auItem(), new Set(['', '-GR']));
  assert.equal(partial.rows.length, 1);
  assert.equal(partial.unparsable, 1);
  const allBad = auSnap(auItem(), new Set(['']));
  assert.equal(allBad.rows.length, 0);
  assert.ok(allBad.unparsable >= 1);
});

console.log('');
console.log('au PAY の応答 (XML) の読み取り');

const ITEMS_XML = `<?xml version="1.0" encoding="UTF-8"?><response><result><status>0</status></result>
<searchResult><maxCount>2</maxCount><resultCount>2</resultCount><startCount>1</startCount>
<resultItems><itemCode>a1</itemCode><itemName>商品A</itemName><itemPrice>1000</itemPrice>
<taxSegment>1</taxSegment><postageSegment>2</postageSegment><postage></postage>
<deliveryMethod><deliveryMethodName>追跡可能メール便</deliveryMethodName></deliveryMethod></resultItems>
<resultItems><itemCode>a2</itemCode><itemPrice>2000</itemPrice><postageSegment>1</postageSegment></resultItems>
</searchResult></response>`;

const STOCKS_XML = `<?xml version="1.0" encoding="UTF-8"?><response><result><status>0</status></result>
<searchResult><maxCount>2</maxCount>
<resultStocks><itemCode>a1</itemCode>
<choicesStocks><choicesStockHorizontalCode>-</choicesStockHorizontalCode><choicesStockVerticalCode>BK</choicesStockVerticalCode></choicesStocks>
<choicesStocks><choicesStockHorizontalCode>-</choicesStockHorizontalCode><choicesStockVerticalCode>WH</choicesStockVerticalCode></choicesStocks>
</resultStocks>
<resultStocks><itemCode>a2</itemCode></resultStocks>
</searchResult></response>`;

t('[!] 商品一覧の XML から価格・税区分・送料区分・配送方法を読む', () => {
  const r = parseAupayItemsXml(ITEMS_XML);
  assert.equal(r.maxCount, 2);
  assert.equal(r.rows.length, 2);
  assert.equal(r.rows[0].itemCode, 'a1');
  assert.equal(r.rows[0].itemPrice, '1000');
  assert.equal(r.rows[0].postageSegment, '2');
  assert.equal(r.rows[0].deliveryMethodName, '追跡可能メール便');
  assert.equal(r.rows[1].deliveryMethodName, null, '配送方法が無い商品を空文字にしている');
});

t('[!] 在庫一覧の XML からカラバリの子コードを読む (「-」と空はカラバリではない)', () => {
  const r = parseAupayStocksXml(STOCKS_XML);
  assert.equal(r.maxCount, 2);
  assert.deepEqual([...r.rows[0].choices], ['BK', 'WH']);
  assert.equal(r.rows[1].choices.size, 0, 'カラバリの無い商品に子コードを作っている');
});

t('[!] 縦横の両方に実体があれば、つないだものが子コードになる', () => {
  const xml = STOCKS_XML.replace('<choicesStockHorizontalCode>-</choicesStockHorizontalCode><choicesStockVerticalCode>BK</choicesStockVerticalCode>',
    '<choicesStockHorizontalCode>-L</choicesStockHorizontalCode><choicesStockVerticalCode>BK</choicesStockVerticalCode>');
  assert.ok([...parseAupayStocksXml(xml).rows[0].choices].includes('-LBK'));
});

t('[!] au PAY は失敗も HTTP 200 + status≠0 で返す。status を先に見る', () => {
  const ng = `<response><result><status>1</status><error><code>E001</code><message>だめ</message></error></result></response>`;
  assert.throws(() => parseAupayItemsXml(ng), /au PAY がエラーを返しました/);
  assert.throws(() => parseAupayItemsXml('<response></response>'), /response がありません/);
});



console.log('');
console.log('au PAY の取得 (deps 差し替え・API は叩かない)');

/** 一覧 API の差し替え。ページングと maxCount の申告をまねる */
const auPages = (rows, { maxCount, pageSize = 500 } = {}) => async ({ startCount, totalCount }) => ({
  maxCount: maxCount ?? rows.length,
  rows: rows.slice(startCount - 1, startCount - 1 + Math.min(totalCount, pageSize)),
});
const auStocks = (map) => auPages(Object.entries(map).map(([itemCode, choices]) => ({ itemCode, choices: new Set(choices) })));

await ta('[!] au PAY: 商品とカラバリを突き合わせて行を作る', async () => {
  const r = await fetchAupayListings(db, {
    aupayPageSize: 2,
    aupayItemPage: auPages([auItem({ itemCode: 'solo' }), auItem({ itemCode: 'vari' })]),
    aupayStockPage: auStocks({ solo: [], vari: ['-GR', '-WH'] }),
    archive: false,
  });
  assert.equal(r.status, 'ok');
  assert.equal(r.items, 2);
  assert.equal(r.itemsWithChoices, 1);
  assert.equal(r.count, 3, 'カラバリ 2 + 単独 1 = 3 行にならない');
  const keys = db.prepare('SELECT mall_item_key FROM mall_price_snapshot WHERE run_id = ? ORDER BY mall_item_key').all(r.runId);
  assert.deepEqual(keys.map(k => k.mall_item_key), ['solo', 'vari/-GR', 'vari/-WH']);
});

await ta('[!] au PAY: 同じ集合を 2 回取ったら 2 回とも ok', async () => {
  const fresh = createExpectedProfitSchema(new Database(path.join(process.env.DATA_DIR, 'au-twice.db')));
  try {
    const deps = {
      aupayPageSize: 10,
      aupayItemPage: auPages([auItem({ itemCode: 'twice' })]),
      aupayStockPage: auStocks({ twice: [] }),
      archive: false,
    };
    const a = await fetchAupayListings(fresh, deps);
    const b = await fetchAupayListings(fresh, deps);
    assert.equal(a.status, 'ok');
    assert.equal(b.status, 'ok', `2 回目が ${b.status} (消えた ${b.disappeared} 件)`);
  } finally { fresh.close(); }
});

await ta('[!] au PAY: モールが言った件数と受け取った件数が違えば partial', async () => {
  const r = await fetchAupayListings(db, {
    aupayPageSize: 10,
    // 「3 件ある」と言いながら 2 件しか返さない
    aupayItemPage: auPages([auItem({ itemCode: 'x1' }), auItem({ itemCode: 'x2' })], { maxCount: 3 }),
    aupayStockPage: auStocks({ x1: [], x2: [] }),
    archive: false,
  });
  assert.equal(r.status, 'partial');
  assert.ok(r.problems >= 1);
});

await ta('[!] au PAY: 在庫の一覧が取れない夜は 1 行も作らない (カラバリを親 1 行に化けさせない)', async () => {
  // 🚨 ここが効いていないと、カラバリ商品が親 1 行になって**別商品の原価**で計算される
  let archived = null;
  const r = await fetchAupayListings(db, {
    aupayPageSize: 10,
    aupayItemPage: auPages([auItem({ itemCode: 'vari' })]),
    aupayStockPage: async () => { throw new Error('proxy 503'); },
    archive: async (args) => { archived = args; return { code: 'ok', action: 'saved' }; },
  });
  assert.equal(r.count, 0, '在庫が取れていないのに行を作っている');
  assert.equal(r.stocksOk, false);
  assert.equal(r.status, 'failed');
  assert.equal(archived.meta.complete, false);
});

await ta('[!] au PAY: 出品が 0 件で返ったら失敗にする (0 件を正常として通さない)', async () => {
  const r = await fetchAupayListings(db, {
    aupayPageSize: 10, aupayItemPage: auPages([]), aupayStockPage: auStocks({}), archive: false,
  });
  assert.equal(r.status, 'failed');
  assert.equal(r.reason, 'empty_enumeration');
});

await ta('[!] au PAY: 期限を過ぎたら取りに行かない', async () => {
  let called = 0;
  const r = await fetchAupayListings(db, {
    deadline: new Date(Date.now() - 1000),
    aupayPageSize: 10,
    aupayItemPage: async (a) => { called++; return auPages([auItem()])(a); },
    aupayStockPage: auStocks({}),
    archive: false,
  });
  assert.equal(called, 0, '期限を過ぎているのに一覧を叩いた');
  assert.equal(r.deadlineHit, true);
});

await ta('[!] au PAY: 履歴には商品数・カラバリ数・取りこぼしの内訳を残す', async () => {
  let archived = null;
  await fetchAupayListings(db, {
    aupayPageSize: 10,
    aupayItemPage: auPages([auItem({ itemCode: 'h1' }), auItem({ itemCode: 'h2' })]),
    aupayStockPage: auStocks({ h1: ['-A'], h2: [] }),
    archive: async (args) => { archived = args; return { code: 'ok', action: 'saved' }; },
  });
  assert.equal(archived.meta.details.items_enumerated, 2);
  assert.equal(archived.meta.details.items_with_choices, 1);
  assert.equal(archived.meta.details.rows, 2);
  assert.equal(archived.meta.complete, true);
});


console.log('');
console.log('au PAY: Codex R1 で見つかった穴');

t('[!] 税区分が確かめられない出品は価格を採らない (税を二重に割り戻さない)', () => {
  for (const seg of [null, '', '2', 'x']) {
    const r = auSnap(auItem({ taxSegment: seg }), null).rows[0];
    assert.equal(r.price_incl_tax, null, `taxSegment=${seg} を税込として通している`);
    assert.equal(r.price_tax_included, null);
    assert.equal(r.fetch_status, 'tax_segment_unknown');
  }
  assert.equal(auSnap(auItem({ taxSegment: '1' }), null).rows[0].fetch_status, 'ok');
});

t('[!] XML: 子要素を持つ値を空文字に均さない (壊れた子コードがカラバリなしに化けない)', () => {
  // 🚨 xml2js は空要素を '' で返すが、子要素を持つものは object になる。
  //    object を '' に均すと「カラバリなし」= 親 1 行 = 別商品の原価になる (Codex R1 P1)
  const broken = `<response><result><status>0</status></result><searchResult><maxCount>1</maxCount>
    <resultStocks><itemCode>a1</itemCode>
    <choicesStocks><choicesStockHorizontalCode><nested>x</nested></choicesStockHorizontalCode>
    <choicesStockVerticalCode>BK</choicesStockVerticalCode></choicesStocks>
    </resultStocks></searchResult></response>`;
  const r = parseAupayStocksXml(broken);
  assert.equal(r.rows[0].broken, true, '壊れた子コードを読めたことにしている');
  assert.equal(r.rows[0].choices.size, 0);
});

t('[!] XML: 商品側も読めない値があれば broken にする', () => {
  const broken = `<response><result><status>0</status></result><searchResult><maxCount>1</maxCount>
    <resultItems><itemCode>a1</itemCode><itemPrice><nested>1</nested></itemPrice>
    <taxSegment>1</taxSegment><postageSegment>2</postageSegment></resultItems></searchResult></response>`;
  assert.equal(parseAupayItemsXml(broken).rows[0].broken, true);
});

t('[!] XML: 総件数が読めなければ null (Number("") の 0 にしない)', () => {
  const noMax = `<response><result><status>0</status></result><searchResult><maxCount></maxCount></searchResult></response>`;
  assert.equal(parseAupayItemsXml(noMax).maxCount, null);
  const noTag = `<response><result><status>0</status></result><searchResult></searchResult></response>`;
  assert.equal(parseAupayStocksXml(noTag).maxCount, null);
});

await ta('[!] 在庫の一覧に出てこなかった商品は、カラバリなしと決めない', async () => {
  // 🚨 これが効いていないと、在庫側が取りこぼした商品が親 1 行になって別商品の原価で計算される
  const r = await fetchAupayListings(db, {
    aupayPageSize: 10,
    aupayItemPage: auPages([auItem({ itemCode: 'seen' }), auItem({ itemCode: 'unseen' })]),
    // 在庫側は 2 件あると言いながら 1 件しか返さない
    aupayStockPage: auPages([{ itemCode: 'seen', choices: new Set() }], { maxCount: 2 }),
    archive: false,
  });
  assert.equal(r.count, 0, '在庫の一覧が欠けているのに行を作っている');
  assert.equal(r.status, 'failed');
});

await ta('[!] 在庫の一覧が 0 件で返った夜も 1 行も作らない', async () => {
  const r = await fetchAupayListings(db, {
    aupayPageSize: 10,
    aupayItemPage: auPages([auItem({ itemCode: 'parent' })]),
    aupayStockPage: auPages([]),
    archive: false,
  });
  assert.equal(r.count, 0, '在庫が 0 件なのに親の行を作っている');
  // 在庫側は「0 件」を最後まで取れてはいる。止めているのは
  // 「在庫の一覧に出てこなかった商品をカラバリなしと決めない」ガードのほう
  assert.equal(r.status, 'failed');
  assert.ok(r.unparsable >= 1, '在庫に出てこない商品を数えていない');
});

await ta('[!] 在庫の一覧に壊れた行があれば、その夜は行を作らない', async () => {
  const r = await fetchAupayListings(db, {
    aupayPageSize: 10,
    aupayItemPage: auPages([auItem({ itemCode: 'a1' })]),
    aupayStockPage: auPages([{ itemCode: 'a1', choices: new Set(), broken: true }]),
    archive: false,
  });
  assert.equal(r.count, 0);
  assert.equal(r.stocksOk, false);
});

await ta('[!] 途中のページで総件数が変わったら partial', async () => {
  let page = 0;
  const r = await fetchAupayListings(db, {
    aupayPageSize: 2,
    aupayItemPage: async ({ startCount }) => {
      page++;
      const rows = [auItem({ itemCode: `p${startCount}` }), auItem({ itemCode: `p${startCount + 1}` })];
      return { maxCount: page === 1 ? 4 : 5, rows: startCount > 3 ? [] : rows };
    },
    aupayStockPage: auPages([1, 2, 3, 4].map(i => ({ itemCode: `p${i}`, choices: new Set() }))),
    archive: false,
  });
  assert.notEqual(r.status, 'ok', '総件数が変わったのに ok と報告している');
});

await ta('[!] 商品一覧の途中で落ちても、そこまでの商品は捨てない', async () => {
  const r = await fetchAupayListings(db, {
    aupayPageSize: 2,
    aupayItemPage: async ({ startCount }) => {
      if (startCount > 2) throw new Error('proxy 503');
      return { maxCount: 4, rows: [auItem({ itemCode: 'k1' }), auItem({ itemCode: 'k2' })] };
    },
    aupayStockPage: auPages([{ itemCode: 'k1', choices: new Set() }, { itemCode: 'k2', choices: new Set() }]),
    archive: false,
  });
  assert.equal(r.count, 2, '落ちたページのせいで取れた分まで捨てている');
  assert.equal(r.status, 'partial');
});

await ta('[!] 応答が返ったあとにも期限を見る (最後のページが期限をまたいでも ok にしない)', async () => {
  const deadline = new Date(Date.now() + 60);
  const r = await fetchAupayListings(db, {
    deadline, aupayPageSize: 1,
    aupayItemPage: async ({ startCount }) => {
      await new Promise(res => setTimeout(res, 80));
      return { maxCount: 5, rows: [auItem({ itemCode: `d${startCount}` })] };
    },
    aupayStockPage: auPages([]),
    archive: false,
  });
  assert.equal(r.deadlineHit, true, '期限をまたいだのに気づいていない');
  assert.notEqual(r.status, 'ok');
});


console.log('');
console.log('au PAY: Codex R2 で見つかった穴 (実 XML とページ境界を通す)');

/** 実 XML を返す一覧 API。ページ境界と解析を一緒に通す */
const auXmlItems = (pages) => async ({ startCount }) => {
  const p = pages[Math.floor((startCount - 1) / 1)] ?? { maxCount: pages[0].maxCount, items: [] };
  return parseAupayItemsXml(`<response><result><status>0</status></result><searchResult>`
    + `<maxCount>${p.maxCount}</maxCount>`
    + p.items.map(c => `<resultItems><itemCode>${c}</itemCode><itemPrice>1000</itemPrice>`
      + `<taxSegment>1</taxSegment><postageSegment>2</postageSegment></resultItems>`).join('')
    + `</searchResult></response>`);
};
/** 実 XML を返す在庫 API。choices は [[itemCode, [子コード...]], ...] */
const auXmlStocks = (pages) => async ({ startCount }) => {
  const p = pages[Math.floor((startCount - 1) / 1)] ?? { maxCount: pages[0].maxCount, stocks: [] };
  return parseAupayStocksXml(`<response><result><status>0</status></result><searchResult>`
    + `<maxCount>${p.maxCount}</maxCount>`
    + p.stocks.map(([code, kids]) => `<resultStocks><itemCode>${code}</itemCode>`
      + (kids || []).map(k => `<choicesStocks><choicesStockHorizontalCode>-</choicesStockHorizontalCode>`
        + `<choicesStockVerticalCode>${k}</choicesStockVerticalCode></choicesStocks>`).join('')
      + `</resultStocks>`).join('')
    + `</searchResult></response>`);
};

await ta('[!] 在庫で同じ商品が 2 ページに出たら、子コードを上書きせずその夜は行を作らない', async () => {
  // 🚨 上書きすると 1 ページ目の子コードが消える (実 XML + ページ境界でしか出ない — Codex R2 P1)
  const r = await fetchAupayListings(db, {
    aupayPageSize: 1,
    aupayItemPage: auXmlItems([{ maxCount: 1, items: ['a'] }, { maxCount: 1, items: [] }]),
    aupayStockPage: auXmlStocks([
      { maxCount: 2, stocks: [['a', ['BK']]] },
      { maxCount: 2, stocks: [['a', ['WH']]] },
    ]),
    archive: false,
  });
  assert.equal(r.count, 0, '重複した在庫から行を作っている');
  assert.equal(r.stocksOk, false);
});

t('[!] choicesStocks の枠があるのに縦横のタグが無ければ壊れた行 (カラバリなしと区別がつかない)', () => {
  // 🚨 実測 2026-09-19: カラバリの無い商品は枠自体が無く (3,680 件)、
  //    枠がある商品は必ず縦か横のタグを持つ (どちらも無い行は 0 件)
  const xml = `<response><result><status>0</status></result><searchResult><maxCount>1</maxCount>
    <resultStocks><itemCode>a</itemCode><choicesStocks><unexpected>BK</unexpected></choicesStocks></resultStocks>
    </searchResult></response>`;
  const r = parseAupayStocksXml(xml);
  assert.equal(r.rows[0].broken, true, '枠だけの行をカラバリなしにしている');
});

await ta('[!] 枠だけの在庫が来た夜は、親の行を作らない (XML から DB まで通す)', async () => {
  const r = await fetchAupayListings(db, {
    aupayPageSize: 10,
    aupayItemPage: auXmlItems([{ maxCount: 1, items: ['a'] }]),
    aupayStockPage: async () => parseAupayStocksXml(
      `<response><result><status>0</status></result><searchResult><maxCount>1</maxCount>
       <resultStocks><itemCode>a</itemCode><choicesStocks><unexpected>BK</unexpected></choicesStocks></resultStocks>
       </searchResult></response>`),
    archive: false,
  });
  assert.equal(r.count, 0, '枠だけの在庫から親の行を作っている');
  assert.equal(r.stocksOk, false);
});

await ta('[!] 在庫の総件数が途中で変わったら、最後に件数が合っても行を作らない', async () => {
  // 🚨 総件数 3 → 2 に変わり、取れた行数は 2 で「合う」ケース。
  //    complete を立てると、不完全な在庫から行を作ってしまう (Codex R2 P2)
  const r = await fetchAupayListings(db, {
    aupayPageSize: 1,
    aupayItemPage: auXmlItems([{ maxCount: 2, items: ['a'] }, { maxCount: 2, items: ['b'] }, { maxCount: 2, items: [] }]),
    aupayStockPage: auXmlStocks([
      { maxCount: 3, stocks: [['a', []]] },
      { maxCount: 2, stocks: [['b', []]] },
    ]),
    archive: false,
  });
  assert.equal(r.count, 0, '途中で総件数が変わった在庫から行を作っている');
  assert.equal(r.stocksOk, false);
});

await ta('[!] 最後のページが期限をまたいだら ok にしない', async () => {
  // 🚨 完了判定 (最終ページ) が期限確認より先にあると、またいでも ok になる (Codex R2 P2)
  const deadline = new Date(Date.now() + 120);
  const r = await fetchAupayListings(db, {
    deadline, aupayPageSize: 10,
    aupayItemPage: async () => {
      await new Promise(res => setTimeout(res, 200));   // 応答が返った時点で期限を過ぎている
      return parseAupayItemsXml(`<response><result><status>0</status></result><searchResult>`
        + `<maxCount>1</maxCount><resultItems><itemCode>z</itemCode><itemPrice>100</itemPrice>`
        + `<taxSegment>1</taxSegment><postageSegment>2</postageSegment></resultItems></searchResult></response>`);
    },
    aupayStockPage: auXmlStocks([{ maxCount: 0, stocks: [] }]),
    archive: false,
  });
  assert.equal(r.deadlineHit, true, '最終ページで期限をまたいだのに気づいていない');
  assert.notEqual(r.status, 'ok');
});

await ta('[!] 残り時間が通信 1 回ぶんも無ければ、要求そのものを出さない', async () => {
  let called = 0;
  const r = await fetchAupayListings(db, {
    deadline: new Date(Date.now() + 200),   // 1 秒未満
    aupayPageSize: 10,
    aupayItemPage: async () => { called++; return { maxCount: 0, rows: [] }; },
    aupayStockPage: auXmlStocks([{ maxCount: 0, stocks: [] }]),
    archive: false,
  });
  assert.equal(called, 0, `残り 0.2 秒なのに要求を ${called} 回出した`);
  assert.equal(r.deadlineHit, true);
});

db.close();
fs.rmSync(process.env.DATA_DIR, { recursive: true, force: true });

  assert.equal(r.deadlineHit, true);
  assert.equal(r.status, 'failed');                  // 1 件も取れていないので failed (0 件を通さない)
});

console.log(`\n${passed} 件 PASS`);
