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

const { initExpectedProfitDB } = await import('./db.js');
const {
  toIntPrice, amazonListingStatus, amazonRowToSnapshot, rakutenItemToSnapshots,
  evaluateEnumeration, loadLastCompleteKeys, fetchAmazonListings, fetchRakutenListings,
  enumStatusWithParseFailures, amazonFulfillment, rakutenItemToSnapshotsDetailed,
} = await import('./fetch-listings.js');
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
  const deadline = new Date('2026-09-08T06:00:00Z');
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

db.close();
fs.rmSync(process.env.DATA_DIR, { recursive: true, force: true });
console.log(`\n${passed} 件 PASS`);
