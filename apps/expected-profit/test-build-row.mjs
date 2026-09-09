/**
 * test-build-row.mjs — 1出品 → 1行 の組み立て 受入試験
 *
 * Codex R3 が世代ビルダーの受入条件として挙げた3点を中心に固める:
 *   1. 送料収入の「不明」と「送料込み」を区別し、不明はランキングから外す
 *   2. resolveCost → buildProfitInputs → computeProfit を接続。税率不一致も採用状態に反映
 *   3. 期限は保存済みの status を信じず、計算時刻で判定し直す
 *
 * 実行: node apps/expected-profit/test-build-row.mjs
 */
import assert from 'node:assert/strict';
import { buildRow, resolveNeCode, fbmNeCode } from './build-row.js';
import { normalizeQty } from './load-inputs.js';
// 手作りキーだと保存側とのズレを検出できない (Codex R4-2)。本番と同じ関数で作る
import { feeCacheKey } from './calc.js';

let passed = 0;
function t(name, fn) {
  try { fn(); passed++; console.log(`  ok  ${name}`); }
  catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; }
}
const near = (a, b, eps = 1e-6) => Math.abs(a - b) < eps;

const NOW = new Date('2026-09-07T12:00:00Z');
const FUTURE = '2099-01-01T00:00:00Z';
const PAST = '2026-09-01T00:00:00Z';

const NEKOPOSU = { 大分類区分: 'メール便', 小分類区分名称: 'ネコポス', 送料: 198, 出荷作業料: 20, 想定梱包資材費: 10, 想定人件費: 9 };

const baseCtx = (over = {}) => ({
  generationId: 'g1',
  now: NOW,
  codeVersion: 'test',
  sellerId: 'S1',
  marketplaceId: 'M1',
  products: new Map([['ne001', {
    商品コード: 'ne001', 商品名: 'テスト商品', 原価: 600, 原価ソース: 'NE',
    原価状態: 'COMPLETE', 消費税率: 0.1, 送料コード: '501', 配送方法: 'ネコポス', 売上分類: 3,
  }]]),
  shippingRates: new Map([['501', NEKOPOSU]]),
  // 本番と同じ形にする: Amazon の対応表は数量を持つ (v_sku_resolved.数量)。
  // 楽天は数量列が無いので、楽天の試験は qty: null の Map を明示的に渡す
  skuMap: new Map([['sku1', [{ ne_code: 'ne001', qty: 1 }]]]),
  feeEstimates: new Map(),
  masterFreshness: { costValidUntil: FUTURE, shippingMasterValidUntil: FUTURE },
  runInfo: { listingEnumStatus: 'ok', listingEnumValidUntil: FUTURE, priceRunId: 'r1' },
  ...over,
});

const rakutenListing = (over = {}) => ({
  mall: 'rakuten', shop_id: '1', mall_item_key: 'item/sku1', mall_item_ref: 'sku1',
  fulfillment: 'self', price_incl_tax: 1100, price_tax_included: 1, mall_tax_rate: 0.1,
  postage_included: 1, postage_revenue_incl_tax: 0, points: 0,
  listing_status: 'active', fetch_status: 'ok', valid_until: FUTURE, fetched_at: '2026-09-07T00:00:00Z',
  ...over,
});

console.log('通しの計算');

t('楽天・送料込み: 手計算と一致し rank_eligible=1', () => {
  const r = buildRow(rakutenListing(), baseCtx());
  // 税抜1000 − 原価600 − 配送219(180+39) − 手数料100(1100×10%÷1.1) = 81
  assert.equal(r.calculation_status, 'ok');
  assert.ok(near(r.expected_profit, 81), `期待81, 実際 ${r.expected_profit}`);
  assert.ok(near(r.expected_margin_rate, 81 / 1000));
  assert.equal(r.rank_eligible, 1);
  assert.equal(r.shipping_method, 'ネコポス');
  assert.equal(r.cost_method, 'single');
});

t('[!] どの配送方法で計算したかが行に残る (中原さん 2026-09-08)', () => {
  // 送料コードだけでは「501 が何なのか」が画面から読めない。
  // 金額を引いてきた送料マスタの区分名を、行そのものに持たせる
  const r = buildRow(rakutenListing({ shipping_group: null }), baseCtx());
  assert.equal(r.shipping_rate_name, 'ネコポス');
  assert.equal(r.shipping_rate_category, 'メール便');
  assert.equal(r.shipping_code, '501');
});

t('[!] 送料区分が未登録なら、使った配送も空のままにする (推測で埋めない)', () => {
  const ctx = baseCtx();
  ctx.shippingRates = new Map();
  const r = buildRow(rakutenListing(), ctx);
  assert.equal(r.shipping_master_status, 'missing');
  assert.equal(r.shipping_rate_name, null);
  assert.equal(r.shipping_rate_category, null);
});

t('[!] モール側の配送パターンを行に残す (FBM の送料込み判断根拠 §16-13)', () => {
  const r = buildRow(rakutenListing({ shipping_group: 'ネコポスマケプレプライム設定' }), baseCtx());
  assert.equal(r.shipping_group, 'ネコポスマケプレプライム設定');
});

t('input_snapshot に再現用の入力が残る', () => {
  const r = buildRow(rakutenListing(), baseCtx());
  const snap = JSON.parse(r.input_snapshot);
  assert.equal(snap.cost_ex_tax, 600);
  assert.equal(snap.shipping_code, '501');
  assert.deepEqual(snap.shipping_rate, NEKOPOSU);
  assert.equal(snap.tax_rate, 0.1);
});

console.log('\n受入条件1: 送料収入の不明と送料込みを区別する');

t('[!] 送料込み (postage_included=1) は「included」で計算する', () => {
  const r = buildRow(rakutenListing({ postage_included: 1, postage_revenue_incl_tax: 0 }), baseCtx());
  assert.equal(r.shipping_revenue_status, 'included');
  assert.equal(r.rank_eligible, 1);
});

t('[!] 別途徴収で額が取れない出品は unknown → ランキング外 (参考値)', () => {
  const r = buildRow(rakutenListing({ postage_included: 0, postage_revenue_incl_tax: null }), baseCtx());
  assert.equal(r.shipping_revenue_status, 'unknown');
  assert.equal(r.calculation_status, 'incomplete');
  assert.equal(r.rank_eligible, 0);
  assert.equal(r.rank_exclusion_reason, 'shipping_revenue_unknown');
  // 参考値としての利益は出す (0円送料として)
  assert.ok(Number.isFinite(r.expected_profit));
});

t('[!] 扱いが不明なのに金額だけある行を売上に足さない (赤字が黒字に化ける)', () => {
  // Codex R4-1 の例: 足すと -119円 が +181円 になる
  const ctx = baseCtx();
  ctx.products.set('ne001', { ...ctx.products.get('ne001'), 原価: 800 });
  const r = buildRow(rakutenListing({ postage_included: null, postage_revenue_incl_tax: 330 }), ctx);
  assert.equal(r.shipping_revenue_status, 'unknown');
  assert.equal(r.postage_revenue_ex_tax, 0, '扱い不明なのに送料収入を足している');
  assert.equal(r.rank_eligible, 0);
  // 税抜1000 − 原価800 − 配送219 − 手数料100 = -119
  assert.ok(near(r.expected_profit, -119), `期待 -119, 実際 ${r.expected_profit}`);
});

t('[!] 送料の扱いそのものが不明 (postage_included=null) も unknown', () => {
  const r = buildRow(rakutenListing({ postage_included: null, postage_revenue_incl_tax: null }), baseCtx());
  assert.equal(r.shipping_revenue_status, 'unknown');
  assert.equal(r.rank_eligible, 0);
});

t('別途徴収で額が取れれば ok。収入を足したうえで配送費も引く', () => {
  const r = buildRow(rakutenListing({ postage_included: 0, postage_revenue_incl_tax: 330 }), baseCtx());
  assert.equal(r.shipping_revenue_status, 'ok');
  assert.ok(near(r.postage_revenue_ex_tax, 300));
  assert.ok(near(r.revenue_ex_tax, 1300));
  assert.ok(near(r.shipping_total_ex_tax, 219));   // 送料込みでも別途でも配送費は引く
});

console.log('\n受入条件2: 採用・再利用の検証を接続する');

t('[!] 原価が未登録なら計算不能 (0で引かない)', () => {
  const ctx = baseCtx();
  ctx.products.set('ne001', { ...ctx.products.get('ne001'), 原価: 0 });
  const r = buildRow(rakutenListing(), ctx);
  assert.equal(r.calculation_status, 'incomplete');
  assert.equal(r.incomplete_reason, 'cost_missing');
  assert.equal(r.expected_profit, null);
});

t('[!] セットで原価状態が COMPLETE でなければ計算不能', () => {
  const ctx = baseCtx();
  ctx.products.set('ne001', { ...ctx.products.get('ne001'), 原価ソース: 'セット計算', 原価状態: 'PARTIAL' });
  const r = buildRow(rakutenListing(), ctx);
  assert.equal(r.incomplete_reason, 'set_cost_incomplete');
});

t('[!] 商品税率とモール税率が食い違えば tax_mismatch (ランキング外)', () => {
  // モールは8%、マスタは10% → どちらが正しいか決められない
  const r = buildRow(rakutenListing({ mall_tax_rate: 0.08 }), baseCtx());
  assert.equal(r.price_status, 'tax_mismatch');
  assert.equal(r.calculation_status, 'incomplete');
  assert.equal(r.rank_eligible, 0);
  assert.match(r.incomplete_reason, /tax_mismatch/);
});

t('モール税率が無ければ不一致judgeをしない (Amazon)', () => {
  const r = buildRow(rakutenListing({ mall_tax_rate: null }), baseCtx());
  assert.equal(r.price_status, 'ok');
});

t('[!] 1出品が複数のNE商品を指すなら ambiguous (原価構成が決まらない)', () => {
  const ctx = baseCtx({ skuMap: new Map([['sku1', [{ ne_code: 'ne001' }, { ne_code: 'ne002' }]]]) });
  const r = buildRow(rakutenListing(), ctx);
  assert.equal(r.cost_status, 'ambiguous');
  assert.equal(r.incomplete_reason, 'multiple_ne_codes');
  assert.equal(r.rank_eligible, 0);
});

t('対応表に無い出品は unresolved', () => {
  const ctx = baseCtx({ skuMap: new Map() });
  const r = buildRow(rakutenListing(), ctx);
  assert.equal(r.cost_status, 'unresolved');
});

console.log('\n受入条件3: 期限は計算時刻で判定し直す');

t('[!] 保存済みの状態を信じず、価格の期限切れを今の時刻で判定する', () => {
  const r = buildRow(rakutenListing({ valid_until: PAST }), baseCtx());
  assert.equal(r.price_status, 'expired');
  assert.equal(r.rank_eligible, 0);
  assert.equal(r.rank_exclusion_reason, 'price_expired');
});

t('[!] 出品列挙の期限切れもランキングから外す', () => {
  const ctx = baseCtx({ runInfo: { listingEnumStatus: 'ok', listingEnumValidUntil: PAST, priceRunId: 'r1' } });
  const r = buildRow(rakutenListing(), ctx);
  assert.equal(r.listing_enum_status, 'expired');
  assert.equal(r.rank_eligible, 0);
});

t('[!] 原価マスタの期限切れもランキングから外す', () => {
  const ctx = baseCtx({ masterFreshness: { costValidUntil: PAST, shippingMasterValidUntil: FUTURE } });
  const r = buildRow(rakutenListing(), ctx);
  assert.equal(r.cost_status, 'expired');
  assert.equal(r.rank_eligible, 0);
});

t('[!] 配送マスタの期限切れもランキングから外す', () => {
  const ctx = baseCtx({ masterFreshness: { costValidUntil: FUTURE, shippingMasterValidUntil: PAST } });
  const r = buildRow(rakutenListing(), ctx);
  assert.equal(r.shipping_master_status, 'expired');
  assert.equal(r.rank_eligible, 0);
});

console.log('\nAmazon の経路');

const amazonListing = (over = {}) => ({
  mall: 'amazon', shop_id: 'S1@M1', mall_item_key: 'sku1', mall_item_ref: 'B001',
  fulfillment: 'FBA', price_incl_tax: 1980, price_tax_included: 1, mall_tax_rate: null,
  // fetch 層 (amazonRowToSnapshot) が FBA に対して実際に返す値と揃える。
  // ここを実態とズラすと「テストは通るが本番で全件除外される」ことになる (Codex R4-3)
  postage_included: 1, postage_revenue_incl_tax: 0, points: 0,
  listing_status: 'active', fetch_status: 'ok', valid_until: FUTURE, fetched_at: '2026-09-07T00:00:00Z',
  ...over,
});

const feeCache = (over = {}) => new Map([[
  feeCacheKey({ seller_id: 'S1', marketplace_id: 'M1', seller_sku: 'sku1',
    in_listing_price: 1980, in_shipping: 0, in_points: 0, in_fulfillment: 'FBA' }),
  {
    seller_id: 'S1', marketplace_id: 'M1', seller_sku: 'sku1', asin: 'B001',
    in_listing_price: 1980, in_shipping: 0, in_points: 0, in_fulfillment: 'FBA', in_currency: 'JPY',
    referral_fee_ex_tax: 166, closing_fee_ex_tax: 0, per_item_fee_ex_tax: 0,
    fba_fee_incl_tax: 462, fee_status: 'ok', fee_breakdown: '[]',
    fetched_at: '2026-09-06T00:00:00Z', valid_until: FUTURE, ...over,
  },
]]);

t('Amazon FBA: 手計算 614円 と一致し、自社配送費を引かない', () => {
  const r = buildRow(amazonListing(), baseCtx({ feeEstimates: feeCache() }));
  assert.equal(r.calculation_status, 'ok');
  // 税抜1800 − 原価600 − FBA420(462÷1.1) − 手数料166 = 614
  assert.ok(near(r.expected_profit, 614), `期待614, 実際 ${r.expected_profit}`);
  assert.equal(r.shipping_total_ex_tax, 0);
  assert.ok(near(r.fba_fee_ex_tax, 420));
  assert.ok(near(r.fee_total_ex_tax, 166));      // FBA費用を手数料合計に混ぜない
  assert.equal(r.expense_scope_version, 'fba_v1');
});

t('[!] FBA は自社配送区分が無くても計算できる (not_applicable)', () => {
  const ctx = baseCtx({ feeEstimates: feeCache(), shippingRates: new Map() });
  const r = buildRow(amazonListing(), ctx);
  assert.equal(r.shipping_master_status, 'not_applicable');
  assert.equal(r.calculation_status, 'ok');
  assert.equal(r.rank_eligible, 1);
});

t('[!] 手数料見積が無ければ計算不能', () => {
  const r = buildRow(amazonListing(), baseCtx({ feeEstimates: new Map() }));
  assert.equal(r.fee_status, 'missing');
  assert.equal(r.calculation_status, 'incomplete');
});

t('[!] 見積の入力が今の価格と違えば使わない (価格が変われば別の見積)', () => {
  // キャッシュは 1980 円のもの。2200 円の出品には使わない (キーが違うので見つからない)
  const r = buildRow(amazonListing({ price_incl_tax: 2200 }), baseCtx({ feeEstimates: feeCache() }));
  assert.equal(r.fee_status, 'missing');
  assert.equal(r.calculation_status, 'incomplete');
  assert.equal(r.expected_profit, null);
});

t('[!] キーは同じでも ASIN が変わっていれば使わない (PK に無い入力も比較する)', () => {
  const r = buildRow(amazonListing({ mall_item_ref: 'B999' }), baseCtx({ feeEstimates: feeCache() }));
  assert.equal(r.fee_status, 'unusable:input_mismatch:asin');
  assert.equal(r.calculation_status, 'incomplete');
});

t('[!] 期限切れの見積は使わない', () => {
  const r = buildRow(amazonListing(), baseCtx({ feeEstimates: feeCache({ valid_until: PAST }) }));
  assert.equal(r.fee_status, 'unusable:expired');
});

t('[!] 壊れた見積 (inconsistent) は使わない', () => {
  const r = buildRow(amazonListing(), baseCtx({ feeEstimates: feeCache({ fee_status: 'inconsistent' }) }));
  assert.match(r.fee_status, /unusable:fee_status_inconsistent/);
});

t('[!] fulfillment が未解決の Amazon 出品は計算しない', () => {
  const r = buildRow(amazonListing({ fulfillment: null }), baseCtx({ feeEstimates: feeCache() }));
  assert.equal(r.incomplete_reason, 'fulfillment_unresolved');
  assert.equal(r.rank_eligible, 0);
});

console.log('\nランキングの適格判定');

t('[!] Inactive は計算するが既定ランキング外', () => {
  const r = buildRow(rakutenListing({ listing_status: 'inactive' }), baseCtx());
  assert.equal(r.calculation_status, 'ok');       // 計算はする
  assert.ok(Number.isFinite(r.expected_profit));
  assert.equal(r.rank_eligible, 0);               // ランキングには載せない
  assert.equal(r.rank_exclusion_reason, 'listing_inactive');
});

t('楽天の hidden もランキング外', () => {
  const r = buildRow(rakutenListing({ listing_status: 'hidden' }), baseCtx());
  assert.equal(r.rank_eligible, 0);
});

t('[!] FBA と自社配送で費用範囲の版が分かれる', () => {
  const fba = buildRow(amazonListing(), baseCtx({ feeEstimates: feeCache() }));
  const self = buildRow(rakutenListing(), baseCtx());
  assert.equal(fba.expense_scope_version, 'fba_v1');
  assert.equal(self.expense_scope_version, 'self_v1');
});

console.log('\n対応付け');

t('resolveNeCode: 1対1 なら ok', () => {
  const r = resolveNeCode({ mall: 'amazon', mall_item_key: 'SKU1' }, new Map([['sku1', [{ ne_code: 'ne001' }]]]));
  assert.equal(r.status, 'ok');
  assert.equal(r.neCode, 'ne001');
});

t('resolveNeCode: 1対多 は ambiguous', () => {
  const r = resolveNeCode({ mall: 'amazon', mall_item_key: 'sku1' },
    new Map([['sku1', [{ ne_code: 'a' }, { ne_code: 'b' }]]]));
  assert.equal(r.status, 'ambiguous');
});

t('resolveNeCode: 見つからなければ unresolved', () => {
  const r = resolveNeCode({ mall: 'amazon', mall_item_key: 'x' }, new Map());
  assert.equal(r.status, 'unresolved');
});


console.log('');
console.log('まとめ買いSKU の数量 (実データで判明: v_sku_resolved.数量 を読み捨てていた)');

t('[!] 数量12 のSKUは 原価 × 12 になる', () => {
  // 実データ: opbs454 (有機ピーナッツバター 454g) が 数量12。
  // 単品原価 1,001 のまま計算していたので利益率 79.4% と出ていた
  const ctx = baseCtx({ skuMap: new Map([['sku1', [{ ne_code: 'ne001', qty: 12 }]]]) });
  const r = buildRow(amazonListing(), ctx);
  assert.equal(r.unit_quantity, 12);
  assert.ok(near(r.cost_ex_tax, 600 * 12), `期待 7200, 実際 ${r.cost_ex_tax}`);
});

t('[!] 利益も数量倍した原価で計算される (表示だけ直っていて利益が古い、を防ぐ)', () => {
  // 🚨 cost_ex_tax の列だけ見るテストでは、buildProfitInputs に単品原価を
  //    渡したままでも通ってしまう。実データで実際にそうなった (2026-09-07)
  const ctx = baseCtx({ skuMap: new Map([['sku1', [{ ne_code: 'ne001', qty: 12 }]]]), feeEstimates: feeCache() });
  const r12 = buildRow(amazonListing(), ctx);
  const r1 = buildRow(amazonListing(), baseCtx({ feeEstimates: feeCache() }));
  assert.equal(r12.calculation_status, 'ok');
  // 原価が 600 → 7,200 に増えた分、そのまま利益が減る
  assert.ok(near(r12.expected_profit, r1.expected_profit - 600 * 11),
    `期待 ${r1.expected_profit - 6600}, 実際 ${r12.expected_profit}`);
  assert.ok(r12.expected_margin_rate < r1.expected_margin_rate);
  // 内訳と結果が合っていること (validateGeneration と同じ検算)
  assert.ok(near(r12.expected_profit,
    r12.revenue_ex_tax - r12.cost_ex_tax - (r12.shipping_total_ex_tax || 0)
    - (r12.fba_fee_ex_tax || 0) - (r12.fee_total_ex_tax || 0)));
});

t('input_snapshot に単品原価と数量が残る (あとから検算できる)', () => {
  const ctx = baseCtx({ skuMap: new Map([['sku1', [{ ne_code: 'ne001', qty: 12 }]]]), feeEstimates: feeCache() });
  const snap = JSON.parse(buildRow(amazonListing(), ctx).input_snapshot);
  assert.equal(snap.unit_cost_ex_tax, 600);
  assert.equal(snap.unit_quantity, 12);
  assert.equal(snap.cost_ex_tax, 7200);
});

t('数量1 なら原価はそのまま', () => {
  const ctx = baseCtx({ skuMap: new Map([['sku1', [{ ne_code: 'ne001', qty: 1 }]]]) });
  const r = buildRow(amazonListing(), ctx);
  assert.equal(r.unit_quantity, 1);
  assert.ok(near(r.cost_ex_tax, 600));
});

t('[!] 数量が分からない (楽天) ときは単品として計算する', () => {
  // 楽天の対応表には数量列が無い。まとめ買いは価格の開きで別途外す
  const r = buildRow(rakutenListing(), baseCtx({ skuMap: new Map([['sku1', [{ ne_code: 'ne001', qty: null }]]]) }));
  assert.equal(r.unit_quantity, null);
  assert.ok(near(r.cost_ex_tax, 600));
});

t('[!] 数量12 の FBA はランキングに載る (SP-API が実SKUで見積もるので送料の問題が無い)', () => {
  const ctx = baseCtx({ skuMap: new Map([['sku1', [{ ne_code: 'ne001', qty: 12 }]]]), feeEstimates: feeCache() });
  const r = buildRow(amazonListing(), ctx);
  assert.equal(r.calculation_status, 'ok');
  assert.equal(r.rank_eligible, 1);
});

t('[!] 数量12 の自社配送はランキングから外す (送料マスタが単品1個ぶんのため)', () => {
  // 実データ: 数量100 のカミソリが「長3封筒」区分だった
  const ctx = baseCtx({ skuMap: new Map([['sku1', [{ ne_code: 'ne001', qty: 12 }]]]) });
  const r = buildRow(rakutenListing(), ctx);
  assert.equal(r.calculation_status, 'ok', '計算はする (参考値として見える)');
  assert.equal(r.rank_eligible, 0);
  assert.equal(r.rank_exclusion_reason, 'quantity_shipping_unknown');
});

t('normalizeQty: 1以上の整数だけ採用する (0 や小数や null は数量不明)', () => {
  assert.equal(normalizeQty(12), 12);
  assert.equal(normalizeQty('3'), 3);
  assert.equal(normalizeQty(1), 1);
  assert.equal(normalizeQty(0), null);
  assert.equal(normalizeQty(-2), null);
  assert.equal(normalizeQty(1.5), null);
  assert.equal(normalizeQty(null), null);
  assert.equal(normalizeQty('あ'), null);
});

console.log('\n楽天の紐づけ: システム連携用SKU番号 → 空欄なら商品番号 (中原さん 2026-09-09)');

// 🚨 実物の形。mall_item_key = 商品管理番号/SKU管理番号、mall_item_ref = システム連携用SKU番号
const rakutenKeys = (over = {}) => ({
  mall: 'rakuten', mall_item_key: 'treemuddler200/treemuddler200',
  mall_item_ref: null, mall_item_number: 'treemuddler100-2', ...over,
});

t('[!] システム連携用SKU番号があれば、それで引く', () => {
  const m = new Map([['am-001', [{ ne_code: 'ne-correct', qty: null }]],
    ['treemuddler100-2', [{ ne_code: 'ne-other', qty: null }]]]);
  const r = resolveNeCode(rakutenKeys({ mall_item_ref: 'AM-001' }), m);
  assert.equal(r.neCode, 'ne-correct');
  assert.equal(r.source, 'sku_map');
});

// 🚨 商品番号は**対応表を通さず** NE の商品マスタに直接当てる。
//    対応表の商品番号の行は「同じページのどれか 1 SKU の答え」なので、
//    AM 有りと空欄が混ざると取得順で別 SKU の原価が付く (Codex P1 2026-09-09)
const neProducts = (...codes) => new Map(codes.map((c) => [c, { 商品コード: c }]));

t('[!] システム連携用SKU番号が空欄なら、商品番号で引く', () => {
  for (const am of [null, undefined, '', '   ']) {
    const r = resolveNeCode(rakutenKeys({ mall_item_ref: am }), new Map(), neProducts('treemuddler100-2'));
    assert.equal(r.neCode, 'treemuddler100-2', `mall_item_ref=${JSON.stringify(am)} で商品番号に落ちていない`);
    assert.equal(r.source, 'rakuten_item_number');
    assert.equal(r.qty, null, '楽天は数量を持たない');
  }
});

t('[!] 商品番号は対応表ではなく商品マスタに当てる (同じページの別 SKU を引かない)', () => {
  // 対応表の商品番号の行が別商品を指していても、そちらへ行かない
  const m = new Map([['treemuddler100-2', [{ ne_code: 'ne-of-another-sku', qty: null }]]]);
  const r = resolveNeCode(rakutenKeys(), m, neProducts('treemuddler100-2'));
  assert.equal(r.neCode, 'treemuddler100-2');
});

t('[!] SKU管理番号では紐づけない (同名の別商品の原価を拾わない)', () => {
  // 🚨 2026-09-09 の実害そのもの。商品管理番号と同じ `treemuddler200` が
  //    NE の別商品として実在し、その原価 ¥330 が想定利益に使われていた
  const m = new Map([['treemuddler200', [{ ne_code: 'treemuddler200', qty: null }]]]);   // 拾ってはいけない
  const r = resolveNeCode(rakutenKeys(), m, neProducts('treemuddler100-2', 'treemuddler200'));
  assert.equal(r.neCode, 'treemuddler100-2', 'SKU管理番号の側を拾っている');
});

t('[!] 商品番号でも当たらなければ、別のコードで拾い直さない', () => {
  const m = new Map([['treemuddler200', [{ ne_code: 'treemuddler200', qty: null }]]]);
  const r = resolveNeCode(rakutenKeys(), m, neProducts('treemuddler200'));
  assert.equal(r.status, 'unresolved');
  assert.equal(r.reason, 'ne_code_not_found');
});

t('[!] システム連携用SKU番号が入っているのに当たらなくても、商品番号へ落とさない', () => {
  // 🚨 ルールの条件は「空欄なら」。当たらないのは NE 側の登録が要るという意味
  const r = resolveNeCode(rakutenKeys({ mall_item_ref: 'am-not-registered' }), new Map(),
    neProducts('treemuddler100-2'));
  assert.equal(r.status, 'unresolved');
});

t('商品番号も空なら未紐づけ (合成キーで引きに行かない)', () => {
  const m = new Map([['treemuddler200/treemuddler200', [{ ne_code: 'x', qty: null }]]]);
  assert.equal(resolveNeCode(rakutenKeys({ mall_item_number: null }), m, neProducts('x')).status, 'unresolved');
});

t('システム連携用SKU番号で当たったときは sku_map として記録する', () => {
  const m = new Map([['am-001', [{ ne_code: 'ne1', qty: null }]]]);
  assert.equal(resolveNeCode(rakutenKeys({ mall_item_ref: 'am-001' }), m).source, 'sku_map');
});

t('resolveNeCode は数量も返す', () => {
  const m = new Map([['sku1', [{ ne_code: 'ne001', qty: 5 }]]]);
  assert.equal(resolveNeCode({ mall: 'amazon', mall_item_key: 'sku1' }, m).qty, 5);
  const m2 = new Map([['sku1', [{ ne_code: 'ne001' }]]]);
  assert.equal(resolveNeCode({ mall: 'amazon', mall_item_key: 'sku1' }, m2).qty, null);
});


console.log('');
console.log('Amazon FBM は SKU がそのまま NE の商品コード (中原さん 2026-09-08)');

// 自社出荷 (FBM) の Amazon 出品。送料は自社もちなので送料区分が要る
const fbmListing = (over = {}) => ({
  mall: 'amazon', shop_id: 'S1@M1', mall_item_key: 'ne001', mall_item_ref: 'B001',
  fulfillment: 'FBM', price_incl_tax: 1980, price_tax_included: 1, mall_tax_rate: null,
  postage_included: 1, postage_revenue_incl_tax: 0, points: 0,
  listing_status: 'active', fetch_status: 'ok', valid_until: FUTURE, fetched_at: '2026-09-07T00:00:00Z',
  ...over,
});

// FBM の見積 (送料込みなので in_shipping = 0)
const fbmFee = () => new Map([[
  feeCacheKey({ seller_id: 'S1', marketplace_id: 'M1', seller_sku: 'ne001',
    in_listing_price: 1980, in_shipping: 0, in_points: 0, in_fulfillment: 'FBM' }),
  {
    seller_id: 'S1', marketplace_id: 'M1', seller_sku: 'ne001', asin: 'B001',
    in_listing_price: 1980, in_shipping: 0, in_points: 0, in_fulfillment: 'FBM', in_currency: 'JPY',
    referral_fee_ex_tax: 166, closing_fee_ex_tax: 0, per_item_fee_ex_tax: 0,
    fba_fee_incl_tax: null, fee_status: 'ok', fee_breakdown: '[]',
    fetched_at: '2026-09-06T00:00:00Z', valid_until: FUTURE,
  },
]]);

t('[!] 対応表に無くても、SKU が NE の商品コードなら紐づく', () => {
  // 実測: 未紐づけ FBM 3,445 件のうち 3,369 件が台帳に同じコードで存在した
  const ctx = baseCtx({ skuMap: new Map(), feeEstimates: fbmFee() });   // 対応表は空
  const r = buildRow(fbmListing(), ctx);
  assert.equal(r.ne_code, 'ne001');
  assert.equal(r.ne_code_source, 'fbm_ne_code');
  assert.equal(r.calculation_status, 'ok');
});

t('[!] そのとき数量は 1 (セットなら NE 側に構成品ぶんの原価が入っている)', () => {
  // 🚨 「3個セット」の NE 原価は既に 3 個ぶん。ここで掛けたら二重になる
  const ctx = baseCtx({ skuMap: new Map(), feeEstimates: fbmFee() });
  const r = buildRow(fbmListing(), ctx);
  assert.equal(r.unit_quantity, 1);
  assert.ok(near(r.cost_ex_tax, 600), `NE の原価そのまま (実際 ${r.cost_ex_tax})`);
});

t('[!] FBA には使わない (FBA は対応表で紐づける決まり)', () => {
  // 実測でも FBA 1,311 件のうち台帳に一致するのは 4 件だけ = ルールどおり
  const ctx = baseCtx({ skuMap: new Map(), feeEstimates: feeCache() });
  const r = buildRow(amazonListing({ mall_item_key: 'ne001' }), ctx);
  assert.equal(r.calculation_status, 'incomplete');
  assert.equal(r.incomplete_reason, 'ne_code_not_found');
});

t('[!] 台帳にも無い FBM は、これまでどおり未紐づけ', () => {
  const ctx = baseCtx({ skuMap: new Map(), feeEstimates: fbmFee() });
  const r = buildRow(fbmListing({ mall_item_key: 'nosuchcode' }), ctx);
  assert.equal(r.incomplete_reason, 'ne_code_not_found');
  assert.equal(r.ne_code_source, null);
});

t('[!] 対応表にあれば対応表を優先する (直引きで上書きしない)', () => {
  const ctx = baseCtx({
    skuMap: new Map([['ne001', [{ ne_code: 'ne001', qty: 3 }]]]),
    feeEstimates: fbmFee(),
  });
  const r = buildRow(fbmListing(), ctx);
  assert.equal(r.ne_code_source, 'sku_map');
  assert.equal(r.unit_quantity, 3, '対応表の数量が生きる');
});

t('[!] セット原価 (原価ソース = セット計算) もそのまま使える', () => {
  const ctx = baseCtx({
    skuMap: new Map(), feeEstimates: fbmFee(),
    products: new Map([['ne001', {
      商品コード: 'ne001', 商品名: '3個セット', 原価: 1800, 原価ソース: 'セット計算',
      原価状態: 'COMPLETE', 消費税率: 0.1, 送料コード: '501', 配送方法: 'ネコポス', 売上分類: 3,
    }]]),
  });
  const r = buildRow(fbmListing(), ctx);
  assert.equal(r.cost_method, 'set_master');
  assert.ok(near(r.cost_ex_tax, 1800), 'セットの原価は構成品ぶん。数量を掛けない');
  assert.equal(r.unit_quantity, 1);
});

t('[!] 原価が未完成のセットは、これまでどおり計算しない', () => {
  const ctx = baseCtx({
    skuMap: new Map(), feeEstimates: fbmFee(),
    products: new Map([['ne001', {
      商品コード: 'ne001', 商品名: '未完成セット', 原価: 1800, 原価ソース: 'セット計算',
      原価状態: 'PARTIAL', 消費税率: 0.1, 送料コード: '501', 配送方法: 'ネコポス', 売上分類: 3,
    }]]),
  });
  const r = buildRow(fbmListing(), ctx);
  assert.equal(r.incomplete_reason, 'set_cost_incomplete');
});

t('[!] 送料コードが無ければ計算しない (実測: セット 865 件中 99 件が該当)', () => {
  const ctx = baseCtx({
    skuMap: new Map(), feeEstimates: fbmFee(),
    products: new Map([['ne001', {
      商品コード: 'ne001', 商品名: '送料区分なし', 原価: 600, 原価ソース: 'NE',
      原価状態: 'COMPLETE', 消費税率: 0.1, 送料コード: null, 配送方法: null, 売上分類: 3,
    }]]),
  });
  const r = buildRow(fbmListing(), ctx);
  assert.equal(r.incomplete_reason, 'shipping_master_missing');
});

t('fbmNeCode は大文字小文字を吸収する', () => {
  const products = new Map([['ne001', {}]]);
  assert.equal(fbmNeCode({ mall: 'amazon', fulfillment: 'FBM', mall_item_key: 'NE001' }, products).neCode, 'ne001');
});

t('fbmNeCode は Amazon FBM 以外に効かない', () => {
  const products = new Map([['ne001', {}]]);
  assert.equal(fbmNeCode({ mall: 'rakuten', fulfillment: 'self', mall_item_key: 'ne001' }, products), null);
  assert.equal(fbmNeCode({ mall: 'amazon', fulfillment: 'FBA', mall_item_key: 'ne001' }, products), null);
  assert.equal(fbmNeCode({ mall: 'amazon', fulfillment: null, mall_item_key: 'ne001' }, products), null);
  assert.equal(fbmNeCode({ mall: 'amazon', fulfillment: 'FBM', mall_item_key: '' }, products), null);
  assert.equal(fbmNeCode({ mall: 'amazon', fulfillment: 'FBM', mall_item_key: 'ne001' }, null), null);
});

console.log('');
console.log('在庫数・取扱区分 (2026-09-09 中原さん指示。計算には使わない材料)');

const withStock = (over = {}) => baseCtx({
  products: new Map([['ne001', {
    商品コード: 'ne001', 商品名: 'テスト商品', 原価: 600, 原価ソース: 'NE',
    原価状態: 'COMPLETE', 消費税率: 0.1, 送料コード: '501', 配送方法: 'ネコポス', 売上分類: 3,
    取扱区分: '取扱中', 在庫数: 12, 引当数: 3, ...over,
  }]]),
});

t('在庫数・引当数・取扱区分が行に載る', () => {
  const r = buildRow(rakutenListing(), withStock());
  assert.equal(r.handling_class, '取扱中');
  assert.equal(r.stock_qty, 12);
  assert.equal(r.stock_allocated_qty, 3);
});

t('[!] 在庫と取扱区分は想定利益を1円も変えない (計算に混ぜていない)', () => {
  // 逆検証: 在庫を書き換えて利益が動くなら、どこかで計算に使ってしまっている
  const base = buildRow(rakutenListing(), withStock());
  const other = buildRow(rakutenListing(), withStock({ 取扱区分: '取扱終了', 在庫数: 0, 引当数: 0 }));
  assert.equal(other.expected_profit, base.expected_profit);
  assert.equal(other.expected_margin_rate, base.expected_margin_rate);
  assert.equal(other.rank_eligible, base.rank_eligible);
  assert.equal(other.calculation_status, base.calculation_status);
  // 値そのものは入れ替わっている (何も見ていない試験にしない)
  assert.equal(other.stock_qty, 0);
  assert.equal(other.handling_class, '取扱終了');
});

t('[!] 読めない在庫を 0 にしない (在庫0 と「分からない」は別)', () => {
  for (const v of [null, undefined, '', '未設定', 1.5, NaN]) {
    const r = buildRow(rakutenListing(), withStock({ 在庫数: v, 引当数: v }));
    assert.equal(r.stock_qty, null, `在庫数 ${JSON.stringify(v)} が null になっていない`);
    assert.equal(r.stock_allocated_qty, null, `引当数 ${JSON.stringify(v)} が null になっていない`);
  }
});

t('引き当て超過でマイナスになった在庫も、そのまま持つ (0 に丸めない)', () => {
  const r = buildRow(rakutenListing(), withStock({ 在庫数: -2 }));
  assert.equal(r.stock_qty, -2);
});

t('取扱区分が空文字なら null にする (空の札を画面に出さない)', () => {
  const r = buildRow(rakutenListing(), withStock({ 取扱区分: '' }));
  assert.equal(r.handling_class, null);
});

t('[!] 原価が未登録で計算できない行にも、在庫と取扱区分は載る', () => {
  // 「原価未登録の赤字候補」でも、取扱終了・在庫0なら後回しでよい、が画面で分かること
  const r = buildRow(rakutenListing(), withStock({ 原価: null, 原価状態: 'MISSING' }));
  assert.equal(r.calculation_status, 'incomplete');
  assert.equal(r.stock_qty, 12);
  assert.equal(r.handling_class, '取扱中');
});

t('[!] NE 品番に紐づかない行は null のまま (在庫0 と読ませない)', () => {
  // 紐づく側では 12 が入ることを同じ試験の中で確かめる (null が常に null なだけの試験にしない)
  assert.equal(buildRow(rakutenListing(), withStock()).stock_qty, 12);
  const unresolved = buildRow(rakutenListing(), baseCtx({ skuMap: new Map() }));
  assert.equal(unresolved.incomplete_reason, 'ne_code_not_found');
  assert.equal(unresolved.stock_qty, null);
  assert.equal(unresolved.stock_allocated_qty, null);
  assert.equal(unresolved.handling_class, null);
});

console.log(`\n${passed} 件 PASS`);
