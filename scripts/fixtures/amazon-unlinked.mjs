/**
 * fixtures/amazon-unlinked.mjs — 「Amazon の出品一覧・まだつないでいない SKU」(0074) の試験の部品 (test-amazon-unlinked.mjs と test-amazon-unlinked-pg.mjs が使う)
 *
 *   - レポートの見出し: 英語 = SP-API の説明の GET_MERCHANT_LISTINGS_ALL_DATA の 1 行目 (All Listings Report の列の並び)。
 *     日本語 = 日本のマーケットプレイスの今のレポートの列 (expected-profit/fetch-listings.js が読む 出品者SKU・商品ID・価格・フルフィルメント・チャンネル・ステータス・ポイント・
 *     merchant-shipping-group と、AI_reference『商品別想定利益_要件定義』§15-4 の列の一覧 (数量・在庫数・出品日)・profit-calculator の 商品名 から並べた)
 */
/** 0074 の前 (= 出品一覧の表の無い DB) の番号 */
export const VERSION_BEFORE_0074 = '0073';

/** SP-API の説明の GET_MERCHANT_LISTINGS_ALL_DATA の見出し (英語) */
export const HEADER_EN = ['item-name', 'item-description', 'listing-id', 'seller-sku', 'price', 'quantity', 'open-date', 'image-url', 'item-is-marketplace', 'product-id-type',
  'zshop-shipping-fee', 'item-note', 'item-condition', 'zshop-category1', 'zshop-browse-path', 'zshop-storefront-feature', 'asin1', 'asin2', 'asin3', 'will-ship-internationally',
  'expedited-shipping', 'zshop-boldface', 'product-id', 'bid-for-featured-placement', 'add-delete', 'pending-quantity', 'fulfillment-channel', 'merchant-shipping-group', 'status'];
/** 日本のマーケットプレイスのレポートの見出し (日本語・merchant-shipping-group だけ英語のまま) */
export const HEADER_JP = ['商品名', '出品ID', '出品者SKU', '価格', '数量', '出品日', '商品IDタイプ', 'コンディション説明', 'コンディション', '国外へ配送可', '迅速な配送', '商品ID',
  '在庫数', 'フルフィルメント・チャンネル', 'merchant-shipping-group', 'ステータス', '最低販売価格', '最高販売価格', 'ポイント'];
const EN_OF = { sku: 'seller-sku', name: 'item-name', price: 'price', qty: 'quantity', open: 'open-date', asin: 'asin1', pid: 'product-id', ch: 'fulfillment-channel', st: 'status' };
const JP_OF = { sku: '出品者SKU', name: '商品名', price: '価格', qty: '数量', open: '出品日', asin: '商品ID', pid: '商品ID', ch: 'フルフィルメント・チャンネル', st: 'ステータス' };

/**
 * 行 ({ sku, name, price, qty, open, asin, ch, st }) → TSV の原文 (見出し + 行・改行は \r\n = Amazon の原文と同じ)。
 * header = HEADER_EN / HEADER_JP / 自分で並べた見出し。of = 見出しの名前の対応 (既定 = header で決まる)
 */
export function buildTsv(rows, header = HEADER_JP) {
  const of = header === HEADER_EN ? EN_OF : JP_OF;
  const lines = [header.join('\t')];
  for (const r of rows) {
    const cells = header.map((h) => {
      for (const [k, name] of Object.entries(of)) if (name === h && r[k] !== undefined) return String(r[k]);
      if (h === 'merchant-shipping-group') return r.ch === 'DEFAULT' ? '移行済みのテンプレート' : '';
      if (h === 'ポイント') return '0';
      if (h === 'コンディション' || h === 'item-condition') return '11';
      return '';
    });
    lines.push(cells.join('\t'));
  }
  return lines.join('\r\n') + '\r\n';
}

export const AMZ = 'main@A1VC38T7YXB528';
/** 夜間ロードの材料: a001・a002 (単品)。Amazon の出品 = pr_a001 (SKU マスタの構成 = つないでいる)・pr_nocomp (出品だけ・構成なし) */
export function makePlan() {
  const single = (code, name) => ({ code, name, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3, cost: { jpy: 100, source: 'ne', status: 'COMPLETE' } });
  const amazon = (listingCode, title, comps) => ({ mall: 'amazon', shopCode: AMZ, marketplaceId: 'A1VC38T7YXB528', listingCode, title, status: 'active', components: comps, asinCandidates: [], fnskuCandidates: [], evidenceSource: 'mirror_sku_master' });
  return {
    skus: [single('a001', '単品 1'), single('a002', '単品 2')],
    variationGroups: [], setComponents: [],
    listings: [
      amazon('pr_a001', 'SKU マスタの 1', [{ code: 'a001', qty: 1, sortOrder: 0, resolution: 'imported', evidence: { source: 'm_sku_master' } }]),
      amazon('pr_nocomp', '出品だけ', []),
    ],
    observations: [], physicals: [], compliance: [], workers: [], suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: [],
  };
}

export const addDays = (d, n) => new Date(Date.parse(`${d}T00:00:00Z`) + n * 864e5).toISOString().slice(0, 10);
export const jstToday = (ms = Date.now()) => new Date(ms + 9 * 3600e3).toISOString().slice(0, 10);
/**
 * 試験の出品一覧 (6 行・出品日は today からの日数): つないでいる pr_a001・出品だけ PR_NOCOMP (大文字 = 正規化で pr_nocomp)・Company DB に無い新しい FBA の SKU (停止中 = 納品前)・
 * 昔の FBA の SKU (出品日の表記が / 区切り)・自社発送 1 つ (価格に , )・バリエーションの親 (価格なし・Incomplete)
 */
export function reportRows(today) {
  const at = (n, hms = '10:00:00') => `${addDays(today, -n)} ${hms} JST`;
  return [
    { sku: 'pr_a001', name: 'はちみつ 300g', price: '1980', qty: '', open: at(200), asin: 'B00000A001', ch: 'AMAZON_JP', st: 'Active' },
    { sku: 'PR_NOCOMP', name: 'ハチミツ 500g (出品だけ)', price: '2980', qty: '', open: at(20, '09:15:00'), asin: 'B00000A002', ch: 'AMAZON_JP', st: 'Active' },
    { sku: 'fba_new1', name: '新しい FBA の商品 (まだ納品前)', price: '1500', qty: '', open: at(2, '18:00:00'), asin: 'B00000A003', ch: 'AMAZON_JP', st: 'Inactive' },
    { sku: 'fba_old', name: '昔の FBA の商品', price: '900', qty: '', open: `${addDays(today, -800).replace(/-/g, '/')} 08:00:00 JST`, asin: 'B00000A004', ch: 'AMAZON_JP', st: 'Inactive' },
    { sku: 'fbm_1', name: '自社発送の商品', price: '1,280', qty: '5', open: at(60, '11:11:11'), asin: 'B00000A005', ch: 'DEFAULT', st: 'Active' },
    { sku: 'parent_1', name: 'バリエーションの親', price: '', qty: '', open: at(40, '00:00:00'), asin: 'B00000A006', ch: 'AMAZON_JP', st: 'Incomplete' },
  ];
}
