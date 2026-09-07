/**
 * 商品別 想定利益 — 計算の純関数
 *
 * 正本 = AI_reference『システム設計/商品別想定利益_要件定義_20260907.md』(v1.4) §4 / §15
 *
 * 🚨 ここは副作用を持たない。DB も API も触らない。
 *    本番の計算経路とテストが同じ関数を通るようにするため、判断はすべてここに集める
 *    (テスト側にロジックを書き写すと、本番のガードを消しても PASS してしまう — PR #1247 の教訓)
 *
 * 税の基準 (§4.1 / §4.2.1):
 *   すべて **税抜** に統一する。売上も費用も。
 *   「税込売上 − 税込原価」でも一貫はするが、軽減税率8%の商品 (取扱中446件) だけ
 *   利益率が約0.5pt低く出て不当に下位へ沈むため、税抜に揃える。
 */

export const FORMULA_VERSION = 'v1-2026-09';
export const SCENARIO_VERSION = 'v1-2026-09';
export const FEE_RATE_VERSION = 'dim_mall_2026-09';

/** 消費税率の fallback (mirror_products.消費税率 が null/0 のとき) */
export const TAX_RATE_FALLBACK = 0.1;

/** 送料・手数料にかかる税率 (商品の軽減税率を流用しない — §4.2) */
export const SERVICE_TAX_RATE = 0.1;

/** 他モールの簡易料率 (§15-1)。税込売価 × 料率 = 税込手数料とみなす */
export const MALL_FEE_RATE_APPROX = {
  rakuten: 0.10,
  yahoo: 0.10,
  aupay: 0.13,
  linegift: 0.13,
  qoo10: 0.10,
  mercari: 0.10,
};

/** Amazon 販売手数料率の改定日 (跨いだキャッシュは無条件失効 — §7.3) */
export const FEE_REVISION_DATES = ['2026-04-01'];

// ────────────────────────────────────────────────────────────
// 税
// ────────────────────────────────────────────────────────────

/**
 * 商品の消費税率を「係数」に直す。
 * mirror_products.消費税率 は小数 (0.1 / 0.08)。整数 (10) ではない。
 * 数値でない・0以下・1以上は未知の単位として fallback する (PR #1247 と同じ判断)
 */
export function taxMultiplier(rate) {
  if (!Number.isFinite(rate) || rate <= 0 || rate >= 1) return 1 + TAX_RATE_FALLBACK;
  return 1 + rate;
}

/** 有効な商品税率 (小数) を返す。未登録は fallback */
export function effectiveTaxRate(rate) {
  if (!Number.isFinite(rate) || rate <= 0 || rate >= 1) return TAX_RATE_FALLBACK;
  return rate;
}

/** 税込 → 税抜 */
export function exTax(inclTax, rate) {
  if (!Number.isFinite(inclTax)) return null;
  return inclTax / (1 + rate);
}

// ────────────────────────────────────────────────────────────
// セット原価 (§15-5)
// ────────────────────────────────────────────────────────────

/**
 * 原価を決める。
 *
 * PR-0 の実測で、セットの原価は rebuild-m-products.js が
 * `raw_ne_set_products` の 数量 × 構成品原価 で **1セット分**を計算済み (原価ソース = 'セット計算')。
 * よって構成品を再展開しない (二重実装しない)。
 *
 * 有効なゼロは無い = 原価 0 は未登録とみなす (実測: 原価0の取扱中セットは2件のみ)
 *
 * @param {{原価:number|null, 原価ソース:string|null, 原価状態:string|null, 消費税率:number|null}} product
 * @returns {{ok:true, costExTax:number, method:string, taxRate:number} | {ok:false, reason:string}}
 */
export function resolveCost(product) {
  if (!product) return { ok: false, reason: 'product_not_found' };
  const cost = product.原価;
  if (!Number.isFinite(cost) || cost <= 0) return { ok: false, reason: 'cost_missing' };
  // セットは原価状態 COMPLETE のものだけ採用する (rebuild が品質チェック済み)
  const isSet = product.原価ソース === 'セット計算';
  if (isSet && product.原価状態 !== 'COMPLETE') return { ok: false, reason: 'set_cost_incomplete' };
  return {
    ok: true,
    costExTax: cost,
    method: isSet ? 'set_master' : 'single',
    taxRate: effectiveTaxRate(product.消費税率),
  };
}

// ────────────────────────────────────────────────────────────
// 配送関係費 (§15-2)
// ────────────────────────────────────────────────────────────

/**
 * 配送関係費を税抜に直す。
 * 取込元 CSV のヘッダは「送料（税込み）」なので送料だけ ÷1.1。
 * 出荷作業料・想定梱包資材費・想定人件費は社内見積 (税の概念なし) なのでそのまま。
 * 🚨 合計を一律 ÷1.1 しない
 */
export function shippingCostExTax(rate) {
  if (!rate) return { ok: false, reason: 'shipping_master_missing' };
  const fee = Number.isFinite(rate.送料) ? rate.送料 / (1 + SERVICE_TAX_RATE) : null;
  const work = Number.isFinite(rate.出荷作業料) ? rate.出荷作業料 : 0;
  const material = Number.isFinite(rate.想定梱包資材費) ? rate.想定梱包資材費 : 0;
  const labor = Number.isFinite(rate.想定人件費) ? rate.想定人件費 : 0;
  if (fee == null) return { ok: false, reason: 'shipping_fee_missing' };
  return {
    ok: true,
    fee, work, material, labor,
    total: fee + work + material + labor,
  };
}

// ────────────────────────────────────────────────────────────
// Amazon 手数料の正規化 (§15-1)
// ────────────────────────────────────────────────────────────

const KNOWN_FEE_TYPES = new Set(['ReferralFee', 'VariableClosingFee', 'PerItemFee', 'FBAFees']);

/**
 * getMyFeesEstimates の FeesEstimate を保存形へ正規化する。
 *
 * - 最上位の FinalFee だけ合算する。IncludedFeeDetailList は表示用 (二重加算しない)
 * - ReferralFee / VariableClosingFee / PerItemFee は **税抜** (決済突合で ×1.10 が実請求と確認)
 * - FBAFees は **税込** (Amazon JP の FBA 手数料表は税込表示。決済実額と ×1.00 で一致) → ÷1.1
 * - 行があれば 0 は有効。行が無ければ NULL (0 で代用しない)
 * - 未知の FeeType は合計に含めた上で unknown_fee_type
 * - Σ最上位 FinalFee ≠ TotalFeesEstimate なら inconsistent (許容差 0)
 */
export function normalizeFeeEstimate(feesEstimate) {
  if (!feesEstimate || !Array.isArray(feesEstimate.FeeDetailList)) {
    return { status: 'missing', referral: null, closing: null, perItem: null, fbaInclTax: null, sum: null, total: null, unknownTypes: [] };
  }
  const amountOf = (d) => {
    const v = d?.FinalFee?.Amount;
    return Number.isFinite(v) ? v : null;
  };
  let referral = null, closing = null, perItem = null, fbaInclTax = null;
  let sum = 0;
  const unknownTypes = [];
  for (const d of feesEstimate.FeeDetailList) {
    const amt = amountOf(d);
    if (amt == null) continue;
    sum += amt;
    switch (d.FeeType) {
      case 'ReferralFee': referral = amt; break;
      case 'VariableClosingFee': closing = amt; break;
      case 'PerItemFee': perItem = amt; break;
      case 'FBAFees': fbaInclTax = amt; break;
      default:
        if (!KNOWN_FEE_TYPES.has(d.FeeType)) unknownTypes.push(d.FeeType);
    }
  }
  const total = Number.isFinite(feesEstimate.TotalFeesEstimate?.Amount)
    ? feesEstimate.TotalFeesEstimate.Amount : null;

  let status = 'ok';
  if (unknownTypes.length > 0) status = 'unknown_fee_type';
  else if (total != null && sum !== total) status = 'inconsistent';

  return {
    status,
    referral,
    closing,
    perItem,
    fbaInclTax,
    fbaExTax: fbaInclTax == null ? null : fbaInclTax / (1 + SERVICE_TAX_RATE),
    sum,
    total,
    unknownTypes,
  };
}

/**
 * 手数料見積を再利用してよいか。
 * 🚨 PK ではなく全入力を比較する (asin / 通貨を含む — Codex R3)。
 *    期限・改定日跨ぎを価格の一致より先に見る (§7.3 の判定順序)
 */
export function canReuseFeeEstimate(cached, wanted, now = new Date()) {
  if (!cached) return { reuse: false, reason: 'missing' };
  // 1. 期限切れ
  if (!cached.valid_until || new Date(cached.valid_until) <= now) return { reuse: false, reason: 'expired' };
  // 2. 料率改定日を跨いでいる
  const fetchedAt = new Date(cached.fetched_at);
  for (const d of FEE_REVISION_DATES) {
    const rev = new Date(`${d}T00:00:00+09:00`);
    if (fetchedAt < rev && rev <= now) return { reuse: false, reason: 'fee_revision_crossed' };
  }
  // 3. 見積入力の完全一致 (PK に無い asin / currency も見る)
  const keys = ['seller_id', 'marketplace_id', 'seller_sku', 'asin',
    'in_listing_price', 'in_shipping', 'in_points', 'in_fulfillment', 'in_currency'];
  for (const k of keys) {
    if (cached[k] !== wanted[k]) return { reuse: false, reason: `input_mismatch:${k}` };
    }
  return { reuse: true };
}

// ────────────────────────────────────────────────────────────
// 想定利益 (§4.1)
// ────────────────────────────────────────────────────────────

/**
 * 想定利益を計算する。入力はすべて解決済みの数値 (税抜)。
 *
 *   想定売上高(税抜) = 商品売価(税抜) + 別途送料収入(税抜)
 *   想定利益 = 想定売上高 − 商品原価 − 配送・出荷費用 − モール手数料
 *
 * 🚨 fee_total_ex_tax に FBA費用を含めない。FBA費用は配送費側で1回だけ引く (§4.4.1)
 */
export function computeProfit({ priceExTax, postageRevenueExTax = 0, costExTax, shippingTotalExTax = 0, fbaFeeExTax = 0, feeTotalExTax = 0 }) {
  const revenue = priceExTax + postageRevenueExTax;
  if (!Number.isFinite(revenue) || revenue <= 0) return { ok: false, reason: 'revenue_invalid' };
  const profit = revenue - costExTax - shippingTotalExTax - fbaFeeExTax - feeTotalExTax;
  return {
    ok: true,
    revenueExTax: revenue,
    expectedProfit: profit,
    expectedMarginRate: profit / revenue,
  };
}

// ────────────────────────────────────────────────────────────
// 経路別の必須条件 (§4.5) と ランキング適格条件 (§9.3 / §15-9)
// ────────────────────────────────────────────────────────────

/**
 * 計算経路ごとに、どの入力が必須でどれが not_applicable かを返す。
 * 🚨 判定を2か所に書かない。ランキング適格条件もこの表を参照する
 */
export function requiredInputs({ mall, fulfillment }) {
  const base = ['listing_enum', 'price', 'cost'];
  if (mall === 'amazon' && fulfillment === 'FBA') {
    return { required: [...base, 'fee'], notApplicable: ['shipping_master'] };
  }
  if (mall === 'amazon') {
    return { required: [...base, 'fee', 'shipping_master'], notApplicable: [] };
  }
  // 他モールは簡易料率なので手数料に期限が無い
  return { required: [...base, 'shipping_master'], notApplicable: ['fee'] };
}

/**
 * 既定ランキングに載せてよいか (§9.3 + §15-9)。
 *
 * 🚨 not_applicable の入力を判定に含めない (含めると FBA が全部除外される)。
 * 🚨 列挙期限を判定に含める (含めないと期限切れ列挙の行がランキングに残る)。
 */
export function isRankEligible(row) {
  const { required, notApplicable } = requiredInputs(row);
  if (row.calculation_status !== 'ok') return { eligible: false, reason: `calculation_${row.calculation_status}` };
  if (row.scenario_fit !== 'ok') return { eligible: false, reason: 'scenario_undecidable' };
  // Inactive は計算・表示するが既定ランキング外 (中原さん決定)
  if (row.listing_status !== 'active') return { eligible: false, reason: `listing_${row.listing_status}` };
  // 自社モール価格であること (Amazon は my_price。buybox 代替は採用しない)
  if (row.price_source && row.price_source !== 'own_listing') return { eligible: false, reason: 'not_own_price' };
  // FBM の送料収入不明は参考値 (§15-2)
  if (row.shipping_revenue_status === 'unknown') return { eligible: false, reason: 'shipping_revenue_unknown' };
  for (const input of required) {
    const status = row[`${input}_status`];
    if (status !== 'ok') return { eligible: false, reason: `${input}_${status || 'missing'}` };
  }
  for (const input of notApplicable) {
    const status = row[`${input}_status`];
    if (status && status !== 'not_applicable' && status !== 'ok') {
      // not_applicable のはずの入力が別状態なら、経路の判定が間違っている
      return { eligible: false, reason: `${input}_unexpected_${status}` };
    }
  }
  return { eligible: true };
}

/** 費用範囲の識別 (§4.8)。FBA と自社配送を同じランキングに混ぜないために持つ */
export function expenseScopeVersion({ mall, fulfillment }) {
  return (mall === 'amazon' && fulfillment === 'FBA') ? 'fba_v1' : 'self_v1';
}
