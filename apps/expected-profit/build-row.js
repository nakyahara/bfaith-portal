/**
 * 商品別 想定利益 — 1出品 → 1行 の組み立て (純関数)
 *
 * 正本 = §4 / §7.4 / §9.3 / §15
 *
 * 🚨 ここも副作用なし。DB も API も触らない。
 *    世代ビルダー (build-generation.js) は入力を集めてこの関数に渡すだけにする。
 *
 * Codex R3 が世代ビルダーの受入条件として挙げた3点をここで満たす:
 *   1. 送料収入の「不明」と「送料込み」を区別し、不明はランキングから外す
 *   2. resolveCost → buildProfitInputs → computeProfit を接続し、
 *      商品税率とモール税率の不一致も採用状態に反映する
 *   3. 期限は保存済みの status を信じず、計算時刻で判定し直す
 */
import {
  resolveCost, normalizeFeeEstimate, buildProfitInputs, computeProfit,
  canReuseFeeEstimate, isRankEligible, expenseScopeVersion, effectiveTaxRate, feeCacheKey,
  FORMULA_VERSION, SCENARIO_VERSION, FEE_RATE_VERSION,
} from './calc.js';
import { isExpired } from './util.js';

/** 出品 → NE商品コード。1対多は「原価構成が一意に決まらない」= ambiguous (§7.2) */
export function resolveNeCode(listing, skuMap) {
  // 楽天は対応表 (rakuten_code → ne_code)、Amazon は v_sku_resolved (seller_sku → ne_code[])
  const key = listing.mall === 'rakuten'
    ? String(listing.mall_item_ref || listing.mall_item_key.split('/')[1] || '').toLowerCase()
    : String(listing.mall_item_key || '').toLowerCase();
  const hit = skuMap.get(key);
  if (!hit || hit.length === 0) {
    // 楽天は SKU管理番号 でも引けるようにフォールバック (対応表の作りが2系統ある)
    if (listing.mall === 'rakuten') {
      const alt = skuMap.get(String(listing.mall_item_key).toLowerCase());
      if (alt && alt.length === 1) return { status: 'ok', neCode: alt[0].ne_code };
      if (alt && alt.length > 1) return { status: 'ambiguous', reason: 'multiple_ne_codes' };
    }
    return { status: 'unresolved', reason: 'ne_code_not_found' };
  }
  if (hit.length > 1) {
    // 1出品が複数の NE 商品を指す = この出品だけでは原価構成が決まらない
    return { status: 'ambiguous', reason: 'multiple_ne_codes' };
  }
  return { status: 'ok', neCode: hit[0].ne_code };
}

/**
 * 1出品ぶんの行を組み立てる。
 *
 * @param {object} listing mall_price_snapshot の1行
 * @param {object} ctx {
 *   products: Map<ne_code, product>, shippingRates: Map<code, rate>,
 *   skuMap: Map<key, [{ne_code}]>, feeEstimates: Map<cacheKey, estimate>,
 *   masterFreshness: { costValidUntil, shippingMasterValidUntil },
 *   runInfo: { listingEnumStatus, listingEnumValidUntil, priceRunId },
 *   now: Date, codeVersion: string
 * }
 */
export function buildRow(listing, ctx) {
  const now = ctx.now || new Date();
  const mall = listing.mall;
  const fulfillment = listing.fulfillment;
  const row = {
    generation_id: ctx.generationId,
    mall,
    shop_id: listing.shop_id,
    mall_item_key: listing.mall_item_key,
    ne_code: null,
    product_name: null,
    sales_class: null,
    fulfillment,
    listing_status: listing.listing_status,
    price_incl_tax: listing.price_incl_tax,
    price_ex_tax: null,
    postage_revenue_ex_tax: null,
    revenue_ex_tax: null,
    tax_rate: null,
    cost_ex_tax: null,
    cost_method: null,
    shipping_code: null,
    shipping_method: null,
    shipping_fee_ex_tax: null,
    shipping_work_ex_tax: null,
    shipping_material_ex_tax: null,
    shipping_labor_ex_tax: null,
    shipping_total_ex_tax: null,
    fba_fee_ex_tax: null,
    referral_fee_ex_tax: null,
    closing_fee_ex_tax: null,
    per_item_fee_ex_tax: null,
    fee_total_ex_tax: null,
    fee_rate_display: null,
    fee_breakdown: null,
    expected_profit: null,
    expected_margin_rate: null,
    // 入力ごとの状態 (§7.4)
    listing_enum_status: ctx.runInfo.listingEnumStatus,
    listing_enum_valid_until: ctx.runInfo.listingEnumValidUntil,
    price_status: 'ok',
    price_valid_until: listing.valid_until,
    fee_status: 'not_applicable',
    fee_valid_until: null,
    cost_status: 'missing',
    cost_valid_until: ctx.masterFreshness.costValidUntil,
    shipping_master_status: 'not_applicable',
    shipping_master_valid_until: ctx.masterFreshness.shippingMasterValidUntil,
    shipping_revenue_status: null,
    scenario_fit: 'ok',
    calculation_status: 'incomplete',
    incomplete_reason: null,
    rank_eligible: 0,
    rank_exclusion_reason: null,
    expense_scope_version: expenseScopeVersion({ mall, fulfillment }),
    input_snapshot: null,
    formula_version: FORMULA_VERSION,
    scenario_version: SCENARIO_VERSION,
    fee_rate_version: FEE_RATE_VERSION,
    code_version: ctx.codeVersion || 'unknown',
    price_run_id: ctx.runInfo.priceRunId,
    built_at: now.toISOString(),
  };

  // ── 1. 出品列挙の鮮度 (保存済みの status を信じず、今の時刻で判定し直す) ──
  if (isExpired(ctx.runInfo.listingEnumValidUntil, now)) row.listing_enum_status = 'expired';

  // ── 2. 価格 ──
  if (listing.fetch_status !== 'ok' || listing.price_incl_tax == null) {
    row.price_status = listing.fetch_status === 'tax_included_unknown' ? 'tax_unknown' : 'missing';
  } else if (isExpired(listing.valid_until, now)) {
    row.price_status = 'expired';
  }

  // ── 3. フルフィルメントが未解決なら、どの経路でも計算できない (§R3-2) ──
  if (mall === 'amazon' && fulfillment !== 'FBA' && fulfillment !== 'FBM') {
    row.incomplete_reason = 'fulfillment_unresolved';
    return finish(row, 'incomplete', listing);
  }

  // ── 4. NE商品への対応付け ──
  const resolved = resolveNeCode(listing, ctx.skuMap);
  if (resolved.status !== 'ok') {
    row.cost_status = resolved.status === 'ambiguous' ? 'ambiguous' : 'unresolved';
    row.incomplete_reason = resolved.reason;
    return finish(row, 'incomplete', listing);
  }
  row.ne_code = resolved.neCode;

  const product = ctx.products.get(String(resolved.neCode).toLowerCase());
  if (!product) {
    row.cost_status = 'missing';
    row.incomplete_reason = 'product_not_found';
    return finish(row, 'incomplete', listing);
  }
  row.product_name = product.商品名 || null;
  row.sales_class = product.売上分類 ?? null;

  // ── 5. 原価 (resolveCost を通す。0 は未登録扱い) ──
  const cost = resolveCost(product);
  if (!cost.ok) {
    row.cost_status = 'missing';
    row.incomplete_reason = cost.reason;
    return finish(row, 'incomplete', listing);
  }
  row.cost_ex_tax = cost.costExTax;
  row.cost_method = cost.method;
  row.tax_rate = cost.taxRate;
  row.cost_status = isExpired(ctx.masterFreshness.costValidUntil, now) ? 'expired' : 'ok';

  // ── 6. 商品税率とモール税率の不一致 (Codex R3 の受入条件) ──
  if (listing.mall_tax_rate != null) {
    const mallRate = Number(listing.mall_tax_rate);
    if (Number.isFinite(mallRate) && Math.abs(mallRate - cost.taxRate) > 1e-9) {
      row.price_status = 'tax_mismatch';
      row.incomplete_reason = `tax_mismatch:mall=${mallRate},master=${cost.taxRate}`;
    }
  }

  // ── 7. 配送 (FBA は自社配送区分を要求しない) ──
  const isFba = (mall === 'amazon' && fulfillment === 'FBA');
  let shippingRate = null;
  if (!isFba) {
    row.shipping_code = product.送料コード || null;
    row.shipping_method = product.配送方法 || null;
    shippingRate = row.shipping_code ? ctx.shippingRates.get(String(row.shipping_code)) : null;
    if (!shippingRate) {
      row.shipping_master_status = 'missing';
      row.incomplete_reason = row.incomplete_reason || 'shipping_master_missing';
    } else {
      row.shipping_master_status = isExpired(ctx.masterFreshness.shippingMasterValidUntil, now) ? 'expired' : 'ok';
    }
  }

  // ── 8. 送料収入の「不明」と「送料込み」を区別する (Codex R3 の受入条件) ──
  //    snapshot の null は両方に使われるので、postage_included で判断する
  let postageRevenueInclTax;
  if (listing.postage_included === 1) {
    row.shipping_revenue_status = 'included';       // 送料込み = 別途収入 0
    postageRevenueInclTax = 0;
  } else if (listing.postage_included === 0 && listing.postage_revenue_incl_tax != null) {
    // 🚨 「別途徴収と分かっている」ときだけ収入に足す。
    //    postage_included が null (扱い不明) のまま金額だけあっても足さない。
    //    足すと赤字の出品が黒字に化ける (Codex: -119円 → +181円)
    row.shipping_revenue_status = 'ok';
    postageRevenueInclTax = listing.postage_revenue_incl_tax;
  } else {
    // 別途徴収なのに額が取れない / 送料の扱いそのものが不明
    row.shipping_revenue_status = 'unknown';
    postageRevenueInclTax = 0;                      // 参考値としては 0 で計算するが…
    row.incomplete_reason = row.incomplete_reason || 'shipping_revenue_unknown';
  }

  // ── 9. 手数料 ──
  let feeEstimate = null;
  if (mall === 'amazon') {
    const cached = ctx.feeEstimates.get(feeCacheKeyOf(listing, ctx));
    const wanted = feeInputsOf(listing, ctx);
    const verdict = canReuseFeeEstimate(cached, wanted, now);
    if (!verdict.reuse) {
      row.fee_status = verdict.reason === 'missing' ? 'missing' : `unusable:${verdict.reason}`;
      row.incomplete_reason = row.incomplete_reason || `fee_${verdict.reason}`;
    } else {
      row.fee_valid_until = cached.valid_until;
      feeEstimate = {
        status: cached.fee_status,
        referral: cached.referral_fee_ex_tax,
        closing: cached.closing_fee_ex_tax,
        perItem: cached.per_item_fee_ex_tax,
        fbaInclTax: cached.fba_fee_incl_tax,
        fbaExTax: cached.fba_fee_incl_tax == null ? null : cached.fba_fee_incl_tax / 1.1,
      };
      row.fee_status = cached.fee_status === 'ok' ? 'ok' : cached.fee_status;
      row.referral_fee_ex_tax = cached.referral_fee_ex_tax;
      row.closing_fee_ex_tax = cached.closing_fee_ex_tax;
      row.per_item_fee_ex_tax = cached.per_item_fee_ex_tax;
      row.fee_breakdown = cached.fee_breakdown;
    }
  }

  // ── 10. 計算 (buildProfitInputs → computeProfit を通す) ──
  const built = buildProfitInputs({
    mall, fulfillment,
    priceInclTax: row.price_status === 'ok' ? listing.price_incl_tax : null,
    postageRevenueInclTax,
    productTaxRate: cost.taxRate,
    costExTax: cost.costExTax,
    shippingRate,
    feeEstimate,
  });
  if (!built.ok) {
    row.incomplete_reason = row.incomplete_reason || built.reason;
    return finish(row, 'incomplete', listing);
  }
  const profit = computeProfit(built.args);
  if (!profit.ok) {
    row.incomplete_reason = row.incomplete_reason || profit.reason;
    return finish(row, 'incomplete', listing);
  }

  row.price_ex_tax = built.args.priceExTax;
  row.postage_revenue_ex_tax = built.args.postageRevenueExTax;
  row.revenue_ex_tax = profit.revenueExTax;
  row.shipping_total_ex_tax = built.args.shippingTotalExTax;
  row.fba_fee_ex_tax = built.args.fbaFeeExTax;
  row.fee_total_ex_tax = built.args.feeTotalExTax;
  row.fee_rate_display = built.detail.feeRateDisplay;
  if (built.detail.shippingParts) {
    row.shipping_fee_ex_tax = built.detail.shippingParts.fee;
    row.shipping_work_ex_tax = built.detail.shippingParts.work;
    row.shipping_material_ex_tax = built.detail.shippingParts.material;
    row.shipping_labor_ex_tax = built.detail.shippingParts.labor;
  }
  row.expected_profit = profit.expectedProfit;
  row.expected_margin_rate = profit.expectedMarginRate;

  // 再現に必要な入力を不変のコピーとして残す (§7.4)
  row.input_snapshot = JSON.stringify({
    price_incl_tax: listing.price_incl_tax,
    price_tax_included: listing.price_tax_included,
    mall_tax_rate: listing.mall_tax_rate,
    postage_included: listing.postage_included,
    postage_revenue_incl_tax: listing.postage_revenue_incl_tax,
    points: listing.points,
    cost_ex_tax: cost.costExTax,
    cost_source: product.原価ソース,
    cost_state: product.原価状態,
    tax_rate: cost.taxRate,
    shipping_code: row.shipping_code,
    shipping_rate: shippingRate || null,
    fee_inputs: mall === 'amazon' ? feeInputsOf(listing, ctx) : null,
    fee_breakdown: row.fee_breakdown ? JSON.parse(row.fee_breakdown) : null,
    price_fetched_at: listing.fetched_at,
    fee_fetched_at: row.fee_valid_until,
  });

  // 計算そのものは成立。状態が揃っていれば ok
  const hasProblem = row.price_status !== 'ok'
    || row.cost_status !== 'ok'
    || (mall === 'amazon' && row.fee_status !== 'ok')
    || (!isFba && row.shipping_master_status !== 'ok')
    || row.shipping_revenue_status === 'unknown';
  return finish(row, hasProblem ? 'incomplete' : 'ok', listing);
}

function finish(row, status, listing) {
  // 🚨 計算できなかった行こそ「何を見て失敗したか」が要る。入力は必ず残す
  if (row.input_snapshot == null) {
    row.input_snapshot = JSON.stringify({
      incomplete: true,
      reason: row.incomplete_reason,
      price_incl_tax: listing?.price_incl_tax ?? null,
      price_tax_included: listing?.price_tax_included ?? null,
      mall_tax_rate: listing?.mall_tax_rate ?? null,
      postage_included: listing?.postage_included ?? null,
      postage_revenue_incl_tax: listing?.postage_revenue_incl_tax ?? null,
      points: listing?.points ?? null,
      fetch_status: listing?.fetch_status ?? null,
      price_fetched_at: listing?.fetched_at ?? null,
    });
  }
  row.calculation_status = status;
  const verdict = isRankEligible({
    mall: row.mall,
    fulfillment: row.fulfillment,
    listing_status: row.listing_status,
    calculation_status: row.calculation_status,
    scenario_fit: row.scenario_fit,
    price_source: 'own_listing',
    shipping_revenue_status: row.shipping_revenue_status,
    listing_enum_status: row.listing_enum_status,
    price_status: row.price_status,
    cost_status: row.cost_status,
    fee_status: row.fee_status,
    shipping_master_status: row.shipping_master_status,
  });
  row.rank_eligible = verdict.eligible ? 1 : 0;
  row.rank_exclusion_reason = verdict.eligible ? null : verdict.reason;
  return row;
}

/** 手数料見積の入力 (出品から作る)。refresh-fees と同じ形にする */
export function feeInputsOf(listing, ctx) {
  return {
    seller_id: ctx.sellerId,
    marketplace_id: ctx.marketplaceId,
    seller_sku: listing.mall_item_key,
    asin: listing.mall_item_ref,
    in_listing_price: listing.price_incl_tax,
    in_shipping: listing.postage_revenue_incl_tax ?? 0,
    in_points: listing.points,
    in_fulfillment: listing.fulfillment,
    in_currency: 'JPY',
  };
}

export function feeCacheKeyOf(listing, ctx) {
  // 🚨 保存側 (refresh-fees) と同じ関数を使う。ここがズレると見積を1件も引けない
  return feeCacheKey(feeInputsOf(listing, ctx));
}
