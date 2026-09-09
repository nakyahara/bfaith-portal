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
import { skuMapHasQuantity } from './load-inputs.js';

/**
 * 出品 → NE商品コード。1対多は「原価構成が一意に決まらない」= ambiguous (§7.2)
 *
 * 🚨 Amazon は出荷区分で紐づけ方が違う (中原さん 2026-09-08)。
 *    - **FBA** = 対応表 (`v_sku_resolved`) で紐づける。ここに無ければ未紐づけ
 *    - **FBM (自社出荷)** = 在庫連携の都合で、**SKU がそのまま NE の商品コード**
 *      (単品) **または NE のセット商品コード**になっている。
 *      セットの原価は NE 側が構成品から計算済み (`原価ソース = 'セット計算'`)
 *
 *    実測 (2026-09-08): 未紐づけ FBM 3,445 件のうち **3,369 件**が台帳に同じコードで存在
 *    (単品 2,504 / セット 865)。FBA 側は 1,311 件中 4 件しか一致しない = ルールどおり。
 *
 * 🚨 楽天の紐づけ (中原さん 2026-09-09):
 *    「システム連携用SKU番号と紐づけて。システム連携用SKU番号が空欄なら商品番号と紐づけて」
 *    = **AM (merchantDefinedSkuId) → 空欄なら 商品番号 (itemNumber)**。
 *    **SKU管理番号 (variants のキー) では紐づけない**。楽天が自動採番することがあり、
 *    たまたま同名の別 NE 商品に当たる。実害 = 商品ページ `treemuddler200` が、
 *    商品番号 `treemuddler100-2` ではなく同名の別商品の原価 (¥330) で計算されていた。
 *
 * @param {Map} products ne_code(小文字) → 商品。FBM のフォールバックで存在を確かめる
 */
export function rakutenSystemSkuKey(listing) {
  // 🚨 空文字も「空欄」として扱う (trim してから見る)
  return String(listing.mall_item_ref ?? '').trim().toLowerCase() || null;
}

/**
 * 楽天で システム連携用SKU番号 が空欄のときの紐づけ = **商品番号**。
 *
 * 🚨 対応表 (`f_rakuten_sku_map`) を通さず、商品番号を **NE の商品コードに直接**当てる。
 *    商品番号は 1 商品ページに 1 つしか無いので、対応表では「同じページの
 *    どれか 1 SKU の答え」が入ってしまう。AM を持つ SKU と持たない SKU が
 *    同じページに混ざると、**取得順しだいで別 SKU の原価**が付く (Codex P1 2026-09-09)。
 *    直接当てれば答えは順序に依らず一意になる。
 * 🚨 数量は不明のまま (楽天の対応表は数量を持たない。§16-2)
 */
export function rakutenItemNumberNeCode(listing, products) {
  if (!products) return null;
  if (listing?.mall !== 'rakuten') return null;
  if (rakutenSystemSkuKey(listing)) return null;       // AM があるならこちらは使わない
  const code = String(listing.mall_item_number ?? '').trim().toLowerCase();
  if (!code || !products.has(code)) return null;
  return { status: 'ok', neCode: code, qty: null, source: 'rakuten_item_number' };
}

export function resolveNeCode(listing, skuMap, products = null) {
  // 楽天は対応表 (rakuten_code → ne_code)、Amazon は v_sku_resolved (seller_sku → ne_code[])
  const key = listing.mall === 'rakuten'
    ? rakutenSystemSkuKey(listing)
    : String(listing.mall_item_key || '').toLowerCase();
  const hit = key ? skuMap.get(key) : null;
  if (!hit || hit.length === 0) {
    // 🚨 システム連携用SKU番号が空欄なら商品番号で紐づける (中原さん 2026-09-09)。
    //    AM が入っているのに当たらないときは**落とさない**。条件は「空欄なら」であって
    //    「当たらなければ」ではない。落とすとまた別商品の原価を静かに拾う
    const byItemNumber = rakutenItemNumberNeCode(listing, products);
    if (byItemNumber) return byItemNumber;
    // 🚨 SKU管理番号 でも引き直さない (2026-09-09)。それが別商品の原価を使う経路だった
    // 🚨 FBM は対応表に載っていないのが普通。SKU がそのまま NE の商品コード
    //    (単品またはセット) なら、それで紐づける
    const fbm = fbmNeCode(listing, products);
    if (fbm) return fbm;
    return { status: 'unresolved', reason: 'ne_code_not_found' };
  }
  if (hit.length > 1) {
    // 1出品が複数の NE 商品を指す = この出品だけでは原価構成が決まらない
    return { status: 'ambiguous', reason: 'multiple_ne_codes' };
  }
  return { status: 'ok', neCode: hit[0].ne_code, qty: hit[0].qty ?? null, source: 'sku_map' };
}

/**
 * Amazon の自社出荷 (FBM) だけ、SKU をそのまま NE の商品コードとして引く。
 *
 * 🚨 **数量は 1**。NE の商品 (単品でもセットでも) 1 つが、Amazon の 1 出品に対応する。
 *    「3個セット」なら NE 側に 3 個ぶんの原価が入っているので、ここで掛けてはいけない。
 * 🚨 FBA には使わない。FBA は対応表で紐づける決まり (実測でも 1,311 件中 4 件しか一致しない)。
 */
export function fbmNeCode(listing, products) {
  if (!products) return null;
  if (listing?.mall !== 'amazon' || listing?.fulfillment !== 'FBM') return null;
  const code = String(listing.mall_item_key ?? '').toLowerCase();
  if (!code || !products.has(code)) return null;
  return { status: 'ok', neCode: code, qty: 1, source: 'fbm_ne_code' };
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
    ne_code_source: null,
    product_name: null,
    sales_class: null,
    // 🚨 表示専用 (2026-09-09)。計算には使わない。NE 品番が決まって初めて埋まるので、
    //    未紐づけの行では null のまま = 「在庫0」ではなく「分からない」
    handling_class: null,
    stock_qty: null,
    stock_allocated_qty: null,
    fulfillment,
    listing_status: listing.listing_status,
    price_incl_tax: listing.price_incl_tax,
    price_ex_tax: null,
    postage_revenue_ex_tax: null,
    revenue_ex_tax: null,
    tax_rate: null,
    cost_ex_tax: null,
    cost_method: null,
    unit_quantity: null,
    shipping_code: null,
    shipping_method: null,
    shipping_rate_name: null,
    shipping_rate_category: null,
    // モール側の配送パターン。Amazon FBM では送料込みかどうかの判断根拠 (§16-13)。
    // FBA でも「何で配送しているか」を画面に出すため、経路によらず持つ
    shipping_group: listing.shipping_group ?? null,
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
  const resolved = resolveNeCode(listing, ctx.skuMap, ctx.products);
  if (resolved.status !== 'ok') {
    row.cost_status = resolved.status === 'ambiguous' ? 'ambiguous' : 'unresolved';
    row.incomplete_reason = resolved.reason;
    return finish(row, 'incomplete', listing);
  }
  row.ne_code = resolved.neCode;
  // どうやって紐づけたか (対応表 / FBM の商品コード直引き) を残す
  row.ne_code_source = resolved.source ?? null;

  const product = ctx.products.get(String(resolved.neCode).toLowerCase());
  if (!product) {
    row.cost_status = 'missing';
    row.incomplete_reason = 'product_not_found';
    return finish(row, 'incomplete', listing);
  }
  row.product_name = product.商品名 || null;
  row.sales_class = product.売上分類 ?? null;
  // 🚨 在庫と取扱区分は原価が無くても埋める。「原価未登録で判定できない赤字候補」でも、
  //    取扱終了・在庫0なら後回しでよい、という判断が画面でできるようにする
  row.handling_class = product.取扱区分 || null;
  row.stock_qty = intOrNull(product.在庫数);
  row.stock_allocated_qty = intOrNull(product.引当数);

  // ── 5. 原価 (resolveCost を通す。0 は未登録扱い) ──
  const cost = resolveCost(product);
  if (!cost.ok) {
    row.cost_status = 'missing';
    row.incomplete_reason = cost.reason;
    return finish(row, 'incomplete', listing);
  }
  // 🚨 まとめ買いSKU は単品原価 × 数量。ここを掛けないと利益率が数量倍に化ける
  //    (実データ 2026-09-07: opbs454 が 数量12 で 原価1,001 → 79.4%)
  //    Amazon の対応表 (v_sku_resolved) にだけ数量がある。楽天は qty = null
  const qty = resolved.qty;
  row.unit_quantity = qty ?? null;
  // 🚨 数量列を持つモール (Amazon) で数量が読めないなら、原価が決まらない。
  //    単品として計算すると、まとめ買いSKU が過大利益のままランキングに載る (Codex R7-1)。
  //    内訳と利益は一致してしまうので、公開前検証でも捕まえられない
  if (skuMapHasQuantity(mall) && qty == null) {
    row.cost_status = 'missing';
    row.incomplete_reason = 'quantity_unknown';
    return finish(row, 'incomplete', listing);
  }
  // 🚨 以降は必ず costExTax (数量を掛けた後) を使う。cost.costExTax を直接使うと
  //    「表示は数量倍だが利益は単品原価」というズレが出る (実データで実際に出た)
  const costExTax = cost.costExTax * (qty ?? 1);
  row.cost_ex_tax = costExTax;
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
      // 🚨 実際に金額を引いてきた配送区分の名前を残す (2026-09-08 中原さん指示)。
      //    送料コード (501) だけでは「どの配送方法で計算したか」が画面から読めない
      row.shipping_rate_name = shippingRate.小分類区分名称 || null;
      row.shipping_rate_category = shippingRate.大分類区分 || null;
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
    costExTax,
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
    cost_ex_tax: costExTax,
    unit_cost_ex_tax: cost.costExTax,     // 単品いくらだったか (数量倍する前)
    unit_quantity: qty ?? null,
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

/**
 * 在庫の個数。
 * 🚨 読めない値を 0 にしない。「在庫0 (売れない)」と「在庫が分からない」は別の意味で、
 *    0 に倒すと画面が「在庫切れの赤字」を作り出してしまう。
 *    引き当て超過で負になることは実際にあるので、負は落とさずそのまま持つ
 */
function intOrNull(v) {
  if (v == null || v === '') return null;
  const n = Number(v);
  return Number.isInteger(n) ? n : null;
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
    unit_quantity: row.unit_quantity,
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
