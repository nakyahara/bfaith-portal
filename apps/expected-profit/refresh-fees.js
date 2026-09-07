/**
 * 商品別 想定利益 — Amazon 手数料の再見積もり (miniPC で動く)
 *
 * 正本 = §7.3 / §15-1 / §15-6 / §15-13
 *
 * 🚨 既存の warehouse/fetch-amazon-fees.js とは別物。
 *    あちらは daily-sync (07:00) が「速報粗利用のキャッシュ」を更新するもので、
 *    - Shipping を常に 0 で見積もる
 *    - price_used は「直近に売れた価格」
 *    - 保存は seller_sku 1行だけ (見積入力を持たない)
 *    本バッチは **見積入力をキーにして持ち**、入力が1つでも違えば取り直す (§7.3)。
 *
 * 🚨 送料も販売手数料の算定基礎に入る (実測: 1,198円の FBM で Shipping 0 → 101、230 → 120)。
 *    送料別途の出品は in_shipping = 標準送料 で見積もる (§15-13)。
 */
import { getExpectedProfitDB } from './db.js';
import { canReuseFeeEstimate, normalizeFeeEstimate, feeCacheKey } from './calc.js';
import { nowIso, addDays } from './util.js';

const FEE_VALID_DAYS = 14;
const BATCH_SIZE = 20;          // getMyFeesEstimates の batch 上限
const BATCH_SLEEP_MS = 2100;    // restore_rate=2 (0.5 RPS) + 余裕
const MAX_RETRIES = 3;

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

/**
 * 見積が要る対象を洗い出す。
 * 🚨 「キャッシュが無い」だけでなく「入力が変わった」も対象にする
 */
export function planRefresh(targets, cachedByKey, now = new Date()) {
  const need = [];
  const reuse = [];
  for (const t of targets) {
    const key = feeCacheKey(t);
    const cached = cachedByKey.get(key);
    const verdict = canReuseFeeEstimate(cached, t, now);
    if (verdict.reuse) reuse.push({ target: t, cached });
    else need.push({ target: t, reason: verdict.reason });
  }
  return { need, reuse };
}

/**
 * 見積入力が揃っているか。
 * 🚨 欠損を 0 や既定値で埋めて API を呼ばない (Codex R2)。
 *    埋めると「実際とは違う条件の見積」を正常値として保存してしまう
 */
export function validateFeeTarget(t) {
  const missing = [];
  if (!t?.seller_id) missing.push('seller_id');
  if (!t?.marketplace_id) missing.push('marketplace_id');
  if (!t?.seller_sku) missing.push('seller_sku');
  if (!t?.asin) missing.push('asin');
  if (!Number.isInteger(t?.in_listing_price) || t.in_listing_price <= 0) missing.push('in_listing_price');
  if (!Number.isInteger(t?.in_shipping) || t.in_shipping < 0) missing.push('in_shipping');
  if (!Number.isInteger(t?.in_points) || t.in_points < 0) missing.push('in_points');
  if (t?.in_fulfillment !== 'FBA' && t?.in_fulfillment !== 'FBM') missing.push('in_fulfillment');
  if (!t?.in_currency) missing.push('in_currency');
  return missing.length === 0 ? { ok: true } : { ok: false, missing };
}

export function cacheKey(t) {
  return [t.seller_id, t.marketplace_id, t.seller_sku, t.in_listing_price,
    t.in_shipping, t.in_points, t.in_fulfillment].join('');
}

export function loadCache(db) {
  const rows = db.prepare('SELECT * FROM amazon_fee_estimate').all();
  const map = new Map();
  for (const r of rows) map.set(feeCacheKey(r), r);
  return map;
}

/**
 * SP-API のリクエスト body を組む (純関数。テストで固定する)
 * 🚨 marketplace は target のものを使う。環境変数を使うと
 *    「保存した条件」と「実際に送った条件」がずれ、後日の全入力一致検証が嘘になる (Codex R1-6)
 */
export function buildFeeRequest(targets) {
  return targets.map((t, idx) => ({
    FeesEstimateRequest: {
      MarketplaceId: t.marketplace_id,
      IsAmazonFulfilled: t.in_fulfillment === 'FBA',
      PriceToEstimateFees: {
        ListingPrice: { CurrencyCode: t.in_currency, Amount: t.in_listing_price },
        Shipping: { CurrencyCode: t.in_currency, Amount: t.in_shipping },
        // ポイントは見積入力に含める (§15-1)。
        // 🚨 || 0 で埋めない。欠損は validateFeeTarget が先に弾く
        Points: { PointsNumber: t.in_points },
      },
      Identifier: `${t.seller_sku}|${idx}|${Date.now()}`,
    },
    IdType: 'ASIN',
    IdValue: t.asin,
  }));
}

/** レスポンスを保存形へ (純関数) */
export function toEstimateRow(target, feesEstimate, fetchedAt) {
  // 🚨 fulfillment を渡さないと「FBA なのに FBAFees が無い」を検出できない
  const n = normalizeFeeEstimate(feesEstimate, { fulfillment: target.in_fulfillment });
  return {
    seller_id: target.seller_id,
    marketplace_id: target.marketplace_id,
    seller_sku: target.seller_sku,
    asin: target.asin,
    in_listing_price: target.in_listing_price,
    in_shipping: target.in_shipping,
    in_points: target.in_points,
    in_fulfillment: target.in_fulfillment,
    in_currency: target.in_currency,
    referral_fee_ex_tax: n.referral,
    closing_fee_ex_tax: n.closing,
    per_item_fee_ex_tax: n.perItem,
    fba_fee_incl_tax: n.fbaInclTax,
    total_fees_estimate: n.total,
    fee_breakdown: JSON.stringify(feesEstimate?.FeeDetailList ?? []),
    fee_status: n.status,
    fetched_at: fetchedAt,
    valid_until: addDays(fetchedAt, FEE_VALID_DAYS),
  };
}

export function saveEstimates(db, rows) {
  const stmt = db.prepare(`
    INSERT OR REPLACE INTO amazon_fee_estimate
      (seller_id, marketplace_id, seller_sku, asin, in_listing_price, in_shipping, in_points,
       in_fulfillment, in_currency, referral_fee_ex_tax, closing_fee_ex_tax, per_item_fee_ex_tax,
       fba_fee_incl_tax, total_fees_estimate, fee_breakdown, fee_status, fetched_at, valid_until)
    VALUES
      (@seller_id, @marketplace_id, @seller_sku, @asin, @in_listing_price, @in_shipping, @in_points,
       @in_fulfillment, @in_currency, @referral_fee_ex_tax, @closing_fee_ex_tax, @per_item_fee_ex_tax,
       @fba_fee_incl_tax, @total_fees_estimate, @fee_breakdown, @fee_status, @fetched_at, @valid_until)
  `);
  const tx = db.transaction((list) => { for (const r of list) stmt.run(r); });
  tx(rows);
}

/**
 * 再見積もりを実行する。
 * @param {object} db
 * @param {Array} targets 見積入力 (seller_id/marketplace_id/seller_sku/asin/in_* を持つ)
 * @param {object} deps { callFeesApi, now, sleepMs, deadline }
 */
export async function refreshFees(db, targets, deps = {}) {
  const now = deps.now || (() => new Date());
  // 🚨 順序が大事 (Codex R3): 入力検証 → 欠損対象の除外 → 有効対象だけ marketplace 比較。
  //    先に marketplace を見ると、marketplace_id が欠けた1件で全体が例外になり、
  //    正常な対象まで処理されず invalidTargets にも残らない
  const invalid = [];
  const valid = [];
  for (const t of targets) {
    const v = validateFeeTarget(t);
    if (v.ok) valid.push(t);
    else invalid.push({ sku: t?.seller_sku ?? null, missing: v.missing });
  }
  // 入力は揃っているが、環境と違う marketplace のものは混ぜない
  // (送った条件と保存する条件がずれると、全入力一致の検証が意味を失う)
  const envMarketplace = process.env.SP_API_MARKETPLACE_ID || 'A1VC38T7YXB528';
  const mismatched = valid.filter(t => t.marketplace_id !== envMarketplace);
  if (mismatched.length > 0) {
    throw new Error(
      `marketplace_id が環境設定 (${envMarketplace}) と違う対象が ${mismatched.length} 件あります: `
      + `${[...new Set(mismatched.map(t => t.marketplace_id))].join(', ')}`);
  }
  const cached = loadCache(db);
  const { need, reuse } = planRefresh(valid, cached, now());

  const callApi = deps.callFeesApi || defaultCallFeesApi;
  const sleepMs = deps.sleepMs ?? BATCH_SLEEP_MS;
  const saved = [];
  const errors = [];          // 対象 (SKU) 単位の失敗
  const batchErrors = [];     // バッチ単位の失敗 (API 呼び出しそのものが通らなかった)
  let stoppedByDeadline = false;
  let processedTargets = 0;

  for (let i = 0; i < need.length; i += BATCH_SIZE) {
    // 全体終了期限 (§8.4)。超えたら残りは翌日に回す (途中で止めても行は消えない)
    if (deps.deadline && now() >= deps.deadline) { stoppedByDeadline = true; break; }

    const chunk = need.slice(i, i + BATCH_SIZE).map(x => x.target);
    const body = buildFeeRequest(chunk);
    let res;
    let apiThrew = false;
    for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
      // 🚨 リトライの途中でも期限を見る。バックオフ待ちの間に期限を越えることがある
      if (deps.deadline && now() >= deps.deadline) { stoppedByDeadline = true; break; }
      try { res = await callApi(body); apiThrew = false; break; }
      catch (e) {
        apiThrew = true;
        if (attempt === MAX_RETRIES - 1) {
          batchErrors.push({ targets: chunk.length, skus: chunk.map(c => c.seller_sku).slice(0, 5), error: e.message });
        } else await sleep(sleepMs * (attempt + 1));   // 指数バックオフ
      }
    }
    if (stoppedByDeadline) break;
    processedTargets += chunk.length;
    if (apiThrew) continue;                 // 例外側は既に batchErrors に積んである
    // 🚨 例外を投げずに null / 非配列を返す API も「バッチ失敗」として数える。
    //    ここを素通りさせると対象が集計から消える (Codex R2)
    if (!Array.isArray(res)) {
      batchErrors.push({ targets: chunk.length, skus: chunk.map(c => c.seller_sku).slice(0, 5), error: 'レスポンスが配列でない' });
      continue;
    }

    const fetchedAt = nowIso();
    const byIdentifier = new Map();
    for (const r of res) {
      const id = r?.FeesEstimateIdentifier?.SellerInputIdentifier;
      if (id) byIdentifier.set(id, r);
    }
    for (let j = 0; j < chunk.length; j++) {
      const identifier = body[j].FeesEstimateRequest.Identifier;
      const r = byIdentifier.get(identifier);
      if (!r || r.Status !== 'Success' || !r.FeesEstimate) {
        errors.push({ sku: chunk[j].seller_sku, error: r?.Error?.Message || r?.Status || 'no estimate' });
        continue;
      }
      saved.push(toEstimateRow(chunk[j], r.FeesEstimate, fetchedAt));
    }
    if (i + BATCH_SIZE < need.length) await sleep(sleepMs);
  }

  if (saved.length > 0) saveEstimates(db, saved);
  const unknownTypes = saved.filter(r => r.fee_status === 'unknown_fee_type').length;
  const inconsistent = saved.filter(r => r.fee_status === 'inconsistent').length;
  const badStatus = saved.filter(r => r.fee_status !== 'ok').length;
  const failedTargets = errors.length + batchErrors.reduce((a, b) => a + b.targets, 0);
  return {
    targets: targets.length,
    invalidTargets: invalid.length,
    invalid: invalid.slice(0, 20),
    reused: reuse.length,
    refreshed: saved.length,
    // 🚨 SKU 単位とバッチ単位を混ぜない (取得率の判断に使えなくなる)
    failedTargets,
    failedBatches: batchErrors.length,
    pendingTargets: need.length - processedTargets,   // 期限で止めた分
    okEstimates: saved.length - badStatus,
    unknownTypes,
    inconsistent,
    stoppedByDeadline,
    errors: errors.slice(0, 20),
    batchErrors: batchErrors.slice(0, 5),
  };
}

async function defaultCallFeesApi(body) {
  const SellingPartner = (await import('amazon-sp-api')).default;
  const sp = new SellingPartner({
    region: 'fe',
    refresh_token: process.env.SP_API_REFRESH_TOKEN,
    credentials: {
      SELLING_PARTNER_APP_CLIENT_ID: process.env.SP_API_CLIENT_ID,
      SELLING_PARTNER_APP_CLIENT_SECRET: process.env.SP_API_CLIENT_SECRET,
      AWS_ACCESS_KEY_ID: process.env.AWS_ACCESS_KEY_ID,
      AWS_SECRET_ACCESS_KEY: process.env.AWS_SECRET_ACCESS_KEY,
    },
  });
  return sp.callAPI({ operation: 'getMyFeesEstimates', endpoint: 'productFees', body });
}
