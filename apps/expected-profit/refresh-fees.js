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
import { getExpectedProfitDB, getSetting, setSetting, SETTING_AMAZON_SELLER_ID } from './db.js';
import { canReuseFeeEstimate, normalizeFeeEstimate, feeCacheKey } from './calc.js';
import { nowIso, addDays } from './util.js';

/**
 * SP-API の公式値 (2026-09-08 確認)
 *   getMyFeesEstimates : rate 0.5 req/s, burst 1, batch 上限 20
 *   → バッチ間は最低 2,000ms 空ける。ここを詰めると 429 になる
 *   https://developer-docs.amazon.com/sp-api/reference/getmyfeesestimates
 *
 * 🚨 全出品ぶん取り直すと 7,385 ÷ 20 = 370 回 × 2.1 秒 ≒ 13 分。
 *    毎晩これをやると、この 1 機能で日次の枠を食い潰す。**再利用が効いていることが前提**。
 */
export const SP_API_FEES_RATE_PER_SEC = 0.5;   // 公式値 (2026-09-08 確認)
export const SP_API_FEES_MAX_BATCH = 20;       // 公式値
export const BATCH_SIZE = SP_API_FEES_MAX_BATCH;
export const BATCH_SLEEP_MS = 2100;            // = 1/0.5秒 (2,000ms) + 余裕
const MAX_RETRIES = 3;

/**
 * 見積の有効期間。
 *
 * 🚨 全部を同じ日数にすると、同じ晩に取ったものが**同じ晩に一斉失効**する。
 *    14 日ごとに 7,385 件の取り直しが起きる (雪崩)。
 *    SKU ごとに決まったズレを持たせて、失効する晩をばらけさせる。
 *    ズレは SKU から決まるので、同じ SKU は毎回同じ位相になる (毎晩ズレ直さない)。
 */
const FEE_VALID_MIN_DAYS = 7;
const FEE_VALID_SPREAD_DAYS = 8;   // 7〜14 日 → 1 晩あたり約 1/8 が失効
const FEE_VALID_DAYS = FEE_VALID_MIN_DAYS + FEE_VALID_SPREAD_DAYS - 1;   // 上限 = 14 日

/**
 * 1 晩に取り直す上限。
 *
 * 🚨 キーの作り方を間違えると「全件がキャッシュに当たらない」が静かに起きる
 *    (実際に起きた: seller_id が env に無く、キーが null で作られた)。
 *    上限を置いて、1 晩で枠を使い切らないようにする。残りは翌晩に回る。
 *    200 バッチ × 2.1 秒 ≒ 7 分。
 */
const FEE_MAX_FETCH_PER_RUN = 4000;

/** 取り直しが多すぎるときに警告する閾値 (キャッシュが温まっているのに半分以上ならおかしい) */
const REFETCH_ANOMALY_RATIO = 0.5;

/** SKU から決まる 0..(spread-1) のズレ。文字列ハッシュ (djb2) */
export function validityOffsetDays(sellerSku, spread = FEE_VALID_SPREAD_DAYS) {
  let h = 5381;
  const str = String(sellerSku ?? '');
  for (let i = 0; i < str.length; i++) h = ((h * 33) ^ str.charCodeAt(i)) >>> 0;
  return h % spread;
}

/** この SKU の見積が何日もつか (7〜14 日) */
export function feeValidDays(sellerSku) {
  return FEE_VALID_MIN_DAYS + validityOffsetDays(sellerSku);
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

/**
 * 見積が要る対象を洗い出す。
 * 🚨 「キャッシュが無い」だけでなく「入力が変わった」も対象にする
 */
/**
 * いま見積に使っている seller_id。
 *
 * 🚨 env (SP_API_SELLER_ID) より **API が返した値**が正 (§15-3)。
 *    実データ検証で miniPC の env に SP_API_SELLER_ID が無く、
 *    キーが `null` で作られて見積を1件も引けなかった。
 *
 * 🚨 見積テーブルの DISTINCT から推測しない (Codex R7-3)。
 *    旧セラーの行が1つ残っただけで「決められない」に落ち、
 *    env が空の環境では二度とキーを作れなくなる。
 *    覚え書き → env の順で見て、どちらも無ければ null (レスポンスが教えてくれる)。
 */
export function storedSellerId(db, env = process.env.SP_API_SELLER_ID) {
  return getSetting(db, SETTING_AMAZON_SELLER_ID) || env || null;
}

/** レスポンスで分かったセラーを覚える。変わったら記録して呼び出し側に返す */
export function rememberSellerId(db, sellerId, now) {
  if (!sellerId) return { changed: false, previous: null };
  const previous = getSetting(db, SETTING_AMAZON_SELLER_ID);
  if (previous === sellerId) return { changed: false, previous };
  setSetting(db, SETTING_AMAZON_SELLER_ID, sellerId, now || new Date().toISOString());
  return { changed: true, previous };
}

/** 見積入力の seller_id を、保存済みの値で埋める (env が空でもキーが一致するように) */
export function withResolvedSeller(targets, knownSellerId) {
  if (!knownSellerId) return targets;
  return targets.map(t => (t?.seller_id === knownSellerId ? t : { ...t, seller_id: knownSellerId }));
}

/**
 * 何日待ってからやり直すか。
 * 🚨 1回目は翌晩 (一時的な失敗はすぐ回復させる)。続けて失敗するものだけ遠ざける
 */
export const FAILURE_BACKOFF_DAYS = [1, 3, 7, 14];

export function backoffDays(attempts) {
  const i = Math.min(Math.max(attempts, 1), FAILURE_BACKOFF_DAYS.length) - 1;
  return FAILURE_BACKOFF_DAYS[i];
}

/** 待ちの残っている失敗記録 (キー → 記録) */
export function loadFailures(db) {
  const map = new Map();
  try {
    for (const r of db.prepare('SELECT * FROM amazon_fee_failure').all()) map.set(r.fee_key, r);
  } catch { /* テーブルがまだ無い環境 */ }
  return map;
}

/** 失敗を記録して、次に試す時刻を伸ばす */
export function recordFailure(db, target, error, now = new Date()) {
  const key = feeCacheKey(target);
  const prev = db.prepare('SELECT attempts FROM amazon_fee_failure WHERE fee_key = ?').get(key);
  const attempts = (prev?.attempts || 0) + 1;
  const failedAt = now.toISOString();
  db.prepare(`INSERT INTO amazon_fee_failure (fee_key, seller_sku, attempts, last_error, failed_at, retry_after)
              VALUES (?, ?, ?, ?, ?, ?)
              ON CONFLICT(fee_key) DO UPDATE SET attempts = excluded.attempts,
                last_error = excluded.last_error, failed_at = excluded.failed_at,
                retry_after = excluded.retry_after`)
    .run(key, target.seller_sku, attempts, String(error || '').slice(0, 300),
      failedAt, addDays(failedAt, backoffDays(attempts)));
  return attempts;
}

/** 成功したら失敗記録を消す (次からは普通に扱う) */
export function clearFailure(db, target) {
  try { db.prepare('DELETE FROM amazon_fee_failure WHERE fee_key = ?').run(feeCacheKey(target)); }
  catch { /* テーブルが無い環境 */ }
}

/** 取り直しの理由を数える (input_mismatch はどの項目かまでまとめる) */
export function countReasons(need) {
  const out = {};
  for (const n of need) {
    const key = String(n.reason || 'unknown').split(':')[0] === 'input_mismatch'
      ? String(n.reason) : String(n.reason || 'unknown');
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

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
  // 🚨 seller_id はリクエストに含まれない (レスポンスの FeesEstimateIdentifier.SellerId が正)。
  //    ここで必須にすると、env に SP_API_SELLER_ID が無いだけで全件弾かれる (実データで判明)
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
/**
 * カタログID が ASIN の形をしているか。
 *
 * 🚨 ASIN は英数 10 桁 (書籍は ISBN10 がそのまま ASIN になる)。
 *    13 桁の JAN を `IdType: 'ASIN'` で送ると必ず client-side error になる
 *    (実データで 5 件そうなっていた)。公式も「ASIN か SellerSKU。UPC/ISBN 等は不可」
 */
export function looksLikeAsin(v) {
  return /^[A-Z0-9]{10}$/.test(String(v ?? ''));
}

/**
 * 何で商品を指すか。ASIN の形なら ASIN、そうでなければ自社 SKU。
 * 🚨 SellerSKU は公式に認められた IdType (ASIN か SellerSKU の2択)
 */
export function feeIdentifierOf(t) {
  return looksLikeAsin(t.asin)
    ? { IdType: 'ASIN', IdValue: t.asin }
    : { IdType: 'SellerSKU', IdValue: t.seller_sku };
}

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
    ...feeIdentifierOf(t),
  }));
}

/** レスポンスを保存形へ (純関数) */
export function toEstimateRow(target, feesEstimate, fetchedAt, sellerIdFromResponse = null) {
  // 🚨 fulfillment を渡さないと「FBA なのに FBAFees が無い」を検出できない
  const n = normalizeFeeEstimate(feesEstimate, { fulfillment: target.in_fulfillment });
  return {
    // 🚨 実際に見積を返したセラーを保存する (env より API の応答が正)
    seller_id: sellerIdFromResponse || target.seller_id || null,
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
    valid_until: addDays(fetchedAt, feeValidDays(target.seller_sku)),
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
  // 🚨 キャッシュ照合の前に seller_id を揃える。
  //    env が空 / env と API の値が違うと feeCacheKey が一致せず、毎晩 7,597 件を取り直す
  const knownSellerId = storedSellerId(db);
  const resolved = withResolvedSeller(valid, knownSellerId);
  const cached = loadCache(db);
  const planned = planRefresh(resolved, cached, now());
  const reuse = planned.reuse;

  // 🚨 キャッシュが温まっているのに半分以上を取り直すのは、キーの作り方が壊れた合図。
  //    静かに 370 回叩かせず、結果に出す (実際に seller_id で起きた)
  const refetchAnomaly = cached.size > 0 && resolved.length > 0
    && planned.need.length > resolved.length * REFETCH_ANOMALY_RATIO;

  // 取り直す順番: 見積そのものが無い/入力が変わった → 先。期限切れは後。
  // 前者は行が計算できない (キーが当たらない)、後者は数字は出るがランキングに載らないだけ
  const priority = (r) => (r.reason === 'expired' ? 1 : 0);
  // 🚨 前に失敗して、まだ待ち時間が残っているものは今晩は試さない。
  //    85/86 が「終了した出品」で毎晩必ず失敗していた (実データ)。
  //    入力が変われば別キーになるので、値が直れば自動でやり直す
  const failures = loadFailures(db);
  const nowMs = now().getTime();
  const waiting = [];
  const attemptable = [];
  for (const n of planned.need) {
    const f = failures.get(feeCacheKey(n.target));
    const retryAfter = f ? Date.parse(f.retry_after) : NaN;
    if (f && Number.isFinite(retryAfter) && retryAfter > nowMs) waiting.push(n);
    else attemptable.push(n);
  }
  const ordered = attemptable.sort((a, b) => priority(a) - priority(b));
  const maxFetch = deps.maxFetch ?? FEE_MAX_FETCH_PER_RUN;
  const need = ordered.slice(0, maxFetch);
  const deferred = ordered.length - need.length;   // 上限で翌晩に回した数

  const callApi = deps.callFeesApi || defaultCallFeesApi;
  const sleepMs = deps.sleepMs ?? BATCH_SLEEP_MS;
  const saved = [];
  const errors = [];          // 対象 (SKU) 単位の失敗
  const batchErrors = [];     // バッチ単位の失敗 (API 呼び出しそのものが通らなかった)
  let stoppedByDeadline = false;
  let processedTargets = 0;
  // 🚨 「応答が名乗ったセラー」と「既に知っているセラー」を混ぜない (Codex R8-1)。
  //    既知で初期化すると、セラーが正しく1つに切り替わった実行まで競合扱いになり、
  //    覚え書きが古いまま残って世代構築が古いキーを使い続ける
  const observedSellers = new Set();     // この実行で応答が名乗ったセラー

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
        const msg = r?.Error?.Message || r?.Status || 'no estimate';
        const attempts = recordFailure(db, chunk[j], msg, now());
        errors.push({ sku: chunk[j].seller_sku, error: msg, attempts });
        continue;
      }
      clearFailure(db, chunk[j]);   // 直ったら記録を消す
      const sellerIdFromResponse = r?.FeesEstimateIdentifier?.SellerId || null;
      if (sellerIdFromResponse) observedSellers.add(sellerIdFromResponse);
      saved.push(toEstimateRow(chunk[j], r.FeesEstimate, fetchedAt, sellerIdFromResponse));
    }
    if (i + BATCH_SIZE < need.length) await sleep(sleepMs);
  }

  // 応答が名乗ったセラーが**1つに決まったときだけ**、補完して覚える。
  // 🚨 複数のセラーが混ざった実行では補完も記憶もしない (どれが正か決められない)。
  //    応答が1つなら、それが既知と違っても採用する = セラーの切り替えに追従できる (Codex R8-1)
  // 応答が誰も名乗らなかった場合は、入力側 (withResolvedSeller) が既知セラーで
  // 埋めているのでここでの補完は要らない (逆検証で到達しないことを確認済み)
  const sellerConflict = observedSellers.size > 1;
  const observedSellerId = observedSellers.size === 1 ? [...observedSellers][0] : null;
  if (observedSellerId) {
    for (const row of saved) if (!row.seller_id) row.seller_id = observedSellerId;
    // 次回の照合に使えるよう覚えておく (env に無くてもキーが作れる)
    rememberSellerId(db, observedSellerId, nowIso());
  }
  const unresolvedSeller = saved.filter(r => !r.seller_id);
  if (unresolvedSeller.length > 0) {
    // セラーが分からない見積は保存しない (キーが作れない)
    for (const r of unresolvedSeller) errors.push({ sku: r.seller_sku, error: 'seller_id_unresolved' });
  }
  const savable = saved.filter(r => r.seller_id);
  if (savable.length > 0) saveEstimates(db, savable);
  const unknownTypes = savable.filter(r => r.fee_status === 'unknown_fee_type').length;
  const inconsistent = savable.filter(r => r.fee_status === 'inconsistent').length;
  const badStatus = savable.filter(r => r.fee_status !== 'ok').length;
  const failedTargets = errors.length + batchErrors.reduce((a, b) => a + b.targets, 0);
  return {
    targets: targets.length,
    deferred,                 // 1晩の上限で翌晩に回した数
    waitingOnFailure: waiting.length,   // 前に失敗して待ち中 (今晩は試さない)
    refetchAnomaly,           // 🚨 キャッシュが効いていない合図
    plannedRefetch: ordered.length,
    // 取り直しの理由の内訳。キャッシュが当たらない原因が入力なのか期限なのか分かる
    refetchReasons: countReasons(ordered),
    sellerId: observedSellerId || (sellerConflict ? null : knownSellerId),
    sellerConflict,
    observedSellers: [...observedSellers],
    invalidTargets: invalid.length,
    invalid: invalid.slice(0, 20),
    reused: reuse.length,
    refreshed: savable.length,
    // 🚨 SKU 単位とバッチ単位を混ぜない (取得率の判断に使えなくなる)
    failedTargets,
    failedBatches: batchErrors.length,
    pendingTargets: need.length - processedTargets,   // 期限で止めた分
    okEstimates: savable.length - badStatus,
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
