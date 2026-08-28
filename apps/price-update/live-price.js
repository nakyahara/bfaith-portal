/**
 * price-update / ライブの設定価格取得 (要件定義 v1.0 F2)
 *
 * 「今モールに登録されている売価」を API から取る。mirror の日次スナップショットは
 * 更新判断の基準にしない (古い値を基準に値付けすると、別の誰かの変更を踏み潰す)。
 *
 *   楽天  : miniPC /service-api/rakuten-rms/items/details-bulk → variants[sku].standardPrice
 *   Yahoo : VPS /yahoo/get-item-detail → Price (+ SubCodes[].Price)
 *   Amazon: mirror_amazon_price_snapshot_daily (日次・表示のみ。更新しないのでライブ取得は不要)
 *   auPAY / Qoo10: 価格を出さない (要件 F2)
 *
 * 🚨M0実測: 楽天 GET の standardPrice は**文字列**で返る ("1000")。
 *    ここで整数化しておかないと、M2 の楽観ロック照合が全件 conflict になる。
 *    整数として読めない値は null にする (「読めた」ことにして値付けの基準にしない)。
 *
 * ★ライブ価格が取れた行だけを confirmed に昇格させる (= カタログAPIで実在確認済み)。
 *   引き当てマスタに載っているだけの行は rule のまま = 更新不可。
 */
import { fetchItemDetailsBulkDetailed, fetchAllItemCodes } from '../rakuten-yahoo-sync/lib/rakuten-rms-proxy.js';
import { fetchYahooItemDetail } from '../rakuten-yahoo-sync/lib/yahoo-detail-proxy.js';
import { normCode } from './resolve.js';

/** itemNumber → manageNumber の対応表はそう変わらないので短時間だけ使い回す */
const CODE_MAP_TTL_MS = 30 * 60 * 1000;
let _codeMap = null;
let _codeMapAt = 0;

/** 文字列でも数値でも受けて、整数円として読めた時だけ数値を返す (読めなければ null) */
export function toIntPrice(v) {
  if (typeof v === 'number') return Number.isInteger(v) ? v : (Number.isFinite(v) && Number.isInteger(Math.round(v)) && Math.abs(v - Math.round(v)) < 1e-9 ? Math.round(v) : null);
  if (typeof v !== 'string') return null;
  const s = v.trim();
  if (!/^\d+$/.test(s)) return null;
  const n = Number(s);
  return Number.isSafeInteger(n) ? n : null;
}

/** itemNumber → manageNumber (miniPC cache 経由)。失敗しても致命にしない */
async function itemNumberToManageNumber(deps) {
  const now = deps.now ? deps.now() : Date.now();
  if (_codeMap && now - _codeMapAt < CODE_MAP_TTL_MS) return _codeMap;
  try {
    const mapping = await (deps.fetchAllItemCodes || fetchAllItemCodes)();
    const m = new Map();
    for (const [itemNumber, manageNumber] of Object.entries(mapping || {})) {
      if (itemNumber && manageNumber) m.set(normCode(itemNumber), String(manageNumber));
    }
    _codeMap = m;
    _codeMapAt = now;
    return m;
  } catch (e) {
    console.warn('[price-update] all-codes 取得に失敗 (出品コードを管理番号として扱う):', e.message);
    return _codeMap || new Map();
  }
}

/** テスト用: キャッシュを捨てる */
export function _resetCodeMapCache() { _codeMap = null; _codeMapAt = 0; }

/**
 * 楽天のライブ価格。listingCode は AM/AL/W いずれか (mirror_rakuten_sku_map 由来)。
 *
 * @param {string[]} listingCodes
 * @returns {Promise<Map<string, {price:number|null, manageNumber:string|null, skuCode:string|null,
 *                                found:boolean, reason:string|null, itemTitle:string|null}>>}
 *   キーは listingCode の正規化キー
 */
export async function fetchRakutenPrices(listingCodes, deps = {}) {
  const out = new Map();
  const codes = [...new Set((listingCodes || []).map((c) => String(c || '').trim()).filter(Boolean))];
  if (codes.length === 0) return out;

  const codeMap = await itemNumberToManageNumber(deps);
  // listingCode → 問い合わせる manageNumber (対応表に無ければ「コード自体が管理番号」とみなす)
  const wanted = new Map();
  for (const c of codes) {
    const mn = codeMap.get(normCode(c)) || c;
    if (!wanted.has(mn)) wanted.set(mn, []);
    wanted.get(mn).push(c);
  }

  const fetchBulk = deps.fetchItemDetailsBulkDetailed || fetchItemDetailsBulkDetailed;
  const { items, failed } = await fetchBulk([...wanted.keys()]);
  const failedByCode = new Map(failed.map((f) => [normCode(f.manageNumber), f.reason]));

  for (const item of items || []) {
    const mn = item?.manageNumber || '';
    const originals = wanted.get(mn) || wanted.get(String(mn)) || [];
    const variants = item?.variants && typeof item.variants === 'object' ? item.variants : {};
    for (const original of originals) {
      const key = normCode(original);
      // variant の特定: SKU管理番号 (variants のキー) か システム連携SKU番号 で一致する行
      let matchedKey = null;
      for (const [vk, v] of Object.entries(variants)) {
        if (normCode(vk) === key || normCode(v?.merchantDefinedSkuId) === key) { matchedKey = vk; break; }
      }
      // 商品番号/管理番号で引いた場合は variant が1つだけなら確定できる。
      // 複数 variant があるのに特定できない = SKU を取り違える危険があるので確定させない
      if (!matchedKey) {
        const vkeys = Object.keys(variants);
        if (vkeys.length === 1) matchedKey = vkeys[0];
      }
      if (!matchedKey) {
        out.set(key, {
          price: null, manageNumber: mn, skuCode: null, found: false,
          reason: Object.keys(variants).length === 0 ? 'SKU情報が取得できません' : 'どのSKUか特定できません (SKU管理番号で指定してください)',
          itemTitle: item?.title || null,
        });
        continue;
      }
      const price = toIntPrice(variants[matchedKey]?.standardPrice);
      out.set(key, {
        price,
        manageNumber: mn,
        skuCode: matchedKey,
        found: price != null,
        reason: price == null ? '設定価格を整数円として読めません' : null,
        itemTitle: item?.title || null,
      });
    }
  }

  for (const [mn, originals] of wanted) {
    for (const original of originals) {
      const key = normCode(original);
      if (out.has(key)) continue;
      out.set(key, {
        price: null, manageNumber: mn, skuCode: null, found: false,
        reason: failedByCode.get(normCode(mn)) || '楽天に見つかりません',
        itemTitle: null,
      });
    }
  }
  return out;
}

/**
 * Yahoo のライブ価格。sub_code 別価格があれば SubCodes に入る。
 * ★VPS 側の Price 抽出 (PR2) が未デプロイだと Price は undefined で返る。
 *   その場合は「取得できない」として扱い、confirmed に昇格させない (古い値で値付けしない)。
 */
export async function fetchYahooPrices(itemCodes, deps = {}) {
  const out = new Map();
  const codes = [...new Set((itemCodes || []).map((c) => String(c || '').trim()).filter(Boolean))];
  const fetchOne = deps.fetchYahooItemDetail || fetchYahooItemDetail;
  for (const c of codes) {
    const key = normCode(c);
    try {
      const d = await fetchOne(c);
      const price = toIntPrice(d?.Price);
      const subCodes = Array.isArray(d?.SubCodes)
        ? d.SubCodes.map((s) => ({ subCode: s?.SubCode || null, price: toIntPrice(s?.Price) })).filter((s) => s.subCode)
        : [];
      out.set(key, {
        price,
        subCodes,
        found: price != null,
        reason: price == null
          ? (d?.Price === undefined ? '価格が返ってきません (VPS プロキシの Price 抽出が未デプロイの可能性)' : '設定価格を整数円として読めません')
          : null,
        itemName: d?.Name || null,
      });
    } catch (e) {
      out.set(key, { price: null, subCodes: [], found: false, reason: e?.message || 'Yahooから取得できません', itemName: null });
    }
  }
  return out;
}

/** Amazon: 日次スナップショットの最新 (表示のみ・更新対象外) */
export function loadAmazonSnapshot(db, sellerSkus) {
  const out = new Map();
  const skus = [...new Set((sellerSkus || []).map((s) => String(s || '').trim()).filter(Boolean))];
  if (skus.length === 0) return out;
  const stmt = db.prepare(`
    SELECT seller_sku, asin, my_price, buybox_price, date_jst, fetched_at
      FROM mirror_amazon_price_snapshot_daily
     WHERE LOWER(TRIM(seller_sku)) = ?
     ORDER BY date_jst DESC LIMIT 1
  `);
  for (const sku of skus) {
    const r = stmt.get(normCode(sku));
    if (r) {
      out.set(normCode(sku), {
        price: r.my_price == null ? null : Math.round(Number(r.my_price)),
        buyboxPrice: r.buybox_price == null ? null : Math.round(Number(r.buybox_price)),
        asin: r.asin || null,
        dateJst: r.date_jst,
        fetchedAt: r.fetched_at,
      });
    }
  }
  return out;
}
