/**
 * price-update / ライブの設定価格取得 (要件定義 v1.0 F2)
 *
 * 「今モールに登録されている売価」を API から取る。mirror の日次スナップショットは
 * 更新判断の基準にしない (古い値を基準に値付けすると、別の誰かの変更を踏み潰す)。
 *
 *   楽天  : miniPC /service-api/rakuten-rms/items/details-bulk → variants[sku].standardPrice
 *   Yahoo : VPS /yahoo/get-item-detail → Price (+ SubCodes[].Price)
 *   Amazon: mirror_amazon_price_snapshot_daily (日次・表示のみ。更新しないのでライブ取得は不要)
 *   auPAY : VPS /wmshopapi/searchItemInfo → itemPrice (2026-09-02〜)
 *   Qoo10 : 価格を出さない (書き込み経路がまだ無い)
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
import { fetchAupayItemDetail } from './aupay-apply.js';
import { normCode } from './resolve.js';

/** itemNumber → manageNumber の対応表はそう変わらないので短時間だけ使い回す */
const CODE_MAP_TTL_MS = 30 * 60 * 1000;
let _codeMap = null;
let _codeMapAt = 0;

/** 文字列でも数値でも受けて、整数円として読めた時だけ数値を返す (読めなければ null) */
export function toIntPrice(v) {
  // 数値も「整数そのもの」だけ受ける。1000.0000000001 を 1000 に丸めると、
  // 監査に残る値と楽観ロックの基準値が実際の設定価格とずれる (Codex R2)
  if (typeof v === 'number') return Number.isInteger(v) ? v : null;
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
 * 楽天 variant の発送設定を読む (要件外だが、モール間の売価差の理由が発送方法にあるため — 中原さん 8/31)。
 * 楽天が持っているのは「配送方法セットの番号」で、名前 (定形外郵便 等) は RMS のマスタ側にある。
 * ここでは持っている値をそのまま渡し、画面には番号と送料込みかどうかを出す。
 */
export function shippingOfRakutenVariant(v) {
  const s = v?.shipping;
  if (!s || typeof s !== 'object') return null;
  return {
    methodGroup: s.shippingMethodGroup == null ? null : String(s.shippingMethodGroup),
    postageIncluded: typeof s.postageIncluded === 'boolean' ? s.postageIncluded : null,
    singleItemShipping: s.singleItemShipping ?? null,
    deliveryDateId: v?.normalDeliveryDateId ?? null,
  };
}

/**
 * 楽天のライブ価格。
 *
 * @param {Array<{key:string, aliases?:string[], manageNumber?:string|null, manageNumbers?:string[], rowKey?:string}>} targets
 *   key      = 表示中の出品コード (別名の1つとしても使う)
 *   aliases  = AM/AL/W の別名 (mirror_rakuten_sku_map 由来)。variant の特定に使う
 *   skuAliases = そのうち SKU 単位のもの (AM/AL。対応表の source で分けたもの)。variant の照合に使う
 *   amAliases  = そのうち AM (システム連携用SKU番号) 由来のもの。当たった variant の AM はこれとだけ照合する
 *   manageNumber  = 対応表が持っている商品管理番号 (あればこれを最優先で使う)
 *   manageNumbers = 対応表に複数の商品管理番号があった時の一覧 (2つ以上なら取り違え防止で確定しない)
 *   rowKey   = 結果を引くためのキー。★色違いは同じ商品管理番号を共有するので、
 *              出品コードだけをキーにすると同じプレビュー内の別の行と衝突する。省略時は key の正規化
 * @returns {Promise<Map<string, {price:number|null, manageNumber:string|null, skuCode:string|null,
 *                                found:boolean, reason:string|null, itemTitle:string|null}>>}
 */
export async function fetchRakutenPrices(targets, deps = {}) {
  const out = new Map();
  const list = (targets || [])
    .map((t) => ({
      key: normCode(t.key),
      rowKey: t.rowKey ? String(t.rowKey) : normCode(t.key),
      aliases: [...new Set([t.key, ...(t.aliases || [])].map((a) => String(a || '').trim()).filter(Boolean))],
      skuAliases: [...new Set((t.skuAliases || []).map((a) => String(a || '').trim()).filter(Boolean))],
      amAliases: [...new Set((t.amAliases || []).map((a) => String(a || '').trim()).filter(Boolean))],
      manageNumber: String(t.manageNumber || '').trim() || null,
      manageNumbers: [...new Set((t.manageNumbers || []).map((m) => String(m || '').trim()).filter(Boolean))],
    }))
    .filter((t) => t.key);
  if (list.length === 0) return out;

  // ★商品管理番号の決め方 (優先順):
  //   1. 対応表の manage_number (RMS の全SKU一覧から取った値。AM/AL の行にも入っている — 2026-09-01)
  //   2. 別名のどれかが itemNumber として all-codes にあれば、その manageNumber
  //   3. 別名そのもの (単品はコード = 管理番号のことが多い)
  //   1 で全部決まるなら all-codes は取りに行かない
  const needCodeMap = list.some((t) => !t.manageNumber && t.manageNumbers.length <= 1);
  const codeMap = needCodeMap ? await itemNumberToManageNumber(deps) : new Map();
  const wanted = new Map();   // manageNumber → [target...]
  for (const t of list) {
    if (t.manageNumbers.length > 1) {
      // 同じ NE コードが複数の楽天商品に紐づいている。どちらか決めずに未確定 (取り違え防止)
      out.set(t.rowKey, {
        price: null, manageNumber: null, skuCode: null, found: false,
        reason: `このNEコードは複数の楽天商品に紐づいています (${t.manageNumbers.join(', ')})。どちらか特定できないため確定しません`,
        itemTitle: null,
      });
      continue;
    }
    let mn = t.manageNumber;
    if (!mn) for (const a of t.aliases) { const hit = codeMap.get(normCode(a)); if (hit) { mn = hit; break; } }
    if (!mn) mn = t.aliases[0];
    t.manageNumber = mn;
    if (!wanted.has(mn)) wanted.set(mn, []);
    wanted.get(mn).push(t);
  }
  if (wanted.size === 0) return out;

  const fetchBulk = deps.fetchItemDetailsBulkDetailed || fetchItemDetailsBulkDetailed;
  const { items, failed } = await fetchBulk([...wanted.keys()]);
  const failedByCode = new Map((failed || []).map((f) => [normCode(f.manageNumber), f.reason]));
  const itemByMn = new Map((items || []).map((it) => [normCode(it?.manageNumber), it]));

  for (const t of list) {
    if (out.has(t.rowKey)) continue;   // 上で確定しないと決めた行
    const item = itemByMn.get(normCode(t.manageNumber));
    if (!item) {
      out.set(t.rowKey, {
        price: null, manageNumber: t.manageNumber, skuCode: null, found: false,
        reason: failedByCode.get(normCode(t.manageNumber)) || '楽天に見つかりません',
        itemTitle: null,
      });
      continue;
    }
    const variants = item?.variants && typeof item.variants === 'object' ? item.variants : {};
    // variant の特定: SKU管理番号 (variants のキー) か システム連携用SKU番号 (AM) が、
    // ★SKU 単位の別名 (AM/AL。対応表の source で分けたもの) のどれかと一致すること。
    //   ページ単位の値 (商品番号 = 管理番号) は照合に使わない — 偶然それを SKU管理番号や AM に持つ
    //   別の variant に当たりうる (Codex R6)。SKU 単位かどうかは値で推定しない (Codex R5)
    const skuAliasKeys = new Set(t.skuAliases.map(normCode));
    const variantCount = Object.keys(variants).length;
    // ★対応表でこの NE コードに AM (= SKU) が 2 つ以上紐づいている = 同じ商品を 2 つの SKU で出している。
    //   片方が消えた後に残った方へ当ててしまうと「対応表を作った時と違う状態」を確定させることになる (Codex R8)。
    //   両方残っていても「別名が複数の SKU に一致」で確定しない。どちらにせよ人が見るべき状態
    if (t.amAliases.length > 1) {
      out.set(t.rowKey, {
        price: null, manageNumber: item.manageNumber || t.manageNumber, skuCode: null, found: false,
        reason: `対応表でこの NE コードに複数のシステム連携用SKU番号 (${t.amAliases.join(', ')}) が紐づいています。`
          + 'どの SKU が正しいか決められないため確定しません (楽天側の重複出品を整理してください)',
        itemTitle: item?.title || null,
      });
      continue;
    }
    const matched = [];
    for (const [vk, v] of Object.entries(variants)) {
      if (skuAliasKeys.has(normCode(vk)) || skuAliasKeys.has(normCode(v?.merchantDefinedSkuId))) matched.push(vk);
    }
    let matchedKey = matched.length === 1 ? matched[0] : null;
    // SKU 単位の別名が 1 つも当たらない:
    //   - 対応表が SKU 単位の別名を持っている → 記録していた SKU がこの商品から消えている (差し替わった疑い)。確定しない (Codex R4)
    //   - 持っていない (W 行だけ。例: SKU管理番号 normal-inventory の単品) → variant が 1 つだけなら取り違える相手がいないので採用
    let missingSkuAliases = [];
    if (matched.length === 0) {
      missingSkuAliases = t.skuAliases;
      if (missingSkuAliases.length === 0 && variantCount === 1) matchedKey = Object.keys(variants)[0];
    }
    if (!matchedKey) {
      let reason;
      if (matched.length > 1) reason = `別名が複数のSKUに一致しました (${matched.join(', ')})。取り違えを避けるため確定しません`;
      else if (variantCount === 0) reason = 'SKU情報が取得できません';
      else if (missingSkuAliases.length > 0) {
        reason = `対応表に記録された SKU (${missingSkuAliases.join(', ')}) がこの商品に見当たりません。`
          + '対応表を作った後に SKU が差し替わった疑いがあるため確定しません (翌朝の再構築後に再確認してください)';
      } else reason = `どのSKUか特定できません (この商品には ${variantCount} SKU あります)`;
      out.set(t.rowKey, {
        price: null, manageNumber: item.manageNumber || t.manageNumber, skuCode: null, found: false,
        reason, itemTitle: item?.title || null,
      });
      continue;
    }
    // ★取り違えの最終防衛 (Codex R2〜R7): 当たった variant のシステム連携用SKU番号 (AM) を、
    //   対応表の **AM 由来の別名とだけ** 照合する (AL と混ぜない — AL と同じ値の AM が後から付いた場合を区別するため)。
    //   対応表を作った後に SKU が別商品へ移り、空いた SKU管理番号に別の SKU が入った、という隙を塞ぐ。
    //   AM は店舗内で一意なので、これが違えば別の SKU。対応表を作った時と AM の有無・値が違えば、
    //   同じ SKU と言い切れないので確定しない (翌朝の再構築で対応表が追いつけば通る)。
    //   - AM があるのに対応表に AM が無い → 作った時には無かった AM。確定しない
    //   - AM が対応表の AM と違う → 差し替わった疑い。確定しない
    //   - AM が無いのに対応表には AM があった → 消えた or 別の SKU。確定しない
    //   - どちらも無い → 複数SKUの商品では同一性を確かめる手段が無いので確定しない。
    //     単一SKUの商品だけ通す (取り違える相手がいない。実データ: 複数SKU商品で AM 空は 0 件・単一SKUでは 1,652 件)
    const liveAm = String(variants[matchedKey]?.merchantDefinedSkuId || '').trim();
    const amAliasKeys = new Set(t.amAliases.map(normCode));
    let identityProblem = null;
    if (liveAm && amAliasKeys.size === 0) {
      identityProblem = `SKU ${matchedKey} にシステム連携用SKU番号 (${liveAm}) がありますが、対応表を作った時には無かったものです。`
        + '対応表を作った後に変わった疑いがあるため確定しません (翌朝の再構築後に再確認してください)';
    } else if (liveAm && !amAliasKeys.has(normCode(liveAm))) {
      identityProblem = `SKU ${matchedKey} のシステム連携用SKU番号 (${liveAm}) が対応表のもの (${t.amAliases.join(', ')}) と違います。`
        + '対応表を作った後に SKU が差し替わった疑いがあるため確定しません (翌朝の再構築後に再確認してください)';
    } else if (!liveAm && amAliasKeys.size > 0) {
      identityProblem = `SKU ${matchedKey} にシステム連携用SKU番号がありません (対応表を作った時は ${t.amAliases.join(', ')} でした)。`
        + '対応表を作った後に変わった疑いがあるため確定しません (翌朝の再構築後に再確認してください)';
    } else if (!liveAm && variantCount > 1) {
      identityProblem = `SKU ${matchedKey} にシステム連携用SKU番号がありません。複数SKU (${variantCount}) の商品では同じ SKU か確かめられないため確定しません`
        + ' (RMS でこの SKU にシステム連携用SKU番号を設定してください)';
    }
    if (identityProblem) {
      out.set(t.rowKey, {
        price: null, manageNumber: item.manageNumber || t.manageNumber, skuCode: null, found: false,
        reason: identityProblem, itemTitle: item?.title || null,
      });
      continue;
    }
    const price = toIntPrice(variants[matchedKey]?.standardPrice);
    out.set(t.rowKey, {
      price,
      manageNumber: item.manageNumber || t.manageNumber,
      skuCode: matchedKey,
      found: price != null,
      reason: price == null ? '設定価格を整数円として読めません' : null,
      itemTitle: item?.title || null,
      // 発送設定 (SKU単位)。同じ商品でもモールで配送方法が違い、それが売価差の理由になる
      shipping: shippingOfRakutenVariant(variants[matchedKey]),
    });
  }
  return out;
}

/**
 * Yahoo のライブ価格。sub_code 別価格があれば SubCodes に入る。
 * ★VPS 側の Price 抽出 (PR2) が未デプロイだと Price は undefined で返る。
 *   その場合は「取得できない」として扱い、confirmed に昇格させない (古い値で値付けしない)。
 */
export async function fetchYahooPrices(targets, deps = {}) {
  const out = new Map();
  // targets = [{ key, candidates: [問い合わせる item_code 候補] }]
  const list = (targets || [])
    .map((t) => ({
      key: normCode(t.key),
      candidates: [...new Set([t.key, ...(t.candidates || [])].map((c) => String(c || '').trim()).filter(Boolean))],
    }))
    .filter((t) => t.key);
  if (list.length === 0) return out;

  const fetchOne = deps.fetchYahooItemDetail || fetchYahooItemDetail;
  const cache = new Map();   // 同じ item_code を何度も叩かない (親コードは複数のカラーで共有される)

  async function detailOf(code) {
    const k = normCode(code);
    if (!cache.has(k)) {
      try { cache.set(k, { ok: true, d: await fetchOne(code) }); }
      catch (e) { cache.set(k, { ok: false, e }); }
    }
    return cache.get(k);
  }

  for (const t of list) {
    const reasons = [];
    let resolved = null;
    for (const cand of t.candidates) {
      const got = await detailOf(cand);
      if (!got.ok) {
        // Yahoo は「その item_code の商品が無い」も 400 で返す。生の HTTP 400 では読めない
        const msg = String(got.e?.message || '');
        reasons.push(/HTTP 400\b/.test(msg)
          ? `${cand}: この出品コードの商品が見つかりません`
          : `${cand}: ${msg || '取得できません'}`);
        continue;
      }
      const d = got.d;
      if (d?.ok === false) { reasons.push(`${cand}: Yahoo が ok を返しませんでした`); continue; }

      const itemPrice = toIntPrice(d?.Price);
      const subCodes = Array.isArray(d?.SubCodes)
        ? d.SubCodes.map((s) => ({ subCode: s?.SubCode == null ? null : String(s.SubCode), price: toIntPrice(s?.Price) })).filter((s) => s.subCode)
        : [];
      // ★実在確認は「探しているコード (= NEコード) が、応答の ItemCode か SubCodes にある」ことで行う。
      // 親コードで問い合わせた場合は SubCodes 側で当たる (カラバリはこの形で登録されている)。
      // どちらにも無ければ、その候補は別商品なので使わない (取り違え防止)
      const matchedSub = subCodes.find((s) => normCode(s.subCode) === t.key) || null;
      const itemMatches = normCode(d?.ItemCode) === t.key;
      if (!matchedSub && !itemMatches) {
        reasons.push(`${cand}: この商品には ${t.key} が含まれていません`);
        continue;
      }

      const pricedSubs = subCodes.filter((s) => s.price != null);
      let price = null;
      let skuCode = null;
      let reason = null;
      // カラバリで当たった個別商品コード (画面に出す。更新のキーには使わない)
      let matchedSubCode = null;
      // 商品価格を共有する個別商品コード = 専用価格を持たないもの。**変えると全部が変わる**
      let sharedSubCodes = [];
      if (matchedSub) {
        matchedSubCode = matchedSub.subCode;
        if (matchedSub.price != null) {
          // この色だけ専用の価格が入っている。商品価格を送っても直らないので触らない (fail-closed)
          reason = `この個別商品には専用の価格 (${matchedSub.price} 円) が入っています。`
            + '商品の価格を変えてもこの色には効かないため、いまの版では更新できません';
        } else if (pricedSubs.length > 0) {
          // ★自分は継承でも、同じ商品に専用価格の色が混ざっている。
          //   商品価格を変えると継承している色だけが動き、専用価格の色は取り残される。
          //   送信側 (yahoo-apply) はこの商品を丸ごと拒否するので、
          //   ここで確定させると「画面では更新できるのに、送ると必ず失敗する」になる (Codex R1 中)
          reason = `この商品には専用の価格が入っている色があります `
            + `(${pricedSubs.map((x) => x.subCode).join(', ')})。`
            + '商品の価格を変えてもその色には効かないため、いまの版では更新できません';
        } else if (itemPrice == null) {
          reason = '設定価格を整数円として読めません';
        } else {
          price = itemPrice;
          // ★Yahoo は「商品」に1つの価格しか持たない。個別商品コード (色) は商品価格を継承する。
          //   だから更新のキーは **商品コード** にする。子コードをキーにすると
          //   「送るのは商品価格なのに、送った後の照合は子コードで探す」ことになり必ず食い違う
          //   (実測: 全色の価格が変わったうえで failed として記録される)
          skuCode = d?.ItemCode || cand;
          sharedSubCodes = subCodes.filter((s) => s.price == null).map((s) => s.subCode);
        }
      } else if (pricedSubs.length > 0) {
        // 商品コードでは一致したが、SKU別価格を持つ商品 → どのSKUの価格か決められない (fail-closed)
        reason = `SKU別価格のある商品です (${pricedSubs.map((s) => s.subCode).join(', ')})。どのSKUかを特定できないため更新対象にできません`;
      } else if (itemPrice == null) {
        reason = d?.Price === undefined
          ? '価格が返ってきません (VPS プロキシの Price 抽出が未デプロイの可能性)'
          : '設定価格を整数円として読めません';
      } else {
        price = itemPrice;
        // ★SKU別価格が無い普通の商品は、商品コードそのものを SKU のキーにする (Codex R1 High)。
        //   ここを null のままにすると、実行時に "null" という文字列が価格のキーになり、
        //   送った後の照合が必ず食い違う (更新は通っているのに失敗として記録される)
        skuCode = d?.ItemCode || cand;
        sharedSubCodes = subCodes.filter((s) => s.price == null).map((s) => s.subCode);
      }
      resolved = {
        price, subCodes, skuCode, matchedSubCode, sharedSubCodes,
        itemCode: d?.ItemCode || cand,
        found: price != null,
        reason,
        itemName: d?.Name || null,
        // 発送設定 (商品単位)。Delivery = 配送方法、PostageSet = 送料設定、ShipWeight = 配送重量
        shipping: (d?.Delivery != null || d?.PostageSet != null || d?.ShipWeight != null)
          ? { delivery: d.Delivery ?? null, postageSet: d.PostageSet ?? null, shipWeight: d.ShipWeight ?? null }
          : null,
      };
      break;   // 当たった候補で確定 (以降の候補は試さない)
    }
    out.set(t.key, resolved || {
      price: null, subCodes: [], skuCode: null, itemCode: null, found: false,
      reason: reasons.length ? reasons.join(' / ') : 'Yahooから取得できません',
      itemName: null,
    });
  }
  return out;
}

/**
 * au PAY: searchItemInfo で商品ごとの設定価格を取る。
 *
 * ★au PAY は Yahoo と同じで「商品」に1つの価格しか持たない。カラバリは
 *   registerStock 側にあり在庫だけで、価格の項目が無い (2026-09-01 実測)。
 *   つまり **色ごとの値付けはできず、変えるとその商品の全ての色が同じ価格になる**。
 *   送り先のキーは商品コード。ここを取り違えると、送った後の照合が必ず食い違う
 *   ([[feedback_mall_price_granularity_differs]] と同じ罠)。
 *
 * @param {Array<{key:string, candidates:string[]}>} targets
 * @param {object} [deps] テスト用の差し替え ({ fetchAupayItemDetail })
 * @returns {Promise<Map<string, object>>} key → { price, skuCode, itemCode, found, reason, ... }
 */
export async function fetchAupayPrices(targets, deps = {}) {
  const out = new Map();
  const list = (targets || [])
    .map((t) => ({
      key: normCode(t.key),
      candidates: [...new Set([t.key, ...(t.candidates || [])].map((c) => String(c || "").trim()).filter(Boolean))],
    }))
    .filter((t) => t.key);
  if (list.length === 0) return out;

  const fetchOne = deps.fetchAupayItemDetail || fetchAupayItemDetail;
  const gapMs = Number.isInteger(deps.gapMs) ? deps.gapMs : 900;   // 既存の au PAY 取得と同じ間隔
  const cache = new Map();

  async function detailOf(code) {
    const k = normCode(code);
    if (!cache.has(k)) {
      try { cache.set(k, { ok: true, d: await fetchOne(code) }); }
      catch (e) { cache.set(k, { ok: false, e }); }
      if (gapMs > 0) await new Promise((r) => setTimeout(r, gapMs));
    }
    return cache.get(k);
  }

  for (const t of list) {
    const reasons = [];
    let resolved = null;
    for (const cand of t.candidates) {
      const got = await detailOf(cand);
      if (!got.ok) { reasons.push(`${cand}: ${got.e?.message || "取得できません"}`); continue; }
      const d = got.d;
      if (!d || d.ok === false) {
        reasons.push(`${cand}: ${d?.message || "au PAY が ok を返しませんでした"}`);
        continue;
      }
      // ★取り違え防止: 応答の商品コードが **問い合わせた候補** と一致すること。
      //   探しているキー (t.key) と比べると、候補がキーと違う書き方の時に
      //   正しい商品を「別の商品」として捨ててしまう (Codex R1 中)。
      //   候補がそのキーの出品であることは、候補を組み立てた側 (resolve) が保証する
      if (normCode(d.itemCode) !== normCode(cand)) {
        reasons.push(`${cand}: 別の商品が返りました (応答 ${d.itemCode ?? 'なし'})`);
        continue;
      }
      let price = null;
      let reason = null;
      if (d.itemPrice == null) {
        reason = d.itemPriceReadable
          ? "設定価格を整数円として読めません"
          : "価格が返ってきません";
      } else {
        price = d.itemPrice;
      }
      resolved = {
        price,
        // ★送り先は商品コード。au PAY は商品に1つの価格しか持たない
        skuCode: d.itemCode,
        itemCode: d.itemCode,
        found: price != null,
        reason,
        itemName: d.itemName || null,
        // 変えると影響を受ける組み合わせの数。0 = カラバリ無し / null = 数えられなかった。
        // ★null を 0 に潰さない。潰すと「本当はカラバリがあるのに警告が出ない」が起きる (Codex R1)
        choiceCount: d.choiceCount === null || d.choiceCount === undefined ? null : d.choiceCount,
        saleStatus: d.saleStatus || null,
        lotNumber: d.lotNumber || null,
        shipping: (d.postageSegment != null || d.deliveryMethodName != null)
          ? { postageSegment: d.postageSegment, postage: d.postage, methodName: d.deliveryMethodName }
          : null,
      };
      break;
    }
    out.set(t.key, resolved || {
      price: null, skuCode: null, itemCode: null, found: false,
      reason: reasons.length ? reasons.join(" / ") : "au PAY から取得できません",
      itemName: null, choiceCount: 0,
    });
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
