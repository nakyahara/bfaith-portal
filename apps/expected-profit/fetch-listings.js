/**
 * 商品別 想定利益 — 出品列挙 + 価格取得 (miniPC で動く)
 *
 * 正本 = AI_reference『商品別想定利益_要件定義_20260907.md』§5 / §15-4
 *
 * 🚨 母集団は「出品列挙の結果」。売れた実績や手数料キャッシュを起点にしない (§5.1)。
 *    起点を間違えると「売れていない商品が出ない」という元の問題が再発する。
 *
 * 🚨 部分列挙で見つからないことを、削除・未出品の根拠にしない (§5.2.1)。
 *    今夜が partial なら前回の完全集合と UNION し、見つからなかった行は前回の期限を引き継ぐ。
 *
 * 使い方:
 *   node apps/expected-profit/fetch-listings.js            (全モール)
 *   node apps/expected-profit/fetch-listings.js --mall amazon     (amazon / rakuten / yahoo)
 */
import { getExpectedProfitDB, initExpectedProfitDB } from './db.js';
import { newRunId, nowIso, addDays, canonicalShopId, UNKNOWN_SELLER } from './util.js';
import { storedSellerId } from './refresh-fees.js';
import { archiveItems } from '../../scripts/mall-items/archive-items.mjs';

// 失効期限 (§15-8)
const LISTING_ENUM_VALID_DAYS = 7;
const PRICE_VALID_DAYS = 3;

/** 前回の完全集合から消えた率がこれを超えたら partial 扱い (レポート破損の疑い) */
const DISAPPEARED_RATIO_LIMIT = 0.20;

/**
 * Amazon の shop_id = `<セラーID>@<マーケットプレイスID>`。出品の鍵 (shop_id + SKU) の一部になる。
 *
 * 🚨 セラーID は手数料の見積と同じ出どころ (覚え書き → env。refresh-fees.js storedSellerId) から取る。
 *    env だけを見ていた頃は、env に無い回が `unknown@…` になり、env に入った夜から鍵が総入れ替えになった
 *    (2026-09-09 夜〜9/14、Amazon が全部「判定できない」。util.js canonicalShopId の説明を参照)
 */
export function amazonShopId(db) {
  return `${storedSellerId(db) || UNKNOWN_SELLER}@${process.env.SP_API_MARKETPLACE_ID || 'A1VC38T7YXB528'}`;
}
const RAKUTEN_SHOP_ID = () => process.env.RAKUTEN_SHOP_CODE || '1';
/** Yahoo! ショッピングのストアアカウント。出品の鍵 (shop_id + ItemCode) の一部になる */
const YAHOO_SHOP_ID = () => process.env.YAHOO_STORE_ACCOUNT || 'b-faith01';

/**
 * Yahoo myItemList の網羅集合。
 * 🚨 正本は RYS の CANONICAL_QUERIES (yahoo-store-sync.js)。**同じ 36 本でなければ取りこぼす**ので、
 *    片方だけ変えないこと (試験が両者の一致を見張っている)
 */
export const YAHOO_QUERIES = 'abcdefghijklmnopqrstuvwxyz0123456789'.split('');
/** 詳細取得の同時実行数。RYS の yahoo-detail-proxy と同じ 3 に揃える */
const YAHOO_DETAIL_CONCURRENCY = 3;

/**
 * 出品の鍵 (前回集合との突き合わせに使う)。
 *
 * 🚨 **ここ以外で鍵を組み立てない** (Codex R1 P1 2026-09-19)。
 *    区切りに使っている U+001F は**画面に見えない文字**なので、既存行を目で写すと黙って落ちる。
 *    落ちると「前回の集合と 1 件も一致しない」= 毎晩 partial になり、完全集合が二度と更新されない。
 *    実際 Yahoo を足したときに踏んだ (同じ集合を 2 回取ると 2 回目が partial になった)。
 */
export function snapshotKey(shopId, mallItemKey) {
  return String(shopId) + SNAPSHOT_KEY_SEP + String(mallItemKey);
}
const SNAPSHOT_KEY_SEP = String.fromCharCode(0x1f);

/** 整数円として読めた時だけ返す (price-update の toIntPrice と同じ規約) */
export function toIntPrice(v) {
  if (typeof v === 'number') return Number.isInteger(v) ? v : null;
  if (typeof v !== 'string') return null;
  const s = v.trim().replace(/,/g, '');
  if (!/^\d+$/.test(s)) return null;
  const n = Number(s);
  return Number.isSafeInteger(n) ? n : null;
}

/** Amazon 出品レポートの「ステータス」列 → listing_status */
export function amazonListingStatus(raw) {
  const s = String(raw || '').trim().toLowerCase();
  if (s === 'active') return 'active';
  if (s === 'inactive') return 'inactive';
  if (s === 'incomplete') return 'incomplete';
  return 'unknown';
}

/**
 * フルフィルメント・チャンネル列 → FBA / FBM。
 * 🚨 欠損・未知を FBM に倒さない (Codex R3)。FBA の行を FBM として計算すると
 *    配送費 (fba_fee か自社配送関係費か) も費用範囲も変わり、利益の符号と順位が狂う
 */
export function amazonFulfillment(raw) {
  const s = String(raw ?? '').trim().toUpperCase();
  if (!s) return null;                                   // 列が無い / 空 = 未解決
  if (s.includes('AMAZON') || s.startsWith('AFN')) return 'FBA';
  if (s === 'DEFAULT' || s.startsWith('MFN') || s.includes('MERCHANT')) return 'FBM';
  return null;                                           // 未知の値も未解決 (勝手に決めない)
}

/** レポート行 → snapshot 行 (純関数。テストで固定する) */
/**
 * Amazon の出品が「送料込み」か。
 *
 * 🚨 FBA はプライム配送なので常に送料込み。
 * 🚨 FBM (自社出荷) は**配送パターン**で決まる (中原さん決定 2026-09-08)。
 *    実測 (7,597 出品): FBM 3,466 件のうち
 *      ネコポスマケプレプライム設定             3,377
 *      移行された配送パターン                     72
 *      ヤマト北海道・沖縄送料別途設定               11
 *      Selected Self-Ship Template…               3
 *      プライム配送パターン                        3
 *    マケプレプライムは**プライム会員への配送料無料が条件** (沖縄・北海道・離島を除く)。
 *    標準シナリオ (本州・1個・通常購入) では送料込みとして扱う。
 *    → 中原さんの決定 = **送料込みとして扱う**。楽天と同じ扱いに揃える。
 *
 * 🚨 知らない配送パターンは **null (不明)** にする。勝手に送料込みへ倒さない。
 *    新しい配送パターンを作ったときに、黙って利益を高く見せないため。
 */
/**
 * 🚨 ここに載せてよいのは「**プライム扱い = 配送料無料が条件**」と根拠が言えるものだけ。
 *    「移行された配送パターン」(72件) は名前からは何も分からないので**入れない** (Codex R12)。
 *    根拠なく入れると、送料を別途もらっている出品の利益を高く見せてしまう。
 *    → 参考値 (`shipping_revenue_unknown`) のままにして、中原さんの確認を待つ。
 */
export const AMAZON_POSTAGE_INCLUDED_GROUPS = [
  'ネコポスマケプレプライム設定',                                  // 実測 3,377 件
  'プライム配送パターン',                                          // 実測 3 件
  'Selected Self-Ship Templateネコポスマケプレプライム設定',       // 実測 3 件
];

export function amazonPostageIncluded(fulfillment, shippingGroup) {
  if (fulfillment === 'FBA') return true;
  if (fulfillment !== 'FBM') return null;              // 出荷区分が未解決なら判断しない
  const g = String(shippingGroup ?? '').trim();
  if (!g) return null;                                  // 配送パターンが読めない
  if (AMAZON_POSTAGE_INCLUDED_GROUPS.includes(g)) return true;
  return null;                                          // 知らないパターンは不明のまま
}

export function amazonRowToSnapshot(row, { runId, shopId, fetchedAt, validUntil }) {
  const sku = (row['出品者SKU'] || row['seller-sku'] || '').trim();
  const asin = (row['商品ID'] || row['asin1'] || '').trim();
  const price = toIntPrice(row['価格'] ?? row['price']);
  const channel = row['フルフィルメント・チャンネル'] ?? row['fulfillment-channel'];
  const fulfillment = amazonFulfillment(channel);
  // 🚨 「明示的な0」と「列そのものが無い/読めない」を分ける (Codex R1-3)。
  //    ここで 0 に倒すと、ポイント付き出品を誤った条件で見積もってしまう
  const shippingGroup = row['merchant-shipping-group'] ?? row['配送パターン'];
  const postageIncluded = amazonPostageIncluded(fulfillment, shippingGroup);
  const pointsRaw = row['ポイント'] ?? row['points'];
  const hasPointsColumn = pointsRaw !== undefined;
  const points = hasPointsColumn && String(pointsRaw).trim() === '' ? 0 : toIntPrice(pointsRaw);
  return {
    run_id: runId,
    mall: 'amazon',
    shop_id: shopId,
    mall_item_key: sku,
    mall_item_ref: asin,                 // 手数料見積の入力キー (ASIN 単位で引く)
    mall_item_number: null,              // 楽天だけが持つ (商品番号)
    fulfillment,                         // null = 未解決 (見積も計算も通さない)
    ne_code: null,                       // 対応付けは build 側で行う
    price_type: 'normal',
    price_incl_tax: price,
    price_tax_included: 1,               // Amazon の出品価格は税込 (決済の Principal + Tax と一致)
    price_raw: price,
    mall_tax_rate: null,                 // Amazon は税率を返さない (商品マスタ側を使う)
    // 送料込みかどうかは配送パターンで決める (上の amazonPostageIncluded を見よ)
    postage_included: postageIncluded == null ? null : (postageIncluded ? 1 : 0),
    postage_revenue_incl_tax: postageIncluded === true ? 0 : null,
    shipping_group: shippingGroup == null ? null : String(shippingGroup).trim() || null,
    points,                              // null = 取得不能 (見積入力未解決として扱う)
    listing_status: amazonListingStatus(row['ステータス'] || row['status']),
    fetch_status: price == null ? 'not_found' : 'ok',
    resolve_status: 'unresolved',        // build 側で解決する
    resolve_reason: null,
    valid_until: validUntil,
    source: 'merchant_listings_all_data',
    fetched_at: fetchedAt,
  };
}

/** 楽天 items/search の 1 商品 → snapshot 行 (variant ごとに1行) */
export function rakutenItemToSnapshots(item, { runId, shopId, fetchedAt, validUntil }) {
  const r = rakutenItemToSnapshotsDetailed(item, { runId, shopId, fetchedAt, validUntil });
  return r.rows;
}

/**
 * 変換結果と「解析できなかった variant の数」を一緒に返す。
 * 🚨 行が1つでもできれば OK とすると、壊れた variant が静かに消えて
 *    欠落した集合が次の「完全集合」になる (Codex R3)
 */
export function rakutenItemToSnapshotsDetailed(item, { runId, shopId, fetchedAt, validUntil }) {
  const manageNumber = String(item?.manageNumber || '').trim();
  if (!manageNumber) return { rows: [], unparsable: 1 };
  // 🚨 variants は「SKU管理番号 → variant」のオブジェクト。
  //    配列で来たら添字が SKU 管理番号になってしまうので、解析失敗として扱う (Codex R2)
  const v0 = item?.variants;
  if (!v0 || typeof v0 !== 'object' || Array.isArray(v0)) return { rows: [], unparsable: 1 };
  const variants = v0;
  let unparsable = 0;
  // 🚨 payment は **item レベル** にある (variant には無い)。
  //    実データで確認: item のキー = manageNumber, ..., payment, ..., variants
  //    ここを variant から読むと、全出品が「税区分が不明」になって1件も計算できない
  const payment = item?.payment && typeof item.payment === 'object' ? item.payment : null;
  const itemTaxIncluded = payment?.taxIncluded;
  const itemTaxRate = payment?.taxRate != null ? Number(payment.taxRate) : null;
  const hideItem = item?.hideItem === true;
  // 商品番号 (W)。1 商品ページに 1 つ
  const itemNumber = String(item?.itemNumber || '').trim() || null;
  const out = [];
  for (const [variantKey, v] of Object.entries(variants)) {
    // 要素が object でない / キーが空 は解析不能 (行を作らない = 呼び出し側が unparsable に数える)
    if (!variantKey || !v || typeof v !== 'object' || Array.isArray(v)) { unparsable++; continue; }
    const price = toIntPrice(v?.standardPrice);            // 🚨 文字列で返る ("1080")
    // 税区分は item レベル。variant 側にあれば (details-bulk 等) そちらを優先する
    const taxRate = v?.payment?.taxRate != null ? Number(v.payment.taxRate) : itemTaxRate;
    // 🚨 standardPrice が税込とは限らない (Codex R1-5)。taxIncluded を確認し、
    //    税抜登録や不明な区分は「価格が読めなかった」扱いにして、後段で税を二重に割り戻さない
    const taxIncluded = v?.payment?.taxIncluded ?? itemTaxIncluded;
    const postageIncluded = typeof v?.shipping?.postageIncluded === 'boolean' ? v.shipping.postageIncluded : null;
    const singleItemShipping = toIntPrice(v?.shipping?.singleItemShipping);
    const hidden = hideItem || v?.hidden === true;
    out.push({
      run_id: runId,
      mall: 'rakuten',
      shop_id: shopId,
      mall_item_key: `${manageNumber}/${variantKey}`,
      mall_item_ref: v?.merchantDefinedSkuId || null,
      // 🚨 商品番号。システム連携用SKU番号が空欄のときの紐づけ先 (中原さん 2026-09-09)。
      //    item レベルにあるので variant ごとに同じ値が入る
      mall_item_number: itemNumber,
      fulfillment: 'self',
      ne_code: null,
      price_type: 'normal',
      price_incl_tax: taxIncluded === true ? price : null,
      price_tax_included: taxIncluded === true ? 1 : (taxIncluded === false ? 0 : null),
      price_raw: price,                                   // 元の値は残す (調査用)
      mall_tax_rate: Number.isFinite(taxRate) ? taxRate : null,
      postage_included: postageIncluded == null ? null : (postageIncluded ? 1 : 0),
      // 送料込みなら収入 0。別途なら singleItemShipping (取れなければ NULL = unknown)
      postage_revenue_incl_tax: postageIncluded === true ? 0 : singleItemShipping,
      points: 0,
      listing_status: hidden ? 'hidden' : 'active',
      fetch_status: price == null ? 'not_found'
        : (taxIncluded === true ? 'ok' : 'tax_included_unknown'),
      resolve_status: 'unresolved',
      resolve_reason: null,
      valid_until: validUntil,
      shipping_group: null,              // 楽天に配送パターンの概念は無い
      source: 'rms_items_search',
      fetched_at: fetchedAt,
    });
  }
  return { rows: out, unparsable };
}

// ────────────────────────────────────────────────────────────
// Yahoo!ショッピング (2026-09-19 中原さん要望「Yahoo ショッピングも入れたい」)
// ────────────────────────────────────────────────────────────

/**
 * Yahoo の「送料込みか」を配送設定から決める。
 *
 * 🚨 **知らない値は null (不明) にする**。Amazon の配送パターンと同じ規約で、
 *    勝手に送料込みへ倒さない (倒すと、送料を別途もらっている出品の利益を高く見せる)。
 *
 * `Delivery` = "1" が送料無料。実測の裏づけ:
 *   - 直近 90 日の Yahoo 実績 (f_yahoo_finance_sku_daily_v1) の送料収入は **0 円** (2026-09-19)
 *   - Yahoo!Phase1a 設計書に「中原さん全送料無料」と記録がある
 *   - 出品サンプル 30 件はすべて Delivery = "1"
 *   それ以外の値の出品が出てきたら、送料収入が不明な参考値として画面に上がる (黙って 0 にしない)
 */
export function yahooPostageIncluded(delivery) {
  const s = String(delivery ?? '').trim();
  if (s === '1') return true;
  return null;                                     // 空も未知の値も「分からない」
}

/**
 * Yahoo の商品詳細 (get-item-detail) → snapshot 行。
 *
 * 🚨 **SubCode を持つ商品は SubCode 単位で行を作り、親の行は作らない**。
 *    SubCode ごとに NE の商品コードが違う = 原価が違うので、親 1 行にまとめると別商品の原価で計算する。
 *    f_yahoo_finance_sku_daily_v1 の粒度 (sub_code があれば sub_code) と揃える。
 *
 * 🚨 SubCode の Price は「親と同額なら null」で返る (実測)。null を 0 円にしない。親の価格を使う。
 *
 * 🚨 **セール価格は採用しない** (§3.2)。楽天も通常価格 (standardPrice) で計算している。
 *    get-item-detail はセールの期間を返さないので、いま適用中かどうかが分からない。
 *    ただし「セールが設定されている」ことは price_type に残す (後から絞り込めるように)。
 *
 * @returns {{rows: object[], unparsable: number}}
 */
export function yahooDetailToSnapshotsDetailed(detail, { runId, shopId, fetchedAt, validUntil }) {
  if (!detail || detail.ok === false) return { rows: [], unparsable: 1 };
  const itemCode = String(detail.ItemCode ?? '').trim();
  if (!itemCode) return { rows: [], unparsable: 1 };

  const parentPrice = toIntPrice(detail.Price);
  const postageIncluded = yahooPostageIncluded(detail.Delivery);
  // セール価格。数値で入っていれば「セール設定あり」= 標準シナリオに当てはまらない
  const salePrice = toIntPrice(detail.SalePrice);
  const saleSet = salePrice != null;
  // 🚨 セール価格が読めなかった商品は、通常価格も信用しない (price-update の yahoo-apply と同じ判断)
  const saleUnreadable = detail.SalePriceReadable === false;

  // 🚨 SubCodes は **配列でなければ解析失敗**。undefined も null も「SubCode 0 件」と混同しない
  //    (Codex R1 P1 2026-09-19: RYS の詳細クライアントは非配列を undefined に均すので、
  //     `!= null` で守っていると本番経路では素通りし、子商品が親 1 行に化けていた)。
  //    SubCode を持たない商品は空配列 [] が返る (実測 2026-09-19)
  if (!Array.isArray(detail.SubCodes)) return { rows: [], unparsable: 1 };
  const subs = detail.SubCodes;

  const make = (key, price) => ({
    run_id: runId,
    mall: 'yahoo',
    shop_id: shopId,
    mall_item_key: key,
    mall_item_ref: null,
    mall_item_number: itemCode,           // 親の商品コード (SubCode 行から親をたどれるように)
    fulfillment: 'self',
    ne_code: null,
    // 🚨 計算に使うのは通常価格。セールが設定されていることだけ残す (絞り込めるように)
    price_type: saleSet ? 'normal_sale_set' : 'normal',
    // 🚨 Yahoo の価格は税込。税率は API が返さないので NE 商品マスタのものを使う (mall_tax_rate は null)
    price_incl_tax: saleUnreadable ? null : price,
    price_tax_included: 1,
    price_raw: price,
    mall_tax_rate: null,
    postage_included: postageIncluded == null ? null : (postageIncluded ? 1 : 0),
    postage_revenue_incl_tax: postageIncluded === true ? 0 : null,
    points: 0,
    listing_status: 'active',             // myItemList に出ている = 出品中
    fetch_status: saleUnreadable ? 'sale_price_unreadable' : (price == null ? 'not_found' : 'ok'),
    resolve_status: 'unresolved',
    resolve_reason: null,
    valid_until: validUntil,
    // 画面の「モール側の配送パターン」に出す。送料設定の番号だけでも、
    // 送料無料でない出品が出てきたときに何番の設定かが分かる
    shipping_group: detail.PostageSet == null ? null : `送料設定${String(detail.PostageSet)}`,
    source: 'yahoo_item_detail',
    fetched_at: fetchedAt,
  });

  if (subs.length === 0) return { rows: [make(itemCode, parentPrice)], unparsable: 0 };

  const rows = [];
  let unparsable = 0;
  for (const s of subs) {
    if (!s || typeof s !== 'object') { unparsable++; continue; }
    const subCode = String(s.SubCode ?? '').trim();
    if (!subCode) { unparsable++; continue; }
    // 🚨 **明示的な null のときだけ**「親と同額」。キーごと無いのは応答の形が変わった証拠なので
    //    親の価格で埋めない (Codex R1 P1 2026-09-19)
    if (!Object.hasOwn(s, 'Price')) { unparsable++; continue; }
    const price = s.Price === null ? parentPrice : toIntPrice(s.Price);
    rows.push(make(`${itemCode}/${subCode}`, price));
  }
  // SubCode があるのに 1 行も作れなかった = 応答が壊れている (親の行で代用しない)
  if (rows.length === 0) return { rows: [], unparsable: unparsable || 1 };
  return { rows, unparsable };
}

/**
 * 前回の完全集合と突き合わせて、消えた出品の数と partial 判定を返す (§5.2.1)。
 * 🚨 消えた率が大きいときはレポート破損の疑いなので partial にする。
 *    partial のときは呼び出し側が前回集合と UNION する
 */
export function evaluateEnumeration(previousKeys, currentKeys) {
  // 🚨 0 件を正常として通さない (§7.1)。
  //    「正常に全件列挙した結果の 0 件」と「API がおかしい」を外形では区別できない。
  //    B-Faith が全モールで出品ゼロになることは現実に起きないので、異常として扱う。
  //    (前回集合が無い初回でも同じ。ここを ok にすると、空レポートで世代が全消えする)
  if (!currentKeys || currentKeys.size === 0) {
    return { status: 'failed', disappeared: previousKeys?.size || 0, ratio: 1, reason: 'empty_enumeration' };
  }
  if (!previousKeys || previousKeys.size === 0) {
    return { status: 'ok', disappeared: 0, ratio: 0 };   // 初回は比較対象が無い
  }
  let disappeared = 0;
  for (const k of previousKeys) if (!currentKeys.has(k)) disappeared++;
  const ratio = disappeared / previousKeys.size;
  return {
    status: ratio > DISAPPEARED_RATIO_LIMIT ? 'partial' : 'ok',
    disappeared,
    ratio,
  };
}

/**
 * 解析失敗・重複キーがあれば列挙を partial に落とす (Codex R1-2)。
 * 🚨 20%判定は「前回と比べて消えた」を見る補助的な異常検知であって、
 *    解析が成功したことの代わりにはならない。1行でも読めなければ完全集合を名乗らせない
 */
export function enumStatusWithParseFailures(evalResult, unparsable, duplicates) {
  if (evalResult.status === 'failed') return { ...evalResult, unparsable, duplicates };
  if (unparsable > 0 || duplicates > 0) {
    return { ...evalResult, status: 'partial', unparsable, duplicates, reason: 'parse_failure' };
  }
  return { ...evalResult, unparsable, duplicates };
}

function enumSummary(evalResult) {
  if (evalResult.reason === 'empty_enumeration') return '出品が0件で返った (API異常の疑い。0件を正常として通さない)';
  if (evalResult.reason === 'parse_failure') {
    return `解析できない行 ${evalResult.unparsable} 件 / 重複キー ${evalResult.duplicates} 件 (完全集合として扱わない)`;
  }
  return null;
}

// ────────────────────────────────────────────────────────────
// 実行 (I/O を伴う部分)
// ────────────────────────────────────────────────────────────

function insertSnapshots(db, rows) {
  const stmt = db.prepare(`
    INSERT OR REPLACE INTO mall_price_snapshot
      (run_id, mall, shop_id, mall_item_key, mall_item_ref, mall_item_number, fulfillment, ne_code, price_type, price_incl_tax,
       price_tax_included, price_raw, mall_tax_rate, postage_included, postage_revenue_incl_tax, points, listing_status,
       shipping_group,
       fetch_status, resolve_status, resolve_reason, valid_until, source, fetched_at)
    VALUES
      (@run_id, @mall, @shop_id, @mall_item_key, @mall_item_ref, @mall_item_number, @fulfillment, @ne_code, @price_type, @price_incl_tax,
       @price_tax_included, @price_raw, @mall_tax_rate, @postage_included, @postage_revenue_incl_tax, @points, @listing_status,
       @shipping_group,
       @fetch_status, @resolve_status, @resolve_reason, @valid_until, @source, @fetched_at)
  `);
  const tx = db.transaction((list) => { for (const r of list) stmt.run(r); });
  tx(rows);
}

/**
 * 商品一覧の履歴保存 (Company DB構想 06 §7 Step 0、中原さん決定 2026-09-09 D-17)。
 * 取った応答を丸ごと gz + manifest で残す (scripts/mall-items/archive-items.mjs)。
 *
 * 🚨 保存の失敗で取得結果 (listing_enum_status / price_fetch_run) を変えない (fail-soft)。
 *    結果は戻り値 archive に載せ、nightly が ping の note に写す (ロジザード在庫 run-hourly.ps1 step 2b と同じ)。
 * 🚨 0 件・形式不正のときは呼ばない (呼んでも empty で skip する)。
 * deps.archive === false で止められる (試験・手動)。deps.archive に関数を渡せば差し替え
 */
export async function archiveListings(deps, args) {
  if (deps.archive === false) return { code: 'disabled', action: 'skipped' };
  const fn = typeof deps.archive === 'function' ? deps.archive : archiveItems;
  try {
    // 🚨 offsite (rclone) はここでは行わない。最大 3 分待つ同期処理を取得の途中に挟むと、
    //    06:00 の期限を越えて世代作成が中断する (Codex R1-1)。nightly が公開のあとに残り時間の範囲でまとめて行う
    const r = await fn({ ...args, noOffsite: true });
    return {
      code: r.code, action: r.action, file: r.relFile || null, items: r.items ?? null,
      sameAsPrevious: r.sameAsPrevious ?? null, complete: r.complete ?? null, offsite: r.offsite ?? null,
    };
  } catch (e) {
    return { code: e.code || 'error', action: 'error', error: String(e.message).slice(0, 200) };
  }
}

/**
 * 直近で完全列挙できた run の出品キー集合 (§5.2.1 の「完全集合」)。
 * currentShopId を渡すと、古い run の `unknown@<市場>` を今の shop_id に揃えてから鍵にする
 * (util.js canonicalShopId。揃えないと全出品が「消えた」になり、毎晩 partial から抜けられない)
 */
export function loadLastCompleteKeys(db, mall, currentShopId = null) {
  const run = db.prepare(`
    SELECT run_id FROM price_fetch_run
    WHERE mall = ? AND listing_enum_status = 'ok'
    ORDER BY started_at DESC LIMIT 1
  `).get(mall);
  if (!run) return { runId: null, keys: new Set() };
  const rows = db.prepare('SELECT shop_id, mall_item_key FROM mall_price_snapshot WHERE run_id = ?').all(run.run_id);
  return { runId: run.run_id, keys: new Set(rows.map(r => snapshotKey(canonicalShopId(r.shop_id, currentShopId), r.mall_item_key))) };
}

export async function fetchAmazonListings(db, deps = {}) {
  const runId = newRunId();
  const startedAt = nowIso();
  const shopId = amazonShopId(db);
  db.prepare(`INSERT INTO price_fetch_run (run_id, mall, started_at, status, listing_enum_status)
              VALUES (?, 'amazon', ?, 'running', 'failed')`).run(runId, startedAt);

  try {
    // 🚨 期限を過ぎていたら取得そのものを始めない (Codex R5-3)
    if (deps.deadline && new Date() >= deps.deadline) throw new Error('deadline_exceeded');
    const getReport = deps.getActiveListingsReport
      || (await import('../profit-calculator/sp-api.js')).getActiveListingsReport;
    // includeRawText: 履歴保存のために TSV の原文をもらう (画面向けの経路には付かない)
    const report = await getReport({ deadline: deps.deadline, includeRawText: true });
    // 🚨 レスポンス形式そのものを検証する。{} が返ったのを「0件」として通さない (Codex R1-2)
    if (!report || !Array.isArray(report.listings)) {
      throw new Error('出品レポートの形式が不正 (listings が配列でない)');
    }
    const fetchedAt = nowIso();
    const validUntil = addDays(fetchedAt, PRICE_VALID_DAYS);
    const parsed = report.listings.map(r => amazonRowToSnapshot(r, { runId, shopId, fetchedAt, validUntil }));
    // SKU が読めない行は黙って消さず、件数を残す
    const rows = parsed.filter(r => r.mall_item_key);
    const unparsable = parsed.length - rows.length;

    const currentKeys = new Set(rows.map(r => snapshotKey(r.shop_id, r.mall_item_key)));
    // 🚨 重複キーを INSERT OR REPLACE で隠さない
    const duplicates = rows.length - currentKeys.size;
    const prev = loadLastCompleteKeys(db, 'amazon', shopId);
    const evalResult = enumStatusWithParseFailures(
      evaluateEnumeration(prev.keys, currentKeys), unparsable, duplicates);

    // 履歴保存 (Step 0)。レポートは 1 文書なので取得範囲は常に完走 (complete=true)。
    // 原文 (rawText) があればそのまま、無ければ (試験の差し替え) 解析済みの行を NDJSON で
    const hasRaw = typeof report.rawText === 'string';
    const archive = await archiveListings(deps, {
      mall: 'amazon', shopId, source: 'merchant_listings_all_data', runId, fetchedAt,
      format: hasRaw ? 'tsv' : 'ndjson',
      payload: hasRaw ? report.rawText : report.listings,
      items: report.listings.length,
      meta: { complete: true, enum_status: evalResult.status, api_version: report.apiVersion || null },
    });

    insertSnapshots(db, rows);
    db.prepare(`UPDATE price_fetch_run SET finished_at = ?, status = ?, listing_enum_status = ?,
                expected_count = ?, fetched_count = ?, failed_count = ?, disappeared_count = ?,
                error_summary = ? WHERE run_id = ?`)
      .run(nowIso(), evalResult.status, evalResult.status,
        rows.length, rows.filter(r => r.fetch_status === 'ok').length,
        rows.filter(r => r.fetch_status !== 'ok').length, evalResult.disappeared,
        enumSummary(evalResult), runId);
    return { runId, count: rows.length, unparsable, duplicates, ...evalResult, archive };
  } catch (e) {
    db.prepare(`UPDATE price_fetch_run SET finished_at = ?, status = 'failed', listing_enum_status = 'failed',
                error_summary = ? WHERE run_id = ?`).run(nowIso(), String(e.message).slice(0, 500), runId);
    throw e;
  }
}

export async function fetchRakutenListings(db, deps = {}) {
  const runId = newRunId();
  const startedAt = nowIso();
  const shopId = RAKUTEN_SHOP_ID();
  db.prepare(`INSERT INTO price_fetch_run (run_id, mall, started_at, status, listing_enum_status)
              VALUES (?, 'rakuten', ?, 'running', 'failed')`).run(runId, startedAt);

  try {
    // 🚨 既存の /items/all-codes・/items/all-skus は 100頁 = 10,000件で打ち切る (§15-4)。
    //    ここでは /items/search を自分でページングし、打ち切りを partial として検出する
    const searchPage = deps.searchPage || defaultRakutenSearchPage;
    const fetchedAt = nowIso();
    const validUntil = addDays(fetchedAt, PRICE_VALID_DAYS);
    const rows = [];
    const rawItems = [];                      // 履歴保存用: 応答の要素をそのまま (解析前の姿)
    let cursorMark = '*';
    let pages = 0;
    let unparsable = 0;
    let deadlineHit = false;
    const MAX_PAGES = deps.maxPages || 500;   // 50,000 商品。到達したら partial (打ち切りを隠さない)
    let truncated = false;
    const archiveArgs = (meta) => ({
      mall: 'rakuten', shopId, source: 'rms_items_search', runId, fetchedAt,
      format: 'ndjson', payload: rawItems, sortKey: (r) => (r?.item || r)?.manageNumber,
      items: rawItems.length,
      meta: { api_version: 'es/2.0 items/search', ...meta },
    });
    try {
      for (;;) {
        if (pages >= MAX_PAGES) { truncated = true; break; }
        // 🚨 ページごとに期限を見る。1ページ目だけ見ても、39ページ回る間に期限を越える
        if (deps.deadline && new Date() >= deps.deadline) { truncated = true; deadlineHit = true; break; }
        const data = await searchPage(cursorMark);
        pages++;
        // 🚨 形式を検証する。results も items も無いレスポンスを「0件」として通さない
        const items = Array.isArray(data?.results) ? data.results
          : (Array.isArray(data?.items) ? data.items : null);
        if (items === null) throw new Error(`RMS items/search の形式が不正 (${pages}頁目)`);
        for (const r of items) {
          rawItems.push(r);
          const item = r?.item || r;
          const made = rakutenItemToSnapshotsDetailed(item, { runId, shopId, fetchedAt, validUntil });
          // 🚨 商品まるごとの失敗も、variant 単位の失敗も数える
          unparsable += made.unparsable;
          rows.push(...made.rows);
        }
        const next = data?.nextCursorMark;
        if (!next || next === cursorMark || items.length === 0) break;
        cursorMark = next;
      }
    } catch (e) {
      // 途中のページで落ちた (503 など)。取得は失敗のまま (外側で price_fetch_run を failed にする) だが、
      // 取れた分は complete=false の証拠として残す (Codex R1-5)。0 件なら保存器が empty で skip する
      if (rawItems.length > 0) {
        await archiveListings(deps, archiveArgs({
          complete: false, enum_status: 'failed', pages, truncated: true, deadline_hit: false,
          note: `途中で失敗 (${pages}頁目まで取得): ${String(e.message).slice(0, 120)}`,
        }));
      }
      throw e;
    }

    const currentKeys = new Set(rows.map(r => snapshotKey(r.shop_id, r.mall_item_key)));
    const duplicates = rows.length - currentKeys.size;
    const prev = loadLastCompleteKeys(db, 'rakuten');
    const evalResult = enumStatusWithParseFailures(
      evaluateEnumeration(prev.keys, currentKeys), unparsable, duplicates);
    // 0件 (failed) は打ち切りより重い。failed > partial > ok の順で厳しい方を採る
    const enumStatus = evalResult.status === 'failed' ? 'failed' : (truncated ? 'partial' : evalResult.status);
    const summary = enumSummary(evalResult)
      || (deadlineHit ? '全体終了期限に達したので取得を打ち切った'
        : (truncated ? `ページ上限 ${MAX_PAGES} に到達 (打ち切りの疑い)` : null));

    // 履歴保存 (Step 0)。打ち切り・期限切れの夜は complete=false で残す (証拠にはするが削除判定には使わない)
    const archive = await archiveListings(deps, archiveArgs({
      complete: !truncated && !deadlineHit, enum_status: enumStatus, pages, truncated, deadline_hit: deadlineHit,
    }));

    insertSnapshots(db, rows);
    db.prepare(`UPDATE price_fetch_run SET finished_at = ?, status = ?, listing_enum_status = ?,
                expected_count = ?, fetched_count = ?, failed_count = ?, disappeared_count = ?,
                error_summary = ? WHERE run_id = ?`)
      .run(nowIso(), enumStatus, enumStatus,
        rows.length, rows.filter(r => r.fetch_status === 'ok').length,
        rows.filter(r => r.fetch_status !== 'ok').length, evalResult.disappeared,
        summary, runId);
    return { runId, count: rows.length, pages, truncated, deadlineHit, unparsable, duplicates, ...evalResult, status: enumStatus, archive };
  } catch (e) {
    db.prepare(`UPDATE price_fetch_run SET finished_at = ?, status = 'failed', listing_enum_status = 'failed',
                error_summary = ? WHERE run_id = ?`).run(nowIso(), String(e.message).slice(0, 500), runId);
    throw e;
  }
}

async function defaultRakutenSearchPage(cursorMark) {
  const { callRakutenProxy } = await import('./rms-client.js');
  return callRakutenProxy(`/service-api/rakuten-rms/items/search?cursorMark=${encodeURIComponent(cursorMark)}&hits=100`);
}

/**
 * Yahoo の出品を列挙して価格を取る (2026-09-19)。
 *
 * 取り方は 2 段:
 *   ① myItemList を **a-z + 0-9 の 36 本**で引いて ItemCode を集める
 *      (RYS の baseline と同じ網羅集合。ItemCode は英数字始まりなので、この 36 本で分割される。
 *       実測 2026-09-19: 36 本の合計 3,819 件 = ユニーク 3,819 件で重なり 0 = 前方一致)
 *   ② ItemCode ごとに get-item-detail を引いて価格・配送設定・SubCode を取る
 *
 * 🚨 ①で「聞けなかった query」があれば partial。取れた分だけで完全集合を名乗らせない。
 * 🚨 ②で詳細が取れなかった商品も partial の材料にする (unparsable)。価格だけ欠けた行を
 *    静かに混ぜると、その出品が翌晩「消えた」と数えられる。
 */
export async function fetchYahooListings(db, deps = {}) {
  const runId = newRunId();
  const startedAt = nowIso();
  const shopId = YAHOO_SHOP_ID();
  db.prepare(`INSERT INTO price_fetch_run (run_id, mall, started_at, status, listing_enum_status)
              VALUES (?, 'yahoo', ?, 'running', 'failed')`).run(runId, startedAt);

  try {
    const listPage = deps.yahooListPage || defaultYahooListPage;
    const detailOf = deps.yahooDetail || defaultYahooDetail;
    const concurrency = deps.yahooConcurrency || YAHOO_DETAIL_CONCURRENCY;
    const fetchedAt = nowIso();
    const validUntil = addDays(fetchedAt, PRICE_VALID_DAYS);
    // 🚨 期限は **1 ページ・1 チャンクごと**に見る (Codex R1 P1)。query の切れ目でしか見ないと、
    //    36 本 × 複数ページのあいだ期限を越えたまま API を叩き続ける
    const pastDeadline = () => Boolean(deps.deadline) && new Date() >= deps.deadline;
    const remainingMs = () => (deps.deadline ? deps.deadline.getTime() - Date.now() : null);

    // ── ① 商品コードの列挙 ──
    const itemCodes = [];
    const seen = new Set();
    let listCalls = 0;
    let truncated = false;
    let deadlineHit = false;
    const incompleteQueries = [];
    for (const q of YAHOO_QUERIES) {
      if (pastDeadline()) { truncated = true; deadlineHit = true; break; }
      // 🚨 件数は **この query の中でユニークな商品コード**で数える (Codex R1 P1)。
      //    延べ件数で数えると、同じ商品を 2 回返された回が「◯件ある」と一致してしまい、
      //    取りこぼしを ok と報告する
      const inQuery = new Set();
      let received = 0;
      let available = null;
      try {
        for await (const page of listPage(q)) {
          listCalls++;
          if (available === null) available = page.totalResultsAvailable;
          if (!Array.isArray(page.items)) { incompleteQueries.push({ query: q, reason: 'items_not_array' }); break; }
          for (const it of page.items) {
            received++;
            const code = String(it?.ItemCode ?? '').trim();
            // 🚨 読めない要素を黙って飛ばさない。1 件でもあれば完全集合を名乗らせない
            if (!code) { incompleteQueries.push({ query: q, reason: 'item_code_missing' }); continue; }
            inQuery.add(code);
            if (!seen.has(code)) { seen.add(code); itemCodes.push(code); }
          }
          // 🚨 ページの切れ目でも期限を見る。見ないと 1 query が期限をまたいで回り続ける
          if (pastDeadline()) { truncated = true; deadlineHit = true; break; }
        }
      } catch (e) {
        // 🚨 1 本の query が落ちても、そこまでに集めた商品コードは捨てない (partial として進む)。
        //    全部捨てると、プロキシが 1 回 503 を返しただけで夜がまるごと無駄になる
        incompleteQueries.push({ query: q, reason: `error:${String(e.message).slice(0, 60)}` });
      }
      if (deadlineHit) break;
      // 🚨 同じ商品を 2 回返された = ページ送りが壊れている疑い。ユニーク数で隠さず記録する
      if (received !== inQuery.size) {
        incompleteQueries.push({ query: q, reason: `duplicate_items:${received}/${inQuery.size}` });
      }
      // 🚨 モールが「◯件ある」と言った数と、受け取ったユニーク数が違う = 取りこぼし
      if (!Number.isInteger(available) || available < 0) {
        incompleteQueries.push({ query: q, reason: 'total_unavailable' });
      } else if (inQuery.size !== available) {
        incompleteQueries.push({ query: q, reason: `count_mismatch:${inQuery.size}/${available}` });
      }
    }

    // ── ② 価格・配送設定 ──
    const rows = [];
    const rawItems = [];
    let unparsable = 0;
    let detailCalls = 0;
    const failedItems = [];                      // 詳細が取れなかった商品コード (証拠として残す)
    for (let i = 0; i < itemCodes.length; i += concurrency) {
      if (pastDeadline()) { truncated = true; deadlineHit = true; break; }
      const chunk = itemCodes.slice(i, i + concurrency);
      // 🚨 残り時間より長い待ちを入れない。20 秒待つと、期限ぎりぎりで始めた 1 件が期限を越える
      const budget = remainingMs();
      const details = await Promise.all(chunk.map(async (code) => {
        try { return await detailOf(code, budget == null ? undefined : Math.max(1000, Math.min(20000, budget))); }
        // 🚨 1 件の失敗で夜を落とさない。ok:false として数え、partial の材料にする
        catch (e) { return { ok: false, ItemCode: code, error: String(e.message).slice(0, 120) }; }
      }));
      detailCalls += chunk.length;
      for (const d of details) {
        rawItems.push(d);
        const made = yahooDetailToSnapshotsDetailed(d, { runId, shopId, fetchedAt, validUntil });
        unparsable += made.unparsable;
        // 🚨 **一部だけ読めなかった商品も記録する** (Codex R2 P2 2026-09-19)。
        //    「行が 1 つでもできたか」で数えると、SubCode が 3 つのうち 1 つ壊れた商品が
        //    「取れた」に数えられ、履歴も件数も何も問題が無かったように見える
        if (made.rows.length === 0 || made.unparsable > 0) {
          failedItems.push(String(d?.ItemCode ?? '?') + (made.rows.length ? `(一部${made.unparsable})` : ''));
        }
        rows.push(...made.rows);
      }
    }
    // 期限で打ち切った = 聞いていない商品が残っている。これも「取れなかった」に数える
    const notAsked = itemCodes.length - detailCalls;

    const currentKeys = new Set(rows.map(r => snapshotKey(r.shop_id, r.mall_item_key)));
    const duplicates = rows.length - currentKeys.size;
    const prev = loadLastCompleteKeys(db, 'yahoo');
    const evalResult = enumStatusWithParseFailures(
      evaluateEnumeration(prev.keys, currentKeys),
      unparsable + incompleteQueries.length + notAsked, duplicates);
    const enumStatus = evalResult.status === 'failed' ? 'failed' : (truncated ? 'partial' : evalResult.status);
    const summary = enumSummary(evalResult)
      || (deadlineHit ? `全体終了期限に達したので取得を打ち切った (詳細を聞けていない商品 ${notAsked} 件)`
        : (failedItems.length || incompleteQueries.length
          ? `取りこぼしの疑い: 詳細が取れない ${failedItems.length} 件`
            + (incompleteQueries.length ? ` / 一覧 ${incompleteQueries.slice(0, 5).map(x => `${x.query}(${x.reason})`).join(' ')}` : '')
          : null));

    // 🚨 履歴の complete は「一覧も詳細も**全部**取れた」ときだけ true (Codex R1/R2 P2)。
    //    解析できなかった行・重複が 1 つでもあれば false。詳細が全滅した夜も、
    //    SubCode が 1 つだけ壊れた夜も、監視の情報が嘘をつかないようにする
    const complete = !truncated && incompleteQueries.length === 0 && failedItems.length === 0
      && notAsked === 0 && unparsable === 0 && duplicates === 0;
    const archive = await archiveListings(deps, {
      mall: 'yahoo', shopId, source: 'yahoo_item_detail', runId, fetchedAt,
      format: 'ndjson', payload: rawItems, sortKey: (r) => r?.ItemCode,
      items: rawItems.length,
      meta: {
        api_version: 'myItemList + getItemDetail',
        complete, enum_status: enumStatus, truncated, deadline_hit: deadlineHit,
        // 🚨 内訳は details に入れる。manifest は既定の項目しか残さないので、
        //    直に並べると渡したつもりの数字が消える (Codex R2 P2)
        details: {
          queries: YAHOO_QUERIES.length, list_calls: listCalls, items_enumerated: itemCodes.length,
          detail_calls: detailCalls, detail_failed: failedItems.length, detail_not_asked: notAsked,
          rows: rows.length, unparsable, duplicates,
          incomplete_queries: incompleteQueries.slice(0, 20),
          failed_items: failedItems.slice(0, 50),
        },
      },
    });

    insertSnapshots(db, rows);
    db.prepare(`UPDATE price_fetch_run SET finished_at = ?, status = ?, listing_enum_status = ?,
                expected_count = ?, fetched_count = ?, failed_count = ?, disappeared_count = ?,
                error_summary = ? WHERE run_id = ?`)
      .run(nowIso(), enumStatus, enumStatus,
        // 🚨 期待値は「列挙できた商品数」。作れた行だけを数えると、詳細が全滅した夜に
        //    expected も failed も 0 になって「何も問題が無かった」ように見える
        itemCodes.length,
        rows.filter(r => r.fetch_status === 'ok').length,
        rows.filter(r => r.fetch_status !== 'ok').length + failedItems.length + notAsked,
        evalResult.disappeared,
        summary, runId);
    return {
      runId, count: rows.length, items: itemCodes.length, listCalls, detailCalls,
      detailFailed: failedItems.length, notAsked,
      truncated, deadlineHit, unparsable, duplicates, incompleteQueries: incompleteQueries.length,
      ...evalResult, status: enumStatus, archive,
    };
  } catch (e) {
    db.prepare(`UPDATE price_fetch_run SET finished_at = ?, status = 'failed', listing_enum_status = 'failed',
                error_summary = ? WHERE run_id = ?`).run(nowIso(), String(e.message).slice(0, 500), runId);
    throw e;
  }
}

async function* defaultYahooListPage(query) {
  // 🚨 VPS プロキシの呼び出しは RYS の実装を**使い回す** (2 本目を書かない)。
  //    env 名も揃える (YAHOO_PROXY_BASE_URL。miniPC の .env は YAHOO_PROXY_URL なので橋渡しする)
  if (!process.env.YAHOO_PROXY_BASE_URL && process.env.YAHOO_PROXY_URL) {
    process.env.YAHOO_PROXY_BASE_URL = process.env.YAHOO_PROXY_URL;
  }
  const { iterateMyItemList } = await import('../rakuten-yahoo-sync/lib/yahoo-myitemlist-proxy.js');
  yield* iterateMyItemList(query, { pageSize: 100 });
}

async function defaultYahooDetail(itemCode, timeoutMs) {
  if (!process.env.YAHOO_PROXY_BASE_URL && process.env.YAHOO_PROXY_URL) {
    process.env.YAHOO_PROXY_BASE_URL = process.env.YAHOO_PROXY_URL;
  }
  const { fetchYahooItemDetail } = await import('../rakuten-yahoo-sync/lib/yahoo-detail-proxy.js');
  return fetchYahooItemDetail(itemCode, timeoutMs ? { timeoutMs } : undefined);
}

// ────────────────────────────────────────────────────────────
if (process.argv[1] && process.argv[1].endsWith('fetch-listings.js')) {
  const mall = process.argv.includes('--mall') ? process.argv[process.argv.indexOf('--mall') + 1] : null;
  const db = initExpectedProfitDB();
  const run = async () => {
    if (!mall || mall === 'amazon') console.log('[amazon]', JSON.stringify(await fetchAmazonListings(db)));
    if (!mall || mall === 'rakuten') console.log('[rakuten]', JSON.stringify(await fetchRakutenListings(db)));
    if (!mall || mall === 'yahoo') console.log('[yahoo]', JSON.stringify(await fetchYahooListings(db)));
  };
  run().then(() => db.close()).catch(e => { console.error(e); process.exit(1); });
}
