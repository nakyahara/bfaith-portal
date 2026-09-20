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
 *   node apps/expected-profit/fetch-listings.js --mall amazon     (amazon / rakuten / yahoo / aupay / qoo10 / linegift)
 */
import { getExpectedProfitDB, initExpectedProfitDB } from './db.js';
import { newRunId, nowIso, addDays, canonicalShopId, UNKNOWN_SELLER } from './util.js';
import { storedSellerId } from './refresh-fees.js';
import { archiveItems } from '../../scripts/mall-items/archive-items.mjs';
// au PAY の応答は XML。読み方は price-update の aupay-apply.js と同じ設定で揃える
import { parseString } from 'xml2js';

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
/** au PAY マーケットの店舗 ID。出品の鍵 (shop_id + 商品コード) の一部になる */
const AUPAY_SHOP_ID = () => String(process.env.AUPAY_SHOP_ID || '54318092').trim();
/** LINEギフトのショップ ID。出品の鍵 (shop_id + 商品 id/バリエーションコード) の一部になる */
const LINEGIFT_SHOP_ID = () => String(process.env.LINEGIFT_SHOP_ID || '').trim() || 'unknown';
/** Qoo10 の店舗。QAPI は鍵で店舗が決まるので、鍵の持ち主を表す固定値を使う */
const QOO10_SHOP_ID = () => String(process.env.QOO10_SHOP_ID || 'bfaith').trim();
const QOO10_API_BASE = 'https://api.qoo10.jp/GMKT.INC.Front.QAPIService/ebayjapan.qapi';
/** 残り時間がこれを下回ったら、新しい要求を出さない */
const QOO10_MIN_REQUEST_MS = 1000;

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

// ────────────────────────────────────────────────────────────
// au PAY マーケット (2026-09-19 中原さん要望「auPAY もやってよ」)
// ────────────────────────────────────────────────────────────

/** 1 ページで取る件数。実測 (2026-09-19): 500 件で 4.6MB / 3 秒。9 ページで全 4,035 件 */
const AUPAY_PAGE_SIZE = 500;
/** 残り時間がこれを下回ったら、新しい要求を出さない (出せば必ず期限を越える) */
const AUPAY_MIN_REQUEST_MS = 1000;

/**
 * au PAY の「送料込みか」を送料区分から決める。
 *
 * 🚨 **知らない値は null (不明) にする**。Amazon・Yahoo と同じ規約で、勝手に送料込みへ倒さない。
 *
 * `postageSegment` = "2" が送料無料。実測の裏づけ (2026-09-19):
 *   - 全 4,035 出品のうち **"2" が 3,971 / "1" が 64**
 *   - 直近 90 日の注文 6,048 行の `postage_price` は **全部 0 円**
 *   - 実績 (f_aupay_finance_sku_daily_v1) の送料収入も **0 円**
 *   "1" の 64 件は送料を別途もらっている可能性があるので、参考値のまま中原さんの確認を待つ
 */
export function aupayPostageIncluded(postageSegment) {
  const s = String(postageSegment ?? '').trim();
  if (s === '2') return true;
  return null;
}

/**
 * au PAY の商品 1 件 + カラバリ → snapshot 行。
 *
 * 🚨 **カラバリがある商品は子コードごとに行を作り、親の行は作らない**。
 *    子ごとに NE の品番が違う = 原価が違うので、親 1 行にまとめると別商品の原価で計算する
 *    (実測 2026-09-19: カラバリがあり、かつ親コード自体も NE にある商品が 55 件ある)。
 *
 * 🚨 au PAY は**商品に 1 つの価格しか持たない**。カラバリは在庫だけで価格を持たない
 *    (price-update の aupay-apply.js で実測済み)。なので子の行も親の価格を使う。
 *
 * @param {object} item  searchItemInfos の 1 件 (itemCode / itemPrice / taxSegment / postageSegment / deliveryMethodName)
 * @param {Set<string>|null} choices  searchStocks から作った子コードの集合 (無ければ null)
 */
export function aupayItemToSnapshotsDetailed(item, choices, { runId, shopId, fetchedAt, validUntil }) {
  if (!item || typeof item !== 'object') return { rows: [], unparsable: 1 };
  const itemCode = String(item.itemCode ?? '').trim();
  if (!itemCode) return { rows: [], unparsable: 1 };

  // 🚨 価格は「返ってこなかった」と「整数円として読めない」を分ける。0 円にはしない
  const price = toIntPrice(item.itemPrice);
  const postageIncluded = aupayPostageIncluded(item.postageSegment);
  // 🚨 **税込だと確かめられた出品だけ計算する** (Codex R1 P1 2026-09-19)。
  //    taxSegment = "1" が税込 (実測 2026-09-19: 全 4,035 件が "1")。
  //    実測で全部そうだったことは将来の応答を保証しない。欠けたり別の値になったら、
  //    税を二重に割り戻すので計算に通さない (楽天の taxIncluded と同じ扱い)
  const taxIncluded = String(item.taxSegment ?? '').trim() === '1';

  const make = (key) => ({
    run_id: runId,
    mall: 'aupay',
    shop_id: shopId,
    mall_item_key: key,
    mall_item_ref: null,
    mall_item_number: itemCode,          // 親の商品コード (カラバリ行から親をたどれるように)
    fulfillment: 'self',
    ne_code: null,
    price_type: 'normal',
    // 🚨 au PAY の出品価格は税込 (taxSegment=1)。税区分が確かめられない出品は価格を採らない
    price_incl_tax: taxIncluded ? price : null,
    price_tax_included: taxIncluded ? 1 : null,
    price_raw: price,
    mall_tax_rate: null,                 // 税率は返らない。NE 商品マスタのものを使う
    postage_included: postageIncluded == null ? null : (postageIncluded ? 1 : 0),
    postage_revenue_incl_tax: postageIncluded === true ? 0 : null,
    points: 0,
    listing_status: 'active',            // 一覧に出ている = 出品中
    fetch_status: !taxIncluded ? 'tax_segment_unknown' : (price == null ? 'not_found' : 'ok'),
    resolve_status: 'unresolved',
    resolve_reason: null,
    valid_until: validUntil,
    // 画面の「モール側の配送パターン」に出す。送料込みでない出品が出たときの手がかり
    shipping_group: item.deliveryMethodName ? String(item.deliveryMethodName).trim() || null : null,
    source: 'aupay_search_item_infos',
    fetched_at: fetchedAt,
  });

  if (!choices || choices.size === 0) return { rows: [make(itemCode)], unparsable: 0 };

  const rows = [];
  let unparsable = 0;
  for (const c of choices) {
    const choice = String(c ?? '').trim();
    if (!choice) { unparsable++; continue; }
    rows.push(make(`${itemCode}/${choice}`));
  }
  // カラバリがあるのに 1 行も作れなかった = 応答が壊れている (親の行で代用しない)
  if (rows.length === 0) return { rows: [], unparsable: unparsable || 1 };
  return { rows, unparsable };
}

// ────────────────────────────────────────────────────────────
// Qoo10 (2026-09-20 中原さん要望「残りの全モールに対してもお願い」)
// ────────────────────────────────────────────────────────────

/** 1 ページで取る件数は Qoo10 が決める (実測 500 件固定)。ページ番号で送る */
const QOO10_PAGE_SIZE = 500;
/** 詳細取得の同時実行数。実測 73ms/件なので 3 並列で 2,351 件 ≈ 1 分 */
const QOO10_DETAIL_CONCURRENCY = 3;
/**
 * 母集団に入れる出品の状態。
 *
 * 🚨 **S1 (Standby) と S2 (Active) だけ**。`GetItemDetailInfo` はこの 2 つしか返さない
 *    (実測 2026-09-20: S5 の 5 件はすべて `-10009 [Trade Status] S1(Standby), S2(Active)
 *    only you can search.`)。価格が取れない状態を母集団に入れると、毎晩必ず partial になる。
 * 🚨 販売中でない S1 も入れる — Amazon の inactive と同じで、売っていない出品も表に出す
 */
export const QOO10_ITEM_STATUSES = ['S1', 'S2'];
/**
 * 価格を取れない状態。**件数だけ数えて記録する** (存在することを隠さないため)。
 * 🚨 Qoo10 が受け付けるのは全部で 6 つだけ (S4 は弾かれる。実測のエラーメッセージ)
 */
export const QOO10_UNPRICEABLE_STATUSES = ['S0', 'S3', 'S5', 'S8'];
/** 販売中。これ以外は listing_status = 'inactive' にする */
const QOO10_ACTIVE_STATUS = 'S2';

/**
 * Qoo10 の価格は "1480.0000" のような小数文字列で返る。
 * 🚨 **小数部があったら整数円として読めたことにしない** (toIntPrice と同じ規約)。
 *    丸めると、画面の数字とモールの設定価格がずれる
 */
export function qoo10Price(v) {
  if (typeof v === 'number') return Number.isInteger(v) ? v : null;
  if (typeof v !== 'string') return null;
  const s = v.trim();
  if (!/^\d+(\.0+)?$/.test(s)) return null;
  const n = Number(s);
  return Number.isSafeInteger(n) ? n : null;
}

/**
 * Qoo10 の商品詳細 (+ オプション) → snapshot 行。
 *
 * 🚨 **オプション (カラバリ) がある商品は子ごとに行を作り、親の行は作らない**。
 *    子ごとに NE の品番 = 原価が違うので、親 1 行にまとめると別商品の原価で計算する。
 *    オプションは `ItemsLookup.GetGoodsInventoryInfo` の `ItemTypeCode` で取れる
 *    (実測 2026-09-20: 出品 2,346 件のうち 176 件にオプション・行は 1,028。
 *     `ItemTypeCode` が空の行は 0 件なので「ある/ない」がきれいに分かれる)。
 *
 * 🚨 オプションの `Price` は加算額。実測では **全件 0** なので子も親の価格を使うが、
 *    0 でないものが出てきたら足す (足さないと、上乗せぶんの利益を取りこぼす)。
 *
 * 🚨 一覧で見た SellerCode と、詳細が返した SellerCode が違ったら **別商品を取ってきている**。
 *    その行は作らない (別商品の原価で計算しないため)
 *
 * @param {Array|null} options GetGoodsInventoryInfo の行 (取れていなければ null)
 */
export function qoo10DetailToSnapshot(detail, listed, { runId, shopId, fetchedAt, validUntil }, options = null) {   // 🚨 options は配列必須 (渡し忘れは失敗)
  if (!detail || typeof detail !== 'object') return { rows: [], unparsable: 1 };
  const itemCode = String(detail.ItemNo ?? listed?.ItemCode ?? '').trim();
  if (!itemCode) return { rows: [], unparsable: 1 };
  // 🚨 要求した商品と返ってきた商品が違う
  if (listed?.ItemCode && String(listed.ItemCode).trim() !== itemCode) return { rows: [], unparsable: 1 };

  const sellerCode = String(detail.SellerCode ?? '').trim();
  const listedSeller = String(listed?.SellerCode ?? '').trim();
  // 🚨 一覧と詳細で出品者コードが食い違う = どちらが正か決められない。決めない
  if (listedSeller && sellerCode && listedSeller.toLowerCase() !== sellerCode.toLowerCase()) {
    return { rows: [], unparsable: 1 };
  }
  const code = sellerCode || listedSeller;
  const price = qoo10Price(detail.SellPrice);
  const status = String(detail.ItemStatus ?? listed?.ItemStatus ?? '').trim();

  // 🚨 オプションが取れていない商品は行を作らない。「オプションなし」と決めつけると
  //    子ごとに違う原価を親でまとめてしまう (Codex R1/R2 P1 2026-09-20)。
  //    🚨 **配列でなければすべて失敗**。null も含む (既定クライアントは配列でない応答を null にする)。
  //       「オプションなし」と言えるのは **空配列が返ったときだけ**
  if (!Array.isArray(options)) return { rows: [], unparsable: 1 };
  const optionRows = options;

  const make = (key, addPrice) => ({
      run_id: runId,
      mall: 'qoo10',
      shop_id: shopId,
      // 鍵は Qoo10 の商品番号 (オプションがあれば 商品番号/オプションコード)。
      // 出品者コード (= NE 品番の親) は mall_item_ref に持たせる
      mall_item_key: key,
      mall_item_ref: code || null,
      mall_item_number: null,
      fulfillment: 'self',
      ne_code: null,
      price_type: 'normal',
      // 🚨 Qoo10 の販売価格は税込 (モールの表示が税込)。税率は返らないので NE 商品マスタを使う
      price_incl_tax: price == null ? null : price + addPrice,
      price_tax_included: 1,
      price_raw: price,
      mall_tax_rate: null,
      // 🚨 Qoo10 は全商品送料無料 (実測 2026-09-20: 過去 1 年の注文 4,599 行すべて
      //    shipping_rate = 0 / shipping_rate_type は Free か空)。楽天と同じ扱いに揃える
      postage_included: 1,
      postage_revenue_incl_tax: 0,
      points: 0,
      listing_status: status === QOO10_ACTIVE_STATUS ? 'active' : 'inactive',
      fetch_status: price == null ? 'not_found' : 'ok',
      resolve_status: 'unresolved',
      resolve_reason: null,
      valid_until: validUntil,
      // 配送番号。送料込みでない出品が出てきたときの手がかりとして残す
      shipping_group: detail.ShippingNo == null ? null : `配送番号${String(detail.ShippingNo).trim()}` || null,
      source: 'qoo10_item_detail',
      fetched_at: fetchedAt,
  });

  if (optionRows.length === 0) return { rows: [make(itemCode, 0)], unparsable: 0 };

  // 🚨 **子コードが読めない行が 1 つでもあれば、その商品は子を見分けられない** (Codex R2 P1)。
  //    空の子コードを捨てて残りだけ行にすると、捨てた子が黙って消える。
  //    実測で空が 0 件だったことは、将来の空を「子がいない」と決める根拠にならない。
  // 🚨 **同じオプションコードが 2 回出てきたときも同じ**。
  //    実測 2026-09-20: Qoo10 側に Excel のエラー値 `#NAME?` がオプションコードとして
  //    登録されている商品が 3 件あり、1 商品の中で同じコードが 2〜3 回出てくる。
  //    この商品は「どの子がいくらか」が決められないので、
  //    **出品 1 行だけ作り、出品者コードを載せない** (= どの NE 品番か決めない)。
  //    こうすると母集団は欠けず (列挙は ok のまま)、親の原価で計算することもない。
  //    🚨 行を作らずに解析失敗として数えると、モール側のデータが直るまで毎晩 partial になり、
  //       Qoo10 がいつまでもランキングに載らない (§9.3 は列挙が ok でないと載せない)
  const codes = optionRows.map((o) => String(o?.ItemTypeCode ?? '').trim());
  // 🚨 加算額も「整数円として読める」ものだけ受ける。Number(null) も Number('') も 0 になるので、
  //    素の Number で見ると空の値が 0 円として通る (Codex R2 P2)
  const adds = optionRows.map((o) => qoo10Price(o?.Price));
  if (codes.some((c) => !c) || new Set(codes).size !== codes.length || adds.some((a) => a == null)) {
    return { rows: [{ ...make(itemCode, 0), mall_item_ref: null, source: 'qoo10_item_detail:option_unreadable' }], unparsable: 0 };
  }

  return { rows: codes.map((opt, i) => make(`${itemCode}/${opt}`, adds[i])), unparsable: 0 };
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

/**
 * au PAY の出品を列挙して価格を取る (2026-09-19)。
 *
 * 一覧 API 2 本だけで足りる (商品ごとの詳細を叩かない)。実測 2026-09-19:
 *   searchItemInfos … 価格・税区分・送料区分・配送方法   500 件 × 9 ページ = 22 秒
 *   searchStocks    … カラバリの子コード                 500 件 × 9 ページ =  9 秒
 *
 * 🚨 **カラバリを知らないまま親 1 行にしない**。子ごとに NE の品番 = 原価が違うので、
 *    在庫の一覧が取れなかったときは列挙を partial にして、その夜は完全集合を名乗らせない。
 */
export async function fetchAupayListings(db, deps = {}) {
  const runId = newRunId();
  const startedAt = nowIso();
  const shopId = AUPAY_SHOP_ID();
  db.prepare(`INSERT INTO price_fetch_run (run_id, mall, started_at, status, listing_enum_status)
              VALUES (?, 'aupay', ?, 'running', 'failed')`).run(runId, startedAt);

  try {
    const listItems = deps.aupayItemPage || defaultAupayItemPage;
    const listStocks = deps.aupayStockPage || defaultAupayStockPage;
    const pageSize = deps.aupayPageSize || AUPAY_PAGE_SIZE;
    const fetchedAt = nowIso();
    const validUntil = addDays(fetchedAt, PRICE_VALID_DAYS);
    const pastDeadline = () => Boolean(deps.deadline) && new Date() >= deps.deadline;
    const remainingMs = () => (deps.deadline ? deps.deadline.getTime() - Date.now() : null);

    const problems = [];          // 取りこぼしの疑い (完全集合を名乗らせない材料)
    let truncated = false;
    let deadlineHit = false;

    /**
     * 1 本の一覧 API を最後まで読む。
     * 🚨 「最後まで読めた」と「途中で止まった」を戻り値で必ず区別する (Codex R1)。
     *    complete が false のまま先へ進むと、取れていない集合を完全集合として扱ってしまう
     */
    const readAll = async (name, fn) => {
      const out = [];
      let maxCount = null;
      let calls = 0;
      let reachedEnd = false;
      // 🚨 この一覧で見つけた異常の数。**1 つでもあれば complete にしない** (Codex R2 P2)。
      //    件数が最後に一致しても、途中で総件数が変わった回は「全部取れた」と言えない
      let issues = 0;
      const issue = (reason) => { issues++; problems.push({ api: name, reason }); };
      for (let start = 1; ; start += pageSize) {
        // 🚨 期限は **完了判定より先に**見る。あとに置くと最後のページが期限をまたいでも ok になる
        if (pastDeadline()) { truncated = true; deadlineHit = true; break; }
        const budget = remainingMs();
        // 🚨 残り時間が通信 1 回ぶんも無いなら、要求そのものを出さない (出せば必ず期限を越える)
        if (budget != null && budget < AUPAY_MIN_REQUEST_MS) { truncated = true; deadlineHit = true; break; }
        let page;
        try {
          page = await fn({ startCount: start, totalCount: pageSize },
            budget == null ? undefined : Math.min(120_000, budget));
        } catch (e) {
          // 🚨 1 ページ落ちても、そこまでに取れた分は捨てない (Codex R1 P2)
          issue(`error:${String(e.message).slice(0, 60)}`);
          break;
        }
        calls++;
        if (!page || !Array.isArray(page.rows)) { issue('rows_not_array'); break; }
        // 🚨 モールが言う総件数がページごとに変わったら、取りこぼしの疑い (Codex R1 P2)
        if (maxCount === null) maxCount = page.maxCount;
        else if (page.maxCount !== maxCount) {
          issue(`total_changed:${maxCount}->${page.maxCount}`);
          maxCount = page.maxCount;
        }
        out.push(...page.rows);
        if (pastDeadline()) { truncated = true; deadlineHit = true; break; }
        if (page.rows.length < pageSize) { reachedEnd = true; break; }
        if (maxCount != null && out.length >= maxCount) { reachedEnd = true; break; }
        // 暴走防止 (実測 4,035 件 = 9 ページ)
        if (calls >= 100) { issue('page_limit'); truncated = true; break; }
      }
      // 🚨 モールが「◯件ある」と言った数と、受け取った数が違う = 取りこぼし
      if (!Number.isInteger(maxCount) || maxCount < 0) issue('total_unavailable');
      else if (out.length !== maxCount) issue(`count_mismatch:${out.length}/${maxCount}`);
      return { rows: out, maxCount, calls, complete: reachedEnd && issues === 0 };
    };
    // ── ① 商品 (価格・税区分・送料区分) ──
    const itemsRes = await readAll('searchItemInfos', listItems);

    // ── ② カラバリ (在庫の一覧) ──
    // 🚨 これが**最後まで**取れないと、カラバリ商品を親 1 行にしてしまう = 別商品の原価で計算する。
    //    例外だけでなく「件数が合わない」「途中で止まった」も同じ扱いにする (Codex R1 P1)
    let stocksRes = { rows: [], maxCount: null, calls: 0, complete: false };
    if (!deadlineHit) stocksRes = await readAll('searchStocks', listStocks);

    const choicesByItem = new Map();
    const stockSeen = new Set();          // 在庫の一覧で「見えた」商品コード
    let stockBroken = 0;
    let stockDuplicates = 0;
    for (const s of stocksRes.rows) {
      const code = String(s?.itemCode ?? '').trim();
      if (!code) { stockBroken++; continue; }
      // 🚨 読めなかったカラバリがある商品は、「カラバリなし」と混同しない
      if (s.broken) { stockBroken++; continue; }
      // 🚨 同じ商品が 2 回来たら、あとの行で子コードを**上書きしてしまう** (Codex R2 P1)。
      //    実測 2026-09-19 では重複 0 件。起きたらその夜は行を作らない
      if (stockSeen.has(code)) { stockDuplicates++; continue; }
      stockSeen.add(code);
      if (s.choices && s.choices.size) choicesByItem.set(code, s.choices);
    }
    if (stockBroken > 0) problems.push({ api: 'searchStocks', reason: `broken_rows:${stockBroken}` });
    if (stockDuplicates > 0) problems.push({ api: 'searchStocks', reason: `duplicate_items:${stockDuplicates}` });

    // ── ③ 行を作る ──
    const rows = [];
    const rawItems = [];
    let unparsable = 0;
    const failedItems = [];
    // 🚨 在庫の一覧を最後まで取れていない夜は 1 行も作らない。
    //    親 1 行に化けた行を残す方が害が大きい (別商品の原価で黒字に見える)
    const stocksOk = stocksRes.complete && stockBroken === 0 && stockDuplicates === 0;
    if (stocksOk) {
      for (const item of itemsRes.rows) {
        rawItems.push(item);
        // 🚨 応答が読めなかった商品は行を作らない (空の値で計算に通さない)
        if (item?.broken) { unparsable++; failedItems.push(`${item.itemCode ?? '?'}(応答が読めない)`); continue; }
        const code = String(item?.itemCode ?? '').trim();
        // 🚨 **在庫の一覧に出てこなかった商品は「カラバリなし」と決めない** (Codex R1 P1)。
        //    カラバリの有無が分からないまま親 1 行を作ると、別商品の原価で計算する
        if (code && !stockSeen.has(code)) {
          unparsable++;
          failedItems.push(`${code}(在庫一覧に無い)`);
          continue;
        }
        const made = aupayItemToSnapshotsDetailed(item, choicesByItem.get(code) || null,
          { runId, shopId, fetchedAt, validUntil });
        unparsable += made.unparsable;
        if (made.rows.length === 0 || made.unparsable > 0) {
          failedItems.push(String(item?.itemCode ?? '?') + (made.rows.length ? `(一部${made.unparsable})` : ''));
        }
        rows.push(...made.rows);
      }
    } else {
      problems.push({ api: 'searchStocks', reason: 'stocks_incomplete' });
    }

    const currentKeys = new Set(rows.map(r => snapshotKey(r.shop_id, r.mall_item_key)));
    const duplicates = rows.length - currentKeys.size;
    const prev = loadLastCompleteKeys(db, 'aupay');
    const evalResult = enumStatusWithParseFailures(
      evaluateEnumeration(prev.keys, currentKeys), unparsable + problems.length, duplicates);
    const enumStatus = evalResult.status === 'failed' ? 'failed' : (truncated ? 'partial' : evalResult.status);
    const summary = enumSummary(evalResult)
      || (deadlineHit ? '全体終了期限に達したので取得を打ち切った'
        : (problems.length
          ? `取りこぼしの疑い: ${problems.slice(0, 5).map(x => `${x.api}(${x.reason})`).join(' ')}`
          : null));

    const complete = !truncated && problems.length === 0 && failedItems.length === 0
      && unparsable === 0 && duplicates === 0;
    const archive = await archiveListings(deps, {
      mall: 'aupay', shopId, source: 'aupay_search_item_infos', runId, fetchedAt,
      format: 'ndjson', payload: rawItems, sortKey: (r) => r?.itemCode,
      items: rawItems.length,
      meta: {
        api_version: 'searchItemInfos + searchStocks',
        complete, enum_status: enumStatus, truncated, deadline_hit: deadlineHit,
        details: {
          items_enumerated: itemsRes.rows.length, items_max_count: itemsRes.maxCount, item_calls: itemsRes.calls,
          stocks_enumerated: stocksRes.rows.length, stocks_max_count: stocksRes.maxCount, stock_calls: stocksRes.calls,
          items_with_choices: choicesByItem.size,
          rows: rows.length, unparsable, duplicates,
          problems: problems.slice(0, 20), failed_items: failedItems.slice(0, 50),
        },
      },
    });

    insertSnapshots(db, rows);
    db.prepare(`UPDATE price_fetch_run SET finished_at = ?, status = ?, listing_enum_status = ?,
                expected_count = ?, fetched_count = ?, failed_count = ?, disappeared_count = ?,
                error_summary = ? WHERE run_id = ?`)
      .run(nowIso(), enumStatus, enumStatus,
        itemsRes.rows.length,
        rows.filter(r => r.fetch_status === 'ok').length,
        rows.filter(r => r.fetch_status !== 'ok').length + failedItems.length,
        evalResult.disappeared, summary, runId);
    return {
      runId, count: rows.length, items: itemsRes.rows.length, itemsWithChoices: choicesByItem.size,
      itemCalls: itemsRes.calls, stockCalls: stocksRes.calls, stocksOk,
      truncated, deadlineHit, unparsable, duplicates, problems: problems.length,
      ...evalResult, status: enumStatus, archive,
    };
  } catch (e) {
    db.prepare(`UPDATE price_fetch_run SET finished_at = ?, status = 'failed', listing_enum_status = 'failed',
                error_summary = ? WHERE run_id = ?`).run(nowIso(), String(e.message).slice(0, 500), runId);
    throw e;
  }
}

/** au PAY の XML を読む (xml2js。price-update の aupay-apply.js と同じ設定) */
function aupayParseXml(xml) {
  let out = null;
  let err = null;
  parseString(xml, { explicitArray: true, trim: true, async: false }, (e, r) => { err = e; out = r; });
  if (err) throw new Error(`au PAY の応答を XML として読めませんでした: ${err.message}`);
  return out;
}

/** au PAY は失敗も HTTP 200 + status!=0 で返す。status を先に見る (aupay-apply.js と同じ判断) */
function aupayResultRoot(xml) {
  const doc = aupayParseXml(xml);
  const root = doc?.response;
  if (!root) throw new Error('au PAY の応答に response がありません');
  const status = root.result?.[0]?.status?.[0];
  if (status !== '0') {
    const err = root.result?.[0]?.error?.[0];
    throw new Error(`au PAY がエラーを返しました (status=${status ?? 'なし'}`
      + `${err?.code?.[0] ? ` / ${err.code[0]}` : ''}${err?.message?.[0] ? `: ${err.message[0]}` : ''})`);
  }
  return root.searchResult?.[0] || {};
}

/**
 * 要素の値を 1 つ読む。
 * 🚨 **「無い」「空」「読めない」を混ぜない** (Codex R1 P1 2026-09-19)。
 *    xml2js は空要素を '' で返すが、子要素を持つものは object になる。object を '' に均すと
 *    壊れた子コードが「カラバリなし」に化け、親 1 行 = 別商品の原価で計算してしまう。
 *   - 要素そのものが無い      → null
 *   - 空要素 / 文字列          → その文字列 ('' を含む)
 *   - object や 2 個以上の要素 → AUPAY_UNREADABLE (呼び出し側が解析失敗として数える)
 */
export const AUPAY_UNREADABLE = Symbol('aupay_unreadable');
const aupayFirst = (node, tag) => {
  const arr = node?.[tag];
  if (arr == null) return null;
  if (!Array.isArray(arr) || arr.length !== 1) return AUPAY_UNREADABLE;
  const v = arr[0];
  return typeof v === 'string' ? v : AUPAY_UNREADABLE;
};
/** 表示や比較に使う前に、読めなかった値を null へ均す (行は別途 broken として数える) */
const aupayText = (v) => (v === AUPAY_UNREADABLE ? null : v);

/** searchItemInfos の応答 → { maxCount, rows } */
export function parseAupayItemsXml(xml) {
  const sr = aupayResultRoot(xml);
  const rows = (sr.resultItems || []).map((it) => {
    // 🚨 商品コードと価格・税区分・送料区分が読めない行は、**空の値で先へ進ませない**。
    //    broken を立てて呼び出し側に数えさせる (黙って「カラバリなし・税区分なし」にしない)
    const code = aupayFirst(it, 'itemCode');
    const price = aupayFirst(it, 'itemPrice');
    const tax = aupayFirst(it, 'taxSegment');
    const postageSeg = aupayFirst(it, 'postageSegment');
    const broken = [code, price, tax, postageSeg].includes(AUPAY_UNREADABLE);
    return {
      itemCode: aupayText(code),
      itemName: aupayText(aupayFirst(it, 'itemName')),
      itemPrice: aupayText(price),
      taxSegment: aupayText(tax),
      postageSegment: aupayText(postageSeg),
      postage: aupayText(aupayFirst(it, 'postage')),
      deliveryMethodName: aupayText(aupayFirst(it.deliveryMethod?.[0], 'deliveryMethodName')),
      broken,
    };
  });
  return { maxCount: aupayCount(sr), rows };
}

/** 総件数。🚨 Number('') も Number(null) も 0 になるので、数字として読めたときだけ返す */
function aupayCount(sr) {
  const raw = aupayFirst(sr, 'maxCount');
  if (typeof raw !== 'string' || !/^\d+$/.test(raw.trim())) return null;
  const n = Number(raw.trim());
  return Number.isSafeInteger(n) ? n : null;
}

/**
 * searchStocks の応答 → { maxCount, rows: [{ itemCode, choices:Set }] }
 *
 * 🚨 子コードは「縦」「横」のどちらか (または両方) に入る。**`-` と空はカラバリではない**
 *    (実測 2026-09-19: 横だけ 63 / 縦だけ 275 / 縦横両方 17 / カラバリ無し 3,680)。
 *    両方あるときは 横+縦 をつないだものが子コードになる
 */
export function parseAupayStocksXml(xml) {
  const sr = aupayResultRoot(xml);
  const real = (v) => typeof v === 'string' && v.trim() !== '' && v.trim() !== '-';
  const rows = (sr.resultStocks || []).map((st) => {
    const choices = new Set();
    const code = aupayFirst(st, 'itemCode');
    // 🚨 1 つでも読めない子コードがあれば、その商品は broken。
    //    「カラバリなし」に落とすと親 1 行 = 別商品の原価になる (Codex R1 P1)
    let broken = code === AUPAY_UNREADABLE;
    for (const cs of st.choicesStocks || []) {
      const h = aupayFirst(cs, 'choicesStockHorizontalCode');
      const v = aupayFirst(cs, 'choicesStockVerticalCode');
      if (h === AUPAY_UNREADABLE || v === AUPAY_UNREADABLE) { broken = true; continue; }
      // 🚨 **枠があるのに縦横のタグがどちらも無い = カラバリなしと区別がつかない** (Codex R2 P1)。
      //    実測 2026-09-19: カラバリの無い商品は choicesStocks の枠自体が無く (3,680 件)、
      //    枠がある商品は必ず縦か横のタグを持つ (どちらも無い行は 0 件)
      if (h === null && v === null) { broken = true; continue; }
      const H = real(h), V = real(v);
      if (H && V) choices.add(h.trim() + v.trim());
      else if (H) choices.add(h.trim());
      else if (V) choices.add(v.trim());
    }
    return { itemCode: aupayText(code), choices, broken };
  });
  return { maxCount: aupayCount(sr), rows };
}

function aupayProxy() {
  const base = String(process.env.AUPAY_PROXY_URL || '').trim().replace(/\/+$/, '');
  if (!base) throw new Error('AUPAY_PROXY_URL が未設定です');
  const secret = String(process.env.AUPAY_PROXY_SECRET || '').trim();
  if (!secret) throw new Error('AUPAY_PROXY_SECRET が未設定です');
  return { base, secret };
}

async function aupayGet(path, timeoutMs) {
  const { base, secret } = aupayProxy();
  const res = await fetch(`${base}${path}`, {
    headers: { 'X-Proxy-Secret': secret },
    signal: AbortSignal.timeout(Number.isFinite(timeoutMs) ? timeoutMs : 120_000),
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`au PAY プロキシ ${path} が HTTP ${res.status}: ${text.slice(0, 200)}`);
  return text;
}

async function defaultAupayItemPage({ startCount, totalCount }, timeoutMs) {
  const shopId = AUPAY_SHOP_ID();
  return parseAupayItemsXml(await aupayGet(
    `/wmshopapi/searchItemInfos?shopId=${encodeURIComponent(shopId)}&totalCount=${totalCount}&startCount=${startCount}`,
    timeoutMs));
}

async function defaultAupayStockPage({ startCount, totalCount }, timeoutMs) {
  const shopId = AUPAY_SHOP_ID();
  return parseAupayStocksXml(await aupayGet(
    `/wmshopapi/searchStocks?shopId=${encodeURIComponent(shopId)}&totalCount=${totalCount}&startCount=${startCount}`,
    timeoutMs));
}

/**
 * Qoo10 の出品を列挙して価格を取る (2026-09-20)。
 *
 *   ① `ItemsLookup.GetAllGoodsInfo` を状態 6 つ × ページ で引いて ItemCode / SellerCode を集める
 *   ② ItemCode ごとに `ItemsLookup.GetItemDetailInfo` で 価格・出品者コード・配送番号 を取る
 *
 * 実測 2026-09-20: 出品 2,351 件 (S1 66 / S2 2,280 / S5 5)。詳細は 73ms/件。
 */
export async function fetchQoo10Listings(db, deps = {}) {
  const runId = newRunId();
  const startedAt = nowIso();
  const shopId = QOO10_SHOP_ID();
  db.prepare(`INSERT INTO price_fetch_run (run_id, mall, started_at, status, listing_enum_status)
              VALUES (?, 'qoo10', ?, 'running', 'failed')`).run(runId, startedAt);

  try {
    const listPage = deps.qoo10ListPage || defaultQoo10ListPage;
    const detailOf = deps.qoo10Detail || defaultQoo10Detail;
    const optionsOf = deps.qoo10Options || defaultQoo10Options;
    const concurrency = deps.qoo10Concurrency || QOO10_DETAIL_CONCURRENCY;
    const fetchedAt = nowIso();
    const validUntil = addDays(fetchedAt, PRICE_VALID_DAYS);
    const pastDeadline = () => Boolean(deps.deadline) && new Date() >= deps.deadline;
    const remainingMs = () => (deps.deadline ? deps.deadline.getTime() - Date.now() : null);

    const problems = [];
    let truncated = false;
    let deadlineHit = false;
    const issue = (api, reason) => problems.push({ api, reason });

    // ── ① 商品番号の列挙 (状態ごと) ──
    const listed = new Map();          // ItemCode → { ItemCode, SellerCode, ItemStatus }
    let listCalls = 0;
    let duplicateListed = 0;
    for (const status of QOO10_ITEM_STATUSES) {
      if (pastDeadline()) { truncated = true; deadlineHit = true; break; }
      let totalItems = null;
      let got = 0;
      let page = 1;
      for (;;) {
        if (pastDeadline()) { truncated = true; deadlineHit = true; break; }
        const budget = remainingMs();
        if (budget != null && budget < QOO10_MIN_REQUEST_MS) { truncated = true; deadlineHit = true; break; }
        let res;
        try {
          res = await listPage({ status, page },
            budget == null ? undefined : Math.min(60_000, budget));
        } catch (e) {
          // 🚨 1 状態が落ちても、そこまでに集めた商品は捨てない
          issue('GetAllGoodsInfo', `error:${status}:${String(e.message).slice(0, 50)}`);
          break;
        }
        listCalls++;
        // 🚨 0 件の状態は Items を返さないことがある (実測)。TotalItems が 0 なら「0 件」であって壊れてはいない
        if (!res) { issue('GetAllGoodsInfo', `no_response:${status}`); break; }
        if (!Array.isArray(res.items)) {
          if (res.totalItems === 0) { totalItems = 0; break; }
          issue('GetAllGoodsInfo', `rows_not_array:${status}`); break;
        }
        if (totalItems === null) totalItems = res.totalItems;
        else if (res.totalItems !== totalItems) issue('GetAllGoodsInfo', `total_changed:${status}`);
        for (const it of res.items) {
          const code = String(it?.ItemCode ?? '').trim();
          if (!code) { issue('GetAllGoodsInfo', `item_code_missing:${status}`); continue; }
          // 🚨 同じ商品番号が 2 度出てきたら、あとの行で上書きしない (状態が食い違う)
          if (listed.has(code)) { duplicateListed++; continue; }
          listed.set(code, { ItemCode: code, SellerCode: it.SellerCode ?? null, ItemStatus: it.ItemStatus ?? status });
        }
        got += res.items.length;
        if (pastDeadline()) { truncated = true; deadlineHit = true; break; }
        if (res.totalPages == null || page >= res.totalPages) break;
        page++;
        if (page > 200) { issue('GetAllGoodsInfo', `page_limit:${status}`); truncated = true; break; }
      }
      if (deadlineHit) break;
      // 🚨 モールが「◯件ある」と言った数と、受け取った数が違う = 取りこぼし
      if (!Number.isInteger(totalItems) || totalItems < 0) issue('GetAllGoodsInfo', `total_unavailable:${status}`);
      else if (got !== totalItems) issue('GetAllGoodsInfo', `count_mismatch:${status}:${got}/${totalItems}`);
    }
    if (duplicateListed > 0) issue('GetAllGoodsInfo', `duplicate_items:${duplicateListed}`);

    // ── ①b 価格を取れない状態の件数 (母集団には入れないが、隠さない) ──
    // 🚨 「0 件だった」「取れなかった」「そもそも聞いていない」を混ぜない (Codex R1 P2)。
    //    数えられなくても取得の成否は変えない (母集団の外なので) が、分からないことは null で残す
    const unpriceable = Object.fromEntries(QOO10_UNPRICEABLE_STATUSES.map((st) => [st, null]));
    for (const status of QOO10_UNPRICEABLE_STATUSES) {
      if (pastDeadline()) break;
      const budget = remainingMs();
      // 🚨 母集団外を数えるために期限を食わない (Codex R1 P2)
      if (budget != null && budget < QOO10_MIN_REQUEST_MS) break;
      try {
        const res = await listPage({ status, page: 1 }, budget == null ? 30_000 : Math.min(30_000, budget));
        listCalls++;
        unpriceable[status] = Number.isInteger(res?.totalItems) ? res.totalItems : null;
      } catch { unpriceable[status] = null; }
    }

    // ── ② 価格 ──
    const rows = [];
    const rawItems = [];
    let unparsable = 0;
    let detailCalls = 0;
    let itemsWithOptions = 0;
    let optionUnreadable = 0;      // オプションコードが重なっていて子を見分けられない商品
    const failedItems = [];
    const targets = [...listed.values()];
    for (let i = 0; i < targets.length; i += concurrency) {
      if (pastDeadline()) { truncated = true; deadlineHit = true; break; }
      const budget = remainingMs();
      if (budget != null && budget < QOO10_MIN_REQUEST_MS) { truncated = true; deadlineHit = true; break; }
      const chunk = targets.slice(i, i + concurrency);
      const timeout = budget == null ? undefined : Math.min(60_000, budget);
      const details = await Promise.all(chunk.map(async (it) => {
        try {
          // 🚨 価格と**オプション**を両方取る。オプションが取れない商品は行を作らない
          //    (「オプションなし」と決めつけると、子ごとに違う原価を親でまとめてしまう)
          const [detail, options] = await Promise.all([
            detailOf(it.ItemCode, timeout),
            optionsOf(it.ItemCode, timeout),
          ]);
          return { it, detail, options };
        } catch (e) {
          // 🚨 1 件の失敗で夜を落とさない
          return { it, detail: null, options: null, error: String(e.message).slice(0, 120) };
        }
      }));
      detailCalls += chunk.length;
      for (const { it, detail, options, error } of details) {
        rawItems.push(detail ? { ...detail, Options: options ?? null } : { ItemNo: it.ItemCode, error });
        const made = qoo10DetailToSnapshot(detail, it, { runId, shopId, fetchedAt, validUntil }, options);
        unparsable += made.unparsable;
        if (made.rows.length === 0 || made.unparsable > 0) {
          failedItems.push(it.ItemCode + (made.rows.length ? `(一部${made.unparsable})` : ''));
        }
        if (made.rows.some((r) => r.mall_item_key.includes('/'))) itemsWithOptions++;
        if (made.rows.some((r) => r.source === 'qoo10_item_detail:option_unreadable')) optionUnreadable++;
        rows.push(...made.rows);
      }
    }
    const notAsked = targets.length - detailCalls;

    const currentKeys = new Set(rows.map(r => snapshotKey(r.shop_id, r.mall_item_key)));
    const duplicates = rows.length - currentKeys.size;
    const prev = loadLastCompleteKeys(db, 'qoo10');
    const evalResult = enumStatusWithParseFailures(
      evaluateEnumeration(prev.keys, currentKeys), unparsable + problems.length + notAsked, duplicates);
    const enumStatus = evalResult.status === 'failed' ? 'failed' : (truncated ? 'partial' : evalResult.status);
    const summary = enumSummary(evalResult)
      || (deadlineHit ? `全体終了期限に達したので取得を打ち切った (詳細を聞けていない商品 ${notAsked} 件)`
        : (failedItems.length || problems.length
          ? `取りこぼしの疑い: 詳細が取れない ${failedItems.length} 件`
            + (problems.length ? ` / 一覧 ${problems.slice(0, 5).map(x => x.reason).join(' ')}` : '')
          : null));

    const complete = !truncated && problems.length === 0 && failedItems.length === 0
      && notAsked === 0 && unparsable === 0 && duplicates === 0;
    const archive = await archiveListings(deps, {
      mall: 'qoo10', shopId, source: 'qoo10_item_detail', runId, fetchedAt,
      format: 'ndjson', payload: rawItems, sortKey: (r) => r?.ItemNo,
      items: rawItems.length,
      meta: {
        api_version: 'GetAllGoodsInfo + GetItemDetailInfo',
        complete, enum_status: enumStatus, truncated, deadline_hit: deadlineHit,
        details: {
          statuses: QOO10_ITEM_STATUSES.length, list_calls: listCalls, items_enumerated: listed.size,
          // 価格を取れない状態の出品 (母集団の外。存在することは残す)
          unpriceable_items: unpriceable,
          detail_calls: detailCalls, detail_failed: failedItems.length, detail_not_asked: notAsked,
          items_with_options: itemsWithOptions, items_option_unreadable: optionUnreadable,
          rows: rows.length, unparsable, duplicates,
          problems: problems.slice(0, 20), failed_items: failedItems.slice(0, 50),
        },
      },
    });

    insertSnapshots(db, rows);
    db.prepare(`UPDATE price_fetch_run SET finished_at = ?, status = ?, listing_enum_status = ?,
                expected_count = ?, fetched_count = ?, failed_count = ?, disappeared_count = ?,
                error_summary = ? WHERE run_id = ?`)
      .run(nowIso(), enumStatus, enumStatus, listed.size,
        rows.filter(r => r.fetch_status === 'ok').length,
        rows.filter(r => r.fetch_status !== 'ok').length + failedItems.length + notAsked,
        evalResult.disappeared, summary, runId);
    return {
      runId, count: rows.length, items: listed.size, itemsWithOptions, optionUnreadable,
      listCalls, detailCalls, unpriceable,
      detailFailed: failedItems.length, notAsked,
      truncated, deadlineHit, unparsable, duplicates, problems: problems.length,
      ...evalResult, status: enumStatus, archive,
    };
  } catch (e) {
    db.prepare(`UPDATE price_fetch_run SET finished_at = ?, status = 'failed', listing_enum_status = 'failed',
                error_summary = ? WHERE run_id = ?`).run(nowIso(), String(e.message).slice(0, 500), runId);
    throw e;
  }
}

/** QAPI を 1 回叩く。🚨 Qoo10 は失敗も HTTP 200 + ResultCode≠0 で返す */
async function qoo10Api(method, params, timeoutMs) {
  const key = String(process.env.QOO10_CERT_KEY || '').trim();
  if (!key) throw new Error('QOO10_CERT_KEY が未設定です (商品系 API はこの鍵。受注用の QOO10_API_KEY では商品が見えません)');
  const qs = new URLSearchParams({ key, ...params });
  const res = await fetch(`${QOO10_API_BASE}/${method}?${qs}`, {
    headers: { GiosisCertificationKey: key },
    signal: AbortSignal.timeout(Number.isFinite(timeoutMs) ? timeoutMs : 60_000),
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`Qoo10 ${method} が HTTP ${res.status}: ${text.slice(0, 200)}`);
  let json;
  try { json = JSON.parse(text); } catch { throw new Error(`Qoo10 ${method} の応答が JSON でない: ${text.slice(0, 200)}`); }
  if (json?.ResultCode !== 0) {
    throw new Error(`Qoo10 ${method} がエラーを返しました (ResultCode=${json?.ResultCode ?? 'なし'}: ${json?.ResultMsg ?? ''})`);
  }
  return json.ResultObject;
}

async function defaultQoo10ListPage({ status, page }, timeoutMs) {
  const ro = await qoo10Api('ItemsLookup.GetAllGoodsInfo',
    { ItemStatus: status, ...(page > 1 ? { Page: String(page) } : {}) }, timeoutMs);
  return {
    totalItems: Number.isInteger(ro?.TotalItems) ? ro.TotalItems : null,
    totalPages: Number.isInteger(ro?.TotalPages) ? ro.TotalPages : null,
    items: Array.isArray(ro?.Items) ? ro.Items : null,
  };
}

async function defaultQoo10Detail(itemCode, timeoutMs) {
  const ro = await qoo10Api('ItemsLookup.GetItemDetailInfo', { ItemCode: itemCode, SellerCode: '' }, timeoutMs);
  return Array.isArray(ro) ? (ro[0] ?? null) : ro;
}

/**
 * オプション (カラバリ) の一覧。
 * 🚨 `ItemTypeCode` が子コード。実測 2026-09-20: 出品 2,346 件のうち 176 件にオプションがあり、
 *    `ItemTypeCode` が空の行は 0 件。オプションの無い商品は空配列が返る
 */
async function defaultQoo10Options(itemCode, timeoutMs) {
  const ro = await qoo10Api('ItemsLookup.GetGoodsInventoryInfo', { ItemCode: itemCode, SellerCode: '' }, timeoutMs);
  // 🚨 配列でなければ null。呼び出し側は「配列でなければ失敗」なので、ここで [] に均さない
  return Array.isArray(ro) ? ro : null;
}

// ────────────────────────────────────────────────────────────
// LINEギフト (2026-09-20 中原さん要望「残りの全モールに対してもお願い」)
// ────────────────────────────────────────────────────────────

/** 1 ページの件数。実測 2026-09-20: 100 で 36 ページ・12 秒 (全 3,594 件) */
const LINEGIFT_PAGE_SIZE = 100;
/** 詳細取得の同時実行数。実測 400 件を 14 秒 (= 3,594 件で約 2 分) */
const LINEGIFT_DETAIL_CONCURRENCY = 3;
/** 残り時間がこれを下回ったら、新しい要求を出さない */
const LINEGIFT_MIN_REQUEST_MS = 1000;
const LINEGIFT_HOST = 'https://gift-shop-cms.line.biz';
/** 販売中。これ以外は listing_status = 'inactive' */
const LINEGIFT_ACTIVE = 'sale';
const LINEGIFT_VARIATION_ACTIVE = 'variation_sale';

/**
 * LINEギフトの商品詳細 → snapshot 行 (バリエーションごとに 1 行)。
 *
 * 🚨 **バリエーションごとに行を作り、親の行は作らない**。`variations[].code` が NE の品番で、
 *    子ごとに原価が違う (実測 2026-09-20: サンプル 400 商品の variation 496 件のうち
 *    495 件 = 99.8% が NE にある。variations が 0 件の商品は無い)。
 *
 * 🚨 価格は**商品レベル**にしか無い (`variations` に price は無い。2026-09-04 に実データで確認済)。
 *    子も商品の価格を使う。
 *
 * 🚨 `variations` が配列でなければ**失敗**。「バリエーションなし」と決めつけると、
 *    子ごとに違う原価を親でまとめてしまう (Qoo10 で踏んだのと同じ形)。
 */
export function linegiftItemToSnapshots(detail, listed, { runId, shopId, fetchedAt, validUntil }) {
  const item = detail && typeof detail === 'object' ? (detail.item ?? detail) : null;
  if (!item || typeof item !== 'object') return { rows: [], unparsable: 1 };
  const itemId = String(item.id ?? listed?.id ?? '').trim();
  if (!itemId) return { rows: [], unparsable: 1 };
  // 🚨 要求した商品と返ってきた商品が違う
  if (listed?.id != null && String(listed.id).trim() !== itemId) return { rows: [], unparsable: 1 };

  const price = toIntPrice(item.price);
  const itemActive = String(item.status ?? '').trim() === LINEGIFT_ACTIVE;

  const make = (key, active) => ({
    run_id: runId,
    mall: 'linegift',
    shop_id: shopId,
    mall_item_key: key,
    // バリエーションのコード (= NE 品番) は build 側で鍵から取り出す。親のコードは別に持つ
    mall_item_ref: null,
    mall_item_number: String(item.code ?? '').trim() || null,
    fulfillment: 'self',
    ne_code: null,
    price_type: 'normal',
    // 🚨 LINEギフトの価格は税込 (管理画面の表示が税込)。税率は返らないので NE 商品マスタを使う
    price_incl_tax: price,
    price_tax_included: 1,
    price_raw: price,
    mall_tax_rate: null,
    // 🚨 LINEギフトは送料込み (実測 2026-09-20: 直近 90 日の注文 2,483 行すべて shipping_fee が空)
    postage_included: 1,
    postage_revenue_incl_tax: 0,
    points: 0,
    listing_status: active ? 'active' : 'inactive',
    fetch_status: price == null ? 'not_found' : 'ok',
    resolve_status: 'unresolved',
    resolve_reason: null,
    valid_until: validUntil,
    shipping_group: null,
    source: 'linegift_item_detail',
    fetched_at: fetchedAt,
  });

  // 🚨 配列でなければ失敗。「バリエーションなし」と言えるのは空配列が返ったときだけ
  if (!Array.isArray(item.variations)) return { rows: [], unparsable: 1 };
  const variations = item.variations;
  // 実測ではすべての商品に 1 件以上あるが、0 件なら商品 1 行として扱う (親コードで引く)
  if (variations.length === 0) {
    return { rows: [{ ...make(itemId, itemActive), mall_item_ref: String(item.code ?? '').trim() || null }], unparsable: 0 };
  }

  const codes = variations.map((v) => String(v?.code ?? '').trim());
  // 🚨 子コードが読めない / 重なっている商品は、どの子がどれか決められない。
  //    捨てて残りだけ行にすると、捨てた子が黙って消える (Codex R2 の Qoo10 と同じ判断)。
  //    **商品 1 行だけ作り、親のコードも載せない** = どの NE 品番か決めない
  if (codes.some((c) => !c) || new Set(codes).size !== codes.length) {
    return { rows: [{ ...make(itemId, itemActive), source: 'linegift_item_detail:variation_unreadable' }], unparsable: 0 };
  }

  return {
    rows: codes.map((code, i) => make(
      `${itemId}/${code}`,
      itemActive && String(variations[i]?.status ?? '').trim() === LINEGIFT_VARIATION_ACTIVE,
    )),
    unparsable: 0,
  };
}

/**
 * LINEギフトの出品を列挙して価格を取る (2026-09-20)。
 *
 *   ① `GET /api/v1/shops/{shop}/items?page=N&per_page=100` で商品 id を集める
 *   ② id ごとに `GET /api/v1/shops/{shop}/items/{id}` で 価格・バリエーション を取る
 *
 * 実測 2026-09-20: 商品 3,594 件 (sale 3,567 / stop 20 / draft 7)。一覧 36 回 12 秒 + 詳細 約 2 分。
 */
export async function fetchLinegiftListings(db, deps = {}) {
  const runId = newRunId();
  const startedAt = nowIso();
  const shopId = LINEGIFT_SHOP_ID();
  db.prepare(`INSERT INTO price_fetch_run (run_id, mall, started_at, status, listing_enum_status)
              VALUES (?, 'linegift', ?, 'running', 'failed')`).run(runId, startedAt);

  try {
    const listPage = deps.linegiftListPage || defaultLinegiftListPage;
    const detailOf = deps.linegiftDetail || defaultLinegiftDetail;
    const concurrency = deps.linegiftConcurrency || LINEGIFT_DETAIL_CONCURRENCY;
    const pageSize = deps.linegiftPageSize || LINEGIFT_PAGE_SIZE;
    const fetchedAt = nowIso();
    const validUntil = addDays(fetchedAt, PRICE_VALID_DAYS);
    const pastDeadline = () => Boolean(deps.deadline) && new Date() >= deps.deadline;
    const remainingMs = () => (deps.deadline ? deps.deadline.getTime() - Date.now() : null);

    const problems = [];
    let truncated = false;
    let deadlineHit = false;
    const issue = (reason) => problems.push({ api: 'items', reason });

    // ── ① 商品 id の列挙 ──
    const listed = new Map();
    let listCalls = 0;
    let totalCount = null;
    let duplicateListed = 0;
    for (let page = 1; ; page++) {
      if (pastDeadline()) { truncated = true; deadlineHit = true; break; }
      const budget = remainingMs();
      if (budget != null && budget < LINEGIFT_MIN_REQUEST_MS) { truncated = true; deadlineHit = true; break; }
      let res;
      try {
        res = await listPage({ page, perPage: pageSize }, budget == null ? undefined : Math.min(30_000, budget));
      } catch (e) {
        // 🚨 1 ページ落ちても、そこまでに取れた分は捨てない
        issue(`error:${String(e.message).slice(0, 60)}`);
        break;
      }
      listCalls++;
      if (!res || !Array.isArray(res.items)) { issue(`rows_not_array:page${page}`); break; }
      if (totalCount === null) totalCount = res.totalCount;
      else if (res.totalCount !== totalCount) { issue(`total_changed:${totalCount}->${res.totalCount}`); totalCount = res.totalCount; }
      for (const it of res.items) {
        const id = String(it?.id ?? '').trim();
        if (!id) { issue(`item_id_missing:page${page}`); continue; }
        // 🚨 同じ商品が 2 度出てきたら、あとの行で上書きしない
        if (listed.has(id)) { duplicateListed++; continue; }
        listed.set(id, it);
      }
      if (pastDeadline()) { truncated = true; deadlineHit = true; break; }
      if (res.items.length < pageSize) break;
      if (Number.isInteger(totalCount) && listed.size + duplicateListed >= totalCount) break;
      if (page >= 200) { issue('page_limit'); truncated = true; break; }
    }
    if (duplicateListed > 0) issue(`duplicate_items:${duplicateListed}`);
    // 🚨 モールが「◯件ある」と言った数と、受け取った数が違う = 取りこぼし
    if (!Number.isInteger(totalCount) || totalCount < 0) issue('total_unavailable');
    else if (!truncated && listed.size + duplicateListed !== totalCount) {
      issue(`count_mismatch:${listed.size + duplicateListed}/${totalCount}`);
    }

    // ── ② 価格とバリエーション ──
    const rows = [];
    const rawItems = [];
    let unparsable = 0;
    let detailCalls = 0;
    let variationUnreadable = 0;
    const failedItems = [];
    const targets = [...listed.values()];
    for (let i = 0; i < targets.length; i += concurrency) {
      if (pastDeadline()) { truncated = true; deadlineHit = true; break; }
      const budget = remainingMs();
      if (budget != null && budget < LINEGIFT_MIN_REQUEST_MS) { truncated = true; deadlineHit = true; break; }
      const chunk = targets.slice(i, i + concurrency);
      const timeout = budget == null ? undefined : Math.min(30_000, budget);
      const details = await Promise.all(chunk.map(async (it) => {
        try { return { it, detail: await detailOf(it.id, timeout) }; }
        catch (e) { return { it, detail: null, error: String(e.message).slice(0, 120) }; }
      }));
      detailCalls += chunk.length;
      for (const { it, detail, error } of details) {
        rawItems.push(detail || { id: it.id, error });
        const made = linegiftItemToSnapshots(detail, it, { runId, shopId, fetchedAt, validUntil });
        unparsable += made.unparsable;
        if (made.rows.length === 0 || made.unparsable > 0) failedItems.push(String(it.id));
        if (made.rows.some((r) => r.source.endsWith(':variation_unreadable'))) variationUnreadable++;
        rows.push(...made.rows);
      }
    }
    const notAsked = targets.length - detailCalls;

    const currentKeys = new Set(rows.map(r => snapshotKey(r.shop_id, r.mall_item_key)));
    const duplicates = rows.length - currentKeys.size;
    const prev = loadLastCompleteKeys(db, 'linegift');
    const evalResult = enumStatusWithParseFailures(
      evaluateEnumeration(prev.keys, currentKeys), unparsable + problems.length + notAsked, duplicates);
    const enumStatus = evalResult.status === 'failed' ? 'failed' : (truncated ? 'partial' : evalResult.status);
    const summary = enumSummary(evalResult)
      || (deadlineHit ? `全体終了期限に達したので取得を打ち切った (詳細を聞けていない商品 ${notAsked} 件)`
        : (failedItems.length || problems.length
          ? `取りこぼしの疑い: 詳細が取れない ${failedItems.length} 件`
            + (problems.length ? ` / 一覧 ${problems.slice(0, 5).map(x => x.reason).join(' ')}` : '')
          : null));

    const complete = !truncated && problems.length === 0 && failedItems.length === 0
      && notAsked === 0 && unparsable === 0 && duplicates === 0;
    const archive = await archiveListings(deps, {
      mall: 'linegift', shopId, source: 'linegift_item_detail', runId, fetchedAt,
      format: 'ndjson', payload: rawItems, sortKey: (r) => String(r?.item?.id ?? r?.id ?? ''),
      items: rawItems.length,
      meta: {
        api_version: 'gift-shop-cms /api/v1 items',
        complete, enum_status: enumStatus, truncated, deadline_hit: deadlineHit,
        details: {
          list_calls: listCalls, items_enumerated: listed.size, total_count: totalCount,
          detail_calls: detailCalls, detail_failed: failedItems.length, detail_not_asked: notAsked,
          items_variation_unreadable: variationUnreadable,
          rows: rows.length, unparsable, duplicates,
          problems: problems.slice(0, 20), failed_items: failedItems.slice(0, 50),
        },
      },
    });

    insertSnapshots(db, rows);
    db.prepare(`UPDATE price_fetch_run SET finished_at = ?, status = ?, listing_enum_status = ?,
                expected_count = ?, fetched_count = ?, failed_count = ?, disappeared_count = ?,
                error_summary = ? WHERE run_id = ?`)
      .run(nowIso(), enumStatus, enumStatus, listed.size,
        rows.filter(r => r.fetch_status === 'ok').length,
        rows.filter(r => r.fetch_status !== 'ok').length + failedItems.length + notAsked,
        evalResult.disappeared, summary, runId);
    return {
      runId, count: rows.length, items: listed.size, totalCount, listCalls, detailCalls,
      variationUnreadable, detailFailed: failedItems.length, notAsked,
      truncated, deadlineHit, unparsable, duplicates, problems: problems.length,
      ...evalResult, status: enumStatus, archive,
    };
  } catch (e) {
    db.prepare(`UPDATE price_fetch_run SET finished_at = ?, status = 'failed', listing_enum_status = 'failed',
                error_summary = ? WHERE run_id = ?`).run(nowIso(), String(e.message).slice(0, 500), runId);
    throw e;
  }
}

/** LINEギフトの CMS を 1 回叩く。🚨 token は miniPC にしかない */
async function linegiftGet(path, timeoutMs) {
  const token = String(process.env.LINEGIFT_ACCESS_TOKEN || '').trim();
  if (!token) throw new Error('LINEGIFT_ACCESS_TOKEN が未設定です');
  const res = await fetch(`${LINEGIFT_HOST}${path}`, {
    headers: { Authorization: `Bearer ${token}` },
    signal: AbortSignal.timeout(Number.isFinite(timeoutMs) ? timeoutMs : 30_000),
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`LINEギフト ${path} が HTTP ${res.status}: ${text.slice(0, 200)}`);
  let json;
  try { json = JSON.parse(text); } catch { throw new Error(`LINEギフト ${path} の応答が JSON でない: ${text.slice(0, 200)}`); }
  // 🚨 LINEギフトは本文の code でも結果を返す。200 でも code が 200 でなければ失敗として扱う
  if (json?.code != null && Number(json.code) !== 200) {
    throw new Error(`LINEギフト ${path} がエラーを返しました (code=${json.code})`);
  }
  return json;
}

function linegiftShopPath() {
  const shop = String(process.env.LINEGIFT_SHOP_ID || '').trim();
  if (!/^\d+$/.test(shop)) throw new Error('LINEGIFT_SHOP_ID (数字) が未設定です');
  return `/api/v1/shops/${shop}`;
}

async function defaultLinegiftListPage({ page, perPage }, timeoutMs) {
  const json = await linegiftGet(`${linegiftShopPath()}/items?page=${page}&per_page=${perPage}`, timeoutMs);
  return {
    items: Array.isArray(json?.items) ? json.items : null,
    totalCount: Number.isInteger(json?.total_count) ? json.total_count : null,
  };
}

async function defaultLinegiftDetail(itemId, timeoutMs) {
  return linegiftGet(`${linegiftShopPath()}/items/${encodeURIComponent(String(itemId))}`, timeoutMs);
}

// ────────────────────────────────────────────────────────────
if (process.argv[1] && process.argv[1].endsWith('fetch-listings.js')) {
  const mall = process.argv.includes('--mall') ? process.argv[process.argv.indexOf('--mall') + 1] : null;
  const db = initExpectedProfitDB();
  const run = async () => {
    if (!mall || mall === 'amazon') console.log('[amazon]', JSON.stringify(await fetchAmazonListings(db)));
    if (!mall || mall === 'rakuten') console.log('[rakuten]', JSON.stringify(await fetchRakutenListings(db)));
    if (!mall || mall === 'yahoo') console.log('[yahoo]', JSON.stringify(await fetchYahooListings(db)));
    if (!mall || mall === 'aupay') console.log('[aupay]', JSON.stringify(await fetchAupayListings(db)));
    if (!mall || mall === 'qoo10') console.log('[qoo10]', JSON.stringify(await fetchQoo10Listings(db)));
    if (!mall || mall === 'linegift') console.log('[linegift]', JSON.stringify(await fetchLinegiftListings(db)));
  };
  run().then(() => db.close()).catch(e => { console.error(e); process.exit(1); });
}
