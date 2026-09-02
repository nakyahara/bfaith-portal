/**
 * aupay-apply.js — au PAY マーケット (Wowma) の商品価格クライアント
 *
 * ★au PAY は Yahoo と同じで「商品」に1つの価格しか持たない。
 *   カラバリは `registerStock` 側 (choicesStockHorizontals / choicesStockVerticals) にあり、
 *   そこに価格の項目は無く **在庫数だけ**。つまり色ごとの値付けはできず、
 *   `itemPrice` を変えると **その商品の全ての色が同じ価格になる**。
 *   (2026-09-01 実測: 0726-001802 の応答に choicesStock の価格項目が存在しない)
 *   → 送り先のキーは **商品コード (itemCode)**。Yahoo と同じ扱いにする。
 *
 * 経路: Render → VPS プロキシ (固定IP 133.167.122.198) → api.manager.wowma.jp
 *   au PAY は API キー方式で、データセンターIP でないと通らないため VPS 必須。
 *   ★VPS の /wmshopapi/ 中継は **GET だけ**。価格を書くには専用の口を足す (M4-2)。
 *
 * 提供:
 *   fetchAupayItemDetail(itemCode) … 商品1件を読む (価格・税・発送・カラバリの色数)
 */

import { parseString } from 'xml2js';

const DEFAULT_TIMEOUT_MS = 20_000;
/** au PAY の既定店舗ID (aupay-orders.js と同じ値。env で上書きできる) */
const DEFAULT_SHOP_ID = '54318092';

export class AupayProxyError extends Error {
  constructor(kind, message, extra = {}) {
    super(message);
    this.name = 'AupayProxyError';
    this.kind = kind;
    Object.assign(this, extra);
  }
}

function requireEnv(name, fallbackName = null) {
  const v = process.env[name] || (fallbackName ? process.env[fallbackName] : null);
  if (!v || !String(v).trim()) {
    const err = new AupayProxyError('config',
      `${name}${fallbackName ? ` (または ${fallbackName})` : ''} が未設定です。au PAY は送信も取得もしません`);
    err.statusCode = 503;
    throw err;
  }
  return String(v).trim();
}

/**
 * XML を素直に木にする。
 *
 * ★正規表現で拾ってはいけない。au PAY の応答には商品説明がそのまま入っていて、
 *   CDATA の中に `<itemPrice>9999</itemPrice>` のような文字列があると
 *   **説明文をメーカー希望小売価格や設定価格として読んでしまう**。
 *   CDATA 内の `</itemInfo>` で範囲を途中で切ることもできる (Codex R1 高)。
 *   xml2js は既に依存にあるので素直に使う。16KB は木にしても問題ない大きさ。
 *
 * @param {string} xml
 * @returns {object|null} 解析できなければ null
 */
function parseXml(xml) {
  let out = null;
  let err = null;
  // string を渡すと callback は同期で呼ばれる (async:false を明示しておく)
  parseString(xml, { explicitArray: true, trim: true, async: false }, (e, r) => { err = e; out = r; });
  return err ? null : out;
}

/** 木から1つ目の値を取り出す。空文字は null (「空で登録されている」と「無い」を同じに扱う) */
function first(node, tag) {
  const v = node?.[tag];
  if (!Array.isArray(v) || v.length === 0) return null;
  const x = v[0];
  if (x === null || x === undefined) return null;
  // xml2js は空要素を {} で返すことがある
  if (typeof x === 'object') return null;
  const s = String(x).trim();
  return s === '' ? null : s;
}

/** 整数円として読めれば数値、読めなければ null (「0円」と「読めない」を混ぜない) */
export function toIntPrice(v) {
  if (v === null || v === undefined) return null;
  const s = String(v).trim().replace(/,/g, '');
  if (!/^\d+$/.test(s)) return null;
  const n = Number(s);
  return Number.isInteger(n) ? n : null;
}

/**
 * searchItemInfo の応答 (XML) を、必要な項目だけの形にする。
 *
 * ★`itemInfo` の中だけを見る。`registerStock` にも似た名前のタグがあるため、
 *   範囲を切らずに探すと別の値を拾う。
 * ★`makerRetailPrice` (メーカー希望小売価格) を価格と取り違えない。
 *
 * @param {string} xml
 * @returns {{ok:boolean, error?:string, message?:string, itemCode:string|null, itemPrice:number|null,
 *   itemPriceReadable:boolean, itemName:string|null, taxSegment:string|null, postageSegment:string|null,
 *   postage:number|null, deliveryMethodName:string|null, saleStatus:string|null,
 *   choiceCount:number, lotNumber:string|null}}
 */
export function parseSearchItemInfoXml(xml) {
  const bad = (error, message) => ({
    ok: false, error, message, itemCode: null, itemPrice: null, itemPriceReadable: false,
    itemName: null, taxSegment: null, postageSegment: null, postage: null,
    deliveryMethodName: null, saleStatus: null, choiceCount: 0, lotNumber: null,
  });
  if (typeof xml !== 'string' || xml.trim() === '') return bad('EMPTY_RESPONSE', '応答が空です');

  const doc = parseXml(xml);
  const root = doc?.response;
  if (!root) return bad('UNREADABLE_RESPONSE', '応答を XML として読めませんでした');

  // au PAY は失敗も HTTP 200 + <status>1</status> で返すことがある。status を先に見る
  const result = root.result?.[0];
  const status = first(result, 'status');
  if (status !== '0') {
    const err = result?.error?.[0];
    const code = first(err, 'code');
    const message = first(err, 'message');
    return bad('AUPAY_ERROR',
      `au PAY がエラーを返しました (status=${status ?? 'なし'}`
      + `${code ? ` / ${code}` : ''}${message ? `: ${message}` : ''})`);
  }

  const searchResult = root.searchResult?.[0];
  const info = searchResult?.itemInfo?.[0];
  if (!info) return bad('ITEM_NOT_FOUND', 'この商品コードの商品が見つかりません');

  // カラバリの組み合わせ数。au PAY のカラバリは在庫だけで価格を持たないので、
  // 「価格を変えると何通りが影響を受けるか」を数えるために使う。
  // ★数えるのは choicesStocks (= 実際の在庫行 = 縦×横の組み合わせ) だけ。
  //   choicesStockVerticalCode / HorizontalCode は「選択肢の定義」「在庫行」「画像」の
  //   3か所に出てくるので、それを数えると何倍にもなる
  //   (実測 2026-09-01: 6通りの商品で 縦18 + 横7 = 25 と出てしまっていた)
  // ★registerStock そのものが応答に無い時は **null (分からない)**。0 にすると
  //   「カラバリが無い」と区別がつかず、警告を出すべき商品を黙って通しうる。
  //   中身が空の <registerStock></registerStock> は「在庫の枠はあるが選択肢が無い」= 0。
  //   xml2js は空要素を '' で返すので、値の真偽ではなく **キーの有無** で見る
  const hasStock = Object.prototype.hasOwnProperty.call(searchResult || {}, 'registerStock');
  const stock = searchResult?.registerStock?.[0];
  const choiceCount = !hasStock ? null
    : (stock && Array.isArray(stock.choicesStocks) ? stock.choicesStocks.length : 0);

  const rawPrice = first(info, 'itemPrice');
  const itemPrice = toIntPrice(rawPrice);
  return {
    ok: true,
    itemCode: first(info, 'itemCode'),
    itemPrice,
    // 「値が返ってきたが整数円として読めない」と「そもそも返ってこない」を区別する
    itemPriceReadable: rawPrice !== null,
    itemName: first(info, 'itemName'),
    taxSegment: first(info, 'taxSegment'),
    postageSegment: first(info, 'postageSegment'),
    postage: toIntPrice(first(info, 'postage')),
    deliveryMethodName: first(info.deliveryMethod?.[0], 'deliveryMethodName'),
    saleStatus: first(info, 'saleStatus'),
    choiceCount,
    lotNumber: first(info, 'lotNumber'),
  };
}

/**
 * au PAY の商品を1件読む。
 * @param {string} itemCode
 * @param {object} [deps] テスト用の差し替え ({ fetchImpl })
 * @returns {Promise<object>} parseSearchItemInfoXml の戻り
 */
export async function fetchAupayItemDetail(itemCode, deps = {}) {
  const code = String(itemCode ?? '').trim();
  if (!code) throw new AupayProxyError('input', 'itemCode が空です');

  const base = requireEnv('AUPAY_PROXY_BASE_URL', 'AUPAY_PROXY_URL').replace(/\/+$/, '');
  const secret = requireEnv('AUPAY_PROXY_SECRET');
  const shopId = (process.env.AUPAY_SHOP_ID || DEFAULT_SHOP_ID).trim();

  const qs = new URLSearchParams({ shopId, itemCode: code });
  const url = `${base}/wmshopapi/searchItemInfo?${qs}`;
  const fetchImpl = deps.fetchImpl || fetch;

  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), deps.timeoutMs || DEFAULT_TIMEOUT_MS);
  let res;
  try {
    res = await fetchImpl(url, { headers: { 'X-Proxy-Secret': secret }, signal: ac.signal });
  } catch (e) {
    throw new AupayProxyError('network', `[aupay-proxy:searchItemInfo] ${e.message}`);
  } finally {
    clearTimeout(timer);
  }
  const text = await res.text();
  if (!res.ok) {
    throw new AupayProxyError('http', `[aupay-proxy:searchItemInfo] HTTP ${res.status}: ${text.slice(0, 300)}`,
      { statusCode: res.status });
  }
  return parseSearchItemInfoXml(text);
}
