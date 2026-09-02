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
 *   ★VPS の /wmshopapi/ 中継は **GET だけ**。書き込みは価格しか通れない
 *     /aupay/update-item を通る。
 *
 * 提供:
 *   fetchAupayItemDetail(itemCode) … 商品1件を読む (価格・税・発送・カラバリの数)
 *   planAupayUpdate(...)           … 送る前の判定 (楽観ロック・取り違え防止)
 *   makeAupayClient(deps)          … execute.js から楽天・Yahoo と同じ形で呼ばれる
 */

import { parseString } from 'xml2js';

const DEFAULT_TIMEOUT_MS = 20_000;
/** au PAY の既定店舗ID (aupay-orders.js と同じ値。env で上書きできる) */
const DEFAULT_SHOP_ID = '54318092';
/**
 * 送ってよい価格の上限。
 * ★VPS 側 (vps-proxy/aupay-proxy.js の MAX_AUPAY_PRICE) と同じ値。
 *   片方だけ緩いと、通ったあとに向こうで落ちて「結果不明」になる
 */
export const MAX_AUPAY_PRICE = 999999999;

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
 *   choiceCount:number|null, lotNumber:string|null}}
 *   choiceCount … バリエーションの数。0 = 無し / null = 数えられなかった (応答に在庫情報が無い)
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

/**
 * 送る前の判定。miniPC の楽天エンドポイントと同じ形で返すので、
 * execute.js の classify() / 試運転 / ブレーカー がそのまま効く。
 *
 * ★au PAY は「商品」に1つの価格しか持たない。カラバリ (registerStock) は在庫だけで
 *   価格の項目が無い。だから **送り先のキーは商品コード**。
 *   色のコードが渡ってきたら送らない (商品の全色を書き換えたのに、
 *   送った後の照合は色のコードで探す、という食い違いを作らない)。
 *
 * @param {object} detail fetchAupayItemDetail の戻り
 * @param {string} itemCode 送ろうとしている商品コード
 * @param {Record<string, number>} expected 記録した時の価格
 * @param {Record<string, number>} prices 送りたい価格
 */
export function planAupayUpdate(detail, itemCode, expected, prices) {
  const bad = (status, error, message, extra = {}) => ({ ok: false, status, body: { ok: false, error, message, ...extra } });

  if (!detail || detail.ok === false) {
    return bad(404, 'ITEM_NOT_FOUND',
      `au PAY でこの商品を取得できませんでした (${itemCode}${detail?.message ? ': ' + detail.message : ''})`);
  }
  // ★取ってきた商品が本当に送ろうとしている商品か (別商品に値付けしない)
  if (String(detail.itemCode || '').trim().toLowerCase() !== String(itemCode).trim().toLowerCase()) {
    return bad(404, 'ITEM_NOT_FOUND',
      `取得した商品コードが違います (要求 ${itemCode} / 応答 ${detail.itemCode ?? 'なし'})`);
  }

  const wantKeys = Object.keys(prices || {});
  if (wantKeys.length !== 1) {
    return bad(400, 'MULTIPLE_PRICES',
      `au PAY は商品ごとに1つの価格しか送りません (受け取った数: ${wantKeys.length})`);
  }
  const key = wantKeys[0];
  // ★送るキーは商品コードでなければならない (Yahoo と同じ理由。最後の安全弁)
  if (String(key).trim().toLowerCase() !== String(detail.itemCode).trim().toLowerCase()) {
    return bad(400, 'SKU_KEY_MISMATCH',
      `au PAY は商品に1つの価格しか持ちません。送る先は商品コード (${detail.itemCode}) `
      + `でなければなりませんが、${key} が渡されました`);
  }

  const next = toIntPrice(prices[key]);
  // ★上限は VPS 側 (MAX_AUPAY_PRICE) と同じ値にそろえる。ここが緩いと、
  //   アプリの検査を通ったあと VPS で例外になり、HTTP 500 =「結果不明」として
  //   ブレーカーが止まり復旧の対象にもなる (実際には1件も送っていないのに)
  if (next === null || next < 1 || next > MAX_AUPAY_PRICE) {
    return bad(400, 'INVALID_PRICE',
      `送ろうとした価格が 1〜${MAX_AUPAY_PRICE} の整数円ではありません (${prices[key]})`);
  }
  const current = toIntPrice(detail.itemPrice);
  if (current === null) {
    return bad(400, 'CURRENT_PRICE_UNREADABLE', 'いまの価格を整数円として読めません');
  }
  // ★楽観ロック。記録した時の価格と今の価格が違えば送らない (誰かの変更を踏み潰さない)
  const want = toIntPrice(expected?.[key]);
  if (want === null) {
    return bad(400, 'EXPECTED_REQUIRED', '記録時の価格が分からないため送りません');
  }
  if (want !== current) {
    return {
      ok: false, status: 409,
      body: {
        ok: false, state: 'conflict', error: 'CONFLICT',
        message: '現在価格が想定と違います',
        detail: { conflicts: [{ sku: key, expected: want, live: current, reason: '現在価格が想定と違います' }] },
      },
    };
  }
  return { ok: true, currentPrice: current, noop: current === next, sku: key, price: next };
}

/**
 * au PAY 用のクライアント。execute.js からは楽天・Yahoo と同じ形で呼ばれる。
 *
 * ★au PAY の updateItemInfo は **部分更新** (2026-09-02 実測)。
 *   価格を 980 → 981 → 980 と動かしても、商品名・説明・画像・カテゴリは 1 バイトも変わらなかった。
 *   Yahoo の editItem (省略した項目を消す) とは違うので、価格だけ送ってよい。
 *
 * ⚠️フロント反映について: au PAY の API 一覧に Yahoo の submitItem にあたるものが見当たらず、
 *   更新した直後に searchItemInfo を読むと新しい価格が返ってきた。
 *   ただしそれで分かるのは **管理側に反映された** ことまでで、
 *   買う人が見る商品ページに出たかどうかは確かめていない (Codex R1)。
 *   本番の商品で1件通すときに、商品ページを目で見て確かめること。
 *
 * @param {object} deps テスト用の差し替え ({ getDetail / postUpdate })
 */
export function makeAupayClient(deps = {}) {
  const getDetail = deps.getDetail || ((code) => fetchAupayItemDetail(code));
  const postUpdate = deps.postUpdate || defaultAupayPostUpdate;

  return {
    /**
     * 照合のための再取得。楽天と同じ形にそろえる。
     * ★au PAY は商品に1つの価格なので、variants のキーは商品コードそのもの
     */
    async fetchItemDetail(itemCode) {
      const d = await getDetail(itemCode);
      if (!d || d.ok === false || d.itemPrice === null || d.itemPrice === undefined) {
        return { item: null, status: 'not_found' };
      }
      // ★キーは「応答の商品コード」と「問い合わせた商品コード」の両方で引けるようにする。
      //   応答の書き方だけを使うと、要求と大小が違った時に照合が必ず外れる
      //   (更新は通っているのに失敗として記録される)
      const price = { standardPrice: String(d.itemPrice) };
      const variants = { [d.itemCode]: price };
      if (String(itemCode).trim() !== String(d.itemCode).trim()) variants[String(itemCode).trim()] = price;
      return { item: { manageNumber: d.itemCode, variants }, status: 'found' };
    },

    /** 価格を送る。応答は楽天エンドポイントと同じ形にそろえる */
    async patchItemPrices(itemCode, { expected, prices }) {
      const detail = await getDetail(itemCode);
      const plan = planAupayUpdate(detail, itemCode, expected, prices);
      if (!plan.ok) return { status: plan.status, body: plan.body };

      if (plan.noop) {
        return { status: 200, body: { ok: true, state: 'noop', applied: {} } };
      }
      const res = await postUpdate(itemCode, plan.price, plan.currentPrice);
      if (res.status >= 200 && res.status < 300 && res.json?.ok === true) {
        return {
          status: 200,
          body: { ok: true, state: 'applied', applied: { [plan.sku]: plan.price } },
        };
      }
      // ★VPS が「今の価格が想定と違う」と言ってきた時は conflict として返す。
      //   ひとまとめに失敗へ潰すと、誰かの変更とぶつかったのかが記録から分からなくなる
      if (res.status === 409 && res.json?.error === 'CONFLICT') {
        const c = res.json.conflict || {};
        return {
          status: 409,
          body: {
            ok: false, state: 'conflict', error: 'CONFLICT',
            message: c.reason || '現在価格が想定と違います',
            detail: { conflicts: [{ sku: plan.sku, expected: c.expected ?? plan.currentPrice, live: c.live ?? null, reason: c.reason || '現在価格が想定と違います' }] },
          },
        };
      }
      const message = String(res.json?.updateBody || res.body || '').slice(0, 300).replace(/\s+/g, ' ');
      if (res.status >= 400 && res.status < 500) {
        return { status: 400, body: { ok: false, error: 'AUPAY_REJECTED', message } };
      }
      return { status: res.status, body: { ok: false, message } };
    },
  };
}

/** VPS の /aupay/update-item を叩く */
async function defaultAupayPostUpdate(itemCode, price, expectedPrice) {
  const base = requireEnv('AUPAY_PROXY_BASE_URL', 'AUPAY_PROXY_URL').replace(/\/+$/, '');
  const secret = requireEnv('AUPAY_PROXY_SECRET');
  const res = await fetch(`${base}/aupay/update-item`, {
    method: 'POST',
    headers: { 'X-Proxy-Secret': secret, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      itemCode,
      itemPrice: price,
      // ★VPS 側でも「送る直前に読み直して照合」してもらう。
      //   こちら側で読んでから送るまでの間に誰かが変えていたら、VPS が 409 で止める
      expected: expectedPrice,
    }),
    signal: AbortSignal.timeout(30_000),
  });
  const text = await res.text();
  let json = null;
  try { json = JSON.parse(text); } catch { /* JSON でない応答はそのまま扱う */ }
  return { status: res.status, body: text, json };
}
