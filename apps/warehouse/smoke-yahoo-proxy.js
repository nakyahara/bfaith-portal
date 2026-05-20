/**
 * smoke-yahoo-proxy.js — VPS proxy の Yahoo orderInfo 動作確認
 *
 * VPS proxy (aupay-proxy.js) を deploy / secret rotation / 設定変更した直後に必ず実行する。
 * 5/8-10 の regression 事故 (orderInfo の XML 構造が壊れて 3 日間 raw 全空文字) の再発防止。
 *
 * 動作:
 *   1. proxy /yahoo/health で hasTokens 確認
 *   2. orderList で過去 N 日 (default 7) の注文 ID リスト取得
 *   3. その中から「単一明細注文」「複数明細 or SubCode あり注文」を 1 件ずつ pick
 *   4. それぞれ orderInfo で取得 → OrderTime / OrderStatus / Item.ItemId / Item.Quantity / Item.UnitPrice が
 *      全部埋まっていることを verify
 *   5. 1 件でも空 / orderInfo が <Error> を返した → exit 1
 *
 * 使い方:
 *   node apps/warehouse/smoke-yahoo-proxy.js [days]
 *
 * env:
 *   YAHOO_PROXY_URL    (default http://133.167.122.198:8080)
 *   YAHOO_PROXY_SECRET (or AUPAY_PROXY_SECRET、共通)
 */
import 'dotenv/config';
import { parseStringPromise } from 'xml2js';

const YAHOO_PROXY_URL = process.env.YAHOO_PROXY_URL || 'http://133.167.122.198:8080';
const YAHOO_PROXY_SECRET = process.env.YAHOO_PROXY_SECRET || process.env.AUPAY_PROXY_SECRET || '';
const YAHOO_TOKEN_MINT_SECRET = process.env.YAHOO_TOKEN_MINT_SECRET || '';
// STRICT mode: /yahoo/access-token を verify 必須にする。本番 deploy 手順では必ず STRICT=1 を付ける。
const STRICT_YAHOO_ACCESS_TOKEN_SMOKE = process.env.STRICT_YAHOO_ACCESS_TOKEN_SMOKE === '1';

function fail(msg) {
  console.error(`[smoke-yahoo-proxy] ❌ FAIL: ${msg}`);
  process.exit(1);
}
function ok(msg) {
  console.log(`[smoke-yahoo-proxy] ✓ ${msg}`);
}

async function proxyGet(path) {
  const res = await fetch(`${YAHOO_PROXY_URL}${path}`, { headers: { 'X-Proxy-Secret': YAHOO_PROXY_SECRET } });
  if (!res.ok) throw new Error(`proxy ${res.status}: ${await res.text()}`);
  return res;
}
async function proxyPost(path, body) {
  const res = await fetch(`${YAHOO_PROXY_URL}${path}`, {
    method: 'POST',
    headers: { 'X-Proxy-Secret': YAHOO_PROXY_SECRET, 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`proxy ${res.status}: ${await res.text()}`);
  return res.json();
}
function ymd(d) { return d.toISOString().slice(0, 10).replace(/-/g, ''); }

async function main() {
  if (!YAHOO_PROXY_SECRET) fail('YAHOO_PROXY_SECRET (or AUPAY_PROXY_SECRET) 未設定');

  const days = parseInt(process.argv[2]) || 7;

  // 1. health
  let health;
  try {
    const r = await proxyGet('/yahoo/health');
    health = await r.json();
  } catch (e) {
    fail(`health 取得失敗: ${e.message}`);
  }
  if (!health.hasTokens) fail('proxy に Yahoo トークン未初期化');
  ok(`health OK (seller=${health.sellerId}, refresh token expires ${health.refreshTokenExpiresAt})`);

  // 1.5. access-token (used by Render/local clients to call Yahoo product APIs directly)
  //      mint endpoint は YAHOO_TOKEN_MINT_SECRET 専用 secret 必須。
  //      STRICT_YAHOO_ACCESS_TOKEN_SMOKE=1 のとき未設定で fail (本番 deploy 検証用)。
  if (!YAHOO_TOKEN_MINT_SECRET) {
    if (STRICT_YAHOO_ACCESS_TOKEN_SMOKE) {
      fail('STRICT mode: YAHOO_TOKEN_MINT_SECRET 未設定 (本番 deploy では必須)');
    }
    console.log('[smoke-yahoo-proxy] ⚠️  YAHOO_TOKEN_MINT_SECRET 未設定: /yahoo/access-token の verify をスキップ (STRICT=1 で fail に切り替え)');
  } else {
    let tokenRes;
    try {
      tokenRes = await fetch(`${YAHOO_PROXY_URL}/yahoo/access-token`, {
        method: 'POST',
        headers: {
          'X-Proxy-Secret': YAHOO_PROXY_SECRET,
          'X-Token-Mint-Secret': YAHOO_TOKEN_MINT_SECRET,
          'Content-Type': 'application/json',
        },
      });
    } catch (e) {
      fail(`access-token 取得失敗 (network): ${e.message}`);
    }
    if (!tokenRes.ok) {
      const body = await tokenRes.text().catch(() => '');
      fail(`access-token HTTP ${tokenRes.status}: ${body.slice(0, 200)}`);
    }
    const cc = tokenRes.headers.get('cache-control') || '';
    if (!/no-store/i.test(cc)) {
      fail(`access-token: Cache-Control に no-store が無い (got: "${cc}")`);
    }
    let tokenData;
    try { tokenData = await tokenRes.json(); }
    catch (e) { fail(`access-token: JSON parse 失敗: ${e.message}`); }
    if (!tokenData?.ok || typeof tokenData.access_token !== 'string' || !tokenData.access_token) {
      fail(`access-token レスポンス異常 (ok=${tokenData?.ok} access_token=${typeof tokenData?.access_token})`);
    }
    if (tokenData.token_type !== 'Bearer') fail(`access-token: token_type が Bearer ではない (${tokenData.token_type})`);
    if (!tokenData.expires_at) fail('access-token: expires_at が無い');
    ok(`access-token OK (refreshed=${tokenData.refreshed}, expires_at=${tokenData.expires_at}, cache-control=${cc})`);

    // 認証エラー verify (誤った mint secret は 401 で fail-closed)
    const wrongRes = await fetch(`${YAHOO_PROXY_URL}/yahoo/access-token`, {
      method: 'POST',
      headers: {
        'X-Proxy-Secret': YAHOO_PROXY_SECRET,
        'X-Token-Mint-Secret': 'wrong-secret-' + Date.now(),
        'Content-Type': 'application/json',
      },
    });
    if (wrongRes.status !== 401) {
      fail(`access-token: 誤った mint secret なのに HTTP ${wrongRes.status} (期待は 401)`);
    }
    ok('access-token mint-secret fail-closed OK (401 returned for wrong secret)');
  }

  // 2. orderList
  const endDate = new Date();
  const startDate = new Date();
  startDate.setDate(startDate.getDate() - days);
  let orderInfos = [];
  try {
    const r = await proxyGet(`/yahoo/orderList?startDate=${ymd(startDate)}&endDate=${ymd(endDate)}`);
    const xml = await r.text();
    const parsed = await parseStringPromise(xml, { explicitArray: false });
    const root = parsed?.Result || parsed?.ResultSet || parsed;
    if (root?.Error || root?.Status && root.Status !== 'OK' && root.Status !== '0') {
      fail(`orderList が Error: ${JSON.stringify(root?.Error || root?.Status)}`);
    }
    const search = root?.Search || root;
    orderInfos = search?.OrderInfo || [];
    if (!Array.isArray(orderInfos)) orderInfos = [orderInfos];
  } catch (e) {
    fail(`orderList 取得失敗: ${e.message}`);
  }
  const orderIds = orderInfos.map(oi => oi?.OrderId).filter(Boolean);
  if (orderIds.length === 0) fail(`過去 ${days} 日に注文 0 件 (smoke できない、days を増やすか時期を変える)`);
  ok(`orderList OK (${orderIds.length} 件)`);

  // 3-4. 各注文を orderInfo で取得して「単一明細」「複数明細/SubCode」を見つけ次第 verify
  //      (全件は叩かず、両ケース見つかったら終了。最大 30 件まで試す)
  let singleVerified = false, multiVerified = false;
  const requiredOrderFields = ['OrderTime', 'OrderStatus'];
  const requiredItemFields = ['ItemId', 'Quantity', 'UnitPrice'];

  for (const oid of orderIds.slice(0, 30)) {
    if (singleVerified && multiVerified) break;
    let data;
    try {
      const res = await proxyPost('/yahoo/orderInfo', { orderIds: [oid] });
      const r0 = res?.results?.[0];
      if (!r0?.xml) fail(`orderInfo ${oid}: xml が空`);
      data = await parseStringPromise(r0.xml, { explicitArray: false });
    } catch (e) {
      fail(`orderInfo ${oid} 取得/parse 失敗: ${e.message}`);
    }
    // <Error> 検知
    if (data?.Error || data?.ResultSet?.Error || data?.Result?.Error) {
      const err = data.Error || data.ResultSet?.Error || data.Result?.Error;
      fail(`orderInfo ${oid} が Error: code=${err?.Code} msg=${err?.Message}`);
    }
    const resultSet = data?.ResultSet || data?.result || data;
    const result = resultSet?.Result || {};
    const orderInfo = result?.OrderInfo || resultSet?.OrderInfo || resultSet?.Order || resultSet;
    if (!orderInfo || typeof orderInfo !== 'object') fail(`orderInfo ${oid}: OrderInfo オブジェクト無し`);

    // 注文ヘッダ必須 field
    for (const f of requiredOrderFields) {
      if (!orderInfo[f]) fail(`orderInfo ${oid}: ${f} が空 (5/8-10 同型 regression の疑い)`);
    }

    // 明細
    let items = orderInfo.Item || orderInfo.Items?.Item || [];
    if (!Array.isArray(items)) items = [items];
    if (!items.length || !items[0]) fail(`orderInfo ${oid}: 明細が空`);
    for (const item of items) {
      for (const f of requiredItemFields) {
        const v = item[f];
        // ItemId は CDATA 経由で { _: '...' } になることがある (xml2js)
        const sval = (v && typeof v === 'object') ? (v._ ?? '') : (v ?? '');
        if (sval === '' || (f !== 'ItemId' && (parseFloat(sval) || 0) <= 0)) {
          fail(`orderInfo ${oid} item LineId=${item.LineId}: ${f} が空/0 ('${sval}')`);
        }
      }
    }

    const hasSubCode = items.some(it => {
      const sc = it.SubCode;
      return sc && (typeof sc === 'object' ? sc._ : sc);
    });
    if (items.length === 1 && !singleVerified) {
      ok(`単一明細注文 ${oid} verify OK (OrderTime/Status/ItemId/Quantity/UnitPrice 全埋まり)`);
      singleVerified = true;
    }
    if ((items.length >= 2 || hasSubCode) && !multiVerified) {
      ok(`複数明細/SubCode 注文 ${oid} verify OK (${items.length} line${hasSubCode ? ', SubCode あり' : ''})`);
      multiVerified = true;
    }
  }

  if (!singleVerified) console.log('[smoke-yahoo-proxy] ⚠️  過去 ' + days + ' 日に単一明細注文が見つからず (verify スキップ)');
  if (!multiVerified) console.log('[smoke-yahoo-proxy] ⚠️  過去 ' + days + ' 日に複数明細/SubCode 注文が見つからず (verify スキップ)');
  if (!singleVerified && !multiVerified) fail('verify できる注文が 1 件も見つからなかった');

  console.log('[smoke-yahoo-proxy] ✅ Yahoo orderInfo proxy 動作確認 OK');
}

main().catch(e => fail(e.message));
