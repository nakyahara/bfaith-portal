/**
 * linegift-item-probe.js — LINEギフト CMS API の商品取得が、いまのトークンで叩けるかを確かめる
 *
 * ★これは「読むだけ」の実証スクリプト。**書き込みは一切しない** (GET のみ・DB も読むだけ)。
 *
 * ## 何を確かめたいか (2026-09-03)
 * 価格一括改定ツールに LINEギフトを「表示だけ」で足したい。そのために
 *   GET https://gift-shop-cms.line.biz/api/v1/shops/{shop_id}/items/{item_id}
 * が **いま持っている OAuth アクセストークンで叩けるか** を先に確かめる。
 *
 * 未確定だった点:
 *   - 受注 API は `shop-mall.line.me` だが、商品 API は `gift-shop-cms.line.biz` と**ホストが違う**
 *   - Swagger の 403 に `csrf_token_not_found` があり、**ブラウザのセッション専用**の可能性が残る
 *   - 401 の説明が「セッション失効**または**トークン無効」と両方に言及している
 *
 * ## 実験の組み立て (対照を必ず取る)
 *   ①受注 API (動くと分かっている) を同じトークンで叩く  ← **対照**。ここが 401 なら
 *     「CMS が拒んだ」のではなく「トークンが期限切れ」なだけ。切り分けを間違えない
 *   ②CMS の商品 API を、実在する item_id で叩く          ← 本命
 *   ③CMS の商品 API を、存在しない item_id で叩く        ← **404 が返れば認証は通っている**
 *      (401 と 404 の differ で「認証を抜けたか」が分かる)
 *   ④渡し方を2通り試す: `?access_token=` (受注 API と同じ形) と `Authorization: Bearer`
 *
 * ## 🚨やらないこと
 *   - **トークンの更新 (refresh) はしない**。refresh_token は one-time で、取りこぼすと
 *     再認可が要る。更新は本来の linegift-orders.js に任せる (atomic 化されている)
 *   - トークンそのものは**絶対に出力しない** (長さと先頭2文字だけ出す)
 *
 * ## 使い方 (miniPC で実行)
 *   node apps/warehouse/linegift-item-probe.js --shop <ショップID>
 *   node apps/warehouse/linegift-item-probe.js --shop <ショップID> --item <商品ID>
 *
 *   ショップIDは LINEギフト管理画面で商品を開いたときの URL から読める:
 *     https://gift-shop-cms.line.biz/shops/<ショップID>/items/<商品ID>
 *   --item を省くと raw_linegift_orders の直近の受注から実在する item_id を1つ選ぶ。
 *
 * env: LINEGIFT_ACCESS_TOKEN (.env)
 */
import 'dotenv/config';
import { initDB, getDB } from './db.js';

const CMS_HOST = 'https://gift-shop-cms.line.biz';
const ORDER_HOST = 'https://shop-mall.line.me';
const TIMEOUT_MS = 20_000;
/** 存在しないはずの商品ID。401 と 404 を見分けるためだけに使う */
const NONEXISTENT_ITEM_ID = 1;

function arg(name) {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : null;
}

/** ★トークンは出さない。「入っているか」だけ分かる形にする */
function tokenHint(t) {
  if (!t) return '(未設定)';
  return `${t.slice(0, 2)}…(${t.length}文字)`;
}

async function get(url, { bearer = null } = {}) {
  const headers = { Accept: 'application/json' };
  if (bearer) headers.Authorization = `Bearer ${bearer}`;
  try {
    const res = await fetch(url, { headers, signal: AbortSignal.timeout(TIMEOUT_MS) });
    const text = await res.text();
    let body = null;
    try { body = JSON.parse(text); } catch { /* JSON でなければ生のまま扱う */ }
    return { http: res.status, body, text };
  } catch (e) {
    return { http: null, error: e.message };
  }
}

/**
 * 応答を1行にまとめる。
 * ★LINEギフトは HTTP 200 で本文に {"code":401} を返すことがある (Swagger の記載どおり)。
 *   HTTP だけ見て「通った」と判断しない。**本文の code を正とする**
 */
function verdict(r) {
  if (r.http == null) return `送信できず (${r.error})`;
  const code = r.body && typeof r.body.code === 'number' ? r.body.code : null;
  const reason = r.body?.reason ? ` reason=${r.body.reason}` : '';
  const effective = code ?? r.http;
  const label = { 200: '成功', 401: '認証NG', 403: '権限NG', 404: '対象なし' }[effective] || '?';
  return `HTTP ${r.http} / code ${code ?? '(なし)'} → ${label}${reason}`;
}

/** 商品の中身のうち、価格まわりだけを取り出す (個人情報は無い) */
function summarizeItem(body) {
  const it = body?.item;
  if (!it) return null;
  return {
    id: it.id, code: it.code, name: it.name, status: it.status,
    price: it.price, sale_price: it.sale_price, sale_id: it.sale_id,
    variations: (it.variations || []).map((v) => ({
      code: v.code, name_main: v.name_main, name_sub: v.name_sub,
      stock_count: v.stock_count, status: v.status,
      // ★ここに price が無いことを確かめたい (無ければ「1商品1価格」で確定)
      hasPrice: Object.prototype.hasOwnProperty.call(v, 'price'),
    })),
    updated_on: it.updated_on,
  };
}

async function main() {
  const token = String(process.env.LINEGIFT_ACCESS_TOKEN || '').trim();
  const shopId = arg('shop');
  let itemId = arg('item');

  console.log('── LINEギフト 商品API 実証 (読むだけ・書き込みなし) ──');
  console.log(`アクセストークン: ${tokenHint(token)}`);
  if (!token) { console.error('❌ LINEGIFT_ACCESS_TOKEN が未設定です (.env を確認)'); process.exitCode = 1; return; }
  if (!shopId) {
    console.error('❌ --shop <ショップID> が要ります');
    console.error('   管理画面で商品を開いた URL の /shops/<ここ>/items/... から読めます');
    process.exitCode = 1; return;
  }

  // item_id が指定されていなければ、受注データから実在するものを1つ借りる
  if (!itemId) {
    try {
      initDB();
      const row = getDB().prepare(`
        SELECT item_id, parent_item_code, sku_code, MAX(bought_on_unix) AS latest
          FROM raw_linegift_orders
         WHERE item_id IS NOT NULL
      `).get();
      if (row?.item_id) {
        itemId = String(row.item_id);
        console.log(`検体: raw_linegift_orders の直近受注から item_id=${itemId} (商品コード ${row.parent_item_code} / SKU ${row.sku_code})`);
      }
    } catch (e) {
      console.log(`(受注データから検体を選べませんでした: ${e.message})`);
    }
  }
  if (!itemId) { console.error('❌ --item <商品ID> を指定してください (受注データからも選べませんでした)'); process.exitCode = 1; return; }

  const itemPath = (id) => `/api/v1/shops/${encodeURIComponent(shopId)}/items/${encodeURIComponent(id)}`;

  // ── ① 対照: 受注 API。ここが通らなければトークンが期限切れなだけ ──
  console.log('\n① 対照 — 受注API (動くと分かっている経路) を同じトークンで');
  const control = await get(`${ORDER_HOST}/shop/api/1/order/search?access_token=${encodeURIComponent(token)}&page=1&per_page=1`);
  console.log(`   ${verdict(control)}`);
  const controlOk = control.http === 200 && control.body?.code !== 401;
  if (!controlOk) {
    console.log('\n🚨 対照が通っていません = **トークンが期限切れの可能性**。');
    console.log('   CMS が拒んだと判断してはいけません。先に `node apps/warehouse/linegift-orders.js` を');
    console.log('   1回流してトークンを正しく更新してから、この probe をやり直してください。');
    console.log('   ★この probe は refresh をしません (one-time の refresh_token を取りこぼさないため)');
    process.exitCode = 1; return;
  }

  // ── ②③④ 本命: CMS の商品 API ──
  const trials = [
    ['クエリ ?access_token= (受注APIと同じ形) / 実在する商品', `${CMS_HOST}${itemPath(itemId)}?access_token=${encodeURIComponent(token)}`, {}],
    ['Authorization: Bearer / 実在する商品', `${CMS_HOST}${itemPath(itemId)}`, { bearer: token }],
    ['クエリ ?access_token= / 存在しない商品 (401と404の見分け)', `${CMS_HOST}${itemPath(NONEXISTENT_ITEM_ID)}?access_token=${encodeURIComponent(token)}`, {}],
    ['Authorization: Bearer / 存在しない商品 (401と404の見分け)', `${CMS_HOST}${itemPath(NONEXISTENT_ITEM_ID)}`, { bearer: token }],
  ];

  console.log('\n② CMS 商品API — 渡し方を変えて試す');
  const results = [];
  for (const [label, url, opts] of trials) {
    const r = await get(url, opts);
    console.log(`   ${label}\n     ${verdict(r)}`);
    results.push({ label, r });
  }

  // ── 判定 ──
  console.log('\n── 判定 ──');
  const got200 = results.find(({ r }) => r.http === 200 && r.body?.code === 200 && r.body?.item);
  const got404 = results.find(({ r }) => (r.body?.code ?? r.http) === 404);

  if (got200) {
    console.log('✅ **読み取れました**。いまのトークンで LINEギフトの商品価格が取れます。');
    console.log(`   通った渡し方 = ${got200.label}`);
    console.log('\n   取れた中身 (価格まわりだけ):');
    console.log(JSON.stringify(summarizeItem(got200.r.body), null, 2));
    const vs = summarizeItem(got200.r.body)?.variations || [];
    if (vs.length > 0) {
      const anyPrice = vs.some((v) => v.hasPrice);
      console.log(anyPrice
        ? '\n   ⚠️variations に price がありました → 「SKUごとの価格」かもしれません。設計を見直すこと'
        : '\n   ⭐variations に price はありません → **1商品1価格**で確定 (Yahoo / au PAY / Qoo10 と同じ)');
    }
  } else if (got404) {
    console.log('🟡 **認証は通っているが、その商品が見つかりません** (404)。');
    console.log('   ショップIDか商品IDを見直してください。認証の道は開いています。');
  } else {
    console.log('❌ **このトークンでは CMS の商品APIを叩けません**。');
    console.log('   受注API (対照) は通っているので、トークンの期限ではなく **CMS 側が別の認証を求めている**');
    console.log('   = ブラウザのセッション専用の可能性が高い。');
    console.log('   → 価格の取り込みは API では無理。管理画面【商品 > 一括登録】の');
    console.log('      「更新用CSVダウンロード」を使う道を検討することになります。');
  }
}

main().catch((e) => { console.error('想定外のエラー:', e.message); process.exitCode = 1; });
