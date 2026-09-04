/**
 * linegift-price-service.js — LINEギフトの現在売価を読む口 (miniPC service-api)
 *
 * ★**読み取り専用**。書き込みの口は意図的に作っていない。
 *   LINEギフトの更新は `PATCH /api/v1/shops/{shop_id}/items/{item_id}` = **商品まるごと更新**
 *   しか無く、Yahoo `editItem` と同じ「送らなかった項目が消える」危険がある。
 *   さらに GET は画像を `id`+`url` で返すのに PATCH は `temporary_uuid` を要求するため、
 *   **読んだ画像をそのまま送り返せない** = 全上書きだった場合に復元できない。
 *   部分更新かどうかを実測するまで、書き込みは作らない。
 *
 * ## なぜ miniPC に置くか
 *   `LINEGIFT_ACCESS_TOKEN` は miniPC にしか無い。価格の live 取得を miniPC 経由にするのは
 *   楽天 (details-bulk) / Qoo10 (#1125) と同じ、このツールの既存パターン。
 *
 * ## 引き当ての考え方 (🚨ここを間違えると嘘のデータを作る)
 *   商品APIは **数値の item_id** でしか引けず、こちらが持っているのは NEコード
 *   (= LINEギフトの variation.code)。そこで:
 *     ①`raw_linegift_orders` の受注実績から item_id を **探す (手がかり)**
 *     ②見つけた item_id で **商品APIに聞く (正本)**
 *     ③返ってきた商品が本当にそのコードを含むか **照合してから** 価格を返す
 *   ★①は「存在するか」の判定には使わない。受注実績は「売れたものだけ」なので、
 *     これで存在を判定すると楽天で起きた誤判定 (407件中135件が実在した) を繰り返す。
 *     手がかりが無い時は **ITEM_ID_UNKNOWN (引き当てできない)** であって「未出品」ではない。
 *
 * ## 応答の形
 *   200 { ok:true, code, itemId, price, salePrice, status, itemName, variationCodes }
 *   404 { ok:false, error:'ITEM_ID_UNKNOWN' | 'ITEM_NOT_FOUND' | 'CODE_MISMATCH', message }
 *   502 { ok:false, error:'LINEGIFT_API_ERROR', message }
 *
 * env: LINEGIFT_ACCESS_TOKEN / LINEGIFT_SHOP_ID
 */
import express from 'express';
import { getDB } from './db.js';
import { rateLimitMiddleware } from './rate-limiter.js';

const router = express.Router();

const CMS_HOST = 'https://gift-shop-cms.line.biz';
const CALL_TIMEOUT_MS = 20_000;

/** SKU の同一性は大小文字を無視する ([[feedback_sku_case_normalization]]) */
function normCode(v) {
  return String(v == null ? '' : v).trim().toLowerCase();
}

function accessToken() {
  const v = String(process.env.LINEGIFT_ACCESS_TOKEN || '').trim();
  if (!v) { const e = new Error('LINEGIFT_ACCESS_TOKEN が未設定です'); e.statusCode = 503; throw e; }
  return v;
}

function shopId() {
  const v = String(process.env.LINEGIFT_SHOP_ID || '').trim();
  if (!/^\d+$/.test(v)) {
    const e = new Error('LINEGIFT_SHOP_ID (数字) が未設定です。管理画面URL /shops/<ここ>/ から取れます');
    e.statusCode = 503; throw e;
  }
  return v;
}

/**
 * 商品を1件読む。
 * 🚨LINEギフトは **HTTP 200 で本文に {"code":401} / {"code":404}** を返すことがある。
 *   HTTP だけ見て成功と判断しない。**本文の code を正**とする (実測・Swagger の記載どおり)
 */
async function readItem(itemId) {
  const url = `${CMS_HOST}/api/v1/shops/${shopId()}/items/${encodeURIComponent(itemId)}`
    + `?access_token=${encodeURIComponent(accessToken())}`;
  let res;
  try {
    res = await fetch(url, { headers: { Accept: 'application/json' }, signal: AbortSignal.timeout(CALL_TIMEOUT_MS) });
  } catch (e) {
    const err = new Error(`LINEギフトに接続できません (${e.message})`); err.statusCode = 502; throw err;
  }
  const text = await res.text();
  let body = null;
  try { body = JSON.parse(text); } catch { /* JSON でなければ null のまま */ }
  const code = body && typeof body.code === 'number' ? body.code : res.status;

  if (code === 404) return { notFound: true };
  if (code !== 200 || !body?.item) {
    // 401/403 (認証・権限) も 5xx も、呼び出し側から見れば「取れなかった」で同じ扱いにする。
    // ★ここで再試行はしない — 認証切れなら何度やっても同じで、価格は「取れない」が正しい記録
    const err = new Error(`LINEギフトが受け付けませんでした (code ${code}${body?.reason ? ` ${body.reason}` : ''})`);
    err.statusCode = 502;
    throw err;
  }
  return { item: body.item };
}

/**
 * 受注実績から item_id の**手がかり**を引く。
 * ★見つからない = 「出品していない」ではない (売れたことが無いだけかもしれない)。
 *   複数の item_id に紐づく場合は**決めない** (取り違え防止。楽天で同じ形の事故を踏んでいる)
 */
export function lookupItemIdHint(db, code) {
  const key = normCode(code);
  if (!key) return { itemId: null, reason: '出品コードが空です' };
  const rows = db.prepare(`
    SELECT DISTINCT item_id
      FROM raw_linegift_orders
     WHERE item_id IS NOT NULL
       AND (LOWER(TRIM(sku_code)) = ? OR LOWER(TRIM(parent_item_code)) = ?)
  `).all(key, key);
  if (rows.length === 0) {
    return { itemId: null, reason: 'LINEギフトでの受注実績が無いため商品IDが分かりません (出品の有無は判定していません)' };
  }
  if (rows.length > 1) {
    return { itemId: null, reason: `複数の商品IDが該当するため特定できません (${rows.map((r) => r.item_id).join(', ')})` };
  }
  return { itemId: rows[0].item_id, reason: null };
}

/**
 * 返ってきた商品が、本当に要求したコードのものか確かめる。
 * ★手がかりの対応表が古いと、別商品の価格を掴む。Qoo10 で itemNo を照合しているのと同じ守り
 */
export function itemMatchesCode(item, code) {
  const key = normCode(code);
  if (!key) return false;
  if (normCode(item?.code) === key) return true;
  return (item?.variations || []).some((v) => normCode(v?.code) === key);
}

/** 価格まわりだけを取り出す。★sale_price / sale_id は未設定だと項目ごと来ない (実測) */
export function toPriceView(item) {
  const price = Number.isInteger(item?.price) ? item.price : null;
  const salePrice = Number.isInteger(item?.sale_price) ? item.sale_price : null;
  return {
    itemId: item?.id ?? null,
    code: item?.code ?? null,
    itemName: item?.name ?? null,
    status: item?.status ?? null,
    // 商品ページ (要件F5 の目視確認用)。item_id からは組み立てられないので応答の値をそのまま渡す
    webUrl: item?.web_url ?? null,
    price,
    // ★「セール価格が入っている」と「未設定」を分ける (Yahoo で読めないと全部止まった教訓)
    salePrice,
    hasSale: salePrice != null || item?.sale_id != null,
    variationCodes: (item?.variations || []).map((v) => v?.code).filter(Boolean),
  };
}

// ==========================================
//   GET /items/by-code/:code/price — 出品コード (NEコード) で現在売価を読む
//   ★読むだけ。書き込みの口はこのファイルに存在しない
// ==========================================
router.get('/items/by-code/:code/price', rateLimitMiddleware('linegift'), async (req, res) => {
  const code = String(req.params.code || '').trim();
  if (!code) {
    return res.status(400).json({ ok: false, error: 'INVALID_CODE', message: '出品コードが必要です' });
  }
  try {
    const hint = lookupItemIdHint(getDB(), code);
    if (!hint.itemId) {
      // ★404 だが「無い」とは言い切らない。引き当てできなかっただけ
      return res.status(404).json({ ok: false, error: 'ITEM_ID_UNKNOWN', message: hint.reason });
    }
    const got = await readItem(hint.itemId);
    if (got.notFound) {
      return res.status(404).json({
        ok: false, error: 'ITEM_NOT_FOUND',
        message: `商品ID ${hint.itemId} が LINEギフトに見つかりません (削除された可能性があります)`,
      });
    }
    if (!itemMatchesCode(got.item, code)) {
      return res.status(404).json({
        ok: false, error: 'CODE_MISMATCH',
        message: `商品ID ${hint.itemId} は別の商品です (要求 ${code} / 応答 ${got.item?.code ?? 'なし'})`,
      });
    }
    res.json({ ok: true, ...toPriceView(got.item) });
  } catch (e) {
    console.error(`[linegift-price] ERROR code=${code}: ${e.message}`);
    res.status(e.statusCode || 502).json({ ok: false, error: 'LINEGIFT_API_ERROR', message: e.message });
  }
});

export default router;
