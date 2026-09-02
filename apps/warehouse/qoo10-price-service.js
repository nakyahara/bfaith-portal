/**
 * qoo10-price-service.js — Qoo10 の価格を読む・変える (service-api)
 * /service-api/qoo10/* にマウント (価格一括改定ツール M5)
 *
 * 🚨ここが唯一「Qoo10 の売価を書き換える」経路。守っていること:
 *   ・QOO10_WRITE_ENABLED が無ければ 503 (fail-closed)
 *   ・使うのは ItemsOrder.SetGoodsPriceQty (価格専用 API)。
 *     ItemsBasic.UpdateGoods は使わない — 実測で ShippingNo (送料テンプレート) が
 *     黙って書き換わり、発送予定日も毎回上書きされると分かっている
 *     (2026-09-02・AI_reference『価格一括改定ツール_M5実測結果_Qoo10_20260902.md』)
 *   ・商品ロックの中で 読む → expected と整数円で完全一致 → 送る → 読み直して照合
 *   ・🚨SetGoodsPriceQty の省略の罠 (公式に明記):
 *       Qty を省略      → 在庫が 9999 になる
 *       ExpireDate を省略 → 販売終了日が「1年後」に書き換わる
 *     → どちらも読める項目なので、**毎回現在値を明示して送る**
 *   ・自動リトライしない。送信の途中で落ちたら 502 (呼び出し側が「結果不明」に倒す)
 *
 * 鍵: 商品系 API は QOO10_CERT_KEY。受注用の QOO10_API_KEY では商品が1件も見えない (実測)。
 */
import express from 'express';
import { rateLimitMiddleware } from './rate-limiter.js';

const router = express.Router();
const QAPI_BASE = 'https://api.qoo10.jp/GMKT.INC.Front.QAPIService/ebayjapan.qapi';
/** Qoo10 商品番号 (公式: 9〜10桁の半角数字)。パスに使うので厳格に */
const ITEM_NO_RE = /^\d{9,10}$/;
/** 送ってよい価格の上限 (公式: 1〜999999999) */
const MAX_QOO10_PRICE = 999_999_999;
const CALL_TIMEOUT_MS = 30_000;

function certKey() {
  const v = String(process.env.QOO10_CERT_KEY || '').trim();
  if (!v) {
    const err = new Error('QOO10_CERT_KEY が未設定です (商品系 API はこの鍵。受注用の QOO10_API_KEY では商品が見えません)');
    err.statusCode = 503;
    throw err;
  }
  return v;
}

function writeEnabled() {
  return ['1', 'true', 'on', 'yes'].includes(String(process.env.QOO10_WRITE_ENABLED ?? '').trim().toLowerCase());
}

function requireWrite(req, res, next) {
  if (!writeEnabled()) {
    return res.status(503).json({
      ok: false, error: 'QOO10_WRITE_DISABLED',
      message: 'QOO10_WRITE_ENABLED が未設定のため書込は無効です (fail-closed)',
    });
  }
  next();
}

/** QAPI を1回叩く。応答は JSON ({ResultCode, ResultMsg, ResultObject}) */
async function qapi(method, params = {}) {
  const key = certKey();
  const qs = new URLSearchParams({ key, ...params });
  const r = await fetch(`${QAPI_BASE}/${method}?${qs}`, {
    headers: { GiosisCertificationKey: key },
    signal: AbortSignal.timeout(CALL_TIMEOUT_MS),
  });
  const text = await r.text();
  let json = null;
  try { json = JSON.parse(text); } catch { /* JSON でない応答は下で失敗として扱う */ }
  if (!json || typeof json.ResultCode !== 'number') {
    const err = new Error(`Qoo10 の応答を読めません (HTTP ${r.status}: ${text.slice(0, 120)})`);
    err.statusCode = 502;
    throw err;
  }
  return json;
}

/**
 * SellPrice ("2574.0000" のような文字列) を整数円にする。
 * ★小数部が 0 でない値は「読めない」扱い (勝手に丸めると照合の基準がずれる)
 */
export function qooPriceToInt(v) {
  const s = String(v ?? '').trim();
  const m = s.match(/^(\d+)(?:\.(\d+))?$/);
  if (!m) return null;
  if (m[2] && /[1-9]/.test(m[2])) return null;
  const n = Number(m[1]);
  return Number.isSafeInteger(n) ? n : null;
}

/** GetItemDetailInfo → ツールが使う形に。読めない値は null (fail-closed 用) */
export function shapeDetail(o) {
  if (!o || typeof o !== 'object') return null;
  return {
    itemNo: o.ItemNo == null ? null : String(o.ItemNo),
    sellerCode: o.SellerCode == null ? null : String(o.SellerCode),
    itemTitle: o.ItemTitle == null ? null : String(o.ItemTitle),
    itemStatus: o.ItemStatus == null ? null : String(o.ItemStatus),   // S2 = 販売中
    sellPrice: qooPriceToInt(o.SellPrice),
    // ★Qty と ExpireDate は書き込み時に「現在値を明示して送り返す」ために必須。
    //   読めなければ書き込みを止める (省略すると 9999 / 1年後に書き換わる)
    itemQty: qooPriceToInt(o.ItemQty),
    expireDate: (typeof o.ExpireDate === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(o.ExpireDate)) ? o.ExpireDate : null,
  };
}

async function readDetail(itemNo) {
  const j = await qapi('ItemsLookup.GetItemDetailInfo', { ItemCode: itemNo, SellerCode: '' });
  if (j.ResultCode !== 0) {
    return { error: `Qoo10 から商品を取得できません (${j.ResultCode}: ${String(j.ResultMsg).slice(0, 80)})` };
  }
  const o = Array.isArray(j.ResultObject) ? j.ResultObject[0] : j.ResultObject;
  const d = shapeDetail(o);
  if (!d) return { error: 'Qoo10 の応答に商品が入っていません' };
  // ★取り違え防止: 返ってきた商品番号が要求と一致すること
  if (String(d.itemNo) !== String(itemNo)) {
    return { error: `別の商品が返りました (要求 ${itemNo} / 応答 ${d.itemNo ?? 'なし'})` };
  }
  return { detail: d };
}

// ── 商品ごとのロック (読む→照合→送る を割り込ませない) ──
const itemLocks = new Map();
async function withItemLock(itemNo, fn) {
  const k = String(itemNo).trim();
  const prev = itemLocks.get(k);
  let release;
  const mine = new Promise((r) => { release = r; });
  itemLocks.set(k, mine);
  try {
    if (prev) await prev;
    return await fn();
  } finally {
    release();
    if (itemLocks.get(k) === mine) itemLocks.delete(k);
  }
}

/**
 * 送る前の判定 (純関数・テスト用に切り出し)。
 * @returns {{done:{status:number, body:object}}|{proceed:{next:number, qty:number, expireDate:string, sellerCode:string}}}
 */
export function planQoo10Patch(detail, itemNo, want, next) {
  const done=(status,body)=>({done:{status,body}});
  const d=detail;
  // ★実行時にも販売状態を見る。記録した後に停止 (S1/S3) へ変わった商品に値付けしても
  //   客に見えず「変えたつもり」だけが残る (Codex M5 R1)
  if (d.itemStatus !== 'S2') {
    return done(400,{ok:false,state:'failed',error:'NOT_ON_SALE',
      message:`販売中の商品ではありません (状態 ${d.itemStatus ?? '不明'})。記録した後に状態が変わっています`});
  }
  if (d.sellPrice === null) {
    return done(400,{ok:false,state:'failed',error:'CURRENT_PRICE_UNREADABLE',message:'いまの価格を整数円として読めません'});
  }
  // ★Qty / ExpireDate が読めない商品には送らない。省略で送ると
  //   在庫 9999 / 販売終了日1年後 に書き換わる (公式仕様)
  if (d.itemQty === null || d.expireDate === null) {
    return done(400,{ok:false,state:'failed',error:'PRESERVE_FIELDS_UNREADABLE',
      message:`数量か販売終了日が読めません (Qty=${d.itemQty ?? '不明'} / ExpireDate=${d.expireDate ?? '不明'})。現在値を送り返せないため、書き換わる恐れがあり送信しません`});
  }
  if (d.sellPrice !== want) {
    return done(409,{ok:false,state:'conflict',error:'CONFLICT',message:'現在価格が想定と違います',
      detail:{conflicts:[{sku:itemNo,expected:want,live:d.sellPrice,reason:'現在価格が想定と違います'}]}});
  }
  if (d.sellPrice === next) {
    return done(200,{ok:true,state:'noop',reason:'既に同じ価格です'});
  }
  return {proceed:{next, qty:d.itemQty, expireDate:d.expireDate, sellerCode:d.sellerCode ?? ''}};
}

// ==========================================
//   GET /items/:itemNo — 読み取り (引き当て・照合用)
// ==========================================
router.get('/items/:itemNo', rateLimitMiddleware('qoo10'), async (req, res) => {
  const itemNo = String(req.params.itemNo || '').trim();
  if (!ITEM_NO_RE.test(itemNo)) {
    return res.status(400).json({ ok: false, error: 'INVALID_ITEM_NO', message: 'Qoo10 商品番号 (9〜10桁の数字) が必要です' });
  }
  try {
    const got = await readDetail(itemNo);
    if (got.error) return res.status(404).json({ ok: false, error: 'ITEM_NOT_FOUND', message: got.error });
    res.json({ ok: true, item: got.detail });
  } catch (e) {
    res.status(e.statusCode || 502).json({ ok: false, error: 'QAPI_ERROR', message: e.message });
  }
});

// ==========================================
//   PATCH /items/:itemNo/price — 価格を変える
//   body: { operation_id, run_id?, expected: {itemNo:円}, prices: {itemNo:円} }
//   応答は楽天の書き込みエンドポイントと同じ形 (state: applied/noop/conflict/failed)。
//   → Render 側の classify()・試運転・ブレーカーがそのまま効く
// ==========================================
router.patch('/items/:itemNo/price', requireWrite, rateLimitMiddleware('qoo10'), async (req, res) => {
  const itemNo = String(req.params.itemNo || '').trim();
  const operationId = String(req.body?.operation_id || '').trim();
  const expected = req.body?.expected;
  const prices = req.body?.prices;

  if (!ITEM_NO_RE.test(itemNo)) {
    return res.status(400).json({ ok: false, state: 'failed', error: 'INVALID_ITEM_NO', message: 'Qoo10 商品番号 (9〜10桁の数字) が必要です' });
  }
  if (!/^[A-Za-z0-9._-]{8,80}$/.test(operationId)) {
    return res.status(400).json({ ok: false, state: 'failed', error: 'INVALID_OPERATION_ID', message: 'operation_id (8〜80文字の英数.-_) が必要です' });
  }
  if (!prices || typeof prices !== 'object' || Array.isArray(prices)
      || !expected || typeof expected !== 'object' || Array.isArray(expected)) {
    return res.status(400).json({ ok: false, state: 'failed', error: 'INVALID_PAYLOAD', message: 'expected と prices (どちらもオブジェクト) が必要です' });
  }
  // ★Qoo10 は「商品」に1つの価格。キーは商品番号そのもの1つだけ
  const keys = Object.keys(prices);
  if (keys.length !== 1 || String(keys[0]).trim() !== itemNo) {
    return res.status(400).json({
      ok: false, state: 'failed', error: 'SKU_KEY_MISMATCH',
      message: `Qoo10 は商品に1つの価格しか持ちません。prices のキーは商品番号 (${itemNo}) を1つだけにしてください`,
    });
  }
  const next = Number(prices[itemNo]);
  const want = Number(expected[itemNo]);
  if (!Number.isInteger(next) || next < 1 || next > MAX_QOO10_PRICE) {
    return res.status(400).json({ ok: false, state: 'failed', error: 'INVALID_PRICE', message: `価格は 1〜${MAX_QOO10_PRICE} の整数円です (${prices[itemNo]})` });
  }
  if (!Number.isInteger(want)) {
    return res.status(400).json({ ok: false, state: 'failed', error: 'EXPECTED_REQUIRED', message: 'expected (今いくらのはず・整数円) が必要です。照合せずに送ることはできません' });
  }

  try {
    const out = await withItemLock(itemNo, async () => {
      const got = await readDetail(itemNo);
      if (got.error) return { status: 404, body: { ok: false, state: 'failed', error: 'ITEM_NOT_FOUND', message: got.error } };
      const d = got.detail;
      // 送る前の判定 (楽観ロック・省略の罠よけ) は planQoo10Patch に切り出してある
      const plan = planQoo10Patch(d, itemNo, want, next);
      if (plan.done) return plan.done;

      console.log(`[qoo10-price] ${itemNo} (${d.sellerCode}) ${d.sellPrice}円 -> ${next}円 op=${operationId}`);
      const r = await qapi('ItemsOrder.SetGoodsPriceQty', {
        ItemCode: itemNo,
        SellerCode: plan.proceed.sellerCode,
        Price: String(plan.proceed.next),
        // ★現在値を明示して送り返す (省略の罠を踏まない)
        Qty: String(plan.proceed.qty),
        ExpireDate: plan.proceed.expireDate,
      });
      if (r.ResultCode !== 0) {
        console.error(`[qoo10-price] FAILED ${itemNo} op=${operationId}: ${r.ResultCode} ${r.ResultMsg}`);
        return {
          status: 502,
          body: { ok: false, state: 'failed', error: 'QOO10_REJECTED', message: `Qoo10 が受け付けませんでした (${r.ResultCode}: ${String(r.ResultMsg).slice(0, 120)})` },
        };
      }

      // ★送った後に読み直して、本当にその価格・数量・終了日になっているか確かめる
      const after = await readDetail(itemNo);
      if (after.error || after.detail.sellPrice !== next) {
        return {
          status: 502,
          body: {
            ok: false, state: 'failed', error: 'VERIFY_FAILED', mayHaveChanged: true,
            message: `送信は受け付けられましたが、読み直した価格が合いません (期待 ${next} / 実際 ${after.detail?.sellPrice ?? after.error})`,
          },
        };
      }
      if (after.detail.itemQty !== d.itemQty || after.detail.expireDate !== d.expireDate) {
        // 価格は変わったが、守るはずの数量・終了日が変わってしまった。成功にしない (人が見る)
        return {
          status: 502,
          body: {
            ok: false, state: 'failed', error: 'SIDE_EFFECT_DETECTED', mayHaveChanged: true, applied: { [itemNo]: next },
            message: `価格は ${next} 円になりましたが、数量か販売終了日が変わっています `
              + `(Qty ${d.itemQty}→${after.detail.itemQty} / ExpireDate ${d.expireDate}→${after.detail.expireDate})。`
              + '★このツールの復旧は価格しか戻せません。数量と販売終了日は QSM で人が直してください',
          },
        };
      }
      console.log(`[qoo10-price] applied ${itemNo} op=${operationId} ${d.sellPrice}->${next}`);
      return { status: 200, body: { ok: true, state: 'applied', applied: { [itemNo]: next } } };
    });
    res.status(out.status).json(out.body);
  } catch (e) {
    // 送信の途中で落ちた可能性がある → 502 (呼び出し側が「結果不明」に倒し、再送しない)
    console.error(`[qoo10-price] ERROR ${itemNo} op=${operationId}: ${e.message}`);
    res.status(502).json({ ok: false, error: 'QAPI_ERROR', message: e.message });
  }
});

export default router;
