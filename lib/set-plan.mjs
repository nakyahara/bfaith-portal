/**
 * set-plan.mjs — 新商品の登録 (単品) の「セット商品を作るか = 作る」で決める、作るセット (個数・セット商品コード・セット商品名) の決まり
 * (中原さん 2026-10-17「何個セットを作るかも聞いて、セット商品コードと名前をその場で作る。ここで作ったセットは下書き。その後 NE に登録がなければ知らせる」)
 *
 * 画面 (apps/master-edit/public/me-new.js)・登録の API (lib/master-register.mjs の parseCard)・product-hub の取り込み (apps/product-hub/services/cdb-set-plan.js) が
 * 同じ決まりを使う = ここが正本 (どこにも依存しない小さな部品。product-hub の側からも読める)。
 *
 * 決めたこと (初めの値はどれも画面で直せる):
 *   - コード = <単品のコード>-<個数> (NE の今の書き方: u60e-05-2・nametag100-2)。形は新商品のコードと同じ (英字 (大文字も)・数字・- _ の 30 字まで・set- で始めない)
 *   - 名前 = <単品の名前> <個数>個セット (半角の空白を 1 つ・2026-10-17 中原さん: 「ボトルキープ ネームタグ 【100枚】 2個セット」)
 *   - 個数 = 2〜99 の整数・同じ個数は 1 行・1 つの単品に 10 セットまで・コードは単品のコードと同じにしない・行どうしで重ねない (大文字小文字だけ違うのも同じと見る)
 * 🚨 Company DB にはセットの SKU を作らない (セットの登録は開いていない = ops.new_entry_lease_valid('set') が false・sku_components の持ち主は load・設計 24 待ち)。
 *    下書きは product-hub だけに持つ (product_drafts の provisional_code = 1 = NE で本コードを確かめるまで、どのモールにも出さない)
 */
import crypto from 'node:crypto';

/** 作るセットの形の版 (product-hub の表 ph_cdb_set_plans に残す。形を変えたら上げる) */
export const SET_PLAN_SCHEMA = 'ph-set-plan-v1';
/** 1 つの単品に作れるセットの数 (product-hub の派生の上限 MAX_SET_PER_PARENT 20 より少なく = あとで手で足す余地を残す) */
export const MAX_SET_PLANS = 10;
export const SET_QTY_MIN = 2;
export const SET_QTY_MAX = 99;
export const SET_NAME_MAX = 255;
/** NE 登録の前の下書きのまま、この日数を過ぎたら product-hub のボードで知らせる */
export const SET_PLAN_ALERT_DAYS = 3;
const CODE_RE = /^[A-Za-z0-9_-]{1,30}$/;
const CTRL_RE = new RegExp(`[\\x00-\\x1f\\x7f${String.fromCharCode(0x2028, 0x2029)}]`);

/** 初めのコード (<単品のコード>-<個数>) */
export const defaultSetCode = (parentCode, qty) => `${String(parentCode ?? '').trim()}-${qty}`;
/** 初めの名前 (<単品の名前> <個数>個セット) */
export const defaultSetName = (parentName, qty) => `${String(parentName ?? '').trim()} ${qty}個セット`.trim();

/** コードの形の一言 (問題が無ければ '') = 新商品のコード (lib/master-write.mjs の validateNewSkuCode) と同じ決まり */
export function setCodeShapeProblem(code) {
  const s = typeof code === 'string' ? code : '';
  if (!s.trim()) return 'セット商品コードを入れてください';
  if (s !== s.trim()) return 'セット商品コードの前後に空白があります';
  if (!CODE_RE.test(s)) return 'セット商品コードに使える文字は英字 (大文字・小文字)・数字・- と _ だけ (30 字まで)';
  if (/^set-/i.test(s)) return 'set- で始まるコードは使えません (product-hub の仮のコード)';
  return '';
}

/** 個数 (半角・全角の数字の文字か整数) → 2〜99 の整数 / null */
export function setQtyOf(v) {
  if (typeof v === 'number') return Number.isInteger(v) && v >= SET_QTY_MIN && v <= SET_QTY_MAX ? v : null;
  if (typeof v !== 'string') return null;
  const s = v.replace(/[０-９]/g, (c) => String.fromCharCode(c.charCodeAt(0) - 0xFEE0)).trim();
  if (!/^\d{1,2}$/.test(s)) return null;
  const n = Number(s);
  return n >= SET_QTY_MIN && n <= SET_QTY_MAX ? n : null;
}

class SetPlanShapeError extends Error {
  constructor(message, index = null) { super(message); this.index = index; }
}
export { SetPlanShapeError };

/**
 * 送られた作るセット → 決まった形 [{ qty, code, name }] (入れた順)。形が違えば SetPlanShapeError (message = 画面の一言・index = 何行目 (0 から) か null)
 *   parentCode = 単品のコード (同じコードにしない)。DB・product-hub を読まない (重なりは呼び手が確かめる)
 */
export function parseSetPlans(raw, { parentCode = '' } = {}) {
  if (!Array.isArray(raw)) throw new SetPlanShapeError('作るセットの入れ方が違う (配列で送ってください)');
  if (raw.length > MAX_SET_PLANS) throw new SetPlanShapeError(`作るセットは 1 つの単品に ${MAX_SET_PLANS} 個までです`);
  const parentKey = String(parentCode ?? '').trim().toLowerCase();
  const out = []; const qtys = new Set(); const codes = new Set();
  raw.forEach((r, i) => {
    const no = `${i + 1} 行目`;
    if (!r || typeof r !== 'object' || Array.isArray(r)) throw new SetPlanShapeError(`作るセットの ${no} の入れ方が違う`, i);
    const extra = Object.keys(r).filter((k) => !['qty', 'code', 'name'].includes(k));
    if (extra.length) throw new SetPlanShapeError(`作るセットの ${no} に知らない欄があります (${extra.join('・')})`, i);
    const qty = setQtyOf(r.qty);
    if (qty == null) throw new SetPlanShapeError(`作るセットの ${no}: 個数は ${SET_QTY_MIN}〜${SET_QTY_MAX} の整数です`, i);
    if (qtys.has(qty)) throw new SetPlanShapeError(`作るセットの ${no}: ${qty}個セットが 2 行あります (同じ個数は 1 行)`, i);
    qtys.add(qty);
    const shape = setCodeShapeProblem(r.code);
    if (shape) throw new SetPlanShapeError(`作るセットの ${no}: ${shape}`, i);
    const key = r.code.toLowerCase();
    if (parentKey && key === parentKey) throw new SetPlanShapeError(`作るセットの ${no}: セット商品コードが単品のコードと同じです`, i);
    if (codes.has(key)) throw new SetPlanShapeError(`作るセットの ${no}: セット商品コード ${r.code} が 2 行あります (大文字と小文字だけ違うコードも同じと見ます)`, i);
    codes.add(key);
    if (typeof r.name !== 'string') throw new SetPlanShapeError(`作るセットの ${no}: セット商品名を文字で入れてください`, i);
    const name = r.name.trim();
    if (!name) throw new SetPlanShapeError(`作るセットの ${no}: セット商品名を入れてください`, i);
    if ([...name].length > SET_NAME_MAX) throw new SetPlanShapeError(`作るセットの ${no}: セット商品名は ${SET_NAME_MAX} 字までです`, i);
    if (CTRL_RE.test(name)) throw new SetPlanShapeError(`作るセットの ${no}: セット商品名に改行や制御文字は入れられません`, i);
    out.push({ qty, code: r.code, name });
  });
  return out;
}

/** 作るセットの中身の印 (同じ request_id の押し直しが同じ中身かを見る)。並び・書き方 (大文字) も含める */
export function setPlanHash(plans) {
  const s = JSON.stringify((plans || []).map((p) => [p.qty, p.code, p.name]));
  return crypto.createHash('sha256').update(s).digest('hex');
}
