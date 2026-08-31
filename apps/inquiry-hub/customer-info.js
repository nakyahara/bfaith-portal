/**
 * inquiry-hub 顧客情報の手入力 (2026-08-31 中原さん要望)
 *
 * 位置づけ:
 *   詳細画面 右上「顧客情報」パネルの 注文番号・商品コード・商品名 を画面から入力して保存する。
 *   メール問い合わせは注文番号が付いて来ないので、スタッフが NE 等で調べた結果を
 *   その問い合わせに残す (次に開いた人が調べ直さなくて済む)。保存ボタンは無く、
 *   入力欄から出た時点で自動保存する (router.js の /customer-info API)。
 *
 * 確定情報ロック (最重要):
 *   楽天/Yahoo! の購入者問い合わせのように、モール同期 (sync/engine.js) が注文番号等を
 *   返して来たものは「確定情報」なので画面から変更できない。
 *   判定は inquiries.manual_fields (JSON配列。手入力した項目名) で行う:
 *     - 値があり、かつ manual_fields に無い項目 = 同期が入れた確定情報 → ロック
 *     - 値が無い項目、または manual_fields にある項目 = 手入力可 (空にも戻せる)
 *   既存データは manual_fields=NULL なので、値が入っている項目はすべてロック扱い
 *   (この機能より前に手入力の経路は無く、値は必ず同期由来のため)。
 *   同期がその項目に値を返した時点で manual_fields から外れ、以後ロックされる
 *   (engine.js の COALESCE 上書きと同じタイミング。manualFieldsAfterSync)。
 */
import { getDB, logActivity } from './db.js';

/** 手入力できる項目 (キー = inquiries のカラム名) */
export const CUSTOMER_INFO_FIELDS = Object.freeze({
  order_number: { label: '注文番号', max: 100 },
  product_code: { label: '商品コード', max: 100 },
  product_name: { label: '商品名', max: 300 },
});
export const CUSTOMER_INFO_KEYS = Object.freeze(Object.keys(CUSTOMER_INFO_FIELDS));

/** アダプター契約 (engine.js の item) 側のキー名との対応 */
const ITEM_KEY_OF = Object.freeze({ order_number: 'orderNumber', product_code: 'productCode', product_name: 'productName' });

/** inquiries.manual_fields (JSON配列 or NULL) → 既知の項目名だけの配列。壊れた値は「手入力なし」扱い */
export function parseManualFields(raw) {
  if (!raw) return [];
  try {
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr.filter(k => CUSTOMER_INFO_KEYS.includes(k)) : [];
  } catch { return []; }
}

/** 配列 → 保存形式 (空なら NULL) */
export function serializeManualFields(keys) {
  const uniq = CUSTOMER_INFO_KEYS.filter(k => keys.includes(k));
  return uniq.length ? JSON.stringify(uniq) : null;
}

/**
 * 同期がアダプター値で上書きする項目を手入力集合から外す (engine.js の既存チケット更新時に呼ぶ)。
 * item が null を返した項目は手入力値が残る (COALESCE で消えないのと対応)
 */
export function manualFieldsAfterSync(raw, item) {
  const keep = parseManualFields(raw).filter(k => item[ITEM_KEY_OF[k]] == null);
  return serializeManualFields(keep);
}

/** 画面/API用: 項目ごとの { value, manual, locked } */
export function customerInfoState(inq) {
  const manual = new Set(parseManualFields(inq.manual_fields));
  const out = {};
  for (const k of CUSTOMER_INFO_KEYS) {
    const value = inq[k] == null || String(inq[k]) === '' ? null : String(inq[k]);
    out[k] = { value, manual: manual.has(k), locked: value != null && !manual.has(k) };
  }
  return out;
}

/** 入力値の正規化。空は NULL。改行・制御文字は不可 (1行の識別子なので)。長すぎは throw */
export function normalizeCustomerInfoValue(key, raw) {
  const meta = CUSTOMER_INFO_FIELDS[key];
  if (!meta) throw new Error('不正な項目です');
  if (raw == null) return null;
  if (typeof raw !== 'string' && typeof raw !== 'number') throw new Error(`${meta.label}は文字列で指定してください`);
  const v = String(raw).replace(/\s+/g, ' ').trim();
  if (!v) return null;
  // eslint-disable-next-line no-control-regex
  if (/[\u0000-\u001f\u007f]/.test(v)) throw new Error(`${meta.label}に使えない文字が含まれています`);
  if (v.length > meta.max) throw new Error(`${meta.label}は${meta.max}文字以内で入力してください`);
  return v;
}

/**
 * 顧客情報を保存する (項目単位。patch に含まれる既知の項目だけ更新)。
 *   - ロック項目に別の値を入れようとしたら throw (e.code = 'LOCKED')。同じ値なら無視
 *   - 値を入れた項目は manual_fields に加え、空にした項目は外す
 *   - 変更があれば操作ログ customer_info_edit (before/after は変更した項目のみ)
 * 戻り値: { ok, unchanged?, changed: [項目名], state }
 */
export function setCustomerInfo(inquiryId, patch, actorId = null) {
  const db = getDB();
  return db.transaction(() => {
    const inq = db.prepare('SELECT id, order_number, product_code, product_name, manual_fields FROM inquiries WHERE id = ?').get(inquiryId);
    if (!inq) throw new Error('問い合わせが見つかりません');
    const state = customerInfoState(inq);
    const before = {}, after = {};
    const changed = [];
    for (const k of CUSTOMER_INFO_KEYS) {
      if (!Object.prototype.hasOwnProperty.call(patch || {}, k)) continue;
      const v = normalizeCustomerInfoValue(k, patch[k]);
      if (v === state[k].value) continue;
      if (state[k].locked) {
        const e = new Error(`${CUSTOMER_INFO_FIELDS[k].label}はモールから取得した確定情報のため変更できません`);
        e.code = 'LOCKED';
        throw e;
      }
      before[k] = state[k].value;
      after[k] = v;
      changed.push(k);
    }
    if (!changed.length) return { ok: true, unchanged: true, changed: [], state };

    const manual = new Set(parseManualFields(inq.manual_fields));
    for (const k of changed) { if (after[k] == null) manual.delete(k); else manual.add(k); }
    const sets = changed.map(k => `${k} = ?`).concat(['manual_fields = ?', "updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now')"]);
    db.prepare(`UPDATE inquiries SET ${sets.join(', ')} WHERE id = ?`)
      .run(...changed.map(k => after[k]), serializeManualFields([...manual]), inquiryId);
    logActivity(inquiryId, { actorType: 'user', userId: actorId, actionType: 'customer_info_edit', before, after });
    const now = db.prepare('SELECT order_number, product_code, product_name, manual_fields FROM inquiries WHERE id = ?').get(inquiryId);
    return { ok: true, changed, state: customerInfoState(now) };
  }).immediate();
}
