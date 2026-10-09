/**
 * order-settings.mjs — マスタの入力の画面に出す「発注の設定」(発注ロット・発注条件グループ・原料グループ・ケース・容量) (10/9 中原さん)
 *
 * 置き場と正本は発注アプリ (Render の warehouse-mirror.db の po_*)。書くのは発注アプリの 1 つの部品 (apps/purchase-orders/order-settings.js) だけ
 * = 発注アプリのマスタ管理と同じ確かめ・同じ記録 (po_audit_log・via = master-edit:new / master-edit:sku)。
 * 書ける人 = マスタの入力の名簿 (env MASTER_EDITORS) **かつ** 発注アプリの利用権 (server.js の requireAppAccess と同じ判定)。
 *   どちらか片方だけでは書けない (安全な方 = 両方): 名簿の人でも発注アプリの権限が無ければ発注アプリの値を見せない (注文残と同じ #1620 R1 M3)・
 *   発注アプリの権限だけの人はマスタの入力では書けない (発注アプリのマスタ管理で書く)。
 * 単品だけ (発注アプリはセットを扱わない = 構成品で発注する。logic.js computeAll がセットを外す)。
 * Company DB には書かない (この画面の Company DB の切替の門とは別。発注アプリのマスタ管理と同じく、いつでも書ける)。
 */
import { sessionHasApp } from '../../lib/app-access.js';
import { readOrderSettingsForScreen, ATTR_FIELDS, OrderSettingsError } from '../purchase-orders/order-settings.js';

export const PO_APP_ID = 'purchase-orders';
const BODY_KEYS = new Set([...ATTR_FIELDS, 'new_condition', 'new_material']);
const blank = (v) => v == null || (typeof v === 'string' && v.trim() === '');

/** 書けるか。editor = editorGate(req) の答え */
export function orderWriteGate(req, editor) {
  if (!sessionHasApp(req.session, PO_APP_ID)) return { ok: false, reason: 'no_po_access', message: '発注アプリの権限がないので、発注の設定は出せません (保存もできません)' };
  if (!editor || !editor.ok) return { ok: false, reason: 'not_editor', message: (editor && editor.message) || '保存できるのは名簿の人だけです' };
  return { ok: true, reason: null, message: null };
}

/**
 * 画面に出す値。code = null (新商品) / 商品コード。supplier = 今の代表の仕入先 (商品の画面)。
 * { canSee, canWrite, why, data (readOrderSettingsForScreen の答え・見られないとき null), supplier }
 */
export function orderScreen(req, editor, code = null, { supplier = null } = {}) {
  const gate = orderWriteGate(req, editor);
  if (gate.reason === 'no_po_access') return { canSee: false, canWrite: false, why: gate.message, data: null, supplier };
  const data = readOrderSettingsForScreen(code);
  return { canSee: true, canWrite: gate.ok && data.ok, why: gate.ok ? (data.ok ? '' : data.error) : gate.message, data, supplier };
}

/**
 * 画面の本文 (order_settings / values) → 部品の入力 { patch, newCondition, newMaterial }。
 * 新商品の登録: 全部空 = null (発注アプリに何も書かない)。商品の画面 (allowEmpty) = 全部空でも送る (空にする)
 */
export function orderInputOf(raw, { allowEmpty = false } = {}) {
  if (raw == null) return null;
  if (typeof raw !== 'object' || Array.isArray(raw)) throw new OrderSettingsError(400, 'invalid_input', '発注の設定の入れ方が違う');
  for (const k of Object.keys(raw)) {
    if (!BODY_KEYS.has(k)) throw new OrderSettingsError(400, 'invalid_input', `「${k}」は発注の設定の項目ではありません`);
  }
  const patch = {};
  for (const k of ATTR_FIELDS) if (Object.prototype.hasOwnProperty.call(raw, k)) patch[k] = raw[k];
  const newCondition = raw.new_condition == null ? null : raw.new_condition;
  const newMaterial = raw.new_material == null ? null : raw.new_material;
  const empty = Object.values(patch).every(blank) && !newCondition && !newMaterial;
  if (empty && !allowEmpty) return null;
  return { patch, newCondition, newMaterial };
}

/** 部品の誤り → 応答 (欄の名前は画面の行 data-row = order_settings.<欄>) */
export function orderErrorBody(e) {
  const field = e.extra && e.extra.field ? `order_settings.${e.extra.field}` : 'order_settings';
  return { ok: false, error: `発注の設定: ${e.message}`, reason: e.reason, ...e.extra, field };
}
