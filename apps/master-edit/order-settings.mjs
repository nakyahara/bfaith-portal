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
import { readOrderSettingsForScreen, ATTR_FIELDS, OrderSettingsError, parseAttrPatch, parseNewCondition, parseNewMaterial } from '../purchase-orders/order-settings.js';

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
 * 🆕 2026-10-13 (入力なし・#1706 Codex R1 M2): 新商品の登録で発注の設定の 6 つの項目を「値か入力なし」として数えるか。
 *   画面 (views/_order.ejs の「入力なし」と public/me-order.js の items) と API (lib/master-register-blanks.mjs の orderApplicable) は
 *   どちらもこの 1 つの答えを使う = 権限が無い・発注アプリの設定を読めない (画面に欄が出ない) ときは、API も聞かない
 */
export function orderNoneApplies(os) {
  return !!os && os.canSee === true && os.canWrite === true && !!os.data && os.data.ok === true;
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

/**
 * 🆕 0075 (CompanyDB構想 21 C1): 画面の発注の設定 (orderInputOf の答え { patch, newCondition, newMaterial }) と代表の仕入先 →
 *   「発注の設定の知らせ」の中身の共通の部分 (コード以外・lib/order-settings-outbox.mjs の orderPayloadOf に渡す)。
 *   形は発注アプリの部品の決まりで確かめ (誤り = OrderSettingsError)、数は文字にする (DB の hash の stable は整数しか扱わない・
 *   届けるときに発注アプリの部品がもう一度数に直して確かめる)
 */
export function orderBaseOf({ order, supplierCode }) {
  if (!order || typeof order !== 'object' || Array.isArray(order)) throw new OrderSettingsError(400, 'invalid_input', '発注の設定の入れ方が違う');
  const str = (v) => (v == null ? null : String(v));
  const patch = parseAttrPatch(order.patch ?? {});
  const nc = parseNewCondition(order.newCondition ?? null);
  const nm = parseNewMaterial(order.newMaterial ?? null);
  return {
    supplier_code: supplierCode == null || String(supplierCode).trim() === '' ? null : String(supplierCode),
    patch: Object.fromEntries(Object.entries(patch).map(([k, v]) => [k, str(v)])),
    new_condition: nc ? { condition_id: nc.condition_id, display_name: nc.display_name, condition_type: nc.condition_type, unit: nc.unit,
      condition_value: str(nc.condition_value), maker_name: str(nc.maker_name) } : null,
    new_material: nm ? { group_id: nm.group_id, name: nm.name, min_order_qty: str(nm.min_order_qty), unit: str(nm.unit) } : null,
  };
}
