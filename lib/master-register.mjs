/**
 * master-register.mjs — 新商品の登録 (画面 D = apps/master-edit/new?kind=single|set)
 * (Company DB構想 14 §2 D・§9 v2 H4・§10 契約 v3 H3 / Medium 1・§11 の追加の要望 / 10 §2・§3)
 *
 * 保存 1 回 = 1 つの Postgres の取引で、登録だけの security definer の関数 ops.register_new_sku (0052) を 1 回呼ぶ:
 *   番号を振る → 登録の約束 (0051 の約束の表・operation = sku_create) → SKU (単品は商品の行も) + 登録の状態 draft + 代表の仕入先 + 原価 (今日から) +
 *   セットの構成 (= 構成の依頼 ops.sku_component_requests。core.sku_components は「NE で確かめた構成」なので書かない = ⑤-1 と同じ約束) +
 *   product-hub のカードの知らせ (ops.product_hub_outbox) + 保存の記録 (ops.master_edit_requests の sku_create・done) を関数の中の 1 か所で
 *   (⑤-1 = 0051 の 8. の「⑤-2a へ」。画面のロール master_edit に core.products / core.skus / 知らせの INSERT を渡さない = 関数の実行だけ)。
 *   値の計算 (送料の表・仕入先・セットの導く値・カードの中身) はここ (アプリ) でして、関数に渡す
 * 閉じている (⑤-1 と同じ門): 切替の段階が new_open **かつ** 使う列の持ち主が全部 'company' **かつ** env MASTER_EDIT_OPEN = 1。
 *   どれかが欠ける = 409 before_cutover (何も書かない・失敗の記録は残す)
 * コードの決まり (中原さん 2026-10-01): 人が入れる (validateNewSkuCode)。🆕 2026-10-08 夜 (Company DB構想 20 §③・0064): 大文字も使える =
 *   中の鍵 (重なりの確かめ・新しいコードの鍵) は norm (小文字)・外 (SKU の code・CSV・NE・カード) と要求のハッシュは打ったとおり。そのうえで DB を見て
 *   Company DB に無い・代表の名札 / 商品のコードと同じでない・NE の元のコード (0041) に無い・消した SKU のコードでない (使い回さない)
 * 同じ request_id = 残した結果・誤りを返す (中身が違う = 409)。
 * 鍵の順 (⑤-1 の saveSku と同じ): request_id の鍵 → 段階の共有の鍵 → (門) → マスタの書き込みの鍵 (共有・夜間ロードの最中は 3 秒で 409 nightly_load) →
 *   新しいコードの鍵 → 構成品の SKU ごとの鍵 (sku_id の順) → ops.register_new_sku (段階・持ち主表・backfill・コードの決まりを DB でももう一度確かめる)
 * 🚨 DB の守り (0051 の trg_master_edit_guard) は、関数の中の INSERT も呼び手 master_edit として見る (列の持ち主 = skus.sku_kind なども)。
 *    NEW_ENTRY_KEYS はその列を全部含む (試験: scripts/test-master-register.mjs が 0051 の ops.master_edit_owner_keys と突き合わせる)
 * 接続 = 画面だけのロール master_edit (COMPANY_DB_MASTER_EDIT_URL)。権限は scripts/company-db/create-master-edit-roles.mjs
 * 🚨 SQLite と NE には書かない (カードは outbox の後で・NE へは新規登録の CSV = ⑤-2b)
 * 🆕 0075 (CompanyDB構想 21 C1): 発注の設定 (input.order = lib/order-settings-outbox.mjs の orderBaseOf の答え) があれば、登録の関数の後・commit の前に
 *   同じ取引で「発注の設定の知らせ」(ops.order_settings_outbox) を積む = 登録だけ入って発注の設定が消えることは無い (届けるのは commit の後・何度でも)。
 *   🚨 発注の設定は要求のハッシュに**入れない** (0075 の本適用をまたいだ同じ要求の押し直しが同じハッシュ = 冪等・Codex #1703 R1 M2)。押し直しの発注の設定は
 *   呼び手 (apps/master-edit/router.mjs) が知らせの中身の hash と別に照らす。0075 の前の DB = 積まない (opts.orderOutbox が true の時だけ積む・呼び手が今までどおり ② をすぐ書く)
 */
import { normSku } from './sku-norm.js';
import { SKU_LOCK_SQL } from '../apps/master-decisions/ne-csv-lock.mjs';
import { canonicalSupplierCode } from '../apps/company-db/load/sources.mjs';
import { baseGateInTx, newEntryGateInTx, lockCutoverSharedInTx, acquireNewEntryLocksInTx } from './master-owner-gate.mjs';
import { deriveSetCdb } from './master-set-rules.js';
import {
  MasterWriteError, ownerGateError, newEntryClosedFromDb, PARSERS, textIn, intIn, blank, stable, sha256, validateNewSkuCode, componentFacts, jstDate,
  COMPANY_ID, SOURCE_SYSTEM, MAX_COMPONENTS, MAX_YEN, REASON_MAX, REQUEST_LOCK_SQL, lockMasterWriteShared, assertSupplierConfirmed,
} from './master-write.mjs';
import { buildCardPayload, cardEventOf } from './product-hub-outbox.mjs';
import { SET_DECISION_REASONS } from '../apps/product-hub/lib/set-decision.js';
import { SHIPPING_METHOD_GROUPS } from '../apps/product-hub/lib/shipping-groups.js';
import { orderPayloadOf, orderPayloadShapeProblem, enqueueOrderSettingsInTx } from './order-settings-outbox.mjs';
import { lzBarcodeIn, lzBarcodeReady, setLzBarcodeInTx, lzCopyIn } from './logizard-barcode.mjs';
import { makerReady, assertMakerUsable, setProductMakerInTx } from './master-maker.mjs';
import { packageIn, packageReady, setProductPackageInTx, packageNotReadyError } from './master-package.mjs';
import { labelFactsIn, labelReady, setProductLabelInTx, labelNotReadyError } from './master-label.mjs';
import { parseSetPlans, SetPlanShapeError } from './set-plan.mjs';
import { BRAND_LIFE_VALUE_KEYS, checkBrandLifeValues, applyBrandLifeRegister } from './master-brand-life.mjs';

export const KINDS_NEW = Object.freeze({ single: '単品', set: 'セット' });
/** 新商品の登録で書く列の持ち主のキー (全部 'company' でないと保存しない) */
export const NEW_ENTRY_KEYS = Object.freeze({
  single: Object.freeze(['skus.name', 'products.name', 'skus.sku_kind', 'skus.tax_rate', 'skus.tax_class', 'skus.handling', 'products.status', 'products.sales_class',
    'skus.standard_price', 'skus.shipping', 'skus.reorder_months', 'supplier_skus.is_primary', 'sku_costs']),
  set: Object.freeze(['skus.name', 'skus.sku_kind', 'skus.tax_rate', 'skus.tax_class', 'skus.handling', 'products.sales_class',
    'skus.standard_price', 'skus.shipping', 'skus.reorder_months', 'sku_costs', 'sku_components']),
});
/** セット商品を作るか (画面の選択肢)。product-hub の draft_set_decisions へ: 作らない = none / 保留 = hold / 作る = hold (「作る予定」のメモ。作るのは product-hub の「セット商品を作る」) */
export const SET_PLAN_CHOICES = Object.freeze({ create: '作る', none: '作らない', hold: '保留' });
export const MAX_REFERENCE_URLS = 20;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const CTRL_RE = new RegExp(`[\\x00-\\x1f\\x7f${String.fromCharCode(0x2028, 0x2029)}]`);
const bad = (message, field, reason = 'invalid_input', extra = {}) => new MasterWriteError(400, reason, message, { field, ...extra });
const DEFAULT_REASON = '新商品の登録';

// ─── 入力の形 (DB を読まない) ───
function urlIn(v, label, field) {
  if (blank(v)) return null;
  if (typeof v !== 'string') throw bad(`${label}は文字で入れてください`, field);
  const s = v.trim();
  if (s.length > 1000) throw bad(`${label}は 1,000 字までです`, field);
  if (CTRL_RE.test(s) || !/^https?:\/\/\S+$/i.test(s)) throw bad(`${label}は http:// か https:// で始まる URL を入れてください`, field);
  return s;
}
function boolIn(v, label, field) {
  if (blank(v)) return null;
  if (v === true || v === 1 || v === '1' || v === 'true') return true;
  if (v === false || v === 0 || v === '0' || v === 'false') return false;
  throw bad(`${label}は あり / なし で選んでください`, field);
}
/**
 * 原価 (新商品): { jpy } だけ。適用日は今日だけ。
 *   🆕 2026-10-08 中原さん「新規登録の際に原価に理由入れる項目は不要」= 画面に原価の理由の欄は無い。
 *   記録 (core.sku_costs.reason) の理由はサーバーが固定で「新商品の登録」を入れる (DB の関数は 1〜200 字を要る)。
 *   前の画面 (開きっぱなし) が reason を送ってきても使わない (断りもしない = 開き直さなくても登録できる)。
 *   登録の後の 1 件の編集・一括の変更の原価の理由は今までどおり要る (master-write / bulk)。
 */
function newCostIn(v, field) {
  if (v == null) return null;
  if (typeof v !== 'object' || Array.isArray(v)) throw bad('原価の入れ方が違う', field);
  if (blank(v.jpy)) return null;
  return { jpy: intIn(v.jpy, 0, MAX_YEN, '原価', field), reason: DEFAULT_REASON };
}

// 🆕 0084: 単品のメーカー (maker = 名簿の番号・Company DB だけ)。セットは聞かない (構成品から = 送ってきたら 400)
const VALUE_KEYS = Object.freeze({
  single: ['name', 'standard_price', 'shipping_code', 'tax_rate', 'sales_class', 'primary_supplier', 'reorder_months', 'cost', 'expiry_managed', 'inbound_date_managed', 'maker',
    // 🆕 0088: ブランド・区分・終売の予定日 (Company DB だけ・必須でない = 「値か入力なし」に数えない・lib/master-brand-life.mjs)
    'brand', 'life_stage', 'end_planned_on'],
  set: ['name', 'standard_price', 'shipping_code', 'reorder_months', 'components', 'handling_own', 'set_sales_class_override', 'exception_cost'],
});

export function parseCard(raw, kind, parentCode = '') {
  const c = raw == null ? {} : raw;
  if (typeof c !== 'object' || Array.isArray(c)) throw bad('product-hub の欄の入れ方が違う', 'card');
  const create = c.create === undefined ? true : boolIn(c.create, 'カードを作る', 'card.create') !== false;
  const refsRaw = c.reference_urls == null ? [] : c.reference_urls;
  if (!Array.isArray(refsRaw)) throw bad('参考 URL の入れ方が違う', 'card.reference_urls');
  const refs = [];
  for (const [i, u] of refsRaw.entries()) {
    const s = urlIn(u, `参考 URL ${i + 1} つ目`, 'card.reference_urls');
    if (s && !refs.includes(s)) refs.push(s);
  }
  if (refs.length > MAX_REFERENCE_URLS) throw bad(`参考 URL は ${MAX_REFERENCE_URLS} 個までです`, 'card.reference_urls');
  let asin = null;
  if (!blank(c.asin)) {
    asin = String(c.asin).trim().toUpperCase();
    if (!/^[A-Z0-9]{10}$/.test(asin)) throw bad('ASIN は英数字 10 字です (例: B0XXXXXXXX)', 'card.asin');
  }
  let setDecision = null;
  const sd = c.set_decision;
  if (sd != null && !(typeof sd === 'object' && blank(sd.decision))) {
    if (kind !== 'single') throw bad('「セット商品を作るか」は単品だけです', 'card.set_decision');
    if (typeof sd !== 'object' || Array.isArray(sd) || !Object.prototype.hasOwnProperty.call(SET_PLAN_CHOICES, sd.decision)) throw bad('「セット商品を作るか」は 作る / 作らない / 保留 から選んでください', 'card.set_decision');
    const reasonCode = blank(sd.reason_code) ? null : String(sd.reason_code);
    const reasonText = textIn(sd.reason_text, { label: 'セットの理由', field: 'card.set_decision', max: 500 });
    if (sd.decision === 'none') {
      if (!reasonCode || !Object.prototype.hasOwnProperty.call(SET_DECISION_REASONS, reasonCode)) throw bad('作らない理由を選んでください', 'card.set_decision');
      if (reasonCode === 'other' && !reasonText) throw bad('「その他」を選んだときは理由を書いてください', 'card.set_decision');
    } else if (reasonCode) throw bad('作らない理由は「作らない」のときだけ選べます', 'card.set_decision');
    setDecision = { decision: sd.decision, reason_code: sd.decision === 'none' ? reasonCode : null, reason_text: reasonText };
  }
  // 🆕 2026-10-17 中原さん: 「作る」のときに作るセット (個数・セット商品コード・セット商品名) = lib/set-plan.mjs の決まり。
  //   新しい画面は「作る」のとき必ず sets を送る (1 つ以上)。送らない (前の画面) = 今までどおり「作る予定」のメモだけ。
  //   🚨 Company DB のカードの知らせ (ph-card-v1) には入れない (0061 の ops.register_new_sku が知らない欄を断る = buildCardPayload は sets を見ない)。
  //   product-hub の表 (ph_cdb_set_plans) に登録の request_id で残し、カードの取り込みが同じ request_id でセットの下書きを作る (apps/master-edit/router.mjs)
  let sets = null;
  if (c.sets !== undefined && c.sets !== null) {
    const empty = Array.isArray(c.sets) && c.sets.length === 0;
    if (!empty) {
      if (kind !== 'single') throw bad('作るセットは単品の登録だけです', 'card.sets');
      if (!create) throw bad('出品カードを作らないときは、セットの下書きも作れません (カードを作るか、作るセットを外してください)', 'card.sets');
      if (!setDecision || setDecision.decision !== 'create') throw bad('作るセットは「セット商品を作るか」が「作る」のときだけ入れられます', 'card.sets');
    }
    if (setDecision && setDecision.decision === 'create' && create && empty) throw bad('作るセットを 1 つ以上入れてください (個数・セット商品コード・セット商品名)', 'card.sets');
    if (!empty) {
      try { sets = parseSetPlans(c.sets, { parentCode }); } catch (e) {
        if (e instanceof SetPlanShapeError) throw bad(e.message, 'card.sets', 'invalid_input', e.index == null ? {} : { index: e.index });
        throw e;
      }
    }
  }
  let yahoo = null;
  const y = c.yahoo;
  if (y != null) {
    if (typeof y !== 'object' || Array.isArray(y)) throw bad('Yahoo! の欄の入れ方が違う', 'card.yahoo');
    // 0 円は断る (空 = 入れない。仮レビュー L5 = 画面も同じ)
    const price = blank(y.price) ? null : intIn(y.price, 1, MAX_YEN, 'Yahoo!売価', 'card.yahoo.price');
    const priceSagawa = blank(y.price_sagawa) ? null : intIn(y.price_sagawa, 1, MAX_YEN, 'Yahoo!売価 (佐川)', 'card.yahoo.price_sagawa');
    let delivery = null;
    if (!blank(y.delivery_label)) {
      delivery = String(y.delivery_label).trim();
      if (!Object.values(SHIPPING_METHOD_GROUPS).includes(delivery)) throw bad('Yahoo! の配送方法は一覧から選んでください', 'card.yahoo.delivery_label');
    }
    let categoryId = null;
    if (!blank(y.category_id)) {
      const s = String(y.category_id).trim();
      if (!/^\d{1,12}$/.test(s)) throw bad('Yahoo!カテゴリID は数字で入れてください', 'card.yahoo.category_id');
      categoryId = Number(s);
    }
    const path = textIn(y.path, { label: 'Yahoo!path', field: 'card.yahoo.path', max: 500 });
    if ([price, priceSagawa, delivery, categoryId, path].some((x) => x != null)) yahoo = { price, price_sagawa: priceSagawa, delivery_label: delivery, category_id: categoryId, path };
  }
  return {
    create,
    amazon_url: urlIn(c.amazon_url, 'Amazon URL', 'card.amazon_url'),
    asin,
    official_url: urlIn(c.official_url, '公式ページ URL', 'card.official_url'),
    reference_urls: refs,
    set_decision: setDecision,
    yahoo,
    // 作るセットがあるときだけ鍵を足す (無い登録の要求のハッシュは前と同じ = 押し直しが冪等)
    ...(sets ? { sets } : {}),
  };
}

/** 画面から来た登録の中身を確かめて決まった形にする (DB を読まない。ここで落ちた保存は記録しない) */
export function parseRegisterRequest(input) {
  const actor = String(input?.actor ?? '').trim().toLowerCase();
  if (!actor || actor.length > 320 || CTRL_RE.test(actor)) throw bad('保存する人 (ログインのメール) が分からない', 'actor');
  const requestId = String(input?.requestId ?? '').trim().toLowerCase();
  if (!UUID_RE.test(requestId)) throw bad('保存の番号 (request_id) の形が違う。画面を開き直してください', 'request_id');
  const kind = String(input?.kind ?? '');
  if (!Object.prototype.hasOwnProperty.call(KINDS_NEW, kind)) throw bad('種類は 単品 か セット です', 'kind');
  const cv = validateNewSkuCode(input?.code);
  if (!cv.ok) throw bad(cv.message, 'code');
  const reason = textIn(input?.reason, { label: '理由', field: 'reason', max: REASON_MAX });
  const raw = input?.values;
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) throw bad('保存する値が無い', 'values');
  const values = {};
  for (const [k, v] of Object.entries(raw)) {
    if (!VALUE_KEYS[kind].includes(k)) throw bad(`「${k}」は${KINDS_NEW[kind]}の登録では入れられません`, k);
    if (v === undefined) continue;
    if (k === 'cost' || k === 'exception_cost') values[k] = newCostIn(v, k);
    else if (k === 'expiry_managed') values[k] = boolIn(v, '有効期限の管理', k);
    else if (k === 'inbound_date_managed') values[k] = boolIn(v, '入荷日の管理', k);
    else values[k] = PARSERS[k](v);
    if (k === 'maker' && values.maker == null) delete values.maker;   // 🆕 0084: 空 = 送らないのと同じ (要求のハッシュも同じ)
    if (BRAND_LIFE_VALUE_KEYS.includes(k) && values[k] == null) delete values[k];   // 🆕 0088: ブランド・段階も同じ
  }
  // 要る欄 (中原さん 2026-10-01: 売価は必須。名前・単品の税率も)。
  //   🆕 2026-10-08 中原さん: 単品の代表の仕入先は必須 (NE で必須の項目)・発送方法 (送料コード) は必須にしない (届いてサイズを見てから商品の画面で入れる)。
  //   DB の登録の関数 (0060) も同じ決まりをもう一度確かめる
  if (values.name == null) throw bad('名前を入れてください', 'name');
  if (values.standard_price == null) throw bad('売価 (標準売価) を入れてください', 'standard_price');
  if (kind === 'single' && values.tax_rate == null) throw bad('税率を選んでください', 'tax_rate');
  if (kind === 'single' && values.primary_supplier == null) throw bad('代表の仕入先を選んでください (NE で必須の項目です)', 'primary_supplier');
  // 🆕 2026-10-08 中原さん「下書き保存時にわかる内容だから必須にする」: 単品の売上分類 (「あとで」は無い)・ロジザードの有効期限の管理 (なし / あり を選ぶ。
  //   前は選ばないと「なし」になっていた)・推奨保有月数 (単品もセットも) は下書きの保存で要る。入荷日の管理は今までどおり (選ばないと空 = 不明)。
  //   セットの売上分類は構成品から導く (導けないときだけ上書きが要る = registerInTx の set_underivable) = ここでは要らない。DB の登録の関数 (0061) も同じ決まりをもう一度確かめる
  if (kind === 'single' && values.sales_class == null) throw bad('売上分類を選んでください', 'sales_class');
  if (kind === 'single' && values.expiry_managed == null) throw bad('ロジザードの有効期限の管理を なし / あり から選んでください', 'expiry_managed');
  if (values.reorder_months == null) throw bad('推奨保有月数を入れてください (0〜60 か月)', 'reorder_months');
  if (kind === 'set') {
    if (!values.components) throw bad('構成品を入れてください', 'components');
    if (values.handling_own == null) values.handling_own = 'active';
  }
  const card = parseCard(input?.card, kind, cv.code);
  const order = parseOrderBase(input?.order, kind, cv.code);
  // 🆕 0080 (CompanyDB構想 23 v4): ロジザードに送るバーコードの値 ({ kind: 'value', value } / { kind: 'lz_assign' } = 空欄)。単品だけ (セットは聞かない = 送ってきたら断る)。
  //   送らない = null (要るかは API の入口 = router が決める = 0080 がある DB の新しい画面は必ず送る)
  const lzBarcode = lzBarcodeIn(input?.logizardBarcode);
  if (lzBarcode != null && kind !== 'single') throw bad('ロジザードのバーコードは単品だけです (セットは聞きません)', 'logizard_barcode');
  // 🆕 #1719 R1 High 2: ロジザードの写しと照らした結果 (router が渡す・値の行に残す・要求のハッシュには入れない)
  const lzCopy = lzCopyIn(input?.lzCopy);
  // 🆕 0087: 梱包の 縦・横・高さ (cm)・重さ (g) = { length, width, height, weight } (入れなくてよい・送らない / 全部空 = null)。単品だけ (セットは聞かない = 送ってきたら断る)
  const pkg = packageIn(input?.package);
  if (pkg != null && kind !== 'single') throw bad('梱包の大きさ・重さは単品だけです (セットは構成品から)', 'package');
  // 🆕 0090: 表示の項目 = { country, net, net_uom, model, shelf } (入れなくてよい・送らない / 全部空 = null)。単品だけ (セットは聞かない = 送ってきたら断る)
  const lbl = labelFactsIn(input?.label);
  if (lbl != null && kind !== 'single') throw bad('表示の項目 (原産国・内容量・型番・消費期限) は単品だけです (セットは構成品から)', 'label');
  return { actor, requestId, kind, code: cv.code, codeNorm: normSku(cv.code), reason, values, card, order, lzBarcode, lzCopy, pkg, lbl };
}
/**
 * 🆕 0075: 発注の設定 (orderBaseOf の答え = 形はもう確かめた・数は文字) か null。単品だけ (発注アプリはセットを扱わない)。
 *   形は知らせの中身と同じ決まり (orderPayloadShapeProblem = DB の ops.order_settings_payload_problem と同じ) でもう一度確かめる
 */
export function parseOrderBase(raw, kind, code) {
  if (raw == null) return null;
  if (kind !== 'single') throw bad('セットには発注の設定がありません (発注アプリはセットを扱わず、構成品で発注します)', 'order_settings', 'order_settings_set');
  const prob = typeof raw === 'object' && !Array.isArray(raw) ? orderPayloadShapeProblem(orderPayloadOf(code, raw)) : 'payload_not_object';
  if (prob) throw bad(`発注の設定の形が違う (${prob})`, 'order_settings');
  return raw;
}
// 🆕 0064: コードは打ったとおりの書き方で比べる (大文字も使える = ABC-1 と abc-1 は NE では別のコード = 同じ request_id の違う中身)。小文字だけのコードは前と同じハッシュ (code = norm)
//   🆕 0075: 発注の設定は入れない (0075 の前後で同じ要求 = 同じハッシュ。押し直しの発注の設定は呼び手が知らせと照らす)
/** 発注の設定の入力なしの一言 (理由の後ろに足す・制御文字なし・理由と合わせて 200 字まで) */
export function reasonNoteIn(v, reason) {
  if (v == null || v === '') return null;
  if (typeof v !== 'string' || CTRL_RE.test(v)) throw bad('理由の一言の入れ方が違う', 'reason');
  if ([...((reason || DEFAULT_REASON) + v)].length > REASON_MAX) throw bad(`理由が長すぎます (「入力なし」の項目の一覧と合わせて ${REASON_MAX} 字まで)`, 'reason', 'reason_too_long');
  return v;
}
/** DB に書く理由 (理由 + 発注の設定の入力なしの一言) */
const dbReasonOf = (req) => (req.reason || DEFAULT_REASON) + (req.reasonNote || '');
//   🆕 0080: ロジザードのバーコードは送ったときだけ入れる (送らない古い画面の要求のハッシュは前と同じ)
//   🆕 0087: 梱包の大きさ・重さ・🆕 0090: 表示の項目も入れたときだけ (入れない要求のハッシュは前と同じ)
export const registerPayloadHashOf = (req) => sha256(stable({ kind: req.kind, code: req.code, reason: req.reason, values: req.values, card: req.card,
  ...(req.lzBarcode != null ? { logizard_barcode: req.lzBarcode } : {}), ...(req.pkg != null ? { package: req.pkg } : {}), ...(req.lbl != null ? { label: req.lbl } : {}) }));

async function regclass(db, name) {
  return (await db.query('select to_regclass($1) is not null as ok', [name])).rows[0].ok;
}

/** コードの問題 (DB の ops.new_sku_code_problem の答え) → 画面の文 */
export const CODE_PROBLEM_MESSAGES = Object.freeze({
  code_shape: (c) => `商品コード ${c} は形が違います (英字 (大文字・小文字)・数字・- と _ の 30 字まで・set- で始めない)`,
  code_taken: (c) => `商品コード ${c} はもう Company DB にあります`,
  code_is_rep: (c) => `商品コード ${c} は代表 (名札) か商品のコードとして使われています`,
  code_in_ne: (c) => `商品コード ${c} は NE にもうあります (NE から翌朝の取り込みで入るのを待つか、別のコードにしてください)`,
  code_used_before: (c) => `商品コード ${c} は前に使って消したコードです (使い回さない)`,
});

/**
 * 新しいコードを DB で確かめる (画面の「確かめる」と保存で同じ)。戻り値 { ok, reason, message }
 *   決まりは DB の ops.new_sku_code_problem (0052) 1 か所 = 登録の関数も同じものを呼ぶ:
 *   Company DB に無い / 代表の名札・商品の display_code と同じでない / NE の元のコード (0041 の最新の照合) に無い / 消した SKU のコードでない
 *   (どれも norm = 小文字で比べる)。🆕 0064: 答えの code・文 = 打ったとおりの書き方 (大文字も)
 */
export async function checkNewCodeInDb(db, code) {
  const shape = validateNewSkuCode(code);
  if (!shape.ok) return { ok: false, reason: 'code_shape', message: shape.message };
  const p = (await db.query('select ops.new_sku_code_problem($1) as p', [shape.code])).rows[0].p;
  if (p) return { ok: false, reason: p, message: (CODE_PROBLEM_MESSAGES[p] || ((c) => `商品コード ${c} は使えません (${p})`))(shape.code) };
  return { ok: true, reason: null, message: null, code: shape.code };
}

/**
 * 新商品を登録する。input = { actor, requestId, kind, code, reason?, values: {...}, card: {...} }
 * opts = { open (MASTER_EDIT_OPEN)・ownership (試験で差し替え)・now (試験で「今日」)・shippingRates: Map<送料コード, {method, cost}> | null・beforeCommit (試験) }
 * 戻り値 = 結果 (保存の記録にも同じもの)。誤りは MasterWriteError
 */
export async function registerNewSku(db, input, opts = {}) {
  // 持ち主表 = 取引の中で読む DB の active (広げる道 PR-2)。opts.ownership (配った config の持ち主表) はもう使わない
  const req = parseRegisterRequest(input);
  // 🆕 #1706 Codex R2 M: 発注の設定の「入力なし」の一言 (router が初回だけ作る)。DB に書く理由にだけ足す = 要求のハッシュ (registerPayloadHashOf) に入らない
  req.reasonNote = reasonNoteIn(input?.reasonNote, req.reason);
  const ctx = { ownership: null, open: opts.open === true, env: opts.env || process.env, shippingRates: opts.shippingRates ?? null, payloadHash: registerPayloadHashOf(req), today: null, startedAt: null, skuId: null,
    orderOutbox: opts.orderOutbox === true, requireLz: opts.requireLz === true,
    // 🆕 2026-10-17 (Codex R2 Medium): このコードがほかの登録の作るセットに決まっていないか (product-hub の表・呼び手 = router が渡す)。コードの鍵を取った後にもう一度見る
    codeReserved: typeof opts.codeReserved === 'function' ? opts.codeReserved : null };
  await db.query('begin');
  try {
    const t = (await db.query(`select now()::text as started, (now() at time zone 'Asia/Tokyo')::date::text as today`)).rows[0];
    ctx.startedAt = t.started;
    ctx.today = opts.now ? jstDate(opts.now) : t.today;
    const out = await registerInTx(db, req, ctx);
    if (out.replay) { await db.query('rollback'); return out.replay; }
    // 保存の記録 done は登録の関数が同じ取引で書いた (0051 の commit の確かめ = 約束どおりの done がちょうど 1 つ)
    if (opts.beforeCommit) await opts.beforeCommit();
    await db.query('commit');
    return out.result;
  } catch (e0) {
    let e = e0;
    try { await db.query('rollback'); } catch { /* 接続が切れていれば rollback も失敗する */ }
    // 同じコードを夜間ロード (NE から) かほかの登録が先に入れた (コードを確かめた後・入れる前) = 409 もうある
    if (e && e.code === '23505' && /skus_company_id_code_norm_key/.test(String(e.constraint || e.message || ''))) {
      e = new MasterWriteError(409, 'code_taken', `商品コード ${req.code} はちょうどほかの処理が Company DB に入れました。何も保存していません`, { field: 'code' });
    }
    // 同じ request_id を別の取引が先に書いた = 残った記録を返す (⑤-1 の saveSku と同じ)
    if (e && e.code === '23505' && /master_edit_requests_pkey/.test(String(e.constraint || e.message || ''))) {
      const prev = (await db.query('select actor_id, operation, payload_hash, status, result, error from ops.master_edit_requests where request_id = $1', [req.requestId])).rows[0];
      return replayOrThrow(prev, req, ctx);
    }
    if (ctx.startedAt && !(e instanceof MasterWriteError && (e.reason === 'request_id_reused' || e.extra?.replayed))) {
      const locked = e && e.code === '55P03';
      const err = e instanceof MasterWriteError
        ? { status: e.status, reason: e.reason, message: e.message, extra: e.extra }
        : { status: locked ? 409 : 500, reason: locked ? 'locked' : 'error', message: locked ? 'ほかの処理が同じ商品を使っていました' : 'サーバーエラー', pg_code: e && e.code ? String(e.code) : null };
      const prev = await recordFailure(db, req, ctx, err);
      if (prev) return replayOrThrow(prev, req, ctx);   // 待っている間に同じ request_id の登録が終わっていた = その結果
    }
    throw e;
  }
}

/** 失敗の記録を新しい取引で (request_id の鍵を取り、もう結果があれば書かない = その結果を返す)。書けなければ null */
async function recordFailure(db, req, ctx, err) {
  try {
    await db.query('begin');
    await db.query(REQUEST_LOCK_SQL, [req.requestId]);
    const prev = (await db.query('select actor_id, operation, payload_hash, status, result, error from ops.master_edit_requests where request_id = $1', [req.requestId])).rows[0];
    if (!prev) {
      await db.query(`insert into ops.master_edit_requests (request_id, company_id, operation, target_code, sku_id, actor_id, payload_hash, status, error, started_at)
         values ($1, $2, 'sku_create', $3, null, $4, $5, 'failed', $6::jsonb, $7::timestamptz)`,
      [req.requestId, COMPANY_ID, req.code, req.actor, ctx.payloadHash, JSON.stringify(err), ctx.startedAt]);
    }
    await db.query('commit');
    return prev || null;
  } catch (e2) {
    try { await db.query('rollback'); } catch { /* */ }
    console.error(`[master-register] 失敗の記録を残せなかった: ${e2 && e2.message}`);
    return null;
  }
}

/** 残っている同じ request_id の記録 → 結果 / 同じ誤り / 中身が違えば 409 */
function replayOrThrow(prev, req, ctx) {
  // done の payload_hash = DB が作った「書いた値」のハッシュ (0052)。画面の要求のハッシュは結果の request_payload_hash に残っている。failed は要求のハッシュのまま
  const reqHash = prev && prev.status === 'done' && prev.result && prev.result.request_payload_hash ? prev.result.request_payload_hash : prev && prev.payload_hash;
  if (!prev || prev.actor_id !== req.actor || prev.operation !== 'sku_create' || reqHash !== ctx.payloadHash) {
    throw new MasterWriteError(409, 'request_id_reused', '同じ保存の番号 (request_id) で違う中身が来ました。画面を開き直してください');
  }
  if (prev.status === 'done') return { ...prev.result, replayed: true };
  const x = prev.error || {};
  throw new MasterWriteError(x.status || 500, x.reason || 'error', x.message || '前の保存は失敗しました', { ...(x.extra || {}), replayed: true });
}

async function registerInTx(db, req, ctx) {
  const v = req.values;
  await db.query(`select set_config('core.actor_type', 'human', true), set_config('core.actor_id', $1, true), set_config('core.source_system', $2, true),
      set_config('core.request_id', $3, true), set_config('core.reason', $4, true), set_config('core.run_id', '', true)`,
  [req.actor, SOURCE_SYSTEM, req.requestId, dbReasonOf(req)]);
  // 1. request_id の鍵 → 同じ request_id (⑤-1 と同じ)
  await db.query(REQUEST_LOCK_SQL, [req.requestId]);
  const prev = (await db.query('select actor_id, operation, payload_hash, status, result, error from ops.master_edit_requests where request_id = $1', [req.requestId])).rows[0];
  if (prev) return { replay: replayOrThrow(prev, req, ctx) };
  // 1b. 🆕 新規開始の鍵 (許可の共有の鍵を single → set の順に全部。0058 の ops.acquire_new_entry_locks・段階の鍵より前 = 0058 と同じ鍵の順。Codex #1640 R3 Medium 1)。答えは 2 層目の門が使う
  ctx.nePre = await acquireNewEntryLocksInTx(db, req.kind);
  if (ctx.nePre.error) throw ownerGateError(ctx.nePre.refusal);
  // 2. 段階の共有の鍵 (段階を変える取引と並ぶ = この取引の中で段階は変わらない) → 門 (段階 new_open・持ち主表のハッシュ・列の持ち主・MASTER_EDIT_OPEN・backfill)
  await lockCutoverSharedInTx(db);   // 55P03 は 1 回だけ取り直す (設計 M8)
  await assertNewEntryOpen(db, req, ctx);
  // 3. マスタの書き込みの鍵 (共有・夜間ロードが持っていれば短く待って 409 nightly_load) → 新しいコードの鍵 (同じコードを 2 人が同時に登録 = 後の人は待って「もうある」)
  await lockMasterWriteShared(db);
  //    🆕 2026-10-17 (Codex R2 Medium): 作るセットのコードの鍵も取る (同じ鍵 = ふつうの登録とセットの予約が並ばない)。並び = norm の順 (2 つの登録が逆の順で取って止まらない)
  for (const norm of [...new Set([req.codeNorm, ...(req.card.sets || []).map((sp) => normSku(sp.code))])].sort()) {
    await db.query(`select pg_advisory_xact_lock(hashtextextended('core.new_code:' || $1::text, 0))`, [norm]);
  }
  // 4. コード
  const cc = await checkNewCodeInDb(db, req.code);
  if (!cc.ok) throw new MasterWriteError(409, cc.reason, `${cc.message}。何も保存していません`, { field: 'code' });
  // 4a. 🆕 2026-10-17 (Codex R2): ほかの登録の作るセットに決まったコード (product-hub の予約・セットの下書き) = 鍵を取った後にもう一度 (入口の確かめとの間に予約されても通さない)
  if (ctx.codeReserved) {
    const held = await ctx.codeReserved(req.code);
    if (held) throw new MasterWriteError(held.status || 409, held.reason || 'set_code_reserved', `${held.message}。何も保存していません`, { field: 'code' });
  }
  // 4b. 🆕 2026-10-17: 作るセットのコードも同じ決まり (Company DB に無い・名札 / 商品のコードでない・NE で見たことがない・消したコードでない)。読むだけ
  //     (セットの SKU は作らない。product-hub のカードとの重なりは呼び手 (router) が登録の前に SQLite で確かめて予約する)
  for (const [i, sp] of (req.card.sets || []).entries()) {
    const sc = await checkNewCodeInDb(db, sp.code);
    if (!sc.ok) throw new MasterWriteError(409, sc.reason, `作るセットの ${i + 1} 行目: ${sc.message}。何も保存していません`, { field: 'card.sets', index: i });
  }
  // 5. 値 (送料・仕入先・構成)。発送方法が無い (2026-10-08 から必須でない) = 3 つとも null・送料の表は読まなくてよい
  let shipping = { code: null, method: null, cost_jpy: null };
  if (v.shipping_code != null) {
    if (!ctx.shippingRates) throw new MasterWriteError(503, 'shipping_rates_unavailable', '送料の表が読めないので、選んだ発送方法を確かめられません (発送方法を空にすれば登録できます)。何も保存していません', { field: 'shipping_code' });
    const rate = ctx.shippingRates.get(v.shipping_code);
    if (!rate) throw bad(`送料コード ${v.shipping_code} は送料の表にありません`, 'shipping_code');
    if (!rate.method || !String(rate.method).trim()) throw bad(`送料コード ${v.shipping_code} に発送方法 (名前) がありません。送料の表を直してから登録してください`, 'shipping_code');
    shipping = { code: v.shipping_code, method: rate.method ?? null, cost_jpy: rate.cost == null ? null : Math.round(Number(rate.cost)) };
  }
  let supplier = null;
  if (req.kind === 'single') {   // 単品は代表の仕入先がある (parseRegisterRequest が確かめた)
    const want = canonicalSupplierCode(v.primary_supplier);
    supplier = (await db.query('select supplier_id::text as id, code, active from core.suppliers where company_id = $1 and code_norm = core.norm_code($2)', [COMPANY_ID, want])).rows[0];
    if (!supplier) throw bad(`仕入先 ${want} は Company DB にありません`, 'primary_supplier');
    if (!supplier.active) throw bad(`仕入先 ${want} は取引停止なので代表にできません`, 'primary_supplier');
    await assertSupplierConfirmed(db, supplier);   // 0053 (⑤-2b): 「NE に登録した」の申告の前の新しい仕入先は代表にできない (DB の trigger も見る)
  }
  let rows = [];
  let derived = null;
  if (req.kind === 'set') {
    rows = await resolveNewComponents(db, v.components, ctx.today);
    derived = deriveSetCdb(rows, { override: v.set_sales_class_override, handlingOwn: v.handling_own, currentHandling: null, exceptionCost: !!v.exception_cost });
    if (v.set_sales_class_override != null && derived.salesFromComponents != null) {
      throw bad(`構成品から売上分類 (${derived.salesFromComponents}) を導けるので、上書きはできません (導けないとき・輸出 4 が混ざるときだけ)`, 'set_sales_class_override');
    }
    if (derived.blockers.length) throw bad(`このセットは導く値が決まらないので登録しません: ${derived.blockers.join(' / ')}`, 'set', 'set_underivable', { blockers: derived.blockers });
  }
  // 気をつけること (税率の混在・中止の構成品・名前の末尾の資材の印) と このあと の文は、登録の関数が DB の値から結果に入れる (#1566 Codex R4 Medium)
  // 6. 書く値 (登録の関数に渡す)
  const tax = req.kind === 'single'
    ? { rate: v.tax_rate, class: v.tax_rate === 0.08 ? 'REDUCED_8' : 'STANDARD_10' }
    : { rate: derived.tax.taxRate, class: derived.tax.taxClass };
  const handling = req.kind === 'single' ? 'active' : derived.handling;
  let cost = null;
  if (req.kind === 'single' && v.cost) cost = { jpy: v.cost.jpy, source: 'manual', status: 'COMPLETE', reason: v.cost.reason };
  if (req.kind === 'set') {
    if (v.exception_cost) cost = { jpy: v.exception_cost.jpy, source: 'manual', status: 'OVERRIDDEN', reason: v.exception_cost.reason };
    else if (derived.cost.status === 'COMPLETE') cost = { jpy: derived.cost.jpy, source: 'set_calc', status: 'COMPLETE', reason: `構成品から計算 (${DEFAULT_REASON})` };
  }
  // 新しいセットの構成 = 構成の依頼 (元の構成なし)。NE に登録して NE の構成が同じと確かめたら promoteComponentRequest が core に上げる
  const reqRows = rows.map((r) => ({ sku_id: Number(r.child_sku_id), code: r.code, qty: r.qty, sort: r.sort }));
  // カードの知らせ (SKU の番号は知らせの行で結ぶ = payload に入れない・0052)
  const cardEvent = req.card.create
    ? cardEventOf(buildCardPayload({ code: req.code, kind: req.kind, name: v.name, price: v.standard_price, shipping, card: req.card, components: rows, actor: req.actor }))
    : null;
  const entry = {
    kind: req.kind, code: req.code, started_at: ctx.startedAt,
    product: req.kind === 'single'
      ? { name: v.name, sales_class: v.sales_class ?? null, expiry_managed: v.expiry_managed, inbound_date_managed: v.inbound_date_managed ?? null } : null,
    sku: {
      name: v.name, tax_rate: tax.rate, tax_class: tax.class, handling, standard_price_jpy: v.standard_price,
      shipping_code: shipping.code, shipping_method: shipping.method, shipping_cost_jpy: shipping.cost_jpy, reorder_months: v.reorder_months ?? null,
      set_sales_class_override: req.kind === 'set' ? (v.set_sales_class_override ?? null) : null, handling_own: req.kind === 'set' ? v.handling_own : null,
    },
    supplier_id: supplier ? supplier.id : null,
    cost: cost ? { jpy: cost.jpy, source: cost.source, status: cost.status, valid_from: ctx.today, reason: cost.reason } : null,
    component_request: req.kind === 'set' ? { rows: reqRows, rows_hash: sha256(stable(reqRows)), reason: `${DEFAULT_REASON} (構成)` } : null,
    card: cardEvent,
  };
  // 6b. 🆕 0080: ロジザードのバーコードを書けるか (関数を名前で確かめる)。0080 の前の DB に値が来た = 断る (何も保存しない・黙って捨てない)
  //     🆕 API の入口 (router = opts.requireLz) = 0080 がある DB の単品は 値か「空欄」が要る (無い = 前の画面 = 400・門・コードの確かめの後 = 閉じている理由を先に)
  const lzReady = (req.lzBarcode != null || (ctx.requireLz && req.kind === 'single')) ? await lzBarcodeReady(db) : false;
  if (req.lzBarcode != null && !lzReady) {
    throw new MasterWriteError(503, 'db_not_ready', 'Company DB に 0080 (ロジザードのバーコードの値) がまだ入っていないか、権限の流し直しの前です。何も保存していません', { field: 'logizard_barcode' });
  }
  if (ctx.requireLz && req.kind === 'single' && lzReady && req.lzBarcode == null) {
    throw new MasterWriteError(400, 'lzbc_missing', 'ロジザードのバーコードを入れるか「空欄 (ロジザードが番号を付ける)」を選んでください (前の画面には欄がありません。画面を開き直してください)。何も保存していません', { field: 'logizard_barcode' });
  }
  // 6c. 🆕 0084: メーカー (送ったときだけ・単品)。0084 の前の DB・ロールの流し直しの前 = 断る (何も保存しない・黙って捨てない)・名簿に無い = 400
  if (v.maker != null) {
    if (!(await makerReady(db))) throw new MasterWriteError(503, 'db_not_ready', 'Company DB に 0084 (メーカー) がまだ入っていないか、権限の流し直しの前です。何も保存していません', { field: 'maker' });
    await assertMakerUsable(db, v.maker);
  }
  // 6c'. 🆕 0087: 梱包の大きさ・重さ (入れたときだけ・単品)。0087 の前の DB・ロールの流し直しの前 = 断る (何も保存しない・黙って捨てない)
  if (req.pkg != null && !(await packageReady(db))) throw packageNotReadyError();
  // 6d. 🆕 0088: ブランド・区分・終売の予定日 (送ったときだけ・単品)。0088 の前の DB・ロールの流し直しの前 = 断る (何も保存しない)・名簿に無いブランド = 400
  if (req.kind === 'single') await checkBrandLifeValues(db, v);
  // 6d. 🆕 0090: 表示の項目 (入れたときだけ・単品)。0090 の前の DB・ロールの流し直しの前 = 断る (何も保存しない・黙って捨てない)
  if (req.lbl != null && !(await labelReady(db))) throw labelNotReadyError();
  // 7. 書く = 登録の関数 1 回 (番号 → 登録の約束 → 商品・SKU・状態 draft・仕入先・原価・構成の依頼・知らせ → 保存の記録 done)。
  //    結果 (画面に返す・保存の記録に残す) は関数が DB の値だけから作る = ここでは作らない・渡さない (#1566 Codex R4 Medium)
  const out = await callRegister(db, req, ctx, entry);
  ctx.skuId = out.sku_id;
  // 7b. 🆕 0080: ロジザードのバーコード (同じ取引 = 登録だけ入って値が消えることは無い・ほかの商品と同じ値 = 409 で登録ごと戻る)。押し直し (replay) はここに来ない
  if (req.lzBarcode != null) {
    await setLzBarcodeInTx(db, { requestId: req.requestId, actor: req.actor, skuId: out.sku_id, kind: req.lzBarcode.kind, value: req.lzBarcode.value, via: 'register', seenRow: null, copy: req.lzCopy });
  }
  // 7c. 🆕 0084: メーカー (同じ取引 = 登録だけ入ってメーカーが消えることは無い)。押し直し (replay) はここに来ない (前の答え)
  if (v.maker != null) {
    const mk = await setProductMakerInTx(db, { requestId: req.requestId, actor: req.actor, skuId: out.sku_id, makerId: v.maker, seen: null, via: 'register', reason: dbReasonOf(req) });
    out.maker = { id: mk.maker_id, name: mk.maker_name };
  }
  // 7c'. 🆕 0087: 梱包の大きさ・重さの人の値 (同じ取引 = 登録だけ入って値が消えることは無い)。押し直し (replay) はここに来ない (前の答え)
  if (req.pkg != null) {
    await setProductPackageInTx(db, { requestId: req.requestId, actor: req.actor, skuId: out.sku_id, value: req.pkg, seen: null, via: 'register', reason: dbReasonOf(req) });
  }
  // 7d. 🆕 0088: ブランド・区分・終売の予定日 (同じ取引・押し直しはここに来ない)
  if (req.kind === 'single') Object.assign(out, await applyBrandLifeRegister(db, { requestId: req.requestId, actor: req.actor, skuId: out.sku_id, v, via: 'register', reason: dbReasonOf(req) }));
  // 7d. 🆕 0090: 表示の項目の人の値 (同じ取引 = 登録だけ入って値が消えることは無い)。押し直し (replay) はここに来ない (前の答え)
  if (req.lbl != null) {
    await setProductLabelInTx(db, { requestId: req.requestId, actor: req.actor, skuId: out.sku_id, value: req.lbl, seen: null, via: 'register', reason: dbReasonOf(req) });
  }
  // 8. 🆕 0075: 発注の設定の知らせ (同じ取引・DB が「この取引で書いた同じ人の登録の SKU」だけを受ける)。0075 の前の DB = 積まない (呼び手が今までどおり)
  //    🆕 Codex #1703 R2 M: 発注の設定が無い単品の登録も「知らせの道で登録した」印だけ積む (押し直しで 0075 の前の登録と見分ける)。セット = 発注の設定が無い = 積まない
  if (ctx.orderOutbox && req.kind === 'single') {
    try {
      await enqueueOrderSettingsInTx(db, { registrationRequestId: req.requestId, actor: req.actor, items: req.order ? [{ code: req.code }] : [], base: req.order });
    } catch (e) {
      const m = /^([a-z_]+):\s*([\s\S]*)$/.exec(String((e && e.message) || ''));
      if (m && m[1] === 'invalid_input') throw bad(`発注の設定の知らせを積めません (DB: ${m[2]})。何も保存していません`, 'order_settings');
      throw e;
    }
  }
  return { result: out };
}

/**
 * ops.register_new_sku (0052) を呼ぶ。DB が段階・持ち主表で断った = 409 before_cutover / backfill が無い = 409 backfill_missing /
 * コードの決まり = 409 (code_taken など) / 構成品が使えない = 400。それ以外は投げ直す (500)
 */
async function callRegister(db, req, ctx, entry) {
  try {
    return (await db.query('select ops.register_new_sku($1::uuid, $2, $3, $4::jsonb, $5, $6::jsonb) as r',
      [req.requestId, req.actor, dbReasonOf(req), JSON.stringify(ctx.ownership), ctx.payloadHash, JSON.stringify(entry)])).rows[0].r;
  } catch (e) {
    const msg = String((e && e.message) || '');
    const m = /^([a-z_]+):\s*(.*)$/s.exec(msg);
    const key = m ? m[1] : '';
    const closed = newEntryClosedFromDb(e);   // 広げる道 PR-2: DB の新規開始の強制 (PR-1 の 0058) = 409 の分かる文
    if (closed) throw closed;
    if (key === 'before_cutover') {
      throw new MasterWriteError(409, 'before_cutover', `切替前です: 新商品はまだ NE・product-hub で登録します (DB が断った: ${m[2]})。何も保存していません`, { phase: null, open: ctx.open });
    }
    if (key === 'backfill_missing') {
      throw new MasterWriteError(409, 'backfill_missing', '切替の手順の「既存の商品の登録の状態 (backfill)」が済んでいないので、新商品はまだ登録できません (DB が断った)。何も保存していません');
    }
    if (CODE_PROBLEM_MESSAGES[key]) {
      throw new MasterWriteError(409, key, `${CODE_PROBLEM_MESSAGES[key](req.code)}。何も保存していません`, { field: 'code' });
    }
    if (key === 'component_unusable') {
      throw bad(`構成品に使えない商品があります (DB が断った: ${m[2].replace(/^構成品に使えない商品がある: /, '')})。何も保存していません`, 'components');
    }
    if (key === 'set_underivable') throw bad(`このセットは導く値が決まらないので登録しません (DB: ${m[2]})`, 'set', 'set_underivable');
    if (key === 'derived_mismatch') {
      throw new MasterWriteError(409, 'derived_mismatch', `構成品の値がちょうど変わりました (DB が断った: ${m[2]})。何も保存していません。画面を開き直してください`);
    }
    if (key === 'invalid_value' || key === 'invalid_input') throw bad(`登録の値が DB の決まりに合いません (${m[2]})。何も保存していません`, 'values');
    throw e;
  }
}

/** 保存を開いているか (段階 new_open・持ち主表のハッシュが段階の記録と同じ・列の持ち主が company・MASTER_EDIT_OPEN・backfill 済み)。段階の共有の鍵を持った取引の中で */
async function assertNewEntryOpen(db, req, ctx) {
  // 1 層目 = 土台の門 (段階・DB の active・code_behind・段階の記録 = active・MASTER_EDIT_OPEN。広げる道 PR-2 = 持ち主表は DB の active)
  const gate = await baseGateInTx(db, { open: ctx.open, closedWhy: '新商品の登録はまだ開いていません' });
  const phase = gate.phase;
  const refuse = (why, loadKeys = []) => new MasterWriteError(409, 'before_cutover', `切替前です: 新商品はまだ NE・product-hub で登録します (${why})。何も保存していません`,
    { load_keys: loadKeys, open: ctx.open, phase: phase.readable ? phase.phase : null });
  if (!gate.ok) throw ownerGateError(gate, (why) => refuse(why));
  ctx.ownership = gate.ownership;
  const loadKeys = NEW_ENTRY_KEYS[req.kind].filter((k) => ctx.ownership[k] !== 'company');
  if (loadKeys.length) throw refuse('Company DB ではまだ登録できない項目があります', loadKeys);
  // 2 層目 = 新規開始の門 (DB の開放の許可 (lease) が今有効・非常の止め MASTER_NEW_ENTRY_STOP が無い。設計 v11 §3.7)
  const ne = newEntryGateInTx(db, req.kind, { env: ctx.env, pre: ctx.nePre });
  if (!ne.ok) throw ownerGateError({ ...ne, phase });
  // 既存の商品の登録の状態 (切替の日の backfill) が済んでいない = 登録しない (PR #1566 R1 H1。段階の門でも new_open の前提にしている = 保険)
  const bf = (await db.query('select count(*)::int as n from ops.master_registration_backfill')).rows[0].n;
  if (bf !== 1) {
    throw new MasterWriteError(409, 'backfill_missing', '切替の手順の「既存の商品の登録の状態 (backfill)」が済んでいないので、新商品はまだ登録できません。何も保存していません',
      { phase: phase.phase });
  }
}

/** 新しいセットの構成品: Company DB にある・単品・やめた / 要確認 (quarantined) の商品でない。SKU ごとの鍵 (sku_id の順) を取ってから値を読む */
async function resolveNewComponents(db, list, today) {
  if (!Array.isArray(list) || list.length < 1 || list.length > MAX_COMPONENTS) throw bad(`構成品は 1〜${MAX_COMPONENTS} 行です`, 'components');
  const found = (await db.query(`select k.sku_id::text as sku_id, k.code_norm from core.skus k
     where k.company_id = $1 and k.code_norm = any(select core.norm_code(x) from unnest($2::text[]) as t(x))`, [COMPANY_ID, list.map((r) => r.code)])).rows;
  const byNorm = new Map(found.map((f) => [f.code_norm, f.sku_id]));
  const ids = [];
  for (const r of list) {
    const id = byNorm.get(normSku(r.code));
    if (!id) throw bad(`構成品 ${r.code} は Company DB にありません`, 'components');
    ids.push(id);
  }
  const sorted = [...new Set(ids)].sort((a, b) => (BigInt(a) < BigInt(b) ? -1 : BigInt(a) > BigInt(b) ? 1 : 0));
  for (const id of sorted) await db.query(SKU_LOCK_SQL, [id]);
  await db.query('select sku_id from core.skus where sku_id = any($1::bigint[]) order by sku_id for share', [sorted]);
  const facts = await componentFacts(db, sorted, today);
  const regs = (await regclass(db, 'ops.master_registrations'))
    ? new Map((await db.query('select sku_id::text as id, state from ops.master_registrations where sku_id = any($1::bigint[])', [sorted])).rows.map((x) => [x.id, x.state]))
    : new Map();
  return list.map((r, i) => {
    const id = ids[i];
    const k = facts.get(id);
    if (k.sku_kind !== 'single') throw bad(`${k.code} は${k.sku_kind === 'set' ? 'セット' : '例外の SKU'}なので構成品にできません (セットの入れ子は不可)`, 'components');
    const st = regs.get(id);
    if (st === 'cancelled' || st === 'quarantined') throw bad(`${k.code} は${st === 'cancelled' ? '登録をやめた' : '要確認 (NE で見つけた知らない商品)'}なので構成品にできません`, 'components');
    return { ...k, child_sku_id: id, qty: r.qty, sort: i + 1 };
  });
}
