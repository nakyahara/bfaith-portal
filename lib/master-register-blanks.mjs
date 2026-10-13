/**
 * master-register-blanks.mjs — 新商品の登録の「入力なし」(中原さん 2026-10-13)
 *
 * 中原さん「新規商品の登録時に各項目を全部させたい。必須項目は必須なんだけど、そうじゃないところは入力なしのチェックボックスをつけるなどして
 *   全部そろわないと入力できないような仕様にしてほしい」
 *
 * 決まり (画面 = public/me-new.js・me-variation.js・me-none.js と同じ):
 *   - 必須の項目 (コード・名前・売価・税率・売上分類・代表の仕入先・推奨保有月数・有効期限の管理・セットの構成) は今までどおり (入力なしは無い)
 *   - 必須でない項目は、どれも「値を入れる」か「入力なし」のどちらか (両方は持たない)。どちらでもない = 登録しない (400 blank_unmarked・どの項目か)
 *   - 出品カードを作らない (単品・セットの「作らない」) = カードの欄 (Yahoo! も) は数えない (「作らない」と決めた = 欄ごとに聞かない)
 *   - 発注の設定 = 発注アプリに書ける人 (名簿 + 発注アプリの権限) で、発注アプリの設定を読めるときだけ数える (画面に欄が出るときだけ)
 *   - 入力なしにした項目は、登録の理由に「· 入力なし: 原価・発送方法…」と足して残す (今ある記録の場所 = 登録の約束・状態の記録・変更の記録の理由。
 *     Company DB の migration は増やさない)。理由と合わせて 200 字 (REASON_MAX) を超える = 400 (理由を短く)
 *   - 🆕 #1706 Codex R2 M: 発注の設定の 6 つの「入力なし」は、商品の要求のハッシュ (= 理由) に入れない。記録は理由の後ろの
 *     「· 発注の設定の入力なし: …」(reasonNote = DB に書く理由にだけ足す・ハッシュに入らない)。確かめるのは、その request_id の記録がまだ無いとき
 *     (= 初回)。押し直し (記録がある) は、そのときの発注アプリの読める / 読めないに左右されない (#1703 の押し直しの決まり = 発注の設定の照らし合わせ を壊さない)
 *   - 古い画面 (印 blanks を送らない) = 全部の項目に値があれば受ける・無い項目があれば断る (400・「画面を開き直してください」)
 *   - 🚨 入力なしは「登録の時点で なし と決めた」だけ。後の門は変えない (NE 登録の CSV は原価が要る・出品カードに要る物は今までどおり)
 * 確かめるのは API の入口 (router の /api/new・/api/new/variation)。lib の registerNewSku / registerVariationBatch はこの印を知らない (試験・ほかの呼び手はそのまま)
 */
import { MasterWriteError, REASON_MAX, blank } from './master-write.mjs';

const bad = (message, field, reason = 'invalid_input', extra = {}) => new MasterWriteError(400, reason, message, { field, ...extra });
const obj = (x) => (x && typeof x === 'object' && !Array.isArray(x) ? x : {});
const values = (b) => obj(b.values);
const card = (b) => obj(b.card);
const yahoo = (b) => obj(card(b).yahoo);
/** 発注の設定の本文 (画面の order_settings)。送らない = 空 */
const order = (b) => obj(b.order_settings);
const costHas = (x) => !!x && typeof x === 'object' && !Array.isArray(x) && !blank(x.jpy);
const present = (x) => !blank(x);
/** まとまりの子 (配列でない = 無い) */
const kidsOf = (b) => (Array.isArray(b.children) ? b.children.filter((k) => k && typeof k === 'object' && !Array.isArray(k)) : []);
/** まとまりの子の実際の原価があるか (子だけの原価 か 共通の原価) */
const kidCostHas = (b, k) => present(k.cost) || costHas(values(b).cost);

/**
 * 「入力なし」にできる項目。key = 画面の data-none と本文の blanks の名前 / field = 400 のときに画面が飛ぶ欄 (data-row) /
 * short = 理由に残す名前 / group = card (カードを作らないときは数えない) / order (発注の設定の欄が出るときだけ)。並び = 画面の上からの順
 */
const ITEM = {
  cost: { label: '原価', short: '原価', field: 'cost', has: (b) => costHas(values(b).cost),
    // まとまり (#1706 Codex R1 M1): 全部の子に実際の原価 (子だけの原価 か 共通の原価) がある = 値がある。原価の無い子が 1 つでもある = 入力なしが要る (記録は原価の無い子だけ)
    hasVariation: (b) => (kidsOf(b).length ? kidsOf(b).every((k) => kidCostHas(b, k)) : costHas(values(b).cost)), kidBlank: (b, k) => !kidCostHas(b, k) },
  exception_cost: { label: '例外原価', short: '例外原価', field: 'exception_cost', has: (b) => costHas(values(b).exception_cost) },
  'order_settings.order_lot': { label: '発注ロット', short: '発注ロット', field: 'order_settings.order_lot', group: 'order', has: (b) => present(order(b).order_lot) },
  'order_settings.condition': { label: '発注条件グループ', short: '発注条件グループ', field: 'order_settings.condition_id', group: 'order',
    has: (b) => present(order(b).condition_id) || order(b).new_condition != null },
  'order_settings.material': { label: '原料グループ', short: '原料グループ', field: 'order_settings.material_group_id', group: 'order',
    has: (b) => present(order(b).material_group_id) || order(b).new_material != null },
  'order_settings.case_lot': { label: 'ケースロット', short: 'ケースロット', field: 'order_settings.case_lot', group: 'order', has: (b) => present(order(b).case_lot) },
  'order_settings.case_group': { label: 'ケースグループ', short: 'ケースグループ', field: 'order_settings.case_group', group: 'order', has: (b) => present(order(b).case_group) },
  'order_settings.capacity_per_unit': { label: '容量 / 個', short: '容量', field: 'order_settings.capacity_per_unit', group: 'order', has: (b) => present(order(b).capacity_per_unit) },
  shipping_code: { label: '発送方法', short: '発送方法', field: 'shipping_code', has: (b) => present(values(b).shipping_code) },
  inbound_date_managed: { label: 'ロジザードの入荷日の管理', short: '入荷日の管理', field: 'inbound_date_managed', has: (b) => present(values(b).inbound_date_managed) },
  'card.official_url': { label: '公式ページ URL', short: '公式ページURL', field: 'card.official_url', group: 'card', has: (b) => present(card(b).official_url) },
  'card.amazon': { label: 'Amazon URL か ASIN', short: 'Amazon', field: 'card.amazon_url', group: 'card', has: (b) => present(card(b).amazon_url) || present(card(b).asin) },
  'card.reference_urls': { label: '参考 URL', short: '参考URL', field: 'card.reference_urls', group: 'card',
    has: (b) => Array.isArray(card(b).reference_urls) && card(b).reference_urls.some(present) },
  'card.set_decision': { label: 'セット商品を作るか', short: 'セット商品', field: 'card.set_decision', group: 'card', has: (b) => present(obj(card(b).set_decision).decision) },
  'card.yahoo.price': { label: 'Yahoo!売価', short: 'Yahoo!売価', field: 'card.yahoo.price', group: 'card', has: (b) => present(yahoo(b).price) },
  'card.yahoo.price_sagawa': { label: 'Yahoo!売価 (佐川)', short: 'Yahoo!売価(佐川)', field: 'card.yahoo.price_sagawa', group: 'card', has: (b) => present(yahoo(b).price_sagawa) },
  'card.yahoo.delivery_label': { label: 'Yahoo!の配送方法', short: 'Yahoo!配送方法', field: 'card.yahoo.delivery_label', group: 'card', has: (b) => present(yahoo(b).delivery_label) },
  'card.yahoo.category_id': { label: 'Yahoo!カテゴリID', short: 'Yahoo!カテゴリID', field: 'card.yahoo.category_id', group: 'card', has: (b) => present(yahoo(b).category_id) },
  'card.yahoo.path': { label: 'Yahoo!path', short: 'Yahoo!path', field: 'card.yahoo.path', group: 'card', has: (b) => present(yahoo(b).path) },
  // まとまり: JAN は子ごと。「JAN の無い子を JAN なしで登録する」を 1 つの項目にする (全部の子に JAN = 値がある)
  'children.jan': { label: 'JAN (JAN の無い子)', short: 'JAN', field: 'children.jan',
    has: (b) => kidsOf(b).length > 0 && kidsOf(b).every((k) => present(k.jan)), kidBlank: (b, k) => !present(k.jan) },
};
const ORDER_KEYS = ['order_settings.order_lot', 'order_settings.condition', 'order_settings.material', 'order_settings.case_lot', 'order_settings.case_group', 'order_settings.capacity_per_unit'];
const YAHOO_KEYS = ['card.yahoo.price', 'card.yahoo.price_sagawa', 'card.yahoo.delivery_label', 'card.yahoo.category_id', 'card.yahoo.path'];
/** 種類ごとの項目 (画面の上からの順) */
export const BLANK_KEYS = Object.freeze({
  single: Object.freeze(['cost', ...ORDER_KEYS, 'shipping_code', 'inbound_date_managed', 'card.official_url', 'card.amazon', 'card.reference_urls', 'card.set_decision', ...YAHOO_KEYS]),
  set: Object.freeze(['exception_cost', 'shipping_code', 'card.official_url', 'card.amazon', 'card.reference_urls', ...YAHOO_KEYS]),
  variation: Object.freeze(['cost', 'shipping_code', 'inbound_date_managed', ...ORDER_KEYS, 'card.official_url', 'card.amazon', 'children.jan']),
});
/** 画面に渡す名前 (key → { label, short }) */
export const BLANK_LABELS = Object.freeze(Object.fromEntries(Object.entries(ITEM).map(([k, v]) => [k, Object.freeze({ label: v.label, short: v.short })])));
/** その種類の項目の名前だけ (画面に渡す = ほかの種類の名前を出さない) */
export const blankLabelsOf = (kind) => Object.fromEntries((BLANK_KEYS[kind] || []).map((k) => [k, BLANK_LABELS[k]]));
/** 種類ごとの「値があるか」(まとまりの原価は子ごとの実際の原価) */
const hasOf = (kind, k, b) => (kind === 'variation' && ITEM[k].hasVariation ? ITEM[k].hasVariation(b) : ITEM[k].has(b));
const DEFAULT_REASON = Object.freeze({ single: '新商品の登録', set: '新商品の登録', variation: '新商品の登録 (色違い・サイズ違い)' });
export const MAX_BLANKS = 64;
/** 種類の項目 (知らない種類 = null。'constructor' などの名前を拾わない) */
const keysOf = (kind) => (typeof kind === 'string' && Object.prototype.hasOwnProperty.call(BLANK_KEYS, kind) ? BLANK_KEYS[kind] : null);

/** カードを作るか (lib/master-register.mjs parseCard と同じ読み方。まとまりはいつも作る) */
function cardCreated(kind, body) {
  if (kind === 'variation') return true;
  const v = card(body).create;
  return !(v === false || v === 0 || v === '0' || v === 'false');
}

/** この登録で数える項目 (カードを作らない = カードの欄を外す・発注の設定を出さない = 発注の欄を外す) */
export function applicableBlankKeys(kind, body, { orderApplicable = false, only = null } = {}) {
  const b = obj(body);
  const cardOn = cardCreated(kind, b);
  const orderOn = only !== 'main' && (kind === 'single' || kind === 'variation') && (typeof orderApplicable === 'function' ? !!orderApplicable() : !!orderApplicable);
  return (keysOf(kind) || []).filter((k) => (ITEM[k].group !== 'card' || cardOn) && (ITEM[k].group !== 'order' || orderOn) && (only !== 'order' || ITEM[k].group === 'order'));
}
/** 発注の設定の項目か */
export const isOrderBlankKey = (k) => Object.prototype.hasOwnProperty.call(ITEM, k) && ITEM[k].group === 'order';

/** 値の無い項目 (試験と画面の確かめ用) */
export function missingBlankKeys(kind, body, opts = {}) {
  const b = obj(body);
  return applicableBlankKeys(kind, b, opts).filter((k) => !hasOf(kind, k, b));
}

/**
 * 印 (body.blanks) を確かめて、入力なしにした項目を画面の順で返す。どれでもない項目がある = 400 blank_unmarked (field = その欄)・
 * 値と入力なしの両方 = 400 blank_with_value・知らない名前 = 400 blank_unknown。数えない項目 (カードを作らない・発注の欄が出ない) の印は使わない
 */
export function checkRegisterBlanks(kind, body, opts = {}) {
  const b = obj(body);
  if (!keysOf(kind)) return [];
  const raw = b.blanks;
  const outdated = raw === undefined;
  // 送らない (undefined) = 古い画面。null・配列でない = 入れ方の誤り (#1706 Codex R1 L1: null を空の配列として受けない)
  if (!outdated && !Array.isArray(raw)) throw bad('「入力なし」の印の入れ方が違う。画面を開き直してください', 'blanks', 'blank_unknown');
  const list = outdated ? [] : raw;
  if (list.length > MAX_BLANKS) throw bad('「入力なし」の印が多すぎます。画面を開き直してください', 'blanks', 'blank_unknown');
  const marked = new Set();
  for (const k of list) {
    if (typeof k !== 'string' || !keysOf(kind).includes(k)) {
      throw bad(`「${String(k).slice(0, 60)}」は${kind === 'variation' ? 'まとまり' : kind === 'set' ? 'セット' : '単品'}の登録で「入力なし」にできる項目ではありません。画面を開き直してください`, 'blanks', 'blank_unknown');
    }
    marked.add(k);
  }
  const out = [];
  for (const k of applicableBlankKeys(kind, b, opts)) {   // opts.only = 'main' (発注の設定を除く) / 'order' (発注の設定だけ)
    const it = ITEM[k];
    const has = hasOf(kind, k, b);
    if (has && marked.has(k)) {
      throw bad(`「${it.label}」に値と「入力なし」の両方があります。どちらか一方にしてください。何も保存していません`, it.field, 'blank_with_value', { blank_key: k });
    }
    if (!has && !marked.has(k)) {
      throw bad(`「${it.label}」は、値を入れるか「入力なし」を選んでください${outdated ? ' (前の画面には「入力なし」がありません。画面を開き直してください)' : ''}。何も保存していません`,
        it.field, 'blank_unmarked', { blank_key: k, ...(outdated ? { screen_outdated: true } : {}) });
    }
    if (!has) out.push(k);
  }
  return out;
}

/**
 * 登録の理由 (記録に残る) に入力なしの一覧を足す。入力なしが無い = 理由はそのまま (要求のハッシュも前と同じ)。
 * 理由が文字でない = そのまま (lib が断る)。200 字を超える = 400 reason_too_long
 */
export function blankReasonOf(kind, reason, blanks0) {
  const blanks = blanks0.filter((k) => !isOrderBlankKey(k));   // 発注の設定は理由 (要求のハッシュ) に入れない (#1706 Codex R2 M)
  if (!blanks.length) return reason;
  if (reason != null && typeof reason !== 'string') return reason;
  const base = reason && reason.trim() ? reason.trim() : DEFAULT_REASON[kind];
  const tail = ` · 入力なし: ${blanks.map((k) => ITEM[k].short).join('・')}`;
  const s = base + tail;
  const over = [...s].length - REASON_MAX;
  if (over > 0) {
    throw bad(`理由が長すぎます (「入力なし」の項目の一覧と合わせて ${REASON_MAX} 字まで)。理由をあと ${over} 字短くしてください。何も保存していません`, 'reason', 'reason_too_long');
  }
  return s;
}

/**
 * 🆕 #1706 Codex R2 M: 発注の設定の 6 つの「入力なし」を確かめて、DB に書く理由にだけ足す一言 (reasonNote) を返す。要求のハッシュには入らない。
 *   呼び手 (router) は、その request_id の記録がまだ無いとき (初回) だけ呼ぶ = 押し直しは発注アプリの読める / 読めないに左右されない。
 *   reason = 入力なしを足した後の理由 (null = 種類の決まりの文)。戻り値 { ok: true, blanks, note (無い = null) } / { ok: false, status, json }
 */
export function prepareOrderBlanks(kind, body, { orderApplicable = false, reason = null } = {}) {
  const b = obj(body);
  try {
    const blanks = checkRegisterBlanks(kind, b, { orderApplicable, only: 'order' });
    if (!blanks.length) return { ok: true, blanks, note: null };
    const note = ` · 発注の設定の入力なし: ${blanks.map((k) => ITEM[k].short).join('・')}`;
    const base = typeof reason === 'string' && reason.trim() ? reason.trim() : DEFAULT_REASON[kind];
    const over = [...(base + note)].length - REASON_MAX;
    if (over > 0) {
      throw bad(`理由が長すぎます (「入力なし」の項目の一覧と合わせて ${REASON_MAX} 字まで)。理由をあと ${over} 字短くしてください。何も保存していません`, 'reason', 'reason_too_long');
    }
    return { ok: true, blanks, note };
  } catch (e) {
    if (e instanceof MasterWriteError) return { ok: false, status: e.status, json: { ok: false, error: e.message, reason: e.reason, ...e.extra } };
    throw e;
  }
}

/**
 * まとまりの子ごとの理由 (#1706 Codex R1 M1): 子ごとに入力なしの項目が違う (原価・JAN は その子に無いときだけ)。
 *   入力なしが無い登録 = null (前と同じ = まとめての理由)。{ 子のコード: 理由 } (子の項目が無い子 = 入力なしの無い理由)
 */
export function childBlankReasons(body, blanks) {
  const b = obj(body);
  if (!blanks.length) return null;
  const base = typeof b.reason === 'string' && b.reason.trim() ? b.reason.trim() : DEFAULT_REASON.variation;
  const out = {};
  for (const k of kidsOf(b)) {
    if (typeof k.code !== 'string') continue;
    const mine = blanks.filter((key) => !isOrderBlankKey(key) && (!ITEM[key].kidBlank || ITEM[key].kidBlank(b, k)));
    out[k.code] = mine.length ? blankReasonOf('variation', base, mine) : base;
  }
  return out;
}

/**
 * API の入口 (router) で使う: 確かめて、理由に入力なしの一覧を足した本文を返す。まとまりは子ごとの理由 (childReasons) も
 * 戻り値 { ok: true, body, blanks, childReasons } / { ok: false, status, json } (json = 画面の誤りの形 { ok: false, error, reason, field, ... })
 */
export function prepareRegisterBody(kind, body, opts = {}) {
  const b = obj(body);
  try {
    // 🆕 #1706 Codex R2 M: ここでは発注の設定の項目を数えない (prepareOrderBlanks = 初回だけ・ハッシュに入らない)
    const blanks = checkRegisterBlanks(kind, b, { ...opts, only: 'main' });
    const childReasons = kind === 'variation' ? childBlankReasons(b, blanks) : null;
    return { ok: true, blanks, childReasons, body: blanks.length ? { ...b, reason: blankReasonOf(kind, b.reason ?? null, blanks) } : b };
  } catch (e) {
    if (e instanceof MasterWriteError) return { ok: false, status: e.status, json: { ok: false, error: e.message, reason: e.reason, ...e.extra } };
    throw e;
  }
}
