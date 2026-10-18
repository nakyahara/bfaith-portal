/**
 * master-label.mjs — 商品 (単品) の表示の項目 = 原産国・内容量・メーカー型番・消費期限の目安 (日数)
 * (Company DB構想 27 ④-1 / ④-2・§3.4 = v2・中原さんの決定 Q1〜Q9 = 全部 a・2026-10-18・Company DB の 0099 仮番号)
 *
 * 正本 = Company DB・入力 = マスタの入力 (商品の画面・新商品の登録)。product-hub は読んで見せるだけ (差の印・人が「マスタの値を入れる」を押す)。
 *   - 人の値 = core.product_label_facts (商品ごとに 1 行・項目ごとに空でよい)。書くのは DB の関数 ops.set_product_label_facts だけ
 *     (新商品の登録・まとまりの登録・商品の画面の保存の取引の中。守りは梱包の大きさ・重さ (0086)・メーカー (0084) と同じ)
 *   - いま使う値 = core.v_product_label_facts (項目ごとに 人 → 出どころ。内容量だけ出どころ = 夜間ロードが解決した core.products.net_content)
 *   - product-hub の今の値 (同じ商品コードの下書きのページ表記・楽天の型番) = 画面が product-hub の SQLite を直に読んで「参考」に出す (読むだけ・観測にはしない)
 *   🚨 観測 + 規則の版 v2 にしない理由: 夜間ロードは規則の版 v1 に固定 (engine.mjs の RULE_VERSION) で、触った商品の内容量を毎晩 v1 で選び直す =
 *      人の観測を足しても次の夜に product-hub の値に戻る。v1 に規則は足せない (0003 の trg_rules_used_version)。v2 に切り替えると全部の属性の解決が変わる
 *      (梱包の大きさ・重さ 0086 と同じ判断)。product-hub の原産国・型番を観測にもしない (product-hub で消した値が Company DB に残る・Codex 設計 R1 High 1)
 * 単品だけ (セット・例外は商品の行を持たない = 構成品から)。登録で必須にしない (「値か入力なし」lib/master-register-blanks.mjs に入れない)。NE には送らない。
 * 0099 の前の DB・ロールの流し直しの前 (master_edit に実行・読む権限が無い) = labelReady が false = 画面は欄を出さない・保存に値が来たら 503 db_not_ready
 * 🚨 product-hub (apps/product-hub/services/cdb-label-compare.mjs) も比べ方 (countryNorm・netEqual・phCountryOf…) をここから使う = 2 つの画面で同じ決まり
 */
import { MasterWriteError } from './master-write.mjs';
import { parseContent } from '../apps/company-db/load/sources.mjs';

/** 項目 (DB の列) と画面の言葉。並び = 原産国・内容量・メーカー型番・消費期限の目安 */
export const LABEL_ITEMS = Object.freeze([
  Object.freeze({ key: 'country', col: 'country_of_origin', field: 'lbl_country', label: '原産国' }),
  Object.freeze({ key: 'net', col: 'net_content', field: 'lbl_net', label: '内容量' }),
  Object.freeze({ key: 'model', col: 'model_number', field: 'lbl_model', label: 'メーカー型番' }),
  Object.freeze({ key: 'shelf', col: 'shelf_life_days', field: 'lbl_shelf', label: '消費期限の目安' }),
]);
/** 商品の画面の保存の欄 (values のキー)。内容量は数 (lbl_net) と単位 (lbl_net_uom) の 2 つの欄で 1 つの項目 */
export const LABEL_FIELDS = Object.freeze(['lbl_country', 'lbl_net', 'lbl_net_uom', 'lbl_model', 'lbl_shelf']);
/** 欄 → DB の列 (人の値の形 = readHumanLabel の human のキー) */
export const COL_OF_FIELD = Object.freeze({ lbl_country: 'country_of_origin', lbl_net: 'net_content', lbl_net_uom: 'net_content_uom', lbl_model: 'model_number', lbl_shelf: 'shelf_life_days' });
/** 内容量の単位 (DB の check と同じ)。夜間ロードの product-hub の内容量 (parseContent) はもっと広い (L・kg・本…) = ちょうど直せるときだけ入れられる */
export const NET_UOMS = Object.freeze(['ml', 'g', '枚', '個']);
/** 範囲 (DB の check と同じ) */
export const COUNTRY_MAX = 40;
export const MODEL_MAX = 60;
export const SHELF_MAX = 3650;
/** よく使う原産国 (画面の候補だけ・ほかの国名も入れられる) */
export const COUNTRY_SUGGEST = Object.freeze(['日本', '中国', '韓国', '台湾', 'ベトナム', 'タイ', 'インドネシア', 'インド', 'アメリカ', 'イタリア', 'フランス', 'ドイツ']);
export const LABEL_FUNCTION = 'ops.set_product_label_facts(uuid, text, bigint, text, numeric, text, text, integer, bigint, text, text)';

/** 出どころの言葉 (画面) */
export function sourceWords(src) {
  return { human: '人が入れた値', product_hub: '夜間の取り込み (product-hub)', notion_import: '夜間の取り込み (Notion)', amazon_catalog: '夜間の取り込み (Amazon のカタログ)', nightly_load: '夜間の取り込み' }[src] || String(src || '');
}

// ─── 原産国の形 (マスタの入力・product-hub で同じ決まり) ───
/** よくある言い方 → 1 つの書き方 (NFKC・小文字にした後で引く) */
const COUNTRY_ALIASES = Object.freeze({
  '日本国': '日本', japan: '日本', jp: '日本', '中華人民共和国': '中国', china: '中国', prc: '中国', '大韓民国': '韓国', korea: '韓国', 'south korea': '韓国',
  'アメリカ合衆国': 'アメリカ', '米国': 'アメリカ', usa: 'アメリカ', us: 'アメリカ', 'united states': 'アメリカ', taiwan: '台湾', vietnam: 'ベトナム', 'viet nam': 'ベトナム',
  thailand: 'タイ', indonesia: 'インドネシア', india: 'インド', italy: 'イタリア', france: 'フランス', germany: 'ドイツ', 'ドイツ連邦共和国': 'ドイツ', 'フランス共和国': 'フランス', 'イタリア共和国': 'イタリア',
});
/**
 * 原産国の比べる形 (保存もこの形): NFKC → 前後と続きの空白 → 「made in」と末尾の「製」を外す → よくある言い方をそろえる。空 = ''
 *   例: 「中国製」→ 中国・「日本国」→ 日本・「Made in Japan」→ 日本
 */
export function countryNorm(v) {
  let s = String(v ?? '').normalize('NFKC').replace(/\s+/g, ' ').trim();
  s = s.replace(/^made in\s+/i, '').replace(/製$/, '').trim();
  const a = COUNTRY_ALIASES[s.toLowerCase()];
  return a || s;
}

// ─── 入力の形 ───
const toHalfNum = (s) => String(s).replace(/[０-９．，]/g, (c) => String.fromCharCode(c.charCodeAt(0) - 0xFEE0));
const CTRL = /[\u0000-\u001f\u007f]/;
const bad = (message, field) => new MasterWriteError(400, 'invalid_input', message, { field });
const blankOf = (v) => v == null || (typeof v === 'string' && v.trim() === '');

/** 原産国 (国名の文字・countryNorm の形にして持つ)。空 = null。1〜40 字・改行なし */
export function countryIn(v, field = 'lbl_country') {
  if (blankOf(v)) return null;
  if (typeof v !== 'string') throw bad('原産国は国名の文字で入れてください (例: 日本・中国)', field);
  if (CTRL.test(v)) throw bad('原産国に改行は入れられません', field);
  const s = countryNorm(v);
  if (!s) throw bad('原産国は国名で入れてください (「製」だけは入れられません)', field);
  if (s.length > COUNTRY_MAX) throw bad(`原産国は ${COUNTRY_MAX} 字までの国名で入れてください`, field);
  return s;
}
/** メーカー型番。前後の空白を外す。空 = null。1〜60 字・改行なし */
export function modelIn(v, field = 'lbl_model') {
  if (blankOf(v)) return null;
  if (typeof v !== 'string' && typeof v !== 'number') throw bad('メーカー型番は文字で入れてください', field);
  const s = String(v).trim();
  if (CTRL.test(s) || s.length > MODEL_MAX) throw bad(`メーカー型番は ${MODEL_MAX} 字までで入れてください (改行なし)`, field);
  return s;
}
/** 消費期限の目安 (製造 (入荷) からの日数)。空 = null。1〜3650 の整数 */
export function shelfIn(v, field = 'lbl_shelf') {
  if (blankOf(v)) return null;
  const s = toHalfNum(String(v)).replace(/[,\s]/g, '').replace(/日$/, '');
  if (!/^\d{1,4}$/.test(s) || Number(s) < 1 || Number(s) > SHELF_MAX) throw bad(`消費期限の目安は 1〜${SHELF_MAX} の日数 (整数) で入れてください`, field);
  return Number(s);
}
/** 数の文字の余分な 0 を外す (50.00 → 50・1.50 → 1.5・007 → 7) */
export const numText = (n) => (n == null || n === '' ? '' : String(n).replace(/^0+(?=\d)/, '').replace(/(\.\d*?)0+$/, '$1').replace(/\.$/, ''));
/** 内容量の数 (小数 2 けたまで・0 より大きい)。空 = null。DB には文字のまま渡す (numeric = 小数の誤差を作らない) */
export function netNumIn(v, field = 'lbl_net') {
  if (blankOf(v)) return null;
  const s = toHalfNum(String(v)).replace(/[,\s]/g, '');
  if (!/^\d{1,8}(\.\d{1,2})?$/.test(s) || !(Number(s) > 0)) throw bad('内容量は 0 より大きい数 (小数は 2 けたまで) で入れてください (例: 50・1.5)', field);
  return numText(s);
}
/** 内容量の単位。空 = null。ml / g / 枚 / 個 だけ */
export function netUomIn(v, field = 'lbl_net_uom') {
  if (blankOf(v)) return null;
  const s = String(v).normalize('NFKC').trim().toLowerCase();
  if (!NET_UOMS.includes(s)) throw bad(`内容量の単位は ${NET_UOMS.join(' / ')} から選んでください`, field);
  return s;
}
/** 商品の画面の 1 つの欄の値 → 決まった形 (lbl_net = 数の文字・lbl_shelf = 整数・ほか = 文字) か null */
export function labelFieldIn(field, v) {
  if (field === 'lbl_country') return countryIn(v, field);
  if (field === 'lbl_net') return netNumIn(v, field);
  if (field === 'lbl_net_uom') return netUomIn(v, field);
  if (field === 'lbl_model') return modelIn(v, field);
  if (field === 'lbl_shelf') return shelfIn(v, field);
  throw bad(`知らない欄 ${field}`, field);
}
/** 内容量の数と単位がそろっているか (どちらかだけ = 400) */
export function netPairProblem(num, uom, field = 'lbl_net') {
  if ((num == null) !== (uom == null)) return bad(num == null ? '内容量の数を入れてください (単位だけは入れられません)' : '内容量の単位 (ml / g / 枚 / 個) を選んでください', field);
  return null;
}
/**
 * 新商品の登録・まとまりの登録の値 { country, net, net_uom, model, shelf } → { country_of_origin, net_content, net_content_uom, model_number, shelf_life_days } か null
 *   (送らない・全部空 = null = 人の値を入れない。知らないキー = 400)。noModel = まとまり (型番は子ごとに違う = 聞かない・送ってきたら 400)
 */
export function labelFactsIn(raw, { field = 'label', noModel = false } = {}) {
  if (raw == null) return null;
  if (typeof raw !== 'object' || Array.isArray(raw)) throw bad('表示の項目の形が違う。画面を開き直してください', field);
  const allowed = noModel ? ['country', 'net', 'net_uom', 'shelf'] : ['country', 'net', 'net_uom', 'model', 'shelf'];
  for (const k of Object.keys(raw)) {
    if (!allowed.includes(k)) throw bad(k === 'model' && noModel ? 'まとまりの登録ではメーカー型番を聞きません (子ごとに商品の画面で入れてください)' : `表示の項目に知らない項目 ${String(k).slice(0, 20)} があります`, field);
  }
  const out = {
    country_of_origin: countryIn(raw.country, field), net_content: netNumIn(raw.net, field), net_content_uom: netUomIn(raw.net_uom, field),
    model_number: noModel ? null : modelIn(raw.model, field), shelf_life_days: shelfIn(raw.shelf, field),
  };
  const pp = netPairProblem(out.net_content, out.net_content_uom, field);
  if (pp) throw pp;
  return Object.values(out).every((x) => x == null) ? null : out;
}

// ─── 内容量の比べ方・直し方 (丸めない) ───
/** 単位の書き方をそろえる (ML・mL → ml・KG → kg・L → l)。日本語の単位はそのまま */
const uomKey = (u) => { const s = String(u ?? '').normalize('NFKC').trim(); return /^[a-z]+$/i.test(s) ? s.toLowerCase() : s; };
/** 数の文字 → 100 倍の整数 (小数 2 けたまで・ちょうど表せないなら null) */
function cents(num) {
  const s = numText(String(num ?? ''));
  const m = /^(\d+)(?:\.(\d{1,2}))?$/.exec(s);
  if (!m) return null;
  return BigInt(m[1]) * 100n + BigInt((m[2] || '').padEnd(2, '0') || '0');
}
const fromCents = (c) => numText(`${c / 100n}.${String(c % 100n).padStart(2, '0')}`);
/**
 * 内容量 (数・単位) → マスタの単位 (ml / g / 枚 / 個) にちょうど直した { num, uom } か null (直せない = 入れられない・比べられない)。
 *   ml・g・枚・個 = そのまま / l → ml・kg → g = 1000 倍 / cc → ml / mg・本・包・粒・錠など = null。小数 2 けたを超える・0 以下 = null
 */
export function netToMaster(num, uom) {
  const u = uomKey(uom);
  const c = cents(num);
  if (c == null || c <= 0n) return null;
  if (NET_UOMS.includes(u)) return { num: fromCents(c), uom: u };
  if (u === 'cc') return { num: fromCents(c), uom: 'ml' };
  if (u === 'l') return { num: fromCents(c * 1000n), uom: 'ml' };
  if (u === 'kg') return { num: fromCents(c * 1000n), uom: 'g' };
  return null;
}
/** 内容量の文字 (product-hub の「50ml」など) → { num, uom, text, master: { num, uom } | null }。読めない = null (夜間ロードと同じ parseContent) */
export function netFromText(text) {
  const c = parseContent(text);
  if (!c) return null;
  return { num: numText(String(c.num)), uom: c.unit, text: String(text ?? '').trim(), master: netToMaster(String(c.num), c.unit) };
}
/** 2 つの内容量が同じか: true / false / null (比べられない = どちらかがマスタの単位に直せない) */
export function netEqual(a, b) {
  const x = a && netToMaster(a.num, a.uom);
  const y = b && netToMaster(b.num, b.uom);
  if (!x || !y) return null;
  return x.uom === y.uom && x.num === y.num;
}
/** 内容量の言葉 (50ml・1.5g・30枚) */
export const netText = (num, uom) => (num == null || num === '' ? '' : `${numText(num)}${uom || ''}`);

// ─── product-hub の原産国の書き方 (origin_type = 日本製 / 海外製・origin_country) ───
/**
 * product-hub のページ表記 → { value: 国名 (countryNorm) | null, state: 'value' | 'no_country' (海外製で国名が空) | 'empty' }
 *   日本製 = 日本 / 海外製 + 国名 = その国 / 未選択 + 国名 = その国 / 海外製 + 空 = no_country (比べられない)
 */
export function phCountryOf(pageInfo) {
  const t = String(pageInfo?.origin_type ?? '').trim();
  const c = countryNorm(pageInfo?.origin_country);
  if (t === '日本製') return { value: '日本', state: 'value' };
  if (c) return { value: c, state: 'value' };
  if (t === '海外製') return { value: null, state: 'no_country' };
  return { value: null, state: 'empty' };
}
/** マスタの原産国 → product-hub の 2 つの欄 (1 組で入れる): 日本 = 日本製 + 空 / ほか = 海外製 + 国名 */
export function phCountryFields(country) {
  const c = countryNorm(country);
  if (!c) return null;
  return c === '日本' ? { origin_type: '日本製', origin_country: '' } : { origin_type: '海外製', origin_country: c };
}

/** 1 行の言葉 (記録): 日本 · 50ml · 型番 ABC-1 · 365 日 */
export function labelText(v) {
  if (!v) return '';
  return [v.country_of_origin, netText(v.net_content, v.net_content_uom), v.model_number ? `型番 ${v.model_number}` : '', v.shelf_life_days != null ? `${v.shelf_life_days} 日` : ''].filter(Boolean).join(' · ');
}

// ─── DB ───
const LABEL_TABLES = ['core.product_label_facts', 'ops.product_label_fact_events', 'core.v_product_label_facts'];
/** 0099 があり、画面のロール master_edit が書く関数を実行でき、表・ビューを読めるか (ロールの流し直しの後) */
export async function labelReady(db) {
  return (await db.query(`select coalesce((select pg_catalog.has_function_privilege(r.oid, to_regprocedure($1), 'EXECUTE')
        and pg_catalog.has_table_privilege(r.oid, to_regclass('core.product_label_facts'), 'SELECT')
        and pg_catalog.has_table_privilege(r.oid, to_regclass('ops.product_label_fact_events'), 'SELECT')
        and pg_catalog.has_table_privilege(r.oid, to_regclass('core.v_product_label_facts'), 'SELECT')
      from pg_catalog.pg_roles r where r.rolname = 'master_edit' and to_regprocedure($1) is not null and to_regclass('core.v_product_label_facts') is not null), false) as ok`,
  [LABEL_FUNCTION])).rows[0]?.ok === true;
}
const tablesSeen = new WeakMap();
/** 表・ビューがあって今の接続のロールが読めるか (0099 の前・流し直しの前 = 「値なし」として読む = 画面・保存を止めない)。同じ接続では 1 回だけ聞く */
export async function labelTablesExist(db) {
  if (db && typeof db === 'object' && tablesSeen.has(db)) return tablesSeen.get(db);
  const ok = (await db.query(`select coalesce((select bool_and(pg_catalog.has_table_privilege(t.oid, 'SELECT')) from (select to_regclass($1) as oid
      union all select to_regclass($2) union all select to_regclass($3)) t where t.oid is not null having count(*) = 3), false) as ok`, LABEL_TABLES)).rows[0].ok === true;
  if (db && typeof db === 'object') tablesSeen.set(db, ok);
  return ok;
}

const isoOf = (v) => { if (v == null) return null; const d = v instanceof Date ? v : new Date(String(v).replace(' ', 'T').replace(/([+-]\d{2})$/, '$1:00')); return Number.isNaN(d.getTime()) ? null : d.toISOString(); };
const humanOf = (h) => (h ? {
  country_of_origin: h.country_of_origin ?? null, net_content: h.net_content == null ? null : numText(h.net_content), net_content_uom: h.net_content_uom ?? null,
  model_number: h.model_number ?? null, shelf_life_days: h.shelf_life_days == null ? null : Number(h.shelf_life_days),
  set_by: h.set_by, set_via: h.set_via, set_at: isoOf(h.set_at),
} : null);
/** 商品の人の値と最後の記録の番号 (保存の確かめに使う) = { human: {...} | null, event_id: 文字 | null } */
export async function readHumanLabel(db, productId) {
  if (productId == null || !(await labelTablesExist(db))) return { human: null, event_id: null };
  // 🚨 人の値と最後の記録の番号は 1 つの SELECT で読む (別々に読むと、間にほかの人の保存が入ったとき「古い値 + 新しい番号」になり、
  //    後の保存が競合に気づかずに上書きする・Codex 設計 R2 M)。人の値の行が無くても (全部外した後も) 番号は返す
  const r = (await db.query(`select h.product_id is not null as has, h.country_of_origin, h.net_content::text as net_content, h.net_content_uom, h.model_number, h.shelf_life_days,
        h.set_by, h.set_via, h.set_at, (select max(e.event_id) from ops.product_label_fact_events e where e.product_id = x.pid)::text as last_event
      from (select $1::bigint as pid) x left join core.product_label_facts h on h.product_id = x.pid`, [String(productId)])).rows[0];
  return { human: r && r.has ? humanOf(r) : null, event_id: r && r.last_event ? r.last_event : null };
}
/** ビューの 1 行 → { country_of_origin: { value, source, words } | null, net: { num, uom, text, source, words, master } | null, model_number, shelf_life_days } */
export function resolvedOf(v) {
  const one = (val, src) => (val == null || val === '' ? null : { value: val, source: src, words: sourceWords(src) });
  if (!v) return { country_of_origin: null, net: null, model_number: null, shelf_life_days: null };
  return {
    country_of_origin: one(v.country_of_origin, v.country_source),
    net: v.net_content == null ? null : { num: numText(v.net_content), uom: v.net_content_uom ?? '', text: netText(v.net_content, v.net_content_uom), source: v.net_source, words: sourceWords(v.net_source),
      master: netToMaster(v.net_content, v.net_content_uom) },
    model_number: one(v.model_number, v.model_source),
    shelf_life_days: v.shelf_life_days == null ? null : { value: Number(v.shelf_life_days), source: v.shelf_source, words: sourceWords(v.shelf_source) },
  };
}
export const LABEL_VIEW_SQL = `select product_id::text as product_id, country_of_origin, country_source, net_content::text as net_content, net_content_uom, net_source,
    model_number, model_source, shelf_life_days, shelf_source from core.v_product_label_facts`;
/** 商品の画面の「表示の項目」= { human, event_id, resolved } (いま使う値 = ビュー) */
export async function readLabelView(db, productId) {
  const empty = { human: null, event_id: null, resolved: resolvedOf(null) };
  if (productId == null || !(await labelTablesExist(db))) return empty;
  const { human, event_id } = await readHumanLabel(db, productId);
  const v = (await db.query(`${LABEL_VIEW_SQL} where product_id = $1`, [String(productId)])).rows[0] || null;
  return { human, event_id, resolved: resolvedOf(v) };
}

/**
 * product-hub の今の値 (参考・読むだけ) = { draftId, page: bool, country: { value, state, raw }, net: netFromText の答え | null (raw), model: { value, from } | null } か null
 *   探し方: 同じ商品コードの下書き (product_drafts.ne_code・除外を除く) のページ表記・楽天の型番 (SKU ごとの型番の行があればそれ)。
 *   無い = まとまりの子 = 子の SKU の型番の行 (draft_sku_attributes の name = メーカー型番) だけ (親のページの型番・原産国・内容量は子に出さない・Codex 設計 R1 Medium 3)
 *   phDb = product-hub の SQLite (better-sqlite3)。読めない = 投げる (呼び手が「読めない」にする)
 */
export function readPhLabelRefs(phDb, code) {
  const key = String(code ?? '').trim().toLowerCase();
  if (!key) return null;
  const has = (t) => !!phDb.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?").get(t);
  if (!has('product_drafts')) return null;
  const d = phDb.prepare("SELECT id FROM product_drafts WHERE LOWER(TRIM(ne_code)) = ? AND status <> 'excluded' ORDER BY id LIMIT 1").get(key);
  const skuModel = (draftId) => (has('draft_sku_attributes')
    ? phDb.prepare("SELECT draft_id, value FROM draft_sku_attributes WHERE sku_code = ? AND name = 'メーカー型番'" + (draftId == null ? ' ORDER BY draft_id LIMIT 1' : ' AND draft_id = ?')).get(...(draftId == null ? [key] : [key, draftId]))
    : null);
  if (!d) {
    const m = skuModel(null);
    if (!m) return null;
    return { draftId: m.draft_id, page: false, country: { value: null, state: 'empty', raw: '' }, net: null, netRaw: '', model: m.value ? { value: String(m.value).trim(), from: 'sku' } : null };
  }
  const pi = has('draft_page_info') ? phDb.prepare('SELECT origin_type, origin_country, content_volume FROM draft_page_info WHERE draft_id = ?').get(d.id) : null;
  const rk = has('draft_rakuten') ? phDb.prepare('SELECT article_number FROM draft_rakuten WHERE draft_id = ?').get(d.id) : null;
  const own = skuModel(d.id);
  const modelVal = own ? String(own.value ?? '').trim() : String(rk?.article_number ?? '').trim();
  const pc = phCountryOf(pi);
  return {
    draftId: d.id, page: true,
    country: { ...pc, raw: [pi?.origin_type, pi?.origin_country].filter((x) => String(x ?? '').trim()).join(' ') },
    net: pi && pi.content_volume ? netFromText(pi.content_volume) : null, netRaw: String(pi?.content_volume ?? '').trim(),
    model: modelVal ? { value: modelVal, from: own ? 'sku' : 'page' } : null,
  };
}
/** 本番の読み方: product-hub の SQLite (同じプロセス・lib/master-variation.mjs の defaultPhDraftExists と同じ) */
export async function defaultPhLabelLookup(code) {
  const { getDB } = await import('../apps/product-hub/db.js');
  return readPhLabelRefs(getDB(), code);
}

const keyOf = (e) => { const m = /^([a-z_]+):\s*([\s\S]*)$/.exec(String((e && e.message) || '')); return m ? [m[1], m[2]] : ['', '']; };

/**
 * 人の値を入れる / 変える / 外す (呼び手の取引の中・鍵の後)。value = { country_of_origin, net_content (数の文字), net_content_uom, model_number, shelf_life_days }
 * (全部 null か value = null = 外す)・seen = 画面が見ていた最後の記録の番号 (新商品 = null)。戻り値 = DB の答え。誤りは MasterWriteError (field = label)
 */
export async function setProductLabelInTx(db, { requestId, actor, skuId, value = null, seen = null, via, reason = null, label = '' }) {
  const at = label ? `${label}: ` : '';
  const f = { field: 'label', ...(label ? { code: label } : {}) };
  const v = value || {};
  try {
    return (await db.query('select ops.set_product_label_facts($1::uuid, $2, $3::bigint, $4, $5::numeric, $6, $7, $8::integer, $9::bigint, $10, $11) as r',
      [requestId, actor, String(skuId), v.country_of_origin ?? null, v.net_content ?? null, v.net_content_uom ?? null, v.model_number ?? null,
        v.shelf_life_days ?? null, seen == null ? null : String(seen), via, reason || null])).rows[0].r;
  } catch (e) {
    const [key, msg] = keyOf(e);
    if (key === 'version_conflict') throw new MasterWriteError(409, key, `${at}画面を開いた後に、ほかの人がこの商品の表示の項目 (原産国・内容量・型番・消費期限) を変えました。何も保存していません。画面を開き直してください`, f);
    if (key === 'not_single' || key === 'cancelled') throw new MasterWriteError(400, key, `${at}${msg}。何も保存していません`, f);
    if (key === 'before_cutover') throw new MasterWriteError(409, key, `${at}切替前です: 表示の項目はまだ入れられません (${msg})。何も保存していません`, f);
    if (key === 'invalid_input') throw new MasterWriteError(400, key, `${at}表示の項目が DB の決まりに合いません (${msg})。何も保存していません`, f);
    throw e;
  }
}

/** 0099 の前の DB・ロールの流し直しの前に値が来た = 断る (何も保存しない・黙って捨てない) */
export const labelNotReadyError = () => new MasterWriteError(503, 'db_not_ready', 'Company DB に 0099 (表示の項目) がまだ入っていないか、権限の流し直しの前です。何も保存していません', { field: 'label' });
