/**
 * master-package.mjs — 商品 (単品) の梱包した状態の 縦・横・高さ・重さ (中原さん 2026-10-17・Company DB の 0086)
 *
 * 中原さん「マスタの入力に 梱包した状態の 縦・横・高さ・重さ を入れたい。登録の時は必須にしない。
 *           この情報は FBA 納品のアプリとか、定形外の発送方法で情報をもっていたはずだから集約して」
 *   - 人の値 = core.product_packages (商品ごとに 1 行・軸ごとに空でよい)。書くのは DB の関数 ops.set_product_package だけ
 *     (新商品の登録・まとまりの登録・商品の画面の保存の取引の中で。守りはメーカー (0084) と同じ)
 *   - 出どころの値 = core.product_physicals (0003・scope = package)。夜間ロード (engine.mjs の 11.) が定形外 (postage.db pm_skus)・
 *     FBA 納品 (fba-box.db fbx_weight_refs / fbx_weight_current) から毎晩足す。ここでは読むだけ
 *   - まとめた値 = core.v_product_packages (軸ごとに 人 → 出どころの順位 → 新しい順)。この画面の「いま使う値」・一覧・CSV も同じビュー
 *   - 単位: 縦・横・高さ = DB は mm (整数)・画面は cm (小数 1 けた)。重さ = g (整数)
 *   - 単品だけ (セットは構成品から)。登録で必須にしない (「値か入力なし」にも数えない = lib/master-register-blanks.mjs に入れない)
 * 0086 の前の DB・ロールの流し直しの前 (master_edit に実行・読む権限が無い) = packageReady が false = 画面は欄を出さない・保存に値が来たら 503 db_not_ready
 */
import { MasterWriteError } from './master-write.mjs';

/** 軸 (DB の列) と画面の言葉。並び = 縦・横・高さ・重さ */
export const PACKAGE_AXES = Object.freeze([
  Object.freeze({ axis: 'length_mm', field: 'pkg_length', label: '縦', unit: 'cm', key: 'length' }),
  Object.freeze({ axis: 'width_mm', field: 'pkg_width', label: '横', unit: 'cm', key: 'width' }),
  Object.freeze({ axis: 'height_mm', field: 'pkg_height', label: '高さ', unit: 'cm', key: 'height' }),
  Object.freeze({ axis: 'weight_g', field: 'pkg_weight', label: '重さ', unit: 'g', key: 'weight' }),
]);
/** 保存の欄の名前 (商品の画面の values のキー) */
export const PACKAGE_FIELDS = Object.freeze(PACKAGE_AXES.map((a) => a.field));
export const AXIS_OF_FIELD = Object.freeze(Object.fromEntries(PACKAGE_AXES.map((a) => [a.field, a.axis])));
/** 範囲 (DB の check と同じ): 長さ 1〜3000 mm (0.1〜300 cm)・重さ 1〜100000 g */
export const LEN_MM_MAX = 3000;
export const WEIGHT_G_MAX = 100000;
export const PACKAGE_FUNCTION = 'ops.set_product_package(uuid, text, bigint, integer, integer, integer, integer, bigint, text, text)';

/**
 * 出どころの順位 (小さいほど先)。DB の core.package_source_rank と同じ (試験で照らす)。人はいつも 0
 *   measured 1・amazon_catalog 2・yahoo 3 = 0008 の規則 v1 (package_weight_g) と同じ並び。規則に無い出どころは参考のために後ろ
 */
export const SOURCE_RANK = Object.freeze({ human: 0, measured: 1, amazon_catalog: 2, yahoo: 3, supplier: 4, postage_estimate: 8 });
export const sourceRank = (src) => (Object.prototype.hasOwnProperty.call(SOURCE_RANK, src) ? SOURCE_RANK[src] : 5);

/**
 * 出どころの言葉 (画面・CSV)。どのアプリの値か (source_ref) と、測ったか (source_system) の両方を出す
 *   source_ref: pm_skus = 定形外 (送料の計算の商品の表) / fbx_weight_current:<FNSKU> = FBA 納品 (箱詰めの 1 個の重さ) /
 *               fbx_weight_refs:<FNSKU> = FBA 納品 (Amazon のカタログから取った重さ)
 */
export function sourceWords(src, ref) {
  if (src === 'human') return '人';
  const r = String(ref || '');
  const app = r === 'pm_skus' ? '定形外' : r.startsWith('fbx_weight_current:') ? 'FBA 納品' : r.startsWith('fbx_weight_refs:') ? 'FBA 納品' : '';
  const kind = { measured: '実測', amazon_catalog: 'Amazon のカタログ', yahoo: 'Yahoo', supplier: '仕入先の値', postage_estimate: '見込み' }[src] || src;
  return app ? `${app} (${kind})` : kind;
}

// ─── 入力の形 ───
const toHalf = (s) => String(s).replace(/[０-９．，]/g, (c) => String.fromCharCode(c.charCodeAt(0) - 0xFEE0));
const bad = (message, field) => new MasterWriteError(400, 'invalid_input', message, { field });
/** cm の文字 (小数 1 けたまで) → mm の整数。空 = null。0.1〜300 cm */
export function cmIn(v, label = '長さ', field = 'package') {
  if (v == null) return null;
  const s = typeof v === 'number' ? String(v) : toHalf(String(v)).replace(/\s/g, '').replace(/cm$/i, '');
  if (s === '') return null;
  if (!/^\d{1,3}(\.\d)?$/.test(s)) throw bad(`${label}は cm で、小数は 1 けたまでの数で入れてください (例: 23.5)`, field);
  const mm = Math.round(Number(s) * 10);
  if (!(mm >= 1 && mm <= LEN_MM_MAX)) throw bad(`${label}は 0.1〜${LEN_MM_MAX / 10} cm で入れてください`, field);
  return mm;
}
/** g の文字 (整数・, は無視) → 整数。空 = null。1〜100000 g */
export function gramIn(v, label = '重さ', field = 'package') {
  if (v == null) return null;
  const s = typeof v === 'number' ? String(v) : toHalf(String(v)).replace(/[,\s]/g, '').replace(/g$/i, '');
  if (s === '') return null;
  if (!/^\d{1,6}$/.test(s)) throw bad(`${label}は g の整数で入れてください (例: 250)`, field);
  const g = Number(s);
  if (!(g >= 1 && g <= WEIGHT_G_MAX)) throw bad(`${label}は 1〜${WEIGHT_G_MAX.toLocaleString('ja-JP')} g で入れてください`, field);
  return g;
}
/** 商品の画面の 1 つの欄 (pkg_length など) の値 → mm / g か null */
export function packageFieldIn(field, v) {
  const a = PACKAGE_AXES.find((x) => x.field === field);
  if (!a) throw bad(`知らない欄 ${field}`, field);
  return a.axis === 'weight_g' ? gramIn(v, '梱包の重さ', field) : cmIn(v, `梱包の${a.label}`, 'package');   // 縦・横・高さは 1 つの行 (data-row="package")
}
/**
 * 新商品の登録・まとまりの登録の値 { length, width, height (cm の文字), weight (g の文字) } → { length_mm, width_mm, height_mm, weight_g } か null
 *   (送らない・全部空 = null = 人の値を入れない。知らないキー = 400)
 */
export function packageIn(raw, field = 'package') {
  if (raw == null) return null;
  if (typeof raw !== 'object' || Array.isArray(raw)) throw bad('梱包の大きさ・重さの形が違う。画面を開き直してください', field);
  for (const k of Object.keys(raw)) if (!['length', 'width', 'height', 'weight'].includes(k)) throw bad(`梱包の大きさ・重さに知らない項目 ${String(k).slice(0, 20)} があります`, field);
  const out = {
    length_mm: cmIn(raw.length, '梱包の縦', field), width_mm: cmIn(raw.width, '梱包の横', field), height_mm: cmIn(raw.height, '梱包の高さ', field),
    weight_g: gramIn(raw.weight, '梱包の重さ', field),
  };
  return Object.values(out).every((x) => x == null) ? null : out;
}

// ─── 見せ方 ───
/** mm → cm の文字 (小数 1 けた・.0 は付けない)。null = '' */
export const cmText = (mm) => (mm == null ? '' : (Math.round(Number(mm)) / 10).toFixed(1).replace(/\.0$/, ''));
/** まとめた値の 1 行の言葉 (一覧・CSV の参考): 23.5×12×3 cm · 250 g */
export function packageText(p) {
  if (!p) return '';
  const dims = [p.length_mm, p.width_mm, p.height_mm];
  const d = dims.some((x) => x != null) ? `${dims.map((x) => (x == null ? '?' : cmText(x))).join('×')} cm` : '';
  const w = p.weight_g != null ? `${Number(p.weight_g).toLocaleString('ja-JP')} g` : '';
  return [d, w].filter(Boolean).join(' · ');
}

// ─── DB ───
/** 0086 があり、画面のロール master_edit が書く関数を実行でき、表・ビューを読めるか (ロールの流し直しの後) */
export async function packageReady(db) {
  return (await db.query(`select coalesce((select pg_catalog.has_function_privilege(r.oid, to_regprocedure($1), 'EXECUTE')
        and pg_catalog.has_table_privilege(r.oid, to_regclass('core.product_packages'), 'SELECT')
        and pg_catalog.has_table_privilege(r.oid, to_regclass('ops.product_package_events'), 'SELECT')
        and pg_catalog.has_table_privilege(r.oid, to_regclass('core.v_product_packages'), 'SELECT')
        and pg_catalog.has_table_privilege(r.oid, to_regclass('core.product_physicals'), 'SELECT')
      from pg_catalog.pg_roles r where r.rolname = 'master_edit' and to_regprocedure($1) is not null and to_regclass('core.v_product_packages') is not null), false) as ok`,
  [PACKAGE_FUNCTION])).rows[0]?.ok === true;
}
/**
 * 表・ビューがあって今の接続のロールが読めるか (0086 の前の DB は無い・ロールの流し直しの前の master_edit は読めない = どちらも「値なし」として読む =
 * 商品の画面・保存・一覧を止めない)。同じ接続 (要求ごとの db) の中では 1 回だけ聞く
 */
export async function packageTablesExist(db) {
  if (db && typeof db === 'object' && tablesSeen.has(db)) return tablesSeen.get(db);
  const ok = (await db.query(`select coalesce((select bool_and(pg_catalog.has_table_privilege(t.oid, 'SELECT')) from (select to_regclass('core.product_packages') as oid
      union all select to_regclass('ops.product_package_events') union all select to_regclass('core.v_product_packages') union all select to_regclass('core.product_physicals')) t
      where t.oid is not null having count(*) = 4), false) as ok`)).rows[0].ok === true;
  if (db && typeof db === 'object') tablesSeen.set(db, ok);
  return ok;
}
const tablesSeen = new WeakMap();

const numOrNull = (v) => (v == null ? null : Number(v));
/** DB の時刻 (Date か文字) → ISO の文字 (読めない = null) */
const isoOf = (v) => { if (v == null) return null; const d = v instanceof Date ? v : new Date(String(v).replace(' ', 'T').replace(/([+-]\d{2})$/, '$1:00')); return Number.isNaN(d.getTime()) ? null : d.toISOString(); };
/** 商品の人の値と最後の記録の番号 (保存の確かめに使う) = { human: { length_mm, …, set_by, set_via, set_at } | null, event_id: 文字 | null } */
export async function readHumanPackage(db, productId) {
  if (productId == null || !(await packageTablesExist(db))) return { human: null, event_id: null };
  const h = (await db.query(`select length_mm, width_mm, height_mm, weight_g, set_by, set_via, set_at from core.product_packages where product_id = $1`, [String(productId)])).rows[0];
  const ev = (await db.query('select max(event_id)::text as id from ops.product_package_events where product_id = $1', [String(productId)])).rows[0];
  return {
    human: h ? { length_mm: numOrNull(h.length_mm), width_mm: numOrNull(h.width_mm), height_mm: numOrNull(h.height_mm), weight_g: numOrNull(h.weight_g), set_by: h.set_by, set_via: h.set_via, set_at: isoOf(h.set_at) } : null,
    event_id: ev && ev.id ? ev.id : null,
  };
}

/**
 * 商品の画面の「大きさ・重さ」= { human, event_id, sources: [{ source, ref, words, rank, length_mm, …, observed_at }], resolved: { axis: { value, source, ref, words, at } | null }, conflicts: { axis: [値…] } }
 *   sources = 出どころ × 参照ごとの最新の行 (ビューと同じ決まり・順位 → 新しい順)。conflicts = 軸ごとに 人の値と出どころの値で 2 つ以上の違う値がある (人の値も入れる)
 */
export async function readPackageView(db, productId) {
  const empty = { human: null, event_id: null, sources: [], resolved: Object.fromEntries(PACKAGE_AXES.map((a) => [a.axis, null])), conflicts: {} };
  if (productId == null || !(await packageTablesExist(db))) return empty;
  const { human, event_id } = await readHumanPackage(db, productId);
  const rows = (await db.query(`select distinct on (ph.source_system, coalesce(ph.source_ref, '')) ph.source_system as source, ph.source_ref as ref,
        ph.length_mm, ph.width_mm, ph.height_mm, ph.weight_g, ph.observed_at, ph.product_physical_id::text as id
      from core.product_physicals ph where ph.product_id = $1 and ph.scope = 'package' and ph.observed_at <= now()
     order by ph.source_system, coalesce(ph.source_ref, ''), ph.observed_at desc, ph.product_physical_id desc`, [String(productId)])).rows;
  const sources = rows.map((r) => ({ source: r.source, ref: r.ref, words: sourceWords(r.source, r.ref), rank: sourceRank(r.source),
    length_mm: numOrNull(r.length_mm), width_mm: numOrNull(r.width_mm), height_mm: numOrNull(r.height_mm), weight_g: numOrNull(r.weight_g), observed_at: isoOf(r.observed_at), id: r.id }))
    .filter((r) => PACKAGE_AXES.some((a) => r[a.axis] != null))
    .sort((a, b) => a.rank - b.rank || String(b.observed_at).localeCompare(String(a.observed_at)) || Number(b.id) - Number(a.id));
  const v = (await db.query('select * from core.v_product_packages where product_id = $1', [String(productId)])).rows[0] || null;
  const resolved = {}; const conflicts = {};
  for (const a of PACKAGE_AXES) {
    const k = a.axis.replace(/_(mm|g)$/, '');
    resolved[a.axis] = v && v[a.axis] != null
      ? { value: Number(v[a.axis]), source: v[`${k}_source`], ref: v[`${k}_ref`], words: sourceWords(v[`${k}_source`], v[`${k}_ref`]), at: isoOf(v[`${k}_at`]) }
      : null;
    const vals = new Set([...(human && human[a.axis] != null ? [human[a.axis]] : []), ...sources.filter((s) => s[a.axis] != null).map((s) => s[a.axis])]);
    if (vals.size > 1) conflicts[a.axis] = [...vals];
  }
  return { human, event_id, sources, resolved, conflicts };
}

/** 商品ごとのまとめた値 (一覧・CSV) = Map<product_id, { length_mm, width_mm, height_mm, weight_g, sources: { axis: 出どころ } }> */
export async function packagesOf(db, productIds) {
  const ids = [...new Set((productIds || []).filter(Boolean).map(String))];
  if (!ids.length || !(await packageTablesExist(db))) return new Map();
  const rows = (await db.query('select * from core.v_product_packages where product_id = any($1::bigint[])', [ids])).rows;
  return new Map(rows.map((r) => [String(r.product_id), {
    length_mm: numOrNull(r.length_mm), width_mm: numOrNull(r.width_mm), height_mm: numOrNull(r.height_mm), weight_g: numOrNull(r.weight_g),
    sources: { length_mm: r.length_source, width_mm: r.width_source, height_mm: r.height_source, weight_g: r.weight_source },
    refs: { length_mm: r.length_ref, width_mm: r.width_ref, height_mm: r.height_ref, weight_g: r.weight_ref },
  }]));
}

const keyOf = (e) => { const m = /^([a-z_]+):\s*([\s\S]*)$/.exec(String((e && e.message) || '')); return m ? [m[1], m[2]] : ['', '']; };

/**
 * 人の値を入れる / 変える / 外す (呼び手の取引の中・鍵の後)。value = { length_mm, width_mm, height_mm, weight_g } (全部 null か value = null = 外す)・
 * seen = 画面が見ていた最後の記録の番号 (新商品 = null)。戻り値 = DB の答え ({ changed, from, to, event_id, … })。誤りは MasterWriteError (field = package)
 */
export async function setProductPackageInTx(db, { requestId, actor, skuId, value = null, seen = null, via, reason = null, label = '' }) {
  const at = label ? `${label}: ` : '';
  const f = { field: 'package', ...(label ? { code: label } : {}) };
  const v = value || {};
  try {
    return (await db.query('select ops.set_product_package($1::uuid, $2, $3::bigint, $4::integer, $5::integer, $6::integer, $7::integer, $8::bigint, $9, $10) as r',
      [requestId, actor, String(skuId), v.length_mm ?? null, v.width_mm ?? null, v.height_mm ?? null, v.weight_g ?? null, seen == null ? null : String(seen), via, reason || null])).rows[0].r;
  } catch (e) {
    const [key, msg] = keyOf(e);
    if (key === 'version_conflict') throw new MasterWriteError(409, key, `${at}画面を開いた後に、ほかの人がこの商品の梱包の大きさ・重さを変えました。何も保存していません。画面を開き直してください`, f);
    if (key === 'not_single' || key === 'cancelled') throw new MasterWriteError(400, key, `${at}${msg}。何も保存していません`, f);
    if (key === 'before_cutover') throw new MasterWriteError(409, key, `${at}切替前です: 梱包の大きさ・重さはまだ入れられません (${msg})。何も保存していません`, f);
    if (key === 'invalid_input') throw new MasterWriteError(400, key, `${at}梱包の大きさ・重さが DB の決まりに合いません (${msg})。何も保存していません`, f);
    throw e;
  }
}

/** 0086 の前の DB・ロールの流し直しの前に値が来た = 断る (何も保存しない・黙って捨てない) */
export const packageNotReadyError = () => new MasterWriteError(503, 'db_not_ready', 'Company DB に 0086 (梱包の大きさ・重さ) がまだ入っていないか、権限の流し直しの前です。何も保存していません', { field: 'package' });
