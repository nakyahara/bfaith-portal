/**
 * master-brand-life.mjs — ブランドの名簿と商品ごとのブランド・商品の段階 (区分・終売の予定日)・初めて売った日 (中原さん 2026-10-17・Company DB の 0085)
 *
 * 中原さん「メーカーとは別にブランド (自社ブランド・他社ブランド)」「商品の段階: 発売日 (= 初めて売った日)・定番 / 季節 / 試し・終売の予定日」
 *   - Company DB だけに持つ (NE・ロジザード・product-hub には送らない)。core.products.brand は夜間ロードの属性の解決が書く列・own_brand は商品の行の印 = 使わない (0085 の頭)
 *   - どれも必須でない (新商品の登録の「値か入力なし」= lib/master-register-blanks.mjs に数えない)。単品だけ (セットは構成品から)
 *   - 名簿 = core.brands (同じと見る名前 = メーカーと同じ決まり = lib/master-maker.mjs の makerNameNorm・DB の core.maker_name_norm)・自社ブランドの印 own_brand
 *   - 書くのは DB の関数だけ: 名簿に足す ops.create_brand・印を変える ops.set_brand_own (どちらも autocommit の 1 文) /
 *     商品に付ける ops.set_product_brand・段階 ops.set_product_lifecycle (新商品の登録・まとまりの登録・商品の画面の保存の取引の中で)
 *   - 初めて売った日 (発売日) = 人は入れない。ops.sku_first_sold (NE の出荷 + モールの注文のいちばん早い日・注文・出荷の表は画面のロールに読ませない) から毎回出す
 * 0085 の前の DB・ロールの流し直しの前 = brandLifeReady が false = 画面は欄を出さない・保存に値が来たら 503 db_not_ready
 */
import { MasterWriteError } from './master-write.mjs';
import { makerNameClean, makerNameNorm, makerNameProblem } from './master-maker.mjs';

// 🚨 #brand Codex R1 M: master-maker → master-write → この file → master-maker の読み込みの輪 = 一番上ではメーカーの値を読まない (関数の中だけで使う)。
//   字数の上限はメーカーと同じ 100 (lib/master-maker.mjs の MAKER_NAME_MAX・DB の ops.maker_name_problem・試験で照らす)
export const BRAND_NAME_MAX = 100;
/** 画面に渡す名簿の上限 (メーカーと同じ) */
export const BRAND_LIST_MAX = 5000;
/** 区分 (DB の値 → 画面の言葉)。並び = 画面の選ぶ順 */
export const LIFE_STAGES = Object.freeze({ staple: '定番', seasonal: '季節', trial: '試し' });
/** 一覧の「終売の予定が近い」= 今日からこの日数の中 (過ぎたものも入れる) */
export const END_SOON_DAYS = 60;
/** 一覧の絞り込み (life=…) */
export const LIFE_FILTERS = Object.freeze({
  end_soon: `終売が近い (${END_SOON_DAYS} 日以内・過ぎた)`, end_set: '終売の予定日あり',
  staple: '区分: 定番', seasonal: '区分: 季節', trial: '区分: 試し', stage_none: '区分なし',
});
const ID_RE = /^[1-9][0-9]{0,17}$/;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const DATE_RE = /^(\d{4})-(\d{2})-(\d{2})$/;
export const BRAND_LIFE_FUNCTIONS = Object.freeze({
  create: 'ops.create_brand(uuid, text, text, boolean)',
  own: 'ops.set_brand_own(uuid, text, bigint, boolean, boolean, text)',
  setBrand: 'ops.set_product_brand(uuid, text, bigint, bigint, bigint, text, text)',
  setLife: 'ops.set_product_lifecycle(uuid, text, bigint, text, date, text, date, text, text)',
  firstSold: 'ops.sku_first_sold(bigint[])',
  recordsFrom: 'ops.sales_records_from()',
});
/** 名前の形はメーカーと同じ (掃除・同じと見る形・問題)。読み込みの輪のため、呼んだ時にメーカーの関数を使う */
export const brandNameClean = (s) => makerNameClean(s);
export const brandNameNorm = (s) => makerNameNorm(s);
export const brandNameProblem = (s) => makerNameProblem(s);
export const BRAND_PROBLEM_WORDS = Object.freeze({ empty: 'ブランドの名前を入れてください', too_long: `ブランドの名前は ${BRAND_NAME_MAX} 字までです`, control: 'ブランドの名前に改行や制御文字は入れられません' });
const NOT_READY = 'Company DB に 0085 (ブランド・商品の段階) がまだ入っていないか、権限の流し直しの前です。何も保存していません';

/* ---------- 値の形 (保存・登録の PARSERS) ---------- */
/** ブランド = 名簿の番号 (文字) か null (空 = なし) */
export function brandIdIn(v, field = 'brand') {
  if (v == null || (typeof v === 'string' && v.trim() === '')) return null;
  const s = String(v).trim();
  if (!ID_RE.test(s)) throw new MasterWriteError(400, 'invalid_input', 'ブランドの入れ方が違う (一覧から選んでください)。画面を開き直してください', { field });
  return s;
}
/** 区分 = staple / seasonal / trial か null (空 = なし) */
export function lifeStageIn(v, field = 'life_stage') {
  if (v == null || (typeof v === 'string' && v.trim() === '')) return null;
  const s = String(v).trim();
  if (!Object.prototype.hasOwnProperty.call(LIFE_STAGES, s)) throw new MasterWriteError(400, 'invalid_input', '区分は 定番・季節・試し のどれか (なしは空) です', { field });
  return s;
}
/** 終売の予定日 = 'YYYY-MM-DD' (実在の日・2000〜2099 年) か null (空 = なし) */
export function endDateIn(v, field = 'end_planned_on') {
  if (v == null || (typeof v === 'string' && v.trim() === '')) return null;
  const s = String(v).trim();
  const m = DATE_RE.exec(s);
  const d = m ? new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3]))) : null;
  if (!m || Number.isNaN(d.getTime()) || d.toISOString().slice(0, 10) !== s || s < '2000-01-01' || s > '2099-12-31') {
    throw new MasterWriteError(400, 'invalid_input', '終売の予定日は 2000-01-01〜2099-12-31 の日付で入れてください (なしは空)', { field });
  }
  return s;
}

/* ---------- 欄を出すか・読めるか ---------- */
/** 0085 があり、画面のロール master_edit が 6 つの関数を実行できるか (ロールの流し直しの後) */
export async function brandLifeReady(db) {
  const fns = Object.values(BRAND_LIFE_FUNCTIONS);
  return (await db.query(`select coalesce((select bool_and(pg_catalog.has_function_privilege(r.oid, p.fn, 'EXECUTE')) from pg_catalog.pg_roles r
      cross join (select to_regprocedure(x) as fn from unnest($1::text[]) as t(x)) p where r.rolname = 'master_edit' and p.fn is not null
      having count(*) = $2), false) as ok`, [fns, fns.length])).rows[0]?.ok === true;
}
const tablesSeen = new WeakMap();
/** 表があって今の接続のロールが読めるか (0085 の前・ロールの流し直しの前 = どちらも「なし」として読む = 画面・保存・一覧を止めない)。同じ接続では 1 回だけ聞く */
export async function brandLifeTablesExist(db) {
  if (db && typeof db === 'object' && tablesSeen.has(db)) return tablesSeen.get(db);
  const ok = (await db.query(`select coalesce((select bool_and(pg_catalog.has_table_privilege(t.oid, 'SELECT')) from (select to_regclass(x) as oid from unnest($1::text[]) as u(x)) t
      where t.oid is not null having count(*) = 3), false) as ok`, [['core.brands', 'core.product_brands', 'core.product_lifecycle']])).rows[0].ok === true;
  if (db && typeof db === 'object') tablesSeen.set(db, ok);
  return ok;
}

/* ---------- 読む ---------- */
/** 名簿 (使うブランドだけ・同じと見る形の順) = [{ id, name, own }] */
export async function readBrands(db) {
  if (!(await brandLifeTablesExist(db))) return [];
  return (await db.query(`select brand_id::text as id, name, own_brand as own from core.brands where company_id = 1 and active order by name_norm, brand_id limit ${BRAND_LIST_MAX}`)).rows;
}
/** 商品ごとのブランドと段階 (一覧・CSV・商品の画面) = Map<product_id, { brand: { brand_id, name, own_brand } | null, stage, end_planned_on }> */
export async function brandLifeOf(db, productIds) {
  const ids = [...new Set((productIds || []).filter(Boolean).map(String))];
  const out = new Map();
  if (!ids.length || !(await brandLifeTablesExist(db))) return out;
  const rows = (await db.query(`select p.pid::text as pid, b.brand_id::text as brand_id, b.name, b.own_brand, l.stage, l.end_planned_on::text as end_planned_on
      from unnest($1::bigint[]) as p(pid)
      left join core.product_brands x on x.product_id = p.pid left join core.brands b on b.brand_id = x.brand_id
      left join core.product_lifecycle l on l.product_id = p.pid
     where x.product_id is not null or l.product_id is not null`, [ids])).rows;
  for (const r of rows) {
    out.set(String(r.pid), { brand: r.brand_id ? { brand_id: r.brand_id, name: r.name, own_brand: r.own_brand === true } : null, stage: r.stage ?? null, end_planned_on: r.end_planned_on ?? null });
  }
  return out;
}
/** 商品の画面・保存の「今」(readCurrent) に足す: cur.brand = { brand_id, name, own_brand } | null・cur.lifecycle = { stage, end_planned_on } | null */
export async function readBrandLifeInto(db, cur) {
  cur.brand = null;
  cur.lifecycle = null;
  if (!cur.product_id || cur.sku_kind !== 'single') return cur;
  const v = (await brandLifeOf(db, [cur.product_id])).get(String(cur.product_id));
  if (v) {
    cur.brand = v.brand;
    cur.lifecycle = v.stage != null || v.end_planned_on != null ? { stage: v.stage, end_planned_on: v.end_planned_on } : null;
  }
  return cur;
}
/** 編集の印に入れる値 (付いているときだけ = ブランド・段階の無い商品の印は前と同じ = 配った直後に開いていた画面を「変わった」にしない) */
export function brandLifeToken(cur) {
  return {
    brand: cur.brand ? cur.brand.brand_id : undefined,
    life: cur.lifecycle ? [cur.lifecycle.stage ?? null, cur.lifecycle.end_planned_on ?? null] : undefined,
  };
}

/* ---------- 初めて売った日 (発売日) ---------- */
const MALL_WORDS = { amazon: 'Amazon', rakuten: '楽天', yahoo: 'Yahoo!', aupay: 'au PAY', qoo10: 'Qoo10', linegift: 'LINE ギフト', mercari: 'メルカリ', other: 'ほかのモール' };
/** SKU ごとの初めて売った日 = Map<sku_id, { date, via: 'shipment' | 'order', detail }> (売れていない = 無い)。0085 の前・読めない = 空 */
export async function firstSoldOf(db, skuIds) {
  const ids = [...new Set((skuIds || []).filter(Boolean).map(String))];
  if (!ids.length || !(await brandLifeReady(db))) return new Map();
  const rows = (await db.query('select sku_id::text as sku_id, first_date::text as d, via, detail from ops.sku_first_sold($1::bigint[])', [ids])).rows;
  return new Map(rows.map((r) => [r.sku_id, { date: r.d, via: r.via, detail: r.detail || '' }]));
}
/** 記録の始まりの日 = { shipment: 'YYYY-MM-DD' | null, order: … } */
export async function salesRecordsFrom(db) {
  if (!(await brandLifeReady(db))) return { shipment: null, order: null };
  const rows = (await db.query('select src, first_date::text as d from ops.sales_records_from()')).rows;
  const out = { shipment: null, order: null };
  for (const r of rows) out[r.src] = r.d ?? null;
  return out;
}
const dayMs = (s) => Date.parse(`${s}T00:00:00Z`);
/** 記録の始まりからこの日数の中 = 「それより前から売っていたかもしれません」 */
export const FIRST_SOLD_NEAR_START_DAYS = 14;
/** 記録の始まりより前でも「近く」と見る日数 (出荷の記録 = 出荷の日で始まる・初めて売った日 = 受注日) */
export const FIRST_SOLD_BEFORE_START_DAYS = 31;
/**
 * 画面の言葉 (商品の画面の「発売日」)。fs = firstSoldOf の 1 つ (無い = まだ売れていない)・from = salesRecordsFrom
 *   戻り値 { date, text, source, note, beforeRecords }。記録の始まりのどれかから FIRST_SOLD_NEAR_START_DAYS 日の中 = beforeRecords (それより前から売っていたかも)
 */
export function firstSoldView(fs, from) {
  const starts = [['出荷', from && from.shipment], ['モールの注文', from && from.order]].filter(([, d]) => d);
  const records = starts.length ? `記録: ${starts.map(([w, d]) => `${w} ${d.replace(/-/g, '/')}〜`).join(' · ')}` : '記録: まだありません';
  if (!fs || !fs.date) return { date: null, text: 'まだ売れていません', source: '', note: records, beforeRecords: false };
  // 🆕 #brand Codex R1 L: 記録の始まりの近くだけ = 始まりの 31 日前〜14 日後 (出荷の記録の始まりは出荷の日・初めて売った日は受注日 = 始まりの少し前の受注もある)。
  //   ずっと前の日 (ほかの記録の中の日) は「すぐ後」ではない
  const near = starts.find(([, d]) => { const diff = dayMs(fs.date) - dayMs(d); return diff >= -FIRST_SOLD_BEFORE_START_DAYS * 86400e3 && diff <= FIRST_SOLD_NEAR_START_DAYS * 86400e3; });
  const source = fs.via === 'order' ? `${MALL_WORDS[fs.detail] || fs.detail || 'モール'}の注文` : `NE の出荷${fs.detail ? ` (${fs.detail})` : ''}`;
  return {
    date: fs.date, text: fs.date.replace(/-/g, '/'), source,
    note: near ? `記録の始まり (${near[0]} ${near[1].replace(/-/g, '/')}) の近くです。それより前から売っていたかもしれません · ${records}` : records,
    beforeRecords: !!near,
  };
}
/** 終売の予定日までの日数 (今日 = 0・過ぎた = 負) */
export function daysUntil(dateStr, today) {
  if (!dateStr || !today) return null;
  return Math.round((dayMs(dateStr) - dayMs(today)) / 86400e3);
}
/** 一覧・画面の終売の予定日の札 { text, tone } (tone: err = 過ぎた / warn = 近い / '' ) */
export function endBadge(dateStr, today) {
  const n = daysUntil(dateStr, today);
  if (n == null) return null;
  if (n < 0) return { text: '過ぎた', tone: 'err' };
  if (n === 0) return { text: '今日', tone: 'warn' };
  if (n <= END_SOON_DAYS) return { text: `あと ${n} 日`, tone: 'warn' };
  return { text: '', tone: '' };
}

/* ---------- 書く (名簿) ---------- */
const keyOf = (e) => { const m = /^([a-z_]+):\s*([\s\S]*)$/.exec(String((e && e.message) || '')); return m ? [m[1], m[2]] : ['', '']; };
const ridOf = (requestId) => {
  const rid = String(requestId ?? '').trim().toLowerCase();
  if (!UUID_RE.test(rid)) throw new MasterWriteError(400, 'invalid_input', '保存の番号 (request_id) の形が違う。画面を開き直してください', { field: 'request_id' });
  return rid;
};
/**
 * 名簿に足す (同じと見る名前が既にある = それを返す・印も名簿のまま)。own = 自社ブランドとして足すか。1 つの文 = autocommit で 1 つの取引。
 * 戻り値 { ok, brand: { id, name, own }, existed, replayed }。誤りは MasterWriteError
 */
export async function createBrand(db, { requestId, actor, name, own = false }) {
  const rid = ridOf(requestId);
  const prob = brandNameProblem(name);
  if (prob) throw new MasterWriteError(400, 'invalid_input', BRAND_PROBLEM_WORDS[prob], { field: 'brand' });
  if (own != null && typeof own !== 'boolean') throw new MasterWriteError(400, 'invalid_input', '自社ブランドかどうかの形が違う。画面を開き直してください', { field: 'brand' });
  if (!(await brandLifeReady(db))) throw new MasterWriteError(503, 'db_not_ready', NOT_READY, { field: 'brand' });
  try {
    const r = (await db.query('select ops.create_brand($1::uuid, $2, $3, $4) as r', [rid, String(actor ?? '').trim().toLowerCase(), name, own === true])).rows[0].r;
    return { ok: true, brand: { id: String(r.brand_id), name: r.name, own: r.own_brand === true }, existed: r.existed === true, replayed: r.replayed === true };
  } catch (e) {
    const [key, msg] = keyOf(e);
    if (key === 'request_id_reused' || (e && e.code === '23505' && /(created_request_id|brand_requests_pkey)/.test(`${e.constraint || ''} ${e.message || ''}`))) {
      throw new MasterWriteError(409, 'request_id_reused', '同じ保存の番号 (request_id) で違う名前が来ました。画面を開き直してください', { field: 'brand' });
    }
    if (key === 'before_cutover') throw new MasterWriteError(409, key, `切替前です: ブランドはまだ足せません (${msg})。何も保存していません`, { field: 'brand' });
    if (key === 'invalid_input') throw new MasterWriteError(400, key, `ブランドの名前が DB の決まりに合いません (${msg})。何も保存していません`, { field: 'brand' });
    throw e;
  }
}
/** 自社ブランドの印を変える (名簿の行 = そのブランドの全部の商品に効く)。seenOwn = 画面が見ていた印。戻り値 { ok, brand: { id, name, own }, changed, replayed } */
export async function setBrandOwn(db, { requestId, actor, brandId, own, seenOwn }) {
  const rid = ridOf(requestId);
  const id = brandIdIn(brandId);
  if (!id || typeof own !== 'boolean' || typeof seenOwn !== 'boolean') throw new MasterWriteError(400, 'invalid_input', 'ブランドと自社ブランドの印の形が違う。画面を開き直してください', { field: 'brand' });
  if (!(await brandLifeReady(db))) throw new MasterWriteError(503, 'db_not_ready', NOT_READY, { field: 'brand' });
  try {
    const r = (await db.query('select ops.set_brand_own($1::uuid, $2, $3::bigint, $4, $5, null) as r', [rid, String(actor ?? '').trim().toLowerCase(), id, own, seenOwn])).rows[0].r;
    return { ok: true, brand: { id: String(r.brand_id), name: r.name, own: r.own_brand === true }, changed: r.changed === true, replayed: r.replayed === true };
  } catch (e) {
    const [key, msg] = keyOf(e);
    if (key === 'request_id_reused') throw new MasterWriteError(409, key, '同じ保存の番号 (request_id) で違う中身が来ました。画面を開き直してください', { field: 'brand' });
    if (key === 'version_conflict') throw new MasterWriteError(409, key, '画面を開いた後に、ほかの人がこのブランドの「自社ブランド」の印を変えました。何も保存していません。画面を開き直してください', { field: 'brand' });
    if (key === 'brand_unknown') throw new MasterWriteError(400, key, 'このブランドは名簿に無いか、使わないブランドです。何も保存していません', { field: 'brand' });
    if (key === 'before_cutover') throw new MasterWriteError(409, key, `切替前です (${msg})。何も保存していません`, { field: 'brand' });
    if (key === 'invalid_input') throw new MasterWriteError(400, key, `${msg}。何も保存していません`, { field: 'brand' });
    throw e;
  }
}
/** 選んだブランドが名簿にある (使う) か (保存の前の確かめ・DB の関数も同じことをもう一度見る)。戻り値 = { name, own_brand } */
export async function assertBrandUsable(db, brandId) {
  const r = (await db.query('select name, own_brand, active from core.brands where company_id = 1 and brand_id = $1::bigint', [String(brandId)])).rows[0];
  if (!r || !r.active) throw new MasterWriteError(400, 'brand_unknown', '選んだブランドは名簿に無いか、使わないブランドです。一覧から選び直してください。何も保存していません', { field: 'brand' });
  return { name: r.name, own_brand: r.own_brand === true };
}

/* ---------- 書く (商品・呼び手の取引の中) ---------- */
function txError(e, { at, f, what }) {
  const [key, msg] = keyOf(e);
  if (key === 'version_conflict') return new MasterWriteError(409, key, `${at}画面を開いた後に、ほかの人がこの商品の${what}を変えました。何も保存していません。画面を開き直してください`, f);
  if (key === 'brand_unknown') return new MasterWriteError(400, key, `${at}選んだブランドは名簿に無いか、使わないブランドです。一覧から選び直してください。何も保存していません`, f);
  if (key === 'not_single' || key === 'cancelled') return new MasterWriteError(400, key, `${at}${msg}。何も保存していません`, f);
  if (key === 'before_cutover') return new MasterWriteError(409, key, `${at}切替前です: ${what}はまだ入れられません (${msg})。何も保存していません`, f);
  if (key === 'invalid_input') return new MasterWriteError(400, key, `${at}${what}の値が DB の決まりに合いません (${msg})。何も保存していません`, f);
  return e;
}
/** 商品のブランドを付ける / 変える / 外す (呼び手の取引の中・鍵の後)。brandId = 番号 (null = 外す)・seen = 画面が見ていた今のブランド (新商品 = null) */
export async function setProductBrandInTx(db, { requestId, actor, skuId, brandId = null, seen = null, via, reason = null, label = '' }) {
  try {
    return (await db.query('select ops.set_product_brand($1::uuid, $2, $3::bigint, $4::bigint, $5::bigint, $6, $7) as r',
      [requestId, actor, String(skuId), brandId == null ? null : String(brandId), seen == null ? null : String(seen), via, reason || null])).rows[0].r;
  } catch (e) { throw txError(e, { at: label ? `${label}: ` : '', f: { field: 'brand', ...(label ? { code: label } : {}) }, what: 'ブランド' }); }
}
/** 商品の区分・終売の予定日を決める (両方の保存した後の値)。seen = 画面が見ていた今の値 { stage, end_planned_on } (新商品 = null) */
export async function setProductLifecycleInTx(db, { requestId, actor, skuId, stage = null, end = null, seen = null, via, reason = null, label = '' }) {
  try {
    return (await db.query('select ops.set_product_lifecycle($1::uuid, $2, $3::bigint, $4, $5::date, $6, $7::date, $8, $9) as r',
      [requestId, actor, String(skuId), stage, end, seen ? seen.stage ?? null : null, seen ? seen.end_planned_on ?? null : null, via, reason || null])).rows[0].r;
  } catch (e) { throw txError(e, { at: label ? `${label}: ` : '', f: { field: stage !== (seen ? seen.stage ?? null : null) ? 'life_stage' : 'end_planned_on', ...(label ? { code: label } : {}) }, what: '区分・終売の予定日' }); }
}

/* ---------- 商品の画面の保存 (lib/master-write.mjs の diffSingle / applySingle から呼ぶ) ---------- */
/** 欄の名前 (lib/master-write.mjs の SINGLE_FIELDS と同じ。🚨 master-write は読み込みの輪のため、ここの値を一番上で使わない = 同じ名前を自分でも持つ) */
const LABELS = Object.freeze({ brand: 'ブランド', life_stage: '区分 (定番・季節・試し)', end_planned_on: '終売の予定日' });
const stageWord = (s) => (s == null ? null : LIFE_STAGES[s] || s);
/**
 * 差を足す (add = diffSingle の add・bad = 400 の誤りを作る)。商品の行に付く = 行が無い単品は持てない。0085 の欄が使えない DB で値が来た = 503
 */
export async function diffBrandLife(db, cur, v, add, bad) {
  const curBrand = cur.brand ? cur.brand.brand_id : null;
  const curStage = cur.lifecycle ? cur.lifecycle.stage ?? null : null;
  const curEnd = cur.lifecycle ? cur.lifecycle.end_planned_on ?? null : null;
  const wantBrand = v.brand !== undefined && (v.brand ?? null) !== curBrand;
  const wantStage = v.life_stage !== undefined && (v.life_stage ?? null) !== curStage;
  const wantEnd = v.end_planned_on !== undefined && (v.end_planned_on ?? null) !== curEnd;
  if (!wantBrand && !wantStage && !wantEnd) return;
  const first = wantBrand ? 'brand' : wantStage ? 'life_stage' : 'end_planned_on';
  if (!cur.product_id) throw bad(`商品の行が無いので${LABELS[first]}を持てません (何も保存していません)`, first, 'no_product');
  if (!(await brandLifeReady(db))) throw new MasterWriteError(503, 'db_not_ready', NOT_READY, { field: first });
  if (wantBrand) {
    const to = v.brand == null ? null : await assertBrandUsable(db, v.brand);
    add('brand', cur.brand ? cur.brand.name : null, to ? to.name : null, { brand_id: v.brand ?? null, from_id: curBrand });
  }
  const life = { stage: wantStage ? v.life_stage ?? null : curStage, end: wantEnd ? v.end_planned_on ?? null : curEnd, seen: { stage: curStage, end_planned_on: curEnd } };
  if (wantStage) add('life_stage', stageWord(curStage), stageWord(life.stage), { life });
  if (wantEnd) add('end_planned_on', curEnd, life.end, { life });
}
/** 書く (applySingle の中・保存と同じ取引)。今の値が画面の見ていたものと違う = 409 (保存ごと戻る) */
export async function applyBrandLife(db, cur, by, ctx) {
  if (by.brand) await setProductBrandInTx(db, { requestId: ctx.requestId, actor: ctx.actor, skuId: cur.sku_id, brandId: by.brand.brand_id, seen: by.brand.from_id, via: 'sku_page', reason: ctx.reason });
  const life = (by.life_stage || by.end_planned_on || {}).life;
  if (life) await setProductLifecycleInTx(db, { requestId: ctx.requestId, actor: ctx.actor, skuId: cur.sku_id, stage: life.stage, end: life.end, seen: life.seen, via: 'sku_page', reason: ctx.reason });
}

/* ---------- 新商品の登録・まとまりの登録 (lib/master-register.mjs・lib/master-variation.mjs から呼ぶ) ---------- */
/** 登録で送れる値のキー (単品・まとまりの共通の欄)。空 = 送らないのと同じ (要求のハッシュも同じ = 呼び手が消す) */
export const BRAND_LIFE_VALUE_KEYS = Object.freeze(['brand', 'life_stage', 'end_planned_on']);
/** 値が来たときの確かめ (書く前・何も保存しない): 0085 が使える・名簿にある */
export async function checkBrandLifeValues(db, v) {
  if (v.brand == null && v.life_stage == null && v.end_planned_on == null) return;
  const first = v.brand != null ? 'brand' : v.life_stage != null ? 'life_stage' : 'end_planned_on';
  if (!(await brandLifeReady(db))) throw new MasterWriteError(503, 'db_not_ready', NOT_READY, { field: first });
  if (v.brand != null) await assertBrandUsable(db, v.brand);
}
/** 登録した SKU に書く (同じ取引・押し直しはここに来ない)。戻り値 = { brand?: { id, name, own }, life?: { stage, end_planned_on } } */
export async function applyBrandLifeRegister(db, { requestId, actor, skuId, v, via, reason = null, label = '' }) {
  const out = {};
  if (v.brand != null) {
    const r = await setProductBrandInTx(db, { requestId, actor, skuId, brandId: v.brand, via, reason, label });
    out.brand = { id: r.brand_id, name: r.brand_name, own: r.own_brand === true };
  }
  if (v.life_stage != null || v.end_planned_on != null) {
    const r = await setProductLifecycleInTx(db, { requestId, actor, skuId, stage: v.life_stage ?? null, end: v.end_planned_on ?? null, via, reason, label });
    out.life = { stage: r.stage ?? null, end_planned_on: r.end_planned_on ?? null };
  }
  return out;
}
