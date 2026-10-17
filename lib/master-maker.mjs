/**
 * master-maker.mjs — メーカー (製造元) の名簿と、商品ごとのメーカー (中原さん 2026-10-17・Company DB の 0083)
 *
 * 中原さん「マスタの入力で、仕入先とは別にメーカーの情報を入れたい。問屋から仕入れるとメーカー名と仕入先名が違う」
 *   - Company DB だけに持つ (NE・ロジザード・product-hub には送らない)。core.products.manufacturer は夜間ロードの属性の解決が書く列 = 使わない (0083 の頭)
 *   - 名簿 = core.makers (同じと見る名前 = NFKC → 小文字 → ひらがなをカタカナ → 空白を除く。DB の core.maker_name_norm と同じ決まり = 試験で照らす)
 *   - 商品ごと = core.product_makers (単品の商品の行に 1 つ・無い = メーカーなし)。セットは持たない (構成品から)
 *   - 書くのは DB の関数だけ: 名簿に足す = ops.create_maker (同じと見る名前が既にある = それを返す = 重ねない) /
 *     商品に付ける・変える・外す = ops.set_product_maker (新商品の登録・まとまりの登録・商品の画面の保存の取引の中で)
 * 0083 の前の DB・ロールの流し直しの前 (master_edit に実行の権限が無い) = makerReady が false = 画面は欄を出さない・保存に値が来たら 503 db_not_ready
 */
import { MasterWriteError } from './master-write.mjs';
import { foldSearch } from '../apps/master-edit/search-fold.mjs';

export const MAKER_NAME_MAX = 100;
/** 画面に渡す名簿の上限 (これより多いときは先頭から・名簿は数百の見込み) */
export const MAKER_LIST_MAX = 5000;
const CTRL_RE = new RegExp(`[\\x00-\\x1f\\x7f${String.fromCharCode(0x2028, 0x2029)}]`);
const ID_RE = /^[1-9][0-9]{0,17}$/;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
export const MAKER_FUNCTIONS = Object.freeze({
  create: 'ops.create_maker(uuid, text, text)',
  set: 'ops.set_product_maker(uuid, text, bigint, bigint, bigint, text, text)',
});

/** 名前の掃除 (DB の core.maker_name_clean と同じ: 空白 (半角・全角・ノーブレーク) の続きを 1 つの半角の空白に・前後を除く) */
export const makerNameClean = (s) => String(s ?? '').replace(/[ 　 ]+/g, ' ').trim();
/** 同じと見る形 (DB の core.maker_name_norm と同じ: search-fold.mjs の foldSearch + 空白を除く) */
export const makerNameNorm = (s) => foldSearch(s).replace(/ /g, '');
/** 名前の問題 (無い = null)。DB の ops.maker_name_problem と同じ */
export function makerNameProblem(s) {
  if (s == null || typeof s !== 'string') return 'empty';
  const c = makerNameClean(s);
  if (!c || !makerNameNorm(c)) return 'empty';
  if (CTRL_RE.test(s)) return 'control';
  if ([...c].length > MAKER_NAME_MAX) return 'too_long';
  return null;
}
export const MAKER_PROBLEM_WORDS = Object.freeze({ empty: 'メーカーの名前を入れてください', too_long: `メーカーの名前は ${MAKER_NAME_MAX} 字までです`, control: 'メーカーの名前に改行や制御文字は入れられません' });

/** 保存の値: メーカーの番号 (文字) か null (空 = なし) */
export function makerIdIn(v, field = 'maker') {
  if (v == null || (typeof v === 'string' && v.trim() === '')) return null;
  const s = String(v).trim();
  if (!ID_RE.test(s)) throw new MasterWriteError(400, 'invalid_input', 'メーカーの入れ方が違う (一覧から選んでください)。画面を開き直してください', { field });
  return s;
}

/** 0083 があり、画面のロール master_edit が 2 つの関数を実行できるか (ロールの流し直しの後) */
export async function makerReady(db) {
  return (await db.query(`select coalesce((select bool_and(pg_catalog.has_function_privilege(r.oid, p.fn, 'EXECUTE')) from pg_catalog.pg_roles r
      cross join (select to_regprocedure($1) as fn union all select to_regprocedure($2)) p where r.rolname = 'master_edit' and p.fn is not null
      having count(*) = 2), false) as ok`, [MAKER_FUNCTIONS.create, MAKER_FUNCTIONS.set])).rows[0]?.ok === true;
}
/**
 * 表があって今の接続のロールが読めるか (0083 の前の DB は無い・ロールの流し直しの前の master_edit は読めない = どちらも「メーカーなし」として読む =
 * 商品の画面・保存・一覧を止めない)
 */
export async function makerTablesExist(db) {
  // 同じ接続 (要求ごとの db) の中では 1 回だけ聞く (一括の変更などで readCurrent を何回も呼ぶ)
  if (db && typeof db === 'object' && tablesSeen.has(db)) return tablesSeen.get(db);
  const ok = (await db.query(`select coalesce((select bool_and(pg_catalog.has_table_privilege(t.oid, 'SELECT')) from (select to_regclass('core.product_makers') as oid union all select to_regclass('core.makers')) t
      where t.oid is not null having count(*) = 2), false) as ok`)).rows[0].ok === true;
  if (db && typeof db === 'object') tablesSeen.set(db, ok);
  return ok;
}
const tablesSeen = new WeakMap();

/** 名簿 (使うメーカーだけ・同じと見る形の順) = [{ id, name }] */
export async function readMakers(db) {
  if (!(await makerTablesExist(db))) return [];
  return (await db.query(`select maker_id::text as id, name from core.makers where company_id = 1 and active order by name_norm, maker_id limit ${MAKER_LIST_MAX}`)).rows;
}
/** 商品のメーカー (無い = null) = { maker_id, name, set_by, set_via, set_at } */
export async function readProductMaker(db, productId) {
  if (productId == null || !(await makerTablesExist(db))) return null;
  return (await db.query(`select x.maker_id::text as maker_id, m.name, x.set_by, x.set_via, x.set_at::text as set_at from core.product_makers x join core.makers m on m.maker_id = x.maker_id
     where x.product_id = $1`, [String(productId)])).rows[0] ?? null;
}
/** 商品ごとのメーカーの名前 (一覧・CSV) = Map<product_id, name> */
export async function makerNamesOf(db, productIds) {
  const ids = [...new Set((productIds || []).filter(Boolean).map(String))];
  if (!ids.length || !(await makerTablesExist(db))) return new Map();
  return new Map((await db.query(`select x.product_id::text as pid, m.name from core.product_makers x join core.makers m on m.maker_id = x.maker_id
     where x.product_id = any($1::bigint[])`, [ids])).rows.map((r) => [r.pid, r.name]));
}

const keyOf = (e) => { const m = /^([a-z_]+):\s*([\s\S]*)$/.exec(String((e && e.message) || '')); return m ? [m[1], m[2]] : ['', '']; };

/**
 * 名簿に足す (同じと見る名前が既にある = それを返す)。1 つの文 = autocommit で 1 つの取引。
 * 戻り値 { ok, maker: { id, name }, existed, replayed }。誤りは MasterWriteError
 */
export async function createMaker(db, { requestId, actor, name }) {
  const rid = String(requestId ?? '').trim().toLowerCase();
  if (!UUID_RE.test(rid)) throw new MasterWriteError(400, 'invalid_input', '保存の番号 (request_id) の形が違う。画面を開き直してください', { field: 'request_id' });
  const prob = makerNameProblem(name);
  if (prob) throw new MasterWriteError(400, 'invalid_input', MAKER_PROBLEM_WORDS[prob], { field: 'maker' });
  if (!(await makerReady(db))) throw new MasterWriteError(503, 'db_not_ready', 'Company DB に 0083 (メーカー) がまだ入っていないか、権限の流し直しの前です。何も保存していません', { field: 'maker' });
  try {
    const r = (await db.query('select ops.create_maker($1::uuid, $2, $3) as r', [rid, String(actor ?? '').trim().toLowerCase(), name])).rows[0].r;
    return { ok: true, maker: { id: String(r.maker_id), name: r.name }, existed: r.existed === true, replayed: r.replayed === true };
  } catch (e) {
    const [key, msg] = keyOf(e);
    if (e && e.code === '23505' && /(created_request_id|maker_requests_pkey)/.test(`${e.constraint || ''} ${e.message || ''}`)) {
      throw new MasterWriteError(409, 'request_id_reused', '同じ保存の番号 (request_id) で違う名前が来ました。画面を開き直してください', { field: 'maker' });
    }
    if (key === 'request_id_reused') throw new MasterWriteError(409, key, '同じ保存の番号 (request_id) で違う名前が来ました。画面を開き直してください', { field: 'maker' });
    if (key === 'before_cutover') throw new MasterWriteError(409, key, `切替前です: メーカーはまだ足せません (${msg})。何も保存していません`, { field: 'maker' });
    if (key === 'invalid_input') throw new MasterWriteError(400, key, `メーカーの名前が DB の決まりに合いません (${msg})。何も保存していません`, { field: 'maker' });
    throw e;
  }
}

/**
 * 商品のメーカーを付ける / 変える / 外す (呼び手の取引の中・鍵の後)。makerId = 番号 (null = 外す)・seen = 画面が見ていた今のメーカー (新商品 = null)。
 * 戻り値 = DB の答え ({ changed, maker_id, maker_name, from_maker_id, from_maker_name, … })。誤りは MasterWriteError (field = maker)
 */
export async function setProductMakerInTx(db, { requestId, actor, skuId, makerId = null, seen = null, via, reason = null, label = '' }) {
  const at = label ? `${label}: ` : '';
  const f = { field: 'maker', ...(label ? { code: label } : {}) };
  try {
    return (await db.query('select ops.set_product_maker($1::uuid, $2, $3::bigint, $4::bigint, $5::bigint, $6, $7) as r',
      [requestId, actor, String(skuId), makerId == null ? null : String(makerId), seen == null ? null : String(seen), via, reason || null])).rows[0].r;
  } catch (e) {
    const [key, msg] = keyOf(e);
    if (key === 'version_conflict') throw new MasterWriteError(409, key, `${at}画面を開いた後に、ほかの人がこの商品のメーカーを変えました。何も保存していません。画面を開き直してください`, f);
    if (key === 'maker_unknown') throw new MasterWriteError(400, key, `${at}選んだメーカーは名簿に無いか、使わないメーカーです。一覧から選び直してください。何も保存していません`, f);
    if (key === 'not_single' || key === 'cancelled') throw new MasterWriteError(400, key, `${at}${msg}。何も保存していません`, f);
    if (key === 'before_cutover') throw new MasterWriteError(409, key, `${at}切替前です: メーカーはまだ入れられません (${msg})。何も保存していません`, f);
    if (key === 'invalid_input') throw new MasterWriteError(400, key, `${at}メーカーの値が DB の決まりに合いません (${msg})。何も保存していません`, f);
    throw e;
  }
}

/** 選んだメーカーが名簿にある (使う) か (保存の前の確かめ・DB の関数も同じことをもう一度見る) */
export async function assertMakerUsable(db, makerId) {
  const r = (await db.query('select name, active from core.makers where company_id = 1 and maker_id = $1::bigint', [String(makerId)])).rows[0];
  if (!r || !r.active) throw new MasterWriteError(400, 'maker_unknown', '選んだメーカーは名簿に無いか、使わないメーカーです。一覧から選び直してください。何も保存していません', { field: 'maker' });
  return r.name;
}
