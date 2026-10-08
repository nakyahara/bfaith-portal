/**
 * master-reg-parent.mjs — 新商品 (下書きの単品) の「色違い・サイズ違いの代表」(migration 0061・2026-10-08 中原さんの決定 a)
 *
 * 代表の持ち主は NE のまま (config/master-ownership.mjs の products.parent = load)。ここでは Company DB の親 (core.products.parent_product_id) に書かない:
 *   新商品の登録 (画面 D) / 下書きの間の商品の画面で代表を選ぶ → ops.registration_parents (書くのは DB の関数 ops.set_registration_parent だけ)
 *   → NE 登録の CSV の代表の列 (daihyo_syohin_code = NE の元の書き方) と翌朝の照合 ② の expected.parent (lib/master-reg-csv.mjs・ops.ne_reg_canonical)
 *   → NE に取り込む → 夜間ロードが NE の代表商品コードから Company DB の親を付ける → 照合 ② が NE の代表と比べる (違えば partial)
 * 代表 = NE の代表商品コード = 色違い・サイズ違いのまとまりの「名札」(Company DB では SKU を持たない product・display_code = 名札)。
 *   実在する単品のコードのこともある (その単品が親)。ほかのまとまりに入っている単品は選ばない (そのまとまりの名札を選ぶ = 2 段にしない)
 * 新しい名札 (NE にまだ無いコード) はここでは作らない: NE 登録の CSV は NE の元のコードにある代表だけ書く (無ければ止まる理由)。
 *   最初の色違いを登録するときは、元の商品 (単品) を代表に選ぶ (NE はその単品のコードを代表商品コードに持てる)
 */
import { normSku } from './sku-norm.js';
import { MasterWriteError, ownerGateError, textIn, blank, sha256, COMPANY_ID, REASON_MAX, REQUEST_LOCK_SQL, lockMasterWriteShared } from './master-write.mjs';
import { baseGateInTx, lockCutoverSharedInTx } from './master-owner-gate.mjs';

export const REG_PARENT_LABEL = '代表 (色違い・サイズ違い)';
/** NE のコードの形 (0041 の ops.master_ne_codes の ne_code と同じ) */
export const PARENT_CODE_RE = /^[A-Za-z0-9_-]{1,30}$/;
export const PARENT_SEARCH_MAX = 20;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const CTRL_RE = new RegExp(`[\\x00-\\x1f\\x7f${String.fromCharCode(0x2028, 0x2029)}]`);
const bad = (message, reason = 'invalid_input', extra = {}) => new MasterWriteError(400, reason, message, { field: 'variation_parent', ...extra });

async function regclass(db, name) {
  return (await db.query('select to_regclass($1) is not null as ok', [name])).rows[0].ok;
}

/** 画面から来た代表のコード (空 = 代表なし = null)。形だけ見る (Company DB にあるかは DB の関数が見る) */
export function parentCodeIn(v) {
  if (blank(v)) return null;
  if (typeof v !== 'string') throw bad('代表のコードは文字で入れてください');
  const s = v.trim();
  if (!PARENT_CODE_RE.test(s)) throw bad(`代表のコード ${s.slice(0, 40)} は NE のコードの形 (英数字・- と _ の 30 字まで) ではありません`);
  return s;
}
/** 登録の中の代表の約束の request_id (登録の request_id から決まる = 同じ登録の押し直しは同じ番号。JAN の janRequestId と同じ作り方) */
export function regParentRequestId(requestId) {
  const h = sha256(`${requestId}:variation_parent`);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20, 32)}`;
}

/** DB の関数の誤り → 画面の誤り (400 / 409)。それ以外は投げ直す */
export function mapParentError(e) {
  const msg = String((e && e.message) || '');
  const m = /^([a-z_]+): ([\s\S]*)$/.exec(msg);
  if (!m) return e;
  const [, key, text] = m;
  if (['parent_not_found', 'parent_ambiguous', 'parent_not_single', 'parent_nested', 'invalid_value'].includes(key)) return bad(`${text}。何も保存していません`, key === 'invalid_value' ? 'invalid_input' : key);
  if (['not_draft', 'reg_csv_issued', 'version_conflict', 'before_cutover'].includes(key)) return new MasterWriteError(409, key, `${text}。何も保存していません`, { field: 'variation_parent' });
  if (key === 'not_found') return new MasterWriteError(404, 'not_found', text);
  return e;
}

/**
 * DB の関数 ops.set_registration_parent を呼ぶ (取引の中・鍵は関数が取る)。今の取引に約束 (登録の約束など) が残っていれば外してから。
 * 戻り値 = 関数の結果 { ok, code, changed, variation_parent, superseded } / 変わらない = { ok, no_change }
 */
export async function callSetRegistrationParent(db, { requestId, actor, reason = null, ownership, skuId, seen = null, code = null }) {
  // 0061 の前の DB (コードを先に配った) = 何も書かずに分かる誤り (登録ごと巻き戻る)
  if (!(await db.query(`select to_regprocedure('ops.set_registration_parent(uuid, text, text, jsonb, bigint, text, text)') is not null as ok`)).rows[0].ok) {
    throw new MasterWriteError(409, 'not_applied', '色違い・サイズ違いの代表は、Company DB の準備 (migration 0061) の後に使えます。代表を空にすれば登録できます。何も保存していません', { field: 'variation_parent' });
  }
  await db.query(`select set_config('ops.master_write_session', '', true)`);
  try {
    return (await db.query('select ops.set_registration_parent($1::uuid, $2, $3, $4::jsonb, $5::bigint, $6, $7) as r',
      [requestId, actor, reason, JSON.stringify(ownership), String(skuId), seen, code])).rows[0].r;
  } catch (e) {
    throw mapParentError(e);
  }
}

/**
 * 下書きの間の商品の画面で代表を決める / 外す (1 つの取引)。input = { actor, requestId, code (商品のコード), seen (画面が見ていた代表・無し = null), parent (空 = 外す), reason? }
 * 鍵の順 (JAN と同じ): request_id の鍵 → 段階の共有の鍵 → (門) → マスタの書き込みの共有の鍵 → (関数の中) SKU → CSV → 行
 * 同じ request_id = 残した結果 (中身が違えば 409)。opts = { open (MASTER_EDIT_OPEN) }
 */
export async function setRegistrationParent(db, input, opts = {}) {
  const actor = String(input?.actor ?? '').trim().toLowerCase();
  if (!actor || actor.length > 320 || CTRL_RE.test(actor)) throw bad('保存する人 (ログインのメール) が分からない', 'invalid_input', { field: 'actor' });
  const requestId = String(input?.requestId ?? '').trim().toLowerCase();
  if (!UUID_RE.test(requestId)) throw bad('保存の番号 (request_id) の形が違う。画面を開き直してください', 'invalid_input', { field: 'request_id' });
  const code = String(input?.code ?? '').trim();
  if (!code || code.length > 60) throw new MasterWriteError(400, 'invalid_input', '商品のコードが要ります');
  const seen = blank(input?.seen) ? null : String(input.seen).trim();
  const parent = parentCodeIn(input?.parent);
  const reason = textIn(input?.reason, { label: '理由', field: 'reason', max: REASON_MAX });
  await db.query('begin');
  try {
    await db.query(REQUEST_LOCK_SQL, [requestId]);
    const prev = (await db.query('select actor_id, operation, status, result, target_code from ops.master_edit_requests where request_id = $1', [requestId])).rows[0];
    if (prev) {
      await db.query('rollback');
      const to = prev.result?.changed?.[0]?.to ?? null;
      if (prev.actor_id !== actor || prev.operation !== 'reg_parent_set' || normSku(prev.target_code ?? '') !== normSku(code) || normSku(to ?? '') !== normSku(parent ?? '')) {
        throw new MasterWriteError(409, 'request_id_reused', '同じ保存の番号 (request_id) で違う中身が来ました。画面を開き直してください');
      }
      return { ...prev.result, replayed: true };
    }
    await lockCutoverSharedInTx(db);
    const gate = await baseGateInTx(db, { open: opts.open === true });
    if (!gate.ok) throw ownerGateError(gate, (why) => new MasterWriteError(409, 'before_cutover', `切替前です (${why})。何も保存していません`, { field: 'variation_parent' }));
    await lockMasterWriteShared(db);
    const sku = (await db.query('select sku_id::text as id from core.skus where company_id = $1 and code_norm = core.norm_code($2)', [COMPANY_ID, code])).rows[0];
    if (!sku) throw new MasterWriteError(404, 'not_found', `商品コード ${code} は Company DB にありません`);
    const r = await callSetRegistrationParent(db, { requestId, actor, reason, ownership: gate.ownership, skuId: sku.id, seen, code: parent });
    await db.query('commit');
    return r;
  } catch (e) {
    try { await db.query('rollback'); } catch { /* 接続が切れていれば rollback も失敗する */ }
    throw e;
  }
}

// ─── 読む (候補を探す・商品の画面) ───
/**
 * NE の元のコード (0041) で代表がそこにあるか: 名札 (rep) を先に・無ければ商品 (product)。ops.ne_reg_canonical と同じ選び方。
 * 戻り値 Map<norm, { state, ne_code }>。関数が使えない (0053 の前・権限) = null (分からない)
 */
async function neStatesOf(db, norms) {
  const list = [...new Set(norms.filter(Boolean))];
  if (!list.length) return new Map();
  const ok = (await db.query(`select case when to_regprocedure('ops.ne_reg_ne_codes(text[])') is null then false else has_function_privilege(to_regprocedure('ops.ne_reg_ne_codes(text[])'), 'execute') end as ok`)).rows[0].ok;
  if (!ok) return null;
  const r = (await db.query('select ops.ne_reg_ne_codes($1::text[]) as r', [list])).rows[0].r;
  const m = new Map();
  for (const e of r.entries || []) {
    const cur = m.get(e.code_norm);
    if (!cur || (e.kind === 'rep' && cur.kind !== 'rep')) m.set(e.code_norm, { kind: e.kind, state: e.state, ne_code: e.ne_code ?? null });
  }
  return m;
}
const neView = (states, norm) => {
  if (!states) return { known: false, ok: null, ne_code: null };
  const x = states.get(norm);
  return { known: true, ok: !!x && x.state === 'ok', ne_code: x && x.state === 'ok' ? x.ne_code : null, state: x ? x.state : 'absent' };
};
/** 選べない理由 (無ければ null) */
function whyNot(c) {
  if (!PARENT_CODE_RE.test(c.code)) return 'NE のコードの形 (英数字・- _ の 30 字まで) でない';
  if (c.ne.known && !c.ne.ok) return c.ne.state === 'absent' ? 'NE にまだ無い (NE 登録の CSV を作れない)' : 'NE のコードの書き方が 1 つに決まらない';
  return null;
}

/** 代表の候補の中身 (名前・子の数・子の例) を product の番号から */
async function childrenOf(db, productIds) {
  if (!productIds.length) return new Map();
  return new Map((await db.query(`select c.parent_product_id::text as pid, count(*)::int as n, (array_agg(k.code order by k.code_norm))[1:3] as sample
       from core.products c join core.skus k on k.product_id = c.product_id
      where c.parent_product_id = any($1::bigint[]) group by c.parent_product_id`, [productIds])).rows.map((r) => [r.pid, { n: r.n, sample: r.sample || [] }]));
}

/**
 * 代表の候補を探す (コードの前の方・名前の一部)。名札 (SKU を持たない商品) と、まとまりに入っていない単品。
 * まとまりに入っている単品に当たったら、そのまとまりの名札を返す (via = 当たった単品)。self = 登録する商品 (自分は出さない)
 * 戻り値 [{ code, name, kind: 'tag' | 'single', children, sample, via, ne: { known, ok, ne_code }, disabled: 理由 | null }]
 */
export async function searchVariationParents(db, q, { self = null, limit = PARENT_SEARCH_MAX } = {}) {
  const text = String(q ?? '').trim();
  if (!text || text.length > 60 || CTRL_RE.test(text)) return [];
  const norm = normSku(text);
  const like = (s) => s.replace(/[\\%_]/g, (c) => `\\${c}`);
  const selfNorm = self ? normSku(self) : '';
  const rows = (await db.query(`with hits as (
      select p.product_id, p.display_code as code, p.name, 'tag'::text as kind, null::text as via,
             (core.norm_code(p.display_code) = $1) as exact, (core.norm_code(p.display_code) like $2 escape '\\') as pre
        from core.products p
       where p.company_id = $5 and p.display_code is not null and not exists (select 1 from core.skus k where k.product_id = p.product_id)
         and (core.norm_code(p.display_code) like $2 escape '\\' or p.name ilike $3 escape '\\')
      union all
      select coalesce(pp.product_id, p.product_id), coalesce(pp.display_code, k.code), coalesce(pp.name, p.name),
             case when pp.product_id is null then 'single' when exists (select 1 from core.skus k2 where k2.product_id = pp.product_id) then 'single' else 'tag' end,
             case when pp.product_id is not null then k.code end,
             k.code_norm = $1, k.code_norm like $2 escape '\\'
        from core.skus k join core.products p on p.product_id = k.product_id left join core.products pp on pp.product_id = p.parent_product_id
       where k.company_id = $5 and k.sku_kind = 'single' and k.code_norm <> $4
         and (k.code_norm like $2 escape '\\' or k.name ilike $3 escape '\\'))
    select distinct on (product_id) product_id::text as product_id, code, name, kind, via, exact, pre from hits
     where core.norm_code(code) <> $4
     order by product_id, exact desc, pre desc, (via is null) desc
     limit 200`, [norm, `${like(norm)}%`, `%${like(text)}%`, selfNorm, COMPANY_ID])).rows;
  const kids = await childrenOf(db, rows.map((r) => r.product_id));
  const states = await neStatesOf(db, rows.map((r) => normSku(r.code)));
  const out = rows.map((r) => {
    const k = kids.get(r.product_id) || { n: 0, sample: [] };
    const c = { code: r.code, name: r.name, kind: r.kind, children: k.n, sample: k.sample, via: r.via, ne: neView(states, normSku(r.code)), exact: r.exact, pre: r.pre };
    c.disabled = whyNot(c);
    return c;
  });
  out.sort((a, b) => (b.exact - a.exact) || (b.pre - a.pre) || (!!a.disabled - !!b.disabled) || (b.children - a.children) || (a.code < b.code ? -1 : a.code > b.code ? 1 : 0));
  return out.slice(0, Math.max(1, Math.min(limit, PARENT_SEARCH_MAX))).map(({ exact, pre, ...c }) => c);
}

/**
 * 商品の画面: 登録の代表 (ops.registration_parents) と、Company DB の親 (夜間ロードが NE から付けた) が同じか。
 * 戻り値 null (表が無い DB = 0061 の前) / { code: null } (代表なし) / { code, name, kind, children, sample, ne, set_by, set_at, cdb_parent, matches }
 */
export async function readRegistrationParent(db, skuId) {
  if (!(await regclass(db, 'ops.registration_parents'))) return null;
  const r = (await db.query(`select x.parent_code, x.set_by, x.set_at::text as set_at, pp.display_code as cdb_parent
       from core.skus k left join core.products p on p.product_id = k.product_id left join core.products pp on pp.product_id = p.parent_product_id
       left join ops.registration_parents x on x.sku_id = k.sku_id where k.sku_id = $1`, [skuId])).rows[0];
  if (!r || !r.parent_code) return { code: null, cdb_parent: r?.cdb_parent ?? null };
  const norm = normSku(r.parent_code);
  const t = (await db.query(`select p.product_id::text as product_id, p.name, case when exists (select 1 from core.skus k where k.product_id = p.product_id) then 'single' else 'tag' end as kind
       from core.products p where p.company_id = $1 and core.norm_code(p.display_code) = $2
      union all select k.product_id::text, p.name, 'single' from core.skus k join core.products p on p.product_id = k.product_id
       where k.company_id = $1 and k.code_norm = $2 and k.sku_kind = 'single' limit 1`, [COMPANY_ID, norm])).rows[0];
  const kids = t ? (await childrenOf(db, [t.product_id])).get(t.product_id) : null;
  const states = await neStatesOf(db, [norm]);
  return {
    code: r.parent_code, name: t?.name ?? null, kind: t?.kind ?? 'missing', children: kids?.n ?? 0, sample: kids?.sample ?? [],
    ne: neView(states, norm), set_by: r.set_by, set_at: r.set_at,
    cdb_parent: r.cdb_parent ?? null, matches: r.cdb_parent == null ? null : normSku(r.cdb_parent) === norm,
  };
}

/** NE 登録の CSV の材料 (lib/master-reg-csv.mjs の regMaterialOf): 登録の代表のコード (無い・表が無い DB = null) */
export async function registrationParentCode(db, skuId) {
  if (!(await regclass(db, 'ops.registration_parents'))) return null;
  return (await db.query('select parent_code from ops.registration_parents where sku_id = $1', [skuId])).rows[0]?.parent_code ?? null;
}
