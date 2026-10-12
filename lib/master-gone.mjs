/**
 * master-gone.mjs — 業務で使える商品 (登録の状態 available) を「NE から消えた」でやめる (cancelled)。0070・中原さんの決定 10/11 Q1 = a
 *
 * やめてよいかは DB が自分で確かめる (ops.sku_gone_from_ne_check):
 *   完全で信用できる NE の取得 (照合 ② の ops.record_parent_gate の記録の回) に ops.ne_gone_required_runs() (= 3) 回続けて無い・一番新しい記録が新しい・
 *   今の NE のコードの台帳にも無い・単品・生きた NE 登録の CSV の品目が無い・ほかのセットの構成品 / 構成の依頼 / 販路の出品の構成に使われていない・ほかの商品の親でない
 * 動かすのは DB の関数 ops.cancel_sku_gone_from_ne だけ (1 件ずつ・1 つの取引・理由が要る・保存の記録と登録の状態の履歴が残る)。
 *   ここ (lib) は形を確かめて関数を呼ぶだけ (DB の関数が鍵・門・確かめ・約束を全部やり直す = 呼び手を信じない)
 * 使う所: apps/master-edit (商品の画面の「NE から消えた (やめる)」・名簿 MASTER_DECISION_APPROVERS の人) / scripts/company-db/master-gone-from-ne.mjs (CLI・master_ops)
 * 🆕 0072: セットもやめられる (DB の関数が同じ取引で構成 = core.sku_components を外す・答えの components_removed)。NE から消す依頼 (lib/master-ne-delete.mjs) の品目は「消えた」になる
 */
import crypto from 'node:crypto';
import { MasterWriteError } from './master-write.mjs';

export const GONE_CHECK_FN = 'ops.sku_gone_from_ne_check(bigint)';
export const GONE_LIST_FN = 'ops.sku_gone_from_ne_list(text[])';
export const GONE_CANCEL_FN = 'ops.cancel_sku_gone_from_ne(uuid, text, text, bigint)';
export const GONE_REASON_MAX = 200;
/** DB の確かめの理由 (ops.sku_gone_from_ne_check の problems[].reason) の短い言葉 (画面・CLI の見出し。文は DB の text) */
export const GONE_REASON_WORDS = Object.freeze({
  no_sku: '商品が無い', not_company: '会社が違う', not_available: '業務で使える商品でない', not_single: '単品でない', no_trusted_fetch: 'NE の取得の記録が無い',
  fetch_old: 'NE の取得の記録が古い', in_ne_latest: 'NE にある', absent_short: 'まだ回数が足りない', in_ne_codes: 'NE のコードの台帳にある',
  live_file: 'NE 登録の CSV が途中', set_component: 'セットの構成品', component_request: '構成の依頼に入っている', listing_component: '出品の構成に使われている', listing_component_deleted: '削除済みの出品の構成に入っている',
  parent_of_others: 'ほかの商品の親',
  own_component_request: 'このセットの構成の依頼が開いている',   // 🆕 0072 (セット)
});

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const CTRL_RE = new RegExp(`[\\x00-\\x1f\\x7f${String.fromCharCode(0x2028, 0x2029)}]`);
const bad = (message, reason = 'invalid_input', extra = {}) => new MasterWriteError(400, reason, message, extra);
const jsonOf = (v) => (typeof v === 'string' ? JSON.parse(v) : v);

/** DB の関数の誤り (「理由: 文」の形) → MasterWriteError の状態 */
const FN_STATUS = Object.freeze({ invalid_input: 400, not_found: 404, not_gone_from_ne: 409, request_id_reused: 409, before_cutover: 409,
  gone_session_required: 403, master_write_session_required: 403, master_write_session_mismatch: 403, master_write_session_exists: 409 });
async function callFn(db, sql, params) {
  try {
    return jsonOf((await db.query(sql, params)).rows[0].r);
  } catch (e) {
    const m = /^([a-z_]+): ([\s\S]*)$/.exec(String((e && e.message) || ''));
    if (m && e && ['P0001', 'P0002', '22023', '23505', '42501', '55000'].includes(e.code)) throw new MasterWriteError(FN_STATUS[m[1]] ?? 409, m[1], m[2]);
    throw e;
  }
}

/** 0070 の関数があるか (無い = 0070 の前の DB = 画面は何も出さない・CLI は止まる) */
export async function goneAvailable(db) {
  return (await db.query('select to_regprocedure($1) is not null as ok', [GONE_CHECK_FN])).rows[0]?.ok === true;
}

/**
 * 1 つの SKU の確かめ (読むだけ・画面)。0070 の前・実行権が無い (ロールの script を流していない)・読めない = null (画面は何も出さない)。
 * savepoint = 取引の中で読むとき (読めなくても取引を続ける)
 */
export async function readGoneCheck(db, skuId, { savepoint = false } = {}) {
  if (!db || skuId == null || !/^\d{1,19}$/.test(String(skuId))) return null;
  if (savepoint) await db.query('savepoint gone_check');
  try {
    if (!(await goneAvailable(db))) { if (savepoint) await db.query('release savepoint gone_check'); return null; }
    const r = jsonOf((await db.query('select ops.sku_gone_from_ne_check($1::bigint) as r', [String(skuId)])).rows[0].r);
    if (savepoint) await db.query('release savepoint gone_check');
    return r;
  } catch (e) {
    if (savepoint) { try { await db.query('rollback to savepoint gone_check'); await db.query('release savepoint gone_check'); } catch { /* */ } }
    console.error(`[master-gone] 確かめを読めない (SKU ${skuId}): ${String((e && e.message) || e).slice(0, 200)}`);
    return null;
  }
}

/**
 * 確かめの一覧 (読むだけ・CLI の dry-run)。codes = コードの配列 (500 件まで) / null = 候補 (available で、一番新しい完全な取得に無い SKU)。
 * 戻り値 = [ops.sku_gone_from_ne_check の答え] (コードの順)
 */
export async function listGoneChecks(db, codes = null) {
  if (codes != null) {
    if (!Array.isArray(codes) || codes.length === 0 || codes.length > 500) throw bad('コードは 1〜500 件');
    for (const c of codes) if (typeof c !== 'string' || !c.trim() || c.length > 200 || CTRL_RE.test(c)) throw bad(`コードの形が違う: ${String(c).slice(0, 40)}`);
  }
  if (!(await goneAvailable(db))) throw new MasterWriteError(409, 'not_applied', 'Company DB に 0070 (NE から消えた商品をやめる) がまだ無い');
  return callFn(db, 'select ops.sku_gone_from_ne_list($1::text[]) as r', [codes == null ? null : codes.map((c) => c.trim())]);
}

/** 理由の形 (前後の空白を除いて 1〜200 字・改行と制御文字なし) */
export function goneReasonOf(v) {
  const s = String(v ?? '').trim();
  if (!s) throw bad('理由を書いてください (なぜ NE から消えたか・例: 別のコード xxx で作り直した)', 'invalid_input', { field: 'reason' });
  if (s.length > GONE_REASON_MAX || CTRL_RE.test(s)) throw bad(`理由は ${GONE_REASON_MAX} 字まで (改行なし)`, 'invalid_input', { field: 'reason' });
  return s;
}
function actorOf(v) {
  const a = String(v ?? '').trim().toLowerCase();
  if (!a || a.length > 320 || CTRL_RE.test(a)) throw bad('操作する人 (ログインのメール) が分からない');
  return a;
}
/** この操作 1 回の番号 (保存の記録の request_id)。送られない・形が違う = 新しく作る */
export const goneRequestId = (v) => { const s = String(v ?? '').trim().toLowerCase(); return UUID_RE.test(s) ? s : crypto.randomUUID(); };

/**
 * やめる (1 件・1 つの取引)。input = { actor, code | skuId, reason, requestId, confirm (画面 = 「NE で消えたのを確かめた」の印・true が要る) }
 * opts = { open (画面 = MASTER_EDIT_OPEN・CLI = true), requireConfirm (画面 = true), inTx (CLI = 呼び手の取引の中で呼ぶ = begin / commit をしない) }
 * 戻り値 = DB の関数の答え { ok, request_id, code, sku_id, from, state, event_id, evidence, replayed? }
 */
export async function cancelSkuGoneFromNe(db, input, { open = false, requireConfirm = true, inTx = false } = {}) {
  const actor = actorOf(input?.actor);
  const reason = goneReasonOf(input?.reason);
  if (requireConfirm && input?.confirm !== true) throw bad('「NE で消えたのを確かめた」に印を付けてください', 'invalid_input', { field: 'confirm' });
  if (open !== true) throw new MasterWriteError(409, 'before_cutover', '切替前です: この画面の保存はまだ開いていません (MASTER_EDIT_OPEN なし)。何も変えていません');
  const requestId = goneRequestId(input?.requestId);
  const code = input?.code == null ? null : String(input.code).trim();
  const skuIdIn = input?.skuId == null ? null : String(input.skuId);
  if (code == null && skuIdIn == null) throw bad('商品コードが要る');
  if (code != null && (!code || code.length > 60 || CTRL_RE.test(code))) throw bad('商品コードの形が違う');
  if (skuIdIn != null && !/^\d{1,19}$/.test(skuIdIn)) throw bad('SKU の番号の形が違う');
  if (!inTx) await db.query('begin');
  try {
    if (!(await goneAvailable(db))) throw new MasterWriteError(409, 'not_applied', 'Company DB に 0070 (NE から消えた商品をやめる) がまだ無い');
    // SKU の番号 = そのまま DB の関数へ (CLI の master_ops は core.skus を読めない = 有無は関数が確かめる) / コード = 画面のロールが番号を引く
    let id = skuIdIn;
    if (id == null) {
      const row = (await db.query('select sku_id::text as id from core.skus where company_id = 1 and code_norm = core.norm_code($1)', [code])).rows[0];
      if (!row) throw new MasterWriteError(404, 'not_found', `商品コード ${code} は Company DB にありません`);
      id = row.id;
    }
    const r = await callFn(db, 'select ops.cancel_sku_gone_from_ne($1::uuid, $2, $3, $4::bigint) as r', [requestId, actor, reason, id]);
    if (!inTx) await db.query('commit');
    return r;
  } catch (e) {
    if (!inTx) { try { await db.query('rollback'); } catch { /* 接続が切れた = 元の誤りを上げる */ } }
    throw e;
  }
}

/** 画面: 確かめの答え → 見せ方 (show = パネルを出すか・canGo = 押せるか (権限・門は別)・why = 押せない理由の文) */
export function goneView(chk) {
  // 🆕 0072: セットも (決定 10/12 Q4 b・やめるときに構成を外す)
  if (!chk || chk.reg_state !== 'available' || (chk.sku_kind !== 'single' && chk.sku_kind !== 'set')) return { show: false };
  const problems = Array.isArray(chk.problems) ? chk.problems : [];
  const inNe = problems.some((p) => p.reason === 'in_ne_latest' || p.reason === 'no_trusted_fetch');
  if (inNe && !chk.ok) return { show: false };   // NE にある (ふつうの商品) = 何も出さない
  return {
    show: true, canGo: chk.ok === true, streak: Number(chk.streak || 0), required: Number(chk.required_runs || 0),
    latest: chk.latest || null, since: chk.absent_since || null,
    why: problems.map((p) => ({ reason: p.reason, word: GONE_REASON_WORDS[p.reason] || p.reason, text: p.text })),
  };
}

/** キーの順をそろえた JSON (証拠のハッシュ用) */
export function stableJson(v) {
  if (v === null || typeof v !== 'object') return JSON.stringify(v ?? null);
  if (Array.isArray(v)) return `[${v.map(stableJson).join(',')}]`;
  return `{${Object.keys(v).sort().map((k) => `${JSON.stringify(k)}:${stableJson(v[k])}`).join(',')}}`;
}
/** 1 件の証拠の行 (計画のハッシュの材料) = [sku_id, 最初に無かった記録の回, 最後に無かった記録の回, 証拠の全部] */
export const goneEvidenceLine = (skuId, evidence) => [String(skuId), String(evidence?.first_absent?.result_id ?? ''), String(evidence?.last_absent?.result_id ?? ''), stableJson(evidence ?? null)];
/**
 * 🆕 #1683 Codex R1 Medium: やめてよい件の計画のハッシュ (CLI の dry-run が出し、--apply は同じハッシュでなければ 1 件も変えない)。
 *   材料 = やめてよい件ごとの goneEvidenceLine を sku_id の順に並べた JSON の sha256 = 対象の集合と証拠 (最初・最後の回・日数・取得の時刻) が全部同じときだけ同じ
 */
export function gonePlanHash(checks) {
  const lines = (checks || []).filter((r) => r && r.ok === true).map((r) => goneEvidenceLine(r.sku_id, r.evidence))
    .sort((a, b) => (BigInt(a[0]) < BigInt(b[0]) ? -1 : BigInt(a[0]) > BigInt(b[0]) ? 1 : 0));
  return crypto.createHash('sha256').update(JSON.stringify(lines)).digest('hex');
}
