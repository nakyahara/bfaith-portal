/**
 * master-draft-cancel.mjs — 新商品の登録の下書きで、まだ NE に配っていない商品を消す (= 登録をやめた cancelled)。0071・中原さんの頼み 10/11 の場面 a
 *
 * 消してよいかは DB が自分で確かめる (ops.sku_draft_cancel_check):
 *   登録の状態が下書き・NE 登録の CSV を一度も配っていない・NE に一度も現れていない・まとまりの子でない (= 「子の廃止」)・ほかの商品の親でない・
 *   ほかのセットの構成品 / 開いている構成の依頼 / 販路の出品の構成 (削除済みの出品も) に使われていない・カードを取り込み中でない
 * 消すのは DB の関数 ops.cancel_sku_draft だけ (1 件ずつ・1 つの取引・理由が要る)。同じ取引で 作っただけの NE 登録の CSV を使わないにする・セットの構成の依頼を取り下げる・
 *   JAN を外す・まだ取り込んでいないカードの知らせを「作らない」で済ませる・登録の状態を cancelled・保存の記録 done。
 *   ここ (lib) は形と門 (段階・DB の active・MASTER_EDIT_OPEN・夜間の取り込み) を確かめて関数を呼ぶだけ (DB の関数が鍵・門・確かめ・約束を全部やり直す = 呼び手を信じない)
 * 🚨 行は消さない (SKU・商品・原価・仕入先・履歴は残る) = 消したコードはもう使えない (code_taken)。値の入れ間違いなら消さずに直す
 * 🚨 product-hub のカード (Render の SQLite) は Postgres と 1 つの取引にできない = 画面 (apps/master-edit/router.mjs) が commit の後に
 *    apps/product-hub/services/cdb-card-intake.js の excludeCdbCardsForCancelledSku で「除外」にする (失敗しても消したのは成功のまま・画面に出す)
 * 使う所: apps/master-edit (商品の画面の「この下書きを消す」・名簿 MASTER_EDITORS の人)
 */
import crypto from 'node:crypto';
import { MasterWriteError, ownerGateError, lockMasterWriteShared, COMPANY_ID, SOURCE_SYSTEM } from './master-write.mjs';
import { baseGateInTx, lockCutoverSharedInTx } from './master-owner-gate.mjs';

export const DRAFT_CHECK_FN = 'ops.sku_draft_cancel_check(bigint)';
export const DRAFT_CANCEL_FN = 'ops.cancel_sku_draft(uuid, text, text, bigint)';
export const DRAFT_REASON_MAX = 200;
/** DB の確かめの理由 (ops.sku_draft_cancel_check の problems[].reason) の短い言葉 (画面の見出し。文は DB の text) */
export const DRAFT_REASON_WORDS = Object.freeze({
  no_sku: '商品が無い', not_company: '会社が違う', not_draft: '下書きでない', ever_issued: 'NE 登録の CSV を配った', seen_in_ne: 'NE に現れた',
  variation_child: 'まとまりの子', parent_of_others: 'ほかの商品の親', set_component: 'セットの構成品', component_request: '構成の依頼に入っている',
  listing_component: '出品の構成に使われている', listing_component_deleted: '削除済みの出品の構成に入っている', card_busy: 'カードを作っている途中',
});

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const CTRL_RE = new RegExp(`[\\x00-\\x1f\\x7f${String.fromCharCode(0x2028, 0x2029)}]`);
const bad = (message, reason = 'invalid_input', extra = {}) => new MasterWriteError(400, reason, message, extra);
const jsonOf = (v) => (typeof v === 'string' ? JSON.parse(v) : v);

/** DB の関数の誤り (「理由: 文」の形) → MasterWriteError の状態 */
const FN_STATUS = Object.freeze({ invalid_input: 400, not_found: 404, not_cancellable: 409, request_id_reused: 409, before_cutover: 409,
  master_write_session_required: 403, master_write_session_mismatch: 403, master_write_session_exists: 409 });
function fnError(e) {
  if (e instanceof MasterWriteError) return e;
  const m = /^([a-z_]+): ([\s\S]*)$/.exec(String((e && e.message) || ''));
  if (m && e && ['P0001', 'P0002', '22023', '23505', '42501', '55000'].includes(e.code)) {
    const words = m[1] === 'not_cancellable' ? `消せません: ${m[2]}。何もしていません` : m[1] === 'before_cutover' ? `切替前です (DB が断った: ${m[2]})。何もしていません` : m[2];
    return new MasterWriteError(FN_STATUS[m[1]] ?? 409, m[1], words);
  }
  if (e && e.code === '23505' && /master_edit_requests_pkey/.test(String(e.constraint || e.message || ''))) {
    return new MasterWriteError(409, 'request_id_reused', '同じ操作の番号 (request_id) がちょうど使われました。画面を開き直してください');
  }
  return e;
}

/** 0071 の関数があるか (無い = 0071 の前の DB = 画面は何も出さない) */
export async function draftCancelAvailable(db) {
  return (await db.query('select to_regprocedure($1) is not null as ok', [DRAFT_CHECK_FN])).rows[0]?.ok === true;
}

/**
 * 1 つの SKU の確かめ (読むだけ・画面)。0071 の前・実行権が無い (ロールの script を流していない)・読めない = null (画面は何も出さない)。
 * savepoint = 取引の中で読むとき (読めなくても取引を続ける)
 */
export async function readDraftCancelCheck(db, skuId, { savepoint = false } = {}) {
  if (!db || skuId == null || !/^\d{1,19}$/.test(String(skuId))) return null;
  if (savepoint) await db.query('savepoint draft_cancel_check');
  try {
    if (!(await draftCancelAvailable(db))) { if (savepoint) await db.query('release savepoint draft_cancel_check'); return null; }
    const r = jsonOf((await db.query('select ops.sku_draft_cancel_check($1::bigint) as r', [String(skuId)])).rows[0].r);
    if (savepoint) await db.query('release savepoint draft_cancel_check');
    return r;
  } catch (e) {
    if (savepoint) { try { await db.query('rollback to savepoint draft_cancel_check'); await db.query('release savepoint draft_cancel_check'); } catch { /* */ } }
    console.error(`[master-draft-cancel] 確かめを読めない (SKU ${skuId}): ${String((e && e.message) || e).slice(0, 200)}`);
    return null;
  }
}

/** 理由の形 (前後の空白を除いて 1〜200 字・改行と制御文字なし) */
export function draftReasonOf(v) {
  const s = String(v ?? '').trim();
  if (!s) throw bad('理由を書いてください (例: コードを打ち間違えた・発売が中止になった)', 'invalid_input', { field: 'reason' });
  if (s.length > DRAFT_REASON_MAX || CTRL_RE.test(s)) throw bad(`理由は ${DRAFT_REASON_MAX} 字まで (改行なし)`, 'invalid_input', { field: 'reason' });
  return s;
}
function actorOf(v) {
  const a = String(v ?? '').trim().toLowerCase();
  if (!a || a.length > 320 || CTRL_RE.test(a)) throw bad('操作する人 (ログインのメール) が分からない');
  return a;
}
/** この操作 1 回の番号 (保存の記録の request_id)。送られない・形が違う = 新しく作る */
export const draftRequestId = (v) => { const s = String(v ?? '').trim().toLowerCase(); return UUID_RE.test(s) ? s : crypto.randomUUID(); };

/**
 * 消す (1 件・1 つの取引)。input = { actor, code, reason, requestId, confirm (画面 = 確かめの窓で押した = true が要る) }
 * opts = { open (MASTER_EDIT_OPEN), requireConfirm (既定 true) }
 * 門 (今の保存と同じ): 段階 (共有の鍵) → 段階 new_open・DB の active・MASTER_EDIT_OPEN (lib/master-owner-gate.mjs の baseGateInTx) → 夜間の取り込み (マスタの書き込みの共有の鍵) → DB の関数
 * 戻り値 = DB の関数の答え { ok, request_id, code, sku_id, from, state, event_id, cleanup: { reg_csv, card, jans, component_request }, replayed? }
 */
export async function cancelSkuDraft(db, input, { open = false, requireConfirm = true } = {}) {
  const actor = actorOf(input?.actor);
  const reason = draftReasonOf(input?.reason);
  if (requireConfirm && input?.confirm !== true) throw bad('確かめの窓で「消す」を押してください', 'invalid_input', { field: 'confirm' });
  const requestId = draftRequestId(input?.requestId);
  const code = String(input?.code ?? '').trim();
  if (!code || code.length > 60 || CTRL_RE.test(code)) throw bad('商品コードの形が違う');
  await db.query('begin');
  try {
    // 変更の記録 (0026) の 誰が・request_id・理由 = 画面のロールは約束の行から (DB の関数が書く)。持ち主のロールの読みのときのためにも同じ値を置く
    await db.query(`select set_config('core.actor_type', 'human', true), set_config('core.actor_id', $1, true), set_config('core.source_system', $2, true),
        set_config('core.request_id', $3, true), set_config('core.reason', $4, true), set_config('core.run_id', '', true)`, [actor, SOURCE_SYSTEM, requestId, reason]);
    await lockCutoverSharedInTx(db);
    const gate = await baseGateInTx(db, { open: open === true, closedWhy: 'この画面の保存はまだ開いていません' });
    if (!gate.ok) throw ownerGateError(gate, (why) => new MasterWriteError(409, 'before_cutover', `切替前です (${why})。何もしていません`));
    await lockMasterWriteShared(db);
    if (!(await draftCancelAvailable(db))) throw new MasterWriteError(409, 'not_applied', 'Company DB に 0071 (下書きを消す) がまだ無い。何もしていません');
    const row = (await db.query('select sku_id::text as id from core.skus where company_id = $1 and code_norm = core.norm_code($2)', [COMPANY_ID, code])).rows[0];
    if (!row) throw new MasterWriteError(404, 'not_found', `商品コード ${code} は Company DB にありません`);
    const r = jsonOf((await db.query('select ops.cancel_sku_draft($1::uuid, $2, $3, $4::bigint) as r', [requestId, actor, reason, row.id])).rows[0].r);
    await db.query('commit');
    return r;
  } catch (e) {
    try { await db.query('rollback'); } catch { /* 接続が切れた = 元の誤りを上げる */ }
    throw fnError(e);
  }
}

/** 画面: 確かめの答え → 見せ方 (show = 箱を出すか・canGo = 押せるか (権限・門は別)・why = 押せない理由・will = 一緒に片付ける物の文) */
export function draftCancelView(chk) {
  if (!chk || chk.reg_state !== 'draft') return { show: false };
  const problems = Array.isArray(chk.problems) ? chk.problems : [];
  const c = chk.cleanup || {};
  const will = [];
  for (const f of Array.isArray(c.reg_csv) ? c.reg_csv : []) {
    will.push(`作っただけの NE 登録の CSV (ファイル #${f.export_id}) を「使わない」にします${Array.isArray(f.codes) && f.codes.length ? ` (一緒に入っている ${f.codes.join('・')} は CSV を作り直します)` : ''}`);
  }
  if (c.component_request) will.push(`このセットの構成の依頼 (#${c.component_request}) を取り下げます`);
  if (Array.isArray(c.jans) && c.jans.length) will.push(`JAN ${c.jans.join('・')} を外します (正しいコードで登録し直すときに使えます)`);
  if (c.card && c.card.status !== 'done') will.push('product-hub の出品カードはまだ作っていないので、もう作りません');
  if (c.card && c.card.status === 'done' && c.card.draft_id != null) will.push(`product-hub の出品カード (#${c.card.draft_id}) を「除外」にします (出品済みのカードはそのまま)`);
  const variation = problems.some((p) => p.reason === 'variation_child');
  return {
    show: true, canGo: chk.ok === true, variation, will,
    why: problems.map((p) => ({ reason: p.reason, word: DRAFT_REASON_WORDS[p.reason] || p.reason, text: p.text })),
  };
}
