/**
 * master-draft-in-ne.mjs — 「NE にあるので登録済みにする」(0098・2026-10-20 中原さんの承認)
 *
 * 新商品の下書き (NE 登録待ち) で、NE にもう同じ商品がある (NE の画面で先に登録した・NE 登録の CSV を配った後に Company DB の値を直した = 一部違う partial のまま) ものを
 * NE 確認済みにする。登録済みにした後は、ふつうの NE 確認済みの商品と同じ = Company DB と NE の違い (名前ほか) は「NEへ送る」に出る。
 * 決めるのは DB (ops.draft_in_ne_check・ops.confirm_draft_in_ne = security definer・鍵の後にもう一度確かめる):
 *   今朝の NE の取得にそのコードがある (大文字・小文字も同じ)・区分 (単品 / セット) が同じ・生きている NE 登録の CSV の品目は一部違う 1 つだけ (消さずに「使わない」=
 *   resolved_by_human にして記録を残す)・代表 (色違いのまとまり) は NE と同じ (違えば断る = 先に代表をそろえる)・セットの中身は NE と同じ。ロジザードのコードには触らない
 * ここ (lib) は形と門 (段階・DB の active・MASTER_EDIT_OPEN・夜間の取り込み) を確かめて関数を呼ぶだけ (呼び手を信じない = DB が全部やり直す)
 * 使う所: apps/master-edit (商品の画面の箱・「NE 登録の CSV」の画面の「NE にもうある商品」「直すもの」の行ごとのボタン。押せるのは名簿 MASTER_DECISION_APPROVERS の人 = NE 登録の CSV と同じ)
 */
import crypto from 'node:crypto';
import { MasterWriteError, ownerGateError, lockMasterWriteShared, COMPANY_ID, SOURCE_SYSTEM } from './master-write.mjs';
import { baseGateInTx, lockCutoverSharedInTx } from './master-owner-gate.mjs';

export const DRAFT_IN_NE_CHECK_FN = 'ops.draft_in_ne_check(bigint)';
export const DRAFT_IN_NE_CONFIRM_FN = 'ops.confirm_draft_in_ne(uuid, text, bigint, text)';
/** ボタンの文字・押した後の 1 行 (中原さんの決めた言葉) */
export const DRAFT_IN_NE_BUTTON = 'NE にあるので登録済みにする';
export const DRAFT_IN_NE_DONE = '登録済みにしました。NE とちがう所は『NEへ送る』に出ます';
/** NE と違う欄の言葉 (DB の diffs = ops.ne_reg_compare の列) */
export const DRAFT_IN_NE_COL_WORDS = Object.freeze({ name: '名前', supplier: '仕入先', cost: '原価', price: '売価', tax_rate: '税率', handling: '取扱区分' });

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const HEX64 = /^[0-9a-f]{64}$/;
const CTRL_RE = new RegExp(`[\\x00-\\x1f\\x7f${String.fromCharCode(0x2028, 0x2029)}]`);
const bad = (message, reason = 'invalid_input', extra = {}) => new MasterWriteError(400, reason, message, extra);
const jsonOf = (v) => (typeof v === 'string' ? JSON.parse(v) : v);

/** DB の関数の誤り (「理由: 文」の形) → MasterWriteError (文は DB の文のまま = ふつうの言葉) */
const FN_STATUS = Object.freeze({ invalid_input: 400, not_found: 404, not_confirmable: 409, stale: 409, request_id_reused: 409, before_cutover: 409,
  master_write_session_required: 403, confirm_session_required: 403, master_write_session_exists: 409 });
function fnError(e) {
  if (e instanceof MasterWriteError) return e;
  const m = /^([a-z_]+): ([\s\S]*)$/.exec(String((e && e.message) || ''));
  if (m && e && ['P0001', 'P0002', '22023', '23505', '42501', '55000'].includes(e.code)) {
    const words = m[1] === 'before_cutover' ? `切替前です (DB が断った: ${m[2]})。何もしていません` : `${m[2]}。何もしていません`;
    return new MasterWriteError(FN_STATUS[m[1]] ?? 409, m[1], words);
  }
  if (e && e.code === '23505' && /master_edit_requests_pkey/.test(String(e.constraint || e.message || ''))) {
    return new MasterWriteError(409, 'request_id_reused', '同じ操作の番号 (request_id) がちょうど使われました。画面を開き直してください');
  }
  return e;
}

/** 0098 の関数があって、この接続のロールが確かめを実行できるか (無い・権限が無い = 画面は何も出さない) */
export async function draftInNeAvailable(db) {
  return (await db.query(`select coalesce(pg_catalog.has_function_privilege(to_regprocedure($1), 'EXECUTE'), false) as ok`, [DRAFT_IN_NE_CHECK_FN])).rows[0]?.ok === true;
}

/**
 * SKU ごとの確かめ (読むだけ・画面)。0098 の前・実行権が無い・読めない = 空の Map (画面は何も出さない)。
 * savepoint = 取引の中で読むとき (読めなくても取引を続ける)。戻り値 Map<sku_id (文字), 確かめの答え>
 */
export async function readDraftInNeChecks(db, skuIds, { savepoint = false } = {}) {
  const ids = [...new Set((skuIds || []).map(String))].filter((x) => /^\d{1,18}$/.test(x));
  const out = new Map();
  if (!db || !ids.length) return out;
  if (savepoint) await db.query('savepoint draft_in_ne_check');
  try {
    if (await draftInNeAvailable(db)) {
      for (const id of ids) out.set(id, jsonOf((await db.query('select ops.draft_in_ne_check($1::bigint) as r', [id])).rows[0].r));
    }
    if (savepoint) await db.query('release savepoint draft_in_ne_check');
  } catch (e) {
    if (savepoint) { try { await db.query('rollback to savepoint draft_in_ne_check'); await db.query('release savepoint draft_in_ne_check'); } catch { /* */ } }
    console.error(`[master-draft-in-ne] 確かめを読めない: ${String((e && e.message) || e).slice(0, 200)}`);
    return new Map();
  }
  return out;
}
/** 1 つの SKU の確かめ (無い・読めない = null) */
export async function readDraftInNeCheck(db, skuId, opts = {}) {
  if (skuId == null) return null;
  return (await readDraftInNeChecks(db, [skuId], opts)).get(String(skuId)) ?? null;
}

/**
 * 画面: 確かめの答え → 見せ方。show = 箱を出すか (下書き・NE 登録待ちで、NE のコードの台帳に商品のコードとしてある)・canGo = 押せる (権限・門は別)・
 * why = 押せない理由 (DB の文)・diffs = NE と違う欄の言葉 (登録済みにした後「NEへ送る」に出る)・checkHash = 押すときに送る (画面を開いた時の表)・resolveItem = 一緒に「使わない」にするファイル
 */
export function draftInNeView(chk) {
  if (!chk || !['draft', 'ne_pending'].includes(chk.reg_state) || chk.in_ne !== true) return { show: false };
  const problems = Array.isArray(chk.problems) ? chk.problems : [];
  const diffs = (Array.isArray(chk.diffs) ? chk.diffs : []).map((c) => DRAFT_IN_NE_COL_WORDS[c] || c);
  return {
    show: true, canGo: chk.ok === true && problems.length === 0 && HEX64.test(String(chk.check_hash || '')), code: chk.code, skuId: chk.sku_id,
    checkHash: chk.check_hash || null, why: problems.map((p) => ({ reason: p.reason, text: p.text })), diffs,
    resolveItem: chk.item && chk.item.export_id ? { exportId: String(chk.item.export_id) } : null,
  };
}

function actorOf(v) {
  const a = String(v ?? '').trim().toLowerCase();
  if (!a || a.length > 320 || CTRL_RE.test(a)) throw bad('操作する人 (ログインのメール) が分からない');
  return a;
}
/** この操作 1 回の番号 (保存の記録の request_id)。送られない・形が違う = 新しく作る */
export const draftInNeRequestId = (v) => { const s = String(v ?? '').trim().toLowerCase(); return UUID_RE.test(s) ? s : crypto.randomUUID(); };

/**
 * 登録済みにする (1 件・1 つの取引)。input = { actor, code, checkHash (画面が見た確かめの表), requestId }。opts = { open (MASTER_EDIT_OPEN) }
 * 門 (今の保存と同じ): 段階 (共有の鍵) → 段階 new_open・DB の active・MASTER_EDIT_OPEN (baseGateInTx) → 夜間の取り込み (マスタの書き込みの共有の鍵) → DB の関数
 * 戻り値 = DB の関数の答え { ok, request_id, code, sku_id, from, state, event_id, check_hash, resolved_item, diffs, replayed? } + message (画面の 1 行)
 */
export async function confirmDraftInNe(db, input, { open = false } = {}) {
  const actor = actorOf(input?.actor);
  const code = String(input?.code ?? '').trim();
  if (!code || code.length > 60 || CTRL_RE.test(code)) throw bad('商品コードの形が違う');
  const checkHash = String(input?.checkHash ?? '').trim().toLowerCase();
  if (!HEX64.test(checkHash)) throw bad('画面を読み直してから押してください (確かめの表が無い)', 'invalid_input', { field: 'check_hash' });
  const requestId = draftInNeRequestId(input?.requestId);
  await db.query('begin');
  try {
    // 変更の記録 (0026) の 誰が・request_id = 画面のロールは約束の行から (DB の関数が書く)。持ち主のロールのときのためにも同じ値を置く
    await db.query(`select set_config('core.actor_type', 'human', true), set_config('core.actor_id', $1, true), set_config('core.source_system', $2, true),
        set_config('core.request_id', $3, true), set_config('core.reason', '', true), set_config('core.run_id', '', true)`, [actor, SOURCE_SYSTEM, requestId]);
    await lockCutoverSharedInTx(db);
    const gate = await baseGateInTx(db, { open: open === true, closedWhy: 'この画面の保存はまだ開いていません' });
    if (!gate.ok) throw ownerGateError(gate, (why) => new MasterWriteError(409, 'before_cutover', `切替前です (${why})。何もしていません`));
    await lockMasterWriteShared(db);
    if ((await db.query('select to_regprocedure($1) is not null as ok', [DRAFT_IN_NE_CONFIRM_FN])).rows[0]?.ok !== true) {
      throw new MasterWriteError(409, 'not_applied', 'Company DB に 0098 (NE にあるので登録済みにする) がまだ無い。何もしていません');
    }
    const row = (await db.query('select sku_id::text as id from core.skus where company_id = $1 and code_norm = core.norm_code($2)', [COMPANY_ID, code])).rows[0];
    if (!row) throw new MasterWriteError(404, 'not_found', `商品コード ${code} は Company DB にありません`);
    const r = jsonOf((await db.query('select ops.confirm_draft_in_ne($1::uuid, $2, $3::bigint, $4) as r', [requestId, actor, row.id, checkHash])).rows[0].r);
    await db.query('commit');
    return { ...r, message: DRAFT_IN_NE_DONE };
  } catch (e) {
    try { await db.query('rollback'); } catch { /* 接続が切れた = 元の誤りを上げる */ }
    throw fnError(e);
  }
}
