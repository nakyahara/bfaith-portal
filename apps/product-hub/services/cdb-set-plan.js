/**
 * cdb-set-plan.js — 新商品の登録 (単品) で「セット商品を作るか = 作る」のときに決めた、作るセット (個数・コード・名前) を残し、
 * 出品カードの取り込みのときにセットの下書きを作る (2026-10-17 中原さん「ここで作ったらセット商品コードは下書きの状態・その後登録がなければアラート」)
 *
 * なぜ product-hub の表 (ph_cdb_set_plans) に残すか:
 *   Company DB のカードの知らせ (ops.product_hub_outbox・ph-card-v1) は、登録の関数 (0061 の ops.register_new_sku) が欄の名前を決まった名前だけに絞って確かめる
 *   (知らない欄 = 断る)。セットはまだ Company DB に作らない (セットの登録は開いていない = 設計 24 待ち) ので、知らせの形 (と DB) は変えずに、
 *   登録の request_id (= 知らせの行の request_id) で product-hub に残す。カードの取り込み (cdb-card-intake.js) が同じ request_id の知らせを取り込むとき、
 *   同じ SQLite の取引でセットの下書きを作る = 知らせが何度届いても下書きは 1 回だけ (state = applied・items の set_draft_id)。
 * 順番 (apps/master-edit/router.mjs の POST /api/new):
 *   ① 登録の前に writeSetPlan (waiting)。書けない・ほかの下書き / 作るセットとコードが重なる = 登録しない (409)
 *   ② Company DB の登録 (作るセットのコードも ops.new_sku_code_problem で確かめる = Company DB・NE・消したコード)
 *   ③ 通った = markSetPlan registered (SKU の番号)・断られた = dropped (下書きは作らない)
 *   ④ カードの取り込み (保存の直後・ボードを開いたとき・「もう一度」) が applySetPlanInTx (applied)
 *   ③ を書く前に落ちても、④ は知らせの request_id で表を引くので下書きはできる (state は見ない = 知らせがある = 登録は入っている)
 * 🚨 下書きは provisional_code = 1 (NE で本コードを確かめるまで出品しない・services/set-derive.js の createPlannedSetDraft)
 * 🆕 2026-10-21 (セットの登録が開いた後): その下書きと同じコード・同じ構成でセットを登録 (マスタの入力) = 断らない (plannedSetCardCheck)。
 *   登録の後のカードの取り込み (cdb-card-intake.js) が、同じ取引の中でもう一度確かめてから、そのカードを新しいセットの SKU に結ぶ (= 引き継ぐ・新しいカードは作らない)。
 *   順番 = ① 確かめ (読むだけ・登録の取引の中でもう一度) → ② Company DB の登録 (カードの知らせも同じ取引) → ③ 知らせの取り込みで結ぶ (SQLite の 1 つの取引・何度届いても 1 回)。
 *   ③ の前に落ちても知らせは残る (届け直す)。確かめた後にカードが変わった (構成・除外・ほかのカード) = 結ばずに今までどおり衝突 (人が決める)
 */
import { getDB, logEvent } from '../db.js';
import { createPlannedSetDraft } from './set-derive.js';
import { SET_PLAN_SCHEMA, SET_PLAN_ALERT_DAYS, parseSetPlans, setPlanHash, SetPlanShapeError } from '../../../lib/set-plan.mjs';

const ACTOR = 'auto:cdb-register';
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
/** 書いたのに登録の答えが来ない (waiting) 作るセットが、ほかの登録のコードを押さえる時間 (落ちた登録がコードを押さえ続けない) */
const WAITING_HOLD_MINUTES = 60;

function refuse(status, reason, message, extra = {}) {
  return Object.assign(new Error(message), { status, reason, ...extra });
}

/**
 * 作るセットのコードが product-hub で使われているか (読むだけ・画面の確かめと登録の前)。
 *   カード (product_drafts・大文字小文字は同じと見る) か、ほかの登録の作るセット (登録できた / 書いたばかり) にあれば、その一言
 * @returns {null | { reason: 'ph_draft' | 'ph_set_plan', message, draft_id?, is_set? }} is_set = そのカードがセット (の下書き) = ふつうの単品のコードにもさせない
 */
export function setCodeHeld(code, { requestId = null, db = getDB(), includeStale = false } = {}) {
  const key = String(code ?? '').trim().toLowerCase();
  if (!key) return null;
  const d = db.prepare('SELECT id, parent_draft_id FROM product_drafts WHERE LOWER(TRIM(ne_code)) = ? ORDER BY parent_draft_id IS NULL, id LIMIT 1').get(key);
  if (d) {
    const isSet = d.parent_draft_id != null;
    return { reason: 'ph_draft', draft_id: d.id, is_set: isSet, message: `商品コード ${code} は product-hub の${isSet ? 'セットのカード (下書き)' : 'カード'} (#${d.id}) が使っています` };
  }
  const p = db.prepare(`
    SELECT p.request_id, p.parent_code FROM ph_cdb_set_plan_items i JOIN ph_cdb_set_plans p ON p.request_id = i.request_id
    WHERE i.code_key = ? AND (? IS NULL OR p.request_id <> ?)
      AND (p.state = 'registered' OR (p.state = 'waiting' AND (? = 1 OR p.created_at > strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?))))
    LIMIT 1
  `).get(key, requestId, requestId, includeStale ? 1 : 0, `-${WAITING_HOLD_MINUTES} minutes`);
  //   includeStale = 時間を過ぎた waiting も押さえる (Company DB の保存の記録と照らせない呼び手 = product-hub の本コードの差し替え。照らせる master-edit は先に記録で決める)
  if (p) return { reason: 'ph_set_plan', parent_code: p.parent_code, message: `商品コード ${code} は ${p.parent_code} の登録で作るセットに決まっています` };
  return null;
}

const codeKeyOf = (c) => String(c ?? '').trim().toLowerCase();
/** 構成の比べ方: コード (大文字小文字・前後の空白は同じ) → 数 (同じ品は足す)。並びは見ない */
function membersKey(list) {
  const m = new Map();
  for (const x of list || []) {
    const k = codeKeyOf(x && (x.code ?? x.member_ne_code));
    if (!k) continue;
    m.set(k, (m.get(k) || 0) + Number(x.qty));
  }
  return [...m.entries()].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)).map(([k, q]) => `${k}×${q}`).join(',');
}
/** 2 つの構成が同じか (コードは大文字小文字を同じと見る・数・並びは見ない) */
export const sameSetMembers = (a, b) => membersKey(a) === membersKey(b);
const membersText = (list) => (list || []).map((x) => `${x.code ?? x.member_ne_code}×${x.qty}`).join('・') || 'なし';

/**
 * 🆕 2026-10-21: このコードのカードが「新商品の登録 (単品) で決めたセットの下書き」で、セットの登録 (マスタの入力) がそのまま引き継げるか (読むだけ)。
 *   引き継げる = 同じコードのカードがこの 1 枚だけ・セットの下書き (parent_draft_id)・作るセットから作った (ph_cdb_set_plan_items の created)・
 *   まだ Company DB の商品に結んでいない・除外していない・NE で本コードを確かめる前 (provisional_code = 1)・構成が同じ (components を渡したとき)。
 *   🚨 カードの取り込み (cdb-card-intake.js の applyCdbCardEvent) も同じ取引の中でこれで確かめてから結ぶ = 確かめた後にカードが変わっても違うカードに結ばない
 * @returns {null | { draft_id, code, name, is_set, takeable: boolean, why: string | null, parent_code?, members? }} null = このコードのカードは無い
 */
export function plannedSetCardCheck(code, { components = null, db = getDB() } = {}) {
  const key = codeKeyOf(code);
  if (!key) return null;
  const cards = db.prepare('SELECT id, ne_code, name, status, parent_draft_id, provisional_code, cdb_sku_id FROM product_drafts WHERE LOWER(TRIM(ne_code)) = ? ORDER BY parent_draft_id IS NULL, id').all(key);
  if (!cards.length) return null;
  const d = cards[0];
  const base = { draft_id: d.id, code: d.ne_code, name: d.name, is_set: d.parent_draft_id != null };
  const no = (why, more = {}) => ({ ...base, ...more, takeable: false, why });
  if (cards.length > 1) return no(`product-hub に同じコードのカードが ${cards.length} 枚あります (${cards.map((x) => '#' + x.id).join('・')})`);
  if (d.parent_draft_id == null) return no('セットのカードではありません');
  const plan = db.prepare(`SELECT p.parent_code FROM ph_cdb_set_plan_items i JOIN ph_cdb_set_plans p ON p.request_id = i.request_id
    WHERE i.set_draft_id = ? AND i.outcome = 'created' LIMIT 1`).get(d.id);
  if (!plan) return no('新商品の登録で決めたセットのカードではありません');
  if (d.cdb_sku_id != null) return no('もう Company DB の商品に結んであります');
  if (d.status === 'excluded') return no('除外にしてあります');
  if (d.provisional_code !== 1) return no('NE で本コードを確かめ済みです');
  const members = db.prepare('SELECT member_ne_code AS code, qty FROM draft_set_members WHERE set_draft_id = ? ORDER BY sort').all(d.id);
  const more = { parent_code: plan.parent_code, members };
  if (!members.length) return no('カードに構成がありません', more);
  if (components != null && membersKey(members) !== membersKey(components)) {
    return no(`カードの構成 (${membersText(members)}) と入れた構成 (${membersText(components)}) がちがいます`, more);
  }
  return { ...base, ...more, takeable: true, why: null };
}

/** 予約がまだ効いている時間 (登録の答えが来ない waiting が product-hub のコードの差し替えを止める時間) */
const TAKEOVER_HOLD_MINUTES = 60;

/**
 * 🆕 2026-10-21 (Codex R1 Medium): ① 登録の前に、引き継ぐカードを登録の request_id で予約する (同じ request_id = 押し直し = 同じカードなら何もしない)。
 *   取引の中でもう一度 plannedSetCardCheck (引き継げる・同じカード)。断る = throw { status 409, reason, message, draft_id }
 *   予約があると: カードの取り込みはこのカードにだけ結ぶ (コードを変えられていても新しいカードを作らない)・product-hub の本コードの差し替えを止める (liveTakeoverOf)
 */
export function reserveSetCardTakeover({ requestId, draftId, code, components, actor }, { db = getDB() } = {}) {
  const rid = String(requestId ?? '').trim().toLowerCase();
  if (!UUID_RE.test(rid)) throw refuse(400, 'invalid_input', '保存の番号 (request_id) の形が違う', { field: 'request_id' });
  const id = Number(draftId);
  return db.transaction(() => {
    const prev = db.prepare('SELECT * FROM ph_set_card_takeovers WHERE request_id = ?').get(rid);
    if (prev && prev.draft_id !== id) throw refuse(409, 'request_id_reused', '同じ保存の番号 (request_id) で違うカードを引き継ごうとしました。画面を開き直してください');
    const c = plannedSetCardCheck(code, { components, db });
    if (!c || !c.takeable || c.draft_id !== id) {
      throw refuse(409, 'set_card_changed', `product-hub のカード #${id} がちょうど変わったので引き継げません${c && c.why ? ` (${c.why})` : ''}。画面を開き直してください`, { draft_id: id });
    }
    if (prev) {
      db.prepare(`UPDATE ph_set_card_takeovers SET state = 'waiting', note = NULL, created_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE request_id = ? AND state = 'dropped'`).run(rid);
      return { ok: true, replayed: true };
    }
    db.prepare(`INSERT INTO ph_set_card_takeovers (request_id, draft_id, code_key, state, created_by) VALUES (?, ?, ?, 'waiting', ?)`)
      .run(rid, id, codeKeyOf(code), String(actor || ACTOR));
    return { ok: true, replayed: false };
  })();
}

/** ③ 登録の答えを残す。registered = SKU の番号 / dropped = 登録が断られた。applied (もう結んだ) は変えない */
export function markSetCardTakeover(requestId, { state, skuId = null, note = null }, { db = getDB() } = {}) {
  const rid = String(requestId ?? '').trim().toLowerCase();
  if (state === 'registered') {
    return db.prepare(`UPDATE ph_set_card_takeovers SET state = 'registered', cdb_sku_id = ?, note = NULL WHERE request_id = ? AND state IN ('waiting', 'dropped', 'registered')`)
      .run(skuId == null ? null : Number(skuId), rid).changes;
  }
  if (state === 'dropped') return db.prepare(`UPDATE ph_set_card_takeovers SET state = 'dropped', note = ? WHERE request_id = ? AND state = 'waiting'`).run(note ? String(note).slice(0, 500) : null, rid).changes;
  throw new Error(`知らない状態: ${state}`);
}

/** その登録の予約 (カードの取り込みの取引の中で読む)。無い = null */
export function setCardTakeoverOf(db, requestId) {
  const rid = String(requestId ?? '').trim().toLowerCase();
  if (!UUID_RE.test(rid)) return null;
  return db.prepare('SELECT * FROM ph_set_card_takeovers WHERE request_id = ?').get(rid) || null;
}

/**
 * このカードを引き継ぐ登録がまだ動いているか (registered = 取り込み待ち / waiting = 時間の中)。product-hub の本コードの差し替えを止める。
 *   カードを除外した・コードがもう予約のときと違う = 止めない (Codex R2 Medium 2: 片付ける・直す操作を止めない。取り込みは予約で新しいカードを作らないので二重にならない)
 */
export function liveTakeoverOf(draftId, { db = getDB() } = {}) {
  return db.prepare(`SELECT t.* FROM ph_set_card_takeovers t JOIN product_drafts d ON d.id = t.draft_id
      WHERE t.draft_id = ? AND d.status <> 'excluded' AND LOWER(TRIM(d.ne_code)) = t.code_key
        AND (t.state = 'registered' OR (t.state = 'waiting' AND t.created_at > strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?))) ORDER BY t.created_at DESC LIMIT 1`)
    .get(Number(draftId), `-${TAKEOVER_HOLD_MINUTES} minutes`) || null;
}

/** 🆕 2026-10-21: カードの番号で plannedSetCardCheck (「下書き」の画面の「セットとして登録する」)。カードが無い / 同じコードの別のカードが先 = null */
export function plannedSetCardOf(draftId, { db = getDB() } = {}) {
  const d = db.prepare('SELECT ne_code FROM product_drafts WHERE id = ?').get(Number(draftId));
  if (!d) return null;
  const c = plannedSetCardCheck(d.ne_code, { db });
  return c && c.draft_id === Number(draftId) ? c : null;
}

/**
 * 🆕 2026-10-21: まだ Company DB に登録していない「新商品の登録で決めたセットの下書き」(マスタの入力の「下書き」の画面・読むだけ)。
 *   除外したカード・Company DB の商品に結んだカードは出さない。新しい順・limit 件まで
 * @returns {Array<{ draft_id, code, name, status, provisional, parent_code, qty, created_at, members }>}
 */
export function openPlannedSetCards({ db = getDB(), limit = 200 } = {}) {
  const rows = db.prepare(`
    SELECT d.id, d.ne_code, d.name, d.status, d.provisional_code, d.created_at, p.parent_code, i.qty
    FROM ph_cdb_set_plan_items i
    JOIN ph_cdb_set_plans p ON p.request_id = i.request_id
    JOIN product_drafts d ON d.id = i.set_draft_id
    WHERE i.outcome = 'created' AND d.cdb_sku_id IS NULL AND d.status <> 'excluded'
    ORDER BY d.id DESC LIMIT ?
  `).all(Math.max(1, Math.min(1000, Number(limit) || 200)));
  const mem = db.prepare('SELECT member_ne_code AS code, qty FROM draft_set_members WHERE set_draft_id = ? ORDER BY sort');
  return rows.map((r) => ({ draft_id: r.id, code: r.ne_code, name: r.name, status: r.status, provisional: r.provisional_code === 1,
    parent_code: r.parent_code, qty: r.qty, created_at: r.created_at, members: mem.all(r.id) }));
}

/**
 * 予約の時間 (WAITING_HOLD_MINUTES) を過ぎた waiting の作るセットのうち、このコードを持つものの request_id (Codex R4 Medium 1)。
 *   🚨 時間だけで予約を外さない: 登録が通った後に registered を書く前に落ちた (か書けなかった) 作るセットも waiting のまま。
 *      呼び手 (apps/master-edit/router.mjs) が Company DB の保存の記録 (ops.master_edit_requests) と照らし、done = registered・それ以外 = dropped にしてから予約を見る
 */
export function staleWaitingPlansFor(codes, { db = getDB(), actor = null, parentCode = null } = {}) {
  const keys = [...new Set((codes || []).map((c) => String(c ?? '').trim().toLowerCase()).filter(Boolean))];
  if (!keys.length) return [];
  // 🆕 Codex R6 Medium 1: 時間の中でも、同じ人・同じ単品の waiting (前の保存の答えが「分からない」で、画面が新しい番号で押し直した) は呼び手が記録と照らす
  // 🆕 Codex R8 Medium 3: registered (カードの取り込み待ち) も返す = 呼び手が親の下書きを消した (登録の状態 cancelled) かを照らす
  return db.prepare(`
    SELECT DISTINCT p.request_id, p.state, p.cdb_sku_id, p.request_hash FROM ph_cdb_set_plan_items i JOIN ph_cdb_set_plans p ON p.request_id = i.request_id
    WHERE i.code_key IN (${keys.map(() => '?').join(',')})
      AND ((p.state = 'waiting' AND (p.created_at <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?) OR (LOWER(p.created_by) = LOWER(?) AND LOWER(TRIM(p.parent_code)) = LOWER(TRIM(?)))))
           OR (p.state = 'registered' AND p.cdb_sku_id IS NOT NULL))
  `).all(...keys, `-${WAITING_HOLD_MINUTES} minutes`, String(actor ?? ''), String(parentCode ?? ''))
    .map((r) => ({ request_id: r.request_id, state: r.state, sku_id: r.cdb_sku_id == null ? null : String(r.cdb_sku_id), request_hash: r.request_hash }));
}

/** 照らすのを保留した waiting (その登録がまだ動いている) = 時間を延ばす (予約のまま・Codex R8 Medium 1) */
export function touchWaitingPlan(requestId, { db = getDB() } = {}) {
  return db.prepare(`UPDATE ph_cdb_set_plans SET created_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE request_id = ? AND state = 'waiting'`).run(String(requestId)).changes;
}

/** 親の下書きを消した (登録をやめた) = まだ下書きを作っていない作るセット (registered / waiting) を閉じる = 予約を外す (Codex R8 Medium 3) */
export function dropPlansForSku(skuId, { db = getDB(), note = '親の下書きを消した (登録をやめた)' } = {}) {
  // 🆕 2026-10-21: このセットの登録が引き継ぐはずだったカードの予約も外す (カードの取り込みはもう来ない = product-hub の本コードの差し替えを止めない)
  db.prepare(`UPDATE ph_set_card_takeovers SET state = 'dropped', note = ? WHERE cdb_sku_id = ? AND state IN ('registered', 'waiting')`).run(String(note).slice(0, 500), Number(skuId));
  return db.prepare(`UPDATE ph_cdb_set_plans SET state = 'dropped', note = ? WHERE cdb_sku_id = ? AND state IN ('registered', 'waiting')`).run(String(note).slice(0, 500), Number(skuId)).changes;
}

/**
 * ① 登録の前に作るセットを残す (同じ request_id = 押し直し = 同じ中身なら何もしない)。
 *   input = { requestId, requestHash (登録の要求のハッシュ = lib/master-register.mjs の registerPayloadHashOf), parentCode, parentName, sets (lib/set-plan.mjs の形), actor }
 *   断る (throw status/reason/message/field/index): 形が違う 400・カード / ほかの作るセットとコードが重なる 409・同じ request_id で違う中身 409
 * @returns {{ ok: true, replayed: boolean }}
 */
export function writeSetPlan({ requestId, requestHash, parentCode, parentName, sets, actor }, { db = getDB() } = {}) {
  const rid = String(requestId ?? '').trim().toLowerCase();
  if (!UUID_RE.test(rid)) throw refuse(400, 'invalid_input', '保存の番号 (request_id) の形が違う', { field: 'request_id' });
  if (!/^[0-9a-f]{64}$/.test(String(requestHash ?? ''))) throw new Error('登録の要求のハッシュが無い');
  let plans;
  try { plans = parseSetPlans(sets, { parentCode }); } catch (e) {
    if (e instanceof SetPlanShapeError) throw refuse(400, 'invalid_input', e.message, { field: 'card.sets', index: e.index });
    throw e;
  }
  if (!plans.length) throw refuse(400, 'invalid_input', '作るセットを 1 つ以上入れてください', { field: 'card.sets' });
  const hash = setPlanHash(plans);
  return db.transaction(() => {
    const prev = db.prepare('SELECT plan_hash, request_hash, state FROM ph_cdb_set_plans WHERE request_id = ?').get(rid);
    if (prev) {
      if (prev.plan_hash !== hash || prev.request_hash !== requestHash) throw refuse(409, 'request_id_reused', '同じ保存の番号 (request_id) で違う作るセットが来ました。画面を開き直してください');
      if (prev.state === 'waiting' || prev.state === 'dropped') {
        plans.forEach((sp, i) => {
          const held = setCodeHeld(sp.code, { requestId: rid, db });
          if (held) throw refuse(409, 'set_code_taken', `作るセットの ${i + 1} 行目: ${held.message}。何も保存していません`, { field: 'card.sets', index: i });
        });
        db.prepare(`UPDATE ph_cdb_set_plans SET state = 'waiting', created_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE request_id = ? AND state IN ('waiting', 'dropped')`).run(rid);
      }
      return { ok: true, replayed: true };
    }
    plans.forEach((sp, i) => {
      const held = setCodeHeld(sp.code, { requestId: rid, db });
      if (held) throw refuse(409, 'set_code_taken', `作るセットの ${i + 1} 行目: ${held.message}。何も保存していません`, { field: 'card.sets', index: i });
    });
    db.prepare(`INSERT INTO ph_cdb_set_plans (request_id, schema, parent_code, parent_name, plan_hash, request_hash, state, created_by) VALUES (?, ?, ?, ?, ?, ?, 'waiting', ?)`)
      .run(rid, SET_PLAN_SCHEMA, String(parentCode), String(parentName ?? ''), hash, String(requestHash), String(actor || ACTOR));
    const ins = db.prepare('INSERT INTO ph_cdb_set_plan_items (request_id, sort, qty, code, code_key, name) VALUES (?, ?, ?, ?, ?, ?)');
    plans.forEach((sp, i) => ins.run(rid, i, sp.qty, sp.code, sp.code.toLowerCase(), sp.name));
    return { ok: true, replayed: false };
  })();
}

/**
 * ③ 登録の答えを残す。registered = SKU の番号 (下書きはまだ = 取り込みで作る) / dropped = 登録が断られた (下書きは作らない)。
 *   applied はもう変えない (取り込みが先に済んだ)。押し直しで登録が通った = dropped から registered に戻す
 */
export function markSetPlan(requestId, { state, skuId = null, note = null }, { db = getDB() } = {}) {
  const rid = String(requestId ?? '').trim().toLowerCase();
  if (!['registered', 'dropped'].includes(state)) throw new Error(`知らない状態: ${state}`);
  if (state === 'registered') {
    return db.prepare(`UPDATE ph_cdb_set_plans SET state = 'registered', cdb_sku_id = ?, registered_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'), note = NULL
      WHERE request_id = ? AND state IN ('waiting', 'dropped', 'registered')`).run(skuId == null ? null : Number(skuId), rid).changes;
  }
  return db.prepare(`UPDATE ph_cdb_set_plans SET state = 'dropped', note = ? WHERE request_id = ? AND state = 'waiting'`).run(note ? String(note).slice(0, 500) : null, rid).changes;
}

/** その request_id の作るセット (無ければ null)。items は入れた順 */
export function readSetPlan(requestId, { db = getDB() } = {}) {
  const rid = String(requestId ?? '').trim().toLowerCase();
  const p = db.prepare('SELECT * FROM ph_cdb_set_plans WHERE request_id = ?').get(rid);
  if (!p) return null;
  return { ...p, items: db.prepare('SELECT * FROM ph_cdb_set_plan_items WHERE request_id = ? ORDER BY sort').all(rid) };
}

/**
 * ④ カードの取り込み (cdb-card-intake.js の SQLite の取引の中) で、その登録の作るセットの下書きを作る。
 *   作るセットが無い = null (今までどおり)。もう作った (applied) = 前の答え (何度届いても 1 回だけ)。
 *   🚨 結ぶのは 登録の要求のハッシュ (保存の記録 = 通った登録の中身) と単品のコードが同じときだけ (Codex R1 High 2)。
 *      違う = 同じ request_id で断られた押し直しの作るセット = 付けない (null = 作るセットなし)
 *   行ごと: 同じコードのカードがもうある = 作らない (skipped・記録)・作った = set_draft_id。全部の行が済んだら applied
 * @returns {null | { created: [{ code, draft_id }], skipped: [{ code, why }], replayed: boolean }}
 */
/** その知らせの登録の作るセット (登録の要求のハッシュ・単品のコードが同じときだけ) */
function acceptedPlan(db, { requestId, requestHash, parentCode }) {
  const rid = String(requestId ?? '').trim().toLowerCase();
  if (!UUID_RE.test(rid)) return null;
  const p = db.prepare('SELECT * FROM ph_cdb_set_plans WHERE request_id = ?').get(rid);
  if (!p) return null;
  if (!requestHash || p.request_hash !== requestHash) return null;
  if (parentCode != null && String(p.parent_code).trim().toLowerCase() !== String(parentCode).trim().toLowerCase()) return null;
  return p;
}

export function applySetPlanInTx(db, { requestId, requestHash, parentCode = null, parentDraftId, skuId, actor = ACTOR }) {
  const p = acceptedPlan(db, { requestId, requestHash, parentCode });
  if (!p) return null;
  const rid = p.request_id;
  const items = db.prepare('SELECT * FROM ph_cdb_set_plan_items WHERE request_id = ? ORDER BY sort').all(rid);
  const view = () => {
    const now = db.prepare('SELECT * FROM ph_cdb_set_plan_items WHERE request_id = ? ORDER BY sort').all(rid);
    return {
      created: now.filter((x) => x.outcome === 'created').map((x) => ({ code: x.code, draft_id: x.set_draft_id })),
      skipped: now.filter((x) => x.outcome === 'skipped').map((x) => ({ code: x.code, why: x.outcome_note })),
    };
  };
  if (p.state === 'applied') return { ...view(), replayed: true };
  const upd = db.prepare('UPDATE ph_cdb_set_plan_items SET set_draft_id = ?, outcome = ?, outcome_note = ? WHERE request_id = ? AND sort = ?');
  for (const it of items) {
    if (it.outcome) continue;   // 前に済んだ行 (途中で落ちた取引は全部戻る = ふつうは無い)
    const r = createPlannedSetDraft(db, parentDraftId, { qty: it.qty, code: it.code, name: it.name }, p.created_by || actor);
    if (r.draftId) upd.run(r.draftId, 'created', null, rid, it.sort);
    else {
      upd.run(null, 'skipped', String(r.skipped || '作れませんでした').slice(0, 500), rid, it.sort);
      logEvent(db, Number(parentDraftId), 'set_plan_skipped', `新商品の登録で決めたセット ${it.code} (${it.qty}個) は作っていません: ${r.skipped}`, ACTOR);
    }
  }
  db.prepare(`UPDATE ph_cdb_set_plans SET state = 'applied', applied_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'), parent_draft_id = ?,
      cdb_sku_id = COALESCE(cdb_sku_id, ?) WHERE request_id = ?`).run(Number(parentDraftId), skuId == null ? null : Number(skuId), rid);
  return { ...view(), replayed: false };
}

/**
 * 作るセットを作らずに閉じる (人が既存のカードに結んだら、そのカードにもうセットの判断があった = カードの判断が正)。
 *   まだの行を全部 skipped (理由つき) にして applied にする (決めたコードを押さえ続けない)。作るセットが無い・もう済んだ = null
 */
export function skipSetPlanInTx(db, { requestId, requestHash, parentCode = null, parentDraftId, skuId = null, why }) {
  const p = acceptedPlan(db, { requestId, requestHash, parentCode });
  if (!p || p.state === 'applied') return null;
  const rid = p.request_id;
  const todo = db.prepare('SELECT sort, code FROM ph_cdb_set_plan_items WHERE request_id = ? AND outcome IS NULL ORDER BY sort').all(rid);
  db.prepare(`UPDATE ph_cdb_set_plan_items SET outcome = 'skipped', outcome_note = ? WHERE request_id = ? AND outcome IS NULL`).run(String(why).slice(0, 500), rid);
  db.prepare(`UPDATE ph_cdb_set_plans SET state = 'applied', applied_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'), parent_draft_id = ?,
      cdb_sku_id = COALESCE(cdb_sku_id, ?) WHERE request_id = ?`).run(Number(parentDraftId), skuId == null ? null : Number(skuId), rid);
  if (todo.length) logEvent(db, Number(parentDraftId), 'set_plan_skipped', `新商品の登録で決めたセット (${todo.map((x) => x.code).join('・')}) は作っていません: ${why}`, ACTOR);
  return { created: [], skipped: todo.map((x) => ({ code: x.code, why })) };
}

/**
 * 知らせ: 新商品の登録で作ったセットの下書きのうち、NE で本コードを確かめていない (provisional_code = 1) まま日数を過ぎたもの (ボード・NE要対応の表)。
 *   除外 (excluded)・保留 (on_hold) は数えない。表が無い (初期化の前) = 空
 * @returns {Map<number, { days: number, late: boolean, parentCode: string }>} セットの下書きの id → 作ってからの日数
 */
export function setPlanDraftAges(db = getDB(), { alertDays = SET_PLAN_ALERT_DAYS } = {}) {
  try {
    const rows = db.prepare(`
      SELECT i.set_draft_id AS id, d.created_at, p.parent_code,
             CAST(julianday('now') - julianday(COALESCE(p.applied_at, d.created_at)) AS INTEGER) AS days
      FROM ph_cdb_set_plan_items i
      JOIN ph_cdb_set_plans p ON p.request_id = i.request_id
      JOIN product_drafts d ON d.id = i.set_draft_id
      WHERE i.outcome = 'created' AND d.provisional_code = 1 AND d.status NOT IN ('on_hold', 'excluded')
    `).all();
    return new Map(rows.map((r) => [r.id, { days: Math.max(0, Number(r.days) || 0), late: Number(r.days) >= alertDays, parentCode: r.parent_code }]));
  } catch {
    return new Map();
  }
}
