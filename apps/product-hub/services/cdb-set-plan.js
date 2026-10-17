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
export function setCodeHeld(code, { requestId = null, db = getDB() } = {}) {
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
      AND (p.state = 'registered' OR (p.state = 'waiting' AND p.created_at > strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?)))
    LIMIT 1
  `).get(key, requestId, requestId, `-${WAITING_HOLD_MINUTES} minutes`);
  if (p) return { reason: 'ph_set_plan', message: `商品コード ${code} は ${p.parent_code} の登録で作るセットに決まっています` };
  return null;
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
