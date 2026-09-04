/**
 * セット商品の派生ドラフト生成 (2026-08-23)。
 *
 * 工程「セット商品作成検討」で「セットを作る」と判断したとき、単品の情報をコピーした
 * **別ドラフト**を立てる。中原さんの指定:
 *   - 単品の出品は止めない (セットは別カードとして並走する)
 *   - 入口は 2 つ: AI にセット用へ書き換えてもらう / コピーしてすぐ人が直す
 *   - 商品コードは**仮コード** `SET-<親コード>-01` で先に走らせ、NE 登録後に本コードへ差し替える (方式 b)
 *
 * 仮コードのまま楽天に出すと manage_number を後から変えられないため、
 * buildItemPayload が provisional_code=1 を見て出品を止める (出品直前ゲート)。
 */
import { getDB, logEvent } from '../db.js';
import { ensureProgress, setStepState } from '../lib/workflow-progress.js';
import { SET_NE_STEP_CODE } from '../lib/set-decision.js';
import { resolveVariationGroup } from '../lib/variation.js';
// 判断の語彙・理由・表示名は lib 側が正 (カードを組み立てる workflow-progress.js も同じものを使う)
import {
  SET_DECISIONS, SET_DECISIONS_CLOSING, SET_DECISION_REASONS, NE_STATE_LABELS, describeSetDecision,
} from '../lib/set-decision.js';

// 既存の import 元 (router.js / smoke.mjs) を壊さないための再公開
export { SET_DECISIONS, SET_DECISIONS_CLOSING, SET_DECISION_REASONS, NE_STATE_LABELS, describeSetDecision };

/** 生成モード: ai = AI 工程から / copy = 商品説明確認から */
export const SET_MODES = ['ai', 'copy'];
const MAX_MEMBERS = 20;
const MAX_SET_PER_PARENT = 20;

function badRequest(message) {
  const e = new Error(message);
  e.status = 400;
  return e;
}

/**
 * セットにすると値が変わる仕様表の行か (サイズ・内容量・入数・重量など)。
 * 単品の値をそのまま持ち込むと商品ページの表示が誤るので、セット側では空にして
 * 人に入れ直させる。素材・原産国・ブランドなどは数量に依存しないのでコピーする。
 */
const QUANTITY_DEPENDENT_SPEC = /(サイズ|寸法|大きさ|内容量|容量|入数|個数|本数|枚数|重量|重さ|質量|セット内容|梱包|外寸|size|volume|weight)/i;
export function isQuantityDependentSpec(key) {
  return QUANTITY_DEPENDENT_SPEC.test(String(key == null ? '' : key));
}

/**
 * NE 商品マスタで実在が確認できたか。
 * 'conflict' (同じコードが NE に重複) や 'detached' は「確認できた」に数えない (Codex R2)。
 * 曖昧なまま確定すると、仮コードの出品ゲートが外れて誤ったコードで登録される
 */
function confirmedInNe(kind) {
  return kind === 'single' || kind === 'variation';
}

/**
 * 仮コードを採番する。`SET-<親>-01` から順に空きを探す。
 * ne_code は正規化 UNIQUE なので、大文字小文字違いの衝突も避けて確認する。
 */
export function nextProvisionalCode(db, parentCode) {
  const base = `SET-${String(parentCode || '').trim()}`;
  for (let i = 1; i <= MAX_SET_PER_PARENT; i++) {
    const code = `${base}-${String(i).padStart(2, '0')}`;
    const hit = db.prepare('SELECT 1 FROM product_drafts WHERE LOWER(TRIM(ne_code)) = ?').get(code.toLowerCase());
    if (!hit) return code;
  }
  throw badRequest(`この商品のセットは ${MAX_SET_PER_PARENT} 件までです`);
}

/**
 * 単品からセットの派生ドラフトを作る。
 *
 * コピーする    : 公式/参考/Amazon URL・仕様表・カテゴリ属性・ページ表記・Yahoo 項目・自社商品フラグ
 * コピーしない  : 商品コード (仮採番)・売価・JAN・画像・タイトル・説明文
 *   → セットは価格も JAN も画像も別物。ここをコピーすると「直し忘れ」が一番危ない
 *
 * @param {number} parentDraftId 単品のドラフト
 * @param {{mode: 'ai'|'copy', members?: Array<{ne_code: string, qty: number}>, name?: string,
 *   parent_step_version?: number}} opts  parent_step_version = 親の「セット商品作成検討」工程の版数 (CAS 用)
 * @param {{isAdmin: boolean, actorStaffId: number|null}} ctx 権限判定の文脈 (親工程を閉じるため)
 * @returns {{draftId: number, neCode: string}}
 */
export function createSetDraft(parentDraftId, opts, actor, ctx = { isAdmin: false, actorStaffId: null }) {
  const db = getDB();
  const parentId = Number(parentDraftId);
  const mode = String(opts?.mode || '');
  if (!SET_MODES.includes(mode)) throw badRequest('作り方の指定が不正です');

  const parent = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(parentId);
  if (!parent) throw badRequest('元の商品が見つかりません');
  if (parent.parent_draft_id) throw badRequest('セット商品からさらにセットは作れません');

  // 構成 (何を何個)。未指定なら「親を 2 個」を初期値にする (もっとも多い形)
  const rawMembers = Array.isArray(opts?.members) && opts.members.length > 0
    ? opts.members
    : [{ ne_code: parent.ne_code, qty: 2 }];
  if (rawMembers.length > MAX_MEMBERS) throw badRequest(`セットの構成は ${MAX_MEMBERS} 件までです`);
  const members = [];
  const seen = new Set();
  for (const m of rawMembers) {
    const code = String(m?.ne_code == null ? '' : m.ne_code).trim();
    // 丸めてから整数判定すると 1.5 が黙って 2 個になる (Codex R5)。小数はそのまま弾く
    const qty = Number(m?.qty);
    if (!code) throw badRequest('構成の商品コードが空です');
    if (!Number.isInteger(qty) || qty < 1 || qty > 999) throw badRequest(`「${code}」の個数が不正です (1〜999の整数)`);
    const key = code.toLowerCase();
    if (seen.has(key)) throw badRequest(`構成に「${code}」が重複しています`);
    seen.add(key);
    members.push({ code, qty });
  }

  const name = String(opts?.name || '').trim()
    || `${parent.name} ${members.length === 1 ? `${members[0].qty}個` : ''}セット`.replace(/\s+/g, ' ').trim();

  return db.transaction(() => {
    const neCode = nextProvisionalCode(db, parent.ne_code);
    const info = db.prepare(`
      INSERT INTO product_drafts (
        ne_code, name, status, official_url, amazon_url, own_brand, has_variation,
        memo, source, created_by, parent_draft_id, provisional_code,
        ne_registration_state, parent_snapshot_at
      ) VALUES (?, ?, 'draft', ?, ?, ?, 0, ?, 'portal', ?, ?, 1, 'not_requested', ?)
    `).run(
      neCode, name.slice(0, 300),
      parent.official_url, parent.amazon_url, parent.own_brand,
      `${parent.ne_code} から作ったセット商品`,
      actor || null, parentId,
      // 派生した時点の親。親がこの後に変わったら「親が更新されています」を出す (§4.5)
      parent.updated_at || null,
    );
    const setId = Number(info.lastInsertRowid);

    const insMember = db.prepare(
      'INSERT INTO draft_set_members (set_draft_id, member_ne_code, qty, sort) VALUES (?, ?, ?, ?)'
    );
    members.forEach((m, i) => insMember.run(setId, m.code, m.qty, i));

    // 参考URL・仕様表・Yahoo 項目・ページ表記・カテゴリ属性は単品と同じものが使える
    db.prepare(`
      INSERT INTO draft_reference_urls (draft_id, url, sort)
      SELECT ?, url, sort FROM draft_reference_urls WHERE draft_id = ?
    `).run(setId, parentId);
    // 仕様表も**数量で変わる行は落とす** (Codex R2 high)。
    // ページ表記の内容量だけ除外しても、仕様表に「サイズ 10cm」「内容量 50ml」が残ると
    // 2 個セットの商品ページに単品の値が出る。素材・原産国などはそのまま使える
    const specs = db.prepare('SELECT spec_key, spec_value, sort FROM draft_specs WHERE draft_id = ? ORDER BY sort, id').all(parentId);
    const insSpec = db.prepare('INSERT INTO draft_specs (draft_id, spec_key, spec_value, sort) VALUES (?, ?, ?, ?)');
    let dropped = 0;
    for (const sp of specs) {
      if (isQuantityDependentSpec(sp.spec_key)) { dropped++; continue; }
      insSpec.run(setId, sp.spec_key, sp.spec_value, sp.sort);
    }
    // 売価・佐川売価はセットで変わるのでコピーしない (税率・配送・カテゴリだけ引き継ぐ)
    const py = db.prepare('SELECT * FROM draft_yahoo WHERE draft_id = ?').get(parentId);
    if (py) {
      // shipping_override (ヤフーだけ別配送) は配送方法の一部なので一緒に引き継ぐ。
      // 抜かすと親が「定形外（ヤフーのみネコポス）」でもセットは楽天と同じ扱いになる
      db.prepare(`
        INSERT INTO draft_yahoo (draft_id, delivery_label, shipping_override, tax_rate, yahoo_category_id, yahoo_path)
        VALUES (?, ?, ?, ?, ?, ?)
      `).run(setId, py.delivery_label, py.shipping_override ?? 0, py.tax_rate, py.yahoo_category_id, py.yahoo_path);
    }
    // 商品ページ表記は**許可リスト**でコピーする (Codex R1 high 2026-08-23)。
    // 全列コピーすると「50ml」の 2 個セットが内容量 50ml のまま出て、法定表示が誤る。
    // 数量で変わるもの (内容量・サイズ) と食品表示は空にして、人に入れ直させる。
    // ブランド名は数量で変わらないのでコピーする (2026-08-28)
    const pinfo = db.prepare('SELECT * FROM draft_page_info WHERE draft_id = ?').get(parentId);
    if (pinfo) {
      db.prepare(`
        INSERT INTO draft_page_info (
          draft_id, product_type, brand_name, ingredients, usage_notes, origin_type, origin_country,
          category_label, seller_name, importer_name
        )
        SELECT ?, product_type, brand_name, ingredients, usage_notes, origin_type, origin_country,
               category_label, seller_name, importer_name
        FROM draft_page_info WHERE draft_id = ?
      `).run(setId, parentId);
    }
    // 楽天のジャンル・属性はそのまま使える (画像と登録状態は引き継がない)。
    // article_number (メーカー型番) も引き継ぐ (2026-08-31 / Codex R1 high): 入口を
    // article_number に統一したので、ここを抜かすと派生したセット商品から型番が消える
    const prk = db.prepare('SELECT genre_id, attributes_json, article_number, shipping_method_group, postage_included, normal_delivery_date_id FROM draft_rakuten WHERE draft_id = ?').get(parentId);
    if (prk) {
      db.prepare(`
        INSERT INTO draft_rakuten (draft_id, genre_id, attributes_json, article_number, shipping_method_group, postage_included, normal_delivery_date_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      `).run(setId, prk.genre_id, prk.attributes_json, prk.article_number, prk.shipping_method_group, prk.postage_included, prk.normal_delivery_date_id);
    }
    db.prepare(`
      INSERT INTO draft_shop_categories (draft_id, shop_category_id, slot)
      SELECT ?, shop_category_id, slot FROM draft_shop_categories WHERE draft_id = ?
    `).run(setId, parentId);

    // 工程を作る。セットは**セット専用のテンプレート** (構成決定 → NE登録 → 商品情報作成・確認 →
    // 出品準備 → 出品・展開) を持つので、単品の「基本情報入力」「AI情報入力待ち」は無い。
    // どちらのモードでも「構成決定」から始まり、モードの違いは**説明文の初期値**だけ
    // (2026-09-04 要件定義 §4.1/§5.6):
    //   ai   … 説明文を持たせず、AI にセット用へ書かせる
    //   copy … 単品の説明文を引き継いで、人がセット用に直す
    ensureProgress(db, setId);
    if (mode === 'copy') {
      db.prepare(`
        INSERT INTO draft_ai_outputs (draft_id, kind, content, generated_at, model_note, edited_by_human)
        SELECT ?, kind, content, generated_at, '単品からコピー', 0 FROM draft_ai_outputs WHERE draft_id = ?
      `).run(setId, parentId);
    }

    // 「セットを作った」という判断を先に残す (2026-09-04 §4.2)。
    // ⑤はこの記録があるときだけ閉じられる — 記録の前に閉じると順序が逆になる
    recordSetDecision(db, parentId, { decision: 'create', linked_set_draft_id: setId }, actor);

    // 親の「セット展開判断」を完了にする (判断が済んだので親は先へ進める)。
    // 直接 UPDATE せず setStepState を通す (Codex R1 medium 2026-08-23):
    // 権限判定と版数 CAS を迂回すると、他人がレビュー中の工程を古い画面から閉じられる
    setStepState(parentId, 'set_review', {
      state: 'done',
      expected_version: opts?.parent_step_version,
    }, actor, { isAdmin: ctx.isAdmin, actorStaffId: ctx.actorStaffId, requireVersion: true });

    const memberText = members.map((m) => `${m.code}×${m.qty}`).join(' + ');
    logEvent(db, parentId, 'set_draft_created', `${neCode} (${memberText}) を作成 [${mode === 'ai' ? 'AIに書き換え' : 'コピーして手直し'}]`, actor);
    logEvent(db, setId, 'created_from_parent',
      `${parent.ne_code} から作成 (${memberText})`
      + (dropped > 0 ? ` ／ 仕様表 ${dropped} 行はセットで値が変わるため引き継いでいません (要入力)` : ''),
      actor);
    return { draftId: setId, neCode };
  })();
}

/**
 * 「本コードに差し替えたが NE にまだ無かった」商品を、NE に現れた時点で確定させる。
 * 詳細画面を開くたびに呼ぶ自己修復 (mirror の毎時取込を待つ間、出品ゲートは閉じたまま)。
 * @returns {boolean} この呼び出しで確定したか
 */
/**
 * 本コードが確定したセットの「NE登録」工程を自動で閉じる (2026-09-04 §4.1)。
 *
 * この工程は人が押せない (setStepState が仮コードのままの done/skip を拒否する)。
 * ここで閉じないと、本コードが確定してもカードが NE登録の列に残り続ける。
 * **確定と同じトランザクションから呼ぶこと** — 分けると「確定したのに工程は未完了」が残る。
 * @returns 閉じたか (既に決着済み・単品・仮コードのままなら false)
 */
export function closeNeStepIfConfirmed(db, draftId, actor = 'system') {
  const id = Number(draftId);
  const d = db.prepare('SELECT parent_draft_id, provisional_code FROM product_drafts WHERE id = ?').get(id);
  if (!d || d.parent_draft_id == null || d.provisional_code !== 0) return false;
  const row = db.prepare('SELECT state FROM draft_step_progress WHERE draft_id = ? AND step_code = ?')
    .get(id, SET_NE_STEP_CODE);
  if (!row || row.state === 'done' || row.state === 'skip') return false;
  // システムによる完了。担当者が誰であっても閉じる (人の操作ではないので権限判定は通さない)
  setStepState(id, SET_NE_STEP_CODE, { state: 'done' }, actor, { isAdmin: true });
  return true;
}

export function reconcileProvisionalCode(db, draft) {
  if (!draft) return false;
  if (draft.provisional_code !== 1) {
    // 既に確定済み。工程だけ取り残されていないか見て収束させる (Codex R4 medium:
    // 確定と工程完了が別々に落ちた場合、ここを通さないと二度と直らない)
    closeNeStepIfConfirmed(db, draft.id);
    return false;
  }
  // まだ仮コードそのもの (SET-xxx-01) なら確定しようがない
  if (/^SET-/i.test(String(draft.ne_code || ''))) return false;
  if (!confirmedInNe(resolveVariationGroup(db, draft.ne_code, { withMembers: false }).kind)) return false;
  // 確定・記録・工程完了は**ひとまとまり**。分けると「本コードは確定したのに工程は未完了」が残り、
  // 次からは provisional_code !== 1 で素通りするので直る機会が無くなる (Codex R4 medium)
  const done = db.transaction(() => {
    const info = db.prepare(`
      UPDATE product_drafts SET provisional_code = 0, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
      WHERE id = ? AND provisional_code = 1
    `).run(draft.id);
    if (info.changes !== 1) return false;
    logEvent(db, draft.id, 'set_code_confirmed', `${draft.ne_code} をNE商品マスタで確認しました`, 'system');
    closeNeStepIfConfirmed(db, draft.id);
    return true;
  })();
  if (done) draft.provisional_code = 0;
  return done;
}

/** この商品から作られたセット (親側の画面に出す) */
export function setDraftsOf(db, parentDraftId) {
  return db.prepare(`
    SELECT d.id, d.ne_code, d.name, d.status, d.provisional_code,
           (SELECT GROUP_CONCAT(member_ne_code || '×' || qty, ' + ')
              FROM (SELECT member_ne_code, qty FROM draft_set_members WHERE set_draft_id = d.id ORDER BY sort)) AS members
    FROM product_drafts d
    WHERE d.parent_draft_id = ?
    ORDER BY d.id
  `).all(Number(parentDraftId));
}

/** このセットの構成と親 (セット側の画面に出す) */
export function setInfoOf(db, draftId) {
  const id = Number(draftId);
  const row = db.prepare(`
    SELECT parent_draft_id, provisional_code, ne_registration_state, ne_registration_error,
           ne_registration_requested_at, ne_registration_requested_by, parent_snapshot_at, price
    FROM product_drafts WHERE id = ?
  `).get(id);
  if (!row?.parent_draft_id) return null;
  const parent = db.prepare(
    'SELECT id, ne_code, name, price, updated_at FROM product_drafts WHERE id = ?').get(row.parent_draft_id);
  const members = db.prepare(
    'SELECT member_ne_code, qty FROM draft_set_members WHERE set_draft_id = ? ORDER BY sort').all(id);
  return {
    parent: parent || null,
    provisional: row.provisional_code === 1,
    members,
    // 「本コード確定」は列に持たない。provisional_code=0 が唯一の真で、表示はそこから導出する (§4.3)
    neState: row.provisional_code === 1 ? (row.ne_registration_state || 'not_requested') : 'confirmed',
    neError: row.ne_registration_error || null,
    neRequestedAt: row.ne_registration_requested_at || null,
    neRequestedBy: row.ne_registration_requested_by || null,
    // 親が派生後に変わったか (自動追随はしない。人に知らせるだけ — §4.5)
    parentChanged: !!(parent?.updated_at && row.parent_snapshot_at && parent.updated_at > row.parent_snapshot_at),
    // 売価の初期値がどこから来たか (§5.3)。単品×個数で入れているので、その式を1行で見せる
    priceOrigin: (parent?.price != null && members.length === 1 && members[0].member_ne_code === parent.ne_code)
      ? { unit: parent.price, qty: members[0].qty, total: parent.price * members[0].qty }
      : null,
  };
}

// ─── NE登録の進み (2026-09-04 要件定義 §4.3) ──────────────────────────────
//
// 工程 (set_ne_register) とは別に、「NE にいつ依頼して、どこで止まっているか」を状態で持つ。
// 🚨「本コード確定」はここに持たない — provisional_code=0 が唯一の真 (§4.3)。

/** 人が動かせる遷移先。processing は将来の API 化用 (人は押さない) */
export const NE_STATES_MANUAL = ['requested', 'needs_action'];

/**
 * NE登録の進みを1つ動かす (§4.3)。
 * - `requested`: 「NEに登録を依頼した」。依頼した日時と人を残す (滞留警告は工程の stall_days が見る)
 * - `needs_action`: 「登録できなかった」。**理由は必須** — 理由の無い要対応は誰も動かせない
 * 本コードが確定済み (provisional_code=0) の商品はもう動かさない。
 */
export function setNeRegistrationState(db, draftId, input, actor) {
  const id = Number(draftId);
  const state = String(input?.state || '').trim();
  if (!NE_STATES_MANUAL.includes(state)) throw badRequest('NE登録の状態指定が不正です');
  const d = db.prepare(
    'SELECT parent_draft_id, provisional_code, ne_code, ne_registration_state FROM product_drafts WHERE id = ?').get(id);
  if (!d) throw badRequest('商品が見つかりません');
  if (d.parent_draft_id == null) throw badRequest('セット商品ではありません');
  if (d.provisional_code !== 1) throw badRequest('この商品は本コードが確定しています');
  const reason = state === 'needs_action' ? String(input?.reason || '').trim().slice(0, 500) : null;
  if (state === 'needs_action' && !reason) throw badRequest('登録できなかった理由を入れてください');
  const now = new Date().toISOString();
  db.transaction(() => {
    db.prepare(`
      UPDATE product_drafts
      SET ne_registration_state = ?,
          ne_registration_error = ?,
          ne_registration_requested_at = CASE WHEN ? = 'requested' THEN ? ELSE ne_registration_requested_at END,
          ne_registration_requested_by = CASE WHEN ? = 'requested' THEN ? ELSE ne_registration_requested_by END,
          updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
      WHERE id = ?
    `).run(state, reason, state, now, state, actor || null, id);
    logEvent(db, id, 'ne_registration_state',
      state === 'requested'
        ? `NE登録: 依頼済みにしました (${d.ne_code})`
        : `NE登録: 要対応にしました — ${reason}`,
      actor);
  })();
  return { state, reason };
}

/**
 * 「親の更新を確認した」(§4.5)。親の更新時刻を今の値で覚え直すだけ — 自動追随はしない。
 * @returns 覚え直したか (セットでない・親がいないなら false)
 */
export function ackParentSnapshot(db, draftId, actor) {
  const id = Number(draftId);
  const d = db.prepare('SELECT parent_draft_id FROM product_drafts WHERE id = ?').get(id);
  if (!d?.parent_draft_id) return false;
  const parent = db.prepare('SELECT updated_at FROM product_drafts WHERE id = ?').get(d.parent_draft_id);
  if (!parent) return false;
  db.prepare('UPDATE product_drafts SET parent_snapshot_at = ? WHERE id = ?').run(parent.updated_at, id);
  logEvent(db, id, 'parent_snapshot_ack', '親の更新を確認しました', actor);
  return true;
}

// ─── 「セット展開判断」の記録 (2026-09-04 要件定義 §4.2) ────────────────────
//
// 派生を作ったときだけ工程⑤が閉じる作りでは「まだ検討していない」と「検討して作らないと
// 決めた」が区別できなかった。判断そのものを残し、⑤はこの記録があるときだけ閉じられる。

/**
 * 判断を1件記録する (append-only)。⑤を閉じるかは呼び出し側が SET_DECISIONS_CLOSING で判断する。
 * @param {object} input {decision, reason_code?, reason_text?, linked_set_draft_id?}
 */
export function recordSetDecision(db, draftId, input, actor) {
  const decision = String(input?.decision || '').trim();
  if (!SET_DECISIONS.includes(decision)) throw badRequest('判断の指定が不正です');
  const reasonCode = String(input?.reason_code || '').trim() || null;
  const reasonText = String(input?.reason_text || '').trim().slice(0, 500) || null;
  if (decision === 'none') {
    // 「作らない」は後から理由を思い出せないと意味がないので必須にする
    if (!reasonCode || !SET_DECISION_REASONS[reasonCode]) throw badRequest('作らない理由を選んでください');
    if (reasonCode === 'other' && !reasonText) throw badRequest('「その他」を選んだときは理由を書いてください');
  } else if (reasonCode && !SET_DECISION_REASONS[reasonCode]) {
    throw badRequest('作らない理由の指定が不正です');
  }
  let linked = input?.linked_set_draft_id == null ? null : Number(input.linked_set_draft_id);
  if (linked != null && !Number.isInteger(linked)) throw badRequest('紐づけるセットの指定が不正です');
  if (decision === 'existing') {
    if (linked == null) throw badRequest('紐づけるセット商品を選んでください');
    // 自分から派生したセットだけを紐づけられる (他人のセットを勝手に紐づけない)
    const own = db.prepare('SELECT 1 FROM product_drafts WHERE id = ? AND parent_draft_id = ?').get(linked, Number(draftId));
    if (!own) throw badRequest('この商品から作られたセットではありません');
  }
  if (decision === 'none' || decision === 'hold') linked = null;
  db.prepare(`
    INSERT INTO draft_set_decisions (draft_id, decision, reason_code, reason_text, linked_set_draft_id, decided_by)
    VALUES (?, ?, ?, ?, ?, ?)
  `).run(Number(draftId), decision, decision === 'none' ? reasonCode : null, reasonText, linked, actor || null);
  logEvent(db, draftId, 'set_decision_recorded', describeSetDecision({ decision, reason_code: reasonCode, reason_text: reasonText }), actor);
  return { decision, closing: SET_DECISIONS_CLOSING.includes(decision) };
}

/** 最新の判断 (無ければ null)。カードと詳細画面が使う */
export function latestSetDecision(db, draftId) {
  return db.prepare(`
    SELECT decision, reason_code, reason_text, linked_set_draft_id, decided_by, decided_at
    FROM draft_set_decisions WHERE draft_id = ? ORDER BY decided_at DESC, id DESC LIMIT 1
  `).get(Number(draftId)) || null;
}
