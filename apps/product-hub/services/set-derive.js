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
import { resolveVariationGroup } from '../lib/variation.js';

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
    const qty = Math.round(Number(m?.qty));
    if (!code) throw badRequest('構成の商品コードが空です');
    if (!Number.isInteger(qty) || qty < 1 || qty > 999) throw badRequest(`「${code}」の個数が不正です (1〜999)`);
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
        memo, source, created_by, parent_draft_id, provisional_code
      ) VALUES (?, ?, 'draft', ?, ?, ?, 0, ?, 'portal', ?, ?, 1)
    `).run(
      neCode, name.slice(0, 300),
      parent.official_url, parent.amazon_url, parent.own_brand,
      `${parent.ne_code} から作ったセット商品`,
      actor || null, parentId,
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
      db.prepare(`
        INSERT INTO draft_yahoo (draft_id, delivery_label, tax_rate, yahoo_category_id, yahoo_path)
        VALUES (?, ?, ?, ?, ?)
      `).run(setId, py.delivery_label, py.tax_rate, py.yahoo_category_id, py.yahoo_path);
    }
    // 商品ページ表記は**許可リスト**でコピーする (Codex R1 high 2026-08-23)。
    // 全列コピーすると「50ml」の 2 個セットが内容量 50ml のまま出て、法定表示が誤る。
    // 数量で変わるもの (内容量・サイズ) と食品表示は空にして、人に入れ直させる
    const pinfo = db.prepare('SELECT * FROM draft_page_info WHERE draft_id = ?').get(parentId);
    if (pinfo) {
      db.prepare(`
        INSERT INTO draft_page_info (
          draft_id, product_type, ingredients, usage_notes, origin_type, origin_country,
          category_label, seller_name, importer_name
        )
        SELECT ?, product_type, ingredients, usage_notes, origin_type, origin_country,
               category_label, seller_name, importer_name
        FROM draft_page_info WHERE draft_id = ?
      `).run(setId, parentId);
    }
    // 楽天のジャンル・属性はそのまま使える (画像と登録状態は引き継がない)
    const prk = db.prepare('SELECT genre_id, attributes_json, shipping_method_group, postage_included, normal_delivery_date_id FROM draft_rakuten WHERE draft_id = ?').get(parentId);
    if (prk) {
      db.prepare(`
        INSERT INTO draft_rakuten (draft_id, genre_id, attributes_json, shipping_method_group, postage_included, normal_delivery_date_id)
        VALUES (?, ?, ?, ?, ?, ?)
      `).run(setId, prk.genre_id, prk.attributes_json, prk.shipping_method_group, prk.postage_included, prk.normal_delivery_date_id);
    }
    db.prepare(`
      INSERT INTO draft_shop_categories (draft_id, shop_category_id, slot)
      SELECT ?, shop_category_id, slot FROM draft_shop_categories WHERE draft_id = ?
    `).run(setId, parentId);

    // 工程を作り、開始位置をモードで決める。
    //   ai   … 基本情報だけ済み → AI がセット用のタイトル・説明文を書く
    //   copy … AI 工程も飛ばして人がすぐ直す
    ensureProgress(db, setId);
    const done = (code) => db.prepare(`
      UPDATE draft_step_progress SET state = 'done', done_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
        done_by = ?, version = version + 1, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
      WHERE draft_id = ? AND step_code = ? AND state != 'done'
    `).run(actor || 'system', setId, code);
    if (mode === 'copy') {
      // 説明文を引き継いでから「商品説明確認」に置く (人が直す前提)
      db.prepare(`
        INSERT INTO draft_ai_outputs (draft_id, kind, content, generated_at, model_note, edited_by_human)
        SELECT ?, kind, content, generated_at, '単品からコピー', 0 FROM draft_ai_outputs WHERE draft_id = ?
      `).run(setId, parentId);
      done('basic_info');
      done('ai_generate');
    } else {
      done('basic_info');
    }

    // 親の「セット商品作成検討」を完了にする (判断が済んだので親は先へ進める)。
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
export function reconcileProvisionalCode(db, draft) {
  if (!draft || draft.provisional_code !== 1) return false;
  // まだ仮コードそのもの (SET-xxx-01) なら確定しようがない
  if (/^SET-/i.test(String(draft.ne_code || ''))) return false;
  if (!confirmedInNe(resolveVariationGroup(db, draft.ne_code, { withMembers: false }).kind)) return false;
  const info = db.prepare(`
    UPDATE product_drafts SET provisional_code = 0, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
    WHERE id = ? AND provisional_code = 1
  `).run(draft.id);
  if (info.changes !== 1) return false;
  logEvent(db, draft.id, 'set_code_confirmed', `${draft.ne_code} をNE商品マスタで確認しました`, 'system');
  draft.provisional_code = 0;
  return true;
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
  const row = db.prepare('SELECT parent_draft_id, provisional_code FROM product_drafts WHERE id = ?').get(id);
  if (!row?.parent_draft_id) return null;
  const parent = db.prepare('SELECT id, ne_code, name FROM product_drafts WHERE id = ?').get(row.parent_draft_id);
  return {
    parent: parent || null,
    provisional: row.provisional_code === 1,
    members: db.prepare(
      'SELECT member_ne_code, qty FROM draft_set_members WHERE set_draft_id = ? ORDER BY sort'
    ).all(id),
  };
}
