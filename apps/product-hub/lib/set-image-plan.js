/**
 * セットの「画像の引き継ぎ計画」の語彙と枠 (slot) の数え方 (2026-09-04 要件定義 §4.7)。
 *
 * 中原さんの指示: 「詳細画像の既定は『作らない』ではなく、**単品の詳細画像の何枚目を修正**
 * みたいな形で指示を送れるように」。つまりセットの画像は「作る/作らない」の 2 択ではなく、
 * **枠ごとに** 使い回す・直す・作り直す・使わない を決める。
 *
 * ここに置く理由: 計画を作る側 (services/set-derive.js)・直す側 (router)・見せる側 (detail.ejs)・
 * 起動時に埋める側 (db.js) が同じ語彙と枠の数え方を使う。
 * 表示名や slot の数え方をそれぞれに書くと、画面と依頼書と DB でズレる。
 *
 * 「親の枠を数えて計画の行を作る」だけは DB を触るが、**枠の数え方そのもの**なのでここに置く
 * (db を引数で受けるので、db.js からも services からも循環せずに呼べる)。
 */

/** 枠の番号は**既存の楽天スロット番号に合わせる** (lib/folder-import.js の slotOfParsedName)。
 *  1 = TOP画像 (`_top`) / 2 = `_01` / n = `_(n-1)`。白抜き (`_00`) だけは枠を持たないので 0 を当てる。
 *  🚨 `draft_images.sort` は 0 始まり (TOP=0) なので **slot = sort + 1**。混ぜないこと */
export const WHITE_BG_SLOT = 0;
export const MAX_PLAN_SLOT = 20;

/** slot → draft_images.sort (白抜きは draft_images に入らないので null) */
export function imageSortOfSlot(slot) {
  const n = Number(slot);
  return Number.isInteger(n) && n >= 1 ? n - 1 : null;
}

/** draft_images.sort → slot */
export function slotOfImageSort(sort) {
  return Number(sort) + 1;
}

/** 画面・依頼書で使う枠の呼び名 */
export function slotLabel(slot) {
  const n = Number(slot);
  if (n === WHITE_BG_SLOT) return '白抜き';
  if (n === 1) return 'TOP画像';
  return `商品画像 ${n - 1}`;
}

export const SET_IMAGE_ACTIONS = ['reuse', 'modify', 'recreate', 'drop'];

/** 既定は「そのまま使う」。セットでも使える画像は使い回すのが一番早い (中原さん) */
export const DEFAULT_SET_IMAGE_ACTION = 'reuse';

export const SET_IMAGE_ACTION_LABELS = {
  reuse: 'そのまま使う',
  modify: '直して使う',
  recreate: '作り直す',
  drop: '使わない',
};

/** 指示文が要る操作 (modify は必須・recreate は任意)。空の modify は依頼書が「直して」だけになる */
export function instructionRequired(action) {
  return action === 'modify';
}

/** 制作 (依頼→デザイン…) が要る操作か */
export function needsProduction(action) {
  return action === 'modify' || action === 'recreate';
}

/**
 * 計画から「画像の制作が要るか」を決める (§4.7)。
 * 1 枠でも直す/作り直すがあれば制作が要る。全部 そのまま使う/使わない なら要らない
 * (親の画像をコピーしてあるので、出品ゲートの TOP 画像も満たせる)。
 * @param {Array<{action: string}>} plans
 */
export function planNeedsProduction(plans) {
  return (Array.isArray(plans) ? plans : []).some((p) => needsProduction(p?.action));
}

/**
 * 依頼書に出す 1 行 (「枠3: 親の3枚目を直す —『2個並べた写真に』」)。
 * 制作が要る枠だけを、枠の順に並べて返す。
 * @param {Array<{slot: number, action: string, instruction: string|null}>} plans
 */
export function productionInstructions(plans) {
  return (Array.isArray(plans) ? plans : [])
    .filter((p) => needsProduction(p?.action))
    .slice()
    .sort((a, b) => Number(a.slot) - Number(b.slot))
    .map((p) => ({
      slot: Number(p.slot),
      label: slotLabel(p.slot),
      action: p.action,
      actionLabel: SET_IMAGE_ACTION_LABELS[p.action] || p.action,
      instruction: p.instruction || null,
      text: `${slotLabel(p.slot)}: ${SET_IMAGE_ACTION_LABELS[p.action] || p.action}`
        + (p.instruction ? ` — ${p.instruction}` : ''),
    }));
}

/**
 * 親の画像を枠 (slot) の一覧にする。白抜きは 0、TOP は 1、以降 draft_images.sort + 1。
 * 同じ枠に 2 枚あれば先頭だけ、枠から溢れた画像は載せない。
 */
export function parentImageSlots(db, parentDraftId) {
  const id = Number(parentDraftId);
  const out = [];
  const rk = db.prepare('SELECT white_bg_drive_file_id FROM draft_rakuten WHERE draft_id = ?').get(id);
  if (rk?.white_bg_drive_file_id) out.push({ slot: WHITE_BG_SLOT, driveFileId: rk.white_bg_drive_file_id });
  for (const img of db.prepare(
    'SELECT drive_file_id, sort FROM draft_images WHERE draft_id = ? ORDER BY sort, id').all(id)) {
    const slot = slotOfImageSort(img.sort);
    if (slot > MAX_PLAN_SLOT) continue;
    if (out.some((x) => x.slot === slot)) continue;
    out.push({ slot, driveFileId: img.drive_file_id });
  }
  return out.sort((a, b) => a.slot - b.slot);
}

/**
 * 親の枠について計画の行を作る (既定は「そのまま使う」)。親に無い枠の行は作らない。
 * 🚨 **足りない行を足すだけ** — 既にある行の指定は変えない。だから何度呼んでもよく、
 * 途中で落ちて一部しか入らなくても次に呼んだときに残りが埋まる
 * ([[feedback_idempotency_by_broken_state_not_by_record]])。
 * @returns 足した行数
 */
export function seedImagePlans(db, setDraftId, parentDraftId) {
  const slots = parentImageSlots(db, parentDraftId);
  if (slots.length === 0) return 0;
  const ins = db.prepare(`
    INSERT INTO draft_set_image_plans (set_draft_id, slot, parent_drive_file_id, action)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(set_draft_id, slot) DO NOTHING
  `);
  // 1 セットぶんはまとめて入る (途中まで入った状態を残さない)
  return db.transaction(() => {
    let made = 0;
    for (const sl of slots) made += ins.run(Number(setDraftId), sl.slot, sl.driveFileId, DEFAULT_SET_IMAGE_ACTION).changes;
    return made;
  })();
}

/**
 * 導入前に作られたセットにも計画を作る (2026-09-04 §4.7、冪等)。
 *
 * 🚨 **計画の行を作るだけで、画像のコピーも工程の反映もしない**。既存のセットは人が入れた画像や
 * 進行中の画像制作を持っている。「全部そのまま使う」と読んで工程を skip にすると制作を止めてしまう。
 * 次に人が計画を変えたときから追従する。
 * 🚨 判定は「計画が 0 件か」ではなく**毎回すべてのセットに足りない行を足す**。0 件で判定すると、
 * 一部だけ入った状態で止まったとき欠けた枠が永久に補われない (Codex R5 medium)。
 * @returns 行を足したセットの数
 */
export function backfillSetImagePlans(db, logEvent = null) {
  const sets = db.prepare(
    'SELECT id, parent_draft_id FROM product_drafts WHERE parent_draft_id IS NOT NULL').all();
  let touched = 0;
  for (const t of sets) {
    const n = seedImagePlans(db, t.id, t.parent_draft_id);
    if (n === 0) continue;
    touched += 1;
    if (logEvent) {
      logEvent(db, t.id, 'set_image_plan_seeded',
        `画像の計画を後から作りました (${n} 枠。既定は「そのまま使う」)。`
        + '画像とこれまでの工程はそのままです — 直したい枠は画像タブで変えてください',
        'migration');
    }
  }
  return touched;
}
