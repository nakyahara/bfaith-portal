/**
 * cdb-group-gate.js — まとまり (色違い・サイズ違い) のカードの状態と、出品の共通の門
 * (Company DB構想 20 v7 §⑤「NE に入る前のカード」「2 軸」・§⑩ の PR-4)
 *
 * まとまりのカード = product_drafts.cdb_group_product_id があるカード (services/cdb-group-intake.js が作る・結ぶ)。
 * 子・軸・選択肢は SQLite の ph_cdb_group_* (Company DB のスナップショットで全部置き換える・無効の印は消さない)。
 *
 * 状態 (毎回その場で決める = 保存しない):
 *   two_axes   = 2 軸 (横・縦) のまとまり。product-hub が 2 軸を楽天に出せるようになるまで (後の PR)、出品を止める (中原さんの答え 21)
 *   no_children = 有効な子が 0 (全部廃止した) = 出すものが無い
 *   ne_waiting = NE の写し待ち: mirror_products (NE の写し) の子の集まりと代表が、有効な子とちょうど同じでない
 *                (子が NE に無い・代表が違う・NE にだけある子がいる・NE に同じコードが 2 行)。NE に入る前は SQLite の子の一覧が暫定の正本
 *   ready      = ふつう (今までのカードと同じに出品できる。ほかの門 = 画像・税率など はそのまま効く)
 *   🚨 ふつうに戻るのは「有効な子 = NE の子の集まり」かつ「代表 = まとまりのコード」のときだけ。比べるのは有効な子だけ
 *      (廃止した子は比べない。ただし廃止した子が NE に代表つきで現れた = NE にだけある子 = 止める側)
 *
 * 出品の共通の門 (cdbGroupListingBlock): 楽天に書く道は全部ここを通る (サーバー側。画面の表示だけに頼らない):
 *   - 楽天の出品 (registerItem = 詳細画面の「公開で登録」・ボードからの出品・確認済みの再実行) と payload (buildItemPayload = プレビューも)
 *   - 出品の前の確かめ (assertRakutenListable = 詳細画面とボードの両方)
 *   - 今あるページへの SKU の追加 (syncSkuImagesToRms = 楽天のページの SKU に PATCH)
 *   - 公開に切り替える (setItemVisibility の hide = false。非公開にするのは止めない)
 *   まとまりのカードでない (cdb_group_product_id が無い) カードは何も変わらない (null を返す)
 */
import { mirrorReady } from '../lib/variation.js';

const norm = (v) => (v == null ? '' : String(v).trim().toLowerCase());
const IN_CHUNK = 400;

export const CDB_GROUP_STATE_LABELS = Object.freeze({
  two_axes: '2 軸のまとまり (出品を止めています)',
  no_children: '有効な子がありません (出品を止めています)',
  ne_waiting: 'NE の写し待ち (出品を止めています)',
  ready: 'NE と一致 (出品できます)',
});

/** まとまりのカードか (表が無い = 初期化の前 = まとまりのカードではない) */
function groupRowOf(db, draftId) {
  try {
    return db.prepare(`
      SELECT d.id, d.ne_code, d.cdb_group_product_id, d.cdb_group_revision,
             g.group_code, g.group_name, g.group_kind, g.rep_sku_id, g.attention, g.attention_at
        FROM product_drafts d LEFT JOIN ph_cdb_groups g ON g.draft_id = d.id
       WHERE d.id = ? AND d.cdb_group_product_id IS NOT NULL
    `).get(Number(draftId)) || null;
  } catch (_) {
    return null;
  }
}

/**
 * 有効な子と NE の写し (mirror_products) を比べる。
 * @returns {{ readable: boolean, synced: boolean, missing: string[], repDiffers: {code:string, rep:string}[], extra: string[], dup: string[] }}
 */
export function cdbGroupNeSync(db, groupCode, activeCodes) {
  const out = { readable: false, synced: false, missing: [], repDiffers: [], extra: [], dup: [] };
  if (!mirrorReady(db)) return out;
  out.readable = true;
  const gkey = norm(groupCode);
  const keys = [...new Set(activeCodes.map(norm))];
  const byCode = new Map();
  try {
    for (let i = 0; i < keys.length; i += IN_CHUNK) {
      const part = keys.slice(i, i + IN_CHUNK);
      const ph = part.map(() => '?').join(',');
      for (const r of db.prepare(`SELECT 商品コード AS code, 代表商品コード AS rep FROM mirror_products WHERE LOWER(TRIM(商品コード)) IN (${ph})`).all(...part)) {
        const k = norm(r.code);
        if (!byCode.has(k)) byCode.set(k, []);
        byCode.get(k).push(r);
      }
    }
    for (const c of activeCodes) {
      const rows = byCode.get(norm(c)) || [];
      if (rows.length === 0) out.missing.push(c);
      else if (rows.length > 1) out.dup.push(c);
      else if (norm(rows[0].rep) !== gkey) out.repDiffers.push({ code: c, rep: String(rows[0].rep ?? '').trim() });
    }
    // NE にだけある子 (代表 = まとまりのコードなのに、有効な子に無い)。代表の単品自身 (代表 = 自分) は数えない
    const active = new Set(keys);
    for (const r of db.prepare('SELECT 商品コード AS code FROM mirror_products WHERE LOWER(TRIM(代表商品コード)) = ? ORDER BY 商品コード').all(gkey)) {
      const k = norm(r.code);
      if (k !== gkey && !active.has(k)) out.extra.push(String(r.code).trim());
    }
  } catch (_) {
    out.readable = false;
    return out;
  }
  out.synced = keys.length > 0 && !out.missing.length && !out.dup.length && !out.repDiffers.length && !out.extra.length;
  return out;
}

/**
 * まとまりのカードの今の姿 (画面と門の両方)。まとまりのカードでなければ null。
 * @returns {null | { draftId, groupProductId, revision, code, name, kind, attention, attentionAt, axes, options, children, inactiveChildren, twoAxes, sync, state, label }}
 */
export function cdbGroupCardView(db, draftId) {
  const g = groupRowOf(db, draftId);
  if (!g) return null;
  const axes = db.prepare('SELECT axis, name FROM ph_cdb_group_axes WHERE draft_id = ? ORDER BY axis').all(g.id);
  const options = db.prepare('SELECT axis, code, name, sort, active FROM ph_cdb_group_options WHERE draft_id = ? ORDER BY axis, active DESC, sort, code_key').all(g.id);
  const kids = db.prepare(`SELECT cdb_sku_id, code, name, price, choice1, choice2, jans_json, active, inactive_reason
    FROM ph_cdb_group_children WHERE draft_id = ? ORDER BY active DESC, sort, code_key`).all(g.id)
    .map((c) => ({ ...c, jans: (() => { try { return JSON.parse(c.jans_json || '[]'); } catch { return []; } })() }));
  const children = kids.filter((c) => c.active === 1);
  const inactiveChildren = kids.filter((c) => c.active !== 1);
  const code = g.group_code || g.ne_code;
  const twoAxes = axes.length >= 2;
  const sync = cdbGroupNeSync(db, code, children.map((c) => c.code));
  const state = twoAxes ? 'two_axes' : children.length === 0 ? 'no_children' : sync.synced ? 'ready' : 'ne_waiting';
  return {
    draftId: g.id, groupProductId: g.cdb_group_product_id, revision: g.cdb_group_revision, code, name: g.group_name || null, kind: g.group_kind || null,
    attention: g.attention || null, attentionAt: g.attention_at || null,
    axes, options, children, inactiveChildren, twoAxes, sync, state, label: CDB_GROUP_STATE_LABELS[state],
  };
}

/** 門の理由の文 (人が読む) */
function blockMessage(v) {
  if (v.state === 'two_axes') {
    return `2 軸 (${v.axes.map((a) => a.name).join(' × ')}) のまとまりです。product-hub が 2 軸を楽天に出せるようになるまで、楽天への出品を止めています (Company DB の子 ${v.children.length} 件は保存済み)`;
  }
  if (v.state === 'no_children') return 'このまとまりに有効な子がありません (全部廃止した)。楽天への出品を止めています';
  const s = v.sync;
  if (!s.readable) return 'NE の写し待ち: NE の商品マスタの写し (mirror) が読めないので、まとまりの子が NE に入ったか確かめられません。楽天への出品を止めています';
  const parts = [];
  if (s.missing.length) parts.push(`NE にまだ無い子 ${s.missing.length} 件 (${s.missing.slice(0, 5).join('・')}${s.missing.length > 5 ? ' ほか' : ''})`);
  if (s.repDiffers.length) parts.push(`NE の代表が ${v.code} でない子 ${s.repDiffers.length} 件 (${s.repDiffers.slice(0, 3).map((x) => `${x.code} → ${x.rep || '空'}`).join('・')})`);
  if (s.extra.length) parts.push(`NE にだけある子 ${s.extra.length} 件 (${s.extra.slice(0, 5).join('・')})`);
  if (s.dup.length) parts.push(`NE に同じコードが 2 行以上 ${s.dup.join('・')}`);
  return `NE の写し待ち: Company DB の有効な子と NE の子の集まり・代表がまだ同じではありません (${parts.join(' / ') || '確かめ中'})。NE に入って写しが同じになるまで、楽天への出品を止めています`;
}

/**
 * 出品の共通の門。止める = { code, message } / 止めない (まとまりのカードでない・ready) = null
 * @param {{ op?: 'register'|'payload'|'assert'|'sku_images'|'publish' }} _opts 呼び手 (記録だけ・答えは同じ)
 */
export function cdbGroupListingBlock(db, draftId, _opts = {}) {
  let v;
  try {
    v = cdbGroupCardView(db, draftId);
  } catch (e) {
    // まとまりのカードなのに読めない = 止める側 (fail-closed)。まとまりのカードでないかも読めないなら、列で見る
    let isGroup = true;
    try { isGroup = !!db.prepare('SELECT 1 FROM product_drafts WHERE id = ? AND cdb_group_product_id IS NOT NULL').get(Number(draftId)); } catch { /* 列が無い = 初期化の前 */ isGroup = false; }
    return isGroup ? { code: 'unreadable', message: `まとまりのカードの状態を読めません (${String(e?.message || e).slice(0, 120)})。楽天への出品を止めています` } : null;
  }
  if (!v || v.state === 'ready') return null;
  return { code: v.state, message: blockMessage(v) };
}

/** ボードの札: まとまりのカードの draft_id → { state, label, attention } (まとまりのカードだけ) */
export function cdbGroupBoardTags(db) {
  const out = new Map();
  let ids;
  try {
    ids = db.prepare('SELECT id FROM product_drafts WHERE cdb_group_product_id IS NOT NULL').all().map((r) => r.id);
  } catch {
    return out;
  }
  for (const id of ids) {
    try {
      const v = cdbGroupCardView(db, id);
      if (v) out.set(id, { state: v.state, label: v.label, attention: v.attention, children: v.children.length, axes: v.axes.length });
    } catch { /* 札だけ = 出さない (門は別に止める) */ }
  }
  return out;
}
