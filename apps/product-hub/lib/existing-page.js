/**
 * 既存の楽天ページへのバリエーション追加か (2026-09-25 スタッフ要望)。
 *
 * カラバリが増えただけの商品も、NE に新しい商品コードが入ると新商品と同じカードになる
 * (自動取込は代表商品コード = 楽天の管理番号でまとめるので、カードの商品コードは既存ページと同じ)。
 * 既存ページは人が目視でページ編集するので、ボードで見分けられるよう札を出す。
 *
 * 判定の順:
 *   1. 人が決めた値 (product_drafts.existing_page = 1 / 0) があればそれ
 *   2. 無ければ自動: **NE で同じ代表商品コードのグループに、アプリ導入前からある商品がある**
 *      (= 自動取込の初回シードで記録された商品 = ph_ne_seen_codes.draft_id IS NULL)
 *      - グループの子 SKU (代表商品コード = このカードの商品コード) のどれかがシード済み
 *      - または、グループがあってカードの商品コード自身がシード済み
 *        (単品ページにカラーを足して、元の商品の代表商品コードが空のままの形)
 *      単品 (グループが無い) の古い商品はページがまだ無いこともあるので自動では付けない
 *   自動で付けないもの:
 *      - Notion から取り込んだ商品 (アプリ導入前から進めていた新商品なのでシード済みになる = 誤検知)
 *      - このアプリから楽天に出品した商品 (ページを作ったのはアプリ = 新規ページ)
 *
 * NE の代表商品コードが空のまま新しいカラーを登録した商品は自動では分からない → 詳細画面で人が選ぶ。
 * 札は見分けるための目印で、工程のゲートには使わない (判定を誤っても作業は止まらない)。
 */
import { mirrorReady } from './variation.js';

const norm = (v) => String(v == null ? '' : v).trim().toLowerCase();
const IN_CHUNK = 400;

/** 人が選べる値 (詳細画面のプルダウン)。'' = 自動判定に任せる */
export const EXISTING_PAGE_CHOICES = [
  { value: '', label: '自動で判定' },
  { value: '1', label: '既存ページに追加 (カラバリ追加など)' },
  { value: '0', label: '新規ページ' },
];

/**
 * 自動判定で「既存ページ」になる商品コードの集合を引く (正規化済みキー)。
 * mirror / シード表が無い環境では空集合 (札が出ないだけ)。
 */
function autoExistingKeys(db, keys) {
  const out = new Set();
  if (keys.length === 0 || !mirrorReady(db)) return out;
  try {
    for (let i = 0; i < keys.length; i += IN_CHUNK) {
      const part = keys.slice(i, i + IN_CHUNK);
      const ph = part.map(() => '?').join(',');
      // グループがあるキー (子 SKU が 1 件以上)
      const grouped = new Set(db.prepare(`
        SELECT DISTINCT LOWER(TRIM(代表商品コード)) AS k FROM mirror_products
        WHERE LOWER(TRIM(代表商品コード)) IN (${ph})
      `).all(...part).map((r) => r.k));
      if (grouped.size === 0) continue;
      // 子 SKU のどれかがアプリ導入前からある
      for (const r of db.prepare(`
        SELECT DISTINCT LOWER(TRIM(m.代表商品コード)) AS k FROM mirror_products m
        JOIN ph_ne_seen_codes s ON s.code_key = LOWER(TRIM(m.商品コード)) AND s.draft_id IS NULL
        WHERE LOWER(TRIM(m.代表商品コード)) IN (${ph})
      `).all(...part)) out.add(r.k);
      // カード自身の商品コードがアプリ導入前からある (グループがあるときだけ)
      for (const r of db.prepare(`
        SELECT code_key AS k FROM ph_ne_seen_codes WHERE draft_id IS NULL AND code_key IN (${ph})
      `).all(...part)) if (grouped.has(r.k)) out.add(r.k);
    }
  } catch (e) {
    // 目印なので、引けなくても画面は出す (札が出ないだけ)
    console.warn('[product-hub] 既存ページの自動判定に失敗:', e.message);
    return new Set();
  }
  return out;
}

/**
 * 商品ごとの「既存ページか」。
 * @param {Array<{id:number, ne_code:string, existing_page:number|null, added_to_draft_id?:number|null, source?:string|null, rakuten_registered_at?:string|null}>} drafts
 * @returns {Map<number, {existingPage: boolean, auto: boolean}>} auto = 自動判定の結果 (人が決めていない)
 */
export function existingPageOf(db, drafts) {
  const list = Array.isArray(drafts) ? drafts : [];
  const isAddition = (d) => d.added_to_draft_id != null;
  const undecided = list.filter((d) => !isAddition(d) && d.existing_page !== 0 && d.existing_page !== 1
    && d.source !== 'notion_import' && !d.rakuten_registered_at);
  const autoKeys = autoExistingKeys(db, [...new Set(undecided.map((d) => norm(d.ne_code)).filter(Boolean))]);
  const judged = new Set(undecided);
  const out = new Map();
  for (const d of list) {
    // 出品済みページへの色追加のカード (2026-09-25) は常に既存ページ。人の選択で外せない —
    // 商品コードが新しい色の SKU なので、「新規ページ」にして出品すると別ページができる (Codex #1450 R1 high)
    if (isAddition(d)) {
      out.set(d.id, { existingPage: true, auto: false });
    } else if (d.existing_page === 1 || d.existing_page === 0) {
      out.set(d.id, { existingPage: d.existing_page === 1, auto: false });
    } else {
      out.set(d.id, { existingPage: judged.has(d) && autoKeys.has(norm(d.ne_code)), auto: true });
    }
  }
  return out;
}

/** 1 商品ぶん (詳細画面用) */
export function existingPageOfDraft(db, draftId) {
  const d = db.prepare(`
    SELECT d.id, d.ne_code, d.existing_page, d.source, d.added_to_draft_id,
      (SELECT ne_code FROM product_drafts ap WHERE ap.id = d.added_to_draft_id) AS added_to_ne_code,
      (SELECT registered_at FROM draft_rakuten r WHERE r.draft_id = d.id) AS rakuten_registered_at
    FROM product_drafts d WHERE d.id = ?
  `).get(Number(draftId));
  if (!d) return { existingPage: false, auto: true, choice: '', addedTo: null };
  const r = existingPageOf(db, [d]).get(d.id);
  return {
    ...r, choice: d.existing_page == null ? '' : String(d.existing_page),
    // 出品済みページへの色追加のカード (2026-09-25) なら追加先のページ (ドラフト)
    addedTo: d.added_to_draft_id != null ? { id: d.added_to_draft_id, ne_code: d.added_to_ne_code || null } : null,
  };
}
