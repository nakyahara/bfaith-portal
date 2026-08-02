/**
 * 楽天の店舗内カテゴリ (お店の棚) マスタと、ドラフトへの割り当て。
 *
 * 一覧は RMS の店舗内カテゴリ画面/CSV からの貼り付けで取り込む (1行1カテゴリ)。
 * RMS Category API での自動取得・商品への自動紐付けは miniPC service-api への
 * ルート追加が必要なため未実装 (miniPC を触れるときに対応)。それまでこの欄は
 * 「非公開登録した商品を公開するとき、RMS 画面でどのカテゴリに載せるか」の指示として使う。
 */

export const MAX_SHOP_CATEGORY_LINES = 1000;
export const MAX_DRAFT_SHOP_CATEGORIES = 30;
const MAX_PATH_LEN = 300;

// 階層区切りは「>」(全角＞も可)。前後の空白を落として ' > ' に正規化する
function normalizePath(raw) {
  return String(raw)
    .split(/[>＞]/)
    .map((s) => s.trim())
    .filter((s) => s !== '')
    .join(' > ');
}

/**
 * 貼り付けテキスト → カテゴリ行。
 * 受ける形式 (1行1カテゴリ):
 *   - 「犬用品 > おやつ」                … パスのみ
 *   - 「123456,犬用品 > おやつ」        … カテゴリID + パス (カンマ or タブ区切り。IDは数字のみ)
 * 戻り値: { ok, rows: [{categoryId, path, pathKey}], duplicates } / { ok:false, error }
 */
export function parseShopCategoryText(text) {
  const lines = String(text || '').split(/\r?\n/);
  if (lines.length > MAX_SHOP_CATEGORY_LINES) {
    return { ok: false, error: `一度に取り込めるのは ${MAX_SHOP_CATEGORY_LINES} 行までです (${lines.length}行)` };
  }
  const rows = [];
  const seen = new Set();
  let duplicates = 0;
  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (!line) continue;
    let categoryId = null;
    let pathPart = line;
    const m = line.match(/^(\d+)\s*[\t,]\s*(.+)$/);
    if (m) { categoryId = m[1]; pathPart = m[2]; }
    const path = normalizePath(pathPart);
    if (!path) continue;
    if (path.length > MAX_PATH_LEN) {
      return { ok: false, error: `カテゴリ名が長すぎます (${MAX_PATH_LEN}文字まで): ${path.slice(0, 40)}…` };
    }
    const pathKey = path.toLowerCase();
    if (seen.has(pathKey)) { duplicates++; continue; }
    seen.add(pathKey);
    rows.push({ categoryId, path, pathKey });
  }
  return { ok: true, rows, duplicates };
}

/**
 * マスタを貼り付け内容で全置き換えする。
 * 行は消さず is_active で外す (ドラフトの選択が参照しているため)。
 * 既知のカテゴリIDは、ID無しで再取り込みされても保持する (COALESCE)。
 *
 * 事故ガード (Codex R1 High-4): 貼り付けミス (一部だけコピー等) で件数が半分未満に
 * 急減する置き換えは、force 指定が無い限り拒否する (現在10件以上あるときのみ判定)。
 */
export function replaceShopCategories(db, rows, { force = false } = {}) {
  const current = countActiveShopCategories(db);
  if (!force && current >= 10 && rows.length < Math.ceil(current / 2)) {
    return { error: 'too_few', active: current, incoming: rows.length };
  }
  const tx = db.transaction(() => {
    db.prepare('UPDATE ph_shop_categories SET is_active = 0').run();
    const upsert = db.prepare(`
      INSERT INTO ph_shop_categories (category_id, path, path_key, is_active, sort_order, imported_at)
      VALUES (?, ?, ?, 1, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
      ON CONFLICT(path_key) DO UPDATE SET
        category_id = COALESCE(excluded.category_id, category_id),
        path = excluded.path,
        is_active = 1,
        sort_order = excluded.sort_order,
        imported_at = excluded.imported_at
    `);
    rows.forEach((r, i) => upsert.run(r.categoryId, r.path, r.pathKey, i));
    return {
      active: db.prepare('SELECT COUNT(*) AS c FROM ph_shop_categories WHERE is_active = 1').get().c,
      deactivated: db.prepare('SELECT COUNT(*) AS c FROM ph_shop_categories WHERE is_active = 0').get().c,
    };
  });
  return tx();
}

export function countActiveShopCategories(db) {
  return db.prepare('SELECT COUNT(*) AS c FROM ph_shop_categories WHERE is_active = 1').get().c;
}

/**
 * ドラフト保存で受けるカテゴリ id 配列の厳密検証 (Codex R1 Medium-3: parseInt は "12abc" を通す)。
 * 整数 number か「数字だけの文字列」以外は不正。重複は除去。
 */
export function sanitizeShopCategoryIds(input, max = MAX_DRAFT_SHOP_CATEGORIES) {
  if (!Array.isArray(input)) return { error: 'not_array' };
  const ids = [];
  for (const v of input) {
    let n = null;
    if (typeof v === 'number' && Number.isInteger(v)) n = v;
    else if (typeof v === 'string' && /^\d+$/.test(v.trim())) n = Number(v.trim());
    if (n == null || n <= 0) return { error: 'invalid_id' };
    if (!ids.includes(n)) ids.push(n);
  }
  if (ids.length > max) return { error: 'too_many' };
  return { ids };
}

/**
 * 詳細画面用: 有効な全カテゴリ + (一覧から外れたが) このドラフトが選択中のカテゴリ。
 * selected は 0/1。
 */
export function listShopCategoriesForDraft(db, draftId) {
  return db.prepare(`
    SELECT c.id, c.category_id, c.path, c.is_active,
           CASE WHEN s.draft_id IS NULL THEN 0 ELSE 1 END AS selected
    FROM ph_shop_categories c
    LEFT JOIN draft_shop_categories s ON s.shop_category_id = c.id AND s.draft_id = ?
    WHERE c.is_active = 1 OR s.draft_id IS NOT NULL
    ORDER BY c.sort_order, c.id
  `).all(draftId);
}

export function selectedShopCategoryPaths(db, draftId) {
  return db.prepare(`
    SELECT c.path FROM draft_shop_categories s
    JOIN ph_shop_categories c ON c.id = s.shop_category_id
    WHERE s.draft_id = ?
    ORDER BY c.sort_order, c.id
  `).all(draftId).map((r) => r.path);
}

/** ドラフトの選択を丸ごと入れ替える (呼び出し側で存在チェック済みの id 配列)。 */
export function setDraftShopCategories(db, draftId, ids) {
  db.prepare('DELETE FROM draft_shop_categories WHERE draft_id = ?').run(draftId);
  const ins = db.prepare('INSERT INTO draft_shop_categories (draft_id, shop_category_id) VALUES (?, ?)');
  for (const id of ids) ins.run(draftId, id);
}

/**
 * 店舗内カテゴリの AI 初期候補 (2026-08-02 中原さん要望「AIが最初に設定して人が後で修正」)。
 *
 * 外部 API は使わないローカル採点: カテゴリパスの各セグメント語が
 * 「楽天ジャンルのフルパス + 商品名」にどれだけ含まれるかでスコアリングする
 * (店舗の棚名は楽天ジャンルに寄せた命名が多いため、ジャンル確定後ほど精度が上がる)。
 *
 * 採点: 末端セグメント完全一致 4 / 非末端完全一致 2 /
 *       セグメント内の語 (「・」等区切り、2文字以上) の部分一致 = 末端2×命中率・非末端1×命中率 /
 *       語の先頭2文字一致 (「洗濯ネット」⇔「洗濯用品」、「化粧水」⇔「化粧品」の表記ゆれ) =
 *       末端1・非末端0.5×命中率 の補助点 — 補助点だけでは自動適用しきい値 (4) に届かない
 *       (後方の「〜用品」「〜セット」だけが共通するカテゴリを拾わないよう、n-gram でなく語頭で見る)
 *
 * @param {Array<{id, path, is_active?, selected?}>} categories listShopCategoriesForDraft の行
 * @param {{name?: string, genrePath?: string}} source 採点の材料
 * @returns {Array<{id, path, score, leafExact}>} スコア降順 (score > 0 のみ)、最大 max 件
 */
export function suggestShopCategories(categories, { name = '', genrePath = '' } = {}, { max = 5 } = {}) {
  const hay = `${genrePath} ${name}`.toLowerCase();
  if (hay.trim() === '') return [];
  const splitWords = (s) => s.split(/[・･、/／\s>＞]+/).filter((w) => w.length >= 2);
  const hayHeads = new Set(splitWords(hay).map((w) => w.slice(0, 2)));
  const scored = [];
  for (const c of categories || []) {
    if (c.is_active === 0 && !c.selected) continue; // 一覧から外れたカテゴリは提案しない
    const segs = String(c.path).split(/\s*>\s*/).map((s) => s.trim()).filter(Boolean);
    let score = 0;
    let leafExact = false;   // 末端 (実際に商品が載る棚) 自体が言い当てられたか
    segs.forEach((seg, i) => {
      const isLeaf = i === segs.length - 1;
      const s = seg.toLowerCase();
      if (hay.includes(s)) {
        score += isLeaf ? 4 : 2;
        if (isLeaf) leafExact = true;
        return;
      }
      const words = splitWords(s);
      if (words.length === 0) return;
      const hit = words.filter((w) => hay.includes(w)).length;
      if (hit > 0) { score += (isLeaf ? 2 : 1) * (hit / words.length); return; }
      // 完全一致も語一致もないときだけ、語頭2文字の一致で補助点
      const headHit = words.filter((w) => hayHeads.has(w.slice(0, 2))).length;
      if (headHit > 0) score += (isLeaf ? 1 : 0.5) * (headHit / words.length);
    });
    if (score > 0) scored.push({ id: c.id, path: c.path, score: Math.round(score * 100) / 100, leafExact });
  }
  scored.sort((a, b) => b.score - a.score || String(a.path).localeCompare(String(b.path), 'ja'));
  return scored.slice(0, max);
}

/** 自動適用の最低スコア (末端完全一致 = 4 相当) */
export const SHOP_CATEGORY_AUTO_APPLY_MIN_SCORE = 4;

/**
 * 人の確認なしに 1 位を保存してよいか (Codex R1 high 対応)。
 * スコア合計だけで判定すると「親セグメントだけ一致」(例: `日用品 > 洗濯用品 > 無関係A` に対して
 * ジャンルが `… > 日用品 > 洗濯用品`) で 2+2=4 に達し、同点の兄弟から辞書順で無関係な棚が
 * 自動保存されてしまう。そのため **末端そのものの完全一致** と **単独1位** を必須にする。
 */
export function canAutoApplyShopCategory(candidates) {
  const top = (candidates || [])[0];
  if (!top || !top.leafExact || top.score < SHOP_CATEGORY_AUTO_APPLY_MIN_SCORE) return false;
  // 同スコアの対抗馬がいる = どちらの棚か決められない → 人に選ばせる
  if (candidates.length > 1 && candidates[1].score >= top.score) return false;
  return true;
}

/**
 * そのドラフトで店舗内カテゴリが一度も保存されていないか (0 件保存も「保存済み」)。
 * AI 自動適用の可否判定。**書き込みトランザクションの中でも再確認する** (Codex R2 high):
 * GET で autoApply を返した後に人が「全部外して保存」すると、遅れて届いた AI の
 * 自動適用がその意思を上書きしてしまうため。
 */
export function shopCategoriesNeverSaved(db, draftId) {
  return db.prepare(
    `SELECT COUNT(*) AS c FROM draft_events WHERE draft_id = ? AND event = 'shop_categories_saved'`
  ).get(draftId).c === 0;
}

/**
 * ドラフトに紐付けてよいカテゴリ id の件数 (有効なもの + そのドラフトが既に選択中のもの)。
 * 画面・提案の対象 (listShopCategoriesForDraft) と検証条件を揃える (Codex R1 medium)。
 */
export function countSelectableShopCategories(db, draftId, ids) {
  if (!ids || ids.length === 0) return 0;
  const ph = ids.map(() => '?').join(',');
  return db.prepare(`
    SELECT COUNT(*) AS c FROM ph_shop_categories c
    WHERE c.id IN (${ph})
      AND (c.is_active = 1
           OR EXISTS (SELECT 1 FROM draft_shop_categories s
                      WHERE s.shop_category_id = c.id AND s.draft_id = ?))
  `).get(...ids, draftId).c;
}
