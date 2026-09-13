/**
 * 画像タブ「画像制作 → 商品情報」の自動表示 (2026-09-13 スタッフ要望)。
 *
 * それまでは 商品説明タブの説明文 (楽天の PC用商品説明文) と、バリエーションがあれば
 * 基本情報タブの SKU を人がコピペしていた。同じ説明文をここで組み立てて文字にする。
 *   - 手入力 (draft_image_production.product_info_text) があればそちらが優先 (直して保存した値)。
 *     無ければ自動の説明文。定型文 3 種・① の完了条件・ボードの「商品情報 未入力」も同じ見方にする
 *   - カラバリはここに入れない。定型文は composeColorVariations で別に足すので、入れると二重になる
 *     (画面ではカラバリを商品情報の下に読み取り専用で出す)
 *   - HTML でなく文字にする (2026-09-13 中原さん回答: 画面で読めて、ChatGPT にもそのまま渡せる)
 * workflow-progress.js と rakuten-listing.js の両方から使うので、page-info.js 以外を import しない (循環させない)。
 */
import { buildPageInfoHtml, FIXED_NOTES } from './page-info.js';

/** 「説明」行の材料 = AI の特徴・仕様。どちらかが入っていれば自動の説明文がある */
export const AUTO_DESC_KINDS = ['desc_features', 'desc_spec'];

/**
 * 楽天の PC用商品説明文 (HTML)。商品説明タブの 3 欄・出品 payload・画像タブの自動の商品情報が
 * 同じものを使う (別々に組むと、画像タブだけ中身がズレる)。
 * 「説明」行 = AI特徴 + AI仕様 (仕様表・注意書きは表の別行に載る)。
 * **楽天タイトルは入れない** (2026-08-31 中原さん): タイトルは検索用に語を並べたもので、
 * 説明として読ませる文ではない。表の先頭に丸ごと出ると SEO 語の羅列がそのまま載る
 */
export function buildPcDescriptionHtml({ productName, ai, specs, pageInfo }) {
  const descTexts = [];
  if (ai?.desc_features) descTexts.push(String(ai.desc_features).trim());
  if (ai?.desc_spec) descTexts.push(String(ai.desc_spec).trim());
  return buildPageInfoHtml({
    // AI 文が 1 つも無いときだけ「商品名」行として使われる。ここは NE の商品名を渡す
    // (楽天タイトルを渡すと、上で外したはずの SEO 語がフォールバックで出てしまう)
    productName,
    info: pageInfo, // 未保存 (null) でも説明/注意事項/仕様表/広告文責の行は載る
    descriptionText: descTexts.join('\n\n'),
    notesText: ai?.desc_notes ? String(ai.desc_notes).trim() : null,
    specs,
  });
}

const ENTITIES = { amp: '&', lt: '<', gt: '>', quot: '"', '#39': "'", nbsp: ' ' };

/** セル 1 つの HTML → 文字 (<br> は改行・タグは落とす・文字参照は戻す) */
function cellToText(html) {
  return String(html || '')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<[^>]*>/g, '')
    .replace(/&(amp|lt|gt|quot|#39|nbsp);/g, (_, k) => ENTITIES[k])
    .split('\n').map((l) => l.trim()).join('\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

/** 全商品で同じ文面の行。商品の情報ではないので自動の商品情報には入れない */
const SKIP_LABELS = new Set(['広告文責']);

/**
 * PC用商品説明文の表 → 「見出し：値」の行 (値が複数行なら見出しの次の行から)。
 * 「注意事項」の店舗の固定注意書き (FIXED_NOTES) も全商品で同じなので落とす
 */
export function descriptionHtmlToText(html) {
  const fixed = cellToText(FIXED_NOTES);
  const blocks = [];
  for (const tr of String(html || '').matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/gi)) {
    const cells = [...tr[1].matchAll(/<td[^>]*>([\s\S]*?)<\/td>/gi)].map((m) => cellToText(m[1]));
    if (cells.length < 2) continue;
    const [label] = cells;
    let value = cells[1];
    if (!label || SKIP_LABELS.has(label)) continue;
    if (fixed) value = value.replace(fixed, '').trim();
    if (!value) continue;
    blocks.push(value.includes('\n') ? `${label}：\n${value}` : `${label}：${value}`);
  }
  // 複数行の塊 (説明など) の後ろは 1 行空けて、次の見出しと続けて読めないようにする
  return blocks.map((b) => (b.includes('\n') ? `${b}\n` : b)).join('\n').trim();
}

/** 自動の説明文があるか (① の完了条件・ボードの「商品情報 未入力」と同じ基準) */
export function hasAutoDescription(db, draftId) {
  return !!db.prepare(`
    SELECT 1 FROM draft_ai_outputs
    WHERE draft_id = ? AND kind IN (${AUTO_DESC_KINDS.map(() => '?').join(', ')}) AND TRIM(COALESCE(content, '')) <> ''
    LIMIT 1
  `).get(draftId, ...AUTO_DESC_KINDS);
}

/**
 * 自動の商品情報 (文字)。AI の特徴・仕様がまだ無ければ '' —「商品名」だけの表を商品情報と呼ばない
 * (空なら画面が「まだ無い」と出し、人が入れる)
 */
export function autoProductInfoText(db, draftId) {
  if (!hasAutoDescription(db, draftId)) return '';
  const draft = db.prepare('SELECT name FROM product_drafts WHERE id = ?').get(draftId);
  if (!draft) return '';
  const ai = {};
  for (const r of db.prepare('SELECT kind, content FROM draft_ai_outputs WHERE draft_id = ?').all(draftId)) {
    ai[r.kind] = r.content;
  }
  const specs = db.prepare('SELECT spec_key, spec_value FROM draft_specs WHERE draft_id = ? ORDER BY sort, id').all(draftId);
  const pageInfo = db.prepare('SELECT * FROM draft_page_info WHERE draft_id = ?').get(draftId) || null;
  return descriptionHtmlToText(buildPcDescriptionHtml({ productName: draft.name, ai, specs, pageInfo }));
}

/** 定型文が使う「いまの商品情報」= 手入力があればそれ、無ければ自動 */
export function effectiveProductInfo(manualText, autoText) {
  return String(manualText || '').trim() ? String(manualText) : String(autoText || '');
}
