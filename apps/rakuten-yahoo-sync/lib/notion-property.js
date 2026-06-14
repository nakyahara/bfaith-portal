/**
 * Notion page.properties から RYS が使う 12 列を抽出する pure function 群。
 * ローカル RYS src/lib/notion-property.js の ES module 翻訳 (logic 変更なし、 Phase A-D で確定)。
 *
 * 設計原則 (Codex Phase B1 提案):
 *   - publish に影響する 12 列だけ抽出して canonical JSON 化 → SHA-256
 *   - 空文字は null に正規化 (Notion で property を空にしたら DB も NULL)
 *   - 比較は string trim 済み、 number は parseInt/parseFloat 通した値
 *
 * 関連: db/migrations/001_initial.sql (notion_overrides の 12 列カラム定義)
 */

import crypto from 'crypto';

// ─── 個別 extractor (Notion property の type ごとに値を取り出す) ───

export function extractTitle(prop) {
  if (!prop || prop.type !== 'title' || !Array.isArray(prop.title)) return null;
  const s = prop.title.map((p) => p.plain_text || '').join('').trim();
  return s === '' ? null : s;
}

export function extractRichText(prop) {
  if (!prop || prop.type !== 'rich_text' || !Array.isArray(prop.rich_text)) return null;
  const s = prop.rich_text.map((p) => p.plain_text || '').join('').trim();
  return s === '' ? null : s;
}

export function extractNumber(prop) {
  if (!prop || prop.type !== 'number') return null;
  const v = prop.number;
  if (v === null || v === undefined) return null;
  return Number.isFinite(v) ? v : null;
}

export function extractSelect(prop) {
  if (!prop || prop.type !== 'select') return null;
  const v = prop.select;
  if (!v || typeof v.name !== 'string') return null;
  const s = v.name.trim();
  return s === '' ? null : s;
}

// Notion JANコードは number 型だが文字列で保管したい (先頭 0 / 巨大値対応)
export function extractNumberAsString(prop) {
  const n = extractNumber(prop);
  if (n === null) return null;
  return String(n);
}

// ─── 商品マスター DB の property name → 抽出関数の対応 ───
// 名前は Notion DB のプロパティ表示名 (2026-05-27 schema 確定)。
export const PROPERTY_EXTRACTORS = {
  rakuten_manage_number: { prop: '商品コード',                            extract: extractRichText },
  yahoo_title:           { prop: 'Yahoo!タイトル',                          extract: extractRichText },
  yahoo_price:           { prop: '売価',                                   extract: extractNumber },
  yahoo_price_sagawa:    { prop: 'Yahoo価格（佐川の場合）',                extract: extractNumber },
  notion_delivery_label: { prop: '配送方法',                               extract: extractSelect },
  notion_tax_rate:       { prop: '税率',                                   extract: extractSelect },
  notion_has_variation:  { prop: 'バリエーション有無',                     extract: extractSelect },
  yahoo_headline:        { prop: 'キャッチコピー',                         extract: extractRichText },
  yahoo_caption_text:    { prop: '商品説明文',                             extract: extractRichText },
  yahoo_caption_html:    { prop: 'HTML_商品説明文',                        extract: extractRichText },
  yahoo_jan:             { prop: 'JANコード',                              extract: extractNumberAsString },
  notion_status:         { prop: 'Status',                                 extract: extractSelect },
  // Phase E-11-d: Yahoo!ショッピング カテゴリ確定値 + path 確定値
  // Notion 商品マスターに 「Yahoo!カテゴリID」 + 「Yahoo!path」 列を追加すると取り込まれる。
  // 列が無い場合は extract が null を返すので無害 (publish 時は学習辞書 fallback)。
  notion_product_category: { prop: 'Yahoo!カテゴリID',                       extract: extractNumber },
  notion_path:             { prop: 'Yahoo!path',                            extract: extractRichText },
};

/**
 * Notion page object から RYS で扱う 12 列に正規化する。
 * @param {object} page  Notion API page object (results[i])
 * @returns {object|null} { rakuten_manage_number, yahoo_title, ... } もしくは PK 不在で null
 */
export function extractRow(page) {
  if (!page || !page.properties) return null;
  const row = {};
  for (const [col, { prop, extract }] of Object.entries(PROPERTY_EXTRACTORS)) {
    row[col] = extract(page.properties[prop]);
  }
  // PK (商品コード) なしは sync 対象外
  if (!row.rakuten_manage_number) return null;
  return row;
}

/**
 * canonical JSON 化 → SHA-256。
 *   publish に影響しない列の変更で UPDATE 発火しないようにする。
 *
 * Codex E-11 R1 H-1 修正: migration 014 未適用環境では新列を hash 対象から除外する。
 *   そうしないと: insert 時に hash は新列込みで保存される が 新列は DB に書かれない →
 *   後で migration 014 適用後に同 Notion ページを sync しても hash が一致して
 *   touchStmt 経路に落ちて、 実カラムは永久 NULL のままになる。
 *   includeNewColumns=false で hash 算出すれば、 migration 適用後の sync で hash が
 *   変わり update が発火する。
 */
const NEW_NOTION_COLUMNS = ['notion_product_category', 'notion_path'];
export function canonicalHash(row, { includeNewColumns = true } = {}) {
  if (!row) throw new Error('canonicalHash: row is required');
  const keys = Object.keys(PROPERTY_EXTRACTORS)
    .filter((k) => includeNewColumns || !NEW_NOTION_COLUMNS.includes(k))
    .sort();
  const canon = {};
  for (const k of keys) canon[k] = row[k] === undefined ? null : row[k];
  const json = JSON.stringify(canon);
  return crypto.createHash('sha256').update(json, 'utf8').digest('hex');
}

/**
 * Notion page から「sync 対象 12 列 + 監査列」 record を組み立てる。
 *
 * @param {object} page  Notion API page object
 * @param {object} [opts]
 * @param {boolean} [opts.includeNewColumnsInHash=true]  migration 014 未適用環境では false を渡す
 */
export function buildSyncRecord(page, { includeNewColumnsInHash = true } = {}) {
  const row = extractRow(page);
  if (!row) return null;
  return {
    ...row,
    notion_page_id: page.id,
    source_updated_at: page.last_edited_time || null,
    raw_properties_json: JSON.stringify(page.properties),
    source_hash: canonicalHash(row, { includeNewColumns: includeNewColumnsInHash }),
  };
}
