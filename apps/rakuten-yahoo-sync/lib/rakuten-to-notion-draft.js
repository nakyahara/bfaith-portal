/**
 * 楽天 RMS rakutenItem → Notion override の自動下書き値を構築。
 *
 * 設計 (Codex Phase E-13 R1):
 *   - 自動補完対象 (Notion 列 → 楽天 RMS 値):
 *     - Yahoo!タイトル ← rakutenItem.title 65 字 truncate
 *     - 売価           ← variants[].standardPrice (税込)
 *     - 税率           ← payment.taxRate (0.1 → '10%')
 *   - 自動補完対象外 (本 PR スコープ外):
 *     - 配送方法 — 楽天 normalDeliveryDateId は配送業者ではなく lead time 値、 別ロジック要 (R1 H-1)
 *   - 既存値 (Notion で既に値が入ってる項目) は touchしない
 *   - 補完値が組み立てられない場合 (空 / 不正値) は skip + 理由を返す
 *
 * 中原さん運用想定:
 *   1. dashboard ボタンで dry-run 表示 → 補完予定セル一覧と skip 理由を確認
 *   2. 問題なければ apply で Notion 一括 PATCH
 *   3. 中原さんが Notion で確認・微修正 → 「⑨Yahoo!登録」 ステータス変更
 */

const YAHOO_TITLE_MAX_LEN = 65;

/**
 * 楽天 title を Yahoo タイトル 65 字以内に切り詰める。
 *   65 字超なら 末尾を `…` 含めて切る (Codex R1 Q6: 機械短縮で OK、 人が後で直せる前提)。
 */
export function truncateYahooTitle(rakutenTitle) {
  if (typeof rakutenTitle !== 'string') return null;
  const s = rakutenTitle.trim();
  if (!s) return null;
  if (s.length <= YAHOO_TITLE_MAX_LEN) return s;
  // 65 字以内に収める (末尾省略記号なし — 中原さんが Notion で直す前提)
  return s.slice(0, YAHOO_TITLE_MAX_LEN);
}

/**
 * 楽天 payment.taxRate (0.1 / 0.08) → Notion 税率 select label ('10%' / '8%')。
 *   未知値は null (skip)。
 */
export function convertTaxRate(rakutenTaxRate) {
  if (rakutenTaxRate == null) return null;
  const n = Number(rakutenTaxRate);
  if (!Number.isFinite(n)) return null;
  // notion_overrides.notion_tax_rate CHECK 制約は ('10', '10%', '8', '#REF!') で
  // 8% は '8' のみ許可、 10% は '10%' (Notion 既存データに合わせる、 Codex R2 H-1)。
  if (Math.abs(n - 0.1) < 0.001) return '10%';
  if (Math.abs(n - 0.08) < 0.001) return '8';
  if (Math.abs(n - 0) < 0.001) return null; // 0% は税率設定なし
  return null;
}

/**
 * variants[].standardPrice から代表価格を取得。
 *   - 全 variant に standardPrice あり + 全て同値なら その値
 *   - 異なる価格があれば最小値 (中原さん運用: 単純化のため最小値で下書き、 中原さん確認時に修正)
 *   - 楽天 standardPrice は文字列で来ることがあるので parseInt
 */
export function pickRepresentativePrice(rakutenItem) {
  const variants = rakutenItem?.variants;
  if (!variants || typeof variants !== 'object') return null;
  const prices = Object.values(variants)
    .map((v) => (v?.standardPrice ? parseInt(String(v.standardPrice), 10) : null))
    .filter((p) => Number.isFinite(p) && p > 0);
  if (prices.length === 0) return null;
  return Math.min(...prices);
}

/**
 * rakutenItem から Notion properties の補完候補を組み立て。
 *
 * @param {object} rakutenItem 楽天 RMS getItem の生 response (publish-pipeline で使ってる形)
 * @param {object} notionOverride 既存の Notion override 値 (空欄判定用)
 * @returns {{ proposed: object, skipped: object }} proposed=新規補完したい値、 skipped={ reason }
 */
export function buildNotionDraftProposal(rakutenItem, notionOverride) {
  const proposed = {};
  const skipped = {};

  if (!rakutenItem || typeof rakutenItem !== 'object') {
    return { proposed, skipped: { _all: 'no_rakuten_item' } };
  }
  const existing = notionOverride || {};

  // Yahoo!タイトル — RMS の field 名は itemName か title (field-mapper と同形 fallback、 Codex R3 H-1)
  if (existing.yahoo_title && String(existing.yahoo_title).trim()) {
    skipped.yahoo_title = 'already_filled';
  } else {
    const t = truncateYahooTitle(rakutenItem.itemName || rakutenItem.title);
    if (t) proposed.yahoo_title = t;
    else skipped.yahoo_title = 'rakuten_title_missing';
  }

  // 売価 (yahoo_price)
  if (existing.yahoo_price != null && Number.isFinite(Number(existing.yahoo_price)) && Number(existing.yahoo_price) > 0) {
    skipped.yahoo_price = 'already_filled';
  } else {
    const p = pickRepresentativePrice(rakutenItem);
    if (p) proposed.yahoo_price = p;
    else skipped.yahoo_price = 'rakuten_price_missing';
  }

  // 税率
  if (existing.notion_tax_rate && String(existing.notion_tax_rate).trim()) {
    skipped.notion_tax_rate = 'already_filled';
  } else {
    const t = convertTaxRate(rakutenItem.payment?.taxRate);
    if (t) proposed.notion_tax_rate = t;
    else skipped.notion_tax_rate = 'rakuten_tax_rate_unknown';
  }

  // 配送方法は本 PR スコープ外 (Codex R1 H-1)
  if (existing.notion_delivery_label && String(existing.notion_delivery_label).trim()) {
    skipped.notion_delivery_label = 'already_filled';
  } else {
    skipped.notion_delivery_label = 'not_implemented_in_this_pr (楽天 normalDeliveryDateId は lead time で配送業者ではないため別ロジック要)';
  }

  return { proposed, skipped };
}

/**
 * Notion API の properties value 形式に変換。
 *   - Yahoo!タイトル → rich_text (Notion 列名 'Yahoo!タイトル')
 *   - 売価           → number (列名 '売価')
 *   - 税率           → select (列名 '税率')
 *
 * 注意: Notion 列名は商品マスター DB のプロパティ表示名 (中原さん設定)。
 *   PR #343 の seed-category と同じ表記。
 */
export function toNotionProperties(proposed) {
  const properties = {};
  if (proposed.yahoo_title) {
    properties['Yahoo!タイトル'] = {
      rich_text: [{ type: 'text', text: { content: String(proposed.yahoo_title) } }],
    };
  }
  if (Number.isFinite(proposed.yahoo_price)) {
    properties['売価'] = { number: proposed.yahoo_price };
  }
  if (proposed.notion_tax_rate) {
    properties['税率'] = { select: { name: proposed.notion_tax_rate } };
  }
  return properties;
}
