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
  // 楽天 RMS の payment.taxRate 仕様 (実測 2026-06-28):
  //   - 軽減税率 (8%) 等の特別設定がある商品: '0.08' 等明示
  //   - 通常商品: payment.taxRate **省略** (= 店舗 default 10%)
  //   B-Faith 店舗運用は default 10%、 軽減税率商品のみ明示するパターン (中原さん確認)。
  //   Codex Phase E-15 R1 H-1: 「missing = 10% default」 で normalize、 軽減税率明示は '8' に。
  if (rakutenTaxRate == null) return '10%';                // 楽天 RMS で省略 = default 10%
  const n = Number(rakutenTaxRate);
  if (!Number.isFinite(n)) return '10%';                   // 不明値も default
  if (Math.abs(n - 0.1) < 0.001) return '10%';
  if (Math.abs(n - 0.08) < 0.001) return '8';              // CHECK 制約: '8' のみ
  return '10%';                                             // その他値も default fallback
}

/**
 * Phase E-16 (2026-06-28): 楽天 variants[].shipping.shippingMethodGroup ID → Notion 配送方法 (8 値) マッピング。
 *
 *   中原さん指定の変換表 (Notion新規商品登録 - 変換表.csv):
 *     楽天 ID 1 (定形外) → Yahoo 3 (定形外) → Notion 8 値で判別不能 (定形外（ヤフーのみ宅急便50）か ネコポス か)
 *     楽天 ID 2 (クリックポスト 旧/使用不可) → Yahoo 9 (クリックポスト) → Notion 「クリックポスト」
 *     楽天 ID 3 (飛脚宅配便) → Yahoo 4 (佐川宅急便) → Notion 8 値に対応無し
 *     楽天 ID 4 (宅急便) → Yahoo 1 (デフォルト設定) → Notion 8 値に対応無し
 *     楽天 ID 5 (ネコポス) → Yahoo 6 (ネコポス) → Notion 「ネコポス」
 *     楽天 ID 6 (クリックポスト) → Yahoo 9 (クリックポスト) → Notion 「クリックポスト」
 *     楽天 ID 7 (ヤマト運輸宅急便) → Yahoo 11 (クロネコ宅急便) → Notion 「ヤマト宅急便」
 *     楽天 ID 8 (宅急便50サイズ以上) → Yahoo 10 (宅急便ヤマト50サイズ用) → Notion 「ヤマト50サイズ」
 *     楽天 ID 9 (ゆうパケットパフ) → Yahoo 12 (ゆうパケットパフ) → Notion 「ゆうパケットパフ」
 *
 *   override (delivery_mapping seed 001_initial.sql:22-23 で実装済): Notion が 定形外（ヤフーのみ宅急便50）→
 *   Yahoo 10、 定形外（ヤフーのみネコポス）→ Yahoo 6 に変換 (本関数の責務外、 publish 時に処理)。
 */
const RAKUTEN_GROUP_TO_NOTION = {
  '1': null,                  // 定形外 — 判別不能 (人間判断)
  '2': 'クリックポスト',
  '3': null,                  // 飛脚宅配便 — Notion 8 値に対応無し
  '4': null,                  // 宅急便 (Yahoo デフォルト設定) — Notion 8 値に対応無し
  '5': 'ネコポス',
  '6': 'クリックポスト',
  '7': 'ヤマト宅急便',
  '8': 'ヤマト50サイズ',
  '9': 'ゆうパケットパフ',
};

export function mapShippingToNotion(rakutenShippingMethodGroupId) {
  if (rakutenShippingMethodGroupId == null) return null;
  const key = String(rakutenShippingMethodGroupId).trim();
  if (!key) return null;
  return RAKUTEN_GROUP_TO_NOTION[key] ?? null;
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

  // 配送方法 (Phase E-16 2026-06-28): 楽天 RMS variants[].shipping.shippingMethodGroup ID から直接 Notion マッピング。
  //   中原さん変換表 (Notion新規商品登録 - 変換表.csv) に基づく: 楽天 ID → Notion 配送方法 8 値。
  //   各 variant の shippingMethodGroup ID を取って distinct mapping。 1 種類なら採用、 複数なら ambiguous で skip。
  //   ID 1/3/4 は Notion 8 値に対応無し or 判別不能 → unmappable で skip (中原さん手動入力)。
  if (existing.notion_delivery_label && String(existing.notion_delivery_label).trim()) {
    skipped.notion_delivery_label = 'already_filled';
  } else {
    const variants = rakutenItem.variants;
    if (!variants || typeof variants !== 'object') {
      skipped.notion_delivery_label = 'rakuten_variants_missing';
    } else {
      const labels = new Set();
      const unmappableIds = new Set();
      const variantList = Object.values(variants);
      let hasShippingGroup = false;
      for (const v of variantList) {
        const groupId = v?.shipping?.shippingMethodGroup;
        if (groupId == null || String(groupId).trim() === '') continue;
        hasShippingGroup = true;
        const label = mapShippingToNotion(groupId);
        if (label) labels.add(label);
        else unmappableIds.add(String(groupId));
      }
      if (!hasShippingGroup) {
        skipped.notion_delivery_label = 'rakuten_shipping_method_group_missing';
      } else if (labels.size === 1 && unmappableIds.size === 0) {
        proposed.notion_delivery_label = [...labels][0];
      } else if (labels.size === 0 && unmappableIds.size > 0) {
        skipped.notion_delivery_label = `unmappable_rakuten_shipping_group:${[...unmappableIds].join(',')}`;
      } else if (labels.size >= 1 && unmappableIds.size > 0) {
        // Codex R1 改善: mapped + unmappable mixed は ambiguous より先に判定 (診断性向上)
        skipped.notion_delivery_label = `ambiguous_rakuten_shipping_group:${[...labels].join('|')},unmappable:${[...unmappableIds].join(',')}`;
      } else {
        // labels.size > 1 && unmappable=0
        skipped.notion_delivery_label = `ambiguous_rakuten_shipping_group:${[...labels].join('|')}`;
      }
    }
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
  if (proposed.notion_delivery_label) {
    properties['配送方法'] = { select: { name: proposed.notion_delivery_label } };
  }
  return properties;
}
