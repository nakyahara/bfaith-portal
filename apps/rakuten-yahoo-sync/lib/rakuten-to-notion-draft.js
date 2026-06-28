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
 * Phase E-15 (2026-06-28): pd_shipping_rule.shipping_method_code → Notion 配送方法 (8 値) マッピング。
 *
 *   notion_overrides.notion_delivery_label の CHECK 制約 + delivery_mapping FK seed (migration 012) と整合:
 *     ネコポス / ヤマト50サイズ / ヤマト宅急便 / 沖縄北海道別送料_ヤマト宅急便 / クリックポスト /
 *     ゆうパケットパフ / 定形外（ヤフーのみ宅急便50サイズ） / 定形外（ヤフーのみネコポス）
 *
 *   Codex Phase E-15 R2 H-1: teikeigai は pd_shipping_rule CHECK 制約 (packing='manual' OR shipping='nekopos')
 *   により packing=manual のみで存在し、 packing から「ネコポスサイズ vs 宅急便50サイズ」 を判別できない。
 *   よって teikeigai は本 PR では skip (中原さん手動入力、 将来 商品サイズマスタ追加時に分岐実装)。
 */
export function mapShippingToNotion(shippingMethodCode) {
  if (!shippingMethodCode) return null;
  switch (shippingMethodCode) {
    case 'nekopos': return 'ネコポス';
    case 'takkyu50': return 'ヤマト50サイズ';
    case 'hatsubarai': return 'ヤマト宅急便';
    case 'yupacketpuff': return 'ゆうパケットパフ';
    case 'clickpost': return 'クリックポスト';
    // teikeigai は Notion 8 値の 「定形外（ヤフーのみ宅急便50）」 か 「定形外（ヤフーのみネコポス）」
    //   どちらかになるが、 packing からは判別できない (Codex R2 H-1) → skip
    case 'teikeigai': return null;
    // letterpack (沖縄北海道別送料用) / aes / yupack / fukuyamaistar2 / seinokangaroom2 は
    //   Notion 8 値に対応なし → skip (中原さん手動入力)
    default: return null;
  }
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
export function buildNotionDraftProposal(rakutenItem, notionOverride, opts = {}) {
  const proposed = {};
  const skipped = {};

  if (!rakutenItem || typeof rakutenItem !== 'object') {
    return { proposed, skipped: { _all: 'no_rakuten_item' } };
  }
  const existing = notionOverride || {};
  const warehouseDb = opts.warehouseDb || null;
  const manageNumber = rakutenItem.manageNumber || rakutenItem.itemNumber;

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

  // 配送方法 (Phase E-15 2026-06-28): packing-dispatch アプリの pd_shipping_rule から lookup。
  //   product_code (= 楽天 manage_number) で引いて、 shipping_method_code → Notion 配送方法 8 値 にマッピング。
  if (existing.notion_delivery_label && String(existing.notion_delivery_label).trim()) {
    skipped.notion_delivery_label = 'already_filled';
  } else if (!warehouseDb) {
    skipped.notion_delivery_label = 'warehouse_db_unavailable';
  } else if (!manageNumber) {
    skipped.notion_delivery_label = 'manage_number_missing';
  } else {
    let rows = [];
    try {
      // Codex Phase E-15 R3 H-1: product_code 配下に variant 別の rule が複数ある場合、
      //   各 sku_key で配送方法が違う可能性 → distinct label で全部取って 1 種類なら採用、
      //   複数なら ambiguous_shipping_rule で skip (Notion 親項目に誤った 1 variant を固定化させない)。
      //   mall_group 優先順位は rakuten > default。 rakuten で hit したらそれを使い、 default は見ない。
      rows = warehouseDb.prepare(`
        SELECT DISTINCT shipping_method_code, packing_machine_code
        FROM pd_shipping_rule
        WHERE product_code = ?
          AND mall_group = ?
          AND qty_min <= 1 AND (qty_max IS NULL OR qty_max >= 1)
      `).all(manageNumber, 'rakuten');
      if (rows.length === 0) {
        rows = warehouseDb.prepare(`
          SELECT DISTINCT shipping_method_code, packing_machine_code
          FROM pd_shipping_rule
          WHERE product_code = ?
            AND mall_group = ?
            AND qty_min <= 1 AND (qty_max IS NULL OR qty_max >= 1)
        `).all(manageNumber, 'default');
      }
    } catch (e) {
      // pd_shipping_rule table 不在 (warehouse-mirror.db に同居してない環境) は silent
      skipped.notion_delivery_label = `pd_shipping_rule_query_failed:${e.message}`;
    }
    if (rows.length > 0) {
      // 各 row を Notion label にマッピングして distinct 集計
      const labels = new Set();
      const unmappable = new Set();
      for (const r of rows) {
        const label = mapShippingToNotion(r.shipping_method_code);
        if (label) labels.add(label);
        else unmappable.add(r.shipping_method_code);
      }
      if (labels.size === 1 && unmappable.size === 0) {
        proposed.notion_delivery_label = [...labels][0];
      } else if (labels.size === 0 && unmappable.size > 0) {
        skipped.notion_delivery_label = `unmappable_shipping:${[...unmappable].join(',')}`;
      } else if (labels.size > 1) {
        skipped.notion_delivery_label = `ambiguous_shipping_rule:${[...labels].join('|')}`;
      } else {
        // labels=1 + unmappable も同時にあるケース → variant 別で対応分かれてるので skip
        skipped.notion_delivery_label = `ambiguous_shipping_rule:${[...labels].join('|')},unmappable:${[...unmappable].join(',')}`;
      }
    } else if (!skipped.notion_delivery_label) {
      skipped.notion_delivery_label = 'pd_shipping_rule_missing';
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
