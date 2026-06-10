/**
 * 楽天 RMS getItem + Notion override + delivery/variation → Yahoo editItem fields に変換。
 * ローカル RYS src/services/field-mapper.js の buildYahooEditItemFields を ES module 化。
 *
 * 設計原則 (Phase 0 + Codex Phase C R1 stop):
 *   - pure function (DB / API には触れない、 caller が pre-resolve した値を渡す)
 *   - name は Notion yahoo_title 強制 65 字 truncate (Q24)
 *   - price は Notion 売価 + 佐川条件 (Q15)
 *   - caption は Notion HTML > Notion text > 楽天 salesDescription (Q33)
 *   - 通常品: condition=2 + auc_pref/bcid/category 送る (auc_category=固定 26395 Q27)
 *   - バリ品: condition=1 + auc_* 送らない + subcodes (バリ-resolver)
 *   - aucPrefCode は caller から渡す (Codex C R1: process.env 参照を caller 側へ移動で pure 化)
 *   - display は常に 0 (caller が readiness pass 後に 1 に上げる)
 */

import { sanitizeProductHtml } from '../lib/html-sanitize.js';
import { shouldUseSagawaPrice } from './delivery-resolver.js';

export const YAHOO_TITLE_MAX_LEN = 65;

export const FIXED_FIELDS = Object.freeze({
  cross_border_agency_flag: 1,
  template: 'IT03',
  point_immediate: 1,
  is_drug: 0,
  lead_time_instock: 1000,
  AUC_CATEGORY: 26395,
  AUC_PREF_CODE: 27,
});

const segmenter = new Intl.Segmenter('ja', { granularity: 'grapheme' });
function graphemes(s) { return [...segmenter.segment(s)].map((seg) => seg.segment); }
function graphemeLength(s) { return graphemes(s).length; }

/**
 * 65 字以下に切り詰める。
 *   - 文字数チェックは grapheme cluster ベース
 *   - 区切り文字優先、 無ければハードカット + 「…」 (ローカル RYS lib/grapheme.js 同等の minimum 実装)
 */
const SEPARATOR_RE = /[\s　、。・,，/／|｜【】\[\]()（）]/;
export function truncateYahooTitle(s, { maxLen = YAHOO_TITLE_MAX_LEN } = {}) {
  if (s == null) return '';
  const str = String(s);
  if (graphemeLength(str) <= maxLen) return str;
  const g = graphemes(str);
  const searchStart = Math.max(0, maxLen - 15);
  for (let i = maxLen - 2; i >= searchStart; i -= 1) {
    if (SEPARATOR_RE.test(g[i])) return g.slice(0, i).join('') + '…';
  }
  return g.slice(0, maxLen - 1).join('') + '…';
}

/**
 * Notion override > 楽天 fallback の caption 構築。
 */
export function pickCaptionField(notionOverride, rakutenItem) {
  if (notionOverride?.yahoo_caption_html) return sanitizeProductHtml(notionOverride.yahoo_caption_html);
  if (notionOverride?.yahoo_caption_text) return sanitizeProductHtml(notionOverride.yahoo_caption_text);
  return sanitizeProductHtml(rakutenItem?.salesDescription || '');
}

function mapBoolFlag(value, falseValues = [false, 0, null, undefined]) {
  return falseValues.includes(value) ? 0 : 1;
}

/**
 * 楽天 variants 配列を normalize → 表示可能な最小価格を取る。
 */
export function pickRakutenMinPrice(rakutenItem) {
  const v = rakutenItem?.variants;
  const list = Array.isArray(v) ? v : (v && typeof v === 'object' ? Object.values(v) : []);
  const prices = [];
  for (const item of list) {
    if (item?.hidden) continue;
    const p = parseInt(item?.standardPrice ?? item?.price, 10);
    if (Number.isFinite(p) && p > 0) prices.push(p);
  }
  const direct = parseInt(rakutenItem?.standardPrice ?? rakutenItem?.price, 10);
  if (Number.isFinite(direct) && direct > 0) prices.push(direct);
  if (prices.length === 0) return null;
  return Math.min(...prices);
}

export function resolvePrice({ notionOverride, deliveryRow, rakutenItem }) {
  if (notionOverride && shouldUseSagawaPrice(deliveryRow, notionOverride.yahoo_price_sagawa)) {
    return notionOverride.yahoo_price_sagawa;
  }
  if (notionOverride?.yahoo_price > 0) return notionOverride.yahoo_price;
  return pickRakutenMinPrice(rakutenItem);
}

export function mapKeepStock(rakutenItem) {
  const v = rakutenItem?.variants;
  const list = Array.isArray(v) ? v : (v && typeof v === 'object' ? Object.values(v) : []);
  if (list.length === 0) return 1;
  let anyTrue = false;
  let anyDefined = false;
  for (const item of list) {
    if (!item || item.restockOnCancel === undefined || item.restockOnCancel === null) continue;
    anyDefined = true;
    if (item.restockOnCancel === true || item.restockOnCancel === 1) anyTrue = true;
  }
  if (!anyDefined) return 1;
  return anyTrue ? 1 : 0;
}

/**
 * Yahoo editItem 用 fields object を Phase 0 仕様で構築 (pure)。
 */
export function buildYahooEditItemFields({
  rakutenItem,
  notionOverride,
  deliveryRow,
  variationResult,
  productCategory,
  pathName,
  aucPrefCode,
}) {
  if (!rakutenItem) throw new Error('buildYahooEditItemFields: rakutenItem is required');
  if (!variationResult) throw new Error('buildYahooEditItemFields: variationResult is required');

  const fields = {};

  fields.item_code = String(rakutenItem.itemNumber || rakutenItem.manageNumber || '');

  // name (Notion 強制、 65 字)
  const ytRaw = notionOverride?.yahoo_title || '';
  fields.name = ytRaw ? truncateYahooTitle(ytRaw, { maxLen: YAHOO_TITLE_MAX_LEN }) : '';

  // price
  const price = resolvePrice({ notionOverride, deliveryRow, rakutenItem });
  if (price !== null && price > 0) fields.price = price;

  // delivery / postage_set
  if (deliveryRow) {
    fields.delivery = deliveryRow.yahoo_delivery;
    fields.postage_set = deliveryRow.yahoo_postage_set;
  }

  // condition / variation
  fields.condition = variationResult.condition;
  if (variationResult.isVariation) {
    if (variationResult.subcodes?.length > 0) {
      fields.subcodes = variationResult.subcodes;
    }
  } else if (variationResult.sendAucFields) {
    fields.auc_pref_code = Number.isFinite(aucPrefCode) ? aucPrefCode : FIXED_FIELDS.AUC_PREF_CODE;
    fields.auc_bcid = fields.item_code;
    fields.auc_category = FIXED_FIELDS.AUC_CATEGORY;
  }

  // caption / explanation / additional1 / sp_additional (Q33)
  fields.caption = pickCaptionField(notionOverride, rakutenItem);
  fields.explanation = sanitizeProductHtml(rakutenItem.productDescription?.pc || '');
  fields.additional1 = sanitizeProductHtml(rakutenItem.salesDescription || '');
  fields.sp_additional = sanitizeProductHtml(rakutenItem.productDescription?.sp || '');

  // headline / jan (Notion override 任意)
  if (notionOverride?.yahoo_headline) fields.headline = notionOverride.yahoo_headline;
  if (notionOverride?.yahoo_jan) fields.jan = String(notionOverride.yahoo_jan);

  // 楽天 features 由来 flags
  const features = rakutenItem.features || {};
  fields.y_shopping_display_flag = features.searchVisibility === 'HIDDEN' ? 0 : 1;
  fields.item_social_gift_type = mapBoolFlag(features.socialGiftFlag);
  fields.subscription_type = mapBoolFlag(features.displaySubscriptionCartButton);
  fields.keep_stock = mapKeepStock(rakutenItem);

  // 固定値
  fields.cross_border_agency_flag = FIXED_FIELDS.cross_border_agency_flag;
  fields.template = FIXED_FIELDS.template;
  fields.point_immediate = FIXED_FIELDS.point_immediate;
  fields.is_drug = FIXED_FIELDS.is_drug;
  fields.lead_time_instock = FIXED_FIELDS.lead_time_instock;

  // category / path
  if (productCategory) fields.product_category = productCategory;
  if (pathName) fields.path = pathName;

  // display は readiness pass 後に caller が 1 に上げる
  fields.display = 0;

  return fields;
}
