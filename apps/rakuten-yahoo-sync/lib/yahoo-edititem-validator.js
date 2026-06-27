/**
 * Yahoo Shopping editItem field の preflight validator (fail-closed)。
 *
 * publish-executor が Yahoo に送る fields を組み立てた直後、 editItem 呼出前に通す。
 * Yahoo 側 HTTP 400 (it-01033 等) を表面化前に止めて、 publish-runner で `failed`
 * status + 明確な理由を残す。 retryable なので Notion で fix → 再 smoke でループ可能。
 *
 * 範囲:
 *   - 長さ (yahoo-text の units 計算で公式上限と照合)
 *   - 形式 (item_code / jan / product_category / path)
 *
 * Codex Phase E editItem R2 H-1: item_code は editItem 公式上 99 文字 + 半角英数+ハイフン だが、
 *   bfaith-portal の uploadItemImage 側 (PR #353 contract rewrite) は 80 文字 + underscore も許容。
 *   実運用 SKU は英数字のみで underscore は使われていないため、 publish preflight は
 *   editItem 厳格仕様の `^[A-Za-z0-9-]{1,80}$` に寄せ、 uploadItemImage 経路で通ったが
 *   editItem で落ちる SKU を事前検知する。
 */
import { yahooTextUnits } from './yahoo-text.js';

const LIMITS_UNITS = {
  explanation:   1000,    // 全角 500
  caption:      10000,    // 全角 5000
  additional1:  10000,    // 全角 5000
  sp_additional: 10000,   // 全角 5000
  headline:        60,    // 全角 30
};

// Codex R3 H-1: explanation / headline は HTML 不可。 plain 化漏れを fail-closed で検知
const NO_HTML_FIELDS = ['explanation', 'headline'];
const HTML_TAG_RE = /<[A-Za-z!\/][^>]*>/;

const ITEM_CODE_RE = /^[A-Za-z0-9-]{1,80}$/;
const JAN_RE = /^[A-Za-z0-9-]{1,17}$/;
const PRODUCT_CATEGORY_RE = /^\d{1,10}$/;

const PATH_MAX_SEGMENTS = 8;
const PATH_SEGMENT_MAX_UNITS = 40; // 全角 20

export class YahooFieldValidationError extends Error {
  constructor(field, message, { value = null } = {}) {
    super(`[yahoo-field:${field}] ${message}`);
    this.name = 'YahooFieldValidationError';
    this.field = field;
    this.fieldValue = value;
  }
}

export function validateYahooEditItemFields(fields) {
  if (!fields || typeof fields !== 'object') {
    throw new YahooFieldValidationError('_root', 'fields must be object');
  }

  for (const [k, max] of Object.entries(LIMITS_UNITS)) {
    const v = fields[k];
    if (v == null || v === '') continue;
    const n = yahooTextUnits(v);
    if (n > max) {
      throw new YahooFieldValidationError(k, `length ${n} > ${max} units (= 全角 ${Math.floor(max / 2)})`);
    }
  }

  // Codex R3 H-1: HTML 不可 field (explanation / headline) に HTML タグが残っていたら fail-closed
  for (const k of NO_HTML_FIELDS) {
    const v = fields[k];
    if (v == null || v === '') continue;
    if (HTML_TAG_RE.test(String(v))) {
      throw new YahooFieldValidationError(k, `HTML tag not allowed in this field`);
    }
  }

  if (fields.item_code != null && !ITEM_CODE_RE.test(String(fields.item_code))) {
    throw new YahooFieldValidationError('item_code', `format invalid (expected [A-Za-z0-9-]{1,80}): ${fields.item_code}`);
  }

  if (fields.jan != null && fields.jan !== '' && !JAN_RE.test(String(fields.jan))) {
    throw new YahooFieldValidationError('jan', `format invalid (expected [A-Za-z0-9-]{1,17}): ${fields.jan}`);
  }

  if (fields.product_category != null && !PRODUCT_CATEGORY_RE.test(String(fields.product_category))) {
    throw new YahooFieldValidationError('product_category', `format invalid (expected \\d{1,10}): ${fields.product_category}`);
  }

  if (fields.path) {
    const segs = String(fields.path).split(':');
    if (segs.length > PATH_MAX_SEGMENTS) {
      throw new YahooFieldValidationError('path', `> ${PATH_MAX_SEGMENTS} segments: ${fields.path}`);
    }
    for (const s of segs) {
      const n = yahooTextUnits(s);
      if (n > PATH_SEGMENT_MAX_UNITS) {
        throw new YahooFieldValidationError('path', `segment too long (> ${PATH_SEGMENT_MAX_UNITS} units = 全角 ${PATH_SEGMENT_MAX_UNITS / 2}): ${s}`);
      }
    }
  }
}
