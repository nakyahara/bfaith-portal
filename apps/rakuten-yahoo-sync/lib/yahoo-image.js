/**
 * Yahoo 用 画像 URL filter (Phase 0 Q28 + Codex R5 high-7 + Phase E-8 確定):
 *   楽天 images[] から **組込み (coupon / review) + ユーザー追加** パターンにマッチする URL を除外する。
 *
 * 組込みパターンは hardcode のまま維持 (lib/image-exclusion-patterns.js が UI 管理する custom と区別)。
 * 中原さん R1 確定: 部分一致 + 大文字小文字区別なし。
 *
 * 注意: 実 download + JPEG 変換 + uploadItemImage は E-5 で実装する。
 * 本ファイルは E-4 用 (readiness preflight に imagePreflight.ok を渡すための pure filter) +
 * E-8 で詳細情報 (どのパターンで除外されたか) を返す版を追加。
 */

export const EXCLUDED_URL_PATTERNS = ['coupon', 'review'];

/**
 * 1 image エントリ (string URL or { location, ... }) から URL を取り出す。
 */
function extractUrl(img) {
  const url = typeof img === 'string' ? img : (img?.location || '');
  if (typeof url !== 'string') return '';
  return url.trim();
}

/**
 * 楽天 images[] から Yahoo upload 対象の URL だけ抽出 (既存 API、 後方互換)。
 *   - images の要素は { location, ... } object (楽天 RMS 形式) or string URL の両方を許容
 *   - URL (小文字化後) に組込み (coupon/review) + customPatterns のいずれかが含まれる場合は除外
 *   - falsy / 空文字 / 非文字列 URL は除外
 *
 * @param {Array} images
 * @param {object} [opts]
 * @param {string[]} [opts.customPatterns]  - ユーザー追加分 (image-exclusion-patterns table 由来)
 * @returns {string[]} keep される URL の配列
 */
export function filterUploadableImageUrls(images, { customPatterns = [] } = {}) {
  if (!Array.isArray(images)) return [];
  const allPatterns = [...EXCLUDED_URL_PATTERNS, ...customPatterns.map((p) => String(p).toLowerCase())];
  const out = [];
  for (const img of images) {
    const url = extractUrl(img);
    if (!url) continue;
    const lower = url.toLowerCase();
    let exclude = false;
    for (const p of allPatterns) {
      if (typeof p !== 'string' || p.length === 0) continue;
      if (lower.includes(p)) { exclude = true; break; }
    }
    if (!exclude) out.push(url);
  }
  return out;
}

/**
 * Phase E-8 追加: filter の詳細結果を返す (UI ドロワー表示用)。
 *   keep / excluded の両方を返し、 excluded はマッチした pattern と source (builtin/custom) を含む。
 *
 * @param {Array} images
 * @param {object} [opts]
 * @param {string[]} [opts.customPatterns]
 * @returns {{ kept: Array<{url}>, excluded: Array<{url, matchedPattern, source: 'builtin'|'custom'}> }}
 */
export function filterUploadableImageUrlsDetailed(images, { customPatterns = [] } = {}) {
  const result = { kept: [], excluded: [] };
  if (!Array.isArray(images)) return result;
  // Codex E-8 R1 M-2: 比較用 lowercase / 表示用 original を分離
  const builtinPairs = EXCLUDED_URL_PATTERNS.map((p) => ({ original: p, lower: p.toLowerCase() }));
  const customPairs = customPatterns
    .filter((p) => typeof p === 'string' && p.length > 0)
    .map((p) => ({ original: p, lower: p.toLowerCase() }));
  for (const img of images) {
    const url = extractUrl(img);
    if (!url) continue;
    const lower = url.toLowerCase();
    let matched = null;
    let source = null;
    for (const pair of builtinPairs) {
      if (lower.includes(pair.lower)) { matched = pair.original; source = 'builtin'; break; }
    }
    if (!matched) {
      for (const pair of customPairs) {
        if (lower.includes(pair.lower)) { matched = pair.original; source = 'custom'; break; }
      }
    }
    if (matched) {
      result.excluded.push({ url, matchedPattern: matched, source });
    } else {
      result.kept.push({ url });
    }
  }
  return result;
}

/**
 * 画像 preflight stub (E-4): 「フィルター後 1 枚以上残れば ok」 と判定。
 * E-5 で実 uploadItemImage を呼ぶようにここを差し替える予定。
 */
export function imagePreflightStub(rakutenItem, { customPatterns = [] } = {}) {
  const urls = filterUploadableImageUrls(rakutenItem?.images, { customPatterns });
  if (urls.length === 0) {
    return { ok: false, error: 'no_uploadable_image_after_filter' };
  }
  return { ok: true, urlsAvailable: urls.length };
}
