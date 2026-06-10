/**
 * Yahoo 用 画像 URL filter (Phase 0 Q28 + Codex R5 high-7 確定):
 *   楽天 images[] から coupon / review URL を除外する pure function。
 *
 * 注意: 実 download + JPEG 変換 + uploadItemImage は E-5 で実装する。
 * 本ファイルは E-4 用 (readiness preflight に imagePreflight.ok を渡すための pure filter)。
 */

export const EXCLUDED_URL_PATTERNS = ['coupon', 'review'];

/**
 * 楽天 images[] から Yahoo upload 対象の URL だけ抽出。
 *   - images の要素は { location, ... } object (楽天 RMS 形式) or string URL の両方を許容
 *   - URL (小文字化後) に coupon / review が含まれる場合は除外
 *   - falsy / 空文字 / 非文字列 URL は除外
 */
export function filterUploadableImageUrls(images) {
  if (!Array.isArray(images)) return [];
  const out = [];
  for (const img of images) {
    const url = typeof img === 'string' ? img : (img?.location || '');
    if (typeof url !== 'string' || url.trim() === '') continue;
    const lower = url.toLowerCase();
    let exclude = false;
    for (const p of EXCLUDED_URL_PATTERNS) {
      if (lower.includes(p)) { exclude = true; break; }
    }
    if (exclude) continue;
    out.push(url.trim());
  }
  return out;
}

/**
 * 画像 preflight stub (E-4): 「フィルター後 1 枚以上残れば ok」 と判定。
 * E-5 で実 uploadItemImage を呼ぶようにここを差し替える予定。
 */
export function imagePreflightStub(rakutenItem) {
  const urls = filterUploadableImageUrls(rakutenItem?.images);
  if (urls.length === 0) {
    return { ok: false, error: 'no_uploadable_image_after_filter' };
  }
  return { ok: true, urlsAvailable: urls.length };
}
