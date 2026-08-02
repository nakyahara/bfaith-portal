/**
 * inquiry-hub 添付ファイルの MIME 判定 (2026-08-02 添付表示)
 *
 * 方針:
 *   - ブラウザに inline 表示させるのは「画像 + PDF」の allow-list のみ。それ以外は必ず
 *     ダウンロード (Content-Disposition: attachment) にする
 *   - SVG は inline 表示を許さない (SVG内のスクリプトが同一オリジンで動くため)。
 *     HTML/XML 系も同様に octet-stream へ落とす
 *   - 外部が申告する Content-Type は信用しすぎない (拡張子と突き合わせて画像判定を確定させる)
 *
 * adapters (yahoo.js) と attachments.js の両方から使うため独立モジュールにしている
 * (循環 import 回避)。
 */

/** 拡張子 → Content-Type (画面表示で意味がある範囲のみ) */
const EXT_MAP = {
  png: 'image/png', jpg: 'image/jpeg', jpeg: 'image/jpeg', jpe: 'image/jpeg',
  gif: 'image/gif', webp: 'image/webp', bmp: 'image/bmp', heic: 'image/heic', heif: 'image/heif',
  pdf: 'application/pdf', txt: 'text/plain', csv: 'text/csv',
  zip: 'application/zip', xlsx: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  xls: 'application/vnd.ms-excel', docx: 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
  doc: 'application/msword',
};

/** ブラウザに inline 表示させてよい型 (これ以外は必ずダウンロード) */
export const INLINE_SAFE = new Set([
  'image/png', 'image/jpeg', 'image/gif', 'image/webp', 'image/bmp', 'application/pdf',
]);

/** 画面にサムネイル (<img>) を出す型 */
export const IMAGE_TYPES = new Set(['image/png', 'image/jpeg', 'image/gif', 'image/webp', 'image/bmp']);

/** 'PNG' / '.png' / 'png' → 'image/png' (未知は null) */
export function contentTypeFromExt(ext) {
  const e = String(ext || '').trim().toLowerCase().replace(/^\./, '');
  return EXT_MAP[e] || null;
}

/**
 * 保存済みメタデータ・外部の申告値・ファイル名から実際に使う Content-Type を決める。
 * 拡張子から画像/PDFと分かるものはそれを優先 (外部の 'application/octet-stream' 申告で
 * 画像がダウンロード扱いになるのを防ぐ)。判定できなければ octet-stream。
 */
export function resolveContentType(fileName, declared) {
  const byExt = contentTypeFromExt(String(fileName || '').split('.').pop());
  if (byExt) return byExt;
  const d = String(declared || '').split(';')[0].trim().toLowerCase();
  // 申告値が inline-safe な型ならそれを使う。それ以外 (svg/html/xml 含む) は octet-stream
  if (INLINE_SAFE.has(d)) return d;
  if (d === 'text/plain' || d === 'text/csv') return d;
  return 'application/octet-stream';
}

/** inline 表示してよいか (画像・PDFのみ) */
export function isInlineSafe(contentType) {
  return INLINE_SAFE.has(String(contentType || '').split(';')[0].trim().toLowerCase());
}

/**
 * 中身 (先頭バイト) から実際の形式を判定する (2026-08-02 Codexレビュー Medium-4)。
 * 拡張子だけを信じて inline 配信すると、拡張子を偽った別形式のファイルを
 * 画像/PDFとしてブラウザに渡すことになる → 実データと一致したときだけ inline を許す。
 * @returns {string|null} 判定できた Content-Type (判定不能は null)
 */
export function sniffContentType(buffer) {
  if (!buffer || buffer.length < 4) return null;
  const b = buffer;
  const startsWith = (...bytes) => bytes.every((v, i) => b[i] === v);
  if (startsWith(0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a)) return 'image/png';
  if (startsWith(0xff, 0xd8, 0xff)) return 'image/jpeg';
  if (startsWith(0x47, 0x49, 0x46, 0x38)) return 'image/gif';
  if (startsWith(0x42, 0x4d)) return 'image/bmp';
  if (b.length >= 12 && startsWith(0x52, 0x49, 0x46, 0x46)
    && b[8] === 0x57 && b[9] === 0x45 && b[10] === 0x42 && b[11] === 0x50) return 'image/webp';
  if (startsWith(0x25, 0x50, 0x44, 0x46)) return 'application/pdf';    // %PDF
  return null;
}

/**
 * 配信に使う最終的な Content-Type。inline 対象 (画像・PDF) は中身と一致したときだけ
 * その型で返し、食い違う/判定できないものは octet-stream (=必ずダウンロード) に落とす。
 */
export function contentTypeForServing(fileName, declared, buffer) {
  const wanted = resolveContentType(fileName, declared);
  if (!isInlineSafe(wanted)) return wanted;             // もともとダウンロード扱いのものはそのまま
  const actual = sniffContentType(buffer);
  return actual === wanted ? wanted : 'application/octet-stream';
}

/** 画面にサムネイルを出す対象か */
export function isImage(contentType) {
  return IMAGE_TYPES.has(String(contentType || '').split(';')[0].trim().toLowerCase());
}

/** バイト数の人間向け表記 (画面用) */
export function fmtBytes(n) {
  const b = Number(n);
  if (!Number.isFinite(b) || b <= 0) return '';
  if (b < 1024) return `${b}B`;
  if (b < 1048576) return `${Math.round(b / 1024)}KB`;
  return `${(b / 1048576).toFixed(1)}MB`;
}
