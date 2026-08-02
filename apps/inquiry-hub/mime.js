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
