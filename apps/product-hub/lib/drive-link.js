/**
 * Google Drive の URL からファイル/フォルダ ID を抽出する pure function 群。
 *
 * 対応形式:
 *   https://drive.google.com/file/d/<ID>/view?...       → { type: 'file', id }
 *   https://drive.google.com/open?id=<ID>               → { type: 'file', id }
 *   https://drive.google.com/uc?id=<ID>&...             → { type: 'file', id }
 *   https://drive.google.com/drive/folders/<ID>?...     → { type: 'folder', id }
 *   https://drive.google.com/drive/u/0/folders/<ID>     → { type: 'folder', id }
 *   生の ID (25文字以上の [-\w]) をそのまま貼った場合    → { type: 'unknown', id }
 *
 * サムネイルは閲覧者の Google セッションで解決される軽量 endpoint を使う
 * (サービスアカウント不要。社内ユーザーは Drive 権限を既に持っている前提):
 *   https://drive.google.com/thumbnail?id=<ID>&sz=w320
 */

const FILE_PATTERNS = [
  /drive\.google\.com\/file\/d\/([-\w]{10,})/,
  /drive\.google\.com\/(?:open|uc)\?(?:[^#]*&)?id=([-\w]{10,})/,
];
const FOLDER_PATTERN = /drive\.google\.com\/drive\/(?:u\/\d+\/)?folders\/([-\w]{10,})/;
const RAW_ID_PATTERN = /^[-\w]{25,}$/;

export function parseDriveLink(input) {
  if (input == null) return null;
  const s = String(input).trim();
  if (s === '') return null;

  const folder = s.match(FOLDER_PATTERN);
  if (folder) return { type: 'folder', id: folder[1] };

  for (const re of FILE_PATTERNS) {
    const m = s.match(re);
    if (m) return { type: 'file', id: m[1] };
  }

  if (RAW_ID_PATTERN.test(s)) return { type: 'unknown', id: s };
  return null;
}

export function thumbnailUrl(fileId, width = 320) {
  const w = Number.isFinite(width) && width > 0 ? Math.floor(width) : 320;
  return `https://drive.google.com/thumbnail?id=${encodeURIComponent(fileId)}&sz=w${w}`;
}

export function fileViewUrl(fileId) {
  return `https://drive.google.com/file/d/${encodeURIComponent(fileId)}/view`;
}
