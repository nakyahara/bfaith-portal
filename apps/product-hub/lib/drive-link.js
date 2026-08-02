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
 * サムネイルはアプリ内プロキシ (/apps/product-hub/api/thumb/:fileId) を指す。
 * かつての drive.google.com/thumbnail 直リンクは「閲覧者の Google セッション」を
 * サードパーティ Cookie として送れる前提で、Cookie ブロックや複数アカウント
 * (authuser 不一致) 環境で 403 になり表示できなかった (2026-08 プロキシ化)。
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

// プロキシが生成するサムネイル幅の allowlist (一覧=160 / 詳細=320)。
// キャッシュキーが際限なく増えないよう、これ以外の指定は 320 に丸める
export const THUMB_WIDTHS = [160, 320];

// router の /api/thumb/:fileId が受け付ける ID 形式 (parseDriveLink と同じ文字種)
export const DRIVE_FILE_ID_PATTERN = /^[-\w]{10,200}$/;

export function thumbnailUrl(fileId, width = 320) {
  const w = THUMB_WIDTHS.includes(width) ? width : 320;
  return `/apps/product-hub/api/thumb/${encodeURIComponent(fileId)}?w=${w}`;
}

export function fileViewUrl(fileId) {
  return `https://drive.google.com/file/d/${encodeURIComponent(fileId)}/view`;
}
