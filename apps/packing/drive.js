/**
 * packing の Drive 取込ヘルパー — 「出荷_no」フォルダの 納品書_出荷_XX.csv。
 *
 * 別プログラム (伝票出しPC) が各出荷_XXフォルダへ納品書CSVを毎朝配置する運用 (2026-08-16 確認)。
 * picking の drive-sync.js と同じフォルダを見るが、境界を保つため helper は自前で持つ
 * (lib/drive-csv.js の薄いラッパのみ。将来のプロセス分離を跨ぎ依存で阻害しない — 要件§7.2)。
 */
import { listDriveSubfolders, listDriveFilesAcross, downloadDriveFileById } from '../../lib/drive-csv.js';
import { PackError } from './service.js';

// picking と同じ「出荷_no」フォルダ (SA閲覧者共有済み)。専用envがあれば優先
export const DRIVE_FOLDER_ID = process.env.PACKING_DRIVE_FOLDER_ID
  || process.env.PICKING_DRIVE_FOLDER_ID || '110ONn2xHzfEG5HPt1DRy4P64zv2hpGjh';

// サブフォルダ一覧 (出荷_01〜45) は増減しないので60秒キャッシュ
let _subfoldersCache = { at: 0, list: null };
export async function getShippingFolders() {
  if (_subfoldersCache.list && Date.now() - _subfoldersCache.at < 60_000) return _subfoldersCache.list;
  const subs = await listDriveSubfolders({ folderId: DRIVE_FOLDER_ID });
  const list = [{ folder_id: DRIVE_FOLDER_ID, name: '(出荷_no直下)' }, ...subs];
  _subfoldersCache = { at: Date.now(), list };
  return list;
}

/** lib/drive-csv.js のエラーを業務エラーへ変換 (VALIDATION=入力起因400 / それ以外=Drive側502)。 */
export async function driveCall(fn) {
  try {
    return await fn();
  } catch (e) {
    if (e instanceof PackError) throw e;
    throw new PackError(e.code === 'VALIDATION' ? 400 : 502, 'drive_error', e.message);
  }
}

/** 納品書CSVの一覧 (全出荷_XXフォルダ横断)。PDF (納品書_1.pdf 等) は除外。 */
export async function listNouhinCsvFiles() {
  const folders = await getShippingFolders();
  const files = await listDriveFilesAcross({ folders, nameContains: '納品書' });
  return files.filter((f) => /\.csv$/i.test(f.filename));
}

export async function downloadNouhinCsv(fileId) {
  const folders = await getShippingFolders();
  return downloadDriveFileById({ fileId, folderIds: folders.map((f) => f.folder_id) });
}
