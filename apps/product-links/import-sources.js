/**
 * 商品リンク台帳 — 過去分の取込元 (すべて候補表へ。本表には人が accept したときだけ入る)
 *
 *   1. Drive 走査: 共有ドライブ「ドライブ」>「商品別」(folderId は env PRODUCT_LINKS_DRIVE_FOLDER_ID・既定 1wMgG-…) の
 *      直下サブフォルダを **読み取り専用** で一覧し、フォルダ名 `商品コード_商品名` からコードを推定。
 *      1 階層のみ・移動/改名/削除は一切しない (要件定義 §5・中原さん許可 8/27)。lib/drive-csv.js の service account を再利用。
 *   2. Notion 画像DB: 「Canva」「グーグルドライブURL」列を候補に (商品コード列で推定)。product-hub 側で既に
 *      移植済みのカードは product_hub 由来で本表にあるので addCandidates が duplicate で閉じる。
 */
import { listDriveSubfolders } from '../../lib/drive-csv.js';
import { queryDatabaseAll } from '../rakuten-yahoo-sync/lib/notion-client.js';
import { getImageDbConfig, buildImageRecord } from '../product-hub/services/notion-image-import.js';
import { addCandidates, newBatchId, codeFromFolderName } from './candidates.js';

export const DEFAULT_DRIVE_FOLDER_ID = '1wMgG-MvVdun7-y89gvGovv8Y7x8ICAd4';
export function driveFolderId() {
  return (process.env.PRODUCT_LINKS_DRIVE_FOLDER_ID || DEFAULT_DRIVE_FOLDER_ID).trim();
}

/** Drive の「商品別」直下を走査して候補化。返す: { batchId, folders, inserted, skipped, duplicate } */
export async function scanDriveProductFolders(db, { actor, list = listDriveSubfolders } = {}) {
  const folderId = driveFolderId();
  const subs = await list({ folderId });
  const batchId = newBatchId('drive');
  const items = subs.map((f) => ({
    raw_code: codeFromFolderName(f.name),
    raw_name: f.name,
    url: `https://drive.google.com/drive/folders/${f.folder_id}`,
    link_type: 'drive_folder',
    source_entity_id: f.folder_id,
  }));
  const r = addCandidates(db, { batchId, source: 'drive_scan', items, actor });
  return { batchId, folders: subs.length, ...r };
}

/** Notion 画像DB 全件 → Canva / Drive フォルダを候補化。返す: { batchId, pages, inserted, skipped, duplicate } */
export async function importNotionImageDb(db, { actor, query = queryDatabaseAll, config = getImageDbConfig } = {}) {
  const cfg = config();
  const { pages } = await query({ cfg, maxPages: 200 });
  const batchId = newBatchId('notion');
  const items = [];
  for (const page of pages) {
    const rec = buildImageRecord(page);
    if (!rec) continue;
    if (rec.canva_url) items.push({ raw_code: rec.ne_code, raw_name: rec.name, url: rec.canva_url, link_type: 'canva', source_entity_id: rec.notion_page_id });
    if (rec.drive_folder_url) items.push({ raw_code: rec.ne_code, raw_name: rec.name, url: rec.drive_folder_url, link_type: 'drive_folder', source_entity_id: rec.notion_page_id });
  }
  const r = addCandidates(db, { batchId, source: 'notion_image', items, actor });
  return { batchId, pages: pages.length, ...r };
}
