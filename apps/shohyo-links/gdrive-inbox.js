/**
 * shohyo-links — Gドライブの「証憑受け箱」フォルダを拾って受け箱に入れる (証憑自動添付 PR2)
 *
 * 運用 (2026-08-28 中原さん): 人にとって扱いやすいのは Gドライブ。ロボットがDLした物も、スマホで撮った
 * レシートも、全部このフォルダに放り込めば、あとは何もしなくていい。
 *
 *   証憑受け箱/                … ここに入れる (PDF / JPG / PNG)
 *   証憑受け箱/取込済み/       … 受け箱に入った物をロボットがここへ移す (人は触らない)
 *   証憑受け箱/対応外/         … 5MB超・PDF/JPEG/PNG以外 (人が確認)
 *
 * Render 常駐の attach-job (毎時) の先頭で1回走る。Drive は既存の GOOGLE_SERVICE_ACCOUNT_KEY で読む。
 * フォルダ間の移動には書き込み scope (drive) が要るので、product-hub の画像フォルダ作成と同じ client。
 * SA が受け箱フォルダの「コンテンツ管理者」以上でないと移動に失敗する (取込はできる → 次回また拾うが、
 * sha256 で重複登録はしない)。
 *
 * env:
 *   SHOHYO_GDRIVE_INBOX_FOLDER_ID … 受け箱フォルダの Drive ID。無ければこの機能は動かない
 *   GOOGLE_SERVICE_ACCOUNT_KEY    … SA 鍵 (base64)。既存
 */
import { google } from 'googleapis';
import { ingestVoucher } from './ingest.js';

const DRIVE_ID_RE = /^[A-Za-z0-9_-]{5,}$/;
const DRIVE_TIMEOUT_MS = 20_000;
const MAX_FILES_PER_RUN = 50;
const MAX_BYTES = 5 * 1024 * 1024;
const FOLDER_MIME = 'application/vnd.google-apps.folder';
const SUB_INGESTED = '取込済み';
const SUB_UNSUPPORTED = '対応外';
const ACCEPT_MIME = new Set(['application/pdf', 'image/jpeg', 'image/png']);

export function gdriveInboxFolderId(env = process.env) {
  const id = String(env.SHOHYO_GDRIVE_INBOX_FOLDER_ID || '').trim();
  return DRIVE_ID_RE.test(id) ? id : '';
}

export function gdriveInboxEnabled(env = process.env) {
  return Boolean(gdriveInboxFolderId(env) && env.GOOGLE_SERVICE_ACCOUNT_KEY);
}

export function gdriveInboxUrl(env = process.env) {
  const id = gdriveInboxFolderId(env);
  return id ? `https://drive.google.com/drive/folders/${id}` : '';
}

let _drive = null;
function driveClient() {
  if (_drive) return _drive;
  const credentials = JSON.parse(Buffer.from(process.env.GOOGLE_SERVICE_ACCOUNT_KEY, 'base64').toString('utf-8'));
  const auth = new google.auth.GoogleAuth({ credentials, scopes: ['https://www.googleapis.com/auth/drive'] });
  _drive = google.drive({ version: 'v3', auth });
  return _drive;
}

const q = (s) => String(s).replace(/\\/g, '\\\\').replace(/'/g, "\\'");

/** 親フォルダ直下の同名サブフォルダを返す (無ければ作る・冪等) */
export async function ensureSubfolder(drive, parentId, name) {
  const found = await drive.files.list({
    q: `'${parentId}' in parents and trashed = false and mimeType = '${FOLDER_MIME}' and name = '${q(name)}'`,
    fields: 'files(id, name)', pageSize: 2, supportsAllDrives: true, includeItemsFromAllDrives: true,
  }, { timeout: DRIVE_TIMEOUT_MS });
  const hit = (found.data.files || [])[0];
  if (hit) return hit.id;
  const created = await drive.files.create({
    requestBody: { name, mimeType: FOLDER_MIME, parents: [parentId] }, fields: 'id', supportsAllDrives: true,
  }, { timeout: DRIVE_TIMEOUT_MS });
  return created.data.id;
}

/** 受け箱フォルダ直下のファイル (フォルダ以外・ゴミ箱以外) を古い順に */
export async function listInboxFiles(drive, folderId) {
  const res = await drive.files.list({
    q: `'${folderId}' in parents and trashed = false and mimeType != '${FOLDER_MIME}'`,
    fields: 'files(id, name, mimeType, size, modifiedTime)', pageSize: MAX_FILES_PER_RUN, orderBy: 'modifiedTime',
    supportsAllDrives: true, includeItemsFromAllDrives: true,
  }, { timeout: DRIVE_TIMEOUT_MS });
  return res.data.files || [];
}

async function download(drive, fileId) {
  const res = await drive.files.get({ fileId, alt: 'media', supportsAllDrives: true }, { responseType: 'arraybuffer', timeout: DRIVE_TIMEOUT_MS });
  return Buffer.from(res.data);
}

async function moveTo(drive, fileId, fromId, toId) {
  await drive.files.update({ fileId, addParents: toId, removeParents: fromId, fields: 'id', supportsAllDrives: true }, { timeout: DRIVE_TIMEOUT_MS });
}

/**
 * 1周期分: 受け箱フォルダのファイルを受け箱へ入れ、取込済み/対応外 へ移す。
 * @param {{ drive?: object, folderId?: string, ingest?: Function }} deps  テスト用に差し替え可
 * @returns {{ ok, listed, ingested, duplicate, unsupported, failed, errors: string[] }}
 */
export async function runGdriveInbox({ drive = null, folderId = null, ingest = ingestVoucher, env = process.env } = {}) {
  const summary = { ok: true, listed: 0, ingested: 0, duplicate: 0, unsupported: 0, failed: 0, errors: [] };
  const root = folderId || gdriveInboxFolderId(env);
  if (!root) return { ...summary, skipped: 'not_configured' };
  const d = drive || driveClient();
  const files = await listInboxFiles(d, root);
  summary.listed = files.length;
  if (!files.length) return summary;
  const ingestedId = await ensureSubfolder(d, root, SUB_INGESTED);
  const unsupportedId = await ensureSubfolder(d, root, SUB_UNSUPPORTED);

  for (const f of files) {
    try {
      const size = Number(f.size || 0);
      if (size > MAX_BYTES || (f.mimeType && !ACCEPT_MIME.has(f.mimeType) && !/\.(pdf|jpe?g|png)$/i.test(f.name || ''))) {
        summary.unsupported++;
        await moveTo(d, f.id, root, unsupportedId);
        console.warn(`[shohyo-gdrive] 対応外へ: ${f.name} (${f.mimeType}, ${size}B)`);
        continue;
      }
      const buffer = await download(d, f.id);
      let r;
      try {
        r = await ingest(buffer, { file_name: f.name, source: 'gdrive', note: `gdrive:${f.id}` });
      } catch (e) {
        if (e.message === 'unsupported_file') {
          summary.unsupported++;
          await moveTo(d, f.id, root, unsupportedId);
          continue;
        }
        throw e;
      }
      if (r.duplicate) summary.duplicate++; else summary.ingested++;
      await moveTo(d, f.id, root, ingestedId);
    } catch (e) {
      summary.failed++;
      summary.ok = false;
      summary.errors.push(`${f.name}: ${String(e.message).slice(0, 120)}`);
      console.error(`[shohyo-gdrive] ${f.name} failed:`, e.message);
    }
  }
  return summary;
}
