/**
 * Google Drive へのファイルアップロード (FBA納品ピッキング準備のラベルCSV保存用)
 *
 * 既存の Google サービスアカウント (GOOGLE_SERVICE_ACCOUNT_KEY) を流用するが、書き込みのため
 * scope は drive を要求する (サービスアカウントは scope を self-grant できるので追加同意は不要)。
 *
 * 制約: サービスアカウントは「マイドライブ」には容量が無く保存できない。保存先は共有ドライブ
 * (Shared Drive) のフォルダで、サービスアカウントがそのメンバー(コンテンツ管理者以上)である必要がある。
 * supportsAllDrives / includeItemsFromAllDrives で共有ドライブを対象にする。
 */
import { google } from 'googleapis';
import { Readable } from 'node:stream';

function getDriveAuth() {
  const keyBase64 = process.env.GOOGLE_SERVICE_ACCOUNT_KEY;
  if (!keyBase64) throw new Error('GOOGLE_SERVICE_ACCOUNT_KEY が未設定です');
  const keyJson = JSON.parse(Buffer.from(keyBase64, 'base64').toString('utf-8'));
  return new google.auth.GoogleAuth({
    credentials: keyJson,
    scopes: ['https://www.googleapis.com/auth/drive'],
  });
}

/**
 * 指定フォルダ内の同名ファイルを上書き、無ければ新規作成する。共有ドライブ対応。
 * @param {Buffer} buffer 保存する中身 (バイト列。Shift_JIS等エンコード済みでよい)
 * @param {string} filename 固定ファイル名 (例: fbanouhinbangoulist.csv)
 * @param {string} folderId 保存先フォルダID (共有ドライブ配下)
 * @param {string} [mimeType] 既定 text/csv
 * @returns {Promise<{action:'updated'|'created', fileId:string}>}
 */
export async function uploadCsvToDrive(buffer, filename, folderId, mimeType = 'text/csv') {
  const auth = getDriveAuth();
  const drive = google.drive({ version: 'v3', auth });

  // 同名・同フォルダ・非ゴミ箱の既存ファイルを検索 (共有ドライブ横断)
  const escaped = filename.replace(/\\/g, '\\\\').replace(/'/g, "\\'");
  const list = await drive.files.list({
    q: `name = '${escaped}' and '${folderId}' in parents and trashed = false`,
    fields: 'files(id, name)',
    pageSize: 10,
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
    corpora: 'allDrives',
  });

  const media = { mimeType, body: Readable.from(buffer) };
  const existing = list.data.files && list.data.files[0];

  if (existing) {
    // 既存ファイルの中身だけ差し替え (fileId/共有設定は維持)
    await drive.files.update({ fileId: existing.id, media, supportsAllDrives: true });
    return { action: 'updated', fileId: existing.id };
  }

  const created = await drive.files.create({
    requestBody: { name: filename, parents: [folderId] },
    media,
    fields: 'id',
    supportsAllDrives: true,
  });
  return { action: 'created', fileId: created.data.id };
}
