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

// サービスアカウントのメールアドレス (共有設定で使う識別子。private_key は出さない)。
// 取得できなければ null。フォルダ共有エラー時の案内に使う。
function getServiceAccountEmail() {
  try {
    const keyJson = JSON.parse(Buffer.from(process.env.GOOGLE_SERVICE_ACCOUNT_KEY || '', 'base64').toString('utf-8'));
    return keyJson.client_email || null;
  } catch { return null; }
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
  const TIMEOUT = 20000; // Drive応答が遅延しても処理全体を待たせない (Codex Low)
  const esc = (s) => String(s).replace(/\\/g, '\\\\').replace(/'/g, "\\'"); // 検索クエリ用エスケープ

  // 保存先フォルダが属する共有ドライブIDを取得し、検索を当該ドライブ内に限定する。
  // corpora=allDrives は incompleteSearch を返し得て、既存を見落とし重複作成する恐れがあるため避ける (Codex Medium)。
  let driveId = null;
  let folderName = null; // 保存先フォルダ名 (UI に「どこに保存したか」を出すため。取得できなければ null)
  try {
    const meta = await drive.files.get(
      { fileId: folderId, fields: 'id, name, driveId', supportsAllDrives: true },
      { timeout: TIMEOUT }
    );
    driveId = meta.data.driveId || null;
    folderName = meta.data.name || null;
  } catch (e) {
    const email = getServiceAccountEmail();
    throw new Error(
      `保存先フォルダにアクセスできません。共有ドライブに サービスアカウント${email ? ` 「${email}」` : ''} を` +
      `「コンテンツ管理者」で追加し、GCPで Drive API を有効化してください。(詳細: ${e.message})`
    );
  }

  const listParams = {
    q: `name = '${esc(filename)}' and '${esc(folderId)}' in parents and trashed = false`,
    fields: 'files(id, name), incompleteSearch',
    pageSize: 10,
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
    ...(driveId ? { corpora: 'drive', driveId } : { corpora: 'user' }),
  };
  const list = await drive.files.list(listParams, { timeout: TIMEOUT });
  // 検索が不完全なら、既存を見落として重複作成するのを防ぐため中止 (best-effort 失敗)
  if (list.data.incompleteSearch) {
    throw new Error('Drive検索が不完全(incompleteSearch)でした。重複作成防止のため保存を中止しました');
  }

  const media = { mimeType, body: Readable.from(buffer) };
  const existing = list.data.files && list.data.files[0];
  // 同名ファイルが複数あると、更新するのは先頭1件だけで残りは古い中身のまま残る。
  // 読み手が別の1件を掴んでいると「更新したのに反映されない」ので気付けるよう警告する
  // (Codex Medium。既存の挙動は変えない = 勝手に消さない)。
  if (list.data.files && list.data.files.length > 1) {
    console.warn(`[drive-upload] 「${filename}」がフォルダ内に ${list.data.files.length} 件あります`
      + ` (id: ${list.data.files.map((f) => f.id).join(', ')})。更新するのは先頭の1件だけです`);
  }

  if (existing) {
    // 既存ファイルの中身だけ差し替え (fileId/共有設定は維持)
    await drive.files.update({ fileId: existing.id, media, supportsAllDrives: true }, { timeout: TIMEOUT });
    return { action: 'updated', fileId: existing.id, folderName };
  }

  const created = await drive.files.create(
    { requestBody: { name: filename, parents: [folderId] }, media, fields: 'id', supportsAllDrives: true },
    { timeout: TIMEOUT }
  );
  return { action: 'created', fileId: created.data.id, folderName };
}

/**
 * 指定フォルダにサービスアカウントが「書き込み」できるかを非破壊で点検する。
 * Drive の capabilities.canAddChildren (=このフォルダにファイルを作成できるか) を読むだけで、
 * 実ファイルは作成/削除しない (Codex Medium: GET で副作用を起こさない・プローブ累積を作らない)。
 * さらに driveId (共有ドライブ配下か) も併せて判定する。
 *   ok = true になるのは「共有ドライブ配下」かつ「canAddChildren=true」のときだけ。
 *   ※ マイドライブ配下フォルダは canAddChildren=true でも SA に容量が無く実保存に失敗するため false 扱い。
 * go-live 前に「SA がコンテンツ管理者で共有されているか」をブラウザ(GET)から確認する用途。
 * @param {string} folderId 点検するフォルダID
 * @returns {Promise<object>} { ok, stage?, isSharedDrive, canAddChildren, driveId, folderName, isFolder, serviceAccountEmail, message? }
 */
export async function probeFolderWritable(folderId) {
  const auth = getDriveAuth();
  const drive = google.drive({ version: 'v3', auth });
  const TIMEOUT = 20000;
  const email = getServiceAccountEmail();

  let meta;
  try {
    const r = await drive.files.get(
      { fileId: folderId,
        fields: 'id, name, driveId, mimeType, capabilities(canAddChildren, canEdit)',
        supportsAllDrives: true },
      { timeout: TIMEOUT }
    );
    meta = r.data;
  } catch (e) {
    return { ok: false, stage: 'access', isSharedDrive: false, canAddChildren: false,
      driveId: null, folderName: null, isFolder: false, serviceAccountEmail: email,
      message: `フォルダにアクセスできません。SA${email ? ` 「${email}」` : ''} に共有されているか確認してください。(詳細: ${e.message})` };
  }

  const driveId = meta.driveId || null;
  const isSharedDrive = !!driveId;
  const isFolder = meta.mimeType === 'application/vnd.google-apps.folder';
  const canAddChildren = !!(meta.capabilities && meta.capabilities.canAddChildren);
  const folderName = meta.name || null;

  if (!isFolder) {
    return { ok: false, stage: 'not_folder', isSharedDrive, canAddChildren, driveId, folderName, isFolder,
      serviceAccountEmail: email,
      message: `指定IDはフォルダではありません (mimeType=${meta.mimeType})。フォルダIDを確認してください。` };
  }
  if (!canAddChildren) {
    return { ok: false, stage: 'write', isSharedDrive, canAddChildren, driveId, folderName, isFolder,
      serviceAccountEmail: email,
      message: `このフォルダに書き込めません。SA${email ? ` 「${email}」` : ''} を「コンテンツ管理者」以上で招待してください。(詳細: capabilities.canAddChildren=false)` };
  }
  if (!isSharedDrive) {
    return { ok: false, stage: 'not_shared_drive', isSharedDrive, canAddChildren, driveId, folderName, isFolder,
      serviceAccountEmail: email,
      message: `このフォルダは共有ドライブ配下ではありません (マイドライブ配下だと SA は容量が無く実保存に失敗します)。共有ドライブ内のフォルダを指定してください。` };
  }
  return { ok: true, isSharedDrive, canAddChildren, driveId, folderName, isFolder, serviceAccountEmail: email };
}
