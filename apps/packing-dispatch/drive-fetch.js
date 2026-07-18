/**
 * Google Drive からヤマトB2「発行済データ」CSV を直接取得する (packing-dispatch キャリアCSV取込用)
 *
 * B2クラウドから自動DLされた CSV が Drive の固定フォルダに固定ファイル名で置かれる運用。
 * 手元にDLして手動アップロードする手間を省くため、Render サーバが Drive API (読み取り専用) で
 * 直接ダウンロードして importTrackingCsv に流す。
 *
 * 認証は既存の GOOGLE_SERVICE_ACCOUNT_KEY (base64 JSON) を流用 (fba-replenishment/drive-upload.js と同じ)。
 * 対象フォルダをサービスアカウントが閲覧できる必要がある (共有ドライブのメンバー、または
 * フォルダをサービスアカウントのメールアドレスに閲覧者共有)。
 */
import { google } from 'googleapis';

const TIMEOUT = 20000; // Drive応答が遅くても処理全体を待たせない (drive-upload.js と同じ方針)

// source → 取得先。フォルダID/ファイル名は運用固定 (2026-07-18 中原さん指定)。env で差し替え可。
const DRIVE_SOURCES = {
  yamato_b2: {
    label: 'ヤマト B2 (ネコポス・発払い)',
    folderId: process.env.PD_DRIVE_FOLDER_YAMATO_B2 || '1X8LISS4Ck0mohW_7x7BSHNBebZF3VwPn',
    filename: process.env.PD_DRIVE_FILE_YAMATO_B2 || 'ネコ・60サイズ31項目4項目_発行済データ.csv',
  },
  yamato_b2_50: {
    label: 'ヤマト B2 (50サイズ専用)',
    folderId: process.env.PD_DRIVE_FOLDER_YAMATO_B2_50 || '1F_DWgsFs16002cLUK7_7o_GmM5_CDY31',
    filename: process.env.PD_DRIVE_FILE_YAMATO_B2_50 || '50サイズ31項目4項目_発行済データ.csv',
  },
};

function vErr(message, detail) {
  const e = new Error(message);
  e.code = 'VALIDATION';
  if (detail) e.detail = detail;
  return e;
}

function getSourceConfig(source) {
  const cfg = DRIVE_SOURCES[source];
  if (!cfg) throw vErr(`Drive 取込に対応していない source です: ${source}`);
  return cfg;
}

function getDriveClient() {
  const keyBase64 = process.env.GOOGLE_SERVICE_ACCOUNT_KEY;
  if (!keyBase64) throw new Error('GOOGLE_SERVICE_ACCOUNT_KEY が未設定です (Render の env を確認してください)');
  const keyJson = JSON.parse(Buffer.from(keyBase64, 'base64').toString('utf-8'));
  const auth = new google.auth.GoogleAuth({
    credentials: keyJson,
    scopes: ['https://www.googleapis.com/auth/drive.readonly'],
  });
  return { drive: google.drive({ version: 'v3', auth }), serviceAccountEmail: keyJson.client_email || null };
}

// Drive の RFC3339 UTC を「YYYY-MM-DD HH:mm」JST 表記へ (toISOString は UTC のままなので +9h を自前計算)
function toJstDisplay(iso) {
  if (!iso) return null;
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return null;
  const d = new Date(t + 9 * 3600 * 1000);
  const p = (n) => String(n).padStart(2, '0');
  return `${d.getUTCFullYear()}-${p(d.getUTCMonth() + 1)}-${p(d.getUTCDate())} ${p(d.getUTCHours())}:${p(d.getUTCMinutes())}`;
}

/**
 * フォルダ内から対象ファイルを検索して metadata を返す。
 * 同名が複数あれば modifiedTime 最新を採用。見つからなければ VALIDATION エラー。
 */
async function findDriveFile(source) {
  const cfg = getSourceConfig(source);
  const { drive, serviceAccountEmail } = getDriveClient();
  const esc = (s) => String(s).replace(/\\/g, '\\\\').replace(/'/g, "\\'"); // 検索クエリ用エスケープ

  // フォルダの属する共有ドライブIDを特定して検索範囲を限定する (corpora=allDrives の
  // incompleteSearch による見落としを避ける。drive-upload.js と同じ方針)。
  let driveId = null;
  try {
    const meta = await drive.files.get(
      { fileId: cfg.folderId, fields: 'id, driveId', supportsAllDrives: true },
      { timeout: TIMEOUT }
    );
    driveId = meta.data.driveId || null;
  } catch (e) {
    throw new Error(
      `Drive フォルダにアクセスできません (${cfg.label})。フォルダをサービスアカウント` +
      `${serviceAccountEmail ? ` 「${serviceAccountEmail}」` : ''} に閲覧者以上で共有してください。(詳細: ${e.message})`
    );
  }

  const list = await drive.files.list({
    q: `name = '${esc(cfg.filename)}' and '${esc(cfg.folderId)}' in parents and trashed = false`,
    fields: 'files(id, name, modifiedTime, size), incompleteSearch',
    orderBy: 'modifiedTime desc',
    pageSize: 10,
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
    ...(driveId ? { corpora: 'drive', driveId } : { corpora: 'user' }),
  }, { timeout: TIMEOUT });

  const file = list.data.files && list.data.files[0];
  if (!file) {
    throw vErr(`Drive フォルダに「${cfg.filename}」が見つかりません (${cfg.label})。B2クラウドからのDLが済んでいるか確認してください。`);
  }
  return {
    source,
    label: cfg.label,
    filename: file.name,
    file_id: file.id,
    size: file.size != null ? Number(file.size) : null,
    modified_time: file.modifiedTime || null,           // RFC3339 UTC (機械用)
    modified_time_jst: toJstDisplay(file.modifiedTime), // 表示用 JST
  };
}

/** ファイル metadata のみ (UI の「更新日時」表示用)。 */
export async function getDriveCsvInfo(source) {
  return findDriveFile(source);
}

/** CSV 本体をダウンロードして Buffer で返す (エンコーディングは importTrackingCsv 側の parser が判定)。 */
export async function downloadDriveCsv(source) {
  const info = await findDriveFile(source);
  const { drive } = getDriveClient();
  const res = await drive.files.get(
    { fileId: info.file_id, alt: 'media', supportsAllDrives: true },
    { responseType: 'arraybuffer', timeout: TIMEOUT }
  );
  return { ...info, buffer: Buffer.from(res.data) };
}
