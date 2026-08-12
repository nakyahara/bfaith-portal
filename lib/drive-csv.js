/**
 * Google Drive の固定フォルダから固定ファイル名の CSV を取得する共通処理。
 *
 * 「ローカルにDLして手動アップロード」の手間を省くため、Render サーバが Drive API (読み取り専用) で
 * 直接ダウンロードする経路。packing-dispatch のキャリアCSV取込 (2026-07-18) で実運用に入り、
 * purchase-orders のロジザード在庫CSV取込 (2026-07-20) でも使うため lib へ切り出した。
 *
 * 認証は既存の GOOGLE_SERVICE_ACCOUNT_KEY (base64 JSON) を流用 (fba-replenishment/drive-upload.js と同じ)。
 * 対象フォルダをサービスアカウントが閲覧できる必要がある (共有ドライブのメンバー、または
 * フォルダをサービスアカウントのメールアドレスに閲覧者共有)。
 */
import { google } from 'googleapis';

const TIMEOUT = 20000; // Drive応答が遅くても処理全体を待たせない (drive-upload.js と同じ方針)
// 手動アップロード経路の multer 上限と揃える。Drive経由だけ無制限にメモリへ読み込まないための防御。
export const DEFAULT_MAX_CSV_BYTES = 20 * 1024 * 1024;
const INFO_CACHE_TTL_MS = 60 * 1000; // 更新日時表示は 60 秒キャッシュ (表示用ポーリングで Drive API を叩き過ぎない)

export function vErr(message, detail) {
  const e = new Error(message);
  e.code = 'VALIDATION';
  if (detail) e.detail = detail;
  return e;
}

// Drive クライアントはプロセス内で使い回す (毎リクエストの認証クライアント再生成を避ける)。
// GOOGLE_SERVICE_ACCOUNT_KEY は起動時のみ読む運用 (env は再起動で反映) なのでキャッシュ無効化は不要。
let _cached = null;
function getDriveClient() {
  if (_cached) return _cached;
  const keyBase64 = process.env.GOOGLE_SERVICE_ACCOUNT_KEY;
  if (!keyBase64) throw new Error('GOOGLE_SERVICE_ACCOUNT_KEY が未設定です (Render の env を確認してください)');
  const keyJson = JSON.parse(Buffer.from(keyBase64, 'base64').toString('utf-8'));
  const auth = new google.auth.GoogleAuth({
    credentials: keyJson,
    scopes: ['https://www.googleapis.com/auth/drive.readonly'],
  });
  _cached = { drive: google.drive({ version: 'v3', auth }), serviceAccountEmail: keyJson.client_email || null };
  return _cached;
}

// Drive の RFC3339 UTC を「YYYY-MM-DD HH:mm」JST 表記へ (toISOString は UTC のままなので +9h を自前計算)
export function toJstDisplay(iso) {
  if (!iso) return null;
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return null;
  const d = new Date(t + 9 * 3600 * 1000);
  const p = (n) => String(n).padStart(2, '0');
  return `${d.getUTCFullYear()}-${p(d.getUTCMonth() + 1)}-${p(d.getUTCDate())} ${p(d.getUTCHours())}:${p(d.getUTCMinutes())}`;
}

function assertConfig(cfg) {
  if (!cfg || !cfg.folderId || !cfg.filename) {
    throw new Error('Drive取得の設定が不正です (folderId / filename が必要)');
  }
}

/**
 * フォルダ内から対象ファイルを検索して metadata を返す。
 * 同名が複数あれば modifiedTime 最新を採用。見つからなければ VALIDATION エラー。
 */
export async function findDriveFile(cfg) {
  assertConfig(cfg);
  const { drive, serviceAccountEmail } = getDriveClient();
  const esc = (s) => String(s).replace(/\\/g, '\\\\').replace(/'/g, "\\'"); // 検索クエリ用エスケープ
  const label = cfg.label || cfg.filename;

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
      `Drive フォルダにアクセスできません (${label})。フォルダをサービスアカウント` +
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
    throw vErr(`Drive フォルダに「${cfg.filename}」が見つかりません (${label})。${cfg.notFoundHint || ''}`.trim());
  }
  return {
    label,
    filename: file.name,
    file_id: file.id,
    size: file.size != null ? Number(file.size) : null,
    modified_time: file.modifiedTime || null,           // RFC3339 UTC (機械用)
    modified_time_jst: toJstDisplay(file.modifiedTime), // 表示用 JST
  };
}

/**
 * フォルダ内のファイル一覧 (名前の部分一致・更新日時降順)。
 * picking の「Driveから取り込む」のように、固定ファイル名ではなく
 * 「フォルダに溜まっていくCSVから選ぶ」用途向け (2026-08-12 追加)。
 */
export async function listDriveFiles({ folderId, nameContains, pageSize = 50 }) {
  if (!folderId) throw new Error('listDriveFiles: folderId が必要です');
  const { drive, serviceAccountEmail } = getDriveClient();
  const esc = (s) => String(s).replace(/\\/g, '\\\\').replace(/'/g, "\\'");
  let driveId = null;
  try {
    const meta = await drive.files.get(
      { fileId: folderId, fields: 'id, driveId', supportsAllDrives: true },
      { timeout: TIMEOUT }
    );
    driveId = meta.data.driveId || null;
  } catch (e) {
    throw new Error(
      `Drive フォルダにアクセスできません。フォルダをサービスアカウント` +
      `${serviceAccountEmail ? ` 「${serviceAccountEmail}」` : ''} に閲覧者以上で共有してください。(詳細: ${e.message})`
    );
  }
  const q = [`'${esc(folderId)}' in parents`, 'trashed = false']
    .concat(nameContains ? [`name contains '${esc(nameContains)}'`] : [])
    .join(' and ');
  const list = await drive.files.list({
    q,
    fields: 'files(id, name, modifiedTime, size)',
    orderBy: 'modifiedTime desc',
    pageSize,
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
    ...(driveId ? { corpora: 'drive', driveId } : { corpora: 'user' }),
  }, { timeout: TIMEOUT });
  return (list.data.files || []).map((f) => ({
    file_id: f.id,
    filename: f.name,
    size: f.size != null ? Number(f.size) : null,
    modified_time: f.modifiedTime || null,
    modified_time_jst: toJstDisplay(f.modifiedTime),
  }));
}

/**
 * file_id 指定でCSV本体をダウンロードする。
 * confused-deputy 防止のため、必ず「指定フォルダ直下のファイルであること」を検証する
 * (サービスアカウントは他フォルダも閲覧できるため、任意IDのDLを許すと権限の横断になる)。
 */
export async function downloadDriveFileById({ fileId, folderId, maxBytes = DEFAULT_MAX_CSV_BYTES }) {
  if (!fileId || !folderId) throw new Error('downloadDriveFileById: fileId / folderId が必要です');
  const { drive } = getDriveClient();
  const getMeta = async () => {
    const meta = await drive.files.get(
      { fileId, fields: 'id, name, parents, size, modifiedTime, trashed', supportsAllDrives: true },
      { timeout: TIMEOUT }
    );
    return meta.data;
  };
  const inFolder = (f) => !f.trashed && Array.isArray(f.parents) && f.parents.includes(folderId);
  // 検証→DL→再検証 (downloadDriveCsv と同方針)。検証とDLの間にファイルが対象外フォルダへ
  // 移動・更新された場合に古い/権限外の内容を採用しないため、DL後に metadata を取り直して
  // 親と更新時刻の一致を確認する。差し替わっていたら1回だけ取り直す
  for (let attempt = 0; attempt < 2; attempt++) {
    const before = await getMeta();
    if (!inFolder(before)) {
      throw vErr('指定されたファイルは対象フォルダにありません。一覧を更新してやり直してください。');
    }
    if (before.size != null && Number(before.size) > maxBytes) {
      throw vErr(`ファイルが大きすぎます (${Math.round(Number(before.size) / 1024 / 1024)}MB > 上限${Math.round(maxBytes / 1024 / 1024)}MB)。`);
    }
    const res = await drive.files.get(
      { fileId, alt: 'media', supportsAllDrives: true },
      { responseType: 'arraybuffer', timeout: TIMEOUT }
    );
    const buffer = Buffer.from(res.data);
    if (buffer.length > maxBytes) {
      throw vErr(`ファイルが大きすぎます (${Math.round(buffer.length / 1024 / 1024)}MB > 上限${Math.round(maxBytes / 1024 / 1024)}MB)。`);
    }
    const after = await getMeta();
    if (!inFolder(after)) {
      throw vErr('指定されたファイルは対象フォルダにありません。一覧を更新してやり直してください。');
    }
    if (after.modifiedTime === before.modifiedTime) {
      return {
        buffer,
        filename: after.name,
        modified_time: after.modifiedTime || null,
        modified_time_jst: toJstDisplay(after.modifiedTime),
      };
    }
    // DL中に更新された → 取り直し
  }
  throw new Error('Drive ファイルが更新中のようです。少し待ってからもう一度お試しください。');
}

// ── 更新日時表示用 metadata (60 秒キャッシュ) ──
// キーは対象ファイルそのもの (フォルダ+ファイル名) にする。アプリ側の source 名に依存させない。
const _infoCache = new Map(); // `${folderId}/${filename}` → { at: epoch_ms, info }

const cacheKey = (cfg) => `${cfg.folderId}/${cfg.filename}`;

export async function getDriveCsvInfo(cfg) {
  assertConfig(cfg);
  const key = cacheKey(cfg);
  const hit = _infoCache.get(key);
  if (hit && Date.now() - hit.at < INFO_CACHE_TTL_MS) return hit.info;
  const info = await findDriveFile(cfg);
  _infoCache.set(key, { at: Date.now(), info });
  return info;
}

/**
 * CSV 本体をダウンロードして Buffer で返す (エンコーディング判定は呼び出し側の parser に任せる)。
 * DL 後に modifiedTime を再確認し、DL 中にファイルが差し替わっていたら 1 回だけ取り直す
 * (表示する更新日時と取り込んだ中身の世代を一致させる)。
 */
export async function downloadDriveCsv(cfg) {
  assertConfig(cfg);
  const maxBytes = cfg.maxBytes || DEFAULT_MAX_CSV_BYTES;
  const { drive } = getDriveClient();
  for (let attempt = 0; attempt < 2; attempt++) {
    const info = await findDriveFile(cfg);
    if (info.size != null && info.size > maxBytes) {
      throw vErr(`ファイルが大きすぎます (${Math.round(info.size / 1024 / 1024)}MB > 上限${Math.round(maxBytes / 1024 / 1024)}MB)。対象ファイルが正しいか確認してください。`);
    }
    const res = await drive.files.get(
      { fileId: info.file_id, alt: 'media', supportsAllDrives: true },
      { responseType: 'arraybuffer', timeout: TIMEOUT }
    );
    const buffer = Buffer.from(res.data);
    if (buffer.length > maxBytes) {
      throw vErr(`ファイルが大きすぎます (${Math.round(buffer.length / 1024 / 1024)}MB > 上限${Math.round(maxBytes / 1024 / 1024)}MB)。対象ファイルが正しいか確認してください。`);
    }
    // DL 中の差し替え検知: 同名検索をやり直し、file_id と modifiedTime の両方が一致した時だけ採用。
    // 「削除→同名再アップロード」は file_id が変わるため、旧IDの modifiedTime 確認では検知できない。
    // 再確認の検索自体が失敗したら throw されエラー扱い (成功扱いにしない、fail-closed)。
    const after = await findDriveFile(cfg);
    if (after.file_id === info.file_id && after.modified_time === info.modified_time) {
      _infoCache.set(cacheKey(cfg), { at: Date.now(), info }); // 表示キャッシュも取込時点に更新
      return { ...info, buffer };
    }
    // 差し替わっていた → ループ先頭から取り直し (次周は新しい方を取る)
  }
  throw new Error('Drive ファイルが更新中のようです。少し待ってからもう一度お試しください。');
}
