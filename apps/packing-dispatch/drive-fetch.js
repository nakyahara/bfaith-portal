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
// 手動アップロード経路の multer 上限 (router.js の 20MB) と揃える。Drive経由だけ無制限に
// メモリへ読み込まないための防御 (Codex R1 High)。
const MAX_CSV_BYTES = 20 * 1024 * 1024;
const INFO_CACHE_TTL_MS = 60 * 1000; // 更新日時表示は 60 秒キャッシュ (表示用ポーリングで Drive API を叩き過ぎない)

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
  yupacketpuff: {
    label: 'ゆうパケットパフ',
    // ファイル名末尾のタイムスタンプは初回出力時のもので固定 (ゆうプリRが同名上書き更新する運用を実機確認済み 2026-07-18)。
    // 「＿」(全角) と「_」(半角) が混在している点に注意 — Drive 上の実ファイル名そのまま。
    folderId: process.env.PD_DRIVE_FOLDER_YUPACKETPUFF || '1V-4iZWnmi9E2Bi90a2JlTUzqsL3V_Nsu',
    filename: process.env.PD_DRIVE_FILE_YUPACKETPUFF || 'ゆうプリR出荷履歴＿2項目3項目_20250714102302.csv',
    notFoundHint: 'ゆうプリRからのDLが済んでいるか確認してください。',
  },
};

export const DRIVE_IMPORT_SOURCES = Object.keys(DRIVE_SOURCES);

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

// Drive クライアントはプロセス内で使い回す (毎リクエストの認証クライアント再生成を避ける、Codex R1 Medium)。
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
    throw vErr(`Drive フォルダに「${cfg.filename}」が見つかりません (${cfg.label})。${cfg.notFoundHint || 'B2クラウドからのDLが済んでいるか確認してください。'}`);
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

// ── 更新日時表示用 metadata (60 秒キャッシュ) ──
const _infoCache = new Map(); // source → { at: epoch_ms, info }

async function getInfoCached(source) {
  const hit = _infoCache.get(source);
  if (hit && Date.now() - hit.at < INFO_CACHE_TTL_MS) return hit.info;
  const info = await findDriveFile(source);
  _infoCache.set(source, { at: Date.now(), info });
  return info;
}

/**
 * 全 source の metadata をまとめて返す (UI の「更新日時」表示用、1 リクエストで完結)。
 * 個別失敗は他 source を巻き込まない ({ ok:false, message } で返す)。
 */
export async function getDriveCsvInfoAll() {
  const entries = await Promise.all(DRIVE_IMPORT_SOURCES.map(async (source) => {
    try {
      return [source, { ok: true, ...(await getInfoCached(source)) }];
    } catch (e) {
      return [source, { ok: false, message: e.message }];
    }
  }));
  return Object.fromEntries(entries);
}

/**
 * CSV 本体をダウンロードして Buffer で返す (エンコーディングは importTrackingCsv 側の parser が判定)。
 * DL 後に modifiedTime を再確認し、DL 中にファイルが差し替わっていたら 1 回だけ取り直す
 * (表示する更新日時と取り込んだ中身の世代を一致させる、Codex R1 Medium)。
 */
export async function downloadDriveCsv(source) {
  const { drive } = getDriveClient();
  for (let attempt = 0; attempt < 2; attempt++) {
    const info = await findDriveFile(source);
    if (info.size != null && info.size > MAX_CSV_BYTES) {
      throw vErr(`ファイルが大きすぎます (${Math.round(info.size / 1024 / 1024)}MB > 上限20MB)。対象ファイルが正しいか確認してください。`);
    }
    const res = await drive.files.get(
      { fileId: info.file_id, alt: 'media', supportsAllDrives: true },
      { responseType: 'arraybuffer', timeout: TIMEOUT }
    );
    const buffer = Buffer.from(res.data);
    if (buffer.length > MAX_CSV_BYTES) {
      throw vErr(`ファイルが大きすぎます (${Math.round(buffer.length / 1024 / 1024)}MB > 上限20MB)。対象ファイルが正しいか確認してください。`);
    }
    // DL 中の差し替え検知: 同名検索をやり直し、file_id と modifiedTime の両方が一致した時だけ採用。
    // 「削除→同名再アップロード」は file_id が変わるため、旧IDの modifiedTime 確認では検知できない
    // (Codex R2 High)。再確認の検索自体が失敗したら throw されエラー扱い (成功扱いにしない、fail-closed)。
    const after = await findDriveFile(source);
    if (after.file_id === info.file_id && after.modified_time === info.modified_time) {
      _infoCache.set(source, { at: Date.now(), info }); // 表示キャッシュも取込時点に更新
      return { ...info, buffer };
    }
    // 差し替わっていた → ループ先頭から取り直し (次周は新しい方を取る)
  }
  throw new Error('Drive ファイルが更新中のようです。少し待ってからもう一度お試しください。');
}
