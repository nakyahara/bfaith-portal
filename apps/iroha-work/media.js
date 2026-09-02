/**
 * いろは在庫化作業アプリ — 完成写真・動画 (Drive 保存 + Notion 反映)
 *
 * 流れ (要件定義 §6 / §1.7 ② operation_id 付き outbox):
 *   ①iPad から受信 → 検証 (種類・サイズ・マジックバイト・上限枚数) → DATA_DIR に実体保存 +
 *     f_iroha_card_media に status='stored' で記録 → **即応答** (Drive を待たせない)
 *   ②裏のキューが Drive へアップロード (成功で status='uploaded'、実体ファイルは削除)
 *   ③カードの「完成写真」(files プロパティ) を貼り直す (成功で status='synced')
 *   失敗は next_retry_at で再試行。同じ operation_id の再送は既存行を返す (二重登録しない)。
 *   Notion 反映の失敗は現場操作の失敗にしない (アップロード済みならいつでも貼り直せる)。
 *
 * Drive は既存サービスアカウント (GOOGLE_SERVICE_ACCOUNT_KEY) + 共有ドライブのフォルダ
 * (IROHA_WORK_DRIVE_FOLDER_ID。SA をコンテンツ管理者で追加しておく — fba-replenishment と同じ制約)。
 * 公開リンクにはしない (フォルダの共有範囲のまま)。
 */
import fs from 'fs';
import path from 'path';
import { google } from 'googleapis';
import { getDB, logEvent } from './db.js';
import { notionRequest, isNotionConfigured } from '../inbound-check/notion.js';

export const MAX_PHOTOS = 3;
export const MAX_VIDEOS = 1;
export const MAX_PHOTO_BYTES = 8 * 1024 * 1024;     // canvas縮小後は通常1MB未満。余裕をみて8MB
export const MAX_VIDEO_BYTES = 120 * 1024 * 1024;
const RETRY_BASE_MS = 2 * 60 * 1000;
const MAX_ATTEMPTS = 10;
const BLOCKED_UNTIL = '9999-12-31T00:00:00.000Z';   // 使い切ったら管理画面の「再実行」まで止める
const MEDIA_PROP = '完成写真';

const utcNow = () => new Date().toISOString();

export const MEDIA_DIR = process.env.DATA_DIR ? path.join(process.env.DATA_DIR, 'iroha-media') : 'data/iroha-media';

export function isDriveConfigured() {
  return !!(process.env.GOOGLE_SERVICE_ACCOUNT_KEY && process.env.IROHA_WORK_DRIVE_FOLDER_ID);
}

// ─── 受信時の検証 ───

/** 実体の先頭バイトで種類を確かめる (拡張子・Content-Type は自己申告なので信じない) */
export function sniffKind(buf) {
  if (!buf || buf.length < 12) return null;
  if (buf[0] === 0xFF && buf[1] === 0xD8 && buf[2] === 0xFF) return 'photo';           // JPEG
  if (buf.slice(4, 8).toString('latin1') === 'ftyp') return 'video';                   // MP4 / MOV
  return null;
}

export function countActiveMedia(pageId, kind) {
  return getDB().prepare(`SELECT COUNT(*) c FROM f_iroha_card_media
    WHERE page_id = ? AND kind = ? AND deleted_at IS NULL`).get(pageId, kind).c;
}

export function countActivePhotos(pageId) { return countActiveMedia(pageId, 'photo'); }

/** 先頭バイトだけ読む (動画をメモリへ丸ごと載せない) */
function readHead(filePath, n = 16) {
  const buf = Buffer.alloc(n);
  const fd = fs.openSync(filePath, 'r');
  try { fs.readSync(fd, buf, 0, n, 0); } finally { fs.closeSync(fd); }
  return buf;
}

/**
 * 受信を outbox に積む。filePath (multer の一時ファイル) は成功時に MEDIA_DIR へ**移動**する
 * (動画120MBをメモリやコピーで往復させない)。失敗時の一時ファイル掃除は呼び元が行う。
 * 同じ operation_id の再送は既存行を返す (冪等)。
 * @returns {ok:true, media, already?} | {ok:false, error, message}
 */
export function addMedia({ pageId, productCode = null, kind, mime, filePath, worker, deviceLabel = null, operationId }) {
  const opId = String(operationId || '').trim();
  if (!/^[A-Za-z0-9_-]{8,64}$/.test(opId)) return { ok: false, error: 'bad_request', message: 'operation_id が不正です' };
  const size = fs.statSync(filePath).size;
  const sniffed = sniffKind(readHead(filePath));
  if (sniffed !== kind) {
    return { ok: false, error: 'bad_file', message: kind === 'photo' ? '写真 (JPEG) を送ってください' : '動画 (MP4/MOV) を送ってください' };
  }
  const cap = kind === 'photo' ? MAX_PHOTO_BYTES : MAX_VIDEO_BYTES;
  if (size > cap) {
    return { ok: false, error: 'too_large', message: `ファイルが大きすぎます (上限 ${Math.round(cap / 1024 / 1024)}MB)` };
  }
  const db = getDB();
  const dup = db.prepare('SELECT * FROM f_iroha_card_media WHERE operation_id = ?').get(opId);
  if (dup) return { ok: true, already: true, media: publicMedia(dup) };
  const max = kind === 'photo' ? MAX_PHOTOS : MAX_VIDEOS;
  if (countActiveMedia(pageId, kind) >= max) {
    return { ok: false, error: 'cap_reached',
      message: kind === 'photo' ? `写真は${max}枚までです。不要な写真を削除してから撮り直してください` : `動画は${max}本までです。不要な動画を削除してから撮り直してください` };
  }
  fs.mkdirSync(MEDIA_DIR, { recursive: true });
  const ext = kind === 'photo' ? 'jpg' : 'mp4';
  const localPath = path.join(MEDIA_DIR, `${opId}.${ext}`);
  fs.renameSync(filePath, localPath);
  const info = db.prepare(`INSERT INTO f_iroha_card_media
    (operation_id, page_id, product_code, kind, mime, size, local_path, status, worker_id, worker_name, device_label, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, 'stored', ?, ?, ?, ?)`)
    .run(opId, pageId, productCode, kind, mime || null, size, localPath,
      worker.id, worker.display_name, deviceLabel, utcNow());
  const row = db.prepare('SELECT * FROM f_iroha_card_media WHERE id = ?').get(Number(info.lastInsertRowid));
  return { ok: true, media: publicMedia(row) };
}

/** 画面へ出す形 (ローカルパス等の内部情報は出さない) */
export function publicMedia(r) {
  return {
    id: r.id, kind: r.kind, status: r.status, url: r.drive_url || null,
    worker_id: r.worker_id, worker_name: r.worker_name, created_at: r.created_at,
    error: r.next_retry_at === BLOCKED_UNTIL ? (r.error || '失敗') : null,   // 諦めた失敗だけ画面へ
  };
}

/** page_id → 有効なメディア一覧 (一覧・詳細表示用) */
export function mediaByPage() {
  const map = new Map();
  for (const r of getDB().prepare(`SELECT * FROM f_iroha_card_media WHERE deleted_at IS NULL ORDER BY id`).all()) {
    if (!map.has(r.page_id)) map.set(r.page_id, []);
    map.get(r.page_id).push(publicMedia(r));
  }
  return map;
}

/** 論理削除。削除できるのは撮った本人 (または portal セッション)。Notion の貼り直しも積む */
export function softDeleteMedia(id, { workerId = null, actor = null, isSession = false }) {
  const db = getDB();
  const row = db.prepare('SELECT * FROM f_iroha_card_media WHERE id = ?').get(Number(id));
  if (!row || row.deleted_at) return { ok: false, error: 'not_found', message: '写真・動画が見つかりません' };
  if (!isSession && row.worker_id !== Number(workerId)) {
    return { ok: false, error: 'forbidden', message: '削除できるのは撮った本人です (職員はPCの管理画面から)' };
  }
  db.prepare('UPDATE f_iroha_card_media SET deleted_at = ?, deleted_by = ? WHERE id = ?')
    .run(utcNow(), actor || String(workerId), row.id);
  if (row.local_path) { try { fs.unlinkSync(row.local_path); } catch { /* 送信済みなら無い */ } }
  // Notion 側の files からも外すため、同じページの synced を貼り直し対象へ戻す
  db.prepare(`UPDATE f_iroha_card_media SET status = 'uploaded', synced_at = NULL
    WHERE page_id = ? AND status = 'synced' AND deleted_at IS NULL`).run(row.page_id);
  schedule();
  return { ok: true };
}

/** 管理画面の「再実行」: ブロックを解除して即時キュー */
export function resetMedia(id) {
  const n = getDB().prepare(`UPDATE f_iroha_card_media
    SET next_retry_at = NULL, error = NULL, attempt_count = 0 WHERE id = ?`).run(Number(id)).changes;
  if (n > 0) schedule();
  return n > 0;
}

export function listMediaForAdmin(limit = 50) {
  return getDB().prepare('SELECT * FROM f_iroha_card_media ORDER BY id DESC LIMIT ?').all(Number(limit) || 50);
}

// ─── Drive アップロード ───

function getDriveClient() {
  const keyBase64 = process.env.GOOGLE_SERVICE_ACCOUNT_KEY;
  if (!keyBase64) throw new Error('GOOGLE_SERVICE_ACCOUNT_KEY が未設定です');
  const keyJson = JSON.parse(Buffer.from(keyBase64, 'base64').toString('utf-8'));
  const auth = new google.auth.GoogleAuth({ credentials: keyJson, scopes: ['https://www.googleapis.com/auth/drive'] });
  return google.drive({ version: 'v3', auth });
}

/** 実装差し替え可能な Drive アップロード (テストでモックする)。戻り値 {fileId, url} */
async function driveUploadReal({ localPath, filename, mime }) {
  const folderId = process.env.IROHA_WORK_DRIVE_FOLDER_ID;
  if (!folderId) throw new Error('IROHA_WORK_DRIVE_FOLDER_ID が未設定です');
  const drive = getDriveClient();
  const res = await drive.files.create({
    requestBody: { name: filename, parents: [folderId] },
    media: { mimeType: mime || 'application/octet-stream', body: fs.createReadStream(localPath) },
    fields: 'id, webViewLink',
    supportsAllDrives: true,
  }, { timeout: 120000 });
  return { fileId: res.data.id, url: res.data.webViewLink || `https://drive.google.com/file/d/${res.data.id}/view` };
}

let driveUploadImpl = driveUploadReal;
export function _setDriveUpload(fn) { driveUploadImpl = fn || driveUploadReal; }

// ファイル名に個人名を入れない (要件 §6): 日時 + 商品コード + 種類 + operation_id
function filenameFor(r) {
  const ts = String(r.created_at).replace(/[-:TZ.]/g, '').slice(0, 14);
  const code = String(r.product_code || 'nocode').replace(/[^A-Za-z0-9_-]/g, '') || 'nocode';
  return `${ts}_${code}_${r.kind}_${r.operation_id.slice(0, 12)}.${r.kind === 'photo' ? 'jpg' : 'mp4'}`;
}

function markFail(db, r, message) {
  const attempts = (r.attempt_count || 0) + 1;
  const retryAt = attempts >= MAX_ATTEMPTS ? BLOCKED_UNTIL
    : new Date(Date.now() + RETRY_BASE_MS * attempts).toISOString();
  db.prepare('UPDATE f_iroha_card_media SET error = ?, attempt_count = ?, next_retry_at = ? WHERE id = ?')
    .run(String(message).slice(0, 300), attempts, retryAt, r.id);
  if (attempts >= MAX_ATTEMPTS) console.error(`[iroha-work media] #${r.id} を${MAX_ATTEMPTS}回失敗で停止 (管理画面の再実行待ち): ${message}`);
}

// ─── Notion 反映 (カードの「完成写真」files プロパティを貼り直す) ───

let mediaPropCache = null; // { at, ok }
async function ensureMediaProp() {
  if (mediaPropCache && Date.now() - mediaPropCache.at < 10 * 60 * 1000) return mediaPropCache.ok;
  const dbId = process.env.INBOUND_CHECK_NOTION_DB_ID;
  const meta = await notionRequest(`/databases/${dbId}`, 'GET');
  const prop = meta.properties?.[MEDIA_PROP];
  let ok = false;
  if (!prop) {
    await notionRequest(`/databases/${dbId}`, 'PATCH', { properties: { [MEDIA_PROP]: { files: {} } } });
    ok = true;
  } else if (prop.type === 'files') {
    ok = true;
  } else {
    console.warn(`[iroha-work media] Notion の「${MEDIA_PROP}」が files 型ではないため反映をスキップします (${prop.type})`);
  }
  mediaPropCache = { at: Date.now(), ok };
  return ok;
}
export function _clearMediaPropCache() { mediaPropCache = null; }

/** ページの有効メディア (uploaded/synced) を Notion の files に貼り直す */
async function syncPageToNotion(db, pageId) {
  const rows = db.prepare(`SELECT * FROM f_iroha_card_media
    WHERE page_id = ? AND deleted_at IS NULL AND drive_url IS NOT NULL ORDER BY id`).all(pageId);
  const files = rows.map(r => ({ name: filenameFor(r).slice(0, 100), external: { url: r.drive_url } }));
  await notionRequest(`/pages/${pageId}`, 'PATCH', { properties: { [MEDIA_PROP]: { files } } });
  const now = utcNow();
  const ids = rows.map(r => r.id);
  if (ids.length > 0) {
    db.prepare(`UPDATE f_iroha_card_media SET status = 'synced', synced_at = ?, error = NULL, next_retry_at = NULL
      WHERE id IN (${ids.map(() => '?').join(',')})`).run(now, ...ids);
  }
}

// ─── キュー (単一プロセス前提の直列ワーカー) ───

let running = false;
let timer = null;

export async function processMediaQueue() {
  if (running) return { ok: true, skipped: true };
  running = true;
  const db = getDB();
  const now = utcNow();
  const stats = { uploaded: 0, synced: 0, failed: 0 };
  try {
    // ①Drive へ (stored の行)
    const toUpload = db.prepare(`SELECT * FROM f_iroha_card_media
      WHERE status = 'stored' AND deleted_at IS NULL AND (next_retry_at IS NULL OR next_retry_at <= ?)
      ORDER BY id LIMIT 20`).all(now);
    for (const r of toUpload) {
      try {
        if (!r.local_path || !fs.existsSync(r.local_path)) throw new Error('実体ファイルがありません (再起動で消えた可能性。撮り直してください)');
        const { fileId, url } = await driveUploadImpl({ localPath: r.local_path, filename: filenameFor(r), mime: r.mime });
        db.prepare(`UPDATE f_iroha_card_media
          SET status = 'uploaded', drive_file_id = ?, drive_url = ?, uploaded_at = ?, error = NULL, next_retry_at = NULL
          WHERE id = ?`).run(fileId, url, utcNow(), r.id);
        try { fs.unlinkSync(r.local_path); } catch { /* 消せなくても実害なし */ }
        stats.uploaded++;
      } catch (e) {
        markFail(db, r, e.message);
        stats.failed++;
      }
    }
    // ②Notion へ (uploaded の行があるページ)。反映は最終同期扱い — 失敗しても現場は困らない
    if (isNotionConfigured()) {
      const pages = db.prepare(`SELECT DISTINCT page_id FROM f_iroha_card_media
        WHERE status = 'uploaded' AND deleted_at IS NULL AND (next_retry_at IS NULL OR next_retry_at <= ?)`)
        .all(now).map(x => x.page_id);
      for (const pageId of pages) {
        try {
          if (!(await ensureMediaProp())) break;   // プロパティ型が合わない間は全ページ保留
          await syncPageToNotion(db, pageId);
          stats.synced++;
        } catch (e) {
          for (const r of db.prepare(`SELECT * FROM f_iroha_card_media
            WHERE page_id = ? AND status = 'uploaded' AND deleted_at IS NULL`).all(pageId)) {
            markFail(db, r, `Notion反映: ${e.message}`);
          }
          stats.failed++;
        }
      }
    }
  } finally {
    running = false;
  }
  return { ok: true, ...stats };
}

/** すぐ1回まわす (受信・削除の直後に呼ぶ)。失敗はキューの再試行に任せる */
export function schedule() {
  setImmediate(() => { processMediaQueue().catch((e) => console.error('[iroha-work media] queue error', e)); });
}

/** 2分おきの再試行ワーカー (プロセス内。picking の画像キューと同じ扱いで台帳対象の cron ではない) */
export function startMediaWorker() {
  if (timer) return;
  timer = setInterval(() => {
    processMediaQueue().catch((e) => console.error('[iroha-work media] queue error', e));
  }, 2 * 60 * 1000);
  timer.unref?.();
  console.log('[iroha-work] メディア送信ワーカー起動 (2分間隔で再試行)');
}
