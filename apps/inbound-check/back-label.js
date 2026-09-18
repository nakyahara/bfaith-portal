/**
 * 入荷受付チェック — 新商品の「パッケージ裏面ラベル」写真 (2026-09-18 中原さん指示)
 *
 * なぜ要るか: 商品登録 (product-hub) の「基本情報入力」は、成分表示・原材料・内容量など
 *   **実物を見ないと埋まらない項目**があると「パッケージ裏面の確認待ち」で止まる。
 *   現物が通るのは入荷のときだけなので、そこで撮っておけば商品登録が実物を待たずに進む。
 *
 * 流れ (いろは在庫化アプリ apps/iroha-work/media.js と同じ二段構え):
 *   ①iPad から受信 → 検証 (マジックバイト・サイズ・枚数) → DATA_DIR に実体を置く →
 *     f_inbound_check_back_labels に status='stored' で記録 → **即応答**
 *   ②裏のキューが Google Drive へ上げる (成功で status='uploaded'、ローカルの実体は消す)
 *   失敗は next_retry_at で再試行。同じ operation_id の再送は既存行を返す (二重登録しない)
 *
 * 🚨**Drive を待たせない**のが肝。撮影は「確認」の必須条件 (中原さん 2026-09-18) なので、
 *   Drive が落ちている / 遅いだけで入荷受付そのものが止まってはいけない。サーバーに実体が
 *   置けた時点でゲートは満たす。Drive への配送は後追いでよい。
 *
 * 保存先: product-hub の画像親フォルダ (PH_IMAGE_FOLDER_PARENT_ID) 直下の「_裏面ラベル」。
 *   サービスアカウントの権限が既にある場所なので、共有ドライブ側の設定作業なしで動き出せる。
 *   商品ごとの画像フォルダには**入れない** — 商品ページ画像の番号規則 (商品コード_00 …) と
 *   混ざり、「フォルダから自動セット」が裏面写真を商品画像として拾ってしまうため。
 *
 * 環境変数:
 *   GOOGLE_SERVICE_ACCOUNT_KEY           … SA 鍵 (base64)。無ければ Drive 配送は止まる (ローカルには残る)
 *   PH_IMAGE_FOLDER_PARENT_ID            … 画像親フォルダ (product-hub と共用。既定は同アプリの既定値)
 *   INBOUND_CHECK_BACK_LABEL_FOLDER_ID   … 保存先フォルダを直接指定したいとき (親の下に作らせない)
 *   INBOUND_CHECK_BACK_LABEL_REQUIRED    … '0'/'off'/'false' で**必須をやめる** (撮影欄は残る)。
 *                                          カメラ故障などで入荷受付が止まったときの緊急停止
 */
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { google } from 'googleapis';
import { getDB } from './db.js';
import { imageFolderParentId } from '../product-hub/services/drive-image-folder.js';

/** 1商品あたりの上限。裏面のほかに側面の成分表・使用方法を撮ることがあるので複数枚許す */
export const MAX_PHOTOS_PER_PRODUCT = 4;
/** 受け取る上限。端末側で長辺1600pxのJPEGに縮めてから送るので通常は1MB未満 */
export const MAX_PHOTO_BYTES = 8 * 1024 * 1024;
/** Drive 配送の再試行 */
const RETRY_BASE_MS = 2 * 60 * 1000;
const MAX_ATTEMPTS = 10;
const BLOCKED_UNTIL = '9999-12-31T00:00:00.000Z';   // 使い切ったら管理画面の「再実行」まで止める
const ERROR_MAX_LEN = 300;
const DRIVE_TIMEOUT_MS = 120_000;
const DRIVE_META_TIMEOUT_MS = 30_000;
/** 保存先フォルダの名前。先頭の _ は商品フォルダ (商品コード_商品名) と並んだとき上に来るように */
export const BACK_LABEL_FOLDER_NAME = '_裏面ラベル';
const DRIVE_ID_RE = /^[A-Za-z0-9_-]{5,}$/;

export const MEDIA_DIR = process.env.DATA_DIR
  ? path.join(process.env.DATA_DIR, 'inbound-check-back-labels')
  : 'data/inbound-check-back-labels';
const TMP_DIR = path.join(MEDIA_DIR, 'tmp');

const utcNow = () => new Date().toISOString();
const norm = (v) => String(v == null ? '' : v).trim().toLowerCase();

// ─── 撮影を必須にするか (緊急停止) ───

/**
 * 新商品の行で「撮らないと確認を完了できない」か。
 * 既定は必須 (中原さん 2026-09-18)。env で切れるのは、カメラ故障・端末不調で
 * 入荷受付そのものが止まったときに現場を動かし続けるため。
 */
export function isBackLabelRequired() {
  const v = String(process.env.INBOUND_CHECK_BACK_LABEL_REQUIRED ?? '').trim().toLowerCase();
  return !['0', 'off', 'false', 'no'].includes(v);
}

/**
 * 「この行は写真を撮るまで確認できない」か。一覧 (getState) と確認 API が
 * **同じ規則**を見るように、判定はこの1か所に置く。
 * ⚠verdict が 'unknown' のときは required にしない — 判定材料が無いだけで新商品とは限らず、
 *   それで入荷受付が止まると現場が動けなくなる (撮影欄は画面に出す)
 */
export function needsBackLabel(verdict, photoCount) {
  return isBackLabelRequired() && verdict === 'new' && Number(photoCount || 0) === 0;
}

// ─── 受信時の検証 ───

/**
 * 実体の先頭バイトで種類を確かめる (拡張子・Content-Type は自己申告なので信じない)。
 * 受けるのは JPEG と PNG だけ。HEIC/HEIF は端末側の canvas で JPEG に変換してから送る
 * (変換せずに受けると product-hub のサムネイルで開けない)
 */
export function sniffImage(buf) {
  if (!buf || buf.length < 12) return null;
  if (buf[0] === 0xFF && buf[1] === 0xD8 && buf[2] === 0xFF) return 'image/jpeg';
  if (buf.slice(0, 8).equals(Buffer.from([0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]))) return 'image/png';
  return null;
}

/** operation_id の形 (端末が作る冪等キー)。長すぎる値・変な文字はここで落とす */
const OP_ID_RE = /^[A-Za-z0-9_-]{6,64}$/;

/**
 * 受け取ったファイルを確かめる。落ちたら一時ファイルは呼び出し側が消す。
 * @returns {{ok:true, opId:string, size:number, mime:string} | {ok:false, error:string, message:string}}
 */
export function inspectUpload({ filePath, operationId }) {
  const opId = String(operationId || '').trim();
  if (!OP_ID_RE.test(opId)) return { ok: false, error: 'bad_request', message: '送信IDが不正です' };
  let st;
  try { st = fs.statSync(filePath); } catch { return { ok: false, error: 'bad_file', message: 'ファイルを読めませんでした' }; }
  if (!st.isFile() || st.size === 0) return { ok: false, error: 'bad_file', message: 'ファイルが空です' };
  if (st.size > MAX_PHOTO_BYTES) {
    return { ok: false, error: 'too_large', message: `写真が大きすぎます (${Math.round(MAX_PHOTO_BYTES / 1024 / 1024)}MBまで)` };
  }
  let head = Buffer.alloc(0);
  try {
    const fd = fs.openSync(filePath, 'r');
    try {
      const b = Buffer.alloc(12);
      const n = fs.readSync(fd, b, 0, 12, 0);
      head = b.slice(0, n);
    } finally { fs.closeSync(fd); }
  } catch { return { ok: false, error: 'bad_file', message: 'ファイルを読めませんでした' }; }
  const mime = sniffImage(head);
  if (!mime) return { ok: false, error: 'bad_file', message: '写真 (JPEG / PNG) ではありません' };
  return { ok: true, opId, size: st.size, mime };
}

// ─── 参照 ───

/** 画面に出す1枚ぶん (ローカルの置き場所は外へ出さない) */
function publicPhoto(r) {
  if (!r) return null;
  return {
    id: r.id,
    code_key: r.code_key,
    product_id: r.product_id,
    status: r.status,
    // 見られるか: ローカルに実体があるか、Drive に上がっているか
    viewable: r.status === 'uploaded' ? !!r.drive_file_id : !!r.local_path,
    drive_url: r.drive_url || null,
    created_at: r.created_at,
    worker: r.worker || null,
    error: r.error || null,
  };
}

/**
 * 「まだ見られる写真」の条件。消したもの・実体を失ったものは数えない (Codex R1 #2)。
 * 実体を失った写真で「撮ってある」を満たすと、商品登録の側では何も見られないため
 */
const ALIVE = 'deleted_at IS NULL AND missing_file_at IS NULL';

/** 有効な (消していない・見られる) 写真を商品ごとにまとめて引く。画面は必ずこちらを使う */
export function photosByCode(codeKeys) {
  const keys = [...new Set((codeKeys || []).map(norm).filter(Boolean))];
  const out = new Map();
  if (keys.length === 0) return out;
  const db = getDB();
  // IN 句は SQLite の上限 (既定 999) があるので分割する
  for (let i = 0; i < keys.length; i += 500) {
    const chunk = keys.slice(i, i + 500);
    const rows = db.prepare(`SELECT * FROM f_inbound_check_back_labels
      WHERE ${ALIVE} AND code_key IN (${chunk.map(() => '?').join(',')})
      ORDER BY code_key, id`).all(...chunk);
    for (const r of rows) {
      if (!out.has(r.code_key)) out.set(r.code_key, []);
      out.get(r.code_key).push(publicPhoto(r));
    }
  }
  return out;
}

/** 1商品ぶん */
export function photosOf(codeKey) {
  return photosByCode([codeKey]).get(norm(codeKey)) || [];
}

/** 有効な枚数 (上限の判定・ゲートの判定に使う) */
export function countPhotos(codeKey) {
  return getDB().prepare(`SELECT COUNT(*) AS c FROM f_inbound_check_back_labels
    WHERE code_key = ? AND ${ALIVE}`).get(norm(codeKey)).c;
}

/** この行の実体がまだあるか (Drive へ上げ終わった行は local_path が無くて当然なので true) */
function fileAlive(r) {
  if (!r) return false;
  if (r.status === 'uploaded') return !!r.drive_file_id;
  return !!(r.local_path && fs.existsSync(r.local_path));
}

/**
 * その商品の未送信の写真の実体を確かめ、無くなっていれば印を付ける (Codex R2 #3)。
 * ⭐**確認ゲートの直前に呼ぶ** — 一覧 (5秒ポーリング) でやると写真の数だけ stat が走るので、
 *   一覧は楽観的なまま、押した瞬間の判定だけを厳密にする。
 * ⚠ここで付けた印は「Drive にも無い」と決めた訳ではない。キューの見回り
 *   (reconcileMissingFiles) が Drive にあれば拾い直す (Codex R2 #2)
 * @returns {number} 印を付けた枚数
 */
export function verifyStoredPhotos(codeKey) {
  const db = getDB();
  const rows = db.prepare(`SELECT * FROM f_inbound_check_back_labels
    WHERE code_key = ? AND status = 'stored' AND ${ALIVE}`).all(norm(codeKey));
  let n = 0;
  for (const r of rows) {
    if (fileAlive(r)) continue;
    markMissing(db, r.id);
    n++;
  }
  if (n > 0) console.error(`[inbound-check] 裏面ラベル ${n} 枚の実体が見つかりません (${codeKey} — 撮り直しが要ります)`);
  return n;
}

export function getPhotoRow(id) {
  return getDB().prepare('SELECT * FROM f_inbound_check_back_labels WHERE id = ?').get(Number(id)) || null;
}

// ─── 受信 (保存) ───

const storedPathOf = (opId) => path.join(MEDIA_DIR, `${opId}.jpg`);

/**
 * 1枚受け取る。実体を先に置き、置けてから行を作る (行だけあって実体が無い写真を作らない)。
 * @returns {{ok:true, already?:boolean, photo:object} | {ok:false, error:string, message:string}}
 */
export function addPhoto({ codeKey, productId, productName = null, batchId = null, lineKey = null, arNo = null,
  filePath, operationId, worker = null, deviceLabel = null, deviceId = null, inspected = null }) {
  const ins = inspected && inspected.ok ? inspected : inspectUpload({ filePath, operationId });
  if (!ins.ok) return ins;
  const { opId, size, mime } = ins;
  const key = norm(codeKey);
  if (!key) return { ok: false, error: 'bad_request', message: '商品が指定されていません' };
  const db = getDB();

  // 応答が消えての再送: 同じ送信IDなら既存行を返す (二重登録しない)
  const dup = db.prepare('SELECT * FROM f_inbound_check_back_labels WHERE operation_id = ?').get(opId);
  if (dup) {
    if (dup.code_key !== key) {
      return { ok: false, error: 'operation_conflict', message: 'この送信IDは別の商品で使われています (撮り直してください)' };
    }
    // ⚠消された / 実体を失った行を「もう入っています」と返すと、端末は手元の写真を捨てるのに
    //   サーバーには見られる写真が1枚も無い状態になる (Codex R1 #7)。別のエラーにして、
    //   端末が新しい送信IDで送り直せるようにする
    if (dup.deleted_at) {
      return { ok: false, error: 'gone', message: 'この写真は消されています (もう一度送ってください)' };
    }
    if (dup.missing_file_at) {
      return { ok: false, error: 'gone', message: 'この写真は保存できていませんでした (もう一度送ってください)' };
    }
    // ⭐「もう入っています」と返す前に**実体があるか確かめる** (Codex R2 #3)。
    //   キューの見回りより先に再送が来ると、実体を失った行を成功として返し、端末が
    //   手元の写真を捨ててしまう。ここで印を付けて送り直してもらう
    if (dup.status === 'stored' && !fileAlive(dup)) {
      markMissing(db, dup.id);
      return { ok: false, error: 'gone', message: 'この写真は保存できていませんでした (もう一度送ってください)' };
    }
    return { ok: true, already: true, photo: publicPhoto(dup) };
  }
  if (countPhotos(key) >= MAX_PHOTOS_PER_PRODUCT) {
    return { ok: false, error: 'cap_reached', message: `裏面の写真は${MAX_PHOTOS_PER_PRODUCT}枚までです。要らない写真を消してから撮り直してください` };
  }

  // ①実体を置く
  const localPath = storedPathOf(opId);
  try {
    fs.mkdirSync(MEDIA_DIR, { recursive: true });
    fs.renameSync(filePath, localPath);
  } catch (e) {
    // 別ファイルシステムなら rename が落ちる (Render のディスク構成次第) → コピーで置き直す
    try {
      fs.copyFileSync(filePath, localPath);
      try { fs.unlinkSync(filePath); } catch { /* 一時ファイルは sweep が片づける */ }
    } catch (e2) {
      return { ok: false, error: 'store_failed', message: `写真を保存できませんでした (${e2.message})` };
    }
  }

  // ②行を作る。作れなければ実体も捨てる (どちらか片方だけ残さない)
  try {
    const info = db.prepare(`INSERT INTO f_inbound_check_back_labels
      (operation_id, code_key, product_id, product_name, batch_id, line_key, ar_no,
       mime, size, local_path, status, worker, device_label, device_id, created_at)
      VALUES (?,?,?,?,?,?,?,?,?,?,'stored',?,?,?,?)`).run(
      opId, key, String(productId || codeKey), productName, batchId, lineKey, arNo,
      mime, size, localPath, worker, deviceLabel, deviceId == null ? null : Number(deviceId), utcNow());
    schedule();
    return { ok: true, photo: publicPhoto(getPhotoRow(info.lastInsertRowid)) };
  } catch (e) {
    try { fs.unlinkSync(localPath); } catch { /* 無ければよい */ }
    // 同時送信で UNIQUE に負けた = 相手の行が正。そちらを返す
    if (/UNIQUE/i.test(String(e?.message || ''))) {
      const raced = db.prepare('SELECT * FROM f_inbound_check_back_labels WHERE operation_id = ?').get(opId);
      if (raced) return { ok: true, already: true, photo: publicPhoto(raced) };
    }
    return { ok: false, error: 'store_failed', message: `写真を登録できませんでした (${e.message})` };
  }
}

/**
 * 撮り直し (論理削除)。Drive のファイルは消さない — 間違って消したときに人が戻せるように。
 * 消した行は画面にもゲートの数にも出ない。
 */
export function deletePhoto(id, { actor = null } = {}) {
  const db = getDB();
  const r = getPhotoRow(id);
  if (!r) return { ok: false, error: 'not_found', message: '写真が見つかりません' };
  if (r.deleted_at) return { ok: true, already: true };
  db.prepare('UPDATE f_inbound_check_back_labels SET deleted_at = ?, deleted_by = ? WHERE id = ?')
    .run(utcNow(), actor, r.id);
  // ローカルにしか無い写真は実体も片づける (Drive へ上げる前に消したもの)
  if (r.status === 'stored' && r.local_path) {
    try { fs.unlinkSync(r.local_path); } catch { /* 無ければよい */ }
  }
  return { ok: true };
}

// ─── 配信 (画面で開く) ───

/**
 * 1枚の中身を返す。ローカルに実体があればそれ、無ければ Drive から取り直す。
 * @returns {{kind:'local', path:string, mime:string} | {kind:'drive', fileId:string, mime:string} | null}
 */
export function photoSource(id) {
  const r = getPhotoRow(id);
  if (!r || r.deleted_at || r.missing_file_at) return null;
  if (r.local_path && fs.existsSync(r.local_path)) return { kind: 'local', path: r.local_path, mime: r.mime || 'image/jpeg' };
  if (r.drive_file_id) return { kind: 'drive', fileId: r.drive_file_id, mime: r.mime || 'image/jpeg' };
  return null;
}

// ─── Google Drive ───

function getDriveClient() {
  const keyBase64 = process.env.GOOGLE_SERVICE_ACCOUNT_KEY;
  if (!keyBase64) throw new Error('GOOGLE_SERVICE_ACCOUNT_KEY が未設定です');
  const keyJson = JSON.parse(Buffer.from(keyBase64, 'base64').toString('utf-8'));
  const auth = new google.auth.GoogleAuth({ credentials: keyJson, scopes: ['https://www.googleapis.com/auth/drive'] });
  return google.drive({ version: 'v3', auth });
}

export function isDriveConfigured() {
  return !!process.env.GOOGLE_SERVICE_ACCOUNT_KEY;
}

const esc = (s) => String(s).replace(/\\/g, '\\\\').replace(/'/g, "\\'");

/** 保存先フォルダ。env で直接指定があればそれ、無ければ画像親フォルダの下に作る (冪等) */
let folderCache = null;   // { id, at }
const FOLDER_CACHE_MS = 10 * 60 * 1000;
export function _clearFolderCache() { folderCache = null; }

async function ensureFolderId(drive) {
  const direct = String(process.env.INBOUND_CHECK_BACK_LABEL_FOLDER_ID || '').trim();
  if (direct) {
    if (!DRIVE_ID_RE.test(direct)) throw new Error(`INBOUND_CHECK_BACK_LABEL_FOLDER_ID が Drive ID の形式ではありません: ${direct}`);
    return direct;
  }
  if (folderCache && Date.now() - folderCache.at < FOLDER_CACHE_MS) return folderCache.id;
  const parent = imageFolderParentId();
  const list = await drive.files.list({
    q: `'${esc(parent)}' in parents and name = '${esc(BACK_LABEL_FOLDER_NAME)}'`
      + ` and mimeType = 'application/vnd.google-apps.folder' and trashed = false`,
    fields: 'files(id, name), incompleteSearch',
    pageSize: 5,
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
  }, { timeout: DRIVE_META_TIMEOUT_MS });
  // ⚠検索が不完全なまま「無い」と判断すると同じフォルダを二度作る。作らずに次の回へ回す
  if (list.data.incompleteSearch) throw new Error('Drive検索が不完全 (incompleteSearch)。フォルダの二重作成を避けるため中止しました');
  const hit = (list.data.files || [])[0];
  if (hit) {
    folderCache = { id: hit.id, at: Date.now() };
    return hit.id;
  }
  const made = await drive.files.create({
    requestBody: { name: BACK_LABEL_FOLDER_NAME, mimeType: 'application/vnd.google-apps.folder', parents: [parent] },
    fields: 'id',
    supportsAllDrives: true,
  }, { timeout: DRIVE_META_TIMEOUT_MS });
  folderCache = { id: made.data.id, at: Date.now() };
  console.log(`[inbound-check] 裏面ラベルの保存先フォルダを作りました: ${BACK_LABEL_FOLDER_NAME} (${made.data.id})`);
  return made.data.id;
}

/**
 * ファイル名。人が Drive で探すので **商品コードを先頭**に置く (商品コードで並べば同じ商品が固まる)。
 * 個人名は入れない (いろはの写真と同じ規則)。
 */
export function filenameFor(r) {
  const code = String(r.product_id || r.code_key || 'nocode').replace(/[^A-Za-z0-9_.-]/g, '') || 'nocode';
  const ts = String(r.created_at).replace(/[-:TZ.]/g, '').slice(0, 14);
  const ext = r.mime === 'image/png' ? 'png' : 'jpg';
  return `${code}_裏面_${ts}_${String(r.operation_id).slice(0, 8)}.${ext}`;
}

/**
 * 実装差し替え可能な Drive アップロード (テストでモックする)。
 * ⭐冪等: operation_id を appProperties に入れ、作成の**前に**同じ ID のファイルを探して回収する。
 *   「作成は届いたが応答が消えた」再試行で同じ写真が2つできない
 */
/**
 * 送信IDで Drive の既存ファイルを探す (実装差し替え可能)。
 * アップロードの冪等化と、実体を失った行の回収 (Codex R2 #2) の両方で使う。
 * @returns {Promise<{fileId, url}|null>} 見つからなければ null。**聞けなかったときは throw**
 *   (「無い」と「聞けなかった」を混ぜない — 混ぜると Drive にある写真を捨てさせてしまう)
 */
async function driveFindReal({ operationId }) {
  const drive = getDriveClient();
  const folderId = await ensureFolderId(drive);
  let driveId = null;
  try {
    const meta = await drive.files.get({ fileId: folderId, fields: 'id, driveId', supportsAllDrives: true }, { timeout: DRIVE_META_TIMEOUT_MS });
    driveId = meta.data.driveId || null;
  } catch (e) {
    folderCache = null;   // 消された・権限が変わった可能性。次の回は探し直す
    throw new Error(`保存先フォルダにアクセスできません。共有ドライブにサービスアカウントを「コンテンツ管理者」で追加してください (${e.message})`);
  }
  const list = await drive.files.list({
    q: `appProperties has { key='ic_back_label_op' and value='${esc(operationId)}' } and '${esc(folderId)}' in parents and trashed = false`,
    fields: 'files(id, webViewLink), incompleteSearch',
    pageSize: 5,
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
    ...(driveId ? { corpora: 'drive', driveId } : {}),
  }, { timeout: DRIVE_META_TIMEOUT_MS });
  if (list.data.incompleteSearch) throw new Error('Drive検索が不完全 (incompleteSearch)。重複防止のため中止しました');
  const hit = (list.data.files || [])[0];
  return hit ? { fileId: hit.id, url: hit.webViewLink || `https://drive.google.com/file/d/${hit.id}/view` } : null;
}
let driveFindImpl = driveFindReal;
export function _setDriveFind(fn) { driveFindImpl = fn || driveFindReal; }

async function driveUploadReal({ localPath, filename, mime, operationId }) {
  // ⭐作成の**前に**同じ送信IDを探して回収する (応答が消えた再試行で二重に作らない)
  const hit = await driveFindImpl({ operationId });
  if (hit) return hit;
  const drive = getDriveClient();
  const folderId = await ensureFolderId(drive);
  const res = await drive.files.create({
    requestBody: { name: filename, parents: [folderId], appProperties: { ic_back_label_op: String(operationId) } },
    media: { mimeType: mime || 'image/jpeg', body: fs.createReadStream(localPath) },
    fields: 'id, webViewLink',
    supportsAllDrives: true,
  }, { timeout: DRIVE_TIMEOUT_MS });
  return { fileId: res.data.id, url: res.data.webViewLink || `https://drive.google.com/file/d/${res.data.id}/view` };
}
let driveUploadImpl = driveUploadReal;
export function _setDriveUpload(fn) { driveUploadImpl = fn || driveUploadReal; }

/** Drive からの取り出し (配信 API 用) */
async function driveDownloadReal({ fileId }) {
  const drive = getDriveClient();
  const res = await drive.files.get(
    { fileId, alt: 'media', supportsAllDrives: true },
    { responseType: 'stream', timeout: 60_000 },
  );
  return { status: res.status, stream: res.data, contentType: res.headers?.['content-type'] || null };
}
let driveDownloadImpl = driveDownloadReal;
export function _setDriveDownload(fn) { driveDownloadImpl = fn || driveDownloadReal; }
export function driveDownload(args) { return driveDownloadImpl(args); }

// ─── 後片づけ ───

/**
 * どの行からも指されていない実体を消す。**十分に古いものだけ**を見る (送信中を巻き込まない)。
 * multer の一時領域に残ったファイルもここで片づける。
 */
export function sweepOrphanFiles(maxAgeMs = 24 * 3600 * 1000) {
  const db = getDB();
  const limit = Date.now() - maxAgeMs;
  let removed = 0;
  for (const dir of [TMP_DIR, MEDIA_DIR]) {
    let names = [];
    try { names = fs.readdirSync(dir); } catch { continue; }
    for (const name of names) {
      const full = path.join(dir, name);
      let st = null;
      try { st = fs.statSync(full); } catch { continue; }
      if (!st.isFile() || st.mtimeMs >= limit) continue;
      if (dir === MEDIA_DIR) {
        const opId = name.split('.')[0];
        const row = db.prepare('SELECT local_path, deleted_at FROM f_inbound_check_back_labels WHERE operation_id = ?').get(opId);
        // まだ生きている行が指している実体には触らない
        if (row && !row.deleted_at && row.local_path && path.resolve(row.local_path) === path.resolve(full)) continue;
      }
      try { fs.unlinkSync(full); removed++; } catch { /* 消せなければ次の回に */ }
    }
  }
  if (removed > 0) console.log(`[inbound-check] 裏面ラベルの行き場のない実体を ${removed} 件片づけました`);
  return { removed };
}

/**
 * 実体を失った写真の後始末 (Codex R1 #2 / R2 #2)。
 *
 * ローカルの実体が無い `stored` の行を、Drive に聞いてから仕分ける:
 *   Drive にある   → 実は上がっていた (作成は届いたが応答が消えた) → uploaded として拾い直す
 *   Drive にも無い → 見られない写真。印を付けて枚数・ゲートから外す (撮り直してもらう)
 *   Drive に聞けない → **「無い」と決めつけない**。失敗として次の回に持ち越す
 *
 * ⚠印が付いた行も毎回見る。確認ゲートの直前検査 (verifyStoredPhotos) は Drive に聞けないので
 *   保守的に印を付けるが、実は Drive にあったならここで拾い直す。
 * ⚠Drive へ上げ終わった行 (status='uploaded') は local_path が無くて当然なので触らない。
 */
async function reconcileMissingFiles(db) {
  const rows = db.prepare(`SELECT * FROM f_inbound_check_back_labels
    WHERE status = 'stored' AND deleted_at IS NULL`).all();
  const stats = { missing: 0, recovered: 0, failed: 0 };
  for (const r of rows) {
    if (r.local_path && fs.existsSync(r.local_path)) continue;   // 実体がある = 送信待ちのまま
    if (!isDriveConfigured()) {
      // Drive に出していないので回収先が無い。見られない写真として扱う
      if (!r.missing_file_at) { markMissing(db, r.id); stats.missing++; }
      continue;
    }
    let found = null;
    try {
      found = await driveFindImpl({ operationId: r.operation_id });
    } catch (e) {
      // 聞けなかっただけ。印は付けない (付いていれば残す) — 次の回に持ち越す
      markFail(db, r, `Drive に確認できませんでした (${e.message})`);
      stats.failed++;
      continue;
    }
    if (found) {
      db.prepare(`UPDATE f_inbound_check_back_labels
        SET status = 'uploaded', drive_file_id = ?, drive_url = ?, uploaded_at = COALESCE(uploaded_at, ?),
            local_path = NULL, missing_file_at = NULL, error = NULL, next_retry_at = NULL, attempt_count = 0
        WHERE id = ?`).run(found.fileId, found.url, utcNow(), r.id);
      stats.recovered++;
      continue;
    }
    if (!r.missing_file_at) { markMissing(db, r.id); stats.missing++; }
  }
  if (stats.missing > 0) console.error(`[inbound-check] 裏面ラベル ${stats.missing} 枚の実体が見つかりません (撮り直しが要ります)`);
  if (stats.recovered > 0) console.log(`[inbound-check] 裏面ラベル ${stats.recovered} 枚を Drive から拾い直しました`);
  return stats;
}

function markMissing(db, id) {
  db.prepare(`UPDATE f_inbound_check_back_labels
    SET missing_file_at = ?, next_retry_at = ?, error = ? WHERE id = ?`)
    .run(utcNow(), BLOCKED_UNTIL, '実体ファイルがありません (再起動で消えた可能性。撮り直してください)', Number(id));
}

function markFail(db, r, message) {
  const attempts = (r.attempt_count || 0) + 1;
  const retryAt = attempts >= MAX_ATTEMPTS ? BLOCKED_UNTIL
    : new Date(Date.now() + RETRY_BASE_MS * attempts).toISOString();
  db.prepare('UPDATE f_inbound_check_back_labels SET error = ?, attempt_count = ?, next_retry_at = ? WHERE id = ?')
    .run(String(message).slice(0, ERROR_MAX_LEN), attempts, retryAt, r.id);
  if (attempts >= MAX_ATTEMPTS) {
    console.error(`[inbound-check] 裏面ラベル #${r.id} を${MAX_ATTEMPTS}回失敗で停止 (管理画面の再実行待ち): ${message}`);
  }
}

// ─── キュー (単一プロセス前提の直列ワーカー) ───

let running = false;
let timer = null;

export async function processBackLabelQueue() {
  if (running) return { ok: true, skipped: true };
  running = true;
  const stats = { uploaded: 0, failed: 0, missing: 0, recovered: 0 };
  try {
    // ⚠getDB() は try の中で呼ぶ。外で投げると running が立ったままになり、
    //   以後の定期実行も手動再実行も全部 skipped になる (Codex R1 #9)
    const db = getDB();
    sweepOrphanFiles();
    // 実体を失った行を仕分ける (Drive にあれば拾い直し、無ければ撮り直しの印)。
    //   Drive 未設定でもここは行う — 見られない写真で「撮ってある」を満たさないため
    const rec = await reconcileMissingFiles(db);
    stats.missing = rec.missing;
    stats.recovered = rec.recovered;
    stats.failed += rec.failed;
    if (!isDriveConfigured()) return { ok: true, ...stats, disabled: true };
    const now = utcNow();
    const rows = db.prepare(`SELECT * FROM f_inbound_check_back_labels
      WHERE status = 'stored' AND ${ALIVE} AND (next_retry_at IS NULL OR next_retry_at <= ?)
      ORDER BY id LIMIT 20`).all(now);
    for (const r of rows) {
      try {
        if (!r.local_path || !fs.existsSync(r.local_path)) {
          // 直前の reconcile で仕分けたはず。取りこぼしても再試行しない (送るものが無い)
          continue;
        }
        const { fileId, url } = await driveUploadImpl({
          localPath: r.local_path, filename: filenameFor(r), mime: r.mime, operationId: r.operation_id,
        });
        db.prepare(`UPDATE f_inbound_check_back_labels
          SET status = 'uploaded', drive_file_id = ?, drive_url = ?, uploaded_at = ?, error = NULL, next_retry_at = NULL
          WHERE id = ?`).run(fileId, url, utcNow(), r.id);
        // Drive に届いてからローカルを消す。消せなくても実害はない (次の sweep が拾う)
        try { fs.unlinkSync(r.local_path); } catch { /* 無視 */ }
        db.prepare('UPDATE f_inbound_check_back_labels SET local_path = NULL WHERE id = ?').run(r.id);
        stats.uploaded++;
      } catch (e) {
        markFail(db, r, e.message);
        stats.failed++;
      }
    }
  } finally {
    running = false;
  }
  return { ok: true, ...stats };
}

/** すぐ1回まわす (受信の直後に呼ぶ)。失敗はキューの再試行に任せる */
export function schedule() {
  setImmediate(() => { processBackLabelQueue().catch((e) => console.error('[inbound-check] 裏面ラベル queue error', e)); });
}

/** 2分おきの再試行ワーカー (プロセス内。picking の画像キューと同じ扱いで台帳対象の cron ではない) */
export function startBackLabelWorker() {
  if (timer) return;
  // ⭐起動直後に1回まわす。再起動で DATA_DIR の実体が消えていたら、その写真を
  //   「見られない」と印を付けて撮り直してもらう (2分待つ間、死んだ写真で確認が通らないように)
  schedule();
  timer = setInterval(() => {
    processBackLabelQueue().catch((e) => console.error('[inbound-check] 裏面ラベル queue error', e));
  }, 2 * 60 * 1000);
  timer.unref?.();
  console.log('[inbound-check] 裏面ラベルの Drive 送信ワーカー起動 (2分間隔で再試行)');
}

// ─── 管理画面 ───

/** 送信待ち・失敗の様子 (管理画面に出す) */
export function backLabelStatus() {
  const db = getDB();
  const agg = db.prepare(`SELECT
      COUNT(*) AS total,
      SUM(CASE WHEN ${ALIVE} AND status = 'stored' THEN 1 ELSE 0 END) AS pending,
      SUM(CASE WHEN ${ALIVE} AND status = 'uploaded' THEN 1 ELSE 0 END) AS uploaded,
      SUM(CASE WHEN ${ALIVE} AND status = 'stored' AND next_retry_at = ? THEN 1 ELSE 0 END) AS blocked,
      SUM(CASE WHEN deleted_at IS NULL AND missing_file_at IS NOT NULL THEN 1 ELSE 0 END) AS missing
    FROM f_inbound_check_back_labels`).get(BLOCKED_UNTIL);
  const failing = db.prepare(`SELECT id, product_id, error, attempt_count, next_retry_at, created_at
    FROM f_inbound_check_back_labels
    WHERE ${ALIVE} AND status = 'stored' AND error IS NOT NULL
    ORDER BY id DESC LIMIT 20`).all();
  return {
    required: isBackLabelRequired(),
    drive_configured: isDriveConfigured(),
    folder_name: BACK_LABEL_FOLDER_NAME,
    total: agg.total || 0,
    pending: agg.pending || 0,
    uploaded: agg.uploaded || 0,
    blocked: agg.blocked || 0,
    // 実体を失って撮り直しが要る写真 (Drive にも届いていない)。管理画面で気づけるように数える
    missing: agg.missing || 0,
    failing,
  };
}

/** 管理画面の「もう一度送る」: 止まった行の再試行を解除して即キュー */
export function resetBackLabelQueue(id = null) {
  const db = getDB();
  // ⚠実体を失った行は解除しない — 送るものが無いので、解除すると10回失敗してまた止まるだけ
  const n = id == null
    ? db.prepare(`UPDATE f_inbound_check_back_labels SET next_retry_at = NULL, attempt_count = 0, error = NULL
        WHERE status = 'stored' AND ${ALIVE}`).run().changes
    : db.prepare(`UPDATE f_inbound_check_back_labels SET next_retry_at = NULL, attempt_count = 0, error = NULL
        WHERE id = ? AND status = 'stored' AND ${ALIVE}`).run(Number(id)).changes;
  if (n > 0) schedule();
  return n;
}
