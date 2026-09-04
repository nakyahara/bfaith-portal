/**
 * いろは在庫化作業アプリ — 完成写真・動画 (Drive 保存 + Notion 反映)
 *
 * 流れ (要件定義 §6 / §1.7 ② operation_id 付き outbox):
 *   ①iPad から受信 → 検証 (種類・サイズ・マジックバイト・上限枚数) → DATA_DIR に実体保存 +
 *     f_iroha_card_media に status='stored' で記録 → **即応答** (Drive を待たせない)
 *   ②裏のキューが Drive へアップロード (成功で status='uploaded'、実体ファイルは削除)
 *   ③カードの「完成写真」(files プロパティ) を貼り直す (成功で status='synced') — Notion 正本のカード (page_id) だけ。
 *     アプリ正本のカード (task_id) は Notion に貼る先が無いので 'uploaded' が最終状態 (同期キューには入れない)
 *   失敗は next_retry_at で再試行。同じ operation_id の再送は既存行を返す (二重登録しない)。
 *   Notion 反映の失敗は現場操作の失敗にしない (アップロード済みならいつでも貼り直せる)。
 *
 * Drive は既存サービスアカウント (GOOGLE_SERVICE_ACCOUNT_KEY) + 共有ドライブのフォルダ
 * (IROHA_WORK_DRIVE_FOLDER_ID。SA をコンテンツ管理者で追加しておく — fba-replenishment と同じ制約)。
 * 公開リンクにはしない (フォルダの共有範囲のまま)。
 */
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { google } from 'googleapis';
import { getDB, sourceOfTruth } from './db.js';
import { notionRequest, isNotionConfigured } from '../inbound-check/notion.js';
import { codeKeyOf } from '../inbound-check/work-master.js';

export const MAX_PHOTOS = 3;
// 動画は当面なし (中原さん 2026-09-03: iPad でうまく再生できなかった)。0 = 新しく受け付けない。
// 既に保存された動画の行とドライブのファイルはそのまま残す (画面に出さないだけ)
export const MAX_VIDEOS = 0;
export const MAX_PHOTO_BYTES = 8 * 1024 * 1024;     // canvas縮小後は通常1MB未満。余裕をみて8MB
// 動画を再開するときの受け取り上限 (いまは MAX_VIDEOS = 0 なので使っていない。戻すときの値として残す)
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

/**
 * 実体の先頭バイトで種類を確かめる (拡張子・Content-Type は自己申告なので信じない)。
 * ftyp は HEIC/HEIF/AVIF (静止画コンテナ) にもあるため、brand で動画系だけを通す (Codex PR3 #6)
 */
const VIDEO_BRANDS = new Set(['isom', 'iso2', 'iso4', 'iso5', 'iso6', 'mp41', 'mp42', 'avc1', 'qt  ', 'M4V ', '3gp4', '3gp5', 'dash']);
export function sniffKind(buf) {
  if (!buf || buf.length < 12) return null;
  if (buf[0] === 0xFF && buf[1] === 0xD8 && buf[2] === 0xFF) return 'photo';           // JPEG
  if (buf.slice(4, 8).toString('latin1') === 'ftyp') {
    const brand = buf.slice(8, 12).toString('latin1');
    if (VIDEO_BRANDS.has(brand)) return 'video';                                        // MP4 / MOV / 3GP
    return null;                                                                        // HEIC/HEIF/AVIF 等は拒否
  }
  return null;
}

/** カードの参照: アプリ正本は task_id、Notion 正本は page_id (どちらかで数える・引く) */
const refWhere = ({ taskId = null, pageId = null }) => (taskId != null
  ? { sql: 'task_id = ?', arg: Number(taskId) }
  : { sql: 'page_id = ?', arg: pageId });

export function countActiveMedia(ref, kind) {
  const w = refWhere(typeof ref === 'object' && ref !== null ? ref : { pageId: ref });
  // 送信中の行 (staged_at が新しい) は枠として数える — 並行送信で上限を超えないため。
  // 置き去りになった古い行は数えない (sweep が片づけるまで枠を食いつぶさない) — Codex PR1 R8 / R9
  const fresh = new Date(Date.now() - STAGE_TTL_MS).toISOString();
  return getDB().prepare(`SELECT COUNT(*) c FROM f_iroha_card_media
    WHERE ${w.sql} AND kind = ? AND deleted_at IS NULL AND (staged_at IS NULL OR staged_at >= ?)`)
    .get(w.arg, kind, fresh).c;
}

/** 1行 (配信 API 用。削除済みも返すので呼び元で deleted_at を見る) */
export function getMediaRow(id) {
  return getDB().prepare('SELECT * FROM f_iroha_card_media WHERE id = ?').get(Number(id)) || null;
}

/**
 * Drive 側で消えた (404/410) 写真に印を付け、表示と「前回の完成形」候補から外す (Codex R1 #5)。
 * 一時的な失敗 (5xx) では呼ばない。管理画面の「再実行」(resetMedia) で解除できる
 */
/**
 * 「ドライブから消えていた」の報告を溜める。⭐配信 (GET) は DB を変えない —
 * 読むだけの画面 (下見・履歴) で写真を開いただけで DB が変わらないようにするため (Codex PR1 R9)。
 * 実際に印を付けるのは送信キューの回 (processMediaQueue)
 */
const pendingUnavailable = new Map();
export function reportMediaUnavailable(id, reason, driveFileId = null) {
  pendingUnavailable.set(Number(id), { reason: String(reason || 'unavailable'), driveFileId });
  schedule();
}
/**
 * 溜まった報告を反映する (キューの回から呼ぶ)。⭐報告したときと同じ実体 (drive_file_id) の行にだけ
 * 印を付ける — 報告から反映までの間に管理画面で送り直されていたら、新しい実体に古い 404 を貼らない (Codex PR1 R10)
 */
export function flushUnavailableReports() {
  if (pendingUnavailable.size === 0) return 0;
  const entries = [...pendingUnavailable.entries()];
  pendingUnavailable.clear();
  const db = getDB();
  let n = 0;
  for (const [id, { reason, driveFileId }] of entries) {
    if (driveFileId != null) {
      const cur = db.prepare('SELECT drive_file_id FROM f_iroha_card_media WHERE id = ?').get(id);
      if (!cur || cur.drive_file_id !== driveFileId) continue;   // 送り直された = 古い報告は捨てる
    }
    if (markMediaUnavailable(id, reason)) n++;
  }
  return n;
}

export function markMediaUnavailable(id, reason) {
  return getDB().prepare(`UPDATE f_iroha_card_media SET unavailable_at = ?, error = ? WHERE id = ? AND unavailable_at IS NULL`)
    .run(utcNow(), String(reason || 'unavailable').slice(0, 300), Number(id)).changes > 0;
}

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
/**
 * 受け取ったファイルの検査 (DB を触らない)。トランザクションに入る前にここまで済ませておくと、
 * SQLite の書き込みロックを持ったままファイルを読まずに済む (この DB は miniPC も開く — Codex PR1 R6)
 */
export function inspectMediaUpload({ kind, filePath, operationId }) {
  const opId = String(operationId || '').trim();
  if (!/^[A-Za-z0-9_-]{8,64}$/.test(opId)) return { ok: false, error: 'bad_request', message: 'operation_id が不正です' };
  // 種類の判定は**いちばん先**に (ファイルを見る前・再送の照合より前)。
  // 「動画は必ず video_disabled」という約束にする — 壊れたファイルや再送で別の答えが返らないように (Codex R1)
  if (kind === 'video') return { ok: false, error: 'video_disabled', message: '動画は今つかえません (写真をとってください)' };
  if (kind !== 'photo') return { ok: false, error: 'bad_request', message: '種類が不正です (写真だけ受け付けます)' };
  const size = fs.statSync(filePath).size;
  const sniffed = sniffKind(readHead(filePath));
  if (sniffed !== kind) {
    return { ok: false, error: 'bad_file', message: kind === 'photo' ? '写真 (JPEG) を送ってください' : '動画 (MP4/MOV) を送ってください' };
  }
  const cap = kind === 'photo' ? MAX_PHOTO_BYTES : MAX_VIDEO_BYTES;
  if (size > cap) {
    return { ok: false, error: 'too_large', message: `ファイルが大きすぎます (上限 ${Math.round(cap / 1024 / 1024)}MB)` };
  }
  return { ok: true, opId, size };
}

/**
 * 実体を置く前の行の状態。⭐ここに居る行は「まだ無いもの」として扱う —
 * 一覧にも出さず、Drive への送信キューも拾わない (キューは status='stored' だけを見る)。
 * commit の後に実体を置いてから promoteStagedMedia で 'stored' に上げる (Codex PR1 R7)
 */
export const MEDIA_STAGING = 'staging';   // 印は staged_at 列。status の取りうる値は増やさない

/**
 * 保管場所での置き場所。⭐札 (claim) ごとに別のファイル名にする —
 * 同じ operation_id の二重送信が同じパスを奪い合うと、負けた側が勝った側の実体を消してしまう (Codex PR1 R9)
 */
export function stagedPathOf(opId, kind, claim) {
  const ext = kind === 'photo' ? 'jpg' : 'mp4';
  return path.join(MEDIA_DIR, claim ? `${opId}.${claim}.${ext}` : `${opId}.${ext}`);
}

/** 置き去りの staging 行とみなすまでの時間。ここを過ぎた行は sweep が片づけ、枚数上限にも数えない */
export const STAGE_TTL_MS = 30 * 60 * 1000;

/**
 * その写真の持ち主のカードに、**いま**書き込めるか (Codex PR1 R10)。
 * 公開はトランザクションの外 (ファイルを置いた後) に来るので、確かめたときから
 * カードが終了したり正本が変わったりし得る。公開の瞬間にもここを通す。
 */
function cardWritableFor(row) {
  const db = getDB();
  const app = sourceOfTruth() === 'app';
  if (row.task_id != null) {
    if (!app) return false;   // 正本が Notion に戻っていれば、tasks のカードは下見 = 読むだけ
    const t = db.prepare('SELECT status FROM f_iroha_tasks WHERE id = ?').get(row.task_id);
    return !!t && t.status !== 'closed';
  }
  if (app) return false;      // 正本がアプリなら、Notion のカードにはもう書かない
  return !!db.prepare('SELECT page_id FROM f_iroha_app_notion_cache WHERE page_id = ?').get(row.page_id);
}

/**
 * 実体を置き終えた staging 行を公開する。⭐札 (claim) と staged_at の両方が、
 * 自分が見たときのままである要求だけが上げられる (Codex PR1 R8 / R10)。
 * 公開の瞬間に「そのカードにいま書けるか」も確かめる (置いている間に終了・正本切替が起き得る)。
 * @returns {{ok:true, media}} | {{ok:false, reason:'taken'|'gone'|'no_file'|'not_writable', media?}}
 */
export function promoteStagedMedia(id, localPath, { claim = null, stagedAt = null } = {}) {
  const db = getDB();
  // 実体が無いまま公開しない (置けたつもりで DB だけ進めない — Codex PR1 R9)
  if (!fs.existsSync(localPath)) return { ok: false, reason: 'no_file' };
  return db.transaction(() => {
    const cur = db.prepare('SELECT * FROM f_iroha_card_media WHERE id = ?').get(Number(id));
    if (!cur) return { ok: false, reason: 'gone' };
    // 別の要求が先に置いて公開した (同じ operation_id の二重送信)。写真としては成立しているので、
    // その行をそのまま返す。実体は相手が置いたものが正 — こちらから消さない
    if (!cur.staged_at) return { ok: false, reason: 'taken', media: publicMedia(cur) };
    if (!cardWritableFor(cur)) return { ok: false, reason: 'not_writable' };
    const n = db.prepare(`UPDATE f_iroha_card_media SET staged_at = NULL, staged_claim = NULL, local_path = ?
      WHERE id = ? AND staged_at IS NOT NULL AND staged_claim IS ?
        ${stagedAt == null ? '' : 'AND staged_at = ?'}`)
      .run(...[localPath, Number(id), claim].concat(stagedAt == null ? [] : [stagedAt])).changes;
    if (n === 0) return { ok: false, reason: 'taken' };
    return { ok: true, media: publicMedia(db.prepare('SELECT * FROM f_iroha_card_media WHERE id = ?').get(Number(id))) };
  }).immediate();
}

/** 一時ファイルを保管場所へ移す。deferMove で呼んだ側が、トランザクションを抜けてから呼ぶ */
export function moveStoredFile(move) {
  if (!move) return;
  fs.mkdirSync(MEDIA_DIR, { recursive: true });
  fs.renameSync(move.from, move.to);
}

/**
 * 実体を置けなかった行を消す (行だけ残して「実体のない写真」を作らない — Codex PR1 R6)。
 * ⭐札を渡したときは「まだ公開されておらず、自分が札を持っている」ときだけ消す。
 * 二重送信の負けた側が、相手が公開した行まで消さないため (Codex PR1 R8)
 */
export function dropMedia(id, claim = null) {
  if (claim == null) return getDB().prepare('DELETE FROM f_iroha_card_media WHERE id = ?').run(Number(id)).changes;
  return getDB().prepare('DELETE FROM f_iroha_card_media WHERE id = ? AND staged_at IS NOT NULL AND staged_claim IS ?')
    .run(Number(id), claim).changes;
}

/**
 * 置き去りの staging 行を片づける (Codex PR1 R8)。iPad の再読込・端末再起動で再送が来なくなると、
 * 行だけが残って写真の枚数上限を食いつぶす。実体が置けているものは公開し、無いものは消す。
 * @param maxAgeMs これより古い staging 行だけを見る (送信中のものを巻き込まない)
 */
export function sweepStagedMedia(maxAgeMs = STAGE_TTL_MS) {
  const db = getDB();
  const limit = new Date(Date.now() - maxAgeMs).toISOString();
  const rows = db.prepare(`SELECT id, operation_id, kind, page_id, task_id, staged_claim, staged_at
    FROM f_iroha_card_media WHERE staged_at IS NOT NULL AND staged_at < ?`).all(limit);
  let promoted = 0;
  let dropped = 0;
  // ⭐自分が見たときの札・時刻のままの行しか動かさない。見た後に同じ端末から再送されていたら、
  //   それは新しい要求のものなので触らない (Codex PR1 R10)
  const dropIfSame = (r) => db.prepare('DELETE FROM f_iroha_card_media WHERE id = ? AND staged_claim IS ? AND staged_at = ?')
    .run(r.id, r.staged_claim, r.staged_at).changes > 0;
  for (const r of rows) {
    const p = stagedPathOf(r.operation_id, r.kind, r.staged_claim);
    if (!fs.existsSync(p)) {
      if (dropIfSame(r)) dropped++;
      continue;
    }
    // 実体は置けたが公開の前に落ちた → 写真は無事なので公開する。ただし、その間に新しい写真で
    // 枠が埋まっていたら公開しない (後から上限を超えない — Codex PR1 R10)
    const max = r.kind === 'photo' ? MAX_PHOTOS : MAX_VIDEOS;
    const room = countActiveMedia({ pageId: r.page_id, taskId: r.task_id }, r.kind) < max;
    if (room && promoteStagedMedia(r.id, p, { claim: r.staged_claim, stagedAt: r.staged_at }).ok) { promoted++; continue; }
    if (dropIfSame(r)) { dropped++; try { fs.unlinkSync(p); } catch { /* 無ければよい */ } }
  }
  if (promoted > 0 || dropped > 0) console.log(`[iroha-work] 置き去りの写真を片づけました 公開=${promoted} 破棄=${dropped}`);
  return { promoted, dropped };
}

/** staging 行を今回の送信で置き直すときの更新 (実体を差し替えるので中身の情報も今回のものにする) */
const STAGED_RESEND_SQL = `UPDATE f_iroha_card_media
  SET delete_token_hash = ?, staged_claim = ?, staged_at = ?, size = ?, mime = ?,
      worker_id = ?, worker_name = ?, device_label = ?
  WHERE id = ?`;

export function addMedia({ pageId = null, taskId = null, productCode = null, kind, mime, filePath, worker, deviceLabel = null, deviceId = null, operationId, inspected = null, deferMove = false }) {
  const ins = inspected && inspected.ok ? inspected : inspectMediaUpload({ kind, filePath, operationId });
  if (!ins.ok) return ins;
  const { opId, size } = ins;
  if (pageId != null && taskId != null) return { ok: false, error: 'bad_request', message: 'カードの指定が二重です (page_id と task_id の両方)' };
  if (pageId == null && taskId == null) return { ok: false, error: 'bad_request', message: 'カードが指定されていません' };
  const db = getDB();
  const dup = db.prepare('SELECT * FROM f_iroha_card_media WHERE operation_id = ?').get(opId);
  if (dup) {
    // 再送は「同じカード」のときだけ既存行を返す。別カード (切替前の Notion カードを含む) の operation_id なら
    // その写真を今のカードの成功として返さない (Codex A1b R1 #3)
    const sameCard = taskId != null
      ? (Number(dup.task_id) === Number(taskId) && dup.page_id == null)
      : (dup.page_id === pageId && dup.task_id == null);
    if (!sameCard) return { ok: false, error: 'operation_conflict', message: 'この送信は別のカードで使われています (撮り直してください)' };
    // 実体を置く前に落ちた行 (staging) は、今回送られてきたファイルで置き直して復旧させる。
    // これをしないと「DB にだけ残った写真」が、再送のたびに実体なしで成功扱いになる (Codex PR1 R7)。
    // ⭐撮った端末からの再送だけ。実体を差し替えるので、大きさ・形式・撮った人も今回のもので更新する。
    //   新しい札 (claim) を立て、これを持つ要求だけが公開・後始末をする (Codex PR1 R8)
    const sameDevice = deviceId != null && dup.uploader_device_id === Number(deviceId);
    if (dup.staged_at && !dup.deleted_at && deferMove && sameDevice) {
      const token = crypto.randomBytes(16).toString('base64url');
      const claim = crypto.randomBytes(12).toString('base64url');
      db.prepare(STAGED_RESEND_SQL)
        .run(crypto.createHash('sha256').update(token).digest('hex'), claim, utcNow(), size, mime || null,
          worker.id, worker.display_name, deviceLabel, dup.id);
      const fresh = db.prepare('SELECT * FROM f_iroha_card_media WHERE id = ?').get(dup.id);
      return { ok: true, already: true, media: publicMedia(fresh), deleteToken: token, claim,
        move: { from: filePath, to: stagedPathOf(opId, dup.kind, claim) } };
    }
    if (dup.staged_at) {
      // まだ置けていない行がある。別端末からの同 operation_id はここで止める (乗っ取り防止)
      return { ok: false, error: 'not_ready', message: 'まだ保存中です。少し待ってから撮り直してください' };
    }
    // 応答消失→再送 (Codex PR3-R3): 削除トークンを持てていないので、**同じ端末からの再送に
    // 限って**発行し直す (古いトークンは無効になる)。別端末からの同 operation_id には返さない
    if (deviceId != null && dup.uploader_device_id === Number(deviceId) && !dup.deleted_at) {
      const token = crypto.randomBytes(16).toString('base64url');
      // ⭐直前のトークンも 1 つだけ生かしておく。同じ再送が二重に届くと、先に返したトークンが
      //   端末に届く前に無効になり、撮った本人が消せなくなる (Codex PR1 R10)
      db.prepare('UPDATE f_iroha_card_media SET delete_token_hash_prev = delete_token_hash, delete_token_hash = ? WHERE id = ?')
        .run(crypto.createHash('sha256').update(token).digest('hex'), dup.id);
      return { ok: true, already: true, media: publicMedia(dup), deleteToken: token };
    }
    return { ok: true, already: true, media: publicMedia(dup) };
  }
  const max = kind === 'photo' ? MAX_PHOTOS : MAX_VIDEOS;
  if (countActiveMedia({ pageId, taskId }, kind) >= max) {
    return { ok: false, error: 'cap_reached', message: `写真は${max}枚までです。不要な写真を削除してから撮り直してください` };
  }
  const claim = deferMove ? crypto.randomBytes(12).toString('base64url') : null;
  const localPath = stagedPathOf(opId, kind, claim);
  // deferMove のときは移さない。呼び出し側がトランザクションを抜けてから moveStoredFile を呼び、
  // 置けてから promoteStagedMedia で 'stored' に上げる (Codex PR1 R6 / R7)
  if (!deferMove) moveStoredFile({ from: filePath, to: localPath });
  // 削除トークン: アップロードした端末だけに一度だけ返す (worker_id は画面で選べる自己申告なので
  // 「撮った本人」の証明に使わない — Codex PR3 #2)。サーバーはハッシュのみ保存
  const deleteToken = crypto.randomBytes(16).toString('base64url');
  const info = db.prepare(`INSERT INTO f_iroha_card_media
    (operation_id, page_id, task_id, product_code, kind, mime, size, local_path, staged_at, staged_claim, status, worker_id, worker_name, device_label, uploader_device_id, created_at, delete_token_hash)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'stored', ?, ?, ?, ?, ?, ?)`)
    .run(opId, pageId, taskId == null ? null : Number(taskId), productCode, kind, mime || null, size,
      deferMove ? null : localPath, deferMove ? utcNow() : null, claim,
      worker.id, worker.display_name, deviceLabel, deviceId == null ? null : Number(deviceId), utcNow(),
      crypto.createHash('sha256').update(deleteToken).digest('hex'));
  const row = db.prepare('SELECT * FROM f_iroha_card_media WHERE id = ?').get(Number(info.lastInsertRowid));
  return { ok: true, media: publicMedia(row), deleteToken, claim, move: deferMove ? { from: filePath, to: localPath } : null };
}

/** 画面へ出す形 (ローカルパス等の内部情報は出さない) */
export function publicMedia(r) {
  return {
    id: r.id, kind: r.kind, status: r.status, url: r.drive_url || null,
    // 画面で表示できるか (Drive 保存済み、または送信待ちでローカル実体あり)。表示は /api/media/:id/file 経由。
    // Drive から消えた行・10回失敗で停止した行 (実体が残っている保証がない) は表示しない (Codex R1 #10)
    viewable: !r.unavailable_at && !!(r.drive_file_id || (r.local_path && r.next_retry_at !== BLOCKED_UNTIL)),
    unavailable: !!r.unavailable_at,
    worker_id: r.worker_id, worker_name: r.worker_name, created_at: r.created_at,
    error: r.next_retry_at === BLOCKED_UNTIL ? (r.error || '失敗') : null,   // 諦めた失敗だけ画面へ
  };
}

const LOST_LOCAL_MSG = '実体ファイルがありません (再起動で消えた可能性。撮り直してください)';

/** page_id → 有効なメディア一覧 (一覧・詳細表示用) */
export function mediaByPage(opts) { return mediaByCard('page_id', opts); }
/** 同上。アプリ正本のカード用に task_id で引く */
export function mediaByTask(opts) { return mediaByCard('task_id', opts); }

/**
 * @param ids     見るカードを絞る (null = 全部)。1 枚開くだけで全カードを走らないため (Codex PR1 R8)
 * @param repair  実体の消えた行に「失敗」の印を付けるか。⭐読むだけの画面 (下見・履歴) では false —
 *                開いただけで DB が変わらないようにする (Codex PR1 R8)
 */
function mediaByCard(key, { ids = null, repair = true } = {}) {
  const db = getDB();
  const map = new Map();
  // task_id は数値・page_id は文字列。取り違えると絞り込みが空になるので、キーに合わせて整える
  const only = Array.isArray(ids)
    ? (key === 'task_id'
      ? [...new Set(ids.map(Number).filter((n) => Number.isSafeInteger(n)))]
      : [...new Set(ids.map((v) => String(v)).filter((v) => v !== ''))])
    : null;
  if (only && only.length === 0) return map;
  // 実体をまだ置いていない行 (staged_at あり) は「無いもの」として扱う — 画面にも出さない (Codex PR1 R7)
  const rows = db.prepare(`SELECT * FROM f_iroha_card_media
    WHERE deleted_at IS NULL AND staged_at IS NULL AND ${key} IS NOT NULL
      ${only ? `AND ${key} IN (${only.map(() => '?').join(',')})` : ''} ORDER BY id`).all(...(only || []));
  const lost = db.prepare('UPDATE f_iroha_card_media SET next_retry_at = ?, error = ? WHERE id = ?');
  for (const r of rows) {
    // 送信待ちの実体が消えていたら (再起動で一時領域が飛んだ等)、キューの再試行 (最大10回) を待たずに
    // その場で停止扱いにする → 画面は「失敗」を出し、利用者は撮り直せる (Codex R2 #1)。
    // existsSync は送信待ち (stored) の行だけ = 通常 0〜数件。パスが空の行も「実体なし」(Codex R3)
    if (r.status === 'stored' && r.next_retry_at !== BLOCKED_UNTIL && (!r.local_path || !fs.existsSync(r.local_path))) {
      if (repair) lost.run(BLOCKED_UNTIL, LOST_LOCAL_MSG, r.id);
      r.next_retry_at = BLOCKED_UNTIL; r.error = LOST_LOCAL_MSG;   // 印を付けなくても画面には「失敗」を出す
    }
    if (!map.has(r[key])) map.set(r[key], []);
    map.get(r[key]).push(publicMedia(r));
  }
  return map;
}

// ─── HTTP 条件付きリクエストの小道具 (配信 API 用。純粋関数なのでここに置いてテストする) ───

/** If-None-Match: 複数・弱い ETag (W/)・* を受ける (弱い比較) */
export function etagMatches(header, etag) {
  if (!header) return false;
  return String(header).split(',').map(s => s.trim().replace(/^W\//, '')).some(t => t === '*' || t === etag);
}
/** If-Range: 単一の**強い** ETag だけ一致。W/ と * は不一致 = Range を無視して全体を返す (Codex R2 #2)。
 *  HTTP-date は更新日時を検証できないので不一致扱い */
export function ifRangeMatches(header, etag) {
  return String(header || '').trim() === etag;
}
/** Range は単一の bytes=a-b / a- / -n だけ Drive へ転送する。複数・逆転・不正は null (無視して全体を返す)
 *  — Drive の multipart/byteranges を素通しすると Content-Type が合わない (Codex R1 #1) */
export function singleRange(header) {
  const m = /^bytes=(\d*)-(\d*)$/.exec(String(header || '').trim());
  if (!m || (m[1] === '' && m[2] === '')) return null;
  if (m[1] !== '' && m[2] !== '' && Number(m[1]) > Number(m[2])) return null;
  return `bytes=${m[1]}-${m[2]}`;
}

/**
 * 商品コード (codeKey) → 「前回の完成形」候補 (Drive 保存済みの写真、新しい順)。
 * 写真は完了の証拠ではなく**次回同じ商品を作る人への見本** (中原さん 2026-09-03) —
 * 同じ商品コードのカードが次に上がってきたとき、詳細の一番上に出す。
 * ローカル実体だけの行 (stored) は再起動で消え得るので候補にしない。
 * card = カードの識別子 ('t'+task_id か page_id)。「自分のカードの写真」を除くのに使う
 * @returns Map<codeKey, Array<{id, page_id, task_id, card, worker_name, created_at}>>
 */
export function photosByCodeKey() {
  const map = new Map();
  const rows = getDB().prepare(`SELECT id, page_id, task_id, product_code, worker_name, created_at FROM f_iroha_card_media
    WHERE kind = 'photo' AND deleted_at IS NULL AND unavailable_at IS NULL
      AND drive_file_id IS NOT NULL AND product_code IS NOT NULL
    ORDER BY id DESC`).all();
  for (const r of rows) {
    const key = codeKeyOf(r.product_code);
    if (!key) continue;
    if (!map.has(key)) map.set(key, []);
    map.get(key).push({ id: r.id, page_id: r.page_id, task_id: r.task_id, card: cardKeyOf(r), worker_name: r.worker_name, created_at: r.created_at });
  }
  return map;
}
/** 写真・セッションの「どのカードか」を 1 つの文字列で表す (アプリ正本は t+task_id、Notion 時代は page_id) */
export const cardKeyOf = (r) => (r.task_id != null ? `t${r.task_id}` : r.page_id);

/**
 * 論理削除。本人確認は**削除トークン** (アップロードした端末だけが持つ) か portal セッション。
 * 貼り直しはページ単位キューへ積む — 最後の1件を消したときも「空にする」PATCH が飛ぶ (Codex PR3 #1)
 */
export function softDeleteMedia(id, { deleteToken = null, actor = null, isSession = false }) {
  const db = getDB();
  const row = db.prepare('SELECT * FROM f_iroha_card_media WHERE id = ?').get(Number(id));
  if (!row || row.deleted_at) return { ok: false, error: 'not_found', message: '写真・動画が見つかりません' };
  if (!isSession) {
    const hash = deleteToken ? crypto.createHash('sha256').update(String(deleteToken)).digest('hex') : null;
    // 直前のトークンも受ける (再送で配り直した直後の 1 世代だけ — Codex PR1 R10)
    if (!hash || (hash !== row.delete_token_hash && hash !== row.delete_token_hash_prev)) {
      return { ok: false, error: 'forbidden', message: '削除できるのは撮影した端末からだけです (職員はPCの管理画面から)' };
    }
  }
  db.prepare('UPDATE f_iroha_card_media SET deleted_at = ?, deleted_by = ? WHERE id = ?')
    .run(utcNow(), actor || 'device', row.id);
  if (row.local_path) { try { fs.unlinkSync(row.local_path); } catch { /* 送信済みなら無い */ } }
  if (row.page_id != null) { requestPageSync(row.page_id); schedule(); }   // アプリ正本の行は Notion に貼っていない
  return { ok: true };
}

/** Notion 貼り直しをページ単位で予約する (即時対象になる)。revision を進めて「処理中の古い
 *  取得内容で完了扱いされない」ようにする (Codex PR3-R2: PATCH中の削除要求が消える競合) */
export function requestPageSync(pageId) {
  if (pageId == null) return;   // アプリ正本のカードには Notion ページが無い (NULL を積むと永遠に失敗し続ける — Codex A1b R1 #8)
  getDB().prepare(`INSERT INTO f_iroha_media_page_sync (page_id, revision, requested_at, attempt_count, next_retry_at, error)
    VALUES (?, 1, ?, 0, NULL, NULL)
    ON CONFLICT(page_id) DO UPDATE SET
      revision = f_iroha_media_page_sync.revision + 1,
      requested_at = excluded.requested_at, attempt_count = 0, next_retry_at = NULL`)
    .run(pageId, utcNow());
}

/** 管理画面の「再実行」(送信側): ブロックを解除して即時キュー。「ドライブから消えた」印は
 *  recheckUnavailable (Drive で実在を確かめてから解除) が担当 — 未検証のまま候補へ戻さない (Codex R2 #3) */
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

/**
 * 実装差し替え可能な Drive アップロード (テストでモックする)。戻り値 {fileId, url}。
 * ⭐冪等化 (Codex PR3 #3): operation_id を appProperties に入れて作成し、**作成前に同じ
 *   operation_id のファイルを検索して回収**する。「作成は成功したが応答が消えた」再試行で
 *   共有ドライブに同じ写真が複数できない
 */
async function driveUploadReal({ localPath, filename, mime, operationId }) {
  const folderId = process.env.IROHA_WORK_DRIVE_FOLDER_ID;
  if (!folderId) throw new Error('IROHA_WORK_DRIVE_FOLDER_ID が未設定です');
  const drive = getDriveClient();
  const TIMEOUT = 120000;
  const esc = (s) => String(s).replace(/\\/g, '\\\\').replace(/'/g, "\\'");

  // 共有ドライブIDを特定して検索範囲を限定 (corpora=allDrives は incompleteSearch を返し得る — fba と同じ)
  let driveId = null;
  try {
    const meta = await drive.files.get({ fileId: folderId, fields: 'id, driveId', supportsAllDrives: true }, { timeout: 30000 });
    driveId = meta.data.driveId || null;
  } catch (e) {
    throw new Error(`保存先フォルダにアクセスできません。共有ドライブにサービスアカウントを「コンテンツ管理者」で追加してください (${e.message})`);
  }
  const list = await drive.files.list({
    q: `appProperties has { key='iroha_op' and value='${esc(operationId)}' } and '${esc(folderId)}' in parents and trashed = false`,
    fields: 'files(id, webViewLink), incompleteSearch',
    pageSize: 5,
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
    ...(driveId ? { corpora: 'drive', driveId } : {}),
  }, { timeout: 30000 });
  if (list.data.incompleteSearch) throw new Error('Drive検索が不完全 (incompleteSearch)。重複防止のため中止しました');
  const hit = (list.data.files || [])[0];
  if (hit) return { fileId: hit.id, url: hit.webViewLink || `https://drive.google.com/file/d/${hit.id}/view` };

  const res = await drive.files.create({
    requestBody: { name: filename, parents: [folderId], appProperties: { iroha_op: String(operationId) } },
    media: { mimeType: mime || 'application/octet-stream', body: fs.createReadStream(localPath) },
    fields: 'id, webViewLink',
    supportsAllDrives: true,
  }, { timeout: TIMEOUT });
  return { fileId: res.data.id, url: res.data.webViewLink || `https://drive.google.com/file/d/${res.data.id}/view` };
}

let driveUploadImpl = driveUploadReal;
export function _setDriveUpload(fn) { driveUploadImpl = fn || driveUploadReal; }

/**
 * Drive からの取り出し (配信 API 用)。Range をそのまま渡すと 206 + Content-Range が返る (動画のシーク)。
 * @returns {{status:number, stream:ReadableStream, contentType:string|null, contentLength:string|null, contentRange:string|null}}
 */
async function driveDownloadReal({ fileId, range = null }) {
  const drive = getDriveClient();
  const res = await drive.files.get(
    { fileId, alt: 'media', supportsAllDrives: true },
    { responseType: 'stream', timeout: 120000, headers: range ? { Range: range } : {} },
  );
  return {
    status: res.status, stream: res.data,
    contentType: res.headers?.['content-type'] || null,
    contentLength: res.headers?.['content-length'] || null,
    contentRange: res.headers?.['content-range'] || null,
  };
}
let driveDownloadImpl = driveDownloadReal;
export function _setDriveDownload(fn) { driveDownloadImpl = fn || driveDownloadReal; }
export function driveDownload(args) { return driveDownloadImpl(args); }

/** Drive にファイルが実在するか (メタデータだけ取る。ゴミ箱は「無い」扱い) */
async function driveExistsReal(fileId) {
  const drive = getDriveClient();
  const r = await drive.files.get({ fileId, fields: 'id, trashed', supportsAllDrives: true }, { timeout: 30000 });
  return !!r.data?.id && !r.data.trashed;
}
let driveExistsImpl = driveExistsReal;
export function _setDriveExists(fn) { driveExistsImpl = fn || driveExistsReal; }

/**
 * 「ドライブから消えた」印の解除 (管理画面の再実行)。Drive で実在を確かめてから解除する —
 * 未検証のまま表示・「前回の完成形」候補へ戻さない (Codex R2 #3)
 * @returns {ok:true} | {ok:false, error:'not_found'|'not_unavailable'|'still_unavailable'|'drive_error', message}
 */
export async function recheckUnavailable(id) {
  const db = getDB();
  const r = getMediaRow(id);
  if (!r || r.deleted_at) return { ok: false, error: 'not_found', message: '写真・動画が見つかりません' };
  if (!r.unavailable_at) return { ok: false, error: 'not_unavailable', message: 'この行に「ドライブから消えた」印はありません' };
  if (!r.drive_file_id) return { ok: false, error: 'still_unavailable', message: 'Drive のファイルIDがありません' };
  let exists = false;
  try {
    exists = await driveExistsImpl(r.drive_file_id);
  } catch (e) {
    const st = Number(e?.response?.status || e?.status || e?.code) || 0;
    if (st !== 404 && st !== 410) return { ok: false, error: 'drive_error', message: `Drive を確認できませんでした (${e.message})` };
  }
  if (!exists) {
    db.prepare('UPDATE f_iroha_card_media SET error = ? WHERE id = ?').run(`Drive に無いことを再確認 (${utcNow()})`, r.id);
    return { ok: false, error: 'still_unavailable', message: 'Drive にファイルがありません (印はそのまま)' };
  }
  db.prepare('UPDATE f_iroha_card_media SET unavailable_at = NULL, error = NULL WHERE id = ?').run(r.id);
  return { ok: true };
}

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

/** ページの有効メディア (Drive 保存済み) を Notion の files に**貼り直す** (0件なら空にする) */
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
    // 実体を置けないまま残った行を片づける (枚数上限を食いつぶさない — Codex PR1 R8)
    sweepStagedMedia();
    // 配信で見つかった「ドライブから消えた」写真の印をここで付ける (GET では書かない — Codex PR1 R9)
    flushUnavailableReports();
    // ①Drive へ (stored の行)。成功したらそのページの Notion 貼り直しを予約
    const toUpload = db.prepare(`SELECT * FROM f_iroha_card_media
      WHERE status = 'stored' AND staged_at IS NULL AND deleted_at IS NULL AND (next_retry_at IS NULL OR next_retry_at <= ?)
      ORDER BY id LIMIT 20`).all(now);
    for (const r of toUpload) {
      try {
        if (!r.local_path || !fs.existsSync(r.local_path)) throw new Error('実体ファイルがありません (再起動で消えた可能性。撮り直してください)');
        const { fileId, url } = await driveUploadImpl({
          localPath: r.local_path, filename: filenameFor(r), mime: r.mime, operationId: r.operation_id,
        });
        db.prepare(`UPDATE f_iroha_card_media
          SET status = 'uploaded', drive_file_id = ?, drive_url = ?, uploaded_at = ?, error = NULL, next_retry_at = NULL
          WHERE id = ?`).run(fileId, url, utcNow(), r.id);
        try { fs.unlinkSync(r.local_path); } catch { /* 消せなくても実害なし */ }
        if (r.page_id != null) requestPageSync(r.page_id);   // task_id の行は Drive 保存で完了
        stats.uploaded++;
      } catch (e) {
        markFail(db, r, e.message);
        stats.failed++;
      }
    }
    // ②Notion 貼り直し (ページ単位キュー。0件=空にする PATCH も含む)。
    //   反映は最終同期扱い — 失敗しても現場は困らない (Drive には保存済み)
    if (isNotionConfigured()) {
      const syncRows = db.prepare(`SELECT * FROM f_iroha_media_page_sync
        WHERE next_retry_at IS NULL OR next_retry_at <= ? ORDER BY requested_at LIMIT 10`).all(now);
      for (const s of syncRows) {
        try {
          if (!(await ensureMediaProp())) break;   // プロパティ型が合わない間は全ページ保留
          await syncPageToNotion(db, s.page_id);
          // ⚠PATCH を待っている間に新しい要求 (削除など) が来ていたら revision が進んでいる。
          //   そのときは完了扱いにせず残す — 次の巡回が最新の内容で貼り直す (Codex PR3-R2)
          const done = db.prepare('DELETE FROM f_iroha_media_page_sync WHERE page_id = ? AND revision = ?')
            .run(s.page_id, s.revision).changes;
          if (done > 0) stats.synced++;
        } catch (e) {
          const attempts = (s.attempt_count || 0) + 1;
          const retryAt = attempts >= MAX_ATTEMPTS ? BLOCKED_UNTIL
            : new Date(Date.now() + RETRY_BASE_MS * attempts).toISOString();
          // 失敗の記録も revision 一致時のみ (新しい要求はカウンタ0から再試行される)
          db.prepare(`UPDATE f_iroha_media_page_sync SET attempt_count = ?, next_retry_at = ?, error = ?
            WHERE page_id = ? AND revision = ?`)
            .run(attempts, retryAt, String(e.message).slice(0, 300), s.page_id, s.revision);
          stats.failed++;
        }
      }
    }
  } finally {
    running = false;
  }
  return { ok: true, ...stats };
}

/** 管理画面用: Notion 貼り直し待ち・失敗の一覧 */
export function listPageSyncForAdmin() {
  return getDB().prepare('SELECT * FROM f_iroha_media_page_sync ORDER BY requested_at').all();
}

/** 管理画面の「再実行」(貼り直し側): ブロック解除して即時キュー */
export function resetPageSync(pageId) {
  const n = getDB().prepare(`UPDATE f_iroha_media_page_sync
    SET next_retry_at = NULL, attempt_count = 0, error = NULL WHERE page_id = ?`).run(String(pageId)).changes;
  if (n > 0) schedule();
  return n > 0;
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
