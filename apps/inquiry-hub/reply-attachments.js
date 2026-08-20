/**
 * 返信の送信用添付 (2026-08-20 スタッフ要望「PDFなどを添付できるように」)
 *
 * 方針:
 *   - まずメールチャネルのみ (納品書PDF等の用途)。楽天/Yahoo!はAPIの添付仕様を確認してから
 *   - 実体はDBのBLOBに保持 (アップロード〜送信ワーカーの間だけ必要。Renderの永続ディスクに
 *     ファイルを撒かない・バックアップと整合)。上限を厳しくして肥大を防ぐ
 *   - 形式は中身の先頭バイトで判定 (mime.js sniffContentType)。拡張子・申告Content-Typeは信じない。
 *     判定できない形式 (Office文書など) はまず対象外 — 必要になったら安全な検証を足してから広げる
 *   - ジョブ未紐付け (下書き中) の添付は24時間で掃除 (アップロード時に併せて実施。cronを増やさない)
 */
import { getDB } from './db.js';
import { sniffContentType } from './mime.js';

export const MAX_FILE_BYTES = 5 * 1024 * 1024;      // 1ファイル5MB
export const MAX_FILES_PER_REPLY = 3;               // 1返信に3つまで
export const MAX_TOTAL_BYTES = 10 * 1024 * 1024;    // 1返信の合計10MB (Gmail送信のbase64膨張を見込む)
const ORPHAN_TTL_HOURS = 24;

/** 添付を許す形式 = 中身から確実に判定できるもの (mime.js sniff の対象) */
const ALLOWED_TYPES = new Set(['application/pdf', 'image/png', 'image/jpeg', 'image/gif', 'image/webp', 'image/bmp']);
export const ALLOWED_LABEL = 'PDF・画像 (png/jpg/gif/webp/bmp)';

/** ジョブ未紐付けのまま放置された添付を掃除 (アップロード・一覧時に呼ぶ) */
export function pruneOrphanAttachments() {
  return getDB().prepare(`DELETE FROM outbox_attachments
    WHERE outbox_id IS NULL AND created_at < strftime('%Y-%m-%dT%H:%M:%SZ','now','-${ORPHAN_TTL_HOURS} hours')`).run().changes;
}

/**
 * アップロード1件を検証して保存する。
 * @returns {{ id, fileName, contentType, fileSize }}
 * @throws Error (message はそのまま画面に出せる日本語)
 */
export function saveReplyAttachment({ inquiryId, fileName, buffer, uploadedBy }) {
  const db = getDB();
  pruneOrphanAttachments();
  const inq = db.prepare('SELECT id, channel_type FROM inquiries WHERE id = ?').get(inquiryId);
  if (!inq) throw new Error('問い合わせが見つかりません');
  if (inq.channel_type !== 'email') throw new Error('添付はメール返信のみ対応しています (モールのAPI添付仕様を確認してから広げます)');
  if (!Buffer.isBuffer(buffer) || buffer.length === 0) throw new Error('ファイルが空です');
  if (buffer.length > MAX_FILE_BYTES) {
    throw new Error(`ファイルが大きすぎます (${(buffer.length / 1048576).toFixed(1)}MB。上限${Math.round(MAX_FILE_BYTES / 1048576)}MB)`);
  }
  // 中身の先頭バイトで形式を確定する。偽装拡張子・判定不能はここで弾く
  const contentType = sniffContentType(buffer);
  if (!contentType || !ALLOWED_TYPES.has(contentType)) {
    throw new Error(`このファイル形式は添付できません (対応: ${ALLOWED_LABEL})`);
  }
  const name = String(fileName || '').trim().slice(0, 200) || 'attachment';
  const pending = db.prepare(
    'SELECT COUNT(*) AS c, COALESCE(SUM(file_size), 0) AS bytes FROM outbox_attachments WHERE inquiry_id = ? AND outbox_id IS NULL')
    .get(inquiryId);
  if (pending.c >= MAX_FILES_PER_REPLY) throw new Error(`添付は${MAX_FILES_PER_REPLY}つまでです (不要なものを削除してください)`);
  if (pending.bytes + buffer.length > MAX_TOTAL_BYTES) {
    throw new Error(`添付の合計サイズが上限 (${Math.round(MAX_TOTAL_BYTES / 1048576)}MB) を超えます`);
  }
  const r = db.prepare(`INSERT INTO outbox_attachments (inquiry_id, file_name, content_type, file_size, body, uploaded_by)
    VALUES (?,?,?,?,?,?)`).run(inquiryId, name, contentType, buffer.length, buffer, String(uploadedBy || 'portal'));
  return { id: Number(r.lastInsertRowid), fileName: name, contentType, fileSize: buffer.length };
}

/** この問い合わせのジョブ未紐付け添付 (返信フォームの再表示用。本体は返さない) */
export function listPendingAttachments(inquiryId) {
  pruneOrphanAttachments();
  return getDB().prepare(`SELECT id, file_name, content_type, file_size FROM outbox_attachments
    WHERE inquiry_id = ? AND outbox_id IS NULL ORDER BY id`).all(inquiryId)
    .map(r => ({ id: r.id, name: r.file_name, type: r.content_type, size: r.file_size }));
}

/** ジョブ未紐付けの添付を削除 (紐付け済み = 送信対象・送信済みの記録なので消さない) */
export function deletePendingAttachment(inquiryId, attachmentId) {
  const r = getDB().prepare('DELETE FROM outbox_attachments WHERE id = ? AND inquiry_id = ? AND outbox_id IS NULL')
    .run(attachmentId, inquiryId);
  if (!r.changes) throw new Error('この添付は削除できません (既に送信ジョブに紐付いているか、存在しません)');
}

/**
 * ジョブ作成トランザクション内で呼ぶ: 添付IDを検証してジョブに紐付け、attachments_json の中身を返す。
 * 呼び出し側 (outbox.createReplyJob) のトランザクションに同居する前提 (自前でtxは張らない)。
 * @returns {Array<{id, name, type, size}>}
 */
export function claimAttachmentsForJob(db, { inquiryId, outboxId, attachmentIds }) {
  const ids = [...new Set((attachmentIds || []).map(Number))];
  if (!ids.length) return [];
  if (ids.length > MAX_FILES_PER_REPLY) throw new Error(`添付は${MAX_FILES_PER_REPLY}つまでです`);
  if (!ids.every(n => Number.isInteger(n) && n > 0)) throw new Error('添付の指定が不正です');
  const rows = db.prepare(`SELECT id, file_name, content_type, file_size FROM outbox_attachments
    WHERE id IN (${ids.map(() => '?').join(',')}) AND inquiry_id = ? AND outbox_id IS NULL`).all(...ids, inquiryId);
  if (rows.length !== ids.length) {
    throw new Error('添付が見つかりません (削除済みか別の返信に使用済みです。添付し直してください)');
  }
  const total = rows.reduce((s, r) => s + r.file_size, 0);
  if (total > MAX_TOTAL_BYTES) throw new Error(`添付の合計サイズが上限 (${Math.round(MAX_TOTAL_BYTES / 1048576)}MB) を超えます`);
  db.prepare(`UPDATE outbox_attachments SET outbox_id = ? WHERE id IN (${ids.map(() => '?').join(',')})`)
    .run(outboxId, ...ids);
  return rows.map(r => ({ id: r.id, name: r.file_name, type: r.content_type, size: r.file_size }));
}

/** 送信ワーカー用: ジョブに紐付いた添付の実体を読み出す */
export function loadJobAttachments(outboxId) {
  return getDB().prepare('SELECT file_name, content_type, body FROM outbox_attachments WHERE outbox_id = ? ORDER BY id')
    .all(outboxId)
    .map(r => ({ fileName: r.file_name, contentType: r.content_type, buffer: r.body }));
}
