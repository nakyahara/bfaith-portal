/**
 * inquiry-hub 任意フォルダ (2026-08-02 スタッフ要望「受信トレイと送信済みしかフォルダがない」)
 *
 * 位置づけ (中原さん確認済み 2026-08-02):
 *   フォルダは「分類ラベル」であって受信箱の付け替えではない。
 *   フォルダに入れても internal_status は変わらず、未返信なら受信トレイにも出続ける
 *   (フォルダに入れたまま返信を忘れる事故を作らないため)。
 *
 * 仕様:
 *   - 1件の問い合わせは1フォルダ (inquiries.folder_id。NULL=未分類)
 *   - 削除は論理削除のみ (is_active=0)。削除時は中の問い合わせを未分類に戻す
 *     (消えたフォルダに閉じ込められて画面から辿れなくなるのを防ぐ)
 *   - 同名の有効フォルダは作れない (部分UNIQUE index。db.js)
 */
import { getDB, logActivity } from './db.js';

export const FOLDER_NAME_MAX = 40;
/** サイドバーが縦に伸びすぎない上限 (これ以上必要になったら階層化を設計する) */
export const MAX_ACTIVE_FOLDERS = 30;

/** 入力名の正規化 + 検証。不正なら throw */
export function normalizeFolderName(input) {
  const name = String(input ?? '').replace(/[\r\n\t]/g, ' ').trim().replace(/\s{2,}/g, ' ');
  if (!name) throw new Error('フォルダ名が空です');
  if (name.length > FOLDER_NAME_MAX) throw new Error(`フォルダ名が長すぎます (${FOLDER_NAME_MAX}文字まで)`);
  return name;
}

/**
 * フォルダ一覧。件数は未アーカイブの問い合わせのみ数える (一覧画面の見え方と揃える)。
 * @param {object} opts { withCounts?: boolean, includeInactive?: boolean }
 */
export function listFolders({ withCounts = false, includeInactive = false } = {}) {
  const db = getDB();
  const where = includeInactive ? '1=1' : 'f.is_active = 1';
  if (!withCounts) {
    return db.prepare(`SELECT * FROM inquiry_folders f WHERE ${where}
      ORDER BY f.sort_order, f.name, f.id`).all();
  }
  return db.prepare(`SELECT f.*,
      (SELECT COUNT(*) FROM inquiries i WHERE i.folder_id = f.id AND i.is_archived = 0) AS total,
      (SELECT COUNT(*) FROM inquiries i WHERE i.folder_id = f.id AND i.is_archived = 0
         AND i.internal_status IN ('open','in_progress','pending')) AS open_count
    FROM inquiry_folders f WHERE ${where}
    ORDER BY f.sort_order, f.name, f.id`).all();
}

/** 未分類 (folder_id IS NULL) の件数 */
export function countUnfiled() {
  return getDB().prepare('SELECT COUNT(*) AS c FROM inquiries WHERE is_archived = 0 AND folder_id IS NULL').get().c;
}

export function getFolder(id) {
  if (!Number.isInteger(id)) return null;
  return getDB().prepare('SELECT * FROM inquiry_folders WHERE id = ?').get(id) || null;
}

/** 作成。同名 (有効) は作れない */
export function createFolder(nameInput, createdBy = null) {
  const db = getDB();
  const name = normalizeFolderName(nameInput);
  return db.transaction(() => {
    const active = db.prepare('SELECT COUNT(*) AS c FROM inquiry_folders WHERE is_active = 1').get().c;
    if (active >= MAX_ACTIVE_FOLDERS) {
      throw new Error(`フォルダは${MAX_ACTIVE_FOLDERS}個までです (不要なフォルダを削除してください)`);
    }
    if (db.prepare('SELECT id FROM inquiry_folders WHERE is_active = 1 AND name = ?').get(name)) {
      throw new Error(`同じ名前のフォルダがあります: ${name}`);
    }
    // 末尾に追加 (並び順は管理画面から変更できる)
    const maxOrder = db.prepare('SELECT MAX(sort_order) AS m FROM inquiry_folders WHERE is_active = 1').get().m;
    const r = db.prepare('INSERT INTO inquiry_folders (name, sort_order, created_by) VALUES (?,?,?)')
      .run(name, (maxOrder ?? 0) + 10, createdBy);
    return { id: r.lastInsertRowid, name };
  }).immediate();
}

/** 改名 / 並び順変更 */
export function updateFolder(id, { name, sortOrder } = {}) {
  const db = getDB();
  return db.transaction(() => {
    const f = getFolder(id);
    if (!f || !f.is_active) throw new Error('フォルダが見つかりません');
    const newName = name === undefined ? f.name : normalizeFolderName(name);
    if (newName !== f.name && db.prepare('SELECT id FROM inquiry_folders WHERE is_active = 1 AND name = ? AND id != ?').get(newName, id)) {
      throw new Error(`同じ名前のフォルダがあります: ${newName}`);
    }
    const newOrder = Number.isFinite(Number(sortOrder)) ? Number(sortOrder) : f.sort_order;
    db.prepare(`UPDATE inquiry_folders SET name = ?, sort_order = ?,
        updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = ?`).run(newName, newOrder, id);
    return { id, name: newName, sortOrder: newOrder };
  }).immediate();
}

/**
 * 削除 (論理削除)。中の問い合わせは未分類に戻す。
 * @returns {{ id, name, detached: number }} detached = 未分類に戻した件数
 */
export function deleteFolder(id, actorId = null) {
  const db = getDB();
  return db.transaction(() => {
    const f = getFolder(id);
    if (!f || !f.is_active) throw new Error('フォルダが見つかりません');
    // 所属していた問い合わせは未分類へ (履歴には「フォルダ解除」を残す)
    const members = db.prepare('SELECT id FROM inquiries WHERE folder_id = ?').all(id);
    db.prepare(`UPDATE inquiries SET folder_id = NULL,
        updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE folder_id = ?`).run(id);
    for (const m of members) {
      logActivity(m.id, { actorType: 'user', userId: actorId, actionType: 'folder_change',
        before: { folder: f.name }, after: { folder: null } });
    }
    db.prepare(`UPDATE inquiry_folders SET is_active = 0,
        updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = ?`).run(id);
    return { id, name: f.name, detached: members.length };
  }).immediate();
}

/**
 * 問い合わせのフォルダ変更 (null で未分類に戻す)。操作ログも同一トランザクションで記録。
 * @returns {{ ok: true, unchanged?: boolean, folder: string|null }}
 */
export function setInquiryFolder(inquiryId, folderId, actorId = null) {
  const db = getDB();
  return db.transaction(() => {
    const inq = db.prepare('SELECT id, folder_id FROM inquiries WHERE id = ?').get(inquiryId);
    if (!inq) throw new Error('問い合わせが見つかりません');
    let target = null;
    if (folderId != null) {
      target = getFolder(Number(folderId));
      if (!target || !target.is_active) throw new Error('フォルダが見つかりません');
    }
    const beforeId = inq.folder_id ?? null;
    const afterId = target ? target.id : null;
    if (beforeId === afterId) return { ok: true, unchanged: true, folder: target ? target.name : null };
    const before = beforeId == null ? null : (getFolder(beforeId)?.name ?? null);
    db.prepare(`UPDATE inquiries SET folder_id = ?,
        updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = ?`).run(afterId, inquiryId);
    logActivity(inquiryId, { actorType: 'user', userId: actorId, actionType: 'folder_change',
      before: { folder: before }, after: { folder: target ? target.name : null } });
    return { ok: true, folder: target ? target.name : null };
  }).immediate();
}
