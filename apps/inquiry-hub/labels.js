/**
 * inquiry-hub 色付きラベル (2026-08-24 中原さん要望。メールディーラーの「ラベルの設定」相当)
 *
 * 位置づけ:
 *   フォルダ (folders.js) = 分類箱、ラベル = 一覧で目立たせる色付きの目印。
 *   メールディーラーと同じく 1件1ラベル (inquiries.label_id。NULL=ラベルなし)。
 *   ラベルを付けても internal_status は変わらない (受信トレイの見え方は不変)。
 *
 * 仕様 (folders.js と同型):
 *   - 削除は論理削除のみ (is_active=0)。削除時は付いていた問い合わせをラベルなしに戻し、
 *     メールルールの label_id 参照も外す (消えたラベルをルールが付け続けないため)
 *   - 同名の有効ラベルは作れない (部分UNIQUE index。db.js)
 *   - 色は '#rrggbb' のみ (チップの style に埋め込むため厳格に検証する)
 */
import { getDB, logActivity } from './db.js';

export const LABEL_NAME_MAX = 40;
/** ラベルが増えすぎて一覧が色だらけにならない上限 (メールディーラー実運用は20個程度) */
export const MAX_ACTIVE_LABELS = 30;
/** ラベル削除時に問い合わせ1件ずつの操作ログを残す上限 (超えたら削除ログのみ) */
const PER_INQUIRY_LOG_LIMIT = 200;

/** 作成UIに出す色見本 (メールディーラーのラベル色相当。任意の #rrggbb も許可) */
export const LABEL_PALETTE = [
  '#ef4444', // 赤 (クレーム・至急)
  '#ec4899', // ピンク
  '#f97316', // オレンジ
  '#eab308', // 黄
  '#22c55e', // 緑
  '#14b8a6', // ティール
  '#06b6d4', // シアン
  '#3b82f6', // 青
  '#8b5cf6', // 紫
  '#64748b', // グレー
];

/** 色の検証 + 小文字正規化。不正なら throw ('#rrggbb' 以外は style 埋め込みできないため厳格に) */
export function normalizeLabelColor(input) {
  const c = String(input ?? '').trim().toLowerCase();
  if (!/^#[0-9a-f]{6}$/.test(c)) throw new Error('色は #rrggbb 形式で指定してください');
  return c;
}

/** チップの文字色 (背景の明度で白/黒を自動選択。黄色地に白文字で読めない事故を防ぐ) */
export function labelTextColor(color) {
  const m = /^#([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/.exec(String(color || '').toLowerCase());
  if (!m) return '#fff';
  const [r, g, b] = [1, 2, 3].map(i => parseInt(m[i], 16));
  return (0.299 * r + 0.587 * g + 0.114 * b) > 160 ? '#1f2937' : '#fff';
}

/** 入力名の正規化 + 検証 (folders.js と同じ NFKC 正規化) */
export function normalizeLabelName(input) {
  const name = String(input ?? '').normalize('NFKC').replace(/[\r\n\t]/g, ' ').trim().replace(/\s{2,}/g, ' ');
  if (!name) throw new Error('ラベル名が空です');
  if (name.length > LABEL_NAME_MAX) throw new Error(`ラベル名が長すぎます (${LABEL_NAME_MAX}文字まで)`);
  return name;
}

function normalizeSortOrder(v, fallback) {
  if (v === undefined || v === null || v === '') return fallback;
  const n = Number(v);
  if (!Number.isInteger(n) || n < 0 || n > 100000) throw new Error('表示順は0〜100000の整数で指定してください');
  return n;
}

/** 重複判定用の正規化キー (NFKC + 小文字。DBの部分UNIQUE index と同じ値) */
export function labelNameKey(name) {
  return String(name || '').normalize('NFKC').toLowerCase();
}

function findSameName(db, name, exceptId = null) {
  return db.prepare(`SELECT id FROM inquiry_labels
    WHERE is_active = 1 AND name_key = ? AND (? IS NULL OR id != ?)`).get(labelNameKey(name), exceptId, exceptId);
}

/** ラベル一覧。件数は未アーカイブの問い合わせのみ数える */
export function listLabels({ withCounts = false, includeInactive = false } = {}) {
  const db = getDB();
  const where = includeInactive ? '1=1' : 'l.is_active = 1';
  if (!withCounts) {
    return db.prepare(`SELECT * FROM inquiry_labels l WHERE ${where}
      ORDER BY l.sort_order, l.name, l.id`).all();
  }
  return db.prepare(`SELECT l.*,
      (SELECT COUNT(*) FROM inquiries i WHERE i.label_id = l.id AND i.is_archived = 0) AS total
    FROM inquiry_labels l WHERE ${where}
    ORDER BY l.sort_order, l.name, l.id`).all();
}

export function getLabel(id) {
  if (!Number.isInteger(id)) return null;
  return getDB().prepare('SELECT * FROM inquiry_labels WHERE id = ?').get(id) || null;
}

/** 作成。同名 (有効) は作れない */
export function createLabel(nameInput, colorInput, createdBy = null) {
  const db = getDB();
  const name = normalizeLabelName(nameInput);
  const color = normalizeLabelColor(colorInput ?? LABEL_PALETTE[LABEL_PALETTE.length - 1]);
  return db.transaction(() => {
    const active = db.prepare('SELECT COUNT(*) AS c FROM inquiry_labels WHERE is_active = 1').get().c;
    if (active >= MAX_ACTIVE_LABELS) {
      throw new Error(`ラベルは${MAX_ACTIVE_LABELS}個までです (不要なラベルを削除してください)`);
    }
    if (findSameName(db, name)) throw new Error(`同じ名前のラベルがあります: ${name}`);
    const maxOrder = db.prepare('SELECT MAX(sort_order) AS m FROM inquiry_labels WHERE is_active = 1').get().m;
    const r = db.prepare('INSERT INTO inquiry_labels (name, name_key, color, sort_order, created_by) VALUES (?,?,?,?,?)')
      .run(name, labelNameKey(name), color, (maxOrder ?? 0) + 10, createdBy);
    return { id: r.lastInsertRowid, name, color };
  }).immediate();
}

/** 改名 / 色変更 / 並び順変更 */
export function updateLabel(id, { name, color, sortOrder } = {}) {
  const db = getDB();
  return db.transaction(() => {
    const l = getLabel(id);
    if (!l || !l.is_active) throw new Error('ラベルが見つかりません');
    const newName = name === undefined ? l.name : normalizeLabelName(name);
    if (newName !== l.name && findSameName(db, newName, id)) {
      throw new Error(`同じ名前のラベルがあります: ${newName}`);
    }
    const newColor = color === undefined ? l.color : normalizeLabelColor(color);
    const newOrder = normalizeSortOrder(sortOrder, l.sort_order);
    db.prepare(`UPDATE inquiry_labels SET name = ?, name_key = ?, color = ?, sort_order = ?,
        updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = ?`)
      .run(newName, labelNameKey(newName), newColor, newOrder, id);
    return { id, name: newName, color: newColor, sortOrder: newOrder };
  }).immediate();
}

/**
 * 削除 (論理削除)。付いていた問い合わせはラベルなしへ、参照していたメールルールの
 * ラベル指定も外す (ルール自体は残す。フォルダ・完了扱いの動きは変えない)。
 * 再削除は成功として返す (冪等。folders.js と同じ)。
 * @returns {{ id, name, detached: number, rulesDetached: number, alreadyDeleted?: boolean }}
 */
export function deleteLabel(id, actorId = null) {
  const db = getDB();
  return db.transaction(() => {
    const l = getLabel(id);
    if (!l) throw new Error('ラベルが見つかりません');
    if (!l.is_active) return { id, name: l.name, detached: 0, rulesDetached: 0, alreadyDeleted: true };
    const members = db.prepare('SELECT id FROM inquiries WHERE label_id = ? ORDER BY id LIMIT ?')
      .all(id, PER_INQUIRY_LOG_LIMIT + 1);
    const detached = db.prepare(`UPDATE inquiries SET label_id = NULL,
        updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE label_id = ?`).run(id).changes;
    if (members.length <= PER_INQUIRY_LOG_LIMIT) {
      for (const m of members) {
        logActivity(m.id, { actorType: 'user', userId: actorId, actionType: 'label_change',
          before: { label: l.name }, after: { label: null } });
      }
    }
    const rulesDetached = db.prepare(`UPDATE mail_rules SET label_id = NULL,
        updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE label_id = ?`).run(id).changes;
    db.prepare(`UPDATE inquiry_labels SET is_active = 0,
        updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = ?`).run(id);
    return { id, name: l.name, detached, rulesDetached };
  }).immediate();
}

/**
 * 問い合わせのラベル変更 (null でラベルなしに戻す)。操作ログも同一トランザクションで記録。
 * @returns {{ ok: true, unchanged?: boolean, label: string|null }}
 */
export function setInquiryLabel(inquiryId, labelId, actorId = null) {
  const db = getDB();
  return db.transaction(() => {
    const inq = db.prepare('SELECT id, label_id FROM inquiries WHERE id = ?').get(inquiryId);
    if (!inq) throw new Error('問い合わせが見つかりません');
    let target = null;
    if (labelId != null) {
      target = getLabel(Number(labelId));
      if (!target || !target.is_active) throw new Error('ラベルが見つかりません');
    }
    const beforeId = inq.label_id ?? null;
    const afterId = target ? target.id : null;
    if (beforeId === afterId) return { ok: true, unchanged: true, label: target ? target.name : null };
    const before = beforeId == null ? null : (getLabel(beforeId)?.name ?? null);
    db.prepare(`UPDATE inquiries SET label_id = ?,
        updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = ?`).run(afterId, inquiryId);
    logActivity(inquiryId, { actorType: 'user', userId: actorId, actionType: 'label_change',
      before: { label: before }, after: { label: target ? target.name : null } });
    return { ok: true, label: target ? target.name : null };
  }).immediate();
}
