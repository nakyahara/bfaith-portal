/**
 * inquiry-hub 署名 (2026-08-27 中原さん要望。メールディーラー「新規メール作成」1段目の「署名」相当)
 *
 * 位置づけ:
 *   新規メール作成の1段目 (テンプレート / To-From設定 / 署名を選ぶ画面) で選び、
 *   2段目の作成画面に開いた時点で本文の末尾へ展開する。
 *   展開後はただのテキストなので、送信前に画面でそのまま編集できる (メールディーラーと同じ)。
 *
 * 仕様 (labels.js / folders.js と同型):
 *   - 削除は論理削除のみ (is_active=0)。過去に送ったメール本文は展開済みテキストなので影響を受けない
 *   - 同名の有効署名は作れない (部分UNIQUE index。db.js)
 *   - 既定署名は最大1件 (is_default=1)。新しく既定にすると前の既定は自動で外れる
 */
import { getDB } from './db.js';

export const SIGNATURE_NAME_MAX = 40;
export const SIGNATURE_BODY_MAX = 2000;
/** 選択リストが実用的な長さに収まる上限 (メールディーラーの実運用も数件) */
export const MAX_ACTIVE_SIGNATURES = 20;

/** 入力名の正規化 + 検証 (labels.js と同じ NFKC 正規化) */
export function normalizeSignatureName(input) {
  const name = String(input ?? '').normalize('NFKC').replace(/[\r\n\t]/g, ' ').trim().replace(/\s{2,}/g, ' ');
  if (!name) throw new Error('署名の名前が空です');
  if (name.length > SIGNATURE_NAME_MAX) throw new Error(`署名の名前が長すぎます (${SIGNATURE_NAME_MAX}文字まで)`);
  return name;
}

/** 本文の正規化 + 検証。改行はそのまま活かす (署名は複数行が普通) */
export function normalizeSignatureBody(input) {
  const body = String(input ?? '').replace(/\r\n?/g, '\n').replace(/[ \t　]+$/gm, '').trim();
  if (!body) throw new Error('署名の本文が空です');
  if (body.length > SIGNATURE_BODY_MAX) throw new Error(`署名の本文が長すぎます (${SIGNATURE_BODY_MAX}文字まで)`);
  return body;
}

/** 重複判定用の正規化キー (NFKC + 小文字。DBの部分UNIQUE index と同じ値) */
export function signatureNameKey(name) {
  return String(name || '').normalize('NFKC').toLowerCase();
}

function normalizeSortOrder(v, fallback) {
  if (v === undefined || v === null || v === '') return fallback;
  const n = Number(v);
  if (!Number.isInteger(n) || n < 0 || n > 100000) throw new Error('表示順は0〜100000の整数で指定してください');
  return n;
}

function findSameName(db, name, exceptId = null) {
  return db.prepare(`SELECT id FROM inquiry_signatures
    WHERE is_active = 1 AND name_key = ? AND (? IS NULL OR id != ?)`).get(signatureNameKey(name), exceptId, exceptId);
}

/** 署名一覧 (既定 → 表示順 → 名前) */
export function listSignatures({ includeInactive = false } = {}) {
  const where = includeInactive ? '1=1' : 'is_active = 1';
  return getDB().prepare(`SELECT * FROM inquiry_signatures WHERE ${where}
    ORDER BY is_default DESC, sort_order, name, id`).all();
}

export function getSignature(id) {
  if (!Number.isInteger(id)) return null;
  return getDB().prepare('SELECT * FROM inquiry_signatures WHERE id = ?').get(id) || null;
}

/** 有効な既定署名 (無ければ null)。新規メール作成の初期選択に使う */
export function getDefaultSignature() {
  return getDB().prepare('SELECT * FROM inquiry_signatures WHERE is_active = 1 AND is_default = 1 ORDER BY id LIMIT 1')
    .get() || null;
}

/** 作成。同名 (有効) は作れない */
export function createSignature({ name, body, isDefault = false, createdBy = null } = {}) {
  const db = getDB();
  const nm = normalizeSignatureName(name);
  const bd = normalizeSignatureBody(body);
  return db.transaction(() => {
    const active = db.prepare('SELECT COUNT(*) AS c FROM inquiry_signatures WHERE is_active = 1').get().c;
    if (active >= MAX_ACTIVE_SIGNATURES) {
      throw new Error(`署名は${MAX_ACTIVE_SIGNATURES}件までです (不要なものを削除してください)`);
    }
    if (findSameName(db, nm)) throw new Error(`同じ名前の署名があります: ${nm}`);
    const maxOrder = db.prepare('SELECT MAX(sort_order) AS m FROM inquiry_signatures WHERE is_active = 1').get().m;
    // 最初の1件は自動的に既定にする (毎回「署名なし」から選び直す手間を省く)
    const makeDefault = isDefault || active === 0;
    if (makeDefault) db.prepare('UPDATE inquiry_signatures SET is_default = 0 WHERE is_default = 1').run();
    const r = db.prepare(`INSERT INTO inquiry_signatures (name, name_key, body, is_default, sort_order, created_by)
      VALUES (?,?,?,?,?,?)`).run(nm, signatureNameKey(nm), bd, makeDefault ? 1 : 0, (maxOrder ?? 0) + 10, createdBy);
    return { id: Number(r.lastInsertRowid), name: nm, isDefault: makeDefault };
  }).immediate();
}

/** 改名 / 本文変更 / 並び順変更 / 既定切替 */
export function updateSignature(id, { name, body, sortOrder, isDefault } = {}) {
  const db = getDB();
  return db.transaction(() => {
    const s = getSignature(id);
    if (!s || !s.is_active) throw new Error('署名が見つかりません');
    const newName = name === undefined ? s.name : normalizeSignatureName(name);
    const newBody = body === undefined ? s.body : normalizeSignatureBody(body);
    const newOrder = normalizeSortOrder(sortOrder, s.sort_order);
    if (findSameName(db, newName, id)) throw new Error(`同じ名前の署名があります: ${newName}`);
    // 既定は最大1件。外す指定 (false) は「既定なし」を許す (署名なしで送りたい運用もある)
    let newDefault = s.is_default;
    if (isDefault !== undefined) {
      newDefault = isDefault ? 1 : 0;
      if (newDefault) db.prepare('UPDATE inquiry_signatures SET is_default = 0 WHERE is_default = 1 AND id != ?').run(id);
    }
    db.prepare(`UPDATE inquiry_signatures SET name = ?, name_key = ?, body = ?, sort_order = ?, is_default = ?,
        updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = ?`)
      .run(newName, signatureNameKey(newName), newBody, newOrder, newDefault, id);
    return { id, name: newName, isDefault: !!newDefault };
  }).immediate();
}

/** 論理削除。既定だった場合は「既定なし」になる (他を勝手に既定へ昇格させない) */
export function deleteSignature(id) {
  const db = getDB();
  return db.transaction(() => {
    const s = getSignature(id);
    if (!s || !s.is_active) throw new Error('署名が見つかりません');
    db.prepare(`UPDATE inquiry_signatures SET is_active = 0, is_default = 0,
      updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = ?`).run(id);
    return { id, name: s.name };
  }).immediate();
}

/**
 * 本文への署名の差し込み (新規メール作成の2段目を開くときに使う)。
 * テンプレート本文の後ろに空行1つを置いて続ける。どちらか片方だけでも成立する。
 */
export function composeBodyWithSignature(bodyText, signatureBody) {
  const body = String(bodyText || '').replace(/\r\n?/g, '\n').replace(/\s+$/, '');
  const sig = String(signatureBody || '').replace(/\r\n?/g, '\n').trim();
  if (!sig) return body;
  if (!body) return `\n\n${sig}`;   // 本文入力位置を上に空けておく
  return `${body}\n\n${sig}`;
}
