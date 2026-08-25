/**
 * inquiry-hub クイックリンク (2026-08-25 中原さん要望
 * 「リンク先はこっちで登録して自動で設定できるようにしてほしい」)
 *
 * 一覧画面の上部に出す外部サイトへの導線 (楽天R-Messe・Yahoo!ストアクリエイターPro・Gmail・
 * ネクストエンジン・ロジザード等) を、コードに直書きせず画面から登録・編集する。
 *
 * 仕様 (folders.js / labels.js と同型):
 *   - 削除は論理削除のみ (is_active=0)。既定リンクの投入はテーブル新規作成時の1回だけ (db.js)
 *   - URLは http/https のみ許可 (javascript: 等のスキームは画面に出す前に弾く)
 *   - アイコンは絵文字1〜2字 (無ければ 🔗)
 */
import { getDB } from './db.js';

export const LINK_NAME_MAX = 30;
export const LINK_URL_MAX = 500;
/** 上部に並べられる上限 (これ以上は横に溢れて一覧を圧迫する) */
export const MAX_ACTIVE_LINKS = 12;

/** 表示名の正規化 + 検証 */
export function normalizeLinkName(input) {
  const name = String(input ?? '').normalize('NFKC').replace(/[\r\n\t]/g, ' ').trim().replace(/\s{2,}/g, ' ');
  if (!name) throw new Error('リンク名が空です');
  if (name.length > LINK_NAME_MAX) throw new Error(`リンク名が長すぎます (${LINK_NAME_MAX}文字まで)`);
  return name;
}

/**
 * URLの正規化 + 検証。http/https のみ許可する
 * (javascript:/data: を弾く。画面には href として出るため、保存前に確定させる)
 */
export function normalizeLinkUrl(input) {
  const raw = String(input ?? '').trim();
  if (!raw) throw new Error('URLが空です');
  if (raw.length > LINK_URL_MAX) throw new Error(`URLが長すぎます (${LINK_URL_MAX}文字まで)`);
  // 「rmesse.rms.rakuten.co.jp/」のようにスキーム無しで貼られたら https:// を補う
  const withScheme = /^[a-z][a-z0-9+.-]*:/i.test(raw) ? raw : `https://${raw}`;
  let u;
  try { u = new URL(withScheme); } catch { throw new Error('URLの形式が正しくありません'); }
  if (u.protocol !== 'http:' && u.protocol !== 'https:') {
    throw new Error('URLは http:// または https:// で指定してください');
  }
  return u.toString();
}

/** アイコン (絵文字) の正規化。空なら 🔗。長すぎる文字列や改行は弾く */
export function normalizeLinkIcon(input) {
  const raw = String(input ?? '').replace(/[\r\n\t]/g, '').trim();
  if (!raw) return '🔗';
  // 絵文字は複数コードポイントで1字になるため、字数ではなく総長で上限を切る
  if ([...raw].length > 4) throw new Error('アイコンは絵文字1〜2字で指定してください');
  return raw;
}

function normalizeSortOrder(v, fallback) {
  if (v === undefined || v === null || v === '') return fallback;
  const n = Number(v);
  if (!Number.isInteger(n) || n < 0 || n > 100000) throw new Error('表示順は0〜100000の整数で指定してください');
  return n;
}

/** リンク一覧 (表示順)。上部バーもこれを使う */
export function listQuickLinks({ includeInactive = false } = {}) {
  const where = includeInactive ? '1=1' : 'is_active = 1';
  return getDB().prepare(`SELECT * FROM inquiry_quick_links WHERE ${where}
    ORDER BY sort_order, name, id`).all();
}

export function getQuickLink(id) {
  if (!Number.isInteger(id)) return null;
  return getDB().prepare('SELECT * FROM inquiry_quick_links WHERE id = ?').get(id) || null;
}

/** 作成 */
export function createQuickLink({ name, url, icon } = {}, createdBy = null) {
  const db = getDB();
  const n = normalizeLinkName(name);
  const u = normalizeLinkUrl(url);
  const i = normalizeLinkIcon(icon);
  return db.transaction(() => {
    const active = db.prepare('SELECT COUNT(*) AS c FROM inquiry_quick_links WHERE is_active = 1').get().c;
    if (active >= MAX_ACTIVE_LINKS) {
      throw new Error(`リンクは${MAX_ACTIVE_LINKS}個までです (不要なリンクを削除してください)`);
    }
    const maxOrder = db.prepare('SELECT MAX(sort_order) AS m FROM inquiry_quick_links WHERE is_active = 1').get().m;
    const r = db.prepare('INSERT INTO inquiry_quick_links (name, url, icon, sort_order, created_by) VALUES (?,?,?,?,?)')
      .run(n, u, i, (maxOrder ?? 0) + 10, createdBy);
    return { id: r.lastInsertRowid, name: n, url: u, icon: i };
  }).immediate();
}

/** 更新 (名前・URL・アイコン・表示順) */
export function updateQuickLink(id, { name, url, icon, sortOrder } = {}) {
  const db = getDB();
  return db.transaction(() => {
    const l = getQuickLink(id);
    if (!l || !l.is_active) throw new Error('リンクが見つかりません');
    const n = name === undefined ? l.name : normalizeLinkName(name);
    const u = url === undefined ? l.url : normalizeLinkUrl(url);
    const i = icon === undefined ? l.icon : normalizeLinkIcon(icon);
    const o = normalizeSortOrder(sortOrder, l.sort_order);
    db.prepare(`UPDATE inquiry_quick_links SET name = ?, url = ?, icon = ?, sort_order = ?,
        updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = ?`).run(n, u, i, o, id);
    return { id, name: n, url: u, icon: i, sortOrder: o };
  }).immediate();
}

/** 削除 (論理削除)。再削除も成功扱い (冪等。folders.js と同じ) */
export function deleteQuickLink(id) {
  const db = getDB();
  return db.transaction(() => {
    const l = getQuickLink(id);
    if (!l) throw new Error('リンクが見つかりません');
    if (!l.is_active) return { id, name: l.name, alreadyDeleted: true };
    db.prepare(`UPDATE inquiry_quick_links SET is_active = 0,
        updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = ?`).run(id);
    return { id, name: l.name };
  }).immediate();
}
