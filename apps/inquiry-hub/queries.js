/**
 * inquiry-hub queries — 一覧/詳細のクエリロジック (router から分離、smoke.mjs で直接検証)
 */
import { getDB, toUtcIso } from './db.js';

export const CHANNELS = {
  email:   { label: 'メール', badge: 'background:#e0e7ff;color:#3730a3' },
  rakuten: { label: '楽天',   badge: 'background:#fee2e2;color:#b91c1c' },
  yahoo:   { label: 'Yahoo!', badge: 'background:#fef3c7;color:#92400e' },
};
export const STATUSES = {
  open:          { label: '未対応',       badge: 'background:#fee2e2;color:#b91c1c' },
  in_progress:   { label: '対応中',       badge: 'background:#dbeafe;color:#1d4ed8' },
  pending:       { label: '保留',         badge: 'background:#fef3c7;color:#92400e' },
  waiting_reply: { label: '顧客返信待ち', badge: 'background:#ede9fe;color:#6d28d9' },
  done:          { label: '完了',         badge: 'background:#dcfce7;color:#166534' },
};
export const AI_FLAGS = {
  0: { label: '—', badge: '' },
  1: { label: '🤖AI返信', badge: 'background:#ccfbf1;color:#0f766e' },
  2: { label: '👑社長確認', badge: 'background:#fce7f3;color:#be185d' },
  3: { label: '🧑‍💼責任者確認', badge: 'background:#ffedd5;color:#c2410c' },
};

export const PAGE_SIZE = 50;

/** LIKE検索用エスケープ (%_ をリテラル化。ESCAPE '\' とセットで使う) */
export const likeEsc = s => String(s).replace(/[\\%_]/g, c => '\\' + c);

/**
 * 一覧クエリ (設計書§7.2)。q = req.query 相当のプレーンオブジェクト。
 * 返り値: { rows, total, page, pages }
 */
export function listInquiries(q = {}) {
  const db = getDB();
  const where = ['i.is_archived = 0'];
  const params = [];

  if (q.status && STATUSES[q.status]) { where.push('i.internal_status = ?'); params.push(q.status); }
  if (q.channel && CHANNELS[q.channel]) { where.push('i.channel_type = ?'); params.push(q.channel); }
  if (q.shop && /^\d+$/.test(String(q.shop))) { where.push('i.shop_id = ?'); params.push(Number(q.shop)); }
  if (q.assigned === 'none') where.push("(i.assigned_user_id IS NULL OR i.assigned_user_id = '')");
  else if (q.assigned) { where.push('i.assigned_user_id = ?'); params.push(String(q.assigned)); }
  if (q.unread === '1') where.push('i.is_unread = 1');
  if (q.attention === '1') where.push('i.needs_attention = 1');
  if (q.ai === '1') where.push('i.ai_needed != 0');
  // 期間はJST日付で指定 → 保存形式 (UTC 'YYYY-MM-DDTHH:MM:SSZ'、db.js toUtcIso参照) のUTC境界に変換して比較
  if (q.from && /^\d{4}-\d{2}-\d{2}$/.test(q.from)) { where.push('i.received_at >= ?'); params.push(toUtcIso(q.from + 'T00:00:00+09:00')); }
  if (q.to && /^\d{4}-\d{2}-\d{2}$/.test(q.to)) { where.push('i.received_at < ?'); params.push(toUtcIso(Date.parse(q.to + 'T00:00:00+09:00') + 86400000)); }

  const kw = String(q.q || '').trim();
  if (kw) {
    const like = `%${likeEsc(kw)}%`;
    where.push(`(i.customer_name LIKE ? ESCAPE '\\' OR i.customer_identifier LIKE ? ESCAPE '\\'
      OR i.subject LIKE ? ESCAPE '\\' OR i.order_number LIKE ? ESCAPE '\\'
      OR i.product_code LIKE ? ESCAPE '\\' OR i.product_name LIKE ? ESCAPE '\\'
      OR EXISTS (SELECT 1 FROM inquiry_messages m WHERE m.inquiry_id = i.id AND m.message_body_text LIKE ? ESCAPE '\\'))`);
    for (let k = 0; k < 7; k++) params.push(like);
  }

  const page = Math.max(1, parseInt(q.page, 10) || 1);
  const total = db.prepare(`SELECT COUNT(*) AS c FROM inquiries i WHERE ${where.join(' AND ')}`).get(...params).c;
  const rows = db.prepare(`
    SELECT i.*, s.shop_name,
      (SELECT COUNT(*) FROM inquiry_messages m WHERE m.inquiry_id = i.id) AS msg_count
    FROM inquiries i JOIN shops s ON s.id = i.shop_id
    WHERE ${where.join(' AND ')}
    ORDER BY COALESCE(i.last_message_at, i.received_at) DESC, i.id DESC
    LIMIT ? OFFSET ?`).all(...params, PAGE_SIZE, (page - 1) * PAGE_SIZE);
  return { rows, total, page, pages: Math.max(1, Math.ceil(total / PAGE_SIZE)) };
}

/** 一覧画面のフィルタ用マスタ (店舗・担当者・状態別件数) */
export function listFilterOptions() {
  const db = getDB();
  const shops = db.prepare('SELECT id, shop_name, channel_type FROM shops WHERE is_active = 1 ORDER BY channel_type, shop_name').all();
  const assignees = db.prepare("SELECT DISTINCT assigned_user_id AS u FROM inquiries WHERE assigned_user_id IS NOT NULL AND assigned_user_id != '' ORDER BY 1").all().map(r => r.u);
  const counts = db.prepare('SELECT internal_status, COUNT(*) AS c FROM inquiries WHERE is_archived = 0 GROUP BY internal_status').all();
  return { shops, assignees, countMap: Object.fromEntries(counts.map(r => [r.internal_status, r.c])) };
}

/** 詳細画面データ一式。存在しなければ null */
export function getInquiryDetail(id) {
  const db = getDB();
  if (!Number.isInteger(id)) return null;
  const inquiry = db.prepare(`
    SELECT i.*, s.shop_name, s.account_identifier, s.last_synced_at AS shop_last_synced_at
    FROM inquiries i JOIN shops s ON s.id = i.shop_id WHERE i.id = ?`).get(id);
  if (!inquiry) return null;
  const messages = db.prepare('SELECT * FROM inquiry_messages WHERE inquiry_id = ? ORDER BY COALESCE(received_at, sent_at, created_at), id').all(id);
  const attachments = db.prepare(`SELECT a.* FROM inquiry_attachments a
    JOIN inquiry_messages m ON m.id = a.inquiry_message_id WHERE m.inquiry_id = ?`).all(id);
  const notes = db.prepare('SELECT * FROM internal_notes WHERE inquiry_id = ? ORDER BY created_at DESC, id DESC').all(id);
  const logs = db.prepare('SELECT * FROM inquiry_activity_logs WHERE inquiry_id = ? ORDER BY created_at DESC, id DESC LIMIT 50').all(id);
  const draft = db.prepare('SELECT * FROM ai_drafts WHERE inquiry_id = ? ORDER BY id DESC LIMIT 1').get(id);
  return { inquiry, messages, attachments, notes, logs, draft };
}
