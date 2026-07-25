/**
 * inquiry-hub queries — 一覧/詳細のクエリロジック (router から分離、smoke.mjs で直接検証)
 */
import { getDB, toUtcIso } from './db.js';

export const CHANNELS = {
  email:   { label: 'メール', badge: 'background:#e0e7ff;color:#3730a3' },
  rakuten: { label: '楽天',   badge: 'background:#fee2e2;color:#b91c1c' },
  yahoo:   { label: 'Yahoo!', badge: 'background:#fef3c7;color:#92400e' },
};
/**
 * ステータス (メールディーラー準拠。2026-07-25 中原さん要望)
 *   新着       … 顧客から来てまだ返信していない = 自分のタスク
 *   対応中/保留 … 新着の内訳 (調査中・社内確認待ちなど。任意で使う)
 *   返信処理中 … こちらが返信済み = 相手の返事待ち
 *   完了       … 対応終了。顧客から返信が来ると自動で新着に戻る
 */
export const STATUSES = {
  open:          { label: '新着',         badge: 'background:#fee2e2;color:#b91c1c' },
  in_progress:   { label: '対応中',       badge: 'background:#dbeafe;color:#1d4ed8' },
  pending:       { label: '保留',         badge: 'background:#fef3c7;color:#92400e' },
  waiting_reply: { label: '返信処理中',   badge: 'background:#ede9fe;color:#6d28d9' },
  done:          { label: '完了',         badge: 'background:#dcfce7;color:#166534' },
};
/** 受信トレイに出すステータス (=まだこちらのタスク) */
export const INBOX_STATUSES = ['open', 'in_progress', 'pending'];
export const AI_FLAGS = {
  0: { label: '—', badge: '' },
  1: { label: '🤖AI返信', badge: 'background:#ccfbf1;color:#0f766e' },
  2: { label: '👑社長確認', badge: 'background:#fce7f3;color:#be185d' },
  3: { label: '🧑‍💼責任者確認', badge: 'background:#ffedd5;color:#c2410c' },
};

export const PAGE_SIZE = 50;

/**
 * 受信箱ビュー (2026-07-25 中原さん方針 + メールディーラーのステータス管理を踏襲)。
 * 「いまボールが自分にあるもの」だけをTOP (受信トレイ) に出し、こちらが返信したものは
 * 返信処理中へ移す。相手から返事が来れば自動で新着に戻る (スレッド全履歴つき)。
 *
 * 振り分けは internal_status を正とする (手動で完了にできる・返信時に強制完了もできる)。
 * ステータス自体は sync/engine.js が「最後のメッセージの向き」で自動更新するので、
 * メールディーラーで返信した分・アプリから返信した分のどちらでも正しく遷移する
 * (人がステータスを更新し忘れても崩れない)。
 */
const LAST_INCOMING_SQL = `(SELECT m.is_incoming FROM inquiry_messages m
  WHERE m.inquiry_id = i.id ORDER BY COALESCE(m.received_at, m.sent_at, m.created_at) DESC, m.id DESC LIMIT 1)`;
/** 最後のメッセージの日時 (送信済みビューの「N日 返信なし」用)。
 * inquiries.last_message_at は手動の送信確定 (confirmed_sent) では更新されないため、
 * メッセージ本体から取る (Codexレビュー反映) */
const LAST_MESSAGE_AT_SQL = `(SELECT COALESCE(m.received_at, m.sent_at, m.created_at) FROM inquiry_messages m
  WHERE m.inquiry_id = i.id ORDER BY COALESCE(m.received_at, m.sent_at, m.created_at) DESC, m.id DESC LIMIT 1)`;

export const VIEWS = {
  inbox: {
    label: '新着', icon: '📥',
    where: `i.internal_status IN (${INBOX_STATUSES.map(s => `'${s}'`).join(',')})`,
    hint: '返信が必要なもの (完了・返信処理中でも顧客から返事が来ればここに戻ります)',
  },
  sent: {
    label: '返信処理中', icon: '📤',
    where: `i.internal_status = 'waiting_reply'`,
    hint: 'こちらが返信して相手の返事待ちのもの',
  },
  done: {
    label: '完了', icon: '✅',
    where: `i.internal_status = 'done'`,
    hint: '対応を終えたもの',
  },
  all: { label: 'すべて', icon: '🗂️', where: '1=1', hint: '新着・返信処理中・完了を問わず全件' },
};
export const DEFAULT_VIEW = 'inbox';

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

  // ビュー (受信トレイ/送信済み/対応済み)。未知の値は既定の受信トレイに寄せる
  const view = VIEWS[q.view] ? q.view : DEFAULT_VIEW;
  where.push(VIEWS[view].where);

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
      (SELECT COUNT(*) FROM inquiry_messages m WHERE m.inquiry_id = i.id) AS msg_count,
      ${LAST_INCOMING_SQL} AS last_incoming,
      ${LAST_MESSAGE_AT_SQL} AS last_message_at_actual
    FROM inquiries i JOIN shops s ON s.id = i.shop_id
    WHERE ${where.join(' AND ')}
    ORDER BY COALESCE(i.last_message_at, i.received_at) DESC, i.id DESC
    LIMIT ? OFFSET ?`).all(...params, PAGE_SIZE, (page - 1) * PAGE_SIZE);
  return { rows, total, page, pages: Math.max(1, Math.ceil(total / PAGE_SIZE)), view };
}

/** サイドバーのビュー別件数 (未アーカイブのみ) */
export function countByView() {
  const db = getDB();
  const out = {};
  for (const [key, v] of Object.entries(VIEWS)) {
    out[key] = db.prepare(`SELECT COUNT(*) AS c FROM inquiries i WHERE i.is_archived = 0 AND ${v.where}`).get().c;
  }
  return out;
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
