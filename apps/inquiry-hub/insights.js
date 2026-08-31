/**
 * inquiry-hub 実データ調査 (2026-08-28)
 *
 * 目的: AIトリアージ (出荷前のキャンセル・住所変更の検知、担当の振り分け) を設計するにあたって、
 *   「1日に何件来るのか」「そのうち何件がキャンセル・住所変更なのか」「注文番号は何%埋まっているか」
 *   を推測ではなく実データで出す。費用試算の根拠 (本文の平均長) もここで測る。
 *
 * ⭐ロジザードの締めは 09:00 / 12:30 / 14:30 の3回 (中原さん 2026-08-28)。
 *   1日の合計件数より「1回の締めまでに何件さばく必要があるか」の方が運用設計に効くので、
 *   受信時刻を「次にどの締めに間に合わせる必要があったか」で3区分に振り分ける。
 *   ⚠️ 14:30以降に届いたものと翌朝09:00までに届いたものは、どちらも同じ「翌09:00の締め」宛て
 *      なので同じ区分に入れる (Codexレビュー指摘。分けると主目的の数値にならない)
 *
 * すべて読み取り専用。個人情報は返さない (件数・時刻・文字数のみ)。
 */
import { getDB } from './db.js';

/** ロジザードの締め時刻 (JST)。変わったらここだけ直す */
export const WMS_CUTOFFS = [
  { label: '09:00', minutes: 9 * 60 },
  { label: '12:30', minutes: 12 * 60 + 30 },
  { label: '14:30', minutes: 14 * 60 + 30 },
];

/** 締め窓 = 「次にどの締めに間に合わせる必要があるか」。3区分 (締めの回数と一致させる) */
export const CUTOFF_BUCKETS = [
  { key: '09:00', label: '前日14:30〜当日09:00 に受信 → 朝09:00の締めまで' },
  { key: '12:30', label: '09:00〜12:30 に受信 → 12:30の締めまで' },
  { key: '14:30', label: '12:30〜14:30 に受信 → 14:30の締めまで' },
];

/** JST の分数から、間に合わせるべき締めの区分index を返す */
export function cutoffBucketIndex(jstMinutes) {
  const [c9, c1230, c1430] = WMS_CUTOFFS.map(c => c.minutes);
  if (jstMinutes >= c9 && jstMinutes < c1230) return 1;
  if (jstMinutes >= c1230 && jstMinutes < c1430) return 2;
  return 0; // 14:30以降 と 翌09:00未満 は同じ「翌朝09:00の締め」
}

/** 出荷前ブロック候補の検知キーワード (層1: AIを使わない決定的な検知)。
 * ⚠️ recall優先で偽陽性を許す設計。ここで拾えなかったものをAI (層2) が拾う */
export const BLOCK_KEYWORDS = [
  { name: 'キャンセル', patterns: ['%キャンセル%'] },
  { name: '取消・取り消し', patterns: ['%取消%', '%取り消し%'] },
  { name: '注文をやめたい', patterns: ['%注文%やめ%', '%いらなく%', '%不要になり%'] },
  { name: '住所', patterns: ['%住所%'] },
  { name: '宛先・お届け先', patterns: ['%宛先%', '%お届け先%', '%送り先%'] },
  { name: '変更', patterns: ['%変更%'] },
  { name: '引っ越し・転居', patterns: ['%引っ越%', '%引越%', '%転居%'] },
  { name: 'お届け日・日時指定', patterns: ['%お届け日%', '%配達日%', '%日時指定%'] },
  { name: '返金', patterns: ['%返金%'] },
  { name: '返品・交換', patterns: ['%返品%', '%交換%'] },
  { name: '破損・不良', patterns: ['%破損%', '%不良%', '%割れて%', '%壊れて%'] },
  { name: '未着・届かない', patterns: ['%届いていません%', '%届きません%', '%まだ届%'] },
];

/** キャンセル・住所変更まわりだけを抜き出すWHERE句 (締め窓と時刻分布で共用) */
const BLOCK_WHERE = `(
  m.message_body_text LIKE '%キャンセル%' OR m.message_body_text LIKE '%取消%'
  OR m.message_body_text LIKE '%取り消し%' OR m.message_body_text LIKE '%住所%'
  OR m.message_body_text LIKE '%宛先%' OR m.message_body_text LIKE '%お届け先%'
  OR m.message_body_text LIKE '%送り先%'
  OR i.subject LIKE '%キャンセル%' OR i.subject LIKE '%住所%' OR i.subject LIKE '%変更%'
)`;

const INCOMING = 'm.is_incoming = 1';
/** ⚠️ 受信時刻は必ず datetime() で包んでから比較する。
 * 生の文字列比較だと '2026-08-28T01:00:00Z' と '2026-08-28 01:00:00' で辞書順がずれ、
 * 期間の境界付近を取りこぼす (Codexレビュー指摘) */
const AT = 'datetime(COALESCE(m.received_at, m.sent_at))';
const INQ_AT = 'datetime(COALESCE(i.last_message_at, i.received_at))';
/** 受信時刻を JST に直す式 */
const JST = "datetime(COALESCE(m.received_at, m.sent_at), '+9 hours')";
/** JST の「その日の何分目か」 */
const JST_MIN = `CAST(strftime('%H', ${JST}) AS INTEGER) * 60 + CAST(strftime('%M', ${JST}) AS INTEGER)`;

/** 各項目を独立して fail-soft にする。成功でも失敗でも同じ形を返す
 * (失敗値を成功として扱って undefined を表示しないため。Codexレビュー指摘) */
function safe(fn) {
  try { return { data: fn(), error: null }; } catch (e) { return { data: null, error: String(e?.message || e).slice(0, 200) }; }
}

/** 1. 全体件数 */
function totals(db) {
  return db.prepare(`SELECT
    (SELECT COUNT(*) FROM inquiries) AS inquiries_all,
    (SELECT COUNT(*) FROM inquiries WHERE is_archived = 0) AS inquiries_active,
    (SELECT COUNT(*) FROM inquiries WHERE is_archived = 0 AND internal_status IN ('open','in_progress','pending')) AS open_like,
    (SELECT COUNT(*) FROM inquiry_messages) AS messages_all`).get();
}

/** 2. 直近N日の顧客受信メッセージ (チャネル別)。メッセージ数と問い合わせ数の両方を出す */
function byChannel(db, days) {
  return db.prepare(`SELECT i.channel_type AS channel,
      COUNT(*) AS c, COUNT(DISTINCT i.id) AS inquiries,
      ROUND(COUNT(*) * 1.0 / ?, 1) AS per_day
    FROM inquiry_messages m JOIN inquiries i ON i.id = m.inquiry_id
    WHERE ${INCOMING} AND ${AT} >= datetime('now', ?)
    GROUP BY i.channel_type ORDER BY c DESC`).all(days, `-${days} days`);
}

/** 3. 直近14日の日別 (JST基準) */
function byDay(db) {
  return db.prepare(`SELECT substr(${JST}, 1, 10) AS day, COUNT(*) AS c, COUNT(DISTINCT i.id) AS inquiries
    FROM inquiry_messages m JOIN inquiries i ON i.id = m.inquiry_id
    WHERE ${INCOMING} AND ${AT} >= datetime('now', '-14 days')
    GROUP BY day ORDER BY day`).all();
}

/** 4. フォルダ別 = ノイズがどれだけ振り分けられているか */
function byFolder(db, days) {
  return db.prepare(`SELECT COALESCE(f.name, '(未分類 = 受信トレイに出る)') AS folder, COUNT(*) AS c
    FROM inquiries i LEFT JOIN inquiry_folders f ON f.id = i.folder_id
    WHERE ${INQ_AT} >= datetime('now', ?)
    GROUP BY folder ORDER BY c DESC LIMIT 25`).all(`-${days} days`);
}

/** 5. 出荷前ブロック候補キーワードのヒット件数 (メッセージ数と問い合わせ数) */
function keywordHits(db, days) {
  return BLOCK_KEYWORDS.map(k => {
    const like = k.patterns.map(() => '(m.message_body_text LIKE ? OR i.subject LIKE ?)').join(' OR ');
    const params = k.patterns.flatMap(p => [p, p]);
    const r = db.prepare(`SELECT COUNT(*) AS c, COUNT(DISTINCT i.id) AS inquiries
      FROM inquiry_messages m JOIN inquiries i ON i.id = m.inquiry_id
      WHERE ${INCOMING} AND ${AT} >= datetime('now', ?) AND (${like})`).get(`-${days} days`, ...params);
    return { name: k.name, count: r.c, inquiries: r.inquiries, perDay: (r.c / days).toFixed(1) };
  });
}

/**
 * 6. ⭐締め窓ごとの件数 — 「1回の締めまでに何件さばく必要があるか」
 * 同じ問い合わせに複数メッセージが来ることがあるので、問い合わせ数も併記する
 * (オペレーターが処理するのは問い合わせ単位。Codexレビュー指摘)
 */
function byCutoffWindow(db, days) {
  const rows = db.prepare(`SELECT ${JST_MIN} AS jst_min, i.id AS inquiry_id
    FROM inquiry_messages m JOIN inquiries i ON i.id = m.inquiry_id
    WHERE ${INCOMING} AND ${AT} >= datetime('now', ?) AND ${BLOCK_WHERE}`).all(`-${days} days`);
  const buckets = CUTOFF_BUCKETS.map(b => ({ ...b, count: 0, inquiryIds: new Set() }));
  for (const r of rows) {
    const b = buckets[cutoffBucketIndex(r.jst_min)];
    b.count++;
    b.inquiryIds.add(r.inquiry_id);
  }
  return buckets.map(b => ({
    key: b.key, label: b.label, count: b.count, inquiries: b.inquiryIds.size,
    perDay: (b.count / days).toFixed(1),
  }));
}

/** 7. 時刻分布 (JST時) */
function byHour(db, days) {
  return db.prepare(`SELECT strftime('%H', ${JST}) AS hour, COUNT(*) AS c
    FROM inquiry_messages m JOIN inquiries i ON i.id = m.inquiry_id
    WHERE ${INCOMING} AND ${AT} >= datetime('now', ?) AND ${BLOCK_WHERE}
    GROUP BY hour ORDER BY hour`).all(`-${days} days`);
}

/**
 * 8. 注文番号の埋まり率 — 「どの注文の話か」を特定できる割合。
 * ⚠️ Gmailアダプターは orderNumber を常に null で入れる (本文から抽出していない) ので、
 *   メールチャネルは構造的に0%に近いはず。ここが層3 (出荷前かの突合) の穴になる
 */
function orderNumberFill(db, days) {
  return db.prepare(`SELECT i.channel_type AS channel, COUNT(*) AS c,
      SUM(CASE WHEN i.order_number IS NOT NULL AND i.order_number <> '' THEN 1 ELSE 0 END) AS with_order,
      ROUND(100.0 * SUM(CASE WHEN i.order_number IS NOT NULL AND i.order_number <> '' THEN 1 ELSE 0 END)
        / COUNT(*), 1) AS pct
    FROM inquiries i WHERE ${INQ_AT} >= datetime('now', ?)
    GROUP BY i.channel_type ORDER BY c DESC`).all(`-${days} days`);
}

/** 9. 担当者別 / エスカレーションフラグの使われ方 */
function assignment(db) {
  return {
    byAssignee: db.prepare(`SELECT COALESCE(assigned_user_id, '(未割当)') AS assignee, COUNT(*) AS c
      FROM inquiries i WHERE ${INQ_AT} >= datetime('now', '-90 days')
      GROUP BY assignee ORDER BY c DESC LIMIT 20`).all(),
    aiNeeded: db.prepare('SELECT ai_needed, COUNT(*) AS c FROM inquiries GROUP BY ai_needed ORDER BY ai_needed').all(),
    attention: db.prepare('SELECT needs_attention, COUNT(*) AS c FROM inquiries GROUP BY needs_attention').all(),
  };
}

/** 10. テンプレ・Q&Aのカテゴリ = 実務の問い合わせ類型 (権限マップの裏取り) */
function categories(db) {
  return {
    templates: db.prepare(`SELECT COALESCE(category, '(なし)') AS category, COUNT(*) AS c
      FROM reply_templates WHERE is_active = 1 GROUP BY category ORDER BY c DESC`).all(),
    qa: db.prepare(`SELECT COALESCE(category, '(なし)') AS category, COUNT(*) AS c
      FROM qa_entries WHERE is_active = 1 GROUP BY category ORDER BY c DESC`).all(),
  };
}

/** 11. 本文の長さ = トークン量 = 費用の実測根拠 */
function bodyLength(db, days) {
  return db.prepare(`SELECT COUNT(*) AS c,
      ROUND(AVG(LENGTH(message_body_text))) AS avg_chars,
      MAX(LENGTH(message_body_text)) AS max_chars
    FROM inquiry_messages m
    WHERE ${INCOMING} AND ${AT} >= datetime('now', ?)`).get(`-${days} days`);
}

/** 12. AI基盤の稼働実績 (未点火なら0のまま) */
function aiStatus(db) {
  return {
    runs: db.prepare('SELECT COUNT(*) AS c, MAX(started_at) AS last FROM ai_runs').get(),
    drafts: db.prepare('SELECT COUNT(*) AS c, MAX(created_at) AS last FROM ai_drafts').get(),
    queued: db.prepare("SELECT COUNT(*) AS c FROM ai_jobs WHERE status = 'queued'").get(),
  };
}

/**
 * ⚠️ この集計はキーワード12種の全文 LIKE を含み、件数が増えると重くなる。
 * better-sqlite3 は同期APIなので、画面を連打されるとイベントループを塞いで
 * 問い合わせ画面や受信同期まで止まる (Codexレビュー指摘)。
 * 短時間のメモ化で「連打しても1回しか走らない」ようにする。
 */
const CACHE_MS = 60000;
const cache = new Map();

/**
 * 調査結果を1つのオブジェクトで返す。各項目は { data, error } の形で独立して fail-soft。
 * @param {object} opts { days?: number, fresh?: boolean }
 */
export function collectInsights({ days = 30, fresh = false } = {}) {
  const d = Math.min(365, Math.max(7, Number(days) || 30));
  const hit = cache.get(d);
  if (!fresh && hit && Date.now() - hit.at < CACHE_MS) return hit.value;

  const db = getDB();
  const value = {
    days: d,
    generatedAt: new Date().toISOString(),
    totals: safe(() => totals(db)),
    byChannel: safe(() => byChannel(db, d)),
    byDay: safe(() => byDay(db)),
    byFolder: safe(() => byFolder(db, d)),
    keywordHits: safe(() => keywordHits(db, d)),
    cutoffWindows: safe(() => byCutoffWindow(db, d)),
    byHour: safe(() => byHour(db, d)),
    orderNumberFill: safe(() => orderNumberFill(db, d)),
    assignment: safe(() => assignment(db)),
    categories: safe(() => categories(db)),
    bodyLength: safe(() => bodyLength(db, d)),
    aiStatus: safe(() => aiStatus(db)),
  };
  cache.set(d, { at: Date.now(), value });
  return value;
}

/** テスト用: キャッシュを捨てる */
export function clearInsightsCache() { cache.clear(); }

/**
 * 費用試算 (実測した本文長から算出)。
 * gpt-5.6-luna = $0.20 / $1.20 per 1M tokens (2026-08時点)。
 * 日本語は概ね 1文字 ≒ 1トークン強なので、係数はやや保守的に 1.2 を使う。
 * @param {object} p { avgChars, perDay, usdJpy }
 */
export function estimateCost({ avgChars = 0, perDay = 0, usdJpy = 150 } = {}) {
  const IN_USD_PER_M = 0.20, OUT_USD_PER_M = 1.20;
  const CHAR_TO_TOKEN = 1.2;
  const SYSTEM_TOKENS = 900;      // 分類定義・出力スキーマの固定プロンプト
  const OUT_TOKENS = 180;         // JSON1件分
  const inTokens = SYSTEM_TOKENS + Math.round(Math.max(0, avgChars) * CHAR_TO_TOKEN);
  const perCallUsd = (inTokens / 1e6) * IN_USD_PER_M + (OUT_TOKENS / 1e6) * OUT_USD_PER_M;
  const perCallJpy = perCallUsd * usdJpy;
  return {
    inTokens, outTokens: OUT_TOKENS,
    perCallJpy: perCallJpy.toFixed(3),
    perDayJpy: (perCallJpy * perDay).toFixed(1),
    perMonthJpy: Math.round(perCallJpy * perDay * 30),
  };
}
