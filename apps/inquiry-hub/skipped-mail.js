/**
 * skipped-mail.js — メールルールで「取り込まない」(skip) になったメールの記録 (2026-09-21 中原さん「やって」)
 *
 * ⭐なぜ必要か:
 *   skip ルール 758 件は、メールディーラーの「ゴミ箱へ移動」を移したもの = 判断の基準は「お客さまの問い合わせかどうか」。
 *   同じ受信箱には、受注通知 (99%) のほかに **モールの運営からの通知** (イベントの申込締切・アカウント健全性の警告・規約変更の期限) も届く。
 *   それが受注通知と一緒に skip されていても、今までは **件数しか残らなかった** (Gmail 同期のログに「N スレッドをスキップ」) = 何が落ちたのか、誰も後から調べられない。
 *   → 落としたメールの **差出人・件名・当たったルール** だけを残す (本文は持たない)。集計して「期限つきの通知が混ざっていないか」を見る。
 *
 * ⭐設計の要点:
 *   1. **同期を止めない**。記録は補助。失敗しても Gmail 同期は続ける (呼ぶ側が try/catch して警告を出す)
 *   2. **本文を持たない**。件名は 200 文字で切る。保持は 30 日 (記録のたびに古い行を消す)
 *   3. **同じスレッドは 1 行**。同期は同じ窓を何度も読み直すので、スレッド ID で上書きし、見た回数と最後に見た時刻を進める
 *      (件数を「同期の回数」で水増ししない)
 *   4. 集計の鍵は「差出人のドメイン × 当たったルール」。件名の例を 3 つまで添える = 画面で「この型は落としてよいか」を人が判断できる
 */
import { getDB } from './db.js';

export const SKIPPED_KEEP_DAYS = 30;
export const SUBJECT_MAX = 200;
const FROM_MAX = 254;

const clip = (v, n) => String(v ?? '').replace(/[\r\n\t]+/g, ' ').trim().slice(0, n);
export const domainOf = (addr) => { const s = String(addr || '').toLowerCase(); const i = s.lastIndexOf('@'); return i >= 0 ? s.slice(i + 1) : ''; };
const iso = (ms) => (Number.isFinite(ms) && ms > 0 ? new Date(ms).toISOString() : null);

/**
 * skip になったスレッドを記録する。
 * @param {Array<{ threadId, from, subject, receivedAtMs, ruleId, ruleName }>} list
 * @param {{ now?: Date, keepDays?: number }} [opt]
 * @returns {{ recorded: number, purged: number }}
 */
export function recordSkippedMails(list, { now = new Date(), keepDays = SKIPPED_KEEP_DAYS } = {}) {
  const db = getDB();
  const rows = (Array.isArray(list) ? list : []).filter((x) => x && x.threadId);
  const nowIso = now.toISOString();
  const up = db.prepare(`
    INSERT INTO skipped_mail_log (thread_id, from_address, from_domain, subject, received_at, rule_id, rule_name, first_seen_at, last_seen_at, seen_count)
    VALUES (@thread_id, @from_address, @from_domain, @subject, @received_at, @rule_id, @rule_name, @now, @now, 1)
    ON CONFLICT(thread_id) DO UPDATE SET
      from_address = excluded.from_address, from_domain = excluded.from_domain, subject = excluded.subject, received_at = excluded.received_at,
      rule_id = excluded.rule_id, rule_name = excluded.rule_name, last_seen_at = excluded.last_seen_at, seen_count = skipped_mail_log.seen_count + 1`);
  const tx = db.transaction(() => {
    for (const x of rows) {
      const from = clip(x.from, FROM_MAX).toLowerCase();
      up.run({ thread_id: String(x.threadId).slice(0, 64), from_address: from, from_domain: domainOf(from), subject: clip(x.subject, SUBJECT_MAX),
        received_at: iso(Number(x.receivedAtMs)), rule_id: Number.isInteger(x.ruleId) ? x.ruleId : null, rule_name: clip(x.ruleName, 120) || null, now: nowIso });
    }
    // 保持期間を過ぎた行を消す (受信時刻が分からない行は、最後に見た時刻で)
    const limit = new Date(now.getTime() - keepDays * 86400000).toISOString();
    return db.prepare(`DELETE FROM skipped_mail_log WHERE COALESCE(received_at, last_seen_at) < ?`).run(limit).changes;
  });
  const purged = tx();
  return { recorded: rows.length, purged };
}

/**
 * 直近 days 日の集計: 差出人のドメイン × 当たったルール ごとの件数と、件名の例 (3 つまで)。件数の多い順
 * @returns {{ days, total, groups: Array<{ fromDomain, ruleId, ruleName, count, lastReceivedAt, subjects: string[] }> }}
 */
export function summarizeSkippedMails({ days = 14, now = new Date(), limit = 200 } = {}) {
  const db = getDB();
  const d = Math.min(Math.max(Number(days) || 14, 1), SKIPPED_KEEP_DAYS);
  const since = new Date(now.getTime() - d * 86400000).toISOString();
  const groups = db.prepare(`
    SELECT from_domain AS fromDomain, rule_id AS ruleId, MAX(rule_name) AS ruleName, COUNT(*) AS count, MAX(received_at) AS lastReceivedAt
      FROM skipped_mail_log WHERE COALESCE(received_at, last_seen_at) >= ?
     GROUP BY from_domain, rule_id ORDER BY count DESC, from_domain LIMIT ?`).all(since, Math.min(Math.max(Number(limit) || 200, 1), 1000));
  const ex = db.prepare(`
    SELECT subject FROM skipped_mail_log WHERE COALESCE(received_at, last_seen_at) >= ? AND from_domain = ? AND rule_id IS ?
     GROUP BY subject ORDER BY COUNT(*) DESC, subject LIMIT 3`);
  for (const g of groups) g.subjects = ex.all(since, g.fromDomain, g.ruleId).map((r) => r.subject);
  const total = db.prepare(`SELECT COUNT(*) AS n FROM skipped_mail_log WHERE COALESCE(received_at, last_seen_at) >= ?`).get(since).n;
  return { days: d, total, groups };
}

/** 直近 days 日の全件 (CSV の出力用)。新しい順 */
export function listSkippedMails({ days = SKIPPED_KEEP_DAYS, now = new Date(), limit = 50000 } = {}) {
  const d = Math.min(Math.max(Number(days) || SKIPPED_KEEP_DAYS, 1), SKIPPED_KEEP_DAYS);
  const since = new Date(now.getTime() - d * 86400000).toISOString();
  return getDB().prepare(`SELECT received_at, from_address, from_domain, subject, rule_id, rule_name, seen_count FROM skipped_mail_log
    WHERE COALESCE(received_at, last_seen_at) >= ? ORDER BY COALESCE(received_at, last_seen_at) DESC LIMIT ?`).all(since, limit);
}

/** CSV (Excel で開ける = BOM つき・式として解釈されうる先頭の文字は ' で無害化) */
export function skippedMailsCsv(rows) {
  const cell = (v) => { let s = v == null ? '' : String(v); if (/^[=+\-@\t\r]/.test(s)) s = `'${s}`; return `"${s.replace(/"/g, '""')}"`; };
  const head = ['received_at', 'from_address', 'from_domain', 'subject', 'rule_id', 'rule_name', 'seen_count'];
  return String.fromCharCode(0xFEFF) + [head.join(','), ...rows.map((r) => head.map((h) => cell(r[h])).join(','))].join(String.fromCharCode(13, 10)) + String.fromCharCode(13, 10);
}
