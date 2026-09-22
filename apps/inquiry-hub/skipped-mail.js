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
 *   1. **同期を止めない**。記録は補助。失敗しても Gmail 同期は続ける (呼ぶ側が try/catch して警告を出す)。
 *      呼ばれ方は同期関数 (better-sqlite3)。差し替える側も同期関数にする (Promise の reject は呼ぶ側が拾えない)
 *   2. **本文を持たない**。件名は 200 文字で切る
 *   3. **同じスレッドは 1 行**。同期は同じ窓を何度も読み直すので、スレッド ID で上書きし、見た回数と最後に見た時刻を進める
 *      (件数を「同期の回数」で水増ししない)
 *   4. **期間の判定は activity_at** = そのスレッドのいちばん新しい受信時刻。ルールの判定に使うのはスレッドの「最初の受信メッセージ」(差出人・件名・received_at) だが、
 *      それで期間を切ると、古いスレッドに今日の返信が付いて skip された記録が、画面に出ないまま消える (Codex #1400 R1)
 *   5. 保持は activity_at から 30 日。**記録を呼ぶたびに** (skip が 0 件の回も) 古い行を消す = 新しい定期実行は要らない。
 *      同期が止まっている間は消えないが、画面・CSV は期間で切るので見えない
 *   6. これは「過去に skip した観測」の記録。後からルールを変えて取り込まれるようになったスレッドの行も、30 日は残る (= いま未取込の一覧ではない)
 *   7. 集計の鍵は「差出人のドメイン × 当たったルール」。件名は **数字を # に寄せた型** で数え (注文番号つきの件名が 1 通ずつ別の型にならない)、
 *      多い型 3 つ + **まれな型 2 つ** を例に出す (探しているのは、多数派に埋もれた少数の通知)
 */
import { getDB } from './db.js';

export const SKIPPED_KEEP_DAYS = 30;
export const SUBJECT_MAX = 200;
export const SUMMARY_GROUP_LIMIT = 100;
const FROM_MAX = 254;

const clip = (v, n) => String(v ?? '').replace(/[\r\n\t]+/g, ' ').trim().slice(0, n);
export const domainOf = (addr) => { const s = String(addr || '').toLowerCase(); const i = s.lastIndexOf('@'); return i >= 0 ? s.slice(i + 1) : ''; };
/** 受信時刻 (ms) → ISO。読めない・範囲の外 (Date が表せない値は toISOString が例外 = 同じバッチの正常な行まで巻き戻る)・**未来** (時計のずれ 1 日まで) は「分からない」= null。
 *  未来の時刻をそのまま使うと、期間の判定 (activity_at) が先へ延びて 30 日を過ぎても消えない (Codex #1400 R2) */
const FUTURE_SLACK_MS = 86400000;
const iso = (ms, nowMs) => (Number.isFinite(ms) && ms > 0 && ms <= 8.64e15 && ms <= nowMs + FUTURE_SLACK_MS ? new Date(ms).toISOString() : null);
/** 件名の型: 数字の並びを # に (注文番号・日付・金額の違いで別の型にしない)。全角の数字も */
export const subjectPattern = (s) => String(s || '').normalize('NFKC').replace(/\d+/g, '#').replace(/\s+/g, ' ').trim();

/**
 * skip になったスレッドを記録し、保持期間を過ぎた行を消す。**空の配列でも呼ぶ** (古い行を消すため)。
 * @param {Array<{ threadId, from, subject, receivedAtMs, latestReceivedAtMs, ruleId, ruleName }>} list
 *   receivedAtMs = ルールの判定に使ったメッセージの受信時刻 / latestReceivedAtMs = スレッドのいちばん新しい受信時刻 (無ければ receivedAtMs)
 * @returns {{ recorded: number, purged: number }}
 */
export function recordSkippedMails(list, { now = new Date(), keepDays = SKIPPED_KEEP_DAYS } = {}) {
  const db = getDB();
  const rows = (Array.isArray(list) ? list : []).filter((x) => x && x.threadId);
  const nowIso = now.toISOString();
  const up = db.prepare(`
    INSERT INTO skipped_mail_log (thread_id, from_address, from_domain, subject, received_at, activity_at, rule_id, rule_name, first_seen_at, last_seen_at, seen_count)
    VALUES (@thread_id, @from_address, @from_domain, @subject, @received_at, COALESCE(@activity_at, @now), @rule_id, @rule_name, @now, @now, 1)
    ON CONFLICT(thread_id) DO UPDATE SET
      from_address = excluded.from_address, from_domain = excluded.from_domain, subject = excluded.subject, received_at = excluded.received_at,
      -- 受信時刻が分からない再観測で、保持期限を延ばさない (分かっている時刻を保つ)
      activity_at = COALESCE(@activity_at, skipped_mail_log.activity_at),
      rule_id = excluded.rule_id, rule_name = excluded.rule_name, last_seen_at = excluded.last_seen_at, seen_count = skipped_mail_log.seen_count + 1`);
  const tx = db.transaction(() => {
    for (const x of rows) {
      const from = clip(x.from, FROM_MAX).toLowerCase();
      const received = iso(Number(x.receivedAtMs), now.getTime());
      up.run({ thread_id: String(x.threadId).slice(0, 64), from_address: from, from_domain: domainOf(from), subject: clip(x.subject, SUBJECT_MAX),
        received_at: received, activity_at: iso(Number(x.latestReceivedAtMs), now.getTime()) || received, rule_id: Number.isInteger(x.ruleId) ? x.ruleId : null, rule_name: clip(x.ruleName, 120) || null, now: nowIso });
    }
    const limit = new Date(now.getTime() - keepDays * 86400000).toISOString();
    return db.prepare(`DELETE FROM skipped_mail_log WHERE activity_at < ?`).run(limit).changes;
  });
  const purged = tx();
  return { recorded: rows.length, purged };
}

/**
 * 直近 days 日の集計: 差出人のドメイン × 当たったルール ごとの件数と、件名の型の例。件数の多い順
 * @returns {{ days, total, groupCount, shownGroups, groups: Array<{ fromDomain, ruleId, ruleName, count, lastActivityAt, patternCount, subjects: string[], rareSubjects: string[] }> }}
 *   subjects = 多い型 3 つ / rareSubjects = まれな型 2 つ (型が 4 つ以上あるとき。少ない順)。groups は上位 limit 件 (total・groupCount は全体)
 */
export function summarizeSkippedMails({ days = 14, now = new Date(), limit = SUMMARY_GROUP_LIMIT } = {}) {
  const d = Math.min(Math.max(Number(days) || 14, 1), SKIPPED_KEEP_DAYS);
  const since = new Date(now.getTime() - d * 86400000).toISOString();
  const byGroup = new Map();
  let total = 0;
  for (const r of getDB().prepare(`SELECT from_domain, rule_id, rule_name, subject, activity_at FROM skipped_mail_log WHERE activity_at >= ? ORDER BY activity_at DESC`).iterate(since)) {
    total++;
    const key = JSON.stringify([r.from_domain, r.rule_id ?? null]);
    let g = byGroup.get(key);
    // 新しい順に読むので、最初に見た行がそのグループのいちばん新しい行 = ルール名と最後の受信はそこから取る
    if (!g) { g = { fromDomain: r.from_domain, ruleId: r.rule_id, ruleName: r.rule_name, count: 0, lastActivityAt: r.activity_at, patterns: new Map() }; byGroup.set(key, g); }
    g.count++;
    const p = subjectPattern(r.subject);
    const cur = g.patterns.get(p);
    if (cur) cur.n++; else g.patterns.set(p, { n: 1, example: r.subject });
  }
  const all = [...byGroup.values()].sort((a, b) => b.count - a.count || a.fromDomain.localeCompare(b.fromDomain));
  const lim = Math.min(Math.max(Number(limit) || SUMMARY_GROUP_LIMIT, 1), 1000);
  const groups = all.slice(0, lim).map((g) => {
    const pats = [...g.patterns.values()].sort((a, b) => b.n - a.n || a.example.localeCompare(b.example));
    const top = pats.slice(0, 3), rare = pats.length > 3 ? pats.slice(3).sort((a, b) => a.n - b.n || a.example.localeCompare(b.example)).slice(0, 2) : [];
    return { fromDomain: g.fromDomain, ruleId: g.ruleId, ruleName: g.ruleName, count: g.count, lastActivityAt: g.lastActivityAt, patternCount: pats.length,
      subjects: top.map((x) => x.example), rareSubjects: rare.map((x) => x.example) };
  });
  return { days: d, total, groupCount: all.length, shownGroups: groups.length, groups };
}

/** 直近 days 日の全件 (CSV の出力用)。新しい順。**上限で切らない** (「全件」と言って黙って切らない。Codex #1400 R1) */
export function listSkippedMails({ days = SKIPPED_KEEP_DAYS, now = new Date() } = {}) {
  const d = Math.min(Math.max(Number(days) || SKIPPED_KEEP_DAYS, 1), SKIPPED_KEEP_DAYS);
  const since = new Date(now.getTime() - d * 86400000).toISOString();
  return getDB().prepare(`SELECT activity_at, received_at, from_address, from_domain, subject, rule_id, rule_name, seen_count FROM skipped_mail_log
    WHERE activity_at >= ? ORDER BY activity_at DESC, thread_id`).all(since);
}

/** CSV (Excel で開ける = BOM つき・式として解釈されうる先頭の文字は ' で無害化) */
export function skippedMailsCsv(rows) {
  const cell = (v) => { let s = v == null ? '' : String(v); if (/^[=+\-@\t\r]/.test(s)) s = `'${s}`; return `"${s.replace(/"/g, '""')}"`; };
  const head = ['activity_at', 'received_at', 'from_address', 'from_domain', 'subject', 'rule_id', 'rule_name', 'seen_count'];
  return String.fromCharCode(0xFEFF) + [head.join(','), ...rows.map((r) => head.map((h) => cell(r[h])).join(','))].join(String.fromCharCode(13, 10)) + String.fromCharCode(13, 10);
}
