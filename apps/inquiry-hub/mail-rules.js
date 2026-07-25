/**
 * メール取込ルール (メールチャネルのノイズ除去) — 設計・経緯:
 *
 * メールディーラーの「振り分けの設定」858件のうち、実測 (mail_dist_20260718.csv):
 *   - ゴミ箱移動/即削除 = 758件 (88%) → action 'skip' (問い合わせとして取り込まない)
 *   - メール状態=対応完了(203) = 31件   → action 'import_done' (取り込むが対応完了扱い)
 *   - フォルダ振り分け等のみ = 残り     → inquiry-hub ではフォルダ運用をしないため移行対象外
 * 条件は from 784 / subject 457 / reply-to 56 / to 2 / body 1、マッチ方式は「含む」が96%。
 *
 * 評価は Gmail 同期アダプター (Step 3) が受信メールごとに evaluateMailRules() を呼ぶ。
 * priority 昇順・先勝ち。どのルールにも当たらなければ通常取込。
 *
 * ⚠️ skip ルールの過剰マッチは実問い合わせの取りこぼしになるため、
 *    CSV取込では確実に解釈できるルールだけを移行し、それ以外は理由付きでレポートする
 *    (取り込み漏れ側に倒す。ノイズが混ざるのは画面で見えるので安全)。
 */
import { getDB, logActivity } from './db.js';
import { parseCsv } from './templates.js';

export const RULE_FIELDS = ['from', 'to', 'reply_to', 'subject', 'body'];
export const RULE_OPS = ['contains', 'not_contains', 'equals', 'not_equals', 'starts_with', 'ends_with'];
export const RULE_ACTIONS = ['skip', 'import_done'];
const MAX_CONDITIONS = 20;

/** 条件配列の検証 (throw) */
export function validateConditions(conditions) {
  if (!Array.isArray(conditions) || conditions.length === 0) throw new Error('条件が空です');
  if (conditions.length > MAX_CONDITIONS) throw new Error(`条件が多すぎます (${MAX_CONDITIONS}個まで)`);
  for (const c of conditions) {
    if (!RULE_FIELDS.includes(c?.field)) throw new Error(`不正なフィールド: ${c?.field}`);
    if (!RULE_OPS.includes(c?.op)) throw new Error(`不正なマッチ方式: ${c?.op}`);
    const v = String(c?.value ?? '').trim();
    if (!v) throw new Error('条件の文字列が空です');
    if (v.length > 500) throw new Error('条件の文字列が長すぎます (500文字まで)');
  }
}

function condMatches(c, msg) {
  const target = String(msg[c.field] ?? '').toLowerCase();
  const v = String(c.value).toLowerCase();
  switch (c.op) {
    case 'contains': return target.includes(v);
    case 'not_contains': return !target.includes(v);
    case 'equals': return target === v;
    case 'not_equals': return target !== v;
    case 'starts_with': return target.startsWith(v);
    case 'ends_with': return target.endsWith(v);
    default: return false;
  }
}

/**
 * 受信メール1通をルールに通す。
 * @param {object} msg { from, to, reply_to, subject, body } (欠けたフィールドは空文字扱い)
 * @returns {{ action, ruleId, ruleName } | null} 最初に一致した有効ルール。無ければ null (=通常取込)
 */
/** 評価前のルール健全性チェック (壊れたルールは評価しない=fail-open。Codex R1 high:
 *  例外でGmail同期全体を止めない・不正field+not_containsの全メール一致によるskip過剰適用を防ぐ) */
function parseRuleSafe(r) {
  if (!RULE_ACTIONS.includes(r.action)) return null;
  if (!['all', 'any'].includes(r.match_mode)) return null;
  let conditions;
  try { conditions = JSON.parse(r.conditions_json); } catch { return null; }
  if (!Array.isArray(conditions) || conditions.length === 0 || conditions.length > MAX_CONDITIONS) return null;
  for (const c of conditions) {
    if (!c || typeof c !== 'object') return null;
    if (!RULE_FIELDS.includes(c.field) || !RULE_OPS.includes(c.op)) return null;
    if (typeof c.value !== 'string' || !c.value.trim()) return null;
  }
  return conditions;
}

export function evaluateMailRules(msg) {
  const db = getDB();
  const rules = db.prepare('SELECT * FROM mail_rules WHERE is_active = 1 ORDER BY priority ASC, id ASC').all();
  for (const r of rules) {
    const conditions = parseRuleSafe(r);
    if (!conditions) continue; // 壊れたルールは無視 (fail-open=通常取込)
    const hit = r.match_mode === 'any'
      ? conditions.some(c => condMatches(c, msg))
      : conditions.every(c => condMatches(c, msg));
    if (hit) return { action: r.action, ruleId: r.id, ruleName: r.name || null };
  }
  return null;
}

// ─── CRUD ───

export function listMailRules() {
  return getDB().prepare('SELECT * FROM mail_rules ORDER BY priority ASC, id ASC').all();
}

export function addMailRule({ name, matchMode = 'all', conditions, action, priority = 100 }) {
  if (!RULE_ACTIONS.includes(action)) throw new Error(`不正なアクション: ${action}`);
  if (!['all', 'any'].includes(matchMode)) throw new Error(`不正なmatch_mode: ${matchMode}`);
  if (!Number.isInteger(priority) || priority < 0 || priority > 100000) throw new Error('優先度は0〜100000の整数で指定してください');
  validateConditions(conditions);
  const r = getDB().prepare(`INSERT INTO mail_rules (priority, name, match_mode, conditions_json, action)
    VALUES (?,?,?,?,?)`).run(priority, String(name || '').slice(0, 200) || null, matchMode, JSON.stringify(conditions), action);
  return { id: r.lastInsertRowid };
}

/**
 * ルールを既存の取込済みメールにも適用する (2026-07-25 中原さん要望)。
 * ルールは本来「次回の取込」からしか効かないが、自動配信メールが既に大量に溜まっている
 * ため、同じ条件の既存分をまとめて片付けられるようにする。
 * ⚠️ 削除はしない (完了にするだけ = 受信トレイから外れるが履歴は残る)。
 * 対象はメールチャネルのみ・未完了のものだけ。
 *
 * @returns {{ matched: number, completed: number }}
 */
export function applyRuleToExistingMails(conditions, { matchMode = 'all', apply = false, actorId = 'portal' } = {}) {
  validateConditions(conditions);
  const db = getDB();
  // inquiries が持っているのは差出人 (customer_identifier) と件名。本文照合は重いので対象外とし、
  // from/reply_to/to は customer_identifier、subject は subject に対して評価する
  const clauses = [], params = [];
  for (const c of conditions) {
    const col = c.field === 'subject' ? 'i.subject' : 'i.customer_identifier';
    const v = String(c.value);
    switch (c.op) {
      case 'contains':      clauses.push(`LOWER(COALESCE(${col},'')) LIKE ? ESCAPE '\\'`); params.push(`%${likeEscLocal(v)}%`); break;
      case 'not_contains':  clauses.push(`LOWER(COALESCE(${col},'')) NOT LIKE ? ESCAPE '\\'`); params.push(`%${likeEscLocal(v)}%`); break;
      case 'equals':        clauses.push(`LOWER(COALESCE(${col},'')) = ?`); params.push(v.toLowerCase()); break;
      case 'not_equals':    clauses.push(`LOWER(COALESCE(${col},'')) != ?`); params.push(v.toLowerCase()); break;
      case 'starts_with':   clauses.push(`LOWER(COALESCE(${col},'')) LIKE ? ESCAPE '\\'`); params.push(`${likeEscLocal(v)}%`); break;
      case 'ends_with':     clauses.push(`LOWER(COALESCE(${col},'')) LIKE ? ESCAPE '\\'`); params.push(`%${likeEscLocal(v)}`); break;
      default: throw new Error(`不正なマッチ方式: ${c.op}`);
    }
  }
  const where = `i.channel_type = 'email' AND i.is_archived = 0 AND i.internal_status != 'done'
    AND (${clauses.join(matchMode === 'any' ? ' OR ' : ' AND ')})`;
  const matched = db.prepare(`SELECT COUNT(*) AS c FROM inquiries i WHERE ${where}`).get(...params).c;
  if (!apply) return { matched, completed: 0 };

  const rows = db.prepare(`SELECT i.id FROM inquiries i WHERE ${where}`).all(...params);
  const tx = db.transaction(() => {
    const upd = db.prepare(`UPDATE inquiries SET internal_status = 'done', is_unread = 0,
      completed_at = COALESCE(completed_at, strftime('%Y-%m-%dT%H:%M:%SZ','now')),
      updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = ? AND internal_status != 'done'`);
    for (const r of rows) {
      if (upd.run(r.id).changes > 0) {
        logActivity(r.id, { actorType: 'user', userId: actorId, actionType: 'status_change',
          after: { internal_status: 'done', reason: 'メールルールの一括適用' } });
      }
    }
  });
  tx.immediate();
  return { matched, completed: rows.length };
}
/** LIKE用エスケープ (小文字化込み。templates.js の likeEsc と同等) */
const likeEscLocal = s => String(s).toLowerCase().replace(/[\\%_]/g, c => '\\' + c);

export function setMailRuleActive(id, active) {
  const r = getDB().prepare(`UPDATE mail_rules SET is_active = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = ?`)
    .run(active ? 1 : 0, id);
  if (!r.changes) throw new Error('ルールが見つかりません');
}

export function deleteMailRule(id) {
  const r = getDB().prepare('DELETE FROM mail_rules WHERE id = ?').run(id);
  if (!r.changes) throw new Error('ルールが見つかりません');
}

// ─── メールディーラー振り分け設定CSVの取込 ───

// CSV列レイアウト (mail_dist_20260718.csv 実測。ヘッダー名で解決するので列順の多少の変化には耐える)
const FIELD_MAP = { 'from': 'from', 'to': 'to', 'reply-to': 'reply_to', 'subject': 'subject', 'body': 'body' };
// 範囲コード: メールディーラーの選択肢に対応 (実測は 1 が96%。未知コードのルールは移行しない)
const RANGE_MAP = { '1': 'contains', '2': 'not_contains', '3': 'equals', '4': 'not_equals', '5': 'starts_with', '6': 'ends_with' };

/**
 * メールディーラー「振り分けの設定」エクスポートCSV (Shift-JISをデコード済みのテキスト) を取り込む。
 * - 迷惑メール/ゴミ箱 = 1(ゴミ箱)/2(即削除) → skip
 * - 状態 = 203 (対応完了) → import_done
 * - 上記以外 (フォルダ振り分けのみ等) → 移行対象外
 * - and/or が混在する複合条件・未知の範囲コードは安全側に倒して移行しない (レポートに理由を出す)
 * external_key='maildealer:<条件ID>' で冪等 (再取込は既存を更新)
 */
export function importMailDealerRulesCsv(csvText, { apply = false } = {}) {
  const rows = parseCsv(String(csvText).replace(/^﻿/, '')); // UTF-8 BOM除去 (ヘッダー厳密一致のため)
  if (rows.length < 2) throw new Error('CSVにデータ行がありません');
  const header = rows[0];
  const col = name => header.findIndex(h => h === name);
  const idCol = col('条件ID'), prioCol = col('優先度'), nameCol = col('名称');
  const blockAndOrCol = col('条件ブロックのand/or'), trashCol = col('迷惑メール/ゴミ箱'), stateCol = col('状態');
  if (idCol < 0 || trashCol < 0 || stateCol < 0) {
    throw new Error('ヘッダーが想定形ではありません (メールディーラーの「振り分けの設定」エクスポートCSVを指定してください)');
  }
  const itemCols = [];
  for (let n = 1; n <= 20; n++) {
    const i = col(`項目${n}`), s = col(`文字列${n}`), r = col(`範囲${n}`), a = col(`条件のand/or${n}`), b = col(`条件ブロック番号${n}`);
    if (i >= 0 && s >= 0 && r >= 0) itemCols.push({ i, s, r, a, b });
  }

  const report = { total: 0, toSkip: 0, toImportDone: 0, notTarget: 0, unsupported: [], applied: 0, updated: 0 };
  const toUpsert = [];
  for (const row of rows.slice(1)) {
    if (!row[idCol]) continue;
    report.total++;
    const trash = String(row[trashCol] || '').trim();
    const state = String(row[stateCol] || '').trim();
    let action = null;
    if (trash === '1' || trash === '2') action = 'skip';
    else if (state === '203') action = 'import_done';
    if (!action) { report.notTarget++; continue; }

    const condId = String(row[idCol]).trim();
    const ruleName = String(row[nameCol] || '').trim();
    const conditions = [];
    const andOrs = new Set();
    const blocks = new Set();
    let bad = null;
    for (const c of itemCols) {
      const item = String(row[c.i] || '').trim().toLowerCase();
      if (!item) continue;
      const field = FIELD_MAP[item];
      const op = RANGE_MAP[String(row[c.r] || '').trim()];
      const value = String(row[c.s] || '').trim();
      if (!field) { bad = `未対応の条件項目 '${item}'`; break; }
      if (!op) { bad = `未対応の範囲コード '${row[c.r]}'`; break; }
      if (!value) { bad = '条件の文字列が空'; break; }
      conditions.push({ field, op, value });
      const ao = String(row[c.a] || '').trim().toLowerCase();
      if (ao) andOrs.add(ao);
      const bn = String(row[c.b] || '').trim();
      if (bn) blocks.add(bn);
    }
    if (!bad && conditions.length === 0) bad = '条件なし';
    // and/or 混在・複数ブロックは評価器の all/any では正しく再現できない → 安全側 (移行しない)
    if (!bad && andOrs.size > 1) bad = 'and/or が混在する複合条件';
    if (!bad && blocks.size > 1 && String(row[blockAndOrCol] || '').trim()) bad = '複数条件ブロック';
    if (bad) { report.unsupported.push({ condId, name: ruleName, reason: bad }); continue; }

    const matchMode = andOrs.has('or') ? 'any' : 'all';
    if (action === 'skip') report.toSkip++; else report.toImportDone++;
    toUpsert.push({
      externalKey: `maildealer:${condId}`,
      priority: Number(row[prioCol]) || 100,
      name: ruleName || null,
      matchMode, conditions, action,
    });
  }

  if (apply) {
    const db = getDB();
    const tx = db.transaction(() => {
      const upsert = db.prepare(`INSERT INTO mail_rules (priority, name, match_mode, conditions_json, action, external_key)
        VALUES (@priority, @name, @matchMode, @conditionsJson, @action, @externalKey)
        ON CONFLICT(external_key) WHERE external_key IS NOT NULL DO UPDATE SET
          priority = excluded.priority, name = excluded.name, match_mode = excluded.match_mode,
          conditions_json = excluded.conditions_json, action = excluded.action,
          updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now')`);
      for (const u of toUpsert) {
        const existed = db.prepare('SELECT id FROM mail_rules WHERE external_key = ?').get(u.externalKey);
        upsert.run({ ...u, conditionsJson: JSON.stringify(u.conditions) });
        if (existed) report.updated++; else report.applied++;
      }
    });
    tx.immediate();
  }
  return report;
}
