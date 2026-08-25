/**
 * inquiry-hub 一括操作のバッチ記録・取り消し (2026-08-25 Codex議論の採用: 滞留整理の安全装置)
 *
 * 目的: 「誤った条件で大量に完了させた」を、削除やバックアップ復元ではなく
 *       バッチ単位の取り消しで戻せるようにする。6,400件の滞留整理を安心して進める土台。
 *
 * 対象 (1回の実行 = 1バッチ):
 *   - 一覧のチェック行への一括操作 (source='bulk')
 *   - 「この条件の全件を選択」 (source='bulk_filter')
 *   - メールルールの「すでに溜まっている同じメールにも適用」 (source='rule_apply')
 *
 * 取り消しの原則:
 *   - 変更したフィールドだけを before の値に戻す
 *   - **バッチ後に人が手で変えたフィールドは上書きしない** (現在値がバッチの after と
 *     一致するフィールドだけ戻す)。例: 一括完了→誰かが手動で「対応中」にした行のステータスは触らない
 *   - 取り消しは1バッチ1回だけ (再実行は成功扱いで何もしない=冪等)
 *   - 対象の問い合わせが後からアーカイブされていても、状態の巻き戻しは行う (削除はしないため安全)
 */
import { getDB, logActivity } from './db.js';

export const BATCH_SOURCE_LABELS = {
  bulk: '選択した行への一括操作',
  bulk_filter: 'この条件の全件への一括操作',
  rule_apply: 'メールルールの一括適用',
};

/** 取り消し時に「今もバッチの変更のままか」を確認するフィールド (= 巻き戻し対象) */
const MATCH_FIELDS = ['internal_status', 'folder_id', 'label_id', 'assigned_user_id', 'is_unread'];
/** before にだけ持つ付随フィールド (internal_status を戻すときに一緒に戻す。単独では戻さない) */
const CARRY_FIELDS = ['completed_at'];

/**
 * バッチを記録する。呼び出し元のトランザクション内で呼ぶこと (better-sqlite3 は同一接続の
 * 同期実行なので、db.transaction() の中から呼べばアトミックになる)。
 * @param {object} p { actor, source, ops, filter?, targetCount, items: [{inquiryId, before, after}] }
 * @returns {number|null} batchId (変更が0件ならバッチを作らず null)
 */
export function recordBulkBatch(db, { actor, source, ops, filter = null, targetCount, items }) {
  if (!BATCH_SOURCE_LABELS[source]) throw new Error(`不正なバッチ種別: ${source}`);
  if (!items || items.length === 0) return null;
  const r = db.prepare(`INSERT INTO bulk_batches (actor, source, ops_json, filter_json, target_count, changed_count)
    VALUES (?,?,?,?,?,?)`)
    .run(actor || null, source, JSON.stringify(ops ?? {}), filter == null ? null : JSON.stringify(filter),
      targetCount ?? items.length, items.length);
  const ins = db.prepare('INSERT INTO bulk_batch_items (batch_id, inquiry_id, before_json, after_json) VALUES (?,?,?,?)');
  for (const it of items) {
    ins.run(r.lastInsertRowid, it.inquiryId, JSON.stringify(it.before ?? {}), JSON.stringify(it.after ?? {}));
  }
  return r.lastInsertRowid;
}

/** 一括操作の履歴 (⚙️運用管理の表示用。新しい順) */
export function listBulkBatches({ limit = 30 } = {}) {
  return getDB().prepare('SELECT * FROM bulk_batches ORDER BY id DESC LIMIT ?').all(limit);
}

export function getBulkBatch(id) {
  if (!Number.isInteger(id)) return null;
  return getDB().prepare('SELECT * FROM bulk_batches WHERE id = ?').get(id) || null;
}

/** null/'' を同一視した比較 (assigned_user_id は '' と NULL が混在し得る) */
const same = (a, b) => String(a ?? '') === String(b ?? '');

/**
 * バッチを取り消す。
 * @returns {{ ok: true, alreadyReverted?: boolean, reverted: number, skipped: number }}
 *   reverted = 1フィールド以上戻した問い合わせ数 / skipped = 全フィールドが後から変更済みで触らなかった数
 */
export function revertBulkBatch(batchId, actorId = null) {
  const db = getDB();
  return db.transaction(() => {
    const batch = getBulkBatch(batchId);
    if (!batch) throw new Error('バッチが見つかりません');
    if (batch.reverted_at) return { ok: true, alreadyReverted: true, reverted: 0, skipped: 0 };

    const items = db.prepare('SELECT * FROM bulk_batch_items WHERE batch_id = ? ORDER BY id').all(batchId);
    const getInq = db.prepare('SELECT * FROM inquiries WHERE id = ?');
    const NOW = "strftime('%Y-%m-%dT%H:%M:%SZ','now')";
    let reverted = 0, skipped = 0;
    for (const it of items) {
      const cur = getInq.get(it.inquiry_id);
      if (!cur) { skipped++; continue; }
      let before, after;
      try { before = JSON.parse(it.before_json); after = JSON.parse(it.after_json); }
      catch { skipped++; continue; }

      const sets = [], params = [], restored = {};
      for (const f of MATCH_FIELDS) {
        if (!(f in after)) continue;                       // このバッチで触っていないフィールド
        if (!same(cur[f], after[f])) continue;             // 後から人が変えた → 上書きしない
        if (same(cur[f], before[f])) continue;             // すでに元の値 (変化なし)
        sets.push(`${f} = ?`);
        params.push(before[f] ?? null);
        restored[f] = before[f] ?? null;
        // ステータスを戻すときは completed_at も当時の値に揃える (done⇄未完了の整合)
        if (f === 'internal_status') {
          for (const c of CARRY_FIELDS) {
            if (c in before) { sets.push(`${c} = ?`); params.push(before[c] ?? null); }
          }
        }
      }
      if (!sets.length) { skipped++; continue; }
      db.prepare(`UPDATE inquiries SET ${sets.join(', ')}, updated_at = ${NOW} WHERE id = ?`)
        .run(...params, it.inquiry_id);
      logActivity(it.inquiry_id, { actorType: 'user', userId: actorId, actionType: 'bulk_revert',
        before: null, after: { batch: batchId, ...restored, reason: '一括操作の取り消し' } });
      reverted++;
    }
    db.prepare(`UPDATE bulk_batches SET reverted_at = ${NOW}, reverted_by = ? WHERE id = ?`)
      .run(actorId || null, batchId);
    return { ok: true, reverted, skipped };
  }).immediate();
}
