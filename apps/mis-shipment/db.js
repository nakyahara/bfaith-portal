/**
 * mis-shipment DB レイヤー
 *
 * warehouse-mirror.db の f_mis_shipments / f_mis_shipment_status_history を扱う。
 * 接続は warehouse-mirror の getMirrorDB() を共有 (PRAGMA も warehouse-mirror で
 * foreign_keys=ON / busy_timeout=5000 / recursive_triggers=ON が設定済み)。
 *
 * 設計書: g:/共有ドライブ/AI_reference/システム設計/誤出荷管理システム_設計書_v5.md (中身 v7.3)
 */
import crypto from 'node:crypto';
import { getMirrorDB, getMisFieldHistoryInitError } from '../warehouse-mirror/db.js';

// ─── JST 日付ユーティリティ (v7.2 仕様: Date.toISOString() / ローカル getFullYear 禁止) ───
const JST_DATE_FORMATTER = new Intl.DateTimeFormat('en-CA', {
  timeZone: 'Asia/Tokyo',
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
});
/** JST 日付 'YYYY-MM-DD' を返す (UTC 環境でも正しく動く)。 */
export function getJstDateString(date = new Date()) {
  return JST_DATE_FORMATTER.format(date);
}

/** ISO8601 UTC timestamp 'YYYY-MM-DDTHH:MM:SS.sssZ'。timestamps 用 (occurred_on とは別)。 */
export function utcIsoNow() {
  return new Date().toISOString();
}

// ─── payload_hash ヘルパー (server-side authoritative fetch 後の正規化値で計算) ───
/**
 * 正規化された payload を SHA-256 で hash 化して 64-hex 文字列を返す。
 * payload オブジェクトをキーソート JSON にしてから hash。
 */
export function computePayloadHash(payload) {
  const sorted = JSON.stringify(payload, Object.keys(payload).sort());
  return crypto.createHash('sha256').update(sorted).digest('hex');
}

// ─── 列定義 (server-side authoritative fetch 後の正規化 payload) ───
// クライアント送信のフォーム値ではなく、サーバ側で lookup 結果を再取得した正規化値。
const INSERT_COLS = [
  'client_submission_id', 'payload_hash', 'version',
  'occurred_on', 'reported_at',
  'mall_order_id', 'order_id_unknown', 'mall',
  'sku_snapshot', 'product_name_snapshot', 'ordered_qty_snapshot', 'order_date_snapshot',
  'lookup_source',
  'mis_type', 'qty_affected', 'loss_amount_jpy',
  'process_stage', 'root_cause_stage', 'root_cause_note',
  'mix_up_group_id',
  'status', 'reporter_note',
  'reported_by', 'created_at', 'updated_at', 'updated_by',
];

const PLACEHOLDERS = INSERT_COLS.map(() => '?').join(', ');
const INSERT_SQL = `INSERT INTO f_mis_shipments (${INSERT_COLS.join(', ')}) VALUES (${PLACEHOLDERS})`;
const HISTORY_INSERT_SQL = `
  INSERT INTO f_mis_shipment_status_history
    (mis_shipment_id, from_status, to_status, changed_by, changed_at, change_note)
  VALUES (?, ?, ?, ?, ?, ?)
`;

// ─── 項目訂正 (mis_type / process_stage) まわり ───
/**
 * 「誤出荷種別と発見工程が、選んだものではなく先頭の選択肢で登録されていた」不具合
 * (PR #1381 で修正) が直った時刻。これより前に登録された行は、この 2 列が当てにならない。
 *
 * マージは 2026-09-20 13:58 JST。Render のデプロイはその数分〜数十分後なので、
 * 余裕を持って JST 15:00 にしてある。多めに「要確認」を付ける方向 = 安全側。
 * (取りこぼすと嘘の値が黙って残る。余分に付いた分は「確認した」で消せる)
 */
export const FIELD_BUG_FIXED_AT = '2026-09-20T06:00:00.000Z';

const FIELD_HISTORY_INSERT_SQL = `
  INSERT INTO f_mis_shipment_field_history
    (mis_shipment_id, field_name, old_value, new_value, changed_by, changed_at)
  VALUES (?, ?, ?, ?, ?, ?)
`;

// 確認印は項目ごと。片方だけ直して、もう片方の嘘が黙って消えないようにする。
export const REVIEW_MARKER = { mis_type: 'review_mis_type', process_stage: 'review_process_stage' };

// 「要確認」= 不具合が直る前に登録された & 種別と工程のどちらかがまだ未確認
const NEEDS_FIELD_REVIEW_SQL = `
  created_at < ?
  AND EXISTS (
    SELECT 1 FROM (SELECT 'review_mis_type' AS marker UNION ALL SELECT 'review_process_stage') m
     WHERE NOT EXISTS (
       SELECT 1 FROM f_mis_shipment_field_history h
        WHERE h.mis_shipment_id = f_mis_shipments.id
          AND h.field_name = m.marker
     )
  )`;

// 訂正履歴テーブルの DDL は fail-soft なので、無い環境でも一覧が落ちないようにする。
// 接続が張り直されたら見直す (db インスタンスをキーに覚える)。
let fieldHistoryTableCache = null;
function hasFieldHistoryTable() {
  const db = getMirrorDB();
  if (fieldHistoryTableCache && fieldHistoryTableCache.db === db) return fieldHistoryTableCache.exists;
  const row = db.prepare(
    "SELECT 1 AS ok FROM sqlite_master WHERE type = 'table' AND name = 'f_mis_shipment_field_history'"
  ).get();
  fieldHistoryTableCache = { db, exists: !!row };
  return fieldHistoryTableCache.exists;
}

/**
 * 訂正履歴が書けるか。書けないなら訂正させない (履歴なしで黙って直さないため)。
 * 表があるだけでは足りない: 表は出来たが append-only trigger が出来ていない、という
 * 中途半端な状態だと「書き換えられる履歴」が残ってしまう。DDL が最後まで成功したかも見る。
 */
export function canCorrectFields() {
  return getMisFieldHistoryInitError() == null && hasFieldHistoryTable();
}

/**
 * 「要確認」の残り件数。
 * 数えられなかったときは 0 ではなく null を返す (取れなかったことを 0 と混ぜない)。
 */
export function countNeedsFieldReview() {
  const db = getMirrorDB();
  try {
    if (!hasFieldHistoryTable()) {
      const r = db.prepare(
        'SELECT COUNT(*) AS n FROM f_mis_shipments WHERE deleted_at IS NULL AND created_at < ?'
      ).get(FIELD_BUG_FIXED_AT);
      return r ? r.n : null;
    }
    const r = db.prepare(
      `SELECT COUNT(*) AS n FROM f_mis_shipments WHERE deleted_at IS NULL AND ${NEEDS_FIELD_REVIEW_SQL}`
    ).get(FIELD_BUG_FIXED_AT);
    return r ? r.n : null;
  } catch (e) {
    return null;
  }
}

/**
 * 「この値で間違いない」と管理者が確認した印を付ける。
 * レコード自体は変えないので version は進めないが、**表示していた version は照合する**。
 * そうしないと、古い画面を開いたままの人が「自分が見ていない値」を確認済みにできてしまう。
 */
export function markFieldReviewed(id, expectedVersion, reviewedBy) {
  const db = getMirrorDB();
  if (!canCorrectFields()) return { ok: false, reason: 'field_history_unavailable' };

  const tx = db.transaction(() => {
    const row = db.prepare(
      'SELECT id, version, mis_type, process_stage FROM f_mis_shipments WHERE id = ? AND deleted_at IS NULL'
    ).get(id);
    if (!row) return { ok: false, reason: 'not_found' };
    if (row.version !== expectedVersion) return { ok: false, reason: 'version_mismatch' };

    const already = new Set(db.prepare(`
      SELECT DISTINCT field_name FROM f_mis_shipment_field_history
       WHERE mis_shipment_id = ? AND field_name IN (?, ?)
    `).all(id, REVIEW_MARKER.mis_type, REVIEW_MARKER.process_stage).map((r) => r.field_name));

    const now = utcIsoNow();
    const ins = db.prepare(FIELD_HISTORY_INSERT_SQL);
    let marked = 0;
    for (const field of ['mis_type', 'process_stage']) {
      const marker = REVIEW_MARKER[field];
      if (already.has(marker)) continue;   // 既に確認済みの項目は二重に印を付けない
      ins.run(id, marker, row[field], row[field], reviewedBy, now);
      marked++;
    }
    return { ok: true, marked };
  });

  try {
    return tx();
  } catch (e) {
    console.error('[mis-shipment] markFieldReviewed 失敗:', e.message);
    return { ok: false, reason: 'field_history_unavailable' };
  }
}

// ─── 既存レコード参照 (冪等性チェック用) ───
export function findByClientSubmissionId(clientSubmissionId) {
  const db = getMirrorDB();
  return db.prepare('SELECT * FROM f_mis_shipments WHERE client_submission_id = ?')
           .get(clientSubmissionId);
}

// ─── 単独 INSERT (mix_up でない通常ケース) ───
/**
 * 単独レコードを挿入。INSERT-first + UNIQUE 違反 catch でレース耐性。
 *
 * 設計: 既存 client_submission_id の場合:
 *   - payload_hash 一致 → 既存返却 (idempotent retry、200 相当)
 *   - 不一致 → conflict (409 相当)
 *
 * 実装: SELECT 先行だと同時 2 リクエストで両方「無し」→ INSERT 競合 → 後者 500 になる。
 * これを避けるため INSERT を先に試み、UNIQUE 違反のときに既存を読み直して判定。
 * (Codex round 17 medium 指摘対応)
 *
 * @returns {{ inserted: true, id: number } | { inserted: false, conflict: true, existingId: number } | { inserted: false, idempotent: true, existing: object }}
 */
export function insertSingleMisShipment(record) {
  const db = getMirrorDB();
  const tx = db.transaction(() => {
    const values = INSERT_COLS.map((c) => record[c] ?? null);
    const info = db.prepare(INSERT_SQL).run(...values);
    const id = Number(info.lastInsertRowid);
    db.prepare(HISTORY_INSERT_SQL)
      .run(id, null, record.status ?? 'reported', record.reported_by, record.reported_at, null);
    return id;
  });

  try {
    const id = tx();
    return { inserted: true, id };
  } catch (e) {
    if (e && e.code === 'SQLITE_CONSTRAINT_UNIQUE') {
      // 同時送信レース or 再送。既存を読み直して hash 比較
      const existing = findByClientSubmissionId(record.client_submission_id);
      if (!existing) {
        // UNIQUE 違反だが見つからない (削除済み等) → conflict 扱い
        return { inserted: false, conflict: true, existingId: null };
      }
      if (existing.payload_hash === record.payload_hash) {
        return { inserted: false, idempotent: true, existing };
      }
      return { inserted: false, conflict: true, existingId: existing.id };
    }
    throw e;
  }
}

// ─── mix_up 同時 INSERT (2件 + 同じ mix_up_group_id) ───
/**
 * mix_up テレコの 2 件を 1 トランザクションで挿入。
 * 2 件は別々の client_submission_id を持つ。サーバが mix_up_group_id (UUID v4) を生成して両方に付与。
 * 個別の冪等性チェック: 既存に同じ client_submission_id があってハッシュも一致なら既存を返す (両方とも)。
 * どちらかが新規ならトランザクションで全件作成、片方だけ存在 (整合性破れ) はエラー。
 *
 * @param {object} recordA
 * @param {object} recordB
 * @returns {{ inserted: true, ids: [number, number], groupId: string } | { conflict: object }}
 */
export function insertMixUpMisShipments(recordA, recordB) {
  const db = getMirrorDB();
  const groupId = crypto.randomUUID();
  const recA = { ...recordA, mix_up_group_id: groupId };
  const recB = { ...recordB, mix_up_group_id: groupId };

  const tx = db.transaction(() => {
    const insertStmt = db.prepare(INSERT_SQL);
    const historyStmt = db.prepare(HISTORY_INSERT_SQL);

    const valuesA = INSERT_COLS.map((c) => recA[c] ?? null);
    const infoA = insertStmt.run(...valuesA);
    const idA = Number(infoA.lastInsertRowid);
    historyStmt.run(idA, null, recA.status ?? 'reported', recA.reported_by, recA.reported_at, null);

    const valuesB = INSERT_COLS.map((c) => recB[c] ?? null);
    const infoB = insertStmt.run(...valuesB);
    const idB = Number(infoB.lastInsertRowid);
    historyStmt.run(idB, null, recB.status ?? 'reported', recB.reported_by, recB.reported_at, null);

    return [idA, idB];
  });

  // INSERT-first + UNIQUE 違反 catch でレース耐性 (Codex round 17 medium 指摘対応)
  try {
    const [idA, idB] = tx();
    return { inserted: true, ids: [idA, idB], groupId };
  } catch (e) {
    if (e && e.code === 'SQLITE_CONSTRAINT_UNIQUE') {
      // 片方/両方が既に存在 → 個別に読み直して整合判定
      const existingA = findByClientSubmissionId(recordA.client_submission_id);
      const existingB = findByClientSubmissionId(recordB.client_submission_id);
      // 両方 + hash 一致 + 同じ group → idempotent
      if (existingA && existingB &&
          existingA.payload_hash === recordA.payload_hash &&
          existingB.payload_hash === recordB.payload_hash &&
          existingA.mix_up_group_id && existingA.mix_up_group_id === existingB.mix_up_group_id) {
        return {
          inserted: false,
          idempotent: true,
          ids: [existingA.id, existingB.id],
          groupId: existingA.mix_up_group_id,
        };
      }
      return {
        inserted: false,
        conflict: true,
        detail: { existingA: existingA?.id ?? null, existingB: existingB?.id ?? null },
      };
    }
    throw e;
  }
}

// ─── 詳細取得 (status history と mix_up 相方含む) ───
export function getMisShipmentDetail(id) {
  const db = getMirrorDB();
  const row = db.prepare('SELECT * FROM f_mis_shipments WHERE id = ? AND deleted_at IS NULL').get(id);
  if (!row) return null;

  const history = db.prepare(`
    SELECT id, from_status, to_status, changed_by, changed_at, change_note
      FROM f_mis_shipment_status_history
     WHERE mis_shipment_id = ?
     ORDER BY changed_at ASC, id ASC
  `).all(id);

  let related = [];
  if (row.mix_up_group_id) {
    related = db.prepare(`
      SELECT id, mall, mall_order_id, sku_snapshot, status, occurred_on
        FROM f_mis_shipments
       WHERE mix_up_group_id = ?
         AND id != ?
         AND deleted_at IS NULL
       ORDER BY id ASC
    `).all(row.mix_up_group_id, id);
  }

  // 項目の訂正履歴 (テーブルが無い環境では空で返す。一覧・詳細は落とさない)
  let fieldHistory = [];
  const reviewed = { mis_type: false, process_stage: false };
  if (hasFieldHistoryTable()) {
    fieldHistory = db.prepare(`
      SELECT id, field_name, old_value, new_value, changed_by, changed_at
        FROM f_mis_shipment_field_history
       WHERE mis_shipment_id = ?
       ORDER BY changed_at ASC, id ASC
    `).all(id);
    for (const h of fieldHistory) {
      if (h.field_name === REVIEW_MARKER.mis_type) reviewed.mis_type = true;
      if (h.field_name === REVIEW_MARKER.process_stage) reviewed.process_stage = true;
    }
  }
  // 不具合が直る前に登録されていて、種別と工程のどちらかがまだ未確認
  const inBuggyPeriod = row.created_at < FIELD_BUG_FIXED_AT;
  const needsFieldReview = inBuggyPeriod && !(reviewed.mis_type && reviewed.process_stage);

  return {
    row, history, related, fieldHistory,
    needsFieldReview,
    fieldReviewed: reviewed,
    canCorrectFields: canCorrectFields(),
  };
}

// ─── 一覧 (フィルタ) ───
export function listMisShipments({ mall, status, fromDate, toDate, processStage, rootCauseStage, q, needsFieldReview, limit = 100, offset = 0 } = {}) {
  const db = getMirrorDB();
  // 行ごとに「種別・工程が当てにならないか」を一緒に返す (一覧に ⚠️ を出すため)。
  // 訂正履歴テーブルが無い環境では「直った時刻より前」だけで判定する (多めに出す方向)。
  const reviewExpr = hasFieldHistoryTable()
    ? `CASE WHEN ${NEEDS_FIELD_REVIEW_SQL} THEN 1 ELSE 0 END`
    : 'CASE WHEN created_at < ? THEN 1 ELSE 0 END';
  let sql = `SELECT *, (${reviewExpr}) AS needs_field_review FROM f_mis_shipments WHERE deleted_at IS NULL`;
  const params = [FIELD_BUG_FIXED_AT];
  if (mall) { sql += ' AND mall = ?'; params.push(mall); }
  if (status) { sql += ' AND status = ?'; params.push(status); }
  if (fromDate) { sql += ' AND occurred_on >= ?'; params.push(fromDate); }
  if (toDate) { sql += ' AND occurred_on <= ?'; params.push(toDate); }
  if (processStage) { sql += ' AND process_stage = ?'; params.push(processStage); }
  if (rootCauseStage) { sql += ' AND root_cause_stage = ?'; params.push(rootCauseStage); }
  if (q) {
    // 一覧の検索窓 (注文番号 / SKU / 商品名)。
    // LIKE のワイルドカード (% _) とエスケープ文字自体は打ち消しておく。
    // そうしないと「%」1 文字の検索が全件一致になり、絞ったつもりで絞れていない。
    // エスケープ文字は JS/SQL の両方で書きやすい '~' を使う。
    const like = '%' + String(q).replace(/[~%_]/g, (c) => '~' + c) + '%';
    sql += " AND (mall_order_id LIKE ? ESCAPE '~'"
         + " OR sku_snapshot LIKE ? ESCAPE '~'"
         + " OR product_name_snapshot LIKE ? ESCAPE '~')";
    params.push(like, like, like);
  }
  if (needsFieldReview) {
    // 種別・工程が当てにならない行だけ。訂正履歴テーブルが無い環境では
    // 「直った時刻より前」だけで絞る (多めに出す方向 = 安全側)
    sql += hasFieldHistoryTable()
      ? ` AND ${NEEDS_FIELD_REVIEW_SQL}`
      : ' AND created_at < ?';
    params.push(FIELD_BUG_FIXED_AT);
  }
  sql += ' ORDER BY occurred_on DESC, id DESC LIMIT ? OFFSET ?';
  params.push(limit, offset);
  return db.prepare(sql).all(...params);
}

// ─── 楽観ロック付き status 遷移 + history 記録 ───
/**
 * status 遷移を 1 トランザクションで行う。
 * UPDATE は version チェック + version++、history INSERT を併せて実行。
 * @returns {{ ok: true } | { ok: false, reason: 'version_mismatch' | 'not_found' | 'invalid_transition' | 'root_cause_required' }}
 */
export function transitionStatus(id, expectedVersion, newStatus, changedBy, changeNote = null) {
  const db = getMirrorDB();

  const tx = db.transaction(() => {
    const current = db.prepare('SELECT id, version, status, root_cause_stage FROM f_mis_shipments WHERE id = ? AND deleted_at IS NULL').get(id);
    if (!current) return { ok: false, reason: 'not_found' };
    if (current.version !== expectedVersion) return { ok: false, reason: 'version_mismatch', currentVersion: current.version };

    if (!isValidTransition(current.status, newStatus)) {
      return { ok: false, reason: 'invalid_transition', from: current.status, to: newStatus };
    }

    // 設計書 §6 業務ルール 3: resolved/closed への遷移は root_cause_stage 確定が前提
    // (Codex round 17 high 指摘対応)
    if ((newStatus === 'resolved' || newStatus === 'closed') && current.root_cause_stage === 'unknown') {
      return { ok: false, reason: 'root_cause_required', currentRootCause: current.root_cause_stage };
    }

    const now = utcIsoNow();
    const updated = db.prepare(`
      UPDATE f_mis_shipments
         SET status = ?, version = version + 1, updated_at = ?, updated_by = ?
       WHERE id = ? AND version = ? AND deleted_at IS NULL
    `).run(newStatus, now, changedBy, id, expectedVersion);

    if (updated.changes !== 1) return { ok: false, reason: 'version_mismatch' };

    db.prepare(HISTORY_INSERT_SQL).run(id, current.status, newStatus, changedBy, now, changeNote);
    return { ok: true };
  });
  return tx();
}

const VALID_TRANSITIONS = {
  reported:      ['investigating'],
  investigating: ['resolved'],
  resolved:      ['investigating', 'closed'],  // resolved → investigating は管理者が調査再開
  closed:        [],  // terminal
};
function isValidTransition(from, to) {
  if (from === to) return false;
  return (VALID_TRANSITIONS[from] || []).includes(to);
}
export { VALID_TRANSITIONS };

// ─── 楽観ロック付き reporter_note / root_cause_stage / root_cause_note 更新 ───
/**
 * 設計書 §6 業務ルール 3: resolved/closed の レコードは root_cause_stage='unknown' に戻せない
 * (Codex round 18 high 指摘対応: 三層防御で patchEditableFields でも禁止)
 */
export function patchEditableFields(id, expectedVersion, fields, updatedBy) {
  // mis_type / process_stage は設計書 v7.3 では「起票時に確定、編集不可」だった。
  // 2026-09-20 に「画面で何を選んでも先頭の選択肢が保存されていた」不具合が見つかったため、
  // 管理者に限って直せるようにした (router 側で admin を確認)。
  // 直した内容は f_mis_shipment_field_history に必ず残す。
  const allowed = ['reporter_note', 'root_cause_stage', 'root_cause_note', 'mis_type', 'process_stage'];
  const correctable = ['mis_type', 'process_stage'];

  const db = getMirrorDB();
  const touchesCorrectable = correctable.some((k) => Object.prototype.hasOwnProperty.call(fields, k));
  if (touchesCorrectable && !canCorrectFields()) {
    // 履歴が書けないなら直させない (誰がいつ何を変えたか分からない訂正を残さない)
    return { ok: false, reason: 'field_history_unavailable' };
  }

  const tx = db.transaction(() => {
    const current = db.prepare('SELECT * FROM f_mis_shipments WHERE id = ? AND deleted_at IS NULL').get(id);
    if (!current) return { ok: false, reason: 'version_mismatch_or_not_found' };
    if (current.version !== expectedVersion) return { ok: false, reason: 'version_mismatch_or_not_found' };

    // status-aware な禁止: root_cause_stage を 'unknown' に戻す変更は、
    // 現在 status が resolved/closed なら拒否 (Codex round 18 high 指摘対応)
    if (Object.prototype.hasOwnProperty.call(fields, 'root_cause_stage')
        && fields.root_cause_stage === 'unknown'
        && (current.status === 'resolved' || current.status === 'closed')) {
      return { ok: false, reason: 'root_cause_unknown_forbidden_after_resolve' };
    }

    // テレコかどうかは mix_up_group_id と対で CHECK 制約になっている
    // (mis_type='mix_up' ⇔ mix_up_group_id IS NOT NULL)。
    // mis_type だけ動かすと制約違反で落ちるので、ここで止める。
    if (Object.prototype.hasOwnProperty.call(fields, 'mis_type')) {
      const wasMixUp = current.mis_type === 'mix_up';
      const willBeMixUp = fields.mis_type === 'mix_up';
      if (wasMixUp !== willBeMixUp) return { ok: false, reason: 'mix_up_type_locked' };
    }

    const sets = [];
    const vals = [];
    const changed = [];
    for (const k of allowed) {
      if (!Object.prototype.hasOwnProperty.call(fields, k)) continue;
      const next = fields[k] ?? null;
      if ((current[k] ?? null) === next) continue;   // 値が同じなら書かない
      sets.push(`${k} = ?`);
      vals.push(next);
      changed.push({ field: k, from: current[k] ?? null, to: next });
    }

    const now = utcIsoNow();

    if (sets.length > 0) {
      sets.push('updated_at = ?', 'updated_by = ?', 'version = version + 1');
      vals.push(now, updatedBy);
      vals.push(id, expectedVersion);
      const result = db.prepare(`
        UPDATE f_mis_shipments
           SET ${sets.join(', ')}
         WHERE id = ? AND version = ? AND deleted_at IS NULL
      `).run(...vals);
      if (result.changes !== 1) return { ok: false, reason: 'version_mismatch_or_not_found' };

      if (hasFieldHistoryTable()) {
        const ins = db.prepare(FIELD_HISTORY_INSERT_SQL);
        for (const c of changed) {
          ins.run(id, c.field, c.from == null ? null : String(c.from), c.to == null ? null : String(c.to), updatedBy, now);
        }
        // 直した項目だけ「確認した」印を付ける。
        // 種別を直しただけで工程まで確認済みにすると、残った嘘が黙って一覧から消える。
        for (const c of changed) {
          if (!correctable.includes(c.field)) continue;
          const marker = REVIEW_MARKER[c.field];
          const already = db.prepare(
            'SELECT 1 AS ok FROM f_mis_shipment_field_history WHERE mis_shipment_id = ? AND field_name = ? LIMIT 1'
          ).get(id, marker);
          if (!already) ins.run(id, marker, c.from == null ? null : String(c.from), String(c.to), updatedBy, now);
        }
      }
    }

    // 何も変わらなかった場合も成功扱い (同じ値で保存を押しただけ)。
    // version を無駄に進めない。
    return { ok: true, changed: changed.length };
  });

  try {
    return tx();
  } catch (e) {
    // 訂正履歴が書けないとトランザクションごと巻き戻る (履歴なしの訂正は残さない)
    console.error('[mis-shipment] patchEditableFields 失敗:', e.message);
    return { ok: false, reason: 'field_history_unavailable' };
  }
}

// ─── 論理削除 (管理者のみ、楽観ロック付き) ───
export function softDelete(id, expectedVersion, deletedBy) {
  const db = getMirrorDB();
  const now = utcIsoNow();
  const result = db.prepare(`
    UPDATE f_mis_shipments
       SET deleted_at = ?, deleted_by = ?, version = version + 1
     WHERE id = ? AND version = ? AND deleted_at IS NULL
  `).run(now, deletedBy, id, expectedVersion);
  if (result.changes !== 1) return { ok: false, reason: 'version_mismatch_or_not_found' };
  return { ok: true };
}
