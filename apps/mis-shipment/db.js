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
import { getMirrorDB } from '../warehouse-mirror/db.js';

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

  return { row, history, related };
}

// ─── 一覧 (フィルタ) ───
export function listMisShipments({ mall, status, fromDate, toDate, processStage, rootCauseStage, limit = 100, offset = 0 } = {}) {
  const db = getMirrorDB();
  let sql = 'SELECT * FROM f_mis_shipments WHERE deleted_at IS NULL';
  const params = [];
  if (mall) { sql += ' AND mall = ?'; params.push(mall); }
  if (status) { sql += ' AND status = ?'; params.push(status); }
  if (fromDate) { sql += ' AND occurred_on >= ?'; params.push(fromDate); }
  if (toDate) { sql += ' AND occurred_on <= ?'; params.push(toDate); }
  if (processStage) { sql += ' AND process_stage = ?'; params.push(processStage); }
  if (rootCauseStage) { sql += ' AND root_cause_stage = ?'; params.push(rootCauseStage); }
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
export function patchEditableFields(id, expectedVersion, fields, updatedBy) {
  const allowed = ['reporter_note', 'root_cause_stage', 'root_cause_note'];
  const sets = [];
  const vals = [];
  for (const k of allowed) {
    if (Object.prototype.hasOwnProperty.call(fields, k)) {
      sets.push(`${k} = ?`);
      vals.push(fields[k]);
    }
  }
  if (sets.length === 0) return { ok: false, reason: 'no_fields' };

  const db = getMirrorDB();
  const now = utcIsoNow();
  sets.push('updated_at = ?', 'updated_by = ?', 'version = version + 1');
  vals.push(now, updatedBy);
  vals.push(id, expectedVersion);

  const result = db.prepare(`
    UPDATE f_mis_shipments
       SET ${sets.join(', ')}
     WHERE id = ? AND version = ? AND deleted_at IS NULL
  `).run(...vals);

  if (result.changes !== 1) return { ok: false, reason: 'version_mismatch_or_not_found' };
  return { ok: true };
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
