/**
 * Phase E-7-c: 移行除外管理 API (履歴型、 Codex R1/R2 確定)。
 *
 * 提供:
 *   - excludeMigrationItem({ db, itemCode, excludeKind, reason, actor })
 *   - restoreMigrationExclusion({ db, itemCode, reason, actor })
 *   - changeExclusionKind({ db, itemCode, newKind, reason, actor })   — 復元+新規除外を 1 transaction で
 *   - listActiveExclusions(db, { kind?, search? })
 *   - listExclusionHistory(db, itemCode)
 *   - countActiveByKind(db)
 *
 * 設計原則:
 *   - 履歴型: 全イベント (除外 / 復元 / 種別変更) を 1 行ずつ追加、 active 1 行制約は partial UNIQUE INDEX で
 *   - excluded_by 'manual' default、 caller が actor を渡せば上書き
 *   - 種別変更は restore (現 active) + insert (新 kind) の 2 step を 1 transaction で
 *   - UI 表示文言は呼び出し側で翻訳 (このモジュールは英語 enum のまま返す)
 */

const ALLOWED_KINDS = ['temporary', 'permanent'];

function assertKind(kind) {
  if (!ALLOWED_KINDS.includes(kind)) {
    throw new Error(`exclusion: invalid exclude_kind "${kind}" (allowed: ${ALLOWED_KINDS.join(', ')})`);
  }
}

function assertItemCode(itemCode) {
  if (typeof itemCode !== 'string' || itemCode.trim() === '') {
    throw new Error(`exclusion: itemCode is required (got ${typeof itemCode})`);
  }
}

function assertReason(reason) {
  if (typeof reason !== 'string' || reason.trim() === '') {
    throw new Error(`exclusion: reason is required (non-empty string)`);
  }
}

function isoNow() {
  return new Date().toISOString();
}

/**
 * 商品を除外する。 既に active な除外がある場合は throw (種別変更は changeExclusionKind を使う)。
 */
function excludeMigrationItem({ db, itemCode, excludeKind, reason, actor = 'manual' }) {
  assertItemCode(itemCode);
  assertKind(excludeKind);
  assertReason(reason);
  const code = itemCode.trim();
  const trimmedReason = reason.trim();

  // partial UNIQUE INDEX で重複 active は INSERT 段階で UNIQUE constraint violation になるが、
  // エラーメッセージを判りやすくするため事前 check も実施。
  const active = db.prepare(
    'SELECT id, exclude_kind FROM migration_excluded WHERE item_code = ? AND restored_at IS NULL'
  ).get(code);
  if (active) {
    const err = new Error(
      `exclusion: item_code="${code}" already has active exclusion (id=${active.id}, kind=${active.exclude_kind}). ` +
      `Use changeExclusionKind to switch kind, or restoreMigrationExclusion to clear.`
    );
    err.code = 'EXCLUSION_ALREADY_ACTIVE';
    throw err;
  }
  const r = db.prepare(`
    INSERT INTO migration_excluded (item_code, exclude_kind, reason, excluded_at, excluded_by)
    VALUES (?, ?, ?, ?, ?)
  `).run(code, excludeKind, trimmedReason, isoNow(), actor);
  return { id: r.lastInsertRowid, item_code: code, exclude_kind: excludeKind, reason: trimmedReason };
}

/**
 * active 除外を解除 (復元)。 active がなければ throw。
 */
function restoreMigrationExclusion({ db, itemCode, reason = null, actor = 'manual' }) {
  assertItemCode(itemCode);
  const code = itemCode.trim();
  const trimmedReason = reason == null ? null : String(reason).trim();

  const active = db.prepare(
    'SELECT id, exclude_kind FROM migration_excluded WHERE item_code = ? AND restored_at IS NULL'
  ).get(code);
  if (!active) {
    const err = new Error(`exclusion: no active exclusion for item_code="${code}"`);
    err.code = 'EXCLUSION_NOT_ACTIVE';
    throw err;
  }
  db.prepare(`
    UPDATE migration_excluded
    SET restored_at = ?, restored_reason = ?, restored_by = ?
    WHERE id = ? AND restored_at IS NULL
  `).run(isoNow(), trimmedReason, actor, active.id);
  return { restored_id: active.id, item_code: code, prior_kind: active.exclude_kind };
}

/**
 * 種別変更 (例: temporary → permanent)。 「復元 + 新規除外」 を 1 transaction で。
 * 現 active 種別と newKind が同じ場合は throw (操作意図不明)。
 */
function changeExclusionKind({ db, itemCode, newKind, reason, actor = 'manual' }) {
  assertItemCode(itemCode);
  assertKind(newKind);
  assertReason(reason);
  const code = itemCode.trim();
  const trimmedReason = reason.trim();

  const tx = db.transaction(() => {
    const active = db.prepare(
      'SELECT id, exclude_kind FROM migration_excluded WHERE item_code = ? AND restored_at IS NULL'
    ).get(code);
    if (!active) {
      const err = new Error(`exclusion: changeKind requires active exclusion for item_code="${code}"`);
      err.code = 'EXCLUSION_NOT_ACTIVE';
      throw err;
    }
    if (active.exclude_kind === newKind) {
      const err = new Error(
        `exclusion: changeKind no-op (current=${active.exclude_kind}, new=${newKind}) for item_code="${code}"`
      );
      err.code = 'EXCLUSION_KIND_UNCHANGED';
      throw err;
    }
    db.prepare(`
      UPDATE migration_excluded
      SET restored_at = ?, restored_reason = ?, restored_by = ?
      WHERE id = ? AND restored_at IS NULL
    `).run(isoNow(), `kind change to ${newKind}: ${trimmedReason}`, actor, active.id);
    const r = db.prepare(`
      INSERT INTO migration_excluded (item_code, exclude_kind, reason, excluded_at, excluded_by)
      VALUES (?, ?, ?, ?, ?)
    `).run(code, newKind, trimmedReason, isoNow(), actor);
    return {
      restored_id: active.id,
      new_id: r.lastInsertRowid,
      item_code: code,
      prior_kind: active.exclude_kind,
      new_kind: newKind,
    };
  });
  return tx();
}

/**
 * active 除外一覧。 kind / search で絞り込み可。
 *   返却順: excluded_at DESC (最新が先)
 */
function listActiveExclusions(db, { kind = null, search = '', limit = 1000 } = {}) {
  const params = [];
  let sql = `
    SELECT id, item_code, exclude_kind, reason, excluded_at, excluded_by
    FROM migration_excluded
    WHERE restored_at IS NULL
  `;
  if (kind != null) {
    assertKind(kind);
    sql += ' AND exclude_kind = ?';
    params.push(kind);
  }
  if (search && search.trim() !== '') {
    sql += ' AND (item_code LIKE ? OR reason LIKE ?)';
    const like = `%${search.trim()}%`;
    params.push(like, like);
  }
  sql += ' ORDER BY excluded_at DESC LIMIT ?';
  params.push(limit);
  return db.prepare(sql).all(...params);
}

/**
 * 1 商品の全履歴 (active + restored、 古い → 新しい順)。
 */
function listExclusionHistory(db, itemCode) {
  assertItemCode(itemCode);
  return db.prepare(`
    SELECT id, exclude_kind, reason, excluded_at, excluded_by, restored_at, restored_reason, restored_by
    FROM migration_excluded
    WHERE item_code = ?
    ORDER BY excluded_at ASC, id ASC
  `).all(itemCode.trim());
}

/**
 * UI ヘッダー集計用: { temporary: N, permanent: M, total: N+M }
 */
function countActiveByKind(db) {
  const rows = db.prepare(`
    SELECT exclude_kind, COUNT(*) AS c
    FROM migration_excluded
    WHERE restored_at IS NULL
    GROUP BY exclude_kind
  `).all();
  const out = { temporary: 0, permanent: 0, total: 0 };
  for (const r of rows) {
    out[r.exclude_kind] = r.c;
    out.total += r.c;
  }
  return out;
}

/**
 * UI 表示用: 英語 enum を日本語ラベルへ。
 */
function kindLabel(kind) {
  if (kind === 'temporary') return '一時除外';
  if (kind === 'permanent') return '永続除外';
  return kind;
}

export {
  ALLOWED_KINDS,
  excludeMigrationItem,
  restoreMigrationExclusion,
  changeExclusionKind,
  listActiveExclusions,
  listExclusionHistory,
  countActiveByKind,
  kindLabel,
};
