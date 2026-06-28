/**
 * 仕入れ先向け売れ筋共有 — 仕入先マスタ & 公開トークンの永続化ヘルパー
 *
 * supplier_share_master / supplier_share_tokens は warehouse-mirror/db.js の
 * createSupplierShareTables() で作成済み（Render 完結書込）。
 *
 * セキュリティ方針（Codex レビュー反映）:
 *  - 生トークンは 256bit (randomBytes(32)) の URL-safe 文字列。DB には SHA-256 ハッシュのみ保存。
 *  - 生トークンは発行時に一度だけ呼び出し元へ返す（再表示不可）。
 *  - 失効: active=0 / revoked_at セット / expires_at 経過 のいずれかで無効。
 */
import crypto from 'node:crypto';
import { getMirrorDB } from '../warehouse-mirror/db.js';

function nowIso() {
  return new Date().toISOString();
}

export function hashToken(rawToken) {
  return crypto.createHash('sha256').update(rawToken, 'utf8').digest('hex');
}

function generateRawToken() {
  // URL-safe base64（=記号無し）。43 文字 ≒ 256bit。
  return crypto.randomBytes(32).toString('base64url');
}

// ── 仕入先マスタ（コード→表示名） ──────────────────────────────

export function upsertSupplierName(code, displayName, memo) {
  const db = getMirrorDB();
  const now = nowIso();
  db.prepare(`
    INSERT INTO supplier_share_master (仕入先コード, 表示名, memo, created_at, updated_at)
    VALUES (@code, @name, @memo, @now, @now)
    ON CONFLICT(仕入先コード) DO UPDATE SET
      表示名 = excluded.表示名,
      memo   = excluded.memo,
      updated_at = excluded.updated_at
  `).run({ code, name: displayName, memo: memo || null, now });
}

export function getSupplierName(code) {
  const db = getMirrorDB();
  const row = db.prepare('SELECT 表示名 FROM supplier_share_master WHERE 仕入先コード = ?').get(code);
  return row ? row.表示名 : null;
}

/**
 * mirror_products に存在する全仕入先コードを、商品数・表示名つきで列挙。
 * 売上のある商品を 1 つ以上持つ仕入先のみ対象にしたいが、まずは商品マスタ基準で全件返す。
 */
export function listSuppliers() {
  const db = getMirrorDB();
  return db.prepare(`
    SELECT
      p.仕入先コード AS code,
      COALESCE(m.表示名, '') AS display_name,
      COUNT(*) AS product_count,
      (SELECT COUNT(*) FROM supplier_share_tokens t
         WHERE t.仕入先コード = p.仕入先コード AND t.active = 1 AND t.revoked_at IS NULL) AS active_token_count
    FROM mirror_products p
    LEFT JOIN supplier_share_master m ON m.仕入先コード = p.仕入先コード
    WHERE p.仕入先コード IS NOT NULL AND trim(p.仕入先コード) <> ''
    GROUP BY p.仕入先コード, m.表示名
    ORDER BY product_count DESC
  `).all();
}

// ── 公開トークン ──────────────────────────────────────────────

/**
 * 新規トークンを発行。生トークンを返す（保存はハッシュのみ、再取得不可）。
 * @returns { id, rawToken }
 */
export function createToken({ supplierCode, label, expiresAt, createdBy }) {
  const db = getMirrorDB();
  const rawToken = generateRawToken();
  const info = db.prepare(`
    INSERT INTO supplier_share_tokens
      (token_hash, token_plain, 仕入先コード, label, active, expires_at, created_by, created_at, access_count)
    VALUES (@hash, @plain, @code, @label, 1, @expires, @by, @now, 0)
  `).run({
    hash: hashToken(rawToken),
    plain: rawToken,
    code: supplierCode,
    label: label || null,
    expires: expiresAt || null,
    by: createdBy || null,
    now: nowIso(),
  });
  return { id: info.lastInsertRowid, rawToken };
}

export function listTokens(supplierCode) {
  const db = getMirrorDB();
  const where = supplierCode ? 'WHERE 仕入先コード = ?' : '';
  const args = supplierCode ? [supplierCode] : [];
  return db.prepare(`
    SELECT id, 仕入先コード AS supplier_code, label, active, expires_at, revoked_at,
           created_by, created_at, last_accessed_at, access_count, token_plain
    FROM supplier_share_tokens
    ${where}
    ORDER BY created_at DESC
  `).all(...args);
}

export function revokeToken(id) {
  const db = getMirrorDB();
  db.prepare(`
    UPDATE supplier_share_tokens
    SET active = 0, revoked_at = ?
    WHERE id = ? AND revoked_at IS NULL
  `).run(nowIso(), id);
}

/**
 * 生トークンから有効なレコードを引く。無効（失効/期限切れ）なら null。
 * 副作用としてアクセス記録（last_accessed_at / access_count）を更新する。
 */
export function resolveActiveToken(rawToken) {
  if (!rawToken || typeof rawToken !== 'string') return null;
  const db = getMirrorDB();
  const row = db.prepare(`
    SELECT id, 仕入先コード AS supplier_code, label, active, expires_at, revoked_at
    FROM supplier_share_tokens
    WHERE token_hash = ?
  `).get(hashToken(rawToken));
  if (!row) return null;
  if (!row.active || row.revoked_at) return null;
  if (row.expires_at && row.expires_at < nowIso()) return null;
  db.prepare(`
    UPDATE supplier_share_tokens
    SET last_accessed_at = ?, access_count = access_count + 1
    WHERE id = ?
  `).run(nowIso(), row.id);
  return { id: row.id, supplierCode: row.supplier_code, label: row.label };
}
