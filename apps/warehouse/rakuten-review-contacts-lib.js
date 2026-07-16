/**
 * rakuten-review-contacts-lib.js — フォローメール用 連絡先の暗号化保存 (mall-csv-fetcher P2 PR-B)
 *
 * らくらくーぽん置換 (設計書v1.2) の C2。フォロー/クーポンメール送信 (PR-C) の材料となる
 * 「注文ごとのマスクメールアドレス (楽天あんしんメルアド) + 発送日時」を miniPC 内にだけ保存する。
 *
 * PII 衛生 (Codex R1 確定):
 *   - masked_email_enc = AES-256-GCM 暗号化 (鍵=リポジトリ直下 .env の CONTACTS_ENC_KEY)
 *   - masked_email_hash = HMAC-SHA256 (CONTACTS_HMAC_KEY)。suppression 照合・重複判定用
 *   - mirror / Render / ログ / GChat に宛先を出さない。復号は送信直前 (PR-C) のみ
 *   - contact_delete_at = 発送日時+35日。purge で暗号文を null 化 (HMAC は監査用に残す)
 *   - 既存 rakuten-orders.js の「意図的PII除外」は汚さない (別ジョブ・別テーブル)
 *
 * suppression (配信停止):
 *   - rakuten_contact_suppressions (email_hash PK)。登録は手動CLI。解除も明示操作のみ
 *   - contacts が purge されても suppression は残る
 */
import crypto from 'node:crypto';

const trimEmail = (v) => String(v == null ? '' : v).trim().toLowerCase();

// ─── 鍵 ───
export function loadContactKeys(env = process.env) {
  const encHex = (env.CONTACTS_ENC_KEY || '').trim();
  const hmacHex = (env.CONTACTS_HMAC_KEY || '').trim();
  if (!/^[0-9a-fA-F]{64}$/.test(encHex) || !/^[0-9a-fA-F]{64}$/.test(hmacHex)) {
    throw new Error('CONTACTS_KEY_MISSING: CONTACTS_ENC_KEY / CONTACTS_HMAC_KEY (32byte hex) が未設定。miniPCで node apps/warehouse/gen-review-contact-keys.js を実行');
  }
  return { encKey: Buffer.from(encHex, 'hex'), hmacKey: Buffer.from(hmacHex, 'hex') };
}

// ─── 暗号化ヘルパ ───
export function encryptEmail(email, keys) {
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv('aes-256-gcm', keys.encKey, iv);
  const ct = Buffer.concat([cipher.update(trimEmail(email), 'utf8'), cipher.final()]);
  const tag = cipher.getAuthTag();
  return `v1:${iv.toString('base64')}:${tag.toString('base64')}:${ct.toString('base64')}`;
}

export function decryptEmail(enc, keys) {
  const m = String(enc || '').match(/^v1:([^:]+):([^:]+):([^:]+)$/);
  if (!m) throw new Error('CONTACTS_DECRYPT: 暗号文の形式が不正');
  const [, ivB64, tagB64, ctB64] = m;
  const decipher = crypto.createDecipheriv('aes-256-gcm', keys.encKey, Buffer.from(ivB64, 'base64'));
  decipher.setAuthTag(Buffer.from(tagB64, 'base64'));
  return Buffer.concat([decipher.update(Buffer.from(ctB64, 'base64')), decipher.final()]).toString('utf8');
}

export function hmacEmail(email, keys) {
  return crypto.createHmac('sha256', keys.hmacKey).update(trimEmail(email), 'utf8').digest('hex');
}

export function hmacOrderKey(orderNumber, keys) {
  return crypto.createHmac('sha256', keys.hmacKey).update(`order:${String(orderNumber).trim()}`, 'utf8').digest('hex');
}

// ─── DDL ───
export function ensureContactTables(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS rakuten_order_contacts (
    order_number      TEXT PRIMARY KEY,
    order_key_hmac    TEXT NOT NULL UNIQUE,
    masked_email_enc  TEXT,
    masked_email_hash TEXT,
    order_datetime    TEXT,
    shipping_datetime TEXT,
    order_progress    INTEGER,
    contact_delete_at TEXT NOT NULL,
    fetched_at        TEXT NOT NULL,
    updated_at        TEXT NOT NULL,
    purged_at         TEXT
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_roc_delete ON rakuten_order_contacts(contact_delete_at) WHERE purged_at IS NULL`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_roc_ship ON rakuten_order_contacts(shipping_datetime)`);

  db.exec(`CREATE TABLE IF NOT EXISTS rakuten_contact_suppressions (
    email_hash  TEXT PRIMARY KEY,
    reason      TEXT NOT NULL,
    created_at  TEXT NOT NULL
  )`);
}

/** 発送日時 (無ければ注文日時) + 35日 → contact_delete_at (ISO)。Codex R1: 明確な期限計算 */
export function computeDeleteAt(shippingDatetime, orderDatetime) {
  const base = shippingDatetime || orderDatetime;
  const t = Date.parse(base);
  if (!Number.isFinite(t)) throw new Error(`CONTACTS_DELETE_AT: 基準日時が不正 (${base})`);
  return new Date(t + 35 * 86400000).toISOString();
}

/** getOrder 応答 1件 → contacts レコード (割り当てはallowlistのみ。抽出失敗は null を返しスキップ) */
export function extractContact(order, keys) {
  const orderNumber = String(order.orderNumber || '').trim();
  if (!orderNumber) return null;
  const orderer = order.OrdererModel || order.ordererModel || {};
  const email = trimEmail(orderer.emailAddress);
  // 発送日: 全パッケージの ShippingModelList から最終発送日 (Codex: 複数配送は最終を起点)
  let shippingDate = null;
  for (const pkg of order.PackageModelList || []) {
    for (const s of pkg.ShippingModelList || pkg.shippingModelList || []) {
      const d = String(s.shippingDate || '').trim();
      if (d && (!shippingDate || d > shippingDate)) shippingDate = d;
    }
  }
  const orderDatetime = String(order.orderDatetime || '').trim() || null;
  const shippingDatetime = shippingDate ? `${shippingDate}T12:00:00+09:00` : null; // 日付のみ→正午JSTで代表
  let deleteAt;
  try {
    deleteAt = computeDeleteAt(shippingDatetime, orderDatetime);
  } catch {
    return null;
  }
  return {
    order_number: orderNumber,
    order_key_hmac: hmacOrderKey(orderNumber, keys),
    masked_email_enc: email ? encryptEmail(email, keys) : null,
    masked_email_hash: email ? hmacEmail(email, keys) : null,
    order_datetime: orderDatetime,
    shipping_datetime: shippingDatetime,
    order_progress: Number(order.orderProgress ?? 0) || 0,
    contact_delete_at: deleteAt,
  };
}

/** contacts の UPSERT (fetched_at は初回値を保持)。@returns {inserted, updated} */
export function upsertContacts(db, records) {
  const now = new Date().toISOString();
  const stmt = db.prepare(`
    INSERT INTO rakuten_order_contacts (
      order_number, order_key_hmac, masked_email_enc, masked_email_hash,
      order_datetime, shipping_datetime, order_progress, contact_delete_at,
      fetched_at, updated_at, purged_at
    ) VALUES (
      @order_number, @order_key_hmac, @masked_email_enc, @masked_email_hash,
      @order_datetime, @shipping_datetime, @order_progress, @contact_delete_at,
      @now, @now, NULL
    )
    ON CONFLICT(order_number) DO UPDATE SET
      masked_email_enc  = CASE WHEN rakuten_order_contacts.purged_at IS NULL THEN excluded.masked_email_enc ELSE rakuten_order_contacts.masked_email_enc END,
      masked_email_hash = COALESCE(excluded.masked_email_hash, rakuten_order_contacts.masked_email_hash),
      order_datetime    = COALESCE(excluded.order_datetime, rakuten_order_contacts.order_datetime),
      shipping_datetime = COALESCE(excluded.shipping_datetime, rakuten_order_contacts.shipping_datetime),
      order_progress    = excluded.order_progress,
      contact_delete_at = excluded.contact_delete_at,
      updated_at        = excluded.updated_at
  `);
  const existsStmt = db.prepare(`SELECT 1 FROM rakuten_order_contacts WHERE order_number = ?`);
  let inserted = 0, updated = 0;
  const tx = db.transaction(() => {
    for (const r of records) {
      // better-sqlite3 は UPSERT の insert/update を区別しないため、事前に存在チェックで数える
      const exists = !!existsStmt.get(r.order_number);
      stmt.run({ ...r, now });
      if (exists) updated++; else inserted++;
    }
  });
  tx();
  return { inserted, updated };
}

/** 期限超過の暗号文を null 化 (行と HMAC は監査用に残す)。@returns purged件数 */
export function purgeExpiredContacts(db, nowIso = new Date().toISOString()) {
  const info = db.prepare(`
    UPDATE rakuten_order_contacts
       SET masked_email_enc = NULL, purged_at = ?
     WHERE purged_at IS NULL AND contact_delete_at < ?
  `).run(nowIso, nowIso);
  return info.changes;
}

/** suppression 登録 (生アドレスは保存しない — HMACのみ)。@returns email_hash */
export function addSuppression(db, email, reason, keys) {
  const hash = hmacEmail(email, keys);
  db.prepare(`
    INSERT INTO rakuten_contact_suppressions (email_hash, reason, created_at)
    VALUES (?, ?, ?)
    ON CONFLICT(email_hash) DO UPDATE SET reason = excluded.reason
  `).run(hash, String(reason || 'manual'), new Date().toISOString());
  return hash;
}

export function isSuppressed(db, emailHash) {
  return !!db.prepare(`SELECT 1 FROM rakuten_contact_suppressions WHERE email_hash = ?`).get(emailHash);
}
