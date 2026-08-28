/**
 * yahoo-review-suppression-lib.js — Yahoo 版の配信停止 (suppression) 照合 (PR-Y-C4)
 *
 * 楽天版は contacts に masked_email_hash を持つので planner のゲートで照合できるが、
 * Yahoo は約款第10条により宛先をディスクに持たない (VIEW の masked_email_hash は NULL)。
 * → **送信直前に、API から取った宛先を HMAC 化して照合する**のがこのファイルの役目
 *   (要件設計 §Y2 / yahoo-review-campaign-adapter.js 冒頭の「送信直前に HMAC で照合 = PR-Y-C4」)。
 *
 * 保存するのは HMAC だけで、生アドレスは残さない (登録・取込のときも同じ)。
 * 鍵は楽天と別 (YAHOO_SUPPRESS_HMAC_KEY)。**鍵が無ければ送信させない (fail-closed)** —
 * 「照合できないから素通り」は配信停止の申し出を無視することになるため。
 */
import crypto from 'node:crypto';

export const KEY_ENV = 'YAHOO_SUPPRESS_HMAC_KEY';

/** 32 byte hex の HMAC 鍵。未設定・不正は throw (送信を止める) */
export function loadYahooSuppressKey(env = process.env) {
  const hex = (env[KEY_ENV] || '').trim();
  if (!/^[0-9a-fA-F]{64}$/.test(hex)) {
    throw new Error(`YAHOO_SUPPRESS_KEY_MISSING: ${KEY_ENV} (32byte hex) が未設定/不正。`
      + ` 生成: node -e "console.log(require('node:crypto').randomBytes(32).toString('hex'))" → miniPC の .env へ`);
  }
  return Buffer.from(hex, 'hex');
}

export const normalizeEmail = (email) => String(email || '').trim().toLowerCase();

export function hmacEmail(email, key) {
  const e = normalizeEmail(email);
  if (!e) throw new Error('hmacEmail: 空のアドレス');
  return crypto.createHmac('sha256', key).update(e, 'utf8').digest('hex');
}

export function isSuppressedHash(db, emailHash) {
  return !!db.prepare('SELECT 1 FROM yahoo_contact_suppressions WHERE email_hash = ? AND released_by IS NULL').get(emailHash);
}

/**
 * 生アドレスは保存しない。
 * @returns {hash, inserted, reactivated} — ON CONFLICT でも changes>0 になるので、
 *   「今回いくつ増えたか」は事前の SELECT で判定する (Codex Y-C4 R3 Medium)
 */
export function addSuppression(db, { email, reason, source = 'manual', evidence = null, key, nowIso = new Date().toISOString() }) {
  if (!reason) throw new Error('addSuppression: reason は必須');
  const hash = hmacEmail(email, key);
  const before = db.prepare('SELECT released_by FROM yahoo_contact_suppressions WHERE email_hash = ?').get(hash);
  db.prepare(`
    INSERT INTO yahoo_contact_suppressions (email_hash, reason, source, evidence, created_at)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(email_hash) DO UPDATE SET reason = excluded.reason, source = excluded.source, released_by = NULL
  `).run(hash, String(reason).slice(0, 200), String(source).slice(0, 40), evidence ? String(evidence).slice(0, 200) : null, nowIso);
  return { hash, inserted: !before, reactivated: !!before && before.released_by != null };
}

/** 解除は明示操作のみ (誰が解除したかを残す) */
export function releaseSuppression(db, { email, key, by }) {
  if (!by) throw new Error('releaseSuppression: by (解除した人) は必須');
  const hash = hmacEmail(email, key);
  return db.prepare('UPDATE yahoo_contact_suppressions SET released_by = ? WHERE email_hash = ? AND released_by IS NULL').run(String(by).slice(0, 60), hash).changes > 0;
}

export function suppressionStats(db) {
  const a = db.prepare('SELECT COUNT(*) n FROM yahoo_contact_suppressions WHERE released_by IS NULL').get().n;
  const r = db.prepare('SELECT COUNT(*) n FROM yahoo_contact_suppressions WHERE released_by IS NOT NULL').get().n;
  const bySource = db.prepare('SELECT source, COUNT(*) n FROM yahoo_contact_suppressions WHERE released_by IS NULL GROUP BY source').all();
  return { active: a, released: r, bySource };
}

/**
 * CSV テキストからメールアドレスだけを抜き出す (vendor の「除外対象者」CSV 取込用)。
 * 列名も区切りも vendor 仕様に依存させない: 行内のアドレスらしき文字列を拾って正規化・重複排除する。
 * (取り違えるくらいなら拾いすぎる方が安全 = 送らない側に倒れる)
 */
export function extractEmails(text) {
  const found = String(text || '').match(/[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/g) || [];
  return [...new Set(found.map(normalizeEmail))];
}
