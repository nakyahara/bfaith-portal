/**
 * 商品別 想定利益 — 小物
 */
import crypto from 'crypto';

/** run_id / generation_id。時刻順に並ぶ ULID 風 (ms + 乱数。衝突回避は memory の作法どおり) */
export function newRunId(prefix = 'r') {
  const ts = Date.now().toString(36).padStart(9, '0');
  const rand = crypto.randomBytes(5).toString('hex');
  return `${prefix}_${ts}_${rand}`;
}

export function newGenerationId() {
  return newRunId('g');
}

export function nowIso() {
  return new Date().toISOString();
}

export function addDays(iso, days) {
  const d = new Date(iso);
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString();
}

/** 期限切れか (現在時刻で判定する。保存済みの status を信じない — §8.3) */
export function isExpired(validUntil, now = new Date()) {
  if (!validUntil) return true;
  return new Date(validUntil) <= now;
}

/** 世代の内容ハッシュ (manifest 用 — §15-7) */
export function contentHash(rows) {
  const h = crypto.createHash('sha256');
  for (const r of rows) h.update(JSON.stringify(r));
  return h.digest('hex');
}
