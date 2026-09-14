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

/**
 * 日時として読めるか。
 * 🚨 new Date('invalid') は NaN になり、比較が常に false になる。
 *    壊れた日時を「期限内」として通さないために、読めるかどうかを先に判定する
 */
export function parseTime(iso) {
  if (!iso) return null;
  const t = new Date(iso).getTime();
  return Number.isFinite(t) ? t : null;
}

/** 期限切れか (現在時刻で判定する。保存済みの status を信じない — §8.3) */
export function isExpired(validUntil, now = new Date()) {
  const t = parseTime(validUntil);
  if (t == null) return true;          // 欠損・不正は失効扱い
  return t <= now.getTime();
}

/** セラーが分からなかった回の shop_id の頭 (fetch-listings.js amazonShopId) */
export const UNKNOWN_SELLER = 'unknown';

/**
 * 古い run に残る `unknown@<市場>` を、今の shop_id (`<セラー>@<市場>`) と同じ店として扱う。
 *
 * 🚨 2026-09-14 発覚: Amazon の shop_id を env だけから作っていたので、env にセラーID が無い回は
 *    `unknown@…` で記録され、env に入った夜 (9/9) から出品の鍵 (shop_id + SKU) が総入れ替えになった。
 *    →「前回の出品が 100% 消えた」と判定されて毎晩 partial、前回集合との UNION で全出品が 2 重になり、
 *    Amazon が全部「判定できない」のまま 5 日続いた。セラーが分からなかっただけで、別の店ではない。
 * 🚨 揃えるのは「unknown と、同じ市場の実セラー」だけ。別の実セラー・別の市場は揃えない
 *    (本当に別の店なら、消えた・増えたとして見えなければならない)
 */
export function canonicalShopId(shopId, currentShopId) {
  const split = (s) => {
    if (typeof s !== 'string') return null;
    const at = s.indexOf('@');
    return at < 0 ? null : { seller: s.slice(0, at), market: s.slice(at + 1) };
  };
  const old = split(shopId);
  const cur = split(currentShopId);
  if (!old || !cur) return shopId;
  if (old.seller !== UNKNOWN_SELLER || cur.seller === UNKNOWN_SELLER) return shopId;
  return old.market === cur.market ? currentShopId : shopId;
}

/** 世代の内容ハッシュ (manifest 用 — §15-7) */
export function contentHash(rows) {
  const h = crypto.createHash('sha256');
  for (const r of rows) h.update(JSON.stringify(r));
  return h.digest('hex');
}
