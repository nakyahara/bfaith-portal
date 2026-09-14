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
 * 古い run の Amazon の shop_id (`<セラー>@<市場>`) を、同じ市場なら今の shop_id と同じ店として扱う。
 * 前回の出品との突き合わせ (列挙の比較・partial 時の UNION) にだけ使う。
 *
 * 🚨 2026-09-14 発覚: Amazon の shop_id を env だけから作っていたので、env にセラーID が無い回は
 *    `unknown@…` で記録され、env に入った夜 (9/9) から出品の鍵 (shop_id + SKU) が総入れ替えになった。
 *    →「前回の出品が 100% 消えた」と判定されて毎晩 partial、前回集合との UNION で全出品が 2 重になり、
 *    Amazon が全部「判定できない」のまま 5 日続いた。
 * 🚨 unknown だけでなく、実セラー同士でも揃える (Codex R1-P1)。セラーの覚え書きは手数料 API の応答で
 *    自動更新されるので、実 ID が変わった夜にも同じ鍵の総入れ替えが起きうる。
 *    B-Faith の Amazon (日本) は 1 アカウント。本当に別のアカウントに移ったなら SKU が変わるので、
 *    「消えた」は SKU の側で今までどおり見える。別の市場は揃えない
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
  return old.market === cur.market ? currentShopId : shopId;
}

/** 世代の内容ハッシュ (manifest 用 — §15-7) */
export function contentHash(rows) {
  const h = crypto.createHash('sha256');
  for (const r of rows) h.update(JSON.stringify(r));
  return h.digest('hex');
}
