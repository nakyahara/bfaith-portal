/**
 * 準備中数量 (miniPC の Inbound API キャッシュ) を「ちゃんと取れたか」判定する。
 *
 * 流れ: Render → miniPC POST /refresh-inbound-working (取り直して { ok, count } を返す)
 *            → miniPC GET /recommendations-inbound-cache ({ ok, data, count, cachedAt, ageMs })
 * 🚨 取り直しが count 120 を返したあとに miniPC が再起動すると、キャッシュは { data: {}, cachedAt: null } になる。
 *    空のオブジェクトも「取れた」と見ていたので、準備中ゼロのまま下書きが決めてしまえた (Codex PR #1438 R1 High 3)。
 *    → 取り直しの件数とキャッシュの件数が一致し、キャッシュの取得時刻があって新しいときだけ fresh。
 *      本当に準備中ゼロの日 (count 0 / data {} / cachedAt あり) は fresh のまま通す
 */
export const INBOUND_CACHE_MAX_AGE_MS = 5 * 60 * 1000;

/**
 * @param {object} p
 * @param {object|null} p.refresh     POST /refresh-inbound-working の応答
 * @param {object|null} p.cache       GET /recommendations-inbound-cache の応答 (取れなければ null)
 * @param {string|null} p.cacheError  キャッシュを取りに行って失敗したときのエラー文
 * @param {number} p.nowMs          キャッシュを受け取った時刻
 * @param {number} [p.requestedMs]  取り直しを頼んだ時刻。渡されたら、キャッシュがそれより前に作られたものなら fresh にしない
 *                                   (取り直しと GET の間に別の取り直しが入った・古いキャッシュを返した。Codex A2b High 3)
 * @returns {{ source: 'fresh'|'empty'|'inconsistent', data: object, count: number, error: string|null }}
 */
export function judgeInboundFetch({ refresh, cache, cacheError = null, nowMs = Date.now(), requestedMs = null }) {
  const bad = (source, error, data = {}) => ({ source, data, count: Object.keys(data).length, error });
  if (!refresh?.ok || !Number.isInteger(refresh.count) || refresh.count < 0) {
    return bad('empty', 'miniPC が count を返さない');
  }
  const data = cache?.data;
  if (!data || typeof data !== 'object' || Array.isArray(data)) {
    return bad('empty', cacheError || 'miniPC のキャッシュが空');
  }
  const n = Object.keys(data).length;
  // 値は 0 以上の数だけ (壊れた値で準備中を少なく数えない)
  const broken = Object.entries(data).filter(([, v]) => !(typeof v === 'number' && Number.isFinite(v) && v >= 0));
  if (broken.length) return bad('inconsistent', `準備中の数が数でない SKU ${broken.length} 件 (例 ${broken[0][0]})`, data);
  const cachedAt = Number(cache.cachedAt);
  if (!Number.isFinite(cachedAt) || cachedAt <= 0) {
    return bad('inconsistent', `キャッシュの取得時刻が無い (miniPC が再起動した可能性。取り直し ${refresh.count} 件 / キャッシュ ${n} 件)`, data);
  }
  if (n !== refresh.count) {
    return bad('inconsistent', `件数が合わない (取り直し ${refresh.count} 件 / キャッシュ ${n} 件)`, data);
  }
  if (nowMs - cachedAt > INBOUND_CACHE_MAX_AGE_MS) {
    return bad('inconsistent', `キャッシュが古い (${Math.round((nowMs - cachedAt) / 1000)} 秒前)`, data);
  }
  if (cachedAt > nowMs + 60e3) return bad('inconsistent', 'キャッシュの取得時刻が未来', data);
  if (Number.isFinite(requestedMs) && cachedAt < requestedMs - 5000) {
    return bad('inconsistent', 'キャッシュが今回の取り直しより前のもの', data);
  }
  return { source: 'fresh', data, count: n, error: null };
}
