/**
 * ne-registered.js — FBA 補充の NE 受注 CSV に「NE にまだ無い商品コード」を入れない (#1694 Codex R1 Medium・2026-10-12)
 *
 * なぜ: Amazon SKU の対応の構成品に、まだ NE に無い新商品 (Company DB の登録の状態 = 下書き・NE 登録待ち) も使えるようになった (#1694)。
 *   自動の提案は倉庫 (ロジザード) の行が無いので保留になるが、手で足す・古い仮確定・先にロジザードの行ができた、の道では
 *   NE に無いコード入りの受注 CSV を作れてしまう (NE への取り込みでエラー・画面の説明「NE に入るまで FBA 補充は保留」と食い違う)。
 *
 * NE の商品の写し = Render の warehouse-mirror.db の mirror_products (miniPC の m_products = NE の取得の単品 raw_ne_products・
 *   セット raw_ne_set_products・例外原価の表 exception_genka の写し。毎日の daily-sync が全部入れ替える)。
 *   Company DB の下書き・NE 登録待ちは m_products に入らない (NE の取得に無い) = ここに無い。NE に入れば次の写しから有る。
 *   🚨 例外の商品 (exception_genka だけにある = NE に無い) も mirror_products にある = この確かめでは止まらない (前からの動きのまま。
 *   例外の SKU は Amazon の構成品に使えない = 新しく入ることは無い)
 *
 * 決まり: 写しを読めない・0 件 = 断る (fail-closed・CSV を出さない)。読めた = コード (前後の空白・大文字小文字を除いて) の集合
 */

/** 写しの中のコードの比べ方 (mirror_products の LOWER(TRIM(商品コード)) と同じ) */
export const neCodeKey = (v) => String(v ?? '').trim().toLowerCase();

/**
 * NE の商品の写しを読む。{ ok: true, codes: Set<key>, count } / { ok: false, error }
 * mdb = better-sqlite3 の warehouse-mirror.db (getMirrorDB())
 */
export function readNeProductCodes(mdb) {
  try {
    const rows = mdb.prepare('SELECT 商品コード AS code FROM mirror_products').all();
    const codes = new Set();
    for (const r of rows) { const k = neCodeKey(r.code); if (k) codes.add(k); }
    if (!codes.size) return { ok: false, error: 'NE の商品の写し (mirror_products) が空です (同期の前・同期の失敗)' };
    return { ok: true, codes, count: codes.size };
  } catch (e) {
    return { ok: false, error: `NE の商品の写し (mirror_products) を読めません: ${String(e && e.message || e).slice(0, 120)}` };
  }
}

/** 構成品のコードのうち、NE の写しに無いもの (並びは元のまま・重なりは 1 つ)。ne = readNeProductCodes の ok の結果 */
export function missingNeCodes(ne, codes) {
  const out = [];
  const seen = new Set();
  for (const c of codes || []) {
    const k = neCodeKey(c);
    if (!k || seen.has(k)) continue;
    seen.add(k);
    if (!ne.codes.has(k)) out.push(String(c).trim());
  }
  return out;
}

/** SKU の対応の行 (getSkuMappings の 1 行) → 構成品の NE コードの配列 (NE 受注 CSV と同じ展開 = set_components が無ければ ne_code)。読めない = [] */
export function componentCodesOf(mapping) {
  if (!mapping) return [];
  let comps = [];
  if (mapping.set_components) {
    try { comps = typeof mapping.set_components === 'string' ? JSON.parse(mapping.set_components) : mapping.set_components; } catch { comps = []; }
  }
  if (!Array.isArray(comps) || !comps.length) return mapping.ne_code ? [mapping.ne_code] : [];
  return comps.filter((c) => c && c.ne_code).map((c) => c.ne_code);
}

/** NE 受注 CSV の警告の文 (画面の X-NE-Warnings に出る) */
export const notInNeWarning = (sku, name, missing) => `${sku}${name ? ` (${name})` : ''}: NE に未登録の商品コード ${missing.join('・')} があるため出しません（NE 登録の CSV で NE に入れて、次の朝の同期の後に出してください）`;
