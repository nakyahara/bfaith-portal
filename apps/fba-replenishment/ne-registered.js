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
 * 鮮度 (#1694 Codex R2 M1): 写しがいつの物か = mirror_material_generations の products の received_at (Render が写しを入れ替えた時刻・/api/sync が同じ取引で書く)。
 *   世代の無い古い送り手のときは記録が消える = mirror_products の updated_at の最大 (これも入れ替えた時刻 = 同じ同期の now)。mirror_sync_status は使わない。
 *   NE_STALE_HOURS を超えた・時刻が読めない = 🚨 止めない (FBA 補充の現場を止めない・推定で止めない)。警告の文 (note) を付けるだけ。
 *   古い写しで起きうること = NE に入った新商品が「未登録」に見えて CSV から外れる / NE から消えた商品が残る (どちらも警告で人が気づく)
 */

/** 写しが何時間より古ければ知らせるか (毎日の daily-sync = 24 時間 + 遅れの余裕) */
export const NE_STALE_HOURS = 36;
/** 'YYYY-MM-DD HH:MM:SS' (時差なし = /api/sync の now = UTC) と ISO の両方を読む。読めない = null */
export function parseMirrorTime(v) {
  const s = String(v ?? '').trim();
  if (!s) return null;
  const iso = /[zZ]|[+-]\d{2}:?\d{2}$/.test(s) ? s.replace(' ', 'T') : `${s.replace(' ', 'T')}Z`;
  const t = Date.parse(iso);
  return Number.isFinite(t) ? t : null;
}
/** 写しの時刻 { at: ms | null, source } (世代の記録 → 行の updated_at の最大) */
function neCopyTime(mdb) {
  try {
    const g = mdb.prepare("SELECT received_at FROM mirror_material_generations WHERE entity = 'products'").get();
    const t = parseMirrorTime(g?.received_at);
    if (t != null) return { at: t, source: 'generation' };
  } catch { /* 表が無い = 次へ */ }
  try {
    const r = mdb.prepare('SELECT MAX(updated_at) AS t FROM mirror_products').get();
    const t = parseMirrorTime(r?.t);
    if (t != null) return { at: t, source: 'rows' };
  } catch { /* 読めない */ }
  return { at: null, source: null };
}
/** 鮮度の警告の文 (古い・分からない)。新しい = null */
export function freshnessNote(at, nowMs = Date.now(), staleHours = NE_STALE_HOURS) {
  if (at == null) return 'NE の商品の写しがいつの物か分かりません (同期の記録が無い)。NE に入ったばかりの商品が「未登録」に見えることがあります';
  const h = (nowMs - at) / 3600e3;
  if (h <= staleHours) return null;
  return `NE の商品の写しが ${Math.floor(h)} 時間前のものです (同期が止まっているかも)。NE に入ったばかりの商品が「未登録」に見え、NE から消えた商品が残っていることがあります`;
}

/** 写しの中のコードの比べ方 (mirror_products の LOWER(TRIM(商品コード)) と同じ) */
export const neCodeKey = (v) => String(v ?? '').trim().toLowerCase();

/**
 * NE の商品の写しを読む。{ ok: true, codes: Set<key>, count } / { ok: false, error }
 * mdb = better-sqlite3 の warehouse-mirror.db (getMirrorDB())
 */
export function readNeProductCodes(mdb, { nowMs = Date.now(), staleHours = NE_STALE_HOURS } = {}) {
  try {
    const rows = mdb.prepare('SELECT 商品コード AS code FROM mirror_products').all();
    const codes = new Set();
    for (const r of rows) { const k = neCodeKey(r.code); if (k) codes.add(k); }
    if (!codes.size) return { ok: false, error: 'NE の商品の写し (mirror_products) が空です (同期の前・同期の失敗)' };
    const time = neCopyTime(mdb);
    // note = 鮮度の警告 (止めない)。asOf = 写しの時刻 (ISO・分からない = null)
    return { ok: true, codes, count: codes.size, asOf: time.at == null ? null : new Date(time.at).toISOString(), asOfSource: time.source, note: freshnessNote(time.at, nowMs, staleHours) };
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

/**
 * 過去の NE 受注 CSV (履歴) の再ダウンロードの確かめ (#1694 Codex R2 M2)。🚨 止めない (前からある道) = 警告の文の配列だけ返す。
 * record = export_history の行 (sku_detail = 出した時点の [{ sku, qty, comps: [[ne_code, 構成数]] }])。ne = readNeProductCodes の結果
 */
export function historyNotes(record, ne) {
  const notes = [];
  let detail = null;
  try { detail = record && record.sku_detail ? JSON.parse(record.sku_detail) : null; } catch { detail = null; }
  if (!ne || !ne.ok) {
    notes.push(`この履歴ファイルの商品コードが NE にあるかを確かめられません (${(ne && ne.error) || 'NE の商品の写しを読めない'})`);
    return notes;
  }
  if (ne.note) notes.push(ne.note);
  if (!Array.isArray(detail) || !detail.length) {
    notes.push('この履歴ファイルは古い形 (出した時の構成の記録が無い) なので、商品コードが今の NE にあるかを確かめられません。NE に取り込む前に、商品コードを見てください');
    return notes;
  }
  const codes = [];
  for (const d of detail) for (const c of (Array.isArray(d && d.comps) ? d.comps : [])) if (Array.isArray(c) && c[0]) codes.push(c[0]);
  const missing = missingNeCodes(ne, codes);
  if (missing.length) notes.push(`この履歴ファイルには、今の NE の商品の写しに無い商品コード ${missing.slice(0, 10).join('・')}${missing.length > 10 ? ` ほか ${missing.length - 10} 件` : ''} が入っています (出した後に NE から消えた・写しが古い)。NE に取り込む前に見てください`);
  return notes;
}

/** 警告の配列を HTTP のヘッダ (encodeURIComponent した JSON) に。大きすぎないように約 6KB で詰める (NE 受注 CSV の X-NE-Warnings と同じ) */
export function warningsHeader(list, maxBytes = 6000) {
  const picked = [];
  let bytes = 2;
  for (const w of list || []) {
    const enc = encodeURIComponent(JSON.stringify(w));
    if (bytes + enc.length + 1 > maxBytes) break;
    picked.push(w);
    bytes += enc.length + 1;
  }
  return picked.length ? encodeURIComponent(JSON.stringify(picked)) : null;
}

/** NE 受注 CSV の警告の文 (画面の X-NE-Warnings に出る) */
export const notInNeWarning = (sku, name, missing) => `${sku}${name ? ` (${name})` : ''}: NE に未登録の商品コード ${missing.join('・')} があるため出しません（NE 登録の CSV で NE に入れて、次の朝の同期の後に出してください）`;
