/**
 * jst-date — 日付・月キー生成の共通 util (設計監査 2026-07-06 PR-10 / M-1,M-2,M-3)
 *
 * 過去事故:
 *   ・`Date.toISOString().slice(0,7)` は UTC 基準のため、JST 月初 0:00〜8:59 に前月キーを
 *     返す (Render=UTC で毎月月初9時間、miniPC=JST でも UTC 変換で同事故。INV-17)
 *   ・`'2026/3/15'.slice(0,7)` → `'2026/3/'` のゼロ埋め漏れで月キー JOIN から脱落 (INV-16、
 *     mirror に boot 時修復 migration が存在する = 本番発生実績あり)
 *   ・`new Date(y, m+1, 0).toISOString()` は JST 実行で「月末日の前日」に化ける
 *
 * 規約 (設計書「全社DDL・金額規約」§3): 月キー・日付キーは JST 基準、
 * year_month は regex+padStart で組み立て。toISOString 直の月キー生成は禁止。
 */

const JST_OFFSET_MS = 9 * 60 * 60 * 1000;

/** Date → JST の {y, m(1-12), d} */
function jstParts(date = new Date()) {
  const t = new Date(date.getTime() + JST_OFFSET_MS);
  return { y: t.getUTCFullYear(), m: t.getUTCMonth() + 1, d: t.getUTCDate() };
}

/** JST 基準の 'YYYY-MM' */
export function jstYearMonth(date = new Date()) {
  const { y, m } = jstParts(date);
  return `${y}-${String(m).padStart(2, '0')}`;
}

/** JST 基準の 'YYYY-MM-DD' */
export function jstDateStr(date = new Date()) {
  const { y, m, d } = jstParts(date);
  return `${y}-${String(m).padStart(2, '0')}-${String(d).padStart(2, '0')}`;
}

/**
 * 'YYYY-MM' に delta ヶ月加算 (Date 経由しない = TZ 事故なし)。
 * addMonthsYm('2026-01', -1) === '2025-12'
 */
export function addMonthsYm(ym, delta) {
  const m = /^(\d{4})-(\d{2})$/.exec(ym);
  const mo = m ? parseInt(m[2], 10) : 0;
  if (!m || mo < 1 || mo > 12) throw new Error(`year_month 形式不正: "${ym}" (期待: YYYY-MM)`);
  const total = parseInt(m[1], 10) * 12 + (mo - 1) + delta;
  const y = Math.floor(total / 12);
  return `${y}-${String((total % 12) + 1).padStart(2, '0')}`;
}

/** 'YYYY-MM' の月末日 'YYYY-MM-DD' (うるう年対応、Date の TZ を経由しない) */
export function lastDayOfMonthStr(ym) {
  const m = /^(\d{4})-(\d{2})$/.exec(ym);
  const moChk = m ? parseInt(m[2], 10) : 0;
  if (!m || moChk < 1 || moChk > 12) throw new Error(`year_month 形式不正: "${ym}" (期待: YYYY-MM)`);
  const y = parseInt(m[1], 10), mo = moChk;
  // Date.UTC で翌月0日 = 当月末日 (UTC のまま取り出すので TZ ずれなし)
  const last = new Date(Date.UTC(y, mo, 0)).getUTCDate();
  return `${ym}-${String(last).padStart(2, '0')}`;
}

/**
 * 任意形式の日付/年月文字列から 'YYYY-MM' を安全に抽出。
 * '2026/3/15'・'2026-03-15'・'2026/03'・'2026-3' → '2026-03'。抽出不能なら null
 * (slice(0,7) の代替。INV-16: ゼロ埋め漏れ '2026-3-' 事故の再発防止)
 */
export function normalizeYearMonth(s) {
  const m = /^(\d{4})[/-](\d{1,2})/.exec(String(s ?? '').trim());
  if (!m) return null;
  const mo = parseInt(m[2], 10);
  if (mo < 1 || mo > 12) return null;
  return `${m[1]}-${String(mo).padStart(2, '0')}`;
}
