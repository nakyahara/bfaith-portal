/**
 * format.js — 画面に出す値の整形 (純粋関数のみ)
 *
 * ★日時は DB に UTC (new Date().toISOString() = 末尾 Z) で入っている。
 *   画面でそのまま出すと日本時間から9時間ずれる。2026-09-03 に中原さんが
 *   「9/2 10:40 の run」を探して取り違えかけたため、表示は JST に統一する。
 *   ⚠️保存形式は変えない (append-only の監査記録なので UTC のまま持つ)。
 */

/** JST は UTC+9 固定 (日本に夏時間は無い) */
const JST_OFFSET_MS = 9 * 60 * 60 * 1000;

/** タイムゾーンの指定が末尾に付いているか (Z / +09:00 / -0500 など) */
const HAS_TZ_RE = /(?:Z|[+-]\d{2}:?\d{2})$/i;

/**
 * UTC の ISO 文字列を JST の「YYYY-MM-DD HH:MM」に直す。
 *
 * ★分からない値を勝手に整えない: 空なら '—'、日付として読めなければ元の文字列を返す。
 *   ここで嘘の日時を出すと、run の取り違えという一番まずい間違いを誘発する。
 *
 * @param {string|null|undefined} iso  DB の created_at / at
 * @param {{ seconds?: boolean }} [opts]  seconds:true で秒まで出す
 * @returns {string}
 */
export function toJst(iso, opts = {}) {
  if (iso == null || iso === '') return '—';
  const raw = String(iso);
  // タイムゾーンが書かれていない古い値は UTC とみなす
  //   (付けずに new Date() に渡すと実行環境のローカル時刻として解釈され、サーバ次第で答えが変わる)
  const normalized = HAS_TZ_RE.test(raw) ? raw : `${raw}Z`;
  const ms = Date.parse(normalized);
  if (Number.isNaN(ms)) return raw;      // 読めなければ加工しない
  const d = new Date(ms + JST_OFFSET_MS);
  const p2 = (n) => String(n).padStart(2, '0');
  const ymd = `${d.getUTCFullYear()}-${p2(d.getUTCMonth() + 1)}-${p2(d.getUTCDate())}`;
  const hm = `${p2(d.getUTCHours())}:${p2(d.getUTCMinutes())}`;
  return opts.seconds ? `${ymd} ${hm}:${p2(d.getUTCSeconds())}` : `${ymd} ${hm}`;
}

/** ブラウザ側にも同じ関数を渡すための文字列 (run.ejs の claim 表示で使う) */
export const TO_JST_CLIENT_SRC = `
function toJst(iso, withSeconds) {
  if (iso == null || iso === '') return '—';
  var raw = String(iso);
  var normalized = /(?:Z|[+-]\\d{2}:?\\d{2})$/i.test(raw) ? raw : raw + 'Z';
  var ms = Date.parse(normalized);
  if (isNaN(ms)) return raw;
  var d = new Date(ms + ${JST_OFFSET_MS});
  var p2 = function (n) { return String(n).padStart(2, '0'); };
  var ymd = d.getUTCFullYear() + '-' + p2(d.getUTCMonth() + 1) + '-' + p2(d.getUTCDate());
  var hm = p2(d.getUTCHours()) + ':' + p2(d.getUTCMinutes());
  return withSeconds ? ymd + ' ' + hm + ':' + p2(d.getUTCSeconds()) : ymd + ' ' + hm;
}
`.trim();
