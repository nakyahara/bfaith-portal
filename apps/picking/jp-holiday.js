/**
 * 日本の祝日判定 (外部依存なし・アルゴリズム計算)。
 *
 * 用途: 欠品通知の土日祝送り分け (2026-08-15 中原さん要望)。判定はJST基準で、
 * 渡されるのは常に「現在時刻」— 過去年の遡及判定はサポートしない。
 *
 * サポート範囲 = 2022年〜 (Codexレビュー反映):
 *   現行の祝日法が全て揃った年以降のみ (天皇誕生日2/23は2020〜・山の日は2016〜・
 *   五輪特例の移動は2021で終了・振替休日の連休繰り下げ規定は2007〜)。
 *   それ以前の年は判定せず false (平日扱い) を返す。
 * 対応: 固定祝日 / ハッピーマンデー / 春分・秋分 / 振替休日 (日曜の祝日→次の平日) /
 *       国民の休日 (祝日に挟まれた平日)。
 * ⚠ 春分・秋分の正式日付は前年2月の官報告示で確定する。ここの近似式は近年の告示と
 *   一致しており (2025/2026はテストで検証)、将来年は予測値。用途が通知の送り分け
 *   (誤っても平日/休日ラインの取り違えのみ・出荷やデータには無影響) なので許容する。
 * ⚠ 今後の法改正・特例移動が告示されたらこのファイルを更新すること。
 */

const HOLIDAY_VALID_FROM = 2022;
const HOLIDAY_VALID_TO = 2099;   // 春分・秋分の近似式の適用上限

/** y年m月の第n w曜日 (w: 0=日..6=土) の日にちを返す。 */
function nthWeekday(y, m, n, w) {
  const first = new Date(Date.UTC(y, m - 1, 1)).getUTCDay();
  return 1 + ((7 + w - first) % 7) + (n - 1) * 7;
}

/** 春分/秋分の日 (天文近似式。確定済みの近年の官報告示とは一致・将来年は予測値)。 */
function equinoxDay(y, isVernal) {
  const base = isVernal ? 20.8431 : 23.2488;
  return Math.floor(base + 0.242194 * (y - 1980) - Math.floor((y - 1980) / 4));
}

/** y年の祝日 (振替・国民の休日込み) を 'M-D' 文字列のSetで返す。 */
function holidaysOfYear(y) {
  const days = new Set();
  const add = (m, d) => days.add(`${m}-${d}`);
  // 固定祝日
  add(1, 1);    // 元日
  add(2, 11);   // 建国記念の日
  add(2, 23);   // 天皇誕生日 (2020〜)
  add(4, 29);   // 昭和の日
  add(5, 3);    // 憲法記念日
  add(5, 4);    // みどりの日
  add(5, 5);    // こどもの日
  add(8, 11);   // 山の日 (2016〜)
  add(11, 3);   // 文化の日
  add(11, 23);  // 勤労感謝の日
  // ハッピーマンデー
  add(1, nthWeekday(y, 1, 2, 1));    // 成人の日 (1月第2月曜)
  add(7, nthWeekday(y, 7, 3, 1));    // 海の日 (7月第3月曜)
  add(9, nthWeekday(y, 9, 3, 1));    // 敬老の日 (9月第3月曜)
  add(10, nthWeekday(y, 10, 2, 1));  // スポーツの日 (10月第2月曜)
  // 春分・秋分
  add(3, equinoxDay(y, true));
  add(9, equinoxDay(y, false));

  const isBase = (m, d) => days.has(`${m}-${d}`);
  const dow = (m, d) => new Date(Date.UTC(y, m - 1, d)).getUTCDay();
  const nextDay = (m, d) => {
    const t = new Date(Date.UTC(y, m - 1, d + 1));
    return [t.getUTCMonth() + 1, t.getUTCDate()];
  };

  // 国民の休日: 前後を祝日に挟まれた平日 (敬老の日と秋分の間に発生し得る)
  const kokumin = [];
  for (const key of [...days]) {
    const [m, d] = key.split('-').map(Number);
    const [m2, d2] = nextDay(m, d);
    const [m3, d3] = nextDay(m2, d2);
    if (isBase(m3, d3) && !isBase(m2, d2) && dow(m2, d2) !== 0) kokumin.push(`${m2}-${d2}`);
  }
  kokumin.forEach((k) => days.add(k));

  // 振替休日: 日曜の祝日 → 直後の「祝日でない日」(2007年改正: 連休明けまで繰り下げ)
  const furikae = [];
  for (const key of [...days]) {
    const [m, d] = key.split('-').map(Number);
    if (dow(m, d) !== 0) continue;
    let [fm, fd] = nextDay(m, d);
    while (days.has(`${fm}-${fd}`)) [fm, fd] = nextDay(fm, fd);
    furikae.push(`${fm}-${fd}`);
  }
  furikae.forEach((k) => days.add(k));
  return days;
}

const _yearCache = new Map();
function holidaySet(y) {
  if (!_yearCache.has(y)) _yearCache.set(y, holidaysOfYear(y));
  return _yearCache.get(y);
}

/** JSTの (y, m, d, 曜日) を取り出す。 */
function jstParts(date) {
  const t = new Date(date.getTime() + 9 * 3600 * 1000);
  return { y: t.getUTCFullYear(), m: t.getUTCMonth() + 1, d: t.getUTCDate(), dow: t.getUTCDay() };
}

/** JSTで祝日か (土日は含まない)。サポート範囲外の年は false (平日扱い=通常ラインへ)。 */
export function isJpHolidayJst(date = new Date()) {
  const { y, m, d } = jstParts(date);
  if (y < HOLIDAY_VALID_FROM || y > HOLIDAY_VALID_TO) return false;
  return holidaySet(y).has(`${m}-${d}`);
}

/** JSTで土日または祝日か。通知の送り分け用。 */
export function isJstWeekendOrHoliday(date = new Date()) {
  const { dow } = jstParts(date);
  if (dow === 0 || dow === 6) return true;
  return isJpHolidayJst(date);
}
