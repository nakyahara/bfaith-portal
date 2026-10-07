/**
 * reference-combine.mjs — 応答の契約 (apps/company-db/profit/response-contract.mjs) の **参照の組み立て** (試験だけで使う)
 *
 * 月ごとの計算の結果 (月の包む関数の 1 行 + 月の metadata・🚨 形は仮 = PR 3 で包む関数が決まったら合わせる) から、最終の応答を列ごとの規則で作る。
 * PR 6 の本物の組み立ては、この fixture (*.input.json → *.expected.json) と同じ結果を出すこと。
 *   - 丸める前の値は月の行の `<列>_raw` (numeric の文字)。Decimal (ここでは BigInt の固定小数) で足して最後に 1 回だけ小数 2 桁に丸める
 *     (PostgreSQL の round(numeric, 2) = 0 から遠い方へ。試験で PGlite の round と突き合わせる)
 *   - bigint は BigInt で足す (JS の Number に入れない)
 *   - calculated_at は要求の 1 つの値 (月ごとの計算の値は捨てる・R5 Low)
 *   - 🆕 v2 (PR 2c): months[].finance_coverage_token は月の metadata の値をそのまま / 日の行の member_seller_skus は月の行の値をそのまま (つなぐだけ・並べ直さない)
 */
import { TOTALS_COLUMNS, TOTALS_REASONS, AD_STATUS_RANK, MASTER_NOTE_KEYS, MONTH_KEYS, CONTRACT_VERSION, MASTER_BASIS, monthsOf, build503Body } from '../../../apps/company-db/profit/response-contract.mjs';

const DEC_RE = /^(-?)(\d+)(?:\.(\d+))?$/;
/** numeric の文字 → { n: BigInt (10^scale 倍), scale } */
export function parseDecimal(s) {
  const m = DEC_RE.exec(String(s));
  if (!m) throw new Error(`numeric の文字でない: ${s}`);
  const frac = m[3] || '';
  const n = BigInt(m[2] + frac);
  return { n: m[1] === '-' ? -n : n, scale: frac.length };
}
/** numeric の文字の和 (丸めない) */
export function sumDecimals(list) {
  const ps = list.map(parseDecimal);
  const scale = Math.max(0, ...ps.map((p) => p.scale));
  const n = ps.reduce((a, p) => a + p.n * 10n ** BigInt(scale - p.scale), 0n);
  return { n, scale };
}
/** 小数 2 桁に丸める (half away from zero = PostgreSQL の round(numeric, 2)) → "123.45" ("-0.00" にしない) */
export function round2({ n, scale }) {
  let q;
  if (scale <= 2) q = n * 10n ** BigInt(2 - scale);
  else {
    const d = 10n ** BigInt(scale - 2);
    q = n / d;                       // 0 へ切り捨て
    const r = n % d;
    const abs = r < 0n ? -r : r;
    if (abs * 2n >= d) q += n < 0n ? -1n : 1n;
  }
  const neg = q < 0n, a = neg ? -q : q;
  const s = a.toString().padStart(3, '0');
  return `${neg && a !== 0n ? '-' : ''}${s.slice(0, -2)}.${s.slice(-2)}`;
}

const versionMismatch = (reason) => ({ status: 503, body: build503Body('PROFIT_VERSION_MISMATCH', reason) });

function checkMonthsInput(input) {
  const { request, months } = input;
  const want = monthsOf(request.from, request.to);
  if (months.length !== want.length) throw new Error('月の数が要求と違う');
  months.forEach((m, i) => { for (const k of ['month_start', 'period_from', 'period_to']) if (m.meta[k] !== want[i][k]) throw new Error(`月の ${k} が違う`); });
}
const monthsOut = (input, version) => input.months.map((m) => Object.fromEntries(MONTH_KEYS.map((k) => [k,
  k === 'calculation_version' ? version : k === 'calculated_at' ? input.request.calculated_at : m.meta[k]])));
const top = (input, kind, version) => ({
  ok: true, contract: CONTRACT_VERSION, kind, mall: input.request.mall, scope: input.request.scope, from: input.request.from, to: input.request.to,
  master_basis: MASTER_BASIS, calculation_version: version, calculated_at: input.request.calculated_at, master_as_of: input.request.calculated_at,
});

/** /totals: 月ごとの計算 → 最終の応答 ({ status, body }) */
export function combineTotals(input) {
  checkMonthsInput(input);
  const ms = input.months, rows = ms.map((m) => m.totals);
  const versions = new Set([...ms.map((m) => m.meta.calculation_version), ...rows.map((r) => r.calculation_version)]);
  if (versions.size !== 1) return versionMismatch('CALCULATION_VERSION');
  if (rows.some((r) => r.master_basis !== MASTER_BASIS)) return versionMismatch('MASTER_BASIS');
  const [version] = versions;
  const total = {};
  for (const c of TOTALS_COLUMNS) {
    const vals = rows.map((r) => r[c.name]);
    switch (c.rule) {
      case 'omit': break;
      case 'request_bound': total[c.name] = c.name === 'period_from' ? input.request.from : input.request.to; break;
      case 'int_sum': total[c.name] = vals.reduce((a, v) => a + v, 0); break;
      case 'bigint_sum': total[c.name] = vals.reduce((a, v) => a + BigInt(v), 0n).toString(); break;
      case 'bigint_sum_null': total[c.name] = vals.some((v) => v === null) ? null : vals.reduce((a, v) => a + BigInt(v), 0n).toString(); break;
      case 'decimal_sum': case 'decimal_sum_null': {
        const raws = rows.map((r) => r[`${c.name}_raw`]);
        if (raws.some((v) => v === undefined)) throw new Error(`${c.name}_raw が無い`);
        total[c.name] = raws.some((v) => v === null) ? (c.rule === 'decimal_sum' ? (() => { throw new Error(`${c.name} は null にならない`); })() : null) : round2(sumDecimals(raws));
        break;
      }
      case 'state_rank': total[c.name] = AD_STATUS_RANK[Math.max(...vals.map((v) => AD_STATUS_RANK.indexOf(v)))]; break;
      case 'reasons_union': total[c.name] = TOTALS_REASONS.filter((r) => vals.some((v) => v.includes(r))); break;
      case 'dates_concat': total[c.name] = vals.flat(); break;
      case 'jsonb_key_sum': total[c.name] = Object.fromEntries(MASTER_NOTE_KEYS.map((k) => [k, vals.reduce((a, v) => a + v[k], 0)])); break;
      case 'same_all': total[c.name] = vals[0]; break;
      case 'null_in_period': total[c.name] = null; break;
      case 'calculated_at': total[c.name] = input.request.calculated_at; break;
      default: throw new Error(`知らない規則 ${c.rule}`);
    }
  }
  return { status: 200, body: { ...top(input, 'totals', version), total, months: monthsOut(input, version) } };
}

/** /daily: 月ごとの行をつなぐ (足さない)。calculated_at は要求の 1 つの値 */
export function combineDaily(input) {
  checkMonthsInput(input);
  const ms = input.months;
  const versions = new Set([...ms.map((m) => m.meta.calculation_version), ...ms.flatMap((m) => m.rows.map((r) => r.calculation_version))]);
  if (versions.size !== 1) return versionMismatch('CALCULATION_VERSION');
  if (ms.some((m) => m.rows.some((r) => r.master_basis !== MASTER_BASIS))) return versionMismatch('MASTER_BASIS');
  const [version] = versions;
  const rows = ms.flatMap((m) => m.rows.map((r) => ({ ...r, calculated_at: input.request.calculated_at })));
  return { status: 200, body: { ...top(input, 'daily', version), rows, months: monthsOut(input, version) } };
}
