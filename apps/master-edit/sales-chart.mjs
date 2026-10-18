/**
 * sales-chart.mjs — 商品の画面の「販売数のグラフ」(2026-10-17 中原さん「販売数量のグラフが見れるようになるといいかも」。読むだけ。書かない)
 *
 * 材料 (1 回の SQL = 読むだけの関数 2 つ):
 *   - 販売数 = Company DB のモールの注文 (core.orders / order_lines) を日 × モールに足した正味の数 (数量 − 取消) = ops.sku_sales_daily (0084 仮)。
 *       数え方は売上日次 (mart.sales_daily・0021) と商品の動き (mart.sku_activity・0042) と同じ規則: 日 = 注文日 (JST)・セットで売れた分は構成品に入る
 *       (セットの画面 = そのセットとして売れた数)。金額は読まない
 *   - 在庫 = ロジザードの毎日の在庫 (締めた日) = ops.sku_stock_anchors (0083 仮・入出庫の履歴と同じ)。単品だけ (セットは在庫を持たない)
 * 数える範囲: Company DB に注文を入れている 7 モール (楽天・Amazon (FBA も)・Yahoo!・au PAY・Qoo10・LINE ギフト・メルカリ)。
 *   卸・そのほかの NE の店・FBA 納品は入らない = 上の「売れた数 (7 日・30 日)」(商品管理リスト = NE の全部の店 + FBA) と少し違う (画面にそう書く)
 *   注文が日ごとにそろっているのは 2025-01-01 から (SALES_FROM・LINE ギフト 2026-02-07・Qoo10 2026-03-01 から)。その前の期間は空欄 (0 と出さない)
 * 期間: 今日は入れない (前日まで = 7 日・30 日の数と同じ)。週 = 月曜はじまりの 52 週・月 = 24 か月・日 = 90 日。いちばん新しい週・月は途中 (partial)
 * 在庫の線: 期間の最後の締めの日の数 (週・月) / その日の数 (日)。締めの無い日は線を切る。欠品 = 締めた日の数が 0 (その日数を数える)
 */
import { ListTimeoutError } from './deadline.mjs';

export const SALES_FROM = '2025-01-01';
export const VIEW_SIZES = Object.freeze({ week: 52, month: 24, day: 90 });
/** 積み上げの順 (下から)。その他 = au PAY・Qoo10・LINE ギフト・メルカリ・店の分からない注文 */
export const MALL_SERIES = Object.freeze([
  Object.freeze({ key: 'rakuten', label: '楽天' }), Object.freeze({ key: 'amazon', label: 'Amazon' }),
  Object.freeze({ key: 'yahoo', label: 'Yahoo!' }), Object.freeze({ key: 'other', label: 'その他' }),
]);
export const OTHER_MALLS_LABEL = 'au PAY・Qoo10・LINE ギフト・メルカリ';
const DAY_MS = 864e5;

const mallGroup = (m) => (m === 'rakuten' || m === 'amazon' || m === 'yahoo' ? m : 'other');
const utc = (ymd) => Date.parse(`${ymd}T00:00:00Z`);
const ymdOf = (t) => new Date(t).toISOString().slice(0, 10);
export const addDays = (ymd, n) => ymdOf(utc(ymd) + n * DAY_MS);
/** JST の今日 'YYYY-MM-DD' */
export const jstToday = (now) => ymdOf(new Date(now).getTime() + 9 * 3600e3);
/** その日を含む週の月曜 */
export const mondayOf = (ymd) => addDays(ymd, -((new Date(utc(ymd)).getUTCDay() + 6) % 7));
const monthStart = (ymd) => `${ymd.slice(0, 7)}-01`;
function addMonths(ym01, n) {
  const [y, m] = ym01.split('-').map(Number);
  return ymdOf(Date.UTC(y, m - 1 + n, 1));
}
const lastOfMonth = (ym01) => addDays(addMonths(ym01, 1), -1);

/** 期間の区切り (古い順)。[{ from, to }] */
export function bucketsOf(unit, through) {
  const n = VIEW_SIZES[unit];
  const out = [];
  if (unit === 'day') for (let i = n - 1; i >= 0; i--) { const d = addDays(through, -i); out.push({ from: d, to: d }); }
  else if (unit === 'week') { const m0 = mondayOf(through); for (let i = n - 1; i >= 0; i--) { const f = addDays(m0, -7 * i); out.push({ from: f, to: addDays(f, 6) }); } }
  else { const m0 = monthStart(through); for (let i = n - 1; i >= 0; i--) { const f = addMonths(m0, -i); out.push({ from: f, to: lastOfMonth(f) }); } }
  return out;
}

const zeroQty = () => Object.fromEntries(MALL_SERIES.map((s) => [s.key, 0]));

/**
 * グラフの数 (純粋な関数)。
 * @param o.sales   [{ day: 'YYYY-MM-DD', mall, qty, viaSet }] (ops.sku_sales_daily)
 * @param o.anchors [{ day, status, qty }] (ops.sku_stock_anchors・null = 在庫を出さない = セット)
 * @param o.through 'YYYY-MM-DD' この日まで (前日)
 * @returns { through, dataFrom, stockFrom, views: { week|month|day: { buckets: [...], totals } } }
 *   bucket = { from, to, partial (期間が through を越える = 途中), noData (全部 SALES_FROM より前), partialData (途中から),
 *              qty: { rakuten, amazon, yahoo, other } | null, total | null, viaSet | null, stock: { end, endDay, days, zeroDays } | null }
 */
export function buildSalesChart({ sales, anchors, through }) {
  const byDay = new Map();
  for (const r of sales || []) {
    if (!r || !r.day || r.day > through) continue;
    let x = byDay.get(r.day);
    if (!x) { x = { qty: zeroQty(), viaSet: 0 }; byDay.set(r.day, x); }
    x.qty[mallGroup(r.mall)] += Number(r.qty) || 0;
    x.viaSet += Number(r.viaSet) || 0;
  }
  const stockDays = anchors ? anchors.filter((a) => a && a.status === 'complete' && a.qty != null && a.day <= through).map((a) => ({ day: a.day, qty: Number(a.qty) })).sort((a, b) => (a.day < b.day ? -1 : 1)) : null;
  const views = {};
  for (const unit of Object.keys(VIEW_SIZES)) {
    const totals = { ...zeroQty(), total: 0, viaSet: 0 };
    const buckets = bucketsOf(unit, through).map(({ from, to }) => {
      const noData = to < SALES_FROM;
      const b = { from, to, partial: to > through, noData, partialData: !noData && from < SALES_FROM, qty: null, total: null, viaSet: null, stock: null };
      if (!noData) {
        const q = zeroQty(); let via = 0;
        for (let d = from < SALES_FROM ? SALES_FROM : from; d <= to && d <= through; d = addDays(d, 1)) {
          const x = byDay.get(d);
          if (!x) continue;
          for (const s of MALL_SERIES) q[s.key] += x.qty[s.key];
          via += x.viaSet;
        }
        b.qty = q; b.total = MALL_SERIES.reduce((s, m) => s + q[m.key], 0); b.viaSet = via;
        for (const s of MALL_SERIES) totals[s.key] += q[s.key];
        totals.total += b.total; totals.viaSet += via;
      }
      if (stockDays) {
        const inB = stockDays.filter((a) => a.day >= from && a.day <= to);
        if (inB.length) { const last = inB[inB.length - 1]; b.stock = { end: last.qty, endDay: last.day, days: inB.length, zeroDays: inB.filter((a) => a.qty <= 0).length }; }
      }
      return b;
    });
    views[unit] = { buckets, totals };
  }
  return { through, dataFrom: SALES_FROM, stockFrom: stockDays && stockDays.length ? stockDays[0].day : null, views };
}

/** DB の関数の誤り → 画面の言葉。関数が無い (0084 / 0083 の前)・権限が無い (流し直しの前) = no_privilege。期限切れ = timeout */
function chartError(e) {
  if (e instanceof ListTimeoutError || (e && e.code === '57014')) return { ok: false, reason: 'timeout', error: '販売数の読み出しに時間がかかりすぎました。少し待ってから画面を開き直してください' };
  if (e && (e.code === '42883' || e.code === '42501')) return { ok: false, reason: 'no_privilege', error: '画面のロールに販売数を読む関数がまだありません (migration 0084 と create-master-edit-roles.mjs の流し直しが要ります)' };
  console.error(`[master-edit] 販売数のグラフを読めない: ${e && e.message}`);
  return { ok: false, reason: 'error', error: '販売数を読めません' };
}

/**
 * 1 つの商品のグラフを読む (SQL は 1 回)。sku = { sku_id, sku_kind }。db = 期限つきの db (router の deadlineDb) でよい
 * @returns { ok: true, kind, ...buildSalesChart } / { ok: false, reason, error }
 */
export async function readSalesChart(db, sku, { now = Date.now() } = {}) {
  try {
    const through = addDays(jstToday(now), -1);
    const from = bucketsOf('month', through)[0].from;   // いちばん古い期間 (24 か月前の月初) から = 週・日の期間も入る
    const withStock = sku.sku_kind === 'single';
    const r = await db.query(`select 's' as k, date_jst::text as d, mall, qty::bigint as qty, via_set::bigint as via, null::text as status from ops.sku_sales_daily($1::bigint, $2::date)
      union all
      select 'z', snapshot_date::text, null, qty::bigint, null, status from ops.sku_stock_anchors($1::bigint, $2::date) where $3::boolean`, [sku.sku_id, from, withStock]);
    const sales = [], anchors = withStock ? [] : null;
    for (const x of r.rows) {
      if (x.k === 's') sales.push({ day: x.d, mall: x.mall, qty: Number(x.qty), viaSet: Number(x.via) });
      else if (anchors) anchors.push({ day: x.d, status: x.status, qty: x.qty == null ? null : Number(x.qty) });
    }
    return { ok: true, kind: sku.sku_kind, malls: MALL_SERIES, otherMalls: OTHER_MALLS_LABEL, ...buildSalesChart({ sales, anchors, through }) };
  } catch (e) {
    return chartError(e);
  }
}
