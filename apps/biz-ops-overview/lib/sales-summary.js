/**
 * sales-summary.js — biz-ops-overview 全モール売上集計ライブラリ
 *
 * 2026-05-19 中原さん要望 + Codex 助言:
 *   - 売上 = 顧客支払額 (税込、手数料引かず、gmv ベース)
 *   - 期間定義: 前日 (today-1) / 今月累計 (月初〜today-1) / 過去30日 (today-30〜today-1)
 *   - メルカリは当面除外 (NE 連携は Phase 2)
 *   - 欠損は N/A (= NULL) と 0 を区別、SUM の自然動作で NULL は 0 扱いでも OK だが
 *     「その期間に行がない」場合は数値 0 を返し、表示時に N/A 化を別途判定
 *
 * 依存: better-sqlite3、v_mall_sales_daily_unified view
 * 使い方:
 *   import { getSalesSummary } from './lib/sales-summary.js';
 *   const summary = getSalesSummary(db);
 *   // { yesterday: { date: '2026-05-18', total: 1000000, byMall: { amazon: 500000, ... } },
 *   //   monthToDate: { period: '2026-05-01〜2026-05-18', total: ..., byMall: ... },
 *   //   last30days: { period: '2026-04-19〜2026-05-18', total: ..., byMall: ... } }
 */

// PR #155 patch v2: f_sales_by_listing 経由に変更、メルカリ含む 7 モール対応
const MALL_ORDER = ['amazon', 'rakuten', 'yahoo', 'aupay', 'linegift', 'qoo10', 'mercari'];
const MALL_LABEL = {
  amazon: 'Amazon',
  rakuten: '楽天',
  yahoo: 'Yahoo!',
  aupay: 'au PAY',
  linegift: 'LINEギフト',
  qoo10: 'Qoo10',
  mercari: 'メルカリ',
};

/**
 * JST 日付文字列を返す (today-offset_days).
 *   offset=0 → today、offset=1 → yesterday、offset=30 → 30日前
 */
function jstDateString(offsetDays = 0) {
  const d = new Date(Date.now() + 9 * 3600 * 1000 - offsetDays * 86400 * 1000);
  return d.toISOString().slice(0, 10);
}

/**
 * 指定 baseDate (YYYY-MM-DD) の月の月初 (YYYY-MM-01) を返す
 * Codex R1 High #1 反映: 月初 1日に「今月累計」が yesterday 基準で逆転するバグ修正
 *   例: today=2026-06-01 (JST) なら yesterday=2026-05-31、monthStart は 2026-05-01 にすべき (yesterday の月初)
 *   旧実装は today の月初を使うので fromDate=2026-06-01 > toDate=2026-05-31 で集計空、表示逆転
 */
function jstMonthStartOf(baseDate) {
  return baseDate.slice(0, 7) + '-01';
}

/**
 * 期間集計: v_mall_sales_daily_unified から (mall, SUM, COUNT) を取得
 * Codex R1 Medium #1 反映: 行はあるが値が全 NULL のケースを present:false 判定するため
 *   COUNT(sales_gross_jpy_incl) を別途取得して sales_count===0 で N/A 化
 */
function aggregateRange(db, fromDate, toDate) {
  const rows = db.prepare(`
    SELECT
      mall,
      ROUND(SUM(sales_gross_jpy_incl), 0) AS sales,
      SUM(units_net_sold) AS units,
      COUNT(sales_gross_jpy_incl) AS sales_count
    FROM v_mall_sales_daily_unified
    WHERE date_jst >= ? AND date_jst <= ?
    GROUP BY mall
  `).all(fromDate, toDate);
  const byMall = {};
  let total = 0;
  let totalUnits = 0;
  for (const m of MALL_ORDER) byMall[m] = { sales: 0, units: 0, present: false };
  for (const r of rows) {
    if (byMall[r.mall]) {
      // sales_count===0 (= 全行が NULL) を欠損扱い、>0 で present:true
      const present = (r.sales_count || 0) > 0;
      byMall[r.mall] = { sales: r.sales || 0, units: r.units || 0, present };
      if (present) {
        total += r.sales || 0;
        totalUnits += r.units || 0;
      }
    }
  }
  return { byMall, total, totalUnits };
}

/**
 * 全モール 3 期間集計のサマリを返す
 *   - yesterday: 前日 (today-1 のみ)
 *   - monthToDate: 月初〜today-1
 *   - last30days: today-30〜today-1
 */
export function getSalesSummary(db) {
  const yesterday = jstDateString(1);
  // Codex R1 High #1 反映: monthStart は yesterday 基準で算出 (月初 1日の逆転バグ防止)
  const monthStart = jstMonthStartOf(yesterday);
  const last30Start = jstDateString(30);

  const y = aggregateRange(db, yesterday, yesterday);
  const mtd = aggregateRange(db, monthStart, yesterday);
  const last30 = aggregateRange(db, last30Start, yesterday);

  return {
    asOf: yesterday,
    mallOrder: MALL_ORDER,
    mallLabel: MALL_LABEL,
    yesterday: { date: yesterday, ...y },
    monthToDate: { period: `${monthStart}〜${yesterday}`, ...mtd },
    last30days: { period: `${last30Start}〜${yesterday}`, ...last30 },
  };
}

/**
 * GChat 通知用の compact 表 (mrkdwn)
 *   memory ヘッダー規約: *XXサマリ ...*
 *   片方失敗で全落ち防止のため在庫サマリとは独立メッセージ (Codex 助言)
 */
export function formatGChatSummary(summary) {
  const yenFmt = (n) => '¥' + (n || 0).toLocaleString();
  const lines = [];
  lines.push(`*売上サマリ ${summary.asOf} (JST)*`);
  lines.push('```');
  // 表 (mall | 前日 | 今月 | 30日)
  lines.push('モール        | 前日       | 今月累計    | 過去30日');
  lines.push('-------------+------------+-------------+-------------');
  for (const m of summary.mallOrder) {
    const label = (summary.mallLabel[m] || m).padEnd(12, ' ');
    const y = summary.yesterday.byMall[m];
    const mt = summary.monthToDate.byMall[m];
    const l30 = summary.last30days.byMall[m];
    const yStr = y.present ? yenFmt(y.sales).padStart(10, ' ') : '       N/A';
    const mtStr = mt.present ? yenFmt(mt.sales).padStart(11, ' ') : '        N/A';
    const l30Str = l30.present ? yenFmt(l30.sales).padStart(11, ' ') : '        N/A';
    lines.push(`${label} | ${yStr} | ${mtStr} | ${l30Str}`);
  }
  lines.push('-------------+------------+-------------+-------------');
  lines.push(`合計         | ${yenFmt(summary.yesterday.total).padStart(10, ' ')} | ${yenFmt(summary.monthToDate.total).padStart(11, ' ')} | ${yenFmt(summary.last30days.total).padStart(11, ' ')}`);
  lines.push('```');
  lines.push(`期間: 前日=${summary.yesterday.date} / 今月累計=${summary.monthToDate.period} / 過去30日=${summary.last30days.period}`);
  lines.push('※ 顧客支払額 (税込、手数料引かず)。f_sales_by_listing 経由で各モール API 直接集計の業務目線真の売上');
  return lines.join('\n');
}
