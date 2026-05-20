/**
 * sales-summary.js — biz-ops-overview 全モール売上集計ライブラリ
 *
 * 2026-05-19 中原さん要望 + Codex 助言:
 *   - 売上 = 顧客支払額 (税込、手数料引かず、gmv ベース)
 *   - 期間定義: 前日 (today-1) / 今月累計 (月初〜today-1) / 過去30日 (today-30〜today-1)
 *   - 欠損は N/A (= NULL) と 0 を区別
 *
 * 2026-05-20 拡張 (中原さん要望、EC速報運用のベストプラクティス反映):
 *   - (1) 鮮度ガード + sync失敗表示: データ最終取込時刻 + stale 判定 + 前日未取込モール検知
 *   - (5) 概算粗利速報: 業務売上 × モール別粗利率係数 (f_*_finance_sku_daily 由来、係数方式)
 *   - (3) 今月着地見込み (pacing): 今月累計 ÷ 経過日数 × 当月日数
 *
 * 依存: better-sqlite3、v_mall_sales_daily_unified view (data_synced_at 列が必要)
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
 * モール別 概算粗利率 (係数方式)。
 * source: f_*_finance_sku_daily_v1 の variable_margin ÷ sales (算出期間 2026-03-21〜2026-05-20、60日)。
 *   amazon は profit_amount ÷ sales_principal_jpy (税抜基準なので税込売上に当てると僅かに過大、速報では許容)。
 * ⚠ 月次で更新すること (確定原価/手数料/広告の実績変動を反映)。
 *   更新方法: 各 fact で SUM(variable_margin) / SUM(sales) を直近60-90日で再計算。
 * mercari は finance fact が無いため MARGIN_RATE_DEFAULT で代替 (暫定)。
 */
const MARGIN_RATE = {
  amazon: 0.134,
  rakuten: 0.210,
  yahoo: 0.163,
  aupay: 0.149,
  linegift: 0.469,
  qoo10: 0.211,
};
const MARGIN_RATE_DEFAULT = 0.18; // fact 無しモール (mercari 等) の暫定係数
const MARGIN_RATE_UPDATED = '2026-05'; // ⚠ 係数を更新したらここも更新 (通知に表示される)

/**
 * UTC タイムスタンプ文字列 ('YYYY-MM-DD HH:MM:SS' or ISO 'Z') を epoch ms に。
 * warehouse の updated_at/synced_at は new Date().toISOString() 由来 = UTC。
 * TZ マーカーが無ければ UTC とみなす。
 */
function parseUtcMs(s) {
  if (!s) return NaN;
  let t = String(s).trim();
  if (!t) return NaN;
  // TZ マーカー (Z or ±HH:MM) があればそのまま、無ければ UTC とみなして 'Z' 付与
  if (!(/[zZ]$/.test(t) || /[+-]\d\d:?\d\d$/.test(t))) {
    t = t.replace(' ', 'T') + 'Z';
  }
  return Date.parse(t); // 不正フォーマットは NaN (呼び出し側で Number.isFinite ガード)
}

/** JST 日付文字列 (YYYY-MM-DD) に delta 日加算 */
function addDaysJst(dateStr, delta) {
  const d = new Date(dateStr + 'T00:00:00Z');
  d.setUTCDate(d.getUTCDate() + delta);
  return d.toISOString().slice(0, 10);
}

/** epoch ms → JST 'MM/DD HH:MM' 表示 */
function fmtJst(ms) {
  if (!Number.isFinite(ms)) return '不明';
  const d = new Date(ms + 9 * 3600 * 1000);
  const p = (n) => String(n).padStart(2, '0');
  return `${p(d.getUTCMonth() + 1)}/${p(d.getUTCDate())} ${p(d.getUTCHours())}:${p(d.getUTCMinutes())}`;
}

/** JST 日付文字列 (YYYY-MM-DD) の曜日。引数は必ず date_jst (JST) を渡すこと */
function weekdayJpFromJstDate(dateStr) {
  const idx = new Date(dateStr + 'T00:00:00Z').getUTCDay();
  return ['日', '月', '火', '水', '木', '金', '土'][idx];
}

/** 金額を 億・万 単位で (経営向け表示)。1億以上は「N.NN億」、未満は「N,NNN万」 */
function man(n) {
  const v = Math.round(n || 0);
  if (Math.abs(v) >= 1e8) return (v / 1e8).toFixed(2) + '億';
  return Math.round(v / 1e4).toLocaleString() + '万';
}

/**
 * JST 日付文字列を返す (today-offset_days).
 */
function jstDateString(offsetDays = 0) {
  const d = new Date(Date.now() + 9 * 3600 * 1000 - offsetDays * 86400 * 1000);
  return d.toISOString().slice(0, 10);
}

/**
 * 指定 baseDate (YYYY-MM-DD) の月の月初 (YYYY-MM-01) を返す
 * Codex R1 High #1 反映: 月初 1日に「今月累計」が yesterday 基準で逆転するバグ修正
 */
function jstMonthStartOf(baseDate) {
  return baseDate.slice(0, 7) + '-01';
}

/**
 * 期間集計: v_mall_sales_daily_unified から (mall, SUM, COUNT) を取得
 * Codex R1 Medium #1 反映: 行はあるが値が全 NULL のケースを present:false 判定
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
      const present = (r.sales_count || 0) > 0;
      byMall[r.mall] = { sales: r.sales || 0, units: r.units || 0, present };
      if (present) {
        total += r.sales || 0;
        totalUnits += r.units || 0;
      }
    }
  }
  // (5) 概算粗利速報: present なモールのみ 売上 × 係数 を加算
  let grossProfit = 0;
  for (const m of MALL_ORDER) {
    if (byMall[m].present) {
      grossProfit += byMall[m].sales * (MARGIN_RATE[m] ?? MARGIN_RATE_DEFAULT);
    }
  }
  grossProfit = Math.round(grossProfit);
  const marginRate = total > 0 ? grossProfit / total : null;
  return { byMall, total, totalUnits, grossProfit, marginRate };
}

/**
 * (1) データ鮮度: 最終取込時刻 + stale 判定 + 前日未取込モール。
 * view に data_synced_at 列が無い古いスキーマでは available:false で degrade (本体は継続)。
 */
function getDataFreshness(db, asOf) {
  let lastSync, latestDate, perMall;
  try {
    const r = db.prepare(`
      SELECT MAX(data_synced_at) AS last_sync, MAX(date_jst) AS latest_date
      FROM v_mall_sales_daily_unified
    `).get();
    lastSync = r?.last_sync || null;
    latestDate = r?.latest_date || null;
    perMall = db.prepare(`
      SELECT mall, MAX(date_jst) AS latest_date
      FROM v_mall_sales_daily_unified GROUP BY mall
    `).all();
  } catch (e) {
    return { available: false };
  }
  const ageMs = lastSync ? parseUtcMs(lastSync) : NaN;
  const ageHours = Number.isFinite(ageMs) ? (Date.now() - ageMs) / 3600000 : null;
  // 前日 (asOf) が未取込のモール (= 最新日付が asOf に届いていない)。
  // Codex R1 high 反映: 直近7日以内に実績があったモールのみ監視 (未連携/休止モールのノイズ除外)。
  const recentCutoff = addDaysJst(asOf, -7);
  const mallsBehind = (perMall || [])
    .filter((m) => MALL_ORDER.includes(m.mall) && (m.latest_date || '') < asOf && (m.latest_date || '') >= recentCutoff)
    .map((m) => m.mall);
  const staleHours = Number(process.env.SALES_FRESH_MAX_HOURS) || 26;
  const stale =
    (!!latestDate && latestDate < asOf) ||
    (ageHours != null && ageHours > staleHours);
  return {
    available: true,
    lastSync,
    lastSyncMs: Number.isFinite(ageMs) ? ageMs : null,
    ageHours,
    latestDate,
    mallsBehind,
    stale,
  };
}

/**
 * 全モール 3 期間集計のサマリを返す
 */
export function getSalesSummary(db) {
  const yesterday = jstDateString(1);
  const monthStart = jstMonthStartOf(yesterday);
  const last30Start = jstDateString(30);

  const y = aggregateRange(db, yesterday, yesterday);
  const mtd = aggregateRange(db, monthStart, yesterday);
  const last30 = aggregateRange(db, last30Start, yesterday);

  // (3) 今月着地見込み (pacing): 今月累計 ÷ 経過日数 × 当月日数
  const elapsedDays = parseInt(yesterday.slice(8, 10), 10); // monthStart=1日 起点、asOf の日 = 経過日数
  const y4 = parseInt(yesterday.slice(0, 4), 10);
  const m2 = parseInt(yesterday.slice(5, 7), 10);
  const daysInMonth = new Date(Date.UTC(y4, m2, 0)).getUTCDate();
  const forecast = elapsedDays > 0
    ? {
        sales: Math.round((mtd.total / elapsedDays) * daysInMonth),
        grossProfit: Math.round((mtd.grossProfit / elapsedDays) * daysInMonth),
        elapsedDays,
        daysInMonth,
      }
    : null;

  return {
    asOf: yesterday,
    mallOrder: MALL_ORDER,
    mallLabel: MALL_LABEL,
    freshness: getDataFreshness(db, yesterday),
    yesterday: { date: yesterday, ...y },
    monthToDate: { period: `${monthStart}〜${yesterday}`, forecast, ...mtd },
    last30days: { period: `${last30Start}〜${yesterday}`, ...last30 },
  };
}

/**
 * GChat 通知用の compact 表 (mrkdwn)
 *   memory ヘッダー規約: *XXサマリ ...*
 */
export function formatGChatSummary(summary) {
  const yenFmt = (n) => '¥' + (n || 0).toLocaleString();
  const lines = [];

  // ── ヘッダー (前日・暫定ラベル) ──
  lines.push(`*📊 売上速報 ${summary.asOf}(${weekdayJpFromJstDate(summary.asOf)}) ※暫定値*`);

  // ── (1) 鮮度ガード ──
  const f = summary.freshness;
  const incomplete = !!(f && f.available && (f.stale || (f.mallsBehind && f.mallsBehind.length > 0)));
  if (f && f.available) {
    const ageStr = Number.isFinite(f.ageHours) ? `約${Math.round(f.ageHours)}h前` : '時刻不明';
    lines.push(`🕐 最終取込: ${fmtJst(f.lastSyncMs)} (${ageStr})`);
    if (f.stale) {
      lines.push(`⚠️ データが古い可能性。最新の sync が走っていないかも (確認推奨)`);
    }
    if (f.mallsBehind && f.mallsBehind.length > 0) {
      const labels = f.mallsBehind.map((m) => summary.mallLabel[m] || m).join('・');
      lines.push(`⚠️ 前日分が未取込: ${labels}`);
    }
  } else {
    lines.push(`🕐 最終取込: 取得不可 (view 未更新)`);
  }

  // ── 売上テーブル ──
  lines.push('```');
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

  // ── (5) 概算粗利速報 ──
  const yGp = summary.yesterday.grossProfit;
  const yRate = summary.yesterday.marginRate;
  if (summary.yesterday.total > 0) {
    lines.push(`概算粗利(速報): 前日 約${man(yGp)} (粗利率 約${yRate != null ? Math.round(yRate * 100) : '-'}%) ※係数推定`);
  }

  // ── (3) 今月着地見込み ── (Codex R1 high 反映: データ欠損時は「参考値」に弱める)
  const fc = summary.monthToDate.forecast;
  if (fc) {
    const note = incomplete ? ' ※欠損あり・参考値' : '';
    lines.push(`今月着地見込: 売上 ${man(fc.sales)} / 粗利 ${man(fc.grossProfit)}  (累計 ${man(summary.monthToDate.total)}・${fc.elapsedDays}/${fc.daysInMonth}日)${note}`);
  }

  lines.push(`期間: 前日=${summary.yesterday.date} / 今月=${summary.monthToDate.period} / 過去30日=${summary.last30days.period}`);
  lines.push(`※暫定値 (確定で増加、特にAmazonは当日夜〜翌に伸びる)。粗利は係数推定 (${MARGIN_RATE_UPDATED}更新・月次見直し)。f_sales_by_listing 経由・税込`);
  return lines.join('\n');
}
