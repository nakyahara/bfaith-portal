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
// 監査PR-11: MALL_ORDER/MALL_LABEL のハードコードを dim_mall (in_daily_summary=1) に集約。
// 値は従来定数と同一 (amazon→…→mercari の7モール、同ラベル)。
import { loadDimMall } from '../../../lib/dim-mall.js';
function mallOrder(db) { return loadDimMall(db).dailySummaryOrder; }
function mallLabelMap(db) {
  const dim = loadDimMall(db);
  return Object.fromEntries(dim.dailySummaryOrder.map(k => [k, dim.labelOf(k)]));
}

/**
 * 概算粗利率 (あらり率)。中原さんの社内運用値「あらり率(税抜) 13%」を採用。
 *   - 粗利率は税抜で見るのが標準 (会計の売上高=税抜)。中原さんの「全体13%」も税抜ベース。
 *   - 速報の売上金額は税込 (現金入金額) のままなので、率を当てる時だけ税抜換算する。
 *   - 概算粗利(税抜) = 税込売上 ÷ TAX_DIVISOR × GROSS_MARGIN_RATE_TAX_EXCL。
 * 参考: finance fact の variable_margin から積み上げると税抜 ~16% (ただし広告費控除前)。
 *   13% は広告費等を引いた後の中原さんの実感/P/L 値。変えたい時はここの 0.13 を変更。
 */
const GROSS_MARGIN_RATE_TAX_EXCL = 0.13;
const TAX_DIVISOR = 1.10; // 税込→税抜 概算 (大半10%、軽減8%は誤差として許容)

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

/** 'YYYY-MM-DD' → 'M/D' (スマホ向けに短く) */
function mmdd(dateStr) {
  return `${parseInt(dateStr.slice(5, 7), 10)}/${parseInt(dateStr.slice(8, 10), 10)}`;
}

/**
 * 半角 ASCII を全角に変換 (スペース→全角スペース)。
 * 理由: GChat 等幅は日本語(全角)を半角ちょうど2倍で描画しないため、半角ラベルを左に置くと
 *   右側の数字がズレる。ラベルを全て全角に揃えると 1 文字=一定幅になり、左ラベル+右数字でも桁が揃う。
 */
function toFullWidth(s) {
  return String(s)
    .replace(/[\x21-\x7E]/g, (c) => String.fromCharCode(c.charCodeAt(0) + 0xFEE0))
    .replace(/ /g, '　');
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
  for (const m of mallOrder(db)) byMall[m] = { sales: 0, units: 0, present: false };
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
  // (5) 概算粗利速報: あらり率(税抜)13% を税抜売上に当てる。
  //   税抜売上 = 税込売上 ÷ TAX_DIVISOR。粗利率は税抜表示 (中原さんの「13%」と同じ土俵)。
  const salesExcl = total / TAX_DIVISOR;
  const grossProfit = Math.round(salesExcl * GROSS_MARGIN_RATE_TAX_EXCL);
  const marginRate = total > 0 ? GROSS_MARGIN_RATE_TAX_EXCL : null; // 税抜あらり率 (固定)
  return { byMall, total, totalUnits, grossProfit, marginRate };
}

// 「常時稼働」判定: 前日を除く直近7日のうち、これ以上の日数データがあれば毎日売れているモールとみなす。
//   常時稼働モールが前日データ無し = 未取込(同期失敗の疑い)。
//   そうでない(たまにしか売れない)モールが前日データ無し = 売上0。
const REGULAR_DAYS_THRESHOLD = (() => {
  const n = parseInt(process.env.SALES_REGULAR_DAYS, 10);
  return Number.isFinite(n) ? Math.min(7, Math.max(1, n)) : 6; // 1..7 に clamp、既定 6
})();

/**
 * (1) データ鮮度: 最終取込時刻 + stale 判定 + 前日「未取込」モール (売上0 と区別)。
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
    // 前日(asOf)を除く直近7日 [asOf-7, asOf-1] でモール別の実績日数を数える。
    const winStart = addDaysJst(asOf, -7);
    const winEnd = addDaysJst(asOf, -1);
    perMall = db.prepare(`
      SELECT mall,
             MAX(date_jst) AS latest_date,
             COUNT(DISTINCT CASE WHEN date_jst >= ? AND date_jst <= ? THEN date_jst END) AS recent_days
      FROM v_mall_sales_daily_unified GROUP BY mall
    `).all(winStart, winEnd);
  } catch (e) {
    return { available: false };
  }
  const ageMs = lastSync ? parseUtcMs(lastSync) : NaN;
  const ageHours = Number.isFinite(ageMs) ? (Date.now() - ageMs) / 3600000 : null;
  const staleHours = Number(process.env.SALES_FRESH_MAX_HOURS) || 26;
  const stale =
    (!!latestDate && latestDate < asOf) ||
    (ageHours != null && ageHours > staleHours);

  // 常時稼働モール集合 (直近7日で REGULAR_DAYS_THRESHOLD 日以上データあり)
  const order = mallOrder(db);
  const regularMalls = new Set(
    (perMall || [])
      .filter((m) => order.includes(m.mall) && (m.recent_days || 0) >= REGULAR_DAYS_THRESHOLD)
      .map((m) => m.mall)
  );
  const latestByMall = {};
  for (const m of perMall || []) latestByMall[m.mall] = m.latest_date || '';

  // 「前日未取込」= 常時稼働なのに前日データが無いモール (= 同期失敗の疑い)。
  //   全体 stale 時は全モール未取込扱い (パイプライン未実行)。
  const mallsBehind = order.filter((m) => {
    const missingYesterday = (latestByMall[m] || '') < asOf;
    if (!missingYesterday) return false;
    return stale || regularMalls.has(m);
  });

  return {
    available: true,
    lastSync,
    lastSyncMs: Number.isFinite(ageMs) ? ageMs : null,
    ageHours,
    latestDate,
    regularMalls: [...regularMalls],
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
    mallOrder: mallOrder(db),
    mallLabel: mallLabelMap(db),
    freshness: getDataFreshness(db, yesterday),
    yesterday: { date: yesterday, ...y },
    monthToDate: { period: `${monthStart}〜${yesterday}`, forecast, ...mtd },
    last30days: { period: `${last30Start}〜${yesterday}`, ...last30 },
  };
}

/**
 * GChat 通知用テキスト (スマホ最適化、2026-05-20 改訂)。
 *   - 等幅コードブロックの横ワイド表はスマホで折り返して崩れるため廃止。
 *   - 前日 / 今月累計 / 過去30日 を「別セクション」に分割、1モール1行 (ラベル: 金額) の縦並び。
 *   - 各セクションは金額降順 (大きいモールが上)、欠損は N/A で末尾。
 *   - 前日は ¥フル (日次は精度重視)、今月累計/過去30日は 億・万 (大きい値の可読性重視)。
 *   memory ヘッダー規約: *XXサマリ ...* / 経営向け金額は 億・万 ([[feedback-japanese-currency-units]])。
 */
export function formatGChatSummary(summary) {
  const yen = (n) => '¥' + Math.round(n || 0).toLocaleString();
  const lines = [];

  // ── ヘッダー + 鮮度 ──
  lines.push(`*📊 売上速報 ${mmdd(summary.asOf)}(${weekdayJpFromJstDate(summary.asOf)})* ※暫定値`);
  const f = summary.freshness;
  const incomplete = !!(f && f.available && (f.stale || (f.mallsBehind && f.mallsBehind.length > 0)));
  if (f && f.available) {
    const ageStr = Number.isFinite(f.ageHours) ? `約${Math.round(f.ageHours)}h前` : '時刻不明';
    lines.push(`🕐 最終取込 ${fmtJst(f.lastSyncMs)}(${ageStr})`);
    if (f.stale) lines.push(`⚠️ データが古い可能性 (sync 未実行かも・確認推奨)`);
    if (f.mallsBehind && f.mallsBehind.length > 0) {
      lines.push(`⚠️ 前日未取込: ${f.mallsBehind.map((m) => summary.mallLabel[m] || m).join('・')}`);
    }
  } else {
    lines.push('🕐 最終取込 取得不可 (view 未更新)');
  }

  // ── セクション生成 (金額降順、欠損末尾) ──
  //   モール名を左 (全角統一で幅を均一化) → 数字を右に右揃え。
  //   全角ラベルを文字数で揃え + 数字は ASCII 等幅で右揃えなので、両カラムとも桁が揃う。
  //   resolveEmpty(m): データ無しモールの表示を解決 (前日のみ 売上0/未取込 を出し分け、他は N/A)。
  const section = (title, totalText, range, fmt, resolveEmpty) => {
    lines.push('');
    lines.push(`*▼ ${title}｜合計 ${totalText}*`);
    const ordered = [...summary.mallOrder].sort((a, b) => {
      const A = range.byMall[a], B = range.byMall[b];
      if (A.present !== B.present) return A.present ? -1 : 1;
      return (B.sales || 0) - (A.sales || 0);
    });
    const fwLabels = ordered.map((m) => toFullWidth(summary.mallLabel[m] || m));
    const labelW = Math.max(...fwLabels.map((l) => [...l].length));
    const valStrs = ordered.map((m) =>
      range.byMall[m].present ? fmt(range.byMall[m].sales) : (resolveEmpty ? resolveEmpty(m) : 'N/A')
    );
    const valW = Math.max(...valStrs.map((s) => s.length));
    lines.push('```');
    ordered.forEach((m, i) => {
      const labelPad = fwLabels[i] + '　'.repeat(labelW - [...fwLabels[i]].length);
      lines.push(`${labelPad} ${valStrs[i].padStart(valW)}`);
    });
    lines.push('```');
  };

  // 前日 (¥フル)。データ無しは「未取込」(常時稼働モールが欠損 or 全体stale) と「¥0」(売上0) を区別。
  //   鮮度が取得不能 (古いスキーマ等) のときは判定材料が無いため N/A 維持 (0 と誤表示しない)。
  const behindSet = new Set((f && f.available && f.mallsBehind) || []);
  const resolveYesterday = (f && f.available)
    ? (m) => (behindSet.has(m) ? '未取込' : yen(0))
    : undefined;
  section(`前日 ${mmdd(summary.asOf)}`, yen(summary.yesterday.total), summary.yesterday, yen, resolveYesterday);
  if (summary.yesterday.total > 0) {
    const r = summary.yesterday.marginRate;
    lines.push(`└ 概算粗利 約${man(summary.yesterday.grossProfit)} (粗利率${r != null ? Math.round(r * 100) : '-'}%・税抜)`);
  }

  // 今月累計 (億・万)
  section('今月累計', man(summary.monthToDate.total), summary.monthToDate, man);
  const fc = summary.monthToDate.forecast;
  if (fc) {
    lines.push(`└ 着地見込 ${man(fc.sales)} (粗利 ${man(fc.grossProfit)})${incomplete ? ' ※参考値' : ''}`);
  }

  // 過去30日 (億・万)
  section('過去30日', man(summary.last30days.total), summary.last30days, man);

  // ── フッター ──
  lines.push('');
  lines.push('※直近72時間(特にAmazon)は実際より低めに出ます: 支払い未承認(Pending)の注文は金額が後から計上されるため。数日かけて確定値に近づきます');
  lines.push('※粗利率は税抜13%(社内基準)で概算');
  return lines.join('\n');
}
