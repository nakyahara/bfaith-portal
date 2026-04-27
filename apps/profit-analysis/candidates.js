/**
 * 商品収益性ダッシュボード タブB: 候補リスト算出
 *
 * mirror_products / mirror_sales_daily / mirror_sales_monthly /
 * mirror_stock_monthly_snapshot / product_retirement_status を JOIN して
 * 商品単位の基礎メトリクスを取得し、5分類 + 評価不能 + 分類外(理由別) へ振り分ける。
 *
 * 設計書セクション14 / Codex 12回目レビュー反映版に準拠。
 *   - 売上分類を一次軸に適用
 *   - 撤退閾値マトリクスを売上分類別に使う
 *   - 早期警戒は全商品共通、季節性・新商品は判定保留
 *   - 楽天利益統一評価: 売価×0.9 − 原価 − 送料
 *
 * 公開 API:
 *   getCandidates(db, { salesClass, periodDays, today }) → 分類済みの候補リスト
 *
 * 内部でテスト対象にする純関数:
 *   classifyProduct(row, thresholds, { today, periodDays })
 *   applyEarlyWarning(candidates, earlyWarning)
 */

// ─── 定数・ヘルパー ───

/** GMROI 年率化に使う日数（うるう年 0.27% 差は実務影響なしで定数扱い） */
export const DAYS_PER_YEAR = 365;

// ─── 季節性自動判定パラメータ ───
//   過去 SEASONAL_LOOKBACK_MONTHS の月別販売分布から、
//   上位 SEASONAL_TOP_MONTHS の合計が SEASONAL_RATIO_THRESHOLD 以上を占めれば季節商品扱い。
//   total が SEASONAL_MIN_TOTAL 未満（実績不足）の商品は判定不能で auto = なし。
//   手動の seasonality_flag が立っていればそちらが優先。
export const SEASONAL_LOOKBACK_MONTHS = 24;
export const SEASONAL_TOP_MONTHS = 3;
export const SEASONAL_RATIO_THRESHOLD = 0.6;
export const SEASONAL_MIN_TOTAL = 30;

/**
 * GMROI を年率（%）で算出する。設計書§14 の閾値（優良 >200、観察 100-200、
 * 仕入 retire <30 / warn <50）は全て industry-standard の年率前提。
 *
 *   gmroi(年率) = 期間粗利 × (DAYS_PER_YEAR / periodDays) / 平均在庫金額 × 100
 *
 * @returns {number|null} 年率 GMROI（%）、計算不能なら null
 */
export function annualizeGmroiPercent(periodProfit, avgStockValue, periodDays) {
  if (!Number.isFinite(avgStockValue) || avgStockValue <= 0) return null;
  if (!Number.isFinite(periodDays) || periodDays <= 0) return null;
  return (Number(periodProfit) * (DAYS_PER_YEAR / periodDays)) / avgStockValue * 100;
}

// ─── SQL 取得 ───

/**
 * 候補リストの raw rows を取得する。
 * 商品マスタ + 販売集計 + 在庫スナップショット + 撤退ステータス を CTE で結合。
 */
export function fetchCandidatesRaw(db, { salesClass, periodDays = 90, today }) {
  if (!['1', '2', '3'].includes(String(salesClass))) {
    throw new Error(`salesClass は 1|2|3 のいずれか: ${salesClass}`);
  }
  // Codex PR2c Round 2 Low 反映: モジュール境界でも period_days <= 90 を強制
  // mirror_sales_daily は直近90日分のみ保持するため、90日超だと
  // 分子の期間販売数と分母の periodDays がミスマッチして分類が壊れる。
  if (!Number.isInteger(periodDays) || periodDays <= 0 || periodDays > 90) {
    throw new Error(`periodDays は 1〜90 の整数 (mirror日次の90日制約): ${periodDays}`);
  }
  const todayDate = today ? new Date(today) : new Date();
  const iso = (d) => d.toISOString().slice(0, 10);

  const periodStart = new Date(todayDate);
  periodStart.setDate(periodStart.getDate() - periodDays);
  const periodStartStr = iso(periodStart);

  const d30 = new Date(todayDate);
  d30.setDate(d30.getDate() - 30);
  const d30Str = iso(d30);

  const d90 = new Date(todayDate);
  d90.setDate(d90.getDate() - 90);
  const d90Str = iso(d90);

  // 移動平均在庫は直近6ヶ月を想定（snapshot_months で実数を把握してフラグ出し）
  //   今日が 2026-04 のとき「直近6ヶ月」= 2025-11 〜 2026-04 の6ヶ月
  //   Codex PR2c Round 2 Medium 反映: 月末オーバーフロー対策
  //     setMonth(-5) だと 7/31 が 2/31 → 3/03 に流れて2月が除外される不具合
  //     ローカル年月から始月オブジェクトを作って toISOString().slice(0, 7) で YYYY-MM
  //
  //   Codex R1 Medium (本PR) 反映:
  //     起点はローカル年月で取得する。UTC ベースだと JST 月初数時間で「対象24ヶ月」と
  //     isOffSeason の current month (ローカル) がズレるため、両方ともローカルに統一。
  //     YYYY-MM 文字列化のみ UTC を使う（Date.UTC 由来のオブジェクトは TZ 影響を受けない）。
  const y = todayDate.getFullYear();
  const m = todayDate.getMonth(); // 0-indexed, ローカル
  const stock6mStartStr = new Date(Date.UTC(y, m - 5, 1)).toISOString().slice(0, 7);

  // 季節性自動判定用の遡り開始月（今日の月初から SEASONAL_LOOKBACK_MONTHS 前）
  const seasonalStartStr = new Date(Date.UTC(y, m - (SEASONAL_LOOKBACK_MONTHS - 1), 1)).toISOString().slice(0, 7);

  const sql = `
    WITH
      daily_agg AS (
        SELECT
          商品コード,
          MAX(日付) as daily_last_sale,
          SUM(CASE WHEN 日付 >= ? THEN 数量 ELSE 0 END) as sales_period,
          SUM(CASE WHEN 日付 >= ? THEN 数量 ELSE 0 END) as sales_30d,
          SUM(CASE WHEN 日付 >= ? THEN 数量 ELSE 0 END) as sales_90d
        FROM mirror_sales_daily
        WHERE データ種別 = 'by_product'
        GROUP BY 商品コード
      ),
      monthly_agg AS (
        SELECT 商品コード, MAX(月) as monthly_last_month
        FROM mirror_sales_monthly
        WHERE データ種別 = 'by_product'
        GROUP BY 商品コード
      ),
      stock_recent AS (
        SELECT 商品コード,
               AVG(月末在庫数) as avg_stock,
               COUNT(*) as stock_snapshot_months
        FROM mirror_stock_monthly_snapshot
        WHERE 年月 >= ?
        GROUP BY 商品コード
      ),
      latest_stock AS (
        SELECT 商品コード, 月末在庫数 as latest_stock
        FROM (
          SELECT 商品コード, 月末在庫数,
                 ROW_NUMBER() OVER (PARTITION BY 商品コード ORDER BY 年月 DESC) as rn
          FROM mirror_stock_monthly_snapshot
        )
        WHERE rn = 1
      ),
      seasonal_dist AS (
        -- 過去 24ヶ月の月別販売数を 12ヶ月成分（1〜12月）に集計。
        -- 季節性自動判定（detectSeasonality）が JS 側で 60% 集中をチェックする。
        SELECT 商品コード,
               SUM(数量) AS seasonal_total,
               SUM(CASE WHEN substr(月, 6, 2) = '01' THEN 数量 ELSE 0 END) AS m01,
               SUM(CASE WHEN substr(月, 6, 2) = '02' THEN 数量 ELSE 0 END) AS m02,
               SUM(CASE WHEN substr(月, 6, 2) = '03' THEN 数量 ELSE 0 END) AS m03,
               SUM(CASE WHEN substr(月, 6, 2) = '04' THEN 数量 ELSE 0 END) AS m04,
               SUM(CASE WHEN substr(月, 6, 2) = '05' THEN 数量 ELSE 0 END) AS m05,
               SUM(CASE WHEN substr(月, 6, 2) = '06' THEN 数量 ELSE 0 END) AS m06,
               SUM(CASE WHEN substr(月, 6, 2) = '07' THEN 数量 ELSE 0 END) AS m07,
               SUM(CASE WHEN substr(月, 6, 2) = '08' THEN 数量 ELSE 0 END) AS m08,
               SUM(CASE WHEN substr(月, 6, 2) = '09' THEN 数量 ELSE 0 END) AS m09,
               SUM(CASE WHEN substr(月, 6, 2) = '10' THEN 数量 ELSE 0 END) AS m10,
               SUM(CASE WHEN substr(月, 6, 2) = '11' THEN 数量 ELSE 0 END) AS m11,
               SUM(CASE WHEN substr(月, 6, 2) = '12' THEN 数量 ELSE 0 END) AS m12
        FROM mirror_sales_monthly
        WHERE データ種別 = 'by_product'
          AND 月 >= ?
        GROUP BY 商品コード
      )
    SELECT
      p.商品コード, p.商品名, p.売上分類,
      p.標準売価, p.原価, p.送料, p.消費税率,
      p.seasonality_flag, p.season_months,
      p.new_product_flag, p.new_product_launch_date,
      p.仕入先コード, p.在庫数 as 管理在庫数,
      da.daily_last_sale,
      COALESCE(da.sales_period, 0) as sales_period,
      COALESCE(da.sales_30d, 0) as sales_30d,
      COALESCE(da.sales_90d, 0) as sales_90d,
      ma.monthly_last_month,
      COALESCE(sr.avg_stock, 0) as avg_stock,
      COALESCE(sr.stock_snapshot_months, 0) as stock_snapshot_months,
      COALESCE(ls.latest_stock, 0) as latest_stock,
      COALESCE(sd.seasonal_total, 0) as seasonal_total,
      sd.m01, sd.m02, sd.m03, sd.m04, sd.m05, sd.m06,
      sd.m07, sd.m08, sd.m09, sd.m10, sd.m11, sd.m12,
      prs.status as retirement_status,
      prs.next_review_date as retirement_next_review,
      prs.decided_at as retirement_decided_at,
      prs.reason as retirement_reason
    FROM mirror_products p
    LEFT JOIN daily_agg da ON p.商品コード = da.商品コード COLLATE NOCASE
    LEFT JOIN monthly_agg ma ON p.商品コード = ma.商品コード COLLATE NOCASE
    LEFT JOIN stock_recent sr ON p.商品コード = sr.商品コード COLLATE NOCASE
    LEFT JOIN latest_stock ls ON p.商品コード = ls.商品コード COLLATE NOCASE
    LEFT JOIN seasonal_dist sd ON p.商品コード = sd.商品コード COLLATE NOCASE
    LEFT JOIN product_retirement_status prs ON p.商品コード = prs.ne_product_code COLLATE NOCASE
    WHERE p.売上分類 = ?
      AND COALESCE(p.取扱区分, '') = '取扱中'
      -- 設計書§14「粒度は NE商品コード（構成品単位）、セット扱いは構成品単位で集計（ダブルカウント回避）」
      -- に従い、セット商品は候補リストに含めない（販売・在庫は構成品側で既に集計されている）
      AND p.商品区分 IN ('単品', '例外')
  `;

  return db.prepare(sql).all(
    periodStartStr, d30Str, d90Str,
    stock6mStartStr,
    seasonalStartStr,
    parseInt(salesClass, 10),
  );
}

// ─── 分類判定（純関数） ───

/**
 * 最終販売日を daily と monthly から合成する。
 * monthly は月末日を仮定（YYYY-MM → YYYY-MM-末日）して文字列比較。
 */
function computeLastSaleDate(row) {
  const daily = row.daily_last_sale || null;
  let monthlyEnd = null;
  if (row.monthly_last_month) {
    const [y, m] = row.monthly_last_month.split('-').map(Number);
    const end = new Date(Date.UTC(y, m, 0)); // 月末
    monthlyEnd = end.toISOString().slice(0, 10);
  }
  if (daily && monthlyEnd) return daily > monthlyEnd ? daily : monthlyEnd;
  return daily || monthlyEnd || null;
}

/**
 * season_months 文字列 ('1,4,12' 等) を 1〜12 の整数配列にパースして返す。
 * 範囲外・重複・不正トークンは除去。空/null/不正なら空配列。
 *
 * Codex R2 Medium 反映: parseInt は '1x' '08abc' を 1/8 として通してしまうため、
 * 厳密な整数 regex (^(0?[1-9]|1[0-2])$) で 1〜12 の月だけを許容する。
 */
function parseSeasonMonths(seasonMonths) {
  if (!seasonMonths) return [];
  const set = new Set();
  for (const s of String(seasonMonths).split(',')) {
    const t = s.trim();
    if (!/^(0?[1-9]|1[0-2])$/.test(t)) continue; // strict 1〜12 のみ
    set.add(parseInt(t, 10));
  }
  return [...set].sort((a, b) => a - b);
}

/**
 * 季節性オフシーズン判定。
 *   - 有効な seasonality_flag=1 かつ season_months に現在月を含まない → オフシーズン true
 *   - 有効値は手動 (seasonality_flag/season_months) を最優先、なければ自動判定値を使う
 */
function isOffSeason(seasonalityFlag, seasonMonths, today) {
  if (!seasonalityFlag) return false;
  const validMonths = parseSeasonMonths(seasonMonths);
  if (validMonths.length === 0) return false;
  const currentMonth = today.getMonth() + 1;
  return !validMonths.includes(currentMonth);
}

/**
 * 過去24ヶ月の月別販売数（m01〜m12）から季節性を自動判定する。
 *
 * ロジック:
 *   1) 全12ヶ月の合計が SEASONAL_MIN_TOTAL 未満 → 実績不足で判定不能 (null)
 *   2) 上位 SEASONAL_TOP_MONTHS の合計が全体の SEASONAL_RATIO_THRESHOLD 以上 → 季節性あり
 *   3) seasonMonths は上位 N ヶ月（昇順 'M,M,M' 形式）
 *
 * @returns { seasonality_flag, season_months } | null
 */
export function detectSeasonality(monthCounts) {
  if (!Array.isArray(monthCounts) || monthCounts.length !== 12) return null;
  const counts = monthCounts.map(n => Number(n) || 0);
  const total = counts.reduce((s, n) => s + n, 0);
  if (total < SEASONAL_MIN_TOTAL) return null;
  const indexed = counts.map((q, i) => ({ month: i + 1, qty: q }));
  // 安定ソート: 数量降順 → 月昇順（同数の場合は早い月を優先）
  indexed.sort((a, b) => b.qty - a.qty || a.month - b.month);
  const topN = indexed.slice(0, SEASONAL_TOP_MONTHS);
  const topSum = topN.reduce((s, e) => s + e.qty, 0);
  if (topSum / total < SEASONAL_RATIO_THRESHOLD) return null;
  const seasonMonths = topN.map(e => e.month).sort((a, b) => a - b).join(',');
  return { seasonality_flag: 1, season_months: seasonMonths };
}

/** row.m01〜m12 を配列化（SQL から取った 12カラムを纏める） */
function rowMonthCounts(row) {
  return [
    row.m01, row.m02, row.m03, row.m04, row.m05, row.m06,
    row.m07, row.m08, row.m09, row.m10, row.m11, row.m12,
  ];
}

/** 楽天利益単価 = 売価 × 0.9 − 原価 − 送料 */
function rakutenUnitProfit(sellPrice, cost, ship) {
  return sellPrice * 0.9 - cost - ship;
}

/** 新商品判定に使う期間（日数）。設計書§14: 発売後12ヶ月以内 (strict <) は新商品扱い。 */
export const NEW_PRODUCT_WINDOW_DAYS = 365;

/**
 * 入力を「日単位の UTC ミリ秒」に正規化する（時刻成分を捨てる）。
 *   - Date オブジェクト: ローカル年月日を採用（業務日）
 *   - 文字列: 先頭の YYYY[-/]MM[-/]DD を抽出し、月末超過/13月などは無効として null
 *   - null/undefined: 現在のローカル日付を採用
 *
 * 実運用 (today 省略) では `new Date()` の "現在ローカル時刻" の年月日を取る。
 * これにより JST 2026-04-25 00:30 でも `launchUtc(2026,4,25)` と一致する
 * （UTC ミリ秒同士の単純比較で時間帯のズレが起きない）。
 */
function toUtcDayMs(input) {
  let y, mo, d;
  if (input instanceof Date) {
    if (Number.isNaN(input.getTime())) return null;
    y = input.getFullYear(); mo = input.getMonth() + 1; d = input.getDate();
  } else if (input == null) {
    const now = new Date();
    y = now.getFullYear(); mo = now.getMonth() + 1; d = now.getDate();
  } else {
    const m = String(input).match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})/);
    if (!m) return null;
    y = parseInt(m[1], 10); mo = parseInt(m[2], 10); d = parseInt(m[3], 10);
  }
  if (!y || !mo || !d) return null;
  // 月末超過・13月などを排除: Date.UTC の繰り上げで別日付になっていないか検査
  const utc = Date.UTC(y, mo - 1, d);
  if (!Number.isFinite(utc)) return null;
  const probe = new Date(utc);
  if (probe.getUTCFullYear() !== y || probe.getUTCMonth() !== mo - 1 || probe.getUTCDate() !== d) {
    return null;
  }
  return utc;
}

/**
 * 発売日（new_product_launch_date / NE 作成日）から新商品か判定する。
 * NE 商品の 作成日 は "YYYY/MM/DD HH:MM:SS" "YYYY-MM-DD" 等いずれも来うるため、
 * 先頭の YYYY[-/]MM[-/]DD のみ取り出して安全に Date 化する。
 *
 * 比較は「ローカル業務日 (時刻捨て)」同士で行う:
 *   - 実運用で today 省略時は new Date() のローカル年月日を使うため、
 *     JST 真夜中など UTC とのズレで当日発売品が「未来」扱いされない。
 *
 * @returns {boolean} 発売後 NEW_PRODUCT_WINDOW_DAYS 以内 (strict <) なら true
 */
export function isNewProductByLaunchDate(launchDateStr, today) {
  if (!launchDateStr) return false;
  const launchUtc = toUtcDayMs(launchDateStr);
  if (launchUtc == null) return false;
  const todayUtc = toUtcDayMs(today);
  if (todayUtc == null) return false;
  // 未来日付は誤入力扱い → 新商品扱いしない（撤退判定への流入を避けつつ過剰保護も避ける）
  if (launchUtc > todayUtc) return false;
  const daysSinceLaunch = Math.round((todayUtc - launchUtc) / 86400000);
  return daysSinceLaunch < NEW_PRODUCT_WINDOW_DAYS;
}

/**
 * 商品行を分類。副作用なし。
 * @returns { classification, reason, metrics, sales, flags, retirement_status }
 */
export function classifyProduct(row, thresholds, opts = {}) {
  const today = opts.today ? new Date(opts.today) : new Date();
  const periodDays = opts.periodDays || 90;

  const salesClass = String(row.売上分類 ?? '');
  const sellPrice = Number(row.標準売価) || 0;
  const cost = Number(row.原価) || 0;
  const ship = Number(row.送料) || 0;

  const salesPeriodQty = Number(row.sales_period) || 0;
  const sales30d = Number(row.sales_30d) || 0;
  const sales90d = Number(row.sales_90d) || 0;
  const latestStock = Number(row.latest_stock) || 0;
  const avgStock = Number(row.avg_stock) || 0;

  const profitPerUnit = rakutenUnitProfit(sellPrice, cost, ship);
  const marginRate = sellPrice > 0 ? (profitPerUnit / sellPrice * 100) : 0;

  const periodProfit = profitPerUnit * salesPeriodQty;
  const avgStockValue = avgStock * cost;
  const gmroi = annualizeGmroiPercent(periodProfit, avgStockValue, periodDays);

  const dailyAvg = salesPeriodQty / periodDays;
  const turnoverDays = dailyAvg > 0 ? Math.round(latestStock / dailyAvg) : null;

  const lastSaleDate = computeLastSaleDate(row);
  const daysSinceSale = lastSaleDate
    ? Math.floor((today.getTime() - new Date(lastSaleDate).getTime()) / 86400000)
    : Infinity;

  // 季節性: 「flag=1 かつ season_months が valid」 のときだけ手動値を採用。
  //   Codex R1 High (本PR) 反映: flag=1 + season_months が空/不正だと、isOffSeason が常に false を返して
  //   「強い季節性があるのに通常商品」扱いになり値下げ等へ流入するため、手動が壊れていれば自動にフォールバック。
  // なければ過去24ヶ月分布から自動判定（detectSeasonality）。
  let seasonalityFlag = 0;
  let seasonMonths = null;
  let seasonalitySource = null;
  const validManualMonths = parseSeasonMonths(row.season_months);
  if (Number(row.seasonality_flag) === 1 && validManualMonths.length > 0) {
    seasonalityFlag = 1;
    seasonMonths = validManualMonths.join(',');
    seasonalitySource = 'manual';
  } else {
    const auto = detectSeasonality(rowMonthCounts(row));
    if (auto) {
      seasonalityFlag = auto.seasonality_flag;
      seasonMonths = auto.season_months;
      seasonalitySource = 'auto';
    }
  }
  const seasonalityApplicable = isOffSeason(seasonalityFlag, seasonMonths, today);
  // 設計書§14: new_product_flag=1（手動 ON）または launch_date が直近 365日 以内なら新商品扱い。
  // 仕様メモ: 手動 flag は ON のみ意味があり、OFF (=0) は「未設定」と区別しない（既定値）。
  //   現行スキーマ INTEGER DEFAULT 0 ではトライステートを表せないため、明示的な opt-out
  //   （新商品扱いを外す）を表現したい場合は launch_date を 365日 以前の値に上書きする運用。
  //   実運用上、launch_date を「365日 以内に意図的に再設定」しつつ「新商品扱いしない」と
  //   いう要求はほぼ無いため、Phase 1 はこの単純化で問題なし。
  // 厳密に =1 で判定（DB に CHECK 制約がないため、不正な 2/-1 等で誤分類しないよう防御）
  const newProduct = Number(row.new_product_flag) === 1
    || isNewProductByLaunchDate(row.new_product_launch_date, today);

  const metrics = {
    rakuten_unit_profit: round2(profitPerUnit),
    margin_rate: round2(marginRate),
    gmroi: gmroi === null ? null : round2(gmroi),
    turnover_days: turnoverDays,
    days_since_sale: Number.isFinite(daysSinceSale) ? daysSinceSale : null,
    last_sale_date: lastSaleDate,
    period_profit: round2(periodProfit),
    avg_stock: round2(avgStock),
    avg_stock_value: round2(avgStockValue),
    latest_stock: latestStock,
    stock_snapshot_months: row.stock_snapshot_months || 0,
  };
  const sales = {
    sales_period: salesPeriodQty,
    sales_30d: sales30d,
    sales_90d: sales90d,
  };
  const flags = {
    seasonality_off_season: seasonalityApplicable,
    seasonality_source: seasonalitySource,    // 'manual' | 'auto' | null
    season_months: seasonMonths,              // 適用された season_months 文字列（手動/自動どちらでも）
    new_product: newProduct,
    stock_data_insufficient: (row.stock_snapshot_months || 0) < 6,
  };
  const retirement = row.retirement_status ? {
    status: row.retirement_status,
    next_review_date: row.retirement_next_review,
    decided_at: row.retirement_decided_at,
    reason: row.retirement_reason,
  } : null;

  // ── 分類判定 ──

  // 楽天売価未設定 → 評価不能（撤退検知対象としては別処理）
  if (sellPrice <= 0) {
    return pack('評価不能', '楽天売価未設定', metrics, sales, flags, retirement, row);
  }
  // 原価未登録 → 計算不能
  if (cost <= 0) {
    return pack('分類外', '計算不能（原価未登録）', metrics, sales, flags, retirement, row);
  }
  // 新商品保留
  if (newProduct) {
    return pack('分類外', '新商品保留', metrics, sales, flags, retirement, row);
  }
  // 季節性保留（オフシーズン）— 手動/自動の出所も理由に含めると運用判断しやすい
  if (seasonalityApplicable) {
    const src = seasonalitySource === 'auto' ? '自動' : '手動';
    return pack('分類外', `季節性保留（オフシーズン・${src}・販売期${seasonMonths}月）`,
      metrics, sales, flags, retirement, row);
  }

  // 撤退判定（sales_class別）
  const ret = thresholds?.retirement?.[salesClass];
  if (ret) {
    // 撤退候補（retire）
    if (daysSinceSale >= ret.retire_days_no_sales) {
      return pack('撤退候補', `${ret.retire_days_no_sales}日販売なし`,
        metrics, sales, flags, retirement, row);
    }
    // 仕入 GMROI+回転 複合条件
    if (salesClass === '3'
        && ret.retire_gmroi_lt !== undefined && ret.retire_turnover_gt !== undefined
        && gmroi !== null && turnoverDays !== null
        && gmroi < ret.retire_gmroi_lt && turnoverDays > ret.retire_turnover_gt) {
      return pack('撤退候補',
        `GMROI ${Math.round(gmroi)}% < ${ret.retire_gmroi_lt}% かつ 回転 ${turnoverDays}日 > ${ret.retire_turnover_gt}日`,
        metrics, sales, flags, retirement, row);
    }
    // 撤退警戒（warn）
    if (daysSinceSale >= ret.warn_days_no_sales) {
      return pack('撤退警戒', `${ret.warn_days_no_sales}日販売なし`,
        metrics, sales, flags, retirement, row);
    }
    if (salesClass === '3' && ret.warn_gmroi_lt !== undefined
        && gmroi !== null && gmroi < ret.warn_gmroi_lt) {
      return pack('撤退警戒', `GMROI ${Math.round(gmroi)}% < ${ret.warn_gmroi_lt}%`,
        metrics, sales, flags, retirement, row);
    }
  }

  // 5分類の残り（閾値は全売上分類共通）
  const cls = thresholds?.classification || {};

  // 優良在庫: GMROI > 200% AND 回転 30〜90日
  if (cls.good_stock && gmroi !== null && turnoverDays !== null
      && gmroi > cls.good_stock.gmroi_gt
      && turnoverDays >= cls.good_stock.turnover_min
      && turnoverDays <= cls.good_stock.turnover_max) {
    return pack('優良在庫',
      `GMROI ${Math.round(gmroi)}% + 回転 ${turnoverDays}日`,
      metrics, sales, flags, retirement, row);
  }
  // 観察継続: GMROI 100〜200%
  if (cls.observe && gmroi !== null
      && gmroi >= cls.observe.gmroi_min && gmroi <= cls.observe.gmroi_max) {
    return pack('観察継続', `GMROI ${Math.round(gmroi)}%`,
      metrics, sales, flags, retirement, row);
  }
  // 値下げ候補: 回転 > 120日 (上限 turnover_lte) AND 粗利率 > 20% AND 過去90日販売 ≥ sales_90d_min
  //   - turnover_lte (省略可、デフォ 540日): 死蔵レベルは値下げ対象外で別バケット (撤退/分類外) へ
  //   - sales_90d_min (省略可、デフォ 5個): ニッチ・低速回転は値下げ提案しても無効
  if (cls.price_down && turnoverDays !== null
      && turnoverDays > cls.price_down.turnover_gt
      && (cls.price_down.turnover_lte == null || turnoverDays <= cls.price_down.turnover_lte)
      && marginRate > cls.price_down.margin_rate_gt
      && (cls.price_down.sales_90d_min == null || sales90d >= cls.price_down.sales_90d_min)) {
    return pack('値下げ候補',
      `回転 ${turnoverDays}日 + 粗利率 ${Math.round(marginRate)}% + 過去90日 ${sales90d}個`,
      metrics, sales, flags, retirement, row);
  }
  // セット候補: 回転 > 120日（簡易版、同仕入先サジェストは Phase 2）
  if (cls.bundle_candidate && turnoverDays !== null
      && turnoverDays > cls.bundle_candidate.turnover_gt
      && salesPeriodQty > 0) {
    return pack('セット候補',
      `回転 ${turnoverDays}日 + 販売実績あり`,
      metrics, sales, flags, retirement, row);
  }

  // 分類外（理由分解）
  let outReason;
  if (salesPeriodQty === 0 && latestStock > 0) outReason = '販売実績不足（在庫あり）';
  else if (gmroi === null) outReason = '計算不能（在庫平均不足）';
  else outReason = '閾値外';
  return pack('分類外', outReason, metrics, sales, flags, retirement, row);
}

function pack(classification, reason, metrics, sales, flags, retirement, row) {
  return {
    ne_product_code: row.商品コード,
    product_name: row.商品名,
    sales_class: row.売上分類,
    supplier_code: row.仕入先コード,
    classification,
    reason,
    metrics,
    sales,
    flags,
    retirement_status: retirement,
  };
}

function round2(n) {
  if (n === null || n === undefined) return n;
  if (!Number.isFinite(n)) return null;
  return Math.round(n * 100) / 100;
}

// ─── 早期警戒フラグ ───

/**
 * 早期警戒フラグを付与する（全商品対象、季節性・新商品は対象外）。
 *
 * 仕様（ユーザー確定、Codex PR2a メモリ参照）:
 *   - 判定対象: 過去 past_period_days 販売数 ≥ min_past_sales
 *   - 急落判定: 直近 recent_period_days 販売数 ≤ 期待値（過去日平均 × recent × drop_ratio）
 *   - 季節性オフシーズン・新商品 は対象外
 *   - ゼロ割対応: 過去=0/直近=0 → 販売なし、過去=0/直近>0 → 判定不能
 *
 * 実装制約 (Codex PR2c Round 1 Medium #2 反映):
 *   sales_90d / sales_30d が SQL 側で固定集計のため、past=90 / recent=30 以外は動作しない。
 *   validateEarlyWarning で 90/30 を強制している。
 */
export function applyEarlyWarning(candidates, earlyWarning) {
  const { past_period_days, recent_period_days, min_past_sales, drop_ratio } = earlyWarning;
  if (past_period_days !== 90 || recent_period_days !== 30) {
    throw new Error(
      `applyEarlyWarning は現状 past=90/recent=30 固定。` +
      `与えられた past=${past_period_days}/recent=${recent_period_days} は未サポート`
    );
  }
  return candidates.map(c => {
    let flag = null;
    if (c.flags.seasonality_off_season || c.flags.new_product) {
      flag = { type: 'skip', reason: '季節性/新商品は対象外' };
    } else {
      const pastSales = c.sales.sales_90d;   // past_period_days=90 想定
      const recentSales = c.sales.sales_30d; // recent_period_days=30 想定
      if (pastSales === 0 && recentSales === 0) {
        flag = null; // 販売なし（ノイズにしない）
      } else if (pastSales === 0 && recentSales > 0) {
        flag = { type: 'indeterminate', reason: '過去販売0、直近のみ販売あり（判定不能）' };
      } else if (pastSales < min_past_sales) {
        flag = { type: 'insufficient',
          reason: `過去${past_period_days}日販売${pastSales}個 < ${min_past_sales}個（判定不足）` };
      } else {
        const expected = pastSales * (recent_period_days / past_period_days);
        const threshold = expected * drop_ratio;
        if (recentSales <= threshold) {
          flag = { type: 'drop',
            reason: `直近${recent_period_days}日 ${recentSales}個 ≤ ${threshold.toFixed(1)}個（期待${expected.toFixed(1)}×${drop_ratio}）` };
        }
      }
    }
    return { ...c, early_warning: flag };
  });
}

// ─── メイン ───

/**
 * 候補リストを取得して分類・早期警戒付与まで行う。
 * 設定（retirement/classification/early_warning）は呼び出し側が注入する。
 */
export function getCandidates(db, { salesClass, periodDays = 90, today }, settings) {
  const { retirement, classification, earlyWarning } = settings;
  const rows = fetchCandidatesRaw(db, { salesClass, periodDays, today });
  const thresholds = { retirement, classification };
  const classified = rows.map(r => classifyProduct(r, thresholds, { today, periodDays }));
  const withEW = applyEarlyWarning(classified, earlyWarning);
  return withEW;
}
