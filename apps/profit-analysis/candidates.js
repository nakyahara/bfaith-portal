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
  //     月初固定 new Date(Date.UTC(y, m-5, 1)) で安全に切る
  const y = todayDate.getUTCFullYear();
  const m = todayDate.getUTCMonth(); // 0-indexed
  const stock6mStartStr = new Date(Date.UTC(y, m - 5, 1)).toISOString().slice(0, 7);

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
      prs.status as retirement_status,
      prs.next_review_date as retirement_next_review,
      prs.decided_at as retirement_decided_at,
      prs.reason as retirement_reason
    FROM mirror_products p
    LEFT JOIN daily_agg da ON p.商品コード = da.商品コード COLLATE NOCASE
    LEFT JOIN monthly_agg ma ON p.商品コード = ma.商品コード COLLATE NOCASE
    LEFT JOIN stock_recent sr ON p.商品コード = sr.商品コード COLLATE NOCASE
    LEFT JOIN latest_stock ls ON p.商品コード = ls.商品コード COLLATE NOCASE
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
 * 季節性オフシーズン判定。
 *   - seasonality_flag=1 かつ season_months に現在月を含まない → オフシーズン true
 */
function isOffSeason(row, today) {
  if (!row.seasonality_flag || !row.season_months) return false;
  const currentMonth = today.getMonth() + 1;
  const validMonths = String(row.season_months).split(',').map(s => parseInt(s.trim(), 10)).filter(Number.isFinite);
  return validMonths.length > 0 && !validMonths.includes(currentMonth);
}

/** 楽天利益単価 = 売価 × 0.9 − 原価 − 送料 */
function rakutenUnitProfit(sellPrice, cost, ship) {
  return sellPrice * 0.9 - cost - ship;
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
  const gmroi = avgStockValue > 0 ? (periodProfit / avgStockValue * 100) : null;

  const dailyAvg = salesPeriodQty / periodDays;
  const turnoverDays = dailyAvg > 0 ? Math.round(latestStock / dailyAvg) : null;

  const lastSaleDate = computeLastSaleDate(row);
  const daysSinceSale = lastSaleDate
    ? Math.floor((today.getTime() - new Date(lastSaleDate).getTime()) / 86400000)
    : Infinity;

  const seasonalityApplicable = isOffSeason(row, today);
  const newProduct = Boolean(row.new_product_flag);

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
  // 季節性保留（オフシーズン）
  if (seasonalityApplicable) {
    return pack('分類外', '季節性保留（オフシーズン）', metrics, sales, flags, retirement, row);
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
  // 値下げ候補: 回転 > 120日 AND 粗利率 > 20%
  if (cls.price_down && turnoverDays !== null
      && turnoverDays > cls.price_down.turnover_gt
      && marginRate > cls.price_down.margin_rate_gt) {
    return pack('値下げ候補',
      `回転 ${turnoverDays}日 + 粗利率 ${Math.round(marginRate)}%`,
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
