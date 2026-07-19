// ai-insights: 事実層 (月次レポート入力 JSON。PR-3)
//
// 週次 (facts.js) と同じ大原則: 事実は決定的 SQL で全計算済み、AI に算術させない。
// 月次特有:
// - MF 会計 (PL/BS/チャネル別/FY累計/異常検知 R1-R5) が主役。前年同月比・予算差を計算済みで渡す
// - closing (人間の締め宣言状態) を含める → runner が provisional/final/correction を判断
// - pl_hash: 対象月 PL のハッシュ。確定版生成時に保存し「確定後変更」検知に使う (要件 §6)
//
// 経営指標の計算式の正本 = apps/exec-dashboard/router.js /api/fy-summary (CCC/労働分配率等)

import crypto from 'node:crypto';
import { jstDateStr } from '../../lib/jst-date.js';
import { addDaysYmd, coveredDaysForEntity, INPUT_SCHEMA_VERSION } from './facts.js';

const SALES_ENTITY = 'f_sales_by_listing';
const MALLS = ['amazon', 'rakuten', 'yahoo', 'aupay', 'linegift', 'qoo10'];

// 販管費 role の表示名 (正本 = exec-dashboard ROLE_DISPLAY。前年差 top 抽出で使う分のみ)
const SGAE_LABELS = {
  sgae_officer_remuneration: '役員報酬', sgae_salary: '給料賃金', sgae_bonus: '賞与',
  sgae_legal_welfare: '法定福利費', sgae_welfare: '福利厚生費', sgae_outsourcing: '外注費',
  sgae_travel: '旅費交通費', sgae_communication: '通信費', sgae_entertainment: '交際費',
  sgae_meeting: '会議費', sgae_depreciation: '減価償却費', sgae_lt_prepay_amort: '長期前払費用償却',
  sgae_rent: '地代家賃', sgae_insurance: '保険料', sgae_repair: '修繕費', sgae_utility: '水道光熱費',
  sgae_fuel: '燃料費', sgae_supplies: '消耗品費', sgae_tax: '租税公課',
  sgae_ad_fixed: '広告宣伝費(固定)', sgae_pro_fee: '支払報酬', sgae_fee_fixed: '支払手数料(固定)',
  sgae_fee_paypal: 'PayPal手数料', sgae_fee_other: '支払手数料(他)', sgae_dues: '諸会費',
  sgae_system_fixed: 'システム利用料(固定)', sgae_publication: '新聞図書費',
  sgae_training: '教育訓練費', sgae_misc: '雑費',
};

function roundYen(v) { return v == null ? null : Math.round(v); }
function pct(num, den, digits = 1) {
  return den > 0 && num != null ? +((num / den) * 100).toFixed(digits) : null;
}
function round1(v) { return (v == null || !isFinite(v)) ? null : +Number(v).toFixed(1); }

export function isYmStrict(s) {
  return typeof s === 'string' && /^\d{4}-(0[1-9]|1[0-2])$/.test(s);
}
export function addMonths(ym, delta) {
  const [y, m] = ym.split('-').map(Number);
  const t = y * 12 + (m - 1) + delta;
  return `${Math.floor(t / 12)}-${String((t % 12) + 1).padStart(2, '0')}`;
}
export function monthBounds(ym) {
  return { periodStart: `${ym}-01`, periodEnd: `${addMonths(ym, 1)}-01` };
}
/** JST 今日基準の「前月」 */
export function previousMonthYm(now = new Date()) {
  return addMonths(jstDateStr(now).slice(0, 7), -1);
}
function monthDays(ym) {
  const { periodStart, periodEnd } = monthBounds(ym);
  const days = [];
  for (let d = periodStart; d < periodEnd; d = addDaysYmd(d, 1)) days.push(d);
  return days;
}

// ── PL 集計 (SQL は exec-dashboard /api/timeseries と同一定義) ──────────

function plForMonths(db, months) {
  const placeholders = months.map(() => '?').join(',');
  const rows = db.prepare(`
    SELECT month_ym,
      SUM(CASE WHEN role_key IN ('sales','sales_export','sales_wholesale') THEN amount_excl_tax ELSE 0 END)
      - SUM(CASE WHEN role_key='sales_return' THEN amount_excl_tax ELSE 0 END) AS sales,
      SUM(CASE WHEN role_key LIKE 'cogs_%' OR role_key LIKE 'inventory_%' THEN amount_excl_tax ELSE 0 END) AS cogs,
      SUM(CASE WHEN role_key LIKE 'sgae_%' THEN amount_excl_tax ELSE 0 END) AS sgae,
      SUM(CASE WHEN role_key LIKE 'non_op_revenue_%' THEN amount_excl_tax ELSE 0 END) AS non_op_rev,
      SUM(CASE WHEN role_key LIKE 'non_op_expense_%' THEN amount_excl_tax ELSE 0 END) AS non_op_exp,
      SUM(line_count) AS line_count,
      MIN(is_realized_only) AS is_realized_only
    FROM v_mirror_mf_pl_monthly_latest
    WHERE month_ym IN (${placeholders})
    GROUP BY month_ym
  `).all(...months);
  const byYm = {};
  for (const r of rows) {
    const gp = (r.sales || 0) - (r.cogs || 0);
    const op = gp - (r.sgae || 0);
    byYm[r.month_ym] = {
      sales_yen: roundYen(r.sales || 0),
      cogs_yen: roundYen(r.cogs || 0),
      gross_profit_yen: roundYen(gp),
      gross_profit_pct: pct(gp, r.sales || 0),
      sgae_yen: roundYen(r.sgae || 0),
      operating_income_yen: roundYen(op),
      ordinary_income_yen: roundYen(op + (r.non_op_rev || 0) - (r.non_op_exp || 0)),
      line_count: r.line_count || 0,
    };
  }
  return byYm;
}

/** 対象月 PL のハッシュ (role_key×金額。確定後変更検知用。正規化: role順ソート) */
export function computeMonthPlHash(db, ym) {
  const rows = db.prepare(`
    SELECT role_key, amount_excl_tax, tax_amount, line_count
    FROM v_mirror_mf_pl_monthly_latest WHERE month_ym = ? ORDER BY role_key
  `).all(ym);
  if (rows.length === 0) return null;
  return crypto.createHash('sha256')
    .update(JSON.stringify(rows.map((r) => [r.role_key, r.amount_excl_tax, r.tax_amount, r.line_count])))
    .digest('hex');
}

function sgaeYoyTop(db, ym, lyYm, limit = 5) {
  const rows = db.prepare(`
    SELECT role_key,
      SUM(CASE WHEN month_ym = :ym THEN amount_excl_tax ELSE 0 END) AS cur,
      SUM(CASE WHEN month_ym = :ly THEN amount_excl_tax ELSE 0 END) AS ly
    FROM v_mirror_mf_pl_monthly_latest
    WHERE role_key LIKE 'sgae_%' AND month_ym IN (:ym, :ly)
    GROUP BY role_key
    HAVING ABS(cur - ly) > 0
    ORDER BY (cur - ly) DESC
    LIMIT :limit
  `).all({ ym, ly: lyYm, limit });
  return rows.map((r) => ({
    item: SGAE_LABELS[r.role_key] || r.role_key,
    this_year_yen: roundYen(r.cur),
    last_year_yen: roundYen(r.ly),
    diff_yen: roundYen(r.cur - r.ly),
  }));
}

// ── 充足判定 (月次: 週次と同じマーカー方式を月の日数に適用) ──────────────

function assessMonthlyCoverage(db, ym, days) {
  const results = [];
  const salesCovered = coveredDaysForEntity(db, SALES_ENTITY, days);
  const salesMissing = days.length - salesCovered.size;
  results.push({
    source_id: 'sales:all', source_kind: 'sales', unit: 'all', requirement: 'required',
    status: salesMissing === days.length ? 'missing' : salesMissing > 0 ? 'partial' : 'ok',
    detail: salesMissing > 0 ? `未取込 ${salesMissing}/${days.length}日` : null,
  });
  for (const mall of MALLS) {
    const covered = coveredDaysForEntity(db, `${mall}_finance_sku_daily`, days);
    const missing = days.length - covered.size;
    results.push({
      source_id: `finance:${mall}`, source_kind: 'finance', unit: mall, requirement: 'conditional',
      status: missing === days.length ? 'missing' : missing > days.length * 0.2 ? 'partial' : 'ok',
      detail: missing > 0 ? `未取込 ${missing}/${days.length}日` : null,
    });
  }
  const plRow = db.prepare(
    'SELECT COUNT(*) AS c FROM v_mirror_mf_pl_monthly_latest WHERE month_ym = ?'
  ).get(ym);
  results.push({
    source_id: 'mf:pl_monthly', source_kind: 'mf', unit: 'mf', requirement: 'conditional',
    status: (plRow?.c || 0) > 0 ? 'ok' : 'missing',
    detail: (plRow?.c || 0) > 0 ? null : `MF月次PLに ${ym} の行なし`,
  });
  return results;
}

function deriveMonthlyConstraints(coverage, closing) {
  const prohibited = [];
  let degraded = false;
  const sales = coverage.find((c) => c.source_id === 'sales:all');
  const blocked = sales?.status === 'missing';
  if (sales?.status === 'partial') {
    prohibited.push({ area: 'sales_totals_precision', reason: sales.detail });
    degraded = true;
  }
  for (const c of coverage.filter((x) => x.source_kind === 'finance' && x.status !== 'ok')) {
    prohibited.push({ area: `margin:${c.unit}`, reason: c.detail || c.status });
    degraded = true;
  }
  const mf = coverage.find((c) => c.source_id === 'mf:pl_monthly');
  if (mf?.status !== 'ok') {
    prohibited.push({ area: 'mf_pl', reason: 'MF月次PL未取込。会計数値の論点は生成禁止' });
    degraded = true;
  }
  return {
    generation: blocked ? 'blocked' : 'allowed',
    data_quality_status: blocked ? 'blocked' : degraded ? 'degraded' : 'normal',
    mf_reliability: closing.status === 'declared' ? 'confirmed_by_human' : 'provisional',
    prohibited_topic_areas: prohibited,
  };
}

// ── 各セクション ──────────────────────────────────────────────────────

function buildBsFacts(db, ym, prevYm) {
  const rows = db.prepare(`
    SELECT * FROM v_mirror_mf_bs_monthly_latest WHERE month_ym IN (?, ?)
  `).all(ym, prevYm);
  const pick = (m) => rows.find((r) => r.month_ym === m);
  const fmt = (r) => r ? {
    cash_yen: r.cash_total, ar_yen: r.ar_total, inventory_yen: r.inventory_total,
    total_asset_yen: r.total_asset, ap_yen: r.ap_total,
    loans_yen: (r.short_loan_total || 0) + (r.long_loan_total || 0),
    equity_yen: r.display_total_equity ?? r.total_equity,
    equity_ratio_pct: pct(r.display_total_equity ?? r.total_equity, r.total_asset),
  } : null;
  return { this_month: fmt(pick(ym)), prev_month: fmt(pick(prevYm)) };
}

function buildChannelFacts(db, ym, lyYm) {
  const rows = db.prepare(`
    SELECT month_ym, channel_key, channel_display_name, gross_sales_excl_tax,
           pf_fee_excl_tax, ad_cost_excl_tax, net_sales_after_pf_excl_tax, mapping_coverage_pct
    FROM v_mirror_mf_channel_sales_latest WHERE month_ym IN (?, ?)
    ORDER BY gross_sales_excl_tax DESC
  `).all(ym, lyYm);
  const lyByKey = Object.fromEntries(
    rows.filter((r) => r.month_ym === lyYm).map((r) => [r.channel_key, r]),
  );
  return rows.filter((r) => r.month_ym === ym).map((r) => ({
    channel: r.channel_display_name,
    sales_yen: r.gross_sales_excl_tax,
    pf_fee_yen: r.pf_fee_excl_tax,
    ad_cost_yen: r.ad_cost_excl_tax,
    net_after_pf_yen: r.net_sales_after_pf_excl_tax,
    yoy_pct: pct(
      r.gross_sales_excl_tax - (lyByKey[r.channel_key]?.gross_sales_excl_tax || 0),
      lyByKey[r.channel_key]?.gross_sales_excl_tax || 0,
    ),
    mapping_coverage_pct: r.mapping_coverage_pct,
  }));
}

function buildFyFacts(db) {
  const rows = db.prepare(
    'SELECT * FROM v_mirror_mf_fy_summary_latest ORDER BY fy_number DESC LIMIT 2'
  ).all();
  return rows.map((r) => {
    // 計算式の正本 = exec-dashboard /api/fy-summary (periodDays = 月数×30.4)
    const periodDays = r.months_in_cumulative > 0 ? r.months_in_cumulative * 30.4 : 365;
    const dso = r.sales_cum > 0 ? r.ar_average / (r.sales_cum / periodDays) : null;
    const dio = r.cogs_cum > 0 ? r.inventory_average / (r.cogs_cum / periodDays) : null;
    const dpo = r.cogs_cum > 0 ? r.ap_average / (r.cogs_cum / periodDays) : null;
    const monthlyFixedCost = r.months_in_cumulative > 0 ? r.sgae_cum / r.months_in_cumulative : 0;
    return {
      fy_number: r.fy_number,
      through_ym: r.cumulative_through_ym,
      months: r.months_in_cumulative,
      sales_cum_yen: r.sales_cum,
      gross_profit_cum_yen: r.gross_profit_cum,
      gross_profit_pct: pct(r.gross_profit_cum, r.sales_cum),
      operating_income_cum_yen: r.operating_income_cum,
      ordinary_income_cum_yen: r.ordinary_income_cum,
      labor_share_pct: pct(r.personnel_cost_cum, r.gross_profit_cum),
      ccc_days: (dso != null && dio != null && dpo != null) ? round1(dso + dio - dpo) : null,
      dso_days: round1(dso), inventory_days: round1(dio), dpo_days: round1(dpo),
      loans_yen: (r.short_loan_closing || 0) + (r.long_loan_closing || 0),
      cash_closing_yen: r.cash_closing,
      cash_runway_months: round1(monthlyFixedCost > 0 ? r.cash_closing / monthlyFixedCost : null),
    };
  });
}

function buildAnomalyFacts(db) {
  try {
    return db.prepare(`
      SELECT signal_code, severity, title, description, recommended_action, detected_at
      FROM v_mirror_mf_anomaly_signals_latest
      WHERE (state_status IS NULL OR state_status = 'open')
        AND severity IN ('warn', 'high', 'critical')
      ORDER BY severity_rank DESC, detected_at DESC
      LIMIT 10
    `).all();
  } catch {
    return [];
  }
}

function buildMallSalesMonth(db, ym, prevYm, lyYm) {
  const bounds = [ym, prevYm, lyYm].map((m) => monthBounds(m));
  const rows = db.prepare(`
    SELECT SUBSTR(date_jst, 1, 7) AS ym, mall,
           SUM(sales_gross_jpy_incl) AS sales, SUM(units_net_sold) AS units
    FROM v_mall_sales_daily_unified
    WHERE (date_jst >= ? AND date_jst < ?) OR (date_jst >= ? AND date_jst < ?) OR (date_jst >= ? AND date_jst < ?)
    GROUP BY ym, mall
  `).all(bounds[0].periodStart, bounds[0].periodEnd, bounds[1].periodStart, bounds[1].periodEnd,
    bounds[2].periodStart, bounds[2].periodEnd);
  const get = (m, mall) => rows.find((r) => r.ym === m && r.mall === mall);
  const malls = [...new Set(rows.filter((r) => r.ym === ym).map((r) => r.mall))];
  return malls.map((mall) => {
    const cur = get(ym, mall), prev = get(prevYm, mall), ly = get(lyYm, mall);
    return {
      mall,
      sales_yen: roundYen(cur?.sales || 0), units: cur?.units || 0,
      prev_month_sales_yen: roundYen(prev?.sales || 0),
      mom_pct: pct((cur?.sales || 0) - (prev?.sales || 0), prev?.sales || 0),
      last_year_sales_yen: ly ? roundYen(ly.sales) : null,
      yoy_pct: ly ? pct((cur?.sales || 0) - ly.sales, ly.sales) : null,
    };
  }).sort((a, b) => (b.sales_yen || 0) - (a.sales_yen || 0));
}

function buildStockFacts(db, ym, prevYm) {
  const qty = db.prepare(`
    SELECT 年月 AS ym, SUM(月末在庫数) AS qty FROM mirror_stock_monthly_snapshot
    WHERE 年月 IN (?, ?) GROUP BY 年月
  `).all(ym, prevYm);
  const get = (m) => qty.find((r) => r.ym === m)?.qty ?? null;
  // 在庫金額: 月末日 (前後3日の最新) の日次サマリ合計で近似
  const { periodEnd } = monthBounds(ym);
  const lastDay = addDaysYmd(periodEnd, -1);
  const value = db.prepare(`
    SELECT SUM(total_value) AS v FROM mirror_inv_daily_summary
    WHERE market = 'jp' AND business_date = (
      SELECT MAX(business_date) FROM mirror_inv_daily_summary
      WHERE market = 'jp' AND business_date <= ? AND business_date >= ?
    )
  `).get(lastDay, addDaysYmd(lastDay, -3));
  return {
    eom_qty: get(ym), prev_eom_qty: get(prevYm),
    eom_value_yen: roundYen(value?.v),
    note: '在庫金額は月末日近傍の日次スナップショット合計 (FBA+自社倉庫) の近似値',
  };
}

function buildBudgetComparison(db, ym, pl, bs, stock) {
  const rows = db.prepare(`
    SELECT metric, value_yen, budget_row_id FROM v_ai_budget_current
    WHERE budget_kind = 'initial' AND year_month = ?
  `).all(ym);
  if (rows.length === 0) {
    return { available: false, note: '予算未入力。予算差に言及する論点は生成禁止 (前年比で論じる)' };
  }
  const actuals = {
    sales: pl?.sales_yen ?? null,
    gross_profit: pl?.gross_profit_yen ?? null,
    operating_income: pl?.operating_income_yen ?? null,
    cash_eom: bs?.this_month?.cash_yen ?? null,
    inventory_value: stock?.eom_value_yen ?? null,
  };
  return {
    available: true,
    budget_revision_id: rows.map((r) => r.budget_row_id).sort().join(','),
    entries: rows.map((r) => ({
      metric: r.metric,
      budget_yen: r.value_yen,
      actual_yen: actuals[r.metric],
      diff_yen: actuals[r.metric] != null ? actuals[r.metric] - r.value_yen : null,
      achievement_pct: actuals[r.metric] != null ? pct(actuals[r.metric], r.value_yen) : null,
    })),
  };
}

// ── エントリポイント ──────────────────────────────────────────────────

/**
 * 月次レポート入力 JSON。
 * @param {object} opts { month: 'YYYY-MM' (省略時=JST前月) }
 */
export function buildMonthlyReportInput(db, opts = {}) {
  const ym = opts.month || previousMonthYm();
  if (!isYmStrict(ym)) throw new Error(`invalid month: ${ym}`);
  const { periodStart, periodEnd } = monthBounds(ym);
  const prevYm = addMonths(ym, -1);
  const lyYm = addMonths(ym, -12);
  const days = monthDays(ym);

  const closingRow = db.prepare('SELECT * FROM ai_monthly_closing WHERE year_month = ?').get(ym)
    || { year_month: ym, status: 'open', declare_seq: 0 };
  const latestMfRun = db.prepare(`
    SELECT run_id, finished_at, synced_at FROM mirror_mf_publish_runs
    WHERE status = 'success' ORDER BY run_id DESC LIMIT 1
  `).get();
  const finalJob = db.prepare(`
    SELECT j.status FROM ai_report_jobs j
    WHERE j.report_type = 'monthly' AND j.period_start = ? AND j.edition = 'final'
  `).get(periodStart);
  const provisionalJob = db.prepare(`
    SELECT j.status FROM ai_report_jobs j
    WHERE j.report_type = 'monthly' AND j.period_start = ? AND j.edition = 'provisional'
  `).get(periodStart);

  const closing = {
    year_month: ym,
    status: closingRow.status,
    declared_at: closingRow.declared_at || null,
    // 確定版は「宣言後に成功した MF 同期」のスナップショットで作る (要件 §6)
    mf_synced_after_declare: Boolean(
      closingRow.status === 'declared' && closingRow.declared_at && latestMfRun?.synced_at
      && latestMfRun.synced_at > closingRow.declared_at,
    ),
    provisional_state: provisionalJob?.status || null,
    final_state: finalJob?.status || null,
    // 再オープン→再宣言後の確定は correction 版として発行する
    needs_correction: Boolean(
      closingRow.final_report_id
      && closingRow.status === 'declared'
      && (closingRow.final_declare_seq ?? 0) < (closingRow.declare_seq ?? 0),
    ),
  };

  const coverage = assessMonthlyCoverage(db, ym, days);
  const constraints = deriveMonthlyConstraints(coverage, closing);

  const input = {
    meta: {
      report_type: 'monthly',
      month_ym: ym,
      period_start: periodStart,
      period_end: periodEnd,
      period_label: `${ym}月次`,
      generated_at: new Date().toISOString(),
      data_as_of: jstDateStr(new Date()),
      input_schema_version: INPUT_SCHEMA_VERSION,
    },
    closing,
    coverage,
    constraints,
    facts: null,
    events: null,
    budget: null,
    previous_open_topics: null,
    recent_reports: null,
    pl_hash: null,
  };
  if (constraints.generation === 'blocked') return input;

  const plByYm = plForMonths(db, [ym, prevYm, lyYm]);
  const pl = plByYm[ym] || null;
  const bs = buildBsFacts(db, ym, prevYm);
  const stock = buildStockFacts(db, ym, prevYm);

  input.facts = {
    pl: {
      this_month: pl,
      prev_month: plByYm[prevYm] || null,
      last_year_same_month: plByYm[lyYm] || null,
      yoy_sales_pct: pl && plByYm[lyYm] ? pct(pl.sales_yen - plByYm[lyYm].sales_yen, plByYm[lyYm].sales_yen) : null,
      sgae_yoy_top_increases: sgaeYoyTop(db, ym, lyYm),
      reliability: constraints.mf_reliability === 'confirmed_by_human'
        ? '締め宣言済み (人間による締め状態の宣言。会計データの完全性保証ではない)'
        : '暫定 (MF仕訳入力ラグあり。締め宣言前)',
    },
    bs,
    channel_sales: buildChannelFacts(db, ym, lyYm),
    fy: buildFyFacts(db),
    anomaly_signals: buildAnomalyFacts(db),
    mall_sales: buildMallSalesMonth(db, ym, prevYm, lyYm),
    stock,
    cash_flow_month: (() => {
      const flows = db.prepare(`
        SELECT CASE WHEN movement_date >= ? THEN 'this_month' ELSE 'prev_month' END AS bucket,
               direction, SUM(amount_excl_tax) AS amount
        FROM v_mirror_mf_cash_events_daily_latest
        WHERE movement_date >= ? AND movement_date < ?
        GROUP BY bucket, direction
      `).all(periodStart, monthBounds(prevYm).periodStart, periodEnd);
      const out = { this_month: { in: 0, out: 0 }, prev_month: { in: 0, out: 0 } };
      for (const f of flows) {
        if (out[f.bucket] && (f.direction === 'in' || f.direction === 'out')) {
          out[f.bucket][f.direction] = roundYen(f.amount);
        }
      }
      out.note = '銀行口座入出金の gross 集計。口座間振替を含むため純増減の解釈は不可';
      return out;
    })(),
  };
  input.budget = buildBudgetComparison(db, ym, pl, bs, stock);
  input.events = {
    entries: db.prepare(`
      SELECT event_date, event_end_date, description, tag, scope FROM ai_events
      WHERE deleted_at IS NULL AND event_date < ? AND COALESCE(event_end_date, event_date) >= ?
      ORDER BY event_date
    `).all(periodEnd, periodStart),
    note: 'イベントは「実施された」事実のみ。因果は仮説として扱うこと',
  };
  input.previous_open_topics = db.prepare(`
    SELECT t.topic_id, t.title, t.category, t.status, r.public_id, r.period_start
    FROM ai_report_topics t JOIN ai_reports r ON r.report_id = t.report_id
    WHERE r.report_type = 'monthly' AND t.status IN ('open', 'carried', 'worsened')
    ORDER BY r.period_start DESC, t.seq LIMIT 10
  `).all();
  input.recent_reports = db.prepare(`
    SELECT public_id, period_start, edition, data_quality_status FROM ai_reports
    WHERE report_type = 'monthly' ORDER BY period_start DESC, created_at DESC LIMIT 3
  `).all();
  input.pl_hash = computeMonthPlHash(db, ym);
  return input;
}
