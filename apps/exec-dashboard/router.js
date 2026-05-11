/**
 * MFクラウド経営トップダッシュボード
 *   Phase 1a: 基本 KPI + 5 タブ UI
 *   Phase 1b: 数値を MF 試算表と完全一致させた (¥0 差)
 *   Phase 1c: 試算表形式 + Chart.js グラフ + 期間切替 + 補助情報遅延ロード (この修正)
 *
 * 使うデータソース (mirror_mf_*、Render warehouse-mirror.db):
 *   v_mirror_mf_executive_top_latest      経営トップ KPI
 *   v_mirror_mf_pl_monthly_latest         月次 PL (role_key 別)
 *   v_mirror_mf_channel_sales_latest      モール別売上純額
 *   v_mirror_mf_cash_events_daily_latest  日次現金イベント
 *   v_mirror_mf_balance_snapshot_monthly_latest  月末残高
 *   v_mirror_mf_anomaly_signals_latest    異常検知
 *
 * 全 VIEW は status='success' の最新 run のみ公開
 */
import { Router } from 'express';
import { getMirrorDB } from '../warehouse-mirror/db.js';

const router = Router();

// ─── 期間 helper ───
// period パラメータ: '6m' | '12m' (default) | '24m' | 'all'
function periodToMonthsBack(period) {
  const map = { '6m': 6, '12m': 12, '24m': 24, 'all': 60 };
  return map[period] ?? 12;
}
// 最終確定月 (executive_top.current_month_ym) を base に monthsBack ヶ月遡る
//   未確定月 (当月/前月) を除外して、確定済データだけ表示
function getMonthList(monthsBack, db) {
  let baseY, baseM;  // base 月 (1-12)
  if (db) {
    const exec = db.prepare(`SELECT current_month_ym FROM v_mirror_mf_executive_top_latest`).get();
    if (exec && exec.current_month_ym) {
      [baseY, baseM] = exec.current_month_ym.split('-').map(Number);
    }
  }
  if (!baseY) {
    // fallback: JST 当月-2 (安全な確定月想定)
    const today = new Date();
    const jst = new Date(today.getTime() + 9 * 60 * 60 * 1000);
    baseY = jst.getUTCFullYear();
    baseM = jst.getUTCMonth() - 1;  // -2 (JS の getUTCMonth は 0-11 なので -1 で 当月-2)
  }
  const out = [];
  for (let i = monthsBack - 1; i >= 0; i--) {
    const d = new Date(Date.UTC(baseY, baseM - 1 - i, 1));
    out.push(d.toISOString().slice(0, 7));
  }
  return out;
}

// ─── role_key を MF 試算表の科目名に逆引きするマップ ───
// (build-marts.js の map_mf_account_role と対応、display 名は MF CSV 準拠)
const ROLE_DISPLAY = {
  // 売上系
  sales: '売上高',
  sales_export: '売上高_輸出',
  sales_wholesale: '売上高_卸',
  sales_return: '売上値引・返品',
  // 売上原価
  inventory_opening: '期首商品棚卸高',
  inventory_closing: '期末商品棚卸高',
  cogs_purchase: '仕入高',
  cogs_purchase_reduced: '仕入高_軽減８%',
  cogs_purchase_return: '仕入値引・返品',
  cogs_mall_fee: '支払手数料【原価】',
  cogs_shipping: '発送運賃【原価】',
  cogs_ad: '広告宣伝費【原価】',
  cogs_packing: '荷造消耗品費【原価】',
  cogs_system: 'システム利用料【原価】',
  // 販管費
  sgae_officer_remuneration: '役員報酬',
  sgae_salary: '給料賃金',
  sgae_bonus: '賞与',
  sgae_legal_welfare: '法定福利費',
  sgae_welfare: '福利厚生費',
  sgae_outsourcing: '外注費',
  sgae_travel: '旅費交通費',
  sgae_communication: '通信費',
  sgae_entertainment: '交際費',
  sgae_meeting: '会議費',
  sgae_depreciation: '減価償却費',
  sgae_lt_prepay_amort: '長期前払費用償却',
  sgae_rent: '地代家賃',
  sgae_insurance: '保険料',
  sgae_repair: '修繕費',
  sgae_utility: '水道光熱費',
  sgae_fuel: '燃料費',
  sgae_supplies: '消耗品費',
  sgae_tax: '租税公課',
  sgae_ad_fixed: '広告宣伝費【固定費】',
  sgae_pro_fee: '支払報酬',
  sgae_fee_fixed: '支払手数料【固定費】',
  sgae_fee_paypal: '支払手数料_PayPal手数料',
  sgae_fee_other: '支払手数料_その他',
  sgae_dues: '諸会費',
  sgae_system_fixed: 'システム利用料【固定費】',
  sgae_publication: '新聞図書費',
  sgae_training: '教育訓練費',
  sgae_misc: '雑費',
  // 営業外
  non_op_revenue_interest: '受取利息',
  non_op_revenue_misc: '雑収入',
  non_op_revenue_fx: '雑収入_為替差益',
  non_op_revenue_rent: '雑収入_社宅賃料',
  non_op_revenue_points: '雑収入_ポイント',
  non_op_revenue_dividend: '受取配当金',
  non_op_expense_misc: '雑損失',
  non_op_expense_interest: '支払利息',
  non_op_expense_bad_debt: '貸倒引当金繰入額',
};

// 試算表セクション定義 (CSV 順)
const PL_SECTIONS = [
  {
    key: 'sales', name: '売上高', sign: 1,
    items: ['sales', 'sales_export', 'sales_wholesale', 'sales_return'],
  },
  {
    key: 'cogs', name: '売上原価', sign: 1,
    items: [
      'inventory_opening', 'cogs_purchase', 'cogs_purchase_reduced', 'cogs_purchase_return',
      'cogs_mall_fee', 'cogs_shipping', 'cogs_ad', 'cogs_packing', 'cogs_system',
      'inventory_closing',
    ],
  },
  { key: 'gross_profit', name: '売上総利益', sign: 1, computed: true },
  {
    key: 'sgae', name: '販売費及び一般管理費', sign: 1,
    items: [
      'sgae_officer_remuneration', 'sgae_salary', 'sgae_bonus',
      'sgae_legal_welfare', 'sgae_welfare', 'sgae_outsourcing',
      'sgae_travel', 'sgae_communication', 'sgae_entertainment', 'sgae_meeting',
      'sgae_depreciation', 'sgae_lt_prepay_amort', 'sgae_rent', 'sgae_insurance',
      'sgae_repair', 'sgae_utility', 'sgae_fuel', 'sgae_supplies', 'sgae_tax',
      'sgae_ad_fixed', 'sgae_pro_fee', 'sgae_fee_fixed', 'sgae_fee_paypal', 'sgae_fee_other',
      'sgae_dues', 'sgae_system_fixed', 'sgae_publication', 'sgae_training', 'sgae_misc',
    ],
  },
  { key: 'operating_income', name: '営業利益', sign: 1, computed: true },
  {
    key: 'non_op_revenue', name: '営業外収益', sign: 1,
    items: [
      'non_op_revenue_interest', 'non_op_revenue_misc',
      'non_op_revenue_fx', 'non_op_revenue_rent', 'non_op_revenue_points', 'non_op_revenue_dividend',
    ],
  },
  {
    key: 'non_op_expense', name: '営業外費用', sign: 1,
    items: ['non_op_expense_misc', 'non_op_expense_interest', 'non_op_expense_bad_debt'],
  },
  { key: 'ordinary_income', name: '経常利益', sign: 1, computed: true },
];

// ─── メイン画面 ───
router.get('/', (req, res) => {
  res.render('exec-dashboard', {
    title: 'MF経営トップダッシュボード',
    username: req.session?.email,
    displayName: req.session?.displayName,
  });
});

// ─── API: スナップショット (executive_top + 軽い詳細) ───
//   軽量化のため、グラフ用 timeseries / 試算表は別 endpoint に分離 (Phase 1c 遅延ロード)
router.get('/api/snapshot', (req, res) => {
  try {
    const db = getMirrorDB();

    const exec = db.prepare(`SELECT * FROM v_mirror_mf_executive_top_latest`).get();
    if (!exec) {
      return res.json({
        ok: true, available: false,
        message: 'まだ MF データが Render に sync されていません。miniPC で daily-sync を待ってください。',
      });
    }

    const runMeta = db.prepare(`
      SELECT run_id, scope, status, started_at, finished_at, finalized_at, synced_at
      FROM mirror_mf_publish_runs
      WHERE run_id = ?
    `).get(exec.run_id);

    // 異常検知 (open のみ)
    const anomalies = db.prepare(`
      SELECT signal_id, signal_code, severity, severity_rank, title, description,
             observed_value, threshold_value, recommended_action,
             state_status, suppress_until, acked_by, acked_at
      FROM v_mirror_mf_anomaly_signals_latest
      WHERE state_status IS NULL OR state_status NOT IN ('closed', 'snoozed')
      ORDER BY severity_rank DESC, detected_at DESC
      LIMIT 10
    `).all();

    res.json({ ok: true, available: true, exec, runMeta, anomalies });
  } catch (e) {
    console.error('[exec-dashboard] snapshot error:', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ─── API: グラフ用 timeseries (Phase 1c) ───
router.get('/api/timeseries', (req, res) => {
  try {
    const db = getMirrorDB();
    const period = req.query.period || '12m';
    const months = getMonthList(periodToMonthsBack(period), db);

    // 1. 売上 vs 粗利 月次
    const placeholders = months.map(() => '?').join(',');
    const plSummary = db.prepare(`
      SELECT month_ym,
        SUM(CASE WHEN role_key IN ('sales','sales_export','sales_wholesale') THEN amount_excl_tax ELSE 0 END)
        - SUM(CASE WHEN role_key='sales_return' THEN amount_excl_tax ELSE 0 END) AS sales,
        SUM(CASE WHEN role_key LIKE 'cogs_%' OR role_key LIKE 'inventory_%' THEN amount_excl_tax ELSE 0 END) AS cogs,
        SUM(CASE WHEN role_key LIKE 'sgae_%' THEN amount_excl_tax ELSE 0 END) AS sgae,
        SUM(CASE WHEN role_key LIKE 'non_op_revenue_%' THEN amount_excl_tax ELSE 0 END) AS non_op_rev,
        SUM(CASE WHEN role_key LIKE 'non_op_expense_%' THEN amount_excl_tax ELSE 0 END) AS non_op_exp
      FROM v_mirror_mf_pl_monthly_latest
      WHERE month_ym IN (${placeholders})
      GROUP BY month_ym ORDER BY month_ym
    `).all(...months);

    const plByMonth = Object.fromEntries(plSummary.map(r => [r.month_ym, r]));
    const salesVsGross = months.map(m => {
      const r = plByMonth[m] || {};
      const sales = r.sales || 0;
      const gp = sales - (r.cogs || 0);
      return {
        month: m,
        sales,
        gross_profit: gp,
        operating_income: gp - (r.sgae || 0),
        ordinary_income: gp - (r.sgae || 0) + (r.non_op_rev || 0) - (r.non_op_exp || 0),
        gross_profit_pct: sales > 0 ? +((gp / sales) * 100).toFixed(2) : null,
      };
    });

    // 2. 銀行別月末残高 (現金及び預金、月末)
    const cashAccounts = ['普通預金', '当座預金', '現金', '小口現金', '普通預金_PayPal', '普通預金_ペイオニア', '定期預金'];
    const balRows = db.prepare(`
      SELECT month_ym, account_name, sub_account_name, closing_balance_excl_tax
      FROM v_mirror_mf_balance_snapshot_monthly_latest
      WHERE month_ym IN (${placeholders}) AND account_name IN (${cashAccounts.map(()=>'?').join(',')})
    `).all(...months, ...cashAccounts);

    // bank キー解決 (build-marts.js resolveBankKey と同じロジックを JS でミニ実装)
    function resolveBankKey(accountName, subAccountName) {
      if (!accountName) return 'unknown';
      if (accountName === '小口現金' || accountName === '現金') return 'cash_petty';
      if (accountName === '当座預金') return 'cash_current';
      if (accountName === '普通預金_PayPal') return 'paypal';
      if (accountName === '普通預金_ペイオニア') return 'payoneer';
      if (accountName === '定期預金') return 'time_deposit';
      if (accountName === '普通預金') {
        const sub = (subAccountName || '').toLowerCase();
        if (sub.includes('paypay')) return 'paypay';
        if (sub.includes('北おおさか') || sub.includes('十三')) return 'kita_osaka';
        if (sub.includes('京都')) return 'kyoto';
        return 'unknown';
      }
      return 'other';
    }
    const cashByMonth = {};
    for (const m of months) cashByMonth[m] = { paypay: 0, kita_osaka: 0, kyoto: 0, time_deposit: 0, paypal: 0, payoneer: 0, cash_petty: 0, cash_current: 0, unknown: 0, other: 0 };
    for (const r of balRows) {
      const k = resolveBankKey(r.account_name, r.sub_account_name);
      if (cashByMonth[r.month_ym]) cashByMonth[r.month_ym][k] += r.closing_balance_excl_tax || 0;
    }
    const cashTrend = months.map(m => ({ month: m, ...cashByMonth[m] }));

    // 3. モール別売上 月次
    const channelRows = db.prepare(`
      SELECT month_ym, channel_display_name, gross_sales_excl_tax
      FROM v_mirror_mf_channel_sales_latest
      WHERE month_ym IN (${placeholders}) AND gross_sales_excl_tax > 0
    `).all(...months);
    const channelSet = new Set();
    const channelByMonth = {};
    for (const m of months) channelByMonth[m] = {};
    for (const r of channelRows) {
      channelSet.add(r.channel_display_name);
      channelByMonth[r.month_ym][r.channel_display_name] = r.gross_sales_excl_tax;
    }
    const channels = [...channelSet].sort();
    const channelTrend = months.map(m => {
      const obj = { month: m };
      for (const c of channels) obj[c] = channelByMonth[m][c] || 0;
      return obj;
    });

    res.json({
      ok: true, period, months,
      sales_vs_gross: salesVsGross,
      cash_trend: cashTrend,
      channel_trend: channelTrend,
      channels,
    });
  } catch (e) {
    console.error('[exec-dashboard] timeseries error:', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ─── API: PL 試算表形式 (Phase 1c) ───
router.get('/api/trial-balance/pl', (req, res) => {
  try {
    const db = getMirrorDB();
    const period = req.query.period || '12m';
    const months = getMonthList(periodToMonthsBack(period), db);
    const placeholders = months.map(() => '?').join(',');

    // PL 全 role_key × 月別
    const rows = db.prepare(`
      SELECT month_ym, role_key, amount_excl_tax
      FROM v_mirror_mf_pl_monthly_latest
      WHERE month_ym IN (${placeholders})
    `).all(...months);

    // (role_key, month) → amount のテーブル化
    const byKey = {};
    for (const r of rows) {
      if (!byKey[r.role_key]) byKey[r.role_key] = {};
      byKey[r.role_key][r.month_ym] = r.amount_excl_tax;
    }

    // section ごとに totals 計算 + items 並べる
    const sectionTotals = {};
    const result = [];
    for (const sec of PL_SECTIONS) {
      if (sec.computed) {
        // 小計行 (売上総利益, 営業利益, 経常利益)
        let tots;
        if (sec.key === 'gross_profit') {
          tots = months.map(m => (sectionTotals.sales?.[m] || 0) - (sectionTotals.cogs?.[m] || 0));
        } else if (sec.key === 'operating_income') {
          tots = months.map((m, i) => (sectionTotals.gross_profit_arr?.[i] || 0) - (sectionTotals.sgae?.[m] || 0));
        } else if (sec.key === 'ordinary_income') {
          tots = months.map((m, i) => (sectionTotals.operating_income_arr?.[i] || 0) + (sectionTotals.non_op_revenue?.[m] || 0) - (sectionTotals.non_op_expense?.[m] || 0));
        }
        sectionTotals[sec.key + '_arr'] = tots;
        result.push({ name: sec.name, level: 0, is_subtotal: true, totals: tots });
        continue;
      }
      // 通常 section: items の合計 (sales_return は -)
      const monthTotals = months.map(m => {
        let s = 0;
        for (const it of sec.items) {
          const v = byKey[it]?.[m] || 0;
          if (it === 'sales_return') s -= v;
          else s += v;
        }
        return s;
      });
      sectionTotals[sec.key] = Object.fromEntries(months.map((m, i) => [m, monthTotals[i]]));

      const items = sec.items.map(it => ({
        role_key: it,
        name: ROLE_DISPLAY[it] || it,
        totals: months.map(m => byKey[it]?.[m] || 0),
        is_negative_in_section: it === 'sales_return',
      }));
      result.push({ name: sec.name, level: 0, is_subtotal: false, totals: monthTotals, items });
    }

    res.json({ ok: true, period, months, sections: result });
  } catch (e) {
    console.error('[exec-dashboard] trial-balance/pl error:', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ─── API: BS 試算表形式 (Phase 1c、現預金中心) ───
router.get('/api/trial-balance/bs', (req, res) => {
  try {
    const db = getMirrorDB();
    const period = req.query.period || '12m';
    const months = getMonthList(periodToMonthsBack(period), db);
    const placeholders = months.map(() => '?').join(',');

    // 現金及び預金の口座別月末残高
    const cashAccounts = ['普通預金', '当座預金', '現金', '小口現金', '普通預金_PayPal', '普通預金_ペイオニア', '定期預金'];
    const rows = db.prepare(`
      SELECT month_ym, account_name, sub_account_name, closing_balance_excl_tax
      FROM v_mirror_mf_balance_snapshot_monthly_latest
      WHERE month_ym IN (${placeholders}) AND account_name IN (${cashAccounts.map(()=>'?').join(',')})
    `).all(...months, ...cashAccounts);

    // (account_name+sub_account_name, month) → balance
    const byAcc = {};
    const accSet = new Set();
    for (const r of rows) {
      const label = r.account_name + (r.sub_account_name ? '/' + r.sub_account_name : '');
      accSet.add(label);
      if (!byAcc[label]) byAcc[label] = {};
      byAcc[label][r.month_ym] = r.closing_balance_excl_tax;
    }
    const accs = [...accSet].sort();
    const items = accs.map(a => ({
      name: a,
      totals: months.map(m => byAcc[a]?.[m] || 0),
    }));
    const totals = months.map(m => items.reduce((s, it, i) => s + (it.totals[i] || 0), 0));

    res.json({
      ok: true, period, months,
      sections: [
        { name: '現金及び預金合計', level: 0, is_subtotal: false, totals, items },
      ],
    });
  } catch (e) {
    console.error('[exec-dashboard] trial-balance/bs error:', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ─── API: 第N期 累計 FY summary (Phase 1d-2) ───
//   v_mirror_mf_fy_summary_latest から 当期 + 前期 (最大 2 行) を返す
//   経営指標: 売上累計 / 粗利 / 営業利益 / 経常利益 / 当期純利益 / 労働分配率 / ROA / 1人当たり限界利益
//
//   注意 (Codex review):
//     - HEADCOUNT は「現在値」、過去 FY の指標も今の HEADCOUNT で割る (履歴整合性なし)
//       将来要件次第で fy_summary mart に headcount 列追加すべき
//     - LIMIT 2 で固定 (3 行以上は silent drop しない)
const DEFAULT_HEADCOUNT = 13;
function resolveHeadcount() {
  const raw = process.env.MF_HEADCOUNT;
  if (!raw) return DEFAULT_HEADCOUNT;
  const n = parseInt(raw, 10);
  return Number.isFinite(n) && n > 0 ? n : DEFAULT_HEADCOUNT;
}
router.get('/api/fy-summary', (req, res) => {
  try {
    const db = getMirrorDB();
    const rows = db.prepare(`
      SELECT * FROM v_mirror_mf_fy_summary_latest
      ORDER BY fy_number DESC
      LIMIT 2
    `).all();
    if (!rows || rows.length === 0) {
      return res.json({ ok: true, available: false, message: 'FY summary 未 sync (miniPC で Phase 1d-2 build-marts + sync 実行待ち)' });
    }
    const HEADCOUNT = resolveHeadcount();
    // 鮮度情報 (Codex Q6): どの run / どの確定月のデータか UI に出す
    const freshestRow = rows[0];
    const meta = {
      run_id: freshestRow.run_id,
      cumulative_through_ym: freshestRow.cumulative_through_ym,
      synced_at: freshestRow.synced_at || null,
    };
    // 各 FY に経営指標を追加
    const enriched = rows.map(r => {
      const sales = r.sales_cum || 0;
      const grossProfit = r.gross_profit_cum || 0;
      const operating = r.operating_income_cum || 0;
      const ordinary = r.ordinary_income_cum || 0;
      const personnel = r.personnel_cost_cum || 0;
      const assetAvg = r.total_asset_average || 0;
      // 限界利益 = 粗利 (B-Faith 戦略: 原価=変動費)
      const marginalProfit = grossProfit;
      return {
        fy_number: r.fy_number,
        fy_start_ym: r.fy_start_ym,
        fy_end_ym: r.fy_end_ym,
        cumulative_through_ym: r.cumulative_through_ym,
        months_in_cumulative: r.months_in_cumulative,
        is_fy_completed: r.is_fy_completed,
        sales_cum: sales,
        cogs_cum: r.cogs_cum || 0,
        gross_profit_cum: grossProfit,
        sgae_cum: r.sgae_cum || 0,
        operating_income_cum: operating,
        ordinary_income_cum: ordinary,
        personnel_cost_cum: personnel,
        total_asset_average: assetAvg,
        // 経営指標 (PDF 経営実績報告書準拠)
        gross_profit_pct: sales > 0 ? +((grossProfit / sales) * 100).toFixed(2) : null,
        ordinary_profit_pct: sales > 0 ? +((ordinary / sales) * 100).toFixed(2) : null,
        marginal_profit_pct: sales > 0 ? +((marginalProfit / sales) * 100).toFixed(2) : null,
        labor_distribution_pct: marginalProfit > 0 ? +((personnel / marginalProfit) * 100).toFixed(2) : null,
        roa_pct: assetAvg > 0 ? +((ordinary / assetAvg) * 100).toFixed(2) : null,
        marginal_profit_per_employee: HEADCOUNT > 0 ? Math.round(marginalProfit / HEADCOUNT) : null,
        headcount: HEADCOUNT,
      };
    });
    res.json({ ok: true, available: true, fy_summary: enriched, headcount: HEADCOUNT, meta });
  } catch (e) {
    console.error('[exec-dashboard] fy-summary error:', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ─── API: 詳細 (既存 5 タブ用、遅延ロード) ───
router.get('/api/details', (req, res) => {
  try {
    const db = getMirrorDB();

    const plRows = db.prepare(`SELECT * FROM v_mirror_mf_pl_monthly_latest ORDER BY month_ym DESC, role_key`).all();
    const channelRows = db.prepare(`
      SELECT * FROM v_mirror_mf_channel_sales_latest
      WHERE month_ym >= date('now', 'start of month', '-12 months')
      ORDER BY month_ym DESC, gross_sales_excl_tax DESC
    `).all();
    const balanceRows = db.prepare(`
      SELECT month_ym, account_name, sub_account_name, closing_balance_excl_tax
      FROM v_mirror_mf_balance_snapshot_monthly_latest
      WHERE month_ym >= date('now', 'start of month', '-12 months')
        AND account_name IN ('普通預金', '当座預金', '現金', '小口現金', '普通預金_PayPal', '普通預金_ペイオニア', '定期預金')
      ORDER BY month_ym DESC, account_name, sub_account_name
    `).all();
    const cashRows = db.prepare(`
      SELECT bank_account_key,
        SUM(CASE WHEN direction='in' THEN amount_excl_tax ELSE 0 END) as in_total,
        SUM(CASE WHEN direction='out' THEN amount_excl_tax ELSE 0 END) as out_total,
        SUM(CASE WHEN direction='in' THEN event_count ELSE 0 END) as in_cnt,
        SUM(CASE WHEN direction='out' THEN event_count ELSE 0 END) as out_cnt
      FROM v_mirror_mf_cash_events_daily_latest
      WHERE movement_date >= date('now', '-90 days')
      GROUP BY bank_account_key
      ORDER BY (in_total + out_total) DESC
    `).all();

    res.json({ ok: true, plRows, channelRows, balanceRows, cashRows });
  } catch (e) {
    console.error('[exec-dashboard] details error:', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

export default router;
