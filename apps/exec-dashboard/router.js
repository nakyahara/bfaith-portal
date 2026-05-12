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
// period パラメータ: '6m' | '12m' (default) | '24m' | 'all' | 'fy:<N>' (第N期、Phase 1d-3c+)
function periodToMonthsBack(period) {
  const map = { '6m': 6, '12m': 12, '24m': 24, 'all': 60 };
  return map[period] ?? 12;
}
// period 文字列から表示対象の月リスト (YYYY-MM 配列、古い順) を返す。
//   'Nm'/'all': 最終確定月 (executive_top.current_month_ym) を base に N ヶ月遡る
//   'fy:<N>'  : 第N期 の fy_start_ym 〜 cumulative_through_ym (mart_mf_fy_summary より)
function getMonthList(period, db) {
  // fy:<N> モード
  const fyMatch = /^fy:(\d+)$/.exec(String(period || ''));
  if (fyMatch && db) {
    try {
      const fyNum = parseInt(fyMatch[1], 10);
      const fy = db.prepare(`SELECT fy_start_ym, cumulative_through_ym FROM v_mirror_mf_fy_summary_latest WHERE fy_number = ?`).get(fyNum);
      if (fy && fy.fy_start_ym && fy.cumulative_through_ym) {
        const out = [];
        let [y, m] = fy.fy_start_ym.split('-').map(Number);
        const end = fy.cumulative_through_ym;
        while (true) {
          const ym = `${y}-${String(m).padStart(2, '0')}`;
          out.push(ym);
          if (ym >= end) break;
          m++; if (m > 12) { m = 1; y++; }
          if (out.length > 60) break;  // safety
        }
        if (out.length > 0) return out;
      }
    } catch (e) { /* mirror_mf_fy_summary 無し等 → fallback */ }
    // fy モードだが取得失敗 → 12m 相当に fallback
    period = '12m';
  }
  // 'Nm' / 'all' モード
  const monthsBack = periodToMonthsBack(period);
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
    const months = getMonthList(period, db);

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
        cogs: r.cogs || 0,
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

    // 4. BS 推移 (Phase 1d-3c): 資産=負債+純資産 積み上げ棒 + 折れ線用
    //    mirror_mf_bs_monthly が未デプロイの場合は空配列 (UI 側でグラフ非表示)
    let bsTrend = [];
    try {
      const bsRows = db.prepare(`
        SELECT month_ym,
          cash_total, ar_total, inventory_total, other_current_asset,
          tangible_fixed_asset, investment_other, fixed_asset_total, total_asset,
          ap_total, short_loan_total, other_current_liab, current_liab_total,
          long_loan_total, other_fixed_liab, fixed_liab_total, total_liab,
          capital, retained_balance, current_period_profit, display_total_equity
        FROM v_mirror_mf_bs_monthly_latest WHERE month_ym IN (${placeholders})
      `).all(...months);
      const bsByM = {};
      for (const r of bsRows) bsByM[r.month_ym] = r;
      const cogsByM = Object.fromEntries(months.map(m => [m, (plByMonth[m]?.cogs) || 0]));
      bsTrend = months.map(m => {
        const r = bsByM[m];
        if (!r) return { month: m, empty: true };
        const inv = r.inventory_total || 0;
        const monthCogs = cogsByM[m] || 0;
        // 在庫回転日数 (月次): 棚卸資産 / 1日あたり売上原価 (その月の cogs ÷ 30.4)
        const invDays = monthCogs > 0 ? +((inv / (monthCogs / 30.4))).toFixed(1) : null;
        return {
          month: m,
          // 資産側 (積み上げ): 現預金 / 売上債権 / 棚卸資産 / その他流動資産 / 固定資産
          a_cash: r.cash_total || 0,
          a_ar: r.ar_total || 0,
          a_inventory: inv,
          a_other_current: r.other_current_asset || 0,
          a_fixed: r.fixed_asset_total || 0,
          // 負債純資産側 (積み上げ): 仕入債務 / 短期借入金等 / その他流動負債 / 固定負債 / 純資産
          l_ap: r.ap_total || 0,
          l_short_loan: r.short_loan_total || 0,
          l_other_current: r.other_current_liab || 0,
          l_fixed: r.fixed_liab_total || 0,
          l_equity: r.display_total_equity || 0,
          // 折れ線用 サマリ
          total_asset: r.total_asset || 0,
          net_assets: r.display_total_equity || 0,
          total_borrowings: (r.short_loan_total || 0) + (r.long_loan_total || 0),
          // 在庫回転 (月次)
          inventory: inv,
          month_cogs: monthCogs,
          inventory_turnover_days: invDays,
        };
      });
    } catch (e) {
      // mirror_mf_bs_monthly が無い (Phase 1d-3b デプロイ前) → 空のまま
      if (!/no such table|no such view/i.test(String(e && e.message || e))) {
        console.warn('[exec-dashboard] timeseries bs_trend error (非致命):', e.message);
      }
      bsTrend = [];
    }

    res.json({
      ok: true, period, months,
      sales_vs_gross: salesVsGross,
      cash_trend: cashTrend,
      channel_trend: channelTrend,
      channels,
      bs_trend: bsTrend,
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
    const months = getMonthList(period, db);
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

// ─── API: BS 試算表形式 (Phase 1d-3b、全 BS section + 細目) ───
//   mirror_mf_bs_monthly (section合計) + mirror_mf_bs_subaccount_monthly (細目) から組み立て。
//   bs_monthly が空 (Phase 1d-3b デプロイ前の旧 sync) の場合は現預金のみの Phase 1c 形式に fallback。
// VIEW/テーブル未デプロイ (mirror_mf_bs_monthly 等が無い) のみ fallback 対象。それ以外の SQL エラーは握り潰さない。
function isMissingMirrorTable(e) {
  const m = String(e && e.message || e || "");
  return /no such table|no such view/i.test(m);
}
const BS_TABLE_LAYOUT = [
  // [表示名, 種別, bs_monthly列 or null, section群 (細目), is_subtotal]
  { name: '現金及び預金', col: 'cash_total', secs: ['cash'] },
  { name: '売上債権 (売掛金等、貸倒引当金控除後)', col: 'ar_total', secs: ['ar'] },
  { name: '棚卸資産 (商品)', col: 'inventory_total', secs: ['inventory'] },
  { name: 'その他流動資産', col: 'other_current_asset', secs: ['other_current_asset'] },
  { name: '流動資産合計', col: 'current_asset_total', subtotal: true },
  { name: '有形固定資産', col: 'tangible_fixed_asset', secs: ['tangible_fixed_asset'] },
  { name: '投資その他の資産', col: 'investment_other', secs: ['investment_other'] },
  { name: '固定資産合計', col: 'fixed_asset_total', subtotal: true },
  { name: '資産の部合計', col: 'total_asset', subtotal: true },
  { name: '仕入債務 (買掛金)', col: 'ap_total', secs: ['ap'] },
  { name: 'その他流動負債 (短期借入金/未払金/未払消費税/預り金等)', col: null, customCols: ['short_loan_total', 'other_current_liab'], secs: ['short_loan', 'other_current_liab'] },
  { name: '流動負債合計', col: 'current_liab_total', subtotal: true },
  { name: '固定負債 (長期借入金/長期未払金/役員借入金等)', col: 'fixed_liab_total', secs: ['long_loan', 'other_fixed_liab'] },
  { name: '負債の部合計', col: 'total_liab', subtotal: true },
  { name: '資本金', col: 'capital' },
  { name: '繰越利益剰余金 (前期末)', col: 'retained_balance' },
  { name: '当期純利益 (期中累計)', col: 'current_period_profit' },
  { name: '純資産の部合計', col: 'display_total_equity', subtotal: true },
];
router.get('/api/trial-balance/bs', (req, res) => {
  try {
    const db = getMirrorDB();
    const period = req.query.period || '12m';
    const months = getMonthList(period, db);
    const placeholders = months.map(() => '?').join(',');

    // mirror_mf_bs_monthly が利用可能か (Phase 1d-3b デプロイ済か) チェック
    let bsMonthlyRows = [];
    try {
      bsMonthlyRows = db.prepare(`
        SELECT * FROM v_mirror_mf_bs_monthly_latest WHERE month_ym IN (${placeholders})
      `).all(...months);
    } catch (e) {
      if (!isMissingMirrorTable(e)) throw e;  // 未デプロイ (no such table/view) 以外の SQL エラーは握り潰さない
      bsMonthlyRows = [];
    }

    if (bsMonthlyRows.length === 0) {
      // fallback: Phase 1c 形式 (現預金のみ、balance_snapshot 経由)
      const cashAccounts = ['普通預金', '当座預金', '現金', '小口現金', '普通預金_PayPal', '普通預金_ペイオニア', '定期預金'];
      const rows = db.prepare(`
        SELECT month_ym, account_name, sub_account_name, closing_balance_excl_tax
        FROM v_mirror_mf_balance_snapshot_monthly_latest
        WHERE month_ym IN (${placeholders}) AND account_name IN (${cashAccounts.map(()=>'?').join(',')})
      `).all(...months, ...cashAccounts);
      const byAcc = {}; const accSet = new Set();
      for (const r of rows) {
        const label = r.account_name + (r.sub_account_name ? '/' + r.sub_account_name : '');
        accSet.add(label);
        if (!byAcc[label]) byAcc[label] = {};
        byAcc[label][r.month_ym] = r.closing_balance_excl_tax;
      }
      const accs = [...accSet].sort();
      const items = accs.map(a => ({ name: a, totals: months.map(m => byAcc[a]?.[m] || 0) }));
      const totals = months.map(m => items.reduce((s, it, i) => s + (it.totals[i] || 0), 0));
      return res.json({ ok: true, period, months, fallback: true,
        sections: [{ name: '現金及び預金合計', level: 0, is_subtotal: false, totals, items }] });
    }

    // bs_monthly を month → row でindex
    const bsByMonth = {};
    for (const r of bsMonthlyRows) bsByMonth[r.month_ym] = r;

    // bs_subaccount (細目) を section群 → label → month → value
    let subRows = [];
    try {
      subRows = db.prepare(`
        SELECT month_ym, account_name, sub_account_name, section, closing_balance_excl_tax, is_hub_null_sub
        FROM v_mirror_mf_bs_subaccount_monthly_latest WHERE month_ym IN (${placeholders})
      `).all(...months);
    } catch (e) {
      if (!isMissingMirrorTable(e)) throw e;
      subRows = [];
    }
    // section → label → {month: value}
    const subBySection = {};
    for (const r of subRows) {
      const sec = r.section;
      if (!subBySection[sec]) subBySection[sec] = {};
      // 補助科目なし振替ハブは「未振替」と注記
      let label = r.account_name + (r.sub_account_name ? '/' + r.sub_account_name : '');
      if (r.is_hub_null_sub) label = r.account_name + ' (未振替ハブ)';
      if (!subBySection[sec][label]) subBySection[sec][label] = {};
      subBySection[sec][label][r.month_ym] = (subBySection[sec][label][r.month_ym] || 0) + r.closing_balance_excl_tax;
    }

    const sections = [];
    for (const layout of BS_TABLE_LAYOUT) {
      // section 行の monthly totals
      const totals = months.map(m => {
        const row = bsByMonth[m];
        if (!row) return 0;
        if (layout.customCols) return layout.customCols.reduce((s, c) => s + (row[c] || 0), 0);
        return row[layout.col] || 0;
      });
      // 細目 (section群を横断、label でユニーク化。label に空白を含むので Map-of-Maps で保持)
      let items = [];
      if (layout.secs) {
        const seen = new Set();
        const pairs = [];
        for (const sec of layout.secs) {
          for (const label of Object.keys(subBySection[sec] || {})) {
            if (seen.has(label)) continue;
            seen.add(label);
            pairs.push({ sec, label });
          }
        }
        pairs.sort((a, b) => String(a.label).localeCompare(String(b.label), "ja"));
        items = pairs.map(p => ({
          name: p.label,
          totals: months.map(mm => (subBySection[p.sec][p.label] && subBySection[p.sec][p.label][mm]) || 0),
        }));
      }
      sections.push({
        name: layout.name,
        level: 0,
        is_subtotal: !!layout.subtotal,
        totals,
        items: layout.subtotal ? undefined : items,
      });
    }

    // Phase 1d-3c+: 最新確定月 (months の最後) の BS を「5つの箱」図解用に整形
    //   左列 (資産の部): 流動資産 box {現預金/売上債権/棚卸資産/その他流動資産} + 固定資産 box {有形固定資産/投資その他}
    //   右列 (負債純資産の部): 流動負債 box {仕入債務/短期借入金等/その他流動負債} + 固定負債 box {長期借入金等} + 純資産 box {資本金/繰越剰余金/当期純利益(期中)}
    let latestBalance = null;
    const latestYm = months.length > 0 ? months[months.length - 1] : null;
    const latestRow = latestYm ? bsByMonth[latestYm] : null;
    if (latestRow) {
      const sub = (label, value, color) => ({ label, value: value || 0, color });
      latestBalance = {
        month: latestYm,
        total_asset: latestRow.total_asset || 0,
        total_liab: latestRow.total_liab || 0,
        net_assets: latestRow.display_total_equity || 0,
        // 左列 = 資産の部 (上から: ①流動資産、②固定資産)
        asset_boxes: [
          {
            key: 'current_asset', label: '①流動資産', note: '1年以内に現金化できる資産',
            total: latestRow.current_asset_total || 0, color: '#fce4ec', border: '#f48fb1',
            items: [
              sub('現預金', latestRow.cash_total, '#f8bbd0'),
              sub('売上債権 (売掛金等)', latestRow.ar_total, '#f48fb1'),
              sub('棚卸資産 (商品)', latestRow.inventory_total, '#f06292'),
              sub('その他流動資産', latestRow.other_current_asset, '#ec407a'),
            ].filter(i => i.value !== 0),
          },
          {
            key: 'fixed_asset', label: '②固定資産', note: '1年以内に現金化されない資産',
            total: latestRow.fixed_asset_total || 0, color: '#e3f2fd', border: '#90caf9',
            items: [
              sub('有形固定資産', latestRow.tangible_fixed_asset, '#bbdefb'),
              sub('投資その他の資産', latestRow.investment_other, '#90caf9'),
            ].filter(i => i.value !== 0),
          },
        ],
        // 右列 = 負債・純資産の部 (上から: ①流動負債、②固定負債、純資産)
        liab_equity_boxes: [
          {
            key: 'current_liab', label: '①流動負債', note: '1年以内に返済義務がある負債',
            total: latestRow.current_liab_total || 0, color: '#fff3e0', border: '#ffcc80',
            items: [
              sub('仕入債務 (買掛金)', latestRow.ap_total, '#ffe0b2'),
              sub('短期借入金等', latestRow.short_loan_total, '#ffcc80'),
              sub('その他流動負債 (未払金/未払消費税等)', latestRow.other_current_liab, '#ffb74d'),
            ].filter(i => i.value !== 0),
          },
          {
            key: 'fixed_liab', label: '②固定負債', note: '返済までの期間が1年を超える負債',
            total: latestRow.fixed_liab_total || 0, color: '#fffde7', border: '#fff176',
            items: [
              sub('長期借入金等', latestRow.fixed_liab_total, '#fff59d'),
            ].filter(i => i.value !== 0),
          },
          {
            key: 'net_assets', label: '純資産', note: '返済義務がない資産 (自己資本)',
            total: latestRow.display_total_equity || 0, color: '#e8f5e9', border: '#a5d6a7',
            items: [
              sub('資本金', latestRow.capital, '#c8e6c9'),
              sub('繰越利益剰余金 (前期末)', latestRow.retained_balance, '#a5d6a7'),
              sub('当期純利益 (期中累計)', latestRow.current_period_profit, '#81c784'),
            ].filter(i => i.value !== 0),
          },
        ],
      };
    }

    res.json({ ok: true, period, months, sections, latest_balance: latestBalance });
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
    // 各 FY に経営指標を追加 (Phase 1d-3c: KPI 9 個 + 回転日数)
    const enriched = rows.map(r => {
      const sales = r.sales_cum || 0;
      const cogs = r.cogs_cum || 0;
      const grossProfit = r.gross_profit_cum || 0;
      const operating = r.operating_income_cum || 0;
      const ordinary = r.ordinary_income_cum || 0;
      const personnel = r.personnel_cost_cum || 0;
      const assetAvg = r.total_asset_average || 0;
      const arAvg = r.ar_average || 0;
      const apAvg = r.ap_average || 0;
      const invAvg = r.inventory_average || 0;
      const cashClosing = r.cash_closing || 0;
      const arClosing = r.ar_closing || 0;
      const totalAssetClosing = r.total_asset_closing || 0;
      const currentLiabClosing = r.current_liab_closing || 0;
      const equityClosing = r.total_equity_closing || 0;
      const months = r.months_in_cumulative || 0;
      // 限界利益 = 粗利 (B-Faith 戦略: 原価=変動費)
      const marginalProfit = grossProfit;
      const sgaeCum = r.sgae_cum || 0;
      // 期間日数 (近似: 月数 × 30.4)。回転日数計算の分母用
      const periodDays = months > 0 ? months * 30.4 : 365;
      const round2 = (v) => (v == null || !isFinite(v)) ? null : +Number(v).toFixed(2);
      const round1 = (v) => (v == null || !isFinite(v)) ? null : +Number(v).toFixed(1);
      // 月間固定費 = 販管費 月平均 (B-Faith 戦略: 販管費=固定費)
      const monthlyFixedCost = months > 0 ? sgaeCum / months : 0;
      // DSO/在庫回転/DPO の日数 (CCC 計算用、null は 0 扱い)
      const dsoD = sales > 0 ? arAvg / (sales / periodDays) : null;
      const invD = cogs > 0 ? invAvg / (cogs / periodDays) : null;
      const dpoD = cogs > 0 ? apAvg / (cogs / periodDays) : null;
      return {
        fy_number: r.fy_number,
        fy_start_ym: r.fy_start_ym,
        fy_end_ym: r.fy_end_ym,
        cumulative_through_ym: r.cumulative_through_ym,
        months_in_cumulative: months,
        is_fy_completed: r.is_fy_completed,
        sales_cum: sales,
        cogs_cum: cogs,
        gross_profit_cum: grossProfit,
        sgae_cum: r.sgae_cum || 0,
        operating_income_cum: operating,
        ordinary_income_cum: ordinary,
        personnel_cost_cum: personnel,
        total_asset_average: assetAvg,
        ar_average: arAvg, ap_average: apAvg, inventory_average: invAvg,
        cash_closing: cashClosing, ar_closing: arClosing,
        total_asset_closing: totalAssetClosing, current_liab_closing: currentLiabClosing,
        total_equity_closing: equityClosing,
        // ─── 経営指標 KPI 9 個 (PDF 経営実績報告書準拠) ───
        // 1. 限界利益率 (= 粗利率、原価=変動費)
        marginal_profit_pct: round2(sales > 0 ? marginalProfit / sales * 100 : null),
        // 2. 労働分配率 (= 人件費 / 限界利益)
        labor_distribution_pct: round2(marginalProfit > 0 ? personnel / marginalProfit * 100 : null),
        // 3. 1人当たり限界利益
        marginal_profit_per_employee: HEADCOUNT > 0 ? Math.round(marginalProfit / HEADCOUNT) : null,
        // 4. 売上高経常利益率
        ordinary_profit_pct: round2(sales > 0 ? ordinary / sales * 100 : null),
        // 5. 当座比率 (= 当座資産(現預金+売上債権) / 流動負債) ※受取手形・有価証券は B-Faith 無し
        quick_ratio_pct: round2(currentLiabClosing > 0 ? (cashClosing + arClosing) / currentLiabClosing * 100 : null),
        // 6. 自己資本比率 (= 純資産 / 総資産)
        equity_ratio_pct: round2(totalAssetClosing > 0 ? equityClosing / totalAssetClosing * 100 : null),
        // 7. ROA (= 経常利益 / 平均総資産)
        roa_pct: round2(assetAvg > 0 ? ordinary / assetAvg * 100 : null),
        // 8. DSO 売上債権回転日数 (= 平均売上債権 / 1日あたり売上)
        dso_days: round1(dsoD),
        // 9. DPO 仕入債務回転日数 (= 平均仕入債務 / 1日あたり売上原価)
        dpo_days: round1(dpoD),
        // (おまけ) 在庫回転日数 (= 平均棚卸資産 / 1日あたり売上原価)
        inventory_turnover_days: round1(invD),
        // CCC キャッシュ・コンバージョン・サイクル (= DSO + 在庫回転日数 − DPO)。短いほど資金効率良い
        ccc_days: (dsoD != null && invD != null && dpoD != null) ? round1(dsoD + invD - dpoD) : null,
        // 月間固定費 (= 販管費 月平均)
        monthly_fixed_cost: Math.round(monthlyFixedCost),
        // 資金繰り残月数 (= 期末現預金 ÷ 月間固定費)。「あと何ヶ月分の固定費が手元にあるか」
        cash_runway_months: round1(monthlyFixedCost > 0 ? cashClosing / monthlyFixedCost : null),
        // 粗利率 (参考、限界利益率と同値)
        gross_profit_pct: round2(sales > 0 ? grossProfit / sales * 100 : null),
        headcount: HEADCOUNT,
        period_days_used: Math.round(periodDays),
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
