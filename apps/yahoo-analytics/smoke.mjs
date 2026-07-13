// yahoo-analytics 機能スモーク: fixture 投入 → 全 query 関数を実行して形を検証
// 使い方: DATA_DIR=<空ディレクトリ> node apps/yahoo-analytics/smoke.mjs
// ⚠️ DATA_DIR 内の warehouse-mirror.db の対象テーブルを DELETE するため本番 DATA_DIR で実行禁止
import fs from 'fs';
import path from 'path';
import { initMirrorDB, getMirrorDB } from '../warehouse-mirror/db.js';
import * as q from './queries.js';
import * as ins from './insights.js';

// 本番 DB 誤実行ガード 1: DATA_DIR 未指定 (= cwd の本番 DB に向く) なら即中断
if (!process.env.DATA_DIR) {
  console.error('FATAL: DATA_DIR が未指定です。専用の空ディレクトリを指定してください (例: DATA_DIR=c:/tmp/ya-smoke-data)');
  process.exit(2);
}

// 本番 DB 誤実行ガード 2: marker file 方式 (Codex R1 low)。
// この smoke が過去に作った DB (marker あり) 以外の既存 warehouse-mirror.db は DELETE しない
const markerPath = path.join(process.env.DATA_DIR, '.yahoo-analytics-smoke-db');
const dbExisted = fs.existsSync(path.join(process.env.DATA_DIR, 'warehouse-mirror.db'));
if (dbExisted && !fs.existsSync(markerPath)) {
  console.error('FATAL: DATA_DIR に既存の warehouse-mirror.db があります (smoke 生成マーカーなし)。実 DB の可能性があるため中断します');
  process.exit(2);
}

initMirrorDB();
const db = getMirrorDB();
fs.writeFileSync(markerPath, `created by apps/yahoo-analytics/smoke.mjs at ${new Date().toISOString()}\n`);

// 本番 DB 誤実行ガード 3: fixture 規模を超えるデータ、または確定済み月次があれば実 DB とみなし中断
const prodGuard = db.prepare(`SELECT COUNT(*) AS c FROM mirror_yahoo_finance_sku_daily`).get().c;
const martGuard = db.prepare(`SELECT COUNT(*) AS c FROM mart_yahoo_monthly_summary WHERE confirmed_at IS NOT NULL`).get().c;
if (prodGuard > 500 || martGuard > 5) {
  console.error(`FATAL: 実データらしき規模を検出 (fact ${prodGuard} 行 / 確定月 ${martGuard} 件)。本番 DB と思われるため中断します`);
  process.exit(2);
}

const today = q.jstToday();
const d = (n) => q.addDays(today, -n);
const ymNow = today.slice(0, 7);
const ymPrev = q.addMonths(ymNow, -1);

// ─── fixture ───
const tx = db.transaction(() => {
  for (const t of ['mirror_yahoo_finance_sku_daily', 'mart_yahoo_monthly_summary', 'mirror_products', 'yadash_settings', 'yadash_rate_master',
    'mirror_yahoo_item_daily', 'mirror_yahoo_keyword_daily', 'mirror_yahoo_store_device_daily', 'mirror_yahoo_inflow_daily', 'mirror_yahoo_user_attr_daily', 'mirror_yahoo_flash_hourly',
    'yadash_insight_occurrences', 'yadash_insight_events', 'yadash_insight_runs']) {
    try { db.exec(`DELETE FROM ${t}`); } catch {}
  }

  // 統計 fixture: alpha は PV も CVR も健全 / zeta は PV 大量なのに売れない
  const insT = db.prepare(`INSERT INTO mirror_yahoo_item_daily
    (date_jst, item_code, sub_code, item_name, sales_yen, orders, units, buyers, pv_premium_ship, pv_normal, visitors, cart_adds, favorites, source_run_id, source_row_hash, synced_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'smoke', ?, 't')`);
  const insK = db.prepare(`INSERT INTO mirror_yahoo_keyword_daily
    (date_jst, keyword, rank, inflow, sales_yen, orders, source_run_id, source_row_hash, synced_at)
    VALUES (?, ?, ?, ?, ?, ?, 'smoke', ?, 't')`);
  for (let i = 1; i <= 28; i++) {
    const date = d(i);
    insT.run(date, 'ya-alpha', 'ya-alpha-bk', 'アルファ精油BK', 10000, 10, 10, 9, 100, 100, 100, 20, 5, `a${i}`);
    insT.run(date, 'ya-zeta', '', 'ゼータ棚ざらし', 0, 0, 0, 0, 300, 300, 400, 2, 1, `z${i}`);
    // 健全SKU×3 (high_pv_low_cvr の中央値算出には5SKU以上必要)
    insT.run(date, 'ya-eta1', '', 'イータ1', 3000, 3, 3, 3, 20, 20, 40, 5, 1, `e1${i}`);
    insT.run(date, 'ya-eta2', '', 'イータ2', 2000, 2, 2, 2, 15, 15, 30, 4, 1, `e2${i}`);
    insT.run(date, 'ya-eta3', '', 'イータ3', 1000, 1, 1, 1, 10, 10, 20, 3, 1, `e3${i}`);
    // KW「アロマ」: 直近7日 (i<=7) は流入が激減
    insK.run(date, 'アロマ', 1, i <= 7 ? 5 : 40, i <= 7 ? 1000 : 8000, i <= 7 ? 1 : 8, `k${i}`);
    insK.run(date, '精油', 2, 30, 5000, 5, `k2${i}`);
  }

  // 商品マスタ (SKU詳細のマスタ表示 + 未解決候補サジェスト用)
  const insP = db.prepare(`INSERT INTO mirror_products (商品コード, 商品名, 商品区分, 標準売価, 原価, 原価状態, 消費税率, updated_at)
    VALUES (?, ?, '通常', ?, ?, 'confirmed', 0.1, 't')`);
  insP.run('NE-A', 'アルファ精油BK (マスタ)', 1200, 300);
  insP.run('NE-B', 'ベータ茶葉 (マスタ)', 2400, 1000);
  insP.run('ya-gamma-01', 'ガンマ雑貨01 (マスタ)', 600, 200);   // gamma の前方一致候補
  insP.run('ya-gamma-02', 'ガンマ雑貨02 (マスタ)', 700, 250);
  insP.run('ya-delta-01', 'デルタ (マスタ)', 900, 400);         // delta のヒューリスティック候補

  const ins = db.prepare(`INSERT INTO mirror_yahoo_finance_sku_daily (
    date_jst, yahoo_sku_key, ne_code, variant_key, resolution_method, unresolved_sku_flag,
    product_name, units_ordered, units_cancelled, units_net_sold,
    sales_principal_jpy_incl, sales_postage_jpy_incl, gross_sales_jpy_incl,
    net_sales_before_point_jpy_incl, listing_sales_estimated_jpy_incl,
    coupon_shop_jpy_incl, use_point_jpy_incl,
    mall_fee_jpy_incl, mall_fee_calc_method, shipping_cost_jpy_incl, shipping_quality,
    unit_cost_snapshot_incl, cogs_amount_jpy_incl,
    variable_margin_partial_jpy_incl, margin_confidence,
    cost_status, is_cost_complete, data_quality_score,
    source_run_id, source_row_hash, synced_at
  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, 0, ?, ?, ?, ?, ?, ?, 'estimated_10pct', ?, ?, ?, ?, ?, 'partial', ?, ?, ?, 'smoke', 'h', 't')`);

  // 90日分 3 SKU (税込、Yahoo は postage=0 固定・units_cancelled=0 固定)
  for (let i = 0; i < 90; i++) {
    const date = d(i);
    {
      // alpha (sub_match): 10個/日 単価1000。クーポン300・ポイント利用500
      // 実データ仕様: sub_code あり SKU は yahoo_sku_key = variant_key = sub_code 全体 (build SQL 裏取り済)
      const principal = 10000, coupon = 300, usePoint = 500;
      const mallFee = 1000, shipping = 800, cogs = 3000;
      const vm = principal - cogs - mallFee - shipping - coupon - usePoint; // 4400
      ins.run(date, 'ya-alpha-bk', 'NE-A', 'ya-alpha-bk', 'sub_match', 0, 'アルファ精油BK', 10, 10,
        principal, principal, principal - coupon, principal - usePoint - coupon,
        coupon, usePoint, mallFee, shipping, 'actual', 300, cogs, vm,
        'complete', 1, 100);
    }
    {
      // beta (parent_match): 4個/日 単価2000。控除なし
      const principal = 8000, mallFee = 800, shipping = 600, cogs = 4000;
      const vm = principal - cogs - mallFee - shipping; // 2600
      ins.run(date, 'ya-beta', 'NE-B', '', 'parent_match', 0, 'ベータ茶葉', 4, 4,
        principal, principal, principal, principal,
        0, 0, mallFee, shipping, 'estimated_rates', 1000, cogs, vm,
        'complete', 1, 100);
    }
    {
      // gamma (unresolved): 1個/日 単価500。原価0円計上 = 粗利過大の典型
      const principal = 500, mallFee = 50;
      const vm = principal - mallFee; // 450
      ins.run(date, 'ya-gamma', null, '', 'unresolved', 1, 'ガンマ雑貨', 1, 1,
        principal, principal, principal, principal,
        0, 0, mallFee, 0, 'missing', null, 0, vm,
        'missing_cost', 0, 40);
    }
  }

  // 先月は請求明細取込済み (確定): 広告費 40,000 / PF手数料実額 80,000
  db.prepare(`INSERT INTO mart_yahoo_monthly_summary (year_month, ad_cost, pf_fee, confirmed_at)
    VALUES (?, 40000, 80000, 't')`).run(ymPrev);
  // 先々月は過去月一括取込 (/import-history) 相当: pf_fee 未計算 (=0) → 確定扱いしないこと
  db.prepare(`INSERT INTO mart_yahoo_monthly_summary (year_month, ad_cost, pf_fee, confirmed_at)
    VALUES (?, 30000, 0, 't')`).run(q.addMonths(ymNow, -2));
});
tx();

// ─── 実行 ───
let pass = 0, fail = 0;
function check(name, fn) {
  try {
    fn();
    console.log(`✓ ${name}`);
    pass++;
  } catch (e) {
    console.error(`✗ ${name}: ${e.message}`);
    fail++;
  }
}
function assert(cond, msg) { if (!cond) throw new Error(msg); }

const DAY_SALES = 10000 + 8000 + 500;   // 18500
const DAY_VM = 4400 + 2600 + 450;       // 7450
const DAY_FEE = 1000 + 800 + 50;        // 1850

check('resolvePeriod', () => {
  assert(q.resolvePeriod('last_month').from === q.monthStart(ymPrev), 'last_month from');
  assert(q.resolvePeriod('bogus').preset === '30d', 'fallback 30d');
  const c = q.resolvePeriod(null, '2026-01-01', '2026-01-31');
  assert(c.preset === 'custom' && c.from === '2026-01-01', 'custom range');
  assert(q.resolvePeriod(null, '2026-01-31', '2026-01-01').preset === '30d', '逆順は fallback');
  assert(!q.isValidDate('2026-02-30'), '存在しない日付');
  const wide = q.resolvePeriod(null, '2000-01-01', '2026-01-01');
  assert(wide.from === q.addDays('2026-01-01', -729), `巨大range clamp 両端含み730日 (got ${wide.from})`);
});

check('getTrend day→week 自動格上げ', () => {
  const r = q.getTrend(d(365), today, 'day');
  assert(r.granularity === 'week', `半年超の day は week に (got ${r.granularity})`);
});

check('getOverview', () => {
  const r = q.getOverview();
  assert(r.tiles.length === 4, 'tiles 4枚');
  assert(r.data_to === today, `data_to=${r.data_to}`);
  const tYest = r.tiles.find(t => t.key === 'yesterday');
  assert(tYest.sales_incl === DAY_SALES, `昨日売上 (got ${tYest.sales_incl})`);
  assert(tYest.units_net === 15, `昨日販売数 (got ${tYest.units_net})`);
  assert(tYest.variable_margin === DAY_VM, `昨日粗利 (got ${tYest.variable_margin})`);
  assert(tYest.use_point === 500 && tYest.coupon_shop === 300, 'ポイント/クーポン控除');
  assert(tYest.margin_pct !== null, '粗利率');
  assert(tYest.cost_coverage_pct === Math.round(18000 / 18500 * 1000) / 10, `原価カバー率 (got ${tYest.cost_coverage_pct})`);
  const tLast = r.tiles.find(t => t.key === 'last_month');
  assert(tLast.confirmed !== null && tLast.confirmed !== undefined, '先月は確定あり');
  assert(tLast.confirmed.ad_cost === 40000 && tLast.confirmed.pf_fee === 80000, '確定値');
  // 確定寄せ = VM + mall_fee(推定戻し) − pf_fee − ad_cost
  const days = tLast.days_with_data;
  const expected = DAY_VM * days + DAY_FEE * days - 80000 - 40000;
  assert(tLast.confirmed.full_margin === expected, `確定寄せ実質利益 (got ${tLast.confirmed.full_margin}, want ${expected})`);
  const tMonth = r.tiles.find(t => t.key === 'this_month');
  assert(tMonth.confirmed === null, '今月は未確定 (confirmed=null)');
  // SKU 未解決の可視化 (今月の gamma)
  assert(r.unresolved.sku_count === 1, `未解決SKU数 (got ${r.unresolved.sku_count})`);
  assert(r.unresolved.sales_incl === 500 * tMonth.days_with_data, `未解決売上 (got ${r.unresolved.sales_incl})`);
  assert(r.unresolved.share_pct !== null && r.unresolved.share_pct > 0, '未解決シェア');
});

check('getTrend day', () => {
  const r = q.getTrend(d(29), today, 'day');
  assert(r.rows.length === 30, `30日分 (got ${r.rows.length})`);
  const row = r.rows[0];
  assert(row.sales_incl === DAY_SALES, `日次売上 (got ${row.sales_incl})`);
  assert(row.margin_pct !== null, 'margin_pct');
  assert(row.avg_unit_price === Math.round(DAY_SALES / 15), `客単価 (got ${row.avg_unit_price})`);
  assert(row.use_point === 500, 'use_point');
  assert(r.confirmed_months.length === 0, '日次では confirmed なし');
});

check('getTrend month + 確定オーバーレイ', () => {
  const r = q.getTrend(d(89), today, 'month');
  assert(r.rows.length >= 3, `3ヶ月分 (got ${r.rows.length})`);
  const c = r.confirmed_months.find(x => x.year_month === ymPrev);
  assert(c, '先月の確定行あり');
  assert(c.ad_cost === 40000 && c.pf_fee === 80000, '確定値');
  const prevRow = r.rows.find(x => x.bucket === ymPrev);
  assert(c.full_margin === prevRow.variable_margin + prevRow.mall_fee_est - 80000 - 40000, 'full_margin 整合');
  // pf_fee=0 の過去月一括取込行は確定扱いしない (Codex R1 medium)
  assert(!r.confirmed_months.find(x => x.year_month === q.addMonths(ymNow, -2)),
    'pf_fee=0 の行は confirmed_months に含めない');
});

check('部分月には確定オーバーレイを載せない', () => {
  // 先月の途中から今日まで → 先月バケットは部分月なので確定を返さない (Codex R1 medium)
  const midPrev = q.monthStart(ymPrev).slice(0, 8) + '15';
  const r = q.getTrend(midPrev, today, 'month');
  assert(r.rows.length >= 2, `2ヶ月分 (got ${r.rows.length})`);
  assert(r.confirmed_months.length === 0, `部分月に確定なし (got ${JSON.stringify(r.confirmed_months)})`);
});

check('getTrend week (bucket=月曜日始まり)', () => {
  const r = q.getTrend(d(29), today, 'week');
  assert(r.rows.length >= 4 && r.rows.length <= 6, `週次バケット (got ${r.rows.length})`);
  for (const row of r.rows) {
    assert(/^\d{4}-\d{2}-\d{2}$/.test(row.bucket), `bucket は日付 (got ${row.bucket})`);
    const dow = new Date(row.bucket + 'T00:00:00Z').getUTCDay();
    assert(dow === 1, `bucket は月曜 (got dow=${dow} for ${row.bucket})`);
  }
});

check('空期間 (データなし) は 0 で返る', () => {
  const r = q.getTrend('2020-01-01', '2020-01-31', 'day');
  assert(r.rows.length === 0, 'rows 空');
  const ov = q.getOverview();  // 今日タイルはデータあり得るので単に throw しないこと
  assert(ov.tiles.length === 4, 'overview は常に4枚');
});

// ─── P2: 利益分析 ───

check('getWaterfall 全体 (L1まで、非完全月は確定なし)', () => {
  const wf = q.getWaterfall(d(1), d(1), null);
  assert(wf.confirmed_applied === false, '1日期間に確定寄せなし');
  const get = (k) => wf.steps.find(s => s.key === k).amount;
  assert(get('sales') === DAY_SALES, `売上 (got ${get('sales')})`);
  assert(get('coupon_shop') === 300 && get('use_point') === 500, 'クーポン/ポイント');
  assert(get('mall_fee') === DAY_FEE, `手数料 (got ${get('mall_fee')})`);
  assert(get('shipping') === 1400 && get('cogs') === 7000, '送料/原価');
  assert(get('variable_margin') === DAY_VM, `粗利 (got ${get('variable_margin')})`);
  // 検算: 売上 − 控除計 = 粗利
  assert(get('sales') - get('coupon_shop') - get('use_point') - get('mall_fee') - get('shipping') - get('cogs') === get('variable_margin'), 'ステップ整合');
});

check('getWaterfall 先月全体 = 確定寄せ延長', () => {
  const wf = q.getWaterfall(q.monthStart(ymPrev), q.monthEnd(ymPrev), null);
  assert(wf.confirmed_applied === true, '先月完全月は確定寄せあり');
  const get = (k) => wf.steps.find(s => s.key === k).amount;
  assert(get('pf_fee') === 80000 && get('ad_cost') === 40000, '確定実額');
  assert(get('full_margin') === get('variable_margin') + get('mall_fee_reverse') - 80000 - 40000, '実質利益整合');
});

check('getWaterfall 先々月 (pf_fee=0行) は確定扱いしない', () => {
  const ym2 = q.addMonths(ymNow, -2);
  const wf = q.getWaterfall(q.monthStart(ym2), q.monthEnd(ym2), null);
  assert(wf.confirmed_applied === false, 'pf_fee=0 の月は確定寄せなし');
});

check('getWaterfall SKU指定', () => {
  const wf = q.getWaterfall(d(1), d(1), 'ya-alpha-bk');
  const get = (k) => wf.steps.find(s => s.key === k).amount;
  assert(get('sales') === 10000 && get('variable_margin') === 4400, `alpha単体 (got ${get('sales')}/${get('variable_margin')})`);
  assert(wf.confirmed_applied === false, 'SKU指定に確定寄せなし');
});

check('getSkuProfit', () => {
  const r = q.getSkuProfit(d(6), today, {});
  assert(r.total === 3, `3 SKU (got ${r.total})`);
  const alpha = r.rows.find(x => x.yahoo_sku_key === 'ya-alpha-bk');
  assert(alpha.ne_code === 'NE-A' && alpha.resolution_method === 'sub_match', 'alpha解決情報');
  assert(alpha.units_net === 70 && alpha.sales_incl === 70000, `alpha集計 (got ${alpha.units_net}/${alpha.sales_incl})`);
  assert(alpha.variable_margin === 4400 * 7 && alpha.flag === 'ok', 'alpha粗利+flag');
  const gamma = r.rows.find(x => x.yahoo_sku_key === 'ya-gamma');
  assert(gamma.ne_code === null && gamma.flag === 'unresolved', 'gamma=未解決flag');
  assert(gamma.cost_status === 'missing_cost', 'gamma原価状態');
  // デフォルトソート = 粗利降順
  assert(r.rows[0].yahoo_sku_key === 'ya-alpha-bk', 'ソート先頭=alpha');
  // 検索フィルタ
  const f = q.getSkuProfit(d(6), today, { q: 'ne-b' });
  assert(f.total === 1 && f.rows[0].yahoo_sku_key === 'ya-beta', 'NEコード検索');
  // limit/offset
  const p = q.getSkuProfit(d(6), today, { limit: 1, offset: 1 });
  assert(p.rows.length === 1 && p.total === 3, 'ページング');
});

check('getSkuDetail', () => {
  const r = q.getSkuDetail('ya-alpha-bk', d(6), today);
  assert(r.daily.length === 7, `7日分 (got ${r.daily.length})`);
  assert(r.latest.ne_code === 'NE-A', 'latest解決');
  assert(r.master && r.master.unit_cost === 300 && r.master.list_price === 1200, 'マスタ原価/売価');
  const g = q.getSkuDetail('ya-gamma', d(6), today);
  assert(g.master === null, '未解決はマスタなし');
});

// 一時行 INSERT ヘルパ (追加テスト用)
const insTemp = (date, key, ne, variant, resolution, unresolvedFlag, vm, cogs, costStatus, costComplete, hash) =>
  db.prepare(`INSERT INTO mirror_yahoo_finance_sku_daily (
    date_jst, yahoo_sku_key, ne_code, variant_key, resolution_method, unresolved_sku_flag,
    product_name, units_ordered, units_cancelled, units_net_sold,
    sales_principal_jpy_incl, sales_postage_jpy_incl, gross_sales_jpy_incl,
    net_sales_before_point_jpy_incl, listing_sales_estimated_jpy_incl,
    coupon_shop_jpy_incl, use_point_jpy_incl, mall_fee_jpy_incl, mall_fee_calc_method,
    shipping_cost_jpy_incl, shipping_quality, unit_cost_snapshot_incl, cogs_amount_jpy_incl,
    variable_margin_partial_jpy_incl, margin_confidence, cost_status, is_cost_complete,
    data_quality_score, source_run_id, source_row_hash, synced_at
  ) VALUES (?, ?, ?, ?, ?, ?, 'テスト行', 1, 0, 1,
    900, 0, 900, 900, 900, 0, 0, 90, 'estimated_10pct', 0, 'missing', NULL, ?, ?,
    'partial', ?, ?, 40, 'smoke', ?, 't')`)
    .run(date, key, ne, variant, resolution, unresolvedFlag, cogs, vm, costStatus, costComplete, hash);

check('getUnresolved (候補サジェスト付き)', () => {
  const r = q.getUnresolved(180);
  assert(r.total === 1, `未解決1 SKU (got ${r.total})`);
  const g = r.rows[0];
  assert(g.yahoo_sku_key === 'ya-gamma' && g.variant_key === '', '登録キー=yahoo_sku_keyそのもの (variantなし)');
  assert(g.units_net > 0 && g.sales_incl > 0, '集計値');
  assert(g.candidates.length === 2, `前方一致候補2件 (got ${g.candidates.length})`);
  assert(g.candidates[0].ne_code === 'ya-gamma-01', '候補は短い順');
});

check('getUnresolved sub_codeありキー (ヒューリスティック候補)', () => {
  // 実データ仕様: sub_code あり未解決は yahoo_sku_key = variant_key = sub_code 全体
  insTemp(today, 'ya-delta-rd', null, 'ya-delta-rd', 'unresolved', 1, 810, 0, 'missing_cost', 0, 'h2');
  const r = q.getUnresolved(30);
  const dRow = r.rows.find(x => x.yahoo_sku_key === 'ya-delta-rd');
  assert(dRow && dRow.variant_key === 'ya-delta-rd', `sub_code温存 (got ${dRow?.variant_key})`);
  // 末尾セグメント '-rd' を落とした prefix 'ya-delta' で候補が拾える
  assert(dRow.candidates.some(c => c.ne_code === 'ya-delta-01'), `ヒューリスティック候補 (got ${JSON.stringify(dRow.candidates)})`);
  db.prepare(`DELETE FROM mirror_yahoo_finance_sku_daily WHERE yahoo_sku_key = 'ya-delta-rd'`).run();
});

check('getSkuProfit 期間内に未解決日があれば最新日が解決済みでも警告flag (Codex P2-R1 High)', () => {
  // epsilon: 2日前=未解決 (原価0円計上)、1日前=解決済み → 期間集計では粗利過大が混ざるため flag=unresolved
  insTemp(d(2), 'ya-eps', null, '', 'unresolved', 1, 810, 0, 'missing_cost', 0, 'h3');
  insTemp(d(1), 'ya-eps', 'NE-A', '', 'parent_match', 0, 510, 300, 'complete', 1, 'h4');
  const r = q.getSkuProfit(d(6), today, { q: 'ya-eps' });
  assert(r.total === 1, `epsilon 1件 (got ${r.total})`);
  const e = r.rows[0];
  assert(e.ne_code === 'NE-A', `最新日のne_code (got ${e.ne_code})`);
  assert(e.flag === 'unresolved', `期間内未解決あり→警告flag (got ${e.flag})`);
  assert(e.cost_status !== 'complete', '原価状態も不完全表示');
  db.prepare(`DELETE FROM mirror_yahoo_finance_sku_daily WHERE yahoo_sku_key = 'ya-eps'`).run();
});

// ─── P3: 売れ筋 + 設定 ───

check('getBestsellers ランキング/ABC/前期比', () => {
  const r = q.getBestsellers(d(6), today, 'sales');
  assert(r.total_skus === 3, `3 SKU (got ${r.total_skus})`);
  assert(r.ranking[0].yahoo_sku_key === 'ya-alpha-bk', '売上軸の先頭=alpha');
  // ABC は加算前累積比で判定 (Codex P3-R1 medium): 境界をまたぐSKUはA群に含める。
  // alpha(累積0%開始)=A, beta(54%開始<80)=A, gamma(97%開始)=C。先頭SKUは構成比がどれだけ大きくても必ずA
  assert(r.ranking[0].abc === 'A', `先頭SKUは必ずA群 (got ${r.ranking[0].abc})`);
  const beta = r.ranking.find(x => x.yahoo_sku_key === 'ya-beta');
  const gammaR = r.ranking.find(x => x.yahoo_sku_key === 'ya-gamma');
  assert(beta.abc === 'A' && gammaR.abc === 'C', `beta=A/gamma=C (got ${beta.abc}/${gammaR.abc})`);
  assert(r.abc.count.A + r.abc.count.B + r.abc.count.C === 3, 'ABC合計');
  assert(r.abc.total_revenue === (10000 + 8000 + 500) * 7, `売上合計 (got ${r.abc.total_revenue})`);
  // 前期 (同量) → 成長率0%
  const alpha = r.ranking.find(x => x.yahoo_sku_key === 'ya-alpha-bk');
  assert(alpha.units_growth_pct === 0, `前期同量→0% (got ${alpha.units_growth_pct})`);
  assert(alpha.is_new === false, '90日前初売上→NEWでない');
  assert(r.risers.length === 0 && r.fallers.length === 0, '同量なので急上昇/急落なし');
  // 軸切替
  const u = q.getBestsellers(d(6), today, 'units');
  assert(u.axis === 'units_net' && u.ranking[0].yahoo_sku_key === 'ya-alpha-bk', '数量軸');
  const m = q.getBestsellers(d(6), today, 'margin');
  assert(m.axis === 'variable_margin', '粗利軸');
});

check('getBestsellers 曜日パターン + 5のつく日 + スパークライン', () => {
  const r = q.getBestsellers(d(6), today, 'sales');
  assert(r.weekday.length === 7, `曜日7種 (got ${r.weekday.length})`);
  for (const w of r.weekday) assert(w.avg_units === 15, `全日15個/日 (got ${w.avg_units})`);
  const spark = r.ranking[0].spark;
  assert(Array.isArray(spark) && spark.length === 7, `スパークライン7日分 (got ${spark?.length})`);
  // 30日窓なら 5/15/25 が必ず含まれ、全日同量なので lift=0%
  const g30 = q.getBestsellers(d(29), today, 'sales').goen;
  assert(g30.goen_days >= 2, `5のつく日 (got ${g30.goen_days})`);
  assert(g30.lift_pct === 0, `同量→lift 0% (got ${g30.lift_pct})`);
});

check('settings 既定値 + 保存 + バリデーション (Codex P3-R1 medium)', () => {
  const s0 = q.getSettings();
  assert(s0.abc_a_pct === 80 && s0.movers_min_units === 5, '既定値');
  const s1 = q.saveSettings({ abc_a_pct: 70, bogus_key: 1 });
  assert(s1.abc_a_pct === 70, '保存反映');
  assert(!('bogus_key' in s1), '不正キー無視');
  // 範囲外・非数値・A>=B は 400 で reject (部分適用しない)
  let threw = 0;
  try { q.saveSettings({ new_product_days: 'abc' }); } catch (e) { if (e.status === 400) threw++; }
  try { q.saveSettings({ abc_a_pct: 200 }); } catch (e) { if (e.status === 400) threw++; }
  try { q.saveSettings({ movers_min_units: -5 }); } catch (e) { if (e.status === 400) threw++; }
  try { q.saveSettings({ abc_a_pct: 96, abc_b_pct: 95 }); } catch (e) { if (e.status === 400) threw++; }
  assert(threw === 4, `バリデーション4件 (got ${threw})`);
  assert(q.getSettings().abc_a_pct === 70, 'reject時は部分適用なし');
  q.saveSettings({ abc_a_pct: 80 });
});

check('料率マスタ CRUD + 改定日ロジック', () => {
  const r0 = q.getRates();
  assert(Object.keys(r0.current).length === 0 && r0.history.length === 0, '初期は空');
  // 過去日から有効な料率
  q.addRate({ rate_key: 'promo_package', rate_pct: 3, effective_from: '2025-01-01', memo: '加入時' });
  // 未来日の改定予約 (2026-09改定)
  const r1 = q.addRate({ rate_key: 'promo_package', rate_pct: 2, effective_from: '2099-09-01', memo: '改定予約' });
  assert(r1.current.promo_package.rate_pct === 3, `現在有効=3% (got ${r1.current.promo_package?.rate_pct})`);
  assert(r1.upcoming.length === 1 && r1.upcoming[0].rate_pct === 2, '未来日は改定予約');
  // 後から追加した「より新しい過去日」が現在有効になる
  const r2 = q.addRate({ rate_key: 'promo_package', rate_pct: 2.5, effective_from: '2025-06-01', memo: '' });
  assert(r2.current.promo_package.rate_pct === 2.5, `改定日順で最新 (got ${r2.current.promo_package.rate_pct})`);
  // 削除で1つ前に戻る
  const r3 = q.deleteRate(r2.current.promo_package.id);
  assert(r3.current.promo_package.rate_pct === 3, `削除で前の料率に (got ${r3.current.promo_package.rate_pct})`);
  // バリデーション
  let threw = 0;
  try { q.addRate({ rate_key: 'bogus', rate_pct: 1, effective_from: '2025-01-01' }); } catch { threw++; }
  try { q.addRate({ rate_key: 'point_base', rate_pct: 101, effective_from: '2025-01-01' }); } catch { threw++; }
  try { q.addRate({ rate_key: 'point_base', rate_pct: 1, effective_from: '2025-13-01' }); } catch { threw++; }
  try { q.deleteRate(999999); } catch { threw++; }
  assert(threw === 4, `バリデーション4件 (got ${threw})`);
});

// ─── P6: 統計統合 + インサイト基盤 ───

check('統計統合: trafficBySku / bestsellersのPV/CVR / SkuDetailのtraffic_daily / 検索KW / flash', () => {
  const t = q.trafficBySku(d(6), today, );
  const a = t.get('ya-alpha-bk');
  assert(a && a.pv === 200 * 6 || a.pv === 200 * 7, `alpha PV (got ${a?.pv})`);   // d(1)〜d(6)=6日 or 7日分 (当日行なし)
  assert(a.cvr_pct === 9, `alpha CVR 9% (got ${a.cvr_pct})`);
  const bs = q.getBestsellers(d(6), today, 'sales');
  const alpha = bs.ranking.find(x => x.yahoo_sku_key === 'ya-alpha-bk');
  assert(alpha.pv > 0 && alpha.cvr_pct === 9, `売れ筋にPV/CVR (got ${alpha.pv}/${alpha.cvr_pct})`);
  const det = q.getSkuDetail('ya-alpha-bk', d(6), today);
  assert(det.traffic_daily.length === 6, `traffic_daily 6日 (got ${det.traffic_daily.length})`);
  const kw = q.getSearchKeywords(d(6), d(1));
  assert(kw.rows.length === 2 && kw.rows[0].keyword === '精油', `KW流入順 (got ${kw.rows[0]?.keyword})`);
  const fl = q.getFlashLatest();
  assert(fl.date === null || Array.isArray(fl.rows), 'flashは空でも壊れない');
  const acq = q.getAcquisition(d(6), today);
  assert(Array.isArray(acq.device) && Array.isArray(acq.user_attr), 'acquisition形');
});

check('insights: 検出 (赤字/未解決/PV空振り/KW急減) + evidence契約', () => {
  // 赤字SKU fixture: 7日×売上900円 (計6,300>=5,000) × 粗利-2,000/日 (計-14,000 = high)
  for (let i = 1; i <= 7; i++) insTemp(d(i), 'ya-loss', 'NE-B', '', 'parent_match', 0, -2000, 2000, 'complete', 1, `L1-${i}`);
  const r = ins.runInsights('manual');
  assert(r.run_id > 0 && r.found > 0, `検出あり (got ${r.found})`);
  const list = r.insights;
  const loss = list.find(x => x.rule_key === 'loss_sku' && x.target_key === 'ya-loss');
  assert(loss && loss.severity === 'high', `赤字SKU high (got ${loss?.severity})`);
  assert(loss.evidence.observed_at && loss.evidence.run_id === r.run_id && loss.evidence.period, 'evidence契約 (observed_at/run_id/period)');
  assert(loss.suggested_action.action_type && loss.suggested_action.requires_approval === true, 'suggested_action構造化');
  assert(list.find(x => x.rule_key === 'unresolved_sku' && x.target_key === 'ya-gamma'), '未解決SKU検出');
  const zeta = list.find(x => x.rule_key === 'high_pv_low_cvr' && x.target_key === 'ya-zeta');
  assert(zeta, 'PVあるのに売れない検出');
  assert(list.find(x => x.rule_key === 'kw_inflow_drop' && x.target_key === 'アロマ'), 'KW流入急減検出');
});

check('insights: ライフサイクル (解消→再発reopen / dismissed永続 / severity昇格でreopen)', () => {
  // 解消: ya-loss の赤字行を消して再実行 → resolved
  db.prepare(`DELETE FROM mirror_yahoo_finance_sku_daily WHERE yahoo_sku_key = 'ya-loss'`).run();
  ins.runInsights('manual');
  const db2 = getMirrorDB();
  const occ = db2.prepare(`SELECT * FROM yadash_insight_occurrences WHERE rule_key = 'loss_sku' AND target_key = 'ya-loss'`).get();
  assert(occ.resolved_at !== null, '解消でresolved_at');
  // 再発 (warn級: 計-5,600 > -10,000) → reopen (open のまま)
  for (let i = 1; i <= 7; i++) insTemp(d(i), 'ya-loss', 'NE-B', '', 'parent_match', 0, -800, 800, 'complete', 1, `L2-${i}`);
  ins.runInsights('manual');
  const occ2 = db2.prepare(`SELECT * FROM yadash_insight_occurrences WHERE target_key = 'ya-loss'`).get();
  assert(occ2.resolved_at === null && occ2.status === 'open' && occ2.severity === 'warn', `再発でreopen (got ${occ2.status}/${occ2.severity})`);
  // dismissed → 再実行しても dismissed のまま (severity同じ)
  ins.setInsightStatus(occ2.id, 'dismissed', 'ロスリーダー');
  ins.runInsights('manual');
  const occ3 = db2.prepare(`SELECT * FROM yadash_insight_occurrences WHERE id = ?`).get(occ2.id);
  assert(occ3.status === 'dismissed', `dismissed永続 (got ${occ3.status})`);
  // severity昇格 (high級: 計-14,000) → reopen
  db2.prepare(`DELETE FROM mirror_yahoo_finance_sku_daily WHERE yahoo_sku_key = 'ya-loss'`).run();
  for (let i = 1; i <= 7; i++) insTemp(d(i), 'ya-loss', 'NE-B', '', 'parent_match', 0, -2000, 2000, 'complete', 1, `L3-${i}`);
  ins.runInsights('manual');
  const occ4 = db2.prepare(`SELECT * FROM yadash_insight_occurrences WHERE id = ?`).get(occ2.id);
  assert(occ4.status === 'open' && occ4.severity === 'high', `昇格でreopen (got ${occ4.status}/${occ4.severity})`);
  // イベント履歴が遷移分だけ残る
  const events = db2.prepare(`SELECT event_type FROM yadash_insight_events WHERE occurrence_id = ? ORDER BY id`).all(occ2.id).map(e => e.event_type);
  assert(events.includes('detected') || events.includes('reopened'), 'イベント履歴');
  assert(events.filter(e => e === 'resolved').length >= 1 && events.filter(e => e === 'status_changed').length >= 1, 'resolved/status_changedイベント');
  db2.prepare(`DELETE FROM mirror_yahoo_finance_sku_daily WHERE yahoo_sku_key = 'ya-loss'`).run();
});

check('insights: status API バリデーション + listフィルタ', () => {
  let threw = 0;
  try { ins.setInsightStatus(999999, 'done', ''); } catch (e) { if (e.status === 400) threw++; }
  try { ins.setInsightStatus(1, 'bogus', ''); } catch (e) { if (e.status === 400) threw++; }
  assert(threw === 2, `バリデーション2件 (got ${threw})`);
  const open = ins.listInsights({ status: 'open' });
  assert(open.insights.every(x => x.status === 'open'), 'statusフィルタ');
  assert(open.rules_version === ins.RULES_VERSION && open.rule_labels, 'AI契約メタ');
});

console.log(`\n=== smoke: ${pass} PASS / ${fail} FAIL ===`);
process.exit(fail > 0 ? 1 : 0);
