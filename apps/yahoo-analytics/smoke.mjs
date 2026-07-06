// yahoo-analytics 機能スモーク: fixture 投入 → 全 query 関数を実行して形を検証
// 使い方: DATA_DIR=<空ディレクトリ> node apps/yahoo-analytics/smoke.mjs
// ⚠️ DATA_DIR 内の warehouse-mirror.db の対象テーブルを DELETE するため本番 DATA_DIR で実行禁止
import fs from 'fs';
import path from 'path';
import { initMirrorDB, getMirrorDB } from '../warehouse-mirror/db.js';
import * as q from './queries.js';

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
  for (const t of ['mirror_yahoo_finance_sku_daily', 'mart_yahoo_monthly_summary', 'mirror_products']) {
    try { db.exec(`DELETE FROM ${t}`); } catch {}
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

console.log(`\n=== smoke: ${pass} PASS / ${fail} FAIL ===`);
process.exit(fail > 0 ? 1 : 0);
