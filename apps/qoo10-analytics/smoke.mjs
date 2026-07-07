// qoo10-analytics 機能スモーク: fixture 投入 → 全 query 関数を実行して形を検証
// 使い方: DATA_DIR=<空ディレクトリ> node apps/qoo10-analytics/smoke.mjs
// ⚠️ DATA_DIR 内の warehouse-mirror.db の対象テーブルを DELETE するため本番 DATA_DIR で実行禁止
import fs from 'fs';
import path from 'path';
import { initMirrorDB, getMirrorDB } from '../warehouse-mirror/db.js';
import * as q from './queries.js';

// 本番 DB 誤実行ガード 1: DATA_DIR 未指定 (= cwd の本番 DB に向く) なら即中断
if (!process.env.DATA_DIR) {
  console.error('FATAL: DATA_DIR が未指定です。専用の空ディレクトリを指定してください (例: DATA_DIR=c:/tmp/qa-smoke-data)');
  process.exit(2);
}

// 本番 DB 誤実行ガード 2: marker file 方式。
// この smoke が過去に作った DB (marker あり) 以外の既存 warehouse-mirror.db は DELETE しない
const markerPath = path.join(process.env.DATA_DIR, '.qoo10-analytics-smoke-db');
const dbExisted = fs.existsSync(path.join(process.env.DATA_DIR, 'warehouse-mirror.db'));
if (dbExisted && !fs.existsSync(markerPath)) {
  console.error('FATAL: DATA_DIR に既存の warehouse-mirror.db があります (smoke 生成マーカーなし)。実 DB の可能性があるため中断します');
  process.exit(2);
}

initMirrorDB();
const db = getMirrorDB();
fs.writeFileSync(markerPath, `created by apps/qoo10-analytics/smoke.mjs at ${new Date().toISOString()}\n`);

// 本番 DB 誤実行ガード 3: fixture 規模を超えるデータがあれば実 DB とみなし中断
const factGuard = db.prepare(`SELECT COUNT(*) AS c FROM mirror_qoo10_finance_sku_daily`).get().c;
const flashGuard = db.prepare(`SELECT COUNT(*) AS c FROM mirror_f_sales_by_listing WHERE mall = 'qoo10'`).get().c;
if (factGuard > 600 || flashGuard > 300) {
  console.error(`FATAL: 実データらしき規模を検出 (fact ${factGuard} 行 / flash ${flashGuard} 行)。本番 DB と思われるため中断します`);
  process.exit(2);
}

const today = q.jstToday();
const d = (n) => q.addDays(today, -n);
const ymNow = today.slice(0, 7);

// ─── fixture ───
// メガ割開催日 = i % 10 === 0 の日 (today 含む)。判定は fact の megawari_order_count > 0 由来
const isMegaDay = (i) => i % 10 === 0;

q.ensureAppTables();
const tx = db.transaction(() => {
  db.exec(`DELETE FROM mirror_qoo10_finance_sku_daily`);
  db.exec(`DELETE FROM mirror_f_sales_by_listing WHERE mall = 'qoo10'`);
  // アプリ専用テーブルも初期化 (前回 smoke 実行の設定・開催回が残ると検証が汚れる)
  db.exec(`DELETE FROM qooda_settings`);
  db.exec(`DELETE FROM qooda_mega_events`);

  const insFact = db.prepare(`INSERT INTO mirror_qoo10_finance_sku_daily (
    date_jst, sku_code, ne_code, resolution_method, match_tier, unresolved_sku_flag, product_name,
    units_ordered, units_cancelled, units_net_sold,
    gmv_list_price_jpy_incl, customer_paid_jpy_incl, net_settlement_api_jpy_incl,
    platform_fee_jpy_incl,
    megawari_order_count, megawari_discount_amount_jpy_incl,
    megapo_order_count, megapo_discount_amount_jpy_incl,
    other_promo_order_count, other_promo_discount_jpy_incl, total_platform_promo_jpy_incl,
    shipping_cost_jpy_incl, shipping_quality,
    unit_cost_snapshot_incl, cogs_amount_jpy_incl,
    variable_margin_jpy_incl,
    cost_status, is_cost_complete, data_quality_score,
    order_count, line_count,
    source_run_id, source_row_hash, synced_at
  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'smoke', 'h', 't')`);

  const insFlash = db.prepare(`INSERT INTO mirror_f_sales_by_listing (
    date_jst, month_ym, mall, item_code, channel, item_name, units, sales_jpy_incl, order_count,
    data_source, source_updated_at, source_run_id, source_row_hash, synced_at
  ) VALUES (?, ?, 'qoo10', ?, '', ?, ?, ?, ?, 'ne', 't', 'smoke', 'h', 't')`);

  for (let i = 0; i < 90; i++) {
    const date = d(i);
    {
      // alpha (combined): 10個/日 定価1000。手数料10% (settle=9000)
      const gmv = 10000, settle = 9000, cogs = 3000, shipping = 800;
      insFact.run(date, 'q-alpha-pi', 'NE-A', 'master_match', 'combined', 0, 'アルファ精油PI',
        10, 10, gmv, gmv, settle, gmv - settle,
        0, 0, 0, 0, 0, 0, 0,
        shipping, 'estimated_rates', 300, cogs,
        settle - cogs - shipping, // 5200
        'complete', 1, 100, 5, 10);
    }
    {
      // beta (seller_only): 4個/日 定価2000。手数料9% (settle=7280) — 9%/10% 混在の実測を再現
      const gmv = 8000, settle = 7280, cogs = 4000, shipping = 600;
      insFact.run(date, 'q-beta', 'NE-B', 'master_match', 'seller_only', 0, 'ベータ茶葉',
        4, 4, gmv, gmv, settle, gmv - settle,
        0, 0, 0, 0, 0, 0, 0,
        shipping, 'estimated_rates', 1000, cogs,
        settle - cogs - shipping, // 2680
        'complete', 1, 100, 2, 4);
    }
    {
      // gamma (unresolved): 1個/日 定価500。原価0円計上 = 粗利過大の典型
      const gmv = 500, settle = 450;
      insFact.run(date, '__UNRESOLVED__:q-gamma', null, 'unresolved', 'unresolved', 1, 'ガンマ雑貨',
        1, 1, gmv, gmv, settle, gmv - settle,
        0, 0, 0, 0, 0, 0, 0,
        0, 'missing', null, 0,
        settle, // 450
        'missing_cost', 0, 40, 1, 1);
    }
    if (isMegaDay(i)) {
      // mega (メガ割対象): 2個/日 定価2500。20%割引 (discount=1000、Qoo10負担含む全体)
      const gmv = 5000, settle = 4500, paid = 4000, cogs = 1500, shipping = 300;
      insFact.run(date, 'q-mega', 'NE-M', 'master_match', 'combined', 0, 'メガ割対象品',
        2, 2, gmv, paid, settle, gmv - settle,
        1, 1000, 0, 0, 0, 0, 1000,
        shipping, 'estimated_rates', 750, cogs,
        settle - cogs - shipping, // 2700
        'complete', 1, 100, 1, 2);
    }
    if (i >= 35 && i <= 40) {
      // squeeze (プロモ赤字転落の典型): L1 は黒字 (+100/日) だがメガ割負担推定 (400×50%=200/日) で
      // L2 赤字。d(40)〜d(35) の連続6日間 = 開催回逆推定 (suggestions) のテストも兼ねる。
      // タイル検証 (今日/昨日/今月unresolved) に影響しない日付帯に置く
      const gmv = 2000, settle = 1800, paid = 1600, cogs = 1500, shipping = 200;
      insFact.run(date, 'q-squeeze', 'NE-S', 'master_match', 'combined', 0, '負担転落品',
        1, 1, gmv, paid, settle, gmv - settle,
        1, 400, 0, 0, 0, 0, 400,
        shipping, 'estimated_rates', 1500, cogs,
        settle - cogs - shipping, // 100
        'complete', 1, 100, 1, 1);
    }
    // 速報 (NE受注): 全日 20,000円/16個/6注文 (メガ日は +4,000/2個/1注文)
    const mega = isMegaDay(i);
    insFlash.run(date, q.monthOf(date), 'q-listing-1', 'Qoo10出品まとめ',
      16 + (mega ? 2 : 0), 20000 + (mega ? 4000 : 0), 6 + (mega ? 1 : 0));
  }

  // flash-only の日 (factより速報が先行するケース): 対象期間外の孤立バケット
  insFlash.run('2020-06-15', '2020-06', 'q-old', '古い速報のみ行', 3, 3000, 2);
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

// 非メガ日の日次値
const DAY_GMV = 10000 + 8000 + 500;            // 18500
const DAY_PAID = DAY_GMV;                       // 18500
const DAY_SETTLE = 9000 + 7280 + 450;           // 16730
const DAY_FEE = 1000 + 720 + 50;                // 1770
const DAY_VM = 5200 + 2680 + 450;               // 8330
const DAY_FLASH = 20000;

check('resolvePeriod', () => {
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

check('getOverview タイル (昨日 = 非メガ日)', () => {
  const r = q.getOverview();
  assert(r.tiles.length === 4, 'tiles 4枚');
  assert(r.fact_data_to === today, `fact_data_to=${r.fact_data_to}`);
  assert(r.flash_data_to === today, `flash_data_to=${r.flash_data_to}`);
  const t = r.tiles.find(x => x.key === 'yesterday'); // i=1 は非メガ日
  assert(t.flash.sales_incl === DAY_FLASH, `速報売上 (got ${t.flash.sales_incl})`);
  assert(t.flash.units === 16 && t.flash.orders === 6, '速報 units/orders');
  assert(t.fact.gmv === DAY_GMV, `確定GMV (got ${t.fact.gmv})`);
  assert(t.fact.platform_fee === DAY_FEE, `手数料実額 (got ${t.fact.platform_fee})`);
  assert(t.fact.fee_pct === Math.round(DAY_FEE / DAY_GMV * 1000) / 10, `手数料率実測 (got ${t.fact.fee_pct})`);
  assert(t.fact.variable_margin === DAY_VM, `粗利L1 (got ${t.fact.variable_margin})`);
  assert(t.fact.margin_pct === Math.round(DAY_VM / DAY_SETTLE * 1000) / 10, `粗利率 分母=受取額 (got ${t.fact.margin_pct})`);
  assert(t.fact.promo_total === 0 && t.fact.burden_est === 0, '非メガ日は負担0');
  assert(t.fact.l2_ref === DAY_VM, 'L2参考 = L1 (負担0)');
  assert(t.pending_est === DAY_FLASH - DAY_PAID, `確定待ち目安 (got ${t.pending_est})`);
  assert(t.fact.cost_coverage_pct === Math.round(18000 / 18500 * 1000) / 10, `原価カバー率 (got ${t.fact.cost_coverage_pct})`);
});

check('getOverview メガ日タイル (今日 = i=0 メガ日)', () => {
  const r = q.getOverview();
  const t = r.tiles.find(x => x.key === 'today');
  assert(t.fact.megawari_discount === 1000, `メガ割割引 (got ${t.fact.megawari_discount})`);
  assert(t.fact.promo_total === 1000, 'promo_total');
  assert(t.fact.burden_est === 500, `セラー負担推定50% (got ${t.fact.burden_est})`);
  const vmToday = DAY_VM + 2700;
  assert(t.fact.variable_margin === vmToday, `メガ日L1 (got ${t.fact.variable_margin})`);
  assert(t.fact.l2_ref === vmToday - 500, `L2参考 = L1 − 負担推定 (got ${t.fact.l2_ref})`);
  // メガ日: flash 24000 − paid (18500 + 4000) = 1500
  assert(t.pending_est === 24000 - (DAY_PAID + 4000), `メガ日 確定待ち目安 (got ${t.pending_est})`);
  assert(r.burden_rates.megawari_pct === 50 && r.burden_rates.megapo_pct === 50, 'burden_rates 公開');
});

check('SKU未解決の可視化 (今月)', () => {
  const r = q.getOverview();
  assert(r.unresolved.sku_count === 1, `未解決SKU数 (got ${r.unresolved.sku_count})`);
  const tMonth = r.tiles.find(t => t.key === 'this_month');
  assert(r.unresolved.gmv === 500 * tMonth.fact.days_with_data, `未解決GMV (got ${r.unresolved.gmv})`);
  assert(r.unresolved.share_pct !== null && r.unresolved.share_pct > 0, '未解決シェア');
});

check('getTrend day (fact + flash 合流 + メガ判定)', () => {
  const r = q.getTrend(d(29), today, 'day');
  assert(r.rows.length === 30, `30日分 (got ${r.rows.length})`);
  const normal = r.rows.find(x => x.bucket === d(1));
  assert(normal.gmv === DAY_GMV, `日次確定GMV (got ${normal.gmv})`);
  assert(normal.flash_sales === DAY_FLASH, `日次速報 (got ${normal.flash_sales})`);
  assert(normal.is_megawari === false, '非メガ日フラグ');
  assert(normal.margin_pct !== null, 'margin_pct');
  assert(normal.avg_unit_price === Math.round(DAY_PAID / 15), `客単価 (got ${normal.avg_unit_price})`);
  const megaRows = r.rows.filter(x => x.is_megawari);
  assert(megaRows.length === 3, `30日中メガ日3日 (got ${megaRows.length})`);
  assert(megaRows[0].megawari_discount === 1000 && megaRows[0].burden_est === 500, 'メガ日の割引/負担');
});

check('getTrend flash-only バケットも落とさない', () => {
  const r = q.getTrend('2020-06-01', '2020-06-30', 'day');
  assert(r.rows.length === 1, `flash-only 1件 (got ${r.rows.length})`);
  assert(r.rows[0].gmv === 0 && r.rows[0].flash_sales === 3000, 'fact 0 / flash 3000');
  assert(r.rows[0].is_megawari === false && r.rows[0].margin_pct === null, 'fact 無しの派生値は null/false');
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

check('getTrend month', () => {
  const r = q.getTrend(d(89), today, 'month');
  assert(r.rows.length >= 3, `3ヶ月分 (got ${r.rows.length})`);
  const cur = r.rows.find(x => x.bucket === ymNow);
  assert(cur && cur.gmv > 0 && cur.flash_sales > 0, '当月バケットに fact+flash');
  assert(cur.is_megawari === true, '当月はメガ日を含む');
});

check('空期間 (データなし) は空/0 で返る', () => {
  const r = q.getTrend('2019-01-01', '2019-01-31', 'day');
  assert(r.rows.length === 0, 'rows 空');
  const ov = q.getOverview();
  assert(ov.tiles.length === 4, 'overview は常に4枚');
});

// ═══ P3: 設定 / 利益分析 / 売れ筋 / 開催回マスタ ═══

check('設定 保存・検証・APIキー期限', () => {
  const s0 = q.getSettings();
  assert(s0.burden_megawari_pct === 50 && s0.abc_a_pct === 80, 'デフォルト値');
  assert(s0.cert_key_expires_on === null, '発行日未登録なら期限null');
  const s1 = q.saveSettings({ burden_megawari_pct: 40, cert_key_issued_on: '2026-07-01' });
  assert(s1.burden_megawari_pct === 40, '負担率更新');
  assert(s1.cert_key_expires_on === '2027-07-01', `期限=発行+365日 (got ${s1.cert_key_expires_on})`);
  assert(typeof s1.cert_key_days_left === 'number', '残日数');
  let threw = false;
  try { q.saveSettings({ abc_a_pct: 99 }); } catch (e) { threw = e.status === 400; }
  assert(threw, 'abc_a >= abc_b は 400');
  threw = false;
  try { q.saveSettings({ cert_key_issued_on: 'bogus' }); } catch (e) { threw = e.status === 400; }
  assert(threw, '不正日付は 400');
  threw = false;
  try { q.saveSettings({ burden_megawari_pct: 150 }); } catch (e) { threw = e.status === 400; }
  assert(threw, '範囲外は 400');
  q.saveSettings({ burden_megawari_pct: 50, cert_key_issued_on: '' }); // 以降の検証のため復元
  assert(q.getSettings().burden_megawari_pct === 50, '復元');
});

check('getWaterfall (昨日 = 非メガ日)', () => {
  const w = q.getWaterfall(d(1), d(1), null);
  const step = (k) => w.steps.find(s => s.key === k);
  assert(step('gmv').amount === DAY_GMV, `GMV (got ${step('gmv').amount})`);
  assert(step('platform_fee').amount === DAY_FEE, '手数料実額');
  assert(step('net_settlement').amount === DAY_SETTLE, '受取');
  assert(step('variable_margin').amount === DAY_VM, 'L1');
  assert(step('burden_megawari').amount === 0 && step('l2').amount === DAY_VM, '非メガ日はL2=L1');
  // SKU 指定
  const wa = q.getWaterfall(d(1), d(1), 'q-alpha-pi');
  assert(wa.steps.find(s => s.key === 'gmv').amount === 10000, 'SKU絞り込み');
});

check('getSkuProfit (flag/fee_pct/検索/ソート)', () => {
  const r = q.getSkuProfit(d(29), today, {});
  assert(r.total === 4, `30日窓は4 SKU (got ${r.total})`);
  const alpha = r.rows.find(x => x.sku_code === 'q-alpha-pi');
  assert(alpha.fee_pct === 10, `alpha手数料10% (got ${alpha.fee_pct})`);
  assert(alpha.ne_code === 'NE-A' && alpha.match_tier === 'combined', 'latest_key 分解');
  const beta = r.rows.find(x => x.sku_code === 'q-beta');
  assert(beta.fee_pct === 9, `beta手数料9% (got ${beta.fee_pct})`);
  const gamma = r.rows.find(x => x.sku_code.startsWith('__UNRESOLVED__'));
  assert(gamma.flag === 'unresolved', '未解決flag');
  const mega = r.rows.find(x => x.sku_code === 'q-mega');
  assert(mega.burden_est === Math.round(1000 * 3 * 0.5), `メガ負担 (got ${mega.burden_est})`);
  assert(mega.mega_dependency_pct === 100, 'メガ日売上比100%');
  // 検索
  const rq = q.getSkuProfit(d(29), today, { q: 'q-alpha' });
  assert(rq.total === 1, '検索1件');
  // ソート (fee_pct asc → beta 先頭)
  const rs = q.getSkuProfit(d(29), today, { sort: 'fee_pct', dir: 'asc' });
  assert(rs.rows[0].sku_code === 'q-beta', `fee_pct asc 先頭=beta (got ${rs.rows[0].sku_code})`);
});

check('promo_squeeze flag (L1黒字→負担後赤字)', () => {
  const r = q.getSkuProfit(d(40), d(35), {});
  const sq = r.rows.find(x => x.sku_code === 'q-squeeze');
  assert(sq, 'q-squeeze 行あり');
  assert(sq.variable_margin === 600 && sq.burden_est === 1200 && sq.l2 === -600, `L1=600/負担1200/L2=-600 (got ${sq.variable_margin}/${sq.burden_est}/${sq.l2})`);
  assert(sq.flag === 'promo_squeeze', `flag (got ${sq.flag})`);
});

check('getSkuDetail', () => {
  const dd = q.getSkuDetail('q-mega', d(29), today);
  assert(dd.daily.length === 3, `30日窓のメガ日3日分 (got ${dd.daily.length})`);
  assert(dd.daily[0].is_mega === true && dd.daily[0].burden_est === 500, 'メガ日行の負担');
  assert(dd.latest.ne_code === 'NE-M', 'latest');
});

check('getBestsellers (ABC加算前判定/リフト/新商品)', () => {
  const b = q.getBestsellers(d(29), today, 'sales');
  assert(b.total_skus === 4, `4 SKU (got ${b.total_skus})`);
  // 先頭 (最大GMV=alpha 30万) は必ず A (加算前判定 — 1SKU集中でもA群が0にならない)
  const top = b.ranking[0];
  assert(top.sku_code === 'q-alpha-pi' && top.abc === 'A', `先頭はalpha/A (got ${top.sku_code}/${top.abc})`);
  assert(b.abc.count.A >= 1, 'A群1以上');
  assert(b.mega.mega_days === 3 && b.mega.normal_days === 27, `メガ日3/通常27 (got ${b.mega.mega_days}/${b.mega.normal_days})`);
  assert(b.mega.lift_pct !== null && b.mega.lift_pct > 0, 'メガ日リフト正 (メガ日はgmv+5000)');
  assert(b.ranking[0].spark.length > 0, 'スパークラインあり');
  assert(b.weekday.length === 7, '曜日7行');
  // 全SKUが全期間に売れている fixture なので growth はほぼ0%・新商品なし
  assert(top.units_growth_pct === 0, `前期比0% (got ${top.units_growth_pct})`);
  assert(top.is_new === false, '新商品バッジなし');
});

check('開催回マスタ CRUD + fact逆推定', () => {
  const g0 = q.getMegaEvents();
  assert(g0.events.length === 0, '初期は0件');
  // d(40)〜d(35) の連続6日 run が候補に出る (q-squeeze 帯)
  const run = g0.suggestions.find(s => s.kind === 'megawari' && s.days >= 6);
  assert(run, `6日連続runの候補 (got ${JSON.stringify(g0.suggestions.slice(0, 3))})`);
  assert(run.date_from === d(40) && run.date_to === d(35), `run期間 (got ${run.date_from}〜${run.date_to})`);
  // 登録 → 候補から消える
  const added = q.addMegaEvent({ kind: 'megawari', label: 'テスト回', date_from: d(40), date_to: d(35), participated: 1 });
  assert(added.events.length === 1, '登録1件');
  assert(!added.suggestions.find(s => s.kind === 'megawari' && s.date_from === d(40)), '登録済み期間は候補から消える');
  // 重複期間は 400
  let threw = false;
  try { q.addMegaEvent({ kind: 'megawari', date_from: d(38), date_to: d(36) }); } catch (e) { threw = e.status === 400; }
  assert(threw, '期間重複は 400');
  // 別種別 (megapo) なら重複可
  const mp = q.addMegaEvent({ kind: 'megapo', date_from: d(38), date_to: d(36) });
  assert(mp.events.length === 2, 'megapo は登録可');
  // 削除
  const afterDel = q.deleteMegaEvent(added.id);
  assert(afterDel.events.length === 1, '削除後1件');
  threw = false;
  try { q.deleteMegaEvent(99999); } catch (e) { threw = e.status === 400; }
  assert(threw, '存在しないidは 400');
  q.deleteMegaEvent(mp.id);
});

console.log(`\n=== smoke: ${pass} PASS / ${fail} FAIL ===`);
process.exit(fail > 0 ? 1 : 0);
