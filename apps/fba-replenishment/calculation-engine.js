/**
 * FBA在庫補充 計算エンジン
 *
 * 入力: SP-APIスナップショット + SKUマッピング + 倉庫在庫 + 設定
 * 出力: SKUごとの推奨納品数 + 緊急度スコア + アラート
 */
import { getLatestSnapshots, getSkuMappings, getSkuExceptions, getSettings,
         getWarehouseSummary, getDailySnapshots } from './db.js';

/**
 * 推奨リストを生成
 */
export function generateRecommendations() {
  const settings = getSettings();
  const snapshots = getLatestSnapshots();
  const mappings = getSkuMappings();
  const exceptions = getSkuExceptions();
  const warehouseSummary = getWarehouseSummary();

  if (!snapshots.length) return { items: [], errors: ['スナップショットがありません。SP-APIレポートを取得してください。'] };
  if (!mappings.length) return { items: [], errors: ['SKUマッピングがありません。スプシ同期してください。'] };

  // ルックアップ用マップ
  const mappingMap = {};
  for (const m of mappings) mappingMap[m.amazon_sku] = m;

  const exceptionMap = {};
  for (const e of exceptions) exceptionMap[e.amazon_sku] = e;

  const warehouseMap = {};
  for (const w of warehouseSummary) warehouseMap[w.logizard_code] = w;

  const snapshotDate = snapshots[0]?.snapshot_date;
  const workingExpiryDays = parseInt(settings.working_expiry_days || 7);

  const items = [];

  for (const snap of snapshots) {
    const sku = snap.amazon_sku;
    const mapping = mappingMap[sku];
    if (!mapping) continue; // マッピングがないSKUはスキップ

    // --- 実質FBA在庫 ---
    const fbaAvailable = snap.fba_available || 0;
    const inboundShipped = snap.fba_inbound_shipped || 0;
    const inboundReceived = snap.fba_inbound_received || 0;
    let inboundWorking = snap.fba_inbound_working || 0;

    // working_first_seenが7日超なら除外
    if (inboundWorking > 0 && snap.working_first_seen) {
      const daysSinceFirstSeen = Math.floor(
        (new Date(snapshotDate) - new Date(snap.working_first_seen)) / 86400000
      );
      if (daysSinceFirstSeen > workingExpiryDays) {
        inboundWorking = 0; // 放置プラン扱い
      }
    }

    const effectiveFbaStock = fbaAvailable + inboundShipped + inboundReceived + inboundWorking;

    // --- 販売データ ---
    const sold7d = snap.units_sold_7d || 0;
    const sold30d = snap.units_sold_30d || 0;
    const dailySales = sold30d / 30;

    // --- 動的在庫日数目標 ---
    const targetDays = calcTargetDays(sold30d, snap.per_unit_volume || mapping.per_unit_volume || 0, snap, settings);

    // --- 供給日数 ---
    const daysOfSupply = dailySales > 0 ? effectiveFbaStock / dailySales : (effectiveFbaStock > 0 ? 999 : 0);

    // --- 必要補充数 ---
    const targetStock = Math.ceil(dailySales * targetDays);
    const rawNeeded = Math.max(0, targetStock - effectiveFbaStock);

    // --- 倉庫在庫確認 ---
    let warehouseAvailable = 0;
    let warehouseYQty = 0;
    const nonFbaSales30d = mapping.non_fba_sales_30d || 0;
    const nonFbaDailyReserve = Math.ceil((nonFbaSales30d / 30) * parseInt(settings.non_fba_reserve_days || 14));

    if (mapping.is_set && mapping.set_components) {
      // セット商品: 構成商品の最小在庫がボトルネック
      const components = typeof mapping.set_components === 'string'
        ? JSON.parse(mapping.set_components) : mapping.set_components;
      let minSets = Infinity;
      for (const comp of components) {
        const wh = warehouseMap[comp.ne_code];
        const compAvailable = (wh?.warehouse_available || 0) - nonFbaDailyReserve;
        const setsFromComp = Math.floor(Math.max(0, compAvailable) / (comp.qty || 1));
        minSets = Math.min(minSets, setsFromComp);
        warehouseYQty += wh?.y_location_qty || 0;
      }
      warehouseAvailable = minSets === Infinity ? 0 : minSets;
    } else {
      // 単品
      const wh = warehouseMap[mapping.logizard_code || mapping.ne_code];
      warehouseAvailable = Math.max(0, (wh?.warehouse_available || 0) - nonFbaDailyReserve);
      warehouseYQty = wh?.y_location_qty || 0;
    }

    // --- 送れる数 ---
    const recommendedQty = Math.min(rawNeeded, warehouseAvailable);

    // --- 曜日平準化 ---
    const dayOfWeek = new Date().getDay(); // 0=日, 1=月, ..., 5=金, 6=土
    const weekdayBoost = parseFloat(settings.weekday_boost_thu_fri || 1.5);
    const weekdayMultiplier = (dayOfWeek === 4 || dayOfWeek === 5) ? weekdayBoost : 1.0; // 木金
    const adjustedQty = Math.ceil(recommendedQty * weekdayMultiplier);

    // --- 少量商品のSKU分散（曜日ハッシュ） ---
    let skuDayMatch = true;
    if (sold30d <= parseInt(settings.low_volume_threshold || 20) && sold30d > 0) {
      const hash = hashCode(sku) % 5; // 月〜金に振り分け
      const workday = dayOfWeek >= 1 && dayOfWeek <= 5 ? dayOfWeek - 1 : -1; // 0=月,...,4=金
      skuDayMatch = (workday === hash);
    }

    // --- アラート ---
    const alerts = calcAlerts(snap, mapping, effectiveFbaStock, daysOfSupply, warehouseAvailable, settings);

    // --- 緊急度スコア ---
    const urgencyScore = calcUrgencyScore(daysOfSupply, sold30d, sold7d, snap, settings);

    // --- SKU例外処理 ---
    const exception = exceptionMap[sku];

    items.push({
      amazon_sku: sku,
      asin: mapping.asin || '',
      product_name: mapping.product_name || snap.product_name || '',
      ne_code: mapping.ne_code || '',
      is_set: mapping.is_set ? true : false,

      // FBA在庫
      fba_available: fbaAvailable,
      fba_inbound_working: snap.fba_inbound_working || 0,
      fba_inbound_shipped: inboundShipped,
      fba_inbound_received: inboundReceived,
      working_first_seen: snap.working_first_seen || null,
      working_expired: inboundWorking === 0 && (snap.fba_inbound_working || 0) > 0,
      effective_fba_stock: effectiveFbaStock,

      // 販売
      units_sold_7d: sold7d,
      units_sold_30d: sold30d,
      daily_sales: Math.round(dailySales * 100) / 100,
      non_fba_sales_7d: mapping.non_fba_sales_7d || 0,
      non_fba_sales_30d: nonFbaSales30d,

      // 在庫計画
      target_days: targetDays,
      days_of_supply: Math.round(daysOfSupply * 10) / 10,
      target_stock: targetStock,
      raw_needed: rawNeeded,

      // 倉庫
      warehouse_available: warehouseAvailable,
      warehouse_y_qty: warehouseYQty,
      non_fba_reserve: nonFbaDailyReserve,

      // 推奨
      recommended_qty: adjustedQty,
      sku_day_match: skuDayMatch,
      weekday_multiplier: weekdayMultiplier,

      // 価格
      your_price: snap.your_price || 0,
      featured_offer_price: snap.featured_offer_price || 0,
      lowest_price: snap.lowest_price || 0,
      sales_rank: snap.sales_rank || 0,

      // アラート・スコア
      alerts,
      urgency_score: Math.round(urgencyScore * 10) / 10,

      // 例外
      exception_type: exception?.exception_type || null,
    });
  }

  // 緊急度スコア降順でソート
  items.sort((a, b) => b.urgency_score - a.urgency_score);

  return {
    items,
    generated_at: new Date().toISOString(),
    snapshot_date: snapshotDate,
    total_skus: items.length,
    recommended_skus: items.filter(i => i.recommended_qty > 0).length,
    total_units: items.reduce((s, i) => s + i.recommended_qty, 0),
    errors: [],
  };
}

// ===== 動的在庫日数目標 =====
function calcTargetDays(sold30d, perUnitVolume, snap, settings) {
  const highVol = parseInt(settings.high_volume_threshold || 100);
  const lowVol = parseInt(settings.low_volume_threshold || 20);
  const smallVol = parseFloat(settings.small_volume_cm3 || 500);
  const largeVol = parseFloat(settings.large_volume_cm3 || 5000);

  // 季節商品チェック
  const isSeasonal = snap.is_seasonal === 'Yes' || snap.is_seasonal === 'TRUE';
  if (isSeasonal) return parseInt(settings.target_days_seasonal || 50);

  const isLarge = perUnitVolume > largeVol;

  if (sold30d > highVol) {
    return parseInt(isLarge ? settings.target_days_high_volume_large || 30 : settings.target_days_high_volume_small || 40);
  } else if (sold30d >= lowVol) {
    return parseInt(settings.target_days_medium || 35);
  } else {
    return parseInt(isLarge ? settings.target_days_low_volume_large || 60 : settings.target_days_low_volume_small || 120);
  }
}

// ===== アラート判定 =====
function calcAlerts(snap, mapping, effectiveFbaStock, daysOfSupply, warehouseAvailable, settings) {
  const alerts = [];
  const yourPrice = snap.your_price || 0;
  const cartPrice = snap.featured_offer_price || 0;
  const sold7d = snap.units_sold_7d || 0;
  const sold30d = snap.units_sold_30d || 0;

  // カート価格アラート
  if (yourPrice > 0 && cartPrice > 0) {
    const ratio = cartPrice / yourPrice;
    if (snap.fba_available === 0 && ratio < parseFloat(settings.cart_alert_level3_ratio || 0.5)) {
      alerts.push({ type: 'cart_block', level: 3, message: '納品保留推奨: カート価格が大幅に下落' });
    } else if (ratio < parseFloat(settings.cart_alert_level2_ratio || 0.8)) {
      alerts.push({ type: 'cart_warn', level: 2, message: 'カート価格が自社より20%以上低い' });
    } else if (cartPrice < yourPrice) {
      alerts.push({ type: 'cart_info', level: 1, message: 'カート価格 < 自社価格' });
    }
  }

  // 低在庫手数料
  const shortTermDos = snap.short_term_dos || 0;
  const lowInvThreshold = parseFloat(settings.low_inventory_fee_threshold_days || 14);
  if (shortTermDos > 0 && shortTermDos < lowInvThreshold && snap.low_inv_fee_exempt !== 'Yes') {
    alerts.push({ type: 'low_inv_fee', level: 2, message: `低在庫手数料中 (${shortTermDos}日)` });
  }

  // 過剰在庫
  const excessQty = snap.estimated_excess_qty || 0;
  const excessDos = parseFloat(settings.excess_inventory_dos_threshold || 90);
  if (excessQty > 0 || daysOfSupply > excessDos) {
    alerts.push({ type: 'excess', level: 2, message: `過剰在庫 (${excessQty > 0 ? excessQty + '個超過' : daysOfSupply.toFixed(0) + '日分'})` });
  }

  // 送り漏れ
  const bsrThreshold = parseInt(settings.missing_bsr_threshold || 5000);
  if (snap.fba_available === 0 && snap.sales_rank > 0 && snap.sales_rank < bsrThreshold && warehouseAvailable > 0) {
    alerts.push({ type: 'missing', level: 3, message: `送り漏れ: FBA在庫0 + BSR ${snap.sales_rank}` });
  }

  // トレンド
  const surgeRatio = parseFloat(settings.trend_surge_ratio || 2.0);
  const stopRatio = parseFloat(settings.trend_stop_ratio || 0.3);
  if (sold30d > 0) {
    const weeklyToMonthly = (sold7d / 7) * 30;
    const ratio = weeklyToMonthly / sold30d;
    if (ratio > surgeRatio) {
      alerts.push({ type: 'surge', level: 1, message: `急上昇 (7日→30日比 ${ratio.toFixed(1)}倍)` });
    } else if (ratio < stopRatio && sold7d === 0) {
      alerts.push({ type: 'stop', level: 1, message: '急停止 (7日間売上0)' });
    }
  }

  return alerts;
}

// ===== 緊急度スコア =====
function calcUrgencyScore(daysOfSupply, sold30d, sold7d, snap, settings) {
  if (sold30d === 0 && sold7d === 0) return 0;

  // baseScore: 在庫日数が少ないほど高い
  const baseScore = Math.max(0, 100 - (daysOfSupply * 100 / 40));

  // revenueWeight: 月商ベース
  const monthlySales = snap.sales_30d || (snap.your_price || 0) * sold30d;
  const revenueWeight = Math.min(monthlySales / 100000, 5);

  // trendBonus: 7日の急変
  let trendBonus = 1.0;
  if (sold30d > 0) {
    const weeklyToMonthly = (sold7d / 7) * 30;
    const weekRatio = weeklyToMonthly / sold30d;
    trendBonus = Math.max(0.5, Math.min(weekRatio, 3.0));
  }

  // feeBonus: 低在庫手数料中
  const shortTermDos = snap.short_term_dos || 0;
  const feeBonus = (shortTermDos > 0 && shortTermDos < 14 && snap.low_inv_fee_exempt !== 'Yes') ? 2.0 : 1.0;

  return baseScore * (1 + revenueWeight * 0.2) * trendBonus * feeBonus;
}

// ===== ユーティリティ =====
function hashCode(str) {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    hash = ((hash << 5) - hash) + str.charCodeAt(i);
    hash |= 0;
  }
  return Math.abs(hash);
}
