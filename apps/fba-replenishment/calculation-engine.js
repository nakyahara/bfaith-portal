/**
 * FBA在庫補充 計算エンジン
 *
 * 入力: SP-APIスナップショット + SKUマッピング + 倉庫在庫 + 設定
 * 出力: SKUごとの推奨納品数 + 緊急度スコア + アラート
 */
import { getLatestSnapshots, getSkuMappings, getSkuExceptions, getSettings,
         getWarehouseSummary, getDailySnapshots, getAllNonFbaMax60d,
         getWarehouseLocationsByCode,
         getRestockLatest, getPlanningLatestMap } from './db.js';

/**
 * 推奨リストを生成
 * @param {boolean} debug - trueの場合、各SKUの計算過程をcalc_stepsに記録
 *
 * 2段階ロジック:
 *   1. 発注点(reorder_point) — FBA在庫が発注点(個数)を下回ったら推奨に上がる
 *      ※ 発注点は日数で設定し、個数換算(日数×FBA日販)で比較
 *   2. 目標日数(target_days) — 推奨に上がった時に何日分送るかの量
 *   → 低回転商品は発注点14日、目標180日（半年分）= たまに推奨に上がるが、上がったらまとめて送る
 *
 * 発注点で自然に絞り込み、ハードリミットは設けない
 */
export function generateRecommendations(debug = false, inboundWorkingOverride = null) {
  const settings = getSettings();
  const mappings = getSkuMappings();
  const exceptions = getSkuExceptions();
  const warehouseSummary = getWarehouseSummary();

  // --- データソース: RESTOCK (主軸) + PLANNING (補助、欠落許容) ---
  // 旧: getLatestSnapshots() → 新: getRestockLatest() をベースに PLANNING を上乗せ
  const restockRows = getRestockLatest();
  const planningMap = getPlanningLatestMap();

  // RESTOCK が空 (初回 or 取得失敗) の場合は旧 daily_snapshots にフォールバック (移行期間の保険)
  let snapshots;
  let dataSource;
  if (restockRows.length > 0) {
    dataSource = 'restock';
    snapshots = restockRows.map(r => mergeRestockWithPlanning(r, planningMap[r.amazon_sku]));
  } else {
    // フォールバック: 旧 daily_snapshots
    dataSource = 'legacy_snapshots';
    snapshots = getLatestSnapshots();
  }

  if (!snapshots.length) return { items: [], errors: ['スナップショットがありません。SP-APIレポートを取得してください。'] };
  if (!mappings.length) return { items: [], errors: ['SKUマッピングがありません。スプシ同期してください。'] };

  // ルックアップ用マップ
  const mappingMap = {};
  for (const m of mappings) mappingMap[m.amazon_sku] = m;

  const exceptionMap = {};
  for (const e of exceptions) exceptionMap[e.amazon_sku] = e;

  const warehouseMap = {};
  for (const w of warehouseSummary) warehouseMap[w.logizard_code] = w;

  // 他CH売上の60日間最大値 (表示用に残す、発注判定には使わない)
  const nonFbaMax60dList = getAllNonFbaMax60d();
  const nonFbaMax60dMap = {};
  for (const r of nonFbaMax60dList) nonFbaMax60dMap[r.amazon_sku] = r;

  const snapshotDate = snapshots[0]?.snapshot_date || new Date().toISOString().slice(0, 10);
  const oosAmazonRecoThreshold = parseInt(settings.oos_amazon_reco_threshold || 11);
  const items = [];

  for (const snap of snapshots) {
    const sku = snap.amazon_sku;
    const mapping = mappingMap[sku];
    if (!mapping) continue; // マッピングがないSKUはスキップ

    // --- 期限管理商品判定 (effectiveFbaStock 計算と min_shipment_days フィルタで使用) ---
    const hasExpiryManagement = (() => {
      if (!mapping.logizard_code) return false;
      const locations = getWarehouseLocationsByCode(mapping.logizard_code);
      return locations.some(l => l.expiry_date && l.expiry_date.trim() !== '');
    })();

    // --- 実質FBA在庫 ---
    const fbaAvailable = snap.fba_available || 0;
    const inboundShipped = snap.fba_inbound_shipped || 0;
    const inboundReceived = snap.fba_inbound_received || 0;
    const reportWorking = snap.fba_inbound_working || 0;

    // inboundWorking: Inbound API のリアルタイムデータを信頼。
    // レポート側の Working 列は信頼しない (0 のことが多く、遅延・残骸を含むため)
    let inboundWorking;
    let workingSource; // デバッグ用: データソース
    if (inboundWorkingOverride && inboundWorkingOverride[sku] !== undefined) {
      inboundWorking = inboundWorkingOverride[sku];
      workingSource = 'API';
    } else {
      // API失敗時: レポートの working を参考値として使う (通常商品のみ)
      inboundWorking = reportWorking;
      workingSource = 'report';
    }

    // --- 期限商品の例外: 別期限の在庫を送る必要があるため、inboundWorking を effectiveFbaStock から除外する ---
    // (通常商品は inboundWorking を足して二重推奨防止、期限商品はこの除外ルールを無効化)
    const effectiveInboundWorking = hasExpiryManagement ? 0 : inboundWorking;
    const effectiveFbaStock = fbaAvailable + inboundShipped + inboundReceived + effectiveInboundWorking;

    // --- 販売データ ---
    const sold7d = snap.units_sold_7d || 0;
    const sold30d = snap.units_sold_30d || 0;
    const dailySales = sold30d / 30;

    // --- 動的在庫日数目標 & 発注点 ---
    const perUnitVolume = snap.per_unit_volume || mapping.per_unit_volume || 0;
    const targetDays = calcTargetDays(sold30d, perUnitVolume, snap, settings);
    const reorderPointDays = calcReorderPoint(sold30d, sold7d, perUnitVolume, snap, settings);
    // 発注点を個数換算（日数 × FBA日販、最低1個 ※日販>0の場合）
    const reorderPointUnits = reorderPointDays > 0 && dailySales > 0
      ? Math.max(1, Math.ceil(dailySales * reorderPointDays))
      : 0;

    // --- 供給日数 ---
    const daysOfSupply = dailySales > 0 ? effectiveFbaStock / dailySales : (effectiveFbaStock > 0 ? 999 : 0);

    // --- 発注点チェック: FBA在庫 < 発注点(個数) の場合のみ補充推奨 ---
    const needsReplenishment = effectiveFbaStock < reorderPointUnits;

    // --- 必要補充数 ---
    const targetStock = Math.ceil(dailySales * targetDays);
    const rawNeeded = needsReplenishment ? Math.max(0, targetStock - effectiveFbaStock) : 0;

    // --- 倉庫在庫確認 ---
    let warehouseRaw = 0;
    let warehouseYQty = 0;
    const nonFbaSales30d = mapping.non_fba_sales_30d || 0;
    const nonFbaDailySales = nonFbaSales30d / 30;
    const totalDailySales = dailySales + nonFbaDailySales;

    // set_componentsがあれば常にcomponentsロジックを使う（単品qty>1にも対応）
    const components = mapping.set_components
      ? (typeof mapping.set_components === 'string' ? JSON.parse(mapping.set_components) : mapping.set_components)
      : null;

    if (components && components.length > 0) {
      // 構成商品の最小在庫がボトルネック（qty倍率を考慮）
      let minSets = Infinity;
      for (const comp of components) {
        const wh = warehouseMap[comp.ne_code];
        const compRaw = wh?.warehouse_available || 0;
        const setsFromComp = Math.floor(compRaw / (comp.qty || 1));
        minSets = Math.min(minSets, setsFromComp);
        warehouseYQty += wh?.y_location_qty || 0;
      }
      warehouseRaw = minSets === Infinity ? 0 : minSets;
    } else {
      // componentsなし（マッピングにNE商品コードがない場合など）
      const wh = warehouseMap[mapping.logizard_code || mapping.ne_code];
      warehouseRaw = wh?.warehouse_available || 0;
      warehouseYQty = wh?.y_location_qty || 0;
    }

    // --- 最終入荷日から入荷経過日数を算出 ---
    const whLookup = (components && components.length > 1) ? null : warehouseMap[mapping.logizard_code || mapping.ne_code];
    const lastArrivalDate = whLookup?.last_arrival_date || null;
    let daysSinceArrival = null;
    if (lastArrivalDate) {
      daysSinceArrival = Math.floor((new Date() - new Date(lastArrivalDate)) / 86400000);
    }

    // --- 他CH按分は廃止 (ユーザー方針: 他CH販売データを発注判定に使わない) ---
    // 非表示データとして他CH売上は表示するが、倉庫在庫引当には一切使わない
    // recent_arrival_adjusted は UI 互換のため常に false
    const nonFbaReserve = 0;
    const warehouseAvailable = warehouseRaw;
    const recentArrivalAdjusted = false;
    const effectiveNonFbaDailySales = nonFbaDailySales; // 表示用に計算は残す
    const effectiveTotalDailySales = dailySales + nonFbaDailySales; // 表示用
    const max60d = nonFbaMax60dMap[sku]; // 表示用にロードされていれば参照可

    // --- SKU状態分類 (納品推奨タブとFBA欠品タブの振り分け判定) ---
    const amazonReco = snap.amazon_recommended_qty; // null | number
    const stockState = classifyStockState(fbaAvailable, sold30d, amazonReco, oosAmazonRecoThreshold);

    // --- GAS流: 自社理論値 vs Amazon推奨数の小さい方を採用 (Amazon推奨の多めに出る弱点を抑制) ---
    // rawNeeded(自社理論)が既にあるのでそれをベースに min(rawNeeded, amazonReco) で切り詰め
    // ※ amazonReco === null (列欠損) の場合は自社理論のみ採用 (Codex指摘4: 0 vs null を区別)
    let boundedNeeded = rawNeeded;
    if (rawNeeded > 0 && amazonReco !== null && amazonReco !== undefined && amazonReco < rawNeeded) {
      boundedNeeded = amazonReco;
    }

    // --- 送れる数 ---
    // stockState が revivable_long_oos / dead_candidate の SKU は推奨数ゼロ固定 (FBA欠品タブで別扱い)
    let recommendedQty;
    if (stockState === 'revivable_long_oos' || stockState === 'dead_candidate') {
      recommendedQty = 0;
    } else {
      recommendedQty = Math.min(boundedNeeded, warehouseAvailable);
    }

    // --- 有効期限チェック: 同一期限のものしか1回の納品で送れない ---
    // ※ FBAは同一商品で期限違いを同梱不可 → 必ず同一期限のみ送る
    // ※ 期限なしロケに在庫がある場合は「期限管理SKUなのに期限未登録」のデータ不備として警告
    let expiryLimited = false;
    let expiryDate = '';
    let expirySameQty = 0;
    let undatedLocQty = 0; // 期限なしロケにある在庫数（データ不備警告用）
    if (mapping.logizard_code) {
      const locations = getWarehouseLocationsByCode(mapping.logizard_code);
      // 有効期限があるロケが1つでもあるかチェック
      const locsWithExpiry = locations.filter(l => l.expiry_date && l.expiry_date.trim() !== '');
      if (locsWithExpiry.length > 0) {
        // 引当優先順で最初に引き当たる有効期限を基準とする
        const baseExpiry = locsWithExpiry[0].expiry_date.trim();
        expiryDate = baseExpiry; // 常に有効期限を保持（納品プラン作成時に必要）

        // 同一期限のロケ在庫のみを合算（期限なしロケは除外し警告対象）
        let sameExpiryTotal = 0;
        for (const loc of locations) {
          const locExpiry = (loc.expiry_date || '').trim();
          if (locExpiry === baseExpiry) {
            sameExpiryTotal += loc.available_qty;
          } else if (!locExpiry && loc.available_qty > 0) {
            undatedLocQty += loc.available_qty;
          }
        }

        if (recommendedQty > 0 && sameExpiryTotal < recommendedQty) {
          expiryLimited = true;
          expirySameQty = sameExpiryTotal;
          recommendedQty = sameExpiryTotal;
        }
      }
    }

    // --- 最低出荷日数フィルター: 送っても○日分に満たない場合は除外（入荷待ちの方が効率的） ---
    // FBA在庫0でも1日分未満なら送る意味がないので除外
    // ※ 有効期限管理商品は除外しない（古い期限の在庫が滞留して廃棄リスクになるため）
    // hasExpiryManagement は上位スコープで既定義済み (effectiveFbaStock 計算時に必要だったため)
    const minShipmentDays = parseInt(settings.min_shipment_cover_days || 7);
    let skippedByMinDays = false;
    if (recommendedQty > 0 && dailySales > 0 && !hasExpiryManagement) {
      const coverDays = recommendedQty / dailySales;
      // FBA在庫0: 最低1日分は必要（それ未満は焼け石に水）
      // FBA在庫あり: 設定日数分が必要（二度手間防止）
      const threshold = effectiveFbaStock === 0 ? Math.min(minShipmentDays, 1) : minShipmentDays;
      if (coverDays < threshold) {
        skippedByMinDays = true;
        recommendedQty = 0;
      }
    }

    // --- Step A: 5個単位丸め（20個超の場合） ---
    // ※ 有効期限で制限された場合はスキップ（期限在庫をそのまま送る）
    const rawRecommendedQty = recommendedQty; // 丸め前の推奨数を保持
    let adjustedQty = recommendedQty;
    const roundUnit = parseInt(settings.round_unit || 5);
    const roundThreshold = parseInt(settings.round_threshold || 20);
    let roundedQty = recommendedQty; // 丸めのみの数（ロケ補正前）
    if (!expiryLimited && adjustedQty > roundThreshold) {
      adjustedQty = Math.round(adjustedQty / roundUnit) * roundUnit;
      if (adjustedQty > warehouseAvailable) {
        adjustedQty = Math.floor(warehouseAvailable / roundUnit) * roundUnit;
      }
      if (adjustedQty <= 0 && recommendedQty > 0) adjustedQty = recommendedQty;
      roundedQty = adjustedQty;
    }

    // --- Step B: ロケーション補正（±10%以内でロケ在庫の区切りに寄せる） ---
    // ※ 有効期限で制限された場合はスキップ（期限が数量を決定済み）
    let locationAdjusted = false;
    let locationDetail = '';
    const locAdjustPct = parseFloat(settings.location_adjust_pct || 10) / 100;
    if (!expiryLimited && adjustedQty > 0 && mapping.logizard_code) {
      const locations = getWarehouseLocationsByCode(mapping.logizard_code);
      if (locations.length > 0) {
        const lower = adjustedQty * (1 - locAdjustPct);
        const upper = adjustedQty * (1 + locAdjustPct);
        let cumulative = 0;
        const breakpoints = []; // 各ロケ累積の区切り点
        for (const loc of locations) {
          cumulative += loc.available_qty;
          breakpoints.push({
            cumulative,
            location: loc.location,
            biz_type: loc.location_biz_type,
            qty: loc.available_qty,
          });
        }
        // ±10%以内の区切り点から、丸め後に最も近いものを選択
        let bestMatch = null;
        let bestDiff = Infinity;
        for (const bp of breakpoints) {
          if (bp.cumulative >= lower && bp.cumulative <= upper) {
            const diff = Math.abs(bp.cumulative - adjustedQty);
            if (diff < bestDiff) {
              bestDiff = diff;
              bestMatch = bp;
            }
          }
        }
        if (bestMatch && bestMatch.cumulative !== adjustedQty) {
          adjustedQty = bestMatch.cumulative;
          locationAdjusted = true;
          locationDetail = `${bestMatch.location}まで累積${bestMatch.cumulative}個`;
        }
      }
    }

    // --- アラート ---
    const alerts = calcAlerts(snap, mapping, effectiveFbaStock, daysOfSupply, warehouseAvailable, settings);

    // 期限管理SKUに期限なしロケの在庫がある場合はデータ不備警告（FBA同梱不可のため送れない）
    if (undatedLocQty > 0) {
      alerts.push({
        type: 'expiry_missing',
        level: 3,
        message: `期限なしロケに${undatedLocQty}個(期限管理SKU/ロケ期限要登録)`,
      });
    }

    // --- 緊急度スコア ---
    const urgencyScore = calcUrgencyScore(daysOfSupply, sold30d, sold7d, snap, settings);

    // --- SKU例外処理 ---
    const exception = exceptionMap[sku];

    // --- デバッグ: 計算過程の記録 ---
    let calc_steps = null;
    if (debug) {
      const perUnitVol = snap.per_unit_volume || mapping.per_unit_volume || 0;
      calc_steps = [
        `[Step1] 実質FBA在庫 = ${fbaAvailable}(販売可能) + ${inboundShipped}(輸送中) + ${inboundReceived}(受領中) + ${inboundWorking}(準備中[${workingSource}]${reportWorking !== inboundWorking ? `,レポート=${reportWorking}` : ''}) = ${effectiveFbaStock}`,
        `[Step2] 1日あたり販売数 = FBA ${dailySales.toFixed(2)}個/日 (${sold30d}個/30日) + 他CH ${nonFbaDailySales.toFixed(2)}個/日 (${nonFbaSales30d}個/30日) = 合計 ${totalDailySales.toFixed(2)}個/日`,
        `[Step3] 発注点 = ${reorderPointUnits}個 (${reorderPointDays}日×${dailySales.toFixed(2)}個/日) / 目標日数 = ${targetDays}日 (30日売上:${sold30d}個, サイズ:${perUnitVol}cm³${perUnitVol > 5000 ? '→大型' : perUnitVol > 500 ? '→中型' : perUnitVol > 0 ? '→小型' : '→不明'})`,
        `[Step4] FBA在庫 ${effectiveFbaStock}個 ${needsReplenishment ? '< 発注点' + reorderPointUnits + '個 → 補充推奨' : '≧ 発注点' + reorderPointUnits + '個 → 補充不要'} (供給日数: ${daysOfSupply === 999 ? '∞' : daysOfSupply.toFixed(1) + '日'})`,
        `[Step5] 必要補充数 = ${needsReplenishment ? `ceil(${dailySales.toFixed(2)} × ${targetDays}) - ${effectiveFbaStock} = ${targetStock} - ${effectiveFbaStock} = ${rawNeeded}` : '0(発注点以上のため)'}`,
        `[Step6] 倉庫在庫按分 = ${warehouseRaw}個(倉庫,Yロケ除外) × FBA比率${effectiveTotalDailySales > 0 ? (dailySales / effectiveTotalDailySales * 100).toFixed(0) : 100}% = FBA用${warehouseAvailable}個 / 他CH確保${nonFbaReserve}個${recentArrivalAdjusted ? ` [補正: 他CH実効日販${effectiveNonFbaDailySales.toFixed(2)}${max60d?.max_30d > nonFbaSales30d ? `(60日最大値${max60d.max_30d}ベース)` : daysSinceArrival !== null ? `(入荷${daysSinceArrival}日目推定)` : ''}]` : ''}${lastArrivalDate ? ` (最終入荷: ${lastArrivalDate})` : ''}`,
        `[Step7] 推奨数 = min(${rawNeeded}(必要数), ${warehouseAvailable}(FBA用在庫)) = ${skippedByMinDays ? `0 (元${Math.min(rawNeeded, warehouseAvailable)}個→${(Math.min(rawNeeded, warehouseAvailable) / dailySales).toFixed(1)}日分 < 最低${minShipmentDays}日 → 入荷待ち)` : rawRecommendedQty}${hasExpiryManagement && !skippedByMinDays && dailySales > 0 && rawRecommendedQty > 0 && rawRecommendedQty / dailySales < minShipmentDays ? ` (${(rawRecommendedQty / dailySales).toFixed(1)}日分 < ${minShipmentDays}日だが期限管理商品のためフィルター無効)` : ''}`,
        `[Step7.5] 有効期限: ${expiryLimited ? '基準期限' + expiryDate + ' → 同一期限在庫' + expirySameQty + '個 → 推奨数を' + expirySameQty + '個に制限' : '制限なし(期限データなし or 同一期限)'}`,
        `[Step8] 補正: ${expiryLimited ? 'スキップ(有効期限制限済み → ' + adjustedQty + '個をそのまま送る)' : rawRecommendedQty === adjustedQty ? 'なし' : rawRecommendedQty + ' → ' + (roundedQty !== rawRecommendedQty ? roundedQty + '(' + roundUnit + '個丸め)' : String(rawRecommendedQty)) + (locationAdjusted ? ' → ' + adjustedQty + '(ロケ補正: ' + locationDetail + ')' : '') + ' = 最終' + adjustedQty + '個 (' + (rawRecommendedQty > 0 ? ((adjustedQty - rawRecommendedQty) / rawRecommendedQty * 100).toFixed(0) : 0) + '%)'}`,
        `[Step9] 緊急度 = ${urgencyScore.toFixed(1)} (基本:${Math.max(0, 100 - (daysOfSupply * 100 / 40)).toFixed(0)}, 月商W:${Math.min((snap.your_price || 0) * sold30d / 100000, 5).toFixed(1)}, トレンド:${sold30d > 0 ? ((sold7d / 7 * 30) / sold30d).toFixed(1) : '-'})`,
        `[Step10] アラート: ${alerts.length > 0 ? alerts.map(a => a.message).join(' / ') : 'なし'}`,
      ];
    }

    items.push({
      amazon_sku: sku,
      asin: mapping.asin || snap.asin || '',
      product_name: mapping.product_name || snap.product_name || '',
      ne_code: mapping.ne_code || '',
      is_set: mapping.is_set ? true : false,
      set_components: components || null,

      // SKU状態分類 (タブ振り分け用)
      stock_state: stockState,
      alert_type: snap.alert_type || null,
      is_expiry_managed: hasExpiryManagement,

      // FBA在庫
      fba_available: fbaAvailable,
      fba_inbound_working: inboundWorking,
      fba_inbound_working_report: reportWorking,
      fba_inbound_working_source: workingSource,
      fba_inbound_working_effective: effectiveInboundWorking, // 期限商品は0扱い
      fba_inbound_shipped: inboundShipped,
      fba_inbound_received: inboundReceived,
      working_first_seen: snap.working_first_seen || null,
      working_expired: false, // working_first_seen 機構は廃止
      effective_fba_stock: effectiveFbaStock,

      // 販売
      units_sold_7d: sold7d,
      units_sold_30d: sold30d,
      daily_sales: Math.round(dailySales * 100) / 100,
      non_fba_sales_7d: mapping.non_fba_sales_7d || 0,
      non_fba_sales_30d: nonFbaSales30d,

      // 在庫計画
      reorder_point: reorderPointUnits,
      reorder_point_days: reorderPointDays,
      target_days: targetDays,
      days_of_supply: Math.round(daysOfSupply * 10) / 10,
      target_stock: targetStock,
      raw_needed: rawNeeded,
      needs_replenishment: needsReplenishment,

      // 倉庫
      warehouse_available: warehouseAvailable,
      warehouse_y_qty: warehouseYQty,
      non_fba_reserve: nonFbaReserve,
      warehouse_raw: warehouseRaw,
      fba_share_pct: effectiveTotalDailySales > 0 ? Math.round(dailySales / effectiveTotalDailySales * 100) : 100,
      last_arrival_date: lastArrivalDate,
      days_since_arrival: daysSinceArrival,
      recent_arrival_adjusted: recentArrivalAdjusted,

      // 推奨
      recommended_qty: rawRecommendedQty,
      amazon_recommended_qty: amazonReco, // null許容 (Codex指摘4: 0 と未取得を区別)
      raw_needed_before_amazon_cap: rawNeeded, // Amazon推奨で切り詰める前の自社理論値
      amazon_reco_capped: amazonReco !== null && amazonReco !== undefined && amazonReco < rawNeeded,
      expiry_limited: expiryLimited,
      expiry_date: expiryDate,
      expiry_same_qty: expirySameQty,
      rounded_qty: roundedQty,
      adjusted_qty: adjustedQty,
      adjust_diff_pct: rawRecommendedQty > 0 ? Math.round((adjustedQty - rawRecommendedQty) / rawRecommendedQty * 100) : 0,
      location_adjusted: locationAdjusted,
      location_detail: locationDetail,
      skipped_min_days: skippedByMinDays,

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

      // デバッグ
      calc_steps: calc_steps,
    });
  }

  // 緊急度スコア降順でソート
  items.sort((a, b) => b.urgency_score - a.urgency_score);

  // 補充推奨SKU（発注点を下回ったもの）
  const recommendedItems = items.filter(i => i.recommended_qty > 0);

  return {
    items,
    generated_at: new Date().toISOString(),
    snapshot_date: snapshotDate,
    total_skus: items.length,
    recommended_skus: recommendedItems.length,
    recommended_units: recommendedItems.reduce((s, i) => s + i.recommended_qty, 0),
    errors: [],
  };
}

// ===== 動的在庫日数目標（推奨に上がった時に何日分送るか） =====
function calcTargetDays(sold30d, perUnitVolume, snap, settings) {
  const highVol = parseInt(settings.high_volume_threshold || 100);
  const lowVol = parseInt(settings.low_volume_threshold || 20);
  const largeVol = parseFloat(settings.large_volume_cm3 || 5000);

  // 季節商品チェック
  const isSeasonal = snap.is_seasonal === 'Yes' || snap.is_seasonal === 'TRUE';
  if (isSeasonal) return parseInt(settings.target_days_seasonal || 50);

  // サイズ不明(0)は大型扱い（保管料リスク回避）
  const isLarge = perUnitVolume === 0 || perUnitVolume > largeVol;

  if (sold30d > highVol) {
    return parseInt(isLarge ? settings.target_days_high_volume_large || 30 : settings.target_days_high_volume_small || 40);
  } else if (sold30d >= lowVol) {
    return parseInt(settings.target_days_medium || 35);
  } else {
    // 低回転: 大型/不明=90日、小型=180日
    return parseInt(isLarge ? settings.target_days_low_volume_large || 90 : settings.target_days_low_volume_small || 180);
  }
}

// ===== 発注点（FBA在庫がこの個数を下回ったら推奨に上がる） =====
function calcReorderPoint(sold30d, sold7d, perUnitVolume, snap, settings) {
  const highVol = parseInt(settings.high_volume_threshold || 100);
  const lowVol = parseInt(settings.low_volume_threshold || 20);
  const fbaWeeklyThreshold = parseInt(settings.fba_weekly_threshold || 10);

  // 季節商品
  const isSeasonal = snap.is_seasonal === 'Yes' || snap.is_seasonal === 'TRUE';
  if (isSeasonal) return parseInt(settings.reorder_point_seasonal || 21);

  // 売上0の商品 → 発注点0（推奨に上がらない、手動追加で対応）
  if (sold30d === 0) return 0;

  // 7日売上が閾値以上 → 低在庫手数料リスクあり → 最低でも中回転扱い
  if (sold7d >= fbaWeeklyThreshold && sold30d < lowVol) {
    return parseInt(settings.reorder_point_medium || 21);
  }

  if (sold30d > highVol) {
    return parseInt(settings.reorder_point_high_volume || 21);
  } else if (sold30d >= lowVol) {
    return parseInt(settings.reorder_point_medium || 21);
  } else {
    return parseInt(settings.reorder_point_low_volume || 14);
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

/**
 * RESTOCK最新行と PLANNING 最新行 (あれば) をマージして、
 * 旧 daily_snapshots と互換の snap オブジェクトを生成する
 *
 * RESTOCK: 必須、全SKUの主軸 (30日販売、在庫内訳、Amazon推奨数、価格、警告)
 * PLANNING: 補助、欠落許容 (7/60/90日販売、季節性、サイズ、低在庫手数料情報)
 */
function mergeRestockWithPlanning(r, p) {
  const planning = p || {};
  return {
    amazon_sku: r.amazon_sku,
    product_name: r.product_name || '',
    asin: r.asin || '',
    fnsku: r.fnsku || '',

    // FBA在庫 (RESTOCK由来、主軸)
    fba_available: r.fba_available || 0,
    fba_inbound_working: r.fba_inbound_working || 0,
    fba_inbound_shipped: r.fba_inbound_shipped || 0,
    fba_inbound_received: r.fba_inbound_received || 0,
    fba_unfulfillable: r.fba_unfulfillable || 0,

    // 販売データ (30d は RESTOCK、7/60/90d は PLANNING 補助)
    units_sold_30d: r.units_sold_30d || 0,
    units_sold_7d: planning.units_sold_7d ?? 0,
    units_sold_60d: planning.units_sold_60d ?? 0,
    units_sold_90d: planning.units_sold_90d ?? 0,

    // 供給日数 (RESTOCK が主、PLANNING フォールバック)
    days_of_supply: r.days_of_supply ?? 0,

    // 価格 (RESTOCK=your_price、PLANNING=featured_offer / lowest)
    your_price: r.your_price ?? planning.featured_offer_price ?? 0,
    featured_offer_price: planning.featured_offer_price ?? 0,
    lowest_price: planning.lowest_price ?? 0,
    sales_rank: planning.sales_rank ?? 0,

    // サイズ・季節 (PLANNING 専用)
    per_unit_volume: planning.per_unit_volume ?? 0,
    is_seasonal: planning.is_seasonal || '',
    season_name: planning.season_name || '',

    // 低在庫手数料・過剰在庫 (PLANNING 専用、アラート用)
    short_term_dos: planning.short_term_dos ?? 0,
    long_term_dos: planning.long_term_dos ?? 0,
    low_inv_fee_applied: planning.low_inv_fee_applied || '',
    low_inv_fee_exempt: planning.low_inv_fee_exempt || '',
    estimated_excess_qty: planning.estimated_excess_qty ?? 0,
    estimated_storage_cost: planning.estimated_storage_cost ?? 0,

    // RESTOCK 固有 (新規)
    amazon_recommended_qty: r.amazon_recommended_qty,   // null許容
    amazon_recommended_date: r.amazon_recommended_date || null,
    alert_type: r.alert_type || null,

    // 互換性用
    snapshot_date: (r.updated_at || '').slice(0, 10) || new Date().toISOString().slice(0, 10),
    working_first_seen: null, // 旧 working_first_seen 機構は廃止
  };
}

/**
 * SKU状態を分類 (納品推奨タブとFBA欠品タブの振り分け判定用)
 *
 * normal:             在庫あり (通常運転)
 * recent_oos:         FBA在庫0 + 30日販売あり (直近欠品、通常補充ルート)
 * revivable_long_oos: FBA在庫0 + 30日販売0 + Amazon推奨>=閾値 (長期欠品だが復活余地あり)
 * dead_candidate:     FBA在庫0 + 30日販売0 + Amazon推奨0または未取得 (廃番候補)
 */
function classifyStockState(fbaAvailable, sold30d, amazonReco, oosThreshold) {
  if (fbaAvailable > 0) return 'normal';
  if (sold30d > 0) return 'recent_oos';
  // fba=0 AND sold30d=0
  if (typeof amazonReco === 'number' && amazonReco >= oosThreshold) {
    return 'revivable_long_oos';
  }
  return 'dead_candidate';
}
