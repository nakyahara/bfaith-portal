/**
 * Phase 0 仕様の単商品 publish パイプライン (Phase E-4: dry-run mode のみ実装、 実 publish は E-5)。
 *
 * 設計原則:
 *   - 楽天 RMS は miniPC proxy 経由 (lib/rakuten-rms-proxy.js)
 *   - Notion override は専用 RYS DB から SELECT
 *   - 全 resolver → readiness → fields 構築までを実行
 *   - editItem 等の actual Yahoo API 呼び出しは E-5 で実装
 *   - dryRun=true なら jobs 行の readiness 更新もスキップ
 */

import { fetchItemDetail } from '../lib/rakuten-rms-proxy.js';
import { resolveDelivery } from './delivery-resolver.js';
import { resolveVariation } from './variation-resolver.js';
import { evaluateReadiness, persistJobReadiness } from './readiness-check.js';
import { runLeadTimePreflight } from '../lib/yahoo-lead-time.js';
import { imagePreflightStub } from '../lib/yahoo-image.js';
import { getCustomPatternStrings } from '../lib/image-exclusion-patterns.js';
import { resolveCategoryAndPath } from './category-resolver.js';
import { buildYahooEditItemFields, resolvePrice } from './field-mapper.js';

function loadNotionOverride(db, manageNumber) {
  if (!manageNumber) return null;
  return db.prepare(`SELECT * FROM notion_overrides WHERE rakuten_manage_number = ?`).get(manageNumber) || null;
}

/**
 * 単商品の Phase 0 publish-readiness 判定と fields 構築。
 *
 * @param {object} opts
 * @param {Database} opts.db
 * @param {string} opts.itemCode    jobs.item_code (= 楽天 itemNumber)
 * @param {string} opts.manageNumber 楽天 manageNumber (notion_overrides lookup key)
 * @param {boolean} [opts.dryRun=true] true なら readiness を jobs に persist しない
 * @param {number} [opts.aucPrefCode] 通常品 ヤフオク 発送地 prefecture
 * @param {object} [opts.deps] 外部依存差し替え (テスト用)
 * @returns {Promise<{itemCode, status: 'ok'|'blocked', reasons: string[], fields?, debug}>}
 */
export async function evaluateItemForPublish({
  db, itemCode, manageNumber,
  dryRun = true,
  aucPrefCode = null,
  productCategory = null,
  pathName = null,
  yahooProductCategoryId = null,
  deps = {},
}) {
  if (!db) throw new Error('evaluateItemForPublish: db required');
  if (!itemCode) throw new Error('evaluateItemForPublish: itemCode required');

  const _fetchItemDetail = deps.fetchItemDetail || fetchItemDetail;
  const _leadTimePreflight = deps.runLeadTimePreflight || runLeadTimePreflight;
  const _imagePreflight = deps.imagePreflight || imagePreflightStub;

  // 1. 楽天 RMS getItem (miniPC proxy)
  //   Codex E-4 R1 M-1: fetchItemDetail は { item, status, reason? } で返るので partial failure を残す
  let fetchResult;
  try {
    fetchResult = await _fetchItemDetail(manageNumber);
  } catch (e) {
    return {
      itemCode,
      status: 'blocked',
      reasons: [`rakuten_fetch_failed:${e.message || e}`],
      debug: { stage: 'rakuten_fetch' },
    };
  }
  if (fetchResult?.status === 'failed') {
    return {
      itemCode,
      status: 'blocked',
      reasons: [`rakuten_fetch_failed:${fetchResult.reason || 'rms_failure'}`],
      debug: { stage: 'rakuten_fetch' },
    };
  }
  if (!fetchResult?.item) {
    return {
      itemCode,
      status: 'blocked',
      reasons: ['rakuten_item_not_found'],
      debug: { stage: 'rakuten_fetch' },
    };
  }
  const rakutenItem = fetchResult.item;

  // 2. Notion override
  const notionOverride = loadNotionOverride(db, manageNumber);

  // 3. delivery / variation
  const deliveryRow = notionOverride
    ? resolveDelivery(db, notionOverride.notion_delivery_label)
    : null;
  const variationResult = resolveVariation({
    rakutenItem,
    notionHasVariation: notionOverride?.notion_has_variation,
  });

  // 4. lead_time preflight (cache あり)
  let leadTimePreflight;
  try {
    leadTimePreflight = await _leadTimePreflight();
  } catch (e) {
    leadTimePreflight = { ok: false, error: e.message || String(e) };
  }

  // 5. image preflight (E-4 stub、 E-5 で実 upload)
  //    Phase E-8: 楽天画像 URL に対し DB の image_exclusion_patterns + builtin (coupon/review) で除外判定
  //    Codex E-8 R1 H-1: 取得失敗時は fail-closed (空配列で続行すると custom 除外が無音で無効化される)
  let customPatterns = [];
  let imagePatternsLoadError = null;
  try {
    customPatterns = getCustomPatternStrings(db);
  } catch (e) {
    imagePatternsLoadError = e.message || String(e);
  }
  if (imagePatternsLoadError) {
    return {
      itemCode,
      status: 'blocked',
      reasons: [`image_patterns_load_failed:${imagePatternsLoadError}`],
      debug: { stage: 'image_patterns_load' },
    };
  }
  const imagePreflight = _imagePreflight(rakutenItem, { customPatterns });

  // 5.5. Phase E-11-c: category-resolver で productCategory + path を自動解決
  //   - 引数 productCategory / pathName が両方明示されてれば caller 値を採用
  //   - 片方だけは Codex R1 H-4 で禁止 (caller と learned の混在を避ける)
  //   - 両方 null なら Notion override → 学習辞書 (genre_yahoo_category_mapping) の順
  //   - 解決できなければ readiness が product_category_unresolved / path_unresolved で blocked
  let resolvedCategory = null;
  let resolvedPath = null;
  let categorySource = 'unresolved';
  let categorySampleCount = null;
  const callerHasCategory = productCategory != null;
  const callerHasPath = typeof pathName === 'string' && pathName.length > 0;
  if (callerHasCategory && callerHasPath) {
    resolvedCategory = productCategory;
    resolvedPath = pathName;
    categorySource = 'caller';
  } else if (callerHasCategory || callerHasPath) {
    // Codex E-11 R1 H-4: 片方だけ caller 指定は blocked にする (混在防止)
    return {
      itemCode,
      status: 'blocked',
      reasons: ['caller_category_partial:caller specified only one of productCategory/pathName — must be both or none'],
      debug: { stage: 'caller_partial', callerHasCategory, callerHasPath },
    };
  } else {
    const candidate = db.prepare('SELECT rakuten_genre_id FROM migration_candidates WHERE item_code = ?').get(itemCode);
    const rakutenGenreId = candidate?.rakuten_genre_id || null;
    const r = resolveCategoryAndPath({ db, rakutenGenreId, notionOverride });
    resolvedCategory = r.category;
    resolvedPath = r.path;
    categorySource = r.source;
    categorySampleCount = r.sampleCount || null;
  }

  // 6. readiness
  const rakutenTaxRate = rakutenItem.payment?.taxRate;
  const resolvedPrice = resolvePrice({ notionOverride, deliveryRow, rakutenItem });
  const readiness = evaluateReadiness({
    notionOverride,
    rakutenItem,
    rakutenTaxRate,
    deliveryRow,
    variationResult,
    productCategory: resolvedCategory,
    yahooProductCategoryId: yahooProductCategoryId || resolvedCategory,
    path: resolvedPath,
    resolvedPrice,
    imagePreflight,
    leadTimePreflight,
    db,
  });

  // 7. persist (dryRun スキップ)
  if (!dryRun) {
    try {
      persistJobReadiness(db, itemCode, readiness);
    } catch (e) {
      return {
        itemCode,
        status: 'blocked',
        reasons: [...readiness.reasons, `persist_failed:${e.message}`],
        debug: { stage: 'persist' },
      };
    }
  }

  // 8. ok なら fields 構築 (E-11-c: 自動解決後の category/path を渡す)
  let fields;
  if (readiness.status === 'ok') {
    fields = buildYahooEditItemFields({
      rakutenItem,
      notionOverride,
      deliveryRow,
      variationResult,
      productCategory: resolvedCategory,
      pathName: resolvedPath,
      aucPrefCode,
    });
    fields.display = 1; // readiness pass → display=1 昇格
  }

  return {
    itemCode,
    status: readiness.status,
    reasons: readiness.reasons,
    fields,
    debug: {
      notion_present: !!notionOverride,
      delivery_resolved: !!deliveryRow,
      variant_count: variationResult.variantCount,
      lead_time_ok: leadTimePreflight?.ok,
      image_ok: imagePreflight?.ok,
      // Phase E-5b: executor が performRealPublish で使う楽天 raw images を渡す
      rakutenImages: rakutenItem.images || [],
      // Phase E-8: executor が uploadRakutenImagesToYahoo に渡す custom patterns (snapshot)
      customImagePatterns: customPatterns,
      // Phase E-11-c: category 解決元 (notion / learned / unresolved / caller) と sample count を debug に残す
      categorySource,
      categorySampleCount,
      resolvedCategory,
      resolvedPath,
    },
  };
}
