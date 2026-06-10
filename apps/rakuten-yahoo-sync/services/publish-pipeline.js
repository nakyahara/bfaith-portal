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
  const imagePreflight = _imagePreflight(rakutenItem);

  // 6. readiness
  const rakutenTaxRate = rakutenItem.payment?.taxRate;
  const resolvedPrice = resolvePrice({ notionOverride, deliveryRow, rakutenItem });
  const readiness = evaluateReadiness({
    notionOverride,
    rakutenItem,
    rakutenTaxRate,
    deliveryRow,
    variationResult,
    productCategory,
    yahooProductCategoryId,
    path: pathName,
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

  // 8. ok なら fields 構築
  let fields;
  if (readiness.status === 'ok') {
    fields = buildYahooEditItemFields({
      rakutenItem,
      notionOverride,
      deliveryRow,
      variationResult,
      productCategory,
      pathName,
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
    },
  };
}
