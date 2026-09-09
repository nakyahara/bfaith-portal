/**
 * engine.mjs — Company DB 初期ロードの本体 (「ロード計画」→ PostgreSQL)。Company DB構想 06 §5.6 / §7.1 PR-B
 *
 * 入口は sources.mjs が作る「ロード計画 (plan)」= 出どころに依存しない素の配列。ここは Postgres に入れるだけ。
 * 試験は plan を手で組んで PGlite に流す (SQLite の実ファイルは要らない)。
 *
 * 約束 (03 §1 / 06 §5.6 / Codex 総合意見):
 *   - 1 回のロード = 1 トランザクション。途中で失敗したら全部巻き戻す (半端な状態を残さない)
 *   - 冪等: 何度流しても同じ結果 (upsert / on conflict do nothing / 有効期間の付け替え)
 *   - 🚨 投入予定 vs 実投入の diff を必ず出す。core (skus / products / 構成) は「予定 = 投入 + 理由つき skip」でなければ fail-close
 *     (FK 移行で 430 件が無音で欠落した教訓。try/catch で 1 件ずつ握りつぶさない)
 *   - 親不在 (構成の子 SKU が無い、出品の NE コードが無い) は事前に数えて report に載せる。黙って落とさない
 *   - 属性は「観測」として全部残し、解決規則 (rule_version) で 1 つ選ぶ。不一致は report (= 所見の種)
 *   - ASIN は product に直付けしない (catalog_items 経由)。JAN は product、FNSKU は listing、NE コードは sku
 *
 * plan の形 (sources.mjs / test を参照):
 *   { skus:[{code,name,kind,taxRate,taxClass,handling,salesClass,representativeCode,cost:{jpy,source,status}|null}],
 *     setComponents:[{parentCode,childCode,qty,source}],
 *     listings:[{mall,shopCode,listingCode,mallItemId,title,status,components:[{code,qty,resolution,evidence}],asin,asinSource,fnsku}],
 *     observations:[{skuCode,attribute,scope,valueText,valueNum,unit,rawText,source,sourceRef,observedAt}],
 *     physicals:[{skuCode,scope,lengthMm,widthMm,heightMm,weightG,source,isMeasured,observedAt}],
 *     compliance:[{skuCode,ingredients,precautions,distributor,manufacturerJp,allergens,source,sourceRef}],
 *     suppliers:[{code,name,orderMethod,leadTimeDays}], supplierSkus:[{supplierCode,skuCode,vendorCode,stockUnitsPerOrderUnit,minOrderQty,orderMultiple,unitCostJpy}],
 *     workers:[{staffNo,displayName,loginEmail,workerType,active}] }
 */
import crypto from 'node:crypto';
import { normSku } from '../../../lib/sku-norm.js';

export const COMPANY_ID = 1;
export const RULE_VERSION = 'v1';
const CHUNK = 400;

export function newLoadRunId() {
  return `load_${new Date().toISOString().replace(/[-:.TZ]/g, '').slice(0, 15)}_${crypto.randomBytes(3).toString('hex')}`;
}

function sha1(s) { return crypto.createHash('sha1').update(String(s)).digest('hex').slice(0, 16); }

/** 複数行 INSERT (chunk 単位)。returning があれば結果行を全部返す */
async function insertMany(db, table, columns, rows, { onConflict = '', returning = '' } = {}) {
  const out = [];
  for (let i = 0; i < rows.length; i += CHUNK) {
    const chunk = rows.slice(i, i + CHUNK);
    const params = [];
    const values = chunk.map((r) => `(${columns.map((c) => { params.push(r[c] === undefined ? null : r[c]); return `$${params.length}`; }).join(', ')})`).join(', ');
    const sql = `insert into ${table} (${columns.join(', ')}) values ${values} ${onConflict} ${returning ? `returning ${returning}` : ''}`;
    const res = await db.query(sql, params);
    if (returning) out.push(...res.rows);
  }
  return out;
}

function section(report, name, expected) {
  const s = { expected, applied: 0, skipped: [], notes: [] };
  report.sections[name] = s;
  return s;
}

/** 予定 = 投入 + skip でなければ fail-close (core の表だけ) */
function assertBalanced(sec, name) {
  if (sec.expected !== sec.applied + sec.skipped.length) {
    throw Object.assign(new Error(`${name}: 予定 ${sec.expected} ≠ 投入 ${sec.applied} + skip ${sec.skipped.length} (無音の欠落。巻き戻す)`), { code: 'LOAD_UNBALANCED', section: name });
  }
}

/**
 * ロード本体。db = { query(text, params), exec(text) } (pg Client / PGlite の adapter)。
 * 戻り値 = report (JSON にできる)。dryRun のときは全部やってから rollback する (= 本番と同じ検査を通す)
 */
export async function runInitialLoad(db, plan, opts = {}) {
  const runId = opts.runId || newLoadRunId();
  const dryRun = !!opts.dryRun;
  const log = opts.log || ((m) => console.log(`[company-db load] ${m}`));
  const now = opts.now || new Date();
  const nowIso = now.toISOString();
  const jstToday = new Date(now.getTime() + 9 * 3600 * 1000).toISOString().slice(0, 10);
  const report = { run_id: runId, dry_run: dryRun, started_at: nowIso, sections: {}, conflicts: [], unresolved: {}, ok: false };

  await db.exec('begin');
  try {
    // ── 0. 参照: rule_versions / rules ──
    const rules = (await db.query('select attribute, packaging_scope, source_system, priority from core.attribute_resolution_rules where rule_version = $1', [RULE_VERSION])).rows;
    const rulePriority = new Map(rules.map((r) => [`${r.attribute}|${r.packaging_scope}|${r.source_system}`, r.priority]));

    // ── 1. 予定の確定 (正規化衝突は先に落とす) ──
    const skuSec = section(report, 'skus', plan.skus.length);
    const seenNorm = new Map();
    const accepted = [];
    for (const s of plan.skus) {
      const norm = normSku(s.code);
      if (!norm) { skuSec.skipped.push({ code: s.code, reason: 'code が空' }); continue; }
      if (seenNorm.has(norm)) { skuSec.skipped.push({ code: s.code, reason: `正規化すると ${seenNorm.get(norm)} と衝突` }); continue; }
      seenNorm.set(norm, s.code);
      accepted.push(s);
    }

    // ── 2. products (単品 SKU に 1:1)。🚨 skus の CHECK (単品は product 必須) があるので product を先に作る ──
    const prodSec = section(report, 'products', accepted.filter((s) => s.kind === 'single').length);
    const existing = new Map((await db.query('select s.code_norm, s.product_id from core.skus s where s.company_id = $1 and s.product_id is not null', [COMPANY_ID])).rows.map((r) => [r.code_norm, Number(r.product_id)]));
    const productIdBySku = new Map(existing);
    const toCreate = accepted.filter((s) => s.kind === 'single' && !existing.has(normSku(s.code)));
    const created = await insertMany(db, 'core.products', ['company_id', 'display_code', 'name', 'sales_class', 'status', 'created_by_type', 'created_by_id'],
      toCreate.map((s) => ({ company_id: COMPANY_ID, display_code: s.code, name: s.name || s.code, sales_class: s.salesClass ?? null, status: s.handling === 'discontinued' ? 'discontinued' : 'active', created_by_type: 'system', created_by_id: runId })),
      { returning: 'product_id, display_code' });
    for (const r of created) productIdBySku.set(normSku(r.display_code), Number(r.product_id));
    // 既存 product の名前・状態・分類も追随
    for (const s of accepted) {
      if (s.kind !== 'single' || toCreate.includes(s)) continue;
      const pid = productIdBySku.get(normSku(s.code));
      if (!pid) continue;
      await db.query('update core.products set name = $2, sales_class = $3, status = $4 where product_id = $1 and company_id = $5 and (name is distinct from $2 or sales_class is distinct from $3 or status is distinct from $4)', [pid, s.name || s.code, s.salesClass ?? null, s.handling === 'discontinued' ? 'discontinued' : 'active', COMPANY_ID]);
    }
    prodSec.applied = accepted.filter((s) => s.kind === 'single' && productIdBySku.has(normSku(s.code))).length;
    assertBalanced(prodSec, 'products');

    // ── 3. skus (upsert by company + code_norm。単品は product_id つき) ──
    const skuRows = accepted.map((s) => ({
      company_id: COMPANY_ID, product_id: s.kind === 'single' ? productIdBySku.get(normSku(s.code)) : null,
      sku_kind: s.kind, code: s.code, name: s.name || s.code,
      tax_rate: s.taxRate ?? null, tax_class: s.taxClass ?? null, handling: s.handling || 'unknown',
      created_by_type: 'system', created_by_id: runId,
    }));
    const skuIds = new Map();   // code_norm → sku_id
    const returned = await insertMany(db, 'core.skus', ['company_id', 'product_id', 'sku_kind', 'code', 'name', 'tax_rate', 'tax_class', 'handling', 'created_by_type', 'created_by_id'], skuRows, {
      onConflict: 'on conflict (company_id, code_norm) do update set name = excluded.name, sku_kind = excluded.sku_kind, tax_rate = excluded.tax_rate, tax_class = excluded.tax_class, handling = excluded.handling, product_id = coalesce(core.skus.product_id, excluded.product_id)',
      returning: 'sku_id, code_norm',
    });
    for (const r of returned) skuIds.set(r.code_norm, Number(r.sku_id));
    skuSec.applied = returned.length;
    assertBalanced(skuSec, 'skus');
    log(`skus: ${skuSec.applied} (skip ${skuSec.skipped.length})`);
    // バリエーション親 (代表商品コードが自分以外の単品 SKU を指すとき)
    let parents = 0; const parentMissing = [];
    for (const s of plan.skus) {
      if (s.kind !== 'single' || !s.representativeCode) continue;
      const repNorm = normSku(s.representativeCode); const myNorm = normSku(s.code);
      if (repNorm === myNorm) continue;
      const parentPid = productIdBySku.get(repNorm); const pid = productIdBySku.get(myNorm);
      if (!parentPid || !pid) { parentMissing.push({ code: s.code, representative: s.representativeCode }); continue; }
      await db.query('update core.products set parent_product_id = $2 where product_id = $1 and (parent_product_id is distinct from $2)', [pid, parentPid]);
      parents++;
    }
    prodSec.notes.push(`variation parents set: ${parents}`);
    if (parentMissing.length) report.unresolved.variation_parent = parentMissing;
    log(`products: ${prodSec.applied} (new ${created.length}, parents ${parents}, parent missing ${parentMissing.length})`);

    // ── 3. sku_components ──
    const compSec = section(report, 'set_components', plan.setComponents.length);
    const compRows = []; const compKeys = new Set();
    for (const c of plan.setComponents) {
      const p = skuIds.get(normSku(c.parentCode)); const ch = skuIds.get(normSku(c.childCode));
      if (!p) { compSec.skipped.push({ parent: c.parentCode, child: c.childCode, reason: '親 SKU が無い' }); continue; }
      if (!ch) { compSec.skipped.push({ parent: c.parentCode, child: c.childCode, reason: '子 SKU が無い' }); continue; }
      if (p === ch) { compSec.skipped.push({ parent: c.parentCode, child: c.childCode, reason: '自分自身' }); continue; }
      const k = `${p}|${ch}`; if (compKeys.has(k)) { compSec.skipped.push({ parent: c.parentCode, child: c.childCode, reason: '重複' }); continue; } compKeys.add(k);
      if (!(c.qty > 0)) { compSec.skipped.push({ parent: c.parentCode, child: c.childCode, reason: `数量が不正 (${c.qty})` }); continue; }
      compRows.push({ company_id: COMPANY_ID, parent_sku_id: p, child_sku_id: ch, qty: c.qty, source: c.source || 'imported', created_by_type: 'system', created_by_id: runId });
    }
    const compRet = await insertMany(db, 'core.sku_components', ['company_id', 'parent_sku_id', 'child_sku_id', 'qty', 'source', 'created_by_type', 'created_by_id'], compRows,
      { onConflict: 'on conflict (parent_sku_id, child_sku_id) do update set qty = excluded.qty, source = excluded.source', returning: 'parent_sku_id' });
    compSec.applied = compRet.length;
    assertBalanced(compSec, 'set_components');
    if (compSec.skipped.length) report.unresolved.set_components = compSec.skipped;
    log(`set_components: ${compSec.applied} (skip ${compSec.skipped.length})`);

    // ── 4. sku_costs (有効行と違うときだけ付け替え) ──
    const costSec = section(report, 'sku_costs', plan.skus.filter((s) => s.cost && seenNorm.get(normSku(s.code)) === s.code).length);
    const active = new Map((await db.query('select sku_id, cost_jpy, cost_source, cost_status from core.sku_costs where valid_to is null')).rows.map((r) => [Number(r.sku_id), r]));
    const newCosts = [];
    for (const s of plan.skus) {
      if (!s.cost || seenNorm.get(normSku(s.code)) !== s.code) continue;
      const sid = skuIds.get(normSku(s.code));
      const cur = active.get(sid);
      const jpy = Math.round(Number(s.cost.jpy));
      if (!Number.isFinite(jpy) || jpy < 0) { costSec.skipped.push({ code: s.code, reason: `原価が数値でない (${s.cost.jpy})` }); continue; }
      if (cur && Number(cur.cost_jpy) === jpy && cur.cost_source === s.cost.source && cur.cost_status === s.cost.status) { costSec.applied++; continue; }   // 変わっていない
      // 同じ日に 2 回付け替えても valid_to >= valid_from を守る (前の行が今日始まりなら今日で閉じる)
      if (cur) await db.query("update core.sku_costs set valid_to = greatest(valid_from, $2::date - 1) where sku_id = $1 and valid_to is null", [sid, jstToday]);
      newCosts.push({ company_id: COMPANY_ID, sku_id: sid, cost_jpy: jpy, cost_source: s.cost.source, cost_status: s.cost.status, valid_from: jstToday, reason: `initial load ${runId}`, created_by_type: 'system', created_by_id: runId });
    }
    const costRet = await insertMany(db, 'core.sku_costs', ['company_id', 'sku_id', 'cost_jpy', 'cost_source', 'cost_status', 'valid_from', 'reason', 'created_by_type', 'created_by_id'], newCosts, { returning: 'sku_id' });
    costSec.applied += costRet.length;
    assertBalanced(costSec, 'sku_costs');
    log(`sku_costs: ${costSec.applied} (new rows ${costRet.length}, skip ${costSec.skipped.length})`);

    // ── 5. suppliers / supplier_skus ──
    const supSec = section(report, 'suppliers', (plan.suppliers || []).length);
    const supRet = await insertMany(db, 'core.suppliers', ['company_id', 'code', 'name', 'order_method', 'lead_time_days', 'created_by_type', 'created_by_id'],
      (plan.suppliers || []).filter((x) => normSku(x.code)).map((x) => ({ company_id: COMPANY_ID, code: x.code, name: x.name || x.code, order_method: x.orderMethod ?? null, lead_time_days: x.leadTimeDays ?? null, created_by_type: 'system', created_by_id: runId })),
      { onConflict: 'on conflict (company_id, code_norm) do update set name = excluded.name, order_method = coalesce(excluded.order_method, core.suppliers.order_method), lead_time_days = coalesce(excluded.lead_time_days, core.suppliers.lead_time_days)', returning: 'supplier_id, code_norm' });
    const supIds = new Map(supRet.map((r) => [r.code_norm, Number(r.supplier_id)]));
    for (const x of (plan.suppliers || [])) if (!normSku(x.code)) supSec.skipped.push({ code: x.code, reason: 'code が空' });
    supSec.applied = supRet.length;
    const ssSec = section(report, 'supplier_skus', (plan.supplierSkus || []).length);
    const ssRows = []; const ssKeys = new Set();
    for (const x of (plan.supplierSkus || [])) {
      const sup = supIds.get(normSku(x.supplierCode)); const sid = skuIds.get(normSku(x.skuCode));
      if (!sup) { ssSec.skipped.push({ supplier: x.supplierCode, sku: x.skuCode, reason: '仕入先が無い' }); continue; }
      if (!sid) { ssSec.skipped.push({ supplier: x.supplierCode, sku: x.skuCode, reason: 'SKU が無い' }); continue; }
      const k = `${sup}|${sid}`; if (ssKeys.has(k)) { ssSec.skipped.push({ supplier: x.supplierCode, sku: x.skuCode, reason: '重複' }); continue; } ssKeys.add(k);
      ssRows.push({ company_id: COMPANY_ID, supplier_id: sup, sku_id: sid, vendor_code: x.vendorCode ?? null, stock_units_per_order_unit: x.stockUnitsPerOrderUnit ?? null, min_order_qty: x.minOrderQty ?? null, order_multiple: x.orderMultiple ?? null, unit_cost_jpy: x.unitCostJpy ?? null, created_by_type: 'system', created_by_id: runId });
    }
    const ssRet = await insertMany(db, 'core.supplier_skus', ['company_id', 'supplier_id', 'sku_id', 'vendor_code', 'stock_units_per_order_unit', 'min_order_qty', 'order_multiple', 'unit_cost_jpy', 'created_by_type', 'created_by_id'], ssRows,
      { onConflict: 'on conflict (supplier_id, sku_id) do update set vendor_code = coalesce(excluded.vendor_code, core.supplier_skus.vendor_code), stock_units_per_order_unit = coalesce(excluded.stock_units_per_order_unit, core.supplier_skus.stock_units_per_order_unit), min_order_qty = coalesce(excluded.min_order_qty, core.supplier_skus.min_order_qty), order_multiple = coalesce(excluded.order_multiple, core.supplier_skus.order_multiple), unit_cost_jpy = coalesce(excluded.unit_cost_jpy, core.supplier_skus.unit_cost_jpy)', returning: 'sku_id' });
    ssSec.applied = ssRet.length;
    if (ssSec.skipped.length) report.unresolved.supplier_skus = ssSec.skipped.slice(0, 200);
    log(`suppliers: ${supSec.applied}, supplier_skus: ${ssSec.applied} (skip ${ssSec.skipped.length})`);

    // ── 6. listings + components + catalog_items (ASIN) + FNSKU ──
    const lstSec = section(report, 'listings', plan.listings.length);
    const lcSec = section(report, 'listing_components', plan.listings.reduce((n, l) => n + (l.components || []).length, 0));
    const lstRows = []; const lstSeen = new Set();
    for (const l of plan.listings) {
      const norm = normSku(l.listingCode);
      if (!norm) { lstSec.skipped.push({ mall: l.mall, code: l.listingCode, reason: 'code が空' }); continue; }
      const k = `${l.mall}|${l.shopCode || ''}|${norm}`;
      if (lstSeen.has(k)) { lstSec.skipped.push({ mall: l.mall, code: l.listingCode, reason: '正規化すると重複' }); continue; }
      lstSeen.add(k);
      lstRows.push({ company_id: COMPANY_ID, mall: l.mall, shop_code: l.shopCode || '', listing_code: l.listingCode, title: l.title ?? null, status: l.status || 'active', mall_item_id: l.mallItemId ?? null, created_by_type: 'system', created_by_id: runId });
    }
    const lstRet = await insertMany(db, 'core.listings', ['company_id', 'mall', 'shop_code', 'listing_code', 'title', 'status', 'mall_item_id', 'created_by_type', 'created_by_id'], lstRows,
      { onConflict: 'on conflict (mall, shop_code, listing_norm) do update set title = coalesce(excluded.title, core.listings.title), status = excluded.status, mall_item_id = coalesce(excluded.mall_item_id, core.listings.mall_item_id)', returning: 'listing_id, mall, shop_code, listing_norm' });
    const listingIds = new Map(lstRet.map((r) => [`${r.mall}|${r.shop_code}|${r.listing_norm}`, Number(r.listing_id)]));
    lstSec.applied = lstRet.length;
    assertBalanced(lstSec, 'listings');

    const lcRows = []; const lcUnresolved = []; const lcKeys = new Set();
    const asinBySeller = new Map();   // listing_id → {asin, sources:Set}
    const fnskuRows = [];
    for (const l of plan.listings) {
      const lid = listingIds.get(`${l.mall}|${l.shopCode || ''}|${normSku(l.listingCode)}`);
      if (!lid) continue;
      for (const c of (l.components || [])) {
        const sid = skuIds.get(normSku(c.code));
        if (!sid) { lcUnresolved.push({ mall: l.mall, listing: l.listingCode, code: c.code, reason: 'NE コードが無い' }); continue; }
        const k = `${lid}|${sid}`; if (lcKeys.has(k)) { lcUnresolved.push({ mall: l.mall, listing: l.listingCode, code: c.code, reason: '重複' }); continue; } lcKeys.add(k);
        if (!(c.qty > 0)) { lcUnresolved.push({ mall: l.mall, listing: l.listingCode, code: c.code, reason: `数量が不正 (${c.qty})` }); continue; }
        lcRows.push({ company_id: COMPANY_ID, listing_id: lid, sku_id: sid, qty: c.qty, resolution: c.resolution || 'imported', resolved_by_type: 'system', resolved_by_id: runId, evidence: c.evidence ? JSON.stringify(c.evidence) : null });
      }
      // ASIN の候補 (出どころごと)。先頭を採用、別の値があれば conflict に積む (両方は付けない)
      const cands = l.asinCandidates || (l.asin ? [{ asin: l.asin, source: l.asinSource || 'unknown' }] : []);
      for (const c of cands) {
        if (!c.asin) continue;
        const cur = asinBySeller.get(lid);
        if (cur && cur.asin !== c.asin) report.conflicts.push({ kind: 'asin', mall: l.mall, listing: l.listingCode, values: { [cur.source]: cur.asin, [c.source || 'unknown']: c.asin }, adopted: cur.asin });
        else if (!cur) asinBySeller.set(lid, { asin: c.asin, source: c.source || 'unknown', marketplace: l.marketplaceId || 'A1VC38T7YXB528' });
      }
      if (l.fnsku) fnskuRows.push({ company_id: COMPANY_ID, entity_type: 'listing', entity_id: lid, system: 'amazon', id_kind: 'fnsku', external_value: l.fnsku, resolution: 'imported', resolved_by_type: 'system', resolved_by_id: runId });
      for (const x of (l.externalIds || [])) if (x.value) fnskuRows.push({ company_id: COMPANY_ID, entity_type: 'listing', entity_id: lid, system: x.system, id_kind: x.kind, external_value: x.value, resolution: 'imported', resolved_by_type: 'system', resolved_by_id: runId });
    }
    const lcRet = await insertMany(db, 'core.listing_components', ['company_id', 'listing_id', 'sku_id', 'qty', 'resolution', 'resolved_by_type', 'resolved_by_id', 'evidence'], lcRows,
      { onConflict: 'on conflict (listing_id, sku_id) do update set qty = excluded.qty, resolution = excluded.resolution, evidence = excluded.evidence', returning: 'listing_id' });
    lcSec.applied = lcRet.length; lcSec.skipped = lcUnresolved;
    assertBalanced(lcSec, 'listing_components');
    if (lcUnresolved.length) report.unresolved.listing_components = lcUnresolved.slice(0, 500);
    log(`listings: ${lstSec.applied}, components: ${lcSec.applied} (unresolved ${lcUnresolved.length})`);

    // catalog_items (marketplace × ASIN) → listings.catalog_item_id
    const catSec = section(report, 'catalog_items', asinBySeller.size);
    const catRows = [...new Map([...asinBySeller.values()].map((v) => [`${v.marketplace}|${v.asin}`, v])).values()].map((v) => ({ marketplace_id: v.marketplace, asin: v.asin, last_seen_at: nowIso }));
    const catRet = await insertMany(db, 'core.catalog_items', ['marketplace_id', 'asin', 'last_seen_at'], catRows, { onConflict: 'on conflict (marketplace_id, asin) do update set last_seen_at = excluded.last_seen_at', returning: 'catalog_item_id, marketplace_id, asin' });
    const catIds = new Map(catRet.map((r) => [`${r.marketplace_id}|${r.asin}`, Number(r.catalog_item_id)]));
    let linked = 0;
    const linkPairs = [...asinBySeller.entries()].map(([lid, v]) => [lid, catIds.get(`${v.marketplace}|${v.asin}`)]).filter(([, cid]) => cid);
    for (let i = 0; i < linkPairs.length; i += CHUNK) {
      const chunk = linkPairs.slice(i, i + CHUNK); const params = [];
      const vals = chunk.map(([lid, cid]) => { params.push(lid, cid); return `($${params.length - 1}::bigint, $${params.length}::bigint)`; }).join(', ');
      const r = await db.query(`update core.listings l set catalog_item_id = v.cid from (values ${vals}) as v(lid, cid) where l.listing_id = v.lid and l.catalog_item_id is distinct from v.cid`, params);
      linked += r.rowCount ?? 0;
    }
    catSec.applied = catRet.length; catSec.notes.push(`listings linked/updated: ${linked}`);
    // listing の外部 ID (FNSKU・楽天の別名コード)。付け替えは valid_to
    const fnSec = section(report, 'listing_external_ids', fnskuRows.length);
    fnSec.applied = await upsertExternalIds(db, fnskuRows, report, 'listing_external_id');
    log(`catalog_items: ${catSec.applied}, fnsku: ${fnSec.applied}, asin conflicts: ${report.conflicts.filter((c) => c.kind === 'asin').length}`);

    // NE コードは sku の外部 ID としても登録 (03 §2.2)
    const neRows = [...skuIds.entries()].map(([norm, sid]) => ({ company_id: COMPANY_ID, entity_type: 'sku', entity_id: sid, system: 'ne', id_kind: 'product_code', external_value: seenNorm.get(norm), resolution: 'imported', resolved_by_type: 'system', resolved_by_id: runId }));
    const neSec = section(report, 'ne_codes', neRows.length);
    neSec.applied = await upsertExternalIds(db, neRows, report, 'ne_code');

    // ── 7. 観測 (append-only、on conflict do nothing) ──
    const obsSec = section(report, 'observations', (plan.observations || []).length);
    const productBySkuNorm = (norm) => productIdBySku.get(norm) ?? null;
    const obsRows = [];
    for (const o of (plan.observations || [])) {
      const norm = normSku(o.skuCode);
      const sid = skuIds.get(norm);
      if (!sid) { obsSec.skipped.push({ code: o.skuCode, attribute: o.attribute, reason: 'SKU が無い' }); continue; }
      // 商品属性 (jan / brand / 寸法…) は product に、SKU 固有 (税率など) は sku に
      const pid = productBySkuNorm(norm);
      const entityType = o.entity === 'sku' || !pid ? 'sku' : 'product';
      const entityId = entityType === 'product' ? pid : sid;
      const valueText = o.valueText ?? null; const valueNum = o.valueNum ?? null;
      if (valueText == null && valueNum == null) { obsSec.skipped.push({ code: o.skuCode, attribute: o.attribute, reason: '値が空' }); continue; }
      const contentHash = sha1(`${entityType}|${entityId}|${o.attribute}|${o.scope || 'item'}|${valueText}|${valueNum}|${o.unit || ''}|${o.source}`);
      obsRows.push({
        observation_key: `load:${o.source}:${norm}:${o.attribute}:${o.scope || 'item'}:${contentHash}`,
        entity_type: entityType, entity_id: entityId, attribute: o.attribute, packaging_scope: o.scope || 'item',
        value_text: valueText, value_num: valueNum, value_unit: o.unit ?? null, raw_text: o.rawText ?? (valueText ?? String(valueNum)),
        source_system: o.source, source_ref: o.sourceRef ?? null, observed_at: o.observedAt || nowIso, content_hash: contentHash,
      });
    }
    const obsRet = await insertMany(db, 'core.product_attribute_observations', ['observation_key', 'entity_type', 'entity_id', 'attribute', 'packaging_scope', 'value_text', 'value_num', 'value_unit', 'raw_text', 'source_system', 'source_ref', 'observed_at', 'content_hash'], obsRows,
      { onConflict: 'on conflict (observation_key) do nothing', returning: 'observation_id' });
    obsSec.applied = obsRows.length; obsSec.notes.push(`new rows: ${obsRet.length} (rest already observed)`);
    log(`observations: ${obsRows.length} (new ${obsRet.length}, skip ${obsSec.skipped.length})`);

    // ── 8. 解決 (規則 v1) → external_ids (jan) / products.brand,manufacturer / physicals 有効行 ──
    const resSec = section(report, 'resolutions', 0);
    const targets = ['jan', 'brand', 'manufacturer', 'unit_count', 'net_content', 'release_date'];
    const allObs = (await db.query(`select observation_id, entity_type, entity_id, attribute, packaging_scope, value_text, value_num, value_unit, source_system, observed_at
                                     from core.product_attribute_observations where attribute = any($1) and entity_type = 'product'`, [targets])).rows;
    const byKey = new Map();
    for (const o of allObs) {
      const k = `${o.entity_type}|${o.entity_id}|${o.attribute}|${o.packaging_scope}`;
      if (!byKey.has(k)) byKey.set(k, []);
      byKey.get(k).push(o);
    }
    const janWinners = []; const brandUpdates = []; const manufUpdates = []; const unitUpdates = []; const releaseUpdates = []; const netUpdates = [];
    const resRows = [];
    for (const [k, list] of byKey) {
      const [, entityId, attribute, scope] = k.split('|');
      const ranked = list.map((o) => ({ o, p: rulePriority.get(`${attribute}|${scope}|${o.source_system}`) })).filter((x) => x.p != null)
        .sort((a, b) => a.p - b.p || (new Date(b.o.observed_at) - new Date(a.o.observed_at)) || (Number(b.o.observation_id) - Number(a.o.observation_id)));
      if (!ranked.length) continue;
      const win = ranked[0].o;
      // 不一致 = 規則に載っている source どうしで最新値が違う
      const latestPerSource = new Map();
      for (const x of ranked) if (!latestPerSource.has(x.o.source_system)) latestPerSource.set(x.o.source_system, x.o.value_text ?? String(x.o.value_num));
      if (new Set(latestPerSource.values()).size > 1) report.conflicts.push({ kind: attribute, product_id: Number(entityId), values: Object.fromEntries(latestPerSource), adopted: latestPerSource.get(win.source_system) });
      resRows.push({ entity_type: 'product', entity_id: Number(entityId), attribute, packaging_scope: scope, resolved_observation_id: Number(win.observation_id), rule_version: RULE_VERSION });
      if (attribute === 'jan') janWinners.push({ product_id: Number(entityId), jan: win.value_text });
      else if (attribute === 'brand') brandUpdates.push([Number(entityId), win.value_text]);
      else if (attribute === 'manufacturer') manufUpdates.push([Number(entityId), win.value_text]);
      else if (attribute === 'unit_count') unitUpdates.push([Number(entityId), Number(win.value_num), win.value_unit]);
      else if (attribute === 'net_content') netUpdates.push([Number(entityId), Number(win.value_num), win.value_unit]);
      else if (attribute === 'release_date') releaseUpdates.push([Number(entityId), win.value_text]);
    }
    await insertMany(db, 'core.attribute_resolutions', ['entity_type', 'entity_id', 'attribute', 'packaging_scope', 'resolved_observation_id', 'rule_version'], resRows,
      { onConflict: 'on conflict (entity_type, entity_id, attribute, packaging_scope) do update set resolved_observation_id = excluded.resolved_observation_id, rule_version = excluded.rule_version, resolved_at = now()' });
    resSec.expected = resRows.length; resSec.applied = resRows.length;
    for (const [pid, v] of brandUpdates) await db.query('update core.products set brand = $2 where product_id = $1 and brand is distinct from $2', [pid, v]);
    for (const [pid, v] of manufUpdates) await db.query('update core.products set manufacturer = $2 where product_id = $1 and manufacturer is distinct from $2', [pid, v]);
    for (const [pid, n, u] of unitUpdates) if (Number.isInteger(n) && n > 0) await db.query('update core.products set unit_count = $2, unit_count_uom = $3 where product_id = $1 and (unit_count is distinct from $2 or unit_count_uom is distinct from $3)', [pid, n, u]);
    for (const [pid, n, u] of netUpdates) if (Number.isFinite(n) && n >= 0) await db.query('update core.products set net_content = $2, net_content_uom = $3 where product_id = $1 and (net_content is distinct from $2 or net_content_uom is distinct from $3)', [pid, n, u]);
    for (const [pid, v] of releaseUpdates) if (/^\d{4}-\d{2}-\d{2}$/.test(v || '')) await db.query('update core.products set release_date = $2 where product_id = $1 and release_date is distinct from $2', [pid, v]);
    // JAN → external_ids (product)。同じ JAN が別 product に有効なら conflict (両方には付けない)
    const janRows = janWinners.filter((j) => /^\d{8}$|^\d{13}$/.test(j.jan || '')).map((j) => ({ company_id: COMPANY_ID, entity_type: 'product', entity_id: j.product_id, system: 'jan', id_kind: 'jan', external_value: j.jan, resolution: 'imported', resolved_by_type: 'system', resolved_by_id: runId }));
    const janSec = section(report, 'jan', janRows.length);
    janSec.applied = await upsertExternalIds(db, janRows, report, 'jan');
    janSec.notes.push(`jan winners: ${janWinners.length}, valid format: ${janRows.length}`);
    log(`resolutions: ${resRows.length}, jan external ids: ${janSec.applied}, attribute conflicts: ${report.conflicts.filter((c) => c.kind !== 'asin').length}`);

    // ── 9. 物理属性 (行を出どころごとに入れ、規則で有効行を 1 つ) ──
    const phySec = section(report, 'physicals', (plan.physicals || []).length);
    const phyRows = [];
    for (const p of (plan.physicals || [])) {
      const pid = productBySkuNorm(normSku(p.skuCode));
      if (!pid) { phySec.skipped.push({ code: p.skuCode, reason: '単品 product が無い (セット・例外には物理属性を付けない)' }); continue; }
      if (!(p.weightG > 0 || p.lengthMm > 0)) { phySec.skipped.push({ code: p.skuCode, reason: '値が無い' }); continue; }
      phyRows.push({ company_id: COMPANY_ID, product_id: pid, scope: p.scope || 'package', length_mm: p.lengthMm ?? null, width_mm: p.widthMm ?? null, height_mm: p.heightMm ?? null, weight_g: p.weightG ?? null, source_system: p.source, source_ref: p.sourceRef ?? null, is_measured: !!p.isMeasured, observed_at: p.observedAt || nowIso, created_by_type: 'system', created_by_id: runId });
    }
    // 同じ product × scope × source の既存行は消さず、値が同じなら入れない (append 的に増えないようにする)
    const existingPhy = (await db.query("select product_id, scope, source_system, weight_g, length_mm from core.product_physicals")).rows;
    const phySeen = new Set(existingPhy.map((r) => `${r.product_id}|${r.scope}|${r.source_system}|${r.weight_g}|${r.length_mm}`));
    const phyNew = phyRows.filter((r) => !phySeen.has(`${r.product_id}|${r.scope}|${r.source_system}|${r.weight_g}|${r.length_mm}`));
    await insertMany(db, 'core.product_physicals', ['company_id', 'product_id', 'scope', 'length_mm', 'width_mm', 'height_mm', 'weight_g', 'source_system', 'source_ref', 'is_measured', 'observed_at', 'created_by_type', 'created_by_id'], phyNew);
    phySec.applied = phyRows.length; phySec.notes.push(`new rows: ${phyNew.length}`);
    // 有効行: scope ごとに規則 (package_weight_g の priority) で選ぶ
    const cand = (await db.query('select product_physical_id, product_id, scope, source_system, observed_at from core.product_physicals')).rows;
    const bestByPs = new Map();
    for (const r of cand) {
      const p = rulePriority.get(`package_weight_g|${r.scope}|${r.source_system}`) ?? rulePriority.get(`package_weight_g|package|${r.source_system}`);
      if (p == null) continue;
      const k = `${r.product_id}|${r.scope}`;
      const cur = bestByPs.get(k);
      if (!cur || p < cur.p || (p === cur.p && new Date(r.observed_at) > new Date(cur.r.observed_at))) bestByPs.set(k, { p, r });
    }
    await db.query('update core.product_physicals set is_effective = false where is_effective');
    for (const { r } of bestByPs.values()) await db.query('update core.product_physicals set is_effective = true where product_physical_id = $1', [r.product_physical_id]);
    phySec.notes.push(`effective rows: ${bestByPs.size}`);
    log(`physicals: ${phyRows.length} (new ${phyNew.length}, effective ${bestByPs.size})`);

    // ── 10. compliance ──
    const cmpSec = section(report, 'compliance', (plan.compliance || []).length);
    const cmpRows = [];
    for (const c of (plan.compliance || [])) {
      const pid = productBySkuNorm(normSku(c.skuCode));
      if (!pid) { cmpSec.skipped.push({ code: c.skuCode, reason: '単品 product が無い' }); continue; }
      cmpRows.push({ product_id: pid, company_id: COMPANY_ID, ingredients: c.ingredients ?? null, precautions: c.precautions ?? null, distributor: c.distributor ?? null, manufacturer_jp: c.manufacturerJp ?? null, allergens: c.allergens ?? null, source_system: c.source, source_ref: c.sourceRef ?? null, created_by_type: 'system', created_by_id: runId });
    }
    const cmpRet = await insertMany(db, 'core.product_compliance', ['product_id', 'company_id', 'ingredients', 'precautions', 'distributor', 'manufacturer_jp', 'allergens', 'source_system', 'source_ref', 'created_by_type', 'created_by_id'], cmpRows,
      { onConflict: 'on conflict (product_id) do update set ingredients = coalesce(excluded.ingredients, core.product_compliance.ingredients), precautions = coalesce(excluded.precautions, core.product_compliance.precautions), distributor = coalesce(excluded.distributor, core.product_compliance.distributor), manufacturer_jp = coalesce(excluded.manufacturer_jp, core.product_compliance.manufacturer_jp), allergens = coalesce(excluded.allergens, core.product_compliance.allergens), source_system = excluded.source_system', returning: 'product_id' });
    cmpSec.applied = cmpRet.length;

    // ── 11. workers ──
    const wkSec = section(report, 'workers', (plan.workers || []).length);
    const wkRet = await insertMany(db, 'core.workers', ['company_id', 'staff_no', 'display_name', 'login_email', 'worker_type', 'active', 'created_by_type', 'created_by_id'],
      (plan.workers || []).map((w) => ({ company_id: w.companyId || COMPANY_ID, staff_no: w.staffNo ?? null, display_name: w.displayName, login_email: w.loginEmail || null, worker_type: w.workerType || 'employee', active: w.active !== false, created_by_type: 'system', created_by_id: runId })),
      { onConflict: 'on conflict (company_id, staff_no) do update set display_name = excluded.display_name, login_email = excluded.login_email, worker_type = excluded.worker_type, active = excluded.active', returning: 'worker_id' });
    wkSec.applied = wkRet.length;

    // ── 12. 記録 ──
    report.finished_at = new Date().toISOString();
    report.ok = true;
    const summary = Object.fromEntries(Object.entries(report.sections).map(([k, v]) => [k, { expected: v.expected, applied: v.applied, skipped: v.skipped.length }]));
    report.summary = summary;
    await db.query(`insert into ops.ingest_runs (ingest_run_id, source_system, entity, scope_key, host, started_at, finished_at, status, complete, rows_seen, rows_inserted, rows_skipped, source_tz, checksum, format_version)
                    values ($1, 'sqlite_initial_load', 'products', 'render', $2, $3, $4, $5, true, $6, $7, $8, 'UTC', $9, 'plan-v1')
                    on conflict (ingest_run_id) do nothing`,
      [runId, opts.host || 'unknown', report.started_at, report.finished_at, dryRun ? 'partial' : 'success',
        Object.values(summary).reduce((n, s) => n + s.expected, 0), Object.values(summary).reduce((n, s) => n + s.applied, 0), Object.values(summary).reduce((n, s) => n + s.skipped, 0),
        sha1(JSON.stringify(summary))]);
    if (dryRun) { await db.exec('rollback'); log('dry-run: 全部やってから巻き戻した'); }
    else { await db.exec('commit'); log('commit'); }
    return report;
  } catch (e) {
    try { await db.exec('rollback'); } catch { /* 接続が死んでいれば rollback も失敗 */ }
    report.ok = false; report.error = String(e.message); report.error_code = e.code || null;
    throw Object.assign(e, { report });
  }
}

/** 外部 ID の upsert: 同じ (system, id_kind, 値) が別エンティティに有効なら conflict に積んで付けない。同じエンティティに別の値が有効なら valid_to を埋めて付け替え */
async function upsertExternalIds(db, rows, report, label) {
  if (!rows.length) return 0;
  let applied = 0;
  const systems = [...new Set(rows.map((r) => `${r.system}|${r.id_kind}`))];
  const activeByValue = new Map(); const activeByEntity = new Map();
  for (const sk of systems) {
    const [system, kind] = sk.split('|');
    const rowsA = (await db.query('select external_id_row, entity_type, entity_id, external_value, external_norm from core.external_ids where system = $1 and id_kind = $2 and valid_to is null', [system, kind])).rows;
    for (const r of rowsA) { activeByValue.set(`${sk}|${r.external_norm}`, r); activeByEntity.set(`${sk}|${r.entity_type}|${r.entity_id}`, r); }
  }
  const toInsert = [];
  for (const r of rows) {
    const sk = `${r.system}|${r.id_kind}`; const norm = normSku(r.external_value);
    if (!norm) continue;
    const byVal = activeByValue.get(`${sk}|${norm}`);
    if (byVal && !(byVal.entity_type === r.entity_type && Number(byVal.entity_id) === Number(r.entity_id))) {
      report.conflicts.push({ kind: `${label}_taken`, value: r.external_value, held_by: { type: byVal.entity_type, id: Number(byVal.entity_id) }, wanted_by: { type: r.entity_type, id: Number(r.entity_id) } });
      continue;
    }
    if (byVal) { applied++; continue; }   // 既に同じ
    const byEnt = activeByEntity.get(`${sk}|${r.entity_type}|${r.entity_id}`);
    if (byEnt) {
      await db.query('update core.external_ids set valid_to = now() where external_id_row = $1', [byEnt.external_id_row]);
      report.conflicts.push({ kind: `${label}_replaced`, entity: { type: r.entity_type, id: Number(r.entity_id) }, old: byEnt.external_value, new: r.external_value });
      activeByValue.delete(`${sk}|${byEnt.external_norm}`);
    }
    activeByValue.set(`${sk}|${norm}`, { entity_type: r.entity_type, entity_id: r.entity_id, external_norm: norm });
    toInsert.push(r);
  }
  await insertMany(db, 'core.external_ids', ['company_id', 'entity_type', 'entity_id', 'system', 'id_kind', 'external_value', 'resolution', 'resolved_by_type', 'resolved_by_id'], toInsert);
  return applied + toInsert.length;
}

/** report を人が読める短い Markdown に */
export function reportToMarkdown(report) {
  const lines = [`# Company DB 初期ロード ${report.run_id} (${report.dry_run ? 'dry-run' : '本適用'}) ${report.ok ? 'OK' : 'FAILED'}`, ''];
  lines.push('| 区分 | 予定 | 投入 | skip |', '|---|---|---|---|');
  for (const [k, v] of Object.entries(report.sections)) lines.push(`| ${k} | ${v.expected} | ${v.applied} | ${v.skipped.length}${v.notes.length ? ' (' + v.notes.join('; ') + ')' : ''} |`);
  const byKind = {};
  for (const c of report.conflicts) byKind[c.kind] = (byKind[c.kind] || 0) + 1;
  lines.push('', `不一致: ${Object.entries(byKind).map(([k, n]) => `${k}=${n}`).join(', ') || 'なし'}`);
  for (const [k, v] of Object.entries(report.unresolved || {})) lines.push(`未解決 ${k}: ${v.length} 件 (先頭: ${JSON.stringify(v[0])})`);
  if (report.error) lines.push('', `エラー: ${report.error}`);
  return lines.join('\n');
}
