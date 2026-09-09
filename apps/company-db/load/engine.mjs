/**
 * engine.mjs — Company DB 初期ロードの本体 (「ロード計画」→ PostgreSQL)。Company DB構想 06 §5.6 / §7.1 PR-B
 *
 * 入口は sources.mjs が作る「ロード計画 (plan)」= 出どころに依存しない素の配列。ここは Postgres に入れるだけ。
 * 試験は plan を手で組んで PGlite に流す (SQLite の実ファイルは要らない)。
 *
 * 約束 (03 §1 / 06 §5.6 / Codex 総合意見 + PR-B レビュー R1・R2):
 *   - 1 回のロード = 1 トランザクション。途中で失敗したら全部巻き戻す (半端な状態を残さない)
 *   - 冪等: 何度流しても同じ結果 (upsert / 観測の再送は入れない / 有効期間の付け替え)
 *   - 🚨 全ての区分で「予定 = 投入 + 既存と同じ + 理由つき skip」を照合し、合わなければ巻き戻す (fail-close)。
 *     予定は「除外する前」の件数 (取り合い・不採用も理由つき skip として数える)
 *   - 親不在 (構成の子 SKU が無い、出品の NE コードが無い) は skip の理由として report に残す。黙って落とさない
 *   - 正規化衝突で落とした SKU / listing は、以降の処理 (構成・属性・親・外部 ID・listingRef) でも一切使わない (隔離 = 原文のコードが一致するときだけ)
 *   - 属性は「観測」として全部残し、解決規則 (rule_version) で 1 つ選ぶ。不一致は report.conflicts (= 所見の種)
 *   - 観測の再送: 出どころの時刻がある入力は「同じ出どころ・同じ参照・同じ内容・同じ観測時刻」が既にあれば再送 (入れない)。
 *     時刻が無い入力 (observedAt null) は「その出どころ・参照の最新の観測と同じ内容」なら再送、違えばロード時刻で新しい観測。
 *     A→B→A も、同じ内容を新しい時刻で観測し直したものも残る
 *   - ASIN は product に直付けしない (catalog_items 経由)。JAN は product、FNSKU / 楽天別名は listing、NE コードは sku
 *   - 外部 ID は「先に全部読み、全部の要求を集め、移動計画を固定点で解いてから、閉じる → 付ける」。取り合い (同じ値を複数が要求) は誰にも付けない。
 *     別のエンティティが持っている値は、持ち主が今回 別の値へ移れる (= その要求が通る) ときだけ手放す。人が付けた行 (manual) は閉じない
 *   - 今回「完全に読めた」対象 (plan に構成が 1 行以上あり、skip が 1 件も無い listing / セット親) の構成だけ plan に合わせる: plan に無い行は消す。
 *     読めなかった・空・未解決のときは触らない。人が手で確定した行 (manual) は消さず、数量が違えば conflict + skip
 *
 * plan の形 (sources.mjs / test を参照):
 *   { skus:[{code,name,kind,taxRate,taxClass,handling,salesClass,representativeCode,cost:{jpy,source,status}|null}],
 *     setComponents:[{parentCode,childCode,qty,source}],
 *     listings:[{mall,shopCode,listingCode,mallItemId,title,status,components:[{code,qty,resolution,evidence}],
 *                asinCandidates:[{asin,source}],fnskuCandidates:[{fnsku,source}],fnskuCleared,externalIds:[{system,kind,value}],marketplaceId}],
 *     observations:[{skuCode | listingRef:{mall,shopCode,listingCode}, attribute,scope,valueText,valueNum,unit,rawText,source,sourceRef,observedAt|null}],
 *     physicals:[{skuCode,scope,lengthMm,widthMm,heightMm,weightG,unitsPerCase,source,sourceRef,isMeasured,observedAt|null,via:{fnsku,listing}}],
 *     compliance:[{skuCode,ingredients,precautions,distributor,manufacturerJp,allergens,source,sourceRef}],
 *     suppliers:[{code,name,orderMethod,leadTimeDays}], supplierSkus:[{supplierCode,skuCode,vendorCode,stockUnitsPerOrderUnit,minOrderQty,orderMultiple,unitCostJpy}],
 *     workers:[{staffNo,displayName,loginEmail,workerType,active,companyId}], sources:{...} }
 */
import crypto from 'node:crypto';
import { normSku } from '../../../lib/sku-norm.js';

export const COMPANY_ID = 1;
export const RULE_VERSION = 'v1';
/** ASIN の出どころの優先 (06 §5.6: 出品一覧 asin1 → fba_sku_attrs → Sheet → fees)。listing_report は PR-D で raw 層が入ってから */
export const ASIN_SOURCE_PRIORITY = ['listing_report', 'fba_sku_attrs', 'fba_sheet_import', 'amazon_fees'];
export const FNSKU_SOURCE_PRIORITY = ['fba_sku_attrs', 'fba_sheet_import', 'listing_report'];
const CHUNK = 400;

export function newLoadRunId() {
  return `load_${new Date().toISOString().replace(/[-:.TZ]/g, '').slice(0, 15)}_${crypto.randomBytes(3).toString('hex')}`;
}
function sha1(s) { return crypto.createHash('sha1').update(String(s)).digest('hex').slice(0, 16); }
const listingKey = (mall, shopCode, code) => `${mall}|${shopCode || ''}|${normSku(code)}`;
const ms = (v) => (v == null ? null : +new Date(v));

/** 候補の配列から優先順で 1 つ (sources.mjs と共有。同じ規則で採用しないと重量の逆引きがずれる) */
export function pickByPriority(cands, priority, valueKey) {
  const list = cands.filter((c) => c && c[valueKey]);
  if (!list.length) return null;
  const rank = (s) => (priority.indexOf(s) < 0 ? 99 : priority.indexOf(s));
  return [...list].sort((a, b) => rank(a.source) - rank(b.source))[0];
}

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
/** (type, id) の組で絞る where 句 */
const pairIn = (typeCol, idCol, n1, n2) => `(${typeCol}, ${idCol}) in (select unnest($${n1}::text[]), unnest($${n2}::bigint[]))`;

/** 区分ごとの帳尻: expected = applied (新規/更新) + same (既存と同じ) + skipped.length */
function section(report, name, expected) {
  const s = { expected, applied: 0, same: 0, skipped: [], notes: [] };
  report.sections[name] = s;
  return s;
}
function assertBalanced(report) {
  for (const [name, s] of Object.entries(report.sections)) {
    if (s.expected !== s.applied + s.same + s.skipped.length) {
      throw Object.assign(new Error(`${name}: 予定 ${s.expected} ≠ 投入 ${s.applied} + 既存同 ${s.same} + skip ${s.skipped.length} (無音の欠落。巻き戻す)`), { code: 'LOAD_UNBALANCED', section: name });
    }
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
  // 🚨 未来の観測時刻は異常 (時計ずれ・入力ミス)。入れないし、「最新」の選択からも外す (未来の行が最新に居座ると、時刻の無い再送が毎回増える。Codex R4-2)
  const futureLimit = now.getTime() + 5 * 60 * 1000;
  const isFuture = (v) => v != null && ms(v) > futureLimit;
  const report = { run_id: runId, dry_run: dryRun, started_at: nowIso, sections: {}, conflicts: [], unresolved: {}, ok: false };
  const addUnresolved = (k, v) => { (report.unresolved[k] ||= []).push(v); };

  await db.exec('begin');
  try {
    const rules = (await db.query('select attribute, packaging_scope, source_system, priority from core.attribute_resolution_rules where rule_version = $1', [RULE_VERSION])).rows;
    const rulePriority = new Map(rules.map((r) => [`${r.attribute}|${r.packaging_scope}|${r.source_system}`, r.priority]));

    // ── 1. 予定の確定 (正規化衝突は先に落とし、以降は accepted だけを使う) ──
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
    const isAcceptedCode = (code) => code != null && seenNorm.get(normSku(code)) === code;

    // ── 2. products (単品 SKU に 1:1)。skus の CHECK (単品は product 必須) があるので product を先に作る ──
    const prodSec = section(report, 'products', accepted.filter((s) => s.kind === 'single').length);
    const existing = new Map((await db.query('select s.code_norm, s.product_id from core.skus s where s.company_id = $1 and s.product_id is not null', [COMPANY_ID])).rows.map((r) => [r.code_norm, Number(r.product_id)]));
    const productIdBySku = new Map(existing);
    const toCreate = accepted.filter((s) => s.kind === 'single' && !existing.has(normSku(s.code)));
    const toCreateSet = new Set(toCreate);
    const created = await insertMany(db, 'core.products', ['company_id', 'display_code', 'name', 'sales_class', 'status', 'created_by_type', 'created_by_id'],
      toCreate.map((s) => ({ company_id: COMPANY_ID, display_code: s.code, name: s.name || s.code, sales_class: s.salesClass ?? null, status: s.handling === 'discontinued' ? 'discontinued' : 'active', created_by_type: 'system', created_by_id: runId })),
      { returning: 'product_id, display_code' });
    for (const r of created) productIdBySku.set(normSku(r.display_code), Number(r.product_id));
    prodSec.applied = created.length;
    // 既存 product の名前・状態・分類の追随は 1 文で (逐次 UPDATE を避ける)
    const upd = accepted.filter((s) => s.kind === 'single' && !toCreateSet.has(s) && productIdBySku.has(normSku(s.code)));
    let prodUpdated = 0;
    for (let i = 0; i < upd.length; i += CHUNK) {
      const chunk = upd.slice(i, i + CHUNK); const params = [];
      const vals = chunk.map((s) => { params.push(productIdBySku.get(normSku(s.code)), s.name || s.code, s.salesClass ?? null, s.handling === 'discontinued' ? 'discontinued' : 'active'); return `($${params.length - 3}::bigint, $${params.length - 2}::text, $${params.length - 1}::smallint, $${params.length}::text)`; }).join(', ');
      const r = await db.query(`update core.products p set name = v.name, sales_class = v.sc, status = v.st from (values ${vals}) as v(pid, name, sc, st) where p.product_id = v.pid and p.company_id = ${COMPANY_ID} and (p.name is distinct from v.name or p.sales_class is distinct from v.sc or p.status is distinct from v.st)`, params);
      prodUpdated += r.rowCount ?? 0;
    }
    prodSec.applied += prodUpdated; prodSec.same = upd.length - prodUpdated;

    // ── 3. skus (upsert by company + code_norm。単品は product_id つき) ──
    const skuRows = accepted.map((s) => ({
      company_id: COMPANY_ID, product_id: s.kind === 'single' ? productIdBySku.get(normSku(s.code)) : null,
      sku_kind: s.kind, code: s.code, name: s.name || s.code,
      tax_rate: s.taxRate ?? null, tax_class: s.taxClass ?? null, handling: s.handling || 'unknown',
      created_by_type: 'system', created_by_id: runId,
    }));
    const skuIds = new Map();   // code_norm → sku_id (accepted のみ)
    const returned = await insertMany(db, 'core.skus', ['company_id', 'product_id', 'sku_kind', 'code', 'name', 'tax_rate', 'tax_class', 'handling', 'created_by_type', 'created_by_id'], skuRows, {
      onConflict: 'on conflict (company_id, code_norm) do update set name = excluded.name, sku_kind = excluded.sku_kind, tax_rate = excluded.tax_rate, tax_class = excluded.tax_class, handling = excluded.handling, product_id = coalesce(core.skus.product_id, excluded.product_id)',
      returning: 'sku_id, code_norm',
    });
    for (const r of returned) skuIds.set(r.code_norm, Number(r.sku_id));
    skuSec.applied = returned.length;
    const skuIdOf = (code) => (isAcceptedCode(code) ? skuIds.get(normSku(code)) : undefined);
    const productIdOf = (code) => (isAcceptedCode(code) ? productIdBySku.get(normSku(code)) : undefined);
    const productIdsInRun = [...new Set(accepted.map((s) => productIdOf(s.code)).filter(Boolean))];
    log(`skus: ${skuSec.applied} (skip ${skuSec.skipped.length}), products: new ${created.length} / updated ${prodUpdated}`);

    // バリエーション親 (代表商品コードが自分以外の単品 SKU を指すとき)。accepted の行だけ
    let parents = 0;
    const parentPairs = [];
    for (const s of accepted) {
      if (s.kind !== 'single' || !s.representativeCode) continue;
      if (normSku(s.representativeCode) === normSku(s.code)) continue;
      const parentPid = productIdOf(s.representativeCode); const pid = productIdOf(s.code);
      if (!parentPid || !pid) { addUnresolved('variation_parent', { code: s.code, representative: s.representativeCode }); continue; }
      parentPairs.push([pid, parentPid]);
    }
    for (let i = 0; i < parentPairs.length; i += CHUNK) {
      const chunk = parentPairs.slice(i, i + CHUNK); const params = [];
      const vals = chunk.map(([pid, pp]) => { params.push(pid, pp); return `($${params.length - 1}::bigint, $${params.length}::bigint)`; }).join(', ');
      const r = await db.query(`update core.products p set parent_product_id = v.pp from (values ${vals}) as v(pid, pp) where p.product_id = v.pid and p.parent_product_id is distinct from v.pp`, params);
      parents += r.rowCount ?? 0;
    }
    prodSec.notes.push(`variation parents set: ${parents}`);

    // ── 4. sku_components (完全に読めた親だけ plan に合わせる。manual は残し、数量が違えば conflict) ──
    const compSec = section(report, 'set_components', plan.setComponents.length);
    const compCand = []; const compKeys = new Set(); const parentsWithSkip = new Set();
    for (const c of plan.setComponents) {
      const p = skuIdOf(c.parentCode); const ch = skuIdOf(c.childCode);
      if (!p) { compSec.skipped.push({ parent: c.parentCode, child: c.childCode, reason: '親 SKU が無い (または正規化衝突で落とした)' }); continue; }
      if (!ch) { compSec.skipped.push({ parent: c.parentCode, child: c.childCode, reason: '子 SKU が無い (または正規化衝突で落とした)' }); parentsWithSkip.add(p); continue; }
      if (p === ch) { compSec.skipped.push({ parent: c.parentCode, child: c.childCode, reason: '自分自身' }); parentsWithSkip.add(p); continue; }
      const k = `${p}|${ch}`; if (compKeys.has(k)) { compSec.skipped.push({ parent: c.parentCode, child: c.childCode, reason: '重複' }); parentsWithSkip.add(p); continue; } compKeys.add(k);
      if (!(c.qty > 0)) { compSec.skipped.push({ parent: c.parentCode, child: c.childCode, reason: `数量が不正 (${c.qty})` }); parentsWithSkip.add(p); continue; }
      compCand.push({ company_id: COMPANY_ID, parent_sku_id: p, child_sku_id: ch, qty: c.qty, source: c.source || 'imported', created_by_type: 'system', created_by_id: runId, _parent: c.parentCode, _child: c.childCode });
    }
    const compParents = [...new Set(compCand.map((r) => r.parent_sku_id))];
    const compManual = new Map();
    if (compParents.length) for (const r of (await db.query("select parent_sku_id, child_sku_id, qty from core.sku_components where source = 'manual' and parent_sku_id = any($1::bigint[])", [compParents])).rows) compManual.set(`${r.parent_sku_id}|${r.child_sku_id}`, Number(r.qty));
    const compRows = [];
    for (const r of compCand) {
      const mq = compManual.get(`${r.parent_sku_id}|${r.child_sku_id}`);
      if (mq === undefined) { compRows.push(r); continue; }
      if (mq === Number(r.qty)) { compSec.same++; continue; }
      compSec.skipped.push({ parent: r._parent, child: r._child, reason: `人が確定した行 (manual, 数量 ${mq}) と数量が違う (${r.qty})` });
      report.conflicts.push({ kind: 'set_component_manual_mismatch', parent_sku_id: r.parent_sku_id, child_sku_id: r.child_sku_id, manual_qty: mq, plan_qty: r.qty });
      parentsWithSkip.add(r.parent_sku_id);
    }
    const compRet = await insertMany(db, 'core.sku_components', ['company_id', 'parent_sku_id', 'child_sku_id', 'qty', 'source', 'created_by_type', 'created_by_id'], compRows,
      { onConflict: "on conflict (parent_sku_id, child_sku_id) do update set qty = excluded.qty, source = excluded.source where core.sku_components.source <> 'manual'", returning: 'parent_sku_id' });
    compSec.applied = compRet.length;
    // 完全に読めた親 = plan に構成が 1 行以上あり、skip が無い。それ以外 (空・読めない・未解決) は触らない
    const pruneParents = compParents.filter((p) => !parentsWithSkip.has(p));
    if (pruneParents.length) {
      const stale = (await db.query('select parent_sku_id, child_sku_id, source from core.sku_components where parent_sku_id = any($1::bigint[])', [pruneParents])).rows.filter((r) => !compKeys.has(`${r.parent_sku_id}|${r.child_sku_id}`));
      const del = stale.filter((r) => r.source !== 'manual');
      for (const r of stale.filter((r) => r.source === 'manual')) report.conflicts.push({ kind: 'set_component_manual_kept', parent_sku_id: Number(r.parent_sku_id), child_sku_id: Number(r.child_sku_id) });
      if (del.length) await db.query(`delete from core.sku_components where (parent_sku_id, child_sku_id) in (select unnest($1::bigint[]), unnest($2::bigint[]))`, [del.map((r) => r.parent_sku_id), del.map((r) => r.child_sku_id)]);
      compSec.notes.push(`stale removed: ${del.length}`);
    }
    if (compSec.skipped.length) report.unresolved.set_components = compSec.skipped;
    log(`set_components: ${compSec.applied} (same ${compSec.same}, skip ${compSec.skipped.length})`);

    // ── 5. sku_costs (有効行と違うときだけ付け替え) ──
    const costSec = section(report, 'sku_costs', accepted.filter((s) => s.cost).length);
    const active = new Map((await db.query('select sku_id, cost_jpy, cost_source, cost_status from core.sku_costs where valid_to is null')).rows.map((r) => [Number(r.sku_id), r]));
    const newCosts = []; const closeIds = [];
    for (const s of accepted) {
      if (!s.cost) continue;
      const sid = skuIdOf(s.code);
      const cur = active.get(sid);
      const jpy = Math.round(Number(s.cost.jpy));
      if (!Number.isFinite(jpy) || jpy < 0) { costSec.skipped.push({ code: s.code, reason: `原価が数値でない (${s.cost.jpy})` }); continue; }
      if (cur && Number(cur.cost_jpy) === jpy && cur.cost_source === s.cost.source && cur.cost_status === s.cost.status) { costSec.same++; continue; }
      if (cur) closeIds.push(sid);
      newCosts.push({ company_id: COMPANY_ID, sku_id: sid, cost_jpy: jpy, cost_source: s.cost.source, cost_status: s.cost.status, valid_from: jstToday, reason: `initial load ${runId}`, created_by_type: 'system', created_by_id: runId });
    }
    // 同じ日に 2 回付け替えても valid_to >= valid_from を守る (前の行が今日始まりなら今日で閉じる)
    if (closeIds.length) await db.query('update core.sku_costs set valid_to = greatest(valid_from, $2::date - 1) where sku_id = any($1::bigint[]) and valid_to is null', [closeIds, jstToday]);
    const costRet = await insertMany(db, 'core.sku_costs', ['company_id', 'sku_id', 'cost_jpy', 'cost_source', 'cost_status', 'valid_from', 'reason', 'created_by_type', 'created_by_id'], newCosts, { returning: 'sku_id' });
    costSec.applied = costRet.length;
    log(`sku_costs: new ${costSec.applied}, same ${costSec.same}, skip ${costSec.skipped.length}`);

    // ── 6. suppliers / supplier_skus ──
    const supSec = section(report, 'suppliers', (plan.suppliers || []).length);
    const supRowsIn = (plan.suppliers || []).filter((x) => { if (normSku(x.code)) return true; supSec.skipped.push({ code: x.code, reason: 'code が空' }); return false; });
    const supRet = await insertMany(db, 'core.suppliers', ['company_id', 'code', 'name', 'order_method', 'lead_time_days', 'created_by_type', 'created_by_id'],
      supRowsIn.map((x) => ({ company_id: COMPANY_ID, code: x.code, name: x.name || x.code, order_method: x.orderMethod ?? null, lead_time_days: x.leadTimeDays ?? null, created_by_type: 'system', created_by_id: runId })),
      { onConflict: 'on conflict (company_id, code_norm) do update set name = excluded.name, order_method = coalesce(excluded.order_method, core.suppliers.order_method), lead_time_days = coalesce(excluded.lead_time_days, core.suppliers.lead_time_days)', returning: 'supplier_id, code_norm' });
    const supIds = new Map(supRet.map((r) => [r.code_norm, Number(r.supplier_id)]));
    supSec.applied = supRet.length;
    const ssSec = section(report, 'supplier_skus', (plan.supplierSkus || []).length);
    const ssRows = []; const ssKeys = new Set();
    for (const x of (plan.supplierSkus || [])) {
      const sup = supIds.get(normSku(x.supplierCode)); const sid = skuIdOf(x.skuCode);
      if (!sup) { ssSec.skipped.push({ supplier: x.supplierCode, sku: x.skuCode, reason: '仕入先が無い' }); continue; }
      if (!sid) { ssSec.skipped.push({ supplier: x.supplierCode, sku: x.skuCode, reason: 'SKU が無い (または正規化衝突で落とした)' }); continue; }
      const k = `${sup}|${sid}`; if (ssKeys.has(k)) { ssSec.skipped.push({ supplier: x.supplierCode, sku: x.skuCode, reason: '重複' }); continue; } ssKeys.add(k);
      ssRows.push({ company_id: COMPANY_ID, supplier_id: sup, sku_id: sid, vendor_code: x.vendorCode ?? null, stock_units_per_order_unit: x.stockUnitsPerOrderUnit ?? null, min_order_qty: x.minOrderQty ?? null, order_multiple: x.orderMultiple ?? null, unit_cost_jpy: x.unitCostJpy ?? null, created_by_type: 'system', created_by_id: runId });
    }
    const ssRet = await insertMany(db, 'core.supplier_skus', ['company_id', 'supplier_id', 'sku_id', 'vendor_code', 'stock_units_per_order_unit', 'min_order_qty', 'order_multiple', 'unit_cost_jpy', 'created_by_type', 'created_by_id'], ssRows,
      { onConflict: 'on conflict (supplier_id, sku_id) do update set vendor_code = coalesce(excluded.vendor_code, core.supplier_skus.vendor_code), stock_units_per_order_unit = coalesce(excluded.stock_units_per_order_unit, core.supplier_skus.stock_units_per_order_unit), min_order_qty = coalesce(excluded.min_order_qty, core.supplier_skus.min_order_qty), order_multiple = coalesce(excluded.order_multiple, core.supplier_skus.order_multiple), unit_cost_jpy = coalesce(excluded.unit_cost_jpy, core.supplier_skus.unit_cost_jpy)', returning: 'sku_id' });
    ssSec.applied = ssRet.length;
    if (ssSec.skipped.length) report.unresolved.supplier_skus = ssSec.skipped.slice(0, 200);
    log(`suppliers: ${supSec.applied}, supplier_skus: ${ssSec.applied} (skip ${ssSec.skipped.length})`);

    // ── 7. listings (正規化衝突は落とし、以降は acceptedListings だけ。listingRef も原文一致で解決) ──
    const lstSec = section(report, 'listings', plan.listings.length);
    const acceptedListings = []; const acceptedListingCode = new Map();   // key → 原文の listingCode
    for (const l of plan.listings) {
      const norm = normSku(l.listingCode);
      if (!norm) { lstSec.skipped.push({ mall: l.mall, code: l.listingCode, reason: 'code が空' }); continue; }
      const k = listingKey(l.mall, l.shopCode, l.listingCode);
      if (acceptedListingCode.has(k)) { lstSec.skipped.push({ mall: l.mall, code: l.listingCode, reason: `正規化すると ${acceptedListingCode.get(k)} と衝突` }); continue; }
      acceptedListingCode.set(k, l.listingCode);
      acceptedListings.push(l);
    }
    const lstRet = await insertMany(db, 'core.listings', ['company_id', 'mall', 'shop_code', 'listing_code', 'title', 'status', 'mall_item_id', 'created_by_type', 'created_by_id'],
      acceptedListings.map((l) => ({ company_id: COMPANY_ID, mall: l.mall, shop_code: l.shopCode || '', listing_code: l.listingCode, title: l.title ?? null, status: l.status || 'active', mall_item_id: l.mallItemId ?? null, created_by_type: 'system', created_by_id: runId })),
      { onConflict: 'on conflict (mall, shop_code, listing_norm) do update set title = coalesce(excluded.title, core.listings.title), status = excluded.status, mall_item_id = coalesce(excluded.mall_item_id, core.listings.mall_item_id)', returning: 'listing_id, mall, shop_code, listing_norm' });
    const listingIds = new Map(lstRet.map((r) => [`${r.mall}|${r.shop_code}|${r.listing_norm}`, Number(r.listing_id)]));
    lstSec.applied = lstRet.length;
    /** 採用した出品 (原文のコードが一致) だけ listing_id を返す。正規化衝突で落とした出品の参照は undefined */
    const listingIdOf = (ref) => {
      const k = listingKey(ref.mall, ref.shopCode, ref.listingCode);
      return acceptedListingCode.get(k) === ref.listingCode ? listingIds.get(k) : undefined;
    };

    // ── 8. listing_components (完全に読めた出品だけ plan に合わせる) + ASIN / FNSKU / 別名の候補 ──
    const lcSec = section(report, 'listing_components', acceptedListings.reduce((n, l) => n + (l.components || []).length, 0));
    const lcCand = []; const lcKeys = new Set(); const listingsWithSkip = new Set();
    const asinCands = new Map(); const extRows = []; const fnskuClearLids = [];
    for (const l of acceptedListings) {
      const lid = listingIdOf(l); if (!lid) continue;
      for (const c of (l.components || [])) {
        const sid = skuIdOf(c.code);
        if (!sid) { lcSec.skipped.push({ mall: l.mall, listing: l.listingCode, code: c.code, reason: 'NE コードが無い (または正規化衝突で落とした)' }); listingsWithSkip.add(lid); continue; }
        const k = `${lid}|${sid}`; if (lcKeys.has(k)) { lcSec.skipped.push({ mall: l.mall, listing: l.listingCode, code: c.code, reason: '重複' }); listingsWithSkip.add(lid); continue; } lcKeys.add(k);
        if (!(c.qty > 0)) { lcSec.skipped.push({ mall: l.mall, listing: l.listingCode, code: c.code, reason: `数量が不正 (${c.qty})` }); listingsWithSkip.add(lid); continue; }
        lcCand.push({ company_id: COMPANY_ID, listing_id: lid, sku_id: sid, qty: c.qty, resolution: c.resolution || 'imported', resolved_by_type: 'system', resolved_by_id: runId, evidence: c.evidence ? JSON.stringify(c.evidence) : null, _mall: l.mall, _listing: l.listingCode, _code: c.code });
      }
      // ASIN: 出どころの優先で 1 つ採用、違う値は conflict
      const cands = (l.asinCandidates || (l.asin ? [{ asin: l.asin, source: l.asinSource || 'unknown' }] : [])).filter((c) => c.asin);
      if (cands.length) {
        const win = pickByPriority(cands, ASIN_SOURCE_PRIORITY, 'asin');
        const distinct = new Map(cands.map((c) => [c.source, c.asin]));
        if (new Set(distinct.values()).size > 1) report.conflicts.push({ kind: 'asin', mall: l.mall, listing: l.listingCode, values: Object.fromEntries(distinct), adopted: win.asin });
        asinCands.set(lid, { asin: win.asin, source: win.source, marketplace: l.marketplaceId || 'A1VC38T7YXB528' });
      }
      const fn = (l.fnskuCandidates || (l.fnsku ? [{ fnsku: l.fnsku, source: 'fba_sheet_import' }] : [])).filter((c) => c.fnsku);
      if (fn.length) {
        const win = pickByPriority(fn, FNSKU_SOURCE_PRIORITY, 'fnsku');
        const distinct = new Map(fn.map((c) => [c.source, c.fnsku]));
        if (new Set(distinct.values()).size > 1) report.conflicts.push({ kind: 'fnsku', mall: l.mall, listing: l.listingCode, values: Object.fromEntries(distinct), adopted: win.fnsku });
        extRows.push({ company_id: COMPANY_ID, entity_type: 'listing', entity_id: lid, system: 'amazon', id_kind: 'fnsku', external_value: win.fnsku, resolution: 'imported', resolved_by_type: 'system', resolved_by_id: runId });
      } else if (l.fnskuCleared) fnskuClearLids.push(lid);   // 出どころが明示的に FNSKU を外した → 既存の自動付与を閉じる
      for (const x of (l.externalIds || [])) if (x.value) extRows.push({ company_id: COMPANY_ID, entity_type: 'listing', entity_id: lid, system: x.system, id_kind: x.kind, external_value: x.value, resolution: 'imported', resolved_by_type: 'system', resolved_by_id: runId });
    }
    const lcListings = [...new Set(lcCand.map((r) => r.listing_id))];
    const lcManual = new Map();
    if (lcListings.length) for (const r of (await db.query("select listing_id, sku_id, qty from core.listing_components where resolution = 'manual' and listing_id = any($1::bigint[])", [lcListings])).rows) lcManual.set(`${r.listing_id}|${r.sku_id}`, Number(r.qty));
    const lcRows = [];
    for (const r of lcCand) {
      const mq = lcManual.get(`${r.listing_id}|${r.sku_id}`);
      if (mq === undefined) { lcRows.push(r); continue; }
      if (mq === Number(r.qty)) { lcSec.same++; continue; }
      lcSec.skipped.push({ mall: r._mall, listing: r._listing, code: r._code, reason: `人が確定した行 (manual, 数量 ${mq}) と数量が違う (${r.qty})` });
      report.conflicts.push({ kind: 'listing_component_manual_mismatch', listing_id: r.listing_id, sku_id: r.sku_id, manual_qty: mq, plan_qty: r.qty });
      listingsWithSkip.add(r.listing_id);
    }
    const lcRet = await insertMany(db, 'core.listing_components', ['company_id', 'listing_id', 'sku_id', 'qty', 'resolution', 'resolved_by_type', 'resolved_by_id', 'evidence'], lcRows,
      { onConflict: "on conflict (listing_id, sku_id) do update set qty = excluded.qty, resolution = excluded.resolution, evidence = excluded.evidence where core.listing_components.resolution <> 'manual'", returning: 'listing_id' });
    lcSec.applied = lcRet.length;
    // 完全に読めた出品 = plan に構成が 1 行以上あり、skip が無い
    const pruneListings = lcListings.filter((lid) => !listingsWithSkip.has(lid));
    if (pruneListings.length) {
      const stale = (await db.query('select listing_id, sku_id, resolution from core.listing_components where listing_id = any($1::bigint[])', [pruneListings])).rows.filter((r) => !lcKeys.has(`${r.listing_id}|${r.sku_id}`));
      const del = stale.filter((r) => r.resolution !== 'manual');
      for (const r of stale.filter((r) => r.resolution === 'manual')) report.conflicts.push({ kind: 'listing_component_manual_kept', listing_id: Number(r.listing_id), sku_id: Number(r.sku_id) });
      if (del.length) await db.query('delete from core.listing_components where (listing_id, sku_id) in (select unnest($1::bigint[]), unnest($2::bigint[]))', [del.map((r) => r.listing_id), del.map((r) => r.sku_id)]);
      lcSec.notes.push(`stale removed: ${del.length}`);
    }
    if (lcSec.skipped.length) report.unresolved.listing_components = lcSec.skipped.slice(0, 500);
    log(`listings: ${lstSec.applied} (skip ${lstSec.skipped.length}), components: ${lcSec.applied} (same ${lcSec.same}, unresolved ${lcSec.skipped.length})`);

    // catalog_items (marketplace × ASIN) と listings.catalog_item_id (別々に帳尻を取る)
    const catSec = section(report, 'catalog_items', new Set([...asinCands.values()].map((v) => `${v.marketplace}|${v.asin}`)).size);
    const catRows = [...new Map([...asinCands.values()].map((v) => [`${v.marketplace}|${v.asin}`, v])).values()].map((v) => ({ marketplace_id: v.marketplace, asin: v.asin, last_seen_at: nowIso }));
    const catRet = await insertMany(db, 'core.catalog_items', ['marketplace_id', 'asin', 'last_seen_at'], catRows, { onConflict: 'on conflict (marketplace_id, asin) do update set last_seen_at = excluded.last_seen_at', returning: 'catalog_item_id, marketplace_id, asin' });
    const catIds = new Map(catRet.map((r) => [`${r.marketplace_id}|${r.asin}`, Number(r.catalog_item_id)]));
    catSec.applied = catRet.length;
    const linkSec = section(report, 'listing_asin_links', asinCands.size);
    const linkPairs = [...asinCands.entries()].map(([lid, v]) => [lid, catIds.get(`${v.marketplace}|${v.asin}`)]).filter(([, cid]) => cid);
    let linked = 0;
    for (let i = 0; i < linkPairs.length; i += CHUNK) {
      const chunk = linkPairs.slice(i, i + CHUNK); const params = [];
      const vals = chunk.map(([lid, cid]) => { params.push(lid, cid); return `($${params.length - 1}::bigint, $${params.length}::bigint)`; }).join(', ');
      const r = await db.query(`update core.listings l set catalog_item_id = v.cid from (values ${vals}) as v(lid, cid) where l.listing_id = v.lid and l.catalog_item_id is distinct from v.cid`, params);
      linked += r.rowCount ?? 0;
    }
    linkSec.applied = linked; linkSec.same = linkPairs.length - linked;
    // listing の外部 ID (FNSKU・楽天の別名)。🚨 FNSKU の明示的な解除を「先に」やってから付ける (解除した FNSKU を同じ plan で別の出品が要求しても 1 回で移る。Codex R4-1)
    const extSec = section(report, 'listing_external_ids', extRows.length);
    // FNSKU の明示的な解除 (出どころが空にした) → 自動付与の有効行を閉じる。manual は閉じない
    const clrSec = section(report, 'fnsku_clears', fnskuClearLids.length);
    if (fnskuClearLids.length) {
      const open = (await db.query("select external_id_row, entity_id, external_value, resolution from core.external_ids where entity_type = 'listing' and system = 'amazon' and id_kind = 'fnsku' and valid_to is null and entity_id = any($1::bigint[])", [fnskuClearLids])).rows;
      const byLid = new Map(); for (const r of open) { if (!byLid.has(Number(r.entity_id))) byLid.set(Number(r.entity_id), []); byLid.get(Number(r.entity_id)).push(r); }
      const closeRows = [];
      for (const lid of fnskuClearLids) {
        const rows = byLid.get(lid) || [];
        if (!rows.length) { clrSec.same++; continue; }
        const manual = rows.filter((r) => r.resolution === 'manual');
        if (manual.length) { clrSec.skipped.push({ listing_id: lid, reason: '人が付けた FNSKU は外さない' }); report.conflicts.push({ kind: 'fnsku_clear_manual_kept', listing_id: lid, value: manual[0].external_value }); continue; }
        closeRows.push(...rows.map((r) => r.external_id_row)); clrSec.applied++;
        report.conflicts.push({ kind: 'fnsku_cleared', listing_id: lid, old: rows.map((r) => r.external_value) });
      }
      if (closeRows.length) await db.query('update core.external_ids set valid_to = now() where external_id_row = any($1::bigint[]) and valid_to is null', [closeRows]);
    }
    Object.assign(extSec, await upsertExternalIds(db, extRows, report, 'listing_external_id'));
    // 出品に「実際に付いている」FNSKU (same / 新規付与 / manual)。FNSKU 経由の重量はこれと一致するものだけ入れる (Codex R3-3)
    const activeFnskuByLid = new Map();
    const amzLids = acceptedListings.filter((l) => l.mall === 'amazon').map((l) => listingIdOf(l)).filter(Boolean);
    if (amzLids.length) for (const r of (await db.query("select entity_id, external_norm from core.external_ids where entity_type = 'listing' and system = 'amazon' and id_kind = 'fnsku' and valid_to is null and entity_id = any($1::bigint[])", [amzLids])).rows) {
      const lid = Number(r.entity_id); if (!activeFnskuByLid.has(lid)) activeFnskuByLid.set(lid, new Set()); activeFnskuByLid.get(lid).add(r.external_norm);
    }
    /** via = { fnsku, listing } を持つ行は、その出品にその FNSKU が付いているときだけ通す。戻り値 = skip の理由 (通るなら null) */
    const viaFnskuBlocked = (via) => {
      if (!via?.fnsku) return null;
      const lid = via.listing ? listingIdOf(via.listing) : undefined;
      if (!lid) return `FNSKU ${via.fnsku} の出品が無い (または正規化衝突で落とした)`;
      return activeFnskuByLid.get(lid)?.has(normSku(via.fnsku)) ? null : `FNSKU ${via.fnsku} が出品に付いていない (manual / 取り合い / 不採用)`;
    };
    // NE コードは sku の外部 ID としても登録 (03 §2.2)
    const neRows = accepted.map((s) => ({ company_id: COMPANY_ID, entity_type: 'sku', entity_id: skuIdOf(s.code), system: 'ne', id_kind: 'product_code', external_value: s.code, resolution: 'imported', resolved_by_type: 'system', resolved_by_id: runId }));
    const neSec = section(report, 'ne_codes', neRows.length);
    Object.assign(neSec, await upsertExternalIds(db, neRows, report, 'ne_code'));
    log(`catalog_items: ${catSec.applied}, links: ${linkSec.applied}, listing ext ids: ${extSec.applied}/${extSec.same}/${extSec.skipped.length}, fnsku clears: ${clrSec.applied}, asin conflicts: ${report.conflicts.filter((c) => c.kind === 'asin').length}`);

    // ── 9. 観測 (append-only)。再送だけ入れない ──
    const obsSec = section(report, 'observations', (plan.observations || []).length);
    const obsRows = [];
    for (const o of (plan.observations || [])) {
      let entityType, entityId;
      if (o.listingRef) {
        entityType = 'listing'; entityId = listingIdOf(o.listingRef);
        if (!entityId) { obsSec.skipped.push({ listing: o.listingRef.listingCode, attribute: o.attribute, reason: 'listing が無い (または正規化衝突で落とした)' }); continue; }
      } else {
        const sid = skuIdOf(o.skuCode);
        if (!sid) { obsSec.skipped.push({ code: o.skuCode, attribute: o.attribute, reason: 'SKU が無い (または正規化衝突で落とした)' }); continue; }
        const pid = productIdOf(o.skuCode);
        entityType = o.entity === 'sku' || !pid ? 'sku' : 'product';
        entityId = entityType === 'product' ? pid : sid;
      }
      const valueText = o.valueText ?? null; const valueNum = o.valueNum ?? null;
      if (valueText == null && valueNum == null) { obsSec.skipped.push({ code: o.skuCode, attribute: o.attribute, reason: '値が空' }); continue; }
      const blocked = viaFnskuBlocked(o.via);
      if (blocked) { obsSec.skipped.push({ code: o.skuCode, listing: o.listingRef?.listingCode, attribute: o.attribute, reason: blocked }); continue; }
      const scope = o.scope || 'item';
      const observedAt = o.observedAt ? new Date(o.observedAt).toISOString() : null;   // null = 出どころに時刻が無い
      if (isFuture(observedAt)) { obsSec.skipped.push({ code: o.skuCode, listing: o.listingRef?.listingCode, attribute: o.attribute, reason: `観測時刻が未来 (${observedAt})` }); continue; }
      const contentHash = sha1(`${entityType}|${entityId}|${o.attribute}|${scope}|${valueText}|${valueNum}|${o.unit || ''}|${o.source}|${o.sourceRef || ''}`);
      obsRows.push({
        observation_key: `load:${runId}:${o.source}:${entityType}:${entityId}:${o.attribute}:${scope}:${sha1(`${o.sourceRef || ''}|${observedAt || ''}|${contentHash}`)}`,
        entity_type: entityType, entity_id: entityId, attribute: o.attribute, packaging_scope: scope,
        value_text: valueText, value_num: valueNum, value_unit: o.unit ?? null, raw_text: o.rawText ?? (valueText ?? String(valueNum)),
        source_system: o.source, source_ref: o.sourceRef ?? null, observed_at: observedAt, content_hash: contentHash,
      });
    }
    // 同一 run 内の完全な重複 (同じ key) は 1 つに
    const obsByKey = new Map(); for (const r of obsRows) obsByKey.set(r.observation_key, r);
    const obsUnique = [...obsByKey.values()];
    obsSec.skipped.push(...Array.from({ length: obsRows.length - obsUnique.length }, () => ({ reason: '同じ run 内の重複' })));
    // 既存の観測 (今回の対象エンティティだけ読む): 出どころ × 参照ごとの最新 と、(内容, 時刻) の完全一致
    const srcKey = (r) => `${r.entity_type}|${r.entity_id}|${r.attribute}|${r.packaging_scope}|${r.source_system}|${r.source_ref || ''}`;
    const latest = new Map(); const exact = new Set();
    if (obsUnique.length) {
      const ents = [...new Set(obsUnique.map((r) => `${r.entity_type}|${r.entity_id}`))].map((k) => k.split('|'));
      const ex = (await db.query(`select entity_type, entity_id, attribute, packaging_scope, source_system, coalesce(source_ref, '') as source_ref, content_hash, observed_at, observation_id
                                  from core.product_attribute_observations where ${pairIn('entity_type', 'entity_id', 1, 2)}`, [ents.map((e) => e[0]), ents.map((e) => Number(e[1]))])).rows;
      for (const r of ex) {
        const k = srcKey(r);
        exact.add(`${k}|${r.content_hash}|${ms(r.observed_at)}`);
        if (isFuture(r.observed_at)) continue;   // 未来の行は「最新」に数えない
        const cur = latest.get(k);
        if (!cur || ms(r.observed_at) > ms(cur.observed_at) || (ms(r.observed_at) === ms(cur.observed_at) && Number(r.observation_id) > Number(cur.observation_id))) latest.set(k, r);
      }
    }
    const obsNew = [];
    for (const r of obsUnique) {
      const k = srcKey(r);
      if (r.observed_at == null) {
        // 時刻の無い入力: その出どころ・参照の最新と同じ内容なら再送
        const cur = latest.get(k);
        if (cur && cur.content_hash === r.content_hash) { obsSec.same++; continue; }
        r.observed_at = nowIso;
      } else if (exact.has(`${k}|${r.content_hash}|${ms(r.observed_at)}`)) { obsSec.same++; continue; }   // 同じ内容・同じ時刻 = 再送
      obsNew.push(r);
    }
    await insertMany(db, 'core.product_attribute_observations', ['observation_key', 'entity_type', 'entity_id', 'attribute', 'packaging_scope', 'value_text', 'value_num', 'value_unit', 'raw_text', 'source_system', 'source_ref', 'observed_at', 'content_hash'], obsNew);
    obsSec.applied = obsNew.length;
    log(`observations: new ${obsSec.applied}, resend ${obsSec.same}, skip ${obsSec.skipped.length}`);

    // ── 10. 解決 (規則 v1) → products の列 / JAN → external_ids ──
    const targets = ['jan', 'brand', 'manufacturer', 'unit_count', 'net_content', 'release_date'];
    const allObs = productIdsInRun.length
      ? (await db.query(`select observation_id, entity_type, entity_id, attribute, packaging_scope, value_text, value_num, value_unit, source_system, coalesce(source_ref, '') as source_ref, observed_at
                          from core.product_attribute_observations where attribute = any($1) and entity_type = 'product' and entity_id = any($2::bigint[])`, [targets, productIdsInRun])).rows
      : [];
    // 出どころ × 参照ごとの最新観測だけを候補にする (古い観測は候補にしない)
    const latestBySrc = new Map();
    for (const o of allObs) {
      if (isFuture(o.observed_at)) continue;   // 未来の行は採用の候補にしない
      const k = `${o.entity_id}|${o.attribute}|${o.packaging_scope}|${o.source_system}|${o.source_ref}`;
      const cur = latestBySrc.get(k);
      if (!cur || ms(o.observed_at) > ms(cur.observed_at) || (ms(o.observed_at) === ms(cur.observed_at) && Number(o.observation_id) > Number(cur.observation_id))) latestBySrc.set(k, o);
    }
    const byKey = new Map();
    for (const o of latestBySrc.values()) { const k = `${o.entity_id}|${o.attribute}|${o.packaging_scope}`; if (!byKey.has(k)) byKey.set(k, []); byKey.get(k).push(o); }
    const winners = [];   // {entityId, attribute, scope, obs}
    for (const [k, list] of byKey) {
      const [entityId, attribute, scope] = k.split('|');
      const ranked = list.map((o) => ({ o, p: rulePriority.get(`${attribute}|${scope}|${o.source_system}`) })).filter((x) => x.p != null)
        .sort((a, b) => a.p - b.p || (ms(b.o.observed_at) - ms(a.o.observed_at)) || (Number(b.o.observation_id) - Number(a.o.observation_id)));
      if (!ranked.length) continue;
      const win = ranked[0].o;
      const valueOf = (o) => o.value_text ?? String(o.value_num);
      const values = new Map(ranked.map((x) => [`${x.o.source_system}${x.o.source_ref ? ':' + x.o.source_ref : ''}`, valueOf(x.o)]));
      if (new Set(values.values()).size > 1) report.conflicts.push({ kind: attribute, product_id: Number(entityId), values: Object.fromEntries(values), adopted: valueOf(win), adopted_source: win.source_system });
      winners.push({ entityId: Number(entityId), attribute, scope, obs: win });
    }
    // JAN → external_ids (取り合い・既に別の product が持つ・manual は upsertExternalIds が理由つき skip にする)
    const janW = winners.filter((w) => w.attribute === 'jan');
    const janSec = section(report, 'jan', janW.length);
    const janRows = [];
    for (const w of janW) {
      if (!/^\d{8}$|^\d{13}$/.test(w.obs.value_text || '')) { janSec.skipped.push({ entity_type: 'product', entity_id: w.entityId, value: w.obs.value_text, reason: 'JAN の形でない' }); continue; }
      janRows.push({ company_id: COMPANY_ID, entity_type: 'product', entity_id: w.entityId, system: 'jan', id_kind: 'jan', external_value: w.obs.value_text, resolution: 'imported', resolved_by_type: 'system', resolved_by_id: runId });
    }
    const janRes = await upsertExternalIds(db, janRows, report, 'jan');
    janSec.applied = janRes.applied; janSec.same = janRes.same; janSec.skipped.push(...janRes.skipped);
    const janSkippedIds = new Set(janSec.skipped.map((s) => s.entity_id));
    const janAssigned = new Set(janRows.map((r) => r.entity_id).filter((id) => !janSkippedIds.has(id)));
    // 解決結果は「実際に付与できたもの」だけ (JAN が付かなかった product には書かない = 理由つき skip)
    const resSec = section(report, 'resolutions', winners.length);
    const resRows = [];
    for (const w of winners) {
      if (w.attribute === 'jan' && !janAssigned.has(w.entityId)) { resSec.skipped.push({ product_id: w.entityId, attribute: 'jan', reason: 'JAN が付かなかった (取り合い・別の product が保持・形式)' }); continue; }
      resRows.push({ entity_type: 'product', entity_id: w.entityId, attribute: w.attribute, packaging_scope: w.scope, resolved_observation_id: Number(w.obs.observation_id), rule_version: RULE_VERSION });
    }
    const resRet = await insertMany(db, 'core.attribute_resolutions', ['entity_type', 'entity_id', 'attribute', 'packaging_scope', 'resolved_observation_id', 'rule_version'], resRows,
      { onConflict: 'on conflict (entity_type, entity_id, attribute, packaging_scope) do update set resolved_observation_id = excluded.resolved_observation_id, rule_version = excluded.rule_version, resolved_at = now()', returning: 'entity_id' });
    resSec.applied = resRet.length;
    // products の列へ (1 文で)
    const applyCol = async (attr, sqlSet, mapRow) => {
      const list = winners.filter((w) => w.attribute === attr).map(mapRow).filter(Boolean);
      for (let i = 0; i < list.length; i += CHUNK) {
        const chunk = list.slice(i, i + CHUNK); const params = [];
        const vals = chunk.map((vs) => `(${vs.map((v, j) => { params.push(v); return `$${params.length}${j === 0 ? '::bigint' : ''}`; }).join(', ')})`).join(', ');
        await db.query(sqlSet.replace('__VALUES__', () => vals), params);
      }
    };
    await applyCol('brand', 'update core.products p set brand = v.b from (values __VALUES__) as v(pid, b) where p.product_id = v.pid and p.brand is distinct from v.b', (w) => [w.entityId, w.obs.value_text]);
    await applyCol('manufacturer', 'update core.products p set manufacturer = v.b from (values __VALUES__) as v(pid, b) where p.product_id = v.pid and p.manufacturer is distinct from v.b', (w) => [w.entityId, w.obs.value_text]);
    await applyCol('unit_count', 'update core.products p set unit_count = v.n::int, unit_count_uom = v.u from (values __VALUES__) as v(pid, n, u) where p.product_id = v.pid and (p.unit_count is distinct from v.n::int or p.unit_count_uom is distinct from v.u)', (w) => (Number.isInteger(Number(w.obs.value_num)) && Number(w.obs.value_num) > 0 ? [w.entityId, Number(w.obs.value_num), w.obs.value_unit] : null));
    await applyCol('net_content', 'update core.products p set net_content = v.n::numeric, net_content_uom = v.u from (values __VALUES__) as v(pid, n, u) where p.product_id = v.pid and (p.net_content is distinct from v.n::numeric or p.net_content_uom is distinct from v.u)', (w) => (Number.isFinite(Number(w.obs.value_num)) && Number(w.obs.value_num) >= 0 ? [w.entityId, Number(w.obs.value_num), w.obs.value_unit] : null));
    await applyCol('release_date', 'update core.products p set release_date = v.d::date from (values __VALUES__) as v(pid, d) where p.product_id = v.pid and p.release_date is distinct from v.d::date', (w) => (/^\d{4}-\d{2}-\d{2}$/.test(w.obs.value_text || '') ? [w.entityId, w.obs.value_text] : null));
    log(`resolutions: ${resSec.applied} (skip ${resSec.skipped.length}), jan: new ${janSec.applied} same ${janSec.same} skip ${janSec.skipped.length}, attribute conflicts: ${report.conflicts.filter((c) => targets.includes(c.kind)).length}`);

    // ── 11. 物理属性 (出どころごとに行。再送判定は観測と同じ: 時刻あり = 内容 + 時刻の完全一致、時刻なし = 最新と同じ内容。有効行は規則で 1 つ) ──
    const phySec = section(report, 'physicals', (plan.physicals || []).length);
    const phyRows = [];
    for (const p of (plan.physicals || [])) {
      const pid = productIdOf(p.skuCode);
      if (!pid) { phySec.skipped.push({ code: p.skuCode, reason: '単品 product が無い (セット・例外・正規化衝突には物理属性を付けない)' }); continue; }
      if (!(p.weightG > 0 || p.lengthMm > 0 || p.widthMm > 0 || p.heightMm > 0 || p.unitsPerCase > 0)) { phySec.skipped.push({ code: p.skuCode, reason: '値が無い' }); continue; }
      const blocked = viaFnskuBlocked(p.via);
      if (blocked) { phySec.skipped.push({ code: p.skuCode, source: p.sourceRef, reason: blocked }); continue; }
      if (isFuture(p.observedAt)) { phySec.skipped.push({ code: p.skuCode, source: p.sourceRef, reason: `観測時刻が未来 (${p.observedAt})` }); continue; }
      phyRows.push({ company_id: COMPANY_ID, product_id: pid, scope: p.scope || 'package', length_mm: p.lengthMm ?? null, width_mm: p.widthMm ?? null, height_mm: p.heightMm ?? null, weight_g: p.weightG ?? null, units_per_case: p.unitsPerCase ?? null, source_system: p.source, source_ref: p.sourceRef ?? null, is_measured: !!p.isMeasured, observed_at: p.observedAt ? new Date(p.observedAt).toISOString() : null, created_by_type: 'system', created_by_id: runId });
    }
    const phySrc = (r) => `${r.product_id}|${r.scope}|${r.source_system}|${r.source_ref || ''}`;
    const phyContent = (r) => `${r.length_mm ?? ''}|${r.width_mm ?? ''}|${r.height_mm ?? ''}|${r.weight_g ?? ''}|${r.units_per_case ?? ''}`;
    const phyKey = (r) => `${phySrc(r)}|${phyContent(r)}|${r.observed_at == null ? 'null' : ms(r.observed_at)}`;
    const phyByKey = new Map(); for (const r of phyRows) phyByKey.set(phyKey(r), r);   // 同一 run 内の完全な重複は 1 つ
    const phyUnique = [...phyByKey.values()];
    phySec.skipped.push(...Array.from({ length: phyRows.length - phyUnique.length }, () => ({ reason: '同じ run 内の重複' })));
    const phyPids = [...new Set(phyUnique.map((r) => r.product_id))];
    const phyExact = new Set(); const phyLatest = new Map();
    if (phyPids.length) for (const r0 of (await db.query('select product_physical_id, product_id, scope, source_system, source_ref, length_mm, width_mm, height_mm, weight_g, units_per_case, observed_at from core.product_physicals where product_id = any($1::bigint[])', [phyPids])).rows) {
      const r = { ...r0, product_id: Number(r0.product_id) };
      phyExact.add(phyKey(r));
      if (isFuture(r.observed_at)) continue;   // 未来の行は「最新」に数えない
      const k = phySrc(r); const cur = phyLatest.get(k);
      if (!cur || ms(r.observed_at) > ms(cur.observed_at) || (ms(r.observed_at) === ms(cur.observed_at) && Number(r.product_physical_id) > Number(cur.product_physical_id))) phyLatest.set(k, r);
    }
    const phyNew = [];
    for (const r of phyUnique) {
      if (r.observed_at == null) {
        const cur = phyLatest.get(phySrc(r));
        if (cur && phyContent(cur) === phyContent(r)) { phySec.same++; continue; }
        r.observed_at = nowIso;
      } else if (phyExact.has(phyKey(r))) { phySec.same++; continue; }
      phyNew.push(r);
    }
    await insertMany(db, 'core.product_physicals', ['company_id', 'product_id', 'scope', 'length_mm', 'width_mm', 'height_mm', 'weight_g', 'units_per_case', 'source_system', 'source_ref', 'is_measured', 'observed_at', 'created_by_type', 'created_by_id'], phyNew);
    phySec.applied = phyNew.length;
    // 有効行: 今回触った product だけ再計算。product × scope ごとに、規則 (package_weight_g の source 優先) → 観測時刻の新しい順
    let effCount = 0;
    if (phyPids.length) {
      const cand = (await db.query('select product_physical_id, product_id, scope, source_system, observed_at from core.product_physicals where product_id = any($1::bigint[])', [phyPids])).rows;
      const bestByPs = new Map();
      for (const r of cand) {
        if (isFuture(r.observed_at)) continue;   // 未来の行は有効行にしない
        const p = rulePriority.get(`package_weight_g|${r.scope}|${r.source_system}`) ?? rulePriority.get(`package_weight_g|package|${r.source_system}`);
        if (p == null) continue;
        const k = `${r.product_id}|${r.scope}`; const cur = bestByPs.get(k);
        if (!cur || p < cur.p || (p === cur.p && ms(r.observed_at) > ms(cur.r.observed_at)) || (p === cur.p && ms(r.observed_at) === ms(cur.r.observed_at) && Number(r.product_physical_id) > Number(cur.r.product_physical_id))) bestByPs.set(k, { p, r });
      }
      const effIds = [...bestByPs.values()].map((x) => Number(x.r.product_physical_id));
      await db.query('update core.product_physicals set is_effective = false where is_effective and product_id = any($2::bigint[]) and not (product_physical_id = any($1::bigint[]))', [effIds, phyPids]);
      if (effIds.length) await db.query('update core.product_physicals set is_effective = true where product_physical_id = any($1::bigint[]) and not is_effective', [effIds]);
      effCount = effIds.length;
    }
    phySec.notes.push(`effective rows: ${effCount}`);
    log(`physicals: new ${phySec.applied}, resend ${phySec.same}, effective ${effCount}`);

    // ── 12. compliance ──
    const cmpSec = section(report, 'compliance', (plan.compliance || []).length);
    const cmpRows = [];
    for (const c of (plan.compliance || [])) {
      const pid = productIdOf(c.skuCode);
      if (!pid) { cmpSec.skipped.push({ code: c.skuCode, reason: '単品 product が無い (または正規化衝突で落とした)' }); continue; }
      cmpRows.push({ product_id: pid, company_id: COMPANY_ID, ingredients: c.ingredients ?? null, precautions: c.precautions ?? null, distributor: c.distributor ?? null, manufacturer_jp: c.manufacturerJp ?? null, allergens: c.allergens ?? null, source_system: c.source, source_ref: c.sourceRef ?? null, created_by_type: 'system', created_by_id: runId });
    }
    const cmpByPid = new Map(); for (const r of cmpRows) cmpByPid.set(r.product_id, r);
    const cmpUnique = [...cmpByPid.values()];
    cmpSec.skipped.push(...Array.from({ length: cmpRows.length - cmpUnique.length }, () => ({ reason: '同じ product の重複' })));
    const cmpRet = await insertMany(db, 'core.product_compliance', ['product_id', 'company_id', 'ingredients', 'precautions', 'distributor', 'manufacturer_jp', 'allergens', 'source_system', 'source_ref', 'created_by_type', 'created_by_id'], cmpUnique,
      { onConflict: 'on conflict (product_id) do update set ingredients = coalesce(excluded.ingredients, core.product_compliance.ingredients), precautions = coalesce(excluded.precautions, core.product_compliance.precautions), distributor = coalesce(excluded.distributor, core.product_compliance.distributor), manufacturer_jp = coalesce(excluded.manufacturer_jp, core.product_compliance.manufacturer_jp), allergens = coalesce(excluded.allergens, core.product_compliance.allergens), source_system = excluded.source_system, source_ref = excluded.source_ref', returning: 'product_id' });
    cmpSec.applied = cmpRet.length;

    // ── 13. workers ──
    const wkSec = section(report, 'workers', (plan.workers || []).length);
    const wkRows = (plan.workers || []).filter((w) => { if (w.staffNo && w.displayName) return true; wkSec.skipped.push({ staffNo: w.staffNo, reason: 'staff_no か名前が空' }); return false; });
    const wkRet = await insertMany(db, 'core.workers', ['company_id', 'staff_no', 'display_name', 'login_email', 'worker_type', 'active', 'created_by_type', 'created_by_id'],
      wkRows.map((w) => ({ company_id: w.companyId || COMPANY_ID, staff_no: w.staffNo, display_name: w.displayName, login_email: w.loginEmail || null, worker_type: w.workerType || 'employee', active: w.active !== false, created_by_type: 'system', created_by_id: runId })),
      { onConflict: 'on conflict (company_id, staff_no) do update set display_name = excluded.display_name, login_email = excluded.login_email, worker_type = excluded.worker_type, active = excluded.active', returning: 'worker_id' });
    wkSec.applied = wkRet.length;

    // ── 14. 帳尻 (全区分) → 記録 ──
    assertBalanced(report);
    report.finished_at = new Date().toISOString();
    report.ok = true;
    const summary = Object.fromEntries(Object.entries(report.sections).map(([k, v]) => [k, { expected: v.expected, applied: v.applied, same: v.same, skipped: v.skipped.length }]));
    report.summary = summary;
    await db.query(`insert into ops.ingest_runs (ingest_run_id, source_system, entity, scope_key, host, started_at, finished_at, status, complete, rows_seen, rows_inserted, rows_skipped, source_tz, checksum, format_version)
                    values ($1, 'sqlite_initial_load', 'products', 'render', $2, $3, $4, $5, true, $6, $7, $8, 'UTC', $9, 'plan-v3')
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
    report.finished_at = new Date().toISOString();
    throw Object.assign(e, { report });
  }
}

/**
 * 外部 ID の upsert。処理順に依存しないよう「先に全部読む → 全部の要求を集める → 移動計画を固定点で解く → 閉じる → 付ける」。
 *   - 同じ値を同じエンティティが既に持つ → same
 *   - 同じ値を同じ run で複数のエンティティが要求 → conflict *_contended、誰にも付けない (skipped)
 *   - 同じエンティティに人が付けた行 (manual) があり、値が違う → conflict *_manual_kept、付けない (skipped)
 *   - 同じ値を別のエンティティ H が持つ → H が今回 別の値へ移れる (H の要求が通る、かつ H の行が manual でない) ときだけ手放す。
 *     移れなければ conflict *_taken、付けない (skipped)。「移れる」は固定点で解く (連鎖の途中で誰かが止まれば、その前も止まる)
 *   - 通った要求のエンティティの、他の有効行 (manual 以外) は valid_to を埋めて付け替え (conflict *_replaced)
 * 戻り値 { applied, same, skipped:[{entity_type, entity_id, value, reason}] }
 */
async function upsertExternalIds(db, rows, report, label) {
  const out = { applied: 0, same: 0, skipped: [] };
  if (!rows.length) return out;
  const entKey = (r) => `${r.system}|${r.id_kind}|${r.entity_type}|${r.entity_id}`;
  const skOf = (r) => `${r.system}|${r.id_kind}`;
  const systems = [...new Set(rows.map(skOf))];
  const activeByValue = new Map(); const activeByEntity = new Map();   // sk|norm → row / sk|type|id → [rows]
  for (const sk of systems) {
    const [system, kind] = sk.split('|');
    for (const r of (await db.query('select external_id_row, entity_type, entity_id, external_value, external_norm, resolution from core.external_ids where system = $1 and id_kind = $2 and valid_to is null', [system, kind])).rows) {
      activeByValue.set(`${sk}|${r.external_norm}`, r);
      const ek = `${sk}|${r.entity_type}|${r.entity_id}`;
      if (!activeByEntity.has(ek)) activeByEntity.set(ek, []);
      activeByEntity.get(ek).push(r);
    }
  }
  // 要求を整理 (空・同一 run 内の重複を落とす)
  const reqs = []; const seen = new Set();
  for (const r of rows) {
    const norm = normSku(r.external_value);
    if (!norm) { out.skipped.push({ entity_type: r.entity_type, entity_id: r.entity_id, value: r.external_value, reason: '値が空' }); continue; }
    const dk = `${entKey(r)}|${norm}`;
    if (seen.has(dk)) { out.skipped.push({ entity_type: r.entity_type, entity_id: r.entity_id, value: r.external_value, reason: '同じ run 内の重複' }); continue; }
    seen.add(dk);
    reqs.push({ r, norm, vk: `${skOf(r)}|${norm}`, ek: entKey(r), status: 'ok', reason: null });
  }
  // same (既に同じ値を持っている要求は、取り合いにも移動にも関わらない)
  for (const q of reqs) {
    const held = activeByValue.get(q.vk);
    if (held && `${skOf(q.r)}|${held.entity_type}|${held.entity_id}` === q.ek) q.status = 'same';
  }
  // 取り合い (same 以外で、同じ値を複数のエンティティが要求)。既に誰かが持っている値を 1 つのエンティティだけが要求するのは taken の判定へ
  const wantBy = new Map();
  for (const q of reqs) { if (q.status !== 'ok') continue; if (!wantBy.has(q.vk)) wantBy.set(q.vk, []); wantBy.get(q.vk).push(q); }
  for (const [, list] of wantBy) {
    const ents = new Set(list.map((q) => q.ek));
    if (ents.size > 1) {
      report.conflicts.push({ kind: `${label}_contended`, value: list[0].r.external_value, entities: [...new Map(list.map((q) => [q.ek, { type: q.r.entity_type, id: Number(q.r.entity_id) }])).values()] });
      for (const q of list) { q.status = 'contended'; q.reason = '同じ値を複数が要求'; }
    }
  }
  // 保持する値 (same + 通る見込みの ok) と、新しく通る要求の数。same の値は閉じないし、他へ手放さない (Codex R3-1)
  const keep = new Map(); const okCount = new Map();   // ek → Set(norm) / ek → 新規 ok の数
  const addKeep = (ek, norm) => { if (!keep.has(ek)) keep.set(ek, new Set()); keep.get(ek).add(norm); };
  for (const q of reqs) if (q.status === 'same') addKeep(q.ek, q.norm);
  // manual (人が付けた別の値があるエンティティには自動で付けない)
  for (const q of reqs) {
    if (q.status !== 'ok') continue;
    const mine = activeByEntity.get(q.ek) || [];
    const manual = mine.find((x) => x.resolution === 'manual' && x.external_norm !== q.norm);
    if (manual) { q.status = 'manual'; q.reason = `人が付けた ${manual.external_value} がある`; report.conflicts.push({ kind: `${label}_manual_kept`, entity: { type: q.r.entity_type, id: Number(q.r.entity_id) }, manual: manual.external_value, wanted: q.r.external_value }); continue; }
    addKeep(q.ek, q.norm); okCount.set(q.ek, (okCount.get(q.ek) || 0) + 1);
  }
  // 固定点: 別のエンティティが持つ値は、持ち主が今回 別の値へ移り (新規 ok がある)、その値を保持しない (same でない) ときだけ取れる
  const releases = (held, sk) => {
    if (held.resolution === 'manual') return false;
    const hk = `${sk}|${held.entity_type}|${held.entity_id}`;
    if (!(okCount.get(hk) > 0)) return false;
    return !keep.get(hk)?.has(held.external_norm);
  };
  let changed = true;
  while (changed) {
    changed = false;
    for (const q of reqs) {
      if (q.status !== 'ok') continue;
      const held = activeByValue.get(q.vk);
      if (held && !releases(held, skOf(q.r))) {
        q.status = 'taken'; q.reason = `別の ${held.entity_type} ${held.entity_id} が持っている`;
        report.conflicts.push({ kind: `${label}_taken`, value: q.r.external_value, held_by: { type: held.entity_type, id: Number(held.entity_id) }, wanted_by: { type: q.r.entity_type, id: Number(q.r.entity_id) } });
        keep.get(q.ek)?.delete(q.norm); okCount.set(q.ek, okCount.get(q.ek) - 1);
        changed = true;
      }
    }
  }
  // 閉じる (新規 ok が 1 つ以上あるエンティティの、保持しない有効行。manual は閉じない) → 付ける
  const toClose = []; const toInsert = [];
  for (const [ek, n] of okCount) {
    if (!(n > 0)) continue;
    const norms = keep.get(ek) || new Set();
    for (const x of (activeByEntity.get(ek) || [])) {
      if (norms.has(x.external_norm) || x.resolution === 'manual') continue;
      toClose.push(x.external_id_row);
      report.conflicts.push({ kind: `${label}_replaced`, entity: { type: x.entity_type, id: Number(x.entity_id) }, old: x.external_value, new: [...norms].join(',') });
    }
  }
  for (const q of reqs) {
    if (q.status === 'ok') toInsert.push(q.r);
    else if (q.status === 'same') out.same++;
    else out.skipped.push({ entity_type: q.r.entity_type, entity_id: q.r.entity_id, value: q.r.external_value, reason: q.reason });
  }
  if (toClose.length) await db.query('update core.external_ids set valid_to = now() where external_id_row = any($1::bigint[]) and valid_to is null', [toClose]);
  await insertMany(db, 'core.external_ids', ['company_id', 'entity_type', 'entity_id', 'system', 'id_kind', 'external_value', 'resolution', 'resolved_by_type', 'resolved_by_id'], toInsert);
  out.applied = toInsert.length;
  return out;
}

/** report を人が読める短い Markdown に */
export function reportToMarkdown(report) {
  const lines = [`# Company DB 初期ロード ${report.run_id} (${report.dry_run ? 'dry-run' : '本適用'}) ${report.ok ? 'OK' : 'FAILED'}`, ''];
  lines.push('| 区分 | 予定 | 投入 | 既存同 | skip |', '|---|---|---|---|---|');
  for (const [k, v] of Object.entries(report.sections || {})) lines.push(`| ${k} | ${v.expected} | ${v.applied} | ${v.same} | ${v.skipped.length}${v.notes.length ? ' (' + v.notes.join('; ') + ')' : ''} |`);
  const byKind = {};
  for (const c of (report.conflicts || [])) byKind[c.kind] = (byKind[c.kind] || 0) + 1;
  lines.push('', `不一致: ${Object.entries(byKind).map(([k, n]) => `${k}=${n}`).join(', ') || 'なし'}`);
  for (const [k, v] of Object.entries(report.unresolved || {})) lines.push(`未解決 ${k}: ${v.length} 件 (先頭: ${JSON.stringify(v[0])})`);
  if (report.error) lines.push('', `エラー: ${report.error}`);
  return lines.join('\n');
}
