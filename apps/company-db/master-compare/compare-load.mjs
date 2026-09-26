/**
 * compare-load.mjs — 毎朝の照合 ①ロードの検証 (Company DB構想 10 §6.1.1 B2。Codex ③a-2 R0・R1・B-R0)
 *
 * 何をするか: 最新の夜間ロード (Render・02:00) が、実際に読んだ材料 (miniPC の控え) から「ロードの後にあるべき値」を作り直し、
 *   今の Company DB と比べる。差 = ロードの誤り、またはロードの後に誰かが書いた (変更の記録を「候補」として付ける)。
 * 🚨 判定できないときは blocked (「差 0」と言わない):
 *   夜間ロードが今日のものでない / 材料が matched でない / ロードの規則の指紋がこのコードと違う (設定ファイルも指紋に入る = 設定を変えた日も) /
 *   ロードの判断 (0030) が無い・形が違う / 控えが無い・壊れている / 控えを戻した中身がロードの読んだ中身と違う
 * ロードの時の判断 (ops.load_decisions) と持ち主 (ops.load_materials.ownership)・条件 (load_conditions.has0027) を使う (朝の CDB から決め直さない)
 * db = { query } (pg の client でも PGlite でも)。呼ぶ側が REPEATABLE READ READ ONLY の取引を張る
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import { buildPlanFromRender } from '../load/sources.mjs';
import { skuValuesForLoad, SKU_OWNED_COLUMNS, SKU_0027_COLUMNS, costForLoad, LOAD_DECISIONS_FORMAT, LOAD_RULE_FINGERPRINT } from '../load/engine.mjs';
import { readMaterialSnapshot, materialDigest, MATERIAL_COLUMNS } from '../../warehouse/material-lineage.js';
import { MIRROR_PRODUCTS_DDL, MIRROR_SET_COMPONENTS_DDL } from '../../warehouse-mirror/material-tables.js';
import { normSku } from '../../../lib/sku-norm.js';

/** 全件 JSON の形。mc-v2 = 一番上は ① (今までの mc-v1 と同じ項目)・ne = ② の節 (C2)。W13:load は mc-v1 / mc-v2 の両方を読む */
export const COMPARE_FORMAT = 'mc-v2';
/** ① が読んだもの (材料の plan・判断・持ち主・Company DB の値) を ② に渡す。Symbol のキー = 全件 JSON には出ない */
export const LOAD_CTX = Symbol('master-compare-load-ctx');
export const NIGHTLY_HOST = 'render-nightly';
export const COMPANY_ID = 1;
/** 案件の種類 (見張りの subject key = `<種類>:<code_norm>`。種類に ':' を含めない) */
export const ITEM_TYPES = Object.freeze(['missing', 'value', 'cost', 'primary_supplier', 'components']);
export const DECISION_SECTIONS = Object.freeze(['skus', 'sku_costs', 'set_components', 'primary_suppliers']);
export const subjectKey = (type, codeNorm) => `${type}:${codeNorm}`;
export const jstDateOf = (iso) => new Date(Date.parse(iso) + 9 * 3600000).toISOString().slice(0, 10);

const rowsOf = async (db, sql, params) => (await db.query(sql, params)).rows;
const tableExists = async (db, schema, table) => (await rowsOf(db, 'select 1 from information_schema.tables where table_schema = $1 and table_name = $2', [schema, table])).length > 0;
const columnExists = async (db, schema, table, column) => (await rowsOf(db, 'select 1 from information_schema.columns where table_schema = $1 and table_name = $2 and column_name = $3', [schema, table, column])).length > 0;
/** 値が同じか (null と undefined は同じ・数は小さな誤差を許す) */
export function sameValue(a, b) {
  if (a == null && b == null) return true;
  if (a == null || b == null) return false;
  const na = typeof a === 'number' ? a : (typeof a === 'string' && a.trim() !== '' && !Number.isNaN(Number(a)) && typeof b === 'number' ? Number(a) : null);
  const nb = typeof b === 'number' ? b : (typeof b === 'string' && b.trim() !== '' && !Number.isNaN(Number(b)) && typeof a === 'number' ? Number(b) : null);
  if (na != null && nb != null) return Math.abs(na - nb) < 1e-9;
  return a === b;
}

const isStr = (v) => typeof v === 'string';
const isStrOrNull = (v) => v == null || typeof v === 'string';
const isInt = (v) => Number.isInteger(v);
const arrOf = (a, ok) => Array.isArray(a) && a.every(ok);
/**
 * ロードの判断 (0030) の中身が形どおりで、ロードした回の持ち主・条件と合うか。合わなければ理由 (照合は blocked)。
 * 🚨 形が欠けた判断を「比べるものが無い」と読まない (Codex #1456 R1 Medium: payload = {} で原価の比較を全部飛ばして pass になる)
 */
export function decisionsProblem(D, { ownership, has0027 }) {
  const s = D.skus, c = D.sku_costs, k = D.set_components, p = D.primary_suppliers;
  if (!s || !isInt(s.accepted) || !arrOf(s.skipped, (x) => Array.isArray(x) && isStrOrNull(x[0]) && isStr(x[1]))) return 'skus';
  if (!c || typeof c.owned !== 'boolean' || !arrOf(c.skipped, (x) => Array.isArray(x) && isStrOrNull(x[0]) && isStr(x[1]))) return 'sku_costs';
  if (!k || typeof k.owned !== 'boolean' || !arrOf(k.prune_parents, (x) => Array.isArray(x) && isStrOrNull(x[0]) && isInt(x[1]))
    || !arrOf(k.rows, (x) => Array.isArray(x) && isStr(x[0]) && isStr(x[1]) && isInt(x[2]) && (x[3] === 'load' || x[3] === 'manual_same'))
    || !Array.isArray(k.manual_kept_on_prune) || !arrOf(k.skipped, (x) => Array.isArray(x) && isStr(x[2]))) return 'set_components';
  if (!p || typeof p.applied !== 'boolean') return 'primary_suppliers';
  if (p.applied && (!arrOf(p.targets, (x) => Array.isArray(x) && isStr(x[0]) && isStr(x[1])) || !arrOf(p.unresolved, (x) => Array.isArray(x) && isStr(x[2])))) return 'primary_suppliers';
  if (!p.applied && !isStr(p.reason_code)) return 'primary_suppliers';
  // ロードした回の持ち主・条件と整合 (持ち主が load なのに「見送った」、company なのに「書いた」は形がおかしい)
  if (c.owned !== (ownership['sku_costs'] === 'load')) return 'sku_costs_owner';
  if (k.owned !== (ownership['sku_components'] === 'load')) return 'set_components_owner';
  if (p.applied !== (has0027 && ownership['supplier_skus.is_primary'] === 'load')) return 'primary_suppliers_owner';
  if (k.owned && k.rows.length === 0 && k.prune_parents.length > 0) return 'set_components_rows';
  return null;
}

/**
 * Company DB の今のマスタ (照合 ①・② が同じ snapshot で読む)。REPEATABLE READ READ ONLY の取引の中で呼ぶ
 * @returns {{ skus: object[], skuByNorm: Map, idToNorm: Map, costs: Map, primary: Map, comps: Map, has0027: boolean }}
 */
export async function readCdbMaster(db) {
  const has0027 = await columnExists(db, 'core', 'skus', 'standard_price_jpy');
  const skus = await rowsOf(db, `select sku_id::text as sku_id, code, code_norm, sku_kind, name, tax_rate::float8 as tax_rate, tax_class, handling${has0027 ? `,
    standard_price_jpy::float8 as standard_price_jpy, shipping_code, shipping_method, shipping_cost_jpy::float8 as shipping_cost_jpy` : ''}
    from core.skus where company_id = $1`, [COMPANY_ID]);
  const skuByNorm = new Map(skus.map((r) => [r.code_norm, r]));
  const idToNorm = new Map(skus.map((r) => [Number(r.sku_id), r.code_norm]));
  const costs = new Map((await rowsOf(db, `select s.code_norm, c.cost_jpy::float8 as cost_jpy, c.cost_source, c.cost_status from core.sku_costs c join core.skus s on s.sku_id = c.sku_id
    where c.valid_to is null and c.company_id = $1`, [COMPANY_ID])).map((r) => [r.code_norm, r]));
  const primary = new Map();
  if (has0027) {
    for (const r of await rowsOf(db, `select s.code_norm, sup.code_norm as sup_norm from core.supplier_skus x join core.skus s on s.sku_id = x.sku_id join core.suppliers sup on sup.supplier_id = x.supplier_id
      where x.is_primary and x.company_id = $1`, [COMPANY_ID])) { if (!primary.has(r.code_norm)) primary.set(r.code_norm, []); primary.get(r.code_norm).push(r.sup_norm); }
  }
  const comps = new Map();   // parent_norm → Map(child_norm → {qty, source})
  for (const r of await rowsOf(db, `select p.code_norm as parent, c.code_norm as child, x.qty, x.source from core.sku_components x
    join core.skus p on p.sku_id = x.parent_sku_id join core.skus c on c.sku_id = x.child_sku_id where x.company_id = $1`, [COMPANY_ID])) {
    if (!comps.has(r.parent)) comps.set(r.parent, new Map());
    comps.get(r.parent).set(r.child, { qty: Number(r.qty), source: r.source });
  }
  return { skus, skuByNorm, idToNorm, costs, primary, comps, has0027 };
}

/** 夜間ロードの記録 (手動のロードで代用しない。Codex B-R0 #9) */
export async function selectNightlyLoad(db) {
  return (await rowsOf(db, `select ingest_run_id, started_at::text as started_at, finished_at::text as finished_at, host
    from ops.ingest_runs where source_system = 'sqlite_initial_load' and entity = 'products' and scope_key = 'render' and host = $1 and status = 'success' and complete
    order by started_at desc limit 1`, [NIGHTLY_HOST]))[0] || null;
}

/** 控えの行を mirror と同じ型の一時の SQLite に戻し、戻した中身のハッシュを確かめてから buildPlanFromRender を通す */
export function planFromSnapshot({ productsRows, setRows, expected, now, tmpRoot = os.tmpdir() }) {
  const dir = fs.mkdtempSync(path.join(tmpRoot, 'cdb-mc-'));
  try {
    const m = new Database(path.join(dir, 'warehouse-mirror.db'));
    try {
      m.exec(MIRROR_PRODUCTS_DDL); m.exec(MIRROR_SET_COMPONENTS_DDL);
      const ins = (table, cols, rows) => {
        const st = m.prepare(`INSERT INTO ${table} (${[...cols, 'updated_at'].map((c) => `"${c}"`).join(', ')}) VALUES (${[...cols, 'updated_at'].map(() => '?').join(', ')})`);
        m.transaction(() => { for (const r of rows) st.run(...cols.map((c) => (r[c] === undefined ? null : r[c])), 'restored'); })();
      };
      ins('mirror_products', MATERIAL_COLUMNS.products, productsRows);
      ins('mirror_set_components', MATERIAL_COLUMNS.set_components, setRows);
      const back = {
        products: materialDigest('products', m.prepare('SELECT * FROM mirror_products').all()),
        set_components: materialDigest('set_components', m.prepare('SELECT * FROM mirror_set_components').all()),
      };
      for (const e of ['products', 'set_components']) {
        if (back[e].content_hash !== expected[e].content_hash || back[e].row_count !== expected[e].row_count) return { ok: false, reason: 'restore_mismatch', entity: e };
      }
    } finally { m.close(); }
    const plan = buildPlanFromRender({ dataDir: dir, now });
    return { ok: true, plan };
  } finally {
    try { fs.rmSync(dir, { recursive: true, force: true }); } catch { /* Windows は OS に任せる */ }
  }
}

/**
 * ①ロードの検証。戻り値 = 全件 JSON の中身 (format・verdict・blocked_reason・load・materials・counts・items・compared・exclusions)
 * @param {object} p
 * @param {{ query: Function }} p.db   REPEATABLE READ READ ONLY の取引の中の接続
 * @param {string} p.dataDir           miniPC の DATA_DIR (控え)
 * @param {string} p.asOfJst           今日 (JST)
 */
export async function compareLoad({ db, dataDir, asOfJst, localFingerprint = LOAD_RULE_FINGERPRINT, tmpRoot = os.tmpdir() }) {
  const out = { format: COMPARE_FORMAT, as_of: asOfJst, verdict: null, blocked_reason: null, load: null, materials: null, fingerprint: { local: localFingerprint, load: null },
    conditions: null, counts: {}, items: [], compared: {}, exclusions: {} };
  const block = (reason, extra = {}) => Object.assign(out, { verdict: 'blocked', blocked_reason: reason }, extra);

  // 1. 夜間ロード (今日のもの)
  const load = await selectNightlyLoad(db);
  if (!load) return block('no_nightly_load');
  out.load = load;
  if (jstDateOf(load.started_at) !== asOfJst) return block('stale_load');
  // 2. 記録の表 (0029・0030)
  if (!(await columnExists(db, 'ops', 'load_materials', 'rule_fingerprint'))) return block('no_0029');
  if (!(await tableExists(db, 'ops', 'load_decisions'))) return block('no_0030');
  // 3. 材料・規則の指紋・持ち主・条件
  const lm = Object.fromEntries((await rowsOf(db, `select entity, status, generation_id, content_hash, row_count, rule_fingerprint, ownership, load_conditions
    from ops.load_materials where ingest_run_id = $1`, [load.ingest_run_id])).map((r) => [r.entity, r]));
  out.materials = Object.fromEntries(Object.entries(lm).map(([k, v]) => [k, { status: v.status, generation_id: v.generation_id, content_hash: v.content_hash, row_count: v.row_count }]));
  if (!lm.products || !lm.set_components) return block('no_load_materials');
  if (lm.products.status !== 'matched' || lm.set_components.status !== 'matched') return block('material_not_matched');
  out.fingerprint.load = lm.products.rule_fingerprint || null;
  if (!lm.products.rule_fingerprint || lm.products.rule_fingerprint !== lm.set_components.rule_fingerprint) return block('no_fingerprint');
  if (lm.products.rule_fingerprint !== localFingerprint) return block('rule_mismatch');
  const ownership = lm.products.ownership && typeof lm.products.ownership === 'object' ? lm.products.ownership : null;
  const conditions = lm.products.load_conditions && typeof lm.products.load_conditions === 'object' ? lm.products.load_conditions : null;
  if (!ownership || !conditions) return block('no_load_conditions');
  out.conditions = conditions;
  const has0027 = conditions.has0027 === true;
  const loadOwned = (key) => ownership[key] === 'load';
  // 4. ロードの判断 (0030)
  const dec = Object.fromEntries((await rowsOf(db, 'select section, format, payload from ops.load_decisions where ingest_run_id = $1', [load.ingest_run_id])).map((r) => [r.section, r]));
  for (const s of DECISION_SECTIONS) {
    if (!dec[s]) return block('no_decisions', { missing_section: s });
    if (dec[s].format !== LOAD_DECISIONS_FORMAT) return block('decisions_format', { section: s, format: dec[s].format });
  }
  const D = Object.fromEntries(DECISION_SECTIONS.map((s) => [s, dec[s].payload]));
  const dp = decisionsProblem(D, { ownership, has0027 });
  if (dp) return block('decisions_malformed', { section: dp });
  // 5. 控え (材料の世代ごと。期待のハッシュ = ロードが読んだ中身)
  const snaps = {};
  for (const e of ['products', 'set_components']) {
    const g = lm[e].generation_id;
    try {
      snaps[e] = readMaterialSnapshot({ dataDir, generationId: g, expected: { [e]: lm[e].content_hash } });
    } catch (err) {
      return block(err && err.code === 'MATERIAL_HASH_MISMATCH' ? 'snapshot_mismatch' : 'snapshot_unreadable', { entity: e, detail: String(err && err.message).slice(0, 200) });
    }
    if (!snaps[e]) return block('snapshot_missing', { entity: e, generation_id: g });
  }
  // 6. 控えを戻して plan (now = ロードの時刻)
  const pf = planFromSnapshot({ productsRows: snaps.products.products, setRows: snaps.set_components.set_components,
    expected: { products: lm.products, set_components: lm.set_components }, now: new Date(Date.parse(load.started_at)), tmpRoot });
  if (!pf.ok) return block(pf.reason, { entity: pf.entity });
  const plan = pf.plan;

  // 7. 今の Company DB (② も同じものを使う)
  const cdb = await readCdbMaster(db);
  const { skuByNorm, costs, primary, comps } = cdb;

  // 8. 比べる
  const items = [];
  const compared = Object.fromEntries(ITEM_TYPES.map((t) => [t, new Set()]));
  const exclusions = {};   // subject key → 仕様で対象外にした理由 (見張りが「回復」にしない)
  const exclude = (type, norm, reason) => { if (norm) exclusions[subjectKey(type, norm)] = reason; };
  const skipped = new Map((D.skus.skipped || []).map(([code, reason]) => [code, reason]));
  const planByNorm = new Map();
  for (const s of plan.skus) {
    if (skipped.has(s.code)) continue;   // 正規化の衝突で落とした表記 (採用した方が同じ norm で比べられる)
    const norm = normSku(s.code);
    if (!norm) continue;
    planByNorm.set(norm, s);
    compared.missing.add(norm);
    const row = skuByNorm.get(norm);
    if (!row) { items.push({ type: 'missing', code: s.code, norm }); continue; }
    compared.value.add(norm);
    const exp = skuValuesForLoad(s);
    const diffs = [];
    for (const [col, key] of SKU_OWNED_COLUMNS) {
      if (!loadOwned(key)) continue;
      if (!has0027 && SKU_0027_COLUMNS.includes(col)) continue;
      if (!sameValue(exp[col], row[col])) diffs.push({ col, expected: exp[col] ?? null, actual: row[col] ?? null });
    }
    if (diffs.length) items.push({ type: 'value', code: s.code, norm, sku_id: row.sku_id, diffs });
  }
  // 原価 (ロードの時に持ち主が load のときだけ。書かなかった原価 = 無効・空 は仕様で対象外)
  if (D.sku_costs.owned) {
    const costSkipped = new Set((D.sku_costs.skipped || []).map(([code]) => code));
    for (const [norm, s] of planByNorm) {
      if (!s.cost) continue;
      const c = costSkipped.has(s.code) ? null : costForLoad(s.cost);
      if (!c) { exclude('cost', norm, 'invalid_cost'); continue; }
      if (!skuByNorm.has(norm)) continue;   // missing で出す
      compared.cost.add(norm);
      const cur = costs.get(norm);
      const diffs = [];
      for (const col of ['cost_jpy', 'cost_source', 'cost_status']) if (!sameValue(c[col], cur ? cur[col] : null)) diffs.push({ col, expected: c[col], actual: cur ? cur[col] : null });
      if (diffs.length) items.push({ type: 'cost', code: s.code, norm, diffs });
    }
  }
  // 代表の仕入先 (ロードが確かめた後の対象全体。触らなかった SKU は仕様で対象外)
  if (D.primary_suppliers.applied) {
    for (const [skuCode, supCode] of D.primary_suppliers.targets || []) {
      const norm = normSku(skuCode); const want = normSku(supCode);
      if (!norm || !skuByNorm.has(norm)) continue;
      compared.primary_supplier.add(norm);
      const actual = primary.get(norm) || [];
      if (!(actual.length === 1 && actual[0] === want)) items.push({ type: 'primary_supplier', code: skuCode, norm, expected: want, actual });
    }
    for (const [skuCode, , reason] of D.primary_suppliers.unresolved || []) exclude('primary_supplier', normSku(skuCode), reason || 'unresolved');
  }
  // セット構成 (書こうとした行は片方向・削除まで行った親は source = ne の子の集合を双方向。manual で残した行は対象外)
  if (D.set_components.owned) {
    const byParent = new Map();   // parent_norm → { code, rows: Map(child_norm → {qty, kind, child}) }
    for (const [parent, child, qty, kind] of D.set_components.rows || []) {
      const pn = normSku(parent), cn = normSku(child);
      if (!byParent.has(pn)) byParent.set(pn, { code: parent, rows: new Map() });
      byParent.get(pn).rows.set(cn, { qty: Number(qty), kind, child });
    }
    const prune = new Set((D.set_components.prune_parents || []).map(([code]) => normSku(code)));
    for (const [pn, g] of byParent) {
      compared.components.add(pn);
      const cur = comps.get(pn) || new Map();
      const missing = [], qty = [], extra = [];
      for (const [cn, e] of g.rows) {
        const c = cur.get(cn);
        if (!c) { missing.push({ child: e.child, qty: e.qty }); continue; }
        if (c.qty !== e.qty) qty.push({ child: e.child, expected: e.qty, actual: c.qty });
        if (e.kind === 'load' && c.source === 'manual') qty.push({ child: e.child, expected_source: 'ne', actual_source: 'manual' });
      }
      if (prune.has(pn)) for (const [cn, c] of cur) if (c.source !== 'manual' && !g.rows.has(cn)) extra.push({ child: cn, qty: c.qty, source: c.source });
      if (missing.length || qty.length || extra.length) items.push({ type: 'components', code: g.code, norm: pn, pruned: prune.has(pn), missing, qty, extra });
    }
  }

  // 9. ロードの後の変更の「候補」(時刻だけで「後に変更なし」とは言わない。Codex B-R0 #8)
  const ids = [...new Set(items.map((i) => skuByNorm.get(i.norm)?.sku_id).filter(Boolean))];
  if (ids.length && await tableExists(db, 'events', 'master_change_events')) {
    const ev = await rowsOf(db, `select e.entity_type, e.operation, e.attribute, e.old_value, e.new_value, e.actor_type, e.actor_id, e.source_system, e.run_id, e.recorded_at::text as recorded_at,
        coalesce(case when e.entity_type = 'sku' then e.entity_id end, (e.entity_key->>'sku_id')::bigint, (e.entity_key->>'parent_sku_id')::bigint, sc.sku_id) as sku_id
      from events.master_change_events e left join core.sku_costs sc on e.entity_type = 'sku_cost' and sc.sku_cost_id = e.entity_id
      where e.recorded_at >= $1::timestamptz and e.entity_type in ('sku', 'sku_cost', 'supplier_sku', 'sku_component')
        and coalesce(case when e.entity_type = 'sku' then e.entity_id end, (e.entity_key->>'sku_id')::bigint, (e.entity_key->>'parent_sku_id')::bigint, sc.sku_id) = any($2::bigint[])
      order by e.recorded_at desc limit 2000`, [load.started_at, ids]);
    const bySku = new Map();
    for (const e of ev) { const k = String(e.sku_id); if (!bySku.has(k)) bySku.set(k, []); if (bySku.get(k).length < 5) bySku.get(k).push(e); }
    for (const i of items) { const sid = skuByNorm.get(i.norm)?.sku_id; if (sid && bySku.has(String(sid))) i.change_candidates = bySku.get(String(sid)); }
  }
  for (const i of items) i.subject_key = subjectKey(i.type, i.norm);
  out.items = items;
  out.compared = Object.fromEntries(Object.entries(compared).map(([k, v]) => [k, [...v].sort()]));
  out.exclusions = exclusions;
  out.counts = { plan_skus: plan.skus.length, items: items.length, by_type: Object.fromEntries(ITEM_TYPES.map((t) => [t, items.filter((i) => i.type === t).length])),
    compared: Object.fromEntries(Object.entries(compared).map(([k, v]) => [k, v.size])), exclusions: Object.keys(exclusions).length };
  out.verdict = items.length ? 'breach' : 'pass';
  // ② に渡す (全件 JSON には出ない)。t_load = この plan・判断 D・持ち主・条件・① の差
  Object.defineProperty(out, LOAD_CTX, { value: { plan, D, ownership, has0027, cdb, load, items }, enumerable: false });
  return out;
}
