/**
 * compare-ne.mjs — 毎朝のマスタ照合 ②外との照合 = Company DB ↔ NE の「最後まで取れた回」の集合 (Company DB構想 10 §6.1.1 C2 v3〜v6。Codex C2-R0〜R2)
 *
 * 考え方: 次の夜間ロードの結果は予測しない。4 つの値を比べて、NE と Company DB の差を事実で分ける
 *   n = NE (完了した集合・元の値 *_src から値の状態) / c = Company DB の今 /
 *   t_today = 今朝の材料 (今朝の作り直しから作って Render に送った世代 G_today) / t_load = 昨夜のロードが読んだ材料 (① の控え G_load)
 *   A 昨夜の適用 (c と t_load) と B 今朝の変更 (t_load と t_today) を別々に持ち、今朝の変更で昨夜の差を吸収しない
 *   「反映待ち」(lag) は期限の台帳 (pending.mjs) で最大 1 回の夜間ロードまで。期限を過ぎても差が残れば別の分類に変わる
 * 🚨 判定できないときは blocked (「差 0」と言わない)。値の状態が不明・不正 = incomparable (一致にしない・回復にしない)
 * 🚨 ロードの判断の記録 (0030) で説明済みにするのは、記録した保持状態と今の c が一致するときだけ (C2 v6-1)
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import Database from 'better-sqlite3';
import { normSku } from '../../../lib/sku-norm.js';
import { mapHandling, mapTaxRate, canonicalSupplierCode, yenOrNull } from '../load/sources.mjs';
import { skuValuesForLoad, costForLoad, SKU_OWNED_COLUMNS, SKU_0027_COLUMNS } from '../load/engine.mjs';
import { readMaterialSnapshot } from '../../warehouse/material-lineage.js';
import { readEvidence } from '../push/evidence.mjs';
import { planFromSnapshot, subjectKey, sameValue } from './compare-load.mjs';

export const NE_FORMAT = 'mc-ne-v1';
/** NE の取扱区分で知っている語 (2026-09-26 の実データ。これ以外は invalid = 照合しない。今のロードの mapHandling は知らない語も discontinued にする) */
export const HANDLING_WORDS = Object.freeze(['取扱中', '取扱中止', 'ﾒｰｶｰ取扱中止']);
export const PROBLEM_TYPES = Object.freeze(['value', 'cost', 'primary_supplier', 'components', 'only_in_ne', 'only_in_cdb', 'kind']);
/** 判明した差 (案件の集約で breach)。blocked / incomparable は保持、match だけで回復 (C2 v5-4) */
export const KNOWN_DIFF = Object.freeze(['rule', 'rule_lag', 'lag', 'ne_no_value', 'held_by_load', 'load_mismatch', 'unexplained', 'arrival_unknown', 'not_delivered',
  'not_delivered_by_load', 'direction_unknown', 'spec_undecided']);
const KNOWN = new Set(KNOWN_DIFF);
/** 判断の一覧に載せる分類 */
const DECISION_CLASSES = new Set(['rule', 'rule_lag', 'held_by_load', 'spec_undecided', 'ne_no_value']);
/** 承認の指紋の「意味の版」(理由の種類ごとに手で上げる。C2 v4 §6) */
export const SEMANTIC_VERSIONS = Object.freeze({ tax_fallback: 1, tax_unresolved: 1, exception_cost: 1, exception_tax_manual: 1, set_name_blank: 1, set_price_from_goods: 1,
  set_tax_from_components: 1, not_in_latest_fetch: 1, 'load_rule:name_blank_to_code': 1, manual: 1, held_by_load: 1, spec_undecided: 1, ne_no_value: 1, none: 1 });
/** 作り直しの理由の列 → 照合の列 */
const BUILD_COL = { cost: 'cost', tax_rate: 'tax_rate', name: 'name', price: 'standard_price_jpy' };
/** 理由の種類ごとに承認の指紋へ入れる項目 (raw_synced_at など毎朝変わるものは入れない。C2 v4 §6 / Codex C2-R0 M7) */
const REASON_FIELDS = { tax_fallback: ['source', 'value'], exception_cost: ['value'], exception_tax_manual: ['value'], set_price_from_goods: ['value'],
  set_tax_from_components: ['value', 'category'], set_name_blank: [], not_in_latest_fetch: [], tax_unresolved: [], 'load_rule:name_blank_to_code': [] };

/** 「ロードは触らない (保持)」= この列の値を材料が持たない (原価が無い・代表の仕入先が空・構成が 0 行)。ABSENT = その SKU が材料に無い */
export const PRESERVE = '__load_preserves__';
export const ABSENT = '__absent__';
const isMarker = (v) => v === PRESERVE || v === ABSENT;

// ─────────── 値の状態 (C2 v4 §2) ───────────
const NUM_RE = /^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$/;
/**
 * 数値の列の値の状態。src = *_src (JSON の文字列。SQL の NULL = 記録なし)。kind = yen (原価・売価) / tax / qty
 * @returns {{ raw: 'value'|'empty'|'zero'|'null'|'unknown', validity: 'ok'|'invalid', value: any }}
 */
export function numState(src, kind) {
  if (src == null) return { raw: 'unknown', validity: 'invalid', value: null };
  let v;
  try { v = JSON.parse(src); } catch { return { raw: 'unknown', validity: 'invalid', value: null }; }
  if (v === null) return { raw: 'null', validity: kind === 'qty' ? 'invalid' : 'ok', value: null };
  let x;
  if (typeof v === 'number') x = v;
  else if (typeof v === 'string') {
    const t = v.trim();
    if (t === '') return { raw: 'empty', validity: kind === 'qty' ? 'invalid' : 'ok', value: null };
    if (!NUM_RE.test(t)) return { raw: 'value', validity: 'invalid', value: null, text: t };
    x = Number(t);
  } else return { raw: 'value', validity: 'invalid', value: null };
  if (!Number.isFinite(x)) return { raw: 'value', validity: 'invalid', value: null };
  const raw = x === 0 ? 'zero' : 'value';
  if (kind === 'yen') { const y = yenOrNull(x); return y == null ? { raw, validity: 'invalid', value: null } : { raw, validity: 'ok', value: y }; }
  if (kind === 'tax') { const r = mapTaxRate(x); return r == null ? { raw, validity: 'invalid', value: null } : { raw, validity: 'ok', value: r }; }
  return Number.isInteger(x) && x > 0 ? { raw, validity: 'ok', value: x } : { raw, validity: 'invalid', value: null };   // qty
}
/** 文字の列 (名前・仕入先・取扱区分)。NE の取込は `x || ''` で保存 = null・欠落・空文字は区別できない → empty にまとめる */
export function textState(raw, kind) {
  const t = raw == null ? '' : String(raw).trim();
  if (!t) return { raw: 'empty', validity: 'ok', value: null };
  if (kind === 'handling') return HANDLING_WORDS.includes(t) ? { raw: 'value', validity: 'ok', value: mapHandling(t) } : { raw: 'value', validity: 'invalid', value: null, text: t };
  if (kind === 'supplier') return { raw: 'value', validity: 'ok', value: normSku(canonicalSupplierCode(t)) };
  return { raw: 'value', validity: 'ok', value: t };
}
/** 比べやすさ (C2 v5-3): comparable / no_value (NE に値が無い) / incomparable (不明・不正) */
export function comparability(st) {
  if (!st || st.raw === 'unknown' || st.validity === 'invalid') return 'incomparable';
  if (st.raw === 'empty' || st.raw === 'zero' || st.raw === 'null') return 'no_value';
  return 'comparable';
}
/** Company DB の形の値が同じか (印は同じ印どうしだけ・配列は並べて比べる) */
export function eqv(a, b) {
  if (isMarker(a) || isMarker(b)) return a === b;
  if (Array.isArray(a) || Array.isArray(b)) {
    if (!Array.isArray(a) || !Array.isArray(b)) return false;
    const x = [...a].sort(), y = [...b].sort();
    return x.length === y.length && x.every((v, i) => v === y[i]);
  }
  return sameValue(a, b);
}
const show = (v) => (v === PRESERVE ? '(ロードは触らない)' : v === ABSENT ? '(無い)' : v === undefined ? null : v);

// ─────────── 承認の指紋 (C2 v4 §6・M7) ───────────
function canon(v) {
  if (Array.isArray(v)) return v.map(canon);
  if (v && typeof v === 'object') return Object.fromEntries(Object.keys(v).sort().map((k) => [k, canon(v[k])]));
  if (typeof v === 'number') return Number.isFinite(v) ? Math.round(v * 1e6) / 1e6 : null;
  return v === undefined ? null : v;
}
export function approvalFingerprint(p) {
  return crypto.createHash('sha256').update(JSON.stringify(canon(p))).digest('hex');
}
/**
 * 承認の指紋の元 (C2 v4 §6・v6。Codex C2-R0 M7): 対象・種別・列・問題の種類・持ち主・理由の種類と中身 (種類ごとに決めた項目)・n の状態と値・c・提案・意味の版。
 * 作り直しの ID・時刻・ファイルの指紋は入れない
 */
export function decisionPrint({ norm, kind, col, child = null, problem, owner, reasonKind, reason, n_state, n, c, proposal }, versions = SEMANTIC_VERSIONS) {
  return { code_norm: norm, sku_kind: kind, col, child, problem, owner: owner ?? null, reason_kind: reasonKind,
    reason: reasonKind === 'manual' ? { child: reason?.child ?? null, manual_qty: reason?.manual_qty ?? null, ne_qty: reason?.ne_qty ?? null }
      : reasonKind === 'held_by_load' ? { reason_code: reason?.reason_code ?? null } : reasonForPrint(reason),
    n_state: n_state ?? null, n: n ?? null, c: c ?? null, proposal, semantic: `${reasonKind}@${versions[reasonKind] ?? 1}` };
}
function reasonForPrint(r) {
  if (!r) return null;
  const fields = REASON_FIELDS[r.reason] || [];
  return { reason: r.reason, ...Object.fromEntries(fields.map((f) => [f, r[f] ?? null])) };
}

// ─────────── NE 側 (warehouse.db を読み取り専用で 1 つの読み取り取引) ───────────
/** @returns {{ error?: string, meta, products, sets, build, hasSrc }} */
export function readNeSide(dataDir) {
  const file = path.join(dataDir, 'warehouse.db');
  if (!fs.existsSync(file)) return { error: 'no_warehouse_db' };
  const db = new Database(file, { readonly: true, fileMustExist: true });
  try {
    db.exec('BEGIN');
    try {
      const has = (t, c) => db.prepare(`PRAGMA table_info(${t})`).all().some((x) => x.name === c);
      const meta = Object.fromEntries(db.prepare("SELECT key, value FROM sync_meta WHERE key LIKE 'ne_api_%' OR key LIKE 'ne_raw_%'").all().map((r) => [r.key, r.value]));
      const hasSrc = has('raw_ne_products', '原価_src') && has('raw_ne_set_products', '数量_src') && has('m_products_builds', 'ne_products_complete_rev');
      const pAt = meta.ne_api_products_complete_at ?? null, sAt = meta.ne_api_setproducts_complete_at ?? null;
      const products = pAt && hasSrc ? db.prepare(`SELECT 商品コード AS code, 商品名 AS name, 仕入先コード AS supplier, 取扱区分 AS handling,
        原価_src AS cost_src, 売価_src AS price_src, 消費税率_src AS tax_src FROM raw_ne_products WHERE synced_at = ?`).all(pAt) : [];
      const sets = sAt && hasSrc ? db.prepare(`SELECT セット商品コード AS parent, セット商品名 AS name, 商品コード AS child, セット販売価格_src AS price_src, 数量_src AS qty_src
        FROM raw_ne_set_products WHERE synced_at = ?`).all(sAt) : [];
      const setRowsTotal = db.prepare('SELECT COUNT(*) AS c FROM raw_ne_set_products').get().c;
      const build = db.prepare('SELECT * FROM m_products_builds ORDER BY published_at DESC LIMIT 1').get() ?? null;
      if (build) { try { build.reasons = JSON.parse(build.reasons || '[]'); } catch { build.reasons = null; } }
      return { meta, products, sets, setRowsTotal, build, hasSrc };
    } finally { db.exec('COMMIT'); }
  } finally { db.close(); }
}
/** sync_meta の時刻 ('YYYY-MM-DD HH:MM:SS' = UTC。db.js の now()) → JST の日付 */
export const jstDateOfUtcText = (t) => {
  const ms = Date.parse(`${String(t).replace(' ', 'T')}Z`);
  return Number.isFinite(ms) ? new Date(ms + 9 * 3600000).toISOString().slice(0, 10) : null;
};
const jstDateOfIso = (iso) => { const ms = Date.parse(iso); return Number.isFinite(ms) ? new Date(ms + 9 * 3600000).toISOString().slice(0, 10) : null; };
/** 世代 ID (mat_<UTC ミリ秒>_...) の時刻 */
export const generationTime = (id) => {
  const m = String(id || '').match(/^mat_(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})(\d{3})Z_/);
  return m ? new Date(Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], +m[6], +m[7])).toISOString() : null;
};

// ─────────── 3 つの形をそろえる ───────────
function nModelOf(ne) {
  const m = new Map();
  const setNorms = new Set(ne.sets.map((r) => normSku(r.parent)).filter(Boolean));
  for (const r of ne.products) {
    const norm = normSku(r.code); if (!norm || setNorms.has(norm)) continue;   // セットの表にあるコードはセット (商品の表にもあるのは正常)
    m.set(norm, { code: r.code, kind: 'single', cols: {
      name: textState(r.name, 'name'), handling: textState(r.handling, 'handling'), tax_rate: numState(r.tax_src, 'tax'),
      standard_price_jpy: numState(r.price_src, 'yen'), cost: numState(r.cost_src, 'yen'), primary_supplier: textState(r.supplier, 'supplier') } });
  }
  for (const r of ne.sets) {
    const norm = normSku(r.parent); if (!norm) continue;
    if (!m.has(norm)) m.set(norm, { code: r.parent, kind: 'set', cols: { name: textState(r.name, 'name'), standard_price_jpy: numState(r.price_src, 'yen') }, children: new Map() });
    const cn = normSku(r.child); if (cn) m.get(norm).children.set(cn, { code: r.child, st: numState(r.qty_src, 'qty') });
  }
  return m;
}
/** plan → 材料の形 (ロードと同じ関数)。正規化で衝突した表記はロードと同じく先に来た方 */
function tModelOf(plan) {
  const m = new Map();
  const primary = new Map((plan.primarySuppliers || []).map((x) => [normSku(x.skuCode), normSku(x.supplierCode)]));
  const comps = new Map();
  for (const c of plan.setComponents || []) {
    const pn = normSku(c.parentCode), cn = normSku(c.childCode); if (!pn || !cn) continue;
    if (!comps.has(pn)) comps.set(pn, new Map());
    if (!comps.get(pn).has(cn)) comps.get(pn).set(cn, Number(c.qty));
  }
  for (const s of plan.skus || []) {
    const norm = normSku(s.code); if (!norm || m.has(norm)) continue;
    const v = skuValuesForLoad(s);
    const cost = s.cost ? costForLoad(s.cost) : null;
    m.set(norm, { code: s.code, kind: s.kind, vals: { name: v.name, handling: v.handling, tax_rate: v.tax_rate, standard_price_jpy: v.standard_price_jpy,
      cost: cost ? cost.cost_jpy : PRESERVE, primary_supplier: primary.has(norm) ? primary.get(norm) : PRESERVE, kind: s.kind, exists: true },
      children: comps.get(norm) || null });
  }
  return m;
}
const cValue = (cdb, norm, col) => {
  const r = cdb.skuByNorm.get(norm);
  if (col === 'exists') return !!r;
  if (!r) return ABSENT;
  if (col === 'kind') return r.sku_kind;
  if (col === 'cost') { const c = cdb.costs.get(norm); return c ? Number(c.cost_jpy) : null; }
  if (col === 'primary_supplier') return [...(cdb.primary.get(norm) || [])].sort();
  return r[col] ?? null;
};
const tValue = (tm, norm, col) => {
  const t = tm.get(norm);
  if (col === 'exists') return !!t;
  if (!t) return ABSENT;
  return t.vals[col];
};

/**
 * ② 外との照合。戻り値 = 全件 JSON の ne 節 + pendingEntries (台帳に書く中身。JSON には入れない)
 * @param {object} p
 * @param {string} p.dataDir     miniPC の DATA_DIR (warehouse.db・控え・証跡)
 * @param {string} p.asOfJst
 * @param {string|null} p.syncRunId  今日の daily-sync の実行 ID (無ければ日付だけで鮮度を見る)
 * @param {object|null} p.loadCtx    ① の LOAD_CTX (① が判定できたときだけ = P4)
 * @param {object} p.cdb             readCdbMaster の結果 (① と同じ snapshot)
 * @param {object} p.ledger          readLedger の結果 ({ state, entries })
 * @param {string} p.loadVerdict     ① の verdict
 */
export function compareNe({ dataDir, asOfJst, syncRunId = null, loadCtx = null, cdb, ledger, loadVerdict = null, tmpRoot }) {
  const out = { format: NE_FORMAT, verdict: null, blocked_reason: null, prerequisites: {}, generation: null, build: null, ne_marks: null,
    items: [], held: {}, recoverable: [], out_of_scope: {}, decisions: [], raw_diffs: [], counts: {}, pending: { state: ledger?.state ?? null, reason: ledger?.reason ?? null } };
  const pre = out.prerequisites;
  const block = (reason, extra = {}) => { Object.assign(out, { verdict: 'blocked', blocked_reason: reason }, extra); return { result: out, pendingEntries: null }; };

  // ── 1. NE の集合・印・作り直しの記録 ──
  const ne = readNeSide(dataDir);
  if (ne.error) return block(ne.error);
  if (!ne.hasSrc) return block('no_src_columns');   // C1 の前の warehouse.db
  const M = ne.meta;
  const marks = { products: { at: M.ne_api_products_complete_at ?? null, count: M.ne_api_products_complete_count ?? null, rev: M.ne_api_products_complete_rev ?? null },
    sets: { at: M.ne_api_setproducts_complete_at ?? null, count: M.ne_api_setproducts_complete_count ?? null, rev: M.ne_api_setproducts_complete_rev ?? null, parents: M.ne_api_setproducts_complete_parents ?? null },
    cur_rev: { products: M.ne_raw_products_rev ?? null, sets: M.ne_raw_setproducts_rev ?? null } };
  out.ne_marks = marks;
  if (!marks.products.at || !marks.sets.at || marks.products.rev == null || marks.sets.rev == null) return block('no_ne_marks');
  const nm = nModelOf(ne);
  // 前提が欠けても、NE の集合が読めていれば「値の差」だけは一覧に出す (分類はしない)
  const rawDiffs = () => {
    const d = [];
    for (const [norm, n] of nm) {
      if (!cdb.skuByNorm.has(norm)) continue;
      for (const [col, st] of Object.entries(n.cols)) {
        if (comparability(st) !== 'comparable' || (col === 'standard_price_jpy' && !cdb.has0027) || (col === 'primary_supplier' && !cdb.has0027)) continue;
        const c = cValue(cdb, norm, col);
        if (!eqv(col === 'primary_supplier' ? [st.value] : st.value, c)) d.push({ key: subjectKey(col === 'cost' || col === 'primary_supplier' ? col : 'value', norm), col, n: st.value, c });
      }
    }
    return d;
  };
  // ── 2. 鮮度 (C2 v4 §1・Codex C2-R0 M4 = 印も作り直しも古い朝を通さない) ──
  pre.freshness = { products_at_jst: jstDateOfUtcText(marks.products.at), sets_at_jst: jstDateOfUtcText(marks.sets.at), build: ne.build ? ne.build.build_id : null,
    build_date: ne.build ? jstDateOfIso(ne.build.published_at) : null, build_run: ne.build ? ne.build.daily_sync_run_id : null, sync_run_id: syncRunId };
  if (pre.freshness.products_at_jst !== asOfJst || pre.freshness.sets_at_jst !== asOfJst) return block('stale_ne', { raw_diffs: rawDiffs() });
  if (!ne.build || pre.freshness.build_date !== asOfJst || (syncRunId && ne.build.daily_sync_run_id !== syncRunId)) return block('stale_build', { raw_diffs: rawDiffs() });
  out.build = { build_id: ne.build.build_id, published_at: ne.build.published_at, daily_sync_run_id: ne.build.daily_sync_run_id };
  // ── 3. 材料の信用 ──
  const parents = new Set(ne.sets.map((r) => r.parent));
  pre.trust = { products_rows: ne.products.length, sets_rows: ne.sets.length, sets_total: ne.setRowsTotal, parents: parents.size };
  if (String(marks.cur_rev.products) !== String(marks.products.rev) || String(marks.cur_rev.sets) !== String(marks.sets.rev)) return block('ne_written_after_mark', { raw_diffs: rawDiffs() });
  if (String(ne.products.length) !== String(marks.products.count) || String(ne.sets.length) !== String(marks.sets.count) || ne.sets.length !== ne.setRowsTotal) return block('ne_count_mismatch', { raw_diffs: rawDiffs() });
  if (marks.sets.parents == null) return block('no_complete_parents', { raw_diffs: rawDiffs() });
  if (String(parents.size) !== String(marks.sets.parents)) return block('ne_parents_mismatch', { raw_diffs: rawDiffs() });
  const b = ne.build;
  if (b.ne_products_complete_at !== marks.products.at || b.ne_setproducts_complete_at !== marks.sets.at
    || String(b.ne_products_complete_rev) !== String(marks.products.rev) || String(b.ne_setproducts_complete_rev) !== String(marks.sets.rev)) return block('build_ne_mismatch', { raw_diffs: rawDiffs() });
  if (!Array.isArray(b.reasons)) return block('build_reasons_unreadable', { raw_diffs: rawDiffs() });
  // 今朝の世代 (Render 到達の証跡の最後の世代。その作り直し = 今朝の作り直し)
  let ev;
  try { ev = readEvidence(dataDir, asOfJst)['render-master'] ?? null; } catch { ev = null; }
  if (!ev || !ev.generation_id) return block('no_render_master', { raw_diffs: rawDiffs() });
  out.generation = { generation_id: ev.generation_id, build_id: ev.build_id ?? null, created_at: generationTime(ev.generation_id) };
  if (ev.build_id !== b.build_id) return block('generation_build_mismatch', { raw_diffs: rawDiffs() });
  const expected = {};
  for (const e of ['products', 'set_components']) {
    const req = ev.entities && ev.entities[e] && ev.entities[e].requested;
    if (!req || typeof req.content_hash !== 'string') return block('no_generation_hash', { entity: e, raw_diffs: rawDiffs() });
    expected[e] = req;
  }
  let snap;
  try { snap = readMaterialSnapshot({ dataDir, generationId: ev.generation_id, expected: { products: expected.products.content_hash, set_components: expected.set_components.content_hash } }); }
  catch (e) { return block(e && e.code === 'MATERIAL_HASH_MISMATCH' ? 'snapshot_mismatch' : 'snapshot_unreadable', { raw_diffs: rawDiffs() }); }
  if (!snap) return block('snapshot_missing', { raw_diffs: rawDiffs() });
  const pf = planFromSnapshot({ productsRows: snap.products, setRows: snap.set_components, expected, now: new Date(Date.parse(b.published_at)), ...(tmpRoot ? { tmpRoot } : {}) });
  if (!pf.ok) return block(pf.reason, { raw_diffs: rawDiffs() });
  const tToday = tModelOf(pf.plan);
  const blankName = new Set(snap.products.filter((r) => !String(r['商品名'] ?? '').trim()).map((r) => normSku(r['商品コード'])));
  // ── 4. 到達 (信用とは別) ──
  const arrivalOf = (st) => (st === 'recorded' ? 'confirmed' : st === 'unconfirmed' ? 'unknown' : 'not_delivered');
  pre.arrival = { products: arrivalOf(ev.entities.products?.status), set_components: arrivalOf(ev.entities.set_components?.status) };
  // ── 5. 取込の整合 (C1 + C2) ──
  let ip, is;
  try { ip = JSON.parse(M.ne_api_products_integrity); is = JSON.parse(M.ne_api_setproducts_integrity); } catch { return block('no_integrity', { raw_diffs: rawDiffs() }); }
  const okInt = ip && Array.isArray(ip.dup_codes) && Number.isFinite(ip.dropped_no_code) && is && Array.isArray(is.parent_conflicts) && Array.isArray(is.pair_dups) && Number.isFinite(is.dropped_missing_key);
  if (!okInt) return block('no_integrity', { raw_diffs: rawDiffs() });
  const c2Form = Number.isFinite(is.dropped_missing_parent) && Array.isArray(is.missing_child_parents);
  const intBlocked = new Map();   // norm → 理由
  for (const c of ip.dup_codes) intBlocked.set(normSku(c), 'dup_code');
  for (const c of is.parent_conflicts) intBlocked.set(normSku(c), 'parent_conflict');
  for (const d of is.pair_dups) intBlocked.set(normSku(d.parent), 'pair_dup');
  if (c2Form) for (const c of is.missing_child_parents) intBlocked.set(normSku(c), 'missing_child');
  // どの行が落ちたか分からない = 「NE の表に無い」を根拠にする判定を止める
  const absenceUntrusted = ip.dropped_no_code > 0 || (c2Form ? is.dropped_missing_parent > 0 : is.dropped_missing_key > 0);
  const componentsUntrusted = !c2Form && is.dropped_missing_key > 0;
  pre.integrity = { blocked_skus: intBlocked.size, absence_untrusted: absenceUntrusted, components_untrusted: componentsUntrusted, form: c2Form ? 'c2' : 'c1' };
  // ── 6. 昨夜のロード (P4) と台帳 ──
  const p4 = !!loadCtx && (loadVerdict === 'pass' || loadVerdict === 'breach');
  pre.load_basis = p4 ? { ingest_run_id: loadCtx.load.ingest_run_id, started_at: loadCtx.load.started_at } : { missing: true, load_verdict: loadVerdict };
  const tLoad = p4 ? tModelOf(loadCtx.plan) : null;
  const loadStartMs = p4 ? Date.parse(loadCtx.load.started_at) : null;
  const ledgerOk = ledger && (ledger.state === 'ok' || ledger.state === 'initial');
  pre.ledger = { state: ledger?.state ?? null, reason: ledger?.reason ?? null };
  const own = p4 ? loadCtx.ownership : null;
  const ownerKey = (col) => ({ name: 'skus.name', handling: 'skus.handling', tax_rate: 'skus.tax_rate', kind: 'skus.sku_kind', cost: 'sku_costs', primary_supplier: 'supplier_skus.is_primary', components: 'sku_components' })[col]
    ?? (SKU_OWNED_COLUMNS.find(([c]) => c === col) || [])[1] ?? null;
  const loadOwns = (col) => {
    if (col === 'exists') return true;   // SKU の INSERT は持ち主で止めない (engine)
    const k = ownerKey(col); if (!k || !own) return false;
    if ((SKU_0027_COLUMNS.includes(col) || col === 'primary_supplier') && !loadCtx.has0027) return false;
    return own[k] === 'load';
  };
  // ① の差 (load_mismatch を列・子の行で引く)
  const loadItems = new Map(p4 ? loadCtx.items.map((i) => [i.subject_key || subjectKey(i.type, i.norm), i]) : []);
  const loadFlagged = (type, norm, col, child) => {
    if (!p4) return false;
    if (type === 'value' || type === 'kind') { const i = loadItems.get(subjectKey('value', norm)); return !!i && i.diffs.some((d) => d.col === (col === 'kind' ? 'sku_kind' : col)); }
    if (type === 'cost') return loadItems.has(subjectKey('cost', norm));
    if (type === 'primary_supplier') return loadItems.has(subjectKey('primary_supplier', norm));
    if (type === 'only_in_ne') return loadItems.has(subjectKey('missing', norm));
    if (type === 'components') { const i = loadItems.get(subjectKey('components', norm)); return !!i && [...(i.missing || []), ...(i.qty || []), ...(i.extra || [])].some((x) => normSku(x.child) === child); }
    return false;
  };
  // 0030 の記録で説明できるか (記録した保持状態と今の c が一致するときだけ。C2 v6-1)
  const D = p4 ? loadCtx.D : null;
  const explainByDecision = (type, norm, col, child, c, tl) => {
    if (!D) return null;
    if (type === 'components') {
      const cRow = cdb.comps.get(norm)?.get(child) ?? null;
      for (const [p, ch, why, extra] of D.set_components.skipped || []) {
        if (normSku(p) !== norm || normSku(ch) !== child || !extra || typeof extra !== 'object') continue;
        if (why === 'manual_qty_mismatch') {
          if (cRow && cRow.source === 'manual' && cRow.qty === Number(extra.manual_qty) && tl === Number(extra.plan_qty)) return { cls: 'rule', reason: { reason: 'manual', child, manual_qty: cRow.qty, ne_qty: tl } };
          continue;
        }
        const h = extra.held;
        if (h === null && !cRow) return { cls: 'held_by_load', reason: { reason: 'held_by_load', reason_code: why } };
        if (h && typeof h === 'object' && cRow && cRow.qty === Number(h.qty) && cRow.source === h.source) return { cls: 'held_by_load', reason: { reason: 'held_by_load', reason_code: why } };
      }
      for (const [pid, cid, qty] of D.set_components.manual_kept_on_prune || []) {
        if (qty == null || cdb.idToNorm.get(Number(pid)) !== norm || cdb.idToNorm.get(Number(cid)) !== child) continue;
        if (cRow && cRow.source === 'manual' && cRow.qty === Number(qty) && tl === ABSENT) return { cls: 'rule', reason: { reason: 'manual', child, manual_qty: cRow.qty, ne_qty: null } };
      }
      return null;
    }
    if (type === 'primary_supplier' && D.primary_suppliers.applied) {
      for (const [sk, , why, extra] of D.primary_suppliers.unresolved || []) {
        if (normSku(sk) !== norm || !extra || !Array.isArray(extra.held)) continue;
        if (eqv(extra.held, c)) return { cls: 'held_by_load', reason: { reason: 'held_by_load', reason_code: why } };
      }
    }
    return null;
  };

  /**
   * ロードが触らなかった列 (材料に値が無い = PRESERVE) が、ロードの記録した保持状態のまま残っているか (C2 v6-1。Codex #1464 R1 High 1)。
   * 原価: 0030 の原価の skip に保持状態 (金額・source・status / null = 有効な原価なし) があり、今の有効な原価がそれと同じとき。それ以外の列・記録が無い = 確かめられない = false
   */
  const preservedAsRecorded = (type, norm) => {
    if (!D || type !== 'cost') return false;
    for (const [code, , extra] of D.sku_costs.skipped || []) {
      if (normSku(code) !== norm || !extra || typeof extra !== 'object' || extra.held === undefined || extra.held === 'unknown') continue;
      const cur = cdb.costs.get(norm) || null;
      if (extra.held === null) return !cur;
      return !!cur && sameValue(Number(cur.cost_jpy), Number(extra.held.cost_jpy)) && cur.cost_source === extra.held.cost_source && cur.cost_status === extra.held.cost_status;
    }
    return false;
  };

  // ── 7. 分類 (C2 v5-3・v6-2) ──
  const reasonsBy = new Map();   // `${norm}|${col}` → [理由]
  const exceptionNorms = new Set();
  for (const r of b.reasons) {
    const norm = normSku(r.code); if (!norm) continue;
    if (r.kind === '例外') exceptionNorms.add(norm);
    const cols = r.col === '*' ? ['*'] : [BUILD_COL[r.col] || r.col];
    for (const col of cols) { const k = `${norm}|${col}`; if (!reasonsBy.has(k)) reasonsBy.set(k, []); reasonsBy.get(k).push(r); }
  }
  const reasonsFor = (norm, col) => [...(reasonsBy.get(`${norm}|${col}`) || []), ...(col === 'exists' ? reasonsBy.get(`${norm}|*`) || [] : [])];
  const newPending = new Map();   // 台帳の新しい中身
  const unitOf = (key, col, target) => `${key}|${col}|${crypto.createHash('sha256').update(JSON.stringify(canon(target))).digest('hex').slice(0, 16)}`;
  /**
   * 1 つの列 (構成は子の行) を分類する。n = 値の状態 / tt = t_today / tl = t_load (P4 が無ければ undefined) / c = Company DB
   * @returns {{ cls, why?, A?, detail }}
   */
  const classify = ({ key, type, norm, col, child = null, nst, nv, tt, tl, c, entity }) => {
    const detail = { n_state: nst ? nst.raw : null, n_validity: nst ? nst.validity : null, n: nst ? (nst.text ?? show(nv)) : null, t_today: show(tt), t_load: p4 ? show(tl) : '(判定できない)', c: show(c) };
    const reasons = reasonsFor(norm, col);
    if (reasons.length) detail.reasons = reasons.map((r) => ({ reason: r.reason, value: r.value ?? null, source: r.source ?? null }));
    // 反映待ちの台帳: 今朝の値がまだ Company DB と違う単位は、どの分類になっても前の始まりを保つ (① が判定できない朝を挟んでも期限をリセットしない。C2 v4 §4)
    const unit = !isMarker(tt) && tt !== undefined && !eqv(tt, c) ? unitOf(key, child ? `${col}:${child}` : col, tt) : null;
    const prev = unit && ledgerOk ? ledger.entries.get(unit) || null : null;
    if (prev) newPending.set(unit, prev);
    const comp = comparability(nst);
    if (comp === 'incomparable') return { cls: 'incomparable', detail };
    if (comp === 'no_value') return { cls: 'ne_no_value', detail };
    if (eqv(nv, c)) return { cls: 'match', detail };
    // A 昨夜の適用
    let A = null;
    if (p4) {
      // 順番 (Codex #1464 R1): ① がこの項目の差を出していれば load_mismatch が先 (金額・数量だけの一致で applied にしない = source の違いを隠さない)
      //   → ロードが触らなかった (PRESERVE) 列は、記録した保持状態と今の c が一致したときだけ applied (C2 v6-1。記録が無い・違う = unexplained)
      if (loadFlagged(type, norm, col, child)) A = 'load_mismatch';
      else if (tl === PRESERVE) A = preservedAsRecorded(type, norm) ? 'applied' : 'unexplained';
      else if (eqv(c, tl)) A = 'applied';
      else if (!loadOwns(col === 'exists' ? 'exists' : type === 'components' ? 'components' : col)) A = 'not_owned';
      else { const x = explainByDecision(type, norm, col, child, c, tl); A = x ? x : 'unexplained'; }
      if (A === 'unexplained' && tl === PRESERVE) detail.why_a = 'preserve_unverified';
      detail.A = typeof A === 'object' ? A.cls : A;
      detail.B = eqv(tl, tt) ? 'same' : 'changed';
    }
    if (A && A !== 'applied') {
      if (typeof A === 'object') { detail.reasons = [...(detail.reasons || []), A.reason]; return { cls: A.cls, detail, explained: A.reason }; }
      if (A === 'not_owned') return { cls: 'direction_unknown', detail };
      if (A === 'load_mismatch') return { cls: 'load_mismatch', detail };
      return { cls: 'unexplained', why: 'not_applied', detail };
    }
    const loadRule = col === 'name' && blankName.has(norm) ? [{ reason: 'load_rule:name_blank_to_code' }] : [];
    const why = [...reasons, ...loadRule];
    if (eqv(tt, c)) return why.length ? { cls: 'rule', detail, explained: why[0] } : { cls: 'unexplained', why: 'build_without_reason', detail };
    if (!p4) return { cls: 'blocked', why: 'no_load_basis', detail };
    // ここ = 昨夜は適用済み・今朝の値がまだロードに渡っていない
    if (isMarker(tt)) return why.length ? { cls: 'rule', detail, explained: why[0] } : { cls: 'unexplained', why: 'material_has_no_value', detail };
    if (!ledgerOk) return { cls: 'blocked', why: `pending_${ledger?.state ?? 'none'}`, detail };
    const arrival = pre.arrival[entity];
    detail.arrival = arrival;
    if (prev) {
      detail.pending_since = prev.start_at;
      if (loadStartMs > Date.parse(prev.start_at)) return { cls: 'not_delivered_by_load', detail };   // 期限のロードは済んだのに、その材料に目標値が無い
    } else if (arrival === 'confirmed') {
      const e = { unit, key, col: child ? `${col}:${child}` : col, target_hash: unit.split('|').pop(), start_generation: out.generation.generation_id, start_at: out.generation.created_at };
      newPending.set(unit, e); detail.pending_since = e.start_at;
    }
    if (!prev && arrival !== 'confirmed') return { cls: arrival === 'unknown' ? 'arrival_unknown' : 'not_delivered', detail };
    if (eqv(tt, nv)) return { cls: 'lag', detail };
    return why.length ? { cls: 'rule_lag', detail, explained: why[0] } : { cls: 'unexplained', why: 'build_without_reason', detail };
  };

  // ── 8. 案件ごとに評価して集約 ──
  const keys = new Map();   // key → { type, code, norm, kind, cols: [] }
  const addCol = (type, norm, code, kind, r, col, child) => {
    const key = subjectKey(type, norm);
    if (!keys.has(key)) keys.set(key, { subject_key: key, type, code, norm, kind, columns: [] });
    keys.get(key).columns.push({ col, ...(child ? { child } : {}), cls: r.cls, ...(r.why ? { why: r.why } : {}), ...r.detail, ...(r.explained ? { explained: r.explained } : {}) });
  };
  const holdKey = (type, norm, reason) => { out.held[subjectKey(type, norm)] = reason; };
  const carryKeys = new Set();   // 評価しきれなかった案件 = 台帳の単位をそのまま書き写す
  const universe = new Set([...nm.keys(), ...cdb.skuByNorm.keys()]);
  for (const norm of universe) {
    const n = nm.get(norm) || null;
    const cRow = cdb.skuByNorm.get(norm) || null;
    const code = n?.code ?? cRow?.code ?? norm;
    const tk = tToday.get(norm)?.kind;
    if (exceptionNorms.has(norm) || cRow?.sku_kind === 'exception' || tk === 'exception') {
      for (const t of PROBLEM_TYPES) out.out_of_scope[subjectKey(t, norm)] = 'exception_item';
      continue;
    }
    const blockedWhy = intBlocked.get(norm);
    if (blockedWhy) { for (const t of PROBLEM_TYPES) holdKey(t, norm, `ne_integrity:${blockedWhy}`); continue; }
    const entity = (n?.kind ?? cRow?.sku_kind) === 'set' ? 'set_components' : 'products';
    // 有無
    if (n && !cRow) {
      const r = classify({ key: subjectKey('only_in_ne', norm), type: 'only_in_ne', norm, col: 'exists', nst: { raw: 'value', validity: 'ok' }, nv: true,
        tt: tValue(tToday, norm, 'exists'), tl: tLoad ? tValue(tLoad, norm, 'exists') : undefined, c: false, entity: 'products' });
      addCol('only_in_ne', norm, code, n.kind, r, 'exists');
      for (const t of ['value', 'cost', 'primary_supplier', 'components', 'kind']) holdKey(t, norm, 'not_in_cdb');
      out.recoverable.push(subjectKey('only_in_cdb', norm));
      continue;
    }
    if (!n && cRow) {
      const key = subjectKey('only_in_cdb', norm);
      if (absenceUntrusted) { holdKey('only_in_cdb', norm, 'ne_dropped_rows'); for (const t of ['value', 'cost', 'primary_supplier', 'components', 'kind']) holdKey(t, norm, 'not_in_ne'); continue; }
      const inToday = tToday.has(norm);
      const reasons = reasonsFor(norm, 'exists');
      let r;
      if (cRow.sku_kind === 'set') r = { cls: 'spec_undecided', detail: { c: 'セット', t_today: inToday ? 'あり' : '(無い)' } };
      else if (!inToday) r = { cls: 'spec_undecided', detail: { c: '単品', t_today: '(無い)', note: '材料に無い SKU はロードが消さない (削除・保持の仕様は D)' } };
      else r = reasons.some((x) => x.reason === 'not_in_latest_fetch') ? { cls: 'rule', detail: { reasons: reasons.map((x) => ({ reason: x.reason })) }, explained: { reason: 'not_in_latest_fetch' } }
        : { cls: 'unexplained', why: 'cdb_only_without_reason', detail: {} };
      addCol('only_in_cdb', norm, code, cRow.sku_kind, r, 'exists');
      for (const t of ['value', 'cost', 'primary_supplier', 'components', 'kind']) holdKey(t, norm, 'not_in_ne');
      out.recoverable.push(subjectKey('only_in_ne', norm));
      continue;
    }
    // 両方にある
    out.recoverable.push(subjectKey('only_in_ne', norm));
    if (absenceUntrusted && n.kind === 'single' && cRow.sku_kind === 'set') holdKey('kind', norm, 'ne_dropped_rows');
    else if (n.kind !== cRow.sku_kind) {
      const r = classify({ key: subjectKey('kind', norm), type: 'kind', norm, col: 'kind', nst: { raw: 'value', validity: 'ok' }, nv: n.kind,
        tt: tValue(tToday, norm, 'kind'), tl: tLoad ? tValue(tLoad, norm, 'kind') : undefined, c: cRow.sku_kind, entity });
      addCol('kind', norm, code, n.kind, r, 'kind');
      for (const t of ['value', 'cost', 'primary_supplier', 'components']) holdKey(t, norm, 'kind_mismatch');
      out.recoverable.push(subjectKey('only_in_cdb', norm));
      continue;
    } else addCol('kind', norm, code, n.kind, { cls: 'match', detail: {} }, 'kind');
    out.recoverable.push(subjectKey('only_in_cdb', norm));
    const cols = n.kind === 'single' ? [['value', 'name'], ['value', 'handling'], ['value', 'tax_rate'], ['value', 'standard_price_jpy'], ['cost', 'cost'], ['primary_supplier', 'primary_supplier']]
      : [['value', 'name'], ['value', 'standard_price_jpy']];
    for (const [type, col] of cols) {
      if ((col === 'standard_price_jpy' || col === 'primary_supplier') && !cdb.has0027) continue;
      const nst = n.cols[col];
      const nv = col === 'primary_supplier' ? (nst.value == null ? null : [nst.value]) : nst.value;
      const wrap = (v) => (col === 'primary_supplier' && !isMarker(v) && v != null ? [v] : v);
      const r = classify({ key: subjectKey(type, norm), type, norm, col, nst, nv, tt: wrap(tValue(tToday, norm, col)), tl: tLoad ? wrap(tValue(tLoad, norm, col)) : undefined, c: cValue(cdb, norm, col), entity: 'products' });
      addCol(type, norm, code, n.kind, r, col);
    }
    if (n.kind === 'set') {
      if (componentsUntrusted) { holdKey('components', norm, 'ne_dropped_rows'); continue; }
      const nChildren = n.children;
      if ([...nChildren.values()].some((x) => comparability(x.st) !== 'comparable')) {
        // 親ごと比べない。どの子の数量がどういう状態かは残す (空・0・null は判断の一覧に「NE に値が無い (不正)」で載る。Codex #1464 R1 Medium 5)
        for (const [child, x] of nChildren) {
          if (comparability(x.st) === 'comparable') continue;
          const cr = cdb.comps.get(norm)?.get(child);   // 今の Company DB の数量も判断の一覧・承認の指紋に入れる (Codex #1464 R2)
          addCol('components', norm, code, 'set', { cls: 'incomparable', detail: { n_state: x.st.raw, n_validity: x.st.validity, n: x.st.text ?? x.st.value ?? null, c: cr ? cr.qty : show(ABSENT), note: '数量が不明・不正 (親ごと比べない)' } }, 'components', child);
        }
        carryKeys.add(subjectKey('components', norm));   // 台帳の子の単位はそのまま書き写す (期限をリセットしない。Codex #1464 R1 High 2)
        continue;
      }
      const tt = tToday.get(norm)?.children ?? null, tl = tLoad ? (tLoad.get(norm)?.children ?? null) : undefined;
      const cc = cdb.comps.get(norm) || new Map();
      const childKeys = new Set([...nChildren.keys(), ...(tt ? tt.keys() : []), ...(tl ? tl.keys() : []), ...cc.keys()]);
      for (const child of childKeys) {
        const nx = nChildren.get(child);
        const ttv = tt ? (tt.has(child) ? tt.get(child) : ABSENT) : PRESERVE;
        const tlv = tl === undefined ? undefined : tl ? (tl.has(child) ? tl.get(child) : ABSENT) : PRESERVE;
        const cr = cc.get(child);
        const r = classify({ key: subjectKey('components', norm), type: 'components', norm, col: 'components', child,
          nst: nx ? nx.st : { raw: 'value', validity: 'ok' }, nv: nx ? nx.st.value : ABSENT, tt: ttv, tl: tlv, c: cr ? cr.qty : ABSENT, entity: 'set_components' });
        addCol('components', norm, code, 'set', r, 'components', child);
      }
    }
  }
  // 台帳にあるが今回評価しなかった単位は、そのまま書き写す (期限をリセットしない)
  const evaluatedKeys = new Set([...keys.keys()].filter((k) => !carryKeys.has(k)));
  if (ledgerOk) for (const [unit, e] of ledger.entries) if (!evaluatedKeys.has(e.key) && !newPending.has(unit)) newPending.set(unit, e);

  // ── 9. 集約 (C2 v5-4・v6-4 = 4 つの集合は重ならない) ──
  const items = [];
  for (const [key, k] of keys) {
    if (Object.hasOwn(out.out_of_scope, key)) continue;
    const classes = k.columns.map((x) => x.cls);
    if (classes.some((x) => KNOWN.has(x))) { items.push({ ...k, classes: [...new Set(classes)] }); delete out.held[key]; continue; }
    if (classes.some((x) => x === 'blocked' || x === 'incomparable')) { out.held[key] = classes.includes('blocked') ? 'blocked' : 'incomparable'; continue; }
    if (k.columns.length && classes.every((x) => x === 'match')) out.recoverable.push(key);
    else out.held[key] = 'not_evaluated';
  }
  const itemKeys = new Set(items.map((i) => i.subject_key));
  out.recoverable = [...new Set(out.recoverable)].filter((k) => !itemKeys.has(k) && !Object.hasOwn(out.held, k) && !Object.hasOwn(out.out_of_scope, k)).sort();
  for (const k of Object.keys(out.held)) if (itemKeys.has(k) || Object.hasOwn(out.out_of_scope, k)) delete out.held[k];
  out.items = items;

  // ── 10. 判断の一覧 (承認の指紋)。保持 (incomparable) の案件の「NE に値が無い (不正)」も載せる = 評価した案件すべてから ──
  for (const it of keys.values()) {
    if (Object.hasOwn(out.out_of_scope, it.subject_key)) continue;
    for (const col of it.columns) {
      const incomparableNoValue = col.cls === 'incomparable' && ['empty', 'zero', 'null'].includes(col.n_state);
      if (!DECISION_CLASSES.has(col.cls) && !incomparableNoValue) continue;
      const reason = col.explained || (col.reasons && col.reasons[0]) || null;
      const reasonKind = col.cls === 'held_by_load' ? 'held_by_load' : col.cls === 'spec_undecided' ? 'spec_undecided' : reason?.reason || (col.cls === 'ne_no_value' ? 'ne_no_value' : 'none');
      const proposal = col.cls === 'ne_no_value' || incomparableNoValue ? (col.c != null && col.c !== '(無い)' ? { op: 'set_ne_value', value: col.c } : { op: 'decide' })
        : col.cls === 'held_by_load' ? { op: 'fix_load_input', reason_code: reason?.reason_code ?? null }
          : col.cls === 'spec_undecided' ? { op: 'decide_spec' }
            : reasonKind === 'manual' ? { op: 'decide_manual_priority' }
              : reasonKind === 'set_price_from_goods' || reasonKind === 'load_rule:name_blank_to_code' ? { op: 'set_ne_value', value: col.t_today } : { op: 'decide' };
      const owner = col.col === 'exists' ? 'load' : own ? own[ownerKey(col.col)] ?? null : null;   // SKU の INSERT は持ち主で止めない
      const print = decisionPrint({ norm: it.norm, kind: it.kind, col: col.col, child: col.child ?? null, problem: it.type, owner, reasonKind, reason, n_state: col.n_state, n: col.n, c: col.c, proposal });
      out.decisions.push({ subject_key: it.subject_key, code: it.code, norm: it.norm, kind: it.kind, col: col.col, child: col.child ?? null, cls: col.cls, reason_kind: reasonKind,
        n_state: col.n_state ?? null, n: col.n ?? null, c: col.c ?? null, t_today: col.t_today ?? null, reason, proposal, decision_status: 'pending', approval_fingerprint: approvalFingerprint(print) });
    }
  }
  const byClass = {};
  for (const it of items) for (const c of it.columns) byClass[c.cls] = (byClass[c.cls] || 0) + 1;
  out.counts = { ne_skus: nm.size, cdb_skus: cdb.skuByNorm.size, items: items.length, by_type: Object.fromEntries(PROBLEM_TYPES.map((t) => [t, items.filter((i) => i.type === t).length])),
    by_class: byClass, held: Object.keys(out.held).length, recoverable: out.recoverable.length, out_of_scope: Object.keys(out.out_of_scope).length, decisions: out.decisions.length,
    pending: ledgerOk ? newPending.size : null };
  out.verdict = items.length ? 'breach' : 'pass';
  return { result: out, pendingEntries: ledgerOk ? [...newPending.values()] : null };
}
