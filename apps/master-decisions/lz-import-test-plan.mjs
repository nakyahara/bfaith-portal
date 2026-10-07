/**
 * lz-import-test-plan.mjs — 少数件の実機の取込 (中原さんと) の「計画」と、押す直前の照らし直し (純粋・マスタ正本切替 ③c-1b-2b-1a)
 *
 * 設計 = AI_reference CompanyDB構想/10 §6.3「③c-1b-2b 契約 v3」K1・K2・K3・K8。
 *   buildTestPlan    試験の CSV と計画 (plan) を作る。承認の対象 = 計画の全部 (種類・例外の ID・候補・加工・期待・戻しの資料・CSV の sha256) = planSha256 (K2)
 *   checkPlanAgainstPre  実行のとき、鍵とセッションを取った後の直前の一覧で、承認のときの対象・候補・退避した値を照らし直す。違う = 止めて作り直す (K2)
 *   checkTestCsv     取り込む CSV (import.csv) が承認した試験の CSV と同じ (sha256・行) か
 * 試験の種類:
 *   normal   = その日の lz-daily の CSV の行そのまま (Company DB の値)。一覧にあり・削除でない商品だけ
 *   missing  = 一覧に無い ID。承認した既存の商品 (copy_from) の lz-daily の行を写し、ID だけ変える (K1)。文字でも小文字でも一覧に無いこと
 *   deleted  = 削除の商品。今の値 (一覧の値を決まった対応で CSV の列に写す)
 *   case     = 大文字小文字だけ違う ID。候補 (一覧で小文字にすると同じもの) の **商品ID を除く 4 列が全部同じ** ときだけ。値 = 候補の今の値 (K8)
 * 失敗の試験 (missing / deleted / case) は、一覧の値を CSV の列に写す対応 (ふりがなの列・仕入単価の書き方) が決まってから (K1)。
 * 戻しの資料 (restore) = 影響しうる既存の商品の今の値 (43 列)。対応が決まっていれば 5 列の CSV も作る (資料として保存するだけ・自動で取り込まない = K3)。
 *   大文字小文字だけ違う候補は同じ CSV に入れない (重複の禁止を例外で通さない = K8) = 候補ごとに別の CSV に分ける。
 * 退避した値は 43 列の文字とバイト (hex) の両方・計画の中は配列で持つ (商品ID をオブジェクトのキーにしない = __proto__ などで抜けない)。
 * 大文字小文字の組 (groups) は、試験の全部の行と退避した商品について持ち、押す直前に照らし直す (承認の後に a-1 が増えた = 止める)。
 * 一覧に文字の壊れ (U+FFFD) = 計画を作らない・照らし直しは evidence_broken (Codex #1519 R2)。
 */
import crypto from 'node:crypto';
import { LZ_SHOHIN } from './lz-cdb.mjs';
import { buildLosslessCsv, validateImportCsv } from './lz-import-check.mjs';

export const PLAN_VERSION = 'lzt-1';
const H = LZ_SHOHIN.header;
const ci = (name) => H.indexOf(name);
const sha256 = (b) => crypto.createHash('sha256').update(b).digest('hex');
const KINDS = Object.freeze(['normal', 'missing', 'deleted', 'case']);
/** 仕入単価の書き方 (実機で見た変換だけ): same = 一覧の文字そのまま / strip_dot00 = 「1200.00」→「1200」(.00 以外は例外) */
const COST_RULES = Object.freeze(['same', 'strip_dot00']);

/** 決まりの対応の形を確かめる (失敗の試験・戻しの CSV に要る) */
export function checkMapping(mapping) {
  if (!mapping) return null;
  if (typeof mapping.version !== 'string' || !['検索名称', '検索名称2'].includes(mapping.furiganaCol) || !COST_RULES.includes(mapping.costRule)) {
    throw new Error('mapping = { version, furiganaCol: 検索名称 | 検索名称2, costRule: same | strip_dot00 }');
  }
  return { version: mapping.version, furiganaCol: mapping.furiganaCol, costRule: mapping.costRule };
}

/** 一覧の今の値 → CSV の 5 列 (決まった対応だけ・K1) */
export function lzToCsvCells(cells, mapping) {
  const m = checkMapping(mapping);
  if (!m) throw new Error('一覧の値を CSV の列に写す対応 (mapping) が決まっていない (K1)');
  let cost = cells[ci('仕入単価')];
  if (m.costRule === 'strip_dot00') {
    if (!/^(0|[1-9]\d*)\.00$/.test(cost)) throw new Error(`仕入単価の書き方が決まりと違う: ${cost}`);
    cost = cost.slice(0, -3);
  }
  return [cells[ci('商品ID')], cells[ci('商品名')], cells[ci(m.furiganaCol)], cost, cells[ci('商品予備項目００３')]];
}

/** 計画の sha256 (キーを並べた JSON) = 承認に結ぶ値 */
export function planSha256(plan) {
  const canon = (v) => (Array.isArray(v) ? v.map(canon) : v && typeof v === 'object' ? Object.fromEntries(Object.keys(v).sort().map((k) => [k, canon(v[k])])) : v);
  return sha256(Buffer.from(JSON.stringify(canon(plan)), 'utf8'));
}

/**
 * 試験の計画を作る
 * @param {object} p
 * @param {{ run_id: string, as_of: string, csv_sha256: string, table: string[][] }} p.source  その日の lz-daily の正式な証跡と CSV の行 (validateImportCsv の table)
 * @param {object} p.pre   直前の一覧 (readLzShohinMaster・cells つき)
 * @param {{ normal?: string[], missing?: Array<{ id, copy_from }>, deleted?: string[], case?: Array<{ id, from }> }} p.tests
 * @param {object|null} [p.mapping]  一覧の値 → CSV の列の対応 (決まってから。失敗の試験に要る)
 * @returns {{ plan: object, planSha256: string, testCsv: Buffer, restoreCsvs: Buffer[] }}
 */
export function buildTestPlan({ source, pre, tests, mapping = null }) {
  if (!source || !/^[0-9a-f]{64}$/.test(String(source.csv_sha256 || '')) || !Array.isArray(source.table)) throw new Error('source (lz-daily の証跡と CSV の行) が要る');
  if (!pre || !pre.ok || !pre.byId) throw new Error('直前の一覧 (ok) が要る');
  if (!pre.encoding || pre.encoding.fffd > 0) throw new Error('直前の一覧に文字の壊れがある = 計画を作らない (証跡の破損)');
  const m = checkMapping(mapping);
  const t = { normal: [], missing: [], deleted: [], case: [], ...(tests || {}) };
  for (const k of Object.keys(t)) if (!KINDS.includes(k)) throw new Error(`知らない試験の種類: ${k}`);
  const failureKinds = ['missing', 'deleted', 'case'].filter((k) => t[k].length);
  if (failureKinds.length && !m) throw new Error(`失敗の試験 (${failureKinds.join('・')}) は、ふりがなの列と仕入単価の書き方が決まってから (K1)`);
  const srcRow = new Map(source.table.map((r) => [r[0], r]));
  const deletedOf = (cells) => cells[ci('削除フラグ')];
  const rows = [];
  const retainedMap = new Map();   // 商品ID → { cells, raw } (Map = __proto__ などもそのまま)
  const keep = (id) => { if (!retainedMap.has(id)) { const x = pre.byId.get(id); retainedMap.set(id, { cells: [...x.cells], raw: x.raw.map((b) => Buffer.from(b).toString('hex')) }); } };
  const groupOf = (id) => [...(pre.lowerGroups.get(String(id).toLowerCase()) || [])].sort();
  for (const id of t.normal) {
    const r = srcRow.get(id), lz = pre.byId.get(id);
    if (!r) throw new Error(`normal: lz-daily の CSV に無い: ${id}`);
    if (!lz) throw new Error(`normal: 一覧に無い: ${id}`);
    if (deletedOf(lz.cells) !== '0') throw new Error(`normal: 削除の商品: ${id}`);
    if (groupOf(id).length !== 1) throw new Error(`normal: 大文字小文字だけ違う商品が一覧にある: ${groupOf(id).join(', ')}`);
    rows.push({ kind: 'normal', id, cells: [...r], provenance: { from: 'lz_daily', run_id: source.run_id }, expected: 'imported' });
    keep(id);
  }
  for (const { id, copy_from: from } of t.missing) {
    if (pre.byId.has(id) || pre.lowerGroups.has(String(id).toLowerCase())) throw new Error(`missing: 一覧に (小文字にしても) ある: ${id}`);
    const r = srcRow.get(from);
    if (!r || !pre.byId.has(from)) throw new Error(`missing: 写す元が lz-daily の CSV / 一覧に無い: ${from}`);
    rows.push({ kind: 'missing', id, cells: [id, ...r.slice(1)], provenance: { from: 'lz_daily', run_id: source.run_id, copy_from: from, changed: ['形式/型番'] }, expected: 'error_row',
      if_registered: 'ロジザードの画面でこの ID の商品を削除する (思わず新しく登録されたとき・K1)' });
    keep(from);
  }
  for (const id of t.deleted) {
    const lz = pre.byId.get(id);
    if (!lz) throw new Error(`deleted: 一覧に無い: ${id}`);
    if (deletedOf(lz.cells) === '0') throw new Error(`deleted: 削除の商品ではない: ${id}`);
    if (groupOf(id).length !== 1) throw new Error(`deleted: 大文字小文字だけ違う商品が一覧にある: ${groupOf(id).join(', ')}`);
    rows.push({ kind: 'deleted', id, cells: lzToCsvCells(lz.cells, m), provenance: { from: 'lz_current', mapping: m.version }, expected: 'observe' });
    keep(id);
  }
  for (const { id, from } of t.case) {
    if (pre.byId.has(id)) throw new Error(`case: 一覧に同じ文字の ID がある: ${id}`);
    const group = pre.lowerGroups.get(String(id).toLowerCase()) || [];
    if (!group.length || !group.includes(from)) throw new Error(`case: 候補に写す元が無い: ${id} ← ${from}`);
    const four = group.map((g) => JSON.stringify(lzToCsvCells(pre.byId.get(g).cells, m).slice(1)));
    if (new Set(four).size !== 1) throw new Error(`case: 候補の 商品ID を除く 4 列が違う = この試験はしない (K8): ${group.join(', ')}`);
    rows.push({ kind: 'case', id, cells: [id, ...lzToCsvCells(pre.byId.get(from).cells, m).slice(1)], provenance: { from: 'lz_current', mapping: m.version, case_of: from, candidates: [...group].sort() }, expected: 'error_row_or_no_change' });
    for (const g of group) keep(g);
  }
  if (!rows.length) throw new Error('試験の行が無い');
  const test = buildLosslessCsv(rows.map((r) => r.cells));   // 5 列を独立に・読み直して一致・重複 (文字 / 小文字) は例外
  const retained = [...retainedMap].map(([id, v]) => ({ id, cells: v.cells, raw: v.raw }));
  // 大文字小文字の組 = 試験の全部の行と退避した商品 (小文字の鍵ごと・並べ替え済み)
  const keys = [...new Set([...rows.map((r) => r.id), ...retained.map((r) => r.id)].map((id) => String(id).toLowerCase()))].sort();
  const groups = keys.map((key) => ({ key, ids: [...(pre.lowerGroups.get(key) || [])].sort() }));
  const restoreIds = retained.map((r) => r.id).sort();
  // 小文字にすると同じ ID は別の CSV へ (1 つの CSV に 1 つの組から 1 つだけ)
  const batches = [];
  for (const id of restoreIds) {
    const l = id.toLowerCase();
    let b = batches.find((x) => !x.lower.has(l));
    if (!b) { b = { ids: [], lower: new Set() }; batches.push(b); }
    b.ids.push(id); b.lower.add(l);
  }
  const restores = m ? batches.map((b) => ({ ids: b.ids, csv: buildLosslessCsv(b.ids.map((id) => lzToCsvCells(retainedMap.get(id).cells, m))) })) : [];
  const plan = {
    version: PLAN_VERSION,
    source: { run_id: source.run_id, as_of: source.as_of, csv_sha256: source.csv_sha256 },
    mapping: m,
    rows, groups, retained,
    test_csv: { sha256: sha256(test.bytes), rows: test.rows },
    restore_csvs: restores.map((r) => ({ sha256: sha256(r.csv.bytes), rows: r.csv.rows, ids: r.ids })),
    restore_note: '戻しの資料 (自動で取り込まない・人がロジザードで直す材料・K3)',
  };
  return { plan, planSha256: planSha256(plan), testCsv: test.bytes, restoreCsvs: restores.map((r) => r.csv.bytes) };
}

/**
 * 実行のとき、直前の一覧で承認のときの前提を照らし直す (K2)。違う = 止めて計画を作り直す (CSV を自動で直さない)
 * @returns {{ ok: boolean, diffs: Array<{ id, kind, col? }> }}
 */
export function checkPlanAgainstPre(plan, pre) {
  if (!plan || plan.version !== PLAN_VERSION) throw new Error('計画の版が違う');
  if (!pre || !pre.ok) throw new Error('直前の一覧 (ok) が要る');
  const diffs = [];
  if (!pre.encoding || pre.encoding.fffd > 0) return { ok: false, diffs: [{ id: null, kind: 'evidence_broken' }] };
  for (const { id, cells, raw } of plan.retained) {
    const now = pre.byId.get(id);
    if (!now) { diffs.push({ id, kind: 'retained_vanished' }); continue; }
    // 文字とバイトの両方 (違うバイトが同じ文字に読めても見落とさない)
    cells.forEach((c, j) => { if (now.cells[j] !== c || Buffer.from(now.raw[j]).toString('hex') !== raw[j]) diffs.push({ id, kind: 'retained_changed', col: H[j] }); });
  }
  for (const { key, ids } of plan.groups) {
    const now = [...(pre.lowerGroups.get(key) || [])].sort();
    if (JSON.stringify(now) !== JSON.stringify(ids)) diffs.push({ id: key, kind: 'case_group_changed' });   // 承認の後に大文字小文字だけ違う商品が増えた / 消えた
  }
  for (const r of plan.rows) {
    if (r.kind === 'missing' && (pre.byId.has(r.id) || pre.lowerGroups.has(r.id.toLowerCase()))) diffs.push({ id: r.id, kind: 'missing_now_exists' });
    if (r.kind === 'case' && pre.byId.has(r.id)) diffs.push({ id: r.id, kind: 'case_id_now_exists' });
  }
  return { ok: diffs.length === 0, diffs };
}

/** 取り込む CSV (import.csv) = 承認した試験の CSV (sha256・行の数・行の値) */
export function checkTestCsv(plan, bytes) {
  const b = Buffer.from(bytes || []);
  if (sha256(b) !== plan.test_csv.sha256) return { ok: false, reason: 'test_csv_sha256_mismatch' };
  const v = validateImportCsv(b);
  if (!v.ok) return { ok: false, reason: v.reason };
  if (v.rows !== plan.rows.length || v.table.some((row, i) => JSON.stringify(row) !== JSON.stringify(plan.rows[i].cells))) return { ok: false, reason: 'test_csv_rows_mismatch' };
  return { ok: true, reason: null, rows: v.rows };
}
