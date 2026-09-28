/**
 * lz-cdb.mjs — ロジザードの毎日の商品マスタを Company DB の値で作る (マスタ正本切替 ③c-1a = 影運転の続き。まだ取り込まない)
 *   設計 = AI_reference CompanyDB構想/10 §6.3「③c 契約 v1〜v3」と中原さんの答え L-4〜L-8 (2026-09-28)
 *
 * 対象 = NE の取得の商品 (③b-2 と同じ集合) を 3 つに分ける (v3 M5):
 *   compare  = ロジザードにある (商品ID が文字の完全一致) かつ Company DB の値が全部そろう → 両方の道 (Company DB / NE の取得) で行を作って比べる
 *   awaiting = ロジザードに無い = 新商品の登録 (①) 待ち。ロジザードに無い ID の行は取込でエラーになる (中原さん L-7) = 出さない
 *   invalid  = 出さない (理由つき)。0 や空で埋めない (v2 H5)。1 件でも残れば、その商品を中原さんが認めない限り切替の合格にしない
 * 値の出どころ (v2 H5・L-4):
 *   形式/型番 = NE の元の書き方 (ops.master_ne_codes)・商品名 = core.skus.name (前後の空白を削った形 = 許す差 L-4)・
 *   仕入単価 = core.sku_costs の今の原価 (円の整数)・取引先 = 代表の仕入先 (1 つ・4 桁)
 *   🚨 NE の取得の元の商品名が空・空白だけ = 夜間ロードがコードで補った名前の疑い = 出さない (v3 M4)
 * 差の説明 (v2 H6): Company DB の道と NE の取得の道 (GAS と同じ変換と確かめ済み) の差のうち、許すのは次だけ
 *   name_trim   = NE の名前の前後の空白を削ると Company DB の名前 (L-4)
 *   compare_ne  = その朝の照合 ② の差の一覧に、同じ商品・同じ列・同じ両側の値で載っている差
 */
import iconv from 'iconv-lite';
import { parseCsvBytes } from './lz-compare.mjs';
import { cellBytes } from './lz-csv.mjs';

/** ロジザードの商品マスタの書き出し (auto-shohin-csv.js・FM08_01 商品 / デフォルト・全期間・有効 + 無効)。見出しは実ファイルの 1 行目 (2026-09-28) */
export const LZ_SHOHIN = Object.freeze({
  header: Object.freeze(['契約者ID', '契約者名', '荷主ID', '荷主名', '商品ID', '商品名', '検索名称', '検索名称2', '仕入単価', '引当不可日数', '大分類', '中分類', '小分類', 'ロット管理フラグ',
    '有効期限区分', '温度帯区分', '小売価格', 'セット構成区分', '削除フラグ', '登録日時', '変更日時', 'インポート日時', '在庫区分', '入荷期限日数', '入荷日管理フラグ',
    '商品予備項目００１', '商品予備項目００２', '商品予備項目００３', '商品予備項目００４', '商品予備項目００５', '商品予備項目００６', '商品予備項目００７', '商品予備項目００８',
    '商品予備項目００９', '商品予備項目０１０', '部門ID', '小売価格2', '小売価格3', '小売価格4', '小売価格5', '英語名', '重量', 'シリアル登録フラグ']),
  cols: Object.freeze({ id: 4, name: 5, cost: 8, deleted: 18, supplier: 27 }),   // 取引先 = 商品予備項目００３ (2026-09-28 に値で確かめた)
  minRows: 4000,
});

/**
 * ロジザードの商品の全件の一覧を読む。見出しが違う・壊れた CSV・行が少なすぎる = 使わない (ok: false)
 * @returns {{ ok: boolean, reason: string|null, rows: number, byId: Map<string, { name, cost, supplier, deleted }>, lowerGroups: Map<string, string[]> }}
 */
export function readLzShohinMaster(buf, { minRows = LZ_SHOHIN.minRows } = {}) {
  const P = parseCsvBytes(buf);
  const bad = (reason, rows = 0) => ({ ok: false, reason, rows, byId: new Map(), lowerGroups: new Map() });
  if (P.shape.unterminated || P.shape.bare_quote || P.shape.after_quote) return bad('lz_master_broken');
  const dec = (b) => iconv.decode(Buffer.from(b), 'cp932');
  const head = (P.records[0] || { cells: [] }).cells.map(dec);
  if (head.length !== LZ_SHOHIN.header.length || head.some((h, i) => h !== LZ_SHOHIN.header[i])) return bad('lz_master_header');
  const body = P.records.slice(1);
  if (body.some((r) => r.cells.length !== LZ_SHOHIN.header.length)) return bad('lz_master_row_width', body.length);
  if (body.length < minRows) return bad('lz_master_too_few', body.length);
  const C = LZ_SHOHIN.cols, byId = new Map(), lowerGroups = new Map();
  for (const r of body) {
    const id = dec(r.cells[C.id]);
    if (!id) continue;
    if (byId.has(id)) return bad('lz_master_duplicate_id', body.length);
    byId.set(id, { name: dec(r.cells[C.name]), cost: dec(r.cells[C.cost]), supplier: dec(r.cells[C.supplier]), deleted: dec(r.cells[C.deleted]) });
    const l = id.toLowerCase();
    if (!lowerGroups.has(l)) lowerGroups.set(l, []);
    lowerGroups.get(l).push(id);
  }
  return { ok: true, reason: null, rows: body.length, byId, lowerGroups };
}

/**
 * NE の取得の商品を 3 つに分け、比べる商品は両方の道の項目を作る (純粋)
 * @param {object} p
 * @param {Array<{ code_norm, ne_code, code_reason, name, cost_src, supplier }>} p.neItems  lz-snapshot の材料の items (NE の取得の値・元のコード)
 * @param {object} p.cdb  compare-load.mjs readCdbMaster の結果 (skuByNorm・costs・primary)
 * @param {object} p.lz   readLzShohinMaster の結果
 * @returns {{ compare: Array<{ key, cdb: object, ne: object }>, awaiting: object[], invalid: object[], counts: object }}
 */
export function classifyForLz({ neItems, cdb, lz }) {
  const compare = [], awaiting = [], invalid = [];
  const no = (it, reason, extra = {}) => invalid.push({ code_norm: it.code_norm, ne_code: it.ne_code || null, reason, ...extra });
  for (const it of neItems) {
    if (!it.ne_code) { no(it, it.code_reason || 'no_ne_code'); continue; }
    const variants = lz.lowerGroups.get(it.ne_code.toLowerCase()) || [];
    if (variants.some((v) => v !== it.ne_code)) { no(it, 'lz_case_collision', { lz_ids: variants }); continue; }
    const lzRow = lz.byId.get(it.ne_code);
    if (!lzRow) { awaiting.push({ code_norm: it.code_norm, ne_code: it.ne_code }); continue; }
    if (lzRow.deleted !== '0') { no(it, 'lz_deleted'); continue; }
    const sku = cdb.skuByNorm.get(it.code_norm);
    if (!sku) { no(it, 'cdb_no_sku'); continue; }
    if (it.name == null || String(it.name).trim() === '') { no(it, 'ne_name_blank'); continue; }   // コードで補った名前の疑い (v3 M4)
    const name = String(sku.name ?? '').trim();
    if (!name) { no(it, 'cdb_name_blank'); continue; }
    const c = cdb.costs.get(it.code_norm);
    if (!c) { no(it, 'cdb_cost_missing'); continue; }
    const cost = Number(c.cost_jpy);
    if (!Number.isSafeInteger(cost) || cost < 0) { no(it, 'cdb_cost_shape', { cost: c.cost_jpy }); continue; }
    const sups = [...(cdb.primary.get(it.code_norm) || [])];
    if (sups.length !== 1) { no(it, 'cdb_supplier_count', { suppliers: sups }); continue; }
    if (!/^\d{4}$/.test(String(sups[0]))) { no(it, 'cdb_supplier_shape', { supplier: sups[0] }); continue; }
    compare.push({ key: it.ne_code, code_norm: it.code_norm,
      cdb: { code_norm: it.code_norm, ne_code: it.ne_code, code_reason: null, name, cost_text: String(cost), supplier: String(sups[0]) },
      ne: it });
  }
  const reasons = invalid.reduce((m, x) => ((m[x.reason] = (m[x.reason] || 0) + 1), m), {});
  return { compare, awaiting, invalid, counts: { targets: neItems.length, compare: compare.length, awaiting: awaiting.length, invalid: invalid.length, invalid_reasons: reasons } };
}

/** セルのバイト (引用符の外側) = ロジザードの CSV と同じ変換で比べる */
const cellHex = (v) => { const c = cellBytes(v == null ? '' : String(v)); return (c.quoted ? c.bytes.subarray(1, -1) : c.bytes).toString('hex'); };
const LZ_COL_TO_COMPARE = Object.freeze({ 1: 'name', 2: 'name', 3: 'cost', 4: 'primary_supplier' });

/**
 * 照合 ② の全件 JSON (mc-v2 の ne.items) → (code_norm|列) → { n, c } の一覧
 */
export function compareNeIndex(compareJson) {
  const m = new Map();
  for (const x of (compareJson && compareJson.ne && compareJson.ne.items) || []) {
    for (const c of x.columns || []) m.set(`${x.norm}|${c.col}`, { n: c.n, c: c.c, cls: c.cls });
  }
  return m;
}

/**
 * compareLz (gas = NE の取得の道・ours = Company DB の道) の「説明できない値の差」を、許す差に分け直す (純粋)。
 * 許す差にできたものは allowed に移し、残りは unexplained のまま。合否も付け直す
 */
export function explainCdbDiffs(result, { compareIndex, byKey }) {
  const unexplained = [], allowed = [...result.allowed];
  for (const u of result.unexplained) {
    if (u.what !== 'value') { unexplained.push(u); continue; }
    const row = byKey.get(u.code);
    const col = LZ_COL_TO_COMPARE[u.col];
    if (!row || !col) { unexplained.push(u); continue; }
    // L-4: NE の名前の前後の空白を削ると Company DB の名前
    if (col === 'name' && String(row.ne.name).trim() === row.cdb.name && cellHex(row.ne.name) === u.gas.hex && cellHex(row.cdb.name) === u.ours.hex) {
      allowed.push({ ...u, why: 'name_trim' });
      continue;
    }
    // 照合 ② の差の一覧に、同じ商品・同じ列・同じ両側の値で載っている
    const e = compareIndex.get(`${row.code_norm}|${col}`);
    if (e && e.cls !== 'match') {
      const cdbWant = col === 'primary_supplier' ? [row.cdb.supplier] : col === 'cost' ? Number(row.cdb.cost_text) : row.cdb.name;
      const cSame = JSON.stringify(col === 'primary_supplier' ? [...(e.c || [])].sort() : e.c) === JSON.stringify(cdbWant);
      const nWant = col === 'cost' ? neCostNumber(row.ne.cost_src) : col === 'primary_supplier' ? row.ne.supplier : row.ne.name;
      const nSame = JSON.stringify(col === 'primary_supplier' && Array.isArray(e.n) ? e.n[0] : e.n) === JSON.stringify(nWant);
      if (cSame && nSame) { allowed.push({ ...u, why: 'compare_ne', cls: e.cls }); continue; }
    }
    unexplained.push(u);
  }
  const out = { ...result, unexplained, allowed };
  out.verdict = !out.shape.length && !out.undeterminable.length && !unexplained.length ? 'pass' : 'fail';
  out.summary = { ...result.summary, allowed: allowed.length, unexplained: unexplained.length };
  return out;
}
function neCostNumber(src) {
  try { const v = JSON.parse(src); const n = Number(v); return v === '' || v == null || !Number.isFinite(n) ? null : n; } catch { return null; }
}
