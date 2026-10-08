/**
 * master-publish.js — Company DB の写し (マスタ正本切替 ④a。設計 = AI_reference CompanyDB構想/15 §0〜§4・10 §7・Codex ④ 設計 R0 / R1 = 契約)
 *
 * なぜ: 切替の後は、持ち主が C (Company DB) の列は Company DB の値が正。既存のアプリは読み先を変えず m_products (と上書き表) を読み続ける。
 *   m_products は毎朝 NE から作り直される → 作り直しの**同じ取引の中で・作り直しの記録を書く前に** C の値を重ねる
 *   (後から UPDATE すると由来が changed_after_build になり照合 ② が由来を失う。Render で後から重ねると照合 ① が「判定できない」になる。15 §0)。
 * なにを:
 *   - 持ち主の設定の決まり (checkPublishOwnership): 一緒に切り替える組 (products.name + skus.name・products.status + skus.handling・skus.tax_rate + skus.tax_class)・
 *     ④a が写さない列を company にしない (写さなくてよいと明示した列だけ許す)。④a の入口で確かめる (fetch.mjs の cli = 投げる・作り直し = 止める)。
 *     🚨 このファイルを読み込んだ時には投げない (warehouse の router など、TAX_RATES のために作り直しのファイルを読むサーバーごと落とさない)
 *   - 世代の表 (db.js の cdb_publish_generations / cdb_publish_values・sync_meta の cdb_publish_current) の読み方と、ハッシュ・値の範囲の決まり。
 *     書くのは apps/company-db/publish/fetch.mjs (daily-sync の「Company DB の写し」)。読むのは rebuild-m-products.js
 *   - makePublishResolver: 列ごとに「C の値か、今までの値か」。🚨 持ち主が全部 load なら何もしない (今までと同じ値・同じ理由・何も書かない)
 *     持ち主が C の列があるのに、同じ持ち主 (epoch) の世代が無い・C にある SKU の要る列の値が 1 つでも欠ける・
 *     (C に無い = NE にしか無い SKU は止めない = NE の値のまま作り not_in_cdb で知らせる。1 件で全部を止めない)・
 *     m_products の 2 つのコードが同じ C の SKU に当たる = 作り直しを止める (前の m_products のまま)
 *   - 逆向きの決まり (Company DB の形 → m_products の形。15 §2): 原価ソースは新しい語を作らない (COST_SOURCE_TO_M)・C の原価が空 = MISSING・
 *     取扱区分は NE の語が同じ区分に当たるなら NE の語を残す・仕入先コードは同じ仕入先なら NE の書き方を残す
 *   - applySideTables: 上書き表を C の値にそろえる (この作り直しの m_products のコードだけ。Company DB にしか無い SKU の行は作らない)。
 *     exception_genka = 原価が空なら行を消す・値なら直す (行は作らない) / product_shipping = C の送料が全部空なら行を消す・値なら直す (行は作らない) /
 *     m_reorder_setting = 値なら入れる・直す・空なら消す
 *   - verifyApplied: 入れた後に読み直して、世代の値と全部のキー × 持ち主が C の列で比べる (値・空・行の有無・持ち主が load の列は今までの値のまま)
 * 🚨 product_tax_rate / product_sales_class (作り直しの入力) は、持ち主が C の列では m_products を上書きできない (Codex R1 H3 の 2 つめの道):
 *   税率・税区分 = C にある SKU は全部 C の値 (完全さの確かめで m_products に入るコードは全部 C にある) / 売上分類 = 単品は C・セットは構成品の C の値から導く
 *   (セットの product_sales_class の行は使わない。15 §5 の 2 の推奨)。🚨 例外の商品 (NE に無い) は Company DB に売上分類の置き場所が無い = 今までどおり product_sales_class
 */
import crypto from 'node:crypto';
import { normSku } from '../../lib/sku-norm.js';
import { companyOwned } from '../../config/master-ownership.mjs';
import { ownershipHash as canonicalOwnershipHash } from '../../lib/master-cutover.mjs';
import { canonicalSupplierCode, mapHandling } from '../company-db/load/sources.mjs';
import { deriveSetValues } from '../../lib/master-set-rules.js';

/** 写す列 → 持ち主のキー (config/master-ownership.mjs)。④a で写すのはこれだけ (仕入先そのもの = ④b・構成・代表・Amazon SKU = 写さない。15 §2) */
export const PUBLISH_COLUMNS = Object.freeze({
  name: 'skus.name',
  cost: 'sku_costs',
  standard_price: 'skus.standard_price',
  tax_rate: 'skus.tax_rate',
  tax_class: 'skus.tax_class',
  sales_class: 'products.sales_class',
  shipping: 'skus.shipping',
  reorder_months: 'skus.reorder_months',
  handling: 'skus.handling',
  primary_supplier: 'supplier_skus.is_primary',
  // 区分 (単品 / セット / 例外) → m_products.商品区分 (正本 = 10 の表の「商品区分」の行。KIND_TO_M で NE の語に)。
  //   区分が NE と違う SKU (設計 = 広げる道 v3 §4.1 b・v4 の表。C の区分 3 × NE の区分 3 の 9 升):
  //     区分が同じ 3 升 = 今までどおり (C の値を写す)
  //     C = 単品・NE = セット (kind_c_single_ne_set) = 単品の行にして C の値を写す・構成 (m_set_components) は写さない (行を作らない・構成品数は空)
  //     ほかの 5 升 (C = セット・NE = 単品 / 例外を含む 4 升) (kind_c_set_ne_single_frozen) = その SKU は前の m_products・m_set_components の行のまま
  //       (fail-closed。構成 sku_components は写さない列 = セットの行を作れない・例外の所属は設計 v4 で決める)。前の行が無い = 載せない (非掲載)。
  //       どれも毎朝 ⚠️ で知らせる・NE を C の区分に直すと翌朝から写る
  //   持ち主が load の今は写さない (列に入らない = 何も変わらない)
  //   🚨 区分の値の行は世代に作らない = 世代の行の sku_kind (どの行にもある・_sku の行も) が C の区分 (entriesOfRows の e.kind)。
  //   warehouse.db の世代の表 (cdb_publish_values の col の CHECK) も世代の形も変えない
  kind: 'skus.sku_kind',
});
/** 写さなくてよい列 (理由つき)。ここに無い列を company にしたら ④a は止める (写し忘れで「C が正なのに古い表は NE」を作らない。Codex R1 H1) */
export const NO_OLD_TABLE_COPY = Object.freeze({
  'products.name': 'skus.name と一緒に切り替える (m_products の商品名は skus.name から)',
  'products.status': 'skus.handling と一緒に切り替える (m_products の取扱区分は skus.handling から)',
  // ⑤-2b (0053): JAN は古い表 (m_products・横の表) に置き場所が無い = 写すものが無い。JAN の正は Company DB の external_ids と
  //   ⑤-2b の JAN の記録・NE 登録の CSV の道 (#1564 Codex R7 Medium 2)
  'external_ids.jan': '古い表に JAN の置き場所が無い (正 = Company DB の external_ids・⑤-2b の JAN の記録と NE 登録の CSV)',
  // ⑦-2 PR-A: Amazon SKU の対応は m_products でなく m_sku_master・m_sku_components に写す = ④a の世代には入れない (別の工程が写す)
  'listing_components.amazon': '⑦-2 が写す (apps/company-db/publish/amazon-map.mjs = daily-sync の「CompanyDB写し(Amazon SKU)」が m_sku_master・m_sku_components へ。④a の m_products には置き場所が無い)',
});
/** 一緒に切り替える組 (持ち主が違うと、同じものの 2 つの値の片方だけが C になる) */
export const CO_SWITCH_GROUPS = Object.freeze([['products.name', 'skus.name'], ['products.status', 'skus.handling'], ['skus.tax_rate', 'skus.tax_class']]);
/** 0027 の列 (Company DB に 0027 が無ければ C の値を出せない) */
export const PUBLISH_0027_COLUMNS = Object.freeze(['standard_price', 'shipping', 'reorder_months', 'primary_supplier']);
export const PUBLISH_CURRENT_KEY = 'cdb_publish_current';
/** 世代の中の「その SKU が Company DB にある」ことの行 (値の列ではない) */
export const PRESENCE_COL = '_sku';
/** 証跡・ログに出す NE にしか無い SKU のコードの数 */
export const NOT_IN_CDB_SAMPLES = 20;
export const PUBLISH_KEEP_GENERATIONS = 14;
export const GENERATION_ID_RE = /^cpg_\d{8}T\d{9}Z_[0-9a-f]{6}$/;
export const SKU_KINDS = Object.freeze(['single', 'set', 'exception']);
/**
 * 原価の出どころ (Company DB の cost_source) → m_products の原価ソース。新しい語は作らない (expected-profit はセットを 'セット計算' で見分ける)。
 * mapCost (sources.mjs) の逆は 1 つに決まらないので明示する (Codex R0 #8・R1): 人が決めた (manual)・0 円に決めた (override_zero)・外から入れた (imported) 原価は、
 * 今の「例外」と同じ扱い。原価状態は C の値のまま (COMPLETE / OVERRIDDEN …)。C の原価が空 (行が無い) = 原価 NULL・不明・MISSING (0 円とは別)。
 * 元の出どころと世代は作り直しの理由 (company_owned の cdb_cost_source・generation_no) に残す
 */
export const COST_SOURCE_TO_M = Object.freeze({ ne: 'NE', set_calc: 'セット計算', manual: '例外', override_zero: '例外', imported: '例外' });
const COST_STATUSES = ['COMPLETE', 'OVERRIDDEN', 'PARTIAL', 'MISSING'];
const TAX_CLASSES = ['STANDARD_10', 'REDUCED_8', 'MIXED', 'UNKNOWN'];
/**
 * m_products の商品区分ごとに、C の値が必ず要る列 (Codex R1 H1)。
 *   単品 = 全部 / セット = 税率・税区分・売上分類・取扱区分は構成品から導く (要らない) / 例外 = 売上分類は Company DB に置く場所が無い (要らない)
 */
const NOT_REQUIRED = { 単品: [], セット: ['tax_rate', 'tax_class', 'sales_class', 'handling'], 例外: ['sales_class'] };
export const requiredCols = (kind, cols) => cols.filter((c) => c !== 'kind' && !(NOT_REQUIRED[kind] || []).includes(c));   // 区分は値の欄ではなく行の sku_kind
/** 列の組 (m_products の値の取り出し・今までの理由の列・持ち主の列) */
const GROUPS = [
  ['name', (v) => v.name, ['name'], ['name']],
  ['price', (v) => v.price, ['price'], ['standard_price']],
  ['cost', (v) => ({ 原価: v.genka, 原価ソース: v.genkaSource, 原価状態: v.genkaStatus }), ['cost'], ['cost']],
  ['tax_rate', (v) => ({ 消費税率: v.taxRate, 税区分: v.taxCategory }), ['tax_rate'], ['tax_rate', 'tax_class']],
  ['sales_class', (v) => v.salesClass, [], ['sales_class']],
  ['shipping', (v) => ({ 送料: v.shipCost, 送料コード: v.shipCode, 配送方法: v.shipMethod }), [], ['shipping']],
  ['handling', (v) => v.handling, [], ['handling']],
  ['primary_supplier', (v) => v.supplier, [], ['primary_supplier']],
];

export const makeGenerationId = (now = new Date()) => `cpg_${now.toISOString().replace(/[-:.]/g, '')}_${crypto.randomBytes(3).toString('hex')}`;

/** 持ち主が C の列のうち、④a で写すもの (写す列の順) */
export function publishCols(ownership) {
  const owned = new Set(companyOwned(ownership));
  return Object.keys(PUBLISH_COLUMNS).filter((col) => owned.has(PUBLISH_COLUMNS[col]));
}
/**
 * 持ち主の設定が ④a で扱えるか (Codex R1 H1)。戻り値 = 問題の配列 (空 = よい)
 *   co_switch:<a>+<b> = 一緒に切り替える組の持ち主が違う / not_copied:<key> = ④a が写さない列が company (写さなくてよいと明示していない)
 */
export function checkPublishOwnership(ownership) {
  const out = [];
  const o = ownership || {};
  for (const [a, b] of CO_SWITCH_GROUPS) if (o[a] !== o[b]) out.push(`co_switch:${a}+${b}`);
  const copied = new Set(Object.values(PUBLISH_COLUMNS));
  for (const k of companyOwned(o)) if (!copied.has(k) && !Object.hasOwn(NO_OLD_TABLE_COPY, k)) out.push(`not_copied:${k}`);
  return out;
}
export function assertPublishOwnership(ownership) {
  const p = checkPublishOwnership(ownership);
  if (p.length) throw Object.assign(new Error(`master-publish: 持ち主の設定を ④a で扱えない (${p.join(' / ')})`), { code: 'OWNERSHIP_NOT_SUPPORTED', problems: p });
  return ownership;
}
/** 持ち主の設定をキーの順に並べたもの・そのハッシュ (夜間ロードが ops.load_materials に残すのと同じ式。engine.mjs) = 持ち主の epoch。
 *   ハッシュは lib/master-cutover.mjs の ownershipHash の 1 つの式 (load の列は数えない = 記録に無い列は load と同じ。#1564 Codex R3 Medium) */
export const ownershipSorted = (o) => Object.fromEntries(Object.keys(o || {}).sort().map((k) => [k, o[k]]));
export const ownershipHash = canonicalOwnershipHash;

const sha256Lines = (lines) => crypto.createHash('sha256').update([...lines].sort().join('\n'), 'utf8').digest('hex');
/** 世代の中身のハッシュ (行の順に依らない)。rows = [{ code_norm, col, code, sku_kind, value }] (value = JSON の文字列) */
export function publishContentHash(rows) {
  return sha256Lines((rows || []).map((r) => JSON.stringify([r.code_norm, r.col, r.code, r.sku_kind, r.value])));
}

const isYen = (v) => Number.isSafeInteger(v) && v >= 0;
const strOrNull = (v) => v == null || (typeof v === 'string' && v.trim() !== '');
/** 値の範囲 (Company DB の CHECK と同じ + 形)。null = わざと空 (name・handling・shipping の形は空を許さない) */
export function validPublishValue(col, v) {
  switch (col) {
    case PRESENCE_COL: return v === true;
    case 'name': return typeof v === 'string' && v.trim() !== '';
    case 'cost': return v === null || (!!v && typeof v === 'object' && isYen(v.jpy) && Object.hasOwn(COST_SOURCE_TO_M, v.source) && COST_STATUSES.includes(v.status));
    case 'standard_price': return v === null || isYen(v);
    case 'tax_rate': return v === null || v === 0.08 || v === 0.1;
    case 'tax_class': return v === null || TAX_CLASSES.includes(v);
    case 'sales_class': return v === null || [1, 2, 3, 4].includes(v);
    case 'shipping': return !!v && typeof v === 'object' && strOrNull(v.code) && strOrNull(v.method) && (v.cost_jpy === null || isYen(v.cost_jpy));
    case 'reorder_months': return v === null || (typeof v === 'number' && Number.isFinite(v) && v >= 0 && v <= 60);
    case 'handling': return ['active', 'discontinued', 'unknown'].includes(v);
    case 'primary_supplier': return v === null || (typeof v === 'string' && v.trim() !== '');
    default: return false;
  }
}

/**
 * 完全さの確かめ (Codex R1 H1): m_products に入れるコード (codes: code → 商品区分) のうち **Company DB にある SKU** × 要る列の値が世代に全部あるか。
 * C にある SKU の欄 (null は値。欄そのものが無い) が 1 つでも欠ける = 使わない (C が正なのに NE の値で黙って埋めない)。
 * Company DB に無い SKU (NE にしか無い = 夜間ロードがまだ入れていない新しい商品など) は欠けではない = 写さない (NE の値のまま作る)・notInCdb に数えて知らせる
 * (1 件のために全部を止めない。現場を推測で止めない)。
 * 同じ norm になるコードが 2 つ以上あり、その SKU が C にある = 1 つの C の値を何件にも配らない (Codex R0 #10)
 * 種類が食い違う SKU (NE で セット → 単品 にしたのに C はまだセット など) = その SKU だけ写さない (NE の値のまま)・kindMismatch に数えて知らせる
 *   (1 行の食い違いで作り直し全部を止めない。#1564 の見直し M-5。食い違いそのものは照合 ② の kind で出る)
 *   🆕 区分の持ち主が C (cols に kind): C = 単品・NE = セット = 除かない (kindCSingleNeSet。要る列は C の区分 = 単品で決める) /
 *     ほかの食い違い (C = セット・NE = 単品・例外を含む升) = 除く (kindFrozen = 前の行のまま・前の行が無ければ載せない)
 * @param {Map<string, string>} codes  m_products のコード → 商品区分 (単品 / セット / 例外)
 * @param {Map<string, { v: object }>} entries  code_norm → 世代の値 (_sku の行で C にある SKU は全部入る)
 * @returns {{ missing: Array<[string, string]>, collided: string[][], notInCdb: number, notInCdbCodes: string[], kindMismatch: number, kindMismatchCodes: string[], kindMismatchNorms: Set<string> }}
 */
export function completeness(codes, entries, cols) {
  const perNorm = new Map();
  for (const code of codes.keys()) { const k = normSku(code); if (!perNorm.has(k)) perNorm.set(k, []); perNorm.get(k).push(code); }
  const collided = [...perNorm].filter(([k, list]) => list.length > 1 && entries.has(k)).map(([, list]) => list);
  const missing = [];
  const notIn = [];
  const kindMis = [];
  const kindSingleNeSet = [];
  const kindFrozen = [];
  const copyKind = (cols || []).includes('kind');
  for (const [code, kind] of codes) {
    const e = entries.get(normSku(code));
    if (!e) { notIn.push(code); continue; }
    if (kindDiffers(e, kind)) {
      if (!copyKind) { kindMis.push(code); continue; }
      if (!(e.kind === 'single' && kind === 'セット')) { kindFrozen.push(code); continue; }   // C = セット・NE = 単品 / 例外を含む = 前の行のまま
      kindSingleNeSet.push(code);                                                            // C = 単品・NE = セット = 単品として写す
    }
    for (const col of requiredCols(copyKind ? targetKind(e, kind) : kind, cols)) if (!Object.hasOwn(e.v, col)) missing.push([code, col]);
  }
  return { missing, collided, notInCdb: notIn.length, notInCdbCodes: notIn.sort().slice(0, NOT_IN_CDB_SAMPLES),
    kindMismatch: kindMis.length, kindMismatchCodes: [...kindMis].sort().slice(0, NOT_IN_CDB_SAMPLES), kindMismatchNorms: new Set(kindMis.map((c) => normSku(c))),
    kindCSingleNeSet: kindSingleNeSet.length, kindCSingleNeSetCodes: [...kindSingleNeSet].sort().slice(0, NOT_IN_CDB_SAMPLES),
    kindFrozen: kindFrozen.length, kindFrozenCodes: [...kindFrozen].sort().slice(0, NOT_IN_CDB_SAMPLES), kindFrozenAll: [...kindFrozen].sort(), kindFrozenNorms: new Set(kindFrozen.map((c) => normSku(c))) };
}
/** Company DB の SKU の種類 → m_products の商品区分 */
export const KIND_TO_M = Object.freeze({ single: '単品', set: 'セット', exception: '例外' });
/** C の種類と m_products (今朝の NE) の商品区分が違う (種類が分からない = 違うとは言わない) */
export const kindDiffers = (e, mKind) => !!e && Object.hasOwn(KIND_TO_M, e.kind) && KIND_TO_M[e.kind] !== mKind;
/** 区分の持ち主が C のときに m_products に入れる商品区分 (C の区分。知らない区分 = 今までの区分) */
export const targetKind = (e, mKind) => (e && Object.hasOwn(KIND_TO_M, e.kind) ? KIND_TO_M[e.kind] : mKind);
/** entries から norms を除いた写し (種類が食い違う SKU = C に無いのと同じに扱う) */
export function withoutNorms(entries, norms) {
  if (!norms || !norms.size) return entries;
  const m = new Map(entries);
  for (const k of norms) m.delete(k);
  return m;
}
/** 世代の行 → code_norm ごとの値 (readCurrentPublish と同じ形) */
export function entriesOfRows(rows) {
  const entries = new Map();
  for (const r of rows) {
    if (!entries.has(r.code_norm)) entries.set(r.code_norm, { code: r.code, kind: r.sku_kind, v: {} });
    if (r.col === PRESENCE_COL) { JSON.parse(r.value); continue; }   // SKU があることだけ (値の列ではない)
    entries.get(r.code_norm).v[r.col] = JSON.parse(r.value);
  }
  return entries;
}

/** 今の世代の番号 (sync_meta)。無ければ null */
export function currentGenerationNo(db) {
  try {
    const v = db.prepare('SELECT value FROM sync_meta WHERE key = ?').get(PUBLISH_CURRENT_KEY)?.value;
    const n = v == null || v === '' ? null : Number(v);
    return Number.isSafeInteger(n) && n > 0 ? n : null;
  } catch { return null; }
}

/**
 * 今の世代を読む。values = true のときだけ値も読み、入れた時のハッシュと行数を確かめる。
 * @returns {{ generation: object|null, entries: Map<string, { code, kind, v: object }>|null, problem: string|null }}
 *   problem = no_table / no_generation / not_verified / hash_mismatch / value_unreadable
 */
export function readCurrentPublish(db, { values = true } = {}) {
  let no;
  try { db.prepare('SELECT 1 FROM cdb_publish_generations LIMIT 1').get(); no = currentGenerationNo(db); }
  catch { return { generation: null, entries: null, problem: 'no_table' }; }
  if (no == null) return { generation: null, entries: null, problem: 'no_generation' };
  return readPublishGeneration(db, no, { values });
}
/**
 * 番号を指定して世代を読む (作り直しが使った世代で確かめる = 今の印が先へ進んでいても、その作り直しの材料と比べる。Codex #1564 R1 の後の見直し H-A)。
 * 戻り値の形は readCurrentPublish と同じ。消えた (14 世代より古い) = no_generation
 */
export function readPublishGeneration(db, generationNo, { values = true } = {}) {
  let generation;
  try { generation = db.prepare('SELECT * FROM cdb_publish_generations WHERE generation_no = ?').get(generationNo) || null; }
  catch { return { generation: null, entries: null, problem: 'no_table' }; }
  if (!generation) return { generation: null, entries: null, problem: 'no_generation' };
  if (generation.state !== 'verified') return { generation, entries: null, problem: 'not_verified' };
  if (!values) return { generation, entries: null, problem: null };
  const rows = db.prepare('SELECT code_norm, col, code, sku_kind, value FROM cdb_publish_values WHERE generation_no = ?').all(generation.generation_no);
  if (rows.length !== generation.row_count || publishContentHash(rows) !== generation.content_hash) return { generation, entries: null, problem: 'hash_mismatch' };
  try { return { generation, entries: entriesOfRows(rows), problem: null }; } catch { return { generation, entries: null, problem: 'value_unreadable' }; }
}

// ─────────── 逆向きの決まり (Company DB の形 → m_products の形。15 §2) ───────────
/** 原価: C の原価が空 = 原価状態 MISSING・原価 NULL (OVERRIDDEN のまま空にすると品質チェック B6 で作り直し全体が止まる) */
export function costFromCdb(c) {
  if (!c) return { genka: null, genkaSource: '不明', genkaStatus: 'MISSING' };
  const genkaSource = COST_SOURCE_TO_M[c.source];
  if (!genkaSource) throw Object.assign(new Error(`知らない原価の出どころ: ${c.source}`), { code: 'CDB_PUBLISH_COST_SOURCE' });
  return { genka: c.jpy, genkaSource, genkaStatus: c.status };
}
/**
 * 取扱区分: Company DB は「取扱中止」と「ﾒｰｶｰ取扱中止」を discontinued 1 つにまとめる (sources.mjs の mapHandling)。
 * NE の語が同じ区分に当たるときは NE の語を残す (区分が同じなら元の語のまま)。unknown = 空
 */
export function handlingFromCdb(c, neWord) {
  const t = typeof neWord === 'string' ? neWord.trim() : '';
  if (c === 'active') return t === '取扱中' ? neWord : '取扱中';
  if (c === 'discontinued') return t && t !== '取扱中' ? neWord : '取扱中止';
  return t ? null : (neWord ?? null);
}
/** 代表の仕入先: 同じ仕入先 (0 埋めの形で比べる) なら NE の書き方を残す。C に代表が無い = NULL */
export function supplierFromCdb(c, neRaw) {
  if (c == null) return null;
  const t = neRaw == null ? '' : String(neRaw).trim();
  if (t && normSku(canonicalSupplierCode(t)) === normSku(c)) return neRaw;
  return c;
}

const canon = (v) => JSON.stringify(v === undefined ? null : v, (k, x) => (typeof x === 'number' ? Math.round(x * 1e6) / 1e6 : x === undefined ? null : x));
const same = (a, b) => canon(a) === canon(b);
/**
 * 作り直しの理由を直す。C の値で変わった列だけ、その列の今までの理由を外して company_owned を足す (Codex R1 H4):
 *   owner_key = 持ち主のキー / cdb_value = 変換の前の C の値 (世代の欄) / value = 古い表 (m_products) に入れた値 / ne_value = C が無ければ入った値 (今までの決め方) /
 *   generation_no = 使った世代 / cdb_cost_source = 原価の元の出どころ。変わらなかった列の理由はそのまま (今までの順)
 * @param {{ cells?: object|null, generationNo?: number|null }} [ctx]
 */
export function mergeReasons(code, kind, neV, v, reasons, { cells = null, generationNo = null } = {}) {
  const changed = GROUPS.filter(([, pick]) => !same(pick(neV), pick(v)));
  if (!changed.length) return reasons;
  const drop = new Set(changed.flatMap(([, , cols]) => cols));
  return [...reasons.filter((r) => !drop.has(r.col)),
    ...changed.map(([col, pick, , pubCols]) => {
      const cellCols = pubCols.filter((c) => cells && Object.hasOwn(cells, c));
      return {
        code, kind, col, reason: 'company_owned',
        owner_key: pubCols.map((c) => PUBLISH_COLUMNS[c]).join('+'),
        cdb_value: cellCols.length ? Object.fromEntries(cellCols.map((c) => [c, cells[c]])) : null,   // セットの導く列 = セット自身の欄 (無ければ null)
        value: pick(v) ?? null, ne_value: pick(neV) ?? null, generation_no: generationNo,
        ...(col === 'cost' && Object.hasOwn(v, 'cdbCostSource') ? { cdb_cost_source: v.cdbCostSource } : {}),
      };
    })];
}

/**
 * 作り直しの理由: 区分の持ち主が C で、今までの区分 (NE の表の形) と違う C の区分を m_products に入れた (照合 ② が由来を読む・mergeReasons と同じ形)
 */
export function kindReason(code, mKind, toKind, { cdbKind = null, generationNo = null } = {}) {
  return { code, kind: toKind, col: 'kind', reason: 'company_owned', owner_key: 'skus.sku_kind',
    cdb_value: cdbKind ? { kind: cdbKind } : null, value: toKind, ne_value: mKind, generation_no: generationNo };
}

/**
 * 列ごとに「C の値か、今までの値か」を決める道具。
 * @param {object} p
 * @param {object} p.ownership   持ち主の設定 (config/master-ownership.mjs。試験は差し替える)
 * @param {ReturnType<typeof readCurrentPublish>|null} p.publication  今の世代
 * @param {Map<string, '単品'|'セット'|'例外'>} p.staged  この作り直しで m_products に入れるコード (NE の小文字のコード) → 商品区分
 * @param {{ decimal: number, category: string }[]} p.taxRates  既知税率 (rebuild-m-products.js の TAX_RATES)
 * @returns problem = 持ち主が C の列があるのに使えない = 作り直しを止める:
 *   ownership_not_supported (一緒に切り替える組が違う・④a が写さない列が company) / no_generation / not_verified / hash_mismatch /
 *   ownership_mismatch (別の epoch の世代は使わない) / value_missing (m_products に入れるコード × 要る列の欄が欠ける = NE の値で黙って埋めない) /
 *   target_norm_collision (m_products の 2 つ以上のコードが同じ C の SKU に当たる = 1 つの C の値を何件にも配らない) /
 * 区分の持ち主が C で C = セット・NE = 単品 / 例外を含む食い違いの SKU = 止めない・その SKU だけ前の行のまま (frozenCodes。呼び手が前の行を写す・無ければ載せない)
 */
export function makePublishResolver({ ownership, publication = null, staged = new Map(), taxRates = [] }) {
  const cols = publishCols(ownership);
  const gen = publication?.generation ?? null;
  const oh = ownershipHash(ownership);
  const sameOwner = !!gen && gen.state === 'verified' && gen.ownership_hash === oh;
  const genInfo = sameOwner ? { generation_no: gen.generation_no, generation_id: gen.generation_id, content_hash: gen.content_hash, ownership_hash: oh } : null;
  const expected = new Map();   // code → 持ち主が load の列の、今までの決め方の値 (入れた後の確かめで「変えていない」を見る)
  const expectedComps = new Map();   // `${set}|${child}` → 今までの決め方の構成品の名前・原価
  const inactive = {
    active: false, cols, problem: null, problemDetail: null, publication, ownership,
    // 持ち主が全部 load でも、持ち主が同じ今の世代 (値 0 行) を記録に残す (毎日しくみが通ったことの跡)。別の epoch の世代は「使った」と書かない
    generation: cols.length === 0 ? genInfo : null,
    of: () => null, peek: () => null, owns: () => false, has: () => false, stagedCodes: () => [], expect: () => {}, expectComponent: () => {}, expected: null, kindOf: (code, mKind) => mKind,
    frozenCodes: () => [], forget: () => {},
    overlay: (code, neV) => neV, stats: () => ({ used: 0, not_in_ne: 0, not_in_cdb: 0, not_in_cdb_codes: [], kind_mismatch: 0, kind_mismatch_codes: [] }),
  };
  const fail = (problem, problemDetail = null) => ({ ...inactive, generation: null, problem, problemDetail });
  const ownProblems = checkPublishOwnership(ownership);
  if (ownProblems.length) return fail('ownership_not_supported', ownProblems);
  if (cols.length === 0) return inactive;
  if (!publication || !gen) return fail('no_generation');
  if (publication.problem) return fail(publication.problem);
  if (!sameOwner) return fail('ownership_mismatch', { generation_no: gen.generation_no, generation_owner: gen.ownership_hash, local_owner: oh });
  if (!publication.entries) return fail('no_values');
  const ownSet = new Set(cols);
  const has = (e, col) => ownSet.has(col) && Object.hasOwn(e.v, col);
  const comp = completeness(staged, publication.entries, cols);
  if (comp.collided.length) return fail('target_norm_collision', { count: comp.collided.length, samples: comp.collided.slice(0, 5) });
  if (comp.missing.length) return fail('value_missing', { count: comp.missing.length, samples: comp.missing.slice(0, 5) });
  // 種類が食い違う SKU = C に無いのと同じ (写さない・NE の値のまま・確かめも NE の値を待つ)
  // C = セット・NE = 単品 / 例外を含む食い違い (区分の持ち主が C) = 写さない (前の行のまま。呼び手が frozenCodes の前の行を写す)
  const entries = withoutNorms(withoutNorms(publication.entries, comp.kindMismatchNorms), comp.kindFrozenNorms);
  const stagedNorms = new Set([...staged.keys()].map((c) => normSku(c)));
  const used = new Set();
  const categoryOf = (rate) => taxRates.find((t) => t.decimal === rate)?.category ?? 'UNKNOWN';
  const groupOwned = (pubCols) => pubCols.some((c) => ownSet.has(c));
  const r = {
    active: true, cols, problem: null, problemDetail: null, publication, ownership, generation: genInfo,
    owns: (col) => ownSet.has(col),
    has,
    stagedCodes: () => [...staged.keys()],
    /** C の値 (持ち主が C の列だけ)。C に無い SKU (NE にしか無い) = null = 今までの値で作る (写さない) */
    of(code) {
      const k = normSku(code); const e = entries.get(k);
      if (!e) return null;
      used.add(k);
      return e;
    },
    /** of と同じ (数えない) */
    peek: (code) => entries.get(normSku(code)) ?? null,
    /**
     * m_products に入れる商品区分。区分の持ち主が C で C にある SKU = C の区分 (NE と違っても) / それ以外 = 今までの区分 (mKind = NE の表の形)。
     * 呼び手 (rebuild-m-products.js) は区分が今までと違う SKU を、C の区分の形で作る (構成の行・構成品から導く値を使わない)
     */
    kindOf(code, mKind) {
      if (!ownSet.has('kind')) return mKind;
      const e = entries.get(normSku(code));
      return e ? targetKind(e, mKind) : mKind;
    },
    /** C = セット・NE = 単品 / 例外を含む食い違い の m_products のコード (前の行のまま。呼び手が前の m_products・m_set_components の行を写す・無ければ載せない) */
    frozenCodes: () => [...staged.keys()].filter((c) => comp.kindFrozenNorms.has(normSku(c))),
    /** 前の行を写した SKU = 「今までの決め方の値」を確かめない (expect を外す) */
    forget(code) { expected.delete(code); for (const k of [...expectedComps.keys()]) if (k.startsWith(`${code}|`)) expectedComps.delete(k); },
    /**
     * 1 つの SKU の値 (今までの決め方の値 neV) に C の値を重ねる。C に無ければ neV をそのまま返す。
     * set = true (セット): 税率・税区分・売上分類・取扱区分は構成品から導く (呼び手が C の構成品で導き直して neV に入れて渡す)。
     *   原価は C の行が 'set_calc' でない (人が決めた原価) ときだけ C の値 (set_calc・空 = 構成品から導いた値のまま)
     * neV = { name, handling, price, genka, genkaSource, genkaStatus, shipCost, shipCode, shipMethod, taxRate, taxCategory, supplier, salesClass }
     */
    overlay(code, neV, { set = false } = {}) {
      const e = r.of(code);
      if (!e) return neV;
      const v = { ...neV };
      if (has(e, 'name')) v.name = e.v.name;
      if (has(e, 'standard_price')) v.price = e.v.standard_price;
      if (has(e, 'cost') && (!set || (e.v.cost && e.v.cost.source !== 'set_calc'))) { Object.assign(v, costFromCdb(e.v.cost)); v.cdbCostSource = e.v.cost ? e.v.cost.source : null; }
      if (!set) {
        if (has(e, 'tax_rate')) { v.taxRate = e.v.tax_rate; v.taxCategory = has(e, 'tax_class') ? (e.v.tax_class ?? 'UNKNOWN') : categoryOf(e.v.tax_rate); }
        else if (has(e, 'tax_class')) v.taxCategory = e.v.tax_class ?? 'UNKNOWN';
        if (has(e, 'sales_class')) v.salesClass = e.v.sales_class;
        if (has(e, 'handling')) v.handling = handlingFromCdb(e.v.handling, neV.handling);
      }
      if (has(e, 'shipping')) { v.shipCost = e.v.shipping.cost_jpy; v.shipCode = e.v.shipping.code; v.shipMethod = e.v.shipping.method; }
      if (has(e, 'primary_supplier')) v.supplier = supplierFromCdb(e.v.primary_supplier, neV.supplier);
      return v;
    },
    /** 持ち主が load の列の、今までの決め方の値を覚える (入れた後の確かめで「C の値を重ねていない列は今までの値のまま」を見る。Codex R1 H2) */
    expect(code, neV) {
      const keep = {};
      const inCdb = entries.has(normSku(code));
      // C に無い SKU (NE にしか無い) = 全部の列が今までの値のまま / C にある SKU = 持ち主が load の列だけ
      for (const [g, pick, , pubCols] of GROUPS) if (!inCdb || !groupOwned(pubCols)) keep[g] = pick(neV);
      expected.set(code, keep);
    },
    /** 構成品の行の名前・原価。C の値を使った構成品 (fromCdb) = 持ち主が load の列だけ今までの値 / 使わなかった (セットか構成品が C に無い) = 両方とも NE の値のまま */
    expectComponent(setCode, child, { name, cost }, { fromCdb = false } = {}) {
      expectedComps.set(`${setCode}|${child}`, { name: fromCdb && ownSet.has('name') ? undefined : name, cost: fromCdb && ownSet.has('cost') ? undefined : cost });
    },
    get expected() { return { products: expected, components: expectedComps }; },
    /** 重ねた SKU・Company DB にしか無い SKU (m_products に足さない = not_in_ne)・NE にしか無い SKU (写さない = NE の値のまま = not_in_cdb) */
    stats() {
      let notInNe = 0;
      for (const k of entries.keys()) if (!stagedNorms.has(k)) notInNe++;
      return { used: used.size, not_in_ne: notInNe, not_in_cdb: comp.notInCdb, not_in_cdb_codes: comp.notInCdbCodes, kind_mismatch: comp.kindMismatch, kind_mismatch_codes: comp.kindMismatchCodes,
        // 区分の持ち主が C のときだけ: NE と区分が違うのに C の区分で写した SKU (持ち主が load の今は出ない = 記録の形は今までと同じ)
        ...(ownSet.has('kind') ? { kind_c_single_ne_set: comp.kindCSingleNeSet, kind_c_single_ne_set_codes: comp.kindCSingleNeSetCodes,
          kind_c_set_ne_single_frozen: comp.kindFrozen, kind_c_set_ne_single_frozen_codes: comp.kindFrozenCodes } : {}) };
    },
  };
  return r;
}

/** m_products の行 → 列の組の値 (GROUPS と同じ形。入れた後の確かめで今までの値と比べる) */
const groupOfRow = (m) => ({
  name: m.商品名, price: m.標準売価, cost: { 原価: m.原価, 原価ソース: m.原価ソース, 原価状態: m.原価状態 }, tax_rate: { 消費税率: m.消費税率, 税区分: m.税区分 },
  sales_class: m.売上分類, shipping: { 送料: m.送料, 送料コード: m.送料コード, 配送方法: m.配送方法 }, handling: m.取扱区分, primary_supplier: m.仕入先コード,
});

/**
 * 上書き表を C の値にそろえる。rebuild-m-products.js の入れ替えの取引の中・作り直しの記録の前に呼ぶ (m_products は入れ替えた後の値)。
 * この作り直しの m_products のコードで、持ち主が C の列だけ (Company DB にしか無い SKU の行は作らない。Codex R1 H3)。空の表し方:
 *   exception_genka (財務の SQL 5 本が m_products より先に読む): 既にある行だけ。原価が空なら行を消す (行が無い = 例外原価なし)・値なら直す。行は作らない
 *     (原価が空の行を残すと、財務は古い原価を使い、持ち主を load に戻した日に作り直しが B6 で止まる)
 *   product_shipping (財務の送料の質 'actual' = 行の ship_cost が空でない): 既にある行だけ。C の送料が全部空なら行を消す・値なら直す。行は作らない
 *     (代表の名札の行 = SKU ではないので触らない)
 *   m_reorder_setting (商品管理リストの snapshot が直接読む): 値なら入れる・直す / 空 (未登録) なら行を消す
 * @returns {{ exception_genka: { updated, deleted }, product_shipping: { updated, deleted }, m_reorder_setting: { updated, inserted, deleted } }}
 */
export function applySideTables(db, pub, { now = new Date() } = {}) {
  const out = { exception_genka: { updated: 0, deleted: 0 }, product_shipping: { updated: 0, deleted: 0 }, m_reorder_setting: { updated: 0, inserted: 0, deleted: 0 } };
  if (!pub || !pub.active) return out;
  const at = now.toISOString();
  const staged = new Set(pub.stagedCodes());
  const mp = db.prepare('SELECT 商品名, 原価, 送料, 送料コード, 配送方法 FROM m_products WHERE 商品コード = ?');
  const target = (sku, col) => {
    const code = typeof sku === 'string' ? sku.toLowerCase() : null;   // 作り直しと同じ鍵 (小文字)
    if (!code || !staged.has(code)) return null;
    const e = pub.peek(code);
    return e && pub.has(e, col) ? { code, e } : null;
  };
  if (pub.owns('cost')) {
    const upd = db.prepare('UPDATE exception_genka SET genka = ?, synced_at = ? WHERE sku = ?');
    const del = db.prepare('DELETE FROM exception_genka WHERE sku = ?');
    for (const r of db.prepare('SELECT sku, genka FROM exception_genka').all()) {
      const t = target(r.sku, 'cost'); if (!t) continue;
      const m = mp.get(t.code);
      if (!m) continue;
      if (m.原価 == null) { del.run(r.sku); out.exception_genka.deleted++; continue; }
      if (same(r.genka, m.原価)) continue;
      upd.run(m.原価, at, r.sku); out.exception_genka.updated++;
    }
  }
  if (pub.owns('shipping')) {
    const upd = db.prepare('UPDATE product_shipping SET shipping_code = ?, ship_method = ?, ship_cost = ?, synced_at = ? WHERE sku = ?');
    const del = db.prepare('DELETE FROM product_shipping WHERE sku = ?');
    for (const r of db.prepare('SELECT sku, shipping_code, ship_method, ship_cost FROM product_shipping').all()) {
      const t = target(r.sku, 'shipping'); if (!t) continue;
      const s = t.e.v.shipping;
      if (s.cost_jpy == null && s.code == null && s.method == null) { del.run(r.sku); out.product_shipping.deleted++; continue; }
      if (same([r.shipping_code, r.ship_method, r.ship_cost], [s.code, s.method, s.cost_jpy])) continue;
      upd.run(s.code, s.method, s.cost_jpy, at, r.sku); out.product_shipping.updated++;
    }
  }
  if (pub.owns('reorder_months')) {
    const byCode = new Map();
    for (const r of db.prepare('SELECT sku, 推奨保有月数 FROM m_reorder_setting').all()) {
      const k = typeof r.sku === 'string' ? r.sku.toLowerCase() : null; if (!k) continue;
      if (!byCode.has(k)) byCode.set(k, []);
      byCode.get(k).push(r);
    }
    const ins = db.prepare("INSERT INTO m_reorder_setting (sku, 推奨保有月数, 商品名, updated_by, synced_at) VALUES (?, ?, ?, 'company_db', ?)");
    const upd = db.prepare("UPDATE m_reorder_setting SET 推奨保有月数 = ?, updated_by = 'company_db', synced_at = ? WHERE sku = ?");
    const del = db.prepare('DELETE FROM m_reorder_setting WHERE sku = ?');
    for (const code of staged) {
      const e = pub.peek(code); if (!e || !pub.has(e, 'reorder_months')) continue;
      const v = e.v.reorder_months;
      const rows = byCode.get(code) || [];
      if (v == null) { for (const r of rows) { del.run(r.sku); out.m_reorder_setting.deleted++; } continue; }
      if (!rows.length) { ins.run(code, v, mp.get(code)?.商品名 ?? null, at); out.m_reorder_setting.inserted++; continue; }
      for (const r of rows) if (!same(r.推奨保有月数, v)) { upd.run(v, at, r.sku); out.m_reorder_setting.updated++; }
    }
  }
  return out;
}

/**
 * 入れた後の確かめ (Codex R0 #4・R1 H2): m_products・m_set_components・m_reorder_setting・そろえた上書き表を読み直し、世代の値と
 * 「全部のキー × 持ち主が C の列」で比べる (値・空・行の有無)。expected があれば (作り直しの取引の中)、持ち主が load の列が今までの決め方の値のままかも見る。
 * 作り直しの取引の中 (記録の前。違えば巻き戻す) と、daily-sync の次の工程 (fetch.mjs --verify-apply) で呼ぶ。
 * 取扱区分・代表の仕入先は NE の語・書き方を残すので、夜間ロードと同じ向き (mapHandling・0 埋め) にそろえて比べる。
 * セットの導く列 (税率・税区分・売上分類・取扱区分・原価の set_calc / 空) は構成品から導くので世代とは比べない (数だけ数える)
 * NE にしか無い SKU (C に無い = 写していない) は比べない (そのまま) = not_in_cdb に数え、コードの一部を not_in_cdb_codes に
 * @returns {{ ok: boolean, problems: object[], counts: { keys, checked, derived, not_in_ne, not_in_cdb, unchanged }, not_in_cdb_codes: string[], applied_hash: string }}
 */
/**
 * C にあるセットの導き方の入力・構成品の行を入れ替える (作り直しの取引の中で m_products と一緒に。#1564 Codex R7 High)。
 * rows = [{ code, args: deriveSetValues の引数, components: [{ c, qty, name, cost, from_cdb }] }]
 */
export function writeSetPublishExpect(db, rows) {
  db.exec('DELETE FROM m_set_publish_expect');
  const ins = db.prepare('INSERT INTO m_set_publish_expect (set_code, args_json, components_json) VALUES (?, ?, ?)');
  for (const r of rows) ins.run(r.code, JSON.stringify(r.args), JSON.stringify(r.components));
}
/** 前の行のまま の SKU の snapshot に入れる列 (m_products は product_id = 入れ替えで振り直す番号 を除く全部・m_set_components は全部) */
export const KIND_FROZEN_MP_COLS = Object.freeze(['商品コード', '商品名', '商品区分', '取扱区分', '標準売価', '原価', '原価ソース', '原価状態', '送料', '送料コード', '配送方法',
  '消費税率', '税区分', '在庫数', '引当数', '仕入先コード', 'セット構成品数', '売上分類', 'seasonality_flag', 'season_months', 'new_product_flag', 'new_product_launch_date', 'updated_at']);
export const KIND_FROZEN_MSC_COLS = Object.freeze(['セット商品コード', '構成商品コード', '数量', '構成商品名', '構成商品原価', 'updated_at']);
/**
 * 1 つのコードの m_products の行と m_set_components の構成の行 (全部) の正準な形 (JSON の文字列)。行が無い = product null。
 * tables = 読む表 (作り直しは入れ替えの前の main の表 = 固定する行・確かめは今の表)
 */
export function kindFrozenSnapshot(db, code, { products = 'm_products', components = 'm_set_components' } = {}) {
  const q = (cols) => cols.map((c) => `"${c}"`).join(', ');
  const p = db.prepare(`SELECT ${q(KIND_FROZEN_MP_COLS)} FROM ${products} WHERE 商品コード = ?`).raw().get(code) ?? null;
  const cs = db.prepare(`SELECT ${q(KIND_FROZEN_MSC_COLS)} FROM ${components} WHERE セット商品コード = ? ORDER BY 構成商品コード`).raw().all(code);
  return JSON.stringify({ product: p, components: cs });
}
/** 前の行のまま (prev = true) / 載せない (prev = false) にした SKU と固定した時の snapshot (作り直しの取引で m_products と一緒に入れ替える。区分の持ち主が C のときだけ書く) */
export function writeKindFrozen(db, rows) {
  db.exec('DELETE FROM m_publish_kind_frozen');
  const ins = db.prepare('INSERT INTO m_publish_kind_frozen (code, prev_row, snapshot) VALUES (?, ?, ?)');
  for (const r of rows) ins.run(r.code, r.prev ? 1 : 0, r.snapshot);
}
/** 前の行のまま / 載せない にした SKU の印 [{code, prev_row, snapshot}] (表が無い = 空) */
export function readKindFrozen(db) {
  try { return db.prepare('SELECT code, prev_row, snapshot FROM m_publish_kind_frozen ORDER BY code').all(); } catch (e) {
    if (/no such table/.test(String(e && e.message))) return [];
    throw e;
  }
}
/** 作り直しが残したセットの導き方の入力・構成品の行 (表が無い = 空) */
function readSetPublishExpect(db) {
  try {
    return new Map(db.prepare('SELECT set_code, args_json, components_json FROM m_set_publish_expect').all()
      .map((r) => [r.set_code, { args: JSON.parse(r.args_json), components: JSON.parse(r.components_json) }]));
  } catch (e) {
    if (/no such table/.test(String(e && e.message))) return new Map();
    throw e;
  }
}
export function verifyApplied(db, { publication, ownership, taxRates = [], expected = null, maxProblems = 20, kindCopied = null }) {
  const cols = publishCols(ownership);
  const out = { ok: true, problems: [], counts: { keys: 0, checked: 0, derived: 0, not_in_ne: 0, not_in_cdb: 0, unchanged: 0, kind_mismatch: 0, mixed_sets: 0 }, not_in_cdb_codes: [],
    kind_mismatch_codes: [], mixed_set_codes: [], applied_hash: null };
  const lines = [];
  const bad = (code, col, want, actual) => { out.ok = false; if (out.problems.length < maxProblems) out.problems.push({ code, col, expected: want ?? null, actual: actual ?? null }); };
  if (!cols.length) { out.applied_hash = sha256Lines(lines); return out; }
  const allEntries = publication && publication.entries;
  if (!allEntries) { bad(null, '*', 'publication', publication ? publication.problem : 'none'); out.applied_hash = sha256Lines(lines); return out; }
  const ownSet = new Set(cols);
  const categoryOf = (rate) => taxRates.find((t) => t.decimal === rate)?.category ?? 'UNKNOWN';
  const lower = (x) => (typeof x === 'string' ? x.toLowerCase() : null);
  const rows = db.prepare(`SELECT 商品コード, 商品名, 商品区分, 取扱区分, 標準売価, 原価, 原価ソース, 原価状態, 送料, 送料コード, 配送方法, 消費税率, 税区分, 仕入先コード, 売上分類${ownSet.has('kind') ? ', セット構成品数' : ''} FROM m_products`).all();
  const group = (sql) => { const m = new Map(); for (const r of db.prepare(sql).all()) { const k = lower(r.sku); if (!k) continue; if (!m.has(k)) m.set(k, []); m.get(k).push(r); } return m; };
  const reorder = ownSet.has('reorder_months') ? group('SELECT sku, 推奨保有月数 FROM m_reorder_setting') : new Map();
  const eg = ownSet.has('cost') ? group('SELECT sku, genka FROM exception_genka') : new Map();
  const ps = ownSet.has('shipping') ? group('SELECT sku, shipping_code, ship_method, ship_cost FROM product_shipping') : new Map();
  const codes = new Map(rows.map((m) => [m.商品コード, m.商品区分]));
  // C にあるセット: 作り直しが残した導き方の入力 (同じ決め方 = lib/master-set-rules.js の deriveSetValues で導き直す) と構成品の行 (#1564 Codex R7 High)
  const setExpect = readSetPublishExpect(db);
  const setCompRows = new Map();
  for (const r of db.prepare('SELECT セット商品コード AS s, 構成商品コード AS c, 数量 AS qty, 構成商品名 AS name, 構成商品原価 AS cost FROM m_set_components').all()) {
    const k = lower(r.s); if (!setCompRows.has(k)) setCompRows.set(k, []); setCompRows.get(k).push(r);
  }
  const SET_DERIVED_COLS = ['cost', 'tax_rate', 'tax_class', 'sales_class', 'handling'];
  const comp = completeness(codes, allEntries, cols);
  for (const list of comp.collided) bad(list.join(','), '*', 'one_code_per_sku', 'norm_collision');
  for (const [code, col] of comp.missing) bad(code, col, 'value', 'missing');
  out.counts.not_in_cdb = comp.notInCdb;
  out.not_in_cdb_codes = comp.notInCdbCodes;
  // 種類が食い違う SKU = 写していない (NE の値のまま) = 比べない (#1564 の見直し M-5)
  out.counts.kind_mismatch = comp.kindMismatch;
  out.kind_mismatch_codes = comp.kindMismatchCodes;
  // 区分の持ち主が C = 区分も写した (m_products の商品区分 = C の区分のはず。違えば下の cols の kind の確かめで落ちる)。
  //   kindCopied = 区分は比べないが、作り直しが区分を写したか (②b = 世代の持ち主から。前の行のまま (frozen) の SKU を比べない)
  const copyKind = kindCopied ?? ownSet.has('kind');
  // C = セット・NE = 単品 / 例外を含む食い違い で前の行のまま にした SKU (作り直しが m_publish_kind_frozen に残す) = 世代の値とは比べない。
  //   代わりに固定した時の snapshot (行・構成の全部。載せない = 行も構成も無い) と今を比べ、ハッシュに入れる (印を残したまま行を書き換えた = 見つかる。#1641 Codex R1 High)
  const frozenRows = copyKind ? readKindFrozen(db) : [];
  const frozen = new Set(frozenRows.map((f) => f.code));
  for (const f of frozenRows) {
    let want;
    try { want = JSON.parse(f.snapshot); } catch { want = null; }
    const got = JSON.parse(kindFrozenSnapshot(db, f.code));
    out.counts.checked++;
    lines.push(JSON.stringify([f.code, 'kind_frozen', f.prev_row, got]));
    if (!want || !same(want, got) || (f.prev_row === 1) !== (got.product != null)) bad(f.code, 'kind_frozen', want, got);
  }
  if (copyKind) { out.counts.kind_frozen = 0; out.kind_frozen_codes = []; }
  const entries = withoutNorms(withoutNorms(withoutNorms(allEntries, comp.kindMismatchNorms), comp.kindFrozenNorms), new Set([...frozen].map((c) => normSku(c))));
  if (copyKind) {
    const fz = [...new Set([...frozen, ...comp.kindFrozenAll])].filter((c) => codes.has(c)).sort();
    out.counts.kind_frozen = fz.length; out.kind_frozen_codes = fz.slice(0, NOT_IN_CDB_SAMPLES);
  }
  // C にあるセットの構成品に C に無い (NE にしか無い) 単品がある = 導いた値に C と NE の値が混ざる = 知らせる (止めない。#1564 の見直し L-2)
  {
    const mixed = new Set();
    for (const r of db.prepare('SELECT セット商品コード AS s, 構成商品コード AS c FROM m_set_components').all()) {
      if (!entries.has(normSku(r.s)) || entries.has(normSku(r.c)) || !codes.has(r.c)) continue;   // C のセット・構成品は NE にだけある (NE にも無い構成品 = 上流の異常 = 別)
      mixed.add(r.s);
    }
    out.counts.mixed_sets = mixed.size;
    out.mixed_set_codes = [...mixed].sort().slice(0, NOT_IN_CDB_SAMPLES);
  }
  const collidedCodes = new Set(comp.collided.flat());
  const matched = new Set();
  for (const m of rows) {
    const code = m.商品コード; const k = normSku(code); const e = entries.get(k);
    const check = (col, want, actual) => { out.counts.checked++; lines.push(JSON.stringify([code, col, JSON.parse(canon(actual))])); if (!same(want, actual)) bad(code, col, want, actual); };
    // 持ち主が load の列 = 今までの決め方の値のまま (作り直しの取引の中だけ)
    const keep = expected && expected.products.get(code);
    if (keep) { const g = groupOfRow(m); for (const [col, want] of Object.entries(keep)) { out.counts.unchanged++; if (!same(want, g[col])) bad(code, `unchanged:${col}`, want, g[col]); } }
    if (!e || collidedCodes.has(code)) continue;
    matched.add(k); out.counts.keys++;
    const kind = m.商品区分; const isSet = kind === 'セット';
    // 構成品から導いたセット = NE のセットの表から作ったセット (構成の行がある)。区分の持ち主が C で、NE の単品を C のセットとして写した行は
    //   構成の行が無い = C の値をそのまま比べる。持ち主が load の今は今までどおり (セット = 導いたセット)
    const derivedSet = isSet && (!copyKind || setCompRows.has(lower(code)));
    // 区分 (持ち主が C の世代だけ) = C の区分 (NE と違っても)・C の区分が単品・例外なら構成の行が無い (構成品数も空)。区分は値の欄でなく世代の行の sku_kind
    if (ownSet.has('kind')) {
      check('kind', KIND_TO_M[e.kind] ?? null, kind);
      if (e.kind !== 'set') check('kind_components', [0, null], [(setCompRows.get(lower(code)) || []).length, m.セット構成品数 ?? null]);
    }
    // C にあるセット = 導いた値 (原価・税率・税区分・売上分類・取扱区分) を同じ決め方で導き直して m_products と比べる・構成品の行 (コード・数量・名前・原価) を比べる。
    //   どちらもハッシュに入れる (作り直しの後に書き換えられた = applied_hash が変わる)
    if (derivedSet && (cols.some((c) => SET_DERIVED_COLS.includes(c)) || cols.includes('name'))) {
      const ex = setExpect.get(code);
      if (!ex) bad(code, 'set_expect', 'row', 'missing');
      else {
        const d = deriveSetValues(ex.args);
        const selfCost = Object.hasOwn(e.v, 'cost') && e.v.cost && e.v.cost.source !== 'set_calc';   // セット自身の人の決めた原価 = 下の cost で C と比べる
        if (ownSet.has('cost') && !selfCost) check('set_derived:cost', [d.genka, d.genkaSource, d.genkaStatus], [m.原価, m.原価ソース, m.原価状態]);
        if (ownSet.has('tax_rate') || ownSet.has('tax_class')) check('set_derived:tax', [d.taxRate, d.taxCategory], [m.消費税率, m.税区分]);
        if (ownSet.has('sales_class')) check('set_derived:sales_class', d.salesClass ?? null, m.売上分類 ?? null);
        if (ownSet.has('handling')) check('set_derived:handling', d.handling, m.取扱区分);
        const want = [...ex.components].sort((a, b) => (a.c < b.c ? -1 : a.c > b.c ? 1 : 0)).map((x) => [x.c, x.qty, x.name, x.cost]);
        const got = (setCompRows.get(lower(code)) || []).map((r) => [lower(r.c), r.qty, r.name, r.cost]).sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));
        check('set_components', want, got);
        // 構成品の名前・原価が C の値のもの (作り直しで C から取った) = 世代の C の値と同じか (残した行も C と合う)
        for (const x of ex.components) {
          if (!x.from_cdb) continue;
          const ce = entries.get(normSku(x.c));
          if (!ce) { bad(`${code}/${x.c}`, 'set_component_cdb', 'entry', 'missing'); continue; }
          if (ownSet.has('name') && Object.hasOwn(ce.v, 'name') && !same(ce.v.name, x.name)) bad(`${code}/${x.c}`, 'set_component_name', ce.v.name, x.name);
          if (ownSet.has('cost') && Object.hasOwn(ce.v, 'cost') && !same(ce.v.cost ? (ce.v.cost.jpy || null) : null, x.cost)) bad(`${code}/${x.c}`, 'set_component_cost', ce.v.cost, x.cost);
        }
      }
    }
    for (const col of cols) {
      if (!Object.hasOwn(e.v, col)) continue;   // 要る欄が無い = 上の完全さで数えた
      const c = e.v[col];
      switch (col) {
        case 'name': check(col, c, m.商品名); break;
        case 'standard_price': check(col, c, m.標準売価); break;
        case 'cost': {
          if (derivedSet && (!c || c.source === 'set_calc')) out.counts.derived++;
          else { const x = costFromCdb(c); check(col, [x.genka, x.genkaSource, x.genkaStatus], [m.原価, m.原価ソース, m.原価状態]); }
          // 例外原価の行: 原価が空なら行が無い・値なら同じ値 (Codex R1 H3)
          const eRows = (eg.get(lower(code)) || []).map((r) => r.genka);
          check('exception_genka', m.原価 == null ? [] : eRows.map(() => m.原価), eRows);
          break;
        }
        case 'tax_rate':
          if (derivedSet) { out.counts.derived++; break; }
          check(col, c, m.消費税率);
          if (!ownSet.has('tax_class')) check('tax_class', categoryOf(c), m.税区分);
          break;
        case 'tax_class': if (derivedSet) out.counts.derived++; else check(col, c ?? 'UNKNOWN', m.税区分); break;
        case 'sales_class': if (kind !== '単品') out.counts.derived++; else check(col, c, m.売上分類); break;
        case 'shipping': {
          const want = [c.cost_jpy, c.code, c.method];
          check(col, want, [m.送料, m.送料コード, m.配送方法]);
          const sRows = (ps.get(lower(code)) || []).map((r) => [r.ship_cost, r.shipping_code, r.ship_method]);
          check('product_shipping', c.cost_jpy == null && c.code == null && c.method == null ? [] : sRows.map(() => want), sRows);   // 全部空 = 行が無い
          break;
        }
        case 'reorder_months': {
          const vals = (reorder.get(lower(code)) || []).map((r) => r.推奨保有月数);
          check(col, c == null ? [] : [c], c == null ? vals : (vals.length ? [...new Set(vals)] : []));   // 空 = 行が無い / 値 = 行があって全部その値
          break;
        }
        case 'handling': if (derivedSet) out.counts.derived++; else check(col, c, mapHandling(m.取扱区分)); break;
        case 'primary_supplier': {
          const t = m.仕入先コード == null ? '' : String(m.仕入先コード).trim();
          check(col, c == null ? null : normSku(c), t ? normSku(canonicalSupplierCode(t)) : null);
          break;
        }
        default: bad(code, col, 'known_col', col);
      }
    }
  }
  // 構成品の行: 持ち主が load の列 (名前・原価) は今までの決め方の値のまま (作り直しの取引の中だけ)
  if (expected && expected.components.size) {
    for (const r of db.prepare('SELECT セット商品コード AS s, 構成商品コード AS c, 構成商品名 AS name, 構成商品原価 AS cost FROM m_set_components').all()) {
      const want = expected.components.get(`${r.s}|${r.c}`); if (!want) continue;
      if (want.name !== undefined) { out.counts.unchanged++; if (!same(want.name, r.name)) bad(`${r.s}/${r.c}`, 'unchanged:構成商品名', want.name, r.name); }
      if (want.cost !== undefined) { out.counts.unchanged++; if (!same(want.cost, r.cost)) bad(`${r.s}/${r.c}`, 'unchanged:構成商品原価', want.cost, r.cost); }
    }
  }
  for (const k of entries.keys()) if (!matched.has(k)) out.counts.not_in_ne++;
  out.applied_hash = sha256Lines(lines);
  return out;
}
