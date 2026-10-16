/**
 * new-skus.js — 新商品の登録 (apps/master-edit) で作った商品を、NE に入る前から入荷予定・先方番号の対応表で使えるようにする (中原さん 10/14・10/16)
 *
 * なぜ: 入荷予定・対応表は商品管理リスト (PML) で商品を確かめる。PML は NE の商品マスタから毎朝作る = ポータルで作っただけの商品は
 *   「NE に登録 → 翌朝」まで使えなかった。→ PML に無い商品だけ、Company DB の新商品 (登録の状態 origin = new_entry) で補う。
 * 中原さんの決定 (10/16): 下書きから使える (やめた商品は使えない)・ロジザードに無いかもしれない商品は警告だけ・範囲は入荷予定と対応表だけ (発注書は後)
 * 読むもの: core.skus (コード = 打ったとおりの書き方 = ロジザード登録の CSV の商品ID と同じ)・名前・今の原価・代表の仕入先・登録の状態・
 *   ロジザード登録の CSV に出したか (ops.lz_new_export_items・0081 の後だけ)
 * Company DB が読めない (URL が無い・つながらない・3 秒を超えた) = 今の動きのまま (PML だけ)。変換・登録は止めない
 * 🚨 PML にある商品は PML のまま (NE の値が正)。ここで補うのは PML に無い商品だけ
 */
import { withCompanyDb } from '../../lib/product-hub-outbox.mjs';
import { COMPANY_ID } from '../../lib/master-write.mjs';
import { normProductCode, normSupplierCode } from './db.js';

export const NEW_SKUS_DEADLINE_MS = 3000;
/** 入荷予定・対応表に使える登録の状態 (やめた・要確認 = NE で直接作った商品 は使わない。要確認は NE にある = PML に出る) */
export const NEW_SKU_USABLE_STATES = Object.freeze(['draft', 'ne_pending', 'ne_confirmed', 'distributable', 'available']);
export const NEW_SKU_STATE_WORDS = Object.freeze({
  draft: '下書き', ne_pending: 'NE登録待ち', ne_confirmed: 'NE確認済み', distributable: '配る対象', available: '登録済み', quarantined: '要確認', cancelled: '登録をやめた',
});
/** NE にまだ無い (かもしれない) 状態 = 画面に「NE 未登録」の印 */
export const NEW_SKU_NOT_IN_NE = Object.freeze(['draft', 'ne_pending']);

/** 新商品の一覧 (読むだけ)。db.query(sql, params) → { rows } */
export async function loadNewSkus(db) {
  const lzReady = (await db.query("select to_regclass('ops.lz_new_export_items') is not null as ok")).rows[0]?.ok === true;
  const rows = (await db.query(`
    select s.code, s.name, r.state,
           (select c.cost_jpy::text from core.sku_costs c where c.sku_id = s.sku_id and c.valid_to is null order by c.valid_from desc limit 1) as cost,
           (select sp.code from core.supplier_skus x join core.suppliers sp on sp.supplier_id = x.supplier_id where x.sku_id = s.sku_id and x.is_primary limit 1) as supplier
           ${lzReady ? ", exists (select 1 from ops.lz_new_export_items i where i.sku_id = s.sku_id and i.state = 'issued') as lz_issued" : ', null::boolean as lz_issued'}
      from core.skus s join ops.master_registrations r on r.sku_id = s.sku_id
     where s.company_id = $1 and s.sku_kind = 'single' and r.origin = 'new_entry'
     order by s.code_norm`, [COMPANY_ID])).rows;
  return rows;
}

/** 行 → 照合キー (normProductCode) ごとの Map。同じキーが 2 つ (大文字・小文字だけ違う) = どちらとも決めない (ambiguous) */
export function newSkuMap(rows) {
  const map = new Map();
  for (const r of rows || []) {
    const key = normProductCode(r.code);
    if (!key) continue;
    const c = r.cost == null || r.cost === '' ? null : Number(r.cost);
    const v = { code: String(r.code), name: r.name || '', state: r.state, cost: Number.isFinite(c) ? c : null,
      supplier: normSupplierCode(r.supplier) || '', lzIssued: r.lz_issued == null ? null : !!r.lz_issued };
    if (map.has(key)) map.set(key, { ...map.get(key), ambiguous: true });
    else map.set(key, v);
  }
  return map;
}

/**
 * Company DB を読む (3 秒まで)。投げない。
 * @returns {Promise<{ ok: boolean, reason: string|null, map: Map<string, { code, name, state, cost, supplier, lzIssued, ambiguous? }> }>}
 */
export async function readNewSkus({ deadlineMs = NEW_SKUS_DEADLINE_MS } = {}) {
  const none = (reason) => ({ ok: false, reason, map: new Map() });
  const work = withCompanyDb((db) => loadNewSkus(db), { applicationName: 'po-new-skus', statementTimeout: `${Math.max(1, Math.ceil(deadlineMs / 1000))}s` })
    .then((r) => (r && r.ok ? { ok: true, reason: null, map: newSkuMap(r.value) } : none(r && r.error === 'no_url' ? 'not_configured' : 'unreachable')))
    .catch((e) => { console.error(`[purchase-orders] 新商品を読めない: ${e && e.message}`); return none('error'); });
  let timer;
  const deadline = new Promise((resolve) => { timer = setTimeout(() => resolve(none('timeout')), deadlineMs); if (timer.unref) timer.unref(); });
  try { return await Promise.race([work, deadline]); } finally { clearTimeout(timer); }
}

let readerOverride = null;
/** 試験用: 読み手を差し替える (null で元に戻す) */
export function setNewSkuReaderForTest(fn) { readerOverride = fn; }
/** 要求ごとに読む (差し替えがあればそれ)。失敗しても投げない */
export async function readNewSkusForRequest() {
  try { return await (readerOverride ? readerOverride() : readNewSkus()); } catch (e) { return { ok: false, reason: 'error', map: new Map() }; }
}

/**
 * PML に無い商品コードを新商品で確かめる (対応表の 1 件登録・紐づけ・仮商品コード)。
 * @returns {{ sku: object|null, error: string|null }} sku = 使える新商品 / error = 断る理由 (画面の文) / 両方 null = 新商品でもない (呼び手の今の扱い)
 */
export function checkNewSku(newSkus, key, supplier) {
  const n = newSkus && newSkus.map ? newSkus.map.get(key) : null;
  if (!n) return { sku: null, error: null };
  if (n.ambiguous) return { sku: null, error: `新商品の登録に大文字・小文字だけ違う商品コードが 2 つあります: ${n.code} (どちらの商品か決められません)` };
  if (!NEW_SKU_USABLE_STATES.includes(n.state)) return { sku: null, error: `この新商品は${NEW_SKU_STATE_WORDS[n.state] || `「${n.state}」`}の状態です: ${n.code} (入荷予定には使えません)` };
  if (!n.supplier) return { sku: null, error: `この新商品は代表の仕入先が未設定です: ${n.code} (マスタの入力で代表の仕入先を入れてください)` };
  if (supplier && n.supplier !== supplier) return { sku: null, error: `この新商品の代表の仕入先は ${n.supplier} です (仕入先 ${supplier} の商品ではありません)` };
  return { sku: n, error: null };
}

/** 応答に載せる Company DB の読み取りの様子 (map は載せない) */
export function newSkusStatus(ns) {
  if (!ns) return { ok: false, reason: 'not_read' };
  return { ok: !!ns.ok, reason: ns.reason ?? null };
}
