/**
 * vendor-codes.js — 先方番号対応表 (po_vendor_code_map) の 1 行の書き込みを 1 か所に (AI_reference CompanyDB構想/27 ⑤-1・§2.4)
 *
 * 先方番号 = 仕入先ごとの先方の品番 (アメージングクラフト・ビーフリー など)。入数 (qty_per_unit) = 先方 1 単位 = 弊社何個 (入荷予定の換算・NULL = 1)。
 * 正本は発注アプリ (Render の warehouse-mirror.db・D-44)。書く入口:
 *   ① 発注アプリのマスタ管理「先方番号対応表」タブの 1 件の追加・修正・削除と、入荷予定タブの「入数だけ直す」(router.js)
 *   ② マスタの入力 (/apps/master-edit) の商品の画面の「先方品番・入数」(apps/master-edit/order-settings.mjs)
 *   ③ マスタの入力の新商品の登録 (発注の設定の知らせ v2 → order-settings.js の writeRegistrationOrderSettings が同じ取引で書く)
 * どれもこの部品の確かめ・記録 (po_audit_log に via・product_key) を通る = 入口が増えても決まりがずれない。
 *   「この商品の仕入先」(仕入先一致) の決まりも 1 つ (productForVendorMap = 商品管理リスト → 無ければ新商品の代表の仕入先)。
 * CSV の全入れ替えと仮登録の昇格は router.js のまま (一括の口・§2.4 の「残す」)。ただし updated_at はこの部品の vendorStamp で付ける
 *   (前より必ず新しい = 消して作り直しても前の版の印にならない・R2 H3)。
 * 応答 ({ status, body }) は今までの発注アプリの API と同じ形・同じ文 (admin の画面をそのまま動かす)。
 */
import { getDB, normSupplierCode, normProductCode } from './db.js';
import { audit } from './ledger.js';
import { loadPml } from './logic.js';
import { readNewSkusForRequest, checkNewSku } from './new-skus.js';

export const VENDOR_CODE_MAX = 100;   // 先方管理番号の実仕様は十数文字。誤貼り付けの肥大化 (監査ログ・添付CSVに複製される) を防ぐ
export const QPU_MAX = 1000000;       // 入数の上限 (極端な値による換算結果の桁あふれ防止)
export const QPU_ERR = `入数は0より大きく${QPU_MAX.toLocaleString('ja-JP')}以下の数で入力してください`;
/** どの画面から書いたか (記録の via) */
export const VENDOR_VIA = Object.freeze({
  'po-admin': '発注アプリのマスタ管理 (先方番号対応表)',
  'po-inbound': '発注アプリの入荷予定 (入数だけ)',
  'master-edit:sku': 'マスタの入力 (商品の画面)',
  'master-edit:new': 'マスタの入力 (新商品の登録)',
});
/** この商品の先方番号の記録 (新商品の登録の知らせを届けるとき「だれかがもう触った」を見る・§2.4 の 6) */
export const VENDOR_HISTORY_ACTIONS = Object.freeze(['vendor_map_entry_upsert', 'vendor_map_entry_delete', 'vendor_map_qty_per_unit', 'vendor_pending_link', 'vendor_map_import']);

const trimS = (v) => String(v == null ? '' : v).trim();
/** 先方番号の照合キー (trim + 大文字。入荷予定の変換の逆引きと同じ) */
export const normVendorCode = (v) => trimS(v).toUpperCase();
const CTRL_RE = new RegExp(`[\\x00-\\x1f\\x7f${String.fromCharCode(0x2028, 0x2029)}]`);
/** 全角の数字・小数点・カンマを半角に (マスタの入力で全角のまま打っても通す) */
const half = (s) => String(s).replace(/[０-９．，]/g, (c) => (c === '，' ? ',' : String.fromCharCode(c.charCodeAt(0) - 0xFEE0)));

export class VendorCodeError extends Error {
  constructor(status, reason, message, field = null) {
    super(message);
    this.name = 'VendorCodeError';
    this.status = status;
    this.reason = reason;
    this.field = field;
  }
}

// ─── 時刻 (先に読んだ値の印) ───
let lastStamp = null;
/**
 * 対応表の updated_at。必ず前より新しい (同じミリ秒・時計の戻りでも前の版と同じ印にしない)。
 * 出した印のいちばん新しいもの = 先方番号の記録 (po_audit_log) の detail.max_stamp に書き込みと同じ取引で残す (vendorStampMark)。
 * 初めて使うときに、表の版・記録の max_stamp・記録の時刻のいちばん新しいものから始める = 再起動の後に時計が戻っても、
 * 消した行・前の版と同じ印を出さない (Codex #⑤ R1 M3・R2 M3)。発注アプリの SQLite を書くのは Render の 1 プロセスだけ
 */
function seedStamp() {
  if (lastStamp != null) return;
  lastStamp = 0;
  try {
    const db = getDB();
    const marks = VENDOR_HISTORY_ACTIONS.map(() => '?').join(',');
    const a = db.prepare('SELECT max(updated_at) AS t FROM po_vendor_code_map').get().t;
    const b = db.prepare(`SELECT max(occurred_at) AS t FROM po_audit_log WHERE action IN (${marks})`).get(...VENDOR_HISTORY_ACTIONS).t;
    const c = db.prepare(`SELECT max(json_extract(detail_json, '$.max_stamp')) AS t FROM po_audit_log WHERE action IN (${marks}) AND json_valid(detail_json)`).get(...VENDOR_HISTORY_ACTIONS).t;
    const d = db.prepare(`SELECT max(json_extract(detail_json, '$.deleted_updated_at')) AS t FROM po_audit_log WHERE action IN (${marks}) AND json_valid(detail_json)`).get(...VENDOR_HISTORY_ACTIONS).t;
    for (const v of [a, b, c, d]) { const ms = v ? Date.parse(v) : NaN; if (Number.isFinite(ms) && ms > lastStamp) lastStamp = ms; }
  } catch (e) { console.error(`[po-vendor-codes] 版の印の始まりを読めない: ${e && e.message}`); }
}
export function vendorStamp() {
  seedStamp();
  const t = Math.max(Date.now(), lastStamp + 1);
  lastStamp = t;
  return new Date(t).toISOString();
}
/** 今までに出した (と表・記録にある) いちばん新しい印。先方番号の記録の detail.max_stamp に入れる (消した行の版も次の起動で使い回さない) */
export function vendorStampMark() {
  seedStamp();
  return lastStamp ? new Date(lastStamp).toISOString() : null;
}
/** 試験用: 起動し直したことにする (次の印は表と記録から始め直す) */
export function __resetVendorStampForTest() { lastStamp = null; }

/** 入数の文字 → 数 (今までの発注アプリと同じ決まり: カンマを除いて 0 より大きく 1,000,000 以下)。空 = null・不正 = undefined */
export function qtyPerUnitOf(v) {
  if (v == null || trimS(v) === '') return null;
  const n = Number(String(v).replace(/,/g, ''));
  return Number.isFinite(n) && n > 0 && n <= QPU_MAX ? n : undefined;
}

/**
 * マスタの入力の欄 (先方品番・入数) の形を確かめる。両方空 = null。
 * 戻り値 { vendorCode: 文字, qtyPerUnit: 数 | null }。誤り = VendorCodeError (field = vendor_code / qty_per_unit)
 */
export function parseVendorInput(raw) {
  if (raw == null) return null;
  if (typeof raw !== 'object' || Array.isArray(raw)) throw new VendorCodeError(400, 'invalid_input', '先方品番の入れ方が違う', 'vendor_code');
  const vcRaw = raw.vendor_code; const qRaw = raw.qty_per_unit;
  if (vcRaw != null && typeof vcRaw !== 'string' && typeof vcRaw !== 'number') throw new VendorCodeError(400, 'invalid_input', '先方品番は文字で入れてください', 'vendor_code');
  if (qRaw != null && typeof qRaw !== 'string' && typeof qRaw !== 'number') throw new VendorCodeError(400, 'invalid_input', '入数は数で入れてください', 'qty_per_unit');
  const vendorCode = trimS(vcRaw);
  const qText = qRaw == null ? '' : trimS(half(qRaw));
  if (!vendorCode && !qText) return null;
  if (!vendorCode) throw new VendorCodeError(400, 'invalid_input', '入数だけは入れられません (先に先方品番を入れてください)', 'vendor_code');
  if (CTRL_RE.test(vendorCode)) throw new VendorCodeError(400, 'invalid_input', '先方品番に改行などの使えない文字があります', 'vendor_code');
  if (vendorCode.length > VENDOR_CODE_MAX) throw new VendorCodeError(400, 'invalid_input', `先方品番が長すぎます (${VENDOR_CODE_MAX}文字まで)`, 'vendor_code');
  const qtyPerUnit = qtyPerUnitOf(qText);
  if (qtyPerUnit === undefined) throw new VendorCodeError(400, 'invalid_input', `${QPU_ERR}: ${qRaw}`, 'qty_per_unit');
  return { vendorCode, qtyPerUnit };
}

/**
 * 対応表の登録・紐づけで使う商品の確かめ (仕入先一致の決まり): 商品管理リスト (PML) にあればその行の仕入先、無ければ新商品 (Company DB) の代表の仕入先。
 * @returns {Promise<{ prodSup: string, error: string|null, newSku: object|null }>} error = 断る文 (400)
 */
export async function productForVendorMap(productCode, supplier, notFoundHint) {
  const key = normProductCode(productCode);
  const pmlRow = loadPml().rows.find((r) => normProductCode(r['商品コード']) === key);
  if (pmlRow) {
    const prodSup = normSupplierCode(pmlRow['仕入先']);
    return { prodSup, error: prodSup ? null : `この商品は商品マスタで仕入先が未設定です: ${productCode} (先に商品マスタ側を直してください)`, newSku: null };
  }
  const nc = checkNewSku(await readNewSkusForRequest(), key, supplier);
  if (nc.error) return { prodSup: '', error: nc.error, newSku: null };
  if (nc.sku) return { prodSup: nc.sku.supplier, error: null, newSku: nc.sku };
  return { prodSup: '', error: `商品マスタに無い商品コードです: ${productCode} (${notFoundHint})`, newSku: null };
}

const rowOf = (db, supplier, key) => db.prepare('SELECT supplier_code, product_key, product_code, vendor_code, qty_per_unit, updated_at FROM po_vendor_code_map WHERE supplier_code=? AND product_key=?').get(supplier, key) || null;
const viaOf = (via) => {
  const v = String(via || '');
  if (!Object.hasOwn(VENDOR_VIA, v)) throw new Error(`書いた画面 (via) が分からない: ${v}`);
  return v;
};
/** 同じ先方番号がほかの商品にも付いているか (入荷予定の変換で曖昧になる = 警告だけ・止めない) */
function dupWarning(db, supplier, key, vendorCode) {
  const dupOf = db.prepare('SELECT product_code, vendor_code FROM po_vendor_code_map WHERE supplier_code=?').all(supplier)
    .filter((m) => normVendorCode(m.vendor_code) === normVendorCode(vendorCode) && normProductCode(m.product_code) !== key)
    .map((m) => m.product_code);
  return dupOf.length ? `この先方番号は商品 ${dupOf.join(', ')} にも登録されています (入荷予定変換で曖昧扱いになります)` : null;
}

/**
 * 1 件の追加・更新 (upsert)。確かめの順と応答は今までの発注アプリ (POST /api/vendor-map/entry) と同じ。
 * input = {
 *   supplierCode, productCode, vendorCode,
 *   qtyPerUnit: undefined = 変えない / null・'' = 空 (換算なし = 1) / 数か文字,
 *   baseUpdatedAt: undefined = 無条件 (発注アプリのスクリプト用) / null = まだ無いつもり / 文字 = 開いたときの版,
 *   prod: productForVendorMap の答え (呼び手が取引の前に読む・3 秒まで),
 *   actor, via, requestId?,
 * }
 * 戻り値 { status, body, row (書いた後の行 | null) }
 */
export function upsertVendorCode(input) {
  const b = input || {};
  const db = getDB();
  const supplier = normSupplierCode(b.supplierCode);
  const productCode = trimS(b.productCode);
  const vendorCode = trimS(b.vendorCode);
  const fail = (error, status = 400) => ({ status, body: { ok: false, error }, row: null });
  if (!supplier) return fail('仕入先コードが必要です');
  const sup = db.prepare('SELECT name FROM po_suppliers WHERE supplier_code=?').get(supplier);
  if (!sup) return fail(`仕入先が未登録です: ${supplier}`);
  if (!productCode) return fail('商品コードが必要です');
  if (productCode.length > 200) return fail('商品コードが長すぎます');
  if (!vendorCode) return fail('先方管理番号が必要です');
  if (/[\r\n]/.test(vendorCode)) return fail('先方管理番号に改行は使えません');
  if (vendorCode.length > VENDOR_CODE_MAX) return fail(`先方管理番号が長すぎます (${VENDOR_CODE_MAX}文字まで)`);
  const key = normProductCode(productCode);
  const prod = b.prod || { prodSup: '', error: `商品マスタに無い商品コードです: ${productCode}` };
  if (prod.error) return fail(prod.error);
  if (prod.prodSup !== supplier) return fail(`この商品の仕入先は ${prod.prodSup} です (${sup.name} の対応表には登録できません)`);
  // 入数 (任意): undefined=変更しない / 空欄=クリア (換算なし=1扱い) / 正の数=設定
  let qpu;
  if (b.qtyPerUnit !== undefined) {
    qpu = qtyPerUnitOf(b.qtyPerUnit);
    if (qpu === undefined) return fail(`${QPU_ERR}: ${b.qtyPerUnit}`);
  }
  const via = viaOf(b.via);
  let out = null;
  db.transaction(() => {
    const old = rowOf(db, supplier, key);
    if (b.baseUpdatedAt !== undefined) {
      // 楽観ロック: null=「新規のつもり」、文字列=「この版を見て編集した」
      if (b.baseUpdatedAt == null && old) {
        out = { status: 409, body: { ok: false, conflict: true, error: `この商品は既に登録されています (現在: ${old.vendor_code})。一覧のセルを編集してください`, current: { vendor_code: old.vendor_code, updated_at: old.updated_at } }, row: old };
        return;
      }
      if (b.baseUpdatedAt != null && (!old || old.updated_at !== b.baseUpdatedAt)) {
        out = { status: 409, body: { ok: false, conflict: true, error: old ? `他の画面で変更されています (現在: ${old.vendor_code})。最新を確認してください` : '他の画面で削除されています。最新を確認してください', current: old ? { vendor_code: old.vendor_code, updated_at: old.updated_at } : null }, row: old };
        return;
      }
    }
    db.prepare(`INSERT INTO po_vendor_code_map (supplier_code, product_key, product_code, vendor_code, updated_at) VALUES (?,?,?,?,?)
      ON CONFLICT(supplier_code, product_key) DO UPDATE SET product_code=excluded.product_code, vendor_code=excluded.vendor_code, updated_at=excluded.updated_at`)
      .run(supplier, key, productCode, vendorCode, vendorStamp());
    // 入数は指定されたときだけ更新 (未指定なら既存値を保持)
    if (qpu !== undefined) db.prepare('UPDATE po_vendor_code_map SET qty_per_unit=? WHERE supplier_code=? AND product_key=?').run(qpu, supplier, key);
    audit(db, { actorType: b.actorType || 'user', actor: b.actor, action: 'vendor_map_entry_upsert', resource: `supplier:${supplier}`, requestId: b.requestId ?? null,
      detail: { productCode, vendorCode, oldVendorCode: old ? old.vendor_code : null, qtyPerUnit: qpu === undefined ? '(変更なし)' : qpu,
        oldQtyPerUnit: old ? old.qty_per_unit : null, via, product_key: key, max_stamp: vendorStampMark() } });
    const warning = dupWarning(db, supplier, key, vendorCode);
    out = { status: 200, body: { ok: true, updated: !!old, oldVendorCode: old ? old.vendor_code : null, warning }, row: rowOf(db, supplier, key) };
  })();
  return out;
}

/**
 * 入数だけを更新する (発注アプリの入荷予定タブの変換結果からその場で)。応答は今までの POST /api/vendor-map/qty-per-unit と同じ。
 * 先に読んだ印は取らない (今の入荷予定の画面のまま・§2.4 の「残す」)
 */
export function setQtyPerUnit(input) {
  const b = input || {};
  const db = getDB();
  const supplier = normSupplierCode(b.supplierCode);
  const key = normProductCode(trimS(b.productCode));
  if (!supplier || !key) return { status: 400, body: { ok: false, error: '仕入先コードと商品コードが必要です' } };
  // 空欄 = 換算なし (NULL=1扱い)。それ以外は正の数のみ
  const qpu = qtyPerUnitOf(b.qtyPerUnit);
  if (qpu === undefined) return { status: 400, body: { ok: false, error: `${QPU_ERR}: ${b.qtyPerUnit}` } };
  const via = viaOf(b.via || 'po-inbound');
  let out = null;
  db.transaction(() => {
    const old = rowOf(db, supplier, key);
    if (!old) { out = { status: 404, body: { ok: false, error: 'この商品は対応表に未登録です (先に先方番号を登録してください)' } }; return; }
    db.prepare('UPDATE po_vendor_code_map SET qty_per_unit=?, updated_at=? WHERE supplier_code=? AND product_key=?').run(qpu, vendorStamp(), supplier, key);
    audit(db, { actorType: b.actorType || 'user', actor: b.actor, action: 'vendor_map_qty_per_unit', resource: `supplier:${supplier}`,
      detail: { productKey: key, old: old.qty_per_unit, new: qpu, via, product_key: key, max_stamp: vendorStampMark() } });
    out = { status: 200, body: { ok: true, qtyPerUnit: qpu == null ? 1 : qpu } };
  })();
  return out;
}

/**
 * 1 件削除。base = 開いたときの updated_at (発注アプリ: 空 = 無条件・今までどおり / マスタの入力: 呼び手が必須にする)。
 * 応答は今までの DELETE /api/vendor-map/entry と同じ。仕入先一致は見ない (今までどおり・ほかの仕入先の古い行も消せる)
 */
export function deleteVendorCode(input) {
  const b = input || {};
  const db = getDB();
  const supplier = normSupplierCode(b.supplierCode);
  const key = normProductCode(b.productCode);
  if (!supplier || !key) return { status: 400, body: { ok: false, error: '仕入先コードと商品コードが必要です' } };
  const via = viaOf(b.via);
  let out = null;
  db.transaction(() => {
    const old = rowOf(db, supplier, key);
    if (!old) { out = { status: 404, body: { ok: false, error: '対象の対応がありません (既に削除済みかもしれません)' } }; return; }
    if (b.base != null && b.base !== '' && old.updated_at !== b.base) {
      out = { status: 409, body: { ok: false, conflict: true, error: `他の画面で変更されています (現在: ${old.vendor_code})。最新を確認してください`, current: { product_code: old.product_code, vendor_code: old.vendor_code, updated_at: old.updated_at } } };
      return;
    }
    vendorStampMark();   // 消す前に印の始まりを決める (消した行の版を次の印より古くする)
    db.prepare('DELETE FROM po_vendor_code_map WHERE supplier_code=? AND product_key=?').run(supplier, key);
    audit(db, { actorType: b.actorType || 'user', actor: b.actor, action: 'vendor_map_entry_delete', resource: `supplier:${supplier}`, requestId: b.requestId ?? null,
      detail: { productCode: old.product_code, vendorCode: old.vendor_code, qtyPerUnit: old.qty_per_unit, via, product_key: key, deleted_updated_at: old.updated_at, max_stamp: vendorStampMark() } });
    out = { status: 200, body: { ok: true } };
  })();
  return out;
}

/**
 * この商品 (仕入先) の先方番号の記録が 1 つでもあるか (足す・直す・消す・入数・仮登録の昇格・CSV の changed_keys)。
 * 新商品の登録の知らせを届けるとき「だれかがもう触った」= 上書きしない (行が無くても = 足して消した、を戻さない・§2.4 の 6・10)
 */
export function vendorHistoryExists(db, supplier, key) {
  const marks = VENDOR_HISTORY_ACTIONS.map(() => '?').join(',');
  // detail_json は audit() が JSON.stringify で書く。古い記録の鍵の名前はまちまち (productCode / productKey) = 全部見る
  return !!db.prepare(`SELECT 1 FROM po_audit_log WHERE resource = ? AND action IN (${marks}) AND json_valid(detail_json) AND (
      json_extract(detail_json, '$.product_key') = ?
      OR lower(trim(json_extract(detail_json, '$.productCode'))) = ?
      OR lower(trim(json_extract(detail_json, '$.productKey'))) = ?
      OR EXISTS (SELECT 1 FROM json_each(detail_json, '$.changed_keys') j WHERE j.value = ?)) LIMIT 1`)
    .get(`supplier:${supplier}`, ...VENDOR_HISTORY_ACTIONS, key, key, key, key);
}

/** 仮登録 (出荷明細で見た未登録の番号) のうち、この商品か同じ番号のもの (結ぶ候補・自動では結ばない) */
export function pendingCandidates(db, key, vendorCodesBySupplier = new Map()) {
  const rows = db.prepare("SELECT id, supplier_code, vendor_code, vendor_code_norm, vendor_name, provisional_product_code, provisional_qty_per_unit, created_at FROM po_vendor_code_pending WHERE status='pending' ORDER BY created_at DESC LIMIT 500").all();
  return rows.filter((p) => {
    if (p.provisional_product_code && normProductCode(p.provisional_product_code) === key) return true;
    const mine = vendorCodesBySupplier.get(normSupplierCode(p.supplier_code));
    return !!mine && mine.has(p.vendor_code_norm);
  }).slice(0, 20).map((p) => ({ id: p.id, supplier_code: normSupplierCode(p.supplier_code), vendor_code: p.vendor_code, vendor_name: p.vendor_name || '',
    provisional_product_code: p.provisional_product_code || null, provisional_qty_per_unit: p.provisional_qty_per_unit ?? null, created_at: p.created_at }));
}

/**
 * マスタの入力の商品の画面に出す値 (正本を直接読む)。
 * { ok, rows: [{ supplier_code, supplier_name, vendor_code, qty_per_unit, updated_at }], pending: [...], suppliers: { code: { name, lead_days } } }
 * 読めない = { ok: false, error }
 */
export function readVendorCodesForProduct(code, { extraSuppliers = [] } = {}) {
  try {
    const db = getDB();
    const key = normProductCode(code);
    const rows = db.prepare(`SELECT m.supplier_code, m.vendor_code, m.qty_per_unit, m.updated_at, s.name AS supplier_name
      FROM po_vendor_code_map m LEFT JOIN po_suppliers s ON s.supplier_code = m.supplier_code WHERE m.product_key = ? ORDER BY m.supplier_code`).all(key)
      .map((r) => ({ supplier_code: normSupplierCode(r.supplier_code), supplier_name: r.supplier_name || '', vendor_code: r.vendor_code, qty_per_unit: r.qty_per_unit ?? null, updated_at: r.updated_at }));
    const bySup = new Map();
    for (const r of rows) {
      if (!bySup.has(r.supplier_code)) bySup.set(r.supplier_code, new Set());
      bySup.get(r.supplier_code).add(normVendorCode(r.vendor_code));
    }
    const suppliers = {};
    for (const sc of new Set([...rows.map((r) => r.supplier_code), ...extraSuppliers.map(normSupplierCode).filter(Boolean)])) {
      const s = db.prepare('SELECT name, lead_days FROM po_suppliers WHERE supplier_code=?').get(sc);
      suppliers[sc] = { name: s ? s.name || '' : '', lead_days: s && s.lead_days != null ? Number(s.lead_days) : null, known: !!s };
    }
    return { ok: true, rows, pending: pendingCandidates(db, key, bySup), suppliers };
  } catch (e) {
    console.error(`[po-vendor-codes] 先方番号対応表を読めない: ${e && e.message}`);
    return { ok: false, error: '発注アプリの先方番号対応表を読めません' };
  }
}

/**
 * 新商品の登録の前の確かめ (Company DB に書く前・書かない)。代表の仕入先が要る・発注アプリにある・行と記録がまだ無い。
 * 誤り = VendorCodeError (field = vendor_code)
 */
export function checkRegistrationVendor({ supplierCode, code }) {
  const db = getDB();
  const supplier = normSupplierCode(supplierCode);
  const key = normProductCode(code);
  if (!supplier) throw new VendorCodeError(400, 'invalid_input', '先方品番を入れるときは、代表の仕入先を選んでください', 'vendor_code');
  if (!db.prepare('SELECT 1 FROM po_suppliers WHERE supplier_code=?').get(supplier)) throw new VendorCodeError(400, 'invalid_input', `代表の仕入先 ${supplier} は発注アプリにありません (先方品番は入れられません)`, 'vendor_code');
  if (!key) return;
  const old = rowOf(db, supplier, key);
  if (old) throw new VendorCodeError(409, 'stale', `この商品コードには発注アプリの対応表にもう先方品番 (${old.vendor_code}) があります。何も保存していません`, 'vendor_code');
  // 行が無くても、この商品の先方番号の記録 (足して消した・CSV) がある = 届けるときに要確認になる = 登録の前に断る (Codex #⑤ R1 M2)
  if (vendorHistoryExists(db, supplier, key)) throw new VendorCodeError(409, 'stale', 'この商品コードの先方品番は発注アプリでもう扱われました (足して消した など)。登録の後に商品の画面で入れてください。何も保存していません', 'vendor_code');
}
