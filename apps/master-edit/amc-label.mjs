/**
 * amc-label.mjs — 商品の画面の「AMC 商品ラベルのバーコード印字」(Company DB構想 21 A1・中原さんの決定 4 = a: 足す・候補から選ぶ・手で打つときは理由)
 *
 * 置き場と正本は発注アプリ (Render の warehouse-mirror.db の po_barcode_labels + po_barcode_label_events)。表は動かさない。
 * 書くのは発注アプリの部品 (apps/purchase-orders/barcode-labels.js の writeLabelAudited = upsertLabel の version の楽観ロック・events + po_audit_log)。
 * 書ける人 = 発注の設定と同じ (order-settings.mjs の orderWriteGate = マスタの入力の名簿 かつ 発注アプリの利用権)。
 *
 * 🚨 バーコードの 3 つを混ぜない:
 *   ① 識別子 (JAN・FNSKU) = Company DB (core.external_ids)。ここでは候補として読むだけ (書かない)
 *   ② AMC 商品ラベルの印字 (= この欄・発注アプリ admin の「バーコード」タブ) = AMC が刷るパッケージのラベルに何のバーコードが載っているか + 依頼の状態
 *   ③ 入庫で弊社が BC シールを貼るか = 入荷アプリ (f_inbound_info.入庫時BCシール貼りフラグ)。読むだけ・自動で合わせない・食い違いは印だけ
 *
 * 値の候補 (Company DB):
 *   FNSKU = Amazon の出品 (core.listings の mall = amazon / amazon_us) に付いた FNSKU (core.external_ids の listing・amazon・fnsku・有効)。
 *     選べるのは出品の構成が「この商品 1 個だけ」のときだけ (まとめ売り・ほかの商品と組んだ出品・消えた出品の FNSKU をパッケージに刷らない)
 *   JAN = この商品の JAN (core.external_ids の product・jan・有効。単品の product : sku = 1 : 1)
 * 保存のときにサーバーで確かめ直す (Codex R3 M5): 選んだ候補の external_id_row (と出品) を送り、「今も有効・この商品に 1 対 1」を Company DB で読み直してから書く。
 *   値は画面の値ではなく Company DB の値を使う。手で打つ値は理由が要る (記録に残す)・Company DB でほかの商品の JAN / FNSKU なら断る。
 */
import { orderWriteGate } from './order-settings.mjs';
import { COMPANY_ID } from '../../lib/master-write.mjs';
import { getDB } from '../purchase-orders/db.js';
import { barcodeTargetOf, writeLabelAudited, revertLabelAudited, detectBarcodeType, BARCODE_TARGET_REASONS } from '../purchase-orders/barcode-labels.js';

export const AMC_STATUSES = Object.freeze(['printed', 'requested', 'unset', 'not_needed']);
/** 画面の言葉 (発注アプリの admin では printed = 「設定済み」と出る = 同じもの) */
export const AMC_STATUS_WORDS = Object.freeze({ printed: '印字済み', requested: '依頼中', unset: '未設定', not_needed: '不要', unregistered: '未登録' });
export const AMC_STATUS_HINTS = Object.freeze({
  printed: 'ラベルにバーコードが刷ってある', requested: 'AMC に印字を頼んだ', unset: 'これから頼む', not_needed: 'ラベルに場所が無い など',
});
export const MALL_WORDS = Object.freeze({ amazon: 'Amazon 日本', amazon_us: 'Amazon 米国' });
const LISTING_STATUS_WORDS = Object.freeze({ active: '出品中', inactive: '停止中', unknown: '状態不明', deleted: '消えた出品' });
export const MAX_REASON = 200;
const MAX_VALUE = 64;
const MAX_NOTE = 500;
const PICK_KINDS = new Set(['fnsku', 'jan', 'manual', 'manual_other', 'keep', 'none']);
const BODY_KEYS = new Set(['request_id', 'version', 'status', 'pick', 'note']);
const CTRL_RE = new RegExp(`[\\x00-\\x1f\\x7f${String.fromCharCode(0x2028, 0x2029)}]`);

export class AmcLabelError extends Error {
  constructor(status, reason, message, extra = {}) {
    super(message);
    this.name = 'AmcLabelError';
    this.status = status;
    this.reason = reason;
    this.extra = extra;
  }
}
const bad = (message, field) => new AmcLabelError(400, 'invalid_input', message, field ? { field } : {});
const str = (v) => (v == null ? '' : String(v));
const ROW_RE = /^[1-9]\d{0,17}$/;

/** 入庫で BC シールを貼るか (入荷アプリの値) → 要る (true) / 要らない (false) / 分からない (null) */
export function sealNeeded(v) {
  const t = str(v).trim();
  if (!t || /^[－\-ー―]$/.test(t)) return null;
  if (/不要|無し|なし/.test(t)) return false;
  if (/必要|要|貼/.test(t)) return true;
  return null;
}
/** 食い違いの印 (自動では合わせない)。印字済みなのに入庫で BC シールを貼る = 二重に貼る / 印字の値を確かめる */
export function inboundMismatch(status, inbound) {
  if (!inbound || inbound.needed !== true) return null;
  if (status !== 'printed') return null;
  return `入荷アプリでは「入庫で BC シールを貼る」(${inbound.bc_seal}) になっています。ラベルに印字済みなら、入荷アプリの値も確かめてください (自動では直しません)`;
}

/** 入荷アプリの値 (読むだけ)。表が無い・行が無い = null */
export function readInboundSeal(code) {
  try {
    const db = getDB();
    if (!db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'f_inbound_info'").get()) return null;
    const r = db.prepare('SELECT 入庫時BCシール貼りフラグ AS bc_seal FROM f_inbound_info WHERE code_key = ?').get(str(code).trim().toLowerCase());
    if (!r || r.bc_seal == null || !String(r.bc_seal).trim()) return null;
    return { bc_seal: String(r.bc_seal).trim(), needed: sealNeeded(r.bc_seal) };
  } catch (e) {
    console.error(`[master-edit] 入荷アプリの BC シールの値を読めない: ${e && e.message}`);
    return null;
  }
}

const marketOf = (shop) => { const t = str(shop); const i = t.indexOf('@'); return i >= 0 ? t.slice(i + 1) : ''; };
const FNSKU_SQL = `
  select e.external_id_row::text as row, e.external_value as value, l.listing_id::text as listing_id, l.listing_code, l.mall, l.shop_code, l.status,
         lc.qty::int as qty, (select count(*)::int from core.listing_components x where x.listing_id = l.listing_id) as n_comp
    from core.listing_components lc
    join core.listings l on l.company_id = lc.company_id and l.listing_id = lc.listing_id and l.mall in ('amazon', 'amazon_us')
    join core.external_ids e on e.company_id = l.company_id and e.entity_type = 'listing' and e.entity_id = l.listing_id
                            and e.system = 'amazon' and e.id_kind = 'fnsku' and e.valid_to is null
   where lc.company_id = $1 and lc.sku_id = $2::bigint`;
const JAN_SQL = `
  select e.external_id_row::text as row, e.external_value as value from core.external_ids e
   where e.company_id = $1 and e.entity_type = 'product' and e.entity_id = $2::bigint and e.system = 'jan' and e.id_kind = 'jan' and e.valid_to is null`;

/** 出品の FNSKU の行 → 候補 (選べない理由つき) */
function fnskuCand(r) {
  const why = r.status === 'deleted' ? '出品が消えた'
    : r.n_comp > 1 ? 'ほかの商品と組んだ出品' : r.qty !== 1 ? `まとめ売り (${r.qty} 個)` : null;
  return {
    kind: 'fnsku', row: r.row, listing_id: r.listing_id, value: r.value, type: detectBarcodeType(r.value),
    mall: r.mall, mall_words: MALL_WORDS[r.mall] || r.mall, market: marketOf(r.shop_code), listing_code: r.listing_code,
    listing_status: r.status, listing_status_words: LISTING_STATUS_WORDS[r.status] || r.status, why,
  };
}

/**
 * 値の候補 (Company DB を読むだけ)。{ ok: true, candidates (選べる), excluded (選べない FNSKU と理由) } / { ok: false, error }
 * sku = { sku_id, product_id }
 */
export async function readAmcCandidates(db, sku) {
  try {
    const fn = (await db.query(`${FNSKU_SQL} order by l.mall, l.listing_norm, e.external_norm`, [COMPANY_ID, sku.sku_id])).rows.map(fnskuCand);
    const jan = sku.product_id ? (await db.query(`${JAN_SQL} order by e.external_value`, [COMPANY_ID, sku.product_id])).rows
      .map((r) => ({ kind: 'jan', row: r.row, value: r.value, type: detectBarcodeType(r.value), why: null })) : [];
    return { ok: true, candidates: [...fn.filter((c) => !c.why), ...jan], excluded: fn.filter((c) => c.why) };
  } catch (e) {
    console.error(`[master-edit] AMC ラベルの候補を読めない: ${e && e.message}`);
    return { ok: false, error: e && e.code === '42501' ? '画面のロールに出品・JAN を読む権限が無い' : 'Company DB の候補 (FNSKU・JAN) を読めません' };
  }
}

/**
 * 画面に出すもの。cur = 商品の画面の cur (code・sku_kind・sku_id・product_id)。db = 読むだけの接続 (null = 候補なし)
 * { canSee, canWrite, why, kind, target, reason, reasonWords, label, vendorCode, cands, inbound, mismatch, error }
 */
export async function amcLabelScreen(req, editor, db, cur) {
  const gate = orderWriteGate(req, editor);
  if (gate.reason === 'no_po_access') return { canSee: false, canWrite: false, why: gate.message.replace('発注の設定', 'AMC ラベルの印字') };
  const base = { canSee: true, canWrite: gate.ok, why: gate.ok ? '' : gate.message };
  if (cur.sku_kind !== 'single') return { ...base, canWrite: false, target: false, reason: 'set', reasonWords: BARCODE_TARGET_REASONS.set };
  let t;
  try { t = barcodeTargetOf(cur.code); } catch (e) {
    console.error(`[master-edit] AMC ラベルの対象を読めない: ${e && e.message}`);
    return { ...base, canWrite: false, error: '発注アプリの AMC ラベルの表を読めません' };
  }
  if (!t.target) return { ...base, canWrite: false, target: false, reason: t.reason, reasonWords: BARCODE_TARGET_REASONS[t.reason] || t.reason, label: t.label };
  const cands = db ? await readAmcCandidates(db, cur) : { ok: false, error: 'Company DB につながらないので候補を出せません' };
  const inbound = readInboundSeal(cur.code);
  const label = t.label;
  return {
    ...base, target: true, label, vendorCode: t.vendorCode, cands, inbound,
    mismatch: label ? inboundMismatch(label.status, inbound) : null,
  };
}

/** 本文 → 確かめた形 (DB を読まない)。誤り = AmcLabelError 400 */
export function amcInputOf(raw) {
  if (raw == null || typeof raw !== 'object' || Array.isArray(raw)) throw bad('本文の形が違う');
  for (const k of Object.keys(raw)) if (!BODY_KEYS.has(k)) throw bad(`「${k}」は AMC ラベルの項目ではありません`);
  const status = str(raw.status);
  if (!AMC_STATUSES.includes(status)) throw bad('状態 (印字済み / 依頼中 / 未設定 / 不要) を選んでください', 'status');
  let version = null;
  if (raw.version != null) {
    if (!Number.isSafeInteger(raw.version) || raw.version < 1) throw bad('版 (version) が違う。画面を開き直してください');
    version = raw.version;
  }
  const note = str(raw.note).trim();
  if (note.length > MAX_NOTE) throw bad(`メモは ${MAX_NOTE} 文字まで`, 'note');
  if (CTRL_RE.test(note)) throw bad('メモに改行や制御文字は入れられません', 'note');
  const p = raw.pick;
  if (p == null || typeof p !== 'object' || Array.isArray(p) || !PICK_KINDS.has(p.kind)) throw bad('バーコードの値の選び方 (候補・手で打つ・今のまま・空) が分からない', 'value');
  const pick = { kind: p.kind };
  if (p.kind === 'fnsku' || p.kind === 'jan') {
    if (!ROW_RE.test(str(p.row))) throw bad('選んだ候補の番号が違う。画面を開き直してください', 'value');
    pick.row = str(p.row);
    // 候補の値はいつも送らせて比べる (Codex #1701 R1 L1)
    pick.value = str(p.value).trim();
    if (!pick.value) throw bad('選んだ候補の値がありません。画面を開き直してください', 'value');
    if (p.kind === 'fnsku') {
      if (!ROW_RE.test(str(p.listing_id))) throw bad('選んだ出品の番号が違う。画面を開き直してください', 'value');
      pick.listing_id = str(p.listing_id);
    }
  } else if (p.kind === 'manual' || p.kind === 'manual_other') {
    let value = str(p.value).trim();
    if (!value) throw bad('手で打つ値が空です', 'manual_value');
    if (value.length > MAX_VALUE) throw bad(`バーコードの値は ${MAX_VALUE} 文字まで`, 'manual_value');
    if (CTRL_RE.test(value) || /\s/.test(value)) throw bad('バーコードの値に空白や改行は入れられません', 'manual_value');
    // 形 (Codex #1701 R1 M1): ふつうの手入力 = FNSKU (X0 + 英数 8) か JAN-8 / JAN-13 (チェック数字まで)。ほかの形は「例外の形」を明示で選ぶ
    const form = barcodeFormOf(value);
    if (p.kind === 'manual') {
      if (form === 'fnsku') value = value.toUpperCase();
      else if (form !== 'jan') {
        throw bad(looksLikeJan(value) ? `${value} は JAN のチェック数字が合いません (打ち間違い?)` : `${value} は FNSKU (X0 で始まる 10 文字) でも JAN (8 桁・13 桁) でもありません。ほかの形なら「例外の形」に印を付けて理由を書いてください`, 'manual_value');
      }
    } else if (form || looksLikeJan(value) || /^X0/i.test(value)) {
      throw bad(form ? `${value} は ${form === 'jan' ? 'JAN' : 'FNSKU'} の形です。「例外の形」の印を外して、ふつうに手で打ってください` : `${value} は JAN / FNSKU に似ていますが形が合いません (打ち間違い?)。例外の形にはできません`, 'manual_value');
    }
    const reason = str(p.reason).trim();
    if (!reason) throw bad('手で打つときは理由が要ります (候補に無いわけ)', 'manual_reason');
    if (reason.length > MAX_REASON) throw bad(`理由は ${MAX_REASON} 文字まで`, 'manual_reason');
    if (CTRL_RE.test(reason)) throw bad('理由に改行や制御文字は入れられません', 'manual_reason');
    pick.value = value; pick.reason = reason;
  }
  const requestId = raw.request_id == null ? null : str(raw.request_id);
  if (requestId != null && !/^[0-9a-f-]{8,64}$/i.test(requestId)) throw bad('request_id の形が違う');
  return { status, version, note, pick, requestId };
}

/** JAN のチェック数字 (8 桁・13 桁。右から 3・1・3… の重み) */
export function janCheckOk(v) {
  const t = str(v);
  if (!/^\d{8}$/.test(t) && !/^\d{13}$/.test(t)) return false;
  const d = [...t].map(Number);
  const check = d.pop();
  let sum = 0;
  for (let i = d.length - 1, w = 3; i >= 0; i--, w = w === 3 ? 1 : 3) sum += d[i] * w;
  return (10 - (sum % 10)) % 10 === check;
}
const looksLikeJan = (v) => /^\d{8}$/.test(str(v)) || /^\d{13}$/.test(str(v));
/** 手入力の形: 'fnsku' (X0 + 英数 8・大文字小文字は問わない) / 'jan' (チェック数字まで合う 8・13 桁) / null */
export function barcodeFormOf(v) {
  const t = str(v).trim();
  if (/^X0[0-9A-Z]{8}$/i.test(t)) return 'fnsku';
  if (janCheckOk(t)) return 'jan';
  return null;
}

/**
 * 値の持ち主 (Company DB)。この商品の JAN / この商品 1 個だけの出品の FNSKU = mine。
 * { hits (有効な JAN・FNSKU の行), other (ほかの商品のもの・無ければ null) }。Company DB に無い値 = hits 空 (不明 = 通す)
 */
async function ownerOf(db, sku, value) {
  const hits = (await db.query(`
    select e.system, e.external_value,
           case when e.entity_type = 'product' then k.code
                else (select string_agg(k2.code, '・' order by k2.code_norm) from core.listing_components c2 join core.skus k2 on k2.sku_id = c2.sku_id where c2.listing_id = e.entity_id) end as owner,
           case when e.entity_type = 'product' then (k.sku_id = $3::bigint)
                else (exists (select 1 from core.listing_components c where c.listing_id = e.entity_id and c.sku_id = $3::bigint and c.qty = 1)
                      and (select count(*) from core.listing_components c where c.listing_id = e.entity_id) = 1) end as mine
      from core.external_ids e
      left join core.skus k on e.entity_type = 'product' and k.company_id = e.company_id and k.product_id = e.entity_id and k.sku_kind = 'single'
     where e.company_id = $1 and e.valid_to is null and e.external_norm = core.norm_code($2)
       and ((e.system = 'jan' and e.id_kind = 'jan' and e.entity_type = 'product') or (e.system = 'amazon' and e.id_kind = 'fnsku' and e.entity_type = 'listing'))`,
  [COMPANY_ID, value, sku.sku_id])).rows;
  return { hits, other: hits.find((h) => !h.mine) || null };
}
const otherWords = (value, other) => `${value} は Company DB では${other.owner ? `ほかの商品 (${other.owner}) の` : 'ほかの'} ${other.system === 'jan' ? 'JAN' : 'FNSKU'} です (この商品の 1 個だけの出品・JAN ではない)`;

/**
 * 保存の前に Company DB で確かめ直す (Codex R3 M5)。sku = { code, sku_id, product_id }。
 * 戻り値 = { value (書く値・候補は Company DB の値)・audit (記録に残す出どころ) }。誤り = AmcLabelError 409
 *   fnsku: その行が今も有効・その出品に付いている・出品の構成がこの商品 1 個だけ・消えた出品でない・値が画面と同じ
 *   jan:   その行が今も有効・この商品 (product) の JAN・値が画面と同じ
 *   manual / manual_other / keep: Company DB でほかの商品の JAN / FNSKU なら断る (この商品のもの・Company DB に無い値 = 通す)。
 *     keep = 発注アプリの今の行の値 (current) を照らす (Codex #1701 R1 H1: 状態・メモだけの直しでも)
 */
export async function verifyAmcPick(db, sku, pick, { current = null } = {}) {
  if (pick.kind === 'fnsku') {
    const r = (await db.query(`${FNSKU_SQL} and e.external_id_row = $3::bigint and l.listing_id = $4::bigint`, [COMPANY_ID, sku.sku_id, pick.row, pick.listing_id])).rows[0];
    if (!r) throw new AmcLabelError(409, 'candidate_gone', '選んだ FNSKU は、もうこの商品の出品に付いていません (外された・付け替えられた)。画面を開き直して選び直してください', { field: 'value' });
    const c = fnskuCand(r);
    if (c.why) throw new AmcLabelError(409, 'candidate_gone', `選んだ FNSKU は選べなくなりました (${c.why})。画面を開き直して選び直してください`, { field: 'value' });
    if (pick.value !== c.value) throw new AmcLabelError(409, 'candidate_changed', `選んだ FNSKU の値が変わりました (今は ${c.value})。画面を開き直して選び直してください`, { field: 'value' });
    return { value: c.value, audit: { kind: 'fnsku', external_id_row: c.row, listing_id: c.listing_id, listing_code: c.listing_code, mall: c.mall, market: c.market, value: c.value } };
  }
  if (pick.kind === 'jan') {
    const r = sku.product_id ? (await db.query(`${JAN_SQL} and e.external_id_row = $3::bigint`, [COMPANY_ID, sku.product_id, pick.row])).rows[0] : null;
    if (!r) throw new AmcLabelError(409, 'candidate_gone', '選んだ JAN は、もうこの商品の JAN ではありません (外された・付け替えられた)。画面を開き直して選び直してください', { field: 'value' });
    if (pick.value !== r.value) throw new AmcLabelError(409, 'candidate_changed', `選んだ JAN の値が変わりました (今は ${r.value})。画面を開き直して選び直してください`, { field: 'value' });
    return { value: r.value, audit: { kind: 'jan', external_id_row: r.row, value: r.value } };
  }
  if (pick.kind === 'manual' || pick.kind === 'manual_other') {
    const { hits, other } = await ownerOf(db, sku, pick.value);
    if (other) throw new AmcLabelError(409, 'value_of_other', `${otherWords(pick.value, other)}。パッケージに刷る値を確かめてください`, { field: 'manual_value' });
    return { value: pick.value, audit: { kind: pick.kind, value: pick.value, reason: pick.reason, type: detectBarcodeType(pick.value), known_in_company_db: hits.length > 0 } };
  }
  if (pick.kind === 'keep') {
    const value = current ? current.barcodeValue || '' : '';
    if (value) {
      const { hits, other } = await ownerOf(db, sku, value);
      if (other) throw new AmcLabelError(409, 'value_of_other', `今の値の ${otherWords(value, other)}。「今の値のまま」にはできません。候補から選ぶか、手で打ち直してください`, { field: 'value' });
      return { value, audit: { kind: 'keep', value, known_in_company_db: hits.length > 0 } };
    }
    return { value, audit: { kind: 'keep', value: '' } };
  }
  return { value: '', audit: { kind: 'none' } };
}

/**
 * 書いた後の読み直し (Codex #1701 R1 H2)。確かめた時から Company DB が変わっていた (FNSKU が外された・出品の構成・JAN / 値の持ち主) = 理由の文 / 変わっていない = null
 */
export async function recheckAmcPick(db, sku, pick, verified) {
  if (pick.kind === 'none' || !verified.value) return null;
  try {
    if (pick.kind === 'fnsku' || pick.kind === 'jan') await verifyAmcPick(db, sku, { ...pick, value: verified.value });
    else {
      const { other } = await ownerOf(db, sku, verified.value);
      if (other) return otherWords(verified.value, other);
    }
    return null;
  } catch (e) {
    if (e instanceof AmcLabelError) return e.message;
    throw e;
  }
}

/** 試験だけ: 確かめた後・書く前に差し込む (その間に Company DB を変える) */
let beforeWrite = null;
export function __setAmcBeforeWrite(fn) { beforeWrite = fn || null; }

/**
 * 保存の全部 (router から): ① 確かめ (keep = 発注アプリの今の行の値も) → ② 発注アプリに書く → ③ Company DB を読み直す。
 *   ③ で変わっていた = ② を戻す (前の行へ・前が無かった = 未設定 + 要確認のメモ。依頼中 / 印字済みのまま残さない)・po_audit_log に残す・409 recheck_failed。
 *   2 つの DB をまたぐ取引は作らない。受け入れた残り = ① と ② の間に変わり、③ までに元へ戻る (極めてまれ) 場合
 * 戻り値 = { label, pick (記録の出どころ) }
 */
export async function saveAmcLabel(db, { sku, input, actor }) {
  let current = null;
  if (input.pick.kind === 'keep') {
    const t = barcodeTargetOf(sku.code);
    if (!t.label) throw new AmcLabelError(409, 'stale', 'まだ発注アプリに行がありません (今の値のままにはできません)。画面を開き直してください');
    if (input.version == null || t.label.version !== input.version) throw new AmcLabelError(409, 'stale', '他の画面で先に更新されています。画面を開き直してから入れ直してください', { current: t.label });
    current = t.label;
  }
  const verified = await verifyAmcPick(db, sku, input.pick, { current });
  if (beforeWrite) await beforeWrite({ sku, input, verified });
  const w = writeLabelAudited({
    productCode: sku.code, status: input.status, barcodeValue: verified.value, note: input.note, version: input.version,
    actor, via: 'master-edit:sku', requestId: input.requestId, pick: verified.audit,
  });
  const why = await recheckAmcPick(db, sku, input.pick, verified);
  if (!why) return { label: w.label, pick: verified.audit };
  const rv = revertLabelAudited({ productCode: sku.code, version: w.label.version, before: w.before, actor, via: 'master-edit:sku', requestId: input.requestId, why, detail: verified.audit });
  throw new AmcLabelError(409, 'recheck_failed', rv.reverted
    ? `保存した直後に Company DB の値が変わっていたので、保存を戻しました (要確認): ${why}。画面を開き直して選び直してください`
    : `保存した直後に Company DB の値が変わっていましたが、その間にほかの人が直したので戻せませんでした (要確認・発注アプリの値を確かめてください): ${why}`,
  { field: 'value', reverted: rv.reverted, current: rv.label });
}

/** 誤り → 応答の本文 */
export function amcErrorBody(e) {
  return { ok: false, error: e.message, reason: e.reason || 'error', ...(e.extra || {}), ...(e.current !== undefined ? { current: e.current } : {}), ...(e.target_reason ? { target_reason: e.target_reason } : {}) };
}
