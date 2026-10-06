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
import { evaluateBaseline } from './baseline.mjs';

export const NE_FORMAT = 'mc-ne-v1';
/** NE の取扱区分で知っている語 (2026-09-26 の実データ。これ以外は invalid = 照合しない。今のロードの mapHandling は知らない語も discontinued にする) */
export const HANDLING_WORDS = Object.freeze(['取扱中', '取扱中止', 'ﾒｰｶｰ取扱中止']);
export const PROBLEM_TYPES = Object.freeze(['value', 'cost', 'primary_supplier', 'components', 'only_in_ne', 'only_in_cdb', 'kind', 'parent']);
/** 判明した差 (案件の集約で breach)。blocked / incomparable は保持、match だけで回復 (C2 v5-4) */
export const KNOWN_DIFF = Object.freeze(['rule', 'rule_lag', 'lag', 'ne_no_value', 'held_by_load', 'load_mismatch', 'unexplained', 'arrival_unknown', 'not_delivered',
  'not_delivered_by_load', 'direction_unknown', 'spec_undecided', 'reg_stale', 'reg_kind_mismatch']);
const KNOWN = new Set(KNOWN_DIFF);
/** 判断の一覧に載せる分類 */
const DECISION_CLASSES = new Set(['rule', 'rule_lag', 'held_by_load', 'spec_undecided', 'ne_no_value', 'reg_stale', 'reg_kind_mismatch']);
/**
 * ポータルで登録した新商品 (ops.master_registrations の origin = new_entry。0052) の、NE にまだ無くて当然の状態。
 *   draft = 登録した (NE 登録の CSV を取り込んだと申告する前) / ne_pending = 取り込んだと申告した (翌朝の照合の確かめ待ち・確かめが failed でもこのまま)
 *   この状態で NE に無い SKU は「NE 登録待ち」= 差にしない (判断の候補・W13 の件数に入れない。照合の報告に件数と一覧だけ残す)。
 *   最後に状態が進んでから REG_STALE_DAYS 日たっても NE に出てこない = reg_stale (放置を見落とさない = 判断の一覧と W13 に出す)
 * cancelled (登録をやめた) = NE に無くて当然 = 対象外 (reg_cancelled)。ne_confirmed 以降 (NE で一度確かめた)・quarantined (NE で見つけた = origin ne_discovered)・
 *   backfill の行・状態の行が無い SKU は今までどおり (NE から消えた = 差)
 * NE に現れた後の値の差は普通の案件のまま (新規登録の CSV の確かめ = 0053 の partial と二重に数えない = 報告に段階として残すだけ)。
 *   区分 (単品・セット) が違う = reg_kind_mismatch (重大な登録の不一致 = 判断の一覧と W13・朝の要約の先頭の ⚠️)
 */
export const REG_WAIT_STATES = Object.freeze(['draft', 'ne_pending']);
export const REG_STALE_DAYS = 14;
/** 承認の指紋の「意味の版」(理由の種類ごとに手で上げる。C2 v4 §6) */
export const SEMANTIC_VERSIONS = Object.freeze({ tax_fallback: 1, tax_unresolved: 1, exception_cost: 1, exception_tax_manual: 1, set_name_blank: 1, set_price_from_goods: 1,
  set_tax_from_components: 1, not_in_latest_fetch: 1, 'load_rule:name_blank_to_code': 1, manual: 1, held_by_load: 1, spec_undecided: 1, ne_no_value: 1, none: 1, parent_manual: 1,
  company_owned: 1, cdb_name_is_code: 1, name_like_code: 1, cdb_zero_yen: 1, reg_stale: 1, reg_kind_mismatch: 1 });
/** 作り直しの理由の列 → 照合の列 (company_owned の handling・primary_supplier は同じ名前。sales_class・shipping は照合しない列) */
const BUILD_COL = { cost: 'cost', tax_rate: 'tax_rate', name: 'name', price: 'standard_price_jpy', handling: 'handling', primary_supplier: 'primary_supplier' };
/** ④a が持ち主 C の値を m_products に写す、照合の列 (これらの差は「C → NE」の予定の差。compareNe の companyCopied) */
const COMPANY_COPIED = new Set(['name', 'handling', 'tax_rate', 'standard_price_jpy', 'cost', 'primary_supplier']);
/** 理由の種類ごとに承認の指紋へ入れる項目 (raw_synced_at など毎朝変わるものは入れない。C2 v4 §6 / Codex C2-R0 M7) */
const REASON_FIELDS = { tax_fallback: ['source', 'value'], exception_cost: ['value'], exception_tax_manual: ['value'], set_price_from_goods: ['value'],
  set_tax_from_components: ['value', 'category'], set_name_blank: [], not_in_latest_fetch: [], tax_unresolved: [], 'load_rule:name_blank_to_code': [], parent_manual: [],
  // 作り直しが持ち主が C の列に Company DB の値を入れた (④a。rebuild-m-products.js / master-publish.js の mergeReasons)。
  //   持ち主のキー・変換の前の C の値・古い表に入れた値・原価の元の出どころ (世代の番号は毎朝変わるので指紋に入れない)
  company_owned: ['owner_key', 'cdb_value', 'value', 'cdb_cost_source'],
  // 社内の名前が商品コードのまま (夜間ロードが NE の空の名前の代わりにコードを入れた名残。engine の `name || code`)。中身は無い (コードは案件の鍵にある)
  cdb_name_is_code: [],
  // 社内の名前がコードに見える (広い正規化で一致) が、夜間ロードの代わりの値だった証拠が無い (人が決めた名前かもしれない = 持ち主を逆転させない中立の理由)
  name_like_code: [],
  // 社内の売価・原価が 0 円 (0027 の契約 = 0 は実値・null が未取得。原価の override_zero も)。NE は 0 と未入力を区別できない・CSV は 0 を書かない = 人が決める
  cdb_zero_yen: [],
  // ポータルで登録した新商品が、最後に状態が進んでから REG_STALE_DAYS 日たっても NE に無い。登録の状態と、その状態になった日 (JST)。日数は毎朝変わるので入れない
  //   (状態が進む = 別の判断。下書きのまま放置を「差を残す」にしても、取り込んだと申告した後にまた出てこなければもう一度聞く)
  reg_stale: ['state', 'since'],
  // ポータルで登録した新商品 (NE 登録の前〜確かめ待ち) の区分が NE と違う。登録の状態 (区分の値は n・c にある)
  reg_kind_mismatch: ['state'] };
/** 売価・原価 (円)。NE の 0 (0.00) は未入力と区別できない = 値なし (no_value) / Company DB の 0 は実値 (0027。null だけが未取得) */
const YEN_COLS = new Set(['standard_price_jpy', 'cost']);
/**
 * 名前がその SKU の商品コードと同じ (照合の正規化で) = 名前ではない (コードの代わりの値)。
 * 🚨 NE に「名前 = コード」を入れる提案・承認・CSV を作らない (NE の名前を壊す。2026-10-05 の 383 セット)
 */
export const nameIsCode = (v, norm) => typeof v === 'string' && !!norm && normSku(v) === norm;
/**
 * 判断の一覧で「NE をこの値に」と提案してよい値か。空・(無い)・空の配列・売価/原価の 0・名前 = コード は提案しない (決める / 社内を直す)
 */
export function proposableValue(col, v, norm) {
  if (v === null || v === undefined || v === '(無い)' || v === '(ロードは触らない)' || isMarker(v)) return false;
  if (Array.isArray(v) && !v.length) return false;
  if (YEN_COLS.has(col) && Number(v) === 0) return false;
  if (col === 'name' && nameIsCode(v, norm)) return false;
  return true;
}

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
/**
 * 代表 (親) の NE の値の状態 (D3b 契約 v1 §1)。親なし (null) は**比べられる値** (raw 'value')。
 *   空でない値 = 自分自身なら親なし・他はその norm / 空 = 元の値 (_src) が空の文字列のときだけ親なし / それ以外 (記録なし・null) = 不明
 */
export function repState(raw, src, code) {
  const t = raw == null ? '' : String(raw).trim();
  if (t) return { raw: 'value', validity: 'ok', value: normSku(t) === normSku(code) ? null : normSku(t) };
  let v; try { v = src == null ? undefined : JSON.parse(src); } catch { v = undefined; }
  if (typeof v === 'string' && v.trim() === '') return { raw: 'value', validity: 'ok', value: null };
  return { raw: 'unknown', validity: 'invalid', value: null };
}
/** 材料の代表 (t_today / t_load): 他のコード = norm / 自分自身・明示の空 = null (親なし) / 不明 = PRESERVE (ロードは触らない) */
function repOfPlan(s) {
  const r = s.representativeCode;
  if (r && normSku(r) === normSku(s.code)) return null;
  if (r) return normSku(r);
  return s.representativeState === 'empty' ? null : PRESERVE;
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
/**
 * 判断の候補で選べる解決 (設計 10 §6.1.1 D1 契約 v3)。accept_difference = 差を残す (承認で案件を閉じる) / fix_ne・fix_cdb = 目標値つきで直す (目標に届くまで閉じない) /
 * fix_input = 作り直し・材料を直す / spec = 仕様を決める (どちらも自動では完了しない)
 */
export function resolutionsFor({ cls, reasonKind, incomparableNoValue = false, hasC = false }) {
  if (incomparableNoValue) return ['fix_ne'];
  if (cls === 'spec_undecided') return ['spec', 'accept_difference'];
  // 登録から NE に出てこない新商品 = 直すのはこの画面の外 (マスタの入力の「NE 登録の CSV」で NE に取り込む・やめるなら登録をやめる)。
  //   NE を直す (有無の CSV は無い)・社内を直す (SKU を消さない) は出さない
  if (cls === 'reg_stale') return ['spec', 'accept_difference'];
  // 区分違いの新商品 = NE を登録の区分に直す (NE の画面で。種類は CSV にしない) か、仕様を決める (登録が違ったら登録をやめる)。重大なので「差を残す」は出さない
  if (cls === 'reg_kind_mismatch') return ['fix_ne', 'spec'];
  if (cls === 'held_by_load') return ['fix_input', 'accept_difference'];
  if (reasonKind === 'parent_manual') return ['accept_difference', 'fix_ne', 'fix_cdb'];   // 人が決めた親子 (D3b)
  if (reasonKind === 'manual') return ['accept_difference', 'fix_cdb'];
  if (reasonKind === 'cdb_name_is_code') return ['fix_cdb', 'accept_difference'];   // 社内の名前がコードのまま = 社内 (ポータル) で本当の名前を入れる。NE を直すは選べない
  if (reasonKind === 'name_like_code') return ['accept_difference', 'spec'];         // 証拠の無いコードに見える名前 = 中立 (NE を直す・社内を NE に戻すの既定は出さない)
  if (reasonKind === 'cdb_zero_yen') return ['accept_difference', 'fix_cdb'];        // 社内 0 円 = 差を残すか社内を直す (NE を 0 にする CSV は作らない。社内を直すは値を入れて)
  if (cls === 'ne_no_value') return hasC ? ['fix_ne', 'accept_difference'] : ['accept_difference', 'spec'];
  return ['accept_difference', 'fix_ne'];
}
function reasonForPrint(r) {
  if (!r) return null;
  const fields = REASON_FIELDS[r.reason] || [];
  return { reason: r.reason, ...Object.fromEntries(fields.map((f) => [f, r[f] ?? null])) };
}

// ─────────── ポータルで登録した新商品の登録の状態 (0052) ───────────
const jstDateOfEpoch = (sec) => (Number.isFinite(sec) ? new Date(sec * 1000 + 9 * 3600000).toISOString().slice(0, 10) : null);
/**
 * NE 登録の CSV の段階 (0053 の品目の最後の状態 → 報告の段階)。
 *   before_issue = CSV を配る前 (品目なし・作っただけ・使わないにした) / issued = 配った (取り込んだ申告の前) / declared = 申告した (NE の完全な取得の確かめ待ち) /
 *   partial = NE にあるが中身が登録と違う / failed = 申告の後の完全な取得に無い (failed_reason not_in_ne = 取り込めていない) /
 *   rejected = 全部拒まれたと申告した (failed_reason rejected_all。登録は下書きのまま = 作り直す) / verified = 確かめ済み
 */
export const REG_STAGES = Object.freeze(['before_issue', 'issued', 'declared', 'partial', 'failed', 'rejected', 'verified']);
export function regStage(itemState) {
  switch (itemState) {
    case 'issued': return 'issued';
    case 'import_declared': return 'declared';
    case 'partial': return 'partial';
    case 'failed:not_in_ne': return 'failed';
    case 'failed:rejected_all': return 'rejected';
    case 'failed': case 'failed:': return 'failed';   // 理由が読めない failed = 取り込めていない側 (⚠️ を消さない)
    case 'verified': return 'verified';
    default: return 'before_issue';   // null (品目なし)・built・superseded
  }
}
/**
 * 登録の状態を読む (照合の読み取りの取引の中・watcher = ops の select)。origin = new_entry の行だけ (backfill・ne_discovered は今までどおり)。
 * 🚨 読めない = 「登録待ちは無い」と読まない (state = unreadable)。照合 ② は今までどおり (NE に無い = 差) に倒し、報告に「読めない」を残す
 * @returns {Promise<{ state: 'ok'|'not_applied'|'unreadable', byNorm?: Map<string, { state, since, created }>, reason?: string }>}
 */
export async function readRegistrations(db, companyId = 1) {
  await db.query('savepoint master_registrations_read');
  try {
    const exists = (await db.query("select to_regclass('ops.master_registrations') is not null as ok")).rows[0].ok;
    if (!exists) { await db.query('release savepoint master_registrations_read'); return { state: 'not_applied' }; }
    // NE 登録の CSV (0053) の、その SKU の最後の品目の状態 (段階を分けて報告に残す。0053 の前 = 品目なし)
    const hasItems = (await db.query("select to_regclass('ops.ne_reg_export_items') is not null as ok")).rows[0].ok;
    const rows = (await db.query(`select s.code_norm, s.sku_kind, r.state, extract(epoch from r.state_changed_at)::float8 as changed, extract(epoch from r.created_at)::float8 as created,
             ${hasItems ? `(select case when i.state = 'failed' then 'failed:' || coalesce(i.failed_reason, '') else i.state end
                from ops.ne_reg_export_items i where i.sku_id = r.sku_id order by i.item_id desc limit 1)` : 'null::text'} as item_state
        from ops.master_registrations r join core.skus s on s.sku_id = r.sku_id
       where r.company_id = $1 and r.origin = 'new_entry'`, [companyId])).rows;
    await db.query('release savepoint master_registrations_read');
    const byNorm = new Map();
    for (const r of rows) {
      const norm = normSku(r.code_norm); if (!norm) continue;
      byNorm.set(norm, { state: r.state, sku_kind: r.sku_kind, stage: regStage(r.item_state), since: jstDateOfEpoch(Number(r.changed)), created: jstDateOfEpoch(Number(r.created)) });
    }
    return { state: 'ok', byNorm };
  } catch (e) {
    try { await db.query('rollback to savepoint master_registrations_read'); } catch { /* */ }
    return { state: 'unreadable', reason: String(e && e.message).slice(0, 200) };
  }
}
/**
 * NE に無い SKU の登録の扱い。null = 今までどおり (登録の行が無い・new_entry でない・NE で一度確かめた・読めない)
 * @returns {null | { kind: 'waiting'|'stale'|'cancelled', state, since, created, days }}
 */
export function registrationAbsence(registrations, norm, asOfJst, staleDays = REG_STALE_DAYS) {
  if (!registrations || registrations.state !== 'ok') return null;
  const g = registrations.byNorm.get(norm);
  if (!g) return null;
  const ms = Date.parse(`${asOfJst}T00:00:00Z`) - Date.parse(`${g.since}T00:00:00Z`);
  const days = Number.isFinite(ms) ? Math.max(0, Math.round(ms / 86400000)) : null;
  if (g.state === 'cancelled') return { kind: 'cancelled', ...g, days };
  if (!REG_WAIT_STATES.includes(g.state)) return null;
  // 日付が読めない = 待ちの期限を決められない = 放置を隠さない側 (stale) に
  return { kind: days != null && days < staleDays ? 'waiting' : 'stale', ...g, days };
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
      const repSrc = has('raw_ne_products', '代表商品コード_src') ? ', 代表商品コード_src AS rep_src' : '';   // 0036 の日から (無い DB = 空の代表は不明)
      const products = pAt && hasSrc ? db.prepare(`SELECT 商品コード AS code, 商品名 AS name, 仕入先コード AS supplier, 取扱区分 AS handling,
        原価_src AS cost_src, 売価_src AS price_src, 消費税率_src AS tax_src, 代表商品コード AS rep${repSrc} FROM raw_ne_products WHERE synced_at = ?`).all(pAt) : [];
      const sets = sAt && hasSrc ? db.prepare(`SELECT セット商品コード AS parent, セット商品名 AS name, 商品コード AS child, セット販売価格_src AS price_src, 数量_src AS qty_src
        FROM raw_ne_set_products WHERE synced_at = ?`).all(sAt) : [];
      const setRowsTotal = db.prepare('SELECT COUNT(*) AS c FROM raw_ne_set_products').get().c;
      const build = db.prepare('SELECT * FROM m_products_builds ORDER BY published_at DESC LIMIT 1').get() ?? null;
      if (build) { try { build.reasons = JSON.parse(build.reasons || '[]'); } catch { build.reasons = null; } }
      // NE のコードの元の書き方 (③b-1b 契約 v3): 同じ読み取りの取引で、照合に使う取得の世代 (完了の印) の分だけ。集め終えた印が無い側は読まない (= 公開しない)
      const spellings = readSpellings(db, pAt, sAt);
      return { meta, products, sets, setRowsTotal, build, hasSrc, spellings };
    } finally { db.exec('COMMIT'); }
  } finally { db.close(); }
}
/** 書き方を集め終えた印の版 (apps/warehouse/db.js の NE_SPELLING_VERSION と同じ) */
export const NE_SPELLING_VERSION = 'sp1';
/**
 * 取得の世代の書き方を読む (readNeSide の読み取りの取引の中)。商品の世代から single・rep、セットの世代から set・child・set_rep。
 * 両方の側に、その世代の「集め終えた印」(知っている版) があるときだけ ok (片方でも無い = 元の書き方は公開しない。Codex ③b-1b-R1 H1)
 */
function readSpellings(db, pAt, sAt) {
  const tbl = (t) => !!db.prepare(`SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?`).get(t);
  if (!tbl('raw_ne_code_spellings') || !tbl('ne_code_spelling_marks')) return { ok: false, reason: 'no_table' };
  if (!pAt || !sAt) return { ok: false, reason: 'no_complete_mark' };
  const mark = (side, at) => db.prepare('SELECT version, rows FROM ne_code_spelling_marks WHERE side = ? AND synced_at = ?').get(side, at);
  const mp = mark('products', pAt), ms = mark('sets', sAt);
  if (!mp || !ms) return { ok: false, reason: 'not_collected' };
  if (mp.version !== NE_SPELLING_VERSION || ms.version !== NE_SPELLING_VERSION) return { ok: false, reason: 'unknown_version' };
  const rows = (at, kinds) => db.prepare(`SELECT kind, code_norm, spellings FROM raw_ne_code_spellings WHERE synced_at = ? AND kind IN (${kinds.map(() => '?').join(', ')})`).all(at, ...kinds);
  const all = [...rows(pAt, ['single', 'rep']), ...rows(sAt, ['set', 'child', 'set_rep'])];
  if (all.length !== Number(mp.rows) + Number(ms.rows)) return { ok: false, reason: 'rows_mismatch' };   // 印の件数と合わない = 消えた・足りない
  return { ok: true, rows: all };
}
const NE_CODE_RE = /^[A-Za-z0-9_-]{1,30}$/;
/**
 * 元の書き方を決める (③b-1b 契約 v3)。商品のコード (single・set・child) と代表の名札 (rep・set_rep) は別の名前空間。
 * norm (照合の正規化) ごとに書き方の集合を合わせ、1 つ = ok (その書き方) / 2 つ以上 = collided / 使えない文字・小文字が norm と合わない = invalid。
 * 衝突・使えないものも省かずに返す (今回の回に「分からない」と記録する)
 * @returns {{ ok: boolean, reason?: string, entries?: Array<{ code_norm, kind: 'product'|'rep', state: 'ok'|'collided'|'invalid', ne_code: string|null, spellings: string[] }> }}
 */
export function resolveNeCodes(sp) {
  if (!sp || !sp.ok) return { ok: false, reason: sp ? sp.reason : 'not_read' };
  const acc = { product: new Map(), rep: new Map() };
  for (const r of sp.rows) {
    const ns = r.kind === 'rep' || r.kind === 'set_rep' ? 'rep' : 'product';
    const norm = normSku(r.code_norm);
    if (!norm) continue;
    let list; try { list = JSON.parse(r.spellings); } catch { list = null; }
    if (!acc[ns].has(norm)) acc[ns].set(norm, new Set());
    if (!Array.isArray(list) || !list.length) { acc[ns].get(norm).add('\u0000broken'); continue; }   // 壊れた記録 = 使わない
    for (const s of list) acc[ns].get(norm).add(String(s));
  }
  const entries = [];
  for (const kind of ['product', 'rep']) {
    for (const [norm, set] of [...acc[kind]].sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))) {
      const spellings = [...set].filter((s) => s !== '\u0000broken').sort();
      let state, ne_code = null;
      if (set.has('\u0000broken') || set.size > 1) state = set.size > 1 && !set.has('\u0000broken') ? 'collided' : 'invalid';
      else { const s = spellings[0]; if (NE_CODE_RE.test(s) && s.toLowerCase() === norm) { state = 'ok'; ne_code = s; } else state = 'invalid'; }
      entries.push({ code_norm: norm, kind, state, ne_code, spellings });
    }
  }
  return { ok: true, entries };
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
/**
 * NE → 照合の形。🚨 正規化で同じになる別の表記 (x と ｘ など) は後勝ちで潰さず collided に入れる (照合はその SKU を保持 = 回復させない。Codex #1464 R4 High 1)。
 * セットの子どうしの衝突は親を collided に
 * @returns {{ m: Map, collided: Set }}
 */
function nModelOf(ne) {
  const m = new Map();
  const collided = new Set();
  // 衝突は、商品の表・セットの表の親の**全部の表記**で先に調べる (セットの表にあるコードの商品の行を捨てる前に。同じ表記が両方にあるのは正常。Codex #1464 R4 の確認 High)
  const spellings = new Map();
  for (const code of [...ne.products.map((r) => r.code), ...ne.sets.map((r) => r.parent)]) {
    const norm = normSku(code); if (!norm) continue;
    if (!spellings.has(norm)) spellings.set(norm, new Set());
    spellings.get(norm).add(code);
  }
  for (const [norm, s] of spellings) if (s.size > 1) collided.add(norm);
  const setNorms = new Set(ne.sets.map((r) => normSku(r.parent)).filter(Boolean));
  for (const r of ne.products) {
    const norm = normSku(r.code); if (!norm || setNorms.has(norm)) continue;   // セットの表にあるコードはセット (商品の表にもあるのは正常)
    if (m.has(norm)) { if (m.get(norm).code !== r.code) collided.add(norm); continue; }
    m.set(norm, { code: r.code, kind: 'single', cols: {
      name: textState(r.name, 'name'), handling: textState(r.handling, 'handling'), tax_rate: numState(r.tax_src, 'tax'),
      standard_price_jpy: numState(r.price_src, 'yen'), cost: numState(r.cost_src, 'yen'), primary_supplier: textState(r.supplier, 'supplier'),
      parent: repState(r.rep, r.rep_src ?? null, r.code) } });
  }
  for (const r of ne.sets) {
    const norm = normSku(r.parent); if (!norm) continue;
    if (!m.has(norm)) m.set(norm, { code: r.parent, kind: 'set', cols: { name: textState(r.name, 'name'), standard_price_jpy: numState(r.price_src, 'yen') }, children: new Map() });
    else if (m.get(norm).code !== r.parent) { collided.add(norm); continue; }
    const cn = normSku(r.child); if (!cn) continue;
    const ch = m.get(norm).children;
    if (ch.has(cn)) { if (ch.get(cn).code !== r.child) collided.add(norm); continue; }
    ch.set(cn, { code: r.child, st: numState(r.qty_src, 'qty') });
  }
  return { m, collided };
}
/**
 * plan → 材料の形 (ロードと同じ関数)。正規化で衝突した表記はロードと同じく先に来た表記だけを採用し (engine の seenNorm)、衝突した norm は collided に。
 * 代表の仕入先・構成も「採用した表記」の行だけ (engine は skuIdOf = 原文一致で引く = 負けた表記の行は飛ばす。Codex #1464 R4 High 1)
 * @returns {{ m: Map, collided: Set }}
 */
function tModelOf(plan) {
  const m = new Map();
  const collided = new Set();
  const accepted = new Map();   // norm → 採用した表記
  for (const s of plan.skus || []) { const norm = normSku(s.code); if (!norm) continue; if (!accepted.has(norm)) accepted.set(norm, s.code); else if (accepted.get(norm) !== s.code) collided.add(norm); }
  const isAccepted = (code) => { const norm = normSku(code); return !!norm && accepted.get(norm) === code; };
  const primary = new Map();
  for (const x of plan.primarySuppliers || []) { if (!isAccepted(x.skuCode)) continue; const norm = normSku(x.skuCode); if (!primary.has(norm)) primary.set(norm, normSku(x.supplierCode)); }
  const comps = new Map();
  const skipParents = new Set();   // engine が parentsWithSkip に入れる親 (子が無い・自分自身・重複・数量が不正) = 削除まで行かない
  for (const c of plan.setComponents || []) {
    if (!isAccepted(c.parentCode)) continue;   // 親の表記が負け = engine は no_parent で飛ばすだけ (採用した親の削除は止めない)
    const pn = normSku(c.parentCode), cn = normSku(c.childCode);
    if (!isAccepted(c.childCode) || !cn) { skipParents.add(pn); continue; }   // 子の表記が負け・空 = どの子の行か分からない (値には入れない)
    if (pn === cn || !(Number(c.qty) > 0)) skipParents.add(pn);              // 自分自身・数量が不正 = ロードは飛ばす。値 (材料の数量) は残す = 昨夜の適用は記録した保持状態と照らす
    if (!comps.has(pn)) comps.set(pn, new Map());
    if (comps.get(pn).has(cn)) { skipParents.add(pn); continue; }
    comps.get(pn).set(cn, Number(c.qty));
  }
  for (const s of plan.skus || []) {
    const norm = normSku(s.code); if (!norm || m.has(norm) || !isAccepted(s.code)) continue;
    const v = skuValuesForLoad(s);
    const cost = s.cost ? costForLoad(s.cost) : null;
    m.set(norm, { code: s.code, kind: s.kind, vals: { name: v.name, handling: v.handling, tax_rate: v.tax_rate, standard_price_jpy: v.standard_price_jpy,
      cost: cost ? cost.cost_jpy : PRESERVE, primary_supplier: primary.has(norm) ? primary.get(norm) : PRESERVE, kind: s.kind, exists: true,
      parent: s.kind === 'single' ? repOfPlan(s) : PRESERVE },
      children: comps.get(norm) || null });
  }
  return { m, collided, skipParents };
}
const cValue = (cdb, norm, col) => {
  const r = cdb.skuByNorm.get(norm);
  if (col === 'exists') return !!r;
  if (!r) return ABSENT;
  if (col === 'kind') return r.sku_kind;
  if (col === 'cost') { const c = cdb.costs.get(norm); return c ? Number(c.cost_jpy) : null; }
  if (col === 'primary_supplier') return [...(cdb.primary.get(norm) || [])].sort();
  if (col === 'parent') { const p = cdb.parents?.get(norm); if (!p) return ABSENT; return p.pid == null ? null : p.disp; }   // disp = undefined = 親はあるのにコードが読めない (呼び手が保持する)
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
export function compareNe({ dataDir, asOfJst, syncRunId = null, loadCtx = null, cdb, ledger, loadVerdict = null, tmpRoot, decisionLedger = null, baseline = null, regTargets = null,
  registrations = null }) {
  const out = { format: NE_FORMAT, verdict: null, blocked_reason: null, prerequisites: {}, generation: null, build: null, ne_marks: null,
    items: [], held: {}, recoverable: [], out_of_scope: {}, decisions: [], raw_diffs: [], counts: {}, pending: { state: ledger?.state ?? null, reason: ledger?.reason ?? null } };
  const pre = out.prerequisites;
  const block = (reason, extra = {}) => { Object.assign(out, { verdict: 'blocked', blocked_reason: reason }, extra); return { result: out, pendingEntries: null, decisionsDone: [], neCodes: null }; };
  out.decisions_read = decisionLedger ? decisionLedger.state : 'not_applied';
  // 判断の台帳が読めない = 「承認なし」と読まない (承認済みの差を毎朝の差として出し直さない・完了を見落とさない)。② ごと判定できない (D1 契約 v3)
  if (decisionLedger && decisionLedger.state === 'unreadable') return block('decisions_unreadable', { decisions_read_error: decisionLedger.reason ?? null });

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
  const { m: nm, collided: nCollided } = nModelOf(ne);
  // 前提が欠けても、NE の集合が読めていれば「値の差」だけは一覧に出す (分類はしない)
  const rawDiffs = () => {
    const d = [];
    for (const [norm, n] of nm) {
      if (!cdb.skuByNorm.has(norm)) continue;
      for (const [col, st] of Object.entries(n.cols)) {
        if (comparability(st) !== 'comparable' || (col === 'standard_price_jpy' && !cdb.has0027) || (col === 'primary_supplier' && !cdb.has0027)) continue;
        const c = cValue(cdb, norm, col);
        if (col === 'parent' && c === undefined) continue;
        if (!eqv(col === 'primary_supplier' ? [st.value] : st.value, c)) d.push({ key: subjectKey(col === 'cost' || col === 'primary_supplier' || col === 'parent' ? col : 'value', norm), col, n: st.value, c });
      }
    }
    return d;
  };
  // ── 2. 鮮度 (C2 v4 §1・Codex C2-R0 M4 = 印も作り直しも古い朝を通さない) ──
  pre.freshness = { products_at_jst: jstDateOfUtcText(marks.products.at), sets_at_jst: jstDateOfUtcText(marks.sets.at), build: ne.build ? ne.build.build_id : null,
    build_date: ne.build ? jstDateOfIso(ne.build.published_at) : null, build_run: ne.build ? ne.build.daily_sync_run_id : null, sync_run_id: syncRunId };
  if (pre.freshness.products_at_jst !== asOfJst || pre.freshness.sets_at_jst !== asOfJst) return block('stale_ne', { raw_diffs: rawDiffs() });
  if (!ne.build || pre.freshness.build_date !== asOfJst || (syncRunId && ne.build.daily_sync_run_id !== syncRunId)) return block('stale_build', { raw_diffs: rawDiffs() });
  out.build = { build_id: ne.build.build_id, published_at: ne.build.published_at, daily_sync_run_id: ne.build.daily_sync_run_id,
    // 作り直しが使った Company DB の写しの世代 (④a。前の記録 = null)
    cdb_publish: ne.build.cdb_publish_generation_no == null ? null : { generation_no: ne.build.cdb_publish_generation_no, generation_id: ne.build.cdb_publish_generation_id ?? null,
      content_hash: ne.build.cdb_publish_content_hash ?? null, applied_hash: ne.build.cdb_publish_applied_hash ?? null } };
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
  const pf = planFromSnapshot({ productsRows: snap.products, setRows: snap.set_components, expected, now: new Date(Date.parse(b.published_at)), ...(tmpRoot ? { tmpRoot } : {}),
    semantics: snap.generation?.products?.semantics ?? null });   // 今朝の世代の意味の版 (D3b。代表の明示の空)
  if (!pf.ok) return block(pf.reason, { raw_diffs: rawDiffs() });
  const { m: tToday, collided: tTodayCollided, skipParents: todaySkipParents } = tModelOf(pf.plan);
  /**
   * 今朝の材料で、ロードがこの親の構成を削除まで行うか (engine の条件: 材料に 1 行以上・飛ばす行が無い・Company DB の manual の行と数量が違わない)。
   * 行わない親の「子が材料に無い」は削除の目標にしない (反映待ちにしない。Codex #1464 R4 の確認 Medium)
   */
  const prunableToday = (pn) => {
    const ch = tToday.get(pn)?.children;
    if (!ch || !ch.size || todaySkipParents.has(pn)) return false;
    const cur = cdb.comps.get(pn) || new Map();
    for (const [cn, q] of ch) { const r = cur.get(cn); if (r && r.source === 'manual' && r.qty !== q) return false; }
    return true;
  };
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
  const tLoadModel = p4 ? tModelOf(loadCtx.plan) : null;
  const tLoad = tLoadModel ? tLoadModel.m : null;
  const collidedNorms = new Set([...nCollided, ...tTodayCollided, ...(tLoadModel ? tLoadModel.collided : [])]);
  const loadStartMs = p4 ? Date.parse(loadCtx.load.started_at) : null;
  const ledgerOk = ledger && (ledger.state === 'ok' || ledger.state === 'initial');
  pre.ledger = { state: ledger?.state ?? null, reason: ledger?.reason ?? null };
  const own = p4 ? loadCtx.ownership : null;
  const ownerKey = (col) => ({ name: 'skus.name', handling: 'skus.handling', tax_rate: 'skus.tax_rate', kind: 'skus.sku_kind', cost: 'sku_costs', primary_supplier: 'supplier_skus.is_primary', components: 'sku_components', parent: 'products.parent' })[col]
    ?? (SKU_OWNED_COLUMNS.find(([c]) => c === col) || [])[1] ?? null;
  const loadOwns = (col) => {
    if (col === 'exists') return true;   // SKU の INSERT は持ち主で止めない (engine)
    const k = ownerKey(col); if (!k || !own) return false;
    if ((SKU_0027_COLUMNS.includes(col) || col === 'primary_supplier') && !loadCtx.has0027) return false;
    return own[k] === 'load';
  };
  /** 持ち主が C で、④a が古い表 (m_products) に写す列か (昨夜のロードが記録した持ち主で決める) */
  const companyCopied = (type, col) => {
    if (!own || !COMPANY_COPIED.has(col) || !['value', 'cost', 'primary_supplier'].includes(type)) return false;
    if ((SKU_0027_COLUMNS.includes(col) || col === 'primary_supplier') && !loadCtx.has0027) return false;
    return own[ownerKey(col)] === 'company';
  };
  // ① の差 (load_mismatch を列・子の行で引く)
  const loadItems = new Map(p4 ? loadCtx.items.map((i) => [i.subject_key || subjectKey(i.type, i.norm), i]) : []);
  const loadFlagged = (type, norm, col, child) => {
    if (!p4) return false;
    if (type === 'value' || type === 'kind') { const i = loadItems.get(subjectKey('value', norm)); return !!i && i.diffs.some((d) => d.col === (col === 'kind' ? 'sku_kind' : col)); }
    if (type === 'cost') return loadItems.has(subjectKey('cost', norm));
    if (type === 'primary_supplier') return loadItems.has(subjectKey('primary_supplier', norm));
    if (type === 'parent') return loadItems.has(subjectKey('parent', norm));
    if (type === 'only_in_ne') return loadItems.has(subjectKey('missing', norm));
    if (type === 'components') { const i = loadItems.get(subjectKey('components', norm)); return !!i && [...(i.missing || []), ...(i.qty || []), ...(i.extra || [])].some((x) => normSku(x.child) === child); }
    return false;
  };
  // 0030 の記録で説明できるか (記録した保持状態と今の c が一致するときだけ。C2 v6-1)
  const D = p4 ? loadCtx.D : null;
  // 代表 (D3b): ロードが保持した子 (variation_parents.held) = 理由・記録した親の product_id・帰属。記録が無いロード (0036 の前) = 空
  const heldParent = new Map(((D && D.variation_parents && D.variation_parents.held) || []).map(([code, reason, pid, , by]) => [normSku(code), { reason, pid: pid ?? null, by: by ?? null }]));
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
  const classify = ({ key, type, norm, col, child = null, nst, nv, tt, tl, c, entity, neCode = null }) => {
    const detail = { n_state: nst ? nst.raw : null, n_validity: nst ? nst.validity : null, n: nst ? (nst.text ?? show(nv)) : null, t_today: show(tt), t_load: p4 ? show(tl) : '(判定できない)', c: show(c) };
    const reasons = reasonsFor(norm, col);
    if (reasons.length) detail.reasons = reasons.map((r) => ({ reason: r.reason, value: r.value ?? null, source: r.source ?? null }));
    // 反映待ちの台帳: 今朝の値がまだ Company DB と違う単位は、どの分類になっても前の始まりを保つ (① が判定できない朝を挟んでも期限をリセットしない。C2 v4 §4)
    // 目標値 = ロードが入れようとする値。構成の子が材料に無い (ABSENT) = 「その子を消す」という目標 (Codex #1464 R4 Medium 3)。
    //   PRESERVE (ロードは触らない)・値の列の ABSENT (SKU が材料に無い = ロードは SKU を消さない) は目標にしない
    const deleteNotExpected = tt === ABSENT && type === 'components' && !prunableToday(norm);
    const isTarget = tt !== undefined && tt !== PRESERVE && !(tt === ABSENT && type !== 'components') && !deleteNotExpected;
    const unit = isTarget && !eqv(tt, c) ? unitOf(key, child ? `${col}:${child}` : col, tt) : null;
    const prev = unit && ledgerOk ? ledger.entries.get(unit) || null : null;
    if (prev) newPending.set(unit, prev);
    const comp = comparability(nst);
    // 持ち主が C の列 (④a で古い表に写す列。Codex ④ 設計 R0 #2): 夜間ロードは書かない = 「昨夜の適用」(A) は見ない (見ると not_owned = direction_unknown に落ちる)。
    //   今朝の写し (作り直しの材料 t_today) が C と同じ = NE との差は「C → NE へ流す」予定の差 (rule・company_owned・判断の一覧で NE を C の値に)。
    //   写しがまだ C と違う (今朝の写しの後に C が変わった・写しが届かなかった) = 翌朝の写し待ち (rule_lag)。材料に値が無い (PRESERVE) = 写しも空
    //   🚨 NE の値が空・0・null・不正でも先にここで分ける (Codex #1564 R1 M6。NE の値は C が正の列の判断に使わない = incomparable・ne_no_value にしない)。
    //     NE に値が無く C も空 = 一致 / それ以外 = NE を C の値に (C が空なら決める)。NE の状態 (n_state・n_validity・比べやすさ) は detail に残す
    if (companyCopied(type, col)) {
      // 社内に値が無い = null・空の配列だけ (0 円は実値 = 0027 の契約。#1629 Codex R1 High)
      const cEmpty = c == null || (Array.isArray(c) && c.length === 0);
      // 社内の名前がコードに見える = NE と同じでも一致にしない (既存の壊れ・人の決めた名前を判断に出す。#1629 Codex R1 Medium)
      const nameLike = col === 'name' && nameIsCode(c, norm);
      if (!nameLike && (comp === 'comparable' ? eqv(nv, c) : comp === 'no_value' && cEmpty)) return { cls: 'match', detail };
      detail.owner = 'company';
      if (comp !== 'comparable') detail.n_comparability = comp;
      const co = reasons.find((r) => r.reason === 'company_owned') || { reason: 'company_owned' };
      const copy = tt === PRESERVE ? (col === 'primary_supplier' ? [] : null) : tt;
      // 写しがまだ C と違う = 翌朝の写し待ち (rule_lag) を先に (下の理由で隠さない)
      if (!eqv(copy, c)) return { cls: 'rule_lag', detail, explained: co };
      const why = (reason) => { const x = { reason }; detail.reasons = [...(detail.reasons || []), x]; return { cls: 'rule', detail, explained: x }; };
      if (nameLike) {
        // 夜間ロードの `name || code` の名残と言える証拠 = 社内の名前が NE のコードの書き方そのもの かつ 今の NE の取得の名前が空
        //   (材料の snapshot の名前 = blankName は使わない: 持ち主が C の後の材料は C の写し = 由来の証明にならない。#1629 Codex R2)
        const placeholder = c === neCode && nst?.raw === 'empty';
        return why(placeholder ? 'cdb_name_is_code' : 'name_like_code');
      }
      // 社内 0 円 (実値) = NE は 0 と未入力を区別できず CSV も 0 を書かない = 人が決める (一致にしない・NE を直すは出さない)
      if (YEN_COLS.has(col) && typeof c === 'number' && c === 0) return why('cdb_zero_yen');
      return { cls: 'rule', detail, explained: co };
    }
    if (comp === 'incomparable') return { cls: 'incomparable', detail };
    if (comp === 'no_value') return { cls: 'ne_no_value', detail };
    if (eqv(nv, c)) return { cls: 'match', detail };
    // A 昨夜の適用
    let A = null;
    if (p4) {
      // 順番 (Codex #1464 R1): ① がこの項目の差を出していれば load_mismatch が先 (金額・数量だけの一致で applied にしない = source の違いを隠さない)
      //   → ロードが触らなかった (PRESERVE) 列は、記録した保持状態と今の c が一致したときだけ applied (C2 v6-1。記録が無い・違う = unexplained)
      if (type === 'parent') {
        // 代表 (D3b 契約 v2 H1): ① の差 → ロードの保持の記録を**必ず**照らす → 記録が無いときだけ材料と比べる。applied は最終の分類ではない (下の B・到達・lag へ)
        const h = heldParent.get(norm);
        if (loadFlagged(type, norm, col, child)) A = 'load_mismatch';
        else if (h) {
          const cur = cdb.parents.get(norm) || { pid: null, by: null };
          if (cur.pid !== h.pid || cur.by !== h.by) { A = 'unexplained'; detail.why_a = 'held_state_changed'; }   // 記録した親・帰属から変わった
          else if (h.reason === 'manual') A = { cls: 'rule', reason: { reason: 'parent_manual' } };
          else if (h.reason === 'rep_unknown' && tl === PRESERVE) A = 'applied';
          else A = { cls: 'held_by_load', reason: { reason: 'held_by_load', reason_code: h.reason } };
        } else if (tl === PRESERVE) { A = 'unexplained'; detail.why_a = 'preserve_unverified'; }
        else if (eqv(c, tl)) A = 'applied';
        else if (!loadOwns('parent')) A = 'not_owned';
        else A = 'unexplained';
      } else {
        if (loadFlagged(type, norm, col, child)) A = 'load_mismatch';
        else if (tl === PRESERVE) A = preservedAsRecorded(type, norm) ? 'applied' : 'unexplained';
        else if (eqv(c, tl)) A = 'applied';
        else if (!loadOwns(col === 'exists' ? 'exists' : type === 'components' ? 'components' : col)) A = 'not_owned';
        else { const x = explainByDecision(type, norm, col, child, c, tl); A = x ? x : 'unexplained'; }
        if (A === 'unexplained' && tl === PRESERVE) detail.why_a = 'preserve_unverified';
      }
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
    if (!isTarget) return why.length ? { cls: 'rule', detail, explained: why[0] } : { cls: 'unexplained', why: deleteNotExpected ? 'delete_not_expected' : 'material_has_no_value', detail };
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
  const regList = { waiting: [], stale: [], cancelled: [], kind_mismatch: [], partial: [] };   // ポータルで登録した新商品で NE に無いもの・区分違い (照合の報告に残す)
  const regEntry = (code, norm, kind, reg) => ({ code, norm, kind, state: reg.state, stage: reg.stage ?? null, since: reg.since, created: reg.created, days: reg.days ?? null });
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
    // 正規化で同じになる別の表記がある SKU = どの表記の値か確かめられない = 保持 (回復させない。Codex #1464 R4 High 1)
    if (collidedNorms.has(norm)) { for (const t of PROBLEM_TYPES) holdKey(t, norm, 'norm_collision'); continue; }
    const blockedWhy = intBlocked.get(norm);
    if (blockedWhy) { for (const t of PROBLEM_TYPES) holdKey(t, norm, `ne_integrity:${blockedWhy}`); continue; }
    const entity = (n?.kind ?? cRow?.sku_kind) === 'set' ? 'set_components' : 'products';
    // 有無
    if (n && !cRow) {
      const r = classify({ key: subjectKey('only_in_ne', norm), type: 'only_in_ne', norm, col: 'exists', nst: { raw: 'value', validity: 'ok' }, nv: true,
        tt: tValue(tToday, norm, 'exists'), tl: tLoad ? tValue(tLoad, norm, 'exists') : undefined, c: false, entity: 'products' });
      addCol('only_in_ne', norm, code, n.kind, r, 'exists');
      for (const t of ['value', 'cost', 'primary_supplier', 'components', 'kind', 'parent']) holdKey(t, norm, 'not_in_cdb');
      out.recoverable.push(subjectKey('only_in_cdb', norm));
      continue;
    }
    if (!n && cRow) {
      const key = subjectKey('only_in_cdb', norm);
      // NE の行が落ちた朝 = 「NE に無い」と言えない = 先に保持 (NE 登録待ち・やめたの対象外にもしない = 前の朝に開いた区分違い・値の差の案件を「監視対象外」で閉じない。#1635 Codex R1 Medium)
      if (absenceUntrusted) { holdKey('only_in_cdb', norm, 'ne_dropped_rows'); for (const t of ['value', 'cost', 'primary_supplier', 'components', 'kind', 'parent']) holdKey(t, norm, 'not_in_ne'); continue; }
      // ポータルで登録して NE 登録の前〜確かめ待ち・登録をやめた = NE に無くて当然 (完全な取得の朝だけ) → 差にしない・報告に残す
      const reg = registrationAbsence(registrations, norm, asOfJst);
      if (reg && reg.kind !== 'stale') {
        const why = reg.kind === 'cancelled' ? 'reg_cancelled' : 'reg_pending';
        for (const t of PROBLEM_TYPES) out.out_of_scope[subjectKey(t, norm)] = why;
        regList[reg.kind].push(regEntry(code, norm, cRow.sku_kind, reg));
        continue;
      }
      const inToday = tToday.has(norm);
      const reasons = reasonsFor(norm, 'exists');
      let r;
      if (reg) {
        // 登録から REG_STALE_DAYS 日たっても NE に出てこない (放置を見落とさない)。材料・作り直しの理由より先 (直す道はマスタの入力の NE 登録の CSV)
        r = { cls: 'reg_stale', detail: { c: cRow.sku_kind === 'set' ? 'セット' : '単品', t_today: inToday ? 'あり' : '(無い)', reg_state: reg.state, reg_since: reg.since, reg_created: reg.created, reg_days: reg.days,
          note: `ポータルで登録した新商品が ${reg.days ?? '?'} 日 NE に無い (状態 ${reg.state})` }, explained: { reason: 'reg_stale', state: reg.state, since: reg.since } };
        regList.stale.push(regEntry(code, norm, cRow.sku_kind, reg));
      } else if (cRow.sku_kind === 'set') r = { cls: 'spec_undecided', detail: { c: 'セット', t_today: inToday ? 'あり' : '(無い)' } };
      else if (!inToday) r = { cls: 'spec_undecided', detail: { c: '単品', t_today: '(無い)', note: '材料に無い SKU はロードが消さない (削除・保持の仕様は D)' } };
      else r = reasons.some((x) => x.reason === 'not_in_latest_fetch') ? { cls: 'rule', detail: { reasons: reasons.map((x) => ({ reason: x.reason })) }, explained: { reason: 'not_in_latest_fetch' } }
        : { cls: 'unexplained', why: 'cdb_only_without_reason', detail: {} };
      addCol('only_in_cdb', norm, code, cRow.sku_kind, r, 'exists');
      for (const t of ['value', 'cost', 'primary_supplier', 'components', 'kind', 'parent']) holdKey(t, norm, 'not_in_ne');
      out.recoverable.push(subjectKey('only_in_ne', norm));
      continue;
    }
    // 両方にある
    out.recoverable.push(subjectKey('only_in_ne', norm));
    if (absenceUntrusted && n.kind === 'single' && cRow.sku_kind === 'set') {
      // NE が単品に見えるのは「セットの表に無い」から = 行が落ちた朝は確かめられない。種別に依存する案件も保持 (Codex #1464 R4 High 2)
      for (const t of ['kind', 'value', 'cost', 'primary_supplier', 'components', 'parent']) holdKey(t, norm, 'ne_dropped_rows');
      out.recoverable.push(subjectKey('only_in_cdb', norm));
      continue;
    } else if (n.kind !== cRow.sku_kind) {
      // ポータルで登録した新商品 (NE 登録の前〜確かめ待ち) の区分が NE と違う (単品で登録したのに NE はセット など) = 重大な登録の不一致。反映待ち・材料の説明より先に分ける
      const rg = registrations && registrations.state === 'ok' ? registrations.byNorm.get(norm) : null;
      const r = rg && REG_WAIT_STATES.includes(rg.state)
        ? { cls: 'reg_kind_mismatch', detail: { n_state: 'value', n_validity: 'ok', n: n.kind, c: cRow.sku_kind, reg_state: rg.state, reg_stage: rg.stage, reg_since: rg.since,
          note: `ポータルで${cRow.sku_kind === 'set' ? 'セット' : '単品'}として登録したのに NE は${n.kind === 'set' ? 'セット' : '単品'}` }, explained: { reason: 'reg_kind_mismatch', state: rg.state } }
        : classify({ key: subjectKey('kind', norm), type: 'kind', norm, col: 'kind', nst: { raw: 'value', validity: 'ok' }, nv: n.kind,
          tt: tValue(tToday, norm, 'kind'), tl: tLoad ? tValue(tLoad, norm, 'kind') : undefined, c: cRow.sku_kind, entity });
      if (r.cls === 'reg_kind_mismatch') regList.kind_mismatch.push({ ...regEntry(code, norm, cRow.sku_kind, { ...rg, days: null }), ne_kind: n.kind });
      addCol('kind', norm, code, n.kind, r, 'kind');
      for (const t of ['value', 'cost', 'primary_supplier', 'components', 'parent']) holdKey(t, norm, 'kind_mismatch');
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
      const r = classify({ key: subjectKey(type, norm), type, norm, col, nst, nv, tt: wrap(tValue(tToday, norm, col)), tl: tLoad ? wrap(tValue(tLoad, norm, col)) : undefined, c: cValue(cdb, norm, col), entity: 'products', neCode: n.code });
      addCol(type, norm, code, n.kind, r, col);
    }
    // 代表 (親子。D3b): 単品だけ。親はあるのにコードが読めない = 親なしに潰さず保持。セット同士 = 比べない (開いていた案件を閉じる)
    if (n.kind === 'single') {
      const pc = cdb.parents.get(norm);
      if (pc && pc.pid != null && pc.disp === undefined) holdKey('parent', norm, 'cdb_parent_unresolved');
      else if (pc) {
        const nst = n.cols.parent;
        const r = classify({ key: subjectKey('parent', norm), type: 'parent', norm, col: 'parent', nst, nv: nst.value,
          tt: tValue(tToday, norm, 'parent'), tl: tLoad ? tValue(tLoad, norm, 'parent') : undefined, c: pc.pid == null ? null : pc.disp, entity: 'products' });
        addCol('parent', norm, code, n.kind, r, 'parent');
      }
    } else out.out_of_scope[subjectKey('parent', norm)] = 'set_not_compared';
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
      // NE をこの値に = 提案してよい値だけ (空・0 の売価/原価・名前 = コード は「決める」。2026-10-05 の「NE を 0 に」「NE をコードに」)
      const setNe = (v) => (proposableValue(col.col, v, it.norm) ? { op: 'set_ne_value', value: v } : { op: 'decide' });
      const proposal = col.cls === 'reg_stale' ? { op: 'register_in_ne', state: reason?.state ?? null, since: reason?.since ?? null }   // NE 登録の CSV で取り込む・やめるなら登録をやめる
        : col.cls === 'reg_kind_mismatch' ? setNe(col.c)   // NE を登録の区分に (NE の画面で直す)
        : reasonKind === 'cdb_name_is_code' ? { op: 'fill_cdb_name' }   // 社内の名前がコードのまま (NE も空) = 社内 (ポータル) で本当の名前を入れる
        : reasonKind === 'name_like_code' ? { op: 'check_name' }                   // コードに見える名前 = 人が確かめる (どちらにも直す既定は出さない)
          : reasonKind === 'cdb_zero_yen' ? { op: 'decide_zero' }                  // 社内 0 円 = 差を残すか社内を直す
            : col.cls === 'ne_no_value' || incomparableNoValue ? setNe(col.c)
          : col.cls === 'held_by_load' ? { op: 'fix_load_input', reason_code: reason?.reason_code ?? null }
            : col.cls === 'spec_undecided' ? { op: 'decide_spec' }
              : reasonKind === 'manual' || reasonKind === 'parent_manual' ? { op: 'decide_manual_priority' }
                : reasonKind === 'set_price_from_goods' || reasonKind === 'load_rule:name_blank_to_code' ? setNe(col.t_today)
                  // 持ち主が C の列の差 (④a) = NE を C の値に (C が空・0 = 決める)
                  : reasonKind === 'company_owned' ? setNe(col.c) : { op: 'decide' };
      const owner = col.col === 'exists' ? 'load' : own ? own[ownerKey(col.col)] ?? null : null;   // SKU の INSERT は持ち主で止めない
      const print = decisionPrint({ norm: it.norm, kind: it.kind, col: col.col, child: col.child ?? null, problem: it.type, owner, reasonKind, reason, n_state: col.n_state, n: col.n, c: col.c, proposal });
      const fingerprint = approvalFingerprint(print);
      // 全件 JSON に指紋の元・選べる解決・意味の版をそのまま残す = 台帳の入れ直し (replay-decisions.mjs) は再計算しない (Codex D-R1 M2)
      out.decisions.push({ subject_key: it.subject_key, code: it.code, norm: it.norm, code_norm: it.norm, kind: it.kind, col: col.col, child: col.child ?? null, cls: col.cls, reason_kind: reasonKind,
        n_state: col.n_state ?? null, n: col.n ?? null, c: col.c ?? null, t_today: col.t_today ?? null, reason, proposal, decision_status: 'pending',
        fingerprint, approval_fingerprint: fingerprint, print, semantic: print.semantic,
        resolutions: resolutionsFor({ cls: col.cls, reasonKind, incomparableNoValue, hasC: proposal.op === 'set_ne_value' }) });   // 提案できる値が無い (空・0) = NE を直すは値を入れて
    }
  }
  // ── 11. 判断の台帳 (D1 契約 v3): 列の分類はそのまま・判断の状態を重ねる / 差を残す承認だけで埋まった案件を閉じる / 直す承認の完了を目標の単位で確かめる ──
  const decisionsDone = [];
  if (decisionLedger && decisionLedger.state === 'ok') {
    const colOf = new Map();
    for (const it of keys.values()) for (const c of it.columns) colOf.set(`${it.subject_key}|${c.col}|${c.child ?? ''}`, c);
    for (const d of out.decisions) {
      const j = decisionLedger.latest.get(d.fingerprint);
      let st = 'pending';
      if (j && j.kind === 'approved') {
        const isFix = j.resolution === 'fix_ne' || j.resolution === 'fix_cdb';
        st = isFix && decisionLedger.done.has(Number(j.event_id)) ? 'pending' : `approved:${j.resolution}`;   // 直した後にまた同じ差 = 再発 = 判断し直し
      } else if (j && j.kind === 'rejected') st = 'rejected';
      d.decision_status = st;
      if (j) d.decision_event_id = Number(j.event_id);
      const c = colOf.get(`${d.subject_key}|${d.col}|${d.child ?? ''}`);
      if (c) c.decision = st;
    }
    // 閉じる = 非一致の列が全部「差を残す」の有効な承認 かつ 比べられない・判定できない列が無い (Codex D-R0 H1)
    //   比べられない列の候補は fix_ne だけ・0032 が選べる解決に無い承認を拒む = 後半の条件は二重の守り (台帳の外から入った承認でも閉じない)
    for (let i = items.length - 1; i >= 0; i--) {
      const it = items[i];
      const nonMatch = it.columns.filter((c) => c.cls !== 'match');
      if (!nonMatch.length || nonMatch.some((c) => c.cls === 'blocked' || c.cls === 'incomparable')) continue;
      if (nonMatch.every((c) => c.decision === 'approved:accept_difference')) { out.out_of_scope[it.subject_key] = 'approved_exception'; items.splice(i, 1); }
    }
    // 直す承認の完了 = 目標の単位の値が承認した目標値と等しいときだけ (n と c の一致では完了にしない。Codex D-R1 H1・H2)
    const unitOf = (tg) => { const s = String(tg && tg.subject_key || ''); const at = s.indexOf(':'); return at > 0 ? s.slice(at + 1) : null; };
    // 目標の単位の今の値。**信頼できる観測だけ** (比べられる値・行が落ちていない・種類の判定を保留していない)。それ以外は undefined = 今回は完了を確かめない (Codex #1475 R1 High)
    //   子を消す目標 = ABSENT ('__absent__')。有無 = true / false・種類 = 'single' / 'set'
    // 代表 (D3b) の完了を確かめてよい SKU = 両側に単品であり・例外でなく・親が読める (案件で保持・対象外にする回は完了にもしない。Codex #1490 R2 Medium)
    const parentUnitOk = (norm) => {
      const n0 = nm.get(norm), r0 = cdb.skuByNorm.get(norm);
      if (!n0 || !r0 || n0.kind !== 'single' || r0.sku_kind !== 'single') return false;
      if (exceptionNorms.has(norm) || tToday.get(norm)?.kind === 'exception') return false;
      const pc = cdb.parents.get(norm);
      return !!pc && !(pc.pid != null && pc.disp === undefined);
    };
    const neUnit = (norm, col, child) => {
      const n = nm.get(norm);
      if (col === 'exists') return n ? true : (absenceUntrusted ? undefined : false);   // 「NE に無い」は行が落ちた回には言えない
      if (!n) return undefined;
      const c0 = cdb.skuByNorm.get(norm);
      if (absenceUntrusted && n.kind === 'single' && c0 && c0.sku_kind === 'set') return undefined;   // 種類の判定を保留した回
      if (col === 'kind') return n.kind;
      if (col === 'parent') {   // 単品同士だけ (セット・例外・種類違いは確かめない)。比べられない = 不明は目標の親なしとも一致させない
        if (!parentUnitOk(norm)) return undefined;   // 単品同士・例外でない・親が読める (Codex #1490 R1・R2 Medium)
        const st = n.cols.parent; return st && comparability(st) === 'comparable' ? st.value : undefined;
      }
      if (col === 'components') {
        if (componentsUntrusted) return undefined;
        const x = n.children && n.children.get(child); if (!x) return absenceUntrusted ? undefined : ABSENT;   // C2 形の親の行落ちの回も「子が無い」と言えない (Codex #1475 R2 High)
        return comparability(x.st) === 'comparable' ? x.st.value : undefined;
      }
      const st = n.cols[col]; if (!st) return undefined;
      return comparability(st) === 'comparable' ? (col === 'primary_supplier' ? [st.value] : st.value) : undefined;   // 値なし (空・0)・不正・不明では完了にしない
    };
    const cdbUnit = (norm, col, child) => {
      if (col === 'exists') return cdb.skuByNorm.has(norm);
      const r = cdb.skuByNorm.get(norm); if (!r) return undefined;
      if (col === 'kind') return r.sku_kind;
      if (col === 'parent') {   // 単品同士だけ。親はあるのにコードが読めない = 確かめない
        if (!parentUnitOk(norm)) return undefined;   // NE に無い・例外・読めない親の回は確かめない
        const p = cdb.parents.get(norm);
        return p.pid == null ? null : p.disp;
      }
      if (col === 'components') { const x = cdb.comps.get(norm)?.get(child); return x ? x.qty : ABSENT; }
      return cValue(cdb, norm, col);
    };
    for (const [fp, j] of decisionLedger.latest) {
      if (j.kind !== 'approved' || (j.resolution !== 'fix_ne' && j.resolution !== 'fix_cdb') || decisionLedger.done.has(Number(j.event_id))) continue;
      const tg = j.target || {}; const norm = unitOf(tg);
      if (!norm || !tg.col || collidedNorms.has(norm) || intBlocked.has(norm)) continue;   // 信頼できる観測でない = 今回は確かめない
      const v = j.resolution === 'fix_ne' ? neUnit(norm, tg.col, tg.child ?? null) : cdbUnit(norm, tg.col, tg.child ?? null);
      if (v === undefined) continue;
      const want = tg.col === 'primary_supplier' && tg.value != null && !Array.isArray(tg.value) ? [tg.value] : tg.value;
      // 観測 = 承認の目標そのもの (単位と値) + 実際に見た値 (raw)。関数が目標と照らして食い違えば拒む
      if (eqv(want, v)) decisionsDone.push({ approved_event_id: Number(j.event_id), fingerprint: fp, observed: { side: j.resolution === 'fix_ne' ? 'ne' : 'cdb', subject_key: tg.subject_key, col: tg.col, child: tg.child ?? null, value: tg.value, raw: show(v) } });
    }
  }
  out.decisions_done = decisionsDone.map((x) => ({ approved_event_id: x.approved_event_id, subject_key: x.observed.subject_key, col: x.observed.col, child: x.observed.child }));
  // ── 12. 最後に一致した値 (D2 契約 v2。影運転 = ② の分類・verdict・判断は変えない。方向は ne.baseline と列の direction に付けるだけ) ──
  const holdSku = (norm) => (collidedNorms.has(norm) ? 'norm_collision' : intBlocked.has(norm) ? `ne_integrity:${intBlocked.get(norm)}`
    : exceptionNorms.has(norm) || cdb.skuByNorm.get(norm)?.sku_kind === 'exception' || tToday.get(norm)?.kind === 'exception' ? 'exception_item' : null);
  const bl = evaluateBaseline({ nm, cdb, holdSku, absenceUntrusted, setRowsDropped: c2Form ? is.dropped_missing_parent > 0 : is.dropped_missing_key > 0, componentsUntrusted, baseline,
    generation: { products_at: marks.products.at, products_rev: marks.products.rev, sets_at: marks.sets.at, sets_rev: marks.sets.rev } });
  out.baseline = bl.section;
  if (bl.section.state !== 'not_applied') {
    for (const it of keys.values()) {
      const skuHeld = bl.directionOf.get(`${it.norm}|*`);
      for (const c of it.columns) { const d = bl.directionOf.get(`${it.norm}|${c.col}`) ?? skuHeld; if (d) c.direction = d; }   // 列ごとの方向が先 (種類違いの kind の列は kind の方向)
    }
  }
  const byClass = {};
  for (const it of items) for (const c of it.columns) byClass[c.cls] = (byClass[c.cls] || 0) + 1;
  // ポータルで登録した新商品 (照合の報告に残す。一覧はコードの順):
  //   waiting = NE 登録待ち (NE に無い・差にしない) / stale = 登録から日がたっても NE に無い (reg_stale = 差) / cancelled = 登録をやめた (対象外) /
  //   kind_mismatch = 区分違い (reg_kind_mismatch = 差) / partial = NE にあるが新規登録の CSV の確かめで中身が違った (値の差は普通の案件。ここは段階の報告だけ)。
  //   段階 (stage) = CSV を配る前・配った・申告した・partial・failed (申告したのに NE に無い)
  if (registrations && registrations.state === 'ok') {
    for (const [norm, g] of registrations.byNorm) {
      // 区分違い (kind_mismatch) に出した商品は二重に数えない。NE にあるかは問わない (確かめの記録で数える = NE の行が落ちた朝も ⚠️ を消さない。#1635 Codex R1 Medium)
      if (!REG_WAIT_STATES.includes(g.state) || g.stage !== 'partial' || regList.kind_mismatch.some((e) => e.norm === norm)) continue;
      regList.partial.push(regEntry(nm.get(norm)?.code ?? cdb.skuByNorm.get(norm)?.code ?? norm, norm, g.sku_kind, g));
    }
  }
  const byCode = (a, b) => (a.norm < b.norm ? -1 : a.norm > b.norm ? 1 : 0);
  // 日がたった分で「差を残す」の承認で閉じたもの = 一覧には残し、差の数には入れない
  for (const e of regList.stale) if (out.out_of_scope[subjectKey('only_in_cdb', e.norm)] === 'approved_exception') e.accepted = true;
  const absent = [...regList.waiting, ...regList.stale];
  const stages = Object.fromEntries(REG_STAGES.map((st) => [st, regList.waiting.filter((e) => e.stage === st).length]).filter(([, v]) => v));   // NE 登録待ちの段階ごとの数
  const regCounts = { reg_pending: regList.waiting.length, reg_stale: regList.stale.filter((e) => !e.accepted).length, reg_cancelled: regList.cancelled.length, reg_kind_mismatch: regList.kind_mismatch.length,
    reg_partial: regList.partial.length, reg_failed: absent.filter((e) => e.stage === 'failed').length, reg_rejected: absent.filter((e) => e.stage === 'rejected').length };
  out.reg_pending = { state: registrations ? registrations.state : 'not_applied', ...(registrations && registrations.reason ? { reason: registrations.reason } : {}), stale_days: REG_STALE_DAYS,
    stages, waiting: regList.waiting.sort(byCode), stale: regList.stale.sort(byCode), cancelled: regList.cancelled.sort(byCode), kind_mismatch: regList.kind_mismatch.sort(byCode),
    partial: regList.partial.sort(byCode) };
  out.counts = { ne_skus: nm.size, cdb_skus: cdb.skuByNorm.size, items: items.length, by_type: Object.fromEntries(PROBLEM_TYPES.map((t) => [t, items.filter((i) => i.type === t).length])),
    by_class: byClass, held: Object.keys(out.held).length, recoverable: out.recoverable.length, out_of_scope: Object.keys(out.out_of_scope).length, decisions: out.decisions.length,
    pending: ledgerOk ? newPending.size : null,
    decisions_state: out.decisions.reduce((a, d) => { const k = String(d.decision_status).split(':')[0]; a[k] = (a[k] || 0) + 1; return a; }, {}),
    approved_exception: Object.values(out.out_of_scope).filter((v) => v === 'approved_exception').length, decisions_done: decisionsDone.length,
    ...regCounts };
  out.verdict = items.length ? 'breach' : 'pass';
  // NE のコードの元の書き方 (③b-1b): 同じ読み取りで決めたもの。JSON には件数だけ (書くのは run.mjs が判断の台帳の後に)
  const neCodes = resolveNeCodes(ne.spellings);
  out.ne_codes = neCodes.ok ? { state: 'resolved', counts: Object.fromEntries(['ok', 'collided', 'invalid'].map((s) => [s, neCodes.entries.filter((e) => e.state === s).length])) }
    : { state: 'unavailable', reason: neCodes.reason };
  // 新商品の NE 登録の CSV (0053・契約 v3 H5): 同じ完全な取得の中の、確かめ待ちの商品の NE の値 (書くのは run.mjs が判断の台帳の後に)
  const regObs = Array.isArray(regTargets) ? registrationObservations(nm, regTargets, {
    collided: collidedNorms, intBlocked, absenceTrusted: !absenceUntrusted && !componentsUntrusted, productsAt: marks.products.at, setsAt: marks.sets.at,
    // 取得の世代と原本のハッシュ (#1571 Codex R2 Low) = 観測 (nm) を作った NE の完全な取得そのもの (warehouse.db の完了の印と raw の行)。Render の材料の世代・ハッシュではない
    fetch: { ...neFetchIdentity(marks, ne), products_rev: String(marks.products.rev), sets_rev: String(marks.sets.rev) },
  }) : null;
  out.registrations = regObs ? { targets: regTargets.length, observations: regObs.observations.length, present: regObs.observations.filter((o) => o.present).length } : { state: 'not_applied' };
  return { result: out, pendingEntries: ledgerOk ? [...newPending.values()] : null, decisionsDone, baselineWrites: bl.writes, neCodes, regObs };
}

/**
 * NE の完全な取得の世代と原本のハッシュ (新規登録の確かめの観測の出どころ・#1571 Codex R2 Low)。
 *   generation_id = 完了の印 (単品・セットの取得の時刻と版) から決まる名前 (ne_<単品の時刻>_<版>_<セットの時刻>_<版>)
 *   raw_hash = readNeSide が読んだ raw の行 (nm を作った行そのもの) を列の順の配列にして並べ替えた JSON の sha256 (行の並びに依らない)
 */
const NE_RAW_PRODUCT_COLS = ['code', 'name', 'supplier', 'handling', 'cost_src', 'price_src', 'tax_src', 'rep', 'rep_src'];
const NE_RAW_SET_COLS = ['parent', 'name', 'child', 'price_src', 'qty_src'];
export function neFetchIdentity(marks, ne) {
  const digits = (t) => String(t ?? '').replace(/[^0-9]/g, '');
  const rev = (r) => String(r ?? '').replace(/[^A-Za-z0-9_.:-]/g, '');
  const rows = (list, cols) => (list || []).map((r) => JSON.stringify(cols.map((c) => (r[c] === undefined ? null : r[c])))).sort();
  return {
    generation_id: `ne_${digits(marks.products.at)}_${rev(marks.products.rev)}_${digits(marks.sets.at)}_${rev(marks.sets.rev)}`.slice(0, 120),
    raw_hash: crypto.createHash('sha256').update(JSON.stringify({ v: 'ne-raw-1', products_at: marks.products.at, products_rev: String(marks.products.rev),
      sets_at: marks.sets.at, sets_rev: String(marks.sets.rev), products: rows(ne.products, NE_RAW_PRODUCT_COLS), sets: rows(ne.sets, NE_RAW_SET_COLS) })).digest('hex'),
  };
}
/** sync_meta の時刻 ('YYYY-MM-DD HH:MM:SS' = UTC) → ISO */
const utcTextToIso = (t) => { const ms = Date.parse(`${String(t).replace(' ', 'T')}Z`); return Number.isFinite(ms) ? new Date(ms).toISOString() : null; };
/** 値の状態 → 送る形 { st: ok | no_value | invalid, v } */
const stOf = (st) => ({ st: comparability(st) === 'comparable' ? 'ok' : comparability(st) === 'no_value' ? 'no_value' : 'invalid', v: st && st.value !== undefined ? st.value : null });
/**
 * 新規登録の商品ごとの NE の観測 (ops.record_ne_registration_check に送る形)。nm = nModelOf の結果 (完全な取得の集合)。
 * targets = [{ code_norm, sku_kind }] (回の始まりの写し = ops.snapshot_ne_reg_targets)。trusted = 正規化の衝突・取込の整合の問題が無い
 * 単品の列 = 名前・仕入先 (4 桁に揃えた norm)・原価・売価・税率・取扱区分・代表 (親なし = null) / セット = 名前・売価・構成品 (norm と数量)
 */
export function registrationObservations(nm, targets, { collided = new Set(), intBlocked = new Map(), absenceTrusted = false, productsAt = null, setsAt = null, fetch = null } = {}) {
  const observations = [];
  const seen = new Set();
  for (const t of targets) {
    const norm = normSku(t.code_norm ?? '');
    if (!norm || seen.has(norm)) continue;
    seen.add(norm);
    const n = nm.get(norm) || null;
    const trusted = !collided.has(norm) && !intBlocked.has(norm);
    if (!n) { observations.push({ code_norm: norm, present: false, trusted, kind: null }); continue; }
    const c = n.cols;
    const o = { code_norm: norm, present: true, trusted, kind: n.kind, ne_code: n.code };
    if (n.kind === 'single') {
      o.cols = { name: stOf(c.name), supplier: stOf(c.primary_supplier), cost: stOf(c.cost), price: stOf(c.standard_price_jpy), tax_rate: stOf(c.tax_rate),
        handling: stOf(c.handling), parent: stOf(c.parent) };
    } else {
      o.cols = { name: stOf(c.name), price: stOf(c.standard_price_jpy) };
      o.children = [...n.children].map(([cn, ch]) => ({ code_norm: cn, ...stOf(ch.st) }));
    }
    observations.push(o);
  }
  return { fetch, products_at: productsAt ? utcTextToIso(productsAt) : null, sets_at: setsAt ? utcTextToIso(setsAt) : null, absence_trusted: !!absenceTrusted,
    targets: [...seen], observations };
}
