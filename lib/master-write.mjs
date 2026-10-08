/**
 * master-write.mjs — マスタ入力画面 (apps/master-edit) が Company DB の既にある商品・セットを書く
 * (Company DB構想 14 §3「書き方」・§6 ⑤-1 / 10 §2・§8。Codex 設計レビュー ⑤-R0・R1 の契約)
 *
 * 保存 1 回 = 1 つの取引 (保存の記録 ops.master_edit_requests の done も同じ取引。「処理中」のまま残る行は作らない):
 *   1. 記録用の設定 (0026 のトリガーが読む) = actor_type human・actor_id メール・source_system portal_master_edit・request_id・reason。
 *      「今日」= 取引の初めに 1 回だけ決める東京の日付 (opts.now があればそれ・無ければ DB の now())
 *   2. 鍵の順番 = request_id ごとの鍵 → 切替の段階の共有の鍵 → (保存を開いているかをここで見る = 閉じていれば、ほかの鍵を取らずに 409)
 *      → マスタの書き込みの鍵 (共有・短く待つ。夜間ロードが排他で持つ = 待ちきれなければ 409「夜間の取り込み中」・#1563 仮レビュー M3)
 *      → SKU ごとの鍵 (sku_id の順・自分と書くかもしれない含むセット) → 親子の鍵 (代表を変えるときだけ・0036)
 *      → CSV の鍵 hashtext('ops.ne_csv') → 行の for update (商品 → SKU)。構成の依頼の昇格 (promoteComponentRequest) も 段階 → マスタの書き込み → SKU ごと → CSV の順。
 *      🚨 NE に取り込む CSV の行を作る・予約する口も、SKU ごとの鍵を取るなら CSV の鍵より前に取る (ne-csv-lock.mjs)
 *   3. 同じ request_id = 残した結果・誤りをそのまま返す (中身が違えば 409)。request_id の鍵を最初に取るので、違う SKU への同じ番号も並ぶ (#1563 R1 M6)
 *   4. 鍵の後に全部を読み直す。画面が読んだ「編集の印」(editTokenOf = 版つき・読んだ行の全部と「行が無いこと」のハッシュ) と違う = 409 + その間の変更
 *      印が同じなら DB でも保存を始める (ops.begin_master_write = 書き込みの約束: 段階・持ち主表・操作・相手の行・版 (editVersionsOf) を DB で確かめる。
 *      画面のロール master_edit はこの約束の相手・操作の書き方でしか書けない・#1563 R3 M2・R4 M2。actor はアプリが言う値 = DB は人を確かめられない)
 *   5. 保存を開いていない = 409「切替前」= 何も書かない (変わる項目が無い保存も = #1563 R1 M5)。
 *      開いている = 切替の段階が new_open かつ 持ち主表のハッシュが段階の記録と同じ (ops.master_cutover_state・読めない = 閉) **かつ** 変わる列の持ち主が 'company'
 *      **かつ** env MASTER_EDIT_OPEN = 1 (opts.open)。セットの構成の依頼も持ち主 sku_components が 'company' のときだけ
 *   6. この SKU の NE に取り込む CSV が出ている (作った・確かめた = まだ使える / 申告して確かめ待ち) 列を変える = 409 (人がそのファイルを「使わない」にするまで)
 *   7. セットは導く値 (税率・売上分類・原価) が決まらないなら保存しない (lib/master-set-rules.js の deriveSetCdb。上書き・例外原価があれば通る)。
 *      開いている構成の依頼があれば「今の構成」と「依頼の構成」の両方で確かめる (#1563 R1 H4 = 今の構成の値を壊さない)
 *   8. 変わった列だけ UPDATE。原価は今日からだけ (今の行を昨日で閉じる・今日始まった行は消して入れ直す = 期間を重ねない・0051 の守り)
 *   9. 単品の税率・取扱・原価を変えたら、その単品を含むセットの導く値を同じ取引で計算し直す
 *  10. セットの構成は core.sku_components に書かない = 依頼 (ops.sku_component_requests)。NE の観測が依頼と同じになったら promoteComponentRequest が上げる
 *      TODO (中原さん 2026-10-01「いずれ自動化したい」): 構成品を外す・入れ替えるは今は「NE の画面で手で + NE でやること + 観測が同じになったら上げる」。
 *      NE へ自動で入れる方法 (CSV では外せない) は後で決める。それまではこの流れのまま
 *  🚨 SQLite と NE には書かない (古い表への写しは ④・NE へは翌朝の照合 → CSV / NE の画面)
 */
import crypto from 'node:crypto';
import { normSku } from './sku-norm.js';
import { MASTER_OWNERSHIP, validateOwnership } from '../config/master-ownership.mjs';
import { CSV_LOCK_SQL, SKU_LOCK_SQL, csvApplied } from '../apps/master-decisions/ne-csv-lock.mjs';
import { canonicalSupplierCode } from '../apps/company-db/load/sources.mjs';
import { readCutoverPhase, newEntryWritable, CUTOVER_SHARED_LOCK_SQL, MASTER_WRITE_SHARED_LOCK_SQL } from './master-cutover.mjs';
import { baseGateInTx, lockCutoverSharedInTx } from './master-owner-gate.mjs';
import { deriveSetTaxCdb, deriveSetHandlingCdb, deriveSetCostCdb, deriveSetCdb, compositionEquals } from './master-set-rules.js';

export const COMPANY_ID = 1;
export const SOURCE_SYSTEM = 'portal_master_edit';
export const MAX_COMPONENTS = 20;
export const MAX_QTY = 999;
export const MAX_YEN = 999_999_999;
export const REASON_MAX = 200;
/** 編集の印の形の版 (中に入れるものを変えたら上げる = 古い画面の保存は 409 になる) */
export const TOKEN_VERSION = 'met-4';   // met-3 (⑤-2a): 登録の状態・仕入先の有効 / met-4 (⑤-2b): 仕入先ごとの商品の仕入先の登録の状態 (0053)
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const CTRL_RE = new RegExp(`[\\x00-\\x1f\\x7f${String.fromCharCode(0x2028, 0x2029)}]`);
const TOKEN_RE = /^[0-9a-f]{64}$/;
const VER_RE = /^\d{1,19}$/;
/** 商品名の末尾の資材の印 (D-47 = 資材は梱包アプリで登録する。新しく付けない) */
export const MATERIAL_TAG_RE = /_(白ビ袋|梱機プ|長3封|白プチ|ネコ段|K-44|K-50|K-60|厚紙封|パフ箱|その他)$/;
/** request_id ごとの鍵 ($1 = request_id)。保存の取引で最初に取る */
export const REQUEST_LOCK_SQL = "select pg_advisory_xact_lock(hashtextextended('ops.master_edit_request:' || $1::text, 0))";
/**
 * 新商品の入口の許可の鍵 (共有) を取る = DB の関数 ops.acquire_new_entry_locks (migration 0058・広げる道 §3.10 の鍵の順の 2)。
 * 新商品を作る道 (保存・CSV の build / issue) は request の鍵の直後・段階の鍵の前に呼ぶ (DB の関数も段階の鍵の前にもう一度取る)。
 * 許可を出す / 取り消す / 照合 ② の結果の記録 (排他) と順が 1 つになる。鍵の式は DB だけが持つ (JS に写さない)。
 * 戻り値 = その時点で許可が有効か (表示・早い 409 用。保存の強制は DB の関数)・0058 の前の DB = null (鍵は無い)
 */
export async function acquireNewEntryLocks(db, kind = 'single') {
  if (!(await db.query(`select to_regprocedure('ops.acquire_new_entry_locks(text)') is not null as ok`)).rows[0].ok) return null;
  return (await db.query('select ops.acquire_new_entry_locks($1) as v', [kind])).rows[0].v;
}
/** マスタの書き込みの鍵 (共有) を待つ長さ。夜間ロードが持っている間はこれだけ待って 409 (#1563 仮レビュー M3) */
export const MASTER_WRITE_WAIT = '3s';
/** 構成の依頼を上げる観測の古さの上限 (時間)。観測を受ける窓 (0051 の ops.record_ne_set_observations = 36 時間) と同じ (#1563 仮レビュー M2) */
export const OBSERVATION_MAX_AGE_HOURS = 36;
/** 例外原価 (人が決めた値) の原価の出どころ。セットの原価の計算はこれを上書きしない */
export const OVERRIDE_SOURCES = new Set(['manual', 'override_zero']);
const KNOWN_COST = new Set(['COMPLETE', 'OVERRIDDEN']);

/** 画面に返す誤り (status = HTTP の状態・reason = 機械の理由・extra = 画面に出す材料) */
export class MasterWriteError extends Error {
  constructor(status, reason, message, extra = {}) {
    super(message);
    this.code = 'MASTER_WRITE';
    this.status = status;
    this.reason = reason;
    this.extra = extra;
  }
}
const bad = (message, field, reason = 'invalid_input', extra = {}) => new MasterWriteError(400, reason, message, { field, ...extra });
/**
 * 門 (lib/master-owner-gate.mjs の土台の門・新規開始の門) の断り → MasterWriteError (広げる道 PR-2)。
 * before_cutover = 呼び手の言葉 (beforeCutover(why)・今までと同じ文) / owner_unreadable・new_entry_lease_unreadable = 503 / code_behind・new_entry_closed = 409
 */
/**
 * DB の関数が新規開始を拒んだ (PR-1 の 0058: 許可が無い・期限切れ・取り消し = errcode P0001・「new_entry_closed: …」) → 409 の分かる文。
 * lib の門 (newEntryGateInTx) を通った後に、同じ取引の中で DB の強制が断った場合 (二重の守りの DB の側)
 */
export const NEW_ENTRY_CLOSED_DB_RE = /^new_entry_closed:\s*([\s\S]*)$/;
export function newEntryClosedFromDb(e) {
  const m = e && e.code === 'P0001' ? NEW_ENTRY_CLOSED_DB_RE.exec(String(e.message || '')) : null;
  if (!m) return null;
  return new MasterWriteError(409, 'new_entry_closed', `新商品の登録はまだ開いていません (Company DB が断った: 新商品の開放の許可が無い・期限切れ・取り消し${m[1] ? ` = ${m[1]}` : ''})。何も保存していません`, { cause: 'db' });
}
export function ownerGateError(r, beforeCutover = null) {
  if (r.reason === 'before_cutover' && beforeCutover) return beforeCutover(r.why);
  const extra = { phase: r.phase && r.phase.readable ? r.phase.phase : null, ...(r.code_behind ? { code_behind: r.code_behind } : {}), ...(r.cause ? { cause: r.cause } : {}), ...(r.kind ? { kind: r.kind } : {}) };
  if (r.reason === 'new_entry_closed') return new MasterWriteError(409, r.reason, `新商品の登録はまだ開いていません (${r.why})。何も保存していません`, extra);
  return new MasterWriteError(r.status, r.reason, r.why, extra);
}
/**
 * 原価の期間の重なり (0051 の守り = 23P01) → 409 (500 にしない。#1563 仮レビュー M1)。
 * 前からの重なり (夜間ロードが同じ日に 2 回付け替えた [d, d] と [d, null]) がある SKU で、今日始まった行を入れ直すと当たる。⑥ の前に前からの重なりを掃除する
 */
const isCostOverlap = (e) => !!e && e.code === '23P01' && /sku_cost_overlap/.test(String(e.message || ''));
const costOverlapError = (e) => new MasterWriteError(409, 'cost_overlap',
  '原価の期間が、前からある原価の行と重なっています (夜間の取り込みが同じ日に原価を 2 回付け替えたときなど)。何も保存していません。日をあらためるか、システムの担当に知らせてください',
  { pg_message: String((e && e.message) || '').slice(0, 300) });

/**
 * 画面の保存を DB で始める = 書き込みの約束 (0051 の ops.begin_master_write・#1563 R3 M2・R4 M2)。保存の取引で、行の鍵を取って編集の印を確かめた後・書く前に 1 回。
 *   DB が 段階 new_open・持ち主表のハッシュ・操作を確かめ、書いてよい行 (直す SKU + 含むセット・その商品) を決めて鍵を取り、版 (versions = editVersionsOf(cur)) を
 *   DB の版と比べる (編集の印を DB でも確かめる)。画面のロール master_edit は、この約束の相手・操作の書き方でしか書けない (DB の trigger)。
 *   変更の記録の 誰が・request_id・理由 はここで渡した値 (core.actor_* の設定は使わない)。🚨 actor はアプリが言う値 (DB は人を確かめられない)
 * DB が段階・持ち主表で断った = 409 before_cutover / 版が違う = 409 version_conflict
 * 約束した取引は、同じ取引で約束どおりの保存の記録 done を書かないと commit できない (DB の deferred trigger。no_change も done を書く・失敗は巻き戻す)
 * ⑤-2a の新商品の登録 (まだ無い SKU) は、この約束 (既にある SKU を直す sku_edit) を広げないで、登録だけの security definer の関数にする
 *   (番号を振る → 行を入れる → 保存の記録 done を関数の中の 1 か所で)。この約束を広げるなら直す所 = 約束の形 (まだ無い SKU = sku_id が無い)・
 *   入れた行を約束の相手に結ぶ・入れる列の持ち主のキー・画面のロールの列の INSERT の権限と identity の採番・done を作った SKU に結ぶ (0051 の 8.)
 */
export async function beginMasterWrite(db, { requestId, actor, reason = null, ownership, operation = 'sku_edit', skuId, editToken, payloadHash, versions }) {
  try {
    return (await db.query('select ops.begin_master_write($1::uuid, $2, $3, $4::jsonb, $5, $6::bigint, $7, $8, $9::jsonb) as r',
      [requestId, actor, reason || null, JSON.stringify(ownership), operation, String(skuId), editToken, payloadHash, JSON.stringify(versions)])).rows[0].r;
  } catch (e) {
    const msg = String((e && e.message) || '');
    if (/before_cutover/.test(msg)) {
      throw new MasterWriteError(409, 'before_cutover', `切替前です: まだ NE・/register が正です (DB: ${msg.replace(/^before_cutover:\s*/, '')})。何も保存していません`);
    }
    if (/^version_conflict/.test(msg)) {
      throw new MasterWriteError(409, 'version_conflict', '画面を開いた後に、この商品 (または構成品・仕入先・含むセット・構成の依頼) が変わりました。何も保存していません。画面を開き直してから、もう一度入れてください');
    }
    throw e;
  }
}

/** マスタの書き込みの鍵を共有で取る (夜間ロードが排他で持っている間は MASTER_WRITE_WAIT だけ待って 409)。lock_timeout はこの鍵のときだけ変えて戻す */
export async function lockMasterWriteShared(db) {
  const prev = (await db.query(`select current_setting('lock_timeout') as v`)).rows[0].v;
  await db.query(`select set_config('lock_timeout', $1, true)`, [MASTER_WRITE_WAIT]);
  try {
    await db.query(MASTER_WRITE_SHARED_LOCK_SQL);
  } catch (e) {
    if (e && e.code === '55P03') throw new MasterWriteError(409, 'nightly_load', '夜間の取り込み中です。少し待ってからもう一度 (何も保存していません)');
    throw e;
  }
  await db.query(`select set_config('lock_timeout', $1, true)`, [prev]);
}

/**
 * 画面の欄 → 書く列の持ち主のキー (config/master-ownership.mjs)・NE へどう届くか・NE に取り込む CSV の列 (0040 の col)
 *   ne: csv = 翌朝の照合で NE との差になり CSV で入れる / manual = NE の画面で手で / none = NE へ送らない
 * 🚨 単品の変更で動くセットの導く値 (税率 → セットの税率・取扱 → セットの取扱・原価 → セットの原価) は、単品の欄と同じキー
 * 🚨 セットの構成 (components) は core に書かない (依頼の表だけ) が、持ち主 sku_components が 'company' でないと依頼も受けない (#1563 R1 H1)
 */
export const SINGLE_FIELDS = Object.freeze({
  name:             { label: '名前', keys: ['skus.name', 'products.name'], ne: 'csv', csvCol: 'name' },
  handling:         { label: '取扱区分', keys: ['skus.handling', 'products.status'], ne: 'csv', csvCol: 'handling' },
  parent_code:      { label: '代表 (親)', keys: ['products.parent'], ne: 'csv', csvCol: 'parent' },
  standard_price:   { label: '標準売価', keys: ['skus.standard_price'], ne: 'csv', csvCol: 'standard_price_jpy' },
  cost:             { label: '原価', keys: ['sku_costs'], ne: 'csv', csvCol: 'cost' },
  tax_rate:         { label: '税率', keys: ['skus.tax_rate', 'skus.tax_class'], ne: 'csv', csvCol: 'tax_rate' },
  sales_class:      { label: '売上分類', keys: ['products.sales_class'], ne: 'none' },
  primary_supplier: { label: '代表の仕入先', keys: ['supplier_skus.is_primary'], ne: 'csv', csvCol: 'primary_supplier' },
  shipping_code:    { label: '送料', keys: ['skus.shipping'], ne: 'none' },
  reorder_months:   { label: '推奨保有月数', keys: ['skus.reorder_months'], ne: 'none' },
  // 0053 (⑤-2b・契約 v3 H6): 既にある商品の NE の JAN は CSV にしない (NE の画面で)。新商品の NE 登録の CSV には入る
  jan:              { label: 'JAN', keys: ['external_ids.jan'], ne: 'manual' },
});
/**
 * 新商品の NE 登録の CSV (lib/master-reg-csv.mjs・0053) に入る欄。その商品の CSV を配った後はこの欄を直せない (409 reg_csv_issued)。
 * 🚨 単品の税率はそれを含むセットの CSV (tax_rate) にも入る = 含むセットの CSV も見る
 */
export const REG_CSV_FIELDS = Object.freeze({
  single: Object.freeze(['name', 'handling', 'parent_code', 'standard_price', 'cost', 'tax_rate', 'primary_supplier', 'jan']),
  set: Object.freeze(['name', 'standard_price', 'components']),
});
/** 新規登録の CSV の商品の「まだ終わっていない」状態 */
export const REG_LIVE_STATES = Object.freeze(['built', 'issued', 'import_declared', 'partial']);
/** セットの導く値 (税率・税区分・取扱区分・合計の原価) の持ち主。構成の依頼を上げる (promote) ときはこれも 'company' でないと書けない */
export const SET_DERIVED_KEYS = Object.freeze(['skus.tax_rate', 'skus.tax_class', 'skus.handling', 'sku_costs']);
export const SET_FIELDS = Object.freeze({
  name:                     { label: 'セット名', keys: ['skus.name'], ne: 'csv', csvCol: 'name' },
  standard_price:           { label: '標準売価', keys: ['skus.standard_price'], ne: 'csv', csvCol: 'standard_price_jpy' },
  components:               { label: '構成の依頼', keys: ['sku_components'], ne: 'manual' },
  set_sales_class_override: { label: '売上分類の上書き', keys: ['products.sales_class'], ne: 'none' },
  exception_cost:           { label: '例外原価', keys: ['sku_costs'], ne: 'none' },
  handling_own:             { label: 'セット自身の取扱', keys: ['skus.handling'], ne: 'manual' },
  shipping_code:            { label: '送料', keys: ['skus.shipping'], ne: 'none' },
  reorder_months:           { label: '推奨保有月数', keys: ['skus.reorder_months'], ne: 'none' },
});
export const fieldsOf = (kind) => (kind === 'set' ? SET_FIELDS : kind === 'single' ? SINGLE_FIELDS : {});

/** 欄ごとに書けるか (保存を開いている = open かつ 持ち主が全部 'company')。画面の「切替前」の印に使う。open = 段階 new_open かつ MASTER_EDIT_OPEN */
export function fieldOwnership(kind, ownership = MASTER_OWNERSHIP, open = false) {
  const out = {};
  for (const [f, d] of Object.entries(fieldsOf(kind))) {
    const load = d.keys.filter((k) => ownership[k] !== 'company');
    out[f] = { editable: !!open && load.length === 0, load_keys: load };
  }
  return out;
}

// ─── 日付 (東京) ───
/** 東京の日付 (YYYY-MM-DD)。日本は夏時間が無いので UTC + 9 時間 */
export const jstDate = (now = new Date()) => new Date(now.getTime() + 9 * 3600 * 1000).toISOString().slice(0, 10);

// ─── 入力の形 (DB を読む前に見られるもの) ───
const toHalf = (s) => String(s).replace(/[０-９．，－]/g, (c) => String.fromCharCode(c.charCodeAt(0) - 0xFEE0));
export function intIn(v, min, max, label, field) {
  const s = typeof v === 'number' ? String(v) : toHalf(String(v ?? '')).replace(/[,\s]/g, '');
  if (!/^-?\d+$/.test(s)) throw bad(`${label}は整数で入れてください`, field);
  const n = Number(s);
  if (!Number.isSafeInteger(n) || n < min || n > max) throw bad(`${label}は ${min.toLocaleString('ja-JP')}〜${max.toLocaleString('ja-JP')} の整数で入れてください`, field);
  return n;
}
/** 文字 (前後の空白を除く)。空 = null */
export function textIn(v, { label, field, max }) {
  if (v == null) return null;
  if (typeof v !== 'string') throw bad(`${label}は文字で入れてください`, field);
  const s = v.trim();
  if (!s) return null;
  if ([...s].length > max) throw bad(`${label}は ${max} 字までです`, field);
  if (CTRL_RE.test(s)) throw bad(`${label}に改行や制御文字は入れられません`, field);
  return s;
}
export const blank = (v) => v == null || (typeof v === 'string' && v.trim() === '');
/** 原価 = { jpy, reason } (例外原価は { clear: true, reason } も)。適用日は今日だけ (Codex ⑤-R0 High 6) */
function costIn(v, field, { allowClear }) {
  if (v == null) return null;
  if (typeof v !== 'object' || Array.isArray(v)) throw bad('原価の入れ方が違う', field);
  const reason = textIn(v.reason, { label: '原価の理由', field, max: REASON_MAX });
  if (!reason) throw bad('原価を変えるときは理由を入れてください', field);
  if (allowClear && v.clear === true) return { clear: true, reason };
  return { clear: false, jpy: intIn(v.jpy, 0, MAX_YEN, '原価', field), reason };
}
function componentsIn(v) {
  if (!Array.isArray(v)) throw bad('構成の入れ方が違う', 'components');
  if (v.length < 1 || v.length > MAX_COMPONENTS) throw bad(`構成品は 1〜${MAX_COMPONENTS} 行です`, 'components');
  const seen = new Set();
  return v.map((r, i) => {
    const code = textIn(r?.code, { label: `構成品 ${i + 1} 行目のコード`, field: 'components', max: 60 });
    if (!code) throw bad(`構成品 ${i + 1} 行目のコードが空です`, 'components');
    const qty = intIn(r?.qty, 1, MAX_QTY, `構成品 ${code} の数量`, 'components');
    const k = normSku(code);
    if (seen.has(k)) throw bad(`構成品 ${code} が 2 回あります (同じ品は 1 行にして数量で)`, 'components');
    seen.add(k);
    return { code, qty };
  });
}
/** 欄ごとの形の検査 (新商品の登録 lib/master-register.mjs も同じものを使う) */
export const PARSERS = {
  name: (v) => {
    const s = textIn(v, { label: '名前', field: 'name', max: 255 });
    if (s == null) throw bad('名前が空です', 'name');
    if (/^empty$/i.test(s)) throw bad('名前に「empty」だけは使えません (NE の CSV で「空にする」の意味になる)', 'name');
    return s;
  },
  handling: (v) => { if (!['active', 'discontinued', 'unknown'].includes(v)) throw bad('取扱区分は 取扱中 か 中止', 'handling'); return v; },
  handling_own: (v) => { if (blank(v)) return null; if (!['active', 'discontinued'].includes(v)) throw bad('セット自身の取扱は 取扱中 か 中止', 'handling_own'); return v; },
  parent_code: (v) => textIn(v, { label: '代表 (親) のコード', field: 'parent_code', max: 60 }),
  standard_price: (v) => (blank(v) ? null : intIn(v, 1, MAX_YEN, '標準売価', 'standard_price')),
  tax_rate: (v) => {
    if (blank(v)) return null;
    const x = Number(toHalf(String(v)).replace('%', '').trim());
    if (x === 0.1 || x === 10) return 0.1;
    if (x === 0.08 || x === 8) return 0.08;
    throw bad('税率は 8% か 10% です', 'tax_rate');
  },
  sales_class: (v) => (blank(v) ? null : intIn(v, 1, 4, '売上分類', 'sales_class')),
  set_sales_class_override: (v) => (blank(v) ? null : intIn(v, 1, 4, '売上分類の上書き', 'set_sales_class_override')),
  primary_supplier: (v) => textIn(v, { label: '代表の仕入先', field: 'primary_supplier', max: 20 }),
  shipping_code: (v) => textIn(v, { label: '送料コード', field: 'shipping_code', max: 20 }),
  reorder_months: (v) => {
    if (blank(v)) return null;
    const s = toHalf(String(v)).trim();
    if (!/^\d{1,2}(\.\d)?$/.test(s) || Number(s) > 60) throw bad('推奨保有月数は 0〜60 (小数は 1 桁まで)', 'reorder_months');
    return Number(s);
  },
  cost: (v) => costIn(v, 'cost', { allowClear: false }),
  exception_cost: (v) => costIn(v, 'exception_cost', { allowClear: true }),
  components: (v) => componentsIn(v),
  /** JAN (単品)。配列かカンマ・空白区切り。空 = JAN なし。8 桁か 13 桁 + チェック数字・5 つまで */
  jan: (v) => {
    const list = v == null ? [] : Array.isArray(v) ? v : String(v).split(/[\s,、，]+/);
    const out = [];
    for (const x of list) {
      const s = toHalf(String(x ?? '')).trim();
      if (!s) continue;
      if (!janValid(s)) throw bad(`JAN ${s} は 8 桁か 13 桁の数字で、チェック数字が合うものを入れてください`, 'jan');
      if (!out.includes(s)) out.push(s);
    }
    if (out.length > 5) throw bad('JAN は 5 つまでです', 'jan');
    return out;
  },
};
/** JAN の形 (8 桁か 13 桁 + チェック数字) */
export function janValid(s) {
  if (!/^\d{8}$|^\d{13}$/.test(String(s))) return false;
  const d = String(s).split('').map(Number);
  const check = d.pop();
  const sum = d.reverse().reduce((a, x, i) => a + x * (i % 2 === 0 ? 3 : 1), 0);
  return (10 - (sum % 10)) % 10 === check;
}

/** 画面から来た保存の中身を確かめて、決まった形にする (DB を読まない部分。ここで落ちた保存は記録しない) */
export function parseSaveRequest(input) {
  const actor = String(input?.actor ?? '').trim().toLowerCase();
  if (!actor || actor.length > 320 || CTRL_RE.test(actor)) throw bad('保存する人 (ログインのメール) が分からない', 'actor');
  const requestId = String(input?.requestId ?? '').trim().toLowerCase();
  if (!UUID_RE.test(requestId)) throw bad('保存の番号 (request_id) の形が違う。画面を開き直してください', 'request_id');
  const code = textIn(input?.code, { label: '商品コード', field: 'code', max: 60 });
  if (!code) throw bad('商品コードが空です', 'code');
  const reason = textIn(input?.reason, { label: '理由', field: 'reason', max: REASON_MAX });
  const s = input?.seen || {};
  const token = String(s.token ?? '').trim().toLowerCase();
  if (!TOKEN_RE.test(token)) throw bad('画面が読んだ「編集の印」が無い。画面を開き直してください', 'seen');
  const seen = { token, event_id: VER_RE.test(String(s.event_id ?? '')) ? String(s.event_id) : null };
  const raw = input?.values;
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) throw bad('保存する値が無い', 'values');
  const values = {};
  for (const [k, v] of Object.entries(raw)) {
    if (!Object.prototype.hasOwnProperty.call(PARSERS, k)) throw bad(`知らない項目: ${k}`, k);
    if (v === undefined) continue;
    values[k] = PARSERS[k](v);
  }
  return { actor, requestId, code, reason, seen, values };
}

/** 順番によらない JSON (同じ中身の比べ方) */
export function stable(v) {
  if (Array.isArray(v)) return `[${v.map(stable).join(',')}]`;
  if (v && typeof v === 'object') return `{${Object.keys(v).filter((k) => v[k] !== undefined).sort().map((k) => `${JSON.stringify(k)}:${stable(v[k])}`).join(',')}}`;
  return JSON.stringify(v ?? null);
}
export const sha256 = (s) => crypto.createHash('sha256').update(s).digest('hex');
export const payloadHashOf = (req) => sha256(stable({ code: normSku(req.code), reason: req.reason, seen: req.seen, values: req.values }));

/**
 * 新しい商品コードの形 (⑤-2 の新商品の登録で使う。Company DB構想 14 §3・中原さん 2026-10-01「人が入れて画面で検査・新規は大文字を禁止」)。
 * 形だけを見る (DB を読まない)。一意・使ったことが無い・NE の元のコード (0041)・代表の名札に無い、は ⑤-2 で DB を見て確かめる
 * 戻り値 { ok, code, message }
 */
export function validateNewSkuCode(raw) {
  const no = (message) => ({ ok: false, code: null, message });
  const s = typeof raw === 'string' ? raw : '';
  if (!s.trim()) return no('商品コードを入れてください');
  if (s !== s.trim()) return no('商品コードの前後に空白があります');
  if (/[A-Z]/.test(s)) return no('大文字は使えません (NE は大文字と小文字を区別するので、新しいコードは小文字だけ)');
  if (!/^[a-z0-9_-]{1,30}$/.test(s)) return no('使える文字は小文字の英字・数字・- と _ だけ (30 字まで)');
  if (/^set-/.test(s)) return no('set- で始まるコードは使えません (product-hub の仮のコード)');
  return { ok: true, code: s, message: null };
}

// ─── 今の値を読む (画面と保存で同じ読み方) ───
const num = (v) => (v == null ? null : Number(v));
/** その日を覆う原価の行 (0049 と同じ選び方 = 始まりが遅い → 作った時刻が遅い → 番号が大きい) */
const COST_AS_OF = `left join lateral (
    select y.sku_cost_id, y.cost_jpy, y.cost_source, y.cost_status, y.valid_from from core.sku_costs y
     where y.sku_id = %SKU% and y.valid_from <= %DAY%::date and (y.valid_to is null or y.valid_to >= %DAY%::date)
     order by y.valid_from desc, y.created_at desc, y.sku_cost_id desc limit 1) %AS% on true`;
export const costAsOfJoin = (skuExpr, dayExpr, alias) => COST_AS_OF.replaceAll('%SKU%', skuExpr).replaceAll('%DAY%', dayExpr).replaceAll('%AS%', alias);

async function regclass(db, name) {
  return (await db.query('select to_regclass($1) is not null as ok', [name])).rows[0].ok;
}

/** 構成品の SKU の値 (導く値の材料 + 編集の印に入れる版)。ids = sku_id の配列 → Map<sku_id, {...}> */
export async function componentFacts(db, ids, today) {
  if (!ids.length) return new Map();
  const rows = (await db.query(`select k.sku_id::text as sku_id, k.code, k.name, k.sku_kind, k.tax_rate::text as tax_rate, k.handling, k.version::text as version,
        k.standard_price_jpy::text as standard_price, p.sales_class, p.version::text as product_version, x.cost_jpy::text as cost_jpy, x.cost_status
      from core.skus k left join core.products p on p.product_id = k.product_id
      ${costAsOfJoin('k.sku_id', '$2', 'x')}
     where k.sku_id = any($1::bigint[])`, [ids.map(String), today])).rows;
  return new Map(rows.map((r) => [r.sku_id, {
    sku_id: r.sku_id, code: r.code, name: r.name, sku_kind: r.sku_kind, tax_rate: num(r.tax_rate), handling: r.handling, version: r.version, product_version: r.product_version ?? null,
    standard_price: num(r.standard_price), sales_class: num(r.sales_class), cost_jpy: KNOWN_COST.has(r.cost_status) ? Number(r.cost_jpy) : null,
  }]));
}

/** 1 つの SKU の今の値 (単品は商品の行・親・JAN も・セットは構成品と開いている構成の依頼も)。today = 原価を見る日 (東京) */
export async function readCurrent(db, skuId, today) {
  const r = (await db.query(`select s.sku_id::text as sku_id, s.code, s.code_norm, s.sku_kind, s.name, s.tax_rate::text as tax_rate, s.tax_class, s.handling,
        s.standard_price_jpy::text as standard_price, s.shipping_code, s.shipping_method, s.shipping_cost_jpy::text as shipping_cost,
        s.reorder_months::text as reorder_months, s.set_sales_class_override, s.handling_own, s.version::text as version,
        p.product_id::text as product_id, p.name as product_name, p.sales_class, p.status as product_status,
        p.parent_product_id::text as parent_product_id, p.parent_set_by, p.version::text as product_version,
        p.expiry_managed, p.inbound_date_managed, pp.display_code as parent_code, pp.name as parent_name, pp.version::text as parent_version
      from core.skus s
      left join core.products p on p.product_id = s.product_id
      left join core.products pp on pp.product_id = p.parent_product_id
     where s.sku_id = $1`, [skuId])).rows[0];
  if (!r) return null;
  const cur = {
    ...r,
    tax_rate: num(r.tax_rate), standard_price: num(r.standard_price), shipping_cost: num(r.shipping_cost), reorder_months: num(r.reorder_months),
    sales_class: num(r.sales_class), set_sales_class_override: num(r.set_sales_class_override),
  };
  const hasSupReg = await regclass(db, 'ops.supplier_registrations');   // 0053 (⑤-2b)。行が無い = 前からある仕入先 (reg_state null)
  cur.supplier_rows = (await db.query(`select x.supplier_id::text as supplier_id, s.code, x.version::text as version, x.is_primary, s.active, s.version::text as supplier_version,
        ${hasSupReg ? '(select r.state from ops.supplier_registrations r where r.supplier_id = x.supplier_id)' : 'null::text'} as reg_state
     from core.supplier_skus x join core.suppliers s on s.supplier_id = x.supplier_id where x.sku_id = $1 order by x.supplier_id`, [skuId])).rows;
  cur.primary_supplier = cur.supplier_rows.filter((x) => x.is_primary).map((x) => x.code).sort()[0] ?? null;
  cur.cost_rows = (await db.query(`select sku_cost_id::text as id, cost_jpy::text as cost_jpy, cost_source, cost_status, valid_from::text as valid_from, valid_to::text as valid_to
     from core.sku_costs where sku_id = $1 order by sku_cost_id`, [skuId])).rows.map((c) => ({ ...c, cost_jpy: Number(c.cost_jpy) }));
  const open = cur.cost_rows.find((c) => c.valid_to == null);
  cur.open_cost = open ? { ...open } : null;
  const asOf = (await db.query(`select c.sku_cost_id::text as id, c.cost_jpy::text as cost_jpy, c.cost_source, c.cost_status, c.valid_from::text as valid_from
     from (select $1::bigint as sku_id) s ${costAsOfJoin('s.sku_id', '$2', 'c')}`, [skuId, today])).rows[0];
  cur.cost_today = asOf && asOf.id ? { ...asOf, cost_jpy: Number(asOf.cost_jpy) } : null;
  cur.jan_rows = cur.product_id ? (await db.query(`select external_id_row::text as id, external_value, valid_to::text as valid_to from core.external_ids
     where entity_type = 'product' and entity_id = $1 and system = 'jan' and id_kind = 'jan' order by external_id_row`, [cur.product_id])).rows : [];
  // 登録の状態 (0052)。表が無い DB (0052 の前) = null。行が無い = null (切替の前の商品・backfill の前)
  cur.registration = (await regclass(db, 'ops.master_registrations'))
    ? ((await db.query(`select state, origin, state_changed_at::text as state_changed_at, state_changed_by from ops.master_registrations where sku_id = $1`, [skuId])).rows[0] ?? null)
    : null;
  cur.components = [];
  cur.parent_sets = [];
  cur.parent_set_ids = [];
  cur.component_request = null;
  if (cur.sku_kind === 'set') {
    const rows = (await db.query(`select c.child_sku_id::text as child_sku_id, c.qty, c.sort_order, c.source, k.code_norm
        from core.sku_components c join core.skus k on k.sku_id = c.child_sku_id
       where c.parent_sku_id = $1 order by c.sort_order, k.code_norm`, [skuId])).rows;
    const reqRow = (await regclass(db, 'ops.sku_component_requests'))
      ? (await db.query(`select component_request_id::text as id, rows, base_rows, reason, requested_by, created_at::text as created_at, (extract(epoch from created_at) * 1000)::float8 as created_ms
           from ops.sku_component_requests where set_sku_id = $1 and status = 'open'`, [skuId])).rows[0] : null;
    const ids = [...rows.map((x) => x.child_sku_id), ...(reqRow ? reqRow.rows.map((x) => String(x.sku_id)) : [])];
    const facts = await componentFacts(db, [...new Set(ids)], today);
    cur.components = rows.map((c, i) => ({ ...facts.get(c.child_sku_id), child_sku_id: c.child_sku_id, qty: Number(c.qty), sort_order: Number(c.sort_order), position: i + 1, source: c.source }));
    if (reqRow) {
      cur.component_request = {
        id: reqRow.id, reason: reqRow.reason, requested_by: reqRow.requested_by, created_at: reqRow.created_at, created_ms: Number(reqRow.created_ms), base_rows: reqRow.base_rows,
        rows: reqRow.rows.map((x) => ({ ...facts.get(String(x.sku_id)), child_sku_id: String(x.sku_id), code: facts.get(String(x.sku_id))?.code ?? x.code, qty: Number(x.qty), sort: Number(x.sort) })),
      };
    }
  } else {
    cur.parent_sets = (await db.query(`select c.parent_sku_id::text as id, s.version::text as version from core.sku_components c join core.skus s on s.sku_id = c.parent_sku_id
       where c.child_sku_id = $1 order by c.parent_sku_id`, [skuId])).rows;
    cur.parent_set_ids = cur.parent_sets.map((x) => x.id);
  }
  return cur;
}

/**
 * 編集の印 = 版つきで、保存の確かめと導く値に使う行の全部と「行が無いこと」のハッシュ (Codex ⑤-R1 H4):
 * SKU・商品・親の商品 (version) / 仕入先ごとの商品 (全部の行) / 原価 (全部の行) / JAN (全部の行・見るだけでも) / 構成の行 / 構成品の SKU・商品の version /
 * 開いている構成の依頼 / この単品を含むセットの version / 登録の状態 (0052)。どれかの行が増えた・消えた・変わった = 違う印
 */
export function editTokenOf(cur) {
  const comp = (list) => list.map((c) => [c.child_sku_id, c.qty, c.sort_order ?? c.sort, c.source ?? null, c.version ?? null, c.product_version ?? null]);
  return sha256(stable({
    v: TOKEN_VERSION,
    sku: [cur.sku_id, cur.version],
    product: cur.product_id ? [cur.product_id, cur.product_version] : null,
    parent: cur.parent_product_id ? [cur.parent_product_id, cur.parent_version ?? null, cur.parent_set_by ?? null] : [null, cur.parent_set_by ?? null],
    suppliers: cur.supplier_rows.map((x) => [x.supplier_id, x.version, !!x.is_primary, !!x.active, x.supplier_version, x.reg_state ?? null]),
    costs: cur.cost_rows.map((c) => [c.id, c.cost_jpy, c.cost_source, c.cost_status, c.valid_from, c.valid_to]),
    jan: cur.jan_rows.map((j) => [j.id, j.external_value, j.valid_to]),
    components: comp(cur.components),
    component_request: cur.component_request ? [cur.component_request.id, comp(cur.component_request.rows)] : null,
    parent_sets: cur.parent_sets.map((x) => [x.id, x.version]),
    registration: cur.registration ? [cur.registration.state, cur.registration.state_changed_at] : null,
  }));
}

/**
 * 編集の印が見ている行の版 (DB でも確かめる形。0051 の ops.master_edit_versions(sku_id) と同じ形・同じ並び。#1563 R4 M2)。
 * 原価・構成の行の変更は SKU の version を上げる (0026) = sku に入る。JAN は保存で書かないので入れない
 */
export function editVersionsOf(cur) {
  const sorted = (a) => [...a].sort();
  const facts = (list) => sorted(list.map((x) => `${x.child_sku_id}:${x.version ?? ''}:${x.product_version ?? ''}`));
  return {
    sku: `${cur.sku_id}:${cur.version}`,
    product: cur.product_id ? `${cur.product_id}:${cur.product_version}` : null,
    suppliers: sorted(cur.supplier_rows.map((x) => `${x.supplier_id}:${x.version}:${x.supplier_version}`)),
    parent_sets: sorted(cur.parent_sets.map((x) => `${x.id}:${x.version}`)),
    components: facts(cur.components),
    request: cur.component_request ? String(cur.component_request.id) : null,
    request_components: cur.component_request ? facts(cur.component_request.rows) : [],
  };
}

/** セットの導く値 (画面に出す)。今の構成 (core) と、開いている依頼の構成の両方 */
export function setDerivations(cur) {
  const opts = { override: cur.set_sales_class_override, handlingOwn: cur.handling_own, currentHandling: cur.handling, exceptionCost: !!cur.open_cost && OVERRIDE_SOURCES.has(cur.open_cost.cost_source) };
  const priceSum = (rows) => (rows.length && rows.every((c) => c.standard_price != null) ? rows.reduce((a, c) => a + c.standard_price * c.qty, 0) : null);
  return {
    current: { ...deriveSetCdb(cur.components, opts), price_sum: priceSum(cur.components) },
    requested: cur.component_request ? { ...deriveSetCdb(cur.component_request.rows, opts), price_sum: priceSum(cur.component_request.rows) } : null,
  };
}

/** その間の変更 (画面を開いた後の events.master_change_events。SKU・商品・構成・原価・仕入先ごとの商品) */
export async function changesSince(db, { skuId, productId = null, sinceEventId = null, limit = 50 }) {
  return (await db.query(`select e.event_id::text as event_id, e.operation, e.entity_type, e.attribute, e.old_value, e.new_value,
        e.actor_type, e.actor_id, e.source_system, e.reason_text, e.recorded_at::text as recorded_at
      from events.master_change_events e
     where ($3::bigint is null or e.event_id > $3::bigint)
       and ((e.entity_type = 'sku' and e.entity_id = $1::bigint)
         or (e.entity_type = 'product' and $2::bigint is not null and e.entity_id = $2::bigint)
         or (e.entity_type = 'sku_component' and (e.entity_key ->> 'parent_sku_id')::bigint = $1::bigint)
         or (e.entity_type = 'sku_cost' and e.entity_id in (select sku_cost_id from core.sku_costs where sku_id = $1::bigint))
         or (e.entity_type = 'supplier_sku' and (e.entity_key ->> 'sku_id')::bigint = $1::bigint)
         or (e.entity_type = 'external_id' and $2::bigint is not null and e.entity_id in (select x.external_id_row from core.external_ids x
               where x.entity_type = 'product' and x.entity_id = $2::bigint and x.system = 'jan')))
     order by e.event_id desc limit $4`, [skuId, productId, sinceEventId, limit])).rows.reverse();
}

// ─── 保存 ───
/**
 * 1 つの SKU を保存する。input = { actor, requestId, code, reason?, seen: { token, event_id? }, values: {...} }
 * opts = { open (MASTER_EDIT_OPEN = 1 か)・ownership (試験で差し替え)・now (試験で「今日」を決める)・shippingRates: Map<送料コード, { method, cost }> | null・
 *          beforeCommit (試験だけ: commit の直前に待つ) }
 * 戻り値 = 結果 (保存の記録にも同じもの)。誤りは MasterWriteError (status 400 / 404 / 409 / 503)
 */
export async function saveSku(db, input, opts = {}) {
  // 持ち主表 = 取引の中で読む DB の active (広げる道 PR-2)。opts.ownership (配った config の持ち主表) はもう使わない
  const req = parseSaveRequest(input);
  const ctx = {
    ownership: null, open: opts.open === true, payloadHash: payloadHashOf(req), shippingRates: opts.shippingRates ?? null,
    actor: req.actor, reason: req.reason, requestId: req.requestId, notes: [], today: null, startedAt: null, skuId: null,
  };
  await db.query('begin');
  try {
    // 「今日」と受けた時刻は取引の初めに 1 回だけ (日をまたいでも取引の中では変えない)
    const t = (await db.query(`select now()::text as started, (now() at time zone 'Asia/Tokyo')::date::text as today`)).rows[0];
    ctx.startedAt = t.started;
    ctx.today = opts.now ? jstDate(opts.now) : t.today;
    const out = await saveInTx(db, req, ctx);
    if (out.replay) { await db.query('rollback'); return out.replay; }
    await db.query(`insert into ops.master_edit_requests (request_id, company_id, operation, target_code, sku_id, actor_id, payload_hash, status, result, started_at)
       values ($1, $2, 'sku_edit', $3, $4, $5, $6, 'done', $7::jsonb, $8::timestamptz)`,
    [req.requestId, COMPANY_ID, req.code, ctx.skuId, req.actor, ctx.payloadHash, JSON.stringify(out.result), ctx.startedAt]);
    // JAN は JAN の約束で (同じ取引・保存の約束の done の後)。落ちたら保存ごと巻き戻る
    if (ctx.janEdit) out.result.jan = await applyJanEdit(db, req, ctx);
    if (opts.beforeCommit) await opts.beforeCommit();
    await db.query('commit');
    return out.result;
  } catch (caught) {
    try { await db.query('rollback'); } catch { /* 接続が切れていれば rollback も失敗する */ }
    // 原価の期間の重なり (前からの重なりに当たった) = 409 (500 にしない・記録にも 409 で残す)
    const e = isCostOverlap(caught) ? costOverlapError(caught) : caught;
    // 同じ request_id を別の取引が先に書いた (鍵を取らない書き手が居た場合の守り) = 500 にしない。残った記録を読み直して返す (#1563 R1 M6)
    if (e && e.code === '23505' && /master_edit_requests_pkey/.test(String(e.constraint || e.message || ''))) {
      const prev = (await db.query('select actor_id, payload_hash, status, result, error from ops.master_edit_requests where request_id = $1', [req.requestId])).rows[0];
      return replayOrThrow(prev, req, ctx);
    }
    // 巻き戻した保存も記録に残す (同じ request_id の押し直しに同じ誤りを返す)。同じ番号の違う中身・残した誤りを返しただけのときは残さない
    if (ctx.startedAt && !(e instanceof MasterWriteError && (e.reason === 'request_id_reused' || e.extra?.replayed))) {
      const locked = e && e.code === '55P03';
      const err = e instanceof MasterWriteError
        ? { status: e.status, reason: e.reason, message: e.message, extra: e.extra }
        : { status: locked ? 409 : 500, reason: locked ? 'locked' : 'error', message: locked ? 'ほかの処理が同じ商品を使っていました' : 'サーバーエラー', pg_code: e && e.code ? String(e.code) : null };
      const prev = await recordFailure(db, req, ctx, err);
      // 待っている間に同じ request_id の保存が終わっていた = その結果を返す (同じ番号で「失敗」と「成功」を両方残さない。#1563 R2 M6)
      if (prev) return replayOrThrow(prev, req, ctx);
    }
    throw e;
  }
}

/**
 * 失敗の記録を新しい取引で書く: 同じ request_id の鍵を取り、既に結果があれば書かない (その結果を返す)。書けなければ null (記録なし・誤りはそのまま上げる)
 */
async function recordFailure(db, req, ctx, err) {
  try {
    await db.query('begin');
    await db.query(REQUEST_LOCK_SQL, [req.requestId]);
    const prev = (await db.query('select actor_id, payload_hash, status, result, error from ops.master_edit_requests where request_id = $1', [req.requestId])).rows[0];
    if (!prev) {
      await db.query(`insert into ops.master_edit_requests (request_id, company_id, operation, target_code, sku_id, actor_id, payload_hash, status, error, started_at)
         values ($1, $2, 'sku_edit', $3, $4, $5, $6, 'failed', $7::jsonb, $8::timestamptz)`,
      [req.requestId, COMPANY_ID, req.code, ctx.skuId, req.actor, ctx.payloadHash, JSON.stringify(err), ctx.startedAt]);
    }
    await db.query('commit');
    return prev || null;
  } catch (e2) {
    try { await db.query('rollback'); } catch { /* */ }
    console.error(`[master-write] 失敗の記録を残せなかった: ${e2 && e2.message}`);
    return null;
  }
}

/** 残っている同じ request_id の記録 → 結果を返す / 同じ誤りを投げる / 中身が違えば 409 */
function replayOrThrow(prev, req, ctx) {
  if (!prev || prev.actor_id !== req.actor || prev.payload_hash !== ctx.payloadHash) {
    throw new MasterWriteError(409, 'request_id_reused', '同じ保存の番号 (request_id) で違う中身が来ました。画面を開き直してください');
  }
  if (prev.status === 'done') return { ...prev.result, replayed: true };
  const x = prev.error || {};
  throw new MasterWriteError(x.status || 500, x.reason || 'error', x.message || '前の保存は失敗しました', { ...(x.extra || {}), replayed: true });
}

/** SKU ごとの鍵 (sku_id の順)。CSV の鍵より前に取る */
async function lockSkuKeys(db, ids) {
  const list = [...new Set(ids.map(String))].sort((a, b) => (BigInt(a) < BigInt(b) ? -1 : BigInt(a) > BigInt(b) ? 1 : 0));
  for (const id of list) await db.query(SKU_LOCK_SQL, [id]);
  return list;
}
async function lockSupplierRows(db, skuId, primaryCode) {
  const want = primaryCode ? canonicalSupplierCode(primaryCode) : null;
  const ids = (await db.query(`select supplier_id::text as id from core.supplier_skus where sku_id = $1
     union select supplier_id::text from core.suppliers where company_id = $2 and $3::text is not null and code_norm = core.norm_code($3::text)`, [skuId, COMPANY_ID, want])).rows.map((r) => r.id);
  if (ids.length) await db.query('select core.lock_suppliers_for_share($1::bigint[])', [ids]);   // 0051 の関数 (画面のロールに仕入先の update を渡さない)
}
async function lockSkuRows(db, ids) {
  const list = [...new Set(ids.map(String))];
  if (list.length) await db.query('select sku_id from core.skus where sku_id = any($1::bigint[]) order by sku_id for update', [list]);
}

async function saveInTx(db, req, ctx) {
  // 1. 記録用の設定 (取引を出れば消える)
  await db.query(`select set_config('core.actor_type', 'human', true), set_config('core.actor_id', $1, true), set_config('core.source_system', $2, true),
      set_config('core.request_id', $3, true), set_config('core.reason', $4, true), set_config('core.run_id', '', true)`,
  [req.actor, SOURCE_SYSTEM, req.requestId, req.reason || '']);
  // 2a. request_id の鍵 → 同じ request_id (違う SKU への同じ番号も、ここで並んでから読む)
  await db.query(REQUEST_LOCK_SQL, [req.requestId]);
  const prev = (await db.query('select actor_id, payload_hash, status, result, error from ops.master_edit_requests where request_id = $1', [req.requestId])).rows[0];
  if (prev) return { replay: replayOrThrow(prev, req, ctx) };
  // 2b. 切替の段階の共有の鍵 (段階を変える取引・widen と並ぶ = この取引の中で段階と持ち主は変わらない)。55P03 は 1 回だけ取り直す (設計 M8)
  await lockCutoverSharedInTx(db);
  // 鍵の前に種類・今の代表・含むセットを読む (鍵は取らない)
  const pre = (await db.query(`select s.sku_id::text as sku_id, s.sku_kind, p.product_id::text as product_id, pp.display_code as parent_code
      from core.skus s left join core.products p on p.product_id = s.product_id left join core.products pp on pp.product_id = p.parent_product_id
     where s.company_id = $1 and s.code_norm = core.norm_code($2)`, [COMPANY_ID, req.code])).rows[0];
  if (!pre) throw new MasterWriteError(404, 'not_found', `商品コード ${req.code} は Company DB にありません`);
  ctx.skuId = pre.sku_id;
  // 2c. 保存を開いているか (段階・持ち主表のハッシュ・MASTER_EDIT_OPEN) を、ほかの鍵を取る前に見る (#1563 仮レビュー Low 1)。
  //     変わる項目が無い保存も先に見る (切替前の「変わりなし」も 409 = 記録に done を残さない・#1563 R1 M5)。列ごとの持ち主は差が分かってから (5.)
  //     🆕 広げる道 PR-2: 持ち主表 = DB の active (段階の鍵の後に読む)・このコードが扱えない C のキー = 409 code_behind・読めない = 503 (lib/master-owner-gate.mjs の土台の門)
  const gate = await baseGateInTx(db, { open: ctx.open });
  const phase = gate.phase;
  if (!gate.ok) {
    const fields = Object.keys(req.values).map((k) => fieldsOf(pre.sku_kind)[k]?.label).filter(Boolean);
    throw ownerGateError(gate, (why) => new MasterWriteError(409, 'before_cutover', `切替前です: ${fields.length ? fields.join('・') + ' は' : ''}まだ NE・/register が正です (${why})。何も保存していません`,
      { load_keys: [], fields, open: ctx.open, phase: phase.readable ? phase.phase : null }));
  }
  ctx.ownership = gate.ownership;
  // 2d. マスタの書き込みの鍵 (共有・夜間ロードが持っていれば短く待って 409)
  await lockMasterWriteShared(db);
  const parents = pre.sku_kind === 'single'
    ? (await db.query('select parent_sku_id::text as id from core.sku_components where child_sku_id = $1', [pre.sku_id])).rows.map((x) => x.id) : [];
  // 2e. 鍵: SKU ごと (自分 + 含むセット) → 親子 (代表を変えるときだけ・商品の行より先) → CSV → 行
  const locked = new Set(await lockSkuKeys(db, [pre.sku_id, ...parents]));
  let parentLocked = false;
  if (pre.sku_kind === 'single' && req.values.parent_code !== undefined && normSku(req.values.parent_code ?? '') !== normSku(pre.parent_code ?? '')) {
    await db.query(`select set_config('core.parent_protocol', '1', true), pg_advisory_xact_lock(core.parent_lock_key())`);
    parentLocked = true;
  }
  await db.query(CSV_LOCK_SQL);
  if (pre.product_id) await db.query('select 1 from core.products where product_id = $1 for update', [pre.product_id]);
  await lockSkuRows(db, [pre.sku_id, ...parents]);
  // 仕入先の行 (この SKU の仕入先 + 代表にしようとしている仕入先) を共有の鍵で (仕入先を止める処理と並ぶ・#1563 R2 M5)。行の鍵の順 = 商品 → SKU → 仕入先 (id の順)
  await lockSupplierRows(db, pre.sku_id, req.values.primary_supplier);
  // 4. 全部を読み直す
  const cur = await readCurrent(db, pre.sku_id, ctx.today);
  if (cur.parent_set_ids.some((id) => !locked.has(id))) {
    // 鍵を取る前に読んだ後で含むセットが増えた (構成の依頼の昇格など) = 読み直してもらう
    throw new MasterWriteError(409, 'retry', 'この商品を含むセットがちょうど変わりました。何も保存していません。画面を開き直してください');
  }
  if (cur.sku_kind === 'exception') throw new MasterWriteError(400, 'exception_sku', '例外の SKU (NE に無い商品) はこの画面では直せません');
  if (cur.registration?.state === 'cancelled') throw new MasterWriteError(400, 'cancelled_sku', 'この商品は登録をやめた (cancelled) ので直せません');
  const allowed = fieldsOf(cur.sku_kind);
  for (const k of Object.keys(req.values)) {
    if (!allowed[k]) throw bad(`「${k}」は${cur.sku_kind === 'set' ? 'セット' : '単品'}では直せません`, k);
  }
  if (editTokenOf(cur) !== req.seen.token) {
    const events = await changesSince(db, { skuId: cur.sku_id, productId: cur.product_id, sinceEventId: req.seen.event_id });
    throw new MasterWriteError(409, 'version_conflict',
      '画面を開いた後に、ほかの人か夜間の処理がこの商品 (または構成品・仕入先・原価) を変えました。何も保存していません。画面を開き直してから、もう一度入れてください', { events });
  }
  // 4b. DB でも保存を始める = 書き込みの約束 (段階・持ち主表・操作・相手の行・版 = 編集の印を DB で確かめる。画面のロールはこれの後・この相手にしか書けない・#1563 R3 M2・R4 M2)
  await beginMasterWrite(db, { requestId: req.requestId, actor: req.actor, reason: req.reason, ownership: ctx.ownership,
    operation: 'sku_edit', skuId: cur.sku_id, editToken: req.seen.token, payloadHash: ctx.payloadHash, versions: editVersionsOf(cur) });
  // 差 (変わった列だけ)
  const changes = cur.sku_kind === 'single' ? await diffSingle(db, cur, req.values, ctx) : await diffSet(db, cur, req.values, ctx);
  // 5. 変わる列の持ち主が全部 'company' か (段階・持ち主表のハッシュ・MASTER_EDIT_OPEN は 2c で見た = この取引の中で段階は変わらない)
  const needKeys = [...new Set(changes.flatMap((c) => c.keys))];
  const loadKeys = needKeys.filter((k) => ctx.ownership[k] !== 'company');
  if (loadKeys.length) {
    const fields = changes.filter((c) => c.keys.some((k) => loadKeys.includes(k))).map((c) => c.label);
    throw new MasterWriteError(409, 'before_cutover', `切替前です: ${fields.join('・')} はまだ NE・/register が正です (Company DB では直せません)。何も保存していません`,
      { load_keys: loadKeys, fields, open: ctx.open, phase: phase.phase });
  }
  if (!changes.length) return { result: { ok: true, code: cur.code, kind: cur.sku_kind, request_id: req.requestId, no_change: true, changed: [], derived: [], ne_steps: [], warnings: [] } };
  if (changes.some((c) => c.field === 'parent_code') && !parentLocked) {
    throw new MasterWriteError(409, 'retry', '代表 (親) がちょうど変わったところでした。何も保存していません。画面を開き直してください');
  }
  // 6. NE に取り込む CSV が出ている列は変えない (人が「使わない」にするまで)
  const csvCols = [...new Set(changes.map((c) => fieldsOf(cur.sku_kind)[c.field]?.csvCol).filter(Boolean))];
  const issued = await issuedCsv(db, cur.code_norm, csvCols);
  if (issued.length) {
    const ids = [...new Set(issued.map((x) => x.export_id))];
    throw new MasterWriteError(409, 'csv_issued',
      `この商品の ${[...new Set(issued.map((x) => x.col))].join('・')} が入った NE に取り込む CSV (ファイル ${ids.map((i) => `#${i}`).join('・')}) が出ています。何も保存していません。`
      + '「マスタの判断 → NE に取り込む CSV」でそのファイルを「使わない」にしてから直してください (取り込んだと申告したファイルは、翌朝の照合の確かめが終わるまで待ってください)',
      { exports: issued });
  }
  // 6b. 新商品の NE 登録の CSV (0053・契約 v3 H5): 配った後 = 409 / 作っただけ = そのファイルを閉じる (作り直す)。単品の税率はそれを含むセットの CSV にも入る
  const regFields = REG_CSV_FIELDS[cur.sku_kind] || [];
  const regTouched = changes.filter((c) => regFields.includes(c.field));
  // 単品の税率を変える = その単品を含むセット (今の構成 + 開いている構成の依頼 = 新しいセットは依頼だけ) の CSV の tax_rate も古くなる
  const taxParents = cur.sku_kind === 'single' && changes.some((c) => c.field === 'tax_rate') ? [...cur.parent_set_ids, ...await requestParentSets(db, cur.sku_id)] : [];
  const regSkus = regTouched.length ? [cur.sku_id, ...taxParents] : [];
  const reg = await guardRegCsvOnSave(db, { skuIds: regSkus, actor: req.actor, what: regTouched.map((c) => c.label).join('・') });
  // 7. セットの導く値が決まるか (保存の後の姿で)
  const setWarnings = cur.sku_kind === 'set' ? checkSetDerivable(cur, changes) : [];
  // 8〜10. 書く
  const applied = cur.sku_kind === 'single' ? await applySingle(db, cur, changes, ctx) : await applySet(db, cur, changes, ctx);
  // 🚨 既にある商品の NE に取り込む CSV (0040) は 6. で「変える列の CSV が出ている = 409」(列ごと・人が void にするまで。契約 v3 H4 = 予約を黙って外さない)。
  //    JAN はその CSV の列に無い (NE の JAN は画面で) = JAN を変えても外す行は無い。JAN の trigger が SKU の version を変える = 次に作る CSV は新しい版で作る
  return {
    result: {
      ok: true, code: cur.code, kind: cur.sku_kind, request_id: req.requestId,
      changed: changes.map((c) => ({ field: c.field, label: c.label, from: c.from, to: c.to, ne: c.ne })),
      derived: applied.derived.map(({ sku_id, ...d }) => d),
      ne_steps: neSteps(cur, changes, applied.derived),
      warnings: [...warningsOf(changes), ...setWarnings, ...ctx.notes,
        ...(reg.superseded.length ? [`まだ配っていなかった NE 登録の CSV (ファイル ${reg.superseded.map((x) => `#${x}`).join('・')}) を使わないにしました (作り直してください)`] : [])],
    },
  };
}

/** この単品を開いている構成の依頼に入れているセット (新しいセットの構成は依頼だけ) */
async function requestParentSets(db, skuId) {
  if (!(await regclass(db, 'ops.sku_component_requests'))) return [];
  return (await db.query(`select q.set_sku_id::text as id from ops.sku_component_requests q
     where q.status = 'open' and exists (select 1 from jsonb_array_elements(q.rows) x where (x ->> 'sku_id') = $1::text)`, [String(skuId)])).rows.map((r) => r.id);
}
/**
 * 新規登録の CSV (0053) の生きている商品 (SKU → { item_id, export_id, state })。表が無い DB = 空
 */
export async function liveRegItems(db, skuIds) {
  if (!skuIds.length || !(await regclass(db, 'ops.ne_reg_export_items'))) return new Map();
  return new Map((await db.query(`select sku_id::text as sku_id, item_id::text as item_id, export_id::text as export_id, state from ops.ne_reg_export_items
     where sku_id = any($1::bigint[]) and state = any($2::text[])`, [skuIds.map(String), REG_LIVE_STATES])).rows.map((r) => [r.sku_id, r]));
}
/**
 * 保存の取引の中 (SKU の鍵と CSV の鍵の後): 新規登録の CSV を配った後 (issued / import_declared / partial) = 409 reg_csv_issued。
 * 作っただけ (built) = そのファイルを閉じる (中の商品は全部 superseded = 作り直す)。
 * 書くのは DB の関数 ops.ne_reg_guard_on_save (security definer・0053)。画面のロールに新規登録の CSV の表の書き込みは無い
 */
export async function guardRegCsvOnSave(db, { skuIds, actor, what }) {
  if (!skuIds.length || !(await regclass(db, 'ops.ne_reg_export_items'))) return { superseded: [] };
  const r = (await db.query('select ops.ne_reg_guard_on_save($1::bigint[], $2, $3) as r', [skuIds.map(String), actor, what])).rows[0].r;
  if (r.issued.length) {
    const codes = [...new Set(r.issued.map((x) => x.code))].sort();
    const exps = [...new Set(r.issued.map((x) => x.export_id))];
    throw new MasterWriteError(409, 'reg_csv_issued',
      `${codes.join('・')} の NE 登録の CSV (ファイル ${exps.map((x) => `#${x}`).join('・')}) を配った後なので、${what}は直せません。何も保存していません。`
      + '「NE 登録の CSV」でそのファイルを「使わない」にして (NE で何をしたかを書いて) から直してください', { exports: exps });
  }
  return { superseded: r.superseded };
}



/** この SKU の、NE に取り込む CSV が出ている行 (作った・確かめた = まだ使える / 申告して確かめ待ち)。void・確かめが終わった行は入れない */
export async function issuedCsv(db, codeNorm, cols) {
  if (!cols.length || !(await csvApplied(db))) return [];
  return (await db.query(`select r.export_id::text as export_id, r.col, e.state
      from ops.ne_csv_export_rows r join ops.ne_csv_exports e on e.export_id = r.export_id
     where r.code_norm = $1 and r.col = any($2::text[]) and (e.state in ('made', 'checked') or (e.state = 'declared' and r.reserved))
     order by r.export_id, r.col`, [codeNorm, cols])).rows;
}

/**
 * セット: 保存の後の姿で導く値が決まらなければ保存しない。決まるなら「気をつけること」を返す。
 * 🚨 確かめるのは「今の構成 (core = NE で確かめた構成・保存の後もこれで計算する)」と「依頼の構成 (保存の後に開いている依頼があれば)」の両方 (#1563 R1 H4)。
 *    セット全体の上書き (売上分類・例外原価) を外すのは、両方が導けるときだけ通る。上書きを付けられるのは、どちらかが導けないときだけ
 */
function checkSetDerivable(cur, changes) {
  const by = Object.fromEntries(changes.map((c) => [c.field, c]));
  const pending = by.components ? (by.components.action === 'request' ? by.components.rows : null)
    : (cur.component_request ? cur.component_request.rows : null);
  const override = by.set_sales_class_override ? by.set_sales_class_override.to : cur.set_sales_class_override;
  const handlingOwn = by.handling_own ? by.handling_own.to : cur.handling_own;
  const hasException = by.exception_cost ? !by.exception_cost.clear : (!!cur.open_cost && OVERRIDE_SOURCES.has(cur.open_cost.cost_source));
  const opts = { override, handlingOwn, currentHandling: cur.handling, exceptionCost: hasException };
  const dCur = deriveSetCdb(cur.components, opts);
  const dPend = pending ? deriveSetCdb(pending, opts) : null;
  if (by.set_sales_class_override && by.set_sales_class_override.to != null && dCur.salesFromComponents != null && (!dPend || dPend.salesFromComponents != null)) {
    const v = dCur.salesFromComponents;
    throw bad(`構成品から売上分類 (${v}) を導けるので、上書きはできません (今の構成か依頼の構成で導けないとき・輸出 4 が混ざるときだけ)`, 'set_sales_class_override');
  }
  const tag = (label, list) => (dPend ? list.map((x) => `${label}: ${x}`) : list);
  const blockers = [...tag('今の構成', dCur.blockers), ...(dPend ? tag('依頼の構成', dPend.blockers) : [])];
  if (blockers.length) {
    throw bad(`このセットは導く値が決まらないので保存しません: ${blockers.join(' / ')}`, 'set', 'set_underivable', { blockers });
  }
  return [...new Set([...tag('今の構成', dCur.warnings), ...(dPend ? tag('依頼の構成', dPend.warnings) : [])])];
}

/** 原価の適用日は今日だけ。今日より先に始まる行が既にあれば入れない */
function checkCostToday(open, ctx, field) {
  if (open && open.valid_from > ctx.today) throw bad(`${open.valid_from} から始まる原価がもう入っているので、今日からの原価は入れられません`, field, 'cost_future');
}

/** 共通の欄 (名前・売価・送料・推奨保有月数) の差 */
function commonDiff(cur, v, ctx, add) {
  if (v.name !== undefined && v.name !== cur.name) add('name', cur.name, v.name);
  if (v.standard_price !== undefined && v.standard_price !== cur.standard_price) {
    if (v.standard_price == null) throw bad('標準売価は空にできません', 'standard_price');
    add('standard_price', cur.standard_price, v.standard_price);
  }
  if (v.shipping_code !== undefined && (v.shipping_code ?? null) !== (cur.shipping_code ?? null)) {
    if (v.shipping_code == null) throw bad('送料コードは空にできません', 'shipping_code');
    if (!ctx.shippingRates) throw new MasterWriteError(503, 'shipping_rates_unavailable', '送料の表が読めないので、送料は保存できません (何も保存していません)', { field: 'shipping_code' });
    const r = ctx.shippingRates.get(v.shipping_code);
    if (!r) throw bad(`送料コード ${v.shipping_code} は送料の表にありません`, 'shipping_code');
    add('shipping_code', cur.shipping_code, v.shipping_code, { method: r.method ?? null, cost_jpy: r.cost == null ? null : Math.round(Number(r.cost)) });
  }
  if (v.reorder_months !== undefined && v.reorder_months !== cur.reorder_months) {
    if (v.reorder_months == null) throw bad('推奨保有月数は空にできません', 'reorder_months');
    add('reorder_months', cur.reorder_months, v.reorder_months);
  }
}

/** 仕入先の「NE に登録した」の申告 (0053)。行が無い = 前からある仕入先 = 使える / ne_pending = 代表にできない */
export async function assertSupplierConfirmed(db, sup) {
  if (!(await regclass(db, 'ops.supplier_registrations'))) return;
  const st = (await db.query('select state from ops.supplier_registrations where supplier_id = $1', [sup.id])).rows[0]?.state;
  if (st && st !== 'ne_confirmed') throw bad(`仕入先 ${sup.code} は「NE に登録した」の申告がまだなので代表にできません (NE の仕入先の画面で登録してから申告)`, 'primary_supplier', 'supplier_not_confirmed');
}
/** JAN がほかの商品の有効な JAN でないこと (有効な JAN は全体で 1 つ = 0002 の一意)。ほかが持っている = 409 */
export async function assertJanFree(db, jan, productId) {
  const h = (await db.query(`select e.entity_type, e.entity_id::text as entity_id,
        (select s.code from core.skus s where e.entity_type = 'product' and s.product_id = e.entity_id order by s.code_norm limit 1) as code
      from core.external_ids e where e.system = 'jan' and e.id_kind = 'jan' and e.external_norm = core.norm_code($1) and e.valid_to is null`, [jan])).rows[0];
  if (h && !(h.entity_type === 'product' && h.entity_id === String(productId))) {
    throw new MasterWriteError(409, 'jan_taken', `JAN ${jan} はほかの商品 (${h.code ?? `${h.entity_type} ${h.entity_id}`}) の有効な JAN です。何も保存していません (先にそちらの JAN を外す)`, { field: 'jan', holder: h.code ?? null });
  }
}
/** JAN の約束の request_id (保存の request_id から決まる = 同じ保存の押し直しは同じ番号) */
export function janRequestId(requestId) {
  const h = sha256(`${requestId}:jan`);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20, 32)}`;
}
/**
 * JAN を足す・外す = 0053 の ops.edit_sku_jan (JAN の約束 jan_edit・security definer)。保存の約束 (sku_edit) の done を書いた後に、同じ取引で
 * 約束の設定を外してから呼ぶ (画面のロールは core.external_ids を直接書けない・#1571 R1 High 3)。DB が今の JAN と画面が見ていた JAN を比べ、
 * ほかの商品の JAN・配った後の NE 登録の CSV を拒み、変更の記録・SKU の version を書く
 */
async function applyJanEdit(db, req, ctx) {
  await db.query(`select set_config('ops.master_write_session', '', true)`);
  try {
    return (await db.query('select ops.edit_sku_jan($1::uuid, $2, $3, $4::jsonb, $5::bigint, $6::jsonb, $7::jsonb) as r',
      [janRequestId(req.requestId), req.actor, req.reason || null, JSON.stringify(ctx.ownership), String(ctx.skuId), JSON.stringify(ctx.janEdit.seen), JSON.stringify(ctx.janEdit.want)])).rows[0].r;
  } catch (e) {
    const msg = String((e && e.message) || '');
    if (e && e.code === '23505' && /ux_external_ids_active/.test(`${e.constraint || ''} ${msg}`)) {
      throw new MasterWriteError(409, 'jan_taken', 'JAN がちょうどほかの商品に付きました。何も保存していません', { field: 'jan' });
    }
    const m = /^(jan_taken|reg_csv_issued|version_conflict|before_cutover): ([\s\S]*)$/.exec(msg);
    if (m) throw new MasterWriteError(409, m[1], `${m[2]}。何も保存していません`, { field: 'jan' });
    throw e;
  }
}

async function resolveParent(db, code, cur) {
  const bySku = (await db.query(`select k.sku_kind, k.product_id::text as product_id, p.display_code from core.skus k left join core.products p on p.product_id = k.product_id
     where k.company_id = $1 and k.code_norm = core.norm_code($2)`, [COMPANY_ID, code])).rows[0];
  let target;
  if (bySku) {
    if (bySku.sku_kind !== 'single' || !bySku.product_id) throw bad('代表 (親) にできるのは単品か代表の名札だけです', 'parent_code');
    target = { product_id: bySku.product_id, display_code: bySku.display_code ?? code };
  } else {
    const rows = (await db.query(`select p.product_id::text as product_id, p.display_code from core.products p
       where p.company_id = $1 and core.norm_code(p.display_code) = core.norm_code($2) and not exists (select 1 from core.skus k where k.product_id = p.product_id)`, [COMPANY_ID, code])).rows;
    if (!rows.length) throw bad(`代表 (親) のコード ${code} が見つかりません (新しい名札はこの画面ではまだ作れません)`, 'parent_code');
    if (rows.length > 1) throw bad(`代表 (親) のコード ${code} の名札が 2 つ以上あります`, 'parent_code');
    target = rows[0];
  }
  if (target.product_id === cur.product_id) throw bad('自分自身は代表 (親) にできません', 'parent_code');
  const loop = (await db.query(`with recursive up as (
      select product_id, parent_product_id, 1 as d from core.products where product_id = $1
      union all
      select p.product_id, p.parent_product_id, up.d + 1 from core.products p join up on p.product_id = up.parent_product_id where up.d < 200)
    select coalesce(bool_or(product_id = $2), false) as loop, max(d) as depth from up`, [target.product_id, cur.product_id])).rows[0];
  if (loop.loop || Number(loop.depth) >= 200) throw bad('その代表 (親) にすると親子が循環します', 'parent_code');
  return target;
}

async function diffSingle(db, cur, v, ctx) {
  const ch = [];
  const add = (field, from, to, extra = {}) => ch.push({ field, label: SINGLE_FIELDS[field].label, keys: SINGLE_FIELDS[field].keys, ne: SINGLE_FIELDS[field].ne, from, to, ...extra });
  commonDiff(cur, v, ctx, add);
  if (v.handling !== undefined && v.handling !== cur.handling) {
    if (!['active', 'discontinued'].includes(v.handling)) throw bad('取扱区分は 取扱中 か 中止 を選んでください', 'handling');
    add('handling', cur.handling, v.handling);
  }
  if (v.tax_rate !== undefined && v.tax_rate !== cur.tax_rate) {
    if (v.tax_rate == null) throw bad('税率は空にできません', 'tax_rate');
    add('tax_rate', cur.tax_rate, v.tax_rate, { tax_class: v.tax_rate === 0.08 ? 'REDUCED_8' : 'STANDARD_10' });
  }
  if (v.sales_class !== undefined && v.sales_class !== cur.sales_class) {
    if (v.sales_class == null) throw bad('売上分類は空にできません', 'sales_class');
    // 売上分類は商品の行に持つ = 行が無い単品は持てない (「変えた」と記録しない・#1656 Codex R1 M4)
    if (!cur.product_id) throw bad('商品の行が無いので売上分類を持てません (何も保存していません)', 'sales_class', 'no_product');
    add('sales_class', cur.sales_class, v.sales_class);
  }
  if (v.primary_supplier !== undefined) {
    const want = v.primary_supplier ? canonicalSupplierCode(v.primary_supplier) : null;
    if (normSku(want ?? '') !== normSku(cur.primary_supplier ?? '')) {
      if (!want) throw bad('代表の仕入先は空にできません (NE の「設定なし」は 9999)', 'primary_supplier');
      const sup = (await db.query('select supplier_id::text as id, code, active from core.suppliers where company_id = $1 and code_norm = core.norm_code($2)', [COMPANY_ID, want])).rows[0];
      if (!sup) throw bad(`仕入先 ${want} は Company DB にありません`, 'primary_supplier');
      if (!sup.active) throw bad(`仕入先 ${want} は取引停止なので代表にできません`, 'primary_supplier');
      await assertSupplierConfirmed(db, sup);
      add('primary_supplier', cur.primary_supplier, sup.code, { supplier_id: sup.id });
    }
  }
  if (v.jan !== undefined) {
    if (!cur.product_id) throw bad('商品の行が無いので JAN を付けられません', 'jan');
    const active = cur.jan_rows.filter((j) => j.valid_to == null);
    const addList = v.jan.filter((x) => !active.some((j) => j.external_value === x));
    const remove = active.filter((j) => !v.jan.includes(j.external_value));
    if (addList.length || remove.length) {
      for (const x of addList) await assertJanFree(db, x, cur.product_id);
      add('jan', active.map((j) => j.external_value), v.jan, { add: addList, remove: remove.map((j) => j.id) });
    }
  }
  if (v.parent_code !== undefined) {
    const target = v.parent_code ? await resolveParent(db, v.parent_code, cur) : null;
    if ((target?.product_id ?? null) !== (cur.parent_product_id ?? null)) add('parent_code', cur.parent_code ?? null, target?.display_code ?? null, { product_id: target?.product_id ?? null });
  }
  if (v.cost != null) {
    checkCostToday(cur.open_cost, ctx, 'cost');
    if (!(cur.open_cost && cur.open_cost.cost_jpy === v.cost.jpy)) add('cost', cur.cost_today ? cur.cost_today.cost_jpy : null, v.cost.jpy, { cost_reason: v.cost.reason });
  }
  return ch;
}

/** 構成品のコード → SKU (有る・単品・自分でない) + 導く値の材料 */
async function resolveComponents(db, cur, rows, today) {
  const found = (await db.query(`select k.sku_id::text as sku_id, k.code_norm from core.skus k
     where k.company_id = $1 and k.code_norm = any(select core.norm_code(x) from unnest($2::text[]) as t(x))`, [COMPANY_ID, rows.map((r) => r.code)])).rows;
  const byNorm = new Map(found.map((f) => [f.code_norm, f.sku_id]));
  const facts = await componentFacts(db, found.map((f) => f.sku_id), today);
  return rows.map((r, i) => {
    const id = byNorm.get(normSku(r.code));
    if (!id) throw bad(`構成品 ${r.code} は Company DB にありません`, 'components');
    const k = facts.get(id);
    if (id === cur.sku_id) throw bad('セット自身は構成品にできません', 'components');
    if (k.sku_kind !== 'single') throw bad(`${k.code} は${k.sku_kind === 'set' ? 'セット' : '例外の SKU'}なので構成品にできません (セットの入れ子は不可)`, 'components');
    return { ...k, child_sku_id: id, qty: r.qty, sort: i + 1 };
  });
}

async function diffSet(db, cur, v, ctx) {
  const ch = [];
  const add = (field, from, to, extra = {}) => ch.push({ field, label: SET_FIELDS[field].label, keys: SET_FIELDS[field].keys, ne: SET_FIELDS[field].ne, from, to, ...extra });
  commonDiff(cur, v, ctx, add);
  if (v.components !== undefined) {
    const rows = await resolveComponents(db, cur, v.components, ctx.today);
    const text = (list) => list.map((r) => `${r.code}×${r.qty}`).join(', ');
    const asRows = (list, sortKey) => list.map((r) => ({ key: r.child_sku_id, qty: r.qty, sort: r[sortKey] }));
    const base = cur.component_request ? asRows(cur.component_request.rows, 'sort') : asRows(cur.components, 'position');
    const want = asRows(rows, 'sort');
    // 構成は構成品・数量・並び・行の数まで同じで「同じ」(Codex ⑤-R1 H2)
    if (!compositionEquals(base, want)) {
      if (compositionEquals(asRows(cur.components, 'position'), want)) {
        // 今の構成 (core) に戻した = 開いている依頼を取り下げる
        add('components', text(cur.component_request.rows), text(rows), { action: 'withdraw' });
      } else {
        const loadedById = new Map(cur.components.map((c) => [c.child_sku_id, c]));
        const newIds = new Set(rows.map((r) => r.child_sku_id));
        add('components', text(cur.component_request ? cur.component_request.rows : cur.components), text(rows), {
          action: 'request', rows,
          removed: cur.components.filter((c) => !newIds.has(c.child_sku_id)).map((c) => ({ code: c.code, qty: c.qty })),
          added: rows.filter((r) => !loadedById.has(r.child_sku_id)).map((r) => ({ code: r.code, qty: r.qty })),
          qty_changed: rows.filter((r) => loadedById.has(r.child_sku_id) && loadedById.get(r.child_sku_id).qty !== r.qty).map((r) => ({ code: r.code, from: loadedById.get(r.child_sku_id).qty, to: r.qty })),
          reordered: rows.length === cur.components.length && rows.every((r) => loadedById.has(r.child_sku_id) && loadedById.get(r.child_sku_id).qty === r.qty),
        });
      }
    }
  }
  if (v.set_sales_class_override !== undefined && v.set_sales_class_override !== cur.set_sales_class_override) {
    add('set_sales_class_override', cur.set_sales_class_override, v.set_sales_class_override);
  }
  if (v.handling_own !== undefined && v.handling_own !== cur.handling_own) {
    if (v.handling_own == null) throw bad('セット自身の取扱は空にできません', 'handling_own');
    add('handling_own', cur.handling_own, v.handling_own);
  }
  if (v.exception_cost != null) {
    const x = v.exception_cost;
    const open = cur.open_cost;
    checkCostToday(open, ctx, 'exception_cost');
    const overridden = !!open && OVERRIDE_SOURCES.has(open.cost_source);
    if (x.clear) {
      if (overridden) add('exception_cost', open.cost_jpy, null, { clear: true, cost_reason: x.reason });
    } else if (!(overridden && open.cost_jpy === x.jpy)) {
      add('exception_cost', overridden ? open.cost_jpy : null, x.jpy, { clear: false, jpy: x.jpy, cost_reason: x.reason });
    }
  }
  return ch;
}

async function openCost(db, skuId) {
  const r = (await db.query(`select sku_cost_id::text as id, cost_jpy::text as cost_jpy, cost_source, cost_status, valid_from::text as valid_from
     from core.sku_costs where sku_id = $1 and valid_to is null`, [skuId])).rows[0];
  return r ? { ...r, cost_jpy: Number(r.cost_jpy) } : null;
}
/**
 * 🚨 原価の期間は今までどおり両端を含む [valid_from, valid_to] (0049 などの読み手と同じ)。重なりの守りは 0051 の trigger (この画面と昇格の書き込みだけ)。
 *    表全体を半開区間 [from, to) + 排他制約にそろえるのは ⑥ 切替の前提条件 (#1563 R1 M7・0051 の冒頭に読み手・書き手の一覧)
 * 今日から原価を入れ替える (期間を重ねない): 今の行が昨日以前に始まった = 昨日で閉じる / 今日始まった = 消す (今日だけの行) / 明日以降 = 入れない。
 * row があれば今日からの新しい行を足す。row = null なら今日から原価なし (セットの合計ができないとき)
 */
async function replaceCostToday(db, skuId, open, row, ctx) {
  if (open) {
    if (open.valid_from > ctx.today) throw new MasterWriteError(409, 'cost_future', `${open.valid_from} から始まる原価がもう入っているので、今日からの原価は入れられません (何も保存していません)`);
    if (open.valid_from === ctx.today) await db.query('delete from core.sku_costs where sku_cost_id = $1', [open.id]);
    else await db.query('update core.sku_costs set valid_to = $2::date - 1 where sku_cost_id = $1 and valid_to is null', [open.id, ctx.today]);
  }
  if (row) {
    await db.query(`insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from, reason, created_by_type, created_by_id)
       values ($1, $2, $3, $4, $5, $6::date, $7, $8, $9)`, [COMPANY_ID, skuId, row.jpy, row.source, row.status, ctx.today, row.reason, ctx.actorType || 'human', ctx.actor]);
  }
}

/**
 * セットの導く値の計算し直しの「計画」(純粋な関数・DB を読まない)。保存 (recomputeSet) と一括の変更の確かめ (lib/master-bulk.mjs) が同じこれを使う
 * (確かめの値と保存の後の値をずらさない・一括の Codex レビュー High 3)。
 *   set   = { code, tax_rate, tax_class, handling, handling_own, open_cost: { cost_jpy, cost_source, cost_status, valid_from } | null }
 *   comps = 今の構成 (core.sku_components) の構成品 [{ qty, tax_rate, handling, cost_jpy (その日の決まった原価・無ければ null) }]
 *   what  = { tax, handling, cost }・today = 東京の日付
 * 戻り値 = { changes: [{ col, from, to, status? }], notes: [文], costPlan: null | { jpy, source, status } | 'clear' }
 * 先の日付の原価があるセットの原価は計算できない = MasterWriteError 409 set_cost_future
 */
export function planSetDerived(set, comps, what, today) {
  const changes = [];
  const notes = [];
  let costPlan = null;
  if (what.tax) {
    const d = deriveSetTaxCdb(comps.map((c) => c.tax_rate));
    const curRate = num(set.tax_rate);
    if (curRate !== d.taxRate || set.tax_class !== d.taxClass) changes.push({ col: 'tax_rate', from: { rate: curRate, class: set.tax_class }, to: { rate: d.taxRate, class: d.taxClass } });
  }
  if (what.handling) {
    const h = deriveSetHandlingCdb(set.handling_own, comps.map((c) => c.handling), set.handling);
    if (h !== set.handling) changes.push({ col: 'handling', from: set.handling, to: h });
    else if (set.handling_own == null && h === 'discontinued' && !comps.some((c) => c.handling === 'discontinued')) {
      notes.push(`セット ${set.code} は「セット自身の取扱」が決まっていないので中止のままにしました。取扱中に戻すなら、セットの画面で「セット自身の取扱」を選んでください`);
    }
  }
  if (what.cost) {
    const open = set.open_cost || null;
    if (!(open && OVERRIDE_SOURCES.has(open.cost_source))) {   // 例外原価 (人が決めた値) は計算で上書きしない
      if (open && open.valid_from > today) {
        throw new MasterWriteError(409, 'set_cost_future', `セット ${set.code} には ${open.valid_from} から始まる原価がもう入っているので、今日からの計算ができません (何も保存していません)`);
      }
      const d = deriveSetCostCdb(comps.map((c) => ({ costJpy: c.cost_jpy, qty: c.qty })));
      const curJpy = open ? open.cost_jpy : null;
      if (d.status === 'COMPLETE') {
        if (!(open && open.cost_source === 'set_calc' && open.cost_status === 'COMPLETE' && curJpy === d.jpy)) {
          costPlan = { jpy: d.jpy, source: 'set_calc', status: 'COMPLETE' };
          changes.push({ col: 'cost', from: curJpy, to: d.jpy });
        }
      } else if (open) {
        // 構成品の原価が足りない = 合計できない → 今日から原価を空に (古い合計を残さない)
        costPlan = 'clear';
        changes.push({ col: 'cost', from: curJpy, to: null, status: d.status });
      }
    }
  }
  return { changes, notes, costPlan };
}

/** セットの計算し直しの材料 (今の構成の構成品の税率・取扱・その日の原価)。保存と一括の確かめで同じ読み方 */
export async function setComponentInputs(db, setId, today) {
  return (await db.query(`select c.child_sku_id::text as sku_id, c.qty, k.tax_rate::text as tax_rate, k.handling, x.cost_jpy::text as cost_jpy, x.cost_status
      from core.sku_components c join core.skus k on k.sku_id = c.child_sku_id ${costAsOfJoin('c.child_sku_id', '$2', 'x')}
     where c.parent_sku_id = $1 order by c.sort_order, k.code_norm`, [setId, today])).rows
    .map((c) => ({ sku_id: c.sku_id, qty: Number(c.qty), tax_rate: c.tax_rate, handling: c.handling, cost_jpy: KNOWN_COST.has(c.cost_status) ? c.cost_jpy : null }));
}

/**
 * セットの導く値を計算し直す (同じ取引の中で・構成品の値を書いた後)。what = { tax, handling, cost, why }。計算は planSetDerived (一括の確かめと同じ)
 * 戻り値 = 変わった値の一覧 [{ sku_id, code, col, from, to }]
 */
async function recomputeSet(db, setId, what, ctx) {
  const s = (await db.query(`select sku_id::text as sku_id, code, tax_rate::text as tax_rate, tax_class, handling, handling_own from core.skus where sku_id = $1`, [setId])).rows[0];
  const comps = await setComponentInputs(db, setId, ctx.today);
  const open = what.cost ? await openCost(db, s.sku_id) : null;
  const plan = planSetDerived({ ...s, open_cost: open }, comps, what, ctx.today);
  ctx.notes.push(...plan.notes);
  const out = [];
  for (const c of plan.changes) {
    if (c.col === 'tax_rate') await db.query('update core.skus set tax_rate = $2, tax_class = $3 where sku_id = $1', [setId, c.to.rate, c.to.class]);
    if (c.col === 'handling') await db.query('update core.skus set handling = $2 where sku_id = $1', [setId, c.to]);
    if (c.col === 'cost') {
      await replaceCostToday(db, s.sku_id, open, plan.costPlan === 'clear' ? null : { ...plan.costPlan, reason: `構成品から計算 (${what.why})` }, ctx);
    }
    out.push({ sku_id: s.sku_id, code: s.code, ...c });
  }
  return out;
}

/** SKU の列をまとめて UPDATE (変わった列だけ) */
async function updateSku(db, skuId, cols) {
  const keys = Object.keys(cols);
  if (!keys.length) return;
  await db.query(`update core.skus set ${keys.map((k, i) => `${k} = $${i + 2}`).join(', ')} where sku_id = $1`, [skuId, ...keys.map((k) => cols[k])]);
}
function commonCols(by) {
  const cols = {};
  if (by.name) cols.name = by.name.to;
  if (by.standard_price) cols.standard_price_jpy = by.standard_price.to;
  if (by.shipping_code) Object.assign(cols, { shipping_code: by.shipping_code.to, shipping_method: by.shipping_code.method, shipping_cost_jpy: by.shipping_code.cost_jpy });
  if (by.reorder_months) cols.reorder_months = by.reorder_months.to;
  return cols;
}

async function applySingle(db, cur, changes, ctx) {
  const by = Object.fromEntries(changes.map((c) => [c.field, c]));
  const cols = commonCols(by);
  if (by.handling) cols.handling = by.handling.to;
  if (by.tax_rate) Object.assign(cols, { tax_rate: by.tax_rate.to, tax_class: by.tax_rate.tax_class });
  await updateSku(db, cur.sku_id, cols);
  // 商品の行 (単品は名前・取扱も同じ値にそろえる)
  const pcols = {};
  if (by.name && cur.product_name !== by.name.to) pcols.name = by.name.to;
  if (by.handling && cur.product_status !== by.handling.to) pcols.status = by.handling.to;
  if (by.sales_class) pcols.sales_class = by.sales_class.to;
  const pk = Object.keys(pcols);
  if (pk.length) {
    const hit = (await db.query(`update core.products set ${pk.map((k, i) => `${k} = $${i + 2}`).join(', ')} where product_id = $1 returning product_id`, [cur.product_id, ...pk.map((k) => pcols[k])])).rows.length;
    // 商品の行に書けなかった (行が無い・消えた) = 成功にしない (取引ごと巻き戻す)
    if (hit !== 1) throw new MasterWriteError(409, 'no_product', `商品の行に書けませんでした (${Object.keys(pcols).join('・')})。何も保存していません`);
  }
  // 代表 (親) = 人が決めた (manual)。親なし × manual = 人が外した (夜間ロードは付けない・0036)
  if (by.parent_code) await db.query("update core.products set parent_product_id = $2, parent_set_by = 'manual' where product_id = $1", [cur.product_id, by.parent_code.product_id]);
  // 代表の仕入先: 旧い代表を外してから付ける (部分 unique に当たらない)。行が無ければ作る (先方品番などは発注アプリが正のまま)
  if (by.primary_supplier) {
    await db.query('update core.supplier_skus set is_primary = false where sku_id = $1 and is_primary and supplier_id <> $2', [cur.sku_id, by.primary_supplier.supplier_id]);
    await db.query(`insert into core.supplier_skus (company_id, supplier_id, sku_id, is_primary, created_by_type, created_by_id) values ($1, $2, $3, true, 'human', $4)
       on conflict (supplier_id, sku_id) do update set is_primary = true where not core.supplier_skus.is_primary`, [COMPANY_ID, by.primary_supplier.supplier_id, cur.sku_id, ctx.actor]);
  }
  if (by.cost) await replaceCostToday(db, cur.sku_id, cur.open_cost, { jpy: by.cost.to, source: 'manual', status: 'COMPLETE', reason: by.cost.cost_reason }, ctx);
  // JAN (0053): ここでは書かない。保存の約束 (sku_edit) の done の後に、JAN の約束 (ops.edit_sku_jan) で書く (saveSku の applyJanEdit・#1571 R1 High 3)
  if (by.jan) ctx.janEdit = { seen: by.jan.from, want: by.jan.to };
  // この単品を含むセットの導く値
  const derived = [];
  if (by.tax_rate || by.handling || by.cost) {
    for (const setId of cur.parent_set_ids) {
      derived.push(...await recomputeSet(db, setId, { tax: !!by.tax_rate, handling: !!by.handling, cost: !!by.cost, why: `構成品 ${cur.code} の変更` }, ctx));
    }
  }
  return { derived };
}

async function applySet(db, cur, changes, ctx) {
  const by = Object.fromEntries(changes.map((c) => [c.field, c]));
  const cols = commonCols(by);
  if (by.set_sales_class_override) cols.set_sales_class_override = by.set_sales_class_override.to;
  if (by.handling_own) cols.handling_own = by.handling_own.to;
  await updateSku(db, cur.sku_id, cols);
  const derived = [];
  if (by.handling_own) derived.push(...await recomputeSet(db, cur.sku_id, { handling: true, why: 'セット自身の取扱の変更' }, ctx));
  if (by.exception_cost) {
    const x = by.exception_cost;
    const open = await openCost(db, cur.sku_id);
    if (x.clear) {
      // 例外原価をやめる: 例外の行を昨日で終わりにして (今日始まった行なら消して)、今日から構成品の合計に戻す
      await replaceCostToday(db, cur.sku_id, open, null, ctx);
      derived.push(...await recomputeSet(db, cur.sku_id, { cost: true, why: '例外原価をやめた' }, ctx));
    } else {
      await replaceCostToday(db, cur.sku_id, open, { jpy: x.jpy, source: 'manual', status: 'OVERRIDDEN', reason: x.cost_reason }, ctx);
    }
  }
  // 構成は依頼の表だけ (core.sku_components は書かない)。開いている依頼は同じ取引で閉じる
  if (by.components) {
    const c = by.components;
    if (cur.component_request) {
      await db.query(`update ops.sku_component_requests set status = 'cancelled', closed_at = now(), closed_by = $2, close_reason = $3
         where component_request_id = $1 and status = 'open'`, [cur.component_request.id, ctx.actor, c.action === 'withdraw' ? 'withdrawn' : 'superseded']);
    }
    // 依頼が変わった = 前の依頼への食い違い (mismatch / stale) は置き換わった。新しい依頼 = 依頼の無い食い違い (unrequested_diff) もこの依頼で追う
    const kinds = c.action === 'request' ? ['mismatch', 'stale', 'underivable', 'unrequested_diff'] : ['mismatch', 'stale', 'underivable'];
    await db.query(`update ops.sku_component_breaches set status = 'closed', closed_at = now(), closed_by = $2, close_reason = 'superseded'
       where set_sku_id = $1 and status = 'open' and kind = any($3::text[])`, [cur.sku_id, ctx.actor, kinds]);
    if (c.action === 'request') {
      const rows = c.rows.map((r) => ({ sku_id: Number(r.child_sku_id), code: r.code, qty: r.qty, sort: r.sort }));
      const baseRows = cur.components.map((r) => ({ sku_id: Number(r.child_sku_id), code: r.code, qty: r.qty, sort: r.position }));
      await db.query(`insert into ops.sku_component_requests (company_id, set_sku_id, rows, rows_hash, base_rows, reason, requested_by, edit_request_id)
         values ($1, $2, $3::jsonb, $4, $5::jsonb, $6, $7, $8)`,
      [COMPANY_ID, cur.sku_id, JSON.stringify(rows), sha256(stable(rows)), JSON.stringify(baseRows), ctx.reason, ctx.actor, ctx.requestId]);
    }
  }
  return { derived };
}

const HANDLING_TEXT = { active: '取扱中', discontinued: '中止', unknown: '不明' };
const taxText = (t) => (t && t.rate != null ? `${Math.round(t.rate * 100)}% (${t.class})` : `未決 (${t?.class ?? '—'})`);
/** NE でやること (保存の後に画面に出す) */
function neSteps(cur, changes, derived) {
  const steps = [];
  const by = Object.fromEntries(changes.map((c) => [c.field, c]));
  if (by.components && by.components.action === 'request') {
    const c = by.components;
    for (const r of c.removed) steps.push(`NE の画面で、セット ${cur.code} の構成から ${r.code} を外してください (CSV では外せません)`);
    if (c.added.length || c.qty_changed.length) steps.push(`NE の画面 (または一括登録の CSV) で、セット ${cur.code} に ${[...c.added.map((a) => `${a.code}×${a.qty} を足す`), ...c.qty_changed.map((q) => `${q.code} の数量を ${q.from}→${q.to}`)].join('・')}`);
    if (c.reordered) steps.push(`NE の画面で、セット ${cur.code} の構成品の並びを ${c.rows.map((r) => r.code).join(' → ')} にしてください`);
    steps.push('NE の構成が依頼と同じ (構成品・数量・並び) になったのを翌日以降の NE の取得で確かめてから、Company DB の構成に上げて依頼を閉じます (それまでは「NE でやること」に残ります)');
  }
  if (by.components && by.components.action === 'withdraw') steps.push(`セット ${cur.code} の構成の依頼を取り下げました (NE の構成はそのままで大丈夫です)`);
  for (const d of derived) {
    if (d.col === 'tax_rate') steps.push(`NE の画面で、セット ${d.code} の税率を ${taxText(d.to)} に直してください (セットの税率は構成品から決まります)`);
    if (d.col === 'handling') steps.push(`NE の画面で、セット ${d.code} の取扱区分を ${HANDLING_TEXT[d.to] || d.to} に直してください`);
  }
  if (by.handling_own && !derived.some((d) => d.col === 'handling' && d.code === cur.code)) steps.push(`NE の画面で、セット ${cur.code} の取扱区分を確かめてください`);
  if (by.jan && cur.registration?.state !== 'draft') steps.push('NE の JAN を使っているなら、NE の画面で JAN を直してください (JAN は既にある商品の NE に取り込む CSV には入れません)');
  if (changes.some((c) => c.ne === 'csv')) steps.push('NE に送る項目は、翌朝の照合で NE との差として出ます (マスタの判断 → NE に取り込む CSV)');
  return steps;
}
function warningsOf(changes) {
  const by = Object.fromEntries(changes.map((c) => [c.field, c]));
  return by.name && MATERIAL_TAG_RE.test(by.name.to) ? ['名前の末尾に資材の印があります。資材は梱包アプリで登録します (新しい名前には付けない・D-47)'] : [];
}

// ─── NE のセットの構成の観測・構成の依頼を上げる・食い違い (#1563 R1 H2・H3) ───
/**
 * NE のセットの構成の観測を 1 回分書く (ops.record_ne_set_observations を呼ぶだけ。観測のロール master_observer だけ実行できる・⑤-2 で夜間ロードから呼ぶ)。
 * payload = { run_id, observed_at, complete, requested, fetched, raw_hash, source_generation, sets: [{ set_code, rows: [{ code, qty, sort }] }] }
 *   完全な回は、残せないセット・構成品が 1 つも無く、requested = fetched = sets の数・raw_hash と source_generation がある (無ければ DB が拒む)
 */
export async function recordNeSetObservations(db, payload) {
  return (await db.query('select ops.record_ne_set_observations($1::jsonb) as r', [JSON.stringify(payload)])).rows[0].r;
}

/** 依頼から何日たっても NE が違えば stale (mismatch より強い「NE でやること」) */
export const STALE_REQUEST_DAYS = 7;

/**
 * NE のセットの構成の観測 1 つ (ops.ne_set_observations の番号だけ) で、そのセットの構成を確かめる (⑤-2 で夜間ロードが完全な回の観測ごとに呼ぶ。⑤-1 は試験だけ)。
 * 1 つの取引で: 段階の共有の鍵 → (保存が開いているか) → マスタの書き込みの鍵 (共有・夜間ロードの最中は短く待って nightly_load) →
 *   関わる全部の SKU の鍵 (セット・今の構成品・依頼の構成品・依頼したときの構成品・観測の構成品を sku_id の順) → CSV の鍵 → 行 →
 *   観測・依頼・今の構成・構成品の値を DB から読み直す (呼び手の値は信じない) →
 *   開いている依頼があり、観測が完全な回で依頼より後で、そのセットの最新の完全な観測で、構成品・数量・並び・行の数まで同じ = core.sku_components を依頼の構成にし、
 *     導く値を計算し直し、依頼を applied で閉じ、開いている食い違いを resolved で閉じる (promoted)
 *   依頼があって違う = 食い違い mismatch (依頼から STALE_REQUEST_DAYS 日より後なら stale) を残す / 依頼が無く今の構成と違う = unrequested_diff を残す /
 *   同じでも、最新の構成品の値・上書き・例外原価で依頼の構成の導く値が決まらない・構成品が単品でなくなった = core を変えずに underivable を残す (#1563 R2 H1・仮レビュー Low 6)
 *   変更の記録 = actor_type system・source_system ne_observation・run_id = 観測の回
 * 何も書かない (理由を返すだけ): 観測が無い・完全な回でない・受けてから OBSERVATION_MAX_AGE_HOURS 時間より前 (observation_too_old)・セットでなくなった (not_a_set)・
 *   依頼より前の観測 (stale_observation)・同じセットにもっと新しい完全な観測がある (superseded_observation・A → B → A の最初の A)・保存が開いていない (段階・持ち主表)・
 *   夜間ロードの最中 (nightly_load)・原価の期間が前からの行と重なる (cost_overlap)・鍵の後に関わる SKU が増えた (retry)
 * opts = { ownership, now, beforeCommit (試験だけ: 書くときの commit の直前に待つ) }
 */
export async function promoteComponentRequest(db, observationId, opts = {}) {
  const ownership = validateOwnership(opts.ownership || MASTER_OWNERSHIP);
  await db.query('begin');
  try {
    const t = (await db.query(`select (now() at time zone 'Asia/Tokyo')::date::text as today`)).rows[0];
    const ctx = { today: opts.now ? jstDate(opts.now) : t.today, actor: 'ne_observation', actorType: 'system', notes: [] };
    const out = await promoteInTx(db, observationId, ownership, ctx);
    if (out.write && opts.beforeCommit) await opts.beforeCommit();
    await db.query(out.write ? 'commit' : 'rollback');
    const { write, ...rest } = out;
    return rest;
  } catch (caught) {
    try { await db.query('rollback'); } catch { /* */ }
    // 原価の期間の重なり (前からの重なりに当たった・#1563 仮レビュー M1) = 上げない (夜間の段を止めない)
    const e = isCostOverlap(caught) ? costOverlapError(caught) : caught;
    // 導く値を今日から計算できない (先の日付の原価がある)・夜間ロードの最中など = 上げない (何も書かない) = 次の観測でもう一度
    if (e instanceof MasterWriteError) return { promoted: false, reason: e.reason, message: e.message };
    throw e;
  }
}

/** 観測の行・依頼の行 → 比べる形 (知らない構成品のコード = sku_id が無い = 必ず違う) */
const obsKeyRows = (rows) => (rows || []).map((r) => ({ key: r.sku_id == null ? `unknown:${normSku(r.code)}` : String(r.sku_id), qty: Number(r.qty), sort: Number(r.sort) }));

async function involvedSkuIds(db, setId, observationRows) {
  const ids = new Set([String(setId)]);
  for (const r of (await db.query('select child_sku_id::text as id from core.sku_components where parent_sku_id = $1', [setId])).rows) ids.add(r.id);
  const req = (await db.query(`select rows, base_rows from ops.sku_component_requests where set_sku_id = $1 and status = 'open'`, [setId])).rows[0];
  for (const r of [...(req ? req.rows : []), ...(req ? req.base_rows : []), ...(observationRows || [])]) if (r && r.sku_id != null) ids.add(String(r.sku_id));
  return ids;
}

async function promoteInTx(db, observationId, ownership, ctx) {
  const no = (reason, extra = {}) => ({ write: false, promoted: false, reason, ...extra });
  if (!/^\d{1,19}$/.test(String(observationId ?? ''))) return no('no_observation');
  const o0 = (await db.query(`select o.observation_id::text as id, o.run_id, o.set_sku_id::text as set_id, o.rows from ops.ne_set_observations o where o.observation_id = $1`, [String(observationId)])).rows[0];
  if (!o0) return no('no_observation');
  await db.query(`select set_config('core.actor_type', 'system', true), set_config('core.actor_id', 'ne_observation', true), set_config('core.source_system', 'ne_observation', true),
      set_config('core.run_id', $1, true), set_config('core.request_id', '', true), set_config('core.reason', 'NE のセットの構成の観測', true)`, [o0.run_id]);
  // 鍵: 段階 (共有) → (開いているかを見る) → マスタの書き込み (共有・短く待つ) → 関わる全部の SKU (sku_id の順・単品の保存と同じ鍵) → CSV → 行
  await db.query(CUTOVER_SHARED_LOCK_SQL);
  const phase = await readCutoverPhase(db, { inTx: true });
  if (!newEntryWritable(phase, ownership)) return no('before_cutover', { phase: phase.readable ? phase.phase : null });
  const loadKeys = ['sku_components', ...SET_DERIVED_KEYS].filter((k) => ownership[k] !== 'company');
  if (loadKeys.length) return no('before_cutover', { load_keys: loadKeys });
  await lockMasterWriteShared(db);
  const planned = await involvedSkuIds(db, o0.set_id, o0.rows);
  await lockSkuKeys(db, [...planned]);
  await db.query(CSV_LOCK_SQL);
  await lockSkuRows(db, [o0.set_id]);
  // 鍵の後に読み直す。関わる SKU が鍵を取る前より増えた = もう一度 (何も書かない)
  const now = await involvedSkuIds(db, o0.set_id, o0.rows);
  const extra = [...now].filter((id) => !planned.has(id));
  if (extra.length) return no('retry', { extra_sku_ids: extra });
  const obs = (await db.query(`select o.observation_id::text as id, o.set_sku_id::text as set_id, o.rows, r.run_id, r.complete, r.observed_at::text as observed_at,
        (extract(epoch from r.observed_at) * 1000)::float8 as observed_ms, (extract(epoch from r.recorded_at) * 1000)::float8 as recorded_ms,
        r.recorded_at < now() - make_interval(hours => $2::int) as too_old
      from ops.ne_set_observations o join ops.ne_set_observation_runs r on r.run_id = o.run_id where o.observation_id = $1`, [o0.id, OBSERVATION_MAX_AGE_HOURS])).rows[0];
  if (!obs.complete) return no('incomplete_observation', { run_id: obs.run_id });
  // 古い観測は使わない (上げない・食い違いも残さない。#1563 仮レビュー M2): 受けてから OBSERVATION_MAX_AGE_HOURS 時間より前
  if (obs.too_old) return no('observation_too_old', { run_id: obs.run_id, max_age_hours: OBSERVATION_MAX_AGE_HOURS });
  const set = (await db.query('select sku_id::text as sku_id, code, sku_kind from core.skus where sku_id = $1', [obs.set_id])).rows[0];
  if (set.sku_kind !== 'set') return no('not_a_set', { sku_kind: set.sku_kind });   // 夜間ロードがセットでなくした
  const observed = obsKeyRows(obs.rows);
  const req = (await db.query(`select component_request_id::text as id, rows, (extract(epoch from created_at) * 1000)::float8 as created_ms
      from ops.sku_component_requests where set_sku_id = $1 and status = 'open' for update`, [set.sku_id])).rows[0];
  if (req && (!(Number(obs.observed_ms) > Number(req.created_ms)) || !(Number(obs.recorded_ms) > Number(req.created_ms)))) return no('stale_observation', { request_id: req.id });
  // 同じセットに、この観測より新しい完全な回の観測がある = 使わない (A → B → A の最初の A を後から上げると、NE (B) と違う構成になる。#1563 仮レビュー M2)。
  //   新しい = NE から取った時刻 (observed_at) が後 (同じ時刻なら番号が大きい)
  const newer = (await db.query(`select n.observation_id::text as id from ops.ne_set_observations o join ops.ne_set_observation_runs r on r.run_id = o.run_id
      join ops.ne_set_observations n on n.set_sku_id = o.set_sku_id and n.observation_id <> o.observation_id
      join ops.ne_set_observation_runs nr on nr.run_id = n.run_id and nr.complete
     where o.observation_id = $1 and (nr.observed_at > r.observed_at or (nr.observed_at = r.observed_at and n.observation_id > o.observation_id))
     order by nr.observed_at desc, n.observation_id desc limit 1`, [obs.id])).rows[0];
  if (newer) return no('superseded_observation', { newer_observation_id: newer.id, ...(req ? { request_id: req.id } : {}) });
  const current = (await db.query(`select c.child_sku_id::text as sku_id, k.code, c.qty, c.sort_order from core.sku_components c join core.skus k on k.sku_id = c.child_sku_id
      where c.parent_sku_id = $1 order by c.sort_order, k.code_norm`, [set.sku_id])).rows;
  const currentRows = current.map((r, i) => ({ key: r.sku_id, qty: Number(r.qty), sort: i + 1 }));
  const shown = (rows) => rows.map((r) => ({ sku_id: r.sku_id ?? null, code: r.code, qty: Number(r.qty), sort: Number(r.sort ?? r.sort_order) }));
  if (!req) {
    if (compositionEquals(currentRows, observed)) {
      const closed = await closeBreaches(db, set.sku_id, ['unrequested_diff'], 'resolved');
      return { write: closed > 0, promoted: false, reason: 'no_open_request', closed_breaches: closed };
    }
    const b = await openBreach(db, { setId: set.sku_id, kind: 'unrequested_diff', observationId: obs.id, requestId: null, details: { observed: shown(obs.rows), expected: shown(current) } });
    return { write: b.written, promoted: false, reason: 'unrequested_diff', breach_id: b.id };
  }
  const want = req.rows.map((r) => ({ key: String(r.sku_id), qty: Number(r.qty), sort: Number(r.sort) }));
  if (!compositionEquals(want, observed)) {
    const kind = Number(obs.observed_ms) - Number(req.created_ms) > STALE_REQUEST_DAYS * 86400000 ? 'stale' : 'mismatch';
    const b = await openBreach(db, { setId: set.sku_id, kind, observationId: obs.id, requestId: req.id, details: { observed: shown(obs.rows), expected: shown(req.rows) } });
    return { write: b.written, promoted: false, reason: kind, breach_id: b.id, request_id: req.id };
  }
  // 上げる前に、依頼の構成 (最新の構成品の値・セットの上書き・例外原価) で導く値が決まるかを確かめる (#1563 R2 H1)。決まらない = core を変えずに食い違い
  const facts = await componentFacts(db, want.map((w) => w.key), ctx.today);
  // 構成品がまだ単品か (依頼の後に夜間ロードが種類を変えた = セットの入れ子になる) を上げる直前に確かめる (#1563 仮レビュー Low 6)。違う = core を変えずに食い違い
  const notSingle = want.map((w) => facts.get(w.key)).filter((k) => !k || k.sku_kind !== 'single');
  if (notSingle.length) {
    const blockers = notSingle.map((k) => (k ? `${k.code} は${k.sku_kind === 'set' ? 'セット' : '例外の SKU'}になった (セットの入れ子は不可)` : '構成品が Company DB に無い'));
    const b = await openBreach(db, { setId: set.sku_id, kind: 'underivable', observationId: obs.id, requestId: req.id, details: { observed: shown(obs.rows), expected: shown(req.rows), blockers } });
    return { write: b.written, promoted: false, reason: 'underivable', breach_id: b.id, request_id: req.id, blockers };
  }
  const setRow = (await db.query('select handling, handling_own, set_sales_class_override from core.skus where sku_id = $1', [set.sku_id])).rows[0];
  const openRow = await openCost(db, set.sku_id);
  const reqRows = [...want].sort((a, b) => a.sort - b.sort).map((w) => ({ ...facts.get(w.key), qty: w.qty }));
  const d = deriveSetCdb(reqRows, { override: num(setRow.set_sales_class_override), handlingOwn: setRow.handling_own, currentHandling: setRow.handling,
    exceptionCost: !!openRow && OVERRIDE_SOURCES.has(openRow.cost_source) });
  if (d.blockers.length) {
    const b = await openBreach(db, { setId: set.sku_id, kind: 'underivable', observationId: obs.id, requestId: req.id, details: { observed: shown(obs.rows), expected: shown(req.rows), blockers: d.blockers } });
    return { write: b.written, promoted: false, reason: 'underivable', breach_id: b.id, request_id: req.id, blockers: d.blockers };
  }
  // 上げる: core.sku_components を依頼の構成に (並び = 1〜n)
  const ordered = [...want].sort((a, b) => a.sort - b.sort);
  await db.query('delete from core.sku_components where parent_sku_id = $1', [set.sku_id]);
  for (let i = 0; i < ordered.length; i++) {
    await db.query(`insert into core.sku_components (company_id, parent_sku_id, child_sku_id, qty, sort_order, source, created_by_type, created_by_id)
       values ($1, $2, $3, $4, $5, 'ne', 'system', 'ne_observation')`, [COMPANY_ID, set.sku_id, ordered[i].key, ordered[i].qty, i + 1]);
  }
  const derived = await recomputeSet(db, set.sku_id, { tax: true, handling: true, cost: true, why: '構成の依頼を NE の観測で上げた' }, ctx);
  await db.query(`update ops.sku_component_requests set status = 'applied', closed_at = now(), closed_by = 'system', close_reason = 'matched', applied_observation_id = $2
       where component_request_id = $1 and status = 'open'`, [req.id, obs.id]);
  await closeBreaches(db, set.sku_id, ['mismatch', 'stale', 'unrequested_diff', 'underivable'], 'resolved');
  return { write: true, promoted: true, reason: 'matched', request_id: req.id, derived: derived.map(({ sku_id, ...d }) => d), notes: ctx.notes };
}

async function closeBreaches(db, setId, kinds, reason, by = 'system') {
  return (await db.query(`update ops.sku_component_breaches set status = 'closed', closed_at = now(), closed_by = $4, close_reason = $3
     where set_sku_id = $1 and status = 'open' and kind = any($2::text[]) returning breach_id`, [setId, kinds, reason, by])).rows.length;
}
/** 食い違いを残す。同じ種類の開いている食い違いが同じ中身ならそのまま (書かない)・中身が違えば閉じて新しく。mismatch と stale は 1 つだけ開く */
async function openBreach(db, { setId, kind, observationId, requestId, details }) {
  const hash = sha256(stable(details));
  const sameKinds = kind === 'unrequested_diff' ? ['unrequested_diff'] : ['mismatch', 'stale', 'underivable'];
  const open = (await db.query(`select breach_id::text as id, kind, details_hash from ops.sku_component_breaches where set_sku_id = $1 and status = 'open' and kind = any($2::text[])`, [setId, sameKinds])).rows;
  const same = open.find((b) => b.kind === kind && b.details_hash === hash);
  if (same && open.length === 1) return { id: same.id, written: false };
  if (open.length) await db.query(`update ops.sku_component_breaches set status = 'closed', closed_at = now(), closed_by = 'system', close_reason = 'superseded' where breach_id = any($1::bigint[])`, [open.map((b) => b.id)]);
  const id = (await db.query(`insert into ops.sku_component_breaches (set_sku_id, kind, observation_id, component_request_id, details, details_hash) values ($1, $2, $3, $4, $5::jsonb, $6)
     returning breach_id::text as id`, [setId, kind, observationId, requestId, JSON.stringify(details), hash])).rows[0].id;
  return { id, written: true };
}
