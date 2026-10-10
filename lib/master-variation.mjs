/**
 * master-variation.mjs — 色違い・サイズ違いのまとまりの登録 (新商品の登録の「色違い・サイズ違いのまとまり」・AI_reference CompanyDB構想/20 v7 §④・§⑤・§⑩ の PR-7)
 *
 * まとめての登録 1 回 = 1 つの Postgres の取引 (全部できるか、何もできないか = 半分だけできることは無い):
 *   ops.variation_batch_open (0067・まとまりを作る / 今あるまとまりを選ぶ・軸・選択肢・子のコードを全部確かめる・子ごとの request_id を決める) →
 *   子ごとに ops.register_new_sku (0052・今の単品の登録と同じ関数・子ごとのカードの知らせは無し = まとまりで 1 枚) → JAN があれば ops.edit_sku_jan (0053) →
 *   ops.variation_batch_close (子に親を付ける・軸・選択肢・子の選択肢・revision・まとまりの知らせ (ph-group-v1)) → commit
 * 門 (今の単品の登録と同じ + 1 つ): 段階 new_open・DB の active の持ち主・MASTER_EDIT_OPEN・新規開始の許可・backfill・単品で書く列の持ち主が全部 company
 *   **かつ products.parent の DB の active が company** (今は load = 409 parent_not_company・DB も同じ所で断る = 0067 の ops._variation_gate)。
 * 鍵の順 (DB の ops.variation_batch_open と同じ): まとめての request の鍵 → 新規開始の許可 (共有) → 段階 (共有) → マスタの書き込み (共有) → (DB が) 親子 → CSV → NE のコード → まとまりのコード → まとまり → 子のコード
 * 同じ request_id = 閉じた前の答え (DB が中身 = まとまり・軸・選択肢・子のコードを比べる) / 失敗の記録 = 同じ誤りを返す (中身が違えば 409)。
 * 子の名前は画面が作る (共通の商品名【横の選択肢名】【縦の選択肢名】【JAN】・人が直した名前はそのまま)。ここでは形 (255 字・empty でない) だけを見る。
 * 🚨 SQLite と NE には書かない (まとまりのカードは product-hub がまとまりの知らせで作る = PR-4・NE へは NE 登録の CSV のまとまりの版 = 回ごとに 1 ファイル)
 * 🚨 代表 (親) は登録の時に 1 回だけ = ここ (と quarantined の採用) だけが付ける。商品の画面の保存 (saveSku) には代表の欄は無い (PR-7 で外した)
 */
import crypto from 'node:crypto';
import { normSku } from './sku-norm.js';
import { canonicalSupplierCode } from '../apps/company-db/load/sources.mjs';
import { baseGateInTx, newEntryGateInTx, lockCutoverSharedInTx, acquireNewEntryLocksInTx } from './master-owner-gate.mjs';
import {
  MasterWriteError, ownerGateError, newEntryClosedFromDb, PARSERS, textIn, intIn, blank, stable, sha256, validateNewSkuCode, janValid, jstDate, costAsOfJoin,
  COMPANY_ID, SOURCE_SYSTEM, MAX_YEN, REASON_MAX, lockMasterWriteShared, assertSupplierConfirmed,
} from './master-write.mjs';
import { NEW_ENTRY_KEYS, CODE_PROBLEM_MESSAGES, parseCard } from './master-register.mjs';

/** 1 回のまとめての登録の子の数の上限 (中原さんの決定 10/10・40 色 × 3 サイズ)。DB の ops.variation_max_children() と同じ (試験で照らす) */
export const VARIATION_MAX_CHILDREN = 120;
/** まとまりで書く列の持ち主のキー (単品の登録のキー + 代表)。全部 company でないと登録しない */
export const VARIATION_KEYS = Object.freeze([...NEW_ENTRY_KEYS.single, 'products.parent']);
/** 選択肢番号 (コードにつける文字) = 先頭の「-」1 つ + 英数字 1〜10 字 (DB の ops.variation_batch_open と同じ) */
export const OPTION_CODE_RE = /^-[A-Za-z0-9]{1,10}$/;
export const AXIS_NAME_MAX = 100;
export const GROUP_SEARCH_MAX = 30;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const ID_RE = /^[1-9][0-9]{0,17}$/;
const CTRL_RE = new RegExp(`[\\x00-\\x1f\\x7f${String.fromCharCode(0x2028, 0x2029)}]`);
const DEFAULT_REASON = '新商品の登録 (色違い・サイズ違い)';
const COST_REASON = '新商品の登録';
const bad = (message, field, reason = 'invalid_input', extra = {}) => new MasterWriteError(400, reason, message, { field, ...extra });
const conflict = (reason, message, extra = {}) => new MasterWriteError(409, reason, message, extra);
/** 選択肢名・軸の名前の一意の鍵 (DB の name_key = btrim(normalize(name, NFKC)) と同じ) */
export const labelKey = (s) => String(s ?? '').normalize('NFKC').replace(/^ +| +$/g, '');
/** まとめての request_id から子・閉じる・JAN の request_id を決める (DB の ops.variation_sub_request_id と同じ = sha256(request_id:印) の先頭 32 字) */
export function subRequestId(requestId, tag) {
  const h = crypto.createHash('sha256').update(`${requestId}:${tag}`).digest('hex');
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20, 32)}`;
}
/** まとめての request の鍵 (DB の ops.variation_batch_open の最初の鍵と同じ = 2 回目に取っても待たない) */
const VARIATION_REQUEST_LOCK_SQL = "select pg_advisory_xact_lock(hashtextextended('ops.variation_request:' || $1::text, 0))";

/** 軸の名前・選択肢名 (前後の空白を除いた 1〜100 字・改行と制御文字なし。DB の ops.variation_label_problem と同じ) */
function labelIn(v, label, field) {
  if (typeof v !== 'string') throw bad(`${label}を入れてください`, field);
  const s = v.trim();
  if (!s) throw bad(`${label}を入れてください`, field);
  if ([...s].length > AXIS_NAME_MAX) throw bad(`${label}は ${AXIS_NAME_MAX} 字までです`, field);
  if (CTRL_RE.test(s)) throw bad(`${label}に改行や制御文字は入れられません`, field);
  if (!labelKey(s)) throw bad(`${label}を入れてください`, field);
  return s;
}

/**
 * 画面から来たまとめての登録を確かめて決まった形にする (DB を読まない。ここで落ちた登録は記録しない)。
 * input = { actor, requestId, reason?, group: { mode: 'new', code, name } | { mode: 'add', product_id }, axes?: [{ axis, name }],
 *   options: [{ axis, code, name }], children: [{ code, choices: { 1, 2? }, name, price?, cost?, jan? }], values: {...}, card? }
 */
export function parseVariationRequest(input) {
  const actor = String(input?.actor ?? '').trim().toLowerCase();
  if (!actor || actor.length > 320 || CTRL_RE.test(actor)) throw bad('保存する人 (ログインのメール) が分からない', 'actor');
  const requestId = String(input?.requestId ?? '').trim().toLowerCase();
  if (!UUID_RE.test(requestId)) throw bad('保存の番号 (request_id) の形が違う。画面を開き直してください', 'request_id');
  const reason = textIn(input?.reason, { label: '理由', field: 'reason', max: REASON_MAX });
  // まとまり
  const g = input?.group;
  if (!g || typeof g !== 'object' || Array.isArray(g)) throw bad('まとまりを選んでください', 'group');
  let group;
  if (g.mode === 'new') {
    const cv = validateNewSkuCode(g.code);
    if (!cv.ok) throw bad(`まとまりのコード: ${cv.message.replace(/^商品コード/, 'コード')}`, 'group.code');
    const name = PARSERS.name(g.name);
    group = { mode: 'new', code: cv.code, name };
  } else if (g.mode === 'add') {
    const id = String(g.product_id ?? '');
    if (!ID_RE.test(id)) throw bad('足すまとまりの番号の形が違う。画面を開き直してください', 'group');
    group = { mode: 'add', product_id: id };
  } else throw bad('まとまりは「新しいまとまりを作る」か「今あるまとまりに足す」です', 'group');
  // 軸 (新しいまとまり・軸の記録の無い今あるまとまりは 1〜2 つ。軸の記録がある今あるまとまりは送らない = DB が今の軸を使う)
  let axes = null;
  if (input?.axes != null) {
    if (!Array.isArray(input.axes) || input.axes.length < 1 || input.axes.length > 2) throw bad('軸は 1〜2 つです (横・要るときだけ縦)', 'axes');
    axes = input.axes.map((a, i) => {
      if (!a || typeof a !== 'object' || Number(a.axis) !== i + 1) throw bad('軸の入れ方が違う。画面を開き直してください', 'axes');
      return { axis: i + 1, name: labelIn(a.name, i === 0 ? '横軸の名前' : '縦軸の名前', `axes.${i + 1}`) };
    });
    if (axes.length === 2 && labelKey(axes[0].name) === labelKey(axes[1].name)) throw bad('横軸と縦軸が同じ名前です', 'axes.2');
  }
  if (group.mode === 'new' && !axes) throw bad('軸 (横軸の名前) を入れてください', 'axes.1');
  // 足す選択肢 (軸ごとに番号・名前が一意)
  if (!Array.isArray(input?.options)) throw bad('選択肢の入れ方が違う', 'options');
  const seenCode = new Set(); const seenName = new Set();
  const options = input.options.map((o) => {
    if (!o || typeof o !== 'object') throw bad('選択肢の入れ方が違う', 'options');
    const axis = Number(o.axis);
    if (axis !== 1 && axis !== 2) throw bad('選択肢の軸は 1 (横) か 2 (縦) です', 'options');
    if (axes && axis > axes.length) throw bad('縦軸を使わないのに縦の選択肢があります', `options.${axis}`);
    const code = typeof o.code === 'string' ? o.code : '';
    if (!OPTION_CODE_RE.test(code)) throw bad(`コードにつける文字「${code}」は「-」から入れて英字・数字 (10 字まで) です (例 -WH・-90)`, `options.${axis}`, 'option_code_shape');
    const name = labelIn(o.name, '選択肢名', `options.${axis}`);
    const kc = `${axis}:${code.toLowerCase()}`; const kn = `${axis}:${labelKey(name)}`;
    if (seenCode.has(kc)) throw bad(`コードにつける文字 ${code} が 2 回あります (大文字小文字は同じと見ます)`, `options.${axis}`, 'option_exists');
    if (seenName.has(kn)) throw bad(`選択肢名「${name}」が 2 回あります`, `options.${axis}`, 'option_name_exists');
    seenCode.add(kc); seenName.add(kn);
    return { axis, code, name };
  });
  // 子
  const kids = input?.children;
  if (!Array.isArray(kids) || kids.length < 1) throw bad('作る子が 1 つもありません', 'children');
  if (kids.length > VARIATION_MAX_CHILDREN) {
    throw bad(`子が ${kids.length} 件。1 回に作れるのは ${VARIATION_MAX_CHILDREN} 件までです (色そのものを分けて、残りは次に足す)`, 'children', 'too_many');
  }
  const codes = new Set(); const combos = new Set(); const jans = new Map();
  const children = kids.map((k, i) => {
    if (!k || typeof k !== 'object' || Array.isArray(k)) throw bad(`${i + 1} 件目の子の入れ方が違う`, 'children');
    const cv = validateNewSkuCode(k.code);
    if (!cv.ok) throw bad(`子のコード ${String(k.code ?? '')}: ${cv.message}`, 'children', 'code_shape', { code: String(k.code ?? '') });
    const code = cv.code;
    const at = (m, reason = 'invalid_input') => bad(`${code}: ${m}`, 'children', reason, { code });
    const ch = k.choices;
    if (!ch || typeof ch !== 'object' || Array.isArray(ch) || Object.keys(ch).some((x) => x !== '1' && x !== '2') || typeof ch['1'] !== 'string') throw at('選択肢の入れ方が違う');
    const c1 = ch['1']; const c2 = Object.prototype.hasOwnProperty.call(ch, '2') ? ch['2'] : null;
    if (!OPTION_CODE_RE.test(c1) || (c2 !== null && (typeof c2 !== 'string' || !OPTION_CODE_RE.test(c2)))) throw at('選択肢の形が違う');
    if (axes && (c2 !== null) !== (axes.length === 2)) throw at('選択肢の数が軸の数と違う');
    if (group.mode === 'new' && code !== group.code + c1 + (c2 ?? '')) throw at(`コードが まとまりのコード + コードにつける文字 (${group.code + c1 + (c2 ?? '')}) と違う`, 'child_code_not_group_plus_choices');
    const norm = normSku(code);
    if (codes.has(norm)) throw at('コードが 2 回あります (大文字小文字は同じと見ます)', 'child_dup');
    codes.add(norm);
    const combo = `${c1.toLowerCase()}|${(c2 ?? '').toLowerCase()}`;
    if (combos.has(combo)) throw at('同じ選択肢の組の子が 2 つあります', 'child_dup');
    combos.add(combo);
    let name;
    try { name = PARSERS.name(k.name); } catch (e) { throw at(`名前: ${e.message}`); }
    const price = blank(k.price) ? null : (() => { try { return intIn(k.price, 1, MAX_YEN, '売価', 'children'); } catch (e) { throw at(e.message); } })();
    const cost = blank(k.cost) ? null : (() => { try { return intIn(k.cost, 0, MAX_YEN, '原価', 'children'); } catch (e) { throw at(e.message); } })();
    let jan = null;
    if (!blank(k.jan)) {
      jan = String(k.jan).normalize('NFKC').trim();
      if (!janValid(jan)) throw at(`JAN ${jan} は 8 桁か 13 桁の数字で、チェック数字が合うものを入れてください`, 'jan_shape');
      if (jans.has(jan)) throw at(`JAN ${jan} が ${jans.get(jan)} と同じです`, 'jan_dup');
      jans.set(jan, code);
    }
    return { code, choices: c2 === null ? { 1: c1 } : { 1: c1, 2: c2 }, name, price, cost, jan };
  });
  // 共通の欄 (単品の登録と同じ決まり。名前は子ごと)
  const raw = input?.values;
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) throw bad('共通の欄が無い', 'values');
  const KEYS = ['standard_price', 'shipping_code', 'tax_rate', 'sales_class', 'primary_supplier', 'reorder_months', 'cost', 'expiry_managed', 'inbound_date_managed'];
  const values = {};
  for (const [k, v] of Object.entries(raw)) {
    if (!KEYS.includes(k)) throw bad(`「${k}」はまとまりの共通の欄では入れられません`, k);
    if (v === undefined) continue;
    if (k === 'cost') {
      if (v == null || blank(v.jpy)) values.cost = null;
      else if (typeof v !== 'object' || Array.isArray(v)) throw bad('原価の入れ方が違う', 'cost');
      else values.cost = { jpy: intIn(v.jpy, 0, MAX_YEN, '原価', 'cost') };
    } else if (k === 'expiry_managed' || k === 'inbound_date_managed') {
      values[k] = blank(v) ? null : PARSERS[k](v);
    } else values[k] = PARSERS[k](v);
  }
  if (values.standard_price == null) throw bad('売価 (共通) を入れてください', 'standard_price');
  if (values.tax_rate == null) throw bad('税率を選んでください', 'tax_rate');
  if (values.sales_class == null) throw bad('売上分類を選んでください', 'sales_class');
  if (values.primary_supplier == null) throw bad('代表の仕入先を選んでください (NE で必須の項目です)', 'primary_supplier');
  if (values.reorder_months == null) throw bad('推奨保有月数を入れてください (0〜60 か月)', 'reorder_months');
  if (values.expiry_managed == null) throw bad('ロジザードの有効期限の管理を なし / あり から選んでください', 'expiry_managed');
  // 出品カードの欄 (まとまりで 1 枚 = まとまりの知らせの共通の欄)。作らない選択は無い (まとまりの知らせは DB が毎回書く)
  const c = input?.card == null ? {} : input.card;
  if (typeof c !== 'object' || Array.isArray(c)) throw bad('出品カードの欄の入れ方が違う', 'card');
  for (const k of Object.keys(c)) if (!['official_url', 'amazon_url', 'asin', 'reference_urls'].includes(k)) throw bad(`「${k}」はまとまりの出品カードでは入れられません`, 'card');
  const card = parseCard({ create: true, ...c }, 'single');
  return { actor, requestId, reason, group, axes, options, children, values, card };
}
export const variationPayloadHashOf = (req) => sha256(stable({ group: req.group, axes: req.axes, options: req.options, children: req.children, values: req.values, card: req.card, reason: req.reason }));
/** DB の ops.variation_batch_open に渡す中身 (まとまり・軸・選択肢・子のコードと選択肢だけ) */
export function batchSpecOf(req) {
  const spec = {
    group: req.group.mode === 'new' ? { code: req.group.code, name: req.group.name } : { product_id: req.group.product_id },
    options: req.options,
    children: req.children.map((k) => ({ code: k.code, choices: k.choices })),
  };
  if (req.axes) spec.axes = req.axes;
  return spec;
}
/** 子の登録の要求のハッシュ (ops.register_new_sku の p_payload_hash = 子 1 つの中身) */
const childPayloadHash = (req, k) => sha256(stable({ kind: 'single', code: k.code, reason: req.reason, values: { ...req.values, name: k.name, price: k.price, cost: k.cost }, jan: k.jan, batch: req.requestId }));

/**
 * まとめての登録をする。opts = { open (MASTER_EDIT_OPEN)・env・now (試験の「今日」)・shippingRates: Map | null・beforeCommit (試験)・phDraftExists?: (norm) => boolean | null }
 * 戻り値 = { ok, request_id, group_product_id, group_code, group_created, children: [{ code, sku_id }], revision, event_id, replayed? }
 */
export async function registerVariationBatch(db, input, opts = {}) {
  const req = parseVariationRequest(input);
  const ctx = { ownership: null, open: opts.open === true, env: opts.env || process.env, shippingRates: opts.shippingRates ?? null, payloadHash: variationPayloadHashOf(req),
    today: null, startedAt: null, groupCode: req.group.mode === 'new' ? req.group.code : null };
  // 楽天の商品管理番号 (= まとまりのコード) と同じ product-hub の下書きカードがある = 止める (DB は product-hub を読めない = ここで・設計 §③)
  if (req.group.mode === 'new' && opts.phDraftExists) {
    let hit = null;
    try { hit = await opts.phDraftExists(normSku(req.group.code)); } catch { hit = null; }
    if (hit) throw conflict('ph_draft_exists', `product-hub に同じ管理番号「${req.group.code}」の下書きカード (#${hit}) があります。別のコードにするか、product-hub のカードを片付けてから。何も保存していません`, { field: 'group.code' });
  }
  await db.query('begin');
  try {
    const t = (await db.query(`select now()::text as started, (now() at time zone 'Asia/Tokyo')::date::text as today`)).rows[0];
    ctx.startedAt = t.started;
    ctx.today = opts.now ? jstDate(opts.now) : t.today;
    const out = await batchInTx(db, req, ctx);
    if (out.replay) { await db.query('rollback'); return out.replay; }
    if (opts.beforeCommit) await opts.beforeCommit(out.result);
    await db.query('commit');
    return out.result;
  } catch (e0) {
    let e = e0;
    try { await db.query('rollback'); } catch { /* 接続が切れていれば rollback も失敗する */ }
    if (e && e.code === '23505' && /skus_company_id_code_norm_key/.test(String(e.constraint || e.message || ''))) {
      e = conflict('code_taken', 'ちょうどほかの処理が同じコードの商品を Company DB に入れました。何も保存していません', { field: 'children' });
    }
    if (e && e.code === '23505' && /master_edit_requests_pkey/.test(String(e.constraint || e.message || ''))) {
      const prev = await readRequest(db, req.requestId);
      return replayFailureOrThrow(prev, req, ctx);
    }
    if (ctx.startedAt && !(e instanceof MasterWriteError && (e.reason === 'request_id_reused' || e.extra?.replayed))) {
      const locked = e && e.code === '55P03';
      const err = e instanceof MasterWriteError
        ? { status: e.status, reason: e.reason, message: e.message, extra: e.extra }
        : { status: locked ? 409 : 500, reason: locked ? 'locked' : 'error', message: locked ? 'ほかの処理が同じ商品・まとまりを使っていました' : 'サーバーエラー', pg_code: e && e.code ? String(e.code) : null };
      const prev = await recordFailure(db, req, ctx, err);
      if (prev) return replayFailureOrThrow(prev, req, ctx);
    }
    throw e;
  }
}

const readRequest = async (db, rid) => (await db.query('select actor_id, operation, payload_hash, status, result, error from ops.master_edit_requests where request_id = $1', [rid])).rows[0];
/** 失敗の記録 (新しい取引・request の鍵の後にもう結果があれば書かない = その記録を返す) */
async function recordFailure(db, req, ctx, err) {
  try {
    await db.query('begin');
    await db.query(VARIATION_REQUEST_LOCK_SQL, [req.requestId]);
    const prev = await readRequest(db, req.requestId);
    if (!prev) {
      await db.query(`insert into ops.master_edit_requests (request_id, company_id, operation, target_code, sku_id, actor_id, payload_hash, status, error, started_at)
         values ($1, $2, 'variation_batch_open', $3, null, $4, $5, 'failed', $6::jsonb, $7::timestamptz)`,
      [req.requestId, COMPANY_ID, ctx.groupCode ?? `group:${req.group.product_id}`, req.actor, ctx.payloadHash, JSON.stringify(err), ctx.startedAt]);
    }
    await db.query('commit');
    return prev || null;
  } catch (e2) {
    try { await db.query('rollback'); } catch { /* */ }
    console.error(`[master-variation] 失敗の記録を残せなかった: ${e2 && e2.message}`);
    return null;
  }
}
/** 同じ request_id の失敗の記録 = 同じ誤り (人・中身が違えば 409)。done = ここには来ない (DB の open が前の答えを返す) */
function replayFailureOrThrow(prev, req, ctx) {
  if (!prev || prev.actor_id !== req.actor || prev.operation !== 'variation_batch_open') {
    throw conflict('request_id_reused', '同じ保存の番号 (request_id) で違う中身が来ました。画面を開き直してください');
  }
  if (prev.status === 'done') throw conflict('request_id_reused', '同じ保存の番号 (request_id) の登録はもう終わっています。画面を開き直してください');
  if (prev.payload_hash !== ctx.payloadHash) throw conflict('request_id_reused', '同じ保存の番号 (request_id) で違う中身が来ました。画面を開き直してください');
  const x = prev.error || {};
  throw new MasterWriteError(x.status || 500, x.reason || 'error', x.message || '前の保存は失敗しました', { ...(x.extra || {}), replayed: true });
}

async function batchInTx(db, req, ctx) {
  const reason = req.reason || DEFAULT_REASON;
  await db.query(`select set_config('core.actor_type', 'human', true), set_config('core.actor_id', $1, true), set_config('core.source_system', $2, true),
      set_config('core.request_id', $3, true), set_config('core.reason', $4, true), set_config('core.run_id', '', true)`,
  [req.actor, SOURCE_SYSTEM, req.requestId, reason]);
  // 1. まとめての request の鍵 → 前の記録 (失敗 = 同じ誤り・done = DB の open が前の答えを返す)
  await db.query(VARIATION_REQUEST_LOCK_SQL, [req.requestId]);
  const prev = await readRequest(db, req.requestId);
  if (prev && prev.status !== 'done') return { replay: replayFailureOrThrow(prev, req, ctx) };
  if (prev && (prev.operation !== 'variation_batch_open' || prev.actor_id !== req.actor)) throw conflict('request_id_reused', '同じ保存の番号 (request_id) がほかの操作・人で使われています。画面を開き直してください');
  // 2. 新規開始の許可 (共有・段階の鍵より前) → 段階 (共有) → 門 → マスタの書き込み (共有)
  ctx.nePre = await acquireNewEntryLocksInTx(db, 'single');
  if (ctx.nePre.error) throw ownerGateError(ctx.nePre.refusal);
  await lockCutoverSharedInTx(db);
  await assertVariationOpen(db, ctx);
  await lockMasterWriteShared(db);
  // 3. 共通の値 (送料・仕入先)
  const v = req.values;
  let shipping = { code: null, method: null, cost_jpy: null };
  if (v.shipping_code != null) {
    if (!ctx.shippingRates) throw new MasterWriteError(503, 'shipping_rates_unavailable', '送料の表が読めないので、選んだ発送方法を確かめられません (発送方法を空にすれば登録できます)。何も保存していません', { field: 'shipping_code' });
    const rate = ctx.shippingRates.get(v.shipping_code);
    if (!rate) throw bad(`送料コード ${v.shipping_code} は送料の表にありません`, 'shipping_code');
    if (!rate.method || !String(rate.method).trim()) throw bad(`送料コード ${v.shipping_code} に発送方法 (名前) がありません。送料の表を直してから登録してください`, 'shipping_code');
    shipping = { code: v.shipping_code, method: rate.method ?? null, cost_jpy: rate.cost == null ? null : Math.round(Number(rate.cost)) };
  }
  const want = canonicalSupplierCode(v.primary_supplier);
  const supplier = (await db.query('select supplier_id::text as id, code, active from core.suppliers where company_id = $1 and code_norm = core.norm_code($2)', [COMPANY_ID, want])).rows[0];
  if (!supplier) throw bad(`仕入先 ${want} は Company DB にありません`, 'primary_supplier');
  if (!supplier.active) throw bad(`仕入先 ${want} は取引停止なので代表にできません`, 'primary_supplier');
  await assertSupplierConfirmed(db, supplier);
  // 4. 開く (まとまり・軸・選択肢・子のコードを DB が全部確かめる・同じ request_id の閉じた前の答え)
  const open = await callDb(db, 'select ops.variation_batch_open($1::uuid, $2, $3, $4::jsonb, $5::jsonb) as r',
    [req.requestId, req.actor, req.reason || null, JSON.stringify(ctx.ownership), JSON.stringify(batchSpecOf(req))], req);
  if (open.replayed) return { replay: { ...open, replayed: true } };
  ctx.groupCode = open.group_code;
  // 5. 子ごとに登録 (子の request_id = DB が決めた番号・カードの知らせは無し) → JAN
  const byNorm = new Map(req.children.map((k) => [normSku(k.code), k]));
  const kids = [];
  for (const oc of open.children) {
    const k = byNorm.get(normSku(oc.code));
    if (!k || k.code !== oc.code) throw new MasterWriteError(500, 'error', `開いたまとめての登録の子 ${oc.code} が要求と違う`);
    const price = k.price ?? v.standard_price;
    const costJpy = k.cost ?? (v.cost ? v.cost.jpy : null);
    const entry = {
      kind: 'single', code: k.code, started_at: ctx.startedAt,
      product: { name: k.name, sales_class: v.sales_class, expiry_managed: v.expiry_managed, inbound_date_managed: v.inbound_date_managed ?? null },
      sku: {
        name: k.name, tax_rate: v.tax_rate, tax_class: v.tax_rate === 0.08 ? 'REDUCED_8' : 'STANDARD_10', handling: 'active', standard_price_jpy: price,
        shipping_code: shipping.code, shipping_method: shipping.method, shipping_cost_jpy: shipping.cost_jpy, reorder_months: v.reorder_months,
        set_sales_class_override: null, handling_own: null,
      },
      supplier_id: supplier.id,
      cost: costJpy == null ? null : { jpy: costJpy, source: 'manual', status: 'COMPLETE', valid_from: ctx.today, reason: COST_REASON },
      component_request: null, card: null,
    };
    const r = await callDb(db, 'select ops.register_new_sku($1::uuid, $2, $3, $4::jsonb, $5, $6::jsonb) as r',
      [oc.request_id, req.actor, reason, JSON.stringify(ctx.ownership), childPayloadHash(req, k), JSON.stringify(entry)], req, k.code);
    if (k.jan) {
      await callDb(db, 'select ops.edit_sku_jan($1::uuid, $2, $3, $4::jsonb, $5::bigint, $6::jsonb, $7::jsonb) as r',
        [subRequestId(req.requestId, `jan:${normSku(k.code)}`), req.actor, reason, JSON.stringify(ctx.ownership), String(r.sku_id), '[]', JSON.stringify([k.jan])], req, k.code);
    }
    kids.push({ code: k.code, sku_id: String(r.sku_id) });
  }
  // 6. 閉じる (親・軸・選択肢・子の選択肢・revision・まとまりの知らせ = 共通の欄つき)
  const common = { shipping: shipping.code ? shipping : null, amazon_url: req.card.amazon_url, asin: req.card.asin, official_url: req.card.official_url, reference_urls: req.card.reference_urls, yahoo: null };
  const close = await callDb(db, 'select ops.variation_batch_close($1::uuid, $2, $3::jsonb, $4::jsonb) as r', [req.requestId, req.actor, JSON.stringify(ctx.ownership), JSON.stringify(common)], req);
  return { result: { ...close, children: kids, request_id: req.requestId } };
}

/** 門: 単品の登録と同じ (段階・active・MASTER_EDIT_OPEN・単品で書く列・許可・backfill) + products.parent が company */
async function assertVariationOpen(db, ctx) {
  const gate = await baseGateInTx(db, { open: ctx.open, closedWhy: '新商品の登録はまだ開いていません' });
  const phase = gate.phase;
  const refuse = (why, loadKeys = []) => new MasterWriteError(409, 'before_cutover', `切替前です: 新商品はまだ NE・product-hub で登録します (${why})。何も保存していません`,
    { load_keys: loadKeys, open: ctx.open, phase: phase.readable ? phase.phase : null });
  if (!gate.ok) throw ownerGateError(gate, (why) => refuse(why));
  ctx.ownership = gate.ownership;
  if (ctx.ownership['products.parent'] !== 'company') {
    throw conflict('parent_not_company', '色違い・サイズ違いのまとまりは、まだ登録できません (代表の正本を Company DB に切り替える前)。単品・セットは今までどおり登録できます。何も保存していません', { field: 'group' });
  }
  const loadKeys = VARIATION_KEYS.filter((k) => ctx.ownership[k] !== 'company');
  if (loadKeys.length) throw refuse('Company DB ではまだ登録できない項目があります', loadKeys);
  const ne = newEntryGateInTx(db, 'single', { env: ctx.env, pre: ctx.nePre });
  if (!ne.ok) throw ownerGateError({ ...ne, phase });
  const bf = (await db.query('select count(*)::int as n from ops.master_registration_backfill')).rows[0].n;
  if (bf !== 1) throw conflict('backfill_missing', '切替の手順の「既存の商品の登録の状態 (backfill)」が済んでいないので、新商品はまだ登録できません。何も保存していません', { phase: phase.phase });
}

/** DB の断り (「key: 文」) → 画面の誤り (どの欄・どの子) */
const DB_MESSAGES = Object.freeze({
  group_exists: (c) => `まとまりのコード ${c} はもうあります。「今あるまとまりに足す」で選んでください`,
  option_exists: () => 'コードにつける文字がこのまとまりにもうあります (大文字小文字は同じと見ます)',
  option_name_exists: () => '選択肢名がこのまとまりにもうあります',
  option_code_shape: () => 'コードにつける文字は「-」から入れて英字・数字 (10 字まで) です',
  choice_exists: () => '同じ選択肢の組の子がもうあります (前に作った・やめた子のコードは使い回しません)',
  choice_unknown: () => '子の選択肢がまとまりの選択肢にありません。画面を開き直してください',
  child_code_not_group_plus_choices: () => '子のコードが まとまりのコード + コードにつける文字 になっていません',
  child_dup: () => '同じコードの子が 2 つあります',
  axes_fixed: () => 'このまとまりの軸が、画面を開いた後に変わりました。画面を開き直してください',
  too_many: () => `1 回に作れるのは ${VARIATION_MAX_CHILDREN} 件までです`,
  group_ambiguous: () => 'このまとまりのコードがほかのまとまり・商品と重なっていて決められません。管理者へ',
  not_a_group: () => 'この商品はまとまりではありません (新しいまとまりは「新しいまとまりを作る」で)',
  group_has_parent: () => 'この商品はほかのまとまりの子なので、まとまりにできません',
  retry: () => 'ちょうどほかの処理がこのまとまりを変えました。もう一度押してください',
  not_found: () => 'まとまりが見つかりません。画面を開き直してください',
});
const GROUP_FIELD_KEYS = new Set(['group_exists', 'group_ambiguous', 'not_a_group', 'group_has_parent', 'not_found', 'retry', 'axes_fixed']);
async function callDb(db, sql, params, req, childCode = null) {
  try {
    return (await db.query(sql, params)).rows[0].r;
  } catch (e) {
    const msg = String((e && e.message) || '');
    if (e && e.code === '23505' && /ux_external_ids_active/.test(`${e.constraint || ''} ${msg}`)) {
      throw conflict('jan_taken', `${childCode ? `${childCode}: ` : ''}JAN がちょうどほかの商品に付きました。何も保存していません`, { field: 'children', code: childCode });
    }
    const m = /^([a-z_]+):\s*([\s\S]*)$/.exec(msg);
    const key = m ? m[1] : '';
    const closed = newEntryClosedFromDb(e);
    if (closed) throw closed;
    if (key === 'parent_not_company') throw conflict('parent_not_company', `色違い・サイズ違いのまとまりは、まだ登録できません (DB が断った: ${m[2]})。何も保存していません`, { field: 'group' });
    if (key === 'before_cutover') throw conflict('before_cutover', `切替前です (DB が断った: ${m[2]})。何も保存していません`);
    if (key === 'backfill_missing') throw conflict('backfill_missing', '切替の手順の「既存の商品の登録の状態 (backfill)」が済んでいないので、新商品はまだ登録できません (DB が断った)。何も保存していません');
    if (key === 'request_id_reused') throw conflict('request_id_reused', '同じ保存の番号 (request_id) で違う中身が来ました。画面を開き直してください');
    if (key === 'jan_taken' || key === 'reg_csv_issued' || key === 'version_conflict') throw conflict(key, `${childCode ? `${childCode}: ` : ''}${m[2]}。何も保存していません`, { field: 'children', code: childCode });
    // コードの決まり (まとまり / 子)。DB の文 = 「key: まとまりのコード X は…」/「key: 子のコード X は…」/ 登録の関数の「key: …」
    if (CODE_PROBLEM_MESSAGES[key] || key === 'group_exists') {
      const g = /^まとまりのコード (\S+) /.exec(m[2]);
      if (g) {
        const text = key === 'group_exists' ? DB_MESSAGES.group_exists(g[1]) : CODE_PROBLEM_MESSAGES[key](g[1]).replace(/^商品コード/, 'まとまりのコード');
        throw conflict(key, `${text}。何も保存していません`, { field: 'group.code' });
      }
      const c = /^子のコード (\S+) /.exec(m[2]);
      const code = c ? c[1] : childCode;
      throw conflict(key, `${code ? CODE_PROBLEM_MESSAGES[key](code) : m[2]}。何も保存していません`, { field: 'children', code });
    }
    if (DB_MESSAGES[key]) {
      const field = GROUP_FIELD_KEYS.has(key) ? 'group' : /^option/.test(key) ? 'options' : 'children';
      const code = /子 (\S+) /.exec(m[2])?.[1] || childCode;
      const status = key === 'too_many' || key === 'option_code_shape' ? 400 : 409;
      throw new MasterWriteError(status, key, `${code && field === 'children' ? `${code}: ` : ''}${DB_MESSAGES[key](req?.group?.code)} (DB: ${m[2]})。何も保存していません`, { field, code: field === 'children' ? code : null });
    }
    if (key === 'invalid_value' || key === 'invalid_input') throw bad(`登録の値が DB の決まりに合いません (${childCode ? `${childCode}: ` : ''}${m[2]})。何も保存していません`, childCode ? 'children' : 'values', key, { code: childCode });
    throw e;
  }
}

// ─── 読む (画面) ───
const pick1 = (rows) => rows[0] ?? null;
/** まとまりの元の情報 (札 / 単品の代表・コード・予約の由来)。まとまりでない = null */
async function groupHead(db, productId) {
  return pick1((await db.query(`select p.product_id::text as product_id, p.name, p.parent_product_id::text as parent,
        (select count(*)::int from core.skus k where k.product_id = p.product_id) as nsku,
        (select k.code from core.skus k where k.product_id = p.product_id and k.sku_kind = 'single' order by k.sku_id limit 1) as sku_code,
        p.display_code, (select count(*)::int from core.products c where c.parent_product_id = p.product_id) as nkids,
        (select r.source from ops.variation_group_codes r where r.group_product_id = p.product_id) as source,
        coalesce((select r.revision from ops.variation_group_revisions r where r.group_product_id = p.product_id), 0) as revision
       from core.products p where p.company_id = $1 and p.product_id = $2::bigint`, [COMPANY_ID, productId])).rows);
}
function groupKindOf(h) {
  if (!h || h.parent) return null;
  if (h.nsku === 0) return h.display_code && String(h.display_code).trim() ? 'tag' : null;
  if (h.nsku === 1 && h.sku_code && h.nkids > 0) return 'single';
  return null;
}
const flag = (b) => (b === true ? '1' : b === false ? '0' : '');
const numStr = (v) => (v == null ? '' : String(Number(v)));

/**
 * まとまりを探す (コードの前の方・名前の一部・子の商品コード)。札 (SKU の無い商品) と単品の代表 (子のある単品)。子の商品に当たったら、そのまとまり。
 * 戻り値 [{ product_id, code, name, kind (tag / single), source (portal / load / ne_adopt / null), recorded (軸の記録がある), axes: [名前], children }]
 */
export async function searchVariationGroups(db, q, { limit = GROUP_SEARCH_MAX } = {}) {
  const text = String(q ?? '').trim();
  if (text.length > 60 || CTRL_RE.test(text)) return [];
  const norm = normSku(text);
  const like = (s) => s.replace(/[\\%_]/g, (c) => `\\${c}`);
  const rows = (await db.query(`with g as (
      select p.product_id, coalesce((select k.code from core.skus k where k.product_id = p.product_id and k.sku_kind = 'single' order by k.sku_id limit 1), p.display_code) as code, p.name
        from core.products p
       where p.company_id = $1 and p.parent_product_id is null
         and ((not exists (select 1 from core.skus k where k.product_id = p.product_id) and coalesce(btrim(p.display_code), '') <> '')
              or (exists (select 1 from core.products c where c.parent_product_id = p.product_id)
                  and (select count(*) from core.skus k where k.product_id = p.product_id) = 1
                  and exists (select 1 from core.skus k where k.product_id = p.product_id and k.sku_kind = 'single')))),
    hit as (
      select g.product_id, g.code, g.name, (core.norm_code(g.code) = $2) as exact, (core.norm_code(g.code) like $3 escape '\\') as pre from g
       where $2 = '' or core.norm_code(g.code) like $3 escape '\\' or g.name ilike $4 escape '\\'
      union all
      select g.product_id, g.code, g.name, false, false from g join core.products c on c.parent_product_id = g.product_id join core.skus k on k.product_id = c.product_id
       where $2 <> '' and (k.code_norm like $3 escape '\\'))
    select distinct on (product_id) product_id::text as product_id, code, name, exact, pre from hit order by product_id, exact desc, pre desc limit 300`,
  [COMPANY_ID, norm, `${like(norm)}%`, `%${like(text)}%`])).rows;
  if (!rows.length) return [];
  const ids = rows.map((r) => r.product_id);
  const extra = new Map((await db.query(`select p.product_id::text as id,
        (select count(*)::int from core.products c join core.skus k on k.product_id = c.product_id left join ops.master_registrations r on r.sku_id = k.sku_id
          where c.parent_product_id = p.product_id and r.state is distinct from 'cancelled') as kids,
        exists (select 1 from core.skus k where k.product_id = p.product_id) as single,
        (select r.source from ops.variation_group_codes r where r.group_product_id = p.product_id) as source,
        coalesce((select array_agg(a.name order by a.axis) from core.variation_axes a where a.group_product_id = p.product_id), '{}') as axes
       from core.products p where p.product_id = any($1::bigint[])`, [ids])).rows.map((r) => [r.id, r]));
  const out = rows.map((r) => {
    const x = extra.get(r.product_id);
    return { product_id: r.product_id, code: r.code, name: r.name, kind: x.single ? 'single' : 'tag', source: x.source ?? null, recorded: x.axes.length > 0, axes: x.axes, children: x.kids, exact: r.exact, pre: r.pre };
  });
  out.sort((a, b) => (b.exact - a.exact) || (b.pre - a.pre) || (b.children - a.children) || (a.code < b.code ? -1 : a.code > b.code ? 1 : 0));
  return out.slice(0, Math.max(1, Math.min(limit, GROUP_SEARCH_MAX))).map(({ exact, pre, ...c }) => c);
}

/**
 * 今あるまとまりを 1 つ読む (画面の「今あるまとまりに足す」)。まとまりでない = null。
 * 戻り値 { product_id, code, name, kind, source, revision, axes: [{ axis, name }], options: [{ axis, code, name, sort }],
 *   children: [{ sku_id, code, name, state, cancelled, choices: { 1, 2? } | null, values: { price, cost, tax, sales, supplier, months, expiry, inbound, ship } }] }
 *   values = 画面の共通の欄の形 (今ある子の値を写す・値が違う項目は人が選ぶ)
 */
export async function readVariationGroup(db, productId, { now = new Date() } = {}) {
  if (!ID_RE.test(String(productId ?? ''))) return null;
  const h = await groupHead(db, String(productId));
  const kind = groupKindOf(h);
  if (!kind) return null;
  const today = jstDate(now);
  const axes = (await db.query('select axis, name from core.variation_axes where group_product_id = $1 order by axis', [h.product_id])).rows;
  const options = (await db.query('select axis, code, name, sort from core.variation_options where group_product_id = $1 order by axis, sort, option_id', [h.product_id])).rows;
  const kids = (await db.query(`select k.sku_id::text as sku_id, k.code, k.name, k.standard_price_jpy::text as price, k.tax_rate::text as tax, k.reorder_months::text as months, k.shipping_code,
        c.sales_class, c.expiry_managed, c.inbound_date_managed, r.state, x.cost_jpy::text as cost, x.cost_status,
        (select su.code from core.supplier_skus ss join core.suppliers su on su.supplier_id = ss.supplier_id where ss.sku_id = k.sku_id and ss.is_primary order by su.code limit 1) as supplier,
        o1.code as c1, o2.code as c2
       from core.products c join core.skus k on k.product_id = c.product_id
       left join ops.master_registrations r on r.sku_id = k.sku_id
       left join core.sku_variation_choices ch on ch.sku_id = k.sku_id and ch.group_product_id = c.parent_product_id
       left join core.variation_options o1 on o1.option_id = ch.option1_id left join core.variation_options o2 on o2.option_id = ch.option2_id
       ${costAsOfJoin('k.sku_id', '$2', 'x')}
      where c.parent_product_id = $1 order by k.code_norm limit 2000`, [h.product_id, today])).rows;
  return {
    product_id: h.product_id, code: kind === 'single' ? h.sku_code : h.display_code, name: h.name, kind, source: h.source ?? null, revision: Number(h.revision),
    axes: axes.map((a) => ({ axis: Number(a.axis), name: a.name })),
    options: options.map((o) => ({ axis: Number(o.axis), code: o.code, name: o.name, sort: Number(o.sort) })),
    children: kids.map((k) => ({
      sku_id: k.sku_id, code: k.code, name: k.name, state: k.state ?? null, cancelled: k.state === 'cancelled',
      choices: k.c1 ? (k.c2 ? { 1: k.c1, 2: k.c2 } : { 1: k.c1 }) : null,
      values: {
        price: numStr(k.price), cost: ['COMPLETE', 'OVERRIDDEN'].includes(k.cost_status) && k.cost != null ? numStr(k.cost) : '',
        tax: k.tax == null ? '' : Number(k.tax) === 0.08 ? '0.08' : Number(k.tax) === 0.1 ? '0.1' : '', sales: k.sales_class == null ? '' : String(k.sales_class),
        supplier: k.supplier || '', months: numStr(k.months), expiry: flag(k.expiry_managed), inbound: flag(k.inbound_date_managed), ship: k.shipping_code || '',
      },
    })),
  };
}

/** コードの問題 → 画面の文 (まとまりのコード) */
export const GROUP_CODE_MESSAGES = Object.freeze({
  code_shape: () => '英字・数字・- _ だけ (30 字まで)・「set-」で始めない',
  group_exists: (c) => `「${c}」はもうあるまとまりです (大文字小文字は同じと見ます)`,
  code_taken: (c) => `「${c}」はもう商品のコードで使っています`,
  code_in_ne: (c) => `「${c}」は NE にもうあるコードです`,
  code_used_before: (c) => `「${c}」は前に使って消したコードです (使い回さない)`,
});
/**
 * 打った値を DB で確かめる (画面の「打つたびに確かめる」・保存のときは DB の関数が同じ決まりでもう一度)。
 * input = { group_code?, codes: [子のコード], jans: [JAN] }。戻り値 { group: { problem, message, product_id?, ph_draft? } | null, codes: { [code]: { problem, message } }, jans: { [jan]: 持っている商品のコード } }
 */
export async function checkVariationCodes(db, input, { phDraftExists = null } = {}) {
  const out = { group: null, codes: {}, jans: {} };
  const gc = typeof input?.group_code === 'string' ? input.group_code : '';
  if (gc) {
    const cv = validateNewSkuCode(gc);
    let problem = cv.ok ? (await db.query('select ops.variation_group_code_problem($1) as p', [gc])).rows[0].p : 'code_shape';
    let productId = null;
    if (problem === 'group_exists') {
      const n = normSku(gc);
      const hit = (await db.query(`select group_product_id::text as id from ops.variation_group_codes where company_id = $1 and code_norm = $2
          union all select p.product_id::text from core.products p where p.company_id = $1 and core.norm_code(p.display_code) = $2
            and not exists (select 1 from core.skus k where k.product_id = p.product_id) limit 1`, [COMPANY_ID, n])).rows[0];
      productId = hit ? hit.id : null;
      if (productId && !groupKindOf(await groupHead(db, productId))) productId = null;
    }
    let ph = null;
    if (!problem && phDraftExists) {
      try { ph = await phDraftExists(normSku(gc)); } catch { ph = null; }
      if (ph) problem = 'ph_draft_exists';
    }
    const message = problem === 'ph_draft_exists' ? `product-hub に同じ管理番号「${gc}」の下書きカード (#${ph}) があります`
      : problem ? (GROUP_CODE_MESSAGES[problem] || (() => `使えません (${problem})`))(gc) : null;
    out.group = { problem: problem || null, message, product_id: productId, ph_draft: ph };
  }
  const codes = Array.isArray(input?.codes) ? [...new Set(input.codes.filter((c) => typeof c === 'string' && c && c.length <= 60))].slice(0, VARIATION_MAX_CHILDREN * 3) : [];
  const shaped = codes.filter((c) => validateNewSkuCode(c).ok);
  for (const c of codes) if (!validateNewSkuCode(c).ok) out.codes[c] = { problem: 'code_shape', message: validateNewSkuCode(c).message };
  if (shaped.length) {
    const rows = (await db.query('select x as code, ops.new_sku_code_problem(x) as p from unnest($1::text[]) as t(x)', [shaped])).rows;
    for (const r of rows) if (r.p) out.codes[r.code] = { problem: r.p, message: (CODE_PROBLEM_MESSAGES[r.p] || ((c) => `${c} は使えません (${r.p})`))(r.code) };
  }
  const jans = Array.isArray(input?.jans) ? [...new Set(input.jans.map((j) => String(j ?? '').normalize('NFKC').trim()).filter((j) => janValid(j)))].slice(0, VARIATION_MAX_CHILDREN * 3) : [];
  if (jans.length) {
    const rows = (await db.query(`select e.external_value as jan, (select s.code from core.skus s where e.entity_type = 'product' and s.product_id = e.entity_id order by s.code_norm limit 1) as code
       from core.external_ids e where e.system = 'jan' and e.id_kind = 'jan' and e.valid_to is null and e.external_norm = any(select core.norm_code(x) from unnest($1::text[]) as t(x))`, [jans])).rows;
    for (const r of rows) out.jans[r.jan] = r.code ?? '(ほかの商品)';
  }
  return out;
}

/** product-hub の下書きカードに同じ管理番号 (ne_code の小文字) があるか (Render の SQLite)。ある = カードの番号 / 無い = null / 読めない = 投げる */
export async function defaultPhDraftExists(norm) {
  const { getDB } = await import('../apps/product-hub/db.js');
  const r = getDB().prepare('SELECT id FROM product_drafts WHERE LOWER(TRIM(ne_code)) = ? ORDER BY id LIMIT 1').get(String(norm));
  return r ? r.id : null;
}

/** 試験と画面のため (DB を読まない部品) */
export const __forTest = Object.freeze({ childPayloadHash, groupKindOf });
