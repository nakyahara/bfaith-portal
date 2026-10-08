/**
 * bulk.mjs — 一覧で選んだ商品を、まとめて 1 つの項目だけ同じ値に変える (10/8 中原さん「見本どおりで OK」・PR2)
 *
 * 項目 = 原価 (今日からだけ・理由は選ぶ)・売価・取扱 (取扱中 / 中止・中止は理由が必須)・売上分類・税率・仕入先 (代表の仕入先)。値は「同じ値にする」だけ。
 * 流れ (画面 = public/me-bulk.js):
 *   1. POST api/bulk/inspect  選んだ商品の、項目ごとの「変えられる / 対象外 / 保存できない」と今の値 (項目の板の数・値の欄の「今の値」)
 *   2. POST api/bulk/preview  項目と値を決めた後の「前と後」(1 件ずつの今の値・新しい値・利益・編集の印) と、一緒に変わるセット
 *   3. POST api/bulk/apply    20 件ずつ。1 件ずつ今の 1 件の保存 (lib/master-write.mjs の saveSku) を呼ぶ = 持ち主の門・変更の記録・版 (編集の印) は 1 件の保存と同じ
 * 決まり:
 *   - 1 回 BULK_MAX (200) 件まで (画面も選ぶ時点で数える)。対象外 = 例外の SKU (NE に無い・常に)・登録をやめた・セットの原価 / 税率 / 仕入先 / 売上分類 (構成品から計算)・
 *     取扱を中止した商品 (取扱を戻す以外)。保存できない = 先の日の原価 (含むセットも)・NE に取り込む CSV / NE 登録の CSV が出ている・切替前 (持ち主)
 *   - 途中で 1 件だめでも、できた分は変えたまま (今の DB は 1 つの取引で 1 つの商品しか書けない = 1 件 = 1 取引)。だめな分は理由と直し方の組 (FIX_GROUPS) で返す
 *   - request_id = 一括の番号 + 商品コード (+ やり直しの回) から決まる = 途中で切れて押し直しても二重に書かない (済んだ分は前の結果が返る)
 *   - 同じセットに入る単品を続けて変えると、前の保存がセットの版を上げて version_conflict になる → その間の変更がこの一括の分だけ
 *     (変更の記録の request_id がこの一括の番号から決まるもの・同じ人) なら、読み直して新しい印で 1 回だけやり直す。ほかの人・夜間の変更があれば「だめ」
 *   - 一緒に変わるセットの値は、保存と同じ計算 (lib/master-write.mjs の planSetDerived) で、全部の構成品の保存の後の予定値から出す (Codex High 3)。
 *     保存の後に実際に変わったセットは saveSku の derived をそのまま返す (Codex M3)
 */
import crypto from 'node:crypto';
import { normSku } from '../../lib/sku-norm.js';
import { canonicalSupplierCode } from '../company-db/load/sources.mjs';
import {
  saveSku, readCurrent, editTokenOf, sha256, stable, jstDate, issuedCsv, fieldsOf, fieldOwnership, REG_CSV_FIELDS, OVERRIDE_SOURCES,
  MasterWriteError, PARSERS, MAX_YEN, intIn, textIn, planSetDerived, setComponentInputs, COMPANY_ID,
} from '../../lib/master-write.mjs';
import { newEntryWritable } from '../../lib/master-cutover.mjs';
import { masterProfit } from '../../lib/profit-estimate.js';

export const BULK_MAX = 200;
export const BULK_CHUNK = 20;
/** 一括の理由の字数 (保存の理由 200 字に「一括 ◯ 件」を足す分を残す) */
export const BULK_REASON_MAX = 150;
/** 原価を変える理由 (1 件の画面 views/sku.ejs と同じ 4 つ + その他)。一括は初期値なし (Codex M4) */
export const COST_REASONS = Object.freeze(['メーカーからの値上げ通知', 'メーカーからの値下げ通知', '仕入先を変えた', '入力の誤りを直す']);
/**
 * 一括で変えられる項目。single / set = 1 件の保存 (saveSku) の欄の名前 (null = その種類は対象外)。linked = 単品を変えると含むセットの導く値も変わる
 */
export const BULK_FIELDS = Object.freeze({
  cost:             { label: '原価', single: 'cost', set: null, linked: 'cost', setWhy: 'セットの原価は構成品から計算します (構成品を変えると自動で変わります)' },
  standard_price:   { label: '売価', single: 'standard_price', set: 'standard_price', linked: null },
  handling:         { label: '取扱', single: 'handling', set: 'handling_own', linked: 'handling' },
  sales_class:      { label: '売上分類', single: 'sales_class', set: null, linked: null, setWhy: 'セットの売上分類は構成品から決まります (上書きはセットの画面で)' },
  tax_rate:         { label: '税率', single: 'tax_rate', set: null, linked: 'tax', setWhy: 'セットの税率は構成品から計算します (構成品を変えると自動で変わります)' },
  primary_supplier: { label: '仕入先', single: 'primary_supplier', set: null, linked: null, setWhy: 'セットの仕入先は構成品ごとです' },
});
const HANDLING = Object.freeze({ active: '取扱中', discontinued: '中止' });
/**
 * だめだった理由 → 直し方の組 (Codex M7)。retry = 一覧で選び直して、もう一度まとめて変えられる
 *   one    = 1 件の画面で直す (一括では直らない)
 *   latest = 最新の値を確かめてから選び直す
 *   later  = 少し待ってから選び直す
 *   csv    = 出ている CSV を片付けてから (または翌朝の照合の後に) 選び直す
 */
export const FIX_GROUPS = Object.freeze({
  one:    { label: '1 件の画面で直す', retry: false },
  latest: { label: '最新の値を確かめてから選び直す', retry: true },
  later:  { label: '少し待ってから選び直す', retry: true },
  csv:    { label: 'CSV を片付けてから選び直す', retry: true },
});
const GROUP_OF = {
  cost_future: 'one', set_cost_future: 'one', set_underivable: 'one', invalid_input: 'one', supplier_not_confirmed: 'one', cost_overlap: 'one',
  exception_sku: 'one', cancelled_sku: 'one', out: 'one', no_product: 'one',
  version_conflict: 'latest', retry: 'latest', request_id_reused: 'latest', not_found: 'latest',
  csv_issued: 'csv', reg_csv_issued: 'csv',
  nightly_load: 'later', locked: 'later', before_cutover: 'later', owner_unreadable: 'later', code_behind: 'later', error: 'later', not_sent: 'later',
};
export const fixGroupOf = (reason) => GROUP_OF[reason] || 'later';
const FIX_WORDS = {
  cost_future: '1 件の画面で、先の日の原価と合わせて直してください',
  set_cost_future: '含むセットに先の日の原価があります。1 件の画面で直してください',
  version_conflict: 'ほかの人か夜間の処理がこの商品を変えました。1 件の画面で今の値を見てから、もう一度選び直してください',
  csv_issued: 'マスタの判断 → NE に取り込む CSV でそのファイルを「使わない」にするか、翌朝の照合の後に選び直してください',
  reg_csv_issued: '「NE 登録の CSV」でそのファイルを「使わない」にしてから選び直してください',
  nightly_load: '夜間の取り込みが終わってから (少し待って) 選び直してください',
};

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const TOKEN_RE = /^[0-9a-f]{64}$/;
const bad = (message, field = 'bulk', reason = 'invalid_input') => new MasterWriteError(400, reason, message, { field });
const num = (v) => (v == null ? null : Number(v));
const KNOWN_COST = new Set(['COMPLETE', 'OVERRIDDEN']);

/** 一括の保存の request_id (一括の番号 + 商品コード + やり直しの回)。同じ一括の押し直しは同じ番号 = 二重に書かない */
export function bulkRequestId(bulkId, code, attempt = 0) {
  const h = sha256(`master-bulk:${bulkId}:${normSku(code)}:${attempt}`);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20, 32)}`;
}

/**
 * 確かめの切符 (preview ticket・#1656 Codex R1 High 1・M2・M3)。保存 (apply) は「確かめた中身」だけを受ける:
 *   切符 = 署名つき (HMAC) の { 番号・人・項目・値・理由・確かめの瞬間 (seen_event_id)・変わる件数・期限 } + 商品ごとの印 (切符の番号・コード・編集の印・自分の印の HMAC)。
 *   保存は 切符の署名と期限・人が同じ・送った項目 / 値 / 理由 / 件数が切符と同じ・商品ごとの印が正しい (= 確かめで「変わる」になった商品だけ・
 *   切符の件数 (200 以下) より多くは送れない) を確かめる。理由の「一括 ◯ 件」も切符の件数。
 * 🚨 署名にした理由 (サーバーの保存にしない): DB の表を足す (migrate) が要らない・セッションに 200 件分の印を溜めない・小さく閉じる (関数 2 つ)。
 *   鍵は env MASTER_BULK_TICKET_KEY (無ければプロセスごとの乱数 = 再起動で古い切符は 409「もう一度確かめて」に戻るだけ。切符の中身が見えても印は作れない)
 */
export const TICKET_TTL_MS = 30 * 60e3;
let ticketKey = process.env.MASTER_BULK_TICKET_KEY ? Buffer.from(String(process.env.MASTER_BULK_TICKET_KEY)) : crypto.randomBytes(32);
export function __setTicketKey(k) { ticketKey = k ? Buffer.from(String(k)) : crypto.randomBytes(32); }
const hmac = (s) => crypto.createHmac('sha256', ticketKey).update(s).digest('hex');
const sameHex = (a, b) => typeof a === 'string' && typeof b === 'string' && a.length === b.length && /^[0-9a-f]+$/.test(a) && crypto.timingSafeEqual(Buffer.from(a), Buffer.from(b));
export const itemMac = (ticketId, code, token, self) => hmac(`item|${ticketId}|${normSku(code)}|${token}|${self}`);
/** 切符を作る。items = 変わる商品 [{ code, token, self }] → { ticket, id, macs: Map<コード, 印> } */
export function issueTicket({ actor, field, value, reason, seen, items, nowMs = Date.now() }) {
  const id = crypto.randomUUID();
  const body = { v: 1, id, actor, field, value, reason: reason ?? null, seen: String(seen ?? '0'), n: items.length, exp: nowMs + TICKET_TTL_MS };
  const b = Buffer.from(JSON.stringify(body)).toString('base64url');
  const macs = new Map(items.map((x) => [x.code, itemMac(id, x.code, x.token, x.self)]));
  return { ticket: `${b}.${hmac(`ticket|${b}`)}`, id, macs };
}
const TICKET_RE = /^([A-Za-z0-9_-]{10,8000})\.([0-9a-f]{64})$/;
/** 切符を読む (署名・形・期限)。だめ = 409 (画面は「もう一度 前と後を見る」) */
export function readTicket(ticket, nowMs = Date.now()) {
  const again = (reason, why) => new MasterWriteError(409, reason, `${why}。何も保存していません。「前と後を見る」からもう一度確かめてください`);
  const m = TICKET_RE.exec(String(ticket ?? ''));
  if (!m || !sameHex(m[2], hmac(`ticket|${m[1]}`))) throw again('ticket_invalid', '確かめの切符が無いか、正しくありません (サーバーの入れ替えで古くなった・書き換えた)');
  let t;
  try { t = JSON.parse(Buffer.from(m[1], 'base64url').toString('utf8')); } catch { throw again('ticket_invalid', '確かめの切符を読めません'); }
  if (!t || t.v !== 1 || !UUID_RE.test(String(t.id)) || !Number.isSafeInteger(t.n) || t.n < 1 || t.n > BULK_MAX) throw again('ticket_invalid', '確かめの切符の形が違います');
  if (!(Number(t.exp) > nowMs)) throw again('ticket_expired', `確かめてから ${TICKET_TTL_MS / 60e3} 分を過ぎました`);
  return t;
}
/** 一括の操作の印 (切符の番号・人・項目・値・理由・確かめの瞬間・件数)。request_id はこれから決める = 中身の違う操作が前の結果を受け取らない (Codex R1 M3) */
export const opKeyOf = (t) => sha256(stable({ id: t.id, actor: t.actor, field: t.field, value: t.value, reason: t.reason ?? null, seen: t.seen, n: t.n }));

/** 選んだ商品コード (画面から)。重なりは 1 つに・BULK_MAX 件まで */
export function parseCodes(raw) {
  if (!Array.isArray(raw) || !raw.length) throw bad('商品が選ばれていません');
  const out = [];
  const seen = new Set();
  for (const x of raw) {
    const c = textIn(typeof x === 'string' ? x : String(x ?? ''), { label: '商品コード', field: 'codes', max: 60 });
    if (!c) continue;
    const k = normSku(c);
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(c);
  }
  if (!out.length) throw bad('商品が選ばれていません');
  if (out.length > BULK_MAX) throw new MasterWriteError(413, 'too_many', `1 回にまとめて変えられるのは ${BULK_MAX} 件までです (いま ${out.length} 件)。${out.length - BULK_MAX} 件減らしてください`, { max: BULK_MAX, count: out.length });
  return out;
}

/** 項目と値 (画面から) → 決まった形。値は「同じ値にする」だけ */
export function parseFieldValue(field, raw) {
  if (!Object.prototype.hasOwnProperty.call(BULK_FIELDS, field)) throw bad(`まとめて変えられない項目: ${field}`, 'field');
  if (raw == null || raw === '') throw bad('新しい値を入れてください', 'value');
  switch (field) {
    case 'cost': return intIn(raw, 1, MAX_YEN, '原価', 'value');
    case 'standard_price': return intIn(raw, 1, MAX_YEN, '売価', 'value');
    case 'handling': if (!['active', 'discontinued'].includes(raw)) throw bad('取扱は 取扱中 か 中止', 'value'); return raw;
    case 'sales_class': return intIn(raw, 1, 4, '売上分類', 'value');
    case 'tax_rate': { const t = PARSERS.tax_rate(raw); if (t == null) throw bad('税率は 8% か 10% です', 'value'); return t; }
    case 'primary_supplier': {
      const s = textIn(typeof raw === 'string' ? raw : String(raw), { label: '仕入先', field: 'value', max: 20 });
      if (!s) throw bad('仕入先を選んでください', 'value');
      return canonicalSupplierCode(s);
    }
    default: throw bad('知らない項目', 'field');
  }
}

/**
 * 理由 (原価 = 必須・取扱の中止 = 必須・ほか = 無し)。保存の理由 = 「理由 · 一括 ◯ 件」(変更の記録に残る)
 */
export function parseReason(field, value, raw, total) {
  const reason = textIn(raw == null ? null : String(raw), { label: '理由', field: 'reason', max: BULK_REASON_MAX });
  if (field === 'cost' && !reason) throw bad('原価を変える理由を選んでください', 'reason');
  if (field === 'handling' && value === 'discontinued' && !reason) throw bad('中止にする理由を入れてください', 'reason');
  const n = Number(total);
  const tail = Number.isSafeInteger(n) && n > 0 ? `一括 ${n} 件` : '一括';
  return { reason: reason || null, saveReason: [reason, tail].filter(Boolean).join(' · ') };
}

/** 1 件の保存の欄と値 (saveSku の values) */
function saveValuesOf(field, kind, value, reason) {
  const f = BULK_FIELDS[field][kind === 'set' ? 'set' : 'single'];
  if (!f) return null;
  if (field === 'cost') return { cost: { jpy: value, reason } };
  if (field === 'tax_rate') return { tax_rate: value };
  return { [f]: value };
}

/**
 * 一括のやり直しの確かめに使う「この商品そのもの」の印 = 編集の印から、この一括が変えうる版 (この商品の SKU の版・含むセットの版・構成品の版) を除いたもの。
 * 版の違いは変更の記録 (request_id) で確かめる (bulkForeignChanges)。行の増減・構成・仕入先・原価の行・JAN・登録の状態はここで確かめる
 */
export function bulkSelfToken(cur) {
  const comp = (list) => list.map((c) => [c.child_sku_id, c.qty, c.sort_order ?? c.sort, c.source ?? null]);
  return sha256(stable({
    v: 'bulk-self-1',
    sku: cur.sku_id,
    product: cur.product_id ? [cur.product_id, cur.product_version] : null,
    parent: cur.parent_product_id ? [cur.parent_product_id, cur.parent_version ?? null, cur.parent_set_by ?? null] : [null, cur.parent_set_by ?? null],
    suppliers: cur.supplier_rows.map((x) => [x.supplier_id, x.version, !!x.is_primary, !!x.active, x.reg_state ?? null]),
    costs: cur.cost_rows.map((c) => [c.id, c.cost_jpy, c.cost_source, c.cost_status, c.valid_from, c.valid_to]),
    jan: cur.jan_rows.map((j) => [j.id, j.external_value, j.valid_to]),
    components: comp(cur.components),
    component_request: cur.component_request ? [cur.component_request.id, comp(cur.component_request.rows)] : null,
    parent_sets: cur.parent_sets.map((x) => x.id),
    registration: cur.registration ? [cur.registration.state, cur.registration.state_changed_at] : null,
  }));
}

/** この単品を開いている構成の依頼に入れているセット */
async function requestParentIds(db, skuId) {
  if (!(await regclass(db, 'ops.sku_component_requests'))) return [];
  return (await db.query(`select q.set_sku_id::text as id from ops.sku_component_requests q
     where q.status = 'open' and exists (select 1 from jsonb_array_elements(q.rows) x where (x ->> 'sku_id') = $1::text)`, [String(skuId)])).rows.map((r) => r.id);
}
async function regclass(db, name) {
  return (await db.query('select to_regclass($1) is not null as ok', [name])).rows[0].ok;
}

/**
 * 1 件ずつの材料 (今の値・編集の印・保存できない理由のもと)。読む取引は呼び手 (同じ瞬間の値)
 */
async function readFacts(db, codes, today) {
  const rows = (await db.query(`select s.sku_id::text as sku_id, s.code, s.code_norm from core.skus s
     where s.company_id = $1 and s.code_norm = any(select core.norm_code(x) from unnest($2::text[]) as t(x))`, [COMPANY_ID, codes])).rows;
  const byNorm = new Map(rows.map((r) => [r.code_norm, r]));
  const normOf = new Map((await db.query('select x, core.norm_code(x) as n from unnest($1::text[]) as t(x)', [codes])).rows.map((r) => [r.x, r.n]));
  const hasRegItems = await regclass(db, 'ops.ne_reg_export_items');
  const out = [];
  for (const code of codes) {
    const hit = byNorm.get(normOf.get(code));
    if (!hit) { out.push({ code, found: false }); continue; }
    const cur = await readCurrent(db, hit.sku_id, today);
    // NE に取り込む CSV が出ている列 (保存すると 409 csv_issued)
    const cols = [...new Set(Object.values(fieldsOf(cur.sku_kind)).map((d) => d.csvCol).filter(Boolean))];
    const csv = {};
    for (const r of await issuedCsv(db, cur.code_norm, cols)) (csv[r.col] = csv[r.col] || []).includes(r.export_id) || csv[r.col].push(r.export_id);
    // NE 登録の CSV を配った後 (保存すると 409 reg_csv_issued)。単品の税率は含むセットの CSV にも入る
    let regMine = [];
    let regParents = [];
    if (hasRegItems) {
      const issued = ['issued', 'import_declared', 'partial'];
      regMine = (await db.query('select distinct export_id::text as id from ops.ne_reg_export_items where sku_id = $1 and state = any($2::text[]) order by 1', [cur.sku_id, issued])).rows.map((r) => r.id);
      if (cur.sku_kind === 'single') {
        const parents = [...new Set([...cur.parent_set_ids, ...await requestParentIds(db, cur.sku_id)])];
        if (parents.length) regParents = (await db.query('select distinct export_id::text as id from ops.ne_reg_export_items where sku_id = any($1::bigint[]) and state = any($2::text[]) order by 1', [parents, issued])).rows.map((r) => r.id);
      }
    }
    // 含むセットの先の日の原価 (単品の原価を変えるとセットの合計を今日から計算し直す = 409 set_cost_future)
    const parentFuture = cur.sku_kind === 'single' && cur.parent_set_ids.length
      ? (await db.query(`select k.code, c.valid_from::text as valid_from, c.cost_source from core.sku_costs c join core.skus k on k.sku_id = c.sku_id
           where c.sku_id = any($1::bigint[]) and c.valid_to is null and c.valid_from > $2::date order by k.code_norm`, [cur.parent_set_ids, today])).rows
        .filter((r) => !OVERRIDE_SOURCES.has(r.cost_source)) : [];
    out.push({ code, found: true, cur, csv, regMine, regParents, parentFuture, token: editTokenOf(cur), self: bulkSelfToken(cur) });
  }
  return out;
}

/** 今の値 (項目ごと・見せる形)。セットの取扱 = 今の skus.handling (自身の値は own) */
function currentValue(cur, field) {
  switch (field) {
    case 'cost': return cur.open_cost ? cur.open_cost.cost_jpy : null;
    case 'standard_price': return cur.standard_price;
    case 'handling': return cur.handling;
    case 'sales_class': return cur.sales_class;
    case 'tax_rate': return cur.tax_rate;
    case 'primary_supplier': return cur.primary_supplier;
    default: return null;
  }
}

/**
 * 1 件の判定 (値の無い = 項目の板の「変えられる / 対象外 / 保存できない」・値がある = 変わる / 変わらない)。
 *   verdict: out (対象外) / block (保存できない) / ok (値の前) / same (今と同じ) / chg (変わる)
 *   reason = 機械の理由 (fixGroupOf で直し方の組)・why = 画面の文
 */
export function judgeBulk(f, field, value, { editable } = {}) {
  const F = BULK_FIELDS[field];
  if (!f.found) return { verdict: 'out', reason: 'not_found', why: 'Company DB にありません (消えたか、コードが変わった)' };
  const c = f.cur;
  if (c.sku_kind === 'exception') return { verdict: 'out', reason: 'exception_sku', why: '例外の SKU (NE に無い) は一括では変えません' };
  if (c.registration?.state === 'cancelled') return { verdict: 'out', reason: 'cancelled_sku', why: '登録をやめた商品は変えられません' };
  const saveField = F[c.sku_kind === 'set' ? 'set' : 'single'];
  if (!saveField) return { verdict: 'out', reason: 'out', why: F.setWhy || 'この種類では変えられません' };
  if (c.handling === 'discontinued' && field !== 'handling') return { verdict: 'out', reason: 'out', why: '取扱を中止した商品 (先に取扱中に戻す)' };
  if (editable && editable[c.sku_kind] && editable[c.sku_kind][saveField] === false) {
    return { verdict: 'block', reason: 'before_cutover', why: `切替前です: ${F.label}はまだ NE・/register が正です` };
  }
  if (field === 'cost' && c.open_cost && c.open_cost.valid_from > (f.today || '')) {
    return { verdict: 'block', reason: 'cost_future', why: `先の日の原価があります (${c.open_cost.valid_from} から ${c.open_cost.cost_jpy.toLocaleString('ja-JP')} 円)。1 件の画面で直してください` };
  }
  if (field === 'cost' && f.parentFuture && f.parentFuture.length) {
    return { verdict: 'block', reason: 'set_cost_future', why: `含むセット ${f.parentFuture.map((p) => p.code).join('・')} に先の日の原価があります (${f.parentFuture[0].valid_from} から)` };
  }
  const csvCol = fieldsOf(c.sku_kind)[saveField]?.csvCol;
  if (csvCol && f.csv && f.csv[csvCol] && f.csv[csvCol].length) {
    return { verdict: 'block', reason: 'csv_issued', why: `この商品の${F.label}が入った NE に取り込む CSV (ファイル ${f.csv[csvCol].map((x) => `#${x}`).join('・')}) が出ています` };
  }
  const regFields = REG_CSV_FIELDS[c.sku_kind] || [];
  if (regFields.includes(saveField) && f.regMine && f.regMine.length) {
    return { verdict: 'block', reason: 'reg_csv_issued', why: `NE 登録の CSV (ファイル ${f.regMine.map((x) => `#${x}`).join('・')}) を配った後です` };
  }
  if (field === 'tax_rate' && f.regParents && f.regParents.length) {
    return { verdict: 'block', reason: 'reg_csv_issued', why: `含むセットの NE 登録の CSV (ファイル ${f.regParents.map((x) => `#${x}`).join('・')}) を配った後です` };
  }
  if (field === 'sales_class' && !c.product_id) return { verdict: 'block', reason: 'no_product', why: '商品の行が無いので売上分類を持てません' };
  if (value === undefined) return { verdict: 'ok' };
  if (isSame(c, field, value)) return { verdict: 'same', reason: 'same', why: '今と同じ値なので変えません' };
  return { verdict: 'chg' };
}
function isSame(c, field, value) {
  switch (field) {
    case 'cost': return !!c.open_cost && c.open_cost.cost_jpy === value;   // 1 件の保存 (diffSingle) と同じ = 続いている原価の行と比べる
    case 'standard_price': return c.standard_price === value;
    case 'handling': return c.sku_kind === 'set' ? c.handling_own === value : c.handling === value;
    case 'sales_class': return c.sales_class === value;
    case 'tax_rate': return c.tax_rate === value;
    case 'primary_supplier': return normSku(c.primary_supplier ?? '') === normSku(value ?? '');
    default: return false;
  }
}

/** 画面の持ち主 (DB の active) → 種類ごと・欄ごとの「書ける」(1 件の画面と同じ fieldOwnership)。読めない = 全部閉じる */
export function editableOf(phase, owner, open) {
  const ok = !!owner && owner.readable && !owner.code_behind.length;
  const map = ok ? owner.map : {};
  const isOpenNow = open && ok && newEntryWritable(phase, map);
  const of = (kind) => Object.fromEntries(Object.entries(fieldOwnership(kind, map, isOpenNow)).map(([k, v]) => [k, v.editable]));
  return { single: of('single'), set: of('set'), open: isOpenNow };
}

const kindOf = (f) => (f.found ? f.cur.sku_kind : null);
const itemBase = (f) => ({
  code: f.found ? f.cur.code : f.code, name: f.found ? f.cur.name : null, kind: kindOf(f),
  handling: f.found ? f.cur.handling : null, handling_own: f.found ? f.cur.handling_own ?? null : null,
});

/** 読む取引 (同じ瞬間の値・その間の変更の起点も同じ瞬間) */
async function inSnapshot(db, fn) {
  await db.query('begin isolation level repeatable read read only');
  try {
    const seen = (await db.query('select coalesce(max(event_id), 0)::text as id from events.master_change_events')).rows[0].id;
    return await fn(seen);
  } finally {
    await db.query('rollback');
  }
}

/**
 * 1. 選んだ商品の項目ごとの判定 (値の前)。項目の板の数 (変えられる / 対象外 / 保存できない・Codex M5 = 先の日の原価は最初から「保存できない」) と「今の値」
 * opts = { now, editable (editableOf), suppliers: [{ code, name }] (代表にできる仕入先) }
 */
export async function bulkInspect(db, input, { now = new Date(), editable = null, suppliers = [] } = {}) {
  const codes = parseCodes(input?.codes);
  const today = jstDate(now);
  return inSnapshot(db, async () => {
    const facts = await readFacts(db, codes, today);
    const items = facts.map((f) => {
      f.today = today;
      const fields = {};
      for (const field of Object.keys(BULK_FIELDS)) {
        const j = judgeBulk(f, field, undefined, { editable });
        fields[field] = { verdict: j.verdict, reason: j.reason ?? null, why: j.why ?? null, now: f.found ? currentValue(f.cur, field) : null };
      }
      return { ...itemBase(f), fields };
    });
    return { ok: true, today, max: BULK_MAX, chunk: BULK_CHUNK, items, suppliers, costReasons: COST_REASONS };
  });
}

/** 利益 (1 個あたり・参考) の前後。売価・原価だけ */
function profitPair(cur, field, value) {
  if (!['cost', 'standard_price'].includes(field)) return null;
  const cost = cur.cost_today && KNOWN_COST.has(cur.cost_today.cost_status) ? cur.cost_today.cost_jpy : null;
  const base = { price: cur.standard_price, cost, taxRate: cur.tax_rate, shipping: cur.shipping_cost };
  const a = masterProfit(base);
  const b = masterProfit({ ...base, [field === 'cost' ? 'cost' : 'price']: value });
  return { before: a.ok ? a.profit : null, after: b.ok ? b.profit : null };
}

/**
 * 一緒に変わるセット (原価・税率・取扱)。全部の「変わる」商品の保存の後の予定値で、保存と同じ planSetDerived で計算する (Codex High 3)。
 * 戻り値 = { linked: [{ code, name, col, before, after, why }], setAfter: Map<セットの sku_id, 取扱の予定>, notes }
 */
async function planLinked(db, field, value, facts, today) {
  const F = BULK_FIELDS[field];
  const linked = [];
  const notes = [];
  const setAfter = new Map();
  if (!F.linked) return { linked, setAfter, notes };
  const chg = facts.filter((f) => f.verdict === 'chg');
  const planned = new Map(chg.filter((f) => f.cur.sku_kind === 'single').map((f) => [f.cur.sku_id, value]));
  const ownPlanned = new Map(chg.filter((f) => f.cur.sku_kind === 'set').map((f) => [f.cur.sku_id, value]));
  const itemSetIds = new Set(chg.filter((f) => f.cur.sku_kind === 'set').map((f) => f.cur.sku_id));
  const setIds = new Set([...itemSetIds]);
  for (const f of chg) if (f.cur.sku_kind === 'single') for (const id of f.cur.parent_set_ids) setIds.add(id);
  const what = { tax: F.linked === 'tax', handling: F.linked === 'handling', cost: F.linked === 'cost' };
  for (const id of [...setIds].sort((a, b) => Number(a) - Number(b))) {
    const s = (await db.query(`select s.sku_id::text as sku_id, s.code, s.name, s.tax_rate::text as tax_rate, s.tax_class, s.handling, s.handling_own from core.skus s where s.sku_id = $1`, [id])).rows[0];
    if (!s) continue;
    const open = what.cost ? (await db.query(`select cost_jpy::text as cost_jpy, cost_source, cost_status, valid_from::text as valid_from from core.sku_costs where sku_id = $1 and valid_to is null`, [id])).rows[0] : null;
    const comps = (await setComponentInputs(db, id, today)).map((c) => {
      if (!planned.has(c.sku_id)) return c;
      if (what.cost) return { ...c, cost_jpy: planned.get(c.sku_id) };
      if (what.tax) return { ...c, tax_rate: planned.get(c.sku_id) };
      return { ...c, handling: planned.get(c.sku_id) };
    });
    const set = { ...s, handling_own: ownPlanned.has(id) ? ownPlanned.get(id) : s.handling_own, open_cost: open ? { ...open, cost_jpy: Number(open.cost_jpy) } : null };
    let plan;
    try { plan = planSetDerived(set, comps, what, today); } catch (e) {
      if (e instanceof MasterWriteError) { notes.push(e.message); continue; }
      throw e;
    }
    notes.push(...plan.notes);
    const ch = plan.changes[0];
    if (itemSetIds.has(id)) { setAfter.set(id, ch ? ch.to : s.handling); continue; }   // 選んだセット自身は上の一覧に出す
    if (!ch) continue;
    const why = ch.col === 'handling' ? (ch.to === 'discontinued' ? '構成品が中止になるので、このセットも中止になります' : '構成品が取扱中に戻るので、このセットも取扱中に戻ります')
      : ch.col === 'tax_rate' ? (ch.to.class === 'MIXED' ? '構成品の税率が 8% と 10% で混ざるので、低い方の 8% (MIXED) になります' : '構成品から計算し直します')
        : ch.to == null ? '構成品の原価が足りなくなるので、原価が空になります' : '構成品から計算し直します (今日から)';
    linked.push({ code: s.code, name: s.name, col: ch.col, before: ch.from, after: ch.to, why });
  }
  return { linked, setAfter, notes };
}

/**
 * 2. 前と後 (項目と値を決めた後)。1 件ずつの今の値・新しい値・利益の前後・編集の印 (保存で使う)・一緒に変わるセット (保存と同じ計算)。
 * 読むだけ (同じ瞬間の値)。seen_event_id = この瞬間の変更の記録の番号 (保存の「その間の変更」の起点)
 */
export async function bulkPreview(db, input, { now = new Date(), editable = null, suppliers = null, actor = null, nowMs = Date.now() } = {}) {
  const codes = parseCodes(input?.codes);
  const field = input?.field;
  const value = parseFieldValue(field, input?.value);
  const { reason } = parseReason(field, value, input?.reason, 1);   // 理由が要る項目 (原価・中止) はここで断る (切符に入れる)
  const who = String(actor ?? '').trim().toLowerCase();
  if (!who) throw bad('確かめる人が分からない', 'actor');
  if (field === 'primary_supplier' && suppliers && !suppliers.some((s) => normSku(canonicalSupplierCode(s.code)) === normSku(value))) {
    throw bad(`仕入先 ${value} は選べません (取引中で NE に登録済みの仕入先だけ)`, 'value', 'supplier_not_confirmed');
  }
  const today = jstDate(now);
  return inSnapshot(db, async (seen) => {
    const facts = await readFacts(db, codes, today);
    for (const f of facts) {
      f.today = today;
      Object.assign(f, judgeBulk(f, field, value, { editable }));
    }
    const { linked, setAfter, notes } = await planLinked(db, field, value, facts, today);
    const items = facts.map((f) => {
      const base = { ...itemBase(f), verdict: f.verdict, reason: f.reason ?? null, why: f.why ?? null, group: f.verdict === 'block' || f.verdict === 'out' ? fixGroupOf(f.reason) : null };
      if (!f.found) return base;
      const before = currentValue(f.cur, field);
      const it = { ...base, before, after: f.verdict === 'chg' ? value : before };
      if (f.verdict === 'chg') {
        Object.assign(it, { token: f.token, self: f.self, profit: profitPair(f.cur, field, value) });
        // 選んだセットの取扱 = セット自身の値を変える。セットの取扱は構成品からも決まる (中止の構成品があれば中止のまま)
        if (field === 'handling' && f.cur.sku_kind === 'set') {
          it.after = setAfter.has(f.cur.sku_id) ? setAfter.get(f.cur.sku_id) : value;
          if (it.after !== value) it.note = `セット自身は${HANDLING[value]}にしますが、中止の構成品があるのでセットは${HANDLING[it.after] || it.after}のままです`;
        }
      }
      return it;
    });
    const count = (v) => items.filter((x) => x.verdict === v).length;
    // 切符 (変わる商品があるときだけ)。商品ごとの印は「変わる」の商品だけに付ける = それ以外は保存に送れない
    const chg = items.filter((x) => x.verdict === 'chg');
    let ticket = null;
    if (chg.length) {
      const t = issueTicket({ actor: who, field, value, reason, seen, items: chg, nowMs });
      ticket = t.ticket;
      for (const x of chg) x.mac = t.macs.get(x.code);
    }
    return {
      ok: true, field, value, reason, today, seen_event_id: seen, items, linked, notes: [...new Set(notes)], ticket, ticket_ttl_ms: TICKET_TTL_MS,
      counts: { chg: count('chg'), same: count('same'), out: count('out'), block: count('block'), linked: linked.length },
      max: BULK_MAX, chunk: BULK_CHUNK,
    };
  });
}

/**
 * 3. 保存の中身 (画面から・20 件ずつ)。項目・値・理由・件数・確かめの瞬間は切符から (送ってきた値が切符と違う = 断る)。
 * 商品は切符の印が正しいものだけ (確かめで「変わる」になった商品 = 切符の件数 (200 以下) より多くは送れない)
 */
export function parseApply(input, { nowMs = Date.now() } = {}) {
  const actor = String(input?.actor ?? '').trim().toLowerCase();
  if (!actor) throw bad('保存する人が分からない', 'actor');
  const t = readTicket(input?.ticket, nowMs);
  if (t.actor !== actor) throw new MasterWriteError(403, 'ticket_actor', '確かめた人と保存する人が違います。何も保存していません');
  const mismatch = (label) => new MasterWriteError(400, 'ticket_mismatch', `${label}が確かめたときと違います。何も保存していません。「前と後を見る」からもう一度確かめてください`);
  if (input?.field !== undefined && input.field !== t.field) throw mismatch('項目');
  if (input?.value !== undefined && parseFieldValue(t.field, input.value) !== t.value) throw mismatch('値');
  if (input?.reason !== undefined && (textIn(input.reason == null ? null : String(input.reason), { label: '理由', field: 'reason', max: BULK_REASON_MAX }) ?? null) !== (t.reason ?? null)) throw mismatch('理由');
  if (input?.total !== undefined && Number(input.total) !== t.n) throw mismatch('件数');
  const { field, value, n: total } = t;
  const { reason, saveReason } = parseReason(field, value, t.reason, total);
  const raw = input?.items;
  if (!Array.isArray(raw) || !raw.length) throw bad('送る商品がありません', 'items');
  if (raw.length > BULK_CHUNK) throw bad(`1 回に送るのは ${BULK_CHUNK} 件までです`, 'items');
  const seenCodes = new Set();
  const items = raw.map((x) => {
    const code = textIn(String(x?.code ?? ''), { label: '商品コード', field: 'items', max: 60 });
    if (!code) throw bad('商品コードが空です', 'items');
    if (seenCodes.has(normSku(code))) throw bad(`${code} が 2 回あります`, 'items');
    seenCodes.add(normSku(code));
    const token = String(x?.token ?? '').toLowerCase();
    const self = String(x?.self ?? '').toLowerCase();
    if (!TOKEN_RE.test(token) || !TOKEN_RE.test(self)) throw bad(`${code} の編集の印が無い。前と後を見直してください`, 'items');
    if (!sameHex(String(x?.mac ?? '').toLowerCase(), itemMac(t.id, code, token, self))) {
      throw new MasterWriteError(400, 'ticket_mismatch', `${code} は確かめた「変わる」商品に入っていません (印が違う)。何も保存していません`);
    }
    return { code, token, self, eventId: t.seen };
  });
  // request_id の元 = 一括の操作の印 (切符の番号・人・項目・値・理由・件数)
  return { actor, bulkId: opKeyOf(t), ticketId: t.id, field, value, total, reason, saveReason, items };
}

/** 失敗の形 (画面に出す) */
function failure(code, e, extra = {}) {
  const reason = e instanceof MasterWriteError ? e.reason : e && e.code === '55P03' ? 'locked' : 'error';
  const message = e instanceof MasterWriteError ? e.message
    : reason === 'locked' ? 'ほかの処理が同じ商品を使っていました (何も保存していません)' : 'サーバーエラー (この商品は保存していません)';
  if (!(e instanceof MasterWriteError) && reason === 'error') console.error(`[master-bulk] ${code}: ${e && e.stack || e}`);
  return { code, ok: false, error: { reason, message, group: fixGroupOf(reason), fix: FIX_WORDS[reason] || FIX_GROUPS[fixGroupOf(reason)].label }, ...extra };
}
function success(code, r, extra = {}) {
  return { code, ok: true, no_change: !!r.no_change, replayed: !!r.replayed, changed: r.changed || [], derived: r.derived || [], warnings: r.warnings || [], ...extra };
}
/** 保存の記録 (同じ request_id の前の結果) */
async function requestRow(db, rid) {
  return (await db.query('select actor_id, target_code, status, result, error from ops.master_edit_requests where request_id = $1', [rid])).rows[0] || null;
}

/**
 * その間の変更 (seen の後・upto まで) のうち、この一括でないもの。関わる行 = この商品・含むセット・構成品 (と依頼の構成品) の SKU・商品・原価・構成・仕入先ごとの商品・仕入先。
 * この一括 = 変更の記録の request_id が、同じ人の保存の記録 (ops.master_edit_requests) の target_code と一括の番号から決まる番号
 */
async function bulkForeignChanges(db, { cur, seen, upto, bulkId, actor }) {
  const skuIds = [...new Set([cur.sku_id, ...cur.parent_set_ids, ...cur.components.map((c) => c.child_sku_id), ...(cur.component_request ? cur.component_request.rows.map((r) => r.child_sku_id) : [])])];
  const prodIds = (await db.query('select distinct product_id::text as id from core.skus where sku_id = any($1::bigint[]) and product_id is not null', [skuIds])).rows.map((r) => r.id);
  const supIds = cur.supplier_rows.map((x) => x.supplier_id);
  const ev = (await db.query(`select distinct e.request_id from events.master_change_events e
     where e.event_id > $1::bigint and e.event_id <= $2::bigint and (
       (e.entity_type = 'sku' and e.entity_id = any($3::bigint[]))
       or (e.entity_type = 'product' and e.entity_id = any($4::bigint[]))
       or (e.entity_type = 'sku_cost' and (coalesce(e.new_value ->> 'sku_id', e.old_value ->> 'sku_id') in (select unnest($3::bigint[])::text)
            or e.entity_id in (select sku_cost_id from core.sku_costs where sku_id = any($3::bigint[]))))
       or (e.entity_type = 'sku_component' and (e.entity_key ->> 'parent_sku_id') in (select unnest($3::bigint[])::text))
       or (e.entity_type = 'supplier_sku' and (e.entity_key ->> 'sku_id') = $5::text)
       or (e.entity_type = 'supplier' and e.entity_id = any($6::bigint[])))`, [seen ?? '0', upto, skuIds, prodIds, cur.sku_id, supIds])).rows.map((r) => r.request_id);
  if (!ev.length) return [];
  if (ev.some((r) => r == null)) return ['(request_id の無い変更 = 夜間の処理など)'];
  const recs = new Map((await db.query('select request_id::text as id, target_code, actor_id from ops.master_edit_requests where request_id::text = any($1::text[])', [ev])).rows.map((r) => [r.id, r]));
  return ev.filter((rid) => {
    const r = recs.get(rid);
    if (!r || r.actor_id !== actor) return true;
    return ![0, 1].some((a) => bulkRequestId(bulkId, r.target_code, a) === rid);
  });
}

/**
 * 3. 20 件ずつ保存する。1 件 = 1 回の saveSku (1 取引)。戻り値 = { ok, results: [{ code, ok, ... }] }
 * opts = { now (試験)・open (MASTER_EDIT_OPEN)・today (やり直しの読み直しの日) }
 */
export async function bulkApplyChunk(db, input, opts = {}) {
  const req = parseApply(input, { nowMs: opts.nowMs ?? Date.now() });
  const results = [];
  let broken = null;
  for (const it of req.items) {
    if (broken) { results.push(failure(it.code, new MasterWriteError(503, 'not_sent', 'Company DB との接続が切れたので送っていません'), { not_sent: true })); continue; }
    try {
      results.push(await applyOne(db, req, it, opts));
    } catch (e) {
      // 接続が切れた (saveSku の外の読み) = 残りは送らない (押し直しで続きから)
      broken = e;
      results.push(failure(it.code, e));
    }
  }
  return { ok: true, results };
}

async function applyOne(db, req, it, opts) {
  const pre = (await db.query(`select s.sku_id::text as sku_id, s.code, s.sku_kind, s.handling, s.product_id::text as product_id
      ${(await regclass(db, 'ops.master_registrations')) ? ', (select r.state from ops.master_registrations r where r.sku_id = s.sku_id) as reg_state' : ', null::text as reg_state'}
     from core.skus s where s.company_id = $1 and s.code_norm = core.norm_code($2)`, [COMPANY_ID, it.code])).rows[0];
  if (!pre) return failure(it.code, new MasterWriteError(404, 'not_found', `商品コード ${it.code} は Company DB にありません`));
  // 対象外はサーバーでも止める (画面が送ってきても書かない)
  // 確かめと同じ本物の値で判定する (商品の行が無い単品の売上分類も・#1656 Codex R1 M4)
  const j = judgeBulk({ found: true, cur: { sku_kind: pre.sku_kind, handling: pre.handling, registration: pre.reg_state ? { state: pre.reg_state } : null, product_id: pre.product_id } }, req.field, undefined);
  if (j.verdict === 'out') return failure(it.code, new MasterWriteError(400, j.reason === 'out' ? 'out' : j.reason, `対象外: ${j.why}`));
  if (j.reason === 'no_product') return failure(it.code, new MasterWriteError(400, 'no_product', j.why));
  const values = saveValuesOf(req.field, pre.sku_kind, req.value, req.reason || req.saveReason);
  const rid0 = bulkRequestId(req.bulkId, pre.code, 0);
  const rid1 = bulkRequestId(req.bulkId, pre.code, 1);
  const saveOpts = { open: opts.open === true, now: opts.now };
  // 押し直し: やり直しの保存がもう済んでいれば、その結果 (2 回目は書かない)
  //   request_id は一括の操作の印 (切符の番号・人・項目・値・理由・件数) から決まる = 同じ中身の操作だけがここに来る。人と商品も照らす (違えば request_id_reused)
  const prev1 = await requestRow(db, rid1);
  if (prev1) {
    if (prev1.actor_id !== req.actor || normSku(prev1.target_code ?? '') !== normSku(pre.code)) return failure(it.code, new MasterWriteError(409, 'request_id_reused', '同じ一括の番号で違う人・違う商品の保存がありました'));
    if (prev1.status === 'done') return success(it.code, { ...prev1.result, replayed: true }, { retried: true });
    const x = prev1.error || {};
    return failure(it.code, new MasterWriteError(x.status || 500, x.reason || 'error', x.message || '前の保存は失敗しました'), { retried: true, replayed: true });
  }
  const input = { actor: req.actor, code: pre.code, reason: req.saveReason, values };
  try {
    return success(it.code, await saveSku(db, { ...input, requestId: rid0, seen: { token: it.token, event_id: it.eventId } }, saveOpts));
  } catch (e) {
    if (!(e instanceof MasterWriteError) || e.reason !== 'version_conflict') return failure(it.code, e);
    // version_conflict = この一括のほかの商品の保存 (同じセットの構成品など) が版を上げただけか、読み直して確かめる
    const today = opts.now ? jstDate(opts.now) : (await db.query(`select (now() at time zone 'Asia/Tokyo')::date::text as d`)).rows[0].d;
    const again = await inSnapshot(db, async (upto) => {
      const cur = await readCurrent(db, pre.sku_id, today);
      if (bulkSelfToken(cur) !== it.self) return { ok: false };
      const foreign = await bulkForeignChanges(db, { cur, seen: it.eventId, upto, bulkId: req.bulkId, actor: req.actor });
      if (foreign.length) return { ok: false };
      return { ok: true, token: editTokenOf(cur), eventId: upto };
    });
    if (!again.ok) return failure(it.code, e);
    try {
      // seen.event_id は画面が見た番号のまま (押し直しが 2 つ並んでも保存の中身が同じになるように。その間の変更は上で確かめた)
      return success(it.code, await saveSku(db, { ...input, requestId: rid1, seen: { token: again.token, event_id: it.eventId } }, saveOpts), { retried: true });
    } catch (e2) {
      // 押し直しが 2 つ並んだ: もう片方がこのやり直しを先に済ませた (読み直した印が違う = request_id_reused) = その結果を返す (二重に書かない)
      const done = await requestRow(db, rid1);
      if (done && done.status === 'done' && done.actor_id === req.actor && normSku(done.target_code ?? '') === normSku(pre.code)) return success(it.code, { ...done.result, replayed: true }, { retried: true });
      return failure(it.code, e2, { retried: true });
    }
  }
}
