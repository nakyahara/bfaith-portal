/**
 * read.mjs — マスタ入力画面 (apps/master-edit) が読むもの (一覧・1 つの SKU・構成品の引き当て・変更の記録)。書くのは lib/master-write.mjs
 *
 * 一覧 (画面 A): 検索 (コード・名前・JAN)・区分・状態 (利用可・中止)・未入力 (税率・売上分類・送料・推奨月数・原価)・NE との差あり。
 *   セットの税率と原価 (構成品の合計) と売上分類 (構成品から導く) は「構成品から導いた値」= 画面で * を付ける。
 *   セットの売上分類は保存していない (読むときに lib/master-set-rules.js で導く) ので、売上分類の「未入力」だけは JS で絞る
 *   登録の状態 (0052・⑤-2a) = 下書き・NE 登録待ち・NE 確認済み・配る対象・利用可・要確認・やめた。行が無い = 切替の前の商品 (backfill の前)
 */
import { MASTER_OWNERSHIP } from '../../config/master-ownership.mjs';
import { normSku } from '../../lib/sku-norm.js';
import { readCurrent, setDerivations, editTokenOf, changesSince, fieldOwnership, costAsOfJoin, jstDate, COMPANY_ID } from '../../lib/master-write.mjs';
import { deriveSetSalesClassCdb } from '../../lib/master-set-rules.js';
import { readCutoverPhase, newEntryWritable } from '../../lib/master-cutover.mjs';
import { latestRun } from '../master-decisions/decide.mjs';
import { readCardEvent } from '../../lib/product-hub-outbox.mjs';
import { regItemsOfSku } from '../../lib/master-reg-csv.mjs';

/** 代表の仕入先に選べる仕入先 = 取引中・「NE に登録した」の申告が済んだ (新しい仕入先) か前からある仕入先 (0053) */
async function selectableSuppliers(db) {
  const hasReg = await regclass(db, 'ops.supplier_registrations');
  return (await db.query(`select s.code, s.name from core.suppliers s where s.company_id = $1 and s.active
     ${hasReg ? "and not exists (select 1 from ops.supplier_registrations r where r.supplier_id = s.supplier_id and r.state <> 'ne_confirmed')" : ''} order by s.code`, [COMPANY_ID])).rows;
}

export const LIST_LIMIT = 100;
export const KINDS = Object.freeze({ single: '単品', set: 'セット', exception: '例外' });
export const MISSING = Object.freeze({ tax: '税率', sales: '売上分類', shipping: '送料', reorder: '推奨月数', cost: '原価' });
export const STATES = Object.freeze({ available: '利用可', discontinued: '中止' });
/** 登録の状態 (0052)。none = 状態の行が無い (切替の前の商品。backfill の後は無い = 使えない) */
/** カードの知らせ (0052) の絞り込み (仮レビュー L6): 作成待ち (まだ・失敗)・衝突 */
export const CARD_FILTERS = Object.freeze({ waiting: 'カード作成待ち (まだ・失敗)', conflict: 'カードの衝突' });
export const REG_STATES = Object.freeze({
  draft: '下書き', ne_pending: 'NE登録待ち', ne_confirmed: 'NE確認済み', distributable: '配る対象', available: '登録済み (利用可)',
  quarantined: '要確認 (NEで見つけた)', cancelled: 'やめた', none: '状態なし (切替の前)',
});
const KNOWN_COST = new Set(['COMPLETE', 'OVERRIDDEN']);
const num = (v) => (v == null ? null : Number(v));

async function regclass(db, name) {
  return (await db.query('select to_regclass($1) is not null as ok', [name])).rows[0].ok;
}

/** 一覧の絞り込みを決まった形に (知らない値は捨てる) */
export function normalizeFilters(q = {}) {
  const pick = (v, allowed) => (Object.prototype.hasOwnProperty.call(allowed, v) ? v : '');
  const offset = Math.max(0, Math.min(1e6, Number.parseInt(q.offset, 10) || 0));
  return {
    q: String(q.q ?? '').trim().slice(0, 60),
    kind: pick(String(q.kind ?? ''), KINDS),
    state: pick(String(q.state ?? ''), STATES),
    missing: pick(String(q.missing ?? ''), MISSING),
    reg: pick(String(q.reg ?? ''), REG_STATES),
    card: pick(String(q.card ?? ''), CARD_FILTERS),
    diff: q.diff === '1' ? '1' : '',
    offset,
  };
}

/** 一覧 (画面 A)。{ rows, total, offset, limit, filters, latestRun, diffAvailable } */
export async function listSkus(db, filters, { now = new Date() } = {}) {
  const f = normalizeFilters(filters);
  const today = jstDate(now);
  const params = [COMPANY_ID, today];
  const where = ['s.company_id = $1'];
  if (f.q) {
    // LIKE の % と _ と \ は文字として探す (既定の escape = \)
    const likeOf = (s) => `%${s.replace(/[\\%_]/g, (c) => `\\${c}`)}%`;
    params.push(likeOf(normSku(f.q)));
    const codeLike = params.length;
    params.push(likeOf(f.q));
    const nameLike = params.length;
    params.push(f.q);
    const raw = params.length;
    where.push(`(s.code_norm like $${codeLike} or s.name ilike $${nameLike}
      or exists (select 1 from core.external_ids e where e.entity_type = 'product' and e.entity_id = s.product_id and e.system = 'jan' and e.id_kind = 'jan'
                 and e.valid_to is null and e.external_norm = core.norm_code($${raw})))`);
  }
  if (f.kind) { params.push(f.kind); where.push(`s.sku_kind = $${params.length}`); }
  if (f.state === 'discontinued') where.push(`s.handling = 'discontinued'`);
  if (f.state === 'available') where.push(`s.handling <> 'discontinued'`);
  if (f.missing === 'tax') where.push('s.tax_rate is null');
  if (f.missing === 'shipping') where.push('s.shipping_code is null');
  if (f.missing === 'reorder') where.push('s.reorder_months is null');
  if (f.missing === 'cost') where.push(`coalesce(c.cost_status not in ('COMPLETE', 'OVERRIDDEN'), true)`);
  // 登録の状態とカードは別々の絞り込み (両方 = 両方に合う商品。PR #1566 Codex R2 Low)
  const hasReg = await regclass(db, 'ops.master_registrations');
  if (f.reg) {
    if (!hasReg) where.push(f.reg === 'none' ? 'true' : 'false');
    else if (f.reg === 'none') where.push('not exists (select 1 from ops.master_registrations mr where mr.sku_id = s.sku_id)');
    else { params.push(f.reg); where.push(`exists (select 1 from ops.master_registrations mr where mr.sku_id = s.sku_id and mr.state = $${params.length})`); }
  }
  const hasOutbox = await regclass(db, 'ops.product_hub_outbox');
  if (f.card) {
    if (!hasOutbox) where.push('false');
    else if (f.card === 'waiting') where.push(`exists (select 1 from ops.product_hub_outbox o where o.sku_id = s.sku_id and o.status in ('pending', 'failed'))`);
    else if (f.card === 'conflict') where.push(`exists (select 1 from ops.product_hub_outbox o where o.sku_id = s.sku_id and o.status = 'conflict')`);
  }
  const diffAvailable = await regclass(db, 'ops.master_decision_candidates');
  const run = diffAvailable ? await latestRun(db) : null;
  if (f.diff) {
    if (!run) where.push('false');
    else { params.push(run.compare_run_id); where.push(`s.code_norm in (select code_norm from ops.master_decision_candidates where last_seen_run = $${params.length})`); }
  }
  const rows = (await db.query(`select s.sku_id::text as sku_id, s.code, s.code_norm, s.sku_kind, s.name, s.handling, s.tax_rate::text as tax_rate, s.tax_class,
        s.standard_price_jpy::text as standard_price, s.shipping_code, s.reorder_months::text as reorder_months, s.set_sales_class_override,
        p.sales_class, c.cost_jpy::text as cost_jpy, c.cost_source, c.cost_status,
        (select sp.code from core.supplier_skus x join core.suppliers sp on sp.supplier_id = x.supplier_id where x.sku_id = s.sku_id and x.is_primary order by sp.code limit 1) as primary_supplier,
        ${hasReg ? '(select mr.state from ops.master_registrations mr where mr.sku_id = s.sku_id)' : 'null::text'} as reg_state
      from core.skus s
      left join core.products p on p.product_id = s.product_id
      ${costAsOfJoin('s.sku_id', '$2', 'c')}
     where ${where.join(' and ')}
     order by s.code_norm`, params)).rows;
  // セットの売上分類 (構成品から導く・上書き)
  const setIds = rows.filter((r) => r.sku_kind === 'set').map((r) => r.sku_id);
  const compClasses = new Map();
  if (setIds.length) {
    for (const c of (await db.query(`select c.parent_sku_id::text as parent, p.sales_class from core.sku_components c join core.skus k on k.sku_id = c.child_sku_id
        left join core.products p on p.product_id = k.product_id where c.parent_sku_id = any($1::bigint[])`, [setIds])).rows) {
      if (!compClasses.has(c.parent)) compClasses.set(c.parent, []);
      compClasses.get(c.parent).push(num(c.sales_class));
    }
  }
  let list = rows.map((r) => {
    const isSet = r.sku_kind === 'set';
    return {
      sku_id: r.sku_id, code: r.code, code_norm: r.code_norm, kind: r.sku_kind, name: r.name, handling: r.handling,
      tax_rate: num(r.tax_rate), tax_class: r.tax_class, tax_derived: isSet,
      standard_price: num(r.standard_price), cost: KNOWN_COST.has(r.cost_status) ? Number(r.cost_jpy) : null, cost_derived: isSet && r.cost_source === 'set_calc',
      sales_class: isSet ? deriveSetSalesClassCdb(r.set_sales_class_override, compClasses.get(r.sku_id) || []) : num(r.sales_class),
      sales_derived: isSet && r.set_sales_class_override == null,
      primary_supplier: r.primary_supplier, shipping_code: r.shipping_code, reorder_months: num(r.reorder_months),
      state: r.handling === 'discontinued' ? 'discontinued' : 'available',
      reg_state: r.reg_state ?? 'none',
    };
  });
  if (f.missing === 'sales') list = list.filter((r) => r.sales_class == null && r.kind !== 'exception');
  // ⚠ の印 (NE との差・CSV 待ち・構成の依頼) はこのページの分だけ
  const pageRows = list.slice(f.offset, f.offset + LIST_LIMIT);
  const norms = pageRows.map((r) => r.code_norm);
  const diffSet = new Set(); const csvSet = new Set(); const reqSet = new Set();
  if (norms.length && run) for (const r of (await db.query('select distinct code_norm from ops.master_decision_candidates where last_seen_run = $1 and code_norm = any($2::text[])', [run.compare_run_id, norms])).rows) diffSet.add(r.code_norm);
  if (norms.length && await regclass(db, 'ops.ne_csv_export_rows')) for (const r of (await db.query('select distinct code_norm from ops.ne_csv_export_rows where reserved and code_norm = any($1::text[])', [norms])).rows) csvSet.add(r.code_norm);
  const pageSets = pageRows.filter((r) => r.kind === 'set').map((r) => r.sku_id);
  if (pageSets.length && await regclass(db, 'ops.sku_component_requests')) for (const r of (await db.query(`select set_sku_id::text as id from ops.sku_component_requests where status = 'open' and set_sku_id = any($1::bigint[])`, [pageSets])).rows) reqSet.add(r.id);
  const cardOf = new Map();
  if (pageRows.length && hasOutbox) for (const r of (await db.query(`select sku_id::text as id, status from ops.product_hub_outbox where status <> 'done' and sku_id = any($1::bigint[])`, [pageRows.map((x) => x.sku_id)])).rows) cardOf.set(r.id, r.status);
  const CARD_FLAG = { pending: 'カード作成待ち', failed: 'カード作成待ち (失敗)', conflict: 'カードの衝突' };
  for (const r of pageRows) r.flags = [...(diffSet.has(r.code_norm) ? ['NEとの差'] : []), ...(csvSet.has(r.code_norm) ? ['CSV待ち'] : []), ...(reqSet.has(r.sku_id) ? ['構成の依頼'] : []),
    ...(cardOf.has(r.sku_id) ? [CARD_FLAG[cardOf.get(r.sku_id)]] : [])];
  return { rows: pageRows, total: list.length, offset: f.offset, limit: LIST_LIMIT, filters: f, latestRun: run, diffAvailable };
}

/** 1 つの SKU の画面 (B・C) に出すもの。無ければ null。open = env MASTER_EDIT_OPEN (切替の段階 new_open と両方で欄が開く) */
export async function readSkuPage(db, code, { now = new Date(), ownership = MASTER_OWNERSHIP, open = false } = {}) {
  const today = jstDate(now);
  // 画面の値と「その間の変更」の起点は同じ瞬間に読む (読む取引を 1 つに)
  await db.query('begin isolation level repeatable read read only');
  try {
    const seenEventId = (await db.query('select coalesce(max(event_id), 0)::text as id from events.master_change_events')).rows[0].id;
    const phase = await readCutoverPhase(db);
    const id = (await db.query('select sku_id::text as id from core.skus where company_id = $1 and code_norm = core.norm_code($2)', [COMPANY_ID, String(code ?? '')])).rows[0]?.id;
    if (!id) return null;
    const cur = await readCurrent(db, id, today);
    const costs = (await db.query(`select cost_jpy::text as cost_jpy, cost_source, cost_status, valid_from::text as valid_from, valid_to::text as valid_to, reason,
          created_by_type, created_by_id, created_at::text as created_at
        from core.sku_costs where sku_id = $1 order by valid_from desc, created_at desc, sku_cost_id desc limit 30`, [id])).rows.map((c) => ({ ...c, cost_jpy: Number(c.cost_jpy) }));
    const suppliers = (await db.query(`select s.code, s.name, x.is_primary, x.vendor_code from core.supplier_skus x join core.suppliers s on s.supplier_id = x.supplier_id
       where x.sku_id = $1 order by x.is_primary desc, s.code`, [id])).rows;
    const activeSuppliers = cur.sku_kind === 'single' ? await selectableSuppliers(db) : [];
    const jan = cur.product_id ? (await db.query(`select external_value from core.external_ids where entity_type = 'product' and entity_id = $1 and system = 'jan' and id_kind = 'jan' and valid_to is null order by external_value`, [cur.product_id])).rows.map((r) => r.external_value) : [];
    const usedIn = cur.sku_kind === 'single'
      ? (await db.query(`select p.code, p.name, c.qty from core.sku_components c join core.skus p on p.sku_id = c.parent_sku_id where c.child_sku_id = $1 order by p.code_norm limit 50`, [id])).rows.map((r) => ({ ...r, qty: Number(r.qty) }))
      : [];
    const amazon = Number((await db.query(`select count(distinct lc.listing_id)::int as n from core.listing_components lc join core.listings l on l.listing_id = lc.listing_id
       where lc.sku_id = $1 and l.mall in ('amazon', 'amazon_us')`, [id])).rows[0].n);
    const breaches = cur.sku_kind === 'set' && await regclass(db, 'ops.sku_component_breaches')
      ? (await db.query(`select kind, details, created_at::text as created_at from ops.sku_component_breaches where set_sku_id = $1 and status = 'open' order by breach_id`, [id])).rows : [];
    const csvRows = (await regclass(db, 'ops.ne_csv_export_rows'))
      ? (await db.query('select col, child, source, export_id::text as export_id from ops.ne_csv_export_rows where reserved and code_norm = $1 order by col, child', [cur.code_norm])).rows : [];
    const card = await readCardEvent(db, id);
    const regItems = await regItemsOfSku(db, id);
    return {
      cur, costs, suppliers, activeSuppliers, jan, usedIn, amazon, csvRows, today, card, regItems,
      state: cur.handling === 'discontinued' ? 'discontinued' : 'available',
      derived: cur.sku_kind === 'set' ? setDerivations(cur) : null,
      fields: fieldOwnership(cur.sku_kind, ownership, open && newEntryWritable(phase, ownership)),
      breaches,
      phase,
      token: editTokenOf(cur),
      seenEventId,
    };
  } finally {
    await db.query('rollback');
  }
}

/** 新商品の登録 (画面 D) に出すもの: 切替の段階・有効な仕入先・backfill 済みか (新商品の登録の前提) */
export async function readNewPage(db) {
  const phase = await readCutoverPhase(db);
  const activeSuppliers = await selectableSuppliers(db);
  const backfillDone = (await regclass(db, 'ops.master_registration_backfill'))
    ? Number((await db.query('select count(*)::int as n from ops.master_registration_backfill')).rows[0].n) === 1 : false;
  return { phase, activeSuppliers, backfillDone };
}

/** 構成品を足すときの引き当て (コード → 名前・種類・税率・分類・原価・取扱) */
export async function lookupSku(db, code, { now = new Date() } = {}) {
  const today = jstDate(now);
  const r = (await db.query(`select k.code, k.name, k.sku_kind, k.tax_rate::text as tax_rate, k.handling, k.standard_price_jpy::text as standard_price, p.sales_class,
        x.cost_jpy::text as cost_jpy, x.cost_status
      from core.skus k left join core.products p on p.product_id = k.product_id
      ${costAsOfJoin('k.sku_id', '$3', 'x')}
     where k.company_id = $1 and k.code_norm = core.norm_code($2)`, [COMPANY_ID, String(code ?? ''), today])).rows[0];
  if (!r) return null;
  return { code: r.code, name: r.name, kind: r.sku_kind, tax_rate: num(r.tax_rate), handling: r.handling, standard_price: num(r.standard_price),
    sales_class: num(r.sales_class), cost_jpy: KNOWN_COST.has(r.cost_status) ? Number(r.cost_jpy) : null };
}

/** 変更の記録 (新しい順に 200 件まで) と構成の依頼 */
export async function skuHistory(db, code) {
  const r = (await db.query(`select s.sku_id::text as sku_id, s.code, s.name, s.sku_kind, s.product_id::text as product_id from core.skus s
     where s.company_id = $1 and s.code_norm = core.norm_code($2)`, [COMPANY_ID, String(code ?? '')])).rows[0];
  if (!r) return null;
  const events = await changesSince(db, { skuId: r.sku_id, productId: r.product_id, sinceEventId: null, limit: 200 });
  const requests = r.sku_kind === 'set' && await regclass(db, 'ops.sku_component_requests')
    ? (await db.query(`select rows, status, close_reason, requested_by, reason, created_at::text as created_at, closed_at::text as closed_at
         from ops.sku_component_requests where set_sku_id = $1 order by created_at desc limit 50`, [r.sku_id])).rows : [];
  return { sku: r, events: events.reverse(), requests };
}
