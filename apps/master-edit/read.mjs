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
import { foldSearch, foldSql, likeOf } from './search-fold.mjs';
import { backorderOf, backorderKeys, stockOf, buildableOf } from './extras.mjs';
import { TOKEN_RE } from './search-token.mjs';
import { readCurrent, setDerivations, editTokenOf, changesSince, fieldOwnership, costAsOfJoin, jstDate, COMPANY_ID, fieldsOf, REG_CSV_FIELDS, issuedCsv, OVERRIDE_SOURCES } from '../../lib/master-write.mjs';
import { deriveSetSalesClassCdb } from '../../lib/master-set-rules.js';
import { readCutoverPhase, newEntryWritable } from '../../lib/master-cutover.mjs';
import { latestRun } from '../master-decisions/decide.mjs';
import { readCardEvent } from '../../lib/product-hub-outbox.mjs';
import { regItemsOfSku } from '../../lib/master-reg-csv.mjs';
import { hasRegisteredOn, LIST_SORTS, listOrderBy, parseNeCreationDate, REGISTERED_ON_SOURCES } from '../../lib/sku-registered-on.mjs';

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

/** 詳細検索の「複数」の欄で受ける数の上限 (貼り付けた Excel の列。URL に載せるので多すぎない数) */
export const MULTI_MAX = 500;
export const TAX_FILTERS = Object.freeze({ 8: '8%', 10: '10%' });
export const SALES_FILTERS = Object.freeze({ 1: '1 自社', 2: '2 取引先限定', 3: '3 仕入', 4: '4 輸出' });
/** 詳細検索の項目 (URL のクエリの名前)。これが 1 つでも入っていれば詳細検索の板を開いておく */
export const ADV_KEYS = Object.freeze(['codes', 'jans', 'sups', 'parents', 'name', 'cost_min', 'cost_max', 'price_min', 'price_max', 'stock_min', 'stock_max', 'tax', 'sales', 'po', 'reg_from', 'reg_to']);
/**
 * 改行・カンマ・空白・タブ・読点で区切った値 (前後の空白を除く・重複は 1 つ・空は捨てる)。
 * limit = 取り出す数の上限 (そこで走査を止める = 巨大な入力でも長く止まらない。#1620 Codex R2)。重複は Set で O(n)
 */
export function splitMulti(s, limit = Infinity) {
  const out = [];
  const seen = new Set();
  const re = /[^\s,、，;；]+/g;
  const str = String(s ?? '');
  let m;
  while (out.length < limit && (m = re.exec(str))) {
    const v = m[0];
    if (!seen.has(v)) { seen.add(v); out.push(v); }
  }
  return out;
}
/** 複数の欄 = 1 行 1 つ。MULTI_MAX + 1 件まで (1 件多く取って「500 件まで」を知らせる) */
const multiText = (v) => { const xs = splitMulti(v, MULTI_MAX + 1); return xs.length ? xs.join('\n') : ''; };
const dateText = (v) => parseNeCreationDate(String(v ?? '').trim().slice(0, 20)) || '';
const intText = (v) => { const t = String(v ?? '').replace(/[０-９]/g, (c) => String.fromCharCode(c.charCodeAt(0) - 0xFEE0)).replace(/[,，\s円]/g, ''); return /^\d{1,9}$/.test(t) ? String(Number(t)) : ''; };

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
    sort: pick(String(q.sort ?? ''), LIST_SORTS),   // 0057: '' = コード順 / reg_desc = 登録日の新しい順 (① の SQL の order by = ページ分けの前)
    // 詳細検索 (10/5 中原さん「NE の商品詳細検索のような」)。複数の欄は 1 行 1 つの文字に (URL に載る形)
    codes: multiText(q.codes), jans: multiText(q.jans), sups: multiText(q.sups), parents: multiText(q.parents),
    name: String(q.name ?? '').trim().slice(0, 200),   // POST の入口の上限 (SEARCH_VALUE_MAX.name = 200) と同じ (#1620 Codex R3 Low)
    cost_min: intText(q.cost_min), cost_max: intText(q.cost_max), price_min: intText(q.price_min), price_max: intText(q.price_max),
    stock_min: intText(q.stock_min), stock_max: intText(q.stock_max),
    tax: pick(String(q.tax ?? ''), TAX_FILTERS),
    sales: pick(String(q.sales ?? ''), SALES_FILTERS),
    po: q.po === '1' ? '1' : '',
    // 登録日の範囲 (0057)。'YYYY-MM-DD' (input type=date) か 'YYYY/M/D'。読めない日付は捨てる
    reg_from: dateText(q.reg_from), reg_to: dateText(q.reg_to),
    // 長い詳細検索の条件の印 (search-token.mjs・router が中身に戻してから渡す)。形だけ確かめる
    s: TOKEN_RE.test(String(q.s ?? '')) ? String(q.s) : '',
    offset,
  };
}

/**
 * その日の原価 (SKU ごとに 1 行) の CTE の中身。costAsOfJoin (lib/master-write.mjs) と同じ行を選ぶ
 * (valid_from が新しい → created_at が新しい → sku_cost_id が大きい)。原価の表を 1 回だけ走査する (#1589 Codex R1 M1)。
 * dayP = その日の $番号・idsP = 絞る sku_id の配列の $番号 (null = 全部)
 */
const costTodaySql = (dayP, idsP = null) => `select distinct on (y.sku_id) y.sku_id, y.cost_jpy, y.cost_source, y.cost_status
    from core.sku_costs y
   where y.valid_from <= $${dayP}::date and (y.valid_to is null or y.valid_to >= $${dayP}::date)${idsP ? ` and y.sku_id = any($${idsP}::bigint[])` : ''}
   order by y.sku_id, y.valid_from desc, y.created_at desc, y.sku_cost_id desc`;

/**
 * 一覧 (画面 A)。{ rows, total, offset, limit, filters, latestRun, diffAvailable }
 * 🚨 速さ (10/5 中原さん「一覧に戻るのが遅い」): 前は全部の SKU (7,377) に SKU ごとの lateral (原価)・代表の仕入先の副問い合わせを付けて読み、
 *    JS で 100 件に切っていた = 原価の表を SKU の数だけ走査する (PGlite・原価 73,770 行で 65 秒)。
 *    今は ① 絞り込みと並びだけの軽い読み (原価が要るのは「原価が未入力」のときだけ・1 回の集合) → ② このページの 100 件だけ中身を読む (原価・仕入先も 1 回の集合)。
 *    値・並び・件数は前と同じ (試験 test-master-edit で前の形と比べる)
 */
export async function listSkus(db, filters, { now = new Date(), extras = {} } = {}) {
  const f = normalizeFilters(filters);
  const today = jstDate(now);
  const params = [COMPANY_ID];
  const where = ['s.company_id = $1'];
  if (f.q) {
    // コード = normSku (全角→半角・小文字)・名前 = かなの同一視 (search-fold.mjs: NFKC・小文字・ひらがな→カタカナ)・JAN = ぴったり
    params.push(likeOf(normSku(f.q)));
    const codeLike = params.length;
    const fold = foldSql(params);
    params.push(likeOf(foldSearch(f.q)));
    const nameLike = params.length;
    params.push(f.q);
    const raw = params.length;
    where.push(`(s.code_norm like $${codeLike} or ${fold('s.name')} like $${nameLike}
      or exists (select 1 from core.external_ids e where e.entity_type = 'product' and e.entity_id = s.product_id and e.system = 'jan' and e.id_kind = 'jan'
                 and e.valid_to is null and e.external_norm = core.norm_code($${raw})))`);
  }
  if (f.kind) { params.push(f.kind); where.push(`s.sku_kind = $${params.length}`); }
  if (f.state === 'discontinued') where.push(`s.handling = 'discontinued'`);
  if (f.state === 'available') where.push(`s.handling <> 'discontinued'`);
  if (f.missing === 'tax') where.push('s.tax_rate is null');
  if (f.missing === 'shipping') where.push('s.shipping_code is null');
  if (f.missing === 'reorder') where.push('s.reorder_months is null');
  let costCte = '';
  if (f.missing === 'cost' || f.cost_min || f.cost_max) {
    params.push(today);
    costCte = `with c as (${costTodaySql(params.length)}) `;
    if (f.missing === 'cost') where.push(`coalesce(c.cost_status not in ('COMPLETE', 'OVERRIDDEN'), true)`);
    // 原価の範囲 = 一覧に出す原価 (その日の原価で、決まっている = COMPLETE / OVERRIDDEN) だけ。未入力は範囲に入らない
    if (f.cost_min) { params.push(Number(f.cost_min)); where.push(`c.cost_status in ('COMPLETE', 'OVERRIDDEN') and c.cost_jpy >= $${params.length}`); }
    if (f.cost_max) { params.push(Number(f.cost_max)); where.push(`c.cost_status in ('COMPLETE', 'OVERRIDDEN') and c.cost_jpy <= $${params.length}`); }
  }
  // ── 詳細検索 (10/5)。複数の値は 1 本の SQL の = any(配列) ──
  const notFound = [];
  let multiCut = false;
  const multi = (s) => { const xs = splitMulti(s); if (xs.length > MULTI_MAX) multiCut = true; return xs.slice(0, MULTI_MAX); };
  if (f.codes) {
    // 商品コード (複数) = ぴったり (大文字小文字・全角半角は同じ = NE のコードの正規化)。見つからないコードは別に返す
    const raw = multi(f.codes);
    const norms = raw.map((x) => normSku(x));
    const found = new Set((await db.query('select code_norm from core.skus where company_id = $1 and code_norm = any($2::text[])', [COMPANY_ID, norms])).rows.map((r) => r.code_norm));
    raw.forEach((x, i) => { if (!found.has(norms[i])) notFound.push(x); });
    params.push(norms); where.push(`s.code_norm = any($${params.length}::text[])`);
  }
  if (f.jans) {
    params.push(multi(f.jans).map((x) => normSku(x)));
    where.push(`exists (select 1 from core.external_ids e where e.entity_type = 'product' and e.entity_id = s.product_id and e.system = 'jan' and e.id_kind = 'jan'
                 and e.valid_to is null and e.external_norm = any($${params.length}::text[]))`);
  }
  if (f.sups) {
    // 仕入先コード (複数) = 代表の仕入先 (一覧の「仕入先」と同じ)。先頭の 0 の有無は同じ ('0001' と '1')
    const xs = multi(f.sups).map((x) => normSku(x));
    params.push(xs.map((x) => x.replace(/^0+(?=.)/, '')));
    where.push(`exists (select 1 from core.supplier_skus x join core.suppliers sp on sp.supplier_id = x.supplier_id
                 where x.sku_id = s.sku_id and x.is_primary and regexp_replace(sp.code_norm, '^0+(?=.)', '') = any($${params.length}::text[]))`);
  }
  if (f.parents) {
    // 代表 (親) の商品コード (複数) = その代表の商品 (子) と代表そのもの
    params.push(multi(f.parents).map((x) => normSku(x)));
    where.push(`(s.code_norm = any($${params.length}::text[])
      or exists (select 1 from core.skus ps where ps.company_id = s.company_id and ps.sku_kind = 'single' and ps.product_id = p.parent_product_id and ps.code_norm = any($${params.length}::text[])))`);
  }
  if (f.name) {
    const fold = foldSql(params);
    params.push(likeOf(foldSearch(f.name)));
    where.push(`${fold('s.name')} like $${params.length}`);
  }
  if (f.price_min) { params.push(Number(f.price_min)); where.push(`s.standard_price_jpy >= $${params.length}`); }
  if (f.price_max) { params.push(Number(f.price_max)); where.push(`s.standard_price_jpy <= $${params.length}`); }
  if (f.tax) { params.push(Number(f.tax) / 100); where.push(`s.tax_rate = $${params.length}::numeric`); }
  // 注文残あり (発注アプリの台帳)。読めないときは何も当てない (読めないことは画面に出す)
  if (f.po) {
    if (!extras.backorders || !extras.backorders.ok) where.push('false');
    else { params.push(backorderKeys(extras.backorders)); where.push(`lower(trim(s.code)) = any($${params.length}::text[])`); }
  }
  // 在庫 (ロジザード) の範囲 = 一覧に出す値で絞る: 単品・例外 = そのコードの在庫 (写しに無いコード = 0)・
  //   セット = 構成品から作れる数 (下で JS。SQL では通しておく。#1620 Codex R1 M1)。読めないときは何も当てない
  const stockRange = (f.stock_min || f.stock_max) ? { lo: f.stock_min ? Number(f.stock_min) : -Infinity, hi: f.stock_max ? Number(f.stock_max) : Infinity } : null;
  if (stockRange) {
    const st = extras.stock;
    if (!st || !st.ok) where.push('false');
    else {
      const { lo, hi } = stockRange;
      params.push([...st.map].filter(([, n]) => n >= lo && n <= hi).map(([k]) => k));
      const inRange = params.length;
      if (lo <= 0 && hi >= 0) {
        // 0 が範囲に入る = 写しに無いコード (在庫 0) も当てる。🚨 使わない $番号を足さない (型が決まらず SQL が落ちる)
        params.push([...st.map.keys()]);
        where.push(`(s.sku_kind = 'set' or s.code_norm = any($${inRange}::text[]) or not (s.code_norm = any($${params.length}::text[])))`);
      } else where.push(`(s.sku_kind = 'set' or s.code_norm = any($${inRange}::text[]))`);
    }
  }
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
  const hasRegOn = await hasRegisteredOn(db);   // 0057 の前の DB = 登録日の列は無い = コード順・登録日の範囲は何も当てない
  // 登録日の範囲 (0057)。空 (分からない) は範囲に入らない
  if (f.reg_from || f.reg_to) {
    if (!hasRegOn) where.push('false');
    else {
      if (f.reg_from) { params.push(f.reg_from); where.push(`s.registered_on >= $${params.length}::date`); }
      if (f.reg_to) { params.push(f.reg_to); where.push(`s.registered_on <= $${params.length}::date`); }
    }
  }
  if (f.diff) {
    if (!run) where.push('false');
    else { params.push(run.compare_run_id); where.push(`s.code_norm in (select code_norm from ops.master_decision_candidates where last_seen_run = $${params.length})`); }
  }
  // ① 絞り込みと並び (軽い列だけ)。並び (コード順 / 登録日の新しい順 = 空は最後・同じ日はコード順) はここの order by = ページ分けの前。
  //   この後の JS の絞り込み (売上分類・セットの作れる数) は filter だけ = 並びを変えない
  const keys = (await db.query(`${costCte}select s.sku_id::text as sku_id, s.sku_kind, s.set_sales_class_override, p.sales_class
      from core.skus s
      left join core.products p on p.product_id = s.product_id
      ${costCte ? 'left join c on c.sku_id = s.sku_id' : ''}
     where ${where.join(' and ')}
     order by ${listOrderBy(f.sort, { alias: 's', hasColumn: hasRegOn })}`, params)).rows;
  /** セットの構成品の売上分類 (導く材料)。ids = セットの sku_id → Map<sku_id, [分類]> */
  const compClassesOf = async (ids) => {
    const m = new Map();
    if (!ids.length) return m;
    for (const c of (await db.query(`select c.parent_sku_id::text as parent, p.sales_class from core.sku_components c join core.skus k on k.sku_id = c.child_sku_id
        left join core.products p on p.product_id = k.product_id where c.parent_sku_id = any($1::bigint[])`, [ids])).rows) {
      if (!m.has(c.parent)) m.set(c.parent, []);
      m.get(c.parent).push(num(c.sales_class));
    }
    return m;
  };
  const salesOf = (r, comp) => (r.sku_kind === 'set' ? deriveSetSalesClassCdb(r.set_sales_class_override, comp.get(r.sku_id) || []) : num(r.sales_class));
  let matched = keys;
  let compClasses = null;
  // 売上分類の未入力・売上分類で絞る = セットは構成品から導く (保存していない) ので、絞った全部のセットを導いてから JS で絞る
  if (f.missing === 'sales' || f.sales) {
    compClasses = await compClassesOf(keys.filter((r) => r.sku_kind === 'set').map((r) => r.sku_id));
    if (f.missing === 'sales') matched = matched.filter((r) => salesOf(r, compClasses) == null && r.sku_kind !== 'exception');
    if (f.sales) matched = matched.filter((r) => salesOf(r, compClasses) === Number(f.sales));
  }
  /** セットの構成品 (作れる数の材料)。ids = セットの sku_id → Map<sku_id, [{ code_norm, qty }]> */
  const compsOfSets = async (ids) => {
    const m = new Map();
    if (!ids.length || !extras.stock || !extras.stock.ok) return m;
    for (const c of (await db.query(`select c.parent_sku_id::text as parent, k.code_norm, c.qty from core.sku_components c join core.skus k on k.sku_id = c.child_sku_id
        where c.parent_sku_id = any($1::bigint[])`, [ids])).rows) {
      if (!m.has(c.parent)) m.set(c.parent, []);
      m.get(c.parent).push({ code_norm: c.code_norm, qty: Number(c.qty) });
    }
    return m;
  };
  let compsOf = null;
  if (stockRange && extras.stock && extras.stock.ok) {
    // セットは表示と同じ「作れる数」で範囲に入るか (作れる数が決まらない = 構成が無い は入れない)。ページ分けの前
    compsOf = await compsOfSets(matched.filter((r) => r.sku_kind === 'set').map((r) => r.sku_id));
    matched = matched.filter((r) => {
      if (r.sku_kind !== 'set') return true;
      const b = buildableOf(extras.stock, compsOf.get(r.sku_id));
      return b != null && b >= stockRange.lo && b <= stockRange.hi;
    });
  }
  const pageIds = matched.slice(f.offset, f.offset + LIST_LIMIT).map((r) => r.sku_id);
  // ② このページの分だけ中身を読む
  const rowsById = new Map();
  if (pageIds.length) {
    const rows = (await db.query(`with c as (${costTodaySql(2, 1)}),
        ps as (select distinct on (x.sku_id) x.sku_id, sp.code from core.supplier_skus x join core.suppliers sp on sp.supplier_id = x.supplier_id
                where x.sku_id = any($1::bigint[]) and x.is_primary order by x.sku_id, sp.code)
      select s.sku_id::text as sku_id, s.code, s.code_norm, s.sku_kind, s.name, s.handling, s.tax_rate::text as tax_rate, s.tax_class,
        s.standard_price_jpy::text as standard_price, s.shipping_code, s.reorder_months::text as reorder_months, s.set_sales_class_override,
        p.sales_class, c.cost_jpy::text as cost_jpy, c.cost_source, c.cost_status, ps.code as primary_supplier,
        ${hasReg ? '(select mr.state from ops.master_registrations mr where mr.sku_id = s.sku_id)' : 'null::text'} as reg_state,
        ${hasRegOn ? 's.registered_on::text as registered_on, s.registered_on_source' : 'null::text as registered_on, null::text as registered_on_source'}
      from core.skus s
      left join core.products p on p.product_id = s.product_id
      left join c on c.sku_id = s.sku_id
      left join ps on ps.sku_id = s.sku_id
     where s.sku_id = any($1::bigint[])`, [pageIds, today])).rows;
    for (const r of rows) rowsById.set(r.sku_id, r);
  }
  if (!compClasses) compClasses = await compClassesOf(pageIds.filter((id) => rowsById.get(id)?.sku_kind === 'set'));
  // セットの構成品 (作れる数の材料。このページのセットだけ・在庫が読めたときだけ)
  if (!compsOf) compsOf = await compsOfSets(pageIds.filter((id) => rowsById.get(id)?.sku_kind === 'set'));
  const pageRows = pageIds.map((id) => rowsById.get(id)).filter(Boolean).map((r) => {
    const isSet = r.sku_kind === 'set';
    return {
      sku_id: r.sku_id, code: r.code, code_norm: r.code_norm, kind: r.sku_kind, name: r.name, handling: r.handling,
      tax_rate: num(r.tax_rate), tax_class: r.tax_class, tax_derived: isSet,
      standard_price: num(r.standard_price), cost: KNOWN_COST.has(r.cost_status) ? Number(r.cost_jpy) : null, cost_derived: isSet && r.cost_source === 'set_calc',
      sales_class: salesOf(r, compClasses),
      sales_derived: isSet && r.set_sales_class_override == null,
      primary_supplier: r.primary_supplier, shipping_code: r.shipping_code, reorder_months: num(r.reorder_months),
      state: r.handling === 'discontinued' ? 'discontinued' : 'available',
      reg_state: r.reg_state ?? 'none',
      comp_count: isSet ? (compClasses.get(r.sku_id) || []).length : null,
      registered_on: r.registered_on ?? null, registered_on_source: r.registered_on_source ?? null,
      // 参考 (ほかのアプリの値・読めなければ null): 注文残 = 発注アプリの台帳 / 在庫 = ロジザード (セットは作れる数)
      backorder: extras.backorders ? backorderOf(extras.backorders, r.code) : null,
      stock: isSet ? null : (extras.stock ? stockOf(extras.stock, r.code_norm) : null),
      buildable: isSet ? buildableOf(extras.stock, compsOf.get(r.sku_id)) : null,
    };
  });
  // ⚠ の印 (NE との差・CSV 待ち・構成の依頼) はこのページの分だけ
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
  return { rows: pageRows, total: matched.length, offset: f.offset, limit: LIST_LIMIT, filters: f, latestRun: run, diffAvailable, notFound, multiCut, sorts: LIST_SORTS, registeredOnAvailable: hasRegOn };
}

/**
 * 一覧の札の数 (会社全体・絞り込みとは別。1 回の集計)。売上分類の未入力はセットを JS で導くので数えない (札は数なしで出す)。
 * 表が無い DB (0052 の前など) の札は null (= 数を出さない)
 * 🚨 その日の原価は SKU ごとの lateral (costAsOfJoin) にしない = sku_costs を SKU の数だけ走査する (#1589 Codex R1 M1)。
 *    原価の表を 1 回だけ走査して DISTINCT ON (sku_id) で「その日の原価」を作り、SKU に join する。並びは costAsOfJoin と同じ
 *    (valid_from が新しい → created_at が新しい → sku_cost_id が大きい) = 同じ行を選ぶ。ほかの札も SKU ごとの exists にしない (1 回の集合)
 */
export async function listCounts(db, { now = new Date() } = {}) {
  const today = jstDate(now);
  const hasReg = await regclass(db, 'ops.master_registrations');
  const hasOutbox = await regclass(db, 'ops.product_hub_outbox');
  const diffAvailable = await regclass(db, 'ops.master_decision_candidates');
  const run = diffAvailable ? await latestRun(db) : null;
  const params = [COMPANY_ID, today];
  if (run) params.push(run.compare_run_id);
  const r = (await db.query(`with c as (
        ${costTodaySql(2)})
      ${hasReg ? ", rd as (select distinct sku_id from ops.master_registrations where state = 'draft')" : ''}
      ${hasOutbox ? ", ow as (select distinct sku_id from ops.product_hub_outbox where status in ('pending', 'failed')), oc as (select distinct sku_id from ops.product_hub_outbox where status = 'conflict')" : ''}
      ${run ? ', dc as (select distinct code_norm from ops.master_decision_candidates where last_seen_run = $3)' : ''}
      select count(*)::int as n_all,
        count(*) filter (where s.sku_kind = 'single')::int as n_single, count(*) filter (where s.sku_kind = 'set')::int as n_set,
        count(*) filter (where s.sku_kind = 'exception')::int as n_exception,
        count(*) filter (where s.handling = 'discontinued')::int as discontinued,
        count(*) filter (where s.tax_rate is null)::int as miss_tax, count(*) filter (where s.shipping_code is null)::int as miss_shipping,
        count(*) filter (where s.reorder_months is null)::int as miss_reorder,
        count(*) filter (where coalesce(c.cost_status not in ('COMPLETE', 'OVERRIDDEN'), true))::int as miss_cost,
        ${hasReg ? 'count(rd.sku_id)::int' : 'null::int'} as reg_draft,
        ${hasOutbox ? 'count(ow.sku_id)::int' : 'null::int'} as card_waiting,
        ${hasOutbox ? 'count(oc.sku_id)::int' : 'null::int'} as card_conflict,
        ${run ? 'count(dc.code_norm)::int' : 'null::int'} as diff
      from core.skus s
      left join c on c.sku_id = s.sku_id
      ${hasReg ? 'left join rd on rd.sku_id = s.sku_id' : ''}
      ${hasOutbox ? 'left join ow on ow.sku_id = s.sku_id left join oc on oc.sku_id = s.sku_id' : ''}
      ${run ? 'left join dc on dc.code_norm = s.code_norm' : ''}
     where s.company_id = $1`, params)).rows[0];
  return r;
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
    // 最近の変更 (画面の右の「最近の変更」と見出しの「最後に直した人」)。新しい順
    const recent = (await changesSince(db, { skuId: id, productId: cur.product_id, sinceEventId: null, limit: 30 })).reverse();
    const locks = await readFieldLocks(db, cur);
    // 0057: 登録日 (見出しに出すだけ。保存の確かめ = editTokenOf には入れない)。0057 の前の DB = null
    const registered = (await hasRegisteredOn(db))
      ? (await db.query('select registered_on::text as date, registered_on_source as source from core.skus where sku_id = $1', [id])).rows[0] : null;
    if (registered) registered.label = registered.source ? (REGISTERED_ON_SOURCES[registered.source] || registered.source) : null;
    locks.futureCost = await readFutureCostLocks(db, cur, today);
    return {
      cur, costs, suppliers, activeSuppliers, jan, usedIn, amazon, csvRows, today, card, regItems, recent, locks, registered,
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

/**
 * 保存しても断られる欄 (画面は入力欄でなく 🔒 の値で見せる)。読むだけ = 保存の確かめ (lib/master-write.mjs) はそのまま。
 *   reg = 新商品の NE 登録の CSV を配った後 (issued / import_declared / partial) = REG_CSV_FIELDS の欄 (409 reg_csv_issued)。
 *         単品の税率はそれを含むセット (今の構成 + 開いている構成の依頼) の CSV にも入る = そのセットの CSV も見る
 *   csv = 既にある商品の NE に取り込む CSV (0040) が出ている列 (409 csv_issued・issuedCsv と同じ条件)
 * 戻り値 = { fields: { 欄: { why: 'reg' | 'csv', exports: [番号] } }, regExports: [番号], taxParentCodes: [コード] }
 */
async function readFieldLocks(db, cur) {
  const fields = {};
  const put = (f, why, ids) => {
    if (!fields[f]) fields[f] = { why, exports: [] };
    for (const x of ids) if (!fields[f].exports.includes(String(x))) fields[f].exports.push(String(x));
  };
  const kind = cur.sku_kind;
  const defs = fieldsOf(kind);
  const regExports = [];
  const taxParentCodes = [];
  if (await regclass(db, 'ops.ne_reg_export_items')) {
    const issued = ['issued', 'import_declared', 'partial'];
    const mine = (await db.query('select distinct export_id::text as id from ops.ne_reg_export_items where sku_id = $1 and state = any($2::text[]) order by 1', [cur.sku_id, issued])).rows.map((r) => r.id);
    regExports.push(...mine);
    if (mine.length) for (const f of REG_CSV_FIELDS[kind] || []) put(f, 'reg', mine);
    if (kind === 'single') {
      const hasReq = await regclass(db, 'ops.sku_component_requests');
      const parents = (await db.query(`select distinct i.export_id::text as id, k.code from ops.ne_reg_export_items i join core.skus k on k.sku_id = i.sku_id
         where i.state = any($2::text[]) and (i.sku_id in (select c.parent_sku_id from core.sku_components c where c.child_sku_id = $1::bigint)
           ${hasReq ? "or i.sku_id in (select q.set_sku_id from ops.sku_component_requests q where q.status = 'open' and exists (select 1 from jsonb_array_elements(q.rows) x where (x ->> 'sku_id') = $1::text))" : ''})
         order by 1`, [cur.sku_id, issued])).rows;
      if (parents.length) {
        put('tax_rate', 'reg', parents.map((p) => p.id));
        for (const p of parents) if (!taxParentCodes.includes(p.code)) taxParentCodes.push(p.code);
      }
    }
  }
  const colToField = new Map(Object.entries(defs).filter(([, d]) => d.csvCol).map(([f, d]) => [d.csvCol, f]));
  if (colToField.size) {
    for (const r of await issuedCsv(db, cur.code_norm, [...colToField.keys()])) {
      const f = colToField.get(r.col);
      if (f && !fields[f]) put(f, 'csv', [r.export_id]);
      else if (f && fields[f].why === 'csv') put(f, 'csv', [r.export_id]);
    }
  }
  return { fields, regExports, taxParentCodes };
}

/** 例外原価の出どころ = lib/master-write.mjs の OVERRIDE_SOURCES そのもの (写さない = サーバーの判定とずれない。#1589 Codex R3 L4) */
const OVERRIDE_COST_SOURCES = OVERRIDE_SOURCES;
/**
 * 先の日付から始まる原価があって、今日からの原価を入れられない (保存すると 409。#1589 Codex R2 M2)。画面は該当する原価の欄だけを理由つきで閉じる。
 *   own     = この SKU の続いている原価 (valid_to が空) が今日より先に始まる → 単品の原価・セットの例外原価は 409 cost_future (checkCostToday)
 *   parents = 単品の原価を変えるとセットの合計を今日から計算し直すが、そのセット (今の構成) の続いている原価が今日より先に始まり、
 *             例外原価でない → 409 set_cost_future (recomputeSetCost)。開き直しても同じなので、開き直しではなく欄を閉じる
 */
async function readFutureCostLocks(db, cur, today) {
  const own = cur.open_cost && cur.open_cost.valid_from > today ? { valid_from: cur.open_cost.valid_from, cost_jpy: cur.open_cost.cost_jpy } : null;
  let parents = [];
  if (cur.sku_kind === 'single' && cur.parent_set_ids.length) {
    parents = (await db.query(`select k.code, c.valid_from::text as valid_from, c.cost_source from core.sku_costs c join core.skus k on k.sku_id = c.sku_id
       where c.sku_id = any($1::bigint[]) and c.valid_to is null and c.valid_from > $2::date order by k.code_norm`, [cur.parent_set_ids, today])).rows
      .filter((r) => !OVERRIDE_COST_SOURCES.has(r.cost_source)).map((r) => ({ code: r.code, valid_from: r.valid_from }));
  }
  return { own, parents };
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
