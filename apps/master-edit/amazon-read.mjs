/**
 * amazon-read.mjs — マスタ入力画面の「Amazon SKU」タブが読むもの (一覧・1 つの seller SKU・未登録・変更の記録・関連 SKU)。書くのは lib/amazon-map-write.mjs
 * (Company DB構想 16 §2「画面」・§3 #9・§7 v2 M11 / L12。PR ⑦-1)
 *
 * FBA / FBM は Company DB に列を作らない (16 §3 #9) = 画面が Render の mirror_amazon_sku_fees (SQLite) の fulfillment_channel を読む (router の channelProvider)
 * 「未登録」= 直近 7 日の Amazon の注文で、出品が無い・出品に構成が無い FBA の seller SKU (ops.amazon_map_unmapped_recent。自社発送は出さない・10/12)。
 *   売上の日次の公開がそろっていない日があれば「未判定」(0 件と言わない = M11)
 */
import { normSku } from '../../lib/sku-norm.js';
import { foldSearch, foldSql, likeOf, HIRA_CHARS, KATA_CHARS } from './search-fold.mjs';
import { COMPANY_ID, jstDate, MasterWriteError } from '../../lib/master-write.mjs';
import { readAmazonMap, sellerSkuIn, newSellerSkuProblems, newSkuRefusedMessage, newSellerSkuCheckInDb, MAX_MAP_NAME, componentUnusable, USABLE_REG_STATES, NOT_IN_NE_REG_STATES, USABLE_SKU_KINDS, MAP_STATES, AMAZON_JP_SHOP_CODE, AMAZON_SKU_MAX_LEN } from '../../lib/amazon-map-write.mjs';
import { stockOf } from './extras.mjs';
import { readCutoverPhase } from '../../lib/master-cutover.mjs';
import { readReportLoad } from './amazon-unlinked.mjs';
import { readNeSpellings, exportCode, SPELLING_NOTES } from './ne-spelling.mjs';

export { AMAZON_SKU_MAX_LEN };

export const AMAZON_LIST_LIMIT = 100;
export const UNMAPPED_DAYS = 7;
/**
 * 未登録の期間 (10/17 中原さん「未登録の範囲を広げる切り替え」)。既定 = 直近 7 日 (今までどおり)。all = 全期間 (直近 UNMAPPED_ALL_MAX_DAYS 日 = 5 年まで・画面に見た範囲を出す)。
 *   0054 の SQL の関数は 1 回に 60 日まで (least(p_days, 60)) = 長い期間は 60 日ずつに区切って何回か呼び、アプリで足す (migration を足さない)。
 *   画面のロールは注文の表を直接読めない (関数だけ) = 区切って呼ぶのがいちばん小さい
 */
export const UNMAPPED_PERIODS = Object.freeze({ 7: '直近 7 日', 30: '直近 30 日', 90: '直近 90 日', all: '全期間 (5 年まで)' });
/** SQL の関数が 1 回に見る日数の上限 (0054 の ops.amazon_map_unmapped_recent・ops.amazon_map_sales_coverage の least(p_days, 60)) */
export const UNMAPPED_WINDOW_MAX = 60;
/**
 * 全期間の上限 = 5 年 (関数を 31 回ずつ)。🆕 #Codex R1 High: 公開の無い 60 日の幅で途中で止めない (途中に長い欠けがあっても、それより古い注文を落とさない)。
 *   画面のロールは注文・公開の始まりの日を直接読めない = 上限までいつも見る (始まる前の幅は関数が空を返すだけ = 軽い)
 */
export const UNMAPPED_ALL_MAX_DAYS = 1830;
/** 画面・CSV の ?days= → { key, days (all = null) }。知らない値 = 既定の 7 日 */
export function unmappedPeriodOf(q = {}) {
  const v = String(q.days ?? '');
  if (v === 'all') return { key: 'all', days: null };
  return Object.hasOwn(UNMAPPED_PERIODS, v) ? { key: v, days: Number(v) } : { key: String(UNMAPPED_DAYS), days: UNMAPPED_DAYS };
}
const addDaysYmd = (d, n) => new Date(Date.parse(`${d}T00:00:00Z`) + n * 864e5).toISOString().slice(0, 10);
export const CHANNELS = Object.freeze({ FBA: 'FBA', FBM: 'FBM (自社発送)' });
const TS = (col) => `to_char((${col}) at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')`;

/**
 * date の値 → 'YYYY-MM-DD'。node-postgres は date を「その日のサーバーの 0 時」の Date にする = String() だと
 * 「Sat Sep 26 2026 00:00:00 GMT+0900」が画面に出た。Date はサーバーの時計の年月日で読む (作ったときと同じ時間帯)・文字はそのまま先頭 10 文字
 */
export function dateOnly(v) {
  if (v instanceof Date) {
    if (Number.isNaN(v.getTime())) return '';
    return `${v.getFullYear()}-${String(v.getMonth() + 1).padStart(2, '0')}-${String(v.getDate()).padStart(2, '0')}`;
  }
  return String(v ?? '').slice(0, 10);
}

async function regclass(db, name) {
  return (await db.query('select to_regclass($1) is not null as ok', [name])).rows[0].ok;
}

/** 一覧の絞り込み (知らない値は捨てる) */
export function normalizeAmazonFilters(q = {}) {
  const offset = Math.max(0, Math.min(1e6, Number.parseInt(q.offset, 10) || 0));
  const state = Object.prototype.hasOwnProperty.call(MAP_STATES, String(q.state ?? '')) ? String(q.state) : '';
  return { q: String(q.q ?? '').trim().slice(0, 100), state, offset };
}

/** 一覧の絞り込みの where (画面・CSV で同じ) */
function amazonWhere(f) {
  const params = [];
  const where = ['true'];
  if (f.q) {
    // 名前 = かなの同一視 (search-fold.mjs・商品・セットの一覧と同じ決まり)
    params.push(likeOf(normSku(f.q))); const norm = params.length;
    const fold = foldSql(params);
    params.push(likeOf(foldSearch(f.q))); const raw = params.length;
    where.push(`(core.norm_code(m.seller_sku) like $${norm} or ${fold('m.name')} like $${raw}
      or exists (select 1 from core.listing_components c join core.skus k on k.sku_id = c.sku_id where c.listing_id = m.listing_id and k.code_norm like $${norm}))`);
  }
  if (f.state) { params.push(f.state); where.push(`m.state = $${params.length}`); }
  return { where: where.join(' and '), params };
}
/**
 * 一覧と CSV の select。reportParam = Amazon の出品一覧 (0074・前の夜の出品レポート GET_MERCHANT_LISTINGS_ALL_DATA = Amazon の API) の今の取り込みの
 *   load_id の $番号 (読めない・無い = null = 出品一覧を使わない)。
 * ASIN = 出品の ASIN (夜間ロードの catalog_items) が無いときだけ出品一覧の ASIN で埋める (中原さん 10/16「ASIN が空なら Amazon の API で取得して埋めて」)。
 *   asin_from = 'listing' (出品) / 'report' (出品一覧で埋めた) / null。出品一覧に同じ正規化の SKU が 2 行あって ASIN が食い違う = 埋めない (どちらとも決めない)。
 *   区分も同じ決まりで出品一覧の FBA / FBM を report_fulfillment に出す (呼び手が fees の区分が無いときだけ使う)。
 *   🚨 停止中の出品も出品一覧に載る (ALL_DATA)。Amazon で削除した出品は載らない = 空のまま
 * csv = CSV の回だけ、構成の行を 1 つずつ (コード・正規化・数・登録の始まり) comp_list に出す = 構成の商品コードを NE の書き方にそろえる材料 (setExportComps)
 */
function amazonListSelect(reportParam, { csv = false } = {}) {
  const rep = reportParam ? `left join lateral (
          select case when count(distinct r.asin) = 1 then min(r.asin) end as asin,
                 case when count(*) = count(r.fulfillment) and count(distinct r.fulfillment) = 1 then min(r.fulfillment) end as fulfillment
            from ops.amazon_listing_report r
           where r.company_id = l.company_id and r.shop_code = l.shop_code and r.load_id = $${reportParam}::bigint and r.sku_norm = l.listing_norm) rp on true` : '';
  const asin = reportParam ? `coalesce(ci.asin, rp.asin) as asin, case when ci.asin is not null then 'listing' when rp.asin is not null then 'report' end as asin_from, rp.fulfillment as report_fulfillment`
    : `ci.asin, case when ci.asin is not null then 'listing' end as asin_from, null::text as report_fulfillment`;
  return `select m.seller_sku, m.name, m.state, m.origin, ${TS('m.changed_at')} as changed_at, m.changed_by, m.listing_id::text as listing_id, ${asin},
        (select string_agg(k.code || '×' || c.qty, ', ' order by c.sort_order) from core.listing_components c join core.skus k on k.sku_id = c.sku_id where c.listing_id = m.listing_id) as comps
        ${csv ? `, (select jsonb_agg(jsonb_build_object('code', k.code, 'norm', k.code_norm, 'qty', c.qty::text, 'origin', mr.origin) order by c.sort_order)
             from core.listing_components c join core.skus k on k.sku_id = c.sku_id left join ops.master_registrations mr on mr.sku_id = k.sku_id
            where c.listing_id = m.listing_id) as comp_list` : ''}
      from core.amazon_sku_maps m join core.listings l on l.listing_id = m.listing_id left join core.catalog_items ci on ci.catalog_item_id = l.catalog_item_id ${rep}`;
}
/** 出品一覧の今の取り込みの load_id (読めない・無い = null = 使わない・投げない)。古くても ASIN は使う (ASIN は出品の間は変わらない) */
async function reportLoadId(db) {
  try {
    const head = await readReportLoad(db, { nowMs: Date.now() });
    return head.state === 'ok' && head.load ? String(head.load.load_id) : null;
  } catch (e) { console.error(`[master-edit] Amazon の出品一覧の見出しを読めない: ${e && e.message}`); return null; }
}
/** 区分 = 手数料の写し (fees) → 無ければ出品一覧の FBA / FBM (channel_from で出どころ) */
function setChannels(rows, channels) {
  for (const r of rows) {
    const ch = channels ? (channels.get(normSku(r.seller_sku)) ?? null) : null;
    r.channel = ch || r.report_fulfillment || null;
    r.channel_from = ch ? 'fees' : r.report_fulfillment ? 'report' : null;
  }
}

/** 一覧 = Company DB の対応 (墓標も)。今 (切替の前) は 0 件 = 正は miniPC の SKU マスタ */
export async function listAmazonMaps(db, filters, { channels = null } = {}) {
  const f = normalizeAmazonFilters(filters);
  if (!(await regclass(db, 'core.amazon_sku_maps'))) return { rows: [], total: 0, offset: 0, limit: AMAZON_LIST_LIMIT, filters: f, tableMissing: true };
  const { where, params } = amazonWhere(f);
  const total = Number((await db.query(`select count(*)::int as n from core.amazon_sku_maps m where ${where}`, params)).rows[0].n);
  const loadId = await reportLoadId(db);
  if (loadId) params.push(loadId);
  const repP = loadId ? params.length : null;
  params.push(AMAZON_LIST_LIMIT, f.offset);
  const rows = (await db.query(`${amazonListSelect(repP)}
     where ${where}
     order by m.changed_at desc, m.seller_sku limit $${params.length - 1} offset $${params.length}`, params)).rows;
  setChannels(rows, channels);
  return { rows, total, offset: f.offset, limit: AMAZON_LIST_LIMIT, filters: f, tableMissing: false };
}

/** CSV の件数の上限 (商品の一覧の CSV と同じ) */
export const AMAZON_EXPORT_MAX = 10000;
/** CSV 用 = 絞った一覧 (状態・検索) を全部 (ページ分けに関係なく・seller SKU の順)。max 件を超える = { tooMany: true } (行は読まない) */
export async function listAmazonMapsAll(db, filters, { channels = null, max = AMAZON_EXPORT_MAX } = {}) {
  const f = normalizeAmazonFilters(filters);
  if (!(await regclass(db, 'core.amazon_sku_maps'))) return { rows: [], total: 0, max, filters: f, tableMissing: true };
  const { where, params } = amazonWhere(f);
  const total = Number((await db.query(`select count(*)::int as n from core.amazon_sku_maps m where ${where}`, params)).rows[0].n);
  if (total > max) return { rows: [], total, max, filters: f, tableMissing: false, tooMany: true };
  // 数えた後に増えた (別の保存・移行) ときも上限を超えて読まない = 読むのも max + 1 件で切る (Codex R1 P2)
  const loadId = await reportLoadId(db);
  if (loadId) params.push(loadId);
  const repP = loadId ? params.length : null;
  params.push(max + 1);
  const rows = (await db.query(`${amazonListSelect(repP, { csv: true })}
     where ${where}
     order by m.seller_sku limit $${params.length}`, params)).rows;
  if (rows.length > max) return { rows: [], total: rows.length, max, filters: f, tableMissing: false, tooMany: true, atLeast: true };
  setChannels(rows, channels);
  await setExportComps(db, rows);
  return { rows, total, max, filters: f, tableMissing: false };
}
/**
 * CSV の構成 (NE の商品 × 数) の商品コードを、NE から来た商品なら NE の書き方にする (10/16 中原さん・商品の一覧の CSV と同じ決まり = ne-spelling.mjs)。
 * 決まらない構成品 = 今のコードのまま + comp_notes (CSV の最後の列「構成の商品コードの注意」)。NE の元のコードを読めない = 1 つの注意だけ (構成品ごとに並べない)
 */
async function setExportComps(db, rows) {
  const norms = rows.flatMap((r) => (Array.isArray(r.comp_list) ? r.comp_list : []).filter((c) => c.origin !== 'new_entry').map((c) => c.norm));
  const spell = await readNeSpellings(db, norms);
  for (const r of rows) {
    const list = Array.isArray(r.comp_list) ? r.comp_list : null;
    delete r.comp_list;
    r.comp_notes = '';
    if (!list || !list.length) continue;
    const notes = [];
    let unreadable = false;
    r.comps = list.map((c) => {
      const x = exportCode({ code: c.code, code_norm: c.norm, origin: c.origin ?? null }, spell);
      if (x.note === SPELLING_NOTES.unreadable) unreadable = true;
      else if (x.note) notes.push(`${x.code}: ${x.note}`);
      return `${x.code}×${c.qty}`;
    }).join(', ');
    r.comp_notes = unreadable ? SPELLING_NOTES.unreadable : notes.join(' / ');
  }
}

/** 1 つの seller SKU の画面。対応が無くても、Company DB の出品と今の構成 (夜間ロードの写し) を見せる */
export async function readAmazonPage(db, sellerSku, { channels = null, today = null } = {}) {
  const cur = await readAmazonMap(db, sellerSku);
  const phase = await readCutoverPhase(db);
  const related = cur.listing ? (await db.query(`select distinct m2.seller_sku, m2.name from core.listing_components c1
       join core.listing_components c2 on c2.sku_id = c1.sku_id and c2.listing_id <> c1.listing_id
       join core.amazon_sku_maps m2 on m2.listing_id = c2.listing_id
      where c1.listing_id = $1 order by m2.seller_sku limit 50`, [cur.listing.listing_id])).rows : [];
  const lastRequest = cur.listing ? (await db.query(`select operation, actor_id, status, ${TS('finished_at')} as finished_at from ops.master_edit_requests
      where listing_id = $1 order by finished_at desc limit 1`, [cur.listing.listing_id])).rows[0] || null : null;
  const costs = await componentCosts(db, cur.components, { today });
  return { cur, phase, related, lastRequest, channel: channels ? (channels.get(normSku(sellerSku)) ?? null) : null, costs };
}

/** 原価の状態 (core.sku_costs.cost_status の CHECK と同じ 4 つ) */
export const COST_STATUS_WORDS = Object.freeze({ COMPLETE: '確定', OVERRIDDEN: '上書き', PARTIAL: '一部が足りない', MISSING: '不明' });
export const COST_SOURCE_WORDS = Object.freeze({ ne: 'NE から', manual: '人が入れた', set_calc: '構成品の合計', override_zero: '0 円の上書き', imported: '取込' });
/**
 * 構成の原価 (10/17 中原さん「構成の原価の表示」・見るだけ)。今日 (東京) の原価の行 = 商品の画面と同じ選び方 (read.mjs の costTodaySql:
 *   valid_from が今日まで・valid_to が無いか今日から先 → valid_from が新しい → created_at → sku_cost_id)。原価の表は 1 回だけ読む。
 *   components = readAmazonMap の構成 (保存してある構成)。戻り値 { today, lines: [{ code, name, qty, cost, subtotal, status, source, valid_from }], total, known, missing, warn }
 *   total = 全部の構成品に原価があるときだけ Σ 原価 × 数 (欠けている = null・known に分かる分だけの合計)。warn = 状態が 一部が足りない / 不明 の構成品がある
 *   読めない (権限・DB の誤り) = { error } (画面は「読めない」。保存には関係しない)
 */
export async function componentCosts(db, components, { today = null } = {}) {
  const list = Array.isArray(components) ? components : [];
  if (!list.length) return { today, lines: [], total: null, known: 0, missing: 0, warn: false };
  try {
    const day = today || (await db.query(`select (now() at time zone 'Asia/Tokyo')::date::text as d`)).rows[0].d;
    const rows = (await db.query(`select distinct on (y.sku_id) y.sku_id::text as sku_id, y.cost_jpy::text as cost_jpy, y.cost_source, y.cost_status, y.valid_from::text as valid_from
        from core.sku_costs y
       where y.sku_id = any($1::bigint[]) and y.valid_from <= $2::date and (y.valid_to is null or y.valid_to >= $2::date)
       order by y.sku_id, y.valid_from desc, y.created_at desc, y.sku_cost_id desc`, [list.map((c) => String(c.sku_id)), day])).rows;
    const by = new Map(rows.map((r) => [r.sku_id, r]));
    let known = 0; let missing = 0; let warn = false;
    const lines = list.map((c) => {
      const y = by.get(String(c.sku_id));
      const cost = y ? Number(y.cost_jpy) : null;
      const qty = Number(c.qty);
      if (cost == null) missing++; else known += cost * qty;
      if (!y || y.cost_status === 'PARTIAL' || y.cost_status === 'MISSING') warn = true;
      return { code: c.code, name: c.name, qty, cost, subtotal: cost == null ? null : cost * qty, status: y ? y.cost_status : null, source: y ? y.cost_source : null, valid_from: y ? y.valid_from : null };
    });
    return { today: day, lines, total: missing ? null : known, known, missing, warn };
  } catch (e) {
    console.error(`[master-edit] 構成の原価を読めない: ${e && e.message}`);
    return { today, lines: [], total: null, known: 0, missing: 0, warn: false, error: '原価を読めません' };
  }
}

/**
 * 変更の記録 (対応・構成・出品)。新しい順に 200 件。
 * 構成の行は、どの商品か (entity_key の sku_id、無ければ行の中身の sku_id) を商品コードに引いて sku_code に入れる (引けない = sku_id だけ・#1628 Codex R2 M3)
 */
export async function amazonHistory(db, sellerSku) {
  const cur = await readAmazonMap(db, sellerSku);
  if (!cur.listing) return null;
  const skuIdOf = `coalesce(nullif(e.entity_key ->> 'sku_id', ''), nullif(e.new_value ->> 'sku_id', ''), nullif(e.old_value ->> 'sku_id', ''))`;
  const events = (await db.query(`select e.event_id::text as event_id, e.operation, e.entity_type, e.attribute, e.old_value, e.new_value,
        e.actor_type, e.actor_id, e.source_system, e.reason_text, e.recorded_at::text as recorded_at,
        case when e.entity_type = 'listing_component' then ${skuIdOf} end as sku_id,
        case when e.entity_type = 'listing_component' and ${skuIdOf} ~ '^[0-9]{1,18}$'
          then (select k.code from core.skus k where k.sku_id = (${skuIdOf})::bigint) end as sku_code
      from events.master_change_events e
     where (e.entity_type in ('amazon_sku_map', 'listing') and e.entity_id = $1::bigint)
        or (e.entity_type = 'listing_component' and (e.entity_key ->> 'listing_id')::bigint = $1::bigint)
     order by e.event_id desc limit 200`, [cur.listing.listing_id])).rows;
  return { cur, events };
}

/** 手数料の写し (fees) の区分が「新しい」とみなす長さ = 取り直しの硬い期限 132 時間 (fetch-amazon-fees.js の REFRESH_HARD_HOURS) + 1.5 日のゆとり */
export const FEES_FRESH_MS = 168 * 3600e3;
/** fees の fetched_at ('YYYY-MM-DD HH:MM:SS' = UTC・fetch-amazon-fees.js の now()) → ms。読めない = null */
export function feesFetchedMs(v) {
  const s = String(v ?? '').trim();
  const m = /^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(?:\.\d+)?(Z)?$/.exec(s);
  if (!m) return null;
  const t = Date.parse(`${m[1]}T${m[2]}Z`);
  return Number.isNaN(t) ? null : t;
}
/**
 * 手数料の写しの行 [{ seller_sku, fulfillment_channel, fetched_at }] → Map(正規化した seller SKU → 'FBA' | 'FBM')。
 *   🚨 大文字小文字・全角だけ違う SKU が同じ正規化の鍵になって区分が違う = どちらとも言えない = 入れない (後勝ちにしない・#1691 Codex R4 M2)。
 *   map.at = Map(鍵 → fetched_at の ms (同じ鍵のいちばん新しい) | null)・map.conflicts = 区分が食い違った鍵
 */
export function channelsFromFeeRows(rows) {
  const out = new Map(); const at = new Map(); const conflicts = new Set();
  for (const r of rows || []) {
    const ch = String(r.fulfillment_channel || '').trim().toUpperCase();
    if (ch !== 'FBA' && ch !== 'FBM') continue;
    const k = normSku(r.seller_sku);
    if (!k) continue;
    if (conflicts.has(k)) continue;
    if (out.has(k) && out.get(k) !== ch) { out.delete(k); at.delete(k); conflicts.add(k); continue; }
    out.set(k, ch);
    const t = feesFetchedMs(r.fetched_at);
    const prev = at.get(k);
    at.set(k, prev == null ? t : t == null ? prev : Math.max(prev, t));
  }
  out.at = at; out.conflicts = conflicts;
  return out;
}

/**
 * 未登録 (M11): 直近 UNMAPPED_DAYS 日の Amazon の注文で、出品が無い・出品に構成が無い **FBA の** seller SKU。
 * 🚨 自社発送 (FBM) は出さない・数えない (中原さん 10/12「自社発送は 商品コード = Amazon SKU なので対応が要らない」)。区分の切り替えも無い。
 * 区分の決め方 = 鮮度で決める (#1691 Codex R4 M1):
 *   ① Company DB の Amazon の出品一覧 (0074・毎日全件の今の姿) が古くなければ (36 時間以内) それを先に
 *   ② 出品一覧に無い・古い・読めない = 手数料の写し (channels = Render の mirror_amazon_sku_fees) が新しければ (FEES_FRESH_MS 以内) それ
 *   ③ どちらも古い・無い = 区分が分からない
 *   同じ正規化の鍵で区分が食い違う行 (出品一覧・fees とも) = 区分が分からない (後勝ちにしない・R4 M2)
 *   区分が分からない SKU = 一覧と件数に入れず unknown に分けて返す (画面は別の一言 = 黙って隠さない)。
 *   区分の出どころが両方とも古い・読めない = decidable: false (画面は件数も「読めない」・0 件と言わない・R4 M3)。候補が 0 件なら読めなくても 0 件と確定 (R4 L1)
 * 売上の日次の公開がそろっていない日があれば undecided = その日の一覧 (画面は「未判定」)
 */
/*
 * 🆕 10/17 中原さん「未登録の範囲を広げる切り替え」:
 *   days = 期間 (7 / 30 / 90 / null = 全期間)。既定の 7 日は今までどおり関数を 1 回ずつ呼ぶ (区切りは 60 日ずつ・unmappedRange)。
 *   fbm = 自社発送 (FBM) で、seller SKU が NE の商品コード (core.skus) でない SKU。自社発送は ふつう 商品コード = Amazon SKU で対応が要らない (10/12) が、
 *     コードが違う SKU は夜間の取り込みが結べない = 今まではどこにも出なかった (対応が要る)。区分を読めない (decidable: false) ときは出さない (0 件と言わない)。
 *     fbmNeCode = 自社発送で seller SKU が NE の商品コードと同じなのに構成が無い件数 (夜間の取り込みが次に結ぶ・数だけ出す)
 *   titles = 行の Amazon の商品名 (出品一覧 → 出品のタイトル)。その場で登録するときの名前の初めの値・CSV
 */
export async function amazonUnmapped(db, { now = new Date(), channels = null, days = UNMAPPED_DAYS } = {}) {
  const today = jstDate(now);
  const nowMs = now.getTime();
  const period = days == null ? 'all' : String(days);
  if (!(await regclass(db, 'core.amazon_sku_maps'))) return { rows: [], unknown: [], fbm: [], fbmNeCode: 0, today, undecided: [], tableMissing: true, channelsAvailable: !!channels, decidable: false, days, period, from: today };
  const range = await unmappedRange(db, today, days);
  const all = range.rows;
  const base = { today, undecided: range.undecided, tableMissing: false, channelsAvailable: !!channels, shopCode: AMAZON_JP_SHOP_CODE, companyId: COMPANY_ID, days, period, from: range.from, publishedFrom: range.publishedFrom };
  if (!all.length) return { ...base, rows: [], unknown: [], fbm: [], fbmNeCode: 0, decidable: true, source: null };   // 候補が無い = 区分によらず 0 件 (R4 L1)
  // ① 出品一覧 (古くないときだけ)。鍵ごとに区分が 1 つのときだけ採る (食い違い = 'conflict')
  let report = null;
  const head = await readReportLoad(db, { nowMs });
  const reportFresh = head.state === 'ok' && !head.stale;
  if (reportFresh) {
    try {
      report = new Map((await db.query(`select sku_norm, count(distinct coalesce(fulfillment, '?'))::int as n, min(fulfillment) as f from ops.amazon_listing_report
          where company_id = $1 and shop_code = $2 and load_id = $3::bigint and sku_norm = any(select core.norm_code(x) from unnest($4::text[]) as t(x)) group by sku_norm`,
      [COMPANY_ID, AMAZON_JP_SHOP_CODE, head.load.load_id, all.map((r) => r.code)])).rows.map((x) => [x.sku_norm, x.n > 1 ? 'conflict' : x.f || null]));
    } catch (e) { console.error(`[master-edit] 未登録の区分 (出品一覧) を読めない: ${e && e.message}`); report = null; }
  }
  // ② fees (新しいときだけ)
  const feesAt = (k) => (channels && channels.at instanceof Map ? channels.at.get(k) : null);
  const feesFresh = (k) => { const t = feesAt(k); return t != null && nowMs - t <= FEES_FRESH_MS; };
  const anyFeesFresh = !!channels && channels.at instanceof Map && [...channels.at.values()].some((t) => t != null && nowMs - t <= FEES_FRESH_MS);
  for (const r of all) {
    const k = normSku(r.code);
    const rep = report ? report.get(k) : undefined;
    if (rep === 'conflict') { r.channel = null; r.channel_from = 'conflict'; continue; }
    if (rep) { r.channel = rep; r.channel_from = 'report'; continue; }
    if (channels && channels.conflicts instanceof Set && channels.conflicts.has(k)) { r.channel = null; r.channel_from = 'conflict'; continue; }
    const ch = channels ? channels.get(k) : null;
    if (ch && feesFresh(k)) { r.channel = ch; r.channel_from = 'fees'; continue; }
    r.channel = null; r.channel_from = ch ? 'stale' : 'none';
  }
  const decidable = (reportFresh && !!report) || anyFeesFresh;
  const rows = decidable ? all.filter((r) => r.channel === 'FBA') : [];
  const unknown = all.filter((r) => !r.channel);
  // 自社発送のうち seller SKU が NE の商品コードでない SKU (区分を読めないときは出さない)
  const fbmAll = decidable ? all.filter((r) => r.channel === 'FBM') : [];
  const neCodes = fbmAll.length ? new Set((await db.query(`select x as raw from unnest($1::text[]) as t(x)
      where exists (select 1 from core.skus k where k.company_id = $2 and k.code_norm = core.norm_code(x))`, [fbmAll.map((r) => r.code), COMPANY_ID])).rows.map((x) => x.raw)) : new Set();
  const fbm = fbmAll.filter((r) => !neCodes.has(r.code));
  await setUnmappedTitles(db, [...rows, ...fbm, ...unknown], head);
  return { ...base, rows, unknown, fbm, fbmNeCode: fbmAll.length - fbm.length, decidable, reportFresh: reportFresh && !!report, feesFresh: anyFeesFresh };
}

const UNMAPPED_SQL = 'select code, listing_id::text as listing_id, map_state, units::int as units, orders::int as orders, last_date::text as last_date from ops.amazon_map_unmapped_recent($1::date, $2)';
/**
 * 期間の注文の未登録 (関数を 60 日ずつ・新しい幅から呼んで足す)。days = null (全期間) は UNMAPPED_ALL_MAX_DAYS 日まで (途中で止めない)。
 *   足し方: 関数の行の鍵 (コード・出品・対応の状態) ごとに 個数 = 和・注文 = 和 (1 つの注文は 1 つの日 = 幅をまたがない)・最後に売れた日 = いちばん新しい日。
 *   未判定の日 = 公開されていない日。全期間は、見た範囲で公開がいちばん初めの日 (publishedFrom) より前の日を未判定に数えない (売上の日次が始まる前 = 画面に一言で出す)
 *   戻り値 { rows (個数の多い順・コード順), undecided: ['YYYY-MM-DD'], from (見た幅のいちばん古い日), publishedFrom (全期間だけ・無い = null) }
 */
async function unmappedRange(db, today, days) {
  const allTime = days == null;
  let left = allTime ? UNMAPPED_ALL_MAX_DAYS : Math.max(1, Math.floor(days));
  let end = today; let from = today;
  const merged = new Map();
  const missing = new Set();
  while (left > 0) {
    const n = Math.min(UNMAPPED_WINDOW_MAX, left);
    const miss = ((await db.query('select ops.amazon_map_sales_coverage($1::date, $2) as d', [end, n])).rows[0].d || []).map(dateOnly);
    for (const d of miss) missing.add(d);
    for (const r of (await db.query(UNMAPPED_SQL, [end, n])).rows) {
      const k = `${r.code}\u0000${r.listing_id ?? ''}\u0000${r.map_state ?? ''}`;
      const x = merged.get(k);
      if (!x) { merged.set(k, { ...r, units: Number(r.units), orders: Number(r.orders) }); continue; }
      x.units += Number(r.units); x.orders += Number(r.orders);
      if (r.last_date && (!x.last_date || r.last_date > x.last_date)) x.last_date = r.last_date;
    }
    from = addDaysYmd(end, -(n - 1));
    left -= n;
    end = addDaysYmd(from, -1);
  }
  let undecided = [...missing].sort();
  let publishedFrom = null;
  if (allTime) {
    // 公開のいちばん初めの日 (見た幅の中で、公開されていない日でない最初の日)。1 日も無い = 全部を未判定のまま
    let first = null;
    for (let d = from; d <= today; d = addDaysYmd(d, 1)) { if (!missing.has(d)) { first = d; break; } }
    if (first) undecided = undecided.filter((d) => d >= first);
    publishedFrom = first;
  }
  const rows = [...merged.values()].sort((a, b) => b.units - a.units || (a.code < b.code ? -1 : a.code > b.code ? 1 : 0));
  return { rows, undecided, from, publishedFrom };
}

/** 行の Amazon の商品名 (title): 出品一覧の今の取り込み (読めるときだけ) → 出品のタイトル。読めない = 付けない (画面・CSV は空) */
async function setUnmappedTitles(db, list, head) {
  if (!list.length) return;
  try {
    const loadId = head && head.state === 'ok' && head.load ? String(head.load.load_id) : null;
    const m = new Map((await db.query(`select x as raw,
          ${loadId ? `(select r.item_name from ops.amazon_listing_report r where r.company_id = $2 and r.shop_code = $3 and r.load_id = $4::bigint and r.sku_norm = core.norm_code(x)
                        and nullif(btrim(r.item_name), '') is not null order by r.seller_sku limit 1)` : 'null::text'} as report_title,
          (select l.title from core.listings l where l.company_id = $2 and l.mall = 'amazon' and l.shop_code = $3 and l.listing_norm = core.norm_code(x)) as listing_title
        from unnest($1::text[]) as t(x)`, [[...new Set(list.map((r) => r.code))], COMPANY_ID, AMAZON_JP_SHOP_CODE, ...(loadId ? [loadId] : [])])).rows
      .map((x) => [x.raw, x.report_title || x.listing_title || null]));
    for (const r of list) r.title = m.get(r.code) ?? null;
  } catch (e) { console.error(`[master-edit] 未登録の Amazon の商品名を読めない: ${e && e.message}`); }
}

const NAME_CTRL_RE = new RegExp(`[\x00-\x1f\x7f${String.fromCharCode(0x2028, 0x2029)}]`, 'g');
/** 名前の初めの値 = 改行・制御文字は空白に・前後の空白を除く・255 文字まで (保存の決まり = 前後の空白・制御文字は断る) */
export function cleanName(v) {
  const s = String(v ?? '').replace(NAME_CTRL_RE, ' ').trim();
  return [...s].slice(0, MAX_MAP_NAME).join('').trim();
}
/**
 * その場で登録する欄 (未登録・まだつないでいない SKU の一覧の行) が開くときに読む 1 つの seller SKU (読むだけ・10/17)。
 *   版 (保存の seen.versions)・今の対応 (名前・状態)・今の構成・名前の初めの値 (今の対応の名前 → Amazon の出品一覧の商品名 → 出品のタイトル)・
 *   まだ出品の無い SKU を先に作れない形 (new_sku_problems = 保存も断る)。保存は今までどおり POST /api/amazon/save (lib の門・版・request_id)
 */
export async function readMapForInline(db, sellerSku, { nowMs = Date.now() } = {}) {
  const cur = await readAmazonMap(db, sellerSku);
  let reportTitle = null;
  try {
    const head = await readReportLoad(db, { nowMs });
    if (head.state === 'ok') {
      reportTitle = (await db.query(`select r.item_name from ops.amazon_listing_report r where r.company_id = $1 and r.shop_code = $2 and r.load_id = $3::bigint
          and r.sku_norm = core.norm_code($4) and nullif(btrim(r.item_name), '') is not null order by r.seller_sku limit 1`, [COMPANY_ID, AMAZON_JP_SHOP_CODE, head.load.load_id, sellerSku])).rows[0]?.item_name ?? null;
    }
  } catch (e) { console.error(`[master-edit] その場で登録の Amazon の商品名を読めない: ${e && e.message}`); }
  const nameDefault = (cur.map && cur.map.name) || reportTitle || (cur.listing && cur.listing.title) || '';
  const nameFrom = cur.map && cur.map.name ? 'map' : reportTitle ? 'report' : cur.listing && cur.listing.title ? 'listing' : null;
  const problems = cur.listing ? [] : (await newSellerSkuCheckInDb(db, sellerSku)).problems;
  return {
    seller_sku: sellerSku, versions: cur.versions, open_sku: cur.map ? cur.map.seller_sku : sellerSku,
    map: cur.map ? { name: cur.map.name, state: cur.map.state } : null, listing: cur.listing ? { title: cur.listing.title, asin: cur.listing.asin } : null,
    components: cur.map && cur.map.state === 'deleted' ? [] : cur.components.map((c) => ({ code: c.code, name: c.name, qty: c.qty, sku_kind: c.sku_kind, reg_state: c.reg_state })),
    name_default: cleanName(nameDefault), name_from: nameFrom, new_sku_problems: problems,
  };
}


// ─── 新しい seller SKU の対応を自分で作る入口 (10/11 中原さん「売れてから作るしかない」→ まだ売れていない SKU も先に作れる) ───

/** 「もしかして」に出す数 */
export const SIMILAR_LIMIT = 5;
/** 「もしかして」で 1 回に比べる出品の上限 (#1685 Codex R1 Low: 直に連打できる API・JS の比べには statement_timeout が効かない)。超えたら incomplete */
export const SIMILAR_SCAN_MAX = 2000;
/** 「もしかして」で記号 (- _ . /) を除くと同じ出品を比べる上限 (#1685 Codex R2 Low)。超えたら symbols_incomplete */
export const SIMILAR_SYMBOLS_MAX = 50;
const SKELETON_RE = /[-_./\s]/g;
const DIGIT_RE = /[0-9]/;

/** 2 つの文字の差 (足す・抜く・別の字・隣どうしの入れ替え = OSA)。max を超えたら max + 1 (途中で打ち切る) */
export function editDistance(a, b, max = 2) {
  const n = a.length; const m = b.length;
  if (Math.abs(n - m) > max) return max + 1;
  let prev2 = null;
  let prev = Array.from({ length: m + 1 }, (_, j) => j);
  for (let i = 1; i <= n; i++) {
    const cur = [i];
    let rowMin = i;
    for (let j = 1; j <= m; j++) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      let v = Math.min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + cost);
      if (i > 1 && j > 1 && a[i - 1] === b[j - 2] && a[i - 2] === b[j - 1]) v = Math.min(v, prev2[j - 2] + 1);
      cur.push(v);
      if (v < rowMin) rowMin = v;
    }
    if (rowMin > max) return max + 1;
    prev2 = prev; prev = cur;
  }
  return Math.min(prev[m], max + 1);
}

/**
 * 似ているか (どちらも正規化した形 = core.norm_code で渡す)。null = 似ていない /
 *   { kind: 'symbols' } = 記号 (- _ . / 空白) だけ違う / { kind: 'typo', distance } = 1〜2 文字の違い (5 文字以下の短い SKU は 1 文字だけ)。
 *   同じ長さで、違う所が全部「数字 ↔ 数字」= 連番 (pr_a001 と pr_a002) は似ているとしない (新しい連番を作るたびに出ると、読まれなくなる)
 */
export function similarKind(a, b) {
  if (!a || !b || a === b) return null;
  const sa = a.replace(SKELETON_RE, ''); const sb = b.replace(SKELETON_RE, '');
  if (sa && sa === sb) return { kind: 'symbols', distance: editDistance(a, b, 9) };
  if (a.length === b.length) {
    let onlyDigits = true;
    for (let i = 0; i < a.length && onlyDigits; i++) if (a[i] !== b[i] && !(DIGIT_RE.test(a[i]) && DIGIT_RE.test(b[i]))) onlyDigits = false;
    if (onlyDigits) return null;
  }
  const max = Math.min(a.length, b.length) <= 5 ? 1 : 2;
  const d = editDistance(a, b, max);
  return d <= max ? { kind: 'typo', distance: d } : null;
}

/**
 * 気をつけること (止めない): 半角の英数字・記号でない字 (日本語など) が残っている。sku = sellerSkuIn の後の形。
 * 40 文字・途中の空白・全角の英数字 / Unicode のダッシュは、まだ出品の無い SKU では止める (lib の newSellerSkuProblems・#1685 Codex R1 High)
 */
export function sellerSkuWarnings(sku) {
  return /[^\x20-\x7e]/.test(sku) ? ['半角の英数字・記号でない字 (日本語など) が入っています。Seller Central の SKU と同じ書き方か確かめてください'] : [];
}
const lowerSkuOr = (code) => { try { return sellerSkuIn(String(code ?? '')); } catch { return String(code ?? '').trim().toLowerCase(); } };

/**
 * 似た seller SKU (Company DB の Amazon (日本) の出品 = 対応の有効・墓標と、対応の無い出品)。近い順に SIMILAR_LIMIT 件 + 全部の数 + incomplete。
 *   候補 = ① 記号 (- _ . /) を除くと同じ (長さの差によらない・#1685 Codex R1 Medium) + ② 長さ ±2 (1〜2 文字の違い)。
 *   ① は SIMILAR_SYMBOLS_MAX 件まで (超えた = symbols_incomplete・#1685 Codex R2 Low)。② は長さの近い順に SIMILAR_SCAN_MAX 件まで (超えた = incomplete: 全部とは比べていない・#1685 Codex R1 Low)
 */
export async function similarAmazonSkus(db, sellerSku) {
  const norm = (await db.query('select core.norm_code($1) as n', [sellerSku])).rows[0].n || '';
  if (!norm) return { rows: [], total: 0, incomplete: false, symbols_incomplete: false };
  const skel = norm.replace(SKELETON_RE, '');
  const bySkel = skel ? (await db.query(`select l.listing_id::text as listing_id, l.listing_norm from core.listings l
     where l.company_id = $1 and l.mall = 'amazon' and l.shop_code = $2 and regexp_replace(l.listing_norm, '[-_./[:space:]]', '', 'g') = $3 and l.listing_norm <> $4
     order by l.listing_norm limit $5`, [COMPANY_ID, AMAZON_JP_SHOP_CODE, skel, norm, SIMILAR_SYMBOLS_MAX + 1])).rows : [];
  const symbolsIncomplete = bySkel.length > SIMILAR_SYMBOLS_MAX;
  const band = (await db.query(`select l.listing_id::text as listing_id, l.listing_norm from core.listings l
     where l.company_id = $1 and l.mall = 'amazon' and l.shop_code = $2 and length(l.listing_norm) between $3 and $4 and l.listing_norm <> $5
     order by abs(length(l.listing_norm) - $6), l.listing_norm limit $7`,
  [COMPANY_ID, AMAZON_JP_SHOP_CODE, Math.max(1, norm.length - 2), norm.length + 2, norm, norm.length, SIMILAR_SCAN_MAX + 1])).rows;
  const incomplete = band.length > SIMILAR_SCAN_MAX;
  const seen = new Set();
  const hits = [];
  for (const c of [...bySkel.slice(0, SIMILAR_SYMBOLS_MAX), ...band.slice(0, SIMILAR_SCAN_MAX)]) {
    if (seen.has(c.listing_id)) continue;
    seen.add(c.listing_id);
    const s = similarKind(norm, c.listing_norm);
    if (s) hits.push({ listing_id: c.listing_id, norm: c.listing_norm, ...s });
  }
  if (!hits.length) return { rows: [], total: 0, incomplete, symbols_incomplete: symbolsIncomplete };
  const ids = hits.map((h) => h.listing_id);
  const info = new Map((await db.query(`select l.listing_id::text as listing_id, l.listing_code, l.title, m.seller_sku, m.name, m.state, ci.asin
      from core.listings l left join core.amazon_sku_maps m on m.listing_id = l.listing_id left join core.catalog_items ci on ci.catalog_item_id = l.catalog_item_id
     where l.listing_id = any($1::bigint[])`, [ids])).rows.map((r) => [r.listing_id, r]));
  const RANK = { active: 0, deleted: 1 };
  const rows = hits.map((h) => {
    const r = info.get(h.listing_id) || {};
    return { seller_sku: r.seller_sku || lowerSkuOr(r.listing_code), name: r.name || r.title || null, state: r.state || null, asin: r.asin || null, kind: h.kind, distance: h.distance };
  }).sort((x, y) => (x.kind === y.kind ? 0 : x.kind === 'symbols' ? -1 : 1) || x.distance - y.distance
    || (RANK[x.state] ?? 2) - (RANK[y.state] ?? 2) || (x.seller_sku < y.seller_sku ? -1 : x.seller_sku > y.seller_sku ? 1 : 0));
  return { rows: rows.slice(0, SIMILAR_LIMIT), total: rows.length, incomplete, symbols_incomplete: symbolsIncomplete };
}

/**
 * 新しく作る seller SKU を確かめる (読むだけ・何も書かない)。
 *   1. 形 = 保存と同じ sellerSkuIn (前後の空白を除いて小文字・写しの受け手の決まり)。合わない = { ok: false, reason: 'shape', message }
 *   2. もうある: Company DB に対応 (有効・墓標) がある (正規化して同じ = 同じ SKU) = exists.kind 'map' → next 'exists' (そのページへ案内・作らない)
 *      対応は無いが出品 (夜間の取り込みの写し) がある = exists.kind 'listing' → next 'create' (出品のコードの画面で作る)
 *   3. 出品も対応も無い = 先に作る SKU: 正規化しても同じ形・40 文字まで・途中の空白なし でなければ止める (= 保存の lib と同じ newSellerSkuProblems・#1685 Codex R1 High)
 *      → { ok: false, reason: 'not_canonical', message, problems, canonical (半角にそろえた形・作れる形のときだけ) }
 *   4. 打ち間違いの守り (止めない): 似た seller SKU (similar)・気をつけること (warnings) があれば next 'confirm' (見てから「このまま作る」)、無ければ 'create'
 *   open = 作る / 開く画面の seller SKU
 */
export async function checkNewAmazonSku(db, raw) {
  const base = { ok: false, sku: null, open: null, exists: null, similar: [], similar_total: 0, similar_incomplete: false, similar_symbols_incomplete: false, warnings: [], problems: [], canonical: null, next: null };
  let sku;
  try { sku = sellerSkuIn(typeof raw === 'string' ? raw : ''); } catch (e) {
    if (e instanceof MasterWriteError) return { ...base, reason: 'shape', message: e.message };
    throw e;
  }
  if (!(await regclass(db, 'core.amazon_sku_maps'))) return { ...base, sku, reason: 'table_missing', message: 'Company DB に Amazon SKU の対応の表がまだありません (migration 0054 の前)' };
  const cur = await readAmazonMap(db, sku);
  if (cur.map) {
    return { ...base, ok: true, reason: null, message: null, sku, open: cur.map.seller_sku, next: 'exists',
      exists: { kind: 'map', seller_sku: cur.map.seller_sku, name: cur.map.name, state: cur.map.state, same: cur.map.seller_sku === sku, asin: cur.listing.asin ?? null } };
  }
  if (cur.listing) {
    const open = lowerSkuOr(cur.listing.listing_code);
    return { ...base, ok: true, reason: null, message: null, sku, open, next: 'create',
      exists: { kind: 'listing', seller_sku: open, name: cur.listing.title ?? null, state: null, same: open === sku, asin: cur.listing.asin ?? null } };
  }
  const norm = (await db.query('select core.norm_code($1) as n', [sku])).rows[0].n || '';
  const problems = newSellerSkuProblems(sku, norm);
  if (problems.length) {
    let canonical = null;
    try { const c = sellerSkuIn(norm); if (!newSellerSkuProblems(c, c).length) canonical = c; } catch { /* 半角にしても使えない形 */ }
    return { ...base, sku, reason: 'not_canonical', message: newSkuRefusedMessage(sku, problems), problems, canonical };
  }
  const sim = await similarAmazonSkus(db, sku);
  const warnings = sellerSkuWarnings(sku);
  return { ...base, ok: true, reason: null, message: null, sku, open: sku, similar: sim.rows, similar_total: sim.total, similar_incomplete: sim.incomplete, similar_symbols_incomplete: sim.symbols_incomplete, warnings,
    next: sim.rows.length || warnings.length ? 'confirm' : 'create' };
}

// ─── 構成の行の「NE の商品」の欄の候補 (10/12 中原さん「既存の商品コードを検索して選んで登録できるように」) ───

/** 何文字から探すか・何件まで返すか・欄の長さ (コードの欄と同じ 60 = 言葉は多くても 30 個 = 全部の言葉を見る。#1689 Codex R1 Low: 黙って捨てない) */
export const COMPONENT_SEARCH_MIN = 2;
export const COMPONENT_SEARCH_LIMIT = 20;
export const COMPONENT_SEARCH_Q_MAX = 60;
const KIND_WORDS = Object.freeze({ single: '単品', set: 'セット', exception: '例外' });
const REG_WHY = Object.freeze({
  cancelled: '登録をやめた商品',
});
/**
 * 構成品に使えない理由 (画面に出す文)。使える = null。決まりは保存の確かめと同じ関数 (lib/amazon-map-write.mjs の componentUnusable)
 * = 画面で選べる候補は保存で断られない
 */
export function componentUnusableWhy(skuKind, regState) {
  const w = componentUnusable(skuKind, regState);
  if (!w) return null;
  if (w === 'kind') return '例外の SKU (NE に無い商品) = 構成品にできません';
  return REG_WHY[regState] || (regState ? `登録の状態が「${regState}」` : '登録の状態が無い (切替の前の商品)');
}
const escLike = (s) => String(s).replace(/[\\%_]/g, (c) => `\\${c}`);
/** ひらがな (search-fold.mjs の HIRA_CHARS と同じ字: ぁ〜ゖ・ゝゞ) を 1 字でも含むか (PostgreSQL の正規表現) */
const HIRA_CLASS = `[${HIRA_CHARS}]`;

/**
 * 構成の行に入れる NE の商品を探す (読むだけ)。q = 打った文字 (COMPONENT_SEARCH_MIN 文字から)。
 *   当てる: ① 商品コード (前方一致を上に・部分一致も。全角英数・大文字・ハイフンの違いは同じ = normSku / core.norm_code だけ。
 *              🚨 コードには NFKC を掛けない (#1689 Codex R1 M2): core.norm_code はわざと NFKC を使わない (0001) = ﾊﾁ-01 と ハチ-01 は別の商品。保存の重なりの確かめとも同じ)
 *           ② 商品名の言葉 (空白で区切った言葉が全部入っている・全部の言葉を見る。言葉ごとにコードに入っていてもよい。かなの同一視 (NFKC を含む) = search-fold.mjs = 一覧と同じ = 名前だけ)
 *           ③ JAN (ぴったり・一覧の検索と同じ core.external_ids)
 *   並び: コードがぴったり → コードの前方一致 → JAN → コードの部分一致 → 名前。同じ段の中は使える商品が上・コード順。上限 limit 件 (more = もっとある)
 *   使えない商品 (例外の SKU・状態が無い) も出す = usable false と理由 (画面は灰色で押せない)。まだ NE に無い 下書き・NE 登録待ちは使える (not_in_ne = 画面の印)・
 *   要確認 (NE で直接作った商品) も使える (ne_direct = 画面の印・中原さん 10/12)。
 *   登録をやめた商品 (cancelled) は、一覧と同じく、コードをぴったり打ったときだけ出す (#1688 の一覧の決まり)。
 *   重さ: core.skus (会社の SKU は 7,400 ほど) を 1 回走査する 1 本の SQL。コードの前方一致の索引は無い (unique (company_id, code_norm) は照合順が "C" でない) が、
 *     一覧の検索 (read.mjs listSkus の q) と同じく索引なしで足りる (search-fold.mjs)。JAN は ux_external_ids_active のぴったり。
 *     名前のかなの同一視は search-fold.mjs の foldSql と同じ答え (NFKC・小文字・ひらがな → カタカナ) を、1 行に 1 回だけ作る (言葉が増えても作り直さない)。
 *     translate (ひらがな → カタカナ) は重い (本物の PG・8,000 件で 1 回 70ms ほど) = ひらがなを含む名前だけにかける (含まなければ translate しても同じ = 答えは変わらない)
 * stock = readWarehouseStock() の結果 (読めない = null)。単品・例外だけ在庫 (ロジザード) を付ける (セットは null)
 * 戻り値 { items: [{ code, name, kind, kind_label, reg_state, handling, usable, why, stock }], more, short }
 */
export async function searchMapComponents(db, q, { limit = COMPONENT_SEARCH_LIMIT, stock = null } = {}) {
  const text = String(q ?? '').trim();
  if ([...text].length < COMPONENT_SEARCH_MIN) return { items: [], more: false, short: true };
  if (text.length > COMPONENT_SEARCH_Q_MAX) return { items: [], more: false, short: false, too_long: true };   // 入口 (router) でも 400。黙って切らない
  const words = text.split(/\s+/).filter(Boolean);
  const params = [COMPANY_ID];
  params.push(HIRA_CHARS); const hira = params.length;
  params.push(KATA_CHARS); const kata = params.length;
  params.push(HIRA_CLASS); const hasHira = params.length;
  const conds = words.map((w) => {
    params.push(likeOf(normSku(w))); const c = params.length;
    params.push(likeOf(foldSearch(w))); const n = params.length;
    return `(b.code_norm like $${c} or b.fname like $${n})`;
  });
  const whole = normSku(text);
  params.push(whole); const exact = params.length;
  params.push(`${escLike(whole)}%`); const pre = params.length;
  params.push(likeOf(whole)); const any = params.length;
  params.push(text); const raw = params.length;
  params.push([...USABLE_SKU_KINDS]); const kinds = params.length;
  params.push([...USABLE_REG_STATES]); const states = params.length;
  const max = Math.max(1, Math.min(Number(limit) || COMPONENT_SEARCH_LIMIT, COMPONENT_SEARCH_LIMIT));
  params.push(max + 1); const lim = params.length;
  const rows = (await db.query(`with jan as (
      select e.entity_id from core.external_ids e
       where e.system = 'jan' and e.id_kind = 'jan' and e.valid_to is null and e.entity_type = 'product' and e.external_norm = core.norm_code($${raw})),
    low as materialized (
      select s.sku_id, s.code, s.code_norm, s.name, s.sku_kind, s.handling, s.product_id, lower(normalize(s.name, NFKC)) as lname from core.skus s where s.company_id = $1),
    b as materialized (
      select l.*, case when l.lname ~ $${hasHira} then translate(l.lname, $${hira}, $${kata}) else l.lname end as fname from low l),
    hit as (
      select b.code, b.code_norm, b.name, b.sku_kind, b.handling, r.state as reg_state, coalesce(b.product_id in (select entity_id from jan), false) as jan_hit
        from b left join ops.master_registrations r on r.sku_id = b.sku_id
       where ((${conds.join(' and ')}) or b.product_id in (select entity_id from jan))
         and (r.state is distinct from 'cancelled' or b.code_norm = $${exact}))
    select code, code_norm, name, sku_kind, handling, reg_state from hit
     order by case when code_norm = $${exact} then 0 when code_norm like $${pre} then 1 when jan_hit then 2 when code_norm like $${any} then 3 else 4 end,
              case when sku_kind = any($${kinds}::text[]) and reg_state = any($${states}::text[]) then 0 else 1 end,
              code_norm
     limit $${lim}`, params)).rows;
  const more = rows.length > max;
  const items = rows.slice(0, max).map((r) => {
    const why = componentUnusableWhy(r.sku_kind, r.reg_state);
    return { code: r.code, name: r.name, kind: r.sku_kind, kind_label: KIND_WORDS[r.sku_kind] || r.sku_kind, reg_state: r.reg_state ?? null, handling: r.handling,
      usable: !why, why, not_in_ne: NOT_IN_NE_REG_STATES.includes(r.reg_state), ne_direct: r.reg_state === 'quarantined', stock: r.sku_kind === 'set' ? null : stockOf(stock, r.code_norm) };
  });
  return { items, more, short: false };
}
