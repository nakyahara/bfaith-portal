/**
 * amazon-read.mjs — マスタ入力画面の「Amazon SKU」タブが読むもの (一覧・1 つの seller SKU・未登録・変更の記録・関連 SKU)。書くのは lib/amazon-map-write.mjs
 * (Company DB構想 16 §2「画面」・§3 #9・§7 v2 M11 / L12。PR ⑦-1)
 *
 * FBA / FBM は Company DB に列を作らない (16 §3 #9) = 画面が Render の mirror_amazon_sku_fees (SQLite) の fulfillment_channel を読む (router の channelProvider)
 * 「未登録」= 直近 7 日の Amazon の注文で、出品が無い・出品に構成が無い seller SKU (ops.amazon_map_unmapped_recent)。
 *   売上の日次の公開がそろっていない日があれば「未判定」(0 件と言わない = M11)
 */
import { normSku } from '../../lib/sku-norm.js';
import { foldSearch, foldSql, likeOf } from './search-fold.mjs';
import { COMPANY_ID, jstDate } from '../../lib/master-write.mjs';
import { readAmazonMap, MAP_STATES, AMAZON_JP_SHOP_CODE } from '../../lib/amazon-map-write.mjs';
import { readCutoverPhase } from '../../lib/master-cutover.mjs';

export const AMAZON_LIST_LIMIT = 100;
export const UNMAPPED_DAYS = 7;
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

/** 一覧 = Company DB の対応 (墓標も)。今 (切替の前) は 0 件 = 正は miniPC の SKU マスタ */
export async function listAmazonMaps(db, filters, { channels = null } = {}) {
  const f = normalizeAmazonFilters(filters);
  if (!(await regclass(db, 'core.amazon_sku_maps'))) return { rows: [], total: 0, offset: 0, limit: AMAZON_LIST_LIMIT, filters: f, tableMissing: true };
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
  const total = Number((await db.query(`select count(*)::int as n from core.amazon_sku_maps m where ${where.join(' and ')}`, params)).rows[0].n);
  params.push(AMAZON_LIST_LIMIT, f.offset);
  const rows = (await db.query(`select m.seller_sku, m.name, m.state, m.origin, ${TS('m.changed_at')} as changed_at, m.changed_by, m.listing_id::text as listing_id, ci.asin,
        (select string_agg(k.code || '×' || c.qty, ', ' order by c.sort_order) from core.listing_components c join core.skus k on k.sku_id = c.sku_id where c.listing_id = m.listing_id) as comps
      from core.amazon_sku_maps m join core.listings l on l.listing_id = m.listing_id left join core.catalog_items ci on ci.catalog_item_id = l.catalog_item_id
     where ${where.join(' and ')}
     order by m.changed_at desc, m.seller_sku limit $${params.length - 1} offset $${params.length}`, params)).rows;
  for (const r of rows) r.channel = channels ? (channels.get(normSku(r.seller_sku)) ?? null) : null;
  return { rows, total, offset: f.offset, limit: AMAZON_LIST_LIMIT, filters: f, tableMissing: false };
}

/** 1 つの seller SKU の画面。対応が無くても、Company DB の出品と今の構成 (夜間ロードの写し) を見せる */
export async function readAmazonPage(db, sellerSku, { channels = null } = {}) {
  const cur = await readAmazonMap(db, sellerSku);
  const phase = await readCutoverPhase(db);
  const related = cur.listing ? (await db.query(`select distinct m2.seller_sku, m2.name from core.listing_components c1
       join core.listing_components c2 on c2.sku_id = c1.sku_id and c2.listing_id <> c1.listing_id
       join core.amazon_sku_maps m2 on m2.listing_id = c2.listing_id
      where c1.listing_id = $1 order by m2.seller_sku limit 50`, [cur.listing.listing_id])).rows : [];
  const lastRequest = cur.listing ? (await db.query(`select operation, actor_id, status, ${TS('finished_at')} as finished_at from ops.master_edit_requests
      where listing_id = $1 order by finished_at desc limit 1`, [cur.listing.listing_id])).rows[0] || null : null;
  return { cur, phase, related, lastRequest, channel: channels ? (channels.get(normSku(sellerSku)) ?? null) : null };
}

/** 変更の記録 (対応・構成・出品)。新しい順に 200 件 */
export async function amazonHistory(db, sellerSku) {
  const cur = await readAmazonMap(db, sellerSku);
  if (!cur.listing) return null;
  const events = (await db.query(`select e.event_id::text as event_id, e.operation, e.entity_type, e.attribute, e.old_value, e.new_value,
        e.actor_type, e.actor_id, e.source_system, e.reason_text, e.recorded_at::text as recorded_at
      from events.master_change_events e
     where (e.entity_type in ('amazon_sku_map', 'listing') and e.entity_id = $1::bigint)
        or (e.entity_type = 'listing_component' and (e.entity_key ->> 'listing_id')::bigint = $1::bigint)
     order by e.event_id desc limit 200`, [cur.listing.listing_id])).rows;
  return { cur, events };
}

/**
 * 未登録 (M11): 直近 UNMAPPED_DAYS 日の Amazon の注文で、出品が無い・出品に構成が無い seller SKU。channel = 'FBA' / 'FBM' / '' (全部)。
 * 売上の日次の公開がそろっていない日があれば undecided = その日の一覧 (画面は「未判定」)
 */
export async function amazonUnmapped(db, { now = new Date(), channel = 'FBA', channels = null } = {}) {
  const today = jstDate(now);
  if (!(await regclass(db, 'core.amazon_sku_maps'))) return { rows: [], today, undecided: [], tableMissing: true, channel, channelsAvailable: !!channels };
  const missing = (await db.query('select ops.amazon_map_sales_coverage($1::date, $2) as d', [today, UNMAPPED_DAYS])).rows[0].d || [];
  const all = (await db.query('select code, listing_id::text as listing_id, map_state, units::int as units, orders::int as orders, last_date::text as last_date from ops.amazon_map_unmapped_recent($1::date, $2) order by units desc, code',
    [today, UNMAPPED_DAYS])).rows;
  for (const r of all) r.channel = channels ? (channels.get(normSku(r.code)) ?? null) : null;
  const rows = channel && channels ? all.filter((r) => r.channel === channel) : all;
  return { rows, total_all: all.length, today, undecided: missing.map(dateOnly), tableMissing: false, channel, channelsAvailable: !!channels, shopCode: AMAZON_JP_SHOP_CODE, companyId: COMPANY_ID };
}
