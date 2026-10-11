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
import { COMPANY_ID, jstDate, MasterWriteError } from '../../lib/master-write.mjs';
import { readAmazonMap, sellerSkuIn, newSellerSkuProblems, newSkuRefusedMessage, MAP_STATES, AMAZON_JP_SHOP_CODE, AMAZON_SKU_MAX_LEN } from '../../lib/amazon-map-write.mjs';
import { readCutoverPhase } from '../../lib/master-cutover.mjs';

export { AMAZON_SKU_MAX_LEN };

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
