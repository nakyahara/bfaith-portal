/**
 * ingest/amazon-listings.mjs — miniPC から届いた「Amazon の出品一覧」(出品レポート GET_MERCHANT_LISTINGS_ALL_DATA の 1 回分) を
 *   ops.amazon_listing_report に入れる受け口の本体 (2026-10-12 中原さん「Amazon の登録の商品名・まだつないでいない SKU」。受け皿 = migration 0074。
 *   送り手 = apps/company-db/push/amazon-listings.mjs = 毎朝の daily-sync の 1 工程。材料 = 想定利益の夜間 (miniPC 23:30) が残した TSV = SP-API は 2 回取らない)。
 *
 * 1 要求 = 1 回分の全部 = 1 取引: 店の行を丸ごと消して入れ直し、見出し (ops.amazon_listing_report_loads) を 1 行足す。
 *
 * 決め (ad-spend・sku-cost-observed と同じ型):
 *   - 🚨 **取得の世代で古い要求を拒む**: generation = レポートを取り終えた時刻 (ms・manifest の snapshot_at)。送った時刻ではない
 *       Render の今の世代より古い → stale (書かない) / 同じ世代 + 同じ run_id + 同じ指紋 → same / 同じ世代でどれかが違う → CONFLICT (409)
 *       新しい世代で指紋が同じ → refreshed (行は触らず見出しの世代・時刻・run_id だけ進める = 「今朝も届いた」と読める) / 新しい世代で指紋が違う → applied (入れ替え)
 *   - 🚨 0 行は受けない (400): 「取れなかった」を「全部の出品が消えた」と読まない (archive-items.mjs も 0 件は保存しない)
 *   - 行の形はここの listingRowOf (送り手も同じ関数で検証する = 送り手が外した行の数は skipped_count で届く)。1 行でも外れた要求は 400 (送り手が先に外す約束)
 *   - 内容の指紋 = listingChecksum (送り手も同じ関数)。受け口は届いた行から計算し直し、送り手の値と合わなければ 400
 *   - 同時実行は advisory lock (会社 + 店) で直列化。取れなければ LOCKED (503 = 送り手が次の朝にやり直す)
 */
import crypto from 'node:crypto';
import { canonicalSha256 } from '../canonical-hash.mjs';
import { isRealDate, isCount, err } from './stock-daily.mjs';

export const COMPANY_ID = 1;
export const SOURCE_SYSTEM = 'amazon';
export const ENTITY = 'listing_report';
/** 店 = core.amazon_jp_shop_code() (0054) と同じ。2 つ目の店ができたら足す */
export const SHOP_CODES = Object.freeze(['main@A1VC38T7YXB528']);
export const SHOP_CODE = SHOP_CODES[0];
export const CHECKSUM_VERSION = 'alr-v1';
export const MAX_ROWS = 50000;   // 2026-09 の実測 = 7,594 行
export const STATUSES = Object.freeze(['active', 'inactive', 'incomplete', 'unknown']);
export const FULFILLMENTS = Object.freeze(['FBA', 'FBM']);
export const GENERATION_FUTURE_MS = 10 * 60 * 1000;   // 取得の時刻が「いま」より先でも、時計のずれとして許す幅
/** 🛑 受け口の安全弁: 新しい行が今の行 (同じ取引の中で数える) のこの割合を下回ったら入れ替えない (送り手の安全弁と同じ値。#1691 Codex R1 M2) */
export const MIN_ROW_RATIO = 0.8;
const bad = (m) => err('BAD_REQUEST', m);
// 🚨 制御文字はソースにエスケープで書かず、文字コードから作る (stock-daily.mjs と同じ理由)
const CONTROL = new RegExp(`[${String.fromCharCode(0)}-${String.fromCharCode(31)}${String.fromCharCode(127)}]`);
const PRICE_RE = /^(0|[1-9][0-9]{0,11})(\.[0-9]{1,2})?$/;
const ASIN_RE = /^[A-Z0-9]{10}$/;
const RUN_ID_RE = /^[A-Za-z0-9_.:-]{1,100}$/;
const HEX64 = /^[0-9a-f]{64}$/;

/** 金額の文字列 → 小数 2 桁の文字列 ('1980' → '1980.00')。形が外れていれば null */
export function canonPrice(v) {
  if (typeof v !== 'string' || !PRICE_RE.test(v)) return null;
  const [i, f = ''] = v.split('.');
  return `${i}.${f.padEnd(2, '0')}`;
}

const optText = (v, name, max) => {
  if (v === null) return null;
  if (typeof v !== 'string' || v === '' || v.length > max || CONTROL.test(v)) throw new Error(`${name} は null か ${max} 文字までの文字 (空・制御文字なし): ${JSON.stringify(v).slice(0, 40)}`);
  return v;
};

/** 正規の形の 1 行 (鍵の順を固定)。指紋と送る body の両方に使う */
export const canonRow = (r) => ({
  seller_sku: r.seller_sku, item_name: r.item_name, asin: r.asin, open_date: r.open_date, open_date_raw: r.open_date_raw,
  fulfillment: r.fulfillment, channel_raw: r.channel_raw, status: r.status, status_raw: r.status_raw, quantity: r.quantity, price: r.price,
});

/** 1 行を検証して正規化する (送り手も同じ関数を使う)。外れていれば理由の文字列を投げる */
export function listingRowOf(r) {
  if (!r || typeof r !== 'object' || Array.isArray(r)) throw new Error('object でない');
  const sku = r.seller_sku;
  if (typeof sku !== 'string' || sku === '' || sku.length > 255 || sku !== sku.trim() || CONTROL.test(sku)) throw new Error(`seller_sku が不正 (空・255 文字超・前後の空白・制御文字): ${JSON.stringify(String(sku)).slice(0, 40)}`);
  const itemName = optText(r.item_name ?? null, 'item_name', 2000);
  const asin = r.asin ?? null;
  if (asin !== null && (typeof asin !== 'string' || !ASIN_RE.test(asin))) throw new Error(`asin は null か英大文字・数字 10 桁: ${JSON.stringify(asin).slice(0, 30)}`);
  const openDate = r.open_date ?? null;
  if (openDate !== null && !isRealDate(openDate)) throw new Error(`open_date は null か実在する YYYY-MM-DD: ${JSON.stringify(openDate).slice(0, 30)}`);
  const openRaw = optText(r.open_date_raw ?? null, 'open_date_raw', 60);
  const ful = r.fulfillment ?? null;
  if (ful !== null && !FULFILLMENTS.includes(ful)) throw new Error(`fulfillment は null か ${FULFILLMENTS.join(' / ')}: ${JSON.stringify(ful).slice(0, 20)}`);
  const channelRaw = optText(r.channel_raw ?? null, 'channel_raw', 60);
  if (typeof r.status !== 'string' || !STATUSES.includes(r.status)) throw new Error(`status は ${STATUSES.join(' / ')}: ${JSON.stringify(r.status).slice(0, 20)}`);
  const statusRaw = optText(r.status_raw ?? null, 'status_raw', 60);
  const qty = r.quantity ?? null;
  if (qty !== null && !isCount(qty)) throw new Error(`quantity は null か 0 以上の整数: ${String(qty).slice(0, 20)}`);
  let price = null;
  if (r.price !== null && r.price !== undefined) { price = canonPrice(r.price); if (price === null) throw new Error(`price は null か、0 以上・小数 2 桁までの十進の文字列: ${JSON.stringify(r.price).slice(0, 30)}`); }
  return canonRow({ seller_sku: sku, item_name: itemName, asin, open_date: openDate, open_date_raw: openRaw, fulfillment: ful, channel_raw: channelRaw, status: r.status, status_raw: statusRaw, quantity: qty, price });
}

/** 🚨 並びは文字列の UTF-16 の順 (< で比べる。localeCompare は環境で変わる) */
export const bySku = (a, b) => (a.seller_sku < b.seller_sku ? -1 : a.seller_sku > b.seller_sku ? 1 : 0);

/**
 * 内容の指紋 (送り手も同じ関数) = 正規の JSON の SHA-256 ({ format: 版, rows: seller SKU の順 })。
 * 🚨 保存済みの指紋と同じ式のまま変えない (変えるときは CHECKSUM_VERSION を上げる = 次の回は applied で入れ替わる)
 */
export function listingChecksum(rows) {
  return canonicalSha256({ format: CHECKSUM_VERSION, rows: rows.map(canonRow).sort(bySku) });
}

/** 要求の検証。戻り値 = { shopCode, generation, runId, snapshotAt, checksum, skipped, rows } */
export function validateListingBody(body, { nowMs = Date.now() } = {}) {
  if (!body || typeof body !== 'object' || Array.isArray(body)) throw bad('body は object');
  if (typeof body.shop_code !== 'string' || !SHOP_CODES.includes(body.shop_code)) throw bad(`shop_code は ${SHOP_CODES.join(' / ')}: ${String(body.shop_code).slice(0, 40)}`);
  const generation = body.generation;
  if (!Number.isSafeInteger(generation) || generation <= 0) throw bad(`generation は正の整数 (レポートを取り終えた時刻 ms): ${String(generation).slice(0, 30)}`);
  if (generation > nowMs + GENERATION_FUTURE_MS) throw bad(`generation が未来 (${new Date(generation).toISOString()})`);
  if (typeof body.snapshot_at !== 'string' || Date.parse(body.snapshot_at) !== generation) throw bad(`snapshot_at は generation と同じ時刻の日時の文字: ${String(body.snapshot_at).slice(0, 40)}`);
  if (typeof body.report_run_id !== 'string' || !RUN_ID_RE.test(body.report_run_id)) throw bad('report_run_id が無い・不正 (英数字と _ . : - の 100 文字まで)');
  if (typeof body.checksum !== 'string' || !HEX64.test(body.checksum)) throw bad('checksum は 64 桁の 16 進');
  if (!isCount(body.row_count) || !isCount(body.skipped_count)) throw bad('row_count・skipped_count は 0 以上の整数');
  if (body.force !== undefined && typeof body.force !== 'boolean') throw bad('force は boolean (わざと減らすときだけ true)');
  if (!Array.isArray(body.rows)) throw bad('rows は配列');
  if (body.rows.length === 0) throw bad('rows が 0 行 = 受けない (取れなかったを「全部の出品が消えた」と読まない)');
  if (body.rows.length > MAX_ROWS) throw bad(`rows が多すぎる (${body.rows.length} > ${MAX_ROWS})`);
  if (body.row_count !== body.rows.length) throw bad(`row_count (${body.row_count}) が rows の数 (${body.rows.length}) と違う`);
  const rows = [], seen = new Set();
  for (let i = 0; i < body.rows.length; i++) {
    let row;
    try { row = listingRowOf(body.rows[i]); } catch (e) { throw bad(`rows[${i}]: ${e.message}`); }
    if (seen.has(row.seller_sku)) throw bad(`rows[${i}] の seller_sku が重複: ${row.seller_sku.slice(0, 60)}`);
    seen.add(row.seller_sku);
    rows.push(row);
  }
  rows.sort(bySku);
  const checksum = listingChecksum(rows);
  if (body.checksum !== checksum) throw bad(`checksum が届いた行から計算した値と合わない (送り手 ${body.checksum.slice(0, 16)}… / 受け口 ${checksum.slice(0, 16)}…)`);
  return { shopCode: body.shop_code, generation, runId: body.report_run_id, snapshotAt: new Date(generation).toISOString(), checksum, skipped: body.skipped_count, rows, force: body.force === true };
}

export function newListingRunId() {
  const d = new Date();
  const p = (n, w = 2) => String(n).padStart(w, '0');
  const stamp = `${d.getUTCFullYear()}${p(d.getUTCMonth() + 1)}${p(d.getUTCDate())}${p(d.getUTCHours())}${p(d.getUTCMinutes())}${p(d.getUTCSeconds())}${p(d.getUTCMilliseconds(), 3)}`;
  return `alr_${stamp}_${crypto.randomBytes(3).toString('hex')}`;
}

const affected = (r) => (r && (r.rowCount ?? r.affectedRows ?? (Array.isArray(r.rows) ? r.rows.length : 0))) || 0;
const iso = (v) => (v instanceof Date ? v.toISOString() : v == null ? null : new Date(v).toISOString());
const LOAD_COLS = 'load_id, generation, report_run_id, snapshot_at, checksum, row_count, skipped_count, ingest_run_id, received_at, refreshed_at';
const loadOut = (x) => (x ? {
  load_id: Number(x.load_id), generation: Number(x.generation), report_run_id: x.report_run_id, snapshot_at: iso(x.snapshot_at), checksum: x.checksum,
  row_count: Number(x.row_count), skipped_count: Number(x.skipped_count), ingest_run_id: x.ingest_run_id, received_at: iso(x.received_at), refreshed_at: iso(x.refreshed_at),
} : null);

/** 表があるか (0074 の前の DB = 送り手に not_migrated を返す) */
export async function listingTablesReady(db) {
  return (await db.query(`select to_regclass('ops.amazon_listing_report') is not null and to_regclass('ops.amazon_listing_report_loads') is not null as ok`)).rows[0].ok === true;
}

/**
 * 1 回分を入れる。db = { query, exec } (router の pgAdapter / 試験の PGlite)。
 * @returns {{ status: 'applied'|'same'|'refreshed'|'stale', shop_code, generation, checksum, rows, load_id, run_id, replaced?, remote_generation? }}
 *   例外: BAD_REQUEST (400) / NOT_MIGRATED (409) / CONFLICT (409 = 同じ世代で違う内容) / SHRINK (409 = 行が今の 80% 未満・force なし) / LOCKED (503)
 */
export async function ingestAmazonListings(db, body, { host = 'render', companyId = COMPANY_ID, nowMs, afterWrite = null, log = () => {} } = {}) {
  const v = validateListingBody(body, nowMs != null ? { nowMs } : {});
  if (!(await listingTablesReady(db))) throw err('NOT_MIGRATED', 'Company DB に Amazon の出品一覧の表がまだ無い (migration 0074 の前)');
  const one = async (sql, p) => (await db.query(sql, p)).rows[0];
  const base = { shop_code: v.shopCode, generation: v.generation, checksum: v.checksum };
  await db.exec('begin');
  try {
    const got = (await one(`select pg_try_advisory_xact_lock(hashtext($1)) as got`, [`company-db:amazon-listings:${companyId}:${v.shopCode}`])).got;
    if (!got) throw err('LOCKED', `Amazon の出品一覧 (${v.shopCode}) の別の取込が走っている`);
    const cur = await one(`select ${LOAD_COLS} from ops.amazon_listing_report_loads where company_id = $1::smallint and shop_code = $2 order by generation desc limit 1 for update`, [companyId, v.shopCode]);
    if (cur) {
      const rg = Number(cur.generation);
      if (rg > v.generation) {
        await db.exec('rollback');
        log(`stale (Render の世代 ${rg} > 届いた世代 ${v.generation})`);
        return { status: 'stale', ...base, rows: v.rows.length, load_id: null, run_id: null, remote_generation: rg };
      }
      if (rg === v.generation) {
        await db.exec('rollback');
        if (cur.report_run_id === v.runId && cur.checksum === v.checksum && Number(cur.row_count) === v.rows.length) {
          return { status: 'same', ...base, rows: Number(cur.row_count), load_id: Number(cur.load_id), run_id: cur.ingest_run_id };
        }
        throw err('CONFLICT', `同じ世代 (${v.generation}) で${cur.report_run_id !== v.runId ? `別のレポート (${cur.report_run_id} / ${v.runId})` : '内容が違う'} = どちらが新しいか分からないので書かない`);
      }
      if (cur.checksum === v.checksum && Number(cur.row_count) === v.rows.length) {
        // 新しい取得で中身が同じ: 行は触らず見出しだけ進める (見張り・画面が「今朝の取得が届いた」と読める。行の load_id はそのまま)
        await db.query(`update ops.amazon_listing_report_loads set generation = $2, report_run_id = $3, snapshot_at = $4::timestamptz, skipped_count = $5, refreshed_at = now() where load_id = $1`,
          [cur.load_id, v.generation, v.runId, v.snapshotAt, v.skipped]);
        if (afterWrite) await afterWrite();
        await db.exec('commit');
        log(`refreshed (世代 ${cur.generation} → ${v.generation} / 中身は同じ ${cur.row_count} 行)`);
        return { status: 'refreshed', ...base, rows: Number(cur.row_count), load_id: Number(cur.load_id), run_id: cur.ingest_run_id };
      }
    }
    // 🛑 安全弁 (#1691 Codex R1 M2): 送り手の安全弁は Render の前の回があるときだけ = 受け口でも同じ取引の中で今の行を数えて比べる (advisory lock の中 = 並ぶ取込は無い)。
    //   80% 未満 = 入れ替えない (SHRINK = 409)。わざと減らすときだけ送り手の --force (body.force = true)
    const nowRows = Number((await one(`select count(*)::int as n from ops.amazon_listing_report where company_id = $1::smallint and shop_code = $2`, [companyId, v.shopCode])).n);
    if (nowRows > 0 && v.rows.length < nowRows * MIN_ROW_RATIO && !v.force) {
      throw err('SHRINK', `行が今の ${nowRows} の ${MIN_ROW_RATIO * 100}% 未満 (${v.rows.length}) = 入れ替えない (既存の行を消さない)。わざとなら送り手の --force`);
    }
    const runId = newListingRunId();
    await db.query(
      `insert into ops.ingest_runs (ingest_run_id, source_system, entity, scope_key, host, started_at, status, complete, rows_seen, source_tz, checksum, format_version)
       values ($1, $2, $3, $4, $5, now(), 'running', false, $6, 'Asia/Tokyo', $7, $8)`, [runId, SOURCE_SYSTEM, ENTITY, v.shopCode, host, v.rows.length + v.skipped, v.checksum, CHECKSUM_VERSION]);
    const replaced = affected(await db.query(`delete from ops.amazon_listing_report where company_id = $1::smallint and shop_code = $2`, [companyId, v.shopCode]));
    const load = await one(
      `insert into ops.amazon_listing_report_loads (company_id, shop_code, generation, report_run_id, snapshot_at, checksum, row_count, skipped_count, ingest_run_id)
       values ($1::smallint, $2, $3, $4, $5::timestamptz, $6, $7, $8, $9) returning load_id`, [companyId, v.shopCode, v.generation, v.runId, v.snapshotAt, v.checksum, v.rows.length, v.skipped, runId]);
    const loadId = Number(load.load_id);
    const col = (c) => v.rows.map((r) => r[c]);
    const inserted = affected(await db.query(
      `insert into ops.amazon_listing_report (company_id, shop_code, seller_sku, load_id, item_name, asin, open_date, open_date_raw, fulfillment, channel_raw, status, status_raw, quantity, price_jpy)
       select $1::smallint, $2, t.sku, $3::bigint, t.nm, t.asin, t.od, t.odr, t.ful, t.chr, t.st, t.str, t.q, t.p::numeric
         from unnest($4::text[], $5::text[], $6::text[], $7::date[], $8::text[], $9::text[], $10::text[], $11::text[], $12::text[], $13::int[], $14::text[])
              as t(sku, nm, asin, od, odr, ful, chr, st, str, q, p)`,
      [companyId, v.shopCode, loadId, col('seller_sku'), col('item_name'), col('asin'), col('open_date'), col('open_date_raw'), col('fulfillment'), col('channel_raw'), col('status'), col('status_raw'), col('quantity'), col('price')]));
    if (inserted !== v.rows.length) throw err('ROWS_MISMATCH', `入れた行数 ${inserted} が、届いた行数 ${v.rows.length} と違う`);
    await db.query(`update ops.ingest_runs set status = 'success', complete = true, finished_at = now(), rows_inserted = $2, rows_skipped = $3, updated_at = now() where ingest_run_id = $1`, [runId, inserted, v.skipped]);
    if (afterWrite) await afterWrite();   // 試験用: 全部書いた後・commit の前
    await db.exec('commit');
    log(`applied (世代 ${cur ? cur.generation : '-'} → ${v.generation} / 行 ${inserted} / 消した前の行 ${replaced} / 送り手が外した行 ${v.skipped}${v.force ? ' / force' : ''})`);
    return { status: 'applied', ...base, rows: inserted, replaced, load_id: loadId, run_id: runId, forced: v.force && nowRows > 0 && v.rows.length < nowRows * MIN_ROW_RATIO };
  } catch (e) {
    try { await db.exec('rollback'); } catch { /* 取引が既に無い */ }
    throw e;
  }
}

/** 今の見出しと行の数 (送り手が回の始めに読む)。戻り値 = { shop_code, load: {...} | null, rows }。0074 の前 = NOT_MIGRATED */
export async function amazonListingsStatus(db, { companyId = COMPANY_ID, shopCode = SHOP_CODE } = {}) {
  if (!SHOP_CODES.includes(shopCode)) throw bad('shop_code が不正');
  if (!(await listingTablesReady(db))) throw err('NOT_MIGRATED', 'Company DB に Amazon の出品一覧の表がまだ無い (migration 0074 の前)');
  const cur = (await db.query(`select ${LOAD_COLS} from ops.amazon_listing_report_loads where company_id = $1::smallint and shop_code = $2 order by generation desc limit 1`, [companyId, shopCode])).rows[0];
  const n = (await db.query(`select count(*)::int as n from ops.amazon_listing_report where company_id = $1::smallint and shop_code = $2 and load_id = $3::bigint`, [companyId, shopCode, cur ? cur.load_id : null])).rows[0].n;
  return { shop_code: shopCode, load: loadOut(cur), rows: Number(n) };
}
