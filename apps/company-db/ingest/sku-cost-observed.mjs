/**
 * ingest/sku-cost-observed.mjs — miniPC から届いた「観測の原価」(SKU × 期間) を core.sku_cost_observed に入れる受け口の本体
 *   (D7b-2。設計 = AI_reference『CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』§3.4・D-57。受け皿 = migration 0046。送り手 = apps/company-db/push/sku-cost-observed.mjs)。
 *
 * 1 要求 = 全部 = 1 取引: その会社 × 送り元 (warehouse_sqlite) の行を丸ごと消して入れ直し、受領の見出しを 1 行足す。
 *
 * 決め (設計 §3.4):
 *   - 🚨 **世代** (generation = 送り手の台帳の連番) で古い要求を拒む:
 *       Render の今の世代より古い → stale (書かない) / 同じ世代で manifest (checksum・行の数・結びつかない数・曖昧な数) が全部同じ → same (応答だけ失われた再送) /
 *       同じ世代で manifest のどれかが違う → CONFLICT (409) / 新しい世代 → applied (入れ替え)
 *   - 内容の指紋 = observedChecksum (送り手も同じ関数)。受け口は届いた行から計算し直し、送り手の値と合わなければ 400
 *   - 商品コード → SKU は core.norm_code (夜間ロードと同じ正規化)。🚨 Render に無い SKU のコードが 1 つでもあれば全部を拒む (409 SKU_UNRESOLVED。送り手は SKU の一覧を読んでから送る =
 *     その間に SKU が消えた・送り手の一覧が古い)。正規化で同じ SKU になるコードが 2 つ以上あれば 400 (送り手が曖昧として外す約束)
 *   - 同じ商品コードの期間は重ならない (400)。推定の行 (estimated_before_first_snapshot) は必ず終わりがある
 *   - 同時実行は advisory lock (会社 + 送り元) で直列化。取れなければ LOCKED (503 = 送り手がやり直す)
 */
import crypto from 'node:crypto';
import { normSku } from '../../../lib/sku-norm.js';
import { canonicalSha256 } from '../canonical-hash.mjs';
import { isRealDate, jstDate, isValidCode, err } from './stock-daily.mjs';

export const COMPANY_ID = 1;
export const ENTITY = 'sku_cost_observed';
export const SOURCES = ['warehouse_sqlite'];
export const SOURCE = 'warehouse_sqlite';
export const CHECKSUM_VERSION = 'sco-v1';
export const MAX_ROWS = 40000;   // 2026-09 の見込み = 最初の写し 6,882 SKU × (推定 + 観測) + 変化 ≒ 2 万行
export const COST_STATUSES = ['COMPLETE', 'OVERRIDDEN'];
export const BACKFILL_METHODS = ['observed_daily_diff', 'estimated_before_first_snapshot'];
export const MAX_FUTURE_DAYS = 2;   // valid_from は「今日の変化 → 明日から」があるので JST の今日 + 2 日まで (時計のずれ)
const TS_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/;
const bad = (m) => err('BAD_REQUEST', m);
const addDays = (d, n) => new Date(Date.parse(`${d}T00:00:00Z`) + n * 86400000).toISOString().slice(0, 10);
/** UTC の 'YYYY-MM-DDTHH:MM:SSZ' (秒まで・実在する時刻) か */
export const isUtcSecond = (v) => typeof v === 'string' && TS_RE.test(v) && !Number.isNaN(Date.parse(v)) && new Date(Date.parse(v)).toISOString().replace(/\.\d{3}Z$/, 'Z') === v;

/** 🚨 並びは文字列の UTF-16 の順 (< で比べる。localeCompare は環境で変わる)。商品コード → valid_from */
export const byCodeFrom = (a, b) => (a.product_code < b.product_code ? -1 : a.product_code > b.product_code ? 1 : a.valid_from < b.valid_from ? -1 : a.valid_from > b.valid_from ? 1 : 0);

/** 正規の形の 1 行 (鍵の順を固定)。checksum と送る body の両方に使う */
export const canonRow = (r) => ({
  product_code: r.product_code, cost_jpy: r.cost_jpy, cost_status: r.cost_status, valid_from: r.valid_from, valid_to: r.valid_to ?? null,
  backfill_method: r.backfill_method, source_history_id: r.source_history_id, first_observed_at: r.first_observed_at,
});

/**
 * 内容の指紋 (送り手も同じ関数) = 共通の部品 canonicalSha256 (正規の JSON の SHA-256。鍵の順は固定・null は JSON の null・数は整数だけ)。
 *   中身 = { format: 版, rows: 行を 商品コード → valid_from の順に }。日付は YYYY-MM-DD・日時は UTC の秒まで (行の検証で形を確かめ済み)。
 * 🚨 本番に保存済みの指紋と同じ式のまま変えない (変えるときは CHECKSUM_VERSION を上げる = 次の回は新しい世代で入れ替わる)
 */
export function observedChecksum(rows) {
  return canonicalSha256({ format: CHECKSUM_VERSION, rows: [...rows].map(canonRow).sort(byCodeFrom) });
}

/** 1 行を検証して正規化する (送り手も同じ関数を使う)。外れていれば理由の文字列を投げる */
export function observedRowOf(r) {
  if (!r || typeof r !== 'object' || Array.isArray(r)) throw new Error('object でない');
  if (!isValidCode(r.product_code)) throw new Error(`product_code が不正 (空・前後の空白・制御文字・200 文字超): ${JSON.stringify(String(r.product_code)).slice(0, 40)}`);
  if (!Number.isSafeInteger(r.cost_jpy) || r.cost_jpy < 0) throw new Error(`cost_jpy は 0 以上の整数 (円): ${String(r.cost_jpy).slice(0, 20)}`);
  if (typeof r.cost_status !== 'string' || !COST_STATUSES.includes(r.cost_status)) throw new Error(`cost_status は ${COST_STATUSES.join(' / ')}: ${String(r.cost_status).slice(0, 20)}`);
  if (!isRealDate(r.valid_from)) throw new Error(`valid_from は実在する YYYY-MM-DD: ${String(r.valid_from).slice(0, 20)}`);
  if (r.valid_to !== null && (!isRealDate(r.valid_to) || r.valid_to < r.valid_from)) throw new Error(`valid_to は null か valid_from 以後の YYYY-MM-DD: ${String(r.valid_to).slice(0, 20)}`);
  if (typeof r.backfill_method !== 'string' || !BACKFILL_METHODS.includes(r.backfill_method)) throw new Error(`backfill_method は ${BACKFILL_METHODS.join(' / ')}: ${String(r.backfill_method).slice(0, 40)}`);
  if (r.backfill_method === 'estimated_before_first_snapshot' && r.valid_to === null) throw new Error('推定の行 (estimated_before_first_snapshot) には終わり (valid_to) が要る');
  if (!Number.isSafeInteger(r.source_history_id) || r.source_history_id <= 0) throw new Error(`source_history_id は正の整数: ${String(r.source_history_id).slice(0, 20)}`);
  if (!isUtcSecond(r.first_observed_at)) throw new Error(`first_observed_at は UTC の YYYY-MM-DDTHH:MM:SSZ: ${String(r.first_observed_at).slice(0, 30)}`);
  return canonRow(r);
}

const isCount = (v) => Number.isInteger(v) && v >= 0 && v <= 2147483647;

/** 要求の検証。戻り値 = { source, generation, checksum, rowCount, unresolved, ambiguous, rows } */
export function validateObservedBody(body, { todayJst = jstDate(new Date()) } = {}) {
  if (!body || typeof body !== 'object' || Array.isArray(body)) throw bad('body は object');
  if (typeof body.source !== 'string' || !SOURCES.includes(body.source)) throw bad(`source は ${SOURCES.join(' / ')}: ${String(body.source).slice(0, 30)}`);
  const generation = body.generation;
  if (!Number.isSafeInteger(generation) || generation <= 0) throw bad(`generation は正の整数 (送り手の台帳の連番): ${String(generation).slice(0, 30)}`);
  if (typeof body.checksum !== 'string' || !/^[0-9a-f]{64}$/.test(body.checksum)) throw bad('checksum は 64 桁の 16 進');
  for (const k of ['row_count', 'unresolved_code_count', 'ambiguous_code_count']) if (!isCount(body[k])) throw bad(`${k} は 0 以上の整数: ${String(body[k]).slice(0, 20)}`);
  if (!Array.isArray(body.rows)) throw bad('rows は配列');
  if (body.rows.length > MAX_ROWS) throw bad(`rows が多すぎる (${body.rows.length} > ${MAX_ROWS})`);
  if (body.row_count !== body.rows.length) throw bad(`row_count (${body.row_count}) が rows の数 (${body.rows.length}) と違う`);
  const maxFrom = addDays(todayJst, MAX_FUTURE_DAYS);
  const rows = [];
  for (let i = 0; i < body.rows.length; i++) {
    let row;
    try { row = observedRowOf(body.rows[i]); } catch (e) { throw bad(`rows[${i}]: ${e.message}`); }
    if (row.valid_from > maxFrom) throw bad(`rows[${i}]: valid_from が未来すぎる (${row.valid_from} > JST の今日 + ${MAX_FUTURE_DAYS} 日 = ${maxFrom})`);
    rows.push(row);
  }
  rows.sort(byCodeFrom);
  // 同じ商品コードの期間は重ならない / 正規化で同じ SKU になるコードは 2 つ以上来ない (送り手が曖昧として外す)
  const normOwner = new Map();
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i], prev = i > 0 ? rows[i - 1] : null;
    if (prev && prev.product_code === r.product_code && (prev.valid_to === null || prev.valid_to >= r.valid_from)) throw bad(`${r.product_code}: 期間が重なる (${prev.valid_from}〜${prev.valid_to ?? '今'} と ${r.valid_from}〜)`);
    const k = normSku(r.product_code);
    const owner = normOwner.get(k);
    if (owner !== undefined && owner !== r.product_code) throw bad(`商品コード ${owner} と ${r.product_code} は正規化すると同じ SKU (曖昧なコードは送り手が外す)`);
    normOwner.set(k, r.product_code);
  }
  const checksum = observedChecksum(rows);
  if (body.checksum !== checksum) throw bad(`checksum が届いた行から計算した値と合わない (送り手 ${body.checksum.slice(0, 16)}… / 受け口 ${checksum.slice(0, 16)}…)`);
  return { source: body.source, generation, checksum, rowCount: rows.length, unresolved: body.unresolved_code_count, ambiguous: body.ambiguous_code_count, rows };
}

export function newObservedRunId() {
  const d = new Date();
  const p = (n, w = 2) => String(n).padStart(w, '0');
  const stamp = `${d.getUTCFullYear()}${p(d.getUTCMonth() + 1)}${p(d.getUTCDate())}${p(d.getUTCHours())}${p(d.getUTCMinutes())}${p(d.getUTCSeconds())}${p(d.getUTCMilliseconds(), 3)}`;
  return `sco_${stamp}_${crypto.randomBytes(3).toString('hex')}`;
}

const affected = (r) => (r && (r.rowCount ?? r.affectedRows ?? (Array.isArray(r.rows) ? r.rows.length : 0))) || 0;
const loadOut = (x) => (x ? { observed_load_id: Number(x.observed_load_id), generation: Number(x.generation), checksum: x.checksum, row_count: Number(x.row_count),
  unresolved_code_count: Number(x.unresolved_code_count), ambiguous_code_count: Number(x.ambiguous_code_count), ingest_run_id: x.ingest_run_id, sent_at: x.sent_at instanceof Date ? x.sent_at.toISOString() : x.sent_at } : null);
const LOAD_COLS = 'observed_load_id, generation, checksum, row_count, unresolved_code_count, ambiguous_code_count, ingest_run_id, sent_at';
/** manifest が全部同じか (checksum・行の数・結びつかない数・曖昧な数) */
export const sameManifest = (a, b) => a.checksum === b.checksum && Number(a.row_count) === Number(b.row_count)
  && Number(a.unresolved_code_count) === Number(b.unresolved_code_count) && Number(a.ambiguous_code_count) === Number(b.ambiguous_code_count);

/**
 * 全部を入れ替える。db = { query, exec } (router の pgAdapter / 試験の PGlite)。
 * @returns {{ status: 'applied'|'same'|'stale', generation, checksum, rows, observed_load_id, run_id, replaced?, remote_generation? }}
 *   例外: BAD_REQUEST (400) / CONFLICT (409 = 同じ世代で違う manifest) / SKU_UNRESOLVED (409) / LOCKED (503)
 */
export async function ingestSkuCostObserved(db, body, { host = 'render', companyId = COMPANY_ID, todayJst, afterWrite = null, log = () => {} } = {}) {
  const v = validateObservedBody(body, todayJst ? { todayJst } : {});
  const manifest = { checksum: v.checksum, row_count: v.rowCount, unresolved_code_count: v.unresolved, ambiguous_code_count: v.ambiguous };
  const one = async (sql, p) => (await db.query(sql, p)).rows[0];
  const base = { generation: v.generation, checksum: v.checksum };
  await db.exec('begin');
  try {
    const got = (await one(`select pg_try_advisory_xact_lock(hashtext($1)) as got`, [`company-db:sku-cost-observed:${companyId}:${v.source}`])).got;
    if (!got) throw err('LOCKED', `観測の原価 (${v.source}) の別の取込が走っている`);
    const cur = await one(`select ${LOAD_COLS} from core.sku_cost_observed_loads where company_id = $1::smallint and source = $2 order by generation desc limit 1 for update`, [companyId, v.source]);
    if (cur) {
      const rg = Number(cur.generation);
      if (rg > v.generation) {
        await db.exec('rollback');
        log(`stale (Render の世代 ${rg} > 届いた世代 ${v.generation})`);
        return { status: 'stale', ...base, rows: v.rowCount, observed_load_id: null, run_id: null, remote_generation: rg };
      }
      if (rg === v.generation) {
        await db.exec('rollback');
        if (sameManifest(cur, manifest)) return { status: 'same', ...base, rows: Number(cur.row_count), observed_load_id: Number(cur.observed_load_id), run_id: cur.ingest_run_id };
        throw err('CONFLICT', `同じ世代 (${v.generation}) で manifest が違う (checksum ${String(cur.checksum).slice(0, 12)}… / ${v.checksum.slice(0, 12)}…・行 ${cur.row_count} / ${v.rowCount}・結びつかない ${cur.unresolved_code_count} / ${v.unresolved}・曖昧 ${cur.ambiguous_code_count} / ${v.ambiguous}) = どちらが正しいか分からないので書かない`);
      }
    }
    // 商品コード → SKU (夜間ロードと同じ正規化 = core.norm_code。code_norm は core.skus の生成列)
    const codes = [...new Set(v.rows.map((r) => r.product_code))];
    const res = codes.length === 0 ? [] : (await db.query(
      `select t.code, s.sku_id from unnest($2::text[]) as t(code) left join core.skus s on s.company_id = $1::smallint and s.code_norm = core.norm_code(t.code)`, [companyId, codes])).rows;
    const skuOf = new Map(res.map((r) => [r.code, r.sku_id == null ? null : Number(r.sku_id)]));
    const missing = codes.filter((c) => skuOf.get(c) == null);
    if (missing.length) throw err('SKU_UNRESOLVED', `Render に無い SKU の商品コードが ${missing.length} ある (例: ${missing.slice(0, 3).join(', ')}) = 送り手が読んだ SKU の一覧と今の core.skus が違う。次の回で読み直して送る`);
    const runId = newObservedRunId();
    await db.query(
      `insert into ops.ingest_runs (ingest_run_id, source_system, entity, scope_key, host, started_at, status, complete, rows_seen, source_tz, checksum, format_version)
       values ($1, 'warehouse', $2, $3, $4, now(), 'running', false, $5, 'UTC', $6, $7)`, [runId, ENTITY, v.source, host, v.rowCount, v.checksum, CHECKSUM_VERSION]);
    const replaced = affected(await db.query(
      `delete from core.sku_cost_observed where company_id = $1::smallint and observed_load_id in (select observed_load_id from core.sku_cost_observed_loads where company_id = $1::smallint and source = $2)`, [companyId, v.source]));
    const load = await one(
      `insert into core.sku_cost_observed_loads (company_id, source, generation, checksum, row_count, unresolved_code_count, ambiguous_code_count, ingest_run_id)
       values ($1::smallint, $2, $3, $4, $5, $6, $7, $8) returning observed_load_id`, [companyId, v.source, v.generation, v.checksum, v.rowCount, v.unresolved, v.ambiguous, runId]);
    const loadId = Number(load.observed_load_id);
    const col = (c) => v.rows.map((r) => r[c]);
    const inserted = v.rows.length === 0 ? 0 : affected(await db.query(
      `insert into core.sku_cost_observed (observed_load_id, company_id, generation, sku_id, product_code, cost_jpy, cost_status, valid_from, valid_to, backfill_method, first_observed_at, source_history_id)
       select $1::bigint, $2::smallint, $3::bigint, t.sku, t.code, t.cost, t.st, t.vf, t.vt, t.m, t.fo, t.h
         from unnest($4::bigint[], $5::text[], $6::bigint[], $7::text[], $8::date[], $9::date[], $10::text[], $11::timestamptz[], $12::bigint[]) as t(sku, code, cost, st, vf, vt, m, fo, h)`,
      [loadId, companyId, v.generation, v.rows.map((r) => skuOf.get(r.product_code)), col('product_code'), col('cost_jpy'), col('cost_status'), col('valid_from'), col('valid_to'), col('backfill_method'), col('first_observed_at'), col('source_history_id')]));
    if (inserted !== v.rows.length) throw err('ROWS_MISMATCH', `入れた行数 ${inserted} が、届いた行数 ${v.rows.length} と違う`);
    await db.query(`update ops.ingest_runs set status = 'success', complete = true, finished_at = now(), rows_inserted = $2, rows_skipped = 0, updated_at = now() where ingest_run_id = $1`, [runId, inserted]);
    if (afterWrite) await afterWrite();   // 試験用: 全部書いた後・commit の前
    await db.exec('commit');
    log(`applied (世代 ${cur ? cur.generation : '-'} → ${v.generation} / 行 ${inserted} / SKU ${codes.length} / 消した前の行 ${replaced} / 結びつかない ${v.unresolved} / 曖昧 ${v.ambiguous})`);
    return { status: 'applied', ...base, rows: inserted, skus: codes.length, replaced, observed_load_id: loadId, run_id: runId };
  } catch (e) {
    try { await db.exec('rollback'); } catch { /* 取引が既に無い */ }
    throw e;
  }
}

/** 今の世代の見出しと行の数 (送り手が回の始めに読む)。戻り値 = { load: {...} | null, rows, skus } */
export async function skuCostObservedStatus(db, { companyId = COMPANY_ID, source = SOURCE } = {}) {
  if (!SOURCES.includes(source)) throw bad('source が不正');
  const cur = (await db.query(`select ${LOAD_COLS} from core.sku_cost_observed_loads where company_id = $1::smallint and source = $2 order by generation desc limit 1`, [companyId, source])).rows[0];
  const c = (await db.query(`select count(*)::int as n, count(distinct sku_id)::int as skus from core.sku_cost_observed o
     where o.company_id = $1::smallint and o.observed_load_id = $2::bigint`, [companyId, cur ? cur.observed_load_id : null])).rows[0];   // 今の見出しの行だけ数える (Codex #1549 R3 M3)
  return { source, load: loadOut(cur), rows: Number(c.n), skus: Number(c.skus) };
}

/** core.skus の code_norm (送り手が商品コードを結べるか決める)。keyset (collate "C" のバイト順) */
export async function skuCodeNorms(db, { after = '', limit = 20000, companyId = COMPANY_ID } = {}) {
  const lim = Math.min(Math.max(Number.isInteger(limit) ? limit : 20000, 1), 50000);
  const rows = (await db.query(`select code_norm from core.skus where company_id = $1::smallint and code_norm collate "C" > $2 order by code_norm collate "C" limit $3`, [companyId, String(after), lim])).rows;
  const keys = rows.map((r) => r.code_norm);
  return { keys, next: keys.length === lim ? keys[keys.length - 1] : null };
}
