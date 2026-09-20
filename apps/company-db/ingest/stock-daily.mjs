/**
 * ingest/stock-daily.mjs — miniPC から届いた「その日の在庫 (SKU 単位)」を snapshots.sku_stock_daily に入れる受け口の本体 (Company DB構想 08 §3.3 の ③ NE。D2b-1)。
 *
 * 1 日 = 1 要求 = 1 取引: stock_capture_days を building で作る → 行を入れる → complete に上げる (0011 の約束 = 日次の表は building の間だけ書ける・view は complete の日だけ読む)。
 *   途中で落ちれば全部巻き戻る = building の日が残らない。
 *
 * 決め:
 *   - 🚨 **先に確定した日は書き換えない** (first write wins): 同じ内容の再送 = 'same' / 内容の違う再送 = 'conflict' (HTTP 409)。
 *     NE の在庫の日次は「朝の取得の直後の値」で、同じ日に取り直した値で上書きすると、日ごとの比較の時刻がそろわなくなる。直したいときは保守の経路 (snapshots.maintenance) で人がやる
 *   - 取れなかった日は送り手が missing と申告する (過去の日だけ)。missing の日に後から行が届いたら complete に上げる (取り直せた)
 *   - 未来の日付 (JST) は受けない / 行 0 件は受けない (「取れなかった」は missing で言う。0 件を「在庫なし」と読まない)
 *   - SKU の解決 = core.skus.code_norm = core.norm_code(商品コード)。解決できない行も入れる (sku_id = null。件数を返す)
 *   - 内容の指紋 (sha256) は ops.ingest_runs.checksum に残す = 再送の判定はこれで行う
 *   - 同時実行は advisory lock (source + scope) で直列化。取れなければ LOCKED (503 = 送り手がやり直す)
 * source を足すとき (D2b-2 = fba_jp / fba_us): STOCK_SOURCES に足し、FBA の 7 区分の列を rows に通す
 */
import crypto from 'node:crypto';

export const COMPANY_ID = 1;
export const ENTITY = 'stock_daily';
export const MAX_ROWS = 50000;
/** source → { sourceSystem (ops.ingest_runs.source_system), scopes } */
export const STOCK_SOURCES = {
  ne: { sourceSystem: 'ne', scopes: ['main'] },
};

export function err(code, message) { const e = new Error(message); e.code = code; return e; }
const bad = (m) => err('BAD_REQUEST', m);
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
export const isRealDate = (v) => typeof v === 'string' && DATE_RE.test(v) && !Number.isNaN(Date.parse(`${v}T00:00:00Z`)) && new Date(`${v}T00:00:00Z`).toISOString().slice(0, 10) === v;
export const jstDate = (d) => new Date(d.getTime() + 9 * 3600 * 1000).toISOString().slice(0, 10);
const INT32_MAX = 2147483647;
export const CAPTURED_AT_FUTURE_MS = 10 * 60 * 1000;   // 取得時刻が「いま」より先でも、時計のずれとして許す幅

/**
 * 取得時刻の検証 (受け口と送り手で同じ関数を使う。Codex #1383 R1 #4)。通れば UTC の ISO 文字列、外れれば null。
 *   - 形は YYYY-MM-DDTHH:MM:SS(.fff…)(Z | ±HH:MM) だけ。**タイムゾーンの無い日時は受けない** (miniPC と Render で別の時刻に読まれる)
 *   - 実在する日時だけ (2/30・24 時・60 分を、Date が黙って繰り上げたものを通さない)
 *   - 未来は CAPTURED_AT_FUTURE_MS まで (内容の指紋は取得時刻を含まない = 間違った時刻で確定すると、後から同じ内容を送り直しても same で直らない)
 */
export function strictInstant(v, { now = Date.now() } = {}) {
  if (typeof v !== 'string') return null;
  const m = /^(\d{4}-\d{2}-\d{2})T(\d{2}):(\d{2}):(\d{2})(\.\d{1,9})?(Z|[+-]\d{2}:\d{2})$/.exec(v);
  if (!m) return null;
  if (!isRealDate(m[1]) || Number(m[2]) > 23 || Number(m[3]) > 59 || Number(m[4]) > 59) return null;
  if (m[6] !== 'Z' && (Number(m[6].slice(1, 3)) > 14 || Number(m[6].slice(4, 6)) > 59)) return null;
  const t = Date.parse(v);
  if (Number.isNaN(t) || t > now + CAPTURED_AT_FUTURE_MS) return null;
  return new Date(t).toISOString();
}

/** 内容の指紋: 行を商品コードの順に並べて (code, qty) だけを見る (送る順・captured_at には依らない) */
export function stockChecksum(rows) {
  const h = crypto.createHash('sha256');
  for (const r of [...rows].sort((a, b) => (a.code < b.code ? -1 : a.code > b.code ? 1 : 0))) h.update(`${r.code}\u0000${r.qty}\n`);
  return h.digest('hex');
}

/**
 * 要求の検証。戻り値 = { source, scope, snapshotDate, missing, capturedAt, rows: [{ code, qty }] }
 * 商品コードは **受け取った文字列のまま** 鍵にする (trim・小文字化しない = 送り手の検証と同じ値。前後の空白・制御文字は拒否)
 */
export function validateStockDayBody(body, { todayJst = jstDate(new Date()), now = Date.now() } = {}) {
  if (!body || typeof body !== 'object' || Array.isArray(body)) throw bad('body は object');
  const source = body.source, scope = body.scope ?? 'main';
  if (typeof source !== 'string' || !Object.hasOwn(STOCK_SOURCES, source)) throw bad(`source は ${Object.keys(STOCK_SOURCES).join(' / ')}: ${String(source).slice(0, 40)}`);
  if (typeof scope !== 'string' || !STOCK_SOURCES[source].scopes.includes(scope)) throw bad(`scope は ${STOCK_SOURCES[source].scopes.join(' / ')}: ${String(scope).slice(0, 40)}`);
  const snapshotDate = body.snapshot_date;
  if (!isRealDate(snapshotDate)) throw bad(`snapshot_date は実在する YYYY-MM-DD: ${String(snapshotDate).slice(0, 40)}`);
  if (snapshotDate > todayJst) throw bad(`snapshot_date が未来 (JST の今日 = ${todayJst}): ${snapshotDate}`);
  const missing = body.missing === true;
  if (body.missing !== undefined && typeof body.missing !== 'boolean') throw bad('missing は boolean');
  if (missing) {
    if (snapshotDate >= todayJst) throw bad(`今日 (${todayJst}) 以降を missing にはできない (まだ取れるかもしれない): ${snapshotDate}`);
    if (body.rows !== undefined && !(Array.isArray(body.rows) && body.rows.length === 0)) throw bad('missing のときは rows を付けない');
    return { source, scope, snapshotDate, missing: true, capturedAt: null, rows: [] };
  }
  const capturedAt = strictInstant(body.captured_at, { now });
  if (!capturedAt) throw bad(`captured_at は実在する日時で、Z か ±HH:MM つきの ISO 8601 (未来は不可): ${String(body.captured_at).slice(0, 40)}`);
  const rows = body.rows;
  if (!Array.isArray(rows) || rows.length === 0) throw bad('rows が空 (取れなかった日は missing: true で申告する。0 件を「在庫なし」と読まない)');
  if (rows.length > MAX_ROWS) throw bad(`rows が多すぎる (${rows.length} > ${MAX_ROWS})`);
  const seen = new Set();
  const out = [];
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    if (!r || typeof r !== 'object') throw bad(`rows[${i}] は object`);
    const code = r.code;
    if (typeof code !== 'string' || code === '' || code.length > 200 || code !== code.trim() || /[\u0000-\u001f\u007f]/.test(code)) throw bad(`rows[${i}].code が不正 (空・前後の空白・制御文字・200 文字超)`);
    if (seen.has(code)) throw bad(`rows[${i}].code が重複: ${code.slice(0, 60)}`);
    seen.add(code);
    if (!Number.isInteger(r.qty) || r.qty < 0 || r.qty > INT32_MAX) throw bad(`rows[${i}].qty は 0 以上の整数 (int32): ${String(r.qty).slice(0, 40)}`);
    out.push({ code, qty: r.qty });
  }
  return { source, scope, snapshotDate, missing: false, capturedAt, rows: out };
}

export function newStockRunId(source) {
  const d = new Date();
  const p = (n, w = 2) => String(n).padStart(w, '0');
  const stamp = `${d.getUTCFullYear()}${p(d.getUTCMonth() + 1)}${p(d.getUTCDate())}${p(d.getUTCHours())}${p(d.getUTCMinutes())}${p(d.getUTCSeconds())}${p(d.getUTCMilliseconds(), 3)}`;
  return `stk_${source}_${stamp}_${crypto.randomBytes(3).toString('hex')}`;
}

const affected = (r) => (r && (r.rowCount ?? r.affectedRows ?? (Array.isArray(r.rows) ? r.rows.length : 0))) || 0;

/**
 * 1 日ぶんを入れる。db = { query, exec } (router の pgAdapter / 試験の PGlite)。
 * @returns {{ status: 'applied'|'same'|'missing'|'missing_same', source, scope, snapshot_date, rows, resolved, unresolved, run_id, checksum }}
 *   例外: BAD_REQUEST (400) / CONFLICT (409 = 確定済みの日に違う内容) / LOCKED (503)
 */
export async function ingestStockDay(db, body, { host = 'render', companyId = COMPANY_ID, todayJst, now, afterWrite = null, log = () => {} } = {}) {
  const v = validateStockDayBody(body, { ...(todayJst ? { todayJst } : {}), ...(now !== undefined ? { now } : {}) });
  const { source, scope, snapshotDate } = v;
  const one = async (sql, p) => (await db.query(sql, p)).rows[0];
  const checksum = v.missing ? null : stockChecksum(v.rows);
  await db.exec('begin');
  try {
    const got = (await one(`select pg_try_advisory_xact_lock(hashtext($1)) as got`, [`company-db:stock-daily:${source}:${scope}`])).got;
    if (!got) throw err('LOCKED', `${source}/${scope} の別の取込が走っている`);
    const day = await one(
      `select d.status, d.ingest_run_id, r.checksum from snapshots.stock_capture_days d left join ops.ingest_runs r on r.ingest_run_id = d.ingest_run_id
        where d.snapshot_date = $1::date and d.source = $2 and d.scope_key = $3 for update of d`, [snapshotDate, source, scope]);
    if (day && day.status !== 'missing') {
      // 確定済み (complete / partial) ・作りかけ (building は 1 取引の中だけなので、ここで見えることは無いはず) の日は書き換えない
      await db.exec('rollback');
      if (v.missing) throw err('CONFLICT', `${snapshotDate} は ${day.status} で確定済み (missing にはできない)`);
      if (day.status === 'complete' && day.checksum === checksum) {
        log(`${snapshotDate}: same (確定済みと同じ内容)`);
        return { status: 'same', source, scope, snapshot_date: snapshotDate, rows: v.rows.length, resolved: null, unresolved: null, run_id: day.ingest_run_id, checksum };
      }
      throw err('CONFLICT', `${snapshotDate} は ${day.status} で確定済みで、内容が違う (先に確定した日は書き換えない。直すなら保守の経路で)`);
    }
    if (v.missing) {
      if (day) { await db.exec('rollback'); return { status: 'missing_same', source, scope, snapshot_date: snapshotDate, rows: 0, resolved: 0, unresolved: 0, run_id: null, checksum: null }; }
      await db.query(`insert into snapshots.stock_capture_days (snapshot_date, source, scope_key, company_id, status, ingest_run_id) values ($1::date, $2, $3, $4::smallint, 'missing', null)`, [snapshotDate, source, scope, companyId]);
      await db.exec('commit');
      log(`${snapshotDate}: missing (送り手の申告)`);
      return { status: 'missing', source, scope, snapshot_date: snapshotDate, rows: 0, resolved: 0, unresolved: 0, run_id: null, checksum: null };
    }
    const runId = newStockRunId(source);
    await db.query(
      `insert into ops.ingest_runs (ingest_run_id, source_system, entity, scope_key, host, started_at, status, complete, rows_seen, source_tz, checksum, format_version)
       values ($1, $2, $3, $4, $5, now(), 'running', false, $6, 'UTC', $7, 'v1')`, [runId, STOCK_SOURCES[source].sourceSystem, ENTITY, scope, host, v.rows.length, checksum]);
    if (day) {
      // missing だった日に行が届いた (後から取り直せた) → building に上げてから入れる
      await db.query(`update snapshots.stock_capture_days set status = 'building', ingest_run_id = $4, captured_at = $5::timestamptz, rows = $6, built_at = now()
                       where snapshot_date = $1::date and source = $2 and scope_key = $3`, [snapshotDate, source, scope, runId, v.capturedAt, v.rows.length]);
    } else {
      await db.query(`insert into snapshots.stock_capture_days (snapshot_date, source, scope_key, company_id, status, ingest_run_id, captured_at, rows)
                      values ($1::date, $2, $3, $4::smallint, 'building', $5, $6::timestamptz, $7)`, [snapshotDate, source, scope, companyId, runId, v.capturedAt, v.rows.length]);
    }
    const inserted = affected(await db.query(
      `insert into snapshots.sku_stock_daily (snapshot_date, source, scope_key, source_code, company_id, sku_id, qty, captured_at, ingest_run_id)
       select $1::date, $2, $3, t.code, $4::smallint, k.sku_id, t.qty, $6::timestamptz, $5
         from unnest($7::text[], $8::int[]) as t(code, qty)
         left join core.skus k on k.company_id = $4::smallint and k.code_norm = core.norm_code(t.code)`,
      [snapshotDate, source, scope, companyId, runId, v.capturedAt, v.rows.map((r) => r.code), v.rows.map((r) => r.qty)]));
    if (inserted !== v.rows.length) throw err('ROWS_MISMATCH', `入れた行数 ${inserted} が、届いた行数 ${v.rows.length} と違う (SKU の解決で行が増減した)`);
    const resolved = Number((await one(`select count(*)::int as n from snapshots.sku_stock_daily where snapshot_date = $1::date and source = $2 and scope_key = $3 and sku_id is not null`, [snapshotDate, source, scope])).n);
    await db.query(`update snapshots.stock_capture_days set status = 'complete', completed_at = now() where snapshot_date = $1::date and source = $2 and scope_key = $3`, [snapshotDate, source, scope]);
    await db.query(`update ops.ingest_runs set status = 'success', complete = true, finished_at = now(), rows_inserted = $2, rows_skipped = 0 where ingest_run_id = $1`, [runId, inserted]);
    if (afterWrite) await afterWrite();   // 試験用: 全部書いた後・commit の前
    await db.exec('commit');
    log(`${snapshotDate}: applied (行 ${inserted} / SKU が分かった ${resolved} / 分からない ${inserted - resolved})`);
    return { status: 'applied', source, scope, snapshot_date: snapshotDate, rows: inserted, resolved, unresolved: inserted - resolved, run_id: runId, checksum };
  } catch (e) {
    try { await db.exec('rollback'); } catch { /* 取引が既に無い */ }
    throw e;
  }
}

/** 期間の状態 (送り手が「まだ送っていない日」を決めるのに使う)。戻り値 = [{ snapshot_date, status, rows, checksum }] */
export async function stockDayStatus(db, { source, scope = 'main', from, to }) {
  if (typeof source !== 'string' || !Object.hasOwn(STOCK_SOURCES, source)) throw bad('source が不正');
  if (!STOCK_SOURCES[source].scopes.includes(scope)) throw bad('scope が不正');
  if (!isRealDate(from) || !isRealDate(to) || from > to) throw bad('from / to は YYYY-MM-DD で from <= to');
  if ((Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) / 86400000 > 800) throw bad('範囲は 800 日まで');
  const r = await db.query(
    `select d.snapshot_date::text as snapshot_date, d.status, d.rows, r.checksum
       from snapshots.stock_capture_days d left join ops.ingest_runs r on r.ingest_run_id = d.ingest_run_id
      where d.source = $1 and d.scope_key = $2 and d.snapshot_date between $3::date and $4::date order by d.snapshot_date`, [source, scope, from, to]);
  return r.rows.map((x) => ({ snapshot_date: x.snapshot_date, status: x.status, rows: x.rows == null ? null : Number(x.rows), checksum: x.checksum || null }));
}
