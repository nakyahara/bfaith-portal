/**
 * ingest/stock-daily.mjs — miniPC から届いた「その日の在庫 (SKU 単位)」を snapshots.sku_stock_daily に入れる受け口の本体
 *   (Company DB構想 08 §3.3 の ③ NE = D2b-1 / ④ FBA = D2b-2)。
 *
 * 1 日 = 1 要求 = 1 取引: stock_capture_days を building で作る → 行を入れる → complete (FBA で一部だけ取れた日は partial) に上げる
 *   (0011 の約束 = 日次の表は building の間だけ書ける・view は complete の日だけ読む)。途中で落ちれば全部巻き戻る = building の日が残らない。
 *
 * 決め:
 *   - 🚨 **先に確定した日は書き換えない** (first write wins): 同じ内容の再送 = 'same' / 内容の違う再送 = 'conflict' (HTTP 409)。
 *     在庫の日次は「朝の取得の直後の値」で、同じ日に取り直した値で上書きすると、日ごとの比較の時刻がそろわなくなる。直したいときは保守の経路 (snapshots.maintenance) で人がやる。
 *     例外は 2 つだけ (どちらも「取れていなかったものが取れた」): missing → complete / partial、**partial → complete** (FBA: 後から RESTOCK が取れた)
 *   - 取れなかった日は送り手が missing と申告する (過去の日だけ)
 *   - 未来の日付 (JST) は受けない / 行 0 件は受けない (「取れなかった」は missing で言う。0 件を「在庫なし」と読まない)
 *   - 内容の指紋 (sha256) は ops.ingest_runs.checksum に残す = 再送の判定はこれで行う
 *   - 同時実行は advisory lock (source + scope) で直列化。取れなければ LOCKED (503 = 送り手がやり直す)
 * NE (kind = plain): rows = [{ code, qty }]。SKU は core.skus.code_norm = core.norm_code(商品コード)。分からない行も入れる (sku_id = null。件数を返す)
 * FBA (kind = fba。fba_jp / fba_us): rows = [{ code = 出品 SKU, fba_available, fba_fc_transfer, fba_fc_processing, fba_customer_order, fba_inbound_working, fba_inbound_shipped, fba_inbound_received }]
 *   - 🚨 FC 移管中・処理中・出荷待ち (fba_fc_transfer / fba_fc_processing / fba_customer_order) は RESTOCK レポートからしか取れない。**行ごとに「3 つとも数字」か「3 つとも null」**:
 *     null = その SKU は RESTOCK に載っていなかった = 分からない (0 ではない。PLANNING にしか無い SKU。Codex #1388 R1 #1)
 *   - 🚨 **partial**: RESTOCK レポートが丸ごと取れなかった日 = 全部の行が null。日の状態は partial = view は読まない。partial でない日は、数字の入った行が 1 つ以上ある
 *   - qty = FBA の倉庫の中の在庫 = fba_available + FC 移管中 + 処理中 + 出荷待ち (月末の棚卸しと同じ定義)。3 区分が null の行は分かっている fba_available だけ
 *   - SKU の解決 = 出品 (core.resolve_listing_id。会社 × モールで 1 件に当たるときだけ) の構成が **1 SKU × 1 個** のときだけ sku_id を入れる。
 *     まとめ売り (1 SKU × N 個)・セット (複数の SKU) は、FBA の 1 個が SKU の 1 個ではないので入れない (source_code に出品 SKU が残る。SKU の単位への展開は別の view の仕事)
 *   - captured_at_nominal = true: 元データに取得時刻が残っていない過去の日 (送り手がその日の朝の定刻を入れた)。ops.ingest_runs.format_version に 'v1-nominal-time' と残す
 */
import crypto from 'node:crypto';

export const COMPANY_ID = 1;
export const ENTITY = 'stock_daily';
export const MAX_ROWS = 50000;
/** source → { sourceSystem (ops.ingest_runs.source_system), scopes, kind, mall (FBA の出品を探すモール) } */
export const STOCK_SOURCES = {
  ne: { sourceSystem: 'ne', scopes: ['main'], kind: 'plain' },
  fba_jp: { sourceSystem: 'amazon', scopes: ['jp'], kind: 'fba', mall: 'amazon' },
  fba_us: { sourceSystem: 'amazon', scopes: ['us'], kind: 'fba', mall: 'amazon_us' },
};
/** FBA の 7 区分 (sku_stock_daily の列名と同じ)。RESTOCK_COLS = RESTOCK レポートからしか取れない 3 つ */
export const FBA_COLS = ['fba_available', 'fba_fc_transfer', 'fba_fc_processing', 'fba_customer_order', 'fba_inbound_working', 'fba_inbound_shipped', 'fba_inbound_received'];
export const RESTOCK_COLS = ['fba_fc_transfer', 'fba_fc_processing', 'fba_customer_order'];

export function err(code, message) { const e = new Error(message); e.code = code; return e; }
const bad = (m) => err('BAD_REQUEST', m);
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
export const isRealDate = (v) => typeof v === 'string' && DATE_RE.test(v) && !Number.isNaN(Date.parse(`${v}T00:00:00Z`)) && new Date(`${v}T00:00:00Z`).toISOString().slice(0, 10) === v;
export const jstDate = (d) => new Date(d.getTime() + 9 * 3600 * 1000).toISOString().slice(0, 10);
const INT32_MAX = 2147483647;
export const CAPTURED_AT_FUTURE_MS = 10 * 60 * 1000;   // 取得時刻が「いま」より先でも、時計のずれとして許す幅
// 🚨 制御文字はソースにエスケープで書かず、文字コードから作る (書いたエスケープが本物の制御文字でファイルに入り、git がバイナリ扱いにしたことがある)
const SEP = String.fromCharCode(0), LF = String.fromCharCode(10);
const CONTROL_CHARS = new RegExp('[' + String.fromCharCode(0) + '-' + String.fromCharCode(31) + String.fromCharCode(127) + ']');
/** 商品コード・出品 SKU の検証 (送り手と同じ規則): 受け取った文字列のまま鍵にする = trim・小文字化しない。空・前後の空白・制御文字・200 文字超は不正 */
export const isValidCode = (code) => typeof code === 'string' && code !== '' && code.length <= 200 && code === code.trim() && !CONTROL_CHARS.test(code);
const isCount = (v) => Number.isInteger(v) && v >= 0 && v <= INT32_MAX;

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

const byCode = (a, b) => (a.code < b.code ? -1 : a.code > b.code ? 1 : 0);
/** NE の内容の指紋: 行を商品コードの順に並べて (code, qty) だけを見る (送る順・captured_at には依らない)。🚨 本番に保存済みの指紋と同じ式のまま変えない */
export function stockChecksum(rows) {
  const h = crypto.createHash('sha256');
  for (const r of [...rows].sort(byCode)) h.update(`${r.code}${SEP}${r.qty}${LF}`);
  return h.digest('hex');
}
/** FBA の内容の指紋: partial かどうか + 出品 SKU の順に 7 区分 (null は空) */
export function fbaChecksum(rows, partial) {
  const h = crypto.createHash('sha256');
  h.update(`fba-v1${SEP}partial=${partial ? 1 : 0}${LF}`);
  for (const r of [...rows].sort(byCode)) h.update(`${r.code}${SEP}${FBA_COLS.map((c) => (r[c] == null ? '' : r[c])).join(SEP)}${LF}`);
  return h.digest('hex');
}
/** source に合った指紋 (送り手も同じ関数で、確定済みの日と比べる) */
export const checksumOf = (source, rows, partial = false) => (STOCK_SOURCES[source].kind === 'fba' ? fbaChecksum(rows, partial) : stockChecksum(rows));

/** FBA の 1 行を検証して { code, qty, ...7 区分 } にする (送り手も同じ関数を使う)。外れていれば理由の文字列を投げる */
export function fbaRowOf(r, partial) {
  if (!r || typeof r !== 'object') throw new Error('object でない');
  if (!isValidCode(r.code)) throw new Error('code が不正 (空・前後の空白・制御文字・200 文字超)');
  const out = { code: r.code };
  const known = RESTOCK_COLS.filter((c) => r[c] != null).length;   // 3 = RESTOCK に載っていた / 0 = 載っていなかった (不明) / それ以外は不正
  if (partial && known !== 0) throw new Error(`partial の日の ${RESTOCK_COLS.find((c) => r[c] != null)} は null (RESTOCK が取れていない = 分からない)`);
  if (known !== 0 && known !== RESTOCK_COLS.length) throw new Error('FC 移管中・処理中・出荷待ち は 3 つとも数字か、3 つとも null (RESTOCK に載っていない SKU)');
  for (const c of FBA_COLS) {
    const v = r[c];
    if (RESTOCK_COLS.includes(c) && known === 0) { out[c] = null; continue; }
    if (!isCount(v)) throw new Error(`${c} は 0 以上の整数 (int32): ${String(v).slice(0, 30)}`);
    out[c] = v;
  }
  const qty = out.fba_available + (known === 0 ? 0 : out.fba_fc_transfer + out.fba_fc_processing + out.fba_customer_order);
  if (!isCount(qty)) throw new Error('区分の合計が int32 に収まらない');
  out.qty = qty;
  return out;
}

/**
 * 要求の検証。戻り値 = { source, scope, kind, snapshotDate, missing, partial, nominal, capturedAt, rows }
 * 商品コードは **受け取った文字列のまま** 鍵にする (trim・小文字化しない = 送り手の検証と同じ値。前後の空白・制御文字は拒否)
 */
export function validateStockDayBody(body, { todayJst = jstDate(new Date()), now = Date.now() } = {}) {
  if (!body || typeof body !== 'object' || Array.isArray(body)) throw bad('body は object');
  const source = body.source;
  if (typeof source !== 'string' || !Object.hasOwn(STOCK_SOURCES, source)) throw bad(`source は ${Object.keys(STOCK_SOURCES).join(' / ')}: ${String(source).slice(0, 40)}`);
  const spec = STOCK_SOURCES[source];
  const scope = body.scope ?? spec.scopes[0];
  if (typeof scope !== 'string' || !spec.scopes.includes(scope)) throw bad(`scope は ${spec.scopes.join(' / ')}: ${String(scope).slice(0, 40)}`);
  const snapshotDate = body.snapshot_date;
  if (!isRealDate(snapshotDate)) throw bad(`snapshot_date は実在する YYYY-MM-DD: ${String(snapshotDate).slice(0, 40)}`);
  if (snapshotDate > todayJst) throw bad(`snapshot_date が未来 (JST の今日 = ${todayJst}): ${snapshotDate}`);
  for (const k of ['missing', 'partial', 'captured_at_nominal']) if (body[k] !== undefined && typeof body[k] !== 'boolean') throw bad(`${k} は boolean`);
  const missing = body.missing === true, partial = body.partial === true, nominal = body.captured_at_nominal === true;
  if (partial && spec.kind !== 'fba') throw bad('partial は FBA だけ (NE の在庫数は 1 つの取得で全部そろう)');
  if (missing) {
    if (snapshotDate >= todayJst) throw bad(`今日 (${todayJst}) 以降を missing にはできない (まだ取れるかもしれない): ${snapshotDate}`);
    if (body.rows !== undefined && !(Array.isArray(body.rows) && body.rows.length === 0)) throw bad('missing のときは rows を付けない');
    if (partial) throw bad('missing と partial は同時に言えない');
    return { source, scope, kind: spec.kind, snapshotDate, missing: true, partial: false, nominal: false, capturedAt: null, rows: [] };
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
    let row;
    if (spec.kind === 'fba') {
      try { row = fbaRowOf(r, partial); } catch (e) { throw bad(`rows[${i}]: ${e.message}`); }
    } else {
      if (!r || typeof r !== 'object') throw bad(`rows[${i}] は object`);
      if (!isValidCode(r.code)) throw bad(`rows[${i}].code が不正 (空・前後の空白・制御文字・200 文字超)`);
      if (!isCount(r.qty)) throw bad(`rows[${i}].qty は 0 以上の整数 (int32): ${String(r.qty).slice(0, 40)}`);
      row = { code: r.code, qty: r.qty };
    }
    if (seen.has(row.code)) throw bad(`rows[${i}].code が重複: ${row.code.slice(0, 60)}`);
    seen.add(row.code);
    out.push(row);
  }
  if (spec.kind === 'fba' && !partial && !out.some((x) => x.fba_fc_transfer != null)) throw bad('partial でない日なのに、RESTOCK の 3 区分の入った行が 1 つも無い (RESTOCK が取れていないなら partial: true)');
  return { source, scope, kind: spec.kind, snapshotDate, missing: false, partial, nominal, capturedAt, rows: out };
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
 * @returns {{ status: 'applied'|'same'|'missing'|'missing_same', day_status: 'complete'|'partial'|'missing', upgraded: boolean, source, scope, snapshot_date, rows, resolved, unresolved, run_id, checksum }}
 *   例外: BAD_REQUEST (400) / CONFLICT (409 = 確定済みの日に違う内容) / LOCKED (503)
 */
export async function ingestStockDay(db, body, { host = 'render', companyId = COMPANY_ID, todayJst, now, afterWrite = null, log = () => {} } = {}) {
  const v = validateStockDayBody(body, { ...(todayJst ? { todayJst } : {}), ...(now !== undefined ? { now } : {}) });
  const { source, scope, snapshotDate } = v;
  const spec = STOCK_SOURCES[source];
  const one = async (sql, p) => (await db.query(sql, p)).rows[0];
  const checksum = v.missing ? null : checksumOf(source, v.rows, v.partial);
  const base = { source, scope, snapshot_date: snapshotDate };
  await db.exec('begin');
  try {
    const got = (await one(`select pg_try_advisory_xact_lock(hashtext($1)) as got`, [`company-db:stock-daily:${source}:${scope}`])).got;
    if (!got) throw err('LOCKED', `${source}/${scope} の別の取込が走っている`);
    const day = await one(
      `select d.status, d.ingest_run_id, r.checksum from snapshots.stock_capture_days d left join ops.ingest_runs r on r.ingest_run_id = d.ingest_run_id
        where d.snapshot_date = $1::date and d.source = $2 and d.scope_key = $3 for update of d`, [snapshotDate, source, scope]);
    // 「取れていなかったものが取れた」だけは上げてよい: missing → (complete | partial)、partial → complete
    const upgrade = !!day && !v.missing && (day.status === 'missing' || (day.status === 'partial' && !v.partial));
    if (day && !upgrade && day.status !== 'missing') {
      // 確定済み (complete / partial)・作りかけ (building は 1 取引の中だけなので、ここで見えることは無いはず) の日は書き換えない
      await db.exec('rollback');
      if (v.missing) throw err('CONFLICT', `${snapshotDate} は ${day.status} で確定済み (missing にはできない)`);
      if ((day.status === 'complete' || day.status === 'partial') && day.checksum === checksum && (day.status === 'partial') === v.partial) {
        log(`${snapshotDate}: same (確定済みと同じ内容)`);
        return { status: 'same', day_status: day.status, upgraded: false, ...base, rows: v.rows.length, resolved: null, unresolved: null, run_id: day.ingest_run_id, checksum };
      }
      throw err('CONFLICT', `${snapshotDate} は ${day.status} で確定済みで、内容が違う (先に確定した日は書き換えない。直すなら保守の経路で)`);
    }
    if (v.missing) {
      if (day) { await db.exec('rollback'); return { status: 'missing_same', day_status: 'missing', upgraded: false, ...base, rows: 0, resolved: 0, unresolved: 0, run_id: null, checksum: null }; }
      await db.query(`insert into snapshots.stock_capture_days (snapshot_date, source, scope_key, company_id, status, ingest_run_id) values ($1::date, $2, $3, $4::smallint, 'missing', null)`, [snapshotDate, source, scope, companyId]);
      await db.exec('commit');
      log(`${snapshotDate}: missing (送り手の申告)`);
      return { status: 'missing', day_status: 'missing', upgraded: false, ...base, rows: 0, resolved: 0, unresolved: 0, run_id: null, checksum: null };
    }
    const runId = newStockRunId(source);
    await db.query(
      `insert into ops.ingest_runs (ingest_run_id, source_system, entity, scope_key, host, started_at, status, complete, rows_seen, source_tz, checksum, format_version)
       values ($1, $2, $3, $4, $5, now(), 'running', false, $6, 'UTC', $7, $8)`, [runId, spec.sourceSystem, ENTITY, scope, host, v.rows.length, checksum, v.nominal ? 'v1-nominal-time' : 'v1']);
    if (day && day.status === 'partial') {
      // partial → complete: 前の行を消してから入れ直す。日次の表は building の間だけ変えられる (0011 の trigger) → いったん building に戻す (同じ取引の中 = 外からは見えない)
      await db.query(`update snapshots.stock_capture_days set status = 'building' where snapshot_date = $1::date and source = $2 and scope_key = $3`, [snapshotDate, source, scope]);
      await db.query(`delete from snapshots.sku_stock_daily where snapshot_date = $1::date and source = $2 and scope_key = $3`, [snapshotDate, source, scope]);
    }
    if (day) {
      // missing / partial だった日に行が届いた (後から取れた) → building で、この取込の run に付け替えてから入れる
      await db.query(`update snapshots.stock_capture_days set status = 'building', ingest_run_id = $4, captured_at = $5::timestamptz, rows = $6, built_at = now()
                       where snapshot_date = $1::date and source = $2 and scope_key = $3`, [snapshotDate, source, scope, runId, v.capturedAt, v.rows.length]);
    } else {
      await db.query(`insert into snapshots.stock_capture_days (snapshot_date, source, scope_key, company_id, status, ingest_run_id, captured_at, rows)
                      values ($1::date, $2, $3, $4::smallint, 'building', $5, $6::timestamptz, $7)`, [snapshotDate, source, scope, companyId, runId, v.capturedAt, v.rows.length]);
    }
    let inserted;
    if (spec.kind === 'fba') {
      const col = (c) => v.rows.map((r) => r[c]);
      inserted = affected(await db.query(
        `insert into snapshots.sku_stock_daily (snapshot_date, source, scope_key, source_code, company_id, sku_id, qty,
                fba_available, fba_fc_transfer, fba_fc_processing, fba_customer_order, fba_inbound_working, fba_inbound_shipped, fba_inbound_received, captured_at, ingest_run_id)
         select $1::date, $2, $3, t.code, $4::smallint,
                (select min(lc.sku_id) from core.listing_components lc
                  where lc.company_id = $4::smallint and lc.listing_id = core.resolve_listing_id($4::smallint, $7, t.code)
                 having count(*) = 1 and max(lc.qty) = 1),
                t.qty, t.a, t.x, t.p, t.c, t.w, t.s, t.r, $6::timestamptz, $5
           from unnest($8::text[], $9::int[], $10::int[], $11::int[], $12::int[], $13::int[], $14::int[], $15::int[], $16::int[]) as t(code, qty, a, x, p, c, w, s, r)`,
        [snapshotDate, source, scope, companyId, runId, v.capturedAt, spec.mall, col('code'), col('qty'), col('fba_available'), col('fba_fc_transfer'), col('fba_fc_processing'), col('fba_customer_order'),
          col('fba_inbound_working'), col('fba_inbound_shipped'), col('fba_inbound_received')]));
    } else {
      inserted = affected(await db.query(
        `insert into snapshots.sku_stock_daily (snapshot_date, source, scope_key, source_code, company_id, sku_id, qty, captured_at, ingest_run_id)
         select $1::date, $2, $3, t.code, $4::smallint, k.sku_id, t.qty, $6::timestamptz, $5
           from unnest($7::text[], $8::int[]) as t(code, qty)
           left join core.skus k on k.company_id = $4::smallint and k.code_norm = core.norm_code(t.code)`,
        [snapshotDate, source, scope, companyId, runId, v.capturedAt, v.rows.map((r) => r.code), v.rows.map((r) => r.qty)]));
    }
    if (inserted !== v.rows.length) throw err('ROWS_MISMATCH', `入れた行数 ${inserted} が、届いた行数 ${v.rows.length} と違う (SKU の解決で行が増減した)`);
    const resolved = Number((await one(`select count(*)::int as n from snapshots.sku_stock_daily where snapshot_date = $1::date and source = $2 and scope_key = $3 and sku_id is not null`, [snapshotDate, source, scope])).n);
    const dayStatus = v.partial ? 'partial' : 'complete';
    if (v.partial) await db.query(`update snapshots.stock_capture_days set status = 'partial' where snapshot_date = $1::date and source = $2 and scope_key = $3`, [snapshotDate, source, scope]);
    else await db.query(`update snapshots.stock_capture_days set status = 'complete', completed_at = now() where snapshot_date = $1::date and source = $2 and scope_key = $3`, [snapshotDate, source, scope]);
    // 取込の記録: partial の日は「取得範囲を完走していない」= status 'partial'・complete = false
    await db.query(`update ops.ingest_runs set status = $3, complete = $4, finished_at = now(), rows_inserted = $2, rows_skipped = 0 where ingest_run_id = $1`, [runId, inserted, v.partial ? 'partial' : 'success', !v.partial]);
    if (afterWrite) await afterWrite();   // 試験用: 全部書いた後・commit の前
    await db.exec('commit');
    log(`${snapshotDate}: applied ${dayStatus}${upgrade ? ` (${day.status} から上げた)` : ''} (行 ${inserted} / SKU が分かった ${resolved} / 分からない ${inserted - resolved})`);
    return { status: 'applied', day_status: dayStatus, upgraded: upgrade, ...base, rows: inserted, resolved, unresolved: inserted - resolved, run_id: runId, checksum };
  } catch (e) {
    try { await db.exec('rollback'); } catch { /* 取引が既に無い */ }
    throw e;
  }
}

/** 期間の状態 (送り手が「まだ送っていない日」を決めるのに使う)。戻り値 = [{ snapshot_date, status, rows, checksum }] */
export async function stockDayStatus(db, { source, scope, from, to }) {
  if (typeof source !== 'string' || !Object.hasOwn(STOCK_SOURCES, source)) throw bad('source が不正');
  const sc = scope === undefined || scope === '' ? STOCK_SOURCES[source].scopes[0] : scope;
  if (!STOCK_SOURCES[source].scopes.includes(sc)) throw bad('scope が不正');
  if (!isRealDate(from) || !isRealDate(to) || from > to) throw bad('from / to は YYYY-MM-DD で from <= to');
  if ((Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) / 86400000 > 800) throw bad('範囲は 800 日まで');
  const r = await db.query(
    `select d.snapshot_date::text as snapshot_date, d.status, d.rows, r.checksum
       from snapshots.stock_capture_days d left join ops.ingest_runs r on r.ingest_run_id = d.ingest_run_id
      where d.source = $1 and d.scope_key = $2 and d.snapshot_date between $3::date and $4::date order by d.snapshot_date`, [source, sc, from, to]);
  return r.rows.map((x) => ({ snapshot_date: x.snapshot_date, status: x.status, rows: x.rows == null ? null : Number(x.rows), checksum: x.checksum || null }));
}
