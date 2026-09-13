/**
 * logizard.mjs — ロジザード在庫の「毎時の写し」と「日ごとの締め」(Company DB構想 08 §3。D2)
 *
 * 3 段 (08 §3.2):
 *   ① 毎時: mirror_logizard_stock (Render の warehouse-mirror.db。毎時 09〜18 時に miniPC が全置換で送る) を読み、
 *      前の世代と比べて **変わった行だけ** を raw.logizard_inventory_observations に書く (新規・変化 = ok、消えた = not_found)。
 *      同じ世代 (captured_at が同じ) なら `skipped` の run を残して終わる。
 *   ② 日が変わったら: 前日までの未締めの日について、その日の最後に完走した取得の状態から
 *      snapshots.warehouse_stock_daily (sku × ロケ) と sku_stock_daily (sku) を作り、stock_capture_days を building → complete に上げる
 *      (1 日 = 1 トランザクション。取得が 1 回も無い日は missing)。
 *   ③ 締めが追いついている (未締めの日が残っていない) ときだけ、raw の整理 (raw.purge_superseded_observations 30 日 = D-25) と
 *      DB の大きさの記録 (ops.job_runs)。🚨 未締めの日が残る間は整理しない (締めの復元材料 = 古い観測を消してしまう。Codex R1 #2)
 *
 * 約束:
 *   - 世代 (取得時刻) = mirror の captured_at (miniPC が CSV を取った時刻)。observed_at にもこれを使う (在庫日ではない。R3 #5)。
 *     ops.ingest_runs.checksum に世代を ISO で残す (「前回と同じ世代か」「その日の最後の世代はどれか」「締めの走査の起点」の根拠)
 *   - 比較元 = 直前までの **完走した run の状態観測 (ok / not_found)** だけ (失敗した run・error / skipped は根拠にしない。R3 #1 = view と同じ)
 *   - 同時実行は advisory lock (取引ロック) で直列化し、**ロックを取ってから世代を判定する** (ロック前に読んだ世代で判定すると、
 *     待っている間に完走した世代を踏み越えて混ざる。Codex R1 #1)。表のロック順は contents → observations (整理と同じ順。R3 #10)
 *   - run は「running を先に commit → 本体を 1 取引 → success」。途中で落ちたら run を failed にする (比較元にならない)
 *   - 🚨 rows が空・business_key が重複 → その run は failed (黙って合算・全消ししない)
 *   - core.locations は「今回変わった行のブロック × ロケ」を core.ensure_location で足す (別会社の code なら例外 = run failed)
 *   - 日次の表は building の間だけ書ける (0011 の trigger)。締めは capture 行を作る → 行を入れる → complete の順で同じ取引
 *   - 有効期限・入荷日は **実在する日付だけ** date にし、読めない値 (2027/13/01 など) は null にして件数を数える (1 行の不正で日の締めを止めない。R1 #3)
 *   - 品質区分 (良品 / Ｂ品) は分けずに合算する (raw の事実のまま。分けるなら D-38 として決める)
 *
 * ここは Postgres と「行の配列」だけを見る (SQLite は readMirrorLogizardStock だけ)。試験は PGlite で流す (scripts/test-company-db-inventory.mjs)
 */
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import canonicalize from 'canonicalize';
import { jstDateStr } from '../../../lib/jst-date.js';

export const SOURCE = 'logizard';
export const ENTITY = 'inventory';
export const SCOPE = 'main';            // 取得範囲 (倉庫 1 つ)。2 つ目の倉庫・会社ができたら値を足す
export const COMPANY_ID = 1;
export const WAREHOUSE_ID = 1;          // 0008: MAIN (本館 + いろは棟)
export const RAW_SRC = 'logizard_inventory';
export const RETENTION_DAYS = 30;       // D-25
export const JOB_ID = 'company-db-inventory-hourly';
const LOCK_KEY = 'company-db-inventory-hourly';
/** 0011 の許可リストの部分集合。在庫日・captured_at・synced_at は入れない (取込のたびに変わる = volatile) */
export const PAYLOAD_KEYS = ['商品ID', '商品名', 'バーコード', 'ブロック略称', 'ロケ', '品質区分名', '有効期限', '入荷日', '在庫数', '引当数', 'ロケ業務区分', '最終入荷日', '最終出荷日'];
/** business_key の列 (08 §3.2。ブロック略称を含めないと 36 行が衝突する) */
export const KEY_COLS = ['商品ID', 'ブロック略称', 'ロケ', '品質区分名', '有効期限', '入荷日'];
const NUM_KEYS = new Set(['在庫数', '引当数']);

const s = (v) => (v == null ? null : String(v).trim() || null);
export const businessKey = (row) => KEY_COLS.map((k) => s(row[k]) ?? '-').join('|');
export function payloadOf(row) {
  const p = {};
  for (const k of PAYLOAD_KEYS) {
    const v = row[k];
    if (v == null || v === '') continue;
    p[k] = NUM_KEYS.has(k) ? Number(v) : String(v).trim();
  }
  return p;
}
export const contentHashOf = (payload) => crypto.createHash('sha256').update(canonicalize(payload)).digest('hex');
export const locationCode = (block, loke) => `${s(block) ?? '-'}-${s(loke) ?? '-'}`;
export function newRunId(prefix = 'inv') {
  return `${prefix}_${new Date().toISOString().replace(/[-:.TZ]/g, '').slice(0, 15)}_${crypto.randomBytes(3).toString('hex')}`;
}
/**
 * ロジザードの日付文字列 ('2027/03/31' / '2027-3-1' / 先頭に日付があればその部分) → 'YYYY-MM-DD'。実在しない日 (13 月・2/30) や読めない値は null
 */
export function safeDate(v) {
  const t = s(v); if (!t) return null;
  const m = /^(\d{4})[-/](\d{1,2})[-/](\d{1,2})/.exec(t); if (!m) return null;
  const y = Number(m[1]), mo = Number(m[2]), d = Number(m[3]);
  const dt = new Date(Date.UTC(y, mo - 1, d));
  if (dt.getUTCFullYear() !== y || dt.getUTCMonth() !== mo - 1 || dt.getUTCDate() !== d) return null;   // 繰り上がった = 実在しない
  return `${y}-${String(mo).padStart(2, '0')}-${String(d).padStart(2, '0')}`;
}
const isoOf = (v) => { const d = new Date(v); if (!Number.isFinite(d.getTime())) throw new Error(`captured_at が日時として不正: ${v}`); return d.toISOString(); };
const rollbackQuiet = async (db) => { try { await db.exec('rollback'); } catch { /* 取引の外なら何もしない */ } };
const affected = (r) => r.rowCount ?? r.affectedRows ?? 0;

/**
 * Render の warehouse-mirror.db から mirror_logizard_stock を読む (読み取り専用)。表が無い・空なら null
 * @returns {{ capturedAt: string, rows: object[] } | null}
 */
export async function readMirrorLogizardStock(dataDir) {
  const file = path.join(dataDir, 'warehouse-mirror.db');
  if (!fs.existsSync(file)) return null;
  const { default: Database } = await import('better-sqlite3');
  const db = new Database(file, { readonly: true, fileMustExist: true });
  try {
    if (!db.prepare("select 1 from sqlite_master where type = 'table' and name = 'mirror_logizard_stock'").get()) return null;
    const rows = db.prepare(`select ${PAYLOAD_KEYS.map((k) => `"${k}"`).join(', ')}, captured_at from mirror_logizard_stock`).all();
    if (!rows.length) return null;
    const gens = new Set(rows.map((r) => r.captured_at));
    if (gens.size !== 1) throw new Error(`mirror_logizard_stock に複数の世代が混ざっている (${[...gens].slice(0, 3).join(', ')})`);
    return { capturedAt: isoOf(rows[0].captured_at), rows };
  } finally { db.close(); }
}

/** 直前の完走した run (世代つき) */
const lastSuccess = (db, scope) => db.query(
  `select ingest_run_id, checksum from ops.ingest_runs where source_system = $1 and entity = $2 and scope_key = $3 and status = 'success' and complete and checksum is not null order by checksum desc limit 1`,
  [SOURCE, ENTITY, scope]).then((r) => r.rows[0]);

/**
 * ① 毎時の写し。戻り値 = { status: 'success'|'skipped', runId, generation, seen, added, changed, removed, contentsNew, locationsAdded, reason }
 * 失敗は例外 (run は failed にしてから投げる)。db = { query, exec } (pgAdapter / pgliteAdapter)
 */
export async function captureLogizardInventory(db, { rows, capturedAt, host = 'render', runId = newRunId('inv'), companyId = COMPANY_ID, warehouseId = WAREHOUSE_ID, scope = SCOPE, log = () => {} } = {}) {
  const generation = isoOf(capturedAt);
  if (!Array.isArray(rows)) throw new Error('rows が配列ではない');
  const one = async (sql, p) => (await db.query(sql, p)).rows[0];
  const skippedOut = (reason) => ({ status: 'skipped', runId, generation, seen: rows.length, added: 0, changed: 0, removed: 0, contentsNew: 0, locationsAdded: 0, reason });

  // running を先に残す (途中で落ちても「始めた」が分かる)。本体は 1 取引
  await db.query(`insert into ops.ingest_runs (ingest_run_id, source_system, entity, scope_key, host, started_at, status, complete, rows_seen, source_tz, checksum, format_version)
                  values ($1, $2, $3, $4, $5, now(), 'running', false, $6, 'UTC', $7, 'v1')`, [runId, SOURCE, ENTITY, scope, host, rows.length, generation]);
  const markSkipped = async (reason) => {
    await rollbackQuiet(db);
    await db.query(`update ops.ingest_runs set status = 'skipped', complete = false, finished_at = now(), rows_inserted = 0, error = $2 where ingest_run_id = $1`, [runId, reason]);
    log(`skipped: ${reason}`);
    return skippedOut(reason);
  };
  try {
    await db.exec('begin');
    // 同時実行の直列化。別の取込が走っていればこの回は見送る (失敗ではない)
    const got = (await one(`select pg_try_advisory_xact_lock(hashtext($1)) as got`, [LOCK_KEY])).got;
    if (!got) return await markSkipped('別の取込が走っている (advisory lock)');
    // 表のロック順 = contents → observations (整理 raw.purge_superseded_observations と同じ順)
    await db.exec(`lock table raw.${RAW_SRC}_contents, raw.${RAW_SRC}_observations in row exclusive mode`);
    // 🚨 世代の判定はロックを取ってから (待っている間に完走した世代を踏み越えない。R1 #1)。同じ・古い世代は skipped
    const last = await lastSuccess(db, scope);
    if (last && generation <= last.checksum) {
      return await markSkipped(generation === last.checksum ? `同じ世代 (${generation}) = 取り込み済み` : `古い世代 (${generation} < ${last.checksum})`);
    }
    if (!rows.length) throw Object.assign(new Error('rows が空 (全消しは受け付けない。mirror も空を拒む)'), { code: 'EMPTY' });

    // いまの状態 (鍵の重複は失敗 = 黙って合算しない)
    const cur = new Map(); const dups = [];
    for (const row of rows) {
      const key = businessKey(row);
      if (cur.has(key)) { dups.push(key); continue; }
      const payload = payloadOf(row);
      if (!payload['商品ID']) throw Object.assign(new Error(`商品ID が空の行がある (key=${key})`), { code: 'BAD_ROW' });
      cur.set(key, { payload, hash: contentHashOf(payload), row });
    }
    if (dups.length) throw Object.assign(new Error(`business_key が重複している (${dups.length} 件。例: ${dups[0]})`), { code: 'DUPLICATE_KEY' });

    // 比較元 = 直前までの完走した run の状態観測の最新 (view と同じ根拠)
    const prev = new Map();
    for (const r of (await db.query(
      `select distinct on (o.business_key) o.business_key, o.fetch_status, o.content_hash
         from raw.${RAW_SRC}_observations o join ops.ingest_runs r on r.ingest_run_id = o.ingest_run_id
        where o.scope_key = $1 and r.status = 'success' and r.complete and o.fetch_status in ('ok', 'not_found')
        order by o.business_key, o.observed_at desc, o.observation_id desc`, [scope])).rows) prev.set(r.business_key, r);

    // 差分
    const obsKeys = [], obsHashes = [], obsStatus = []; const newContents = new Map();
    let added = 0, changed = 0, removed = 0;
    for (const [key, c] of cur) {
      const p = prev.get(key);
      if (p && p.fetch_status === 'ok' && p.content_hash === c.hash) continue;   // 変わっていない
      if (p && p.fetch_status === 'ok') changed++; else added++;
      obsKeys.push(key); obsHashes.push(c.hash); obsStatus.push('ok');
      newContents.set(c.hash, c);
    }
    for (const [key, p] of prev) {
      if (p.fetch_status === 'ok' && !cur.has(key)) { removed++; obsKeys.push(key); obsHashes.push(null); obsStatus.push('not_found'); }
    }

    // contents (中身の重複排除。同じ内容が既にあれば何もしない) → observations (append-only)
    let contentsNew = 0;
    if (newContents.size) {
      const hs = [...newContents.keys()], ps = hs.map((h) => JSON.stringify(newContents.get(h).payload));
      contentsNew = affected(await db.query(
        `insert into raw.${RAW_SRC}_contents (content_hash, payload, payload_bytes)
           select h, p::jsonb, octet_length(p) from unnest($1::text[], $2::text[]) as t(h, p)
           on conflict (content_hash) do nothing`, [hs, ps]));
    }
    if (obsKeys.length) {
      await db.query(
        `insert into raw.${RAW_SRC}_observations (ingest_run_id, scope_key, business_key, content_hash, fetch_status, observed_at)
           select $1, $2, k, h, st, $3::timestamptz from unnest($4::text[], $5::text[], $6::text[]) as t(k, h, st)`,
        [runId, scope, generation, obsKeys, obsHashes, obsStatus]);
    }

    // ロケーションの自動追加 (今回変わった行の ブロック × ロケ)。別会社の code なら例外 → run failed
    let locationsAdded = 0;
    const locs = new Map();
    for (const c of newContents.values()) { const b = s(c.row['ブロック略称']), l = s(c.row['ロケ']); locs.set(locationCode(b, l), [b, l]); }
    if (locs.size) {
      const before = Number((await one(`select count(*) as n from core.locations where warehouse_id = $1`, [warehouseId])).n);
      const bs = [...locs.values()].map((x) => x[0]), ls = [...locs.values()].map((x) => x[1]);
      await db.query(`select core.ensure_location($1::smallint, $2::smallint, b, l) from unnest($3::text[], $4::text[]) as t(b, l)`, [companyId, warehouseId, bs, ls]);
      locationsAdded = Number((await one(`select count(*) as n from core.locations where warehouse_id = $1`, [warehouseId])).n) - before;
    }

    await db.query(`update ops.ingest_runs set status = 'success', complete = true, finished_at = now(), rows_inserted = $2, rows_skipped = $3 where ingest_run_id = $1`,
      [runId, obsKeys.length, rows.length - added - changed]);
    await db.exec('commit');
    const out = { status: 'success', runId, generation, seen: rows.length, added, changed, removed, contentsNew, locationsAdded };
    log(`success: 世代 ${generation} 行 ${rows.length} (+${added} ~${changed} -${removed}, 中身 +${contentsNew}, ロケ +${locationsAdded})`);
    return out;
  } catch (e) {
    await rollbackQuiet(db);
    try { await db.query(`update ops.ingest_runs set status = 'failed', complete = false, finished_at = now(), error = $2 where ingest_run_id = $1`, [runId, String(e.message || e).slice(0, 500)]); }
    catch (e2) { log(`run を failed にできなかった: ${e2.message}`); }
    throw e;
  }
}

/** 'YYYY-MM-DD' の翌日 (Date を経由しない = TZ 事故なし) */
export function nextDay(d) {
  const [y, m, day] = d.split('-').map(Number);
  const t = new Date(Date.UTC(y, m - 1, day + 1));
  return `${t.getUTCFullYear()}-${String(t.getUTCMonth() + 1).padStart(2, '0')}-${String(t.getUTCDate()).padStart(2, '0')}`;
}

/**
 * ② 1 日を締める (1 取引)。その日 (JST) の最後に完走した取得の状態 → 日次 2 表 → complete。取得が無い日は missing
 * 戻り値 = { day, status: 'complete'|'missing'|'exists', runId, generation, lines, skus, badDates }
 */
export async function closeStockDay(db, day, { companyId = COMPANY_ID, warehouseId = WAREHOUSE_ID, scope = SCOPE, log = () => {} } = {}) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(day)) throw new Error(`day は 'YYYY-MM-DD': ${day}`);
  const one = async (sql, p) => (await db.query(sql, p)).rows[0];
  const exists = await one(`select status from snapshots.stock_capture_days where snapshot_date = $1 and source = $2 and scope_key = $3`, [day, SOURCE, scope]);
  if (exists) return { day, status: 'exists', current: exists.status };
  // その日 (JST) の世代 = [day 00:00, 翌日 00:00) JST に取った完走 run のうち最後
  const from = `${day}T00:00:00+09:00`, to = `${nextDay(day)}T00:00:00+09:00`;
  const run = await one(
    `select ingest_run_id, checksum as generation from ops.ingest_runs
      where source_system = $1 and entity = $2 and scope_key = $3 and status = 'success' and complete
        and checksum::timestamptz >= $4::timestamptz and checksum::timestamptz < $5::timestamptz
      order by checksum desc limit 1`, [SOURCE, ENTITY, scope, from, to]);
  await db.exec('begin');
  try {
    if (!run) {
      await db.query(`insert into snapshots.stock_capture_days (snapshot_date, source, scope_key, company_id, status, ingest_run_id) values ($1, $2, $3, $4, 'missing', null)`, [day, SOURCE, scope, companyId]);
      await db.exec('commit');
      log(`${day}: missing (完走した取得が無い)`);
      return { day, status: 'missing', runId: null, generation: null, lines: 0, skus: 0, badDates: 0 };
    }
    await db.query(`insert into snapshots.stock_capture_days (snapshot_date, source, scope_key, company_id, status, ingest_run_id) values ($1, $2, $3, $4, 'building', $5)`, [day, SOURCE, scope, companyId, run.ingest_run_id]);
    // その世代までの最新の状態観測 (完走 run・ok / not_found) のうち ok の行 = その時点の在庫。日付の列はここ (JS) で実在まで検証する
    const state = (await db.query(
      `with st as (
         select distinct on (o.business_key) o.business_key, o.fetch_status, o.content_hash
           from raw.${RAW_SRC}_observations o join ops.ingest_runs r on r.ingest_run_id = o.ingest_run_id
          where o.scope_key = $1 and r.status = 'success' and r.complete and o.fetch_status in ('ok', 'not_found') and o.observed_at <= $2::timestamptz
          order by o.business_key, o.observed_at desc, o.observation_id desc)
       select st.business_key, c.payload from st join raw.${RAW_SRC}_contents c on c.content_hash = st.content_hash where st.fetch_status = 'ok'`,
      [scope, run.generation])).rows;
    const col = { key: [], code: [], loc: [], block: [], quality: [], expiry: [], received: [], qty: [], alloc: [] };
    let badDates = 0;
    for (const r of state) {
      const p = typeof r.payload === 'string' ? JSON.parse(r.payload) : r.payload;
      const expiry = safeDate(p['有効期限']), received = safeDate(p['入荷日']);
      if (s(p['有効期限']) && !expiry) badDates++;
      if (s(p['入荷日']) && !received) badDates++;
      col.key.push(r.business_key); col.code.push(String(p['商品ID'])); col.loc.push(locationCode(p['ブロック略称'], p['ロケ']));
      col.block.push(s(p['ブロック略称'])); col.quality.push(s(p['品質区分名'])); col.expiry.push(expiry); col.received.push(received);
      col.qty.push(Number(p['在庫数'])); col.alloc.push(Number(p['引当数'] ?? 0));
    }
    let lines = 0, skus = 0;
    if (col.key.length) {
      lines = affected(await db.query(
        `insert into snapshots.warehouse_stock_daily (snapshot_date, source, scope_key, line_key, company_id, sku_id, logizard_code, location_id, location_code, block_code, quality, expiry_date, received_date, qty, allocated_qty, captured_at, ingest_run_id)
         select $1::date, $2, $3, t.k, $4::smallint, k.sku_id, t.code, l.location_id, t.loc, t.block, t.quality, t.expiry, t.received, t.qty, t.alloc, $6::timestamptz, $5
           from unnest($8::text[], $9::text[], $10::text[], $11::text[], $12::text[], $13::date[], $14::date[], $15::int[], $16::int[]) as t(k, code, loc, block, quality, expiry, received, qty, alloc)
           left join core.skus k on k.company_id = $4::smallint and k.code_norm = core.norm_code(t.code)
           left join core.locations l on l.warehouse_id = $7::smallint and l.code = t.loc`,
        [day, SOURCE, scope, companyId, run.ingest_run_id, run.generation, warehouseId, col.key, col.code, col.loc, col.block, col.quality, col.expiry, col.received, col.qty, col.alloc]));
      skus = affected(await db.query(
        `insert into snapshots.sku_stock_daily (snapshot_date, source, scope_key, source_code, company_id, sku_id, qty, allocated_qty, captured_at, ingest_run_id)
         select snapshot_date, source, scope_key, logizard_code, company_id, min(sku_id), sum(qty), sum(allocated_qty), max(captured_at), ingest_run_id
           from snapshots.warehouse_stock_daily where snapshot_date = $1 and source = $2 and scope_key = $3
          group by snapshot_date, source, scope_key, logizard_code, company_id, ingest_run_id`, [day, SOURCE, scope]));
    }
    await db.query(`update snapshots.stock_capture_days set status = 'complete', completed_at = now() where snapshot_date = $1 and source = $2 and scope_key = $3`, [day, SOURCE, scope]);
    await db.exec('commit');
    const out = { day, status: 'complete', runId: run.ingest_run_id, generation: run.generation, lines, skus, badDates };
    log(`${day}: complete (世代 ${run.generation}, ロケ行 ${lines}, SKU ${skus}${badDates ? `, 読めない日付 ${badDates}` : ''})`);
    return out;
  } catch (e) {
    await rollbackQuiet(db);
    throw Object.assign(new Error(`${day} の締めに失敗: ${e.message}`), { code: e.code || 'CLOSE_FAILED', day });
  }
}

/**
 * ② 未締めの日を全部締める (最初の完走世代の日 〜 昨日。今日は締めない)。
 * 戻り値 = { closed: [closeStockDay の戻り値...], firstDay, upTo, backlog }  backlog = maxDays で打ち切り、まだ未締めの日が残っている
 * 走査の起点は ops.ingest_runs の最初の完走世代 (raw の観測は整理で消えるので根拠にしない。R1 #2)
 * @param todayJst 'YYYY-MM-DD' (JST の今日。試験で差し替える)
 */
export async function closeStockDays(db, { todayJst = jstDateStr(new Date()), maxDays = 60, ...rest } = {}) {
  const one = async (sql, p) => (await db.query(sql, p)).rows[0];
  const scope = rest.scope || SCOPE;
  const first = (await one(`select min(checksum) as g from ops.ingest_runs where source_system = $1 and entity = $2 and scope_key = $3 and status = 'success' and complete and checksum is not null`, [SOURCE, ENTITY, scope]))?.g;
  if (!first) return { closed: [], firstDay: null, upTo: todayJst, backlog: false };
  const firstDay = jstDateStr(new Date(first));
  const done = new Set((await db.query(`select snapshot_date::text as d from snapshots.stock_capture_days where source = $1 and scope_key = $2`, [SOURCE, scope])).rows.map((r) => r.d));
  const closed = []; let backlog = false;
  for (let d = firstDay; d < todayJst; d = nextDay(d)) {
    if (done.has(d)) continue;
    if (closed.length >= maxDays) { backlog = true; break; }
    closed.push(await closeStockDay(db, d, rest));
  }
  return { closed, firstDay, upTo: todayJst, backlog };
}

/**
 * ③ 整理と記録: raw の観測を 30 日で整理 (鍵ごとの最新の有効な状態は残る) + DB の大きさを ops.job_runs に残す
 * 🚨 呼ぶのは締めが追いついているとき (closeStockDays の backlog = false) だけ。戻り値 = { purged, dbBytes }
 */
export async function maintainInventory(db, { host = 'render', keepDays = RETENTION_DAYS, jobId = JOB_ID, note = '' } = {}) {
  const startedAt = new Date().toISOString();
  const purged = Number((await db.query(`select raw.purge_superseded_observations($1, $2::integer) as n`, [RAW_SRC, keepDays])).rows[0].n);
  const dbBytes = Number((await db.query(`select pg_database_size(current_database()) as b`)).rows[0].b);
  const summary = JSON.stringify({ step: 'maintain', purged_observations: purged, keep_days: keepDays, db_bytes: dbBytes, db_mb: Math.round(dbBytes / 1048576), note });
  await db.query(`insert into ops.job_runs (job_id, host, started_at, finished_at, status, summary) values ($1, $2, $3::timestamptz, now(), 'ok', $4)`, [jobId, host, startedAt, summary]);
  return { purged, dbBytes };
}
