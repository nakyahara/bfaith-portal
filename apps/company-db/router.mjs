/**
 * router.mjs — Company DB の同期・管理 API (x-sync-key 認証。expected-profit の publish-api と同じ流儀)
 *
 *   POST /apps/company-db/sync/load            dry-run を開始 (全部やって巻き戻す)。202 + run_id を即返す
 *   POST /apps/company-db/sync/load?apply=1    本適用を開始。202 + run_id
 *   GET  /apps/company-db/sync/status          実行中 (current) / 終わった直近 (last) / latest.json / 途中で死んだ記録 (interrupted) / Postgres の件数
 *   GET  /apps/company-db/sync/report/:run_id  その回の report (load-<run_id>.json。conflicts / unresolved / sections の明細)。?format=md で Markdown
 *   GET  /apps/company-db/sync/reports         report の一覧 (run_id・ok・dry_run・finished_at)
 *
 * 🚨 Render 上で動かす前提 (読み込み元の SQLite が Render の DATA_DIR にある)。miniPC で叩いても mirror が無いので 409。
 * 🚨 同時に 2 本走らせない (単一飛行)。1 回 数十秒〜数分なので、HTTP は待たずに 202 を返し、結果は /status で見る
 *    (Render の HTTP は 100 秒程度で切れる。切れても処理は続くが、結果が受け取れないので 202 方式にする。Codex PR-B R1 M13)
 * 🚨 プロセスが途中で再起動しても「始めたのに結果が無い」は running.json (ディスク) から分かり、本適用が commit 済みかは
 *    ops.ingest_runs で確かめる (/status の interrupted。Codex R2 M4)
 * 🚨 認証はヘッダ x-sync-key だけ。クエリ ?sync_key= は受けない (URL はログに残る。Codex M15)
 */
import express from 'express';
import fs from 'node:fs';
import path from 'node:path';
import { runLoadOnce, readRunning, reportDir } from './load/run-initial-load.mjs';
import { newLoadRunId } from './load/engine.mjs';
import { openPgClient, pgAdapter } from '../../scripts/company-db/migrate.mjs';
import { ingestStockDay, stockDayStatus } from './ingest/stock-daily.mjs';
import { ingestShipmentChunk, validateChunk } from './ingest/shipments.mjs';
import { ingestOrderChunk, validateChunk as validateOrderChunk, MALLS } from './ingest/orders.mjs';

const router = express.Router();

/** Postgres の接続の作り方 (試験は PGlite に差し替えて本物の router を HTTP 越しに通す = Codex D5b-1 R1 #7。本番では触らない) */
let pgClientFactory = openPgClient;
export function __setPgClientFactory(fn) { pgClientFactory = fn || openPgClient; }

/**
 * 伝票 (NE) の push の受け口 (D5a。送り手 = apps/company-db/push/ne-shipments.mjs、本体 = ingest/shipments.mjs):
 *   POST /apps/company-db/sync/shipments             1 chunk (≤1000 伝票) を 1 取引で core.apply_shipment_batch() に通す → { applied, same, stale, failed[], stale_slips[], run_id, chunk_index, replay, finished }
 *                                                     再送 (同じ run_id + chunk_index + 同じ内容) は保存した応答を返す。409 = run / chunk の食い違い、503 CHUNK_DEADLINE = 期限超過 (送り手が割って送り直す)
 *   GET  /apps/company-db/sync/shipments/daily?from&to   mart.v_shipments_daily (旧 f_shipments_daily と同じ式) を返す = miniPC 側の突合 (--reconcile) の材料
 *   GET  /apps/company-db/sync/shipments/status      伝票・明細の件数、世代、結ばれていない伝票の理由別件数、直近の run
 *   GET  /apps/company-db/sync/shipments/receipt?run_id&chunk_index   その chunk の受領記録があるか (送り手が Render の復元・作り直しを見つける)
 *   GET  /apps/company-db/sync/shipments/slips?after&limit   投入済みの伝票番号 (送り手が台帳を作り直すとき)
 * 🚨 body の parse は鍵の検査の後 (server.js の共通 parser はこの path を素通りさせる = 未認可の 12MB を読まない。mirror と同じ流儀)
 */
const shipmentsJson = express.json({ limit: '12mb', inflate: false });
function shipmentsParserError(err, req, res, next) {
  if (!err) return next();
  if (err.type === 'entity.too.large') return res.status(413).json({ error: 'payload too large (12MB)' });
  if (err.type === 'encoding.unsupported') return res.status(415).json({ error: 'compressed body is not accepted' });
  if (err.type === 'entity.parse.failed') return res.status(400).json({ error: 'invalid JSON' });
  if (err.type === 'request.aborted') return res.status(400).json({ error: 'request aborted' });
  return next(err);
}

export function requireSyncKey(req, res, next) {
  const key = process.env.MIRROR_SYNC_KEY;
  if (!key) return res.status(503).json({ error: 'MIRROR_SYNC_KEY not configured' });
  const provided = req.headers['x-sync-key'];
  if (typeof provided !== 'string' || provided !== key) return res.status(401).json({ error: 'invalid_sync_key' });
  next();
}

/** 実行中 / 直近の 1 本の状態 (プロセス内。再起動で消えるが running.json / latest.json はディスクに残る) */
const state = { current: null, last: null };
export const getLoadState = () => ({ current: state.current, last: state.last });

/**
 * 開始 (単一飛行のガードはここ)。戻り値 = { started, current, done }
 *   done = 終わったときの current で解決する Promise (HTTP は使わない。夜間の再ロードが結果を待つのに使う)
 */
export function startLoad({ dataDir, url, apply, host = 'render', log = (m) => console.log(m) }) {
  if (state.current) return { started: false, current: state.current, done: state.current._done || Promise.resolve(state.current) };
  const runId = newLoadRunId();
  const cur = { run_id: runId, dry_run: !apply, status: 'running', started_at: new Date().toISOString(), finished_at: null, summary: null, conflicts: null, unresolved: null, error: null, error_code: null };
  state.current = cur;
  let settle = null;
  // 🚨 待つ人がいなくても reject にしない (待たない呼び出し = HTTP のほうが多い)。終わった姿を resolve で返す。
  //    列挙されない形で持つ (current / last はそのまま JSON にして /status に出すので、混ぜない)
  Object.defineProperty(cur, '_done', { value: new Promise((resolve) => { settle = resolve; }), enumerable: false, writable: false });
  const done = (patch) => {
    Object.assign(cur, patch, { finished_at: new Date().toISOString() });
    state.last = cur; state.current = null;
    settle(cur);
  };
  // 202 を先に返してから始める (SQLite の読み取りも応答の後)
  setImmediate(() => {
    runLoadOnce({ dataDir, url, apply, log, host, runId })
      .then((report) => done({ status: report.ok ? 'done' : 'failed', summary: report.summary || null, conflicts: (report.conflicts || []).length, unresolved: Object.fromEntries(Object.entries(report.unresolved || {}).map(([k, v]) => [k, v.length])), sources: report.plan_sources || null }))
      .catch((e) => { log(`[company-db load] FAILED ${runId}: ${e.message}`); done({ status: 'failed', error: String(e.message), error_code: e.code || null, summary: e.report?.summary || null }); });
  });
  return { started: true, current: cur, done: cur._done };
}

/** running.json があり、それが今の current でなければ「途中で死んだ」記録 */
function interruptedRecord(dataDir) {
  const r = readRunning(reportDir(dataDir), { strict: true });   // 壊れた running.json は「記録なし」にしない (interrupted_error に出る)
  if (!r) return null;
  if (state.current && state.current.run_id === r.run_id) return null;
  return r;
}

router.post('/load', requireSyncKey, (req, res) => {
  const dataDir = process.env.DATA_DIR;
  const url = process.env.COMPANY_DB_URL;
  if (!dataDir || !url) return res.status(503).json({ error: 'DATA_DIR / COMPANY_DB_URL not configured' });
  if (!fs.existsSync(path.join(dataDir, 'warehouse-mirror.db'))) return res.status(409).json({ error: 'warehouse-mirror.db not found (run on Render)' });
  const apply = String(req.query.apply || '') === '1';
  let interrupted = null;
  try { interrupted = interruptedRecord(dataDir); } catch (e) { interrupted = { error: e.message }; }
  const r = startLoad({ dataDir, url, apply });
  if (!r.started) return res.status(409).json({ error: 'load already running', run_id: r.current.run_id, started_at: r.current.started_at });
  res.status(202).json({ accepted: true, run_id: r.current.run_id, dry_run: r.current.dry_run, started_at: r.current.started_at, status_url: '/apps/company-db/sync/status', previous_interrupted: interrupted });
});

router.post('/shipments', requireSyncKey, shipmentsJson, shipmentsParserError, async (req, res) => {
  const url = process.env.COMPANY_DB_URL;
  if (!url) return res.status(503).json({ error: 'COMPANY_DB_URL not configured' });
  let chunk;
  try { chunk = validateChunk(req.body); }
  catch (e) { return res.status(400).json({ error: e.message }); }
  let client;
  const t0 = Date.now();
  try {
    client = await pgClientFactory(url);
    // 1 chunk の中で待ち続けない (Render の HTTP は 100 秒程度で切れる。Codex PR #1336 R1 #5): 文・ロック・取引内の空きに上限。全体の期限は ingest 側 (80 秒)
    await client.query(`set statement_timeout = '20s'; set lock_timeout = '10s'; set idle_in_transaction_session_timeout = '60s'`);
    const r = await ingestShipmentChunk(pgAdapter(client), { ...chunk, host: 'render', log: (m) => console.log(`[company-db shipments] ${chunk.runId} ${m}`) });
    res.json(r);
  } catch (e) {
    const status = e.code === 'CHUNK_DEADLINE' ? 503 : (e.code === 'RUN_MISMATCH' || e.code === 'CHUNK_MISMATCH' || e.code === 'RUN_CLOSED') ? 409 : 500;
    console.error(`[company-db shipments] ${chunk.runId} chunk ${chunk.chunkIndex} FAILED (${status}, ${Date.now() - t0} ms): ${e.message}`);
    res.status(status).json({ error: String(e.message).slice(0, 300), code: e.code || null, run_id: chunk.runId, chunk_index: chunk.chunkIndex });
  } finally { if (client) { try { await client.end(); } catch { /* 閉じられなくても応答は出す */ } } }
});

const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
router.get('/shipments/daily', requireSyncKey, async (req, res) => {
  const url = process.env.COMPANY_DB_URL;
  if (!url) return res.status(503).json({ error: 'COMPANY_DB_URL not configured' });
  const from = String(req.query.from || ''), to = String(req.query.to || '');
  if (!DATE_RE.test(from) || !DATE_RE.test(to) || from > to) return res.status(400).json({ error: 'from / to must be YYYY-MM-DD and from <= to' });
  if ((Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) / 86400000 > 400) return res.status(400).json({ error: 'range must be <= 400 days' });
  let client;
  try {
    client = await pgClientFactory(url);
    const rows = (await client.query(
      `select ship_date::text as ship_date, shop_code, delivery_id, delivery_name, slips, cancelled_slips
         from mart.v_shipments_daily where company_id = 1 and ship_date between $1::date and $2::date order by ship_date, shop_code, delivery_id`, [from, to])).rows;
    res.json({ from, to, rows });
  } catch (e) { res.status(500).json({ error: String(e.message).slice(0, 300) }); }
  finally { if (client) { try { await client.end(); } catch { /* */ } } }
});

/**
 * 注文 (モール) の push の受け口 (D5b。送り手 = apps/company-db/push/mall-orders.mjs、本体 = ingest/orders.mjs):
 *   POST /apps/company-db/sync/orders                    1 chunk (1 モール × 1 scope、≤1000 注文) を 1 取引で core.apply_order_batch() に通す
 *   GET  /apps/company-db/sync/orders/status?mall&scope  そのモールの注文・明細の件数、世代、直近の run
 *   GET  /apps/company-db/sync/orders/receipt            受領記録 (伝票と同じ = run_id で引く)
 *   GET  /apps/company-db/sync/orders/keys?mall&scope&after&limit   投入済みの注文番号 (台帳を作り直すとき)
 *   GET  /apps/company-db/sync/orders/daily?mall&scope&from&to      注文日ごとの 注文数 / 明細数 / 商品代 / 取消 (突合の材料。miniPC 側の raw と同じ式)
 *   POST /apps/company-db/sync/shipments/relink {after, limit}      伝票 → 注文の結び直しを集合で (core.relink_shipments_bulk。注文が入った後に送り手が回す)
 */
const withPg = async (res, fn) => {
  const url = process.env.COMPANY_DB_URL;
  if (!url) return res.status(503).json({ error: 'COMPANY_DB_URL not configured' });
  let client;
  try { client = await pgClientFactory(url); await fn(client); }
  catch (e) { res.status(500).json({ error: String(e.message).slice(0, 300) }); }
  finally { if (client) { try { await client.end(); } catch { /* */ } } }
};
const mallScopeOf = (req) => {
  const mall = String(req.query.mall || ''), scope = String(req.query.scope || 'main');
  if (!MALLS.includes(mall) || !/^[0-9A-Za-z][0-9A-Za-z_-]{0,30}$/.test(scope)) return null;
  return { mall, scope };
};

router.post('/orders', requireSyncKey, shipmentsJson, shipmentsParserError, async (req, res) => {
  const url = process.env.COMPANY_DB_URL;
  if (!url) return res.status(503).json({ error: 'COMPANY_DB_URL not configured' });
  let chunk;
  try { chunk = validateOrderChunk(req.body); }
  catch (e) { return res.status(400).json({ error: e.message }); }
  if (!chunk.mall) return res.status(400).json({ error: 'an orders chunk needs at least one row (mall / scope come from the rows)' });
  let client;
  const t0 = Date.now();
  try {
    client = await pgClientFactory(url);
    await client.query(`set statement_timeout = '20s'; set lock_timeout = '10s'; set idle_in_transaction_session_timeout = '60s'`);
    const r = await ingestOrderChunk(pgAdapter(client), { ...chunk, host: 'render', log: (m) => console.log(`[company-db orders ${chunk.mall}] ${chunk.runId} ${m}`) });
    res.json(r);
  } catch (e) {
    const status = e.code === 'CHUNK_DEADLINE' ? 503 : (e.code === 'RUN_MISMATCH' || e.code === 'CHUNK_MISMATCH' || e.code === 'RUN_CLOSED') ? 409 : e.code === 'BAD_REQUEST' ? 400 : 500;
    console.error(`[company-db orders ${chunk.mall}] ${chunk.runId} chunk ${chunk.chunkIndex} FAILED (${status}, ${Date.now() - t0} ms): ${e.message}`);
    res.status(status).json({ error: String(e.message).slice(0, 300), code: e.code || null, run_id: chunk.runId, chunk_index: chunk.chunkIndex });
  } finally { if (client) { try { await client.end(); } catch { /* */ } } }
});

router.get('/orders/status', requireSyncKey, async (req, res) => {
  const ms = mallScopeOf(req); if (!ms) return res.status(400).json({ error: 'mall / scope are required' });
  await withPg(res, async (client) => {
    const q = async (sql, p = []) => (await client.query(sql, p)).rows;
    const [c] = await q(`select (select count(*) from core.orders where company_id = 1 and mall = $1 and scope_key = $2) as orders,
      (select count(*) from core.order_lines l join core.orders o on o.order_id = l.order_id where o.company_id = 1 and o.mall = $1 and o.scope_key = $2 and l.removed_at is null) as lines,
      (select max(received_batch_seq) from core.orders where company_id = 1 and mall = $1 and scope_key = $2) as max_batch_seq,
      (select max(order_date_jst)::text from core.orders where company_id = 1 and mall = $1 and scope_key = $2) as max_order_date`, [ms.mall, ms.scope]);
    const runs = await q(`select r.ingest_run_id, r.status, r.started_at, r.finished_at, r.rows_seen, r.rows_inserted, r.rows_skipped, r.checksum as batch_seq, r.pages as chunks_expected, r.error,
        (select count(*)::int from ops.ingest_chunks c where c.ingest_run_id = r.ingest_run_id) as chunks_received,
        (select coalesce(sum(c.rows_failed), 0)::int from ops.ingest_chunks c where c.ingest_run_id = r.ingest_run_id) as rows_failed,
        (r.status = 'running' and r.started_at < now() - interval '6 hours') as stalled
       from ops.ingest_runs r where r.source_system = $1 and r.entity = 'orders' and r.scope_key = $2 order by r.started_at desc limit 5`, [ms.mall, ms.scope]);
    res.json({ mall: ms.mall, scope: ms.scope, counts: { orders: Number(c.orders), lines: Number(c.lines), max_batch_seq: c.max_batch_seq == null ? null : Number(c.max_batch_seq), max_order_date: c.max_order_date }, runs });
  });
});

router.get('/orders/keys', requireSyncKey, async (req, res) => {
  const ms = mallScopeOf(req); if (!ms) return res.status(400).json({ error: 'mall / scope are required' });
  const after = String(req.query.after || '');
  const limit = Math.min(Math.max(Number(req.query.limit) || 20000, 1), 50000);
  await withPg(res, async (client) => {
    const rows = (await client.query(`select mall_order_no from core.orders where company_id = 1 and mall = $1 and scope_key = $2 and mall_order_no > $3 order by mall_order_no limit $4`, [ms.mall, ms.scope, after, limit])).rows.map((r) => r.mall_order_no);
    res.json({ keys: rows, next: rows.length === limit ? rows[rows.length - 1] : null });
  });
});

router.get('/orders/daily', requireSyncKey, async (req, res) => {
  const ms = mallScopeOf(req); if (!ms) return res.status(400).json({ error: 'mall / scope are required' });
  const from = String(req.query.from || ''), to = String(req.query.to || '');
  if (!DATE_RE.test(from) || !DATE_RE.test(to) || from > to) return res.status(400).json({ error: 'from / to must be YYYY-MM-DD and from <= to' });
  if ((Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) / 86400000 > 400) return res.status(400).json({ error: 'range must be <= 400 days' });
  await withPg(res, async (client) => {
    // miniPC 側 (mall-orders.mjs の dailySql) と同じ式: 注文日 (JST) ごとの 注文数 / 明細数 (現行の集合) / 商品代 (null は 0) / 取消の注文数
    const rows = (await client.query(
      `select o.order_date_jst::text as order_date, count(*)::int as orders,
              coalesce(sum((select count(*) from core.order_lines l where l.order_id = o.order_id and l.removed_at is null)), 0)::int as lines,
              coalesce(sum(coalesce(o.items_amount_jpy, 0)), 0)::bigint as items_amount_jpy,
              (count(*) filter (where o.is_cancelled))::int as cancelled
         from core.orders o where o.company_id = 1 and o.mall = $1 and o.scope_key = $2 and o.order_date_jst between $3::date and $4::date
        group by o.order_date_jst order by o.order_date_jst`, [ms.mall, ms.scope, from, to])).rows;
    res.json({ mall: ms.mall, scope: ms.scope, from, to, rows: rows.map((r) => ({ ...r, items_amount_jpy: Number(r.items_amount_jpy) })) });
  });
});

router.post('/shipments/relink', requireSyncKey, express.json({ limit: '4kb' }), async (req, res) => {
  const after = Number(req.body && req.body.after) || 0, limit = Math.min(Math.max(Number(req.body && req.body.limit) || 20000, 1), 100000);
  await withPg(res, async (client) => {
    await client.query(`set statement_timeout = '60s'; set lock_timeout = '10s'`);
    const r = (await client.query(`select linked, examined, last_id from core.relink_shipments_bulk(1::smallint, $1::bigint, $2::int)`, [after, limit])).rows[0];
    res.json({ linked: Number(r.linked), examined: Number(r.examined), last_id: r.last_id == null ? null : Number(r.last_id) });
  });
});

/**
 * 売上の日次 mart.sales_daily (0021。08 §4.5 / §9 D7a) の作り直し。注文を送った後に送り手 (push/mall-orders.mjs) が呼ぶ。
 *   POST /orders/sales-daily/refresh { mall, scope, limit?, reset? } → { session_id, resumed, run_id, dates_built, remaining, n_rows, n_orders, purged }
 *     resumed = その呼び出しより前から開いていた回の続きだった (🚨 同じ run の 2 回目以降の呼び出しでも true)。前の run が途中で止めた回を終えても、回の開始より後に動いた注文は次の回でないと拾えない
 *       = 送り手は **最初の呼び出しが resumed だったときだけ** もう 1 回ぶん回す (自分で開いた回では回さない = 同じ日を丸ごともう 1 周作らない)
 *     どの日を作り直すかも、回 (session) の続きも DB が覚えている (mart.refresh_sales_daily)。remaining > 0 なら同じ body (reset は外す) で呼び直す。
 *     🚨 外から時刻や回の目印を渡す口は無い (body.session は 400)。未来の時刻を渡されてその日が永久に作り直されなくなる、を作らない (Codex D7a R1 #1)
 *     全部終わった回 (remaining = 0) のついでに、指されなくなった古い行を消す (mart.purge_sales_daily。猶予 3 日)。
 *     0021 がまだ適用されていなければ 409 { error: 'not_migrated' } (送り手は「売上日次は未適用」と出して注文の push 自体は失敗にしない)
 *   GET  /orders/sales-daily/check?mall&scope&from&to → 公開中の集計と材料の食い違い (mart.sales_daily_check()。0 行が正常。取消・売上・払った額・数量も比べる) と公開中の合計
 * 🚨 パスを /orders/ の下に置いているのは server.js の「body parser より前の鍵の検査」が /orders と /shipments の prefix に掛かっているため
 */
const SALES_FN = 'mart.refresh_sales_daily(smallint,text,text,integer,boolean,text)';
const salesReady = async (client) => (await client.query(`select to_regprocedure($1) is not null as ok`, [SALES_FN])).rows[0].ok === true;
router.post('/orders/sales-daily/refresh', requireSyncKey, express.json({ limit: '4kb' }), async (req, res) => {
  const b = req.body || {};
  const ms = mallScopeOf({ query: { mall: b.mall, scope: b.scope } });
  if (!ms || typeof b.scope !== 'string') return res.status(400).json({ error: 'mall / scope are required' });
  const limit = b.limit == null ? 31 : Number(b.limit);
  if (!Number.isInteger(limit) || limit < 1 || limit > 400) return res.status(400).json({ error: 'limit must be 1..400' });
  if (b.session !== undefined || b.session_at !== undefined || b.session_id !== undefined) return res.status(400).json({ error: 'session is kept by the database; do not pass it' });
  if (b.reset != null && typeof b.reset !== 'boolean') return res.status(400).json({ error: 'reset must be a boolean' });
  await withPg(res, async (client) => {
    if (!(await salesReady(client))) return res.status(409).json({ error: 'not_migrated', detail: 'migration 0021 (mart.sales_daily) is not applied' });
    await client.query(`set statement_timeout = '60s'; set lock_timeout = '10s'`);
    const r = (await client.query(`select session_id, resumed, run_id, dates_built, remaining, n_rows, n_orders from mart.refresh_sales_daily(1::smallint, $1, $2, $3::int, $4::boolean, 'render')`,
      [ms.mall, ms.scope, limit, b.reset === true])).rows[0];
    let purged = null;
    if (Number(r.remaining) === 0) purged = Number((await client.query(`select mart.purge_sales_daily(1::smallint, 3) as n`)).rows[0].n);
    res.json({ session_id: r.session_id, resumed: r.resumed === true, run_id: r.run_id, dates_built: Number(r.dates_built), remaining: Number(r.remaining), n_rows: Number(r.n_rows), n_orders: Number(r.n_orders), purged });
  });
});

router.get('/orders/sales-daily/check', requireSyncKey, async (req, res) => {
  const ms = mallScopeOf(req); if (!ms || !req.query.scope) return res.status(400).json({ error: 'mall / scope are required' });
  const from = String(req.query.from || ''), to = String(req.query.to || '');
  if (!DATE_RE.test(from) || !DATE_RE.test(to) || from > to) return res.status(400).json({ error: 'from / to must be YYYY-MM-DD and from <= to' });
  if ((Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) / 86400000 > 400) return res.status(400).json({ error: 'range must be <= 400 days' });
  await withPg(res, async (client) => {
    if (!(await salesReady(client))) return res.status(409).json({ error: 'not_migrated', detail: 'migration 0021 (mart.sales_daily) is not applied' });
    await client.query(`set statement_timeout = '60s'`);
    const diffs = (await client.query(`select date_jst::text as date_jst, is_published, src_lines, pub_lines, src_units, pub_units, src_units_cancelled, pub_units_cancelled, src_items_amount_jpy, pub_items_amount_jpy,
        src_cancelled_amount_jpy, pub_cancelled_amount_jpy, src_sales_jpy, pub_sales_jpy, src_customer_paid_jpy, pub_customer_paid_jpy, src_lines_amount_unknown, pub_lines_amount_unknown
      from mart.sales_daily_check(1::smallint, $1, $2, $3::date, $4::date) limit 200`, [ms.mall, ms.scope, from, to])).rows;
    const tot = (await client.query(`select count(distinct date_jst)::int as dates, coalesce(sum(orders), 0)::bigint as order_grains, coalesce(sum(lines), 0)::bigint as lines, coalesce(sum(items_amount_jpy), 0)::bigint as items_amount_jpy,
        coalesce(sum(cancelled_items_amount_jpy), 0)::bigint as cancelled_items_amount_jpy, coalesce(sum(sales_jpy), 0)::bigint as sales_jpy, coalesce(sum(customer_paid_jpy), 0)::bigint as customer_paid_jpy,
        coalesce(sum(lines_amount_unknown), 0)::bigint as lines_amount_unknown, coalesce(sum(lines_unresolved), 0)::bigint as lines_unresolved
      from mart.v_sales_daily where company_id = 1 and mall = $1 and scope_key = $2 and date_jst between $3::date and $4::date`, [ms.mall, ms.scope, from, to])).rows[0];
    const n = (o) => Object.fromEntries(Object.entries(o).map(([k, v]) => [k, typeof v === 'bigint' || (typeof v === 'string' && /^-?\d+$/.test(v)) ? Number(v) : v]));
    res.json({ mall: ms.mall, scope: ms.scope, from, to, published: n(tot), diffs: diffs.map(n) });
  });
});

/** 送り手が「前回受領確認した chunk が Render にまだあるか」を確かめる (無ければ Render が復元・作り直された = 台帳の指紋を空にして全部送り直す。Codex R3 #2) */
/**
 * 在庫の日次 (SKU 単位。08 §3.3 の ③ NE = D2b-1)。miniPC が朝の在庫スナップショットの直後に 1 日 = 1 要求で送る (apps/company-db/push/stock-daily.mjs)。
 *   POST /apps/company-db/sync/stock-daily  { source, scope?, snapshot_date, captured_at, rows: [{ code, qty }] } | { source, scope?, snapshot_date, missing: true }
 *     → { status: 'applied' | 'same' | 'missing' | 'missing_same', rows, resolved, unresolved, run_id, checksum }
 *     1 取引で stock_capture_days を building → 行 → complete。先に確定した日は書き換えない (同じ内容 = same / 違う内容 = 409)。未来の日付・行 0 件は 400
 *   GET  /apps/company-db/sync/stock-daily/status?source&scope&from&to  → { days: [{ snapshot_date, status, rows, checksum }] } (送り手が「まだ送っていない日」を決める)
 */
const stockJson = express.json({ limit: '4mb' });   // NE = 1 日 約 5,000 行 ≒ 200KB
const stockParserError = (err, req, res, next) => (err ? res.status(err.type === 'entity.too.large' ? 413 : 400).json({ error: `body を読めない: ${String(err.message).slice(0, 200)}` }) : next());
router.post('/stock-daily', requireSyncKey, stockJson, stockParserError, async (req, res) => {
  const url = process.env.COMPANY_DB_URL;
  if (!url) return res.status(503).json({ error: 'COMPANY_DB_URL not configured' });
  let client;
  const t0 = Date.now();
  const tag = `${String(req.body && req.body.source).slice(0, 20)} ${String(req.body && req.body.snapshot_date).slice(0, 12)}`;
  try {
    client = await pgClientFactory(url);
    await client.query(`set statement_timeout = '40s'; set lock_timeout = '10s'; set idle_in_transaction_session_timeout = '60s'`);
    const r = await ingestStockDay(pgAdapter(client), req.body, { host: 'render', log: (m) => console.log(`[company-db stock-daily] ${m}`) });
    res.json({ ...r, ms: Date.now() - t0 });
  } catch (e) {
    const status = e.code === 'BAD_REQUEST' ? 400 : e.code === 'CONFLICT' ? 409 : e.code === 'LOCKED' ? 503 : 500;
    if (status >= 500) console.error(`[company-db stock-daily] ${tag} FAILED (${status}, ${Date.now() - t0} ms): ${e.message}`);
    res.status(status).json({ error: String(e.message).slice(0, 300), code: e.code || null });
  } finally { if (client) { try { await client.end(); } catch { /* 閉じられなくても応答は出す */ } } }
});
router.get('/stock-daily/status', requireSyncKey, async (req, res) => {
  await withPg(res, async (client) => {
    try {
      const days = await stockDayStatus(pgAdapter(client), { source: String(req.query.source || ''), scope: req.query.scope === undefined ? undefined : String(req.query.scope), from: String(req.query.from || ''), to: String(req.query.to || '') });
      res.json({ days });
    } catch (e) {
      if (e.code === 'BAD_REQUEST') return res.status(400).json({ error: e.message });
      throw e;
    }
  });
});

router.get(['/shipments/receipt', '/orders/receipt'], requireSyncKey, async (req, res) => {
  const url = process.env.COMPANY_DB_URL;
  if (!url) return res.status(503).json({ error: 'COMPANY_DB_URL not configured' });
  const runId = String(req.query.run_id || ''), idx = Number(req.query.chunk_index);
  if (!/^ship_[0-9]{15}_[0-9a-f]{6}$/.test(runId) || !Number.isInteger(idx) || idx < 0) return res.status(400).json({ error: 'run_id / chunk_index are required' });
  let client;
  try {
    client = await pgClientFactory(url);
    const r = (await client.query(`select payload_checksum from ops.ingest_chunks where ingest_run_id = $1 and chunk_index = $2`, [runId, idx])).rows[0];
    res.json({ found: !!r, payload_checksum: r ? r.payload_checksum : null });
  } catch (e) { res.status(500).json({ error: String(e.message).slice(0, 300) }); }
  finally { if (client) { try { await client.end(); } catch { /* */ } } }
});

/** 投入済みの伝票番号の一覧 (台帳を作り直すとき、範囲の条件から外れた投入済みの伝票を追跡対象に戻す。Codex R3 #3)。伝票番号順に keyset で送る */
router.get('/shipments/slips', requireSyncKey, async (req, res) => {
  const url = process.env.COMPANY_DB_URL;
  if (!url) return res.status(503).json({ error: 'COMPANY_DB_URL not configured' });
  const after = String(req.query.after || '');
  const limit = Math.min(Math.max(Number(req.query.limit) || 20000, 1), 50000);
  let client;
  try {
    client = await pgClientFactory(url);
    const rows = (await client.query(`select ne_slip_no from core.shipments where company_id = 1 and ne_slip_no > $1 order by ne_slip_no limit $2`, [after, limit])).rows.map((r) => r.ne_slip_no);
    res.json({ slips: rows, next: rows.length === limit ? rows[rows.length - 1] : null });
  } catch (e) { res.status(500).json({ error: String(e.message).slice(0, 300) }); }
  finally { if (client) { try { await client.end(); } catch { /* */ } } }
});

router.get('/shipments/status', requireSyncKey, async (req, res) => {
  const url = process.env.COMPANY_DB_URL;
  if (!url) return res.status(503).json({ error: 'COMPANY_DB_URL not configured' });
  let client;
  try {
    client = await pgClientFactory(url);
    const q = async (sql, p = []) => (await client.query(sql, p)).rows;
    const [c] = await q(`select (select count(*) from core.shipments where company_id = 1) as shipments, (select count(*) from core.shipment_lines where company_id = 1 and removed_at is null) as lines,
      (select max(received_batch_seq) from core.shipments where company_id = 1) as max_batch_seq, (select max(ship_date_jst)::text from core.shipments where company_id = 1) as max_ship_date,
      (select count(*) from core.shipments where company_id = 1 and order_id is not null) as linked`);
    const unlinked = await q(`select reason, count(*)::int as n from mart.v_shipments_unlinked where company_id = 1 group by reason order by reason`);
    // run ごとの失敗の総数は chunk の受領記録から (failed_ranges は先頭 200 件で切る)。running のまま 6 時間過ぎた run は送り手が途中で死んだもの (stalled)
    const runs = await q(`select r.ingest_run_id, r.status, r.started_at, r.finished_at, r.rows_seen, r.rows_inserted, r.rows_skipped, r.checksum as batch_seq, r.pages as chunks_expected, r.error,
        (select count(*)::int from ops.ingest_chunks c where c.ingest_run_id = r.ingest_run_id) as chunks_received,
        (select coalesce(sum(c.rows_failed), 0)::int from ops.ingest_chunks c where c.ingest_run_id = r.ingest_run_id) as rows_failed,
        (r.status = 'running' and r.started_at < now() - interval '6 hours') as stalled
       from ops.ingest_runs r where r.source_system = 'ne' and r.entity = 'shipments' order by r.started_at desc limit 5`);
    res.json({ counts: { shipments: Number(c.shipments), lines: Number(c.lines), linked: Number(c.linked), max_batch_seq: c.max_batch_seq == null ? null : Number(c.max_batch_seq), max_ship_date: c.max_ship_date }, unlinked, runs });
  } catch (e) { res.status(500).json({ error: String(e.message).slice(0, 300) }); }
  finally { if (client) { try { await client.end(); } catch { /* */ } } }
});

/** run_id は newLoadRunId() の形だけ受ける (パスの部品にするので、それ以外は 400) */
const RUN_ID_RE = /^load_[0-9]{15}_[0-9a-f]{6}$/;

router.get('/reports', requireSyncKey, (req, res) => {
  const dataDir = process.env.DATA_DIR;
  if (!dataDir) return res.status(503).json({ error: 'DATA_DIR not configured' });
  const dir = reportDir(dataDir);
  const out = [];
  try {
    for (const f of fs.existsSync(dir) ? fs.readdirSync(dir) : []) {
      const m = /^load-(load_[0-9]{15}_[0-9a-f]{6})\.json$/.exec(f); if (!m) continue;
      try { const j = JSON.parse(fs.readFileSync(path.join(dir, f), 'utf-8')); out.push({ run_id: m[1], ok: j.ok, dry_run: j.dry_run, started_at: j.started_at || null, finished_at: j.finished_at || null, conflicts: (j.conflicts || []).length, error: j.error || null }); }
      catch (e) { out.push({ run_id: m[1], error: `読めない: ${e.message}` }); }
    }
  } catch (e) { return res.status(500).json({ error: e.message }); }
  out.sort((a, b) => (a.run_id < b.run_id ? 1 : -1));
  res.json({ reports: out });
});

router.get('/report/:run_id', requireSyncKey, (req, res) => {
  const dataDir = process.env.DATA_DIR;
  if (!dataDir) return res.status(503).json({ error: 'DATA_DIR not configured' });
  const runId = String(req.params.run_id || '');
  if (!RUN_ID_RE.test(runId)) return res.status(400).json({ error: 'bad run_id' });
  const md = String(req.query.format || '') === 'md';
  const file = path.join(reportDir(dataDir), `load-${runId}.${md ? 'md' : 'json'}`);
  if (!fs.existsSync(file)) return res.status(404).json({ error: 'not found', run_id: runId });
  if (md) { res.type('text/markdown; charset=utf-8'); return res.send(fs.readFileSync(file, 'utf-8')); }
  res.type('application/json; charset=utf-8'); res.send(fs.readFileSync(file, 'utf-8'));
});

router.get('/status', requireSyncKey, async (req, res) => {
  const dataDir = process.env.DATA_DIR;
  const url = process.env.COMPANY_DB_URL;
  const out = { current: state.current, last: state.last, latest: null, interrupted: null, counts: null };
  // latest.json と running.json は別々に読む (latest が壊れていても interrupted と commit 照会は出す。Codex R4-3)
  try {
    const latestPath = dataDir ? path.join(reportDir(dataDir), 'latest.json') : null;
    if (latestPath && fs.existsSync(latestPath)) out.latest = JSON.parse(fs.readFileSync(latestPath, 'utf-8'));
  } catch (e) { out.latest_error = e.message; }
  try {
    if (dataDir) { const r = interruptedRecord(dataDir); if (r) out.interrupted = { ...r, committed: null, note: '始めたのに結果が無い (プロセスが途中で終わった、または結果を書けなかった)。committed が true なら本適用は済んでいる (report だけ無い)' }; }
  } catch (e) { out.interrupted_error = e.message; }
  if (url && String(req.query.counts || '1') !== '0') {
    let client;
    try {
      client = await pgClientFactory(url);
      const db = pgAdapter(client);
      const tables = ['core.products', 'core.skus', 'core.sku_components', 'core.sku_costs', 'core.listings', 'core.listing_components', 'core.catalog_items', 'core.external_ids', 'core.product_attribute_observations', 'core.attribute_resolutions', 'core.product_physicals', 'core.product_compliance', 'core.suppliers', 'core.supplier_skus', 'core.workers'];
      out.counts = {};
      for (const t of tables) out.counts[t] = Number((await db.query(`select count(*)::bigint as n from ${t}`)).rows[0].n);
      out.migrations = (await db.query('select version from ops.schema_migrations order by version')).rows.map((r) => r.version);
      if (out.interrupted) {
        const ir = (await db.query('select status, finished_at from ops.ingest_runs where ingest_run_id = $1', [out.interrupted.run_id])).rows[0];
        out.interrupted.committed = !!ir; if (ir) out.interrupted.ingest_run = { status: ir.status, finished_at: ir.finished_at };
      }
    } catch (e) { out.counts_error = e.message; }
    finally { if (client) await client.end(); }
  }
  res.json(out);
});

export default router;
