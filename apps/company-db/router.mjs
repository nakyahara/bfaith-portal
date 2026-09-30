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
import { ingestAdSpendDay, adSpendStatus, relinkAdSpend } from './ingest/ad-spend.mjs';
import { ingestShipmentChunk, validateChunk } from './ingest/shipments.mjs';
import { ingestOrderChunk, validateChunk as validateOrderChunk, MALLS } from './ingest/orders.mjs';
import { ingestOrderFinanceChunk, validateFinanceChunk, FINANCE_MALLS } from './ingest/order-finance.mjs';
import { ingestSkuCostObserved, skuCostObservedStatus, skuCodeNorms } from './ingest/sku-cost-observed.mjs';
import { applyCoverage, coverageStatus, coverageReady } from './ingest/finance-coverage.mjs';
import { SOURCES as COVERAGE_SOURCES } from './finance/order-finance-checksum.mjs';

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
/** parser の失敗の応答。上限の文言は受け口ごと (#1561 Codex R1 Low: coverage は 64KB なのに「12MB」と返していた) */
const parserErrorFor = (limitLabel) => function parserError(err, req, res, next) {
  if (!err) return next();
  if (err.type === 'entity.too.large') return res.status(413).json({ error: `payload too large (${limitLabel})` });
  if (err.type === 'encoding.unsupported') return res.status(415).json({ error: 'compressed body is not accepted' });
  if (err.type === 'entity.parse.failed') return res.status(400).json({ error: 'invalid JSON' });
  if (err.type === 'request.aborted') return res.status(400).json({ error: 'request aborted' });
  return next(err);
};
const shipmentsParserError = parserErrorFor('12MB');

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
      (select max(order_date_jst)::text from core.orders where company_id = 1 and mall = $1 and scope_key = $2) as max_order_date,
      (select count(*) from core.orders where company_id = 1 and mall = $1 and scope_key = $2 and not is_cancelled and mall_coupon_jpy is null) as mall_coupon_unknown`, [ms.mall, ms.scope]);   // 売上日次は null を 0 として払った額を出す (Yahoo の公開の前提。#1502 Codex R1)
    const runs = await q(`select r.ingest_run_id, r.status, r.started_at, r.finished_at, r.rows_seen, r.rows_inserted, r.rows_skipped, r.checksum as batch_seq, r.pages as chunks_expected, r.error,
        (select count(*)::int from ops.ingest_chunks c where c.ingest_run_id = r.ingest_run_id) as chunks_received,
        (select coalesce(sum(c.rows_failed), 0)::int from ops.ingest_chunks c where c.ingest_run_id = r.ingest_run_id) as rows_failed,
        (r.status = 'running' and r.started_at < now() - interval '6 hours') as stalled
       from ops.ingest_runs r where r.source_system = $1 and r.entity = 'orders' and r.scope_key = $2 order by r.started_at desc limit 5`, [ms.mall, ms.scope]);
    res.json({ mall: ms.mall, scope: ms.scope, counts: { orders: Number(c.orders), lines: Number(c.lines), max_batch_seq: c.max_batch_seq == null ? null : Number(c.max_batch_seq), max_order_date: c.max_order_date, mall_coupon_unknown: Number(c.mall_coupon_unknown) }, runs });
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

/**
 * 注文 (疑似注文) の財務の push の受け口 (F2b-1。送り手 = apps/company-db/push/amazon-finance.mjs (F2b-2)、本体 = ingest/order-finance.mjs・0043):
 *   POST /apps/company-db/sync/order-finance                          1 chunk (1 モール × 1 scope) を 1 取引で core.apply_order_finance_batch() に。集合の指紋は受け口が計算し直す
 *   GET  /apps/company-db/sync/order-finance/status?mall&scope        件数・世代・直近の run・DB の大きさ (pg_database_size と、読めれば WAL の大きさ = 送り手が次の chunk の前に容量を見る)
 *   GET  /apps/company-db/sync/order-finance/receipt                  受領記録 (伝票と同じ = run_id で引く)
 *   GET  /apps/company-db/sync/order-finance/keys?mall&scope&after&limit     受け取った注文番号 (疑似注文も。送り手の全件の作り直しで Render にだけある鍵を見つける)
 *   GET  /apps/company-db/sync/order-finance/daily?mall&scope&from&to        mart.finance_daily_range (0044 = v_finance_daily と同じ式を期間の月だけで。日 × SKU・突き合わせの材料)
 *   GET  /apps/company-db/sync/order-finance/account-fees?mall&scope&from&to mart.v_finance_account_fees_monthly (月 × 手数料の種類)
 *   GET  /apps/company-db/sync/order-finance/uncovered?mall&scope            mart.v_order_finance_uncovered の件数と例 (policy が無い日・source が違う日)
 *   POST /apps/company-db/sync/order-finance/coverage                       決済のそろい (0051・D7b-1b-2) の updating / complete (下の節)
 *   GET  /apps/company-db/sync/order-finance/coverage/status?mall&scope&source   決済のそろいの今の状態・世代
 *   🆕 chunk の body に coverage_generation / run_token (両方) = その世代・token の coverage が updating のときだけ適用 (違えば 409 COVERAGE_MISMATCH)。
 *      無い chunk (今の送り手) は今までどおり受けるが、受領記録を変えたら complete を updating に落とす (応答の coverage_invalidated)
 * 設計 = AI_reference『CompanyDB構想/12_Amazon財務のCompanyDB取込_設計_20260929.md』
 */
const financeMallScopeOf = (req) => {
  const mall = String(req.query.mall || ''), scope = String(req.query.scope || '');
  if (!FINANCE_MALLS.includes(mall) || !/^[0-9A-Za-z][0-9A-Za-z_-]{0,30}$/.test(scope)) return null;
  return { mall, scope };
};
// 2026-02-30 は通さない (DB の 500 ではなく 400)・2026-13-01 は Date が不正 = toISOString の例外の前に NaN で落とす (#1533 Codex R2)
const isRealDate = (s) => { if (!DATE_RE.test(s)) return false; const ms = Date.parse(`${s}T00:00:00Z`); return !Number.isNaN(ms) && new Date(ms).toISOString().slice(0, 10) === s; };
const financeRangeOf = (req, maxDays) => {
  const from = String(req.query.from || ''), to = String(req.query.to || '');
  if (!isRealDate(from) || !isRealDate(to) || from > to) return { error: 'from / to must be real dates (YYYY-MM-DD) and from <= to' };
  if ((Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) / 86400000 + 1 > maxDays) return { error: `range must be <= ${maxDays} days (both ends included)` };
  return { from, to };
};

router.post('/order-finance', requireSyncKey, shipmentsJson, shipmentsParserError, async (req, res) => {
  const url = process.env.COMPANY_DB_URL;
  if (!url) return res.status(503).json({ error: 'COMPANY_DB_URL not configured' });
  let chunk;
  try { chunk = validateFinanceChunk(req.body); }
  catch (e) { return res.status(400).json({ error: e.message }); }
  if (!chunk.mall) return res.status(400).json({ error: 'an order finance chunk needs at least one row (mall / scope come from the rows)' });
  let client;
  const t0 = Date.now();
  try {
    client = await pgClientFactory(url);
    await client.query(`set statement_timeout = '20s'; set lock_timeout = '10s'; set idle_in_transaction_session_timeout = '60s'`);
    const r = await ingestOrderFinanceChunk(pgAdapter(client), { ...chunk, host: 'render', log: (m) => console.log(`[company-db order-finance ${chunk.mall}] ${chunk.runId} ${m}`) });
    res.json(r);
  } catch (e) {
    // NOT_MIGRATED = 新しい形 (分けられない部品の 4 列) の行が 0047 の適用前に届いた・token 付きの chunk が 0051 の適用前に届いた /
    // DOWNGRADE = 今の形の版の注文を旧い版で置き換えようとした (ingest/order-finance.mjs) /
    // COVERAGE_MISMATCH = token 付きの chunk の世代・token の coverage が updating でない (complete の後・別の世代・別の token) / LOCKED = coverage の要求か別の chunk が lock を持ったまま (0051)
    const status = (e.code === 'CHUNK_DEADLINE' || e.code === 'LOCKED') ? 503
      : (e.code === 'RUN_MISMATCH' || e.code === 'CHUNK_MISMATCH' || e.code === 'RUN_CLOSED' || e.code === 'NOT_MIGRATED' || e.code === 'DOWNGRADE' || e.code === 'COVERAGE_MISMATCH') ? 409
        : e.code === 'BAD_REQUEST' ? 400 : 500;
    console.error(`[company-db order-finance ${chunk.mall}] ${chunk.runId} chunk ${chunk.chunkIndex} FAILED (${status}, ${Date.now() - t0} ms): ${e.message}`);
    res.status(status).json({ error: String(e.message).slice(0, 300), code: e.code || null, run_id: chunk.runId, chunk_index: chunk.chunkIndex });
  } finally { if (client) { try { await client.end(); } catch { /* */ } } }
});

/**
 * 決済のそろい (coverage・0051・D7b-1b-2。本体 = ingest/finance-coverage.mjs。設計 = AI_reference『CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』§3.1):
 *   POST /apps/company-db/sync/order-finance/coverage   { state: 'updating' | 'complete', mall, scope, source, generation, run_token, manifest? (complete だけ), request_hash? }
 *     → { status: 'applied' | 'same' | 'stale', state, generation, current_generation?, complete_to?, receipt? }
 *       400 = 形 / 409 = CONFLICT (状態の移り方で受けない)・RECEIPT_MISMATCH (受領記録が manifest と違う = detail.render に Render の数と digest)・NO_POLICY・not_migrated (0051 の前) / 503 LOCKED (lock が空かない)
 *   GET  /apps/company-db/sync/order-finance/coverage/status?mall&scope&source[&receipts=1]
 *     → { mall, scope, source, coverage: 行 | null, effective: { complete_to, generation, source_revision } (core.finance_coverage_state), receipts?: { count, lines, digest } }
 *       世代・source_revision = 10 進の文字列。0051 の前は 409 { error: 'not_migrated' }
 *   🚨 送るのは miniPC の coordinator (D7b-1b-3・後の PR) だけ。今の送り手 (daily-sync) は coverage を送らない = complete が無い間は正式な利益は全部 null のまま
 *   🚨 鍵の検査は server.js の '/apps/company-db/sync/order-finance' の前方一致 (body parser より前) に入る
 */
const coverageJson = express.json({ limit: '64kb', inflate: false });
router.post('/order-finance/coverage', requireSyncKey, coverageJson, parserErrorFor('64KB'), async (req, res) => {
  const url = process.env.COMPANY_DB_URL;
  if (!url) return res.status(503).json({ error: 'COMPANY_DB_URL not configured' });
  let client;
  const t0 = Date.now();
  const tag = `${String(req.body && req.body.mall).slice(0, 12)}/${String(req.body && req.body.scope).slice(0, 12)} ${String(req.body && req.body.state).slice(0, 10)} gen ${String(req.body && req.body.generation).slice(0, 20)}`;
  try {
    client = await pgClientFactory(url);
    // complete は受領記録 (約 51 万注文) を 1 回読む。lock は走っている chunk の後に取れるまで待つ (20 秒で 503 LOCKED = 送り手がやり直す。
    // 設計の complete の送信 = 1 回 30 秒の再試行 → 待ち 20 秒 + digest の計算が 30 秒に入るように)
    await client.query(`set statement_timeout = '60s'; set lock_timeout = '20s'; set idle_in_transaction_session_timeout = '90s'`);
    const r = await applyCoverage(pgAdapter(client), req.body, { log: (m) => console.log(`[company-db coverage] ${tag} ${m}`) });
    res.json({ ...r, ms: Date.now() - t0 });
  } catch (e) {
    if (e.code === 'NOT_MIGRATED') return res.status(409).json({ error: 'not_migrated', detail: 'migration 0051 (core.finance_coverage) is not applied', code: e.code });
    const status = e.code === 'BAD_REQUEST' ? 400 : (e.code === 'CONFLICT' || e.code === 'RECEIPT_MISMATCH' || e.code === 'NO_POLICY') ? 409 : e.code === 'LOCKED' ? 503 : 500;
    console.error(`[company-db coverage] ${tag} FAILED (${status}, ${Date.now() - t0} ms): ${e.message}`);
    res.status(status).json({ error: String(e.message).slice(0, 400), code: e.code || null, ...(e.detail ? { detail: e.detail } : {}) });
  } finally { if (client) { try { await client.end(); } catch { /* 閉じられなくても応答は出す */ } } }
});
router.get('/order-finance/coverage/status', requireSyncKey, async (req, res) => {
  const ms = financeMallScopeOf(req); if (!ms) return res.status(400).json({ error: 'mall / scope are required' });
  const source = String(req.query.source || '');
  if (!COVERAGE_SOURCES.includes(source)) return res.status(400).json({ error: `source must be one of ${COVERAGE_SOURCES.join(', ')}` });
  await withPg(res, async (client) => {
    const db = pgAdapter(client);
    if (!(await coverageReady(db))) return res.status(409).json({ error: 'not_migrated', detail: 'migration 0051 (core.finance_coverage) is not applied' });
    await client.query(`set statement_timeout = '60s'`);
    res.json(await coverageStatus(db, { mall: ms.mall, scope: ms.scope, source, withReceipts: String(req.query.receipts || '') === '1' }));
  });
});

router.get('/order-finance/status', requireSyncKey, async (req, res) => {
  const ms = financeMallScopeOf(req); if (!ms) return res.status(400).json({ error: 'mall / scope are required' });
  await withPg(res, async (client) => {
    const q = async (sql, p = []) => (await client.query(sql, p)).rows;
    const [c] = await q(`select (select count(*) from core.order_finance_receipts where company_id = 1 and mall = $1 and scope_key = $2) as orders,
      (select count(*) from core.order_finance_daily where company_id = 1 and mall = $1 and scope_key = $2) as rows,
      (select max(received_batch_seq) from core.order_finance_receipts where company_id = 1 and mall = $1 and scope_key = $2) as max_batch_seq,
      (select max(economic_date_jst)::text from core.order_finance_daily where company_id = 1 and mall = $1 and scope_key = $2) as max_economic_date,
      pg_database_size(current_database()) as db_bytes`, [ms.mall, ms.scope]);
    // WAL の大きさ (pg_database_size は WAL を含まない)。権限が無ければ null = 送り手は Render での試しで測った倍率で見込む
    let walBytes = null;
    try { walBytes = Number((await q(`select coalesce(sum(size), 0) as b from pg_ls_waldir()`))[0].b); } catch { walBytes = null; }
    const runs = await q(`select r.ingest_run_id, r.status, r.started_at, r.finished_at, r.rows_seen, r.rows_inserted, r.rows_skipped, r.checksum as batch_seq, r.pages as chunks_expected, r.error,
        (select count(*)::int from ops.ingest_chunks c where c.ingest_run_id = r.ingest_run_id) as chunks_received,
        (select coalesce(sum(c.rows_failed), 0)::int from ops.ingest_chunks c where c.ingest_run_id = r.ingest_run_id) as rows_failed,
        (r.status = 'running' and r.started_at < now() - interval '6 hours') as stalled
       from ops.ingest_runs r where r.source_system = $1 and r.entity = 'order_finance' and r.scope_key = $2 order by r.started_at desc limit 5`, [ms.mall, ms.scope]);
    res.json({ mall: ms.mall, scope: ms.scope,
      counts: { orders: Number(c.orders), rows: Number(c.rows), max_batch_seq: c.max_batch_seq == null ? null : Number(c.max_batch_seq), max_economic_date: c.max_economic_date },
      size: { db_bytes: Number(c.db_bytes), wal_bytes: walBytes }, runs });
  });
});

router.get('/order-finance/keys', requireSyncKey, async (req, res) => {
  const ms = financeMallScopeOf(req); if (!ms) return res.status(400).json({ error: 'mall / scope are required' });
  const after = String(req.query.after || '');
  const limitRaw = req.query.limit === undefined ? 20000 : Number(req.query.limit);
  if (!Number.isInteger(limitRaw)) return res.status(400).json({ error: 'limit must be an integer' });
  const limit = Math.min(Math.max(limitRaw, 1), 50000);
  await withPg(res, async (client) => {
    // 受領状態の表 = 空の集合を受け取った注文も入る (lines = 0)。鍵の並びは collate "C" (バイト順・送り手の after と同じ)
    const rows = (await client.query(`select mall_order_no, lines from core.order_finance_receipts where company_id = 1 and mall = $1 and scope_key = $2 and mall_order_no collate "C" > $3 order by mall_order_no collate "C" limit $4`,
      [ms.mall, ms.scope, after, limit])).rows;
    res.json({ keys: rows.map((r) => r.mall_order_no), lines: rows.map((r) => Number(r.lines)), next: rows.length === limit ? rows[rows.length - 1].mall_order_no : null });
  });
});

router.get('/order-finance/daily', requireSyncKey, async (req, res) => {
  const ms = financeMallScopeOf(req); if (!ms) return res.status(400).json({ error: 'mall / scope are required' });
  const rg = financeRangeOf(req, 62); if (rg.error) return res.status(400).json({ error: rg.error });
  await withPg(res, async (client) => {
    // 🚨 view (mart.v_finance_daily) は全期間をまとめてから絞る = 52 万行で 5 分を超えた (2026-09-29) → 期間の月だけ読む関数 (0044)。止まらないように時間の上限も
    await client.query(`set statement_timeout = '120s'`);
    const rows = (await client.query(`select economic_date_jst::text as date_jst, seller_sku, units_ordered, units_refunded_customer, units_marketplace_guarantee, units_a_to_z_refund, units_net_sold,
        sales_principal_jpy, sales_shipping_jpy, sales_giftwrap_jpy, sales_tax_jpy, commission_jpy, fba_fulfillment_jpy, fba_storage_jpy, closing_fee_jpy,
        shipping_chargeback_jpy, giftwrap_chargeback_jpy, promotion_jpy, promotion_tax_jpy, points_jpy, warehouse_damage_jpy, warehouse_lost_jpy, safe_t_jpy,
        refund_principal_jpy, reversal_reimbursement_jpy, misc_fee_jpy, other_fee_jpy, other_amount_jpy, profit_before_cogs_jpy
       from mart.finance_daily_range(1::smallint, $1, $2, $3::date, $4::date)
       order by economic_date_jst, seller_sku collate "C"`, [ms.mall, ms.scope, rg.from, rg.to])).rows;
    res.json({ mall: ms.mall, scope: ms.scope, from: rg.from, to: rg.to, rows: rows.map((r) => Object.fromEntries(Object.entries(r).map(([k, v]) => [k, k === 'date_jst' || k === 'seller_sku' ? v : Number(v)]))) });
  });
});

router.get('/order-finance/account-fees', requireSyncKey, async (req, res) => {
  const ms = financeMallScopeOf(req); if (!ms) return res.status(400).json({ error: 'mall / scope are required' });
  const rg = financeRangeOf(req, 800); if (rg.error) return res.status(400).json({ error: rg.error });
  await withPg(res, async (client) => {
    await client.query(`set statement_timeout = '120s'`);
    const rows = (await client.query(`select month_start_jst::text as month_start_jst, fee_type, amount_jpy, row_count from mart.v_finance_account_fees_monthly
       where company_id = 1 and mall = $1 and scope_key = $2 and month_start_jst between date_trunc('month', $3::date) and $4::date order by 1, 2`, [ms.mall, ms.scope, rg.from, rg.to])).rows;
    res.json({ mall: ms.mall, scope: ms.scope, rows: rows.map((r) => ({ ...r, amount_jpy: Number(r.amount_jpy), row_count: Number(r.row_count) })) });
  });
});

router.get('/order-finance/uncovered', requireSyncKey, async (req, res) => {
  const ms = financeMallScopeOf(req); if (!ms) return res.status(400).json({ error: 'mall / scope are required' });
  await withPg(res, async (client) => {
    const rows = (await client.query(`select reason, count(*)::int as n, min(economic_date_jst)::text as first_date, max(economic_date_jst)::text as last_date
       from mart.v_order_finance_uncovered where company_id = 1 and mall = $1 and scope_key = $2 group by reason order by reason`, [ms.mall, ms.scope])).rows;
    res.json({ mall: ms.mall, scope: ms.scope, rows });
  });
});

/**
 * Amazon の利益の mart (D7b-3・0049。設計 = AI_reference『CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』§3.5・§3.6) の読む口:
 *   GET /apps/company-db/sync/amazon-profit/daily?mall=amazon&scope=jp&from&to    mart.amazon_profit_daily_range      (日 × 出品の寄与の利益。構成と出品の結びつけは今のマスタ = master_basis current)
 *   GET /apps/company-db/sync/amazon-profit/totals?mall=amazon&scope=jp&from&to   mart.amazon_profit_day_totals_range (日 / 暦の月 / 期間の中の月の小計 / 期間の合計 = row_kind)
 *   契約 = from <= to・/daily は 1 回 93 日まで (長い期間は日の範囲で区切る)・/totals は 400 日まで (両端を含む)・今は amazon / jp だけ (400)・statement_timeout 120s・0049 の前は 409 not_migrated
 *   🚨 設計書 (13 §4) の `/apps/company-db/api/...` ではなく、Company DB の既存の読む口の流儀 (/sync の下 + x-sync-key) にそろえた (#1559 Codex R1 Medium 4)
 *   JSON: ID と ID の配列 (listing_id・*_ids・世代・版) = 10 進の文字列 / 円 (*_jpy)・個数・件数 = 数 /
 *         金額の numeric (税抜・広告費・0 と仮定・手数料の後) = 小数 2 桁の文字列 / units_*_unrounded (丸める前の返品数) = 小数 最大 6 桁の文字列 (返品なし = "0") /
 *         日付 = YYYY-MM-DD / 時刻 = UTC の ISO (ミリ秒)。列は関数の戻りの定義 (pg_proc) から作る = 関数に列を足しても受け口を直さなくてよい
 *   🚨 決済のそろい (D7b-1b) の前は正式な利益 (contribution_* / profit_after_account_fees_*) は全部 null。0 と仮定の値 (…_assuming_incomplete_zero_…) を「利益」と読まない
 */
const PROFIT_ID_COLS = new Set(['listing_id', 'observed_generation', 'finance_coverage_generation', 'finance_source_revision']);
const PROFIT_FNS = {
  // /daily は行が多い (1 日 数百の出品) = 1 回の要求は 93 日まで (長い期間は日の範囲で区切って何回かに分けて読む・#1559 Codex R1 Medium 1)。
  // /totals は日 + 月 + 合計だけ (400 日でも 400 行と少し) = 関数の契約どおり 400 日まで
  daily: { fn: 'mart.amazon_profit_daily_range', maxDays: 93, order: `order by r.economic_date_jst, r.listing_id is null, r.listing_id, r.seller_sku_norm collate "C"` },
  totals: { fn: 'mart.amazon_profit_day_totals_range', maxDays: 400, order: `order by case r.row_kind when 'day' then 1 when 'range_total' then 3 else 2 end, r.period_from` },
};
/** 関数の戻りの列 (RETURNS TABLE = proargmodes 't') から select の式と JS の直し方を作る */
const profitSelect = async (client, fn) => {
  const cols = (await client.query(`select a.name, format_type(a.typ, null) as type
      from pg_proc p cross join lateral unnest(p.proargnames, p.proallargtypes, p.proargmodes::text[]) with ordinality as a(name, typ, mode, ord)
     where p.oid = $1::regprocedure and a.mode = 't' order by a.ord`, [`${fn}(smallint,text,text,date,date)`])).rows;
  if (!cols.length || cols.some((c) => !/^[a-z_][a-z0-9_]*$/.test(c.name))) throw new Error(`unexpected result columns of ${fn}`);
  const exprs = cols.map(({ name, type }) => {
    const q = `r."${name}"`;
    if (type === 'date' || type === 'numeric' || (type === 'bigint' && PROFIT_ID_COLS.has(name))) return `${q}::text as "${name}"`;
    if (type === 'date[]' || type === 'bigint[]') return `${q}::text[] as "${name}"`;   // bigint[] は全部 ID の配列
    if (type === 'timestamp with time zone') return `to_char(${q} at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') as "${name}"`;
    return `${q} as "${name}"`;
  });
  const numeric = new Set(cols.filter((c) => c.type === 'integer' || (c.type === 'bigint' && !PROFIT_ID_COLS.has(c.name))).map((c) => c.name));
  return { list: exprs.join(', '), fix: (row) => Object.fromEntries(Object.entries(row).map(([k, v]) => [k, v != null && numeric.has(k) ? Number(v) : v])) };
};
for (const [kind, spec] of Object.entries(PROFIT_FNS)) {
  router.get(`/amazon-profit/${kind}`, requireSyncKey, async (req, res) => {
    const ms = financeMallScopeOf(req); if (!ms) return res.status(400).json({ error: 'mall / scope are required' });
    if (ms.mall !== 'amazon' || ms.scope !== 'jp') return res.status(400).json({ error: 'only mall=amazon & scope=jp for now (the listing resolution does not look at shop_code)' });
    const rg = financeRangeOf(req, spec.maxDays); if (rg.error) return res.status(400).json({ error: rg.error });
    await withPg(res, async (client) => {
      if (!(await client.query(`select to_regprocedure($1) is not null as ok`, [`${spec.fn}(smallint,text,text,date,date)`])).rows[0].ok) {
        return res.status(409).json({ error: 'not_migrated', detail: 'migration 0049 (Amazon profit mart) is not applied' });
      }
      const sel = await profitSelect(client, spec.fn);
      await client.query(`set statement_timeout = '120s'`);
      const rows = (await client.query(`select ${sel.list} from ${spec.fn}(1::smallint, $1, $2, $3::date, $4::date) r ${spec.order}`, [ms.mall, ms.scope, rg.from, rg.to])).rows;
      res.json({ mall: ms.mall, scope: ms.scope, from: rg.from, to: rg.to, master_basis: 'current', rows: rows.map(sel.fix) });
    });
  });
}

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

/**
 * 広告費の日次 (Company DB構想 11 の ②。本体 = ingest/ad-spend.mjs、送り手 = apps/company-db/push/ad-spend.mjs)。
 *   POST /apps/company-db/sync/ad-spend/day     { mall, scope, ad_type, date_jst, generation, report_id, checksum, rows: [...] }
 *     → { status: 'applied' | 'same' | 'refreshed' | 'stale', rows, resolved, unresolved_sku, run_id, checksum }。同じ世代で違う内容 = 409
 *   GET  /apps/company-db/sync/ad-spend/status?mall&scope&ad_type&from&to  → { days: [{ date_jst, generation, report_id, checksum, row_count, cost_total }] }
 *   POST /apps/company-db/sync/ad-spend/relink  → { relinked, unresolved_sku } (マスタが後から増えた SKU の行を出品に結び直す)
 */
router.post('/ad-spend/day', requireSyncKey, stockJson, stockParserError, async (req, res) => {
  const url = process.env.COMPANY_DB_URL;
  if (!url) return res.status(503).json({ error: 'COMPANY_DB_URL not configured' });
  let client;
  const t0 = Date.now();
  const tag = `${String(req.body && req.body.mall).slice(0, 20)} ${String(req.body && req.body.date_jst).slice(0, 12)}`;
  try {
    client = await pgClientFactory(url);
    await client.query(`set statement_timeout = '40s'; set lock_timeout = '10s'; set idle_in_transaction_session_timeout = '60s'`);
    const r = await ingestAdSpendDay(pgAdapter(client), req.body, { host: 'render', log: (m) => console.log(`[company-db ad-spend] ${m}`) });
    res.json({ ...r, ms: Date.now() - t0 });
  } catch (e) {
    const status = e.code === 'BAD_REQUEST' ? 400 : e.code === 'CONFLICT' ? 409 : e.code === 'LOCKED' ? 503 : 500;
    if (status >= 500) console.error(`[company-db ad-spend] ${tag} FAILED (${status}, ${Date.now() - t0} ms): ${e.message}`);
    res.status(status).json({ error: String(e.message).slice(0, 300), code: e.code || null });
  } finally { if (client) { try { await client.end(); } catch { /* 閉じられなくても応答は出す */ } } }
});
router.get('/ad-spend/status', requireSyncKey, async (req, res) => {
  await withPg(res, async (client) => {
    try {
      const q = (k) => String(req.query[k] || '');
      res.json({ days: await adSpendStatus(pgAdapter(client), { mall: q('mall'), scope: q('scope'), adType: q('ad_type'), from: q('from'), to: q('to') }) });
    } catch (e) {
      if (e.code === 'BAD_REQUEST') return res.status(400).json({ error: e.message });
      throw e;
    }
  });
});
router.post('/ad-spend/relink', requireSyncKey, async (req, res) => {
  await withPg(res, async (client) => { await client.query(`set statement_timeout = '120s'`); res.json(await relinkAdSpend(pgAdapter(client))); });
});

/**
 * 観測の原価 (D7b-2。本体 = ingest/sku-cost-observed.mjs、送り手 = apps/company-db/push/sku-cost-observed.mjs、受け皿 = 0046)。
 *   POST /apps/company-db/sync/sku-cost-observed   { source, generation, checksum, row_count, unresolved_code_count, ambiguous_code_count, rows: [...] }
 *     → { status: 'applied' | 'same' | 'stale', generation, checksum, rows, observed_load_id, run_id }。全部 = 1 要求 = 1 取引で入れ替える。
 *       同じ世代で manifest が違う = 409 CONFLICT / Render に無い SKU の商品コード = 409 SKU_UNRESOLVED / 別の取込が走っている = 503 LOCKED
 *   GET  /apps/company-db/sync/sku-cost-observed/status      → { source, load: { generation, checksum, row_count, unresolved_code_count, ambiguous_code_count, … } | null, rows, skus }。0046 の適用前は 409 { error: 'not_migrated' }
 *   GET  /apps/company-db/sync/sku-cost-observed/sku-codes?after&limit → { keys: [code_norm], next } (送り手が商品コードを結べるか決める)
 * 🚨 body の parse は鍵の検査の後 (server.js の共通 parser はこの path を素通りさせる)。1 回で 2 万行 ≒ 5MB = 伝票と同じ 12MB・圧縮なしの parser
 */
router.post('/sku-cost-observed', requireSyncKey, shipmentsJson, shipmentsParserError, async (req, res) => {
  const url = process.env.COMPANY_DB_URL;
  if (!url) return res.status(503).json({ error: 'COMPANY_DB_URL not configured' });
  let client;
  const t0 = Date.now();
  try {
    client = await pgClientFactory(url);
    await client.query(`set statement_timeout = '60s'; set lock_timeout = '10s'; set idle_in_transaction_session_timeout = '90s'`);
    const r = await ingestSkuCostObserved(pgAdapter(client), req.body, { host: 'render', log: (m) => console.log(`[company-db sku-cost-observed] ${m}`) });
    res.json({ ...r, ms: Date.now() - t0 });
  } catch (e) {
    const status = e.code === 'BAD_REQUEST' ? 400 : (e.code === 'CONFLICT' || e.code === 'SKU_UNRESOLVED') ? 409 : e.code === 'LOCKED' ? 503 : 500;
    if (status >= 500) console.error(`[company-db sku-cost-observed] FAILED (${status}, ${Date.now() - t0} ms): ${e.message}`);
    res.status(status).json({ error: String(e.message).slice(0, 300), code: e.code || null });
  } finally { if (client) { try { await client.end(); } catch { /* 閉じられなくても応答は出す */ } } }
});
router.get('/sku-cost-observed/status', requireSyncKey, async (req, res) => {
  await withPg(res, async (client) => {
    // 0046 の適用前 = 409 not_migrated (送り手は「⚠️ 0046 が未適用」で送らない = マージから migrate までの朝を ❌ にしない。売上日次の 0021 と同じ流儀)
    if ((await client.query(`select to_regclass('core.sku_cost_observed_loads') is not null as ok`)).rows[0].ok !== true) return res.status(409).json({ error: 'not_migrated', detail: 'migration 0046 (core.sku_cost_observed) is not applied' });
    res.json(await skuCostObservedStatus(pgAdapter(client)));
  });
});
router.get('/sku-cost-observed/sku-codes', requireSyncKey, async (req, res) => {
  const limitRaw = req.query.limit === undefined ? 20000 : Number(req.query.limit);
  if (!Number.isInteger(limitRaw)) return res.status(400).json({ error: 'limit must be an integer' });
  await withPg(res, async (client) => { res.json(await skuCodeNorms(pgAdapter(client), { after: String(req.query.after || ''), limit: limitRaw })); });
});

router.get(['/shipments/receipt', '/orders/receipt', '/order-finance/receipt'], requireSyncKey, async (req, res) => {
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
