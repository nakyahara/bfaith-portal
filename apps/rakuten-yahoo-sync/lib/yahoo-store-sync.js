/**
 * Phase E-7-a: Yahoo baseline 確立 service。
 * ローカル RYS yahoo-store-sync.js を ES module 化、 bfaith-portal の VPS proxy 経由 + sync_runs +
 * yahoo_baseline_state 正本に書き換え (Codex R2 Critical-6)。
 *
 * 提供:
 *   - CANONICAL_QUERIES, CANONICAL_QUERY_HASH    — Yahoo myItemList の網羅 query 集合 (a-z + 0-9 = 36 本)
 *   - querySetHash, envOverrideQueries           — テスト / 検証用 helper
 *   - syncYahooBaseline({ db, deps, ... })       — baseline run 1 回分の実行
 *   - assertYahooBaselineComplete(db, { ... })   — 候補生成 / publish-executor 冒頭で呼ぶ fail-closed guard
 *
 * 設計原則 (Codex R1/R2 確定):
 *   - YAHOO_DIFF_QUERIES env override が active な間は baseline 確立も write も拒否 (検証用迂回路防止)
 *   - sync_runs はランごとに INSERT、 yahoo_baseline_state は 「最新の正本」 で UPSERT
 *   - totalResultsAvailable と取得件数の不一致は baseline incomplete (Codex R2 A-6)
 *   - 直近 run で last_seen_at 更新されなかった既存行 (stale_rows_count) > 0 なら baseline incomplete
 *   - assertYahooBaselineComplete は yahoo_baseline_state を見る (sync_runs.establishes_baseline を見ない)
 */

import crypto from 'node:crypto';

import { iterateMyItemList } from './yahoo-myitemlist-proxy.js';

const DEFAULT_BASELINE_MAX_AGE_HOURS = 30;

// canonical full query set: 完全性 (baseline) の唯一の定義。 env override に影響されない固定値。
// myItemList は query 部分一致。 ItemCode は英数字基本なので a-z + 0-9 で網羅。
const CANONICAL_QUERIES = 'abcdefghijklmnopqrstuvwxyz0123456789'.split('');

function querySetHash(queries) {
  return crypto.createHash('sha256').update([...queries].sort().join(',')).digest('hex');
}

const CANONICAL_QUERY_HASH = querySetHash(CANONICAL_QUERIES);

// 実行時 override 専用 (検証 / 段階移行用)。 設定されている間は baseline 確立も write も禁止する。
function envOverrideQueries() {
  const env = process.env.YAHOO_DIFF_QUERIES;
  if (env && env.trim()) {
    return env.split(',').map((s) => s.trim()).filter(Boolean);
  }
  return null;
}

function toBool(v) {
  if (v === undefined || v === null) return false;
  if (typeof v === 'boolean') return v;
  const s = String(v).toLowerCase().trim();
  return s === 'true' || s === '1' || s === 'yes';
}

function isoNow() {
  return new Date().toISOString();
}

/**
 * 1 query 分を全件取得して { items: Map<ItemCode, info>, perQuery: { query, matched, pages, completeness } } を返す。
 * R2 High A-6: totalResultsAvailable と取得件数の不一致を completeness で記録、 baseline incomplete 判定に使う。
 *
 * Codex E-7-a R2 High-2: totalResultsAvailable に number/integer/non-negative validation。
 *   proxy が壊れて null / "100" / -1 等を返した場合は baseline 確立しない (incomplete 扱い)。
 */
function validateTotalAvailable(totalAvailable, query) {
  if (totalAvailable === null || totalAvailable === undefined) return false;
  if (typeof totalAvailable !== 'number') return false;
  if (!Number.isInteger(totalAvailable)) return false;
  if (totalAvailable < 0) return false;
  return true;
}

async function fetchYahooItemsForQuery(query, opts = {}) {
  const items = new Map();
  let pages = 0;
  let firstValidTotal = null;
  let totalReturned = 0;
  // Codex R3 High-2 + R4 High-1: page consistency 全網羅。
  //   1. 全 page で totalResultsAvailable を validate
  //   2. first valid total から変動なし
  //   3. page.totalResultsReturned === page.items.length (報告 vs 実数)
  //   4. page.firstResultPosition === offset + 1 (offset 整合)
  //   5. page.items.length <= pageSize (proxy が pageSize 超えて返さない)
  //   6. offset + items.length <= firstValidTotal (over-run 検出)
  //   7. 同 query 内で ItemCode 重複なし (proxy が同 page 二度返し検出)
  //   いずれか違反 → perPageInvalid (unknown 扱い、 baseline 確立しない)
  let perPageInvalid = false;
  const pageSize = opts.pageSize ?? 100;
  for await (const page of iterateMyItemList(query, opts)) {
    pages += 1;
    // (1) total validation
    if (validateTotalAvailable(page.totalResultsAvailable, query)) {
      if (firstValidTotal === null) {
        firstValidTotal = page.totalResultsAvailable;
      } else if (page.totalResultsAvailable !== firstValidTotal) {
        // (2) page 間で total 変動 = 不整合
        perPageInvalid = true;
      }
    } else {
      perPageInvalid = true;
    }
    // (3) totalResultsReturned: integer / non-negative / === items.length (Codex R5 strict)
    if (!Number.isInteger(page.totalResultsReturned) ||
        page.totalResultsReturned < 0 ||
        page.totalResultsReturned !== page.items.length) {
      perPageInvalid = true;
    }
    // (4) firstResultPosition: integer / === offset + 1 (Codex R5 strict)
    if (!Number.isInteger(page.firstResultPosition) ||
        page.firstResultPosition !== page.offset + 1) {
      perPageInvalid = true;
    }
    // (5) page.items.length <= pageSize
    if (page.items.length > pageSize) {
      perPageInvalid = true;
    }
    // (6) offset + items.length <= firstValidTotal (over-run)
    if (firstValidTotal !== null && page.offset + page.items.length > firstValidTotal) {
      perPageInvalid = true;
    }
    // (7) ItemCode validation (Codex R5 High-1): 欠落 / 空文字 / 非 string は invalid 扱い
    //     proxy contract 上 ItemCode は必須。 invalid item は map に入れない。
    // (8) ItemCode 重複 = proxy が同 page を 2 度返した疑い
    for (const it of page.items) {
      const code = it.ItemCode;
      if (typeof code !== 'string' || code.trim() === '') {
        perPageInvalid = true;
        continue;
      }
      if (items.has(code)) {
        perPageInvalid = true;
      } else {
        items.set(code, { name: it.Name ?? null, hasSubCode: toBool(it.HasSubCode) });
      }
    }
    totalReturned += page.items.length;
  }
  // 全 page valid + 変動なし + returned >= total なら complete、 そうでなければ unknown/partial
  const completeness =
    perPageInvalid || firstValidTotal === null ? 'unknown' :
    totalReturned >= firstValidTotal ? 'complete' : 'partial';
  return {
    items,
    pages,
    matched: items.size,
    returned: totalReturned,
    totalAvailable: firstValidTotal,
    completeness,
  };
}

/**
 * 全 query を回して merged 商品 map を返す。
 */
async function fetchAllYahooItems(opts = {}) {
  const qs = opts.queries || envOverrideQueries() || CANONICAL_QUERIES;
  const merged = new Map();
  const perQuery = [];
  for (const q of qs) {
    const result = await fetchYahooItemsForQuery(q, opts);
    for (const [code, info] of result.items) {
      if (!merged.has(code)) merged.set(code, info);
    }
    perQuery.push({
      query: q,
      matched: result.matched,
      returned: result.returned,
      totalAvailable: result.totalAvailable,
      completeness: result.completeness,
      pages: result.pages,
    });
    if (opts.onProgress) opts.onProgress({ query: q, matched: result.matched, uniqueSoFar: merged.size, completeness: result.completeness });
  }
  return { items: merged, perQuery };
}

// ────────────────── DB 書き込み helpers ──────────────────

function insertSyncRun(db, { syncName, triggeredBy, triggerRequestId = null, baselineQueryHash = null, baselineQueryCount = null, startedAt = isoNow(), runLeaseExpiresAt = null, runToken = null }) {
  const r = db.prepare(`
    INSERT INTO sync_runs (sync_name, started_at, status, triggered_by, trigger_request_id, baseline_query_hash, baseline_query_count, run_lease_expires_at, run_token)
    VALUES (?, ?, 'running', ?, ?, ?, ?, ?, ?)
  `).run(syncName, startedAt, triggeredBy, triggerRequestId, baselineQueryHash, baselineQueryCount, runLeaseExpiresAt, runToken);
  return r.lastInsertRowid;
}

/**
 * Codex E-7-a R2 High-1: finishSyncRun は run_token 一致を要求する。
 *   stealStaleRunningRuns で別 run に lease を奪われていた場合、 旧 run の finish は 0 rows となり throw する。
 *   結果として「lease lost」 が呼び出し側で検出でき、 古い run が success に戻して二重 baseline 確立する事故を防ぐ。
 *
 * @throws {Error} 自分の run_token + running 状態に一致する row が無い場合 (= lease lost)
 */
function finishSyncRun(db, id, runToken, fields) {
  if (!runToken) throw new Error('finishSyncRun: runToken is required');
  const cols = [];
  const vals = [];
  for (const [k, v] of Object.entries(fields)) {
    cols.push(`${k} = ?`);
    vals.push(v);
  }
  cols.push("finished_at = ?");
  vals.push(isoNow());
  vals.push(id, runToken);
  const r = db.prepare(
    `UPDATE sync_runs SET ${cols.join(', ')} WHERE id = ? AND status = 'running' AND run_token = ?`
  ).run(...vals);
  if (r.changes !== 1) {
    const cur = db.prepare('SELECT id, status, run_token FROM sync_runs WHERE id = ?').get(id);
    const err = new Error(
      `finishSyncRun lease-lost: expected run_token=${runToken?.slice(0, 8)} running, found ` +
      `${JSON.stringify({ id: cur?.id, status: cur?.status, token_short: cur?.run_token?.slice(0, 8) })}`
    );
    err.code = 'LEASE_LOST';
    throw err;
  }
}

/**
 * Codex E-7-a R2 High-1: state 更新前に呼ぶ 「自分の lease がまだ生きてるか」 chec。
 */
function assertRunOwned(db, id, runToken) {
  const cur = db.prepare("SELECT status, run_token FROM sync_runs WHERE id = ?").get(id);
  if (!cur || cur.status !== 'running' || cur.run_token !== runToken) {
    const err = new Error(`assertRunOwned lease-lost: id=${id} cur=${JSON.stringify(cur)}`);
    err.code = 'LEASE_LOST';
    throw err;
  }
}

function syncYahooRegisteredItems(db, items, runStartedAt) {
  // Codex E-7-a R3 High-1: last_seen_at は monotonic upsert (古い run が新しい timestamp を上書きできないように)。
  //   WHERE excluded.last_seen_at >= yahoo_registered_items.last_seen_at は SQLite で UPSERT 条件として有効。
  const upsert = db.prepare(`
    INSERT INTO yahoo_registered_items (item_code, yahoo_item_code, has_sub_code, last_seen_at, source)
    VALUES (@code, @code, @hasSub, @ts, 'yahoo_existing')
    ON CONFLICT(item_code) DO UPDATE SET
      yahoo_item_code = excluded.yahoo_item_code,
      has_sub_code    = excluded.has_sub_code,
      last_seen_at    = excluded.last_seen_at
      WHERE excluded.last_seen_at >= yahoo_registered_items.last_seen_at
  `);
  let count = 0;
  for (const [code, info] of items) {
    upsert.run({ code, hasSub: info.hasSubCode ? 1 : 0, ts: runStartedAt });
    count += 1;
  }
  return count;
}

function upsertBaselineState(db, { baselineRunId, observedCount, queryHash, isComplete, establishedAt }) {
  const now = isoNow();
  db.prepare(`
    INSERT INTO yahoo_baseline_state (id, baseline_run_id, established_at, observed_count, query_hash, is_complete, updated_at)
    VALUES (1, @baselineRunId, @establishedAt, @observedCount, @queryHash, @isComplete, @now)
    ON CONFLICT(id) DO UPDATE SET
      baseline_run_id = excluded.baseline_run_id,
      established_at  = excluded.established_at,
      observed_count  = excluded.observed_count,
      query_hash      = excluded.query_hash,
      is_complete     = excluded.is_complete,
      updated_at      = excluded.updated_at
  `).run({ baselineRunId, observedCount, queryHash, isComplete: isComplete ? 1 : 0, establishedAt, now });
}

function getBaselineState(db) {
  return db.prepare('SELECT * FROM yahoo_baseline_state WHERE id = 1').get() || null;
}

// ────────────────── public API ──────────────────

/**
 * Yahoo baseline 1 run を実行する。
 *
 * @param {object} args
 * @param {Database} args.db                       — better-sqlite3 instance
 * @param {object}   args.deps                     — fetcher 依存 ({ callApi, delayMs, queries })
 * @param {'cron'|'manual'} args.triggeredBy
 * @param {string|null} args.triggerRequestId      — on-demand 冪等 key (任意)
 * @returns {Promise<object>} result with sync_run_id, establishedBaseline, etc.
 */
// Codex E-7-a R3 Other: 36 query * 数十秒で 30 分超える運用ケースに備える、 lease 90 分 + grace 15 分 (heartbeat 無し前提)。
//   heartbeat 実装は将来検討、 一旦は十分 conservative な値で safety を優先。
const DEFAULT_RUN_LEASE_MS = 90 * 60 * 1000;
const STALE_RUNNING_GRACE_MS = 15 * 60 * 1000;

/**
 * Codex E-7-a R1 High-2: crash 後の永久ロック対策。
 * 同一 sync_name で run_lease_expires_at が切れた running 行を failed に finalize する。
 *
 * Codex R2 Medium-1: STALE_RUNNING_GRACE_MS で grace を適用 (lease 切れた直後の race を緩衝)。
 *   steal 条件は `run_lease_expires_at < now - GRACE`。
 */
function stealStaleRunningRuns(db, syncName) {
  const now = Date.now();
  const cutoff = new Date(now - STALE_RUNNING_GRACE_MS).toISOString();
  const nowIso = new Date(now).toISOString();
  const r = db.prepare(`
    UPDATE sync_runs
       SET status = 'failed',
           finished_at = ?,
           error_message = COALESCE(error_message, '') ||
             '[E-7-a lease-steal] run_lease_expires_at + grace expired (' || COALESCE(run_lease_expires_at, 'null') || ' < ' || ? || ')'
     WHERE sync_name = ?
       AND status = 'running'
       AND (run_lease_expires_at IS NULL OR run_lease_expires_at < ?)
  `).run(nowIso, cutoff, syncName, cutoff);
  return r.changes;
}

async function syncYahooBaseline({ db, deps = {}, triggeredBy = 'cron', triggerRequestId = null, runLeaseMs = DEFAULT_RUN_LEASE_MS }) {
  // Codex E-7-a R1 Critical-1: env override が active な間は baseline write を一切しない。
  // baseline_state を作らないだけでは不足 (sync_runs/yahoo_registered_items への書き込みが残る)。
  if (envOverrideQueries()) {
    const err = new Error(
      'YAHOO_DIFF_QUERIES override is active — refusing baseline writes (sync_runs / yahoo_registered_items).'
    );
    err.statusCode = 412;
    throw err;
  }
  const override = deps.queries; // テスト用の deps.queries のみ受け付ける (env override は上で除外済)
  const usingOverride = !!override;
  const qs = override || CANONICAL_QUERIES;
  const queryHash = querySetHash(qs);
  const isFullQuerySet = !usingOverride && queryHash === CANONICAL_QUERY_HASH;

  const runStartedAt = isoNow();
  const runLeaseExpiresAt = new Date(Date.now() + runLeaseMs).toISOString();
  // Codex R2 High-1: 自分の run を識別する token (UUID)。 finishSyncRun はこれの一致を要求する
  const runToken = crypto.randomUUID();

  // R1 High-2 + R2 Medium-1: 開始前に同名 stale running (lease+grace 切れ) を回収
  stealStaleRunningRuns(db, 'yahoo_baseline');

  const syncRunId = insertSyncRun(db, {
    syncName: 'yahoo_baseline',
    triggeredBy,
    triggerRequestId,
    baselineQueryHash: queryHash,
    baselineQueryCount: qs.length,
    startedAt: runStartedAt,
    runLeaseExpiresAt,
    runToken,
  });

  try {
    const { items, perQuery } = await fetchAllYahooItems({ ...deps, queries: qs });

    // Codex E-7-a R3 High-1: post-fetch mutation phase 全体を 1 transaction で囲む。
    //   - assertRunOwned → upsert → staleRows → establish 判定 → assertRunOwned → state upsert → finishSyncRun
    //   - SQLite transaction 内なら writes は atomic、 他 connection は wait する。
    //   - assertRunOwned を 2 回挟むことで 「mid-write で別 run に steal された」 race も検出。
    const writeTx = db.transaction(() => {
      assertRunOwned(db, syncRunId, runToken);

      const inserted = syncYahooRegisteredItems(db, items, runStartedAt);

      const staleRows = db.prepare(
        `SELECT COUNT(*) AS c FROM yahoo_registered_items
         WHERE source = 'yahoo_existing' AND last_seen_at < ?`
      ).get(runStartedAt).c;

      const anyPartial = perQuery.some((q) => q.completeness === 'partial');
      // Codex R2 High-2: 'unknown' も 'complete' とは見なさない、 全 query が 'complete' で確立可。
      const anyIncomplete = perQuery.some((q) => q.completeness !== 'complete');
      const establishesBaseline = isFullQuerySet && staleRows === 0 && !anyIncomplete;

      if (establishesBaseline) {
        // 2 度目の assertRunOwned (state upsert 直前)
        assertRunOwned(db, syncRunId, runToken);
        upsertBaselineState(db, {
          baselineRunId: syncRunId,
          observedCount: items.size,
          queryHash,
          isComplete: true,
          establishedAt: runStartedAt,
        });
      }

      finishSyncRun(db, syncRunId, runToken, {
        status: 'success',
        item_count_observed: items.size,
        stale_rows_count: staleRows,
        establishes_baseline: establishesBaseline ? 1 : 0,
        per_query_json: JSON.stringify(perQuery),
      });

      return { inserted, staleRows, anyPartial, anyIncomplete, establishesBaseline };
    });

    const { inserted, staleRows, anyPartial, anyIncomplete, establishesBaseline } = writeTx();

    return {
      syncRunId,
      itemsObserved: items.size,
      itemsUpserted: inserted,
      staleRows,
      perQuery,
      isFullQuerySet,
      usingOverride,
      anyPartial,
      anyIncomplete,
      establishesBaseline,
      baselineState: getBaselineState(db),
    };
  } catch (e) {
    // LEASE_LOST の場合は finish しても 0 rows、 catch 内で throw を二重させず無視する。
    try {
      finishSyncRun(db, syncRunId, runToken, {
        status: 'failed',
        error_message: String(e.message || e).slice(0, 4000),
      });
    } catch (e2) {
      if (e2.code !== 'LEASE_LOST') throw e2;
      // lease 喪失中の他 process に任せる、 元 throw を継続伝播
    }
    throw e;
  }
}

/**
 * 書き込み / 差分検出フェーズの入口で呼ぶ fail-closed guard。
 * baseline 未確立 / incomplete / env override 中 / baseline が古い = 禁止。
 *
 * @throws {Error} fail-closed 時。 error.statusCode = 412 (Precondition Failed)
 */
function assertYahooBaselineComplete(db, { maxAgeHours = DEFAULT_BASELINE_MAX_AGE_HOURS } = {}) {
  if (envOverrideQueries()) {
    const err = new Error(
      'YAHOO_DIFF_QUERIES override is active — baseline write guard is disabled for safety. ' +
      'Unset it and run a full syncYahooBaseline before writing.'
    );
    err.statusCode = 412;
    throw err;
  }
  const state = getBaselineState(db);
  if (!state || !state.is_complete) {
    const err = new Error(
      'Yahoo baseline not established (or last sync was incomplete). ' +
      'Run a full syncYahooBaseline (canonical query set, staleRows=0) before any write operation.'
    );
    err.statusCode = 412;
    throw err;
  }
  if (state.query_hash !== CANONICAL_QUERY_HASH) {
    const err = new Error(
      `Yahoo baseline query set changed (baseline=${state.query_hash?.slice(0, 12)} canonical=${CANONICAL_QUERY_HASH.slice(0, 12)}). ` +
      'Re-run a full syncYahooBaseline before writing.'
    );
    err.statusCode = 412;
    throw err;
  }
  const establishedTs = Date.parse(state.established_at);
  if (Number.isNaN(establishedTs)) {
    const err = new Error(`Yahoo baseline established_at is invalid (${state.established_at}).`);
    err.statusCode = 412;
    throw err;
  }
  const ageMs = Date.now() - establishedTs;
  if (ageMs > maxAgeHours * 3600 * 1000) {
    const err = new Error(
      `Yahoo baseline is stale (established=${state.established_at}, ageHours=${Math.round(ageMs / 3600000)}, maxAgeHours=${maxAgeHours}). ` +
      'Re-run a full syncYahooBaseline before proceeding.'
    );
    err.statusCode = 412;
    throw err;
  }
}

export {
  CANONICAL_QUERIES,
  CANONICAL_QUERY_HASH,
  DEFAULT_BASELINE_MAX_AGE_HOURS,
  querySetHash,
  envOverrideQueries,
  fetchAllYahooItems,
  syncYahooBaseline,
  assertYahooBaselineComplete,
  // for testing
  getBaselineState,
  toBool,
  finishSyncRun,
  assertRunOwned,
  stealStaleRunningRuns,
};
