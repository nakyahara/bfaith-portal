#!/usr/bin/env node
/**
 * amazon-finance-coverage-run.js — Amazon の決済の取込と Company DB の財務の送信と「決済のそろい」(coverage) を 1 回として回す coordinator (D7b-1b-3)
 *
 * 設計 = AI_reference システム設計/CompanyDB構想/13_Amazon利益のmart_設計_20260930.md v26 §3.1 (coordinator・lease・source_revision・一覧の窓の連続・
 *   最新の観測・CANCELLED・期間の型・receipt digest・manifest)・D-65 (初期の印)・D-66 (決済ごとに採る文書の版を 1 つ)
 *   Render 側 = PR #1561 (POST /apps/company-db/sync/order-finance/coverage・GET …/coverage/status・財務の chunk の coverage_generation / run_token)
 *
 * 1 回の順 (lease を取る・放すのは この親だけ。子のステップ (取込・送り手) は lease を引数で受け、自分では取らない):
 *   ⓪ lease (warehouse.db の 1 行) を取る = 持ち主の判定は retry-lock.js と同じ (pid が生きている node で開始が lease より前でない)・心拍の期限では奪わない
 *   ① 過去の行に文書の版を付ける (backfill・版の要約) — lease を取引の中で確かめる
 *   ② Company DB の mode を決める: 財務のバックフィルの完了印が無い = ingest_only (取込だけ) / Render に 0051 が無い = legacy (今までの送り方・coverage なし) / coverage
 *   ③ (coverage) Render の今の世代を読み、台帳 (company-db-push.db) の世代を少なくともそこまで進め、新しい世代と token を **HTTP の前に台帳と lease に保存**
 *      → Render の coverage を updating (失敗なら取込を始めない = 生の表を書く前に無効にする。R16 H2)
 *   ④ 決済の取込 = 手で積んだファイル (amazon-settlement-manual-file.js) → SP-API の一覧と取込 (fetch-amazon-settlements.js)。
 *      生の表の取引はどれも「lease が自分の世代・token のまま」を確かめてから書く。一覧の回には世代・token・最新の初期の印の epoch を書く
 *   ⑤ 送り手 (amazon-finance.mjs) = 渡された世代・token を全部の chunk に付ける。coverage の回は --full (全部の注文を変換 = receipt digest)。
 *      走査の同じ読み取りの取引の中で manifest を計算 (amazon-finance-coverage.js)・送れた注文の「読み直す注文」を R 以下だけ消す
 *   ⑥ 完成の判定 (completionBlockers) → complete の直前に source_revision を読み直す (R と違えば送らない)・lease がまだ自分のものか確かめる → complete
 *      (1 回 30 秒・3 回まで)。一覧・合計・receipt digest・版・採った文書・policy の指紋が全部そろったときだけ
 *   途中で落ちる・PC が止まる = Render は updating のまま (fail-closed = 正式な利益は null)
 *
 * 使い方 (daily-sync・retry と同じ):
 *   node apps/warehouse/amazon-finance-coverage-run.js                 → 1 回 (取込 + 送信 + coverage)
 *   node apps/warehouse/amazon-finance-coverage-run.js --dry-run       → 書かない・送らない (取込は数えるだけ・送り手は変換と manifest の判定だけ・Render は読むだけ)
 *   node apps/warehouse/amazon-finance-coverage-run.js --source v1     → 旧 V1 レポートで取る (11/11 まで。V1 の一覧では coverage は complete にならない = 必須は V2)
 * 終了コード: 0 = 済んだ (complete / 印が無いなど人が直すまでの ⚠️ / 取込だけ) / 1 = 失敗 (retry) / 3 = 取り込めない V2 がある (規則を足す)
 * env: DATA_DIR (必須)・RENDER_MIRROR_URL / MIRROR_SYNC_KEY・CDB_DB_LIMIT_BYTES (送るとき)・SP-API の鍵
 */
import 'dotenv/config';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import Database from 'better-sqlite3';
import { initDB, getDB } from './db.js';
import { runSettlementFetch, spClients, SOURCES } from './fetch-amazon-settlements.js';
import * as V from './amazon-settlement-versions.js';
import { ingestQueuedManualFiles } from './amazon-settlement-manual-file.js';
import { evaluateCoverage, completionBlockers, policyOrigin } from './amazon-finance-coverage.js';
import { isAliveNodeSince } from './retry-lock.js';
import { amazonFinanceDailyArgs } from './amazon-finance-months.js';
import { openLedger } from '../company-db/push/ledger.mjs';
import { pushAmazonFinance, FINANCE_KIND, META, retryStore, capacityFromEnv } from '../company-db/push/amazon-finance.mjs';
import { FINANCE_MALL, FINANCE_SCOPE, FINANCE_SOURCE } from '../company-db/push/amazon-finance-transform.mjs';
import { syncBase } from '../company-db/push/ne-shipments.mjs';
import { getJson, DEFAULT_CHUNK, MAX_CHUNK } from '../company-db/push/pipeline.mjs';
import { writeEvidence } from '../company-db/push/evidence.mjs';
import { validateCoverageManifest, coverageRequestHash } from '../company-db/finance/coverage-manifest.mjs';

export const COMPANY_ID = 1;
export const STATUS_PATH = `/order-finance/coverage/status?mall=${FINANCE_MALL}&scope=${FINANCE_SCOPE}&source=${FINANCE_SOURCE}`;
export const COVERAGE_PATH = '/order-finance/coverage';
export const LEDGER_META = { generation: 'coverage_generation', run: 'coverage_run' };
export const COVERAGE_POST_TIMEOUT_MS = 30000;
export const COVERAGE_POST_ATTEMPTS = 3;
const defaultSleep = (ms) => new Promise((r) => setTimeout(r, ms));
const todayJst = () => new Date(Date.now() + 9 * 3600e3).toISOString().slice(0, 10);

/** coverage の POST (updating / complete)。1 回 30 秒・3 回まで (5xx・通信の失敗・503 LOCKED は間を空けて送り直す・4xx は止める) */
export async function postCoverage(fetchImpl, { base, syncKey, body, sleep = defaultSleep, log = () => {}, attempts = COVERAGE_POST_ATTEMPTS, timeoutMs = COVERAGE_POST_TIMEOUT_MS }) {
  let last = null;
  for (let i = 1; i <= attempts; i++) {
    try {
      const res = await fetchImpl(`${base}${COVERAGE_PATH}`, { method: 'POST', headers: { 'content-type': 'application/json', 'x-sync-key': syncKey }, body: JSON.stringify(body), signal: AbortSignal.timeout(timeoutMs) });
      const text = await res.text();
      let json = null; try { json = JSON.parse(text); } catch { /* */ }
      if (res.ok && json && typeof json === 'object') return json;
      last = Object.assign(new Error(`coverage ${body.state} HTTP ${res.status} ${json && json.code ? `${json.code} ` : ''}${json && json.error ? json.error : text.slice(0, 200)}`), { status: res.status, code: json && json.code, body: json });
      if (res.status >= 400 && res.status < 500 && res.status !== 429) throw Object.assign(last, { fatal: true });
    } catch (e) {
      if (e.fatal) throw e;
      if (e !== last) last = e;
    }
    if (i < attempts) { log(`  coverage ${body.state} の送信に失敗 (${String(last && last.message).slice(0, 120)}) → 送り直す (${i}/${attempts})`); await sleep(5000 * i); }
  }
  throw last;
}

/** 最新の初期の印の epoch (無ければ null) */
const latestEpoch = (db) => db.prepare(`SELECT MAX(evidence_epoch) e FROM initial_marker_headers`).get().e ?? null;

/**
 * 1 回を回す (main と試験から)。戻り = { exitCode, summary, mode, generation, ingest, push, coverage, reasons }
 * deps: dataDir・source ('v2' | 'v1')・dryRun・fetchImpl・base・syncKey・sp / inventorySp / downloadTsv (SP-API)・now・isAlive (lease の持ち主の判定)・log・
 *       capacity (容量の見張り・既定 = env)・chunkSize・sleep・businessDate (legacy の曜日)・hooks { afterUpdating(db), beforeComplete(db) } (試験の差し込み口)
 */
export async function runCoverage({
  dataDir, source = 'v2', dryRun = false, fetchImpl = fetch, base, syncKey, sp, inventorySp = null, downloadTsv = null, now = () => new Date(),
  isAlive = isAliveNodeSince, log = console.log, capacity = undefined, chunkSize = DEFAULT_CHUNK, sleep = defaultSleep, businessDate = todayJst(), hooks = {},
  pid = process.pid, readonlyTimeoutMs = 60000,
}) {
  if (!dataDir) throw new Error('DATA_DIR が無い');
  if (!Object.hasOwn(SOURCES, source)) throw new Error(`source は v1 か v2: ${source}`);
  await initDB();
  const db = getDB();
  const out = { exitCode: 0, summary: '', mode: null, generation: null, ingest: null, push: null, coverage: null, reasons: [], manual: [] };
  const runId = `settlement-${now().getTime()}`;
  const ledger = openLedger(dataDir, { kind: FINANCE_KIND });
  const openReader = () => new Database(path.join(dataDir, 'warehouse.db'), { readonly: true, fileMustExist: true, timeout: readonlyTimeoutMs });
  const getOpts = { sleep, log };

  // ── dry-run = lease を取らない・書かない・Render は読むだけ ──
  if (dryRun) {
    try {
      out.mode = 'dry_run';
      out.ingest = await runSettlementFetch({ reportId: null, dryRun: true, source }, { db, sp, inventorySp, runId, downloadTsv, now });
      let status = null;
      try { status = await getJson(fetchImpl, `${base}${STATUS_PATH}`, syncKey, 'Render の決済のそろい', getOpts); } catch (e) { log(`[coverage] Render を読めない (dry-run は続ける): ${e.message}`); }
      const policy = status && status.policy ? status.policy : { rows: [{ period_from: '2026-01-01', period_to: null, source: FINANCE_SOURCE }], fingerprint: '0'.repeat(64) };
      const w = openReader();
      try {
        const r = await pushAmazonFinance({ warehouse: w, ledger, base, syncKey, mode: 'full', dryRun: true, fetchImpl, log, now, chunkSize,
          coverage: { generation: 0, runToken: 'dry-run-no-token', onScanSnapshot: ({ warehouse, stats, receipts }) => ({ ...evaluateCoverage(warehouse, { generation: 0, runToken: 'dry-run-no-token', policy, source: FINANCE_SOURCE, now: now(), receipts, sourceRevision: stats.sourceRevision }), sourceRevision: stats.sourceRevision }) } });
        out.push = r;
        const snap = r.scanSnapshot;
        out.reasons = snap ? snap.reasons : [];
        log(`[coverage] dry-run の判定: settlements_through ${snap && snap.diag.settlementsThrough} / 期待の集合 ${snap && snap.diag.expectedItems} / 理由 ${out.reasons.map((x) => x.code).join(', ') || 'なし (今回の一覧の回の条件は本番の回でだけ満たす)'}`);
      } finally { w.close(); }
      out.summary = `✅ Amazon 決済と財務 (dry-run): 書かない・送らない / 変換 ${out.push.scanned} 注文 / coverage の理由 ${out.reasons.length}`;
      return out;
    } finally { ledger.close(); }
  }

  // ── ⓪ lease ──
  const got = V.acquireCoverageLease(db, { isAlive, pid, now: now() });
  if (!got.ok) {
    ledger.close();
    out.exitCode = 1;
    out.summary = `❌ Amazon 決済と財務: 別の回が lease を持っている (pid ${got.held.pid}・開始 ${got.held.started_at}・世代 ${got.held.generation ?? '-'}) = 見送った (retry で回す)`;
    return out;
  }
  const lease = got.lease;
  if (got.recovered) log(`[coverage] 前の回 (pid ${got.recovered.pid}・開始 ${got.recovered.started_at}・世代 ${got.recovered.generation ?? '-'}) は死んでいた = lease を取った (その回の token の子はもう書けない)`);
  const check = (dbx) => V.assertLease(dbx, lease);
  try {
    // ── ① 過去の行の版 ──
    V.backfillDocumentVersions(db, { check, log });
    V.refreshStaleVersionDetails(db, { check, log });

    // ── ② mode ──
    const backfilled = ledger.getMeta(META.backfill) === '1';
    let status = null;
    if (!backfilled) out.mode = 'ingest_only';
    else {
      if (!base || !syncKey) throw new Error('送り先 (RENDER_MIRROR_URL) か MIRROR_SYNC_KEY が無い');
      try { status = await getJson(fetchImpl, `${base}${STATUS_PATH}`, syncKey, 'Render の決済のそろい', getOpts); out.mode = 'coverage'; }
      catch (e) { if (/HTTP 409/.test(e.message) && /not_migrated/.test(e.message)) out.mode = 'legacy'; else throw e; }
    }
    log(`[coverage] mode = ${out.mode}`);

    // ── ③ 世代と token を台帳と lease に保存 → updating ──
    let policy = null, generation = null;
    if (out.mode === 'coverage') {
      policy = status.policy;
      if (!policy || !Array.isArray(policy.rows) || !/^[0-9a-f]{64}$/.test(String(policy.fingerprint))) throw new Error('Render の状態に policy (rows・fingerprint) が無い');
      if (!policyOrigin(policy.rows, FINANCE_SOURCE)) throw new Error(`policy に source ${FINANCE_SOURCE} が無い (amazon / jp)`);
      const renderGen = BigInt(status.coverage && status.coverage.generation != null ? status.coverage.generation : 0);
      const ledgerGen = BigInt(ledger.getMeta(LEDGER_META.generation) ?? 0);
      const g = (renderGen > ledgerGen ? renderGen : ledgerGen) + 1n;
      if (g > BigInt(Number.MAX_SAFE_INTEGER)) throw new Error('coverage の世代が大きすぎる');
      generation = Number(g);
      ledger.setMeta({ [LEDGER_META.generation]: String(g), [LEDGER_META.run]: JSON.stringify({ generation: String(g), run_token: lease.runToken, started_at: now().toISOString(), state: 'updating' }) });
      V.setLeaseGeneration(db, lease, generation, now());
      out.generation = generation;
      const u = await postCoverage(fetchImpl, { base, syncKey, sleep, log, body: { state: 'updating', mall: FINANCE_MALL, scope: FINANCE_SCOPE, source: FINANCE_SOURCE, generation: String(g), run_token: lease.runToken } });
      if (u.status !== 'applied' && u.status !== 'same') throw new Error(`coverage updating が ${u.status} (Render の今の世代 ${u.current_generation ?? '-'}) = 取込を始めない`);
      log(`[coverage] 世代 ${generation} を updating にした (${u.status})`);
      if (hooks.afterUpdating) await hooks.afterUpdating(db);
    }

    // ── ④ 取込 (手で積んだファイル → SP-API) ──
    out.manual = ingestQueuedManualFiles(db, { lease, generation, log, now });
    out.ingest = await runSettlementFetch({ reportId: null, dryRun: false, source }, {
      db, sp, inventorySp, runId, downloadTsv, now, lease, coverage: { generation, runToken: lease.runToken, evidenceEpoch: latestEpoch(db) },
    });
    V.refreshStaleVersionDetails(db, { check, log });
    if (out.ingest.blocked.length) {
      out.exitCode = 3;
      out.summary = `❌ Amazon 決済と財務: 取り込めない V2 のレポート ${out.ingest.blocked.length} 本 (${out.ingest.blocked.map((b) => b.reportId).join(', ')}) → amazon-settlement-v2.js に規則を足す。Company DB には送らない (coverage は updating のまま)`;
      return out;
    }
    const ingestPart = `取込 決済の行 +${out.ingest.totalLines} (見出し +${out.ingest.totalHeaders})${out.ingest.inventory ? `・一覧 ${out.ingest.inventory.count} 本` : ''}${out.manual.length ? `・手のファイル ${out.manual.filter((m) => m.status === 'ingested').length}/${out.manual.length}` : ''}${out.ingest.blockedCovered.length ? `・⚠️ 取り込めない V2 ${out.ingest.blockedCovered.length} 本 (ほかの版あり)` : ''}`;
    const manualFailed = out.manual.filter((m) => m.status !== 'ingested').length;

    // ── ⑤ 送る ──
    if (out.mode === 'ingest_only') {
      out.summary = `✅ Amazon 決済と財務: ${ingestPart} | 財務 push: ⏭️ 初回のバックフィル前 (台帳に完了印が無い) = 送らない${manualFailed ? ` | ⚠️ 手のファイルの失敗 ${manualFailed}` : ''}`;
      if (manualFailed) out.summary = out.summary.replace(/^✅/, '⚠️');
      return out;
    }
    const cap = capacity === undefined ? capacityFromEnv() : capacity;
    if (!(cap && cap.limitBytes > 0)) throw new Error('容量の上限 (CDB_DB_LIMIT_BYTES) が無い = D-W5 (Render の Postgres のプラン) を決めるまで送らない');
    const pushMode = out.mode === 'coverage' ? 'full' : amazonFinanceDailyArgs(businessDate)[0].replace(/^--/, '');
    const w = openReader();
    let r;
    try {
      r = await pushAmazonFinance({
        warehouse: w, ledger, base, syncKey, mode: pushMode, fetchImpl, log, now, capacity: cap, chunkSize, sleep,
        dirty: { clear: (nos, rev) => V.clearDirtyOrders(db, nos, rev, { check }) },
        coverage: out.mode === 'coverage' ? coverageHooks({ db, lease, generation, policy, fetchImpl, base, syncKey, sleep, log, now, hooks, check }) : null,
      });
    } finally { w.close(); }
    out.push = r;
    const f = r.finance || {};
    try {
      writeEvidence(dataDir, 'finance-amazon', { kind: 'order_finance', mall: FINANCE_MALL, scope: FINANCE_SCOPE, mode: pushMode, ok: !!r.ok, run_id: r.runId ?? null, batch_seq: r.batchSeq ?? null,
        started_at: now().toISOString(), changed: r.changed, applied: r.applied, same: r.same, stale: r.stale, failed: r.failed.length, transform_errors: r.transformErrors.length,
        unkeyed: (f.unkeyed || []).length, unmapped_rows: f.unmapped ? f.unmapped.rows : 0, coverage_generation: generation, coverage_complete: !!(f.coverage && f.coverage.complete) });
    } catch (e) { log(`[coverage] ⚠️ 証跡を書けない: ${e.message}`); }
    if (r.lockedBy) throw new Error(`別の送り手が走っている (${r.lockedBy.owner} pid ${r.lockedBy.pid})`);
    const pushPart = `財務 push (${pushMode}): 変わった ${r.changed} 注文 (applied ${r.applied} / same ${r.same} / stale ${r.stale} / failed ${r.failed.length} / 整形できない ${r.transformErrors.length})・読み直す注文 ${f.dirtyOrders ?? 0} (消した ${f.dirtyCleared ?? 0})`;
    const pushWarn = (f.unmapped && f.unmapped.rows) || r.ledgerReset || r.ledgerRebuilt;
    if (out.mode === 'legacy') {
      out.exitCode = r.ok ? 0 : 1;
      out.summary = `${r.ok ? '⚠️' : '❌'} Amazon 決済と財務: ${ingestPart} | ${pushPart} | coverage: Render に 0051 が無い = coverage を送らない (今までの送り方)`;
      return out;
    }
    const cov = f.coverage || { complete: false, reasons: [{ code: 'no_finalize', detail: '完成の判定まで進まなかった', human: false }] };
    out.coverage = cov;
    out.reasons = cov.reasons || [];
    let covPart;
    if (cov.complete) covPart = `coverage: ✅ complete (世代 ${generation}・complete_to ${cov.complete_to}・${cov.status})`;
    else {
      const codes = [...new Set(out.reasons.map((x) => x.code))];
      const human = out.reasons.length > 0 && out.reasons.every((x) => x.human) && !cov.error;
      covPart = `coverage: ${human ? '⚠️' : '❌'} complete にしない (世代 ${generation} は updating のまま = 正式な利益は null) 理由 ${codes.join(', ')}${cov.error ? ` / ${cov.error}` : ''}`;
      if (!human) out.exitCode = 1;
      for (const x of out.reasons.slice(0, 15)) log(`  [coverage] ${x.human ? '⚠️' : '❌'} ${x.code}: ${String(x.detail || '').slice(0, 300)}`);
    }
    if (!r.ok) out.exitCode = 1;
    const head = out.exitCode !== 0 ? '❌' : (!cov.complete || pushWarn || manualFailed || out.ingest.blockedCovered.length) ? '⚠️' : '✅';
    out.summary = `${head} Amazon 決済と財務: ${ingestPart} | ${pushPart} | ${covPart}${manualFailed ? ` | ⚠️ 手のファイルの失敗 ${manualFailed}` : ''}`;
    return out;
  } catch (e) {
    out.exitCode = 1;
    out.summary = `❌ Amazon 決済と財務: ${String(e && e.message).replace(/\s+/g, ' ').slice(0, 400)}${out.generation ? ` (coverage の世代 ${out.generation} は updating のまま)` : ''}`;
    out.error = e;
    return out;
  } finally {
    try { V.releaseCoverageLease(db, lease, now()); } catch { /* 放せなくても次の回が持ち主の死を見て取る */ }
    ledger.close();
  }
}

/** 送り手に渡す coverage の差し込み口 (世代・token・走査の前の確かめ・manifest・完成の判定と complete) */
function coverageHooks({ db, lease, generation, policy, fetchImpl, base, syncKey, sleep, log, now, hooks, check }) {
  const runToken = lease.runToken;
  return {
    generation, runToken,
    // 走査の前: Render の coverage がこの世代・token の updating か (送り手は世代を採らない・coverage を変えない。R19 M4)
    beforeScan: async () => {
      const st = await getJson(fetchImpl, `${base}${STATUS_PATH}`, syncKey, 'Render の決済のそろい (走査の前)', { sleep, log });
      const c = st && st.coverage;
      if (!c || String(c.generation) !== String(generation) || c.run_token !== runToken || c.state !== 'updating') {
        throw new Error(`Render の coverage が今回の世代 ${generation} の updating でない (今 = ${c ? `${c.state} 世代 ${c.generation}` : 'なし'}) = 送らない`);
      }
    },
    // 走査の同じ読み取りの取引の中で manifest (書かない)
    onScanSnapshot: ({ warehouse, stats, receipts }) => ({
      ...evaluateCoverage(warehouse, { generation, runToken, policy, source: FINANCE_SOURCE, now: now(), receipts, sourceRevision: stats.sourceRevision }),
      sourceRevision: stats.sourceRevision,
    }),
    // 送り終えた後 (台帳の lock の中): 完成の判定 → source_revision の読み直し → lease の確かめ → complete
    finalize: async ({ r, scanSnapshot, stats, ledger }) => {
      const retryLeft = retryStore(ledger).list().length;
      const rev = scanSnapshot ? scanSnapshot.sourceRevision : null;
      const dirtyLeft = rev == null ? 1 : db.prepare(`SELECT COUNT(*) n FROM amazon_settlement_dirty_orders WHERE revision <= ?`).get(rev).n;
      if (hooks.beforeComplete) await hooks.beforeComplete(db);
      const revNow = V.readSourceRevision(db);   // 🚨 complete の直前に読み直す (R と違えば送らない = 次の回で拾う)
      const reasons = completionBlockers({ r, snapshot: scanSnapshot, retryLeft, dirtyLeft, sourceRevisionNow: revNow, unkeyed: (stats.unkeyed || []).length, pseudoBlocked: stats.pseudoBlocked || 0 });
      if (reasons.length) return { complete: false, reasons };
      let manifest;
      try { manifest = validateCoverageManifest(scanSnapshot.manifest, { now: now() }); } catch (e) { return { complete: false, reasons: [{ code: 'manifest_invalid', detail: e.message, human: false }] }; }
      try { db.transaction(() => check(db)).immediate(); } catch (e) { return { complete: false, reasons: [{ code: 'lease_lost', detail: e.message, human: false }], error: e.message }; }
      const requestHash = coverageRequestHash({ companyId: COMPANY_ID, mall: FINANCE_MALL, scopeKey: FINANCE_SCOPE, source: FINANCE_SOURCE, generation: String(generation), runToken, manifest });
      try {
        const resp = await postCoverage(fetchImpl, { base, syncKey, sleep, log, body: { state: 'complete', mall: FINANCE_MALL, scope: FINANCE_SCOPE, source: FINANCE_SOURCE, generation: String(generation), run_token: runToken, manifest, request_hash: requestHash } });
        if (resp.state !== 'complete' || (resp.status !== 'applied' && resp.status !== 'same')) return { complete: false, reasons: [{ code: 'complete_rejected', detail: JSON.stringify(resp).slice(0, 300), human: false }], error: `complete が ${resp.status}` };
        log(`[coverage] ✅ complete (世代 ${generation}・complete_to ${manifest.complete_to}・${resp.status})`);
        return { complete: true, complete_to: resp.complete_to ?? manifest.complete_to, status: resp.status, manifest, requestHash, reasons: [] };
      } catch (e) {
        return { complete: false, reasons: [{ code: 'complete_post', detail: e.message, human: false }], error: `complete を送れない: ${String(e.message).slice(0, 200)}` };
      }
    },
  };
}

export function parseArgs(argv) {
  const out = { dryRun: false, source: 'v2', chunk: null };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--dry-run') out.dryRun = true;
    else if (a === '--source') out.source = argv[++i];
    else if (a === '--chunk') out.chunk = argv[++i];
    else throw new Error(`知らない引数: ${a}`);
  }
  if (!Object.hasOwn(SOURCES, out.source)) throw new Error(`--source は v1 か v2: ${out.source}`);
  return out;
}

const isDirectRun = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isDirectRun) {
  (async () => {
    const a = parseArgs(process.argv.slice(2));
    const dataDir = (process.env.DATA_DIR || '').trim();
    if (!dataDir) throw new Error('DATA_DIR が無い (cwd の data に作らない)');
    const chunkSize = a.chunk != null ? Number(a.chunk) : (process.env.CDB_PUSH_CHUNK ? Number(process.env.CDB_PUSH_CHUNK) : DEFAULT_CHUNK);
    if (!Number.isInteger(chunkSize) || chunkSize < 1 || chunkSize > MAX_CHUNK) throw new Error(`chunk が不正: ${chunkSize}`);
    const { sp, inventorySp } = spClients();
    const r = await runCoverage({ dataDir, source: a.source, dryRun: a.dryRun, base: syncBase(), syncKey: process.env.MIRROR_SYNC_KEY || '', sp, inventorySp, chunkSize });
    console.log(r.summary);
    process.exitCode = r.exitCode;
  })().catch((e) => {
    console.error(`❌ Amazon 決済と財務: ${String(e && e.message).replace(/\s+/g, ' ').slice(0, 400)}`);
    process.exitCode = 1;
  }).finally(() => { setTimeout(() => process.exit(process.exitCode ?? 0), 10000).unref(); });
}
