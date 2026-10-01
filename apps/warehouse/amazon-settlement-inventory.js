/**
 * amazon-settlement-inventory.js — 決済のレポートの一覧 (inventory) を記録する部品 (D7b-1b の下ごしらえ)
 *
 * 設計 = AI_reference システム設計/CompanyDB構想/13_Amazon利益のmart_設計_20260930.md §3.1
 *   「決済のレポートの一覧 (inventory) を持つ」「一覧の窓が途切れていないことを証明する」
 *
 * なぜ: 生の表 (raw_amazon_settlement_*) を見ても「レポートが 1 本まるごと取れていない」は分からない。
 *   今の取込は Reports API の一覧を残さず、DONE でないレポートは黙って飛ばし、20 ページで黙って打ち切る。
 *   → 毎回の一覧 (どの report を見たか・状態・期間・文書 ID・取込の結果) を SQLite に残す。
 *
 * この部品は **記録だけ**。読み手 = coverage の判定 (amazon-finance-coverage.js)。
 * 🆕 2026-10-01 (D7b-1b-3): coordinator の回は見出しに coverage_generation・run_token・evidence_epoch (その時の最新の初期の印) を書き、
 *   記録の取引の中で lease を確かめる (params.check)。行には取込が入れた文書の版 (document_version_seq) も書く。
 *   一覧の記録用の getReports は **取込の一覧とは別の要求** (時間の上限つきの専用の接続・失敗しても取込は続ける)。
 *   🆕 2026-10-01 (#1567 Codex R4): 取込の一覧も **同じ固定の窓** (回の開始とその 85 日前) = 証拠の一覧に出ない report
 *   (85〜90 日前に作られた・回の開始の後に作られた) は取込まない (前 = 取込は日時の境なし = Amazon の既定の 90 日)。
 *
 * 表 (db.js):
 *   amazon_settlement_report_inventory_runs = 一覧の回の見出し (窓・最後のページまで取れたか・数・snapshot の digest)
 *   amazon_settlement_report_inventory      = 一覧の行 (report ごと・取込の結果)
 */
import { canonicalSha256 } from '../company-db/canonical-hash.mjs';

/** 一覧の窓 = 回の開始の時刻から 85 日前 (Reports API の保持 90 日より控えめ・設計 §3.1) */
export const INVENTORY_WINDOW_DAYS = 85;
/** 1 回の一覧で読むページの上限 = 今の取込の打ち切りと同じ (旧コードは `pageNum > 20` で break = 21 ページ目まで読む) */
export const MAX_LIST_PAGES = 21;
/** 取込の結果 (固定の集合・db.js の CHECK と同じ)。今の fetch-amazon-settlements.js の分岐に 1 つずつ対応 */
export const IMPORT_RESULTS = Object.freeze([
  'not_processed',     // 一覧には居たが、この回の取込の繰り返しで扱わなかった (取込の一覧に無い・途中で落ちた)
  'skipped_not_done',  // processingStatus が DONE でない / reportDocumentId が無い (今のコードは飛ばす)
  'skipped_v1',        // (旧) V2 のレポートだが、同じ決済を V1 で取込済みで入れなかった (2026-10-01 に廃止 = V2 も必ず版として保存。過去の行に残る)
  'imported',          // 取込の処理を通した (INSERT OR IGNORE = 既に入っていた行は 0 行でも imported)
  'failed',            // 取り込まなかった (V2 の並べ直しの規則に無い = blocked) / ダウンロード・処理で例外
]);

/** Date → UTC の YYYY-MM-DDTHH:MM:SSZ (秒未満は切り捨て) */
export function toUtcSeconds(d) {
  if (!(d instanceof Date) || Number.isNaN(d.getTime())) throw new Error(`日時でない: ${d}`);
  return d.toISOString().slice(0, 19) + 'Z';
}

const ISO_TIME_RE = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(\.\d+)?(Z|[+-]\d{2}:\d{2})$/;
/**
 * API の日時 (例 2026-09-20T03:04:05+00:00) → UTC の YYYY-MM-DDTHH:MM:SSZ。
 * null / 空 = null。**読めない (形が違う・暦に無い・時差が無い) ものは元の文字のまま** 返す
 *   (null にすると「期間が無い」と区別できない。後の coverage が「読めない」を未充足にする・設計 §3.1)
 */
export function normalizeApiTime(v) {
  if (v == null || v === '') return null;
  const s = String(v);
  const m = ISO_TIME_RE.exec(s);
  if (!m) return s;
  const [, y, mo, d, h, mi, se, , tz] = m;
  const base = Date.UTC(+y, +mo - 1, +d, +h, +mi, +se);
  const c = new Date(base);
  if (c.getUTCFullYear() !== +y || c.getUTCMonth() !== +mo - 1 || c.getUTCDate() !== +d
    || c.getUTCHours() !== +h || c.getUTCMinutes() !== +mi || c.getUTCSeconds() !== +se) return s;
  let offMin = 0;
  if (tz !== 'Z') {
    const hh = +tz.slice(1, 3), mm = +tz.slice(4, 6);
    if (hh > 23 || mm > 59) return s;
    offMin = (tz[0] === '-' ? -1 : 1) * (hh * 60 + mm);
  }
  return toUtcSeconds(new Date(base - offMin * 60000));
}

/** 一覧の窓 (最初の要求で明示して固定)。startedAt = 回の開始の時刻 → createdUntil (秒に切り捨て)・createdSince = その 85 日前 */
export function inventoryWindow(startedAt) {
  const until = new Date(Math.floor(startedAt.getTime() / 1000) * 1000);
  const since = new Date(until.getTime() - INVENTORY_WINDOW_DAYS * 86400000);
  return { createdSince: toUtcSeconds(since), createdUntil: toUtcSeconds(until) };
}

/** 一覧の記録用の要求の時間の上限 (全ページの合計)。超えたら一覧の失敗 = 取込には影響させない (Codex #1555 R1 High) */
export const INVENTORY_TIMEOUT_MS = 120000;
/** 一覧の回の所属 (後の coverage の証拠の鎖の鍵)。今は 1 つだけ = 固定で入れる (Codex #1555 R1 M3) */
export const INVENTORY_SCOPE = Object.freeze({ companyId: 1, mall: 'amazon', scopeKey: 'jp' });

/**
 * getReports をページで読む (nextToken は単独で渡す = SP-API の決まり)。
 * 上限のページに来ても nextToken が残っていたら打ち切るが、**黙らない** = lastPageReached: false を返す。
 * strict = ページの応答の形を確かめる (null・reports が配列でない・nextToken が文字でない = 例外 = 一覧の失敗。
 *   「最後まで取れた空のページ」にしない・Codex #1555 R1 M1)。取込の一覧は今までどおり strict にしない (取込を変えない)
 */
export async function listReportPages(sp, firstQuery, { maxPages = MAX_LIST_PAGES, label = 'list', strict = false } = {}) {
  const reports = [];
  let nextToken = null;
  let pages = 0;
  do {
    pages++;
    const query = nextToken ? { nextToken } : firstQuery;
    const resp = await sp.callAPI({ operation: 'getReports', endpoint: 'reports', query });
    if (strict) {
      if (resp == null || typeof resp !== 'object' || !Array.isArray(resp.reports)) throw new Error(`${pages} ページ目の応答の形が違う (reports が配列でない)`);
      if (resp.nextToken != null && (typeof resp.nextToken !== 'string' || resp.nextToken === '')) throw new Error(`${pages} ページ目の nextToken が文字でない`);
    }
    if (resp.reports) reports.push(...resp.reports);   // 取込の一覧 = 今までと同じ (resp が null なら例外)
    nextToken = resp.nextToken || null;
    console.log(`[${label}] page ${pages}: cumulative ${reports.length}, nextToken=${nextToken ? 'yes' : 'no'}`);
    if (nextToken && pages >= maxPages) {
      console.log(`[${label}] ⚠️ ${pages} ページで打ち切り (nextToken が残っている = 一覧の途中まで)`);
      break;
    }
  } while (nextToken);
  return { reports, pages, lastPageReached: !nextToken };
}

/**
 * 一覧の記録用の **専用の SP-API の接続** の options (amazon-sp-api 1.2.0 の lib/SellingPartner.js の名前・Codex #1555 R3):
 *   - auto_request_throttled: false = 429 (QuotaExceeded) で待って再試行しない (_retryThrottledRequest の timer を作らない)
 *   - retry_remote_timeout: false = ETIMEDOUT / ENOTFOUND / ECONNRESET で再試行しない
 *   - timeouts = 要求の時間の上限 (lib/TimeoutManager.js が期限で req.destroy = socket を破棄)。要求ごとに残り時間で上書きする (listInventoryReports)
 * 🚨 取込に使う接続 (getClient) の設定は変えない
 */
export function inventoryClientOptions(timeoutMs = INVENTORY_TIMEOUT_MS) {
  return {
    // auto_request_tokens: false = 403 (access token expired) でトークンを取り直して callAPI を再帰で呼ばない
    //   (再帰の時は要求ごとの timeouts を失う = 期限を越えて要求が生きる。Codex #1555 R4)。トークンは ensureAccessToken で最初に 1 回だけ取る
    auto_request_tokens: false,
    auto_request_throttled: false,
    retry_remote_timeout: false,
    timeouts: { response: timeoutMs, idle: timeoutMs, deadline: timeoutMs },
  };
}

/**
 * 専用の接続のアクセストークンを、全体の期限の中で (残り時間の timeouts で) 明示的に 1 回だけ取る (Codex #1555 R4)。
 * amazon-sp-api 1.2.0: refreshAccessToken(scope) は LWA (api.amazon.com) への要求に this._current_call_timeouts を使う
 *   → 取る前に残り時間を入れる。トークンを持っている・refreshAccessToken の無い接続 (試験の fake) は何もしない
 */
async function ensureAccessToken(sp, leftMs) {
  if (typeof sp.refreshAccessToken !== 'function' || sp.access_token) return;
  sp._current_call_timeouts = { response: leftMs, idle: leftMs, deadline: leftMs };
  await sp.refreshAccessToken();
}

/** 期限つきの待ち。期限を過ぎたら例外 (要求そのものは options.timeouts でライブラリが止める。ここは待つ側の保険・後で失敗しても unhandled にしない) */
function withDeadline(promise, deadline, timeoutMs) {
  Promise.resolve(promise).catch(() => {});
  const ms = deadline - Date.now();
  const err = () => new Error(`一覧の要求の時間切れ (${timeoutMs} ms)`);
  if (ms <= 0) return Promise.reject(err());
  let timer;
  const timeout = new Promise((_, reject) => { timer = setTimeout(() => reject(err()), ms); });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

/**
 * 一覧の記録用の要求 (窓を明示して固定)。失敗・時間切れは投げる (呼び手が ⚠️ にして取込は続ける)。
 * 🚨 呼び手は取込の一覧の要求を **先に** 確定してから呼ぶ (既定の 90 日の窓の「今」をずらさない・レートの枠を先に使わない・Codex #1555 R1 High)
 */
export async function listInventoryReports(sp, { reportType, marketplaceId, startedAt, maxPages = MAX_LIST_PAGES, timeoutMs = INVENTORY_TIMEOUT_MS }) {
  const window = inventoryWindow(startedAt);
  const deadline = Date.now() + timeoutMs;
  // 要求ごとに残り時間を timeouts に入れる = ライブラリが期限で socket を破棄する (待つのをやめるだけだと timer / socket が残り、node が終わらない)
  const timedSp = {
    callAPI: (req) => {
      const left = Math.max(1, deadline - Date.now());
      const timeouts = { response: left, idle: left, deadline: left };
      return withDeadline(sp.callAPI({ ...req, options: { ...(req.options || {}), timeouts } }), deadline, timeoutMs);
    },
  };
  const firstQuery = {
    reportTypes: [reportType],
    marketplaceIds: [marketplaceId],
    pageSize: 100,
    createdSince: window.createdSince,
    createdUntil: window.createdUntil,
  };
  // 先にトークンを 1 回だけ (期限の中で)。403 (expired) が来ても取り直さない = 一覧の失敗
  await withDeadline(ensureAccessToken(sp, Math.max(1, deadline - Date.now())), deadline, timeoutMs);
  const r = await listReportPages(timedSp, firstQuery, { maxPages, label: 'inventory', strict: true });
  return { ...r, window };
}

const byUtf8 = (a, b) => Buffer.compare(Buffer.from(a, 'utf8'), Buffer.from(b, 'utf8'));
const strOrNull = (v) => (v == null || v === '' ? null : String(v));

/**
 * API の report の並び → 一覧の行 (同じ report ID を 2 回見たら **最後に見た状態**・last_seen_ordinal = 並びの 1 始まりの位置)。
 * reportId の無いものは数えて落とす (missingId)。
 */
export function inventoryEntries(reports, fallbackReportType) {
  const map = new Map();
  let missingId = 0;
  reports.forEach((r, i) => {
    const reportId = strOrNull(r?.reportId);
    if (!reportId) { missingId++; return; }
    map.set(reportId, {
      report_id: reportId,
      report_type: strOrNull(r.reportType) || fallbackReportType,
      processing_status: strOrNull(r.processingStatus),
      created_time: normalizeApiTime(r.createdTime),
      data_start_time: normalizeApiTime(r.dataStartTime),
      data_end_time: normalizeApiTime(r.dataEndTime),
      report_document_id: strOrNull(r.reportDocumentId),
      last_seen_ordinal: i + 1,
    });
  });
  const entries = [...map.values()].sort((a, b) => byUtf8(a.report_id, b.report_id));
  return { entries, missingId };
}

/**
 * snapshot の digest = report ID の UTF-8 のバイトの順に並べた
 *   {report_id, processing_status, created_time, data_start_time, data_end_time, report_document_id}
 * の正規の JSON (canonical-hash.mjs) の SHA-256 (hex)。日時は UTC の YYYY-MM-DDTHH:MM:SSZ・null は null。
 * 🚨 保存済みの digest と同じ式のまま変えない (変えるときは版を足す)
 */
export function inventorySnapshotDigest(entries) {
  const arr = entries
    .map((e) => ({
      report_id: String(e.report_id),
      processing_status: e.processing_status ?? null,
      created_time: e.created_time ?? null,
      data_start_time: e.data_start_time ?? null,
      data_end_time: e.data_end_time ?? null,
      report_document_id: e.report_document_id ?? null,
    }))
    .sort((a, b) => byUtf8(a.report_id, b.report_id));
  for (let i = 1; i < arr.length; i++) {
    if (arr[i - 1].report_id === arr[i].report_id) throw new Error(`同じ report ID が 2 つ: ${arr[i].report_id}`);
  }
  return canonicalSha256(arr);
}

/**
 * 一覧の回を記録する (見出し + 行を 1 つの取引で)。返り値 = 回の id。completed_at は入れない (recordInventorySnapshot が入れる)。
 * listing = listInventoryReports の返り値 (失敗なら null)・listError = 失敗の文言・ingestError = 取込の例外・
 * recordError = 記録の失敗 (これを渡したときは行を書かない = 見出しだけの「記録できなかった回」)
 */
export function recordInventoryRun(db, { reportType, marketplaceId, window, startedAt, listCompletedAt, listing, listError, ingestRunId, ingestError = null, recordError = null,
  coverageGeneration = null, runToken = null, evidenceEpoch = null }) {
  const headerOnly = recordError != null;
  const { entries, missingId } = listing && !headerOnly ? inventoryEntries(listing.reports, reportType) : { entries: [], missingId: 0 };
  const errors = [];
  if (listError) errors.push(listError);
  if (missingId) errors.push(`reportId の無い report ${missingId} 件`);
  // evidence_epoch / coverage_generation / run_token = coordinator の回だけ (null の回はどの印の鎖にも属さない = coverage の積み上げに使わない)
  const insertRun = db.prepare(`INSERT INTO amazon_settlement_report_inventory_runs (
      company_id, mall, scope_key, report_type, marketplace_id, query_created_since, query_created_until,
      started_at, list_completed_at, completed_at, last_page_reached, page_count, report_count,
      snapshot_digest, list_error, ingest_error, record_error, evidence_epoch, coverage_generation, run_token, inventory_run_seq, ingest_run_id
    ) VALUES (
      @company_id, @mall, @scope_key, @report_type, @marketplace_id, @query_created_since, @query_created_until,
      @started_at, @list_completed_at, NULL, @last_page_reached, @page_count, @report_count,
      @snapshot_digest, @list_error, @ingest_error, @record_error, @evidence_epoch, @coverage_generation, @run_token, @inventory_run_seq, @ingest_run_id
    )`);
  const insertRow = db.prepare(`INSERT INTO amazon_settlement_report_inventory (
      inventory_run_id, report_id, report_type, processing_status, created_time,
      data_start_time, data_end_time, report_document_id, import_result, last_seen_ordinal
    ) VALUES (
      @inventory_run_id, @report_id, @report_type, @processing_status, @created_time,
      @data_start_time, @data_end_time, @report_document_id, 'not_processed', @last_seen_ordinal
    )`);
  const txn = db.transaction(() => {
    const seq = db.prepare(`SELECT COALESCE(MAX(inventory_run_seq), 0) + 1 AS n FROM amazon_settlement_report_inventory_runs`).get().n;
    const runId = Number(insertRun.run({
      company_id: INVENTORY_SCOPE.companyId,
      mall: INVENTORY_SCOPE.mall,
      scope_key: INVENTORY_SCOPE.scopeKey,
      report_type: reportType,
      marketplace_id: marketplaceId ?? null,
      query_created_since: window.createdSince,
      query_created_until: window.createdUntil,
      started_at: toUtcSeconds(startedAt),
      list_completed_at: listCompletedAt ? toUtcSeconds(listCompletedAt) : null,
      last_page_reached: listing && !listError && !headerOnly && listing.lastPageReached ? 1 : 0,
      page_count: listing ? listing.pages : 0,
      report_count: entries.length,
      snapshot_digest: listing && !headerOnly ? inventorySnapshotDigest(entries) : null,
      list_error: errors.length ? errors.join(' / ') : null,
      ingest_error: ingestError,
      record_error: recordError,
      inventory_run_seq: seq,
      ingest_run_id: ingestRunId,
      evidence_epoch: evidenceEpoch ?? null,
      coverage_generation: coverageGeneration ?? null,
      run_token: runToken ?? null,
    }).lastInsertRowid);
    for (const e of entries) insertRow.run({ ...e, inventory_run_id: runId });
    return runId;
  });
  return txn();
}

/**
 * 取込の結果を一覧の行に書く (その回の一覧に無い report = 0 行・何もしない)。返り値 = 書いた行の数
 * extra = { note, settlementId, fileHash, importedReportDocumentId, headerInserted, linesInserted, documentVersionSeq }
 */
export function recordImportResult(db, inventoryRunId, reportId, result, extra = {}) {
  if (!IMPORT_RESULTS.includes(result) || result === 'not_processed') throw new Error(`取込の結果が違う: ${result}`);
  return db.prepare(`UPDATE amazon_settlement_report_inventory SET
      import_result = @result, import_note = @note, settlement_id = @settlement_id,
      source_file_hash = @source_file_hash, imported_report_document_id = @imported_report_document_id,
      header_inserted = @header_inserted, lines_inserted = @lines_inserted, document_version_seq = @document_version_seq
    WHERE inventory_run_id = @run_id AND report_id = @report_id`).run({
    result,
    note: extra.note ?? null,
    settlement_id: extra.settlementId ?? null,
    source_file_hash: extra.fileHash ?? null,
    imported_report_document_id: extra.importedReportDocumentId ?? null,
    header_inserted: extra.headerInserted ?? null,
    lines_inserted: extra.linesInserted ?? null,
    document_version_seq: extra.documentVersionSeq ?? null,
    run_id: inventoryRunId,
    report_id: String(reportId),
  }).changes;
}

/** 回の完了 (取込の繰り返しが最後まで回った)。途中で落ちた回は completed_at が null のまま = 完了していない回 */
export function finishInventoryRun(db, inventoryRunId, completedAt) {
  db.prepare(`UPDATE amazon_settlement_report_inventory_runs SET completed_at = ? WHERE id = ?`).run(toUtcSeconds(completedAt), inventoryRunId);
}

/**
 * 取込のループが終わった後に、一覧の回・行・取込の結果・完了を **1 つの取引** で書く (Codex #1555 R2)。
 *   results = Map<report_id, { result, extra }> (取込のループがメモリに持った結果・同じ report は最後の結果)
 *   completedAt = 取込が最後まで回ったときだけ (例外で止まった回は null + ingestError)
 * 🚨 どこかで失敗したら取引ごと戻し、見出しだけを record_error つき・completed_at null で書き直す
 *   (= 「成功した回」に見せない)。その書き直しも失敗したら何も残らない (= coverage は使えない = 安全側)。
 * 返り値 = { id, recordError }
 */
export function recordInventorySnapshot(db, params, results, completedAt) {
  const txn = db.transaction(() => {
    if (params.check) params.check(db);   // coordinator の lease (違えば LEASE_LOST = 何も書かない)
    const id = recordInventoryRun(db, params);
    for (const [reportId, r] of results) recordImportResult(db, id, reportId, r.result, r.extra);
    if (completedAt && !params.ingestError) finishInventoryRun(db, id, completedAt);
    return id;
  });
  try {
    return { id: txn.immediate(), recordError: null };
  } catch (e) {
    if (e && e.code === 'LEASE_LOST') return { id: null, recordError: `一覧を記録しない (lease を失った): ${e.message}` };
    const recordError = `一覧を記録できない: ${e?.message || e}`;
    let id = null;
    try { id = recordInventoryRun(db, { ...params, recordError }); }
    catch (e2) { return { id: null, recordError: `${recordError} / 見出しも書けない: ${e2?.message || e2}` }; }
    return { id, recordError };
  }
}
