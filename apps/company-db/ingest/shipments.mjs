/**
 * ingest/shipments.mjs — miniPC から届いた伝票 (NE) の 1 chunk を Company DB に適用する (Render 側。0013 の core.apply_shipment_batch を呼ぶ)。D5a
 *
 * 契約 (08 §4.7 を HTTP に写したもの。送り手 = apps/company-db/push/ne-shipments.mjs):
 *   body = { run_id, batch_seq, chunk_index, last, transform_version, rows: [{ ne_slip_no, header, lines }] }
 *   - batch_seq = 送り手の世代 (単調増加。内容が同じでも進める)。古い世代は伝票ごとに 'stale' で拒まれる (apply_shipment_batch)
 *   - chunk_index = run の中の通し番号 (0 から)。last = true の chunk が来て 0〜last が全部そろったら run を閉じる (chunk の数は送り手が先に決めなくてよい = 流しながら送れる)
 *   - 1 chunk = 1 取引。伝票ごとに savepoint を切り、失敗した伝票だけを failed に積んで他は commit する
 *     (1 伝票の不良で 1 日分を止めない。送り手は failed / stale の伝票を台帳に書かない = 次回また送る)
 *   - 再送は冪等: 同じ (run_id, chunk_index) に同じ内容 (Render が計算した指紋) → 適用せず保存した応答を返す (ops.ingest_chunks。集計を二重に数えない)。
 *     同じ chunk_index に違う内容 → 409。run の世代 / transform_version は最初の chunk で固まり、違えば 409
 *   - 期限: chunk 全体の期限 (既定 80 秒。Render の HTTP は 100 秒程度で切れる) を過ぎたら全部 rollback して CHUNK_DEADLINE (503)。送り手は chunk を半分に割って送り直す
 *   - ops.ingest_runs に run を残す (source_system='ne', entity='shipments'。checksum=世代、format_version=transform_version、pages=chunk の数 (閉じたとき)、
 *     rows_seen / rows_inserted (applied) / rows_skipped (same + stale)。failed_ranges = 失敗した伝票 (先頭 200 件)、error = 失敗の総数)
 *
 * 🚨 mirror (Render の SQLite) は経由しない: 受け皿の関数が世代・冪等・明細集合の置換を担うので「公開マーカー」は要らない
 *    (chunk が commit されるか、されないかの 2 択)。写しを SQLite に残すと年 50 万伝票ぶん Render のディスクを食う。
 */
import crypto from 'node:crypto';
import { canonicalJson } from '../push/ne-shipments-transform.mjs';

export const RUN_ID_RE = /^ship_[0-9]{15}_[0-9a-f]{6}$/;
export const SLIP_RE = /^[0-9A-Za-z][0-9A-Za-z._-]{0,39}$/;
export const MAX_ROWS_PER_CHUNK = 1000;
export const MAX_LINES_PER_SLIP = 500;
export const MAX_LINES_PER_CHUNK = 5000;
export const DEFAULT_DEADLINE_MS = 80000;
const MAX_FAILED_KEPT = 200;

function err(code, message) { const e = new Error(message); e.code = code; return e; }
const bad = (m) => err('BAD_REQUEST', m);

/** body の形を確かめて正規化する (throw code=BAD_REQUEST → 400) */
export function validateChunk(body) {
  if (!body || typeof body !== 'object' || Array.isArray(body)) throw bad('body must be a JSON object');
  const runId = String(body.run_id || '');
  if (!RUN_ID_RE.test(runId)) throw bad('bad run_id (ship_<15 digits>_<6 hex>)');
  const batchSeq = Number(body.batch_seq);
  if (!Number.isInteger(batchSeq) || batchSeq <= 0 || batchSeq > Number.MAX_SAFE_INTEGER) throw bad('batch_seq must be a positive integer');
  const chunkIndex = Number(body.chunk_index);
  if (!Number.isInteger(chunkIndex) || chunkIndex < 0 || chunkIndex > 1000000) throw bad('chunk_index must be an integer >= 0');
  if (typeof body.last !== 'boolean') throw bad('last must be a boolean');
  const transformVersion = String(body.transform_version || '');
  if (!transformVersion || transformVersion.length > 40) throw bad('transform_version is required');
  if (!Array.isArray(body.rows)) throw bad('rows must be an array');
  if (body.rows.length > MAX_ROWS_PER_CHUNK) throw bad(`rows must be <= ${MAX_ROWS_PER_CHUNK} per chunk`);
  const seen = new Set();
  let totalLines = 0;
  const rows = body.rows.map((r, i) => {
    if (!r || typeof r !== 'object' || Array.isArray(r)) throw bad(`rows[${i}] must be an object`);
    if (typeof r.ne_slip_no !== 'string' && typeof r.ne_slip_no !== 'number') throw bad(`rows[${i}].ne_slip_no must be a string`);
    const slip = String(r.ne_slip_no).trim();
    if (!SLIP_RE.test(slip)) throw bad(`rows[${i}].ne_slip_no has a bad form`);
    if (seen.has(slip)) throw bad(`rows[${i}]: duplicate ne_slip_no ${slip} in one chunk`);
    seen.add(slip);
    if (!r.header || typeof r.header !== 'object' || Array.isArray(r.header)) throw bad(`rows[${i}].header must be an object`);
    if (!Array.isArray(r.lines)) throw bad(`rows[${i}].lines must be an array`);
    if (r.lines.length > MAX_LINES_PER_SLIP) throw bad(`rows[${i}].lines must be <= ${MAX_LINES_PER_SLIP}`);
    if (r.lines.some((l) => !l || typeof l !== 'object' || Array.isArray(l))) throw bad(`rows[${i}].lines must contain objects`);
    totalLines += r.lines.length;
    if (r.header.transform_version !== transformVersion) throw bad(`rows[${i}].header.transform_version differs from the chunk`);
    return { ne_slip_no: slip, header: r.header, lines: r.lines };
  });
  if (totalLines > MAX_LINES_PER_CHUNK) throw bad(`lines must be <= ${MAX_LINES_PER_CHUNK} per chunk`);
  return { runId, batchSeq, chunkIndex, last: body.last, transformVersion, rows };
}

/** Render が受け取った rows の指紋 (送り手の値は使わない) */
export function payloadChecksum(rows) {
  return crypto.createHash('sha256').update(canonicalJson(rows)).digest('hex');
}

/**
 * 1 chunk を適用する。戻り値 = { applied, same, stale, failed: [{ ne_slip_no, error }], stale_slips, run_id, chunk_index, replay, finished }
 * db = pgAdapter / pgliteAdapter (query / exec)。取引はこの中で begin〜commit する。
 * throw: code = BAD_REQUEST (400) / RUN_MISMATCH・CHUNK_MISMATCH・RUN_CLOSED (409) / CHUNK_DEADLINE (503) / その他 (500)
 */
export async function ingestShipmentChunk(db, { companyId = 1, runId, batchSeq, chunkIndex, last, transformVersion, rows, host = 'render', log = () => {}, deadlineMs = DEFAULT_DEADLINE_MS, now = () => Date.now() }) {
  const started = now();
  const checksum = payloadChecksum(rows);
  await db.exec('begin');
  try {
    await db.query(
      `insert into ops.ingest_runs (ingest_run_id, source_system, entity, scope_key, host, started_at, status, source_tz, checksum, format_version, rows_seen, rows_inserted, rows_skipped)
       values ($1, 'ne', 'shipments', 'main', $2, now(), 'running', 'Asia/Tokyo', $3, $4, 0, 0, 0)
       on conflict (ingest_run_id) do nothing`,
      [runId, host, String(batchSeq), transformVersion]);
    const run = (await db.query(`select status, checksum, format_version, pages from ops.ingest_runs where ingest_run_id = $1 for update`, [runId])).rows[0];
    if (run.checksum !== String(batchSeq) || run.format_version !== transformVersion) {
      throw err('RUN_MISMATCH', `run ${runId} was started with batch_seq ${run.checksum} / ${run.format_version}, not ${batchSeq} / ${transformVersion}`);
    }
    // 再送 (同じ chunk_index): 同じ内容なら保存した応答を返す。違う内容は拒む
    const prev = (await db.query(`select payload_checksum, result from ops.ingest_chunks where ingest_run_id = $1 and chunk_index = $2`, [runId, chunkIndex])).rows[0];
    if (prev) {
      if (prev.payload_checksum !== checksum) throw err('CHUNK_MISMATCH', `chunk ${chunkIndex} of run ${runId} was already received with different content`);
      await db.exec('commit');
      log(`chunk ${chunkIndex} replay (same content) → stored result`);
      return { ...prev.result, replay: true };
    }
    if (run.status !== 'running') throw err('RUN_CLOSED', `run ${runId} is already ${run.status}`);
    if (run.pages != null && chunkIndex >= run.pages) throw err('RUN_CLOSED', `run ${runId} ended at chunk ${run.pages - 1}; chunk ${chunkIndex} is beyond it`);

    let applied = 0, same = 0, stale = 0;
    const failed = [], staleSlips = [];
    for (const r of rows) {
      if (now() - started > deadlineMs) throw err('CHUNK_DEADLINE', `chunk ${chunkIndex} exceeded ${deadlineMs} ms after ${applied + same + stale + failed.length} of ${rows.length} slips (send smaller chunks)`);
      await db.exec('savepoint slip');
      try {
        const res = (await db.query(
          `select core.apply_shipment_batch($1::smallint, $2, $3::bigint, $4::jsonb, $5::jsonb) as r`,
          [companyId, r.ne_slip_no, batchSeq, JSON.stringify(r.header), JSON.stringify(r.lines)])).rows[0].r;
        if (res === 'applied') applied++;
        else if (res === 'same') same++;
        else if (res === 'stale') { stale++; staleSlips.push(r.ne_slip_no); }
        else throw new Error(`apply_shipment_batch returned ${res}`);
        await db.exec('release savepoint slip');
      } catch (e) {
        await db.exec('rollback to savepoint slip');
        await db.exec('release savepoint slip');
        failed.push({ ne_slip_no: r.ne_slip_no, error: String(e && e.message ? e.message : e).slice(0, 300) });
      }
    }
    const result = { applied, same, stale, failed, stale_slips: staleSlips, run_id: runId, chunk_index: chunkIndex };
    await db.query(
      `insert into ops.ingest_chunks (ingest_run_id, chunk_index, payload_checksum, rows_seen, rows_applied, rows_same, rows_stale, rows_failed, result)
       values ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)`,
      [runId, chunkIndex, checksum, rows.length, applied, same, stale, failed.length, JSON.stringify(result)]);
    // run の集計を進める。last なら chunk の数 (pages) を固める。0〜pages-1 が全部そろったら閉じる (failed があれば partial)
    await db.query(
      `update ops.ingest_runs set
         rows_seen = coalesce(rows_seen, 0) + $2, rows_inserted = coalesce(rows_inserted, 0) + $3, rows_skipped = coalesce(rows_skipped, 0) + $4,
         failed_ranges = (select coalesce(jsonb_agg(x), '[]'::jsonb) from (select x from jsonb_array_elements(coalesce(failed_ranges, '[]'::jsonb) || $5::jsonb) x limit ${MAX_FAILED_KEPT}) s),
         pages = case when $6 then $7 else pages end
       where ingest_run_id = $1`,
      [runId, rows.length, applied, same + stale, JSON.stringify(failed), !!last, chunkIndex + 1]);
    const closed = (await db.query(
      `update ops.ingest_runs r set
         finished_at = now(), complete = true,
         status = case when c.failed > 0 then 'partial' else 'success' end,
         error = case when c.failed > 0 then c.failed || ' slips failed (see failed_ranges / ops.ingest_chunks)' else null end
       from (select count(*)::int as n, coalesce(sum(rows_failed), 0)::int as failed from ops.ingest_chunks where ingest_run_id = $1) c
       where r.ingest_run_id = $1 and r.status = 'running' and r.pages is not null and c.n = r.pages
       returning r.status`, [runId])).rows[0];
    await db.exec('commit');
    const finished = !!closed;
    log(`chunk ${chunkIndex}${last ? ' (last)' : ''}: rows ${rows.length} applied ${applied} same ${same} stale ${stale} failed ${failed.length} in ${now() - started} ms${finished ? ` → run ${closed.status}` : ''}`);
    return { ...result, replay: false, finished };
  } catch (e) {
    try { await db.exec('rollback'); } catch { /* 取引が既に無い */ }
    throw e;
  }
}
