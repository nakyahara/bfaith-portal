/**
 * ingest/shipments.mjs — miniPC から届いた伝票 (NE) の 1 chunk を Company DB に適用する (Render 側。0013 の core.apply_shipment_batch を呼ぶ)。D5a
 *
 * 契約 (08 §4.7 を HTTP に写したもの。送り手 = apps/company-db/push/ne-shipments.mjs):
 *   body = { run_id, batch_seq, chunk_index, chunk_count, transform_version, rows: [{ ne_slip_no, header, lines }] }
 *   - batch_seq = 送り手の世代 (単調増加。内容が同じでも進める)。古い世代は伝票ごとに 'stale' で拒まれる (apply_shipment_batch)
 *   - 1 chunk = 1 取引。伝票ごとに savepoint を切り、失敗した伝票だけを failed に積んで他は commit する
 *     (1 伝票の不良で 1 日分を止めない。ただし failed が 1 つでもあれば送り手はカーソルを進めず exit 1 = 翌日また送る + 朝の通知に ❌)
 *   - 再送は冪等 (同じ世代・同じ内容 → 'same')。chunk の途中で HTTP が切れて送り手が再送しても二重にはならない
 *   - ops.ingest_runs に run を残す (source_system='ne', entity='shipments'。最初の chunk で running、最後の chunk で success / partial)。
 *     rows_seen / rows_inserted (applied) / rows_skipped (same + stale) を chunk ごとに足す。failed は failed_ranges (先頭 200 件)
 *
 * 🚨 mirror (Render の SQLite) は経由しない: 受け皿の関数が世代・冪等・明細集合の置換を担うので「公開マーカー」は要らない
 *    (chunk が commit されるか、されないかの 2 択)。写しを SQLite に残すと年 50 万伝票ぶん Render のディスクを食う。
 */

export const RUN_ID_RE = /^ship_[0-9]{15}_[0-9a-f]{6}$/;
export const MAX_ROWS_PER_CHUNK = 1000;
const MAX_FAILED_KEPT = 200;

function bad(message) { const e = new Error(message); e.code = 'BAD_REQUEST'; return e; }

/** body の形を確かめて正規化する (throw = 400) */
export function validateChunk(body) {
  if (!body || typeof body !== 'object' || Array.isArray(body)) throw bad('body must be a JSON object');
  const runId = String(body.run_id || '');
  if (!RUN_ID_RE.test(runId)) throw bad('bad run_id (ship_<15 digits>_<6 hex>)');
  const batchSeq = Number(body.batch_seq);
  if (!Number.isInteger(batchSeq) || batchSeq <= 0 || batchSeq > Number.MAX_SAFE_INTEGER) throw bad('batch_seq must be a positive integer');
  const chunkIndex = Number(body.chunk_index), chunkCount = Number(body.chunk_count);
  if (!Number.isInteger(chunkIndex) || !Number.isInteger(chunkCount) || chunkCount <= 0 || chunkIndex < 0 || chunkIndex >= chunkCount) throw bad('chunk_index / chunk_count are inconsistent');
  const transformVersion = String(body.transform_version || '');
  if (!transformVersion) throw bad('transform_version is required');
  if (!Array.isArray(body.rows)) throw bad('rows must be an array');
  if (body.rows.length > MAX_ROWS_PER_CHUNK) throw bad(`rows must be <= ${MAX_ROWS_PER_CHUNK} per chunk`);
  const seen = new Set();
  const rows = body.rows.map((r, i) => {
    if (!r || typeof r !== 'object') throw bad(`rows[${i}] must be an object`);
    const slip = String(r.ne_slip_no || '').trim();
    if (!slip) throw bad(`rows[${i}].ne_slip_no is required`);
    if (seen.has(slip)) throw bad(`rows[${i}]: duplicate ne_slip_no ${slip} in one chunk`);
    seen.add(slip);
    if (!r.header || typeof r.header !== 'object' || Array.isArray(r.header)) throw bad(`rows[${i}].header must be an object`);
    if (!Array.isArray(r.lines)) throw bad(`rows[${i}].lines must be an array`);
    if (r.header.transform_version !== transformVersion) throw bad(`rows[${i}].header.transform_version differs from the chunk`);
    return { ne_slip_no: slip, header: r.header, lines: r.lines };
  });
  return { runId, batchSeq, chunkIndex, chunkCount, transformVersion, rows };
}

/**
 * 1 chunk を適用する。戻り値 = { applied, same, stale, failed: [{ ne_slip_no, error }], run_id, chunk_index, finished }
 * db = pgAdapter / pgliteAdapter (query / exec)。取引はこの中で begin〜commit する
 */
export async function ingestShipmentChunk(db, { companyId = 1, runId, batchSeq, chunkIndex, chunkCount, transformVersion, rows, host = 'render', log = () => {} }) {
  const isLast = chunkIndex === chunkCount - 1;
  await db.exec('begin');
  try {
    await db.query(
      `insert into ops.ingest_runs (ingest_run_id, source_system, entity, scope_key, host, started_at, status, source_tz, checksum, format_version, rows_seen, rows_inserted, rows_skipped, pages)
       values ($1, 'ne', 'shipments', 'main', $2, now(), 'running', 'Asia/Tokyo', $3, $4, 0, 0, 0, $5)
       on conflict (ingest_run_id) do nothing`,
      [runId, host, String(batchSeq), transformVersion, chunkCount]);
    let applied = 0, same = 0, stale = 0;
    const failed = [];
    for (const r of rows) {
      await db.exec('savepoint slip');
      try {
        const res = (await db.query(
          `select core.apply_shipment_batch($1::smallint, $2, $3::bigint, $4::jsonb, $5::jsonb) as r`,
          [companyId, r.ne_slip_no, batchSeq, JSON.stringify(r.header), JSON.stringify(r.lines)])).rows[0].r;
        if (res === 'applied') applied++;
        else if (res === 'same') same++;
        else if (res === 'stale') stale++;
        else throw new Error(`apply_shipment_batch returned ${res}`);
        await db.exec('release savepoint slip');
      } catch (e) {
        await db.exec('rollback to savepoint slip');
        await db.exec('release savepoint slip');
        failed.push({ ne_slip_no: r.ne_slip_no, error: String(e && e.message ? e.message : e).slice(0, 300) });
      }
    }
    // run の集計を進める。最後の chunk で閉じる (failed があれば partial)。failed_ranges には失敗した伝票を積む (先頭 200 件)
    await db.query(
      `update ops.ingest_runs set
         rows_seen = coalesce(rows_seen, 0) + $2, rows_inserted = coalesce(rows_inserted, 0) + $3, rows_skipped = coalesce(rows_skipped, 0) + $4,
         failed_ranges = (select coalesce(jsonb_agg(x), '[]'::jsonb) from (select x from jsonb_array_elements(coalesce(failed_ranges, '[]'::jsonb) || $5::jsonb) x limit ${MAX_FAILED_KEPT}) s),
         finished_at = case when $6 then now() else finished_at end,
         complete = case when $6 then true else complete end,
         status = case when $6 then (case when jsonb_array_length(coalesce(failed_ranges, '[]'::jsonb) || $5::jsonb) > 0 then 'partial' else 'success' end) else status end,
         error = case when $6 and jsonb_array_length(coalesce(failed_ranges, '[]'::jsonb) || $5::jsonb) > 0
                      then jsonb_array_length(coalesce(failed_ranges, '[]'::jsonb) || $5::jsonb) || ' slips failed (see failed_ranges)' else error end
       where ingest_run_id = $1`,
      [runId, rows.length, applied, same + stale, JSON.stringify(failed), isLast]);
    await db.exec('commit');
    log(`chunk ${chunkIndex + 1}/${chunkCount}: rows ${rows.length} applied ${applied} same ${same} stale ${stale} failed ${failed.length}`);
    return { applied, same, stale, failed, run_id: runId, chunk_index: chunkIndex, finished: isLast };
  } catch (e) {
    try { await db.exec('rollback'); } catch { /* 取引が既に無い */ }
    throw e;
  }
}
