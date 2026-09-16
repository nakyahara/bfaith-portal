/**
 * ingest/shipments.mjs — miniPC から届いた伝票 (NE) の 1 chunk を Company DB に適用する (Render 側。0013 の core.apply_shipment_batch を呼ぶ)。D5a
 * 共通部 (chunk の検証・run の記録・再送・期限) は ingest/chunk.mjs。ここは伝票の行の形と apply だけ。
 *   rows の要素 = { ne_slip_no, header, lines }。応答の failed は [{ key, ne_slip_no, error }]、stale は stale_keys と stale_slips (同じ内容。送り手 = ne-shipments.mjs)
 */
import { ingestChunk, validateChunkBody, bad, payloadChecksum, RUN_ID_RE, MAX_ROWS_PER_CHUNK, MAX_LINES_PER_ROW, MAX_LINES_PER_CHUNK, DEFAULT_DEADLINE_MS, STATEMENT_TIMEOUT_MS } from './chunk.mjs';

export { payloadChecksum, RUN_ID_RE, MAX_ROWS_PER_CHUNK, MAX_LINES_PER_CHUNK, DEFAULT_DEADLINE_MS, STATEMENT_TIMEOUT_MS };
export const MAX_LINES_PER_SLIP = MAX_LINES_PER_ROW;
export const SLIP_RE = /^[0-9A-Za-z][0-9A-Za-z._-]{0,39}$/;

/** body の形を確かめて正規化する (throw code=BAD_REQUEST → 400) */
export function validateChunk(body) {
  return validateChunkBody(body, (r, i) => {
    if (typeof r.ne_slip_no !== 'string' && typeof r.ne_slip_no !== 'number') throw bad(`rows[${i}].ne_slip_no must be a string`);
    const slip = String(r.ne_slip_no).trim();
    if (!SLIP_RE.test(slip)) throw bad(`rows[${i}].ne_slip_no has a bad form`);
    return { key: slip, ne_slip_no: slip, header: r.header, lines: r.lines };
  });
}

/** 1 chunk を適用する (ingest/chunk.mjs)。戻り値には stale_slips と failed[].ne_slip_no も入れる (送り手の互換) */
export async function ingestShipmentChunk(db, { companyId = 1, rows, ...opts }) {
  const r = await ingestChunk(db, {
    ...opts,
    rows: rows.map((x) => (x.key != null ? x : { ...x, key: String(x.ne_slip_no) })),   // validateChunk を通らない呼び方 (試験) でも鍵を持たせる
    run: { sourceSystem: 'ne', entity: 'shipments', scopeKey: 'main' },
    rowWord: 'slip', labelRow: (x) => ({ ne_slip_no: x.ne_slip_no }),
    apply: async (dbx, row, batchSeq) => (await dbx.query(
      `select core.apply_shipment_batch($1::smallint, $2, $3::bigint, $4::jsonb, $5::jsonb) as r`,
      [companyId, row.ne_slip_no, batchSeq, JSON.stringify(row.header), JSON.stringify(row.lines)])).rows[0].r,
  });
  // D5a が保存した応答 (再送で返る) は stale_slips だけを持つ → どちらの名前でも同じ配列を返す (デプロイをまたぐ再送。Codex D5b-1 R1 #3)
  const stale = r.stale_keys ?? r.stale_slips ?? [];
  return { ...r, stale_keys: stale, stale_slips: stale, failed: r.failed.map((f) => ({ ...f, ne_slip_no: f.ne_slip_no ?? f.key })) };
}
