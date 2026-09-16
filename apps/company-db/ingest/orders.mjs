/**
 * ingest/orders.mjs — miniPC から届いたモールの注文の 1 chunk を Company DB に適用する (Render 側。0013 の core.apply_order_batch を呼ぶ)。D5b
 * 共通部 (chunk の検証・run の記録・再送・期限) は ingest/chunk.mjs。ここは注文の行の形と apply だけ。
 *   rows の要素 = { mall, scope_key, mall_order_no, header, lines }。1 run は 1 モール × 1 scope (run の source_system = mall、entity = 'orders')
 *   応答の failed は [{ key, mall_order_no, error }] (key = mall|scope|注文番号)、stale は stale_keys
 */
import { ingestChunk, validateChunkBody, bad, payloadChecksum } from './chunk.mjs';

export { payloadChecksum };
export const MALLS = ['amazon', 'rakuten', 'yahoo', 'aupay', 'qoo10', 'linegift', 'mercari', 'other'];
export const ORDER_NO_RE = /^[0-9A-Za-z][0-9A-Za-z._:-]{0,60}$/;
export const SCOPE_RE = /^[0-9A-Za-z][0-9A-Za-z_-]{0,30}$/;
export const orderKey = (mall, scope, no) => `${mall}|${scope}|${no}`;

/** body の形を確かめて正規化する (throw code=BAD_REQUEST → 400)。mall / scope は chunk の中で 1 つ (run = 1 モール × 1 scope) */
export function validateChunk(body) {
  let mall = null, scope = null;
  const v = validateChunkBody(body, (r, i) => {
    if (!MALLS.includes(r.mall)) throw bad(`rows[${i}].mall is not a known mall`);
    if (typeof r.scope_key !== 'string' || !SCOPE_RE.test(r.scope_key)) throw bad(`rows[${i}].scope_key has a bad form`);
    if (typeof r.mall_order_no !== 'string' && typeof r.mall_order_no !== 'number') throw bad(`rows[${i}].mall_order_no must be a string`);
    const no = String(r.mall_order_no).trim();
    if (!ORDER_NO_RE.test(no)) throw bad(`rows[${i}].mall_order_no has a bad form`);
    if (mall == null) { mall = r.mall; scope = r.scope_key; }
    else if (r.mall !== mall || r.scope_key !== scope) throw bad(`rows[${i}]: one chunk must hold one mall / scope (${mall}/${scope})`);
    return { key: orderKey(r.mall, r.scope_key, no), mall: r.mall, scope_key: r.scope_key, mall_order_no: no, header: r.header, lines: r.lines };
  });
  return { ...v, mall, scope };
}

/** 1 chunk を適用する (ingest/chunk.mjs)。mall / scope は validateChunk が決めたもの (rows が空なら body の mall / scope を使う) */
export async function ingestOrderChunk(db, { companyId = 1, mall, scope, ...opts }) {
  const m = mall ?? (opts.rows[0] && opts.rows[0].mall), s = scope ?? (opts.rows[0] && opts.rows[0].scope_key);
  if (!MALLS.includes(m) || !s) throw bad('mall / scope are required for an orders chunk');
  return ingestChunk(db, {
    ...opts,
    run: { sourceSystem: m, entity: 'orders', scopeKey: s },
    rowWord: 'order', labelRow: (x) => ({ mall_order_no: x.mall_order_no }),
    apply: async (dbx, row, batchSeq) => (await dbx.query(
      `select core.apply_order_batch($1::smallint, $2, $3, $4, $5::bigint, $6::jsonb, $7::jsonb) as r`,
      [companyId, row.mall, row.scope_key, row.mall_order_no, batchSeq, JSON.stringify(row.header), JSON.stringify(row.lines)])).rows[0].r,
  });
}
