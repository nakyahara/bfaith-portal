/**
 * ingest/order-finance.mjs — miniPC から届いた注文 (疑似注文) の財務の 1 chunk を Company DB に適用する (Render 側。0043 の core.apply_order_finance_batch を呼ぶ)。F2b-1
 * 共通部 (chunk の検証・run の記録・再送・期限) は ingest/chunk.mjs。ここは財務の行の形と apply だけ。
 *   rows の要素 = { mall, scope_key, mall_order_no, header: { transform_version, set_checksum }, lines: [財務の行] }。1 run は 1 モール × 1 scope (entity = 'order_finance')
 *   🚨 集合の指紋は **受け口が内容から計算し直す** (finance/order-finance-checksum.mjs = 送り手と同じ 1 つの関数)。送り手の申告 (header.set_checksum) と違えば その注文は 400
 * 設計 = AI_reference『CompanyDB構想/12_Amazon財務のCompanyDB取込_設計_20260929.md』§3.2 / §4.6
 */
import { ingestChunk, validateChunkBody, bad } from './chunk.mjs';
import { MALLS, SCOPE_RE, orderKey } from './orders.mjs';
import { validateFinanceRows, orderFinanceChecksum, isPseudoOrderNo } from '../finance/order-finance-checksum.mjs';

export const FINANCE_MALLS = MALLS.filter((m) => m !== 'other');   // 0043 の mall の CHECK
export const FINANCE_ORDER_NO_RE = /^([0-9A-Za-z][0-9A-Za-z._:-]{0,60}|-:\d{4}-\d{2}-\d{2})$/;   // 本物の注文番号 / 疑似注文 '-:YYYY-MM-DD'

/** body の形を確かめて正規化する (throw code=BAD_REQUEST → 400)。mall / scope は chunk の中で 1 つ */
export function validateFinanceChunk(body) {
  let mall = null, scope = null;
  const v = validateChunkBody(body, (r, i) => {
    if (!FINANCE_MALLS.includes(r.mall)) throw bad(`rows[${i}].mall is not a known mall`);
    if (typeof r.scope_key !== 'string' || !SCOPE_RE.test(r.scope_key)) throw bad(`rows[${i}].scope_key has a bad form`);
    const no = typeof r.mall_order_no === 'string' ? r.mall_order_no.trim() : '';
    if (!FINANCE_ORDER_NO_RE.test(no)) throw bad(`rows[${i}].mall_order_no has a bad form`);
    if (mall == null) { mall = r.mall; scope = r.scope_key; }
    else if (r.mall !== mall || r.scope_key !== scope) throw bad(`rows[${i}]: one chunk must hold one mall / scope (${mall}/${scope})`);
    let content;
    try { content = validateFinanceRows(no, r.lines); } catch (e) { throw bad(`rows[${i}] (${no}): ${e.message}`); }
    const checksum = orderFinanceChecksum(content);
    // 指紋に入れない付け足し (元の最終計上時刻・行の指紋) は送り手の値を確かめて引き継ぐ
    const lines = content.map((c, j) => {
      const src = r.lines[j];
      if (typeof src.source_updated_at !== 'string' || Number.isNaN(Date.parse(src.source_updated_at))) throw bad(`rows[${i}].lines[${j}].source_updated_at must be a timestamp`);
      if (typeof src.content_hash !== 'string' || !src.content_hash || src.content_hash.length > 128) throw bad(`rows[${i}].lines[${j}].content_hash must be a string`);
      return { ...c, source_updated_at: src.source_updated_at, content_hash: src.content_hash };
    });
    if (r.header.set_checksum !== checksum) throw bad(`rows[${i}] (${no}): set_checksum differs from the content (sent ${r.header.set_checksum}, computed ${checksum})`);
    return { key: orderKey(r.mall, r.scope_key, no), mall: r.mall, scope_key: r.scope_key, mall_order_no: no, pseudo: isPseudoOrderNo(no), set_checksum: checksum, lines };
  });
  return { ...v, mall, scope };
}

/** 1 chunk を適用する (ingest/chunk.mjs)。content_hash / source_updated_at は送り手の値 (行の中身の指紋は集合の checksum で守る) */
export async function ingestOrderFinanceChunk(db, { companyId = 1, mall, scope, transformVersion, ...opts }) {
  const m = mall ?? (opts.rows[0] && opts.rows[0].mall), s = scope ?? (opts.rows[0] && opts.rows[0].scope_key);
  if (!FINANCE_MALLS.includes(m) || !s) throw bad('mall / scope are required for an order finance chunk');
  return ingestChunk(db, {
    ...opts, transformVersion,
    run: { sourceSystem: m, entity: 'order_finance', scopeKey: s },
    rowWord: 'order', labelRow: (x) => ({ mall_order_no: x.mall_order_no }),
    apply: async (dbx, row, batchSeq) => (await dbx.query(
      `select core.apply_order_finance_batch($1::smallint, $2, $3, $4, $5::bigint, $6, $7, $8::jsonb) as r`,
      [companyId, row.mall, row.scope_key, row.mall_order_no, batchSeq, row.set_checksum, transformVersion,
        JSON.stringify(row.lines)])).rows[0].r,
  });
}
