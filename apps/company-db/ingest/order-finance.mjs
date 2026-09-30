/**
 * ingest/order-finance.mjs — miniPC から届いた注文 (疑似注文) の財務の 1 chunk を Company DB に適用する (Render 側。0043 の core.apply_order_finance_batch を呼ぶ)。F2b-1
 * 共通部 (chunk の検証・run の記録・再送・期限) は ingest/chunk.mjs。ここは財務の行の形と apply だけ。
 *   rows の要素 = { mall, scope_key, mall_order_no, header: { transform_version, set_checksum }, lines: [財務の行] }。1 run は 1 モール × 1 scope (entity = 'order_finance')
 *   🚨 集合の指紋は **受け口が内容から計算し直す** (finance/order-finance-checksum.mjs = 送り手と同じ 1 つの関数)。送り手の申告 (header.set_checksum) と違えば その注文は 400
 * 設計 = AI_reference『CompanyDB構想/12_Amazon財務のCompanyDB取込_設計_20260929.md』§3.2 / §4.6
 */
import { ingestChunk, validateChunkBody, bad } from './chunk.mjs';
import { MALLS, SCOPE_RE, orderKey } from './orders.mjs';
import { validateFinanceRows, orderFinanceChecksum, financeRowsFormat, assertVersionForm, versionHasClass, isPseudoOrderNo, CLASS_COLUMNS } from '../finance/order-finance-checksum.mjs';

export const FINANCE_MALLS = MALLS.filter((m) => m !== 'other');   // 0043 の mall の CHECK
// 本物の注文番号 / 疑似注文 '-:YYYY-MM-DD'。🚨 返送 (RemovalComplete / FBA Removal Order) の注文番号は + / を含む (例 '+3gubNop3S'・'a+aKPxfQ/X'。
//   本番の決済に 26 注文・2026-09-29 に miniPC の 8 月の見積りで見つけた) → + / = も通す。先頭は '-' 以外 (疑似注文と重ならない)
export const FINANCE_ORDER_NO_RE = /^([0-9A-Za-z+/=][0-9A-Za-z._:+/=-]{0,60}|-:\d{4}-\d{2}-\d{2})$/;

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
    let content, format;
    try { format = financeRowsFormat(r.lines); assertVersionForm(r.header.transform_version, format); content = validateFinanceRows(no, r.lines); } catch (e) { throw bad(`rows[${i}] (${no}): ${e.message}`); }
    // 🚨 古い送り手 (0047 の前の版 = 4 列の鍵が無い) の行 = 指紋は旧い形の列で計算し (送り手の申告と合わせる)、正規化した行からも 4 列を外す
    //    (送り手の受領記録の指紋 (receiptRows = この関数の戻り) と Render の指紋を、古い送り手と新しい受け口の組でも同じにする =
    //     「Render が復元された」と誤って台帳を空にしない)。保存するときは 0047 の apply が 4 列を 0 にする (既存の行と同じ)
    const legacy = format === 'legacy';
    const checksum = orderFinanceChecksum(content, { legacy });
    // 指紋に入れない付け足し (元の最終計上時刻・行の指紋) は送り手の値を確かめて引き継ぐ
    const lines = content.map((c, j) => {
      const src = r.lines[j];
      if (typeof src.source_updated_at !== 'string' || Number.isNaN(Date.parse(src.source_updated_at))) throw bad(`rows[${i}].lines[${j}].source_updated_at must be a timestamp`);
      if (typeof src.content_hash !== 'string' || !src.content_hash || src.content_hash.length > 128) throw bad(`rows[${i}].lines[${j}].content_hash must be a string`);
      const out = { ...c, source_updated_at: src.source_updated_at, content_hash: src.content_hash };
      if (legacy) for (const k of CLASS_COLUMNS) delete out[k];
      return out;
    });
    if (r.header.set_checksum !== checksum) throw bad(`rows[${i}] (${no}): set_checksum differs from the content (sent ${r.header.set_checksum}, computed ${checksum})`);
    return { key: orderKey(r.mall, r.scope_key, no), mall: r.mall, scope_key: r.scope_key, mall_order_no: no, pseudo: isPseudoOrderNo(no), set_checksum: checksum, lines };
  });
  return { ...v, mall, scope };
}

/** 0047 (分けられない部品の 4 列) が Company DB に入っているか */
export async function classColumnsReady(db) {
  const r = (await db.query(`select count(*)::int as n from information_schema.columns
     where table_schema = 'core' and table_name = 'order_finance_daily' and column_name in (${CLASS_COLUMNS.map((c) => `'${c}'`).join(', ')})`)).rows[0];
  return Number(r.n) === CLASS_COLUMNS.length;
}

/** 1 chunk を適用する (ingest/chunk.mjs)。content_hash / source_updated_at は送り手の値 (行の中身の指紋は集合の checksum で守る) */
export async function ingestOrderFinanceChunk(db, { companyId = 1, mall, scope, transformVersion, ...opts }) {
  const m = mall ?? (opts.rows[0] && opts.rows[0].mall), s = scope ?? (opts.rows[0] && opts.rows[0].scope_key);
  if (!FINANCE_MALLS.includes(m) || !s) throw bad('mall / scope are required for an order finance chunk');
  // 🚨 新しい形 (4 列あり) の行を 0047 の前の apply に渡すと 4 列が黙って落ち、受領記録の指紋には入る (= 送り直しても 'same' で直らない) →
  //    0047 の適用前は受けない (409 NOT_MIGRATED = 送り手の回は ❌・次の回に送り直す)。古い形の行はそのまま受ける
  if ((versionHasClass(transformVersion) || opts.rows.some((x) => x.lines.length && CLASS_COLUMNS.every((c) => Object.hasOwn(x.lines[0], c)))) && !(await classColumnsReady(db))) {
    throw Object.assign(new Error('not_migrated: migration 0047 (core.order_finance_daily の分けられない部品の 4 列) is not applied'), { code: 'NOT_MIGRATED' });
  }
  // 🚨 旧い形の版への戻し (downgrade) を拒む (#1554 Codex R1 High): 受領記録が今の形の版 (amazon_finance_v2 以上) の注文を、旧い版の chunk で置き換えると
  //    4 列が 0 に戻り、分けられない部品の数が黙って消える (正式な利益が fail-open)。chunk ごと 409 (送り手は ❌)。
  //    0047 の apply も同じ規則で例外にする (同時の書き込みの保険)。墓石 (空の集合) も旧い版なら拒む (受領記録の版を戻さない)
  if (!versionHasClass(transformVersion) && opts.rows.length) {
    const nos = opts.rows.map((x) => x.mall_order_no);
    const cur = (await db.query(`select mall_order_no, transform_version from core.order_finance_receipts
       where company_id = $1 and mall = $2 and scope_key = $3 and mall_order_no = any($4::text[])`, [companyId, m, s, nos])).rows;
    const down = cur.filter((x) => versionHasClass(x.transform_version));
    if (down.length) {
      throw Object.assign(new Error(`downgrade: ${down.length} order(s) were received with ${down[0].transform_version} (e.g. ${down[0].mall_order_no}); `
        + `transform_version ${transformVersion} would drop the unclassified component counts (do not go back to the old sender)`), { code: 'DOWNGRADE' });
    }
  }
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
