/**
 * lz-snapshot.mjs — ロジザード用 CSV の影運転の材料 = NE の取得の完了の世代 1 つ + 元のコード (③b-2a。契約 v3 M4)
 *   miniPC で読むだけ: warehouse.db (SQLite) を 1 つの読み取りの取引で (照合 ② と同じ readNeSide)・Company DB (watcher) を 1 つの読み取りの取引で。
 *
 * 元のコード = Company DB の ops.master_ne_codes (kind = product・state = ok) を対応づけに使う (③c の道と同じ)。
 *   照合の回は NE の取得の世代を Company DB に残していない (0034 は observed_at だけ) ので、
 *   「同じ世代の対応か」は中身で確かめる = この世代の書き方 (warehouse.db の raw_ne_code_spellings) から照合と同じ resolveNeCodes で作った対応と、
 *   Company DB の対応が商品ごとに同じときだけ使う。違う・無い = その商品は作れない (理由つき。判定できない)
 * 🚨 NE の取得の印 (完了の時刻・件数・通し番号) が今の中身と合わないときは、全部を作れないにする (途中の取得を材料にしない)
 */
import { normSku } from '../../lib/sku-norm.js';
import { readNeSide, resolveNeCodes } from '../company-db/master-compare/compare-ne.mjs';

export const LZ_SNAPSHOT_FORMAT = 'lz-snap-v1';

/**
 * warehouse.db の側 (読むだけ)。印が今の中身と合うかを確かめる (照合 ② の 3. 材料の信用 と同じ決まり)
 * @returns {{ ok: boolean, reason?: string, marks, products?: Array, entries?: Array, spellings_reason?: string|null }}
 */
export function readNeForLz(dataDir) {
  const ne = readNeSide(dataDir);
  if (ne.error) return { ok: false, reason: ne.error, marks: null };
  const M = ne.meta;
  const marks = {
    products: { at: M.ne_api_products_complete_at ?? null, count: M.ne_api_products_complete_count ?? null, rev: M.ne_api_products_complete_rev ?? null, cur_rev: M.ne_raw_products_rev ?? null },
    sets: { at: M.ne_api_setproducts_complete_at ?? null, count: M.ne_api_setproducts_complete_count ?? null, rev: M.ne_api_setproducts_complete_rev ?? null, cur_rev: M.ne_raw_setproducts_rev ?? null },
  };
  const fail = (reason) => ({ ok: false, reason, marks });
  if (!ne.hasSrc) return fail('no_src_columns');
  if (!marks.products.at || marks.products.rev == null || !marks.sets.at || marks.sets.rev == null) return fail('no_ne_marks');
  if (String(marks.products.cur_rev) !== String(marks.products.rev) || String(marks.sets.cur_rev) !== String(marks.sets.rev)) return fail('ne_written_after_mark');
  if (String(ne.products.length) !== String(marks.products.count) || String(ne.sets.length) !== String(marks.sets.count) || ne.sets.length !== ne.setRowsTotal) return fail('ne_count_mismatch');
  const resolved = resolveNeCodes(ne.spellings);
  return {
    ok: true, marks,
    products: ne.products.map((r) => ({ code: r.code, name: r.name, cost_src: r.cost_src, supplier: r.supplier })),
    entries: resolved.ok ? resolved.entries.filter((e) => e.kind === 'product') : null,
    spellings_reason: resolved.ok ? null : resolved.reason,
  };
}

/**
 * Company DB の側 (watcher・読むだけの取引 1 つ)。db = pgAdapter (query(sql, params) → { rows })
 * @returns {Promise<{ mark: object|null, rows: Array<{ code_norm, state, ne_code }> }>}
 */
export async function readCdbNeCodes(db) {
  await db.query('begin transaction isolation level repeatable read read only');
  try {
    const mark = (await db.query(`select compare_run_id, to_char(observed_at at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') as observed_at, content_hash, counts,
      to_char(recorded_at at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') as recorded_at from ops.master_ne_code_mark where id = 1`)).rows[0] ?? null;
    const rows = (await db.query(`select code_norm, state, ne_code from ops.master_ne_codes where kind = 'product'`)).rows;
    await db.query('commit');
    return { mark, rows };
  } catch (e) { try { await db.query('rollback'); } catch { /* */ } throw e; }
}

/**
 * 2 つの側を合わせて、影運転の材料 (商品ごとの元のコード・値) にする。純粋な関数
 * 商品ごとの理由: no_spelling (この世代で書き方を集めていない) / code_collided / code_invalid / code_not_in_cdb / code_cdb_mismatch / norm_collided (正規化で同じになる別のコード)
 */
export function joinLzSnapshot(ne, cdb, { takenAt }) {
  const snap = { format: LZ_SNAPSHOT_FORMAT, taken_at: takenAt, ne: { ok: ne.ok, reason: ne.reason ?? null, marks: ne.marks, spellings_reason: ne.spellings_reason ?? null },
    cdb: { mark: cdb ? cdb.mark : null, product_codes: cdb ? cdb.rows.length : null }, items: [], counts: {} };
  if (!ne.ok) return snap;
  const own = new Map((ne.entries || []).map((e) => [e.code_norm, e]));
  const pg = new Map((cdb ? cdb.rows : []).map((r) => [r.code_norm, r]));
  const normCount = new Map();
  for (const r of ne.products) { const n = normSku(r.code); normCount.set(n, (normCount.get(n) || 0) + 1); }
  const reasons = {};
  for (const r of ne.products) {
    const norm = normSku(r.code);
    let reason = null, neCode = null;
    const e = own.get(norm), p = pg.get(norm);
    if (!norm || normCount.get(norm) > 1) reason = 'norm_collided';
    else if (!ne.entries) reason = `no_spelling_${ne.spellings_reason || 'unknown'}`;
    else if (!e) reason = 'no_spelling';
    else if (e.state !== 'ok') reason = `code_${e.state}`;
    else if (!cdb) reason = 'code_not_read';
    else if (!p) reason = 'code_not_in_cdb';
    else if (p.state !== 'ok' || p.ne_code !== e.ne_code) reason = 'code_cdb_mismatch';
    else neCode = e.ne_code;
    if (reason) reasons[reason] = (reasons[reason] || 0) + 1;
    snap.items.push({ code_norm: norm || String(r.code), ne_code: neCode, code_reason: reason, name: r.name, cost_src: r.cost_src, supplier: r.supplier });
  }
  snap.items.sort((a, b) => (a.code_norm < b.code_norm ? -1 : a.code_norm > b.code_norm ? 1 : 0));
  snap.counts = { products: ne.products.length, with_code: snap.items.filter((x) => x.ne_code).length, reasons };
  return snap;
}
