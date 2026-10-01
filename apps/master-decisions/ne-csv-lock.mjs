/**
 * ne-csv-lock.mjs — NE に取り込む CSV の鍵と「置き換わった承認の予約を外す」(③b-1。Company DB構想 10 §6.1.1「③b の契約 v3」H3)
 *
 * 判断の API (decide.mjs) と CSV の操作 (ne-csv.mjs) の両方が使う (互いに import し合わないよう、ここに分ける)。
 * 🚨 鍵の順番 = ① CSV の鍵 (取引の advisory lock) → ② 候補の行を指紋の順に for update。どちらの口もこの順 (待ち合いはしてもデッドロックしない)
 * 🚨 マスタの入力 (lib/master-write.mjs・Company DB構想 14 ⑤-1) は SKU ごとの鍵 (SKU_LOCK_SQL・sku_id の順) → CSV の鍵 → 行 の順。
 *    CSV の行を作る・予約する口が SKU ごとの鍵も取るなら、必ず CSV の鍵より前に取る (後で取るとマスタの入力と待ち合う)
 */

/** CSV の鍵 (取引の終わりで外れる) */
export const CSV_LOCK_SQL = `select pg_advisory_xact_lock(hashtext('ops.ne_csv'))`;
/** SKU ごとの鍵 ($1 = sku_id。取引の終わりで外れる)。CSV の鍵より前に、sku_id の小さい順に取る */
export const SKU_LOCK_SQL = `select pg_advisory_xact_lock(hashtextextended('core.sku:' || $1::text, 0))`;

/** 0040 (CSV の記録の表) が入っているか。入る前の DB でも判断の API は今までどおり動く */
export async function csvApplied(db) {
  return (await db.query(`select to_regclass('ops.ne_csv_export_rows') is not null as ok`)).rows[0].ok;
}

/**
 * 新しい判断 (承認・却下・取り消し) で置き換わった承認の予約を外す。(SKU・列・子) の有効な予約を全部外し (理由 superseded)、
 * まだ申告していないファイル (made / checked) は void にして残りの行の予約も外す (理由 void)。申告したファイルはそのまま (行は「取り消し」)。
 * 判断の出来事を書いたのと同じ取引の中で呼ぶ
 * @returns {Promise<{ released: number, voided: number[] }>}
 */
export async function releaseSuperseded(db, { code_norm, col, child = null, actor }) {
  const rel = (await db.query(`update ops.ne_csv_export_rows set reserved = false, released_at = now(), release_reason = 'superseded'
     where reserved and code_norm = $1 and col = $2 and coalesce(child, '') = coalesce($3, '') returning export_id`, [code_norm, col, child])).rows;
  const ids = [...new Set(rel.map((r) => Number(r.export_id)))].sort((a, b) => a - b);
  const voided = [];
  for (const id of ids) {
    const v = (await db.query(`update ops.ne_csv_exports set state = 'void', void_at = now(), void_reason = 'superseded', void_by = $2
       where export_id = $1 and state in ('made', 'checked') returning export_id`, [id, actor])).rows;
    if (!v.length) continue;
    voided.push(id);
    await db.query(`update ops.ne_csv_export_rows set reserved = false, released_at = now(), release_reason = 'void' where export_id = $1 and reserved`, [id]);
  }
  return { released: rel.length, voided };
}
