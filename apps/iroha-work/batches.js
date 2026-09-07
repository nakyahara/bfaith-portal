/**
 * 作業の「まとまり」(要件 §AB)。
 *
 * 1 枚のカード (f_iroha_tasks = 入荷受付の 1 行) の下に、**独立して作業・完成・棚入れできる単位**を持つ。
 *
 *   ふだんは 1 枚のカードに **まとまり 1 つ**。見え方も操作もいままでと同じ。
 *   2 つ以上になるのは次のときだけ:
 *     - 一部を外部施設 (羅針盤・ワークセンター) に預ける
 *     - 1 つの入荷に期限の違う物が混ざっている (まれ)
 *
 * ⭐**カードは分割しない**。入荷受付の 1 行との 1 対 1 (destination_id UNIQUE) を壊さないため。
 *   写真・作業時間・ラベル待ちはカードに付いたままなので、「どちらに付けるか」で悩む場面が起きない。
 *
 * この PR (土台) でやること = 表を作り、**すべてのカードにまとまりを 1 つ用意する**ところまで。
 * 画面はまだ何も変わらない。
 */
// ⚠db.js がこの module を読むので、**こちらから db.js を読み返さない** (循環にしない)。
//   接続は必ず引数で渡す。読む側 (service.js) が getDB() を渡す
import { LEGACY_ON_HOLD } from './tasks.js';

const utcNow = () => new Date().toISOString();

/**
 * カードの進捗 → まとまりの作業状態。
 * 終了 (closed) は理由で分かれる: 棚入完了 = done / 取消・対象外 = cancelled。
 * 旧「保留」(2026-09-05 まで進捗に混ざっていた) は作業中として移す。
 */
export function batchStatusOfTask(task) {
  if (task.status === 'closed') return task.close_reason === 'stocked' ? 'done' : 'cancelled';
  if (task.status === LEGACY_ON_HOLD) return 'in_progress';
  return task.status;
}

/**
 * そのカードに「まとまり」が 1 つも無ければ 1 つ作る (冪等)。
 * ⭐必ず**呼び出し側の書き込みトランザクション (BEGIN IMMEDIATE) の中で**呼ぶこと。
 *   カードだけできて まとまり が無い瞬間を作らないため。
 *   ⚠「あるか見る → 無ければ入れる」の間に別の接続が入れる隙間を、索引にも守らせている
 *   (ON CONFLICT DO NOTHING)。呼び出し側がトランザクションを忘れても行が重複しない (Codex R1 中6)。
 *
 * 移行で入れる できた数 (`good_qty`) には `good_qty_source = 'migrated'` を付ける。
 * 移行前は棚入待ちにした瞬間に予定数で上書きしていたので、**実績として信用できない**ため
 * (要件 §AB-3)。人が数えた値は 'counted' になる。
 *
 * @returns {number} 作った行数 (0 = すでにあった)
 */
export function ensureBatchForTask(db, task) {
  if (!task || task.id == null) return 0;
  const has = db.prepare('SELECT 1 FROM f_iroha_task_batches WHERE task_id = ? LIMIT 1').get(task.id);
  if (has) return 0;
  const now = utcNow();
  const done = task.done_qty ?? null;
  const info = db.prepare(`INSERT INTO f_iroha_task_batches
      (task_id, seq, planned_qty, facility_code, expiry, work_status, good_qty, good_qty_source, created_at, updated_at)
    VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(task_id, seq) DO NOTHING`)
    .run(task.id, task.qty ?? null, task.facility_code ?? null, task.expiry ?? null,
      batchStatusOfTask(task), done, done == null ? null : 'migrated', now, now);
  return info.changes;
}

/**
 * まとまりを持っていないカード全部に 1 つずつ用意する (起動時。冪等)。
 * カードが先にできて まとまり が無い状態は、作成経路の取りこぼしや古い DB でしか起きないが、
 * **読むだけの画面で作らない**ためにここでまとめて直す (要件: 読むだけの画面では DB を変えない)。
 * @returns {number} 作った行数
 */
export function backfillBatches(db) {
  // ⭐**探すところから作るところまで同じトランザクション**の中で。
  //   外で読んでから書くと、その間に別のプロセスが進捗を変えたとき**古い状態を写して固定してしまう**
  //   (まとまりができた後は探す対象から外れるので、ずれが残り続ける — Codex R1 重大1)
  const made = db.transaction(() => {
    const rows = db.prepare(`SELECT t.* FROM f_iroha_tasks t
      WHERE NOT EXISTS (SELECT 1 FROM f_iroha_task_batches b WHERE b.task_id = t.id)`).all();
    let n = 0;
    for (const t of rows) n += ensureBatchForTask(db, t);
    return n;
  }).immediate();
  if (made > 0) console.log(`[iroha-work] 作業のまとまりを ${made} 件のカードに用意しました`);
  return made;
}

/** そのカードのまとまり (seq 順)。まとまりが 1 つなら、いままでどおりの 1 枚のカードとして扱う */
export function listBatchesOfTask(db, taskId) {
  const n = Number(taskId);
  if (!Number.isInteger(n) || n <= 0) return [];
  return db.prepare('SELECT * FROM f_iroha_task_batches WHERE task_id = ? ORDER BY seq').all(n);
}

/** 複数カードのまとまりをまとめて引く (一覧で N+1 にしない)。Map<task_id, batch[]> */
export function batchesByTask(db, taskIds) {
  const ids = [...new Set((taskIds || []).map(Number).filter((n) => Number.isInteger(n) && n > 0))];
  const out = new Map();
  if (ids.length === 0) return out;
  const rows = db.prepare(`SELECT * FROM f_iroha_task_batches WHERE task_id IN (${ids.map(() => '?').join(',')}) ORDER BY task_id, seq`).all(...ids);
  for (const r of rows) {
    if (!out.has(r.task_id)) out.set(r.task_id, []);
    out.get(r.task_id).push(r);
  }
  return out;
}

/**
 * 次の seq (そのカードの中の並び順)。
 *
 * ⭐**まとまりは消さない**。要らなくなったら `work_status = 'cancelled'` にする。
 *   消さないので、この番号が使い回されることもない。
 * ⭐履歴・ラベル・預けが指すのは **`id` (表ぜんぶで通し番号・再利用されない)**。`seq` は並び順だけ。
 *   だから万一 seq が重なっても、どのまとまりの記録かは分からなくならない (要件 §AB-13)。
 */
export function nextSeq(db, taskId) {
  const r = db.prepare('SELECT MAX(seq) m FROM f_iroha_task_batches WHERE task_id = ?').get(taskId);
  return (r && r.m ? r.m : 0) + 1;
}

/**
 * ⭐できた数・作れなかった数を**まとまり**に書き、カードの done_qty をその合計に直す (要件 §AB-3)。
 *
 * `good_qty` / `loss_qty` は NULL = まだ数えていない。**0 と区別する**。
 * 予定 (planned_qty) より多い数も入る — 1010 個できることが実際にある。
 * ⚠必ず呼び出し側の書き込みトランザクションの中で。
 *
 * @param {object} counts { goodQty, lossQty, note } — undefined の項目は触らない
 * @returns {boolean} 変えたか
 */
export function recordBatchCounts(db, batchId, { goodQty, lossQty, note } = {}) {
  const b = db.prepare('SELECT * FROM f_iroha_task_batches WHERE id = ?').get(batchId);
  if (!b) return false;
  const nextGood = goodQty === undefined ? (b.good_qty ?? null) : goodQty;
  const nextLoss = lossQty === undefined ? (b.loss_qty ?? null) : lossQty;
  const nextNote = note === undefined ? (b.variance_note ?? null) : (note || null);
  // 数と出どころは必ず対 (DB の CHECK と同じ約束)。人が入れたものは 'counted'
  const nextSrc = nextGood == null ? null
    : (goodQty === undefined ? (b.good_qty_source ?? 'counted') : 'counted');
  if (nextGood === (b.good_qty ?? null) && nextLoss === (b.loss_qty ?? null)
    && nextNote === (b.variance_note ?? null) && nextSrc === (b.good_qty_source ?? null)) return false;
  db.prepare(`UPDATE f_iroha_task_batches
      SET good_qty = ?, loss_qty = ?, good_qty_source = ?, variance_note = ?, version = version + 1, updated_at = ?
    WHERE id = ?`).run(nextGood, nextLoss, nextSrc, nextNote, new Date().toISOString(), batchId);
  recomputeTaskDoneQty(db, b.task_id);
  return true;
}

/**
 * カードの done_qty を、まとまりのできた数の**合計**に直す。
 *
 * ⭐カードの done_qty は**まとまりから出す控え**。手で書き換える正本にしない (要件 §AB-1)。
 *   1 つも数えていなければ NULL のまま (0 で代用しない)。
 */
export function recomputeTaskDoneQty(db, taskId) {
  const r = db.prepare(`SELECT COUNT(good_qty) n, SUM(good_qty) s FROM f_iroha_task_batches
    WHERE task_id = ? AND work_status <> 'cancelled'`).get(taskId);
  const total = r && r.n > 0 ? r.s : null;
  return db.prepare('UPDATE f_iroha_tasks SET done_qty = ? WHERE id = ? AND (done_qty IS NOT ?)')
    .run(total, taskId, total).changes;
}

/** そのカードの「いま作業しているまとまり」。1 つしか無ければそれ (要件 §AB-1: ふだんは 1 つ) */
export function soleBatchOfTask(db, taskId) {
  const rows = db.prepare("SELECT * FROM f_iroha_task_batches WHERE task_id = ? AND work_status <> 'cancelled' ORDER BY seq").all(taskId);
  return rows.length === 1 ? rows[0] : null;
}

/**
 * カードの進捗が変わったとき、**まとまりが 1 つだけ**ならその作業状態も合わせる。
 *
 * ⭐これは移行のあいだの橋渡し。まとまりが 2 つ以上になったら**まとまり側が正本**になり、
 *   カードの進捗はそちらから導出する (要件 §AB-1)。だから 2 つ以上のときは触らない。
 * ⚠必ずカードを更新したのと**同じトランザクションの中で**呼ぶこと。
 *
 * @returns {number} 直した行数 (0 = まとまりが複数、または変化なし)
 */
export function syncSingleBatchStatus(db, taskId, task) {
  const rows = db.prepare('SELECT id, work_status FROM f_iroha_task_batches WHERE task_id = ?').all(taskId);
  if (rows.length !== 1) return 0;
  const want = batchStatusOfTask(task);
  if (rows[0].work_status === want) return 0;
  return db.prepare('UPDATE f_iroha_task_batches SET work_status = ?, version = version + 1, updated_at = ? WHERE id = ?')
    .run(want, new Date().toISOString(), rows[0].id).changes;
}

/**
 * カードごとの数のまとめ (一覧・詳細に出す用)。まとまりを足したもの。
 * ⭐1 つも数えていなければ NULL のまま (0 で代用しない — 要件 §AB-3)。
 * @returns Map<task_id, { done_qty, loss_qty, variance_note, counted }>
 */
export function countsByTask(db, taskIds) {
  const ids = [...new Set((taskIds || []).map(Number).filter((n) => Number.isInteger(n) && n > 0))];
  const out = new Map();
  if (ids.length === 0) return out;
  const rows = db.prepare(`SELECT task_id,
      COUNT(good_qty) gn, SUM(good_qty) gs, COUNT(loss_qty) ln, SUM(loss_qty) ls,
      SUM(CASE WHEN good_qty_source = 'counted' THEN 1 ELSE 0 END) counted
    FROM f_iroha_task_batches WHERE task_id IN (${ids.map(() => '?').join(',')}) AND work_status <> 'cancelled'
    GROUP BY task_id`).all(...ids);
  const notes = db.prepare(`SELECT task_id, variance_note FROM f_iroha_task_batches
    WHERE task_id IN (${ids.map(() => '?').join(',')}) AND variance_note IS NOT NULL AND work_status <> 'cancelled'
    ORDER BY task_id, seq`).all(...ids);
  const noteBy = new Map();
  for (const n of notes) if (!noteBy.has(n.task_id)) noteBy.set(n.task_id, n.variance_note);
  for (const r of rows) {
    out.set(r.task_id, {
      done_qty: r.gn > 0 ? r.gs : null,
      loss_qty: r.ln > 0 ? r.ls : null,
      variance_note: noteBy.get(r.task_id) || null,
      // ⭐人が数えた値かどうか。移行で持ってきた値は「確認ずみ」に見せない (要件 §AB-3)
      counted: r.counted > 0,
    });
  }
  return out;
}

/**
 * ⭐棚に入れた実績を 1 行足す (要件 §AB-2)。**まとまりごと**。
 *
 * いまは「棚入れする」で**まだ棚に入れていない残り全部**を 1 行にする。
 * 数えていない (good_qty が NULL) まとまりは `qty` も NULL = **数えずに棚に入れた** (0 と区別)。
 * ⚠必ず呼び出し側の書き込みトランザクションの中で。
 *
 * @returns {number} 足した行数 (0 = 残りが無い)
 */
export function recordStocking(db, batchId, { at = null, by = null, note = null } = {}) {
  const b = db.prepare('SELECT * FROM f_iroha_task_batches WHERE id = ?').get(batchId);
  if (!b) return 0;
  const done = stockedQtyOf(db, batchId);
  // 数えていれば「できた数 − すでに棚へ入れた数」。数えていなければ数量なしで 1 行だけ
  let qty = null;
  if (b.good_qty != null) {
    qty = b.good_qty - (done.qty ?? 0);
    if (qty <= 0) return 0;                      // もう全部入れてある
  } else if (done.rows > 0) {
    return 0;                                    // 数えていないまとまりは 1 回だけ
  }
  const now = new Date().toISOString();
  db.prepare(`INSERT INTO f_iroha_stocking_records (batch_id, qty, stocked_at, stocked_by, note, created_at)
    VALUES (?, ?, ?, ?, ?, ?)`).run(batchId, qty, at || now, by, note, now);
  return 1;
}

/** そのまとまりを棚に入れた合計。{ qty: 数えた合計 (NULL = 1 度も数えていない), rows: 実績の件数 } */
export function stockedQtyOf(db, batchId) {
  const r = db.prepare('SELECT COUNT(*) rows, COUNT(qty) n, SUM(qty) s FROM f_iroha_stocking_records WHERE batch_id = ?').get(batchId);
  return { qty: r && r.n > 0 ? r.s : null, rows: r ? r.rows : 0 };
}

/** カードの棚入れ実績をまとめて (履歴・詳細に出す用)。まとまりを問わず新しい順 */
export function stockingOfTask(db, taskId) {
  return db.prepare(`SELECT s.*, b.seq FROM f_iroha_stocking_records s
    JOIN f_iroha_task_batches b ON b.id = s.batch_id
    WHERE b.task_id = ? ORDER BY s.id DESC`).all(taskId);
}

/**
 * カードを棚入完了にしたとき、そのカードのまとまり全部に実績を足す。
 * ⚠必ず呼び出し側の書き込みトランザクションの中で。
 * @returns {number} 足した行数
 */
export function recordStockingForTask(db, taskId, opts = {}) {
  const rows = db.prepare("SELECT id FROM f_iroha_task_batches WHERE task_id = ? AND work_status <> 'cancelled'").all(taskId);
  let n = 0;
  for (const b of rows) n += recordStocking(db, b.id, opts);
  return n;
}

/**
 * 既に棚入完了になっているカードに、実績が無ければ足す (起動時。冪等)。
 * この機能より前に棚に入れたぶんは、いつ・誰が入れたかをカードの closed_at / closed_by から持ってくる。
 * @returns {number} 足した行数
 */
export function backfillStocking(db) {
  const made = db.transaction(() => {
    const rows = db.prepare(`SELECT b.id, t.closed_at, t.closed_by FROM f_iroha_task_batches b
      JOIN f_iroha_tasks t ON t.id = b.task_id
      WHERE b.work_status = 'done'
        AND NOT EXISTS (SELECT 1 FROM f_iroha_stocking_records s WHERE s.batch_id = b.id)`).all();
    let n = 0;
    for (const r of rows) {
      n += recordStocking(db, r.id, { at: r.closed_at || null, by: r.closed_by || null, note: '(この機能より前に棚に入れたぶん)' });
    }
    return n;
  }).immediate();
  if (made > 0) console.log(`[iroha-work] 棚入れの実績を ${made} 件ぶん用意しました (この機能より前のぶん)`);
  return made;
}
