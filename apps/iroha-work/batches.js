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
 * ⭐必ず**呼び出し側のトランザクションの中で**呼ぶこと。カードだけできて まとまり が無い瞬間を作らない。
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
  db.prepare(`INSERT INTO f_iroha_task_batches
      (task_id, seq, planned_qty, facility_code, expiry, work_status, good_qty, good_qty_source, created_at, updated_at)
    VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?, ?)`)
    .run(task.id, task.qty ?? null, task.facility_code ?? null, task.expiry ?? null,
      batchStatusOfTask(task), done, done == null ? null : 'migrated', now, now);
  return 1;
}

/**
 * まとまりを持っていないカード全部に 1 つずつ用意する (起動時。冪等)。
 * カードが先にできて まとまり が無い状態は、作成経路の取りこぼしや古い DB でしか起きないが、
 * **読むだけの画面で作らない**ためにここでまとめて直す (要件: 読むだけの画面では DB を変えない)。
 * @returns {number} 作った行数
 */
export function backfillBatches(db) {
  const rows = db.prepare(`SELECT t.* FROM f_iroha_tasks t
    WHERE NOT EXISTS (SELECT 1 FROM f_iroha_task_batches b WHERE b.task_id = t.id)`).all();
  if (rows.length === 0) return 0;
  const run = db.transaction(() => {
    let n = 0;
    for (const t of rows) n += ensureBatchForTask(db, t);
    return n;
  });
  const made = run.immediate();
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
