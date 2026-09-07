/**
 * 外部施設への預け (要件 §AB-5 / §AB-7)。
 *
 * 羅針盤・ワークセンターに**物を持ち帰ってもらい**、後日できあがりを持ってきてもらう。
 * 外部施設はこのアプリを触らないので、作業時間も完成写真も記録しない。
 * 残すのは「何を・いくつ渡して、いくつ返ってきたか」だけ。
 *
 * ⭐**カードは分割しない**。1 枚のカードの下の「まとまり」を割る (要件 §AB-1)。
 *   割るのは**まだ手をつけていない分だけ** — できた分は元のまとまりに残すので、
 *   作業時間・できた数・メモを「どちらに付けるか」という判断が**そもそも発生しない** (要件 §AB-5)。
 *   ⭐完成した商品を外部に渡すことは絶対にない (中原さん 2026-09-07) ので、これで全部表せる。
 */
import { getDB } from './db.js';
import { nextSeq, recomputeTaskDoneQty } from './batches.js';

const utcNow = () => new Date().toISOString();

/** 数の受け取り。0 以下・小数・数でないものは断る (要件 §AB-3 と同じ流儀) */
function normQty(v, label) {
  if (typeof v !== 'number' && typeof v !== 'string') return { error: 'bad_qty', message: `${label}は数で入れてください` };
  const n = Number(typeof v === 'string' ? v.trim() : v);
  if (!Number.isInteger(n) || n <= 0) return { error: 'bad_qty', message: `${label}は 1 以上の整数で入れてください` };
  if (n > 1_000_000) return { error: 'bad_qty', message: `${label}が大きすぎます` };
  return { value: n };
}

/**
 * そのまとまりから**切り出せる上限**。
 *
 * ⭐実績から自動で決めない (要件 §AB-5)。「予定 −(できた + 作れなかった)」は未着手数ではない —
 *   袋を開けて作業の途中の分が含まれるし、できた数は作業を終えたときに入れるので
 *   作業中はずっと NULL = 全部未着手に見えてしまう。
 *   ここが返すのは**機械的な上限**だけで、実際に何個渡せるかは人が入れる。
 */
export function splittableMax(db, batch) {
  const done = (batch.good_qty ?? 0) + (batch.loss_qty ?? 0);
  const consigned = db.prepare(`SELECT COALESCE(SUM(planned_qty), 0) n FROM f_iroha_consignments
    WHERE batch_id = ? AND state <> 'cancelled'`).get(batch.id).n;
  if (batch.planned_qty == null) return null;             // つくる数が分からないなら上限も分からない
  return Math.max(0, batch.planned_qty - done - consigned);
}

/**
 * ⭐まとまりを割ってはいけない理由があれば返す (無ければ null)。
 * 実績のあるまとまりを割ると、その実績をどちらに付けるか決められなくなる。
 */
export function whyCannotSplit(db, batch) {
  if (!batch) return { error: 'not_found', message: 'まとまりが見つかりません' };
  if (batch.work_status === 'done' || batch.work_status === 'cancelled') {
    return { error: 'batch_closed', message: 'このぶんはもう終わっています' };
  }
  // ⚠作業時間の記録はまだ**カード単位** (まとまりに紐づけるのは要件 §AB-10 の PR)。
  //   だから「このカードで作業中の人がいるか」で見る。分けるより厳しい側に倒れるので安全
  const active = db.prepare(`SELECT COUNT(*) c FROM f_iroha_work_sessions
    WHERE task_id = ? AND ended_at IS NULL AND voided_at IS NULL`).get(batch.task_id).c;
  if (active > 0) return { error: 'active_sessions', message: 'いま作業している人がいます。作業を終えてからにしてください' };
  const stocked = db.prepare('SELECT COUNT(*) c FROM f_iroha_stocking_records WHERE batch_id = ?').get(batch.id).c;
  if (stocked > 0) return { error: 'already_stocked', message: 'もう棚に入れたぶんです' };
  const printed = db.prepare('SELECT COUNT(*) c FROM f_iroha_print_jobs WHERE batch_id = ?').get(batch.id).c;
  if (printed > 0) {
    return { error: 'already_printed', message: 'このぶんの箱ラベルはもう出しています。分けるなら先にラベルを整理してください' };
  }
  return null;
}

/**
 * ⭐外部施設に預ける (要件 §AB-7)。**1 回の書き込み**で:
 *   ①まとまりから未着手分を切り出して新しいまとまりを作る
 *   ②その新しいまとまりに預けの行を足す
 *
 * 「まとまりを分割する」という操作は現場に見せない。押すのは「🚚 外部にあずける」だけ (要件 §AB-5)。
 * 全部を預けるとき (切り出す数 = 上限) は割らず、そのまとまりごと預ける。
 *
 * 競合対策: **親カードの version を確かめてから進める**。行ごとの version だけだと、
 * 別々の新規行が同時に入って合計が予定数を超える (要件 §AB-7)。
 */
export function startConsignment({ taskId, batchId, facilityCode, qty, dueDate = null, expectVersion,
  actor = null, guard = null }) {
  const db = getDB();
  const q = normQty(qty, '預ける数');
  if (q.error) return { ok: false, error: q.error, message: q.message };
  const due = dueDate == null || dueDate === '' ? null : String(dueDate);
  if (due && !/^\d{4}-\d{2}-\d{2}$/.test(due)) return { ok: false, error: 'bad_request', message: '日付は YYYY-MM-DD で入れてください' };
  return db.transaction(() => {
    if (guard) { const g = guard(); if (g) return g; }
    const t = db.prepare('SELECT * FROM f_iroha_tasks WHERE id = ?').get(taskId);
    if (!t) return { ok: false, error: 'not_found', message: 'カードが見つかりません' };
    if (expectVersion == null || Number(expectVersion) !== t.version) {
      return { ok: false, error: 'conflict', message: '他の端末で変更されています。最新の状態を表示します' };
    }
    if (t.status === 'closed') return { ok: false, error: 'closed_task', message: '終了したカードは預けられません' };
    const fac = db.prepare('SELECT * FROM f_iroha_facilities WHERE code = ? AND active = 1').get(facilityCode);
    if (!fac) return { ok: false, error: 'bad_facility', message: 'その拠点は選べません' };
    if (!fac.external) {
      return { ok: false, error: 'not_external', message: 'その拠点は物を持ち帰らないので、預けるのではなく「どこが」で選んでください' };
    }
    const b = db.prepare('SELECT * FROM f_iroha_task_batches WHERE id = ? AND task_id = ?').get(batchId, taskId);
    const no = whyCannotSplit(db, b);
    if (no) return { ok: false, ...no };
    const max = splittableMax(db, b);
    if (max != null && q.value > max) {
      return { ok: false, error: 'too_many', message: `渡せるのは ${max} 個までです (残っているぶん)`, max };
    }
    const now = utcNow();
    // ⭐全部を預けるならまとまりは割らない (行を増やす意味が無い)
    let target = b;
    if (max == null || q.value < max || b.planned_qty !== q.value) {
      const seq = nextSeq(db, taskId);
      const info = db.prepare(`INSERT INTO f_iroha_task_batches
          (task_id, seq, planned_qty, facility_code, expiry, work_status, split_from_batch_id, split_qty, split_at, split_by, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, 'not_started', ?, ?, ?, ?, ?, ?)`)
        .run(taskId, seq, q.value, facilityCode, b.expiry ?? null, b.id, q.value, now, actor, now, now);
      const newId = Number(info.lastInsertRowid);
      // ⭐元のまとまりは**予定数が減るだけ**。できた数も作業時間もそのまま残る (帰属の判断が起きない)
      db.prepare(`UPDATE f_iroha_task_batches SET planned_qty = planned_qty - ?, version = version + 1, updated_at = ?
        WHERE id = ?`).run(q.value, now, b.id);
      target = db.prepare('SELECT * FROM f_iroha_task_batches WHERE id = ?').get(newId);
    } else {
      db.prepare('UPDATE f_iroha_task_batches SET facility_code = ?, version = version + 1, updated_at = ? WHERE id = ?')
        .run(facilityCode, now, b.id);
      target = db.prepare('SELECT * FROM f_iroha_task_batches WHERE id = ?').get(b.id);
    }
    const ci = db.prepare(`INSERT INTO f_iroha_consignments
        (batch_id, facility_code, planned_qty, state, due_date, planned_at, planned_by, created_at, updated_at)
      VALUES (?, ?, ?, 'planned', ?, ?, ?, ?, ?)`)
      .run(target.id, facilityCode, q.value, due, now, actor, now, now);
    // ⭐親カードの版を進める。預けの追加・数量変更・取消は必ずここを通す (合計が予定数を超えないように)
    db.prepare('UPDATE f_iroha_tasks SET version = version + 1, updated_at = ?, updated_by = ? WHERE id = ? AND version = ?')
      .run(now, actor, t.id, t.version);
    recomputeTaskDoneQty(db, t.id);
    return { ok: true, consignment: getConsignment(Number(ci.lastInsertRowid), db), batch: target };
  }).immediate();
}

/** 箱とラベルを用意した (職員が現物を確かめた節目。印刷が成功しただけでは prepared にしない — 要件 §AB-7) */
export function markPrepared({ consignmentId, qty = null, expectVersion, actor = null, guard = null }) {
  return updateConsignment(consignmentId, expectVersion, guard, (db, c, now) => {
    if (c.state !== 'planned') return { ok: false, error: 'bad_state', message: 'もう用意ずみか、渡したあとです' };
    const n = qty == null ? c.planned_qty : normQty(qty, '用意した数');
    if (n && n.error) return { ok: false, error: n.error, message: n.message };
    const v = qty == null ? c.planned_qty : n.value;
    db.prepare(`UPDATE f_iroha_consignments SET state = 'prepared', prepared_qty = ?, prepared_at = ?, prepared_by = ?,
      version = version + 1, updated_at = ? WHERE id = ?`).run(v, now, actor, now, c.id);
    return { ok: true };
  });
}

/**
 * 渡した (要件 §AB-7)。⭐**当日 78 個しかなかった**が普通に起きるので、渡した数は別に受け取る。
 * 予定数を書き換えない — 書き換えると「80 枚ぶん用意した」履歴が消える。
 */
export function markHanded({ consignmentId, qty = null, expectVersion, actor = null, guard = null }) {
  return updateConsignment(consignmentId, expectVersion, guard, (db, c, now) => {
    if (c.state !== 'planned' && c.state !== 'prepared') {
      return { ok: false, error: 'bad_state', message: 'もう渡したあとです' };
    }
    const n = qty == null ? null : normQty(qty, '渡した数');
    if (n && n.error) return { ok: false, error: n.error, message: n.message };
    const handed = n ? n.value : c.planned_qty;
    db.prepare(`UPDATE f_iroha_consignments SET state = 'handed', handed_qty = ?,
        prepared_qty = COALESCE(prepared_qty, ?), handed_at = ?, handed_by = ?,
        version = version + 1, updated_at = ? WHERE id = ?`)
      .run(handed, handed, now, actor, now, c.id);
    // 渡したら、そのぶんは いろは の手を離れる = 作業中にする (外部が作業している)
    db.prepare("UPDATE f_iroha_task_batches SET work_status = 'in_progress', version = version + 1, updated_at = ? WHERE id = ? AND work_status = 'not_started'")
      .run(now, c.batch_id);
    return { ok: true };
  });
}

/** 渡す前ならやめられる。切り出したまとまりは取消にして、元のまとまりに数を戻す */
export function cancelConsignment({ consignmentId, expectVersion, actor = null, guard = null }) {
  return updateConsignment(consignmentId, expectVersion, guard, (db, c, now) => {
    if (c.state === 'handed' || c.state === 'settled') {
      return { ok: false, error: 'bad_state', message: 'もう渡したあとは取り消せません。返却で受け取ってください' };
    }
    if (c.state === 'cancelled') return { ok: true, already: true };
    db.prepare(`UPDATE f_iroha_consignments SET state = 'cancelled', version = version + 1, updated_at = ? WHERE id = ?`).run(now, c.id);
    const b = db.prepare('SELECT * FROM f_iroha_task_batches WHERE id = ?').get(c.batch_id);
    // 切り出して作ったまとまりなら、取消にして元へ数を戻す
    if (b && b.split_from_batch_id) {
      db.prepare("UPDATE f_iroha_task_batches SET work_status = 'cancelled', version = version + 1, updated_at = ? WHERE id = ?")
        .run(now, b.id);
      db.prepare('UPDATE f_iroha_task_batches SET planned_qty = planned_qty + ?, version = version + 1, updated_at = ? WHERE id = ?')
        .run(b.planned_qty ?? 0, now, b.split_from_batch_id);
    }
    return { ok: true };
  });
}

/**
 * 返却を受け取る (要件 §AB-7)。⭐**確定するのは いろは 側**。
 * 一度で全部返るとは限らないので、返るたびに 1 行足す。
 * 全部そろったら (返ってきた合計 >= 渡した数) 精算ずみにする。
 */
export function recordReturn({ consignmentId, returnedQty, goodQty = undefined, lossQty = undefined,
  note = null, idempotencyKey = null, expectVersion, actor = null, guard = null }) {
  const db = getDB();
  const r = normQty(returnedQty, '返ってきた数');
  if (r.error) return { ok: false, error: r.error, message: r.message };
  const good = goodQty === undefined || goodQty === null ? null : normQty(goodQty, '使える数');
  if (good && good.error) return { ok: false, error: good.error, message: good.message };
  const loss = lossQty === undefined || lossQty === null ? null : Number(lossQty);
  if (loss != null && (!Number.isInteger(loss) || loss < 0)) {
    return { ok: false, error: 'bad_qty', message: '作れなかった数は 0 以上の整数で入れてください' };
  }
  return db.transaction(() => {
    if (guard) { const g = guard(); if (g) return g; }
    if (idempotencyKey) {
      const dup = db.prepare('SELECT * FROM f_iroha_consignment_returns WHERE idempotency_key = ?').get(idempotencyKey);
      if (dup) return { ok: true, replayed: true, consignment: getConsignment(dup.consignment_id, db) };
    }
    const c = db.prepare('SELECT * FROM f_iroha_consignments WHERE id = ?').get(consignmentId);
    if (!c) return { ok: false, error: 'not_found', message: '預けの記録が見つかりません' };
    if (expectVersion == null || Number(expectVersion) !== c.version) {
      return { ok: false, error: 'conflict', message: '他の端末で変更されています' };
    }
    if (c.state !== 'handed') return { ok: false, error: 'bad_state', message: 'まだ渡していないか、もう精算ずみです' };
    const already = db.prepare('SELECT COALESCE(SUM(returned_qty), 0) n FROM f_iroha_consignment_returns WHERE consignment_id = ?').get(c.id).n;
    if (already + r.value > (c.handed_qty ?? 0)) {
      return { ok: false, error: 'too_many', message: `渡したのは ${c.handed_qty} 個です (もう ${already} 個返っています)` };
    }
    const now = utcNow();
    db.prepare(`INSERT INTO f_iroha_consignment_returns
        (consignment_id, returned_qty, good_qty, loss_qty, returned_at, returned_by, note, idempotency_key, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(c.id, r.value, good ? good.value : null, loss, now, actor, note || null, idempotencyKey || null, now);
    const total = already + r.value;
    const settled = total >= (c.handed_qty ?? 0);
    db.prepare(`UPDATE f_iroha_consignments SET state = ?, settled_at = ?, settled_by = ?, version = version + 1, updated_at = ?
      WHERE id = ?`).run(settled ? 'settled' : 'handed', settled ? now : null, settled ? actor : null, now, c.id);
    // 返ってきた良品は、そのまとまりの「できた数」になる
    const goods = db.prepare('SELECT COUNT(good_qty) n, SUM(good_qty) s, SUM(loss_qty) l FROM f_iroha_consignment_returns WHERE consignment_id = ?').get(c.id);
    if (goods.n > 0) {
      db.prepare(`UPDATE f_iroha_task_batches SET good_qty = ?, good_qty_source = 'counted', loss_qty = ?,
          version = version + 1, updated_at = ? WHERE id = ?`)
        .run(goods.s, goods.l ?? null, now, c.batch_id);
      const b = db.prepare('SELECT task_id FROM f_iroha_task_batches WHERE id = ?').get(c.batch_id);
      if (b) recomputeTaskDoneQty(db, b.task_id);
    }
    if (settled) {
      db.prepare("UPDATE f_iroha_task_batches SET work_status = 'ready_for_stocking', version = version + 1, updated_at = ? WHERE id = ? AND work_status = 'in_progress'")
        .run(now, c.batch_id);
    }
    return { ok: true, consignment: getConsignment(c.id, db), settled };
  }).immediate();
}

/** 預けの 1 行 (返却の記録つき) */
export function getConsignment(id, db = getDB()) {
  const c = db.prepare('SELECT * FROM f_iroha_consignments WHERE id = ?').get(id);
  if (!c) return null;
  c.returns = db.prepare('SELECT * FROM f_iroha_consignment_returns WHERE consignment_id = ? ORDER BY id').all(id);
  c.returned_total = c.returns.reduce((a, x) => a + Number(x.returned_qty), 0);
  return c;
}

/** そのカードの預け (新しい順)。取消したものも見せる (何が起きたか追えるように) */
export function consignmentsOfTask(db, taskId) {
  const rows = db.prepare(`SELECT c.*, b.seq FROM f_iroha_consignments c
    JOIN f_iroha_task_batches b ON b.id = c.batch_id
    WHERE b.task_id = ? ORDER BY c.id DESC`).all(taskId);
  for (const c of rows) {
    c.returns = db.prepare('SELECT * FROM f_iroha_consignment_returns WHERE consignment_id = ? ORDER BY id').all(c.id);
    c.returned_total = c.returns.reduce((a, x) => a + Number(x.returned_qty), 0);
  }
  return rows;
}

/** 施設ごとの預け残高 (要件 §AB-8。いまは残高を見せるだけで、上限で止めない) */
export function facilityBalances(db = getDB()) {
  return db.prepare(`SELECT c.facility_code,
      COUNT(*) cards, SUM(c.handed_qty) qty,
      SUM(CASE WHEN c.due_date IS NOT NULL AND c.due_date < date('now', '+9 hours') THEN 1 ELSE 0 END) overdue
    FROM f_iroha_consignments c WHERE c.state = 'handed' GROUP BY c.facility_code`).all();
}

/** 版を見て 1 行だけ直す共通の作り (確かめるところから書くところまで同じトランザクション) */
function updateConsignment(consignmentId, expectVersion, guard, fn) {
  const db = getDB();
  return db.transaction(() => {
    if (guard) { const g = guard(); if (g) return g; }
    const c = db.prepare('SELECT * FROM f_iroha_consignments WHERE id = ?').get(consignmentId);
    if (!c) return { ok: false, error: 'not_found', message: '預けの記録が見つかりません' };
    if (expectVersion == null || Number(expectVersion) !== c.version) {
      return { ok: false, error: 'conflict', message: '他の端末で変更されています' };
    }
    const r = fn(db, c, utcNow());
    if (!r.ok) return r;
    return { ...r, consignment: getConsignment(c.id, db) };
  }).immediate();
}
