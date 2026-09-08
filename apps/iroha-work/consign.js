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
import { nextSeq, recomputeTaskDoneQty, applyDerivedTaskStatus, stockedQtyOf } from './batches.js';

const utcNow = () => new Date().toISOString();

/** 数の受け取り。0 以下・小数・数でないものは断る (要件 §AB-3 と同じ流儀) */
function normQty(v, label) {
  return normNum(v, label, 1);
}
/** ⭐0 も許す数 (使える数 0 個 = 全部だめだった、は起きる — Codex R1 重大3) */
function normQty0(v, label) {
  return normNum(v, label, 0);
}
function normNum(v, label, min) {
  if (typeof v !== 'number' && typeof v !== 'string') return { error: 'bad_qty', message: `${label}は数で入れてください` };
  // ⭐空文字を 0 と読まない。未入力なのに「0 個」として保存してしまう (Codex R2 中5)
  if (typeof v === 'string' && v.trim() === '') return { error: 'bad_qty', message: `${label}を入れてください` };
  const n = Number(typeof v === 'string' ? v.trim() : v);
  if (!Number.isInteger(n) || n < min) return { error: 'bad_qty', message: `${label}は ${min} 以上の整数で入れてください` };
  if (n > 1_000_000) return { error: 'bad_qty', message: `${label}が大きすぎます` };
  return { value: n };
}
/** 日付。⭐形を見るだけでなく**実在する日か**まで見る (2026-99-99 を通さない — Codex R1 軽微) */
function normDue(v) {
  if (v == null || v === '') return { value: null };
  const t = String(v);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(t)) return { error: 'bad_request', message: '日付は YYYY-MM-DD で入れてください' };
  const d = new Date(t + 'T00:00:00Z');
  if (Number.isNaN(d.getTime()) || d.toISOString().slice(0, 10) !== t) {
    return { error: 'bad_request', message: 'その日付はありません' };
  }
  return { value: t };
}

/**
 * ⭐渡さなかったぶん・やめたぶんを**いろは の手元に戻す** (Codex R1 重大2・重大5)。
 *
 * 80 個切り出して 78 個しか渡せなかったとき、残り 2 個が外部のまとまりに残ると、
 * 78 個返ってきた時点でそのまとまりごと棚入待ちになり、2 個が宙に浮く。
 *
 * 戻し先は「元のまとまり」。ただし**元が今も手元にあるときだけ** —
 * 元をよそへ渡していたり終わっていたら、戻すと相手の数が増えてしまう (重大5)。
 * そのときは**新しいまとまりを 1 つ作って**そこに戻す。
 */
function giveBackToHand(db, awayBatch, qty, now, actor, homeHint = null) {
  if (!qty || qty <= 0) return null;
  const origin = awayBatch.split_from_batch_id
    ? db.prepare('SELECT * FROM f_iroha_task_batches WHERE id = ?').get(awayBatch.split_from_batch_id) : null;
  // ⭐戻してよい元の条件。ここを緩めると、直したはずの問題が戻し先で再発する (Codex R2 重大1・重大2)
  //   - まだ手元で作業できる状態か (棚入待ち・終了・取消のまとまりに未着手分を足さない)
  //   - ⭐「手元」= 物を持ち帰らない拠点 (いろは・パレット・ジョブサポ = offsite 0)。
  //     「いろは」だけに限ると、パレットが担当のカードの戻り先が無くなる (自己レビュー D)
  //   - よそへ預けていないか (相手の数が勝手に増えないように)
  //   - 棚に入れた記録・箱ラベルを刷った記録が無いか (数が動くと記録と合わなくなる)
  //   - ⭐予定数が分かっているか。NULL (数不明) に足すと「不明」が「40 個」に化ける
  const originHome = !!origin && !isOffsiteFacility(db, origin.facility_code);
  const originOk = origin && originHome
    && (origin.work_status === 'not_started' || origin.work_status === 'in_progress')
    && origin.planned_qty != null
    && db.prepare("SELECT COUNT(*) c FROM f_iroha_consignments WHERE batch_id = ? AND state <> 'cancelled'").get(origin.id).c === 0
    && db.prepare('SELECT COUNT(*) c FROM f_iroha_stocking_records WHERE batch_id = ?').get(origin.id).c === 0
    && db.prepare('SELECT COUNT(*) c FROM f_iroha_print_jobs WHERE batch_id = ?').get(origin.id).c === 0;
  if (originOk) {
    db.prepare('UPDATE f_iroha_task_batches SET planned_qty = planned_qty + ?, version = version + 1, updated_at = ? WHERE id = ?')
      .run(qty, now, origin.id);
    return origin.id;
  }
  // 新しいまとまりの担当は、元が手元の拠点ならそれを継ぐ (パレットのカードはパレットへ)。
  // ⭐丸ごと預けたまとまり (元が無い) は、預けの行に覚えた「預ける前の担当」(prev_facility_code) を継ぐ (Codex R4 中5)。
  //   どちらも無ければ未定 (NULL)
  const homeFac = originHome ? (origin.facility_code ?? null)
    : (homeHint && !isOffsiteFacility(db, homeHint) ? homeHint : null);
  const seq = nextSeq(db, awayBatch.task_id);
  const info = db.prepare(`INSERT INTO f_iroha_task_batches
      (task_id, seq, planned_qty, facility_code, expiry, work_status, split_from_batch_id, split_qty, split_at, split_by, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, 'not_started', ?, ?, ?, ?, ?, ?)`)
    .run(awayBatch.task_id, seq, qty, homeFac, awayBatch.expiry ?? null, awayBatch.id, qty, now, actor, now, now);
  return Number(info.lastInsertRowid);
}

/** 物を持ち帰る拠点か (NULL = 未定 は手元扱い)。拠点マスタの offsite で見る — コードの決め打ちにしない */
function isOffsiteFacility(db, code) {
  if (!code) return false;
  const f = db.prepare('SELECT offsite FROM f_iroha_facilities WHERE code = ?').get(code);
  return !!(f && f.offsite);
}

/** 親カードが終了していれば断りを返す (預けを前に進める操作の前に見る — Codex R4 重大2) */
function parentClosed(db, batchId) {
  const t = db.prepare('SELECT t.status FROM f_iroha_task_batches b JOIN f_iroha_tasks t ON t.id = b.task_id WHERE b.id = ?').get(batchId);
  if (t && t.status === 'closed') return { ok: false, error: 'closed_task', message: '終了したカードの預けは進められません (返却・精算・取消はできます)' };
  return null;
}

/**
 * 親カードの版を進める。⭐預けの追加・数量の変更・取消は必ずここを通す (要件 §AB-7)。
 * ⭐ついでに**カードの進捗をまとまりから導き直す** — 渡したら「作業中」、返ってきたら「棚入待ち」に
 *   なるので、カードの見え方も一緒に合わせる (要件 §AB-1)
 */
function bumpTask(db, taskId, now, actor) {
  applyDerivedTaskStatus(db, taskId, now, actor);
  db.prepare('UPDATE f_iroha_tasks SET version = version + 1, updated_at = ?, updated_by = ? WHERE id = ?')
    .run(now, actor, taskId);
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
  // ⭐差し引くのは「まだ外にある (返ってきていない) 預け」と「精算で**返ってこなかった数**」。
  //   精算ずみで返ってきたぶんは good/loss に表れるので二重に引かない。
  //   返ってこなかった 2 個 (missing_qty) は good にも loss にも無いので、ここで引かないと
  //   存在しない 2 個を再び切り出せてしまう (Codex R4 重大3)
  const r = db.prepare(`SELECT
      COALESCE(SUM(CASE WHEN state IN ('planned','prepared','handed') THEN planned_qty END), 0) open_qty,
      COALESCE(SUM(CASE WHEN state = 'settled' THEN missing_qty END), 0) missing
    FROM f_iroha_consignments WHERE batch_id = ?`).get(batch.id);
  if (batch.planned_qty == null) return null;             // つくる数が分からないなら上限も分からない
  return Math.max(0, batch.planned_qty - done - r.open_qty - r.missing);
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
  // ⭐棚入待ち = 「全部そろった」。ここから未着手分を切り出すと「そろった」が嘘になる (Codex R3 重大1)。
  //   完成した商品を外部に渡すことは絶対にない (中原さん 2026-09-07)
  if (batch.work_status === 'ready_for_stocking') {
    return { error: 'batch_closed', message: 'このぶんはできあがっています (棚入待ち)。渡すなら職員が作業中に戻してください' };
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
 * ⭐**予定数を丸ごと預けるときだけ**割らない (行を増やす意味が無いので)。
 *   完成した分や別の預けが元に残るなら、上限いっぱいでも割る。
 *
 * 競合対策: **親カードの version を確かめてから進める**。行ごとの version だけだと、
 * 別々の新規行が同時に入って合計が予定数を超える (要件 §AB-7)。
 */
export function startConsignment({ taskId, batchId, facilityCode, qty, dueDate = null, expectVersion,
  actor = null, guard = null, capacityGuard = null }) {
  const db = getDB();
  const q = normQty(qty, '預ける数');
  if (q.error) return { ok: false, error: q.error, message: q.message };
  const dd = normDue(dueDate);
  if (dd.error) return { ok: false, error: dd.error, message: dd.message };
  const due = dd.value;
  return db.transaction(() => {
    if (guard) { const g = guard(); if (g) return g; }
    const t = db.prepare('SELECT * FROM f_iroha_tasks WHERE id = ?').get(taskId);
    if (!t) return { ok: false, error: 'not_found', message: 'カードが見つかりません' };
    if (expectVersion == null || Number(expectVersion) !== t.version) {
      return { ok: false, error: 'conflict', message: '他の端末で変更されています。最新の状態を表示します' };
    }
    if (t.status === 'closed') return { ok: false, error: 'closed_task', message: '終了したカードは預けられません' };
    // ⭐カードが棚入待ち = 「全部そろった」。まとまりが 2 つ以上だとカードの状態はまとまりに写らないので、
    //   親カード側でも断る (分割 → 返却 → 作り終えた → 残り 2 個を再び預ける、を通さない — Codex R4 重大1)
    if (t.status === 'ready_for_stocking') {
      return { ok: false, error: 'batch_closed', message: 'このカードはできあがっています (棚入待ち)。渡すなら職員が作業中に戻してください' };
    }
    const fac = db.prepare('SELECT * FROM f_iroha_facilities WHERE code = ? AND active = 1').get(facilityCode);
    if (!fac) return { ok: false, error: 'bad_facility', message: 'その拠点は選べません' };
    // ⭐預けられるのは**物を持ち帰る**拠点だけ。パレット・ジョブサポは別の事業者でも いろは の中で作業する
    if (!fac.offsite) {
      return { ok: false, error: 'not_external', message: 'その拠点は物を持ち帰らないので、預けるのではなく「どこが」で選んでください' };
    }
    const b = db.prepare('SELECT * FROM f_iroha_task_batches WHERE id = ? AND task_id = ?').get(batchId, taskId);
    const no = whyCannotSplit(db, b);
    if (no) return { ok: false, ...no };
    const max = splittableMax(db, b);
    if (max != null && q.value > max) {
      return { ok: false, error: 'too_many', message: `渡せるのは ${max} 個までです (残っているぶん)`, max };
    }
    // ⭐受け入れ枠 (要件 §AB-8)。**止めるのは箱数だけ**、しかも「置き場の都合で本当に上限がある」と
    //   決めた拠点だけ。想定時間は概算 (外部は進捗を入れないので残りが分からない) なので、
    //   超えても止めず、画面で注意するだけにする。
    // ⭐**残高を数えるのもここ (トランザクションの中)** — 外で数えた残高を持ち回ると、
    //   2 つの預けが同時に入って上限を超えられる (Codex R1 中2)
    if (capacityGuard) {
      const cap = capacityGuard(facilityCode, taskId, q.value);
      if (cap) return { ok: false, ...cap };
    }
    const now = utcNow();
    // ⭐予定数を丸ごと預けるときだけ、まとまりを割らない (行を増やす意味が無い)。
    //   完成分や別の預けが残っているなら、上限まででも割る
    let target = b;
    let prevFac;
    let splitCreated = 0;
    if (max == null || q.value < max || b.planned_qty !== q.value) {
      splitCreated = 1;
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
      prevFac = undefined;
    } else {
      // 丸ごと預ける = まとまりを割らない。⭐やめたときに戻せるよう、元の担当拠点を覚えておく (Codex R1 中6)
      prevFac = b.facility_code ?? null;
      db.prepare('UPDATE f_iroha_task_batches SET facility_code = ?, version = version + 1, updated_at = ? WHERE id = ?')
        .run(facilityCode, now, b.id);
      target = db.prepare('SELECT * FROM f_iroha_task_batches WHERE id = ?').get(b.id);
    }
    // ⭐この預けのためにまとまりを切り出したかは**預けの行に**残す (split_created)。
    //   まとまりの split_from_batch_id は過去の分割の履歴なので、それで「取消のとき戻すか」を決めると
    //   前に切り出したまとまりを丸ごと預けてやめたときに、そのまとまりごと取消にしてしまう (Codex R3 中4)
    const ci = db.prepare(`INSERT INTO f_iroha_consignments
        (batch_id, facility_code, planned_qty, state, due_date, prev_facility_code, split_created, planned_at, planned_by, created_at, updated_at)
      VALUES (?, ?, ?, 'planned', ?, ?, ?, ?, ?, ?, ?)`)
      .run(target.id, facilityCode, q.value, due, prevFac === undefined ? null : prevFac, splitCreated, now, actor, now, now);
    bumpTask(db, t.id, now, actor);
    recomputeTaskDoneQty(db, t.id);
    return { ok: true, consignment: getConsignment(Number(ci.lastInsertRowid), db), batch: target };
  }).immediate();
}

/** 箱とラベルを用意した (職員が現物を確かめた節目。印刷が成功しただけでは prepared にしない — 要件 §AB-7) */
export function markPrepared({ consignmentId, qty = null, expectVersion, actor = null, guard = null }) {
  return updateConsignment(consignmentId, expectVersion, guard, (db, c, now, actorIn) => {
    if (c.state !== 'planned') return { ok: false, error: 'bad_state', message: 'もう用意ずみか、渡したあとです' };
    const pc = parentClosed(db, c.batch_id);
    if (pc) return pc;
    const n = qty == null ? null : normQty(qty, '用意した数');
    if (n && n.error) return { ok: false, error: n.error, message: n.message };
    const v = n ? n.value : c.planned_qty;
    if (v > c.planned_qty) return { ok: false, error: 'too_many', message: `渡す予定は ${c.planned_qty} 個です` };
    db.prepare(`UPDATE f_iroha_consignments SET state = 'prepared', prepared_qty = ?, prepared_at = ?, prepared_by = ?,
      version = version + 1, updated_at = ? WHERE id = ?`).run(v, now, actorIn, now, c.id);
    return { ok: true };
  }, actor);
}

/**
 * 渡した (要件 §AB-7)。⭐**当日 78 個しかなかった**が普通に起きるので、渡した数は別に受け取る。
 * 予定数を書き換えない — 書き換えると「80 枚ぶん用意した」履歴が消える。
 */
export function markHanded({ consignmentId, qty = null, expectVersion, actor = null, guard = null }) {
  return updateConsignment(consignmentId, expectVersion, guard, (db, c, now, actorIn) => {
    if (c.state !== 'planned' && c.state !== 'prepared') {
      return { ok: false, error: 'bad_state', message: 'もう渡したあとです' };
    }
    // ⭐終了したカードから新たに「渡した」へは進めない (Codex R4 重大2)。返却・精算・取消は通す (外にある数を減らす側)
    const pc = parentClosed(db, c.batch_id);
    if (pc) return pc;
    const n = qty == null ? null : normQty(qty, '渡した数');
    if (n && n.error) return { ok: false, error: n.error, message: n.message };
    const handed = n ? n.value : c.planned_qty;
    // ⭐**確保した数より多くは渡せない** (Codex R1 重大1)。
    //   多く渡したいなら、いったんやめてから決め直す (そうしないと元の数を超えて実績が積み上がる)
    if (handed > c.planned_qty) {
      return { ok: false, error: 'too_many',
        message: `渡す予定は ${c.planned_qty} 個です。もっと渡すなら、いったんやめて決め直してください` };
    }
    db.prepare(`UPDATE f_iroha_consignments SET state = 'handed', handed_qty = ?, handed_at = ?, handed_by = ?,
        version = version + 1, updated_at = ? WHERE id = ?`)
      .run(handed, now, actorIn, now, c.id);
    const b = db.prepare('SELECT * FROM f_iroha_task_batches WHERE id = ?').get(c.batch_id);
    // ⭐渡さなかったぶんは**手元に戻す** (Codex R1 重大2)。
    //   外部のまとまりに残したままだと、渡したぶんが返ってきた時点で一緒に棚入待ちになり宙に浮く
    const left = (b.planned_qty ?? 0) - handed;
    if (left > 0) {
      db.prepare('UPDATE f_iroha_task_batches SET planned_qty = ?, version = version + 1, updated_at = ? WHERE id = ?')
        .run(handed, now, b.id);
      giveBackToHand(db, b, left, now, actorIn, c.prev_facility_code);
    }
    // 渡したら、そのぶんは いろは の手を離れる = 作業中にする (外部が作業している)
    db.prepare("UPDATE f_iroha_task_batches SET work_status = 'in_progress', version = version + 1, updated_at = ? WHERE id = ? AND work_status = 'not_started'")
      .run(now, c.batch_id);
    return { ok: true, taskId: b.task_id };
  }, actor);
}

/** 渡す前ならやめられる。切り出したまとまりは取消にして、元のまとまりに数を戻す */
export function cancelConsignment({ consignmentId, expectVersion, actor = null, guard = null }) {
  return updateConsignment(consignmentId, expectVersion, guard, (db, c, now, actorIn) => {
    if (c.state === 'handed' || c.state === 'settled') {
      return { ok: false, error: 'bad_state', message: 'もう渡したあとは取り消せません。返却で受け取ってください' };
    }
    if (c.state === 'cancelled') return { ok: true, already: true };
    db.prepare(`UPDATE f_iroha_consignments SET state = 'cancelled', version = version + 1, updated_at = ? WHERE id = ?`).run(now, c.id);
    const b = db.prepare('SELECT * FROM f_iroha_task_batches WHERE id = ?').get(c.batch_id);
    if (b && c.split_created) {
      // この預けのために切り出したまとまり → 取消にして、数を手元に戻す。
      // ⭐戻し先が今も手元にあるかを見る。よそへ渡した相手に足してしまわない (Codex R1 重大5)
      db.prepare("UPDATE f_iroha_task_batches SET work_status = 'cancelled', version = version + 1, updated_at = ? WHERE id = ?")
        .run(now, b.id);
      giveBackToHand(db, b, b.planned_qty ?? 0, now, actorIn);
    } else if (b) {
      // 割らずに丸ごと預けたまとまり → 担当拠点を元に戻す (Codex R1 中6)。
      // ⭐前の分割で生まれたまとまりでも、この預けで切り出したのでなければ消さない (Codex R3 中4)
      db.prepare('UPDATE f_iroha_task_batches SET facility_code = ?, version = version + 1, updated_at = ? WHERE id = ?')
        .run(c.prev_facility_code ?? null, now, b.id);
    }
    return { ok: true, taskId: b ? b.task_id : null };
  }, actor);
}

/**
 * ⭐物として返ってこないぶんを職員が確かめて精算する (Codex R1 中10)。
 * 100 個渡して 98 個返り、2 個は外部で壊れて捨てた — この 2 個を入れて閉じる。
 * 返ってきた物の内訳 (good / loss) とは別のもの。
 */
export function settleConsignment({ consignmentId, missingQty = 0, note = null, expectVersion, actor = null, guard = null }) {
  return updateConsignment(consignmentId, expectVersion, guard, (db, c, now, actorIn) => {
    if (c.state !== 'handed') return { ok: false, error: 'bad_state', message: 'まだ渡していないか、もう精算ずみです' };
    const m = normQty0(missingQty, '返らなかった数');
    if (m.error) return { ok: false, error: m.error, message: m.message };
    const back = db.prepare('SELECT COALESCE(SUM(returned_qty), 0) n FROM f_iroha_consignment_returns WHERE consignment_id = ?').get(c.id).n;
    // 渡した数より多く返っていれば「返らなかった数」は 0 だけ (多く返った理由は返却の行にある)
    const handed = c.handed_qty ?? 0;
    if (back >= handed ? m.value !== 0 : back + m.value !== handed) {
      return { ok: false, error: 'bad_qty',
        message: `渡した ${handed} 個のうち ${back} 個が返っています。残り ${Math.max(0, handed - back)} 個を「返らなかった数」に入れてください` };
    }
    db.prepare(`UPDATE f_iroha_consignments SET state = 'settled', missing_qty = ?, note = ?, settled_at = ?, settled_by = ?,
      version = version + 1, updated_at = ? WHERE id = ?`).run(m.value, note || c.note || null, now, actorIn, now, c.id);
    closeBatchIfSettled(db, c.batch_id, now);
    const b = db.prepare('SELECT task_id FROM f_iroha_task_batches WHERE id = ?').get(c.batch_id);
    return { ok: true, taskId: b ? b.task_id : null };
  }, actor);
}

/** 預けが片づいたら、そのまとまりを棚入待ちへ */
function closeBatchIfSettled(db, batchId, now) {
  const open = db.prepare("SELECT COUNT(*) c FROM f_iroha_consignments WHERE batch_id = ? AND state IN ('planned','prepared','handed')").get(batchId).c;
  if (open > 0) return;
  db.prepare("UPDATE f_iroha_task_batches SET work_status = 'ready_for_stocking', version = version + 1, updated_at = ? WHERE id = ? AND work_status = 'in_progress'")
    .run(now, batchId);
}

/**
 * 返却を受け取る (要件 §AB-7)。⭐**確定するのは いろは 側**。
 * 一度で全部返るとは限らないので、返るたびに 1 行足す。
 * 全部そろったら (返ってきた合計 >= 渡した数) 精算ずみにする。
 */
/**
 * ⭐返ってきた物の内訳 (使える数・作れなかった数) の決まりごと。**1 か所に集める** —
 * 受け取るときと、あとから入れ直すときで規則がずれると、片方だけ通る数ができてしまう。
 *
 * - どちらも省ける (= まだ数えていない。**0 とは違う**)
 * - 単独でも、合計でも、返ってきた数を超えない
 *
 * @returns {{error, message}|{good, loss}} good/loss は {value} か null (数えていない)
 */
function normReturnCounts(returnedQty, goodQty, lossQty) {
  const good = goodQty === undefined || goodQty === null ? null : normQty0(goodQty, '使える数');
  if (good && good.error) return { error: good.error, message: good.message };
  const loss = lossQty === undefined || lossQty === null ? null : normQty0(lossQty, '作れなかった数');
  if (loss && loss.error) return { error: loss.error, message: loss.message };
  if (good && good.value > returnedQty) {
    return { error: 'bad_qty', message: `使える数は、返ってきた ${returnedQty} 個より多くできません` };
  }
  // ⭐作れなかった数も**単独で**超えられない。合計だけ見ると、使える数を省いたときに素通りする (Codex R2 重大3)
  if (loss && loss.value > returnedQty) {
    return { error: 'bad_qty', message: `作れなかった数は、返ってきた ${returnedQty} 個より多くできません` };
  }
  if (good && loss && good.value + loss.value > returnedQty) {
    return { error: 'bad_qty', message: `使える数と作れなかった数の合計が、返ってきた ${returnedQty} 個を超えています` };
  }
  return { good, loss };
}

/**
 * ⭐返却の行から、そのまとまりの「できた数」を出し直す。**1 か所に集める** —
 * 受け取ったときと、あとから数を入れたときで違う集計をすると、画面の数が食い違う。
 *
 * 🚨**1 つでも「分からない」があれば確定しない** (Codex #1245 R3 重大2)。
 *   60 個 (使える 60) → 40 個 (分からない) と返ってきたのに 60 を確定にすると、
 *   100 個のうち 60 個しかできなかったと読める。分からないものは NULL のまま。
 */
function applyReturnsToBatch(db, consignmentId, batchId, now) {
  const agg = db.prepare(`SELECT COUNT(*) rows, COUNT(good_qty) gn, SUM(good_qty) gs, COUNT(loss_qty) ln, SUM(loss_qty) ls
    FROM f_iroha_consignment_returns WHERE consignment_id = ?`).get(consignmentId);
  const cur = db.prepare('SELECT good_qty, good_qty_source, loss_qty FROM f_iroha_task_batches WHERE id = ?').get(batchId);
  if (!cur) return false;
  const nextGood = agg.rows > 0 && agg.gn === agg.rows ? agg.gs : null;
  const nextLoss = agg.rows > 0 && agg.ln === agg.rows ? agg.ls : null;
  if (nextGood === (cur.good_qty ?? null) && nextLoss === (cur.loss_qty ?? null)
    && !(nextGood != null && cur.good_qty_source !== 'counted')) return false;
  db.prepare(`UPDATE f_iroha_task_batches SET good_qty = ?, good_qty_source = ?, loss_qty = ?,
      version = version + 1, updated_at = ? WHERE id = ?`)
    .run(nextGood, nextGood == null ? null : 'counted', nextLoss, now, batchId);
  const b = db.prepare('SELECT task_id FROM f_iroha_task_batches WHERE id = ?').get(batchId);
  if (b) recomputeTaskDoneQty(db, b.task_id);
  return true;
}

/**
 * ⭐返ってきた物の数を**あとから**入れる / 直す (要件 §AB: 精算ずみ返却行の good_qty 補完)。
 *
 * 受け取るときは「分からなければ空のまま」でよい。ただし 1 つでも空があると、そのまとまりの
 * 「できた数」は NULL のまま = **棚入れに進めない** (要件 §AB-3「現物がすべて説明できる状態」)。
 * 数え終わってから入れられる口がないと、そこで詰まる。
 *
 * ⭐**精算ずみでも直せる**。数を数えるのは物が返ってきたあとで、精算より遅れることがある。
 * ⭐競合は**預けの版**で見る (返却の行に版を持たせていない)。同じ預けを 2 人が同時に触れば片方が断られる。
 */
export function updateReturnCounts({ consignmentId, returnId, goodQty = undefined, lossQty = undefined,
  note = undefined, expectVersion, actor = null, guard = null }) {
  return updateConsignment(consignmentId, expectVersion, guard, (db, c, now, actorIn) => {
    const row = db.prepare('SELECT * FROM f_iroha_consignment_returns WHERE id = ?').get(Number(returnId));
    if (!row || row.consignment_id !== c.id) return { ok: false, error: 'not_found', message: 'その返却の記録が見つかりません (画面を更新してください)' };
    // 🚨**棚に入れたあとは数を変えさせない** (Codex #1268 R1 重大)。
    //   98 個で棚に入れたあとに 38 → 30 と直すと、棚入れの記録は 98 のままカードの数だけ減る。
    //   空に戻すと「棚に入れたのに、棚入れできる条件を満たさない」ことにもなる。
    //   ⚠**やり直しても棚入れの記録は消えない** (消す処理がそもそも無い)。だから「やり直せば直せる」とは案内しない。
    //   直す必要があるときは職員へ — 記録の付け替えは人が判断する
    const goodChanges = goodQty !== undefined || lossQty !== undefined;
    if (goodChanges && stockedQtyOf(db, c.batch_id).rows > 0) {
      return { ok: false, error: 'stocked_batch',
        message: 'このぶんはもう棚に入れています。棚に入れた記録と食い違うため、ここから数は直せません。'
          + '直す必要があれば職員に相談してください (「ひとこと」だけなら直せます)' };
    }
    // ⭐**省略 = いまの値のまま / null = 空にする (まだ数えていないに戻す)**。
    //   省略を空と同じに扱うと、使える数だけ入れたときに、入っていた「作れなかった数」が消える (R1 中2)
    const curGood = row.good_qty ?? null;
    const curLoss = row.loss_qty ?? null;
    const wantGood = goodQty === undefined ? curGood : goodQty;
    const wantLoss = lossQty === undefined ? curLoss : lossQty;
    // ⭐合計は**残す値も入れて**確かめる (片方だけ直したときに、合わせて超えていないか)
    const n = normReturnCounts(row.returned_qty, wantGood, wantLoss);
    if (n.error) return { ok: false, error: n.error, message: n.message };
    const nextGood = n.good ? n.good.value : null;
    const nextLoss = n.loss ? n.loss.value : null;
    const nextNote = note === undefined ? (row.note ?? null) : (String(note).trim() || null);
    if (nextGood === (row.good_qty ?? null) && nextLoss === (row.loss_qty ?? null) && nextNote === (row.note ?? null)) {
      return { ok: true, already: true };
    }
    db.prepare('UPDATE f_iroha_consignment_returns SET good_qty = ?, loss_qty = ?, note = ? WHERE id = ?')
      .run(nextGood, nextLoss, nextNote, row.id);
    applyReturnsToBatch(db, c.id, c.batch_id, now);
    const bt = db.prepare('SELECT task_id FROM f_iroha_task_batches WHERE id = ?').get(c.batch_id);
    return { ok: true, taskId: bt ? bt.task_id : null };
  }, actor);
}

export function recordReturn({ consignmentId, returnedQty, goodQty = undefined, lossQty = undefined,
  note = null, idempotencyKey = null, expectVersion, actor = null, guard = null }) {
  const db = getDB();
  const r = normQty(returnedQty, '返ってきた数');
  if (r.error) return { ok: false, error: r.error, message: r.message };
  // ⭐内訳の決まりごとは normReturnCounts に集めてある (あとから直すときと同じ規則を使う)。
  //   使える数は **0 も許す** (全部だめだった、は起きる)。画面の min="0" とも合わせる (Codex R1 重大3)
  const n = normReturnCounts(r.value, goodQty, lossQty);
  if (n.error) return { ok: false, error: n.error, message: n.message };
  const good = n.good;
  const loss = n.loss;
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
    // ⭐渡した数より多く返ってくることもある (1000 個渡して数え直したら 1010 個 — 数は合わない、の前提)。
    //   ただし**黙っては通さない**: 理由 (note) を書いたときだけ受け取る。理由が無ければ数え間違いの可能性が高いので断る (Codex R4 中4)
    if (already + r.value > (c.handed_qty ?? 0) && !(note && String(note).trim())) {
      return { ok: false, error: 'too_many',
        message: `渡したのは ${c.handed_qty} 個です (もう ${already} 個返っています)。それより多く返ってきたなら、理由を書いて受け取ってください` };
    }
    const now = utcNow();
    db.prepare(`INSERT INTO f_iroha_consignment_returns
        (consignment_id, returned_qty, good_qty, loss_qty, returned_at, returned_by, note, idempotency_key, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(c.id, r.value, good ? good.value : null, loss ? loss.value : null, now, actor, note || null, idempotencyKey || null, now);
    const total = already + r.value;
    const settled = total >= (c.handed_qty ?? 0);
    db.prepare(`UPDATE f_iroha_consignments SET state = ?, settled_at = ?, settled_by = ?, version = version + 1, updated_at = ?
      WHERE id = ?`).run(settled ? 'settled' : 'handed', settled ? now : null, settled ? actor : null, now, c.id);
    // 返ってきた良品は、そのまとまりの「できた数」になる。
    // ⭐集計は applyReturnsToBatch に集めてある — あとから数を入れ直したときと**同じ計算**を使う
    //   (別々に書くと、受け取ったときと直したときで画面の数が食い違う)
    applyReturnsToBatch(db, c.id, c.batch_id, now);
    if (settled) closeBatchIfSettled(db, c.batch_id, now);
    const bt = db.prepare('SELECT task_id FROM f_iroha_task_batches WHERE id = ?').get(c.batch_id);
    if (bt) bumpTask(db, bt.task_id, now, actor);   // ⭐数が動いたので親カードの版も進める
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
  // ⭐いま外にあるのは「渡した数 − 返ってきた数」。100 個渡して 90 個返っても 100 と出さない (Codex R1 中9)。
  //   件数は**カードの数**を数える (預けの行の数ではない)
  return db.prepare(`SELECT c.facility_code,
      COUNT(DISTINCT b.task_id) cards,
      SUM(COALESCE(c.handed_qty, 0) - COALESCE((SELECT SUM(r.returned_qty) FROM f_iroha_consignment_returns r WHERE r.consignment_id = c.id), 0)) qty,
      SUM(CASE WHEN c.due_date IS NOT NULL AND c.due_date < date('now', '+9 hours') THEN 1 ELSE 0 END) overdue
    FROM f_iroha_consignments c JOIN f_iroha_task_batches b ON b.id = c.batch_id
    WHERE c.state = 'handed' GROUP BY c.facility_code`).all();
}

/** 版を見て 1 行だけ直す共通の作り (確かめるところから書くところまで同じトランザクション) */
function updateConsignment(consignmentId, expectVersion, guard, fn, actor = null) {
  const db = getDB();
  return db.transaction(() => {
    if (guard) { const g = guard(); if (g) return g; }
    const c = db.prepare('SELECT * FROM f_iroha_consignments WHERE id = ?').get(consignmentId);
    if (!c) return { ok: false, error: 'not_found', message: '預けの記録が見つかりません' };
    if (expectVersion == null || Number(expectVersion) !== c.version) {
      return { ok: false, error: 'conflict', message: '他の端末で変更されています' };
    }
    const now = utcNow();
    const r = fn(db, c, now, actor);
    if (!r.ok) return r;
    // ⭐数が動く操作は**親カードの版も進める** (Codex R1 重大4)。
    //   進めないと、取消の前のカードを見ている端末の要求がそのまま通ってしまう。
    //   ⚠ただし**何も変えなかったとき (already) は進めない** — 同じ要求を繰り返すだけで
    //     親の版が上がり続け、他の端末に無用な競合を起こす (Codex R2 中4)
    const taskId = r.taskId ?? (db.prepare('SELECT task_id FROM f_iroha_task_batches WHERE id = ?').get(c.batch_id) || {}).task_id;
    if (taskId && !r.already) bumpTask(db, taskId, now, actor);
    return { ...r, consignment: getConsignment(c.id, db) };
  }).immediate();
}

/**
 * ⭐一覧用に「あと何個渡せるか / なぜ渡せないか」を**まとめて**引く (Codex R2 中6)。
 *
 * 1 まとまりずつ `whyCannotSplit` / `splittableMax` を呼ぶと、2000 枚 × 4 クエリで 8000 回になる。
 * ここは 4 本の集計で全部ぶんを取り、Map で引く。
 * @returns Map<batch_id, { max: number|null, why: string|null }>
 */
export function consignableByBatch(db, batchIds) {
  const ids = [...new Set((batchIds || []).map(Number).filter((n) => Number.isInteger(n) && n > 0))];
  const out = new Map();
  if (ids.length === 0) return out;
  const inq = ids.map(() => '?').join(',');
  const rows = db.prepare(`SELECT * FROM f_iroha_task_batches WHERE id IN (${inq})`).all(...ids);
  const taskIds = [...new Set(rows.map((b) => b.task_id))];
  const tinq = taskIds.map(() => '?').join(',');
  const active = new Map(db.prepare(`SELECT task_id, COUNT(*) c FROM f_iroha_work_sessions
    WHERE task_id IN (${tinq}) AND ended_at IS NULL AND voided_at IS NULL GROUP BY task_id`).all(...taskIds).map((r) => [r.task_id, r.c]));
  const stocked = new Map(db.prepare(`SELECT batch_id, COUNT(*) c FROM f_iroha_stocking_records
    WHERE batch_id IN (${inq}) GROUP BY batch_id`).all(...ids).map((r) => [r.batch_id, r.c]));
  const printed = new Map(db.prepare(`SELECT batch_id, COUNT(*) c FROM f_iroha_print_jobs
    WHERE batch_id IN (${inq}) GROUP BY batch_id`).all(...ids).map((r) => [r.batch_id, r.c]));
  // まだ外にある預け + 精算で返ってこなかった数 (splittableMax と同じ式 — 二重に定義しない)
  const consigned = new Map(db.prepare(`SELECT batch_id,
      COALESCE(SUM(CASE WHEN state IN ('planned','prepared','handed') THEN planned_qty END), 0)
        + COALESCE(SUM(CASE WHEN state = 'settled' THEN missing_qty END), 0) n
    FROM f_iroha_consignments WHERE batch_id IN (${inq}) GROUP BY batch_id`).all(...ids).map((r) => [r.batch_id, r.n]));
  // 親カードが棚入待ち・終了なら、まとまりの状態に関わらず渡せない (Codex R4 重大1)
  const taskStatus = new Map(db.prepare(`SELECT id, status FROM f_iroha_tasks WHERE id IN (${tinq})`).all(...taskIds).map((r) => [r.id, r.status]));
  for (const b of rows) {
    let why = null;
    const ts = taskStatus.get(b.task_id);
    if (b.work_status === 'done' || b.work_status === 'cancelled') why = 'このぶんはもう終わっています';
    else if (ts === 'closed') why = '終了したカードは預けられません';
    else if (b.work_status === 'ready_for_stocking' || ts === 'ready_for_stocking') why = 'このぶんはできあがっています (棚入待ち)。渡すなら職員が作業中に戻してください';
    else if ((active.get(b.task_id) || 0) > 0) why = 'いま作業している人がいます。作業を終えてからにしてください';
    else if ((stocked.get(b.id) || 0) > 0) why = 'もう棚に入れたぶんです';
    else if ((printed.get(b.id) || 0) > 0) why = 'このぶんの箱ラベルはもう出しています。分けるなら先にラベルを整理してください';
    const done = (b.good_qty ?? 0) + (b.loss_qty ?? 0);
    const max = b.planned_qty == null ? null : Math.max(0, b.planned_qty - done - (consigned.get(b.id) || 0));
    out.set(b.id, { max: why ? 0 : max, why });
  }
  return out;
}
