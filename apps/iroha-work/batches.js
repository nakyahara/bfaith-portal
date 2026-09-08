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

/**
 * ⭐**期限が違うぶんを、人が手で分ける** (要件 §AB-11 の 8 / §AB-12)。
 *
 * これまで、まとまりが分かれるのは「外部にあずける」ときだけだった。だから
 * **同じカードに期限の違う物が混ざって届いても分けられず**、箱ラベルの期限を 1 つしか選べなかった。
 * (§AB-12「期限が違うまとまりを誤って併合しない」を守るには、まず分けられる必要がある)
 *
 * 分ける = 元から数を引いて、新しいまとまりを 1 つ作る。**元の記録は動かさない**。
 *
 * ⭐分けてよい元の条件は「預けで分ける」(consign.js の giveBackToHand) と**同じ考え方**に揃える —
 *   数が動くと辻褄が合わなくなるものが付いていたら分けない:
 *   - まだ作業できる状態か (棚入待ち・終了・取消のまとまりからは分けない)
 *   - 予定数が分かっているか (NULL から引くと「不明」が数に化ける)
 *   - 外に預けていないか (相手に渡した数が勝手に減らない)
 *   - 棚に入れた記録・箱ラベルを刷った記録が無いか (刷ったラベルの数と合わなくなる)
 *   - できた数を数えていないか (数え終わったものを分けると、どちらが何個できたか決められない)
 *
 * ⭐**元のまとまりの版 (expectVersion) を必ず見る**。見ないと、同じ 100 個を 2 人が別々に開いて
 *   40 個ずつ分けたとき、どちらも条件を通って「20 + 40 + 40」になる (合計は合うのに、期限ごとの数が
 *   現物とずれる)。応答を失って送り直したときも同じ (Codex #1270 R1 重大)。
 *
 * @param {object} o  { taskId, batchId, qty (新しいぶんの数), expiry (新しいぶんの期限。null 可), expectVersion, actor }
 * @returns {{ok:true, batch}|{ok:false, error, message}}
 */
export function splitBatchByExpiry(db, { taskId, batchId, qty, expiry = null, expectVersion = null, actor = null }) {
  const b = db.prepare('SELECT * FROM f_iroha_task_batches WHERE id = ? AND task_id = ?').get(Number(batchId), Number(taskId));
  if (!b) return { ok: false, error: 'bad_batch', message: 'そのぶんはこのカードにありません (画面を更新してください)' };
  // 🚨**画面で見ていたときの版**と同じでなければ断る (二重に分けない・古い画面で分けない)
  if (expectVersion == null || Number(expectVersion) !== b.version) {
    return { ok: false, error: 'conflict', current: b,
      message: 'このぶんは他の端末で変わっています。画面を更新して、もう一度確かめてください' };
  }
  const n = Number(qty);
  if (!Number.isSafeInteger(n) || n < 1) {
    return { ok: false, error: 'bad_qty', message: '分ける数は 1 以上の整数で入れてください' };
  }
  if (b.work_status !== 'not_started' && b.work_status !== 'in_progress') {
    return { ok: false, error: 'bad_state', message: 'このぶんはもう分けられません (棚入待ち・終了・取消のぶんは分けられません)' };
  }
  if (b.planned_qty == null) {
    return { ok: false, error: 'bad_qty', message: 'このぶんは数が分かっていないので分けられません (先に数を決めてください)' };
  }
  // ⭐**全部は分けられない**。元が 0 個になると「分けた」ではなく「移した」で、記録の意味が変わる
  if (n >= b.planned_qty) {
    return { ok: false, error: 'bad_qty',
      message: `このぶんは ${b.planned_qty} 個です。分けられるのは ${b.planned_qty - 1} 個までです (全部は分けられません)` };
  }
  const away = db.prepare("SELECT COUNT(*) c FROM f_iroha_consignments WHERE batch_id = ? AND state <> 'cancelled'").get(b.id).c;
  if (away > 0) return { ok: false, error: 'consign_open', message: '外にあずけているぶんは分けられません (先に返却を受け取るか、預けをやめてください)' };
  const stocked = db.prepare('SELECT COUNT(*) c FROM f_iroha_stocking_records WHERE batch_id = ?').get(b.id).c;
  if (stocked > 0) return { ok: false, error: 'stocked_batch', message: 'もう棚に入れているぶんは分けられません' };
  const printed = db.prepare('SELECT COUNT(*) c FROM f_iroha_print_jobs WHERE batch_id = ?').get(b.id).c;
  if (printed > 0) return { ok: false, error: 'printed_batch', message: 'もう箱ラベルを出しているぶんは分けられません (刷ったラベルの数と合わなくなります)' };
  if (b.good_qty != null) {
    return { ok: false, error: 'counted_batch', message: 'もうできた数を数えているぶんは分けられません (どちらが何個できたか決められません)' };
  }
  const exp = expiry == null ? null : String(expiry).trim();
  if (exp != null && exp.length > 40) {
    return { ok: false, error: 'bad_request', message: '期限は 40 字までで入れてください' };
  }
  const now = new Date().toISOString();
  db.prepare('UPDATE f_iroha_task_batches SET planned_qty = planned_qty - ?, version = version + 1, updated_at = ? WHERE id = ?')
    .run(n, now, b.id);
  // ⭐新しいぶんは**元と同じ担当**を継ぐ (どこが作業するかは変わらない)。期限だけが違う
  const info = db.prepare(`INSERT INTO f_iroha_task_batches
      (task_id, seq, planned_qty, facility_code, expiry, work_status, split_from_batch_id, split_qty, split_at, split_by, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, 'not_started', ?, ?, ?, ?, ?, ?)`)
    .run(b.task_id, nextSeq(db, b.task_id), n, b.facility_code ?? null, exp === "" ? null : exp,
      b.id, n, now, actor, now, now);
  const made = db.prepare('SELECT * FROM f_iroha_task_batches WHERE id = ?').get(Number(info.lastInsertRowid));
  return { ok: true, batch: made };
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
export function recordStocking(db, batchId, { at = undefined, by = null, note = null } = {}) {
  const b = db.prepare('SELECT * FROM f_iroha_task_batches WHERE id = ?').get(batchId);
  if (!b) return 0;
  const done = stockedQtyOf(db, batchId);
  // ⭐**数の分からない実績が 1 つでもあれば、残りを自分で決めない** (要件: 欠損値を 0 で代用しない)。
  //   「数えずに入れた」あとに できた数 を 100 と入れても、既に何個運んだか分からないので
  //   残りが 100 とは言えない (Codex R1 中1)。この場合も「数は分からない」1 行として足す
  let qty = null;
  if (b.good_qty != null && done.unknown === 0) {
    qty = b.good_qty - (done.qty ?? 0);
    if (qty <= 0) return 0;                      // もう全部入れてある
  } else if (b.good_qty == null && done.unknown > 0) {
    // 数えていないまとまりに「数は分からない」で入れた記録が既にある = もう運んである。二度書かない。
    // ⚠**数ありの記録があるかどうかでは判定しない** — 200 個入れたあと できた数 を「分からない」に
    //   直して残りを入れるとき、残量は不明なのに「残りなし」と同じ扱いになってしまう (Codex R2 中)
    return 0;
  } else if (done.unknown > 0 && b.good_qty != null) {
    // 数の分からない実績があるところに、あとから できた数 が入った。
    // 何個ぶん残っているか決められないので、数を書かずに 1 行だけ足す (人が見て直せる)
    qty = null;
  }
  const now = new Date().toISOString();
  // ⭐`at` を渡さなければ「いま」。**`at: null` を渡したときだけ空のまま** (= いつ入れたか分からない)。
  //   `created_at` には記録した時刻が残るので、あとから追える
  db.prepare(`INSERT INTO f_iroha_stocking_records (batch_id, qty, stocked_at, stocked_by, note, created_at)
    VALUES (?, ?, ?, ?, ?, ?)`).run(batchId, qty, at === undefined ? now : at, by, note, now);
  return 1;
}

/**
 * そのまとまりを棚に入れた合計。
 * @returns {{ qty: number|null, rows: number, unknown: number }}
 *   qty = **数が分かっているぶんの合計** (NULL = 1 つも数えていない) /
 *   rows = 実績の件数 / unknown = 数の分からない実績の件数。
 *   ⭐unknown > 0 なら、全部で何個入れたかは**決められない**。qty を全体の合計として使わないこと
 */
export function stockedQtyOf(db, batchId) {
  const r = db.prepare(`SELECT COUNT(*) rows, COUNT(qty) n, SUM(qty) s
    FROM f_iroha_stocking_records WHERE batch_id = ?`).get(batchId);
  const rows = r ? r.rows : 0;
  const n = r ? r.n : 0;
  return { qty: n > 0 ? r.s : null, rows, unknown: rows - n };
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
  // ⭐外部にあずけたまま (渡す予定・用意ずみ・渡した) のまとまりは**棚に入れていない**ので飛ばす。
  //   ここで作ってしまうと、まだ外にある物が「棚に入れた」記録になる (自己レビュー A)。
  //   ふつうは手前 (changeTaskStatus / bulkCloseReady) で consign_open として断るので、ここは念のための二重の関門
  const rows = db.prepare(`SELECT id FROM f_iroha_task_batches b WHERE task_id = ? AND work_status <> 'cancelled'
    AND NOT EXISTS (SELECT 1 FROM f_iroha_consignments c WHERE c.batch_id = b.id AND c.state IN ('planned','prepared','handed'))`).all(taskId);
  let n = 0;
  for (const b of rows) n += recordStocking(db, b.id, opts);
  return n;
}

/**
 * ⭐カードごと閉じた・やり直したとき、**すべてのまとまり**をその状態に合わせる (要件 §AB-1)。
 *
 * `syncSingleBatchStatus` はまとまりが 1 つのときだけの橋渡し。こちらは 2 つ以上でも動かす。
 * 呼ぶのは**行き先が 1 つに決まる操作だけ** — 終了 (全部 done か cancelled) と、
 * 終了からのやり直し (全部 作業中に戻す)。
 * 「作り終えた」を全まとまりに広げるようなことはしない (どのぶんが終わったのかが決まらない)。
 *
 * ⭐**やり直し (作業中に戻す) では、取消になっていたまとまりも戻す**。
 *   カードごと取消にしたときに全まとまりを取消にしているので、戻さないと
 *   「カードは作業中なのに、作業できるまとまりが 1 つも無い」= 数も入れられない状態になる。
 *   ただし**預けをやめて数を元に戻したまとまり (split_created の預けが取消)** は戻さない —
 *   そのぶんの数はもう別のまとまりに足してあるので、戻すと二重になる。
 * ⚠必ずカードを更新したのと同じトランザクションの中で。
 *
 * @returns {number} 直した行数
 */
export function syncAllBatchesStatus(db, taskId, task) {
  const want = batchStatusOfTask(task);
  if (!['done', 'cancelled', 'in_progress'].includes(want)) return 0;
  const now = new Date().toISOString();
  if (want === 'in_progress') {
    return db.prepare(`UPDATE f_iroha_task_batches SET work_status = 'in_progress', version = version + 1, updated_at = ?
      WHERE task_id = ? AND work_status <> 'in_progress'
        AND NOT EXISTS (SELECT 1 FROM f_iroha_consignments c
          WHERE c.batch_id = f_iroha_task_batches.id AND c.split_created = 1 AND c.state = 'cancelled')`)
      .run(now, taskId).changes;
  }
  // 終了へ: 取消したまとまりは触らない (もう無かったことにしたぶん)
  return db.prepare(`UPDATE f_iroha_task_batches SET work_status = ?, version = version + 1, updated_at = ?
    WHERE task_id = ? AND work_status <> 'cancelled' AND work_status <> ?`)
    .run(want, now, taskId, want).changes;
}

/**
 * ⭐**まとまりの遷移表** (要件 §AB-11 の 5)。カードの表とは別に持つ。
 *
 * 未着手から直接「作り終えた」へ行けるのは、**そのぶんだけの「作業をはじめる」が無い**から
 * (作業時間の記録はまだカード単位。まとまりに紐づけるのは次の PR — 要件 §AB-10)。
 * カード側の表 (tasks.js の TRANSITIONS) は今までどおり 未着手 → 作業中 → 棚入待ち のまま。
 */
export const BATCH_TRANSITIONS = {
  not_started: ['in_progress', 'ready_for_stocking'],
  in_progress: ['ready_for_stocking'],
  ready_for_stocking: ['in_progress', 'done'],
  done: ['in_progress'],
};
export function canBatchTransition(from, to) {
  return Array.isArray(BATCH_TRANSITIONS[from]) && BATCH_TRANSITIONS[from].includes(to);
}
/** 職員だけができるまとまりの操作: 棚入完了にする / 棚入待ち・棚入完了からやり直す (カードと同じ考え方) */
export function batchTransitionNeedsStaff(from, to) {
  if (to === 'done') return true;
  if ((from === 'ready_for_stocking' || from === 'done') && to === 'in_progress') return true;
  return false;
}

/**
 * ⭐まとまりから導いた進捗を**カードに書き戻す** (要件 §AB-1: 進捗の正本はまとまり)。
 *
 * 預けで まとまりの状態が変わったとき (渡した = 作業中 / 返ってきた = 棚入待ち) に、
 * カードの進捗もそこから導く。人が押していないので、**終了 (done) にはしない** —
 * 全部棚に入ったあとカードを閉じるのは、職員が「棚入完了」を押したときだけ。
 *
 * ⭐**止まっている札が付いているうちは棚入待ちへ進めない**。棚入待ちのカードは札を持てない
 * 決まり (validateTaskInvariants) なので、進めるなら札を消すことになるが、
 * ここは人が押していない経路なので「なぜ札が消えたか」を残せない。札を外したときに導き直す。
 * ⭐申し送り (hold_memo) も消さない — 消した中身を履歴に残せないため。
 * ⚠必ず呼び出し側の書き込みトランザクションの中で。
 *
 * @returns {number} 直した行数
 */
export function applyDerivedTaskStatus(db, taskId, now = new Date().toISOString(), actor = null) {
  const t = db.prepare('SELECT * FROM f_iroha_tasks WHERE id = ?').get(taskId);
  if (!t || t.status === 'closed') return 0;      // 終了したカードは触らない (戻すのは職員の操作)
  let d = deriveTaskStatus(db, taskId);
  if (!d || d === 'done') return 0;
  if (d === 'ready_for_stocking' && t.blocked_reason) d = 'in_progress';
  // ⭐一度はじめたカードを「未着手」へ戻さない (Codex R1 中2)。
  //   預けをやめた拍子に、作業している人がいるのに未着手に見える — が起きる
  if (d === 'not_started' && t.started_at) return 0;
  if (d === t.status) return 0;
  const toReady = d === 'ready_for_stocking';
  return db.prepare(`UPDATE f_iroha_tasks SET status = ?, started_at = ?, ready_at = ?, updated_at = ?, updated_by = ?
    WHERE id = ?`)
    .run(d,
      d === 'not_started' ? t.started_at : (t.started_at || now),
      toReady ? (t.ready_at || now) : null,
      now, actor, taskId).changes;
}

/**
 * ⭐まとまりから**カードの進捗を導く** (要件 §AB-1: 進捗の正本はまとまり)。
 *
 * まとまりが 1 つのカード (ふだんの全部) では、今までと同じ値になる。
 * 2 つ以上あるときは「いろはのぶんは棚に入れた・外部のぶんはまだ作業中」のような
 * 食い違いが起きるので、**いちばん進んでいない側に合わせる**。
 *
 * @returns 'not_started' | 'in_progress' | 'ready_for_stocking' | 'done' (全部棚に入った) / null (まとまりが無い)
 */
export function deriveTaskStatus(db, taskId) {
  const rows = db.prepare("SELECT work_status FROM f_iroha_task_batches WHERE task_id = ? AND work_status <> 'cancelled'").all(taskId);
  if (rows.length === 0) return null;
  if (rows.every((r) => r.work_status === 'done')) return 'done';
  if (rows.every((r) => r.work_status === 'ready_for_stocking' || r.work_status === 'done')) return 'ready_for_stocking';
  if (rows.every((r) => r.work_status === 'not_started')) return 'not_started';
  return 'in_progress';
}

/** そのまとまりに、まだ外にある (返ってきていない) 預けが何件あるか */
export function batchConsignedOutCount(db, batchId) {
  return db.prepare("SELECT COUNT(*) c FROM f_iroha_consignments WHERE batch_id = ? AND state IN ('planned','prepared','handed')").get(batchId).c;
}

/**
 * ⭐カードで「作業をはじめる」を押したとき、**手元のまとまり**も作業中にする。
 *
 * 作業時間の記録はまだカード単位 (§AB-10 は次の PR) なので、「そのぶんだけ始める」という操作が無い。
 * 動かすのは**物を持ち帰らない拠点 (offsite = 0) の未着手のまとまり**だけ —
 * 外部に預けたぶんは「渡した」ときに作業中になるので、ここでは触らない。
 * ⚠必ずカードを更新したのと同じトランザクションの中で。
 *
 * @returns {number} 直した行数
 */
export function startHomeBatches(db, taskId, now = new Date().toISOString()) {
  return db.prepare(`UPDATE f_iroha_task_batches SET work_status = 'in_progress', version = version + 1, updated_at = ?
    WHERE task_id = ? AND work_status = 'not_started'
      AND (facility_code IS NULL OR facility_code IN (SELECT code FROM f_iroha_facilities WHERE offsite = 0))
      AND NOT EXISTS (SELECT 1 FROM f_iroha_consignments c WHERE c.batch_id = f_iroha_task_batches.id AND c.state <> 'cancelled')`)
    .run(now, taskId).changes;
}

/** そのカードで、まだ外にある (返ってきていない) 預けの数。0 なら棚入待ち・棚入完了にしてよい */
export function openConsignmentCount(db, taskId) {
  return db.prepare(`SELECT COUNT(*) c FROM f_iroha_consignments c JOIN f_iroha_task_batches b ON b.id = c.batch_id
    WHERE b.task_id = ? AND c.state IN ('planned','prepared','handed')`).get(taskId).c;
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
      // ⭐いつ入れたか分からないカードは `stocked_at` を**空のまま**にする。
      //   移行した日を入れると「その日に棚入れした」と嘘の記録になり、以後直る機会も無い (Codex R1 中2)
      n += recordStocking(db, r.id, { at: r.closed_at || null, by: r.closed_by || null,
        note: '(この機能より前に棚に入れたぶん' + (r.closed_at ? '' : '・いつ入れたかは記録がありません') + ')' });
    }
    return n;
  }).immediate();
  if (made > 0) console.log(`[iroha-work] 棚入れの実績を ${made} 件ぶん用意しました (この機能より前のぶん)`);
  return made;
}
