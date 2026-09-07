/**
 * 🚨 2026-09-08 の事故の復旧 — 空の入荷CSVで一斉に取り消されたカードを元に戻す。
 *
 * ## 何が起きたか
 * 9/7 に足した 00:20 の入荷CSV取得が、初回 (9/8 00:20) に **0 行のCSV**を作り、
 * 前日 36 行の良いファイルを上書きした。取込側には
 * 「新しいCSVから消えた確認済みの行は、行き先を取り消す」という規則があるため、
 * **全行が「消えた」と判定**され、行き先が全部取り消され、いろはのカードも連鎖して
 * 取り消された (未着手・実績なしは自動で終了:取消)。一覧から商品が全部消えた。
 *
 * ## 何を戻すか
 * ⭐**その取込が触ったものだけ**。時刻の窓 + 取消の出どころで絞る。
 *   - 行き先 (f_inbound_check_destinations): cancelled_by='import' かつ窓の中
 *   - カード (f_iroha_tasks): close_reason='cancelled' かつ cancellation_source='inbound_import' かつ窓の中
 *   - まとまり (f_iroha_task_batches): そのカードのぶんで、同じときに取消になったもの
 *
 * ⭐**人が取り消したもの・前からの取消には触らない**。出どころが 'import' でないもの、
 *   窓の外のものは対象にしない。
 * ⭐**作業が進んでいたカードは自動で閉じていない** (要確認になっているだけ) ので、
 *   そもそも一覧から消えていない。ここでは触らない。
 * ⭐**何をしたかを 1 件ずつ操作履歴に残す** (f_iroha_app_events)。
 */
import { getDB } from './db.js';
import { safeLogTaskEvent } from './tasks-db.js';

const utcNow = () => new Date().toISOString();

/** その表・列があるか (古い DB でも落ちないように) */
const hasTable = (db, name) => !!db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?").get(name);
const hasCol = (db, table, col) =>
  db.prepare(`PRAGMA table_info(${table})`).all().some((c) => c.name === col);

/**
 * 戻せるものを数える (書き込まない)。
 * @param {string} from  窓の始まり (UTC ISO)
 * @param {string} to    窓の終わり (UTC ISO)
 */
export function surveyCancelled({ from, to }, db = getDB()) {
  const w = normWindow(from, to);
  if (w.error) return w;
  const tasks = db.prepare(`SELECT id, destination_id, product_code, product_name, closed_at, cancellation_source
    FROM f_iroha_tasks
    WHERE status = 'closed' AND close_reason = 'cancelled' AND cancellation_source = 'inbound_import'
      AND closed_at >= ? AND closed_at <= ?
    ORDER BY id`).all(w.from, w.to);
  const dests = hasTable(db, 'f_inbound_check_destinations') && hasCol(db, 'f_inbound_check_destinations', 'cancelled_at')
    ? db.prepare(`SELECT id, cancel_reason, cancelled_at FROM f_inbound_check_destinations
        WHERE cancelled_at IS NOT NULL AND cancelled_by = 'import' AND cancelled_at >= ? AND cancelled_at <= ?
        ORDER BY id`).all(w.from, w.to)
    : [];
  return {
    ok: true, from: w.from, to: w.to,
    tasks: tasks.length, destinations: dests.length,
    // 目で確かめられるように少しだけ中身を返す (全部は返さない)
    sample: tasks.slice(0, 10).map((t) => ({ id: t.id, code: t.product_code, name: t.product_name, closed_at: t.closed_at })),
    task_ids: tasks.map((t) => t.id),
    destination_ids: dests.map((d) => d.id),
  };
}

/**
 * 戻す。⭐**数えた件数と合っているときだけ**書き込む (expectTasks / expectDestinations)。
 * 調べたあとに別の取消が起きていたら、黙って巻き込まずに断る。
 */
export function restoreCancelled({ from, to, expectTasks, expectDestinations, actor = 'restore' }, db = getDB()) {
  const pre = normWindow(from, to);
  if (pre.error) return pre;
  const now = utcNow();
  // ⭐**調べ直しも件数の照合も、書き込みと同じトランザクションの中で**やる。
  //   外で数えると、その直後に別の端末が同じ行を (窓の外の理由で) 取り消したとき、
  //   その取消まで戻してしまう (Codex 指摘)
  const done = db.transaction(() => {
    const s = surveyCancelled({ from, to }, db);
    if (!s.ok) return { mismatch: s };
    if (Number(expectTasks) !== s.tasks || Number(expectDestinations) !== s.destinations) {
      return { mismatch: { ok: false, error: 'count_mismatch',
        message: `件数が変わりました (カード ${s.tasks} 件・行き先 ${s.destinations} 件)。もう一度調べ直してください`,
        survey: s } };
    }
    let dests = 0;
    if (s.destination_ids.length && hasTable(db, 'f_inbound_check_destinations')) {
      const up = db.prepare(`UPDATE f_inbound_check_destinations
        SET cancelled_at = NULL, cancelled_by = NULL, cancel_reason = NULL
        WHERE id = ? AND cancelled_at IS NOT NULL AND cancelled_by = 'import'
          AND cancelled_at >= ? AND cancelled_at <= ?`);   // ⭐窓の中であることを書き込みの条件にも入れる
      for (const id of s.destination_ids) dests += up.run(id, s.from, s.to).changes;
    }
    // ⭐カードは 1 枚ずつ、**取消のままのものだけ**戻す (途中で誰かが触っていたら数えない)
    const upTask = db.prepare(`UPDATE f_iroha_tasks
      SET status = 'not_started', close_reason = NULL, closed_at = NULL, closed_by = NULL,
          cancellation_requested_at = NULL, cancellation_source = NULL,
          version = version + 1, updated_at = ?, updated_by = ?
      WHERE id = ? AND status = 'closed' AND close_reason = 'cancelled' AND cancellation_source = 'inbound_import'
        AND closed_at >= ? AND closed_at <= ?`);   // ⭐窓の中であることを書き込みの条件にも入れる (二重の守り)
    // まとまりも戻す。⭐**そのカードのまとまりが 1 つで、取消になっているとき**だけ
    //   (2 つ以上に分かれていたカードは自動取消の対象外 = 実績があるので、ここには来ない)
    const upBatch = db.prepare(`UPDATE f_iroha_task_batches
      SET work_status = 'not_started', version = version + 1, updated_at = ?
      WHERE task_id = ? AND work_status = 'cancelled'
        AND (SELECT COUNT(*) FROM f_iroha_task_batches b2 WHERE b2.task_id = f_iroha_task_batches.task_id) = 1`);
    let tasks = 0;
    let batches = 0;
    for (const id of s.task_ids) {
      if (upTask.run(now, actor, id, s.from, s.to).changes !== 1) continue;   // もう誰かが触っている・窓の外になった
      tasks += 1;
      batches += upBatch.run(now, id).changes;
      // ⭐1 件ずつ証跡を残す。あとから「何を戻したか」を追えるように
      safeLogTaskEvent({ taskId: id, action: 'task_status', workerName: actor,
        from: 'closed:cancelled (auto)', to: 'not_started (空CSV事故の復旧)', ok: true });
    }
    return { tasks, batches, dests, from: s.from, to: s.to, surveyed: { tasks: s.tasks, destinations: s.destinations } };
  }).immediate();
  if (done.mismatch) return done.mismatch;
  return { ok: true, from: done.from, to: done.to,
    restored: { tasks: done.tasks, batches: done.batches, dests: done.dests }, surveyed: done.surveyed };
}

/** 窓の検査。⭐**必ず両端を要求する** — 開けっぱなしで全期間を戻さない */
function normWindow(from, to) {
  const ok = (v) => typeof v === 'string' && /^\d{4}-\d{2}-\d{2}T/.test(v) && !Number.isNaN(Date.parse(v));
  if (!ok(from) || !ok(to)) {
    return { ok: false, error: 'bad_request', message: 'from と to を UTC の ISO 時刻で指定してください (例 2026-09-07T15:00:00.000Z)' };
  }
  const f = new Date(from).toISOString();
  const t = new Date(to).toISOString();
  if (f >= t) return { ok: false, error: 'bad_request', message: 'from は to より前にしてください' };
  // ⭐窓は 7 日まで。広く開けて古い取消まで戻さない
  if (Date.parse(t) - Date.parse(f) > 7 * 86400000) {
    return { ok: false, error: 'bad_request', message: '窓は 7 日までにしてください' };
  }
  return { ok: true, from: f, to: t };
}
