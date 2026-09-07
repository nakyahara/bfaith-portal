/**
 * 🚨 2026-09-08 の事故の復旧のテスト。
 *
 * 空の入荷CSVで一斉に取り消されたカードを戻す処理が、
 *   - 事故で取り消されたぶんは戻す
 *   - **それ以外には触らない** (人が取り消したもの・前からの取消・作業が進んでいたもの)
 * ことを、事故そのものを再現して確かめる。
 */
import fs from 'fs';
import os from 'os';
import path from 'path';

if (!process.env.DATA_DIR) {
  process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'iroha-restore-test-'));
}

let pass = 0, fail = 0;
function ok(cond, label) {
  if (cond) { pass++; console.log(`  ✓ ${label}`); } else { fail++; console.log(`  ✗ ${label}`); }
}

const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const { getDB } = await import('../apps/iroha-work/db.js');
const TD = await import('../apps/iroha-work/tasks-db.js');
const { surveyCancelled, restoreCancelled } = await import('../apps/iroha-work/restore-cancelled.js');
const db = getDB();

const mk = (page, dest, name) => TD.upsertTaskFromImport({ notion_page_id: page, status: 'not_started',
  facility_code: 'iroha', destination_id: dest, product_code: 'R-' + dest, product_name: name, qty: 100,
}, { batchId: 'restore' }).id;

/** 事故と同じ形にする: 未着手・実績なしのカードを inbound_import で自動取消 */
const cancelLikeIncident = (taskId, at) => {
  db.prepare(`UPDATE f_iroha_tasks SET status = 'closed', close_reason = 'cancelled', closed_at = ?,
    closed_by = 'import', cancellation_source = 'inbound_import', version = version + 1, updated_at = ?
    WHERE id = ?`).run(at, at, taskId);
  db.prepare("UPDATE f_iroha_task_batches SET work_status = 'cancelled' WHERE task_id = ?").run(taskId);
};

const AT = '2026-09-07T15:20:05.000Z';          // 事故のとき (JST 9/8 00:20)
const FROM = '2026-09-07T15:00:00.000Z';
const TO = '2026-09-07T16:00:00.000Z';

console.log('\n[1] 事故で取り消されたカードを戻す');
const t1 = mk('r-1', 91001, '事故で消えた A');
const t2 = mk('r-2', 91002, '事故で消えた B');
cancelLikeIncident(t1, AT);
cancelLikeIncident(t2, AT);
{
  const s = surveyCancelled({ from: FROM, to: TO });
  ok(s.ok && s.tasks === 2, '調べると 2 件出る');
  ok(s.sample.length === 2 && s.sample[0].name.includes('事故で消えた'), '中身も見える (目で確かめられる)');
  ok(TD.getTask(t1).status === 'closed', '(前提) いまは閉じている');
  const r = restoreCancelled({ from: FROM, to: TO, expectTasks: s.tasks, expectDestinations: s.destinations, actor: 'test' });
  ok(r.ok && r.restored.tasks === 2, '⭐2 件とも戻る');
  ok(r.restored.batches === 2, 'まとまりも戻る');
  const back = TD.getTask(t1);
  ok(back.status === 'not_started' && back.close_reason === null && back.closed_at === null,
    '⭐未着手にもどり、終了の跡が消える');
  ok(back.cancellation_source === null && back.cancellation_requested_at === null, '取消の跡も消える');
  ok(db.prepare("SELECT work_status FROM f_iroha_task_batches WHERE task_id = ?").get(t1).work_status === 'not_started',
    'まとまりも未着手にもどる');
  ok(db.prepare("SELECT COUNT(*) c FROM f_iroha_app_events WHERE action = 'task_status' AND to_value LIKE '%復旧%'").get().c >= 2,
    '⭐何を戻したかが 1 件ずつ操作履歴に残る');
}

console.log('\n[2] ⭐それ以外には触らない');
{
  // ① 人が取り消したもの
  const t3 = mk('r-3', 91003, '人が取り消した');
  db.prepare(`UPDATE f_iroha_tasks SET status = 'closed', close_reason = 'cancelled', closed_at = ?,
    closed_by = 'やまだ', cancellation_source = 'staff', version = version + 1, updated_at = ? WHERE id = ?`).run(AT, AT, t3);
  // ② 窓の外 (前の日に取り消された)
  const t4 = mk('r-4', 91004, '前からの取消');
  cancelLikeIncident(t4, '2026-09-01T00:00:00.000Z');
  // ③ 棚入完了で閉じたもの
  const t5 = mk('r-5', 91005, '棚入完了');
  db.prepare(`UPDATE f_iroha_tasks SET status = 'closed', close_reason = 'stocked', closed_at = ?,
    closed_by = 'やまだ', version = version + 1, updated_at = ? WHERE id = ?`).run(AT, AT, t5);
  // ④ 事故で消えたもの (これだけ戻る)
  const t6 = mk('r-6', 91006, '事故で消えた C');
  cancelLikeIncident(t6, AT);

  const s = surveyCancelled({ from: FROM, to: TO });
  ok(s.tasks === 1 && s.task_ids[0] === t6, '⭐対象は事故のぶんだけ (人の取消・前からの取消・棚入完了は入らない)');
  const r = restoreCancelled({ from: FROM, to: TO, expectTasks: 1, expectDestinations: s.destinations, actor: 'test' });
  ok(r.ok && r.restored.tasks === 1, '1 件だけ戻る');
  ok(TD.getTask(t3).status === 'closed' && TD.getTask(t3).close_reason === 'cancelled', '人が取り消したものはそのまま');
  ok(TD.getTask(t4).status === 'closed', '前からの取消もそのまま');
  ok(TD.getTask(t5).status === 'closed' && TD.getTask(t5).close_reason === 'stocked', '棚入完了もそのまま');
  ok(TD.getTask(t6).status === 'not_started', '事故のぶんだけ戻る');
}

console.log('\n[3] ⭐数が合わなければ書き込まない');
{
  const t7 = mk('r-7', 91007, '事故で消えた D');
  cancelLikeIncident(t7, AT);
  const bad = restoreCancelled({ from: FROM, to: TO, expectTasks: 99, expectDestinations: 0, actor: 'test' });
  ok(!bad.ok && bad.error === 'count_mismatch', '⭐調べた件数と違えば断る (黙って巻き込まない)');
  ok(TD.getTask(t7).status === 'closed', '断ったときは何も戻していない');
  const good = restoreCancelled({ from: FROM, to: TO, expectTasks: 1, expectDestinations: 0, actor: 'test' });
  ok(good.ok && TD.getTask(t7).status === 'not_started', '合っていれば戻る');
  // 2 回目は 0 件 (冪等)
  const again = restoreCancelled({ from: FROM, to: TO, expectTasks: 0, expectDestinations: 0, actor: 'test' });
  ok(again.ok && again.restored.tasks === 0, '⭐2 回押しても二重に戻さない');
}

console.log('\n[4] ⭐窓の指定を必ず求める');
{
  ok(!surveyCancelled({ from: null, to: TO }).ok, '始まりが無ければ断る');
  ok(!surveyCancelled({ from: FROM, to: null }).ok, '終わりが無ければ断る');
  ok(!surveyCancelled({ from: TO, to: FROM }).ok, '順が逆なら断る');
  ok(!surveyCancelled({ from: '2026-01-01T00:00:00.000Z', to: TO }).ok,
    '⭐広すぎる窓は断る (古い取消まで戻さない)');
  ok(!surveyCancelled({ from: 'きのう', to: TO }).ok, '日付でない文字は断る');
}

console.log('\n[5] ⭐起動のときに一度だけ戻る');
{
  const D = await import('../apps/iroha-work/db.js');
  const { runIncidentRestoreOnce } = await import('../apps/iroha-work/restore-cancelled.js');
  const meta = { getMeta: D.getMeta, setMetaValue: D.setMetaValue };
  // 印を消して、事故のカードを 1 枚つくる
  D.setMetaValue('restore_20260908_empty_csv', null);
  const t8 = mk('r-8', 91008, '空 CSV で消えた');
  cancelLikeIncident(t8, AT);
  const r1 = runIncidentRestoreOnce(db, meta);
  ok(r1.ok && r1.restored && r1.restored.tasks === 1, '⭐起動で自動に戻る (人が押さなくてもよい)');
  ok(TD.getTask(t8).status === 'not_started', '未着手にもどる');
  ok(D.getMeta('restore_20260908_empty_csv'), '実行した印が付く');
  // 2 回目は走らない
  const t9 = mk('r-9', 91009, 'あとから人が取り消した');
  cancelLikeIncident(t9, AT);
  const r2 = runIncidentRestoreOnce(db, meta);
  ok(r2.ok && r2.skipped, '⭐二度目は走らない');
  ok(TD.getTask(t9).status === 'closed',
    '⭐後から取り消されたものを、再起動で勝手に戻さない');
  // 戻すものが無ければ何もしない
  D.setMetaValue('restore_20260908_empty_csv', null);
  db.prepare("UPDATE f_iroha_tasks SET cancellation_source = 'staff' WHERE id = ?").run(t9);
  const r3 = runIncidentRestoreOnce(db, meta);
  ok(r3.ok && r3.skipped, '戻すものが無ければ何もしない');
  ok(D.getMeta('restore_20260908_empty_csv'), 'それでも印は付ける (毎回調べない)');
}

console.log(`\n結果: ${pass} PASS / ${fail} FAIL`);
process.exitCode = fail > 0 ? 1 : 0;
