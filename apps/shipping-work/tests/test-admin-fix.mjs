/**
 * 管理者の救済 (手動ステータス訂正・セッションの採用/除外判定) のテスト。
 *   node test-admin-fix.mjs <repo-root>
 */
import path from 'path';
import fs from 'fs';
import os from 'os';
import { pathToFileURL } from 'url';

// 引数省略時はこのファイルの位置からリポジトリルートを解決する
const repo = process.argv[2]
  || path.resolve(path.dirname(new URL(import.meta.url).pathname.replace(/^\/(?=[A-Za-z]:)/, '')), '../../..');
if (!repo) { console.error('usage: node test-admin-fix.mjs <repo-root>'); process.exit(1); }

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'sw-adm-'));
process.env.DATA_DIR = tmp;

const dbMod = await import(pathToFileURL(path.join(repo, 'apps/shipping-work/db.js')));
const svc = await import(pathToFileURL(path.join(repo, 'apps/shipping-work/service.js')));
dbMod.initShippingWorkDB();
const db = dbMod.getDB();

let pass = 0, fail = 0;
const ok = (c, n) => { if (c) { pass++; console.log(`  ok - ${n}`); } else { fail++; console.log(`  NG - ${n}`); } };
function expectErr(fn, status, name, codeExpect) {
  try { fn(); fail++; console.log(`  NG - ${name} (エラーにならなかった)`); }
  catch (e) {
    const okS = e instanceof svc.SwError && e.status === status;
    const okC = codeExpect === undefined || e.code === codeExpect;
    if (okS && okC) { pass++; console.log(`  ok - ${name} [${e.status} ${e.message}]`); }
    else { fail++; console.log(`  NG - ${name} (got ${e.status ?? e.code}: ${e.message})`); }
  }
}
const uuid = () => crypto.randomUUID();
const sess = (id) => db.prepare('SELECT * FROM sw_sessions WHERE id=?').get(id);
const W = 'w@b-faith.biz';
const ADMIN = 'admin@b-faith.biz';
const today = dbMod.jstToday();

function reset() {
  for (const t of ['sw_print_attempts', 'sw_print_jobs', 'sw_pauses', 'sw_mistakes',
    'sw_status_events', 'sw_operations', 'sw_audit_logs', 'sw_sessions', 'sw_batches']) {
    db.prepare(`DELETE FROM ${t}`).run();
  }
}
const mk = (o) => dbMod.createBatch(
  { packingMethod: 'manual', bunrui: 'tanpin', docUrl: `https://drive.google.com/d/${o.shippingNo}`, ...o }, ADMIN);

console.log('# 1. バッチ詳細 (履歴・セッション・印刷ジョブが1画面で見える)');
reset();
{
  const id = mk({ workDate: today, shippingNo: 's01', slipCount: 20 });
  const r = svc.startProcess('picking', id, W, uuid());
  svc.pauseProcess('picking', r.session_id, W, 'pk1', '', uuid());
  svc.resumeProcess('picking', r.session_id, W, uuid());
  svc.completeProcess('picking', r.session_id, W, uuid());
  const d = svc.getBatchDetail(id);
  ok(d.batch.id === id, 'バッチが取れる');
  ok(d.sessions.length === 1 && d.sessions[0].pauses.length === 1, 'セッションと保留区間が取れる');
  ok(d.events.length >= 4, `ステータス履歴が取れる (${d.events.length}件)`);
  ok(d.printJobs.length === 1, '印刷ジョブが取れる');
  ok(/^\d{2}\/\d{2} \d{2}:\d{2}$/.test(d.sessions[0].requestedAtJst), '日時はJSTで整形される');
  ok(svc.getBatchDetail(99999) === null, '存在しないバッチは null');
}

console.log('# 2. 手動ステータス訂正');
reset();
{
  const id = mk({ workDate: today, shippingNo: 's01', slipCount: 20 });
  const r = svc.startProcess('picking', id, W, uuid());   // 進行中のまま放置された状態
  expectErr(() => svc.adminFixStatus(id, 'done', ADMIN, '', uuid()), 400, '理由なしは 400');
  expectErr(() => svc.adminFixStatus(id, 'bogus', ADMIN, '理由', uuid()), 400, '不正なステータスは 400');
  expectErr(() => svc.adminFixStatus(99999, 'done', ADMIN, '理由', uuid()), 404, '存在しないバッチは 404');

  const before = sess(r.session_id);
  const op = uuid();
  const fixed = svc.adminFixStatus(id, 'done', ADMIN, '現物は梱包済みだった', op);
  ok(fixed.status === 'done' && fixed.closedSessions === 1, '訂正でき、開いていたセッションを閉じた');
  ok(dbMod.getBatch(id).status === 'done', 'バッチが done になる');

  const after = sess(r.session_id);
  ok(after.outcome === 'cancelled' && after.ended_at != null, '開いていたセッションは cancelled で閉じる');
  ok(after.requested_at === before.requested_at, '⭐開始時刻は書き換わらない');
  ok(after.validity === 'review_required' && /管理者がステータスを訂正/.test(after.correction_reason),
    '閉じたセッションは確認待ちになる');
  ok(after.paused === 0, '一人一作業を塞がないよう paused も戻す');

  const ev = db.prepare('SELECT * FROM sw_status_events WHERE batch_id=? ORDER BY id DESC').get(id);
  ok(ev.to_status === 'done' && ev.via === 'admin' && /現物は梱包済み/.test(ev.reason), '履歴に理由付きで残る');
  const audit = db.prepare('SELECT * FROM sw_audit_logs ORDER BY id DESC').get();
  ok(audit.action === 'admin_fix_status' && audit.actor === ADMIN && /現物は梱包済み/.test(audit.reason),
    '監査ログに残る');
  ok(JSON.parse(audit.before_json).status === 'picking' && JSON.parse(audit.after_json).status === 'done',
    '監査ログに before/after が入る');

  ok(svc.adminFixStatus(id, 'done', ADMIN, '現物は梱包済みだった', op).already, '同一 op_id の再送は already');
  ok(svc.adminFixStatus(id, 'done', ADMIN, '別の理由', uuid()).already, '同じ状態への訂正は already (noop)');
}

console.log('# 3. 訂正しても作業者が詰まらない');
reset();
{
  const a = mk({ workDate: today, shippingNo: 's01', slipCount: 5 });
  const b = mk({ workDate: today, shippingNo: 's02', slipCount: 5 });
  const r = svc.startProcess('picking', a, W, uuid());
  // 管理者が「もう終わっている」として直す
  svc.adminFixStatus(a, 'done', ADMIN, '現物確認済み', uuid());
  const started = svc.startProcess('picking', b, W, uuid());
  ok(!!started.session_id, '⭐管理者が直した後、作業者は次の作業を開始できる (open セッションが残らない)');
  ok(db.prepare("SELECT COUNT(*) c FROM sw_sessions WHERE worker=? AND outcome='open'").get(W).c === 1,
    'open セッションは新しい1件だけ');
}

console.log('# 4. 保留中バッチの訂正 (未解除の保留も閉じる)');
reset();
{
  const id = mk({ workDate: today, shippingNo: 's01', slipCount: 8 });
  const r = svc.startProcess('picking', id, W, uuid());
  svc.pauseProcess('picking', r.session_id, W, 'pk1', '', uuid());
  ok(dbMod.getBatch(id).status === 'hold', '保留中');
  svc.adminFixStatus(id, 'ready', ADMIN, 'やり直しのため戻す', uuid());
  ok(dbMod.getBatch(id).status === 'ready', 'ready に戻せる');
  ok(dbMod.getBatch(id).hold_from_status === null, 'hold_from_status がクリアされる');
  const p = db.prepare('SELECT * FROM sw_pauses WHERE session_id=?').get(r.session_id);
  ok(p.resumed_at != null, '未解除の保留が閉じられる (保留時間が伸び続けない)');
  // 戻した後に別の作業者が取れる
  const r2 = svc.startProcess('picking', id, 'w2@b-faith.biz', uuid());
  ok(!!r2.session_id, '戻したバッチを他の作業者が開始できる');
}

console.log('# 5. セッションの採用/除外判定');
reset();
{
  const id = mk({ workDate: today, shippingNo: 's01', slipCount: 9 });
  const r = svc.startProcess('picking', id, W, uuid());
  svc.completeProcess('picking', r.session_id, W, uuid());
  const before = sess(r.session_id);

  expectErr(() => svc.adminJudgeSession(r.session_id, 'invalid', ADMIN, '', uuid()), 400, '理由なしは 400');
  expectErr(() => svc.adminJudgeSession(r.session_id, 'bogus', ADMIN, '理由', uuid()), 400, '不正な判定値は 400');
  expectErr(() => svc.adminJudgeSession(99999, 'invalid', ADMIN, '理由', uuid()), 404, '存在しないセッションは 404');

  const j = svc.adminJudgeSession(r.session_id, 'invalid', ADMIN, '実際には作業していない', uuid());
  ok(j.validity === 'invalid', '除外できる');
  const after = sess(r.session_id);
  ok(after.validity === 'invalid' && after.validity_by === ADMIN && after.validity_at != null, '判定が記録される');
  ok(/実際には作業していない/.test(after.validity_reason), '判定理由が残る');
  ok(after.requested_at === before.requested_at && after.ended_at === before.ended_at
    && after.active_sec === before.active_sec && after.outcome === before.outcome,
    '⭐除外しても計測時刻・結果は一切書き換わらない');
  const audit = db.prepare("SELECT * FROM sw_audit_logs WHERE action='admin_judge_session' ORDER BY id DESC").get();
  ok(JSON.parse(audit.before_json).validity === 'valid' && JSON.parse(audit.after_json).validity === 'invalid',
    '判定そのものも監査ログに追記される');
  // 採用に戻せる
  svc.adminJudgeSession(r.session_id, 'valid', ADMIN, '本人に確認したので採用', uuid());
  ok(sess(r.session_id).validity === 'valid', '採用に戻せる');
  ok(db.prepare("SELECT COUNT(*) c FROM sw_audit_logs WHERE action='admin_judge_session'").get().c === 2,
    '判定のたびに監査ログが増える (上書きでなく追記)');
}

console.log('# 5b. Codexレビュー指摘の回帰');
reset();
{
  // ① 「除外」が実際に集計から外れる (以前はUIだけで集計に効いていなかった)
  const id = mk({ workDate: today, shippingNo: 's01', slipCount: 11 });
  const r = svc.startProcess('picking', id, W, uuid());
  svc.completeProcess('picking', r.session_id, W, uuid());
  ok(svc.getWorkerState('picking', W).stats.batches === 1, '完了直後は実績1バッチ');
  svc.adminJudgeSession(r.session_id, 'invalid', ADMIN, '実際には作業していない', uuid());
  ok(svc.getWorkerState('picking', W).stats.batches === 0, '⭐除外すると実績から外れる (集計に効く)');
  ok(svc.listRecentCompleted('picking', W)[0].workSec === 0, '⭐除外した時間は合算にも入らない');
  svc.adminJudgeSession(r.session_id, 'valid', ADMIN, 'やはり作業していた', uuid());
  ok(svc.getWorkerState('picking', W).stats.batches === 1, '採用に戻すと実績に戻る');

  // ② 作業中の状態へは直接飛ばせない (担当者のいない「作業中」を作らない)
  expectErr(() => svc.adminFixStatus(id, 'picking', ADMIN, '理由', uuid()), 400,
    '⭐picking へ直接変更できない');
  expectErr(() => svc.adminFixStatus(id, 'hold', ADMIN, '理由', uuid()), 400, 'hold へ直接変更できない');

  // ③ 判定は valid/invalid のみ
  expectErr(() => svc.adminJudgeSession(r.session_id, 'review_required', ADMIN, '理由', uuid()), 400,
    '⭐review_required は判定値として受け付けない');

  // ④ 未終了セッションを閉じると active_sec が確定する (採用しても集計対象が空にならない)
  const id2 = mk({ workDate: today, shippingNo: 's02', slipCount: 4 });
  const r2 = svc.startProcess('picking', id2, W, uuid());
  svc.adminFixStatus(id2, 'done', ADMIN, '現物確認済み', uuid());
  const closed = sess(r2.session_id);
  ok(closed.active_sec != null, '⭐閉じたセッションに実作業秒数が入る');
  ok(closed.ended_at != null, '終了時刻が入る');
  const au = db.prepare("SELECT * FROM sw_audit_logs WHERE action='admin_close_session' ORDER BY id DESC").get();
  ok(!!au && JSON.parse(au.before_json).outcome === 'open' && JSON.parse(au.after_json).outcome === 'cancelled',
    '⭐セッションごとに before/after の監査ログが残る');

  // ⑤ 状態が同じでも open セッションが残っていれば閉じられる (壊れた状態の救済)
  const id3 = mk({ workDate: today, shippingNo: 's03', slipCount: 4 });
  const r3 = svc.startProcess('picking', id3, W, uuid());
  db.prepare("UPDATE sw_batches SET status='done' WHERE id=?").run(id3);  // 不整合を作る
  const fixed = svc.adminFixStatus(id3, 'done', ADMIN, '取り残されたセッションを閉じる', uuid());
  ok(fixed.closedSessions === 1, '⭐状態が同じでも取り残された作業中セッションを閉じられる');
  ok(sess(r3.session_id).outcome === 'cancelled', 'セッションが閉じている');
}

console.log('# 5c. 後続工程が進んでいる場合の巻き戻し (二段階確認)');
reset();
{
  const id = mk({ workDate: today, shippingNo: 's01', slipCount: 6 });
  const r = svc.startProcess('picking', id, W, uuid());
  svc.completeProcess('picking', r.session_id, W, uuid());
  // 梱包が始まった状態を作る (PR4相当。手で packing セッションを入れる)
  db.prepare(`INSERT INTO sw_sessions (batch_id, process, worker, requested_at)
    VALUES (?, 'packing', 'w2@b-faith.biz', ?)`).run(id, dbMod.utcNow());
  db.prepare("UPDATE sw_batches SET status='packing' WHERE id=?").run(id);

  let caught = null;
  try { svc.adminFixStatus(id, 'ready', ADMIN, 'やり直す', uuid()); } catch (e) { caught = e; }
  ok(caught && caught.code === 'later_process', '⭐後の工程が進んでいると、確認なしでは巻き戻せない');
  // ready まで戻すので、進行中の梱包 + 完了済みのピッキングの両方が巻き戻し対象として出る
  const procs = (caught.detail?.sessions || []).map((s) => s.process).sort();
  ok(procs.join() === 'packing,picking', '巻き戻すことになる工程が全部返る (完了済みピッキング + 進行中梱包)');
  ok(dbMod.getBatch(id).status === 'packing', '拒否された時点では何も変わっていない');
  // 内容を確認して force で確定
  const forced = svc.adminFixStatus(id, 'ready', ADMIN, 'やり直す', uuid(), { force: true });
  ok(forced.status === 'ready' && forced.closedSessions === 1, 'force で確定できる');
  const audit = db.prepare("SELECT * FROM sw_audit_logs WHERE action='admin_fix_status' ORDER BY id DESC").get();
  ok(JSON.parse(audit.after_json).force === true, '強行したことが監査ログに残る');
}

console.log('# 5d. 巻き戻し警告の境界 (Codex round2: 位置 × outcome の組み合わせ)');
reset();
{
  // picked → ready: 完了済み picking は「直後の工程の completed」→ 警告される
  const a = mk({ workDate: today, shippingNo: 's01', slipCount: 3 });
  const ra = svc.startProcess('picking', a, W, uuid());
  svc.completeProcess('picking', ra.session_id, W, uuid());
  let caught = null;
  try { svc.adminFixStatus(a, 'ready', ADMIN, 'やり直し', uuid()); } catch (e) { caught = e; }
  ok(caught?.code === 'later_process', '⭐picked→ready は完了済みピッキングの巻き戻しとして警告される');
  // invalid 判定済みなら警告されない (工程が進んだ証拠ではない)
  svc.adminJudgeSession(ra.session_id, 'invalid', ADMIN, '誤計測', uuid());
  const f1 = svc.adminFixStatus(a, 'ready', ADMIN, 'やり直し', uuid());
  ok(f1.status === 'ready', '⭐invalid 判定済みのセッションは警告対象にならない (force不要)');
}
reset();
{
  // picking (open) → ready: 「今の工程の中止」なので警告なし
  const b = mk({ workDate: today, shippingNo: 's02', slipCount: 3 });
  svc.startProcess('picking', b, W, uuid());
  const f = svc.adminFixStatus(b, 'ready', ADMIN, '取り残しを戻す', uuid());
  ok(f.status === 'ready' && f.closedSessions === 1, 'picking(open)→ready は警告なしで閉じて戻せる');
}
reset();
{
  // done → sorted: 完了済み packing は警告される / cancelled の packing は警告されない
  const c = mk({ workDate: today, shippingNo: 's03', slipCount: 3 });
  db.prepare("UPDATE sw_batches SET status='done' WHERE id=?").run(c);
  db.prepare(`INSERT INTO sw_sessions (batch_id, process, worker, requested_at, ended_at, outcome, active_sec)
    VALUES (?, 'packing', 'w2@b-faith.biz', ?, ?, 'completed', 120)`).run(c, dbMod.utcNow(), dbMod.utcNow());
  let caught = null;
  try { svc.adminFixStatus(c, 'sorted', ADMIN, '仕分けからやり直し', uuid()); } catch (e) { caught = e; }
  ok(caught?.code === 'later_process' && caught.detail.sessions[0].process === 'packing',
    '⭐done→sorted は完了済み梱包の巻き戻しとして警告される');
  db.prepare("UPDATE sw_sessions SET outcome='cancelled' WHERE batch_id=?").run(c);
  const f = svc.adminFixStatus(c, 'sorted', ADMIN, '仕分けからやり直し', uuid());
  ok(f.status === 'sorted', '⭐cancelled のセッションは警告対象にならない');
}

console.log('# 6. 確認待ち一覧');
reset();
{
  const a = mk({ workDate: today, shippingNo: 's01', slipCount: 5 });
  const r = svc.startProcess('picking', a, W, uuid());
  svc.completeProcess('picking', r.session_id, W, uuid());
  ok(svc.listSessionsForReview().length === 0, '通常の完了は確認待ちに出ない');
  // 作業者が完了を訂正 → 確認待ちに乗る
  svc.correctCompletion('picking', r.session_id, W, 'misclick', '', uuid());
  const list = svc.listSessionsForReview();
  ok(list.length === 1 && list[0].id === r.session_id, '⭐作業者が訂正した分が確認待ちに出る');
  ok(list[0].shipping_no_label === '出荷No①' && list[0].work_date === today, 'バッチ情報が付いてくる');
  ok(/押し間違え/.test(list[0].correction_reason), '訂正理由が見える');
  ok(/^\d{2}\/\d{2} \d{2}:\d{2}$/.test(list[0].requestedAtJst), '日時はJST');
  // 判定すると一覧から消える
  svc.adminJudgeSession(r.session_id, 'valid', ADMIN, '実作業はしていたので採用', uuid());
  ok(svc.listSessionsForReview().length === 0, '判定すると確認待ちから消える');
}

console.log(`\n結果: ${pass} passed / ${fail} failed`);
db.close();
try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* Windowsのロックは無視 */ }
process.exit(fail === 0 ? 0 : 1);
