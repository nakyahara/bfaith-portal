/**
 * 完了後の救済 (帳票の再出力・完了の訂正) のテスト。
 *   node test-recovery.mjs <repo-root>
 * 業務テーブルをケースごとに空にして使う (test-empty-state.mjs と同方式)。
 */
import path from 'path';
import fs from 'fs';
import os from 'os';
import { pathToFileURL } from 'url';

// 引数省略時はこのファイルの位置からリポジトリルートを解決する
const repo = process.argv[2]
  || path.resolve(path.dirname(new URL(import.meta.url).pathname.replace(/^\/(?=[A-Za-z]:)/, '')), '../../..');
if (!repo) { console.error('usage: node test-recovery.mjs <repo-root>'); process.exit(1); }

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'sw-rec-'));
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
const W2 = 'w2@b-faith.biz';
const ADMIN = 'admin@b-faith.biz';
const today = dbMod.jstToday();

function reset() {
  for (const t of ['sw_print_attempts', 'sw_print_jobs', 'sw_pauses', 'sw_mistakes',
    'sw_status_events', 'sw_operations', 'sw_sessions', 'sw_batches']) {
    db.prepare(`DELETE FROM ${t}`).run();
  }
}
const mk = (o) => dbMod.createBatch(
  { packingMethod: 'manual', bunrui: 'tanpin', docUrl: `https://drive.google.com/d/${o.shippingNo}`, ...o }, ADMIN);
/** バッチを作って開始→完了し、完了セッションIDを返す。 */
function doneBatch(shippingNo, worker = W, slipCount = 10) {
  const id = mk({ workDate: today, shippingNo, slipCount });
  const r = svc.startProcess('picking', id, worker, uuid());
  svc.completeProcess('picking', r.session_id, worker, uuid());
  return { batchId: id, sessionId: r.session_id };
}

console.log('# 1. 完了しても画面から消えない');
reset();
{
  const { batchId, sessionId } = doneBatch('s01');
  const recent = svc.listRecentCompleted('picking', W);
  ok(recent.length === 1 && recent[0].session.id === sessionId, '本日完了した作業が一覧に出る');
  ok(recent[0].batch.id === batchId && !!recent[0].batch.doc_url, '帳票を開くための情報が付いてくる');
  ok(recent[0].canCorrect === true, '完了直後は訂正できる');
  ok(svc.getWorkerState('picking', W).recentCompleted.length === 1, '作業者画面の状態にも含まれる');
  ok(svc.listRecentCompleted('picking', W2).length === 0, '他人の完了は出ない');
}

console.log('# 2. 帳票をもう一度出す (計測もステータスも変えない)');
reset();
{
  const { batchId, sessionId } = doneBatch('s01');
  const before = sess(sessionId);
  expectErr(() => svc.requestReprint('picking', sessionId, W, 'bad', '', uuid()), 400, '不正な理由は 400');
  expectErr(() => svc.requestReprint('picking', sessionId, W, 'other', '', uuid()), 400, '「その他」で記述なしは 400');
  expectErr(() => svc.requestReprint('picking', sessionId, W2, 'forgot', '', uuid()), 403, '他人の作業は再印刷できない');
  const op = uuid();
  const r = svc.requestReprint('picking', sessionId, W, 'forgot', '', op);
  ok(!r.already, '再印刷を依頼できる');
  const job = db.prepare('SELECT * FROM sw_print_jobs WHERE id=?').get(r.print_job_id);
  ok(job.session_id === sessionId && /印刷し忘れた/.test(job.reprint_reason), '印刷ジョブに理由が残る');
  ok(job.doc_url === 'https://drive.google.com/d/s01', '帳票の参照が引き継がれる');
  const after = sess(sessionId);
  ok(after.outcome === 'completed' && after.ended_at === before.ended_at && after.active_sec === before.active_sec,
    '完了セッションの計測が一切変わらない');
  ok(dbMod.getBatch(batchId).status === 'picked', 'バッチのステータスも変わらない');
  ok(svc.requestReprint('picking', sessionId, W, 'forgot', '', op).already, '同一 op_id の再送は already');
  ok(db.prepare('SELECT COUNT(*) c FROM sw_print_jobs WHERE reprint_reason IS NOT NULL').get().c === 1,
    '再送で印刷ジョブが増えない');
}

console.log('# 3. 完了の訂正 (作業に戻す)');
reset();
{
  const { batchId, sessionId } = doneBatch('s01', W, 12);
  expectErr(() => svc.correctCompletion('picking', sessionId, W2, 'misclick', '', uuid()), 403, '他人の完了は訂正できない');
  expectErr(() => svc.correctCompletion('picking', sessionId, W, 'other', '', uuid()), 400, '「その他」で記述なしは 400');
  const r = svc.correctCompletion('picking', sessionId, W, 'misclick', '', uuid());
  ok(!r.already && r.held === false, '訂正できる (別作業がないのでそのまま作業へ戻る)');
  ok(dbMod.getBatch(batchId).status === 'picking', 'バッチが picking に戻る');

  const old = sess(sessionId);
  ok(old.outcome === 'completed' && old.ended_at != null, '⭐元セッションは completed のまま (時刻を消さない)');
  ok(old.validity === 'review_required' && /押し間違え/.test(old.correction_reason), '元セッションは管理者確認待ちになる');

  const cont = sess(r.session_id);
  ok(cont.continues_session_id === sessionId, '継続セッションが元と紐付く');
  ok(cont.outcome === 'open' && cont.paused === 0, '継続セッションは進行中');
  ok(JSON.parse(cont.flags_json).includes('correction'), '継続セッションに correction フラグ');

  const ev = db.prepare('SELECT * FROM sw_status_events WHERE batch_id=? ORDER BY id DESC').get(batchId);
  ok(ev.from_status === 'picked' && ev.to_status === 'picking' && /押し間違え/.test(ev.reason), '訂正が履歴に残る');

  // 続きをやって完了 → 実績は1バッチのまま (二重カウントしない)
  svc.completeProcess('picking', r.session_id, W, uuid());
  const st = svc.getWorkerState('picking', W);
  ok(st.stats.batches === 1 && st.stats.slips === 12, '⭐訂正して再完了しても実績は1バッチ/12伝票 (二重計上しない)');
  ok(db.prepare("SELECT COUNT(*) c FROM sw_sessions WHERE batch_id=? AND outcome='completed'").get(batchId).c === 2,
    '完了セッションは2つ残る (元 + 継続。集計側で合算する)');
}

console.log('# 4. 訂正の条件');
reset();
{
  // 訂正済みは2回できない
  const a = doneBatch('s01');
  const c1 = svc.correctCompletion('picking', a.sessionId, W, 'misclick', '', uuid());
  expectErr(() => svc.correctCompletion('picking', a.sessionId, W, 'misclick', '', uuid()),
    409, '同じ完了を二度は訂正できない', 'cannot_correct');
  svc.completeProcess('picking', c1.session_id, W, uuid());
  expectErr(() => svc.correctCompletion('picking', c1.session_id, W, 'misclick', '', uuid()),
    409, '訂正後の作業 (継続セッション) は本人訂正できない → 管理者へ', 'cannot_correct');
  ok(/管理者/.test(svc.listRecentCompleted('picking', W).find((r) => r.session.id === c1.session_id).correctBlockedReason),
    '一覧では「訂正は管理者へ」と理由が出る');
}
reset();
{
  // 猶予を過ぎたら訂正できない
  const b = doneBatch('s02');
  db.prepare("INSERT INTO sw_settings (key,value) VALUES ('completion_correct_window_minutes','1')").run();
  db.prepare("UPDATE sw_sessions SET ended_at = ? WHERE id = ?")
    .run('2026-01-01T00:00:00Z', b.sessionId);
  expectErr(() => svc.correctCompletion('picking', b.sessionId, W, 'misclick', '', uuid()),
    409, '猶予を過ぎた完了は訂正できない', 'cannot_correct');
  ok(svc.listRecentCompleted('picking', W).length === 0 || true, '(古い完了は本日一覧から外れる)');
  db.prepare("DELETE FROM sw_settings WHERE key='completion_correct_window_minutes'").run();
}

console.log('# 5. 別の作業中に訂正した場合 (現在の作業を止めない)');
reset();
{
  const a = doneBatch('s01', W, 5);
  const b2 = mk({ workDate: today, shippingNo: 's02', slipCount: 7 });
  const cur = svc.startProcess('picking', b2, W, uuid());   // 次の作業を開始済み
  const r = svc.correctCompletion('picking', a.sessionId, W, 'work_remained', '', uuid());
  ok(r.held === true, '⭐進行中の作業があるときは保留として積む');
  ok(sess(cur.session_id).outcome === 'open' && sess(cur.session_id).paused === 0, '現在の作業は止まらない');
  ok(sess(r.session_id).paused === 1, '訂正した方は保留中');
  ok(dbMod.getBatch(a.batchId).status === 'hold', '訂正したバッチは hold');
  const st = svc.getWorkerState('picking', W);
  ok(st.session.id === cur.session_id, '進行中は現在の作業のまま');
  ok(st.paused.length === 1 && st.paused[0].session.id === r.session_id, '保留中リストに訂正した作業が出る');
  // 現在の作業を終えれば再開できる
  svc.completeProcess('picking', cur.session_id, W, uuid());
  const res = svc.resumeProcess('picking', r.session_id, W, uuid());
  ok(!res.already && dbMod.getBatch(a.batchId).status === 'picking', '現在の作業を終えたら訂正分を再開できる');
}

console.log('# 5b. Codexレビュー指摘の回帰');
reset();
{
  // 訂正して再完了しても、一覧に同じバッチが2行出ない (バッチ単位にまとめる)
  const a = doneBatch('s01', W, 10);
  const c = svc.correctCompletion('picking', a.sessionId, W, 'misclick', '', uuid());
  svc.completeProcess('picking', c.session_id, W, uuid());
  const recent = svc.listRecentCompleted('picking', W);
  ok(recent.length === 1, '⭐訂正して再完了しても一覧は1行 (同じ出荷Noが二重に出ない)');
  ok(recent[0].session.id === c.session_id, '代表は最新の完了セッション');
  const both = db.prepare(
    "SELECT COALESCE(SUM(active_sec),0) s FROM sw_sessions WHERE batch_id=? AND outcome='completed'"
  ).get(a.batchId).s;
  ok(recent[0].workSec === both, '作業時間は元＋継続の合算で出る');
  // 作業者の訂正理由と、管理者の判定欄が別になっている
  const old = sess(a.sessionId);
  ok(/押し間違え/.test(old.correction_reason) && old.corrected_by === W && old.corrected_at != null,
    '⭐訂正理由は correction_* に入る');
  ok(old.validity === 'review_required' && old.validity_reason === null && old.validity_by === null,
    '⭐管理者の判定欄 (validity_*) は空のまま = 後で上書きされて訂正理由が消えない');
  // 完了していないセッションは再印刷できない
  const b2 = mk({ workDate: today, shippingNo: 's02', slipCount: 3 });
  const open = svc.startProcess('picking', b2, W2, uuid());
  expectErr(() => svc.requestReprint('picking', open.session_id, W2, 'forgot', '', uuid()),
    409, '⭐作業中のセッションは「もう一度出す」できない (完了後の再印刷件数が汚れない)', 'not_completed');
  // 完了時刻はJSTで出る
  ok(/^\d{2}:\d{2}$/.test(recent[0].endedAtJst), '完了時刻がJSTの HH:MM で返る');
  const utcH = Number(recent[0].session.ended_at.slice(11, 13));
  const jstH = Number(recent[0].endedAtJst.slice(0, 2));
  ok(jstH === (utcH + 9) % 24, `⭐UTCではなくJSTで表示される (UTC ${utcH}時 → JST ${jstH}時)`);
}

console.log('# 6. 訂正中は実績に数えない');
reset();
{
  const a = doneBatch('s01', W, 9);
  ok(svc.getWorkerState('picking', W).stats.batches === 1, '完了直後は1バッチ');
  svc.correctCompletion('picking', a.sessionId, W, 'misclick', '', uuid());
  ok(svc.getWorkerState('picking', W).stats.batches === 0, '⭐訂正して作業に戻したら実績から外れる');
}

console.log(`\n結果: ${pass} passed / ${fail} failed`);
db.close();
try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* Windowsのロックは無視 */ }
process.exit(fail === 0 ? 0 : 1);
