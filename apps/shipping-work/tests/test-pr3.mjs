/**
 * shipping-work PR3 テスト (service 状態機械 + 冪等レイヤー)。
 * DATA_DIR を一時ディレクトリに向けて実行する。
 *   node test-pr3.mjs <repo-root>
 */
import path from 'path';
import fs from 'fs';
import os from 'os';
import { pathToFileURL } from 'url';

// 引数省略時はこのファイルの位置からリポジトリルートを解決する
const repo = process.argv[2]
  || path.resolve(path.dirname(new URL(import.meta.url).pathname.replace(/^\/(?=[A-Za-z]:)/, '')), '../../..');
if (!repo) { console.error('usage: node test-pr3.mjs <repo-root>'); process.exit(1); }

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'sw-pr3-'));
process.env.DATA_DIR = tmp;

const dbMod = await import(pathToFileURL(path.join(repo, 'apps/shipping-work/db.js')));
const svc = await import(pathToFileURL(path.join(repo, 'apps/shipping-work/service.js')));

const { initShippingWorkDB, getDB, createBatch, getBatch, jstToday } = dbMod;
const {
  SwError, startProcess, completeProcess, pauseProcess, resumeProcess, troubleProcess,
  startNextReady, getWorkerState, listStartableBatches,
} = svc;

initShippingWorkDB();
const db = getDB();
const today = jstToday();

let pass = 0, fail = 0;
function ok(cond, name) {
  if (cond) { pass++; console.log(`  ok - ${name}`); }
  else { fail++; console.log(`  NG - ${name}`); }
}
function expectErr(fn, status, name, codeExpect) {
  try { fn(); fail++; console.log(`  NG - ${name} (エラーにならなかった)`); }
  catch (e) {
    const okStatus = e instanceof SwError && e.status === status;
    const okCode = codeExpect === undefined || e.code === codeExpect;
    if (okStatus && okCode) { pass++; console.log(`  ok - ${name} [${e.status} ${e.message}]`); }
    else { fail++; console.log(`  NG - ${name} (got ${e.status ?? e.code}: ${e.message})`); }
  }
}
const uuid = () => crypto.randomUUID();
const sess = (id) => db.prepare('SELECT * FROM sw_sessions WHERE id=?').get(id);
const A = 'worker-a@b-faith.biz';
const B = 'worker-b@b-faith.biz';
const ADMIN = 'admin@b-faith.biz';

// ─── 準備。開始には帳票PDFが必須なので pdfPath を付ける (ファイル実体は不要) ───
const mk = (o) => createBatch({ packingMethod: 'manual', docUrl: `https://drive.google.com/file/d/${o.shippingNo}/view`, ...o }, ADMIN);
const b1 = mk({ workDate: today, shippingNo: 's01', bunrui: 'tanpin', carriers: ['c01'], slipCount: 20 });
const b2 = mk({ workDate: today, shippingNo: 's02', bunrui: 'assort', slipCount: 30 });
const b3 = mk({ workDate: today, shippingNo: 's03', bunrui: 'tanpin', slipCount: 10 });
const b4 = mk({ workDate: '2026-07-31', shippingNo: 's04', bunrui: 'tanpin', slipCount: 5 });
const b5 = mk({ workDate: today, shippingNo: 's05', bunrui: 'tanpin', slipCount: 15 });
// 帳票未添付バッチ (開始できないことの確認用)
const bNoDoc = createBatch({ workDate: today, shippingNo: 's06', bunrui: 'tanpin', packingMethod: 'manual' }, ADMIN);

console.log('# 1. 開始・リース・排他');
const op1 = uuid();
const r1 = startProcess('picking', b1, A, op1);
const S1 = r1.session_id;
ok(!r1.already && r1.batch_id === b1, 'A が b1 を開始できる');
ok(getBatch(b1).status === 'picking', 'b1 が picking になる');
const ev1 = db.prepare('SELECT * FROM sw_status_events WHERE batch_id=? ORDER BY id DESC').get(b1);
ok(ev1.from_status === 'ready' && ev1.to_status === 'picking' && ev1.actor === A && ev1.via === 'button', 'status_events に開始が記録される');
const job1 = db.prepare('SELECT * FROM sw_print_jobs WHERE session_id=?').get(S1);
ok(job1 && job1.status === 'requested' && job1.doc_url === 'https://drive.google.com/file/d/s01/view', '開始で印刷ジョブが作られる');
expectErr(() => startProcess('picking', b1, B, uuid()), 409, 'B は同じ b1 を開始できない', 'lease_lost');
expectErr(() => startProcess('picking', b2, A, uuid()), 409, 'A は作業中のため b2 を開始できない', 'busy_worker');
expectErr(() => startProcess('picking', bNoDoc, B, uuid()), 409, '帳票未設定バッチは開始できない', 'no_document');
ok(getBatch(bNoDoc).status === 'ready', '帳票未設定で弾かれたバッチは ready のまま (開始済みデータを残さない)');
const r1b = startProcess('picking', b1, A, op1);
ok(r1b.already && r1b.session_id === S1, '同一 op_id 再送は already で同セッションを返す (冪等)');

console.log('# 1b. op_id の束縛 (Codex#1: 別対象への使い回しを拒否)');
expectErr(() => startProcess('picking', b3, A, op1), 409, '同じ op_id を別バッチに送ると 409', 'op_conflict');
expectErr(() => startProcess('picking', b1, B, op1), 409, '他人の op_id を使うと 409', 'op_conflict');
expectErr(() => completeProcess('picking', S1, A, op1), 409, 'start の op_id を complete に使うと 409', 'op_conflict');

console.log('# 2. 保留・再開');
expectErr(() => pauseProcess('picking', S1, A, 'pk7', '', uuid()), 400, '「その他」で記述なしは 400');
expectErr(() => pauseProcess('picking', S1, A, 'bad', '', uuid()), 400, '不正理由は 400');
expectErr(() => pauseProcess('picking', S1, B, 'pk1', '', uuid()), 403, '他人のセッションは保留できない');
const opPause = uuid();
const p1 = pauseProcess('picking', S1, A, 'pk1', '', opPause);
ok(!p1.already, '保留できる');
ok(getBatch(b1).status === 'hold' && getBatch(b1).hold_from_status === 'picking', 'バッチが hold + 復帰先保存');
ok(JSON.parse(sess(S1).flags_json).includes('early_pause'), '開始60秒以内の保留は early_pause フラグ');
ok(sess(S1).paused === 1, '保留セッションは paused=1');
ok(pauseProcess('picking', S1, A, 'pk1', '', opPause).already, '同一 op_id の保留再送は already');
ok(pauseProcess('picking', S1, A, 'pk1', '', uuid()).already, '別 op_id の二重クリックも already (状態判定)');
expectErr(() => pauseProcess('picking', S1, A, 'pk2', '', opPause), 409, '同じ op_id で理由だけ変えると 409', 'op_conflict');
expectErr(() => completeProcess('picking', S1, A, uuid()), 409, '保留中は完了できない');
expectErr(() => troubleProcess('picking', S1, A, 'jam', '', 'abort', uuid()), 409, '保留中はトラブル処理できない');

// 保留中は「他作業への応援」に行ける (保留理由マスタにある運用。migration v3)
const rHelp = startProcess('picking', b3, A, uuid());
const SHELP = rHelp.session_id;
ok(!rHelp.already, '保留中でも別バッチを開始できる (他作業への応援)');
const stHelp = getWorkerState('picking', A);
ok(stHelp.session.id === SHELP, '進行中は応援先のセッション');
ok(stHelp.paused.length === 1 && stHelp.paused[0].session.id === S1, '保留中リストに元の作業が出る');
ok(stHelp.paused[0].reasonLabel === '在庫不足', '保留中リストに理由ラベルが出る');
expectErr(() => resumeProcess('picking', S1, A, uuid()), 409, '進行中があると保留を再開できない', 'busy_worker');
completeProcess('picking', SHELP, A, uuid());
ok(getBatch(b3).status === 'picked', '応援先バッチが picked');

const opResume = uuid();
const res1 = resumeProcess('picking', S1, A, opResume);
ok(!res1.already, '再開できる');
ok(getBatch(b1).status === 'picking' && getBatch(b1).hold_from_status === null, 'バッチが picking に復帰');
ok(sess(S1).paused === 0, '再開で paused=0 に戻る');
ok(resumeProcess('picking', S1, A, opResume).already, '再開の再送は already');
ok(db.prepare('SELECT * FROM sw_pauses WHERE session_id=?').get(S1).resumed_at != null, '保留区間が閉じられている');

console.log('# 2b. 遅延再送の防御 (Codex#2: 保留A → 再開 → Aが遅延再送)');
const beforeReplay = db.prepare('SELECT COUNT(*) c FROM sw_pauses WHERE session_id=?').get(S1).c;
const replay = pauseProcess('picking', S1, A, 'pk1', '', opPause);
ok(replay.already, '再開後に古い保留リクエストが遅延再送されても already');
ok(db.prepare('SELECT COUNT(*) c FROM sw_pauses WHERE session_id=?').get(S1).c === beforeReplay, '新しい保留区間が作られない');
ok(getBatch(b1).status === 'picking', 'バッチが hold に巻き戻らない');
ok(resumeProcess('picking', S1, A, opResume).already, '古い再開リクエストの遅延再送も already');

console.log('# 3. 完了 (計測記録・異常フラグ)');
const opDone = uuid();
const c1 = completeProcess('picking', S1, A, opDone);
ok(!c1.already && c1.flags.includes('too_short') && c1.flags.includes('early_pause'), '完了 + too_short/early_pause フラグ');
ok(typeof c1.workSec === 'number' && typeof c1.pauseSec === 'number', 'workSec/pauseSec が返る');
ok(getBatch(b1).status === 'picked', 'b1 が picked');
ok(sess(S1).outcome === 'completed' && sess(S1).ended_at != null, 'セッションが completed + ended_at');
ok(sess(S1).active_sec === c1.workSec, '実作業秒数 (active_sec) が保存される = 異常判定の根拠が残る');
ok(db.prepare('SELECT status FROM sw_print_jobs WHERE session_id=?').get(S1).status === 'cancelled', '未消化の印刷ジョブは cancelled (PR6事故防止)');
ok(completeProcess('picking', S1, A, opDone).already, '完了の再送は already');
ok(completeProcess('picking', S1, A, uuid()).already, '別 op_id の二重クリックも already');

console.log('# 4. 印刷トラブル (reprint / abort)');
const r2 = startProcess('picking', b2, A, uuid());
const S2 = r2.session_id;
expectErr(() => troubleProcess('picking', S2, A, 'other', '', 'reprint', uuid()), 400, 'トラブル「その他」記述なしは 400');
const opTrouble = uuid();
const t1 = troubleProcess('picking', S2, A, 'jam', '', 'reprint', opTrouble);
ok(!t1.already && t1.session_id !== S2, 'reprint で新セッションが作られる');
ok(JSON.parse(sess(t1.session_id).flags_json).includes('reprint'), '新セッションに reprint フラグ');
ok(sess(S2).outcome === 'voided' && /紙詰まり/.test(sess(S2).void_reason), '旧セッションは voided + 理由');
ok(getBatch(b2).status === 'picking', 'バッチは picking のまま');
ok(db.prepare('SELECT COUNT(*) c FROM sw_print_jobs WHERE session_id=?').get(t1.session_id).c === 1, '再印刷で新しい印刷ジョブが作られる');
const t1b = troubleProcess('picking', S2, A, 'jam', '', 'reprint', opTrouble);
ok(t1b.already && t1b.session_id === t1.session_id, 'トラブル再送は already で同じ新セッションを返す');
ok(db.prepare('SELECT COUNT(*) c FROM sw_sessions WHERE batch_id=?').get(b2).c === 2, '再送でセッションが増えない');
const opAbort = uuid();
const t2 = troubleProcess('picking', t1.session_id, A, 'no_output', '', 'abort', opAbort);
ok(t2.aborted === true, 'abort できる');
ok(getBatch(b2).status === 'ready', 'バッチが ready に戻る');
ok(troubleProcess('picking', t1.session_id, A, 'no_output', '', 'abort', opAbort).already, 'abort 再送は already');

console.log('# 5. 完了して次を開始 (持ち越し優先順・再送で別バッチを指さない)');
ok(listStartableBatches('picking', today)[0].id === b4, '持ち越し (7/31) が先頭に並ぶ');
const r3 = startProcess('picking', b2, B, uuid());
ok(getWorkerState('picking', B).session.id === r3.session_id, 'getWorkerState が B の進行中セッションを返す');
completeProcess('picking', r3.session_id, B, uuid());
const opNext = uuid();
const n1 = startNextReady('picking', B, opNext);
ok(n1 && n1.batch_id === b4, '次を開始で持ち越しバッチが開始される');
// Codex#4: 再送時に候補一覧の先頭ではなく、前回開始したバッチを返すこと
const n1b = startNextReady('picking', B, opNext);
ok(n1b.already && n1b.batch_id === b4 && n1b.session_id === n1.session_id, '再送は前回開始したバッチ/セッションを返す');
ok(db.prepare("SELECT COUNT(*) c FROM sw_sessions WHERE worker=? AND outcome='open'").get(B).c === 1, '再送でセッションが増えない');
completeProcess('picking', n1.session_id, B, uuid());

console.log('# 6. 状態・統計');
const stA = getWorkerState('picking', A);
ok(stA.session === null && stA.paused.length === 0, 'A は未作業・保留なし');
ok(stA.ready.map((b) => b.id).sort().join() === [b5, bNoDoc].sort().join(), 'A の開始可能一覧は b5 と帳票未設定バッチ');
ok(stA.stats.batches === 2 && stA.stats.slips === 30, 'A の本日実績 = 2バッチ/30伝票 (b1:20+b3:10)');
const stB = getWorkerState('picking', B);
ok(stB.stats.batches === 2 && stB.stats.slips === 35, 'B の本日実績 = 2バッチ/35伝票 (b2:30+b4:5)');
// 「次を開始」は帳票未添付を自動では選ばない
const nSkip = startNextReady('picking', A, uuid());
ok(nSkip && nSkip.batch_id === b5, '次を開始は帳票未設定バッチを飛ばして b5 を選ぶ');
completeProcess('picking', nSkip.session_id, A, uuid());
ok(startNextReady('picking', A, uuid()) === null, '開始できる候補が帳票未設定だけなら null');

console.log('# 6b. 「次を開始」の候補なしも冪等 (Codex round2 #high/#medium)');
// ここで開始可能なのは帳票未添付バッチのみ = 候補なし
const opEmpty = uuid();
ok(startNextReady('picking', A, opEmpty) === null, '候補なしなら null');
// 「候補なし」の後で ready バッチが増えても、同じ op_id の再送は開始しない
const bLate = mk({ workDate: today, shippingNo: 's07', bunrui: 'tanpin', slipCount: 7 });
ok(startNextReady('picking', A, opEmpty) === null, '候補が増えても同一 op_id の再送は開始しない');
ok(getBatch(bLate).status === 'ready', '後から増えたバッチは ready のまま');
// 手動開始の op_id を op_id_next に使い回すと拒否される (start: と start-next: は別操作)
const opManual = uuid();
const rMan = startProcess('picking', bLate, A, opManual);
expectErr(() => startNextReady('picking', A, opManual), 409, '手動開始の op_id を「次を開始」に使うと 409', 'op_conflict');
completeProcess('picking', rMan.session_id, A, uuid());

// 空状態の出し分け (noBatchesAtAll) は状態を作り分ける必要があるため test-empty-state.mjs に分離

console.log('# 7. バリデーション');
expectErr(() => startProcess('picking', 99999, A, uuid()), 404, '存在しないバッチは 404');
expectErr(() => startProcess('picking', b5, A, 'x'), 400, '短すぎる op_id は 400');
expectErr(() => startProcess('bad-process', b5, A, uuid()), 400, '不明工程は 400');
expectErr(() => pauseProcess('picking', S1, A, 'pk1', 'あ'.repeat(501), uuid()), 400, '補足500文字超は 400');
expectErr(() => troubleProcess('picking', 12345, A, 'jam', '', 'reprint', uuid()), 404, '存在しないセッションは 404');
expectErr(() => completeProcess('picking', S1, B, uuid()), 403, '他人の完了は 403');
expectErr(() => troubleProcess('picking', S1, A, 'jam', '', 'bad', uuid()), 400, '不正な action は 400');

console.log('# 8. 追記型の保証');
const evCount = db.prepare('SELECT COUNT(*) c FROM sw_status_events').get().c;
const sesCount = db.prepare('SELECT COUNT(*) c FROM sw_sessions').get().c;
const opCount = db.prepare('SELECT COUNT(*) c FROM sw_operations').get().c;
ok(evCount >= 15, `status_events が追記されている (${evCount}件)`);
ok(sesCount === 8, `sw_sessions は8件 (A:b1/b3/b2/b2再/b5, B:b2/b4/s07) → 実際 ${sesCount}件`);
ok(db.prepare("SELECT COUNT(*) c FROM sw_operations WHERE operation LIKE 'start-next:%'").get().c >= 2,
  '「次を開始」が start-next 操作として記録されている (候補なしも含む)');
ok(db.prepare("SELECT COUNT(*) c FROM sw_sessions WHERE outcome='voided'").get().c === 2, 'voided セッションが2件残っている (削除されない)');
ok(opCount > 0 && db.prepare("SELECT COUNT(*) c FROM sw_operations WHERE operation LIKE 'start:%'").get().c > 0, `sw_operations に操作が記録されている (${opCount}件)`);
ok(db.prepare("SELECT COUNT(*) c FROM sw_sessions WHERE outcome='open'").get().c === 0, 'open セッションが残っていない');

console.log(`\n結果: ${pass} passed / ${fail} failed`);
db.close();
try { fs.rmSync(tmp, { recursive: true, force: true }); } catch {}
process.exit(fail === 0 ? 0 : 1);
