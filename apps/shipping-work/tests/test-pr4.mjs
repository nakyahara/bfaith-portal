/**
 * PR4 (仕分け・梱包フロー + ミス記録) のテスト。
 *   node test-pr4.mjs [repo-root]
 */
import path from 'path';
import fs from 'fs';
import os from 'os';
import { pathToFileURL } from 'url';

// 引数省略時はこのファイルの位置からリポジトリルートを解決する
const repo = process.argv[2]
  || path.resolve(path.dirname(new URL(import.meta.url).pathname.replace(/^\/(?=[A-Za-z]:)/, '')), '../../..');

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'sw-pr4-'));
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
    'sw_status_events', 'sw_operations', 'sw_audit_logs', 'sw_sessions', 'sw_batches']) {
    db.prepare(`DELETE FROM ${t}`).run();
  }
}
const mk = (o) => dbMod.createBatch(
  { packingMethod: 'manual', bunrui: 'tanpin', docUrl: `https://drive.google.com/d/${o.shippingNo}`, ...o }, ADMIN);
/** picking を完了させて picked にする。 */
function pick(batchId, worker = W) {
  const r = svc.startProcess('picking', batchId, worker, uuid());
  svc.completeProcess('picking', r.session_id, worker, uuid());
  return r.session_id;
}

console.log('# 1. 非アソート: picked → 梱包 → done (仕分けなし)');
reset();
{
  const id = mk({ workDate: today, shippingNo: 's01', slipCount: 10 });
  pick(id);
  ok(dbMod.getBatch(id).status === 'picked', 'ピッキング完了で picked');
  // 仕分けは対象外
  expectErr(() => svc.startProcess('sorting', id, W2, uuid()), 409, '単品バッチに仕分けは無い', 'not_applicable');
  // 梱包は picked から直接
  const rp = svc.startProcess('packing', id, W2, uuid());
  ok(dbMod.getBatch(id).status === 'packing', '梱包開始で packing');
  ok(db.prepare('SELECT COUNT(*) c FROM sw_print_jobs WHERE session_id=?').get(rp.session_id).c === 0,
    '⭐梱包開始では印刷ジョブを積まない (納品書は事前印刷)');
  const c = svc.completeProcess('packing', rp.session_id, W2, uuid());
  ok(dbMod.getBatch(id).status === 'done', '⭐梱包完了で done (全工程完了)');
  ok(typeof c.workSec === 'number', '梱包時間が計測される (現行Notionでは計測ゼロだったもの)');
}

console.log('# 2. アソート: picked → 仕分け → sorted → 梱包 → done');
reset();
{
  const id = mk({ workDate: today, shippingNo: 's01', bunrui: 'assort', slipCount: 20 });
  pick(id);
  // アソートは仕分けを経ないと梱包できない
  expectErr(() => svc.startProcess('packing', id, W2, uuid()), 409,
    '⭐アソートは仕分け前に梱包できない (from=sorted)', 'lease_lost');
  const rs = svc.startProcess('sorting', id, W2, uuid());
  ok(dbMod.getBatch(id).status === 'sorting', '仕分け開始で sorting');
  svc.completeProcess('sorting', rs.session_id, W2, uuid());
  ok(dbMod.getBatch(id).status === 'sorted', '仕分け完了で sorted');
  const rp = svc.startProcess('packing', id, W2, uuid());
  svc.completeProcess('packing', rp.session_id, W2, uuid());
  ok(dbMod.getBatch(id).status === 'done', 'アソートも梱包完了で done');
  // 各工程が別セッションとして計測される
  const procs = db.prepare('SELECT process FROM sw_sessions WHERE batch_id=? ORDER BY id').all(id).map((s) => s.process);
  ok(procs.join() === 'picking,sorting,packing', '3工程それぞれのセッションが残る');
}

console.log('# 3. ミス記録 (梱包完了時の任意入力)');
reset();
{
  const id = mk({ workDate: today, shippingNo: 's01', slipCount: 10 });
  pick(id);
  const rp = svc.startProcess('packing', id, W2, uuid());
  expectErr(() => svc.completeProcess('packing', rp.session_id, W2, uuid(),
    { mistakes: [{ kind: 'bad', count: 1 }] }), 400, '不正な分類は 400');
  expectErr(() => svc.completeProcess('packing', rp.session_id, W2, uuid(),
    { mistakes: [{ kind: 'toriwasure', count: 0 }] }), 400, '件数0は 400');
  const op = uuid();
  svc.completeProcess('packing', rp.session_id, W2, op,
    { mistakes: [{ kind: 'toriwasure', count: 2 }, { kind: 'machigai', count: 1 }] });
  const rows = db.prepare('SELECT * FROM sw_mistakes WHERE batch_id=? ORDER BY kind').all(id);
  ok(rows.length === 2, '⭐ミスが記録される (2分類)');
  ok(rows.find((m) => m.kind === 'toriwasure')?.count === 2 && rows[0].process === 'packing',
    '分類・件数・工程が入る');
  // 再送で二重記録されない
  svc.completeProcess('packing', rp.session_id, W2, op,
    { mistakes: [{ kind: 'toriwasure', count: 2 }, { kind: 'machigai', count: 1 }] });
  ok(db.prepare('SELECT COUNT(*) c FROM sw_mistakes WHERE batch_id=?').get(id).c === 2,
    '⭐同一 op_id の再送でミスが二重記録されない');
  // ミスなし (省略) も通る
  const id2 = mk({ workDate: today, shippingNo: 's02', slipCount: 5 });
  pick(id2);
  const rp2 = svc.startProcess('packing', id2, W2, uuid());
  svc.completeProcess('packing', rp2.session_id, W2, uuid());
  ok(db.prepare('SELECT COUNT(*) c FROM sw_mistakes WHERE batch_id=?').get(id2).c === 0, 'ミスなし完了も通る');
  // ピッキングでは mistakes を受け付けない (無視される)
  const id3 = mk({ workDate: today, shippingNo: 's03', slipCount: 5 });
  const r3 = svc.startProcess('picking', id3, W, uuid());
  svc.completeProcess('picking', r3.session_id, W, uuid(), { mistakes: [{ kind: 'toriwasure', count: 9 }] });
  ok(db.prepare('SELECT COUNT(*) c FROM sw_mistakes WHERE batch_id=?').get(id3).c === 0,
    'ピッキング完了の mistakes は無視される (recordMistakes=false)');
}

console.log('# 4. 開始候補の一覧 (工程ごと)');
reset();
{
  const t1 = mk({ workDate: today, shippingNo: 's01', slipCount: 5 });               // 単品 ready
  const a1 = mk({ workDate: today, shippingNo: 's02', bunrui: 'assort', slipCount: 8 });
  const t2 = mk({ workDate: today, shippingNo: 's03', slipCount: 6 });
  pick(t1); pick(a1);   // t1=picked(単品) a1=picked(アソート) t2=ready
  ok(svc.listStartableBatches('picking', today).map((b) => b.id).join() === String(t2), 'picking候補 = ready のみ');
  ok(svc.listStartableBatches('sorting', today).map((b) => b.id).join() === String(a1),
    '⭐sorting候補 = picked のアソートのみ');
  ok(svc.listStartableBatches('packing', today).map((b) => b.id).join() === String(t1),
    '⭐packing候補 = picked の非アソート (アソートは sorted になってから)');
  // 仕分けが終わるとアソートが梱包候補に入る
  const rs = svc.startProcess('sorting', a1, W2, uuid());
  svc.completeProcess('sorting', rs.session_id, W2, uuid());
  ok(svc.listStartableBatches('packing', today).map((b) => b.id).sort().join() === [t1, a1].sort().join(),
    '仕分け完了後はアソートも梱包候補に入る');
  // 「次を開始」も工程の候補から選ぶ
  const n = svc.startNextReady('packing', W2, uuid());
  ok(n && (n.batch_id === t1 || n.batch_id === a1), '梱包の「次を開始」が動く');
}

console.log('# 5. 完了訂正の後続工程判定 (工程順ベース)');
reset();
{
  // 梱包完了の訂正: ピッキングの記録は「前の工程」なので妨げない
  const id = mk({ workDate: today, shippingNo: 's01', slipCount: 5 });
  pick(id, W);
  const rp = svc.startProcess('packing', id, W2, uuid());
  svc.completeProcess('packing', rp.session_id, W2, uuid());
  const r = svc.correctCompletion('packing', rp.session_id, W2, 'misclick', '', uuid());
  ok(!!r.session_id, '⭐梱包完了を訂正できる (前工程のピッキング記録があっても妨げない)');
  ok(dbMod.getBatch(id).status === 'packing', 'バッチが packing に戻る');
  svc.completeProcess('packing', r.session_id, W2, uuid());

  // ピッキング完了の訂正: 梱包が始まっていたら不可
  const id2 = mk({ workDate: today, shippingNo: 's02', slipCount: 5 });
  const ps = pick(id2, W);
  const rp2 = svc.startProcess('packing', id2, W2, uuid());
  expectErr(() => svc.correctCompletion('picking', ps, W, 'misclick', '', uuid()), 409,
    '⭐後続の梱包が始まったらピッキングの訂正は不可 → 管理者へ', 'cannot_correct');
  svc.completeProcess('packing', rp2.session_id, W2, uuid());
}

console.log('# 6. 工程間でも一人一作業 / 保留は工程別理由');
reset();
{
  const id = mk({ workDate: today, shippingNo: 's01', slipCount: 5 });
  const id2 = mk({ workDate: today, shippingNo: 's02', slipCount: 5 });
  pick(id, W);
  const rp = svc.startProcess('packing', id, W, uuid());
  expectErr(() => svc.startProcess('picking', id2, W, uuid()), 409,
    '⭐梱包中はピッキングを開始できない (工程をまたいで一人一作業)', 'busy_worker');
  // 梱包の保留理由は pause_reason_pack (pc1=商品不足)
  expectErr(() => svc.pauseProcess('packing', rp.session_id, W, 'pk1', '', uuid()), 400,
    'ピッキング用の理由コードは梱包で使えない');
  const p = svc.pauseProcess('packing', rp.session_id, W, 'pc1', '', uuid());
  ok(!p.already && dbMod.getBatch(id).status === 'hold', '梱包を保留できる (理由=梱包マスタ)');
  // 保留中に別工程の作業へ応援に行ける
  const r2 = svc.startProcess('picking', id2, W, uuid());
  ok(!!r2.session_id, '保留中は別工程の作業を開始できる');
  svc.completeProcess('picking', r2.session_id, W, uuid());
  svc.resumeProcess('packing', rp.session_id, W, uuid());
  svc.completeProcess('packing', rp.session_id, W, uuid());
  ok(dbMod.getBatch(id).status === 'done', '再開して完了まで通る');
}

console.log('# 7. 工程別の実績と最近完了');
reset();
{
  const id = mk({ workDate: today, shippingNo: 's01', slipCount: 7 });
  pick(id, W);
  const rp = svc.startProcess('packing', id, W, uuid());
  svc.completeProcess('packing', rp.session_id, W, uuid());
  const stPick = svc.getWorkerState('picking', W);
  const stPack = svc.getWorkerState('packing', W);
  ok(stPick.stats.batches === 1 && stPack.stats.batches === 1,
    '実績は工程別 (同じバッチのピッキング1・梱包1)');
  ok(svc.listRecentCompleted('picking', W).length === 1 && svc.listRecentCompleted('packing', W).length === 1,
    '最近完了も工程別に出る');
  // 梱包完了後の帳票再出力もできる (доc_url 参照)
  const rr = svc.requestReprint('packing', rp.session_id, W, 'damaged', '', uuid());
  ok(!!rr.print_job_id, '梱包完了後も帳票を出し直せる');
}

console.log(`\n結果: ${pass} passed / ${fail} failed`);
db.close();
try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* Windowsのロックは無視 */ }
process.exit(fail === 0 ? 0 : 1);
