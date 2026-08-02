/**
 * noBatchesAtAll の境界テスト (Codexの助言: 過去日完了 / 過去日持ち越し / 未来日のみ / cancelledのみ)。
 *   node test-empty-state.mjs <repo-root>
 *
 * 注: DBは1つだけ作り、ケースごとに業務テーブルを空にする。
 * DATA_DIR を切り替えて module を再読み込みする方式は使えない —
 * service.js が import する db.js は素のパスで解決されるためキャッシュが効き、
 * 最初に読まれた DATA_DIR に固定されてしまう。
 */
import path from 'path';
import fs from 'fs';
import os from 'os';
import { pathToFileURL } from 'url';

// 引数省略時はこのファイルの位置からリポジトリルートを解決する
const repo = process.argv[2]
  || path.resolve(path.dirname(new URL(import.meta.url).pathname.replace(/^\/(?=[A-Za-z]:)/, '')), '../../..');
if (!repo) { console.error('usage: node test-empty-state.mjs <repo-root>'); process.exit(1); }

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'sw-empty-'));
process.env.DATA_DIR = tmp;

const dbMod = await import(pathToFileURL(path.join(repo, 'apps/shipping-work/db.js')));
const svc = await import(pathToFileURL(path.join(repo, 'apps/shipping-work/service.js')));
dbMod.initShippingWorkDB();
const db = dbMod.getDB();

let pass = 0, fail = 0;
const ok = (c, n) => { if (c) { pass++; console.log(`  ok - ${n}`); } else { fail++; console.log(`  NG - ${n}`); } };

const W = 'worker@b-faith.biz';
const ADMIN = 'admin@b-faith.biz';
const today = dbMod.jstToday();
const dayOffset = (n) => new Date(Date.now() + n * 86400000).toISOString().slice(0, 10);

/** 業務テーブルを空にする (子テーブルから順に。マスタと設定は残す)。 */
function reset() {
  for (const t of ['sw_print_attempts', 'sw_print_jobs', 'sw_pauses', 'sw_mistakes',
    'sw_status_events', 'sw_operations', 'sw_sessions', 'sw_batches']) {
    db.prepare(`DELETE FROM ${t}`).run();
  }
}

const mk = (o) => dbMod.createBatch(
  { packingMethod: 'manual', bunrui: 'tanpin', docUrl: `https://drive.google.com/file/d/${o.shippingNo}/view`, ...o }, ADMIN);

console.log('# 1. バッチが1件も無い');
reset();
{
  const st = svc.getWorkerState('picking', W);
  ok(st.ready.length === 0 && st.noBatchesAtAll === true, 'バッチ0件 → noBatchesAtAll=true (作成を促す)');
}

console.log('# 2. 未来日のバッチだけ');
reset();
{
  mk({ workDate: dayOffset(3), shippingNo: 's01' });
  const st = svc.getWorkerState('picking', W);
  ok(st.ready.length === 0, '未来日は開始候補に出ない');
  ok(st.noBatchesAtAll === true, '未来日のみ → noBatchesAtAll=true (今日はまだ未登録)');
}

console.log('# 3. 取消済みのバッチだけ');
reset();
{
  const id = mk({ workDate: today, shippingNo: 's01' });
  dbMod.cancelBatch(id, ADMIN, 'テスト取消');
  const st = svc.getWorkerState('picking', W);
  ok(st.ready.length === 0, '取消済みは開始候補に出ない');
  ok(st.noBatchesAtAll === true, 'cancelledのみ → noBatchesAtAll=true (実質未登録)');
}

console.log('# 4. 完了したバッチがある (全部やり終えた)');
reset();
{
  const id = mk({ workDate: today, shippingNo: 's01', slipCount: 5 });
  const r = svc.startProcess('picking', id, W, crypto.randomUUID());
  svc.completeProcess('picking', r.session_id, W, crypto.randomUUID());
  const st = svc.getWorkerState('picking', W);
  ok(st.ready.length === 0, '完了済みは開始候補に出ない');
  ok(st.noBatchesAtAll === false, '完了バッチがある → noBatchesAtAll=false (「お疲れさまでした」)');
}

console.log('# 5. 過去日の持ち越しバッチがある');
reset();
{
  const id = mk({ workDate: dayOffset(-2), shippingNo: 's01', slipCount: 4 });
  const st = svc.getWorkerState('picking', W);
  ok(st.ready.length === 1 && st.ready[0].id === id, '過去日の未完了は持ち越しとして開始候補に出る');
  ok(st.noBatchesAtAll === false, '持ち越しがある → noBatchesAtAll=false');
}

console.log('# 6. 作業中');
reset();
{
  const id = mk({ workDate: today, shippingNo: 's01', slipCount: 6 });
  svc.startProcess('picking', id, W, crypto.randomUUID());
  const st = svc.getWorkerState('picking', W);
  ok(st.session !== null && st.noBatchesAtAll === false, '作業中 → noBatchesAtAll=false (空表示にならない)');
}

console.log(`\n結果: ${pass} passed / ${fail} failed`);
db.close();
try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* Windowsのロックは無視 */ }
process.exit(fail === 0 ? 0 : 1);
