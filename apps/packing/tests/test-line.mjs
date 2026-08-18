/**
 * test-line.mjs — 梱包機ライン管理 (PAS/MELT — 紙台帳の置き換え) の検証
 *
 * 実行: node apps/packing/tests/test-line.mjs
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'packing-line-test-'));
process.env.DATA_DIR = tmpDir;
delete process.env.PACKING_NOTION_TOKEN;
delete process.env.PICKING_NOTION_TOKEN;

const { initPackingDB, getDB, utcNow } = await import('../db.js');
const { applyEvent, lineKindOf, listLineRuns, PackError } = await import('../service.js');
const { packBatchNotionState, STATUS_PACKING, STATUS_PACK_DONE, STATUS_SORTED } = await import('../notion.js');

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const eq = (a, b, label) => ok(JSON.stringify(a) === JSON.stringify(b), `${label} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);
const throws = (fn, code, label) => {
  try { fn(); ok(false, `${label} (エラーにならなかった)`); }
  catch (e) { ok(e instanceof PackError && e.code === code, `${label} (${e.code || e.message})`); }
};

initPackingDB();
const db = getDB();
// picking 所有の pk_batches を試験用に用意 (実環境では picking が作る)
db.exec('CREATE TABLE IF NOT EXISTS pk_batches (id INTEGER PRIMARY KEY, hikiate_class TEXT)');
db.prepare('INSERT INTO pk_batches (id, hikiate_class) VALUES (1, ?), (2, ?), (3, ?)').run(
  'ネコポス【梱包機PAS-LINE《3つ折り》】単品',
  'ネコポス【梱包機MELT-LINE】複数SKU複数個',
  'ゆうパケットパフ《単品、複数個を含む全て》',
);
const now = utcNow();
const mkBatch = (id, pkId, slips) => db.prepare(`
  INSERT INTO pk_pack_batches (id, tb_key, folder_name, work_date, slip_count, line_count, total_qty,
    pk_batch_id, match_status, status, csv_sha256, imported_by, created_at, updated_at)
  VALUES (?, ?, ?, '2026-08-18', ?, ?, ?, ?, 'ok', 'ready', 'x', 'test', ?, ?)
`).run(id, `TB${id}`, `出荷_${id}`, slips, slips, slips, pkId, now, now);
mkBatch(1, 1, 100);   // PAS
mkBatch(2, 2, 50);    // MELT
mkBatch(3, 3, 10);    // 手梱包

let op = 0;
const ev = (batchId, event, extra = {}, worker = '倉田') =>
  applyEvent(batchId, { opId: `t${++op}`, event, ...extra }, worker);

console.log('── lineKindOf ──');
{
  eq(lineKindOf('ネコポス【梱包機PAS-LINE《2つ折り》】単品'), 'pas', 'PAS-LINE 判定');
  eq(lineKindOf('ネコポス【梱包機MELT-LINE】1SKU複数個'), 'melt', 'MELT-LINE 判定');
  eq(lineKindOf('定形外手動《単品、複数個を含む全て》'), null, '手梱包は null');
  eq(lineKindOf(null), null, '分類なしは null');
}

console.log('\n── PAS: 流し開始→終了 ──');
{
  throws(() => ev(3, 'line_start'), 'not_line_batch', '手梱包バッチにはライン操作不可');
  throws(() => ev(1, 'line_sort_start'), 'not_melt', 'PAS に仕分け工程はない');
  ev(1, 'line_start');
  const b = db.prepare('SELECT * FROM pk_pack_batches WHERE id=1').get();
  eq(b.status, 'packing', '流し開始でバッチは作業中');
  eq(b.worker, '倉田', '担当者が入る');
  const run = listLineRuns(1).find((r) => r.phase === 'run');
  eq(run.planned_count, 100, 'PAS の予定件数=伝票数');
  throws(() => ev(1, 'line_start'), 'already_started', '二重開始は拒否');
  throws(() => ev(1, 'line_done', { finalCount: 101 }), 'bad_count', '完了件数>予定は拒否');
  throws(() => ev(1, 'line_done', { finalCount: 99, manualCount: 100 }), 'bad_count', '手動>完了は拒否');
  throws(() => ev(1, 'line_done', { finalCount: 99 }, '別人'), 'taken', '担当者以外は操作不可');
  ev(1, 'line_done', { finalCount: 99, manualCount: 2, note: '1件ピッキングミス' });
  const b2 = db.prepare('SELECT * FROM pk_pack_batches WHERE id=1').get();
  eq(b2.status, 'done', '終了でバッチ完了');
  const run2 = listLineRuns(1).find((r) => r.phase === 'run');
  eq([run2.final_count, run2.manual_count, run2.note], [99, 2, '1件ピッキングミス'], '件数・手動・備考を記録');
  throws(() => ev(1, 'line_done', { finalCount: 99 }), 'already_done', '完了後の再終了は拒否');
}

console.log('\n── MELT: 仕分け→流し ──');
{
  throws(() => ev(2, 'line_start'), 'sort_first', '仕分け前の流し開始は拒否');
  ev(2, 'line_sort_start');
  eq(db.prepare('SELECT status FROM pk_pack_batches WHERE id=2').get().status, 'packing', '仕分け開始で作業中');
  throws(() => ev(2, 'line_sort_start'), 'already_started', '仕分けの二重開始は拒否');
  throws(() => ev(2, 'line_sort_done', { finalCount: 51 }), 'bad_count', '最終通過>予定は拒否');
  ev(2, 'line_sort_done', { finalCount: 47, note: '3件を手動へ変更' });
  const sort = listLineRuns(2).find((r) => r.phase === 'sort');
  eq([sort.planned_count, sort.final_count, sort.note], [50, 47, '3件を手動へ変更'], '仕分け結果を記録');
  // Notion ラベル: 仕分け完了〜流し開始は「仕分け完了」
  const bMid = db.prepare('SELECT * FROM pk_pack_batches WHERE id=2').get();
  eq(packBatchNotionState(bMid, { kind: 'melt', sortDone: true, runStarted: false, finalCount: null }).label,
    STATUS_SORTED, '仕分け完了〜流し開始のラベルは仕分け完了');
  ev(2, 'line_start');
  const run = listLineRuns(2).find((r) => r.phase === 'run');
  eq(run.planned_count, 47, '流しの予定=仕分け後の最終通過件数 (申し送り)');
  eq(packBatchNotionState(bMid, { kind: 'melt', sortDone: true, runStarted: true, finalCount: null }).label,
    STATUS_PACKING, '流し開始後のラベルは梱包作業中');
  ev(2, 'line_done', { finalCount: 47, manualCount: 0 });
  const bDone = db.prepare('SELECT * FROM pk_pack_batches WHERE id=2').get();
  eq(bDone.status, 'done', 'MELT 完了');
  const st = packBatchNotionState(bDone, { kind: 'melt', sortDone: true, runStarted: true, finalCount: 47 });
  eq(st.label, STATUS_PACK_DONE, '完了ラベル');
  eq(st.times.lineCount, 47, '秒/伝票の分母は実際に流した件数');
}

console.log('\n── replay / cancel ──');
{
  // 同一 op_id の再送は保存結果を返す (line 系 payload も比較対象)
  const opId = `t${++op}`;
  mkBatch(4, 1, 30);
  applyEvent(4, { opId, event: 'line_start' }, '倉田');
  const replayed = applyEvent(4, { opId, event: 'line_start' }, '倉田');
  ok(replayed.replayed === true, '同一 op_id は replay');
  throws(() => applyEvent(4, { opId, event: 'line_done', finalCount: 30 }, '倉田'), 'op_conflict',
    '同一 op_id の別内容は409');
  // cancel でライン工程も初期化
  ev(4, 'cancel');
  eq(listLineRuns(4).length, 0, 'cancel で line_runs も初期化');
  eq(db.prepare('SELECT status FROM pk_pack_batches WHERE id=4').get().status, 'ready', 'バッチは未着手へ');
}

console.log('\n── 伝票イベントの相互排他 / 段階的取消 ──');
{
  mkBatch(5, 2, 40);   // MELT
  throws(() => ev(5, 'start'), 'line_batch', '梱包機バッチに伝票 start は不可');
  throws(() => ev(5, 'shortage', { slipSeq: 1, sku: 'x', qty: 1 }), 'line_batch', '不足イベントも不可 (再オープン経路を塞ぐ)');
  ev(5, 'line_sort_start');
  throws(() => ev(5, 'line_sort_done', {}), 'bad_count', '件数未入力は拒否 (0扱いしない)');
  ev(5, 'line_sort_done', { finalCount: 38 });
  ev(5, 'line_start');
  throws(() => ev(5, 'line_done', {}), 'bad_count', '完了件数未入力は拒否');
  ev(5, 'line_done', { finalCount: 38 });
  eq(db.prepare('SELECT status FROM pk_pack_batches WHERE id=5').get().status, 'done', 'MELT 完了');
  // 段階的取消: 流し終了→流し開始→仕分け完了→仕分け開始 の順に1段ずつ戻る
  throws(() => ev(5, 'undo', {}), 'bad_reason', 'undo は理由必須');
  ev(5, 'undo', { reason: '誤タップ' });
  eq(db.prepare('SELECT status FROM pk_pack_batches WHERE id=5').get().status, 'packing', '完了取消でバッチは作業中へ');
  let run = listLineRuns(5).find((r) => r.phase === 'run');
  ok(run && !run.finished_at, '流し終了が取り消される (開始は残る)');
  ev(5, 'undo', { reason: '誤タップ' });
  ok(!listLineRuns(5).find((r) => r.phase === 'run'), '2回目で流し開始も取消');
  ev(5, 'undo', { reason: '誤タップ' });
  const sort = listLineRuns(5).find((r) => r.phase === 'sort');
  ok(sort && !sort.finished_at && sort.final_count == null, '3回目で仕分け完了を取消');
  ev(5, 'undo', { reason: '誤タップ' });
  eq(listLineRuns(5).length, 0, '4回目で仕分け開始も取消');
  const b5 = db.prepare('SELECT status, worker, started_at FROM pk_pack_batches WHERE id=5').get();
  eq([b5.status, b5.worker, b5.started_at], ['ready', null, null],
    '最初の工程まで取り消すとバッチは未着手へ (誤開始時刻を残さない)');
  throws(() => ev(5, 'undo', { reason: '誤タップ' }), 'not_packing', '未着手へ戻った後の取消は状態ガードで拒否');

  // 中断中はライン操作不可 (pause_* が残ったまま done になる矛盾を塞ぐ)
  mkBatch(6, 1, 10);   // PAS
  ev(6, 'line_start');
  ev(6, 'pause', { reason: '休憩' });
  throws(() => ev(6, 'line_done', { finalCount: 10 }), 'not_packing', '中断中の終了は拒否');
  throws(() => ev(6, 'line_start'), 'already_started', '中断中の再開始も進まない');
  ev(6, 'resume');
  ev(6, 'line_done', { finalCount: 10 });
  eq(db.prepare('SELECT status FROM pk_pack_batches WHERE id=6').get().status, 'done', '再開後は完了できる');
}

db.close();
try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* 後始末失敗は無視 */ }
console.log(`\n${failed === 0 ? '✅ 全テスト PASS' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
