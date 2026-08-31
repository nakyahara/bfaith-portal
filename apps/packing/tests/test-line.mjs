/**
 * test-line.mjs — 梱包機ライン管理 (PAS/MELT — 紙台帳の置き換え) の検証
 *
 * v2 (2026-08-18 実機フィードバック): 終了=時計停止→件数入力の2段階 (line_stop)・
 * MELT仕分け=除外件数入力→流し件数自動計算・本日累計 (lineDailyTotal)
 *
 * 実行: node apps/packing/tests/test-line.mjs
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'packing-line-test-'));
process.env.DATA_DIR = tmpDir;
delete process.env.PACKING_NOTION_TOKEN;
delete process.env.PICKING_NOTION_TOKEN;

const { initPackingDB, getDB, utcNow } = await import('../db.js');
const { applyEvent, lineKindOf, listLineRuns, lineDailyTotal, resolveIncident, getWorkState, PackError } = await import('../service.js');
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

console.log('\n── PAS: 流し開始→停止→件数入力 (2段階) ──');
{
  throws(() => ev(3, 'line_start'), 'not_line_batch', '手梱包バッチにはライン操作不可');
  throws(() => ev(1, 'line_sort_start'), 'not_melt', 'PAS に仕分け工程はない');
  ev(1, 'line_start');
  const b = db.prepare('SELECT * FROM pk_pack_batches WHERE id=1').get();
  eq([b.status, b.worker], ['packing', '倉田'], '流し開始でバッチは作業中+担当者');
  eq(listLineRuns(1).find((r) => r.phase === 'run').planned_count, 100, 'PAS の予定件数=伝票数');
  throws(() => ev(1, 'line_start'), 'already_started', '二重開始は拒否');
  throws(() => ev(1, 'line_done', { finalCount: 99 }), 'not_stopped', '停止前の件数記録は拒否 (先に時間を止める)');
  ev(1, 'line_stop');
  const stopped = listLineRuns(1).find((r) => r.phase === 'run');
  ok(stopped.finished_at && stopped.final_count == null, '停止=時刻のみ確定 (件数は未記録)');
  eq(db.prepare('SELECT status FROM pk_pack_batches WHERE id=1').get().status, 'packing',
    '件数を入力するまでバッチは完了しない');
  throws(() => ev(1, 'line_stop'), 'already_done', '二重停止は拒否');
  throws(() => ev(1, 'line_done', {}), 'bad_count', '完了件数未入力は拒否 (0扱いしない)');
  throws(() => ev(1, 'line_done', { finalCount: 101 }), 'bad_count', '完了件数>予定は拒否');
  throws(() => ev(1, 'line_done', { finalCount: 99, manualCount: 100 }), 'bad_count', '手動>完了は拒否');
  throws(() => ev(1, 'line_done', { finalCount: 99 }, '別人'), 'taken', '担当者以外は操作不可');
  ev(1, 'line_done', { finalCount: 99, manualCount: 2, note: '1件ピッキングミス' });
  const b2 = db.prepare('SELECT * FROM pk_pack_batches WHERE id=1').get();
  eq(b2.status, 'done', '件数記録でバッチ完了');
  const run2 = listLineRuns(1).find((r) => r.phase === 'run');
  eq([run2.final_count, run2.manual_count, run2.note], [99, 2, '1件ピッキングミス'], '件数・手動・備考を記録');
  eq(b2.finished_at, run2.finished_at, 'バッチ終了時刻=停止時刻 (件数入力の時間は含めない)');
  throws(() => ev(1, 'line_done', { finalCount: 99 }), 'already_done', '記録済みの再記録は拒否');
}

console.log('\n── MELT: 仕分け (除外件数方式) →流し ──');
{
  throws(() => ev(2, 'line_start'), 'sort_first', '仕分け前の流し開始は拒否');
  ev(2, 'line_sort_start');
  throws(() => ev(2, 'line_sort_start'), 'already_started', '仕分けの二重開始は拒否');
  throws(() => ev(2, 'line_sort_done', {}), 'bad_count', '除外件数未入力は拒否');
  throws(() => ev(2, 'line_sort_done', { excludedCount: 51 }), 'bad_count', '除外>伝票数は拒否');
  ev(2, 'line_sort_done', { excludedCount: 3, note: '3件を手動へ変更' });
  const sort = listLineRuns(2).find((r) => r.phase === 'sort');
  eq([sort.planned_count, sort.excluded_count, sort.final_count], [50, 3, 47],
    '機械に流す件数=伝票50−除外3=47 を自動計算');
  const bMid = db.prepare('SELECT * FROM pk_pack_batches WHERE id=2').get();
  eq(packBatchNotionState(bMid, { kind: 'melt', sortDone: true, runStarted: false, finalCount: null }).label,
    STATUS_SORTED, '仕分け完了〜流し開始のラベルは仕分け完了');
  ev(2, 'line_start');
  eq(listLineRuns(2).find((r) => r.phase === 'run').planned_count, 47, '流しの予定=自動計算した47 (申し送り)');
  eq(packBatchNotionState(bMid, { kind: 'melt', sortDone: true, runStarted: true, finalCount: null }).label,
    STATUS_PACKING, '流し開始後のラベルは梱包作業中');
  ev(2, 'line_stop');
  ev(2, 'line_done', { finalCount: 47, manualCount: 0 });
  const bDone = db.prepare('SELECT * FROM pk_pack_batches WHERE id=2').get();
  eq(bDone.status, 'done', 'MELT 完了');
  const st = packBatchNotionState(bDone, { kind: 'melt', sortDone: true, runStarted: true, finalCount: 47 });
  eq([st.label, st.times.lineCount], [STATUS_PACK_DONE, 47], '完了ラベル+秒/伝票分母=実際に流した件数');
}

console.log('\n── 本日累計 (lineDailyTotal — 日付でリセット) ──');
{
  eq(lineDailyTotal('2026-08-18', 'pas'), { total: 99, machine: 97, transferredIn: 0 },
    'PAS累計: 出荷99 (うち手動2) → 機械通過97');
  eq(lineDailyTotal('2026-08-18', 'melt'), { total: 47, machine: 47, transferredIn: 0 }, 'MELT累計は別集計');
  eq(lineDailyTotal('2026-08-19', 'pas'), { total: 0, machine: 0, transferredIn: 0 }, '翌日は0にリセット');
}

console.log('\n── 伝票イベントの相互排他 / 段階的取消 ──');
{
  mkBatch(5, 2, 40);   // MELT
  throws(() => ev(5, 'start'), 'line_batch', '梱包機バッチに伝票 start は不可');
  throws(() => ev(5, 'next', { slipSeq: 1 }), 'line_batch', '伝票の完了 (next) も不可');
  throws(() => ev(5, 'jump', { slipSeq: 1 }), 'line_batch', 'ジャンプも不可');
  throws(() => ev(5, 'excess', { sku: 'x', qty: 1 }), 'line_batch', '余り (棚戻し) は対象外');
  // 依頼系 (不足/配送変更/再印刷) はラインからも出せる (2026-08-31)。伝票が無いバッチなので伝票検証まで進む
  throws(() => ev(5, 'shortage', { slipSeq: 1, sku: 'x', qty: 1 }), 'sku_not_in_slip', '不足はライン制限では弾かれない (伝票検証へ進む)');
  ev(5, 'line_sort_start');
  ev(5, 'line_sort_done', { excludedCount: 2 });
  ev(5, 'line_start');
  ev(5, 'line_stop');
  ev(5, 'line_done', { finalCount: 38 });
  eq(db.prepare('SELECT status FROM pk_pack_batches WHERE id=5').get().status, 'done', 'MELT 完了');
  // 段階的取消: 件数→停止→流し開始→仕分け完了→仕分け開始 の順に1段ずつ
  throws(() => ev(5, 'undo', {}), 'bad_reason', 'undo は理由必須');
  ev(5, 'undo', { reason: '誤タップ' });
  eq(db.prepare('SELECT status FROM pk_pack_batches WHERE id=5').get().status, 'packing', '①件数取消で作業中へ');
  let run = listLineRuns(5).find((r) => r.phase === 'run');
  ok(run && run.finished_at && run.final_count == null, '①停止時刻は保持 (入力画面に戻る)');
  ev(5, 'undo', { reason: '誤タップ' });
  run = listLineRuns(5).find((r) => r.phase === 'run');
  ok(run && !run.finished_at, '②停止を取消 (タイマー再開)');
  ev(5, 'undo', { reason: '誤タップ' });
  ok(!listLineRuns(5).find((r) => r.phase === 'run'), '③流し開始を取消');
  ev(5, 'undo', { reason: '誤タップ' });
  const sort = listLineRuns(5).find((r) => r.phase === 'sort');
  ok(sort && !sort.finished_at && sort.excluded_count == null, '④仕分け完了を取消 (除外件数もクリア)');
  ev(5, 'undo', { reason: '誤タップ' });
  eq(listLineRuns(5).length, 0, '⑤仕分け開始も取消');
  const b5 = db.prepare('SELECT status, worker, started_at FROM pk_pack_batches WHERE id=5').get();
  eq([b5.status, b5.worker, b5.started_at], ['ready', null, null], '最初まで取り消すと未着手へ');
  throws(() => ev(5, 'undo', { reason: '誤タップ' }), 'not_packing', '未着手へ戻った後は状態ガードで拒否');

  // 中断中はライン操作不可
  mkBatch(6, 1, 10);   // PAS
  ev(6, 'line_start');
  ev(6, 'pause', { reason: '休憩' });
  throws(() => ev(6, 'line_stop'), 'not_packing', '中断中の停止は拒否');
  ev(6, 'resume');
  ev(6, 'line_stop');
  ev(6, 'line_done', { finalCount: 10 });
  eq(db.prepare('SELECT status FROM pk_pack_batches WHERE id=6').get().status, 'done', '再開後は完了できる');
  eq(lineDailyTotal('2026-08-18', 'pas'), { total: 109, machine: 107, transferredIn: 0 }, '累計にバッチ6の10件が加算');
}

console.log('\n── 仕分けと流しの担当者分離 (中原さん指示 2026-08-20) ──');
{
  mkBatch(8, 2, 20);   // MELT
  ev(8, 'line_sort_start', {}, '仕分A');
  ev(8, 'line_sort_done', { excludedCount: 0 }, '仕分A');
  // 流しは別人B: 工程の開始時は担当交代を許可 (作業者欄で自分を選んで開始するだけ)
  ev(8, 'line_start', {}, '流しB');
  eq(db.prepare('SELECT worker FROM pk_pack_batches WHERE id=8').get().worker, '流しB', '流し開始で現在の担当がBへ');
  eq(listLineRuns(8).find((r) => r.phase === 'sort').worker, '仕分A', '仕分け担当Aは工程別に残る');
  // 工程の途中操作はその工程の担当者のみ (誤操作防止は維持・判定は工程行のworker)
  throws(() => ev(8, 'line_stop', {}, '仕分A'), 'taken', '停止は流し担当B以外は不可');
  // 途中の実引き継ぎは明示の takeover — 進行中の工程行の担当も引き継ぐ (Codex high)
  ev(8, 'takeover', {}, '交代C');
  eq(listLineRuns(8).find((r) => r.phase === 'run').worker, '交代C', 'takeoverで流し工程の担当もCへ');
  eq(listLineRuns(8).find((r) => r.phase === 'sort').worker, '仕分A', '完了済みの仕分け担当記録は変えない');
  throws(() => ev(8, 'line_stop', {}, '流しB'), 'taken', '交代後は旧担当Bは操作不可');
  ev(8, 'line_stop', {}, '交代C');
  ev(8, 'line_done', { finalCount: 20 }, '交代C');
  eq(listLineRuns(8).find((r) => r.phase === 'run').worker, '交代C', '最終の流し担当Cが工程に記録される');
  eq(db.prepare('SELECT status FROM pk_pack_batches WHERE id=8').get().status, 'done', '完了');
  // 工程開始での交代はイベントpayloadに監査記録 (switchedFrom)
  const startEv = db.prepare(
    "SELECT payload_json FROM pk_pack_events WHERE batch_id=8 AND event='line_start'").get();
  eq(JSON.parse(startEv.payload_json).switchedFrom, '仕分A', 'line_startのpayloadに旧担当を記録');

  // 工程行だけ担当が食い違う不整合は、本人の takeover で修復できる (Codex medium)
  mkBatch(9, 2, 10);
  ev(9, 'line_sort_start', {}, '担当D');
  db.prepare("UPDATE pk_pack_line_runs SET worker='ズレE' WHERE batch_id=9 AND phase='sort'").run();
  throws(() => ev(9, 'line_sort_done', { excludedCount: 0 }, '担当D'), 'taken', '不整合状態では操作不可');
  ev(9, 'takeover', {}, '担当D');   // batch.worker は既にD (変更なし) でも工程行を修復
  ev(9, 'line_sort_done', { excludedCount: 0 }, '担当D');
  ok(true, '本人takeoverで工程行の担当を修復→操作再開');

  // 手梱包バッチの担当ガードは従来どおり (非回帰)
  ev(3, 'start', {}, '手梱A');
  throws(() => ev(3, 'start', {}, '手梱B'), 'taken', '手梱包の別人startは拒否のまま');
  ev(3, 'takeover', {}, '手梱B');
  eq(db.prepare('SELECT worker FROM pk_pack_batches WHERE id=3').get().worker, '手梱B', '手梱包の交代はtakeoverで');
}

console.log('\n[完了後の担当修正 (2026-08-21)]');
{
  // ライン: batch 8 は完了済み (流し=交代C)。完了後の takeover で流し担当を修正できる
  ev(8, 'takeover', {}, '修正D');
  eq(db.prepare('SELECT worker FROM pk_pack_batches WHERE id=8').get().worker, '修正D', '完了後もbatch担当を修正できる');
  eq(listLineRuns(8).find((r) => r.phase === 'run').worker, '修正D', '流し担当も修正される');
  eq(listLineRuns(8).find((r) => r.phase === 'sort').worker, '仕分A', '仕分け担当は変えない');
  // 手梱包: batch 3 を done にして修正
  db.prepare("UPDATE pk_pack_batches SET status='done' WHERE id=3").run();
  ev(3, 'takeover', {}, '手梱C');
  eq(db.prepare('SELECT worker FROM pk_pack_batches WHERE id=3').get().worker, '手梱C', '手梱包も完了後に修正できる');
}

console.log('\n── replay ──');
{
  const opId = `t${++op}`;
  mkBatch(7, 1, 30);
  applyEvent(7, { opId, event: 'line_start' }, '倉田');
  ok(applyEvent(7, { opId, event: 'line_start' }, '倉田').replayed === true, '同一 op_id は replay');
  throws(() => applyEvent(7, { opId, event: 'line_stop' }, '倉田'), 'op_conflict', '同一 op_id の別内容は409');
  ev(7, 'cancel');
  eq(listLineRuns(7).length, 0, 'cancel で line_runs も初期化');
}

console.log('\n── 一時中断 (2026-08-25 現場意見: 資材交換で止めたいのに「終了」しかなかった) ──');
{
  mkBatch(10, 1, 40);   // PAS
  throws(() => ev(10, 'pause', { reason: '資材の交換' }), 'not_packing', '未開始の中断は拒否');
  ev(10, 'line_start');
  ev(10, 'pause', { reason: '資材の交換' });
  let b = db.prepare('SELECT * FROM pk_pack_batches WHERE id=10').get();
  eq([b.status, b.pause_reason], ['paused', '資材の交換'], '中断でバッチは paused + 理由');
  throws(() => ev(10, 'line_stop'), 'not_packing', '中断中の「流し終了」は拒否 (作業終了にならない)');
  throws(() => ev(10, 'line_done', { finalCount: 40 }), 'not_stopped', '中断中の件数記録も拒否');
  throws(() => ev(10, 'resume', {}, '別人'), 'taken', '別人の再開は拒否');
  // 中断開始を2分前に巻き戻して再開 → 工程行に中断秒が積まれる
  db.prepare("UPDATE pk_pack_batches SET pause_started_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '-120 seconds') WHERE id=10").run();
  ev(10, 'resume');
  b = db.prepare('SELECT * FROM pk_pack_batches WHERE id=10').get();
  const run = listLineRuns(10).find((r) => r.phase === 'run');
  eq([b.status, b.pause_started_at], ['packing', null], '再開で packing に戻る');
  ok(b.paused_total_sec >= 119 && b.paused_total_sec <= 122, `バッチの中断秒 (${b.paused_total_sec})`);
  ok(run.paused_total_sec >= 119 && run.paused_total_sec <= 122 && run.finished_at == null, `工程行 run にも中断秒 (${run.paused_total_sec})・停止はされていない`);
  ev(10, 'line_stop');
  throws(() => ev(10, 'pause', { reason: '休憩' }), 'no_phase', '停止後 (件数入力待ち) の中断は拒否');
  ev(10, 'line_done', { finalCount: 40, manualCount: 0 });
  eq(db.prepare('SELECT status FROM pk_pack_batches WHERE id=10').get().status, 'done', '件数記録で完了');
  // 停止の取消 (undo) で中断秒は保持される
  ev(10, 'undo', { reason: '誤タップ' });
  ev(10, 'undo', { reason: '誤タップ' });
  const run2 = listLineRuns(10).find((r) => r.phase === 'run');
  ok(run2.finished_at == null && run2.paused_total_sec === run.paused_total_sec, '停止取消後も工程行の中断秒は保持');
}

console.log('\n── MELT 仕分け: PAS-LINE へ移した件数を PAS の本日累計へ (2026-08-31 現場意見) ──');
{
  mkBatch(20, 2, 30);   // MELT (2026-08-18)
  const before = lineDailyTotal('2026-08-18', 'pas');
  ev(20, 'line_sort_start');
  throws(() => ev(20, 'line_sort_done', { excludedCount: 2, toPasCount: 3 }), 'bad_count', 'PAS へ移す件数 > 除外件数は拒否 (内数)');
  throws(() => ev(20, 'line_sort_done', { excludedCount: 2, toPasCount: -1 }), 'bad_count', '負数は拒否');
  ev(20, 'line_sort_done', { excludedCount: 3, toPasCount: 1 });
  const sort = listLineRuns(20).find((r) => r.phase === 'sort');
  eq([sort.excluded_count, sort.to_pas_count, sort.final_count], [3, 1, 27], '除外3 (うちPAS 1) → 機械に流す 27');
  const after = lineDailyTotal('2026-08-18', 'pas');
  eq([after.transferredIn - before.transferredIn, after.machine - before.machine, after.total - before.total], [1, 1, 1],
    'PAS の累計 (出荷・機械通過) に MELT から移した 1 件が乗る');
  eq(lineDailyTotal('2026-08-18', 'melt').transferredIn, 0, 'MELT 側の累計には乗らない');
  // 未指定 (旧画面) は 0 扱い
  mkBatch(21, 2, 10);
  ev(21, 'line_sort_start');
  ev(21, 'line_sort_done', { excludedCount: 1 });
  eq(listLineRuns(21).find((r) => r.phase === 'sort').to_pas_count, 0, 'to_pas_count 未指定は 0');
  // 取消で内訳も消える
  ev(20, 'undo', { reason: '誤タップ' });
  const undone = listLineRuns(20).find((r) => r.phase === 'sort');
  eq([undone.finished_at, undone.excluded_count, undone.to_pas_count], [null, null, null], '仕分け完了の取消で to_pas_count も戻る');
  eq(lineDailyTotal('2026-08-18', 'pas').transferredIn, before.transferredIn, '取消後は PAS 累計から消える');
  // 同一 op_id の replay は同じ内容なら通る・内訳違いは op_conflict
  const opId = `t${++op}`;
  applyEvent(20, { opId, event: 'line_sort_done', excludedCount: 2, toPasCount: 2 }, '倉田');
  ok(applyEvent(20, { opId, event: 'line_sort_done', excludedCount: 2, toPasCount: 2 }, '倉田').replayed, '同一内容の再送は replay');
  throws(() => applyEvent(20, { opId, event: 'line_sort_done', excludedCount: 2, toPasCount: 1 }, '倉田'), 'op_conflict', '内訳が違う再送は op_conflict');
}

console.log('\n── ライン (紙作業) からの伝票ごとの依頼: 再ピック・配送変更・再印刷 (2026-08-31) ──');
{
  // PAS バッチに伝票を持たせる (実運用では CS03003 取込で入る)
  mkBatch(30, 1, 3);
  const insSlip = db.prepare(`INSERT INTO pk_pack_slips
    (batch_id, seq, ne_slip_no, slip_no, recipient_name, site_order_no, status, delivery_method)
    VALUES (30, ?, ?, ?, ?, ?, 'pending', 'ネコポス')`);
  const insLine = db.prepare('INSERT INTO pk_pack_lines (slip_id, sku, product_name, qty) VALUES (?, ?, ?, ?)');
  for (let i = 1; i <= 3; i++) {
    const sid = Number(insSlip.run(i, `NE-30-${i}`, `SP30-${i}`, `客${i}`, `SO-${i}`).lastInsertRowid);
    insLine.run(sid, 'sku-a', '商品A', 2);
    if (i === 2) insLine.run(sid, 'sku-b', '商品B', 1);
  }
  // 未開始 (ready) のラインバッチでも依頼は出せる・工程担当と別人でも出せる
  ev(30, 'line_start', {}, '流し担当');
  const st0 = getWorkState(30);
  eq(st0.slips.length, 3, '伝票3件');
  ev(30, 'shortage', { slipSeq: 2, sku: 'sku-b', qty: 1 }, '仕分け担当');
  const st1 = getWorkState(30);
  eq([st1.slips[1].status, st1.slips[1].hold_reason], ['held', 'repick'], '不足 → 伝票2は保留 (repick)');
  eq(st1.incidents.length, 1, '不足の候補が1件');
  eq(db.prepare('SELECT status FROM pk_pack_batches WHERE id=30').get().status, 'packing', 'ラインの工程状態は変わらない');
  // ラインは終了画面が無い → 伝票が pending のままでも即送信できる。担当者一致も課さない
  const inc = resolveIncident(st1.incidents[0].id, 'confirm', '仕分け担当', 30);
  eq(inc.dispatchedTasks.map((x) => x.kind), ['repick'], '確定で再ピックタスクが出る (伝票未完了でも可)');
  const task = db.prepare("SELECT * FROM pk_pack_tasks WHERE batch_id=30 AND slip_seq=2").get();
  eq([task.kind, task.sku, task.req_qty, task.status], ['repick', 'sku-b', 1, 'requested'], 'タスク内容');
  eq(getWorkState(30).repickBySlip[2], 'requested', 'ライン画面の「進行中の依頼」= 依頼中');
  // 受領はピッカーが fulfilled にしてから
  throws(() => ev(30, 'receive', { slipSeq: 2 }, '流し担当'), 'repick_not_ready', '未完了の再ピックは受領できない');
  db.prepare("UPDATE pk_pack_tasks SET status='fulfilled' WHERE id=?").run(task.id);
  eq(getWorkState(30).repickBySlip[2], 'fulfilled', '完了 → 受取可');
  ev(30, 'receive', { slipSeq: 2 }, '流し担当');
  eq(getWorkState(30).slips[1].status, 'pending', '受領で保留解除');
  eq(db.prepare('SELECT status FROM pk_pack_tasks WHERE id=?').get(task.id).status, 'received', 'タスクは received');
  // 見つかった (取下げ): 候補ごと取り下げ・保留解除
  ev(30, 'shortage', { slipSeq: 1, sku: 'sku-a', qty: 2 }, '流し担当');
  ev(30, 'found', { slipSeq: 1 }, '流し担当');
  eq(getWorkState(30).slips[0].status, 'pending', '見つかった → 保留解除');
  eq(db.prepare("SELECT status FROM pk_pack_incidents WHERE batch_id=30 AND slip_seq=1").get().status, 'withdrawn', '候補は取下げ');
  // 配送変更・再印刷 (伝票状態は変えない・記録だけ)
  ev(30, 'ship_change', { slipSeq: 3, proposedMethod: '宅急便60サイズ', reason: '入らない' }, '仕分け担当');
  const sc = db.prepare('SELECT * FROM pk_pack_ship_changes WHERE batch_id=30').get();
  eq([sc.slip_seq, sc.current_method, sc.proposed_method, sc.reason, sc.status], [3, 'ネコポス', '宅急便60サイズ', '入らない', 'requested'], '配送変更の依頼が記録される');
  const rp = ev(30, 'reprint', { slipSeq: 3 }, '仕分け担当');
  ok(rp.reprintId > 0, '再印刷依頼が記録される');
  eq(getWorkState(30).slips[2].status, 'pending', '配送変更・再印刷で伝票状態は変わらない');
  // 完了済みラインバッチへの不足は「再オープン」しない (バッチ完了は件数記録で決まる)
  ev(30, 'line_stop', {}, '流し担当');
  ev(30, 'line_done', { finalCount: 3 }, '流し担当');
  eq(db.prepare('SELECT status FROM pk_pack_batches WHERE id=30').get().status, 'done', '件数記録で完了');
  ev(30, 'shortage', { slipSeq: 3, sku: 'sku-a', qty: 1 }, '流し担当');
  eq(db.prepare('SELECT status FROM pk_pack_batches WHERE id=30').get().status, 'done', '完了後の不足依頼でもラインバッチは done のまま');
  eq(getWorkState(30).slips[2].status, 'held', '伝票は保留になる (再ピック依頼は出せる)');
  // 伝票の完了 (next) は引き続き不可
  throws(() => ev(30, 'next', { slipSeq: 1 }, '流し担当'), 'line_batch', 'ラインでは next は不可のまま');
}

console.log('\n── ライン: 同じ伝票で複数SKUが不足 (Codex R1 High) / 緩和範囲の限定 ──');
{
  mkBatch(31, 1, 1);
  const sid = Number(db.prepare(`INSERT INTO pk_pack_slips
    (batch_id, seq, ne_slip_no, slip_no, recipient_name, site_order_no, status, delivery_method)
    VALUES (31, 1, 'NE-31-1', 'SP31-1', '客', 'SO', 'pending', 'ネコポス')`).run().lastInsertRowid);
  const insLine = db.prepare('INSERT INTO pk_pack_lines (slip_id, sku, product_name, qty) VALUES (?, ?, ?, ?)');
  insLine.run(sid, 'sku-a', '商品A', 1);
  insLine.run(sid, 'sku-b', '商品B', 2);
  ev(31, 'line_start');
  ev(31, 'shortage', { slipSeq: 1, sku: 'sku-a', qty: 1 });
  const incA = getWorkState(31).incidents[0];
  resolveIncident(incA.id, 'confirm', '倉田', 31);
  eq(getWorkState(31).slips[0].status, 'held', '1品目の依頼で保留');
  ev(31, 'shortage', { slipSeq: 1, sku: 'sku-b', qty: 2 });   // 保留中でも 2品目を出せる
  const incB = getWorkState(31).incidents.find((i) => i.sku === 'sku-b');
  ok(incB, '保留中の伝票にも 2品目の不足を記録できる');
  resolveIncident(incB.id, 'confirm', '倉田', 31);
  eq(db.prepare("SELECT COUNT(*) c FROM pk_pack_tasks WHERE batch_id=31 AND slip_seq=1 AND status='requested'").get().c, 2, '再ピックタスクが 2 件');
  throws(() => ev(31, 'shortage', { slipSeq: 1, sku: 'sku-b', qty: 1 }), 'dup_task', '同じ伝票×SKU の二重依頼は拒否');
  eq(getWorkState(31).repickBySlip[1], 'requested', '受取可は全タスク完了まで出ない');
  db.prepare("UPDATE pk_pack_tasks SET status='fulfilled' WHERE batch_id=31").run();
  ev(31, 'receive', { slipSeq: 1 });
  eq(getWorkState(31).slips[0].status, 'pending', '2件とも完了 → 受領で保留解除');
  // Codex R2: SKU 単位の「見つかった」と、未送信候補が残る間の受領禁止
  ev(31, 'shortage', { slipSeq: 1, sku: 'sku-a', qty: 1 });
  const ia = getWorkState(31).incidents.find((i) => i.sku === 'sku-a');
  resolveIncident(ia.id, 'confirm', '倉田', 31);                       // sku-a は依頼済み (タスク)
  ev(31, 'shortage', { slipSeq: 1, sku: 'sku-b', qty: 1 });           // sku-b は候補のまま (送信失敗を想定)
  db.prepare("UPDATE pk_pack_tasks SET status='fulfilled' WHERE batch_id=31 AND status='requested'").run();
  throws(() => ev(31, 'receive', { slipSeq: 1 }), 'repick_not_ready', '未送信の候補 (sku-b) が残る間は受領できない');
  ev(31, 'found', { slipSeq: 1, sku: 'sku-a' });                        // 届いた sku-a ではなく…でも取下げ可 (見つかった)
  eq(getWorkState(31).slips[0].status, 'held', 'SKU 単位の見つかった: 他の商品 (sku-b 候補) が残るので保留のまま');
  eq(db.prepare("SELECT status FROM pk_pack_tasks WHERE batch_id=31 AND sku='sku-a' ORDER BY id DESC LIMIT 1").get().status, 'cancelled', 'sku-a のタスクだけ取消');
  eq(getWorkState(31).incidents.map((i) => i.sku), ['sku-b'], 'sku-b の候補は残る');
  ev(31, 'found', { slipSeq: 1, sku: 'sku-b' });
  eq(getWorkState(31).slips[0].status, 'pending', '最後の商品も見つかった → 保留解除');
  eq(getWorkState(31).incidents.length, 0, '候補は全部取下げ');
  // sku 指定なしの found は従来どおり伝票の全依頼を取り下げる
  ev(31, 'shortage', { slipSeq: 1, sku: 'sku-a', qty: 1 });
  ev(31, 'shortage', { slipSeq: 1, sku: 'sku-b', qty: 1 });
  ev(31, 'found', { slipSeq: 1 });
  eq([getWorkState(31).slips[0].status, getWorkState(31).incidents.length], ['pending', 0], 'sku なしの found = 全部見つかった');
  // 手梱包バッチでは従来どおり保留中に不足は記録できない (挙動不変)
  mkBatch(32, 3, 1);
  const sid2 = Number(db.prepare(`INSERT INTO pk_pack_slips
    (batch_id, seq, ne_slip_no, slip_no, recipient_name, site_order_no, status, delivery_method)
    VALUES (32, 1, 'NE-32-1', 'SP32-1', '客', 'SO', 'pending', '箱')`).run().lastInsertRowid);
  insLine.run(sid2, 'sku-a', '商品A', 1);
  insLine.run(sid2, 'sku-b', '商品B', 1);
  ev(32, 'start');
  ev(32, 'shortage', { slipSeq: 1, sku: 'sku-a', qty: 1 });
  throws(() => ev(32, 'shortage', { slipSeq: 1, sku: 'sku-b', qty: 1 }), 'not_pending', '手梱包は保留中の伝票に追加の不足を記録しない (従来どおり)');
  // 緩和は不足候補だけ: 余り (excess) の候補はラインでも従来ルール (担当者一致・一通り終えてから)
  db.prepare(`INSERT INTO pk_pack_incidents (batch_id, slip_seq, kind, sku, qty, status, detected_by, created_at, updated_at)
    VALUES (31, NULL, 'excess', 'sku-z', 1, 'candidate', '倉田', ?, ?)`).run(now, now);
  const exc = db.prepare("SELECT id FROM pk_pack_incidents WHERE batch_id=31 AND kind='excess'").get();
  throws(() => resolveIncident(exc.id, 'confirm', '別人', 31), 'taken', '余りの候補は担当者一致が要る (緩和対象外)');
  throws(() => resolveIncident(exc.id, 'confirm', '倉田', 31), 'batch_not_done', '余りの候補は一通り終えてから (緩和対象外)');
  // already_resolved は現在の status を同梱 (再送の成功/競合の判別用 — Codex R5)
  try { resolveIncident(incA.id, 'confirm', '倉田', 31); ok(false, 'already_resolved にならなかった'); }
  catch (e) { eq([e.code, e.body?.status], ['already_resolved', 'confirmed'], 'already_resolved に status=confirmed が付く'); }
}

console.log('\n── line.ejs の描画 + インラインJSの構文 (2026-08-25 事故: confirm文字列に改行が混入し全ボタン無反応) ──');
{
  const ejs = (await import('ejs')).default;
  const vm = await import('node:vm');
  const src = fs.readFileSync(new URL('../views/line.ejs', import.meta.url), 'utf8');
  const base = { title: 't', workers: [{ name: 'A' }], statusLabels: {}, dailyTotal: { total: 0, machine: 0 },
    hikiateClass: 'x', pauseReasons: ['資材の交換', '休憩', 'その他'], floorAlerts: [] };
  const cases = [
    ['MELT ready', { kind: 'melt', batch: { id: 1, status: 'ready', slip_count: 3 }, runs: [] }],
    ['PAS running', { kind: 'pas', batch: { id: 1, status: 'packing', slip_count: 3 }, runs: [{ phase: 'run', started_at: now, paused_total_sec: 0 }] }],
    ['PAS paused', { kind: 'pas', batch: { id: 1, status: 'paused', slip_count: 3, pause_reason: '資材の交換', pause_started_at: now }, runs: [{ phase: 'run', started_at: now, paused_total_sec: 30 }] }],
    ['PAS stopped', { kind: 'pas', batch: { id: 1, status: 'packing', slip_count: 3 }, runs: [{ phase: 'run', started_at: now, finished_at: now, paused_total_sec: 0, planned_count: 3 }] }],
    ['MELT sorting (2026-08-31: PAS振替入力+依頼カード)', { kind: 'melt', batch: { id: 1, status: 'packing', slip_count: 3 }, runs: [{ phase: 'sort', started_at: now, paused_total_sec: 0, planned_count: 3 }],
      slips: [
        { seq: 1, neSlipNo: '1', slipNo: 'SP1', siteOrderNo: 'SO1', recipientName: '山田 <太郎>', deliveryMethod: 'ネコポス', status: 'held', holdReason: 'repick', lines: [{ sku: 'a', name: '商品"A"', qty: 2 }] },
        { seq: 2, neSlipNo: '2', slipNo: 'SP2', siteOrderNo: null, recipientName: "O'Brien", deliveryMethod: null, status: 'pending', holdReason: null, lines: [] },
      ],
      incidents: [{ id: 9, slipSeq: 1, kind: 'shortage', sku: 'a', qty: 1 }], repickBySlip: { 1: 'fulfilled' },
      tasks: [{ id: 5, slipSeq: 1, sku: 'a', qty: 1, status: 'fulfilled' }, { id: 6, slipSeq: 1, sku: "b'x", qty: 1, status: 'requested' }],
      methodOptions: ['定形外', 'ネコポス'], shipChangeReasons: ['入らない', 'その他'] }],
    ['PAS with transferredIn', { kind: 'pas', batch: { id: 1, status: 'packing', slip_count: 3 }, runs: [{ phase: 'run', started_at: now, finished_at: now, paused_total_sec: 0, planned_count: 3 }],
      dailyTotal: { total: 165, machine: 165, transferredIn: 1 }, slips: [] }],
  ];
  for (const [label, locals] of cases) {
    let html = null;
    try { html = ejs.render(src, { ...base, ...locals }, { filename: fileURLToPath(new URL('../views/line.ejs', import.meta.url)) }); } catch (e) { ok(false, `${label}: 描画失敗 ${e.message}`); continue; }
    const js = html.split('<script>').pop().split('</script>')[0];
    let syntaxOk = true;
    try { new vm.Script(js); } catch (e) { syntaxOk = false; console.log('   ', e.message); }
    ok(syntaxOk, `${label}: インラインJSが構文エラーでない`);
    ok(!/onclick="[^"]*"[^ >]/.test(html), `${label}: onclick 属性の引用符が壊れていない`);
    ok((html.match(/<script>/g) || []).length === (html.match(/<\/script>/g) || []).length, `${label}: script の開始/終了タグ数が一致`);
    if (label.startsWith('MELT sorting')) {
      ok(html.includes('id="sortToPas"') && html.includes('id="sortOther"'), `${label}: PAS へ移す件数の入力がある`);
      ok(html.includes('id="reqCard"') && html.includes('ピッキングへ送信') && html.includes('見つかった'), `${label}: 依頼カード (未送信候補・SKU単位の見つかった)`);
      ok(!html.includes('全部受け取った'), `${label}: 未送信候補が残る間は「全部受け取った」を出さない`);
      ok(!html.includes('foundSku(1,'), `${label}: SKU を onclick に文字列埋め込みしない`);
      ok(html.includes('data-sku="b&#39;x"'), `${label}: SKU は data 属性でエスケープして渡す (記号入りも加工しない)`);
      ok(!html.includes('<太郎>') && html.includes('\\u003c太郎>'), `${label}: 埋め込み JSON の < がエスケープされる`);
    }
    if (label.startsWith('PAS with transferredIn')) {
      ok(html.includes('MELT-LINE から移した分'), `${label}: 累計に MELT からの振替が表示される`);
      ok(html.includes('計測中') === false || html.includes('live'), `${label}: 描画OK`);
    }
    if (label === 'PAS running') ok(html.includes('計測中'), `${label}: 計測中の表示がある`);
    if (label === 'PAS paused') ok(html.includes('計測停止中'), `${label}: 中断中は計測停止中の表示`);
    if (label === 'PAS stopped') ok(!html.includes('⏱ 計測中'), `${label}: 停止後は計測中バーが出ない`);
    ok(html.includes('一覧へ戻る'), `${label}: 一覧へ戻るボタンがある`);
  }
}

db.close();
try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* 後始末失敗は無視 */ }
console.log(`\n${failed === 0 ? '✅ 全テスト PASS' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
