/**
 * picking — 出荷フロアボード (floor.js) のテスト。
 *
 * 検証の要点:
 *   - 完了予測 (computeEta): 残0=done / ペース0=measuring / 締切との余裕・超過
 *   - 締切グループ: AESだけ16:00、他は16:30
 *   - lineKindOfClass が packing の lineKindOf と同じ判定 (複製のドリフト検知)
 *   - flow: picking+packing の状態から Notionカンバンの5列を導出
 *   - 要対応: 中断バッチ (pause イベントからの経過分)・突合差分・ミス候補
 *   - packing 未導入 (テーブル無し) でも picking 部分だけで応答する fail-soft
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'picking-floor-test-'));
process.env.PICKING_STATS_MIN_DATE = '2026-08-01';
process.env.PACKING_STATS_MIN_DATE = '2026-08-01';
process.env.FLOOR_DEADLINE_AES = '16:00';
process.env.FLOOR_DEADLINE_STD = '16:30';
process.env.FLOOR_RATE_WINDOW_MIN = '60';

const { initPickingDB, getDB } = await import('../db.js');
const { initPackingDB } = await import('../../packing/db.js');
const { lineKindOf } = await import('../../packing/service.js');
const {
  getFloorData, computeEta, deadlineGroupOf, lineKindOfClass, deadlineToMs, packBatchCounts,
} = await import('../floor.js');

initPickingDB();
initPackingDB();
const db = getDB();

let passed = 0;
function t(name, fn) { fn(); passed++; console.log(`  ok: ${name}`); }
async function ta(name, fn) { await fn(); passed++; console.log(`  ok: ${name}`); }

// ─── 純関数 ───

t('deadlineGroupOf: AESだけ aes、他は std', () => {
  assert.equal(deadlineGroupOf('AES《単品》'), 'aes');
  assert.equal(deadlineGroupOf('ネコポス【梱包機PAS-LINE《3つ折り》】単品'), 'std');
  assert.equal(deadlineGroupOf('ゆうパケットパフ《単品、複数個を含む全て》'), 'std');
  assert.equal(deadlineGroupOf(''), 'std');
  assert.equal(deadlineGroupOf(null), 'std');
});

t('lineKindOfClass は packing の lineKindOf と同じ判定 (複製ドリフト検知)', () => {
  const samples = [
    'ネコポス【梱包機PAS-LINE《3つ折り》】単品',
    'ネコポス【梱包機PAS-LINE《2つ折り》】単品',
    'ネコポス【梱包機MELT-LINE】単品',
    'ネコポス手動単品', 'AES《単品》', '', null,
  ];
  for (const s of samples) assert.equal(lineKindOfClass(s), lineKindOf(s), `sample: ${s}`);
});

t('computeEta: 残り0 = done', () => {
  const r = computeEta({ nowMs: 0, remaining: 0, recentDone: 10, windowMin: 60, deadlineMs: 3600000 });
  assert.equal(r.status, 'done');
});

t('computeEta: ペース未計測 = measuring', () => {
  const r = computeEta({ nowMs: 0, remaining: 100, recentDone: 0, windowMin: 60, deadlineMs: 3600000 });
  assert.equal(r.status, 'measuring');
});

t('computeEta: 残り60件を毎分1件 → 60分後。締切90分後なら余裕30分', () => {
  const r = computeEta({ nowMs: 0, remaining: 60, recentDone: 60, windowMin: 60, deadlineMs: 90 * 60000 });
  assert.equal(r.status, 'ok');
  assert.equal(r.etaMs, 60 * 60000);
  assert.equal(r.marginMin, 30);
});

t('computeEta: 締切を過ぎる見込みは late (マイナス余裕)', () => {
  const r = computeEta({ nowMs: 0, remaining: 120, recentDone: 60, windowMin: 60, deadlineMs: 90 * 60000 });
  assert.equal(r.status, 'late');
  assert.equal(r.marginMin, -30);
});

t('deadlineToMs: JSTの時刻として解釈される', () => {
  // 2026-08-20 16:00 JST = 07:00 UTC
  assert.equal(deadlineToMs('2026-08-20', '16:00'), Date.parse('2026-08-20T07:00:00Z'));
});

t('packBatchCounts: ラインバッチは完了までまとめて残数扱い', () => {
  const cutoff = '2026-08-20T04:00:00Z';
  const running = { cls: 'ネコポス【梱包機PAS-LINE《3つ折り》】単品', status: 'packing', slip_count: 100, remain_slips: 100, recent_done: 0, finished_at: null };
  assert.equal(packBatchCounts(running, cutoff).remaining, 100);
  const done = { ...running, status: 'done', finished_at: '2026-08-20T04:30:00Z' };
  const c = packBatchCounts(done, cutoff);
  assert.equal(c.remaining, 0);
  assert.equal(c.recentDone, 100);   // カットオフ後の完了 → 直近ペースに計上
});

// ─── データを組んで getFloorData ───

// 「今」= 2026-08-20 14:00 JST (05:00 UTC)。workDate = 2026-08-20
const NOW = new Date('2026-08-20T05:00:00Z');
const D = '2026-08-20';
const iso = (utc) => `${D}T${utc}:00.000Z`;

let pickId = 0;
function makePick({ cls, status, folder, worker = 'w@test', lines = 10, doneLines = 0, shortage = 0 }) {
  const id = ++pickId;
  db.prepare(`
    INSERT INTO pk_batches (id, tb_no, hikiate_class, folder_name, work_date, composition,
      line_count, slip_count, total_qty, status, worker, validity, csv_sha256, imported_by, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, '単品', ?, ?, ?, ?, ?, 'valid', 'sha', 'test', ?, ?)
  `).run(id, `TB${id}`, cls, folder, D, lines, lines, lines, status, worker, iso('00:00'), iso('00:00'));
  for (let i = 1; i <= lines; i++) {
    const st = i <= shortage ? 'shortage' : (i <= doneLines + shortage ? 'done' : 'pending');
    db.prepare(`
      INSERT INTO pk_lines (batch_id, seq, location, sku, qty, status, shown_at, done_at)
      VALUES (?, ?, '00000000', 'SKU', 1, ?, ?, ?)
    `).run(id, i, st, st === 'pending' ? null : iso('01:00'), st === 'done' ? iso('01:01') : null);
  }
  return id;
}

let packId = 0;
function makePack({ pkBatchId = null, status, folder, worker = 'p@test', slips = 10, doneSlips = 0,
  doneAt = '04:30', matchStatus = 'ok' }) {
  const id = ++packId;
  db.prepare(`
    INSERT INTO pk_pack_batches (id, tb_key, folder_name, work_date, slip_count, line_count, total_qty,
      pk_batch_id, match_status, status, worker, finished_at, validity, csv_sha256, imported_by, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'valid', 'sha', 'test', ?, ?)
  `).run(id, `KEY${id}`, folder, D, slips, slips, slips, pkBatchId, matchStatus, status, worker,
    status === 'done' ? iso(doneAt) : null, iso('00:00'), iso('00:00'));
  for (let i = 1; i <= slips; i++) {
    const done = i <= doneSlips;
    db.prepare(`
      INSERT INTO pk_pack_slips (batch_id, seq, ne_slip_no, slip_no, status, shown_at, done_at)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `).run(id, i, `NE${id}-${i}`, `SP${id}-${i}`, done ? 'done' : 'pending',
      done ? iso('04:00') : null, done ? iso(doneAt) : null);
  }
  return id;
}

// 未着手1・ピッキング中1 (中断・pauseイベント14:00の30分前)・ピッキング完了1・梱包中1・完了1
makePick({ cls: 'ネコポス手動単品', status: 'ready', folder: '出荷_01' });
const pausedPick = makePick({ cls: 'ネコポス手動単品', status: 'paused', folder: '出荷_02', doneLines: 4, shortage: 2 });
db.prepare(`
  INSERT INTO pk_events (op_id, batch_id, worker, event, result_json, at)
  VALUES ('op-pause-1', ?, 'w@test', 'pause', '{}', ?)
`).run(pausedPick, iso('04:30'));   // 13:30 JST = 30分前
const pickedPick = makePick({ cls: 'AES《単品》', status: 'done', folder: '出荷_03', doneLines: 10 });
const packingPick = makePick({ cls: '50サイズ宅急便単品', status: 'done', folder: '出荷_04', doneLines: 10 });
const donePick = makePick({ cls: 'AES《単品》', status: 'done', folder: '出荷_05', doneLines: 10 });

// 梱包側: 出荷_03 = ready (梱包待ち)・出荷_04 = packing (5/10済・直近)・出荷_05 = done (直近完了)
makePack({ pkBatchId: pickedPick, status: 'ready', folder: '出荷_03' });                            // AES 残10
makePack({ pkBatchId: packingPick, status: 'packing', folder: '出荷_04', doneSlips: 5 });           // std 残5・直近5
makePack({ pkBatchId: donePick, status: 'done', folder: '出荷_05', doneSlips: 10, doneAt: '04:40' }); // AES 完了・直近10
// 突合差分 (ready) → 要対応
makePack({ status: 'ready', folder: '出荷_99', matchStatus: 'mismatch', slips: 3 });

// ミス候補1件 (要対応 + 品質)・確定1件 (品質)
db.prepare(`
  INSERT INTO pk_pack_incidents (batch_id, kind, sku, qty, status, detected_by, created_at, updated_at)
  VALUES (2, 'shortage', 'SKU1', 1, 'candidate', 'p@test', ?, ?)
`).run(iso('04:00'), iso('04:00'));
db.prepare(`
  INSERT INTO pk_pack_incidents (batch_id, kind, sku, qty, status, attributed_worker, detected_by, confirmed_by, created_at, updated_at)
  VALUES (2, 'wrong_item', 'SKU2', 1, 'confirmed', 'w@test', 'p@test', 'admin', ?, ?)
`).run(iso('04:00'), iso('04:00'));

const data = await getFloorData({ now: NOW });

t('flow: 5列の導出 (未着手1・ピッキング中1・ピッキング完了1・梱包中1・完了1 + 突合なしready)', () => {
  assert.equal(data.flow.notStarted, 1);
  assert.equal(data.flow.picking, 1);       // paused も「ピッキング中」
  assert.equal(data.flow.picked, 2);        // 出荷_03 (pack ready) + 出荷_99 (突合なし ready)
  assert.equal(data.flow.packing, 1);
  assert.equal(data.flow.done, 1);
  assert.equal(data.flow.total, 6);
});

t('eta: AES と std に分かれ、残数が正しい', () => {
  const aes = data.eta.groups.find((g) => g.key === 'aes');
  const std = data.eta.groups.find((g) => g.key === 'std');
  assert.equal(aes.remaining, 10);          // 出荷_03 の10伝票 (ready)
  assert.equal(std.remaining, 8);           // 出荷_04 残5 + 出荷_99 残3
  assert.equal(aes.deadline, '16:00');
  assert.equal(std.deadline, '16:30');
});

t('eta: 直近ペースから予測が出る (AES=直近10件/60分 → 残10で約60分後 = 15:00頃)', () => {
  const aes = data.eta.groups.find((g) => g.key === 'aes');
  assert.equal(aes.status, 'ok');
  // 14:00 + 10件÷(10件/60分) = 15:00 JST = 06:00 UTC
  assert.ok(Math.abs(aes.etaMs - Date.parse('2026-08-20T06:00:00Z')) < 60000, `etaMs=${new Date(aes.etaMs).toISOString()}`);
  assert.equal(aes.marginMin, 60);          // 締切16:00まで余裕60分
});

t('eta: overall は一番遅いグループ', () => {
  // std: 残8 ÷ (5件/60分) = 96分 → 15:36。締切16:30 → 余裕54分 (=遅い方)
  assert.equal(data.eta.overall.status, 'ok');
  assert.ok(data.eta.overall.etaMs >= data.eta.groups.find((g) => g.key === 'aes').etaMs);
  assert.equal(data.eta.remaining, 18);
});

t('要対応: 中断30分 (bad)・突合差分・ミス候補', () => {
  const paused = data.alerts.find((a) => a.what.includes('ピッキング中断'));
  assert.ok(paused, JSON.stringify(data.alerts));
  assert.equal(paused.severity, 'bad');     // 30分 ≥ 20分
  assert.ok(paused.what.includes('30分'));
  assert.equal(paused.who, '出荷_02');
  assert.ok(data.alerts.some((a) => a.what.includes('差分あり 1バッチ')));
  assert.ok(data.alerts.some((a) => a.what.includes('候補 1件')));
});

t('品質: 確定1・候補1・欠品2', () => {
  assert.equal(data.quality.misConfirmed, 1);
  assert.equal(data.quality.misCandidate, 1);
  assert.equal(data.quality.shortages, 2);
});

t('梱包進捗: 伝票ベース (done=15 / total=33)', () => {
  // 出荷_03: 0/10, 出荷_04: 5/10, 出荷_05: 10/10, 出荷_99: 0/3
  assert.equal(data.packing.progress.doneSlips, 15);
  assert.equal(data.packing.progress.totalSlips, 33);
});

t('いま動いている: 梱包中バッチが載る', () => {
  assert.equal(data.packing.active.length, 1);
  assert.equal(data.packing.active[0].folder, '出荷_04');
  assert.equal(data.packing.active[0].paused, false);
});

t('統計は共通形 (count/secPerUnit) へ正規化される', () => {
  assert.ok(data.picking.stats.baseline.every((c) => typeof c.count === 'number'));
  assert.ok(data.picking.stats.workers.every((w) => 'secPerUnit' in w));
  assert.ok(data.packing.stats.workers.every((w) => 'secPerUnit' in w));
});

t('ヒートマップの分類は上位12まで', () => {
  assert.ok(data.picking.stats.baseline.length <= 12);
  assert.ok(data.packing.stats.baseline.length <= 12);
});

await ta('packing テーブルが無くても picking 部分は応答する (fail-soft)', async () => {
  // pk_pack_* を落とした別DBで確認
  db.exec('DROP TABLE pk_pack_line_runs; DROP TABLE pk_pack_incidents; DROP TABLE pk_pack_events;');
  db.exec('DROP TABLE pk_pack_slips; DROP TABLE pk_pack_batches;');
  const d2 = await getFloorData({ now: NOW });
  assert.equal(d2.ok, true);
  assert.equal(d2.packing, null);
  assert.equal(d2.eta, null);
  assert.ok(d2.flow.total >= 5);                 // picking 側だけで数える
  assert.equal(d2.quality.misConfirmed, null);   // 不明は null (0と区別)
  assert.ok(d2.picking.stats.baseline.length > 0);
});

console.log(`\npicking test-floor: ${passed} 件 pass`);
