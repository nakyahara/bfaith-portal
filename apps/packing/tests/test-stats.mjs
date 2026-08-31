/**
 * packing — 梱包実績統計 (getPackingStats) のテスト。
 *
 * 検証の要点:
 *   - 単位は伝票 (done_at - shown_at)。作業者の帰属は「次へ」イベント優先 (交代しても分かれる)
 *   - 梱包機ライン (PAS/MELT) のバッチは集計対象外 (手梱包と土俵が違う)
 *   - 外れ値の除外と除外件数の報告 (黙って捨てない — picking と同じ)
 *   - 引当分類は picking の pk_batches から引く。突合なしは '(分類不明)'
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'packing-stats-test-'));
process.env.PACKING_STATS_MIN_DATE = '2026-08-01';
process.env.PACKING_STATS_OUTLIER_SEC = '1800';
process.env.PACKING_STATS_MIN_SLIPS = '10';
process.env.PACKING_STATS_MIN_CLASS_SLIPS = '5';

const { initPickingDB } = await import('../../picking/db.js');
const { initPackingDB, getDB } = await import('../db.js');
const {
  getPackingStats, packStatsRange, getTodayPackingProgress,
  PACK_STATS_MIN_DATE, PACK_STATS_OUTLIER_SEC, PACK_STATS_MIN_SLIPS, PACK_STATS_MIN_CLASS_SLIPS,
} = await import('../stats.js');

initPickingDB();   // pk_batches (引当分類の参照元) — 同一DBファイル同居
initPackingDB();
const db = getDB();

let passed = 0;
function t(name, fn) { fn(); passed++; console.log(`  ok: ${name}`); }

// ─── テストデータ組み立て ───

let pickSeq = 0;
let packSeq = 0;
let opSeq = 0;

/** 秒を UTC ISO に (基準は各日 01:00Z = JST 10:00)。 */
function at(workDate, offsetSec) {
  const base = Date.parse(`${workDate}T01:00:00Z`);
  return new Date(base + offsetSec * 1000).toISOString().slice(0, 19) + 'Z';
}

/** picking 側バッチ (引当分類の参照元)。 */
function makePickBatch(workDate, cls) {
  const id = ++pickSeq;
  db.prepare(`
    INSERT INTO pk_batches (id, tb_no, hikiate_class, work_date, composition,
      line_count, slip_count, total_qty, status, validity, csv_sha256, imported_by, created_at, updated_at)
    VALUES (?, ?, ?, ?, '単品', 1, 1, 1, 'done', 'valid', 'sha', 'test', ?, ?)
  `).run(id, `TB${id}`, cls, workDate, at(workDate, 0), at(workDate, 0));
  return id;
}

/**
 * 完了した梱包バッチを1つ作る。
 * @param {{workDate, cls: string|null, worker, secs: number[],
 *          perSlipWorker?: (i:number)=>string, status?: string, validity?: string}} spec
 *   cls=null で pk_batch_id 無し ('(分類不明)' になる)
 */
function makePackBatch(spec) {
  const id = ++packSeq;
  const pkBatchId = spec.cls != null ? makePickBatch(spec.workDate, spec.cls) : null;
  const status = spec.status || 'done';
  const secs = spec.secs;
  const totalSec = secs.reduce((s, v) => s + v, 0);
  db.prepare(`
    INSERT INTO pk_pack_batches (id, tb_key, folder_name, work_date, slip_count, line_count, total_qty,
      pk_batch_id, match_status, status, worker, started_at, finished_at,
      validity, csv_sha256, imported_by, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'sha', 'test', ?, ?)
  `).run(
    id, `KEY${id}`, `出荷_${id}`, spec.workDate, secs.length, secs.length, secs.length,
    pkBatchId, pkBatchId ? 'ok' : 'no_picking', status, spec.worker,
    at(spec.workDate, 0), at(spec.workDate, totalSec),
    spec.validity || 'valid', at(spec.workDate, 0), at(spec.workDate, 0),
  );
  let offset = 0;
  secs.forEach((sec, i) => {
    const done = status === 'done';
    db.prepare(`
      INSERT INTO pk_pack_slips (batch_id, seq, ne_slip_no, slip_no, mall, status, shown_at, done_at)
      VALUES (?, ?, ?, ?, 'テスト店', ?, ?, ?)
    `).run(id, i + 1, `NE${id}-${i}`, `SP${id}-${i}`,
      done ? 'done' : 'pending',
      done ? at(spec.workDate, offset) : null,
      done ? at(spec.workDate, offset + sec) : null);
    if (done && spec.perSlipWorker) {
      db.prepare(`
        INSERT INTO pk_pack_events (op_id, batch_id, worker, event, slip_seq, result_json, at)
        VALUES (?, ?, ?, 'next', ?, '{}', ?)
      `).run(`op${++opSeq}`, id, spec.perSlipWorker(i), i + 1, at(spec.workDate, offset + sec));
    }
    offset += sec;
  });
  return id;
}

const D = '2026-08-19';
const CLS_A = 'ネコポス手動単品';
const CLS_B = '50サイズ宅急便単品';

// 花子: 分類Aを 20秒×6伝票 / 太郎: 分類Aを 40秒×6伝票 (花子が倍速)
makePackBatch({ workDate: D, cls: CLS_A, worker: 'hanako@test', secs: [20, 20, 20, 20, 20, 20] });
makePackBatch({ workDate: D, cls: CLS_A, worker: 'taro@test', secs: [40, 40, 40, 40, 40, 40] });
// 分類B: 花子 30秒×4 (分類最低数5未満 = 参考値)
makePackBatch({ workDate: D, cls: CLS_B, worker: 'hanako@test', secs: [30, 30, 30, 30] });
// 梱包機ライン (集計対象外)
makePackBatch({ workDate: D, cls: 'ネコポス【梱包機PAS-LINE《3つ折り》】単品', worker: 'taro@test', secs: [5, 5, 5] });
// 外れ値 (1800秒超) を含むバッチ: 次郎 2000秒×2 + 正常50秒×1
makePackBatch({ workDate: D, cls: CLS_A, worker: 'jiro@test', secs: [2000, 2000, 50] });
// 交代の帰属: バッチ最終担当は太郎だが、伝票1〜2の「次へ」は花子
makePackBatch({
  workDate: D, cls: CLS_A, worker: 'taro@test', secs: [10, 10, 30],
  perSlipWorker: (i) => (i < 2 ? 'hanako@test' : 'taro@test'),
});
// 突合なし (分類不明)
makePackBatch({ workDate: D, cls: null, secs: [25], worker: 'hanako@test' });
// 完了していないバッチ・無効バッチは対象外
makePackBatch({ workDate: D, cls: CLS_A, worker: 'hanako@test', secs: [15], status: 'packing' });
makePackBatch({ workDate: D, cls: CLS_A, worker: 'hanako@test', secs: [15], validity: 'invalid' });

const stats = getPackingStats({ until: D, days: 30 });

// ─── 検証 ───

t('期間の床止め (PACK_STATS_MIN_DATE より前に遡らない)', () => {
  const r = packStatsRange('2026-08-05', 30);
  assert.equal(r.since, PACK_STATS_MIN_DATE);
  assert.equal(r.clamped, true);
});

t('総数: 手梱包の完了伝票のみ (ライン・未完了・invalid・外れ値を除く)', () => {
  // 6+6+4+1(50秒)+3(交代)+1(分類不明) = 21
  assert.equal(stats.total.slips, 21);
  assert.equal(stats.total.excluded.slips, 2);   // 2000秒×2
});

t('梱包機ラインの分類は baseline に現れない', () => {
  assert.ok(!stats.baseline.some((c) => c.key.includes('PAS-LINE')));
});

t('分類Aの平均と作業者内訳 (花子20秒 / 太郎40秒)', () => {
  const a = stats.baseline.find((c) => c.key === CLS_A);
  assert.ok(a);
  const hanako = a.workers.find((w) => w.worker === 'hanako@test');
  const taro = a.workers.find((w) => w.worker === 'taro@test');
  // 花子: 20×6 + 交代分10×2 = 8伝票・平均17.5秒 / 太郎: 40×6 + 交代分30×1 = 7伝票
  assert.equal(hanako.slips, 8);
  assert.ok(Math.abs(hanako.secPerSlip - 17.5) < 0.01);
  assert.equal(taro.slips, 7);
  assert.ok(taro.secPerSlip > hanako.secPerSlip);
  // 分類内順: 花子 (速い) が先
  assert.ok(a.workers.indexOf(hanako) < a.workers.indexOf(taro));
});

t('交代しても伝票単位で帰属する (バッチ最終担当に全部つかない)', () => {
  const a = stats.baseline.find((c) => c.key === CLS_A);
  const taro = a.workers.find((w) => w.worker === 'taro@test');
  // 太郎に交代バッチの3伝票全部 (10+10+30) がつくなら 9伝票になるはず
  assert.equal(taro.slips, 7);
});

t('突合なしバッチは (分類不明) に入る', () => {
  const unknown = stats.baseline.find((c) => c.key === '(分類不明)');
  assert.ok(unknown);
  assert.equal(unknown.slips, 1);
});

t('外れ値だけの作業者も行が残り除外件数が見える', () => {
  const jiro = stats.workers.find((w) => w.worker === 'jiro@test');
  assert.ok(jiro);
  assert.equal(jiro.excluded, 2);
  assert.equal(jiro.slips, 1);   // 正常な50秒だけ
});

t('母数不足は参考値フラグ (総合10 / 分類5)', () => {
  assert.equal(stats.minSlips, PACK_STATS_MIN_SLIPS);
  assert.equal(stats.minClassSlips, PACK_STATS_MIN_CLASS_SLIPS);
  const b = stats.baseline.find((c) => c.key === CLS_B);
  assert.equal(b.workers[0].provisional, true);    // 4伝票 < 5
  const jiro = stats.workers.find((w) => w.worker === 'jiro@test');
  assert.equal(jiro.provisional, true);            // 1伝票 < 10
  const hanako = stats.workers.find((w) => w.worker === 'hanako@test');
  assert.equal(hanako.provisional, false);         // 8+4+1 = 13伝票 ≥ 10
});

t('速さ指数: 花子 > 100 > 太郎 (分類の重さ補正後)', () => {
  const hanako = stats.workers.find((w) => w.worker === 'hanako@test');
  const taro = stats.workers.find((w) => w.worker === 'taro@test');
  assert.ok(hanako.index > 100, `花子 index=${hanako.index}`);
  assert.ok(taro.index < 100, `太郎 index=${taro.index}`);
});

t('日別推移が出る', () => {
  assert.equal(stats.byDate.length, 1);
  assert.equal(stats.byDate[0].slips, 21);
});

t('外れ値しきい値は設定値', () => {
  assert.equal(stats.outlierSec, PACK_STATS_OUTLIER_SEC);
});

// ─── 本日の進捗 (実績ボード 2026-08-31) ───
t('本日の進捗: 手梱包の done 伝票 + ラインは final_count・作業中一覧・ライン累計', () => {
  const day = '2026-08-30';
  // 手梱包: 完了 3伝票 (10,20,30秒) と 作業中 (2伝票中1完了)
  makePackBatch({ workDate: day, cls: CLS_A, worker: 'taro@test', secs: [10, 20, 30] });
  const wip = makePackBatch({ workDate: day, cls: CLS_A, worker: 'hanako@test', secs: [15, 25], status: 'packing' });
  // 作業中バッチの伝票は helper が pending で作る → 1枚目だけ完了 (15秒) にする
  db.prepare("UPDATE pk_pack_slips SET status='done', shown_at=?, done_at=? WHERE batch_id=? AND seq=1").run(at(day, 1000), at(day, 1015), wip);
  // 梱包機ライン (PAS): 完了 final_count=40 (伝票 42)
  const pasPk = makePickBatch(day, 'ネコポス【梱包機PAS-LINE《3つ折り》】単品');
  db.prepare(`INSERT INTO pk_pack_batches (id, tb_key, folder_name, work_date, slip_count, line_count, total_qty, pk_batch_id,
      match_status, status, worker, validity, csv_sha256, imported_by, created_at, updated_at)
    VALUES (900, 'KEY900', '出荷_90', ?, 42, 42, 42, ?, 'ok', 'done', 'pas@test', 'valid', 'sha', 'test', ?, ?)`).run(day, pasPk, at(day, 0), at(day, 0));
  db.prepare(`INSERT INTO pk_pack_line_runs (batch_id, phase, started_at, finished_at, planned_count, final_count, manual_count, worker, updated_at)
    VALUES (900, 'run', ?, ?, 42, 40, 2, 'pas@test', ?)`).run(at(day, 0), at(day, 600), at(day, 600));
  // ラインバッチに時刻つきの done 伝票が (何かの経路で) あっても、手梱包の秒/伝票には混ぜない (Codex)
  db.prepare(`INSERT INTO pk_pack_slips (batch_id, seq, ne_slip_no, slip_no, mall, status, shown_at, done_at)
    VALUES (900, 1, 'NE900-1', 'SP900-1', 'テスト店', 'done', ?, ?)`).run(at(day, 2000), at(day, 2900));
  const p = getTodayPackingProgress(day);
  assert.equal(p.batchCount, 2, '手梱包のバッチ数 (ラインは別枠)');
  assert.equal(p.doneCount, 1);
  assert.equal(p.totalSlips, 3 + 2, '手梱包の伝票数のみ');
  assert.equal(p.doneSlips, 3 + 1, '手梱包の done 伝票のみ');
  assert.equal(p.remainingSlips, 1);
  assert.deepEqual(p.line, { batchCount: 1, doneCount: 1, totalSlips: 42, doneSlips: 40 }, 'ラインは別枠 (final_count)');
  assert.equal(Math.round(p.secPerSlip), Math.round((10 + 20 + 30 + 15) / 4), '秒/伝票は手梱包の done 伝票の平均');
  assert.deepEqual(p.active.map((a) => [a.folder, a.paused, a.line]), [[`出荷_${wip}`, false, false]]);
  assert.deepEqual([p.lines.pas.total, p.lines.pas.machine], [40, 38], 'PAS 累計 (出荷/機械通過)');
  assert.equal(p.lines.melt.total, 0);
});

console.log(`\npacking test-stats: ${passed} 件 pass`);
