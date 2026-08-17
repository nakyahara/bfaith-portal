/**
 * picking — 作業実績の統計 (30日ローリング) のテスト。
 *
 * 検証の要点:
 *   - 作業者の帰属が「バッチの最終担当者」ではなく明細1件ごと (交代しても分かれる)
 *   - 引当分類の重さを補正した速さ指数 (重い分類を引いた人が遅く見えない)
 *   - 放置による外れ値を除外し、捨てた件数を報告する (黙って捨てない)
 *   - 集計開始日の床止め (テスト運用期間を混ぜない)
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'picking-test-'));
// 実運用の値に依存しないよう、テスト内で明示的に固定する
process.env.PICKING_STATS_MIN_DATE = '2026-08-15';
process.env.PICKING_STATS_OUTLIER_SEC = '180';
process.env.PICKING_STATS_MIN_LINES = '10';
process.env.PICKING_STATS_MIN_CLASS_LINES = '5';

const { initPickingDB, getDB } = await import('../db.js');
const {
  getPickingStats, getTodayProgress, statsRange, shiftDate, displayWorkerName,
  STATS_MIN_DATE, STATS_MIN_LINES, STATS_OUTLIER_SEC, STATS_MIN_CLASS_LINES,
} = await import('../service.js');

initPickingDB();
const db = getDB();

let passed = 0;
function t(name, fn) { fn(); passed++; console.log(`  ok: ${name}`); }

// ─── テストデータ組み立て ───

let batchSeq = 0;
let opSeq = 0;

/** 秒を UTC 'YYYY-MM-DDTHH:MM:SSZ' に (基準は各日 01:00Z = JST 10:00)。 */
function at(workDate, offsetSec) {
  const base = Date.parse(`${workDate}T01:00:00Z`);
  return new Date(base + offsetSec * 1000).toISOString().slice(0, 19) + 'Z';
}

/**
 * 完了バッチを1つ作る。
 * @param {{workDate, cls, worker, secs: number[], perLineWorker?: (i:number)=>string,
 *          status?: string, validity?: string, shortageAt?: number}} spec
 */
function makeBatch(spec) {
  const id = ++batchSeq;
  const secs = spec.secs;
  const status = spec.status || 'done';
  const totalSec = secs.reduce((s, v) => s + v, 0);
  db.prepare(`
    INSERT INTO pk_batches (id, tb_no, hikiate_class, folder_name, work_date, composition,
      line_count, slip_count, total_qty, status, worker, started_at, finished_at,
      validity, csv_sha256, imported_by, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, '単品', ?, ?, ?, ?, ?, ?, ?, ?, 'sha', 'test', ?, ?)
  `).run(
    id, `TB${String(id).padStart(6, '0')}`, spec.cls, spec.folder || `出荷_${id}`, spec.workDate,
    secs.length, secs.length, secs.length, status, spec.worker,
    at(spec.workDate, 0), at(spec.workDate, totalSec),
    spec.validity || 'valid', at(spec.workDate, 0), at(spec.workDate, totalSec),
  );

  let cursor = 0;
  secs.forEach((sec, i) => {
    const seq = i + 1;
    const shownAt = at(spec.workDate, cursor);
    cursor += sec;
    const doneAt = at(spec.workDate, cursor);
    const isShortage = spec.shortageAt === seq;
    // 未完了バッチは明細も pending のまま (完了時刻を持たない)
    const lineDone = status === 'done' || i === 0;
    db.prepare(`
      INSERT INTO pk_lines (batch_id, seq, location, block, sku, product_name, qty, status, shown_at, done_at, shortage_qty)
      VALUES (?, ?, ?, 'P3FB', ?, ?, 1, ?, ?, ?, ?)
    `).run(
      id, seq, String(100 + seq).padStart(8, '0'), `sku${seq}`, `商品${seq}`,
      isShortage ? 'shortage' : (lineDone ? 'done' : 'pending'),
      lineDone ? shownAt : null, lineDone ? doneAt : null, isShortage ? 1 : null,
    );
    if (!lineDone) return;
    const worker = spec.perLineWorker ? spec.perLineWorker(i) : spec.worker;
    db.prepare(`
      INSERT INTO pk_events (op_id, batch_id, worker, event, line_seq, result_json, at)
      VALUES (?, ?, ?, ?, ?, '{}', ?)
    `).run(`op${++opSeq}`, id, worker, isShortage ? 'shortage' : 'next', seq, doneAt);
  });
  return id;
}

// ─── 期間の計算 ───

t('shiftDate: 月跨ぎ・年跨ぎ', () => {
  assert.equal(shiftDate('2026-08-17', -1), '2026-08-16');
  assert.equal(shiftDate('2026-09-01', -1), '2026-08-31');
  assert.equal(shiftDate('2026-01-01', -1), '2025-12-31');
  assert.equal(shiftDate('2026-08-17', 0), '2026-08-17');
});

t('statsRange: until を含む30日窓 / 開始日は床止め', () => {
  const far = statsRange('2026-12-01', 30);
  assert.equal(far.since, '2026-11-02', '30日窓 = until を含めて30日');
  assert.equal(far.until, '2026-12-01');
  assert.equal(far.clamped, false);

  const near = statsRange('2026-08-17', 30);
  assert.equal(near.since, STATS_MIN_DATE, 'テスト運用期間まで遡らない');
  assert.equal(near.clamped, true, '床止めしたことが分かる');
});

t('displayWorkerName: emailはローカル部だけ (掲示に社内メールを出さない)', () => {
  assert.equal(displayWorkerName('有國陽'), '有國陽');
  assert.equal(displayWorkerName('d.nakahara@b-faith.biz'), 'd.nakahara');
  assert.equal(displayWorkerName(''), '(不明)');
  assert.equal(displayWorkerName(null), '(不明)');
});

// ─── 集計本体 ───

t('作業者の帰属は明細1件ごと (交代してもバッチ最終担当者に全量計上しない)', () => {
  // 10明細のうち前半5件をA、後半5件をBが処理。バッチの worker 列は最終担当のBだけ
  makeBatch({
    workDate: '2026-08-16', cls: '軽い分類', worker: 'B',
    secs: [10, 10, 10, 10, 10, 20, 20, 20, 20, 20],
    perLineWorker: (i) => (i < 5 ? 'A' : 'B'),
  });
  const s = getPickingStats({ until: '2026-08-16', days: 30 });
  const a = s.workers.find((w) => w.name === 'A');
  const b = s.workers.find((w) => w.name === 'B');
  assert.equal(a.lines, 5);
  assert.equal(b.lines, 5);
  assert.equal(a.sec, 50);
  assert.equal(b.sec, 100);
  assert.equal(a.secPerLine, 10);
  assert.equal(b.secPerLine, 20);
});

t('速さ指数: 引当分類の重さを補正する', () => {
  db.exec('DELETE FROM pk_events; DELETE FROM pk_lines; DELETE FROM pk_batches;');
  batchSeq = 0;
  // 重い分類 (基準30秒) を C が24秒で、軽い分類 (基準10秒) を D が12秒でこなした。
  // 生の秒/明細では D (12秒) の方が速く見えるが、指数では C が上に来るべき
  makeBatch({ workDate: '2026-08-16', cls: '重い分類', worker: 'C', secs: Array(10).fill(24) });
  makeBatch({ workDate: '2026-08-16', cls: '重い分類', worker: 'E', secs: Array(10).fill(36) });
  makeBatch({ workDate: '2026-08-16', cls: '軽い分類', worker: 'D', secs: Array(10).fill(12) });
  makeBatch({ workDate: '2026-08-16', cls: '軽い分類', worker: 'E', secs: Array(10).fill(8) });

  const s = getPickingStats({ until: '2026-08-16', days: 30 });
  const heavy = s.baseline.find((c) => c.key === '重い分類');
  const light = s.baseline.find((c) => c.key === '軽い分類');
  assert.equal(heavy.avgSec, 30, '基準秒 = その分類の全員平均');
  assert.equal(light.avgSec, 10);

  const c = s.workers.find((w) => w.name === 'C');
  const d = s.workers.find((w) => w.name === 'D');
  assert.equal(c.index, 125, '30秒の仕事を24秒 → 125');
  assert.equal(d.index, 83, '10秒の仕事を12秒 → 83');
  assert.ok(c.secPerLine > d.secPerLine, '生の秒ではCの方が遅く見える');
  assert.ok(s.workers.findIndex((w) => w.name === 'C') < s.workers.findIndex((w) => w.name === 'D'),
    '指数順ではCが上');

  // E は重い分類で遅く (36秒/基準30秒)、軽い分類で速い (8秒/基準10秒)。
  // 指数は「合計時間の短縮率」なので、時間を多く使う重い分類での遅れが強く効く
  // (期待 30×10 + 10×10 = 400秒 ÷ 実測 360 + 80 = 440秒 → 91)
  const e = s.workers.find((w) => w.name === 'E');
  assert.equal(e.index, 91);
});

t('引当分類ごとに「誰が速いか」を出す (分類内は重さ補正なしの素の比較)', () => {
  db.exec('DELETE FROM pk_events; DELETE FROM pk_lines; DELETE FROM pk_batches;');
  batchSeq = 0;
  // 重い分類: S=20秒 / T=40秒 (分類平均30秒) → 分類内では S が上位
  makeBatch({ workDate: '2026-08-16', cls: '重い分類', worker: 'S', secs: Array(10).fill(20) });
  makeBatch({ workDate: '2026-08-16', cls: '重い分類', worker: 'T', secs: Array(10).fill(40) });
  // 軽い分類: T=8秒 / S=12秒 (分類平均10秒) → 分類内では T が上位 (総合順位とは別)
  makeBatch({ workDate: '2026-08-16', cls: '軽い分類', worker: 'T', secs: Array(10).fill(8) });
  makeBatch({ workDate: '2026-08-16', cls: '軽い分類', worker: 'S', secs: Array(10).fill(12) });

  const s = getPickingStats({ until: '2026-08-16', days: 30 });
  const heavy = s.baseline.find((c) => c.key === '重い分類');
  const light = s.baseline.find((c) => c.key === '軽い分類');

  assert.deepEqual(heavy.workers.map((w) => w.name), ['S', 'T'], '重い分類はSが速い');
  assert.deepEqual(light.workers.map((w) => w.name), ['T', 'S'], '軽い分類はTが速い (分類ごとに順位が変わる)');
  assert.equal(heavy.workers[0].secPerLine, 20);
  assert.equal(heavy.workers[0].index, 150, '分類平均30秒 ÷ 実測20秒 = 150');
  assert.equal(heavy.workers[1].index, 75);
  assert.equal(light.workers[0].index, 125);
  assert.equal(heavy.workerCount, 2);
  assert.equal(s.minClassLines, STATS_MIN_CLASS_LINES);
});

t('分類別も母数不足は参考値として順位から外す', () => {
  db.exec('DELETE FROM pk_events; DELETE FROM pk_lines; DELETE FROM pk_batches;');
  batchSeq = 0;
  makeBatch({ workDate: '2026-08-16', cls: '分類P', worker: 'U', secs: Array(8).fill(20) });
  makeBatch({ workDate: '2026-08-16', cls: '分類P', worker: 'V', secs: [1, 1] });   // 速いが2明細だけ
  const s = getPickingStats({ until: '2026-08-16', days: 30 });
  const c = s.baseline.find((x) => x.key === '分類P');
  assert.deepEqual(c.workers.map((w) => w.name), ['U', 'V'], '参考値は速くても下');
  assert.equal(c.workers[0].provisional, false);
  assert.equal(c.workers[1].provisional, true);
});

t('外れ値: 上限超えの明細は除外し、件数を報告する', () => {
  db.exec('DELETE FROM pk_events; DELETE FROM pk_lines; DELETE FROM pk_batches;');
  batchSeq = 0;
  // 10秒×9件 + 「開いたまま放置」1件 (600秒)
  makeBatch({ workDate: '2026-08-16', cls: '分類X', worker: 'F', secs: [...Array(9).fill(10), 600] });
  const s = getPickingStats({ until: '2026-08-16', days: 30 });
  const f = s.workers.find((w) => w.name === 'F');
  assert.equal(f.lines, 9, '放置1件は集計に入らない');
  assert.equal(f.secPerLine, 10, '平均が壊れない');
  assert.equal(s.total.excluded.lines, 1);
  assert.equal(s.total.excluded.sec, 600, '捨てた時間も見える');
  assert.equal(s.outlierSec, STATS_OUTLIER_SEC);
});

t('全明細が外れ値だった作業者も行として残る (除外が誰にも見えなくならない)', () => {
  db.exec('DELETE FROM pk_events; DELETE FROM pk_lines; DELETE FROM pk_batches;');
  batchSeq = 0;
  makeBatch({ workDate: '2026-08-16', cls: '分類R', worker: 'Q', secs: Array(12).fill(10) });
  makeBatch({ workDate: '2026-08-16', cls: '分類R', worker: 'R', secs: [600, 900] });   // 全部放置
  const s = getPickingStats({ until: '2026-08-16', days: 30 });
  const r = s.workers.find((w) => w.name === 'R');
  assert.ok(r, '一覧から消えない');
  assert.equal(r.lines, 0);
  assert.equal(r.secPerLine, null, '0除算でNaNにしない');
  assert.equal(r.index, null);
  assert.equal(r.excluded, 2);
  assert.equal(r.provisional, true);
  assert.equal(s.workers[0].name, 'Q', '実績のある人が先頭');
  assert.equal(s.workers.find((w) => w.name === 'Q').excluded, 0);
});

t('母数不足は参考値 (provisional) として順位から外し末尾へ', () => {
  db.exec('DELETE FROM pk_events; DELETE FROM pk_lines; DELETE FROM pk_batches;');
  batchSeq = 0;
  // G は爆速だが3明細だけ / H は平均的だが20明細
  makeBatch({ workDate: '2026-08-16', cls: '分類Y', worker: 'G', secs: [1, 1, 1] });
  makeBatch({ workDate: '2026-08-16', cls: '分類Y', worker: 'H', secs: Array(20).fill(20) });
  const s = getPickingStats({ until: '2026-08-16', days: 30 });
  const g = s.workers.find((w) => w.name === 'G');
  const h = s.workers.find((w) => w.name === 'H');
  assert.equal(g.provisional, true, `${STATS_MIN_LINES}明細未満は参考値`);
  assert.equal(h.provisional, false);
  assert.equal(s.workers[0].name, 'H', '参考値は指数が高くても先頭に来ない');
  assert.equal(s.minLines, STATS_MIN_LINES);
});

t('集計対象は「完了・有効」バッチのみ / 期間外は入らない', () => {
  db.exec('DELETE FROM pk_events; DELETE FROM pk_lines; DELETE FROM pk_batches;');
  batchSeq = 0;
  makeBatch({ workDate: '2026-08-16', cls: '分類Z', worker: 'I', secs: Array(4).fill(10) });
  makeBatch({ workDate: '2026-08-16', cls: '分類Z', worker: 'J', secs: Array(4).fill(10), status: 'picking' });
  makeBatch({ workDate: '2026-08-16', cls: '分類Z', worker: 'K', secs: Array(4).fill(10), validity: 'invalid' });
  makeBatch({ workDate: '2026-08-14', cls: '分類Z', worker: 'L', secs: Array(4).fill(10) });   // 床止めより前
  const s = getPickingStats({ until: '2026-08-16', days: 30 });
  assert.deepEqual(s.workers.map((w) => w.name), ['I']);
  assert.equal(s.total.lines, 4);
  assert.equal(s.total.batches, 1);
  assert.equal(s.since, STATS_MIN_DATE, 'テスト運用の8/14は範囲外');
});

t('欠品は件数として併記される (止まって報告した人が損に見えないように)', () => {
  db.exec('DELETE FROM pk_events; DELETE FROM pk_lines; DELETE FROM pk_batches;');
  batchSeq = 0;
  makeBatch({ workDate: '2026-08-16', cls: '分類W', worker: 'M', secs: Array(5).fill(10), shortageAt: 3 });
  const s = getPickingStats({ until: '2026-08-16', days: 30 });
  const m = s.workers.find((w) => w.name === 'M');
  assert.equal(m.shortages, 1);
  assert.equal(m.lines, 5, '欠品明細も所要時間の集計には入る');
});

t('日別集計とバッチ数・稼働日数', () => {
  db.exec('DELETE FROM pk_events; DELETE FROM pk_lines; DELETE FROM pk_batches;');
  batchSeq = 0;
  makeBatch({ workDate: '2026-08-15', cls: '分類V', worker: 'N', secs: Array(5).fill(10) });
  makeBatch({ workDate: '2026-08-16', cls: '分類V', worker: 'N', secs: Array(5).fill(20) });
  const s = getPickingStats({ until: '2026-08-16', days: 30 });
  assert.deepEqual(s.byDate.map((d) => d.date), ['2026-08-15', '2026-08-16'], '日付昇順');
  assert.equal(s.byDate[0].secPerLine, 10);
  assert.equal(s.byDate[1].secPerLine, 20);
  const n = s.workers[0];
  assert.equal(n.batches, 2);
  assert.equal(n.days, 2);
  assert.equal(s.total.secPerLine, 15);
  assert.equal(s.total.linesPerHour, 240, '10明細/150秒 = 240明細/時');
});

t('データが無い期間でも落ちない', () => {
  const s = getPickingStats({ until: '2026-08-20', days: 1 });
  assert.equal(s.total.lines, 0);
  assert.equal(s.total.secPerLine, null);
  assert.deepEqual(s.workers, []);
  assert.deepEqual(s.baseline, []);
});

// ─── 当日進捗 (掲示ボード) ───

t('getTodayProgress: 残り明細と作業中の人', () => {
  db.exec('DELETE FROM pk_events; DELETE FROM pk_lines; DELETE FROM pk_batches;');
  batchSeq = 0;
  makeBatch({ workDate: '2026-08-17', cls: '分類U', worker: 'O', secs: Array(6).fill(10) });
  // 進行中バッチ: 明細1件だけ完了させてある
  makeBatch({ workDate: '2026-08-17', cls: '分類U', worker: 'P', secs: Array(4).fill(10), status: 'picking' });
  const p = getTodayProgress('2026-08-17');
  assert.equal(p.batchCount, 2);
  assert.equal(p.doneCount, 1);
  assert.equal(p.totalLines, 10);
  assert.equal(p.doneLines, 7, '完了6 + 進行中バッチの1件');
  assert.equal(p.remainingLines, 3);
  assert.equal(p.secPerLine, 10, '完了バッチのみで算出');
  assert.deepEqual(p.active.map((a) => a.name), ['P']);
  assert.equal(p.active[0].paused, false);
});

console.log(`\ntest-stats: ${passed} 件 pass`);
