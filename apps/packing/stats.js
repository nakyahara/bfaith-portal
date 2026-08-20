/**
 * 梱包の作業実績統計 (30日ローリング) — picking の getPickingStats と同型。
 *
 * フロアボード (43インチ掲示モニター) の「引当分類 × 作業者」ヒートマップ梱包版のデータ源。
 * ピッキングは 秒/明細、梱包は 秒/伝票 (pk_pack_slips.done_at - shown_at)。
 *
 * 対象は手梱包のみ (中原さん決定 2026-08-20):
 *   梱包機ライン (PAS/MELT) は機械が流すため、手梱包の秒/伝票と同じ土俵に乗せない。
 *   ライン実績はフロアボードの別枠帯 (lineDailyTotal / pk_pack_line_runs) で出す。
 *   ラインバッチは伝票単位のイベントを持たない (SLIP_ONLY_EVENTS 不可) ため
 *   shown_at/done_at が刻まれず自然に落ちるが、引当分類でも明示的に除外する。
 *
 * 作業者の帰属は伝票単位 (picking と同思想):
 *   「次へ」イベント (packing_completed) の worker を優先し、無ければバッチの最終担当者。
 *   交代 (takeover) しても交代前の伝票は前任者に付く。
 */
import { getDB, jstToday } from './db.js';
import { lineKindOf } from './service.js';
import { displayWorkerName, shiftDate } from '../picking/service.js';

// 集計の床止め (梱包の本稼働開始より前のテスト運用期間を混ぜない)
export const PACK_STATS_MIN_DATE = process.env.PACKING_STATS_MIN_DATE || '2026-08-17';
export const PACK_STATS_WINDOW_DAYS = Number(process.env.PACKING_STATS_WINDOW_DAYS) || 30;
// 外れ値: 既存の日次サマリ (getDailySummary) が 1800 秒で切っているのに合わせる。
// 梱包はギフト・大型で数分かかる伝票が正常に存在するため picking (180秒) より大きい
export const PACK_STATS_OUTLIER_SEC = Number(process.env.PACKING_STATS_OUTLIER_SEC) || 1800;
export const PACK_STATS_MIN_SLIPS = Number(process.env.PACKING_STATS_MIN_SLIPS) || 30;
export const PACK_STATS_MIN_CLASS_SLIPS = Number(process.env.PACKING_STATS_MIN_CLASS_SLIPS) || 10;

/** 集計期間 (until を含む直近 days 日。ただし PACK_STATS_MIN_DATE より前には遡らない)。 */
export function packStatsRange(until = jstToday(), days = PACK_STATS_WINDOW_DAYS) {
  const rawSince = shiftDate(until, -(Math.max(1, days) - 1));
  const since = rawSince < PACK_STATS_MIN_DATE ? PACK_STATS_MIN_DATE : rawSince;
  return { since, until, days, clamped: rawSince < PACK_STATS_MIN_DATE };
}

/**
 * 期間内の「完了バッチの完了伝票」を1件ずつ返す。
 * 引当分類は picking の pk_batches から引く (同一DBファイル同居・要件§7.3)。
 * 突合できなかったバッチ (pk_batch_id NULL) は '(分類不明)'。
 */
function loadStatsSlips(since, until) {
  return getDB().prepare(`
    SELECT b.work_date, b.id AS batch_id, s.seq,
      COALESCE(pb.hikiate_class, '(分類不明)') AS hikiate_class,
      CAST(ROUND((julianday(s.done_at) - julianday(s.shown_at)) * 86400) AS INTEGER) AS sec,
      COALESCE(
        (SELECT e.worker FROM pk_pack_events e
          WHERE e.batch_id = s.batch_id AND e.slip_seq = s.seq AND e.event = 'next'
          ORDER BY e.id DESC LIMIT 1),
        b.worker
      ) AS worker
    FROM pk_pack_slips s
    JOIN pk_pack_batches b ON b.id = s.batch_id
    LEFT JOIN pk_batches pb ON pb.id = b.pk_batch_id
    WHERE b.work_date >= ? AND b.work_date <= ?
      AND b.validity = 'valid' AND b.status = 'done'
      AND s.status = 'done' AND s.shown_at IS NOT NULL AND s.done_at IS NOT NULL
  `).all(since, until);
}

/**
 * 梱包実績の統計。返す形は picking の getPickingStats と同じ思想:
 *   - baseline: 引当分類ごとの基準秒 + 分類内の作業者内訳 (速い順)
 *   - workers: 分類の重さを補正した速さ指数つき (100 = 全体平均どおり)
 *   - 単位は伝票 (フィールド名も slips / secPerSlip — pk_pack_lines の「明細」と混同しない)
 *
 * @param {{until?: string, days?: number}} opts
 */
export function getPackingStats({ until = jstToday(), days = PACK_STATS_WINDOW_DAYS } = {}) {
  const range = packStatsRange(until, days);
  const rows = loadStatsSlips(range.since, range.until);

  // ── 外れ値と梱包機ラインの仕分け ──
  // 捨てた件数は作業者別にも出す (picking と同じ: 黙って捨てると放置の多い人ほど
  // 悪い記録が消える非対称性が見えなくなる)。ラインバッチは「除外」ではなく対象外
  const kept = [];
  const excludedByWorker = new Map();
  let excludedSlips = 0;
  let excludedSec = 0;
  for (const r of rows) {
    if (lineKindOf(r.hikiate_class) !== null) continue;   // 梱包機ライン = 集計対象外
    const sec = Number(r.sec);
    if (!Number.isFinite(sec) || sec < 0 || sec > PACK_STATS_OUTLIER_SEC) {
      excludedSlips++;
      if (Number.isFinite(sec) && sec > 0) excludedSec += sec;
      const k = r.worker || '(不明)';
      excludedByWorker.set(k, (excludedByWorker.get(k) || 0) + 1);
      continue;
    }
    kept.push({ ...r, sec });
  }

  // ── 引当分類ごとの基準秒 (期間内の全員の平均) ──
  const classMap = new Map();
  for (const r of kept) {
    const key = r.hikiate_class;
    if (!classMap.has(key)) classMap.set(key, { key, slips: 0, sec: 0, workers: new Set() });
    const c = classMap.get(key);
    c.slips++; c.sec += r.sec; c.workers.add(r.worker);
  }
  const baselineRaw = [...classMap.values()]
    .map((c) => ({ key: c.key, slips: c.slips, sec: c.sec, avgSec: c.sec / c.slips, workerCount: c.workers.size }))
    .sort((a, b) => b.slips - a.slips);
  const baselineByKey = new Map(baselineRaw.map((c) => [c.key, c.avgSec]));

  // ── 引当分類 × 作業者 ──
  const classWorkerMap = new Map();
  for (const r of kept) {
    const ck = r.hikiate_class;
    const wk = r.worker || '(不明)';
    if (!classWorkerMap.has(ck)) classWorkerMap.set(ck, new Map());
    const m = classWorkerMap.get(ck);
    if (!m.has(wk)) m.set(wk, { worker: wk, name: displayWorkerName(wk), slips: 0, sec: 0 });
    const e = m.get(wk);
    e.slips++; e.sec += r.sec;
  }

  const baseline = baselineRaw.map((c) => ({
    ...c,
    workers: [...(classWorkerMap.get(c.key)?.values() ?? [])]
      .map((w) => ({
        worker: w.worker,
        name: w.name,
        slips: w.slips,
        sec: w.sec,
        secPerSlip: w.sec / w.slips,
        index: w.sec > 0 ? Math.round((c.avgSec * w.slips / w.sec) * 100) : null,
        provisional: w.slips < PACK_STATS_MIN_CLASS_SLIPS,
      }))
      .sort((a, b) => {
        if (a.provisional !== b.provisional) return a.provisional ? 1 : -1;
        return a.secPerSlip - b.secPerSlip;   // 速い順
      }),
  }));

  // ── 作業者別 (分類の重さを補正した速さ指数) ──
  const workerMap = new Map();
  for (const r of kept) {
    const key = r.worker || '(不明)';
    if (!workerMap.has(key)) {
      workerMap.set(key, {
        worker: key, name: displayWorkerName(key),
        slips: 0, sec: 0, expectedSec: 0, batches: new Set(), days: new Set(),
      });
    }
    const w = workerMap.get(key);
    w.slips++;
    w.sec += r.sec;
    w.expectedSec += baselineByKey.get(r.hikiate_class) ?? r.sec;
    w.batches.add(r.batch_id);
    w.days.add(r.work_date);
  }

  // 全伝票が外れ値だった作業者も行だけは残して除外件数を出す (picking と同じ)
  for (const key of excludedByWorker.keys()) {
    if (workerMap.has(key)) continue;
    workerMap.set(key, {
      worker: key, name: displayWorkerName(key),
      slips: 0, sec: 0, expectedSec: 0, batches: new Set(), days: new Set(),
    });
  }

  const workers = [...workerMap.values()].map((w) => ({
    worker: w.worker,
    name: w.name,
    slips: w.slips,
    sec: w.sec,
    secPerSlip: w.slips > 0 ? w.sec / w.slips : null,
    index: w.sec > 0 ? Math.round((w.expectedSec / w.sec) * 100) : null,
    provisional: w.slips < PACK_STATS_MIN_SLIPS,
    batches: w.batches.size,
    days: w.days.size,
    excluded: excludedByWorker.get(w.worker) || 0,
  })).sort((a, b) => {
    if (a.provisional !== b.provisional) return a.provisional ? 1 : -1;
    if ((b.index ?? 0) !== (a.index ?? 0)) return (b.index ?? 0) - (a.index ?? 0);
    return b.slips - a.slips;
  });

  // ── 日別 (推移) ──
  const dateMap = new Map();
  for (const r of kept) {
    if (!dateMap.has(r.work_date)) dateMap.set(r.work_date, { date: r.work_date, slips: 0, sec: 0 });
    const d = dateMap.get(r.work_date);
    d.slips++; d.sec += r.sec;
  }
  const byDate = [...dateMap.values()]
    .map((d) => ({ date: d.date, slips: d.slips, sec: d.sec, secPerSlip: d.sec / d.slips }))
    .sort((a, b) => a.date.localeCompare(b.date));

  const totalSec = kept.reduce((s, r) => s + r.sec, 0);
  const total = {
    slips: kept.length,
    sec: totalSec,
    secPerSlip: kept.length > 0 ? totalSec / kept.length : null,
    batches: new Set(kept.map((r) => r.batch_id)).size,
    workers: workerMap.size,
    days: dateMap.size,
    excluded: { slips: excludedSlips, sec: excludedSec },
  };

  return {
    ...range,
    minSlips: PACK_STATS_MIN_SLIPS,
    minClassSlips: PACK_STATS_MIN_CLASS_SLIPS,
    outlierSec: PACK_STATS_OUTLIER_SEC,
    total,
    baseline,
    workers,
    byDate,
  };
}
