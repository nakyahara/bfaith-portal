/**
 * 出荷フロアボード (43インチ掲示モニター) のデータ集約 — /apps/picking/api/floor。
 *
 * 目的 (Codex設計議論 2026-08-20): 「今日の出荷が時間内に終わるか」と「どこに手が要るか」を
 * 現場全員が3秒で判断できる画面。左 = 完了予測・工程進捗・要対応 (常時固定)、
 * 右 = ピッキング/梱包の実績ヒートマップ (30秒ローテーション)。
 *
 * データ源:
 *   - picking: 本体 (getPickingStats / getTodayProgress)
 *   - packing: pk_pack_* テーブルは picking.db に同居 (要件§7.3) のため、進捗・要対応・
 *     ライン状態は picking の DB ハンドルで直接読む (テーブル未作成環境は fail-soft で null)。
 *     実績ヒートマップだけは packing モジュールを動的 import する (standalone の
 *     PACKING_ENABLED=0 分離を尊重 — 梱包側の障害でボード全体を落とさない)
 *
 * 完了予測 (中原さん決定 2026-08-20):
 *   締切は便ごとに2本 — AES 16:00 / その他 (ヤマト・日本郵便) 16:30。
 *   予測 = 残り伝票数 ÷ 直近60分の完了ペース (単純移動平均)。分母 (本日総量) は
 *   日中の追加取込で動くため、毎回取込済みベースで再計算する。
 */
import { getDB, jstToday } from './db.js';
import { getPickingStats, getTodayProgress, displayWorkerName } from './service.js';

export const FLOOR_DEADLINES = {
  aes: process.env.FLOOR_DEADLINE_AES || '16:00',
  std: process.env.FLOOR_DEADLINE_STD || '16:30',
};
export const FLOOR_RATE_WINDOW_MIN = Number(process.env.FLOOR_RATE_WINDOW_MIN) || 60;
export const GROUP_LABELS = { aes: 'AES', std: 'ヤマト・日本郵便' };

/** 引当分類 → 締切グループ。AESだけ集荷が早い (16:00)。 */
export function deadlineGroupOf(hikiateClass) {
  return String(hikiateClass || '').includes('AES') ? 'aes' : 'std';
}

// 梱包機ラインの判定 (packing service.js の lineKindOf と同じ規則。
// packing を静的 import しないための複製 — 整合は test-floor.mjs で担保)
export function lineKindOfClass(hikiateClass) {
  const c = String(hikiateClass || '');
  if (c.includes('PAS-LINE')) return 'pas';
  if (c.includes('MELT-LINE')) return 'melt';
  return null;
}

/** 'HH:MM' (JST) → workDate 当日のエポックms。 */
export function deadlineToMs(workDate, hhmm) {
  return Date.parse(`${workDate}T${hhmm}:00+09:00`);
}

/**
 * 完了予測 (純関数)。remaining=0 → done / ペース未計測 → measuring。
 * @returns {{status: 'done'|'measuring'|'ok'|'late', etaMs, marginMin}}
 */
export function computeEta({ nowMs, remaining, recentDone, windowMin, deadlineMs }) {
  if (remaining <= 0) return { status: 'done', etaMs: null, marginMin: null };
  const rate = recentDone / Math.max(1, windowMin);   // 伝票/分
  if (rate <= 0) return { status: 'measuring', etaMs: null, marginMin: null };
  const etaMs = nowMs + (remaining / rate) * 60000;
  const marginMin = Math.round((deadlineMs - etaMs) / 60000);
  return { status: marginMin >= 0 ? 'ok' : 'late', etaMs, marginMin };
}

/**
 * 本日の梱包バッチのスナップショット (pk_pack_* 直読み)。
 * packing 未導入 (テーブル無し) は null。
 */
function loadPackBatches(db, workDate, cutoffIso) {
  try {
    return db.prepare(`
      SELECT b.id, b.pk_batch_id, b.status, b.slip_count, b.folder_name, b.worker,
        b.finished_at, b.match_status,
        COALESCE(pb.hikiate_class, '') AS cls,
        (SELECT COUNT(*) FROM pk_pack_slips s
          WHERE s.batch_id = b.id AND s.status IN ('pending','held')) AS remain_slips,
        (SELECT COUNT(*) FROM pk_pack_slips s
          WHERE s.batch_id = b.id AND s.status = 'done') AS done_slips,
        (SELECT COUNT(*) FROM pk_pack_slips s
          WHERE s.batch_id = b.id AND s.status = 'cancelled') AS cancelled_slips,
        (SELECT COUNT(*) FROM pk_pack_slips s
          WHERE s.batch_id = b.id AND s.status = 'done' AND s.done_at >= ?) AS recent_done
      FROM pk_pack_batches b
      LEFT JOIN pk_batches pb ON pb.id = b.pk_batch_id
      WHERE b.work_date = ? AND b.validity = 'valid' AND b.status != 'cancelled'
    `).all(cutoffIso, workDate);
  } catch {
    return null;
  }
}

/**
 * バッチ1件の 残り/完了/対象の伝票数。
 * cancelled は残数・完了・分母のどれにも入れない (完了扱いすると進捗が水増しされる —
 * Codexレビュー medium 2026-08-20)。
 * 梱包機ライン: 伝票単位の完了が刻まれない (SLIP_ONLY_EVENTS 不可) ため、
 * バッチ完了で対象伝票をまとめて計上する近似。
 */
export function packBatchCounts(b, cutoffIso) {
  const total = Math.max(0, b.slip_count - (b.cancelled_slips || 0));
  if (lineKindOfClass(b.cls) !== null) {
    const done = b.status === 'done';
    return {
      total,
      remaining: done ? 0 : total,
      recentDone: done && b.finished_at && b.finished_at >= cutoffIso ? total : 0,
      done: done ? total : 0,
    };
  }
  return {
    total,
    remaining: b.remain_slips,
    recentDone: b.recent_done,
    done: b.done_slips,
  };
}

/** 締切グループ別の完了予測。 */
function buildEta(packBatches, workDate, nowMs, cutoffIso) {
  if (!packBatches) return null;
  const agg = new Map([['aes', { remaining: 0, recentDone: 0 }], ['std', { remaining: 0, recentDone: 0 }]]);
  for (const b of packBatches) {
    const g = agg.get(deadlineGroupOf(b.cls));
    const c = packBatchCounts(b, cutoffIso);
    g.remaining += c.remaining;
    g.recentDone += c.recentDone;
  }
  const groups = [...agg.entries()].map(([key, g]) => {
    const dMs = deadlineToMs(workDate, FLOOR_DEADLINES[key]);
    return {
      key,
      label: GROUP_LABELS[key],
      deadline: FLOOR_DEADLINES[key],
      remaining: g.remaining,
      ...computeEta({ nowMs, remaining: g.remaining, recentDone: g.recentDone, windowMin: FLOOR_RATE_WINDOW_MIN, deadlineMs: dMs }),
    };
  });
  return {
    groups,
    overall: pickOverallEta(groups),
    remaining: groups.reduce((s, g) => s + g.remaining, 0),
    windowMin: FLOOR_RATE_WINDOW_MIN,
  };
}

/**
 * 全体表示に使うグループの選定 (純関数)。
 * 締切が便ごとに違うため「ETAが一番遅い」ではなく「余裕が一番小さい」を採る
 * (Codexレビュー medium 2026-08-20: AES 40分超過 と 他 20分超過 が同時に起きたとき、
 *  ETAの遅い方を選ぶと軽い方の20分が表に出て深刻な方が隠れる)。
 * late > measuring > ok > done の順に深刻とみなす。
 */
export function pickOverallEta(groups) {
  const minMargin = (list) => list.reduce((m, g) => (m == null || g.marginMin < m.marginMin ? g : m), null);
  const late = groups.filter((g) => g.status === 'late');
  if (late.length) { const g = minMargin(late); return { status: 'late', etaMs: g.etaMs, marginMin: g.marginMin }; }
  if (groups.some((g) => g.status === 'measuring')) return { status: 'measuring', etaMs: null, marginMin: null };
  const ok = groups.filter((g) => g.status === 'ok');
  if (ok.length) { const g = minMargin(ok); return { status: 'ok', etaMs: g.etaMs, marginMin: g.marginMin }; }
  return { status: 'done', etaMs: null, marginMin: null };
}

/** 出荷バッチの工程別本数 (Notionカンバンの5列を picking+packing の状態から導出)。 */
function buildFlow(db, workDate, packBatches) {
  const picks = db.prepare(`
    SELECT id, status FROM pk_batches
    WHERE work_date = ? AND validity = 'valid' AND status != 'cancelled' AND origin != 'repick'
  `).all(workDate);
  const pickIds = new Set(picks.map((p) => p.id));
  const packByPick = new Map();
  for (const b of packBatches ?? []) {
    if (b.pk_batch_id != null) packByPick.set(b.pk_batch_id, b);
  }
  const flow = { notStarted: 0, picking: 0, picked: 0, packing: 0, done: 0 };
  for (const p of picks) {
    const pack = packByPick.get(p.id);
    if (pack && (pack.status === 'packing' || pack.status === 'paused')) flow.packing++;
    else if (pack && pack.status === 'done') flow.done++;
    else if (p.status === 'done') flow.picked++;
    else if (p.status === 'picking' || p.status === 'paused') flow.picking++;
    else flow.notStarted++;
  }
  // picking に紐付かない梱包バッチ (no_picking / 別日突合) は梱包側の状態だけで数える
  for (const b of packBatches ?? []) {
    if (b.pk_batch_id != null && pickIds.has(b.pk_batch_id)) continue;
    if (b.status === 'packing' || b.status === 'paused') flow.packing++;
    else if (b.status === 'done') flow.done++;
    else flow.picked++;   // ready = 梱包待ち
  }
  flow.total = flow.notStarted + flow.picking + flow.picked + flow.packing + flow.done;
  return flow;
}

/** 最後の pause イベントからの経過分。イベントが無ければ null。 */
function pausedMinutes(db, table, batchId, nowMs) {
  try {
    const at = db.prepare(
      `SELECT at FROM ${table} WHERE batch_id = ? AND event = 'pause' ORDER BY id DESC LIMIT 1`
    ).get(batchId)?.at;
    return at ? Math.max(0, Math.round((nowMs - Date.parse(at)) / 60000)) : null;
  } catch {
    return null;
  }
}

// 要対応の重症度しきい値: 中断がこれを超えたら bad (置きっぱなしの検知が目的)
const ALERT_BAD_PAUSE_MIN = 20;

/** 要対応の一覧 (最大 max 件。bad → 経過分の降順)。 */
function buildAlerts(db, workDate, packBatches, nowMs, max = 6) {
  const alerts = [];
  const pausedPicks = db.prepare(`
    SELECT id, folder_name FROM pk_batches
    WHERE work_date = ? AND validity = 'valid' AND status = 'paused' AND origin != 'repick'
  `).all(workDate);
  for (const b of pausedPicks) {
    const min = pausedMinutes(db, 'pk_events', b.id, nowMs);
    alerts.push({
      severity: min != null && min >= ALERT_BAD_PAUSE_MIN ? 'bad' : 'warn',
      who: b.folder_name || `バッチ${b.id}`,
      what: `ピッキング中断${min != null ? ` ${min}分` : ''}`,
      min: min ?? 0,
    });
  }
  for (const b of packBatches ?? []) {
    if (b.status !== 'paused') continue;
    const min = pausedMinutes(db, 'pk_pack_events', b.id, nowMs);
    alerts.push({
      severity: min != null && min >= ALERT_BAD_PAUSE_MIN ? 'bad' : 'warn',
      who: b.folder_name || `梱包${b.id}`,
      what: `梱包中断${min != null ? ` ${min}分` : ''}`,
      min: min ?? 0,
    });
  }
  const mismatch = (packBatches ?? []).filter((b) => b.status === 'ready' && b.match_status === 'mismatch').length;
  if (mismatch > 0) {
    alerts.push({ severity: 'warn', who: '取込突合', what: `差分あり ${mismatch}バッチ (確認待ち)`, min: 0 });
  }
  try {
    // 取消済み・旧版 (invalid) バッチのミス候補は数えない (Codexレビュー medium 2巡目)
    const cand = db.prepare(`
      SELECT COUNT(*) c FROM pk_pack_incidents i
      JOIN pk_pack_batches b ON b.id = i.batch_id
      WHERE b.work_date = ? AND b.validity = 'valid' AND b.status != 'cancelled'
        AND i.status = 'candidate'
    `).get(workDate).c;
    if (cand > 0) alerts.push({ severity: 'warn', who: 'ミス記録', what: `候補 ${cand}件 確認待ち`, min: 0 });
  } catch { /* packing 未導入 */ }
  return alerts
    .sort((a, b) => (a.severity === b.severity ? b.min - a.min : (a.severity === 'bad' ? -1 : 1)))
    .slice(0, max)
    .map(({ severity, who, what }) => ({ severity, who, what }));
}

/** 今日の品質 (件数のみ。個人名は出さない — 誰のミスかは管理画面の仕事)。 */
function buildQuality(db, workDate) {
  const quality = { misConfirmed: null, misCandidate: null, shortages: 0 };
  // 取消済み・旧版 (invalid) バッチの分は数えない (Codexレビュー medium 2巡目:
  // 再取込やバッチ取消の後も件数が残ると、現場に不要な確認作業を促す)
  quality.shortages = db.prepare(`
    SELECT COUNT(*) c FROM pk_lines l
    JOIN pk_batches b ON b.id = l.batch_id
    WHERE b.work_date = ? AND b.validity = 'valid' AND b.status != 'cancelled'
      AND l.status = 'shortage'
  `).get(workDate).c;
  try {
    const rows = db.prepare(`
      SELECT i.status, COUNT(*) c FROM pk_pack_incidents i
      JOIN pk_pack_batches b ON b.id = i.batch_id
      WHERE b.work_date = ? AND b.validity = 'valid' AND b.status != 'cancelled'
        AND i.status IN ('confirmed','candidate')
      GROUP BY i.status
    `).all(workDate);
    quality.misConfirmed = 0;
    quality.misCandidate = 0;
    for (const r of rows) {
      if (r.status === 'confirmed') quality.misConfirmed = r.c;
      else quality.misCandidate = r.c;
    }
  } catch { /* packing 未導入 → null のまま (画面は「-」表示) */ }
  return quality;
}

/** 梱包機ラインの現在状態 (PAS/MELT 別枠帯 — 手梱包の表とは土俵が違う)。 */
function buildLineStrip(db, workDate) {
  let rows;
  try {
    rows = db.prepare(`
      SELECT r.batch_id, r.phase, r.started_at, r.finished_at,
        r.planned_count, r.final_count, r.manual_count, r.excluded_count,
        b.status, b.slip_count, COALESCE(pb.hikiate_class, '') AS cls
      FROM pk_pack_line_runs r
      JOIN pk_pack_batches b ON b.id = r.batch_id
      LEFT JOIN pk_batches pb ON pb.id = b.pk_batch_id
      WHERE b.work_date = ? AND b.validity = 'valid' AND b.status != 'cancelled'
      ORDER BY r.batch_id, r.phase DESC
    `).all(workDate);
  } catch {
    return null;
  }
  const byKind = { pas: [], melt: [] };
  const byBatch = new Map();
  for (const r of rows) {
    const kind = lineKindOfClass(r.cls);
    if (!kind) continue;
    if (!byBatch.has(r.batch_id)) {
      const entry = { kind, status: r.status, slipCount: r.slip_count, sort: null, run: null };
      byBatch.set(r.batch_id, entry);
      byKind[kind].push(entry);
    }
    byBatch.get(r.batch_id)[r.phase] = r;
  }
  const stripOf = (kind) => {
    const list = byKind[kind];
    // 優先順: 機械流し中 > (MELT) 仕分け完了で流し待ち > 仕分け中 > 本日完了 > 未着手
    const running = list.find((e) => e.run?.started_at && !e.run?.finished_at);
    if (running) {
      return {
        state: 'running',
        planned: running.run.planned_count ?? running.sort?.final_count ?? running.slipCount,
        doneToday: dailyTotal(list),
      };
    }
    const sorted = list.find((e) => e.sort?.finished_at && !e.run?.started_at && e.status !== 'done');
    if (sorted) {
      return { state: 'sorted', planned: sorted.sort.final_count ?? sorted.slipCount, doneToday: dailyTotal(list) };
    }
    const sorting = list.find((e) => e.sort?.started_at && !e.sort?.finished_at);
    if (sorting) {
      return { state: 'sorting', planned: sorting.sort.planned_count ?? sorting.slipCount, doneToday: dailyTotal(list) };
    }
    const total = dailyTotal(list);
    if (total > 0 && list.every((e) => e.status === 'done')) return { state: 'done', planned: null, doneToday: total };
    if (list.length > 0) return { state: 'idle', planned: null, doneToday: total };
    return null;   // 本日そのラインのバッチ無し
  };
  const dailyTotal = (list) => list.reduce((s, e) => s + (e.run?.final_count ?? 0), 0);
  return { pas: stripOf('pas'), melt: stripOf('melt') };
}

/** 統計を掲示用の共通形へ (pick: 明細 / pack: 伝票 — フィールド名の違いを吸収)。 */
function toBoardStats(stats, kind) {
  const cnt = kind === 'pick' ? 'lines' : 'slips';
  const spu = kind === 'pick' ? 'secPerLine' : 'secPerSlip';
  return {
    since: stats.since,
    until: stats.until,
    days: stats.days,
    minCount: kind === 'pick' ? stats.minLines : stats.minSlips,
    minClassCount: kind === 'pick' ? stats.minClassLines : stats.minClassSlips,
    outlierSec: stats.outlierSec,
    total: {
      count: stats.total[cnt],
      secPerUnit: stats.total[spu],
      excludedCount: stats.total.excluded[cnt],
    },
    // 掲示は明細数上位の分類のみ (既存 /api/board と同じ。全量は管理画面で見る)
    baseline: stats.baseline.slice(0, 12).map((c) => ({
      key: c.key,
      count: c[cnt],
      avgSec: c.avgSec,
      workerCount: c.workerCount,
      workers: c.workers.map((w) => ({
        worker: w.worker, name: w.name, count: w[cnt], secPerUnit: w[spu],
        index: w.index, provisional: w.provisional,
      })),
    })),
    workers: stats.workers.map((w) => ({
      worker: w.worker, name: w.name, count: w[cnt], secPerUnit: w[spu],
      index: w.index, provisional: w.provisional,
      shortages: w.shortages ?? null,
    })),
  };
}

// packing モジュールの動的 import (1回だけ・失敗したら次回また試す)。
// standalone は PACKING_ENABLED=0 で packing を切り離せる — その分離をボードでも壊さない
let packStatsModPromise = null;
async function packStatsMod() {
  if (!packStatsModPromise) {
    packStatsModPromise = import('../packing/stats.js').catch((e) => {
      packStatsModPromise = null;
      throw e;
    });
  }
  return packStatsModPromise;
}

/**
 * フロアボードの全データ。picking 本体以外は fail-soft (壊れた部分だけ null)。
 * @param {{now?: Date, days?: number}} opts days = ヒートマップの集計窓
 */
export async function getFloorData({ now = new Date(), days } = {}) {
  const db = getDB();
  const nowMs = now.getTime();
  const workDate = jstToday(now);
  const cutoffIso = new Date(nowMs - FLOOR_RATE_WINDOW_MIN * 60000).toISOString();

  // PACKING_ENABLED=0 = 梱包アプリを意図的に切り離した状態。ボードも読まない
  // (無効化の理由が migration 失敗などのとき、古い/壊れたデータを現況として出さない)
  const packingDisabled = process.env.PACKING_ENABLED === '0';
  const packBatches = packingDisabled ? null : loadPackBatches(db, workDate, cutoffIso);

  // 梱包の当日進捗 (伝票ベース・cancelled は分母から除外)
  let packProgress = null;
  let packActive = null;
  if (packBatches) {
    let done = 0;
    let total = 0;
    for (const b of packBatches) {
      const c = packBatchCounts(b, cutoffIso);
      total += c.total;
      done += c.done;
    }
    packProgress = { doneSlips: done, totalSlips: total, remainingSlips: Math.max(0, total - done) };
    packActive = packBatches
      .filter((b) => b.status === 'packing' || b.status === 'paused')
      .map((b) => ({
        folder: b.folder_name,
        name: displayWorkerName(b.worker),
        cls: b.cls,
        paused: b.status === 'paused',
        line: lineKindOfClass(b.cls),
      }));
  }

  // 梱包ヒートマップ (動的 import — 失敗しても他は出す)。
  // ⭐db を渡して picking の既存接続で読む (packing の getDB() は未初期化だと
  //   migration を実行する = 掲示端末のGETがDB書き込みになる。Codexレビュー high)
  let packStats = null;
  let packStatsError = null;
  if (!packingDisabled) {
    try {
      const mod = await packStatsMod();
      // until を明示する (デフォルトの jstToday() と now 起点の workDate を一致させる)
      packStats = toBoardStats(mod.getPackingStats({ until: workDate, db, ...(days ? { days } : {}) }), 'pack');
    } catch (e) {
      // 詳細 (テーブル名・パス等) はログのみ。画面には固定文言 (Codexレビュー low)
      console.error('[picking] floor: 梱包統計の取得に失敗', e);
      packStatsError = '梱包実績を取得できません';
    }
  }

  return {
    ok: true,
    now: now.toISOString(),
    workDate,
    deadlines: FLOOR_DEADLINES,
    picking: {
      today: getTodayProgress(workDate),
      stats: toBoardStats(getPickingStats({ until: workDate, ...(days ? { days } : {}) }), 'pick'),
    },
    packing: packBatches ? {
      progress: packProgress,
      active: packActive,
      stats: packStats,
      statsError: packStatsError,
      lines: buildLineStrip(db, workDate),
    } : null,
    flow: buildFlow(db, workDate, packBatches),
    eta: buildEta(packBatches, workDate, nowMs, cutoffIso),
    alerts: buildAlerts(db, workDate, packBatches, nowMs),
    quality: buildQuality(db, workDate),
  };
}
