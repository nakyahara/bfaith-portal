/**
 * Phase E-7-b: 楽天 ↔ Yahoo 差分検出 (Codex R1/R2 確定)。
 *
 * ローカル RYS src/services/diff-detector.js の ES module 翻訳 + bfaith-portal 環境に合わせて改修。
 *
 * 提供:
 *   - detectMigrationCandidates({ db, rakutenItems, triggeredBy, ... })
 *
 * フロー:
 *   1. Yahoo baseline guard (assertYahooBaselineComplete) で fail-closed
 *   2. sync_runs に running 行 INSERT (lease + run_token)、 同名 stale steal
 *   3. 楽天 itemSearch (miniPC proxy 経由) で itemNumber → manageNumber map 取得
 *   4. yahoo_registered_items から Yahoo 側商品 set 構築
 *   5. overlap + overlap_rate を計算、 fail-closed (overlap=0 / 前回成功比 50% 急減 / env 下限)
 *   6. 差分 (楽天有 ∩ Yahoo無) を candidates として migration_candidates に upsert
 *   7. 「楽天に居なくなった候補」 は missing_rakuten_count++、 連続 2 回 (env 設定可) で status='stale'
 *   8. 「Yahoo に出た候補」 は status='resolved' + resolved_at 記録
 *   9. sync_runs を success に flip (token check 経由)、 db.transaction で wrap
 *
 * 設計判断 (Codex R1/R2):
 *   - jobs テーブル再利用しない、 専用 migration_candidates (R1 最重要 1)
 *   - excluded は candidates の status に持たず migration_excluded を active JOIN (R2 B-1)
 *   - overlap=0 fail-closed (allowZeroOverlap=true 明示時のみ通す、 R2 E)
 *   - overlap_rate 急減 (前回成功比 50% 以下) で fail-closed (R2 B-3)
 *   - env RYS_MIN_OVERLAP_RATE で任意下限を追加可 (R2 B-3、 固定値は危険)
 *   - stale 判定は連続欠落 (default 2 回、 R2 Critical-5)
 */

import crypto from 'node:crypto';

import { fetchAllItemCodes } from './rakuten-rms-proxy.js';
import {
  assertYahooBaselineComplete,
  finishSyncRun,
  assertRunOwned,
  stealStaleRunningRuns,
} from './yahoo-store-sync.js';

// lease は yahoo_baseline と同じ感覚で 90 min + grace 15 min (実 fetch は数十秒、 余裕は十分)
const DEFAULT_RUN_LEASE_MS = 90 * 60 * 1000;
const DEFAULT_STALE_MISSING_THRESHOLD = 2;            // 連続欠落 N 回で stale flip
const OVERLAP_RATE_DROP_RATIO_THRESHOLD = 0.5;        // 前回比 50% 以下に下落 → fail-closed

function isoNow() {
  return new Date().toISOString();
}

function insertDiffSyncRun(db, { triggeredBy, triggerRequestId = null, startedAt, runLeaseExpiresAt, runToken, dryRun = false }) {
  const r = db.prepare(`
    INSERT INTO sync_runs (sync_name, started_at, status, triggered_by, trigger_request_id, run_lease_expires_at, run_token, dry_run)
    VALUES ('rakuten_diff', ?, 'running', ?, ?, ?, ?, ?)
  `).run(startedAt, triggeredBy, triggerRequestId, runLeaseExpiresAt, runToken, dryRun ? 1 : 0);
  return r.lastInsertRowid;
}

/**
 * Codex E-7-b R1 High-1: baseline guard と 「diff が読む Yahoo set」 を同じ complete snapshot に固定する。
 *   yahoo-store-sync は incomplete run でも yahoo_registered_items を更新 (monotonic upsert) するため、
 *   establish 後に走った incomplete sync の観測が diff に混入する可能性がある。
 *
 *   guard: 「最新 yahoo_baseline success の run_id が baseline_state.baseline_run_id と一致」 かつ
 *   「その run の establishes_baseline=1」 を要求。 そうでなければ 412 fail-closed。
 */
function assertYahooBaselineFixed(db) {
  const state = db.prepare('SELECT baseline_run_id FROM yahoo_baseline_state WHERE id = 1').get();
  if (!state) {
    const err = new Error('detectMigrationCandidates: yahoo_baseline_state missing (guard ordering issue)');
    err.statusCode = 412;
    throw err;
  }
  const latestSuccess = db.prepare(`
    SELECT id, establishes_baseline FROM sync_runs
    WHERE sync_name = 'yahoo_baseline' AND status = 'success'
    ORDER BY id DESC LIMIT 1
  `).get();
  if (!latestSuccess) {
    const err = new Error('detectMigrationCandidates: no successful yahoo_baseline sync_run');
    err.statusCode = 412;
    throw err;
  }
  // baseline 確立後に incomplete success が走ってる場合 → yahoo_registered_items snapshot が複雑化
  if (latestSuccess.id !== state.baseline_run_id) {
    const err = new Error(
      `detectMigrationCandidates: latest yahoo_baseline success (id=${latestSuccess.id}) is not the baseline_state.baseline_run_id (${state.baseline_run_id}). ` +
      `An incomplete sync ran after the last complete baseline — re-run a full syncYahooBaseline before diff.`
    );
    err.statusCode = 412;
    throw err;
  }
  if (latestSuccess.establishes_baseline !== 1) {
    const err = new Error(
      `detectMigrationCandidates: latest yahoo_baseline success run did not establish baseline (establishes_baseline=${latestSuccess.establishes_baseline}).`
    );
    err.statusCode = 412;
    throw err;
  }
}

/**
 * 楽天 itemNumber → manageNumber マップを Map に正規化。
 *   rakuten-rms-proxy fetchAllItemCodes は Object を返すので Map に変換。
 *   既に Map を渡された場合 (テスト) はそのまま返す。
 */
function normalizeRakutenItems(rakutenItems) {
  if (rakutenItems instanceof Map) return rakutenItems;
  if (rakutenItems && typeof rakutenItems === 'object') return new Map(Object.entries(rakutenItems));
  throw new Error(`detectMigrationCandidates: rakutenItems must be Map or plain object (got ${typeof rakutenItems})`);
}

/**
 * 楽天↔Yahoo 差分検出 1 run を実行する。
 *
 * @param {object} args
 * @param {Database} args.db
 * @param {Map<string,string>|object|null} [args.rakutenItems]   — テスト用注入。 null なら fetchAllItemCodes() で取得
 * @param {'cron'|'manual'} [args.triggeredBy]
 * @param {string|null} [args.triggerRequestId]
 * @param {boolean} [args.dryRun]                                — true なら writes なし、 stats のみ
 * @param {boolean} [args.allowZeroOverlap]                      — overlap=0 を許容 (本番運用では false 固定推奨)
 * @param {number}  [args.staleMissingThreshold]                 — 連続欠落 N 回で stale flip (default 2)
 * @returns {Promise<object>}
 */
async function detectMigrationCandidates({
  db,
  rakutenItems = null,
  triggeredBy = 'cron',
  triggerRequestId = null,
  dryRun = false,
  allowZeroOverlap = false,
  staleMissingThreshold = DEFAULT_STALE_MISSING_THRESHOLD,
  runLeaseMs = DEFAULT_RUN_LEASE_MS,
} = {}) {
  // 1a. Yahoo baseline guard (state-based、 fail-closed if incomplete/stale/override)
  assertYahooBaselineComplete(db);
  // 1b. Codex R1 High-1: baseline_state を読む snapshot と一致しているか追加チェック
  //     (yahoo-store-sync の incomplete run でも yahoo_registered_items は更新されるので、
  //      最新 yahoo_baseline success が必ず establish したものであることを要求する)
  assertYahooBaselineFixed(db);

  const runStartedAt = isoNow();
  const runToken = crypto.randomUUID();
  const runLeaseExpiresAt = new Date(Date.now() + runLeaseMs).toISOString();

  // 2. sync_runs row INSERT (dry_run フラグも記録 — Codex R1 High-2)
  stealStaleRunningRuns(db, 'rakuten_diff');
  const syncRunId = insertDiffSyncRun(db, {
    triggeredBy, triggerRequestId, startedAt: runStartedAt, runLeaseExpiresAt, runToken, dryRun,
  });

  try {
    // 3. 楽天 itemNumber → manageNumber マップ
    const rawRakuten = rakutenItems || (await fetchAllItemCodes());
    const rakuten = normalizeRakutenItems(rawRakuten);

    // Codex R2 High: 楽天 fetch 中に別の yahoo_baseline success が走った可能性 → SELECT 直前に再 guard。
    //   さらに writeTx 内でも 1 度 check (state 行が GC されてないことの保険)。
    assertYahooBaselineFixed(db);

    // 4. Yahoo 側商品 set (yahoo_registered_items から)
    const yahooCodes = new Set(
      db.prepare('SELECT item_code FROM yahoo_registered_items').all().map((r) => r.item_code)
    );

    // 5. overlap 計算
    let overlap = 0;
    for (const k of rakuten.keys()) if (yahooCodes.has(k)) overlap += 1;
    const overlapRate = rakuten.size > 0 ? overlap / rakuten.size : 0;

    // 5a. overlap=0 fail-closed (命名不一致ガード、 R2 E)
    if (!allowZeroOverlap && rakuten.size > 0 && overlap === 0) {
      throw new Error(
        `Rakuten↔Yahoo overlap=0 across ${rakuten.size} Rakuten items (yahoo_total=${yahooCodes.size}). ` +
        `itemNumber↔ItemCode naming mismatch suspected — fail-closed. ` +
        `Pass allowZeroOverlap:true if this is a genuine full new migration.`
      );
    }

    // 5b. overlap_rate 急減 fail-closed (前回成功比 50% 以下 → 命名/API/店舗ズレ疑い、 R2 B-3)
    //     Codex R1 High-2: dry_run の success は比較対象から除外 (汚染防止)
    const lastSuccessRow = db.prepare(`
      SELECT overlap_rate FROM sync_runs
      WHERE sync_name = 'rakuten_diff' AND status = 'success' AND overlap_rate IS NOT NULL
        AND COALESCE(dry_run, 0) = 0
      ORDER BY id DESC LIMIT 1
    `).get();
    if (lastSuccessRow && lastSuccessRow.overlap_rate > 0) {
      const dropRatio = overlapRate / lastSuccessRow.overlap_rate;
      if (dropRatio < OVERLAP_RATE_DROP_RATIO_THRESHOLD) {
        throw new Error(
          `overlap_rate dropped to ${overlapRate.toFixed(3)} from ${lastSuccessRow.overlap_rate.toFixed(3)} ` +
          `(ratio=${dropRatio.toFixed(2)} < ${OVERLAP_RATE_DROP_RATIO_THRESHOLD}) — fail-closed.`
        );
      }
    }

    // 5c. env 下限 (任意、 R2 B-3 「固定値は危険、 env で低めの下限のみ」)
    //     Codex R1 Medium-1: strict parse (Number、 0..1 range)、 invalid は throw (黙って無視しない)
    const envMinRaw = process.env.RYS_MIN_OVERLAP_RATE;
    if (envMinRaw !== undefined && envMinRaw.trim() !== '') {
      const envMin = Number(envMinRaw);
      if (!Number.isFinite(envMin) || envMin < 0 || envMin > 1) {
        throw new Error(
          `RYS_MIN_OVERLAP_RATE invalid value: "${envMinRaw}" (must be 0..1 finite number) — fail-closed`
        );
      }
      if (rakuten.size > 0 && overlapRate < envMin) {
        throw new Error(
          `overlap_rate ${overlapRate.toFixed(3)} < env RYS_MIN_OVERLAP_RATE=${envMin} — fail-closed.`
        );
      }
    }

    // 6. 候補抽出 (楽天有 ∩ Yahoo無)
    const candidatePairs = [];
    for (const [itemNumber, manageNumber] of rakuten) {
      if (!yahooCodes.has(itemNumber)) candidatePairs.push([itemNumber, manageNumber]);
    }

    if (dryRun) {
      // dry-run: stats のみ、 candidates 書き込まない
      try {
        finishSyncRun(db, syncRunId, runToken, {
          status: 'success',
          rakuten_total: rakuten.size,
          yahoo_total: yahooCodes.size,
          overlap,
          overlap_rate: overlapRate,
          candidates_new: 0,
          candidates_resolved: 0,
        });
      } catch (e) {
        if (e.code !== 'LEASE_LOST') throw e;
      }
      return {
        syncRunId,
        dryRun: true,
        rakutenTotal: rakuten.size,
        yahooTotal: yahooCodes.size,
        overlap,
        overlapRate,
        candidatesDetected: candidatePairs.length,
        newlyDetected: 0,
        alreadyKnown: 0,
        resolved: 0,
        staleFlipped: 0,
        missedThisRun: 0,
      };
    }

    // 7. 本書き込みは 1 transaction で wrap (Codex R3 High-1 同パターン)
    const writeTx = db.transaction(() => {
      assertRunOwned(db, syncRunId, runToken);
      // Codex R2 High: write 確定の直前にも baseline guard を再確認 (transaction 内で commit までに変動した場合の保険)
      assertYahooBaselineFixed(db);

      const candidatePairsSet = new Set(candidatePairs.map(([k]) => k));
      const rakutenSet = new Set(rakuten.keys());

      let newlyDetected = 0;
      let alreadyKnown = 0;
      let resolved = 0;
      let staleFlipped = 0;
      let missedThisRun = 0;

      // (a) 候補 upsert: 「楽天有 + Yahoo無」 を candidate にする。
      //     status=stale だった row が再度候補化したら status='candidate' に戻す + missing_rakuten_count=0。
      //     Codex R1 Medium-2: resolved row は何も触らない (status / その他列とも一切更新しない)。
      const insertCandidate = db.prepare(`
        INSERT INTO migration_candidates (
          item_code, rakuten_manage_number,
          first_detected_at, last_detected_at, last_seen_in_rakuten_at, last_checked_yahoo_baseline_at,
          missing_rakuten_count, stale_at, status, source
        )
        VALUES (
          @code, @mn,
          @ts, @ts, @ts, @ts,
          0, NULL, 'candidate', 'diff_detector'
        )
      `);
      const updateCandidate = db.prepare(`
        UPDATE migration_candidates
        SET rakuten_manage_number          = @mn,
            last_detected_at               = @ts,
            last_seen_in_rakuten_at        = @ts,
            last_checked_yahoo_baseline_at = @ts,
            missing_rakuten_count          = 0,
            stale_at                       = NULL,
            status                         = 'candidate'
        WHERE item_code = @code AND status IN ('candidate', 'stale')
      `);
      const selectStatus = db.prepare('SELECT status FROM migration_candidates WHERE item_code = ?');
      for (const [code, mn] of candidatePairs) {
        const before = selectStatus.get(code);
        if (!before) {
          insertCandidate.run({ code, mn, ts: runStartedAt });
          newlyDetected += 1;
        } else if (before.status === 'resolved') {
          // resolved は触らない (一貫した terminal ルール)
          continue;
        } else {
          updateCandidate.run({ code, mn, ts: runStartedAt });
          alreadyKnown += 1;
        }
      }

      // (b) Yahoo に出た候補 → resolved (yahooCodes に存在する candidates)
      //     migration_candidates の candidate or stale で yahooCodes に含まれる → resolved
      const candidateRowsForResolve = db.prepare(`
        SELECT item_code FROM migration_candidates
        WHERE status IN ('candidate', 'stale')
      `).all();
      const resolveStmt = db.prepare(`
        UPDATE migration_candidates
        SET status = 'resolved', resolved_at = @ts, last_checked_yahoo_baseline_at = @ts
        WHERE item_code = @code AND status IN ('candidate', 'stale')
      `);
      for (const row of candidateRowsForResolve) {
        if (yahooCodes.has(row.item_code)) {
          resolveStmt.run({ code: row.item_code, ts: runStartedAt });
          resolved += 1;
        }
      }

      // (c) 「楽天に居なくなった候補」 = candidate のうち、 今回 rakutenSet に居ない & yahoo にも居ない
      //     missing_rakuten_count++、 閾値で stale flip
      const candidateRowsForStale = db.prepare(`
        SELECT item_code, missing_rakuten_count FROM migration_candidates
        WHERE status = 'candidate'
      `).all();
      const incMissing = db.prepare(`
        UPDATE migration_candidates
        SET missing_rakuten_count = missing_rakuten_count + 1,
            last_checked_yahoo_baseline_at = @ts
        WHERE item_code = @code AND status = 'candidate'
      `);
      const flipStale = db.prepare(`
        UPDATE migration_candidates
        SET status = 'stale', stale_at = @ts
        WHERE item_code = @code AND status = 'candidate'
      `);
      for (const row of candidateRowsForStale) {
        if (!rakutenSet.has(row.item_code)) {
          incMissing.run({ code: row.item_code, ts: runStartedAt });
          missedThisRun += 1;
          const nextMissing = row.missing_rakuten_count + 1;
          if (nextMissing >= staleMissingThreshold) {
            flipStale.run({ code: row.item_code, ts: runStartedAt });
            staleFlipped += 1;
          }
        }
      }

      // 完了直前 lease 再確認 + sync_run finish
      assertRunOwned(db, syncRunId, runToken);
      finishSyncRun(db, syncRunId, runToken, {
        status: 'success',
        rakuten_total: rakuten.size,
        yahoo_total: yahooCodes.size,
        overlap,
        overlap_rate: overlapRate,
        candidates_new: newlyDetected,
        candidates_resolved: resolved,
      });

      return { newlyDetected, alreadyKnown, resolved, staleFlipped, missedThisRun };
    });

    const { newlyDetected, alreadyKnown, resolved, staleFlipped, missedThisRun } = writeTx();

    return {
      syncRunId,
      dryRun: false,
      rakutenTotal: rakuten.size,
      yahooTotal: yahooCodes.size,
      overlap,
      overlapRate,
      candidatesDetected: candidatePairs.length,
      newlyDetected,
      alreadyKnown,
      resolved,
      staleFlipped,
      missedThisRun,
    };
  } catch (e) {
    // LEASE_LOST 時は他 run に任せて元 throw を継続
    try {
      finishSyncRun(db, syncRunId, runToken, {
        status: 'failed',
        error_message: String(e.message || e).slice(0, 4000),
      });
    } catch (e2) {
      if (e2.code !== 'LEASE_LOST') throw e2;
    }
    throw e;
  }
}

export {
  DEFAULT_RUN_LEASE_MS,
  DEFAULT_STALE_MISSING_THRESHOLD,
  OVERLAP_RATE_DROP_RATIO_THRESHOLD,
  detectMigrationCandidates,
  // testing
  normalizeRakutenItems,
};
