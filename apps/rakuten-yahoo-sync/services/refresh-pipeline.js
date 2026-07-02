/**
 * 再設計 R4: 「全部更新」パイプライン。
 *
 * 手動ボタン連打だった日次運用を 1 本に直列化する:
 *   1. full_sync      : Yahoo baseline → 楽天 diff → 候補 upsert (+title backfill)  … rys-full-sync.js
 *   2. genre_backfill : 楽天 genre_id 取りこぼし埋め (カテゴリ自動解決の前提)        … rakuten-title-backfill.js
 *   3. notion_pages   : Notion 未登録 SKU に page 自動作成                            … notion-create-page.js
 *   4. draft_seed     : Notion 空欄 (タイトル/売価/税率) を楽天から自動下書き         … notion-draft-seed.js
 *   5. notion_sync    : Notion → notion_overrides full sync (sync-lock 下)            … notion-sync.js
 *
 * 設計原則 (Codex R4-R1 High ×4 反映):
 *   - 排他: refresh_runs は partial UNIQUE INDEX (status='running') で running 1 行を DB 制約で保証。
 *     SELECT→INSERT の race でも 2 本目の INSERT が SQLITE_CONSTRAINT で落ちて 409。
 *   - owner CAS: 全 UPDATE (進捗/finalize) は run_token 一致 + status='running' を要求。
 *     lease 切れで steal された古い run は LEASE_LOST で停止し、 新 run の記録を上書きしない。
 *     各ステップ完了時に lease を延長する (長時間 run でも生存中は steal されない)。
 *   - fail-closed: ステップ内部のエラー (RMS fetch 失敗 / Notion PATCH 失敗 / sync row error) も
 *     成功扱いにせず pipeline failure に倒す。 各ステップは「不足分だけ処理する」semantics
 *     なので、 失敗後の単純再実行で続きから相当になる。
 *   - notion_sync は手動 /api/notion/sync と同じ sync-lock (file lock) を取る。
 *
 * 実行 API:
 *   startRefreshRun(db, {triggeredBy}) → { runId, runToken }   (同期、 排他ここで確定)
 *   executeRefreshPipeline({db, runId, runToken, triggeredBy}) → { runId, status, steps }
 */

import crypto from 'node:crypto';

import { runRysFullSync } from './rys-full-sync.js';
import { backfillRakutenGenre, countMissingRakutenGenre } from '../lib/rakuten-title-backfill.js';
import { createNotionPagesFromRakuten } from './notion-create-page.js';
import { seedNotionDrafts } from './notion-draft-seed.js';
import { syncNotionOverrides } from './notion-sync.js';
import { acquire, SyncLockError, getNotionSyncLockPath } from '../lib/sync-lock.js';

const LEASE_MS = 90 * 60 * 1000;          // ステップごとに延長するので「1 ステップの最大想定時間」
const GENRE_BACKFILL_MAX_ROUNDS = 10;     // 100 件/round × 10 = 最大 1,000 genre/run
const NOTION_PAGES_MAX_ROUNDS = 5;        // 100 件/round × 5
const DRAFT_SEED_MAX_ROUNDS = 3;          // 200 件/round × 3
const STEP_NAMES = ['full_sync', 'genre_backfill', 'notion_pages', 'draft_seed', 'notion_sync'];

function isoNow() { return new Date().toISOString(); }
function leaseFromNow() { return new Date(Date.now() + LEASE_MS).toISOString(); }

/** lease 切れ running を failed に倒す (steal)。 */
function stealStaleRuns(db) {
  db.prepare(`
    UPDATE refresh_runs
    SET status = 'failed', finished_at = ?, error_message = 'stale lease steal (process restart or hang?)'
    WHERE status = 'running' AND (lease_expires_at IS NULL OR lease_expires_at < ?)
  `).run(isoNow(), isoNow());
}

/** running で lease 生存中の run (排他表示用)。 */
export function findActiveRefreshRun(db) {
  try {
    stealStaleRuns(db);
    return db.prepare(`
      SELECT run_id, started_at, current_step, triggered_by FROM refresh_runs
      WHERE status = 'running'
      ORDER BY run_id DESC LIMIT 1
    `).get() || null;
  } catch (_) {
    return null; // migration 022 未適用
  }
}

export function getRefreshRun(db, runId = null) {
  try {
    const cols = 'run_id, started_at, finished_at, status, triggered_by, current_step, steps_json, error_message';
    const row = runId
      ? db.prepare(`SELECT ${cols} FROM refresh_runs WHERE run_id = ?`).get(runId)
      : db.prepare(`SELECT ${cols} FROM refresh_runs ORDER BY run_id DESC LIMIT 1`).get();
    if (!row) return null;
    let steps = null;
    try { steps = row.steps_json ? JSON.parse(row.steps_json) : null; } catch (_) { /* 壊れ JSON は null */ }
    return { ...row, steps };
  } catch (_) {
    return null;
  }
}

/**
 * run 行を確保する (同期)。 排他は partial UNIQUE INDEX が保証。
 * @returns {{ runId: number, runToken: string }}
 * @throws statusCode=409 (既に running が生きている)
 */
export function startRefreshRun(db, { triggeredBy = 'manual' } = {}) {
  stealStaleRuns(db);
  const runToken = crypto.randomUUID();
  try {
    const ins = db.prepare(`
      INSERT INTO refresh_runs (triggered_by, status, current_step, run_token, lease_expires_at)
      VALUES (?, 'running', 'full_sync', ?, ?)
    `).run(triggeredBy, runToken, leaseFromNow());
    return { runId: Number(ins.lastInsertRowid), runToken };
  } catch (e) {
    if (/UNIQUE constraint failed/i.test(e.message || '')) {
      const active = db.prepare(`SELECT run_id, current_step FROM refresh_runs WHERE status = 'running' ORDER BY run_id DESC LIMIT 1`).get();
      const err = new Error(`refresh pipeline already running (run_id=${active?.run_id ?? '?'}, step=${active?.current_step ?? '?'})`);
      err.statusCode = 409;
      err.runId = active?.run_id ?? null;
      throw err;
    }
    throw e;
  }
}

/** owner CAS 付き UPDATE。 steal されていたら LEASE_LOST を throw (新 run の記録を守る)。 */
function updateRunCAS(db, runId, runToken, fields) {
  const sets = [];
  const params = [];
  for (const [k, v] of Object.entries(fields)) {
    sets.push(`${k} = ?`);
    params.push(v);
  }
  params.push(runId, runToken);
  const r = db.prepare(`
    UPDATE refresh_runs SET ${sets.join(', ')}
    WHERE run_id = ? AND run_token = ? AND status = 'running'
  `).run(...params);
  if (r.changes === 0) {
    const err = new Error(`refresh run ${runId}: lease lost (stolen by newer run?)`);
    err.code = 'LEASE_LOST';
    throw err;
  }
}

/**
 * パイプライン実行本体 (await で完走まで)。 startRefreshRun で取った runId/runToken 必須。
 * @returns {{ runId, status: 'success', steps }}
 * @throws 失敗時 (err.runId / err.failedStep 付き)。 LEASE_LOST 時は DB を触らず throw。
 */
export async function executeRefreshPipeline({ db, runId, runToken, triggeredBy = 'manual', deps = {} } = {}) {
  if (!runId || !runToken) throw new Error('executeRefreshPipeline: runId/runToken required (startRefreshRun を先に)');

  const steps = {};
  // 各ステップ完了時: 進捗保存 + lease 延長 (owner CAS)
  const persistSteps = (currentStep) => {
    updateRunCAS(db, runId, runToken, {
      steps_json: JSON.stringify(steps),
      current_step: currentStep,
      lease_expires_at: leaseFromNow(),
    });
  };
  const impl = {
    runRysFullSync, backfillRakutenGenre, countMissingRakutenGenre,
    createNotionPagesFromRakuten, seedNotionDrafts, syncNotionOverrides,
    acquireNotionSyncLock: () => acquire(getNotionSyncLockPath()),
    ...deps, // テスト注入用
  };

  try {
    // ── 1. full sync (Yahoo baseline → 楽天 diff → title backfill) ──
    {
      const t0 = Date.now();
      const r = await impl.runRysFullSync({ db, triggeredBy, triggerRequestId: `refresh-${runId}` });
      steps.full_sync = {
        ok: true, ms: Date.now() - t0,
        rakutenTotal: r.diff?.rakutenTotal, overlap: r.diff?.overlap,
        candidatesNew: r.diff?.newlyDetected, candidatesResolved: r.diff?.resolved,
        titleBackfilled: r.titleBackfill?.updated ?? null,
      };
      persistSteps('genre_backfill');
    }

    // ── 2. genre backfill (進捗がある限り小 batch ループ) ──
    {
      const t0 = Date.now();
      let updated = 0, rounds = 0, remaining = null;
      for (; rounds < GENRE_BACKFILL_MAX_ROUNDS; rounds++) {
        remaining = impl.countMissingRakutenGenre(db);
        if (remaining <= 0) break;
        const r = await impl.backfillRakutenGenre({ db, limit: 100 });
        updated += r.updated || 0;
        if ((r.updated || 0) === 0) break; // 進捗なし (楽天側に genre 無し等) → 打ち切り (エラーではない)
      }
      steps.genre_backfill = { ok: true, ms: Date.now() - t0, updated, rounds, remaining };
      persistSteps('notion_pages');
    }

    // ── 3. Notion 未登録 SKU に page 自動作成 ──
    //   Codex R4-R1/R2 High: createNotionPagesFromRakuten は失敗を throw せず
    //   { error } や行 outcome (precheck_failed / rakuten_fetch_failed / rakuten_itemName_missing /
    //   create_failed / lease_lost) で返すので、 許容 outcome の allowlist 以外は fail-closed。
    {
      const NOTION_PAGE_OK_OUTCOMES = new Set(['created', 'already_exists_in_notion', 'skip_locked']);
      const t0 = Date.now();
      let created = 0, rounds = 0, lastSummary = null;
      for (; rounds < NOTION_PAGES_MAX_ROUNDS; rounds++) {
        const r = await impl.createNotionPagesFromRakuten(db, { dryRun: false, limit: 100 });
        if (r.error) {
          throw new Error(`notion_pages: ${r.error} (created=${created} まで反映済み、 再実行で続きから)`);
        }
        const summary = (r.results || []).reduce((acc, x) => { acc[x.outcome] = (acc[x.outcome] || 0) + 1; return acc; }, {});
        lastSummary = summary;
        created += summary.created || 0;
        const badOutcomes = Object.entries(summary).filter(([k]) => !NOTION_PAGE_OK_OUTCOMES.has(k));
        if (badOutcomes.length > 0) {
          const detail = badOutcomes.map(([k, v]) => `${k}=${v}`).join(', ');
          throw new Error(`notion_pages: 行単位の失敗 ${detail} (created=${created} まで反映済み、 再実行で続きから)`);
        }
        if ((summary.created || 0) === 0) break;
      }
      steps.notion_pages = { ok: true, ms: Date.now() - t0, created, rounds, lastSummary };
      persistSteps('draft_seed');
    }

    // ── 4. Notion 空欄の自動下書き ──
    //   Codex R4-R1 High: PATCH 失敗 (errors) を成功扱いにしない。
    {
      const t0 = Date.now();
      let applied = 0, rounds = 0;
      for (; rounds < DRAFT_SEED_MAX_ROUNDS; rounds++) {
        const r = await impl.seedNotionDrafts({ db, dryRun: false, limit: 200 });
        applied += r.applied || 0;
        if ((r.errors || 0) > 0) {
          throw new Error(`draft_seed: ${r.errors} 件の Notion PATCH エラー (applied=${applied} まで反映済み)`);
        }
        if ((r.applied || 0) === 0) break;
      }
      steps.draft_seed = { ok: true, ms: Date.now() - t0, applied, errors: 0, rounds };
      persistSteps('notion_sync');
    }

    // ── 5. Notion full sync (手動 /api/notion/sync と同じ file lock 下で) ──
    //   Codex R4-R1 High: lock 無しだと手動 sync と削除検出・sync_state 更新が並列になる。
    {
      const t0 = Date.now();
      let release = null;
      try {
        try {
          release = impl.acquireNotionSyncLock();
        } catch (e) {
          if (e instanceof SyncLockError) {
            throw new Error(`notion_sync: 手動の Notion 取込が実行中のため中断 (${e.reason})。 完了後に再実行してください`);
          }
          throw e;
        }
        const r = await impl.syncNotionOverrides({ db, mode: 'full', dryRun: false });
        const rowErrors = (r.errors || []).length;
        if (rowErrors > 0) {
          // Codex R4-R1 High: 手動 endpoint の partial-fail (207 + audit failed) と同じく失敗に倒す
          throw new Error(`notion_sync: ${rowErrors} 行の取込エラー (inserted=${r.inserted}, updated=${r.updated} は反映済み)`);
        }
        steps.notion_sync = {
          ok: true, ms: Date.now() - t0,
          inserted: r.inserted, updated: r.updated, skipped: r.skipped, deleted: r.deleted,
          rowErrors: 0,
        };
      } finally {
        if (release) { try { release(); } catch (_) {} }
      }
    }

    updateRunCAS(db, runId, runToken, {
      status: 'success', finished_at: isoNow(), current_step: null,
      steps_json: JSON.stringify(steps),
    });
    return { runId, status: 'success', steps };
  } catch (e) {
    if (e.code === 'LEASE_LOST') {
      // steal 済み: DB は新 run の所有物なので触らず終了
      console.warn(`[refresh-pipeline] run ${runId}: ${e.message}`);
      throw e;
    }
    const failedAt = STEP_NAMES[Object.keys(steps).length] || 'unknown';
    steps[failedAt] = { ok: false, error: String(e.message || e).slice(0, 1000) };
    try {
      updateRunCAS(db, runId, runToken, {
        status: 'failed', finished_at: isoNow(), current_step: failedAt,
        steps_json: JSON.stringify(steps),
        error_message: String(e.message || e).slice(0, 4000),
      });
    } catch (casErr) {
      if (casErr.code !== 'LEASE_LOST') console.warn(`[refresh-pipeline] run ${runId} finalize失敗: ${casErr.message}`);
    }
    const err = new Error(`refresh pipeline failed at ${failedAt}: ${e.message}`);
    err.cause = e;
    err.runId = runId;
    err.failedStep = failedAt;
    throw err;
  }
}
