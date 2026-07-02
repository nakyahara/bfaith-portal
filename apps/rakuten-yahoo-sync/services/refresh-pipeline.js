/**
 * 再設計 R4: 「全部更新」パイプライン。
 *
 * 手動ボタン連打だった日次運用を 1 本に直列化する:
 *   1. full_sync      : Yahoo baseline → 楽天 diff → 候補 upsert (+title backfill)  … rys-full-sync.js
 *   2. genre_backfill : 楽天 genre_id 取りこぼし埋め (カテゴリ自動解決の前提)        … rakuten-title-backfill.js
 *   3. notion_pages   : Notion 未登録 SKU に page 自動作成                            … notion-create-page.js
 *   4. draft_seed     : Notion 空欄 (タイトル/売価/税率) を楽天から自動下書き         … notion-draft-seed.js
 *   5. notion_sync    : Notion → notion_overrides full sync                           … notion-sync.js
 *
 * 設計原則:
 *   - 各ステップの要約を refresh_runs.steps_json に逐次書く (UI polling で進捗が見える)
 *   - ステップ失敗は fail-closed: 以降のステップは走らせず status='failed'
 *     (途中までの steps_json は残る。 再実行すれば冪等に続きから相当の処理になる —
 *      各ステップは「不足分だけ処理する」semantics なので単純再実行で良い)
 *   - 同時実行は refresh_runs status='running' + lease で排他 (stale は steal)
 *   - 反復系 (backfill / pages / draft) は小 batch を進捗がある限りループ (上限あり)
 */

import { runRysFullSync } from './rys-full-sync.js';
import { backfillRakutenGenre, countMissingRakutenGenre } from '../lib/rakuten-title-backfill.js';
import { createNotionPagesFromRakuten } from './notion-create-page.js';
import { seedNotionDrafts } from './notion-draft-seed.js';
import { syncNotionOverrides } from './notion-sync.js';

const LEASE_MS = 90 * 60 * 1000;          // パイプライン全体の lease (full_sync 単体より長め)
const GENRE_BACKFILL_MAX_ROUNDS = 10;     // 100 件/round × 10 = 最大 1,000 genre/run
const NOTION_PAGES_MAX_ROUNDS = 5;        // 100 件/round × 5
const DRAFT_SEED_MAX_ROUNDS = 3;          // 200 件/round × 3

function isoNow() { return new Date().toISOString(); }

/** running で lease 生存中の run があるか。 stale (lease 切れ running) は failed に倒す。 */
export function findActiveRefreshRun(db) {
  try {
    db.prepare(`
      UPDATE refresh_runs
      SET status = 'failed', finished_at = ?, error_message = 'stale lease steal (process restart?)'
      WHERE status = 'running' AND (lease_expires_at IS NULL OR lease_expires_at < ?)
    `).run(isoNow(), isoNow());
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
    const row = runId
      ? db.prepare('SELECT * FROM refresh_runs WHERE run_id = ?').get(runId)
      : db.prepare('SELECT * FROM refresh_runs ORDER BY run_id DESC LIMIT 1').get();
    if (!row) return null;
    let steps = null;
    try { steps = row.steps_json ? JSON.parse(row.steps_json) : null; } catch (_) { /* 壊れ JSON は null */ }
    return { ...row, steps };
  } catch (_) {
    return null;
  }
}

function updateRun(db, runId, fields) {
  const sets = [];
  const params = [];
  for (const [k, v] of Object.entries(fields)) {
    sets.push(`${k} = ?`);
    params.push(v);
  }
  params.push(runId);
  db.prepare(`UPDATE refresh_runs SET ${sets.join(', ')} WHERE run_id = ?`).run(...params);
}

/**
 * パイプライン実行本体 (await で完走まで)。 endpoint からは background 起動を推奨。
 * @returns {{ runId, status, steps }}
 * @throws 排他 (別 run 実行中) は statusCode=409 の Error
 */
export async function runRefreshPipeline({ db, triggeredBy = 'manual', deps = {} } = {}) {
  const active = findActiveRefreshRun(db);
  if (active) {
    const err = new Error(`refresh pipeline already running (run_id=${active.run_id}, step=${active.current_step})`);
    err.statusCode = 409;
    err.runId = active.run_id;
    throw err;
  }
  const ins = db.prepare(`
    INSERT INTO refresh_runs (triggered_by, status, current_step, lease_expires_at)
    VALUES (?, 'running', 'full_sync', ?)
  `).run(triggeredBy, new Date(Date.now() + LEASE_MS).toISOString());
  const runId = Number(ins.lastInsertRowid);

  const steps = {};
  const persistSteps = (currentStep) => {
    updateRun(db, runId, { steps_json: JSON.stringify(steps), current_step: currentStep });
  };
  const impl = {
    runRysFullSync, backfillRakutenGenre, countMissingRakutenGenre,
    createNotionPagesFromRakuten, seedNotionDrafts, syncNotionOverrides,
    ...deps, // テスト注入用
  };

  try {
    // ── 1. full sync (Yahoo baseline → 楽天 diff → title backfill) ──
    {
      const t0 = Date.now();
      const r = await impl.runRysFullSync({ db, triggeredBy: 'cron', triggerRequestId: `refresh-${runId}` });
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
        if ((r.updated || 0) === 0) break; // 進捗なし (楽天側に genre 無し等) → 打ち切り
      }
      steps.genre_backfill = { ok: true, ms: Date.now() - t0, updated, rounds, remaining };
      persistSteps('notion_pages');
    }

    // ── 3. Notion 未登録 SKU に page 自動作成 ──
    {
      const t0 = Date.now();
      let created = 0, rounds = 0, lastSummary = null;
      for (; rounds < NOTION_PAGES_MAX_ROUNDS; rounds++) {
        const r = await impl.createNotionPagesFromRakuten(db, { dryRun: false, limit: 100 });
        const summary = (r.results || []).reduce((acc, x) => { acc[x.outcome] = (acc[x.outcome] || 0) + 1; return acc; }, {});
        lastSummary = summary;
        created += summary.created || 0;
        if ((summary.created || 0) === 0) break;
      }
      steps.notion_pages = { ok: true, ms: Date.now() - t0, created, rounds, lastSummary };
      persistSteps('draft_seed');
    }

    // ── 4. Notion 空欄の自動下書き ──
    {
      const t0 = Date.now();
      let applied = 0, errors = 0, rounds = 0;
      for (; rounds < DRAFT_SEED_MAX_ROUNDS; rounds++) {
        const r = await impl.seedNotionDrafts({ db, dryRun: false, limit: 200 });
        applied += r.applied || 0;
        errors += r.errors || 0;
        if ((r.applied || 0) === 0) break;
      }
      steps.draft_seed = { ok: true, ms: Date.now() - t0, applied, errors, rounds };
      persistSteps('notion_sync');
    }

    // ── 5. Notion full sync ──
    {
      const t0 = Date.now();
      const r = await impl.syncNotionOverrides({ db, mode: 'full', dryRun: false });
      steps.notion_sync = {
        ok: true, ms: Date.now() - t0,
        inserted: r.inserted, updated: r.updated, skipped: r.skipped, deleted: r.deleted,
        rowErrors: (r.errors || []).length,
      };
      persistSteps(null);
    }

    updateRun(db, runId, {
      status: 'success', finished_at: isoNow(), current_step: null,
      steps_json: JSON.stringify(steps),
    });
    return { runId, status: 'success', steps };
  } catch (e) {
    const failedStep = Object.keys(steps).length; // 完了済ステップ数 = 失敗したのは次
    const stepNames = ['full_sync', 'genre_backfill', 'notion_pages', 'draft_seed', 'notion_sync'];
    const failedAt = stepNames[failedStep] || 'unknown';
    steps[failedAt] = { ok: false, error: String(e.message || e).slice(0, 1000) };
    try {
      updateRun(db, runId, {
        status: 'failed', finished_at: isoNow(), current_step: failedAt,
        steps_json: JSON.stringify(steps),
        error_message: String(e.message || e).slice(0, 4000),
      });
    } catch (_) { /* best-effort */ }
    const err = new Error(`refresh pipeline failed at ${failedAt}: ${e.message}`);
    err.cause = e;
    err.runId = runId;
    err.failedStep = failedAt;
    throw err;
  }
}
