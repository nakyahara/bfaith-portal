/**
 * RakutenYahooSync (RYS) — bfaith-portal app router (Phase E-2: Notion sync 追加)
 *
 * 設計原則 (Codex Phase E R3/R4 確定):
 *   - 楽天 RMS は miniPC proxy 経由 (E-3 以降で実装)
 *   - Notion は Render 直接 (RYS_NOTION_TOKEN)
 *   - secret 値は UI / DB / log に出さない
 *   - RYS state は 専用 SQLite (rakuten-yahoo-sync.db)
 *   - 実 publish は RYS_PUBLISH_ENABLED=0 default
 *
 * E-2 で追加:
 *   - GET  /api/notion/sync/status  : sync_state + readiness summary
 *   - POST /api/notion/sync          : 手動 sync 実行 (body: {dryRun?, mode?})
 */

import { Router } from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import { inspectEnvStatus } from './env-check.js';
import { getDB } from './db.js';
import { acquire, SyncLockError } from './lib/sync-lock.js';
import { syncNotionOverrides } from './services/notion-sync.js';
import { evaluateItemForPublish } from './services/publish-pipeline.js';
import { fetchAllItemCodes, fetchItemDetail } from './lib/rakuten-rms-proxy.js';
import { executePublish, isPublishEnabled, buildIdempotencyKey } from './services/publish-executor.js';
import { translateReason, summarizeReasons, categorizeReason } from './lib/reason-translator.js';
import {
  excludeMigrationItem,
  restoreMigrationExclusion,
  changeExclusionKind,
  listActiveExclusions,
  listExclusionHistory,
  countActiveByKind,
  kindLabel,
} from './lib/exclusion.js';
import { runRysFullSync } from './services/rys-full-sync.js';
import {
  startBulkPublish,
  listFailedItemCodes,
  getBatchStatus,
  getBatchItems,
  getRunningBatch,
} from './services/bulk-publish.js';
import {
  listAllPatterns,
  getCustomPatternStrings,
  addPattern,
  removePattern,
} from './lib/image-exclusion-patterns.js';
import { filterUploadableImageUrlsDetailed } from './lib/yahoo-image.js';
import { backfillRakutenTitles, countMissingRakutenTitles } from './lib/rakuten-title-backfill.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = Router();

function renderView(res, viewName, data = {}) {
  res.render(path.join(__dirname, 'views', viewName), data);
}

function getLockPath() {
  const dataDir = process.env.DATA_DIR || path.join(__dirname, '..', '..', 'data');
  return path.resolve(dataDir, 'rakuten-yahoo-sync.notion-sync.lock');
}

function audit(db, action, detail, { actor = 'http', result = 'success', errorMessage = null } = {}) {
  try {
    db.prepare(`
      INSERT INTO audit_log (actor, action, after_json, result, error_message)
      VALUES (?, ?, ?, ?, ?)
    `).run(actor, action, detail ? JSON.stringify(detail) : null, result, errorMessage);
  } catch (_) { /* best-effort */ }
}

function getPublishSummary(db) {
  let rows;
  try {
    rows = db.prepare(`
      SELECT status, COUNT(*) AS n FROM publish_idempotency GROUP BY status
    `).all();
  } catch (_) {
    return null; // table 未作成 (migration 003 未適用)
  }
  const out = { in_progress: 0, success: 0, failed: 0, not_implemented: 0, total: 0 };
  for (const r of rows) {
    if (Object.prototype.hasOwnProperty.call(out, r.status)) out[r.status] = r.n;
    out.total += r.n;
  }
  return out;
}

function getReadinessSummary(db) {
  const rows = db.prepare(`
    SELECT readiness_status, COUNT(*) AS n FROM jobs GROUP BY readiness_status
  `).all();
  const summary = { pending: 0, ok: 0, blocked: 0, total: 0 };
  for (const r of rows) {
    if (Object.prototype.hasOwnProperty.call(summary, r.readiness_status)) {
      summary[r.readiness_status] = r.n;
    }
    summary.total += r.n;
  }
  return summary;
}

function getNotionOverrideStats(db) {
  return db.prepare(`
    SELECT COUNT(*) AS total,
           SUM(CASE WHEN yahoo_title IS NOT NULL THEN 1 ELSE 0 END) AS with_title,
           SUM(CASE WHEN yahoo_price IS NOT NULL THEN 1 ELSE 0 END) AS with_price,
           SUM(CASE WHEN notion_delivery_label IS NOT NULL THEN 1 ELSE 0 END) AS with_delivery
      FROM notion_overrides
  `).get();
}

function getSyncState(db) {
  return db.prepare(`SELECT * FROM sync_state WHERE source = 'notion_overrides'`).get() || null;
}

/**
 * Phase E-6 UI 再設計: 商品リスト用データ構築。
 *   notion_overrides + publish_idempotency + jobs を JOIN し、
 *   業務担当者向けの「商品 1 件 = カード 1 枚」 形式で返す。
 *
 *   status を 4 種に簡易分類:
 *     - done       : publish_idempotency.status='success'
 *     - actionable : Notion 必須項目 (yahoo_title / yahoo_price / notion_delivery_label / notion_tax_rate) が
 *                    全部入っている (≒ 移行できる、 厳密な readiness 評価は publish 時に行う安全弁あり)
 *     - fixable    : Notion 必須項目に欠けがある (= 修正必要)
 *     - unknown    : それ以外 (基本ない、 保険)
 */
function listProductsForUI(db, { filter = 'all', search = '' } = {}) {
  // Codex E-7 R1/R2 確定:
  //   - 起点 = migration_candidates (楽天有 ∩ Yahoo無)
  //   - JOIN: notion_overrides は **rakuten_manage_number** で結合 (R2 Critical D-0)
  //   - migration_excluded は restored_at IS NULL の active のみ
  //   - publish_idempotency は 「一度でも success」 を success 優先で集約 (R2 Critical D-1)
  //   - yahoo_registered_items に居れば done 扱い (Yahoo 実存ベース)
  //   - タブ precedence: excluded > done > actionable > fixable > stale (R2 Critical D-2)
  //   - 「Notion override あるが候補にない」 は別ビュー orphan_overrides (R1 ⑥確定)
  const candidateRows = db.prepare(`
    SELECT
      c.item_code,
      c.rakuten_manage_number,
      c.rakuten_title,
      c.status                          AS candidate_status,
      c.first_detected_at,
      c.last_detected_at,
      c.missing_rakuten_count,
      c.stale_at,
      no.notion_page_id,
      no.yahoo_title,
      no.yahoo_price,
      no.yahoo_price_sagawa,
      no.notion_delivery_label,
      no.notion_tax_rate,
      no.notion_status,
      no.synced_at,
      j.readiness_status,
      j.readiness_blocked_reasons,
      j.last_readiness_at,
      e.id              AS exclusion_id,
      e.exclude_kind    AS exclusion_kind,
      e.reason          AS exclusion_reason,
      e.excluded_at     AS exclusion_at,
      pi.status         AS publish_status,
      pi.completed_at   AS publish_completed_at,
      pi.error_message  AS publish_error,
      r.yahoo_item_code AS yahoo_observed_code
    FROM migration_candidates c
    LEFT JOIN notion_overrides no
      ON no.rakuten_manage_number = COALESCE(c.rakuten_manage_number, c.item_code)
    LEFT JOIN jobs j
      ON j.item_code = c.item_code     -- Codex R1 High: jobs.item_code は楽天 itemNumber 前提 (manage_number ではない)、 候補の item_code と一致させる
    LEFT JOIN migration_excluded e
      ON e.item_code = c.item_code AND e.restored_at IS NULL
    LEFT JOIN (
      -- 一度でも success があれば success を優先 (E-6-1 と同じ集約 SQL を流用)
      SELECT item_code,
             COALESCE(
               MAX(CASE WHEN status='success' THEN status END),
               (SELECT status FROM publish_idempotency p2
                  WHERE p2.item_code = p1.item_code
                  ORDER BY p2.rowid DESC LIMIT 1)
             ) AS status,
             COALESCE(
               MAX(CASE WHEN status='success' THEN completed_at END),
               (SELECT completed_at FROM publish_idempotency p3
                  WHERE p3.item_code = p1.item_code
                  ORDER BY p3.rowid DESC LIMIT 1)
             ) AS completed_at,
             CASE WHEN MAX(CASE WHEN status='success' THEN 1 ELSE 0 END) = 1
                  THEN NULL
                  ELSE (SELECT error_message FROM publish_idempotency p4
                          WHERE p4.item_code = p1.item_code
                          ORDER BY p4.rowid DESC LIMIT 1)
             END AS error_message
      FROM publish_idempotency p1
      GROUP BY item_code
    ) pi ON pi.item_code = c.item_code
    LEFT JOIN yahoo_registered_items r
      ON r.item_code = c.item_code
    ORDER BY c.item_code
  `).all();

  // status 計算 + reason 翻訳 (Codex R2 D-2 precedence: excluded > done > actionable > fixable > stale)
  const products = candidateRows.map((r) => {
    const isExcluded = r.exclusion_id !== null && r.exclusion_id !== undefined;
    const isDone = (r.yahoo_observed_code !== null && r.yahoo_observed_code !== undefined)
                || r.publish_status === 'success';

    let status, reasons = [], notionFields = [];
    if (isExcluded) {
      status = 'excluded';
    } else if (isDone) {
      status = 'done';
    } else if (r.candidate_status === 'stale') {
      status = 'stale';
    } else {
      // candidate / resolved 状態だがまだ done でない → Notion 必須 4 項目で actionable/fixable 判定
      const missing = [];
      if (!r.yahoo_title)            missing.push('notion_title_missing');
      if (!r.yahoo_price)            missing.push('price_invalid_or_zero');
      if (!r.notion_delivery_label)  missing.push('delivery_mapping_unresolved');
      if (!r.notion_tax_rate)        missing.push('notion_tax_rate_missing');
      let existingReasons = [];
      if (r.readiness_blocked_reasons) {
        try { existingReasons = JSON.parse(r.readiness_blocked_reasons); } catch (_) {}
      }
      const allReasons = [...missing, ...existingReasons.filter((x) => !missing.includes(x))];
      if (allReasons.length === 0) {
        status = 'actionable';
      } else {
        status = 'fixable';
        const summarized = summarizeReasons(allReasons);
        reasons = summarized.items;
        notionFields = summarized.notionFields;
      }
    }

    return {
      itemCode:           r.item_code,
      rakutenManageNumber: r.rakuten_manage_number,
      rakutenTitle:       r.rakuten_title,
      candidateStatus:    r.candidate_status,
      notionPageId:       r.notion_page_id,
      yahooTitle:         r.yahoo_title,
      yahooPrice:         r.yahoo_price,
      sagawaPrice:        r.yahoo_price_sagawa,
      delivery:           r.notion_delivery_label,
      taxRate:            r.notion_tax_rate,
      notionStatus:       r.notion_status,
      syncedAt:           r.synced_at,
      exclusionKind:      r.exclusion_kind,
      exclusionReason:    r.exclusion_reason,
      exclusionAt:        r.exclusion_at,
      publishStatus:      r.publish_status,
      publishCompletedAt: r.publish_completed_at,
      publishError:       r.publish_error,
      yahooObservedCode:  r.yahoo_observed_code,
      isExcluded, isDone,
      status,
      reasons,
      notionFields,
      primaryReason: reasons[0]?.message || '',
    };
  });

  // 救済タブ用: notion_overrides にあるが migration_candidates に居ない商品 (R1 ⑥確定)
  //   候補生成と起点が違う = 楽天 RMS で消えた / 楽天に最初から居ない / Notion 先行登録
  const orphanRows = db.prepare(`
    SELECT
      no.rakuten_manage_number AS item_code,
      no.notion_page_id,
      no.yahoo_title,
      no.notion_status,
      no.synced_at
    FROM notion_overrides no
    LEFT JOIN migration_candidates c
      ON c.rakuten_manage_number = no.rakuten_manage_number
      OR c.item_code = no.rakuten_manage_number
    WHERE c.item_code IS NULL
    ORDER BY no.rakuten_manage_number
  `).all();
  const orphanProducts = orphanRows.map((r) => ({
    itemCode:     r.item_code,
    notionPageId: r.notion_page_id,
    yahooTitle:   r.yahoo_title,
    notionStatus: r.notion_status,
    syncedAt:     r.synced_at,
    status:       'orphan',
  }));

  // filter 適用
  let filtered = products;
  if (filter === 'actionable') filtered = products.filter((p) => p.status === 'actionable');
  else if (filter === 'fixable') filtered = products.filter((p) => p.status === 'fixable');
  else if (filter === 'done')   filtered = products.filter((p) => p.status === 'done');
  else if (filter === 'excluded') filtered = products.filter((p) => p.status === 'excluded');
  else if (filter === 'stale')  filtered = products.filter((p) => p.status === 'stale');
  else if (filter === 'orphan') filtered = orphanProducts;

  // 検索 (商品コード / タイトル 部分一致、 case-insensitive)
  const term = String(search || '').trim().toLowerCase();
  if (term) {
    filtered = filtered.filter((p) =>
      (p.itemCode && p.itemCode.toLowerCase().includes(term))
      || (p.yahooTitle && p.yahooTitle.toLowerCase().includes(term))
    );
  }

  // 集計 (excluded + stale + orphan も含めて全タブを表示)
  const summary = {
    total:      products.length,
    actionable: products.filter((p) => p.status === 'actionable').length,
    fixable:    products.filter((p) => p.status === 'fixable').length,
    done:       products.filter((p) => p.status === 'done').length,
    excluded:   products.filter((p) => p.status === 'excluded').length,
    stale:      products.filter((p) => p.status === 'stale').length,
    orphan:     orphanProducts.length,
  };

  // 不備種類別 (fixable のみ): 「売価未入力 45 件」 等
  const fixableByCategory = {};
  for (const p of products.filter((x) => x.status === 'fixable')) {
    const cats = new Set();
    for (const r of p.reasons) {
      // 元 raw reason を持ってないので、 notionField を bucket key として使う
      const label = r.notionField
        ? ({ 'Yahoo!タイトル': 'タイトル未入力',
              '売価': '売価未入力',
              '配送方法': '配送方法未設定',
              '税率': '税率の問題',
              'カテゴリ': 'カテゴリ未設定',
              '画像': '画像の問題',
              'バリエーション': 'バリエーションの問題',
              'バリエーション有無': 'バリエーションの問題',
              'バリエーション項目': 'バリエーションの問題',
            }[r.notionField] || 'その他')
        : 'その他';
      cats.add(label);
    }
    for (const c of cats) fixableByCategory[c] = (fixableByCategory[c] || 0) + 1;
  }

  return {
    products: filtered,
    summary,
    fixableByCategory,
  };
}

// Notion ページ URL 構築 helper (UI から「Notion で直す」 リンク)
function notionPageUrl(pageId) {
  if (!pageId) return null;
  const id = String(pageId).replace(/-/g, '');
  return `https://www.notion.so/${id}`;
}

// ───────────────── 画面 ─────────────────

router.get('/', (req, res) => {
  const status = inspectEnvStatus();
  let syncState = null;
  let publishSummary = null;
  let products = [];
  let summary = { total: 0, actionable: 0, fixable: 0, done: 0, excluded: 0, stale: 0, orphan: 0 };
  let fixableByCategory = {};
  let exclusionCount = { temporary: 0, permanent: 0, total: 0 };
  let rakutenTitleMissing = 0;
  const filter = ['all', 'actionable', 'fixable', 'done', 'excluded', 'stale', 'orphan'].includes(req.query.filter)
    ? req.query.filter
    : 'actionable'; // default は「すぐ移行できる」 = やるべきこと
  const search = String(req.query.q || '');

  try {
    const db = getDB();
    syncState = getSyncState(db);
    publishSummary = getPublishSummary(db);
    const listed = listProductsForUI(db, { filter, search });
    products = listed.products;
    summary = listed.summary;
    fixableByCategory = listed.fixableByCategory;
    exclusionCount = countActiveByKind(db);
    try { rakutenTitleMissing = countMissingRakutenTitles(db); } catch (_) { /* migration 011 未適用 */ }
  } catch (_) {
    // DB 未初期化等は空 state で表示 continue
  }

  // Notion sync 鮮度 (3 日以上前なら警告)
  let syncDaysAgo = null;
  if (syncState?.last_successful_sync_at) {
    const diff = Date.now() - new Date(syncState.last_successful_sync_at).getTime();
    syncDaysAgo = Math.floor(diff / (1000 * 60 * 60 * 24));
  }

  renderView(res, 'dashboard', {
    status,
    syncState,
    syncDaysAgo,
    publishSummary,
    publishEnabled: isPublishEnabled(),
    products,
    summary,
    fixableByCategory,
    exclusionCount,                                       // E-7-c: 移行除外管理 header 集計
    rakutenTitleMissing,                                  // E-9: 楽天タイトル backfill ボタンの未取得件数 badge
    filter,
    search,
    notionPageUrl,  // EJS から呼べるように
    kindLabel,                                            // EJS から英語 enum → 日本語ラベル変換
  });
});

// ───────────────── API ─────────────────

router.get('/api/health', (_req, res) => {
  res.json(inspectEnvStatus());
});

router.get('/api/notion/sync/status', (_req, res) => {
  try {
    const db = getDB();
    res.json({
      sync_state: getSyncState(db),
      notion_overrides: getNotionOverrideStats(db),
      readiness: getReadinessSummary(db),
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

router.post('/api/notion/sync', async (req, res) => {
  const body = req.body || {};
  const mode = body.mode || 'full';
  const dryRun = !!body.dryRun;

  // Codex E-2 R1 M-2: delta mode は実装未完なので 400 reject (since/cursor 未配線)
  if (mode === 'delta') {
    return res.status(400).json({
      status: 'fail',
      error: 'mode=delta is experimental and not yet implemented (since/cursor wiring pending)',
    });
  }

  const lockPath = getLockPath();
  let release;
  try {
    try {
      release = acquire(lockPath);
    } catch (e) {
      if (e instanceof SyncLockError) {
        return res.status(409).json({ status: 'skip-locked', reason: e.reason, message: e.message });
      }
      return res.status(500).json({ status: 'fail', stage: 'lock', error: e.message });
    }
    try {
      const db = getDB();
      const result = await syncNotionOverrides({ db, mode, dryRun });
      const errorCount = result.errors.length;
      // Codex E-2 R1 M-1: 行レベル errors > 0 は partial fail として 207 で返す + audit failed
      if (errorCount > 0) {
        audit(db, 'notion_sync_partial_fail', {
          mode, dryRun, runId: result.runId,
          inserted: result.inserted, updated: result.updated,
          skipped: result.skipped, deleted: result.deleted,
          errors: errorCount,
        }, { result: 'failed', errorMessage: `partial-fail: ${errorCount} row error(s)` });
        return res.status(207).json({ status: 'partial-fail', ...result });
      }
      audit(db, 'notion_sync', {
        mode, dryRun, runId: result.runId,
        inserted: result.inserted, updated: result.updated,
        skipped: result.skipped, deleted: result.deleted,
        errors: 0,
      });
      return res.json({ status: 'ok', ...result });
    } catch (e) {
      try {
        const db = getDB();
        audit(db, 'notion_sync_fail', { mode, error: e.message }, { result: 'failed', errorMessage: e.message });
      } catch (_) { /* best-effort */ }
      return res.status(500).json({ status: 'fail', error: e.message });
    }
  } finally {
    if (release) {
      try { release(); } catch (_) {}
    }
  }
});

// ───────────────── Phase E-4: publish dry-run ─────────────────

/**
 * 単商品の Phase 0 publish-pipeline を dry-run。
 *   body: {
 *     manageNumber?: string,        // 省略時は jobs.payload_json の rakuten_manage_number
 *     productCategory?: string,     // E-4 では未解決でも OK (readiness 側で fail-closed)
 *     pathName?: string,
 *     yahooProductCategoryId?: number,
 *     aucPrefCode?: number,
 *     dryRun?: boolean              // default true (E-4 では false 不可、 E-5 で flip)
 *   }
 */
router.post('/api/publish/evaluate/:itemCode', async (req, res) => {
  try {
    const itemCode = req.params.itemCode;
    const body = req.body || {};
    const dryRun = body.dryRun !== false;
    if (!dryRun) {
      return res.status(403).json({
        status: 'fail',
        error: 'Real publish not yet implemented (E-5). Only dryRun=true is allowed.',
      });
    }
    const db = getDB();
    const job = db.prepare(`SELECT payload_json FROM jobs WHERE item_code = ?`).get(itemCode);
    let payload = {};
    if (job?.payload_json) {
      try { payload = JSON.parse(job.payload_json); } catch (_) {}
    }
    const bodyMn = typeof body.manageNumber === 'string' ? body.manageNumber.trim() : '';
    const payloadMn = typeof payload.rakuten_manage_number === 'string' ? payload.rakuten_manage_number.trim() : '';
    // Codex E-4 R1 M-2: itemCode (= itemNumber) を manageNumber に誤用すると false blocked
    // が出るので、 fallback は itemNumber → manageNumber map で解決。
    let manageNumber = bodyMn || payloadMn || null;
    if (!manageNumber) {
      try {
        const mapping = await fetchAllItemCodes();
        manageNumber = mapping[itemCode] || null;
      } catch (e) {
        return res.status(502).json({
          status: 'fail',
          error: `failed to resolve manage_number from itemNumber via warehouse proxy: ${e.message}`,
        });
      }
    }
    if (!manageNumber) {
      return res.status(400).json({
        error: 'manage_number_required',
        itemCode,
        hint: 'pass body.manageNumber explicitly, set payload.rakuten_manage_number in jobs, or ensure warehouse all-codes map includes this itemNumber',
      });
    }
    const result = await evaluateItemForPublish({
      db,
      itemCode,
      manageNumber,
      dryRun: true,
      productCategory: body.productCategory || null,
      pathName: body.pathName || null,
      yahooProductCategoryId: body.yahooProductCategoryId ?? null,
      aucPrefCode: body.aucPrefCode ?? null,
    });
    audit(db, 'publish_evaluate', {
      itemCode, manageNumber, dryRun: true, status: result.status,
      reason_count: result.reasons.length,
    });
    return res.json(result);
  } catch (e) {
    return res.status(500).json({ status: 'fail', error: e.message });
  }
});

// ───────────────── Phase E-5a: 実 publish (placeholder) ─────────────────

/**
 * 実 publish 実行。 Phase E-5a では Yahoo API 呼び出し本体は未実装、 readiness pass + idempotency 監査のみ。
 *
 * body: {
 *   manageNumber?: string,
 *   idempotencyKey?: string,    // 省略時は executor が生成 (caller 指定推奨)
 *   productCategory?, pathName?, yahooProductCategoryId?, aucPrefCode?
 * }
 */
router.post('/api/publish/execute/:itemCode', async (req, res) => {
  try {
    // 受理時 dual check #1
    if (!isPublishEnabled()) {
      return res.status(403).json({
        status: 'fail',
        error: 'RYS_PUBLISH_ENABLED=0 (kill-switch). Set env to 1 to enable real publish.',
      });
    }
    const itemCode = req.params.itemCode;
    const body = req.body || {};
    const db = getDB();

    // manageNumber resolve (E-4 と同じロジック)
    const job = db.prepare(`SELECT payload_json FROM jobs WHERE item_code = ?`).get(itemCode);
    let payload = {};
    if (job?.payload_json) { try { payload = JSON.parse(job.payload_json); } catch (_) {} }
    const bodyMn = typeof body.manageNumber === 'string' ? body.manageNumber.trim() : '';
    const payloadMn = typeof payload.rakuten_manage_number === 'string' ? payload.rakuten_manage_number.trim() : '';
    let manageNumber = bodyMn || payloadMn || null;
    if (!manageNumber) {
      try {
        const mapping = await fetchAllItemCodes();
        manageNumber = mapping[itemCode] || null;
      } catch (e) {
        return res.status(502).json({ status: 'fail', error: `warehouse proxy: ${e.message}` });
      }
    }
    if (!manageNumber) {
      return res.status(400).json({ error: 'manage_number_required', itemCode });
    }

    const result = await executePublish({
      db, itemCode, manageNumber,
      createdBy: 'http',
      idempotencyKey: body.idempotencyKey || null,
      publishOpts: {
        productCategory: body.productCategory || null,
        pathName: body.pathName || null,
        yahooProductCategoryId: body.yahooProductCategoryId ?? null,
        aucPrefCode: body.aucPrefCode ?? null,
      },
    });

    // status マッピング (Codex E-5b R1 H-3: publish_failed / lease_lost 追加)
    if (result.status === 'in_progress_conflict') return res.status(409).json(result);
    if (result.status === 'dedupe') return res.status(200).json(result);
    if (result.status === 'readiness_blocked') return res.status(422).json(result);
    if (result.status === 'not_implemented') return res.status(501).json(result);
    if (result.status === 'flag_off') return res.status(403).json(result);
    if (result.status === 'publish_failed') return res.status(502).json(result);  // Yahoo / proxy 起因の publish 失敗
    if (result.status === 'lease_lost') return res.status(423).json(result);      // CAS 喪失 (locked / 競合)
    if (result.status === 'fail') return res.status(500).json(result);
    return res.json(result);
  } catch (e) {
    return res.status(500).json({ status: 'fail', error: e.message });
  }
});

// idempotency key を caller 側で確認できる helper
router.post('/api/publish/idempotency-key', (req, res) => {
  try {
    const { itemCode, manageNumber, scope, isoDate } = req.body || {};
    if (!itemCode) return res.status(400).json({ error: 'itemCode required' });
    const key = buildIdempotencyKey({ itemCode, manageNumber, scope, isoDate });
    return res.json({ idempotencyKey: key });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
});

// ───────────────── Phase E-6-2: 商品詳細ドロワー API ─────────────────

/**
 * 1 商品の full context を返す (drawer 表示用)。
 *   - candidate: migration_candidates の status + detected_at 等
 *   - notion:    notion_overrides の Yahoo!タイトル / 売価 / 配送方法 / 税率 等
 *   - yahoo:     yahoo_registered_items の観測情報
 *   - publishHistory: publish_idempotency の全 row (時系列、 最新が先)
 *   - exclusionHistory: listExclusionHistory (active + restored)
 *   - readiness: jobs.readiness_blocked_reasons + last_readiness_at
 *
 * itemCode が migration_candidates にない場合でも notion_overrides にあれば 「orphan」 として返す。
 * どちらにもなければ 404。
 */
router.get('/api/products/:itemCode/detail', (req, res) => {
  try {
    const db = getDB();
    const itemCode = String(req.params.itemCode || '').trim();
    if (!itemCode) return res.status(400).json({ status: 'fail', error: 'itemCode required' });

    const candidate = db.prepare('SELECT * FROM migration_candidates WHERE item_code = ?').get(itemCode);

    // notion_overrides は rakuten_manage_number が PK だが、 itemCode 直接 or candidate.rakuten_manage_number で検索
    const notion = db.prepare(`
      SELECT * FROM notion_overrides
      WHERE rakuten_manage_number = COALESCE(?, ?)
    `).get(candidate?.rakuten_manage_number || null, itemCode);

    if (!candidate && !notion) {
      return res.status(404).json({ status: 'fail', error: `product not found: ${itemCode}` });
    }

    const yahoo = db.prepare('SELECT * FROM yahoo_registered_items WHERE item_code = ?').get(itemCode);

    const publishHistory = db.prepare(`
      SELECT idempotency_key, manage_number, status, attempt, completed_at, error_message, created_at, updated_at
      FROM publish_idempotency
      WHERE item_code = ?
      ORDER BY created_at DESC, rowid DESC
    `).all(itemCode);

    const exclusionHistory = listExclusionHistory(db, itemCode);

    const job = db.prepare(`
      SELECT readiness_status, readiness_blocked_reasons, last_readiness_at, current_state
      FROM jobs WHERE item_code = ?
    `).get(itemCode);

    let readinessReasons = [];
    if (job?.readiness_blocked_reasons) {
      try { readinessReasons = JSON.parse(job.readiness_blocked_reasons); } catch (_) {}
    }
    const translatedReadiness = readinessReasons.length > 0
      ? summarizeReasons(readinessReasons).items
      : [];

    return res.json({
      itemCode,
      candidate: candidate || null,
      notion: notion || null,
      yahoo: yahoo || null,
      publishHistory,
      exclusionHistory,
      readiness: job ? {
        status: job.readiness_status,
        blockedReasons: readinessReasons,
        blockedReasonsTranslated: translatedReadiness,
        lastReadinessAt: job.last_readiness_at,
        currentState: job.current_state,
      } : null,
      notionPageUrl: notion?.notion_page_id ? notionPageUrl(notion.notion_page_id) : null,
    });
  } catch (e) {
    return res.status(500).json({ status: 'fail', error: e.message });
  }
});

// ───────────────── Phase E-7-c: 移行除外管理 API ─────────────────

router.get('/api/exclusions', (req, res) => {
  try {
    const db = getDB();
    const kind = req.query.kind ? String(req.query.kind) : null;
    const search = req.query.q ? String(req.query.q) : '';
    const items = listActiveExclusions(db, { kind, search });
    const counts = countActiveByKind(db);
    return res.json({ items, counts });
  } catch (e) {
    // Codex R1 Medium: assertKind 等の validation error は 400 にマップ
    if (/exclusion:/.test(e.message)) return res.status(400).json({ status: 'fail', error: e.message });
    return res.status(500).json({ status: 'fail', error: e.message });
  }
});

router.get('/api/exclusions/history/:itemCode', (req, res) => {
  try {
    const db = getDB();
    const history = listExclusionHistory(db, req.params.itemCode);
    return res.json({ item_code: req.params.itemCode, history });
  } catch (e) {
    return res.status(400).json({ status: 'fail', error: e.message });
  }
});

router.post('/api/exclusions', (req, res) => {
  try {
    const db = getDB();
    const { itemCode, excludeKind, reason } = req.body || {};
    const r = excludeMigrationItem({ db, itemCode, excludeKind, reason, actor: 'http' });
    audit(db, 'migration_exclude', { id: r.id, itemCode: r.item_code, excludeKind: r.exclude_kind });
    return res.status(201).json({ status: 'ok', ...r });
  } catch (e) {
    if (e.code === 'EXCLUSION_ALREADY_ACTIVE') return res.status(409).json({ status: 'fail', code: e.code, error: e.message });
    // Codex R1 Medium: race で事前 select すり抜け → SQLite UNIQUE constraint error も 409 にマップ
    //   (better-sqlite3 の SqliteError は code: 'SQLITE_CONSTRAINT_UNIQUE' を持つ)
    if (e.code === 'SQLITE_CONSTRAINT_UNIQUE' && /idx_migration_excluded_active|migration_excluded/.test(e.message)) {
      return res.status(409).json({ status: 'fail', code: 'EXCLUSION_ALREADY_ACTIVE', error: 'item_code already has an active exclusion (race)' });
    }
    if (/exclusion:/.test(e.message)) return res.status(400).json({ status: 'fail', error: e.message });
    return res.status(500).json({ status: 'fail', error: e.message });
  }
});

router.post('/api/exclusions/restore', (req, res) => {
  try {
    const db = getDB();
    const { itemCode, reason } = req.body || {};
    const r = restoreMigrationExclusion({ db, itemCode, reason, actor: 'http' });
    audit(db, 'migration_exclude_restore', { restored_id: r.restored_id, itemCode: r.item_code, priorKind: r.prior_kind });
    return res.json({ status: 'ok', ...r });
  } catch (e) {
    if (e.code === 'EXCLUSION_NOT_ACTIVE') return res.status(404).json({ status: 'fail', code: e.code, error: e.message });
    if (/exclusion:/.test(e.message)) return res.status(400).json({ status: 'fail', error: e.message });
    return res.status(500).json({ status: 'fail', error: e.message });
  }
});

// ───────────────── Phase E-7-e: 楽天↔Yahoo 差分手動同期 ─────────────────

/**
 * 「いま再取得」 ボタンの実体: Yahoo baseline → 楽天 diff の順序実行を同期で run。
 * Render 完結なので polling 不要 (同 DB / 同プロセス)、 完了まで blocked、 結果を JSON で返す。
 *   body: { dryRun?: boolean, allowZeroOverlap?: boolean, triggerRequestId?: string }
 *   res:  200 + 結果、 409 (running 競合)、 412 (baseline guard)、 500 (失敗)
 */
router.post('/api/diff-sync/trigger', async (req, res) => {
  const body = req.body || {};
  const dryRun = !!body.dryRun;
  const triggerRequestId = typeof body.triggerRequestId === 'string' ? body.triggerRequestId : null;
  try {
    const db = getDB();
    const r = await runRysFullSync({
      db, triggeredBy: 'manual', triggerRequestId, dryRun, allowZeroOverlap: !!body.allowZeroOverlap,
    });
    audit(db, 'rys_diff_sync_manual', {
      rysFullSyncRunId: r.rysFullSyncRunId,
      baselineId: r.baseline?.syncRunId,
      diffId: r.diff?.syncRunId,
      newlyDetected: r.diff?.newlyDetected,
      resolved: r.diff?.resolved,
      triggerRequestId, dryRun,
      durationMs: r.durationMs,
    });
    return res.json({ status: 'ok', ...r });
  } catch (e) {
    // Codex R1 Medium-3: 失敗時にも audit を残す (stage / partial sync_run_ids / triggerRequestId / dryRun)
    try {
      const db = getDB();
      audit(db, 'rys_diff_sync_manual_fail', {
        stage: e.stage || 'unknown',
        rysFullSyncRunId: e.partial?.rysFullSyncRunId,
        baselineId: e.partial?.baseline?.syncRunId,
        diffId: e.partial?.diff?.syncRunId,
        triggerRequestId, dryRun,
        error: e.message,
      }, { result: 'failed', errorMessage: String(e.message).slice(0, 4000) });
    } catch (_) { /* best-effort */ }

    // running 競合 → 409
    if (/UNIQUE/.test(e.message) && /sync_runs/.test(e.message)) {
      return res.status(409).json({ status: 'fail', stage: e.stage || 'lock', error: 'A sync is already running' });
    }
    // baseline guard / precondition 失敗 → 412
    if (e.statusCode === 412 || e.cause?.statusCode === 412) {
      return res.status(412).json({ status: 'fail', stage: e.stage || 'baseline_guard', error: e.message, partial: e.partial });
    }
    return res.status(500).json({ status: 'fail', stage: e.stage || 'unknown', error: e.message, partial: e.partial });
  }
});

router.post('/api/exclusions/change-kind', (req, res) => {
  try {
    const db = getDB();
    const { itemCode, newKind, reason } = req.body || {};
    const r = changeExclusionKind({ db, itemCode, newKind, reason, actor: 'http' });
    audit(db, 'migration_exclude_change_kind', { restoredId: r.restored_id, newId: r.new_id, itemCode: r.item_code, priorKind: r.prior_kind, newKind: r.new_kind });
    return res.json({ status: 'ok', ...r });
  } catch (e) {
    if (e.code === 'EXCLUSION_NOT_ACTIVE') return res.status(404).json({ status: 'fail', code: e.code, error: e.message });
    if (e.code === 'EXCLUSION_KIND_UNCHANGED') return res.status(409).json({ status: 'fail', code: e.code, error: e.message });
    if (/exclusion:/.test(e.message)) return res.status(400).json({ status: 'fail', error: e.message });
    return res.status(500).json({ status: 'fail', error: e.message });
  }
});

// ───────────────── Phase E-6-3: 一括 publish + エラー再実行 ─────────────────

/**
 * 一括 publish trigger。 itemCodes [] を順次 publish。 即時に batch_id を返し、 実行は背景で続行。
 *   body: { itemCodes: string[], triggeredBy?: 'manual' | 'retry_failed' }
 *   res:  202 + { batchId, total } / 400 / 403 (kill-switch) / 409 (concurrent batch)
 */
router.post('/api/publish/bulk/start', (req, res) => {
  try {
    const db = getDB();
    const body = req.body || {};
    const itemCodes = body.itemCodes;
    const triggeredBy = body.triggeredBy === 'retry_failed' ? 'retry_failed' : 'manual';
    const result = startBulkPublish({ db, itemCodes, triggeredBy });
    audit(db, 'bulk_publish_start', { batchId: result.batchId, total: result.total, triggeredBy });
    return res.status(202).json({ status: 'ok', ...result });
  } catch (e) {
    if (e.statusCode === 400) return res.status(400).json({ status: 'fail', error: e.message });
    if (e.statusCode === 403) return res.status(403).json({ status: 'fail', error: e.message });
    if (/UNIQUE/.test(e.message) && /bulk_publish_batches/.test(e.message)) {
      return res.status(409).json({ status: 'fail', error: 'A bulk publish batch is already running' });
    }
    return res.status(500).json({ status: 'fail', error: e.message });
  }
});

/**
 * 失敗 publish の itemCodes を一括再実行 (= retry_failed)。
 *   body: { limit?: number } default 50
 */
router.post('/api/publish/bulk/retry-failed', (req, res) => {
  try {
    const db = getDB();
    const body = req.body || {};
    // Codex R1 Medium-4: limit は最大 50 (E-6-3 設計スコープに準拠、 retry 大量実行で Yahoo レート逸脱を防ぐ)
    const limit = Number.isInteger(body.limit) && body.limit > 0 && body.limit <= 50 ? body.limit : 50;
    const itemCodes = listFailedItemCodes(db, { limit });
    if (itemCodes.length === 0) {
      return res.json({ status: 'ok', batchId: null, total: 0, message: '失敗中の publish はありません' });
    }
    const result = startBulkPublish({ db, itemCodes, triggeredBy: 'retry_failed' });
    audit(db, 'bulk_publish_retry_failed', { batchId: result.batchId, total: result.total });
    return res.status(202).json({ status: 'ok', ...result, itemCodes });
  } catch (e) {
    if (e.statusCode === 400) return res.status(400).json({ status: 'fail', error: e.message });
    if (e.statusCode === 403) return res.status(403).json({ status: 'fail', error: e.message });
    if (/UNIQUE/.test(e.message)) return res.status(409).json({ status: 'fail', error: 'A bulk publish batch is already running' });
    return res.status(500).json({ status: 'fail', error: e.message });
  }
});

/**
 * 現在 running な batch を返す (UI ヘッダーで 「実行中: N/M」 を表示するため)。
 *   Codex R1 Medium-1: static route は parameter route より前に登録 (Express は :batchId が active を食う)。
 */
router.get('/api/publish/bulk/active/current', (req, res) => {
  try {
    const db = getDB();
    return res.json({ status: 'ok', batch: getRunningBatch(db) });
  } catch (e) {
    return res.status(500).json({ status: 'fail', error: e.message });
  }
});

/**
 * batch の進捗 (polling 用)。
 *   res: { batch, items? } (items は ?withItems=1 で含める、 進捗 polling 中は省略)
 */
router.get('/api/publish/bulk/:batchId', (req, res) => {
  try {
    const db = getDB();
    const batchId = Number(req.params.batchId);
    if (!Number.isInteger(batchId) || batchId <= 0) {
      return res.status(400).json({ status: 'fail', error: 'invalid batchId' });
    }
    const batch = getBatchStatus(db, batchId);
    if (!batch) return res.status(404).json({ status: 'fail', error: 'batch not found' });
    const includeItems = req.query.withItems === '1' || req.query.withItems === 'true';
    const items = includeItems ? getBatchItems(db, batchId) : null;
    return res.json({ status: 'ok', batch, items });
  } catch (e) {
    return res.status(500).json({ status: 'fail', error: e.message });
  }
});

// ───────────────── Phase E-8: 楽天画像ファイル名パターン除外管理 + 楽天画像ドロワー表示 ─────────────────

/**
 * 組込み (coupon/review) + ユーザー追加分の image_exclusion_patterns を返す。
 *   res: { patterns: [{pattern, reason, source, id?, created_at?, created_by?}] }
 */
router.get('/api/image-exclusion/patterns', (req, res) => {
  try {
    const db = getDB();
    const patterns = listAllPatterns(db);
    return res.json({ status: 'ok', patterns });
  } catch (e) {
    return res.status(500).json({ status: 'fail', error: e.message });
  }
});

/**
 * 画像除外パターンを追加 (ユーザー追加分のみ。 組込みは追加不可)。
 *   body: { pattern: string, reason: string }
 */
router.post('/api/image-exclusion/patterns', (req, res) => {
  try {
    const db = getDB();
    const { pattern, reason } = req.body || {};
    const r = addPattern({ db, pattern, reason, actor: 'http' });
    audit(db, 'image_exclusion_add', { id: r.id, pattern: r.pattern });
    return res.status(201).json({ status: 'ok', ...r });
  } catch (e) {
    if (e.code === 'PATTERN_DUPLICATE') return res.status(409).json({ status: 'fail', code: e.code, error: e.message });
    if (e.code === 'PATTERN_CONFLICT_BUILTIN') return res.status(409).json({ status: 'fail', code: e.code, error: e.message });
    if (/image-exclusion:/.test(e.message)) return res.status(400).json({ status: 'fail', error: e.message });
    return res.status(500).json({ status: 'fail', error: e.message });
  }
});

/**
 * 画像除外パターンを削除 (ユーザー追加分のみ。 組込みは id を持たない)。
 */
router.delete('/api/image-exclusion/patterns/:id', (req, res) => {
  try {
    const db = getDB();
    const id = Number(req.params.id);
    if (!Number.isInteger(id) || id <= 0) {
      return res.status(400).json({ status: 'fail', error: 'invalid id' });
    }
    const r = removePattern({ db, id });
    audit(db, 'image_exclusion_remove', { id });
    return res.json({ status: 'ok', ...r });
  } catch (e) {
    if (e.code === 'PATTERN_NOT_FOUND') return res.status(404).json({ status: 'fail', code: e.code, error: e.message });
    return res.status(500).json({ status: 'fail', error: e.message });
  }
});

/**
 * 1 商品の楽天画像 + 除外結果を返す (ドロワー 「楽天画像」 タブ用)。
 *   楽天 RMS から live で fetch (cache 無し、 ドロワーを開いた時のみ)。
 *   - kept:     Yahoo へ移行する URL
 *   - excluded: 除外 URL + どのパターン (builtin/custom) でマッチしたか
 *
 *   itemCode → manageNumber は migration_candidates / notion_overrides から resolve。
 *   どちらにも無ければ 404。 楽天 RMS が失敗したら 502 で詳細を返す。
 */
router.get('/api/products/:itemCode/rakuten-images', async (req, res) => {
  try {
    const db = getDB();
    const itemCode = String(req.params.itemCode || '').trim();
    if (!itemCode) return res.status(400).json({ status: 'fail', error: 'itemCode required' });

    const cand = db.prepare('SELECT rakuten_manage_number FROM migration_candidates WHERE item_code = ?').get(itemCode);
    let manageNumber = cand?.rakuten_manage_number || null;
    if (!manageNumber) {
      const notion = db.prepare('SELECT rakuten_manage_number FROM notion_overrides WHERE rakuten_manage_number = ?').get(itemCode);
      manageNumber = notion?.rakuten_manage_number || itemCode;  // fallback: itemCode == manageNumber 規約
    }

    let fetchResult;
    try {
      fetchResult = await fetchItemDetail(manageNumber);
    } catch (e) {
      return res.status(502).json({ status: 'fail', stage: 'rakuten_fetch', error: e.message });
    }
    if (fetchResult?.status === 'failed' || !fetchResult?.item) {
      return res.status(404).json({
        status: 'fail',
        error: `rakuten item not found: ${manageNumber} (${fetchResult?.reason || 'no_item'})`,
      });
    }

    const customPatterns = getCustomPatternStrings(db);
    const detail = filterUploadableImageUrlsDetailed(fetchResult.item.images || [], { customPatterns });

    return res.json({
      status: 'ok',
      itemCode,
      manageNumber,
      keptCount: detail.kept.length,
      excludedCount: detail.excluded.length,
      kept: detail.kept,
      excluded: detail.excluded,
    });
  } catch (e) {
    return res.status(500).json({ status: 'fail', error: e.message });
  }
});

// ───────────────── Phase E-9: 楽天タイトル backfill API ─────────────────

/**
 * 不足してる楽天タイトルを bulk fetch して埋める手動 trigger。
 *   body: { limit?: number } default 100、 max 200 (proxy timeout 保護)
 */
router.post('/api/rakuten-title-backfill', async (req, res) => {
  try {
    const db = getDB();
    const body = req.body || {};
    const limit = Number.isInteger(body.limit) && body.limit > 0 && body.limit <= 200 ? body.limit : 100;
    const r = await backfillRakutenTitles({ db, limit });
    const remaining = countMissingRakutenTitles(db);
    audit(db, 'rakuten_title_backfill', { ...r, remaining });
    return res.json({ status: 'ok', ...r, remaining });
  } catch (e) {
    return res.status(500).json({ status: 'fail', error: e.message });
  }
});

router.get('/api/rakuten-title-backfill/status', (req, res) => {
  try {
    const db = getDB();
    return res.json({ status: 'ok', remaining: countMissingRakutenTitles(db) });
  } catch (e) {
    return res.status(500).json({ status: 'fail', error: e.message });
  }
});

// Codex E-7-d-1 R1 Medium: テストから直接呼べるよう named export
export { listProductsForUI, notionPageUrl };

export default router;
