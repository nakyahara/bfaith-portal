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
import { fetchAllItemCodes } from './lib/rakuten-rms-proxy.js';
import { executePublish, isPublishEnabled, buildIdempotencyKey } from './services/publish-executor.js';
import { translateReason, summarizeReasons, categorizeReason } from './lib/reason-translator.js';

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
  //   Codex E-6 R1 High-2: jobs JOIN は 1 商品 1 行を保証 (item_code は PK だが防御的に LIMIT)
  //   Codex E-6 R1 Medium-1: publish_idempotency は「一度でも success」 なら done
  //     最新 status を見るのではなく、 success 履歴を優先する SELECT に変更
  const rows = db.prepare(`
    SELECT
      no.rakuten_manage_number  AS item_code,
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
      pi.status         AS publish_status,
      pi.completed_at   AS publish_completed_at,
      pi.error_message  AS publish_error
    FROM notion_overrides no
    LEFT JOIN jobs j ON j.item_code = no.rakuten_manage_number
    LEFT JOIN (
      -- 一度でも success があれば success を優先、 なければ最新 status (Codex R1 Medium-1)
      -- Codex R2 Medium-1: status が success のとき completed_at / error_message も success 由来に揃える。
      --   そうしないと UI で 「過去 success → その後 failed」 の商品が done 扱いなのに 「失敗時刻」 を表示する。
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
    ) pi ON pi.item_code = no.rakuten_manage_number
    ORDER BY no.rakuten_manage_number
  `).all();

  // status 計算 + reason 翻訳
  const products = rows.map((r) => {
    let status, reasons = [], notionFields = [];
    if (r.publish_status === 'success') {
      status = 'done';
    } else {
      // 簡易 readiness 判定 (Notion 必須 4 項目)
      const missing = [];
      if (!r.yahoo_title)            missing.push('notion_title_missing');
      if (!r.yahoo_price)            missing.push('price_invalid_or_zero');
      if (!r.notion_delivery_label)  missing.push('delivery_mapping_unresolved');
      if (!r.notion_tax_rate)        missing.push('notion_tax_rate_missing');
      // 既存の readiness 評価結果も併合
      let existingReasons = [];
      if (r.readiness_blocked_reasons) {
        try { existingReasons = JSON.parse(r.readiness_blocked_reasons); } catch (_) {}
      }
      const allReasons = [...missing, ...existingReasons.filter((x) => !missing.includes(x))];
      // Codex E-6 R1 Medium-3: readiness 鮮度判定。
      //   last_readiness_at が synced_at 以降なら厳密判定済 → blocked reasons を信頼。
      //   それより古い or 未評価なら 「Notion 簡易判定だけで actionable」 と判断。
      const readinessFresh = r.last_readiness_at && r.synced_at && r.last_readiness_at >= r.synced_at;
      if (allReasons.length === 0) {
        status = 'actionable';
      } else {
        status = 'fixable';
        const summarized = summarizeReasons(allReasons);
        reasons = summarized.items;
        notionFields = summarized.notionFields;
      }
      // フラグだけ立てる (UI 側で「再評価推奨」 等表示できるように)
      // 現状 Phase 1 では使わない、 Phase 2 detail drawer で表示予定
      void readinessFresh;
    }
    return {
      itemCode:     r.item_code,
      notionPageId: r.notion_page_id,
      yahooTitle:   r.yahoo_title,
      yahooPrice:   r.yahoo_price,
      sagawaPrice:  r.yahoo_price_sagawa,
      delivery:     r.notion_delivery_label,
      taxRate:      r.notion_tax_rate,
      notionStatus: r.notion_status,
      syncedAt:     r.synced_at,
      publishStatus: r.publish_status,
      publishCompletedAt: r.publish_completed_at,
      publishError: r.publish_error,
      status,
      reasons,
      notionFields,
      primaryReason: reasons[0]?.message || '',
    };
  });

  // filter 適用
  let filtered = products;
  if (filter === 'actionable') filtered = products.filter((p) => p.status === 'actionable');
  else if (filter === 'fixable') filtered = products.filter((p) => p.status === 'fixable');
  else if (filter === 'done') filtered = products.filter((p) => p.status === 'done');

  // 検索 (商品コード / タイトル 部分一致、 case-insensitive)
  const term = String(search || '').trim().toLowerCase();
  if (term) {
    filtered = filtered.filter((p) =>
      (p.itemCode && p.itemCode.toLowerCase().includes(term))
      || (p.yahooTitle && p.yahooTitle.toLowerCase().includes(term))
    );
  }

  // 集計
  const summary = {
    total: products.length,
    actionable: products.filter((p) => p.status === 'actionable').length,
    fixable: products.filter((p) => p.status === 'fixable').length,
    done: products.filter((p) => p.status === 'done').length,
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
  let summary = { total: 0, actionable: 0, fixable: 0, done: 0 };
  let fixableByCategory = {};
  const filter = ['all', 'actionable', 'fixable', 'done'].includes(req.query.filter)
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
    filter,
    search,
    notionPageUrl,  // EJS から呼べるように
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

    // status マッピング
    if (result.status === 'in_progress_conflict') return res.status(409).json(result);
    if (result.status === 'dedupe') return res.status(200).json(result);
    if (result.status === 'readiness_blocked') return res.status(422).json(result);
    if (result.status === 'not_implemented') return res.status(501).json(result);
    if (result.status === 'flag_off') return res.status(403).json(result);
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

export default router;
