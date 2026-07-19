// ai-insights: AI経営アドバイス定期配信 (PR-1: Render側基盤)
//
// マウント構成 (server.js):
//   /apps/ai-insights  (session + requireAppAccess('ai-insights')) ← ⚙️経営設定画面 + 画面用API
//   /api/ai-insights   (トークン認証。session 不要)
//     GET  /report-input          x-read-token = AI_READ_TOKEN (read-only、PC runner が取得)
//     POST /service/jobs/*        Bearer = AI_INSIGHT_SERVICE_TOKEN (ジョブ状態機械)
//
// ジョブ状態機械 (要件 §8。Codexラウンド2-3 High対応):
//   pending → generating → generated → posting → posted
//                └→ failed (再claim可)      └→ (孤児) reconciliation_required (自動再投稿禁止)
// GChat は Incoming Webhook (write-only) のため、posting 孤児は人間照合 (⚙️画面) でのみ解決する。

import express from 'express';
import { getMirrorDB } from '../warehouse-mirror/db.js';
import {
  initAiInsightsTables, newId, nowIso, isYmd, isYm, buildPublicId,
} from './db.js';
import { buildWeeklyReportInput, addDaysYmd } from './facts.js';
import {
  buildMonthlyReportInput, isYmStrict, monthBounds, previousMonthYm,
} from './facts-monthly.js';

const ORPHAN_TIMEOUT_MS = 30 * 60 * 1000; // claim後30分無heartbeatで孤児扱い (要件 §8)
const BUDGET_METRICS = ['sales', 'gross_profit', 'operating_income', 'cash_eom', 'inventory_value'];
const EVENT_TAGS = ['sale', 'price', 'ad', 'incident', 'procure', 'other'];
const MAX_VALUE_YEN = 1e13; // 10兆円: 入力ミス (桁違い) ガード

// ── init (fail-soft: mirror 全体を道連れにしない。#482 教訓) ──────────────
let inited = false;
export let aiInsightsInitError = null;

function ensureInit() {
  if (inited) return true;
  try {
    const db = getMirrorDB(); // mirror 未init時は throw → 次リクエストで再試行
    initAiInsightsTables(db);
    inited = true;
    aiInsightsInitError = null;
    return true;
  } catch (e) {
    aiInsightsInitError = e;
    console.error('[ai-insights] init failed:', e.message);
    return false;
  }
}

function ensureReady(req, res, next) {
  if (!ensureInit()) {
    return res.status(503).json({ error: 'ai-insights init failed', detail: aiInsightsInitError?.message });
  }
  next();
}

// ── トークン認証 (warehouse-mirror requireReadToken / warehouse serviceAuth の踏襲) ──

// read-only: AI_READ_TOKEN (x-read-token ヘッダのみ。query 禁止。未設定 fail-closed 503)
function requireAiReadToken(req, res, next) {
  const token = process.env.AI_READ_TOKEN;
  if (!token) {
    return res.status(503).json({ error: 'AI_READ_TOKEN not configured' });
  }
  if (req.query.token || req.query.read_token) {
    return res.status(400).json({ error: 'token must be sent via x-read-token header, not query' });
  }
  const provided = req.get('x-read-token');
  if (!provided || provided !== token) {
    return res.status(401).json({ error: 'invalid read token' });
  }
  next();
}

// 書き込み系: AI_INSIGHT_SERVICE_TOKEN (Bearer / X-Service-Token。query 禁止。fail-closed)
function requireAiServiceToken(req, res, next) {
  const token = process.env.AI_INSIGHT_SERVICE_TOKEN;
  if (!token) {
    return res.status(503).json({ error: 'AI_INSIGHT_SERVICE_TOKEN not configured' });
  }
  if (req.query.token || req.query.service_token) {
    return res.status(400).json({ error: 'token must be sent via header, not query' });
  }
  let provided = null;
  const auth = req.get('authorization');
  if (auth && auth.startsWith('Bearer ')) provided = auth.slice(7).trim();
  if (!provided) provided = req.get('x-service-token') || null;
  if (!provided) return res.status(401).json({ error: 'missing service token' });
  if (provided !== token) return res.status(403).json({ error: 'invalid service token' });
  next();
}

// ── 共通ヘルパー ──────────────────────────────────────────────────────

function sessionUser(req) {
  return req.session?.username || 'unknown';
}

/** 孤児ジョブ回収 (claim / settings 参照時に走る)。要件 §8/§8.1 */
function sweepOrphans(db) {
  const cutoff = new Date(Date.now() - ORPHAN_TIMEOUT_MS).toISOString();
  const now = nowIso();
  // generating 孤児 → failed (再claim可能。生成は冪等: 同一入力なら再生成してよい)
  db.prepare(`
    UPDATE ai_report_jobs
    SET status = 'failed', error_class = 'orphaned',
        error_detail = 'generating のまま heartbeat 途絶 (lease超過)', updated_at = ?
    WHERE status = 'generating' AND COALESCE(heartbeat_at, claimed_at) < ?
  `).run(now, cutoff);
  // posting 孤児 → reconciliation_required (投稿済みか不明。自動再投稿は絶対にしない)
  db.prepare(`
    UPDATE ai_report_jobs
    SET status = 'reconciliation_required',
        error_detail = 'posting のまま heartbeat 途絶。GChat投稿済みかを public_id で人間照合すること',
        updated_at = ?
    WHERE status = 'posting' AND COALESCE(heartbeat_at, claimed_at) < ?
  `).run(now, cutoff);
}

function getJob(db, jobId) {
  return db.prepare('SELECT * FROM ai_report_jobs WHERE job_id = ?').get(jobId);
}

function validatePeriod(reportType, periodStart, periodEnd) {
  if (!isYmd(periodStart) || !isYmd(periodEnd)) return 'period_start/period_end must be YYYY-MM-DD';
  if (periodEnd <= periodStart) return 'period_end must be after period_start';
  if (reportType === 'weekly' && addDaysYmd(periodStart, 7) !== periodEnd) {
    return 'weekly period must be exactly 7 days';
  }
  if (reportType === 'monthly') {
    const ym = periodStart.slice(0, 7);
    if (!isYmStrict(ym)) return 'monthly period_start must be a month start';
    const bounds = monthBounds(ym);
    if (periodStart !== bounds.periodStart || periodEnd !== bounds.periodEnd) {
      return 'monthly period must be exactly one calendar month (1日〜翌月1日排他)';
    }
  }
  return null;
}

// ════════════════════════════════════════════════════════════════════
// ① トークン認証 API ルーター (/api/ai-insights)
// ════════════════════════════════════════════════════════════════════

export const aiInsightsApiRouter = express.Router();

// ── レポート入力 JSON (PC runner が取得。read-only) ──
aiInsightsApiRouter.get('/report-input', requireAiReadToken, ensureReady, (req, res) => {
  try {
    const type = req.query.type || 'weekly';
    const db = getMirrorDB();
    if (type === 'weekly') {
      const opts = {};
      if (req.query.period_start) {
        if (!isYmd(req.query.period_start)) {
          return res.status(400).json({ error: 'period_start must be YYYY-MM-DD' });
        }
        opts.periodStart = req.query.period_start;
      }
      return res.json(buildWeeklyReportInput(db, opts));
    }
    if (type === 'monthly') {
      const opts = {};
      if (req.query.month) {
        if (!isYmStrict(req.query.month)) return res.status(400).json({ error: 'month must be YYYY-MM' });
        opts.month = req.query.month;
      }
      return res.json(buildMonthlyReportInput(db, opts));
    }
    return res.status(400).json({ error: `unknown report type '${type}'` });
  } catch (e) {
    console.error('[ai-insights] report-input error:', e);
    res.status(e.message?.includes('must be a Monday') ? 400 : 500).json({ error: e.message });
  }
});

// ── ジョブ状態機械 (service token) ──
const serviceRouter = express.Router();
aiInsightsApiRouter.use('/service', requireAiServiceToken, express.json({ limit: '2mb' }), ensureReady, serviceRouter);

/**
 * claim: ジョブの原子的獲得 (UPDATE ... WHERE status の行数で判定。要件 §8)
 * body: { report_type, period_start, period_end, edition, claimed_by }
 * edition: 'final' | 'provisional' | 'correction-next' (次番号を採番) | 'correction-N'
 * 応答 result:
 *   claimed              → 生成から開始 (status=generating)
 *   claimed_for_posting  → 生成済みレポートの投稿だけ再開 (status=posting)
 *   already_posted / busy / reconciliation_required → 何もしない
 */
serviceRouter.post('/jobs/claim', (req, res) => {
  const db = getMirrorDB();
  const { report_type, period_start, period_end, claimed_by } = req.body || {};
  let { edition } = req.body || {};
  if (!['weekly', 'monthly'].includes(report_type)) {
    return res.status(400).json({ error: 'report_type must be weekly|monthly' });
  }
  const perr = validatePeriod(report_type, period_start, period_end);
  if (perr) return res.status(400).json({ error: perr });
  if (!edition) edition = 'final';
  if (edition !== 'final' && edition !== 'provisional'
    && edition !== 'correction-next' && !/^correction-[1-9]\d*$/.test(edition)) {
    return res.status(400).json({ error: 'invalid edition' });
  }

  // 月次: claim 時点の宣言番号をジョブに固定する (生成中に再オープン/再宣言されても
  // 古い生成を確定扱いしない。final/correction では現在の closing と一致必須)
  let declareSeq = null;
  if (report_type === 'monthly' && edition !== 'provisional') {
    const closingRow = db.prepare(
      'SELECT declare_seq, status FROM ai_monthly_closing WHERE year_month = ?'
    ).get(period_start.slice(0, 7));
    if (!closingRow || closingRow.status !== 'declared') {
      return res.status(409).json({ error: '確定宣言されていない月の final/correction は claim できません' });
    }
    declareSeq = closingRow.declare_seq;
    const clientSeq = req.body?.declare_seq;
    if (clientSeq != null && clientSeq !== declareSeq) {
      return res.status(409).json({ error: `declare_seq が進んでいます (client=${clientSeq}, server=${declareSeq})。入力を取り直してください` });
    }
  }

  sweepOrphans(db);
  const now = nowIso();

  const insertJob = (ed) => {
    const jobId = newId('jb');
    db.prepare(`
      INSERT INTO ai_report_jobs
        (job_id, report_type, period_start, period_end, edition, declare_seq, status, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)
    `).run(jobId, report_type, period_start, period_end, ed, declareSeq, now, now);
    return jobId;
  };

  let job = null;
  if (edition === 'correction-next') {
    // 1宣言 = 1 correction ジョブ (Codex PR-3 high 対応):
    // 同じ declare_seq の correction が既にあればそれを再利用 (下の状態機械 claim に任せる)。
    // 無い場合のみ新番号を採番する → 失敗/照合待ちを迂回して番号が増殖しない
    const existing = db.prepare(`
      SELECT edition FROM ai_report_jobs
      WHERE report_type = ? AND period_start = ? AND period_end = ?
        AND edition LIKE 'correction-%' AND declare_seq = ?
    `).get(report_type, period_start, period_end, declareSeq);
    if (existing) {
      edition = existing.edition;
    } else {
      // UNIQUE 制約 + 再試行で原子的に採番 (Codexラウンド5 合意方式)
      for (let attempt = 0; attempt < 5; attempt++) {
        const maxRow = db.prepare(`
          SELECT MAX(CAST(SUBSTR(edition, 12) AS INTEGER)) AS n FROM ai_report_jobs
          WHERE report_type = ? AND period_start = ? AND period_end = ? AND edition LIKE 'correction-%'
        `).get(report_type, period_start, period_end);
        const candidate = `correction-${(maxRow?.n || 0) + 1}`;
        try {
          insertJob(candidate);
          edition = candidate;
          break;
        } catch (e) {
          if (!/UNIQUE constraint/.test(e.message)) throw e; // 採番競合以外は伝播
          if (attempt === 4) return res.status(500).json({ error: 'correction 採番が5回競合' });
        }
      }
    }
  } else {
    try {
      insertJob(edition);
    } catch (e) {
      if (!/UNIQUE constraint/.test(e.message)) throw e; // 既存行あり → 下で状態を見る
    }
  }

  job = db.prepare(`
    SELECT * FROM ai_report_jobs
    WHERE report_type = ? AND period_start = ? AND period_end = ? AND edition = ?
  `).get(report_type, period_start, period_end, edition);
  if (!job) return res.status(500).json({ error: 'job row not found after insert' });

  if (job.status === 'posted') return res.json({ result: 'already_posted', job });
  if (job.status === 'reconciliation_required') {
    return res.json({ result: 'reconciliation_required', job });
  }

  const claimer = String(claimed_by || 'runner').slice(0, 100);
  // レポート未生成 (report_id なし) の pending/failed → generating
  const claimed = db.prepare(`
    UPDATE ai_report_jobs
    SET status = 'generating', claimed_by = ?, claimed_at = ?, heartbeat_at = ?,
        error_class = NULL, error_detail = NULL, updated_at = ?
    WHERE job_id = ? AND status IN ('pending', 'failed') AND report_id IS NULL
  `).run(claimer, now, now, now, job.job_id);
  if (claimed.changes === 1) {
    return res.json({ result: 'claimed', job: getJob(db, job.job_id) });
  }
  // レポート生成済み (generated / 投稿失敗後の failed / 照合後 repost の pending) → posting。
  // 再生成すると既存 ai_reports と UNIQUE 衝突するため、投稿だけを再開する
  const reclaimed = db.prepare(`
    UPDATE ai_report_jobs
    SET status = 'posting', claimed_by = ?, claimed_at = ?, heartbeat_at = ?,
        error_class = NULL, error_detail = NULL, updated_at = ?
    WHERE job_id = ? AND status IN ('generated', 'pending', 'failed') AND report_id IS NOT NULL
  `).run(claimer, now, now, now, job.job_id);
  if (reclaimed.changes === 1) {
    const fresh = getJob(db, job.job_id);
    const report = fresh.report_id
      ? db.prepare('SELECT * FROM ai_reports WHERE report_id = ?').get(fresh.report_id)
      : null;
    return res.json({ result: 'claimed_for_posting', job: fresh, report });
  }
  return res.json({ result: 'busy', job: getJob(db, job.job_id) });
});

serviceRouter.post('/jobs/:jobId/heartbeat', (req, res) => {
  const db = getMirrorDB();
  const r = db.prepare(`
    UPDATE ai_report_jobs SET heartbeat_at = ?, updated_at = ?
    WHERE job_id = ? AND status IN ('generating', 'posting')
  `).run(nowIso(), nowIso(), req.params.jobId);
  if (r.changes !== 1) return res.status(409).json({ error: 'job not in active state' });
  res.json({ ok: true });
});

/**
 * 生成結果の保存: generating → generated
 * body: { generation_mode, body_json, topics[], input_hash, data_as_of, data_quality_status,
 *         prompt_version, input_schema_version, budget_revision_id, topic_updates[] }
 * 同一入力ハッシュの既存レポートがある場合は再生成 skip 用に 409 + 既存を返す
 */
serviceRouter.post('/jobs/:jobId/report', (req, res) => {
  const db = getMirrorDB();
  const job = getJob(db, req.params.jobId);
  if (!job) return res.status(404).json({ error: 'job not found' });
  if (job.status !== 'generating') {
    return res.status(409).json({ error: `job status is ${job.status}, expected generating` });
  }
  const b = req.body || {};
  if (!['claude', 'fallback'].includes(b.generation_mode)) {
    return res.status(400).json({ error: 'generation_mode must be claude|fallback' });
  }
  if (b.body_json == null || typeof b.body_json !== 'object') {
    return res.status(400).json({ error: 'body_json (object) required' });
  }
  const topics = Array.isArray(b.topics) ? b.topics : [];
  if (topics.length > 5) return res.status(400).json({ error: 'topics: max 5' });
  for (const t of topics) {
    if (!t || typeof t.title !== 'string' || !t.title.trim()) {
      return res.status(400).json({ error: 'each topic requires title' });
    }
  }
  const topicUpdates = Array.isArray(b.topic_updates) ? b.topic_updates : [];
  for (const u of topicUpdates) {
    if (!u?.topic_id || !['carried', 'resolved', 'worsened', 'dismissed'].includes(u.status)) {
      return res.status(400).json({ error: 'topic_updates: {topic_id, status(carried|resolved|worsened|dismissed)}' });
    }
  }

  const now = nowIso();
  const reportId = newId('rp');
  const publicId = buildPublicId(job.report_type, job.period_start, job.edition);
  try {
    db.transaction(() => {
      db.prepare(`
        INSERT INTO ai_reports
          (report_id, public_id, report_type, period_start, period_end, edition,
           data_as_of, data_quality_status, prompt_version, input_schema_version,
           generation_mode, input_hash, body_json, budget_revision_id, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        reportId, publicId, job.report_type, job.period_start, job.period_end, job.edition,
        b.data_as_of || null, b.data_quality_status || null,
        b.prompt_version || null, b.input_schema_version || null,
        b.generation_mode, b.input_hash || null, JSON.stringify(b.body_json),
        b.budget_revision_id || null, now,
      );
      const topicStmt = db.prepare(`
        INSERT INTO ai_report_topics
          (topic_id, report_id, seq, title, category, status, body_json, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, 'open', ?, ?, ?)
      `);
      topics.forEach((t, i) => {
        topicStmt.run(newId('tp'), reportId, i + 1, t.title.trim().slice(0, 200),
          t.category || null, t.body ? JSON.stringify(t.body) : null, now, now);
      });
      // 更新できるのは「同じ report_type の、対象期間より前のレポートの未解決論点」のみ
      // (token 漏洩や runner 不具合で無関係な論点を書き換えられないようスコープを絞る)
      const updStmt = db.prepare(`
        UPDATE ai_report_topics SET status = ?, updated_at = ?
        WHERE topic_id = ?
          AND status IN ('open', 'carried', 'worsened')
          AND EXISTS (
            SELECT 1 FROM ai_reports r
            WHERE r.report_id = ai_report_topics.report_id
              AND r.report_type = ? AND r.period_start < ?
          )
      `);
      for (const u of topicUpdates) {
        updStmt.run(u.status, now, u.topic_id, job.report_type, job.period_start);
      }
      db.prepare(`
        UPDATE ai_report_jobs SET status = 'generated', report_id = ?, input_hash = ?,
          heartbeat_at = ?, updated_at = ? WHERE job_id = ?
      `).run(reportId, b.input_hash || null, now, now, job.job_id);
      // 月次の確定版 (final / correction-N): 締め行に確定情報を記録し「確定後変更検知」の基準にする。
      // ⚠️ claim 時に固定した job.declare_seq と現在の closing.declare_seq が一致する時のみ。
      //   生成中に再オープン→再宣言された古い生成を確定扱いすると、新しい宣言に対する
      //   訂正版が永久に出なくなる (Codex PR-3 high)。不一致は保存ごと中止して 409 にする
      if (job.report_type === 'monthly' && job.edition !== 'provisional') {
        const ym = job.period_start.slice(0, 7);
        const updated = db.prepare(`
          UPDATE ai_monthly_closing
          SET final_report_id = ?, final_declare_seq = ?,
              final_pl_hash = ?, change_notified_at = NULL, updated_at = ?
          WHERE year_month = ? AND declare_seq = ?
        `).run(reportId, job.declare_seq, b.pl_hash || null, now, ym, job.declare_seq);
        if (updated.changes !== 1) {
          const err = new Error('stale_declare_seq');
          err.staleDeclare = true;
          throw err;
        }
      }
    })();
  } catch (e) {
    if (e.staleDeclare) {
      // 生成中に再オープン→再宣言された。古い生成は破棄し、runner は入力を取り直す
      return res.status(409).json({
        error: 'declare_seq changed during generation (再宣言により旧生成は破棄)。入力を取り直して再実行してください',
        error_class: 'stale_declare_seq',
      });
    }
    if (/UNIQUE constraint/.test(e.message)) {
      // 同一 (type, period, edition) のレポートが既に存在 = リラン。既存を返す
      const existing = db.prepare(`
        SELECT * FROM ai_reports WHERE report_type = ? AND period_start = ? AND period_end = ? AND edition = ?
      `).get(job.report_type, job.period_start, job.period_end, job.edition);
      return res.status(409).json({ error: 'report already exists for this period/edition', existing });
    }
    throw e;
  }
  res.json({ ok: true, report_id: reportId, public_id: publicId });
});

serviceRouter.post('/jobs/:jobId/posting', (req, res) => {
  const db = getMirrorDB();
  const now = nowIso();
  const r = db.prepare(`
    UPDATE ai_report_jobs SET status = 'posting', heartbeat_at = ?, updated_at = ?
    WHERE job_id = ? AND status = 'generated'
  `).run(now, now, req.params.jobId);
  if (r.changes !== 1) {
    const job = getJob(db, req.params.jobId);
    return res.status(409).json({ error: `cannot transition to posting from ${job?.status || 'missing'}` });
  }
  res.json({ ok: true });
});

serviceRouter.post('/jobs/:jobId/posted', (req, res) => {
  const db = getMirrorDB();
  const job = getJob(db, req.params.jobId);
  if (!job) return res.status(404).json({ error: 'job not found' });
  const now = nowIso();
  // ジョブ遷移とレポート側の投稿記録は 1 トランザクション (中間クラッシュで不整合を残さない)
  let changed = 0;
  db.transaction(() => {
    changed = db.prepare(`
      UPDATE ai_report_jobs SET status = 'posted', updated_at = ? WHERE job_id = ? AND status = 'posting'
    `).run(now, req.params.jobId).changes;
    if (changed === 1 && job.report_id) {
      db.prepare(`
        UPDATE ai_reports SET gchat_message_id = ?, body_hash = ?, posted_at = ? WHERE report_id = ?
      `).run(req.body?.gchat_message_id || null, req.body?.body_hash || null, now, job.report_id);
    }
  })();
  if (changed !== 1) {
    return res.status(409).json({ error: `cannot mark posted from ${job.status}` });
  }
  res.json({ ok: true });
});

// 失敗の申告。posting→failed は「配信されていないと runner が確信できる場合のみ」
// (送信前の失敗など)。不確実なら申告せず放置 → 孤児sweepが reconciliation_required へ送る
serviceRouter.post('/jobs/:jobId/failed', (req, res) => {
  const db = getMirrorDB();
  const now = nowIso();
  const r = db.prepare(`
    UPDATE ai_report_jobs SET status = 'failed', error_class = ?, error_detail = ?, updated_at = ?
    WHERE job_id = ? AND status IN ('generating', 'posting')
  `).run(
    String(req.body?.error_class || 'unknown').slice(0, 50),
    String(req.body?.error_detail || '').slice(0, 2000),
    now, req.params.jobId,
  );
  if (r.changes !== 1) {
    const job = getJob(db, req.params.jobId);
    return res.status(409).json({ error: `cannot mark failed from ${job?.status || 'missing'}` });
  }
  res.json({ ok: true });
});

// ════════════════════════════════════════════════════════════════════
// ② ⚙️経営設定画面ルーター (/apps/ai-insights、session 認証は server.js 側)
// ════════════════════════════════════════════════════════════════════

const router = express.Router();

router.get('/', ensureReady, (req, res) => {
  res.render('ai-insights', {
    title: 'AI経営レポート設定',
    username: req.session?.username || '',
    displayName: req.session?.displayName || req.session?.username || '',
  });
});

router.get('/api/settings', ensureReady, (req, res) => {
  const db = getMirrorDB();
  sweepOrphans(db);
  const budget = db.prepare(`
    SELECT year_month, metric, value_yen, revision_no, entered_by, entered_at
    FROM v_ai_budget_current WHERE budget_kind = 'initial'
    ORDER BY year_month, metric
  `).all();
  const events = db.prepare(`
    SELECT event_id, event_date, event_end_date, description, tag, scope, entered_by, entered_at
    FROM ai_events WHERE deleted_at IS NULL
    ORDER BY event_date DESC, entered_at DESC LIMIT 200
  `).all();
  const sources = db.prepare(`
    SELECT source_id, source_kind, unit, requirement, valid_from, valid_to, note
    FROM ai_expected_sources ORDER BY source_kind, unit
  `).all();
  const reconciliation = db.prepare(`
    SELECT j.*, r.public_id FROM ai_report_jobs j
    LEFT JOIN ai_reports r ON r.report_id = j.report_id
    WHERE j.status = 'reconciliation_required'
    ORDER BY j.updated_at DESC
  `).all();
  const recentJobs = db.prepare(`
    SELECT j.job_id, j.report_type, j.period_start, j.period_end, j.edition, j.status,
           j.error_class, j.updated_at, r.public_id, r.posted_at
    FROM ai_report_jobs j
    LEFT JOIN ai_reports r ON r.report_id = j.report_id
    ORDER BY j.updated_at DESC LIMIT 20
  `).all();
  // 月次締め: 直近3ヶ月分 (行が無い月は open として合成)
  const closingMonths = [0, 1, 2].map((i) => {
    const [y, m] = previousMonthYm().split('-').map(Number);
    const t = y * 12 + (m - 1) - i;
    return `${Math.floor(t / 12)}-${String((t % 12) + 1).padStart(2, '0')}`;
  });
  const closingRows = db.prepare(`
    SELECT * FROM ai_monthly_closing WHERE year_month IN (?, ?, ?)
  `).all(...closingMonths);
  const jobsByMonth = db.prepare(`
    SELECT period_start, edition, status FROM ai_report_jobs WHERE report_type = 'monthly'
  `).all();
  const closing = closingMonths.map((ym) => {
    const row = closingRows.find((r) => r.year_month === ym) || { year_month: ym, status: 'open' };
    const editions = jobsByMonth
      .filter((j) => j.period_start === `${ym}-01`)
      .map((j) => ({ edition: j.edition, status: j.status }));
    return { ...row, editions };
  });
  res.json({ budget, events, sources, reconciliation, recent_jobs: recentJobs, closing });
});

/**
 * 月次締めの確定宣言 (要件 §6)。宣言後、次の MF 同期成功のスナップショットで確定版が発行される。
 * 「確定 = 人間による締め状態の宣言。会計データの完全性保証ではない」(UI にも明記)
 */
router.post('/api/closing/:ym/declare', ensureReady, (req, res) => {
  const ym = req.params.ym;
  if (!isYmStrict(ym)) return res.status(400).json({ error: 'ym must be YYYY-MM' });
  if (ym > previousMonthYm()) {
    return res.status(400).json({ error: '締め宣言できるのは前月以前の月だけです (当月・未来月は不可)' });
  }
  const db = getMirrorDB();
  const now = nowIso();
  const user = sessionUser(req);
  db.prepare(`
    INSERT OR IGNORE INTO ai_monthly_closing (year_month, status, updated_at) VALUES (?, 'open', ?)
  `).run(ym, now);
  const r = db.prepare(`
    UPDATE ai_monthly_closing
    SET status = 'declared', declare_seq = declare_seq + 1,
        declared_by = ?, declared_at = ?, updated_at = ?
    WHERE year_month = ? AND status IN ('open', 'reopened')
  `).run(user, now, now, ym);
  if (r.changes !== 1) {
    const cur = db.prepare('SELECT status FROM ai_monthly_closing WHERE year_month = ?').get(ym);
    return res.status(409).json({ error: `既に ${cur?.status} 状態です` });
  }
  res.json({ ok: true, status: 'declared' });
});

/** 誤操作時の再オープン。既存確定版は superseded として保持 (再宣言で correction 版が出る) */
router.post('/api/closing/:ym/reopen', ensureReady, (req, res) => {
  const ym = req.params.ym;
  if (!isYmStrict(ym)) return res.status(400).json({ error: 'ym must be YYYY-MM' });
  const db = getMirrorDB();
  const now = nowIso();
  const r = db.prepare(`
    UPDATE ai_monthly_closing
    SET status = 'reopened', reopened_by = ?, reopened_at = ?, updated_at = ?
    WHERE year_month = ? AND status = 'declared'
  `).run(sessionUser(req), now, now, ym);
  if (r.changes !== 1) return res.status(409).json({ error: 'declared 状態の月のみ再オープンできます' });
  res.json({ ok: true, status: 'reopened' });
});

/**
 * 予算の一括保存 (append-only revision)。
 * body: { entries: [{year_month, metric, value_yen}], reason }
 * 空欄 = entries に含めない (0 と区別。要件 §9.1)。同値の再送は新 revision を作らない
 */
router.post('/api/budget', ensureReady, (req, res) => {
  const db = getMirrorDB();
  const entries = Array.isArray(req.body?.entries) ? req.body.entries : null;
  if (!entries || entries.length === 0) {
    return res.status(400).json({ error: 'entries required' });
  }
  if (entries.length > 200) return res.status(400).json({ error: 'entries: max 200' });
  for (const e of entries) {
    if (!isYm(e?.year_month)) return res.status(400).json({ error: `invalid year_month: ${e?.year_month}` });
    if (!BUDGET_METRICS.includes(e?.metric)) return res.status(400).json({ error: `invalid metric: ${e?.metric}` });
    if (!Number.isInteger(e?.value_yen) || Math.abs(e.value_yen) >= MAX_VALUE_YEN) {
      return res.status(400).json({ error: `value_yen must be integer yen (${e.year_month}/${e.metric})` });
    }
  }
  const reason = String(req.body?.reason || '').slice(0, 500);
  const user = sessionUser(req);
  const now = nowIso();
  let saved = 0, unchanged = 0;
  db.transaction(() => {
    const currentStmt = db.prepare(`
      SELECT value_yen, revision_no FROM v_ai_budget_current
      WHERE year_month = ? AND metric = ? AND budget_kind = 'initial'
    `);
    const insertStmt = db.prepare(`
      INSERT INTO ai_budget
        (budget_row_id, year_month, metric, budget_kind, value_yen, revision_no, reason, entered_by, entered_at)
      VALUES (?, ?, ?, 'initial', ?, ?, ?, ?, ?)
    `);
    for (const e of entries) {
      const cur = currentStmt.get(e.year_month, e.metric);
      if (cur && cur.value_yen === e.value_yen) { unchanged++; continue; }
      insertStmt.run(newId('bg'), e.year_month, e.metric, e.value_yen,
        (cur?.revision_no || 0) + 1, reason || null, user, now);
      saved++;
    }
  })();
  res.json({ ok: true, saved, unchanged });
});

/**
 * 予算プリフィル用の実績 (前年実績×成長率で仮予算を作る材料。要件 §13-2)
 * MF月次PL から売上/粗利/営業利益。SQL は exec-dashboard /api/timeseries と同一定義
 */
router.get('/api/budget/actuals', ensureReady, (req, res) => {
  const year = String(req.query.year || '');
  if (!/^\d{4}$/.test(year)) return res.status(400).json({ error: 'year=YYYY required' });
  const months = Array.from({ length: 12 }, (_, i) => `${year}-${String(i + 1).padStart(2, '0')}`);
  const db = getMirrorDB();
  const placeholders = months.map(() => '?').join(',');
  const rows = db.prepare(`
    SELECT month_ym,
      SUM(CASE WHEN role_key IN ('sales','sales_export','sales_wholesale') THEN amount_excl_tax ELSE 0 END)
      - SUM(CASE WHEN role_key='sales_return' THEN amount_excl_tax ELSE 0 END) AS sales,
      SUM(CASE WHEN role_key LIKE 'cogs_%' OR role_key LIKE 'inventory_%' THEN amount_excl_tax ELSE 0 END) AS cogs,
      SUM(CASE WHEN role_key LIKE 'sgae_%' THEN amount_excl_tax ELSE 0 END) AS sgae
    FROM v_mirror_mf_pl_monthly_latest
    WHERE month_ym IN (${placeholders})
    GROUP BY month_ym ORDER BY month_ym
  `).all(...months);
  const actuals = rows.map((r) => {
    const sales = Math.round(r.sales || 0);
    const gp = Math.round((r.sales || 0) - (r.cogs || 0));
    return {
      year_month: r.month_ym,
      sales, gross_profit: gp,
      operating_income: Math.round(gp - (r.sgae || 0)),
    };
  });
  res.json({ year, actuals, note: 'MF月次PL実績 (税抜)。cash_eom / inventory_value のプリフィルは対象外' });
});

router.post('/api/events', ensureReady, (req, res) => {
  const b = req.body || {};
  if (!isYmd(b.event_date)) return res.status(400).json({ error: 'event_date must be YYYY-MM-DD' });
  if (b.event_end_date != null && b.event_end_date !== '') {
    if (!isYmd(b.event_end_date)) return res.status(400).json({ error: 'event_end_date must be YYYY-MM-DD' });
    if (b.event_end_date < b.event_date) return res.status(400).json({ error: 'event_end_date must be >= event_date' });
  }
  const description = String(b.description || '').trim();
  if (!description) return res.status(400).json({ error: 'description required' });
  if (description.length > 300) return res.status(400).json({ error: 'description: max 300 chars' });
  if (b.tag != null && b.tag !== '' && !EVENT_TAGS.includes(b.tag)) {
    return res.status(400).json({ error: `tag must be one of ${EVENT_TAGS.join(',')}` });
  }
  const db = getMirrorDB();
  const id = newId('ev');
  db.prepare(`
    INSERT INTO ai_events (event_id, event_date, event_end_date, description, tag, scope, entered_by, entered_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `).run(id, b.event_date, b.event_end_date || null, description,
    b.tag || null, String(b.scope || 'all').slice(0, 50), sessionUser(req), nowIso());
  res.json({ ok: true, event_id: id });
});

router.post('/api/events/:eventId/delete', ensureReady, (req, res) => {
  const db = getMirrorDB();
  const r = db.prepare(`
    UPDATE ai_events SET deleted_at = ?, deleted_by = ? WHERE event_id = ? AND deleted_at IS NULL
  `).run(nowIso(), sessionUser(req), req.params.eventId);
  if (r.changes !== 1) return res.status(404).json({ error: 'event not found (or already deleted)' });
  res.json({ ok: true });
});

/**
 * reconciliation_required の人間照合 (要件 §8.1)。
 * action: 'mark_posted' (GChatで投稿を確認できた) | 'repost' (未投稿と確認 → pending に戻す)
 */
router.post('/api/jobs/:jobId/reconcile', ensureReady, (req, res) => {
  const action = req.body?.action;
  if (!['mark_posted', 'repost'].includes(action)) {
    return res.status(400).json({ error: "action must be 'mark_posted' or 'repost'" });
  }
  const db = getMirrorDB();
  const now = nowIso();
  const user = sessionUser(req);
  const to = action === 'mark_posted' ? 'posted' : 'pending';
  // ジョブ遷移とレポート側の記録は 1 トランザクション (/posted と同じ理由)
  let changed = 0;
  db.transaction(() => {
    const job = getJob(db, req.params.jobId);
    changed = db.prepare(`
      UPDATE ai_report_jobs
      SET status = ?, resolved_by = ?, resolved_at = ?, updated_at = ?,
          claimed_by = NULL, claimed_at = NULL, heartbeat_at = NULL
      WHERE job_id = ? AND status = 'reconciliation_required'
    `).run(to, user, now, now, req.params.jobId).changes;
    if (changed === 1 && action === 'mark_posted' && job?.report_id) {
      db.prepare('UPDATE ai_reports SET posted_at = COALESCE(posted_at, ?) WHERE report_id = ?')
        .run(now, job.report_id);
    }
  })();
  if (changed !== 1) return res.status(409).json({ error: 'job is not in reconciliation_required' });
  res.json({ ok: true, status: to });
});

export default router;
