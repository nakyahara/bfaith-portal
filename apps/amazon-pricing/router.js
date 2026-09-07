/**
 * Amazon 価格管理 (自社プライスター) — 画面 + API
 *
 * 🚨このアプリは Amazon に何も書き込まない。SP-API の書き込み関数も、miniPC の書き込み口 (research-service.js の
 *   価格更新ルート) を呼ぶコードも、ここには存在しない (test-no-write-path.mjs が機械的に確認する)。
 *   できるのは:
 *     1. 出品ごとの値付け方針 (追従モード・赤字/高値ストッパー・上乗せ・最低粗利率) を決めて、変更履歴つきで記録する
 *     2. ルール (engine.js) が「もし動くならこうする」を毎日出す (判定 = 提案) → 人が 👍/👎 で採点する
 *     3. 1 出品の全部 (価格・カート・原価・手数料・粗利・販売・方針・判定) を 1 行で見る
 *   価格を実際に変える段階 (人が承認 → miniPC 経由で 1 件送る) は別 PR。設計 = AI_reference
 *   『システム設計/Amazon価格管理_自社プライスター_要件定義_20260907.md』。
 *
 * CSRF: price-update と同じ二段ガード (非GET は Origin 一致必須 403 + application/json 以外 415)。
 */
import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import {
  getDB, savePolicy, listPolicyEvents, countPolicyEvents, latestSuccessRun, evaluationsOfRun, evaluationsForSku,
  addReview, reviewStats, listRuns, REASON_CODES, REVIEW_VERDICTS, POLICY_FIELDS,
} from './db.js';
import { loadListings, loadListing, priceHistory, dataFreshness, mirrorTablesAvailable } from './read-model.js';
import { evaluateListing, MODES, ACTIONS, REASONS, FLAGS, RULE_VERSION, DEFAULT_MIN_MARGIN_RATE } from './engine.js';
import { inputOf, runEvaluation, ensureEvaluation } from './evaluate.js';
import { toJst } from '../price-update/format.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = express.Router();
const view = (name) => path.join(__dirname, 'views', name);

// ─── CSRF 二段ガード ───
router.use('/api/', (req, res, next) => {
  if (['GET', 'HEAD', 'OPTIONS'].includes(req.method)) return next();
  const origin = req.headers.origin;
  let host = null;
  try { host = origin ? new URL(origin).host : null; } catch { /* 壊れた Origin は不一致扱い */ }
  if (!host || host !== req.headers.host) {
    return res.status(403).json({ ok: false, error: 'origin_mismatch', message: 'ブラウザから操作してください (Origin ヘッダが必要です)' });
  }
  if (!/^application\/json\b/i.test(String(req.headers['content-type'] || ''))) {
    return res.status(415).json({ ok: false, error: 'Content-Type は application/json にしてください' });
  }
  next();
});
router.use(express.json({ limit: '256kb' }));

const actorOf = (req) => req.session?.email || req.session?.displayName || 'unknown';

export const FIELD_LABELS = {
  mode: '追従モード', floor_price: '赤字ストッパー', ceiling_price: '高値ストッパー',
  offset_jpy: '上乗せ', min_margin_rate: '最低粗利率', note: 'メモ',
};

/** 画面が使う整形ヘルパー (テストも同じものを渡す) */
export const VIEW_HELPERS = {
  toJst,
  yen: (v) => (v == null || v === '' || Number.isNaN(Number(v)) ? '—' : `${Math.round(Number(v)).toLocaleString()}`),
  pct: (v, digits = 1) => (v == null || Number.isNaN(Number(v)) ? '—' : `${(Number(v) * 100).toFixed(digits)}%`),
  num: (v) => (v == null || Number.isNaN(Number(v)) ? '—' : Number(v).toLocaleString()),
  MODES, ACTIONS, REASONS, FLAGS, REASON_CODES, REVIEW_VERDICTS, FIELD_LABELS, POLICY_FIELDS, RULE_VERSION, DEFAULT_MIN_MARGIN_RATE,
  fieldValue: (field, v) => {
    if (v == null || v === '') return '(なし)';
    if (field === 'mode') return MODES[v] || v;
    if (field === 'min_margin_rate') return `${(Number(v) * 100).toFixed(1)}%`;
    if (field === 'note') return v;
    return `${Number(v).toLocaleString()} 円`;
  },
};

function common(req, title, nav) {
  return {
    title, nav,
    displayName: req.session?.displayName || req.session?.email || '',
    isAdmin: req.session?.role === 'admin',
    ...VIEW_HELPERS,
  };
}

/** 360 行にいまの判定 (方針の現在値で計算) を付ける */
export function enrich(row) {
  const live = evaluateListing(inputOf(row));
  return {
    ...row,
    live,
    computed_floor: live.computedFloor,
    effective_floor: live.effectiveFloor,
    gross_now: live.grossNow,
    buybox_gap: (row.my_price != null && row.buybox_price != null) ? Math.round(row.my_price - row.buybox_price) : null,
    has_policy: !!row.policy_updated_at,
  };
}

const FLAG_FILTERS = {
  below_floor: (r) => r.live.flags.includes('BELOW_COST_FLOOR') || r.live.flags.includes('BELOW_STOPPER'),
  cost_unknown: (r) => r.live.flags.includes('COST_UNKNOWN'),
  buybox_lost: (r) => r.buybox_is_mine === 0,
  no_price: (r) => r.my_price == null,
  low_margin: (r) => r.gross_now.rate != null && r.gross_now.rate < (r.min_margin_rate ?? DEFAULT_MIN_MARGIN_RATE),
  no_sales: (r) => r.live.flags.includes('NO_SALES_30D'),
};
export const FLAG_FILTER_LABELS = {
  below_floor: '⚠️ 赤字の疑い (今の価格が下限より低い)', cost_unknown: '原価不明', buybox_lost: 'カートを他社が持っている',
  no_price: '価格が取れていない', low_margin: '粗利率が最低粗利率を下回る', no_sales: '30日売れていない',
};

const SORTS = {
  units: (a, b) => (b.units_30d ?? -1) - (a.units_30d ?? -1) || a.seller_sku.localeCompare(b.seller_sku),
  price: (a, b) => (b.my_price ?? -1) - (a.my_price ?? -1),
  gap: (a, b) => (b.buybox_gap ?? -Infinity) - (a.buybox_gap ?? -Infinity),
  margin: (a, b) => (a.gross_now.rate ?? Infinity) - (b.gross_now.rate ?? Infinity),
  sku: (a, b) => a.seller_sku.localeCompare(b.seller_sku),
};

/** 絞り込み・並び替え (画面と CSV で同じ) */
export function applyFilters(rows, q) {
  const text = String(q.q || '').trim().toLowerCase();
  let out = rows;
  if (text) {
    out = out.filter((r) => [r.seller_sku, r.asin, r.ne_name, r.ne_code].some((v) => v && String(v).toLowerCase().includes(text)));
  }
  if (q.mode) {
    if (q.mode === 'set') out = out.filter((r) => r.has_policy);
    else if (q.mode === 'unset') out = out.filter((r) => !r.has_policy);
    else out = out.filter((r) => r.mode === q.mode);
  }
  if (q.channel) out = out.filter((r) => String(r.channel || '').toUpperCase() === String(q.channel).toUpperCase());
  if (q.action) out = out.filter((r) => r.live.action === q.action);
  if (q.flag && FLAG_FILTERS[q.flag]) out = out.filter(FLAG_FILTERS[q.flag]);
  const sort = SORTS[q.sort] || SORTS.units;
  return [...out].sort(sort);
}

function statsOf(rows) {
  const s = { total: rows.length, with_policy: 0, by_action: { raise: 0, lower: 0, keep: 0, hold: 0 }, flags: {} };
  for (const r of rows) {
    if (r.has_policy) s.with_policy += 1;
    s.by_action[r.live.action] += 1;
    for (const k of Object.keys(FLAG_FILTERS)) if (FLAG_FILTERS[k](r)) s.flags[k] = (s.flags[k] || 0) + 1;
  }
  return s;
}

function apiError(res, e, where) {
  if (e?.code === 'VALIDATION') return res.status(400).json({ ok: false, error: e.message });
  if (e?.code === 'NO_MIRROR') return res.status(503).json({ ok: false, error: e.message });
  console.error(`[amazon-pricing] ${where}:`, e);
  return res.status(500).json({ ok: false, error: 'サーバーエラーが発生しました' });
}

// ─── 画面 ────────────────────────────────────────────────

router.get('/', (req, res) => {
  const db = getDB();
  const avail = mirrorTablesAvailable(db);
  const base = common(req, 'Amazon 価格管理', 'index');
  if (!avail.ok) {
    return res.render(view('index.ejs'), {
      ...base, unavailable: avail.missing, rows: [], total: 0, page: 1, per: 100, pages: 1,
      filters: {}, stats: statsOf([]), run: null, freshness: null, auto: null, flagLabels: FLAG_FILTER_LABELS,
    });
  }
  // ★表示の絞り込み・LIMIT より前に、全出品を対象に「今日の判定」を作っておく
  const auto = ensureEvaluation(db, actorOf(req));
  const run = latestSuccessRun(db);
  const all = loadListings(db).map(enrich);
  const filters = {
    q: String(req.query.q || ''), mode: String(req.query.mode || ''), channel: String(req.query.channel || ''),
    action: String(req.query.action || ''), flag: String(req.query.flag || ''), sort: String(req.query.sort || 'units'),
  };
  const filtered = applyFilters(all, filters);
  const per = [50, 100, 300].includes(Number(req.query.per)) ? Number(req.query.per) : 100;
  const pages = Math.max(1, Math.ceil(filtered.length / per));
  const page = Math.min(pages, Math.max(1, parseInt(req.query.page, 10) || 1));
  res.render(view('index.ejs'), {
    ...base, unavailable: null,
    rows: filtered.slice((page - 1) * per, page * per), total: filtered.length, page, per, pages, filters,
    stats: statsOf(all), run, freshness: dataFreshness(db), auto, flagLabels: FLAG_FILTER_LABELS,
  });
});

router.get('/listings/:sku', (req, res) => {
  const db = getDB();
  const base = common(req, `出品 ${req.params.sku}`, 'index');
  const avail = mirrorTablesAvailable(db);
  const raw = avail.ok ? loadListing(db, req.params.sku) : null;
  if (!raw) return res.status(404).render(view('listing.ejs'), { ...base, row: null, events: [], evaluations: [], history: [] });
  const row = enrich(raw);
  res.render(view('listing.ejs'), {
    ...base, title: `出品 ${row.ne_name || row.seller_sku}`, row,
    events: listPolicyEvents(db, { sku: row.seller_sku, limit: 100 }),
    evaluations: evaluationsForSku(db, row.seller_sku, 30),
    history: priceHistory(db, row.seller_sku, 90),
  });
});

router.get('/evaluations', (req, res) => {
  const db = getDB();
  const base = common(req, '今日の判定', 'evaluations');
  const avail = mirrorTablesAvailable(db);
  const auto = avail.ok ? ensureEvaluation(db, actorOf(req)) : { skipped: true, run: null, error: `表がありません: ${avail.missing.join(', ')}` };
  const run = latestSuccessRun(db);
  const evals = run ? evaluationsOfRun(db, run.run_id) : [];
  const tab = ['raise', 'lower', 'hold', 'keep', 'all', 'reviewed'].includes(req.query.tab) ? req.query.tab : 'change';
  let shown = evals;
  if (tab === 'change') shown = evals.filter((e) => e.action === 'raise' || e.action === 'lower');
  else if (tab === 'reviewed') shown = evals.filter((e) => e.review_verdict);
  else if (tab !== 'all') shown = evals.filter((e) => e.action === tab);
  const limit = 300;
  res.render(view('evaluations.ejs'), {
    ...base, run, auto, tab, evals: shown.slice(0, limit), shownTotal: shown.length, limit,
    counts: { all: evals.length, change: evals.filter((e) => e.action === 'raise' || e.action === 'lower').length,
      raise: evals.filter((e) => e.action === 'raise').length, lower: evals.filter((e) => e.action === 'lower').length,
      hold: evals.filter((e) => e.action === 'hold').length, keep: evals.filter((e) => e.action === 'keep').length,
      reviewed: evals.filter((e) => e.review_verdict).length },
    reviews: run ? reviewStats(db, run.run_id) : { agree: 0, disagree: 0, unsure: 0 },
    runs: listRuns(db, 10),
  });
});

router.get('/history', (req, res) => {
  const db = getDB();
  const sku = String(req.query.sku || '').trim();
  const events = listPolicyEvents(db, { sku: sku || null, limit: 300 });
  // 1 回の保存でまとめて変わった列を束ねる
  const groups = [];
  const byGroup = new Map();
  for (const e of events) {
    if (!byGroup.has(e.change_group)) {
      const g = { change_group: e.change_group, at: e.at, actor_id: e.actor_id, actor_type: e.actor_type, seller_sku: e.seller_sku,
        reason_code: e.reason_code, reason_text: e.reason_text, source: e.source, changes: [] };
      byGroup.set(e.change_group, g);
      groups.push(g);
    }
    byGroup.get(e.change_group).changes.push(e);
  }
  res.render(view('history.ejs'), { ...common(req, '方針の変更履歴', 'history'), groups, sku, total: countPolicyEvents(db) });
});

// ─── API ─────────────────────────────────────────────────

/** 方針の保存 (変わった列だけ履歴に残る) */
router.post('/api/policies/:sku', (req, res) => {
  try {
    const db = getDB();
    const sku = String(req.params.sku || '').trim();
    const raw = loadListing(db, sku);
    if (!raw) throw Object.assign(new Error('その SKU の出品が見つかりません (mirror_amazon_sku_fees に無い)'), { code: 'VALIDATION' });
    const body = req.body || {};
    const patch = {};
    for (const f of POLICY_FIELDS) if (f in body) patch[f] = body[f];
    const result = savePolicy(db, {
      sku, patch, actorId: actorOf(req), reasonCode: String(body.reason_code || ''), reasonText: body.reason_text ?? null, source: 'ui',
    });
    const row = enrich(loadListing(db, sku));
    res.json({ ok: true, changed: result.changed, policy: result.policy, live: publicLive(row) });
  } catch (e) { apiError(res, e, 'policies'); }
});

/** いま判定を作り直す (同じ日でも新しい run を作る) */
router.post('/api/evaluations/run', (req, res) => {
  try {
    const db = getDB();
    const r = runEvaluation(db, { trigger: 'manual', actorId: actorOf(req), force: true });
    res.json({ ok: true, run: r.run, summary: r.summary });
  } catch (e) { apiError(res, e, 'evaluations/run'); }
});

/** 判定の採点 (追記のみ。押し直しは新しい行になる) */
router.post('/api/evaluations/:id/review', (req, res) => {
  try {
    const db = getDB();
    const id = parseInt(req.params.id, 10);
    if (!Number.isInteger(id)) throw Object.assign(new Error('判定 ID が不正です'), { code: 'VALIDATION' });
    const reviewId = addReview(db, { decisionId: id, reviewerId: actorOf(req), verdict: String(req.body?.verdict || ''), comment: req.body?.comment ?? null });
    res.json({ ok: true, review_id: reviewId });
  } catch (e) { apiError(res, e, 'review'); }
});

/** 一覧を JSON で (AI・スクリプト向け。画面と同じ絞り込みが効く) */
router.get('/api/listings.json', (req, res) => {
  try {
    const db = getDB();
    const avail = mirrorTablesAvailable(db);
    if (!avail.ok) return res.status(503).json({ ok: false, error: `表がありません: ${avail.missing.join(', ')}` });
    const rows = applyFilters(loadListings(db).map(enrich), req.query).map((r) => ({ ...stripLive(r), live: publicLive(r) }));
    res.json({ ok: true, rule_version: RULE_VERSION, freshness: dataFreshness(db), count: rows.length, rows });
  } catch (e) { apiError(res, e, 'listings.json'); }
});

/** 一覧を CSV で (BOM 付き UTF-8。Excel で開ける) */
router.get('/api/export.csv', (req, res) => {
  try {
    const db = getDB();
    const avail = mirrorTablesAvailable(db);
    if (!avail.ok) return res.status(503).send(`表がありません: ${avail.missing.join(', ')}`);
    const rows = applyFilters(loadListings(db).map(enrich), req.query);
    const cols = ['seller_sku', 'asin', 'channel', 'ne_code', 'ne_name', 'my_price', 'buybox_price', 'buybox_is_mine', 'cost_incl_tax',
      'referral_fee_rate', 'fba_fee', 'ship_cost', 'gross_now', 'gross_rate_now', 'units_30d', 'computed_floor', 'floor_price', 'ceiling_price',
      'offset_jpy', 'min_margin_rate', 'mode', 'action', 'proposed_price', 'reason_code', 'reason_text', 'confidence', 'flags', 'snapshot_date_jst'];
    const esc = (v) => {
      if (v == null) return '';
      const s = String(v);
      return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
    };
    const lines = [cols.join(',')];
    for (const r of rows) {
      lines.push(cols.map((c) => {
        if (c === 'gross_now') return esc(r.gross_now.gross);
        if (c === 'gross_rate_now') return esc(r.gross_now.rate == null ? null : Math.round(r.gross_now.rate * 1000) / 1000);
        if (c === 'action') return esc(r.live.action);
        if (c === 'proposed_price') return esc(r.live.proposedPrice);
        if (c === 'reason_code') return esc(r.live.reasonCode);
        if (c === 'reason_text') return esc(r.live.reasonText);
        if (c === 'confidence') return esc(r.live.confidence);
        if (c === 'flags') return esc(r.live.flags.join('|'));
        return esc(r[c]);
      }).join(','));
    }
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="amazon-pricing-${new Date().toISOString().slice(0, 10)}.csv"`);
    res.send('﻿' + lines.join('\r\n'));
  } catch (e) { apiError(res, e, 'export.csv'); }
});

/** 稼働状況 (鮮度・最新 run・件数) */
router.get('/api/health', (req, res) => {
  try {
    const db = getDB();
    const avail = mirrorTablesAvailable(db);
    res.json({
      ok: true, writes_to_amazon: false, rule_version: RULE_VERSION,
      mirror_tables: avail, freshness: avail.ok ? dataFreshness(db) : null,
      latest_run: latestSuccessRun(db), policy_events: countPolicyEvents(db),
    });
  } catch (e) { apiError(res, e, 'health'); }
});

function publicLive(r) {
  const l = r.live;
  return {
    action: l.action, proposed_price: l.proposedPrice, current_price: l.currentPrice, reason_code: l.reasonCode,
    reason_text: l.reasonText, confidence: l.confidence, flags: l.flags, computed_floor: l.computedFloor,
    effective_floor: l.effectiveFloor, gross_now: l.grossNow,
  };
}
function stripLive(r) {
  const { live, ...rest } = r;
  return rest;
}

export default router;
