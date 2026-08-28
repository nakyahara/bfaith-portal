/**
 * 新商品企画スカウト (apps/product-scout) DBレイヤー
 *
 * warehouse-mirror.db 同居の scout_* を扱う。接続は warehouse-mirror の getMirrorDB() を共有。
 *
 * ⭐このアプリの正本はここ (Render) の1箇所だけ。
 *   Googleスプレッドシートやローカルの CSV は「読み取り専用のエクスポート先」であって正本ではない。
 *   正本を2箇所に置くと、片方の書き込みが失敗したときにどちらが正しいか誰にも言えなくなる。
 *
 * ⭐採否は scout_decisions への追記だけで表現する。
 *   scout_concepts に「現在の状態」列を持たせて上書きすると、
 *   「以前は不採用 → 条件が変わって再審査 → 採用」という経過が残らない。
 *   不採用理由の蓄積こそが資産なので、履歴を消せない構造にしてある (DBトリガーで UPDATE/DELETE 禁止)。
 */
import crypto from 'node:crypto';
import { getMirrorDB } from '../warehouse-mirror/db.js';

// テストと本番で同じコードを通すため、DBハンドルを差し替えられるようにしておく。
// 既定は warehouse-mirror の共有接続 (本番はこれしか使わない)。
function resolveDb(handle) { return handle || getMirrorDB(); }


/** テーマの安定キー。取り込み直しても同じテーマには同じ id が付き、過去の判断と繋がる */
export function conceptIdOf(categoryPath, form) {
  return crypto.createHash('sha1').update(`${categoryPath}||${form}`).digest('hex').slice(0, 24);
}

export function utcIsoNow() {
  return new Date().toISOString();
}

const JST_FMT = new Intl.DateTimeFormat('en-CA', {
  timeZone: 'Asia/Tokyo', year: 'numeric', month: '2-digit', day: '2-digit',
});
/** JST の 'YYYY-MM-DD' (UTC環境でも正しい。toISOString() は月初がずれる) */
export function jstDate(d = new Date()) {
  return JST_FMT.format(d);
}

// ─────────────────────────────────────────────────────────────
// 取り込み (miniPC の product-idea-scout から push される)
// ─────────────────────────────────────────────────────────────

/**
 * スナップショットを1件取り込む。全体を1トランザクションで入れる。
 * 途中で落ちたら丸ごと無かったことにする — 半分だけ新しい画面を人に見せない。
 */
export function ingestSnapshot(payload, handle) {
  const db = resolveDb(handle);
  const now = utcIsoNow();
  const snapshotId = payload.snapshotId
    || crypto.createHash('sha1').update(`${payload.generatedAt}|${now}`).digest('hex').slice(0, 16);

  const insSnap = db.prepare(`
    INSERT INTO scout_snapshots (snapshot_id, generated_at, ingested_at, algorithm_version,
                                 source_products, after_base_filter, concept_count, last_progress_at, remaining_total)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(snapshot_id) DO UPDATE SET
      ingested_at = excluded.ingested_at, concept_count = excluded.concept_count,
      last_progress_at = excluded.last_progress_at, remaining_total = excluded.remaining_total
  `);
  const insCat = db.prepare(`
    INSERT INTO scout_categories (snapshot_id, root_category, name, state, asin_target, fetched,
                                  complete, estimated_missing, fetched_at, remaining)
    VALUES (@snapshotId, @rootCategory, @name, @state, @asinTarget, @fetched,
            @complete, @estimatedMissing, @fetchedAt, @remaining)
    ON CONFLICT(snapshot_id, root_category) DO UPDATE SET
      name = excluded.name, state = excluded.state, asin_target = excluded.asin_target,
      fetched = excluded.fetched, complete = excluded.complete,
      estimated_missing = excluded.estimated_missing, fetched_at = excluded.fetched_at,
      remaining = excluded.remaining
  `);
  const insConcept = db.prepare(`
    INSERT INTO scout_concepts (
      concept_id, snapshot_id, concept, category_path, root_category_name, form,
      amc_capable, hard_gate, gate_fail_reason, commodity, big_brand,
      product_count, total_monthly_sold, brand_count, top1_brand, top1_share_pct, top3_share_pct,
      median_price, median_fee_pct, small_size_rate_pct, ascii_brand_rate_pct, median_review_count,
      source_complete, source_fetched_at, examples_json, rank_in_snapshot, first_seen_at, updated_at)
    VALUES (
      @conceptId, @snapshotId, @concept, @categoryPath, @rootCategoryName, @form,
      @amcCapable, @hardGate, @gateFailReason, @commodity, @bigBrand,
      @productCount, @totalMonthlySold, @brandCount, @top1Brand, @top1SharePct, @top3SharePct,
      @medianPrice, @medianFeePct, @smallSizeRatePct, @asciiBrandRatePct, @medianReviewCount,
      @sourceComplete, @sourceFetchedAt, @examplesJson, @rank, @now, @now)
    ON CONFLICT(concept_id) DO UPDATE SET
      snapshot_id = excluded.snapshot_id, concept = excluded.concept,
      root_category_name = excluded.root_category_name, form = excluded.form,
      amc_capable = excluded.amc_capable, hard_gate = excluded.hard_gate,
      gate_fail_reason = excluded.gate_fail_reason, commodity = excluded.commodity,
      big_brand = excluded.big_brand, product_count = excluded.product_count,
      total_monthly_sold = excluded.total_monthly_sold, brand_count = excluded.brand_count,
      top1_brand = excluded.top1_brand, top1_share_pct = excluded.top1_share_pct,
      top3_share_pct = excluded.top3_share_pct, median_price = excluded.median_price,
      median_fee_pct = excluded.median_fee_pct, small_size_rate_pct = excluded.small_size_rate_pct,
      ascii_brand_rate_pct = excluded.ascii_brand_rate_pct,
      median_review_count = excluded.median_review_count,
      source_complete = excluded.source_complete, source_fetched_at = excluded.source_fetched_at,
      examples_json = excluded.examples_json, rank_in_snapshot = excluded.rank_in_snapshot,
      updated_at = excluded.updated_at
      -- first_seen_at は更新しない (そのテーマを初めて見た日を残す)
  `);

  const run = db.transaction(() => {
    insSnap.run(snapshotId, payload.generatedAt, now, payload.algorithmVersion ?? 1,
      payload.sourceProducts ?? null, payload.afterBaseFilter ?? null,
      (payload.concepts || []).length, payload.lastProgressAt ?? null, payload.remainingTotal ?? null);

    for (const c of payload.collection || []) {
      insCat.run({
        snapshotId,
        rootCategory: String(c.rootCategory),
        name: c.name || String(c.rootCategory),
        state: c.state || 'collected',
        asinTarget: c.asinTarget ?? null,
        fetched: c.fetched ?? null,
        complete: c.complete === null || c.complete === undefined ? null : (c.complete ? 1 : 0),
        estimatedMissing: c.estimatedMissing ?? null,
        fetchedAt: c.fetchedAt || null,
        remaining: c.remaining ?? null,
      });
    }

    for (const c of payload.concepts || []) {
      insConcept.run({
        conceptId: conceptIdOf(c.categoryPath, c.form),
        snapshotId,
        concept: c.concept,
        categoryPath: c.categoryPath,
        rootCategoryName: c.rootCategoryName ?? null,
        form: c.form ?? null,
        amcCapable: c.gates?.amc ?? null,
        hardGate: c.hardGate || 'unknown',
        gateFailReason: c.gateFailReason ?? null,
        commodity: c.gates?.commodity ?? null,
        bigBrand: c.gates?.bigBrand ?? null,
        productCount: c.productCount ?? null,
        totalMonthlySold: c.totalMonthlySold ?? null,
        brandCount: c.brandCount ?? null,
        top1Brand: c.top1Brand ?? null,
        top1SharePct: c.top1SharePct ?? null,
        top3SharePct: c.top3SharePct ?? null,
        medianPrice: c.medianPrice ?? null,
        medianFeePct: c.medianReferralFeePct ?? null,
        smallSizeRatePct: c.smallSizeRatePct ?? null,
        asciiBrandRatePct: c.asciiBrandRatePct ?? null,
        medianReviewCount: c.medianReviewCount ?? null,
        sourceComplete: c.sourceComplete === null || c.sourceComplete === undefined
          ? null : (c.sourceComplete ? 1 : 0),
        sourceFetchedAt: c.sourceFetchedAt ?? null,
        examplesJson: JSON.stringify(c.examples || []),
        rank: c.rank ?? null,
        now,
      });
    }
  });
  run();
  return { snapshotId, concepts: (payload.concepts || []).length, categories: (payload.collection || []).length };
}

// ─────────────────────────────────────────────────────────────
// 参照
// ─────────────────────────────────────────────────────────────

/** 最新スナップショット (無ければ null) */
export function getLatestSnapshot(handle) {
  const db = resolveDb(handle);
  return db.prepare('SELECT * FROM scout_snapshots ORDER BY ingested_at DESC LIMIT 1').get() || null;
}

/** 収集の工程表。⭐complete=0 のカテゴリは「下限」としてしか読めないので、そのまま返して画面で明示する */
export function listCategories(snapshotId, handle) {
  const db = resolveDb(handle);
  if (!snapshotId) return [];
  return db.prepare(`
    SELECT * FROM scout_categories WHERE snapshot_id = ? ORDER BY rowid
  `).all(snapshotId);
}

/**
 * テーマ一覧 + 各テーマの最新判断。
 * 現在状態は scout_decisions の最新イベントから導出する (concepts 側に持たない)。
 */
export function listConcepts({ gate = 'pass', status = 'undecided', limit = 50, offset = 0 } = {}, handle) {
  const db = resolveDb(handle);
  const where = [];
  const params = {};
  if (gate && gate !== 'all') { where.push('c.hard_gate = @gate'); params.gate = gate; }
  if (status === 'undecided') where.push('d.decision IS NULL');
  else if (status && status !== 'all') { where.push('d.decision = @status'); params.status = status; }

  const sql = `
    SELECT c.*, d.decision, d.reason_code, d.comment, d.recheck_condition,
           d.decided_by, d.decided_at
    FROM scout_concepts c
    LEFT JOIN (
      SELECT s.* FROM scout_decisions s
      JOIN (SELECT concept_id, MAX(decided_at) AS m FROM scout_decisions GROUP BY concept_id) l
        ON l.concept_id = s.concept_id AND l.m = s.decided_at
    ) d ON d.concept_id = c.concept_id
    ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
    ORDER BY c.rank_in_snapshot
    LIMIT @limit OFFSET @offset
  `;
  const rows = db.prepare(sql).all({ ...params, limit, offset });
  return rows.map((r) => ({ ...r, examples: safeJson(r.examples_json) }));
}

export function countConcepts(handle) {
  const db = resolveDb(handle);
  const rows = db.prepare(`
    SELECT c.hard_gate, (d.decision IS NULL) AS undecided, COUNT(*) AS n
    FROM scout_concepts c
    LEFT JOIN (
      SELECT s.* FROM scout_decisions s
      JOIN (SELECT concept_id, MAX(decided_at) AS m FROM scout_decisions GROUP BY concept_id) l
        ON l.concept_id = s.concept_id AND l.m = s.decided_at
    ) d ON d.concept_id = c.concept_id
    GROUP BY c.hard_gate, undecided
  `).all();
  const out = { pass: 0, unknown: 0, fail: 0, undecidedPass: 0, decided: 0 };
  for (const r of rows) {
    out[r.hard_gate] = (out[r.hard_gate] || 0) + r.n;
    if (r.undecided) { if (r.hard_gate === 'pass') out.undecidedPass += r.n; }
    else out.decided += r.n;
  }
  return out;
}

export function getConcept(conceptId, handle) {
  const db = resolveDb(handle);
  const c = db.prepare('SELECT * FROM scout_concepts WHERE concept_id = ?').get(conceptId);
  if (!c) return null;
  const history = db.prepare(
    'SELECT * FROM scout_decisions WHERE concept_id = ? ORDER BY decided_at DESC'
  ).all(conceptId);
  return { ...c, examples: safeJson(c.examples_json), history };
}

function safeJson(s) {
  try { return JSON.parse(s || '[]'); } catch { return []; }
}

// ─────────────────────────────────────────────────────────────
// 採否 (追記のみ)
// ─────────────────────────────────────────────────────────────

export const REASON_CODES = [
  { code: 'amc_cannot_make', label: 'AMCで作れない' },
  { code: 'commodity_price', label: '中華コモディティで価格勝負にならない' },
  { code: 'too_large', label: '大型・物流費超過' },
  { code: 'regulation', label: '規制リスク (薬機法・医薬部外品など)' },
  { code: 'ip_risk', label: '知財リスク' },
  { code: 'weak_intent', label: '検索意図が弱い / 検索語が書けない' },
  { code: 'seasonal', label: '一過性・季節需要' },
  { code: 'no_margin', label: '広告物流込みで利益が出ない' },
  { code: 'no_edge', label: '勝てる理由が作れない' },
  { code: 'duplicate', label: '既存商品と重複' },
  { code: 'other', label: 'その他 (コメント必須)' },
];
const REASON_SET = new Set(REASON_CODES.map((r) => r.code));

/**
 * 採否を1件記録する。⭐判断時点の指標をイベント側に固定して保存する。
 * 収集はこの先も更新され続けるので、「何を見て決めたか」を残さないと後から検証できない。
 */
export function recordDecision({ conceptId, decision, reasonCode, comment, recheckCondition, decidedBy }, handle) {
  const db = resolveDb(handle);
  const concept = db.prepare('SELECT * FROM scout_concepts WHERE concept_id = ?').get(conceptId);
  if (!concept) throw Object.assign(new Error('テーマが見つかりません'), { status: 404 });
  if (!['adopt', 'reject', 'hold'].includes(decision)) {
    throw Object.assign(new Error('decision が不正です'), { status: 400 });
  }
  if (decision === 'reject') {
    if (!reasonCode || !REASON_SET.has(reasonCode)) {
      throw Object.assign(new Error('不採用には理由コードが必要です'), { status: 400 });
    }
    if (reasonCode === 'other' && !String(comment || '').trim()) {
      throw Object.assign(new Error('理由が「その他」のときはコメントが必要です'), { status: 400 });
    }
  }

  const prior = db.prepare(
    'SELECT decision_id FROM scout_decisions WHERE concept_id = ? ORDER BY decided_at DESC LIMIT 1'
  ).get(conceptId);

  const metrics = {
    productCount: concept.product_count, totalMonthlySold: concept.total_monthly_sold,
    brandCount: concept.brand_count, top1Brand: concept.top1_brand,
    top1SharePct: concept.top1_share_pct, medianPrice: concept.median_price,
    smallSizeRatePct: concept.small_size_rate_pct, asciiBrandRatePct: concept.ascii_brand_rate_pct,
    medianReviewCount: concept.median_review_count,
    sourceComplete: concept.source_complete, sourceFetchedAt: concept.source_fetched_at,
    hardGate: concept.hard_gate, gateFailReason: concept.gate_fail_reason,
  };
  // 一意IDは ms + 乱数 (同一msの二重採番を避ける)
  const decisionId = `${Date.now().toString(36)}-${crypto.randomBytes(6).toString('hex')}`;
  db.prepare(`
    INSERT INTO scout_decisions (decision_id, concept_id, decision, failed_gate, reason_code,
      comment, recheck_condition, decided_by, decided_at, snapshot_id, metrics_json, prior_decision_id)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(decisionId, conceptId, decision, concept.gate_fail_reason || null,
    reasonCode || null, comment || null, recheckCondition || null,
    decidedBy, utcIsoNow(), concept.snapshot_id, JSON.stringify(metrics),
    prior ? prior.decision_id : null);

  return { decisionId };
}
