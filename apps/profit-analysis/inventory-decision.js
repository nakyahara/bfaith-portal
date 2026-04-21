/**
 * 商品収益性ダッシュボード タブB: 在庫整理・撤退判断支援 API
 *
 * マウント先: /apps/profit-analysis/api/inventory
 * feature flag: INVENTORY_DECISION_ENABLED（OFFなら全エンドポイント 503）
 *
 * 現状の実装範囲（PR2b）:
 *   GET    /thresholds            - 閾値マトリクス・早期警戒・処分率の取得
 *   PUT    /thresholds            - 上記の更新
 *   GET    /status/:code          - 商品コード単位の現ステータス取得
 *   POST   /status                - 撤退判断ステータス CRUD
 *   GET    /candidates            - 5分類ビュー（PR2c で実装、現状 501）
 */
import { Router } from 'express';
import { getMirrorDB } from '../warehouse-mirror/db.js';
import {
  seedDefaultsIfMissing,
  getRetirementThresholds,
  getClassificationThresholds,
  getEarlyWarning,
  getDisposalRateDefault,
  setSetting,
  validateRetirementThresholds,
  validateEarlyWarning,
  KEYS,
} from './retirement-thresholds.js';

const router = Router();

// ─── Feature flag middleware ───

// Dark Launch: INVENTORY_DECISION_ENABLED=true でのみ有効化。
// 未設定・'false' は全エンドポイント 503 を返す。
function featureEnabled() {
  const flag = process.env.INVENTORY_DECISION_ENABLED;
  return flag === 'true' || flag === '1';
}

router.use((req, res, next) => {
  if (!featureEnabled()) {
    return res.status(503).json({
      error: 'inventory-decision feature is disabled',
      hint: 'set INVENTORY_DECISION_ENABLED=true to enable',
    });
  }
  next();
});

// ─── 閾値 GET/PUT ───

/**
 * 現在の閾値・早期警戒・処分率を一括取得
 * 初回アクセス時はデフォルト値を seed する
 */
router.get('/thresholds', (req, res) => {
  try {
    const db = getMirrorDB();
    seedDefaultsIfMissing(db);
    res.json({
      retirement: getRetirementThresholds(db),
      classification: getClassificationThresholds(db),
      early_warning: getEarlyWarning(db),
      disposal_rate_default: getDisposalRateDefault(db),
    });
  } catch (e) {
    console.error('[inventory-decision] GET /thresholds error:', e);
    res.status(500).json({ error: e.message });
  }
});

/**
 * 閾値・早期警戒・処分率の更新（部分更新可）
 * body: { retirement?, classification?, early_warning?, disposal_rate_default?, updated_by? }
 */
router.put('/thresholds', (req, res) => {
  try {
    const db = getMirrorDB();
    const { retirement, classification, early_warning, disposal_rate_default, updated_by } = req.body || {};
    const who = typeof updated_by === 'string' && updated_by ? updated_by : (req.session?.email || 'admin');

    const tx = db.transaction(() => {
      if (retirement !== undefined) {
        validateRetirementThresholds(retirement);
        setSetting(db, KEYS.RETIREMENT, retirement, who);
      }
      if (classification !== undefined) {
        // classification は現時点で構造が柔軟なので最低限のオブジェクトチェックのみ
        if (!classification || typeof classification !== 'object') {
          throw new Error('classification はオブジェクトである必要があります');
        }
        setSetting(db, KEYS.CLASSIFICATION, classification, who);
      }
      if (early_warning !== undefined) {
        validateEarlyWarning(early_warning);
        setSetting(db, KEYS.EARLY_WARNING, early_warning, who);
      }
      if (disposal_rate_default !== undefined) {
        const v = Number(disposal_rate_default);
        if (!Number.isFinite(v) || v <= 0 || v > 1) {
          throw new Error('disposal_rate_default は 0 〜 1 の数値');
        }
        setSetting(db, KEYS.DISPOSAL_RATE, { value: v }, who);
      }
    });
    tx();
    res.json({ ok: true });
  } catch (e) {
    console.error('[inventory-decision] PUT /thresholds error:', e.message);
    res.status(400).json({ error: e.message });
  }
});

// ─── ステータス CRUD (product_retirement_status) ───

export const VALID_STATUSES = new Set([
  '継続', '値下げ検討', '撤退検討', '撤退確定',
  // 自社追加ステータス（設計書セクション14 / Codex 12回目 #9）
  '消化計画中', 'リブランディング検討', '再生産判断中',
]);

// 追加ステータスは next_review_date と reason 必須（設計書§14 §4-8）
// Codex PR2b review Medium #1 反映: 設計書では reason も必須
export const REVIEW_REQUIRED_STATUSES = new Set([
  '消化計画中', 'リブランディング検討', '再生産判断中',
]);

const RETIREMENT_STATUSES = new Set(['撤退検討', '撤退確定']);

/**
 * POST /status のリクエスト body を検証する（純関数、Test 12 が直接呼ぶ）
 * @param body リクエストボディ
 * @throws Error バリデーション失敗時
 */
export function validateStatusBody(body) {
  const b = body || {};
  if (!b.ne_product_code || typeof b.ne_product_code !== 'string') {
    throw new Error('ne_product_code は必須');
  }
  if (!VALID_STATUSES.has(b.status)) {
    throw new Error(`status が不正: ${b.status} (有効値: ${[...VALID_STATUSES].join(', ')})`);
  }
  if (REVIEW_REQUIRED_STATUSES.has(b.status)) {
    if (!b.next_review_date) {
      throw new Error(`status=${b.status} は next_review_date 必須`);
    }
    // Codex PR2b review Medium #1 反映: 追加3ステータスは reason も必須
    if (!b.reason) {
      throw new Error(`status=${b.status} は reason 必須`);
    }
  }
  if (RETIREMENT_STATUSES.has(b.status) && !b.reason) {
    throw new Error(`status=${b.status} は reason 必須`);
  }
  // Codex PR2b review Medium #2 反映: disposal_rate の範囲チェック（PUT /thresholds と揃える）
  if (b.disposal_rate !== undefined && b.disposal_rate !== null) {
    const v = Number(b.disposal_rate);
    if (!Number.isFinite(v) || v <= 0 || v > 1) {
      throw new Error('disposal_rate は 0 より大きく 1 以下の数値');
    }
  }
}

/**
 * 商品単位の現ステータス取得
 * GET /status/:code
 */
router.get('/status/:code', (req, res) => {
  try {
    const db = getMirrorDB();
    const code = req.params.code;
    const row = db.prepare(`SELECT * FROM product_retirement_status WHERE ne_product_code = ?`).get(code);
    if (!row) return res.status(404).json({ error: 'not found', ne_product_code: code });
    // JSON カラムはそのまま文字列で返す（クライアント側でパース）
    res.json(row);
  } catch (e) {
    console.error('[inventory-decision] GET /status error:', e);
    res.status(500).json({ error: e.message });
  }
});

/**
 * 撤退判断ステータス CRUD（UPSERT）
 * POST /status
 * body: {
 *   ne_product_code, status, decided_by?, reason?, next_review_date?,
 *   plan_details, decision_metrics, thresholds_json,
 *   disposal_rate
 * }
 * 判断時メトリクス・閾値・処分率をスナップショットとして保存
 */
router.post('/status', (req, res) => {
  try {
    const db = getMirrorDB();
    const body = req.body || {};
    validateStatusBody(body);
    const {
      ne_product_code, status, decided_by, reason, next_review_date,
      plan_details, decision_metrics, thresholds, disposal_rate,
    } = body;

    const who = typeof decided_by === 'string' && decided_by ? decided_by : (req.session?.email || 'admin');
    const ts = new Date().toISOString().replace('T', ' ').slice(0, 19);

    const stmt = db.prepare(`INSERT INTO product_retirement_status
      (ne_product_code, status, decided_by, decided_at, reason, next_review_date,
       plan_details_json, decision_metrics_json, thresholds_json, disposal_rate, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(ne_product_code) DO UPDATE SET
        status = excluded.status,
        decided_by = excluded.decided_by,
        decided_at = excluded.decided_at,
        reason = excluded.reason,
        next_review_date = excluded.next_review_date,
        plan_details_json = excluded.plan_details_json,
        decision_metrics_json = excluded.decision_metrics_json,
        thresholds_json = excluded.thresholds_json,
        disposal_rate = excluded.disposal_rate,
        updated_at = excluded.updated_at
    `);
    stmt.run(
      ne_product_code, status, who, ts,
      reason || null,
      next_review_date || null,
      plan_details ? JSON.stringify(plan_details) : null,
      decision_metrics ? JSON.stringify(decision_metrics) : null,
      thresholds ? JSON.stringify(thresholds) : null,
      (disposal_rate !== undefined && disposal_rate !== null) ? Number(disposal_rate) : null,
      ts,
    );

    res.json({ ok: true, ne_product_code, status });
  } catch (e) {
    console.error('[inventory-decision] POST /status error:', e.message);
    res.status(400).json({ error: e.message });
  }
});

// ─── 5分類ビュー（PR2c 実装予定、現状 501） ───

router.get('/candidates', (req, res) => {
  res.status(501).json({
    error: 'not implemented',
    note: 'PR2c で実装予定（売上分類別 5分類 + 早期警戒 + 分類外理由分解）',
  });
});

export default router;
