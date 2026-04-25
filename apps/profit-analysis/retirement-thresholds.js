/**
 * 商品収益性ダッシュボード タブB 閾値管理モジュール
 *
 * `dashboard_settings` (Render側、warehouse-mirror.db) から閾値JSONを読み書きする。
 * 初回アクセス時にデフォルト値を seed する。
 *
 * 売上分類は m_products.売上分類 の整数値に対応:
 *   1: 自社商品
 *   2: 取引先限定商品 (= 設計書の「取扱限定」)
 *   3: 仕入れ商品
 *   4: 米国Amazon輸出 (本ダッシュボード対象外)
 */

// ─── 閾値のデフォルト値（設計書セクション14 + Codex 12回目 + ユーザー合意済み） ───

/** 撤退・警戒閾値マトリクス（売上分類別） */
export const DEFAULT_RETIREMENT_THRESHOLDS = {
  '1': {
    // 自社商品: 大ロット前提で甘め。180日警戒 → 365日撤退検討（二段階）
    warn_days_no_sales: 180,
    retire_days_no_sales: 365,
  },
  '2': {
    // 取引先限定: 中間設定
    warn_days_no_sales: 120,
    retire_days_no_sales: 180,
  },
  '3': {
    // 仕入れ商品: ライバル参入リスクが高く、きつめ
    warn_days_no_sales: 90,
    warn_gmroi_lt: 50,              // GMROI 50% 未満で警戒
    retire_days_no_sales: 180,
    retire_gmroi_lt: 30,            // GMROI 30% 未満 かつ
    retire_turnover_gt: 180,        // 回転 180日 超 で撤退検討
  },
};

/** 5分類（優良/観察/値下げ/セット）の判定閾値（全売上分類共通） */
export const DEFAULT_CLASSIFICATION_THRESHOLDS = {
  good_stock:       { gmroi_gt: 200, turnover_min: 30, turnover_max: 90 },
  observe:          { gmroi_min: 100, gmroi_max: 200 },
  price_down:       { turnover_gt: 120, margin_rate_gt: 20 },
  bundle_candidate: { turnover_gt: 120 },
};

/** 早期警戒フラグ（全商品共通、ユーザー判断で「仕入中心」→「全商品共通」に変更済） */
export const DEFAULT_EARLY_WARNING = {
  past_period_days:  90,    // 過去基準期間
  recent_period_days: 30,   // 直近期間
  min_past_sales:    10,    // 過去期間販売数の最低（これ未満は「判定不足」）
  drop_ratio:        0.33,  // 直近が過去平均の 1/3 以下で急落判定
};

/** 評価損見込み計算の想定処分率（デフォルト 50%） */
export const DEFAULT_DISPOSAL_RATE = 0.5;

// ─── dashboard_settings キー定数 ───
export const KEYS = {
  RETIREMENT: 'retirement_thresholds',
  CLASSIFICATION: 'classification_thresholds',
  EARLY_WARNING: 'early_warning',
  DISPOSAL_RATE: 'disposal_rate_default',
};

// ─── 共通 CRUD ───

function now() {
  return new Date().toISOString().replace('T', ' ').slice(0, 19);
}

/**
 * 任意キーのJSON値を読む。
 *   - キーが存在しない → fallback を返す
 *   - キーが存在して JSON parse に失敗 → throw（Codex PR2b review Low-Medium #3 反映）
 *   - DB エラーはそのまま throw（本番障害を隠さない）
 */
export function getSetting(db, key, fallback) {
  const row = db.prepare('SELECT value_json FROM dashboard_settings WHERE key = ?').get(key);
  if (!row?.value_json) return fallback;
  try {
    return JSON.parse(row.value_json);
  } catch (e) {
    console.error(`[retirement-thresholds] dashboard_settings.key="${key}" の JSON parse に失敗:`, e.message);
    throw new Error(`dashboard_settings key "${key}" の JSON が壊れています`);
  }
}

/** 任意キーのJSON値を書く */
export function setSetting(db, key, value, updatedBy = 'admin') {
  db.prepare(`INSERT OR REPLACE INTO dashboard_settings (key, value_json, updated_at, updated_by)
              VALUES (?, ?, ?, ?)`).run(key, JSON.stringify(value), now(), updatedBy);
}

// ─── 初回起動時のデフォルト投入（idempotent） ───

/**
 * dashboard_settings に未投入のキーだけデフォルト値を入れる。
 * 複数回呼んでも既存値を上書きしない。
 */
export function seedDefaultsIfMissing(db) {
  const existing = new Set(
    db.prepare('SELECT key FROM dashboard_settings').all().map(r => r.key)
  );
  const seeds = [
    [KEYS.RETIREMENT,     DEFAULT_RETIREMENT_THRESHOLDS],
    [KEYS.CLASSIFICATION, DEFAULT_CLASSIFICATION_THRESHOLDS],
    [KEYS.EARLY_WARNING,  DEFAULT_EARLY_WARNING],
    [KEYS.DISPOSAL_RATE,  { value: DEFAULT_DISPOSAL_RATE }],
  ];
  const stmt = db.prepare(`INSERT INTO dashboard_settings (key, value_json, updated_at, updated_by)
                           VALUES (?, ?, ?, ?)`);
  const tx = db.transaction(() => {
    const ts = now();
    for (const [k, v] of seeds) {
      if (!existing.has(k)) stmt.run(k, JSON.stringify(v), ts, 'system-init');
    }
  });
  tx();
}

// ─── ドメイン別アクセサ ───

export function getRetirementThresholds(db) {
  return getSetting(db, KEYS.RETIREMENT, DEFAULT_RETIREMENT_THRESHOLDS);
}

export function getClassificationThresholds(db) {
  return getSetting(db, KEYS.CLASSIFICATION, DEFAULT_CLASSIFICATION_THRESHOLDS);
}

export function getEarlyWarning(db) {
  return getSetting(db, KEYS.EARLY_WARNING, DEFAULT_EARLY_WARNING);
}

export function getDisposalRateDefault(db) {
  return getSetting(db, KEYS.DISPOSAL_RATE, { value: DEFAULT_DISPOSAL_RATE }).value;
}

// ─── バリデーション（PUT 時に使う） ───

const VALID_SALES_CLASSES = ['1', '2', '3'];

/** 数値が「有限かつ正」であることを検証するヘルパー（Codex PR2b review Low #4 反映） */
function isFinitePositive(n) {
  return typeof n === 'number' && Number.isFinite(n) && n > 0;
}

/** retirement thresholds の最低限のバリデーション */
export function validateRetirementThresholds(obj) {
  if (!obj || typeof obj !== 'object') throw new Error('retirement_thresholds はオブジェクトである必要があります');
  for (const cls of VALID_SALES_CLASSES) {
    const v = obj[cls];
    if (!v || typeof v !== 'object') throw new Error(`sales_class ${cls} の閾値が欠落しています`);
    if (!isFinitePositive(v.warn_days_no_sales)) {
      throw new Error(`sales_class ${cls}: warn_days_no_sales は有限の正数`);
    }
    if (!isFinitePositive(v.retire_days_no_sales)) {
      throw new Error(`sales_class ${cls}: retire_days_no_sales は有限の正数`);
    }
    if (v.warn_days_no_sales > v.retire_days_no_sales) {
      throw new Error(`sales_class ${cls}: warn_days_no_sales は retire_days_no_sales 以下`);
    }
  }
}

/** early_warning の最低限のバリデーション */
export function validateEarlyWarning(obj) {
  if (!obj || typeof obj !== 'object') throw new Error('early_warning はオブジェクトである必要があります');
  const requiredPositive = ['past_period_days', 'recent_period_days', 'min_past_sales'];
  for (const k of requiredPositive) {
    if (!isFinitePositive(obj[k])) throw new Error(`early_warning.${k} は有限の正数`);
  }
  if (typeof obj.drop_ratio !== 'number' || !Number.isFinite(obj.drop_ratio)) {
    throw new Error('early_warning.drop_ratio は有限の数値');
  }
  if (obj.drop_ratio <= 0 || obj.drop_ratio >= 1) {
    throw new Error('early_warning.drop_ratio は 0〜1 の範囲（exclusive）');
  }
  if (obj.recent_period_days >= obj.past_period_days) {
    throw new Error('early_warning.recent_period_days は past_period_days 未満');
  }
  // Codex PR2c Round 1 Medium #2 反映:
  //   現状の applyEarlyWarning は sales_90d / sales_30d 固定で集計しているため、
  //   past_period_days=90 / recent_period_days=30 以外はサポートしない。
  //   SQL 側に可変集計を追加する改修は Phase 2 送り。
  //   それまでは設定変更を拒否して期間ミスマッチを防ぐ。
  if (obj.past_period_days !== 90) {
    throw new Error('early_warning.past_period_days は現状 90 固定（SQL側の集計列が sales_90d に依存）');
  }
  if (obj.recent_period_days !== 30) {
    throw new Error('early_warning.recent_period_days は現状 30 固定（SQL側の集計列が sales_30d に依存）');
  }
}

/** 売上分類の有効値チェック */
export function isValidSalesClass(cls) {
  return VALID_SALES_CLASSES.includes(String(cls));
}
