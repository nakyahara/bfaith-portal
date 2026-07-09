/**
 * dim-mall — モールマスタ dim_mall (warehouse-mirror.db) の共有アクセサ (設計監査 2026-07-06 PR-11)
 *
 * 背景: MALL_ORDER / MALL_LABEL / TAX_INCLUDED_MALLS / MALL_FEE_RATES 等のモール定数が
 * 5箇所以上のアプリに別々にハードコードされ、新モール追加のたびに全箇所修正が必要だった
 * (監査③「新モール追加シミュレーション: 粗利系は不合格」)。
 * dim_mall 表 (warehouse-mirror/db.js が boot 時に code-owned seed で全置換) を正本とし、
 * アプリはこのアクセサ経由で参照する。
 *
 * 注意:
 *   ・mall_key の正準語彙は小文字 (amazon/rakuten/yahoo/aupay/linegift/qoo10/mercari/
 *     dshop/amazon_usa + チャネル粒度 amazon_fba/amazon_fbm/wholesale/base)
 *   ・mgmt-accounting の MALL_NAMES は別キー空間 (amazon_jp/amazon_usa) + 別表記のため
 *     対象外 (billing ドメインの語彙。統一は将来課題)
 *   ・fee_rate_approx は profit-analysis の管理近似値 (請求実額ではない)
 */

let _cache = null;

/** @param {import('better-sqlite3').Database} db - warehouse-mirror.db ハンドル */
export function loadDimMall(db) {
  if (_cache) return _cache;
  const rows = db.prepare(`
    SELECT mall_key, label, display_order, is_channel, in_daily_summary, tax_included, fee_rate_approx
    FROM dim_mall ORDER BY display_order
  `).all();
  const byKey = new Map(rows.map(r => [r.mall_key, r]));
  _cache = {
    rows,
    byKey,
    /** 表示ラベル。未登録キーはキーをそのまま返す (従来の `LABELS[k] || k` と同挙動) */
    labelOf(key, fallback) {
      const r = byKey.get(key);
      return r ? r.label : (fallback !== undefined ? fallback : key);
    },
    /** biz-ops 日次サマリの対象モール (表示順) */
    dailySummaryOrder: rows.filter(r => r.in_daily_summary === 1).map(r => r.mall_key),
    /** mart の売上/手数料/広告費が税込で入るモール (mgmt-accounting の /1.1 対象) */
    taxIncludedSet: new Set(rows.filter(r => r.tax_included === 1).map(r => r.mall_key)),
    /** 管理近似の手数料率 (profit-analysis 用)。未登録はフォールバック */
    feeRateOf(key, fallback = 0.10) {
      const r = byKey.get(key);
      return (r && r.fee_rate_approx != null) ? r.fee_rate_approx : fallback;
    },
  };
  return _cache;
}
