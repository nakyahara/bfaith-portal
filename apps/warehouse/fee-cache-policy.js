/**
 * fee-cache-policy.js — Amazon手数料キャッシュ (amazon_sku_fees) の鮮度ポリシー集約。
 *
 * 2 つの閾値を 1 箇所で管理し、別ファイル (fetch-amazon-fees.js / monitor-fee-coverage.js) に
 * ハードコードして将来ドリフトする事故を防ぐ (2026-05-22 の 168h 境界レース事故の根治、Codex+Claude)。
 *
 *   STALE_TTL_HOURS      監視 (monitor-fee-coverage.js) が stale=alert と判定する閾値 (= SLO)
 *   REFRESH_TTL_HOURS    更新ジョブ (fetch-amazon-fees.js) が再取得する基準閾値
 *   REFRESH_JITTER_HOURS 再取得閾値の per-SKU ジッター幅 (±)。一括投入バッチの同日一斉失効
 *                        (thundering herd) を destagger し、日次更新負荷と一過性 batch 失敗の影響を平準化する
 *
 * 不変条件: REFRESH_TTL_HOURS + REFRESH_JITTER_HOURS < STALE_TTL_HOURS
 *   = どの SKU も「監視が stale 判定する前に必ず先に更新される」マージンを保証。
 *   実効再取得閾値は 96〜144h、監視 168h との最小マージンは 24h (日次 1 回スキップ耐性)。
 */
export const STALE_TTL_HOURS = 7 * 24;       // 168h。監視の stale/alert 閾値 (SLO)
export const REFRESH_TTL_HOURS = 5 * 24;     // 120h。更新ジョブの基準閾値 (監視 168h より 48h 手前)
export const REFRESH_JITTER_HOURS = 24;      // ±24h → 実効 96〜144h

// 起動時に不変条件を自己検証 (将来 STALE を下げる / REFRESH を上げる改変で気付けるように)
if (REFRESH_TTL_HOURS + REFRESH_JITTER_HOURS >= STALE_TTL_HOURS) {
  throw new Error(
    `[fee-cache-policy] invariant violated: REFRESH_TTL_HOURS(${REFRESH_TTL_HOURS}) + ` +
    `REFRESH_JITTER_HOURS(${REFRESH_JITTER_HOURS}) must be < STALE_TTL_HOURS(${STALE_TTL_HOURS})`
  );
}

/**
 * seller_sku から決定的なジッター付き再取得閾値 (h) を返す。範囲 [REFRESH_TTL_HOURS ± REFRESH_JITTER_HOURS]。
 * 同じ SKU は常に同じ値を返す (毎回ランダムだと収束せず destagger が効かないため決定的ハッシュ)。
 * 同日一括投入されたバッチも SKU ごとに失効日がばらけ、thundering herd を防ぐ。
 */
export function refreshThresholdHours(sellerSku) {
  const key = String(sellerSku ?? '');
  let h = 0;
  for (let i = 0; i < key.length; i++) h = (Math.imul(h, 31) + key.charCodeAt(i)) | 0;
  const span = 2 * REFRESH_JITTER_HOURS + 1;
  const jitter = (((h % span) + span) % span) - REFRESH_JITTER_HOURS;  // [-J, +J] に正規化
  return REFRESH_TTL_HOURS + jitter;
}
