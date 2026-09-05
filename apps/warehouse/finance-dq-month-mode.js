/**
 * finance DQ 共通: 対象月の判定 (JST) と閾値の選び方
 *
 *   current     = 当月
 *   recent_past = 前月 かつ 月初 graceDays 日以内 (既定 14 日)
 *   past        = それ以前
 *
 * なぜ recent_past が要るか (2026-09-05):
 *   配送完了ステータス (Qoo10 Delivered(5) / LINEギフト received / Yahoo・auPAY の出荷済) への遷移には
 *   1〜2 週間かかる。月が締まった直後は「月末に受注した分がまだ配送中」なので whitelist_coverage_pct が
 *   構造的に低く出る。2026-08 の Qoo10 は 8/31 64% → 9/1 77% → 9/3 85% → 9/5 89% と毎日上がるだけの
 *   遷移中に、月が変わった瞬間から PAST 閾値 (error 90%) を当てられ、5 日間 mirror 同期が止まった
 *   (実害: Render の 8 月分 Qoo10 粗利が届かない)。原因はデータ不良ではなく閾値の当て方。
 *
 *   recent_past では whitelist_coverage_pct だけ CURRENT の (緩い) 閾値を使い、他のチェック
 *   (row_count_drift / missing_cost / unresolved_sku / zero_cost 等) は PAST のまま厳しく見る。
 *   14 日を過ぎても低ければ本物の停滞として PAST 閾値で error になる。
 */
export const RECENT_PAST_GRACE_DAYS = 14;

/** JST の「今」を UTC 表現にずらした Date (既存 isCurrentMonth と同じ流儀: getUTC* / toISOString で JST を読む) */
export function jstShifted(now = new Date()) {
  return new Date(now.getTime() + 9 * 3600 * 1000);
}

/**
 * @param {string} ym 'YYYY-MM'
 * @param {{ now?: Date, graceDays?: number }} [opt]
 * @returns {'current'|'recent_past'|'past'}
 */
export function monthMode(ym, { now = new Date(), graceDays = RECENT_PAST_GRACE_DAYS } = {}) {
  const j = jstShifted(now);
  const cur = j.toISOString().slice(0, 7);
  if (ym === cur) return 'current';
  const prev = new Date(Date.UTC(j.getUTCFullYear(), j.getUTCMonth() - 1, 1)).toISOString().slice(0, 7);
  if (ym === prev && j.getUTCDate() <= graceDays) return 'recent_past';
  return 'past';
}

/**
 * モードに応じた閾値表。recent_past は PAST を土台に whitelist_coverage_pct だけ CURRENT を採用。
 * whitelist_coverage_pct を持たない表 (楽天・Amazon) では PAST と同じになる。
 */
export function pickThresholds(mode, pastThresholds, currentThresholds) {
  if (mode === 'current') return currentThresholds;
  if (mode === 'recent_past' && currentThresholds.whitelist_coverage_pct) {
    return { ...pastThresholds, whitelist_coverage_pct: currentThresholds.whitelist_coverage_pct };
  }
  return pastThresholds;
}

/** ログ用ラベル。既存ログの `${isCur ? 'CURRENT' : 'PAST'} mode` の置き換え */
export function modeLabel(mode, graceDays = RECENT_PAST_GRACE_DAYS) {
  if (mode === 'current') return 'CURRENT';
  if (mode === 'recent_past') return `PAST+grace (前月・月初${graceDays}日以内: whitelist_coverage は当月閾値)`;
  return 'PAST';
}
