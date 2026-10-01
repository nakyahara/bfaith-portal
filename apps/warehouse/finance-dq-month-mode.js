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

/**
 * 月初の猶予: 当月がまだ 0 行でも CRITICAL (exit 1) にしない日数 (JST の「日」がこれ以下なら ⚠️ 警告・exit 0)。
 *
 * なぜ要るか (2026-10-01):
 *   毎月 1 日の朝の daily-sync で Yahoo・au PAY・LINE ギフト・Qoo10 の finance DQ が
 *   「f_xxx_finance_sku_daily_v1 に 2026-10 のデータが 0 行」で CRITICAL になった。
 *   4 つの fact は「出荷した (配送が終わった) 注文」しか数えないので、月の最初の出荷が取り込まれるまで当月は 0 行が当然。
 *   受注の取込は 07:00 の daily-sync の 1 日 1 回 = 1 日に出荷した分が入るのは早くても 2 日の朝。
 *
 * 日数の根拠 (モールごと。「その月の最初の行が入る朝」の遅いほう):
 *   - yahoo    (受注日・order_status=5 + ship_status=3 = 出荷完了)
 *   - aupay    (受注日・orderStatus='完了' = 発送待ちの次 = 発送後)
 *   - linegift (received_date_jst = 受け取り = 店が発送した後の終端の状態。日付そのものが出荷日)
 *       → どれも「月の最初の出荷日 S の翌朝」に最初の行が入る。S は 平日の 1 日なら 1 日 (2 日の朝)、
 *         1 日が土曜なら 3 日 (4 日の朝)、GW で 5/1 が土曜の年は 5/6 (5/7 の朝)、
 *         年末年始 (12/29〜1/3 休み) は 1/4〜1/6 (1/5〜1/7 の朝)。いちばん遅い朝が 7 日 → 6 日目までは 0 行でよい = 6
 *   - qoo10    (受注日・shipping_status='Delivered(5)' = 配送完了)
 *       → 出荷のあと配達と Qoo10 側の Delivered への切り替えを待つ (On delivery(4) のまま 5 日以上の注文もある。2026-09-05 の実測)。
 *         ほかの 3 つより 2〜3 日遅い → 9
 *   過去の月の 0 行・猶予を過ぎた当月の 0 行は今までどおり CRITICAL (本物の止まりを隠さない)。
 */
export const MONTH_START_EMPTY_GRACE_DAYS = Object.freeze({ yahoo: 6, aupay: 6, linegift: 6, qoo10: 9 });

/** 猶予の日数として受ける上限 (前月の whitelist の猶予 RECENT_PAST_GRACE_DAYS と同じ。これより長い猶予は「月初」ではない) */
const MONTH_START_EMPTY_GRACE_MAX = RECENT_PAST_GRACE_DAYS;

/**
 * 対象月が 0 行のとき、それを「月初の猶予」(⚠️ 警告・exit 0) として扱ってよいか。
 *   grace = true は「当月 (JST)」かつ「JST の日 ≤ graceDays」のときだけ。
 *   前月 (recent_past を含む)・それより前・未来の月・猶予を過ぎた当月は false (= 呼び手は今までどおり CRITICAL)。
 * @param {string} ym 'YYYY-MM'
 * @param {{ now?: Date, graceDays: number }} opt
 * @returns {{ grace: boolean, mode: 'current'|'recent_past'|'past', dayOfMonth: number, graceDays: number }}
 */
export function monthStartEmptyGrace(ym, { now = new Date(), graceDays } = {}) {
  // 日数が壊れていたら猶予を出さずに投げる (呼び手のスクリプトは落ちて exit 1 = CRITICAL と同じ向き)
  if (!Number.isInteger(graceDays) || graceDays < 0 || graceDays > MONTH_START_EMPTY_GRACE_MAX) {
    throw new Error(`monthStartEmptyGrace: graceDays は 0〜${MONTH_START_EMPTY_GRACE_MAX} の整数 (受け取った値: ${graceDays})`);
  }
  if (!(now instanceof Date) || Number.isNaN(now.getTime())) throw new Error('monthStartEmptyGrace: now が日時でない');
  const mode = monthMode(ym, { now });
  const dayOfMonth = jstShifted(now).getUTCDate();
  return { grace: mode === 'current' && dayOfMonth <= graceDays, mode, dayOfMonth, graceDays };
}

/** 猶予で通したときの最後の行 (daily-sync は子の最後の行を要約に使う = ⚠️ で始める) */
export function monthStartEmptyNote(table, ym, g) {
  return `⚠️ 月初の猶予: ${table} に ${ym} はまだ 0 行 (JST ${g.dayOfMonth} 日・猶予は ${g.graceDays} 日まで = その月の最初の行の取込待ち)。${g.graceDays + 1} 日の朝も 0 行なら CRITICAL`;
}

/**
 * 試験用の --now (月の判定と月初の猶予だけに効く。SQL の date('now') は動かさない)。
 * 時差の書いていない日時はこの PC の時刻として読まれて JST の判定がずれるので、Z か ±hh:mm を必須にする
 */
export function parseNowArg(s) {
  if (typeof s !== 'string' || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:\d{2})$/.test(s)) {
    throw new Error(`--now は時差つきの ISO8601 (例 2026-10-01T07:00:00+09:00): "${s}"`);
  }
  const d = new Date(s);
  if (Number.isNaN(d.getTime())) throw new Error(`--now が日時として読めない: "${s}"`);
  return d;
}
