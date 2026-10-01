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
 * ─── 月初の猶予 (2026-10-01・PR #1572) ───
 * 当月がまだ 0 行でも、条件を全部満たすときだけ CRITICAL (exit 1) にせず ⚠️ 警告・exit 0 にする。
 *
 * なぜ要るか:
 *   毎月 1 日の朝の daily-sync で 5 モール (楽天・Yahoo・au PAY・LINE ギフト・Qoo10) の finance DQ が
 *   「当月のデータが 0 行」で CRITICAL になっていた (miniPC の dq_run_results の実測: 6/1〜10/1 の毎月 1 日)。
 *   fact は出荷した (Qoo10 は配送が終わった) 注文しか数えないので、その月の最初の出荷が取り込まれるまで 0 行が当然。
 *   受注の取込は 07:00 の daily-sync の 1 日 1 回 = 1 日に出荷した分が入るのは早くても 2 日の朝。
 *
 * 猶予の条件 (decideMonthStartEmpty。1 つでも欠けたら今までどおり CRITICAL):
 *   ① 当月 (JST) で、JST の日 ≤ monthStartGraceDays(mall, ym)
 *   ② この mall・月が前に一度も 0 行でなくなっていない (dq_run_results の記録で見る = 一度入った行が消えたのは本物の異常)
 *   ③ 前月の fact の最新の日付が、前月の末日から prevMonthFreshDays 日以内 (前月の終わりから取込が止まっていない)
 *   ④ 呼び手が猶予を禁じていない (daily-sync はこの回のモールの取込が ❌ なら --no-month-start-grace を付ける)
 *
 * 日数の根拠 (miniPC の dq_run_results の実測。6〜10 月の毎月の初め・daily-sync の DQ):
 *   「当月の行が最初に 0 でなくなった朝」
 *     楽天・Yahoo・LINE ギフト = 6〜9 月すべて 2 日 / au PAY = 6・8・9 月は 2 日、7 月は 4 日 (件数が少ない) /
 *     Qoo10 = 3 日 (1・2 日は 0 行 = error。3 日からは当月の行があり、row_count_drift は「直近 8 日」の比べ方の warn に変わる = この猶予とは別・変えない)
 *   8/1 (土) 始まりの月も 2 日の朝に行が入った = 土曜も出荷している。日曜始まりの月 (2026-11) はまだ実測が無いので 1 日足す。
 *     楽天・Yahoo・LINE ギフト: 実測 1 日 (2 日の朝に入る) + 日曜始まり 1 + 余裕 1 = 3
 *     au PAY: 実測 3 日 (4 日の朝) + 日曜始まり 1 = 4
 *     Qoo10: 実測 2 日 (3 日の朝) + 日曜始まり 1 = 3
 *   1 月は年末年始 (12/29〜1/3 休み。config/watch-checks.mjs の NON_BUSINESS_DAYS) で最初の出荷が 3 日遅れる → + MONTH_START_JANUARY_EXTRA_DAYS
 *   前月の新しさ (prevMonthFreshDays): 前月の最後の注文 (Qoo10 は配送完了) が入るのは月末の数日前まで。
 *     年末 (12/29〜31 休み) で 3 日、件数の少ない au PAY と配送完了を待つ Qoo10 はさらに 2 日ほど → 5 / 7
 *   ★ 数字を差し替えるときはこの表だけを直す (試験 scripts/test-finance-dq-month-mode.mjs は表の値から境界を作る)
 */
export const MONTH_START_GRACE = Object.freeze({
  rakuten:  Object.freeze({ table: 'f_rakuten_finance_sku_daily_v1',  graceDays: 3, prevMonthFreshDays: 5 }),
  yahoo:    Object.freeze({ table: 'f_yahoo_finance_sku_daily_v1',    graceDays: 3, prevMonthFreshDays: 5 }),
  linegift: Object.freeze({ table: 'f_linegift_finance_sku_daily_v1', graceDays: 3, prevMonthFreshDays: 5 }),
  aupay:    Object.freeze({ table: 'f_aupay_finance_sku_daily_v1',    graceDays: 4, prevMonthFreshDays: 7 }),
  qoo10:    Object.freeze({ table: 'f_qoo10_finance_sku_daily_v1',    graceDays: 3, prevMonthFreshDays: 7 }),
});
/** 1 月だけ足す日数 (年末年始 12/29〜1/3 の休みで最初の出荷が遅れる分) */
export const MONTH_START_JANUARY_EXTRA_DAYS = 3;

/** 猶予の日数として受ける上限 (前月の whitelist の猶予 RECENT_PAST_GRACE_DAYS と同じ。これより長い猶予は「月初」ではない) */
const MONTH_START_EMPTY_GRACE_MAX = RECENT_PAST_GRACE_DAYS;

function graceSpec(mall) {
  const s = Object.hasOwn(MONTH_START_GRACE, mall) ? MONTH_START_GRACE[mall] : null;
  if (!s) throw new Error(`月初の猶予: 知らないモール "${mall}"`);
  return s;
}

/** その mall・月の猶予の日数 (表の値 + 1 月の上乗せ) */
export function monthStartGraceDays(mall, ym) {
  const s = graceSpec(mall);
  return s.graceDays + (/^\d{4}-01$/.test(ym) ? MONTH_START_JANUARY_EXTRA_DAYS : 0);
}

/**
 * 暦の上の判定だけ (条件 ①)。grace = true は「当月 (JST)」かつ「JST の日 ≤ graceDays」のときだけ。
 *   前月 (recent_past を含む)・それより前・未来の月・猶予を過ぎた当月は false。
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

/** 'YYYY-MM' の前月 */
export function prevMonthOf(ym) {
  const [y, m] = ym.split('-').map(Number);
  return new Date(Date.UTC(y, m - 2, 1)).toISOString().slice(0, 7);
}

/** 毎回の DQ が残す「この月の行数」の記録 (条件 ② の材料)。check_name は MONTH_ROW_COUNT_CHECK */
export const MONTH_ROW_COUNT_CHECK = 'month_row_count';
export function monthRowCountDetails(mall, ym, count) {
  return { mall, month: ym, month_row_count: count, note: '月初の猶予の印 (一度でも 0 でなかった月は猶予を使わない)' };
}

/**
 * 条件 ②: この mall・月が前に一度でも 0 行でなくなったか (dq_run_results の記録で見る)。
 *   - 新しい記録: check_name = 'month_row_count' で actual_value > 0 (details の mall / month で引く)
 *   - それより前の記録 (この PR より前の run): run_id が 'dq-<mall>-<YYYY-MM>-' で始まる row_count_drift のうち
 *     details の daily_row_count が 0 でないもの (0 行の run は必ず daily_row_count: 0 を残している。
 *     LINE・Qoo10 の当月の「直近 8 日」の記録は daily_row_count を持たないが、当月の行が 1 以上のときしか出ない)
 *   表が読めないなど判定できないときは true (= 猶予を使わない向き)
 */
export function monthHadRowsBefore(db, mall, ym) {
  graceSpec(mall);
  if (!/^\d{4}-\d{2}$/.test(ym)) return true;
  try {
    const hit = db.prepare(`
      SELECT 1 AS hit FROM dq_run_results
      WHERE (check_name = ? AND actual_value > 0
             AND (CASE WHEN details_json IS NOT NULL AND json_valid(details_json)
                       THEN json_extract(details_json, '$.mall') = ? AND json_extract(details_json, '$.month') = ?
                       ELSE 0 END))
         OR (check_name = 'row_count_drift' AND run_id LIKE ?
             AND (CASE WHEN details_json IS NULL OR NOT json_valid(details_json) THEN -1
                       ELSE COALESCE(json_extract(details_json, '$.daily_row_count'), -1) END) <> 0)
      LIMIT 1
    `).get(MONTH_ROW_COUNT_CHECK, mall, ym, `dq-${mall}-${ym}-%`);
    return !!hit;
  } catch {
    return true;
  }
}

/**
 * 条件 ③: 前月の fact の最新の日付が前月の末日から freshDays 日以内か。
 * @returns {{ ok: boolean, prevYm: string, maxDate: string|null, gapDays: number|null }}
 */
export function prevMonthFreshness(db, table, ym, freshDays) {
  const prevYm = prevMonthOf(ym);
  if (!/^f_[a-z0-9_]+_finance_sku_daily_v1$/.test(table)) throw new Error(`prevMonthFreshness: 表の名前が違う "${table}"`);
  let maxDate = null;
  try { maxDate = db.prepare(`SELECT MAX(date_jst) AS d FROM ${table} WHERE substr(date_jst, 1, 7) = ?`).get(prevYm)?.d || null; }
  catch { maxDate = null; }
  if (!maxDate || !/^\d{4}-\d{2}-\d{2}/.test(maxDate)) return { ok: false, prevYm, maxDate, gapDays: null };
  const [y, m] = prevYm.split('-').map(Number);
  const lastDay = Date.UTC(y, m, 0);                                    // 前月の末日 (UTC の 0 時として数える)
  const d = Date.UTC(...maxDate.slice(0, 10).split('-').map((v, i) => (i === 1 ? Number(v) - 1 : Number(v))));
  const gapDays = Math.round((lastDay - d) / 86400000);
  return { ok: gapDays <= freshDays, prevYm, maxDate: maxDate.slice(0, 10), gapDays };
}

/**
 * 当月 0 行を「月初の猶予」にしてよいか (条件 ①〜④ を全部見る。4 本 + 楽天の DQ はこれだけを呼ぶ)。
 * @param {import('better-sqlite3').Database} db
 * @param {{ mall: string, ym: string, now?: Date, noGrace?: boolean }} opt
 * @returns {{ grace: boolean, reasons: string[], calendar: object, hadRowsBefore: boolean|null, prev: object|null, graceDays: number }}
 */
export function decideMonthStartEmpty(db, { mall, ym, now = new Date(), noGrace = false }) {
  const spec = graceSpec(mall);
  const graceDays = monthStartGraceDays(mall, ym);
  const calendar = monthStartEmptyGrace(ym, { now, graceDays });
  const reasons = [];
  if (!calendar.grace) reasons.push(calendar.mode === 'current' ? `猶予 (${graceDays} 日まで) を過ぎた` : '当月でない');
  if (noGrace) reasons.push('呼び手が猶予を禁じた (この回のモールの取込が ❌)');
  let hadRowsBefore = null;
  let prev = null;
  if (reasons.length === 0) {
    hadRowsBefore = monthHadRowsBefore(db, mall, ym);
    if (hadRowsBefore) reasons.push('この月は前に行があった (一度 0 でなくなった月の 0 行 = 消えた)');
    prev = prevMonthFreshness(db, spec.table, ym, spec.prevMonthFreshDays);
    if (!prev.ok) reasons.push(prev.maxDate
      ? `前月 ${prev.prevYm} の最新の日付が ${prev.maxDate} (末日の ${prev.gapDays} 日前・許すのは ${spec.prevMonthFreshDays} 日まで) = 前月の終わりから止まっている疑い`
      : `前月 ${prev.prevYm} の行が無い`);
  }
  return { grace: reasons.length === 0, reasons, calendar, hadRowsBefore, prev, graceDays };
}

/** 猶予で通したときの最後の行 (daily-sync は子の最後の行を要約に使う = ⚠️ で始める。isMonthStartGraceSummary で見分ける) */
export const MONTH_START_GRACE_PREFIX = '⚠️ 月初の猶予:';
export function monthStartEmptyNote(table, ym, g) {
  const c = g.calendar || g;
  return `${MONTH_START_GRACE_PREFIX} ${table} に ${ym} はまだ 0 行 (JST ${c.dayOfMonth} 日・猶予は ${c.graceDays} 日まで = その月の最初の行の取込待ち)。${c.graceDays + 1} 日の朝も 0 行なら CRITICAL`;
}
/** daily-sync: 子の要約が月初の猶予の警告か (warn を立てて見出しを ⚠️ にする) */
export function isMonthStartGraceSummary(summary) {
  return typeof summary === 'string' && summary.trimStart().startsWith(MONTH_START_GRACE_PREFIX);
}
/**
 * 猶予の中で info に落とす検査 = 当月の行が 0 だと構造的に外れる検査だけ:
 *   listing_diff_pct (fact の合計 vs NE の売上) / whitelist_coverage_pct (出荷の進み具合) / fee_rate_drift_pct (LINE: 手数料 ÷ 売上 = 0 ÷ 0 → 0% で 12.95pp ずれる)
 * ほかの検査 (raw・全期間・原価・fact と raw の一致) はそのまま流す
 */
export const SKIP_IN_MONTH_START_GRACE = Object.freeze(['listing_diff_pct', 'whitelist_coverage_pct', 'fee_rate_drift_pct']);

/**
 * sync (au PAY・LINE ギフト・Qoo10): 0 行のとき空の chunk (= Render のその月を消す) を送らずに終えてよいか。
 *   送らないのは「暦の上で月初の猶予の中」かつ「この月が一度も 0 でなくなっていない」ときだけ。
 *   一度 0 でなくなった月の 0 行は今までどおり送る (DQ は CRITICAL なので daily-sync からは sync まで来ない)
 */
export function shouldSkipEmptyMonthClear(db, { mall, ym, now = new Date() }) {
  if (!ym || !/^\d{4}-\d{2}$/.test(ym)) return false;
  const calendar = monthStartEmptyGrace(ym, { now, graceDays: monthStartGraceDays(mall, ym) });
  if (!calendar.grace) return false;
  return !monthHadRowsBefore(db, mall, ym);
}

/**
 * 試験用の --now。env FINANCE_DQ_ALLOW_NOW=1 のときだけ受ける (本番で月の判定・閾値を動かせないように)。
 * 効くのは月の判定 (monthMode・月初の猶予) だけ。SQL の date('now') は動かさない。
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
export function resolveDqNow(nowArg, env = process.env) {
  if (nowArg == null) return new Date();
  if (env.FINANCE_DQ_ALLOW_NOW !== '1') throw new Error('--now は試験専用 (env FINANCE_DQ_ALLOW_NOW=1 のときだけ受ける)');
  return parseNowArg(nowArg);
}

/**
 * DQ の recordResult の入口で呼ぶ: 月初の猶予の中は SKIP_IN_MONTH_START_GRACE の検査を info に落とす (元の判定は details に残す)。
 * 猶予でない回 (grace = null) は何も変えない
 */
export function applyMonthStartSkip(grace, checkName, severity, details) {
  if (!grace || !SKIP_IN_MONTH_START_GRACE.includes(checkName)) return { severity, details };
  return { severity: 'info', details: { ...(details || {}), skipped_by_month_start_grace: true, severity_without_grace: severity } };
}
