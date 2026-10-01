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
 *   ② この mall・月が前に一度も 0 行でなくなっていない (消えない印 dq_month_high_water で見る = 一度入った行が消えたのは本物の異常)
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

/** 毎回の DQ が dq_run_results に残す「この月の行数」(見るための記録。判定には使わない = 同じ run_id の再実行で消えるため。印は dq_month_high_water) */
export const MONTH_ROW_COUNT_CHECK = 'month_row_count';
export function monthRowCountDetails(mall, ym, count) {
  return { mall, month: ym, month_row_count: count, note: 'この月の行数 (見るための記録。月初の猶予の印は dq_month_high_water)' };
}

/**
 * 条件 ② の印 = mall・月ごとの「一度でも 0 行でなくなった」記録 (PR #1572 R2)。
 *   dq_run_results は同じ run_id の再実行で消える (各 DQ が最初に DELETE する) ので頼らない。
 *   印は一度付いたら消えない・減らない (max_row_count は大きい方を残すだけ。行を消す・0 に戻す処理はどこにも無い)。
 *   DQ は dq_run_results の DELETE より前に prepareMonthHighWater を呼ぶ (表を作る → 前の記録を 1 回だけ移す → この回の行数で印を付ける)。
 */
export const HIGH_WATER_TABLE = 'dq_month_high_water';
export function ensureMonthHighWater(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS dq_month_high_water (
      mall             TEXT NOT NULL,
      month            TEXT NOT NULL CHECK (month GLOB '[0-9][0-9][0-9][0-9]-[0-1][0-9]'),
      first_nonzero_at TEXT NOT NULL,                       -- 初めて 0 行でなくなったのを見た時刻 (前の記録から移したものはその記録の checked_at)
      max_row_count    INTEGER NOT NULL CHECK (max_row_count > 0),
      source           TEXT NOT NULL,                       -- 'dq' (DQ が見た) / 'legacy' (この PR より前の dq_run_results から移した)
      PRIMARY KEY (mall, month)
    );
    CREATE TABLE IF NOT EXISTS dq_month_high_water_legacy (
      id          INTEGER PRIMARY KEY CHECK (id = 1),       -- 前の記録を移したのは 1 回だけ
      migrated_at TEXT NOT NULL,
      marked      INTEGER NOT NULL
    );`);
}
/** 印を付ける (count > 0 のときだけ。既にあれば max_row_count を大きい方にするだけ = 減らない・消えない) */
export function markMonthHighWater(db, mall, ym, count, at, source = 'dq') {
  if (!(Number.isFinite(count) && count > 0) || !/^\d{4}-\d{2}$/.test(ym)) return false;
  db.prepare(`
    INSERT INTO dq_month_high_water (mall, month, first_nonzero_at, max_row_count, source) VALUES (?, ?, ?, ?, ?)
    ON CONFLICT (mall, month) DO UPDATE SET max_row_count = MAX(dq_month_high_water.max_row_count, excluded.max_row_count)
  `).run(mall, ym, at, Math.floor(count), source);
  return true;
}

/** この PR より前の run の check の組み合わせから、どのモールの DQ だったかを当てる (run_id を手で付けた run 用) */
const LEGACY_SIGNATURES = [
  ['rakuten', (c) => c.has('date_mismatch_units')],
  ['aupay', (c) => c.has('request_price_reconcile_diff_pct')],
  ['linegift', (c) => c.has('monthless_received_rows')],
  ['qoo10', (c) => c.has('settle_price_formula_match_pct')],
  ['yahoo', (c) => c.has('normalized_collision_count') && !c.has('request_price_reconcile_diff_pct')],
];
const LEGACY_RUN_ID_RE = /^dq-(rakuten|yahoo|aupay|linegift|qoo10)-(\d{4}-\d{2})-/;
function jstMonthOfIso(s) {
  const t = Date.parse(s);
  return Number.isNaN(t) ? null : jstShifted(new Date(t)).toISOString().slice(0, 7);
}
/**
 * 前の記録 (dq_run_results の全部の run) から印へ移す。1 回だけ (dq_month_high_water_legacy に済みの行)。
 *   0 行でなかった run = row_count_drift の details の daily_row_count が 0 でない (無い・壊れている JSON も「行があった」側に数える。
 *     LINE・Qoo10 の当月の「直近 8 日」の記録は daily_row_count を持たないが、当月の行が 1 以上のときしか出ない)。
 *     R1 の形の month_row_count (actual > 0・details に mall / month) も数える。
 *   モールと月: run_id が既定の形 (dq-<mall>-<YYYY-MM>-…) ならそこから。手で付けた run_id なら、モールは同じ run の check の組み合わせ、
 *     月はその run の checked_at の JST の月 (過去の月を手で流した run だと実際より新しい月に印が付く = 猶予を使わない向きにずれるだけ)。
 *   モールが当てられない run は数えない (0 行でなかった run は検査を全部流しているので、組み合わせは必ずある)。
 */
export function migrateLegacyHighWater(db, at) {
  if (db.prepare('SELECT 1 FROM dq_month_high_water_legacy WHERE id = 1').get()) return null;
  let marked = 0;
  let rows = [];
  try {
    rows = db.prepare(`
      SELECT r.run_id, r.check_name, r.actual_value, r.details_json, r.checked_at,
             (SELECT group_concat(c.check_name, ',') FROM dq_run_results c WHERE c.run_id = r.run_id) AS checks
      FROM dq_run_results r WHERE r.check_name IN ('row_count_drift', ?)
    `).all(MONTH_ROW_COUNT_CHECK);
  } catch { rows = []; }                                        // dq_run_results が無い DB = 移すものが無い
  for (const r of rows) {
    let d = null;
    let broken = false;
    if (r.details_json != null) { try { d = JSON.parse(r.details_json); } catch { broken = true; } }
    if (r.check_name === MONTH_ROW_COUNT_CHECK) {
      if (d && r.actual_value > 0 && Object.hasOwn(MONTH_START_GRACE, d.mall) && /^\d{4}-\d{2}$/.test(d.month || '')) {
        if (markMonthHighWater(db, d.mall, d.month, r.actual_value, r.checked_at || at, 'legacy')) marked++;
      }
      continue;
    }
    const nonzero = broken || !d || typeof d !== 'object' || d.daily_row_count !== 0;
    if (!nonzero) continue;
    const m = LEGACY_RUN_ID_RE.exec(r.run_id || '');
    let mall = m ? m[1] : null;
    let month = m ? m[2] : null;
    if (!mall) {
      const checks = new Set(String(r.checks || '').split(','));
      mall = (LEGACY_SIGNATURES.find(([, f]) => f(checks)) || [null])[0];
      month = jstMonthOfIso(r.checked_at);
    }
    if (!mall || !month) continue;
    const count = d && Number.isFinite(d.daily_row_count) && d.daily_row_count > 0 ? d.daily_row_count : 1;
    if (markMonthHighWater(db, mall, month, count, r.checked_at || at, 'legacy')) marked++;
  }
  db.prepare('INSERT INTO dq_month_high_water_legacy (id, migrated_at, marked) VALUES (1, ?, ?)').run(at, marked);
  return marked;
}
/** DQ の入口 (dq_run_results の DELETE より前): 表を作る → 前の記録を 1 回だけ移す → この回の行数で印を付ける (1 つの取引) */
export function prepareMonthHighWater(db, { mall, ym, count, at = new Date().toISOString() }) {
  graceSpec(mall);
  db.transaction(() => {
    ensureMonthHighWater(db);
    migrateLegacyHighWater(db, at);
    markMonthHighWater(db, mall, ym, count, at, 'dq');
  })();
}

/**
 * 条件 ②: この mall・月が前に一度でも 0 行でなくなったか (印 dq_month_high_water を見る)。
 *   表が無い・読めないなど判定できないときは true (= DQ は猶予を使わない向き / sync は今までどおり空の chunk を送る向き)
 */
export function monthHadRowsBefore(db, mall, ym) {
  graceSpec(mall);
  if (!/^\d{4}-\d{2}$/.test(ym)) return true;
  try {
    const row = db.prepare('SELECT max_row_count FROM dq_month_high_water WHERE mall = ? AND month = ?').get(mall, ym);
    return !!row && row.max_row_count > 0;
  } catch {
    return true;
  }
}

/** 'YYYY-MM-DD' が実在の日か (UTC で組み直して元の文字と一致) */
export function isRealYmd(s) {
  if (typeof s !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(s)) return false;
  const [y, m, d] = s.split('-').map(Number);
  const t = new Date(Date.UTC(y, m - 1, d));
  return t.toISOString().slice(0, 10) === s;
}

/**
 * 条件 ③: 前月の fact の最新の日付が前月の末日から freshDays 日以内か。
 *   「新しい」のは、最新の日付が実在の日で、前月の中で、0 ≤ 末日からの日数 ≤ freshDays のときだけ (R2: 2026-09-99 のような壊れた日付を新しいと読まない)
 * @returns {{ ok: boolean, prevYm: string, maxDate: string|null, gapDays: number|null, invalid?: boolean }}
 */
export function prevMonthFreshness(db, table, ym, freshDays) {
  const prevYm = prevMonthOf(ym);
  if (!/^f_[a-z0-9_]+_finance_sku_daily_v1$/.test(table)) throw new Error(`prevMonthFreshness: 表の名前が違う "${table}"`);
  let maxDate = null;
  try { maxDate = db.prepare(`SELECT MAX(date_jst) AS d FROM ${table} WHERE substr(date_jst, 1, 7) = ?`).get(prevYm)?.d ?? null; }
  catch { maxDate = null; }
  if (maxDate == null || maxDate === '') return { ok: false, prevYm, maxDate: null, gapDays: null };
  if (!isRealYmd(maxDate) || maxDate.slice(0, 7) !== prevYm) return { ok: false, prevYm, maxDate: String(maxDate), gapDays: null, invalid: true };
  const [y, m] = prevYm.split('-').map(Number);
  const lastDay = Date.UTC(y, m, 0);                                    // 前月の末日 (UTC の 0 時として数える)
  const [yy, mm, dd] = maxDate.split('-').map(Number);
  const gapDays = Math.round((lastDay - Date.UTC(yy, mm - 1, dd)) / 86400000);
  return { ok: gapDays >= 0 && gapDays <= freshDays, prevYm, maxDate, gapDays };
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
    if (!prev.ok) reasons.push(prev.invalid
      ? `前月 ${prev.prevYm} の最新の日付 "${prev.maxDate}" が実在の日でない (日付が壊れている疑い)`
      : prev.maxDate
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
