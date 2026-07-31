/**
 * rakuten-store-coupon-lib.js — 楽天ストアクーポン 定期入れ替え の純ロジック (API 呼び出しなし)
 *
 * 何をするか:
 *   雑貨イズム楽天市場店の「N点以上ご購入でX円引きクーポン」4本を、期限が近づいたら
 *   次の期間ぶんとして作り直す。金額は恒常値引きに見えないよう毎回ずらす (中原さん運用)。
 *
 * 楽天側の前提 (2026-07-31 実測 coupon.search + 公式):
 *   - **クーポン発行APIがある** (Yahoo!と違い画面操作は不要) → RMS の自動化検知とも無関係
 *   - couponStartDate は「現在の60分後〜30日後」の範囲。開始60分前を過ぎると編集・削除不可
 *   - 有効期間は最大3ヶ月 (現行 2026/05/01 00:00〜07/31 23:59 = 開始から91日差 が実在)
 *   - **クーポン画像は店舗ロゴが自動で入り金額は焼き込まれない** → Yahoo!版のような
 *     「画像と金額の対応」制約が無く、金額を自由に振れる
 *   - 現行4本の設定: itemType=4 (受注) / discountType=1 (定額) / combineFlag=0 (併用不可) /
 *     displayFlag=1 (公開) / issueCount=0・memberAvailMaxCount=0 (無制限) / RS004=N (利用個数)
 *
 * 金額は巡回リストの「今の金額の次」を選ぶ = 状態ファイル不要で冪等 (Yahoo!版と同じ設計)。
 * ⚠️ **現況は coupon.search から読む。クーポン名ではなく discountFactor と RS004 を正とする**
 *    — 手作業時代に「3点以上ご購入で100円引きクーポン」なのに実額110円というズレが
 *    繰り返し発生しており、名前を信じると金額を誤って引き継ぐため。
 */

/**
 * 対象クーポンの定義。cycle = 金額の巡回リスト (±10円の2値往復、中原さん決定 2026-07-31)。
 * 現在の実額がリストに無い場合は cycle[0] へリセットする (履歴に 155円 のような
 * イレギュラーがあるため。リセットしたことは通知に明記する)。
 */
export const STORE_COUPON_DEFS = [
  { key: 'qty3', orderCount: 3, cycle: [100, 110] },
  { key: 'qty4', orderCount: 4, cycle: [150, 160] },
  { key: 'qty5', orderCount: 5, cycle: [200, 210] },
  { key: 'qty6', orderCount: 6, cycle: [250, 260] },
];

/** クーポン名は毎回の実額から作る (名前と実額のズレを構造的に防ぐ) */
export const COUPON_NAME_TEMPLATE = '{orderCount}点以上ご購入で{amount}円引きクーポン';

/** 発行時に固定する属性 (現行4本の実測値と同一) */
export const STORE_COUPON_FIXED = {
  couponCaption: '',
  issueCount: 0,            // 全体の利用回数上限なし
  itemType: 4,              // 受注 (ストア全品)
  discountType: 1,          // 定額値引き
  memberAvailMaxCount: 0,   // 1人あたりの利用回数上限なし
  combineFlag: 0,           // 併用不可
  displayFlag: 1,           // 公開 (クーポン一覧に掲載)
};

const DAY_MS = 86400000;
const pad2 = (n) => String(n).padStart(2, '0');

// ─────────── 日付ユーティリティ (JST壁時計をUTCとして扱い、ローカル時刻に触れない) ───────────

/** 'YYYY-MM-DD' → UTCミリ秒 (暦日として扱う)。不正なら null */
export function ymdToMs(ymd) {
  const m = String(ymd || '').trim().match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return null;
  const [y, mo, d] = [Number(m[1]), Number(m[2]), Number(m[3])];
  const ms = Date.UTC(y, mo - 1, d);
  const back = new Date(ms);
  if (back.getUTCFullYear() !== y || back.getUTCMonth() !== mo - 1 || back.getUTCDate() !== d) return null;
  return ms;
}

export function msToYmd(ms) {
  const d = new Date(ms);
  return `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}-${pad2(d.getUTCDate())}`;
}

export function addDays(ymd, n) {
  const ms = ymdToMs(ymd);
  return ms === null ? null : msToYmd(ms + n * DAY_MS);
}

export function diffDays(fromYmd, toYmd) {
  const a = ymdToMs(fromYmd);
  const b = ymdToMs(toYmd);
  if (a === null || b === null) return null;
  return Math.round((b - a) / DAY_MS);
}

/** JST の暦日 'YYYY-MM-DD' */
export function todayJst(nowMs = Date.now()) {
  return msToYmd(nowMs + 9 * 3600 * 1000);
}

/**
 * '2026-07-31T23:59:59+09:00' → '2026-07-31'。
 * 楽天は +09:00 で返すが、別オフセット (Z など) で返ってきた場合に JST の暦日がずれると
 * 重複判定や期間の連結を誤るため、その場合は JST に直してから日付を取る。
 * 解釈できない値・実在しない日付は null (fail-closed。呼び出し側で error になる)。
 */
export function isoToYmd(iso) {
  const s = String(iso || '');
  const m = s.match(/^(\d{4}-\d{2}-\d{2})T\d{2}:\d{2}:\d{2}\+09:00$/);
  if (m) return ymdToMs(m[1]) === null ? null : m[1];
  const ms = Date.parse(s);
  if (!Number.isFinite(ms)) return null;
  return msToYmd(ms + 9 * 3600 * 1000);
}

/** ymd の月から addMonths ヶ月後の月末 'YYYY-MM-DD' */
export function monthEndYmd(ymd, addMonths = 0) {
  const ms = ymdToMs(ymd);
  if (ms === null) return null;
  const d = new Date(ms);
  const total = d.getUTCFullYear() * 12 + d.getUTCMonth() + addMonths + 1; // +1 = 翌月
  const yy = Math.floor(total / 12);
  const mm = total % 12;
  return msToYmd(Date.UTC(yy, mm, 1) - DAY_MS);
}

/** UTCミリ秒 (JST壁時計) → '2026-08-01T00:00:00+09:00' */
export function msToJstIso(ms) {
  return `${new Date(ms).toISOString().slice(0, 19)}+09:00`;
}

// ─────────── 金額の巡回 ───────────

/**
 * 巡回リストの「次の金額」。
 * 現在値がリストに無ければ cycle[0] にリセットする (reset=true を返す)。
 */
export function nextAmount(cycle, current) {
  if (!Array.isArray(cycle) || cycle.length === 0) return { amount: null, reset: false };
  const i = cycle.indexOf(current);
  if (i < 0) return { amount: cycle[0], reset: true };
  return { amount: cycle[(i + 1) % cycle.length], reset: false };
}

export function renderCouponName({ orderCount, amount }) {
  return COUPON_NAME_TEMPLATE
    .replaceAll('{orderCount}', String(orderCount))
    .replaceAll('{amount}', String(amount));
}

// ─────────── 現況の読み取り ───────────

/**
 * coupon.search の結果から、この定義 (N点以上) のコピー元にすべき最新クーポンを選ぶ。
 * 判定は**クーポン名ではなく設定値** (定額値引き・受注・公開・RS004=N) で行う。
 * 最新 = couponEndDate が最も遅いもの。
 */
export function pickSourceCoupon(coupons, orderCount) {
  const cands = (coupons || []).filter((c) => c.discountType === 1
    && c.itemType === 4
    && c.displayFlag === 1
    && c.orderCountCond === orderCount
    && Number.isFinite(c.discountFactor));
  if (cands.length === 0) return null;
  return cands.slice().sort((a, b) => Date.parse(b.couponEndDate || 0) - Date.parse(a.couponEndDate || 0))[0];
}

/**
 * 同じ条件・同じ開始日のクーポンが既にあるか (二重発行の防止)。
 * couponName を渡すと「同名・同開始日」も重複とみなす — RS004 の読み取りに失敗した
 * レスポンスがあっても重複を見落とさないための保険 (見落とすと二重発行になる)。
 */
export function findExistingForStart(coupons, orderCount, startYmd, couponName = null) {
  if (!startYmd) return null;
  return (coupons || []).find((c) => {
    if (isoToYmd(c.couponStartDate) !== startYmd) return false;
    if (c.orderCountCond === orderCount && c.discountType === 1 && c.itemType === 4) return true;
    return couponName != null && c.couponName === couponName;
  }) || null;
}

// ─────────── 期間の決定 ───────────

/**
 * 次の掲載期間を決める。
 *   開始 = 前回の終了日の翌日 (通常は月初) / 終了 = 開始月の2ヶ月後の月末
 *   (= 月初〜3ヶ月後の月末。中原さん決定 2026-07-31)
 *
 * 実行が遅れて開始日が過去になっている場合は「今から minLeadMinutes 分後」に繰り上げる
 * (楽天は開始が現在+60分以降でないと登録できないため。空白は生じるが復活を優先)。
 *
 * maxSpanDays = 開始日と終了日の**日付差**の上限 (既定91 = 8/01〜10/31。暦日数では92日ぶん)。
 * 超える場合は終了日を切り詰める。楽天の実際の上限は「3ヶ月」で、91日差が通ることは
 * 2026-07-31 にテストクーポンの発行で実測済み。90日に締めたい場合は 90 を渡す。
 */
export function computeNextPeriod({ prevEndYmd, todayYmd, nowMs = Date.now(), maxSpanDays = 91, minLeadMinutes = 90 }) {
  const startYmd = addDays(prevEndYmd, 1);
  if (!startYmd) return null;
  const lead = diffDays(todayYmd, startYmd);
  if (lead === null) return null;

  let startIso;
  let delayed = false;
  let effectiveStartYmd = startYmd;
  if (lead > 0) {
    startIso = `${startYmd}T00:00:00+09:00`;
  } else {
    // 開始日が今日以前 = 遅延。今から minLeadMinutes 分後 (分単位切り上げ) から開始する
    delayed = true;
    const ms = Math.ceil((nowMs + minLeadMinutes * 60000) / 60000) * 60000;
    startIso = msToJstIso(ms + 9 * 3600 * 1000);
    effectiveStartYmd = isoToYmd(startIso);
  }

  let endYmd = monthEndYmd(effectiveStartYmd, 2); // 開始月 + 2ヶ月 の月末 = 3ヶ月ぶん
  if (!endYmd) return null;
  let truncated = false;
  const span = diffDays(effectiveStartYmd, endYmd);
  if (span !== null && span > maxSpanDays) {
    endYmd = addDays(effectiveStartYmd, maxSpanDays);
    truncated = true;
  }
  return {
    startYmd: effectiveStartYmd,
    endYmd,
    startIso,
    endIso: `${endYmd}T23:59:59+09:00`,
    delayed,
    truncated,
    spanDays: diffDays(effectiveStartYmd, endYmd),
  };
}

/** 楽天側の制約に照らした検証。戻り値は違反理由の配列 (空なら合格) */
export function validatePeriod({ startIso, endIso, nowMs = Date.now(), maxSpanDays = 91 }) {
  const errs = [];
  const startMs = Date.parse(startIso);
  const endMs = Date.parse(endIso);
  if (!Number.isFinite(startMs)) { errs.push(`開始日時が不正 (${startIso})`); return errs; }
  if (!Number.isFinite(endMs)) { errs.push(`終了日時が不正 (${endIso})`); return errs; }
  const leadMin = (startMs - nowMs) / 60000;
  if (leadMin < 60) errs.push(`開始が現在+60分未満 (${Math.floor(leadMin)}分後、楽天の下限は60分)`);
  if (leadMin > 30 * 24 * 60) errs.push(`開始が現在+30日超 (${Math.floor(leadMin / 1440)}日後、楽天の上限は30日)`);
  if (endMs <= startMs) errs.push(`終了が開始以前 (${startIso}〜${endIso})`);
  const span = diffDays(isoToYmd(startIso), isoToYmd(endIso));
  if (span !== null && span > maxSpanDays) errs.push(`開始〜終了の日付差が上限${maxSpanDays}日を超過 (${span}日)`);
  return errs;
}

// ─────────── 1本ぶんの計画 ───────────

/**
 * leadDays = 「次期間の開始まで残り何日になったら作るか」。毎日実行して、
 * 期限が近づいた1回だけ作る運用を想定 (既定7日)。
 */
export function planStoreCoupon({ def, coupons, todayYmd, nowMs = Date.now(), leadDays = 7, maxSpanDays = 91 }) {
  const src = pickSourceCoupon(coupons, def.orderCount);
  if (!src) {
    return { key: def.key, orderCount: def.orderCount, status: 'error', reason: `コピー元が見つからない (${def.orderCount}点以上の定額クーポンが1本も無い)` };
  }
  const srcEndYmd = isoToYmd(src.couponEndDate);
  const srcStartYmd = isoToYmd(src.couponStartDate);
  if (!srcEndYmd || !srcStartYmd) {
    return { key: def.key, orderCount: def.orderCount, status: 'error', reason: `コピー元の期間を解釈できない (${src.couponStartDate}〜${src.couponEndDate})`, source: src };
  }
  // 最新クーポンの掲載がまだ始まっていない = 次期間ぶんを作成済み
  const startLead = diffDays(todayYmd, srcStartYmd);
  if (startLead !== null && startLead > 0) {
    return {
      key: def.key, orderCount: def.orderCount, status: 'skip',
      reason: `次期間のクーポンが作成済み (${src.discountFactor}円 ${srcStartYmd}〜${srcEndYmd})`,
      source: src,
    };
  }
  const period = computeNextPeriod({ prevEndYmd: srcEndYmd, todayYmd, nowMs, maxSpanDays });
  if (!period) {
    return { key: def.key, orderCount: def.orderCount, status: 'error', reason: `期間を計算できない (前回終了=${srcEndYmd})`, source: src };
  }
  // 期限までまだ日があるうちは作らない
  const lead = diffDays(todayYmd, period.startYmd);
  if (lead !== null && lead > leadDays) {
    return {
      key: def.key, orderCount: def.orderCount, status: 'skip',
      reason: `作成にはまだ早い (次期間の開始=${period.startYmd}、あと${lead}日。${leadDays}日前になったら作成)`,
      source: src, period,
    };
  }
  const { amount, reset } = nextAmount(def.cycle, src.discountFactor);
  if (amount === null) {
    return { key: def.key, orderCount: def.orderCount, status: 'error', reason: `巡回リストが空 (設定を確認)`, source: src, period };
  }
  const couponName = renderCouponName({ orderCount: def.orderCount, amount });
  const dup = findExistingForStart(coupons, def.orderCount, period.startYmd, couponName);
  if (dup) {
    return {
      key: def.key, orderCount: def.orderCount, status: 'skip',
      reason: `同じ開始日のクーポンが既に存在 (${dup.discountFactor}円 ${period.startYmd}〜)`,
      source: src, period,
    };
  }
  const errs = validatePeriod({ startIso: period.startIso, endIso: period.endIso, nowMs, maxSpanDays });
  if (errs.length > 0) {
    return { key: def.key, orderCount: def.orderCount, status: 'error', reason: errs.join(' / '), source: src, period };
  }
  return {
    key: def.key,
    orderCount: def.orderCount,
    status: 'ready',
    source: src,
    prevAmount: src.discountFactor,
    amount,
    amountReset: reset,
    period,
    couponName,
  };
}

/** 計画 → coupon.issue のパラメータ */
export function planToIssueParams(plan) {
  return {
    ...STORE_COUPON_FIXED,
    couponName: plan.couponName,
    couponStartDate: plan.period.startIso,
    couponEndDate: plan.period.endIso,
    discountFactor: plan.amount,
    orderCountCond: plan.orderCount,
  };
}
