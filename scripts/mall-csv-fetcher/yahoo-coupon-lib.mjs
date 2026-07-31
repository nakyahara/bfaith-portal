/**
 * Yahoo!ストアクーポン 定期入れ替え — 純ロジック (画面操作なし)
 *
 * 背景 (実測 2026-07-31):
 *   - Yahoo!ショッピングにクーポン発行APIは存在しない → ストクリ画面操作が唯一の手段
 *   - **公開後のクーポンは編集・削除ができない** → 期限が来たら「コピーして新規発行」が正規運用
 *   - 公開期間と利用期間は同一期間しか設定できない (通常ストアクーポン)
 *   - ⭐**公開終了日時は公開開始日時より90日以内**という制約がある
 *     → 「3ヶ月」(91日) は設定不可。現行運用の 5/03〜7/31 がちょうど89日なのはこのため
 *   - 公開開始日時は登録日より60日以内
 *
 * 金額を毎回ずらすのは中原さんの運用意図 (同額を延々と続けると恒常値引きになるため)。
 * 巡回リストを設定で持ち、「今の金額の次」を選ぶ = 状態ファイル不要で冪等。
 */

/** 「2点以上購入で55円割引」形式のクーポン名を分解する */
export function parseCouponTitle(title) {
  const m = String(title || '').trim().match(/^(\d+)点以上購入で(\d+)円割引$/);
  if (!m) return null;
  return { orderCount: Number(m[1]), amount: Number(m[2]) };
}

/** 巡回リストの「次の金額」。現在値がリストに無ければ null (安全側で止める) */
export function nextAmount(cycle, current) {
  if (!Array.isArray(cycle) || !cycle.length) return null;
  const i = cycle.indexOf(current);
  if (i < 0) return null;
  return cycle[(i + 1) % cycle.length];
}

// ─────────── 日付ユーティリティ (UTCのみで計算。JSTローカル時刻に触れない) ───────────

const pad2 = (n) => String(n).padStart(2, '0');

/** 'YYYY/MM/DD' → UTCミリ秒 (暦日として扱う) */
export function ymdToMs(ymd) {
  const m = String(ymd || '').trim().match(/^(\d{4})\/(\d{1,2})\/(\d{1,2})$/);
  if (!m) return null;
  const [y, mo, d] = [Number(m[1]), Number(m[2]), Number(m[3])];
  const ms = Date.UTC(y, mo - 1, d);
  const back = new Date(ms);
  // 2026/02/31 のような実在しない日を弾く
  if (back.getUTCFullYear() !== y || back.getUTCMonth() !== mo - 1 || back.getUTCDate() !== d) return null;
  return ms;
}

/** UTCミリ秒 → 'YYYY/MM/DD' */
export function msToYmd(ms) {
  const d = new Date(ms);
  return `${d.getUTCFullYear()}/${pad2(d.getUTCMonth() + 1)}/${pad2(d.getUTCDate())}`;
}

export function addDays(ymd, n) {
  const ms = ymdToMs(ymd);
  return ms === null ? null : msToYmd(ms + n * 86400000);
}

export function diffDays(fromYmd, toYmd) {
  const a = ymdToMs(fromYmd);
  const b = ymdToMs(toYmd);
  if (a === null || b === null) return null;
  return Math.round((b - a) / 86400000);
}

/** 実行時点のJST暦日 'YYYY/MM/DD' */
export function todayJst(nowMs = Date.now()) {
  return msToYmd(nowMs + 9 * 3600 * 1000);
}

/**
 * 次の掲載期間を決める。
 *   開始 = 前回の利用終了日の翌日 / 終了 = 開始 + periodDays 日
 *   ただし実行が遅れて「前回終了日の翌日」が過去になっている場合は翌日始まりに繰り上げる
 *   (空白期間は生じるが、クーポンが復活しないよりはよい)。
 * 時刻は開始00:00・終了23:00 (現行運用と同じ) を呼び出し側が付ける。
 */
export function computeNextPeriod(prevEndYmd, periodDays, todayYmd = null) {
  let start = addDays(prevEndYmd, 1);
  if (!start) return null;
  let delayed = false;
  if (todayYmd) {
    const lead = diffDays(todayYmd, start);
    if (lead !== null && lead <= 0) {
      start = addDays(todayYmd, 1); // 最短で明日から
      delayed = true;
    }
  }
  const end = addDays(start, periodDays);
  if (!end) return null;
  return { startYmd: start, endYmd: end, delayed };
}

/**
 * Yahoo側の制約に照らして期間を検証する。戻り値は違反理由の配列 (空なら合格)。
 *   - 公開終了は公開開始より90日以内
 *   - 公開開始は登録日 (today) より60日以内、かつ過去日でない
 */
export function validatePeriod({ startYmd, endYmd, todayYmd }) {
  const errs = [];
  const span = diffDays(startYmd, endYmd);
  if (span === null) { errs.push(`期間の日付が不正 (${startYmd}〜${endYmd})`); return errs; }
  if (span < 1) errs.push(`終了日が開始日以前 (${startYmd}〜${endYmd})`);
  if (span > 90) errs.push(`公開期間が90日を超過 (${span}日、Yahoo制約は90日以内)`);
  const lead = diffDays(todayYmd, startYmd);
  if (lead === null) { errs.push(`基準日が不正 (${todayYmd})`); return errs; }
  if (lead < 0) errs.push(`開始日が過去 (${startYmd} < 今日${todayYmd})`);
  if (lead > 60) errs.push(`開始日が登録日から60日超 (${lead}日先、Yahoo制約は60日以内)`);
  return errs;
}

/** クーポン名・説明文を実額に合わせて生成する (テンプレは設定側) */
export function renderTemplate(tpl, { orderCount, amount }) {
  return String(tpl || '')
    .replaceAll('{orderCount}', String(orderCount))
    .replaceAll('{amount}', String(amount));
}

/**
 * 一覧行から、定義 (orderCount) に対応する「コピー元にすべき最新クーポン」を選ぶ。
 * 最新 = 利用終了日時が最も遅いもの。削除済みは除外する。
 */
export function pickSourceRow(rows, orderCount) {
  const cands = rows
    .map((r) => ({ row: r, parsed: parseCouponTitle(r.title) }))
    .filter((x) => x.parsed && x.parsed.orderCount === orderCount)
    .filter((x) => x.row.state !== '削除済み');
  if (!cands.length) return null;
  cands.sort((a, b) => (ymdToMs(b.row.useEndYmd) ?? 0) - (ymdToMs(a.row.useEndYmd) ?? 0));
  return { ...cands[0].row, ...cands[0].parsed };
}

/**
 * 同じ内容 (同一 orderCount) で、これから作ろうとしている期間と重なるクーポンが既にあるか。
 * 二重作成の防止。開始日が一致するものがあれば「作成済み」とみなす。
 */
export function findExistingForPeriod(rows, orderCount, startYmd) {
  return rows.find((r) => {
    const p = parseCouponTitle(r.title);
    return p && p.orderCount === orderCount && r.state !== '削除済み' && r.useStartYmd === startYmd;
  }) || null;
}

/**
 * 1本ぶんの入れ替え計画を組み立てる。
 * leadDays = 「次期間の開始まで残り何日になったら作るか」。毎日実行して、
 *   期限が近づいた1回だけ作る運用を想定 (既定7日)。
 */
export function planCoupon({ def, rows, periodDays, todayYmd, leadDays = 7 }) {
  const src = pickSourceRow(rows, def.orderCount);
  if (!src) {
    return { key: def.key, status: 'error', reason: `コピー元が見つからない (${def.orderCount}点以上のクーポンが一覧に無い)` };
  }
  // 最新クーポンの掲載がまだ始まっていない = 次期間ぶんを作成済み → 何もしない
  const startLead = diffDays(todayYmd, src.useStartYmd);
  if (startLead !== null && startLead > 0) {
    return {
      key: def.key, status: 'skip',
      reason: `次期間のクーポンが作成済み (${src.couponId} ${src.title} ${src.useStartYmd}〜${src.useEndYmd})`,
      source: src,
    };
  }
  const amount = nextAmount(def.cycle, src.amount);
  if (amount === null) {
    return {
      key: def.key, status: 'error',
      reason: `現在の金額 ${src.amount}円 が巡回リスト [${def.cycle.join(', ')}] に無い (設定を確認)`,
      source: src,
    };
  }
  const period = computeNextPeriod(src.useEndYmd, periodDays, todayYmd);
  if (!period) {
    return { key: def.key, status: 'error', reason: `期間を計算できない (前回終了=${src.useEndYmd})`, source: src };
  }
  // 期限までまだ日があるうちは作らない (毎日実行して、期限が近づいた1回だけ作る)
  const lead = diffDays(todayYmd, period.startYmd);
  if (lead !== null && lead > leadDays) {
    return {
      key: def.key, status: 'skip',
      reason: `作成にはまだ早い (次期間の開始=${period.startYmd}、あと${lead}日。${leadDays}日前になったら作成)`,
      source: src, period,
    };
  }
  const errs = validatePeriod({ ...period, todayYmd });
  if (errs.length) {
    return { key: def.key, status: 'error', reason: errs.join(' / '), source: src, period };
  }
  const dup = findExistingForPeriod(rows, def.orderCount, period.startYmd);
  if (dup) {
    return {
      key: def.key, status: 'skip',
      reason: `同じ開始日のクーポンが既に存在 (${dup.couponId} ${dup.title})`,
      source: src, period,
    };
  }
  // 画像は金額と1対1。対応画像が無い金額では発行しない (画像と実額のズレを構造的に防ぐ)
  const image = def.images ? def.images[amount] : def.image;
  if (!image) {
    return {
      key: def.key, status: 'error',
      reason: `${amount}円に対応するクーポン画像が未登録 — 画像を作ってストアエディタにアップロードし、設定の images に追加してください`,
      source: src, period,
    };
  }
  return {
    key: def.key,
    status: 'ready',
    source: src,
    amount,
    orderCount: def.orderCount,
    period,
    title: renderTemplate(def.titleTemplate, { orderCount: def.orderCount, amount }),
    description: renderTemplate(def.descriptionTemplate, { orderCount: def.orderCount, amount }),
    image,
  };
}
