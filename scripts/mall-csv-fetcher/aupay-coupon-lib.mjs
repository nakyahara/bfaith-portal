/**
 * au PAYマーケット ストアクーポン 月次入れ替え — 純ロジック (画面操作なし)
 *
 * 背景 (実測 2026-07-31):
 *   - au PAY マーケットにクーポン発行APIは無い → Wow!manager の画面操作が唯一の手段
 *   - 一覧の「コピー」は素の href (/distributionCoupon/copyCreate/<couponId>) で開ける。
 *     コピーすると**日付欄だけが空になる**ので、期間を入れ直して作成する = 正規の月次運用
 *   - 公開後のクーポンは編集不可 (詳細画面から押せるのは「配布停止」だけ)
 *   - Yahoo! と違いクーポン画像を使っていない ("画像登録なし") → 金額を自由に振れる
 *
 * 運用ルール (中原さん確定 2026-07-31):
 *   - 期間は月初〜月末で固定する。配布 = 翌月1日00:00〜翌月末日23:59、
 *     利用 = 翌月1日00:00〜翌々月1日23:59 (現行と同じく配布終了の翌日まで使える)
 *   - 金額は2値を毎月交互に振る (同額を続けると恒常値引きになるため)
 *   - 毎月25日に翌月分を作る (毎日実行し、25日以降で未作成なら作る = 失敗した日の翌日に自動で挽回)
 */

const pad2 = (n) => String(n).padStart(2, '0');

// ─────────── 日付ユーティリティ (UTCのみで計算。JSTローカル時刻に触れない) ───────────

/** 'YYYY-MM-DD' → UTCミリ秒 (暦日として扱う)。実在しない日は null */
export function ymdToMs(ymd) {
  const m = String(ymd || '').trim().match(/^(\d{4})-(\d{1,2})-(\d{1,2})$/);
  if (!m) return null;
  const [y, mo, d] = [Number(m[1]), Number(m[2]), Number(m[3])];
  const ms = Date.UTC(y, mo - 1, d);
  const back = new Date(ms);
  if (back.getUTCFullYear() !== y || back.getUTCMonth() !== mo - 1 || back.getUTCDate() !== d) return null;
  return ms;
}

/** UTCミリ秒 → 'YYYY-MM-DD' */
export function msToYmd(ms) {
  const d = new Date(ms);
  return `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}-${pad2(d.getUTCDate())}`;
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

/** 実行時点のJST暦日 'YYYY-MM-DD' */
export function todayJst(nowMs = Date.now()) {
  return msToYmd(nowMs + 9 * 3600 * 1000);
}

/** n ヶ月先の1日 ('YYYY-MM-01') */
export function firstDayOfMonthAfter(ymd, n = 1) {
  const ms = ymdToMs(ymd);
  if (ms === null) return null;
  const d = new Date(ms);
  return msToYmd(Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + n, 1));
}

/** その日が属する月の末日 */
export function lastDayOfMonth(ymd) {
  const ms = ymdToMs(ymd);
  if (ms === null) return null;
  const d = new Date(ms);
  return msToYmd(Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + 1, 0));
}

/**
 * ⭐au PAY の制約 (実測 2026-07-31): 配布期間の終了日時は開始日時の「2時間後〜30日後」まで。
 * 開始 00:00 / 終了 23:59 なので、月末を終了日にできるのは開始が月末の29日前まで。
 *   31日ある月 → 2日開始 (8/02 00:00 〜 8/31 23:59 = 29日23:59)
 *   30日以下の月 → 1日開始 (9/01 00:00 〜 9/30 23:59)
 * 現行の手作業クーポンが「7/02〜7/31」だったのはこの制約によるもので、日付のズレではない。
 */
export const MAX_DISTRIBUTION_DAYS = 30;

/**
 * コピー元として許容する「利用終了からの経過日数」の上限。
 * 月次運用なら最新クーポンの利用終了は未来か、せいぜい数週間前 (25日実行なら24日前) のはず。
 * これを大きく超えるのは一覧の並び順や表示件数が変わったサインとみなして止める。
 */
export const STALE_SOURCE_DAYS = 45;

/**
 * 翌月分の掲載期間を決める。
 *   配布: (翌月1日 または 月末-29日のうち遅い方) 00:00 〜 翌月末日 23:59
 *   利用: 配布と同じ開始 〜 配布終了の翌日 23:59 (現行運用と同じ「翌日まで使える」形)
 * 時刻の「分秒」は画面が自動で 00 / 59 を入れる。
 */
export function computeNextMonthPeriod(todayYmd, maxDistDays = MAX_DISTRIBUTION_DAYS) {
  const firstOfNext = firstDayOfMonthAfter(todayYmd, 1);
  if (!firstOfNext) return null;
  const distEndYmd = lastDayOfMonth(firstOfNext);
  const earliestStart = addDays(distEndYmd, -(maxDistDays - 1));
  if (!distEndYmd || !earliestStart) return null;
  const distStartYmd = ymdToMs(earliestStart) > ymdToMs(firstOfNext) ? earliestStart : firstOfNext;
  return {
    monthLabel: firstOfNext.slice(0, 7),   // 'YYYY-MM'
    distStartYmd,
    distEndYmd,
    useStartYmd: distStartYmd,
    useEndYmd: addDays(distEndYmd, 1),     // 配布終了の翌日まで使える
  };
}

// ─────────── 一覧行のパース ───────────

/** 'ZACCA IZMで使える！商品2点以上で30円引きクーポン！' → { orderCount, amount } */
export function parseCouponTitle(title) {
  const m = String(title || '').match(/商品(\d+)点以上で(\d+)円引き/);
  if (!m) return null;
  return { orderCount: Number(m[1]), amount: Number(m[2]) };
}

/** '金額指定 30 円OFF' → 30 (％指定など金額以外は null) */
export function parseDiscountAmount(text) {
  const m = String(text || '').replace(/[,\s]/g, '').match(/^金額指定(\d+)円OFF$/);
  return m ? Number(m[1]) : null;
}

/** '2026-07-02 ~ 2026-08-01' → { startYmd, endYmd } */
export function parseUsePeriod(text) {
  const m = String(text || '').match(/(\d{4}-\d{2}-\d{2})\s*[~〜]\s*(\d{4}-\d{2}-\d{2})/);
  return m ? { startYmd: m[1], endYmd: m[2] } : null;
}

// ─────────── 金額の巡回 ───────────

/** 巡回リストの「次の金額」。現在値がリストに無ければ null (安全側で止める) */
export function nextAmount(cycle, current) {
  if (!Array.isArray(cycle) || !cycle.length) return null;
  const i = cycle.indexOf(current);
  if (i < 0) return null;
  return cycle[(i + 1) % cycle.length];
}

// ─────────── 計画 ───────────

/** 配布を止めたクーポン (中原さんが意図的に止めたもの) */
export function isStopped(row) {
  return /停止/.test(String(row?.distributionState || ''));
}

/**
 * 定義 (orderCount) に対応する「コピー元にすべき最新クーポン」= 利用終了が最も遅いもの。
 * 配布停止したものは除外する — 止めたクーポンを翌月また作り直してしまわないため
 * (全部止まっていればコピー元なし = エラーになり、人が気づける)。
 */
export function pickSourceRow(rows, orderCount) {
  const cands = rows.filter((r) => r.parsed && r.parsed.orderCount === orderCount && !isStopped(r));
  if (!cands.length) return null;
  return cands.slice().sort((a, b) => (ymdToMs(b.useEndYmd) ?? 0) - (ymdToMs(a.useEndYmd) ?? 0))[0];
}

/**
 * 作ろうとしている月のクーポンが既にあるか (二重作成の防止)。
 * 開始日ちょうどではなく「利用開始が同じ月」で見る — 手作業で 1日/2日 のどちらで
 * 作られていても作成済みと判定できるようにするため。
 */
export function findExistingForMonth(rows, orderCount, monthLabel) {
  return rows.find((r) => r.parsed && r.parsed.orderCount === orderCount
    && String(r.useStartYmd || '').slice(0, 7) === monthLabel) || null;
}

/**
 * 作成したクーポンが一覧に出ているかを探す (作成後の実在確認)。
 * ⚠️一覧のクーポン名には配布中だと「獲得用URLコピー」が続くので、名前の完全一致では
 * 見つからない。点数・金額・利用開始日で照合する。
 */
export function findCreatedRow(rows, { orderCount, amount, useStartYmd }) {
  return rows.find((r) => r.parsed && r.parsed.orderCount === orderCount
    && r.parsed.amount === amount && r.useStartYmd === useStartYmd) || null;
}

/**
 * 「二重作成の判定に使える一覧か」を確かめる。
 *
 * 一覧はページングされる (1ページ最大200件)。対象月の既存クーポンが次ページに押し出されると
 * 「未作成」と誤判定して**取り消せない二重作成**をしてしまう。そこで
 *   ①利用開始日の降順に並んでいる ②最古の行が対象月より前まで届いている
 * の両方が成り立つときだけ「対象月はこのページに全部載っている」と判断する。
 * (2026-07-31 実測: psz=200 で 2022-08 まで届き、降順の逆転は0件)
 */
export function checkListCoverage(rows, monthLabel) {
  if (!rows.length) return { ok: false, reason: '一覧が空 (画面仕様変更の疑い)' };
  // 日付を読めない行を黙って捨てると、それが対象月の既存クーポンだった場合に
  // 「未作成」と誤判定してしまう。1件でもあれば一覧全体を信用しない
  const undated = rows.filter((r) => ymdToMs(r.useStartYmd) === null);
  if (undated.length) {
    const e = undated[0];
    return {
      ok: false,
      reason: `利用期間を読み取れない行が ${undated.length}/${rows.length} 件ある `
        + `(例: ${e.couponId || '?'} "${e.usePeriodText || e.useStartYmd || ''}")。`
        + '対象月の既存クーポンを見落とす恐れがあるため中止する',
    };
  }
  const dates = rows.map((r) => r.useStartYmd);
  for (let i = 1; i < dates.length; i++) {
    if (dates[i - 1] < dates[i]) {
      return { ok: false, reason: `一覧が利用開始日の降順で並んでいない (${dates[i - 1]} の次が ${dates[i]})` };
    }
  }
  const oldest = dates[dates.length - 1];
  if (!(oldest < `${monthLabel}-01`)) {
    return {
      ok: false,
      reason: `一覧が ${monthLabel} より古い行まで届いていない (最古=${oldest})。`
        + '表示件数を増やさないと、既存クーポンの見落とし=二重作成につながる',
    };
  }
  return { ok: true, oldest, count: rows.length };
}

/**
 * 対象月にあって「クーポン名を解析できない」行。
 * 手作業で表記を変えたクーポンがあると二重作成の見落としにつながるため、人に確認させる。
 * (日付そのものを読めない行は checkListCoverage が先に弾く)
 */
export function findUnparsedInMonth(rows, monthLabel) {
  return rows.filter((r) => !r.parsed && String(r.useStartYmd || '').slice(0, 7) === monthLabel);
}

/** 今この瞬間に有効な (利用期間中の) クーポンがあるか。無ければ運用に穴が空いている */
export function hasActiveCoupon(rows, orderCount, todayYmd) {
  return rows.some((r) => {
    if (!r.parsed || r.parsed.orderCount !== orderCount || isStopped(r)) return false;
    const s = diffDays(r.useStartYmd, todayYmd);
    const e = diffDays(todayYmd, r.useEndYmd);
    return s !== null && e !== null && s >= 0 && e >= 0;
  });
}

/** クーポン名・説明文を実額に合わせて生成する (テンプレは設定側) */
export function renderTemplate(tpl, { orderCount, amount }) {
  return String(tpl || '')
    .replaceAll('{orderCount}', String(orderCount))
    .replaceAll('{amount}', String(amount));
}

/**
 * 1本ぶんの入れ替え計画を組み立てる。
 * createDay = 「毎月何日に翌月分を作るか」。毎日実行し、その日以降で未作成なら作る
 *   (作成日に失敗しても翌日以降に自動で挽回できる = 月末までのリトライ猶予)。
 */
export function planCoupon({ def, rows, todayYmd, createDay = 25, staleSourceDays = STALE_SOURCE_DAYS }) {
  const src = pickSourceRow(rows, def.orderCount);
  if (!src) {
    return { key: def.key, status: 'error', reason: `コピー元が見つからない (${def.orderCount}点以上のクーポンが一覧に無い、または全部配布停止)` };
  }
  // コピー元の期間や実額が読めない = 一覧のパースが壊れている。不可逆な登録には進まない
  if (ymdToMs(src.useStartYmd) === null || ymdToMs(src.useEndYmd) === null) {
    return {
      key: def.key, status: 'error',
      reason: `コピー元の利用期間を読み取れない (${src.couponId} "${src.usePeriodText || `${src.useStartYmd}〜${src.useEndYmd}`}")`,
      source: src,
    };
  }
  if (diffDays(src.useStartYmd, src.useEndYmd) < 0) {
    return {
      key: def.key, status: 'error',
      reason: `コピー元の利用期間が逆転している (${src.couponId} ${src.useStartYmd}〜${src.useEndYmd})`,
      source: src,
    };
  }
  if (src.discountAmount === null || src.discountAmount === undefined) {
    return {
      key: def.key, status: 'error',
      reason: `コピー元の割引額を読み取れない (${src.couponId} "${src.discountText || ''}")。`
        + '画面表記が変わった可能性があるため、実額を確認できないままでは作らない',
      source: src,
    };
  }

  // 一覧はページングされる。並び順が変わって古い行を最新と誤認すると、
  // 「作成済みなのに未作成と判定 → 二重作成」につながる。最新のはずのコピー元が
  // 大きく過去に終わっていたら、一覧の見え方がおかしいとみなして止める。
  const staleness = diffDays(src.useEndYmd, todayYmd);
  if (staleness !== null && staleness > staleSourceDays) {
    return {
      key: def.key, status: 'error',
      reason: `コピー元が古すぎる (最新の${def.orderCount}点クーポンが ${src.useEndYmd} に利用終了、${staleness}日前)。`
        + '一覧の並び順・表示件数が変わっていないか確認してください',
      source: src,
    };
  }
  // タイトルの金額と「割引種別」列の実額がずれていたら触らない (名前と実額の食い違いを伝播させない)
  if (src.discountAmount !== src.parsed.amount) {
    return {
      key: def.key, status: 'error',
      reason: `コピー元のクーポン名と実際の割引額が不一致 (名前=${src.parsed.amount}円 / 設定=${src.discountAmount}円、${src.couponId})`,
      source: src,
    };
  }

  const period = computeNextMonthPeriod(todayYmd);
  if (!period || !period.distEndYmd || !period.useEndYmd) {
    return { key: def.key, status: 'error', reason: `期間を計算できない (today=${todayYmd})`, source: src };
  }

  const dup = findExistingForMonth(rows, def.orderCount, period.monthLabel);
  if (dup) {
    return {
      key: def.key, status: 'skip',
      reason: `${period.monthLabel} 分は作成済み (${dup.couponId} ${dup.title} ${dup.useStartYmd}〜${dup.useEndYmd})`,
      source: src, period,
    };
  }

  const day = Number(todayYmd.slice(8, 10));
  if (day < createDay) {
    return {
      key: def.key, status: 'skip',
      reason: `作成日 (毎月${createDay}日) より前 (今日=${day}日)`,
      source: src, period,
    };
  }

  const amount = nextAmount(def.cycle, src.parsed.amount);
  if (amount === null) {
    return {
      key: def.key, status: 'error',
      reason: `現在の金額 ${src.parsed.amount}円 が巡回リスト [${def.cycle.join(', ')}] に無い (設定を確認)`,
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
    // 運用の穴 (今有効なクーポンが無い) は作成そのものは止めないが通知で目立たせる
    gapWarning: hasActiveCoupon(rows, def.orderCount, todayYmd)
      ? null
      : `現在有効な${def.orderCount}点クーポンがありません (直近=${src.useStartYmd}〜${src.useEndYmd})`,
  };
}
