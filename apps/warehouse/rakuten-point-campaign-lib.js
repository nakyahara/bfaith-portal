/**
 * rakuten-point-campaign-lib.js — 商品別ポイント変倍 (Item API 2.0 pointCampaign) の純関数群
 *
 * 仕様 (2026-08-04 に zz- テスト商品で実測):
 *   PATCH /es/2.0/items/manage-numbers/{mn}
 *     { "pointCampaign": { "applicablePeriod": { "start", "end" }, "benefits": { "pointRate": N } } }
 *   → 204。GET で読み戻すと end は「その時間の :59:59」に正規化される
 *     (23:59:59 を送れば月末いっぱいでそのまま一致する)。倍率は 2〜20。
 *
 * 運用 (中原さん指定 2026-08-04):
 *   毎月1日の深夜に「当月1日 12:00 〜 月末 23:59:59」を設定する。
 *   商品別ポイント変倍は1商品1期間しか持てず予約が効かないため、
 *   前月分が月末 23:59:59 で終わった後〜当日 12:00 の開始前 (深夜帯) に書くのが安全。
 */

/** JST の年月日時分秒に分解する (toISOString の UTC ズレ対策で +9h して UTC getter を使う)。不正な日時は null */
export function jstParts(nowIso) {
  const ms = Date.parse(nowIso);
  if (!Number.isFinite(ms)) return null;
  const t = new Date(ms + 9 * 3600 * 1000);
  return {
    year: t.getUTCFullYear(),
    month: t.getUTCMonth() + 1, // 1-12
    day: t.getUTCDate(),
    hour: t.getUTCHours(),
    minute: t.getUTCMinutes(),
  };
}

const pad2 = (n) => String(n).padStart(2, '0');

/** その月の末日 (JST) */
export function lastDayOfMonth(year, month) {
  return new Date(Date.UTC(year, month, 0)).getUTCDate();
}

export function jstIso(year, month, day, hh, mm, ss) {
  return `${year}-${pad2(month)}-${pad2(day)}T${pad2(hh)}:${pad2(mm)}:${pad2(ss)}+09:00`;
}

/**
 * シートの「ポイント割引率」セルを倍率 (整数) にする。
 * "10倍" / "10" / 全角 "１０倍" を受け付ける。RMS の制約 (2〜20) を外れたら error。
 */
export function parsePointRate(raw) {
  const s = String(raw ?? '').normalize('NFKC').trim().replace(/倍$/, '');
  if (!/^\d+$/.test(s)) return { ok: false, error: `倍率が読めません: "${raw}"` };
  const n = parseInt(s, 10);
  if (n < 2 || n > 20) return { ok: false, error: `倍率 ${n} は範囲外です (RMS の商品別ポイント変倍は 2〜20 倍)` };
  return { ok: true, rate: n };
}

/**
 * Sheets values.get の生 rows (A:商品コード / B:商品名 / C:ポイント割引率) を対象リストにする。
 * ヘッダー行はA列「商品コード」で判定して除外。空行は無視。
 * 不正行が1つでもあれば ok:false (書き込み前に必ず全行検証する — 部分適用で歯抜けにしない)。
 */
export function parseSheetRows(values, { maxRows = 100 } = {}) {
  const rows = [];
  const errors = [];
  // 先頭の非空行が期待どおりのヘッダーであることを必須にする。
  // 先頭シート依存 (rangeにタブ名を付けていない) なので、シートのタブ順が入れ替わって
  // 別の表を読んでしまった場合にここで止まるようにする (Codex指摘)
  const firstNonEmpty = (values || []).find((r) => String(r?.[0] ?? '').trim() !== '');
  const headerOk = firstNonEmpty
    && String(firstNonEmpty[0]).trim() === '商品コード'
    && String(firstNonEmpty[1] ?? '').trim() === '商品名'
    && String(firstNonEmpty[2] ?? '').trim() === 'ポイント割引率';
  if (!headerOk) {
    return { ok: false, errors: [`先頭行がヘッダー (商品コード/商品名/ポイント割引率) ではありません: ${JSON.stringify(firstNonEmpty ?? null)}。別のシートを読んでいる可能性があるため中止`], rows: [] };
  }
  for (const [i, row] of (values || []).entries()) {
    const code = String(row?.[0] ?? '').trim();
    if (!code) continue;
    if (code === '商品コード') continue; // ヘッダー
    // RMS の商品管理番号は半角英数とハイフン (大文字はRMS側が小文字扱いなので寄せる)
    const mn = code.normalize('NFKC').toLowerCase();
    if (!/^[a-z0-9][a-z0-9_-]*$/.test(mn)) {
      errors.push(`${i + 1}行目: 商品コードが不正です: "${code}"`);
      continue;
    }
    const name = String(row?.[1] ?? '').trim();
    const rate = parsePointRate(row?.[2]);
    if (!rate.ok) {
      errors.push(`${i + 1}行目 (${code}): ${rate.error}`);
      continue;
    }
    rows.push({ manageNumber: mn, name, pointRate: rate.rate });
  }
  if (rows.length === 0) errors.push('対象商品が0件です (シートが空か、全行が不正)');
  if (rows.length > maxRows) errors.push(`対象が ${rows.length} 件あります (安全上限 ${maxRows} 件)。上限を見直すか、シートを確認してください`);
  const dup = rows.map((r) => r.manageNumber).filter((v, i, a) => a.indexOf(v) !== i);
  if (dup.length > 0) errors.push(`商品コードが重複しています: ${[...new Set(dup)].join(', ')}`);
  return errors.length > 0 ? { ok: false, errors, rows } : { ok: true, rows };
}

/**
 * 当月のポイント変倍期間を決める。
 *   通常 (1日深夜の定時実行): 当月1日 12:00 〜 月末 23:59:59
 *   遅延リカバリ (2日目以降や 11:00 過ぎ): 開始を「次に間に合う 12:00」へずらす
 *     (RMS は過去開始を受けないため。11:00〜12:00 は余裕1時間を切るので翌日に送る)
 * 月末までに開始できない場合は ok:false。
 */
export function computeMonthlyPeriod(nowIso) {
  const now = jstParts(nowIso);
  if (!now) return { ok: false, error: `実行日時が不正です: ${nowIso}` };
  const last = lastDayOfMonth(now.year, now.month);
  let startDay;
  if (now.day === 1 && now.hour < 11) {
    startDay = 1;
  } else {
    startDay = now.hour < 11 ? now.day : now.day + 1;
    if (startDay > last) return { ok: false, error: `今月 (${now.year}-${pad2(now.month)}) はもう開始できる日がありません` };
  }
  return {
    ok: true,
    ym: `${now.year}-${pad2(now.month)}`,
    start: jstIso(now.year, now.month, startDay, 12, 0, 0),
    end: jstIso(now.year, now.month, last, 23, 59, 59),
    delayed: startDay !== 1,
  };
}

/** 定時実行の実行日ゲート: 1〜3日 (1日が本番、2〜3日は取りこぼしリカバリ)。不正日時は false (実行しない側に倒す) */
export function isRunDay(nowIso) {
  return (jstParts(nowIso)?.day ?? 99) <= 3;
}

/** RMS から読んだ pointCampaign が desired (rate + period) と一致しているか */
export function campaignEquals(current, { pointRate, start, end }) {
  if (!current) return false;
  return current?.benefits?.pointRate === pointRate
    && current?.applicablePeriod?.start === start
    && current?.applicablePeriod?.end === end;
}

/**
 * 既存 pointCampaign の状態。
 *   'none'     — 未設定
 *   'expired'  — 終了済み (上書きしてよい)
 *   'blocking' — 有効中 or 未来開始 (人が入れた設定かもしれないので上書きしない)
 *   'invalid'  — 期間が解釈できない (安全側に倒して上書きしない)
 * NaN 比較は常に false になり「上書きしてよい」側に落ちるため、明示的に invalid を分ける (Codex指摘)。
 */
export function campaignState(current, nowIso) {
  if (!current) return 'none';
  const s = Date.parse(current?.applicablePeriod?.start);
  const e = Date.parse(current?.applicablePeriod?.end);
  const t = Date.parse(nowIso);
  if (![s, e, t].every(Number.isFinite) || s > e) return 'invalid';
  return e < t ? 'expired' : 'blocking';
}

/**
 * 商品ごとの実行計画。
 * @param {Array} rows parseSheetRows の結果
 * @param {Map<string, {status:number, item:object|null}>} currentByMn GET 結果 (manageNumber → {status, item})
 * @param {{start,end,ym,delayed}} period computeMonthlyPeriod の結果
 * @param {string} nowIso
 * @param {boolean} overwriteActive 有効中の異なる設定を上書きするか (既定 false = conflict でスキップ)
 */
export function planPointCampaigns({ rows, currentByMn, period, nowIso, overwriteActive = false }) {
  const desiredBase = { start: period.start, end: period.end };
  return rows.map((row) => {
    const cur = currentByMn.get(row.manageNumber);
    const desired = { ...desiredBase, pointRate: row.pointRate };
    if (!cur || cur.status === 404) {
      return { ...row, action: 'error', reason: 'RMS に商品が見つかりません (商品コードの誤りか、削除済み)' };
    }
    if (cur.status !== 200 || !cur.item) {
      return { ...row, action: 'error', reason: `RMS の取得に失敗 (HTTP ${cur.status}${cur.error ? `: ${cur.error}` : ''})` };
    }
    const current = cur.item.pointCampaign ?? null;
    if (campaignEquals(current, desired)) {
      return { ...row, action: 'skip', reason: '設定済み (今月分と一致)', current };
    }
    const state = campaignState(current, nowIso);
    if (state === 'invalid') {
      return { ...row, action: 'error', current, reason: `既存設定の期間が解釈できません: ${JSON.stringify(current?.applicablePeriod ?? null)}` };
    }
    if (state === 'blocking' && !overwriteActive) {
      return {
        ...row, action: 'conflict', current,
        reason: `有効中または未来開始の別設定 (${current?.benefits?.pointRate}倍 〜${current?.applicablePeriod?.end}) があるため上書きしません (--overwrite で強制)`,
      };
    }
    return {
      ...row, action: 'set', current, desired,
      patchBody: { pointCampaign: { applicablePeriod: { start: desired.start, end: desired.end }, benefits: { pointRate: desired.pointRate } } },
    };
  });
}
