/**
 * jobs-monitor — 判定の純ロジック (I/Oなし・テスト可能)
 *
 * dead-man 方式: 「期限までに ok ping が観測されなかったこと」を検知する。
 * 「失敗した」はジョブ自身の通知が担当し、ここでは扱わない (通知疲れの回避)。
 *
 * scheduled_job の締切はアンカー方式 (Codexレビュー R1 high の反映):
 *   「最後の成功 + period」基準だと、遅れて成功するたびに締切が後ろへドリフトする
 *   (13:59に成功→翌日は20:59まで鳴らない)。そこで
 *   **「直近の予定時刻 (anchor_hour_jst) までに始まった実行が、猶予内に成功したか」**で判定する。
 *   late ⇔ now > 直近アンカー + grace かつ 最終成功 < 直近アンカー
 *
 * 初回ping未着の扱い (R1 high):
 *   永遠に uninitialized (情報扱い) だと、ping配線忘れ・ディスク消失が無音になる。
 *   台帳エントリを初めて見た時刻 (firstSeenAtMs) を起点に、同じ締切ルールで late へ昇格させる。
 *
 * status:
 *   ok             — 問題なし
 *   uninitialized  — ping未着だが、まだ初回締切前 (導入直後の正常状態)
 *   late           — scheduled_job: 締切超過 (走らなかった/固まった/止まっている/配線忘れ)
 *   due_soon       — human_obligation: 期限まで warn_days 以内
 *   overdue        — human_obligation: 期限超過
 *   remove_due     — temporary_asset: remove_by を過ぎた (片付け忘れ)
 */

const HOUR_MS = 3600 * 1000;
const DAY_MS = 24 * HOUR_MS;
const JST_OFFSET_MS = 9 * HOUR_MS;

/** 'YYYY-MM-DD' (JST暦日) → その日のJST 23:59:59 のUTCミリ秒。実在しない日は null */
export function removeByDeadlineMs(ymd) {
  const m = String(ymd || '').match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return null;
  const [y, mo, d] = [Number(m[1]), Number(m[2]), Number(m[3])];
  const ms = Date.UTC(y, mo - 1, d, 14, 59, 59); // JST 23:59:59
  const back = new Date(ms + JST_OFFSET_MS);
  if (back.getUTCFullYear() !== y || back.getUTCMonth() !== mo - 1 || back.getUTCDate() !== d) return null;
  return ms;
}

/** 直近の「JST hh:mm」の発生時刻 (UTCミリ秒)。now ちょうども過去として含む */
export function lastAnchorMs(nowMs, hourJst, minuteJst = 0) {
  const jst = new Date(nowMs + JST_OFFSET_MS);
  let anchor = Date.UTC(jst.getUTCFullYear(), jst.getUTCMonth(), jst.getUTCDate(), hourJst, minuteJst) - JST_OFFSET_MS;
  if (anchor > nowMs) anchor -= DAY_MS;
  return anchor;
}

/** ミリ秒差 → 人が読める "N日" / "N時間" */
export function fmtAge(ms) {
  if (!Number.isFinite(ms)) return '不明';
  const abs = Math.abs(ms);
  if (abs >= DAY_MS) return `${Math.floor(abs / DAY_MS)}日`;
  return `${Math.max(1, Math.floor(abs / HOUR_MS))}時間`;
}

/** 実行時点のJST暦日 'YYYY-MM-DD' */
export function todayJst(nowMs = Date.now()) {
  return new Date(nowMs + JST_OFFSET_MS).toISOString().slice(0, 10);
}

/**
 * 台帳の1エントリを評価する。
 * @param def   台帳エントリ
 * @param state { lastOkAtMs?: number|null, firstSeenAtMs?: number|null }
 * @param nowMs 現在時刻 (UTCミリ秒)
 */
export function evaluateEntry(def, state, nowMs) {
  const base = { id: def.id, type: def.type, importance: def.importance, purpose: def.purpose, runbook: def.runbook };
  const lastOk = state?.lastOkAtMs ?? null;
  const firstSeen = state?.firstSeenAtMs ?? nowMs;
  const neverPinged = lastOk === null;

  if (def.type === 'temporary_asset') {
    const deadline = removeByDeadlineMs(def.remove_by);
    if (deadline === null) return { ...base, status: 'remove_due', detail: `remove_by が不正 (${def.remove_by})。台帳を直してください` };
    if (nowMs > deadline) {
      return { ...base, status: 'remove_due', detail: `撤去期限 ${def.remove_by} を ${fmtAge(nowMs - deadline)} 過ぎています` };
    }
    return { ...base, status: 'ok', detail: `撤去期限 ${def.remove_by} まであと ${fmtAge(deadline - nowMs)}` };
  }

  if (def.type === 'scheduled_job') {
    const anchor = lastAnchorMs(nowMs, def.anchor_hour_jst, def.anchor_minute_jst ?? 0);
    const graceMs = def.grace_hours * HOUR_MS;
    // 「監視を始める前のアンカー」には責任を持てない (デプロイ直後の誤報防止)。
    // firstSeen より後のアンカーだけを評価対象にする
    const accountable = anchor >= firstSeen;
    const okSinceAnchor = lastOk !== null && lastOk >= anchor;

    // 「猶予までに成功が無ければ、その時点で late」— 境界含む (>=)
    if (accountable && nowMs >= anchor + graceMs && !okSinceAnchor) {
      const detail = neverPinged
        ? `ping が一度も来ないまま初回締切を超過。ジョブ側の ping 配線か、ジョブ自体が動いていない可能性`
        : `直近の予定 (JST ${String(def.anchor_hour_jst).padStart(2, '0')}:${String(def.anchor_minute_jst ?? 0).padStart(2, '0')} + 猶予${def.grace_hours}h) までに成功が来ていません (最終成功 ${fmtAge(nowMs - lastOk)} 前)`;
      return { ...base, status: 'late', detail };
    }
    if (neverPinged) {
      return { ...base, status: 'uninitialized', detail: 'ping がまだ来ていません (導入直後なら正常。ジョブ側の ping 配線を確認)' };
    }
    return { ...base, status: 'ok', detail: `最終成功 ${fmtAge(nowMs - lastOk)} 前` };
  }

  if (def.type === 'human_obligation') {
    // 未初期化でも firstSeen を起点に締切を数える (配線忘れ・記録忘れを無音にしない)
    const baseline = lastOk ?? firstSeen;
    const deadline = baseline + def.period_hours * HOUR_MS;
    const warnMs = (def.warn_days ?? 5) * DAY_MS;
    const doneNote = neverPinged ? '前回実施の記録がありません (実施済みなら ping で刻んでください)。監視開始起点で計算' : `前回実施 ${fmtAge(nowMs - baseline)} 前`;
    if (nowMs > deadline) {
      return { ...base, status: 'overdue', detail: `期限を ${fmtAge(nowMs - deadline)} 過ぎています (${doneNote})` };
    }
    if (nowMs > deadline - warnMs) {
      return { ...base, status: 'due_soon', detail: `期限まであと ${fmtAge(deadline - nowMs)} (${doneNote})` };
    }
    if (neverPinged) {
      return { ...base, status: 'uninitialized', detail: `${doneNote}。次回期限まであと ${fmtAge(deadline - nowMs)}` };
    }
    return { ...base, status: 'ok', detail: `次回期限まであと ${fmtAge(deadline - nowMs)}` };
  }

  return { ...base, status: 'ok', detail: `未知の type (${def.type})` };
}

/** 登録一覧と状態から全評価を返す */
export function evaluateAll(registry, statesById, nowMs) {
  return registry.map((def) => evaluateEntry(def, statesById[def.id] || null, nowMs));
}

/** 即時通知の対象か (P1/P2 の late/overdue のみ。それ以外は毎朝のサマリで扱う) */
export function needsImmediateAlert(result) {
  return (result.importance === 'P1' || result.importance === 'P2')
    && (result.status === 'late' || result.status === 'overdue');
}

/** 再通知間隔 (ms)。P1=6時間ごと / P2=24時間ごと */
export function realertIntervalMs(importance) {
  return importance === 'P1' ? 6 * HOUR_MS : 24 * HOUR_MS;
}

/**
 * 毎朝の「要対応」サマリ本文を組み立てる。
 * アクションが必要なものだけを並べ、無ければ1行で終わる (読み手ファースト)。
 *
 * 見出しの日付は必ず JST 暦日。呼び出し側に日付文字列を作らせない
 * (ISO文字列を受け取る形にしていたため、配信時刻 08:50 JST = 前日 23:50 UTC で
 *  毎朝ちょうど1日前の日付が出ていた。2026-08-02 に修正)。
 * @param nowMs 現在時刻 (UTCミリ秒)
 */
export function buildDigest(results, unknownIds = [], nowMs = Date.now()) {
  const attention = results.filter((r) => r.status !== 'ok');
  const lines = [];
  lines.push(`*🩺 定期実行 今日の要対応* (${todayJst(nowMs)})`);
  lines.push('');

  if (!attention.length && !unknownIds.length) {
    lines.push('✅ すべて正常です。対応が必要なものはありません。');
    return lines.join('\n');
  }

  const order = { late: 0, overdue: 0, remove_due: 1, due_soon: 2, uninitialized: 3 };
  const icon = { late: '🔴', overdue: '🔴', remove_due: '🟠', due_soon: '🟡', uninitialized: '⚪' };
  attention.sort((a, b) => (order[a.status] ?? 9) - (order[b.status] ?? 9));

  for (const r of attention) {
    lines.push(`${icon[r.status] ?? '❓'} *${r.id}* (${r.importance})`);
    lines.push(`　${r.detail}`);
    if (r.status !== 'uninitialized' && r.runbook) lines.push(`　▶ ${r.runbook}`);
    lines.push('');
  }

  if (unknownIds.length) {
    lines.push(`⚠️ *台帳に無い id から ping が来ています*: ${unknownIds.join(', ')}`);
    lines.push('　→ config/jobs-registry.mjs に登録するか、ping を止めてください (不明なまま残さない)');
  }

  return lines.join('\n').trimEnd();
}
