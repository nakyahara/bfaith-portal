/**
 * yahoo-token-expiry-lib.js — Yahoo refresh token の期限を見て「再認可してください」の GChat 文面を作る (純粋関数)
 *
 * 背景: Yahoo!ショッピング API の refresh token は 28 日で失効し、切れると受注取込・未発送アラート・
 * レビューメール (P2-Y) まで Yahoo 系が全滅する。再認可自体は在庫検索ボットに「yahoo再認可」と
 * 送れば 1 分で終わる (#958) ので、**期限が近づいたら向こうから声をかける**のがこの仕組み。
 *
 * 通知の段階 (既定 5 日前から。env YAHOO_TOKEN_WARN_DAYS で変更可):
 *   - 残り N 日以下  → 🔑 リマインド (毎日 1 通)
 *   - 残り 2 日以下  → 🔴 至急
 *   - 失効済み       → 🔴 失効 (Yahoo 系が止まっている)
 *   - health 到達不可 / トークン未初期化 / 期限不明 → 🔴 (期限を見張れていない状態も異常として通知)
 * 通知は「1 日 1 通」に絞る (daily-sync は retry で最大 4 回走るため)。判定は JST 暦日。
 */

export const DEFAULT_WARN_DAYS = 5;
export const URGENT_DAYS = 2;

/** JST 暦日 'YYYY-MM-DD' */
export function jstDay(nowIso) {
  const t = Date.parse(nowIso);
  if (!Number.isFinite(t)) return null; // 壊れた値でも throw しない (通知ジョブを落とさない)
  return new Date(t + 9 * 3600 * 1000).toISOString().slice(0, 10);
}

/** 期限までの残り日数 (JST 暦日の差。当日=0) */
export function daysUntil(expiryIso, nowIso) {
  const ed = jstDay(expiryIso), nd = jstDay(nowIso);
  if (!ed || !nd) return null;
  const a = Date.parse(`${ed}T00:00:00Z`);
  const b = Date.parse(`${nd}T00:00:00Z`);
  if (!Number.isFinite(a) || !Number.isFinite(b)) return null;
  return Math.round((a - b) / 86400000);
}

/**
 * 通知が必要かを判定して文面を作る。不要なら null。
 * @param {object} p { health, healthError, authUrl, nowIso, warnDays }
 *   health = /yahoo/health のレスポンス ({ hasTokens, refreshTokenExpiresAt, ... })
 * @returns {null | { kind, level, daysLeft, text }}  kind = 重複抑止のキー
 */
export function buildTokenExpiryNotice({ health, healthError = null, authUrl = null, nowIso = new Date().toISOString(), warnDays = DEFAULT_WARN_DAYS }) {
  const how = [
    '',
    '▼ 再認可のしかた (1 分)',
    '1. Google Chat の「在庫検索ボット」との DM に *yahoo再認可* と送る',
    '2. 返ってくるリンクを開いて Yahoo! JAPAN ID (b-faith01 のストア用) でログイン → 「同意」',
    '3. `https://b-faith.biz/?code=…` に戻ったら、その URL をそのままボットに貼る → 完了',
    ...(authUrl ? ['', `(ボットを使わずに始めるなら: ${authUrl})`] : []),
  ];
  const stop = 'これが切れると Yahoo の受注取込・未発送アラート・レビューメールが止まります。';

  if (healthError) {
    return { kind: 'health_unreachable', level: 'critical', daysLeft: null,
      text: [`🔴 *Yahoo API プロキシに接続できません* (${healthError})`, '',
        'トークンの期限を見張れていない状態です。VPS (aupay-proxy) の稼働を確認してください。', stop].join('\n') };
  }
  if (!health || health.hasTokens === false) {
    return { kind: 'no_tokens', level: 'critical', daysLeft: null,
      text: [`🔴 *Yahoo API のトークンが未初期化です*`, stop, ...how].join('\n') };
  }
  if (!health.refreshTokenExpiresAt) {
    return { kind: 'expiry_unknown', level: 'critical', daysLeft: null,
      text: ['🔴 *Yahoo refresh token の期限が不明です* (古い認可のまま)', '',
        '一度再認可すると期限が記録され、以後は自動で見張れるようになります。', ...how].join('\n') };
  }
  const daysLeft = daysUntil(health.refreshTokenExpiresAt, nowIso);
  if (daysLeft === null) {  // 期限の値が壊れている (Date.parse 不能)
    return { kind: 'expiry_unknown', level: 'critical', daysLeft: null,
      text: ['🔴 *Yahoo refresh token の期限を解釈できません*', `値: ${String(health.refreshTokenExpiresAt).slice(0, 40)}`, ...how].join('\n') };
  }
  const expiryDay = jstDay(health.refreshTokenExpiresAt) || '不明';
  if (daysLeft < 0) {
    return { kind: 'expired', level: 'critical', daysLeft,
      text: [`🔴 *Yahoo refresh token が失効しました* (期限 ${expiryDay})`, stop.replace('これが切れると', '現在、'), ...how].join('\n') };
  }
  if (daysLeft > warnDays) return null;
  const urgent = daysLeft <= URGENT_DAYS;
  return {
    kind: 'expiring', level: urgent ? 'critical' : 'warn', daysLeft,
    text: [
      `${urgent ? '🔴' : '🔑'} *Yahoo API の再認可をお願いします* — 残り ${daysLeft} 日 (期限 ${expiryDay} まで)`,
      stop, ...how,
    ].join('\n'),
  };
}
