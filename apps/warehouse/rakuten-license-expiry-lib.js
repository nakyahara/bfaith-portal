/**
 * rakuten-license-expiry-lib.js — 楽天RMS ライセンスキーの期限を見て「更新してください」の GChat 文面を作る (純粋関数)
 *
 * 背景: RMS WEB SERVICE の licenseKey は **90日** で失効する (2026-08-30 実測: 発行日 8/30 → 期限 11/28)。
 * 切れると serviceSecret/licenseKey を使う楽天 API が **全部** 401 GA0001 'Un-Authorised' になり、
 * 受注取込・未発送アラート・問い合わせ返信・クーポン・価格改定・product-hub 出品まで一斉に止まる
 * (2026-08-30 に実際に発生。返信の送信失敗で初めて気づいた)。
 *
 * 期限は API で取れる (実測):
 *   GET /es/1.0/license-management/license-key/expiry-date?licenseKey=<key>
 *   → 200 { "expiryDate": "2026-11-28T23:59:59" }   ※タイムゾーン表記なしの JST
 *
 * 通知の段階 (既定 14 日前から。env RAKUTEN_LICENSE_WARN_DAYS で変更可):
 *   - 残り N 日以下 → 🔑 リマインド (毎日 1 通)
 *   - 残り 3 日以下 → 🔴 至急
 *   - 失効済み / 401 → 🔴 失効 (楽天系が止まっている)
 *   - 期限を取得できない → 🔴 (期限を見張れていない状態も異常として通知)
 * Yahoo 版 (yahoo-token-expiry-lib.js) と同じ思想・同じ構造。
 */

export const DEFAULT_WARN_DAYS = 14;  // 更新は RMS 画面 + .env + サービス再起動の手作業。土日を跨いでも間に合う幅
export const URGENT_DAYS = 3;

/**
 * RMS が返す expiryDate を epoch ms にする。
 * タイムゾーン表記が無い形 ('2026-11-28T23:59:59') は **JST** として解釈する
 * (ホスト TZ に依存させない。miniPC 以外で動かしても同じ結果にする)。
 */
export function parseRmsExpiry(raw) {
  if (raw == null) return null;
  const s = String(raw).trim();
  if (!s) return null;
  const hasTz = /(?:Z|[+-]\d{2}:?\d{2})$/.test(s);
  const iso = hasTz ? s
    : /^\d{4}-\d{2}-\d{2}$/.test(s) ? `${s}T23:59:59+09:00`
      : `${s}+09:00`;
  const ms = Date.parse(iso);
  return Number.isFinite(ms) ? ms : null;
}

/** JST 暦日 'YYYY-MM-DD' (epoch ms / ISO 文字列どちらでも) */
export function jstDay(input) {
  const t = typeof input === 'number' ? input : Date.parse(input);
  if (!Number.isFinite(t)) return null; // 壊れた値でも throw しない (通知ジョブを落とさない)
  return new Date(t + 9 * 3600 * 1000).toISOString().slice(0, 10);
}

/** 期限までの残り日数 (JST 暦日の差。当日=0) */
export function daysUntil(expiryMs, nowMs) {
  const ed = jstDay(expiryMs), nd = jstDay(nowMs);
  if (!ed || !nd) return null;
  const a = Date.parse(`${ed}T00:00:00Z`);
  const b = Date.parse(`${nd}T00:00:00Z`);
  if (!Number.isFinite(a) || !Number.isFinite(b)) return null;
  return Math.round((a - b) / 86400000);
}

// 更新手順は「発行 → .env → 再起動」を必ずセットで書く。
// 2026-08-30 の復旧で .env だけ直して再起動を忘れ、失効したままの時間が伸びた
const HOW = [
  '',
  '▼ 更新のしかた (5分)',
  '1. RMS →「店舗様向け情報・サービス」→「WEB APIサービス」',
  '2. 「2-1 アプリ一覧」→「ライセンスキーの確認・変更」→「新しいライセンスキーを発行」',
  '3. miniPC の C:\\Users\\bfaith\\bfaith-portal\\.env の RAKUTEN_LICENSE_KEY= を新しい値に差し替え',
  '4. miniPC で Restart-Service WarehouseServer (再起動しないと古いキーのままです)',
  '',
  '※ 新しいキーを発行した瞬間に古いキーは使えなくなります。3〜4 は続けて実施してください',
];
const STOP = 'これが切れると楽天の受注取込・未発送アラート・問い合わせ返信・クーポン発行・価格改定・出品が全部止まります。';

/**
 * 通知が必要かを判定して文面を作る。不要なら null。
 * @param {object} p
 *   expiryDate  … API が返した expiryDate (文字列)。取れていなければ null
 *   authFailed  … 期限APIが 401 だった (= キー自体が失効している)
 *   fetchError  … 期限を取得できなかった理由 (401 以外。null なら成功)
 *   nowIso / nowMs, warnDays
 * @returns {null | { kind, level, daysLeft, expiryDay, text }}  kind = 重複抑止のキー
 */
export function buildLicenseExpiryNotice({
  expiryDate = null, authFailed = false, fetchError = null,
  nowIso = new Date().toISOString(), warnDays = DEFAULT_WARN_DAYS,
}) {
  const nowMs = Date.parse(nowIso);

  if (authFailed) {
    return {
      kind: 'expired', level: 'critical', daysLeft: null, expiryDay: null,
      text: ['🔴 *楽天RMS のライセンスキーが失効しています* (401 GA0001 Un-Authorised)', '',
        STOP.replace('これが切れると', '現在、').replace('止まります。', '止まっています。'), ...HOW].join('\n'),
    };
  }
  if (fetchError) {
    return {
      kind: 'expiry_unreachable', level: 'critical', daysLeft: null, expiryDay: null,
      text: [`🔴 *楽天RMS のライセンス期限を確認できません* (${fetchError})`, '',
        '期限を見張れていない状態です。楽天API の一時障害なら翌朝の実行で自動復帰します。',
        '続くようなら miniPC の疎通と RAKUTEN_SERVICE_SECRET / RAKUTEN_LICENSE_KEY を確認してください。', STOP].join('\n'),
    };
  }
  const expiryMs = parseRmsExpiry(expiryDate);
  if (expiryMs === null) {
    return {
      kind: 'expiry_unknown', level: 'critical', daysLeft: null, expiryDay: null,
      text: ['🔴 *楽天RMS のライセンス期限を解釈できません*', `値: ${String(expiryDate).slice(0, 40)}`, '', STOP, ...HOW].join('\n'),
    };
  }

  const daysLeft = daysUntil(expiryMs, nowMs);
  const expiryDay = jstDay(expiryMs) || '不明';
  if (daysLeft === null) {
    return {
      kind: 'expiry_unknown', level: 'critical', daysLeft: null, expiryDay: null,
      text: ['🔴 *楽天RMS のライセンス期限を解釈できません*', `値: ${String(expiryDate).slice(0, 40)}`, '', STOP, ...HOW].join('\n'),
    };
  }
  if (daysLeft < 0) {
    // 期限は過ぎているのに API は通っている (猶予期間 or 期限の解釈違い)。止まる前に必ず気づかせる
    return {
      kind: 'expired', level: 'critical', daysLeft, expiryDay,
      text: [`🔴 *楽天RMS のライセンスキーが期限切れです* (期限 ${expiryDay})`, '', STOP, ...HOW].join('\n'),
    };
  }
  if (daysLeft > warnDays) return null;

  const urgent = daysLeft <= URGENT_DAYS;
  return {
    kind: 'expiring', level: urgent ? 'critical' : 'warn', daysLeft, expiryDay,
    text: [
      `${urgent ? '🔴' : '🔑'} *楽天RMS のライセンスキー更新をお願いします* — 残り ${daysLeft} 日 (期限 ${expiryDay} まで)`,
      '', STOP, ...HOW,
    ].join('\n'),
  };
}
