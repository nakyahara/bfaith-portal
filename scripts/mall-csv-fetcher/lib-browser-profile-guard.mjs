/**
 * lib-browser-profile-guard.mjs — Playwright 永続プロファイルを SYSTEM で開かせないための共通ガード
 *
 * 🚨2026-08-28 の事故: Yahoo のクーポン発行を SYSTEM のタスクで動かしたところ、
 * `.profile-yahoo` の Cookies が書き換わって**ストアのログインセッションが消えた**。
 * Chromium の Cookie は DPAPI で Windows ユーザーに紐づけて暗号化されるため、SYSTEM から開くと
 * 復号できず「ログアウト状態」と判断されて Cookie が破棄される。復旧には現地での 2FA 再ログインが要る。
 *
 * 同じ地雷は楽天・auPAY・Qoo10 の永続プロファイルにもあるので、判定と拒否をここ 1 箇所にまとめる。
 * **読み取りのつもりでも開いた時点で壊れる**ので、ブラウザを起動する前に止めること。
 */

/**
 * 実行アカウントが LocalSystem かを判定する (Windows)。
 * SYSTEM は %USERPROFILE% が C:\Windows\system32\config\systemprofile、
 * %USERNAME% が <COMPUTERNAME>$ になる。どちらかが当てはまれば SYSTEM とみなす。
 * @param env テスト用の注入点
 */
export function isSystemAccount(env = process.env) {
  const profile = String(env.USERPROFILE || '').replace(/\//g, '\\').toLowerCase();
  if (profile.includes('\\config\\systemprofile')) return true;
  const user = String(env.USERNAME || '').toLowerCase();
  const host = String(env.COMPUTERNAME || '').toLowerCase();
  if (user === 'system') return true;
  return !!host && user === `${host}$`;
}

/**
 * SYSTEM なら throw する。永続プロファイルを開く直前に必ず呼ぶ。
 * @param mall ログに出すモール名 (エラー文の主語)
 */
export function assertNotSystemAccount(mall) {
  if (!isSystemAccount()) return;
  throw new Error(`SYSTEM_ACCOUNT: SYSTEM 権限では ${mall} の永続プロファイルを開けません `
    + '(開くとログインセッションが壊れ、miniPC の画面で再ログインが必要になります)。'
    + 'このジョブは bfaith (Interactive) で実行してください');
}
