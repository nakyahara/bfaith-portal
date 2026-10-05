/**
 * app-access — 「この人はこのアプリを使えるか」の判定 (1 か所)。
 *
 * server.js の requireAppAccess (アプリを mount するときの守り) と、ほかのアプリの情報を見せる画面
 * (例: マスタの入力が発注アプリの注文残を見せる = 発注アプリの利用権も要る) が同じ判定を使う。
 * allowedApps = '*' (全部) か、アプリ ID の配列。ログインしていない = 使えない
 */
export function sessionHasApp(session, appId) {
  if (!session || !session.authenticated) return false;
  const allowed = session.allowedApps;
  return allowed === '*' || (Array.isArray(allowed) && allowed.includes(appId));
}
