/**
 * Yahoo!ストアクリエイターPro ログイン共通モジュール (mall-csv-fetcher P1-Y)
 *
 * 前提 (実測 2026-07-11):
 *   - Yahoo!ビジネスIDはパスワードログイン廃止 → Yahoo! JAPAN ID連携に統一。
 *     自動ログイン(ID/PW入力)は不可能で、.profile-yahoo の永続セッション再利用が唯一の方式
 *   - 初回/セッション切れ時は中原さんが MANUAL=1 (yahoo-login-spike.mjs) で手動ログイン
 *     (確認コードはメール受信に設定済み)
 *   - ログイン関門URLをheadlessで連打するとCAPTCHAが出る → 常にツールURLへ直行し、
 *     切れていたら 2FA_REQUIRED を投げて手動再ログインを促す (関門を自動で触らない)
 *
 * 認証判定はCodex R2レビュー (Critical) 反映の「肯定証拠方式・fail-closed」:
 *   ドメインや否定文言でなく、認証後にしか出ない証拠 (ストアアカウント表示+ログアウト導線)
 *   が確認できた時だけログイン済みとみなす。判定材料が取れない場合はログイン失敗扱い。
 */

import { chromium } from 'playwright';
import { config as loadEnv } from 'dotenv';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const __dirname = dirname(fileURLToPath(import.meta.url));
loadEnv({ path: join(__dirname, '.env') });

// miniPC では secret をリポジトリ直下 .env に集約する運用 → 無いキーだけ選択的フォールバック
{
  const missing = ['YAHOO_BIZ_ID', 'YAHOO_STORE_ACCOUNT'].filter((k) => !process.env[k]);
  if (missing.length) {
    try {
      const txt = readFileSync(join(__dirname, '..', '..', '.env'), 'utf8');
      for (const k of missing) {
        const m = txt.match(new RegExp(`^\\s*${k}\\s*=\\s*"?([^"\\r\\n]+)"?\\s*$`, 'm'));
        if (m) process.env[k] = m[1].trim();
      }
    } catch { /* 直下 .env 無しは通常 */ }
  }
}

export const PROFILE_DIR = join(__dirname, '.profile-yahoo');
export const STORE_ACCOUNT = (process.env.YAHOO_STORE_ACCOUNT || 'b-faith01').trim();
export const STORE_TOP_URL = `https://pro.store.yahoo.co.jp/pro.${STORE_ACCOUNT}`;

export function safeHost(url) {
  try { return new URL(url).hostname; } catch { return ''; }
}

export async function openYahooContext() {
  return chromium.launchPersistentContext(PROFILE_DIR, {
    headless: process.env.HEADLESS === '1',
    slowMo: process.env.HEADLESS === '1' ? 0 : 150,
    locale: 'ja-JP',
    viewport: { width: 1400, height: 1000 },
    acceptDownloads: true,
  });
}

/**
 * 認証済みの肯定証拠を検査する (fail-closed)。
 * 戻り値: { ok, reason, markers } — 判定材料が取得できなければ ok=false
 */
export async function checkLoggedInEvidence(page) {
  const host = safeHost(page.url());
  if (host !== 'pro.store.yahoo.co.jp') {
    return { ok: false, reason: `host=${host} (pro.store.yahoo.co.jp 以外)` };
  }
  let body = null;
  try {
    body = await page.locator('body').innerText({ timeout: 10000 });
  } catch {
    return { ok: false, reason: 'body取得失敗 (判定材料なし → fail-closed)' }; // Codex High対応
  }
  const compact = body.replace(/\s+/g, ' ');
  // 明示的なエラー/未認証ページの否定 (denylist)
  if (/ページが表示できません|指定されたページは存在しません|権限がないため表示できません|ログインが必要|再度ログイン/.test(compact)) {
    return { ok: false, reason: 'エラー/未認証ページ文言を検出' };
  }
  // 肯定証拠 (Codex Critical対応): ①ストアアカウント表示 は必須。
  // ②ログアウト導線 or 管理ナビ一式 (注文管理+販売管理) のどちらか
  //   (実測: ストクリtopはログアウトが「詳細」ドロップダウン内で innerText に出ない)
  const hasAccount = compact.includes(STORE_ACCOUNT);
  const hasLogout = /ログアウト/.test(compact);
  const hasNav = /注文管理/.test(compact) && /販売管理/.test(compact);
  if (hasAccount && (hasLogout || hasNav)) {
    return { ok: true, reason: 'ok', markers: { account: true, logout: hasLogout, nav: hasNav } };
  }
  return { ok: false, reason: `肯定証拠不足 (account=${hasAccount} logout=${hasLogout} nav=${hasNav})` };
}

/**
 * ストクリのページへ遷移して認証状態を検証する。
 * - goto失敗/HTTPエラーは throw (前ページで判定を続けない — Codex High対応)
 * - ログイン関門へ飛ばされたら 2FA_REQUIRED を throw (自動で関門を触らない=CAPTCHA回避)
 */
export async function gotoStorePage(page, url, label = url) {
  let resp = null;
  try {
    resp = await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 45000 });
  } catch (e) {
    throw new Error(`NAV_FAILED: ${label} へ遷移できず (${String(e.message).split('\n')[0]})`);
  }
  await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
  await page.waitForTimeout(1500);

  const finalHost = safeHost(page.url());
  // ⚠️2026-07-25 以降、セッション切れの飛び先が LINEヤフー Business ID (account.line.biz) に
  // 変わった。これを 2FA_REQUIRED として扱わないと「AUTH_VERIFY: 認証済み証拠を確認できず」
  // という分かりにくい通知になり、実際に6日間 (7/25〜7/30) 停止に気付けなかった。
  if (/login\.yahoo\.co\.jp|login\.bizmanager|account\.line\.biz/.test(finalHost)) {
    throw new Error(
      '2FA_REQUIRED: Yahoo!のセッションが切れています (再ログインが必要)。'
      + 'ミニPCの画面で デスクトップの「Yahoo-Relogin.bat」をダブルクリック → '
      + '「Yahoo! JAPAN ID」を選んでログイン (確認コードはメール受信)。'
      + 'コマンドで実行する場合: node scripts/mall-csv-fetcher/yahoo-manual-login.mjs'
    );
  }
  // メインframeのHTTP statusを検証 (リダイレクト後の最終レスポンス)
  if (resp && resp.status() >= 400) {
    throw new Error(`NAV_FAILED: ${label} が HTTP ${resp.status()}`);
  }
  const ev = await checkLoggedInEvidence(page);
  if (!ev.ok) {
    throw new Error(`AUTH_VERIFY: ${label} で認証済み証拠を確認できず (${ev.reason})`);
  }
  return page;
}

/** セッション健全性チェック: ストクリtopに直行して肯定証拠を検証 */
export async function ensureStoreLogin(page) {
  await gotoStorePage(page, STORE_TOP_URL, 'ストクリtop');
  console.log(`[yahoo-login] セッション有効 (store=${STORE_ACCOUNT}, 肯定証拠OK)`);
}
