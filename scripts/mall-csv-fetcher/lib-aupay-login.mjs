/**
 * au PAYマーケット WOW!マネージャー ログイン共通モジュール (mall-csv-fetcher P1-A)
 *
 * 前提 (実測 2026-07-12〜13):
 *   - ログインは https://manager.wowma.jp/wmshopclient/authclient/login の素直な
 *     ID(メール)/PWフォーム。初回のみメール暗証番号 (sndAuth) が出るが端末記憶
 *     (.profile-aupay) で以後は不要 → **ID/PW自動ログインだけで恒久無人化できる** (3モール唯一)
 *   - セッションは短命 (数十分) → 毎実行「セッション再利用を試し、切れていたら自動再ログイン」
 *   - sndAuth (メール暗証番号) が再び出た場合のみ 2FA_REQUIRED throw → MANUAL=1 で手動セットアップ
 *
 * 認証判定は肯定証拠方式・fail-closed (Yahoo P1-Y で確立したCodex方針):
 *   会員番号表示 + 管理ナビ語彙 (受注管理+店舗分析) が確認できた時だけログイン済みとみなす。
 */

import { chromium } from 'playwright';
import { assertNotSystemAccount } from './lib-browser-profile-guard.mjs';
import { config as loadEnv } from 'dotenv';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const __dirname = dirname(fileURLToPath(import.meta.url));
loadEnv({ path: join(__dirname, '.env') });

// miniPC では secret をリポジトリ直下 .env に集約する運用 → 無いキーだけ選択的フォールバック
{
  const missing = ['WOWMA_LOGIN_ID', 'WOWMA_LOGIN_PW'].filter((k) => !process.env[k]);
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

export const PROFILE_DIR = join(__dirname, '.profile-aupay');
export const WOWMA_BASE = 'https://manager.wowma.jp/wmshopclient/';
export const LOGIN_URL = `${WOWMA_BASE}authclient/login`;
export const TOP_URL = `${WOWMA_BASE}top/index`;

const MANUAL_HINT = '$env:MANUAL=1; node scripts/mall-csv-fetcher/aupay-login-spike.mjs (メール暗証番号を入力)';

export function safeHost(url) {
  try { return new URL(url).hostname; } catch { return ''; }
}

export function assertLoginEnv() {
  const missing = ['WOWMA_LOGIN_ID', 'WOWMA_LOGIN_PW'].filter((k) => !process.env[k]);
  if (missing.length) {
    throw new Error(`ENV_MISSING: ${missing.join(', ')} が未設定です (.env に記入は中原さん)`);
  }
}

export async function openAupayContext() {
  assertNotSystemAccount('auPAY'); // 2026-08-28 の事故: SYSTEM で開くとセッションが壊れる
  return chromium.launchPersistentContext(PROFILE_DIR, {
    headless: process.env.HEADLESS === '1',
    slowMo: process.env.HEADLESS === '1' ? 0 : 150,
    locale: 'ja-JP',
    viewport: { width: 1400, height: 1000 },
    acceptDownloads: true,
  });
}

/**
 * WOW!マネージャー画面の認証済み肯定証拠 (fail-closed)。
 * 実測: ログイン後は全画面に「会員番号：54318092」+ 左ナビ (受注管理/店舗分析) が出る
 */
export async function checkLoggedInEvidence(page) {
  const host = safeHost(page.url());
  if (host !== 'manager.wowma.jp') {
    return { ok: false, reason: `host=${host} (manager.wowma.jp 以外)` };
  }
  if (/authclient\/(login|sndAuth)/.test(page.url())) {
    return { ok: false, reason: 'ログイン/追加認証画面' };
  }
  let body = null;
  try {
    body = await page.locator('body').innerText({ timeout: 10000 });
  } catch {
    return { ok: false, reason: 'body取得失敗 (判定材料なし → fail-closed)' };
  }
  const compact = body.replace(/\s+/g, ' ');
  if (/再度ログインしてください|システムエラー|メンテナンス中/.test(compact)) {
    return { ok: false, reason: 'エラー/未認証ページ文言を検出' };
  }
  const hasMemberNo = /会員番号[:：]\s*\d+/.test(compact);
  const hasNav = /受注管理/.test(compact) && /店舗分析/.test(compact);
  if (hasMemberNo && hasNav) return { ok: true, reason: 'ok' };
  return { ok: false, reason: `肯定証拠不足 (memberNo=${hasMemberNo} nav=${hasNav})` };
}

/** ログインフォームで ID/PW 自動ログイン。sndAuth (メール暗証番号) が出たら 2FA_REQUIRED */
async function doAutoLogin(page) {
  assertLoginEnv();
  if (!/authclient\/login/.test(page.url())) {
    await page.goto(LOGIN_URL, { waitUntil: 'domcontentloaded', timeout: 45000 });
    await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
  }
  await page.locator('#loginId').fill(process.env.WOWMA_LOGIN_ID, { timeout: 10000 });
  await page.locator('#password').fill(process.env.WOWMA_LOGIN_PW, { timeout: 10000 });
  // #btnLogin は div ラッパーと input の2要素にマッチする (実測) → input を明示
  await page.locator('input#btnLogin').click({ timeout: 10000 });
  await page.waitForLoadState('networkidle', { timeout: 30000 }).catch(() => {});
  await page.waitForTimeout(2000);
  if (/authclient\/sndAuth/.test(page.url())) {
    throw new Error(`2FA_REQUIRED: メール暗証番号 (sndAuth) を要求されました (端末記憶が切れた模様)。miniPCで手動再登録: ${MANUAL_HINT}`);
  }
  if (/authclient\/login/.test(page.url())) {
    throw new Error('AUTH_FAILED: ID/PW自動ログインが弾かれました (認証情報またはロックを確認)');
  }
}

/**
 * WOW!マネージャーのページへ遷移して認証を保証する。
 * セッション切れ (ログイン画面へのリダイレクト) は1回だけ自動再ログイン → 再遷移。
 */
export async function gotoWowmaPage(page, url, label = url) {
  for (let attempt = 0; attempt < 2; attempt++) {
    let resp = null;
    try {
      resp = await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 45000 });
    } catch (e) {
      throw new Error(`NAV_FAILED: ${label} へ遷移できず (${String(e.message).split('\n')[0]})`);
    }
    await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
    await page.waitForTimeout(1200);

    if (/authclient\/login/.test(page.url())) {
      if (attempt === 1) throw new Error(`AUTH_FAILED: 再ログイン後も ${label} でログイン画面に戻される`);
      console.log('  [aupay-login] セッション切れ → ID/PW自動再ログイン');
      await doAutoLogin(page);
      continue; // 再遷移
    }
    if (resp && resp.status() >= 400) {
      throw new Error(`NAV_FAILED: ${label} が HTTP ${resp.status()}`);
    }
    const ev = await checkLoggedInEvidence(page);
    if (!ev.ok) throw new Error(`AUTH_VERIFY: ${label} で認証済み証拠を確認できず (${ev.reason})`);
    return page;
  }
  throw new Error(`AUTH_FAILED: ${label} へ到達できず`); // unreachable
}

/** セッション健全性チェック: 管理画面topで肯定証拠を検証 (切れていたら自動再ログイン) */
export async function ensureWowmaLogin(page) {
  await gotoWowmaPage(page, TOP_URL, 'WOW!マネージャーtop');
  console.log('[aupay-login] セッション有効 (肯定証拠OK)');
}

/**
 * SSO経由の外部広告コンソールへ遷移する (プラチナマッチ等)。
 * expectHost の画面に到達し expectText が出ていることを検証 (fail-closed)。
 */
export async function gotoSsoConsole(page, ssoPath, targetUrl, { expectHost, expectText, label }) {
  const ssoUrl = `${WOWMA_BASE}${ssoPath}?target=${encodeURIComponent(targetUrl)}`;
  try {
    await page.goto(ssoUrl, { waitUntil: 'domcontentloaded', timeout: 45000 });
  } catch (e) {
    throw new Error(`NAV_FAILED: ${label} SSO遷移に失敗 (${String(e.message).split('\n')[0]})`);
  }
  await page.waitForLoadState('networkidle', { timeout: 25000 }).catch(() => {});
  await page.waitForTimeout(1500);
  if (/authclient\/login/.test(page.url())) {
    // wmshopclient セッション切れ → 再ログインして SSO をやり直し (1回だけ)
    console.log('  [aupay-login] セッション切れ (SSO前) → ID/PW自動再ログイン');
    await doAutoLogin(page);
    await page.goto(ssoUrl, { waitUntil: 'domcontentloaded', timeout: 45000 });
    await page.waitForLoadState('networkidle', { timeout: 25000 }).catch(() => {});
    await page.waitForTimeout(1500);
  }
  const host = safeHost(page.url());
  if (host !== expectHost) {
    throw new Error(`AUTH_VERIFY: ${label} SSO後のhostが想定外 (${host} != ${expectHost})`);
  }
  const body = (await page.locator('body').innerText({ timeout: 10000 }).catch(() => '')).replace(/\s+/g, ' ');
  if (!body || (expectText && !body.includes(expectText))) {
    throw new Error(`AUTH_VERIFY: ${label} の画面証拠 (「${expectText}」) を確認できず`);
  }
  return page;
}
