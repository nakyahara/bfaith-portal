/**
 * 楽天RMS ログイン共通モジュール (P0スパイクで確立した手順を部品化)
 *
 * 提供する関数:
 *   - openContext()            : 信頼端末プロファイルでブラウザを開く
 *   - loginAndGoto(page, url)  : RMSにログイン済みにして目的URLへ。既にログイン済みなら素通り
 *   - tryFill / tryClick       : 要素が可視になるまで待って操作する汎用ヘルパー
 *   - dumpControls / dumpForm  : 画面のDOMを診断出力
 *
 * ⚠️ 2段階認証が要求されたら Error('2FA_REQUIRED: ...') を投げる。
 *    呼び出し側はこれを捕まえてGChat通知+手動再セットアップ案内に回すこと。
 */

import { chromium } from 'playwright';
import { config as loadEnv } from 'dotenv';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const __dirname = dirname(fileURLToPath(import.meta.url));
loadEnv({ path: join(__dirname, '.env') });

export const PROFILE_DIR = join(__dirname, '.profile-rakuten');
const RMS_LOGIN_URL = 'https://glogin.rms.rakuten.co.jp/';

const {
  RMS_RLOGIN_ID, RMS_RLOGIN_PW, RMS_MEMBER_ID, RMS_MEMBER_PW,
  HEADLESS = '0',
} = process.env;

export function safeHost(url) {
  try { return new URL(url).hostname; } catch { return ''; }
}

export async function openContext() {
  return chromium.launchPersistentContext(PROFILE_DIR, {
    headless: HEADLESS === '1',
    slowMo: HEADLESS === '1' ? 0 : 150,
    locale: 'ja-JP',
    viewport: { width: 1280, height: 1000 },
    acceptDownloads: true, // ★レポートZIPを受け取るため必須
  });
}

export async function tryFill(page, cands, value, label, timeoutMs = 20000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    for (const sel of cands) {
      const el = page.locator(sel).first();
      try {
        if (await el.isVisible().catch(() => false)) {
          await el.fill(value, { timeout: 5000 });
          console.log(`  [fill] ${label}: "${sel}"`);
          return sel;
        }
      } catch { /* 次候補 */ }
    }
    await page.waitForTimeout(400);
  }
  console.warn(`  [warn] ${label}: 入力欄が現れず (timeout)`);
  return null;
}

export async function tryClick(page, cands, label, timeoutMs = 20000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    for (const sel of cands) {
      const el = page.locator(sel).first();
      try {
        if (await el.isVisible().catch(() => false)) {
          await el.click({ timeout: 5000 });
          console.log(`  [click] ${label}: "${sel}"`);
          return sel;
        }
      } catch { /* 次候補 */ }
    }
    await page.waitForTimeout(400);
  }
  console.warn(`  [warn] ${label}: ボタンが現れず (timeout)`);
  return null;
}

/** 画面の入力欄・ボタンを一覧出力 (セレクタ特定用) */
export async function dumpControls(page, label) {
  const info = await page.evaluate(() => {
    const vis = (el) => !!(el.offsetParent || el.getClientRects().length);
    const inputs = [...document.querySelectorAll('input')].filter(vis).map((el) => ({
      name: el.name || '', id: el.id || '', type: el.type || '', value: el.value || '',
      checked: el.checked, placeholder: el.placeholder || '',
    }));
    const buttons = [...document.querySelectorAll('button, input[type=submit], input[type=button], a[role=button]')]
      .filter(vis).map((el) => ({ text: (el.innerText || el.value || '').trim(), disabled: el.disabled || false }));
    const selects = [...document.querySelectorAll('select')].filter(vis).map((el) => ({
      name: el.name || '', id: el.id || '',
      options: [...el.options].map((o) => ({ value: o.value, text: o.text.trim() })),
    }));
    return { inputs, buttons, selects };
  }).catch(() => ({ inputs: [], buttons: [], selects: [] }));
  console.log(`  [DOM:${label}] inputs=${JSON.stringify(info.inputs)}`);
  console.log(`  [DOM:${label}] selects=${JSON.stringify(info.selects)}`);
  console.log(`  [DOM:${label}] buttons=${JSON.stringify(info.buttons)}`);
  return info;
}

async function gotoSafe(page, url) {
  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 }).catch(() => {});
}

/** 「お気をつけください」等のお知らせ確認ページ(glogin上, 次へボタン)を通過する */
async function passNotice(page) {
  for (let i = 0; i < 3; i++) {
    const host = safeHost(page.url());
    if (host !== 'glogin.rms.rakuten.co.jp') break;
    const next = page.locator('text=次へ').first();
    if (!(await next.isVisible().catch(() => false))) break;
    console.log('  [notice] お知らせページ「次へ」を通過');
    await next.click().catch(() => {});
    await page.waitForLoadState('domcontentloaded').catch(() => {});
    await page.waitForTimeout(1000);
  }
}

/** R-Login → 楽天会員 → RMS のログイン列を実行。2FA要求時は throw。 */
async function runLogin(page) {
  console.log('[login] RMSログイン開始');
  await gotoSafe(page, RMS_LOGIN_URL);

  // R-Login (共通ID版=ボタン「楽天会員ログインへ」/旧版=「ログイン」の両対応)
  await tryFill(page, ['input[name="login_id"]', 'input#rlogin-username-ja', 'input[type="text"]:visible'], RMS_RLOGIN_ID, 'R-Login ID');
  await tryFill(page, ['input[name="passwd"]', 'input#rlogin-password-ja', 'input[type="password"]:visible'], RMS_RLOGIN_PW, 'R-Login PW');
  await tryClick(page, ['text=楽天会員ログインへ', 'text=ログイン', 'button[type="submit"]'], 'R-Login送信');
  await page.waitForLoadState('networkidle', { timeout: 30000 }).catch(() => {});

  // 楽天会員ID (プロファイル記憶時は欄が出ないので短めタイムアウトで素通り)
  await tryFill(page, ['input[name="username"]', 'input[type="email"]', 'input[type="text"]:visible'], RMS_MEMBER_ID, '楽天会員ID', 8000);
  await tryClick(page, ['text=次へ', 'button:has-text("次へ")'], '会員ID次へ', 5000);

  // 楽天会員PW
  const pwFilled = await tryFill(page, ['input[type="password"]:visible', 'input[name="password"]'], RMS_MEMBER_PW, '楽天会員PW', 15000);
  if (pwFilled) {
    const clicked = await tryClick(page, ['text=次へ', 'button[type="submit"]'], '会員PW送信', 8000);
    if (!clicked) await page.locator('input[type="password"]:visible').first().press('Enter').catch(() => {});
  }

  // 楽天ログインSPAから離脱するのを待つ
  await page.waitForURL((u) => u.hostname !== 'login.account.rakuten.com', { timeout: 30000 }).catch(() => {});
  await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});

  // 2FA検知
  if (safeHost(page.url()) === 'login.account.rakuten.com') {
    const body = await page.locator('body').innerText().catch(() => '');
    if (/2段階認証|ワンタイム|認証コード|本人確認|確認コード/.test(body)) {
      throw new Error('2FA_REQUIRED: 楽天が2段階認証を要求。$env:MANUAL=1 で信頼できる端末を再登録してください');
    }
  }
  await passNotice(page);
  console.log(`[login] 完了 host=${safeHost(page.url())}`);
}

/**
 * RMSにログイン済みにして targetUrl を開く。
 * 既にセッションが生きていれば直接遷移し、ログインが必要ならログイン列を実行する。
 */
export async function loginAndGoto(page, targetUrl) {
  const targetHost = safeHost(targetUrl);
  await gotoSafe(page, targetUrl);
  await passNotice(page);

  if (safeHost(page.url()) === targetHost) {
    console.log(`[login] セッション有効。${targetHost} に直接到達`);
    return;
  }

  await runLogin(page);
  await gotoSafe(page, targetUrl);
  await passNotice(page);

  if (safeHost(page.url()) !== targetHost) {
    throw new Error(`ログイン後も目的ページに到達できず: ${page.url()}`);
  }
  console.log(`[login] ${targetHost} に到達`);
}
