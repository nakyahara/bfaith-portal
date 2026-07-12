/**
 * P0-A スパイク: au PAYマーケット WOW!マネージャー ログインの自動化可否検証
 *
 * 実測 (2026-07-12): ログインは https://manager.wowma.jp/wmshopclient/authclient/login の
 * 素直なフォーム (#loginId=メールアドレス / #password / #btnLogin)。2FA/CAPTCHAの気配なし
 * (実ログインで最終確認する)。楽天/Yahoo!と同じ永続プロファイル (.profile-aupay) を使い、
 * セッション再利用 + 切れたらID/PW自動ログイン、を狙う。
 *
 * ⚠️ 認証情報はコードに書かない。WOWMA_LOGIN_ID / WOWMA_LOGIN_PW を .env から読む
 *    (miniPCはリポジトリ直下 .env でもOK。記入は中原さん)。
 *
 * 実行:
 *   node scripts/mall-csv-fetcher/aupay-login-spike.mjs            # 自動ログイン検証
 *   $env:MANUAL=1; node scripts/mall-csv-fetcher/aupay-login-spike.mjs  # 手動 (2FA等が出た場合)
 *   SPIKE_URL=... で到達後にそのURLへ遷移してDOMダンプ (画面調査用)
 */

import { chromium } from 'playwright';
import { config as loadEnv } from 'dotenv';
import { mkdir } from 'node:fs/promises';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUT_DIR = join(__dirname, 'spike-output');
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

const { WOWMA_LOGIN_ID, WOWMA_LOGIN_PW, HEADLESS = '0', MANUAL = '0' } = process.env;

const LOGIN_URL = 'https://manager.wowma.jp/wmshopclient/authclient/login';
const TOP_URL = 'https://manager.wowma.jp/wmshopclient/';
export const PROFILE_DIR = join(__dirname, '.profile-aupay');

const host = (u) => { try { return new URL(u).hostname; } catch { return ''; } };

async function snap(page, label) {
  await page.screenshot({ path: join(OUT_DIR, `aupay_${label}.png`), fullPage: true }).catch(() => {});
  console.log(`  [snap] aupay_${label}  url=${page.url()}  title=${await page.title().catch(() => '?')}`);
}

async function dumpControls(page, label) {
  const info = await page.evaluate(() => {
    const vis = (el) => !!(el.offsetParent || el.getClientRects().length);
    const inputs = [...document.querySelectorAll('input, select')].filter(vis).map((el) => ({
      tag: el.tagName.toLowerCase(), name: el.name || '', id: el.id || '', type: el.type || '',
    }));
    const buttons = [...document.querySelectorAll('button, input[type=submit], input[type=button]')]
      .filter(vis).map((el) => (el.innerText || el.value || '').trim().replace(/\s+/g, ' ').slice(0, 26)).filter(Boolean);
    return { inputs, buttons };
  }).catch(() => ({ inputs: [], buttons: [] }));
  console.log(`  [DOM:${label}] inputs=${JSON.stringify(info.inputs.slice(0, 20))}`);
  console.log(`  [DOM:${label}] buttons=${JSON.stringify(info.buttons.slice(0, 25))}`);
}

async function dumpLinks(page, keywords, label) {
  const links = await page.evaluate((kws) => {
    return [...document.querySelectorAll('a')]
      .map((a) => ({
        text: (a.innerText || a.textContent || '').trim().replace(/\s+/g, ' ').slice(0, 40),
        href: a.href || '',
        hidden: !(a.offsetParent || a.getClientRects().length),
      }))
      .filter((l) => l.text && l.href && kws.some((k) => l.text.includes(k)));
  }, keywords).catch(() => []);
  console.log(`  [LINKS:${label}] ${JSON.stringify(links, null, 1)}`);
}

/** ログイン済みの肯定証拠 (fail-closed。Yahoo P1-Yで確立したCodex方針) */
async function looksLoggedIn(page) {
  if (host(page.url()) !== 'manager.wowma.jp') return false;
  if (/authclient\/login/.test(page.url())) return false;
  let body = '';
  try { body = await page.locator('body').innerText({ timeout: 10000 }); } catch { return false; }
  const compact = body.replace(/\s+/g, ' ');
  if (/再度ログインしてください|ログインID\(メールアドレス\)/.test(compact)) return false;
  // 肯定証拠: ログアウト導線 or 管理メニュー語彙
  return /ログアウト/.test(compact) || (/受注/.test(compact) && /商品/.test(compact));
}

async function runManualSetup(context) {
  const page = context.pages()[0] || await context.newPage();
  console.log('=== 手動セットアップモード (WOW!マネージャー) ===');
  console.log('開いたブラウザでログインし、管理画面トップまで入れたら閉じてください。\n');
  await page.goto(LOGIN_URL, { waitUntil: 'domcontentloaded', timeout: 45000 }).catch(() => {});
  await page.waitForTimeout(15 * 60 * 1000).catch(() => {});
}

async function main() {
  await mkdir(OUT_DIR, { recursive: true });
  const context = await chromium.launchPersistentContext(PROFILE_DIR, {
    headless: MANUAL === '1' ? false : HEADLESS === '1',
    slowMo: HEADLESS === '1' ? 0 : 300,
    locale: 'ja-JP',
    viewport: { width: 1400, height: 1000 },
    acceptDownloads: true,
  });

  if (MANUAL === '1') {
    try { await runManualSetup(context); } finally { await context.close(); }
    return;
  }

  console.log('=== au PAYマーケット WOW!マネージャー ログインスパイク ===');
  const page = context.pages()[0] || await context.newPage();

  try {
    console.log('[Step 0] 管理画面トップへ (セッション再利用の確認)');
    await page.goto(TOP_URL, { waitUntil: 'domcontentloaded', timeout: 45000 }).catch(() => {});
    await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
    await page.waitForTimeout(2000);
    await snap(page, '0_entry');

    if (await looksLoggedIn(page)) {
      console.log('  ✅ セッション有効 (プロファイル再利用でログイン済み)');
    } else {
      console.log('[Step 1] 未ログイン → ID/PW自動ログインを試行');
      if (!WOWMA_LOGIN_ID || !WOWMA_LOGIN_PW) {
        console.log('  ⚠️ WOWMA_LOGIN_ID / WOWMA_LOGIN_PW 未設定。.env に記入してください (中原さん)');
        return;
      }
      await page.goto(LOGIN_URL, { waitUntil: 'domcontentloaded', timeout: 45000 }).catch(() => {});
      await page.waitForTimeout(1500);
      await dumpControls(page, 'login');
      await page.locator('#loginId').fill(WOWMA_LOGIN_ID, { timeout: 10000 });
      await page.locator('#password').fill(WOWMA_LOGIN_PW, { timeout: 10000 });
      await snap(page, '1a_filled');
      await page.locator('#btnLogin').click({ timeout: 10000 });
      await page.waitForLoadState('networkidle', { timeout: 30000 }).catch(() => {});
      await page.waitForTimeout(3000);
      await snap(page, '1b_after_login');
    }

    console.log('\n[判定]');
    console.log(`  最終URL: ${page.url()}`);
    const body = await page.locator('body').innerText().catch(() => '');
    const twoFa = ['ワンタイム', '認証コード', '確認コード', 'SMS', '画像認証', 'CAPTCHA'].filter((w) => body.includes(w));
    if (await looksLoggedIn(page)) {
      console.log('  ✅ WOW!マネージャーにログイン成功');
      const target = (process.env.SPIKE_URL || '').trim();
      if (target) {
        console.log(`\n[調査] ${target} へ遷移`);
        await page.goto(target, { waitUntil: 'domcontentloaded', timeout: 45000 }).catch(() => {});
        await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
        await page.waitForTimeout(3000);
        console.log(`  [arrived] ${page.url()}`);
        await dumpControls(page, 'target');
        await snap(page, 'target');
        const bodyT = await page.locator('body').innerText().catch(() => '');
        console.log(`  [body] ${bodyT.replace(/\s+/g, ' ').slice(0, 900)}`);
      }
      await dumpLinks(page, ['分析', 'アクセス', 'レポート', 'CSV', 'ダウンロード', '統計', 'ランキング', 'レビュー', '検索', '販促', '広告', '集計'], 'nav');
    } else if (twoFa.length) {
      console.log(`  ⚠️ 追加認証を要求: ${twoFa.join(', ')} → MANUAL=1 で手動ログインしてセッションを焼いてください`);
    } else {
      console.log('  ❓ ログイン未確立。スクショとDOMを確認');
      await dumpControls(page, 'final');
    }
    await snap(page, 'final');
  } catch (e) {
    console.error('[ERROR]', e.message);
    await snap(page, 'error');
    process.exitCode = 1;
  } finally {
    if (HEADLESS !== '1') {
      console.log('\n目視用にブラウザを120秒開いたままにします。');
      await page.waitForTimeout(120000).catch(() => {});
    }
    await context.close().catch(() => {});
  }
}

main();
