/**
 * P0-Y スパイク: Yahoo!ショッピング ストアクリエイターPro ログインの自動化可否検証
 *
 * 目的 (要件定義 §6 P0-2):
 *   - ストアクリエイターPro のログインを Playwright で通過できるか確認する
 *   - ⚠️Yahoo はデフォルトで SMS 2段階認証。どの認証 (SMS確認コード / パスワード /
 *     パスキー) が要求されるかを実地確認する
 *   - 固定プロファイル (.profile-yahoo) でセッションがどの程度持続するかを検証する
 *     (楽天の「信頼できる端末」相当の運用が可能か)
 *
 * 運用方針 (楽天P0と同じ):
 *   1. まず MANUAL=1 で中原さんが手動ログイン (SMSコード入力) → プロファイルにセッション保存
 *      ※「ログインしたままにする」系のチェックがあれば必ずON
 *   2. 以後 MANUAL=0 (自動) でセッション再利用できるかを検証。切れた時に何を要求されるかを記録
 *
 * ⚠️ 認証情報はコードに書かない。YAHOO_BIZ_ID / YAHOO_BIZ_PW を .env から読む (記入は中原さん)。
 *
 * 実行:
 *   $env:MANUAL=1; node scripts/mall-csv-fetcher/yahoo-login-spike.mjs   # 初回手動セットアップ
 *   node scripts/mall-csv-fetcher/yahoo-login-spike.mjs                  # セッション再利用検証
 *   SPIKE_URL=... を指定するとログイン確認後にそのURLへ遷移してDOMをダンプ (画面調査用)
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
  const missing = ['YAHOO_BIZ_ID', 'YAHOO_BIZ_PW'].filter((k) => !process.env[k]);
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

const { YAHOO_BIZ_ID, YAHOO_BIZ_PW, HEADLESS = '0', MANUAL = '0' } = process.env;

// ストアクリエイターPro 入口。未ログインなら login.yahoo.co.jp へリダイレクトされる想定
const STORE_PRO_URL = 'https://editor.store.yahoo.co.jp/';
export const PROFILE_DIR = join(__dirname, '.profile-yahoo');

const host = (u) => { try { return new URL(u).hostname; } catch { return ''; } };

async function snap(page, label) {
  await page.screenshot({ path: join(OUT_DIR, `yahoo_${label}.png`), fullPage: true }).catch(() => {});
  console.log(`  [snap] yahoo_${label}  url=${page.url()}  title=${await page.title().catch(() => '?')}`);
}

async function dumpControls(page, label) {
  const info = await page.evaluate(() => {
    const vis = (el) => !!(el.offsetParent || el.getClientRects().length);
    const inputs = [...document.querySelectorAll('input')].filter(vis).map((el) => ({
      name: el.name || '', id: el.id || '', type: el.type || '', placeholder: el.placeholder || '',
      autocomplete: el.autocomplete || '',
    }));
    const buttons = [...document.querySelectorAll('button, input[type=submit], a[role=button]')]
      .filter(vis).map((el) => (el.innerText || el.value || '').trim()).filter(Boolean);
    return { inputs, buttons };
  }).catch(() => ({ inputs: [], buttons: [] }));
  console.log(`  [DOM:${label}] 入力欄=${JSON.stringify(info.inputs)}`);
  console.log(`  [DOM:${label}] ボタン=${JSON.stringify(info.buttons)}`);
}

async function dumpLinks(page, keywords, label) {
  const links = await page.evaluate((kws) => {
    return [...document.querySelectorAll('a')]
      .map((a) => ({ text: (a.innerText || a.textContent || '').trim().replace(/\s+/g, ' ').slice(0, 40), href: a.href || '' }))
      .filter((l) => l.text && kws.some((k) => l.text.includes(k)));
  }, keywords).catch(() => []);
  console.log(`  [LINKS:${label}] ${JSON.stringify(links, null, 1)}`);
}

async function tryFill(page, cands, value, label, timeoutMs = 15000) {
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
    await page.waitForTimeout(500);
  }
  console.warn(`  [warn] ${label}: 入力欄が現れず`);
  return null;
}

async function tryClick(page, cands, label, timeoutMs = 15000) {
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
    await page.waitForTimeout(500);
  }
  console.warn(`  [warn] ${label}: ボタンが現れず`);
  return null;
}

/** ストアクリエイターProに入れているか (login.yahoo.co.jp に飛ばされていないか) */
async function looksLoggedIn(page) {
  const h = host(page.url());
  if (h.includes('login.yahoo')) return false;
  if (!h.endsWith('store.yahoo.co.jp') && !h.endsWith('yahoo.co.jp')) return false;
  const body = await page.locator('body').innerText().catch(() => '');
  if (/ログインが必要|再度ログイン|セッションが切れ/.test(body)) return false;
  return h.endsWith('store.yahoo.co.jp');
}

async function runManualSetup(context) {
  const page = context.pages()[0] || await context.newPage();
  console.log('=== 手動セットアップモード (Yahoo! ストアクリエイターPro) ===');
  console.log('手順 (中原さん):');
  console.log('  1. 開いたブラウザでストア用Yahoo! JAPAN ID (ビジネスID) でログイン');
  console.log('  2. SMS確認コード等の認証を完了');
  console.log('  3. 「ログインしたままにする」系のチェックがあれば必ずON');
  console.log('  4. ストアクリエイターProのトップまで入れたら閉じてOK (セッションがプロファイルに保存される)');
  console.log('  完了後、MANUAL なしで再実行するとセッション再利用を検証できます\n');
  await page.goto(STORE_PRO_URL, { waitUntil: 'domcontentloaded', timeout: 45000 }).catch(() => {});
  console.log('ブラウザを最大15分開いたままにします。ログインが済んだら閉じてください。');
  await page.waitForTimeout(15 * 60 * 1000).catch(() => {});
}

async function main() {
  await mkdir(OUT_DIR, { recursive: true });
  const context = await chromium.launchPersistentContext(PROFILE_DIR, {
    headless: MANUAL === '1' ? false : HEADLESS === '1',
    slowMo: HEADLESS === '1' ? 0 : 300,
    locale: 'ja-JP',
    viewport: { width: 1280, height: 1000 },
    acceptDownloads: true,
  });

  if (MANUAL === '1') {
    try { await runManualSetup(context); } finally { await context.close(); }
    return;
  }

  console.log('=== Yahoo! ストアクリエイターPro ログインスパイク ===');
  console.log(`HEADLESS=${HEADLESS} / プロファイル=${PROFILE_DIR}\n`);
  const page = context.pages()[0] || await context.newPage();

  try {
    console.log('[Step 0] ストアクリエイターPro 入口');
    await page.goto(STORE_PRO_URL, { waitUntil: 'domcontentloaded', timeout: 45000 }).catch(() => {});
    await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
    await page.waitForTimeout(2000);
    await snap(page, '0_entry');

    if (await looksLoggedIn(page)) {
      console.log('  ✅ セッション有効 (プロファイル再利用でログイン済み)');
    } else if (host(page.url()).includes('login.yahoo')) {
      console.log('[Step 1] Yahoo!ログイン画面に遷移。自動入力を試行');
      await dumpControls(page, 'login1');
      if (!YAHOO_BIZ_ID) {
        console.log('  ⚠️ YAHOO_BIZ_ID 未設定。ここから先は .env 記入後に再実行 (または MANUAL=1 で手動ログイン)');
        await snap(page, '1_login_no_creds');
        return;
      }
      await tryFill(page, ['input#login_handle', 'input[name="handle"]', 'input[type="text"]:visible', 'input[autocomplete="username"]'], YAHOO_BIZ_ID, 'Yahoo ID');
      await snap(page, '1a_id_filled');
      await tryClick(page, ['button:has-text("次へ")', 'button[type="submit"]', 'text=次へ'], 'ID次へ');
      await page.waitForTimeout(3000);
      await dumpControls(page, 'login2');
      await snap(page, '1b_after_id');

      // パスワード欄が出ればパスワード認証、出なければSMS等
      const pwFilled = await tryFill(page, ['input[type="password"]:visible', 'input[name="password"]'], YAHOO_BIZ_PW || '', 'パスワード', 8000);
      if (pwFilled) {
        await tryClick(page, ['button:has-text("次へ")', 'button:has-text("ログイン")', 'button[type="submit"]'], 'PW送信');
        await page.waitForTimeout(4000);
      }
      await snap(page, '1c_after_pw');
      await dumpControls(page, 'login3');
    } else {
      console.log(`  ❓ 想定外の着地: ${page.url()}`);
    }

    // 判定
    const body = await page.locator('body').innerText().catch(() => '');
    const smsHits = ['確認コード', 'SMS', 'ショートメッセージ', '認証コード', 'ワンタイム'].filter((w) => body.includes(w));
    console.log('\n[判定]');
    console.log(`  最終URL: ${page.url()}`);
    if (await looksLoggedIn(page)) {
      console.log('  ✅ ストアクリエイターProにログイン済み');
      // ログイン済みなら画面調査: 統計/広告系の導線をダンプ
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
        console.log(`  [body] ${bodyT.replace(/\s+/g, ' ').slice(0, 800)}`);
      }
      await dumpLinks(page, ['統計', '分析', 'アクセス', '広告', 'アイテムマッチ', 'ストアマッチ', 'CSV', 'ダウンロード', 'レポート', '販促'], 'nav');
    } else if (smsHits.length) {
      console.log(`  ⚠️ 2FA_REQUIRED: 追加認証を要求 (${smsHits.join(', ')})`);
      console.log('  → MANUAL=1 で中原さんが手動ログイン (SMSコード入力) してセッションを焼く:');
      console.log('     $env:MANUAL=1; node scripts/mall-csv-fetcher/yahoo-login-spike.mjs');
    } else {
      console.log('  ❓ ログイン未確立。スクショとDOMダンプを確認');
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
    await context.close();
  }
}

main();
