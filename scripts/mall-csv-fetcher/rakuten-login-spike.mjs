/**
 * P0 スパイク: 楽天RMS 3段階ログインの自動化可否検証
 *
 * 目的 (要件定義 §6 P0-1):
 *   - 2026年1月に3段階化したRMSログイン (R-Login → 楽天会員ID → 楽天会員PW) を
 *     Playwrightで通過できるか確認する
 *   - CAPTCHA / SMS / メール等の追加認証が挟まるかを実地で確認する
 *   - ログイン後、RPPパフォーマンスレポートのDL画面まで到達できるかを確認する
 *
 * このスクリプトは「検証」が目的。ダウンロードの本実装ではない。
 * 各ステップでスクリーンショットとURL/タイトルを記録し、どこで詰まるかを可視化する。
 *
 * ⚠️ 認証情報はコードに書かない。すべて .env から読む (secret不触の原則)。
 * ⚠️ 初回実行・認証情報投入は中原さんが行う。Claudeは雛形と手順まで。
 *
 * 実行:
 *   1) このディレクトリの README.md の手順で Playwright をインストール
 *   2) .env.example を .env にコピーし、店舗運用専用IDの認証情報を記入
 *   3) node scripts/mall-csv-fetcher/rakuten-login-spike.mjs
 *
 * まずは HEADLESS=0 (ブラウザ表示) で流し、どの画面で何を聞かれるか目視すること。
 */

import { chromium } from 'playwright';
import { config as loadEnv } from 'dotenv';
import { mkdir } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

loadEnv();

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUT_DIR = join(__dirname, 'spike-output');

const {
  RMS_RLOGIN_ID,
  RMS_RLOGIN_PW,
  RMS_MEMBER_ID,
  RMS_MEMBER_PW,
  HEADLESS = '0', // 既定でブラウザ表示。検証はまず目視で
} = process.env;

const RMS_LOGIN_URL = 'https://glogin.rms.rakuten.co.jp/';

function requireEnv() {
  const missing = ['RMS_RLOGIN_ID', 'RMS_RLOGIN_PW', 'RMS_MEMBER_ID', 'RMS_MEMBER_PW']
    .filter((k) => !process.env[k]);
  if (missing.length) {
    console.error(`[FATAL] 認証情報が未設定です: ${missing.join(', ')}`);
    console.error('  → .env.example を .env にコピーして記入してください (中原さん作業)');
    process.exit(2);
  }
}

async function snap(page, label) {
  const path = join(OUT_DIR, `${label}.png`);
  await page.screenshot({ path, fullPage: true }).catch(() => {});
  console.log(`  [snap] ${label}  url=${page.url()}  title=${await page.title().catch(() => '?')}`);
}

/**
 * 画面上に「それらしい」入力欄が現れるまで待ち、値を入れて次へ進む汎用ヘルパー。
 * RMSのDOMは変わりうるので、複数のセレクタ候補を順に試す。
 * P0の目的は「どのセレクタが効くか」を突き止めることなので、失敗しても落とさず記録する。
 */
async function tryFill(page, selectorCandidates, value, fieldLabel) {
  for (const sel of selectorCandidates) {
    const el = page.locator(sel).first();
    if (await el.count().catch(() => 0)) {
      try {
        await el.fill(value, { timeout: 5000 });
        console.log(`  [fill] ${fieldLabel}: セレクタ "${sel}" にヒット`);
        return sel;
      } catch { /* 次の候補へ */ }
    }
  }
  console.warn(`  [warn] ${fieldLabel}: 既知セレクタ候補すべて不発。spike-output のスクショでDOMを確認して候補を追加すること`);
  return null;
}

async function tryClick(page, selectorCandidates, btnLabel) {
  for (const sel of selectorCandidates) {
    const el = page.locator(sel).first();
    if (await el.count().catch(() => 0)) {
      try {
        await el.click({ timeout: 5000 });
        console.log(`  [click] ${btnLabel}: セレクタ "${sel}" にヒット`);
        return sel;
      } catch { /* 次の候補へ */ }
    }
  }
  console.warn(`  [warn] ${btnLabel}: 押下ボタン候補すべて不発`);
  return null;
}

async function main() {
  requireEnv();
  await mkdir(OUT_DIR, { recursive: true });

  console.log('=== 楽天RMS 3段階ログイン P0スパイク ===');
  console.log(`HEADLESS=${HEADLESS} (0=ブラウザ表示 / 1=ヘッドレス)`);
  console.log('目的: 各段階の通過可否と追加認証(CAPTCHA/SMS/メール)の有無を確認\n');

  const browser = await chromium.launch({
    headless: HEADLESS === '1',
    slowMo: HEADLESS === '1' ? 0 : 300, // 目視しやすいよう少し遅く
  });
  const context = await browser.newContext({
    locale: 'ja-JP',
    viewport: { width: 1280, height: 1000 },
  });
  const page = await context.newPage();

  try {
    // --- Step 0: ログイン入口 ---
    console.log('[Step 0] ログイン画面を開く');
    await page.goto(RMS_LOGIN_URL, { waitUntil: 'domcontentloaded', timeout: 30000 });
    await snap(page, '0_login_entry');

    // --- Step 1: R-Login ID / PW ---
    console.log('[Step 1] R-Login 認証');
    await tryFill(page, ['input[name="login_id"]', 'input#login_id', 'input[type="text"]'], RMS_RLOGIN_ID, 'R-Login ID');
    await tryFill(page, ['input[name="passwd"]', 'input#passwd', 'input[type="password"]'], RMS_RLOGIN_PW, 'R-Login PW');
    await snap(page, '1a_rlogin_filled');
    await tryClick(page, ['button[type="submit"]', 'input[type="submit"]', 'text=ログイン'], 'R-Loginログイン');
    await page.waitForLoadState('networkidle', { timeout: 30000 }).catch(() => {});
    await snap(page, '1b_after_rlogin');

    // --- Step 2: 楽天会員ID ---
    console.log('[Step 2] 楽天会員ID');
    await tryFill(page, ['input[name="username"]', 'input#loginInner_u', 'input[type="email"]', 'input[type="text"]'], RMS_MEMBER_ID, '楽天会員ID');
    await snap(page, '2a_member_id_filled');
    await tryClick(page, ['button[type="submit"]', 'text=次へ', 'input[type="submit"]'], '会員ID次へ');
    await page.waitForLoadState('networkidle', { timeout: 30000 }).catch(() => {});
    await snap(page, '2b_after_member_id');

    // --- Step 3: 楽天会員PW ---
    console.log('[Step 3] 楽天会員PW');
    await tryFill(page, ['input[name="password"]', 'input#loginInner_p', 'input[type="password"]'], RMS_MEMBER_PW, '楽天会員PW');
    await snap(page, '3a_member_pw_filled');
    await tryClick(page, ['button[type="submit"]', 'text=ログイン', 'input[type="submit"]'], '会員PWログイン');
    await page.waitForLoadState('networkidle', { timeout: 30000 }).catch(() => {});
    await snap(page, '3b_after_login');

    // --- 判定: RMSトップに到達したか / 追加認証が出ているか ---
    console.log('\n[判定] ログイン後の状態を確認');
    const url = page.url();
    const bodyText = await page.locator('body').innerText().catch(() => '');
    const suspicious = ['認証', 'ワンタイム', 'SMS', 'コード', 'captcha', 'CAPTCHA', '確認コード', '本人確認'];
    const hits = suspicious.filter((w) => bodyText.includes(w));

    console.log(`  最終URL: ${url}`);
    if (hits.length) {
      console.log(`  ⚠️ 追加認証らしきキーワードを検出: ${hits.join(', ')}`);
      console.log('  → 追加認証が挟まる = 完全無人化は困難。要件定義 §7 リスク表に反映すること');
    } else if (/rms\.rakuten\.co\.jp/.test(url) && !/glogin/.test(url)) {
      console.log('  ✅ RMS管理画面ドメインに到達 (追加認証なしの可能性)');
      console.log('  → 次: RPPパフォーマンスレポート画面のURL/DL動線を rakuten-rpp-download.mjs で実装');
    } else {
      console.log('  ❓ 判定不能。spike-output のスクショで最終状態を目視確認すること');
    }
    await snap(page, '4_final_state');

    console.log(`\n完了。スクリーンショットは ${OUT_DIR} を参照。`);
  } catch (err) {
    console.error('[ERROR] スパイク中に例外:', err.message);
    await snap(page, 'error_state');
    process.exitCode = 1;
  } finally {
    if (HEADLESS !== '1') {
      console.log('\nブラウザは開いたままにします (目視確認用)。確認後このプロセスをCtrl+Cで終了してください。');
      // 目視確認のため待機。ヘッドレス時は即閉じる。
      await new Promise((r) => setTimeout(r, 120000));
    }
    await browser.close();
  }
}

main();
