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

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUT_DIR = join(__dirname, 'spike-output');

// .env はこのスクリプトと同じディレクトリから読む (どこから node を実行しても動くように)。
// 既定の loadEnv() は process.cwd() を見るため、リポジトリルートから実行するとscripts配下の.envを読めない。
loadEnv({ path: join(__dirname, '.env') });

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
 * 入力欄が「表示される」まで待ってから値を入れる汎用ヘルパー。
 * 楽天会員ID/PW画面 (login.account.rakuten.com) はSPAで、URLを変えずに
 * ページ内部だけ切り替わる。load完了を待っても欄がまだ描画されていないことがあるため、
 * セレクタが可視になるまでポーリングする (P0で判明したタイミング問題への対処)。
 */
async function tryFill(page, selectorCandidates, value, fieldLabel, timeoutMs = 20000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    for (const sel of selectorCandidates) {
      const el = page.locator(sel).first();
      try {
        if (await el.isVisible().catch(() => false)) {
          await el.fill(value, { timeout: 5000 });
          console.log(`  [fill] ${fieldLabel}: セレクタ "${sel}" にヒット`);
          return sel;
        }
      } catch { /* 次の候補へ */ }
    }
    await page.waitForTimeout(500);
  }
  console.warn(`  [warn] ${fieldLabel}: 制限時間内に入力欄が現れず。spike-output のスクショでDOMを確認し候補を追加すること`);
  return null;
}

async function tryClick(page, selectorCandidates, btnLabel, timeoutMs = 20000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    for (const sel of selectorCandidates) {
      const el = page.locator(sel).first();
      try {
        if (await el.isVisible().catch(() => false)) {
          await el.click({ timeout: 5000 });
          console.log(`  [click] ${btnLabel}: セレクタ "${sel}" にヒット`);
          return sel;
        }
      } catch { /* 次の候補へ */ }
    }
    await page.waitForTimeout(500);
  }
  console.warn(`  [warn] ${btnLabel}: 制限時間内にボタンが現れず`);
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

    // --- Step 2: 楽天会員ID (login.account.rakuten.com のSPA) ---
    console.log('[Step 2] 楽天会員ID');
    await tryFill(page, ['input[name="username"]', 'input[type="email"]', 'input[type="text"]:visible'], RMS_MEMBER_ID, '楽天会員ID');
    await snap(page, '2a_member_id_filled');
    // このSPAは「次へ」。ログイン系ボタンの表記ゆれも一応候補に入れる
    await tryClick(page, ['button:has-text("次へ")', 'button[type="submit"]', 'text=次へ'], '会員ID次へ');
    await snap(page, '2b_after_member_id');

    // --- Step 3: 楽天会員PW (SPAが #/sign_in/password へ内部遷移してから欄が現れる) ---
    console.log('[Step 3] 楽天会員PW');
    // ★P0で判明: 次へ押下直後は欄が未描画。tryFill が可視になるまで待つ (最大20秒)
    await tryFill(page, ['input[type="password"]:visible', 'input[name="password"]'], RMS_MEMBER_PW, '楽天会員PW');
    await snap(page, '3a_member_pw_filled');
    // ★P0で判明: Step2で効いた text=次へ を先頭に (button:has-text は空振りした)
    const clicked = await tryClick(page, ['text=次へ', 'button:has-text("次へ")', 'button[type="submit"]'], '会員PW送信', 10000);
    if (!clicked) {
      console.log('  [fallback] ボタン不発 → パスワード欄で Enter 送信を試行');
      await page.locator('input[type="password"]:visible').first().press('Enter').catch(() => {});
    }
    // ログイン確定 = 楽天ログインSPA(login.account.rakuten.com)から離脱するのを待つ
    await page.waitForURL((u) => u.hostname !== 'login.account.rakuten.com', { timeout: 30000 }).catch(() => {});
    await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
    await snap(page, '3b_after_login');

    // --- 判定: RMSに到達したか / 追加認証が出ているか ---
    console.log('\n[判定] ログイン後の状態を確認');
    const url = page.url();
    let hostname = '';
    try { hostname = new URL(url).hostname; } catch { /* 解析不能 */ }
    const bodyText = await page.locator('body').innerText().catch(() => '');
    const suspicious = ['ワンタイム', 'SMS', '確認コード', '本人確認', 'captcha', 'CAPTCHA', '認証コード'];
    const hits = suspicious.filter((w) => bodyText.includes(w));
    const onRakutenLogin = hostname === 'login.account.rakuten.com'; // ★redirect_uri内のrms文字列に釣られないようホスト名で判定
    const onRms = hostname.endsWith('rms.rakuten.co.jp');

    console.log(`  最終URL: ${url}`);
    console.log(`  最終ホスト: ${hostname}`);
    if (onRakutenLogin) {
      if (hits.length) {
        console.log(`  ⚠️ 楽天ログイン画面で追加認証キーワードを検出: ${hits.join(', ')}`);
        console.log('  → 追加認証が挟まる = 完全無人化は困難。要件定義 §7 リスク表に反映すること');
      } else {
        console.log('  ❓ まだ楽天ログイン画面に留まっている (ボタン押下失敗の可能性)。3a/3b/4のスクショでボタンDOMを確認');
      }
    } else if (onRms) {
      const backToRLogin = await page.locator('input[name="login_id"]').isVisible().catch(() => false);
      if (backToRLogin) {
        console.log('  ❓ R-Login画面に戻っている = セッション確立失敗。認証情報を確認');
      } else {
        console.log('  ✅ RMSにログイン成功 (追加認証なし)。3段階ログインは自動化可能');
        console.log('  → 次: RPPパフォーマンスレポートのDL動線を rakuten-rpp-download.mjs で実装');
      }
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
