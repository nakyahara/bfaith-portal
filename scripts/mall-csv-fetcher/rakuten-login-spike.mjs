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
  MANUAL = '0',   // 1=手動セットアップモード(人がログイン+2FA+信頼端末登録。自動入力しない)
} = process.env;

const RMS_LOGIN_URL = 'https://glogin.rms.rakuten.co.jp/';

// ★信頼できる端末(14日間2FAスキップ)のCookieを焼き付ける固定プロファイル。
// ここにセッションが残るので、初回に人が2FA+信頼端末登録すれば以後14日間は自動ログインで2FA不要。
const PROFILE_DIR = join(__dirname, '.profile-rakuten');

function requireEnv() {
  // 手動セットアップモードでは自動入力しないので認証情報は必須ではない
  if (MANUAL === '1') return;
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
 * 画面上の入力欄・ボタンの実DOM属性をコンソールに書き出す診断ヘルパー。
 * 楽天がページを変えてセレクタが合わなくなったとき、何を狙えばいいかを一目で分かるようにする。
 */
async function dumpControls(page, label) {
  const info = await page.evaluate(() => {
    const vis = (el) => !!(el.offsetParent || el.getClientRects().length);
    const inputs = [...document.querySelectorAll('input')].filter(vis).map((el) => ({
      name: el.name || '', id: el.id || '', type: el.type || '', placeholder: el.placeholder || '',
    }));
    const buttons = [...document.querySelectorAll('button, input[type=submit], a[role=button]')]
      .filter(vis).map((el) => (el.innerText || el.value || '').trim()).filter(Boolean);
    return { inputs, buttons };
  }).catch(() => ({ inputs: [], buttons: [] }));
  console.log(`  [DOM:${label}] 入力欄=${JSON.stringify(info.inputs)}`);
  console.log(`  [DOM:${label}] ボタン=${JSON.stringify(info.buttons)}`);
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

/**
 * 手動セットアップモード: 固定プロファイルでブラウザを開き、人が手で
 * ログイン → 2段階認証 → 「信頼できる端末に登録」にチェック する。
 * これでプロファイルに信頼端末Cookieが焼かれ、以後14日間は自動ログインで2FA不要になる。
 */
async function runManualSetup(context) {
  const page = context.pages()[0] || await context.newPage();
  console.log('=== 手動セットアップモード (信頼できる端末の登録) ===');
  console.log('手順:');
  console.log('  1. 開いたブラウザで R-Login → 楽天会員 と手でログイン');
  console.log('  2. 2段階認証を完了 (未登録なら先に楽天会員情報で2FAを登録)');
  console.log('  3. 「信頼できる端末として登録する」に必ずチェックを入れる');
  console.log('  4. RMSトップまで入れたら、このプロファイルにセッションが保存される');
  console.log('  完了後、ブラウザを閉じるか Ctrl+C で終了。以後は MANUAL=0 で自動ログインを試す\n');
  await page.goto(RMS_LOGIN_URL, { waitUntil: 'domcontentloaded', timeout: 30000 }).catch(() => {});
  console.log('ブラウザを最大15分開いたままにします。ログインが済んだら閉じてください。');
  await page.waitForTimeout(15 * 60 * 1000).catch(() => {});
}

async function main() {
  requireEnv();
  await mkdir(OUT_DIR, { recursive: true });

  // ★固定プロファイル(launchPersistentContext)で起動。信頼端末Cookieが .profile-rakuten に永続する。
  const context = await chromium.launchPersistentContext(PROFILE_DIR, {
    headless: MANUAL === '1' ? false : HEADLESS === '1', // 手動モードは必ず画面表示
    slowMo: HEADLESS === '1' ? 0 : 300,
    locale: 'ja-JP',
    viewport: { width: 1280, height: 1000 },
  });

  if (MANUAL === '1') {
    try { await runManualSetup(context); }
    finally { await context.close(); }
    return;
  }

  console.log('=== 楽天RMS 自動ログイン P0スパイク (信頼端末モード) ===');
  console.log(`HEADLESS=${HEADLESS} / プロファイル=${PROFILE_DIR}`);
  console.log('前提: 事前に MANUAL=1 で手動ログイン+信頼端末登録を済ませていること\n');

  const page = context.pages()[0] || await context.newPage();

  try {
    // --- Step 0: ログイン入口 ---
    console.log('[Step 0] ログイン画面を開く');
    await page.goto(RMS_LOGIN_URL, { waitUntil: 'domcontentloaded', timeout: 30000 });
    await snap(page, '0_login_entry');

    // --- Step 1: R-Login ID / PW ---
    // ※R-Loginには複数レイアウトがある(共通ID版=ボタン「楽天会員ログインへ」/旧版=「ログイン」)。
    //   どのページか診断ダンプで確認しつつ、両対応のセレクタで埋める。
    console.log('[Step 1] R-Login 認証');
    await dumpControls(page, 'rlogin');
    await tryFill(page, [
      'input[name="login_id"]', 'input#login_id',
      'input[type="text"]:visible',
      'input:not([type="password"]):not([type="hidden"]):not([type="checkbox"]):visible',
    ], RMS_RLOGIN_ID, 'R-Login ID');
    await tryFill(page, ['input[name="passwd"]', 'input#passwd', 'input[type="password"]:visible'], RMS_RLOGIN_PW, 'R-Login PW');
    await snap(page, '1a_rlogin_filled');
    await tryClick(page, [
      'text=楽天会員ログインへ', 'text=ログイン',
      'button[type="submit"]', 'input[type="submit"]',
    ], 'R-Loginログイン');
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
        console.log(`  ⚠️ 楽天ログイン画面で2段階認証を要求: ${hits.join(', ')}`);
        console.log('  → 信頼できる端末の登録が切れている(14日経過)か、まだ未登録。');
        console.log('  → MANUAL=1 で手動ログインし「信頼できる端末に登録」にチェックを入れ直すこと:');
        console.log('     $env:MANUAL=1; node scripts/mall-csv-fetcher/rakuten-login-spike.mjs');
      } else {
        console.log('  ❓ まだ楽天ログイン画面に留まっている (ボタン押下失敗の可能性)。3a/3b/4のスクショでボタンDOMを確認');
      }
    } else if (onRms) {
      const backToRLogin = await page.locator('input[name="login_id"]').isVisible().catch(() => false);
      if (backToRLogin) {
        console.log('  ❓ R-Login画面に戻っている = セッション確立失敗。認証情報を確認');
      } else {
        console.log('  ✅ RMSにログイン成功 (信頼端末で2FAスキップ)。自動ログイン確立');
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
    await context.close();
  }
}

main();
