/**
 * R0スパイク: Qoo10 QSM (Sales Manager) ログインの自動化可否検証 (mall-csv-fetcher P1-Q)
 *
 * 要件定義: AI_reference『Qoo10_QSM自動取得_P1-Q_要件定義_20260713.md』§5 (R0チェックリスト)
 * 初回はログイン画面のDOM実測から (CAPTCHA/OTP/端末認証の有無は未知 → まず観測、決め打ちしない)。
 * 楽天/Yahoo/auPAYと同じ永続プロファイル (.profile-qoo10) でセッション再利用+自動ログインを狙う。
 *
 * ⚠️ 認証情報はコードに書かない。QSM_LOGIN_ID / QSM_LOGIN_PW を .env から読む
 *    (miniPCはリポジトリ直下 .env 集約。記入は中原さん)。
 *
 * 実行:
 *   node scripts/mall-csv-fetcher/qoo10-login-spike.mjs            # 自動ログイン検証
 *   $env:MANUAL=1; node scripts/mall-csv-fetcher/qoo10-login-spike.mjs  # 手動 (CAPTCHA/OTP時)
 *   SPIKE_URL=... で到達後にそのURLへ遷移してDOM/リンクをダンプ (帳票棚卸し用)
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
  const missing = ['QSM_LOGIN_ID', 'QSM_LOGIN_PW', 'QSM_SUB_ID'].filter((k) => !process.env[k]);
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

const { QSM_LOGIN_ID, QSM_LOGIN_PW, QSM_SUB_ID, HEADLESS = '0', MANUAL = '0' } = process.env;

// QSM入口 (実測でリダイレクト先を確認する。ログインURLは初回観測に任せる)
const QSM_TOP_URL = 'https://qsm.qoo10.jp/';
export const PROFILE_DIR = join(__dirname, '.profile-qoo10');

const host = (u) => { try { return new URL(u).hostname; } catch { return ''; } };

async function snap(page, label) {
  await page.screenshot({ path: join(OUT_DIR, `qoo10_${label}.png`), fullPage: true }).catch(() => {});
  console.log(`  [snap] qoo10_${label}  url=${page.url()}  title=${await page.title().catch(() => '?')}`);
}

async function dumpControls(page, label) {
  const info = await page.evaluate(() => {
    const vis = (el) => !!(el.offsetParent || el.getClientRects().length);
    const inputs = [...document.querySelectorAll('input, select')].filter(vis).map((el) => ({
      tag: el.tagName.toLowerCase(), name: el.name || '', id: el.id || '', type: el.type || '',
      ph: el.placeholder || undefined,
    }));
    const buttons = [...document.querySelectorAll('button, input[type=submit], input[type=button], a[onclick]')]
      .filter(vis).map((el) => (el.innerText || el.value || '').trim().replace(/\s+/g, ' ').slice(0, 26)).filter(Boolean);
    const frames = [...document.querySelectorAll('iframe')].map((f) => (f.src || '').slice(0, 100));
    return { inputs, buttons, frames };
  }).catch(() => ({ inputs: [], buttons: [], frames: [] }));
  console.log(`  [DOM:${label}] inputs=${JSON.stringify(info.inputs.slice(0, 20))}`);
  console.log(`  [DOM:${label}] buttons=${JSON.stringify(info.buttons.slice(0, 25))}`);
  if (info.frames.length) console.log(`  [DOM:${label}] iframes=${JSON.stringify(info.frames)}`);
}

async function dumpLinks(page, keywords, label) {
  const links = await page.evaluate((kws) => {
    return [...document.querySelectorAll('a')]
      .map((a) => ({
        text: (a.innerText || a.textContent || '').trim().replace(/\s+/g, ' ').slice(0, 40),
        href: (a.getAttribute('href') || '').slice(0, 140),
        hidden: !(a.offsetParent || a.getClientRects().length),
      }))
      .filter((l) => l.text && kws.some((k) => l.text.includes(k)));
  }, keywords).catch(() => []);
  console.log(`  [LINKS:${label}] ${JSON.stringify(links, null, 1)}`);
}

/** ログイン済みの肯定証拠 (fail-closed)。QSMは要実測 — まずは緩め+ダンプで観測し、確定後に厳格化 */
async function looksLoggedIn(page) {
  if (!/qoo10\.jp$/.test(host(page.url()))) return false;
  if (/login/i.test(page.url())) return false;
  let body = '';
  try { body = await page.locator('body').innerText({ timeout: 10000 }); } catch { return false; }
  const compact = body.replace(/\s+/g, ' ');
  if (/ログインしてください|セッションが切れ|Please login/i.test(compact)) return false;
  // 肯定証拠候補: ログアウト導線 or QSM管理メニュー語彙 (精算管理/配送管理)。実測で厳格化する
  return /ログアウト|Logout/i.test(compact) || (/精算/.test(compact) && /配送/.test(compact));
}

async function runManualSetup(context) {
  const page = context.pages()[0] || await context.newPage();
  console.log('=== 手動セットアップモード (QSM) ===');
  console.log('開いたブラウザでログインし、QSM管理画面トップまで入れたら閉じてください。\n');
  await page.goto(QSM_TOP_URL, { waitUntil: 'domcontentloaded', timeout: 45000 }).catch(() => {});
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

  console.log('=== Qoo10 QSM ログインスパイク (R0) ===');
  const page = context.pages()[0] || await context.newPage();
  page.on('dialog', (d) => { console.log(`  [dialog] ${d.message().slice(0, 120)}`); d.accept().catch(() => {}); });

  try {
    console.log('[Step 0] QSMトップへ (セッション再利用の確認)');
    await page.goto(QSM_TOP_URL, { waitUntil: 'domcontentloaded', timeout: 45000 }).catch(() => {});
    await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
    await page.waitForTimeout(2500);
    await snap(page, '0_entry');

    if (await looksLoggedIn(page)) {
      console.log('  ✅ セッション有効 (プロファイル再利用でログイン済み)');
    } else {
      console.log('[Step 1] 未ログイン → ログイン画面のDOMを観測してから ID/PW自動ログインを試行');
      await dumpControls(page, 'login');
      if (!QSM_LOGIN_ID || !QSM_LOGIN_PW) {
        console.log('  ⚠️ QSM_LOGIN_ID / QSM_LOGIN_PW 未設定。.env に記入してください (中原さん)');
        return;
      }
      // セレクタ候補を順に試す (QSMのログインフォームは初回実測 → 確定後にこのリストを絞る)
      const idSel = ['#txtLoginID', 'input[name=loginId]', 'input[name*=login i]', 'input[type=text]', 'input[type=email]'];
      const pwSel = ['#txtPassword', 'input[name=password]', 'input[name*=pass i]', 'input[type=password]'];
      let filled = false;
      for (const is of idSel) {
        const loc = page.locator(is).first();
        if (await loc.isVisible().catch(() => false)) {
          await loc.fill(QSM_LOGIN_ID, { timeout: 8000 });
          for (const ps of pwSel) {
            const pl = page.locator(ps).first();
            if (await pl.isVisible().catch(() => false)) {
              await pl.fill(QSM_LOGIN_PW, { timeout: 8000 });
              filled = true;
              break;
            }
          }
          break;
        }
      }
      if (!filled) {
        console.log('  ❓ ログインフォームを特定できず (上のDOMダンプを確認して次スパイクでセレクタ確定)');
        await snap(page, '1_form_unknown');
        return;
      }
      await snap(page, '1a_filled');
      // 送信: ログイン系ボタン候補をクリック (無ければEnter)
      const btn = page.locator('button:has-text("ログイン"), input[type=submit], a:has-text("ログイン")').first();
      if (await btn.isVisible().catch(() => false)) await btn.click({ timeout: 8000 });
      else await page.keyboard.press('Enter');
      await page.waitForLoadState('networkidle', { timeout: 30000 }).catch(() => {});
      await page.waitForTimeout(3000);
      await snap(page, '1b_after_login');

      // [Step 2] サブID入力段 (実運用で確認済み: 本ID/PWの直後にサブIDを求められる)
      {
        const bodyNow = (await page.locator('body').innerText().catch(() => '')).replace(/\s+/g, ' ');
        if (/サブ\s*ID|SUB\s*ID/i.test(bodyNow)) {
          console.log('[Step 2] サブID入力画面を検出');
          await dumpControls(page, 'subid');
          if (!QSM_SUB_ID) {
            console.log('  ⚠️ QSM_SUB_ID 未設定。.env に記入してください (中原さん)');
            await snap(page, '2_subid_missing');
            return;
          }
          // 可視のテキスト入力にサブIDを入れる (パスワード欄が同時にあれば本PWを再入力)
          const subInput = page.locator('input[type=text]:visible, input:not([type]):visible').first();
          if (!(await subInput.isVisible().catch(() => false))) {
            console.log('  ❓ サブID入力欄を特定できず (DOMダンプからセレクタ確定)');
            await snap(page, '2_subid_unknown');
            return;
          }
          await subInput.fill(QSM_SUB_ID, { timeout: 8000 });
          const subPw = page.locator('input[type=password]:visible').first();
          if (await subPw.isVisible().catch(() => false)) {
            await subPw.fill(QSM_LOGIN_PW, { timeout: 8000 });
            console.log('  [subid] パスワード欄も検出 → 本PWを入力 (違うPWなら次スパイクで env 分離)');
          }
          await snap(page, '2a_subid_filled');
          const btn2 = page.locator('button:has-text("ログイン"), button:has-text("確認"), input[type=submit], a:has-text("ログイン"), a:has-text("確認")').first();
          if (await btn2.isVisible().catch(() => false)) await btn2.click({ timeout: 8000 });
          else await page.keyboard.press('Enter');
          await page.waitForLoadState('networkidle', { timeout: 30000 }).catch(() => {});
          await page.waitForTimeout(3000);
          await snap(page, '2b_after_subid');
        }
      }
    }

    console.log('\n[判定]');
    console.log(`  最終URL: ${page.url()}`);
    const body = await page.locator('body').innerText().catch(() => '');
    const compact = body.replace(/\s+/g, ' ');
    const twoFa = ['認証コード', '確認コード', 'ワンタイム', 'OTP', 'SMS', '画像認証', 'CAPTCHA', 'reCAPTCHA', '追加認証']
      .filter((w) => compact.includes(w));
    if (await looksLoggedIn(page)) {
      console.log('  ✅ QSMにログイン成功');
      // ショップ名/IDの肯定証拠候補を観測 (複数ショップ誤取得防止 — Codex)
      const shopHint = compact.match(/.{0,40}(セラー|ショップ|Seller|Shop).{0,60}/);
      console.log(`  [shop-hint] ${shopHint ? shopHint[0] : '(要目視: スクショ確認)'}`);
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
      await dumpLinks(page, ['精算', '販売分析', 'Analytics', '内訳', 'プロモーション', 'スマートセールス', 'ダウンロード', 'Excel', '統計', 'レポート', '配送', '広告'], 'nav');
    } else if (twoFa.length) {
      console.log(`  ⚠️ 追加認証/CAPTCHAを検出: ${twoFa.join(', ')} → MANUAL=1 で手動ログインしてセッションを焼いてください`);
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
