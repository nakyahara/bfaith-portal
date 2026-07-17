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
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const __dirname = dirname(fileURLToPath(import.meta.url));
loadEnv({ path: join(__dirname, '.env') });

// miniPC では secret をリポジトリ直下 .env に集約する運用 (daily-sync も直下を読む) のため、
// scripts/.env に無い RMS_* は直下 .env から選択的にフォールバックで拾う (該当キーのみ。
// 直下 .env の全変数を process.env に流し込むことはしない)。実運用 2026-07-09 で
// 中原さんが直下に記入した構成をそのまま有効にする
{
  const missing = ['RMS_RLOGIN_ID', 'RMS_RLOGIN_PW', 'RMS_MEMBER_ID', 'RMS_MEMBER_PW']
    .filter((k) => !process.env[k]);
  if (missing.length) {
    try {
      const txt = readFileSync(join(__dirname, '..', '..', '.env'), 'utf8');
      for (const k of missing) {
        const m = txt.match(new RegExp(`^\\s*${k}\\s*=\\s*"?([^"\\r\\n]+)"?\\s*$`, 'm'));
        if (m) process.env[k] = m[1].trim();
      }
    } catch { /* 直下 .env 無し (開発PC等) は通常 */ }
  }
}

export const PROFILE_DIR = join(__dirname, '.profile-rakuten');
const RMS_LOGIN_URL = 'https://glogin.rms.rakuten.co.jp/';

const {
  RMS_RLOGIN_ID, RMS_RLOGIN_PW, RMS_MEMBER_ID, RMS_MEMBER_PW,
  HEADLESS = '0',
} = process.env;

export function safeHost(url) {
  try { return new URL(url).hostname; } catch { return ''; }
}

/** .env fail-fast (Codex高): 認証情報が欠けたまま画面遷移して途中で空振りしない。
 *  値そのものはログに出さない (feedback_secret_hygiene)。 */
export function assertLoginEnv() {
  const required = { RMS_RLOGIN_ID, RMS_RLOGIN_PW, RMS_MEMBER_ID, RMS_MEMBER_PW };
  const missing = Object.entries(required).filter(([, v]) => !v || !String(v).trim()).map(([k]) => k);
  if (missing.length) {
    throw new Error(`ENV_MISSING: .env に ${missing.join(', ')} がありません (scripts/mall-csv-fetcher/.env を確認。記入は中原さん)`);
  }
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

// ─── ログイン回数の上限 (1プロセスあたり) ───
// 2026-07-17 実障害: 楽天側の利用規制でログイン直後にセッションが失効し続け、
// セクションごとの ensureRmsLogin が1回の実行で11回ログイン (= 規制を悪化させる燃料)。
// 正常時はセッションが持続するので1回で足りる。上限超過は「ログインしても維持できない
// 状態」であり、以降は再ログインせず即エラーにする (手動DLフォールバックへ)。
const MAX_LOGINS_PER_RUN = Number(process.env.RMS_MAX_LOGINS_PER_RUN || 3);
let loginCount = 0;

/** R-Login → 楽天会員 → RMS のログイン列を実行。2FA要求時は throw。 */
async function runLogin(page) {
  if (loginCount >= MAX_LOGINS_PER_RUN) {
    throw new Error(`RMS_SESSION_UNSTABLE: ログイン試行が上限${MAX_LOGINS_PER_RUN}回に達した (ログインしてもセッションが維持されない = 楽天側の利用規制/障害の可能性)。ログイン連打を避けるため以降は再ログインせず中止 — 必要分は手動DLで incoming/ へ`);
  }
  loginCount++;
  console.log(`[login] RMSログイン開始 (${loginCount}/${MAX_LOGINS_PER_RUN}回目)`);
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

// ★正しいRMS入口は末尾 /rms。/ だけだと「再度ログインをお願いします」擬似ページに着地する。
const MAINMENU_URL = 'https://mainmenu.rms.rakuten.co.jp/rms';

/** 本当にRMSにログインできているか (システムエラー/再ログイン誘導/ログインフォームを弾く) */
async function looksLoggedIn(page) {
  const url = page.url();
  if (/system_error/i.test(url)) return false;
  const host = safeHost(url);
  if (host === 'login.account.rakuten.com') return false;
  // ★glogin.* はログイン入口(未ログイン)。ここに居る=まだログインしていない
  if (host === 'glogin.rms.rakuten.co.jp' || host.startsWith('glogin.')) return false;
  // R-Loginのログインフォームが出ている=未ログイン
  const hasLoginForm = await page.locator('input[name="login_id"]').isVisible().catch(() => false);
  if (hasLoginForm) return false;
  if (!host.endsWith('rms.rakuten.co.jp')) return false;
  // 「再度ログインをお願いいたします」等の誘導ページを弾く
  const body = await page.locator('body').innerText().catch(() => '');
  if (/再度ログイン|操作を継続して受け付ける|利用者登録.*完了されていますか/.test(body)) return false;
  return true;
}

/** 画面上のリンクを全フレームから一覧出力 (メニュー構造の把握用。RMSはframe構成のことがある) */
export async function dumpLinks(page, label, max = 80) {
  for (const frame of page.frames()) {
    const links = await frame.evaluate((mx) => {
      const vis = (el) => !!(el.offsetParent || el.getClientRects().length);
      return [...document.querySelectorAll('a')].filter(vis)
        .map((a) => ({ text: (a.innerText || '').trim().slice(0, 24), href: a.href }))
        .filter((l) => l.text || l.href).slice(0, mx);
    }, max).catch(() => []);
    if (links.length) {
      console.log(`  [LINKS:${label}] frame=${frame.url().slice(0, 70)}`);
      console.log(`  [LINKS:${label}] ${JSON.stringify(links)}`);
    }
  }
}

/** リダイレクト連鎖が落ち着くまで待つ (mainmenu→gloginへの遷移中にログイン判定すると
 *  「未ログインなのにセッション有効」と誤判定する — miniPC初回疎通 2026-07-09 で実発生) */
async function settleRedirects(page) {
  await page.waitForLoadState('networkidle', { timeout: 15000 }).catch(() => {});
  let prev = page.url();
  for (let i = 0; i < 10; i++) {
    await page.waitForTimeout(500);
    const cur = page.url();
    if (cur === prev) return;
    prev = cur;
  }
}

/** RMSにログイン済みの状態にする (メインメニュー到達)。2FA要求時は throw。 */
export async function ensureRmsLogin(page) {
  await gotoSafe(page, MAINMENU_URL);
  await settleRedirects(page);
  await passNotice(page);
  if (await looksLoggedIn(page)) {
    console.log(`[login] セッション有効 host=${safeHost(page.url())}`);
    return;
  }
  console.log(`[login] セッション無効と判定 host=${safeHost(page.url())} → ログイン実行`);
  await runLogin(page); // 成功時は notice通過後 mainmenu に着地
  await settleRedirects(page);
  await passNotice(page);
  if (await looksLoggedIn(page)) {
    console.log(`[login] メインメニュー到達 host=${safeHost(page.url())}`);
    return;
  }
  // 予備: 改めてメインメニューを開く
  await gotoSafe(page, MAINMENU_URL);
  await settleRedirects(page);
  await passNotice(page);
  if (!(await looksLoggedIn(page))) {
    throw new Error(`ログイン確認できず: ${page.url()}`);
  }
  console.log(`[login] メインメニュー到達(再) host=${safeHost(page.url())}`);
}
