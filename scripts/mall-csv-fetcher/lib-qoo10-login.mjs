/**
 * Qoo10 QSM ログイン共通モジュール (mall-csv-fetcher P1-Q)
 *
 * 前提 (実測 2026-07-13〜14、要件定義=AI_reference『Qoo10_QSM自動取得_P1-Q_要件定義_20260713.md』):
 *   - QSMログインは reCAPTCHA Enterprise 付き → ID/PW完全自動は不可
 *   - 認証は ASP.NET session cookie (ASP.NET_SessionId/GiosisGsmJP)。Chromiumは
 *     session cookie をディスク保存しない → 永続プロファイル方式は不成立
 *   - **storageState 方式**: 初回 MANUAL=1 (qoo10-login-spike.mjs) でログイン完了を検知して
 *     .qoo10-state.json に保存 (session cookie込み) → 本番は newContext({storageState}) で注入。
 *     セッションは15時間生存を実証済み。**毎run成功後に再保存して延命する** (rolling)
 *   - 切れていたら 2FA_REQUIRED throw → GChat通知 → 中原さんが MANUAL=1 で手動再ログイン
 *
 * 認証判定は肯定証拠方式・fail-closed:
 *   ログアウト導線 + ショップ名ヒント (QSM_SHOP_HINT、既定 'b-faith') の両方を必須
 *   (Codex: 「認証成功」だけでなく正しいショップの証拠を要求 — 複数ショップ誤取得防止)
 */

import { chromium } from 'playwright';
import { config as loadEnv } from 'dotenv';
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const __dirname = dirname(fileURLToPath(import.meta.url));
loadEnv({ path: join(__dirname, '.env') });

export const STATE_PATH = join(__dirname, '.qoo10-state.json');
export const QSM_TOP_URL = 'https://qsm.qoo10.jp/GMKT.INC.Gsm.Web/Default.aspx';
export const ANALYTICS_BASE = 'https://seller.qoo10.jp';
const SHOP_HINT = (process.env.QSM_SHOP_HINT || 'b-faith').trim();

const MANUAL_HINT = '$env:MANUAL=1; node scripts/mall-csv-fetcher/qoo10-login-spike.mjs (本ID/PW+reCAPTCHA+サブID→「✅storageState保存」を確認)';

export function safeHost(url) {
  try { return new URL(url).hostname; } catch { return ''; }
}

/** storageState を注入した browser+context を開く。stateが無ければ 2FA_REQUIRED */
export async function openQoo10Context() {
  if (!existsSync(STATE_PATH)) {
    throw new Error(`2FA_REQUIRED: ${STATE_PATH} がありません。miniPCで手動ログインしてください: ${MANUAL_HINT}`);
  }
  const browser = await chromium.launch({ headless: process.env.HEADLESS === '1' });
  const context = await browser.newContext({
    storageState: STATE_PATH,
    locale: 'ja-JP',
    viewport: { width: 1500, height: 1100 },
    acceptDownloads: true,
  });
  return { browser, context };
}

/** run成功後にセッションを再保存 (rolling延命)。失敗しても致命ではない (次回は古いstateで再試行) */
export async function saveQoo10State(context) {
  try {
    await context.storageState({ path: STATE_PATH });
    console.log('[qoo10-login] storageState を再保存 (セッション延命)');
  } catch (e) {
    console.warn(`[qoo10-login] ⚠ storageState再保存失敗 (${e.message})`);
  }
}

/** ログイン済みの肯定証拠 (fail-closed)。QSM/Analytics 共通 */
async function checkEvidence(page, { hostSuffix, label }) {
  const host = safeHost(page.url());
  if (!host.endsWith(hostSuffix)) {
    return { ok: false, reason: `host=${host} (${hostSuffix} 以外)` };
  }
  if (/login/i.test(page.url())) return { ok: false, reason: 'ログイン画面' };
  let body = null;
  try {
    body = await page.locator('body').innerText({ timeout: 10000 });
  } catch {
    return { ok: false, reason: 'body取得失敗 (判定材料なし → fail-closed)' };
  }
  const compact = body.replace(/\s+/g, ' ');
  if (/ログインしてください|セッションが切れ|再度ログイン/.test(compact)) {
    return { ok: false, reason: '未認証文言を検出' };
  }
  const hasLogout = /ログアウト|Logout/i.test(compact);
  const hasShop = compact.includes(SHOP_HINT);
  if (hasLogout && hasShop) return { ok: true, reason: 'ok' };
  return { ok: false, reason: `肯定証拠不足 (logout=${hasLogout} shop[${SHOP_HINT}]=${hasShop}) — ${label}` };
}

/**
 * 認証必須ページへ遷移して検証する。セッション切れ (login画面/証拠不足) は 2FA_REQUIRED throw。
 * QSMのsession認証は自動再ログインできない (reCAPTCHA) ため、リトライせず即通知に倒す。
 */
export async function gotoQoo10Page(page, url, label = url) {
  let resp = null;
  try {
    resp = await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 45000 });
  } catch (e) {
    throw new Error(`NAV_FAILED: ${label} へ遷移できず (${String(e.message).split('\n')[0]})`);
  }
  await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
  await page.waitForTimeout(2000);

  if (/login/i.test(page.url())) {
    throw new Error(`2FA_REQUIRED: Qoo10セッションが切れています。miniPCで手動再ログイン: ${MANUAL_HINT}`);
  }
  if (resp && resp.status() >= 400) {
    throw new Error(`NAV_FAILED: ${label} が HTTP ${resp.status()}`);
  }
  const hostSuffix = safeHost(url).endsWith('seller.qoo10.jp') ? 'seller.qoo10.jp' : 'qsm.qoo10.jp';
  const ev = await checkEvidence(page, { hostSuffix, label });
  if (!ev.ok) {
    throw new Error(`2FA_REQUIRED: ${label} で認証済み証拠を確認できず (${ev.reason})。手動再ログイン: ${MANUAL_HINT}`);
  }
  return page;
}

/** セッション健全性チェック: QSMトップで肯定証拠を検証 */
export async function ensureQsmLogin(page) {
  await gotoQoo10Page(page, QSM_TOP_URL, 'QSMトップ');
  console.log(`[qoo10-login] セッション有効 (shop=${SHOP_HINT}, 肯定証拠OK)`);
}
