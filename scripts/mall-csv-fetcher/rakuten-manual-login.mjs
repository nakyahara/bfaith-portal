/**
 * 楽天RMS 手動再ログイン (セッション焼き直し・信頼できる端末の登録)
 *
 * rakuten-login-spike.mjs の MANUAL=1 は「15分待って閉じるだけ」で、
 * ログインできたのか失敗したのかが人にも自動化にも分からなかった (2026-08-30)。
 * ここでは Yahoo 版 (yahoo-manual-login.mjs) と同じく **ログイン完了をポーリングで検知して
 * はっきり成否を出す**。判定は自動ログインと同じ looksLoggedIn を使う (基準を二重に持たない)。
 *
 * 実行 (ミニPCの画面で): node scripts/mall-csv-fetcher/rakuten-manual-login.mjs
 */
import { openContext, looksLoggedIn, PROFILE_DIR, safeHost } from './lib-rakuten-login.mjs';

const RMS_LOGIN_URL = 'https://glogin.rms.rakuten.co.jp/';
const WAIT_MS = 15 * 60 * 1000;
const POLL_MS = 5000;

process.env.HEADLESS = '0'; // 手動なので必ず画面表示
const ctx = await openContext();
const page = ctx.pages()[0] || (await ctx.newPage());

console.log('=== Rakuten RMS re-login (manual) ===');
console.log(`profile = ${PROFILE_DIR}`);
console.log('');
console.log('[1] Log in with the R-Login ID / password.');
console.log('[2] Then log in as the Rakuten member (2-step verification).');
console.log('[3] IMPORTANT: tick "register as a trusted device" (skips 2FA for 14 days).');
console.log('[4] This window closes automatically once the RMS main menu is reached.');
console.log('');

await page.goto(RMS_LOGIN_URL, { waitUntil: 'domcontentloaded', timeout: 60000 }).catch(() => {});

const deadline = Date.now() + WAIT_MS;
let ok = false;
let lastHost = '';
while (Date.now() < deadline) {
  await new Promise((r) => setTimeout(r, POLL_MS));
  const pages = ctx.pages();
  if (!pages.length) break; // 人がブラウザを閉じた
  for (const p of pages) {
    const host = safeHost(p.url());
    if (host && host !== lastHost) { console.log(`  ... ${host}`); lastHost = host; }
    if (await looksLoggedIn(p).catch(() => false)) { ok = true; break; }
  }
  if (ok) break;
}

if (ok) {
  console.log('');
  console.log('*** LOGIN OK - session saved to .profile-rakuten ***');
  console.log('    Automated download should work now.');
} else {
  console.log('');
  console.log('!!! NOT CONFIRMED - the RMS main menu was never reached');
  console.log('    (timeout, browser closed early, or the login was rejected)');
  console.log('    Do NOT retry many times in a row - repeated logins get the account throttled.');
  process.exitCode = 1;
}
await ctx.close().catch(() => {});
