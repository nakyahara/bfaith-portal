/**
 * Yahoo!ストアクリエイターPro 手動再ログイン (セッション焼き直し)
 *
 * 旧 yahoo-login-spike.mjs の MANUAL=1 は入口が editor.store.yahoo.co.jp (現在は表示不可) の
 * ため使えない。現行の入口 (lib-yahoo-login.mjs の STORE_TOP_URL) を開き、
 * ログイン完了を5秒ごとにポーリングして自動で終了する。
 *
 * 実行 (ミニPCの画面で): node scripts/mall-csv-fetcher/yahoo-manual-login.mjs
 */
import { openYahooContext, STORE_TOP_URL, checkLoggedInEvidence, PROFILE_DIR } from './lib-yahoo-login.mjs';

const WAIT_MS = 15 * 60 * 1000;
const POLL_MS = 5000;

const ctx = await openYahooContext();
const page = ctx.pages()[0] || (await ctx.newPage());

console.log('=== Yahoo re-login (manual) ===');
console.log(`profile = ${PROFILE_DIR}`);
console.log(`open    = ${STORE_TOP_URL}`);
console.log('');
console.log('[1] Log in with "Yahoo! JAPAN ID" on the LINE Business ID screen.');
console.log('[2] Verification code -> bfaith.openclaw@gmail.com');
console.log('[3] Turn ON "stay logged in" if offered.');
console.log('[4] This window closes automatically once login is confirmed.');
console.log('');

await page.goto(STORE_TOP_URL, { waitUntil: 'domcontentloaded', timeout: 60000 }).catch(() => {});

const deadline = Date.now() + WAIT_MS;
let ok = false;
while (Date.now() < deadline) {
  await new Promise((r) => setTimeout(r, POLL_MS));
  const pages = ctx.pages();
  if (!pages.length) break; // ユーザーがブラウザを閉じた
  let anyOk = false;
  for (const p of pages) {
    const ev = await checkLoggedInEvidence(p).catch(() => ({ ok: false }));
    if (ev.ok) { anyOk = true; break; }
  }
  if (anyOk) { ok = true; break; }
}

if (ok) {
  console.log('');
  console.log('*** LOGIN OK - session saved to .profile-yahoo ***');
} else {
  console.log('');
  console.log('!!! NOT CONFIRMED - login was not detected (timeout or browser closed early)');
  console.log('    If you reached the store top page, run this again to verify.');
  process.exitCode = 1;
}
await ctx.close().catch(() => {});
