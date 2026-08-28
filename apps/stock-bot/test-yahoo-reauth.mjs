#!/usr/bin/env node
/** test-yahoo-reauth.mjs — GChat からの Yahoo 再認可 (偽 fetch 注入)。実行: node apps/stock-bot/test-yahoo-reauth.mjs */
import { parseYahooReauthCommand, reauthUserAllowed, handleYahooReauth } from './yahoo-reauth.js';
let passed = 0, failed = 0;
const check = (name, cond, detail = '') => { if (cond) { console.log(`  ✅ ${name}`); passed++; } else { console.log(`  ❌ ${name} ${detail}`); failed++; } };

check('parse: yahoo再認可 / yahoo reauth', parseYahooReauthCommand('yahoo再認可')?.kind === 'auth-url' && parseYahooReauthCommand(' Yahoo reauth ')?.kind === 'auth-url');
check('parse: リダイレクト URL の貼り付け', JSON.stringify(parseYahooReauthCommand('https://b-faith.biz/?code=AbC-123_xyz&state=1')) === '{"kind":"code","code":"AbC-123_xyz"}');
check('parse: yahoo code XXXX', parseYahooReauthCommand('yahoo code abcdef12')?.code === 'abcdef12');
check('parse: 商品検索は null (ハッカ油 / code / yahoo)', parseYahooReauthCommand('ハッカ油') === null && parseYahooReauthCommand('code') === null && parseYahooReauthCommand('yahoo') === null);
check('parse: b-faith.biz 以外の ?code= や別パスは拾わない (Codex R1)', parseYahooReauthCommand('https://example.com/?code=ABCDEF123') === null
  && parseYahooReauthCommand('https://b-faith.biz/shop/?code=ABCDEF123') === null && parseYahooReauthCommand('yahoo code ABC') === null
  && parseYahooReauthCommand('https://www.b-faith.biz/?state=x&code=ABCDEF123')?.code === 'ABCDEF123');
check('allowed: 未設定は誰も不可 / 大文字小文字無視', !reauthUserAllowed('a@b-faith.biz', {}) && reauthUserAllowed('A@b-faith.biz', { YAHOO_REAUTH_USERS: 'x@b-faith.biz, a@b-faith.biz' })
  && reauthUserAllowed('a@b-faith.biz', { PD_RULE_APPROVERS: 'a@b-faith.biz' }));

const ENV = { YAHOO_REAUTH_USERS: 'me@b-faith.biz', YAHOO_PROXY_URL: 'http://vps/', YAHOO_PROXY_SECRET: 's' };
const calls = [];
const mkFetch = (routes) => async (url, init = {}) => { calls.push({ url, init }); const r = routes[url.replace('http://vps', '')]; return { ok: r.status < 300, status: r.status, json: async () => r.body }; };
const okFetch = mkFetch({
  '/yahoo/auth-url': { status: 200, body: { url: 'https://auth.login.yahoo.co.jp/x?client_id=1' } },
  '/yahoo/health': { status: 200, body: { refreshTokenExpiresAt: new Date(Date.now() + 27 * 86400000).toISOString() } },
  '/yahoo/token/init': { status: 200, body: { ok: true } },
});
const r0 = await handleYahooReauth({ kind: 'auth-url' }, { email: 'other@b-faith.biz', env: ENV, fetchImpl: okFetch });
check('権限なし → 拒否 (VPS を呼ばない)', /権限がありません/.test(r0.text) && calls.length === 0);
const r1 = await handleYahooReauth({ kind: 'auth-url' }, { email: 'me@b-faith.biz', env: ENV, fetchImpl: okFetch });
check('auth-url: URL と手順・現在の期限', r1.text.includes('https://auth.login.yahoo.co.jp/x?client_id=1') && /残り 2[67] 日/.test(r1.text) && calls[0].init.headers['X-Proxy-Secret'] === 's');
calls.length = 0;
const r2 = await handleYahooReauth({ kind: 'code', code: 'SECRETCODE' }, { email: 'me@b-faith.biz', env: ENV, fetchImpl: okFetch });
check('code: token/init に POST → ✅ と新期限', /✅/.test(r2.text) && calls[0].url.endsWith('/yahoo/token/init') && JSON.parse(calls[0].init.body).code === 'SECRETCODE' && /残り 2[67] 日/.test(r2.text));
check('code は応答文に出ない', !r2.text.includes('SECRETCODE'));
const badFetch = mkFetch({ '/yahoo/token/init': { status: 400, body: { error: 'invalid_grant leaked-detail' } }, '/yahoo/health': { status: 200, body: {} } });
const r3 = await handleYahooReauth({ kind: 'code', code: 'OLD' }, { email: 'me@b-faith.biz', env: ENV, fetchImpl: badFetch });
check('交換失敗 → やり直し案内 (上流文言は返さない)', /❌/.test(r3.text) && /HTTP 400/.test(r3.text) && !/leaked/.test(r3.text));
const r4 = await handleYahooReauth({ kind: 'auth-url' }, { email: 'me@b-faith.biz', env: ENV, fetchImpl: async () => { throw new Error('ECONNREFUSED'); } });
check('VPS 到達不可 → network 案内', /network/.test(r4.text));
const r5 = await handleYahooReauth({ kind: 'auth-url' }, { email: 'me@b-faith.biz', env: { YAHOO_REAUTH_USERS: 'me@b-faith.biz', YAHOO_PROXY_URL: 'http://vps', AUPAY_PROXY_SECRET: 'x' }, fetchImpl: okFetch });
check('proxy secret は YAHOO_PROXY_SECRET のみ (AUPAY の流用不可)', /YAHOO_PROXY_URL/.test(r5.text));
check('手順文に DM 推奨・削除案内', /DM/.test(r1.text) && /削除/.test(r1.text));

console.log(`\n結果: ${passed} PASS / ${failed} FAIL`);
process.exit(failed > 0 ? 1 : 0);
