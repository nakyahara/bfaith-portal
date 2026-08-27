#!/usr/bin/env node
/** test-yahoo-order-contact.mjs — PR-Y-B miniPC 側クライアントのスモーク (偽 fetch 注入)。実行: node apps/warehouse/test-yahoo-order-contact.mjs */
import { fetchYahooOrderContact } from './yahoo-order-contact-lib.js';
let passed = 0, failed = 0;
const check = (name, cond, detail = '') => { if (cond) { console.log(`  ✅ ${name}`); passed++; } else { console.log(`  ❌ ${name} ${detail}`); failed++; } };
const mk = (status, body, headers = {}) => async () => ({ status, ok: status >= 200 && status < 300, headers: { get: (k) => headers[k.toLowerCase()] || null }, json: async () => body });
const OPTS = { proxyUrl: 'http://proxy/', secret: 's' };
const expectErr = async (fn, code, retryable) => { try { await fn(); return false; } catch (e) { return e.code === code && e.retryable === retryable; } };

check('設定不備は非 retryable', await expectErr(() => fetchYahooOrderContact('b-faith01-1', { proxyUrl: '', secret: '' }), 'config', false));
check('注文ID不正は非 retryable', await expectErr(() => fetchYahooOrderContact('a b', OPTS), 'invalid_order_id', false));
let captured;
const okFetch = async (url, init) => { captured = { url, init }; return mk(200, { ok: true, contact: { orderId: 'b-faith01-1', orderStatus: '2', shipStatus: '3', shipDate: '2026-08-27', socialGiftType: '0', email: 'u@example.jp' } })(); };
const r = await fetchYahooOrderContact('b-faith01-1', { ...OPTS, fetchImpl: okFetch });
check('正常: POST /yahoo/orderContact に secret ヘッダ・宛先と発送日を返す', r.email === 'u@example.jp' && r.shipDate === '2026-08-27'
  && captured.url === 'http://proxy/yahoo/orderContact' && captured.init.headers['X-Proxy-Secret'] === 's' && JSON.parse(captured.init.body).orderId === 'b-faith01-1');
check('429 → retryable', await expectErr(() => fetchYahooOrderContact('b-faith01-1', { ...OPTS, fetchImpl: mk(429, {}, { 'retry-after': '5' }) }), 'http_429', true));
check('502 public_key_auth_failed → retryable (設定不備、送信前)', await expectErr(() => fetchYahooOrderContact('b-faith01-1', { ...OPTS, fetchImpl: mk(502, { error: 'public_key_auth_failed', authorizeStatus: 'expired-key-version' }) }), 'public_key_auth_failed', true));
check('503 → retryable', await expectErr(() => fetchYahooOrderContact('b-faith01-1', { ...OPTS, fetchImpl: mk(503, { error: 'x' }) }), 'x', true));
check('403 → retryable (secret 不一致)', await expectErr(() => fetchYahooOrderContact('b-faith01-1', { ...OPTS, fetchImpl: mk(403, { error: 'Forbidden' }) }), 'http_403', true));
check('Yahoo が注文なしを返す → order_not_found (非 retryable)', await expectErr(() => fetchYahooOrderContact('b-faith01-1', { ...OPTS, fetchImpl: mk(404, { ok: false, error: 'yahoo_error', code: 'od10001' }) }), 'order_not_found', false));
check('ソーシャルギフト → 非 retryable (送らない)・値は文言に出さない', await expectErr(() => fetchYahooOrderContact('b-faith01-1', { ...OPTS, fetchImpl: mk(200, { ok: true, contact: { orderId: 'b-faith01-1', email: 'u@example.jp', socialGiftType: '2' } }) }), 'social_gift', false));
check('メール空 → 非 retryable', await expectErr(() => fetchYahooOrderContact('b-faith01-1', { ...OPTS, fetchImpl: mk(200, { ok: true, contact: { orderId: 'b-faith01-1', email: '', socialGiftType: '0' } }) }), 'no_email', false));
check('proxy の 502 order_id_mismatch も非 retryable', await expectErr(() => fetchYahooOrderContact('b-faith01-1', { ...OPTS, fetchImpl: mk(502, { ok: false, error: 'order_id_mismatch' }) }), 'order_id_mismatch', false));
check('返却 orderId が要求と違う → order_id_mismatch (非 retryable、送らない)', await expectErr(() => fetchYahooOrderContact('b-faith01-1', { ...OPTS, fetchImpl: mk(200, { ok: true, contact: { orderId: 'b-faith01-2', email: 'u@example.jp', socialGiftType: '0' } }) }), 'order_id_mismatch', false));
check('ネットワーク断 → retryable', await expectErr(() => fetchYahooOrderContact('b-faith01-1', { ...OPTS, fetchImpl: async () => { throw new Error('ECONNRESET'); } }), 'network', true));
let thrown;
try { await fetchYahooOrderContact('b-faith01-1', { ...OPTS, fetchImpl: mk(500, { error: 'boom', message: 'contact u@example.jp leaked?' }) }); } catch (e) { thrown = e; }
check('エラーメッセージに上流 message を含めない (固定文言 + status のみ)', thrown && !/example\.jp|leaked/.test(thrown.message) && /http 500/.test(thrown.message));
let thrown2;
try { await fetchYahooOrderContact('b-faith01-1', { ...OPTS, fetchImpl: async () => { throw new Error('ECONNRESET to u@example.jp'); } }); } catch (e) { thrown2 = e; }
check('ネットワーク例外の文言も持ち回らない', thrown2 && !/example\.jp/.test(thrown2.message) && thrown2.code === 'network');
let thrown3;
try { await fetchYahooOrderContact('b-faith01-1', { ...OPTS, fetchImpl: mk(404, { ok: false, error: 'yahoo_error', code: 'od10001 <u@example.jp>' }) }); } catch (e) { thrown3 = e; }
check('上流 code は英数のみ通す', thrown3 && !/example/.test(thrown3.message) && /code=-/.test(thrown3.message));

console.log(`\n結果: ${passed} PASS / ${failed} FAIL`);
process.exit(failed > 0 ? 1 : 0);
