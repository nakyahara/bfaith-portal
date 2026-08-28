#!/usr/bin/env node
/** test-yahoo-token-expiry.mjs — Yahoo トークン期限アラートの文面判定。実行: node apps/warehouse/test-yahoo-token-expiry.mjs */
import { buildTokenExpiryNotice, daysUntil, jstDay, DEFAULT_WARN_DAYS } from './yahoo-token-expiry-lib.js';
let passed = 0, failed = 0;
const check = (name, cond, detail = '') => { if (cond) { console.log(`  ✅ ${name}`); passed++; } else { console.log(`  ❌ ${name} ${detail}`); failed++; } };

const NOW = '2026-09-20T00:00:00.000Z';         // JST 9/20 09:00
const AUTH = 'https://auth.login.yahoo.co.jp/yconnect/v2/authorization?x=1';
const h = (iso, extra = {}) => ({ hasTokens: true, refreshTokenExpiresAt: iso, ...extra });
const notice = (iso, opts = {}) => buildTokenExpiryNotice({ health: h(iso), authUrl: AUTH, nowIso: NOW, ...opts });

check('jstDay: UTC→JST 暦日', jstDay('2026-09-25T20:00:00.000Z') === '2026-09-26' && jstDay('2026-09-25T05:00:00.000Z') === '2026-09-25');
check('daysUntil: JST 暦日の差 (当日=0)', daysUntil('2026-09-25T05:00:27.000Z', NOW) === 5 && daysUntil('2026-09-20T05:00:00.000Z', NOW) === 0
  && daysUntil('2026-09-19T05:00:00.000Z', NOW) === -1
  && daysUntil('2026-09-20T23:00:00.000Z', NOW) === 1); // 23:00Z = JST 翌日 08:00

check('残り 27 日 → 通知しない', notice('2026-10-17T05:00:00.000Z') === null);
check('残り 6 日 → 通知しない (既定しきい値 5)', notice('2026-09-26T05:00:00.000Z') === null);
const n5 = notice('2026-09-25T05:00:00.000Z');
check('残り 5 日 → 🔑 リマインド (kind=expiring, level=warn)', n5?.kind === 'expiring' && n5.level === 'warn' && n5.daysLeft === 5 && n5.text.startsWith('🔑'), JSON.stringify(n5?.daysLeft));
check('文面に 期限日 / ボット手順 / 認可URL / 影響', n5.text.includes('2026-09-25') && n5.text.includes('yahoo再認可') && n5.text.includes(AUTH)
  && n5.text.includes('受注取込') && n5.text.includes('b-faith.biz/?code='));
const n2 = notice('2026-09-22T05:00:00.000Z');
check('残り 2 日 → 🔴 至急 (level=critical)', n2.level === 'critical' && n2.text.startsWith('🔴') && n2.daysLeft === 2);
const n0 = notice('2026-09-20T05:00:00.000Z');
check('当日 (残り 0 日) → 🔴', n0.daysLeft === 0 && n0.level === 'critical');
const nEx = notice('2026-09-18T05:00:00.000Z');
check('失効済み → kind=expired・止まっている旨', nEx.kind === 'expired' && nEx.daysLeft === -2 && nEx.text.includes('失効しました') && nEx.text.includes('現在、'));

check('しきい値は env で変えられる (warnDays=10 なら残り 6 日でも通知)', notice('2026-09-26T05:00:00.000Z', { warnDays: 10 })?.daysLeft === 6);
check('既定しきい値は 5', DEFAULT_WARN_DAYS === 5);

const hErr = buildTokenExpiryNotice({ health: null, healthError: 'timeout', authUrl: null, nowIso: NOW });
check('プロキシ到達不可 → 🔴 health_unreachable (見張れていないことを通知) + 再認可手順も入れる',
  hErr.kind === 'health_unreachable' && hErr.level === 'critical' && hErr.text.includes('timeout') && hErr.text.includes('yahoo再認可'));
const noTok = buildTokenExpiryNotice({ health: { hasTokens: false }, authUrl: AUTH, nowIso: NOW });
check('トークン未初期化 → 🔴 no_tokens + 手順', noTok.kind === 'no_tokens' && noTok.text.includes('yahoo再認可'));
const unk = buildTokenExpiryNotice({ health: { hasTokens: true }, authUrl: AUTH, nowIso: NOW });
check('期限不明 → 🔴 expiry_unknown', unk.kind === 'expiry_unknown');
const bad = buildTokenExpiryNotice({ health: h('not-a-date'), authUrl: AUTH, nowIso: NOW });
check('期限が壊れている → 🔴 (値は 40 文字で切る)', bad.kind === 'expiry_unknown' && bad.text.includes('not-a-date'));
check('認可URLが取れなくても文面は成立', !notice('2026-09-25T05:00:00.000Z', { authUrl: null }).text.includes('undefined'));
check('kind は重複抑止のキーとして安定 (同じ状況なら同じ kind)', notice('2026-09-25T05:00:00.000Z').kind === notice('2026-09-24T05:00:00.000Z').kind);

console.log(`\n結果: ${passed} PASS / ${failed} FAIL`);
process.exit(failed > 0 ? 1 : 0);
