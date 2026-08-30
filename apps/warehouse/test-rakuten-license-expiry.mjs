#!/usr/bin/env node
/** test-rakuten-license-expiry.mjs — 楽天ライセンス期限アラートの文面判定 + RMSエラー抽出。
 *  実行: node apps/warehouse/test-rakuten-license-expiry.mjs */
import { buildLicenseExpiryNotice, parseRmsExpiry, daysUntil, jstDay, DEFAULT_WARN_DAYS, URGENT_DAYS } from './rakuten-license-expiry-lib.js';
import { describeRmsError, rmsErrorSuffix } from './rakuten-client.js';
import { describeRakutenError } from '../inquiry-hub/sync/adapters/rakuten.js';

let passed = 0, failed = 0;
const check = (name, cond, detail = '') => { if (cond) { console.log(`  ✅ ${name}`); passed++; } else { console.log(`  ❌ ${name} ${detail}`); failed++; } };

const NOW = '2026-11-14T01:00:00.000Z';        // JST 11/14 10:00
const notice = (expiryDate, opts = {}) => buildLicenseExpiryNotice({ expiryDate, nowIso: NOW, ...opts });

console.log('\n── 期限のパース (タイムゾーン表記なし = JST) ──');
// 実測レスポンス: { "expiryDate": "2026-11-28T23:59:59" } — TZ 無しなので JST として解釈する
check('TZなし → JST 解釈 (ホストTZに依存しない)', parseRmsExpiry('2026-11-28T23:59:59') === Date.parse('2026-11-28T23:59:59+09:00'));
check('TZ付き (+09:00) → そのまま', parseRmsExpiry('2026-11-28T23:59:59+09:00') === Date.parse('2026-11-28T23:59:59+09:00'));
check('TZ付き (Z) → そのまま', parseRmsExpiry('2026-11-28T14:59:59Z') === Date.parse('2026-11-28T14:59:59Z'));
check('日付だけ → その日の JST 23:59:59', parseRmsExpiry('2026-11-28') === Date.parse('2026-11-28T23:59:59+09:00'));
check('壊れた値 / 空 / null → null', parseRmsExpiry('not-a-date') === null && parseRmsExpiry('') === null && parseRmsExpiry(null) === null);
check('jstDay: UTC→JST 暦日', jstDay(Date.parse('2026-11-28T20:00:00Z')) === '2026-11-29' && jstDay(Date.parse('2026-11-28T05:00:00Z')) === '2026-11-28');
check('daysUntil: JST 暦日の差 (当日=0)',
  daysUntil(parseRmsExpiry('2026-11-28T23:59:59'), Date.parse(NOW)) === 14
  && daysUntil(parseRmsExpiry('2026-11-14T23:59:59'), Date.parse(NOW)) === 0
  && daysUntil(parseRmsExpiry('2026-11-13T23:59:59'), Date.parse(NOW)) === -1);

console.log('\n── 通知の段階 ──');
check(`残り 15 日 → 通知しない (既定しきい値 ${DEFAULT_WARN_DAYS})`, notice('2026-11-29T23:59:59') === null);
const n14 = notice('2026-11-28T23:59:59');
check('残り 14 日 → 🔑 リマインド (kind=expiring, level=warn)',
  n14?.kind === 'expiring' && n14.level === 'warn' && n14.daysLeft === 14 && n14.text.startsWith('🔑'), JSON.stringify(n14?.daysLeft));
check('文面に 期限日 / RMS手順 / .env / 再起動 / 影響範囲',
  n14.text.includes('2026-11-28') && n14.text.includes('ライセンスキーの確認・変更') && n14.text.includes('RAKUTEN_LICENSE_KEY')
  && n14.text.includes('Restart-Service WarehouseServer') && n14.text.includes('受注取込') && n14.text.includes('問い合わせ返信'));
const n3 = notice('2026-11-17T23:59:59');
check(`残り ${URGENT_DAYS} 日 → 🔴 至急 (level=critical)`, n3.level === 'critical' && n3.text.startsWith('🔴') && n3.daysLeft === 3);
const n0 = notice('2026-11-14T23:59:59');
check('当日 (残り 0 日) → 🔴', n0.daysLeft === 0 && n0.level === 'critical');
const nPast = notice('2026-11-12T23:59:59');
check('期限が過ぎている → kind=expired', nPast.kind === 'expired' && nPast.daysLeft === -2 && nPast.text.includes('期限切れ'));
check('しきい値は env で変えられる (warnDays=30 なら残り 20 日でも通知)', notice('2026-12-04T23:59:59', { warnDays: 30 })?.daysLeft === 20);
check('しきい値 0 なら当日だけ通知', notice('2026-11-15T23:59:59', { warnDays: 0 }) === null && notice('2026-11-14T23:59:59', { warnDays: 0 })?.daysLeft === 0);

console.log('\n── 異常系 ──');
const nAuth = buildLicenseExpiryNotice({ authFailed: true, nowIso: NOW });
check('401 (キー失効) → 🔴 expired + GA0001 + 手順', nAuth.kind === 'expired' && nAuth.level === 'critical'
  && nAuth.text.includes('GA0001') && nAuth.text.includes('止まっています') && nAuth.text.includes('ライセンスキーの確認・変更'));
const nErr = buildLicenseExpiryNotice({ fetchError: '期限API タイムアウト', nowIso: NOW });
check('期限APIが落ちている → 🔴 expiry_unreachable (理由つき)',
  nErr.kind === 'expiry_unreachable' && nErr.text.includes('タイムアウト') && nErr.text.includes('翌朝の実行で自動復帰'));
const nBad = notice('not-a-date');
check('期限が壊れている → 🔴 expiry_unknown (値を 40 文字で切って出す)', nBad.kind === 'expiry_unknown' && nBad.text.includes('not-a-date'));
check('期限が null → 🔴 expiry_unknown', notice(null).kind === 'expiry_unknown');
check('kind は重複抑止のキーとして安定 (残り日数が変わっても同じ)', notice('2026-11-28T23:59:59').kind === notice('2026-11-20T23:59:59').kind);
check('文面に undefined / null が混ざらない', [n14, n3, n0, nPast, nAuth, nErr, nBad].every(n => !/undefined|null/.test(n.text)));

console.log('\n── RMS エラー本文の抽出 (401 が「{}」になっていた件) ──');
// 2026-08-30 実測: 楽天は認証エラーを errors[] で返す。これを拾えないと原因が分からない
const AUTH_BODY = { errors: [{ code: 'GA0001', message: 'Un-Authorised' }] };
check('warehouse: errors[] から code/message', describeRmsError(AUTH_BODY).code === 'GA0001' && describeRmsError(AUTH_BODY).message === 'Un-Authorised');
check('warehouse: MessageModelList (受注API系)',
  describeRmsError({ MessageModelList: [{ messageType: 'ERROR', messageCode: 'IE001', message: 'bad parameter' }] }).code === 'IE001');
check('warehouse: MessageModelList は ERROR を優先',
  describeRmsError({ MessageModelList: [{ messageType: 'INFO', messageCode: 'I1', message: 'i' }, { messageType: 'ERROR', messageCode: 'E1', message: 'e' }] }).code === 'E1');
check('warehouse: {code,message} 形式', describeRmsError({ code: 'X1', message: 'oops' }).code === 'X1');
check('warehouse: 文字列本文・null・空配列でも落ちない',
  describeRmsError('plain text').message === 'plain text' && describeRmsError(null).code === null
  && describeRmsError({ errors: [] }).code === null && describeRmsError({ errors: ['x'] }).code === null);
check('warehouse: suffix は " (GA0001: Un-Authorised)"', rmsErrorSuffix(AUTH_BODY) === ' (GA0001: Un-Authorised)' && rmsErrorSuffix(null) === '');

// inquiry-hub 側 (画面に出る文面)。旧実装は JSON.stringify({code:undefined,...}) で '{}' になっていた
const ih = describeRakutenError(JSON.stringify(AUTH_BODY), 401);
check('inquiry-hub: 401 で GA0001 と復旧手順が出る', ih.includes('GA0001: Un-Authorised') && ih.includes('ライセンスキー失効の可能性') && ih.includes('Restart-Service'));
check('inquiry-hub: 400 には認証ヒントを付けない',
  !describeRakutenError(JSON.stringify({ errors: [{ code: 'IE001', message: 'bad parameter' }] }), 400).includes('ライセンスキー'));
check('inquiry-hub: passthrough の {ok,error,message} 形式',
  describeRakutenError(JSON.stringify({ ok: false, error: 'RMS_API_ERROR', message: 'HTTP 502' }), 502).includes('RMS_API_ERROR: HTTP 502'));
check('inquiry-hub: {error:{code,message,targets}} 形式 (従来形)',
  describeRakutenError(JSON.stringify({ error: { code: 'E9', message: 'ng', targets: ['a'] } }), 400).includes('E9: ng'));
check('inquiry-hub: 空ボディ・非JSONでも落ちない',
  describeRakutenError('', 401).includes('エラー詳細なし') && describeRakutenError('<html>500</html>', 500).includes('<html>500</html>')
  && describeRakutenError(null, 401).includes('エラー詳細なし'));
check('inquiry-hub: パース済みオブジェクトも受け付ける', describeRakutenError(AUTH_BODY, 401).includes('GA0001'));
check('inquiry-hub: 本文は 220 文字で切る (問い合わせ本文の露出防止)',
  describeRakutenError(JSON.stringify({ errors: [{ code: 'X', message: 'あ'.repeat(500) }] }), 400).length <= 240);

console.log(`\n結果: ${passed} PASS / ${failed} FAIL`);
process.exitCode = failed > 0 ? 1 : 0;
