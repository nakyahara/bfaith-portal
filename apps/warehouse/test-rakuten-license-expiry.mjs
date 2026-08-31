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
check('存在しない暦日は拒否 (翌月へ正規化させない)', parseRmsExpiry('2026-02-30') === null && parseRmsExpiry('2026-13-01') === null && parseRmsExpiry('2027-02-29') === null);
check('うるう年 2/29 は通る', parseRmsExpiry('2028-02-29') === Date.parse('2028-02-29T23:59:59+09:00'));
check('時分秒の範囲外は拒否', parseRmsExpiry('2026-11-28T24:00:00') === null && parseRmsExpiry('2026-11-28T23:60:00') === null);
check('小文字 z / +0900 形式も受ける',
  parseRmsExpiry('2026-11-28t14:59:59z') === Date.parse('2026-11-28T14:59:59Z')
  && parseRmsExpiry('2026-11-28T23:59:59+0900') === Date.parse('2026-11-28T23:59:59+09:00'));
check('ミリ秒つきも受ける', parseRmsExpiry('2026-11-28T23:59:59.123') === Date.parse('2026-11-28T23:59:59+09:00'));
check('前後の余計な文字列は拒否 (部分一致させない)', parseRmsExpiry('exp: 2026-11-28') === null && parseRmsExpiry('2026-11-28 ちょっとメモ') === null);
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

console.log('\n── 異常な本文でも壊れない (Codex R1) ──');
check('制御文字・改行は潰す (ログ1行を壊さない)',
  describeRakutenError({ errors: [{ code: 'XY', message: 'a\nb\tc' }] }, 400) === 'XY: a b c'
  , JSON.stringify(describeRakutenError({ errors: [{ code: 'XY', message: 'a\nb\tc' }] }, 400)));
check('warehouse 側も制御文字を潰す', describeRmsError({ errors: [{ code: 'A\nB', message: 'xy' }] }).code === 'A B');
check('64KB 超の本文は解析せず先頭だけ', describeRakutenError('x'.repeat(70000), 500).length < 200);
check('errors[] に非オブジェクトが混ざっても最初の object を拾う',
  describeRakutenError({ errors: [null, 'x', { code: 'GA0001', message: 'Un-Authorised' }] }, 401).includes('GA0001'));
check('巨大 errors[] でも落ちない', describeRakutenError({ errors: Array.from({ length: 50000 }, () => null).concat([{ code: 'Z', message: 'z' }]) }, 400).includes('Z'));
const circular = { error: { code: 'E', message: 'm', targets: {} } };
circular.error.targets.self = circular;   // 循環参照 (JSON.stringify は throw する)
check('targets が循環参照でも throw しない', describeRakutenError(circular, 400).includes('E: m'));
check('targets は配列の先頭3件・primitive のみ',
  describeRakutenError({ error: { code: 'E', message: 'm', targets: ['a', 'b', 'c', 'd'] } }, 400).includes('targets=a,b,c')
  && !describeRakutenError({ error: { code: 'E', message: 'm', targets: ['a', 'b', 'c', 'd'] } }, 400).includes('d'));
check('targets に BigInt が混ざっても落ちない', describeRakutenError({ error: { code: 'E', message: 'm', targets: [10n] } }, 400).includes('targets=10'));

console.log(`\n結果: ${passed} PASS / ${failed} FAIL`);
process.exitCode = failed > 0 ? 1 : 0;
