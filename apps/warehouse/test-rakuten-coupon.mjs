#!/usr/bin/env node
/**
 * test-rakuten-coupon.mjs — PR-C3 スモークテスト (クーポンlib、ネットワーク不使用)
 * 実行: node apps/warehouse/test-rakuten-coupon.mjs
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import {
  MONTHLY_COUPON_DEFAULTS, xmlEscape, buildIssueXml, buildDeleteXml, parseCouponResult,
  monthlyCouponParams, ensureCouponRegistry, getRegisteredCoupon, recordIssuedCoupon,
  maskCode, maskUrl,
} from './rakuten-coupon-lib.js';

let passed = 0, failed = 0;
function check(name, cond, detail = '') {
  if (cond) { console.log(`  ✅ ${name}`); passed++; }
  else { console.log(`  ❌ ${name} ${detail}`); failed++; }
}

console.log('=== 1. XMLビルダー (公式仕様の完全形) ===');
{
  const params = {
    ...MONTHLY_COUPON_DEFAULTS,
    couponStartDate: '2026-08-01T00:00:00+09:00',
    couponEndDate: '2026-10-31T23:59:59+09:00',
  };
  const r = buildIssueXml(params);
  check('正常パラメータでXML生成', r.ok, JSON.stringify(r.errors));
  // 公式サンプルの要素順 (2026-07-18 実測で発行成功した順序) を厳密に検証
  const order = ['couponName', 'couponCaption', 'couponStartDate', 'couponEndDate', 'issueCount',
    'itemType', 'discountType', 'discountFactor', 'memberAvailMaxCount', 'purchaseHistoryCond',
    'multiRankCond', 'genderCond', 'ageRangeCond', 'birthmonthCond', 'multiPrefectureCond',
    'combineFlag', 'displayFlag'];
  let lastIdx = -1, orderOk = true;
  for (const el of order) {
    const idx = r.xml.indexOf(`<${el}>`);
    if (idx < 0 || idx < lastIdx) { orderOk = false; break; }
    lastIdx = idx;
  }
  check('要素順が公式サンプルどおり', orderOk);
  check('必須の固定条件が全部入り (purchaseHistoryCond/genderCond/ageRangeCond/birthmonthCond/multiPrefectureCond)',
    r.xml.includes('<type>0</type>') && r.xml.includes('<genderCond>NONE</genderCond>')
    && r.xml.includes('<lowerBound>0</lowerBound>') && r.xml.includes('<birthmonthCond>0</birthmonthCond>')
    && r.xml.includes('<prefectureCond>NONE</prefectureCond>'));
  check('ルート構造 request>couponIssueRequest>coupon', /<request>\s*<couponIssueRequest>\s*<coupon>/.test(r.xml));
  check('キャプションの改行が保持される', r.xml.includes('5％OFFクーポンです。\n他のクーポン'));

  const esc = buildIssueXml({ ...params, couponName: 'A&B <5%> "特価"' });
  check('XMLエスケープ (& < > ")', esc.ok && esc.xml.includes('A&amp;B &lt;5%&gt; &quot;特価&quot;'));
  check('xmlEscape 単体', xmlEscape(`&<>"'`) === '&amp;&lt;&gt;&quot;&apos;');
}

console.log('=== 2. XMLビルダー検証 (fail-fast) ===');
{
  const base = {
    ...MONTHLY_COUPON_DEFAULTS,
    couponStartDate: '2026-08-01T00:00:00+09:00',
    couponEndDate: '2026-10-31T23:59:59+09:00',
  };
  check('couponName 60文字超は拒否', !buildIssueXml({ ...base, couponName: 'あ'.repeat(61) }).ok);
  check('couponName 空は拒否', !buildIssueXml({ ...base, couponName: '' }).ok);
  check('オフセットなし日時は拒否', !buildIssueXml({ ...base, couponStartDate: '2026-08-01T00:00:00' }).ok);
  check('終了が開始+5分未満は拒否', !buildIssueXml({ ...base, couponEndDate: '2026-08-01T00:04:59+09:00' }).ok);
  check('定率で discountFactor=0 は拒否', !buildIssueXml({ ...base, discountFactor: 0 }).ok);
  check('定率で discountFactor=100 は拒否', !buildIssueXml({ ...base, discountFactor: 100 }).ok);
  check('displayFlag=2 は拒否', !buildIssueXml({ ...base, displayFlag: 2 }).ok);
  check('delete: 不正コードは拒否', !buildDeleteXml('has space').ok && !buildDeleteXml('<x>').ok);
  check('delete: 正常コードはXML生成', buildDeleteXml('PFL7-2MRH-FBGF-A39W').ok);
}

console.log('=== 3. レスポンスパース (公式サンプル) ===');
{
  const okXml = `<?xml version="1.0" encoding="UTF-8"?><result><status><interfaceId>coupon.issue</interfaceId><systemStatus>OK</systemStatus><message>OK</message><requestId>x</requestId></status><coupon><couponCode>PFL7-2MRH-FBGF-A39W</couponCode><pcGetUrl>https://coupon.rakuten.co.jp/getCoupon?getkey=Mk1SSC1QRkw3LUZCR0YtQTM5Vw--&amp;rt=</pcGetUrl></coupon></result>`;
  const ok = parseCouponResult(okXml);
  check('正常系: ok=true + couponCode + pcGetUrl (&amp;復号)', ok.ok && ok.couponCode === 'PFL7-2MRH-FBGF-A39W' && ok.pcGetUrl.endsWith('&rt='));

  const errXml = `<?xml version="1.0" encoding="UTF-8"?><result><status><interfaceId>coupon.issue</interfaceId><systemStatus>OK</systemStatus><message>OK</message></status><errors><error><code>COUPON_EE03-002</code><message>discountType.out_of_bounds</message></error></errors></result>`;
  const err = parseCouponResult(errXml);
  check('エラー系: systemStatus OK でも errors があれば ok=false', !err.ok && err.errors[0].code === 'COUPON_EE03-002');

  const wrongFormat = `<?xml version="1.0" encoding="UTF-8"?><result><status><interfaceId>coupon.issue</interfaceId><systemStatus>NG</systemStatus><message>Request data is wrong format</message></status></result>`;
  const wf = parseCouponResult(wrongFormat);
  check('wrong format 400: ok=false + message保持', !wf.ok && wf.message === 'Request data is wrong format');

  check('mask: コード/URLが伏せられる', maskCode('PFL7-2MRH') === 'PFL***' && maskUrl('https://x/getCoupon?getkey=SECRET&rt=') === 'https://x/getCoupon?getkey=***&rt=');

  // search の requests エコー罠 (実機実測 2026-07-18): status 内の couponCode を拾わない
  const searchEcho = `<?xml version="1.0"?><result><status><interfaceId>coupon.search</interfaceId><systemStatus>OK</systemStatus><message>OK</message><requests><couponName/><couponCode>ZBKX-TEST-CODE-0001</couponCode></requests></status><allCount>0</allCount><coupons/></result>`;
  const echo = parseCouponResult(searchEcho);
  check('search 0件: requests エコーの couponCode を拾わず allCount=0', echo.couponCode === null && echo.allCount === 0);
  const searchHit = `<?xml version="1.0"?><result><status><requests><couponCode>ZBKX-A</couponCode></requests><systemStatus>OK</systemStatus><message>OK</message></status><allCount>1</allCount><coupons><coupon><couponCode>ZBKX-A</couponCode><pcGetUrl>https://coupon.rakuten.co.jp/getCoupon?getkey=k&amp;rt=</pcGetUrl></coupon></coupons></result>`;
  const hit = parseCouponResult(searchHit);
  check('search 1件: coupons 側の couponCode + allCount=1', hit.couponCode === 'ZBKX-A' && hit.allCount === 1);
  check('issue レスポンス (allCountなし) は allCount=null', ok.allCount === null);
}

console.log('=== 4. 月次パラメータ計算 (JST壁時計) ===');
{
  // 前月末に翌月分を発行する標準ケース
  const p = monthlyCouponParams('2026-08', '2026-07-25T00:00:00.000Z');
  check('開始=月初00:00 JST', p.couponStartDate === '2026-08-01T00:00:00+09:00');
  check('終了=翌々月末 23:59:59 JST', p.couponEndDate === '2026-10-31T23:59:59+09:00');
  // 年跨ぎ (12月 → 翌々月=翌年2月末、うるう年でない2027)
  const dec = monthlyCouponParams('2026-12', '2026-11-25T00:00:00.000Z');
  check('12月分: 終了=2027-02-28 (年跨ぎ+非うるう)', dec.couponEndDate === '2027-02-28T23:59:59+09:00');
  // うるう年 (2027-12 → 2028-02-29)
  const leap = monthlyCouponParams('2027-12', '2027-11-25T00:00:00.000Z');
  check('うるう年: 終了=2028-02-29', leap.couponEndDate === '2028-02-29T23:59:59+09:00');
  // 当月途中の発行 → 開始=now+61分 (API制約: 最短60分後)
  const mid = monthlyCouponParams('2026-07', '2026-07-18T03:00:00.000Z'); // 12:00 JST
  check('当月途中: 開始=now+61分 (13:01 JST)', mid.couponStartDate === '2026-07-18T13:01:00+09:00', mid.couponStartDate);
  check('当月途中でも終了は翌々月末', mid.couponEndDate === '2026-09-30T23:59:59+09:00');
  // 30日超先はエラー
  let farErr = false;
  try { monthlyCouponParams('2026-10', '2026-07-18T00:00:00.000Z'); } catch { farErr = true; }
  check('月初が30日超先は拒否 (API制約)', farErr);
  // 過去月はエラー
  let pastErr = false;
  try { monthlyCouponParams('2026-01', '2026-07-18T00:00:00.000Z'); } catch { pastErr = true; }
  check('有効期限が過去の月は拒否', pastErr);
  let fmtErr = false;
  try { monthlyCouponParams('2026-13'); } catch { fmtErr = true; }
  check('YYYY-MM 形式違反は拒否', fmtErr);
  // 生成パラメータがそのまま buildIssueXml を通る
  check('monthlyCouponParams → buildIssueXml が ok', buildIssueXml(p).ok);
}

console.log('=== 5. 台帳 (rakuten_campaign_coupons) ===');
{
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'rcoupon-smoke-'));
  const db = new Database(path.join(tmp, 'warehouse.db'));
  ensureCouponRegistry(db);
  ensureCouponRegistry(db); // 冪等
  check('未発行は null', getRegisteredCoupon(db, '2026-08') === null);
  recordIssuedCoupon(db, {
    month: '2026-08', couponCode: 'TEST-CODE-0001', pcGetUrl: 'https://coupon.rakuten.co.jp/getCoupon?getkey=abc&rt=',
    couponStart: '2026-08-01T00:00:00+09:00', couponEnd: '2026-10-31T23:59:59+09:00', nowIso: '2026-07-25T00:00:00.000Z',
  });
  const row = getRegisteredCoupon(db, '2026-08');
  check('記録した行が読める (フル値保存)', row?.coupon_code === 'TEST-CODE-0001' && row?.pc_get_url.includes('getkey=abc'));
  let dup = false;
  try {
    recordIssuedCoupon(db, { month: '2026-08', couponCode: 'TEST-CODE-0002', pcGetUrl: 'x', couponStart: 'a', couponEnd: 'b' });
  } catch { dup = true; }
  check('同月の二重記録は PK が拒否 (二重発行防止)', dup);
  let badMonth = false;
  try {
    recordIssuedCoupon(db, { month: '202608', couponCode: 'TEST-CODE-0003', pcGetUrl: 'x', couponStart: 'a', couponEnd: 'b' });
  } catch { badMonth = true; }
  check('month 形式は CHECK 制約で拒否', badMonth);
  db.close();
  fs.rmSync(tmp, { recursive: true, force: true });
}

console.log(`\n結果: ${passed} PASS / ${failed} FAIL`);
process.exit(failed > 0 ? 1 : 0);
