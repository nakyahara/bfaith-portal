// inquiry-hub 宛先ドメインの事前確認スモーク (2026-08-27): 打ち間違いは止める・DNS不通では止めない
// 使い方: node apps/inquiry-hub/smoke-mx.mjs   (DBもネットワークも使わない = DNSはスタブ)
import { checkRecipientDomain, domainOf, clearMxCache } from './mx-check.js';

let passed = 0, failed = 0;
const check = (name, cond, detail) => {
  if (cond) { passed++; console.log(`  PASS ${name}`); }
  else { failed++; console.error(`  FAIL ${name}${detail ? ' — ' + detail : ''}`); }
};

/** DNSスタブ。呼ばれた回数も数えてキャッシュの効きを見る */
function stub({ mx, a, aaaa } = {}) {
  const calls = { mx: 0, a: 0, aaaa: 0 };
  const answer = (kind, v) => {
    calls[kind]++;
    if (v instanceof Error) return Promise.reject(v);
    return Promise.resolve(v || []);
  };
  return {
    calls,
    resolveMx: () => answer('mx', mx),
    resolve4: () => answer('a', a),
    resolve6: () => answer('aaaa', aaaa),
  };
}
const err = (code) => Object.assign(new Error(`stub ${code}`), { code });

console.log('1. ドメインの取り出し');
{
  check('アドレスからドメインを取る', domainOf('taro@example.com') === 'example.com');
  check('大文字は小文字に', domainOf('Taro@Example.CO.JP') === 'example.co.jp');
  check('@が無い/末尾が@ は null', domainOf('example.com') === null && domainOf('a@') === null);
  check('ドットの無いドメインは null (判定材料にしない)', domainOf('a@localhost') === null);
}

console.log('2. 受け取れるドメイン');
{
  clearMxCache();
  const r = await checkRecipientDomain('taro@ok-domain.test',
    { resolver: stub({ mx: [{ exchange: 'mail.ok-domain.test', priority: 10 }] }), useCache: false });
  check('MXがあれば通す', r.ok === true && !r.skipped);

  clearMxCache();
  const r2 = await checkRecipientDomain('taro@a-only.test',
    { resolver: stub({ mx: [], a: ['192.0.2.1'] }), useCache: false });
  check('MXが無くてもAがあれば通す (implicit MX)', r2.ok === true);

  clearMxCache();
  const r3 = await checkRecipientDomain('taro@v6-only.test',
    { resolver: stub({ mx: err('ENODATA'), a: err('ENODATA'), aaaa: ['2001:db8::1'] }), useCache: false });
  check('AAAAだけでも通す', r3.ok === true);
}

console.log('3. 止めるケース (打ち間違い)');
{
  clearMxCache();
  const r = await checkRecipientDomain('taro@gmial.test',
    { resolver: stub({ mx: err('ENOTFOUND'), a: err('ENOTFOUND'), aaaa: err('ENOTFOUND') }), useCache: false });
  check('ドメイン自体が無ければ止める', r.ok === false && r.reason.includes('見つかりません'));
  check('画面にそのまま出せる案内文', r.reason.includes('打ち間違い'));

  clearMxCache();
  const r2 = await checkRecipientDomain('taro@no-mail.test',
    { resolver: stub({ mx: [], a: [], aaaa: [] }), useCache: false });
  check('メールサーバーの登録がまったく無ければ止める',
    r2.ok === false && r2.reason.includes('メールサーバーが登録されていません'));

  clearMxCache();
  const r3 = await checkRecipientDomain('taro@null-mx.test',
    { resolver: stub({ mx: [{ exchange: '.', priority: 0 }] }), useCache: false });
  check('null MX (RFC 7505 = メールを受け取らない宣言) は止める',
    r3.ok === false && r3.reason.includes('メールを受け取らない設定'));
}

console.log('4. 判断できないときは止めない (fail-open)');
{
  clearMxCache();
  const r = await checkRecipientDomain('taro@slow.test',
    { resolver: stub({ mx: err('ETIMEOUT') }), useCache: false });
  check('DNSタイムアウトでは止めない', r.ok === true && r.skipped === true);

  clearMxCache();
  const r2 = await checkRecipientDomain('taro@servfail.test',
    { resolver: stub({ mx: err('SERVFAIL') }), useCache: false });
  check('SERVFAILでも止めない', r2.ok === true && r2.skipped === true);

  clearMxCache();
  const r3 = await checkRecipientDomain('taro@half.test',
    { resolver: stub({ mx: err('ENODATA'), a: err('ETIMEOUT'), aaaa: err('ETIMEOUT') }), useCache: false });
  check('MXは無いがA/AAAAの確認に失敗 → 止めない', r3.ok === true && r3.skipped === true);

  clearMxCache();
  const r3b = await checkRecipientDomain('taro@mixed.test',
    { resolver: stub({ mx: err('ENODATA'), a: err('ENODATA'), aaaa: err('ETIMEOUT') }), useCache: false });
  check('Aは「無い」がAAAAが確認不能なら止めない (言い切れないものは通す)',
    r3b.ok === true && r3b.skipped === true);

  clearMxCache();
  const broken = await checkRecipientDomain('taro@broken.test', {
    resolver: { resolveMx: () => { throw new Error('リゾルバが壊れています'); },
      resolve4: () => { throw new Error('x'); }, resolve6: () => { throw new Error('x'); } },
    useCache: false,
  });
  check('リゾルバが例外を投げても落ちず・止めない', broken.ok === true && broken.skipped === true);

  clearMxCache();
  const slow = await checkRecipientDomain('taro@hang.test', {
    resolver: { resolveMx: () => new Promise(() => {}), resolve4: () => new Promise(() => {}), resolve6: () => new Promise(() => {}) },
    overallTimeoutMs: 50, useCache: false,
  });
  check('DNSが返ってこなくても全体タイムアウトで通す (送信ボタンを固めない)',
    slow.ok === true && slow.skipped === true && slow.why.includes('50ms'));

  const r4 = await checkRecipientDomain('こわれたアドレス', { resolver: stub({}) });
  check('アドレスの形が取れないものは判定しない (形式検証は compose.js の役目)',
    r4.ok === true && r4.skipped === true);
}

console.log('5. キャッシュ');
{
  clearMxCache();
  const s = stub({ mx: [{ exchange: 'mail.cached.test', priority: 10 }] });
  await checkRecipientDomain('a@cached.test', { resolver: s });
  await checkRecipientDomain('b@cached.test', { resolver: s });
  check('同じドメインは引き直さない', s.calls.mx === 1);

  clearMxCache();
  const s2 = stub({ mx: err('ETIMEOUT') });
  await checkRecipientDomain('a@retry.test', { resolver: s2 });
  await checkRecipientDomain('b@retry.test', { resolver: s2 });
  check('確認できなかった (fail-open) 結果はキャッシュしない', s2.calls.mx === 2);

  clearMxCache();
  const s3 = stub({ mx: [{ exchange: 'mail.ttl.test', priority: 10 }] });
  await checkRecipientDomain('a@ttl.test', { resolver: s3 });
  await checkRecipientDomain('b@ttl.test', { resolver: s3, now: Date.now() + 11 * 60 * 1000 });
  check('10分を過ぎたら引き直す', s3.calls.mx === 2);

  // 「受け取れない」結果は短命にする (DNSを直したのに10分止まり続けるのを避ける)
  clearMxCache();
  const s4 = stub({ mx: err('ENOTFOUND'), a: err('ENOTFOUND'), aaaa: err('ENOTFOUND') });
  const t0 = Date.now();
  await checkRecipientDomain('a@ng-cache.test', { resolver: s4, now: t0 });
  await checkRecipientDomain('b@ng-cache.test', { resolver: s4, now: t0 + 30 * 1000 });
  check('否定結果も短時間はキャッシュする (連打でDNSを叩かない)', s4.calls.mx === 1);
  await checkRecipientDomain('c@ng-cache.test', { resolver: s4, now: t0 + 61 * 1000 });
  check('否定結果は1分で引き直す (直したらすぐ送れる)', s4.calls.mx === 2);
}

console.log('6. 無効化スイッチ');
{
  clearMxCache();
  process.env.INQUIRY_HUB_MX_CHECK = 'off';
  const r = await checkRecipientDomain('taro@gmial.test');
  check('INQUIRY_HUB_MX_CHECK=off で丸ごと止められる (DNSが使えない環境用)',
    r.ok === true && r.skipped === true && r.why.includes('無効'));
  delete process.env.INQUIRY_HUB_MX_CHECK;
}

console.log(`\n${failed === 0 ? 'OK' : 'NG'}: ${passed} PASS / ${failed} FAIL`);
process.exitCode = failed === 0 ? 0 : 1;
