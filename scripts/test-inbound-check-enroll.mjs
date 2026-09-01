/**
 * 端末登録 (登録コード方式) のテスト
 *
 * 実行: node scripts/test-inbound-check-enroll.mjs
 * 検証: 発行→引き換え→端末Cookieが有効 / 期限切れ / 使用済み / 打ち間違い回数 / 形式不正 /
 *       引き換えても管理系は触れない (端末の権限境界) / 総当たりのレート制限
 */
import fs from 'fs';
import os from 'os';
import path from 'path';

if (!process.env.DATA_DIR) process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'ic-enroll-'));
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const db = await import('../apps/inbound-check/db.js');
const {
  getDB, createEnrollCode, redeemEnrollCode, countEnrollAttempt, listActiveEnrollCodes,
  verifyDevice, listDevices, revokeDevice, ENROLL_MAX_ATTEMPTS,
  checkEnrollRate, recordEnrollAttempt, ENROLL_RATE_PER_IP, ENROLL_RATE_GLOBAL,
} = db;

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };
const throwsWith = (fn, re, l) => { try { fn(); ok(false, `${l} (例外なし)`); } catch (e) { ok(re.test(e.message), `${l}: ${e.message}`); } };

console.log('DATA_DIR =', process.env.DATA_DIR);

console.log('\n[1] 発行 → 引き換え');
{
  const c = createEnrollCode('入荷iPad1', 'admin@example.com');
  ok(/^\d{6}$/.test(c.code), `6桁の数字 (${c.code})`);
  ok(c.label === '入荷iPad1' && Date.parse(c.expiresAt) > Date.now(), 'ラベルと有効期限');
  ok(listActiveEnrollCodes().length === 1, '未使用の一覧に出る');
  const r = redeemEnrollCode(c.code);
  ok(r.ok && typeof r.token === 'string' && r.label === '入荷iPad1', '引き換えでトークンが返る');
  const d = verifyDevice(r.token);
  ok(d && d.label === '入荷iPad1', 'そのトークンで端末として認識される');
  ok(String(d.created_by).startsWith('enroll:'), `発行者が記録される (${d.created_by})`);
  ok(listActiveEnrollCodes().length === 0, '使ったコードは一覧から消える');
}

console.log('\n[2] 使い回しできない');
{
  const c = createEnrollCode('入荷iPad2', 'admin@example.com');
  ok(redeemEnrollCode(c.code).ok, '1回目は成功');
  const r2 = redeemEnrollCode(c.code);
  ok(!r2.ok && r2.error === 'used', '2回目は used で拒否');
  ok(listDevices().filter(d => d.label === '入荷iPad2').length === 1, '端末は1つしか作られない');
}

console.log('\n[3] 期限切れ');
{
  const c = createEnrollCode('期限切れiPad', 'admin@example.com');
  // 期限を過去にする (時間を待たずに検証)
  getDB().prepare("UPDATE f_inbound_check_enroll_codes SET expires_at = '2020-01-01T00:00:00.000Z' WHERE used_at IS NULL").run();
  const r = redeemEnrollCode(c.code);
  ok(!r.ok && r.error === 'expired', '期限切れは拒否');
  ok(listActiveEnrollCodes().length === 0, '期限切れは一覧に出ない');
}

console.log('\n[4] 打ち間違い・形式不正');
{
  const c = createEnrollCode('入荷iPad3', 'admin@example.com');
  for (const bad of ['', '12345', '1234567', 'abcdef', '12 34 56']) {
    const r = redeemEnrollCode(bad);
    ok(!r.ok && r.error === 'bad_code', `形式不正を拒否 (${JSON.stringify(bad)})`);
  }
  // 存在しないコード
  const wrong = String((Number(c.code) + 1) % 1000000).padStart(6, '0');
  ok(!redeemEnrollCode(wrong).ok, '違うコードは拒否');
  // 総当たり: 同じコードへの失敗を数え、上限で無効化
  for (let i = 0; i < ENROLL_MAX_ATTEMPTS; i++) countEnrollAttempt(c.code);
  const r = redeemEnrollCode(c.code);
  ok(!r.ok && r.error === 'too_many', `${ENROLL_MAX_ATTEMPTS}回ミスすると正しいコードでも無効`);
  ok(listActiveEnrollCodes().length === 0, '無効化されたコードは一覧に出ない');
}

console.log('\n[5] 総当たり (存在しないコードを順に試す)');
{
  const c = createEnrollCode('総当たりテスト', 'admin@example.com');
  const IP = '203.0.113.9';
  // ⚠旧実装はここが素通りだった: countEnrollAttempt は「実在する行」しか数えないので、
  //   当たり以外を叩き続ける限りカウントが増えず、100万通りを試し放題だった
  let blocked = 0;
  for (let i = 0; i < ENROLL_RATE_PER_IP + 3; i++) {
    const guess = String(i).padStart(6, '0');
    const gate = checkEnrollRate({ ip: IP });
    if (!gate.allowed) { blocked++; recordEnrollAttempt({ ip: IP, ok: false }); continue; }
    recordEnrollAttempt({ ip: IP, ok: redeemEnrollCode(guess).ok });
  }
  ok(blocked >= 3, `存在しないコードの連打でも止まる (${blocked}回ブロック)`);
  ok(!checkEnrollRate({ ip: IP }).allowed, '打ち止め後は当たりのコードでも受け付けない');
  ok(checkEnrollRate({ ip: '198.51.100.1' }).allowed, '別の相手はまだ試せる (巻き添えにしない)');
  // 分散して来られても全体上限で止まる
  for (let i = 0; i < ENROLL_RATE_GLOBAL; i++) recordEnrollAttempt({ ip: `198.51.100.${i % 200}`, ok: false });
  ok(!checkEnrollRate({ ip: '198.51.100.77' }).allowed, 'IP を変えて分散しても全体上限で止まる');
  // 後片付け (時間が経って記録が消えた状態を作る)
  getDB().prepare('DELETE FROM f_inbound_check_enroll_attempts').run();
  ok(checkEnrollRate({ ip: IP }).allowed, '時間が経てば (記録が消えれば) また試せる');
  ok(redeemEnrollCode(c.code).ok, '正しいコードは引き換えられる');
}

console.log('\n[5b] コードの保存形');
{
  const crypto = await import('node:crypto');
  const c = createEnrollCode('保存形テスト', 'admin@example.com');
  const rows = getDB().prepare('SELECT code_hash FROM f_inbound_check_enroll_codes WHERE used_at IS NULL').all();
  const plainSha = crypto.createHash('sha256').update(c.code).digest('hex');
  ok(!rows.some(r => r.code_hash === c.code), 'コードそのものは保存しない');
  // 6桁 = 100万通りなので、ソルト無しの sha256 だと DB が漏れた時点で逆算できる
  ok(!rows.some(r => r.code_hash === plainSha), 'ソルト無し sha256 では保存しない (総当たりで逆算できてしまう)');
}

console.log('\n[5c] 有効なコードは同時に1つだけ');
{
  const a = createEnrollCode('iPad-A', 'admin@example.com');
  const b = createEnrollCode('iPad-B', 'admin@example.com');
  ok(listActiveEnrollCodes().length === 1, '新しく発行すると古い未使用コードは無効になる');
  ok(!redeemEnrollCode(a.code).ok, '古いコードはもう使えない');
  ok(redeemEnrollCode(b.code).ok, '新しいコードは使える');
}

console.log('\n[6] 端末の失効');
{
  const c = createEnrollCode('失効テスト', 'admin@example.com');
  const r = redeemEnrollCode(c.code);
  const d = verifyDevice(r.token);
  ok(!!d, '登録直後は有効');
  ok(revokeDevice(d.id) && verifyDevice(r.token) === null, '失効させると使えなくなる');
}

console.log('\n[7] 入力検証');
{
  throwsWith(() => createEnrollCode('', 'admin'), /1〜40/, '端末名なしは拒否');
  throwsWith(() => createEnrollCode('あ'.repeat(41), 'admin'), /1〜40/, '長すぎる端末名は拒否');
}

console.log(`\n${pass} PASS / ${fail} FAIL`);
process.exitCode = fail ? 1 : 0;
