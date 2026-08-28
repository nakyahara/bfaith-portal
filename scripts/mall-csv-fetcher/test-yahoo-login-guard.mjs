#!/usr/bin/env node
/**
 * test-yahoo-login-guard.mjs — SYSTEM 実行を弾くガードのスモーク (2026-08-28 の事故の再発防止)。
 * 実行: node scripts/mall-csv-fetcher/test-yahoo-login-guard.mjs
 */
import { isSystemAccount, openYahooContext } from './lib-yahoo-login.mjs';

let passed = 0, failed = 0;
const check = (name, cond, detail = '') => { if (cond) { console.log(`  ✅ ${name}`); passed++; } else { console.log(`  ❌ ${name} ${detail}`); failed++; } };

console.log('=== SYSTEM 判定 ===');
check('SYSTEM のプロファイルパスで true',
  isSystemAccount({ USERPROFILE: 'C:\\Windows\\system32\\config\\systemprofile' }) === true);
check('区切りが / でも見抜く',
  isSystemAccount({ USERPROFILE: 'C:/Windows/system32/config/systemprofile' }) === true);
check('大文字小文字を問わない',
  isSystemAccount({ USERPROFILE: 'C:\\WINDOWS\\SYSTEM32\\CONFIG\\SYSTEMPROFILE' }) === true);
check('USERNAME=SYSTEM でも true', isSystemAccount({ USERNAME: 'SYSTEM' }) === true);
check('マシンアカウント (<COMPUTERNAME>$) でも true',
  isSystemAccount({ USERNAME: 'MINIPC$', COMPUTERNAME: 'minipc' }) === true);
check('bfaith は false',
  isSystemAccount({ USERPROFILE: 'C:\\Users\\bfaith', USERNAME: 'bfaith', COMPUTERNAME: 'MINIPC' }) === false);
check('env が空でも false (誤検知しない = 通常実行を止めない)', isSystemAccount({}) === false);
check('COMPUTERNAME 未設定なら $ 付きユーザー名だけでは判定しない',
  isSystemAccount({ USERNAME: 'foo$' }) === false);

console.log('=== 実際に開こうとしても止まるか ===');
{
  const saved = process.env.USERPROFILE;
  process.env.USERPROFILE = 'C:\\Windows\\system32\\config\\systemprofile';
  let msg = null;
  try { await openYahooContext(); } catch (e) { msg = e.message; }
  process.env.USERPROFILE = saved;
  // ブラウザを起動する前に落ちること (起動してしまうとその時点でプロファイルが壊れる)
  check('SYSTEM ならブラウザを起動せず throw', /^SYSTEM_ACCOUNT:/.test(msg || ''), msg?.slice(0, 60));
  check('復旧手順が読める文言で出る', /2FA|再ログイン/.test(msg || ''));
}

console.log(`\n結果: ${passed} PASS / ${failed} FAIL`);
process.exitCode = failed > 0 ? 1 : 0;
