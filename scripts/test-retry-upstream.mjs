#!/usr/bin/env node
/**
 * test-retry-upstream.mjs — retry-failed-jobs.js の「上流 (取込) → 下流」の規則の試験。
 *   取込 (Qoo10) が失敗した朝に見送った Company DB への送信は、取込の再試行が成功した回に送る。取込がまた失敗した回は送らない (古い・途中の raw を送らない)。
 * 🚨 ここで見るのは規則 (純粋関数) と表の整合だけ。retry の本体 (子プロセスの起動・state の読み書き・通知) と daily-sync.js は試験に無い
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { JOB_DEFINITIONS, RETRY_ORDER, UPSTREAM_OF, upstreamBlock } from '../apps/warehouse/retry-failed-jobs.js';

let ok = 0, ng = 0;
const t = (name, fn) => { try { fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.message || e)); } };
const root = path.join(path.dirname(fileURLToPath(import.meta.url)), '..');
const dailySync = fs.readFileSync(path.join(root, 'apps', 'warehouse', 'daily-sync.js'), 'utf8');
const retryable = JSON.parse(`[${/const RETRYABLE_JOBS = \[([^\]]*)\]/.exec(dailySync)[1].replace(/'/g, '"')}]`);

t('表の整合: 上流も下流も retry の定義と順序にあり、上流が先に走る。上流は daily-sync でも retry の対象', () => {
  for (const [down, up] of Object.entries(UPSTREAM_OF)) {
    for (const j of [down, up]) { assert.ok(Object.hasOwn(JOB_DEFINITIONS, j), `${j} が JOB_DEFINITIONS に無い`); assert.ok(RETRY_ORDER.includes(j), `${j} が RETRY_ORDER に無い`); assert.ok(retryable.includes(j), `${j} が daily-sync の RETRYABLE_JOBS に無い`); }
    assert.ok(RETRY_ORDER.indexOf(up) < RETRY_ORDER.indexOf(down), `${up} が ${down} より後に走る`);
  }
  assert.deepEqual(UPSTREAM_OF, { 'CompanyDB注文(Qoo10)': 'Qoo10' });
});
t('定義と順序の食い違いが無い (定義にあるのに順序に無いジョブは、retry-state に載っても永久に走らない)', () => {
  assert.deepEqual(Object.keys(JOB_DEFINITIONS).filter((j) => !RETRY_ORDER.includes(j)), []);
  assert.deepEqual(RETRY_ORDER.filter((j) => !Object.hasOwn(JOB_DEFINITIONS, j)), []);
  // 'Amazon finance build' は意図された例外 (--month の引数が動的なので未登録のまま。retry 側が unhandled として通知する = retry-failed-jobs.js のコメント)。それ以外に増えたら気づく
  assert.deepEqual(retryable.filter((j) => !Object.hasOwn(JOB_DEFINITIONS, j)), ['Amazon finance build'], 'daily-sync が retry に載せるのに、retry 側に定義が無いジョブが増えた');
});
t('規則: この回で上流を再試行して失敗 → 下流は見送り / 成功 → 走らせる / 上流をこの回で試していない (朝は成功・前の回で復旧済み) → 走らせる / 表に無いジョブ・継承プロパティの名前は対象外', () => {
  assert.equal(upstreamBlock('CompanyDB注文(Qoo10)', [{ name: 'Qoo10', success: false }]), 'Qoo10 再失敗');
  assert.equal(upstreamBlock('CompanyDB注文(Qoo10)', [{ name: 'Qoo10', success: true }]), null);
  assert.equal(upstreamBlock('CompanyDB注文(Qoo10)', [{ name: '楽天未発送アラート', success: false }]), null);
  assert.equal(upstreamBlock('CompanyDB注文(Qoo10)', []), null);
  assert.equal(upstreamBlock('CompanyDB注文(楽天)', [{ name: 'Qoo10', success: false }]), null);
  assert.equal(upstreamBlock('toString', [{ name: 'Qoo10', success: false }]), null);
});
t('daily-sync: Qoo10 の取込が失敗した朝は、送信を「⏭️ skipped」の失敗として結果に載せる (= retry-state に入る)。取込が retry されないモールの見送りは載せない', () => {
  assert.match(dailySync, /results\.push\(\{ name: 'CompanyDB注文\(Qoo10\)', success: false, summary: '⏭️ skipped \(Qoo10 の取込が失敗/);
  for (const mall of ['楽天', 'Amazon', 'auPAY', 'LINEギフト']) assert.equal(new RegExp(`results\\.push\\(\\{ name: 'CompanyDB注文\\(${mall}\\)', success: false`).test(dailySync), false, `${mall} の見送りを retry に載せている (取込が retry されないので、失敗したままの raw を送ってしまう)`);
});

console.log(`\n${ok} ok / ${ng} NG`);
process.exit(ng ? 1 : 0);
