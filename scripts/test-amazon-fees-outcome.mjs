#!/usr/bin/env node
/**
 * test-amazon-fees-outcome.mjs — Amazon手数料の取得の「終了コードと最後の 1 行」(apps/warehouse/amazon-fees-outcome.js) の試験。
 *   1 SKU の入力の誤り (ClientError) でステップ全体を落とさない・でも黙って緑にもしない / 通信・サーバ側の失敗は今までどおり落とす / 入力の誤りが多すぎれば落とす
 * 🚨 ここで見るのは判定 (純粋関数) と、fetch-amazon-fees.js がそれを最後の行と終了コードに使っていること (ソースの形)。SP-API を叩く本体は試験に無い
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { summarizeFeeOutcome, splitErrors, isInputError, INPUT_FAIL_MIN_LIMIT } from '../apps/warehouse/amazon-fees-outcome.js';

let ok = 0, ng = 0;
const t = (name, fn) => { try { fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.message || e)); } };
const ce = (sku) => ({ sku, error: 'ClientError', errorMsg: 'There is an client-side error. Please verify your inputs.' });
const many = (n, mk) => Array.from({ length: n }, (_, i) => mk(`sku-${i}`));

t('🚨 9/18〜19 に実際に起きた形: 取り直し 0・見送り 2,958・ClientError 1 件 → 落とさない (exit 0)。最後の行は ⚠️ で SKU と「やり直しても直らない」「何を確かめるか」が読める', () => {
  const o = summarizeFeeOutcome({ refreshed: 0, skipped: 2958, inputErrors: [ce('pr_1272115_f_20240901_22186309_0001')], hardErrors: [] });
  assert.deepEqual([o.exitCode, o.level, o.attempted], [0, 'warn', 1]);
  assert.match(o.line, /^⚠️ Amazon手数料: 取り直し 0 \/ 期限内で見送り 2958 \/ 手数料が取れない SKU 1 件/);
  assert.match(o.line, /pr_1272115_f_20240901_22186309_0001 \(ClientError\)/);
  assert.match(o.line, /やり直しても直らない/);
  assert.match(o.line, /Amazon の出品 \(ASIN・価格\) を確かめる/);
});
t('何も失敗していなければ ✅ (exit 0)。取り直しが 0 件でも失敗ではない (再試行の回は朝の回が済ませた後なので 0 が普通)', () => {
  assert.deepEqual([summarizeFeeOutcome({ refreshed: 412, skipped: 2500 }).exitCode, summarizeFeeOutcome({ refreshed: 412, skipped: 2500 }).line], [0, '✅ Amazon手数料: 取り直し 412 / 期限内で見送り 2500']);
  assert.equal(summarizeFeeOutcome({ refreshed: 0, skipped: 2958 }).level, 'ok');
});
t('🚨 通信・サーバ側の失敗は 1 件でも今までどおり落とす (exit 1 = 自動再試行の対象): batch ごと落ちた・ServerError・応答の形が違う・SKU と突き合わせられない', () => {
  for (const hard of [[{ sku: 'a', error: 'fetch failed' }], [{ sku: 'a', error: 'ServerError' }], [{ identifier: 'all', error: 'Response is not an array' }], [{ identifier: 'x', error: 'Cannot match identifier to original SKU' }]]) {
    const o = summarizeFeeOutcome({ refreshed: 400, skipped: 0, inputErrors: [ce('b')], hardErrors: hard });
    assert.deepEqual([o.exitCode, o.level], [1, 'fail'], JSON.stringify(hard));
    assert.match(o.line, /^❌ Amazon手数料: .*取得に失敗 1 件/);
    assert.match(o.line, /入力の誤りで取れない SKU 1 件/);
  }
});
t('🚨 入力の誤りが多すぎれば落とす (個々の SKU ではなく設定の問題を疑う): 上限 = 5 件 と 試した数の 5% の大きいほう', () => {
  assert.equal(summarizeFeeOutcome({ refreshed: 0, skipped: 3000, inputErrors: many(INPUT_FAIL_MIN_LIMIT, ce) }).exitCode, 0, '5 件ちょうどは上限の中');
  const over = summarizeFeeOutcome({ refreshed: 0, skipped: 3000, inputErrors: many(INPUT_FAIL_MIN_LIMIT + 1, ce) });
  assert.deepEqual([over.exitCode, over.limit], [1, 5]);
  assert.match(over.line, /上限 5 件を超えた = 個々の SKU ではなく設定の問題を疑う/);
  assert.equal(summarizeFeeOutcome({ refreshed: 380, skipped: 0, inputErrors: many(20, ce) }).exitCode, 0, '400 件試して 20 件 = 5% ちょうどは上限の中');
  assert.equal(summarizeFeeOutcome({ refreshed: 379, skipped: 0, inputErrors: many(21, ce) }).exitCode, 1);
  assert.equal(summarizeFeeOutcome({ refreshed: 0, skipped: 0, inputErrors: many(50, ce) }).exitCode, 1, '全部が ClientError (マーケットプレイスの取り違えなど)');
  assert.match(summarizeFeeOutcome({ refreshed: 0, skipped: 0, inputErrors: many(50, ce) }).line, /sku-0 \(ClientError\), sku-1 \(ClientError\), sku-2 \(ClientError\) ほか 47 件/);
});
t('分類: 入力の誤り = SKU の分かる ClientError / No ASIN だけ。SKU の無い失敗・知らない種類・継承プロパティの名前は「それ以外」(= 落とす側)', () => {
  assert.deepEqual([isInputError(ce('a')), isInputError({ sku: 'a', error: 'No ASIN' })], [true, true]);
  for (const e of [{ sku: 'a', error: 'ServerError' }, { error: 'ClientError' }, { sku: '', error: 'ClientError' }, { sku: 'a', error: 'toString' }, { sku: 'a', error: 'constructor' }, { sku: 'a' }, null, undefined])
    assert.equal(isInputError(e), false, JSON.stringify(e));
  const s = splitErrors([ce('a'), { sku: 'b', error: 'timeout' }, { sku: 'c', error: 'No ASIN' }, { identifier: 'all', error: 'Response is not an array' }]);
  assert.deepEqual([s.input.map((e) => e.sku), s.hard.length], [['a', 'c'], 2]);
  assert.deepEqual(splitErrors(undefined), { input: [], hard: [] });
});
t('fetch-amazon-fees.js は判定を「切る前の全部の失敗」で行い、その 1 行を最後に出して、その終了コードで終わる (failed > 0 で一律に落とす形に戻っていない)', () => {
  const src = fs.readFileSync(path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'apps', 'warehouse', 'fetch-amazon-fees.js'), 'utf8');
  assert.equal(/process\.exit\(result\.failed > 0/.test(src), false);
  assert.match(src, /splitErrors\(allErrors\)/);
  const tail = src.slice(src.lastIndexOf('console.log('));
  assert.match(tail, /console\.log\(`\\n\$\{outcome\.line\}`\);\s*process\.exit\(outcome\.exitCode\);\s*\}\s*$/, '最後の console.log が outcome.line でない (daily-sync は最後の行を朝の通知に載せる)');
  assert.equal((src.match(/withOutcome\(/g) || []).length, 4, '結果を返す 3 か所 (ASIN 付き無し・取り直し対象無し・通常) が全部 withOutcome を通っていない');
});

console.log(`\n${ok} ok / ${ng} NG`);
process.exit(ng ? 1 : 0);
