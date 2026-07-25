/**
 * libuv-transient-crash の判定テーブルテスト (2026-07-25、Codex Low 指摘で常設化)
 *
 *   node scripts/test/libuv-transient-crash.test.mjs
 *
 * 「拾うべき1ケースだけを拾い、それ以外は絶対に再実行させない」ことを固定する。
 * 判定を緩める変更をしたら必ずここを落とすこと。
 */
import { isLibuvTransientCrash, WINDOWS_ABORT_STATUS } from '../../lib/libuv-transient-crash.js';

const ASSERT_LINE =
  'Assertion failed: !(handle->flags & UV_HANDLE_CLOSING), file src\\win\\async.c, line 76';

/** 2026-07-25 楽天レビュー取込で実測した形 (status=3221226505 signal=null) */
const realCrash = { status: WINDOWS_ABORT_STATUS, signal: null, code: null, stderr: ASSERT_LINE, stdout: '' };

const cases = [
  // [名前, エラー, オプション, 期待値]
  ['実クラッシュ (assert + abort status + win32) は再実行する', realCrash, {}, true],
  ['本処理成功後に abort した型も再実行する',
    { ...realCrash, stdout: '=== summary: ok=1 failed=0 ===\n' }, {}, true],
  ['assert 行に前後のログが付いていても拾う',
    { ...realCrash, stderr: `[import] 開始\n${ASSERT_LINE}\n` }, {}, true],

  ['非 Windows では再実行しない', realCrash, { platform: 'linux' }, false],
  ['同じ assert 文字列でも status=1 なら再実行しない', { ...realCrash, status: 1 }, {}, false],
  ['timeout kill (signal=SIGTERM) は再実行しない',
    { ...realCrash, signal: 'SIGTERM' }, {}, false],
  ['spawn 失敗 (status=null, code=ENOENT) は再実行しない',
    { ...realCrash, status: null, code: 'ENOENT' }, {}, false],
  ['maxBuffer 超過 (ENOBUFS) は再実行しない',
    { ...realCrash, status: null, code: 'ENOBUFS' }, {}, false],
  ['assert 文字列が stdout にしか無い (子が中継した他プロセスのログ) なら再実行しない',
    { ...realCrash, stderr: '', stdout: ASSERT_LINE }, {}, false],
  ['別の assert (UV_HANDLE_CLOSING 以外) は再実行しない',
    { ...realCrash, stderr: 'Assertion failed: !(handle->flags & UV_HANDLE_ACTIVE), file src\\win\\async.c, line 76' }, {}, false],
  ['abort status だけで assert 行が無ければ再実行しない',
    { ...realCrash, stderr: 'FATAL: out of memory' }, {}, false],
  ['stderr が undefined でも落ちない', { ...realCrash, stderr: undefined }, {}, false],
  ['null / undefined を渡しても落ちない', null, {}, false],
];

let pass = 0, fail = 0;
for (const [name, err, opts, expected] of cases) {
  let actual;
  try {
    actual = isLibuvTransientCrash(err, { platform: 'win32', ...opts });
  } catch (e) {
    actual = `throw: ${e.message}`;
  }
  if (actual === expected) { pass++; console.log(`  ✅ ${name}`); }
  else { fail++; console.log(`  ❌ ${name} — 期待 ${expected} / 実際 ${actual}`); }
}

console.log(`\n===== libuv-transient-crash: PASS ${pass} / FAIL ${fail} =====`);
process.exitCode = fail > 0 ? 1 : 0;
