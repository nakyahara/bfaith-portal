/**
 * select-set のテストを全部実行する。
 *   node apps/select-set/tests/run-all.mjs
 *
 * ここに入れるのは DB・外部APIに触れないテストだけ。
 * 本番の過去受注に対する検証は verify-against-production.mjs (miniPCで実行) を使う。
 */
import { spawnSync } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';

const dir = path.dirname(fileURLToPath(import.meta.url));
const repo = path.resolve(dir, '../../..');
const suites = ['test-expand.mjs', 'test-db.mjs', 'test-master.mjs', 'test-views.mjs', 'test-extension.mjs', 'test-extension-logic.mjs'];

let failed = 0;
for (const f of suites) {
  console.log(`\n=== ${f} ===`);
  const r = spawnSync(process.execPath, [path.join(dir, f), repo], { stdio: 'inherit' });
  if (r.status !== 0) failed++;
}
console.log(failed === 0 ? '\n全スイート pass' : `\n${failed} スイートが失敗`);
process.exit(failed === 0 ? 0 : 1);
