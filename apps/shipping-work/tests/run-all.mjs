/**
 * shipping-work のテストを全部実行する。
 *   node apps/shipping-work/tests/run-all.mjs
 * 各テストは DATA_DIR を一時ディレクトリに向けるため、本番・開発DBには触れない。
 */
import { spawnSync } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';

const dir = path.dirname(fileURLToPath(import.meta.url));
const repo = path.resolve(dir, '../../..');
const suites = ['test-pr3.mjs', 'test-empty-state.mjs', 'test-recovery.mjs', 'test-admin-fix.mjs'];

let failed = 0;
for (const f of suites) {
  const r = spawnSync(process.execPath, [path.join(dir, f), repo], { stdio: 'inherit' });
  if (r.status !== 0) failed++;
}
console.log(failed === 0 ? '\n全スイート pass' : `\n${failed} スイートが失敗`);
process.exit(failed === 0 ? 0 : 1);
