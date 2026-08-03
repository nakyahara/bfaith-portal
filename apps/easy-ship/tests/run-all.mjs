/**
 * easy-ship のテストを全部実行する。
 *   node apps/easy-ship/tests/run-all.mjs
 * 各テストは DATA_DIR を一時ディレクトリに向けるため、本番・開発DBには触れない。
 */
import { spawnSync } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';

const dir = path.dirname(fileURLToPath(import.meta.url));
const repo = path.resolve(dir, '../../..');
const suites = ['test-master.mjs', 'test-csv-import.mjs', 'test-ext-api.mjs'];

let failed = 0;
for (const f of suites) {
  console.log(`\n=== ${f} ===`);
  const r = spawnSync(process.execPath, [path.join(dir, f), repo], { stdio: 'inherit' });
  if (r.status !== 0) failed++;
}
console.log(failed === 0 ? '\n全スイート pass' : `\n${failed} スイートが失敗`);
process.exit(failed === 0 ? 0 : 1);
