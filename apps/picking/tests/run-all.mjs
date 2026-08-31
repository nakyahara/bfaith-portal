/**
 * picking のテストを全部実行する。
 *   node apps/picking/tests/run-all.mjs
 * 各テストは DATA_DIR を一時ディレクトリに向けるため、本番・開発DBには触れない。
 */
import { spawnSync } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';

const dir = path.dirname(fileURLToPath(import.meta.url));
const suites = ['test-import.mjs', 'test-work.mjs', 'test-images.mjs', 'test-device.mjs', 'test-notify.mjs', 'test-drive-sync.mjs', 'test-line-search.mjs', 'test-stats.mjs', 'test-board-acl.mjs', 'test-floor.mjs', 'test-floor-disabled.mjs', 'test-repick.mjs', 'test-floor-alerts.mjs', 'test-miss-stats.mjs', 'test-next-sign.mjs', 'test-shortage-link.mjs', 'test-missing-images.mjs'];

let failed = 0;
for (const f of suites) {
  const r = spawnSync(process.execPath, [path.join(dir, f)], { stdio: 'inherit' });
  if (r.status !== 0) failed++;
}
console.log(failed === 0 ? '\n全スイート pass' : `\n${failed} スイートが失敗`);
process.exit(failed === 0 ? 0 : 1);
