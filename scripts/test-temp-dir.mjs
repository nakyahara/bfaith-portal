import fs from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const MARKER = 'BFAITH_TEST_TEMP_ENTRY';

/**
 * Run the script in a child process so Windows releases all DB handles before
 * cleanup. Only the parent-created directory is removed, never supplied DATA_DIR.
 * Call before importing modules that open databases. No background janitor.
 */
export async function temporaryTestDataDir(entryUrl, prefix, { reuseProvided = false } = {}) {
  const entry = fileURLToPath(entryUrl);
  if (process.env[MARKER] === entry) {
    delete process.env[MARKER];
    if (!process.env.DATA_DIR) throw new Error('Missing child test DATA_DIR');
    return process.env.DATA_DIR;
  }
  if (reuseProvided && process.env.DATA_DIR) return process.env.DATA_DIR;
  if (!/^[a-zA-Z0-9_-]+-$/.test(prefix)) throw new Error('Invalid test directory prefix');

  const parent = await fs.realpath(os.tmpdir());
  const owned = await fs.mkdtemp(path.join(parent, prefix));
  let child;
  const forward = signal => { if (child && child.exitCode === null && child.signalCode === null) child.kill(signal); };
  const onInt = () => forward('SIGINT');
  const onTerm = () => forward('SIGTERM');
  let code = 1;
  try {
    child = spawn(process.execPath, [...process.execArgv, entry, ...process.argv.slice(2)], {
      cwd: process.cwd(), stdio: 'inherit', windowsHide: true,
      env: { ...process.env, DATA_DIR: owned, [MARKER]: entry },
    });
    process.on('SIGINT', onInt);
    process.on('SIGTERM', onTerm);
    code = await new Promise((resolve, reject) => {
      child.once('error', reject);
      child.once('close', (status, signal) => resolve(status ?? (128 + (os.constants.signals[signal] || 1))));
    });
  } finally {
    process.off('SIGINT', onInt);
    process.off('SIGTERM', onTerm);
    // Resolve again immediately before recursive deletion; reject replacement
    // of the owned root with a junction/symlink pointing elsewhere.
    const real = await fs.realpath(owned).catch(error => {
      if (error.code === 'ENOENT') return null;
      throw error;
    });
    if (real !== null) {
      if (real !== owned || path.dirname(real) !== parent) throw new Error('Test cleanup path changed; directory retained');
      await fs.rm(real, { recursive: true, force: true, maxRetries: 10, retryDelay: 200 });
      console.log('[test-temp] removed ' + owned);
    }
  }
  process.exit(code);
}
