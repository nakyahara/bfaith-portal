#!/usr/bin/env node
/**
 * safe-debug.mjs — warehouse上でデバッグクエリを安全に実行するラッパー
 *
 * 本番ミニPC上で手動デバッグスクリプトを走らせたときに:
 *   - 無限ループ or 重いクエリで長時間CPUを占有 → warehouseサーバーを圧迫
 *   - 多重起動で累積的にCPU枯渇
 * という事故を防ぐため、全デバッグスクリプトはこのラッパー経由で起動する。
 *
 * 使い方:
 *   node scripts/safe-debug.mjs check-rakuten.mjs
 *   node scripts/safe-debug.mjs --timeout=600 check-rakuten.mjs
 *
 * 提供する安全装置:
 *   - 単一実行: 既に別インスタンスが動いていたら即失敗 (lockfile方式)
 *   - ハードタイムアウト: デフォルト5分、超過で自動kill
 *   - 低優先度: Windowsで BelowNormal に設定、warehouseより負けるように
 */
import { spawn } from 'child_process';
import fs from 'fs';
import os from 'os';
import path from 'path';

const LOCK = path.join(os.tmpdir(), 'warehouse-debug.lock');
const DEFAULT_TIMEOUT_MS = 5 * 60 * 1000;

const args = process.argv.slice(2);
let timeoutMs = DEFAULT_TIMEOUT_MS;
const scriptArgs = [];
for (const a of args) {
  const m = a.match(/^--timeout=(\d+)$/);
  if (m) { timeoutMs = parseInt(m[1], 10) * 1000; continue; }
  scriptArgs.push(a);
}
if (scriptArgs.length === 0) {
  console.error('Usage: node scripts/safe-debug.mjs [--timeout=SECONDS] <script.mjs> [...args]');
  process.exit(64);
}

if (fs.existsSync(LOCK)) {
  const existingPid = fs.readFileSync(LOCK, 'utf-8').trim();
  try {
    process.kill(+existingPid, 0);
    console.error(`[safe-debug] 別のデバッグスクリプトが実行中 (PID ${existingPid}). 中断します.`);
    process.exit(73);
  } catch {
    fs.unlinkSync(LOCK);
  }
}
fs.writeFileSync(LOCK, String(process.pid));
const cleanup = () => { try { fs.unlinkSync(LOCK); } catch {} };
process.on('exit', cleanup);
process.on('SIGINT', () => { cleanup(); process.exit(130); });
process.on('SIGTERM', () => { cleanup(); process.exit(143); });

const [scriptPath, ...rest] = scriptArgs;
console.log(`[safe-debug] script=${scriptPath} timeout=${timeoutMs/1000}s pid=${process.pid}`);
const child = spawn(process.execPath, [scriptPath, ...rest], {
  stdio: 'inherit',
  env: process.env,
});

try { os.setPriority(child.pid, 10); } catch {}

const killTimer = setTimeout(() => {
  console.error(`[safe-debug] タイムアウト ${timeoutMs/1000}s — プロセスkill`);
  child.kill('SIGKILL');
}, timeoutMs);

child.on('exit', (code, signal) => {
  clearTimeout(killTimer);
  cleanup();
  if (signal) {
    console.error(`[safe-debug] 終了 signal=${signal}`);
    process.exit(137);
  }
  console.log(`[safe-debug] 終了 code=${code}`);
  process.exit(code ?? 0);
});
