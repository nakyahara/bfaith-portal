/**
 * 郵便料金判定 API — HTTP スモーク (server.js を子プロセスで起動し、本番と同じ middleware の並びで叩く)
 *
 * 実行: node scripts/smoke-postage-judge-http.mjs
 * 検証: judge-api が requireAppAccess より先に mount されていること / 共通 JSON parser (10MB) が
 *       この経路を除外していて、認証前に本文を読まない・認証後は 256KB で止まること / 画面はログイン必須
 */
import fs from 'fs';
import os from 'os';
import path from 'path';
import { spawn } from 'child_process';
import { fileURLToPath } from 'url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const PORT = 3462;
const BASE = `http://127.0.0.1:${PORT}`;
const API = `${BASE}/apps/postage/judge-api`;
const DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'postage-http-'));
const KEY = 'smoke-judge-key';

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };

const child = spawn(process.execPath, ['server.js'], {
  cwd: ROOT,
  env: {
    ...process.env, DATA_DIR, PORT: String(PORT), PORTAL_PASS: 'smoke', NODE_ENV: 'development', SESSION_SECRET: 'smoke-secret',
    INBOUND_INFO_SYNC_ENABLED: 'false', POSTAGE_JUDGE_KEY: KEY,
  },
  stdio: ['ignore', 'pipe', 'pipe'],
});
let logs = '';
child.stdout.on('data', (d) => { logs += d; });
child.stderr.on('data', (d) => { logs += d; });

async function waitUp() {
  for (let i = 0; i < 120; i++) {
    try { const r = await fetch(`${BASE}/login`); if (r.status === 200) return true; } catch { /* まだ */ }
    await new Promise((r) => setTimeout(r, 500));
  }
  return false;
}
const post = (body, key) => fetch(`${API}/batch`, {
  method: 'POST',
  headers: { 'content-type': 'application/json', ...(key ? { 'x-api-key': key } : {}) },
  body: typeof body === 'string' ? body : JSON.stringify(body),
});

try {
  if (!(await waitUp())) throw new Error(`server が起動しませんでした\n${logs.slice(-3000)}`);

  const h = await fetch(`${API}/health`, { headers: { 'x-api-key': KEY } });
  ok(h.status === 200 && (await h.json()).ok === true, 'health: キー付きで 200');
  ok((await fetch(`${API}/health`)).status === 401, 'health: キー無しは 401 (requireAppAccess の 302 ではない = mount 順が正しい)');

  const b = await post({ slip_nos: ['1'] }, KEY);
  const bj = await b.json();
  ok(b.status === 200 && bj.results?.[0]?.status === 'unknown', 'batch: 判定して 200 (構成が無いので不明)');

  const big = JSON.stringify({ slip_nos: ['1'], pad: 'x'.repeat(300 * 1024) });
  ok((await post(big, KEY)).status === 413, 'batch: 300KB の本文は 413 (共通 parser の 10MB ではなく専用 256KB が効く)');
  ok((await post(big, null)).status === 401, 'batch: キー無しの 300KB は 401 (認証前に本文を読まない)');

  const d = await fetch(`${BASE}/apps/postage/decisions`, { redirect: 'manual' });
  ok(d.status === 302 && /\/login/.test(d.headers.get('location') || ''), '/decisions はログイン必須');
} catch (e) {
  fail++;
  console.log(`  ✗ ${e.message}`);
} finally {
  child.kill();
  await new Promise((r) => setTimeout(r, 500));
  try { fs.rmSync(DATA_DIR, { recursive: true, force: true }); } catch { /* Windows で掴んだままなら残す */ }
}
console.log(`\n${fail === 0 ? '✅' : '❌'} ${pass} passed, ${fail} failed`);
process.exitCode = fail === 0 ? 0 : 1;
