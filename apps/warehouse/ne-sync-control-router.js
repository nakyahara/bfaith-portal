/**
 * NE 反映 worker 起動 control endpoint (ミニPC 側、2026-06-06 PR-C 構成 4)
 *
 * Render の UI ボタンが押されると、Render が start-run で run_id を発行し、その run_id を
 * 持ってこの endpoint を叩く。ミニPC は runner.js を child_process.spawn で起動して即返却。
 *
 * フロー:
 *   Render UI 押下
 *     ↓ POST Render /api/ne-sync/runs → run_id 取得
 *     ↓ POST wh.bfaith-wh.uk/ne-sync-control/start with { run_id }
 *   ミニPC (このファイル)
 *     ↓ spawn detached: node apps/ne-sync-runner/runner.js --run-id=<uuid>
 *     ↓ 即返却 { ok, run_id, pid }
 *   worker (runner.js、別プロセス、PID lock + 1 件ずつ NE 反映)
 *
 * 認証: requireMinipcNeSyncRunKey (Bearer fail-closed) + CF Access (server.js mount で前段)
 */
import { Router } from 'express';
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { requireMinipcNeSyncRunKey } from './ne-sync-control-auth.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const RUNNER_PATH = path.resolve(__dirname, '..', 'ne-sync-runner', 'runner.js');
const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), 'data');
const PID_FILE = path.join(DATA_DIR, 'ne-sync-runner.pid');
const NE_TOKEN_FILE = path.join(DATA_DIR, 'ne-tokens.json');

export const neSyncControlRouter = Router();
neSyncControlRouter.use(requireMinipcNeSyncRunKey);

// 簡易 UUID v4 形式チェック (worker への引数を validate、shell injection 防御)
const UUID_V4_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function isProcessAlive(pid) {
  try { process.kill(pid, 0); return true; } catch { return false; }
}

// Codex R1 High 2 対応: spawn 前に preflight して即死要因を 409/503 で返す。
function preflight() {
  if (!fs.existsSync(RUNNER_PATH)) {
    return { status: 500, body: { ok: false, error: 'RUNNER_NOT_FOUND', message: 'runner.js not found' } };
  }
  if (!process.env.NE_CLIENT_ID || !process.env.NE_CLIENT_SECRET) {
    return { status: 503, body: { ok: false, error: 'NE_NOT_CONFIGURED',
      message: 'NE_CLIENT_ID / NE_CLIENT_SECRET が未設定です' } };
  }
  if (!process.env.RENDER_NE_SYNC_WORKER_KEY) {
    return { status: 503, body: { ok: false, error: 'WORKER_KEY_NOT_CONFIGURED',
      message: 'RENDER_NE_SYNC_WORKER_KEY が未設定です' } };
  }
  if (!fs.existsSync(NE_TOKEN_FILE)) {
    return { status: 503, body: { ok: false, error: 'NE_TOKENS_NOT_FOUND',
      message: 'NE OAuth トークンファイルが見つかりません (data/ne-tokens.json)' } };
  }
  // PID lock 衝突確認 (既存 PID 生存中なら 409)
  if (fs.existsSync(PID_FILE)) {
    let existingPid;
    try { existingPid = parseInt(fs.readFileSync(PID_FILE, 'utf-8').trim(), 10); } catch { existingPid = NaN; }
    if (existingPid && isProcessAlive(existingPid)) {
      return { status: 409, body: { ok: false, error: 'WORKER_ALREADY_RUNNING',
        message: `worker is already running (pid=${existingPid})` } };
    }
  }
  return null; // OK
}

neSyncControlRouter.post('/start', async (req, res) => {
  const run_id = (req.body && req.body.run_id) || '';
  if (!UUID_V4_RE.test(run_id)) {
    return res.status(400).json({ ok: false, error: 'INVALID_RUN_ID',
      message: 'run_id must be a UUID v4' });
  }
  // Preflight (Codex R1 High 2)
  const pf = preflight();
  if (pf) return res.status(pf.status).json(pf.body);

  let child;
  try {
    child = spawn(process.execPath, [RUNNER_PATH, `--run-id=${run_id}`], {
      detached: true,
      stdio: 'ignore',
      windowsHide: true,
    });
  } catch (e) {
    console.error('[ne-sync-control] spawn failed', e.message);
    return res.status(500).json({ ok: false, error: 'SPAWN_FAILED', message: e.message });
  }
  // 短時間 (1.5s) 待って、即 exit (preflight すり抜けた env エラー等) を捕捉。
  // exit 0 もしくは exit code > 0 が 1.5s 以内に来たら失敗扱い。
  // 1.5s 経って生きていれば成功扱いで即返却 (detach)。
  const result = await new Promise((resolve) => {
    let settled = false;
    const onExit = (code) => {
      if (settled) return;
      settled = true;
      resolve({ ok: false, exitCode: code });
    };
    child.once('exit', onExit);
    child.once('error', (e) => {
      if (settled) return;
      settled = true;
      resolve({ ok: false, error: e.message });
    });
    setTimeout(() => {
      if (settled) return;
      settled = true;
      child.removeListener('exit', onExit);
      child.unref(); // 親から detach
      resolve({ ok: true });
    }, 1500);
  });
  if (!result.ok) {
    console.error('[ne-sync-control] worker exited early', { run_id, ...result });
    return res.status(500).json({ ok: false, error: 'WORKER_EXITED_EARLY',
      message: `worker died within 1.5s${result.exitCode != null ? ' (code=' + result.exitCode + ')' : ''}` });
  }
  console.log('[ne-sync-control] spawn worker', { run_id, pid: child.pid });
  return res.json({ ok: true, run_id, pid: child.pid });
});
