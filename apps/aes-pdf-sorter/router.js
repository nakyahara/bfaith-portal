/**
 * AES PDF Sorter - Express Router
 * Python FastAPIバックエンドへのプロキシ
 */
import { Router } from 'express';
import { spawn } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';
import { bootStart, bootEnd, bootNote, bootFail } from '../observability/boot-log.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = Router();

const PYTHON_PORT = process.env.AES_PYTHON_PORT || 8001;
const PYTHON_BASE = `http://127.0.0.1:${PYTHON_PORT}`;

let pythonProcess = null;
let stopping = false;
const RESPAWN_DELAY_MS = 30_000;

// クラッシュ時のGChat通知 (DiskWatch等と同じ運用スペース GCHAT_WEBHOOK。未設定ならログのみ)
// 2026-07-23 実障害: OOM killでワーカーが静かに死んでいたことに気づけなかったため追加
const GCHAT_WEBHOOK = process.env.GCHAT_WEBHOOK;
const NOTIFY_TIMEOUT_MS = 10_000;
const CRASH_WINDOW_MS = 15 * 60 * 1000;        // この時間内のクラッシュ回数を数える
const CRASH_LOOP_THRESHOLD = 3;                // 回数がこれ以上なら「連続クラッシュ」扱い
const CRASH_LOOP_COOLDOWN_MS = 60 * 60 * 1000; // 連続クラッシュの通知は1時間に1回まで

let crashTimes = [];
let lastLoopNotifyAt = 0;

async function notifyGchat(text) {
  if (!GCHAT_WEBHOOK) return;
  try {
    const res = await fetch(GCHAT_WEBHOOK, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text }),
      signal: AbortSignal.timeout(NOTIFY_TIMEOUT_MS),
    });
    if (!res.ok) console.error(`[AES-Python] GChat通知エラー: HTTP ${res.status}`);
  } catch (e) {
    console.error(`[AES-Python] GChat通知エラー: ${e.message}`);
  }
}

function notifyCrash(code) {
  const now = Date.now();
  crashTimes = crashTimes.filter((t) => now - t < CRASH_WINDOW_MS);
  crashTimes.push(now);
  if (crashTimes.length >= CRASH_LOOP_THRESHOLD) {
    // 自動再起動が効いていても恒常的な問題 (メモリ不足等) が隠れている状態
    if (now - lastLoopNotifyAt >= CRASH_LOOP_COOLDOWN_MS) {
      lastLoopNotifyAt = now;
      notifyGchat(`🚨 *AESワーカー異常* Python処理が${CRASH_WINDOW_MS / 60000}分間に` +
        `${crashTimes.length}回落ちています (直近code=${code})。` +
        `自動再起動は継続しますが、Renderログ ([AES-Python] で検索) の確認が必要です`);
    }
  } else {
    notifyGchat(`⚠️ *AESワーカー* Python処理が終了しました (code=${code})。` +
      `30秒後に自動再起動します`);
  }
}

/**
 * Pythonバックエンドを起動
 */
export function startPythonBackend() {
  bootStart('aes-python', 'python-uvicorn');
  if (pythonProcess) {
    bootNote('aes-python', `既存child pid=${pythonProcess.pid} が生存中のためspawnスキップ`);
    return;
  }
  const pythonDir = path.join(__dirname, 'python');
  // Render (Docker): venv内のpython、ローカル: python/python3
  const venvPython = path.join(__dirname, '..', '..', 'venv', 'bin', 'python3');
  const pythonCmd = process.env.RENDER ? venvPython
    : process.platform === 'win32' ? 'python' : 'python3';

  pythonProcess = spawn(pythonCmd, [
    '-m', 'uvicorn', 'main:app',
    '--host', '127.0.0.1',
    '--port', String(PYTHON_PORT),
    '--log-level', 'warning'
  ], {
    cwd: pythonDir,
    stdio: ['ignore', 'pipe', 'pipe'],
    env: { ...process.env }
  });

  const childPid = pythonProcess.pid;

  pythonProcess.stdout.on('data', (data) => {
    console.log(`[AES-Python pid=${childPid}] ${data.toString().trim()}`);
  });

  pythonProcess.stderr.on('data', (data) => {
    const msg = data.toString().trim();
    if (msg) console.log(`[AES-Python pid=${childPid}] ${msg}`);
  });

  pythonProcess.on('close', (code) => {
    bootNote('aes-python', `child pid=${childPid} 終了 code=${code}`);
    console.log(`[AES-Python] プロセス終了 (code: ${code})`);
    pythonProcess = null;
    // Drive自動化ワーカーもこのプロセス内で動いているため、落ちたら自動復帰させる
    // (2026-07-23 実障害: OOMと思われるkillでcode=null終了→そのまま停止し続けた)
    if (!stopping) {
      notifyCrash(code);
      console.log(`[AES-Python] ${RESPAWN_DELAY_MS / 1000}秒後に自動再起動します`);
      setTimeout(() => {
        if (!pythonProcess && !stopping) startPythonBackend();
      }, RESPAWN_DELAY_MS).unref();
    }
  });

  pythonProcess.on('error', (err) => {
    bootFail('aes-python', `child pid=${childPid}`, err);
    console.error(`[AES-Python] 起動エラー: ${err.message}`);
    pythonProcess = null;
  });

  bootEnd('aes-python', 'python-uvicorn', `child_pid=${childPid} port=${PYTHON_PORT}`);
  console.log(`[AES-Python] バックエンド起動中 (port: ${PYTHON_PORT})...`);
}

/**
 * Pythonバックエンドを停止
 */
export function stopPythonBackend() {
  stopping = true;
  if (pythonProcess) {
    pythonProcess.kill();
    pythonProcess = null;
  }
}

// メインページ（HTML直接配信）
router.get('/', (req, res) => {
  res.render(path.join(__dirname, 'views', 'index'), {
    portalPrefix: '/apps/aes-pdf-sorter'
  });
});

// プロキシ: POST /process_aes_sorting → Python
router.post('/process_aes_sorting', async (req, res) => {
  try {
    // multerで処理せず、生のリクエストをそのままPythonに転送
    const headers = { ...req.headers };
    delete headers['host'];
    delete headers['connection'];

    const proxyRes = await fetch(`${PYTHON_BASE}/process_aes_sorting`, {
      method: 'POST',
      headers: headers,
      body: req,
      duplex: 'half'
    });

    res.status(proxyRes.status);
    for (const [key, value] of proxyRes.headers.entries()) {
      if (key.toLowerCase() !== 'transfer-encoding') {
        res.setHeader(key, value);
      }
    }
    const buffer = Buffer.from(await proxyRes.arrayBuffer());
    res.send(buffer);
  } catch (e) {
    console.error('[AES-Proxy] process_aes_sorting エラー:', e.message);
    res.status(502).json({ detail: 'Pythonバックエンドに接続できません。しばらく待ってから再度お試しください。' });
  }
});

// プロキシ: GET /download/:fileId → Python
router.get('/download/:fileId', async (req, res) => {
  try {
    const proxyRes = await fetch(`${PYTHON_BASE}/download/${req.params.fileId}`);

    res.status(proxyRes.status);
    for (const [key, value] of proxyRes.headers.entries()) {
      if (key.toLowerCase() !== 'transfer-encoding') {
        res.setHeader(key, value);
      }
    }
    const buffer = Buffer.from(await proxyRes.arrayBuffer());
    res.send(buffer);
  } catch (e) {
    console.error('[AES-Proxy] download エラー:', e.message);
    res.status(502).json({ detail: 'Pythonバックエンドに接続できません。' });
  }
});

// プロキシ: GET /cleanup_session/:sessionId → Python
router.get('/cleanup_session/:sessionId', async (req, res) => {
  try {
    const proxyRes = await fetch(`${PYTHON_BASE}/cleanup_session/${req.params.sessionId}`);
    const data = await proxyRes.json();
    res.json(data);
  } catch (e) {
    res.status(502).json({ detail: 'Pythonバックエンドに接続できません。' });
  }
});

// ヘルスチェック
router.get('/health', async (req, res) => {
  try {
    const proxyRes = await fetch(`${PYTHON_BASE}/health`);
    const data = await proxyRes.json();
    res.json({ ...data, proxy: 'ok' });
  } catch (e) {
    res.json({ status: 'python_unavailable', proxy: 'ok' });
  }
});

export default router;
