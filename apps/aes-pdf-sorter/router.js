/**
 * AES PDF Sorter - Express Router
 * Python FastAPIバックエンドへのプロキシ
 */
import { Router } from 'express';
import { spawn } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = Router();

const PYTHON_PORT = process.env.AES_PYTHON_PORT || 8001;
const PYTHON_BASE = `http://127.0.0.1:${PYTHON_PORT}`;

let pythonProcess = null;

/**
 * Pythonバックエンドを起動
 */
export function startPythonBackend() {
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

  pythonProcess.stdout.on('data', (data) => {
    console.log(`[AES-Python] ${data.toString().trim()}`);
  });

  pythonProcess.stderr.on('data', (data) => {
    const msg = data.toString().trim();
    if (msg) console.log(`[AES-Python] ${msg}`);
  });

  pythonProcess.on('close', (code) => {
    console.log(`[AES-Python] プロセス終了 (code: ${code})`);
    pythonProcess = null;
  });

  pythonProcess.on('error', (err) => {
    console.error(`[AES-Python] 起動エラー: ${err.message}`);
    pythonProcess = null;
  });

  console.log(`[AES-Python] バックエンド起動中 (port: ${PYTHON_PORT})...`);
}

/**
 * Pythonバックエンドを停止
 */
export function stopPythonBackend() {
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
