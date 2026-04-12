import { Router } from 'express';
import https from 'https';
import http from 'http';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = Router();

// === 画像ダウンロードジョブ管理 ===
const downloadJobs = {};

function downloadImageFile(url, filepath) {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(filepath);
    const mod = url.startsWith('https') ? https : http;
    const req = mod.get(url, res => {
      if (res.statusCode !== 200) {
        file.close();
        fs.unlink(filepath, () => {});
        reject(new Error('HTTP ' + res.statusCode));
        return;
      }
      res.pipe(file);
      file.on('finish', () => { file.close(); resolve(); });
    });
    req.on('error', e => { file.close(); fs.unlink(filepath, () => {}); reject(e); });
    req.setTimeout(15000, () => { req.destroy(); reject(new Error('timeout')); });
  });
}

async function runDownloadJob(jobId, items, outputDir) {
  const job = downloadJobs[jobId];
  const concurrency = 5;
  let idx = 0;

  async function worker() {
    while (idx < items.length) {
      const item = items[idx++];
      try {
        const fname = item.code + '_' + item.position + '.jpg';
        const fpath = path.join(outputDir, fname);
        await downloadImageFile(item.url, fpath);
        job.done++;
      } catch (e) {
        job.errors.push({ file: item.code + '_' + item.position + '.jpg', error: e.message });
        job.done++;
      }
    }
  }

  await Promise.all(Array.from({ length: concurrency }, worker));
  job.complete = true;
  console.log('[Download] Job ' + jobId + ' complete: ' + job.done + '/' + job.total + ' done, ' + job.errors.length + ' errors');
}

// --- ルート ---

// HTMLページ配信
router.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

// 楽天RMS APIプロキシ → ミニPC経由（APIキーはミニPC側で管理）
const WAREHOUSE_URL = process.env.WAREHOUSE_URL || 'https://wh.bfaith-wh.uk';
function getServiceHeaders() {
  return {
    'CF-Access-Client-Id': process.env.CF_ACCESS_CLIENT_ID || '',
    'CF-Access-Client-Secret': process.env.CF_ACCESS_CLIENT_SECRET || '',
    'Authorization': `Bearer ${process.env.WAREHOUSE_SERVICE_TOKEN || ''}`,
  };
}

router.get('/proxy', async (req, res) => {
  // フロントからのAuthorizationヘッダーは無視（ミニPC側のキーを使う）
  const cursorMark = req.query.cursorMark || '*';
  const hits = req.query.hits || '100';

  try {
    const url = `${WAREHOUSE_URL}/service-api/rakuten-rms/items/search?cursorMark=${encodeURIComponent(cursorMark)}&hits=${hits}`;
    const response = await fetch(url, { headers: getServiceHeaders(), signal: AbortSignal.timeout(30000) });
    const data = await response.json();
    res.status(response.status).json(data);
  } catch (e) {
    res.status(502).json({ error: 'ミニPCへの接続に失敗: ' + e.message });
  }
});

// 画像ダウンロード開始
router.post('/download-images', (req, res) => {
  try {
    const { items, dir } = req.body;
    const outputDir = path.resolve(__dirname, dir || 'linegift_images');
    if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });
    const jobId = Date.now().toString();
    downloadJobs[jobId] = { done: 0, total: items.length, errors: [], complete: false, dir: outputDir };
    console.log('[Download] Job ' + jobId + ' started: ' + items.length + ' images → ' + outputDir);
    runDownloadJob(jobId, items, outputDir);
    res.json({ jobId, total: items.length, dir: outputDir });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

// ダウンロード進捗確認
router.get('/download-status', (req, res) => {
  const jobId = req.query.id;
  const job = downloadJobs[jobId];
  if (!job) {
    return res.status(404).json({ error: 'Job not found' });
  }
  res.json(job);
});

export default router;
