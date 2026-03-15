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

// 楽天RMS APIプロキシ
router.get('/proxy', (req, res) => {
  const auth = req.headers['authorization'];
  if (!auth) {
    return res.status(401).json({ error: 'Authorization header required' });
  }

  const cursorMark = req.query.cursorMark || '*';
  const hits = req.query.hits || '100';
  const rmsPath = `/es/2.0/items/search?cursorMark=${encodeURIComponent(cursorMark)}&hits=${hits}`;

  const opts = {
    hostname: 'api.rms.rakuten.co.jp',
    path: rmsPath,
    headers: { 'Authorization': auth }
  };

  https.get(opts, (apiRes) => {
    let body = '';
    apiRes.on('data', d => body += d);
    apiRes.on('end', () => {
      try {
        const d = JSON.parse(body);
        const results = d.results || d.items || [];
        const isFirstPage = decodeURIComponent(cursorMark) === '*';
        if (isFirstPage && results[0]) {
          const firstItem = (results[0].item) || results[0];
          console.log(`[RMS API] HTTP ${apiRes.statusCode} | numFound:${d.numFound} results:${results.length}`);
        } else {
          console.log(`[RMS API] HTTP ${apiRes.statusCode} | results:${results.length}`);
        }
      } catch (e) {
        console.log(`[RMS API] HTTP ${apiRes.statusCode} | parse error: ${e.message}`);
      }
      res.status(apiRes.statusCode).type('json').send(body);
    });
  }).on('error', e => {
    res.status(502).json({ error: e.message });
  });
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
