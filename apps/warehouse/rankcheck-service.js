/**
 * 楽天順位チェッカー サービスAPI (miniPC側)
 * /service-api/rankcheck/* にマウント
 *
 * 案B: ranking-checker.db を miniPC 上で一元管理する。
 *   - Render UI は /apps/ranking-checker/router.js 経由でここを proxy する
 *   - Render CSV cron も本APIを叩いて集計対象を取得する
 *   - Runner (Task Scheduler 起動の CLI) は API を通さず直接 DB 書き込み
 *
 * 既存の /apps/ranking-checker/router.js の内部ロジック (normalize/upsert) を
 * 再利用するため、同モジュールから normalizeProductInput / encodeImportRank を
 * import して重複を避ける。
 */
import { Router } from 'express';
import { spawn } from 'child_process';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { okResponse, errorResponse } from './error-handler.js';
import * as rdb from '../ranking-checker/db.js';
import {
  normalizeProductInput,
  encodeImportRank,
} from '../ranking-checker/router.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.join(__dirname, '..', '..');
const RUNNER_SCRIPT = path.join(REPO_ROOT, 'apps', 'ranking-checker-runner', 'runner.js');
const DATA_DIR = process.env.DATA_DIR || path.join(REPO_ROOT, 'data');
const LOG_FILE = path.join(DATA_DIR, 'ranking-checker.log');
// Render→miniPC への一時アップロード保存先 (Phase2 デプロイで本番JSONを取り込む用)
const LEGACY_UPLOAD_DIR = process.env.RANKCHECK_LEGACY_UPLOAD_DIR
  || (process.platform === 'win32' ? 'C:\\tools\\rankcheck-runner\\data-migrate' : path.join(DATA_DIR, 'data-migrate'));

const router = Router();

// ── 読み取り系 ──

/**
 * GET /service-api/rankcheck/data
 *   UI 表示用の legacy JSON 形 { products: [...] } を返す。
 *   Render の /apps/ranking-checker/data が proxy する先。
 */
router.get('/data', (req, res) => {
  try {
    res.json(rdb.exportLegacyShape());
  } catch (e) {
    errorResponse(res, { status: 500, error: 'DB_EXPORT_ERROR', message: e.message, requestId: req.requestId });
  }
});

/**
 * GET /service-api/rankcheck/master
 *   Runner や管理スクリプトが商品マスタだけを必要とする場合の軽量版。
 *   history 無し、全商品 ID + master fields を返す。
 */
router.get('/master', (req, res) => {
  try {
    const products = [];
    for (const p of rdb.iterAllProducts()) {
      products.push({
        client_id: p.client_id,
        keyword: p.keyword,
        product_code: p.product_code || null,
        own_url: p.own_url || null,
        yahoo_url: p.yahoo_url || null,
        amazon_url: p.amazon_url || null,
        amazon_asin: p.amazon_asin || null,
        competitor1_url: p.competitor1_url || null,
        competitor2_url: p.competitor2_url || null,
        review_count: p.review_count,
      });
    }
    okResponse(res, { count: products.length, products });
  } catch (e) {
    errorResponse(res, { status: 500, error: 'DB_MASTER_ERROR', message: e.message, requestId: req.requestId });
  }
});

/**
 * GET /service-api/rankcheck/run-status
 *   実行中/最新 run を返す。Phase2 UI polling と健全性監視で使用。
 */
router.get('/run-status', (req, res) => {
  try {
    okResponse(res, {
      latest: rdb.getLatestRun(),
      running: rdb.getRunningRun(),
    });
  } catch (e) {
    errorResponse(res, { status: 500, error: 'DB_RUN_STATUS_ERROR', message: e.message, requestId: req.requestId });
  }
});

// ── 書き込み系 ──

/**
 * POST /service-api/rankcheck/data
 *   UI 全件保存相当。商品マスタのみ (history は無視)。
 *   normalizeProductInput を router.js と共有。
 */
router.post('/data', (req, res) => {
  try {
    const body = req.body;
    if (!body || typeof body !== 'object') return errorResponse(res, { status: 400, error: 'invalid_body' });
    if (!Array.isArray(body.products)) return errorResponse(res, { status: 400, error: 'products_not_array' });

    const seenClientIds = new Set();
    const normalized = [];
    try {
      for (let i = 0; i < body.products.length; i++) {
        normalized.push(normalizeProductInput(body.products[i], i, { seenClientIds }));
      }
    } catch (e) {
      if (e && e.status === 400) return res.status(400).json({ ok: false, ...e.body, message: e.message });
      throw e;
    }

    if (normalized.length === 0 && body.confirmEmpty !== true) {
      return errorResponse(res, { status: 400, error: 'empty_without_confirm' });
    }

    const incomingClientIds = seenClientIds;
    const db = rdb.getDb();
    const apply = db.transaction(() => {
      for (const p of normalized) rdb.upsertProduct(p);
      let deleted = 0;
      const existing = [];
      for (const row of rdb.iterAllProducts()) {
        existing.push({ id: row.id, client_id: row.client_id });
      }
      for (const ex of existing) {
        if (!incomingClientIds.has(ex.client_id)) {
          rdb.deleteProduct(ex.id);
          deleted++;
        }
      }
      return { inserted_or_updated: normalized.length, deleted };
    });
    okResponse(res, apply());
  } catch (e) {
    errorResponse(res, { status: 500, error: 'DB_WRITE_ERROR', message: e.message, requestId: req.requestId });
  }
});

/**
 * POST /service-api/rankcheck/data/import
 *   履歴含む復元。replaceAll=true で差分削除。
 */
router.post('/data/import', (req, res) => {
  try {
    const body = req.body;
    if (!body || typeof body !== 'object') return errorResponse(res, { status: 400, error: 'invalid_body' });
    if (!Array.isArray(body.products)) return errorResponse(res, { status: 400, error: 'products_not_array' });
    const replaceAll = body.replaceAll === true;

    const seenClientIds = new Set();
    const normalized = [];
    try {
      for (let i = 0; i < body.products.length; i++) {
        normalized.push(normalizeProductInput(body.products[i], i, { seenClientIds, allowHistory: true }));
      }
    } catch (e) {
      if (e && e.status === 400) return res.status(400).json({ ok: false, ...e.body, message: e.message });
      throw e;
    }

    if (replaceAll && normalized.length === 0 && body.confirmEmpty !== true) {
      return errorResponse(res, { status: 400, error: 'empty_without_confirm' });
    }

    const incomingClientIds = seenClientIds;
    const db = rdb.getDb();
    const apply = db.transaction(() => {
      let productsUpserted = 0;
      let historyUpserted = 0;
      for (const p of normalized) {
        const productId = rdb.upsertProduct(p);
        productsUpserted++;
        for (const h of p.history) {
          if (!h || !h.date) continue;
          rdb.upsertHistory({
            product_id: productId,
            date: h.date,
            own_rank: encodeImportRank(h.own_rank),
            competitor1_rank: encodeImportRank(h.competitor1_rank),
            competitor2_rank: encodeImportRank(h.competitor2_rank),
            yahoo_own_rank: encodeImportRank(h.yahoo_own_rank),
            amazon_own_rank: encodeImportRank(h.amazon_own_rank),
          });
          historyUpserted++;
        }
      }
      let deleted = 0;
      if (replaceAll) {
        const existing = [];
        for (const row of rdb.iterAllProducts()) {
          existing.push({ id: row.id, client_id: row.client_id });
        }
        for (const ex of existing) {
          if (!incomingClientIds.has(ex.client_id)) {
            rdb.deleteProduct(ex.id);
            deleted++;
          }
        }
      }
      return { productsUpserted, historyUpserted, deleted, mode: replaceAll ? 'replace' : 'merge' };
    });
    okResponse(res, apply());
  } catch (e) {
    errorResponse(res, { status: 500, error: 'DB_IMPORT_ERROR', message: e.message, requestId: req.requestId });
  }
});

// ── Runner kick ──

let _lastKickAt = 0;
const KICK_COOLDOWN_MS = 60_000; // 1分以内の連続キックは拒否

/**
 * POST /service-api/rankcheck/run-check
 *   Render UI の「順位チェック開始」ボタンから呼ばれる。
 *   miniPC 上で runner.js を子プロセスとして detached 起動し、即応答する。
 *
 *   safe-debug 思想: サービスプロセス内で重い処理を走らせず、別プロセスに分離。
 *   Runner は終了で成果物を run_state に残すので、呼び出し側は /run-status で追跡。
 */
router.post('/run-check', (req, res) => {
  try {
    // 先に stale running (>=3h) を failed 化してから判定する。
    // そうしないと UI から stale run を手動復旧できない。
    const staleFixed = rdb.markStaleRunning();
    if (staleFixed > 0) {
      console.log(`[rankcheck-service] run-check: stale running を failed 化: ${staleFixed} 件`);
    }

    // 残った running は「本当に実行中」。UI には 200 で返す。
    const running = rdb.getRunningRun();
    if (running) {
      return okResponse(res, { started: false, message: '既に実行中', running });
    }

    const now = Date.now();
    if (now - _lastKickAt < KICK_COOLDOWN_MS) {
      return errorResponse(res, { status: 429, error: 'KICK_COOLDOWN', message: '直前のキックから 60 秒未満です' });
    }

    const force = req.body && req.body.force === true;
    const args = [RUNNER_SCRIPT];
    if (force) args.push('--force');

    // spawn の stdio は独立ログへ redirect する。'ignore' だと Node 起動失敗や
    // env 欠落 (exit 78) が追跡不能。
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
    const SPAWN_LOG = path.join(DATA_DIR, 'rankcheck-runner-spawn.log');
    const spawnLogFd = fs.openSync(SPAWN_LOG, 'a');
    fs.writeSync(spawnLogFd, `\n--- spawn ${new Date().toISOString()} force=${force} ---\n`);

    const child = spawn(process.execPath, args, {
      cwd: REPO_ROOT,
      env: process.env,
      detached: true,
      stdio: ['ignore', spawnLogFd, spawnLogFd],
    });
    child.unref();
    // fd は spawn 後に親側を close。子は複製した fd を持ち続ける。
    fs.closeSync(spawnLogFd);
    _lastKickAt = now;

    okResponse(res, { started: true, pid: child.pid, force, spawn_log: SPAWN_LOG });
  } catch (e) {
    errorResponse(res, { status: 500, error: 'RUN_KICK_ERROR', message: e.message, requestId: req.requestId });
  }
});

// ── 一時アップロード (Phase2 デプロイ専用) ──

/**
 * POST /service-api/rankcheck/upload-legacy-json
 *   Render Persistent Disk 上の `data/ranking-checker.json` (80-150MB推定) を
 *   miniPC へ送り込むための受信口。serviceAuth 配下なので token 必須。
 *
 *   server.js 側でこのパスは JSON parser を skip させ、req を直接 stream で
 *   受けてディスクに書く (メモリにロードしない)。
 *
 *   保存先は LEGACY_UPLOAD_DIR (default `C:\tools\rankcheck-runner\data-migrate`)。
 *   一時ファイル `*.uploading.<ts>` に書いてから atomic rename。
 *
 *   Phase 2 デプロイ完了後はこの endpoint は不要。Phase 3 で削除可。
 */
router.post('/upload-legacy-json', (req, res) => {
  try {
    if (!fs.existsSync(LEGACY_UPLOAD_DIR)) fs.mkdirSync(LEGACY_UPLOAD_DIR, { recursive: true });
  } catch (e) {
    return errorResponse(res, { status: 500, error: 'MKDIR_FAILED', message: e.message });
  }

  const targetPath = path.join(LEGACY_UPLOAD_DIR, 'ranking-checker.json');
  const tmpPath = `${targetPath}.uploading.${Date.now()}`;

  let bytes = 0;
  let finished = false;
  const ws = fs.createWriteStream(tmpPath);

  req.on('data', chunk => { bytes += chunk.length; });
  req.on('aborted', () => {
    if (!finished) {
      ws.destroy();
      try { fs.unlinkSync(tmpPath); } catch {}
      if (!res.headersSent) errorResponse(res, { status: 499, error: 'CLIENT_ABORTED' });
    }
  });
  ws.on('error', err => {
    finished = true;
    try { fs.unlinkSync(tmpPath); } catch {}
    if (!res.headersSent) errorResponse(res, { status: 500, error: 'WRITE_ERROR', message: err.message });
  });
  ws.on('finish', () => {
    finished = true;
    try {
      fs.renameSync(tmpPath, targetPath);
      okResponse(res, { bytes, path: targetPath });
    } catch (e) {
      try { fs.unlinkSync(tmpPath); } catch {}
      errorResponse(res, { status: 500, error: 'RENAME_FAILED', message: e.message });
    }
  });
  req.pipe(ws);
});

// ── ログ尾読み ──

router.get('/logs', (req, res) => {
  // 無効値 / 負 / 非数値は既定値に、上限2000・下限1でclamp。
  const parsed = parseInt(req.query.lines || '200', 10);
  const lines = Number.isFinite(parsed) && parsed > 0 ? Math.min(2000, parsed) : 200;
  try {
    const content = fs.readFileSync(LOG_FILE, 'utf-8');
    const all = content.split('\n');
    const tail = all.slice(-lines).join('\n');
    res.type('text/plain; charset=utf-8').send(tail);
  } catch {
    res.type('text/plain').send('(ログファイルなし)');
  }
});

export default router;
