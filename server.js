import express from 'express';
import session from 'express-session';
import connectSqlite3 from 'connect-sqlite3';
import BetterSqlite3 from 'better-sqlite3';
import bcrypt from 'bcryptjs';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';
import { createRequire } from 'module';
import { monitorEventLoopDelay, performance } from 'node:perf_hooks';
import linegiftRouter from './apps/linegift-sync/router.js';
import mercariRouter from './apps/mercari-sync/router.js';
import rakutenYahooSyncRouter from './apps/rakuten-yahoo-sync/router.js';
import aesRouter, { startPythonBackend, stopPythonBackend } from './apps/aes-pdf-sorter/router.js';
import rankingRouter from './apps/ranking-checker/router.js';
import { startScheduler } from './apps/ranking-checker/scheduler.js';
import { startWarehouseHealthcheck } from './apps/warehouse/healthcheck.js';
import { startMetrics } from './apps/observability/metrics.js';
import { startDiskWatch } from './apps/observability/disk-watch.js';
import { bootStart, bootEnd, bootNote, bootFail, getBootId } from './apps/observability/boot-log.js';
import profitRouter from './apps/profit-calculator/router.js';
import { startNotificationJob as startInventoryNotificationJob } from './apps/profit-analysis/notify-job.js';
import { startMarginAlertJob } from './apps/profit-analysis/margin-alert-job.js';
import { startSalesNotificationJob } from './apps/biz-ops-overview/notify-job.js';
import { startRysCron } from './apps/rakuten-yahoo-sync/services/rys-cron.js';
import { startInquiryHubSyncCron, startInquiryHubOutboxCron, startInquiryHubCutoffCron } from './apps/inquiry-hub/sync/cron.js';
import { startRenderBackupCron } from './apps/render-backup/backup-render.js';
import { startCompanyDbNightlyLoadCron } from './apps/company-db/nightly.mjs';
import fbaRouter from './apps/fba-replenishment/router.js';
import fbaPublicPrintRouter from './apps/fba-replenishment/public-router.js';
import warehouseRouter from './apps/warehouse/router.js';
import ordersLookupRouter from './apps/warehouse/orders-lookup-router.js';
import mirrorRouter from './apps/warehouse-mirror/router.js';
import amazonAccountingRouter from './apps/amazon-accounting/router.js';
import amazonUsaAccountingRouter from './apps/amazon-usa-accounting/router.js';
import rakutenAccountingRouter from './apps/rakuten-accounting/router.js';
import aupayAccountingRouter from './apps/aupay-accounting/router.js';
import yahooAccountingRouter from './apps/yahoo-accounting/router.js';
import linegiftAccountingRouter from './apps/linegift-accounting/router.js';
import qoo10AccountingRouter from './apps/qoo10-accounting/router.js';
import fbaProfitabilityRouter from './apps/fba-profitability/router.js';
import mercariAccountingRouter from './apps/mercari-accounting/router.js';
import profitAnalysisRouter from './apps/profit-analysis/router.js';
import expectedProfitSyncRouter from './apps/expected-profit/publish-api.js';
import companyDbSyncRouter from './apps/company-db/router.mjs';
import amazonDashboardRouter from './apps/amazon-dashboard/router.js';
import rakutenAnalyticsRouter from './apps/rakuten-analytics/router.js';
import yahooAnalyticsRouter from './apps/yahoo-analytics/router.js';
import aupayAnalyticsRouter from './apps/aupay-analytics/router.js';
import qoo10AnalyticsRouter from './apps/qoo10-analytics/router.js';
import bizOpsOverviewRouter from './apps/biz-ops-overview/router.js';
import productManagementListRouter from './apps/product-management-list/router.js';
import execDashboardRouter from './apps/exec-dashboard/router.js';
import mgmtAccountingRouter, { startMgmtAutoSyncScheduler } from './apps/mgmt-accounting/router.js';
import crossSellFinderRouter from './apps/cross-sell-finder/router.js';
import giftsetAssemblyRouter from './apps/giftset-assembly/router.js';
import inboundInfoRouter from './apps/inbound-info/router.js';
import { startInboundInfoCron } from './apps/inbound-info/sync-job.js';
import inboundCheckRouter from './apps/inbound-check/router.js';
import irohaWorkRouter from './apps/iroha-work/router.js';
import fbaBoxRouter from './apps/fba-box/router.js';
import { startMediaWorker as startIrohaMediaWorker } from './apps/iroha-work/media.js';
import { startIrohaPrintQueueWorker } from './apps/iroha-work/print-worker.js';
import { startNotifyOutbox as startFbaBoxNotifyOutbox } from './apps/fba-box/notify-outbox.js';
import staffRouter from './apps/staff/router.js';
import { startInboundCheckCron, startInboundCheckPrintQueueWorker } from './apps/inbound-check/sync-job.js';
import salesAnalyticsLinegiftRouter from './apps/sales-analytics-linegift/router.js';
import packingDispatchRouter, { neSyncWorkerRouter as packingDispatchNeSyncWorkerRouter } from './apps/packing-dispatch/router.js';
import packingDispatchRuleChangeApiRouter from './apps/packing-dispatch/rule-change-api.js';
import inventoryMonthlyRouter, { apiRouter as inventoryMonthlyApiRouter } from './apps/inventory-monthly/router.js';
import misShipmentRouter from './apps/mis-shipment/router.js';
import productScoutRouter, { ingestRouter as productScoutIngestRouter } from './apps/product-scout/router.js';
import shippingLogViewRouter from './apps/shipping-log/view-router.js';
import siteProductsRouter from './apps/site-products/router.js';
import siteContactRouter from './apps/site-contact/router.js';
import supplierSalesRouter from './apps/supplier-sales/router.js';
import productHubRouter, { serviceApiRouter as productHubServiceApiRouter } from './apps/product-hub/router.js';
import { startProductHubIntakeCron } from './apps/product-hub/intake-cron.js';
import productLinksRouter from './apps/product-links/router.js';
import postageRouter from './apps/postage/router.js';
import postageJudgeRouter from './apps/postage/judge-router.js';
import { startProductLinksCron } from './apps/product-links/cron.js';
import purchaseOrdersRouter from './apps/purchase-orders/router.js';
import priceUpdateRouter from './apps/price-update/router.js';
import amazonPricingRouter from './apps/amazon-pricing/router.js';
import inquiryHubRouter from './apps/inquiry-hub/router.js';
import shippingWorkRouter from './apps/shipping-work/router.js';
import pickingRouter from './apps/picking/router.js';
import pickingIngestRouter from './apps/picking/ingest-router.js';
import easyShipRouter from './apps/easy-ship/router.js';
import easyShipExtRouter from './apps/easy-ship/ext-router.js';
import selectSetRouter from './apps/select-set/router.js';
import selectSetExtRouter from './apps/select-set/ext-router.js';
import selectSetMasterRouter from './apps/select-set/master-router.js';
import fbaTrackingExtRouter from './apps/fba-replenishment/tracking-ext-router.js';
import inquiryHubAiApiRouter from './apps/inquiry-hub/ai-api.js';
import aiInsightsRouter, { aiInsightsApiRouter } from './apps/ai-insights/router.js';
import { startAiInsightsNotifyJob } from './apps/ai-insights/notify-job.js';
import supplierSalesPublicRouter from './apps/supplier-sales/public-router.js';
import serviceRouter from './apps/warehouse/service-router.js';
import { serviceAuth } from './apps/warehouse/service-auth.js';
import { neSyncControlRouter } from './apps/warehouse/ne-sync-control-router.js';
import abaExtRouter from './apps/aba-keywords/router.js';
import { isWarehouseDbReady } from './apps/warehouse/router.js';
import jobsMonitorRouter from './apps/jobs-monitor/router.js';
import { startJobsMonitor } from './apps/jobs-monitor/notify-job.js';
import stockBotRouter, { stockBotAuth } from './apps/stock-bot/router.js';
import shohyoLinksRouter from './apps/shohyo-links/router.js';
import { startShohyoAttachCron } from './apps/shohyo-links/attach-job.js';
import { apps, warehouseVariantDashboardApps } from './lib/portal-apps.js';
import { dashboardLocals, validateRegistry } from './lib/portal-dashboard.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
const SQLiteStore = connectSqlite3(session);

// --- 設定 ---
const PORT = process.env.PORT || 3000;
const SESSION_SECRET = process.env.SESSION_SECRET || 'dev-secret-change-me';
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const USERS_FILE = path.join(DATA_DIR, 'users.json');

// production では MIRROR_SYNC_KEY 未設定だと sync endpoint が無防備になるため即起動失敗。
// dev で skip したい場合は ALLOW_INSECURE_MIRROR_SYNC=1 を明示する。
if (process.env.NODE_ENV === 'production'
    && !process.env.MIRROR_SYNC_KEY
    && process.env.ALLOW_INSECURE_MIRROR_SYNC !== '1') {
  console.error('[FATAL] MIRROR_SYNC_KEY 未設定で production 起動不可 (ALLOW_INSECURE_MIRROR_SYNC=1 で回避可)');
  process.exit(78);
}

// --- データディレクトリ初期化 ---
function ensureDataDir() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
}
ensureDataDir();

// --- ユーザー永続化 ---

function loadUsers() {
  try {
    if (fs.existsSync(USERS_FILE)) {
      return JSON.parse(fs.readFileSync(USERS_FILE, 'utf-8'));
    }
  } catch (e) {
    console.warn('[Users] 読み込み失敗:', e.message);
  }
  return null;
}

function saveUsers(users) {
  ensureDataDir();
  fs.writeFileSync(USERS_FILE, JSON.stringify(users, null, 2), 'utf-8');
}

// 初期ユーザー or ファイルから読み込み
let users = loadUsers();
if (!users) {
  // 初回起動: 管理者ユーザーを作成
  users = [
    {
      email: 'd.nakahara@b-faith.biz',
      passwordHash: bcrypt.hashSync(process.env.PORTAL_PASS || 'changeme', 10),
      displayName: '中原 大輔',
      role: 'admin',
      allowedApps: '*',
    },
  ];
  saveUsers(users);
  console.log('[Users] 初期管理者ユーザーを作成しました');
}

// --- ミドルウェア ---
app.set('trust proxy', 1); // Cloudflare Tunnel経由
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));

// --- Liveness / Readiness probes (loopback only) ---
// C:\tools\watchdog\watchdog.ps1 が 30秒毎に http://127.0.0.1:3000/{livez,readyz} を叩く。
// loopback 限定なので外部からは 404 を返す (情報漏えい / 偵察対策)。
//
// trust proxy が有効なので req.ip は X-Forwarded-For を信頼する。実IPは
// req.socket.remoteAddress を直接見る必要がある。Cloudflare Tunnel経由でも
// この値は CF のIPであって 127.0.0.1 にはならない。
function loopbackOnly(req, res, next) {
  const sock = req.socket?.remoteAddress || '';
  if (sock === '127.0.0.1' || sock === '::1' || sock === '::ffff:127.0.0.1') return next();
  return res.status(404).end();
}
app.get('/livez', loopbackOnly, (req, res) => {
  // Express が応答できる = プロセス生存 + event loop 健全。それ以上は判定しない。
  res.status(200).json({ status: 'alive', pid: process.pid });
});
app.get('/readyz', loopbackOnly, (req, res) => {
  // 「再起動で改善しうる必須初期化」だけを見る。
  // - warehouse.db: 主要データソース、未init は 503 → watchdog が再起動を打ってよい
  // - AES-Python など「再起動で直らない」系は readyz には入れない (無意味な再起動ループ防止)
  if (!isWarehouseDbReady()) {
    return res.status(503).json({ status: 'not_ready', reason: 'warehouse-db' });
  }
  res.status(200).json({ status: 'ready', pid: process.pid });
});

// === [perf] 一時計測ミドルウェア（「重い」原因の切り分け用） ===========================
// PERF_LOG=1 のときだけ有効。OFF時はオーバーヘッドゼロ（middleware を一切挟まない）。
// 出力は JSON 1行。閾値超過のみログするので Render ログを汚さない。機微値は出さない。
//  - perf.req  : リクエスト総時間 totalMs / セッションストア読込 sessionMs / 応答バイト
//  - perf.loop : イベントループ遅延 p50/p95/p99 と RSS/heap/external（同期ブロック検知）
const PERF_ON = process.env.PERF_LOG === '1' || process.env.PERF_LOG === 'true';
// 下限クランプ: 不正/極小値で自爆的にログ・CPUを増やさない（Codex指摘）
const PERF_REQ_MS = Math.max(50, parseInt(process.env.PERF_REQ_MS, 10) || 300);   // この ms 以上の req だけログ
const PERF_LOOP_MS = Math.max(5000, parseInt(process.env.PERF_LOOP_MS, 10) || 30000); // ループ計測の出力間隔
const PERF_SESSION_MS = Math.max(20, parseInt(process.env.PERF_SESSION_MS, 10) || 50); // 単発セッション操作のログ閾値
// パス中の業務ID(SKU/ASIN/商品コード/一時ファイルID等)を Render ログに残さない（Codex指摘）。
// ルート形状は保ちつつ、ID らしいセグメントを :x に伏せる。
function perfSafePath(p) {
  if (!p) return p;
  return p.split('/').map((seg) => {
    if (!seg) return seg;
    if (/^\d+$/.test(seg)) return ':x';                       // 数値ID
    if (seg.length >= 20) return ':x';                        // 長いトークン/ファイルID
    if (/\d/.test(seg) && /[A-Za-z]/.test(seg) && seg.length >= 8) return ':x'; // SKU/ASIN風
    return seg;
  }).join('/');
}
// セッションストア操作の集計（loop ごとに flush）。set/touch=書込はネットワークディスクfsyncで重くなりやすい。
const perfSession = { get: [0, 0], set: [0, 0], touch: [0, 0], destroy: [0, 0] }; // op -> [count, totalMs]
function wrapSessionStore(store) {
  if (!PERF_ON || !store) return store;
  for (const op of ['get', 'set', 'touch', 'destroy']) {
    const orig = store[op];
    if (typeof orig !== 'function') continue;
    store[op] = function (...args) {
      const cb = args[args.length - 1];
      if (typeof cb !== 'function') return orig.apply(this, args);
      const s = performance.now();
      args[args.length - 1] = function (...cbArgs) {
        const ms = performance.now() - s;
        perfSession[op][0] += 1;
        perfSession[op][1] += ms;
        if (ms >= PERF_SESSION_MS) console.log(JSON.stringify({ t: 'perf.session', op, ms: Math.round(ms) }));
        return cb.apply(this, cbArgs);
      };
      return orig.apply(this, args);
    };
  }
  return store;
}
let _perfMonitorStarted = false;
function startPerfMonitor() {
  if (!PERF_ON || _perfMonitorStarted) return;
  _perfMonitorStarted = true;
  const eld = monitorEventLoopDelay({ resolution: 20 });
  eld.enable();
  let lastElu = performance.eventLoopUtilization();
  const timer = setInterval(() => {
    const elu = performance.eventLoopUtilization(lastElu);
    lastElu = performance.eventLoopUtilization();
    const mem = process.memoryUsage();
    // セッション集計のスナップショット＆リセット
    const sess = {};
    for (const op of ['get', 'set', 'touch', 'destroy']) {
      sess[op] = perfSession[op][0];
      sess[op + 'Ms'] = Math.round(perfSession[op][1]);
      perfSession[op][0] = 0; perfSession[op][1] = 0;
    }
    console.log(JSON.stringify({
      t: 'perf.loop',
      elu: Number(elu.utilization.toFixed(3)),        // 1.0 に近いほどループが詰まっている
      d50: Math.round(eld.percentile(50) / 1e6),
      d95: Math.round(eld.percentile(95) / 1e6),      // p95 遅延(ms)が跳ねる時間帯=ブロック発生
      d99: Math.round(eld.percentile(99) / 1e6),
      rssMb: Math.round(mem.rss / 1048576),
      heapMb: Math.round(mem.heapUsed / 1048576),
      extMb: Math.round((mem.external || 0) / 1048576),
      sess,                                            // セッションストア get/set/touch の件数と合計ms
    }));
    eld.reset();
  }, PERF_LOOP_MS);
  timer.unref?.();
  console.log(`[perf] monitor started (reqLog>=${PERF_REQ_MS}ms, loopEvery=${PERF_LOOP_MS}ms)`);
}
if (PERF_ON) {
  app.use((req, res, next) => {
    const start = performance.now();
    req._perf = { start, sessionMs: 0 };
    res.on('finish', () => {
      const totalMs = performance.now() - start;
      if (totalMs >= PERF_REQ_MS) {
        const len = Number(res.getHeader('content-length'));
        console.log(JSON.stringify({
          t: 'perf.req',
          m: req.method,
          path: perfSafePath((req.baseUrl || '') + (req.path || '')),
          s: res.statusCode,
          totalMs: Math.round(totalMs),
          sessionMs: Math.round(req._perf.sessionMs),   // session load(get) の所要。書込は perf.loop.sess を見る
          bytes: Number.isFinite(len) ? len : null,
        }));
      }
    });
    next();
  });
}

app.use(express.urlencoded({ extended: true }));
// グローバル JSON parser (10MB)。ただし大容量受信が必要な endpoint は除外。
// 除外対象 endpoint は route 側で独自の parser (例: 50MB) を定義する。
// 単純に全体 limit を上げると未認可リクエストのDoS面が広がるため、例外列挙方式を採る。
const LARGE_BODY_ROUTES = [
  '/apps/ranking-checker/data/import',      // 履歴付き JSON バックアップ復元 (router 側で 50MB)
  // /service-api/* は serviceAuth 後に独自 parser が走るため、この配列ではなく
  // 上記 middleware で startsWith('/service-api/') として一括 exempt している。
  // /apps/mirror/api/sync* は requireSyncKey 後に独自 parser (8MB) が走るため、
  // startsWith 判定で一括 exempt している (下の startsWith 分岐参照)。
];
const globalJsonParser = express.json({ limit: '10mb' });
app.use((req, res, next) => {
  if (req.method === 'POST') {
    // trailing slash 差異を許容して比較
    const normalizedPath = req.path.replace(/\/+$/, '') || '/';
    // /service-api/* は serviceAuth + 専用 parser が後段 (app.use('/service-api', ...)) で
    // 走るためここでは parse しない。Bearer 検証前に body を読まないことで
    // 未認可 DoS 面を閉じる。
    if (normalizedPath.startsWith('/service-api/') || normalizedPath === '/service-api') return next();
    // /apps/mirror/api/sync* も同様に API key 認証前 body parse を避ける。
    if (normalizedPath.startsWith('/apps/mirror/api/sync')) return next();
    // /api/ai-insights/service/* は AI_INSIGHT_SERVICE_TOKEN 認証後に専用 parser (2MB) が走る。
    if (normalizedPath.startsWith('/api/ai-insights/service')) return next();
    // /apps/stock-bot は Chat Bearer 検証 (stockBotAuth) 後に専用 parser (256kb) が走る。
    // 認証前に body を読まない (未認可 DoS 面を閉じる)
    if (normalizedPath.startsWith('/apps/stock-bot')) return next();
    // /apps/postage/judge-api は x-api-key 検証後に専用 parser (256kb) が走る (伝票出しPCのランチャー向け)
    if (normalizedPath.startsWith('/apps/postage/judge-api')) return next();
    // mirror read API (GET専用、監査S-2で認証追加) への POST も認証前 body parse を避ける
    // (POST は router 側に route が無く 404 になるだけなので parse 不要)。
    if (/^\/apps\/mirror\/api\/(products|sales|status|download)(\/|$)/.test(normalizedPath)) return next();
    // 会計系 /import-history (監査S-4でアプリ別envトークン化) は requireImportKey 認証後に
    // 専用 parser (importJsonParser) が走るため、認証前 body parse を避ける。
    if (/^\/apps\/[a-z0-9-]+-accounting\/import-history$/.test(normalizedPath)) return next();
    // mgmt-accounting は mount 側で「認証ゲート → 50MB parser」の順に処理する (Excel seed 等の
    // 大容量投入があるため global 10MB を通すと mount 側 50MB が無効化される問題も同時に解消)。
    if (normalizedPath.startsWith('/apps/mgmt-accounting')) return next();
    // /aba-ext-api は router 内で「x-api-key 認証 → 64KB parser」の順に処理 (認証前 body parse を避ける)
    if (normalizedPath.startsWith('/aba-ext-api')) return next();
    // /apps/easy-ship/ext-api も同様に router 内で「x-api-key 認証 → 64KB parser」の順に処理
    if (normalizedPath.startsWith('/apps/easy-ship/ext-api')) return next();
    // /apps/fba-replenishment/ext-api も同様 (x-api-key 認証 → 64KB parser の順に router 内で処理)
    if (normalizedPath.startsWith('/apps/fba-replenishment/ext-api')) return next();
    // /apps/select-set/ext-api も同様 (NE伝票画面のChrome拡張向け)
    if (normalizedPath.startsWith('/apps/select-set/ext-api')) return next();
    // /apps/select-set/master-api は miniPC が x-sync-key で取りに来るマスタ配信 (Render側で有効)
    if (normalizedPath.startsWith('/apps/select-set/master-api')) return next();
    if (LARGE_BODY_ROUTES.includes(normalizedPath)) return next();
  }
  return globalJsonParser(req, res, next);
});
app.use(express.static(path.join(__dirname, 'public')));
// 📷 入荷受付チェック「商品から探す」のカメラ読み取りに使うデコーダ (zxing-wasm)。
// 🚨 **Safari は BarcodeDetector (Shape Detection API) を実装していない** ので、iPad でカメラから
//    バーコードを読むにはデコーダを自前で配る必要がある (2026-09-07。最初の実装はこれを知らずに
//    BarcodeDetector を使い、iPad では一度もカメラが起動しなかった)。
// ⭐node_modules から直接配る = **JS と wasm の版が必ず揃う**。public/ に写すと片方だけ古くなる。
//   Cache-Control は付けない (既定 = 毎回 ETag 検証 → 304)。版が変わったときに
//   JS だけ新しく wasm が古い、という組み合わせを作らないため。読むのはボタンを押したときだけ
// 🚨 解決に失敗してもポータル全体を落とさない (カメラが使えないだけで、検索も値札も動く)
try {
  const zxingWasm = createRequire(import.meta.url).resolve('zxing-wasm/reader/zxing_reader.wasm');
  app.use('/vendor/zxing-wasm', express.static(path.dirname(path.dirname(zxingWasm))));
} catch (e) {
  console.warn('[server] zxing-wasm を配れません (📷 カメラ読み取りは使えません):', e.message);
}

// セッションストア(connect-sqlite3)は全リクエストで sessions.db を読み書きする。Render の
// network-attached disk では rollback-journal モードの fsync が遅く、どのページでも TTFB を
// 底上げしてしまう。journal_mode=WAL は DB ヘッダに永続記録されるため、ここで一度だけ
// better-sqlite3 で設定しておけば、後段の connect-sqlite3 接続も WAL を引き継ぐ(書き込み軽量化)。
// 認証ロジックには一切触れない。失敗してもセッションは動くので起動は止めない(best-effort)。
try {
  const SESSIONS_DB = path.join(DATA_DIR, 'sessions.db');
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  const sdb = new BetterSqlite3(SESSIONS_DB);
  const mode = sdb.pragma('journal_mode = WAL', { simple: true });
  sdb.pragma('busy_timeout = 5000');
  sdb.close();
  console.log(`[session-store] sessions.db journal_mode=${mode}`);
} catch (e) {
  console.warn('[session-store] WAL 設定スキップ:', e.message);
}

const sessionMiddleware = session({
  store: wrapSessionStore(new SQLiteStore({ db: 'sessions.db', dir: DATA_DIR })),
  secret: SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  // rolling: 使っている間はセッションを延長 (maxAge=無操作24時間で失効)。
  // 従来は「ログインから固定24時間」で、CSV取込の途中など作業の真ん中で session_expired になった
  // (2026-07-14 発注補助のロジザード取込で実発生)。store.touch (期限UPDATE) は resave:false+touch実装
  // ストアでは従来から毎リクエスト実行されており、rolling で新たに増えるのは毎応答の Set-Cookie
  // (ブラウザ側Cookie期限の更新=ストア側期限との一致) のみ
  rolling: true,
  cookie: {
    maxAge: 1 * 24 * 60 * 60 * 1000, // 無操作24時間
    httpOnly: true,
    secure: true,
    sameSite: 'lax',
  }
});
// [perf] PERF_LOG=1 のときはセッションストア読込(store.get)の所要時間を計測。
// connect-sqlite3 はネットワークディスク上の sessions.db を全リクエストで読むため、
// ここが遅いと「どのページでも常に重い」の主因になりうる（Codex 本命仮説の検証）。
app.use(PERF_ON
  ? (req, res, next) => {
      const s = performance.now();
      sessionMiddleware(req, res, (err) => {
        if (req._perf) req._perf.sessionMs = performance.now() - s;
        next(err);
      });
    }
  : sessionMiddleware);

// --- 認証ミドルウェア ---
// /api/ パスへの未認証アクセスはHTMLリダイレクトではなくJSONで401/403を返す
// (fetch が追従したログインHTMLを res.json() でパースして壊れるのを防ぐ)
function isApiRequest(req) {
  return req.path.startsWith('/api/') || req.xhr || (req.get('accept') || '').includes('application/json');
}

// 認証後に元のURLへ戻すための保存ヘルパ
// オープンリダイレクト防止: 相対パス(`/...`)のみ許可、`//` や `/login` 自身は除外
function rememberReturnTo(req) {
  if (req.method !== 'GET') return;
  const url = req.originalUrl || req.url;
  if (!url || !url.startsWith('/') || url.startsWith('//')) return;
  if (url === '/login' || url.startsWith('/login?')) return;
  req.session.returnTo = url;
}

function popReturnTo(req) {
  const dest = req.session && req.session.returnTo;
  if (req.session) delete req.session.returnTo;
  if (typeof dest === 'string' && dest.startsWith('/') && !dest.startsWith('//')) return dest;
  return '/';
}

function requireAuth(req, res, next) {
  if (req.session && req.session.authenticated) return next();
  if (isApiRequest(req)) return res.status(401).json({ error: 'session_expired' });
  rememberReturnTo(req);
  res.redirect('/login');
}

// アプリ別アクセス制御ミドルウェア
function requireAppAccess(appId) {
  return (req, res, next) => {
    if (!req.session || !req.session.authenticated) {
      if (isApiRequest(req)) return res.status(401).json({ error: 'session_expired' });
      rememberReturnTo(req);
      return res.redirect('/login');
    }
    const allowed = req.session.allowedApps;
    if (allowed === '*' || (Array.isArray(allowed) && allowed.includes(appId))) {
      return next();
    }
    if (isApiRequest(req)) return res.status(403).json({ error: 'forbidden' });
    res.status(403).render('forbidden', { username: req.session.email, displayName: req.session.displayName });
  };
}

// 管理者専用ミドルウェア
function requireAdmin(req, res, next) {
  if (!req.session || !req.session.authenticated) return res.redirect('/login');
  if (req.session.role !== 'admin') {
    return res.status(403).render('forbidden', { username: req.session.email, displayName: req.session.displayName });
  }
  next();
}

// --- アプリ一覧 ---
// 中身は lib/portal-apps.js (registry)。トップ画面の組み立ては lib/portal-dashboard.js。
// 書き方の抜け (分類・一行説明・キーワード) は起動時に警告だけ出す (起動は止めない: 止めると全アプリが落ちる)
for (const problem of validateRegistry()) console.warn(`[Portal] アプリ一覧 (lib/portal-apps.js): ${problem}`);

// ─── PORTAL_VARIANT (どの環境で動かしているか) ───
// 'render'    : 社内ツールポータル本体 (default、bfaith-portal.onrender.com 等)
// 'warehouse' : miniPC 上の warehouse / マスタ登録専用 (wh.bfaith-wh.uk)
//               ダッシュボードは「マスタ登録」と admin の「ユーザー管理」だけに絞る
// fail-fast: 未知の値なら起動時に exit (typo を運用に持ち込ませない)
const PORTAL_VARIANT = (process.env.PORTAL_VARIANT || 'render').toLowerCase();
if (!['render', 'warehouse'].includes(PORTAL_VARIANT)) {
  console.error(`FATAL: PORTAL_VARIANT は 'render' か 'warehouse': "${PORTAL_VARIANT}"`);
  process.exit(2);
}
console.log(`[Portal] PORTAL_VARIANT=${PORTAL_VARIANT}`);

// warehouse variant のトップに出すカード。定義と設計判断のメモは lib/portal-apps.js
const WAREHOUSE_VARIANT_DASHBOARD_APPS = warehouseVariantDashboardApps;

/**
 * variant の scope 内で表示・編集対象にする app id 集合。
 *   - render variant: null (= 全 apps)
 *   - warehouse variant: WAREHOUSE_VARIANT_DASHBOARD_APPS で挙げた requiresAccess の集合
 *
 * 用途: /admin/users と /admin/permissions の UI で、その variant で実体が動いていない
 * Render 専用アプリのチェックボックスを出さない (誤操作・誤解防止)。
 * 権限保存時は variant scope 外の既存権限を保持 (防衛策、users.json が誤って共有された
 * 場合に Render 側の権限を巻き戻さない)。
 */
function variantVisibleAppIds() {
  if (PORTAL_VARIANT === 'warehouse') {
    return new Set(WAREHOUSE_VARIANT_DASHBOARD_APPS.map(a => a.requiresAccess));
  }
  return null;
}

function variantVisibleApps() {
  const ids = variantVisibleAppIds();
  return ids === null ? apps : apps.filter(a => ids.has(a.id));
}

/**
 * 提出された allowedApps をマージ保存用に整形する。
 *   - variant scope 外の既存 allowedApps エントリは保持
 *   - submitted は variant scope 内に絞ってから足す (UIに出してない app id を勝手に
 *     混入されないように、サーバ側でもう一度濾す)
 */
function mergeAllowedApps(currentAllowed, submittedAllowed) {
  const submittedArr = Array.isArray(submittedAllowed)
    ? submittedAllowed
    : (submittedAllowed ? [submittedAllowed] : []);
  const visibleIds = variantVisibleAppIds();
  if (visibleIds === null) {
    // render variant: 全 apps が編集可能 → 完全置換
    return submittedArr;
  }
  const currentArr = Array.isArray(currentAllowed) ? currentAllowed : [];
  const preserved = currentArr.filter(id => !visibleIds.has(id));
  const accepted = submittedArr.filter(id => visibleIds.has(id));
  return [...preserved, ...accepted];
}

// --- ルート ---

// ログインページ
app.get('/login', (req, res) => {
  if (req.session.authenticated) return res.redirect(popReturnTo(req));
  res.render('login', { error: null });
});

// ログイン処理
app.post('/login', (req, res) => {
  const { email, password } = req.body;
  const user = users.find(u => u.email.toLowerCase() === email.toLowerCase());
  if (user && bcrypt.compareSync(password, user.passwordHash)) {
    req.session.authenticated = true;
    req.session.email = user.email;
    req.session.displayName = user.displayName;
    req.session.role = user.role;
    req.session.allowedApps = user.allowedApps;
    return res.redirect(popReturnTo(req));
  }
  res.render('login', { error: 'メールアドレスまたはパスワードが正しくありません' });
});

// ログアウト
app.get('/logout', (req, res) => {
  req.session.destroy(() => res.redirect('/login'));
});

// ダッシュボード (ポータルトップ)。並べる中身は lib/portal-dashboard.js が allowedApps と
// PORTAL_VARIANT から組み立てる (warehouse variant は「マスタ登録」だけ。表示は requireAppAccess の認可と揃えてある)
app.get('/', requireAuth, (req, res) => {
  if (!req.session.allowedApps) {
    return req.session.destroy(() => res.redirect('/login'));
  }
  res.render('dashboard', dashboardLocals({ session: req.session, variant: PORTAL_VARIANT }));
});

// --- パスワード変更 ---
app.get('/change-password', requireAuth, (req, res) => {
  res.render('change-password', {
    displayName: req.session.displayName,
    username: req.session.email,
    error: null, success: false,
  });
});

app.post('/change-password', requireAuth, (req, res) => {
  const { currentPassword, newPassword, confirmPassword } = req.body;
  const user = users.find(u => u.email === req.session.email);

  if (!user || !bcrypt.compareSync(currentPassword, user.passwordHash)) {
    return res.render('change-password', {
      displayName: req.session.displayName, username: req.session.email,
      error: '現在のパスワードが正しくありません', success: false,
    });
  }
  if (newPassword.length < 6) {
    return res.render('change-password', {
      displayName: req.session.displayName, username: req.session.email,
      error: 'パスワードは6文字以上で設定してください', success: false,
    });
  }
  if (newPassword !== confirmPassword) {
    return res.render('change-password', {
      displayName: req.session.displayName, username: req.session.email,
      error: '新しいパスワードが一致しません', success: false,
    });
  }

  user.passwordHash = bcrypt.hashSync(newPassword, 10);
  saveUsers(users);
  res.render('change-password', {
    displayName: req.session.displayName, username: req.session.email,
    error: null, success: true,
  });
});

// アプリルート
app.use('/apps/linegift-sync', requireAppAccess('linegift-sync'), linegiftRouter);
app.use('/apps/mercari-sync', requireAppAccess('mercari-sync'), mercariRouter);
app.use('/apps/rakuten-yahoo-sync', requireAppAccess('rakuten-yahoo-sync'), rakutenYahooSyncRouter);
app.use('/apps/aes-pdf-sorter', requireAppAccess('aes-pdf-sorter'), aesRouter);
app.use('/apps/ranking-checker', requireAppAccess('ranking-checker'), rankingRouter);
app.use('/apps/profit-calculator', requireAppAccess('profit-calculator'), profitRouter);
// FBA納品 → 福山通運の伝票CSV: Chrome拡張向けAPI (x-api-key 認証・fail-closed) は
// セッション認証付き本体より先に mount する
app.use('/apps/fba-replenishment/ext-api', fbaTrackingExtRouter);
app.use('/apps/fba-replenishment', requireAppAccess('fba-replenishment'), fbaRouter);
// 子会社向け公開印刷 (ログイン不要・トークン認可)。requireAppAccess の外側に置く。
app.use('/print', fbaPublicPrintRouter);
// 仕入れ先向け 売れ筋共有 (ログイン不要・トークンURL)。requireAuth/requireAppAccess の外側。
app.use('/share', supplierSalesPublicRouter);
app.use('/apps/warehouse', requireAppAccess('warehouse'), warehouseRouter);

// === Mirror subtree middleware (Codex 6周レビュー反映) ===
// accessLog は /apps/mirror 全体に掛ける (401含めて全requestを観測できる)。
// 認証+8MB parser+parser error handler は /apps/mirror/api/sync* のみ (mutation専用)。
// read API (/apps/mirror/api/products 等) は「portalセッション or MIRROR_READ_TOKEN」必須
// (設計監査 2026-07-06 S-2: 原価・仕入先・全モール売上・全件CSVが公開URLから素通しだった)。
function mirrorAccessLog(req, res, next) {
  const start = Date.now();
  res.on('finish', () => {
    const dur = Date.now() - start;
    const cl = req.headers['content-length'] || '';
    const rb = req.rawBodyBytes ?? '';
    const runId = req.headers['x-sync-run-id'] || '';
    console.log(
      `[Mirror-IN] boot=${getBootId()} method=${req.method} path=${req.path} ` +
      `ip=${req.ip} content_length=${cl} raw_bytes=${rb} ` +
      `status=${res.statusCode} duration_ms=${dur} sync_run_id=${runId}`
    );
  });
  next();
}

// MIRROR_SYNC_KEY 必須化 (未設定 production は起動失敗で既に弾かれているので、ここは二重防御)。
// dev で skip したい時は ALLOW_INSECURE_MIRROR_SYNC=1 を明示。
function requireSyncKeyStrict(req, res, next) {
  const key = process.env.MIRROR_SYNC_KEY;
  if (!key) {
    if (process.env.ALLOW_INSECURE_MIRROR_SYNC === '1') return next();
    return res.status(503).json({ error: 'mirror_sync_key_unset' });
  }
  const provided = req.headers['x-sync-key'] || req.query.sync_key;
  if (provided !== key) return res.status(401).json({ error: 'invalid_sync_key' });
  next();
}

// express.json が投げる parser error を分類して log + 適切な status code で返す。
function mirrorParserErrorHandler(err, req, res, next) {
  if (err && err.type === 'entity.too.large') {
    console.error(
      `[Mirror-ERR] entity.too.large path=${req.path} ip=${req.ip} ` +
      `limit=${err.limit} content_length=${err.length}`
    );
    return res.status(413).json({ error: 'payload_too_large', limit: err.limit });
  }
  if (err && err.type === 'encoding.unsupported') {
    console.error(`[Mirror-ERR] encoding.unsupported path=${req.path} encoding=${err.encoding}`);
    return res.status(415).json({ error: 'unsupported_encoding' });
  }
  if (err && err.type === 'request.aborted') {
    console.warn(`[Mirror-ERR] request.aborted path=${req.path} ip=${req.ip}`);
    return;  // client gone, no response
  }
  return next(err);
}

app.use('/apps/mirror', mirrorAccessLog);

// /api/sync* のみ API キー認証 + 8MB parser + error handler を適用。
app.use('/apps/mirror/api/sync', requireSyncKeyStrict);
app.use('/apps/mirror/api/sync', express.json({
  limit: '12mb',                                      // ミニPC sync-to-render.js の 9MB chunk を確実に受ける余裕値
  inflate: false,                                     // gzip は 415 で reject
  verify: (req, res, buf) => { req.rawBodyBytes = buf.length; },
}));
app.use('/apps/mirror/api/sync', mirrorParserErrorHandler);

// read API 認証 (監査 S-2 対応): portal セッション or x-read-token (MIRROR_READ_TOKEN) のどちらかで許可。
//   ・対象は認証が無かった read 系 (products / sales配下全体 / status / download配下全体)。
//     sales/download は prefix mount なので将来の追加 route も自動的に保護される (安全側デフォルト)。
//   ・独自 token を持つ既存ルート (/api/sync* = sync key, /api/pml/* = PML_*_TOKEN,
//     /api/sku-master/* = MIRROR_READ_TOKEN) は各自の認証を維持するため対象外 (二重認証にしない)。
//   ・session 経路は requireAppAccess('warehouse') 相当の認可まで要求 (Codex R2 high:
//     原価・仕入先・全モール売上を含むため、ログイン済みなら誰でも可では低権限ユーザーへ横展開する)。
//   ・token 提示あり + MIRROR_READ_TOKEN 未設定は 503 (requireReadToken と同じ fail-closed シグナル)、
//     不一致は 401。warehouse 権限なし session は 403、session も token も無ければ 401。
//   ・token は header only (query 受理は URL/アクセスログ残留のため禁止。requireReadToken と同方針)。
//   ・x-sync-key (MIRROR_SYNC_KEY) も許可: miniPC sync-to-render.js が同期後の件数検証で
//     /api/status を読む (2026-07-07 朝、認証追加でこの読み取りが 401 → 全カウント0の
//     誤「データ不一致」アラートが発生)。sync key 保持者は write 権限持ち = read は当然許可できる。
function requireSessionOrReadToken(req, res, next) {
  const sessionAuthed = !!(req.session && req.session.authenticated);
  if (sessionAuthed) {
    const allowed = req.session.allowedApps;
    if (allowed === '*' || (Array.isArray(allowed) && allowed.includes('warehouse'))) return next();
  }
  const providedSync = req.headers['x-sync-key'];
  if (providedSync) {
    const syncKey = process.env.MIRROR_SYNC_KEY;
    if (!syncKey) return res.status(503).json({ error: 'mirror_sync_key_unset' });
    if (providedSync === syncKey) return next();
    return res.status(401).json({ error: 'invalid_sync_key' });
  }
  const provided = req.headers['x-read-token'];
  if (provided) {
    const token = process.env.MIRROR_READ_TOKEN;
    if (!token) return res.status(503).json({ error: 'mirror_read_token_unset' });
    if (provided === token) return next();
    return res.status(401).json({ error: 'invalid_read_token' });
  }
  if (sessionAuthed) return res.status(403).json({ error: 'forbidden' });
  return res.status(401).json({ error: 'auth_required' });
}
app.use(
  [
    '/apps/mirror/api/products',
    '/apps/mirror/api/sales',
    '/apps/mirror/api/status',
    '/apps/mirror/api/download',
  ],
  requireSessionOrReadToken
);

// mirrorRouter 内部の `router.post('/api/sync', requireSyncKey, ...)` が二重防御として残る。
app.use('/apps/mirror', mirrorRouter);

// === Lookup API (Render→ミニPC、read-only 専用) ===
// 誤出荷管理システム (apps/mis-shipment) からの注文番号 lookup 用。
// 専用 WAREHOUSE_LOOKUP_TOKEN で認証 (SERVICE_TOKEN/WAREHOUSE_API_KEY とは完全分離)。
// 設計書: g:/共有ドライブ/AI_reference/システム設計/誤出荷管理システム_設計書_v5.md (中身 v7.3)
// blast radius 最小化: read-only token、専用 path、専用 router、未設定時 fail-closed (503)。
// serviceRouter (/service-api) より前にマウントして path 衝突を予防 (Codex round 14 指摘)。
app.use('/lookup-api', express.json({ limit: '64kb' }), ordersLookupRouter);

// サービスAPI（Render→ミニPC、トークン認証）。
// rankcheck の履歴込みインポートで 10MB を超える可能性があるため 50MB まで許容。
// 未認可 DoS 回避のため、serviceAuth を body parser **より前** に置く。
// そうしないと token 無しリクエストが最大 50MB を parse してから 401 になる。
//
// SERVICE_RAW_PATHS にリストした path は body parser を skip して req を
// stream のまま route に渡す (大容量アップロード用)。
const SERVICE_RAW_PATHS = ['/rankcheck/upload-legacy-json'];
app.use('/service-api', serviceAuth, (req, res, next) => {
  if (req.method === 'POST' && SERVICE_RAW_PATHS.includes(req.path)) return next();
  return express.json({ limit: '50mb' })(req, res, next);
}, serviceRouter);

// NE 反映 worker 起動 control endpoint (2026-06-06 PR-C 構成 4)
// 既存 serviceAuth と独立した別 mount で、専用 Bearer (MINIPC_NE_SYNC_RUN_KEY) で保護。
// CF Access は edge で前段、ここは Bearer fail-closed + spawn 即返却。
//
// Codex R1 High 1 対応: ENABLE_MINIPC_NE_SYNC_CONTROL=1 のミニPC 専用 env で mount 自体を gate。
// Render では mount しない (= worker spawn endpoint が Render に立たない、攻撃面削減)。
if (process.env.ENABLE_MINIPC_NE_SYNC_CONTROL === '1') {
  app.use('/ne-sync-control', express.json({ limit: '64kb' }), neSyncControlRouter);
  console.log('[server] ne-sync-control mounted (miniPC mode)');
}

// ABAキーワード拡張 API (セラースプライト置換)。ミニPC 専用 env で mount 自体を gate
// (ne-sync-control と同パターン: Render には endpoint 自体が立たない = 攻撃面削減)。
// 認証 (x-api-key = ABA_EXT_TOKEN, fail-closed) と 64KB parser は router 内で処理。
if (process.env.ENABLE_ABA_EXT === '1') {
  app.use('/aba-ext-api', abaExtRouter);
  console.log('[server] aba-ext-api mounted (miniPC mode)');
}

// 定期実行の見張り役 (jobs-monitor)。Render 専用 env で mount 自体を gate —
// miniPC も同じ server.js を動かすため、無条件 mount だと監視が二重に立ち
// 誤通知の温床になる (feedback_minipc_shares_portal_server_js の教訓)。
// 認証は router 内 (Bearer JOBS_MONITOR_TOKEN、health のみ公開・情報最小)。
if (process.env.JOBS_MONITOR_ENABLED === '1') {
  app.use('/apps/jobs-monitor', jobsMonitorRouter);
  startJobsMonitor();
  console.log('[server] jobs-monitor mounted');
}
// Google Chat 在庫検索ボット (Render専用)。STOCK_BOT_PROJECT_NUMBER (GCPプロジェクト番号) が
// ある環境のみ mount — miniPC は同じ server.js を動かすため未設定=非mount (二重応答防止)。
// 認証は router 内 (Google Chat の Bearer IDトークン検証 + 社内ドメイン制限、fail-closed)。
if (process.env.STOCK_BOT_PROJECT_NUMBER) {
  // stockBotAuth (Chat Bearer 検証) を parser より前に — 未認可リクエストに body を読ませない
  app.use('/apps/stock-bot', stockBotAuth, express.json({ limit: '256kb' }), stockBotRouter);
  console.log('[server] stock-bot mounted');
}
app.use('/apps/amazon-accounting', (req, res, next) => {
  if (req.path === '/import-history' && req.method === 'POST') return next();  // APIキー認証に委譲
  requireAuth(req, res, next);
}, amazonAccountingRouter);
app.use('/apps/amazon-usa-accounting', (req, res, next) => {
  if (req.path === '/import-history' && req.method === 'POST') return next();
  requireAuth(req, res, next);
}, amazonUsaAccountingRouter);
app.use('/apps/rakuten-accounting', requireAuth, rakutenAccountingRouter);
app.use('/apps/aupay-accounting', (req, res, next) => {
  if (req.path === '/import-history' && req.method === 'POST') return next();
  requireAuth(req, res, next);
}, aupayAccountingRouter);
app.use('/apps/yahoo-accounting', (req, res, next) => {
  if (req.path === '/import-history' && req.method === 'POST') return next();  // requireImportKey に委譲 (他6アプリと統一)
  requireAuth(req, res, next);
}, yahooAccountingRouter);
app.use('/apps/linegift-accounting', (req, res, next) => {
  if (req.path === '/import-history' && req.method === 'POST') return next();
  requireAuth(req, res, next);
}, linegiftAccountingRouter);
app.use('/apps/qoo10-accounting', (req, res, next) => {
  if (req.path === '/import-history' && req.method === 'POST') return next();
  requireAuth(req, res, next);
}, qoo10AccountingRouter);
app.use('/apps/fba-profitability', requireAppAccess('fba-profitability'), fbaProfitabilityRouter);
app.use('/apps/profit-analysis', requireAppAccess('profit-analysis'), profitAnalysisRouter);
// 想定利益: miniPC から世代を受け取る口。ログイン不要 (sync key 認証)。
// 既存 mirror sync と同じ流儀にする (12MB parser + parser error handler)。
// 🚨 全置換ではなく「世代を作り切ってからポインタを切り替える」ので、受信中も画面は前の世代を見る
app.use('/apps/expected-profit/sync', requireSyncKeyStrict);
app.use('/apps/expected-profit/sync', express.json({
  limit: '12mb',
  inflate: false,
  verify: (req, res, buf) => { req.rawBodyBytes = buf.length; },
}));
app.use('/apps/expected-profit/sync', mirrorParserErrorHandler);
app.use('/apps/expected-profit/sync', expectedProfitSyncRouter);
// Company DB (Postgres) の初期ロード・状態 (x-sync-key)。読み込み元の SQLite は Render の DATA_DIR にある
app.use('/apps/company-db/sync', companyDbSyncRouter);
app.use('/apps/amazon-dashboard', requireAppAccess('amazon-dashboard'), express.json({ limit: '256kb' }), amazonDashboardRouter);
app.use('/apps/rakuten-analytics', requireAppAccess('rakuten-analytics'), rakutenAnalyticsRouter);
app.use('/apps/yahoo-analytics', requireAppAccess('yahoo-analytics'), express.json({ limit: '256kb' }), yahooAnalyticsRouter);
app.use('/apps/aupay-analytics', requireAppAccess('aupay-analytics'), aupayAnalyticsRouter);
app.use('/apps/qoo10-analytics', requireAppAccess('qoo10-analytics'), express.json({ limit: '64kb' }), qoo10AnalyticsRouter);
// qoo10-analytics の parser error を JSON で返す (画面 fetch が { error } 形式を期待するため。
// mirrorParserErrorHandler は /apps/mirror 専用なのでここで個別に受ける)
app.use('/apps/qoo10-analytics', (err, req, res, next) => {
  if (err && (err.type === 'entity.too.large' || err.type === 'entity.parse.failed')) {
    return res.status(err.type === 'entity.too.large' ? 413 : 400)
      .json({ error: err.type === 'entity.too.large' ? 'リクエストが大きすぎます (64KB上限)' : 'JSON の解析に失敗しました' });
  }
  return next(err);
});
app.use('/apps/biz-ops-overview', requireAppAccess('biz-ops-overview'), bizOpsOverviewRouter);
app.use('/apps/product-management-list', requireAppAccess('product-management-list'), productManagementListRouter);
app.use('/apps/exec-dashboard', requireAppAccess('exec-dashboard'), express.json({ limit: '1mb' }), execDashboardRouter);
// AI経営レポート (apps/ai-insights): ⚙️設定画面は session、/api/ai-insights はトークン認証
// (report-input=AI_READ_TOKEN read-only / service=AI_INSIGHT_SERVICE_TOKEN。いずれも fail-closed)
app.use('/api/ai-insights', aiInsightsApiRouter);
app.use('/apps/ai-insights', requireAppAccess('ai-insights'), express.json({ limit: '256kb' }), aiInsightsRouter);
app.use('/apps/cross-sell-finder', requireAppAccess('cross-sell-finder'), crossSellFinderRouter);
app.use('/apps/giftset-assembly', requireAppAccess('giftset-assembly'), express.json({ limit: '256kb' }), giftsetAssemblyRouter);
app.use('/apps/inbound-info', requireAppAccess('inbound-info'), express.json({ limit: '256kb' }), inboundInfoRouter);
// 入荷受付チェック (iPad): picking と同じく requireAppAccess を掛けない (登録端末Cookie を通すため)。
// 認可は router 内 (セッション or 端末Cookie。管理系はセッション必須・端末登録等は admin)
app.use('/apps/inbound-check', express.json({ limit: '256kb' }), inboundCheckRouter);
// いろは在庫化 作業アプリ (iPad): inbound-check と同じく requireAppAccess を掛けない (登録端末Cookie を通すため)。
// 認可は router 内 (セッション or 端末Cookie。管理系はセッション必須・端末登録/作業者は admin)
app.use('/apps/iroha-work', express.json({ limit: '256kb' }), irohaWorkRouter);
// FBA箱詰め記録 (iPad): 同じく requireAppAccess を掛けない (登録端末Cookie を通すため)。
// 認可は router 内 (セッション or 端末Cookie。納品回管理はセッション・端末登録/作業者は admin)
app.use('/apps/fba-box', express.json({ limit: '256kb' }), fbaBoxRouter);
// スタッフマスタ (staff.db): 管理画面/API は router 内で管理者限定。/export だけトークン認証 (miniPC 同期用)
app.use('/apps/staff', express.json({ limit: '256kb' }), staffRouter);
// MF仕訳用 証憑リンク集 (apps/shohyo-links): 専用DB shohyo-links.db (DATA_DIR)。Notion「支払い関係リンク先」の移行先
// limit 8mb = MF照合画面の証憑添付 (MFの上限5MBファイル → base64で約6.7MB) を受けるため
app.use('/apps/shohyo-links', requireAppAccess('shohyo-links'), express.json({ limit: '8mb' }), shohyoLinksRouter);
app.use('/apps/sales-analytics-linegift', requireAppAccess('sales-analytics-linegift'), express.json({ limit: '256kb' }), salesAnalyticsLinegiftRouter);
// 構成 B (2026-06-05 中原さん確定): NE 反映 worker (miniPC) は session 認証なし、Bearer fail-closed のみ。
// packing-dispatch 本体 (requireAppAccess) より「前」に mount しないと、miniPC が 401/403 で弾かれる。
app.use('/apps/packing-dispatch/api/ne-sync-worker', express.json({ limit: '2mb' }), packingDispatchNeSyncWorkerRouter);
// 配送ルール変更の承認フロー (miniPC梱包画面→申請→GChat承認カード)。x-api-key 認証・セッション外
app.use('/apps/packing-dispatch/rule-change-api', express.json({ limit: '256kb' }), packingDispatchRuleChangeApiRouter);
app.use('/apps/packing-dispatch', requireAppAccess('packing-dispatch'), express.json({ limit: '2mb' }), packingDispatchRouter);
// 誤出荷管理 (apps/mis-shipment): warehouse-mirror.db 同居の f_mis_shipments を CRUD、注文 lookup は miniPC GET 経由
app.use('/apps/mis-shipment', requireAppAccess('mis-shipment'), express.json({ limit: '256kb' }), misShipmentRouter);

// 新商品企画スカウト (apps/product-scout): warehouse-mirror.db 同居の scout_* が正本。
// ⚠️/ingest だけは miniPC のバッチが叩くのでセッションを持てない。router 内で MIRROR_SYNC_KEY を
//   検証するため、社内ログインを掛けない経路として先に mount する (fail-closed: 鍵未設定なら503)。
app.use('/apps/product-scout/ingest', productScoutIngestRouter);
app.use('/apps/product-scout', requireAppAccess('product-scout'), productScoutRouter);
// 出荷件数ダッシュボード (apps/shipping-log)。
// GAS からの伝票取込 API (/apps/shipping-log/api) は 2026-08-26 に廃止 (吸い上げ全廃)。
app.use('/apps/shipping-log', requireAppAccess('shipping-log'), shippingLogViewRouter);
// コーポレートサイト向け商品スナップショット (apps/site-products): 専用read token・読み取り専用 (session なし)
app.use('/apps/site-products/api', siteProductsRouter);
// コーポレートサイト問い合わせ受付 (apps/site-contact): Bearer service token・冪等 (session なし)
app.use('/apps/site-contact/api', express.json({ limit: '64kb' }), siteContactRouter);
// 仕入れ先向け 売れ筋共有 (社内管理): 仕入先名登録・共有URL発行・プレビュー
app.use('/apps/supplier-sales', requireAppAccess('supplier-sales'), express.json({ limit: '256kb' }), supplierSalesRouter);
// 郵便料金判定 (postage): 専用DB postage.db (DATA_DIR)。
// カバー率は miniPC の warehouse.db (NE受注) か、Render では packing-dispatch の出力履歴 (warehouse-mirror.db) を読み取り専用で参照する。
// judge-api は伝票出しPCのランチャーが叩く (x-api-key = POSTAGE_JUDGE_KEY、未設定なら 503)。requireAppAccess より先に mount
// express.json はルータ側で持つ (取込だけ multipart のため)
app.use('/apps/postage/judge-api', postageJudgeRouter);
app.use('/apps/postage', requireAppAccess('postage'), postageRouter);
app.use('/apps/product-hub/service-api', productHubServiceApiRouter); // トークン認証 (PH_SERVICE_TOKEN, fail-closed)
app.use('/apps/product-hub', requireAppAccess('product-hub'), productHubRouter);
// 商品リンク台帳: warehouse-mirror.db 同居 (product-hub の保存と同一トランザクションで写す)。編集は product-hub 権限。
// product-hub を使える人は権限付与なしでも閲覧できる (付け忘れで編集者が 403 にならないように — Codex PR1 R1 L12)
app.use('/apps/product-links', (req, res, next) => {
  const a = req.session?.authenticated ? req.session.allowedApps : null;
  if (Array.isArray(a) && a.includes('product-hub')) return next();
  return requireAppAccess('product-links')(req, res, next);
}, productLinksRouter);
// 仕入先発注補助: mirror PML(read-only) + po_* マスタ/発注履歴 (warehouse-mirror.db 同居)
app.use('/apps/purchase-orders', requireAppAccess('purchase-orders'), express.json({ limit: '1mb' }), purchaseOrdersRouter);
// 価格一括改定 (price-update): mirror(read-only) + pu_* 監査 (warehouse-mirror.db 同居)。
// M1 は読み取り専用 — モールへの書き込みは無い。express.json は router 側で CSRF ガードの後に付ける
// (Content-Type 検査より先に body を読ませない)
app.use('/apps/price-update', requireAppAccess('price-update'), priceUpdateRouter);
// Amazon 価格管理 (amazon-pricing): ap_* (warehouse-mirror.db 同居)。Amazon へ書き込まない (apps/amazon-pricing/README.md)。
app.use('/apps/amazon-pricing', requireAppAccess('amazon-pricing'), amazonPricingRouter);
// 問い合わせ管理 (inquiry-hub): 専用DB inquiry-hub.db (DATA_DIR)。
// AI連携API (ローカルClaude Codeランナー用) は X-AI-Key 認証・セッション外 (設計書§9.2 権限分離。
// 先に mount してポータルセッション認証を通さない。product-hub/service-api と同パターン)
app.use('/apps/inquiry-hub/ai-api', express.json({ limit: '1mb' }), inquiryHubAiApiRouter);
// limit 2mb = メールディーラーCSV取込 (テンプレート~150KB+JSONエスケープ膨張) を JSON body で受けるため
app.use('/apps/inquiry-hub', requireAppAccess('inquiry-hub'), express.json({ limit: '2mb' }), inquiryHubRouter);
app.use('/apps/shipping-work', requireAppAccess('shipping-work'), express.json({ limit: '256kb' }), shippingWorkRouter);
// picking: 引当RPA (伝票出しPC) 向け取込API は x-api-key 認証・セッション外なので本体より先に mount
app.use('/apps/picking/ingest-api', pickingIngestRouter);
// picking: 倉庫の共用端末 (登録端末Cookie) でも使うため requireAppAccess は付けず、
// router 内の pickingAccess (セッション or 登録端末) で制御する。管理系は router 内で admin 必須
app.use('/apps/picking', express.json({ limit: '256kb' }), pickingRouter);
// easy-ship: Chrome拡張向けAPI (x-api-key 認証・fail-closed) はセッション認証付き本体より先に mount
app.use('/apps/easy-ship/ext-api', easyShipExtRouter);
app.use('/apps/easy-ship', requireAppAccess('easy-ship'), express.json({ limit: '2mb' }), easyShipRouter);
// select-set: NE伝票画面のChrome拡張向けAPI も同じく本体より先に mount
app.use('/apps/select-set/ext-api', selectSetExtRouter);
// マスタ配信 (Render → miniPC)。x-sync-key 認証なのでセッション認証より先に mount
app.use('/apps/select-set/master-api', express.json({ limit: '64kb' }), selectSetMasterRouter);
app.use('/apps/select-set', requireAppAccess('select-set'), express.json({ limit: '512kb' }), selectSetRouter);
app.use('/apps/mgmt-accounting', (req, res, next) => {
  // 管理系API (x-sync-key 直呼び対象) はセッション認証の代わりに parser より前で key 認証。
  // 監査 2026-07-06 I-43: 従来は 50MB parser が認証より前 + router 内 checkAuth が
  // MIRROR_SYNC_KEY 未設定で素通り (fail-open) だった。router 内 checkAuth は二重防御として残る。
  // /auto-sync-sales, /admin/* は router 内コメントで「MIRROR_SYNC_KEY 認証」と明記されながら
  // session バイパスが無く key 単体で到達不能だったため対象に追加 (Codex R1 medium)。
  const adminPaths = [
    '/import-historical', '/bulk-calculate', '/cleanup-invalid',
    '/auto-sync-sales', '/admin/load-historical-seed', '/admin/purge-months-before',
  ];
  if (req.method === 'POST' && adminPaths.includes(req.path)) {
    if (req.session?.authenticated) return next();
    const key = process.env.MIRROR_SYNC_KEY;
    if (!key) return res.status(503).json({ error: 'mirror_sync_key_unset' });
    if (req.headers['x-sync-key'] !== key) return res.status(401).json({ error: 'Invalid sync key' });
    return next();
  }
  requireAuth(req, res, next);
}, express.json({ limit: '50mb' }), mgmtAccountingRouter);
app.use('/apps/mercari-accounting', (req, res, next) => {
  if (req.path === '/import-history' && req.method === 'POST') return next();
  requireAuth(req, res, next);
}, mercariAccountingRouter);
// daily-sync (miniPC) からの cron 呼び出し用 API。x-sync-key 認証で守る (セッション認証ではない)
app.use('/apps/inventory-monthly/api', requireSyncKeyStrict, express.json({ limit: '64kb' }), inventoryMonthlyApiRouter);
// 既存 UI ルート (セッション認証)
app.use('/apps/inventory-monthly', requireAppAccess('inventory-monthly'), inventoryMonthlyRouter);

// 未実装アプリのプレースホルダー
app.get('/apps/:appId', requireAuth, (req, res) => {
  const appInfo = apps.find(a => a.id === req.params.appId);
  if (!appInfo) return res.status(404).send('Not found');
  res.render('coming-soon', { app: appInfo });
});

// --- 管理者ルート: 権限管理 ---
app.get('/admin/permissions', requireAdmin, (req, res) => {
  const nonAdminUsers = users.filter(u => u.role !== 'admin');
  res.render('admin-permissions', {
    users: nonAdminUsers, apps: variantVisibleApps(),
    username: req.session.email, displayName: req.session.displayName,
    success: req.query.success === '1',
  });
});

app.post('/admin/permissions', requireAdmin, (req, res) => {
  const perms = req.body.permissions || {};
  users.forEach(user => {
    if (user.role !== 'admin') {
      user.allowedApps = mergeAllowedApps(user.allowedApps, perms[user.email]);
    }
  });
  saveUsers(users);
  res.redirect('/admin/permissions?success=1');
});

// --- 管理者ルート: ユーザー管理 ---
app.get('/admin/users', requireAdmin, (req, res) => {
  res.render('admin-users', {
    users, apps: variantVisibleApps(),
    username: req.session.email, displayName: req.session.displayName,
    success: req.query.success, error: req.query.error,
  });
});

app.post('/admin/users/add', requireAdmin, (req, res) => {
  const { email, displayName, password, role, allowedApps } = req.body;

  if (!email || !displayName || !password) {
    return res.redirect('/admin/users?error=' + encodeURIComponent('全項目を入力してください'));
  }
  if (users.find(u => u.email.toLowerCase() === email.toLowerCase())) {
    return res.redirect('/admin/users?error=' + encodeURIComponent('このメールアドレスは既に登録されています'));
  }
  if (password.length < 6) {
    return res.redirect('/admin/users?error=' + encodeURIComponent('パスワードは6文字以上で設定してください'));
  }

  const parsedRole = role || 'user';
  const parsedApps = parsedRole === 'admin'
    ? '*'
    : mergeAllowedApps([], allowedApps);

  users.push({
    email: email.toLowerCase(),
    passwordHash: bcrypt.hashSync(password, 10),
    displayName,
    role: parsedRole,
    allowedApps: parsedApps,
  });
  saveUsers(users);
  res.redirect('/admin/users?success=' + encodeURIComponent(`${displayName} を追加しました`));
});

app.post('/admin/users/delete', requireAdmin, (req, res) => {
  const { email } = req.body;
  if (email === req.session.email) {
    return res.redirect('/admin/users?error=' + encodeURIComponent('自分自身は削除できません'));
  }
  const idx = users.findIndex(u => u.email === email);
  if (idx === -1) {
    return res.redirect('/admin/users?error=' + encodeURIComponent('ユーザーが見つかりません'));
  }
  const removed = users.splice(idx, 1)[0];
  saveUsers(users);
  res.redirect('/admin/users?success=' + encodeURIComponent(`${removed.displayName} を削除しました`));
});

// ユーザー別の権限更新（Ajax）
app.post('/admin/users/permissions', requireAdmin, express.json(), (req, res) => {
  const { email, allowedApps } = req.body;
  const user = users.find(u => u.email === email);
  if (!user) return res.status(404).json({ error: 'ユーザーが見つかりません' });
  if (user.role === 'admin') return res.status(400).json({ error: '管理者の権限は変更できません' });
  user.allowedApps = mergeAllowedApps(user.allowedApps, allowedApps);
  saveUsers(users);
  res.json({ ok: true });
});

app.post('/admin/users/reset-password', requireAdmin, (req, res) => {
  const { email, newPassword } = req.body;
  const user = users.find(u => u.email === email);
  if (!user) {
    return res.redirect('/admin/users?error=' + encodeURIComponent('ユーザーが見つかりません'));
  }
  if (!newPassword || newPassword.length < 6) {
    return res.redirect('/admin/users?error=' + encodeURIComponent('パスワードは6文字以上で設定してください'));
  }
  user.passwordHash = bcrypt.hashSync(newPassword, 10);
  saveUsers(users);
  res.redirect('/admin/users?success=' + encodeURIComponent(`${user.displayName} のパスワードをリセットしました`));
});

// --- 起動 ---
bootNote('web', `server.js ロード完了 (Node ${process.version}, PORT=${PORT}, RENDER=${!!process.env.RENDER})`);
bootStart('web', 'express-listen');
app.listen(PORT, () => {
  bootEnd('web', 'express-listen', `port=${PORT}`);
  console.log(`B-Faith Portal running at http://localhost:${PORT}`);

  // [perf] イベントループ遅延 / メモリ推移の定期計測（PERF_LOG=1 のときのみ）
  try { startPerfMonitor(); } catch (e) { console.warn('[perf] monitor 起動スキップ:', e.message); }

  // AES Pythonバックエンドは既定で Render (本番ポータル) のみ起動する。
  // 同じ server.js は miniPC でも warehouse/API host (port 3000) として動いているが、
  // miniPC には実体のPythonが無く (Store エイリアスのみ・SYSTEM実行でPATH外)、
  // spawn('python') が ENOENT (-4058) で失敗 → #605 自動再起動ループ + #607 GChat通知が
  // 誤発報していた (2026-07-24)。
  // AES_PYTHON_ENABLED を明示した場合はそれを優先 (Render側の緊急停止=0 も可能)、
  // 未指定なら RENDER の有無を既定値にする。
  const aesPythonEnabled = process.env.AES_PYTHON_ENABLED !== undefined
    ? process.env.AES_PYTHON_ENABLED === '1'
    : !!process.env.RENDER;
  if (aesPythonEnabled) {
    try {
      startPythonBackend();
    } catch (e) {
      bootFail('aes-python', 'startPythonBackend', e);
      console.warn(`[AES-Python] 起動スキップ: ${e.message}`);
      console.warn('[AES-Python] Python環境がない場合、AESラベル並び替え機能は使用できません');
    }
  } else {
    bootNote('aes-python', 'RENDER未設定のため起動スキップ (AES_PYTHON_ENABLED=1 で明示起動可)');
    console.log('[AES-Python] 非Render環境のため起動スキップ (AESラベル並び替えはRender側で提供)');
  }

  // 楽天順位チェッカー スケジューラー
  startScheduler();

  // 売上分類別粗利集計 売上自動同期スケジューラー（Render完結。Render 環境でのみ起動）
  if (process.env.RENDER) {
    try { startMgmtAutoSyncScheduler(); }
    catch (e) { console.warn('[mgmt-auto-sync] scheduler 起動スキップ:', e.message); }
  }

  // ミニPC warehouse死活監視
  startWarehouseHealthcheck();

  // event loop lag + heap/rss 観測
  startMetrics();

  // DATA_DIR (Persistent Disk) 使用率観測 — 2026-07-12 disk full 障害の再発防止
  startDiskWatch(DATA_DIR);

  // (2026-09-07) 旧・価格改定ワーカー (price-scheduler.js) は削除した。Amazon の価格管理は
  // apps/amazon-pricing (方針の記録 + 判定のシャドー運用のみ。Amazon へ書き込まない) に作り直し。

  // 経営インサイトGChat通知 (在庫サマリ、INVENTORY_NOTIFY_ENABLED=true で起動)
  startInventoryNotificationJob();
  // 低粗利商品アラート (粗利率10%割れ、MARGIN_ALERT_ENABLED=true で起動、JST 10:00)
  startMarginAlertJob();
  // biz-ops-overview 売上サマリ GChat 通知 (在庫と独立メッセージ、SALES_NOTIFY_ENABLED=true で起動)
  startSalesNotificationJob();
  // ai-insights 月次締めリマインダー+確定後変更検知 (AI_INSIGHTS_NOTIFY_ENABLED=true で起動、JST 10:00)
  startAiInsightsNotifyJob();

  // RYS 楽天↔Yahoo 差分検出 daily sync (RYS_FULL_SYNC_CRON_ENABLED=true で起動、 Dark Launch)
  startRysCron();

  // 入庫情報管理: NE商品マスタ(ミラー)から新商品を自動追加 + 入荷予定 nefuda.csv 取得。
  // 既定で有効 (JST 09:00 = ミラー同期完了後)。止める場合のみ INBOUND_INFO_SYNC_ENABLED=false
  startInboundInfoCron();
  startInboundCheckCron();
  // (2026-09-05 廃止 → 2026-09-09 コード削除) 在庫化カードの Notion 送信 (17:30 cron・台帳
  // inbound-check-notion-cards)。いろは行きの作業指示は「確認」と同じトランザクションで
  // 在庫化アプリ (f_iroha_tasks) の未着手に入る
  // 🏷 値札印刷キューの見張り (30秒間隔。滞留→manual / 報告なし→unknown / 倉庫PCエージェントの生存を台帳 nefuda-print-agent へ中継)
  startInboundCheckPrintQueueWorker();
  // いろは作業アプリ: 完成写真・動画の Drive/Notion 送信キュー (プロセス内2分間隔の再試行。
  // picking の画像キューと同じ扱いで、台帳対象の独立 cron ではない)
  startIrohaMediaWorker();
  // 🏷 いろは作業アプリ: 保管箱ラベル印刷キューの見張り (30秒間隔。滞留→manual / 報告なし→unknown / いろはPC エージェントの生存を台帳 iroha-label-print-agent へ中継)
  startIrohaPrintQueueWorker();
  // 📨 FBA箱詰め: 完了通知 (本社の Google Chat) の送信待ちを送る。起動直後に 1 回 + 再試行待ちがあるときだけその時刻に
  // (完了と同じトランザクションで積んだ outbox。いろはの写真キューと同じ扱いで、台帳対象の独立 cron ではない)
  startFbaBoxNotifyOutbox();
  startProductHubIntakeCron();
  // 商品リンク台帳: 夜間照合 (09:45 JST) + 台帳が空なら起動時バックフィル。既定 ON (PRODUCT_LINKS_RECONCILE_ENABLED=false で停止)
  startProductLinksCron();
  // 証憑受け箱の突合+添付 (毎時・台帳 shohyo-voucher-attach)。jobs-monitor と同じ Render 専用ガード内
  startShohyoAttachCron();

  // inquiry-hub 受信同期 (楽天15分+deep日次。INQUIRY_HUB_SYNC_CRON_ENABLED=true で起動、Dark Launch)
  startInquiryHubSyncCron();

  // inquiry-hub 送信ワーカー (outbox 30秒。INQUIRY_HUB_OUTBOX_CRON_ENABLED=true で起動、
  // メール実送信はさらに INQUIRY_HUB_MAIL_SEND_MODE=live が必要。既定=dryrun)
  startInquiryHubOutboxCron();

  // inquiry-hub ⏰締め前通知 (ロジザードの締め 09:00/12:30/14:30 の15分前にGChat。0件でも送る=dead-man)。
  // INQUIRY_HUB_CUTOFF_CRON_ENABLED=true で起動、Dark Launch
  startInquiryHubCutoffCron();

  // Render 一次データ自己バックアップ (JST 03:30、Google Drive へ外向き送信のみ =
  // DB ダウンロード用の公開エンドポイントは作らない。RENDER_BACKUP_CRON_ENABLED=1 で起動、Dark Launch)
  startRenderBackupCron();

  // Company DB を毎晩そっくり合わせ直す (JST 02:00 = バックアップ 03:30 の前。
  // COMPANY_DB_LOAD_CRON_ENABLED=1 で起動、Dark Launch)
  startCompanyDbNightlyLoadCron();
});

process.on('SIGTERM', () => {
  bootNote('web', 'SIGTERM受信 → shutdown');
  stopPythonBackend();
  process.exit(0);
});
process.on('SIGINT', () => {
  bootNote('web', 'SIGINT受信 → shutdown');
  stopPythonBackend();
  process.exit(0);
});
process.on('exit', (code) => {
  bootNote('web', `process.exit code=${code}`);
});
process.on('uncaughtException', (err) => {
  bootFail('web', 'uncaughtException', err);
});
process.on('unhandledRejection', (reason) => {
  bootFail('web', 'unhandledRejection', reason);
});
