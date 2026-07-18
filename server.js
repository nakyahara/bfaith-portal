import express from 'express';
import session from 'express-session';
import connectSqlite3 from 'connect-sqlite3';
import BetterSqlite3 from 'better-sqlite3';
import bcrypt from 'bcryptjs';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';
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
import { startPriceWorker, startMaintenanceJobs } from './apps/profit-calculator/price-scheduler.js';
import { startNotificationJob as startInventoryNotificationJob } from './apps/profit-analysis/notify-job.js';
import { startSalesNotificationJob } from './apps/biz-ops-overview/notify-job.js';
import { startRysCron } from './apps/rakuten-yahoo-sync/services/rys-cron.js';
import { startInquiryHubSyncCron } from './apps/inquiry-hub/sync/cron.js';
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
import salesAnalyticsLinegiftRouter from './apps/sales-analytics-linegift/router.js';
import packingDispatchRouter, { neSyncWorkerRouter as packingDispatchNeSyncWorkerRouter } from './apps/packing-dispatch/router.js';
import inventoryMonthlyRouter, { apiRouter as inventoryMonthlyApiRouter } from './apps/inventory-monthly/router.js';
import misShipmentRouter from './apps/mis-shipment/router.js';
import shippingLogRouter from './apps/shipping-log/router.js';
import supplierSalesRouter from './apps/supplier-sales/router.js';
import productHubRouter, { serviceApiRouter as productHubServiceApiRouter } from './apps/product-hub/router.js';
import purchaseOrdersRouter from './apps/purchase-orders/router.js';
import inquiryHubRouter from './apps/inquiry-hub/router.js';
import supplierSalesPublicRouter from './apps/supplier-sales/public-router.js';
import serviceRouter from './apps/warehouse/service-router.js';
import { serviceAuth } from './apps/warehouse/service-auth.js';
import { neSyncControlRouter } from './apps/warehouse/ne-sync-control-router.js';
import { isWarehouseDbReady } from './apps/warehouse/router.js';

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
    // mirror read API (GET専用、監査S-2で認証追加) への POST も認証前 body parse を避ける
    // (POST は router 側に route が無く 404 になるだけなので parse 不要)。
    if (/^\/apps\/mirror\/api\/(products|sales|status|download)(\/|$)/.test(normalizedPath)) return next();
    // 会計系 /import-history (監査S-4でアプリ別envトークン化) は requireImportKey 認証後に
    // 専用 parser (importJsonParser) が走るため、認証前 body parse を避ける。
    if (/^\/apps\/[a-z0-9-]+-accounting\/import-history$/.test(normalizedPath)) return next();
    // mgmt-accounting は mount 側で「認証ゲート → 50MB parser」の順に処理する (Excel seed 等の
    // 大容量投入があるため global 10MB を通すと mount 側 50MB が無効化される問題も同時に解消)。
    if (normalizedPath.startsWith('/apps/mgmt-accounting')) return next();
    if (LARGE_BODY_ROUTES.includes(normalizedPath)) return next();
  }
  return globalJsonParser(req, res, next);
});
app.use(express.static(path.join(__dirname, 'public')));

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

// --- カテゴリ定義 ---
const categories = [
  { id: 'product-sync', name: '商品登録・同期', icon: '🔄' },
  { id: 'shipping', name: '出荷・伝票', icon: '🚚' },
  { id: 'support', name: '問い合わせ対応', icon: '💬' },
  { id: 'analysis', name: '商品分析', icon: '📊' },
  { id: 'purchasing', name: '仕入れ', icon: '💰' },
  { id: 'fba', name: 'FBA管理', icon: '📦' },
  { id: 'data', name: 'データ基盤', icon: '🗄️' },
  { id: 'accounting', name: '売上・会計', icon: '📒' },
];

// --- アプリ一覧 ---
const apps = [
  {
    id: 'product-hub',
    name: '商品登録ハブ',
    description: '新商品ドラフト一元管理 (Notionカード自動作成 + AI生成 + 楽天出品へ)',
    icon: '📦',
    path: '/apps/product-hub',
    status: 'active',
    category: 'product-sync',
  },
  {
    id: 'linegift-sync',
    name: '楽天→LINEギフト同期',
    description: '楽天商品をLINEギフト形式に変換・同期',
    icon: '🎁',
    path: '/apps/linegift-sync',
    status: 'active',
    category: 'product-sync',
  },
  {
    id: 'mercari-sync',
    name: '楽天→メルカリShops同期',
    description: '楽天商品をメルカリShops形式に変換・CSV出力',
    icon: '🛒',
    path: '/apps/mercari-sync',
    status: 'active',
    category: 'product-sync',
  },
  {
    id: 'rakuten-yahoo-sync',
    name: '楽天→Yahoo!ショッピング 商品移行',
    description: '楽天商品をYahoo!ショッピングへ移行 (Notion 補完 + readiness ゲート)',
    icon: '🟣',
    path: '/apps/rakuten-yahoo-sync',
    status: 'active',
    category: 'product-sync',
  },
  {
    id: 'aes-pdf-sorter',
    name: 'AESラベル並び替え',
    description: 'AES配送ラベル・納品書の自動照合・並び替え',
    icon: '📄',
    path: '/apps/aes-pdf-sorter',
    status: 'active',
    category: 'shipping',
  },
  {
    id: 'ranking-checker',
    name: '楽天検索順位チェッカー',
    description: 'キーワード別の楽天検索順位確認',
    icon: '📊',
    path: '/apps/ranking-checker',
    status: 'active',
    category: 'analysis',
  },
  {
    id: 'profit-calculator',
    name: 'リサーチ仕入れツール',
    description: 'リサーチ・見積もり・利益計算・商品登録の一元管理',
    icon: '💰',
    path: '/apps/profit-calculator/',
    status: 'active',
    category: 'purchasing',
  },
  {
    id: 'fba-replenishment',
    name: 'FBA在庫補充',
    description: 'FBA納品の推奨数量計算・納品プラン作成',
    icon: '📦',
    path: '/apps/fba-replenishment',
    status: 'active',
    category: 'fba',
  },
  {
    id: 'warehouse',
    name: 'データウェアハウス',
    description: '社内マスターデータ基盤・販売データ蓄積・AI分析用',
    icon: '🗄️',
    path: '/apps/warehouse',
    status: 'active',
    category: 'data',
  },
  {
    id: 'amazon-accounting',
    name: 'Amazon売上集計',
    description: 'ペイメントレポートCSVから税率別・セグメント別の売上集計を自動計算',
    icon: '📒',
    path: '/apps/amazon-accounting',
    status: 'active',
    category: 'accounting',
  },
  {
    id: 'amazon-usa-accounting',
    name: '米国Amazon売上集計',
    description: 'Monthly Unified Transaction CSVから輸出売上をUSD→JPY換算して集計(全売上=セグメント4)',
    icon: '🇺🇸',
    path: '/apps/amazon-usa-accounting',
    status: 'active',
    category: 'accounting',
  },
  {
    id: 'rakuten-accounting',
    name: '楽天売上集計',
    description: '楽天RMS注文データCSVから税率別・セグメント別の売上集計を自動計算',
    icon: '📕',
    path: '/apps/rakuten-accounting',
    status: 'active',
    category: 'accounting',
  },
  {
    id: 'aupay-accounting',
    name: 'auペイマーケット売上集計',
    description: 'auペイマーケット会計用注文データCSVから税率別・セグメント別の売上集計を自動計算',
    icon: '📙',
    path: '/apps/aupay-accounting',
    status: 'active',
    category: 'accounting',
  },
  {
    id: 'yahoo-accounting',
    name: 'Yahoo!売上集計',
    description: 'Yahoo注文データCSVから税率別・セグメント別の売上集計を自動計算',
    icon: '📗',
    path: '/apps/yahoo-accounting',
    status: 'active',
    category: 'accounting',
  },
  {
    id: 'qoo10-accounting',
    name: 'Qoo10売上集計',
    description: 'Qoo10セリングレポートCSVから税率別・セグメント別の売上集計を自動計算',
    icon: '📔',
    path: '/apps/qoo10-accounting',
    status: 'active',
    category: 'accounting',
  },
  {
    id: 'linegift-accounting',
    name: 'LINEギフト売上集計',
    description: 'LINEギフト注文CSVから税率別・セグメント別の売上集計を自動計算',
    icon: '💚',
    path: '/apps/linegift-accounting',
    status: 'active',
    category: 'accounting',
  },
  {
    id: 'fba-profitability',
    name: 'FBA収益性分析',
    description: 'FBA全商品の利益率を分析・低利益率商品を検出',
    icon: '📉',
    path: '/apps/fba-profitability',
    status: 'active',
    category: 'fba',
  },
  {
    id: 'mercari-accounting',
    name: 'メルカリShops売上集計',
    description: 'メルカリShops売上レポート+注文CSVから税率別・セグメント別の売上集計を自動計算',
    icon: '🛒',
    path: '/apps/mercari-accounting',
    status: 'active',
    category: 'accounting',
  },
  {
    id: 'profit-analysis',
    name: '商品収益性ダッシュボード',
    description: '商品別粗利分析 + 在庫整理・撤退判断支援',
    icon: '💹',
    path: '/apps/profit-analysis',
    status: 'active',
    category: 'analysis',
  },
  {
    id: 'amazon-dashboard',
    name: 'Amazon統合ダッシュボード',
    description: 'Amazonの売上・広告・利益を統合管理 (タイル速報 + 利益ウォーターフォール + 広告×利益 + 売れ筋 + 診断)',
    icon: '🛒',
    path: '/apps/amazon-dashboard',
    status: 'active',
    category: 'analysis',
  },
  {
    id: 'rakuten-analytics',
    name: '楽天分析ツール',
    description: '楽天の売上・広告・利益・検索順位を統合管理 (タイル速報 + 実質利益 + RPP×損益分岐ROAS + 順位重ね合わせ)',
    icon: '📊',
    path: '/apps/rakuten-analytics',
    status: 'active',
    category: 'analysis',
  },
  {
    id: 'yahoo-analytics',
    name: 'ヤフー分析ツール',
    description: 'Yahoo!ショッピングの売上・広告・利益・検索順位を統合管理 (タイル速報 + 実質利益 + 広告×利益 + キャンペーン損益)',
    icon: '🛍️',
    path: '/apps/yahoo-analytics',
    status: 'active',
    category: 'analysis',
  },
  {
    id: 'aupay-analytics',
    name: 'auPAY分析ツール',
    description: 'au PAYマーケットの売上・利益・キャンペーンを統合管理 (タイル速報 + ポイント後利益L2 + 三太郎の日ハイライト + 送料負け診断)',
    icon: '🧡',
    path: '/apps/aupay-analytics',
    status: 'active',
    category: 'analysis',
  },
  {
    id: 'qoo10-analytics',
    name: 'Qoo10分析ツール',
    description: 'Qoo10の売上・広告・利益・メガ割損益を統合管理 (タイル速報 + 手数料実額 + メガ割セラー負担 + 開催回損益)',
    icon: '🛒',
    path: '/apps/qoo10-analytics',
    status: 'active',
    category: 'analysis',
  },
  {
    id: 'biz-ops-overview',
    name: '業務オペ概要',
    description: '全モール売上 (前日/今月/30日) + 出荷率等の日次経営指標集約',
    icon: '📊',
    path: '/apps/biz-ops-overview',
    status: 'active',
    category: 'analysis',
  },
  {
    id: 'product-management-list',
    name: '商品管理リスト',
    description: 'NE全商品コード軸の在庫×販売(7d/30d FBA別)×利益×発注 統合表 (毎朝更新・CSV出力)',
    icon: '📋',
    path: '/apps/product-management-list',
    status: 'active',
    category: 'analysis',
  },
  {
    id: 'cross-sell-finder',
    name: '同梱商品検索',
    description: 'NE商品コードで過去90日に一緒に買われた商品を全モール横断で表示',
    icon: '🛍️',
    path: '/apps/cross-sell-finder',
    status: 'active',
    category: 'analysis',
  },
  {
    id: 'giftset-assembly',
    name: 'ギフトセット組み依頼',
    description: '構成品のピッキング表(ロジザード貼り付け)と子会社Notionの作業カードを発行',
    icon: '🎁',
    path: '/apps/giftset-assembly',
    status: 'active',
    category: 'shipping',
  },
  {
    id: 'sales-analytics-linegift',
    name: 'LINEギフト 売上分析ダッシュボード',
    description: 'KPI / 月次トレンド / 商品ランキング / 価格帯別 / シーン(母の日等)期間絞り込み。既存 mirror_linegift_finance_sku_daily を Render で集計、商品分類は v2.0+',
    icon: '💚',
    path: '/apps/sales-analytics-linegift',
    status: 'active',
    category: 'analysis',
  },
  {
    id: 'packing-dispatch',
    name: '梱包機振り分け・配送方法',
    description: 'NE受注CSVを取り込み、配送方法と梱包機(pasline/meltline)を判定・編集して再出力。Tapes代替',
    icon: '📦',
    path: '/apps/packing-dispatch',
    status: 'active',
    category: 'shipping',
  },
  {
    id: 'mgmt-accounting',
    name: '売上分類別粗利集計',
    description: '各モール売上データ+運賃・資材費から売上分類別の変動費・粗利益を管理会計用に集計',
    icon: '📊',
    path: '/apps/mgmt-accounting',
    status: 'active',
    category: 'accounting',
  },
  {
    id: 'inventory-monthly',
    name: '月末棚卸しツール',
    description: 'FBA在庫(発注推奨レポート)・自社倉庫CSV・発注後未着商品を集計し、税抜原価ベースの月末棚卸資産を算出・履歴保存',
    icon: '📦',
    path: '/apps/inventory-monthly',
    status: 'active',
    category: 'accounting',
  },
  {
    id: 'exec-dashboard',
    name: 'MF経営トップダッシュボード',
    description: 'MFクラウド会計データから売上・粗利・現金残高・モール別売上を1画面集約 (Phase 1a)',
    icon: '📊',
    path: '/apps/exec-dashboard',
    status: 'active',
    category: 'analysis',
  },
  {
    id: 'mis-shipment',
    name: '誤出荷管理',
    description: '誤出荷の記録・分析、モール別誤出荷率と工程別/原因別の可視化 (Phase 1)',
    icon: '⚠️',
    path: '/apps/mis-shipment',
    status: 'active',
    category: 'shipping',
  },
  {
    id: 'purchase-orders',
    name: '発注補助',
    description: '仕入先への発注を1画面で完結。要発注判定・推奨発注量・最低発注条件ゲージ・ついで買い候補・発注履歴 (旧: 発注対象商品シート+発注条件マスタ)',
    icon: '🛒',
    path: '/apps/purchase-orders',
    status: 'active',
    category: 'purchasing',
  },
  {
    id: 'inquiry-hub',
    name: '問い合わせ管理',
    description: 'メール+楽天R-Messe+Yahoo!問い合わせの一元管理 (メールディーラー置き換え)。Step 1: 一覧/詳細/担当/メモ/検索 (read-only運用)',
    icon: '💬',
    path: '/apps/inquiry-hub',
    status: 'active',
    category: 'support',
  },
  {
    id: 'supplier-sales',
    name: '仕入れ先 売れ筋共有',
    description: '全モール(Amazon・楽天 ほか)の販売実績を仕入先別に集計し、ログイン不要の共有URLを発行。原価非開示・販売数/売上のみ',
    icon: '🏭',
    path: '/apps/supplier-sales',
    status: 'active',
    category: 'analysis',
  },
];

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

/**
 * warehouse variant 専用 dashboard 表示エントリ。
 *
 * 設計判断 (Codex round 1 critical 反映):
 *   apps 配列に新規 id を追加すると、認可 (requireAppAccess) の app id とズレが生じる。
 *   既存ルート保護は /apps/warehouse 配下が requireAppAccess('warehouse') に依存しており、
 *   dashboard 表示用の id を分けたいだけなのに認可まで再設計する必要が出てしまう。
 *   そこで dashboard 表示用エントリは別配列で持ち、requiresAccess で「どの allowedApps id を
 *   持つユーザーに見せるか」を明示する。これで:
 *     - allowedApps に 'warehouse' を持つユーザー → 「マスタ登録」が見える + クリックして 403 にならない
 *     - admin 画面 (apps 配列ベース) は無変更、warehouse へのアクセス権付与は従来通り
 *     - 認可は variant に依存しない (本来 PR の要件外、dashboard 表示の問題のみ解決)
 *
 *   「ユーザー管理」リンクは既に dashboard.ejs ヘッダーで admin 限定に表示される実装済みのため、
 *   ここには含めない。
 */
const WAREHOUSE_VARIANT_DASHBOARD_APPS = [
  {
    id: 'warehouse-register-display', // dashboard 表示用 (key として使われる程度)
    requiresAccess: 'warehouse',      // 必要な allowedApps の app id (= 既存の warehouse)
    name: 'マスタ登録',
    description: 'SKU・送料・原価・売上分類・税率の登録/編集',
    icon: '📋',
    path: '/apps/warehouse/register',
    status: 'active',
    category: 'data',
  },
];

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

// 外部リンク
const externalLinks = [
  {
    name: '発注条件参照ツール',
    description: '商品コード/名前で発注条件・在庫を検索',
    icon: '📦',
    url: 'https://script.google.com/a/macros/b-faith.biz/s/AKfycbxxn6HcHZKgAKAww1k-AFMER6SVt_-PRTQp1EJoPEclBczvUKEw1VBOWPhAo0O9Z1VO1Q/exec',
  },
  {
    name: 'ピッキングKPIダッシュボード',
    description: 'ピッキング作業のKPI・パフォーマンス分析',
    icon: '📈',
    url: 'https://script.google.com/a/macros/b-faith.biz/s/AKfycbxKrVwCJWtZOr1lS_-rzEvamatfpZ3UV2NCDwnwlO083Vhx3Gn2T2N6H5GfPPuZCIkUhw/exec',
  },
];

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

// ダッシュボード
app.get('/', requireAuth, (req, res) => {
  const allowed = req.session.allowedApps;
  if (!allowed) {
    return req.session.destroy(() => res.redirect('/login'));
  }
  if (PORTAL_VARIANT === 'warehouse') {
    // warehouse variant: 「マスタ登録」のみ表示。requiresAccess='warehouse' で
    // ユーザーの allowedApps に warehouse 権限があるかを判定 (認可は既存ルートの
    // requireAppAccess('warehouse') に揃う、dashboard 表示と認可がズレない)
    const visibleApps = allowed === '*'
      ? WAREHOUSE_VARIANT_DASHBOARD_APPS
      : WAREHOUSE_VARIANT_DASHBOARD_APPS.filter(a => allowed.includes(a.requiresAccess));
    return res.render('dashboard', {
      apps: visibleApps, categories, externalLinks: [],
      username: req.session.email, displayName: req.session.displayName,
      role: req.session.role,
    });
  }
  // render variant (default): 既存挙動維持
  const visibleApps = allowed === '*' ? apps : apps.filter(a => allowed.includes(a.id));
  const visibleExtLinks = allowed === '*' ? externalLinks : [];
  res.render('dashboard', {
    apps: visibleApps, categories, externalLinks: visibleExtLinks,
    username: req.session.email, displayName: req.session.displayName,
    role: req.session.role,
  });
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
app.use('/apps/cross-sell-finder', requireAppAccess('cross-sell-finder'), crossSellFinderRouter);
app.use('/apps/giftset-assembly', requireAppAccess('giftset-assembly'), express.json({ limit: '256kb' }), giftsetAssemblyRouter);
app.use('/apps/sales-analytics-linegift', requireAppAccess('sales-analytics-linegift'), express.json({ limit: '256kb' }), salesAnalyticsLinegiftRouter);
// 構成 B (2026-06-05 中原さん確定): NE 反映 worker (miniPC) は session 認証なし、Bearer fail-closed のみ。
// packing-dispatch 本体 (requireAppAccess) より「前」に mount しないと、miniPC が 401/403 で弾かれる。
app.use('/apps/packing-dispatch/api/ne-sync-worker', express.json({ limit: '2mb' }), packingDispatchNeSyncWorkerRouter);
app.use('/apps/packing-dispatch', requireAppAccess('packing-dispatch'), express.json({ limit: '2mb' }), packingDispatchRouter);
// 誤出荷管理 (apps/mis-shipment): warehouse-mirror.db 同居の f_mis_shipments を CRUD、注文 lookup は miniPC GET 経由
app.use('/apps/mis-shipment', requireAppAccess('mis-shipment'), express.json({ limit: '256kb' }), misShipmentRouter);
// 出荷実績ログ (apps/shipping-log): 出荷_no 掃除 GAS からの伝票取込。Bearer fail-closed のみ (session なし)
app.use('/apps/shipping-log/api', express.json({ limit: '2mb' }), shippingLogRouter);
// 仕入れ先向け 売れ筋共有 (社内管理): 仕入先名登録・共有URL発行・プレビュー
app.use('/apps/supplier-sales', requireAppAccess('supplier-sales'), express.json({ limit: '256kb' }), supplierSalesRouter);
app.use('/apps/product-hub/service-api', productHubServiceApiRouter); // トークン認証 (PH_SERVICE_TOKEN, fail-closed)
app.use('/apps/product-hub', requireAppAccess('product-hub'), productHubRouter);
// 仕入先発注補助: mirror PML(read-only) + po_* マスタ/発注履歴 (warehouse-mirror.db 同居)
app.use('/apps/purchase-orders', requireAppAccess('purchase-orders'), express.json({ limit: '1mb' }), purchaseOrdersRouter);
// 問い合わせ管理 (inquiry-hub): 専用DB inquiry-hub.db (DATA_DIR)。
// limit 2mb = メールディーラーCSV取込 (テンプレート~150KB+JSONエスケープ膨張) を JSON body で受けるため
app.use('/apps/inquiry-hub', requireAppAccess('inquiry-hub'), express.json({ limit: '2mb' }), inquiryHubRouter);
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

  try {
    startPythonBackend();
  } catch (e) {
    bootFail('aes-python', 'startPythonBackend', e);
    console.warn(`[AES-Python] 起動スキップ: ${e.message}`);
    console.warn('[AES-Python] Python環境がない場合、AESラベル並び替え機能は使用できません');
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

  // 価格改定ワーカー — 安全装置未実装のため無効化 (2026-03-30)
  // startPriceWorker();

  // 価格改定メンテナンスジョブ — 同上理由で無効化 (2026-03-30)
  // startMaintenanceJobs();

  // 経営インサイトGChat通知 (在庫サマリ、INVENTORY_NOTIFY_ENABLED=true で起動)
  startInventoryNotificationJob();
  // biz-ops-overview 売上サマリ GChat 通知 (在庫と独立メッセージ、SALES_NOTIFY_ENABLED=true で起動)
  startSalesNotificationJob();

  // RYS 楽天↔Yahoo 差分検出 daily sync (RYS_FULL_SYNC_CRON_ENABLED=true で起動、 Dark Launch)
  startRysCron();

  // inquiry-hub 受信同期 (楽天15分+deep日次。INQUIRY_HUB_SYNC_CRON_ENABLED=true で起動、Dark Launch)
  startInquiryHubSyncCron();
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
