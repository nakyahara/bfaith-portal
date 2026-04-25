import express from 'express';
import session from 'express-session';
import connectSqlite3 from 'connect-sqlite3';
import bcrypt from 'bcryptjs';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';
import linegiftRouter from './apps/linegift-sync/router.js';
import mercariRouter from './apps/mercari-sync/router.js';
import aesRouter, { startPythonBackend, stopPythonBackend } from './apps/aes-pdf-sorter/router.js';
import rankingRouter from './apps/ranking-checker/router.js';
import { startScheduler } from './apps/ranking-checker/scheduler.js';
import { startWarehouseHealthcheck } from './apps/warehouse/healthcheck.js';
import { startMetrics } from './apps/observability/metrics.js';
import { bootStart, bootEnd, bootNote, bootFail, getBootId } from './apps/observability/boot-log.js';
import profitRouter from './apps/profit-calculator/router.js';
import { startPriceWorker, startMaintenanceJobs } from './apps/profit-calculator/price-scheduler.js';
import fbaRouter from './apps/fba-replenishment/router.js';
import warehouseRouter from './apps/warehouse/router.js';
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
import mgmtAccountingRouter from './apps/mgmt-accounting/router.js';
import serviceRouter from './apps/warehouse/service-router.js';
import { serviceAuth } from './apps/warehouse/service-auth.js';

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
    if (LARGE_BODY_ROUTES.includes(normalizedPath)) return next();
  }
  return globalJsonParser(req, res, next);
});
app.use(express.static(path.join(__dirname, 'public')));

app.use(session({
  store: new SQLiteStore({ db: 'sessions.db', dir: DATA_DIR }),
  secret: SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  cookie: {
    maxAge: 1 * 24 * 60 * 60 * 1000, // 1日間
    httpOnly: true,
    secure: true,
    sameSite: 'lax',
  }
}));

// --- 認証ミドルウェア ---
// /api/ パスへの未認証アクセスはHTMLリダイレクトではなくJSONで401/403を返す
// (fetch が追従したログインHTMLを res.json() でパースして壊れるのを防ぐ)
function isApiRequest(req) {
  return req.path.startsWith('/api/') || req.xhr || (req.get('accept') || '').includes('application/json');
}

function requireAuth(req, res, next) {
  if (req.session && req.session.authenticated) return next();
  if (isApiRequest(req)) return res.status(401).json({ error: 'session_expired' });
  res.redirect('/login');
}

// アプリ別アクセス制御ミドルウェア
function requireAppAccess(appId) {
  return (req, res, next) => {
    if (!req.session || !req.session.authenticated) {
      if (isApiRequest(req)) return res.status(401).json({ error: 'session_expired' });
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
  { id: 'analysis', name: '商品分析', icon: '📊' },
  { id: 'purchasing', name: '仕入れ', icon: '💰' },
  { id: 'fba', name: 'FBA管理', icon: '📦' },
  { id: 'data', name: 'データ基盤', icon: '🗄️' },
  { id: 'accounting', name: '売上・会計', icon: '📒' },
];

// --- アプリ一覧 ---
const apps = [
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
    id: 'mgmt-accounting',
    name: '売上分類別粗利集計',
    description: '各モール売上データ+運賃・資材費から売上分類別の変動費・粗利益を管理会計用に集計',
    icon: '📊',
    path: '/apps/mgmt-accounting',
    status: 'active',
    category: 'accounting',
  },
];

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
  if (req.session.authenticated) return res.redirect('/');
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
    return res.redirect('/');
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
app.use('/apps/aes-pdf-sorter', requireAppAccess('aes-pdf-sorter'), aesRouter);
app.use('/apps/ranking-checker', requireAppAccess('ranking-checker'), rankingRouter);
app.use('/apps/profit-calculator', requireAppAccess('profit-calculator'), profitRouter);
app.use('/apps/fba-replenishment', requireAppAccess('fba-replenishment'), fbaRouter);
app.use('/apps/warehouse', requireAppAccess('warehouse'), warehouseRouter);

// === Mirror subtree middleware (Codex 6周レビュー反映) ===
// accessLog は /apps/mirror 全体に掛ける (401含めて全requestを観測できる)。
// 認証+8MB parser+parser error handler は /apps/mirror/api/sync* のみ (mutation専用)。
// 既存の read API (/apps/mirror/api/products など) は従来通り認証なしで素通し。
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
  limit: '8mb',
  inflate: false,                                     // gzip は 415 で reject
  verify: (req, res, buf) => { req.rawBodyBytes = buf.length; },
}));
app.use('/apps/mirror/api/sync', mirrorParserErrorHandler);

// read API (/api/products 等) は従来通り認証なしで素通し。
// mirrorRouter 内部の `router.post('/api/sync', requireSyncKey, ...)` が二重防御として残る。
app.use('/apps/mirror', mirrorRouter);
// サービスAPI（Render→ミニPC、トークン認証）。
// rankcheck の履歴込みインポートで 10MB を超える可能性があるため 50MB まで許容。
// 未認可 DoS 回避のため、serviceAuth を body parser **より前** に置く。
// そうしないと token 無しリクエストが最大 50MB を parse してから 401 になる。
app.use('/service-api', serviceAuth, express.json({ limit: '50mb' }), serviceRouter);
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
app.use('/apps/yahoo-accounting', requireAuth, yahooAccountingRouter);
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
app.use('/apps/mgmt-accounting', express.json({ limit: '50mb' }), (req, res, next) => {
  // 管理系APIはセッション認証スキップ（内部で checkAuth により key/session のいずれか必須）
  const adminPaths = ['/import-historical', '/bulk-calculate', '/cleanup-invalid'];
  if (req.method === 'POST' && adminPaths.includes(req.path)) return next();
  requireAuth(req, res, next);
}, mgmtAccountingRouter);
app.use('/apps/mercari-accounting', (req, res, next) => {
  if (req.path === '/import-history' && req.method === 'POST') return next();
  requireAuth(req, res, next);
}, mercariAccountingRouter);

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
    users: nonAdminUsers, apps,
    username: req.session.email, displayName: req.session.displayName,
    success: req.query.success === '1',
  });
});

app.post('/admin/permissions', requireAdmin, (req, res) => {
  const perms = req.body.permissions || {};
  users.forEach(user => {
    if (user.role !== 'admin') {
      const val = perms[user.email];
      user.allowedApps = Array.isArray(val) ? val : (val ? [val] : []);
    }
  });
  saveUsers(users);
  res.redirect('/admin/permissions?success=1');
});

// --- 管理者ルート: ユーザー管理 ---
app.get('/admin/users', requireAdmin, (req, res) => {
  res.render('admin-users', {
    users, apps,
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
  const parsedApps = parsedRole === 'admin' ? '*'
    : Array.isArray(allowedApps) ? allowedApps
    : allowedApps ? [allowedApps]
    : [];

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
  user.allowedApps = Array.isArray(allowedApps) ? allowedApps : [];
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

  try {
    startPythonBackend();
  } catch (e) {
    bootFail('aes-python', 'startPythonBackend', e);
    console.warn(`[AES-Python] 起動スキップ: ${e.message}`);
    console.warn('[AES-Python] Python環境がない場合、AESラベル並び替え機能は使用できません');
  }

  // 楽天順位チェッカー スケジューラー
  startScheduler();

  // ミニPC warehouse死活監視
  startWarehouseHealthcheck();

  // event loop lag + heap/rss 観測
  startMetrics();

  // 価格改定ワーカー — 安全装置未実装のため無効化 (2026-03-30)
  // startPriceWorker();

  // 価格改定メンテナンスジョブ — 同上理由で無効化 (2026-03-30)
  // startMaintenanceJobs();
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
