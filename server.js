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

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
const SQLiteStore = connectSqlite3(session);

// --- 設定 ---
const PORT = process.env.PORT || 3000;
const SESSION_SECRET = process.env.SESSION_SECRET || 'dev-secret-change-me';

// --- ユーザー定義（ロールベースアクセス制御） ---
const users = [
  {
    username: 'admin',
    passwordHash: bcrypt.hashSync(process.env.PORTAL_PASS || 'changeme', 10),
    displayName: '管理者',
    role: 'admin',
    allowedApps: '*', // 全アプリ
  },
  {
    username: 'shipping',
    passwordHash: bcrypt.hashSync(process.env.SHIPPING_PASS || 'shipping123', 10),
    displayName: '出荷担当',
    role: 'shipping',
    allowedApps: ['aes-pdf-sorter'],
  },
  {
    username: 'product',
    passwordHash: bcrypt.hashSync(process.env.PRODUCT_PASS || 'product123', 10),
    displayName: '商品担当',
    role: 'product',
    allowedApps: ['linegift-sync', 'mercari-sync', 'ranking-checker'],
  },
];

// --- 権限永続化 ---
const PERMISSIONS_FILE = path.join(__dirname, 'data', 'permissions.json');

function loadPermissions() {
  try {
    if (fs.existsSync(PERMISSIONS_FILE)) {
      const saved = JSON.parse(fs.readFileSync(PERMISSIONS_FILE, 'utf-8'));
      users.forEach(user => {
        if (user.role !== 'admin' && saved[user.username]) {
          user.allowedApps = saved[user.username];
        }
      });
    }
  } catch (e) {
    console.warn('[Permissions] 読み込み失敗:', e.message);
  }
}

function savePermissions() {
  const data = {};
  users.forEach(user => {
    if (user.role !== 'admin') {
      data[user.username] = user.allowedApps;
    }
  });
  const dir = path.dirname(PERMISSIONS_FILE);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(PERMISSIONS_FILE, JSON.stringify(data, null, 2), 'utf-8');
}

loadPermissions();

// --- ミドルウェア ---
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));
app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

app.use(session({
  store: new SQLiteStore({ db: 'sessions.db', dir: __dirname }),
  secret: SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  cookie: {
    maxAge: 7 * 24 * 60 * 60 * 1000, // 7日間
    httpOnly: true,
  }
}));

// --- 認証ミドルウェア ---
function requireAuth(req, res, next) {
  if (req.session && req.session.authenticated) return next();
  res.redirect('/login');
}

// アプリ別アクセス制御ミドルウェア
function requireAppAccess(appId) {
  return (req, res, next) => {
    if (!req.session || !req.session.authenticated) return res.redirect('/login');
    const allowed = req.session.allowedApps;
    if (allowed === '*' || (Array.isArray(allowed) && allowed.includes(appId))) {
      return next();
    }
    res.status(403).render('forbidden', { username: req.session.username, displayName: req.session.displayName });
  };
}

// 管理者専用ミドルウェア
function requireAdmin(req, res, next) {
  if (!req.session || !req.session.authenticated) return res.redirect('/login');
  if (req.session.role !== 'admin') {
    return res.status(403).render('forbidden', { username: req.session.username, displayName: req.session.displayName });
  }
  next();
}

// --- カテゴリ定義 ---
const categories = [
  { id: 'product-sync', name: '商品登録・同期', icon: '🔄' },
  { id: 'shipping', name: '出荷・伝票', icon: '🚚' },
  { id: 'analysis', name: '商品分析', icon: '📊' },
];

// --- アプリ一覧（ここに追加していく） ---
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
    name: '楽天順位チェッカー',
    description: 'キーワード別の楽天・Yahoo・Amazon順位確認',
    icon: '📊',
    path: '/apps/ranking-checker',
    status: 'coming-soon',
    category: 'analysis',
  },
];

// 外部リンク
const externalLinks = [
  {
    name: '発注条件参照ツール',
    description: '商品コード/名前で発注条件・在庫を検索',
    icon: '📦',
    url: '#', // 後でURL設定
  },
  {
    name: 'ピッキングKPIダッシュボード',
    description: 'ピッキング作業のKPI・パフォーマンス分析',
    icon: '📈',
    url: '#',
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
  const { username, password } = req.body;
  const user = users.find(u => u.username === username);
  if (user && bcrypt.compareSync(password, user.passwordHash)) {
    req.session.authenticated = true;
    req.session.username = user.username;
    req.session.displayName = user.displayName;
    req.session.role = user.role;
    req.session.allowedApps = user.allowedApps;
    return res.redirect('/');
  }
  res.render('login', { error: 'ユーザー名またはパスワードが正しくありません' });
});

// ログアウト
app.get('/logout', (req, res) => {
  req.session.destroy(() => res.redirect('/login'));
});

// ダッシュボード（認証必須）
app.get('/', requireAuth, (req, res) => {
  const allowed = req.session.allowedApps;
  if (!allowed) {
    // 古いセッション（ロール情報なし）→ 再ログイン
    return req.session.destroy(() => res.redirect('/login'));
  }
  const visibleApps = allowed === '*' ? apps : apps.filter(a => allowed.includes(a.id));
  const visibleExtLinks = allowed === '*' ? externalLinks : [];
  res.render('dashboard', {
    apps: visibleApps, categories, externalLinks: visibleExtLinks,
    username: req.session.username, displayName: req.session.displayName,
    role: req.session.role,
  });
});

// アプリルート（認証 + アプリ別アクセス制御）
app.use('/apps/linegift-sync', requireAppAccess('linegift-sync'), linegiftRouter);
app.use('/apps/mercari-sync', requireAppAccess('mercari-sync'), mercariRouter);
app.use('/apps/aes-pdf-sorter', requireAppAccess('aes-pdf-sorter'), aesRouter);
// app.use('/apps/ranking-checker', requireAppAccess('ranking-checker'), rankingRouter);

// 未実装アプリのプレースホルダー
app.get('/apps/:appId', requireAuth, (req, res) => {
  const appInfo = apps.find(a => a.id === req.params.appId);
  if (!appInfo) return res.status(404).send('Not found');
  res.render('coming-soon', { app: appInfo });
});

// --- 管理者ルート ---
app.get('/admin/permissions', requireAdmin, (req, res) => {
  const nonAdminUsers = users.filter(u => u.role !== 'admin');
  res.render('admin-permissions', {
    users: nonAdminUsers, apps,
    username: req.session.username, displayName: req.session.displayName,
    success: req.query.success === '1',
  });
});

app.post('/admin/permissions', requireAdmin, (req, res) => {
  const perms = req.body.permissions || {};
  users.forEach(user => {
    if (user.role !== 'admin') {
      const val = perms[user.username];
      user.allowedApps = Array.isArray(val) ? val : (val ? [val] : []);
    }
  });
  savePermissions();
  res.redirect('/admin/permissions?success=1');
});

// --- 起動 ---
app.listen(PORT, () => {
  console.log(`B-Faith Portal running at http://localhost:${PORT}`);

  // AES PDF Sorter Pythonバックエンド起動
  try {
    startPythonBackend();
  } catch (e) {
    console.warn(`[AES-Python] 起動スキップ: ${e.message}`);
    console.warn('[AES-Python] Python環境がない場合、AESラベル並び替え機能は使用できません');
  }
});

// プロセス終了時にPythonバックエンドも停止
process.on('SIGTERM', () => { stopPythonBackend(); process.exit(0); });
process.on('SIGINT', () => { stopPythonBackend(); process.exit(0); });
