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

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
const SQLiteStore = connectSqlite3(session);

// --- 設定 ---
const PORT = process.env.PORT || 3000;
const SESSION_SECRET = process.env.SESSION_SECRET || 'dev-secret-change-me';
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const USERS_FILE = path.join(DATA_DIR, 'users.json');

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
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));
app.use(express.urlencoded({ extended: true }));
app.use(express.json({ limit: '10mb' }));
app.use(express.static(path.join(__dirname, 'public')));

app.use(session({
  store: new SQLiteStore({ db: 'sessions.db', dir: DATA_DIR }),
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
app.listen(PORT, () => {
  console.log(`B-Faith Portal running at http://localhost:${PORT}`);

  try {
    startPythonBackend();
  } catch (e) {
    console.warn(`[AES-Python] 起動スキップ: ${e.message}`);
    console.warn('[AES-Python] Python環境がない場合、AESラベル並び替え機能は使用できません');
  }

  // 楽天順位チェッカー スケジューラー
  startScheduler();
});

process.on('SIGTERM', () => { stopPythonBackend(); process.exit(0); });
process.on('SIGINT', () => { stopPythonBackend(); process.exit(0); });
