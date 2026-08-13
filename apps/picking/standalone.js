/**
 * picking スタンドアロンサーバ — ミニPC自前ホスト用エントリポイント
 *
 * 背景 (2026-08-12 中原さん決定):
 *   Render の bfaith-portal は永続ディスク付きのためデプロイのたびに数分停止し、
 *   モノレポ同居で無関係アプリのマージでも倉庫のピッキングが巻き添えで落ちる。
 *   picking だけをミニPC上の独立プロセスに分離し、デプロイ(pull+restart)のタイミングを
 *   自分で制御できるようにする。あわせて「戻る操作でポータルに出てログインを求められる」
 *   問題も解消する (このサーバにはポータル画面が存在せず / は一覧へリダイレクト)。
 *
 * 起動:  node apps/picking/standalone.js
 * env :  PICKING_PORT (default 3100) / DATA_DIR (専用ディレクトリ必須 — warehouse variant と共有しない)
 *        SESSION_SECRET / PORTAL_PASS (初回の管理者ブートストラップ)
 *        + picking 本体の env (PICKING_INGEST_KEY / PICKING_NOTION_TOKEN / PICKING_ALERT_WEBHOOK /
 *          GOOGLE_SERVICE_ACCOUNT_KEY / WAREHOUSE_URL / WAREHOUSE_SERVICE_TOKEN / CF_ACCESS_* /
 *          RAKUTEN_SHOP_SLUG / PICKING_DRIVE_FOLDER_ID)
 *
 * URLパスは portal と完全に同一 (/apps/picking/...) — iPhone の PWA と引当RPA の
 * PICKING_API_URL は「ホスト名の変更だけ」で移行できる。
 *
 * セットアップ手順書: AI_reference/システム設計/ピッキング支援システム_ミニPC移行手順_20260812.md
 */
// ミニPCでは env を .env ファイルで管理する (Render の env 注入に相当。CWD の .env を起動時に1回読む。
// ⚠ .env を変更したらサービス再起動が必要 — feedback_インフラ運用「常駐サーバは.envを起動時に1回しか読まない」)
import 'dotenv/config';
import express from 'express';
import session from 'express-session';
import connectSqlite3 from 'connect-sqlite3';
import bcrypt from 'bcryptjs';
import BetterSqlite3 from 'better-sqlite3';
import crypto from 'node:crypto';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

// router の import 時に initPickingDB() が走る (migration 失敗はここで throw して起動を止める)
import pickingRouter from './router.js';
import pickingIngestRouter from './ingest-router.js';
import { getDB as getPickingDB } from './db.js';
import { handleLineWebhook } from './notify.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, '..', '..');   // リポジトリルート (public/ と views/login.ejs を流用)

const app = express();
const SQLiteStore = connectSqlite3(session);

// --- 設定 ---
const PORT = Number(process.env.PICKING_PORT || 3100);
const SESSION_SECRET = process.env.SESSION_SECRET || 'dev-secret-change-me';
const DATA_DIR = process.env.DATA_DIR || path.join(ROOT, 'data');
const USERS_FILE = path.join(DATA_DIR, 'users.json');

if (process.env.NODE_ENV === 'production' && SESSION_SECRET === 'dev-secret-change-me') {
  console.error('[FATAL] SESSION_SECRET 未設定で production 起動不可');
  process.exit(78);
}
// 初期管理者のパスワードも production では必須 (既知の既定値 changeme で外部公開しない — Codex high)
if (process.env.NODE_ENV === 'production' && !process.env.PORTAL_PASS && !fs.existsSync(path.join(process.env.DATA_DIR || path.join(ROOT, 'data'), 'users.json'))) {
  console.error('[FATAL] PORTAL_PASS 未設定で users.json も無いため production 起動不可 (初期管理者を作れない)');
  process.exit(78);
}

// --- boot-id (コンテナ/サービス再起動の切り分け用。feedback_インフラ運用の教訓) ---
const BOOT_ID = crypto.randomBytes(4).toString('hex');
const BOOT_AT = Date.now();
function bootLog(msg) {
  console.log(`[picking-standalone boot=${BOOT_ID} up=${Math.round((Date.now() - BOOT_AT) / 1000)}s] ${msg}`);
}

// --- ユーザー永続化 (server.js と同形式の users.json。DATA_DIR が別なのでファイルも別) ---
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

function loadUsers() {
  // ファイルが「存在するのに読めない/壊れている」場合は起動を止める。
  // 破損時に初期管理者で上書き再生成すると、登録済みユーザーが消えた上に
  // 既知パスワードの管理者が出来てしまう (Codex high)
  if (!fs.existsSync(USERS_FILE)) return null;
  try {
    const parsed = JSON.parse(fs.readFileSync(USERS_FILE, 'utf-8'));
    if (!Array.isArray(parsed) || parsed.length === 0) throw new Error('users.json が空または配列ではない');
    return parsed;
  } catch (e) {
    console.error(`[FATAL] users.json の読み込みに失敗 (上書きせず停止): ${e.message}`);
    process.exit(78);
  }
}

let users = loadUsers();
if (!users) {
  users = [{
    email: 'd.nakahara@b-faith.biz',
    passwordHash: bcrypt.hashSync(process.env.PORTAL_PASS || 'changeme', 10),
    displayName: '中原 大輔',
    role: 'admin',
    allowedApps: '*',
  }];
  fs.writeFileSync(USERS_FILE, JSON.stringify(users, null, 2), 'utf-8');
  bootLog('[Users] 初期管理者ユーザーを作成しました');
}

// --- ミドルウェア ---
app.set('trust proxy', 1); // Cloudflare Tunnel 経由
app.set('view engine', 'ejs');
app.set('views', path.join(ROOT, 'views'));
app.use(express.static(path.join(ROOT, 'public')));
app.use(express.urlencoded({ extended: false }));

// --- Liveness / Readiness probes (loopback only。watchdog.ps1 の流用先) ---
function loopbackOnly(req, res, next) {
  const sock = req.socket?.remoteAddress || '';
  if (sock === '127.0.0.1' || sock === '::1' || sock === '::ffff:127.0.0.1') return next();
  return res.status(404).end();
}
app.get('/livez', loopbackOnly, (req, res) => {
  res.status(200).json({ status: 'alive', pid: process.pid, bootId: BOOT_ID, uptimeSec: Math.round((Date.now() - BOOT_AT) / 1000) });
});
app.get('/readyz', loopbackOnly, (req, res) => {
  // DBに実際に触れて確認する (ディスク満杯・ロック等で liveness と乖離しないように — Codex medium)
  try {
    getPickingDB().prepare('SELECT 1').get();
    res.status(200).json({ status: 'ready', pid: process.pid, bootId: BOOT_ID });
  } catch (e) {
    res.status(503).json({ status: 'not_ready', error: e.message, bootId: BOOT_ID });
  }
});

// --- セッション (server.js と同方式: connect-sqlite3 + WAL。DATA_DIR が別なので portal とは独立) ---
try {
  const sdb = new BetterSqlite3(path.join(DATA_DIR, 'sessions.db'));
  const mode = sdb.pragma('journal_mode = WAL', { simple: true });
  sdb.pragma('busy_timeout = 5000');
  sdb.close();
  bootLog(`[session-store] sessions.db journal_mode=${mode}`);
} catch (e) {
  console.warn('[session-store] WAL 設定スキップ:', e.message);
}

app.use(session({
  store: new SQLiteStore({ db: 'sessions.db', dir: DATA_DIR }),
  secret: SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  rolling: true, // 無操作24時間で失効 (portal と同じ)
  cookie: {
    maxAge: 1 * 24 * 60 * 60 * 1000,
    httpOnly: true,
    // Tunnel 経由 (X-Forwarded-Proto: https) では Secure。ローカル http の smoke テストでも動くよう 'auto'
    secure: 'auto',
    sameSite: 'lax',
  },
}));

// --- ログイン (server.js の最小サブセット。users.json + bcrypt。ビューは portal の login.ejs を流用) ---
function popReturnTo(req) {
  const dest = req.session && req.session.returnTo;
  if (req.session) delete req.session.returnTo;
  if (typeof dest === 'string' && dest.startsWith('/') && !dest.startsWith('//')) return dest;
  return '/apps/picking/';
}

app.get('/login', (req, res) => {
  if (req.session.authenticated) return res.redirect(popReturnTo(req));
  res.render('login', { error: null });
});

app.post('/login', (req, res) => {
  const { email, password } = req.body || {};
  const user = users.find(u => u.email.toLowerCase() === String(email || '').toLowerCase());
  if (user && bcrypt.compareSync(String(password || ''), user.passwordHash)) {
    req.session.authenticated = true;
    req.session.email = user.email;
    req.session.displayName = user.displayName;
    req.session.role = user.role;
    req.session.allowedApps = user.allowedApps;
    return res.redirect(popReturnTo(req));
  }
  res.render('login', { error: 'メールアドレスまたはパスワードが正しくありません' });
});

app.get('/logout', (req, res) => {
  req.session.destroy(() => res.redirect('/login'));
});

// --- LINE webhook (groupId取得用・署名検証つき)。raw body が必要なので json parser より先 ---
app.post('/apps/picking/line-webhook', express.raw({ type: () => true, limit: '1mb' }), (req, res) => {
  if (handleLineWebhook(req.body, req.headers['x-line-signature'])) return res.status(200).end();
  res.status(403).end();
});

// --- picking 本体 (mount パスは server.js と同一) ---
// 取込API (x-api-key 認証・セッション外) は本体より先に mount
app.use('/apps/picking/ingest-api', pickingIngestRouter);
// 認証は router 内の pickingAccess (セッション or 登録端末Cookie) が担う
app.use('/apps/picking', express.json({ limit: '256kb' }), pickingRouter);

// ルートはポータルではなく picking 一覧へ (このサーバにダッシュボードは存在しない)
app.get('/', (req, res) => res.redirect('/apps/picking/'));

// --- 起動 ---
// 既定は loopback のみ (Cloudflare Tunnel が同一マシンから localhost を叩く構成)。
// 全インターフェース待ち受けだと LAN から平文HTTPで直接届き、Tunnel/TLS を迂回できてしまう
// (Codex high)。LAN公開が必要になったら PICKING_HOST=0.0.0.0 を明示する
const HOST = process.env.PICKING_HOST || '127.0.0.1';
const server = app.listen(PORT, HOST, () => {
  bootLog(`picking standalone listening on http://${HOST}:${PORT} (DATA_DIR=${DATA_DIR})`);
});

// --- graceful shutdown (winsw の stop / 再起動で計測イベントを取りこぼさない) ---
for (const sig of ['SIGTERM', 'SIGINT']) {
  process.on(sig, () => {
    bootLog(`${sig} received — closing`);
    server.close(() => process.exit(0));
    // close が10秒で完了しない場合は強制終了 (keep-alive 接続の待ちで固まらない)
    setTimeout(() => process.exit(0), 10_000).unref();
  });
}
