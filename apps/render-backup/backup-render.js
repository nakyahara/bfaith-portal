/**
 * backup-render.js — Render 一次データの自己完結バックアップ
 *
 * 背景: inquiry-hub のチケット/メール履歴・出荷実績 (sl_*)・発注 (po_*)・誤出荷 (f_mis_*)・
 *   月末棚卸 (inv_*)・経営確定値 (mgmt_*)・AI レポート (ai_*) 等は Render の persistent disk が
 *   一次保管で、miniPC からは再構築できない (2026-07-19 バックアップ体制判断。
 *   warehouse.db 版 = apps/warehouse/backup-warehouse.js の姉妹版)。
 *
 * 設計 (セキュリティ判断 2026-07-19、中原さん承認):
 *   - Render 自己完結: サーバ内 cron → スナップショット → 検証 → gzip → rclone で Google Drive へ
 *     **外向き送信のみ**。DB ダウンロード用の新規公開エンドポイントは作らない。miniPC も関与しない
 *   - warehouse-mirror.db は mirror_ / mart_ / sync_ 系 (miniPC から再構築可能) を除いた
 *     一次データテーブルだけを論理エクスポート (ディスク 5GB 制約と転送量のため)
 *   - inquiry-hub.db ほか単体 DB は VACUUM INTO で丸ごと
 *
 * 対象:
 *   mirror-primary  = warehouse-mirror.db の非派生テーブル (論理エクスポート)
 *   inquiry-hub     = inquiry-hub.db (VACUUM INTO)
 *   小物 DB         = fba / profit / ranking-checker / rakuten-yahoo-sync / mercari-settings / easy-ship / shohyo-links / staff
 *                     / fba-box (FBA箱詰め記録) / postage (定形外送料) (存在すれば)
 *   users.json      = アカウント定義 (構造検証つき)
 *   対象外          = sessions.db (揮発)、warehouse.db (miniPC が正本)、mirror_ / mart_ / sync_ 系 (再構築可)、
 *                     picking.db (miniPC 移行済の残骸)、jobs-monitor.db (再構築可)
 *
 * 監視: 成功/失敗を jobs-monitor に ping する (台帳 id = render-backup、config/jobs-registry.mjs)。
 *   2026-09-05 の実機確認で「Dark Launch のまま 7 週間・台帳未登録で無音」だったことが判明したため追加。
 *   有効化 (RENDER_BACKUP_CRON_ENABLED) されていない間は ping が来ない = 締切超過として要対応に出る (それが狙い)。
 *
 * 欠落防止 (Codex R1 High#1): cron 定刻だけでなく、起動5分後の catch-up (当日分が無く定刻を
 *   過ぎていれば実行) + 6時間毎の staleness 監視 (最終成功から26h超で実行) を持つ。
 *   最終成功は BACKUP_DIR/last-success.json に永続化。
 *
 * 起動: server.js から startRenderBackupCron() (RENDER_BACKUP_CRON_ENABLED=1 のときだけ、Dark Launch)。
 *   手動: node apps/render-backup/backup-render.js run
 *   ※ 手動プロセスと server cron の同時実行は run.lock (pid 記録、死亡 pid は自動解放) で排他
 *
 * env:
 *   RENDER_BACKUP_CRON_ENABLED   '1'|'true' で cron 有効 (default OFF)
 *   RENDER_BACKUP_CRON           cron 式 UTC (default '30 18 * * *' = JST 03:30)
 *   BACKUP_RCLONE_REMOTE         例 gdrive:bfaith-backup/render (miniPC と同じ変数名の流儀)
 *   BACKUP_RCLONE_CONFIG         rclone.conf のパス (Render Secret File 例 /etc/secrets/rclone.conf)
 *   BACKUP_REQUIRE_REMOTE        '0' で offsite 未設定をローカルのみとして許容 (default 1 = 失敗扱い)
 *   BACKUP_LOCAL_KEEP            ローカル保持世代 (default 1 — Render ディスクは狭い)
 *   BACKUP_UPLOAD_TIMEOUT_MS     rclone 転送上限 (default 3600000)
 *   BACKUP_REMOTE_DAILY_KEEP_DAYS / BACKUP_REMOTE_MONTHLY_KEEP_DAYS (default 14 / 400)
 *   BACKUP_GZIP_LEVEL            1-9 (default 5)
 *   GCHAT_WEBHOOK                結果通知 (disk-watch と同じ。未設定なら stdout のみ)
 */
import Database from 'better-sqlite3';
import cron from 'node-cron';
import { execFileSync } from 'child_process';
import fs from 'fs';
import path from 'path';
import zlib from 'zlib';
import crypto from 'crypto';
import { pipeline } from 'stream/promises';
import { Writable } from 'stream';
// jobs-monitor への成否記録。監視側の失敗はヘルパー内で握り潰されるのでバックアップ本体を巻き添えにしない
import { pingJob } from '../jobs-monitor/ping-local.js';

const JOB_ID = 'render-backup'; // config/jobs-registry.mjs の id

const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), 'data');
const BACKUP_DIR = process.env.BACKUP_DIR || path.join(DATA_DIR, 'backup-render');
const DAILY_DIR = path.join(BACKUP_DIR, 'daily');
const LAST_SUCCESS_FILE = path.join(BACKUP_DIR, 'last-success.json');
const LOCK_FILE = path.join(BACKUP_DIR, 'run.lock');

function envInt(name, def, min, max) {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return def;
  const n = Number(raw);
  if (!Number.isInteger(n) || n < min || n > max) {
    throw new Error(`env ${name} が不正です: "${raw}" (整数 ${min}〜${max})`);
  }
  return n;
}

const REQUIRE_REMOTE = process.env.BACKUP_REQUIRE_REMOTE !== '0';
const REMOTE = (process.env.BACKUP_RCLONE_REMOTE || '').trim();
const RCLONE_CONFIG = (process.env.BACKUP_RCLONE_CONFIG || '').trim();

// warehouse-mirror.db のうち「miniPC から再構築できる派生データ」の接頭辞 (これ以外を全部エクスポート =
// 新設テーブルの取り漏らしを防ぐ除外方式)。sync_ は同期台帳全般 (Codex R1 High#3: 個別列挙だと
// 将来の sync_* 追加が一次データ扱いになり肥大する)
const DERIVED_PREFIXES = ['mirror_', 'mart_', 'sync_', 'sqlite_'];

// 各対象の sentinel (0件なら空バックアップとして失敗)。常に1件以上あるはずの表だけを選ぶ
const TARGETS = [
  {
    key: 'mirror-primary',
    file: 'warehouse-mirror.db',
    mode: 'logical',
    required: true,
    // sl_shipping_slips は 2026-08-26 の shipping-log 吸い上げ廃止で DROP 済みのため sentinel から外した
    sentinels: ['po_orders', 'f_mis_shipments', 'inv_snapshot'],
  },
  { key: 'inquiry-hub', file: 'inquiry-hub.db', mode: 'vacuum', required: true, sentinels: ['inquiries', 'inquiry_messages'] },
  { key: 'fba', file: 'fba.db', mode: 'vacuum', required: false, sentinels: [] },
  { key: 'profit', file: 'profit.db', mode: 'vacuum', required: false, sentinels: [] },
  { key: 'ranking-checker', file: 'ranking-checker.db', mode: 'vacuum', required: false, sentinels: [] },
  { key: 'rakuten-yahoo-sync', file: 'rakuten-yahoo-sync.db', mode: 'vacuum', required: false, sentinels: [] },
  { key: 'mercari-settings', file: 'mercari-settings.db', mode: 'vacuum', required: false, sentinels: [] },
  { key: 'easy-ship', file: 'easy-ship.db', mode: 'vacuum', required: false, sentinels: ['es_package_size_master'] },
  { key: 'shohyo-links', file: 'shohyo-links.db', mode: 'vacuum', required: false, sentinels: ['vendor_links'] },
  { key: 'staff', file: 'staff.db', mode: 'vacuum', required: false, sentinels: ['staff'] },
  // 2026-09-05 実機確認 (CompanyDB構想 R-2) で対象外だと判明した Render 正本 2 本。
  // 両方まだ小さく (0.2MB / 4KB+WAL)、行が増える前の DB なので sentinel (1件以上) は置かず、
  // 中核テーブルの存在だけ expect_tables で検証する (スキーマ消失・別 DB を ok 扱いしない)
  { key: 'fba-box', file: 'fba-box.db', mode: 'vacuum', required: false, sentinels: [], expect_tables: ['fbx_runs', 'fbx_placements', 'fbx_events'] },
  { key: 'postage', file: 'postage.db', mode: 'vacuum', required: false, sentinels: [], expect_tables: ['pm_settings', 'pm_tariff_bands', 'pm_skus'] },
  { key: 'users', file: 'users.json', mode: 'file', required: true, sentinels: [] },
];

function jstToday() {
  return new Date(Date.now() + 9 * 3600 * 1000).toISOString().slice(0, 10);
}

function jstHourMin() {
  const d = new Date(Date.now() + 9 * 3600 * 1000);
  return d.getUTCHours() * 60 + d.getUTCMinutes();
}

function fmtMB(bytes) {
  if (bytes >= 1e9) return `${Math.round((bytes / 1e9) * 100) / 100}GB`;
  return `${Math.round((bytes / 1e6) * 10) / 10}MB`;
}

function freeBytes(dir) {
  const s = fs.statfsSync(dir);
  return s.bavail * s.bsize;
}

async function notify(text) {
  const hook = process.env.GCHAT_WEBHOOK;
  if (!hook) {
    console.warn('[render-backup] [NOTIFY:status=skipped] GCHAT_WEBHOOK未設定');
    return;
  }
  try {
    const res = await fetch(hook, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text }),
      signal: AbortSignal.timeout(15000),
    });
    console.log(`[render-backup] [NOTIFY:status=${res.ok ? 'sent' : 'failed'}]`);
  } catch (e) {
    console.error('[render-backup] [NOTIFY:status=failed]', e.message);
  }
}

// rclone 実行 (backup-warehouse.js と同方針: shell:false、自己上限 --max-duration/--cutoff-mode hard、
// 非転送フェーズのハングも --timeout/--contimeout/--retries で有限化)
function rclone(args, timeoutMs) {
  const durArgs = [
    '--max-duration', `${Math.ceil(timeoutMs / 1000)}s`, '--cutoff-mode', 'hard',
    '--timeout', '5m', '--contimeout', '1m', '--retries', '1',
  ];
  const fullArgs = RCLONE_CONFIG ? ['--config', RCLONE_CONFIG, ...durArgs, ...args] : [...durArgs, ...args];
  try {
    return execFileSync('rclone', fullArgs, {
      encoding: 'utf-8', shell: false, windowsHide: true, timeout: timeoutMs, maxBuffer: 16 * 1024 * 1024,
    });
  } catch (e) {
    if (e.code === 'ENOENT') throw new Error('rclone が見つかりません (Dockerfile の rclone インストールを確認)');
    const stderrTail = (e.stderr || '').trim().split('\n').slice(-2).join(' | ');
    const kind = e.signal ? `timeout/${e.signal}` : `exit=${e.status}`;
    throw new Error(`rclone ${args[0]} 失敗 (${kind}): ${stderrTail || e.message.split('\n')[0]}`);
  }
}

// sentinels    = 1件以上あるはずの表 (0件なら空バックアップとして失敗)
// expectTables = 存在だけを検証する表 (行数は問わない)。まだ行が増えていない新しい DB 向け —
//                sentinel が置けない DB でも「スキーマを失った空 SQLite / 別 DB」を ok 扱いしないため (Codex R1 Medium)
function quickCheckAndSentinels(dbPath, label, sentinels, expectTables = []) {
  const db = new Database(dbPath, { readonly: true });
  try {
    db.pragma('busy_timeout = 60000');
    const values = db.pragma('quick_check').map((r) => Object.values(r)[0]);
    if (values.length !== 1 || values[0] !== 'ok') {
      throw new Error(`${label} quick_check NG: ${values.slice(0, 3).join(' / ')}`);
    }
    for (const t of expectTables) {
      if (!/^[A-Za-z0-9_]+$/.test(t)) throw new Error(`不正な expect_tables 名: ${t}`);
      const hit = db.prepare(`SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?`).get(t);
      if (!hit) throw new Error(`${label} 期待テーブル ${t} が存在しません (スキーマ消失 / 別 DB の疑い)`);
    }
    const counts = {};
    for (const t of sentinels) {
      if (!/^[A-Za-z0-9_]+$/.test(t)) throw new Error(`不正な sentinel 名: ${t}`);
      let c;
      try {
        c = db.prepare(`SELECT count(*) AS c FROM "${t}"`).get().c;
      } catch {
        throw new Error(`${label} sentinel ${t} が存在しません`);
      }
      if (c === 0) throw new Error(`${label} sentinel ${t} が 0 件 (空バックアップの疑い)`);
      counts[t] = c;
    }
    return counts;
  } finally {
    db.close();
  }
}

async function gzipFile(src, dest, level) {
  await pipeline(fs.createReadStream(src), zlib.createGzip({ level }), fs.createWriteStream(dest));
}

async function verifyGzip(gzPath, expectedBytes) {
  let total = 0;
  await pipeline(fs.createReadStream(gzPath), zlib.createGunzip(), new Writable({
    write(chunk, _e, cb) { total += chunk.length; cb(); },
  }));
  if (total !== expectedBytes) throw new Error(`gzip 検証NG: ${total} ≠ ${expectedBytes}`);
}

async function sha256File(p) {
  const hash = crypto.createHash('sha256');
  await pipeline(fs.createReadStream(p), new Writable({
    write(chunk, _e, cb) { hash.update(chunk); cb(); },
  }));
  return hash.digest('hex');
}

/**
 * warehouse-mirror.db から一次データテーブルだけを新しい SQLite へ論理エクスポート。
 * - 除外方式 (DERIVED_PREFIXES 以外は全部コピー) — 新設テーブルの取り漏らしを防ぐ
 * - テーブル DDL + データ + 対象テーブルのインデックスをコピー。index 作成失敗は
 *   バックアップ失敗 (Codex R1 Medium: UNIQUE index の欠落は整合性に響くため握り潰さない)。
 *   view/trigger は各アプリの initDB が起動時に再作成するためコピーしない (restore 手順書に明記)
 */
function logicalExport(srcPath, destPath) {
  const src = new Database(srcPath, { readonly: true });
  let tables;
  try {
    src.pragma('busy_timeout = 60000');
    tables = src.prepare(
      "SELECT name, sql FROM sqlite_master WHERE type='table' AND sql IS NOT NULL"
    ).all().filter((t) => !DERIVED_PREFIXES.some((p) => t.name.startsWith(p)));
  } finally {
    src.close();
  }
  if (tables.length === 0) throw new Error('mirror-primary: エクスポート対象テーブルが0件');
  const tableNames = new Set(tables.map((t) => t.name));

  const dest = new Database(destPath);
  try {
    // staging 専用ファイルなので耐障害性より速度 (途中で死んだら作り直し)
    dest.pragma('journal_mode = OFF');
    dest.pragma('synchronous = OFF');
    // 素のパスで ATTACH (?mode=ro は URI ファイル名でしか効かず SQLITE_CANTOPEN になる。
    // このコードは src に対して SELECT しか発行しない)
    dest.exec(`ATTACH DATABASE '${srcPath.replace(/'/g, "''")}' AS src`);
    // FK 検査は切る (トランザクション外でないと効かない)。better-sqlite3 は既定 ON なので、
    // 親テーブルより先に子テーブルへ INSERT すると「no such table: main.<親>」で落ちる
    // (2026-09-05 初回実運用: draft_step_progress → ph_steps の FK で FATAL、7 週間分の初回バックアップが失敗)。
    // ここは元 DB の一貫断面をそのまま写すだけなので、写し側で FK を検証する意味はない
    dest.pragma('foreign_keys = OFF');
    // 元DB側は1つの読み取りトランザクションで一貫断面を取る。
    // 先に全テーブルを作ってから INSERT する (FK OFF に加えて、作成順に依存しない二段構え)
    dest.exec('BEGIN');
    for (const t of tables) dest.exec(t.sql);
    for (const t of tables) {
      dest.exec(`INSERT INTO main."${t.name}" SELECT * FROM src."${t.name}"`);
    }
    dest.exec('COMMIT');
    // 対象テーブルのインデックスを tbl_name で正確に選択 (SQL文字列の正規表現判定はしない)
    const indexes = dest.prepare(
      "SELECT name, sql, tbl_name FROM src.sqlite_master WHERE type='index' AND sql IS NOT NULL"
    ).all().filter((i) => tableNames.has(i.tbl_name));
    for (const i of indexes) {
      dest.exec(i.sql); // 失敗 = throw = バックアップ失敗
    }
    dest.exec('DETACH DATABASE src');
  } finally {
    dest.close();
  }
  return tables.length;
}

function vacuumInto(srcPath, destPath) {
  const src = new Database(srcPath, { readonly: true });
  try {
    src.pragma('busy_timeout = 60000');
    src.prepare('VACUUM INTO ?').run(destPath);
  } finally {
    src.close();
  }
}

// users.json の構造検証 (Codex R1 Medium: JSON.parse が通るだけでは復元不能な内容でも成功になる)。
// server.js loadUsers() の実構造 = [{email, passwordHash, role, ...}] の配列で admin が1人以上
function validateUsersJson(buf) {
  const data = JSON.parse(buf.toString('utf-8'));
  if (!Array.isArray(data) || data.length === 0) throw new Error('users.json が配列でないか空');
  for (const u of data) {
    if (!u || typeof u.email !== 'string' || typeof u.passwordHash !== 'string') {
      throw new Error('users.json に email/passwordHash を欠くエントリ');
    }
  }
  if (!data.some((u) => u.role === 'admin')) throw new Error('users.json に admin がいない');
  return data.length;
}

// ─── 排他 (Codex: 手動 run プロセスと server cron の同時実行対策)。
// Render は container 単位で死ぬので「pid 死亡 = 残置ロック」と断定してよい (孤児プロセス問題なし)
function acquireRunLock() {
  fs.mkdirSync(BACKUP_DIR, { recursive: true });
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      fs.writeFileSync(LOCK_FILE, String(process.pid), { flag: 'wx' }); // wx = 原子的な作成
      return;
    } catch (e) {
      if (e.code !== 'EEXIST') throw e;
      // stale 判定は「pid が読めない/死んでいる」場合のみ (Codex R2 High#2: 生存 pid への
      // TTL 強制解放は長時間 run と手動 run の並走を生む。Render は container 単位で死ぬため
      // pid 生存確認が信頼できる。SIGKILL で孤児化した子 rclone の残存は SHA 一意名で実害を局限)
      let stale = false;
      try {
        const pid = Number(fs.readFileSync(LOCK_FILE, 'utf-8').trim());
        if (!Number.isInteger(pid) || pid <= 0) stale = true;
        else if (pid === process.pid) stale = true; // 自プロセスの残置 (前回 crash)
        else {
          try { process.kill(pid, 0); } catch (err) { if (err.code === 'ESRCH') stale = true; }
        }
      } catch { stale = true; }
      if (!stale) throw new Error('別プロセスが render-backup 実行中 (run.lock)');
      // 原子的 stale 回収 (Codex R3 High: 検出→unlink は非原子で、2プロセスが同時に stale を
      // 検出すると「Aが取得した新ロックをBが消す」二重取得になる。rename は原子的で回収の
      // 勝者は必ず1人。回収後の取得も wx (原子的) で決着し、敗者は再判定で生存ロックなら throw)
      const claimed = `${LOCK_FILE}.stale.pid${process.pid}`;
      try {
        fs.renameSync(LOCK_FILE, claimed);
        fs.unlinkSync(claimed);
      } catch { /* 他プロセスが先に回収 → 次のループの wx で決着 */ }
    }
  }
  throw new Error('run.lock の取得に失敗 (競合)');
}

function releaseRunLock() {
  try {
    const pid = Number(fs.readFileSync(LOCK_FILE, 'utf-8').trim());
    if (pid === process.pid) fs.unlinkSync(LOCK_FILE);
  } catch {}
}

// staging 残骸掃除: pid が死んでいる .tmp は年齢に関係なく削除 (Codex R1 High#2:
// 12h ルールだけだと数GBの残骸が半日ディスクを塞ぐ)。pid 不明な .tmp は 12h 超で削除
function cleanStaleStaging() {
  let names = [];
  try { names = fs.readdirSync(DAILY_DIR).filter((n) => n.endsWith('.tmp')); } catch { return; }
  const cutoff = Date.now() - 12 * 3600000;
  for (const n of names) {
    const fp = path.join(DAILY_DIR, n);
    const m = n.match(/\.pid(\d+)\.tmp$/);
    let dead = false;
    if (m) {
      const pid = Number(m[1]);
      if (pid === process.pid) dead = true;
      else {
        try { process.kill(pid, 0); } catch (e) { if (e.code === 'ESRCH') dead = true; }
      }
    }
    try {
      if (dead || fs.statSync(fp).mtimeMs < cutoff) {
        fs.unlinkSync(fp);
        console.log(`[render-backup] staging 残骸削除: ${n}`);
      }
    } catch {}
  }
}

function readLastSuccess() {
  try {
    const j = JSON.parse(fs.readFileSync(LAST_SUCCESS_FILE, 'utf-8'));
    if (j && /^\d{4}-\d{2}-\d{2}$/.test(j.business_date) && typeof j.at === 'string') return j;
  } catch {}
  return null;
}

let running = false;

/** 1回分のバックアップ本体。戻り値 = GChat 通知用サマリー文字列。失敗は throw */
export async function runRenderBackup() {
  if (running) throw new Error('前回の render-backup がまだ実行中 (スキップ)');
  running = true;
  const t0 = Date.now();
  const date = jstToday();
  const isMonthFirst = date.slice(8, 10) === '01';
  const warnings = [];
  const pid = process.pid;

  try {
    if (!REMOTE && REQUIRE_REMOTE) {
      throw new Error('BACKUP_RCLONE_REMOTE 未設定 (offsite が本目的。意図的なローカルのみ運用は BACKUP_REQUIRE_REMOTE=0)');
    }
    const LOCAL_KEEP = envInt('BACKUP_LOCAL_KEEP', 1, 1, 30);
    const UPLOAD_TIMEOUT_MS = envInt('BACKUP_UPLOAD_TIMEOUT_MS', 3600000, 60000, 4 * 3600000);
    const DAILY_KEEP_DAYS = envInt('BACKUP_REMOTE_DAILY_KEEP_DAYS', 14, 2, 3650);
    const MONTHLY_KEEP_DAYS = envInt('BACKUP_REMOTE_MONTHLY_KEEP_DAYS', 400, 35, 3650);
    const GZIP_LEVEL = envInt('BACKUP_GZIP_LEVEL', 5, 1, 9);

    acquireRunLock();
    // この run が確定名まで進めたファイル (途中失敗時の掃除対象候補 — Codex R2 High#1)
    const artifacts = []; // {key, gzPath, gzBytes, rawBytes, sha, remoteName, sentinels}
    const manifestPath = path.join(DAILY_DIR, `render-${date}.manifest.json`);
    try {
      // テスト専用フック: ロック保持時間を人工的に延ばして排他を検証する (本番では未設定)
      if (process.env.RENDER_BACKUP_TEST_HOLD_MS) {
        await new Promise((r) => setTimeout(r, envInt('RENDER_BACKUP_TEST_HOLD_MS', 0, 0, 60000)));
      }
      fs.mkdirSync(DAILY_DIR, { recursive: true });
      cleanStaleStaging();
      for (const target of TARGETS) {
        const srcPath = path.join(DATA_DIR, target.file);
        if (!fs.existsSync(srcPath)) {
          if (target.required) throw new Error(`必須対象が見つかりません: ${target.file}`);
          warnings.push(`🟡 ${target.key} なし (スキップ)`);
          continue;
        }
        const srcSize = fs.statSync(srcPath).size;
        // 空き容量ガード (Codex R1 Medium: モード別に見積る)。
        //   vacuum/file: raw ≈ srcSize + gz ≤ raw で 2倍 + 余裕
        //   logical: 大半を除外するので srcSize 基準は過大 → 固定余裕のみで開始し、
        //            raw 完成後に gzip 分 (rawSize×1.1) を再チェック
        const need = target.mode === 'logical' ? 300e6 : srcSize * 2 + 100e6;
        const free = freeBytes(DAILY_DIR);
        if (free < need) {
          throw new Error(`空き容量不足: 残り ${fmtMB(free)} < 必要目安 ${fmtMB(need)} (${target.key})`);
        }

        const rawTmp = path.join(DAILY_DIR, `${target.key}-${date}.raw.pid${pid}.tmp`);
        const gzTmp = path.join(DAILY_DIR, `${target.key}-${date}.gz.pid${pid}.tmp`);
        try {
          console.log(`[render-backup] ${target.key}: snapshot 開始 (元 ${fmtMB(srcSize)})`);
          let sentinelCounts = {};
          if (target.mode === 'logical') {
            const n = logicalExport(srcPath, rawTmp);
            console.log(`[render-backup] ${target.key}: ${n} テーブルを論理エクスポート`);
            sentinelCounts = quickCheckAndSentinels(rawTmp, target.key, target.sentinels, target.expect_tables || []);
          } else if (target.mode === 'vacuum') {
            vacuumInto(srcPath, rawTmp);
            sentinelCounts = quickCheckAndSentinels(rawTmp, target.key, target.sentinels, target.expect_tables || []);
          } else {
            const content = fs.readFileSync(srcPath);
            const n = validateUsersJson(content);
            sentinelCounts = { users: n };
            fs.writeFileSync(rawTmp, content);
          }
          const rawBytes = fs.statSync(rawTmp).size;
          if (target.mode === 'logical' && freeBytes(DAILY_DIR) < rawBytes * 1.1 + 50e6) {
            throw new Error(`空き容量不足 (gzip 分): raw ${fmtMB(rawBytes)} に対し残り ${fmtMB(freeBytes(DAILY_DIR))}`);
          }
          await gzipFile(rawTmp, gzTmp, GZIP_LEVEL);
          await verifyGzip(gzTmp, rawBytes);
          fs.unlinkSync(rawTmp);
          const gzBytes = fs.statSync(gzTmp).size;
          const sha = await sha256File(gzTmp);
          const ext = target.mode === 'file' ? 'json.gz' : 'db.gz';
          const finalName = `${target.key}-${date}-${sha.slice(0, 8)}.${ext}`;
          const finalPath = path.join(DAILY_DIR, finalName);
          fs.renameSync(gzTmp, finalPath);
          artifacts.push({ key: target.key, gzPath: finalPath, gzBytes, rawBytes, sha, remoteName: finalName, sentinels: sentinelCounts });
          console.log(`[render-backup] ${target.key}: ${fmtMB(rawBytes)}→gz ${fmtMB(gzBytes)}`);
        } finally {
          // 失敗時も自分の staging を即時削除 (Codex R1 High#2: 狭いディスクに数GB残さない)
          try { fs.unlinkSync(rawTmp); } catch {}
          try { fs.unlinkSync(gzTmp); } catch {}
        }
      }

      // ─── 2. manifest (この run の全 artifact を記述) ───
      const manifest = {
        business_date: date,
        created_at: new Date().toISOString(),
        artifacts: artifacts.map((a) => ({
          key: a.key, file: a.remoteName, raw_bytes: a.rawBytes, gz_bytes: a.gzBytes, gz_sha256: a.sha, sentinels: a.sentinels,
        })),
        derived_prefixes_excluded: DERIVED_PREFIXES,
        note: 'mirror-primary は view/trigger 未収録 (各アプリ initDB が起動時に再作成)。mirror_/mart_/sync_ は miniPC から再同期で復元',
      };
      const manifestTmp = `${manifestPath}.pid${pid}.tmp`;
      fs.writeFileSync(manifestTmp, JSON.stringify(manifest, null, 2));
      fs.renameSync(manifestTmp, manifestPath);

      // ─── 3. rclone offsite ───
      let remoteNote = '🟡 offsite未設定(ローカルのみ)';
      let remoteOk = false;
      if (REMOTE) {
        for (const a of artifacts) {
          console.log(`[render-backup] upload ${a.remoteName} (${fmtMB(a.gzBytes)})`);
          rclone(['copyto', a.gzPath, `${REMOTE}/daily/${a.remoteName}`], UPLOAD_TIMEOUT_MS);
        }
        rclone(['copyto', manifestPath, `${REMOTE}/daily/render-${date}.manifest.json`], 300000);
        remoteOk = true;
        remoteNote = `offsite=${artifacts.length}本`;
        // 同日の旧 artifact (再実行で SHA が変わり manifest から外れたもの) をリモートからも掃除
        // (Codex R1 High#4)。失敗は warning (保持過多になるだけでデータは失われない)
        try {
          const current = new Set(artifacts.map((a) => a.remoteName));
          const listed = rclone(['lsf', `${REMOTE}/daily`, '--include', `*-${date}-*`], 300000)
            .split('\n').map((s) => s.trim()).filter(Boolean);
          for (const n of listed) {
            if (!current.has(n)) rclone(['deletefile', `${REMOTE}/daily/${n}`], 300000);
          }
        } catch (e) {
          warnings.push(`🟡 リモート同日旧世代の掃除失敗 (${e.message})`);
        }
        try {
          rclone(['delete', `${REMOTE}/daily`, '--min-age', `${DAILY_KEEP_DAYS}d`], 600000);
        } catch (e) {
          warnings.push(`🟡 リモート日次掃除失敗 (${e.message})`);
        }
        if (isMonthFirst) {
          const ym = date.slice(0, 7);
          for (const a of artifacts) {
            rclone(['copyto', `${REMOTE}/daily/${a.remoteName}`, `${REMOTE}/monthly/${a.remoteName.replace(date, ym)}`], 1800000);
          }
          rclone(['copyto', `${REMOTE}/daily/render-${date}.manifest.json`, `${REMOTE}/monthly/render-${ym}.manifest.json`], 300000);
          try {
            rclone(['delete', `${REMOTE}/monthly`, '--min-age', `${MONTHLY_KEEP_DAYS}d`], 600000);
          } catch (e) {
            warnings.push(`🟡 リモート月次掃除失敗 (${e.message})`);
          }
          remoteNote += ` +monthly`;
        }
      }

      // ─── 4. ローカル世代整理 (upload 成功後のみ)。
      // 旧日付世代 + 同日の manifest 非参照 artifact (再実行の残骸) を削除 (Codex R1 High#4)
      if (remoteOk || !REQUIRE_REMOTE) {
        const currentFiles = new Set([...artifacts.map((a) => a.remoteName), `render-${date}.manifest.json`]);
        const allFiles = fs.readdirSync(DAILY_DIR).filter((n) => !n.endsWith('.tmp'));
        const dates = [...new Set(
          allFiles.map((n) => (n.match(/-(\d{4}-\d{2}-\d{2})[-.]/) || [])[1]).filter(Boolean)
        )].sort().reverse();
        for (const oldDate of dates.slice(LOCAL_KEEP)) {
          for (const n of allFiles.filter((x) => x.includes(oldDate))) {
            try { fs.unlinkSync(path.join(DAILY_DIR, n)); } catch {}
          }
          console.log(`[render-backup] ローカル旧世代削除: ${oldDate}`);
        }
        for (const n of allFiles.filter((x) => x.includes(date) && !currentFiles.has(x))) {
          try { fs.unlinkSync(path.join(DAILY_DIR, n)); } catch {}
          console.log(`[render-backup] 同日旧artifact削除: ${n}`);
        }
      }

      // ─── 5. 月初のみ: offsite からのフル復元テスト (最重要 = mirror-primary) ───
      let restoreNote = '';
      if (isMonthFirst && REMOTE) {
        const main = artifacts.find((a) => a.key === 'mirror-primary');
        if (main && freeBytes(BACKUP_DIR) > main.rawBytes * 1.2 + main.gzBytes) {
          const rGz = path.join(BACKUP_DIR, `restore.pid${pid}.gz.tmp`);
          const rDb = path.join(BACKUP_DIR, `restore.pid${pid}.db.tmp`);
          try {
            rclone(['copyto', `${REMOTE}/daily/${main.remoteName}`, rGz], UPLOAD_TIMEOUT_MS);
            await pipeline(fs.createReadStream(rGz), zlib.createGunzip(), fs.createWriteStream(rDb));
            quickCheckAndSentinels(rDb, '復元テスト', TARGETS[0].sentinels);
            restoreNote = ' 復元テスト(offsite)ok';
            console.log('[render-backup] 復元テスト(offsite) ok');
          } finally {
            try { fs.unlinkSync(rGz); } catch {}
            try { fs.unlinkSync(rDb); } catch {}
          }
        } else {
          warnings.push('🟡 月初復元テストskip (空き容量不足)');
        }
      }

      // ─── 6. 成功記録 (catch-up / staleness 監視が参照) ───
      const successTmp = `${LAST_SUCCESS_FILE}.pid${pid}.tmp`;
      fs.writeFileSync(successTmp, JSON.stringify({ business_date: date, at: new Date().toISOString() }));
      fs.renameSync(successTmp, LAST_SUCCESS_FILE);

      const sec = Math.round((Date.now() - t0) / 1000);
      const totalGz = artifacts.reduce((s, a) => s + a.gzBytes, 0);
      const warnNote = warnings.length ? ` ${warnings.join(' / ')}` : '';
      return `${artifacts.length}対象 計gz ${fmtMB(totalGz)}, ${remoteNote}${restoreNote} (${sec}秒)${warnNote}`;
    } catch (e) {
      // 途中失敗時: この run が確定名まで進めたファイルのうち、ディスク上の manifest から
      // 参照されていないものを削除 (Codex R2 High#1: 巨大 artifact が残ると空き容量チェックを
      // 阻害して失敗が自己固定化する)。当日の成功世代 (manifest 参照分) は温存
      try {
        const referenced = new Set([path.basename(manifestPath)]);
        try {
          const m = JSON.parse(fs.readFileSync(manifestPath, 'utf-8'));
          if (m && m.business_date === date && Array.isArray(m.artifacts)) {
            for (const a of m.artifacts) referenced.add(a.file);
          }
        } catch {}
        for (const a of artifacts) {
          if (!referenced.has(a.remoteName)) {
            try { fs.unlinkSync(a.gzPath); console.log(`[render-backup] 失敗runのartifact削除: ${a.remoteName}`); } catch {}
          }
        }
      } catch {}
      throw e;
    } finally {
      releaseRunLock();
    }
  } finally {
    running = false;
  }
}

async function runAndNotify(label) {
  try {
    const summary = await runRenderBackup();
    console.log(`[render-backup] 完了 (${label}): ${summary}`);
    pingJob(JOB_ID, 'ok', `${label}: ${summary}`);
    await notify(`✅ *Renderバックアップ ${jstToday()}${label === 'cron' ? '' : ` (${label})`}*\n${summary}`);
  } catch (e) {
    console.error(`[render-backup] 失敗 (${label}):`, e.message);
    pingJob(JOB_ID, 'fail', `${label}: ${e.message}`);
    await notify(`❌ *Renderバックアップ ${jstToday()} 失敗${label === 'cron' ? '' : ` (${label})`}*\n${e.message}`);
  }
}

// cron 定刻 (UTC 18:30 = JST 03:30) を分に直した閾値。catch-up は「定刻+15分」を過ぎていたら発火
const SCHEDULED_JST_MIN = 3 * 60 + 30;

/**
 * server.js から呼ぶ。RENDER_BACKUP_CRON_ENABLED のときだけ schedule (Dark Launch)。
 * 欠落防止 (Codex R1 High#1):
 *   - 起動5分後: 当日分の成功記録が無く、定刻を過ぎていれば catch-up 実行
 *     (03:30 のデプロイ再起動・夜間クラッシュで cron を取りこぼした日を救う)
 *   - 6時間毎: 最終成功から 26h 超なら実行 (長期の無言欠落を検知して自己修復)
 */
export function startRenderBackupCron() {
  const enabled = process.env.RENDER_BACKUP_CRON_ENABLED;
  if (enabled !== '1' && enabled !== 'true') {
    console.log('[render-backup] RENDER_BACKUP_CRON_ENABLED 未設定のため cron 起動せず (Dark Launch)');
    return;
  }
  const expr = process.env.RENDER_BACKUP_CRON || '30 18 * * *'; // UTC 18:30 = JST 03:30
  if (!cron.validate(expr)) {
    // 有効化されているのに式が不正 = バックアップが永久に走らない設定事故 → GChat で顕在化 (Codex R1 Low)
    console.error(`[render-backup] RENDER_BACKUP_CRON が不正: "${expr}" → cron 起動せず`);
    notify(`❌ *Renderバックアップ 設定エラー*\nRENDER_BACKUP_CRON が不正: "${expr}" (cron 未起動)`).catch(() => {});
    return;
  }
  cron.schedule(expr, () => { runAndNotify('cron'); }, { timezone: 'UTC' });
  console.log(`[render-backup] cron 起動 (${expr} UTC)`);

  const bootTimer = setTimeout(() => {
    const last = readLastSuccess();
    if ((!last || last.business_date !== jstToday()) && jstHourMin() >= SCHEDULED_JST_MIN + 15) {
      console.log('[render-backup] 当日分の成功記録なし + 定刻超過 → catch-up 実行');
      runAndNotify('catch-up');
    }
  }, 5 * 60 * 1000);
  bootTimer.unref();

  const watchTimer = setInterval(() => {
    const last = readLastSuccess();
    const staleMs = last ? Date.now() - Date.parse(last.at) : Infinity;
    if (staleMs > 26 * 3600000 && !running) {
      console.warn('[render-backup] 最終成功から26h超 → staleness 実行');
      runAndNotify('staleness-recovery');
    }
  }, 6 * 3600000);
  watchTimer.unref();
}

// 手動実行: node apps/render-backup/backup-render.js run
if (process.argv[2] === 'run') {
  runRenderBackup()
    .then(async (summary) => {
      console.log(`[render-backup] 完了: ${summary}`);
      // 手動 run は別プロセスだが jobs-monitor.db は同じファイルなので、JOBS_MONITOR_ENABLED=1 の環境 (Render Shell) なら記録される
      pingJob(JOB_ID, 'ok', `manual: ${summary}`);
      await notify(`✅ *Renderバックアップ ${jstToday()} (手動)*\n${summary}`);
    })
    .catch(async (e) => {
      console.error(`FATAL: ${e.message}`);
      pingJob(JOB_ID, 'fail', `manual: ${e.message}`);
      await notify(`❌ *Renderバックアップ ${jstToday()} 失敗 (手動)*\n${e.message}`);
      process.exit(1);
    });
}
