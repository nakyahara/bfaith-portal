/**
 * test-render-backup.mjs — apps/render-backup/backup-render.js のローカルテスト
 * 使い方: node scripts/test-render-backup.mjs (repoルートで。テスト用DATA_DIRを自前で組む)
 */
import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';
import os from 'os';

const TEST_DIR = path.join(os.tmpdir(), `render-backup-test-${Date.now()}`);
fs.mkdirSync(TEST_DIR, { recursive: true });
process.env.DATA_DIR = TEST_DIR;
process.env.BACKUP_REQUIRE_REMOTE = '0';
delete process.env.BACKUP_RCLONE_REMOTE;
delete process.env.GCHAT_WEBHOOK;

// ── warehouse-mirror.db: mirror_* (除外対象) + 一次データ + index/view/trigger ──
{
  const db = new Database(path.join(TEST_DIR, 'warehouse-mirror.db'));
  db.exec(`
    CREATE TABLE mirror_products (id INTEGER PRIMARY KEY, name TEXT);
    CREATE TABLE mart_pl (id INTEGER PRIMARY KEY, v REAL);
    CREATE TABLE sync_run_ledger (id INTEGER PRIMARY KEY, note TEXT);
    -- 生成列 (VIRTUAL / STORED) は INSERT 対象外 → 論理エクスポートが SELECT * に依存しないことの回帰テスト (Codex R1)
    CREATE TABLE po_orders (id INTEGER PRIMARY KEY, supplier TEXT, status TEXT, qty INTEGER NOT NULL DEFAULT 1,
      qty_x2 INTEGER GENERATED ALWAYS AS (qty * 2) VIRTUAL, qty_x3 INTEGER GENERATED ALWAYS AS (qty * 3) STORED);
    CREATE TABLE f_mis_shipments (id INTEGER PRIMARY KEY, detail TEXT);
    CREATE TABLE inv_snapshot (id INTEGER PRIMARY KEY, month TEXT);
    CREATE TABLE ai_reports (id INTEGER PRIMARY KEY, body TEXT);
    -- FK の子 (名前順で先) → 親 (名前順で後)。2026-09-05 本番初回で draft_step_progress → ph_steps がこの形で
    -- 「no such table: main.ph_steps」を起こした。論理エクスポートが作成順・FK 検査に依存しないことの回帰テスト
    CREATE TABLE a_step_progress (id INTEGER PRIMARY KEY, step_id INTEGER NOT NULL REFERENCES z_steps(id));
    CREATE TABLE z_steps (id INTEGER PRIMARY KEY, name TEXT);
    CREATE INDEX idx_po_orders_status ON po_orders(status);
    CREATE VIEW v_po_summary AS SELECT status, count(*) c FROM po_orders GROUP BY status;
    CREATE TRIGGER trg_mis_append BEFORE DELETE ON f_mis_shipments BEGIN SELECT RAISE(ABORT, 'append-only'); END;
  `);
  const insPo = db.prepare('INSERT INTO po_orders (supplier, status) VALUES (?, ?)');
  for (let i = 0; i < 500; i++) insPo.run(`sup${i % 7}`, i % 2 ? 'open' : 'done');
  db.prepare('INSERT INTO mirror_products (name) VALUES (?)').run('should-be-excluded');
  db.prepare('INSERT INTO f_mis_shipments (detail) VALUES (?)').run('m-1');
  db.prepare('INSERT INTO inv_snapshot (month) VALUES (?)').run('2026-06');
  db.prepare('INSERT INTO ai_reports (body) VALUES (?)').run('r-1');
  db.prepare('INSERT INTO z_steps (id, name) VALUES (1, ?)').run('step-1');
  db.prepare('INSERT INTO a_step_progress (step_id) VALUES (1)').run(); // 子→親 FK あり。子が先にエクスポートされる
  db.close();
}
// ── inquiry-hub.db ──
{
  const db = new Database(path.join(TEST_DIR, 'inquiry-hub.db'));
  db.exec(`
    CREATE TABLE inquiries (id INTEGER PRIMARY KEY, subject TEXT);
    CREATE TABLE inquiry_messages (id INTEGER PRIMARY KEY, body TEXT);
  `);
  db.prepare('INSERT INTO inquiries (subject) VALUES (?)').run('test');
  db.prepare('INSERT INTO inquiry_messages (body) VALUES (?)').run('hello');
  db.close();
}
fs.writeFileSync(path.join(TEST_DIR, 'users.json'), JSON.stringify([
  { email: 'd.nakahara@b-faith.biz', passwordHash: 'hash', role: 'admin', allowedApps: '*' },
]));
// sessions.db は対象外であることの確認用に置いておく
new Database(path.join(TEST_DIR, 'sessions.db')).close();

const { runRenderBackup } = await import('../apps/render-backup/backup-render.js');

let failures = 0;
function check(name, cond) {
  console.log(`${cond ? 'PASS' : 'FAIL'}: ${name}`);
  if (!cond) failures++;
}

// ── T1: 正常系 (ローカルのみ) ──
const summary = await runRenderBackup();
console.log('summary:', summary);
const dailyDir = path.join(TEST_DIR, 'backup-render', 'daily');
const files = fs.readdirSync(dailyDir);
check('T1 mirror-primary gz あり', files.some((f) => f.startsWith('mirror-primary-')));
check('T1 inquiry-hub gz あり', files.some((f) => f.startsWith('inquiry-hub-')));
check('T1 users gz あり', files.some((f) => f.startsWith('users-')));
check('T1 manifest あり', files.some((f) => /^render-\d{4}-\d{2}-\d{2}\.manifest\.json$/.test(f)));
check('T1 sessions は対象外', !files.some((f) => f.includes('sessions')));
check('T1 staging ゴミなし', !files.some((f) => f.endsWith('.tmp')));
check('T1 小物DB欠如は警告扱い', summary.includes('🟡 fba なし'));

// ── T2: 論理エクスポートの中身検証 (mirror_* が入っていない・一次データが入っている) ──
{
  const gz = files.find((f) => f.startsWith('mirror-primary-'));
  const zlib = await import('zlib');
  const raw = zlib.gunzipSync(fs.readFileSync(path.join(dailyDir, gz)));
  const tmpDb = path.join(TEST_DIR, 'verify.db');
  fs.writeFileSync(tmpDb, raw);
  const db = new Database(tmpDb, { readonly: true });
  const names = db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((r) => r.name);
  check('T2 mirror_products 除外', !names.includes('mirror_products'));
  check('T2 mart_pl 除外', !names.includes('mart_pl'));
  check('T2 sync_* 除外', !names.includes('sync_run_ledger'));
  check('T2 po_orders 収録', names.includes('po_orders'));
  check('T2 po_orders 500行', db.prepare('SELECT count(*) c FROM po_orders').get().c === 500);
  const idx = db.prepare("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_po_orders_status'").get();
  check('T2 index 収録', !!idx);
  const views = db.prepare("SELECT count(*) c FROM sqlite_master WHERE type='view'").get().c;
  check('T2 view 非収録 (initDBが再作成)', views === 0);
  db.close();
  fs.unlinkSync(tmpDb);
}

// ── T3: manifest 内容 ──
{
  const mf = JSON.parse(fs.readFileSync(path.join(dailyDir, files.find((f) => f.endsWith('.manifest.json')))));
  const mp = mf.artifacts.find((a) => a.key === 'mirror-primary');
  check('T3 manifest sha あり', /^[0-9a-f]{64}$/.test(mp.gz_sha256));
  check('T3 sentinel 記録', mp.sentinels.po_orders === 500);
}

// ── T4: sentinel 0件 → 失敗 + staging即時掃除 (失敗経路で .tmp を残さない)
//        (対象は inv_snapshot。f_mis_shipments は append-only trigger で DELETE できない) ──
{
  const db = new Database(path.join(TEST_DIR, 'warehouse-mirror.db'));
  db.exec('DELETE FROM inv_snapshot');
  db.close();
  let threw = false;
  try { await runRenderBackup(); } catch (e) { threw = /inv_snapshot/.test(e.message); }
  check('T4 sentinel 0件で失敗', threw);
  check('T4 失敗後も staging ゴミなし', !fs.readdirSync(dailyDir).some((f) => f.endsWith('.tmp')));
  check('T4 失敗後も run.lock 解放済み', !fs.existsSync(path.join(TEST_DIR, 'backup-render', 'run.lock')));
  const db2 = new Database(path.join(TEST_DIR, 'warehouse-mirror.db'));
  db2.prepare('INSERT INTO inv_snapshot (month) VALUES (?)').run('2026-06');
  db2.close();
}

// ── T6: 同日再実行 (データ変化で別SHA) → 旧artifactがローカルから消える ──
{
  const before = fs.readdirSync(dailyDir).filter((f) => f.startsWith('mirror-primary-'));
  const db = new Database(path.join(TEST_DIR, 'warehouse-mirror.db'));
  db.prepare('INSERT INTO po_orders (supplier, status) VALUES (?, ?)').run('supX', 'open');
  db.close();
  await runRenderBackup();
  const after = fs.readdirSync(dailyDir).filter((f) => f.startsWith('mirror-primary-'));
  check('T6 同日再実行で mirror-primary は1本のみ', after.length === 1);
  check('T6 旧SHAのartifactは削除済み', !after.includes(before[0]));
  const mf2 = JSON.parse(fs.readFileSync(path.join(dailyDir, fs.readdirSync(dailyDir).find((f) => f.endsWith('.manifest.json')))));
  check('T6 manifestは新artifactを参照', mf2.artifacts.find((a) => a.key === 'mirror-primary').file === after[0]);
}

// ── T7: last-success 記録 ──
{
  const ls = JSON.parse(fs.readFileSync(path.join(TEST_DIR, 'backup-render', 'last-success.json')));
  check('T7 last-success 記録あり', /^\d{4}-\d{2}-\d{2}$/.test(ls.business_date));
}

// ── T8: users.json 構造不正 → 失敗 + 当日の成功世代 (manifest参照分) は温存 ──
{
  const beforeFiles = new Set(fs.readdirSync(dailyDir));
  fs.writeFileSync(path.join(TEST_DIR, 'users.json'), JSON.stringify({ notusers: true }));
  let threw = false;
  try { await runRenderBackup(); } catch (e) { threw = /users\.json/.test(e.message); }
  check('T8 users.json 構造不正で失敗', threw);
  const mf = JSON.parse(fs.readFileSync(path.join(dailyDir, [...beforeFiles].find((f) => f.endsWith('.manifest.json')))));
  const stillThere = mf.artifacts.every((a) => fs.existsSync(path.join(dailyDir, a.file)));
  check('T8 失敗しても manifest 参照の成功世代は温存', stillThere);
  fs.writeFileSync(path.join(TEST_DIR, 'users.json'), JSON.stringify([{ email: 'a@b.c', passwordHash: 'x', role: 'admin' }]));
}

// ── T10: 成功世代が無い状態での途中失敗 → 確定名まで進んだartifactも掃除される (子プロセス) ──
{
  const T10 = path.join(os.tmpdir(), `render-backup-t10-${Date.now()}`);
  fs.mkdirSync(T10, { recursive: true });
  {
    const db = new Database(path.join(T10, 'warehouse-mirror.db'));
    db.exec(`CREATE TABLE po_orders (id INTEGER PRIMARY KEY, s TEXT);
      CREATE TABLE f_mis_shipments (id INTEGER PRIMARY KEY);
      CREATE TABLE inv_snapshot (id INTEGER PRIMARY KEY);`);
    db.prepare('INSERT INTO po_orders (s) VALUES (?)').run('x');
    db.exec('INSERT INTO f_mis_shipments DEFAULT VALUES; INSERT INTO inv_snapshot DEFAULT VALUES;');
    db.close();
    const db2 = new Database(path.join(T10, 'inquiry-hub.db'));
    db2.exec('CREATE TABLE inquiries (id INTEGER PRIMARY KEY); CREATE TABLE inquiry_messages (id INTEGER PRIMARY KEY);');
    db2.exec('INSERT INTO inquiries DEFAULT VALUES; INSERT INTO inquiry_messages DEFAULT VALUES;');
    db2.close();
    fs.writeFileSync(path.join(T10, 'users.json'), '{"broken":true}'); // 最後の対象で失敗させる
  }
  const { execFileSync } = await import('child_process');
  let exitCode = 0;
  try {
    execFileSync(process.execPath, ['apps/render-backup/backup-render.js', 'run'], {
      encoding: 'utf-8',
      env: { ...process.env, DATA_DIR: T10, BACKUP_REQUIRE_REMOTE: '0' },
    });
  } catch (e) { exitCode = e.status; }
  check('T10 途中失敗で exit 1', exitCode === 1);
  const t10daily = path.join(T10, 'backup-render', 'daily');
  const leftovers = fs.existsSync(t10daily) ? fs.readdirSync(t10daily).filter((f) => f.endsWith('.gz')) : [];
  check('T10 確定名まで進んだartifactも掃除済み', leftovers.length === 0);
  fs.rmSync(T10, { recursive: true, force: true });
}

// ── T9: run.lock を他プロセスpid(生存)で握られている → 失敗 / 死亡pid → 自動解放 ──
{
  const lockPath = path.join(TEST_DIR, 'backup-render', 'run.lock');
  fs.writeFileSync(lockPath, '999999'); // 死んでいるpid
  await runRenderBackup(); // 自動解放して成功するはず
  check('T9 死亡pidロックは自動解放', true);
  fs.writeFileSync(lockPath, String(process.ppid || 1)); // 生きているpid (親)
  let threw = false;
  try { await runRenderBackup(); } catch (e) { threw = /実行中/.test(e.message); }
  check('T9 生存pidロックで拒否', threw);
  fs.unlinkSync(lockPath);
}

// ── T5: remote 必須 (default) で未設定 → 即失敗 ──
{
  // モジュールは import 済みで REQUIRE_REMOTE はロード時評価のため、子プロセスで検証
  const { execFileSync } = await import('child_process');
  let out = '';
  try {
    execFileSync(process.execPath, ['apps/render-backup/backup-render.js', 'run'], {
      encoding: 'utf-8',
      env: { ...process.env, BACKUP_REQUIRE_REMOTE: '1', DATA_DIR: TEST_DIR },
    });
  } catch (e) {
    out = `${e.stdout || ''}${e.stderr || ''}`;
  }
  check('T5 remote未設定はFATAL', out.includes('BACKUP_RCLONE_REMOTE 未設定'));
}

// ── T11: stale lock を2プロセスが同時回収 → 排他が破れない (勝者は1人だけ) ──
{
  const lockPath = path.join(TEST_DIR, 'backup-render', 'run.lock');
  fs.writeFileSync(lockPath, '999999'); // stale (死亡pid)
  const { spawn } = await import('child_process');
  const runChild = () => new Promise((resolve) => {
    const p = spawn(process.execPath, ['apps/render-backup/backup-render.js', 'run'], {
      env: { ...process.env, DATA_DIR: TEST_DIR, BACKUP_REQUIRE_REMOTE: '0', RENDER_BACKUP_TEST_HOLD_MS: '2500' },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let out = '';
    p.stdout.on('data', (d) => { out += d; });
    p.stderr.on('data', (d) => { out += d; });
    p.on('close', (code) => resolve({ code, out }));
  });
  const [a, b] = await Promise.all([runChild(), runChild()]);
  const successes = [a, b].filter((r) => r.code === 0).length;
  const refused = [a, b].filter((r) => r.code === 1 && /実行中/.test(r.out)).length;
  check('T11 同時実行で成功はちょうど1本', successes === 1);
  check('T11 もう1本は「実行中」で拒否', refused === 1);
}

// ── T12: expect_tables (存在だけ検証) — 0件の正しい DB は成功 / スキーマを失った DB は失敗 (Codex R1 Medium) ──
{
  const postagePath = path.join(TEST_DIR, 'postage.db');
  {
    const db = new Database(postagePath);
    db.exec(`CREATE TABLE pm_settings (k TEXT PRIMARY KEY, v TEXT);
      CREATE TABLE pm_tariff_bands (id INTEGER PRIMARY KEY);
      CREATE TABLE pm_skus (sku TEXT PRIMARY KEY);`); // 全部 0 件のまま
    db.close();
  }
  let ok = true;
  try { await runRenderBackup(); } catch (e) { ok = false; console.error('T12 unexpected:', e.message); }
  check('T12 期待テーブルが揃った 0件 DB は成功 (sentinel なしでも ok)', ok);
  fs.unlinkSync(postagePath);
  {
    const db = new Database(postagePath);
    db.exec('CREATE TABLE something_else (id INTEGER PRIMARY KEY)'); // スキーマ消失 / 別 DB を模す
    db.close();
  }
  let threw = false;
  try { await runRenderBackup(); } catch (e) { threw = /postage 期待テーブル pm_settings が存在しません/.test(e.message); }
  check('T12 期待テーブルが無い DB は失敗', threw);
  fs.unlinkSync(postagePath);
}

fs.rmSync(TEST_DIR, { recursive: true, force: true });
console.log(failures === 0 ? '\nALL PASS' : `\n${failures} FAILURES`);
process.exit(failures === 0 ? 0 : 1);
