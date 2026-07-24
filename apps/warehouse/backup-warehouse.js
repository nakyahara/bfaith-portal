/**
 * backup-warehouse.js — warehouse.db 日次バックアップ
 *
 * 背景: warehouse.db はモール横断の歴史データで再取得不能 (楽天RMS自動取得規制、
 *   モールCSVの遡及窓切れ)。これまで miniPC ローカルの .bak しか無く、ディスク故障で
 *   全消失するリスクがあった (2026-07-19 バックアップ体制判断で実装決定)。
 *
 * 処理: VACUUM INTO でスナップショット → quick_check + sentinel 検証 → gzip 圧縮 →
 *   gzip 整合性検証 → 原子的リネームで当日世代を確定 → rclone で Google Drive へ
 *   offsite 転送 (日次世代 + 月初のみ月次世代) → upload 成功後にローカル世代整理 →
 *   月初は offsite からダウンロードしてのフル復元テスト。
 *
 * 冪等性 (Codex R1 Critical#2): 当日分が既に完成済み (.gz + manifest 一致) なら
 *   スナップショット作成をスキップし、offsite 転送以降だけやり直す。成功済みの
 *   当日世代を失敗 run が壊すことはない (staging → 検証 → rename の原子的置換)。
 *
 * 呼び出し: daily-sync.js の終盤ジョブ 'DBバックアップ' (retry 対象)。単体実行:
 *     node -r dotenv/config apps/warehouse/backup-warehouse.js [--data-dir <dir>]
 *
 * env:
 *   DATA_DIR                        warehouse.db の場所 (db.js と同じ解決順)
 *   BACKUP_DIR                      出力先 (default: <DATA_DIR>/backup)。空き容量はこちらのボリュームで判定
 *   BACKUP_LOCAL_KEEP               ローカル保持世代数 (default: 3)。「直近3日」ではなく「成功した直近3回分」
 *   BACKUP_REQUIRE_REMOTE           '0' で offsite 未設定をローカルのみ運用として許容 (default: '1' = 未設定なら即失敗。
 *                                   ディスク故障対策が本目的のため、設定漏れをサイレントに流さない)
 *   BACKUP_RCLONE_REMOTE            例 gdrive:bfaith-backup/warehouse
 *   BACKUP_RCLONE_CONFIG            rclone.conf のパス (SYSTEM 実行時は必須。例 C:\tools\rclone\rclone.conf)
 *   BACKUP_UPLOAD_TIMEOUT_MS        rclone アップロードの上限 (default: 5400000 = 90分)
 *   BACKUP_REMOTE_DAILY_KEEP_DAYS   リモート日次世代の保持日数 (default: 14)
 *   BACKUP_REMOTE_MONTHLY_KEEP_DAYS リモート月次世代の保持日数 (default: 400 ≒ 13か月)
 *   BACKUP_GZIP_LEVEL               gzip レベル 1-9 (default: 5)
 *   BACKUP_SENTINEL_TABLES          意味的検証する必須テーブル (default: 'f_sales_by_product,m_products'。空文字で無効)
 *   WAREHOUSE_BUSINESS_DATE         daily-sync が渡す JST 業務日付 (無ければ自前で JST 計算)
 */
import 'dotenv/config';
import Database from 'better-sqlite3';
import { acquireLock, releaseLock } from './job-locks.js';
import { execFileSync } from 'child_process';
import fs from 'fs';
import path from 'path';
import zlib from 'zlib';
import crypto from 'crypto';
import { pipeline } from 'stream/promises';
import { Writable } from 'stream';

// ─── env 厳密検証 (Codex R1 Critical#1: NaN が slice() に流れると全世代削除事故) ───
function envInt(name, def, min, max) {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return def;
  const n = Number(raw);
  if (!Number.isInteger(n) || n < min || n > max) {
    throw new Error(`env ${name} が不正です: "${raw}" (整数 ${min}〜${max} を指定)`);
  }
  return n;
}

// ─── 引数・パス解決 (db.js と同じ優先順: --data-dir > env DATA_DIR > cwd/data) ───
const argv = process.argv.slice(2);
let dataDirArg = null;
for (let i = 0; i < argv.length; i++) {
  if (argv[i] === '--data-dir' && argv[i + 1]) dataDirArg = argv[++i];
  // 位置引数は無視 (daily-sync runScript は引数なし時 '7' を強制付与する旧仕様互換のため)
}
const DATA_DIR = dataDirArg || process.env.DATA_DIR || path.join(process.cwd(), 'data');
const DB_FILE = path.join(DATA_DIR, 'warehouse.db');
const BACKUP_DIR = process.env.BACKUP_DIR || path.join(DATA_DIR, 'backup');
const DAILY_DIR = path.join(BACKUP_DIR, 'daily');

const REQUIRE_REMOTE = process.env.BACKUP_REQUIRE_REMOTE !== '0';
const REMOTE = (process.env.BACKUP_RCLONE_REMOTE || '').trim();
const RCLONE_CONFIG = (process.env.BACKUP_RCLONE_CONFIG || '').trim();
let LOCAL_KEEP, UPLOAD_TIMEOUT_MS, REMOTE_DAILY_KEEP_DAYS, REMOTE_MONTHLY_KEEP_DAYS, GZIP_LEVEL;
try {
  LOCAL_KEEP = envInt('BACKUP_LOCAL_KEEP', 3, 1, 60);
  UPLOAD_TIMEOUT_MS = envInt('BACKUP_UPLOAD_TIMEOUT_MS', 5400000, 60000, 4 * 3600000);
  REMOTE_DAILY_KEEP_DAYS = envInt('BACKUP_REMOTE_DAILY_KEEP_DAYS', 14, 2, 3650);
  REMOTE_MONTHLY_KEEP_DAYS = envInt('BACKUP_REMOTE_MONTHLY_KEEP_DAYS', 400, 35, 3650);
  GZIP_LEVEL = envInt('BACKUP_GZIP_LEVEL', 5, 1, 9);
} catch (e) {
  console.error(`FATAL: ${e.message}`);
  process.exit(1);
}
const SENTINEL_TABLES = (process.env.BACKUP_SENTINEL_TABLES ?? 'f_sales_by_product,m_products')
  .split(',').map((s) => s.trim()).filter(Boolean);

// JST 業務日付 (feedback_jst_to_iso_string_trap: toISOString は UTC なので +9h してから切り出す)
function jstToday() {
  const d = new Date(Date.now() + 9 * 3600 * 1000);
  return d.toISOString().slice(0, 10);
}
// 実在日検証 (Codex R2 Low: 形式一致だけだと 2026-99-01 等で誤った世代名になる)
function isRealDate(s) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return false;
  const [y, m, d] = s.split('-').map(Number);
  const dt = new Date(Date.UTC(y, m - 1, d));
  return dt.getUTCFullYear() === y && dt.getUTCMonth() === m - 1 && dt.getUTCDate() === d;
}
const BUSINESS_DATE = isRealDate(process.env.WAREHOUSE_BUSINESS_DATE || '')
  ? process.env.WAREHOUSE_BUSINESS_DATE
  : jstToday();
const YM = BUSINESS_DATE.slice(0, 7);
const IS_MONTH_FIRST = BUSINESS_DATE.slice(8, 10) === '01';

// staging は pid 付き一意名 (Codex R1 Medium: 固定 tmp 名は手動実行/retry の同時実行で相互破壊)
const PID = process.pid;
const SNAPSHOT_TMP = path.join(DAILY_DIR, `warehouse-${BUSINESS_DATE}.db.pid${PID}.tmp`);
const GZ_TMP = path.join(DAILY_DIR, `warehouse-${BUSINESS_DATE}.db.gz.pid${PID}.tmp`);
const MANIFEST_TMP = path.join(DAILY_DIR, `warehouse-${BUSINESS_DATE}.manifest.pid${PID}.tmp`);
// アップロード元は run 専有 (Codex R8: 共有名 GZ_FINAL を並行 run が rename 置換すると
// 「自分の sha 名 × 他人の実体」の不一致アップロードになる → 自分が検証した inode への
// hardlink をアップロード元に固定する。hardlink 先の共有名が置換されても inode は不変)
const GZ_UPLOAD_TMP = path.join(DAILY_DIR, `warehouse-${BUSINESS_DATE}.db.gz.upload.pid${PID}.tmp`);
const MANIFEST_UPLOAD_TMP = path.join(DAILY_DIR, `warehouse-${BUSINESS_DATE}.manifest.upload.pid${PID}.tmp`);
const GZ_FINAL = path.join(DAILY_DIR, `warehouse-${BUSINESS_DATE}.db.gz`);
const MANIFEST_FINAL = path.join(DAILY_DIR, `warehouse-${BUSINESS_DATE}.manifest.json`);
const RESTORE_GZ_TMP = path.join(BACKUP_DIR, `restore-verify.pid${PID}.db.gz.tmp`);
const RESTORE_DB_TMP = path.join(BACKUP_DIR, `restore-verify.pid${PID}.db.tmp`);

const warnings = [];

function fmtGB(bytes) {
  if (bytes < 1e9) return `${Math.round(bytes / 1e6)}MB`;
  return `${Math.round((bytes / 1e9) * 100) / 100}GB`;
}

function freeBytes(dir) {
  const s = fs.statfsSync(dir);
  return s.bavail * s.bsize;
}

function rmSoft(p) {
  try {
    if (fs.existsSync(p)) fs.unlinkSync(p);
  } catch (e) {
    warnings.push(`🟡 一時ファイル削除失敗 ${path.basename(p)} (${e.message})`);
  }
}

// hardlink で run 専有の別名を作る (同一 inode = コピーコスト0、共有名側の rename 置換の影響を受けない)。
// hardlink 不可のファイルシステム (exFAT 等) ではフルコピーにフォールバック
function linkOrCopy(src, dest) {
  rmSoft(dest);
  try {
    fs.linkSync(src, dest);
  } catch {
    fs.copyFileSync(src, dest);
  }
}

// 12時間より古い staging 残骸 (クラッシュした過去 run のもの) を掃除。
// 実行中の別プロセスの staging (新しい) は触らない。
function cleanStaleStaging() {
  const cutoff = Date.now() - 12 * 3600000;
  for (const dir of [DAILY_DIR, BACKUP_DIR]) {
    let names = [];
    try {
      names = fs.readdirSync(dir).filter((n) => n.endsWith('.tmp'));
    } catch { continue; }
    for (const n of names) {
      const fp = path.join(dir, n);
      try {
        if (fs.statSync(fp).mtimeMs < cutoff) {
          fs.unlinkSync(fp);
          console.log(`[Backup] 古い staging 残骸を削除: ${n}`);
        }
      } catch (e) {
        warnings.push(`🟡 staging 残骸削除失敗 ${n} (${e.message})`);
      }
    }
  }
}

// rclone 実行 (execFileSync shell:false — feedback_execfile_vs_execsync と同方針)
// エラーは timeout / exit code / stderr 末尾を含めて分類 (Codex R1 Low)
// --max-duration + --cutoff-mode hard (Codex R6 High): 親 Node が timeout kill され execFileSync の
// 監視が消えても、rclone 自身が同じ上限で確実に自己終了する (孤児が lock ttl を超えて生き残らない)。
// ※ --cutoff-mode は rclone 1.57+ (soft だと転送中ファイルを中断しないため hard 必須)
function rclone(args, timeoutMs) {
  // --timeout/--contimeout/--retries: 非転送フェーズ (認証/listing) のハングと再試行ループも
  // 有限時間に抑える (Codex R7: --max-duration は転送上限であり非転送ハングを殺さない)
  const durArgs = [
    '--max-duration', `${Math.ceil(timeoutMs / 1000)}s`, '--cutoff-mode', 'hard',
    '--timeout', '5m', '--contimeout', '1m', '--retries', '1',
  ];
  const fullArgs = RCLONE_CONFIG ? ['--config', RCLONE_CONFIG, ...durArgs, ...args] : [...durArgs, ...args];
  try {
    return execFileSync('rclone', fullArgs, {
      encoding: 'utf-8',
      shell: false,
      windowsHide: true,
      timeout: timeoutMs,
      maxBuffer: 16 * 1024 * 1024,
    });
  } catch (e) {
    if (e.code === 'ENOENT') {
      throw new Error('rclone が見つかりません (PATH 未登録 or 未インストール)。引き継ぎ文書のセットアップ手順を参照');
    }
    const stderrTail = (e.stderr || '').trim().split('\n').slice(-2).join(' | ');
    const kind = e.signal ? `timeout/${e.signal}` : `exit=${e.status}`;
    throw new Error(`rclone ${args[0]} 失敗 (${kind}): ${stderrTail || e.message.split('\n')[0]}`);
  }
}

// quick_check + sentinel テーブル検証。戻り値: sentinel 件数マップ
function verifyDb(dbPath, label) {
  const db = new Database(dbPath, { readonly: true });
  try {
    db.pragma('busy_timeout = 60000');
    const rows = db.pragma('quick_check');
    const values = rows.map((r) => Object.values(r)[0]);
    if (values.length !== 1 || values[0] !== 'ok') {
      throw new Error(`${label} quick_check NG: ${values.slice(0, 5).join(' / ')}`);
    }
    const tables = db.prepare("SELECT count(*) AS c FROM sqlite_master WHERE type='table'").get().c;
    if (tables < 10) {
      throw new Error(`${label} テーブル数が異常に少ない (${tables}個)。スナップショット不完全の疑い`);
    }
    // 意味的検証 (Codex R1 Medium): 物理破損だけでなく「中身が空」を検出する
    const sentinels = {};
    for (const t of SENTINEL_TABLES) {
      if (!/^[A-Za-z0-9_]+$/.test(t)) throw new Error(`BACKUP_SENTINEL_TABLES に不正なテーブル名: "${t}"`);
      let c;
      try {
        c = db.prepare(`SELECT count(*) AS c FROM ${t}`).get().c;
      } catch {
        throw new Error(`${label} sentinel テーブル ${t} が存在しません`);
      }
      if (c === 0) throw new Error(`${label} sentinel テーブル ${t} が 0 件 (空バックアップの疑い)`);
      sentinels[t] = c;
    }
    return { tables, sentinels };
  } finally {
    db.close();
  }
}

// gzip 展開ストリーム検証 (ディスクに書かず、展開後バイト数が元サイズと一致するか)
async function verifyGzipStream(gzPath, expectedBytes) {
  let total = 0;
  const counter = new Writable({
    write(chunk, _enc, cb) {
      total += chunk.length;
      cb();
    },
  });
  await pipeline(fs.createReadStream(gzPath), zlib.createGunzip(), counter);
  if (total !== expectedBytes) {
    throw new Error(`gzip 検証NG: 展開サイズ ${total} ≠ 元サイズ ${expectedBytes}`);
  }
}

function readManifest() {
  try {
    const m = JSON.parse(fs.readFileSync(MANIFEST_FINAL, 'utf-8'));
    if (m && m.business_date === BUSINESS_DATE && Number.isInteger(m.snap_bytes) &&
        Number.isInteger(m.gz_bytes) && typeof m.gz_sha256 === 'string' && m.src_sig) return m;
  } catch {}
  return null;
}

// 元DBの鮮度シグネチャ (Codex R2 High#1: 先行ジョブの retry 復旧後に古い snapshot を
// 再利用しないため)。WAL モードでは本体 mtime がチェックポイントまで動かないので -wal も見る
function srcSignature() {
  const st = fs.statSync(DB_FILE);
  const sig = { db_mtime_ms: Math.round(st.mtimeMs), db_bytes: st.size, wal_mtime_ms: 0, wal_bytes: 0 };
  try {
    const wal = fs.statSync(`${DB_FILE}-wal`);
    sig.wal_mtime_ms = Math.round(wal.mtimeMs);
    sig.wal_bytes = wal.size;
  } catch {}
  return sig;
}

async function sha256File(p) {
  const hash = crypto.createHash('sha256');
  await pipeline(fs.createReadStream(p), new Writable({
    write(chunk, _enc, cb) { hash.update(chunk); cb(); },
  }));
  return hash.digest('hex');
}

// VACUUM INTO (SQLITE_BUSY は 60秒待って 1 回だけ再試行 — Codex R1 Medium)
async function vacuumInto(target) {
  for (let attempt = 1; ; attempt++) {
    const src = new Database(DB_FILE, { readonly: true });
    try {
      src.pragma('busy_timeout = 60000');
      src.prepare('VACUUM INTO ?').run(target);
      return;
    } catch (e) {
      rmSoft(target);
      if (attempt === 1 && /SQLITE_BUSY/.test(e.code || e.message)) {
        console.log('[Backup] SQLITE_BUSY → 60秒待って再試行');
        await new Promise((r) => setTimeout(r, 60000));
        continue;
      }
      throw e;
    } finally {
      src.close();
    }
  }
}

async function main() {
  const t0 = Date.now();
  console.log(`[Backup] 開始 business_date=${BUSINESS_DATE} db=${DB_FILE}`);

  // offsite 必須チェックは重い処理の前に fail-fast (retry も数秒で終わる)
  if (!REMOTE && REQUIRE_REMOTE) {
    throw new Error('BACKUP_RCLONE_REMOTE 未設定。offsite が本目的のため失敗扱いにしています。' +
      'rclone セットアップ (引き継ぎ文書参照) するか、意図的にローカルのみで良ければ BACKUP_REQUIRE_REMOTE=0 を .env に');
  }
  if (!fs.existsSync(DB_FILE)) {
    throw new Error(`warehouse.db が見つかりません: ${DB_FILE} (DATA_DIR 設定を確認)`);
  }
  fs.mkdirSync(DAILY_DIR, { recursive: true });

  // ─── 排他ロック (Codex R3 High: 同日並行実行だと gz と manifest の rename が交錯し
  // 不整合ペアを offsite へ上げうる)。既存成果物の確認〜rename〜upload 完了まで保持。
  //
  // heartbeat は使わない (Codex R4 High: VACUUM/execFileSync(rclone) 等の同期処理が
  // イベントループを塞ぎ setInterval が最大90分止まる → ttl 30分では実行中に失効する)。
  // 代わりに「生きている run は絶対に失効しない + ttl 失効時点で孤児も確実に死んでいる」を
  // ttl = 親timeout(6h) + rclone自己上限の最大値(UPLOAD_TIMEOUT_MS) + 余裕30分 で構造的に保証:
  //   - 生きた run は親 timeout 6h 以内に終わる/殺される → ttl 内
  //   - 親 kill で孤児化した rclone も --max-duration (Codex R6) で spawn から自己上限内に自己終了。
  //     spawn は遅くとも親 kill 時点 (acquire+6h) なので、孤児の死は acquire + 6h + 上限 < ttl
  // クラッシュ残置ロックの pid 生存確認による即時解放は行わない (Codex R5 High: 孤児 rclone
  // 継続中に次 run が走り並行アップロードになる)。残置は ttl 失効の takeover (acquireLock 内蔵)
  // に任せる = クラッシュ後は同日 retry が ttl までスキップされるが安全側
  //
  // ロックは warehouse.db ではなく専用ファイルに置く: warehouse.db に書くと lock の
  // INSERT/DELETE 自体が src_sig (鮮度シグネチャ) を毎回動かし高速パスが永久に無効化されるうえ、
  // 本番 DB への不要な書き込みになる。※ show-job-locks.js (warehouse.db 対象) には表示されない
  const LOCK_TTL_MS = 6 * 3600000 + UPLOAD_TIMEOUT_MS + 30 * 60000;
  const lockDb = new Database(path.join(BACKUP_DIR, 'backup-locks.db'));
  lockDb.pragma('busy_timeout = 60000');
  lockDb.exec(`CREATE TABLE IF NOT EXISTS job_locks (
    job_name      TEXT PRIMARY KEY,
    holder_id     TEXT NOT NULL,
    acquired_at   TEXT NOT NULL,
    expires_at    TEXT NOT NULL,
    heartbeat_at  TEXT NOT NULL
  )`);
  const lock = acquireLock(lockDb, 'backup-warehouse', { ttlMs: LOCK_TTL_MS });
  if (!lock) {
    lockDb.close();
    console.error('FATAL: backup-warehouse lock 取得失敗 (別プロセスがバックアップ実行中)。今回はスキップ (retry対象)');
    process.exit(75); // EX_TEMPFAIL (pml-pipeline lock と同規約)
  }

  try {
    await run(t0);
  } finally {
    try { releaseLock(lockDb, lock); } catch (e) { warnings.push(`🟡 lock 解放失敗 (${e.message})`); }
    lockDb.close();
  }
}


async function run(t0) {
  cleanStaleStaging();

  const srcSize = fs.statSync(DB_FILE).size;

  // ─── 1. 当日世代の作成 (完成済み かつ 元DBが変わっていなければスキップ = 冪等の高速パス) ───
  // 鮮度条件 (Codex R2 High#1): manifest 記録時の src_sig (db+wal の mtime/size) が現在と一致する
  // 場合のみ再利用。夜間 retry で f_sales 等が復旧して DB が変わっていれば作り直す。
  // 同一性は gz の SHA-256 で検証 (Codex R2 Medium#2: サイズ一致だけでは新gz+旧manifest を検出できない)
  let snapSize, gzSize, gzSha, verifyInfo, manifestJson;
  const manifest = readManifest();
  let reused = false;
  const curSig = srcSignature();
  if (manifest && fs.existsSync(GZ_FINAL) &&
      JSON.stringify(manifest.src_sig) === JSON.stringify(curSig)) {
    // 先に hardlink で inode を確保してから検証する (検証→リンクの間に他 run が置換する隙を作らない)
    linkOrCopy(GZ_FINAL, GZ_UPLOAD_TMP);
    const actualSha = await sha256File(GZ_UPLOAD_TMP);
    if (actualSha === manifest.gz_sha256) {
      console.log('[Backup] 当日分は作成済み+元DB変化なし → 再利用 (retry 高速パス)');
      snapSize = manifest.snap_bytes;
      gzSize = manifest.gz_bytes;
      gzSha = manifest.gz_sha256;
      verifyInfo = { tables: manifest.tables, sentinels: manifest.sentinels || {} };
      manifestJson = JSON.stringify(manifest, null, 2);
      reused = true;
    } else {
      rmSoft(GZ_UPLOAD_TMP);
      console.log('[Backup] gz と manifest の SHA 不一致 → 作り直し');
    }
  }
  if (!reused) {
    // 空き容量ガード: 出力先ボリュームで判定 (Codex R1 High: DATA_DIR と BACKUP_DIR は別ボリュームでありうる)
    const need = Math.ceil(srcSize * 1.6);
    const free = freeBytes(DAILY_DIR);
    if (free < need) {
      throw new Error(`空き容量不足: ${BACKUP_DIR} のボリューム残り ${fmtGB(free)} < 必要 ${fmtGB(need)} (元DB ${fmtGB(srcSize)})`);
    }

    // VACUUM INTO: 読み取りトランザクションなので WAL の並行 writer は妨げない。
    // 逆に言うと「daily-sync 完了直後 ± WarehouseServer の並行書き込み」時点のスナップショット
    // であり、厳密な業務断面ではない (Codex R1 Medium — 日次バックアップ用途では許容)。
    console.log(`[Backup] VACUUM INTO 開始 (元 ${fmtGB(srcSize)})`);
    await vacuumInto(SNAPSHOT_TMP);
    snapSize = fs.statSync(SNAPSHOT_TMP).size;
    console.log(`[Backup] snapshot 完了 ${fmtGB(snapSize)} (${Math.round((Date.now() - t0) / 1000)}秒)`);

    verifyInfo = verifyDb(SNAPSHOT_TMP, 'snapshot');
    console.log(`[Backup] 検証 ok (テーブル ${verifyInfo.tables} 個, sentinel: ${Object.entries(verifyInfo.sentinels).map(([k, v]) => `${k}=${v}`).join(', ') || 'なし'})`);

    await pipeline(
      fs.createReadStream(SNAPSHOT_TMP),
      zlib.createGzip({ level: GZIP_LEVEL }),
      fs.createWriteStream(GZ_TMP)
    );
    await verifyGzipStream(GZ_TMP, snapSize);
    gzSize = fs.statSync(GZ_TMP).size;
    gzSha = await sha256File(GZ_TMP);
    // manifest も staging → rename で gz とペア確定 (Codex R2 Medium#2)。
    // 順序は gz → manifest。間で落ちても「新gz+旧manifest」は SHA 不一致として検出され作り直しになる
    manifestJson = JSON.stringify({
      business_date: BUSINESS_DATE,
      snap_bytes: snapSize,
      gz_bytes: gzSize,
      gz_sha256: gzSha,
      src_bytes: srcSize,
      src_sig: curSig,
      tables: verifyInfo.tables,
      sentinels: verifyInfo.sentinels,
      gzip_level: GZIP_LEVEL,
      created_at: new Date().toISOString(),
    }, null, 2);
    fs.writeFileSync(MANIFEST_TMP, manifestJson);
    // アップロード元 inode を rename 前に確保 (Codex R8: 共有名は並行 run に置換されうる)
    linkOrCopy(GZ_TMP, GZ_UPLOAD_TMP);
    // 原子的置換 (Windows の fs.rename は MoveFileEx REPLACE_EXISTING 相当で既存を上書き)。
    // 検証済み staging を最後に据えるので、途中失敗が成功済み当日世代を壊すことはない
    fs.renameSync(GZ_TMP, GZ_FINAL);
    fs.renameSync(MANIFEST_TMP, MANIFEST_FINAL);
    rmSoft(SNAPSHOT_TMP);
    console.log(`[Backup] gzip 完了 ${fmtGB(gzSize)} (圧縮率 ${Math.round((gzSize / snapSize) * 100)}%)`);
  }

  // ─── 2. rclone offsite ───
  let remoteNote = '🟡 offsite未設定(ローカルのみ運用、BACKUP_REQUIRE_REMOTE=0)';
  let remoteOk = false;
  // リモート gz 名には SHA 先頭8桁を含めて一意化 (Codex R7 対策の本丸: 万一 ttl を超えて
  // 生き残った孤児 rclone がいても、run ごとに別オブジェクト名なので同名上書き競合が構造的に
  // 起きない。manifest は last-writer-wins だが、どちらが勝っても自分の SHA 名 gz を指す)
  const gzRemoteName = `warehouse-${BUSINESS_DATE}-${gzSha.slice(0, 8)}.db.gz`;
  if (REMOTE) {
    // アップロード元は run 専有ファイルのみ (Codex R8): gz = 検証済み inode への hardlink、
    // manifest = 自分が検証/生成した JSON を専有 tmp に書いたもの。共有名は一切読まない
    fs.writeFileSync(MANIFEST_UPLOAD_TMP, manifestJson);
    console.log(`[Backup] rclone アップロード → ${REMOTE}/daily/${gzRemoteName}`);
    rclone(['copyto', GZ_UPLOAD_TMP, `${REMOTE}/daily/${gzRemoteName}`], UPLOAD_TIMEOUT_MS);
    // manifest は世代の構成要素 (検証情報・SHA・gz名) なので失敗=ジョブ失敗 (Codex R2 Medium#1。
    // retry は高速パスで gz を再利用し copyto は同一内容をスキップするので軽い)
    rclone(['copyto', MANIFEST_UPLOAD_TMP, `${REMOTE}/daily/warehouse-${BUSINESS_DATE}.manifest.json`], 300000);
    remoteOk = true;
    remoteNote = `offsite=daily/${gzRemoteName}`;

    // リモート日次世代の掃除 (warehouse-* に限定 — 他用途ファイルの誤削除防止)
    try {
      rclone(['delete', `${REMOTE}/daily`, '--min-age', `${REMOTE_DAILY_KEEP_DAYS}d`, '--include', 'warehouse-*'], 600000);
    } catch (e) {
      warnings.push(`🟡 リモート日次世代の掃除失敗 (${e.message})`);
    }

    // 月初: 月次世代 (サーバーサイドコピー)。失敗はジョブ失敗 (Codex R1 Medium:
    // warning 止まりだと翌日は IS_MONTH_FIRST=false でその月の月次世代が永久欠落する。
    // 失敗→夜間 retry は高速パスで .gz 再利用するので再アップロードも軽い)
    if (IS_MONTH_FIRST) {
      rclone(['copyto', `${REMOTE}/daily/${gzRemoteName}`, `${REMOTE}/monthly/warehouse-${YM}-${gzSha.slice(0, 8)}.db.gz`], 1800000);
      try {
        rclone(['delete', `${REMOTE}/monthly`, '--min-age', `${REMOTE_MONTHLY_KEEP_DAYS}d`, '--include', 'warehouse-*'], 600000);
      } catch (e) {
        warnings.push(`🟡 リモート月次世代の掃除失敗 (${e.message})`);
      }
      remoteNote += ` +monthly/${YM}`;
    }
  }

  // ─── 3. ローカル世代整理 (offsite 成功後のみ。upload 失敗時は旧世代を温存 — Codex R1 High) ───
  if (remoteOk || !REQUIRE_REMOTE) {
    const gens = fs.readdirSync(DAILY_DIR)
      .filter((n) => /^warehouse-\d{4}-\d{2}-\d{2}\.db\.gz$/.test(n))
      .sort()
      .reverse();
    for (const old of gens.slice(LOCAL_KEEP)) {
      rmSoft(path.join(DAILY_DIR, old));
      rmSoft(path.join(DAILY_DIR, old.replace(/\.db\.gz$/, '.manifest.json')));
      console.log(`[Backup] ローカル旧世代削除: ${old}`);
    }
  }

  // ─── 4. 月初のみ: フル復元テスト。「戻せた実績」を毎月作る ───
  // offsite 設定時はリモートからダウンロードして検証 = 認証〜転送〜展開の復元経路全体を通す
  let restoreNote = '';
  if (IS_MONTH_FIRST) {
    if (freeBytes(BACKUP_DIR) > snapSize * 1.2 + gzSize) {
      const label = REMOTE ? 'offsite' : 'ローカル';
      console.log(`[Backup] 月初フル復元テスト開始 (${label})`);
      let gzForRestore = GZ_FINAL;
      if (REMOTE) {
        rclone(['copyto', `${REMOTE}/daily/${gzRemoteName}`, RESTORE_GZ_TMP], UPLOAD_TIMEOUT_MS);
        gzForRestore = RESTORE_GZ_TMP;
      }
      await pipeline(fs.createReadStream(gzForRestore), zlib.createGunzip(), fs.createWriteStream(RESTORE_DB_TMP));
      verifyDb(RESTORE_DB_TMP, `復元テスト(${label})`);
      rmSoft(RESTORE_DB_TMP);
      rmSoft(RESTORE_GZ_TMP);
      restoreNote = ` 復元テスト(${label})ok`;
      console.log(`[Backup] 復元テスト(${label}) ok`);
    } else {
      warnings.push('🟡 月初復元テストskip (空き容量不足)');
    }
  }

  rmSoft(GZ_UPLOAD_TMP);
  rmSoft(MANIFEST_UPLOAD_TMP);

  const sec = Math.round((Date.now() - t0) / 1000);
  const warnNote = warnings.length ? ` ${warnings.join(' / ')}` : '';
  const reusedNote = reused ? ' (retry再利用)' : '';
  // 最終行が daily-sync の GChat summary になる
  console.log(`${fmtGB(snapSize)}→gz ${fmtGB(gzSize)}${reusedNote}, ${remoteNote}${restoreNote} (${sec}秒)${warnNote}`);
}

main().catch((e) => {
  // 自分の staging だけ掃除 (成功済み GZ_FINAL / manifest は温存)
  rmSoft(SNAPSHOT_TMP);
  rmSoft(GZ_TMP);
  rmSoft(MANIFEST_TMP);
  rmSoft(GZ_UPLOAD_TMP);
  rmSoft(MANIFEST_UPLOAD_TMP);
  rmSoft(RESTORE_GZ_TMP);
  rmSoft(RESTORE_DB_TMP);
  console.error(`FATAL: ${e.message}`);
  process.exit(1);
});
